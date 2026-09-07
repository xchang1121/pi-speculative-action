import assert from "node:assert/strict";
import { fork } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { serialize } from "node:v8";

// Fixed-command qualification, not a plugin provider or an arbitrary-script sandbox.
// Dependencies are explicitly installed outside this package; never downloaded by this script.
const dependencyRoot = process.argv[2];
assert.ok(dependencyRoot, "Usage: node bench/portable-kernel.mjs <directory containing node_modules> [--pi-tools]");
const started = performance.now(), worker = await prepareWorker();
try {
	const seed = { "/workspace/a.txt": Buffer.from("before\nneedle\nafter\n"), "/workspace/sub/b.txt": Buffer.from("needle two\n") };
	const kernel = (commands, options) => worker.request({ kind: "kernel", image: { files: seed, directories: [] }, commands }, options);
	const [pipeline, search, overwrite] = await kernel([
		{ name: "sh", args: ["-c", "cat /workspace/a.txt | grep needle > /workspace/derived.txt"] },
		{ name: "rg", args: ["--json", "--line-number", "--color=never", "--sort=path", "--hidden", "--no-config", "--", "needle", "/workspace"] },
		{ name: "sh", args: ["-c", "printf changed > /workspace/a.txt; cat /workspace/a.txt"] },
	]);
	assert.equal(pipeline.exitCode, 0, pipeline.stderr); assert.equal(search.exitCode, 0, search.stderr);
	const matches = search.stdout.trim().split("\n").map(JSON.parse).filter((event) => event.type === "match")
		.map((event) => [event.data.path.text, event.data.line_number, event.data.lines.text]);
	assert.deepEqual(matches, [["/workspace/a.txt", 2, "needle\n"], ["/workspace/derived.txt", 1, "needle\n"], ["/workspace/sub/b.txt", 1, "needle two\n"]]);
	assert.equal(overwrite.stdout, "changed"); assert.equal(seed["/workspace/a.txt"].toString(), "before\nneedle\nafter\n");
	for (const command of ["cat /etc/hostname", "cat /dev/host", "cat /dev/hostreq", "curl https://example.com"]) {
		assert.notEqual((await kernel([{ name: "sh", args: ["-c", command] }]))[0].exitCode, 0, command + ": unexpected host capability");
	}
	for (const command of ["printf '%01048577d' 1", "while :; do printf 'repeat\\n'; done | cat"]) {
		await assert.rejects(kernel([{ name: "sh", args: ["-c", command] }]), /budget/, command);
	}
	await assert.rejects(worker.request({ kind: "kernel", image: { files: seed, directories: [] }, probeSparse: true }), /filesystem budget/);
	assert.equal((await kernel([{ name: "sh", args: ["-c", "cat /workspace/a.txt"] }]))[0].stdout, seed["/workspace/a.txt"].toString());
	for (let repeat = 0; repeat < 2; repeat++) assert.equal((await kernel([{ name: "sh", args: ["-c", "stat -c %Y /workspace/a.txt"] }]))[0].stdout, "1700000000\n");
	assert.ok(pipeline.clocks > 0 && search.clocks > 0 && search.random > 0);
	const pi = process.argv.includes("--pi-tools") ? await qualifyPiSearch(worker) : undefined;
	const cancellation = [];
	for (const mode of ["abort", "deadline", "input abort", "input deadline"]) {
		const interrupted = await prepareWorker(), controller = new AbortController();
		const late = Promise.withResolvers(), inputCompleted = Promise.withResolvers(), inputWait = mode.startsWith("input");
		try {
			const arrived = performance.now(); let entered = false;
			const abort = () => { entered = true; if (mode.endsWith("abort")) controller.abort(new Error("cancelled running guest")); };
			await assert.rejects(interrupted.request(inputWait ? { kind: "grep", root: process.cwd(), args: { pattern: "needle" } } : { kind: "spin" }, {
				signal: controller.signal, timeoutMs: mode.endsWith("deadline") ? 200 : 5000,
				onStarted: () => { if (!inputWait) abort(); },
				onInput: async () => {
					abort(); await late.promise; inputCompleted.resolve();
					if (mode.endsWith("abort")) throw new Error("late input failure");
					return { directory: true, size: 0 };
				},
			}), mode.endsWith("abort") ? /cancelled running guest/ : /deadline/);
			assert.ok(interrupted.closed(), "Actor fallback must not race a still-running worker");
			assert.ok(entered, "cancellation must exercise an entered guest, not just process startup");
			late.resolve(); if (inputWait) await inputCompleted.promise;
			cancellation.push({ mode, retirementMs: performance.now() - arrived });
		} finally { late.resolve(); await interrupted.dispose(); }
	}
	console.log(JSON.stringify({ platform: process.platform, node: process.version, engines: worker.engines, profile: worker.profile,
		workerPreparationMs: worker.preparationMs, processTotalMs: performance.now() - started,
		assertions: { sharedFilesystem: true, copyOnWrite: true, noGrantedHostPorts: true, moduleMemoryCapMiB: 64,
			writeBudget: true, sparseStoreWriteBudget: true, noPartialResultOnQuotaFailure: true, fixedMetadata: true, cancellation },
		pipeline, search: { ...search, stdout: undefined, matches }, pi,
		admission: "qualification only: full grep IPC and termination, not arbitrary shell mutations, native equivalence, or production enablement" }, null, 2));
} finally { await worker.dispose(); }

async function prepareWorker() {
	const started = performance.now(), child = fork(new URL("./portable-worker.mjs", import.meta.url), [path.resolve(dependencyRoot)], {
		execArgv: ["--wasm-max-mem-pages=1024", "--max-old-space-size=128"], serialization: "advanced", silent: true, windowsHide: true,
	});
	let pending, nextID = 0, closed = false, diagnosticBytes = 0;
	let readyResolve, readyReject, closeResolve;
	const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
	const closure = new Promise((resolve) => { closeResolve = resolve; });
	const terminate = (error) => { if (pending) pending.failure ??= error; readyReject(error); child.kill("SIGKILL"); };
	const startup = setTimeout(() => terminate(new Error("worker preparation deadline")), 15_000);
	child.on("error", terminate);
	child.on("close", () => {
		closed = true; clearTimeout(startup);
		const error = pending?.failure ?? new Error("worker closed before completion");
		pending?.settle(error); readyReject(error); closeResolve();
	});
	for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => {
		if ((diagnosticBytes += bytes.length) > 1024 * 1024) terminate(new Error("worker diagnostic budget"));
	});
	child.on("message", (message) => {
		if (message.type === "ready") { clearTimeout(startup); readyResolve(message); return; }
		if (!pending || message.id !== pending.id || pending.failure) return;
		if (message.type === "started") { try { pending.onStarted?.(); } catch (error) { terminate(error); } }
		else if (message.type === "checkpoint") {
			const admitted = pending;
			Promise.resolve().then(() => admitted.onCheckpoint?.()).then(() => {
				if (pending === admitted && !admitted.failure) child.send({ type: "resume", id: admitted.id }, (error) => { if (error) terminate(error); });
			}, terminate);
		} else if (message.type === "input") {
			const admitted = pending;
			Promise.resolve().then(() => admitted.onInput(message.operation, message.target)).then((value) => ({ value }),
				(error) => ({ error: String(error.message ?? error).slice(0, 8192), code: error.code })).then((response) => {
				if (pending !== admitted || admitted.failure) return;
				if ((admitted.inputBytes += serialize(response).byteLength) > admitted.inputLimit) response = { error: "input byte budget" };
				child.send({ type: "input", id: admitted.id, sequence: message.sequence, ...response },
					(error) => { if (error) terminate(error); });
			}).catch(terminate);
		} else if (message.type === "result") pending.settle(message.error ? new Error(message.error) : undefined, message.result);
	});
	try {
		const { profile, engines } = await ready;
		return { profile, engines, preparationMs: performance.now() - started, closed: () => closed, closure,
			dispose: async () => { if (!closed) terminate(new Error("worker disposed")); await closure; },
			request: (input, { signal, timeoutMs = 5000, onStarted, onCheckpoint, onInput } = {}) => new Promise((resolve, reject) => {
				if (signal?.aborted) { reject(signal.reason); return; }
				if (closed || pending) { reject(new Error("worker unavailable/busy")); return; }
				assert.ok(serialize(input).byteLength <= profile.limits.requestBytes, "request frame budget");
				assert.ok(Object.values(input.image?.files ?? {}).reduce((size, bytes) => size + bytes.length, 0) <= profile.limits.inputBytes, "input byte budget");
				const id = ++nextID, abort = () => terminate(signal.reason);
				const timer = setTimeout(() => terminate(new Error("worker execution deadline")), timeoutMs);
				pending = { id, onStarted, onCheckpoint, onInput, inputBytes: 0, inputLimit: profile.limits.inputBytes, settle: (error, result) => {
					clearTimeout(timer); signal?.removeEventListener("abort", abort); pending = undefined;
					if (error) reject(error); else resolve(result);
				} };
				signal?.addEventListener("abort", abort, { once: true });
				child.send({ type: "request", id, input }, (error) => { if (error) terminate(error); });
			}),
		};
	} catch (error) { terminate(error); await closure; throw error; }
}

/** Full original Pi tool in independent Actor/producer workers; Runtime owns admission and adoption. */
async function qualifyPiSearch(worker) {
	const { createGrepToolDefinition } = await import("@earendil-works/pi-coding-agent");
	const { createFauxCore, fauxAssistantMessage, fauxToolCall } = await import("@earendil-works/pi-ai");
	const { createSpeculativeActionHost } = await import("../dist/agent-integration.js");
	const { ActionSemanticsRegistry, PI_ACTION_SEMANTICS } = await import("../dist/action-semantics.js");
	const { RESOURCE_OBSERVATION_EFFECTS } = await import("../dist/effect-model.js");
	const { createResourceSnapshotExecutionWorld } = await import("../dist/agent-execution-world.js");
	const profile = worker.profile;
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-portable-profile-"));
	let actorWorker;
	const counts = { producer: 0, actor: 0, contextReads: 0 }, reads = new Set();
	let inputRequests = 0, inputBytes = 0;
	const execute = async (role, source, request, checkpoint) => {
		counts[role]++;
		const output = await (role === "producer" ? worker : actorWorker).request({ kind: "grep", root, args: request.args, pause: !!checkpoint },
			{ signal: request.signal, onCheckpoint: checkpoint, onInput: async (operation, target) => {
				inputRequests++;
				if (operation === "readFile") reads.add(target);
				const value = await readInput(source, root, operation, target, profile.limits.inputBytes);
				inputBytes += serialize(value).byteLength; return value;
			} });
		counts.contextReads += output.contextReads;
		return { result: output.result, isError: output.isError };
	};
	const tool = createGrepToolDefinition(root), definition = { ...PI_ACTION_SEMANTICS.definition("grep"), epoch: profile.id,
		effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "tree_content" };
	const semantics = new ActionSemanticsRegistry([definition]);
	const resources = createResourceSnapshotExecutionWorld(semantics, { tools: ["grep"], maxBytes: () => profile.limits.inputBytes });
	const model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
	const signal = new AbortController().signal, journeys = [];
	const args = { pattern: "needle", path: ".", context: 1, limit: 1000 };
	function journey(checkpoint, capacity = 1) {
		const candidate = Promise.withResolvers(), authorized = Promise.withResolvers();
		let prediction = true, turnID, actorWaiting = false, actorCalls = 0, feedback;
		const invocation = { executor: profile.id, identity: profile,
			filesystem: async (view, request) => execute("producer", view, request, checkpoint) };
		const host = createSpeculativeActionHost("portable-" + journeys.length, {
			cwd: root, getSettings: () => ({ enabled: true, drafterEnabled: prediction, drafterGateEnabled: false,
				drafterMaxDepth: 0, candidateLimit: 1, maxConcurrentActions: capacity, tools: prediction ? ["grep"] : [],
				patternAware: { enabled: false }, selfSpeculation: { enabled: false } }),
			draftModel: model, complete: async () => fauxAssistantMessage(fauxToolCall("grep", args), { stopReason: "toolUse" }),
			actionSemantics: semantics, resolveInvocation: () => invocation,
			preflight: () => { if (actorWaiting) authorized.resolve(); return true; },
			executionWorlds: [resources],
			onEvent: (event) => {
				if (event.type === "candidate" && event.candidate.origin === "prediction" && event.state.status !== "running") candidate.resolve(event.state);
			},
			onActorActionSettled: ({ settlement }) => feedback?.resolve(settlement),
		});
		journeys.push(host);
		return { host, candidate: candidate.promise, authorized: authorized.promise, actorCalls: () => actorCalls,
			start: async (id, predict = true) => {
				if (turnID) await host.finishTurn(turnID);
				turnID = id; prediction = predict;
				await host.startTurn({ turnID, actorModel: model, actorOptions: undefined, tools: [tool],
					context: { systemPrompt: "qualification", messages: [], tools: [tool] } });
			},
			actor: async (id, query = args) => {
				actorWaiting = true; feedback = Promise.withResolvers();
				const arrived = performance.now();
				const output = await host.execute({ turnID, id, tool: "grep", args: query, tools: [tool] }, signal, async (operation) => {
					actorCalls++; return (await execute("actor", fs, { args: operation.input, signal: operation.signal })).result;
				});
				actorWaiting = false;
				return { output, totalMs: performance.now() - arrived, settlement: await bounded(feedback.promise, "Actor settlement") };
			},
		};
	}
	try {
		actorWorker = await prepareWorker();
		assert.deepEqual(actorWorker.profile, profile, "independent execution capacity must keep the same identity");
		await fs.mkdir(path.join(root, ".git")); await fs.mkdir(path.join(root, "empty"));
		await fs.writeFile(path.join(root, ".git/HEAD"), "ref: refs/heads/main\n");
		await fs.writeFile(path.join(root, ".gitignore"), "ignored.*\n");
		await fs.writeFile(path.join(root, "ignored.bin"), Buffer.alloc(16 * 1024 * 1024, "x"));
		await fs.writeFile(path.join(root, "ignored.txt"), "needle ignored\n");
		await fs.writeFile(path.join(root, "notes.txt"), "before\nneedle\nafter\n");
		for (let index = 0; index < 16; index++) await fs.writeFile(path.join(root, "data-" + index + ".txt"), "no match\n".repeat(8192) + "needle " + index + "\n");
		const sample = async (execute) => {
			const times = []; let output;
			for (let index = 0; index < 3; index++) { const started = performance.now(); output = await execute(); times.push(performance.now() - started); }
			return { output, medianMs: times.sort((a, b) => a - b)[1] };
		};
		const baseline = await sample(async () => execute("actor", fs, { args, signal }));
		const inputTransport = { meanRequests: inputRequests / 3, meanPayloadBytes: inputBytes / 3, ignoredBytes: 16 * 1024 * 1024 };
		assert.ok(!reads.has("/workspace/ignored.bin") && !reads.has("/workspace/ignored.txt"), "the broker transferred ignored content");
		await assert.rejects(execute("actor", fs, { args: { ...args, path: "ignored.bin" }, signal }), /input byte budget/);
		await assert.rejects(actorWorker.request({ kind: "grep", root, args }, {
			onInput: () => { throw new Error("resource_access_unproven"); },
		}), /resource_access_unproven/, "a guest must not turn missing authority into an empty successful search");
		const native = await sample(() => tool.execute("native", args, signal)), expected = baseline.output.result;
		const ready = journey(); await ready.start("produce");
		const completed = await bounded(ready.candidate, "completed candidate"); assert.equal(completed.status, "succeeded", JSON.stringify(completed));
		await ready.start("recall", false);
		const beforeHit = counts.producer, adopted = await ready.actor("ready");
		assert.deepEqual(adopted.output, expected); assert.equal(adopted.settlement.provider.kind, "speculative");
		assert.equal(counts.producer, beforeHit, "completed adoption executed the search again");
		const queries = [{ ...args, pattern: "after", limit: 1 }, { ...args, pattern: "absent" }, { ...args, glob: "*.txt", context: 0 }];
		for (const query of queries) {
			const actor = await execute("actor", fs, { args: query, signal });
			const reconstructed = await ready.actor("retained-" + queries.indexOf(query), query);
			assert.deepEqual(reconstructed.output, actor.result);
			assert.equal(reconstructed.settlement.provider.kind, query.glob ? "actor" : "speculative");
			if (!query.glob) assert.equal(reconstructed.settlement.provider.match?.projector, "resource.inputs");
		}
		assert.equal(ready.actorCalls(), 1, "a new query cannot extend a sealed candidate's input authority");
		const reached = Promise.withResolvers(), resume = Promise.withResolvers();
		const running = journey(async () => { reached.resolve(); await resume.promise; });
		await running.start("running");
		try {
			assert.equal(await bounded(Promise.race([reached.promise, running.candidate]), "running search checkpoint"), undefined);
			const joined = running.actor("join");
			assert.equal(await bounded(Promise.race([running.authorized, joined]), "Runtime running admission"), undefined);
			resume.resolve();
			const hit = await joined;
			assert.deepEqual(hit.output, expected); assert.equal(hit.settlement.provider.kind, "speculative");
			assert.equal(running.actorCalls(), 0);
		} finally { resume.resolve(); await running.host.dispose(); }
		await fs.writeFile(path.join(root, "notes.txt"), "changed\n");
		const stale = await ready.actor("stale");
		assert.equal(stale.settlement.provider.kind, "actor"); assert.equal(ready.actorCalls(), 2);
		assert.notDeepEqual(stale.output, expected);
		await ready.start("observed", false);
		const observed = await ready.actor("observed");
		assert.deepEqual(observed.output, stale.output);
		if (!resources.observation.capabilities.length) assert.equal(observed.settlement.provider.kind, "actor", "unproven host observations cannot be promoted");
		else if (observed.settlement.provider.kind === "actor") {
			const gate = observed.settlement.rejections.find((rejection) => rejection.cause.code === "candidate_join_not_profitable");
			assert.ok(gate, JSON.stringify(observed.settlement)); assert.ok(JSON.parse(gate.cause.detail).expectedNetBenefitMs < 0);
		}
		assert.equal(counts.producer, beforeHit + queries.length + 1, "rejected completed replay must not execute guest code");
		assert.equal(ready.actorCalls(), observed.settlement.provider.kind === "actor" ? 3 : 2);
		const changed = journey(() => fs.writeFile(path.join(root, "notes.txt"), "changed during search\n"));
		await changed.start("changing");
		const failed = await bounded(changed.candidate, "changed search");
		assert.equal(failed.status, "failed"); assert.match(JSON.stringify(failed.cause), /resource_fingerprint_changed/);
		assert.deepEqual((await changed.actor("changed")).output, stale.output); assert.equal(changed.actorCalls(), 1);
		const paused = Promise.withResolvers(), released = Promise.withResolvers();
		const cancelled = journey(async () => { paused.resolve(); await released.promise; }, 2);
		const disabled = { enabled: false, resourceCacheMaxEntries: 32, predictionTimeoutMs: 5000, tools: ["grep"] };
		await cancelled.start("cancelled");
		try {
			assert.equal(await bounded(Promise.race([paused.promise, cancelled.candidate]), "independent Actor checkpoint"), undefined);
			const different = { ...args, pattern: "absent" };
			const direct = await execute("actor", fs, { args: different, signal });
			const fallback = await cancelled.actor("independent", different);
			assert.deepEqual(fallback.output, direct.result); assert.equal(fallback.settlement.provider.kind, "actor");
			assert.equal(await Promise.race([cancelled.candidate, Promise.resolve("still running")]), "still running");
			assert.ok(!worker.closed(), "Actor fallback must not depend on producer termination");
			await cancelled.host.runtime.settingsChanged(disabled);
		} finally { released.resolve(); }
		assert.equal((await bounded(cancelled.candidate, "cancelled candidate")).status, "cancelled");
		await bounded(worker.closure, "cancelled worker retirement"); assert.ok(worker.closed());
		await cancelled.host.runtime.settingsChanged({ ...disabled, enabled: true });
		await cancelled.start("recovery", false);
		assert.deepEqual((await cancelled.actor("recovery")).output, stale.output); assert.equal(cancelled.actorCalls(), 2);
		assert.ok(counts.contextReads > 0, "the original Pi context reread was not exercised");
		return { nativeActorMs: native.medianMs, profileWarmActorMs: baseline.medianMs, inputTransport, speculativeMs: completed.executionMs,
			readyAdoptionMs: adopted.totalMs, calibratedReplay: { totalMs: observed.totalMs, ...observed.settlement }, retainedInputQueries: queries.length - 1, uncapturedInputFallbacks: 1, ...counts,
			crossTurnResultReuse: true, runningRuntimeJoin: true, runningActorCalls: running.actorCalls(),
			staleActorExecutions: stale.settlement.provider.kind === "actor" ? 1 : 0, changedDuringSearchRejected: true, cancelledFullToolDiscarded: true,
			actorRanWhileProducerPaused: true, ignoredContentNotTransferred: true, recoveryActorCalls: 1, actorWorkerPreparationMs: actorWorker.preparationMs,
			scope: "Runtime-owned explicit common-profile full-tool IPC; not native equivalence or production enablement" };
	} finally {
		await Promise.all(journeys.map((host) => host.dispose())); await actorWorker?.dispose();
		assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true });
	}
}

/** The same closed namespace for live Actor fixture IO and token-owned sealed inputs. */
async function readInput(source, root, operation, target, maxBytes) {
	assert.ok(["stat", "readdir", "readFile"].includes(operation) && path.posix.isAbsolute(target), "input operation denied");
	const fail = (code) => { throw Object.assign(new Error(`${code}: ${target}`), { code }); };
	const normalized = path.posix.normalize(target);
	if (normalized === "/") return operation === "stat" ? { directory: true, size: 0 } : operation === "readdir" ? ["workspace"] : fail("EISDIR");
	const relative = path.posix.relative("/workspace", normalized);
	if (relative === ".." || relative.startsWith("../")) return fail("ENOENT"); // Closed virtual namespace, never the host root.
	let physical = root;
	for (const segment of relative ? relative.split("/") : []) {
		if (!(await source.stat(physical)).isDirectory()) return fail("ENOTDIR");
		if (!(await source.readdir(physical)).includes(segment)) return fail("ENOENT"); // Negative evidence comes from captured entries.
		physical = path.join(physical, segment);
	}
	const stat = await source.stat(physical), directory = stat.isDirectory();
	if (operation === "stat") {
		assert.ok(directory || Number.isSafeInteger(stat.size), "file size is not proven by retained content");
		return { directory, size: directory ? 0 : stat.size };
	}
	if (operation === "readdir") return directory ? source.readdir(physical) : fail("ENOTDIR");
	if (directory) return fail("EISDIR");
	assert.ok(stat.size <= maxBytes, "input byte budget");
	return source.readFile(physical);
}

function bounded(promise, label) {
	let timer;
	return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label + " deadline")), 15_000); })])
		.finally(() => clearTimeout(timer));
}
