import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { createHash } from "node:crypto";
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
	for (const mode of ["abort", "deadline"]) {
		const interrupted = await prepareWorker(), controller = new AbortController();
		try {
			const arrived = performance.now(); let entered = false;
			await assert.rejects(interrupted.request({ kind: "spin", image: { files: {}, directories: [] } }, {
				signal: controller.signal, timeoutMs: mode === "deadline" ? 200 : 5000,
				onStarted: () => { entered = true; if (mode === "abort") controller.abort(new Error("cancelled running guest")); },
			}), mode === "abort" ? /cancelled running guest/ : /deadline/);
			assert.ok(interrupted.closed(), "Actor fallback must not race a still-running worker");
			assert.ok(entered, "cancellation must exercise an entered guest, not just process startup");
			cancellation.push({ mode, retirementMs: performance.now() - arrived });
		} finally { await interrupted.dispose(); }
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
		} else if (message.type === "result") pending.settle(message.error ? new Error(message.error) : undefined, message.result);
	});
	try {
		const { profile, engines } = await ready;
		return { profile, engines, preparationMs: performance.now() - started, closed: () => closed,
			dispose: async () => { if (!closed) terminate(new Error("worker disposed")); await closure; },
			request: (input, { signal, timeoutMs = 5000, onStarted, onCheckpoint } = {}) => new Promise((resolve, reject) => {
				if (signal?.aborted) { reject(signal.reason); return; }
				if (closed || pending) { reject(new Error("worker unavailable/busy")); return; }
				assert.ok(serialize(input).byteLength <= profile.limits.requestBytes, "request frame budget");
				assert.ok(Object.values(input.image.files).reduce((size, bytes) => size + bytes.length, 0) <= profile.limits.inputBytes, "input byte budget");
				const id = ++nextID, abort = () => terminate(signal.reason);
				const timer = setTimeout(() => terminate(new Error("worker execution deadline")), timeoutMs);
				pending = { id, onStarted, onCheckpoint, settle: (error, result) => {
					clearTimeout(timer); signal?.removeEventListener("abort", abort); pending = undefined;
					if (error) reject(error); else resolve(result);
				} };
				signal?.addEventListener("abort", abort, { once: true });
				child.send({ type: "request", id, input }, (error) => { if (error) terminate(error); });
			}),
		};
	} catch (error) { terminate(error); await closure; throw error; }
}

/** Full original Pi tool in a worker; evidence capture, validation, and adoption stay in the parent. */
async function qualifyPiSearch(worker) {
	const { createGrepToolDefinition } = await import("@earendil-works/pi-coding-agent");
	const { ActionSemanticsRegistry, PI_ACTION_SEMANTICS } = await import("../dist/action-semantics.js");
	const { RESOURCE_OBSERVATION_EFFECTS } = await import("../dist/effect-model.js");
	const { createResourceSnapshotExecutionWorld } = await import("../dist/agent-execution-world.js");
	const { ToolExecutionGateway } = await import("../dist/tool-execution-gateway.js");
	const { waitForCandidate } = await import("../dist/scheduler.js");
	const profile = worker.profile, fingerprint = createHash("sha256").update(JSON.stringify(profile)).digest("hex");
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-portable-profile-"));
	const virtual = (target) => path.posix.join("/workspace", path.relative(root, target).split(path.sep).join("/"));
	const collect = async (source, target = root, image = { files: {}, directories: [] }) => {
		if ((await source.stat(target)).isDirectory()) {
			image.directories.push(virtual(target));
			for (const name of await source.readdir(target)) await collect(source, path.join(target, name), image);
		} else image.files[virtual(target)] = await source.readFile(target);
		return image;
	};
	let executions = 0, contextReads = 0, afterSearch;
	const execute = async (image, request) => {
		executions++;
		const output = await worker.request({ kind: "grep", image, root, args: request.args, pause: !!afterSearch },
			{ signal: request.signal, onCheckpoint: afterSearch });
		contextReads += output.contextReads;
		return { result: output.result, isError: output.isError };
	};
	const tool = createGrepToolDefinition(root), definition = { ...PI_ACTION_SEMANTICS.definition("grep"), epoch: profile.id,
		effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "tree_content" };
	const semantics = new ActionSemanticsRegistry([definition]);
	const invocation = { executor: profile.id, identity: profile, filesystem: async (view, request) => execute(await collect(view), request) };
	const world = createResourceSnapshotExecutionWorld(semantics, { tools: ["grep"], maxBytes: () => profile.limits.inputBytes });
	const gateway = new ToolExecutionGateway([world]), signal = new AbortController().signal, cases = [];
	const args = { pattern: "needle", path: ".", context: 1, limit: 1000 };
	try {
		await fs.mkdir(path.join(root, ".git")); await fs.mkdir(path.join(root, "empty"));
		await fs.writeFile(path.join(root, ".git/HEAD"), "ref: refs/heads/main\n");
		await fs.writeFile(path.join(root, ".gitignore"), "ignored.txt\n");
		await fs.writeFile(path.join(root, "ignored.txt"), "needle ignored\n");
		await fs.writeFile(path.join(root, "notes.txt"), "before\nneedle\nafter\n");
		for (let index = 0; index < 16; index++) await fs.writeFile(path.join(root, "data-" + index + ".txt"), "no match\n".repeat(8192) + "needle " + index + "\n");
		const sample = async (execute) => {
			const times = []; let output;
			for (let index = 0; index < 3; index++) { const started = performance.now(); output = await execute(); times.push(performance.now() - started); }
			return { output, medianMs: times.sort((a, b) => a - b)[1] };
		};
		const baseline = await sample(async () => execute(await collect(fs), { args, callID: "profile-actor", signal }));
		const native = await sample(() => tool.execute("native", args, signal)), expected = baseline.output;
		const action = semantics.buildKey("grep", args, root, "", { fingerprint, context: invocation });
		const operation = { tool: "grep", input: args, action, callID: "profile-speculation", signal };
		const route = await gateway.resolve({ operation, ...definition }, { cwd: root }); assert.ok(route);
		const started = performance.now();
		const branch = await gateway.executeSpeculative(operation, route, () => ({ cwd: root, tool, toolName: "grep", args, action, callID: operation.callID, signal }));
		const speculativeMs = performance.now() - started, beforeHit = executions;
		try {
			const arrived = performance.now(); assert.equal((await branch.validate()).status, "valid");
			assert.deepEqual(await branch.commit(), expected);
			const readyAdoptionMs = performance.now() - arrived;
			assert.equal(executions, beforeHit, "adoption executed the search again");
			for (const query of [{ ...args, pattern: "after", limit: 1 }, { ...args, pattern: "absent" }, { ...args, glob: "*.txt", context: 0 }]) {
				const actor = await execute(await collect(fs), { args: query, callID: "other-query", signal });
				const key = semantics.buildKey("grep", query, root, "", { fingerprint, context: invocation });
				assert.equal((await branch.validate()).status, "valid");
				assert.deepEqual(await branch.reconstruct({ action: key, args: query, callID: "retained-inputs", signal }), actor);
				assert.equal((await branch.validate()).status, "valid"); cases.push(query);
			}
			const reached = Promise.withResolvers(), resume = Promise.withResolvers();
			afterSearch = async () => { reached.resolve(); await resume.promise; };
			const running = gateway.executeSpeculative(operation, route, () => ({ cwd: root, tool, toolName: "grep", args, action, callID: "running", signal }));
			let runningActorCalls = 0;
			try {
				assert.equal(await Promise.race([reached.promise, running]), undefined, "search must reach its context-read barrier");
				const joined = gateway.executeAuthoritative(operation, async () => { runningActorCalls++; throw new Error("unexpected Actor reexecution"); }, {
					reuse: async () => {
						const wait = await waitForCandidate(running, signal, 1000);
						assert.equal(wait.status, "completed"); assert.equal((await wait.value.validate()).status, "valid");
						return wait.value.commit();
					},
				});
				resume.resolve(); assert.deepEqual(await joined, expected); assert.equal(runningActorCalls, 0);
			} finally { resume.resolve(); afterSearch = undefined; await running.then((branch) => branch.dispose(), () => {}); }
			await fs.writeFile(path.join(root, "notes.txt"), "changed\n");
			assert.equal((await branch.validate()).status, "stale");
			let fallbacks = 0;
			const stale = await gateway.executeAuthoritative(operation, async () => {
				fallbacks++; return execute(await collect(fs), { args, callID: "stale-actor", signal });
			}, { reuse: async () => (await branch.validate()).status === "valid" ? branch.commit() : undefined });
			assert.equal(fallbacks, 1); assert.notDeepEqual(stale, expected);
			afterSearch = () => fs.writeFile(path.join(root, "notes.txt"), "changed during search\n");
			await assert.rejects(gateway.executeSpeculative(operation, route, () => ({ cwd: root, tool, toolName: "grep", args, action, callID: "changing", signal })), /resource_observation_window_changed/);
			const cancellation = new AbortController();
			afterSearch = () => cancellation.abort(new Error("cancelled before Pi context reread"));
			const abandoned = gateway.executeSpeculative({ ...operation, signal: cancellation.signal }, route,
				() => ({ cwd: root, tool, toolName: "grep", args, action, callID: "cancelled", signal: cancellation.signal }));
			await assert.rejects(abandoned, /cancelled before Pi context reread/);
			assert.ok(worker.closed(), "cancelled producer must be gone before Actor fallback");
			afterSearch = undefined;
			const recovered = await prepareWorker(); let recoveryActorCalls = 0;
			try {
				assert.deepEqual(recovered.profile, profile, "recovery must keep the same execution identity");
				const fresh = await gateway.executeAuthoritative(operation, async () => {
					recoveryActorCalls++;
					const output = await recovered.request({ kind: "grep", root, args, image: await collect(fs) });
					return { result: output.result, isError: output.isError };
				}, { reuse: () => abandoned.then((branch) => branch.commit()) });
				assert.deepEqual(fresh, stale); assert.equal(recoveryActorCalls, 1);
			} finally { await recovered.dispose(); }
			assert.ok(contextReads > 0, "the original Pi context reread was not exercised");
			return { nativeActorMs: native.medianMs, profileWarmActorMs: baseline.medianMs, speculativeMs, readyAdoptionMs,
				retainedInputQueries: cases.length, staleActorExecutions: fallbacks, contextReads, freshAdoption: true,
				changedInputRejected: true, changedDuringSearchRejected: true, runningGatewayJoin: true, runningActorCalls,
				cancelledFullToolDiscarded: true, recoveryActorCalls,
				scope: "explicit common-profile full-tool IPC; not native equivalence or production enablement" };
		} finally { await branch.dispose(); }
	} finally { await gateway.dispose(); assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true }); }
}
