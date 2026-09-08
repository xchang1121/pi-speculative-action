import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { serialize } from "node:v8";
import { launchClosedSearchWorker } from "../dist/closed-search-process.mjs";
import { createClosedSearchProfile, readClosedSearchInput } from "../dist/pi-tool-invocation.js";

// Fixed-command qualification, not a plugin provider or an arbitrary-script sandbox.
// Uses pure module bytes from explicit setup, never the CLI-bearing npm package.
const moduleFile = process.argv[2];
assert.ok(moduleFile, "Usage: node bench/portable-kernel.mjs <installed closed-search.wasm> [--pi-tools]");
const started = performance.now(), worker = await prepareWorker(true);
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
	const rejectedModules = await fs.mkdtemp(path.join(os.tmpdir(), "pi-search-module-"));
	try {
		for (const [file, bytes, reason] of [["wrong", 4, /Requalify the search module/], ["large", 4 * 1024 * 1024 + 1, /search module byte budget/],
			...(process.platform === "linux" ? [["fifo", undefined, /search module must be a regular file/]] : [])]) {
			const target = path.join(rejectedModules, file);
			if (bytes === undefined) execFileSync("mkfifo", [target]); else await fs.writeFile(target, Buffer.alloc(bytes));
			const denied = launchClosedSearchWorker(target);
			try { await assert.rejects(denied.ready, reason); assert.ok(denied.closed()); }
			finally { await denied.dispose(); }
		}
	} finally { assert.equal(path.dirname(rejectedModules), path.resolve(os.tmpdir())); await fs.rm(rejectedModules, { recursive: true, force: true }); }
	const pi = {};
	if (process.argv.includes("--pi-tools")) for (const name of ["grep", "find"]) pi[name] = await qualifyPiSearch(name);
	const extension = process.argv.includes("--pi-tools") ? await qualifySearchExtension() : undefined;
	const cancellation = [];
	for (const mode of ["abort", "deadline", "input abort", "input deadline"]) {
		const interrupted = await prepareWorker(!mode.startsWith("input")), controller = new AbortController();
		const late = Promise.withResolvers(), inputCompleted = Promise.withResolvers(), inputWait = mode.startsWith("input");
		try {
			const arrived = performance.now(); let entered = false;
			const reason = inputWait ? 0 : new Error("cancelled running guest");
			const abort = () => { entered = true; if (mode.endsWith("abort")) controller.abort(reason); };
			await assert.rejects(interrupted.request(inputWait ? { kind: "grep", root: process.cwd(), args: { pattern: "needle" } } : { kind: "spin" }, {
				signal: controller.signal, timeoutMs: mode.endsWith("deadline") ? 200 : 5000,
				onStarted: () => { if (!inputWait) abort(); },
				onInput: async () => {
					abort(); await late.promise; inputCompleted.resolve();
					if (mode.endsWith("abort")) throw new Error("late input failure");
					return { directory: true, size: 0 };
				},
			}), mode.endsWith("abort") ? (error) => error === reason : /deadline/);
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
		pipeline, search: { ...search, stdout: undefined, matches }, pi, extension,
		admission: "explicit portable search and TUI integration; not arbitrary Bash, native search equivalence, macOS or ThinkThread Runtime qualification" }, null, 2));
} finally { await worker.dispose(); }

async function prepareWorker(qualification = false) {
	const worker = launchClosedSearchWorker(moduleFile, qualification ? new URL("./portable-worker.mjs", import.meta.url) : undefined);
	try { return { ...worker, ...await worker.ready }; }
	catch (error) { await worker.dispose(); throw error; }
}

/** Full original Pi tool in independent Actor/producer workers; Runtime owns admission and adoption. */
async function qualifyPiSearch(name) {
	const { createGrepToolDefinition, createFindToolDefinition } = await import("@earendil-works/pi-coding-agent");
	const { createFauxCore, fauxAssistantMessage, fauxToolCall } = await import("@earendil-works/pi-ai");
	const { createSpeculativeActionHost } = await import("../dist/agent-integration.js");
	const { PI_ACTION_SEMANTICS } = await import("../dist/action-semantics.js");
	const { createResourceSnapshotExecutionWorld } = await import("../dist/agent-execution-world.js");
	const { captureResourceVersion } = await import("../dist/resource-version.js");
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-portable-profile-")), searchRoot = path.join(root, "search");
	const { pool, profile, invocations } = await createClosedSearchProfile(root, moduleFile), bound = invocations.get(name);
	const counts = { producer: 0, actor: 0, contextReads: 0 }, reads = new Set();
	let inputRequests = 0, inputBytes = 0;
	const execute = async (role, source, request, checkpoint) => {
		counts[role]++;
		let checkpointReached = false;
		const inputs = source === fs ? await captureResourceVersion(undefined, root, semantics, profile.limits.inputBytes) : undefined;
		try {
			const output = await pool.request(role, { kind: name, root, args: request.args },
				{ signal: request.signal, onInput: async (operation, target) => {
					inputRequests++;
					if (operation === "readFile") reads.add(target);
					const value = await readClosedSearchInput(inputs?.view ?? source, root, operation, target, profile.limits.inputBytes);
					inputBytes += serialize(value).byteLength;
					if (checkpoint && !checkpointReached && operation === "readFile" && target === "/workspace/.gitignore") {
						checkpointReached = true; await checkpoint();
					}
					return value;
				} });
			counts.contextReads += output.contextReads;
			return { result: output.result, isError: output.isError };
		} finally { inputs?.release(); }
	};
	const tool = ({ grep: createGrepToolDefinition, find: createFindToolDefinition })[name](root);
	const semantics = PI_ACTION_SEMANTICS;
	const resources = createResourceSnapshotExecutionWorld(semantics, { tools: [name], maxBytes: () => profile.limits.inputBytes });
	const model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
	const signal = new AbortController().signal, journeys = [];
	const args = { pattern: name === "grep" ? "needle" : "*.txt", path: "search", ...(name === "grep" ? { context: 1 } : {}), limit: 1000 };
	function journey(checkpoint, capacity = 1) {
		const candidate = Promise.withResolvers(), authorized = Promise.withResolvers();
		let prediction = true, turnID, actorWaiting = false, actorCalls = 0, feedback;
		const invocation = { ...bound,
			filesystem: async (view, request) => {
				if (checkpoint) return execute("producer", view, request, checkpoint);
				counts.producer++; return bound.filesystem(view, request);
			} };
		const host = createSpeculativeActionHost("portable-" + journeys.length, {
			cwd: root, getSettings: () => ({ enabled: true, drafterEnabled: prediction, drafterGateEnabled: false,
				drafterMaxDepth: 0, candidateLimit: 1, maxConcurrentActions: capacity, tools: prediction ? [name] : [],
				patternAware: { enabled: false }, selfSpeculation: { enabled: false } }),
			draftModel: model, complete: async () => fauxAssistantMessage(fauxToolCall(name, args), { stopReason: "toolUse" }),
				resolveInvocation: () => invocation,
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
				const output = await host.execute({ turnID, id, tool: name, args: query, tools: [tool] }, signal, async (operation) => {
					assert.deepEqual(operation.invocation?.identity, bound.identity, "Actor execution must retain the selected executor independently of K(a)");
					actorCalls++; counts.actor++;
					return (await operation.invocation.authoritative({ args: operation.input, signal: operation.signal, callID: id })).result;
				});
				actorWaiting = false;
				return { output, totalMs: performance.now() - arrived, settlement: await bounded(feedback.promise, "Actor settlement") };
			},
		};
	}
	try {
		await assert.rejects(pool.request("actor", { kind: "kernel", commands: [] }), /closed search operation denied/);
		await fs.mkdir(path.join(root, ".git")); await fs.mkdir(searchRoot); await fs.mkdir(path.join(searchRoot, "empty"));
		await fs.writeFile(path.join(root, ".git/HEAD"), "ref: refs/heads/main\n");
		await fs.writeFile(path.join(root, ".gitignore"), "ignored.*\n");
		await fs.writeFile(path.join(root, "outside.txt"), "needle outside\n");
		await fs.writeFile(path.join(searchRoot, "ignored.bin"), Buffer.alloc(16 * 1024 * 1024, "x"));
		await fs.writeFile(path.join(searchRoot, "ignored.txt"), "needle ignored\n");
		await fs.writeFile(path.join(searchRoot, "notes.txt"), "before\nneedle\nafter\n");
		for (let index = 0; index < 16; index++) await fs.writeFile(path.join(searchRoot, "data-" + index + ".txt"), "no match\n".repeat(8192) + "needle " + index + "\n");
		await fs.mkdir(path.join(searchRoot, "nested"));
		for (const [file, content] of Object.entries({ "nested/.gitignore": "*.txt\n!kept.txt\n", "nested/kept.txt": "needle kept\n",
			"nested/skipped.txt": "needle skipped\n", "UPPER.TXT": "needle case\n", "中文 name.txt": "needle utf8\n",
			"utf16.txt": Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from("needle unicode\r\n", "utf16le")]) })) await fs.writeFile(path.join(searchRoot, file), content);
		const behaviors = [];
		for (const pattern of name === "find" ? ["empty", "nested/**/*.txt", "*.TXT", "absent", "*.txt"] : ["unicode", "needle", "absent"]) {
			const query = { ...args, pattern, limit: 2 };
			const output = (await execute("actor", fs, { args: query, signal })).result;
			assert.deepEqual((await execute("producer", fs, { args: query, signal })).result, output);
			const native = await tool.execute("native-case", query, signal);
			const equal = JSON.stringify(output) === JSON.stringify(native);
			behaviors.push({ pattern, nativeOutputEqual: equal, ...(!equal ? { native: native.content, profile: output.content } : {}) });
			if (name === "find" && pattern === "empty") assert.equal(output.content[0].text, "empty/");
			if (pattern === "nested/**/*.txt") assert.equal(output.content[0].text, "nested/kept.txt");
			if (pattern === "*.TXT") assert.equal(output.content[0].text, "UPPER.TXT");
			if (pattern === "absent") assert.equal(output.content[0].text, name === "find" ? "No files found matching pattern" : "No matches found");
		}
		const external = await fs.mkdtemp(path.join(os.tmpdir(), "pi-portable-external-")), link = path.join(searchRoot, "alias");
		try {
			await fs.writeFile(path.join(external, "secret.txt"), "needle host-only\n");
			for (const target of [path.join(searchRoot, "nested"), external]) {
				await fs.symlink(target, link, process.platform === "win32" ? "junction" : "dir");
				try {
					for (const role of ["actor", "producer"]) {
						const result = execute(role, fs, { args: { ...args, path: "search/alias" }, signal });
						if (target === external) await assert.rejects(result, /resource_symlink_escapes_workspace/);
						else assert.match((await result).result.content[0].text, /kept\.txt/);
					}
				} finally { await fs.rm(link, { force: true }); }
			}
		} finally { assert.equal(path.dirname(external), path.resolve(os.tmpdir())); await fs.rm(external, { recursive: true, force: true }); }
		if (process.platform === "linux") {
			const fifo = path.join(searchRoot, ".gitignore"); execFileSync("mkfifo", [fifo]);
			try {
				for (const role of ["actor", "producer"]) await assert.rejects(execute(role, fs, { args, signal }), /file size is not proven by retained content/);
				assert.ok(!reads.has("/workspace/search/.gitignore"), "FIFO must be refused before content access, not by a worker timeout");
			}
			finally { await fs.rm(fifo); }
		}
		inputRequests = 0; inputBytes = 0; reads.clear();
		const sample = async (execute) => {
			const times = []; let output;
			for (let index = 0; index < 3; index++) { const started = performance.now(); output = await execute(); times.push(performance.now() - started); }
			return { output, medianMs: times.sort((a, b) => a - b)[1] };
		};
		const baseline = await sample(async () => execute("actor", fs, { args, signal }));
		const inputTransport = { meanRequests: inputRequests / 3, meanPayloadBytes: inputBytes / 3, ignoredBytes: 16 * 1024 * 1024 };
		assert.ok(!reads.has("/workspace/search/ignored.bin") && !reads.has("/workspace/search/ignored.txt"), "the broker transferred ignored content");
		if (name === "grep") await assert.rejects(execute("actor", fs, { args: { ...args, path: "search/ignored.bin" }, signal }), /input byte budget/);
		await assert.rejects(pool.request("actor", { kind: name, root, args }, {
			onInput: () => { throw new Error("resource_access_unproven"); },
		}), /resource_access_unproven/, "a guest must not turn missing authority into an empty successful search");
		const native = await sample(() => tool.execute("native", args, signal)), expected = baseline.output.result;
		const both = Promise.withResolvers(), proceed = Promise.withResolvers(); let entered = 0;
		const concurrent = [0, 1].map(() => execute("producer", fs, { args, signal }, async () => {
			if (++entered === 2) both.resolve(); await proceed.promise;
		}));
		try { await bounded(Promise.race([both.promise, Promise.all(concurrent)]), "concurrent producer barrier"); }
		finally { proceed.resolve(); }
		assert.equal(entered, 2); for (const output of await Promise.all(concurrent)) assert.deepEqual(output.result, expected);
		const ready = journey(); await ready.start("produce");
		const completed = await bounded(ready.candidate, "completed candidate"); assert.equal(completed.status, "succeeded", JSON.stringify(completed));
		await ready.start("recall", false);
		const beforeHit = counts.producer, adopted = await ready.actor("ready");
		assert.deepEqual(adopted.output, expected); assert.equal(adopted.settlement.provider.kind, "speculative");
		assert.equal(counts.producer, beforeHit, "completed adoption executed the search again");
		const queries = [{ ...args, pattern: name === "grep" ? "after" : "data-0.txt", limit: 1 }, { ...args, pattern: "absent" }, { ...args, path: "." }];
		for (const query of queries) {
			const actor = await execute("actor", fs, { args: query, signal });
			const reconstructed = await ready.actor("retained-" + queries.indexOf(query), query);
			assert.deepEqual(reconstructed.output, actor.result);
			assert.equal(reconstructed.settlement.provider.kind, query.path === "." ? "actor" : "speculative");
			if (query.path !== ".") assert.equal(reconstructed.settlement.provider.match?.projector, "resource.inputs");
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
		await fs.appendFile(path.join(root, ".gitignore"), "data-*.txt\nnotes.txt\n中文*\nutf16.txt\nUPPER.TXT\nnested/\n");
		const stale = await ready.actor("stale");
		assert.equal(stale.settlement.provider.kind, "actor"); assert.equal(ready.actorCalls(), 2);
		assert.notDeepEqual(stale.output, expected);
		const beforeReplay = counts.producer;
		await ready.start("observed", false);
		const observed = await ready.actor("observed");
		assert.deepEqual(observed.output, stale.output);
		if (!resources.observation.capabilities.length) assert.equal(observed.settlement.provider.kind, "actor", "unproven host observations cannot be promoted");
		else if (observed.settlement.provider.kind === "actor") {
			const gate = observed.settlement.rejections.find((rejection) => rejection.cause.code === "candidate_join_not_profitable");
			assert.ok(gate, JSON.stringify(observed.settlement)); assert.ok(JSON.parse(gate.cause.detail).expectedNetBenefitMs < 0);
		}
		assert.equal(counts.producer, beforeReplay, "completed result adoption must not execute guest code");
		assert.equal(ready.actorCalls(), observed.settlement.provider.kind === "actor" ? 3 : 2);
		const changed = journey(() => fs.appendFile(path.join(root, ".gitignore"), "# changed during search\n"));
		await changed.start("changing");
		const failed = await bounded(changed.candidate, "changed search");
		assert.equal(failed.status, "failed"); assert.match(JSON.stringify(failed.cause), /resource_fingerprint_changed/);
		assert.deepEqual((await changed.actor("changed")).output, stale.output); assert.equal(changed.actorCalls(), 1);
		const paused = Promise.withResolvers(), released = Promise.withResolvers();
		const cancelled = journey(async () => { paused.resolve(); await released.promise; }, 2);
		const disabled = { enabled: false, resourceCacheMaxEntries: 32, predictionTimeoutMs: 5000, tools: [name] };
		await cancelled.start("cancelled");
		try {
			assert.equal(await bounded(Promise.race([paused.promise, cancelled.candidate]), "independent Actor checkpoint"), undefined);
			const different = { ...args, pattern: "absent" };
			const direct = await execute("actor", fs, { args: different, signal });
			const fallback = await cancelled.actor("independent", different);
			assert.deepEqual(fallback.output, direct.result); assert.equal(fallback.settlement.provider.kind, "actor");
			assert.equal(await Promise.race([cancelled.candidate, Promise.resolve("still running")]), "still running");
			await cancelled.host.runtime.settingsChanged(disabled);
		} finally { released.resolve(); }
		assert.equal((await bounded(cancelled.candidate, "cancelled candidate")).status, "cancelled");
		await cancelled.host.runtime.settingsChanged({ ...disabled, enabled: true });
		await cancelled.start("recovery", false);
		assert.deepEqual((await cancelled.actor("recovery")).output, stale.output); assert.equal(cancelled.actorCalls(), 2);
		assert.equal(counts.contextReads > 0, name === "grep", "the original Pi context reread was not exercised");
		const arrivals = { actor: Promise.withResolvers(), producer: Promise.withResolvers() }, drain = Promise.withResolvers();
		const actor = execute("actor", fs, { args, signal }, async () => { arrivals.actor.resolve(); await drain.promise; });
		const producer = execute("producer", fs, { args, signal }, async () => { arrivals.producer.resolve(); await drain.promise; });
		const rejected = assert.rejects(producer, /worker disposed/);
		try {
			await bounded(Promise.all(Object.values(arrivals).map((arrival) => arrival.promise)), "retirement barrier");
			const retiring = pool.dispose(); assert.equal(pool.dispose(), retiring);
			await rejected;
			assert.equal(await Promise.race([retiring, Promise.resolve("pending")]), "pending", "retirement must drain admitted Actors");
			drain.resolve(); assert.deepEqual((await actor).result, stale.output); await retiring;
			await assert.rejects(pool.request("actor", { kind: name, root, args }), /search pool retired/);
		} finally { drain.resolve(); await Promise.allSettled([actor, rejected]); }
		return { nativeActorMs: native.medianMs, profileWarmActorMs: baseline.medianMs, behaviors, rejectedEscapingLinks: true,
			specialFileGate: process.platform === "linux" ? "FIFO rejected before open" : "not run: FIFO unavailable", inputTransport, speculativeMs: completed.executionMs,
			readyAdoptionMs: adopted.totalMs, calibratedReplay: { totalMs: observed.totalMs, ...observed.settlement }, retainedInputQueries: queries.length - 1, uncapturedInputFallbacks: 1, ...counts,
			crossTurnResultReuse: true, runningRuntimeJoin: true, runningActorCalls: running.actorCalls(),
			staleActorExecutions: stale.settlement.provider.kind === "actor" ? 1 : 0, changedDuringSearchRejected: true, cancelledFullToolDiscarded: true,
			actorRanWhileProducerPaused: true, concurrentProducers: true, retirementDrainsActorOnly: true, ignoredContentNotTransferred: true, recoveryActorCalls: 1,
			scope: "Runtime-owned explicit common-profile full-tool IPC; not native equivalence" };
	} finally {
		await Promise.all(journeys.map((host) => host.dispose())); await pool.dispose();
		assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true });
	}
}

/** Real extension callbacks, persistent settings, workers and Runtime; only model/UI input is scripted. */
async function qualifySearchExtension() {
	const { createSpeculativeActionExtension } = await import("../dist/extension.js");
	const { createSpeculativeActionHost } = await import("../dist/agent-integration.js");
	const { createPiToolDefinitions } = await import("../dist/pi-tool-invocation.js");
	const { createFauxCore, fauxAssistantMessage, fauxToolCall } = await import("@earendil-works/pi-ai");
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-search-extension-")), cwd = path.join(root, "workspace"), agent = path.join(root, "agent");
	const previousAgent = process.env.PI_CODING_AGENT_DIR;
	process.env.PI_CODING_AGENT_DIR = agent;
	const handlers = new Map(), commands = new Map(), registered = createPiToolDefinitions(cwd), notices = [], results = {};
	let selected, candidate, settlement;
	const choices = new Map(Object.entries({
		"Speculative action": ["Tools & execution", "Enabled", "Apply changes", "Close"],
		"Tools & execution": ["Execution routes", "Back"], "Execution routes": ["Search execution", "Back"],
		"Search execution (grep/find)": ["Portable search"],
	}));
	const model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
	const context = { cwd, mode: "tui", hasUI: true, model, isProjectTrusted: () => true, getSystemPrompt: () => "qualification",
		sessionManager: { getSessionId: () => "closed-search", getSessionFile: () => undefined }, thinkingLevel: "off",
		modelRegistry: { getAvailable: () => [model], complete: async () => fauxAssistantMessage(fauxToolCall(...selected), { stopReason: "toolUse" }) },
		ui: { notify: (text) => notices.push(text), setStatus: () => {}, select: async (title, options) => {
			const next = choices.get(title)?.shift(); return options.find((option) => next && option.startsWith(next));
		} },
	};
	const emit = async (name, event = {}) => { for (const handler of handlers.get(name) ?? []) await handler(event, context); };
	const command = (args) => commands.get("speculative-action").handler(args, context);
	const invoke = (name, args) => registered.get(name).execute("actor", args, undefined, undefined, context);
	try {
		await fs.mkdir(cwd); await fs.mkdir(path.join(agent, "speculative-action"), { recursive: true });
		await fs.writeFile(path.join(cwd, "notes.txt"), "before\nneedle\nafter\n");
		await fs.writeFile(path.join(agent, "speculative-action.json"), JSON.stringify({ enabled: false,
			drafterGateEnabled: false, drafterMaxDepth: 0, candidateLimit: 1, maxConcurrentActions: 1, tools: ["grep", "find"],
			patternAware: { enabled: false }, selfSpeculation: { enabled: false } }));
		const module = path.join(agent, "speculative-action", "closed-search.wasm"); await fs.copyFile(moduleFile, module);
		await createSpeculativeActionExtension({ createHost: (sessionID, options) => createSpeculativeActionHost(sessionID, {
			...options, onEvent: (event) => { options.onEvent?.(event);
				if (event.type === "candidate" && event.candidate.origin === "prediction" && event.state.status !== "running") candidate?.resolve(event.state);
			}, onActorActionSettled: (value) => { options.onActorActionSettled?.(value); settlement?.resolve(value.settlement); },
		}) })({ on: (name, handler) => handlers.set(name, [...handlers.get(name) ?? [], handler]),
			registerCommand: (name, value) => commands.set(name, value), registerTool: (tool) => registered.set(tool.name, tool),
			getActiveTools: () => [...registered.keys()], getAllTools: () => [...registered.values()].map((tool) => ({ ...tool,
				sourceInfo: { path: `<builtin:${tool.name}>`, source: "builtin", scope: "temporary", origin: "top-level" } })),
		});
		await emit("session_start"); await command("");
		assert.match(notices.join("\n"), /settings applied/);
		for (const name of ["grep", "find"]) {
			const args = { pattern: name === "grep" ? "needle" : "notes.txt", path: "." }; selected = [name, args];
			await invoke(name, args); // Exclude worker startup from the warm Actor baseline.
			const baselineAt = performance.now(), expected = await invoke(name, args), warmActorMs = performance.now() - baselineAt;
			candidate = Promise.withResolvers(); settlement = Promise.withResolvers();
			await emit("context", { messages: [] });
			const completed = await bounded(candidate.promise, "extension candidate"); assert.equal(completed.status, "succeeded", JSON.stringify(completed));
			const arrived = performance.now(); assert.deepEqual(await invoke(name, args), expected); const hitMs = performance.now() - arrived;
			const feedback = await bounded(settlement.promise, "extension settlement");
			assert.equal(feedback.provider.kind, "speculative", JSON.stringify(feedback));
			results[name] = { warmActorMs, hitMs, actorArrivalSpeedup: warmActorMs / hitMs };
			await emit("agent_end");
		}
		await fs.rename(module, module + ".held"); await command("status");
		await assert.rejects(invoke(...selected), /setup:search/); // Cannot reuse an old identity or silently switch to native search.
		await fs.rename(module + ".held", module); await command("status"); await invoke(...selected);
		await command("off");
		const stock = createPiToolDefinitions(cwd).get(selected[0]);
		assert.deepEqual(await invoke(...selected), await stock.execute("native", selected[1], undefined, undefined, context));
		await emit("session_shutdown"); // The real store drains its publication queue at shutdown.
		assert.equal(JSON.parse(await fs.readFile(path.join(agent, "speculative-action.json"))).searchExecution, "closed");
		return { results, appliedThroughTui: true, missingSetupRejected: true, refreshRecovered: true, disabledNativeActor: true };
	} finally {
		try { await emit("session_shutdown"); }
		finally {
			if (previousAgent === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgent;
			assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true });
		}
	}
}

function bounded(promise, label) {
	let timer;
	return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label + " deadline")), 15_000); })])
		.finally(() => clearTimeout(timer));
}
