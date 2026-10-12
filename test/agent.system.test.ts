import { link, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createEditTool, createFindTool, createGrepTool, createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { bindResourceOperation, createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { disposable, onExit, deferred, nextTurn } from "./async.ts";
import { commitBenchmarkFixture, createLinuxProcessBenchmark, textOutput } from "./linux-process-fixture.ts";

const directories = temporaryDirectories("pi-agent-system-");
afterEach(directories.dispose);

test.for(["actor", "preview", "dynamic", "dynamic-preview", "hints", "hints-preview", "retired", "retired-preview", "predicted", "stale", "binding",
	"calculation", "calculation-preview", "calculation-unrelated", "calculation-unrelated-preview", "calculation-hint", "calculation-hint-preview", "calculation-key",
	"seed-dynamic", "seed-dynamic-preview", "seed-calculation", "seed-calculation-preview", "seed-stale", "seed-binding", "seed-calculation-key", "seed-unbounded", "seed-repeat", "seed-revoke",
	"mixed-producer", "mixed-consumer", "mixed-stale", "mixed-failure", "mixed-budget", "mixed-outside", "mixed-revoke", "mixed-overlap", "mixed-cancel", "mixed-cancel-return", "mixed-unkeyed", "mixed-binding-unkeyed",
	...["read", "names"].flatMap(input => ["", "-preview", "-stale", "-copy", "-change", "-budget", "-outside", "-unkeyed"].map(suffix => `input-${input}${suffix}`)),
	"input-read-range", "input-read-cancel", "input-read-failure", "unregistered-dynamic", "unregistered-mixed-producer", "late-dynamic",
	...["dynamic", "dynamic-preview", "calculation", "calculation-preview", "calculation-unrelated", "calculation-key", "stale", "binding", "retired", "unbounded", "repeat", "revoke", "rejected-capture", "rejected-branch", "rejected-output",
		"mixed-producer", "mixed-consumer", "mixed-stale", "mixed-failure", "mixed-budget", "mixed-outside", "mixed-revoke", "mixed-overlap", "mixed-cancel", "mixed-cancel-return", "mixed-unkeyed",
		...["read", "names"].flatMap(input => ["", "-stale", "-copy", "-change", "-budget", "-outside", "-unkeyed"].map(suffix => `input-${input}${suffix}`)),
		"input-read-range", "input-read-cancel", "input-read-failure"].map(mode => `unkeyed-${mode}`),
	...["large", "opaque"].flatMap(output => ["seed-dynamic", "seed-calculation", "mixed-producer", "unkeyed-dynamic", "unkeyed-calculation", "unkeyed-mixed-producer", "input-read", "input-names",
		"unkeyed-input-read-stale", "unkeyed-input-names-stale", "unkeyed-mixed-failure", "unkeyed-mixed-budget", "unkeyed-mixed-revoke", "unkeyed-revoke", "unkeyed-calculation-unrelated"].map(mode => `${output}-${mode}`)),
	...["after", "after-cancel", "consume", "consume-cancel", "consume-cancel-return", "before"].flatMap(failure => ["seed-dynamic", "seed-calculation", "mixed-producer", "input-read", "input-names", "unkeyed-dynamic", "unkeyed-calculation",
		"unkeyed-input-read-stale", "unkeyed-input-names-stale", "unkeyed-mixed-stale", "unkeyed-mixed-failure", "unkeyed-mixed-cancel-return", "unkeyed-mixed-budget", "unkeyed-mixed-revoke", "unkeyed-revoke"].map(mode => `${failure}:${mode}`))])("shares actual inputs across independently formatted tools (%s)", async scenario => {
	const parentFailure = scenario.includes(":") ? scenario.split(":")[0] : undefined, early = parentFailure === "before";
	scenario = scenario.replace(/^.*:/, "");
	const parentOutput = scenario.startsWith("large-") ? { transcript: "x".repeat(128 * 1024) } : scenario.startsWith("opaque-") ? { callback: () => "parent-only" } : {};
	scenario = scenario.replace(/^(large|opaque)-/, "");
	const mode = scenario.replace(/^(seed|unkeyed|unregistered|late)-/, ""), direct = mode.startsWith("input-"), listing = mode.startsWith("input-names"), changes = mode.endsWith("change"), range = mode.endsWith("range");
	const unkeyed = /^(unkeyed|unregistered|late)-/.test(scenario), unknown = scenario.startsWith("unregistered-");
	const seeded = unkeyed || scenario.startsWith("seed-") || direct && !mode.endsWith("preview") || ["mixed-producer", "mixed-budget", "mixed-outside"].includes(mode), bound = seeded || direct || mode.startsWith("mixed");
	const cwd = await directories.create(), file = path.join(mode.endsWith("outside") ? await directories.create() : cwd, "input.txt"), shared = {};
	await writeFile(file, "one\ntwo\n"); if (listing) await writeFile(path.join(path.dirname(file), "second.txt"), "");
	let builds = 0, renders = 0, disposed = 0, effects = 0, borrowers = 0, bindings = 0, revoke: (() => void | Promise<void>) | undefined;
	const pure = mode.startsWith("calculation"), unrelated = mode.includes("unrelated") || mode.startsWith("calculation-hint");
	const stale = mode.endsWith("stale"), cancelled = mode.endsWith("cancel"), overlap = mode === "mixed-cancel" || mode === "mixed-overlap", failed = mode.endsWith("failure");
	const unavailable = early || mode.endsWith("budget") || mode.endsWith("outside") || mode.startsWith("rejected"), mutates = (index: number) => (direct || mode.startsWith("mixed")) && index === Number(mode !== "mixed-producer");
	const entered = deferred(), joined = deferred(), release = deferred(), controller = new AbortController(), actors: Promise<unknown>[] = [];
	const producer = new AbortController(), parentError = new Error("producer failed after independent work");
	const names = ["count_records", "summarize_records", "other_records"], read = PI_ACTION_SEMANTICS.definition("read")!;
	const definitions = names.map((tool, index) => ({ ...read, ...(mutates(index) ? PI_ACTION_SEMANTICS.definition("write") :
		mode === "unbounded" && !index ? PI_ACTION_SEMANTICS.definition("bash") : {}), tool, epoch: `custom:${tool}`, projectors: [],
		resourceScope: mutates(index) || mode === "unbounded" && !index ? undefined : "captured_inputs" as const,
		canonicalize: (input: unknown, cwd: string) => { const key = !(index ? mode.endsWith("unkeyed") : unkeyed) && read.canonicalize(input, cwd); return key ? { ...key,
			resources: pure || index && mode.startsWith("dynamic") ? [] : index && mode.startsWith("hints") ? ["selector"] : key.resources } : undefined; } }));
	const actionSemantics = new ActionSemanticsRegistry(unknown ? definitions.slice(1) : definitions), settings = { drafterEnabled: false, resourceCacheMaxBytes: 32 * 1024 };
	const render = (tool: string, count: number) => { renders++; return { content: [{ type: "text" as const, text: `${tool}:${count}` }], details: { tool, ...(tool === names[0] ? parentOutput : {}) } }; };
	const count = async (read: () => Promise<Buffer | string>) => { const text = pure ? "one\ntwo\n" : await read(); builds++; return text.toString().trim().split("\n").length; };
	const tools = names.map(name => ({ ...createReadTool(cwd), name, execute: async () => render(name, await count(() => readFile(file))) }));
	const invocations = new Map(names.map((tool, index) => {
		const execute: NonNullable<import("../src/tool-settlement.ts").ToolInvocation["filesystem"]> = async (view, request) => {
			const fail = (phase: string) => { if (!index && parentFailure?.startsWith(phase)) { if (parentFailure.includes("cancel")) producer.abort(parentError); if (!parentFailure.endsWith("return")) throw parentError; } }; fail("before");
			const build = async (inputs: typeof view) => ({ value: await count(() => inputs.readFile(file)), bytes: 8, dispose: () => { disposed++; } });
			if (mode === "repeat" && !index) await view.prepare!(shared, "lines:v1", build, async value => value, file, request.signal);
			const consume = async (count: number) => {
				if (mutates(index) && overlap) { if (++borrowers === 1) entered.resolve(); else joined.resolve(); await release.promise; request.signal.throwIfAborted(); }
				if (mutates(index) && mode === "mixed-revoke" || !index && mode === "revoke") { await revoke?.(); expect(disposed).toBe(0); }
				if (mutates(index)) { effects++; await view.writeFile!(path.join(cwd, `${tool}.txt`), String(count)); if (failed) throw new Error("consumer failed after writing"); }
				const result = render(tool, count); if (index && mode === "mixed-cancel-return") controller.abort(); fail("consume"); return result;
			};
			const read = async () => {
				const load = () => listing ? view.readdir!(path.dirname(file)) : view.readFile(file, index && range ? 4 : undefined);
				const pending = load(); if (index && cancelled) controller.abort(new Error("consumer cancelled"));
				let value = await pending;
				if (index && mode.endsWith("copy")) { if (Array.isArray(value)) value.push("changed"); else value.fill(0); value = await load(); }
				if (index && changes) { effects++; await view.writeFile!(listing ? path.join(path.dirname(file), "third.txt") : file, "one\ntwo\nthree\n"); value = await load(); }
				return Array.isArray(value) ? value.join("\n") : value;
			};
			const output = { isError: false, result: direct ? await consume(await count(read)) : await view.prepare!(mode.includes("binding") && index || index === 2 ? definitions[index]! : shared,
				mode === "calculation-key" && index ? "next" : "lines:v1", build, consume, mode.startsWith("calculation-hint") ? path.join(cwd, `hint-${index}`) : pure ? undefined : file, request.signal) };
			fail("after"); return output;
		};
		const binding = bound ? bindResourceOperation(execute, cwd) : { filesystem: execute, filesystemRoot: cwd };
		return [tool, { executor: `custom:${tool}`, identity: { tool }, semantics: unknown && !index ? undefined : definitions[index], ...binding,
			...(binding.captureInputs ? { captureInputs: (...args: Parameters<NonNullable<typeof binding.captureInputs>>) => { const capture = binding.captureInputs!(...args); revoke = capture.dispose;
				return { ...capture, seal: async () => { const branch = await capture.seal();
					expect(branch).not.toHaveProperty("output"); await expect(branch.commit()).rejects.toMatchObject({ disposition: "recoverable" }); return branch;
				} }; } } : {}) }];
	}));
	const world = createResourceSnapshotExecutionWorld(actionSemantics, { maxBytes: () => mode.endsWith("budget") ? 64 : 1024 * 1024 });
	if (mode.startsWith("rejected")) { const capture = world.observation!.capture;
		Object.assign(world.observation!, { capture: async (context: Parameters<typeof capture>[0]) => {
			const result = await capture(context);
			return mode === "rejected-capture" ? { ...result, inputsOnly: undefined } : { ...result, seal: async (output: Parameters<typeof result.seal>[0]) => ({ ...await result.seal(output), ...(mode === "rejected-output" ? { output } : { inputsOnly: undefined }) }) };
		} }); }
	const f = systemHost(cwd, tools, { actionSemantics, resolveInvocation: tool => { if (tool === names[0]) bindings++; return invocations.get(tool); },
		complete: async () => fauxAssistantMessage(fauxToolCall("speculative_workflow", { steps: [{ tool: names[unrelated ? 2 : 1], input: { path: "input.txt" } }] }), { stopReason: "toolUse" }),
		executionWorlds: [world] }, settings);
	await using _cleanup = onExit(async () => { controller.abort(); release.resolve(); await Promise.allSettled(actors); await f.dispose(); expect(disposed).toBe(direct ? 0 : builds - (bound ? 0 : f.fallbacks.length)); });
	const late = scenario.startsWith("late-") ? tools.shift() : undefined; await f.start(); if (late) tools.unshift(late);
	if (parentFailure && !parentFailure.endsWith("return")) await expect(f.execute(names[0]!, { path: "input.txt" }, producer.signal)).rejects.toBe(parentError);
	else if (seeded) expect(await f.execute(names[0]!, { path: "input.txt" }, producer.signal)).toEqual({ content: [{ type: "text", text: "count_records:2" }], details: { tool: names[0], ...parentOutput } });
	else { await f.preview(names[0]!, { path: "input.txt" }); await f.ready(); }
	if (unkeyed) { expect(bindings).toBe(1); expect(f.events.find(event => event.type === "actor_action")).toMatchObject({ settlement: { provider: { kind: "actor", cause: { code: "action_not_keyable" } } } }); }
	expect({ builds, renders }).toEqual({ builds: Number(!early), renders: Number(!early) });
	if (parentFailure) expect(f.events.find(event => event.type === "actor_action")).toMatchObject({ settlement: { provider: { kind: "actor", isError: !parentFailure.endsWith("return") } }, computation: { hiddenComputeMs: 0 } });
	if (mode.startsWith("retired") || mode === "predicted") { await f.finish(); tools.shift(); settings.drafterEnabled = mode === "predicted"; await f.start("consumer"); }
	if (mode === "predicted") await f.ready(2);
	if (unrelated) { await f.finish(); settings.drafterEnabled = true; await f.start("unrelated"); await f.ready(seeded ? 1 : 2); await f.finish(); settings.drafterEnabled = false; await f.start("consumer"); }
	if (mode.endsWith("preview") && !direct) { await f.preview(names[1]!, { path: "input.txt" }); await expect.poll(() => renders).toBe(unrelated ? 3 : 2); }
	if (stale) await writeFile(listing ? path.join(path.dirname(file), "third.txt") : file, "one\ntwo\nthree\n");
	const result = f.execute(names[1]!, { path: "input.txt" }, controller.signal); actors.push(result); void result.catch(() => {});
	if (overlap) {
		await entered.promise; actors.push(f.execute(names[1]!, { path: "input.txt" })); await joined.promise;
		expect(disposed).toBe(0); if (cancelled) controller.abort(new Error("consumer cancelled")); release.resolve();
		expect(textOutput(await actors[1] as Awaited<typeof result>)).toBe("summarize_records:2");
	}
	if (failed || cancelled) await expect(result).rejects.toThrow(failed ? "consumer failed after writing" : "consumer cancelled");
	else expect(await result).toEqual({ content: [{ type: "text", text: `summarize_records:${stale || changes ? 3 : range ? 1 : 2}` }], details: { tool: names[1] } });
	expect({ builds, renders, effects }).toEqual({ builds: (direct || stale || unavailable || mode.includes("binding") || mode === "calculation-key" || mode === "revoke" ? 2 : 1) + Number(unrelated) - Number(direct && cancelled) - Number(early),
		renders: 2 + Number(mode === "stale") + Number(unrelated) + Number(overlap) - Number(failed || cancelled) - Number(early), effects: Number(direct || mode.startsWith("mixed")) + Number(changes) + Number(overlap && !cancelled) - Number(direct && cancelled) - Number(early && mutates(0)) });
	expect(f.fallbacks).toEqual([...(seeded ? [names[0]!] : []), ...(early || mutates(1) || mode === "revoke" || mode.startsWith("rejected") ? Array(1 + Number(overlap)).fill(names[1]!) : [])]);
	if (direct && cancelled) await expect(readFile(path.join(cwd, `${names[1]}.txt`))).rejects.toMatchObject({ code: "ENOENT" });
	else if ((direct || mode.startsWith("mixed")) && !(early && mutates(0))) expect(await readFile(path.join(cwd, `${names[Number(mode !== "mixed-producer")]}.txt`), "utf8")).toBe(String(stale || changes ? 3 : range ? 1 : 2));
	await f.finish();
	const timing = f.events.filter(event => event.type === "actor_action").at(-1)?.computation;
	if (mode !== "binding") expect(timing?.hiddenComputeMs! > 0, JSON.stringify({ timing, events: f.events })).toBe(!stale && !unavailable && (!failed || direct) && !(direct && cancelled) && !["calculation-key", "revoke"].includes(mode));
	if (cancelled) expect(f.events.filter(event => event.type === "actor_action")[0]?.computation?.hiddenComputeMs).toBe(0);
});

test("learns from executed tool workflows, reopens the saved state and supplies the next read", async () => {
	const cwd = await directories.create(), state = path.join(cwd, "patterns.json");
	await writeFile(path.join(cwd, "first.txt"), "first\n"); await writeFile(path.join(cwd, "next.txt"), "next\n");
	const settings = patternAwareSettings({ enabled: true, minOccurrences: 2, maxContextLength: 1, maxFutureGap: 0, beamWidth: 1, presets: [] });
	const createStore = () => new PatternAwareStore(settings, state, patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd));
	const store = createStore(); await store.load();
	for (let round = 0; round < 4; round++) {
		await using f = disposable(systemHost(cwd, [createReadTool(cwd)], { patternStore: store }, { patternAware: settings }, `training-${round}`));
		await f.start(); await f.execute("read", { path: "first.txt" }); await f.finish();
		await f.start("next"); await f.execute("read", { path: "next.txt" }); await f.finish(true);
	}
	await store.flush();
	const reopened = createStore(); await reopened.load();
	const f = systemHost(cwd, [createReadTool(cwd)], { patternStore: reopened }, { patternAware: settings }, "learned");
	await using _cleanup = onExit(async () => { await f.dispose(); await reopened.flush(); });
	await f.start(); await f.execute("read", { path: "first.txt" }); await f.finish();
	await f.start("next");
	await expect.poll(() => f.events.some(event => event.type === "candidate" && event.candidate.source === "pattern_aware" &&
		event.state.status === "succeeded"), { timeout: 10_000 }).toBe(true);
	f.fallbacks.length = 0;
	expect(textOutput(await f.execute("read", { path: "next.txt" }))).toBe("next\n");
	expect(f.fallbacks).toEqual([]);
});

test.for([false, true])("reconstructs shared searches after cancellation and namespace changes (cancel all: %s)", async (cancelAll, { skip }) => {
	const cwd = await directories.create();
	await writeFile(path.join(cwd, "a.txt"), "one\ntwo\nthree\n");
	await writeFile(path.join(cwd, ".ignore"), "ignored.txt\n");
	await writeFile(path.join(cwd, "ignored.txt"), "two ignored\n");
	const profile = await createClosedSearchProfile(cwd);
	if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg unavailable"); }
	const entered = deferred(), joined = deferred(), release = deferred(), controllers = [new AbortController(), new AbortController()];
	let holding = false, borrowers = 0, builds = 0, shared: AbortSignal | undefined;
	const grep = profile.invocations.get("grep")!, actors: Promise<unknown>[] = [];
	const invocation: typeof grep = { ...grep, filesystem: (view, request) => grep.filesystem!({ ...view,
		prepare: (binding, key, build, consume, target, signal) => {
			if (holding && target === path.join(cwd, "a.txt") && ++borrowers === 2) joined.resolve();
			return view.prepare!(binding, key, async (inputs, signal) => {
				if (holding && target === path.join(cwd, "a.txt")) { builds++; shared = signal; entered.resolve(); await release.promise; }
				return build(inputs, signal);
			}, consume, target, signal);
		},
	}, request) };
	const tools = [createGrepTool(cwd), createFindTool(cwd), createLsTool(cwd), createReadTool(cwd)];
	const f = systemHost(cwd, tools, {
		resolveInvocation: (tool, input) => tool === "grep" ? invocation : profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
	});
	await using _cleanup = onExit(async () => { for (const controller of controllers) controller.abort(); release.resolve(); await Promise.allSettled(actors); await f.dispose(); await profile.pool.dispose(); });
	await f.start(); await f.preview("grep", { pattern: "one", path: "." }); await f.ready();
	await f.finish(); await f.start("queries");
	holding = true;
	actors.push(f.execute("grep", { pattern: "two", path: ".", limit: 1 }, controllers[0]!.signal).catch(error => error)); await entered.promise;
	actors.push(f.execute("grep", { pattern: "two", path: ".", limit: 2 }, controllers[1]!.signal).catch(error => error)); await joined.promise; await nextTurn();
	controllers[0]!.abort(); if (cancelAll) controllers[1]!.abort();
	expect(shared?.aborted).toBe(cancelAll); release.resolve();
	const results = await Promise.all(actors); holding = false;
	if (!cancelAll) expect(results[1]).toEqual(await f.oracle("grep", { pattern: "two", path: ".", limit: 2 }));
	expect(builds).toBe(1); expect(f.fallbacks).toEqual(cancelAll ? ["grep", "grep"] : ["grep"]); f.fallbacks.length = 0;
	const queries = [
		{ tool: "grep", input: { pattern: "two", path: "." } },
		{ tool: "find", input: { pattern: "*.txt", path: "." } },
		{ tool: "read", input: { path: "a.txt", offset: 2, limit: 1 } },
	];
	for (const query of queries) expect(await f.execute(query.tool, query.input)).toEqual(await f.oracle(query.tool, query.input));
	expect(f.fallbacks).toEqual([]);
	await writeFile(path.join(cwd, "a.txt"), "two changed\n");
	await writeFile(path.join(cwd, "added.txt"), "two new\n");
	for (const query of queries) expect(await f.execute(query.tool, query.input)).toEqual(await f.oracle(query.tool, query.input));
});

test.for(["in-place", "git"] as const)("keeps effects private, retries restored preimages and transfers an aborted claim (%s)", async driver => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "one two\n"); await link(file, path.join(cwd, "alias.txt"));
	const entered = deferred(), release = deferred(), secondResolved = deferred(), controller = new AbortController();
	let hold = false, waiting = false, first: Promise<unknown> | undefined, second: Promise<unknown> | undefined;
	const f = systemHost(cwd, [createEditTool(cwd), createReadTool(cwd)], {
		executionWorlds: [sandbox.createExecutionWorld(driver === "git" ? { driver, inPlaceMutations: false } : {})],
		preflight: async () => { if (hold) { hold = false; waiting = true; entered.resolve(); await release.promise; } return true; },
		resolveInvocation: (tool, input) => { if (waiting) secondResolved.resolve(); return resolvePiToolInvocation(tool, input, { cwd, environment: {} }); },
	});
	try {
		await f.start();
		const args = { path: "notes.txt", edits: [{ oldText: "two", newText: "three" }] };
		await f.preview("edit", args); await f.ready();
		expect(await readFile(file, "utf8")).toBe("one two\n");
		await f.finish(true); await f.start("changed-input");
		await writeFile(file, "changed two\n");
		await f.execute("edit", args);
		expect(await readFile(file, "utf8")).toBe("changed three\n");
		expect(textOutput(await f.execute("read", { path: "alias.txt" }))).toBe("changed three\n");
		await writeFile(file, "one two\n"); hold = true;
		first = f.execute("edit", args, controller.signal).catch(error => error); await entered.promise;
		second = f.execute("edit", args); await secondResolved.promise; await new Promise<void>(setImmediate);
		expect(f.fallbacks.filter(tool => tool === "edit")).toEqual(["edit"]); expect(await readFile(file, "utf8")).toBe("one two\n");
		controller.abort(); release.resolve(); await first; await second;
		expect(await readFile(file, "utf8")).toBe("one three\n"); expect(f.fallbacks.filter(tool => tool === "edit")).toEqual(["edit", "edit"]);
		await f.finish(); await f.start("discard");
		await f.preview("edit", { path: "notes.txt", edits: [{ oldText: "three", newText: "discarded" }] });
		await f.ready(2);
	} finally { controller.abort(); release.resolve(); await Promise.allSettled([first, second]); await f.dispose(); await sandbox.dispose(); }
	expect(await readFile(file, "utf8")).toBe("one three\n");
});

test.each(["ordered", "independent", "dependent"])("adopts only the proven effects of a predicted workflow across prompts (%s)", async order => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "original\n"); await link(file, path.join(cwd, "alias.txt"));
	const steps = [
		{ tool: "write", input: { path: order === "independent" ? "unused.txt" : "notes.txt", content: "alpha\nbeta\n" } },
		{ tool: "edit", input: { path: "alias.txt", edits: [{ oldText: order === "independent" ? "original" : "alpha", newText: "omega" }] } },
		{ tool: "edit", input: { path: "notes.txt", edits: [{ oldText: "omega", newText: "final" }] } },
	];
	let predict = true;
	const world = sandbox.createExecutionWorld({ driver: "git" }), fork = deferred(), release = deferred();
	const f = systemHost(cwd, [createWriteTool(cwd), createEditTool(cwd)], {
		executionWorlds: [{ ...world, speculation: { ...world.speculation, execute: async context => {
			if (order === "independent" && (context.args as { path: string }).path === "notes.txt" && context.toolName === "edit") { fork.resolve(); await release.promise; }
			return world.speculation.execute(context);
		} } }],
		complete: async () => fauxAssistantMessage(fauxToolCall("speculative_workflow", { steps }), { stopReason: "toolUse" }),
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, maxConcurrentActions: 2,
			drafterMaxDepth: 2, tools: ["write", "edit"], patternAware: { enabled: false } }),
	});
	await using _cleanup = onExit(async () => { release.resolve(); await f.dispose(); await sandbox.dispose(); });
	await f.start(); await f.ready(order === "independent" ? 2 : 3); predict = false;
	expect(await readFile(file, "utf8")).toBe("original\n");
	if (order === "independent") {
		await fork.promise; await f.execute(steps[1]!.tool, steps[1]!.input); release.resolve(); await f.ready(3);
	}
	await f.finish(true);
	if (order === "dependent") {
		await f.start("missing-parent"); await expect(f.execute(steps[1]!.tool, steps[1]!.input)).rejects.toThrow();
		expect(await readFile(file, "utf8")).toBe("original\n"); await f.finish();
	}
	for (const [index, step] of steps.entries()) {
		if (order === "independent" && index < 2) continue;
		await f.start(`step-${index}`);
		await f.execute(step.tool, step.input); await f.finish();
	}
	expect(f.fallbacks).toEqual(order === "dependent" ? ["edit"] : []);
	const expected = order === "independent" ? "final\n" : "final\nbeta\n";
	expect(await readFile(file, "utf8")).toBe(expected);
	expect(await readFile(path.join(cwd, "alias.txt"), "utf8")).toBe(expected);
	if (order === "independent") await expect(readFile(path.join(cwd, "unused.txt"))).rejects.toMatchObject({ code: "ENOENT" });
});

test.for(["unrelated", "changed", "descendant"])("retains native workflow results across prompts and validates Actor changes (%s)", async (change, { skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("native process backend requires x86-64 Linux");
	const native = await createLinuxProcessBenchmark("pi-sealed-result-", "git"), sandbox = new WorkspaceSandboxService();
	const { workspace: cwd } = native;
	await writeFile(path.join(cwd, "a.txt"), "original\n"); await writeFile(path.join(cwd, "b.txt"), "other\n");
	await writeFile(path.join(cwd, "c.txt"), "second\n");
	await commitBenchmarkFixture(cwd, "Sealed result reuse");
	let predict = change === "descendant";
	const f = systemHost(cwd, [native.tool, createWriteTool(cwd)], {
		executionWorlds: [native.world, sandbox.createExecutionWorld()],
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: native.environment, shellPath: native.shellPath }),
		complete: async () => fauxAssistantMessage(fauxToolCall("speculative_workflow", { steps: [
			{ tool: "bash", input: { command: "cat a.txt" } }, { tool: "bash", input: { command: "cat c.txt" } },
		] }), { stopReason: "toolUse" }),
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, drafterMaxDepth: 1,
			tools: ["bash", "write"], patternAware: { enabled: false } }),
	});
	await using _cleanup = onExit(async () => { await f.dispose(); await sandbox.dispose(); await native.dispose(); });
	const status = await native.backend.check(true);
	if (status.state !== "ready") return skip(status.detail);
	await f.start(); if (!predict) await f.preview("bash", { command: "cat a.txt" });
	await f.ready(predict ? 2 : 1); predict = false;
	await f.finish(true); await f.start("next-prompt");
	await f.execute("write", { path: change === "changed" ? "a.txt" : "b.txt", content: "updated\n" });
	expect(textOutput(await f.execute("bash", { command: "cat a.txt" }))).toBe(change === "changed" ? "updated\n" : "original\n");
	if (change === "descendant") {
		await f.finish(); await f.start("next");
		expect(textOutput(await f.execute("bash", { command: "cat c.txt" }))).toBe("second\n");
	}
	if (change !== "changed") {
		expect(f.fallbacks).toEqual(["write"]);
		expect(f.events.filter(event => event.type === "candidate" && event.state.status === "running")).toHaveLength(change === "descendant" ? 2 : 1);
	}
});

test("reconsiders restored results while sharing pending reads and retains fresh queries across turns", async () => {
	const cwd = await directories.create(), reader = createReadTool(cwd);
	const file = path.join(cwd, "notes.txt"), original = Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join("\n");
	await writeFile(file, original);
	const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { maxBytes: () => 1024 * 1024 });
	const computed = deferred(), release = deferred(), entered = deferred(), controller = new AbortController();
	const args = { path: "notes.txt", offset: 5, limit: 1 }, wide = { ...args, offset: 1, limit: 10 };
	let runs = 0, authorizations = 0;
	const f = systemHost(cwd, [reader, createWriteTool(cwd)], {
		preflight: context => {
			if ((context.args as { offset?: number }).offset === 5 && ++authorizations > 100)
				controller.abort(new Error("shared waiters prevented producer progress"));
			return true;
		},
		executionWorlds: [{ ...world, speculation: { ...world.speculation!, execute: async context => {
			const first = ++runs === 1, branch = await world.speculation!.execute(context);
			if (!first) { computed.resolve(); await release.promise; } return first ? { ...branch, reconstruct: undefined, reconstructionScope: undefined,
				validate: async () => { const result = await branch.validate!(); if (result.status === "stale") entered.resolve(); return result; },
			} : branch;
		} } }],
	});
	let actors: Promise<unknown>[] = [];
	await using _cleanup = onExit(async () => { controller.abort(); release.resolve(); await Promise.allSettled(actors); await f.dispose(); });
	await f.start(); await f.preview("read", args); await f.ready();
	await writeFile(file, original.replaceAll("line", "edit"));
	await f.preview("read", wide); await computed.promise;
	actors = [1, 2].map(() => f.execute("read", args, controller.signal).catch(error => error)); await entered.promise;
	await writeFile(file, original); release.resolve();
	const expected = await reader.execute("oracle", args); expect(await Promise.all(actors)).toEqual([expected, expected]);
	await f.preview("read", { ...args, offset: 6 }); await f.finish(); await f.start("second");
	for (const event of f.events.filter(event => event.type === "actor_action")) {
		expect(event.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } }); expect(event.computation?.hiddenComputeMs).toBeGreaterThan(0);
	}
	for (const input of [args, wide])
		expect(await f.execute("read", input)).toEqual(await reader.execute("reference", input));
	expect({ runs, fallbacks: f.fallbacks }).toEqual({ runs: 2, fallbacks: [] });
	await f.execute("write", { path: "notes.txt", content: "new\n" });
	await f.preview("read", wide, undefined, "fresh");
	expect(textOutput(await f.execute("read", wide, undefined, "fresh"))).toBe("new\n");
	await writeFile(file, "prepared\n"); await f.preview("read", wide); await writeFile(file, "changed again\n");
	expect(textOutput(await f.execute("read", wide))).toBe("changed again\n");
	await writeFile(file, original); expect(await f.execute("read", args)).toEqual(await reader.execute("restored", args)); await f.finish();
	const settled = f.events.filter(event => event.type === "actor_action");
	expect(settled.at(-3)?.computation?.hiddenComputeMs).toBeGreaterThan(0);
	expect(settled.at(-2)?.computation?.hiddenComputeMs).toBe(0);
	expect(settled.at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } });
});
