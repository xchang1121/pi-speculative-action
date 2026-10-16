import { link, readFile, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall, Type } from "@earendil-works/pi-ai";
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
import { commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark, textOutput } from "./linux-process-fixture.ts";
import { EXACT_TOOL_OBSERVATION, type ToolObservationContract } from "../src/tool-settlement.ts";

const directories = temporaryDirectories("pi-agent-system-");
afterEach(directories.dispose);

const inputScenarios = (preview = false) => [
	...["read", "names"].flatMap(input => ["", ...preview ? ["-preview"] : [], "-stale", "-copy", "-change", "-budget", "-outside", "-unkeyed"].map(suffix => `input-${input}${suffix}`)),
	"input-read-range", "input-read-cancel", "input-read-failure",
];
const mixedScenarios = ["producer", "consumer", "stale", "failure", "budget", "outside", "revoke", "overlap", "cancel", "cancel-return", "unkeyed"].map(mode => `mixed-${mode}`);

test.for(["actor", "preview", "dynamic", "dynamic-preview", "hints", "hints-preview", "retired", "retired-preview", "predicted", "stale", "binding",
	"calculation", "calculation-preview", "calculation-unrelated", "calculation-unrelated-preview", "calculation-hint", "calculation-hint-preview", "calculation-key",
	"seed-dynamic", "seed-dynamic-preview", "seed-calculation", "seed-calculation-preview", "seed-stale", "seed-binding", "seed-calculation-key", "seed-unbounded", "seed-repeat", "seed-revoke",
	...mixedScenarios, "mixed-binding-unkeyed", ...inputScenarios(true), "unregistered-dynamic", "unregistered-mixed-producer", "late-dynamic",
	...["dynamic", "dynamic-preview", "calculation", "calculation-preview", "calculation-unrelated", "calculation-key", "stale", "binding", "retired", "unbounded", "repeat", "revoke", "rejected-capture", "rejected-branch", "rejected-output",
		...mixedScenarios, ...inputScenarios()].map(mode => `unkeyed-${mode}`),
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
		complete: async () => fauxAssistantMessage(fauxToolCall(names[unrelated ? 2 : 1]!, { path: "input.txt" }), { stopReason: "toolUse" }),
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

test.for(["independent", "unchanged", "batch", "batch-known", "mismatch-text", "mismatch-error", "mismatch-inflight", "mismatch-budget", "parent-miss", "stale", "same-resource", "missing-input", "independent-create",
	"budget", "depth-zero", "denied-root", "denied-child", "unknown-batch", "filtered-batch", "malformed-observation", "malformed-type", "abort-observation", "actual-before-forecast", "actual-before-malformed",
	"actual-before-failure", "actual-before-batch", "request-failure", "malformed-extra", "mismatch-exit", "canonical", "canonical-mismatch", "canonical-version", "canonical-schema", "canonical-unbound", "normalizer-failure"])("executes out-of-order drafts using private observations (%s)", async mode => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), release = deferred(), forecast = deferred(), observing = deferred(), firstResult = deferred();
	const actualFirst = mode.startsWith("actual-before-"), normalized = mode.startsWith("canonical"), wrong = mode.startsWith("mismatch") || mode === "canonical-mismatch";
	const known = mode === "batch-known", recoverable = ["request-failure", "malformed-observation", "malformed-type", "malformed-extra"].includes(mode);
	const inflight = mode === "mismatch-inflight", repairBudget = mode === "mismatch-budget", actualBatch = mode === "actual-before-batch";
	const textMismatch = mode.startsWith("mismatch") && !["mismatch-error", "mismatch-exit"].includes(mode), contractChanged = ["canonical-version", "canonical-schema", "canonical-unbound"].includes(mode);
	let version = "test-result/1", bound = true;
	let schema = Type.Array(Type.Object({ type: Type.Literal("text"), text: Type.String({ pattern: '"artifact":"abc"' }) }));
	const canonicalize: NonNullable<ToolObservationContract["canonicalize"]> = content => content.map(block => {
		if (mode === "normalizer-failure") throw new Error("invalid normalizer");
		if (block.type !== "text") return block;
		const { duration: _duration, pid: _pid, tempPath: _tempPath, ...kept } = JSON.parse(block.text); return { ...block, text: JSON.stringify(kept) };
	});
	const mutation = ["same-resource", "missing-input", "independent-create"].includes(mode), dependent = mutation && mode !== "independent-create", count = mode.startsWith("batch") || actualBatch ? 2 : 1;
	const read = PI_ACTION_SEMANTICS.definition("read")!, names = ["inspect_rows", "render_rows"];
	const definitions = names.map(tool => ({ ...read, tool, epoch: tool, projectors: [] })), semantics = new ActionSemanticsRegistry([...definitions, PI_ACTION_SEMANTICS.definition("write")!]);
	const reader = createReadTool(cwd), tools = [...names.map(name => ({ ...reader, name })), createWriteTool(cwd)] as Parameters<typeof systemHost>[1];
	const roots = Array.from({ length: count }, (_, index) => ({ tool: mutation ? "write" : names[0]!, args: mutation ? { path: "root-0.txt", content: "prepared\n" }
		: { path: `root-${index}.txt`, offset: normalized ? 1 : 2, limit: normalized ? 10 : 1 } }));
	for (const root of roots) if (mode !== "missing-input" && mode !== "independent-create") await writeFile(path.join(cwd, root.args.path), normalized
		? '{"failedTests":[],"artifact":"abc","duration":14,"pid":123,"tempPath":"/tmp/actual"}\n' : "one\ntwo\nthree\n");
	await writeFile(path.join(cwd, "child.txt"), "child\n");
	if (wrong) await writeFile(path.join(cwd, "discarded.txt"), "discarded\n");
	const child = { tool: names[1]!, args: { path: dependent ? "root-0.txt" : "child.txt" } };
	const expected = await Promise.all(roots.map(root => mutation ? { content: [{ type: "text" as const, text: "Successfully wrote 9 bytes to root-0.txt" }] }
		: reader.execute("oracle", root.args)));
	if (normalized) expect(canonicalize(expected[0]!.content)).toEqual([{ type: "text", text: '{"failedTests":[],"artifact":"abc"}' }]);
	const worlds = [createResourceSnapshotExecutionWorld(semantics, { maxBytes: () => 1024 * 1024 }), sandbox.createExecutionWorld({ driver: "git" })];
	let requests = 0, drafts = 0, rootContext: import("@earendil-works/pi-ai").Context | undefined, observationSignal: AbortSignal | undefined;
	const privateContexts: import("@earendil-works/pi-ai").Context[] = [], executed: string[] = [];
	const settings = { tools: mode === "filtered-batch" ? [names[0]!] : undefined, drafterEnabled: true, maxConcurrentActions: 3, drafterMaxDepth: mode === "depth-zero" ? 0 : mode === "denied-child" ? 2 : 1,
		candidateLimit: mode === "unchanged" ? 3 : 1, predictionTimeoutMs: 30_000, drafterTaskMaxRequests: mode === "budget" ? 2 : repairBudget ? 3 : 32 };
	const f = systemHost(cwd, tools, { actionSemantics: semantics,
		resources: { initial: { cpuCount: 3, idleCpuCount: 3 }, sample: () => new Promise(() => {}) },
		onEvent: event => { if (known && event.type === "candidate" && event.state.status === "succeeded" && event.candidate.tool === names[0]) firstResult.resolve(); },
		resolveInvocation: async (tool, args) => { if (known && (args as { path: string }).path === "root-1.txt") await firstResult.promise; return { ...resolvePiToolInvocation(tool === "write" ? tool : "read", args, { cwd, environment: {} })!,
			...(bound && tool === names[0] && (normalized || mode === "normalizer-failure") ? { observation: { ...EXACT_TOOL_OBSERVATION, version, content: normalized ? schema : undefined, canonicalize } } : {}),
			semantics: tool === "write" ? PI_ACTION_SEMANTICS.definition(tool) : definitions[names.indexOf(tool)] }; },
		preflight: context => !(mode === "denied-root" && context.toolName !== child.tool || mode === "denied-child" && context.toolName === child.tool),
		executionWorlds: worlds.map(world => ({ ...world, speculation: { ...world.speculation!, execute: async context => {
			executed.push(context.toolName); const branch = await world.speculation!.execute(context);
			if (context.toolName !== child.tool && !(known && (context.args as { path: string }).path === "root-0.txt")) await release.promise;
			if (actualBatch && (context.args as { path: string }).path === "root-0.txt") await firstResult.promise; return branch;
		} } })),
		complete: async (_model, context, options) => {
			requests++; expect(options?.apiKey).toBe("shared-credential");
			if (mode === "unchanged" && rootContext && !context.messages.length) return fauxAssistantMessage("No alternative.");
			if (context.tools?.[0]?.name === "speculative_continuations") return fauxAssistantMessage(fauxToolCall("speculative_continuations", {
				scopes: [[...roots.map((_, index) => `root-${index}`), ...(["unknown-batch", "filtered-batch"].includes(mode) ? ["unknown"] : [])]],
			}));
			if (context.tools?.[0]?.name === "speculative_observations") {
				observationSignal = options!.signal;
				const payload = JSON.parse(String(context.messages.at(-1)!.content).split("\n").slice(1).join("\n"));
				expect(payload.draft).toMatchObject([{ type: "thinking", thinking: "Inspect the input." }, ...roots.map((root, index) =>
					({ type: "toolCall", id: `root-${index}`, name: root.tool, arguments: root.args }))]);
				const partial = actualBatch && payload.observations.length > 0;
				expect(payload.observations).toEqual(known || partial ? [expect.objectContaining({ role: "toolResult", toolCallId: `root-${Number(partial)}`, content: expected[Number(partial)]!.content })] : []);
				const predictedRoots = known ? roots.slice(1) : partial ? roots.slice(0, 1) : roots;
				if (partial) firstResult.resolve();
				expect(payload.candidates.map((item: { tool: unknown }) => item.tool)).toEqual(JSON.parse(JSON.stringify(predictedRoots.map(root => tools.find(tool => tool.name === root.tool)))));
				for (const item of payload.candidates) {
					expect(item.observation.version).toBe(normalized || mode === "normalizer-failure" ? version : "model-visible/1");
					if (normalized) expect(item.observation.content).toEqual(JSON.parse(JSON.stringify(schema)));
				}
				observing.resolve();
				if (mode === "abort-observation") await new Promise((_, reject) => options!.signal!.addEventListener("abort", () => reject(options!.signal!.reason), { once: true }));
				if (actualFirst) await forecast.promise;
				if (mode === "request-failure" || mode === "actual-before-failure") throw new Error("observation transport failure");
				const response = fauxAssistantMessage(fauxToolCall("speculative_observations", { results: predictedRoots.map(root => { const index = roots.indexOf(root); return {
					callID: mode === "malformed-observation" ? "unknown" : `root-${index}`, isError: mode === "malformed-type" || mode === "actual-before-malformed" ? "false" : mode === "mismatch-error",
					...(mode === "malformed-extra" ? { score: 1_000_000 } : {}), ...(mode === "mismatch-exit" ? { exitCode: 7 } : {}),
					content: normalized ? [{ type: "text", text: JSON.stringify({ failedTests: mode === "canonical-mismatch" ? ["a.test.ts:3"] : [], artifact: "abc", duration: 999, pid: 456, tempPath: "/tmp/predicted" }) }]
						: textMismatch || actualFirst ? [{ type: "text", text: "incorrect forecast" }] : expected[index]!.content,
				}; }) }), { stopReason: "toolUse" });
				return { ...response, usage: { ...response.usage, input: 11, output: 7, totalTokens: 18 } };
			}
			if (++drafts === 1) { rootContext = context; return fauxAssistantMessage([{ type: "thinking", thinking: "Inspect the input." },
				...roots.map((root, index) => fauxToolCall(root.tool, root.args, { id: `root-${index}` })), ...(["unknown-batch", "filtered-batch"].includes(mode) ? [fauxToolCall(mode === "unknown-batch" ? "unavailable" : child.tool, child.args, { id: "unknown" })] : [])], { stopReason: "toolUse" }); }
			privateContexts.push(context); const discarded = wrong && privateContexts.length === 1;
			if (inflight && discarded) { observationSignal = options!.signal; firstResult.resolve(); await forecast.promise; }
			return fauxAssistantMessage(fauxToolCall(child.tool, discarded ? { path: "discarded.txt" } : child.args,
				{ id: "child" }), { stopReason: "toolUse" });
		}, getDraftOptions: () => ({ apiKey: "shared-credential" }),
	}, settings);
	await using _cleanup = onExit(async () => { release.resolve(); forecast.resolve(); firstResult.resolve(); await f.dispose(); await sandbox.dispose(); });
	await f.start();
	const stopped = ["budget", "depth-zero", "denied-root", "unknown-batch", "filtered-batch", "malformed-observation", "malformed-type", "abort-observation", "normalizer-failure", "request-failure", "malformed-extra"].includes(mode), rootOnly = ["depth-zero", "denied-root", "unknown-batch", "filtered-batch"].includes(mode);
	if (mode === "abort-observation") { await observing.promise; release.resolve(); await f.dispose(); }
	else if (actualFirst || inflight) {
		await (inflight ? firstResult.promise : observing.promise); release.resolve(); await f.ready(count + 1);
		expect(f.events.some(event => event.type === "source_request" && event.request.settlement.status === "timeout")).toBe(false);
		expect(observationSignal?.aborted).toBe(true); expect(f.host.drafterBudgetSnapshot().budget.reservedTokens).toBeGreaterThan(0); forecast.resolve();
		await expect.poll(() => f.host.drafterBudgetSnapshot().budget.reservedTokens).toBe(0);
	}
	else if (stopped || mode === "denied-child") {
		await expect.poll(() => f.events.filter(event => event.type === "source_request").length).toBeGreaterThanOrEqual(rootOnly ? 1 : 2);
	} else if (dependent) {
		await expect.poll(() => drafts).toBe(2); await nextTurn(); expect(executed).not.toContain(child.tool);
		release.resolve(); await f.ready(count + 1);
		if (mode === "missing-input") await expect(readFile(path.join(cwd, "root-0.txt"))).rejects.toMatchObject({ code: "ENOENT" });
		else expect(await readFile(path.join(cwd, "root-0.txt"), "utf8")).toBe("one\ntwo\nthree\n");
	} else {
		await f.ready(known ? 2 : 1); expect(f.events.flatMap(event => event.type === "candidate" && event.state.status === "succeeded" ? [event.candidate.tool] : [])).toEqual([...(known ? [names[0]] : []), child.tool]);
		expect(executed.filter(tool => tool !== child.tool)).toHaveLength(count);
	}
	expect(rootContext?.messages).toEqual([]);
	if (stopped) expect(drafts).toBe(1);
	else {
		expect(privateContexts).toHaveLength(inflight ? 2 : 1);
		const messages = privateContexts[0]!.messages;
		expect(messages[0]).toMatchObject({ role: "assistant", content: [{ type: "thinking", thinking: "Inspect the input." }, ...roots.map((root, index) =>
			({ type: "toolCall", id: `root-${index}`, name: root.tool, arguments: root.args }))] });
		expect(messages.slice(1)).toEqual(expected.map((result, index) => expect.objectContaining({ role: "toolResult", toolCallId: `root-${index}`,
			content: normalized ? [{ type: "text", text: JSON.stringify({ failedTests: mode === "canonical-mismatch" ? ["a.test.ts:3"] : [], artifact: "abc" }) }]
				: textMismatch ? [{ type: "text", text: "incorrect forecast" }] : result.content, isError: mode === "mismatch-error" })));
	}
	release.resolve();
	if (mode !== "denied-root" && mode !== "abort-observation") await f.ready(count + Number((!stopped || recoverable) && mode !== "denied-child") + Number(wrong && !inflight && !repairBudget));
	if (wrong && !repairBudget || recoverable) expect(privateContexts.at(-1)!.messages.slice(1)).toEqual(expected.map((result, index) => expect.objectContaining({
		role: "toolResult", toolCallId: `root-${index}`, content: normalized ? canonicalize(result.content) : result.content, isError: false })));
	const { budget, observations } = f.host.drafterBudgetSnapshot();
	expect(budget.reportedTokens).toBe(rootOnly || ["abort-observation", "request-failure", "actual-before-failure"].includes(mode) ? 0 : actualBatch ? 36 : 18);
	const forecasted = rootOnly || actualFirst || ["malformed-observation", "malformed-type", "abort-observation", "normalizer-failure", "request-failure", "malformed-extra"].includes(mode) ? 0 : count - Number(known);
	expect(observations).toEqual({ forecasted, compared: forecasted, matched: wrong ? 0 : forecasted });
	expect(budget.requests).toBe(requests); expect(budget.reservedTokens).toBe(0);
	expect(requests).toBe((rootOnly ? 1 : stopped && !recoverable ? 2 : wrong && !repairBudget ? 4 : 3) + (mode === "unchanged" ? 2 : 0)
		+ Number(count > 1 || ["unknown-batch", "filtered-batch"].includes(mode)) + Number(actualBatch));
	if (mode === "budget" || repairBudget) expect(budget.skippedRequests).toBe(1);
	if (mode === "abort-observation") expect(budget.unreportedTokens).toBeGreaterThan(0);
	if (mode === "normalizer-failure") expect(await f.execute(roots[0]!.tool, roots[0]!.args)).toMatchObject(expected[0]!);
	if (stopped && !recoverable || mode === "denied-child") return;
	if (mode === "canonical-version") version = "test-result/2";
	if (mode === "canonical-schema") schema = Type.Array(schema.items, { minItems: 1 });
	if (mode === "canonical-unbound") bound = false;
	if (mode === "parent-miss") await f.execute(names[0]!, { path: "child.txt" });
	else for (const root of roots) expect(await f.execute(root.tool, root.args)).toMatchObject(expected[roots.indexOf(root)]!);
	settings.drafterEnabled = false; await f.finish(); await f.start("child");
	if (mode === "stale") await writeFile(path.join(cwd, "child.txt"), "fresh\n");
	expect(textOutput(await f.execute(child.tool, child.args))).toBe(dependent ? "prepared\n" : mode === "stale" ? "fresh\n" : "child\n");
	expect(f.fallbacks).toEqual([]);
	await f.finish();
	if (mode === "stale" || repairBudget) expect(f.events.filter(event => event.type === "actor_action").at(-1)).toMatchObject({ computation: { hiddenComputeMs: 0 } });
	if (wrong && !repairBudget || recoverable || actualFirst || known || contractChanged) expect(f.events.filter(event => event.type === "actor_action").at(-1)!.computation!.hiddenComputeMs).toBeGreaterThan(0);
	if (wrong && !inflight || mode === "parent-miss" || contractChanged) expect(f.events.some(event => event.type === "prediction" && event.tool === child.tool &&
		event.settlement.observation === "unobserved" && event.settlement.cause.code === "dependency_impossible")).toBe(true);
	if (mode !== "parent-miss" && !repairBudget) expect(f.events.some(event => event.type === "prediction" && event.tool === child.tool &&
		event.settlement.observation === "observed" && event.settlement.match.matched)).toBe(true);
	if (inflight) expect(executed.filter(tool => tool === child.tool)).toHaveLength(1);
	expect(requests).toBe(budget.requests + Number(contractChanged)); expect(f.host.drafterBudgetSnapshot().budget.requests).toBe(requests);
});

test.for(["completed", "inflight", "forecast", "binding", "parent-binding", "reverse", "both", "unchanged", "malformed-scope", "duplicate-scope", "extra-scope", "budget", "stale", "deep"])("repairs only branches that read changed observations (%s)", async mode => {
	const cwd = await directories.create(), gates = [deferred(), deferred()], delayed = deferred();
	const scopes = mode.endsWith("-scope") ? [["a", "b"]] : [["a"], ["b"], ["a", "b"]];
	const unchanged = mode === "unchanged", both = mode === "both", deep = mode === "deep";
	const names = ["inspect_rows", "render_rows"], reader = createReadTool(cwd), read = PI_ACTION_SEMANTICS.definition("read")!;
	const definitions = names.map(tool => ({ ...read, tool, epoch: tool, projectors: [] })), semantics = new ActionSemanticsRegistry(definitions);
	const tools = names.map(name => ({ ...reader, name })) as Parameters<typeof systemHost>[1], world = createResourceSnapshotExecutionWorld(semantics, { maxBytes: () => 1024 * 1024 });
	const roots = ["a", "b"].map(id => ({ id, path: `${id}.txt`, text: `${id}-real\n` }));
	for (const root of roots) await writeFile(path.join(cwd, root.path), root.text);
	for (const branch of ["a", "b", "ab"]) for (const version of ["old", "new"]) for (const suffix of ["", ".next"])
		await writeFile(path.join(cwd, `${branch}-${version}${suffix}.txt`), `${branch}-${version}${suffix}\n`);
	const calls: { path: string; signal: AbortSignal }[] = [], forecasts: AbortSignal[] = [], executed: string[] = [];
	const settings = { drafterEnabled: true, candidateLimit: 1, maxConcurrentActions: 3, predictionTimeoutMs: 30_000, drafterMaxDepth: deep ? 2 : 1, drafterTaskMaxRequests: mode === "budget" ? 8 : 32 };
	const f = systemHost(cwd, tools, { actionSemantics: semantics,
		resources: { initial: { cpuCount: 3, idleCpuCount: 3 }, sample: () => new Promise(() => {}) },
		resolveInvocation: async (tool, args) => {
			const file = String((args as { path: string }).path);
			if (mode === "binding" && file.includes("old") || mode === "parent-binding" && file === "b.txt") await delayed.promise;
			return { ...resolvePiToolInvocation("read", args, { cwd, environment: {} })!, semantics: definitions[names.indexOf(tool)] };
		},
		executionWorlds: [{ ...world, speculation: { ...world.speculation!, execute: async context => {
			const file = String((context.args as { path: string }).path); executed.push(file);
			const result = await world.speculation!.execute(context), index = roots.findIndex(root => root.path === file);
			if (index >= 0) await gates[index]!.promise;
			return result;
		} } }],
		complete: async (_model, context, options) => {
			const tool = context.tools?.[0]?.name;
			if (tool === "speculative_continuations") {
				expect(JSON.stringify(context.messages)).not.toMatch(/a-real|b-real|a-wrong|b-wrong/);
				return fauxAssistantMessage(fauxToolCall(tool, mode === "extra-scope" ? { scopes: [["a"]], extra: "invalid" }
					: { scopes: mode === "malformed-scope" ? [["unknown"]] : mode === "duplicate-scope" ? [["a"], ["a"]] : scopes }));
			}
			if (tool === "speculative_observations") {
				const payload = JSON.parse(String(context.messages.at(-1)!.content).split("\n").slice(1).join("\n"));
				const ids: string[] = payload.candidates.map((item: { call: { id: string } }) => item.call.id);
				if (ids.length === 1 && ids[0] === "b" && !payload.observations.length) expect(JSON.stringify(context.messages)).not.toContain("a-real");
				if (mode === "forecast" && ids.includes("a")) { forecasts.push(options!.signal!); await delayed.promise; }
				return fauxAssistantMessage(fauxToolCall(tool, { results: payload.candidates.map(({ call }: { call: { id: string; arguments: { path: string } } }) => ({
					callID: call.id, isError: false, content: [{ type: "text", text: call.id === "a" ? unchanged ? "a-real\n" : "a-wrong\n"
						: call.id === "b" ? both ? "b-wrong\n" : "b-real\n" : `${call.arguments.path.replace(/\.txt$/, "")}\n` }],
				})) }));
			}
			if (!context.messages.length) return fauxAssistantMessage(roots.map(root => fauxToolCall(names[0]!, { path: root.path }, { id: root.id })));
			const results = context.messages.filter(message => message.role === "toolResult"), last = results.at(-1)!;
			let file: string;
			if (last.toolCallId.startsWith("child-")) file = `${last.toolCallId.slice(6)}.next.txt`;
			else {
				const visible = results.filter(result => textOutput(result) !== "Observation withheld from this continuation branch.");
				const ids = visible.map(result => result.toolCallId), wrong = visible.some(result => textOutput(result).includes("wrong"));
				expect(scopes).toContainEqual(ids);
				file = `${ids.join("")}-${wrong ? "old" : "new"}.txt`;
				if (ids.length === 1) expect(textOutput(results.find(result => result.toolCallId !== ids[0])!)).toBe("Observation withheld from this continuation branch.");
			}
			calls.push({ path: file, signal: options!.signal! });
			if ((mode === "inflight" && file.includes("old") || mode === "reverse" && file === "a-old.txt") && !file.includes(".next")) await delayed.promise;
			return fauxAssistantMessage(fauxToolCall(names[1]!, { path: file }, { id: `child-${file.replace(/\.txt$/, "")}` }));
		},
	}, settings);
	await using _cleanup = onExit(async () => { for (const gate of gates) gate.resolve(); delayed.resolve(); await f.dispose(); });
	const completed = (file: string) => f.events.find(event => event.type === "candidate" && event.state.status === "succeeded" && JSON.parse(event.candidate.predictedAction).input.path === file);
	const ready = async (files: string[]) => { for (const file of files) await expect.poll(() => Boolean(completed(file))).toBe(true); };
	await f.start();
	if (mode === "forecast") { await expect.poll(() => forecasts.length).toBe(2); await ready(["b-new.txt"]); }
	else if (mode === "parent-binding") await expect.poll(() => calls.some(call => call.path === "a-old.txt")).toBe(true);
	else if (mode === "inflight" || mode === "binding") { await expect.poll(() => calls.length).toBe(3); await ready(["b-new.txt"]); }
	else if (mode === "reverse") { await ready(["b-new.txt", "ab-old.txt"]); delayed.resolve(); await ready(["a-old.txt"]); }
	else await ready(scopes.map(ids => `${ids.join("")}-${unchanged || ids.join("") === "b" && !both ? "new" : "old"}.txt`));
	if (deep) await ready(["a-old.next.txt", "b-new.next.txt", "ab-old.next.txt"]);
	const bCandidate = completed("b-new.txt");
	gates[0]!.resolve(); if (both) gates[1]!.resolve();
	if (mode === "parent-binding") { await expect.poll(() => calls.some(call => call.path === "a-new.txt")).toBe(true); expect(executed).not.toContain("b.txt"); delayed.resolve(); }
	const expected = scopes.map(ids => `${ids.join("")}-new.txt`);
	if (mode === "budget") await expect.poll(() => f.host.drafterBudgetSnapshot().budget.skippedRequests).toBe(2);
	else await ready(expected);
	if (deep) await ready(["a-new.next.txt", "ab-new.next.txt"]);
	if (!both) expect(completed("b.txt")).toBeUndefined();
	if (bCandidate) { expect(completed("b-new.txt")).toBe(bCandidate); expect(calls.filter(call => call.path === "b-new.txt")).toHaveLength(1);
		expect(calls.find(call => call.path === "b-new.txt")!.signal.aborted).toBe(false); }
	if (mode === "inflight") expect(calls.filter(call => call.path.includes("old")).every(call => call.signal.aborted)).toBe(true);
	if (mode === "forecast") expect(forecasts.every(signal => signal.aborted)).toBe(true);
	if (mode === "forecast" || mode === "inflight") { expect(f.host.drafterBudgetSnapshot().budget.reservedTokens).toBeGreaterThan(0); delayed.resolve(); }
	if (mode === "binding") delayed.resolve();
	await expect.poll(() => f.host.drafterBudgetSnapshot().budget.reservedTokens).toBe(0);
	if (unchanged) expect(calls).toHaveLength(3);
	gates[1]!.resolve(); await ready(["a.txt", "b.txt"]);
	for (const root of roots) expect(textOutput(await f.execute(names[0]!, { path: root.path }))).toBe(root.text);
	settings.drafterEnabled = false; await f.finish(); await f.start("children");
	if (mode === "stale") await writeFile(path.join(cwd, "a-new.txt"), "fresh\n");
	for (const file of expected) {
		expect(textOutput(await f.execute(names[1]!, { path: file }))).toBe(mode === "stale" && file === "a-new.txt" ? "fresh\n" : `${file.replace(/\.txt$/, "")}\n`);
		const consumed = () => f.events.find(event => event.type === "actor_action" && JSON.parse(event.actualAction).input.path === file);
		await expect.poll(consumed).toBeDefined(); const event = consumed()!;
		if (event.type !== "actor_action") throw new Error("Missing actual consumption");
		if (mode === "budget" && file !== "b-new.txt" || mode === "stale" && file === "a-new.txt") expect(event.computation!.hiddenComputeMs).toBe(0);
		else expect(event.computation!.hiddenComputeMs).toBeGreaterThan(0);
	}
	if (deep) {
		await f.finish(); await f.start("grandchildren");
		for (const file of ["a-new.next.txt", "b-new.next.txt", "ab-new.next.txt"]) {
			expect(textOutput(await f.execute(names[1]!, { path: file }))).toBe(`${file.replace(/\.txt$/, "")}\n`);
			await expect.poll(() => f.events.find(event => event.type === "actor_action" && JSON.parse(event.actualAction).input.path === file)).toMatchObject({ computation: { hiddenComputeMs: expect.any(Number) } });
			expect(f.events.filter(event => event.type === "actor_action").at(-1)!.computation!.hiddenComputeMs).toBeGreaterThan(0);
		}
		expect(calls.filter(call => call.path.startsWith("b-new"))).toHaveLength(2);
	}
	await f.finish();
	for (const file of [...(scopes.length > 1 ? ["b-new.txt"] : []), ...(deep ? ["b-new.next.txt"] : [])]) expect(f.events.filter(event =>
		event.type === "prediction" && JSON.parse(event.predictedAction!).input.path === file)).toMatchObject([{ settlement: { observation: "observed", match: { matched: true } } }]);
	expect(f.events.some(event => event.type === "source_request" && event.request.settlement.status === "timeout")).toBe(false);
	if (["inflight", "forecast", "binding"].includes(mode)) expect(executed.some(file => file.includes("old"))).toBe(false);
	if (bCandidate) expect(executed.filter(file => file === "b-new.txt")).toHaveLength(1);
});

test.for(["dependent", "independent"])("preserves sibling resource effects outside observation scopes (%s)", async mode => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), release = deferred();
	await writeFile(path.join(cwd, "selector.txt"), "select\n"); await writeFile(path.join(cwd, "written.txt"), "before\n");
	await writeFile(path.join(cwd, "independent.txt"), "independent\n");
	const child = { path: mode === "dependent" ? "written.txt" : "independent.txt" }, tools = [createReadTool(cwd), createWriteTool(cwd)];
	const worlds = [createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { maxBytes: () => 1024 * 1024 }), sandbox.createExecutionWorld({ driver: "git" })];
	let drafted = 0; const executed: string[] = [], settings = { drafterEnabled: true, candidateLimit: 1, drafterMaxDepth: 1, maxConcurrentActions: 3 };
	const f = systemHost(cwd, tools as Parameters<typeof systemHost>[1], {
		resources: { initial: { cpuCount: 3, idleCpuCount: 3 }, sample: () => new Promise(() => {}) },
		executionWorlds: worlds.map(world => ({ ...world, speculation: { ...world.speculation!, execute: async context => {
			executed.push(String((context.args as { path: string }).path)); const branch = await world.speculation!.execute(context);
			if (context.toolName === "write") await release.promise;
			return branch;
		} } })),
		complete: async (_model, context) => {
			const tool = context.tools?.[0]?.name;
			if (tool === "speculative_continuations") return fauxAssistantMessage(fauxToolCall(tool, { scopes: [["selector"]] }));
			if (tool === "speculative_observations") return fauxAssistantMessage(fauxToolCall(tool, { results: [{ callID: "selector", isError: false, content: [{ type: "text", text: "select\n" }] }] }));
			if (!context.messages.length) return fauxAssistantMessage([fauxToolCall("read", { path: "selector.txt" }, { id: "selector" }),
				fauxToolCall("write", { path: "written.txt", content: "after\n" }, { id: "writer" })]);
			drafted++; return fauxAssistantMessage(fauxToolCall("read", child));
		},
	}, settings);
	await using _cleanup = onExit(async () => { release.resolve(); await f.dispose(); await sandbox.dispose(); });
	await f.start(); await expect.poll(() => drafted).toBe(1); await nextTurn();
	if (mode === "dependent") expect(executed.filter(file => file === child.path)).toHaveLength(1); // Only the still-pending writer.
	else await f.ready(2);
	release.resolve(); await f.ready(3);
	expect(textOutput(await f.execute("read", { path: "selector.txt" }))).toBe("select\n");
	await f.execute("write", { path: "written.txt", content: "after\n" });
	settings.drafterEnabled = false; await f.finish(); await f.start("child");
	expect(textOutput(await f.execute("read", child))).toBe(mode === "dependent" ? "after\n" : "independent\n");
	await f.finish(); expect(drafted).toBe(1);
	expect(f.events.filter(event => event.type === "actor_action").at(-1)!.computation!.hiddenComputeMs).toBeGreaterThan(0);
});

test.each(["ordered", "independent", "dependent"])("adopts only the proven effects of a predicted workflow across prompts (%s)", async order => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "original\n"); await link(file, path.join(cwd, "alias.txt"));
	const steps = [
		{ tool: "write", input: { path: order === "independent" ? "unused.txt" : "notes.txt", content: "alpha\nbeta\n" } },
		{ tool: "edit", input: { path: "alias.txt", edits: [{ oldText: order === "independent" ? "original" : "alpha", newText: "omega" }] } },
		{ tool: "edit", input: { path: "notes.txt", edits: [{ oldText: "omega", newText: "final" }] } },
	];
	let predict = true, drafted = 0;
	const world = sandbox.createExecutionWorld({ driver: "git" }), fork = deferred(), release = deferred();
	const f = systemHost(cwd, [createWriteTool(cwd), createEditTool(cwd)], {
		executionWorlds: [{ ...world, speculation: { ...world.speculation, execute: async context => {
			if (order === "independent" && (context.args as { path: string }).path === "notes.txt" && context.toolName === "edit") { fork.resolve(); await release.promise; }
			return world.speculation.execute(context);
		} } }],
		complete: async (_model, context) => {
			const index = context.tools?.[0]?.name === "speculative_observations" ? drafted - 1 : drafted++, step = steps[index]!;
			return fauxAssistantMessage(fauxToolCall(context.tools?.[0]?.name === "speculative_observations" ? "speculative_observations" : step.tool,
				context.tools?.[0]?.name === "speculative_observations" ? { results: [{ callID: `draft-${index}`, isError: false, content: [{ type: "text",
					text: step.tool === "write" ? `Successfully wrote ${step.input.content!.length} bytes to ${step.input.path}` : `Successfully replaced 1 block(s) in ${step.input.path}.` }] }] } : step.input,
				{ id: `draft-${index}` }), { stopReason: "toolUse" });
		},
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, maxConcurrentActions: 2,
			drafterMaxDepth: 2, patternAware: { enabled: false } }),
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

test.for(["unrelated", "changed", "descendant", "mutation", "mutation-stat", "mutation-split", "mutation-stale"])("retains native workflow results across prompts and validates Actor changes (%s)", async (change, { skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("native process backend requires x86-64 Linux");
	const mutation = change.startsWith("mutation"), metadata = change.endsWith("stat"), split = /split|stale/.test(change), stale = change.endsWith("stale");
	const native = await createLinuxProcessBenchmark("pi-sealed-result-", mutation ? "overlayfs" : "git"), { workspace: cwd, workspaceSandbox: sandbox } = native;
	for (const [name, text] of [["a", "original"], ["b", "other"], ["c", "second"]]) await writeFile(path.join(cwd, `${name}.txt`), text + "\n");
	if (mutation) await compileBenchmarkHelper(cwd, { source: "reader.c", output: "reader", code: `#include <fcntl.h>\n#include <stdio.h>\n#include <sys/stat.h>\n#include <unistd.h>\nint main(void){char text[64];int f=open("a.txt",O_RDONLY),n=read(f,text,sizeof text);${metadata ? 'struct stat s;if(fstat(f,&s))return 1;dprintf(1,"%llu:%lld:%ld\\n",(unsigned long long)s.st_ino,(long long)s.st_ctim.tv_sec,s.st_ctim.tv_nsec);' : ""}close(f);for(volatile unsigned long i=0;i<600000000ul;i++);return n<0||write(1,text,n)!=n;}\n` });
	await commitBenchmarkFixture(cwd, "Sealed result reuse");
	let predict = change === "descendant" || mutation, requests = 0, drafted = 0; const prepared: string[] = [];
	const body = mutation ? `${split ? "stat --printf='%i:%z\\n' a.txt; " : ""}./reader` : "cat a.txt", command = body + (mutation ? "; printf once >> marker" : "");
	const steps = mutation ? [{ tool: "write", input: { path: "a.txt", content: "prepared\n" } }, { tool: "bash", input: { command } }]
		: [{ tool: "bash", input: { command: "cat a.txt" } }, { tool: "bash", input: { command: "cat c.txt" } }];
	const f = systemHost(cwd, [native.tool, createWriteTool(cwd)], {
		executionWorlds: [{ ...native.world, speculation: { ...native.world.speculation, execute: async context => {
			const branch = await native.world.speculation.execute(context); if (!branch.inputsOnly) prepared.push(textOutput(branch.output.result)); return branch;
		} } }, sandbox.createExecutionWorld()],
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: native.environment, shellPath: native.shellPath }),
		complete: async (_model, context) => {
			requests++;
			if (context.tools?.[0]?.name === "speculative_observations") return fauxAssistantMessage(fauxToolCall("speculative_observations", {
				results: [{ callID: "draft-0", isError: false, content: [{ type: "text", text: mutation ? "Successfully wrote 9 bytes to a.txt" : "original\n" }] }],
			}), { stopReason: "toolUse" });
			const step = steps[drafted]!;
			return fauxAssistantMessage(fauxToolCall(step.tool, step.input, { id: `draft-${drafted++}` }), { stopReason: "toolUse" });
		},
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, drafterMaxDepth: 1,
			patternAware: { enabled: false } }),
	});
	await using _cleanup = onExit(async () => { await f.dispose(); await native.dispose(); });
	const status = await native.backend.check(true); if (status.state !== "ready") return skip(status.detail);
	await f.start(); if (!predict) await f.preview("bash", { command: "cat a.txt" });
	await f.ready(predict ? 2 : 1); predict = false;
	expect(await readFile(path.join(cwd, "a.txt"), "utf8")).toBe("original\n");
	await expect(readFile(path.join(cwd, "marker"))).rejects.toMatchObject({ code: "ENOENT" });
	if (!mutation) { await f.finish(true); await f.start("next-prompt"); }
	await f.execute("write", mutation ? steps[0]!.input : { path: change === "changed" ? "a.txt" : "b.txt", content: "updated\n" });
	if (mutation) { await f.finish(); await f.start("process"); if (stale) await writeFile(path.join(cwd, "a.txt"), "fresh\n"); }
	expect(await readFile(path.join(cwd, "a.txt"), "utf8")).toBe(stale ? "fresh\n" : mutation ? "prepared\n" : change === "changed" ? "updated\n" : "original\n");
	const expected = execFileSync("/bin/bash", ["-c", body], { cwd, env: native.environment, encoding: "utf8" });
	expect(textOutput(await f.execute("bash", { command }))).toBe(expected);
	if (change === "descendant") { await f.finish(); await f.start("next"); expect(textOutput(await f.execute("bash", { command: "cat c.txt" }))).toBe("second\n"); }
	await f.finish(true);
	const actors = f.events.filter(event => event.type === "actor_action"), timing = actors.at(-1)!.computation!, fallback = metadata || split || change === "changed";
	expect(timing.hiddenComputeMs > 0).toBe(!metadata && !stale && change !== "changed"); expect(timing.toolComputeMs).toBeGreaterThanOrEqual(timing.hiddenComputeMs);
	expect(f.fallbacks, JSON.stringify(actors)).toEqual([...(mutation ? [] : ["write"]), ...(fallback ? ["bash"] : [])]);
	expect(f.events.filter(event => event.type === "candidate" && event.state.status === "running")).toHaveLength(change === "descendant" || mutation ? 2 : 1);
	if (mutation) {
		expect(requests).toBeGreaterThanOrEqual(2); expect(requests).toBeLessThanOrEqual(3); expect(actors.map(event => event.settlement.provider.kind)).toEqual(["speculative", fallback ? "actor" : "speculative"]);
		expect(prepared[0] === expected).toBe(!metadata && !split); expect(native.backend.actorMetrics().hits).toBe(Number(split && !stale)); expect(await readFile(path.join(cwd, "marker"), "utf8")).toBe("once");
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
