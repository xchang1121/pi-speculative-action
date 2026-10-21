import { access, link, mkdir, readFile, rename, rm, stat, symlink, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall, Type, fauxText, fauxThinking, type Context } from "@earendil-works/pi-ai";
import { createEditTool, createFindTool, createGrepTool, createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { bindResourceOperation } from "../src/agent-execution-world.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { ActionSemanticsRegistry, PI_ACTION_SEMANTICS, type ActionSemanticsDefinition } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import { readText, temporaryDirectories } from "./filesystem.ts";
import { expectHiddenCompute, idleResources, resourceWorld, systemHost } from "./agent-system-fixture.ts";
import { disposable, onExit, deferred, gated, nextTurn } from "./async.ts";
import { createLinuxProcessBenchmark, textOutput } from "./linux-process-fixture.ts";
import { EXACT_TOOL_OBSERVATION, type ToolObservationContract, type ToolInvocation } from "../src/tool-settlement.ts";
import { ActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
import type { AgentPlanSource, AgentRuntimeEvents } from "../src/agent-runtime-types.ts";
import { toolUse } from "./model.ts";

const writeText = (cwd: string, file: string, text: string) => writeFile(path.join(cwd, file), text);
const track = <T>(pending: Promise<unknown>[], task: Promise<T>) => { pending.push(task); void task.catch(() => {}); return task; };
const toolReply = (...args: Parameters<typeof fauxToolCall>) => fauxAssistantMessage(fauxToolCall(...args));
const resourceInvocation = (cwd: string, tool: string, input: unknown) => {
	const invocation = resolvePiToolInvocation(tool, input, { cwd, environment: {} })!;
	return { ...invocation, ...bindResourceOperation(invocation.filesystem!, cwd) };
};

const directories = temporaryDirectories("pi-agent-system-");
afterEach(directories.dispose);

test.each([[false, true], [true, true], [false, false], [true, false]])("observes and reuses work with prediction sources off (production %s, reuse %s)", async (enabled, reuseEnabled) => {
	const cwd = await directories.create(), tools = [createReadTool(cwd)], learning: boolean[] = [];
	for (const name of ["a", "b"]) await writeText(cwd, `${name}.txt`, name);
	const settings = { enabled, reuseEnabled, drafterEnabled: false, prepareObservedOperations: false,
		patternAware: { enabled: false, presets: [], minOccurrences: 1, maxContextLength: 1, maxFutureGap: 0 } };
	const store = new PatternAwareStore(patternAwareSettings(settings.patternAware)), world = resourceWorld();
	const f = systemHost(cwd, tools, { patternStore: store, resolveInvocation: (tool, input) => resourceInvocation(cwd, tool, input), executionWorlds: [{ ...world,
		observeOperations: (context, execute, observe) => { expect(context.reuseEnabled).toBe(reuseEnabled); learning.push(context.learn === true); return world.observeOperations!(context, execute, observe); },
	}] }, settings);
	await using _cleanup = onExit(f.dispose);
	let failures = 0;
	const observed: string[] = [];
	const unsubscribe = f.host.runtime.subscribe({ actor_observed: event => {
		failures++; event.concrete.path = "poison"; event.output?.result.content.splice(0); throw new Error("observer failed");
	} });
	f.host.runtime.subscribe({ actor_observed: event => { observed.push(String(event.concrete.path)); } });
	for (const turn of ["first", "second"]) {
		await f.start(turn);
		for (const name of ["a", "b"]) expect(textOutput(await f.execute("read", { path: `${name}.txt` }))).toBe(name);
		await f.finish();
		unsubscribe();
	}
	expect(failures).toBe(2);
	expect(observed).toEqual(["a.txt", "b.txt", "a.txt", "b.txt"]);
	expect(f.fallbacks).toEqual(Array(reuseEnabled ? 1 : 4).fill("read"));
	expect(learning).toEqual(Array(reuseEnabled ? 1 : 4).fill(true));
	expect(store.snapshot().length).toBeGreaterThan(0);
	expect(f.materialized).toEqual([]);
});

test.each(["global", "source", "reuse"])("keeps independent production, reuse and feedback after disabling %s", async toggle => {
	const cwd = await directories.create(), tools = [createReadTool(cwd)];
	await writeText(cwd, "input.txt", "policy");
	const settings = { enabled: true, reuseEnabled: true, drafterEnabled: true, drafterMaxDepth: 0 };
	const f = systemHost(cwd, tools, { complete: async () => toolReply("read", { path: "input.txt" }),
		resolveInvocation: (tool, input) => resourceInvocation(cwd, tool, input) }, settings);
	await using _cleanup = onExit(f.dispose);
	const feedback: AgentRuntimeEvents["prediction_feedback"][] = [], observations: string[] = [];
	f.host.runtime.subscribe({ prediction_feedback: event => { feedback.push(event); },
		actor_observed: event => { observations.push(event.tool); } });
	await f.start(); await f.ready();
	settings[toggle === "global" ? "enabled" : toggle === "source" ? "drafterEnabled" : "reuseEnabled"] = false;
	await f.host.runtime.settingsChanged({ ...settings, prepareObservedOperations: false, resourceCacheMaxEntries: 512, predictionTimeoutMs: 10_000,
		sourceConfig: { patternAware: { enabled: false } } });
	await nextTurn(); expect(feedback).toEqual([]);
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("policy"); await f.finish();
	if (toggle !== "reuse") {
		expect(f.fallbacks).toEqual([]);
		expect(f.actors[0]?.candidate?.source).toBe("drafter");
		expect(feedback).toMatchObject([{ served: true, settlement: { prediction: { source: "drafter" }, observation: "observed", match: { matched: true } } }]);
		await f.start("after"); await f.finish();
		expect(f.materialized).toHaveLength(1);
		return;
	}
	await f.start("uncached");
	await expect.poll(() => f.materialized.length).toBe(2);
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("policy"); await f.finish();
	expect(f.completed()).toHaveLength(1);
	expect(feedback).toHaveLength(2);
	expect(feedback.every(event => !event.served && event.settlement.observation === "observed")).toBe(true);
	settings.enabled = false; settings.reuseEnabled = true;
	await f.start("direct");
	for (let i = 0; i < 2; i++) expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("policy");
	await f.finish();
	expect(f.fallbacks).toEqual(["read", "read", "read"]);
	expect(observations).toEqual(["read", "read", "read", "read"]);
});

test("prepares previously observed work independently of prediction and preparation switches", async () => {
	const cwd = await directories.create(), tools = [createReadTool(cwd)], world = resourceWorld(), learning: boolean[] = [];
	const settings = { enabled: false, drafterEnabled: false, prepareObservedOperations: false };
	await writeText(cwd, "input.txt", "observed");
	const f = systemHost(cwd, tools, { speculativeExecutionWorldEnabled: () => settings.prepareObservedOperations,
		resolveInvocation: (tool, input) => resourceInvocation(cwd, tool, input),
		executionWorlds: [{ ...world,
			observeOperations: async (context, execute, observe) => {
				const output = await world.observeOperations!(context, execute, observe);
				learning.push(context.learn === true && context.enabled === true);
				if (context.action) observe([Object.freeze({ backend: world.id, identity: "observed-read", permissionHash: context.action.hash,
					semantics: PI_ACTION_SEMANTICS.definition("read")!, executionMs: 1, stale: async () => true })]);
				return output;
			},
			speculation: { ...world.speculation!, execute: async context => {
				const operation = (context.action.executionContext as ToolInvocation).operation!;
				expect(operation.permission.hash).toBe(operation.binding.permissionHash);
				return world.speculation!.execute({ ...context, action: operation.permission, args: operation.permission.input });
			} },
		}],
	}, settings);
	await using _cleanup = onExit(f.dispose);
	await f.start(); expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("observed"); await f.finish();
	expect(learning).toEqual([true]); expect(f.completed()).toEqual([]);
	settings.enabled = settings.prepareObservedOperations = true;
	await f.start("prepare"); await f.ready(1, "execution_preparation"); await f.finish();
	expect(f.completed().map(event => event.candidate.kind)).toEqual(["operation"]);
	expect(f.materialized).toEqual([]);
});

function readDefinition(tool: string): ActionSemanticsDefinition {
	return { ...PI_ACTION_SEMANTICS.definition("read")!, tool, epoch: tool, projectors: [] };
}

function readScenario(cwd: string, names: readonly string[], definitions = names.map(readDefinition)) {
	const reader = createReadTool(cwd), actionSemantics = new ActionSemanticsRegistry(definitions);
	return { reader, definitions, actionSemantics, tools: names.map(name => ({ ...reader, name })) as Parameters<typeof systemHost>[1],
		world: resourceWorld(actionSemantics),
		resolveInvocation: (tool: string, input: unknown) => ({ ...resolvePiToolInvocation(tool === "write" ? tool : "read", input, { cwd, environment: {} })!, semantics: definitions.find(definition => definition.tool === tool) }) };
}

const inputScenarios = (preview = false) => [
	...["read", "names", "stat", "type", "entry", "exists"].flatMap(input => ["", ...preview ? ["-preview"] : [], "-stale", "-copy", "-change", "-budget", "-outside", "-unkeyed"]
		.filter(suffix => ["read", "names"].includes(input) || !["-copy", "-change", ...input === "exists" ? ["-stale"] : []].includes(suffix)).map(suffix => `input-${input}${suffix}`)),
	...["read-range", "read-cancel", "read-failure", "stat-read", "entry-read", "exists-read", "stat-directory", "entry-link", "type-kind", "entry-kind",
		"stat-same-size", "entry-same-size", "exists-missing", "exists-create", "exists-deleted", "exists-broken", ...process.platform === "linux" ? ["stat-special"] : []].map(mode => `input-${mode}`),
];
const mixedScenarios = ["producer", "consumer", "stale", "failure", "budget", "outside", "revoke", "overlap", "cancel", "cancel-return", "unkeyed"].map(mode => `mixed-${mode}`);
const retainedScenarios = ["seed-dynamic", "seed-calculation", "mixed-producer", "unkeyed-dynamic", "unkeyed-calculation", "input-read", "input-names",
	"unkeyed-input-read-stale", "unkeyed-input-names-stale", "unkeyed-mixed-failure", "unkeyed-mixed-budget", "unkeyed-mixed-revoke", "unkeyed-revoke"];

const withPreview = (modes: readonly string[]) => modes.flatMap(mode => [mode, `${mode}-preview`]);

test.for([...["prediction", "preview"].flatMap(source => ["dynamic", "calculation", "input-read", "stale", "dynamic-budget", "calculation-budget", "input-read-budget"].map(mode => `live-${source}-${mode}`)),
	...["seed-dynamic", "seed-calculation", "input-read", "input-read-stale", "unkeyed-dynamic"].flatMap(mode => ["raw", "unindexed"].map(access => `${access}-${mode}`)),
	...["seed-calculation", "seed-dynamic", "input-read"].map(mode => `disabled-${mode}`),
	"actor", "preview", ...withPreview(["dynamic", "hints", "retired", "calculation", "calculation-unrelated", "calculation-hint"]), "predicted", "stale", "binding", "calculation-key",
	...withPreview(["seed-dynamic", "seed-calculation"]), "seed-stale", "seed-binding", "seed-calculation-key", "seed-unbounded", "seed-repeat", "seed-revoke",
	...mixedScenarios, "mixed-binding-unkeyed", ...inputScenarios(true), "unregistered-dynamic", "unregistered-mixed-producer", "late-dynamic",
	...[...withPreview(["seed-dynamic", "seed-calculation"]), "seed-binding", "seed-calculation-key", "mixed-producer", "mixed-consumer", "unkeyed-dynamic", "unkeyed-calculation",
		"input-read", "input-names", "input-stat-transfer", "input-exists-missing-transfer", ...process.platform === "linux" ? ["input-stat-special-transfer"] : [], "unkeyed-input-read-stale", "unkeyed-mixed-stale", "unkeyed-mixed-failure", "unkeyed-mixed-budget", "unkeyed-mixed-revoke",
		"unkeyed-mixed-overlap", "unkeyed-mixed-cancel", "revoked-unkeyed-mixed-overlap", "after:unkeyed-mixed-overlap", "after-cancel:unkeyed-mixed-overlap", "after:seed-calculation", "after-cancel-return:seed-calculation"].map(mode => `live-${mode}`),
	...[...withPreview(["dynamic", "calculation"]), "calculation-unrelated", "calculation-key", "stale", "binding", "retired", "unbounded", "repeat", "revoke", "rejected-capture", "rejected-branch", "rejected-output",
		...mixedScenarios, ...inputScenarios()].map(mode => `unkeyed-${mode}`),
	...["large", "opaque"].flatMap(output => [...retainedScenarios, "unkeyed-mixed-producer", "unkeyed-calculation-unrelated"].map(mode => `${output}-${mode}`)),
	...["after", "after-cancel", "consume", "consume-cancel", "consume-cancel-return", "before"].flatMap(failure => [...retainedScenarios,
		"unkeyed-mixed-stale", "unkeyed-mixed-cancel-return"].map(mode => `${failure}:${mode}`))])("shares actual inputs across independently formatted tools (%s)", async scenario => {
	const prefix = /^(?:(disabled)-)?(?:(raw|unindexed)-)?(?:(live)-(?:(prediction|preview)-|(revoked)-)?)?/.exec(scenario)!;
	const [, noReuse, discovery, live, source, revoked] = prefix; scenario = scenario.slice(prefix[0].length);
	const parentFailure = scenario.includes(":") ? scenario.split(":")[0] : undefined, early = parentFailure === "before", parentThrows = parentFailure && !parentFailure.endsWith("return");
	scenario = scenario.replace(/^.*:/, "");
	const parentOutput = scenario.startsWith("large-") ? { transcript: "x".repeat(128 * 1024) } : scenario.startsWith("opaque-") ? { callback: () => "parent-only" } : {};
	scenario = scenario.replace(/^(large|opaque)-/, "");
	const mode = scenario.replace(/^(seed|unkeyed|unregistered|late)-/, ""), direct = mode.startsWith("input-"), listing = mode.startsWith("input-names"), changes = mode.endsWith("change"), range = mode.endsWith("range"), preview = mode.endsWith("preview");
	const unkeyed = /^(unkeyed|unregistered|late)-/.test(scenario), unknown = scenario.startsWith("unregistered-");
	const mixed = mode.startsWith("mixed"), rejected = mode.startsWith("rejected");
	const seeded = !source && (live || unkeyed || scenario.startsWith("seed-") || direct && !preview || ["mixed-producer", "mixed-budget", "mixed-outside"].includes(mode)), bound = live || seeded || direct || mixed;
	const args = { path: "input.txt" }, cwd = await directories.create(), file = path.join(mode.endsWith("outside") ? await directories.create() : cwd, "input.txt"), shared = {};
	const metadata = /^input-(stat|type|entry|exists)/.exec(mode)?.[1], transfer = mode.endsWith("-transfer");
	if (mode.includes("-special")) execFileSync("mkfifo", [file]); else if (!mode.includes("-missing") && !mode.endsWith("-create")) await writeFile(file, "one\ntwo\n");
	if (listing) await writeText(path.dirname(file), "second.txt", "");
	let metadataTarget = file;
	if (/-(directory|link|broken)$/.test(mode)) { metadataTarget = path.join(cwd, "directory"); await mkdir(metadataTarget); if (!mode.endsWith("-directory")) {
		const alias = path.join(cwd, "alias"); await symlink(metadataTarget, alias, "junction"); if (mode.endsWith("-broken")) await rename(metadataTarget, `${metadataTarget}-retired`); metadataTarget = alias; } }
	let builds = 0, renders = 0, disposed = 0, effects = 0, borrowers = 0, bindings = 0, revoke: (() => void | Promise<void>) | undefined, revokeParent: typeof revoke;
	const pure = mode.startsWith("calculation"), unrelated = mode.includes("unrelated") || mode.startsWith("calculation-hint");
	const stale = mode.endsWith("stale") || mode.endsWith("-create") || mode.endsWith("-deleted"), cancelled = mode.endsWith("cancel"), overlap = mode === "mixed-cancel" || mode === "mixed-overlap", failed = mode.endsWith("failure");
	const unavailable = !!noReuse || early || mode.endsWith("budget") && !source || mode.endsWith("outside") || rejected, mutates = (index: number) => (direct || mixed) && index === Number(mode !== "mixed-producer");
	const entered = deferred(), joined = deferred(), release = deferred(), controller = new AbortController(), actors: Promise<unknown>[] = [];
	const parentGate = gated(), producer = new AbortController(), parentError = new Error("producer failed after independent work");
	const names = ["count_records", "summarize_records", "other_records"], read = readDefinition("read");
	const definitions = names.map((tool, index) => ({ ...read, ...(mutates(index) ? PI_ACTION_SEMANTICS.definition("write") :
		mode === "unbounded" && !index ? PI_ACTION_SEMANTICS.definition("bash") : {}), tool, epoch: `custom:${tool}`, projectors: [],
		resourceScope: mutates(index) || mode === "unbounded" && !index ? undefined : "captured_inputs" as const,
		canonicalize: (input: unknown, cwd: string) => { const key = !(index ? mode.endsWith("unkeyed") : unkeyed) && read.canonicalize(input, cwd); return key ? { ...key,
			resources: pure || index && mode.startsWith("dynamic") ? [] : index && mode.startsWith("hints") ? ["selector"] : key.resources } : undefined; } }));
	const actionSemantics = new ActionSemanticsRegistry(unknown ? definitions.slice(1) : definitions), settings = { enabled: !seeded || preview, reuseEnabled: !noReuse, drafterEnabled: source === "prediction", resourceCacheMaxBytes: 32 * 1024,
		...(live ? { scheduling: { heavyCpu: 1 } } : {}) }; // Each live call owns one slot.
	const render = (tool: string, count: unknown, actual = true) => { if (actual) renders++; return { content: [fauxText(`${tool}:${count}`)], details: { tool, ...(tool === names[0] ? parentOutput : {}) } }; };
	const count = async (read: () => Promise<unknown>, actual = true) => { const text = pure ? "one\ntwo\n" : await read(); if (actual) builds++; return metadata ? JSON.stringify(text) : String(text).trim().split("\n").length; };
	const tools = names.map(name => ({ ...createReadTool(cwd), name, execute: async () => render(name, await count(() => readFile(file))) }));
	const invocations = names.map((tool, index) => {
		const execute: NonNullable<import("../src/tool-settlement.ts").ToolInvocation["filesystem"]> = async (view, request) => {
			const actual = request.callID !== "oracle";
			const fail = (phase: string) => { if (!index && parentFailure?.startsWith(phase)) { if (parentFailure.includes("cancel")) producer.abort(parentError); if (parentThrows) throw parentError; } }; fail("before");
			const build = async (inputs: typeof view) => ({ value: await count(() => inputs.readFile(file)), bytes: 8, dispose: () => { disposed++; } });
			if (mode === "repeat" && !index) await view.prepare!(shared, "lines:v1", build, async value => value, file, request.signal);
			const consume = async (count: unknown) => {
				if (mutates(index) && overlap) { if (++borrowers === 1) entered.resolve(); else joined.resolve(); await release.promise; request.signal.throwIfAborted(); }
				if (mutates(index) && mode === "mixed-revoke" || !index && mode === "revoke") { await revoke?.(); expect(disposed).toBe(0); }
				if (actual && mutates(index)) { effects++; await view.writeFile!(path.join(cwd, `${tool}.txt`), String(count)); if (failed) throw new Error("consumer failed after writing"); }
				const result = render(tool, count, actual); if (index && mode === "mixed-cancel-return") controller.abort(); fail("consume"); return result;
			};
			const read = async () => {
				const load = async () => {
					if (!metadata || !index && mode.endsWith("-read")) return listing ? view.readdir!(path.dirname(file)) : view.readFile(file, index && range ? 4 : undefined);
					if (metadata === "exists") return view.exists!(metadataTarget);
					const value = await view.stat!(metadataTarget, metadata === "entry" || metadata === "type" ? metadata : undefined, !mode.endsWith("-kind"));
					return { directory: value.isDirectory(), size: value.size, type: value.type, link: value.link, realPath: value.realPath };
				};
				const pending = load(); if (index && cancelled) controller.abort(new Error("consumer cancelled"));
				let value = await pending;
				if (index && mode.endsWith("copy")) { if (Array.isArray(value)) value.push("changed"); else (value as Buffer).fill(0); value = await load(); }
				if (index && changes) { effects++; await view.writeFile!(listing ? path.join(path.dirname(file), "third.txt") : file, "one\ntwo\nthree\n"); value = await load(); }
				return Array.isArray(value) ? value.join("\n") : value;
			};
			const output = { isError: false, result: direct ? await consume(await count(read, actual)) : await view.prepare!(mode.includes("binding") && index || index === 2 ? definitions[index]! : shared,
				mode === "calculation-key" && index ? "next" : "lines:v1", build, consume, mode.startsWith("calculation-hint") ? path.join(cwd, `hint-${index}`) : pure ? undefined : file, request.signal) };
			if (actual && !index && live) await TimelineInterval.outside(() => parentGate.wait());
			fail("after"); return output;
		};
		const binding = bound ? bindResourceOperation(execute, cwd) : { filesystem: execute, filesystemRoot: cwd };
		return { executor: `custom:${tool}`, identity: { tool }, semantics: unknown && !index ? undefined : definitions[index], ...binding,
			...(binding.captureInputs ? { captureInputs: (...args: Parameters<NonNullable<typeof binding.captureInputs>>) => { const capture = binding.captureInputs!(...args); revoke = capture.dispose;
				if (!index) revokeParent = revoke;
				return { ...capture, seal: async () => { const branch = await capture.seal();
					expect(branch).not.toHaveProperty("output"); await expect(branch.commit()).rejects.toMatchObject({ disposition: "recoverable" }); return discovery ? { ...branch, reconstruct: undefined, inputResources: discovery === "unindexed" ? undefined : branch.inputResources } : branch;
				} }; } } : {}) };
	});
	const world = resourceWorld(actionSemantics, () => mode.endsWith("budget") && (!source || renders > 0) ? 64 : 1024 * 1024);
	if (rejected) { const capture = world.observation!.capture;
		Object.assign(world.observation!, { capture: async (context: Parameters<typeof capture>[0]) => {
			const result = await capture(context);
			return mode === "rejected-capture" ? { ...result, inputsOnly: undefined } : { ...result, seal: async (output: Parameters<typeof result.seal>[0]) => ({ ...await result.seal(output), ...(mode === "rejected-output" ? { output } : { inputsOnly: undefined }) }) };
		} }); }
	const f = systemHost(cwd, tools, { actionSemantics, resolveInvocation: tool => { if (tool === names[0]) bindings++; return invocations[names.indexOf(tool)]; },
		complete: async () => toolUse(names[source ? 0 : unrelated ? 2 : 1]!, args),
		executionWorlds: [world] }, settings);
	await using _cleanup = onExit(async () => { controller.abort(); release.resolve(); parentGate.release(); await Promise.allSettled(actors); await f.dispose(); expect(disposed).toBe(direct ? 0 : builds - (bound ? 0 : f.fallbacks.length)); });
	const reference = (index: number, count: number) => metadata ? f.oracle(names[index]!, args, undefined, undefined, invocations[index]) : render(names[index]!, count, false);
	const initial = await reference(0, 2);
	const late = scenario.startsWith("late-") ? tools.shift() : undefined; await f.start(); if (late) tools.unshift(late);
	if (live) {
		if (source !== "prediction") track<unknown>(actors, f[source ? "preview" : "execute"](names[0]!, args, producer.signal));
		await parentGate.entered;
	}
	else if (parentThrows) await expect(f.execute(names[0]!, args, producer.signal)).rejects.toBe(parentError);
	else if (seeded) expect(await f.execute(names[0]!, args, producer.signal)).toEqual(initial);
	else { await f.preview(names[0]!, args); await f.ready(); }
	expect(builds).toBe(Number(!early)); expect(renders).toBe(builds);
	if (mode.startsWith("retired") || mode === "predicted") { await f.finish(); tools.shift(); settings.drafterEnabled = mode === "predicted"; await f.start("consumer"); }
	if (mode === "predicted") await f.ready(2);
	if (unrelated) { await f.finish(); settings.enabled = settings.drafterEnabled = true; await f.start("unrelated"); await f.ready(seeded ? 1 : 2); await f.finish(); settings.drafterEnabled = false; await f.start("consumer"); }
	if (preview && !direct) { await f.preview(names[1]!, args); await expect.poll(() => renders).toBe(unrelated ? 3 : 2); }
	if (stale) await writeFile(listing ? path.join(path.dirname(file), "third.txt") : file, "one\ntwo\nthree\n");
	if (mode.endsWith("-same-size")) await writeFile(file, "new\none\n");
	if (mode.endsWith("-deleted")) await rm(file);
	const expected = await reference(1, stale || changes ? 3 : range ? 1 : 2);
	const result = track(actors, f.execute(names[1]!, args, controller.signal));
	const finishParent = async () => {
		parentGate.release();
		if (source) { if (!direct) await f.ready(); }
		else if (parentThrows) await expect(actors[0]).rejects.toBe(parentError);
		else expect(await actors[0]).toEqual(initial);
	};
	if (overlap) {
		await entered.promise; const sibling = f.execute(names[1]!, args); actors.push(sibling); await joined.promise;
		if (revoked) { actors.push(Promise.resolve(revokeParent!())); await nextTurn(); }
		if (live && parentFailure) await finishParent();
		expect(disposed).toBe(0); if (cancelled) controller.abort(new Error("consumer cancelled")); release.resolve();
		expect(textOutput(await sibling)).toBe("summarize_records:2");
	}
	if (failed || cancelled) await expect(result).rejects.toThrow(failed ? "consumer failed after writing" : "consumer cancelled");
	else expect(await result).toEqual(expected);
	expect({ builds, renders, effects }).toEqual({ builds: (direct || stale || unavailable || mode.includes("binding") || mode === "calculation-key" || mode === "revoke" ? 2 : 1) + Number(unrelated) - Number(direct && cancelled) - Number(early),
		renders: 2 + Number(unrelated) + Number(overlap) - Number(failed || cancelled) - Number(early), effects: Number(direct || mixed) + Number(changes) + Number(overlap && !cancelled) - Number(direct && cancelled) - Number(early && mutates(0)) });
	expect(f.fallbacks).toEqual([...(seeded ? [names[0]!] : []), ...(noReuse || discovery || live && !preview || early || mutates(1) || mode === "revoke" || rejected ? Array(1 + Number(overlap)).fill(names[1]!) : [])]);
	if (direct && cancelled) await expect(readFile(path.join(cwd, `${names[1]}.txt`))).rejects.toMatchObject({ code: "ENOENT" });
	else if ((direct || mixed) && !(early && mutates(0))) expect(await readText(cwd, `${names[Number(mode !== "mixed-producer")]}.txt`)).toBe(metadata ? textOutput(expected).slice(names[1]!.length + 1) : String(stale || changes ? 3 : range ? 1 : 2));
	if (transfer) { await revokeParent!(); expect(await f.execute(names[1]!, args)).toEqual(expected); }
	if (live) {
		if (!parentFailure || !overlap) expect(f.actors.some(event => event.settlement.tool === names[0])).toBe(false);
		await finishParent();
	}
	const parent = f.actors.find(event => event.settlement.tool === names[0]);
	if (unkeyed) { expect(bindings).toBe(1); expect(parent).toMatchObject({ settlement: { provider: { kind: "actor", cause: { code: "action_not_keyable" } } } }); }
	if (parentFailure) expect(parent).toMatchObject({ settlement: { provider: { kind: "actor", isError: !!parentThrows } }, computation: { hiddenComputeMs: 0 } });
	await f.finish();
	const timing = f.actors.filter(event => event.settlement.tool === names[1]).at(-1)?.computation;
	if (mode !== "binding") expectHiddenCompute(timing!, !stale && !unavailable && (!failed || direct) && !(direct && cancelled) && !["calculation-key", "revoke"].includes(mode));
	if (cancelled) expectHiddenCompute(f.actors[0]!.computation!, false);
});

test.for(["read", "weak", "full", "invalidated", "directory", "directory-full", "missing", ...process.platform === "linux" ? ["special"] : []])("retains only independently proven metadata (%s)", async mode => {
	const cwd = await directories.create(), target = path.join(cwd, "input");
	if (mode.startsWith("directory")) await mkdir(target); else if (mode === "special") execFileSync("mkfifo", [target]); else if (mode !== "missing") await writeFile(target, "one\ntwo\n");
	const manager = new ResourceVersionManager(cwd, { watch: false }), version = await manager.capture(undefined, 32 * 1024), view = version.view!;
	await using _original = onExit(async () => { await version.release(); });
	await view[mode === "read" ? "readFile" : mode === "weak" || mode === "missing" ? "exists" : mode.startsWith("directory") ? "readdir" : "stat"](target);
	if (["weak", "full", "invalidated", "directory-full"].includes(mode)) await view[mode === "directory-full" ? "stat" : "readFile"](target);
	if (mode === "invalidated") view.invalidate(new Set([...version.observations].filter(([, value]) => value.scope === "content").map(([key]) => key)));
	view.seal();
	if (mode === "weak") await view.evaluate(input => input.stat(target), dependencies => { expect([...dependencies ?? []].map(key => version.observations.get(key)?.scope)).toContain("content"); });
	const retained = manager.retain({ ...version, observations: new Map([...version.observations].filter(([, value]) => mode === "read" || value.scope !== "content")) }, 32 * 1024);
	await using _retained = onExit(async () => { await retained.release(); }); await version.release();
	expect(retained.view).toBeDefined(); expect((await manager.validate(retained)).expired).toBe(false);
	expect(await retained.view!.exists(target)).toBe(mode !== "missing");
	if (["weak", "directory", "missing"].includes(mode)) await expect(retained.view!.stat(target)).rejects.toThrow("resource_access_unproven");
	else { const actual = await retained.view!.stat(target), expected = await stat(target);
		expect({ directory: actual.isDirectory(), size: actual.size, type: actual.type }).toEqual({ directory: expected.isDirectory(), size: expected.size,
			type: mode === "special" ? "special" : expected.isDirectory() ? "directory" : "file" }); }
});

test.for(["stale", "all-stale", "binding", "failure", "cancel", "composed", "revoke",
	...["preview", "fresh", "stale", "all-stale", "alias", "failure", "cancel", "composed", "revoke"].map(mode => `input-${mode}`)])("selects valid preparations across live producers (%s)", async mode => {
	const input = mode.startsWith("input-"); mode = mode.replace(/^input-/, "");
	const cwd = await directories.create(), file = path.join(cwd, "input.txt"), extra = path.join(cwd, "extra.txt"), binding = {};
	await writeFile(file, "one\ntwo\n"); await writeFile(extra, "before\n");
	const alias = path.join(cwd, "current"), args = { path: mode === "alias" ? "current" : "input.txt" };
	if (mode === "alias") { for (const name of ["old", "new"]) await mkdir(path.join(cwd, name)); await symlink(path.join(cwd, "old"), alias, "junction"); }
	const names = ["prepare_count", "prepare_summary", "consume_count"], { definitions, actionSemantics, tools, world } = readScenario(cwd, names,
		names.map(tool => ({ ...readDefinition(tool), ...(input && !(["preview", "alias"].includes(mode) && tool === names[2]) ? { canonicalize: () => undefined } : {}) })));
	const buildGates = [gated(), gated()], parentGates = [gated(), gated()];
	const controller = new AbortController(), failure = new Error("consumer stopped after its effect"), wrapper = {};
	let builds = 0, disposals = 0, effects = 0, compositions = 0, compositionDisposals = 0, revoke: (() => void | Promise<void>) | undefined;
	const invocations = names.map((tool, index) => ({ semantics: definitions[index], executor: tool, ...bindResourceOperation(async (view, request) => {
		const consume = async (count: number) => { if (index === 2) { effects++; if (mode === "cancel") controller.abort(failure); if (mode === "failure" || mode === "cancel") throw failure; } return count; };
		const calculate = (inputs: typeof view, consume: (count: number) => Promise<number>) => inputs.prepare!(input || mode === "binding" && index === 1 ? definitions[index]! : binding, "line-count", async inputs => {
			const count = input ? (await inputs.stat!(mode === "alias" ? alias : file)).size ?? 0 : (await inputs.readFile(file)).toString().trim().split("\n").length;
			if (!input && !index) await inputs.readFile(extra);
			builds++; if (index < 2) await buildGates[index]!.wait();
			return { value: count, bytes: 8, dispose: () => { disposals++; } };
		}, consume, file, request.signal);
		const count = input && index === 1 ? (await view.readFile(file)).length : index === 2 && mode === "composed" ? await view.prepare!(wrapper, "render", async inputs => {
			const value = await calculate(inputs, async count => count); compositions++; return { value, bytes: 8, dispose: () => { compositionDisposals++; } };
		}, consume, file, request.signal) : await calculate(view, consume);
		if (index < 2) await parentGates[index]!.wait();
		return { result: { content: [fauxText(`${tool}:${count}`)], details: {} }, isError: false };
	}, cwd) }));
	const capture = invocations[0]!.captureInputs!;
	invocations[0]!.captureInputs = (...args) => { const result = capture(...args); revoke = result.dispose; return result; };
	const parents: Promise<unknown>[] = [];
	const f = systemHost(cwd, tools, { actionSemantics, resolveInvocation: tool => invocations[names.indexOf(tool)],
		resources: idleResources(3),
		executionWorlds: [world],
	}, { maxConcurrentActions: 3, scheduling: { heavyCpu: 1 } });
	await using _cleanup = onExit(async () => { for (const gate of [...buildGates, ...parentGates]) gate.release(); await Promise.allSettled(parents); await f.dispose(); expect(disposals).toBe(builds); expect(compositionDisposals).toBe(compositions); });
	await f.start();
	for (let index = 0; index < 2; index++) {
		parents.push(f.execute(names[index]!, args));
		if (!input || !index) await buildGates[index]!.entered;
		if (input) { buildGates[index]!.release(); await parentGates[index]!.entered; if (!index && mode !== "fresh") await writeFile(file, "one\ntwo\nthree\n"); }
		else if (!index) { buildGates[index]!.release(); await parentGates[index]!.entered; await writeFile(extra, "intermediate\n"); }
	}
	if (mode === "alias") { await rename(alias, path.join(cwd, "retired")); await symlink(path.join(cwd, "new"), alias, "junction"); }
	for (let index = 0; index < 2; index++) { buildGates[index]!.release(); await parentGates[index]!.entered; }
	expect(builds).toBe(input ? 1 : 2); await writeFile(extra, "after\n");
	if (mode === "all-stale") await writeFile(file, "one\ntwo\nthree\n");
	if (mode === "revoke") await revoke!();
	if (input) {
		// The older metadata owner finishes last, ahead of the valid payload in recency order.
		for (const index of [1, 0]) { parentGates[index]!.release(); await parents[index]; }
		if (mode === "all-stale") await writeFile(file, "one\ntwo\nthree\nfour\n");
	}
	if (input && mode === "preview") await f.preview(names[2]!, args);
	const result = f.execute(names[2]!, args, controller.signal);
	if (mode === "failure" || mode === "cancel") await expect(result).rejects.toBe(failure);
	else expect(textOutput(await result)).toBe(`consume_count:${input ? mode === "alias" ? (await stat(alias)).size : mode === "fresh" ? 8 : mode === "all-stale" ? 19 : 14 : mode === "all-stale" ? 3 : 2}`);
	if (mode === "composed") expect(textOutput(await f.execute(names[2]!, args))).toBe(`consume_count:${input ? 14 : 2}`);
	expect(effects).toBe(mode === "composed" ? 2 : 1); expect(compositions).toBe(Number(mode === "composed"));
	expect(builds).toBe(!input && ["all-stale", "binding"].includes(mode) ? 3 : 2);
	if (input) { await f.finish(); const timing = f.actors.filter(event => event.settlement.tool === names[2]).at(-1)!.computation!;
		expectHiddenCompute(timing, !["all-stale", "alias", "failure", "cancel"].includes(mode)); }
});

test.for([...["actor", "prediction", "preview"].flatMap(source => ["pending", "completed", "pure", "stale", "binding", "budget", "composed", "failure", "build-failure"].map(mode => `${source}:${mode}`)),
	...["actor", "preview"].flatMap(source => ["cancel-producer", "cancel-consumer", "cancel-all"].map(mode => `${source}:${mode}`)), "actor:cycle-2", "actor:cycle-3"])
("joins preparations across active tool owners (%s)", async scenario => {
	const [source, mode] = scenario.split(":") as [string, string], cycle = mode.startsWith("cycle"), count = cycle ? Number(mode.slice(-1)) : 2;
	const cwd = await directories.create(), file = path.join(cwd, "input.txt"), binding = {}, wrapper = {}, args = { path: "input.txt" };
	await writeFile(file, "one\ntwo\n");
	const names = Array.from({ length: count }, (_, index) => `format_${index}`), { definitions, actionSemantics, tools, world } = readScenario(cwd, names,
		names.map(tool => ({ ...readDefinition(tool), resourceScope: "captured_inputs" })));
	const buildGate = gated(cycle ? count : 1), parentGate = gated(), emergency = deferred<number>(), controllers = names.map(() => new AbortController()), jobs: Promise<unknown>[] = [];
	const failure = new Error("producer failed after independent computation"), builds = names.map(() => 0), effects = names.map(() => 0);
	let produced = 0, disposed = 0, compositions = 0, compositionDisposals = 0, joined = false, shared: AbortSignal | undefined, preparation: { borrowers: number } | undefined;
	const resource = (value: number) => { produced++; return { value, bytes: mode === "budget" ? 2 * 1024 * 1024 : 8, dispose: () => { disposed++; } }; };
	const invocations = names.map((tool, index) => ({ semantics: definitions[index], executor: tool, ...bindResourceOperation(async (view, request) => {
		const read = async (inputs: typeof view) => mode === "pure" ? 2 : (await inputs.readFile(file)).toString().trim().split("\n").length;
		const consume = async (value: number) => { effects[index]!++; return value; };
		const calculate = (inputs: typeof view, consume: (value: number) => Promise<number>) => inputs.prepare!(index && mode === "binding" ? definitions[index]! : binding, cycle ? String(index) : "count", async (inputs, signal) => {
			let value = await read(inputs); builds[index]!++;
			if (!index || cycle) {
				if (!index) { shared = signal; let owner: object = inputs;
					while (Reflect.get(owner, "owner")) owner = Reflect.get(owner, "owner");
					preparation = Reflect.get(owner, "prepared")?.bindings.get(binding)?.get("count"); }
				await TimelineInterval.outside(() => buildGate.wait());
				if (mode === "build-failure") throw failure;
				if (cycle) {
					const pending = inputs.prepare!(binding, String((index + 1) % count), async inputs => resource(await read(inputs)), async value => value, file, request.signal);
					// Failed liveness checks still release all owners using an independently read value.
					value = await Promise.race([pending, emergency.promise.then(() => value)]);
				}
			}
			return resource(value);
		}, consume, mode === "pure" ? undefined : file, request.signal);
		const value = index && mode === "composed" ? await view.prepare!(wrapper, "render", async inputs => {
			const value = await calculate(inputs, async count => count); compositions++;
			return { value, bytes: 8, dispose: () => { compositionDisposals++; } };
		}, consume, file, request.signal) : await calculate(view, consume);
		if (!index && !cycle) { await TimelineInterval.outside(() => parentGate.wait()); if (mode === "failure") throw failure; }
		return { result: { content: [fauxText(`${tool}:${value}`)], details: {} }, isError: false };
	}, cwd) }));
	let requests = 0;
	const f = systemHost(cwd, tools, { actionSemantics, resources: idleResources(8), resolveInvocation: tool => invocations[names.indexOf(tool)], executionWorlds: [world],
		complete: async () => requests++ ? fauxAssistantMessage("done") : toolUse(names[0]!, args),
	}, { drafterEnabled: source === "prediction", maxConcurrentActions: 4, scheduling: { heavyCpu: 1 } });
	await using _cleanup = onExit(async () => { buildGate.release(); parentGate.release(); emergency.resolve(2); for (const controller of controllers) controller.abort();
		await Promise.allSettled(jobs); await f.dispose(); expect(disposed).toBe(produced); expect(compositionDisposals).toBe(compositions); });
	await f.start();
	if (cycle) {
		const actors = names.map((tool, index) => track(jobs, f.execute(tool, args, controllers[index]!.signal))), timeout = new AbortController();
		await buildGate.entered; buildGate.release();
		const completed = await Promise.race([Promise.all(actors), delay(2_000, undefined, { signal: timeout.signal }).catch(() => undefined)]); timeout.abort();
		expect(completed, "crossed preparations must progress before emergency completion").toBeDefined();
		expect(completed!.map(textOutput)).toEqual(names.map(tool => `${tool}:2`)); expect(effects).toEqual(names.map(() => 1)); expect(produced).toBe(count + 1);
		return;
	}
	const producer = source === "prediction" ? undefined : track<unknown>(jobs, f[source === "actor" ? "execute" : "preview"](names[0]!, args, controllers[0]!.signal));
	await buildGate.entered;
	if (mode === "completed") { buildGate.release(); await parentGate.entered; }
	const consumer = track(jobs, f.execute(names[1]!, args, controllers[1]!.signal));
	if (mode !== "completed") {
		await expect.poll(() => { joined ||= (preparation?.borrowers ?? 0) > 1; return joined || builds[1]! > 0; }).toBe(true); expect(joined).toBe(mode !== "binding");
		if (mode === "stale") await writeFile(file, "one\ntwo\nthree\n");
		if (mode === "cancel-producer" || mode === "cancel-all") controllers[0]!.abort(failure);
		if (mode === "cancel-consumer" || mode === "cancel-all") controllers[1]!.abort(failure);
		expect(shared?.aborted).toBe(mode === "cancel-all" && source === "actor"); buildGate.release(); parentGate.release();
	}
	if (controllers[1]!.signal.aborted) await expect(consumer).rejects.toBe(failure);
	else expect(textOutput(await consumer)).toBe(`${names[1]}:${mode === "stale" ? 3 : 2}`);
	parentGate.release();
	if (source === "actor" && (controllers[0]!.signal.aborted || mode === "failure" || mode === "build-failure")) await expect(producer).rejects.toBe(failure); else await producer;
	if (source !== "actor") await expect.poll(() => f.events.some(event => event.type === "candidate" && ["succeeded", "failed", "cancelled"].includes(event.state.status))).toBe(true);
	if (mode === "composed") {
		expect(textOutput(await f.execute(names[1]!, args))).toBe(`${names[1]}:2`); expect({ builds, compositions }).toEqual({ builds: [1, 0], compositions: 1 });
		await writeFile(file, "one\ntwo\nthree\n");
		expect(textOutput(await f.execute(names[1]!, args))).toBe(`${names[1]}:3`); expect({ builds, compositions }).toEqual({ builds: [1, 1], compositions: 2 });
	} else {
		expect(builds[1]).toBe(Number(["stale", "binding", "build-failure"].includes(mode)));
		expect(effects).toEqual([Number(mode !== "build-failure" && (source === "actor" ? !controllers[0]!.signal.aborted : mode !== "stale")), Number(!controllers[1]!.signal.aborted)]);
	}
	await f.finish();
	if (!controllers[1]!.signal.aborted && !["stale", "binding", "build-failure", "composed"].includes(mode)) expectHiddenCompute(f.actors.find(event => event.settlement.tool === names[1])!.computation!, true);
});

test.for(["reopened", "inflight", "completed", "binding", "reverse", "deep", "added", "changed", "removed", "shared", "shared-refresh", "shared-stale", "shared-unused", "shared-native", "shared-native-stale", "shared-exact", "shared-exact-stale",
	...["", "-stale", "-failure-before", "-cancel-before", "-failure-after", "-cancel-after", "-close", "-terminal", "-disable", "-dispose", "-poison"].map(suffix => `shared-exact-fallback${suffix}`)])("retains learned continuations across authoritative batch updates (%s)", async mode => {
	const changed = mode === "changed", removed = mode === "removed", replaces = changed || removed, added = mode === "added", deep = mode === "deep", reopen = mode === "reopened";
	const fallback = mode.includes("fallback"), closure = mode.match(/-(close|terminal|dispose)$/)?.[1], disabled = mode.endsWith("-disable"), poison = mode.endsWith("poison"), failed = /failure|cancel/.test(mode);
	const shared = mode.startsWith("shared"), stale = mode.endsWith("stale"), unused = mode === "shared-unused", native = mode.includes("native"), exact = mode.includes("exact"), consumed = shared && !stale && !unused && !closure && !poison && !mode.endsWith("before");
	const cwd = await directories.create(), names = [reopen ? "read" : "inspect_index", reopen ? "read" : "open_record", "render_record", replaces ? "status_index" : "background_status", "summarize_record", "tail_record"];
	const files = ["root.txt", "child.txt", "leaf.txt", "status.txt", "other.txt", "tail.txt"];
	for (const file of files) await writeText(cwd, file, `${file}\n`);
	const { actionSemantics, tools, world, resolveInvocation } = readScenario(cwd, [...new Set(names)], [...new Set(names)].map(tool => ({
		...readDefinition(tool), epoch: `learned:${tool}`, ...(native && tool === names[4] ? { canonicalize: () => undefined } : {}) })));
	const settings = patternAwareSettings({ enabled: true, minOccurrences: 2, maxContextLength: added ? 2 : 1, maxFutureGap: 0, beamWidth: added ? 2 : 1, maxPredictionDepth: 3, presets: [] });
	const createStore = () => new PatternAwareStore(settings, path.join(cwd, "patterns.json"), patternAwareActionSemantics(actionSemantics, cwd));
	const store = createStore(); await store.load();
	const options = { actionSemantics, patternStore: store, executionWorlds: [world], resolveInvocation };
	const chains = [[[0], [1], ...reopen ? [] : [[2]], ...deep ? [[5]] : []], ...changed || added ? [[added ? [3, 0] : [3], [4], [5]]] : []];
	for (const chain of chains) for (let round = 0; round < 4; round++) {
		await using training = disposable(systemHost(cwd, tools, options, { patternAware: settings }, `chain-${chain[0]}-${round}`));
		for (const batch of chain) { await training.start(`step-${batch}`); await Promise.all(batch.map(step => training.execute(names[step]!, { path: files[step] }))); await training.finish(batch === chain.at(-1)); }
	}
	await store.flush(); const reopened = createStore(); await reopened.load();
	const slowGate = gated(), leafGate = gated(), nativeGate = gated();
	const controller = new AbortController(), failure = new Error("consumer stopped"); let allowSelection = true, nativeCalls = 0, commits = 0;
	const runtimeSettings = { patternAware: settings, maxConcurrentActions: 3, scheduling: { heavyCpu: 1 } };
	let leafSignal: AbortSignal | undefined, slow: Promise<unknown> | undefined, bindings = 0;
	const f = systemHost(cwd, tools, { ...options, patternStore: reopened, preflight: () => allowSelection,
		resolveInvocation: async (tool, input) => {
			if (mode === "binding" && tool === names[2]) { bindings++; await leafGate.wait(); }
			const invocation = options.resolveInvocation(tool, input);
			return { ...invocation, ...(native && tool === names[4] ? bindResourceOperation(invocation.filesystem!, cwd) : {}), ...(fallback && tool === names[1] ? bindResourceOperation(async (view, request) => {
				if (allowSelection) return invocation.filesystem!(view, request);
				nativeCalls++; await nativeGate.wait();
				const result = mode.endsWith("before") || closure ? undefined : await invocation.filesystem!(view, request);
				if (failed || closure) { if (mode.includes("cancel")) controller.abort(failure); throw failure; }
				return result!;
			}, cwd) : {}), ...(tool === names[3] ? bindResourceOperation(async (view, request) => {
				const result = await invocation.filesystem!(view, request);
				await slowGate.wait();
				return result;
			}, cwd) : {}) };
		},
		executionWorlds: [{ ...world, speculation: { ...world.speculation!, execute: async context => {
			if (context.toolName === names[2]) { leafSignal = context.signal; await leafGate.wait(); context.signal.throwIfAborted(); }
			const branch = await world.speculation!.execute(context);
			return poison && context.toolName === names[1] ? { ...branch, commit: async () => { commits++; throw failure; } } : branch;
		} } }],
	}, runtimeSettings, "late-batch");
	const actors: Promise<unknown>[] = [];
	await using _cleanup = onExit(async () => { slowGate.release(); leafGate.release(); nativeGate.release(); await Promise.allSettled([...actors, slow]); await f.dispose(); await reopened.flush(); });
	const predictions = (step: number) => f.predictions.filter(event => event.tool === names[step]);
	await f.start();
	if (reopen) {
		await f.execute("read", { path: files[0] }); await f.finish(); await f.start("next");
		await f.ready([files[1]!], "pattern_aware"); f.fallbacks.length = 0;
		expect(textOutput(await f.execute("read", { path: files[1] }))).toBe(`${files[1]}\n`);
		expect(f.fallbacks).toEqual([]); return;
	}
	if (mode === "reverse") await f.execute(names[0]!, { path: files[0] });
	if (mode !== "shared") { slow = f.execute(names[3]!, { path: files[3] }); await slowGate.entered; }
	if (mode !== "reverse") await f.execute(names[0]!, { path: files[0] });
	if (mode === "completed" || deep) leafGate.release();
	await leafGate.entered;
	if (mode === "completed" || deep) await f.ready([files[deep ? 5 : 2]!], "pattern_aware");
	const original = leafSignal; slowGate.release(); await slow; await nextTurn();
	await f.finish();
	if (mode !== "binding") expect(original?.aborted).toBe(false);
	expect(predictions(2)).toMatchObject(replaces ? [{ settlement: { observation: "unobserved", cause: { code: "superseded" } } }] : []);
	if (deep) expect(predictions(5)).toEqual([]);
	if (mode === "binding") expect(bindings).toBe(1);
	leafGate.release(); if (mode === "binding") await f.ready([files[2]!], "pattern_aware");
	runtimeSettings.patternAware = { ...settings, enabled: false };
	const prior = reopened.snapshot().filter(pattern => pattern.targetTool === names[1]);
	if (stale) await writeText(cwd, files[1]!, "changed\n");
	f.fallbacks.length = 0;
	for (const batch of shared ? [[1]] : changed ? [[4], [5]] : added ? [[1, 4], [2, 5]] : [[1], [2], ...deep ? [[5]] : []]) {
		await f.ready(batch.map(step => files[step]!), "pattern_aware");
		await f.start(`consume-${batch}`); if (fallback && !poison) allowSelection = false;
		await Promise.all(batch.map(async step => {
			const result = track(actors, f.execute(names[shared && !exact ? 4 : step]!, { path: files[unused ? 4 : step] }, controller.signal));
			if (fallback && !poison) {
				await nativeGate.entered; await nextTurn(); expect(predictions(step)).toEqual([]);
				if (disabled) {
					await f.host.runtime.settingsChanged({ enabled: false, prepareObservedOperations: false, resourceCacheMaxEntries: 512, predictionTimeoutMs: 10_000,
						sourceConfig: { patternAware: { ...settings, enabled: false } } });
					expect(predictions(step)).toEqual([]);
				} else if (closure) {
					const closed = closure === "dispose" ? f.dispose() : f.finish(closure === "terminal");
					track(actors, closed); await expect.poll(() => predictions(step).length).toBe(1); nativeGate.release(); await closed;
				}
				nativeGate.release();
			}
			if (poison) await expect(result).rejects.toMatchObject({ disposition: "poisoned" });
			else if (failed || closure) await expect(result).rejects.toBe(failure);
			else expect(textOutput(await result)).toBe(stale ? "changed\n" : `${files[unused ? 4 : step]}\n`);
		})); await nextTurn(); if (fallback) for (const step of batch) expect(predictions(step)).toHaveLength(1); await f.finish();
		if (!removed) for (const step of batch) expect(predictions(step)).toMatchObject([{ settlement: native ? { observation: "unobserved", cause: { code: "actor_action_not_keyable" } } : { observation: "observed", match: { matched: !shared || exact } } }]);
	}
	expect(f.fallbacks).toEqual(fallback && !poison ? [names[1]] : native ? [names[4]] : []);
	if (fallback) { expect(nativeCalls).toBe(Number(!poison)); expect(commits).toBe(Number(poison));
		const timing = f.actors.filter(event => event.settlement.tool === names[1]).at(-1)?.computation;
		if (closure || poison) expect(timing).toBeUndefined(); else expectHiddenCompute(timing!, consumed); }
	if (mode !== "binding") expect(leafSignal).toBe(original);
	if (shared) {
		await f.dispose(); await reopened.flush(); const saved = createStore(); await saved.load();
		expect(prior.length).toBeGreaterThan(0);
		for (const previous of prior) expect(saved.snapshot().find(pattern => pattern.id === previous.id)?.feedback).toMatchObject({
			observed: previous.feedback.observed + Number(!native), matched: previous.feedback.matched + Number(exact), adopted: previous.feedback.adopted + Number(consumed),
			recentMismatchedWeight: Number(!consumed && !native && !exact), recentRejectedWeight: Number(exact && !consumed),
			...(native ? { unobserved: { "matching:actor_action_not_keyable": 1 } } : {}),
		});
	}
});

test.for(["refresh", "replace", "remove", "replace-shared", "remove-shared",
	...["first", "last", "both", "none", "stale", "failure-before", "failure-after", "cancel-before", "cancel-after"].map(mode => `early-${mode}`)])("settles claimed consumption after a late source update (%s)", async scenario => {
	const early = scenario.startsWith("early-"), usage = scenario.slice(6), mode = early ? "remove" : scenario.split("-")[0], shared = early || scenario.endsWith("-shared");
	const consumed = !early || !["last", "none", "stale"].includes(usage) && !usage.endsWith("before"), lateConsumed = early && ["last", "both"].includes(usage);
	const failed = early && /failure|cancel/.test(usage), controller = new AbortController(), failure = new Error("consumer failed");
	const cwd = await directories.create(), gate = gated(), background = gated(), args = { path: "input.txt" };
	for (const file of ["input.txt", "other.txt", "unused.txt", "unrelated.txt"]) await writeText(cwd, file, `${file}\n`);
	const settled: AgentRuntimeEvents["prediction_feedback"][] = [], readers: Promise<unknown>[] = [];
	const received = () => settled.filter(item => item.feedback === "before");
	let native = false, updated = false, backgroundFinished = false;
	const proposal = (after = false) => ({ id: "claimed", source: "late-source", revision: Number(after), actions: after && mode === "remove" ? [] : [{
		id: "read", type: "tool_call" as const, tool: "read", input: { path: after && mode === "replace" ? "other.txt" : args.path }, feedback: after ? "after" : "before",
	}] });
	const fork = new class extends ActorForkPlanSource {
		override readonly source: AgentPlanSource = {
			id: "late-source", enabled: () => true,
			propose: () => proposal(),
		};
	}();
	const { tools, actionSemantics, world, resolveInvocation } = readScenario(cwd, ["read", "summarize_rows", ...early ? ["background_status"] : []]);
	const f = systemHost(cwd, tools, { actionSemantics, executionWorlds: [world], actorForkPlanSource: fork, preflight: () => !native,
		resolveInvocation: (tool, input) => {
			const invocation = resolveInvocation(tool, input);
			return { ...invocation, ...bindResourceOperation(async (view, request) => {
				const foreground = native && request.callID !== "oracle" && tool !== "background_status" && (request.args as typeof args).path !== "other.txt";
				const before = foreground && early && usage.endsWith("before");
				if (before) await TimelineInterval.outside(() => gate.wait());
				const result = before ? undefined : await invocation.filesystem!(view, request);
				if (foreground && !before) await TimelineInterval.outside(() => gate.wait());
				if (tool === "background_status" && request.callID !== "oracle") { await TimelineInterval.outside(() => background.wait()); backgroundFinished = true; }
				if (foreground && failed) { if (usage.startsWith("cancel")) controller.abort(failure); throw failure; }
				return result!;
			}, cwd) };
		},
	});
	f.host.runtime.subscribe({
		actor_observed: async ({ startInput, concrete }) => {
			if (concrete.path !== "other.txt") return;
			updated = true; await f.host.runtime.submitPlan(startInput, proposal(true));
		},
		prediction_feedback: feedback => { settled.push(feedback); },
	});
	await using _cleanup = onExit(async () => { gate.release(); background.release(); await Promise.allSettled(readers); await f.dispose(); });
	await f.start(); await f.ready(); native = true;
	const producer = f.completed()[0]!.candidate.id, backgroundArgs = { path: lateConsumed ? args.path : "unrelated.txt" };
	if (early && usage === "stale") await writeText(cwd, args.path, "fresh\n");
	const slow = early ? track(readers, f.execute("background_status", backgroundArgs)) : undefined; if (early) await background.entered;
	const tool = shared ? "summarize_rows" : "read", input = early && ["last", "none"].includes(usage) ? { path: "unused.txt" } : args;
	const result = track(readers, f.execute(tool, input, controller.signal));
	await gate.entered; await nextTurn(); expect(settled).toEqual([]);
	expect(textOutput(await f.execute("read", { path: "other.txt" }))).toBe("other.txt\n");
	await expect.poll(() => updated).toBe(true); await nextTurn(); expect(received()).toEqual([]);
	gate.release(); if (failed) await expect(result).rejects.toBe(failure); else expect(await result).toEqual(await f.oracle(tool, input));
	if (early) {
		await nextTurn(); expect(backgroundFinished).toBe(false);
		if (consumed) await expect.poll(() => received().length).toBe(1);
		else expect(received()).toEqual([]);
		expect(backgroundFinished).toBe(false); background.release(); expect(await slow).toEqual(await f.oracle("background_status", backgroundArgs));
	}
	await f.finish(true); expect(received()).toMatchObject([{
		served: consumed || lateConsumed, settlement: !shared ? { observation: "observed", match: { matched: true, adoption: { status: "rejected" } } }
			: { observation: "unobserved", cause: { code: "superseded" } },
	}]);
	if (!early) expectHiddenCompute(f.actors.at(-1)!.computation!, true);
	else for (const [name, used] of [[tool, consumed], ["background_status", lateConsumed]] as const) {
		const event = f.actors.find(event => event.settlement.tool === name)!; expectHiddenCompute(event.computation!, used);
		expect(TimelineInterval.serialize(event.settlement.provider.toolExecution)!.nodes.some(node => node.producer?.id?.endsWith(":" + producer))).toBe(used);
	}
});

test.for([false, true])("reconstructs shared searches after cancellation and namespace changes (cancel all: %s)", async (cancelAll, { skip }) => {
	const cwd = await directories.create();
	for (const [file, text] of [["a.txt", "one\ntwo\nthree\n"], [".ignore", "ignored.txt\n"], ["ignored.txt", "two ignored\n"]]) await writeText(cwd, file, text);
	const profile = await createClosedSearchProfile(cwd);
	if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg unavailable"); }
	const gate = gated(), joined = deferred(), controllers = [new AbortController(), new AbortController()];
	let holding = false, borrowers = 0, builds = 0, shared: AbortSignal | undefined;
	const grep = profile.invocations.get("grep")!, actors: Promise<unknown>[] = [];
	const invocation: typeof grep = { ...grep, filesystem: (view, request) => grep.filesystem!({ ...view,
		prepare: (binding, key, build, consume, target, signal) => {
			if (holding && target === path.join(cwd, "a.txt") && ++borrowers === 2) joined.resolve();
			return view.prepare!(binding, key, async (inputs, signal) => {
				if (holding && target === path.join(cwd, "a.txt")) { builds++; shared = signal; await gate.wait(); }
				return build(inputs, signal);
			}, consume, target, signal);
		},
	}, request) };
	const tools = [createGrepTool(cwd), createFindTool(cwd), createLsTool(cwd), createReadTool(cwd)];
	const f = systemHost(cwd, tools, {
		resolveInvocation: (tool, input) => tool === "grep" ? invocation : profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
	});
	await using _cleanup = onExit(async () => { for (const controller of controllers) controller.abort(); gate.release(); await Promise.allSettled(actors); await f.dispose(); await profile.pool.dispose(); });
	await f.start(); await f.preview("grep", { pattern: "one", path: "." }); await f.ready();
	await f.finish(); await f.start("queries");
	holding = true;
	actors.push(f.execute("grep", { pattern: "two", path: ".", limit: 1 }, controllers[0]!.signal).catch(error => error)); await gate.entered;
	actors.push(f.execute("grep", { pattern: "two", path: ".", limit: 2 }, controllers[1]!.signal).catch(error => error)); await joined.promise; await nextTurn();
	controllers[0]!.abort(); if (cancelAll) controllers[1]!.abort();
	expect(shared?.aborted).toBe(cancelAll); gate.release();
	const results = await Promise.all(actors); holding = false;
	if (!cancelAll) expect(results[1]).toEqual(await f.oracle("grep", { pattern: "two", path: ".", limit: 2 }));
	expect(builds).toBe(1); expect(f.fallbacks).toEqual(cancelAll ? ["grep", "grep"] : ["grep"]); f.fallbacks.length = 0;
	const queries = [
		{ tool: "grep", input: { pattern: "two", path: "." } },
		{ tool: "find", input: { pattern: "*.txt", path: "." } },
		{ tool: "read", input: { path: "a.txt", offset: 2, limit: 1 } },
	];
	const verify = async () => { for (const query of queries) expect(await f.execute(query.tool, query.input)).toEqual(await f.oracle(query.tool, query.input)); };
	await verify();
	expect(f.fallbacks).toEqual([]);
	await writeText(cwd, "a.txt", "two changed\n");
	await writeText(cwd, "added.txt", "two new\n");
	await verify();
});

test.for(["in-place", "git", "single-in-place", "single-git", "checkpoint-git", "single-checkpoint-git"])("keeps effects private, retries restored preimages and transfers an aborted claim (%s)", async driver => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	const alias = driver.startsWith("single-") ? "notes.txt" : "alias.txt", read = () => readText(file);
	await writeFile(file, "one two\n"); if (alias !== "notes.txt") await link(file, path.join(cwd, alias));
	const gate = gated(), nativeGate = gated(), secondResolved = deferred(), controller = new AbortController();
	const native = createEditTool(cwd, { operations: { access, readFile, writeFile: async (...args) => { await nativeGate.wait(); return writeFile(...args); } } });
	let hold = false, waiting = false, first: Promise<unknown> | undefined, second: Promise<unknown> | undefined;
	const f = systemHost(cwd, [createEditTool(cwd), createReadTool(cwd)], {
		executionWorlds: [sandbox.createExecutionWorld(driver.endsWith("git") ? { driver: "git", inPlaceMutations: false } : {})],
		preflight: async () => { if (hold) { hold = false; waiting = true; await gate.wait(); } return true; },
		resolveInvocation: (tool, input) => { if (waiting) secondResolved.resolve(); return resolvePiToolInvocation(tool, input, { cwd, environment: {} }); },
	});
	try {
		const original = (await stat(file, { bigint: true })).ino;
		let parent: ReturnType<typeof sandbox.checkpoint>;
		for (const content of ["raw write\n", "one two\n"]) {
			const before = await read(), captured = await sandbox.capture({ cwd, driver: "git", execute: async workspace => {
				await writeFile(path.join(workspace.sandboxRoot, "notes.txt"), content);
				return { result: { content: [], details: {} }, isError: false };
			} }, parent);
			expect(await read()).toBe(before);
			if (driver.includes("checkpoint")) {
				const branch = sandbox.branch({ cwd, action: PI_ACTION_SEMANTICS.buildKey("write", { path: "notes.txt", content }, cwd, "checkpoint")! }, captured);
				await branch.commit(); parent = sandbox.checkpoint(branch.checkpoint, cwd); await branch.dispose();
			} else await sandbox.commitDelta(captured);
			for (const name of new Set([file, path.join(cwd, alias)])) expect((await stat(name, { bigint: true })).ino).toBe(original);
		}
		await f.start();
		const args = { path: "notes.txt", edits: [{ oldText: "two", newText: "three" }] };
		await f.preview("edit", args); await f.ready();
		expect(await read()).toBe("one two\n");
		await f.finish(true); await f.start("changed-input");
		first = native.execute("native", { path: alias, edits: [{ oldText: "one", newText: "changed" }] }); await nativeGate.entered;
		second = f.execute("edit", args); await new Promise(resolve => setTimeout(resolve, 200));
		nativeGate.release(); await first; await second;
		expect(await read()).toBe("changed three\n");
		expect(textOutput(await f.execute("read", { path: alias }))).toBe("changed three\n");
		await writeFile(file, "one two\n"); hold = true;
		first = f.execute("edit", args, controller.signal).catch(error => error); await gate.entered;
		second = f.execute("edit", args); await secondResolved.promise; await nextTurn();
		expect(f.fallbacks.filter(tool => tool === "edit")).toEqual(["edit"]); expect(await read()).toBe("one two\n");
		controller.abort(); gate.release(); await first; await second;
		expect(await read()).toBe("one three\n"); expect(f.fallbacks.filter(tool => tool === "edit")).toEqual(["edit", "edit"]);
		await f.finish(); await f.start("discard");
		await f.preview("edit", { path: "notes.txt", edits: [{ oldText: "three", newText: "discarded" }] });
		await f.ready(2);
	} finally { controller.abort(); gate.release(); nativeGate.release(); await Promise.allSettled([first, second]); await f.dispose(); await sandbox.dispose(); }
	expect(await read()).toBe("one three\n");
});

test.for(["independent", "unchanged", "batch", "batch-known", "mismatch-text", "mismatch-error", "mismatch-inflight", "mismatch-budget", "parent-miss", "stale", "same-resource", "missing-input", "independent-create",
	"budget", "depth-zero", "denied-root", "denied-child", "unknown-batch", "filtered-batch", "malformed-observation", "malformed-type", "abort-observation", "actual-before-forecast", "actual-before-malformed",
	"actual-before-failure", "actual-before-batch", "request-failure", "malformed-extra", "mismatch-exit", "canonical", "canonical-mismatch", "canonical-version", "canonical-schema", "canonical-unbound", "normalizer-failure",
	...["whole", "fallback", "unused", "stale"].map(mode => `learning-retired-${mode}`),
	...["", ...process.platform !== "win32" ? ["captured-"] : []].flatMap(prefix => ["whole", "fallback", "cross-tool", "unkeyed", "unused", "stale", "unkeyed-unused", "unkeyed-stale"].map(mode => `learning-${prefix}${mode}`))])("executes out-of-order drafts using private observations (%s)", async mode => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), release = deferred(), forecast = deferred(), observing = deferred(), firstResult = deferred();
	const learning = mode.startsWith("learning-"), captured = mode.startsWith("learning-captured-"), cross = learning && /cross-tool|unkeyed/.test(mode), stale = mode.endsWith("stale"), unused = mode.endsWith("unused");
	const retired = mode.startsWith("learning-retired-");
	let allow = true, seeding = captured, changed = false;
	const actualFirst = mode.startsWith("actual-before-"), normalized = mode.startsWith("canonical"), wrong = mode.startsWith("mismatch") || mode === "canonical-mismatch";
	const known = mode === "batch-known", recoverable = ["request-failure", "malformed-observation", "malformed-type", "malformed-extra"].includes(mode);
	const extraCall = ["unknown-batch", "filtered-batch"].includes(mode);
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
	const names = ["inspect_rows", "render_rows", ...learning ? ["format_rows"] : []];
	const { actionSemantics: semantics, tools, world, reader, resolveInvocation } = readScenario(cwd, names, [...names.map((tool, index) => ({ ...readDefinition(tool),
		...(index === 2 && mode.includes("unkeyed") ? { canonicalize: () => undefined } : {}) })), PI_ACTION_SEMANTICS.definition("write")!]);
	tools.push(createWriteTool(cwd));
	const roots = Array.from({ length: count }, (_, index) => ({ tool: mutation ? "write" : names[0]!, args: mutation ? { path: "root-0.txt", content: "prepared\n" }
		: { path: `root-${index}.txt`, offset: normalized ? 1 : 2, limit: normalized ? 10 : 1 } }));
	for (const root of roots) if (mode !== "missing-input" && mode !== "independent-create") await writeText(cwd, root.args.path, normalized
		? '{"failedTests":[],"artifact":"abc","duration":14,"pid":123,"tempPath":"/tmp/actual"}\n' : "one\ntwo\nthree\n");
	await writeText(cwd, "child.txt", "child\n");
	if (learning) await writeText(cwd, "unused.txt", "unused\n");
	if (wrong) await writeText(cwd, "discarded.txt", "discarded\n");
	const child = { tool: names[1]!, args: { path: dependent ? "root-0.txt" : "child.txt" } };
	const expected = await Promise.all(roots.map(root => mutation ? { content: [fauxText("Successfully wrote 9 bytes to root-0.txt")] }
		: reader.execute("oracle", root.args)));
	const rootDraft = [fauxThinking("Inspect the input."), ...roots.map((root, index) => fauxToolCall(root.tool, root.args, { id: `root-${index}` }))];
	const resultMessages = (predicted = false) => expected.map((result, index) => expect.objectContaining({ role: "toolResult", toolCallId: `root-${index}`,
		content: normalized ? predicted ? [fauxText(JSON.stringify({ failedTests: mode === "canonical-mismatch" ? ["a.test.ts:3"] : [], artifact: "abc" }))] : canonicalize(result.content)
			: predicted && textMismatch ? [fauxText("incorrect forecast")] : result.content, isError: predicted && mode === "mismatch-error" }));
	if (normalized) expect(canonicalize(expected[0]!.content)).toEqual([fauxText('{"failedTests":[],"artifact":"abc"}')]);
	let requests = 0, drafts = 0, rootContext: Context | undefined, observationSignal: AbortSignal | undefined;
	const privateContexts: Context[] = [], executed: string[] = [];
	const settings = { tools: mode === "filtered-batch" ? [names[0]!] : undefined, drafterEnabled: !seeding, maxConcurrentActions: 3, drafterMaxDepth: mode === "depth-zero" ? 0 : mode === "denied-child" ? 2 : 1,
		candidateLimit: mode === "unchanged" ? 3 : 1, predictionTimeoutMs: 30_000, drafterTaskMaxRequests: mode === "budget" ? 2 : repairBudget ? 3 : 32 };
	const f = systemHost(cwd, tools, { actionSemantics: semantics,
		resources: idleResources(3),
		onEvent: event => { if (known && event.type === "candidate" && event.state.status === "succeeded" && event.candidate.tool === names[0]) firstResult.resolve(); },
		resolveInvocation: async (tool, args) => { if (known && (args as { path: string }).path === "root-1.txt") await firstResult.promise;
			const invocation = resolveInvocation(tool, args);
			return { ...invocation, ...(learning && !seeding ? bindResourceOperation(retired && tool === names[0] ? async (view, request) => {
				const result = await invocation.filesystem!(view, request), text = textOutput(result.result);
				return text === textOutput(expected[0]!) ? result : { ...result, result: { ...result.result,
					content: [fauxText(text + (unused ? "" : String(await view.readFile(path.join(cwd, "child.txt")))))] } };
			} : invocation.filesystem!, cwd) : {}),
			...(bound && tool === names[0] && (normalized || mode === "normalizer-failure") ? { observation: { ...EXACT_TOOL_OBSERVATION, version, content: normalized ? schema : undefined, canonicalize } } : {}) }; },
		preflight: context => allow && !(mode === "denied-root" && context.toolName !== child.tool || mode === "denied-child" && context.toolName === child.tool),
		executionWorlds: [world, sandbox.createExecutionWorld({ driver: "git" })].map(world => ({ ...world, speculation: { ...world.speculation!, execute: async context => {
			executed.push(context.toolName); const branch = await world.speculation!.execute(context);
			if (context.toolName !== child.tool && !(known && (context.args as { path: string }).path === "root-0.txt")) await release.promise;
			if (actualBatch && (context.args as { path: string }).path === "root-0.txt") await firstResult.promise; return branch;
		} } })),
		complete: async (_model, context, options) => {
			requests++; expect(options?.apiKey).toBe("shared-credential");
			if (changed && context.messages.length) return fauxAssistantMessage("done");
			if (mode === "unchanged" && rootContext && !context.messages.length) return fauxAssistantMessage("No alternative.");
			if (context.tools?.[0]?.name === "speculative_continuations") return toolReply("speculative_continuations", {
				scopes: [[...roots.map((_, index) => `root-${index}`), ...extraCall ? ["unknown"] : []]],
			});
			if (context.tools?.[0]?.name === "speculative_observations") {
				observationSignal = options!.signal;
				const payload = JSON.parse(String(context.messages.at(-1)!.content).split("\n").slice(1).join("\n"));
				expect(payload.draft).toMatchObject(rootDraft);
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
				const response = toolUse("speculative_observations", { results: predictedRoots.map(root => { const index = roots.indexOf(root); return {
					callID: mode === "malformed-observation" ? "unknown" : `root-${index}`, isError: mode === "malformed-type" || mode === "actual-before-malformed" ? "false" : mode === "mismatch-error",
					...(mode === "malformed-extra" ? { score: 1_000_000 } : {}), ...(mode === "mismatch-exit" ? { exitCode: 7 } : {}),
					content: normalized ? [fauxText(JSON.stringify({ failedTests: mode === "canonical-mismatch" ? ["a.test.ts:3"] : [], artifact: "abc", duration: 999, pid: 456, tempPath: "/tmp/predicted" }))]
						: textMismatch || actualFirst ? [fauxText("incorrect forecast")] : expected[index]!.content,
				}; }) });
				return { ...response, usage: { ...response.usage, input: 11, output: 7, totalTokens: 18 } };
			}
			if (++drafts === 1) { rootContext = context; return fauxAssistantMessage([...rootDraft,
				...extraCall ? [fauxToolCall(mode === "unknown-batch" ? "unavailable" : child.tool, child.args, { id: "unknown" })] : []], { stopReason: "toolUse" }); }
			privateContexts.push(context); const discarded = wrong && privateContexts.length === 1;
			if (inflight && discarded) { observationSignal = options!.signal; firstResult.resolve(); await forecast.promise; }
			return toolUse(child.tool, discarded ? { path: "discarded.txt" } : child.args,
				{ id: "child" });
		}, getDraftOptions: () => ({ apiKey: "shared-credential" }),
	}, settings);
	await using _cleanup = onExit(async () => { release.resolve(); forecast.resolve(); firstResult.resolve(); await f.dispose(); await sandbox.dispose(); });
	if (seeding) { await f.start("seed"); expect(await f.execute(child.tool, child.args)).toEqual(await f.oracle(child.tool, child.args)); await f.finish(); }
	seeding = false; settings.drafterEnabled = true;
	await f.start();
	const rootOnly = extraCall || ["depth-zero", "denied-root"].includes(mode), stopped = rootOnly || recoverable || ["budget", "abort-observation", "normalizer-failure"].includes(mode);
	if (mode === "abort-observation") { await observing.promise; release.resolve(); await f.dispose(); }
	else if (actualFirst || inflight) {
		await (inflight ? firstResult.promise : observing.promise); release.resolve(); await f.ready(count + 1);
		expect(f.events.some(event => event.type === "source_request" && event.request.settlement.status === "timeout")).toBe(false);
		expect(observationSignal?.aborted).toBe(true); expect(f.drafter.budget.reservedTokens).toBeGreaterThan(0); forecast.resolve();
		await expect.poll(() => f.drafter.budget.reservedTokens).toBe(0);
	}
	else if (stopped || mode === "denied-child") {
		await expect.poll(() => f.events.filter(event => event.type === "source_request").length).toBeGreaterThanOrEqual(rootOnly ? 1 : 2);
	} else if (dependent) {
		await expect.poll(() => drafts).toBe(2); await nextTurn(); expect(executed).not.toContain(child.tool);
		release.resolve(); await f.ready(count + 1);
		if (mode === "missing-input") await expect(readFile(path.join(cwd, "root-0.txt"))).rejects.toMatchObject({ code: "ENOENT" });
		else expect(await readText(cwd, "root-0.txt")).toBe("one\ntwo\nthree\n");
	} else {
		if (captured) await expect.poll(() => f.materialized.some(candidate => candidate.tool === child.tool)).toBe(true);
		await f.ready(captured ? 0 : known ? 2 : 1); expect(f.completed().map(event => event.candidate.tool)).toEqual(captured ? [] : [...(known ? [names[0]] : []), child.tool]);
		expect(executed.filter(tool => tool !== child.tool)).toHaveLength(count);
	}
	expect(rootContext?.messages).toEqual([]);
	if (stopped) expect(drafts).toBe(1);
	else {
		expect(privateContexts).toHaveLength(inflight ? 2 : 1);
		const messages = privateContexts[0]!.messages;
		expect(messages[0]).toMatchObject({ role: "assistant", content: rootDraft });
		expect(messages.slice(1)).toEqual(resultMessages(true));
	}
	release.resolve();
	if (mode !== "denied-root" && mode !== "abort-observation") await f.ready(count + Number(!captured && (!stopped || recoverable) && mode !== "denied-child") + Number(wrong && !inflight && !repairBudget));
	if (wrong && !repairBudget || recoverable) expect(privateContexts.at(-1)!.messages.slice(1)).toEqual(resultMessages());
	const { budget, observations } = f.drafter;
	expect(budget.reportedTokens).toBe(rootOnly || ["abort-observation", "request-failure", "actual-before-failure"].includes(mode) ? 0 : actualBatch ? 36 : 18);
	const forecasted = rootOnly || actualFirst || recoverable || ["abort-observation", "normalizer-failure"].includes(mode) ? 0 : count - Number(known);
	expect(observations).toEqual({ forecasted, compared: forecasted, matched: wrong ? 0 : forecasted });
	expect(budget.requests).toBe(requests); expect(budget.reservedTokens).toBe(0);
	expect(requests).toBe((rootOnly ? 1 : stopped && !recoverable ? 2 : wrong && !repairBudget ? 4 : 3) + (mode === "unchanged" ? 2 : 0)
		+ Number(count > 1 || extraCall) + Number(actualBatch));
	if (mode === "budget" || repairBudget) expect(budget.skippedRequests).toBe(1);
	if (mode === "abort-observation") expect(budget.unreportedTokens).toBeGreaterThan(0);
	if (mode === "normalizer-failure") expect(await f.execute(roots[0]!.tool, roots[0]!.args)).toMatchObject(expected[0]!);
	if (stopped && !recoverable || mode === "denied-child") return;
	if (mode === "canonical-version") version = "test-result/2";
	if (mode === "canonical-schema") schema = Type.Array(schema.items, { minItems: 1 });
	if (mode === "canonical-unbound") bound = false;
	if (retired) {
		changed = true; allow = !mode.endsWith("-fallback");
		await writeText(cwd, roots[0]!.args.path, "four\nfive\nsix\n");
		if (stale) await writeText(cwd, "child.txt", "fresh\n");
	}
	if (mode === "parent-miss") await f.execute(names[0]!, { path: "child.txt" });
	else for (const root of roots) expect(await f.execute(root.tool, root.args)).toMatchObject(retired ? await f.oracle(root.tool, root.args) : expected[roots.indexOf(root)]!);
	const consumer = cross ? names[2]! : child.tool, args = unused ? { path: "unused.txt" } : child.args;
	if (!retired) {
		settings.drafterEnabled = false; await f.finish(); await f.start("child");
		if (stale) await writeText(cwd, "child.txt", "fresh\n");
		allow = !mode.endsWith("-fallback");
		expect(textOutput(await f.execute(consumer, args))).toBe(dependent ? "prepared\n" : stale ? "fresh\n" : unused ? "unused\n" : "child\n");
	}
	expect(f.fallbacks).toEqual([...captured ? [child.tool] : [], ...!allow || mode.includes("unkeyed") ? [retired ? names[0] : consumer] : []]);
	await f.finish();
	if (stale || repairBudget || unused) expectHiddenCompute(f.actors.at(-1)!.computation!, false);
	if (wrong && !repairBudget || recoverable || actualFirst || known || contractChanged) expectHiddenCompute(f.actors.at(-1)!.computation!, true);
	if (retired || wrong && !inflight || mode === "parent-miss" || contractChanged) expect(f.predictions.some(event => event.tool === child.tool &&
		event.settlement.observation === "unobserved" && event.settlement.cause.code === "dependency_impossible")).toBe(true);
	if (mode !== "parent-miss" && !retired && !repairBudget && !cross && !unused) expect(f.predictions.some(event => event.tool === child.tool &&
		event.settlement.observation === "observed" && event.settlement.match.matched)).toBe(true);
	if (inflight) expect(executed.filter(tool => tool === child.tool)).toHaveLength(1);
	expect(requests).toBe(budget.requests + Number(contractChanged || retired && allow)); expect(f.drafter.budget.requests).toBe(requests);
	if (learning) {
		expectHiddenCompute(f.actors.at(-1)!.computation!, !stale && !unused);
		allow = true; settings.drafterEnabled = true; settings.drafterMaxDepth = 0; await f.start("learning");
		await expect.poll(() => f.materialized.find(candidate => candidate.turnID === "learning")).toMatchObject({
			empiricalProbability: cross || retired ? 0.5 : unused ? 1 / 3 : 2 / 3,
			adoptionProbability: !stale && !unused ? 2 / 3 : cross || unused || retired ? 0.5 : 1 / 3,
		});
	}
});

test.for(["completed", "inflight", "forecast", "binding", "parent-binding", "reverse", "both", "unchanged", "malformed-scope", "duplicate-scope", "extra-scope", "budget", "stale", "deep"])("repairs only branches that read changed observations (%s)", async mode => {
	const cwd = await directories.create(), gates = [deferred(), deferred()], delayed = deferred();
	const scopes = mode.endsWith("-scope") ? [["a", "b"]] : [["a"], ["b"], ["a", "b"]];
	const unchanged = mode === "unchanged", both = mode === "both", deep = mode === "deep";
	const names = ["inspect_rows", "render_rows"], { actionSemantics, tools, world, resolveInvocation } = readScenario(cwd, names);
	const roots = ["a", "b"].map(id => ({ id, path: `${id}.txt`, text: `${id}-real\n` }));
	for (const root of roots) await writeText(cwd, root.path, root.text);
	for (const branch of ["a", "b", "ab"]) for (const version of ["old", "new"]) for (const suffix of ["", ".next"])
		await writeText(cwd, `${branch}-${version}${suffix}.txt`, `${branch}-${version}${suffix}\n`);
	const calls: { path: string; signal: AbortSignal }[] = [], forecasts: AbortSignal[] = [], executed: string[] = [];
	const settings = { drafterEnabled: true, candidateLimit: 1, maxConcurrentActions: 3, predictionTimeoutMs: 30_000, drafterMaxDepth: deep ? 2 : 1, drafterTaskMaxRequests: mode === "budget" ? 8 : 32 };
	const f = systemHost(cwd, tools, { actionSemantics,
		resources: idleResources(3),
		resolveInvocation: async (tool, args) => {
			const file = String((args as { path: string }).path);
			if (mode === "binding" && file.includes("old") || mode === "parent-binding" && file === "b.txt") await delayed.promise;
			return resolveInvocation(tool, args);
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
				return toolReply(tool, mode === "extra-scope" ? { scopes: [["a"]], extra: "invalid" }
					: { scopes: mode === "malformed-scope" ? [["unknown"]] : mode === "duplicate-scope" ? [["a"], ["a"]] : scopes });
			}
			if (tool === "speculative_observations") {
				const payload = JSON.parse(String(context.messages.at(-1)!.content).split("\n").slice(1).join("\n"));
				const ids: string[] = payload.candidates.map((item: { call: { id: string } }) => item.call.id);
				if (ids.length === 1 && ids[0] === "b" && !payload.observations.length) expect(JSON.stringify(context.messages)).not.toContain("a-real");
				if (mode === "forecast" && ids.includes("a")) { forecasts.push(options!.signal!); await delayed.promise; }
				return toolReply(tool, { results: payload.candidates.map(({ call }: { call: { id: string; arguments: { path: string } } }) => ({
					callID: call.id, isError: false, content: [fauxText(call.id === "a" ? unchanged ? "a-real\n" : "a-wrong\n"
						: call.id === "b" ? both ? "b-wrong\n" : "b-real\n" : `${call.arguments.path.replace(/\.txt$/, "")}\n`)],
				})) });
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
			return toolReply(names[1]!, { path: file }, { id: `child-${file.replace(/\.txt$/, "")}` });
		},
	}, settings);
	await using _cleanup = onExit(async () => { for (const gate of gates) gate.resolve(); delayed.resolve(); await f.dispose(); });
	await f.start();
	if (mode === "forecast") { await expect.poll(() => forecasts.length).toBe(2); await f.ready(["b-new.txt"]); }
	else if (mode === "parent-binding") await expect.poll(() => calls.some(call => call.path === "a-old.txt")).toBe(true);
	else if (mode === "inflight" || mode === "binding") { await expect.poll(() => calls.length).toBe(3); await f.ready(["b-new.txt"]); }
	else if (mode === "reverse") { await f.ready(["b-new.txt", "ab-old.txt"]); delayed.resolve(); await f.ready(["a-old.txt"]); }
	else await f.ready(scopes.map(ids => `${ids.join("")}-${unchanged || ids.join("") === "b" && !both ? "new" : "old"}.txt`));
	if (deep) await f.ready(["a-old.next.txt", "b-new.next.txt", "ab-old.next.txt"]);
	const bCandidate = f.completed("b-new.txt")[0];
	gates[0]!.resolve(); if (both) gates[1]!.resolve();
	if (mode === "parent-binding") { await expect.poll(() => calls.some(call => call.path === "a-new.txt")).toBe(true); expect(executed).not.toContain("b.txt"); delayed.resolve(); }
	const expected = scopes.map(ids => `${ids.join("")}-new.txt`);
	if (mode === "budget") await expect.poll(() => f.drafter.budget.skippedRequests).toBe(2);
	else await f.ready(expected);
	if (deep) await f.ready(["a-new.next.txt", "ab-new.next.txt"]);
	if (!both) expect(f.completed("b.txt")[0]).toBeUndefined();
	if (bCandidate) { expect(f.completed("b-new.txt")[0]).toBe(bCandidate); expect(calls.filter(call => call.path === "b-new.txt")).toHaveLength(1);
		expect(calls.find(call => call.path === "b-new.txt")!.signal.aborted).toBe(false); }
	if (mode === "inflight") expect(calls.filter(call => call.path.includes("old")).every(call => call.signal.aborted)).toBe(true);
	if (mode === "forecast") expect(forecasts.every(signal => signal.aborted)).toBe(true);
	if (mode === "forecast" || mode === "inflight") { expect(f.drafter.budget.reservedTokens).toBeGreaterThan(0); delayed.resolve(); }
	if (mode === "binding") delayed.resolve();
	await expect.poll(() => f.drafter.budget.reservedTokens).toBe(0);
	if (unchanged) expect(calls).toHaveLength(3);
	gates[1]!.resolve(); await f.ready(["a.txt", "b.txt"]);
	for (const root of roots) expect(textOutput(await f.execute(names[0]!, { path: root.path }))).toBe(root.text);
	settings.drafterEnabled = false; await f.finish(); await f.start("children");
	if (mode === "stale") await writeText(cwd, "a-new.txt", "fresh\n");
	for (const file of expected) {
		expect(textOutput(await f.execute(names[1]!, { path: file }))).toBe(mode === "stale" && file === "a-new.txt" ? "fresh\n" : `${file.replace(/\.txt$/, "")}\n`);
		const consumed = () => f.actors.find(event => JSON.parse(event.actualAction).input.path === file);
		await expect.poll(consumed).toBeDefined(); const event = consumed()!;
		expectHiddenCompute(event.computation!, !(mode === "budget" && file !== "b-new.txt" || mode === "stale" && file === "a-new.txt"));
	}
	if (deep) {
		await f.finish(); await f.start("grandchildren");
		for (const file of ["a-new.next.txt", "b-new.next.txt", "ab-new.next.txt"]) {
			expect(textOutput(await f.execute(names[1]!, { path: file }))).toBe(`${file.replace(/\.txt$/, "")}\n`);
			await expect.poll(() => f.actors.find(event => JSON.parse(event.actualAction).input.path === file)).toMatchObject({ computation: { hiddenComputeMs: expect.any(Number) } });
			expectHiddenCompute(f.actors.at(-1)!.computation!, true);
		}
		expect(calls.filter(call => call.path.startsWith("b-new"))).toHaveLength(2);
	}
	await f.finish();
	for (const file of [...(scopes.length > 1 ? ["b-new.txt"] : []), ...(deep ? ["b-new.next.txt"] : [])]) expect(f.predictions.filter(event => JSON.parse(event.predictedAction!).input.path === file)).toMatchObject([{ settlement: { observation: "observed", match: { matched: true } } }]);
	expect(f.events.some(event => event.type === "source_request" && event.request.settlement.status === "timeout")).toBe(false);
	if (["inflight", "forecast", "binding"].includes(mode)) expect(executed.some(file => file.includes("old"))).toBe(false);
	if (bCandidate) expect(executed.filter(file => file === "b-new.txt")).toHaveLength(1);
});

test.for(["dependent", "independent"])("preserves sibling resource effects outside observation scopes (%s)", async mode => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), release = deferred();
	for (const [file, text] of [["selector.txt", "select\n"], ["written.txt", "before\n"], ["independent.txt", "independent\n"]]) await writeText(cwd, file, text);
	const child = { path: mode === "dependent" ? "written.txt" : "independent.txt" }, tools = [createReadTool(cwd), createWriteTool(cwd)];
	let drafted = 0; const executed: string[] = [], settings = { drafterEnabled: true, candidateLimit: 1, drafterMaxDepth: 1, maxConcurrentActions: 3 };
	const f = systemHost(cwd, tools, {
		resources: idleResources(3),
		executionWorlds: [resourceWorld(), sandbox.createExecutionWorld({ driver: "git" })].map(world => ({ ...world, speculation: { ...world.speculation!, execute: async context => {
			executed.push(String((context.args as { path: string }).path)); const branch = await world.speculation!.execute(context);
			if (context.toolName === "write") await release.promise;
			return branch;
		} } })),
		complete: async (_model, context) => {
			const tool = context.tools?.[0]?.name;
			if (tool === "speculative_continuations") return toolReply(tool, { scopes: [["selector"]] });
			if (tool === "speculative_observations") return toolReply(tool, { results: [{ callID: "selector", isError: false, content: [fauxText("select\n")] }] });
			if (!context.messages.length) return fauxAssistantMessage([fauxToolCall("read", { path: "selector.txt" }, { id: "selector" }),
				fauxToolCall("write", { path: "written.txt", content: "after\n" }, { id: "writer" })]);
			drafted++; return toolReply("read", child);
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
	expectHiddenCompute(f.actors.at(-1)!.computation!, true);
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
	const world = sandbox.createExecutionWorld({ driver: "git" }), fork = gated();
	const f = systemHost(cwd, [createWriteTool(cwd), createEditTool(cwd)], {
		executionWorlds: [{ ...world, speculation: { ...world.speculation, execute: async context => {
			if (order === "independent" && (context.args as { path: string }).path === "notes.txt" && context.toolName === "edit") await fork.wait();
			return world.speculation.execute(context);
		} } }],
		complete: async (_model, context) => {
			const observing = context.tools?.[0]?.name === "speculative_observations", index = observing ? drafted - 1 : drafted++, step = steps[index]!;
			return toolUse(observing ? "speculative_observations" : step.tool,
				observing ? { results: [{ callID: `draft-${index}`, isError: false, content: [fauxText(step.tool === "write" ? `Successfully wrote ${step.input.content!.length} bytes to ${step.input.path}` : `Successfully replaced 1 block(s) in ${step.input.path}.`)] }] } : step.input,
				{ id: `draft-${index}` });
		},
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, maxConcurrentActions: 2,
			drafterMaxDepth: 2, patternAware: { enabled: false } }),
	});
	await using _cleanup = onExit(async () => { fork.release(); await f.dispose(); await sandbox.dispose(); });
	await f.start(); await f.ready(order === "independent" ? 2 : 3); predict = false;
	expect(await readText(file)).toBe("original\n");
	if (order === "independent") {
		await fork.entered; await f.execute(steps[1]!.tool, steps[1]!.input); fork.release(); await f.ready(3);
	}
	await f.finish(true);
	if (order === "dependent") {
		await f.start("missing-parent"); await expect(f.execute(steps[1]!.tool, steps[1]!.input)).rejects.toThrow();
		expect(await readText(file)).toBe("original\n"); await f.finish();
	}
	for (const [index, step] of steps.entries()) {
		if (order === "independent" && index < 2) continue;
		await f.start(`step-${index}`);
		await f.execute(step.tool, step.input); await f.finish();
	}
	expect(f.fallbacks).toEqual(order === "dependent" ? ["edit"] : []);
	for (const name of ["notes.txt", "alias.txt"])
		expect(await readText(cwd, name)).toBe(order === "independent" ? "final\n" : "final\nbeta\n");
	if (order === "independent") await expect(readFile(path.join(cwd, "unused.txt"))).rejects.toMatchObject({ code: "ENOENT" });
});

test.for(["unrelated", "changed", "descendant", "mutation", "mutation-stat", "mutation-split", "mutation-stale"])("retains native workflow results across prompts and validates Actor changes (%s)", async (change, { skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("native process backend requires x86-64 Linux");
	const mutation = change.startsWith("mutation"), metadata = change.endsWith("stat"), split = /split|stale/.test(change), stale = change.endsWith("stale");
	const native = await createLinuxProcessBenchmark("pi-sealed-result-", mutation ? "overlayfs" : "git"), { workspace: cwd, workspaceSandbox: sandbox } = native;
	for (const [name, text] of [["a", "original"], ["b", "other"], ["c", "second"]]) await writeText(cwd, `${name}.txt`, text + "\n");
	if (mutation) await native.compile("reader", { code: `#include <fcntl.h>\n#include <stdio.h>\n#include <sys/stat.h>\n#include <unistd.h>\nint main(void){char text[64];int f=open("a.txt",O_RDONLY),n=read(f,text,sizeof text);${metadata ? 'struct stat s;if(fstat(f,&s))return 1;dprintf(1,"%llu:%lld:%ld\\n",(unsigned long long)s.st_ino,(long long)s.st_ctim.tv_sec,s.st_ctim.tv_nsec);' : ""}close(f);for(volatile unsigned long i=0;i<600000000ul;i++);return n<0||write(1,text,n)!=n;}\n` });
	await native.commit("Sealed result reuse");
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
			if (context.tools?.[0]?.name === "speculative_observations") return toolUse("speculative_observations", {
				results: [{ callID: "draft-0", isError: false, content: [fauxText(mutation ? "Successfully wrote 9 bytes to a.txt" : "original\n")] }],
			});
			const step = steps[drafted]!;
			return toolUse(step.tool, step.input, { id: `draft-${drafted++}` });
		},
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, drafterMaxDepth: 1,
			patternAware: { enabled: false } }),
	});
	await using _cleanup = onExit(async () => { await f.dispose(); await native.dispose(); });
	const status = await native.backend.check(true); if (status.state !== "ready") return skip(status.detail);
	await f.start(); if (!predict) await f.preview("bash", { command: "cat a.txt" });
	await f.ready(predict ? 2 : 1); predict = false;
	expect(await readText(cwd, "a.txt")).toBe("original\n");
	await expect(readFile(path.join(cwd, "marker"))).rejects.toMatchObject({ code: "ENOENT" });
	if (!mutation) { await f.finish(true); await f.start("next-prompt"); }
	await f.execute("write", mutation ? steps[0]!.input : { path: change === "changed" ? "a.txt" : "b.txt", content: "updated\n" });
	if (mutation) { await f.finish(); await f.start("process"); if (stale) await writeText(cwd, "a.txt", "fresh\n"); }
	expect(await readText(cwd, "a.txt")).toBe(stale ? "fresh\n" : mutation ? "prepared\n" : change === "changed" ? "updated\n" : "original\n");
	const expected = execFileSync("/bin/bash", ["-c", body], { cwd, env: native.environment, encoding: "utf8" });
	expect(textOutput(await f.execute("bash", { command }))).toBe(expected);
	if (change === "descendant") { await f.finish(); await f.start("next"); expect(textOutput(await f.execute("bash", { command: "cat c.txt" }))).toBe("second\n"); }
	await f.finish(true);
	const actors = f.actors, timing = actors.at(-1)!.computation!, fallback = mutation || change === "changed";
	expectHiddenCompute(timing, !metadata && !stale && change !== "changed");
	expect(f.fallbacks, JSON.stringify(actors)).toEqual([...(mutation ? [] : ["write"]), ...(fallback ? ["bash"] : [])]);
	expect(f.events.filter(event => event.type === "candidate" && event.state.status === "running")).toHaveLength(change === "descendant" || mutation ? 2 : 1);
	if (mutation) {
		expect(requests).toBeGreaterThanOrEqual(2); expect(requests).toBeLessThanOrEqual(3); expect(actors.map(event => event.settlement.provider.kind)).toEqual(["speculative", fallback ? "actor" : "speculative"]);
		expect(prepared[0] === expected).toBe(!metadata && !split); expect(native.backend.actorMetrics().hits).toBe(Number(!metadata && !stale)); expect(await readText(cwd, "marker")).toBe("once");
	}
});

test("reconsiders restored results while sharing pending reads and retains fresh queries across turns", async () => {
	const cwd = await directories.create(), reader = createReadTool(cwd);
	const file = path.join(cwd, "notes.txt"), original = Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join("\n");
	await writeFile(file, original);
	const world = resourceWorld();
	const computed = gated(), entered = deferred(), controller = new AbortController();
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
			if (!first) await computed.wait(); return first ? { ...branch, reconstruct: undefined, reconstructionScope: undefined,
				validate: async () => { const result = await branch.validate!(); if (result.status === "stale") entered.resolve(); return result; },
			} : branch;
		} } }],
	});
	let actors: Promise<unknown>[] = [];
	await using _cleanup = onExit(async () => { controller.abort(); computed.release(); await Promise.allSettled(actors); await f.dispose(); });
	await f.start(); await f.preview("read", args); await f.ready();
	await writeFile(file, original.replaceAll("line", "edit"));
	await f.preview("read", wide); await computed.entered;
	actors = [1, 2].map(() => f.execute("read", args, controller.signal).catch(error => error)); await entered.promise;
	await writeFile(file, original); computed.release();
	const expected = await reader.execute("oracle", args); expect(await Promise.all(actors)).toEqual([expected, expected]);
	await f.preview("read", { ...args, offset: 6 }); await f.finish(); await f.start("second");
	for (const event of f.actors) {
		expect(event.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } }); expectHiddenCompute(event.computation!, true);
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
	const settled = f.actors;
	expectHiddenCompute(settled.at(-3)!.computation!, true);
	expectHiddenCompute(settled.at(-2)!.computation!, false);
	expect(settled.at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } });
});
