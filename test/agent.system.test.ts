import { mkdir, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall, fauxText, type Context } from "@earendil-works/pi-ai";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import { bindResourceOperation } from "../src/agent-execution-world.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { ActionSemanticsRegistry, PI_ACTION_SEMANTICS, type ActionSemanticsDefinition } from "../src/action-semantics.ts";
import { readText, temporaryDirectories } from "./filesystem.ts";
import { expectHiddenCompute, idleResources, resourceWorld, systemHost } from "./agent-system-fixture.ts";
import { onExit, deferred, gated, nextTurn } from "./async.ts";
import { textOutput } from "./linux-process-fixture.ts";
import { toolUse } from "./model.ts";

const writeText = (cwd: string, file: string, text: string) => writeFile(path.join(cwd, file), text);
const track = <T>(pending: Promise<unknown>[], task: Promise<T>) => { pending.push(task); void task.catch(() => {}); return task; };
const toolReply = (...args: Parameters<typeof fauxToolCall>) => fauxAssistantMessage(fauxToolCall(...args));


const directories = temporaryDirectories("pi-agent-system-");
afterEach(directories.dispose);

test.for(["unbound", "bound", "default-unbound", "default-bound"])("requires actual resource bindings at every capture entry (%s)", async mode => {
	const cwd = await directories.create(), definition = readDefinition("inspect_input"), args = { path: "input.txt" };
	const actionSemantics = new ActionSemanticsRegistry([definition]), bound = mode.endsWith("-bound") || mode === "bound";
	await writeText(cwd, args.path, "source");
	let calls = 0;
	const result = (text: string) => ({ content: [fauxText(text)], details: undefined });
	const tool = { ...createReadTool(cwd), name: definition.tool, execute: async () => result(`unobserved-${++calls}`) };
	const binding = bound ? bindResourceOperation(async view => {
		calls++; return { result: result((await view.readFile(path.join(cwd, args.path))).toString()), isError: false };
	}, cwd, undefined, true) : {};
	const f = systemHost(cwd, [tool], { actionSemantics,
		executionWorlds: mode.startsWith("default-") ? [] : [resourceWorld(actionSemantics)],
		resolveInvocation: () => ({ executor: "closure-input", identity: {}, semantics: definition, ...binding }),
	});
	await using _cleanup = onExit(f.dispose);
	for (const turn of ["first", "second"]) {
		await f.start(turn);
		expect(textOutput(await f.execute(tool.name, args))).toBe(bound ? "source" : `unobserved-${turn === "first" ? 1 : 2}`);
		await f.finish();
	}
	if (bound) {
		expectHiddenCompute(f.actors.at(-1)!.computation!, true);
		expect(f.fallbacks).toEqual([tool.name]);
	} else {
		expect(calls).toBe(2); expect(f.fallbacks).toEqual([tool.name, tool.name]);
		expectHiddenCompute(f.actors.at(-1)!.computation!, false);
	}
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



test.for([
	"seed-dynamic", "seed-calculation", "unkeyed-dynamic", "unregistered-dynamic",
	"raw-seed-calculation", "unindexed-input-read", "disabled-seed-dynamic", "dynamic-preview",
	"live-prediction-dynamic", "live-preview-calculation", "live-input-read", "live-unkeyed-dynamic",
	"seed-stale", "seed-binding", "seed-calculation-key", "seed-revoke",
	"mixed-producer", "mixed-consumer", "mixed-failure", "mixed-budget",
	"mixed-overlap", "mixed-cancel", "mixed-outside", "input-read",
	"input-read-stale", "input-read-copy", "input-read-range", "input-read-cancel",
	"input-read-change", "input-names", "input-names-stale", "input-stat",
	"input-entry-link", "input-type-kind", "input-exists-missing", "input-exists-create",
	"unkeyed-input-read", "unkeyed-input-read-stale", "opaque-unkeyed-dynamic", "large-seed-calculation",
	"after:seed-calculation", "before:seed-dynamic", "consume-cancel:seed-calculation", "unkeyed-rejected-output"
])("shares actual inputs across independently formatted tools (%s)", async scenario => {
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

test.for(["matched", "mismatch", "stale", "cancel"])("executes out-of-order drafts using private observations (%s)", async mode => {
	const cwd = await directories.create(), release = deferred(), observing = deferred();
	const names = ["inspect_rows", "render_rows"];
	const { actionSemantics, tools, world, reader, resolveInvocation } = readScenario(cwd, names);
	await writeText(cwd, "root.txt", "one\ntwo\nthree\n");
	await writeText(cwd, "child.txt", "child\n");
	await writeText(cwd, "discarded.txt", "discarded\n");
	const root = { tool: names[0]!, args: { path: "root.txt", offset: 2, limit: 1 } };
	const child = { tool: names[1]!, args: { path: "child.txt" } };
	const expected = await reader.execute("oracle", root.args);
	const draft = fauxToolCall(root.tool, root.args, { id: "root" });
	const contexts: Context[] = [], executed: string[] = [];
	let drafts = 0;
	const f = systemHost(cwd, tools, { actionSemantics, resolveInvocation, resources: idleResources(3),
		executionWorlds: [{ ...world, speculation: { ...world.speculation!, execute: async context => {
			executed.push(context.toolName);
			const branch = await world.speculation!.execute(context);
			if (context.toolName === root.tool) await release.promise;
			return branch;
		} } }],
		complete: async (_model, context, options) => {
			if (context.tools?.[0]?.name === "speculative_continuations")
				return toolReply("speculative_continuations", { scopes: [["root"]] });
			if (context.tools?.[0]?.name === "speculative_observations") {
				observing.resolve();
				if (mode === "cancel") await new Promise((_, reject) => options!.signal!.addEventListener("abort", () => reject(options!.signal!.reason), { once: true }));
				return toolUse("speculative_observations", { results: [{ callID: "root", isError: false,
					content: mode === "mismatch" ? [fauxText("incorrect forecast")] : expected.content }] });
			}
			if (++drafts === 1) return fauxAssistantMessage(draft);
			contexts.push(context);
			return toolUse(child.tool, mode === "mismatch" && contexts.length === 1 ? { path: "discarded.txt" } : child.args, { id: "child" });
		},
	}, { drafterEnabled: true, drafterMaxDepth: 1, candidateLimit: 1, maxConcurrentActions: 3 });
	await using _cleanup = onExit(async () => { release.resolve(); await f.dispose(); });
	await f.start();
	if (mode === "cancel") {
		await observing.promise; release.resolve(); await f.dispose();
		expect(executed).not.toContain(child.tool);
		return;
	}
	await f.ready();
	expect(f.completed().map(event => event.candidate.tool)).toEqual([child.tool]);
	expect(contexts[0]!.messages.slice(1)).toEqual([expect.objectContaining({ role: "toolResult", toolCallId: "root",
		content: mode === "mismatch" ? [fauxText("incorrect forecast")] : expected.content })]);
	release.resolve();
	await f.ready(mode === "mismatch" ? 3 : 2);
	if (mode === "mismatch") expect(contexts.at(-1)!.messages.slice(1)).toEqual([expect.objectContaining({ content: expected.content })]);
	expect(await f.execute(root.tool, root.args)).toMatchObject(expected);
	if (mode === "stale") await writeText(cwd, "child.txt", "fresh\n");
	expect(textOutput(await f.execute(child.tool, child.args))).toBe(mode === "stale" ? "fresh\n" : "child\n");
	const timing = f.actors.at(-1)!.computation!;
	// Fresh content can still borrow unchanged metadata, so stale content need not erase every benefit.
	if (mode !== "stale") expectHiddenCompute(timing, true);
	else expect(timing.hiddenComputeMs).toBeLessThanOrEqual(timing.toolComputeMs!);
	expect(f.drafter.observations).toMatchObject({ compared: 1, matched: mode === "mismatch" ? 0 : 1 });
	await f.finish();
});
