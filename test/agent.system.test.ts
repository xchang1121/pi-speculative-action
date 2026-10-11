import { link, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createEditTool, createFindTool, createGrepTool, createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { deferred } from "./async.ts";
import { commitBenchmarkFixture, createLinuxProcessBenchmark, textOutput } from "./linux-process-fixture.ts";

const directories = temporaryDirectories("pi-agent-system-");
afterEach(directories.dispose);

test("learns from executed tool workflows, reopens the saved state and supplies the next read", async () => {
	const cwd = await directories.create(), state = path.join(cwd, "patterns.json");
	await writeFile(path.join(cwd, "first.txt"), "first\n"); await writeFile(path.join(cwd, "next.txt"), "next\n");
	const settings = patternAwareSettings({ enabled: true, minOccurrences: 2, maxContextLength: 1, maxFutureGap: 0, beamWidth: 1, presets: [] });
	const createStore = () => new PatternAwareStore(settings, state, patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd));
	const store = createStore(); await store.load();
	for (let round = 0; round < 4; round++) {
		const f = systemHost(cwd, [createReadTool(cwd)], { patternStore: store }, { patternAware: settings }, `training-${round}`);
		try {
			await f.start(); await f.execute("read", { path: "first.txt" }); await f.finish();
			await f.start("next"); await f.execute("read", { path: "next.txt" }); await f.finish(true);
		} finally { await f.dispose(); }
	}
	await store.flush();
	const reopened = createStore(); await reopened.load();
	const f = systemHost(cwd, [createReadTool(cwd)], { patternStore: reopened }, { patternAware: settings }, "learned");
	try {
		await f.start(); await f.execute("read", { path: "first.txt" }); await f.finish();
		await f.start("next");
		await expect.poll(() => f.events.some(event => event.type === "candidate" && event.candidate.source === "pattern_aware" &&
			event.state.status === "succeeded"), { timeout: 10_000 }).toBe(true);
		f.fallbacks.length = 0;
		expect(textOutput(await f.execute("read", { path: "next.txt" }))).toBe("next\n");
		expect(f.fallbacks).toEqual([]);
	} finally { await f.dispose(); await reopened.flush(); }
});

test("reconstructs new searches from captured inputs across turns and observes namespace changes", async ({ skip }) => {
	const cwd = await directories.create();
	await writeFile(path.join(cwd, "a.txt"), "one\ntwo\nthree\n");
	await writeFile(path.join(cwd, ".ignore"), "ignored.txt\n");
	await writeFile(path.join(cwd, "ignored.txt"), "two ignored\n");
	const profile = await createClosedSearchProfile(cwd);
	if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg unavailable"); }
	const tools = [createGrepTool(cwd), createFindTool(cwd), createLsTool(cwd), createReadTool(cwd)];
	const f = systemHost(cwd, tools, {
		resolveInvocation: (tool, input) => profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
	});
	const oracle = async (tool: string, args: Record<string, unknown>) => {
		const invocation = profile.invocations.get(tool);
		return invocation?.authoritative
			? (await invocation.authoritative({ callID: "oracle", args, signal: new AbortController().signal })).result
			: tools.find(value => value.name === tool)!.execute("oracle", args as never);
	};
	try {
		await f.start(); await f.preview("grep", { pattern: "one", path: "." }); await f.ready();
		await f.finish(); await f.start("queries");
		const queries = [
			{ tool: "grep", input: { pattern: "two", path: "." } },
			{ tool: "find", input: { pattern: "*.txt", path: "." } },
			{ tool: "read", input: { path: "a.txt", offset: 2, limit: 1 } },
		];
		for (const query of queries) expect(await f.execute(query.tool, query.input)).toEqual(await oracle(query.tool, query.input));
		expect(f.fallbacks).toEqual([]);
		await writeFile(path.join(cwd, "a.txt"), "two changed\n");
		await writeFile(path.join(cwd, "added.txt"), "two new\n");
		for (const query of queries) expect(await f.execute(query.tool, query.input)).toEqual(await oracle(query.tool, query.input));
	} finally { await f.dispose(); await profile.pool.dispose(); }
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
	try {
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
	} finally { release.resolve(); await f.dispose(); await sandbox.dispose(); }
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
	try {
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
	} finally { await f.dispose(); await sandbox.dispose(); await native.dispose(); }
});

test("reselects later read results, retains queries across turns and observes changed bytes", async () => {
	const cwd = await directories.create(), reader = createReadTool(cwd);
	const file = path.join(cwd, "notes.txt"), original = Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join("\n");
	await writeFile(file, original);
	const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 1024 * 1024 });
	const computed = deferred(), release = deferred(), entered = deferred();
	let runs = 0;
	const f = systemHost(cwd, [reader, createWriteTool(cwd)], {
		preflight: context => { if ((context.args as { offset?: number }).offset === 5) entered.resolve(); return true; },
		executionWorlds: [{ ...world, speculation: { ...world.speculation!, execute: async context => {
			const first = ++runs === 1, branch = await world.speculation!.execute(context);
			if (first) { computed.resolve(); await release.promise; } return branch;
		} } }],
	});
	let actor: Promise<unknown> | undefined;
	try {
		await f.start(); await f.preview("read", { path: "notes.txt", offset: 1, limit: 10 }); await computed.promise;
		const args = { path: "notes.txt", offset: 5, limit: 1 };
		actor = f.execute("read", args); await entered.promise;
		await f.preview("read", { path: "notes.txt", offset: 3, limit: 10 }); await f.ready();
		expect(await actor).toEqual(await reader.execute("oracle", args));
		release.resolve(); await f.preview("read", { ...args, offset: 6 }); await f.finish(); await f.start("second");
		for (const input of [args, { path: "notes.txt", offset: 1, limit: 10 }])
			expect(await f.execute("read", input)).toEqual(await reader.execute("reference", input));
		expect({ runs, fallbacks: f.fallbacks }).toEqual({ runs: 2, fallbacks: [] });
		await f.execute("write", { path: "notes.txt", content: "new\n" });
		const wide = { path: "notes.txt", offset: 1, limit: 10 };
		expect(textOutput(await f.execute("read", wide))).toBe("new\n");
		await writeFile(file, original); expect(await f.execute("read", wide)).toEqual(await reader.execute("restored", wide)); await f.finish();
		expect(f.events.filter(event => event.type === "actor_action").at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } });
	} finally { release.resolve(); await actor; await f.dispose(); }
});
