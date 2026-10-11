import { link, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createEditTool, createFindTool, createGrepTool, createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { systemHost } from "./agent-system-fixture.ts";
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

test.for(["in-place", "git"] as const)("keeps speculative effects private and refuses stale preimages (%s)", async driver => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "one two\n"); await link(file, path.join(cwd, "alias.txt"));
	const f = systemHost(cwd, [createEditTool(cwd), createReadTool(cwd)], {
		executionWorlds: [sandbox.createExecutionWorld(driver === "git" ? { driver, inPlaceMutations: false } : {})],
	});
	try {
		await f.start();
		const args = { path: "notes.txt", edits: [{ oldText: "two", newText: "three" }] };
		await f.preview("edit", args); await f.ready();
		expect(await readFile(file, "utf8")).toBe("one two\n");
		await writeFile(file, "changed two\n");
		await f.execute("edit", args);
		expect(await readFile(file, "utf8")).toBe("changed three\n");
		expect(textOutput(await f.execute("read", { path: "alias.txt" }))).toBe("changed three\n");
		await f.finish(); await f.start("discard");
		await f.preview("edit", { path: "notes.txt", edits: [{ oldText: "three", newText: "discarded" }] });
		await f.ready(2);
	} finally { await f.dispose(); await sandbox.dispose(); }
	expect(await readFile(file, "utf8")).toBe("changed three\n");
});

test("reuses completed reads and narrower queries across turns, then observes changed bytes", async () => {
	const cwd = await directories.create(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "one\ntwo\nthree\n");
	const reader = createReadTool(cwd), f = systemHost(cwd, [reader, createWriteTool(cwd)]);
	try {
		await f.start(); await f.preview("read", { path: "notes.txt" }); await f.ready();
		const range = { path: "notes.txt", offset: 2, limit: 1 };
		expect(await f.execute("read", range)).toEqual(await reader.execute("reference", range));
		await f.finish(); await f.start("second");
		expect(textOutput(await f.execute("read", { path: "notes.txt" }))).toBe("one\ntwo\nthree\n");
		expect(f.fallbacks).toEqual([]);
		await f.execute("write", { path: "notes.txt", content: "new\n" });
		expect(textOutput(await f.execute("read", { path: "notes.txt" }))).toBe("new\n");
	} finally { await f.dispose(); }
});

test("executes a predicted write/edit workflow privately and commits it once across turns", async () => {
	const cwd = await directories.create(), sandbox = new WorkspaceSandboxService(), file = path.join(cwd, "notes.txt");
	await writeFile(file, "original\n"); await link(file, path.join(cwd, "alias.txt"));
	const steps = [
		{ tool: "write", input: { path: "notes.txt", content: "alpha\nbeta\n" } },
		{ tool: "edit", input: { path: "alias.txt", edits: [{ oldText: "alpha", newText: "omega" }] } },
		{ tool: "edit", input: { path: "notes.txt", edits: [{ oldText: "omega", newText: "final" }] } },
	];
	let predict = true;
	const f = systemHost(cwd, [createWriteTool(cwd), createEditTool(cwd)], {
		executionWorlds: [sandbox.createExecutionWorld({ driver: "git" })],
		complete: async () => fauxAssistantMessage(fauxToolCall("speculative_workflow", { steps }), { stopReason: "toolUse" }),
		getSettings: () => ({ enabled: true, drafterEnabled: predict, candidateLimit: 1, maxConcurrentActions: 2,
			drafterMaxDepth: 2, tools: ["write", "edit"], patternAware: { enabled: false } }),
	});
	try {
		await f.start(); await f.ready(3); predict = false;
		expect(await readFile(file, "utf8")).toBe("original\n");
		for (const [index, step] of steps.entries()) {
			if (index) await f.start(`step-${index}`);
			await f.execute(step.tool, step.input); await f.finish();
		}
		expect(f.fallbacks).toEqual([]);
		expect(await readFile(file, "utf8")).toBe("final\nbeta\n");
		expect(await readFile(path.join(cwd, "alias.txt"), "utf8")).toBe("final\nbeta\n");
	} finally { await f.dispose(); await sandbox.dispose(); }
});

test.for(["unrelated", "changed"])("retains a sealed Bash result after an Actor write (%s) and validates it at adoption", async (change, { skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("native process backend requires x86-64 Linux");
	const native = await createLinuxProcessBenchmark("pi-sealed-result-", "git"), sandbox = new WorkspaceSandboxService();
	const { workspace: cwd } = native;
	await writeFile(path.join(cwd, "a.txt"), "original\n"); await writeFile(path.join(cwd, "b.txt"), "other\n");
	await commitBenchmarkFixture(cwd, "Sealed result reuse");
	const f = systemHost(cwd, [native.tool, createWriteTool(cwd)], {
		executionWorlds: [native.world, sandbox.createExecutionWorld()],
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: native.environment, shellPath: native.shellPath }),
	});
	try {
		const status = await native.backend.check(true);
		if (status.state !== "ready") return skip(status.detail);
		await f.start(); await f.preview("bash", { command: "cat a.txt" }); await f.ready();
		await f.execute("write", { path: change === "changed" ? "a.txt" : "b.txt", content: "updated\n" });
		expect(textOutput(await f.execute("bash", { command: "cat a.txt" }))).toBe(change === "changed" ? "updated\n" : "original\n");
		if (change === "unrelated") {
			expect(f.fallbacks).toEqual(["write"]);
			expect(f.events.filter(event => event.type === "candidate" && event.state.status === "running")).toHaveLength(1);
		}
	} finally { await f.dispose(); await sandbox.dispose(); await native.dispose(); }
});

test("retains completed native descendants when their parent survives an unrelated write", async ({ skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("native process backend requires x86-64 Linux");
	const native = await createLinuxProcessBenchmark("pi-retained-workflow-", "git"), sandbox = new WorkspaceSandboxService();
	const cwd = native.workspace;
	await writeFile(path.join(cwd, "a.txt"), "first\n"); await writeFile(path.join(cwd, "c.txt"), "second\n");
	await writeFile(path.join(cwd, "b.txt"), "original unrelated\n");
	await commitBenchmarkFixture(cwd, "Native workflow");
	let predict = true;
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
		const status = await native.backend.check(true); if (status.state !== "ready") return skip(status.detail);
		await f.start(); await f.ready(2); predict = false;
		await f.execute("write", { path: "b.txt", content: "unrelated\n" });
		expect(textOutput(await f.execute("bash", { command: "cat a.txt" }))).toBe("first\n");
		await f.finish(); await f.start("next");
		expect(textOutput(await f.execute("bash", { command: "cat c.txt" }))).toBe("second\n");
		expect(f.fallbacks, JSON.stringify(f.events)).toEqual(["write"]);
		expect(f.events.filter(event => event.type === "candidate" && event.state.status === "running")).toHaveLength(2);
	} finally { await f.dispose(); await sandbox.dispose(); await native.dispose(); }
});
