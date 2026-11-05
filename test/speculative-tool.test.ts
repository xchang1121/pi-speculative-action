import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { Type } from "typebox";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import { createSpeculativeActionHost, defineSpeculativeStep, defineSpeculativeTool } from "@earendil-works/pi-speculative-action";
import { createSpeculativeActionExtension } from "@earendil-works/pi-speculative-action/extension";
import { afterEach, expect, test, vi } from "vitest";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred, onExit } from "./async.ts";
import { inspectDataset, writeDatasetReport } from "../examples/speculative-tool.ts";

const directories = temporaryDirectories("pi-template-");
afterEach(async () => { vi.unstubAllEnvs(); await directories.dispose(); });
async function fixture(tools: ToolDefinition<any, any>[], enabled = false) {
	const cwd = await directories.create(), agentDir = await directories.create();
	await mkdir(path.join(cwd, "data")); await writeFile(path.join(cwd, "data/rows.json"), "[2,3]");
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled, reuseEnabled: true,
		drafterEnabled: false, prepareObservedOperations: false, patternAware: { enabled: false }, resourceCacheMaxBytes: 4 * 1024 * 1024 }));
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	const provider = fauxProvider({ provider: "template", models: [{ id: "actor" }], tokensPerSecond: 4000 });
	const events: SpeculativeActionEvent<string>[] = [];
	let host!: ReturnType<typeof createSpeculativeActionHost>, turn!: Parameters<typeof host.startTurn>[0], reuse = true;
	const extension = createSpeculativeActionExtension({ createHost: (id, options) => {
		host = createSpeculativeActionHost(id, { ...options, getSettings: () => ({ ...options.getSettings?.(), reuseEnabled: reuse }),
			resources: { initial: { cpuCount: 2, idleCpuCount: 2 }, sample: () => new Promise(() => {}) },
			onEvent: event => { events.push(event); return options.onEvent?.(event); } });
		return { ...host, startTurn: async (input, signal) => { turn = input; return host.startTurn(input, signal); } };
	} });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true,
		extensionFactories: [pi => { for (const tool of tools) pi.registerTool(tool); }, extension] });
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(provider.provider);
	const { session } = await createAgentSession({ cwd, agentDir, model: provider.getModel(), modelRuntime, settingsManager,
		resourceLoader, sessionManager: SessionManager.inMemory(cwd), tools: tools.map(tool => tool.name) });
	await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
	let sequence = 0;
	return { cwd, events, abort: () => session.abort(), setReuse: (value: boolean) => { reuse = value; },
		execute: async (name: string, args: Record<string, unknown>, before?: (preview: () => Promise<void>) => Promise<void>, error = false) => {
			const id = `template-${++sequence}`;
			provider.setResponses([async () => {
				await before?.(async () => { await host.previewActorCall({ turnID: turn.turnID, id, tool: name, args, tools: turn.tools }); });
				return fauxAssistantMessage(fauxToolCall(name, args, { id }), { stopReason: "toolUse" });
			}, fauxAssistantMessage("done")]);
			await session.prompt(`Run ${name}.`);
			const result = session.messages.find(message => message.role === "toolResult" && message.toolCallId === id);
			expect(result?.role).toBe("toolResult"); if (result?.role !== "toolResult") throw new Error("Missing tool result");
			expect(result.isError, JSON.stringify(result)).toBe(error); return result;
		},
		dispose: async () => { try { await session.abort(); await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); } finally { session.dispose(); } },
	};
}

const parameters = Type.Object({ path: Type.String(), factor: Type.Number(), label: Type.String() });
test("registered templates share declared asynchronous children across tools", async () => {
	let loads = 0, indexes = 0, reports = 0, effects = 0;
	const rows = defineSpeculativeStep({ id: "example.rows", version: "1", dependencies: {},
		run: async ({ fs }, input: { path: string }) => { loads++; return JSON.parse((await fs.readFile(input.path)).toString("utf8")) as number[]; } });
	const index = defineSpeculativeStep({ id: "example.index", version: "1", dependencies: { rows },
		run: async ({ step }, input: { path: string; factor: number }) => {
			indexes++; return (await step("rows", { path: input.path })).reduce((sum, value) => sum + value * input.factor, 0);
		} });
	const report = defineSpeculativeStep({ id: "example.report", version: "1", dependencies: { index },
		run: async ({ step }, input: { path: string; factor: number; label: string }) => {
			reports++; return { label: input.label, total: await step("index", { path: input.path, factor: input.factor }) };
		} });
	const tools = ["template_summary", "template_export"].map(name => defineSpeculativeTool({ name, label: name, description: name,
		parameters, version: "1", effect: "actor", steps: { report }, resources: args => [args.path],
		execute: async ({ step, fs }, args) => {
			const result = await step("report", args); effects++;
			await fs.writeFile(`${name}.txt`, `${effects}`);
			return { content: [{ type: "text", text: `${name}:${result.total}` }], details: result };
		} }));
	const f = await fixture(tools);
	await using _cleanup = onExit(f.dispose);
	const input = { path: "data/rows.json", factor: 2, label: "first" };
	expect((await f.execute(tools[0]!.name, input)).details).toEqual({ total: 10, label: "first" });
	const second = await f.execute(tools[1]!.name, input);
	expect(second.details).toEqual({ total: 10, label: "first" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 1, indexes: 1, reports: 1, effects: 2 });
	expect(await readFile(path.join(f.cwd, "template_export.txt"), "utf8")).toBe("2");
	(second.details as { total: number }).total = -1;
	expect((await f.execute(tools[0]!.name, input)).details).toEqual({ total: 10, label: "first" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 1, indexes: 1, reports: 1, effects: 3 });
	const changed = { ...input, label: "second" };
	expect((await f.execute(tools[1]!.name, changed)).details).toEqual({ total: 10, label: "second" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 1, indexes: 1, reports: 2, effects: 4 });
	changed.factor = 3;
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({ total: 15, label: "second" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 1, indexes: 2, reports: 3, effects: 5 });
	await writeFile(path.join(f.cwd, input.path), "[5,7]");
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({ total: 36, label: "second" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 2, indexes: 3, reports: 4, effects: 6 });
	f.setReuse(false);
	expect((await f.execute(tools[0]!.name, changed)).details).toEqual({ total: 36, label: "second" });
	expect({ loads, indexes, reports, effects }).toEqual({ loads: 3, indexes: 4, reports: 5, effects: 7 });
});

test("cancelled registered work publishes no result and leaves subsequent calls usable", async () => {
	const entered = deferred(); let builds = 0, effects = 0;
	const value = defineSpeculativeStep({ id: "cancel.value", version: "1", dependencies: {},
		run: async ({ signal }, input: { value: number }) => {
			if (++builds === 1) { entered.resolve(); await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true })); }
			signal.throwIfAborted(); return input.value;
		} });
	const tool = defineSpeculativeTool({ name: "template_cancel", label: "Cancel", description: "Cancel", version: "1", effect: "actor",
		parameters: Type.Object({ value: Type.Number() }), steps: { value },
		execute: async ({ step }, args) => { const result = await step("value", args); effects++; return { content: [], details: result }; } });
	const f = await fixture([tool]);
	await using _cleanup = onExit(f.dispose);
	const cancelled = f.execute(tool.name, { value: 7 }, undefined, true);
	await Promise.race([entered.promise, cancelled.then(() => { throw new Error("Step did not start"); })]);
	await f.abort(); await cancelled; expect(effects).toBe(0);
	for (let i = 0; i < 2; i++) expect((await f.execute(tool.name, { value: 7 })).details).toBe(7);
	expect({ builds, effects }).toEqual({ builds: 2, effects: 2 });
});

test("the packaged example runs without the extension and enforces its data and path boundary", async () => {
	const cwd = await directories.create(), controller = new AbortController();
	const actor = { cwd } as Parameters<typeof inspectDataset.execute>[4];
	await writeFile(path.join(cwd, "rows.json"), "[2,3]");
	const args = { path: "rows.json", multiplier: 2, title: "Report" };
	expect((await inspectDataset.execute("read", args, controller.signal, undefined, actor)).details).toEqual({ count: 2, total: 10, text: "Report: 2 rows, total 10" });
	await writeDatasetReport.execute("write", { ...args, output: "result.txt" }, controller.signal, undefined, actor);
	expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("Report: 2 rows, total 10\n");
	await expect(inspectDataset.execute("escape", { ...args, path: "../outside.json" }, controller.signal, undefined, actor)).rejects.toThrow("outside its workspace");
	await writeFile(path.join(cwd, "rows.json"), '["invalid"]');
	await expect(inspectDataset.execute("invalid", args, controller.signal, undefined, actor)).rejects.toThrow("finite numbers");
	controller.abort(new Error("cancelled"));
	await expect(writeDatasetReport.execute("cancel", { ...args, output: "result.txt" }, controller.signal, undefined, actor)).rejects.toThrow("cancelled");
	expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("Report: 2 rows, total 10\n");
});

test.each(["observation", "workspace_mutation", "actor"] as const)("uses the declared execution boundary (%s)", async effect => {
	let builds = 0, runs = 0;
	const calculated = defineSpeculativeStep({ id: "boundary.value", version: "1", dependencies: {},
		run: async ({ fs }, input: { path: string }) => { builds++; return (await fs.readFile(input.path)).toString("utf8"); } });
	const tool = defineSpeculativeTool({ name: `template_${effect}`, label: effect, description: effect,
		parameters: Type.Object({ path: Type.String() }), version: "1", effect, steps: { calculated }, resources: args => [args.path],
		execute: async ({ step, fs }, args) => {
			runs++; const value = await step("calculated", args);
			if ("writeFile" in fs) await fs.writeFile("effect.txt", "once");
			return { content: [{ type: "text", text: value }], details: {} };
		} });
	const f = await fixture([tool], true);
	await using _cleanup = onExit(f.dispose);
	await f.execute(tool.name, { path: "data/rows.json" }, async preview => {
		await preview();
		if (effect !== "actor") await expect.poll(() => f.events.some(event => event.type === "candidate" && event.state.status === "succeeded")).toBe(true);
		expect(await readFile(path.join(f.cwd, "effect.txt"), "utf8").catch(() => "missing")).toBe("missing");
		expect(runs).toBe(effect === "actor" ? 0 : 1);
	});
	expect({ builds, runs }).toEqual({ builds: 1, runs: 1 });
	expect(await readFile(path.join(f.cwd, "effect.txt"), "utf8").catch(() => "missing")).toBe(effect === "observation" ? "missing" : "once");
});
