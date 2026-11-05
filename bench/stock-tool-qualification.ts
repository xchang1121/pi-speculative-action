import assert from "node:assert/strict";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createEditToolDefinition, createFindToolDefinition, createGrepToolDefinition,
	createLsToolDefinition, createReadToolDefinition, createWriteToolDefinition, createAgentSession, DefaultResourceLoader,
	ModelRuntime, SessionManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { isPoisonedEffectCommit } from "../src/effect-transaction.ts";
import { slash } from "../src/path-utils.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { toolErrorSettlement, type ToolSettlement } from "../src/tool-settlement.ts";

export const STOCK_TOOL_CASES = [
	["read", { path: "notes.txt" }],
	["grep", { pattern: "alpha", path: "." }],
	["find", { pattern: "notes.txt", path: "." }],
	["ls", { path: "." }],
	["write", { path: "generated.txt", content: "generated\n" }],
	["edit", { path: "notes.txt", edits: [{ oldText: "beta", newText: "gamma" }] }],
] as const;

/** Compare the default extension route with stock Pi, including search adoption and speculative effects. */
export async function qualifyStockTool(
	name: (typeof STOCK_TOOL_CASES)[number][0],
	input: (typeof STOCK_TOOL_CASES)[number][1],
	searchExecution?: "native",
) {
	const fixtureParent = path.resolve(os.tmpdir());
	const root = await mkdtemp(path.join(fixtureParent, "pi-tool-qualification-"));
	assert.equal(path.dirname(root), fixtureParent);
	const cwd = path.join(root, "workspace"), agentDir = path.join(root, "agent");
	await mkdir(agentDir);
	const previousAgentDir = process.env.PI_CODING_AGENT_DIR; process.env.PI_CODING_AGENT_DIR = agentDir;
	const args = { ...input };
	const tools = [
		createReadToolDefinition(cwd), createGrepToolDefinition(cwd), createFindToolDefinition(cwd),
		createLsToolDefinition(cwd), createWriteToolDefinition(cwd), createEditToolDefinition(cwd),
	];
	const definition = tools.find((tool) => tool.name === name)!;
	const actor = async (): Promise<ToolSettlement> => {
		try {
			return { result: await definition.execute("oracle", args as never, undefined, undefined, undefined as never), isError: false };
		} catch (error) { return toolErrorSettlement(error); }
	};
	const reset = async () => {
		// Only this mkdtemp-owned fixture is reset, never the caller's cwd.
		assert.equal(path.dirname(cwd), root); await rm(cwd, { recursive: true, force: true });
		await mkdir(path.join(cwd, "nested"), { recursive: true });
		await writeFile(path.join(cwd, "notes.txt"), "alpha\nbeta\n");
		await writeFile(path.join(cwd, "nested", "todo.txt"), "beta\n");
	};
	let poisoned = false, session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
	try {
		await reset();
		const initial = await workspaceState(cwd);
		const actorOutput = await actor();
		const baseline = observable(actorOutput);
		assert.equal(baseline.isError, false, `${name}: Actor baseline failed`);
		const expected = await workspaceState(cwd); await reset();
		await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled: true, drafterEnabled: false,
			prepareObservedOperations: false, patternAware: { enabled: false }, searchExecution }));
		const events: SpeculativeActionEvent<string>[] = [];
		let host!: ReturnType<typeof createSpeculativeActionHost>, turn!: Parameters<typeof host.startTurn>[0];
		const extension = createSpeculativeActionExtension({ createHost: (id, options) => {
			host = createSpeculativeActionHost(id, { ...options, resources: { initial: { cpuCount: 2, idleCpuCount: 2 }, sample: () => new Promise(() => {}) },
				onEvent: event => { events.push(event); return options.onEvent?.(event); } });
			return { ...host, startTurn: (input, signal) => { turn = input; return host.startTurn(input, signal); } };
		} });
		const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
		const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
		await resourceLoader.reload();
		const provider = fauxProvider({ provider: "stock-qualification", models: [{ id: "actor" }], tokensPerSecond: 4000 });
		const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
		modelRuntime.registerNativeProvider(provider.provider);
		({ session } = await createAgentSession({ cwd, agentDir, model: provider.getModel(), modelRuntime, settingsManager, resourceLoader,
			sessionManager: SessionManager.inMemory(cwd), tools: [name] }));
		await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
		let failure: unknown;
		provider.setResponses([async () => {
			try {
				await host.previewActorCall({ turnID: turn.turnID, tool: name, args, id: "stock", tools: turn.tools });
				const deadline = performance.now() + 10000;
				while (!searchExecution && !events.some(event => event.type === "candidate" && event.state.status === "succeeded")) {
					assert.ok(performance.now() < deadline, JSON.stringify(events)); await new Promise(resolve => setTimeout(resolve, 10));
				}
				assert.deepEqual(await workspaceState(cwd), initial, `${name}: speculative effects leaked before adoption`);
			} catch (error) { failure = error; throw error; }
			return fauxAssistantMessage(fauxToolCall(name, args, { id: "stock" }), { stopReason: "toolUse" });
		}, fauxAssistantMessage("done")]);
		await session.prompt(`Run ${name}.`); if (failure) throw failure;
		const result = session.messages.find(message => message.role === "toolResult")!;
		assert.ok(result, JSON.stringify(session.messages));
		assert.deepEqual(observable({ result: { content: result.content, details: result.details }, isError: result.isError }), baseline, `${name}: output differs`);
		assert.deepEqual(await workspaceState(cwd), expected, `${name}: adopted file effects differ`);
		assert.equal(events.some(event => event.type === "actor_action" && (event.computation?.hiddenComputeMs ?? 0) > 0), !searchExecution, `${name}: adoption differs`);
	} catch (error) {
		poisoned = isPoisonedEffectCommit(error);
		if (poisoned) console.error(`Indeterminate adoption: retain fixture without further writes at ${root}`);
		throw error;
	} finally {
		try { if (session) { await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); } }
		finally {
			if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
			if (!poisoned) await rm(root, { recursive: true, force: true });
		}
	}
}

function observable(output: ToolSettlement): ToolSettlement {
	// Compare public results without the extension's in-memory Symbol bookkeeping.
	return JSON.parse(JSON.stringify({ ...output, result: { ...output.result, details: output.result.details ?? {} } }));
}

async function workspaceState(root: string, relative = ""): Promise<unknown[]> {
	const result: unknown[] = [];
	for (const name of (await readdir(path.join(root, relative))).sort()) {
		const file = path.join(relative, name);
		const absolute = path.join(root, file);
		const info = await lstat(absolute);
		assert.ok(info.isFile() || info.isDirectory() || info.isSymbolicLink(), `Unexpected special file: ${file}`);
		const kind = info.isSymbolicLink() ? "symlink" : info.isDirectory() ? "directory" : "file";
		result.push({ path: slash(file), kind, mode: info.mode & 0o777,
			content: kind === "file" ? (await readFile(absolute)).toString("base64")
				: kind === "symlink" ? await readlink(absolute) : null });
		if (kind === "directory") result.push(...await workspaceState(root, file));
	}
	return result;
}
