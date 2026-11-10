import { writeFile } from "node:fs/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { DrafterTaskBudget } from "../src/core.ts";
import { normalizeDrafterRequestSettings } from "../src/common.ts";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { testModel } from "./model.ts";

const directories = temporaryDirectories("pi-budget-scope-");
afterEach(async () => { vi.unstubAllEnvs(); vi.restoreAllMocks(); await directories.dispose(); });

test("the Pi extension retains its injected host budget across internal SDK prompts", async () => {
	const cwd = await directories.create(), agentDir = await directories.create();
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	await writeFile(path.join(cwd, "evidence.txt"), "source evidence");
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({
		enabled: true, reuseEnabled: false, drafterEnabled: false, prepareObservedOperations: false, searchExecution: "native",
		patternAware: { enabled: false }, selfSpeculation: { enabled: false },
	}));
	const budget = new DrafterTaskBudget(), finished = vi.spyOn(budget, "finishTask");
	const policy = normalizeDrafterRequestSettings({ drafterTaskMaxRequests: 1, drafterBudgetInitialFraction: 1 });
	let sent = 0;
	const extension = createSpeculativeActionExtension({ drafterBudget: budget, createExecutionWorlds: () => [],
		createHost: (id, options) => {
			const host = createSpeculativeActionHost(id, options);
			return { ...host, startTurn: async (input, signal) => {
				await options.drafterBudget!.run({ model: testModel("draft"), context: { messages: [] }, policy,
					options: { maxTokens: 10 }, complete: async () => { sent++; return fauxAssistantMessage("forecast"); } });
				return host.startTurn(input, signal);
			} };
		} });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const loader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
	await loader.reload();
	const actor = fauxProvider({ provider: "budget-scope-actor", models: [{ id: "actor" }], tokensPerSecond: 10000 });
	actor.setResponses([fauxAssistantMessage(fauxToolCall("read", { path: "evidence.txt" }), { stopReason: "toolUse" }),
		fauxAssistantMessage("inspection done"), fauxAssistantMessage("recovery done"), fauxAssistantMessage("new task done")]);
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager,
		resourceLoader: loader, sessionManager: SessionManager.inMemory(cwd), tools: ["read"] });
	try {
		await session.bindExtensions({ mode: "print", onError: error => { throw error; } });
		await budget.withTask(async () => {
			await session.prompt("Inspect."); expect(finished.mock.calls.length).toBeGreaterThanOrEqual(1);
			await session.prompt("Finish using existing evidence.");
		});
		expect(sent).toBe(1); expect(budget.snapshot().requests).toBe(1);
		await budget.withTask(async () => { await session.prompt("A new user task."); });
		expect(sent).toBe(2); expect(budget.snapshot().requests).toBe(1);
	} finally {
		try { await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); }
		finally { session.dispose(); }
	}
});
