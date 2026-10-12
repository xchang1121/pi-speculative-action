import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { ProcessExecutionCoordinator } from "../src/process-execution.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-extension-system-");
afterEach(async () => { vi.unstubAllEnvs(); await directories.dispose(); });

test.each([false, true])("runs the shipped extension through a real Pi session (enabled=%s)", async enabled => {
	const cwd = await directories.create(), agentDir = path.join(cwd, "agent");
	await mkdir(agentDir); vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\n");
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({ enabled, drafterEnabled: false,
		patternAware: { enabled: false }, tools: ["read", "write", "edit"], predictionTimeoutMs: 10_000 }));
	const actor = fauxProvider({ provider: "functional-actor", models: [{ id: "actor" }], tokensPerSecond: 4_000, tokenSize: { min: 1, max: 2 } });
	const response = (name: string, args: Record<string, unknown>) => fauxAssistantMessage(fauxToolCall(name, args), { stopReason: "toolUse" });
	actor.setResponses([
		response("read", { path: "notes.txt" }),
		response("write", { path: "result.txt", content: "first\n" }),
		response("edit", { path: "result.txt", edits: [{ oldText: "first", newText: "final" }] }),
		response("read", { path: "result.txt" }), fauxAssistantMessage("done"),
	]);
	const events: SpeculativeActionEvent<string>[] = [];
	const extension = createSpeculativeActionExtension({ createHost: (id, options) => createSpeculativeActionHost(id, {
		...options, resources: { initial: { cpuCount: 2, idleCpuCount: 2 }, sample: () => new Promise(() => {}) },
		onEvent: event => { events.push(event); return options.onEvent?.(event); },
	}) });
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: null, refreshOnCreate: false });
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({ cwd, agentDir, model: actor.getModel(), modelRuntime, settingsManager, resourceLoader,
		sessionManager: SessionManager.inMemory(cwd), tools: ["read", "write", "edit"] });
	try {
		await session.bindExtensions({ mode: "print" });
		await session.prompt("Read notes.txt, write and edit result.txt, then read the final file.");
		expect(await readFile(path.join(cwd, "notes.txt"), "utf8")).toBe("one\ntwo\n");
		expect(await readFile(path.join(cwd, "result.txt"), "utf8")).toBe("final\n");
		const results = session.messages.filter(message => message.role === "toolResult");
		expect(results).toHaveLength(4);
		expect(results.every(message => !message.isError)).toBe(true);
		expect(results.at(-1)?.content).toEqual([{ type: "text", text: "final\n" }]);
		if (enabled) expect(events.some(event => event.type === "actor_action")).toBe(true);
		const failure = new Error("process launch failed"), coordinator = new ProcessExecutionCoordinator({ execute: async () => ({ exitCode: 0 }) });
		await coordinator.runWith({ execute: () => { throw failure; } }, async () => {
			for (const execute of [() => coordinator.execute({ command: "", cwd, environment: {}, onData: () => {} }), () => coordinator.operations.exec("", cwd, { onData: () => {} })]) await expect(execute()).rejects.toBe(failure);
		});
	} finally { await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); }
});
