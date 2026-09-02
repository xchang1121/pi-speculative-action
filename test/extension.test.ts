import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Model } from "@earendil-works/pi-ai";

import {
	createAgentSession,
	createBashToolDefinition,
	createEditToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
	createWriteToolDefinition,
	DefaultResourceLoader,
	type ExtensionAPI,
	type ExtensionCommandContext,
	type ExtensionContext,
	type ExtensionFactory,
	SessionManager,
	SettingsManager,
	type SourceInfo,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CreateSpeculativeActionHostOptions, SpeculativeActionHost } from "../src/agent-integration.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import {
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import { emptyWorldReuseMetrics, type ExecutionWorldDiagnosticSnapshot } from "../src/execution-world.ts";
import {
	createSpeculativeActionExtension,
	formatSpeculativeActionEvent,
	formatSpeculativeActionStatus,
	resolveSpeculativeDraftModel,
	type SpeculativeActionMetrics,
	type SpeculativeSettingsStore,
} from "../src/extension.ts";
import type { SpeculativeActionPackageSettings } from "../src/settings-store.ts";
import { SELF_SPECULATION_DEFAULTS } from "../src/self-speculation.ts";
import { toolErrorSettlement } from "../src/tool-settlement.ts";
import { emptySpeculativeTraceSummary } from "../src/trace-summary.ts";

const roots: string[] = [];

type StockToolDefinition =
	| ReturnType<typeof createReadToolDefinition>
	| ReturnType<typeof createBashToolDefinition>
	| ReturnType<typeof createEditToolDefinition>
	| ReturnType<typeof createWriteToolDefinition>
	| ReturnType<typeof createGrepToolDefinition>
	| ReturnType<typeof createFindToolDefinition>
	| ReturnType<typeof createLsToolDefinition>;

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("zero-modification Pi extension", () => {
	it("uses the actor model by default and accepts either the same or a different configured model", () => {
		const actor = testModel("actor");
		const draft = testModel("draft");
		const registry = {
			getAll: () => [actor, draft],
			hasConfiguredAuth: () => true,
		} as unknown as Parameters<typeof resolveSpeculativeDraftModel>[2];

		expect(resolveSpeculativeDraftModel(undefined, actor, registry)).toBe(actor);
		expect(resolveSpeculativeDraftModel("openai/actor", actor, registry)).toBe(actor);
		expect(resolveSpeculativeDraftModel("openai/draft", actor, registry)).toBe(draft);
		expect(resolveSpeculativeDraftModel("openai/missing", actor, registry)).toBe(actor);
	});

	it("registers stock-shaped overrides and returns a speculative hit without executing the base tool", async () => {
		const fixture = await createFixture({
			consume: async () => ({ result: textResult("cached"), isError: false }),
		});
		await fixture.emit("session_start", {}, fixture.context);

		expect([...fixture.tools.keys()].sort()).toEqual(["bash", "edit", "find", "grep", "ls", "read", "write"]);
		const read = fixture.tools.get("read");
		expect(read).toMatchObject({ name: "read", label: "read" });
		await fixture.emit("context", { messages: [] }, fixture.context);
		const result = await read?.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);

		expect(result?.content).toEqual([{ type: "text", text: "cached" }]);
		expect(fixture.host.consume).toHaveBeenCalledOnce();
		expect(fixture.host.actual).not.toHaveBeenCalled();
	});

	it("keeps the inference bridge inactive when the package-level switch is off", async () => {
		const fixture = await createFixture({
			settings: {
				...effectiveSettings(),
				enabled: false,
				selfSpeculation: { ...SELF_SPECULATION_DEFAULTS, enabled: true },
			},
		});
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.emit("context", { messages: [] }, fixture.context);
		const decorate = fixture.handlers.get("before_provider_request")?.[0];
		const payload = { model: "actor" };

		expect(await decorate?.({ payload } as never, fixture.context)).toBe(payload);
	});

	it("previews the streamed tool before its complete call without claiming either", async () => {
		const fixture = await createFixture();
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.emit("context", { messages: [] }, fixture.context);
		const partial = {
			content: [{ type: "toolCall", id: "actor-read", name: "read", arguments: {} }],
		};
		await fixture.emit(
			"message_update",
			{ assistantMessageEvent: { type: "toolcall_start", contentIndex: 0, partial } },
			fixture.context,
		);
		expect(fixture.host.previewActorTool).toHaveBeenCalledWith({ turnID: "turn_1", tool: "read" }, undefined);
		await fixture.emit(
			"message_update",
			{ assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '{"path":', partial } },
			fixture.context,
		);
		expect(fixture.host.previewActorTool).toHaveBeenCalledOnce();
		expect(fixture.host.previewActorCall).not.toHaveBeenCalled();
		await fixture.emit(
			"message_update",
			{ assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '"notes.txt"}', partial } },
			fixture.context,
		);
		await vi.waitFor(() => expect(fixture.host.previewActorCall).toHaveBeenCalledOnce());
		await fixture.emit(
			"message_update",
			{
				assistantMessageEvent: {
					type: "toolcall_end",
					contentIndex: 0,
					toolCall: { type: "toolCall", id: "actor-read", name: "read", arguments: { path: "notes.txt" } },
					partial,
				},
			},
			fixture.context,
		);
		expect(fixture.host.previewActorCall).toHaveBeenCalledWith(
			{
				turnID: "turn_1",
				id: "actor-read",
				tool: "read",
				args: { path: "notes.txt" },
				tools: expect.any(Array),
			},
			undefined,
		);
		expect(fixture.host.consume).not.toHaveBeenCalled();
	});

	it("preserves stock renderers on every wrapper for interactive and HTML output", async () => {
		const fixture = await createFixture();
		await fixture.emit("session_start", {}, fixture.context);

		for (const name of ["bash", "edit", "find", "grep", "ls", "read", "write"]) {
			const tool = fixture.tools.get(name);
			expect(tool?.renderCall, `${name} renderCall`).toBeTypeOf("function");
			expect(tool?.renderResult, `${name} renderResult`).toBeTypeOf("function");
		}
		expect(fixture.tools.get("edit")?.renderShell).toBe("self");
	});

	it("keeps same-name extension tools authoritative and excludes them from speculation", async () => {
		const fixture = await createFixture({ overriddenTools: ["read"] });
		const customRead = fixture.customTools.get("read") as ReturnType<typeof createReadToolDefinition> | undefined;
		await fixture.emit("session_start", {}, fixture.context);

		expect(fixture.tools.has("read")).toBe(false);
		expect(fixture.actorTools.get("read")).toBe(customRead);
		await fixture.emit("context", { messages: [] }, fixture.context);
		const turn = vi.mocked(fixture.host.startTurn).mock.calls[0]?.[0];
		expect(turn?.tools.map((tool) => tool.name)).not.toContain("read");

		const result = await customRead?.execute(
			"actor-read",
			{ path: "notes.txt" },
			undefined,
			undefined,
			fixture.context,
		);
		expect(result?.content).toEqual([{ type: "text", text: "custom read" }]);
		expect(fixture.host.consume).not.toHaveBeenCalled();
		expect(fixture.host.actual).not.toHaveBeenCalled();

		await fixture.commands.get("speculative-action")?.handler("status", fixture.context as ExtensionCommandContext);
		expect(fixture.ui.notify).toHaveBeenLastCalledWith(
			expect.stringContaining("read (cli: custom-read.ts); excluded from speculation"),
			"warning",
		);
	});

	it.each(["before", "after"] as const)(
		"preserves a same-name extension loaded %s the speculative package",
		async (position) => {
			const cwd = await mkdtemp(path.join(os.tmpdir(), "pi-spec-extension-order-"));
			roots.push(cwd);
			const agentDir = path.join(cwd, "agent");
			await mkdir(agentDir, { recursive: true });
			const host = mockHost();
			const customExecute = vi.fn(async () => ({
				content: [{ type: "text" as const, text: "custom read" }],
				details: undefined,
			}));
			const customRead = { ...createReadToolDefinition(cwd), label: "custom read", execute: customExecute };
			const customExtension: ExtensionFactory = (pi) => pi.registerTool(customRead);
			const speculativeExtension = createSpeculativeActionExtension({
				createHost: () => host,
				createSettingsStore: () => memorySettingsStore(),
				createExecutionWorlds: () => [],
			});
			const extensionFactories =
				position === "before" ? [customExtension, speculativeExtension] : [speculativeExtension, customExtension];
			const settingsManager = SettingsManager.create(cwd, agentDir);
			const resourceLoader = new DefaultResourceLoader({
				cwd,
				agentDir,
				settingsManager,
				extensionFactories,
			});
			await resourceLoader.reload();
			const { session } = await createAgentSession({
				cwd,
				agentDir,
				model: testModel(),
				settingsManager,
				sessionManager: SessionManager.inMemory(),
				resourceLoader,
			});

			try {
				await session.bindExtensions({});
				expect(session.getToolDefinition("read")).toBe(customRead);
				const actorRead = session.agent.state.tools.find((tool) => tool.name === "read");
				const result = await actorRead?.execute("actor-read", { path: "notes.txt" });
				expect(result?.content).toEqual([{ type: "text", text: "custom read" }]);
				expect(customExecute).toHaveBeenCalledOnce();
				expect(host.consume).not.toHaveBeenCalled();
			} finally {
				session.dispose();
			}
		},
	);

	it("recognizes its own wrappers across repeated session starts", async () => {
		const fixture = await createFixture();
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("status", fixture.context as ExtensionCommandContext);

		expect([...fixture.tools.keys()].sort()).toEqual(["bash", "edit", "find", "grep", "ls", "read", "write"]);
		expect(fixture.ui.notify).toHaveBeenLastCalledWith(
			expect.stringContaining("Custom tool conflicts: none"),
			"warning",
		);
	});

	it("delegates misses to Pi's public tool factory and records the authoritative result", async () => {
		const fixture = await createFixture({ consume: async () => undefined });
		await writeFile(path.join(fixture.cwd, "notes.txt"), "from upstream read", "utf8");
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.emit("context", { messages: [] }, fixture.context);
		const read = fixture.tools.get("read");
		const result = await read?.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);

		expect(result?.content).toEqual([{ type: "text", text: "from upstream read" }]);
		expect(fixture.host.actual).toHaveBeenCalledWith(
			expect.objectContaining({
				tool: "read",
				args: { path: "notes.txt" },
				output: expect.objectContaining({ isError: false }),
			}),
		);
	});

	it("never lets cache or telemetry failures replace the actor's stock tool result", async () => {
		const fixture = await createFixture();
		await writeFile(path.join(fixture.cwd, "notes.txt"), "authoritative", "utf8");
		vi.mocked(fixture.host.consume).mockRejectedValue(new Error("cache failed"));
		vi.mocked(fixture.host.actual).mockRejectedValue(new Error("telemetry failed"));
		vi.mocked(fixture.host.finishTurn).mockRejectedValue(new Error("cleanup failed"));
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.emit("context", { messages: [] }, fixture.context);

		const result = await fixture.tools
			.get("read")
			?.execute("actor-read", { path: "notes.txt" }, undefined, undefined, fixture.context);
		await expect(fixture.emit("turn_end", {}, fixture.context)).resolves.toBeUndefined();

		expect(result?.content).toEqual([{ type: "text", text: "authoritative" }]);
	});

	it("keeps mixed-tool reuse primary and Bash child reuse secondary", () => {
		const settings = effectiveSettings();
		settings.selfSpeculation = { ...settings.selfSpeculation, enabled: true, forkTransport: "sidecar" };
		const status = formatSpeculativeActionStatus({
			settings,
			metrics: {
				...emptyMetrics(),
				actorActions: 4,
				speculativeHits: 2,
				exactReuseHits: 1,
				partialResultReuseHits: 1,
				executionAheadMs: 300,
				hitLatencyMs: 40,
				processReuse: {
					...emptyWorldReuseMetrics(), requests: 3, hits: 1, timedHits: 1, crossTurnHits: 1,
					avoidedProcessMs: 1200, timedHitOverheadMs: 200,
				},
			},
		});

		expect(status).toContain("Prediction tools: read write edit bash");
		expect(status).toContain(
			"Execution routing: isolated runtime first; validated reads or private workspaces next; otherwise Actor execution",
		);
		expect(status).not.toMatch(/OCI|AppContainer|Docker|Podman/);
		expect(status).not.toContain("sandbox");
		expect(status).toContain("Tool calls reused: 2/4 (50%); 1 exact, 1 partial; 300ms ready early, 40ms wait after match");
		expect(status).toContain("Bash child commands: 1/3 (33%) reused; ~1s estimated time saved (83%); 1 earlier-turn");
		expect(status).toContain("Task timing: n/a (no completed task)");
		expect(status).toContain("Actor probe: On (sidecar)");
		expect(status).toContain("early tool execution On (tool-name confidence ≥90%)");
		expect(status).not.toContain("prior execution");
		expect(status).not.toMatch(/\bL[12]\b/);
	});

	it("labels an adopted read projection as partial-result reuse", () => {
		const event = {
			sessionID: "session",
			turnID: "turn",
			timestamp: 1,
			cache: emptyMetrics().cache,
			type: "actor_action" as const,
			actualAction: "read notes.txt:20-29",
			settlement: {
				actorAction: { id: "actor", sequence: 1, turnID: "turn" },
				tool: "read",
				matchedPredictions: [],
				rejections: [],
				provider: {
					kind: "speculative" as const,
					candidateID: "candidate",
					match: { kind: "projected" as const, projector: "read.range", distance: 90 },
					timing: { executionAheadMs: 5, attemptLeadMs: 10, hitLatencyMs: 1 },
					toolExecution: { startedAt: 0, completedAt: 5 },
				},
			},
		};

		expect(formatSpeculativeActionEvent(event)).toContain("partial-result reuse (read.range)");
	});

	it("keeps tool execution policy hierarchical and explains the fallback boundary", async () => {
		const fixture = await createFixture({ settings: { enabled: true } });
		vi.mocked(fixture.host.executionWorldDiagnostics).mockResolvedValue(portableDiagnostics({
			entries: 3, maxEntries: 32, bytes: 2048, maxBytes: 4096, orphanArtifacts: 1, overBudget: false,
		}));
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Tools & execution", "Prediction sources", "Enabled", "Discard changes", "Save settings to", "Close"],
			"Tools & execution": ["Tool policy", "Execution routes", "Back"],
			"Tool policy · [x] active · [~] selected · [ ] off": ["[~] bash", "[ ] bash", "Back"],
			"Prediction sources": ["Actor probe", "Back"],
			"Actor probe": ["Back"],
			"Save settings to": ["This project"],
		});
		await fixture.emit("session_start", {}, fixture.context);
		const command = fixture.commands.get("speculative-action");
		await command?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Speculative action")).toEqual(
			expect.arrayContaining([
				expect.stringMatching(/^Prediction sources/),
				expect.stringMatching(/^Tools & execution/),
				expect.stringMatching(/^Advanced settings/),
			]),
		);
		expect(menus.get("Tools & execution")).toEqual(
			expect.arrayContaining(["Tool policy › 6/6 active", "Execution routes"]),
		);
		expect(menus.get("Tool policy · [x] active · [~] selected · [ ] off")).toEqual(expect.arrayContaining([
			expect.stringMatching(/^\[ \] bash · unavailable · Linux host required/),
		]));
		expect(menus.get("Actor probe")).toEqual(expect.arrayContaining(["Actor probe prediction: Off"]));
		expect(menus.get("Actor probe")).not.toEqual(
			expect.arrayContaining([expect.stringMatching(/^Use forked calls/)]),
		);
		expect(menus.get("Actor probe")).not.toEqual(
			expect.arrayContaining([expect.stringMatching(/^Minimum tool-name confidence/)]),
		);
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Each tool uses the first ready execution route"), "info");
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("bash cannot be enabled here"), "warning");
		expect(fixture.ui.notify).toHaveBeenCalledWith(
			expect.stringContaining("storage 3/32, 2 KiB/4 KiB, 1 orphan artifacts"), "info",
		);
		expect(fixture.host.executionWorldDiagnostics).toHaveBeenCalledTimes(2);
		expect(JSON.stringify([...menus.values()])).not.toContain("sandbox");
		expect(await fixture.hostSettings()).toMatchObject({ tools: ["read", "grep", "find", "ls", "write", "edit"] });
		const footer = vi.mocked(fixture.ui.setStatus).mock.calls.at(-1)?.[1] ?? "";
		expect(footer).toContain("tools reused 0/0 (n/a)");
		expect(footer).toContain("reuse history 3 entries (2 KiB)");
		expect(footer).not.toContain("Bash");
		expect(fixture.store.effective()).toEqual({ enabled: true });
		expect(fixture.store.scope).toBe("project");
	});

	it("keeps only direct choices in the Model Drafter menu", async () => {
		const fixture = await createFixture();
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Prediction sources", "Close"],
			"Prediction sources": ["Model Drafter", "Back"],
			"Model Drafter": ["Advanced settings", "Back"],
			"Model Drafter advanced": ["Back"],
		});
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Model Drafter")).toEqual(
			expect.arrayContaining(["Enabled: On", expect.stringMatching(/^Model ›/), "Candidate requests per decision: 2"]),
		);
		expect(menus.get("Model Drafter")).not.toEqual(
			expect.arrayContaining([expect.stringMatching(/^Sampling temperature:/)]),
		);
		expect(menus.get("Model Drafter advanced")).toEqual(
			expect.arrayContaining([
				"Pause when measured cost exceeds benefit: On",
				"Follow-up tool steps: 1",
				"Maximum output tokens: Provider default",
				"Temperature-0 candidates: 1",
				"Sampling temperature: 0.7-0.7",
			]),
		);
	});

	it("binds typed inputs through the advanced hierarchy", async () => {
		const configure = vi.fn();
		const maintain = vi.fn(async () => ({ removedEntries: 2, removedArtifacts: 3, removedBytes: 4096 }));
		const fixture = await createFixture({
			executionWorlds: [{ storage: { configure, maintain } } as unknown as SpeculativeAgentExecutionWorld],
		});
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Advanced settings", "Prediction sources", "Apply changes", "Close"],
			"Advanced settings": ["Scheduling and storage", "Actor probe and target verification", "Learned-pattern tuning", "Back"],
			"Scheduling and storage": ["Live result memory", "Reusable command history entries", "Reusable command history memory", "Reclaim", "Clear", "Back"],
			"Actor probe advanced": ["Integration and authentication", "Fork decoding", "Target verification", "Benefit control", "Back"],
			"Integration and authentication": ["Integration", "Control service URL", "Back"],
			"Fork decoding": ["Back"],
			"Target verification": ["Back"],
			"Benefit control": ["Back"],
			"Learned-pattern advanced": ["Learning history", "Multi-step search", "Back"],
			"Learning history": ["Early-prediction coverage", "Back"],
			"Multi-step search": ["Back"],
			"Prediction sources": ["Actor probe", "Back"],
			"Actor probe": ["Minimum tool-name confidence", "Back"],
			"Actor probe integration": ["Sidecar service"],
		});
		fixture.ui.input = async (title) =>
			({
				"Live result memory (MiB)": "96",
				"Reusable command history entries": "2048",
				"Reusable command history memory (MiB)": "768",
				"Control service URL": "file:///unsafe",
				"Minimum tool-name confidence": "0.75",
				"Early-prediction coverage (0-1)": "0.8",
			} as Readonly<Record<string, string>>)[title];
		fixture.ui.confirm = async (title) => title === "Clear reusable command history?";

		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(fixture.store.effective()).toMatchObject({
			resourceCacheMaxBytes: 96 * 1024 * 1024,
			executionStoreMaxEntries: 2048,
			executionStoreMaxBytes: 768 * 1024 * 1024,
			selfSpeculation: {
				endpoint: "http://127.0.0.1:8000",
				forkTransport: "sidecar",
				forkActionMinConfidence: 0.75,
			},
			patternAware: { futureGapCoverage: 0.8 },
		});
		expect(configure).toHaveBeenLastCalledWith({ maxEntries: 2048, maxBytes: 768 * 1024 * 1024 });
		expect(maintain.mock.calls).toEqual([["gc"], ["clear"]]);
		expect(menus.get("Fork decoding")).not.toEqual(
			expect.arrayContaining([expect.stringMatching(/^Require token probabilities/)]),
		);
		expect(fixture.ui.notify).toHaveBeenCalledWith(
			"Reusable command history cleared: 2 entries, 3 artifacts, 4 KiB.",
			"info",
		);
		expect(fixture.ui.notify).toHaveBeenCalledWith(
			"Endpoint must be an absolute HTTP(S) URL.",
			"warning",
		);
		expect(menus.get("Actor probe")).toEqual(
			expect.arrayContaining([
				"Use forked calls for tool pre-execution: On",
				"Minimum tool-name confidence: 75%",
			]),
		);
	});
});

interface FixtureOptions {
	readonly consume?: SpeculativeActionHost["consume"];
	readonly executionWorlds?: readonly SpeculativeAgentExecutionWorld[];
	readonly overriddenTools?: readonly string[];
	readonly settings?: SpeculativeActionPackageSettings;
}

async function createFixture(options: FixtureOptions = {}) {
	const cwd = await mkdtemp(path.join(os.tmpdir(), "pi-spec-extension-"));
	roots.push(cwd);
	const handlers = new Map<string, Array<(event: never, context: ExtensionContext) => unknown>>();
	const tools = new Map<string, ToolDefinition>();
	const baseTools = new Map<string, StockToolDefinition>();
	baseTools.set("read", createReadToolDefinition(cwd));
	baseTools.set("bash", createBashToolDefinition(cwd));
	baseTools.set("edit", createEditToolDefinition(cwd));
	baseTools.set("write", createWriteToolDefinition(cwd));
	baseTools.set("grep", createGrepToolDefinition(cwd));
	baseTools.set("find", createFindToolDefinition(cwd));
	baseTools.set("ls", createLsToolDefinition(cwd));
	const actorTools = new Map<string, StockToolDefinition | ToolDefinition>(baseTools);
	const toolSources = new Map<string, SourceInfo>(
		[...baseTools.keys()].map((name) => [name, sourceInfo(`<builtin:${name}>`, "builtin")]),
	);
	const customTools = new Map<string, StockToolDefinition>();
	for (const name of options.overriddenTools ?? []) {
		const base = baseTools.get(name);
		if (!base) throw new Error(`Unknown fixture tool override: ${name}`);
		const custom = {
			...base,
			label: `custom ${name}`,
			execute: vi.fn(async () => textResult(`custom ${name}`)),
		} as StockToolDefinition;
		customTools.set(name, custom);
		actorTools.set(name, custom);
		toolSources.set(name, sourceInfo(`custom-${name}.ts`, "cli"));
	}
	const commands = new Map<
		string,
		{ handler: (args: string, context: ExtensionCommandContext) => Promise<void> | void }
	>();
	const host = mockHost(options.consume);
	const ui = {
		select: async (_title: string, _options: string[]) => undefined as string | undefined,
		confirm: async (_title: string, _message?: string) => false,
		input: async (_title: string, _placeholder?: string) => undefined as string | undefined,
		notify: vi.fn(),
		setStatus: vi.fn(),
	};
	const context = {
		cwd,
		mode: "tui",
		hasUI: true,
		ui,
		model: testModel(),
		modelRegistry: {
			complete: vi.fn(),
			getAll: () => [testModel()],
			getAvailable: () => [testModel()],
			hasConfiguredAuth: () => true,
			getApiKeyAndHeaders: async () => ({ ok: true }),
		},
		sessionManager: { getSessionId: () => "session", getSessionFile: () => undefined },
		isProjectTrusted: () => true,
		getSystemPrompt: () => "system",
		signal: undefined,
		thinkingLevel: "off",
	} as unknown as ExtensionContext;
	const store = memorySettingsStore(options.settings);
	let getHostSettings: CreateSpeculativeActionHostOptions["getSettings"];
	const pi = {
		on: (event: string, handler: (event: never, context: ExtensionContext) => unknown) => {
			handlers.set(event, [...(handlers.get(event) ?? []), handler]);
		},
		registerTool: (tool: ToolDefinition) => {
			tools.set(tool.name, tool);
			actorTools.set(tool.name, tool);
			toolSources.set(tool.name, sourceInfo("speculative-action.ts", "cli"));
		},
		registerCommand: (name: string, command: typeof commands extends Map<string, infer T> ? T : never) =>
			commands.set(name, command),
		getActiveTools: () => [...actorTools.keys()],
		getAllTools: () =>
			[...actorTools.values()].map((tool) => ({
				...tool,
				sourceInfo: toolSources.get(tool.name) ?? sourceInfo("unknown"),
			})),
	} as unknown as ExtensionAPI;
	const factory = createSpeculativeActionExtension({
		createHost: (_sessionID, hostOptions) => {
			getHostSettings = hostOptions.getSettings;
			return host;
		},
		createSettingsStore: () => store,
		createExecutionWorlds: () => options.executionWorlds ?? [],
	});
	await factory(pi);
	const emit = async (event: string, payload: object, eventContext: ExtensionContext) => {
		for (const handler of handlers.get(event) ?? []) await handler(payload as never, eventContext);
	};
	return {
		actorTools, baseTools, commands, context, customTools, cwd, emit, handlers, host,
		hostSettings: async () => getHostSettings?.(), store, tools, ui,
	};
}

function driveSettingsMenus(
	fixture: Awaited<ReturnType<typeof createFixture>>,
	routes: Readonly<Record<string, readonly string[]>>,
): Map<string, string[]> {
	const pending = new Map(Object.entries(routes).map(([title, choices]) => [title, [...choices]]));
	const menus = new Map<string, string[]>();
	fixture.ui.select = async (title, options) => {
		menus.set(title, [...options]);
		const prefix = pending.get(title)?.shift();
		return prefix ? options.find((option) => option === prefix || option.startsWith(prefix)) : undefined;
	};
	return menus;
}

function sourceInfo(path: string, source = "test"): SourceInfo {
	return { path, source, scope: "temporary", origin: "top-level" };
}

function mockHost(consume: SpeculativeActionHost["consume"] = async () => undefined): SpeculativeActionHost {
	const consumeMock = vi.fn(consume);
	const actual = vi.fn();
	const host: SpeculativeActionHost = {
		sessionID: "session",
		executionWorldDiagnostics: vi.fn(async () => portableDiagnostics()),
		runtime: {
			settingsChanged: vi.fn(),
			inspect: () => ({
				activeTurns: 0,
				exclusiveCandidates: 0,
				sharedCandidates: 0,
				pendingPredictions: 0,
				deferredPlanActions: 0,
				activePlanActions: 0,
				executionBlockedPlanActions: 0,
				blockedPlanActions: 0,
			}),
		} as unknown as SpeculativeActionHost["runtime"],
		startTurn: vi.fn(),
		previewActorTool: vi.fn(),
		previewActorCall: vi.fn(),
		consume: consumeMock,
		execute: vi.fn(async (input, signal, executor) => {
			if (input.turnID) {
				try {
					const cached = await consumeMock(
						{ ...input, turnID: input.turnID },
						signal,
					);
					if (cached) return cached.result;
				} catch {
					// Match the production gateway's best-effort reuse contract.
				}
			}
			const startedAt = performance.now();
			try {
				const result = await executor({
					tool: input.tool,
					input: input.args,
					...(input.id ? { callID: input.id } : {}),
					...(signal ? { signal } : {}),
				});
				if (input.turnID) {
					try {
						await actual({
							...input,
							turnID: input.turnID,
							durationMs: performance.now() - startedAt,
							output: { result, isError: false },
						});
					} catch {}
				}
				return result;
			} catch (error) {
				if (input.turnID) {
					try {
						await actual({
							...input,
							turnID: input.turnID,
							durationMs: performance.now() - startedAt,
							output: toolErrorSettlement(error),
						});
					} catch {}
				}
				throw error;
			}
		}),
		actual,
		finishTurn: vi.fn(),
		drafterGateSnapshot: () => ({ skippedBatches: 0, samples: 0 }),
		dispose: vi.fn(),
	};
	return host;
}

function portableDiagnostics(
	storage?: NonNullable<ExecutionWorldDiagnosticSnapshot["storage"]>,
): readonly ExecutionWorldDiagnosticSnapshot[] {
	return [
		{
			id: "linux_process_reuse", scope: "runtime", isolation: "runtime_sandbox",
			capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			state: "unavailable", detail: "Linux host required", ...(storage ? { storage } : {}),
		},
		{
			id: "git_worktree", scope: "fallback", isolation: "workspace_branch",
			capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
			state: "registered", detail: "Checked on first use",
		},
		{
			id: "resource_version", scope: "fallback", isolation: "resource_snapshot",
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			state: "ready", detail: "Resource validation ready",
		},
	];
}

function memorySettingsStore(initial: SpeculativeActionPackageSettings = { enabled: false }): SpeculativeSettingsStore {
	let value: SpeculativeActionPackageSettings | undefined = initial;
	let scope: "global" | "project" = "global";
	return {
		get scope() {
			return scope;
		},
		load: async () => undefined,
		effective: () => value,
		editable: () => value,
		setEffective: (next) => {
			value = next;
		},
		clear: () => {
			value = undefined;
		},
		setScope: (next) => {
			scope = next;
		},
		flush: async () => undefined,
	};
}

function textResult(text: string): AgentToolResult<unknown> {
	return { content: [{ type: "text", text }], details: {} };
}

function testModel(id = "mock"): Model<"openai-responses"> {
	return {
		id,
		name: id,
		api: "openai-responses",
		provider: "openai",
		baseUrl: "https://example.invalid",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 8192,
		maxTokens: 2048,
	};
}

function effectiveSettings() {
	return {
		enabled: true,
		drafterEnabled: true,
		drafterGateEnabled: true,
		drafterMaxDepth: 1,
		drafterMaxTokens: 128,
		drafterDeterministicCandidates: 1,
		drafterTemperatureMin: 0.7,
		drafterTemperatureMax: 0.7,
		candidateLimit: 1,
		maxConcurrentActions: 1,
		resourceCacheMaxEntries: 8,
		resourceCacheMaxBytes: 1024,
		executionStoreMaxEntries: 32,
		executionStoreMaxBytes: 4096,
		predictionTimeoutMs: 1000,
		patternAware: {
			enabled: false,
			multiStepEnabled: true,
			maxContextLength: 4,
			beamWidth: 2,
			maxPredictionDepth: 2,
			maxFutureGap: 1,
			futureGapCoverage: 0.8,
			decayHalfLifeEvents: 100,
			minOccurrences: 2,
			maxPatterns: 100,
			minBindingReplayProbability: 0.8,
		},
		selfSpeculation: { ...SELF_SPECULATION_DEFAULTS },
		tools: ["read", "write", "edit", "bash"],
	};
}

function emptyMetrics(): SpeculativeActionMetrics {
	return emptySpeculativeTraceSummary({
		cacheCapacity: 8,
		cacheByteCapacity: 1024,
		cacheCold: 0,
		cacheHot: 0,
		inFlightJobs: 0,
		resultEntries: 0,
		resultBytes: 0,
		branchEntries: 0,
		branchBytes: 0,
		exclusiveCandidates: 0,
		sharedCandidates: 0,
		cacheTools: [],
		cacheExecutions: [],
	});
}
