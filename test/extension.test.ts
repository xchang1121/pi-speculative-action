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
import type { SpeculativeActionHost } from "../src/agent-integration.ts";
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

	it("states the unified execution boundary without claiming a bundled process sandbox", () => {
		const settings = effectiveSettings();
		settings.selfSpeculation = { ...settings.selfSpeculation, enabled: true, forkTransport: "sidecar" };
		const status = formatSpeculativeActionStatus({
			settings,
			metrics: emptyMetrics(),
		});

		expect(status).toContain("Prediction tools: read write edit bash");
		expect(status).toContain(
			"Execution boundary: runtime sandbox first; resource snapshots or Git worktrees second; otherwise Actor fallback",
		);
		expect(status).not.toMatch(/OCI|AppContainer|Docker|Podman/);
		expect(status).toContain("Actor actions: 0/0 (n/a)");
		expect(status).toContain("Process reuse (validated L2): 0/0 (n/a)");
		expect(status).toContain("Task timing: n/a (no completed task)");
		expect(status).not.toContain("0ms saved");
		expect(status).toContain("Execution ahead: 0ms; hit latency: 0ms; attempt lead: 0ms; Actor execution: 0ms");
		expect(status).toContain("Reuse: 0 exact actions; 0 partial results (none)");
		expect(status).toContain("sidecar action source On (confidence ≥0.9)");
	});

	it("reports execution-world health instead of treating registration as availability", async () => {
		const fixture = await createFixture();
		vi.mocked(fixture.host.executionWorldDiagnostics).mockResolvedValue([
			{
				id: "linux_process_reuse",
				scope: "runtime",
				isolation: "runtime_sandbox",
				state: "unavailable",
				detail: "Linux host required",
				storage: {
					entries: 3,
					maxEntries: 32,
					bytes: 2048,
					maxBytes: 4096,
					orphanArtifacts: 1,
					overBudget: false,
				},
			},
		]);
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("status", fixture.context as ExtensionCommandContext);

		expect(fixture.ui.notify).toHaveBeenLastCalledWith(
			expect.stringContaining("linux_process_reuse [runtime/runtime_sandbox]: unavailable — Linux host required"),
			"warning",
		);
		expect(fixture.ui.notify).toHaveBeenLastCalledWith(
			expect.stringContaining("storage 3/32, 2 KiB/4 KiB, 1 orphan artifacts"),
			"warning",
		);
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
		const fixture = await createFixture();
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Tools & execution", "Target decoding", "Close"],
			"Tools & execution": ["Execution guarantees", "Back"],
			"Self-speculation": ["Back"],
		});
		await fixture.emit("session_start", {}, fixture.context);
		const command = fixture.commands.get("speculative-action");
		await command?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Speculative action")).toEqual(
			expect.arrayContaining([
				expect.stringMatching(/^Prediction sources/),
				expect.stringMatching(/^Target decoding/),
				expect.stringMatching(/^Scheduling & cache/),
				expect.stringMatching(/^Tools & execution/),
			]),
		);
		expect(menus.get("Tools & execution")).toEqual(
			expect.arrayContaining([expect.stringMatching(/^Tool policy/), "Execution guarantees"]),
		);
		expect(menus.get("Self-speculation")).toEqual(expect.arrayContaining(["Fork action confidence: 0.9"]));
		expect(fixture.ui.notify).toHaveBeenCalledWith(expect.stringContaining("runtime-wide sandbox"), "info");
	});

	it("exposes Drafter request policy in the Drafter submenu", async () => {
		const fixture = await createFixture();
		const menus = driveSettingsMenus(fixture, {
			"Speculative action": ["Prediction sources", "Close"],
			"Prediction sources": ["Drafter", "Back"],
			Drafter: ["Back"],
		});
		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(menus.get("Drafter")).toEqual(
			expect.arrayContaining([
				"Action utility gate: On",
				"Rollout depth: 1",
				"Output tokens: provider default",
				"Deterministic requests: 1",
				"Temperature range: 0.7-0.7",
			]),
		);
	});

	it("binds typed setting descriptors across root, self-speculation, and PatternAware settings", async () => {
		const fixture = await createFixture();
		driveSettingsMenus(fixture, {
			"Speculative action": ["Scheduling & cache", "Target decoding", "Prediction sources", "Apply changes", "Close"],
			"Scheduling & cache": ["Resource cache memory", "Back"],
			"Self-speculation": ["Endpoint", "Fork action confidence", "Back"],
			"Prediction sources": ["PatternAware", "Back"],
			PatternAware: ["Learning", "Back"],
			"PatternAware learning": ["Future gap coverage", "Back"],
		});
		fixture.ui.input = async (title) =>
			({
				"Resource cache memory (MiB)": "96",
				"Self-speculation endpoint": "file:///unsafe",
				"Fork action minimum confidence": "0.75",
				"Future gap coverage (0-1)": "0.8",
			} as Readonly<Record<string, string>>)[title];

		await fixture.emit("session_start", {}, fixture.context);
		await fixture.commands.get("speculative-action")?.handler("", fixture.context as ExtensionCommandContext);

		expect(fixture.store.effective()).toMatchObject({
			resourceCacheMaxBytes: 96 * 1024 * 1024,
			selfSpeculation: {
				endpoint: "http://127.0.0.1:8000",
				forkActionMinConfidence: 0.75,
			},
			patternAware: { futureGapCoverage: 0.8 },
		});
		expect(fixture.ui.notify).toHaveBeenCalledWith(
			"Endpoint must be an absolute HTTP(S) URL.",
			"warning",
		);
	});
});

interface FixtureOptions {
	readonly consume?: SpeculativeActionHost["consume"];
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
		confirm: async () => false,
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
		createHost: () => host,
		createSettingsStore: () => store,
		createExecutionWorlds: () => [],
	});
	await factory(pi);
	const emit = async (event: string, payload: object, eventContext: ExtensionContext) => {
		for (const handler of handlers.get(event) ?? []) await handler(payload as never, eventContext);
	};
	return { actorTools, baseTools, commands, context, customTools, cwd, emit, handlers, host, store, tools, ui };
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
		executionWorldDiagnostics: vi.fn(async () => []),
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

function memorySettingsStore(initial: SpeculativeActionPackageSettings = { enabled: false }): SpeculativeSettingsStore {
	let value: SpeculativeActionPackageSettings | undefined = initial;
	let scope: "global" | "project" = "global";
	return {
		get scope() {
			return scope;
		},
		load: async () => undefined,
		effective: () => value,
		overlay: () => value as unknown as Readonly<Record<string, unknown>> | undefined,
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
