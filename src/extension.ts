import type { AgentMessage, AgentTool } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Model } from "@earendil-works/pi-ai";
import {
	convertToLlm,
	getAgentDir,
	getShellConfig,
	SettingsManager,
	type ExtensionAPI,
	type ExtensionCommandContext,
	type ExtensionContext,
	type ExtensionFactory,
	type ExtensionUIContext,
} from "@earendil-works/pi-coding-agent";
import { AsyncLocalStorage } from "node:async_hooks";
import { mkdir, realpath } from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type AgentExecutionWorld } from "./agent-execution-world.ts";
import {
	attachSdkTools,
	createSpeculativeActionHost,
	currentSdkTurn,
	executeSdkTool,
	isStockSdkTool,
	normalizeSpeculativeAgentSettings,
	sdkCallbackTool,
	sdkToolInvocation,
	type ActionDrafterBudgetSnapshot,
	type CreateSpeculativeActionHostOptions,
} from "./agent-integration.ts";
import {
	positiveInteger,
	resolveSpeculativeConfig,
	SpeculativeActionSettingsStore,
	SpeculativeConfiguration,
	speculativeConfig,
	withSpeculativeConfig,
	type SpeculativeActionPackageSettings,
	type SpeculativeSettingsScope,
} from "./config-schema.ts";
import { DrafterTaskBudget, forceToolChoice } from "./drafter-plan-source.ts";
import {
	executionCapabilityStatus,
	type ExecutionWorldDiagnosticSnapshot,
	type ExecutionWorldHealthState,
	type SpeculativeExecution,
	type WorldReuseMetrics,
} from "./execution-world.ts";
import {
	checkLinuxEnvironment,
	installLinuxDependencies,
	LinuxProcessReuseBackend,
} from "./linux-process-backend.ts";
import {
	adaptProcessToolOperations,
	createControlledBashOperations,
	createRestorableProcessExecutionWorld,
	definedProcessEnvironment,
	ProcessExecutionCoordinator,
	type ProcessRouteSnapshot,
} from "./linux-process-world.ts";
import { patternAwarePersistenceFile } from "./pattern-aware.ts";
import {
	createClosedSearchProfile,
	createPiToolDefinitions,
	PI_CLOSED_SEARCH_TOOLS,
	resolvePiToolInvocation,
} from "./pi-tool-invocation.ts";
import { asRecord, errorMessage } from "./primitives.ts";
import type { SpeculativeActionEvent } from "./settlement.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import {
	SelfSpeculationCoordinator,
	type SelfSpeculationCoordinatorSnapshot,
	type SelfSpeculationSettings,
} from "./self-speculation.ts";
import { ActorStreamPreviewTracker } from "./settlement.ts";
import {
	emptySpeculativeTraceSummary,
	reduceSpeculativeTrace,
	toolSpeedup,
	type SpeculativeTraceSummary,
} from "./task-timing.ts";
import { resolvePatternWorkspaceIdentity, WorkspaceSandboxService } from "./workspace-sandbox.ts";
const STATUS_KEY = "speculative-action";
const CLOSE = "Close";
const BACK = "Back";
const USE_ACTIVE_MODEL = "Use active model";
const CUSTOM_MODEL = "Custom model...";
export type EffectiveSpeculativeActionSettings = ReturnType<typeof normalizeSpeculativeActionSettings>;
const SETTING_LABELS: Readonly<Record<string, string>> = {
	enabled: "Speculative production",
	reuseEnabled: "Cache & reuse",
	candidateLimit: "Candidate requests per Actor decision",
	maxConcurrentActions: "Speculative resource units per session",
	resourceCacheMaxEntries: "Live result entries",
	resourceCacheMaxBytes: "Live result memory",
	executionStoreMaxEntries: "Reusable command history entries",
	executionStoreMaxBytes: "Reusable command history memory",
	drafterTemperatureMin: "Minimum sampling temperature",
	drafterTemperatureMax: "Maximum sampling temperature",
	prepareObservedOperations: "Prepare observed operations",
	"executionRouting.primary": "Unified execution environment",
	"executionRouting.nativeFallback": "Local safe fallback",
	searchExecution: "Search execution",
	drafterMaxDepth: "Drafter out-of-order follow-up batches",
	drafterDeterministicCandidates: "Temperature-0 Drafter candidates",
	"selfSpeculation.endpoint": "Control service URL",
	"selfSpeculation.requestIDField": "Provider request ID field",
	"selfSpeculation.candidatePath": "Candidate registration path",
	"selfSpeculation.forkPath": "Actor probe path",
	"selfSpeculation.clearPath": "Clear request path",
	"selfSpeculation.capabilitiesPath": "Capabilities path",
	"selfSpeculation.forkActionMinConfidence": "Minimum tool-name confidence",
	"selfSpeculation.maxCandidates": "Candidates sent per Actor decision",
	"selfSpeculation.maxDraftTokens": "Draft-token limit per candidate",
	"selfSpeculation.actorProfile": "Actor tool-call Profile ('auto' to derive)",
	"selfSpeculation.draftFormat": "Target tool-call format",
	"selfSpeculation.draftBoundary": "Target tool-call boundary override ('auto' to derive)",
	"selfSpeculation.timeoutMs": "Control request timeout (ms)",
	"selfSpeculation.forkTemperature": "Actor probe temperature",
	"selfSpeculation.forkMaxAttempts": "Maximum probes per Actor decision",
	"selfSpeculation.forkRetryStreamUpdates": "Stream updates between retries",
	"selfSpeculation.forkBoundaryStreamUpdates": "Stream updates before sentence-boundary retry",
	"selfSpeculation.forkDecoder": "Forked tool-call decoder",
	"selfSpeculation.forkForcedPrefix": "Forced tool-call prefix override ('auto' to derive)",
	"selfSpeculation.apiKeyEnv": "Authentication token environment variable name (not the token)",
	"patternAware.maxContextLength": "Previous actions used as context",
	"patternAware.maxFutureGap": "Maximum skipped Actor decisions",
	"patternAware.futureGapCoverage": "Early-prediction coverage (0-1)",
	"patternAware.decayHalfLifeEvents": "History half-life (events)",
	"patternAware.minOccurrences": "Uses required before learning a pattern",
	"patternAware.maxPatterns": "Stored pattern limit",
	"patternAware.beamWidth": "Alternatives retained per tool",
	"patternAware.maxPredictionDepth": "Maximum predicted tool steps",
	drafterEnabled: "Enabled",
	drafterPatternHints: "Show PatternAware expected calls",
	"selfSpeculation.enabled": "Verify predicted calls during Actor decoding",
	"selfSpeculation.forkEnabled": "Actor probe prediction",
	"selfSpeculation.forkActionEnabled": "Use forked calls for tool pre-execution",
	"selfSpeculation.requireLogprobs": "Require token probabilities",
	"selfSpeculation.forkTransport": "Integration",
	"patternAware.enabled": "Pattern predictions (learning stays active)",
	"patternAware.multiStepEnabled": "Predict follow-up tool steps",
};
export type SpeculativeActionMetrics = SpeculativeTraceSummary & {
	/** Reuse on the authoritative Actor path, excluding reuse internal to speculative branches. */
	readonly actorProcessReuse: WorldReuseMetrics;
};
type ToolCapabilityRow = Readonly<Record<"replay" | "observe" | "fork", ExecutionWorldHealthState>>;
export type ExecutionRoutesSnapshot = {
	readonly worlds: readonly ExecutionWorldDiagnosticSnapshot[];
	readonly actorProcessReplay?: ProcessRouteSnapshot;
	readonly primaryIDs: ReadonlySet<string>;
	readonly searchDetail?: string;
};
export type SpeculativeSettingsStore = Pick<
	SpeculativeActionSettingsStore,
	"scope" | "load" | "effective" | "editable" | "setEffective" | "clear" | "setScope" | "flush"
>;
type SpeculativeActionController = Readonly<Awaited<ReturnType<typeof installController>>>;
export interface SpeculativeActionExtensionDependencies {
	readonly createExecutionWorlds?: (
		context: SpeculativeActionExecutionWorldContext,
	) => readonly AgentExecutionWorld[];
	readonly createHost?: typeof createSpeculativeActionHost;
	readonly createSettingsStore?: (cwd: string) => SpeculativeSettingsStore;
	readonly createWorkspaceSandboxService?: () => WorkspaceSandboxService;
	readonly selfSpeculationFetch?: typeof globalThis.fetch;
	/** Observes every Drafter response, including Actor probes, without receiving credentials. */
	readonly onDrafterResponse?: (
		message: AssistantMessage,
		requestSessionID?: string,
		request?: {
			readonly toolChoice?: unknown;
			readonly toolNames: readonly string[];
		},
	) => void;
	readonly onMetrics?: (metrics: SpeculativeActionMetrics, routes: ExecutionRoutesSnapshot) => void;
}
export interface SpeculativeActionExecutionWorldContext {
	readonly cwd: string;
	readonly autoResizeImages: boolean;
	readonly settings: () => EffectiveSpeculativeActionSettings;
}
export function normalizeSpeculativeActionSettings(input: SpeculativeActionPackageSettings | undefined) {
	const config = resolveSpeculativeConfig(input?.config);
	const agent = withSpeculativeConfig(config, () => normalizeSpeculativeAgentSettings(input));
	const model = typeof input?.draftModel === "string" ? input.draftModel.trim() : undefined;
	const { maxCertificates, maxBytes } = config.storage;
	return {
		...agent,
		...(input?.config ? { config: input.config } : {}),
		...(model ? { draftModel: model } : {}),
		searchExecution: input?.searchExecution === "native" ? "native" : "captured",
		executionStoreMaxEntries: positiveInteger(input?.executionStoreMaxEntries, maxCertificates),
		executionStoreMaxBytes: positiveInteger(input?.executionStoreMaxBytes, maxBytes),
		executionRouting: {
			primary: input?.executionRouting?.primary !== false,
			nativeFallback: input?.executionRouting?.nativeFallback !== false,
		},
	} as const;
}
export function formatSpeculativeActionStatus(input: {
	readonly settings: EffectiveSpeculativeActionSettings;
	readonly metrics: SpeculativeActionMetrics;
}): string {
	const { settings, metrics } = input;
	const cache = metrics.cache;
	const self = settings.selfSpeculation;
	return [
		`Speculative production: ${onOff(settings.enabled)}; cache & reuse: ${onOff(settings.reuseEnabled)}; observation and learning remain active`,
		`Model Drafter: ${onOff(settings.drafterEnabled)}`,
		`Drafter model: ${settings.draftModel ?? "active model"}`,
		`Candidate requests per Actor decision: ${settings.candidateLimit}`,
		`Model Drafter policy: ${settings.drafterMaxDepth} out-of-order follow-up batches; ${settings.drafterDeterministicCandidates} temperature-0 candidates; sampling ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
		`Speculative resource units per session: ${settings.maxConcurrentActions}`,
		`Storage policy: ${settings.resourceCacheMaxEntries} live results/${formatBytes(settings.resourceCacheMaxBytes)}; ${settings.executionStoreMaxEntries} reusable commands/${formatBytes(settings.executionStoreMaxBytes)}`,
		`Pattern predictions: ${onOff(settings.patternAware.enabled)}; learning active; follow-up steps: ${onOff(settings.patternAware.multiStepEnabled)} (alternatives/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}, learn after ${settings.patternAware.minOccurrences}, gap ${settings.patternAware.maxFutureGap}, coverage ${formatPercent(settings.patternAware.futureGapCoverage)}, half-life ${settings.patternAware.decayHalfLifeEvents})`,

		`Actor probe: ${self.enabled && self.forkEnabled ? `On (${self.forkTransport})` : "Off"}; target verification ${onOff(self.enabled)}; early tool execution ${self.enabled && self.forkTransport !== "provider" && self.forkEnabled && self.forkActionEnabled ? (self.forkTransport === "drafter" ? "On (Drafter)" : `On (tool-name confidence ≥${formatPercent(self.forkActionMinConfidence)})`) : "Off"}; ${self.maxCandidates} candidates × ${self.maxDraftTokens} draft tokens; Actor Profile=${self.actorProfile}; ${self.draftFormat} (${self.draftBoundary === "auto" ? "automatic" : self.draftBoundary} boundary); ${self.forkTransport === "sidecar" ? self.endpoint : FORK_TRANSPORT_LABELS[self.forkTransport]}`,
		`Execution routing: admitted providers ${onOff(settings.executionRouting.primary)}; local routes ${onOff(settings.executionRouting.nativeFallback)}; Actor always available`,
		`Search execution: ${searchExecutionLabel(settings.searchExecution)}`,
		`Tool calls reused: ${formatRatio(metrics.speculativeHits, metrics.actorActions)}; ${metrics.exactReuseHits} exact, ${metrics.inputReuseHits} inputs`,
		...formatSubcomputations(metrics),
		...(hasProcessReuse(metrics.actorProcessReuse)
			? [`Bash Actor reuse: ${formatActorProcessReuse(metrics.actorProcessReuse)}`]
			: []),
		...(hasProcessReuse(metrics.processReuse)
			? [`Bash work reused inside speculative branches: ${formatProcessWorkReuse(metrics.processReuse)}`]
			: []),
		`Predictions: ${formatRatio(metrics.predictionsMatched, metrics.predictionsObserved)} matched; ${formatRatio(metrics.predictionsAdopted, metrics.predictionsMatched)} adopted; unobserved: ${metrics.predictionsSettled - metrics.predictionsObserved}`,
		`Predictions by source (matched/observed, adopted): ${tallySummary(metrics.predictionsBySource, ([observed = 0, matched = 0, adopted = 0]) => `${matched}/${observed}, ${adopted}`)}`,
		`Mode results: ${modeSummary(metrics.modesBySource)}`,
		`Tool calls reused by tool: ${tallySummary(metrics.actorActionsByTool, ([actions = 0, reused = 0]) => `${reused}/${actions}`)}`,
		...(metrics.lastSourceFailure ? [`Last prediction source failure: ${metrics.lastSourceFailure}`] : []),
		`Prediction rejections after match: ${countSummary(metrics.predictionRejectedAfterMatch)}`,
		`Actor candidate rejections: ${countSummary(metrics.actorCandidateRejections)}`,
		`Candidates: ${metrics.candidateStarted} started; ${metrics.candidateSucceeded} succeeded; ${metrics.candidateFailed} failed; ${metrics.candidateCancelled} cancelled`,
		metrics.tasks > 0
			? `Tool timing (${metrics.tasks} completed tasks): ${formatTaskTiming(metrics)}.`
			: "Tool timing: n/a (no completed task).",
		`Prediction Drafter tokens (input + output): ${metrics.totalDraftTokens}${metrics.hiddenComputeMs > 0 && !metrics.hiddenComputeIncomplete ? `; ${Math.round((metrics.totalDraftTokens * 1000) / metrics.hiddenComputeMs)} per second of tool computation hidden` : ""}`,
		`Live speculative results: ${cache.resultEntries}/${cache.cacheCapacity}, ${formatBytes(cache.resultBytes)}/${formatBytes(cache.cacheByteCapacity ?? 0)}; cold: ${cache.cacheCold}; hot: ${cache.cacheHot}; jobs: ${cache.inFlightJobs}; branches: ${cache.branchEntries} (${formatBytes(cache.branchBytes)})`,
	].join("\n");
}
export function createSpeculativeActionExtension(
	dependencies: SpeculativeActionExtensionDependencies = {},
): ExtensionFactory {
	return (pi) => {
		let controller: SpeculativeActionController | undefined;
		const sessions = new RuntimeLifecycleLane();
		const actorStream = new ActorStreamPreviewTracker();
		const providerRequest = new AsyncLocalStorage<"drafter">();
		pi.on("before_provider_request", (event) =>
			providerRequest.getStore() === "drafter"
				? event.payload
				: controller?.decorateActorPayload(event.payload),
		);
		for (const definition of createPiToolDefinitions(process.cwd()).values()) {
			pi.registerTool({
				...(definition as import("@earendil-works/pi-coding-agent").ToolDefinition),
				execute: (callID, input, signal, onUpdate, context) => {
					const fallback: AgentTool = {
						...definition,
						execute: (id, args, abort, update) =>
							definition.execute(id, args as never, abort, update as never, context),
					};
					return executeSdkTool(context, fallback, [callID, input, signal, onUpdate as never]);
				},
			});
		}
		pi.on("session_start", (_event, ctx) =>
			sessions.run(async () => {
				await controller?.dispose();
				controller = await installController(ctx, pi, dependencies, providerRequest);
			}),
		);
		pi.on("context", async (event, ctx) => {
			actorStream.clear();
			await controller?.startTurn(event.messages, ctx);
		});
		pi.on("message_update", (event, ctx) => {
			controller?.observeActorOutput(event.assistantMessageEvent);
			for (const preview of actorStream.observe(event.assistantMessageEvent)) {
					controller?.previewActorCall(
						preview.call.name,
						preview.call.id,
						preview.call.arguments,
						ctx.signal,
					);
			}
		});
		pi.on("message_end", (event) => {
			if (event.message.role === "assistant") controller?.finishActorOutput();
		});
		pi.on("turn_end", () => controller?.finishTurn(false));
		pi.on("agent_settled", () => controller?.finishTurn(true));
		pi.on("session_shutdown", (_event, ctx) =>
			sessions.run(async () => {
				ctx.ui.setStatus(STATUS_KEY, undefined);
				const current = controller;
				controller = undefined;
				await current?.dispose().catch(() => undefined);
			}),
		);
		pi.registerCommand("speculative-action", {
			description: "Configure speculative action pre-execution",
			handler: (args: string, ctx: ExtensionCommandContext) => runCommand(args, ctx, controller),
		});
	};
}
export default createSpeculativeActionExtension();
async function installController(
	context: ExtensionContext,
	pi: ExtensionAPI,
	dependencies: SpeculativeActionExtensionDependencies,
	providerRequest: AsyncLocalStorage<"drafter">,
) {
	let ui: ExtensionUIContext | undefined;
	let latestContext = context;
	let currentTurnID: string | undefined;
	let unavailableDraftModel: string | undefined;
	const sessionID = context.sessionManager.getSessionId();
	let lastTurnID: string | undefined;
	let turnSequence = 0;
	let turnTools: readonly AgentTool[] = [];
	const recentEvents: string[] = [];
	const settingsStore =
		dependencies.createSettingsStore?.(context.cwd) ?? new SpeculativeActionSettingsStore(context.cwd);
	await settingsStore.load(context.isProjectTrusted());
	const initial = settingsStore.effective();
	const configuration = new SpeculativeConfiguration(initial?.config);
	let currentSettings = normalizeSpeculativeActionSettings(initial);
	let currentMetrics: SpeculativeTraceSummary = emptySpeculativeTraceSummary({
		cacheCapacity: currentSettings.resourceCacheMaxEntries,
		cacheByteCapacity: currentSettings.resourceCacheMaxBytes,
	});
	const settings = () => currentSettings;
	const drafterBudget = new DrafterTaskBudget();
	const lifecycle = new RuntimeLifecycleLane();
	type SearchProfile = Awaited<ReturnType<typeof createClosedSearchProfile>>;
	type SearchRoute = {
		ready: Promise<SearchProfile | undefined>;
		profile?: SearchProfile;
	};
	let search: SearchRoute | undefined;
	const closedSearchEnabled = () => !lifecycle.sealed && currentSettings.searchExecution !== "native";
	const prepareSearch = (): Promise<SearchProfile | undefined> => {
		if (search) return search.ready;
		const entry: SearchRoute = {
			ready: createClosedSearchProfile(context.cwd).then(
				(profile) => (entry.profile = profile),
				() => undefined,
			),
		};
		search = entry;
		return entry.ready;
	};
	const resetSearch = async () => {
		const previous = search;
		search = undefined;
		await previous?.ready.then((profile) => profile?.dispose());
	};
	// Registry completion takes raw per-API options; Drafter options are simple (reasoning levels) like the Actor's.
	const completeDraft: CreateSpeculativeActionHostOptions["complete"] = (model, llmContext, options) =>
		providerRequest.run("drafter", async () => {
			const registry = latestContext.modelRegistry;
			const provider = registry.getProvider(model.provider);
			const auth = await registry.getApiKeyAndHeaders(model);
			if (!provider || !auth.ok)
				throw new Error(auth.ok ? `Unknown provider: ${model.provider}` : auth.error);
			let request: Parameters<NonNullable<SpeculativeActionExtensionDependencies["onDrafterResponse"]>>[2];
			const message = await provider
				.streamSimple({ ...model, baseUrl: auth.baseUrl ?? model.baseUrl }, llmContext, {
					...options,
					apiKey: options?.apiKey ?? auth.apiKey,
					onPayload: dependencies.onDrafterResponse
						? async (payload, model) => {
								const next = await options?.onPayload?.(payload, model);
								try {
									const selected = asRecord(next === undefined ? payload : next);
									request = {
										...(selected?.tool_choice === undefined ? {} : { toolChoice: selected.tool_choice }),
										toolNames: Array.isArray(selected?.tools)
											? selected.tools.flatMap((tool) => {
													const definition = asRecord(tool);
													const name = asRecord(definition?.function)?.name ?? definition?.name;
													return typeof name === "string" ? [name] : [];
												})
											: [],
									};
								} catch {
									/* Diagnostics cannot change the provider payload. */
								}
								return next;
							}
						: options?.onPayload,
					headers: { ...auth.headers, ...options?.headers },
					env: { ...auth.env, ...options?.env },
				})
				.result();
			try {
				dependencies.onDrafterResponse?.(message, options?.sessionId, request);
			} catch {
				/* Reporting cannot change inference. */
			}
			return message;
		});
	const draftModelFor = (actorModel: Model<Api>) => {
		const reference = settings().draftModel;
		const model = reference
			? findExactModelReferenceMatch(reference, latestContext.modelRegistry.getAvailable())
			: actorModel;
		if (!model && reference !== unavailableDraftModel)
			ui?.notify(`Drafter model ${reference} is unavailable; drafting with the active model.`, "warning");
		unavailableDraftModel = model ? undefined : reference;
		return model ?? actorModel;
	};
	const selfSpeculation = new SelfSpeculationCoordinator({
		settings: () => {
			const configured = settings().selfSpeculation;
			return settings().enabled ? configured : { ...configured, enabled: false };
		},
		fetch: dependencies.selfSpeculationFetch,
		// Without reasoning, the Drafter must answer with the calls the Actor's own reasoning is heading for.
		draftFork: async ({ model, context: actorContext, reasoning, content, signal, temperature }) => {
			const message = await drafterBudget.run({
				model: draftModelFor(model),
				context: {
					...actorContext,
					messages: [
						...actorContext.messages,
						{
							role: "user",
							timestamp: Date.now(),
							content: `The assistant has begun its next reply. Its reasoning so far:\n<reasoning>\n${reasoning}\n</reasoning>${content ? `\nIts reply so far:\n${content}` : ""}\nCall exactly the tool or tools it is about to call next, with the arguments it will use.`,
						},
					],
				},
				options: { signal, temperature, onPayload: forceToolChoice(undefined) },
				complete: completeDraft,
			});
			return (
				message?.content.flatMap((item) =>
					item.type === "toolCall" ? [{ tool: item.name, input: item.arguments }] : [],
				) ?? []
			);
		},
	});
	const piToolSettings = loadPiToolSettings(context);
	const patternWorkspaceIdentity = await resolvePatternWorkspaceIdentity(context.cwd);
	const primaryExecutionWorlds = configuration.run(
		() =>
			dependencies.createExecutionWorlds?.({
				cwd: context.cwd,
				autoResizeImages: piToolSettings.autoResizeImages,
				settings,
			}) ?? [],
	);
	let storeRoot =
		configuration.current.storage.root ?? path.join(getAgentDir(), "speculative-action", "process-reuse");
	try {
		await Promise.all(
			[storeRoot, path.dirname(patternAwarePersistenceFile(patternWorkspaceIdentity, getAgentDir()))].map(
				(root) => mkdir(root, { recursive: true, mode: 0o700 }),
			),
		);
		storeRoot = await realpath(storeRoot);
	} catch {
		/* An unavailable cache cannot prevent the original tools from running. */
	}
	configuration.update({
		...configuration.current,
		storage: { ...configuration.current.storage, root: storeRoot },
	});
	const store = { reservedRoot: true } as const;
	const rawProcessExecutor = adaptProcessToolOperations(
		createControlledBashOperations({
			shellPath: getShellConfig(piToolSettings.shellPath).shell,
		}),
	);
	const processBackend = configuration.run(() => new LinuxProcessReuseBackend({ storeRoot, store }));
	const processCoordinator = new ProcessExecutionCoordinator(
		processBackend.completedReplayExecutor(rawProcessExecutor, {
			sourceRoot: context.cwd,
			invocation: (request) =>
				resolvePiToolInvocation(
					"bash",
					{ command: request.command, timeout: request.timeout },
					{
						cwd: request.cwd,
						shellPath: piToolSettings.shellPath,
						environment: definedProcessEnvironment(request.environment),
					},
				)?.process,
		}),
	);
	const processWorld = configuration.run(() =>
		createRestorableProcessExecutionWorld({
			backend: processBackend,
			coordinator: processCoordinator,
			tools: ["bash"],
			storeRoot,
		}),
	);
	const workspaceSandbox = configuration.run(
		() => dependencies.createWorkspaceSandboxService?.() ?? new WorkspaceSandboxService(),
	);
	workspaceSandbox.useContentStore(processBackend.store);
	const resourceWorld = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, {
		maxBytes: () => currentSettings.resourceCacheMaxBytes,
		persistentInputs: { store: processBackend.store, internalCache: true },
	});
	const executionWorlds = [
		...new Set([
			...primaryExecutionWorlds,
			processWorld,
			workspaceSandbox.createExecutionWorld(),
			{ ...resourceWorld, storage: processWorld.storage },
		]),
	];
	const primaryExecutionWorldIDs = new Set(primaryExecutionWorlds.map((world) => world.id));
	const speculativeExecutionWorldEnabled = (backend: string): boolean =>
		primaryExecutionWorldIDs.has(backend)
			? currentSettings.executionRouting.primary
			: currentSettings.executionRouting.nativeFallback;
	const controls = new Set(executionWorlds.flatMap((world) => (world.storage ? [world.storage] : [])));
	const configureExecutionStorage = () => {
		for (const control of controls)
			control.configure({
				maxEntries: currentSettings.executionStoreMaxEntries,
				maxBytes: currentSettings.executionStoreMaxBytes,
			});
	};
	configureExecutionStorage();
	let executionDiagnostics: readonly ExecutionWorldDiagnosticSnapshot[] = [];
	const executionRoutes = (): ExecutionRoutesSnapshot => ({
		worlds: executionDiagnostics,
		actorProcessReplay: processWorld.nativeDiagnostics(
			currentSettings.reuseEnabled && currentSettings.executionRouting.nativeFallback,
		),
		primaryIDs: primaryExecutionWorldIDs,
		searchDetail:
			!closedSearchEnabled() || (search && !search.profile)
				? "Native Pi"
				: `${searchExecutionLabel(currentSettings.searchExecution)}; ${search?.profile ? [...search.profile.invocations.keys()].join(", ") + " prepared" : "not checked"}`,
	});
	const agentTools = new Map(
		[
			...createPiToolDefinitions(context.cwd, {
				read: { autoResizeImages: piToolSettings.autoResizeImages },
				bash: {
					operations: processCoordinator.operations,
					shellPath: piToolSettings.shellPath,
					commandPrefix: piToolSettings.shellCommandPrefix,
				},
			}),
		].map(([name, definition]): [string, AgentTool] => [
			name,
			{
				...definition,
				execute: (callID, input, signal, onUpdate) =>
					definition.execute(callID, input as never, signal, onUpdate as never, latestContext),
			},
		]),
	);
	const selectTool = (tool: AgentTool) =>
		(isStockSdkTool(tool) && agentTools.get(tool.name)) || sdkCallbackTool(tool);
	const toolCapabilities = () => {
		const { worlds, actorProcessReplay } = executionRoutes();
		const states = new Map<string, ToolCapabilityRow>();
		for (const source of sdk.tools()) {
			const selected = selectTool(source);
			const tool = selected.name;
			const captured = closedSearchEnabled() ? search?.profile?.invocations.get(tool) : undefined;
			const requirements =
				agentTools.get(tool) === selected
					? (captured?.semantics?.requirements ?? PI_ACTION_SEMANTICS.requirements(tool))
					: sdkToolInvocation(selected, latestContext.cwd).semantics?.requirements;
			const capability = (mode: "speculation" | "observation") =>
				requirements ? executionCapabilityStatus(requirements, worlds, mode, tool).state : "unavailable";
			const fork = capability("speculation");
			const observe = capability("observation");
			const actor =
				requirements && actorProcessReplay ? processRouteCapability(actorProcessReplay.state) : "unavailable";
			states.set(tool, { fork, observe, replay: bestCapabilityState([fork, observe, actor]) });
		}
		return states;
	};
	const visibleMetrics = (): SpeculativeActionMetrics => ({
		...currentMetrics,
		actorProcessReuse: processWorld.actorMetrics(),
	});
	function renderFooter(): void {
		if (!ui) return;
		ui.setStatus(STATUS_KEY, formatSpeculativeFooter(settings(), visibleMetrics(), executionRoutes()));
	}
	const host = (dependencies.createHost ?? createSpeculativeActionHost)(sessionID, {
		cwd: context.cwd,
		configuration,
		getSettings: settings,
		complete: completeDraft,
		drafterBudget,
		draftModel: draftModelFor,
		preflight: ({ toolName }) => latestContext.isProjectTrusted() && pi.getActiveTools().includes(toolName),
		resolveInvocation: async (tool, input, definition) => {
			if (!definition || agentTools.get(tool) !== definition)
				return definition && sdkToolInvocation(definition, latestContext.cwd);
			if (closedSearchEnabled() && PI_CLOSED_SEARCH_TOOLS.includes(tool)) {
				const invocation = (await prepareSearch())?.invocations.get(tool);
				if (invocation) return invocation; // Qualification precedes identity binding; never switch an admitted call.
			}
			return resolvePiToolInvocation(tool, input, {
				cwd: latestContext.cwd,
				environment: piShellEnvironment(latestContext),
				autoResizeImages: piToolSettings.autoResizeImages,
				modelSupportsImages: latestContext.model?.input.includes("image") ?? true,
				shellPath: piToolSettings.shellPath,
				shellCommandPrefix: piToolSettings.shellCommandPrefix,
			});
		},
		executionWorlds,
		provenanceStore: processBackend.store,
		speculativeExecutionWorldEnabled,
		actorForkPlanSource: selfSpeculation.actorForkPlanSource,
		patternStateDirectory: getAgentDir(),
		patternStoreReservedRoot: true,
		patternWorkspaceIdentity,
		onEvent: (event) => {
			currentMetrics = reduceSpeculativeTrace(currentMetrics, event);
			recentEvents.push(formatSpeculativeActionEvent(event));
			if (recentEvents.length > speculativeConfig().runtime.recentEvents)
				recentEvents.splice(0, recentEvents.length - speculativeConfig().runtime.recentEvents);
			renderFooter();
		},
	});
	const disconnectProbe = host.runtime.subscribe({
		turn_started: ({ startInput, decisionSequence }) =>
			selfSpeculation.startTurn(
				startInput.turnID,
				startInput.actorModel,
				startInput.context,
				decisionSequence,
			),
		candidate_materialized: (candidate) => selfSpeculation.addCandidate(candidate),
		actor_materialized: ({ action }) => selfSpeculation.observeActorAction(action),
		actor_settled: ({ settlement }) => selfSpeculation.observeActorSettlement(settlement),
		prediction_settled: (feedback) => selfSpeculation.observePredictionSettlement(feedback),
	});
	const refreshExecutionDiagnostics = (refresh = false): Promise<void> =>
		lifecycle.admit(async () => {
			if (refresh) await resetSearch();
			const preparations = [
				closedSearchEnabled() ? prepareSearch() : undefined,
				Promise.resolve().then(() => host.executionWorldDiagnostics(refresh)),
			] as const;
			// A failed provider cannot detach preparation from refresh or shutdown ownership.
			const [, diagnostics] = await Promise.all(preparations).finally(() => Promise.allSettled(preparations));
			executionDiagnostics = diagnostics;
			await recoverSpeculation(() => host.runtime.settingsChanged());
			renderFooter();
		});
	const controller = {
		settings,
		editableSettings: () => normalizeSpeculativeActionSettings(settingsStore.editable()),
		settingsScope: () => settingsStore.scope,
		setSettingsScope: (scope: SpeculativeSettingsScope) => settingsStore.setScope(scope),
		toolCapabilities,
		recentEvents: (): readonly string[] => [...recentEvents],
		refreshExecutionDiagnostics,
		executionRoutes,
		maintainExecutionStorage: async (operation: "gc" | "clear") => {
			let entries = 0;
			let artifacts = 0;
			let bytes = 0;
			let failed = 0;
			for (const control of controls) {
				try {
					const result = await control.maintain(operation);
					entries += result.removedEntries;
					artifacts += result.removedArtifacts;
					bytes += result.removedBytes;
				} catch {
					failed++;
				}
			}
			await recoverSpeculation(() => refreshExecutionDiagnostics(true));
			return {
				text: `Reusable command history ${operation === "gc" ? "reclaimed" : "cleared"}: ${entries} entries, ${artifacts} artifacts, ${formatBytes(bytes)}${failed ? `; ${failed} execution worlds failed` : ""}.`,
				failed: failed > 0,
			};
		},
		setSettings: async (value: SpeculativeActionPackageSettings | undefined) => {
			const previous = currentSettings;
			if (value)
				settingsStore.setEffective(
					value,
					normalizeSpeculativeActionSettings(
						settingsStore.scope === "project" ? settingsStore.editable("global") : undefined,
					),
				);
			else settingsStore.clear();
			const effective = settingsStore.effective();
			const updatedConfig = effective?.config;
			configuration.update({ ...updatedConfig, storage: { ...updatedConfig?.storage, root: storeRoot } });
			currentSettings = normalizeSpeculativeActionSettings(effective);
			configureExecutionStorage();
			if (!currentSettings.enabled || !currentSettings.selfSpeculation.enabled) selfSpeculation.reset();
			await recoverSpeculation(() =>
				refreshExecutionDiagnostics(
					previous.searchExecution !== currentSettings.searchExecution ||
						!isDeepStrictEqual(previous.executionRouting, currentSettings.executionRouting) ||
						!isDeepStrictEqual(previous.config, currentSettings.config),
				),
			);
			await settingsStore.flush(); // Applied for this session either way; callers report whether it was saved.
		},
		startTurn: async (messages: AgentMessage[], nextContext: ExtensionContext) => {
			latestContext = nextContext;
			const model = nextContext.model;
			const turn = currentSdkTurn(nextContext);
			if (!model || !turn) return;
			try {
				if (currentTurnID) await host.finishTurn(currentTurnID);
				const turnID = (currentTurnID = `turn_${++turnSequence}`);
				const tools = (turnTools = turn.tools.map(selectTool));
				turn.execute = (tool, args) => {
					const [callID, input, signal] = args;
					const selected = selectTool(tool);
					return host.execute(
						{
							turnID,
							id: callID,
							tool: tool.name,
							args: input,
							tools: tools.map((current) => (current.name === tool.name ? selected : current)),
						},
						signal,
						async (operation) =>
							operation.invocation?.authoritative
								? (
										await operation.invocation.authoritative({
											callID,
											args: operation.input,
											signal: signal ?? new AbortController().signal,
										})
									).result
								: selected.execute(...args),
					);
				};
				const actorContext = {
					systemPrompt: nextContext.getSystemPrompt(),
					messages: convertToLlm(messages),
					tools: [...turnTools],
				};
				await host.startTurn(
					{
						turnID: currentTurnID,
						actorModel: model,
						context: actorContext,
						actorOptions: nextContext.signal ? { signal: nextContext.signal } : undefined,
						tools: turnTools,
					},
					nextContext.signal,
				);
				void recoverSpeculation(() => refreshExecutionDiagnostics());
			} catch {
				// Speculation is optional; the actor request remains authoritative.
			}
		},
		previewActorCall: (tool: string, callID: string, input: unknown, signal?: AbortSignal) => {
			const turnID = currentTurnID;
			if (!turnID) return;
			void recoverSpeculation(() =>
				host.previewActorCall({ turnID, id: callID, tool, args: input, tools: turnTools }, signal),
			);
		},
		decorateActorPayload: (payload: unknown) => selfSpeculation.decorateActorPayload(payload),
		observeActorOutput: (event: Parameters<SelfSpeculationCoordinator["observeActorOutput"]>[0]) =>
			selfSpeculation.observeActorOutput(event),
		finishActorOutput: () => selfSpeculation.finishActorOutput(),
		finishTurn: async (terminal = false) => {
			const turnID = currentTurnID ?? (terminal ? lastTurnID : undefined);
			if (!turnID) return;
			if (currentTurnID) {
				currentTurnID = undefined;
				lastTurnID = turnID;
			}
			await recoverSpeculation(() => host.finishTurn(turnID, terminal));
			if (terminal) selfSpeculation.reset();
			else selfSpeculation.endTurn();
			if (terminal) lastTurnID = undefined;
		},
		statusText: () =>
			[
				formatSpeculativeActionStatus({ settings: settings(), metrics: visibleMetrics() }),
				formatDrafterBudgetStatus(host.drafterBudgetSnapshot()),
				formatSelfSpeculationStatus(selfSpeculation.snapshot()),
				executionWorldSummary(toolCapabilities(), executionRoutes()),
			].join("\n"),
		dispose: () => {
			sdk.dispose();
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
			return lifecycle.dispose(async () => {
				const routes = executionRoutes();
				return Promise.resolve()
					.then(() => settingsStore.flush())
					.finally(() => host.dispose())
					.finally(disconnectProbe)
					.finally(() => workspaceSandbox.dispose())
					.finally(() => selfSpeculation.dispose())
					.finally(resetSearch)
					.finally(() => {
						try {
							dependencies.onMetrics?.(visibleMetrics(), routes);
						} catch {
							/* Reporting cannot change cleanup. */
						}
					});
			});
		},
	} as const;
	const sdk = attachSdkTools(context, [...agentTools.values()], () => pi.getActiveTools());
	await recoverSpeculation(() => refreshExecutionDiagnostics());
	ui = context.ui;
	renderFooter();
	return configuration.bind(controller);
}
async function recoverSpeculation<T>(operation: () => Promise<T>): Promise<T | undefined> {
	try {
		return await operation();
	} catch {
		return undefined;
	}
}
function piShellEnvironment(context: ExtensionContext): Readonly<Record<string, string>> {
	const environment: NodeJS.ProcessEnv = { ...process.env };
	const pathKey = Object.keys(environment).find((key) => key.toLowerCase() === "path") ?? "PATH";
	const binDirectory = path.join(getAgentDir(), "bin");
	const currentPath = environment[pathKey] ?? "";
	if (!currentPath.split(path.delimiter).includes(binDirectory)) {
		environment[pathKey] = [binDirectory, currentPath].filter(Boolean).join(path.delimiter);
	}
	delete environment.PI_SESSION_ID;
	delete environment.PI_SESSION_FILE;
	delete environment.PI_PROVIDER;
	delete environment.PI_MODEL;
	delete environment.PI_REASONING_LEVEL;
	environment.PI_SESSION_ID = context.sessionManager.getSessionId();
	const sessionFile = context.sessionManager.getSessionFile();
	if (sessionFile) environment.PI_SESSION_FILE = sessionFile;
	if (context.model) {
		environment.PI_PROVIDER = context.model.provider;
		environment.PI_MODEL = context.model.id;
	}
	if (context.thinkingLevel) environment.PI_REASONING_LEVEL = context.thinkingLevel;
	return definedProcessEnvironment(environment);
}
function loadPiToolSettings(context: ExtensionContext) {
	const settings = SettingsManager.create(context.cwd, getAgentDir(), {
		projectTrusted: context.isProjectTrusted(),
	});
	return {
		shellPath: settings.getShellPath(),
		shellCommandPrefix: settings.getShellCommandPrefix(),
		autoResizeImages: settings.getImageAutoResize(),
	};
}
async function runCommand(
	args: string,
	ctx: ExtensionContext,
	controller: SpeculativeActionController | undefined,
): Promise<void> {
	if (!controller) {
		ctx.ui.notify("Speculative action runtime is unavailable.", "error");
		return;
	}
	const { ui } = ctx;
	const command = args.trim().toLowerCase();
	if (command === "on" || command === "off") {
		const enabled = command === "on";
		const saved = await controller.setSettings({ ...controller.editableSettings(), enabled }).then(
			() => "",
			(error: unknown) => ` (not saved: ${errorMessage(error)})`,
		);
		const effective = controller.settings();
		ui.notify(
			`Speculative production ${effective.enabled ? "enabled" : "disabled"}${effective.enabled === enabled ? "" : " by the active project settings"}${saved}. Cache & reuse: ${onOff(effective.reuseEnabled)}; observation and learning remain active.`,
			saved ? "warning" : "info",
		);
		return;
	}
	if (command === "reset") {
		await controller.setSettings(undefined);
		ui.notify("Active speculative action settings reset.", "info");
	} else if (command === "events") {
		ui.notify(controller.recentEvents().join("\n") || "No speculative action events recorded yet.", "info");
	} else if (command === "status" || (!command && ctx.mode !== "tui")) {
		await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
		ui.notify(controller.statusText(), "info");
	} else if (command) ui.notify("Usage: /speculative-action [on|off|status|events|reset]", "warning");
	else await openSettings(ctx, controller);
}
const SETTINGS_PAGES: Readonly<Record<string, readonly string[]>> = {
	"Speculative action": [
		"enabled",
		"reuseEnabled",
		"Save settings to",
		"Prediction sources",
		"Tools & execution",
		"Advanced settings",
		"Apply changes",
		"Discard changes",
		"Status",
		"Recent events",
		"Restore defaults",
	],
	"Prediction sources": ["Model Drafter", "Prediction tools", "Actor probe", "Learned patterns"],
	"Advanced settings": ["Model Drafter", "Actor probe", "Learned patterns", "Concurrency and storage"],
	"Model Drafter": [
		"draftModel",
		...Object.keys(SETTING_LABELS).filter((key) => key.startsWith("drafter") || key === "candidateLimit"),
	],
	"Prediction tools": ["tools"],
	"Tools & execution": [
		"prepareObservedOperations",
		"Execution routes",
		"Check Linux environment",
		"Install / update Linux dependencies",
	],
	"Execution routes": [
		"executionRouting.primary",
		"executionRouting.nativeFallback",
		"searchExecution",
		"Refresh and show capabilities",
	],
	"Actor probe": Object.keys(SETTING_LABELS).filter((key) => key.startsWith("selfSpeculation.")),
	"Learned patterns": Object.keys(SETTING_LABELS).filter((key) => key.startsWith("patternAware.")),
	"Concurrency and storage": [
		...Object.keys(SETTING_LABELS).filter((key) =>
			/^(maxConcurrentActions|resourceCache|executionStore)/.test(key),
		),
		"Reclaim reusable command history",
		"Clear reusable command history",
	],
};

async function openSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	const { ui } = ctx;
	const pages = ["Speculative action"];
	let applied = controller.editableSettings();
	let draft = applied;
	const reload = () => {
		applied = controller.editableSettings();
		draft = applied;
	};
	const refresh = async () => {
		await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
		ui.notify(executionWorldSummary(controller.toolCapabilities(), controller.executionRoutes()), "info");
	};
	while (pages.length) {
		const page = pages.at(-1)!;
		const dirty = !isDeepStrictEqual(draft, applied);
		const global = controller.settingsScope() === "global";
		const menu = new Map<string, string>();
		const available = page === "Prediction tools" ? [...controller.toolCapabilities().keys()] : [];
		const tools = draft.tools ?? available;
		const labels: Record<string, string> =
			pages.length === 1
				? {
						"Apply changes": dirty ? "Apply changes (pending)" : "Apply changes",
						"Prediction sources": `Prediction sources › ${sourceSummary(draft)}`,
						"Advanced settings": "Advanced settings › tuning, decoding, concurrency, storage",
						"Save settings to": `Save settings to: ${global ? "All projects" : "This project"}${isDeepStrictEqual(applied, controller.settings()) ? "" : " (this project overrides shared settings)"}`,
					}
				: page === "Model Drafter"
					? { draftModel: `Model: ${draft.draftModel ?? activeModelReference(ctx)}` }
					: {};
		const fieldValue = (settings: EffectiveSpeculativeActionSettings, field: string) => {
			const [group, member] = field.split(".");
			const record: Record<string, unknown> = member ? Reflect.get(settings, group!) : settings;
			return record[member ?? group!];
		};
		for (const field of SETTINGS_PAGES[page]!) {
			if (!dirty && field === "Discard changes") continue;
			if (!global && ["selfSpeculation.endpoint", "selfSpeculation.apiKeyEnv"].includes(field)) continue;
			if (field === "tools") {
				menu.set("Use all available tools", field);
				for (const tool of available)
					menu.set(`[${tools.includes(tool) ? "x" : " "}] ${tool}`, `tool:${tool}`);
			} else if (SETTING_LABELS[field]) {
				const value = fieldValue(draft, field);
				const display =
					typeof value === "boolean"
						? onOff(value)
						: field.endsWith("Bytes")
							? formatBytes(Number(value))
							: (value ?? "None");
				menu.set(`${SETTING_LABELS[field]}: ${display}`, field);
			} else menu.set(labels[field] ?? field, field);
		}
		const choice = await ui.select(page, [...menu.keys(), pages.length === 1 ? CLOSE : BACK]);
		const field = choice && menu.get(choice);
		if (!field) {
			if (
				pages.length > 1 ||
				!dirty ||
				(await ui.confirm(
					"Discard changes?",
					"Close without applying the pending speculative-action changes?",
				))
			)
				pages.pop();
		} else if (SETTINGS_PAGES[field]) pages.push(field);
		else if (field === "Save settings to") {
			const scope = await ui.select(field, ["All projects", "This project", BACK]);
			if (scope !== "All projects" && scope !== "This project") continue;
			if (dirty && !(await ui.confirm("Discard changes?", "Switch configuration scope without applying?")))
				continue;
			controller.setSettingsScope(scope === "All projects" ? "global" : "project");
			reload();
		} else if (field === "Apply changes") {
			if (!dirty) {
				ui.notify("No pending speculative-action changes.", "info");
				continue;
			}
			try {
				await controller.setSettings(draft);
				ui.notify("Speculative-action settings applied.", "info");
			} catch (error) {
				ui.notify(
					`Speculative-action settings apply to this session but were not saved: ${errorMessage(error)}`,
					"error",
				);
			}
			reload();
		} else if (field === "Discard changes") draft = applied;
		else if (field === "Status" || field === "Recent events")
			await runCommand(field === "Status" ? "status" : "events", ctx, controller);
		else if (field === "Check Linux environment") await checkLinuxEnvironment(ctx, refresh);
		else if (field === "Install / update Linux dependencies") await installLinuxDependencies(ctx, refresh);
		else if (field === "Refresh and show capabilities") await refresh();
		else if (field.endsWith("reusable command history")) {
			const operation = field.startsWith("Clear") ? "clear" : "gc";
			if (
				operation === "clear" &&
				!(await ui.confirm(
					"Clear reusable command history?",
					"Delete all reusable command results and file effects?",
				))
			)
				continue;
			const report = await recoverSpeculation(() => controller.maintainExecutionStorage(operation));
			ui.notify(
				report?.text ?? "Reusable command history maintenance failed.",
				report && !report.failed ? "info" : "warning",
			);
		} else if (field === "Restore defaults") {
			if (
				!(await ui.confirm(
					"Restore defaults?",
					"Restore tuning values while keeping the production, reuse and source switches?",
				))
			)
				continue;
			const { enabled, reuseEnabled, drafterEnabled, tools, prepareObservedOperations } = draft;
			const { multiStepEnabled, presets } = draft.patternAware;
			const { forkEnabled, forkActionEnabled } = draft.selfSpeculation;
			draft = normalizeSpeculativeActionSettings({
				enabled,
				reuseEnabled,
				drafterEnabled,
				tools,
				prepareObservedOperations,
				patternAware: {
					enabled: draft.patternAware.enabled,
					multiStepEnabled,
					presets,
				},
				selfSpeculation: {
					enabled: draft.selfSpeculation.enabled,
					forkEnabled,
					forkActionEnabled,
				},
			});
		} else if (field === "tools" || field.startsWith("tool:")) {
			const tool = field.slice(5);
			const selection = tools.includes(tool) ? tools.filter((name) => name !== tool) : [...tools, tool];
			draft = normalizeSpeculativeActionSettings({
				...draft,
				tools: field === "tools" ? undefined : selection,
			});
		} else if (field === "draftModel") {
			const models = ctx.modelRegistry
				.getAvailable()
				.map((model) => `${model.provider}/${model.id}`)
				.sort();
			const active = `${USE_ACTIVE_MODEL} (${activeModelReference(ctx)})`;
			let model = await ui.select("Drafter model", [active, ...models, CUSTOM_MODEL, BACK]);
			if (!model || model === BACK) continue;
			if (model === CUSTOM_MODEL) model = await ui.input("Custom drafter model", "provider/model");
			if (model !== undefined)
				draft = normalizeSpeculativeActionSettings({
					...draft,
					draftModel: model === active ? undefined : model,
				});
		} else {
			const label = SETTING_LABELS[field]!;
			const value = fieldValue(draft, field);
			const bytes = field.endsWith("Bytes");
			if (field === "executionRouting.primary" && !controller.executionRoutes().primaryIDs.size) {
				ui.notify("No unified execution environment is installed for this profile.", "info");
				continue;
			}
			let next: unknown = !value;
			const choices =
				field === "selfSpeculation.forkTransport"
					? FORK_TRANSPORT_LABELS
					: field === "searchExecution"
						? { native: "Native Pi", captured: "Captured search (shared fixed search engine)" }
						: undefined;
			if (choices) {
				const choice = await ui.select(label, [...Object.values(choices), BACK]);
				next = Object.entries(choices).find(([, name]) => name === choice)?.[0];
				if (!next) continue;
			} else if (typeof value !== "boolean") {
				const input = await ui.input(
					label + (bytes ? " (MiB)" : ""),
					String(bytes ? Number(value) / 1048576 : (value ?? "")),
				);
				if (input === undefined) continue;
				next =
					typeof value === "number"
						? input.trim()
							? Number(input) * (bytes ? 1048576 : 1)
							: NaN
						: input.trim() || undefined;
				if (bytes && !Number.isSafeInteger(next)) {
					ui.notify(`Invalid ${label}.`, "warning");
					continue;
				}
			}
			const [group, member] = field.split(".");
			const patch = member ? { ...Reflect.get(draft, group!), [member]: next } : next;
			const updated = { ...draft, [group!]: patch };
			if (typeof next === "number") {
				if (field === "drafterTemperatureMin" && next > draft.drafterTemperatureMax)
					updated.drafterTemperatureMax = next;
				if (field === "drafterTemperatureMax" && next < draft.drafterTemperatureMin)
					updated.drafterTemperatureMin = next;
			}
			const normalized = normalizeSpeculativeActionSettings(updated);
			if (!isDeepStrictEqual(fieldValue(normalized, field), next)) ui.notify(`Invalid ${label}.`, "warning");
			else draft = normalized;
		}
	}
}
export function formatSpeculativeActionEvent(event: SpeculativeActionEvent<string>): string {
	const parts = [
		`[${event.type}]`,
		`session ${compactEventText(String(event.sessionID))}`,
		`turn ${compactEventText(event.turnID)}`,
	];
	switch (event.type) {
		case "task":
			parts.push(formatTaskTiming(event.timing));
			break;
		case "source_request":
			parts.push(
				event.request.request.source,
				event.request.settlement.status,
				formatDuration(event.request.durationMs),
			);
			break;
		case "prediction": {
			const settlement = event.settlement;
			parts.push(settlement.prediction.source, settlement.prediction.actionID);
			if (settlement.observation === "unobserved") parts.push(`unobserved ${causeSummary(settlement.cause)}`);
			else if (!settlement.match.matched) parts.push("not matched");
			else if (settlement.match.adoption.status === "adopted") parts.push("matched and adopted");
			else parts.push(`matched, rejected ${causeSummary(settlement.match.adoption.cause)}`);
			break;
		}
		case "candidate": {
			const { candidate, state } = event;
			const { route } = candidate;
			parts.push(
				`candidate ${compactEventText(candidate.id)}`,
				candidate.tool,
				candidate.source,
				`${route.backend}/${executionKinds[route.isolation]}/${route.reuse}`,
				state.status,
			);
			if (state.status === "running")
				parts.push(
					`${candidate.origin === "actor_preview" ? "previewed" : "predicted"} ${compactEventText(candidate.predictedAction)}`,
				);
			else if (state.status === "succeeded") {
				parts.push(formatDuration(state.executionMs));
				const reuse = candidate.world?.executionMetrics.reuse;
				if (reuse && hasProcessReuse(reuse)) parts.push(`Bash branch work ${formatProcessWorkReuse(reuse)}`);
			} else parts.push(causeSummary(state.cause), formatDuration(state.executionMs));
			break;
		}
		case "actor_action": {
			const { provider, tool, matchedPredictions } = event.settlement;
			const sources = [...new Set(matchedPredictions.map((prediction) => prediction.source))];
			parts.push(tool, sources.join("+") || (provider.kind === "speculative" ? "cache" : "no prediction"));
			if (provider.kind === "speculative") {
				const { match } = provider;
				parts.push(match.kind === "inputs" ? "sealed-input reuse" : "exact-action reuse");
			} else parts.push(`${formatDuration(provider.durationMs)} Actor ${provider.origin} execution`);
			parts.push(compactEventText(event.actualAction));
			break;
		}
	}
	return parts.join(" · ");
}
function causeSummary(value: {
	readonly stage: string;
	readonly code: string;
	readonly detail?: string;
}): string {
	return `${value.stage}:${value.code}${value.detail ? ` (${compactEventText(value.detail)})` : ""}`;
}
function compactEventText(value: string): string {
	const compact = value.replace(/\s+/g, " ").trim();
	const limit = speculativeConfig().runtime.logCharacters;
	return compact.length <= limit ? compact : `${compact.slice(0, Math.max(0, limit - 3))}...`;
}
function findExactModelReferenceMatch(
	reference: string,
	models: readonly Model<Api>[],
): Model<Api> | undefined {
	const normalized = reference.trim().toLowerCase();
	if (!normalized) return undefined;
	const canonical = models.filter((model) => `${model.provider}/${model.id}`.toLowerCase() === normalized);
	if (canonical.length === 1) return canonical[0];
	if (canonical.length > 1 || normalized.includes("/")) return undefined;
	const byID = models.filter((model) => model.id.toLowerCase() === normalized);
	return byID.length === 1 ? byID[0] : undefined;
}
const onOff = (value: boolean) => (value ? "On" : "Off");
function sourceSummary(settings: EffectiveSpeculativeActionSettings): string {
	if (!settings.enabled) return "Predictions off";
	const sources = [
		settings.drafterEnabled && "Model Drafter",
		settings.selfSpeculation.enabled && settings.selfSpeculation.forkEnabled && "Actor probe",
		settings.patternAware.enabled && "Learned patterns",
	]
		.filter(Boolean)
		.join(" + ");
	return sources || "No source enabled";
}
function formatDrafterBudgetStatus(snapshot: ActionDrafterBudgetSnapshot): string {
	const { budget, observations } = snapshot;
	return `Action Drafter usage: ${budget.requests} requests, ${budget.reportedTokens} reported tokens, ${budget.unreportedRequests} requests without usage\nPrivate observations: ${observations.forecasted} forecasted, ${observations.matched}/${observations.compared} exact matches against completed speculative results`;
}
const FORK_TRANSPORT_LABELS: Readonly<Record<SelfSpeculationSettings["forkTransport"], string>> = {
	provider: "Provider-integrated",
	sidecar: "Sidecar service",
	drafter: "Drafter reads Actor reasoning",
};
function formatSelfSpeculationStatus(bridge: SelfSpeculationCoordinatorSnapshot): string {
	return [
		`Self-speculation: ${bridge.bufferedCandidates} buffered`,
		...(bridge.resolvedActorProfile
			? [
					`Resolved Actor Profile: ${bridge.resolvedActorProfile}${bridge.profileResolutionSource ? ` (${bridge.profileResolutionSource})` : ""}`,
				]
			: []),
		`${bridge.candidateSubmissions} bundles/${bridge.candidateReceipts} receipts`,
		`${bridge.forkRequests}/${bridge.forkCompletions} probes completed (${bridge.forkRetries} later-snapshot retries)`,
		`${bridge.forkCandidates} fork candidates (${bridge.forkAgreements} source agreements, ${bridge.forkExactMatches} exact Actor matches)`,
		`${bridge.submittedDraftTokens} draft tokens registered (${bridge.acceptedDraftTokens} acknowledged)`,
		`${bridge.verifiedAcceptedDraftTokens}/${bridge.verifiedDraftTokens} target-verified accepted, ${bridge.verifiedRejectedDraftTokens} rejected, ${bridge.unresolvedDraftTokens} unresolved`,
		`${formatDuration(bridge.forkLatencyMs)} fork latency${bridge.forkMeanLogprob === undefined ? "" : `, mean logprob ${formatNumber(bridge.forkMeanLogprob)}`}`,
		`${bridge.failures} failures${bridge.lastError ? `, last error: ${bridge.lastError}` : ""}`,
	].join("; ");
}
function activeModelReference(ctx: ExtensionContext): string {
	return ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : "active model";
}
function bestCapabilityState(states: readonly ExecutionWorldHealthState[]): ExecutionWorldHealthState {
	return states.includes("ready") ? "ready" : states.includes("registered") ? "registered" : "unavailable";
}
function processRouteCapability(state: ProcessRouteSnapshot["state"]): ExecutionWorldHealthState {
	return state === "ready" ? "ready" : state === "idle" || state === "probing" ? "registered" : "unavailable";
}
function searchExecutionLabel(mode: string): string {
	return mode === "captured" ? "Captured search" : "Native Pi";
}
function processRouteLabel(state: ProcessRouteSnapshot["state"]): string {
	return {
		disabled: "Disabled",
		idle: "Idle",
		probing: "Checking",
		ready: "Ready",
		unavailable: "Unavailable",
	}[state];
}
const executionKinds: Record<SpeculativeExecution, string> = {
	runtime_sandbox: "isolated runtime",
	resource_snapshot: "sealed file inputs",
	workspace_branch: "private workspace",
};
const capabilityLabels: Record<ExecutionWorldHealthState, string> = {
	ready: "Ready",
	registered: "Check",
	unavailable: "Unavailable",
};
function formatSubcomputations(metrics: SpeculativeTraceSummary): string[] {
	return Object.entries(metrics.subcomputations ?? {}).map(([name, { hits, misses, reusedMs }]) =>
		"Step " + name + " " + formatRatio(hits, hits + misses) + " hits, " + formatDuration(reusedMs) + " reused",
	);
}
function formatSpeculativeFooter(
	settings: EffectiveSpeculativeActionSettings,
	metrics: SpeculativeActionMetrics,
	routes: ExecutionRoutesSnapshot,
): string {
	const { worlds, actorProcessReplay } = routes;
	const providers = worlds.length + (actorProcessReplay ? 1 : 0);
	const ready =
		worlds.filter((world) => world.state === "ready" || world.observation?.state === "ready").length +
		(actorProcessReplay && processRouteCapability(actorProcessReplay.state) === "ready" ? 1 : 0);
	const reuse = metrics.actorProcessReuse;
	const storageWorlds = worlds.filter((world) => world.storage);
	const storedEntries = storageWorlds.reduce((total, world) => total + (world.storage?.entries ?? 0), 0);
	const storedBytes = storageWorlds.reduce((total, world) => total + (world.storage?.bytes ?? 0), 0);
	return [
		`spec: ${settings.enabled ? "on" : "off"}, reuse: ${settings.reuseEnabled ? "on" : "off"}`,
		metrics.tasks > 0 ? `${formatSpeedup(metrics)}; ${formatHiddenCompute(metrics)}` : "Tool SpeedUp n/a",
		`tools reused ${formatRatio(metrics.speculativeHits, metrics.actorActions)}`,
		...formatSubcomputations(metrics),
		...(hasProcessReuse(reuse) ? [`Bash Actor ${formatActorProcessFooter(reuse)}`] : []),
		`live results ${metrics.cache.resultEntries}/${metrics.cache.cacheCapacity} (${formatBytes(metrics.cache.resultBytes)})`,
		...(storageWorlds.length ? [`reuse history ${storedEntries} entries (${formatBytes(storedBytes)})`] : []),
		providers > 0 ? `providers ${ready}/${providers} ready` : "providers probing",
		"unsafe→Actor",
	].join(" · ");
}
function executionWorldSummary(
	tools: ReadonlyMap<string, ToolCapabilityRow>,
	routes: ExecutionRoutesSnapshot,
): string {
	const { worlds, actorProcessReplay } = routes;
	if (!worlds.length && !actorProcessReplay) return "Execution capabilities: unavailable";
	return [
		"Execution capabilities:",
		capabilityTable(tools),
		...(routes.searchDetail ? [`Search executor: ${routes.searchDetail}`] : []),
		"Providers:",
		...(actorProcessReplay
			? [
					`- Native child reuse: ${processRouteLabel(actorProcessReplay.state)} — ${actorProcessReplay.detail}`,
				]
			: []),
		...worlds.map(
			(world) =>
				`- ${executionKinds[world.isolation]} (${world.id}): Fork ${capabilityLabels[world.state]} — ${world.detail}; Observe ${world.observation ? `${capabilityLabels[world.observation.state]} — ${world.observation.detail}` : "Unavailable"}${
					world.storage
						? `; storage ${world.storage.entries}/${world.storage.maxEntries}, ${formatBytes(world.storage.bytes)}/${formatBytes(world.storage.maxBytes)}, ${world.storage.orphanArtifacts ?? 0} orphan artifacts${world.storage.overBudget ? "; over budget" : ""}`
						: ""
				}`,
		),
	].join("\n");
}
function capabilityTable(tools: ReadonlyMap<string, ToolCapabilityRow>): string {
	const rows = [...tools].map(([tool, capability]) => [
		tool,
		capabilityLabels[capability.replay],
		capabilityLabels[capability.observe],
		capabilityLabels[capability.fork],
	]);
	const table = [["Tool", "Replay", "Observe", "Fork"], ...rows];
	const widths = table[0]!.map((_, column) => Math.max(...table.map((row) => row[column]!.length)));
	return table
		.map((row) =>
			row
				.map((cell, column) => cell.padEnd(widths[column]!))
				.join("  ")
				.trimEnd(),
		)
		.join("\n");
}
function tallySummary(
	counts: Readonly<Record<string, readonly number[]>>,
	format: (counts: readonly number[]) => string,
): string {
	return (
		Object.entries(counts)
			.map(([key, value]) => `${key} ${format(value)}`)
			.join("; ") || "none"
	);
}
function modeSummary(sources: SpeculativeTraceSummary["modesBySource"]): string {
	return (
		Object.entries(sources)
			.flatMap(([source, modes]) =>
				Object.entries(modes).map(([mode, result]) => {
					const label = source === "pattern_aware" ? mode : `${source}/${mode}`;
					return `${label}: ${result.matched}/${result.observed} matched, ${result.adopted} adopted; ${formatDuration(result.hiddenComputeMs)} hidden, ${formatDuration(result.productionMs)} production wall (${result.started} started)`;
				}),
			)
			.join("; ") || "none"
	);
}
function countSummary(counts: Readonly<Record<string, number>>): string {
	const entries = Object.entries(counts).sort(([leftKey, left], [rightKey, right]) =>
		right === left ? leftKey.localeCompare(rightKey) : right - left,
	);
	return entries.length > 0 ? entries.map(([key, count]) => `${key}=${count}`).join(", ") : "none";
}
type TimingSummary = Pick<SpeculativeTraceSummary, "toolComputeMs" | "hiddenComputeMs"> & {
	readonly hiddenComputeIncomplete?: true;
	readonly adoptionWaitMs?: number;
};
function formatHiddenCompute(timing: TimingSummary): string {
	return `${formatDuration(timing.hiddenComputeMs)} tool computation hidden${timing.hiddenComputeIncomplete ? " (known lower bound)" : ""}`;
}
function formatTaskTiming(timing: TimingSummary): string {
	return (
		`${formatSpeedup(timing)}; ${formatHiddenCompute(timing)}; ` +
		`${formatDuration(timing.toolComputeMs ?? NaN)} total tool computation` +
		(timing.adoptionWaitMs === undefined
			? ""
			: `; ${formatDuration(timing.adoptionWaitMs)} adoption/control wait`)
	);
}
function formatSpeedup(timing: TimingSummary): string {
	const ratio = toolSpeedup(timing);
	return `Tool SpeedUp ${ratio === null ? (!timing.hiddenComputeIncomplete && Number.isFinite(timing.toolComputeMs) && timing.toolComputeMs === timing.hiddenComputeMs && timing.hiddenComputeMs > 0 ? "fully hidden" : "n/a") : `${ratio.toFixed(2)}x`}`;
}
function formatDuration(ms: number): string {
	if (!Number.isFinite(ms)) return "n/a";
	const value = Math.max(0, ms);
	if (value > 0 && value < 1) return "<1ms";
	if (value < 1000) return `${Math.round(value)}ms`;
	if (value < 60000) return `${(value / 1000).toFixed(value < 10000 ? 1 : 0).replace(/\.0$/, "")}s`;
	const seconds = Math.round(value / 1000);
	return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}
function formatBytes(bytes: number): string {
	if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
	if (bytes < 1024) return `${Math.round(bytes)} B`;
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1).replace(/\.0$/, "")} KiB`;
	if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1).replace(/\.0$/, "")} MiB`;
	return `${(bytes / (1024 * 1024 * 1024)).toFixed(1).replace(/\.0$/, "")} GiB`;
}
function formatPercent(value: number): string {
	return `${Math.round(value * 100)}%`;
}
function formatNumber(value: number): string {
	return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));
}
function formatRatio(numerator: number, denominator: number): string {
	return `${numerator}/${denominator} (${denominator > 0 ? formatPercent(numerator / denominator) : "n/a"})`;
}
function hasProcessReuse(reuse: WorldReuseMetrics): boolean {
	return reuse.requests + reuse.wholeCommandRequests + reuse.fileInputHits + reuse.fileInputsPublished > 0;
}
function formatProcessWorkReuse(reuse: WorldReuseMetrics): string {
	const hits = reuse.hits + reuse.wholeCommandHits;
	const workMs = reuse.reusedProcessMs + reuse.wholeCommandReusedProcessMs;
	return [
		reuse.wholeCommandRequests
			? `whole ${formatRatio(reuse.wholeCommandHits, reuse.wholeCommandRequests)}`
			: "",
		reuse.requests ? `child ${formatRatio(reuse.hits, reuse.requests)}` : "",
		reuse.fileInputHits || reuse.fileInputsPublished
			? `${reuse.fileInputHits} file inputs reused, ${reuse.fileInputsPublished} published`
			: "",
		hits > 0
			? workMs > 0
				? `${formatDuration(workMs)} recorded process work reused`
				: "work timing unavailable"
			: "",
	]
		.filter(Boolean)
		.join("; ");
}
function formatActorProcessReuse(reuse: WorldReuseMetrics): string {
	const origins = [
		reuse.sameTurnHits ? `${reuse.sameTurnHits} same-turn` : "",
		reuse.crossTurnHits ? `${reuse.crossTurnHits} earlier-turn` : "",
		reuse.unattributedHits ? `${reuse.unattributedHits} stored` : "",
		reuse.joinedHits ? `${reuse.joinedHits} joined` : "",
	].filter(Boolean);
	return [formatProcessWorkReuse(reuse), ...origins].filter(Boolean).join("; ");
}
function formatActorProcessFooter(reuse: WorldReuseMetrics): string {
	const hits = reuse.hits + reuse.wholeCommandHits;
	const requests = reuse.requests + reuse.wholeCommandRequests;
	const workMs = reuse.reusedProcessMs + reuse.wholeCommandReusedProcessMs;
	return [`${formatRatio(hits, requests)} reused`, workMs > 0 ? `${formatDuration(workMs)} work` : ""]
		.filter(Boolean)
		.join(" · ");
}
