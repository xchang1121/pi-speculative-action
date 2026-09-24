import { AsyncLocalStorage } from "node:async_hooks";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { AgentMessage, AgentTool, AgentToolResult, AgentToolUpdateCallback } from "@earendil-works/pi-agent-core";
import type { Api, Model } from "@earendil-works/pi-ai";
import { convertToLlm, createLocalBashOperations, type ExtensionAPI, type ExtensionCommandContext, type ExtensionContext, type ExtensionFactory,
	type ExtensionUIContext, getAgentDir, getShellConfig, SettingsManager, type SourceInfo, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import {
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
	PI_ACTION_SEMANTICS,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "./action-semantics.ts";
import { ActorStreamPreviewTracker } from "./actor-stream-preview.ts";
import { createResourceSnapshotExecutionWorld, type AgentExecutionWorld } from "./agent-execution-world.ts";
import { createSpeculativeActionHost, normalizeSpeculativeAgentSettings } from "./agent-integration.ts";
import {
	clampCandidateLimit,
} from "./common.ts";
import type { DrafterUtilityGateSnapshot } from "./drafter-utility-gate.ts";
import type { PatternAwareSettings } from "./pattern-aware.ts";
import { createClosedSearchProfile, createPiToolDefinitions, PI_CLOSED_SEARCH_TOOLS, PI_OPERATION_TOOLS, resolvePiToolInvocation, type PiToolDefinition } from "./pi-tool-invocation.ts";
import type { ToolInvocation } from "./tool-settlement.ts";
import { LinuxProcessReuseBackend } from "./linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "./linux-process-world.ts";
import {
	executionCapabilityStatus,
	type ExecutionWorldDiagnosticSnapshot,
	type ExecutionWorldHealthState,
	type SpeculativeExecution,
	type WorldReuseMetrics,
} from "./execution-world.ts";
import {
	adaptProcessToolOperations,
	definedProcessEnvironment,
	ProcessExecutionCoordinator,
	type ProcessRouteSnapshot,
} from "./process-execution.ts";
import { DEFAULT_PROVENANCE_STORE_LIMITS } from "./reuse-store.ts";
import type { SpeculativeActionEvent } from "./runtime.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { nonEmptyTextInput, nonNegativeIntegerInput, nonNegativeNumberInput, optionalTextInput,
	positiveIntegerInput, positiveInteger, probabilityInput, settingInput, type SettingInputDescriptor } from "./setting-input.ts";
import { errorMessage } from "./error-utils.ts";
import {
	SelfSpeculationCoordinator,
	type SelfSpeculationCoordinatorSnapshot,
	type SelfSpeculationSettings,
} from "./self-speculation.ts";
import {
	type SpeculativeActionPackageSettings,
	SpeculativeActionSettingsStore,
	type SpeculativeSettingsScope,
} from "./settings-store.ts";
import { emptySpeculativeTraceSummary, reduceSpeculativeTrace, type SpeculativeTraceSummary } from "./trace-summary.ts";
import { resolvePatternWorkspaceIdentity } from "./workspace-identity.ts";
import { WorkspaceSandboxService } from "./workspace-sandbox.ts";

const STATUS_KEY = "speculative-action";
const CLOSE = "Close";
const BACK = "Back";
const USE_ACTIVE_MODEL = "Use active model";
const CUSTOM_MODEL = "Custom model...";
const RECENT_EVENT_LIMIT = 50;

export type EffectiveSpeculativeActionSettings = ReturnType<typeof normalizeSpeculativeActionSettings>;

type SettingInputDescriptors<T, K extends keyof T> = {
	readonly [Field in K]: SettingInputDescriptor<T[Field]>;
};

const ROOT_SETTING_INPUTS = {
	candidateLimit: positiveIntegerInput("Candidate requests per Actor decision", { transform: clampCandidateLimit }),
	maxConcurrentActions: positiveIntegerInput("Simultaneous speculative tools", { transform: clampCandidateLimit }),
	resourceCacheMaxEntries: positiveIntegerInput("Live result entries"),
	resourceCacheMaxBytes: mebibyteInput("Live result memory"),
	executionStoreMaxEntries: positiveIntegerInput("Reusable command history entries"),
	executionStoreMaxBytes: mebibyteInput("Reusable command history memory"),
	predictionTimeoutMs: positiveIntegerInput("Prediction wait limit (ms)"),
	drafterMaxTokens: positiveIntegerInput("Maximum Drafter output tokens"),
	drafterMaxDepth: nonNegativeIntegerInput("Drafter follow-up tool steps"),
	drafterDeterministicCandidates: nonNegativeIntegerInput("Temperature-0 Drafter candidates"),
} satisfies Partial<SettingInputDescriptors<EffectiveSpeculativeActionSettings, keyof EffectiveSpeculativeActionSettings>>;

const SELF_SPECULATION_INPUTS = {
	endpoint: settingInput("Control service URL", String, (input) => {
		const value = input.trim();
		return /^https?:\/\/[^\s]+$/u.test(value)
			? { ok: true, value }
			: { ok: false, error: "Endpoint must be an absolute HTTP(S) URL." };
	}),
	forkActionMinConfidence: probabilityInput("Minimum tool-name confidence"),
	forkGateMinSamples: positiveIntegerInput("Benefit-gate warm-up samples"),
	forkGateWindowSize: positiveIntegerInput("Benefit-gate rolling window"),
	forkGateMinNetBenefitMs: nonNegativeNumberInput("Minimum expected time saved (ms)"),
	forkGateProbeInterval: positiveIntegerInput("Recovery probe interval"),
	forkGateFailureThreshold: positiveIntegerInput("Consecutive-failure limit"),
	maxCandidates: positiveIntegerInput("Candidates sent per Actor decision"),
	maxDraftTokens: positiveIntegerInput("Draft-token limit per candidate"),
	actorProfile: nonEmptyTextInput("Actor tool-call Profile ('auto' to derive)"),
	draftFormat: nonEmptyTextInput("Target tool-call format"),
	draftBoundary: nonEmptyTextInput("Target tool-call boundary override ('auto' to derive)"),
	forkMaxTokens: positiveIntegerInput("Actor probe output-token limit"),
	timeoutMs: positiveIntegerInput("Control request timeout (ms)"),
	forkTemperature: nonNegativeNumberInput("Actor probe temperature"),
	forkDecoder: nonEmptyTextInput("Forked tool-call decoder"),
	forkForcedPrefix: nonEmptyTextInput("Forced tool-call prefix override ('auto' to derive)"),
	apiKeyEnv: optionalTextInput("Authentication token environment variable name (not the token)"),
} satisfies Partial<SettingInputDescriptors<SelfSpeculationSettings, keyof SelfSpeculationSettings>>;

const PATTERN_SETTING_INPUTS = {
	maxContextLength: positiveIntegerInput("Previous actions used as context"),
	maxFutureGap: nonNegativeIntegerInput("Maximum skipped Actor decisions"),
	futureGapCoverage: probabilityInput("Early-prediction coverage (0-1)", {
		error: "Early-prediction coverage must be between 0 and 1.",
	}),
	decayHalfLifeEvents: positiveIntegerInput("History half-life (events)", {
		error: "Pattern half-life must be a positive integer.",
	}),
	minOccurrences: positiveIntegerInput("Uses required before learning a pattern"),
	maxPatterns: positiveIntegerInput("Stored pattern limit"),
	beamWidth: positiveIntegerInput("Alternatives retained per tool"),
	maxPredictionDepth: positiveIntegerInput("Maximum predicted tool steps"),
	minBindingReplayProbability: probabilityInput("Minimum argument-replay confidence (0-1)", {
		error: "Minimum argument-replay confidence must be between 0 and 1.",
	}),
} satisfies Partial<SettingInputDescriptors<PatternAwareSettings, keyof PatternAwareSettings>>;

const DRAFTER_TEMPERATURE_INPUT = settingInput<readonly [number, number]>(
	"Drafter sampling temperature range",
	([lower, upper]) => `${formatNumber(lower)},${formatNumber(upper)}`,
	(input) => {
		const [lower, upper, ...extra] = input.split(",").map((item) => Number(item.trim()));
		return extra.length === 0 && Number.isFinite(lower) && Number.isFinite(upper) && lower >= 0 && upper >= lower
			? { ok: true, value: [lower, upper] as const }
			: {
					ok: false,
					error: "Drafter temperature range must be two non-negative comma-separated numbers in ascending order.",
				};
	},
);

export type SpeculativeActionMetrics = SpeculativeTraceSummary & {
	/** Reuse on the authoritative Actor path, excluding reuse internal to speculative branches. */
	readonly actorProcessReuse: WorldReuseMetrics;
};

type CapabilityState = "on" | "off" | ExecutionWorldHealthState;
type ToolCapabilityRow = Readonly<Record<"predict" | "replay" | "observe" | "fork", CapabilityState>>;
type ExecutionRoutesSnapshot = {
	readonly worlds: readonly ExecutionWorldDiagnosticSnapshot[];
	readonly actorProcessReplay?: ProcessRouteSnapshot;
	readonly primaryIDs: ReadonlySet<string>;
	readonly searchDetail?: string;
};

export type SpeculativeSettingsStore = Pick<SpeculativeActionSettingsStore,
	"scope" | "load" | "effective" | "editable" | "setEffective" | "clear" | "setScope" | "flush">;

type SpeculativeActionController = Readonly<Awaited<ReturnType<typeof installController>>>;

export interface SpeculativeActionExtensionDependencies {
	readonly createExecutionWorlds?: (
		context: SpeculativeActionExecutionWorldContext,
	) => readonly AgentExecutionWorld[];
	readonly createHost?: typeof createSpeculativeActionHost;
	readonly createSettingsStore?: (cwd: string) => SpeculativeSettingsStore;
	readonly createWorkspaceSandboxService?: () => WorkspaceSandboxService;
	readonly selfSpeculationFetch?: typeof globalThis.fetch;
}

export interface SpeculativeActionExecutionWorldContext {
	readonly cwd: string;
	readonly autoResizeImages: boolean;
}

export function normalizeSpeculativeActionSettings(
	input: SpeculativeActionPackageSettings | undefined,
) {
	return {
		...normalizeSpeculativeAgentSettings(input),
		searchExecution: input?.searchExecution === "captured" ? "captured" : "native",
		...(typeof input?.draftModel === "string" && input.draftModel.trim()
			? { draftModel: input.draftModel.trim() }
			: {}),
		executionStoreMaxEntries: positiveInteger(
			input?.executionStoreMaxEntries,
			DEFAULT_PROVENANCE_STORE_LIMITS.maxCertificates,
		),
		executionStoreMaxBytes: positiveInteger(input?.executionStoreMaxBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes),
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
		`Enabled: ${settings.enabled ? "On" : "Off"}`,
		`Model Drafter: ${settings.drafterEnabled ? "On" : "Off"}`,
		`Drafter model: ${settings.draftModel ?? "active model"}`,
		`Candidate requests per Actor decision: ${settings.candidateLimit}`,
		`Model Drafter policy: ${settings.drafterMaxDepth} follow-up steps; ${settings.drafterMaxTokens} tokens; ${settings.drafterDeterministicCandidates} temperature-0 candidates; sampling ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
		`Simultaneous speculative tools: ${settings.maxConcurrentActions}`,
		`Storage policy: ${settings.resourceCacheMaxEntries} live results/${formatBytes(settings.resourceCacheMaxBytes)}; ${settings.executionStoreMaxEntries} reusable commands/${formatBytes(settings.executionStoreMaxBytes)}`,
		`Prediction wait limit: ${formatDuration(settings.predictionTimeoutMs)}`,
		`Learned patterns: ${settings.patternAware.enabled ? "On" : "Off"}; follow-up steps: ${settings.patternAware.multiStepEnabled ? "On" : "Off"} (alternatives/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}, learn after ${settings.patternAware.minOccurrences}, replay confidence≥${formatPercent(settings.patternAware.minBindingReplayProbability)}, gap ${settings.patternAware.maxFutureGap}, coverage ${formatPercent(settings.patternAware.futureGapCoverage)}, half-life ${settings.patternAware.decayHalfLifeEvents})`,
		`Actor probe: ${self.enabled && self.forkEnabled ? `On (${self.forkTransport})` : "Off"}; target verification ${self.enabled ? "On" : "Off"}; early tool execution ${self.enabled && self.forkTransport === "sidecar" && self.forkEnabled && self.forkActionEnabled ? `On (tool-name confidence ≥${formatPercent(self.forkActionMinConfidence)})` : "Off"}; benefit control ${self.forkGateEnabled ? `On (${self.forkGateWindowSize} samples, ≥${formatDuration(self.forkGateMinNetBenefitMs)} net)` : "Off"}; ${self.maxCandidates} candidates × ${self.maxDraftTokens} draft tokens; Actor Profile=${self.actorProfile}; ${self.draftFormat} (${syntaxSettingLabel(self.draftBoundary)} boundary); ${self.forkTransport === "sidecar" ? self.endpoint : "provider-integrated"}`,
		`Prediction tools: ${toolsSummary(settings.tools)}`,
		`Execution routing: unified ${settings.executionRouting.primary ? "On" : "Off"}; native fallback ${settings.executionRouting.nativeFallback ? "On" : "Off"}; Actor always available`,
		`Search execution when enabled: ${searchExecutionLabel(settings.searchExecution)}`,
		`Tool calls reused: ${formatRatio(metrics.speculativeHits, metrics.actorActions)}; ${metrics.exactReuseHits} exact, ${metrics.partialResultReuseHits} partial, ${metrics.inputReuseHits} inputs; ${formatDuration(metrics.executionAheadMs)} ready early, ${formatDuration(metrics.hitLatencyMs)} wait after match`,
		...(hasProcessReuse(metrics.actorProcessReuse)
			? [`Bash Actor reuse: ${formatActorProcessReuse(metrics.actorProcessReuse)}`]
			: []),
		...(hasProcessReuse(metrics.processReuse)
			? [`Bash work reused inside speculative branches: ${formatProcessWorkReuse(metrics.processReuse)}`]
			: []),
		`Predictions: ${formatRatio(metrics.predictionsMatched, metrics.predictionsObserved)} matched; ${formatRatio(metrics.predictionsAdopted, metrics.predictionsMatched)} adopted; unobserved: ${metrics.predictionsSettled - metrics.predictionsObserved}`,
		`Prediction rejections after match: ${countSummary(metrics.predictionRejectedAfterMatch)}`,
		`Actor candidate rejections: ${countSummary(metrics.actorCandidateRejections)}`,
		`Candidates: ${metrics.candidateStarted} started; ${metrics.candidateSucceeded} succeeded; ${metrics.candidateFailed} failed; ${metrics.candidateCancelled} cancelled`,
		metrics.tasks > 0
			? `Task timing (${metrics.tasks} completed): ${formatTaskTiming(metrics)}. Estimated savings are optimistic, not a measured no-speculation comparison.`
			: "Task timing: n/a (no completed task); serialized overlap and speedup are not reported as 0.",
		`Draft tokens: ${metrics.totalDraftTokens}`,
		`Live speculative results: ${cache.resultEntries}/${cache.cacheCapacity}, ${formatBytes(cache.resultBytes)}/${formatBytes(cache.cacheByteCapacity ?? 0)}; cold: ${cache.cacheCold}; hot: ${cache.cacheHot}; jobs: ${cache.inFlightJobs}; branches: ${cache.branchEntries} (${formatBytes(cache.branchBytes)})`,
	].join("\n");
}

export function createSpeculativeActionExtension(
	dependencies: SpeculativeActionExtensionDependencies = {},
): ExtensionFactory {
	return (pi) => {
		let controller: SpeculativeActionController | undefined;
		const sessions = new RuntimeLifecycleLane();
		const wrapperSources = new Map<string, string>();
		const actorStream = new ActorStreamPreviewTracker();
		const providerRequest = new AsyncLocalStorage<"drafter">();

		pi.on("before_provider_request", (event) =>
			providerRequest.getStore() === "drafter" ? event.payload : controller?.decorateActorPayload(event.payload),
		);

		pi.on("session_start", (_event, ctx) => sessions.run(async () => {
			await controller?.dispose();
			controller = await installController(ctx, pi, dependencies, wrapperSources, providerRequest);
			controller.attachUI(ctx.ui);
		}));
		pi.on("context", async (event, ctx) => {
			actorStream.clear();
			await controller?.startTurn(event.messages, ctx);
		});
		pi.on("message_update", (event, ctx) => {
			controller?.observeActorOutput(event.assistantMessageEvent);
			for (const preview of actorStream.observe(event.assistantMessageEvent)) {
				if (preview.type === "tool") {
					controller?.previewActorTool(preview.tool, ctx.signal);
				} else {
					controller?.previewActorCall(preview.call.name, preview.call.id, preview.call.arguments, ctx.signal);
				}
			}
		});
		pi.on("turn_end", async () => {
			await controller?.finishTurn(false);
		});
		pi.on("agent_end", async () => {
			await controller?.finishTurn(true);
		});
		pi.on("session_shutdown", (_event, ctx) => sessions.run(async () => {
			ctx.ui.setStatus(STATUS_KEY, undefined);
			const current = controller;
			controller = undefined;
			current?.detachUI();
			await current?.dispose().catch(() => undefined);
		}));

		const command = {
			description: "Configure speculative action pre-execution",
			handler: (args: string, ctx: ExtensionCommandContext) => runCommand(args, ctx, controller),
		};
		pi.registerCommand("speculative-action", command);
	};
}

const speculativeActionExtension = createSpeculativeActionExtension();
export default speculativeActionExtension;

async function installController(
	context: ExtensionContext,
	pi: ExtensionAPI,
	dependencies: SpeculativeActionExtensionDependencies,
	wrapperSources: Map<string, string>,
	providerRequest: AsyncLocalStorage<"drafter">,
) {
	let ui: ExtensionUIContext | undefined;
	let latestContext = context;
	let currentTurnID: string | undefined, unavailableDraftModel: string | undefined;
	const sessionID = context.sessionManager.getSessionId();
	let lastTurnID: string | undefined;
	let turnSequence = 0;
	let turnTools: readonly AgentTool[] = [];
	const recentEvents: string[] = [];
	const settingsStore =
		dependencies.createSettingsStore?.(context.cwd) ?? new SpeculativeActionSettingsStore(context.cwd);
	await settingsStore.load(context.isProjectTrusted());
	let currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
	let currentMetrics: SpeculativeTraceSummary = emptySpeculativeTraceSummary({
		cacheCapacity: currentSettings.resourceCacheMaxEntries, cacheByteCapacity: currentSettings.resourceCacheMaxBytes,
	});
	const settings = () => currentSettings;
	const lifecycle = new RuntimeLifecycleLane();
	type SearchProfile = Awaited<ReturnType<typeof createClosedSearchProfile>>;
	type SearchRoute = { ready: Promise<SearchProfile | undefined>; profile?: SearchProfile };
	let search: SearchRoute | undefined;
	const closedSearchEnabled = () => !lifecycle.sealed && currentSettings.enabled && currentSettings.searchExecution !== "native";
	const prepareSearch = (): Promise<SearchProfile | undefined> => {
		if (search) return search.ready;
		const entry: SearchRoute = { ready: createClosedSearchProfile(context.cwd).then(
				(profile) => (entry.profile = profile), () => undefined) };
		search = entry; return entry.ready;
	};
	const resetSearch = async () => {
		const previous = search; search = undefined;
		await previous?.ready.then((profile) => profile?.pool.dispose());
	};
	const selfSpeculation = new SelfSpeculationCoordinator({
		settings: () => {
			const configured = settings().selfSpeculation;
			return settings().enabled ? configured : { ...configured, enabled: false };
		},
		...(dependencies.selfSpeculationFetch ? { fetch: dependencies.selfSpeculationFetch } : {}),
	});
	const [piToolSettings, patternWorkspaceIdentity] = await Promise.all([
		loadPiToolSettings(context),
		resolvePatternWorkspaceIdentity(context.cwd),
	]);
	const primaryExecutionWorlds = dependencies.createExecutionWorlds?.({
		cwd: context.cwd,
		autoResizeImages: piToolSettings.autoResizeImages,
	}) ?? [];
	const processBackend = new LinuxProcessReuseBackend({
		storeRoot: path.join(getAgentDir(), "speculative-action", "process-reuse"),
	});
	const shell = getShellConfig(piToolSettings.shellPath);
	const actorReplayEnabled = () => currentSettings.enabled;
	const rawProcessExecutor = adaptProcessToolOperations(createLocalBashOperations({ shellPath: shell.shell }));
	const processCoordinator = new ProcessExecutionCoordinator(
		rawProcessExecutor,
		{
			enabled: actorReplayEnabled,
			prepare: (refresh) => processBackend.prepareActorReplay(rawProcessExecutor, {
				sourceRoot: context.cwd,
				...(shell.commandTransport !== "stdin" ? { held: {
					realShell: shell.shell,
					executor: (shellPath) => adaptProcessToolOperations(createLocalBashOperations({ shellPath })),
				} } : {}),
				invocation: (request) => resolvePiToolInvocation("bash", {
					command: request.command,
					...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
				}, {
					cwd: request.cwd,
					environment: definedProcessEnvironment(request.environment),
					...(piToolSettings.shellPath ? { shellPath: piToolSettings.shellPath } : {}),
				})?.process,
			}, refresh),
			reset: () => processBackend.resetActorReplay(),
		},
	);
	const workspaceSandbox = dependencies.createWorkspaceSandboxService?.() ?? new WorkspaceSandboxService();
	const executionWorlds = [
		...new Set([
			...primaryExecutionWorlds,
			createLinuxProcessExecutionWorld({
				coordinator: processCoordinator,
				tools: PI_OPERATION_TOOLS.process,
				backend: processBackend,
				storeRoot: path.join(getAgentDir(), "speculative-action", "process-reuse"),
				workspaceSandbox,
			}),
			workspaceSandbox.createExecutionWorld(),
			createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, {
				tools: [...PI_OPERATION_TOOLS.resources, ...PI_CLOSED_SEARCH_TOOLS], maxBytes: () => currentSettings.resourceCacheMaxBytes,
			}),
		]),
	];
	const primaryExecutionWorldIDs = new Set(primaryExecutionWorlds.map((world) => world.id));
	const speculativeExecutionWorldEnabled = (backend: string): boolean =>
		currentSettings.enabled && (primaryExecutionWorldIDs.has(backend)
			? currentSettings.executionRouting.primary
			: currentSettings.executionRouting.nativeFallback);
	const configureExecutionStorage = () => {
		for (const world of executionWorlds)
			world.storage?.configure({
				maxEntries: currentSettings.executionStoreMaxEntries,
				maxBytes: currentSettings.executionStoreMaxBytes,
			});
	};
	configureExecutionStorage();
	let executionDiagnostics: readonly ExecutionWorldDiagnosticSnapshot[] = [];
	const executionRoutes = (): ExecutionRoutesSnapshot => ({
		worlds: executionDiagnostics, actorProcessReplay: processCoordinator.actorDiagnostics(), primaryIDs: primaryExecutionWorldIDs,
		searchDetail: !closedSearchEnabled() || (search && !search.profile) ? "Native Pi" : `${searchExecutionLabel(currentSettings.searchExecution)}; ${search?.profile ? [...search.profile.invocations.keys()].join(", ") + " prepared" : "not checked"}`,
	});
	const availableTools = new Map(pi.getAllTools().map((tool) => [tool.name, tool]));
	const toolConflicts = new Map<string, string>();
	// Pi exposes metadata, but not another extension's execute function. Only stock tools and our own
	// wrappers can be intercepted without silently substituting different tool semantics.
	const baseDefinitions = new Map(
		[...createPiToolDefinitions(context.cwd, {
			read: { autoResizeImages: piToolSettings.autoResizeImages },
			bash: {
				operations: processCoordinator.operations,
				shellPath: piToolSettings.shellPath,
				commandPrefix: piToolSettings.shellCommandPrefix,
			},
		})].filter(([name]) => {
			const available = availableTools.get(name);
			if (!available) return false;
			const source = toolSourceFingerprint(available.sourceInfo);
			if (available.sourceInfo.source === "builtin" || wrapperSources.get(name) === source) return true;
			toolConflicts.set(name, `${available.sourceInfo.source}: ${available.sourceInfo.path}`);
			return false;
		}),
	);
	const agentTools = new Map(
		[...baseDefinitions].map(([name, definition]) => [name, toAgentTool(definition, () => latestContext)]),
	);
	const toolCapabilities = () => resolveToolCapabilities(
		currentSettings, baseDefinitions.keys(), toolConflicts, executionRoutes(),
		closedSearchEnabled() ? search?.profile?.invocations : undefined,
	);
	const runtimeSettings = () => ({
		...currentSettings,
		tools: predictionTools(currentSettings, baseDefinitions.keys()),
	});
	const visibleMetrics = (): SpeculativeActionMetrics => ({
		...currentMetrics,
		actorProcessReuse: processBackend.actorMetrics(),
	});
	function renderFooter(): void {
		if (!ui) return;
		ui.setStatus(
			STATUS_KEY,
			formatSpeculativeFooter(settings(), visibleMetrics(), executionRoutes(), toolConflicts.size),
		);
	}
	const host = (dependencies.createHost ?? createSpeculativeActionHost)(sessionID, {
		cwd: context.cwd,
		getSettings: runtimeSettings,
		// Registry completion takes raw per-API options; Drafter options are simple (reasoning levels) like the Actor's.
		complete: (model, llmContext, options) => providerRequest.run("drafter", async () => {
			const registry = latestContext.modelRegistry, provider = registry.getProvider(model.provider), auth = await registry.getApiKeyAndHeaders(model);
			if (!provider || !auth.ok) throw new Error(auth.ok ? `Unknown provider: ${model.provider}` : auth.error);
			return provider.streamSimple({ ...model, baseUrl: auth.baseUrl ?? model.baseUrl }, llmContext, { ...options, apiKey: options?.apiKey ?? auth.apiKey,
				headers: { ...auth.headers, ...options?.headers }, env: { ...auth.env, ...options?.env } }).result();
		}),
		draftModel: (actorModel) => {
			const reference = settings().draftModel, model = reference ? findExactModelReferenceMatch(reference, latestContext.modelRegistry.getAvailable()) : actorModel;
			if (!model && reference !== unavailableDraftModel) ui?.notify(`Drafter model ${reference} is unavailable; drafting with the active model.`, "warning");
			unavailableDraftModel = model ? undefined : reference;
			return model ?? actorModel;
		},
		preflight: ({ toolName }) =>
			latestContext.isProjectTrusted() && baseDefinitions.has(toolName) && pi.getActiveTools().includes(toolName),
		resolveInvocation: async (tool, input) => {
			if (closedSearchEnabled() && PI_CLOSED_SEARCH_TOOLS.includes(tool)) {
				const invocation = (await prepareSearch())?.invocations.get(tool);
				if (invocation) return invocation; // Qualification precedes identity binding; never switch an admitted call.
			}
			return resolvePiToolInvocation(tool, input, {
				cwd: latestContext.cwd,
				environment: piShellEnvironment(latestContext),
				autoResizeImages: piToolSettings.autoResizeImages,
				modelSupportsImages: latestContext.model?.input.includes("image") ?? true,
				...(piToolSettings.shellPath ? { shellPath: piToolSettings.shellPath } : {}),
				...(piToolSettings.shellCommandPrefix ? { shellCommandPrefix: piToolSettings.shellCommandPrefix } : {}),
			});
		},
		executionWorlds,
		speculativeExecutionWorldEnabled,
		actorForkPlanSource: selfSpeculation.actorForkPlanSource,
		patternStateDirectory: getAgentDir(),
		patternWorkspaceIdentity,
		onTurnStarted: ({ turnID, actorModel, context: actorContext, decisionSequence }) =>
			selfSpeculation.startTurn(turnID, actorModel, actorContext, decisionSequence),
		onCandidateMaterialized: (candidate) => selfSpeculation.addCandidate(candidate),
		onActorActionMaterialized: ({ action }) => selfSpeculation.observeActorAction(action),
		onActorActionSettled: ({ settlement }) => selfSpeculation.observeActorSettlement(settlement),
		onPredictionSettled: (feedback) => selfSpeculation.observePredictionSettlement(feedback),
		onEvent: (event) => {
			currentMetrics = reduceSpeculativeTrace(currentMetrics, event);
			recentEvents.push(formatSpeculativeActionEvent(event));
			if (recentEvents.length > RECENT_EVENT_LIMIT) recentEvents.splice(0, recentEvents.length - RECENT_EVENT_LIMIT);
			renderFooter();
		},
	});
	const refreshExecutionDiagnostics = (refresh = false): Promise<void> => lifecycle.admit(async () => {
		if (refresh) await resetSearch();
		const preparations = [
			closedSearchEnabled() ? prepareSearch() : undefined,
			refresh ? processCoordinator.refreshActorRoute() : undefined,
			Promise.resolve().then(() => host.executionWorldDiagnostics(refresh && currentSettings.enabled)),
		] as const;
		// A failed provider cannot detach preparation from refresh or shutdown ownership.
		const [, , diagnostics] = await Promise.all(preparations).finally(() => Promise.allSettled(preparations));
		executionDiagnostics = diagnostics;
		await recoverSpeculation(() => host.runtime.settingsChanged(runtimeSettings()));
		renderFooter();
	});

	const controller = {
		settings,
		editableSettings: () => normalizeSpeculativeActionSettings(settingsStore.editable()),
		settingsScope: () => settingsStore.scope,
		setSettingsScope: (scope: SpeculativeSettingsScope) => settingsStore.setScope(scope),
		metrics: visibleMetrics,
		registeredTools: (): ReadonlySet<string> => new Set(baseDefinitions.keys()),
		toolCapabilities,
		toolConflicts: (): ReadonlyMap<string, string> => new Map(toolConflicts),
		recentEvents: (): readonly string[] => [...recentEvents],
		refreshExecutionDiagnostics,
		executionRoutes,
		maintainExecutionStorage: async (operation: "gc" | "clear") => {
			const controls = executionWorlds.flatMap((world) => (world.storage ? [world.storage] : []));
			if (!controls.length) return { text: "No execution world exposes persistent storage.", failed: true };
			let entries = 0, artifacts = 0, bytes = 0, failed = 0;
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
			return { text: `Reusable command history ${operation === "gc" ? "reclaimed" : "cleared"}: ${entries} entries, ${artifacts} artifacts, ${formatBytes(bytes)}${failed ? `; ${failed} execution worlds failed` : ""}.`, failed: failed > 0 };
		},
		setSettings: async (value: SpeculativeActionPackageSettings | undefined) => {
			const previous = currentSettings;
			if (value) settingsStore.setEffective(value, normalizeSpeculativeActionSettings(settingsStore.scope === "project" ? settingsStore.editable("global") : undefined));
			else settingsStore.clear();
			currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
			configureExecutionStorage();
			if (!currentSettings.enabled || !currentSettings.selfSpeculation.enabled) selfSpeculation.reset();
			await recoverSpeculation(() => refreshExecutionDiagnostics(
				previous.enabled !== currentSettings.enabled ||
				previous.searchExecution !== currentSettings.searchExecution ||
				previous.executionRouting.primary !== currentSettings.executionRouting.primary ||
				previous.executionRouting.nativeFallback !== currentSettings.executionRouting.nativeFallback,
			));
			await settingsStore.flush(); // Applied for this session either way; callers report whether it was saved.
		},
		attachUI: (nextUI: ExtensionUIContext) => {
			ui = nextUI;
			renderFooter();
		},
		detachUI: () => {
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
		},
		startTurn: async (messages: AgentMessage[], nextContext: ExtensionContext) => {
			latestContext = nextContext;
			const model = nextContext.model;
			if (!model) return;
			try {
				if (currentTurnID) await host.finishTurn(currentTurnID);
				currentTurnID = `turn_${++turnSequence}`;
				turnTools = pi
					.getActiveTools()
					.map((name) => agentTools.get(name))
					.filter((tool): tool is AgentTool => tool !== undefined);
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
			if (!turnID || !baseDefinitions.has(tool)) return;
			void recoverSpeculation(() =>
				host.previewActorCall({ turnID, id: callID, tool, args: input, tools: turnTools }, signal),
			);
		},
		previewActorTool: (tool: string, signal?: AbortSignal) => {
			const turnID = currentTurnID;
			if (!turnID || !baseDefinitions.has(tool)) return;
			void recoverSpeculation(() => host.previewActorTool({ turnID, tool }, signal));
		},
		decorateActorPayload: (payload: unknown) => selfSpeculation.decorateActorPayload(payload),
		observeActorOutput: (event: Parameters<SelfSpeculationCoordinator["observeActorOutput"]>[0]) => selfSpeculation.observeActorOutput(event),
		selfSpeculationSnapshot: () => selfSpeculation.snapshot(),
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
		execute: async (tool: string, callID: string, input: unknown, signal: AbortSignal | undefined,
			onUpdate: AgentToolUpdateCallback<unknown> | undefined, nextContext: ExtensionContext): Promise<AgentToolResult<unknown>> => {
			latestContext = nextContext;
			const definition = baseDefinitions.get(tool);
			if (!definition) throw new Error(`Speculative wrapper has no base tool ${tool}`);
			const turnID = currentTurnID;
			return processCoordinator.runActor(turnID ? { sessionID, turnID } : undefined, () => host.execute(
				{ ...(turnID ? { turnID } : {}), id: callID, tool, args: input, tools: turnTools },
				signal,
				async (operation) =>
					operation.invocation?.authoritative ? (await operation.invocation.authoritative({
						callID, args: operation.input, signal: operation.signal ?? new AbortController().signal,
					})).result : await definition.execute(callID, operation.input as never, operation.signal, onUpdate as never, nextContext),
			));
		},
		statusText: () => {
			const effective = settings();
			return [
				formatSpeculativeActionStatus({ settings: { ...effective, tools: runtimeSettings().tools }, metrics: visibleMetrics() }),
				formatDrafterGateStatus(effective.drafterGateEnabled, host.drafterGateSnapshot()),
				formatSelfSpeculationStatus(selfSpeculation.snapshot()),
				executionWorldSummary(toolCapabilities(), executionRoutes()),
				`Custom tool conflicts: ${toolConflictSummary(toolConflicts)}`,
			].join("\n");
		},
		dispose: () => {
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
			return lifecycle.close(() => Promise.resolve().then(() => settingsStore.flush())
				.finally(() => processCoordinator.dispose().catch(() => undefined))
				.finally(() => host.dispose())
				.finally(() => workspaceSandbox.dispose())
				.finally(() => selfSpeculation.dispose())
				.finally(resetSearch));
		},
	} as const;
	for (const definition of baseDefinitions.values())
		pi.registerTool(speculativeToolDefinition(definition, controller));
	const registeredTools = new Map(pi.getAllTools().map((tool) => [tool.name, tool]));
	for (const name of baseDefinitions.keys()) {
		const registered = registeredTools.get(name);
		if (registered) wrapperSources.set(name, toolSourceFingerprint(registered.sourceInfo));
	}
	await recoverSpeculation(() => refreshExecutionDiagnostics());
	return controller;
}

async function recoverSpeculation<T>(operation: () => Promise<T>): Promise<T | undefined> {
	try {
		return await operation();
	} catch {
		return undefined;
	}
}

interface PiToolSettings {
	readonly shellPath?: string;
	readonly shellCommandPrefix?: string;
	readonly autoResizeImages: boolean;
}

function speculativeToolDefinition(base: PiToolDefinition, controller: SpeculativeActionController): ToolDefinition {
	return {
		...base,
		renderCall: base.renderCall as ToolDefinition["renderCall"],
		renderResult: base.renderResult as ToolDefinition["renderResult"],
		async execute(callID, input, signal, onUpdate, context) {
			return controller.execute(base.name, callID, input, signal, onUpdate, context);
		},
	};
}

function toolSourceFingerprint(source: SourceInfo): string {
	return [source.path, source.source, source.scope, source.origin, source.baseDir ?? ""].join("\0");
}

function toolConflictSummary(conflicts: ReadonlyMap<string, string>): string {
	if (conflicts.size === 0) return "none";
	return [...conflicts]
		.sort(([left], [right]) => left.localeCompare(right))
		.map(([tool, source]) => `${tool} (${source}); excluded from speculation`)
		.join(", ");
}

function toAgentTool(base: PiToolDefinition, context: () => ExtensionContext): AgentTool {
	return {
		...base,
		execute: (callID, input, signal, onUpdate) => base.execute(callID, input as never, signal, onUpdate as never, context()),
	};
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

function loadPiToolSettings(context: ExtensionContext): PiToolSettings {
	const settings = SettingsManager.create(context.cwd, getAgentDir(), {
		projectTrusted: context.isProjectTrusted(),
	});
	const shellPath = settings.getShellPath();
	const shellCommandPrefix = settings.getShellCommandPrefix();
	return {
		...(shellPath ? { shellPath } : {}),
		...(shellCommandPrefix ? { shellCommandPrefix } : {}),
		autoResizeImages: settings.getImageAutoResize(),
	};
}

async function runCommand(
	args: string,
	ctx: ExtensionCommandContext,
	controller: SpeculativeActionController | undefined,
): Promise<void> {
	if (!controller) {
		ctx.ui.notify("Speculative action runtime is unavailable.", "error");
		return;
	}
	const command = args.trim().toLowerCase();
	if (command === "on" || command === "off") {
		const saved = await controller.setSettings({ ...controller.editableSettings(), enabled: command === "on" }).then(() => "", (error: unknown) => ` (not saved: ${errorMessage(error)})`);
		ctx.ui.notify(`Speculative action ${controller.settings().enabled ? "enabled" : "disabled"}${controller.settings().enabled === (command === "on") ? "" : " by the active project settings"}${saved}.`, saved ? "warning" : "info");
		return;
	}
	if (command === "reset") {
		await controller.setSettings(undefined);
		ctx.ui.notify("Active speculative action settings reset.", "info");
		return;
	}
	if (command === "events") {
		showRecentEvents(ctx, controller);
		return;
	}
	if (command === "status" || (command === "" && ctx.mode !== "tui")) {
		await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
		ctx.ui.notify(controller.statusText(), controller.settings().enabled ? "info" : "warning");
		return;
	}
	if (command) {
		ctx.ui.notify("Usage: /speculative-action [on|off|status|events|reset]", "warning");
		return;
	}
	await openSettings(ctx, controller);
}

async function openSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	let applied = structuredClone(controller.editableSettings());
	let draft = structuredClone(applied);
	const editor: SpeculativeActionController = {
		...controller,
		settings: () => draft,
		setSettings: async (value) => {
			draft = structuredClone(normalizeSpeculativeActionSettings(value));
		},
	};
	const reload = () => {
		applied = structuredClone(controller.editableSettings());
		draft = structuredClone(applied);
	};
	while (true) {
		const dirty = !isDeepStrictEqual(draft, applied);
		const toolPolicy = toolPolicyCounts(draft, controller.registeredTools());
		const scope = controller.settingsScope() === "global" ? "All projects" : "This project";
		const actions = new Map<string, MenuAction>([
			[`Enabled: ${draft.enabled ? "On" : "Off"}`, () => editor.setSettings({ ...draft, enabled: !draft.enabled })],
			[`Save settings to: ${scope}${isDeepStrictEqual(applied, controller.settings()) ? "" : " (this project overrides shared settings)"}`, async () => {
				const selected = await ctx.ui.select("Save settings to", ["All projects", "This project", BACK]);
				if ((selected === "All projects" || selected === "This project") &&
					(!dirty || await ctx.ui.confirm("Discard changes?", "Switch configuration scope without applying?"))) {
					controller.setSettingsScope(selected === "All projects" ? "global" : "project");
					reload();
				}
			}],
			[`Prediction sources › ${sourceSummary(draft)}`, () => openPredictionSources(ctx, editor)],
			[`Tools & execution › ${toolPolicy.enabled}/${toolPolicy.available} enabled for prediction`, () => openToolsAndExecution(ctx, editor, controller)],
			["Advanced settings › tuning, decoding, scheduling, storage", () => openAdvancedSettings(ctx, editor)],
			[`Apply changes${dirty ? " (pending)" : ""}`, async () => {
				if (!dirty) return ctx.ui.notify("No pending speculative-action changes.", "info");
				const saved = await controller.setSettings(draft).then(() => true, (error: unknown) => void ctx.ui.notify(`Speculative-action settings apply to this session but were not saved: ${errorMessage(error)}`, "error"));
				reload();
				if (saved) ctx.ui.notify("Speculative-action settings applied.", "info");
			}],
		]);
		if (dirty) actions.set("Discard changes", () => { draft = structuredClone(applied); });
		actions.set("Status", async () => {
			await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
			ctx.ui.notify(controller.statusText(), "info");
		});
		actions.set("Recent events", () => showRecentEvents(ctx, controller));
		actions.set("Restore defaults", async () => {
			if (!await ctx.ui.confirm("Restore defaults?", "Restore tuning values while keeping the main switch and prediction-source choices?")) return;
			const defaults = normalizeSpeculativeActionSettings(undefined), { patternAware: pattern, selfSpeculation: probe } = draft;
			await editor.setSettings({ ...defaults, enabled: draft.enabled, drafterEnabled: draft.drafterEnabled,
				patternAware: { ...defaults.patternAware, enabled: pattern.enabled, multiStepEnabled: pattern.multiStepEnabled },
				selfSpeculation: { ...defaults.selfSpeculation, enabled: probe.enabled, forkEnabled: probe.forkEnabled, forkActionEnabled: probe.forkActionEnabled } });
		});
		const choice = await ctx.ui.select("Speculative action", [...actions.keys(), CLOSE]);
		if (!choice || choice === CLOSE) {
			if (
				!dirty ||
				(await ctx.ui.confirm("Discard changes?", "Close without applying the pending speculative-action changes?"))
			)
				return;
			continue;
		}
		await actions.get(choice)?.();
	}
}

function openPredictionSources(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Prediction sources", () => {
		const settings = controller.settings();
		return new Map<string, MenuAction>([
			[`Model Drafter › ${settings.drafterEnabled ? "On" : "Off"}, ${settings.draftModel ?? activeModelReference(ctx)}`, () => openDrafterSettings(ctx, controller)],
			[`Actor probe › ${actorForkSummary(settings.selfSpeculation)}`, () => openActorForkSettings(ctx, controller)],
			[`Learned patterns › ${settings.patternAware.enabled ? "On" : "Off"}, ${settings.patternAware.multiStepEnabled ? "follow-up steps" : "next step only"}`, () => openPatternAwareSettings(ctx, controller)],
		]);
	});
}

function openAdvancedSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Advanced settings", () => {
		const settings = controller.settings();
		return new Map<string, MenuAction>([
			[`Model Drafter tuning › ${settings.candidateLimit} requests, ${settings.drafterMaxDepth} follow-up steps`, () => openDrafterSettings(ctx, controller, true)],
			[`Actor probe and target verification › ${settings.selfSpeculation.forkTransport}`, () => openActorForkSettings(ctx, controller, "advanced")],
			[`Learned-pattern tuning › ${settings.patternAware.maxPatterns} stored patterns`, () => openPatternAwareSettings(ctx, controller, "advanced")],
			[`Scheduling and storage › ${settings.maxConcurrentActions} simultaneous tools`, () => openSchedulingAndCache(ctx, controller)],
		]);
	});
}

function openDrafterSettings(ctx: ExtensionContext, controller: SpeculativeActionController, advanced = false): Promise<void> {
	return runActionMenuLoop(ctx, advanced ? "Model Drafter advanced" : "Model Drafter", () => {
		const settings = controller.settings();
		const { input, toggle } = settingActions(ctx, settings, ROOT_SETTING_INPUTS, controller.setSettings);
		return new Map<string, MenuAction>(!advanced ? [
			toggle("drafterEnabled", "Enabled"),
			[`Model › ${settings.draftModel ?? activeModelReference(ctx)}`, () => editDraftModel(ctx, controller, settings)],
			input("candidateLimit", "Candidate requests per decision"),
			[`Advanced settings › sampling, follow-up steps, cost control`, () => openDrafterSettings(ctx, controller, true)],
		] : [
			toggle("drafterGateEnabled", "Pause drafts on estimated negative utility"),
			input("drafterMaxDepth", "Follow-up tool steps"),
			input("drafterMaxTokens", "Maximum output tokens"),
			input("drafterDeterministicCandidates", "Temperature-0 candidates"),
			[`Sampling temperature: ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`, () => editDrafterTemperatureRange(ctx, controller, settings)],
		]);
	});
}

type ActorForkMenu = "basic" | "advanced" | "integration" | "fork" | "target" | "benefit";

function openActorForkSettings(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	menu: ActorForkMenu = "basic",
): Promise<void> {
	const titles: Readonly<Record<ActorForkMenu, string>> = {
		basic: "Actor probe",
		advanced: "Actor probe advanced",
		integration: "Integration and authentication",
		fork: "Fork decoding",
		target: "Target verification",
		benefit: "Benefit control",
	};
	return runActionMenuLoop(ctx, titles[menu], () => {
		const settings = controller.settings();
		const self = settings.selfSpeculation;
		const save = (selfSpeculation: SelfSpeculationSettings) => controller.setSettings({ ...settings, selfSpeculation });
		const { input, toggle } = settingActions(ctx, self, SELF_SPECULATION_INPUTS, save);
		const actions = new Map<string, MenuAction>();
		if (menu === "basic") {
			const active = self.enabled && self.forkEnabled;
			actions.set(`Actor probe prediction: ${active ? "On" : "Off"}`, () => save({ ...self, enabled: active ? self.enabled : true, forkEnabled: !active }));
			actions.set("Advanced settings › integration, decoding, verification, benefit control", () => openActorForkSettings(ctx, controller, "advanced"));
			if (self.forkTransport === "sidecar") {
				actions.set(...toggle("forkActionEnabled", "Use forked calls for tool pre-execution"));
				if (self.forkActionEnabled) actions.set(...input("forkActionMinConfidence", undefined, formatPercent));
			}
		} else if (menu === "advanced") {
			actions.set(`Integration and authentication › ${self.forkTransport === "provider" ? "Provider-integrated" : "Sidecar service"}`, () => openActorForkSettings(ctx, controller, "integration"));
			actions.set(`Fork decoding › ${self.forkDecoder}, ${self.forkMaxTokens} tokens`, () => openActorForkSettings(ctx, controller, "fork"));
			actions.set(`Target verification › ${self.maxCandidates} candidates × ${self.maxDraftTokens} tokens`, () => openActorForkSettings(ctx, controller, "target"));
			actions.set(`Benefit control › ${self.forkGateEnabled ? "Adaptive pause on" : "Always fork"}`, () => openActorForkSettings(ctx, controller, "benefit"));
		} else if (menu === "integration") {
			actions.set(`Integration: ${self.forkTransport === "provider" ? "Provider-integrated" : "Sidecar service"}`, async () => {
				const selected = await ctx.ui.select("Actor probe integration", ["Provider-integrated", "Sidecar service", BACK]);
				if (selected === "Provider-integrated" || selected === "Sidecar service")
					await save({ ...self, forkTransport: selected === "Provider-integrated" ? "provider" : "sidecar" });
			});
			if (self.forkTransport === "sidecar") {
				actions.set(...input("endpoint"));
				actions.set(...input("timeoutMs", "Request timeout", formatDuration));
				actions.set(...input("apiKeyEnv", "Authentication token variable", value => value ?? "None"));
			}
		} else if (menu === "fork") {
			actions.set(...input("forkMaxTokens", "Maximum output tokens"));
			actions.set(...input("forkTemperature", "Sampling temperature", formatNumber));
			actions.set(...input("forkDecoder", "Tool-call decoder"));
			actions.set(...input("forkForcedPrefix", "Forced tool-call prefix", syntaxSettingLabel));
		} else if (menu === "target") {
			actions.set(...toggle("enabled", "Verify predicted calls during Actor decoding"));
			actions.set(...input("maxCandidates", "Candidates sent per decision"));
			actions.set(...input("maxDraftTokens"));
			actions.set(...input("actorProfile", "Actor Profile"));
			actions.set(...input("draftFormat", "Tool-call format override"));
			actions.set(...input("draftBoundary", "Tool-call boundary", syntaxSettingLabel));
		} else {
			actions.set(...toggle("forkGateEnabled", "Pause forks that stop saving time"));
			if (self.forkGateEnabled) {
				actions.set(...input("forkGateMinSamples", "Warm-up samples"));
				actions.set(...input("forkGateWindowSize", "Rolling samples"));
				actions.set(...input("forkGateMinNetBenefitMs", "Minimum expected time saved", formatDuration));
				actions.set(...input("forkGateProbeInterval"));
				actions.set(...input("forkGateFailureThreshold"));
			}
		}
		return actions;
	});
}

function openPatternAwareSettings(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	menu: "basic" | "advanced" | "learning" | "multiStep" = "basic",
): Promise<void> {
	const title = { basic: "Learned patterns", advanced: "Learned-pattern advanced", learning: "Learning history", multiStep: "Multi-step search" }[menu];
	return runActionMenuLoop(ctx, title, () => {
		const settings = controller.settings();
		const pattern = settings.patternAware;
		const { input, toggle } = settingActions(ctx, pattern, PATTERN_SETTING_INPUTS,
			(patternAware) => controller.setSettings({ ...settings, patternAware }));
		if (menu === "basic") return new Map<string, MenuAction>([
			toggle("enabled", "Enabled"),
			toggle("multiStepEnabled", "Predict follow-up tool steps"),
			[`Advanced settings › history, confidence, search limits`, () => openPatternAwareSettings(ctx, controller, "advanced")],
		]);
		if (menu === "advanced") {
			const actions = new Map<string, MenuAction>([
				[`Learning history › ${pattern.maxContextLength} previous actions`, () => openPatternAwareSettings(ctx, controller, "learning")],
			]);
			if (pattern.multiStepEnabled)
				actions.set(`Multi-step search › ${pattern.beamWidth} alternatives/tool, ${pattern.maxPredictionDepth} steps`, () => openPatternAwareSettings(ctx, controller, "multiStep"));
			return actions;
		}
		return new Map<string, MenuAction>(menu === "learning" ? [
			input("maxContextLength"),
			input("maxFutureGap"),
			input("futureGapCoverage", "Early-prediction coverage", formatPercent),
			input("decayHalfLifeEvents", "History half-life", value => `${value} events`),
			input("minOccurrences", "Uses before learning a pattern"),
			input("maxPatterns"),
		] : [
			input("beamWidth"),
			input("maxPredictionDepth"),
			input("minBindingReplayProbability", "Minimum argument-replay confidence", formatPercent),
		]);
	});
}

function openSchedulingAndCache(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Scheduling and storage", () => {
		const settings = controller.settings();
		const { input } = settingActions(ctx, settings, ROOT_SETTING_INPUTS, controller.setSettings);
		const actions = new Map<string, MenuAction>([
			input("maxConcurrentActions"),
			input("predictionTimeoutMs", "Prediction wait limit", formatDuration),
			input("resourceCacheMaxEntries"),
			input("resourceCacheMaxBytes", "Live result memory", formatBytes),
			input("executionStoreMaxEntries"),
			input("executionStoreMaxBytes", "Reusable command history memory", formatBytes),
		]);
		for (const [label, operation] of [["Reclaim", "gc"], ["Clear", "clear"]] as const) actions.set(`${label} reusable command history`, async () => {
			if (operation === "clear" && !(await ctx.ui.confirm("Clear reusable command history?", "Delete all reusable command results and file effects? This cannot be undone."))) return;
			const report = await recoverSpeculation(() => controller.maintainExecutionStorage(operation));
			ctx.ui.notify(
				report?.text ?? "Reusable command history maintenance failed.",
				report && !report.failed ? "info" : "warning",
			);
		});
		return actions;
	});
}

type MenuAction = () => void | Promise<void>;

async function runActionMenuLoop(
	ctx: ExtensionContext,
	title: string,
	actionsForCurrentSettings: () => ReadonlyMap<string, MenuAction>,
): Promise<void> {
	while (true) {
		const actions = actionsForCurrentSettings();
		const choice = await ctx.ui.select(title, [...actions.keys(), BACK]);
		if (!choice || choice === BACK) return;
		await actions.get(choice)?.();
	}
}

function openToolsAndExecution(
	ctx: ExtensionContext,
	editor: SpeculativeActionController,
	controller: SpeculativeActionController,
): Promise<void> {
	return runActionMenuLoop(ctx, "Tools & execution", () => {
		const settings = editor.settings();
		const policy = toolPolicyCounts(settings, controller.registeredTools());
		return new Map<string, MenuAction>([
			[`Tool policy › ${policy.enabled}/${policy.available} enabled for prediction`, () => editToolPolicy(ctx, editor, controller.registeredTools(), controller.toolConflicts())],
			["Execution routes", () => openExecutionRoutes(ctx, editor, controller)],
		]);
	});
}

async function openExecutionRoutes(
	ctx: ExtensionContext,
	editor: SpeculativeActionController,
	controller: SpeculativeActionController,
): Promise<void> {
	await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
	ctx.ui.notify(
		`Predict controls what sources may propose. Replay, Observe, and Fork are independent runtime capabilities. Order: unified environment → local safe fallback → Actor. Local routes include sealed file inputs, private workspace transactions, and qualified native processes. Diagnostics refresh enabled providers only.\n${executionWorldSummary(controller.toolCapabilities(), controller.executionRoutes())}`,
		"info",
	);
	return runActionMenuLoop(ctx, "Execution routes", () => {
		const settings = editor.settings();
		const { worlds, primaryIDs } = controller.executionRoutes();
		const actions = new Map<string, MenuAction>();
		const routes = [
			["primary", "Unified execution environment", primaryIDs.size, primaryIDs.size > 0],
			["nativeFallback", "Local safe fallback", worlds.filter((world) => !primaryIDs.has(world.id)).length, true],
		] as const;
		for (const [field, label, providers, available] of routes) {
			const status = available ? `${providers} provider${providers === 1 ? "" : "s"}` : "not installed";
			actions.set(
				`${available && settings.executionRouting[field] ? "[x]" : "[ ]"} ${label} · ${status}`,
				available
					? () => editor.setSettings({
							...settings,
							executionRouting: { ...settings.executionRouting, [field]: !settings.executionRouting[field] },
						})
					: () => ctx.ui.notify("No unified execution environment is installed for this profile.", "info"),
			);
		}
		actions.set(`Search execution › ${searchExecutionLabel(settings.searchExecution)}`, async () => {
			const choice = await ctx.ui.select("Search execution", ["Native Pi (default)", "Captured search (find + available rg; no installation)", BACK]);
			if (!choice || choice === BACK) return;
			if (choice.startsWith("Captured")) ctx.ui.notify("Actor and speculation share Pi's installed find matcher and, when qualified, pinned rg (Windows x64 15.2.0; Linux x64 14.1.0). grep sorts paths and ignores RIPGREP_CONFIG_PATH/global Git ignores; parent ignore rules remain. Not Native Pi defaults. No installation. An admitted profile call never switches executor on failure.", "warning");
			await editor.setSettings({ ...settings, searchExecution: choice.startsWith("Captured") ? "captured" : "native" });
		});
		actions.set("Actor execution · always available", () =>
			ctx.ui.notify("Actor execution is the authoritative final route and cannot be disabled here.", "info"));
		actions.set("Refresh and show capabilities", async () => {
			await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
			ctx.ui.notify(executionWorldSummary(controller.toolCapabilities(), controller.executionRoutes()), "info");
		});
		return actions;
	});
}

function editToolPolicy(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	registered: ReadonlySet<string>,
	conflicts: ReadonlyMap<string, string>,
): Promise<void> {
	return runActionMenuLoop(ctx, "Tool policy · [x] prediction on · [ ] prediction off", () => {
		const settings = controller.settings();
		const capabilities = controller.toolCapabilities();
		const tools = [...new Set([...KEYABLE_TOOLS, ...settings.tools, ...registered])].sort(
			(left, right) => toolCategory(left) - toolCategory(right) || left.localeCompare(right),
		);
		return new Map<string, MenuAction>(tools.map((tool) => {
			const supported = (KEYABLE_TOOLS as readonly string[]).includes(tool);
			const selected = supported && settings.tools.includes(tool);
			const capability = capabilities.get(tool);
			const staged = capability ? { ...capability, predict: selected ? "on" as const : "off" as const } : undefined;
			return [`${selected ? "[x]" : "[ ]"} ${tool} · ${capabilityRowLabel(staged)}`, async () => {
				if (!supported) {
					ctx.ui.notify(`${tool} has no speculative action semantics.`, "warning");
					return;
				}
				if (!selected && !registered.has(tool)) {
					const conflict = conflicts.get(tool);
					ctx.ui.notify(
						conflict
							? `${tool} is provided by ${conflict}. Custom tool overrides remain authoritative and are excluded from speculation.`
							: `${tool} is not registered in the current Pi session.`,
						"warning",
					);
					return;
				}
				const next = selected ? settings.tools.filter((item) => item !== tool) : [...settings.tools, tool];
				await controller.setSettings({ ...settings, tools: next });
			}];
		}));
	});
}

async function promptSetting<T>(
	ctx: ExtensionContext,
	current: T,
	descriptor: SettingInputDescriptor<T>,
	publish: (value: T) => Promise<void>,
): Promise<void> {
	const input = await ctx.ui.input(descriptor.title, descriptor.format(current));
	if (input === undefined) return;
	const parsed = descriptor.parse(input);
	if (!parsed.ok) ctx.ui.notify(parsed.error, "warning");
	else await publish(parsed.value);
}

function settingActions<T extends object, Field extends keyof T>(
	ctx: ExtensionContext,
	current: T,
	descriptors: SettingInputDescriptors<T, Field>,
	publish: (value: T) => Promise<void>,
) {
	const update = (field: keyof T, value: unknown) => {
		const next = { ...current };
		if (value === undefined) Reflect.deleteProperty(next, field);
		else Object.assign(next, { [field]: value });
		return publish(next);
	};
	return {
		input<Key extends Field>(key: Key, label = descriptors[key].title, format: (value: T[Key]) => string | number = String): [string, MenuAction] {
			return [`${label}: ${format(current[key])}`, () => promptSetting(ctx, current[key], descriptors[key], value => update(key, value))];
		},
		toggle(key: { [Key in keyof T]: T[Key] extends boolean ? Key : never }[keyof T], label: string): [string, MenuAction] {
			return [`${label}: ${current[key] ? "On" : "Off"}`, () => update(key, !current[key])];
		},
	};
}

async function editDrafterTemperatureRange(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	await promptSetting(
		ctx,
		[settings.drafterTemperatureMin, settings.drafterTemperatureMax] as const,
		DRAFTER_TEMPERATURE_INPUT,
		async ([drafterTemperatureMin, drafterTemperatureMax]) => {
			await controller.setSettings({ ...settings, drafterTemperatureMin, drafterTemperatureMax });
		},
	);
}

async function editDraftModel(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const models = ctx.modelRegistry
		.getAvailable()
		.sort((left, right) => `${left.provider}/${left.id}`.localeCompare(`${right.provider}/${right.id}`));
	const providers = new Map<string, typeof models>();
	for (const model of models) providers.set(model.provider, [...(providers.get(model.provider) ?? []), model]);
	const providerLabels = new Map(
		[...providers].map(([provider, providerModels]) => [`${provider} (${providerModels.length} models) ›`, provider]),
	);
	const active = `${USE_ACTIVE_MODEL} (${activeModelReference(ctx)})`;
	const choice = await ctx.ui.select("Drafter model", [active, ...providerLabels.keys(), CUSTOM_MODEL, BACK]);
	if (!choice || choice === BACK) return;
	const { draftModel: _previousDraftModel, ...baseSettings } = settings;
	if (choice === active) {
		await controller.setSettings(baseSettings);
		return;
	}
	if (choice === CUSTOM_MODEL) {
		const value = await ctx.ui.input("Custom drafter model", "provider/model");
		if (value === undefined) return;
		const draftModel = value.trim();
		await controller.setSettings({ ...baseSettings, ...(draftModel ? { draftModel } : {}) });
		return;
	}
	const provider = providerLabels.get(choice);
	if (!provider) return;
	const labels = new Map(
		(providers.get(provider) ?? []).map((model) => {
			const reference = `${model.provider}/${model.id}`;
			return [
				`${settings.draftModel === reference ? "[x] " : ""}${model.id}${model.name && model.name !== model.id ? ` — ${model.name}` : ""}`,
				reference,
			];
		}),
	);
	const selected = await ctx.ui.select(`${provider} models`, [...labels.keys(), BACK]);
	if (!selected || selected === BACK) return;
	const draftModel = labels.get(selected);
	if (draftModel) await controller.setSettings({ ...baseSettings, draftModel });
}

function showRecentEvents(ctx: ExtensionContext, controller: SpeculativeActionController): void {
	const events = controller.recentEvents();
	ctx.ui.notify(events.length > 0 ? events.join("\n") : "No speculative action events recorded yet.", "info");
}

export function formatSpeculativeActionEvent(event: SpeculativeActionEvent<string>): string {
	const parts = [`[${event.type}]`, `session ${compactEventText(String(event.sessionID))}`, `turn ${compactEventText(event.turnID)}`];
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
		case "operation_prediction":
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
			const route = event.candidate.route;
			parts.push(
				`candidate ${compactEventText(event.candidate.id)}`,
				event.candidate.tool,
				event.candidate.source,
				route
					? `${route.backend}/${executionRouteKind(route.isolation)}/${route.reuse}`
					: `${event.candidate.world?.backend ?? "unknown backend"}/${executionRouteKind(event.candidate.execution)}`,
				event.state.status,
			);
			if (event.state.status === "running") {
				parts.push(
					`${event.candidate.origin === "actor_preview" ? "previewed" : "predicted"} ${compactEventText(event.candidate.predictedAction)}`,
				);
			} else if (event.state.status === "succeeded") {
				parts.push(formatDuration(event.state.executionMs));
				const reuse = event.candidate.world?.executionMetrics.reuse;
				if (reuse && hasProcessReuse(reuse)) {
					parts.push(`Bash branch work ${formatProcessWorkReuse(reuse)}`);
				}
			} else {
				parts.push(causeSummary(event.state.cause), formatDuration(event.state.executionMs));
			}
			break;
		}
		case "actor_action": {
			const sources = [...new Set(event.settlement.matchedPredictions.map((prediction) => prediction.source))];
			parts.push(
				event.settlement.tool,
				sources.join("+") || (event.settlement.provider.kind === "speculative" ? "cache" : "no prediction"),
			);
			if (event.settlement.provider.kind === "speculative") {
				const match = event.settlement.provider.match;
				parts.push(
					match.kind === "projected" ? `partial-result reuse (${match.projector})` : match.kind === "inputs" ? "sealed-input reuse" : "exact-action reuse",
					`${formatDuration(event.settlement.provider.timing.executionAheadMs)} ahead`,
					`${formatDuration(event.settlement.provider.timing.hitLatencyMs)} hit latency`,
					`${formatDuration(event.settlement.provider.timing.attemptLeadMs)} attempt lead`,
				);
			} else {
				parts.push(
					`${formatDuration(event.settlement.provider.durationMs)} Actor ${event.settlement.provider.origin} execution`,
				);
			}
			parts.push(compactEventText(event.actualAction));
			break;
		}
	}
	return parts.join(" · ");
}
function causeSummary(value: { readonly stage: string; readonly code: string; readonly detail?: string }): string {
	return `${value.stage}:${value.code}${value.detail ? ` (${compactEventText(value.detail)})` : ""}`;
}

function compactEventText(value: string): string {
	const compact = value.replace(/\s+/g, " ").trim();
	return compact.length <= 120 ? compact : `${compact.slice(0, 117)}...`;
}

function findExactModelReferenceMatch(reference: string, models: readonly Model<Api>[]): Model<Api> | undefined {
	const normalized = reference.trim().toLowerCase();
	if (!normalized) return undefined;
	const canonical = models.filter((model) => `${model.provider}/${model.id}`.toLowerCase() === normalized);
	if (canonical.length === 1) return canonical[0];
	if (canonical.length > 1 || normalized.includes("/")) return undefined;
	const byID = models.filter((model) => model.id.toLowerCase() === normalized);
	return byID.length === 1 ? byID[0] : undefined;
}

function mebibyteInput(title: string): SettingInputDescriptor<number> {
	return positiveIntegerInput(`${title} (MiB)`, {
		error: `${title} must be a positive integer in MiB.`,
		format: (bytes) => String(Math.max(1, Math.round(bytes / (1024 * 1024)))),
		transform: (mebibytes) => mebibytes * 1024 * 1024,
	});
}

function sourceSummary(settings: EffectiveSpeculativeActionSettings): string {
	if (!settings.enabled) return "Inactive";
	const sources = [
		settings.drafterEnabled ? "Model Drafter" : undefined,
		settings.selfSpeculation.enabled && settings.selfSpeculation.forkEnabled ? "Actor probe" : undefined,
		settings.patternAware.enabled ? "Learned patterns" : undefined,
	]
		.filter((source): source is string => source !== undefined)
		.join(" + ");
	return sources || "No source enabled";
}

function actorForkSummary(settings: SelfSpeculationSettings): string {
	if (!settings.enabled || !settings.forkEnabled) return "Off";
	return `On, ${settings.forkTransport === "provider" ? "provider-integrated" : "sidecar service"}`;
}

function syntaxSettingLabel(value: string): string {
	return value === "auto" ? "automatic" : value;
}

function formatDrafterGateStatus(enabled: boolean, gate: DrafterUtilityGateSnapshot): string {
	return `Action Drafter gate: ${enabled ? "On" : "Off"}; ${gate.skippedBatches} batches skipped, ${gate.samples} samples${gate.expectedNetBenefitMs === undefined ? ", benefit unmeasured" : `, ${formatDuration(gate.expectedNetBenefitMs)} budget estimate`}`;
}

function formatSelfSpeculationStatus(bridge: SelfSpeculationCoordinatorSnapshot): string {
	return [
		`Self-speculation: ${bridge.bufferedCandidates} buffered`,
		...(bridge.resolvedActorProfile
			? [
					`Resolved Actor Profile: ${bridge.resolvedActorProfile}${bridge.profileResolutionSource ? ` (${bridge.profileResolutionSource})` : ""}`,
				]
			: []),
		`${bridge.candidateSubmissions} bundles/${bridge.candidateReceipts} receipts`,
		`${bridge.forkRequests}/${bridge.forkCompletions} probes completed (${bridge.forkRetries} later-snapshot retries), ${bridge.forkGateSkips} gated${bridge.forkGateExpectedNetBenefitMs === undefined ? ", benefit unmeasured" : ` at ${formatDuration(bridge.forkGateExpectedNetBenefitMs)} budget estimate`}`,
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

function resolveToolCapabilities(
	settings: EffectiveSpeculativeActionSettings,
	registered: Iterable<string>,
	conflicts: ReadonlyMap<string, string>,
	routes: ExecutionRoutesSnapshot,
	bindings?: ReadonlyMap<string, ToolInvocation>,
): ReadonlyMap<string, ToolCapabilityRow> {
	const registeredTools = new Set(registered);
	const { worlds, actorProcessReplay } = routes;
	return new Map<string, ToolCapabilityRow>(
		KEYABLE_TOOLS.map((tool): [string, ToolCapabilityRow] => {
			const requirements = bindings?.get(tool)?.semantics?.requirements ?? PI_ACTION_SEMANTICS.requirements(tool);
			const unavailable = conflicts.get(tool) ?? (!registeredTools.has(tool) ? "not registered" : undefined);
			if (unavailable || !requirements) {
				return [tool, { predict: "unavailable", replay: "unavailable", observe: "unavailable", fork: "unavailable" }];
			}
			const fork = executionCapabilityStatus(requirements, worlds, "speculation", tool);
			const observe = executionCapabilityStatus(requirements, worlds, "observation", tool);
			const replay = bestCapabilityState([
				fork.state,
				observe.state,
				...(tool === "bash" && actorProcessReplay ? [processRouteCapability(actorProcessReplay.state)] : []),
			]);
			return [tool, {
				predict: settings.tools.includes(tool) ? "on" : "off",
				replay,
				observe: observe.state,
				fork: fork.state,
			}];
		}),
	);
}

function predictionTools(
	settings: EffectiveSpeculativeActionSettings,
	registered: Iterable<string>,
): readonly string[] {
	const available = new Set(registered);
	return [...new Set(settings.tools)].filter((tool) => available.has(tool));
}

function toolPolicyCounts(
	settings: EffectiveSpeculativeActionSettings,
	registered: ReadonlySet<string>,
): { readonly enabled: number; readonly available: number } {
	return {
		enabled: predictionTools(settings, registered).length,
		available: registered.size,
	};
}

function bestCapabilityState(states: readonly ExecutionWorldHealthState[]): ExecutionWorldHealthState {
	return states.includes("ready") ? "ready" : states.includes("registered") ? "registered" : "unavailable";
}

function processRouteCapability(state: ProcessRouteSnapshot["state"]): ExecutionWorldHealthState {
	return state === "ready" || state === "degraded"
		? "ready"
		: state === "idle" || state === "probing" ? "registered" : "unavailable";
}

function searchExecutionLabel(mode: string): string {
	return mode === "captured" ? "Captured search" : "Native Pi";
}

function processRouteLabel(state: ProcessRouteSnapshot["state"]): string {
	return { disabled: "Disabled", idle: "Idle", probing: "Checking", ready: "Ready", degraded: "Limited", unavailable: "Unavailable" }[state];
}

function executionRouteKind(isolation: SpeculativeExecution): string {
	switch (isolation) {
		case "runtime_sandbox": return "isolated runtime";
		case "resource_snapshot": return "sealed file inputs";
		case "workspace_branch": return "private workspace";
	}
}

function capabilityRowLabel(row: ToolCapabilityRow | undefined): string {
	if (!row) return "unsupported";
	return (["predict", "replay", "observe", "fork"] as const)
		.map((name) => `${name[0]!.toUpperCase()}${name.slice(1)} ${capabilityLabel(row[name])}`)
		.join(" · ");
}

function capabilityLabel(state: CapabilityState): string {
	switch (state) {
		case "on": return "On";
		case "off": return "Off";
		case "ready": return "Ready";
		case "registered": return "Check";
		case "unavailable": return "Unavailable";
	}
}

function toolCategory(tool: string): number {
	if ((OBSERVATION_ACTION_TOOLS as readonly string[]).includes(tool)) return 0;
	if ((WORKSPACE_MUTATION_ACTION_TOOLS as readonly string[]).includes(tool)) return 1;
	if ((UNBOUNDED_ACTION_TOOLS as readonly string[]).includes(tool)) return 2;
	return 3;
}

function toolsSummary(tools: readonly string[]): string {
	return tools.length > 0 ? tools.join(" ") : "none";
}

function formatSpeculativeFooter(
	settings: EffectiveSpeculativeActionSettings,
	metrics: SpeculativeActionMetrics,
	routes: ExecutionRoutesSnapshot,
	conflicts: number,
): string {
	if (!settings.enabled) return "spec: off";
	const { worlds, actorProcessReplay } = routes;
	const providers = worlds.length + (actorProcessReplay ? 1 : 0);
	const ready = worlds.filter((world) => world.state === "ready" || world.observation?.state === "ready").length +
		(actorProcessReplay && processRouteCapability(actorProcessReplay.state) === "ready" ? 1 : 0);
	const reuse = metrics.actorProcessReuse;
	const storageWorlds = worlds.filter((world) => world.storage);
	const storedEntries = storageWorlds.reduce((total, world) => total + (world.storage?.entries ?? 0), 0);
	const storedBytes = storageWorlds.reduce((total, world) => total + (world.storage?.bytes ?? 0), 0);
	return [
		"spec: on",
		metrics.tasks > 0 ? `${formatSpeedups(metrics)}; ${formatDuration(metrics.endToEndMs)} wall` : "End-to-End SpeedUp n/a; Tool time speed up n/a",
		`tools reused ${formatRatio(metrics.speculativeHits, metrics.actorActions)}`,
		...(hasProcessReuse(reuse) ? [`Bash Actor ${formatActorProcessFooter(reuse)}`] : []),
		`live results ${metrics.cache.resultEntries}/${metrics.cache.cacheCapacity} (${formatBytes(metrics.cache.resultBytes)})`,
		...(storageWorlds.length ? [`reuse history ${storedEntries} entries (${formatBytes(storedBytes)})`] : []),
		providers > 0 ? `providers ${ready}/${providers} ready` : "providers probing",
		"unsafe→Actor",
		...(conflicts > 0 ? [`${conflicts} tool conflict${conflicts === 1 ? "" : "s"}`] : []),
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
			? [`- Actor Bash history: ${processRouteLabel(actorProcessReplay.state)} — ${actorProcessReplay.detail}`]
			: []),
		...worlds.map(
			(world) =>
				`- ${executionRouteKind(world.isolation)} (${world.id}): Fork ${capabilityLabel(world.state)} — ${world.detail}; Observe ${
					world.observation ? `${capabilityLabel(world.observation.state)} — ${world.observation.detail}` : "Unavailable"
				}${
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
		capabilityLabel(capability.predict),
		capabilityLabel(capability.replay),
		capabilityLabel(capability.observe),
		capabilityLabel(capability.fork),
	]);
	const table = [["Tool", "Predict", "Replay", "Observe", "Fork"], ...rows];
	const widths = table[0]!.map((_, column) => Math.max(...table.map((row) => row[column]!.length)));
	return table.map((row) => row.map((cell, column) => cell.padEnd(widths[column]!)).join("  ").trimEnd()).join("\n");
}

function countSummary(counts: Readonly<Record<string, number>>): string {
	const entries = Object.entries(counts).sort(([leftKey, left], [rightKey, right]) =>
		right === left ? leftKey.localeCompare(rightKey) : right - left,
	);
	return entries.length > 0 ? entries.map(([key, count]) => `${key}=${count}`).join(", ") : "none";
}

function formatTaskTiming(timing: Pick<SpeculativeTraceSummary, "endToEndMs" | "estimatedSavingsMs" | "hiddenLatencyMs" | "toolExecutionMs">): string {
	return `${formatDuration(timing.endToEndMs)} wall; ${formatDuration(timing.estimatedSavingsMs)} estimated savings; ${formatSpeedups(timing)}; ${formatDuration(timing.hiddenLatencyMs)} of ${formatDuration(timing.toolExecutionMs)} tool time hidden`;
}

function formatSpeedups(timing: Pick<SpeculativeTraceSummary, "endToEndMs" | "estimatedSavingsMs" | "hiddenLatencyMs" | "toolExecutionMs">): string {
	const savings = timing.estimatedSavingsMs;
	const percent = timing.endToEndMs > 0 && Number.isFinite(savings) ? `+${(100 * savings / timing.endToEndMs).toFixed(1)}%` : "n/a";
	const toolPercent = timing.toolExecutionMs > 0 && Number.isFinite(timing.hiddenLatencyMs) ? `${(100 * timing.hiddenLatencyMs / timing.toolExecutionMs).toFixed(1)}%` : "n/a";
	return `End-to-End SpeedUp ${percent}; Tool time speed up ${toolPercent}`;
}

function formatDuration(ms: number): string {
	if (!Number.isFinite(ms)) return "n/a";
	const value = Math.max(0, ms);
	if (value > 0 && value < 1) return "<1ms";
	if (value < 1000) return `${Math.round(value)}ms`;
	if (value < 60_000) return `${(value / 1000).toFixed(value < 10_000 ? 1 : 0).replace(/\.0$/, "")}s`;
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
	return reuse.requests + reuse.wholeCommandRequests > 0;
}

function formatProcessWorkReuse(reuse: WorldReuseMetrics): string {
	const hits = reuse.hits + reuse.wholeCommandHits;
	const workMs = reuse.reusedProcessMs + reuse.wholeCommandReusedProcessMs;
	return [
		reuse.wholeCommandRequests ? `whole ${formatRatio(reuse.wholeCommandHits, reuse.wholeCommandRequests)}` : "",
		reuse.requests ? `child ${formatRatio(reuse.hits, reuse.requests)}` : "",
		hits > 0 ? (workMs > 0 ? `${formatDuration(workMs)} recorded process work reused` : "work timing unavailable") : "",
	].filter(Boolean).join("; ");
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
	return [
		`${formatRatio(hits, requests)} reused`,
		workMs > 0 ? `${formatDuration(workMs)} work` : "",
	].filter(Boolean).join(" · ");
}
