import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
import path from "node:path";
import type { AgentMessage, AgentTool, AgentToolResult, AgentToolUpdateCallback } from "@earendil-works/pi-agent-core";
import type { Api, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import {
	convertToLlm,
	createBashToolDefinition,
	createLocalBashOperations,
	createEditToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
	createWriteToolDefinition,
	type ExtensionAPI,
	type ExtensionCommandContext,
	type ExtensionContext,
	type ExtensionFactory,
	type ExtensionUIContext,
	getAgentDir,
	type ModelRegistry,
	type SourceInfo,
	type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import {
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
	PI_ACTION_SEMANTICS,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "./action-semantics.ts";
import { ActorStreamPreviewTracker } from "./actor-stream-preview.ts";
import type { SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import { createSpeculativeActionHost } from "./agent-integration.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection,
} from "./common.ts";
import type { DrafterUtilityGateSnapshot } from "./drafter-utility-gate.ts";
import { PATTERN_AWARE_DEFAULTS, type PatternAwareSettings, patternAwareSettings } from "./pattern-aware.ts";
import { PI_BASH_TAIL_LINES_PROJECTION_RULE } from "./pi-bash-projection.ts";
import {
	canPreviewIncompletePiCall,
	PI_READ_RANGE_PROJECTION_RULE,
	withPiProjectionCoverage,
} from "./pi-read-projection.ts";
import { resolvePiToolInvocation } from "./pi-tool-invocation.ts";
import { createLinuxProcessExecutionWorld } from "./linux-process-world.ts";
import {
	executionCapabilityStatus,
	type ExecutionCapabilityStatus,
	type ExecutionWorldDiagnosticSnapshot,
	type SpeculativeExecution,
	type WorldReuseMetrics,
} from "./execution-world.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator, type ProcessToolOperations } from "./process-execution.ts";
import { DEFAULT_PROVENANCE_STORE_LIMITS } from "./reuse-store.ts";
import type { SpeculativeActionEvent } from "./runtime.ts";
import {
	nonEmptyTextInput,
	nonNegativeIntegerInput,
	nonNegativeNumberInput,
	optionalPositiveIntegerInput,
	optionalTextInput,
	positiveIntegerInput,
	probabilityInput,
	settingInput,
	type SettingInputDescriptor,
} from "./setting-input.ts";
import {
	normalizeSelfSpeculationSettings,
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

type BaseToolDefinition =
	| ReturnType<typeof createReadToolDefinition>
	| ReturnType<typeof createBashToolDefinition>
	| ReturnType<typeof createEditToolDefinition>
	| ReturnType<typeof createWriteToolDefinition>
	| ReturnType<typeof createGrepToolDefinition>
	| ReturnType<typeof createFindToolDefinition>
	| ReturnType<typeof createLsToolDefinition>;

export interface EffectiveSpeculativeActionSettings {
	readonly enabled: boolean;
	readonly drafterEnabled: boolean;
	readonly drafterGateEnabled: boolean;
	readonly drafterMaxDepth: number;
	readonly drafterMaxTokens?: number;
	readonly drafterDeterministicCandidates: number;
	readonly drafterTemperatureMin: number;
	readonly drafterTemperatureMax: number;
	readonly draftModel?: string;
	readonly candidateLimit: number;
	readonly maxConcurrentActions: number;
	readonly resourceCacheMaxEntries: number;
	readonly resourceCacheMaxBytes: number;
	readonly executionStoreMaxEntries: number;
	readonly executionStoreMaxBytes: number;
	readonly predictionTimeoutMs: number;
	readonly patternAware: PatternAwareSettings;
	readonly selfSpeculation: SelfSpeculationSettings;
	readonly tools: readonly string[];
}

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
	drafterMaxTokens: optionalPositiveIntegerInput("Maximum Drafter output tokens (blank for provider default)"),
	drafterMaxDepth: nonNegativeIntegerInput("Drafter follow-up tool steps"),
	drafterDeterministicCandidates: nonNegativeIntegerInput("Temperature-0 Drafter candidates"),
} satisfies Partial<SettingInputDescriptors<EffectiveSpeculativeActionSettings, keyof EffectiveSpeculativeActionSettings>>;
type RootInputField = keyof typeof ROOT_SETTING_INPUTS;

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
	draftFormat: nonEmptyTextInput("Target tool-call format"),
	draftBoundary: nonEmptyTextInput("Target tool-call boundary"),
	forkMaxTokens: positiveIntegerInput("Actor probe output-token limit"),
	timeoutMs: positiveIntegerInput("Control request timeout (ms)"),
	forkTemperature: nonNegativeNumberInput("Actor probe temperature"),
	forkDecoder: nonEmptyTextInput("Forked tool-call decoder"),
	forkForcedPrefix: nonEmptyTextInput("Forced tool-call prefix"),
	apiKeyEnv: optionalTextInput("Authentication token environment variable name (not the token)"),
} satisfies Partial<SettingInputDescriptors<SelfSpeculationSettings, keyof SelfSpeculationSettings>>;
type SelfSpeculationInputField = keyof typeof SELF_SPECULATION_INPUTS;

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
type PatternInputField = keyof typeof PATTERN_SETTING_INPUTS;

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

export type SpeculativeActionMetrics = SpeculativeTraceSummary;

export interface SpeculativeSettingsStore {
	readonly scope: SpeculativeSettingsScope;
	readonly load: () => Promise<void>;
	readonly effective: () => SpeculativeActionPackageSettings | undefined;
	readonly editable: (scope?: SpeculativeSettingsScope) => SpeculativeActionPackageSettings | undefined;
	readonly setEffective: (settings: SpeculativeActionPackageSettings, inherited?: SpeculativeActionPackageSettings) => void;
	readonly clear: () => void;
	readonly setScope: (scope: SpeculativeSettingsScope) => void;
	readonly flush: () => Promise<void>;
}

interface SpeculativeActionController {
	readonly settings: () => EffectiveSpeculativeActionSettings;
	readonly editableSettings: () => EffectiveSpeculativeActionSettings;
	readonly settingsScope: () => SpeculativeSettingsScope;
	readonly setSettingsScope: (scope: SpeculativeSettingsScope) => void;
	readonly metrics: () => SpeculativeActionMetrics;
	readonly registeredTools: () => ReadonlySet<string>;
	readonly toolCapabilities: () => ReadonlyMap<string, ExecutionCapabilityStatus>;
	readonly toolConflicts: () => ReadonlyMap<string, string>;
	readonly recentEvents: () => readonly string[];
	readonly refreshExecutionDiagnostics: (refresh?: boolean) => Promise<void>;
	readonly executionSummary: () => string;
	readonly maintainExecutionStorage: (operation: "gc" | "clear") => Promise<{ text: string; failed: boolean }>;
	readonly setSettings: (settings: SpeculativeActionPackageSettings | undefined) => void;
	readonly attachUI: (ui: ExtensionUIContext) => void;
	readonly detachUI: () => void;
	readonly startTurn: (messages: AgentMessage[], context: ExtensionContext) => Promise<void>;
	readonly previewActorTool: (tool: string, signal?: AbortSignal) => void;
	readonly previewActorCall: (tool: string, callID: string, input: unknown, signal?: AbortSignal) => void;
	readonly decorateActorPayload: (payload: unknown) => unknown;
	readonly observeActorOutput: (event: Parameters<SelfSpeculationCoordinator["observeActorOutput"]>[0]) => void;
	readonly selfSpeculationSnapshot: () => SelfSpeculationCoordinatorSnapshot;
	readonly finishTurn: (terminal?: boolean) => Promise<void>;
	readonly execute: (
		tool: string,
		callID: string,
		input: unknown,
		signal: AbortSignal | undefined,
		onUpdate: AgentToolUpdateCallback<unknown> | undefined,
		context: ExtensionContext,
	) => Promise<AgentToolResult<unknown>>;
	readonly statusText: () => string;
	readonly dispose: () => Promise<void>;
}

export interface SpeculativeActionExtensionDependencies {
	readonly createExecutionWorlds?: () => readonly SpeculativeAgentExecutionWorld[];
	readonly createHost?: typeof createSpeculativeActionHost;
	readonly createSettingsStore?: (cwd: string) => SpeculativeSettingsStore;
	readonly createWorkspaceSandboxService?: () => WorkspaceSandboxService;
	readonly selfSpeculationFetch?: typeof globalThis.fetch;
}

export function normalizeSpeculativeActionSettings(
	input: SpeculativeActionPackageSettings | undefined,
): EffectiveSpeculativeActionSettings {
	const drafter = normalizeDrafterRequestSettings(input);
	return {
		enabled: typeof input?.enabled === "boolean" ? input.enabled : DEFAULTS.enabled,
		drafterEnabled: typeof input?.drafterEnabled === "boolean" ? input.drafterEnabled : DEFAULTS.drafterEnabled,
		drafterGateEnabled:
			typeof input?.drafterGateEnabled === "boolean" ? input.drafterGateEnabled : DEFAULTS.drafterGateEnabled,
		...drafter,
		...(typeof input?.draftModel === "string" && input.draftModel.trim()
			? { draftModel: input.draftModel.trim() }
			: {}),
		candidateLimit: clampCandidateLimit(input?.candidateLimit ?? DEFAULTS.candidateLimit),
		maxConcurrentActions: clampCandidateLimit(input?.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions),
		resourceCacheMaxEntries: positiveInteger(input?.resourceCacheMaxEntries, DEFAULTS.resourceCacheMaxEntries),
		resourceCacheMaxBytes: positiveInteger(input?.resourceCacheMaxBytes, DEFAULTS.resourceCacheMaxBytes),
		executionStoreMaxEntries: positiveInteger(
			input?.executionStoreMaxEntries,
			DEFAULT_PROVENANCE_STORE_LIMITS.maxCertificates,
		),
		executionStoreMaxBytes: positiveInteger(input?.executionStoreMaxBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes),
		predictionTimeoutMs: positiveInteger(input?.predictionTimeoutMs, DEFAULTS.predictionTimeoutMs),
		patternAware: patternAwareSettings(input?.patternAware ?? PATTERN_AWARE_DEFAULTS),
		selfSpeculation: normalizeSelfSpeculationSettings(input?.selfSpeculation),
		tools: normalizeSpeculativeToolSelection(input?.tools, KEYABLE_TOOLS),
	};
}

export function formatSpeculativeActionStatus(input: {
	readonly settings: EffectiveSpeculativeActionSettings;
	readonly metrics: SpeculativeActionMetrics;
}): string {
	const { settings, metrics } = input;
	const cache = metrics.cache;
	const processReuse = metrics.processReuse;
	const self = settings.selfSpeculation;
	return [
		`Enabled: ${settings.enabled ? "On" : "Off"}`,
		`Model Drafter: ${settings.drafterEnabled ? "On" : "Off"}`,
		`Drafter model: ${settings.draftModel ?? "active model"}`,
		`Candidate requests per Actor decision: ${settings.candidateLimit}`,
		`Model Drafter policy: ${settings.drafterMaxDepth} follow-up steps; ${settings.drafterMaxTokens ?? "provider default"} tokens; ${settings.drafterDeterministicCandidates} temperature-0 candidates; sampling ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
		`Simultaneous speculative tools: ${settings.maxConcurrentActions}`,
		`Storage policy: ${settings.resourceCacheMaxEntries} live results/${formatBytes(settings.resourceCacheMaxBytes)}; ${settings.executionStoreMaxEntries} reusable commands/${formatBytes(settings.executionStoreMaxBytes)}`,
		`Prediction wait limit: ${formatDuration(settings.predictionTimeoutMs)}`,
		`Learned patterns: ${settings.patternAware.enabled ? "On" : "Off"}; follow-up steps: ${settings.patternAware.multiStepEnabled ? "On" : "Off"} (alternatives/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}, learn after ${settings.patternAware.minOccurrences}, replay confidence≥${formatPercent(settings.patternAware.minBindingReplayProbability)}, gap ${settings.patternAware.maxFutureGap}, coverage ${formatPercent(settings.patternAware.futureGapCoverage)}, half-life ${settings.patternAware.decayHalfLifeEvents})`,
		`Actor probe: ${self.enabled && self.forkEnabled ? `On (${self.forkTransport})` : "Off"}; target verification ${self.enabled ? "On" : "Off"}; early tool execution ${self.enabled && self.forkTransport === "sidecar" && self.forkEnabled && self.forkActionEnabled ? `On (tool-name confidence ≥${formatPercent(self.forkActionMinConfidence)})` : "Off"}; benefit control ${self.forkGateEnabled ? `On (${self.forkGateWindowSize} samples, ≥${formatDuration(self.forkGateMinNetBenefitMs)} net)` : "Off"}; ${self.maxCandidates} candidates × ${self.maxDraftTokens} draft tokens; ${self.draftFormat} at ${self.draftBoundary}; ${self.forkTransport === "sidecar" ? self.endpoint : "provider-integrated"}`,
		`Prediction tools: ${toolsSummary(settings.tools)}`,
		"Execution routing: isolated runtime first; validated reads or private workspaces next; otherwise Actor execution",
		`Tool calls reused: ${formatRatio(metrics.speculativeHits, metrics.actorActions)}; ${metrics.exactReuseHits} exact, ${metrics.partialResultReuseHits} partial; ${formatDuration(metrics.executionAheadMs)} ready early, ${formatDuration(metrics.hitLatencyMs)} wait after match`,
		...(processReuse.requests > 0 ? [`Bash child commands: ${formatBashReuse(processReuse)}`] : []),
		`Predictions: ${formatRatio(metrics.predictionsMatched, metrics.predictionsObserved)} matched; ${formatRatio(metrics.predictionsAdopted, metrics.predictionsMatched)} adopted; unobserved: ${metrics.predictionsSettled - metrics.predictionsObserved}`,
		`Prediction rejections after match: ${countSummary(metrics.predictionRejectedAfterMatch)}`,
		`Actor candidate rejections: ${countSummary(metrics.actorCandidateRejections)}`,
		`Candidates: ${metrics.candidateStarted} started; ${metrics.candidateSucceeded} succeeded; ${metrics.candidateFailed} failed; ${metrics.candidateCancelled} cancelled`,
		metrics.tasks > 0
			? `Task timing (${metrics.tasks} completed; same-run accounting): ${formatDuration(metrics.endToEndMs)} wall time; ${formatDuration(metrics.serializedMs)} serialized counterfactual; ${formatDuration(metrics.hiddenLatencyMs)} observed overlap; ${formatDuration(metrics.nonToolMs)} non-tool; ${formatDuration(metrics.toolExecutionMs)} authoritative tools. Overlap is not a causal speedup estimate.`
			: "Task timing: n/a (no completed task); serialized overlap and speedup are not reported as 0.",
		`No-safe-route potential: ${metrics.executionBlockedActorActions} Actor actions; ${formatDuration(metrics.executionBlockedPotentialHiddenLatencyMs)} could be hidden; ${formatDuration(metrics.executionBlockedPotentialHitLatencyMs)} would remain; ${formatDuration(metrics.executionBlockedAttemptLeadMs)} attempt lead`,
		`Draft tokens: ${metrics.totalDraftTokens}`,
		`Live speculative results: ${cache.resultEntries}/${cache.cacheCapacity}, ${formatBytes(cache.resultBytes)}/${formatBytes(cache.cacheByteCapacity ?? 0)}; cold: ${cache.cacheCold}; hot: ${cache.cacheHot}; jobs: ${cache.inFlightJobs}; branches: ${cache.branchEntries} (${formatBytes(cache.branchBytes)})`,
	].join("\n");
}

export function createSpeculativeActionExtension(
	dependencies: SpeculativeActionExtensionDependencies = {},
): ExtensionFactory {
	return (pi) => {
		let controller: SpeculativeActionController | undefined;
		const wrapperSources = new Map<string, string>();
		const actorStream = new ActorStreamPreviewTracker(canPreviewIncompletePiCall);
		const providerRequest = new AsyncLocalStorage<"drafter">();

		pi.on("before_provider_request", (event) =>
			providerRequest.getStore() === "drafter" ? event.payload : controller?.decorateActorPayload(event.payload),
		);

		pi.on("session_start", async (_event, ctx) => {
			await controller?.dispose();
			controller = await installController(ctx, pi, dependencies, wrapperSources, providerRequest);
			controller.attachUI(ctx.ui);
		});
		pi.on("context", async (event, ctx) => {
			actorStream.clear();
			await controller?.startTurn(event.messages, ctx);
		});
		pi.on("message_update", (event, ctx) => {
			controller?.observeActorOutput(event.assistantMessageEvent);
			for (const preview of actorStream.observe(event.assistantMessageEvent)) {
				if (preview.type === "tool") {
					if (controller?.registeredTools().has(preview.tool))
						controller.previewActorTool(preview.tool, ctx.signal);
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
		pi.on("session_shutdown", async (_event, ctx) => {
			ctx.ui.setStatus(STATUS_KEY, undefined);
			const current = controller;
			controller = undefined;
			current?.detachUI();
			await current?.dispose().catch(() => undefined);
		});

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
): Promise<SpeculativeActionController> {
	let currentMetrics = emptyMetrics();
	let ui: ExtensionUIContext | undefined;
	let latestContext = context;
	let currentTurnID: string | undefined;
	let lastTurnID: string | undefined;
	let turnSequence = 0;
	let turnTools: readonly AgentTool[] = [];
	const recentEvents: string[] = [];
	const settingsStore =
		dependencies.createSettingsStore?.(context.cwd) ?? new SpeculativeActionSettingsStore(context.cwd);
	await settingsStore.load();
	let currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
	const settings = () => currentSettings;
	const selfSpeculation = new SelfSpeculationCoordinator({
		settings: () => {
			const configured = settings().selfSpeculation;
			return settings().enabled ? configured : { ...configured, enabled: false };
		},
		...(dependencies.selfSpeculationFetch ? { fetch: dependencies.selfSpeculationFetch } : {}),
	});
	const [piToolSettings, patternWorkspaceIdentity] = await Promise.all([
		loadPiToolSettings(context.cwd),
		resolvePatternWorkspaceIdentity(context.cwd),
	]);
	const localProcessOperations = createLocalBashOperations({
		...(piToolSettings.shellPath ? { shellPath: piToolSettings.shellPath } : {}),
	});
	const processCoordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(localProcessOperations));
	let workspaceSandbox: WorkspaceSandboxService | undefined;
	let configuredExecutionWorlds = dependencies.createExecutionWorlds?.();
	if (!configuredExecutionWorlds) {
		workspaceSandbox = dependencies.createWorkspaceSandboxService?.() ?? new WorkspaceSandboxService();
		configuredExecutionWorlds = [
			createLinuxProcessExecutionWorld({
				coordinator: processCoordinator,
				storeRoot: path.join(getAgentDir(), "speculative-action", "process-reuse"),
				workspaceSandbox,
			}),
			workspaceSandbox.createExecutionWorld(),
		];
	}
	const executionWorlds = [
		...new Set(configuredExecutionWorlds),
	];
	const configureExecutionStorage = () => {
		for (const world of executionWorlds)
			world.storage?.configure({
				maxEntries: currentSettings.executionStoreMaxEntries,
				maxBytes: currentSettings.executionStoreMaxBytes,
			});
	};
	configureExecutionStorage();
	let executionDiagnostics: readonly ExecutionWorldDiagnosticSnapshot[] = [];
	let executionDiagnosticsKnown = false;
	const availableTools = new Map(pi.getAllTools().map((tool) => [tool.name, tool]));
	const toolConflicts = new Map<string, string>();
	// Pi exposes metadata, but not another extension's execute function. Only stock tools and our own
	// wrappers can be intercepted without silently substituting different tool semantics.
	const baseDefinitions = new Map(
		[...createBaseToolDefinitions(context.cwd, piToolSettings, processCoordinator.operations)].filter(([name]) => {
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
	const toolCapabilities = () => resolveToolCapabilities(executionDiagnostics, executionDiagnosticsKnown);
	const runtimeSettings = () => ({
		...currentSettings,
		tools: activeTools(currentSettings, baseDefinitions.keys(), toolCapabilities()),
	});
	function renderFooter(): void {
		if (!ui) return;
		ui.setStatus(
			STATUS_KEY,
			formatSpeculativeFooter(settings(), currentMetrics, executionDiagnostics, toolConflicts.size),
		);
	}
	const host = (dependencies.createHost ?? createSpeculativeActionHost)(context.sessionManager.getSessionId(), {
		cwd: context.cwd,
		getSettings: runtimeSettings,
		complete: (model, llmContext, options) =>
			providerRequest.run("drafter", () => latestContext.modelRegistry.complete(model, llmContext, options)),
		draftModel: (actorModel) =>
			resolveSpeculativeDraftModel(settings().draftModel, actorModel, latestContext.modelRegistry),
		getDraftOptions: async ({ draftModel, actorOptions, signal }) =>
			resolveDraftOptions({
				draftModel,
				actorOptions,
				signal,
				modelRegistry: latestContext.modelRegistry,
			}),
		preflight: ({ toolName }) =>
			latestContext.isProjectTrusted() && baseDefinitions.has(toolName) && pi.getActiveTools().includes(toolName),
		resolveInvocation: (tool, input) =>
			resolvePiToolInvocation(tool, input, {
				cwd: latestContext.cwd,
				environment: piShellEnvironment(latestContext),
				...(piToolSettings.shellPath ? { shellPath: piToolSettings.shellPath } : {}),
				...(piToolSettings.shellCommandPrefix ? { shellCommandPrefix: piToolSettings.shellCommandPrefix } : {}),
			}),
		projectionRules: [
			PI_READ_RANGE_PROJECTION_RULE,
			...(piToolSettings.shellCommandPrefix ? [] : [PI_BASH_TAIL_LINES_PROJECTION_RULE]),
		],
		executionWorlds,
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
	const refreshExecutionDiagnostics = async (refresh = false): Promise<void> => {
		executionDiagnostics = await host.executionWorldDiagnostics(refresh);
		executionDiagnosticsKnown = true;
		await recoverSpeculation(() => host.runtime.settingsChanged(runtimeSettings()));
		renderFooter();
	};

	const controller: SpeculativeActionController = {
		settings,
		editableSettings: () => normalizeSpeculativeActionSettings(settingsStore.editable()),
		settingsScope: () => settingsStore.scope,
		setSettingsScope: (scope) => settingsStore.setScope(scope),
		metrics: () => currentMetrics,
		registeredTools: () => new Set(baseDefinitions.keys()),
		toolCapabilities,
		toolConflicts: () => new Map(toolConflicts),
		recentEvents: () => [...recentEvents],
		refreshExecutionDiagnostics,
		executionSummary: () => executionWorldSummary(executionDiagnostics),
		maintainExecutionStorage: async (operation) => {
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
		setSettings: (value) => {
			if (value)
				settingsStore.setEffective(value, normalizeSpeculativeActionSettings(settingsStore.editable("global")));
			else settingsStore.clear();
			currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
			configureExecutionStorage();
			if (!currentSettings.enabled || !currentSettings.selfSpeculation.enabled) selfSpeculation.reset();
			void recoverSpeculation(() => host.runtime.settingsChanged(runtimeSettings()));
			renderFooter();
		},
		attachUI: (nextUI) => {
			ui = nextUI;
			renderFooter();
		},
		detachUI: () => {
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
		},
		startTurn: async (messages, nextContext) => {
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
		previewActorCall: (tool, callID, input, signal) => {
			const turnID = currentTurnID;
			if (!turnID || !baseDefinitions.has(tool)) return;
			void recoverSpeculation(() =>
				host.previewActorCall({ turnID, id: callID, tool, args: input, tools: turnTools }, signal),
			);
		},
		previewActorTool: (tool, signal) => {
			const turnID = currentTurnID;
			if (!turnID || !baseDefinitions.has(tool)) return;
			void recoverSpeculation(() => host.previewActorTool({ turnID, tool }, signal));
		},
		decorateActorPayload: (payload) => selfSpeculation.decorateActorPayload(payload),
		observeActorOutput: (event) => selfSpeculation.observeActorOutput(event),
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
		execute: async (tool, callID, input, signal, onUpdate, nextContext) => {
			latestContext = nextContext;
			const definition = baseDefinitions.get(tool);
			if (!definition) throw new Error(`Speculative wrapper has no base tool ${tool}`);
			const turnID = currentTurnID;
			return host.execute(
				{ ...(turnID ? { turnID } : {}), id: callID, tool, args: input, tools: turnTools },
				signal,
				async (operation) =>
					withPiProjectionCoverage(
						operation.tool,
						operation.input,
						await definition.execute(
							callID,
							operation.input as never,
							operation.signal,
							onUpdate as never,
							nextContext,
						),
					),
			);
		},
		statusText: () => {
			const effective = settings();
			return [
				formatSpeculativeActionStatus({ settings: { ...effective, tools: runtimeSettings().tools }, metrics: currentMetrics }),
				formatDrafterGateStatus(effective.drafterGateEnabled, host.drafterGateSnapshot()),
				formatSelfSpeculationStatus(selfSpeculation.snapshot()),
				executionWorldSummary(executionDiagnostics),
				`Custom tool conflicts: ${toolConflictSummary(toolConflicts)}`,
			].join("\n");
		},
		dispose: async () => {
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
			await settingsStore.flush();
			try {
				await host.dispose();
			} finally {
				try {
					await workspaceSandbox?.dispose();
				} finally {
					await selfSpeculation.dispose();
				}
			}
		},
	};
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

function createBaseToolDefinitions(
	cwd: string,
	settings: PiToolSettings,
	processOperations: ProcessToolOperations,
): Map<string, BaseToolDefinition> {
	return new Map<string, BaseToolDefinition>([
		["read", createReadToolDefinition(cwd, { autoResizeImages: settings.autoResizeImages })],
		[
			"bash",
			createBashToolDefinition(cwd, {
				operations: processOperations,
				...(settings.shellPath ? { shellPath: settings.shellPath } : {}),
				...(settings.shellCommandPrefix ? { commandPrefix: settings.shellCommandPrefix } : {}),
			}),
		],
		["edit", createEditToolDefinition(cwd)],
		["write", createWriteToolDefinition(cwd)],
		["grep", createGrepToolDefinition(cwd)],
		["find", createFindToolDefinition(cwd)],
		["ls", createLsToolDefinition(cwd)],
	]);
}

function speculativeToolDefinition(base: BaseToolDefinition, controller: SpeculativeActionController): ToolDefinition {
	return {
		name: base.name,
		label: base.label,
		description: base.description,
		parameters: base.parameters,
		...(base.promptSnippet ? { promptSnippet: base.promptSnippet } : {}),
		...(base.promptGuidelines ? { promptGuidelines: base.promptGuidelines } : {}),
		...(base.prepareArguments ? { prepareArguments: base.prepareArguments } : {}),
		...(base.constrainedSampling ? { constrainedSampling: base.constrainedSampling } : {}),
		...(base.executionMode ? { executionMode: base.executionMode } : {}),
		...(base.renderShell ? { renderShell: base.renderShell } : {}),
		...(base.renderCall ? { renderCall: base.renderCall as ToolDefinition["renderCall"] } : {}),
		...(base.renderResult ? { renderResult: base.renderResult as ToolDefinition["renderResult"] } : {}),
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

function toAgentTool(base: BaseToolDefinition, context: () => ExtensionContext): AgentTool {
	return {
		name: base.name,
		label: base.label,
		description: base.description,
		parameters: base.parameters,
		...(base.prepareArguments ? { prepareArguments: base.prepareArguments } : {}),
		...(base.executionMode ? { executionMode: base.executionMode } : {}),
		execute: async (callID, input, signal, onUpdate) =>
			withPiProjectionCoverage(
				base.name,
				input,
				await base.execute(callID, input as never, signal, onUpdate as never, context()),
			),
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
	return Object.fromEntries(
		Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined),
	);
}

async function loadPiToolSettings(cwd: string): Promise<PiToolSettings> {
	const [global, project] = await Promise.all([
		readJsonRecord(path.join(getAgentDir(), "settings.json")),
		readJsonRecord(path.join(cwd, ".pi", "settings.json")),
	]);
	const shellPath = stringSetting(project.shellPath) ?? stringSetting(global.shellPath);
	const shellCommandPrefix = stringSetting(project.shellCommandPrefix) ?? stringSetting(global.shellCommandPrefix);
	const projectImages = recordSetting(project.images);
	const globalImages = recordSetting(global.images);
	const autoResize = projectImages.autoResize ?? globalImages.autoResize;
	return {
		...(shellPath ? { shellPath: expandHome(shellPath) } : {}),
		...(shellCommandPrefix ? { shellCommandPrefix } : {}),
		autoResizeImages: typeof autoResize === "boolean" ? autoResize : true,
	};
}

async function readJsonRecord(file: string): Promise<Record<string, unknown>> {
	try {
		const value: unknown = JSON.parse(await readFile(file, "utf8"));
		return recordSetting(value);
	} catch {
		return {};
	}
}

function recordSetting(value: unknown): Record<string, unknown> {
	return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function stringSetting(value: unknown): string | undefined {
	return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function expandHome(value: string): string {
	if (value !== "~" && !value.startsWith("~/") && !value.startsWith("~\\")) return value;
	const home = process.env.USERPROFILE ?? process.env.HOME;
	return home ? path.join(home, value.slice(2)) : value;
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
		controller.setSettings({ ...controller.settings(), enabled: command === "on" });
		ctx.ui.notify(`Speculative action ${command === "on" ? "enabled" : "disabled"}.`, "info");
		return;
	}
	if (command === "reset") {
		controller.setSettings(undefined);
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
	let applied = cloneSettings(controller.editableSettings());
	let draft = cloneSettings(applied);
	const editor: SpeculativeActionController = {
		...controller,
		settings: () => draft,
		setSettings: (value) => {
			draft = cloneSettings(normalizeSpeculativeActionSettings(value));
		},
	};
	const reload = () => {
		applied = cloneSettings(controller.editableSettings());
		draft = cloneSettings(applied);
	};
	while (true) {
		const dirty = !sameSettings(draft, applied);
		const toolPolicy = toolPolicyCounts(draft, controller.registeredTools(), controller.toolCapabilities());
		const scope = controller.settingsScope() === "global" ? "All projects" : "This project";
		const choice = await ctx.ui.select("Speculative action", [
			`Enabled: ${draft.enabled ? "On" : "Off"}`,
			`Save settings to: ${scope}${sameSettings(applied, controller.settings()) ? "" : " (this project overrides shared settings)"}`,
			`Prediction sources › ${sourceSummary(draft)}`,
			`Tools & execution › ${toolPolicy.active}/${toolPolicy.available} active`,
			"Advanced settings › tuning, decoding, scheduling, storage",
			`Apply changes${dirty ? " (pending)" : ""}`,
			...(dirty ? ["Discard changes"] : []),
			"Status",
			"Recent events",
			"Restore defaults",
			CLOSE,
		]);
		if (!choice || choice === CLOSE) {
			if (
				!dirty ||
				(await ctx.ui.confirm("Discard changes?", "Close without applying the pending speculative-action changes?"))
			)
				return;
			continue;
		}
		if (choice.startsWith("Enabled:")) {
			editor.setSettings({ ...draft, enabled: !draft.enabled });
			continue;
		}
		if (choice.startsWith("Save settings to:")) {
			const selected = await ctx.ui.select("Save settings to", ["All projects", "This project", BACK]);
			if (
				(selected === "All projects" || selected === "This project") &&
				(!dirty || (await ctx.ui.confirm("Discard changes?", "Switch configuration scope without applying?")))
			) {
				controller.setSettingsScope(selected === "All projects" ? "global" : "project");
				reload();
			}
			continue;
		}
		if (choice.startsWith("Prediction sources")) {
			await openPredictionSources(ctx, editor);
			continue;
		}
		if (choice.startsWith("Tools & execution")) {
			await openToolsAndExecution(ctx, editor, controller);
			continue;
		}
		if (choice.startsWith("Advanced settings")) {
			await openAdvancedSettings(ctx, editor);
			continue;
		}
		if (choice.startsWith("Apply changes")) {
			if (!dirty) {
				ctx.ui.notify("No pending speculative-action changes.", "info");
				continue;
			}
			controller.setSettings(draft);
			reload();
			ctx.ui.notify("Speculative-action settings applied.", "info");
			continue;
		}
		if (choice === "Discard changes") {
			draft = cloneSettings(applied);
			continue;
		}
		if (choice === "Status") {
			await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
			ctx.ui.notify(controller.statusText(), "info");
			continue;
		}
		if (choice === "Recent events") {
			showRecentEvents(ctx, controller);
			continue;
		}
		if (choice === "Restore defaults") {
			if (
				!(await ctx.ui.confirm(
					"Restore defaults?",
					"Restore tuning values while keeping the main switch and prediction-source choices?",
				))
			)
				continue;
			const defaults = normalizeSpeculativeActionSettings(undefined);
			editor.setSettings({
				...defaults,
				enabled: draft.enabled,
				drafterEnabled: draft.drafterEnabled,
				patternAware: {
					...defaults.patternAware,
					enabled: draft.patternAware.enabled,
					multiStepEnabled: draft.patternAware.multiStepEnabled,
				},
				selfSpeculation: {
					...defaults.selfSpeculation,
					enabled: draft.selfSpeculation.enabled,
					forkEnabled: draft.selfSpeculation.forkEnabled,
					forkActionEnabled: draft.selfSpeculation.forkActionEnabled,
				},
			});
		}
	}
}

async function openPredictionSources(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const choice = await ctx.ui.select("Prediction sources", [
			`Model Drafter › ${settings.drafterEnabled ? "On" : "Off"}, ${settings.draftModel ?? activeModelReference(ctx)}`,
			`Actor probe › ${actorForkSummary(settings.selfSpeculation)}`,
			`Learned patterns › ${settings.patternAware.enabled ? "On" : "Off"}, ${settings.patternAware.multiStepEnabled ? "follow-up steps" : "next step only"}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Model Drafter")) await openDrafterSettings(ctx, controller);
		if (choice.startsWith("Actor probe")) await openActorForkSettings(ctx, controller);
		if (choice.startsWith("Learned patterns")) await openPatternAwareSettings(ctx, controller);
	}
}

function openAdvancedSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Advanced settings", () => {
		const settings = controller.settings();
		return new Map<string, MenuAction>([
			[`Model Drafter tuning › ${settings.candidateLimit} requests, ${settings.drafterMaxDepth} follow-up steps`, () => openDrafterAdvancedSettings(ctx, controller)],
			[`Actor probe and target verification › ${settings.selfSpeculation.forkTransport}`, () => openActorForkSettings(ctx, controller, "advanced")],
			[`Learned-pattern tuning › ${settings.patternAware.maxPatterns} stored patterns`, () => openPatternAdvancedSettings(ctx, controller)],
			[`Scheduling and storage › ${settings.maxConcurrentActions} simultaneous tools`, () => openSchedulingAndCache(ctx, controller)],
		]);
	});
}

function openDrafterSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Model Drafter", () => {
		const settings = controller.settings();
		const edit = (field: RootInputField) => editRootSetting(ctx, controller, settings, field);
		return new Map<string, MenuAction>([
			[`Enabled: ${settings.drafterEnabled ? "On" : "Off"}`, () => controller.setSettings({ ...settings, drafterEnabled: !settings.drafterEnabled })],
			[`Model › ${settings.draftModel ?? activeModelReference(ctx)}`, () => editDraftModel(ctx, controller, settings)],
			[`Candidate requests per decision: ${settings.candidateLimit}`, () => edit("candidateLimit")],
			[`Advanced settings › sampling, follow-up steps, cost control`, () => openDrafterAdvancedSettings(ctx, controller)],
		]);
	});
}

function openDrafterAdvancedSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Model Drafter advanced", () => {
		const settings = controller.settings();
		const edit = (field: RootInputField) => editRootSetting(ctx, controller, settings, field);
		return new Map<string, MenuAction>([
			[`Pause when measured cost exceeds benefit: ${settings.drafterGateEnabled ? "On" : "Off"}`, () => controller.setSettings({ ...settings, drafterGateEnabled: !settings.drafterGateEnabled })],
			[`Follow-up tool steps: ${settings.drafterMaxDepth}`, () => edit("drafterMaxDepth")],
			[`Maximum output tokens: ${settings.drafterMaxTokens ?? "Provider default"}`, () => edit("drafterMaxTokens")],
			[`Temperature-0 candidates: ${settings.drafterDeterministicCandidates}`, () => edit("drafterDeterministicCandidates")],
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
		const edit = (field: SelfSpeculationInputField) => editSelfSpeculationSetting(ctx, controller, settings, field);
		const actions = new Map<string, MenuAction>();
		if (menu === "basic") {
			const active = self.enabled && self.forkEnabled;
			actions.set(`Actor probe prediction: ${active ? "On" : "Off"}`, () => updateSelfSpeculation(controller, settings, { enabled: active ? self.enabled : true, forkEnabled: !active }));
			actions.set("Advanced settings › integration, decoding, verification, benefit control", () => openActorForkSettings(ctx, controller, "advanced"));
			if (self.forkTransport === "sidecar") {
				actions.set(`Use forked calls for tool pre-execution: ${self.forkActionEnabled ? "On" : "Off"}`, () => updateSelfSpeculation(controller, settings, { forkActionEnabled: !self.forkActionEnabled }));
				if (self.forkActionEnabled) actions.set(`Minimum tool-name confidence: ${formatPercent(self.forkActionMinConfidence)}`, () => edit("forkActionMinConfidence"));
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
					updateSelfSpeculation(controller, settings, { forkTransport: selected === "Provider-integrated" ? "provider" : "sidecar" });
			});
			if (self.forkTransport === "sidecar") {
				actions.set(`Control service URL: ${self.endpoint}`, () => edit("endpoint"));
				actions.set(`Request timeout: ${formatDuration(self.timeoutMs)}`, () => edit("timeoutMs"));
				actions.set(`Authentication token variable: ${self.apiKeyEnv ?? "None"}`, () => edit("apiKeyEnv"));
			}
		} else if (menu === "fork") {
			actions.set(`Maximum output tokens: ${self.forkMaxTokens}`, () => edit("forkMaxTokens"));
			actions.set(`Sampling temperature: ${formatNumber(self.forkTemperature)}`, () => edit("forkTemperature"));
			actions.set(`Tool-call decoder: ${self.forkDecoder}`, () => edit("forkDecoder"));
			actions.set(`Forced tool-call prefix: ${self.forkForcedPrefix}`, () => edit("forkForcedPrefix"));
		} else if (menu === "target") {
			actions.set(`Verify predicted calls during Actor decoding: ${self.enabled ? "On" : "Off"}`, () => updateSelfSpeculation(controller, settings, { enabled: !self.enabled }));
			actions.set(`Candidates sent per decision: ${self.maxCandidates}`, () => edit("maxCandidates"));
			actions.set(`Draft-token limit per candidate: ${self.maxDraftTokens}`, () => edit("maxDraftTokens"));
			actions.set(`Tool-call format: ${self.draftFormat}`, () => edit("draftFormat"));
			actions.set(`Tool-call boundary: ${self.draftBoundary}`, () => edit("draftBoundary"));
		} else {
			actions.set(`Pause forks that stop saving time: ${self.forkGateEnabled ? "On" : "Off"}`, () => updateSelfSpeculation(controller, settings, { forkGateEnabled: !self.forkGateEnabled }));
			if (self.forkGateEnabled) {
				actions.set(`Warm-up samples: ${self.forkGateMinSamples}`, () => edit("forkGateMinSamples"));
				actions.set(`Rolling samples: ${self.forkGateWindowSize}`, () => edit("forkGateWindowSize"));
				actions.set(`Minimum expected time saved: ${formatDuration(self.forkGateMinNetBenefitMs)}`, () => edit("forkGateMinNetBenefitMs"));
				actions.set(`Recovery probe interval: ${self.forkGateProbeInterval}`, () => edit("forkGateProbeInterval"));
				actions.set(`Consecutive-failure limit: ${self.forkGateFailureThreshold}`, () => edit("forkGateFailureThreshold"));
			}
		}
		return actions;
	});
}

function openPatternAwareSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Learned patterns", () => {
		const settings = controller.settings();
		const pattern = settings.patternAware;
		return new Map<string, MenuAction>([
			[`Enabled: ${pattern.enabled ? "On" : "Off"}`, () => controller.setSettings({ ...settings, patternAware: { ...pattern, enabled: !pattern.enabled } })],
			[`Predict follow-up tool steps: ${pattern.multiStepEnabled ? "On" : "Off"}`, () => controller.setSettings({ ...settings, patternAware: { ...pattern, multiStepEnabled: !pattern.multiStepEnabled } })],
			[`Advanced settings › history, confidence, search limits`, () => openPatternAdvancedSettings(ctx, controller)],
		]);
	});
}

function openPatternAdvancedSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runActionMenuLoop(ctx, "Learned-pattern advanced", () => {
		const pattern = controller.settings().patternAware;
		const actions = new Map<string, MenuAction>([
			[`Learning history › ${pattern.maxContextLength} previous actions`, () => openPatternAdvancedGroup(ctx, controller, "learning")],
		]);
		if (pattern.multiStepEnabled)
			actions.set(`Multi-step search › ${pattern.beamWidth} alternatives/tool, ${pattern.maxPredictionDepth} steps`, () => openPatternAdvancedGroup(ctx, controller, "multiStep"));
		return actions;
	});
}

async function openPatternAdvancedGroup(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	group: "learning" | "multiStep",
): Promise<void> {
	return runActionMenuLoop(ctx, group === "learning" ? "Learning history" : "Multi-step search", () => {
		const settings = controller.settings();
		const pattern = settings.patternAware;
		const edit = (field: PatternInputField) => editPatternSetting(ctx, controller, settings, field);
		const actions = group === "learning"
			? new Map<string, () => Promise<void>>([
				[`Previous actions used as context: ${pattern.maxContextLength}`, () => edit("maxContextLength")],
				[`Maximum skipped Actor decisions: ${pattern.maxFutureGap}`, () => edit("maxFutureGap")],
				[`Early-prediction coverage: ${formatPercent(pattern.futureGapCoverage)}`, () => edit("futureGapCoverage")],
				[`History half-life: ${pattern.decayHalfLifeEvents} events`, () => edit("decayHalfLifeEvents")],
				[`Uses before learning a pattern: ${pattern.minOccurrences}`, () => edit("minOccurrences")],
				[`Stored pattern limit: ${pattern.maxPatterns}`, () => edit("maxPatterns")],
			])
			: new Map<string, () => Promise<void>>([
				[`Alternatives retained per tool: ${pattern.beamWidth}`, () => edit("beamWidth")],
				[`Maximum predicted tool steps: ${pattern.maxPredictionDepth}`, () => edit("maxPredictionDepth")],
				[`Minimum argument-replay confidence: ${formatPercent(pattern.minBindingReplayProbability)}`, () => edit("minBindingReplayProbability")],
			]);
		return actions;
	});
}

async function openSchedulingAndCache(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const fields = new Map<string, RootInputField>([
			[`Simultaneous speculative tools: ${settings.maxConcurrentActions}`, "maxConcurrentActions"],
			[`Prediction wait limit: ${formatDuration(settings.predictionTimeoutMs)}`, "predictionTimeoutMs"],
			[`Live result entries: ${settings.resourceCacheMaxEntries}`, "resourceCacheMaxEntries"],
			[`Live result memory: ${formatBytes(settings.resourceCacheMaxBytes)}`, "resourceCacheMaxBytes"],
			[`Reusable command history entries: ${settings.executionStoreMaxEntries}`, "executionStoreMaxEntries"],
			[`Reusable command history memory: ${formatBytes(settings.executionStoreMaxBytes)}`, "executionStoreMaxBytes"],
		]);
		const reclaim = "Reclaim reusable command history";
		const clear = "Clear reusable command history";
		const choice = await ctx.ui.select("Scheduling and storage", [...fields.keys(), reclaim, clear, BACK]);
		if (!choice || choice === BACK) return;
		if (choice === clear && !(await ctx.ui.confirm("Clear reusable command history?", "Delete all reusable command results and file effects? This cannot be undone."))) continue;
		const operation = choice === reclaim ? "gc" : choice === clear ? "clear" : undefined;
		if (operation) {
			const report = await recoverSpeculation(() => controller.maintainExecutionStorage(operation));
			ctx.ui.notify(
				report?.text ?? "Reusable command history maintenance failed.",
				report && !report.failed ? "info" : "warning",
			);
			continue;
		}
		const field = fields.get(choice);
		if (field) await editRootSetting(ctx, controller, settings, field);
	}
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

async function openToolsAndExecution(
	ctx: ExtensionContext,
	editor: SpeculativeActionController,
	controller: SpeculativeActionController,
): Promise<void> {
	while (true) {
		const settings = editor.settings();
		const policy = toolPolicyCounts(settings, controller.registeredTools(), controller.toolCapabilities());
		const choice = await ctx.ui.select("Tools & execution", [
			`Tool policy › ${policy.active}/${policy.available} active`,
			"Execution routes",
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Tool policy"))
			await editToolPolicy(
				ctx,
				editor,
				controller.registeredTools(),
				controller.toolConflicts(),
				controller.toolCapabilities(),
			);
		if (choice === "Execution routes") {
			await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
			ctx.ui.notify(
				`Each tool uses the first ready execution route whose guarantees cover its effects. Read-only tools can use validated results, write/edit can use private Git workspaces, and tools without a safe route remain with the Actor.\n${controller.executionSummary()}`,
				"info",
			);
		}
	}
}

async function editToolPolicy(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	registered: ReadonlySet<string>,
	conflicts: ReadonlyMap<string, string>,
	capabilities: ReadonlyMap<string, ExecutionCapabilityStatus>,
): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const tools = [...new Set([...KEYABLE_TOOLS, ...settings.tools, ...registered])].sort(
			(left, right) => toolCategory(left) - toolCategory(right) || left.localeCompare(right),
		);
		const labels = new Map<string, string>();
		for (const tool of tools) {
			const supported = (KEYABLE_TOOLS as readonly string[]).includes(tool);
			const selected = supported && settings.tools.includes(tool);
			const route = capabilities.get(tool);
			const active = selected && registered.has(tool) && route?.state !== "unavailable" && !conflicts.has(tool);
			const marker = active ? "[x]" : selected ? "[~]" : "[ ]";
			const availability = conflicts.has(tool)
				? "custom override"
				: !registered.has(tool)
					? "not registered"
					: routeLabel(route);
			labels.set(`${marker} ${tool} · ${availability}`, tool);
		}
		const choice = await ctx.ui.select("Tool policy · [x] active · [~] selected · [ ] off", [...labels.keys(), BACK]);
		if (!choice || choice === BACK) return;
		const tool = labels.get(choice);
		if (!tool) continue;
		if (!(KEYABLE_TOOLS as readonly string[]).includes(tool)) {
			ctx.ui.notify(`${tool} has no speculative action semantics.`, "warning");
			continue;
		}
		const selected = settings.tools.includes(tool);
		if (!selected && capabilities.get(tool)?.state === "unavailable") {
			ctx.ui.notify(`${tool} cannot be enabled here: ${routeLabel(capabilities.get(tool))}.`, "warning");
			continue;
		}
		if (!selected && !registered.has(tool)) {
			const conflict = conflicts.get(tool);
			ctx.ui.notify(
				conflict
					? `${tool} is provided by ${conflict}. Custom tool overrides remain authoritative and are excluded from speculation.`
					: `${tool} is not registered in the current Pi session.`,
				"warning",
			);
			continue;
		}
		const next = selected ? settings.tools.filter((item) => item !== tool) : [...settings.tools, tool];
		controller.setSettings({ ...settings, tools: next });
	}
}

function updateSelfSpeculation(
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	update: Partial<SelfSpeculationSettings>,
): void {
	controller.setSettings({
		...settings,
		selfSpeculation: { ...settings.selfSpeculation, ...update },
	});
}

async function promptSetting<T>(
	ctx: ExtensionContext,
	current: T,
	descriptor: SettingInputDescriptor<T>,
): Promise<{ readonly accepted: false } | { readonly accepted: true; readonly value: T }> {
	const input = await ctx.ui.input(descriptor.title, descriptor.format(current));
	if (input === undefined) return { accepted: false };
	const parsed = descriptor.parse(input);
	if (!parsed.ok) {
		ctx.ui.notify(parsed.error, "warning");
		return { accepted: false };
	}
	return { accepted: true, value: parsed.value };
}

function replaceSetting<T extends object, Field extends keyof T>(current: T, field: Field, value: T[Field]): T {
	const next = { ...current };
	if (value === undefined) Reflect.deleteProperty(next, field);
	else Object.assign(next, { [field]: value });
	return next;
}

function inputDescriptor<T, Field extends keyof T>(
	descriptors: Partial<SettingInputDescriptors<T, keyof T>>,
	field: Field,
): SettingInputDescriptor<T[Field]> {
	return descriptors[field] as SettingInputDescriptor<T[Field]>;
}

async function editRootSetting<Field extends RootInputField>(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: Field,
): Promise<void> {
	const edited = await promptSetting(
		ctx,
		settings[field],
		inputDescriptor<EffectiveSpeculativeActionSettings, Field>(ROOT_SETTING_INPUTS, field),
	);
	if (edited.accepted) controller.setSettings(replaceSetting(settings, field, edited.value));
}

async function editSelfSpeculationSetting<Field extends SelfSpeculationInputField>(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: Field,
): Promise<void> {
	const edited = await promptSetting(
		ctx,
		settings.selfSpeculation[field],
		inputDescriptor<SelfSpeculationSettings, Field>(SELF_SPECULATION_INPUTS, field),
	);
	if (edited.accepted) {
		controller.setSettings({
			...settings,
			selfSpeculation: replaceSetting(settings.selfSpeculation, field, edited.value),
		});
	}
}

async function editPatternSetting<Field extends PatternInputField>(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: Field,
): Promise<void> {
	const edited = await promptSetting(
		ctx,
		settings.patternAware[field],
		inputDescriptor<PatternAwareSettings, Field>(PATTERN_SETTING_INPUTS, field),
	);
	if (edited.accepted) {
		controller.setSettings({
			...settings,
			patternAware: replaceSetting(settings.patternAware, field, edited.value),
		});
	}
}

async function editDrafterTemperatureRange(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const edited = await promptSetting(
		ctx,
		[settings.drafterTemperatureMin, settings.drafterTemperatureMax] as const,
		DRAFTER_TEMPERATURE_INPUT,
	);
	if (!edited.accepted) return;
	const [drafterTemperatureMin, drafterTemperatureMax] = edited.value;
	controller.setSettings({ ...settings, drafterTemperatureMin, drafterTemperatureMax });
}

async function editDraftModel(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const models = ctx.modelRegistry
		.getAvailable()
		.filter((model) => ctx.modelRegistry.hasConfiguredAuth(model))
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
		controller.setSettings(baseSettings);
		return;
	}
	if (choice === CUSTOM_MODEL) {
		const value = await ctx.ui.input("Custom drafter model", "provider/model");
		if (value === undefined) return;
		const draftModel = value.trim();
		controller.setSettings({ ...baseSettings, ...(draftModel ? { draftModel } : {}) });
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
	if (draftModel) controller.setSettings({ ...baseSettings, draftModel });
}

function showRecentEvents(ctx: ExtensionContext, controller: SpeculativeActionController): void {
	const events = controller.recentEvents();
	ctx.ui.notify(events.length > 0 ? events.join("\n") : "No speculative action events recorded yet.", "info");
}

export function formatSpeculativeActionEvent(event: SpeculativeActionEvent<string>): string {
	const parts = [`[${event.type}]`, `session ${compactEventText(String(event.sessionID))}`, `turn ${compactEventText(event.turnID)}`];
	switch (event.type) {
		case "task":
			parts.push(
				`${formatDuration(event.timing.endToEndMs)} wall`,
				`${formatDuration(event.timing.serializedMs)} serialized counterfactual`,
				`${formatDuration(event.timing.hiddenLatencyMs)} observed overlap (not causal speedup)`,
			);
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
				if (reuse?.requests) {
					parts.push(`Bash child commands ${formatBashReuse(reuse)}`);
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
					match.kind === "projected" ? `partial-result reuse (${match.projector})` : "exact-action reuse",
					`${formatDuration(event.settlement.provider.timing.executionAheadMs)} ahead`,
					`${formatDuration(event.settlement.provider.timing.hitLatencyMs)} hit latency`,
					`${formatDuration(event.settlement.provider.timing.attemptLeadMs)} attempt lead`,
				);
			} else {
				parts.push(
					`${formatDuration(event.settlement.provider.durationMs)} Actor ${event.settlement.provider.origin} execution`,
				);
				const timing =
					event.settlement.provider.origin === "fallback"
						? event.settlement.provider.executionBlockedTiming
						: undefined;
				if (timing) {
					parts.push(
						`${formatDuration(timing.executionAheadMs)} potentially hidden`,
						`${formatDuration(timing.hitLatencyMs)} would remain`,
						`${formatDuration(timing.attemptLeadMs)} prediction lead`,
					);
				}
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

export function resolveSpeculativeDraftModel(
	reference: string | undefined,
	actorModel: Model<Api>,
	modelRegistry: ModelRegistry,
): Model<Api> {
	if (!reference) return actorModel;
	const model = findExactModelReferenceMatch(reference, modelRegistry.getAll());
	return model && modelRegistry.hasConfiguredAuth(model) ? model : actorModel;
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

async function resolveDraftOptions(input: {
	readonly draftModel: Model<Api>;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly signal: AbortSignal;
	readonly modelRegistry: ModelRegistry;
}): Promise<SimpleStreamOptions> {
	const base: SimpleStreamOptions = {
		...input.actorOptions,
		signal: input.signal,
	};
	const auth = await input.modelRegistry.getApiKeyAndHeaders(input.draftModel);
	if (!auth.ok) throw new Error(auth.error);
	return { ...base, apiKey: auth.apiKey, headers: auth.headers, env: auth.env };
}

function emptyMetrics(): SpeculativeActionMetrics {
	return emptySpeculativeTraceSummary({
		cacheCapacity: DEFAULTS.resourceCacheMaxEntries,
		cacheByteCapacity: DEFAULTS.resourceCacheMaxBytes,
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

function positiveInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function mebibyteInput(title: string): SettingInputDescriptor<number> {
	return positiveIntegerInput(`${title} (MiB)`, {
		error: `${title} must be a positive integer in MiB.`,
		format: (bytes) => String(Math.max(1, Math.round(bytes / (1024 * 1024)))),
		transform: (mebibytes) => mebibytes * 1024 * 1024,
	});
}

function cloneSettings(settings: EffectiveSpeculativeActionSettings): EffectiveSpeculativeActionSettings {
	return structuredClone(settings);
}

function sameSettings(left: EffectiveSpeculativeActionSettings, right: EffectiveSpeculativeActionSettings): boolean {
	return JSON.stringify(left) === JSON.stringify(right);
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

function formatDrafterGateStatus(enabled: boolean, gate: DrafterUtilityGateSnapshot): string {
	return `Action Drafter gate: ${enabled ? "On" : "Off"}; ${gate.skippedBatches} batches skipped, ${gate.samples} samples${gate.expectedNetBenefitMs === undefined ? "" : `, ${formatDuration(gate.expectedNetBenefitMs)} expected net`}`;
}

function formatSelfSpeculationStatus(bridge: SelfSpeculationCoordinatorSnapshot): string {
	return [
		`Self-speculation: ${bridge.bufferedCandidates} buffered`,
		`${bridge.candidateSubmissions} bundles/${bridge.candidateReceipts} receipts`,
		`${bridge.forkRequests}/${bridge.forkCompletions} probes completed (${bridge.forkRetries} later-snapshot retries), ${bridge.forkGateSkips} gated${bridge.forkGateExpectedNetBenefitMs === undefined ? "" : ` at ${formatDuration(bridge.forkGateExpectedNetBenefitMs)} expected net`}`,
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
	worlds: readonly ExecutionWorldDiagnosticSnapshot[],
	known = true,
): ReadonlyMap<string, ExecutionCapabilityStatus> {
	return new Map(
		KEYABLE_TOOLS.map((tool) => {
			const requirements = PI_ACTION_SEMANTICS.requirements(tool);
			const status = requirements && known
				? executionCapabilityStatus(requirements, worlds)
				: { state: "registered" as const, candidates: [] };
			return [tool, status];
		}),
	);
}

function activeTools(
	settings: EffectiveSpeculativeActionSettings,
	registered: Iterable<string>,
	capabilities: ReadonlyMap<string, ExecutionCapabilityStatus>,
): readonly string[] {
	const available = new Set(registered);
	return [...new Set(settings.tools)].filter(
		(tool) => available.has(tool) && capabilities.get(tool)?.state !== "unavailable",
	);
}

function toolPolicyCounts(
	settings: EffectiveSpeculativeActionSettings,
	registered: ReadonlySet<string>,
	capabilities: ReadonlyMap<string, ExecutionCapabilityStatus>,
): { readonly active: number; readonly available: number } {
	return {
		active: activeTools(settings, registered, capabilities).length,
		available: [...registered].filter((tool) => capabilities.get(tool)?.state !== "unavailable").length,
	};
}

function executionRouteKind(isolation: SpeculativeExecution): string {
	switch (isolation) {
		case "runtime_sandbox": return "isolated runtime";
		case "resource_snapshot": return "validated read";
		case "workspace_branch": return "private workspace";
	}
}

function routeLabel(status: ExecutionCapabilityStatus | undefined): string {
	if (!status) return "unsupported";
	if (status.primary && status.state === "ready") return `ready · ${executionRouteKind(status.primary.isolation)}`;
	if (status.primary && status.state === "registered")
		return `available · ${executionRouteKind(status.primary.isolation)} (checked on first use)`;
	if (status.state === "registered") return "availability pending";
	const reasons = [...new Set(status.candidates.map((candidate) => candidate.detail))];
	return `unavailable · ${reasons.join("; ") || "no safe execution route"}`;
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
	worlds: readonly ExecutionWorldDiagnosticSnapshot[],
	conflicts: number,
): string {
	if (!settings.enabled) return "spec: off";
	const ready = worlds.filter((world) => world.state === "ready").length;
	const reuse = metrics.processReuse;
	const storageWorlds = worlds.filter((world) => world.storage);
	const storedEntries = storageWorlds.reduce((total, world) => total + (world.storage?.entries ?? 0), 0);
	const storedBytes = storageWorlds.reduce((total, world) => total + (world.storage?.bytes ?? 0), 0);
	return [
		"spec: on",
		`tools reused ${formatRatio(metrics.speculativeHits, metrics.actorActions)}`,
		...(reuse.requests > 0 ? [`Bash child ${formatBashReuse(reuse)}`] : []),
		metrics.tasks > 0 ? `${formatDuration(metrics.hiddenLatencyMs)} observed overlap` : "timing n/a",
		`live results ${metrics.cache.resultEntries}/${metrics.cache.cacheCapacity} (${formatBytes(metrics.cache.resultBytes)})`,
		...(storageWorlds.length ? [`reuse history ${storedEntries} entries (${formatBytes(storedBytes)})`] : []),
		worlds.length > 0 ? `routes ${ready}/${worlds.length} ready` : "routes probing",
		"unsafe→Actor",
		...(conflicts > 0 ? [`${conflicts} tool conflict${conflicts === 1 ? "" : "s"}`] : []),
	].join(" · ");
}

function executionWorldSummary(worlds: readonly ExecutionWorldDiagnosticSnapshot[]): string {
	if (!worlds.length) return "Execution routes: unavailable";
	return [
		"Execution routes:",
		...worlds.map(
			(world) =>
				`- ${executionRouteKind(world.isolation)} (${world.id}): ${world.state} — ${world.detail}${
					world.storage
						? `; storage ${world.storage.entries}/${world.storage.maxEntries}, ${formatBytes(world.storage.bytes)}/${formatBytes(world.storage.maxBytes)}, ${world.storage.orphanArtifacts ?? 0} orphan artifacts${world.storage.overBudget ? "; over budget" : ""}`
						: ""
				}`,
		),
	].join("\n");
}

function countSummary(counts: Readonly<Record<string, number>>): string {
	const entries = Object.entries(counts).sort(([leftKey, left], [rightKey, right]) =>
		right === left ? leftKey.localeCompare(rightKey) : right - left,
	);
	return entries.length > 0 ? entries.map(([key, count]) => `${key}=${count}`).join(", ") : "none";
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

function measuredReuseDelta(reuse: WorldReuseMetrics): number | undefined {
	if (reuse.timedHits <= 0 || reuse.avoidedProcessMs <= 0) return undefined;
	return reuse.avoidedProcessMs - reuse.timedHitOverheadMs;
}

function formatReuseDelta(reuse: WorldReuseMetrics): string | undefined {
	const delta = measuredReuseDelta(reuse);
	if (delta === undefined) return undefined;
	if (delta === 0) return "no estimated time change";
	return `~${formatDuration(Math.abs(delta))} estimated ${delta > 0 ? "time saved" : "extra time"} (${formatPercent(Math.abs(delta) / reuse.avoidedProcessMs)})`;
}

function formatBashReuse(reuse: WorldReuseMetrics): string {
	const benefit = formatReuseDelta(reuse);
	const origins = [
		reuse.sameTurnHits ? `${reuse.sameTurnHits} same-turn` : "",
		reuse.crossTurnHits ? `${reuse.crossTurnHits} earlier-turn` : "",
		reuse.unattributedHits ? `${reuse.unattributedHits} stored` : "",
		reuse.joinedHits ? `${reuse.joinedHits} joined` : "",
	].filter(Boolean);
	return [
		`${formatRatio(reuse.hits, reuse.requests)} reused`,
		...(benefit ? [benefit] : []),
		...origins,
	].join("; ");
}
