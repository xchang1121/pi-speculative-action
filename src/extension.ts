import { AsyncLocalStorage } from "node:async_hooks";
import { mkdir, realpath } from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { AgentMessage, AgentTool } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Model } from "@earendil-works/pi-ai";
import { convertToLlm, createLocalBashOperations, type ExtensionAPI, type ExtensionCommandContext, type ExtensionContext, type ExtensionFactory,
	type ExtensionUIContext, getAgentDir, getShellConfig, SettingsManager } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { ActorStreamPreviewTracker } from "./actor-stream-preview.ts";
import { createResourceSnapshotExecutionWorld, type AgentExecutionWorld } from "./agent-execution-world.ts";
import { createSpeculativeActionHost, normalizeSpeculativeAgentSettings, type ActionDrafterBudgetSnapshot, type CreateSpeculativeActionHostOptions } from "./agent-integration.ts";
import { DrafterTaskBudget } from "./drafter-budget.ts";
import { toolSpeedup } from "./task-timing.ts";
import { forceToolChoice } from "./drafter-plan-source.ts";
import { DEFAULTS } from "./common.ts";
import { PATTERN_AWARE_PRESETS, patternAwarePersistenceFile, type PatternAwareSettings } from "./pattern-aware.ts";
import { createClosedSearchProfile, createPiToolDefinitions, PI_CLOSED_SEARCH_TOOLS, resolvePiToolInvocation } from "./pi-tool-invocation.ts";
import { checkLinuxEnvironment, installLinuxDependencies } from "./linux-setup.ts";
import { emptyWorldReuseMetrics, executionCapabilityStatus, type ExecutionWorldDiagnosticSnapshot, type ExecutionWorldHealthState, type SpeculativeExecution,
	type WorldReuseMetrics } from "./execution-world.ts";
import { adaptProcessToolOperations, definedProcessEnvironment, ProcessExecutionCoordinator,
	type ProcessRouteSnapshot } from "./process-execution.ts";
import { DEFAULT_PROVENANCE_STORE_LIMITS, ProvenanceCertificateStore } from "./reuse-store.ts";
import type { SpeculativeActionEvent } from "./runtime.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { millisecondsInput, nonEmptyTextInput, nonNegativeIntegerInput, nonNegativeNumberInput, optionalTextInput,
	positiveIntegerInput, positiveInteger, positiveMilliseconds, probabilityInput, settingInput, type SettingInputDescriptor } from "./setting-input.ts";
import { errorMessage } from "./error-utils.ts";
import { asRecord } from "./stable-json.ts";
import { httpEndpoint, httpPath, SelfSpeculationCoordinator, type SelfSpeculationCoordinatorSnapshot, type SelfSpeculationSettings } from "./self-speculation.ts";
import type { SchedulingSettings } from "./scheduling-settings.ts";
import { type SpeculativeActionPackageSettings, SpeculativeActionSettingsStore, type SpeculativeSettingsScope } from "./settings-store.ts";
import { emptySpeculativeTraceSummary, reduceSpeculativeTrace, type SpeculativeTraceSummary } from "./trace-summary.ts";
import { resolvePatternWorkspaceIdentity } from "./workspace-identity.ts";
import { WorkspaceSandboxService } from "./workspace-sandbox.ts";
import { attachSdkTools, currentSdkTurn, isStockSdkTool, sdkCallbackTool, sdkToolInvocation } from "./sdk-tool-binding.ts";

const STATUS_KEY = "speculative-action";
const CLOSE = "Close";
const BACK = "Back";
const USE_ACTIVE_MODEL = "Use active model";
const CUSTOM_MODEL = "Custom model...";
const RECENT_EVENT_LIMIT = 50;

export type EffectiveSpeculativeActionSettings = ReturnType<typeof normalizeSpeculativeActionSettings>;

type SettingInputDescriptors<T, K extends keyof T> = { readonly [Field in K]: SettingInputDescriptor<T[Field]> };

const ROOT_SETTING_INPUTS = {
	candidateLimit: positiveIntegerInput("Candidate requests per Actor decision"),
	maxConcurrentActions: positiveIntegerInput("Speculative resource units per session"),
	resourceCacheMaxEntries: positiveIntegerInput("Live result entries"),
	resourceCacheMaxBytes: mebibyteInput("Live result memory"),
	executionStoreMaxEntries: positiveIntegerInput("Reusable command history entries"),
	executionStoreMaxBytes: mebibyteInput("Reusable command history memory"),
	predictionTimeoutMs: millisecondsInput("Prediction wait limit (ms)"),
	thinkThreadTimeoutMs: millisecondsInput("ThinkThread execution timeout (ms)"),
	drafterMaxTokens: positiveIntegerInput("Maximum Drafter output tokens"),
	drafterTaskMaxRequests: positiveIntegerInput("Maximum Drafter requests per task"),
	drafterTaskMaxTokens: positiveIntegerInput("Maximum Drafter input/output tokens per task"),
	drafterMaxDepth: nonNegativeIntegerInput("Drafter out-of-order follow-up batches"),
	drafterDeterministicCandidates: nonNegativeIntegerInput("Temperature-0 Drafter candidates"),
} satisfies Partial<SettingInputDescriptors<EffectiveSpeculativeActionSettings, keyof EffectiveSpeculativeActionSettings>>;

function protocolInput(title: string, normalize: (value: unknown, fallback: string) => string, error: string) {
	return settingInput(title, String, input => { const value = normalize(input, ""); return value ? { ok: true, value } : { ok: false, error }; });
}
const servicePathInput = (title: string) => protocolInput(title, httpPath, "Service path must start with one / and contain no whitespace or backslashes.");
const SELF_SPECULATION_INPUTS = {
	endpoint: protocolInput("Control service URL", httpEndpoint, "Endpoint must be an absolute HTTP(S) URL."),
	requestIDField: nonEmptyTextInput("Provider request ID field"),
	candidatePath: servicePathInput("Candidate registration path"), forkPath: servicePathInput("Actor probe path"),
	clearPath: servicePathInput("Clear request path"), capabilitiesPath: servicePathInput("Capabilities path"),
	forkActionMinConfidence: probabilityInput("Minimum tool-name confidence"),
	maxCandidates: positiveIntegerInput("Candidates sent per Actor decision"),
	maxDraftTokens: positiveIntegerInput("Draft-token limit per candidate"),
	actorProfile: nonEmptyTextInput("Actor tool-call Profile ('auto' to derive)"),
	draftFormat: nonEmptyTextInput("Target tool-call format"),
	draftBoundary: nonEmptyTextInput("Target tool-call boundary override ('auto' to derive)"),
	forkMaxTokens: positiveIntegerInput("Actor probe output-token limit"),
	timeoutMs: millisecondsInput("Control request timeout (ms)"),
	forkTemperature: nonNegativeNumberInput("Actor probe temperature"),
	forkMaxAttempts: positiveIntegerInput("Maximum probes per Actor decision"),
	forkRetryStreamUpdates: positiveIntegerInput("Stream updates between retries"),
	forkBoundaryStreamUpdates: positiveIntegerInput("Stream updates before sentence-boundary retry"),
	forkDecoder: nonEmptyTextInput("Forked tool-call decoder"),
	forkForcedPrefix: nonEmptyTextInput("Forced tool-call prefix override ('auto' to derive)"),
	apiKeyEnv: optionalTextInput("Authentication token environment variable name (not the token)"),
} satisfies Partial<SettingInputDescriptors<SelfSpeculationSettings, keyof SelfSpeculationSettings>>;

const SCHEDULING_INPUTS = {
	resourcePollIntervalMs: millisecondsInput("Resource sampling interval (ms)"),
	gpuPollIntervalMs: millisecondsInput("GPU sampling interval (ms)"),
	gpuProbeTimeoutMs: millisecondsInput("GPU query timeout (ms)"),
	failureThreshold: positiveIntegerInput("Failures before circuit opens"),
	failureRetryDecisions: positiveIntegerInput("Retry every N eligible Actor decisions"),
	heavyCpu: positiveIntegerInput("Process/tree CPU units"), lightCpu: positiveIntegerInput("File snapshot CPU units"),
	heavyMemoryBytes: mebibyteInput("Process/tree memory"), lightMemoryBytes: mebibyteInput("File snapshot memory"),
	heavyIo: probabilityInput("Process/tree I/O fraction"), lightIo: probabilityInput("File snapshot I/O fraction"),
} satisfies SettingInputDescriptors<SchedulingSettings, keyof SchedulingSettings>;

const PATTERN_SETTING_INPUTS = {
	maxContextLength: positiveIntegerInput("Previous actions used as context"),
	maxFutureGap: nonNegativeIntegerInput("Maximum skipped Actor decisions"),
	futureGapCoverage: probabilityInput("Early-prediction coverage (0-1)", { error: "Early-prediction coverage must be between 0 and 1." }),
	decayHalfLifeEvents: positiveIntegerInput("History half-life (events)", { error: "Pattern half-life must be a positive integer." }),
	minOccurrences: positiveIntegerInput("Uses required before learning a pattern"),
	maxPatterns: positiveIntegerInput("Stored pattern limit"),
	beamWidth: positiveIntegerInput("Alternatives retained per tool"),
	maxPredictionDepth: positiveIntegerInput("Maximum predicted tool steps"),
} satisfies Partial<SettingInputDescriptors<PatternAwareSettings, keyof PatternAwareSettings>>;

const DRAFTER_TEMPERATURE_INPUT = settingInput<readonly [number, number]>(
	"Drafter sampling temperature range",
	([lower, upper]) => `${formatNumber(lower)},${formatNumber(upper)}`,
	(input) => {
		const [lower, upper, ...extra] = input.split(",").map((item) => item.trim() ? Number(item.trim()) : NaN);
		return extra.length === 0 && Number.isFinite(lower) && Number.isFinite(upper) && lower >= 0 && upper >= lower
			? { ok: true, value: [lower, upper] as const }
			: { ok: false, error: "Drafter temperature range must be two non-negative comma-separated numbers in ascending order." };
	},
);

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
	/** Observes every Drafter response, including Actor probes, without receiving credentials. */
	readonly onDrafterResponse?: (message: AssistantMessage, requestSessionID?: string,
		request?: { readonly toolChoice?: unknown; readonly toolNames: readonly string[] }) => void;
	readonly onMetrics?: (metrics: SpeculativeActionMetrics, routes: ExecutionRoutesSnapshot) => void;
}

export interface SpeculativeActionExecutionWorldContext { readonly cwd: string; readonly autoResizeImages: boolean; readonly settings: () => EffectiveSpeculativeActionSettings; }

export function normalizeSpeculativeActionSettings(
	input: SpeculativeActionPackageSettings | undefined,
) {
	return {
		...normalizeSpeculativeAgentSettings(input),
		searchExecution: input?.searchExecution === "captured" ? "captured" : "native",
		thinkThreadTimeoutMs: positiveMilliseconds(input?.thinkThreadTimeoutMs, DEFAULTS.thinkThreadTimeoutMs),
		...(typeof input?.draftModel === "string" && input.draftModel.trim() ? { draftModel: input.draftModel.trim() } : {}),
		executionStoreMaxEntries: positiveInteger(input?.executionStoreMaxEntries, DEFAULT_PROVENANCE_STORE_LIMITS.maxCertificates),
		executionStoreMaxBytes: positiveInteger(input?.executionStoreMaxBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes),
		executionRouting: { primary: input?.executionRouting?.primary !== false, nativeFallback: input?.executionRouting?.nativeFallback !== false },
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
		`Speculative production: ${settings.enabled ? "On" : "Off"}; cache & reuse: ${settings.reuseEnabled ? "On" : "Off"}; observation and learning remain active`,
		`Model Drafter: ${settings.drafterEnabled ? "On" : "Off"}`,
		`Drafter model: ${settings.draftModel ?? "active model"}`,
		`Candidate requests per Actor decision: ${settings.candidateLimit}`,
		`Model Drafter policy: ${settings.drafterMaxDepth} out-of-order follow-up batches; ${settings.drafterMaxTokens} output tokens per request; ${settings.drafterTaskMaxRequests} requests / ${settings.drafterTaskMaxTokens} input+output tokens per task; ${settings.drafterDeterministicCandidates} temperature-0 candidates; sampling ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
		`Speculative resource units per session: ${settings.maxConcurrentActions}`,
		`Scheduler: sampling ${formatDuration(settings.scheduling.resourcePollIntervalMs)}; failure circuit ${settings.scheduling.failureThreshold} failures / ${settings.scheduling.failureRetryDecisions} eligible decisions`,
		`Storage policy: ${settings.resourceCacheMaxEntries} live results/${formatBytes(settings.resourceCacheMaxBytes)}; ${settings.executionStoreMaxEntries} reusable commands/${formatBytes(settings.executionStoreMaxBytes)}`,
		`Prediction wait limit: ${formatDuration(settings.predictionTimeoutMs)}`,
		`Pattern predictions: ${settings.patternAware.enabled ? "On" : "Off"}; learning active; follow-up steps: ${settings.patternAware.multiStepEnabled ? "On" : "Off"} (alternatives/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}, learn after ${settings.patternAware.minOccurrences}, gap ${settings.patternAware.maxFutureGap}, coverage ${formatPercent(settings.patternAware.futureGapCoverage)}, half-life ${settings.patternAware.decayHalfLifeEvents})`,
		`Prebuilt modes: ${settings.patternAware.presets.length}/${PATTERN_AWARE_PRESETS.length} selected${settings.patternAware.enabled ? "" : " (inactive)"}; ${PATTERN_AWARE_PRESETS.filter(preset => settings.patternAware.presets.includes(preset.id)).map(preset => preset.label).join(", ") || "None"}`,
		`Actor probe: ${self.enabled && self.forkEnabled ? `On (${self.forkTransport})` : "Off"}; target verification ${self.enabled ? "On" : "Off"}; early tool execution ${self.enabled && self.forkTransport !== "provider" && self.forkEnabled && self.forkActionEnabled ? self.forkTransport === "drafter" ? "On (Drafter)" : `On (tool-name confidence ≥${formatPercent(self.forkActionMinConfidence)})` : "Off"}; ${self.maxCandidates} candidates × ${self.maxDraftTokens} draft tokens; Actor Profile=${self.actorProfile}; ${self.draftFormat} (${syntaxSettingLabel(self.draftBoundary)} boundary); ${self.forkTransport === "sidecar" ? self.endpoint : FORK_TRANSPORT_LABELS[self.forkTransport]}`,
		`Execution routing: admitted providers ${settings.executionRouting.primary ? "On" : "Off"}; local file routes ${settings.executionRouting.nativeFallback ? "On" : "Off"}; Actor always available`,
		`Search execution: ${searchExecutionLabel(settings.searchExecution)}`,
		`Tool calls reused: ${formatRatio(metrics.speculativeHits, metrics.actorActions)}; ${metrics.exactReuseHits} exact, ${metrics.partialResultReuseHits} partial, ${metrics.inputReuseHits} inputs`,
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
		`Prediction Drafter tokens (input + output): ${metrics.totalDraftTokens}${metrics.hiddenComputeMs > 0 && !metrics.hiddenComputeIncomplete ? `; ${Math.round(metrics.totalDraftTokens * 1000 / metrics.hiddenComputeMs)} per second of tool computation hidden` : ""}`,
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
			providerRequest.getStore() === "drafter" ? event.payload : controller?.decorateActorPayload(event.payload),
		);

		pi.on("session_start", (_event, ctx) => sessions.run(async () => {
			await controller?.dispose();
			controller = await installController(ctx, pi, dependencies, providerRequest);
			controller.attachUI(ctx.ui);
		}));
		pi.on("context", async (event, ctx) => { actorStream.clear(); await controller?.startTurn(event.messages, ctx); });
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
		pi.on("message_end", (event) => { if (event.message.role === "assistant") controller?.finishActorOutput(); });
		pi.on("turn_end", async () => { await controller?.finishTurn(false); });
		pi.on("agent_settled", async () => { await controller?.finishTurn(true); });
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
	const settingsStore = dependencies.createSettingsStore?.(context.cwd) ?? new SpeculativeActionSettingsStore(context.cwd);
	await settingsStore.load(context.isProjectTrusted());
	let currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
	let currentMetrics: SpeculativeTraceSummary = emptySpeculativeTraceSummary({
		cacheCapacity: currentSettings.resourceCacheMaxEntries, cacheByteCapacity: currentSettings.resourceCacheMaxBytes,
	});
	const settings = () => currentSettings;
	const drafterBudget = new DrafterTaskBudget();
	const lifecycle = new RuntimeLifecycleLane();
	type SearchProfile = Awaited<ReturnType<typeof createClosedSearchProfile>>;
	type SearchRoute = { ready: Promise<SearchProfile | undefined>; profile?: SearchProfile };
	let search: SearchRoute | undefined;
	const closedSearchEnabled = () => !lifecycle.sealed && currentSettings.searchExecution !== "native";
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
	// Registry completion takes raw per-API options; Drafter options are simple (reasoning levels) like the Actor's.
	const completeDraft: CreateSpeculativeActionHostOptions["complete"] = (model, llmContext, options) => providerRequest.run("drafter", async () => {
		const registry = latestContext.modelRegistry, provider = registry.getProvider(model.provider), auth = await registry.getApiKeyAndHeaders(model);
		if (!provider || !auth.ok) throw new Error(auth.ok ? `Unknown provider: ${model.provider}` : auth.error);
		let request: Parameters<NonNullable<SpeculativeActionExtensionDependencies["onDrafterResponse"]>>[2];
		const message = await provider.streamSimple({ ...model, baseUrl: auth.baseUrl ?? model.baseUrl }, llmContext, { ...options, apiKey: options?.apiKey ?? auth.apiKey,
			onPayload: dependencies.onDrafterResponse ? async (payload, model) => {
				const next = await options?.onPayload?.(payload, model);
				try {
					const selected = asRecord(next === undefined ? payload : next);
					request = { ...(selected?.tool_choice === undefined ? {} : { toolChoice: selected.tool_choice }),
						toolNames: Array.isArray(selected?.tools) ? selected.tools.flatMap(tool => {
							const definition = asRecord(tool), name = asRecord(definition?.function)?.name ?? definition?.name;
							return typeof name === "string" ? [name] : [];
						}) : [] };
				} catch { /* Diagnostics cannot change the provider payload. */ }
				return next;
			} : options?.onPayload,
			headers: { ...auth.headers, ...options?.headers }, env: { ...auth.env, ...options?.env } }).result();
		try { dependencies.onDrafterResponse?.(message, options?.sessionId, request); } catch { /* Reporting cannot change inference. */ }
		return message;
	});
	const draftModelFor = (actorModel: Model<Api>) => {
		const reference = settings().draftModel, model = reference ? findExactModelReferenceMatch(reference, latestContext.modelRegistry.getAvailable()) : actorModel;
		if (!model && reference !== unavailableDraftModel) ui?.notify(`Drafter model ${reference} is unavailable; drafting with the active model.`, "warning");
		unavailableDraftModel = model ? undefined : reference;
		return model ?? actorModel;
	};
	const selfSpeculation = new SelfSpeculationCoordinator({
		settings: () => {
			const configured = settings().selfSpeculation;
			return settings().enabled ? configured : { ...configured, enabled: false };
		},
		...(dependencies.selfSpeculationFetch ? { fetch: dependencies.selfSpeculationFetch } : {}),
		// Without reasoning, the Drafter must answer with the calls the Actor's own reasoning is heading for.
		draftFork: async ({ model, context: actorContext, reasoning, content, signal, maxTokens, temperature }) => {
			const message = await drafterBudget.run({ model: draftModelFor(model), context: { ...actorContext, messages: [...actorContext.messages, { role: "user", timestamp: Date.now(),
				content: `The assistant has begun its next reply. Its reasoning so far:\n<reasoning>\n${reasoning}\n</reasoning>${content ? `\nIts reply so far:\n${content}` : ""}\nCall exactly the tool or tools it is about to call next, with the arguments it will use.` }] },
				options: { signal, maxTokens, temperature, onPayload: forceToolChoice(undefined) }, policy: settings(), complete: completeDraft });
			return message?.content.flatMap((item) => item.type === "toolCall" ? [{ tool: item.name, input: item.arguments }] : []) ?? [];
		},
	});
	const piToolSettings = loadPiToolSettings(context);
	const patternWorkspaceIdentity = await resolvePatternWorkspaceIdentity(context.cwd);
	const primaryExecutionWorlds = dependencies.createExecutionWorlds?.({
		cwd: context.cwd,
		autoResizeImages: piToolSettings.autoResizeImages,
		settings,
	}) ?? [];
	let storeRoot = path.join(getAgentDir(), "speculative-action", "process-reuse");
	try {
		await Promise.all([storeRoot, path.dirname(patternAwarePersistenceFile(patternWorkspaceIdentity, getAgentDir()))]
			.map(root => mkdir(root, { recursive: true, mode: 0o700 })));
		storeRoot = await realpath(storeRoot);
	}
	catch { /* An unavailable cache cannot prevent the original tools from running. */ }
	const inputStore = new ProvenanceCertificateStore(storeRoot, { reservedRoot: true });
	const rawProcessExecutor = adaptProcessToolOperations(createLocalBashOperations({ shellPath: getShellConfig(piToolSettings.shellPath).shell }));
	const processCoordinator = new ProcessExecutionCoordinator(rawProcessExecutor);
	const workspaceSandbox = dependencies.createWorkspaceSandboxService?.() ?? new WorkspaceSandboxService();
	const executionWorlds = [
		...new Set([
			...primaryExecutionWorlds,
			workspaceSandbox.createExecutionWorld(),
			{ ...createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { maxBytes: () => currentSettings.resourceCacheMaxBytes,
				persistentInputs: { store: inputStore, internalCache: true } }),
				storage: {
					configure: ({ maxEntries, maxBytes }: { maxEntries: number; maxBytes: number }) => inputStore.configure({ maxCertificates: maxEntries, maxBytes }),
					maintain: async (operation: "gc" | "clear") => {
						const result = await (operation === "gc" ? inputStore.gc() : inputStore.clear());
						return { removedEntries: result.removedCertificates + result.removedInputVersions, removedArtifacts: result.removedArtifacts, removedBytes: result.removedBytes };
					},
				}, dispose: () => inputStore.dispose() },
		]),
	];
	const primaryExecutionWorldIDs = new Set(primaryExecutionWorlds.map((world) => world.id));
	const speculativeExecutionWorldEnabled = (backend: string): boolean =>
		primaryExecutionWorldIDs.has(backend)
			? currentSettings.executionRouting.primary
			: currentSettings.executionRouting.nativeFallback;
	const configureExecutionStorage = () => {
		for (const world of executionWorlds)
			world.storage?.configure({ maxEntries: currentSettings.executionStoreMaxEntries, maxBytes: currentSettings.executionStoreMaxBytes });
	};
	configureExecutionStorage();
	let executionDiagnostics: readonly ExecutionWorldDiagnosticSnapshot[] = [];
	const executionRoutes = (): ExecutionRoutesSnapshot => ({
		worlds: executionDiagnostics, actorProcessReplay: { state: "unavailable", detail: "Native process reuse is outside the supported resource closure; Actor execution is unchanged" }, primaryIDs: primaryExecutionWorldIDs,
		searchDetail: !closedSearchEnabled() || (search && !search.profile) ? "Native Pi" : `${searchExecutionLabel(currentSettings.searchExecution)}; ${search?.profile ? [...search.profile.invocations.keys()].join(", ") + " prepared" : "not checked"}`,
	});
	const agentTools = new Map([...createPiToolDefinitions(context.cwd, {
		read: { autoResizeImages: piToolSettings.autoResizeImages },
		bash: {
			operations: processCoordinator.operations,
			shellPath: piToolSettings.shellPath,
			commandPrefix: piToolSettings.shellCommandPrefix,
		},
	})].map(([name, definition]): [string, AgentTool] => [name, { ...definition,
		execute: (callID, input, signal, onUpdate) => definition.execute(callID, input as never, signal, onUpdate as never, latestContext),
	}]));
	const selectTool = (tool: AgentTool) => (isStockSdkTool(tool) && agentTools.get(tool.name)) || sdkCallbackTool(tool);
	const toolCapabilities = () => resolveToolCapabilities(
		new Map(sdk.tools().map(selectTool).map(tool => [tool.name, agentTools.get(tool.name) === tool
			? (closedSearchEnabled() ? search?.profile?.invocations.get(tool.name)?.semantics : undefined)?.requirements ?? PI_ACTION_SEMANTICS.requirements(tool.name)
			: sdkToolInvocation(tool).semantics?.requirements])), executionRoutes(),
	);
	const runtimeSettings = () => currentSettings;
	const visibleMetrics = (): SpeculativeActionMetrics => ({ ...currentMetrics, actorProcessReuse: emptyWorldReuseMetrics() });
	function renderFooter(): void {
		if (!ui) return;
		ui.setStatus(STATUS_KEY, formatSpeculativeFooter(settings(), visibleMetrics(), executionRoutes()));
	}
	const host = (dependencies.createHost ?? createSpeculativeActionHost)(sessionID, {
		cwd: context.cwd,
		getSettings: runtimeSettings,
		complete: completeDraft,
		drafterBudget,
		draftModel: draftModelFor,
		preflight: ({ toolName }) =>
			latestContext.isProjectTrusted() && pi.getActiveTools().includes(toolName),
		resolveInvocation: async (tool, input, definition) => {
			if (!definition || agentTools.get(tool) !== definition) return definition && sdkToolInvocation(definition);
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
		patternStoreReservedRoot: true,
		patternWorkspaceIdentity,
		onEvent: (event) => {
			currentMetrics = reduceSpeculativeTrace(currentMetrics, event);
			recentEvents.push(formatSpeculativeActionEvent(event));
			if (recentEvents.length > RECENT_EVENT_LIMIT) recentEvents.splice(0, recentEvents.length - RECENT_EVENT_LIMIT);
			renderFooter();
		},
	});
	const disconnectProbe = host.runtime.subscribe({
		turn_started: ({ startInput, decisionSequence }) =>
			selfSpeculation.startTurn(startInput.turnID, startInput.actorModel, startInput.context, decisionSequence),
		candidate_materialized: candidate => selfSpeculation.addCandidate(candidate),
		actor_materialized: ({ action }) => selfSpeculation.observeActorAction(action),
		actor_settled: ({ settlement }) => selfSpeculation.observeActorSettlement(settlement),
		prediction_settled: feedback => selfSpeculation.observePredictionSettlement(feedback),
	});
	const refreshExecutionDiagnostics = (refresh = false): Promise<void> => lifecycle.admit(async () => {
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
		metrics: visibleMetrics,
		toolCapabilities,
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
				previous.searchExecution !== currentSettings.searchExecution ||
				previous.executionRouting.primary !== currentSettings.executionRouting.primary ||
				previous.executionRouting.nativeFallback !== currentSettings.executionRouting.nativeFallback,
			));
			await settingsStore.flush(); // Applied for this session either way; callers report whether it was saved.
		},
		attachUI: (nextUI: ExtensionUIContext) => { ui = nextUI; renderFooter(); },
		detachUI: () => { ui?.setStatus(STATUS_KEY, undefined); ui = undefined; },
		startTurn: async (messages: AgentMessage[], nextContext: ExtensionContext) => {
			latestContext = nextContext;
			const model = nextContext.model, turn = currentSdkTurn(nextContext);
			if (!model || !turn) return;
			try {
				if (currentTurnID) await host.finishTurn(currentTurnID);
				const turnID = currentTurnID = `turn_${++turnSequence}`;
				const tools = turnTools = turn.tools.map(selectTool);
				turn.execute = (tool, args) => {
					const [callID, input, signal] = args, selected = selectTool(tool);
					return host.execute({ turnID, id: callID, tool: tool.name, args: input,
						tools: tools.map(current => current.name === tool.name ? selected : current) }, signal, async operation =>
						operation.invocation?.authoritative ? (await operation.invocation.authoritative({
							callID, args: operation.input, signal: signal ?? new AbortController().signal,
						})).result : selected.execute(...args));
				};
				const actorContext = { systemPrompt: nextContext.getSystemPrompt(), messages: convertToLlm(messages), tools: [...turnTools] };
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
			void recoverSpeculation(() => host.previewActorCall({ turnID, id: callID, tool, args: input, tools: turnTools }, signal));
		},
		previewActorTool: (tool: string, signal?: AbortSignal) => {
			const turnID = currentTurnID;
			if (!turnID) return;
			void recoverSpeculation(() => host.previewActorTool({ turnID, tool }, signal));
		},
		decorateActorPayload: (payload: unknown) => selfSpeculation.decorateActorPayload(payload),
		observeActorOutput: (event: Parameters<SelfSpeculationCoordinator["observeActorOutput"]>[0]) => selfSpeculation.observeActorOutput(event),
		finishActorOutput: () => selfSpeculation.finishActorOutput(),
		selfSpeculationSnapshot: () => selfSpeculation.snapshot(),
		finishTurn: async (terminal = false) => {
			const turnID = currentTurnID ?? (terminal ? lastTurnID : undefined);
			if (!turnID) return;
			if (currentTurnID) { currentTurnID = undefined; lastTurnID = turnID; }
			await recoverSpeculation(() => host.finishTurn(turnID, terminal));
			if (terminal) selfSpeculation.reset();
			else selfSpeculation.endTurn();
			if (terminal) lastTurnID = undefined;
		},
		statusText: () => {
			const effective = settings();
			return [
				formatSpeculativeActionStatus({ settings: effective, metrics: visibleMetrics() }),
				formatDrafterBudgetStatus(host.drafterBudgetSnapshot()),
				formatSelfSpeculationStatus(selfSpeculation.snapshot()),
				executionWorldSummary(toolCapabilities(), executionRoutes()),
			].join("\n");
		},
		dispose: () => {
			sdk.dispose();
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
			return lifecycle.close(async () => {
				await lifecycle.drain();
				const routes = executionRoutes();
				return Promise.resolve().then(() => settingsStore.flush())
				.finally(() => host.dispose())
				.finally(disconnectProbe)
					.finally(() => workspaceSandbox.dispose())
				.finally(() => selfSpeculation.dispose())
				.finally(resetSearch).finally(() => {
					try { dependencies.onMetrics?.(visibleMetrics(), routes); } catch { /* Reporting cannot change cleanup. */ }
				});
			});
		},
	} as const;
	const sdk = attachSdkTools(context);
	await recoverSpeculation(() => refreshExecutionDiagnostics());
	return controller;
}

async function recoverSpeculation<T>(operation: () => Promise<T>): Promise<T | undefined> {
	try { return await operation(); } catch { return undefined; }
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
	if (context.model) { environment.PI_PROVIDER = context.model.provider; environment.PI_MODEL = context.model.id; }
	if (context.thinkingLevel) environment.PI_REASONING_LEVEL = context.thinkingLevel;
	return definedProcessEnvironment(environment);
}

function loadPiToolSettings(context: ExtensionContext) {
	const settings = SettingsManager.create(context.cwd, getAgentDir(), { projectTrusted: context.isProjectTrusted() });
	return {
		shellPath: settings.getShellPath(),
		shellCommandPrefix: settings.getShellCommandPrefix(),
		autoResizeImages: settings.getImageAutoResize(),
	};
}

async function runCommand(
	args: string,
	ctx: ExtensionCommandContext,
	controller: SpeculativeActionController | undefined,
): Promise<void> {
	if (!controller) { ctx.ui.notify("Speculative action runtime is unavailable.", "error"); return; }
	const command = args.trim().toLowerCase();
	if (command === "on" || command === "off") {
		const saved = await controller.setSettings({ ...controller.editableSettings(), enabled: command === "on" }).then(() => "", (error: unknown) => ` (not saved: ${errorMessage(error)})`);
		ctx.ui.notify(`Speculative production ${controller.settings().enabled ? "enabled" : "disabled"}${controller.settings().enabled === (command === "on") ? "" : " by the active project settings"}${saved}. Cache & reuse: ${controller.settings().reuseEnabled ? "On" : "Off"}; observation and learning remain active.`, saved ? "warning" : "info");
		return;
	}
	if (command === "reset") {
		await controller.setSettings(undefined);
		ctx.ui.notify("Active speculative action settings reset.", "info");
		return;
	}
	if (command === "events") { showRecentEvents(ctx, controller); return; }
	if (command === "status" || (command === "" && ctx.mode !== "tui")) {
		await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
		ctx.ui.notify(controller.statusText(), "info");
		return;
	}
	if (command) { ctx.ui.notify("Usage: /speculative-action [on|off|status|events|reset]", "warning"); return; }
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
	const reload = () => { applied = structuredClone(controller.editableSettings()); draft = structuredClone(applied); };
	while (true) {
		const dirty = !isDeepStrictEqual(draft, applied);
		const scope = controller.settingsScope() === "global" ? "All projects" : "This project";
		const actions = new Map<string, MenuAction>([
			[`Speculative production: ${draft.enabled ? "On" : "Off"}`, () => editor.setSettings({ ...draft, enabled: !draft.enabled })],
			[`Cache & reuse: ${draft.reuseEnabled ? "On" : "Off"}`, () => editor.setSettings({ ...draft, reuseEnabled: !draft.reuseEnabled })],
			[`Save settings to: ${scope}${isDeepStrictEqual(applied, controller.settings()) ? "" : " (this project overrides shared settings)"}`, async () => {
				const selected = await ctx.ui.select("Save settings to", ["All projects", "This project", BACK]);
				if ((selected === "All projects" || selected === "This project") &&
					(!dirty || await ctx.ui.confirm("Discard changes?", "Switch configuration scope without applying?"))) {
					controller.setSettingsScope(selected === "All projects" ? "global" : "project");
					reload();
				}
			}],
			[`Prediction sources › ${sourceSummary(draft)}`, () => openPredictionSources(ctx, editor)],
			["Tools & execution", () => openToolsAndExecution(ctx, editor, controller)],
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
			if (!await ctx.ui.confirm("Restore defaults?", "Restore tuning values while keeping the production, reuse and source switches?")) return;
			const defaults = normalizeSpeculativeActionSettings(undefined), { patternAware: pattern, selfSpeculation: probe } = draft;
			await editor.setSettings({ ...defaults, enabled: draft.enabled, reuseEnabled: draft.reuseEnabled, drafterEnabled: draft.drafterEnabled, tools: draft.tools, prepareObservedOperations: draft.prepareObservedOperations,
				patternAware: { ...defaults.patternAware, enabled: pattern.enabled, multiStepEnabled: pattern.multiStepEnabled, presets: pattern.presets },
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
	return runSettingsMenu(ctx, controller, "Prediction sources", settings => [
		[`Model Drafter › ${settings.drafterEnabled ? "On" : "Off"}, ${settings.draftModel ?? activeModelReference(ctx)}`, () => openDrafterSettings(ctx, controller)],
		[`Prediction tools › ${settings.tools ? settings.tools.join(", ") || "None" : "All available"}`, () => openPredictionTools(ctx, controller)],
		[`Actor probe › ${actorForkSummary(settings.selfSpeculation)}`, () => openActorForkSettings(ctx, controller)],
		[`Learned patterns › ${settings.patternAware.enabled ? "On" : "Off"}, ${settings.patternAware.multiStepEnabled ? "follow-up steps" : "next step only"}, ${settings.patternAware.presets.length}/${PATTERN_AWARE_PRESETS.length} prebuilt modes selected`, () => openPatternAwareSettings(ctx, controller)],
	]);
}

function openPredictionTools(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runSettingsMenu(ctx, controller, "Prediction tools", settings => {
		const available = [...controller.toolCapabilities().keys()], selected = settings.tools ?? available;
		return [
			["Use all available tools", () => { const { tools, ...all } = settings; return controller.setSettings(all); }],
			...available.map((tool): [string, MenuAction] => [`[${selected.includes(tool) ? "x" : " "}] ${tool}`, () =>
				controller.setSettings({ ...settings, tools: selected.includes(tool) ? selected.filter(name => name !== tool) : [...selected, tool] })]),
		];
	});
}

function openAdvancedSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runSettingsMenu(ctx, controller, "Advanced settings", settings => [
		[`Model Drafter tuning › ${settings.candidateLimit} requests, ${settings.drafterMaxDepth} out-of-order follow-up batches`, () => openDrafterSettings(ctx, controller, true)],
		[`Actor probe and target verification › ${settings.selfSpeculation.forkTransport}`, () => openActorForkSettings(ctx, controller, "advanced")],
		[`Learned-pattern tuning › ${settings.patternAware.maxPatterns} stored patterns`, () => openPatternAwareSettings(ctx, controller, "advanced")],
		[`Scheduling and storage › ${settings.maxConcurrentActions} resource units`, () => openSchedulingAndCache(ctx, controller)],
	]);
}

function openDrafterSettings(ctx: ExtensionContext, controller: SpeculativeActionController, advanced = false): Promise<void> {
	return runSettingsMenu(ctx, controller, advanced ? "Model Drafter advanced" : "Model Drafter", settings => {
		const { input, toggle } = settingActions(ctx, settings, ROOT_SETTING_INPUTS, controller.setSettings);
		return !advanced ? [
			toggle("drafterEnabled", "Enabled"),
			[`Model › ${settings.draftModel ?? activeModelReference(ctx)}`, () => editDraftModel(ctx, controller, settings)],
			input("candidateLimit", "Candidate requests per decision"),
			[`Advanced settings › sampling, out-of-order follow-up batches, cost control`, () => openDrafterSettings(ctx, controller, true)],
		] : [
			toggle("drafterPatternHints", "Show PatternAware's expected calls"),
			input("drafterMaxDepth", "Out-of-order follow-up batches"),
			input("drafterMaxTokens", "Maximum output tokens"),
			input("drafterTaskMaxRequests", "Requests per task"),
			input("drafterTaskMaxTokens", "Input/output tokens per task"),
			input("drafterDeterministicCandidates", "Temperature-0 candidates"),
			[`Sampling temperature: ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`, () => editDrafterTemperatureRange(ctx, controller, settings)],
		];
	});
}

type ActorForkMenu = "basic" | "advanced" | "integration" | "protocol" | "fork" | "target";

function openActorForkSettings(ctx: ExtensionContext, controller: SpeculativeActionController, menu: ActorForkMenu = "basic"): Promise<void> {
	const titles = { basic: "Actor probe", advanced: "Actor probe advanced", integration: "Integration and authentication",
		protocol: "Service protocol", fork: "Fork decoding", target: "Target verification" };
	return runSettingsMenu(ctx, controller, titles[menu], settings => {
		const self = settings.selfSpeculation, remote = self.forkTransport !== "drafter";
		const save = (selfSpeculation: SelfSpeculationSettings) => controller.setSettings({ ...settings, selfSpeculation });
		const { input, toggle } = settingActions(ctx, self, SELF_SPECULATION_INPUTS, save);
		const link = (label: string, page: ActorForkMenu): [string, MenuAction] => [label, () => openActorForkSettings(ctx, controller, page)];
		if (menu === "basic") {
			const active = self.enabled && self.forkEnabled;
			return [
				[`Actor probe prediction: ${active ? "On" : "Off"}`, () => save({ ...self, enabled: active ? self.enabled : true, forkEnabled: !active })],
				link("Advanced settings › integration, decoding, verification", "advanced"),
				...(self.forkTransport !== "provider" ? [toggle("forkActionEnabled", "Use forked calls for tool pre-execution")] : []),
				...(remote && (self.forkTransport === "provider" || self.forkActionEnabled) ? [input("forkActionMinConfidence", undefined, formatPercent)] : []),
			];
		}
		if (menu === "advanced") return [
			link(`Integration and authentication › ${FORK_TRANSPORT_LABELS[self.forkTransport]}`, "integration"),
			link(`Fork decoding › ${self.forkMaxTokens} tokens, ${self.forkMaxAttempts} probes`, "fork"),
			...(remote ? [link(`Target verification › ${self.maxCandidates} candidates × ${self.maxDraftTokens} tokens`, "target")] : []),
		];
		if (menu === "integration") {
			const actions = new Map<string, MenuAction>([[`Integration: ${FORK_TRANSPORT_LABELS[self.forkTransport]}`, async () => {
				const selected = await ctx.ui.select("Actor probe integration", [...Object.values(FORK_TRANSPORT_LABELS), BACK]);
				const transport = Object.entries(FORK_TRANSPORT_LABELS).find(([, label]) => label === selected)?.[0] as SelfSpeculationSettings["forkTransport"] | undefined;
				if (transport) await save({ ...self, forkTransport: transport });
			}]]);
			if (remote) {
				actions.set(...input("timeoutMs", "Request timeout", formatDuration));
				if (controller.settingsScope() === "global") {
					actions.set(...input("endpoint"));
					actions.set(...input("apiKeyEnv", "Authentication token variable", value => value ?? "None"));
				} else actions.set("Service URL and authentication › All projects only", () => ctx.ui.notify("Choose Save settings to → All projects in the main menu to edit the service URL and authentication.", "info"));
				actions.set(...link("Service protocol › request field, paths, probability evidence", "protocol"));
			}
			return actions;
		}
		if (menu === "protocol") return [...(["requestIDField", "candidatePath", "forkPath", "clearPath", "capabilitiesPath"] as const).map(key => input(key)), toggle("requireLogprobs", "Require token probabilities")];
		if (menu === "fork") return [
			input("forkMaxTokens", "Maximum output tokens"), input("forkTemperature", "Sampling temperature", formatNumber),
			...(["forkMaxAttempts", "forkRetryStreamUpdates", "forkBoundaryStreamUpdates"] as const).map(key => input(key)),
			...(remote ? [input("forkDecoder", "Tool-call decoder"), input("forkForcedPrefix", "Forced tool-call prefix", syntaxSettingLabel)] : []),
		];
		return [toggle("enabled", "Verify predicted calls during Actor decoding"), input("maxCandidates", "Candidates sent per decision"),
			input("maxDraftTokens"), input("actorProfile", "Actor Profile"), input("draftFormat", "Tool-call format override"), input("draftBoundary", "Tool-call boundary", syntaxSettingLabel)];
	});
}

function openPatternAwareSettings(ctx: ExtensionContext, controller: SpeculativeActionController, menu: "basic" | "presets" | "advanced" | "learning" | "multiStep" = "basic"): Promise<void> {
	const title = { basic: "Learned patterns", presets: "Prebuilt modes", advanced: "Learned-pattern advanced", learning: "Learning history", multiStep: "Multi-step search" }[menu];
	return runSettingsMenu(ctx, controller, title, settings => {
		const pattern = settings.patternAware;
		const { input, toggle } = settingActions(ctx, pattern, PATTERN_SETTING_INPUTS,
			(patternAware) => controller.setSettings({ ...settings, patternAware }));
		if (menu === "basic") return [
			toggle("enabled", "Pattern predictions (learning stays active)"),
			toggle("multiStepEnabled", "Predict follow-up tool steps"),
			[`Prebuilt modes › ${pattern.presets.length}/${PATTERN_AWARE_PRESETS.length} selected`, () => openPatternAwareSettings(ctx, controller, "presets")],
			[`Advanced settings › history, confidence, search limits`, () => openPatternAwareSettings(ctx, controller, "advanced")],
		];
		if (menu === "presets") return PATTERN_AWARE_PRESETS.map(preset => [
			`[${pattern.presets.includes(preset.id) ? "x" : " "}] ${preset.label} · ${preset.description}`,
			() => controller.setSettings({ ...settings, patternAware: { ...pattern, presets: PATTERN_AWARE_PRESETS
				.filter(candidate => candidate.id === preset.id ? !pattern.presets.includes(candidate.id) : pattern.presets.includes(candidate.id))
				.map(candidate => candidate.id) } }),
		]);
		if (menu === "advanced") {
			const actions = new Map<string, MenuAction>([
				[`Learning history › ${pattern.maxContextLength} previous actions`, () => openPatternAwareSettings(ctx, controller, "learning")],
			]);
			if (pattern.multiStepEnabled)
				actions.set(`Multi-step search › ${pattern.beamWidth} alternatives/tool, ${pattern.maxPredictionDepth} steps`, () => openPatternAwareSettings(ctx, controller, "multiStep"));
			return actions;
		}
		return menu === "learning" ? [
			input("maxContextLength"),
			input("maxFutureGap"),
			input("futureGapCoverage", "Early-prediction coverage", formatPercent),
			input("decayHalfLifeEvents", "History half-life", value => `${value} events`),
			input("minOccurrences", "Uses before learning a pattern"),
			input("maxPatterns"),
		] : [input("beamWidth"), input("maxPredictionDepth")];
	});
}

function openSchedulingAndCache(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	return runSettingsMenu(ctx, controller, "Scheduling and storage", settings => {
		const { input } = settingActions(ctx, settings, ROOT_SETTING_INPUTS, controller.setSettings);
		const actions = new Map<string, MenuAction>([
			input("maxConcurrentActions"),
			["Scheduler policy › sampling, failure recovery", () => openSchedulingPolicy(ctx, controller)],
			["Resource estimates › process/tree and file snapshots", () => openSchedulingPolicy(ctx, controller, true)],
			input("predictionTimeoutMs", "Prediction wait limit", formatDuration),
			input("thinkThreadTimeoutMs", "ThinkThread execution timeout", formatDuration),
			input("resourceCacheMaxEntries"),
			input("resourceCacheMaxBytes", "Live result memory", formatBytes),
			input("executionStoreMaxEntries"),
			input("executionStoreMaxBytes", "Reusable command history memory", formatBytes),
		]);
		for (const [label, operation] of [["Reclaim", "gc"], ["Clear", "clear"]] as const) actions.set(`${label} reusable command history`, async () => {
			if (operation === "clear" && !(await ctx.ui.confirm("Clear reusable command history?", "Delete all reusable command results and file effects? This cannot be undone."))) return;
			const report = await recoverSpeculation(() => controller.maintainExecutionStorage(operation));
			ctx.ui.notify(report?.text ?? "Reusable command history maintenance failed.", report && !report.failed ? "info" : "warning");
		});
		return actions;
	});
}

function openSchedulingPolicy(ctx: ExtensionContext, controller: SpeculativeActionController, resources = false): Promise<void> {
	return runSettingsMenu(ctx, controller, resources ? "Resource estimates" : "Scheduler policy", settings => {
		const policy = settings.scheduling;
		const { input } = settingActions(ctx, policy, SCHEDULING_INPUTS, scheduling => controller.setSettings({ ...settings, scheduling }));
		return (Object.keys(SCHEDULING_INPUTS) as (keyof SchedulingSettings)[])
			.filter(key => /^(heavy|light)/.test(key) === resources).map(key => input(key));
	});
}

type MenuAction = () => void | Promise<void>;

async function runSettingsMenu(ctx: ExtensionContext, controller: SpeculativeActionController, title: string,
	actionsForCurrentSettings: (settings: EffectiveSpeculativeActionSettings) => Iterable<readonly [string, MenuAction]>): Promise<void> {
	while (true) {
		const actions = new Map(actionsForCurrentSettings(controller.settings()));
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
	return runSettingsMenu(ctx, editor, "Tools & execution", settings => {
		const refresh = async () => {
			await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
			ctx.ui.notify(executionWorldSummary(controller.toolCapabilities(), controller.executionRoutes()), "info");
		};
		return [
			[`Prepare observed operations › ${settings.prepareObservedOperations ? "On" : "Off"}`, () => editor.setSettings({ ...settings, prepareObservedOperations: !settings.prepareObservedOperations })],
			["Execution routes", () => openExecutionRoutes(ctx, editor, controller)],
			["Check Linux environment", () => checkLinuxEnvironment(ctx, refresh)],
			["Install / update Linux dependencies", () => installLinuxDependencies(ctx, refresh)],
		];
	});
}

async function openExecutionRoutes(
	ctx: ExtensionContext,
	editor: SpeculativeActionController,
	controller: SpeculativeActionController,
): Promise<void> {
	await recoverSpeculation(() => controller.refreshExecutionDiagnostics(true));
	ctx.ui.notify(
		`Supported reuse covers bound file inputs, deterministic computations and qualified file transactions. Native processes and external runtime sandboxes execute through the Actor. The same boundary applies to prediction, result capture, internal observation and capability refresh; settings cannot widen it.\n${executionWorldSummary(controller.toolCapabilities(), controller.executionRoutes())}`,
		"info",
	);
	return runSettingsMenu(ctx, editor, "Execution routes", settings => {
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
		input<Key extends Field>(key: Key, label = descriptors[key].title, format: (value: T[Key]) => string | number = descriptors[key].format): [string, MenuAction] {
			return [`${label}: ${format(current[key])}`, () => promptSetting(ctx, current[key], descriptors[key], value => update(key, value))];
		},
		toggle(key: { [Key in keyof T]: T[Key] extends boolean ? Key : never }[keyof T], label: string): [string, MenuAction] {
			return [`${label}: ${current[key] ? "On" : "Off"}`, () => update(key, !current[key])];
		},
	};
}

function editDrafterTemperatureRange(ctx: ExtensionContext, controller: SpeculativeActionController, settings: EffectiveSpeculativeActionSettings): Promise<void> {
	return promptSetting(ctx, [settings.drafterTemperatureMin, settings.drafterTemperatureMax] as const, DRAFTER_TEMPERATURE_INPUT,
		([drafterTemperatureMin, drafterTemperatureMax]) => controller.setSettings({ ...settings, drafterTemperatureMin, drafterTemperatureMax }));
}

async function editDraftModel(ctx: ExtensionContext, controller: SpeculativeActionController, settings: EffectiveSpeculativeActionSettings): Promise<void> {
	const models = ctx.modelRegistry.getAvailable().sort((left, right) => `${left.provider}/${left.id}`.localeCompare(`${right.provider}/${right.id}`));
	const providers = new Map<string, typeof models>();
	for (const model of models) providers.set(model.provider, [...(providers.get(model.provider) ?? []), model]);
	const providerLabels = new Map([...providers].map(([provider, providerModels]) => [`${provider} (${providerModels.length} models) ›`, provider]));
	const active = `${USE_ACTIVE_MODEL} (${activeModelReference(ctx)})`;
	const choice = await ctx.ui.select("Drafter model", [active, ...providerLabels.keys(), CUSTOM_MODEL, BACK]);
	if (!choice || choice === BACK) return;
	const { draftModel: _previousDraftModel, ...baseSettings } = settings;
	if (choice === active) { await controller.setSettings(baseSettings); return; }
	if (choice === CUSTOM_MODEL) {
		const value = await ctx.ui.input("Custom drafter model", "provider/model");
		if (value === undefined) return;
		const draftModel = value.trim();
		await controller.setSettings({ ...baseSettings, ...(draftModel ? { draftModel } : {}) });
		return;
	}
	const provider = providerLabels.get(choice);
	if (!provider) return;
	const labels = new Map((providers.get(provider) ?? []).map(model => {
		const reference = `${model.provider}/${model.id}`;
		return [`${settings.draftModel === reference ? "[x] " : ""}${model.id}${model.name && model.name !== model.id ? ` — ${model.name}` : ""}`, reference];
	}));
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
			parts.push(event.request.request.source, event.request.settlement.status, formatDuration(event.request.durationMs));
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
			const { candidate, state } = event, { route } = candidate;
			parts.push(`candidate ${compactEventText(candidate.id)}`, candidate.tool, candidate.source,
				`${route.backend}/${executionRouteKind(route.isolation)}/${route.reuse}`, state.status);
			if (state.status === "running") parts.push(`${candidate.origin === "actor_preview" ? "previewed" : "predicted"} ${compactEventText(candidate.predictedAction)}`);
			else if (state.status === "succeeded") {
				parts.push(formatDuration(state.executionMs));
				const reuse = candidate.world?.executionMetrics.reuse;
				if (reuse && hasProcessReuse(reuse)) parts.push(`Bash branch work ${formatProcessWorkReuse(reuse)}`);
			} else parts.push(causeSummary(state.cause), formatDuration(state.executionMs));
			break;
		}
		case "actor_action": {
			const { provider, tool, matchedPredictions } = event.settlement, sources = [...new Set(matchedPredictions.map(prediction => prediction.source))];
			parts.push(tool, sources.join("+") || (provider.kind === "speculative" ? "cache" : "no prediction"));
			if (provider.kind === "speculative") {
				const { match } = provider;
				parts.push(match.kind === "projected" ? `partial-result reuse (${match.projector})` : match.kind === "inputs" ? "sealed-input reuse" : "exact-action reuse");
			} else parts.push(`${formatDuration(provider.durationMs)} Actor ${provider.origin} execution`);
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
	if (!settings.enabled) return "Predictions off";
	const sources = [settings.drafterEnabled && "Model Drafter", settings.selfSpeculation.enabled && settings.selfSpeculation.forkEnabled && "Actor probe",
		settings.patternAware.enabled && "Learned patterns"].filter(Boolean).join(" + ");
	return sources || "No source enabled";
}

function actorForkSummary(settings: SelfSpeculationSettings): string {
	if (!settings.enabled || !settings.forkEnabled) return "Off";
	return `On, ${FORK_TRANSPORT_LABELS[settings.forkTransport]}`;
}

function syntaxSettingLabel(value: string): string {
	return value === "auto" ? "automatic" : value;
}

function formatDrafterBudgetStatus(snapshot: ActionDrafterBudgetSnapshot): string {
	const { budget, observations } = snapshot;
	return `Action Drafter task budget: ${budget.requests} requests, ${budget.reportedTokens} reported + ${budget.unreportedTokens} estimated without usage + ${budget.reservedTokens} reserved tokens, ${budget.skippedRequests} requests skipped\nPrivate observations: ${observations.forecasted} forecasted, ${observations.matched}/${observations.compared} exact matches against completed speculative results`;
}

const FORK_TRANSPORT_LABELS: Readonly<Record<SelfSpeculationSettings["forkTransport"], string>> = { provider: "Provider-integrated", sidecar: "Sidecar service", drafter: "Drafter reads Actor reasoning" };

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

function resolveToolCapabilities(
	registered: ReadonlyMap<string, ReturnType<typeof PI_ACTION_SEMANTICS.requirements>>,
	routes: ExecutionRoutesSnapshot,
): ReadonlyMap<string, ToolCapabilityRow> {
	const { worlds, actorProcessReplay } = routes;
	return new Map<string, ToolCapabilityRow>(
		[...registered].map(([tool, requirements]): [string, ToolCapabilityRow] => {
			if (!requirements) {
				return [tool, { replay: "unavailable", observe: "unavailable", fork: "unavailable" }];
			}
			const fork = executionCapabilityStatus(requirements, worlds, "speculation", tool);
			const observe = executionCapabilityStatus(requirements, worlds, "observation", tool);
			const replay = bestCapabilityState([
				fork.state,
				observe.state,
				...(actorProcessReplay ? [processRouteCapability(actorProcessReplay.state)] : []),
			]);
			return [tool, { replay, observe: observe.state, fork: fork.state }];
		}),
	);
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
	return { disabled: "Disabled", idle: "Idle", probing: "Checking", ready: "Ready", unavailable: "Unavailable" }[state];
}

function executionRouteKind(isolation: SpeculativeExecution): string {
	switch (isolation) {
		case "runtime_sandbox": return "isolated runtime";
		case "resource_snapshot": return "sealed file inputs";
		case "workspace_branch": return "private workspace";
	}
}

function capabilityLabel(state: ExecutionWorldHealthState): string {
	switch (state) {
		case "ready": return "Ready";
		case "registered": return "Check";
		case "unavailable": return "Unavailable";
	}
}

function formatSpeculativeFooter(
	settings: EffectiveSpeculativeActionSettings,
	metrics: SpeculativeActionMetrics,
	routes: ExecutionRoutesSnapshot,
): string {
	const { worlds, actorProcessReplay } = routes;
	const providers = worlds.length + (actorProcessReplay ? 1 : 0);
	const ready = worlds.filter((world) => world.state === "ready" || world.observation?.state === "ready").length +
		(actorProcessReplay && processRouteCapability(actorProcessReplay.state) === "ready" ? 1 : 0);
	const reuse = metrics.actorProcessReuse;
	const storageWorlds = worlds.filter((world) => world.storage);
	const storedEntries = storageWorlds.reduce((total, world) => total + (world.storage?.entries ?? 0), 0);
	const storedBytes = storageWorlds.reduce((total, world) => total + (world.storage?.bytes ?? 0), 0);
	return [
		`spec: ${settings.enabled ? "on" : "off"}, reuse: ${settings.reuseEnabled ? "on" : "off"}`,
		metrics.tasks > 0 ? `${formatSpeedup(metrics)}; ${formatHiddenCompute(metrics)}` : "Tool SpeedUp n/a",
		`tools reused ${formatRatio(metrics.speculativeHits, metrics.actorActions)}`,
		...(hasProcessReuse(reuse) ? [`Bash Actor ${formatActorProcessFooter(reuse)}`] : []),
		`live results ${metrics.cache.resultEntries}/${metrics.cache.cacheCapacity} (${formatBytes(metrics.cache.resultBytes)})`,
		...(storageWorlds.length ? [`reuse history ${storedEntries} entries (${formatBytes(storedBytes)})`] : []),
		providers > 0 ? `providers ${ready}/${providers} ready` : "providers probing",
		"unsafe→Actor",
	].join(" · ");
}

function executionWorldSummary(tools: ReadonlyMap<string, ToolCapabilityRow>, routes: ExecutionRoutesSnapshot): string {
	const { worlds, actorProcessReplay } = routes;
	if (!worlds.length && !actorProcessReplay) return "Execution capabilities: unavailable";
	return [
		"Execution capabilities:",
		capabilityTable(tools),
		...(routes.searchDetail ? [`Search executor: ${routes.searchDetail}`] : []),
		"Providers:",
		...(actorProcessReplay
			? [`- Native child reuse: ${processRouteLabel(actorProcessReplay.state)} — ${actorProcessReplay.detail}`]
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
		capabilityLabel(capability.replay),
		capabilityLabel(capability.observe),
		capabilityLabel(capability.fork),
	]);
	const table = [["Tool", "Replay", "Observe", "Fork"], ...rows];
	const widths = table[0]!.map((_, column) => Math.max(...table.map((row) => row[column]!.length)));
	return table.map((row) => row.map((cell, column) => cell.padEnd(widths[column]!)).join("  ").trimEnd()).join("\n");
}

function tallySummary(counts: Readonly<Record<string, readonly number[]>>, format: (counts: readonly number[]) => string): string {
	return Object.entries(counts).map(([key, value]) => `${key} ${format(value)}`).join("; ") || "none";
}

function modeSummary(sources: SpeculativeTraceSummary["modesBySource"]): string {
	return Object.entries(sources).flatMap(([source, modes]) => Object.entries(modes).map(([mode, result]) => {
		const label = source === "pattern_aware" ? PATTERN_AWARE_PRESETS.find(preset => preset.id === mode)?.label ?? mode : `${source}/${mode}`;
		return `${label}: ${result.matched}/${result.observed} matched, ${result.adopted} adopted; ${formatDuration(result.hiddenComputeMs)} hidden, ${formatDuration(result.productionMs)} production wall (${result.started} started)`;
	})).join("; ") || "none";
}

function countSummary(counts: Readonly<Record<string, number>>): string {
	const entries = Object.entries(counts).sort(([leftKey, left], [rightKey, right]) =>
		right === left ? leftKey.localeCompare(rightKey) : right - left,
	);
	return entries.length > 0 ? entries.map(([key, count]) => `${key}=${count}`).join(", ") : "none";
}

type TimingSummary = Pick<SpeculativeTraceSummary, "toolComputeMs" | "hiddenComputeMs"> & { readonly hiddenComputeIncomplete?: true; readonly adoptionWaitMs?: number };

function formatHiddenCompute(timing: TimingSummary): string {
	return `${formatDuration(timing.hiddenComputeMs)} tool computation hidden${timing.hiddenComputeIncomplete ? " (known lower bound)" : ""}`;
}

function formatTaskTiming(timing: TimingSummary): string {
	return `${formatSpeedup(timing)}; ${formatHiddenCompute(timing)}; ` +
		`${formatDuration(timing.toolComputeMs ?? NaN)} total tool computation` +
		(timing.adoptionWaitMs === undefined ? "" : `; ${formatDuration(timing.adoptionWaitMs)} adoption/control wait`);
}

function formatSpeedup(timing: TimingSummary): string {
	const ratio = toolSpeedup(timing);
	return `Tool SpeedUp ${ratio === null ? !timing.hiddenComputeIncomplete && Number.isFinite(timing.toolComputeMs) && timing.toolComputeMs === timing.hiddenComputeMs && timing.hiddenComputeMs > 0 ? "fully hidden" : "n/a" : `${ratio.toFixed(2)}x`}`;
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
	return [`${formatRatio(hits, requests)} reused`, workMs > 0 ? `${formatDuration(workMs)} work` : ""].filter(Boolean).join(" · ");
}
