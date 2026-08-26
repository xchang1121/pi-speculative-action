import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
import path from "node:path";
import type { AgentMessage, AgentTool, AgentToolResult, AgentToolUpdateCallback } from "@earendil-works/pi-agent-core";
import type { Api, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import {
	convertToLlm,
	createBashToolDefinition,
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
	buildPiActionKey,
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
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
import { PATTERN_AWARE_DEFAULTS, type PatternAwareSettings, patternAwareSettings } from "./pattern-aware.ts";
import { PI_BASH_TAIL_LINES_PROJECTION_RULE } from "./pi-bash-projection.ts";
import {
	canPreviewIncompletePiCall,
	PI_READ_RANGE_PROJECTION_RULE,
	withPiProjectionCoverage,
} from "./pi-read-projection.ts";
import { resolvePiToolInvocation } from "./pi-tool-invocation.ts";
import type { SpeculativeActionEvent } from "./runtime.ts";
import { SelfSpeculationActionBridge } from "./self-speculation-action-bridge.ts";
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
import type { ToolSettlement } from "./tool-settlement.ts";
import { emptySpeculativeTraceSummary, reduceSpeculativeTrace, type SpeculativeTraceSummary } from "./trace-summary.ts";
import { resolvePatternWorkspaceIdentity } from "./workspace-identity.ts";
import { createWorkspaceSandbox } from "./workspace-sandbox.ts";

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
	readonly predictionTimeoutMs: number;
	readonly patternAware: PatternAwareSettings;
	readonly selfSpeculation: SelfSpeculationSettings;
	readonly tools: readonly string[];
}

export type SpeculativeActionMetrics = SpeculativeTraceSummary;

export interface SpeculativeSettingsStore {
	readonly scope: SpeculativeSettingsScope;
	readonly load: () => Promise<void>;
	readonly effective: () => SpeculativeActionPackageSettings | undefined;
	readonly overlay: () => Readonly<Record<string, unknown>> | undefined;
	readonly setEffective: (settings: SpeculativeActionPackageSettings) => void;
	readonly clear: () => void;
	readonly setScope: (scope: SpeculativeSettingsScope) => void;
	readonly flush: () => Promise<void>;
}

interface SpeculativeActionController {
	readonly settings: () => EffectiveSpeculativeActionSettings;
	readonly settingsScope: () => SpeculativeSettingsScope;
	readonly setSettingsScope: (scope: SpeculativeSettingsScope) => void;
	readonly metrics: () => SpeculativeActionMetrics;
	readonly registeredTools: () => ReadonlySet<string>;
	readonly toolConflicts: () => ReadonlyMap<string, string>;
	readonly recentEvents: () => readonly string[];
	readonly executionSummary: () => string;
	readonly setSettings: (settings: SpeculativeActionPackageSettings | undefined) => void;
	readonly attachUI: (ui: ExtensionUIContext) => void;
	readonly detachUI: () => void;
	readonly startTurn: (messages: AgentMessage[], context: ExtensionContext) => Promise<void>;
	readonly previewActorTool: (tool: string, signal?: AbortSignal) => void;
	readonly previewActorCall: (tool: string, callID: string, input: unknown, signal?: AbortSignal) => void;
	readonly decorateActorPayload: (payload: unknown) => unknown;
	readonly decorateDrafterPayload: (payload: unknown) => unknown;
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
	const hitRate = Math.round(metrics.hitRate * 100);
	const cache = metrics.cache;
	return [
		`Enabled: ${settings.enabled ? "On" : "Off"}`,
		`Drafter: ${settings.drafterEnabled ? "On" : "Off"}`,
		`Draft model: ${settings.draftModel ?? "active model"}`,
		`Drafter requests: ${settings.candidateLimit}`,
		`Drafter request policy: rollout depth ${settings.drafterMaxDepth}; ${settings.drafterMaxTokens ?? "provider default"} tokens; ${settings.drafterDeterministicCandidates} deterministic; temperature ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
		`Concurrent actions: ${settings.maxConcurrentActions}`,
		`Resource cache: ${settings.resourceCacheMaxEntries}`,
		`Resource cache memory: ${formatBytes(settings.resourceCacheMaxBytes)}`,
		`Prediction timeout: ${formatDuration(settings.predictionTimeoutMs)}`,
		`PatternAware: ${settings.patternAware.enabled ? "On" : "Off"}; multi-step: ${settings.patternAware.multiStepEnabled ? "On" : "Off"} (beam/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}, promotion ${settings.patternAware.minOccurrences}, binding≥${settings.patternAware.minBindingReplayProbability}, gap ${settings.patternAware.maxFutureGap}, coverage ${formatPercent(settings.patternAware.futureGapCoverage)}, half-life ${settings.patternAware.decayHalfLifeEvents})`,
		`Self-speculation: ${settings.selfSpeculation.enabled ? "On" : "Off"}; ${settings.selfSpeculation.forkTransport} fork ${settings.selfSpeculation.forkEnabled ? "On" : "Off"}; sidecar action source ${settings.selfSpeculation.enabled && settings.selfSpeculation.forkTransport === "sidecar" && settings.selfSpeculation.forkEnabled && settings.selfSpeculation.forkActionEnabled ? `On (confidence ≥${formatNumber(settings.selfSpeculation.forkActionMinConfidence)})` : "Off"}; Drafter provider self-fork ${settings.selfSpeculation.forkTransport === "provider" && settings.selfSpeculation.forkEnabled && settings.selfSpeculation.drafterEnabled ? "On" : "Off"}; fork gate ${settings.selfSpeculation.forkGateEnabled ? `On (${settings.selfSpeculation.forkGateWindowSize} samples, ≥${formatDuration(settings.selfSpeculation.forkGateMinNetBenefitMs)} net)` : "Off"}; ${settings.selfSpeculation.maxCandidates} candidates × ${settings.selfSpeculation.maxDraftTokens} draft tokens; ${settings.selfSpeculation.draftFormat} at ${settings.selfSpeculation.draftBoundary}; ${settings.selfSpeculation.endpoint}`,
		`Prediction tools: ${toolsSummary(settings.tools)}`,
		"Execution boundary: runtime sandbox first; resource snapshots or Git worktrees second; otherwise Actor fallback",
		`Actor actions: ${metrics.speculativeHits}/${metrics.actorActions} speculative hits (${hitRate}%); previews: ${metrics.actorPreviews}; fallbacks: ${metrics.actorFallbacks}`,
		`Predictions: ${metrics.predictionsMatched}/${metrics.predictionsObserved} matched (${formatPercent(metrics.predictionPrecision)}); ${metrics.predictionsAdopted}/${metrics.predictionsMatched} adopted (${formatPercent(metrics.adoptionYield)}); unobserved: ${metrics.predictionsSettled - metrics.predictionsObserved}`,
		`Prediction rejections after match: ${countSummary(metrics.predictionRejectedAfterMatch)}`,
		`Actor candidate rejections: ${countSummary(metrics.actorCandidateRejections)}`,
		`Candidates: ${metrics.candidateStarted} started; ${metrics.candidateSucceeded} succeeded; ${metrics.candidateFailed} failed; ${metrics.candidateCancelled} cancelled`,
		`Task timing: ${formatDuration(metrics.endToEndMs)} actual; ${formatDuration(metrics.serializedMs)} serialized; ${formatDuration(metrics.hiddenLatencyMs)} serialized overlap; ${formatDuration(metrics.nonToolMs)} non-tool; ${formatDuration(metrics.toolExecutionMs)} tools`,
		`Execution ahead: ${formatDuration(metrics.executionAheadMs)}; hit latency: ${formatDuration(metrics.hitLatencyMs)}; attempt lead: ${formatDuration(metrics.attemptLeadMs)}; Actor execution: ${formatDuration(metrics.actorExecutionMs)}`,
		`Isolation-blocked potential: ${metrics.executionBlockedActorActions} Actor actions; ${formatDuration(metrics.executionBlockedPotentialHiddenLatencyMs)} could be hidden; ${formatDuration(metrics.executionBlockedPotentialHitLatencyMs)} would remain; ${formatDuration(metrics.executionBlockedAttemptLeadMs)} attempt lead`,
		`Draft tokens: ${metrics.totalDraftTokens}`,
		`Results: ${cache.resultEntries}/${cache.cacheCapacity}, ${formatBytes(cache.resultBytes)}/${formatBytes(cache.cacheByteCapacity ?? 0)}; cold: ${cache.cacheCold}; hot: ${cache.cacheHot}; jobs: ${cache.inFlightJobs}; branches: ${cache.branchEntries} (${formatBytes(cache.branchBytes)})`,
	].join("\n");
}

export function createSpeculativeActionExtension(
	dependencies: SpeculativeActionExtensionDependencies = {},
): ExtensionFactory {
	return (pi) => {
		let controller: SpeculativeActionController | undefined;
		const wrapperSources = new Map<string, string>();
		const actorStream = new ActorStreamPreviewTracker(canPreviewIncompletePiCall);
		const providerRole = new AsyncLocalStorage<"drafter">();

		pi.on("before_provider_request", (event) =>
			providerRole.getStore() === "drafter"
				? controller?.decorateDrafterPayload(event.payload)
				: controller?.decorateActorPayload(event.payload),
		);

		pi.on("session_start", async (_event, ctx) => {
			await controller?.dispose();
			controller = await installController(ctx, pi, dependencies, wrapperSources, providerRole);
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
	providerRole: AsyncLocalStorage<"drafter">,
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
	const selfSpeculationActions = new SelfSpeculationActionBridge();
	const selfSpeculation = new SelfSpeculationCoordinator({
		settings: () => {
			const configured = settings().selfSpeculation;
			return settings().enabled ? configured : { ...configured, enabled: false };
		},
		...(dependencies.selfSpeculationFetch ? { fetch: dependencies.selfSpeculationFetch } : {}),
		actionKey: (tool, input) => buildPiActionKey(tool, input, context.cwd)?.key,
		actionBridge: selfSpeculationActions,
	});
	const executionWorlds = [...new Set(dependencies.createExecutionWorlds?.() ?? [createWorkspaceSandbox()])];
	const [piToolSettings, patternWorkspaceIdentity] = await Promise.all([
		loadPiToolSettings(context.cwd),
		resolvePatternWorkspaceIdentity(context.cwd),
	]);
	const availableTools = new Map(pi.getAllTools().map((tool) => [tool.name, tool]));
	const toolConflicts = new Map<string, string>();
	// Pi exposes metadata, but not another extension's execute function. Only stock tools and our own
	// wrappers can be intercepted without silently substituting different tool semantics.
	const baseDefinitions = new Map(
		[...createBaseToolDefinitions(context.cwd, piToolSettings)].filter(([name]) => {
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
	function renderFooter(): void {
		if (!ui) return;
		const effective = settings();
		if (!effective.enabled) {
			ui.setStatus(STATUS_KEY, "spec: off");
			return;
		}
		const conflictText =
			toolConflicts.size > 0 ? ` · ${toolConflicts.size} tool conflict${toolConflicts.size === 1 ? "" : "s"}` : "";
		ui.setStatus(
			STATUS_KEY,
			`spec: on · unsafe routes fall back${conflictText} · ${currentMetrics.speculativeHits}/${currentMetrics.actorActions} adopted · ${currentMetrics.predictionsMatched}/${currentMetrics.predictionsObserved} predictions matched · ${formatDuration(currentMetrics.hiddenLatencyMs)} serialized overlap (${formatDuration(currentMetrics.endToEndMs)}/${formatDuration(currentMetrics.serializedMs)}) · ${currentMetrics.cache.resultEntries}/${currentMetrics.cache.cacheCapacity} results (${formatBytes(currentMetrics.cache.resultBytes)}) · ${currentMetrics.cache.inFlightJobs} jobs · ${currentMetrics.cache.branchEntries} branches`,
		);
	}
	const host = (dependencies.createHost ?? createSpeculativeActionHost)(context.sessionManager.getSessionId(), {
		cwd: context.cwd,
		getSettings: settings,
		complete: (model, llmContext, options) =>
			providerRole.run("drafter", () => latestContext.modelRegistry.complete(model, llmContext, options)),
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
		selfSpeculationActionBridge: selfSpeculationActions,
		patternStateDirectory: getAgentDir(),
		patternWorkspaceIdentity,
		onTurnStarted: ({ turnID, actorModel, context: actorContext, decisionSequence }) =>
			selfSpeculation.startTurn(turnID, actorModel, actorContext, decisionSequence),
		onCandidateMaterialized: (candidate) => selfSpeculation.addCandidate(candidate),
		onEvent: (event) => {
			currentMetrics = reduceSpeculativeTrace(currentMetrics, event);
			recentEvents.push(formatSpeculativeActionEvent(event));
			if (recentEvents.length > RECENT_EVENT_LIMIT) recentEvents.splice(0, recentEvents.length - RECENT_EVENT_LIMIT);
			renderFooter();
		},
	});

	const controller: SpeculativeActionController = {
		settings,
		settingsScope: () => settingsStore.scope,
		setSettingsScope: (scope) => settingsStore.setScope(scope),
		metrics: () => currentMetrics,
		registeredTools: () => new Set(baseDefinitions.keys()),
		toolConflicts: () => new Map(toolConflicts),
		recentEvents: () => [...recentEvents],
		executionSummary: () => executionWorldSummary(executionWorlds),
		setSettings: (value) => {
			if (value) settingsStore.setEffective(value);
			else settingsStore.clear();
			currentSettings = normalizeSpeculativeActionSettings(settingsStore.effective());
			if (!currentSettings.enabled || !currentSettings.selfSpeculation.enabled) selfSpeculation.reset();
			void recoverSpeculation(() => host.runtime.settingsChanged(currentSettings));
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
		decorateDrafterPayload: (payload) => selfSpeculation.decorateDrafterPayload(payload),
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
			selfSpeculation.observeActorAction(tool, input);
			const definition = baseDefinitions.get(tool);
			if (!definition) throw new Error(`Speculative wrapper has no base tool ${tool}`);
			const turnID = currentTurnID;
			if (turnID) {
				const cached = await recoverSpeculation(() =>
					host.consume({ turnID, id: callID, tool, args: input, tools: turnTools }, signal),
				);
				if (cached) return cached.result;
			}
			const startedAt = performance.now();
			try {
				const result = withPiProjectionCoverage(
					tool,
					input,
					await definition.execute(callID, input as never, signal, onUpdate as never, nextContext),
				);
				if (turnID) {
					await recoverSpeculation(() =>
						host.actual({
							turnID,
							id: callID,
							tool,
							args: input,
							tools: turnTools,
							durationMs: performance.now() - startedAt,
							output: { result, isError: false },
						}),
					);
				}
				return result;
			} catch (error) {
				if (turnID) {
					await recoverSpeculation(() =>
						host.actual({
							turnID,
							id: callID,
							tool,
							args: input,
							tools: turnTools,
							durationMs: performance.now() - startedAt,
							output: errorToolSettlement(error),
						}),
					);
				}
				throw error;
			}
		},
		statusText: () => {
			const effective = settings();
			const bridge = selfSpeculation.snapshot();
			const drafterGate = host.drafterGateSnapshot();
			return `${formatSpeculativeActionStatus({ settings: effective, metrics: currentMetrics })}\nAction Drafter gate: ${effective.drafterGateEnabled ? "On" : "Off"}; ${drafterGate.skippedBatches} batches skipped, ${drafterGate.samples} samples${drafterGate.expectedNetBenefitMs === undefined ? "" : `, ${formatDuration(drafterGate.expectedNetBenefitMs)} expected net`}\nSelf-speculation bridge: ${bridge.bufferedCandidates} buffered; ${bridge.candidateSubmissions} bundles/${bridge.candidateReceipts} receipts; ${bridge.forkRequests}/${bridge.forkCompletions} forks completed, ${bridge.forkGateSkips} gated${bridge.forkGateExpectedNetBenefitMs === undefined ? "" : ` at ${formatDuration(bridge.forkGateExpectedNetBenefitMs)} expected net`}; ${bridge.forkCandidates} fork candidates (${bridge.forkAgreements} source agreements, ${bridge.forkExactMatches} exact Actor matches); ${bridge.submittedDraftTokens} draft tokens registered (${bridge.acceptedDraftTokens} acknowledged); ${bridge.verifiedAcceptedDraftTokens}/${bridge.verifiedDraftTokens} target-verified accepted, ${bridge.verifiedRejectedDraftTokens} rejected, ${bridge.unresolvedDraftTokens} unresolved; ${formatDuration(bridge.forkLatencyMs)} fork latency${bridge.forkMeanLogprob === undefined ? "" : `; mean logprob ${formatNumber(bridge.forkMeanLogprob)}`}; ${bridge.failures} failures${bridge.lastError ? `; last error: ${bridge.lastError}` : ""}\n${executionWorldSummary(executionWorlds)}\nCustom tool conflicts: ${toolConflictSummary(toolConflicts)}`;
		},
		dispose: async () => {
			ui?.setStatus(STATUS_KEY, undefined);
			ui = undefined;
			await settingsStore.flush();
			try {
				await host.dispose();
			} finally {
				await selfSpeculation.dispose();
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

function createBaseToolDefinitions(cwd: string, settings: PiToolSettings): Map<string, BaseToolDefinition> {
	return new Map<string, BaseToolDefinition>([
		["read", createReadToolDefinition(cwd, { autoResizeImages: settings.autoResizeImages })],
		[
			"bash",
			createBashToolDefinition(cwd, {
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

function errorToolSettlement(error: unknown): ToolSettlement {
	const message = error instanceof Error ? error.message : String(error);
	return {
		result: { content: [{ type: "text", text: message }], details: {} },
		isError: true,
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
	let applied = cloneSettings(controller.settings());
	let draft = cloneSettings(applied);
	const editor: SpeculativeActionController = {
		...controller,
		settings: () => draft,
		setSettings: (value) => {
			draft = cloneSettings(normalizeSpeculativeActionSettings(value));
		},
	};
	const reload = () => {
		applied = cloneSettings(controller.settings());
		draft = cloneSettings(applied);
	};
	while (true) {
		const dirty = !sameSettings(draft, applied);
		const choice = await ctx.ui.select("Speculative action", [
			`Enabled: ${applied.enabled ? "On" : "Off"}`,
			`Configuration scope: ${controller.settingsScope()}`,
			`Prediction sources › ${sourceSummary(draft)}`,
			`Target decoding › ${selfSpeculationSummary(draft.selfSpeculation)}`,
			`Scheduling & cache › ${draft.candidateLimit} draft requests, ${draft.maxConcurrentActions} concurrent, ${draft.resourceCacheMaxEntries} entries`,
			`Tools & execution › ${enabledToolCount(draft)} tools`,
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
			const enabled = !applied.enabled;
			controller.setSettings({ ...applied, enabled });
			applied = cloneSettings(controller.settings());
			draft = { ...draft, enabled };
			continue;
		}
		if (choice.startsWith("Configuration scope:")) {
			const selected = await ctx.ui.select("Configuration scope", ["global", "project", BACK]);
			if (selected === "global" || selected === "project") controller.setSettingsScope(selected);
			continue;
		}
		if (choice.startsWith("Prediction sources")) {
			await openPredictionSources(ctx, editor);
			continue;
		}
		if (choice.startsWith("Target decoding")) {
			await openSelfSpeculationSettings(ctx, editor);
			continue;
		}
		if (choice.startsWith("Scheduling & cache")) {
			await openSchedulingAndCache(ctx, editor);
			continue;
		}
		if (choice.startsWith("Tools & execution")) {
			await openToolsAndExecution(ctx, editor, controller);
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
					"Restore tunable settings while preserving enabled sources and target decoding?",
				))
			)
				continue;
			const defaults = normalizeSpeculativeActionSettings(undefined);
			controller.setSettings({
				...defaults,
				enabled: applied.enabled,
				drafterEnabled: applied.drafterEnabled,
				patternAware: { ...defaults.patternAware, enabled: applied.patternAware.enabled },
				selfSpeculation: {
					...defaults.selfSpeculation,
					enabled: applied.selfSpeculation.enabled,
				},
			});
			reload();
			ctx.ui.notify("Speculative-action defaults restored.", "info");
		}
	}
}

async function openPredictionSources(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const choice = await ctx.ui.select("Prediction sources", [
			`Drafter › ${settings.drafterEnabled ? "On" : "Off"}, ${settings.draftModel ?? activeModelReference(ctx)}, rollout ${settings.drafterMaxDepth}`,
			`PatternAware › ${settings.patternAware.enabled ? "On" : "Off"}, ${settings.patternAware.multiStepEnabled ? "multi-step" : "single-step"}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Drafter")) await openDrafterSettings(ctx, controller);
		if (choice.startsWith("PatternAware")) await openPatternAwareSettings(ctx, controller);
	}
}

async function openSelfSpeculationSettings(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const self = settings.selfSpeculation;
		const choice = await ctx.ui.select("Self-speculation", [
			`Enabled: ${self.enabled ? "On" : "Off"}`,
			`Endpoint: ${self.endpoint}`,
			`Fork transport: ${self.forkTransport}`,
			`Actor fork: ${self.forkEnabled ? "On" : "Off"}`,
			`Fork action source: ${self.forkActionEnabled ? "On" : "Off"}`,
			`Fork action confidence: ${formatNumber(self.forkActionMinConfidence)}`,
			`Drafter fork: ${self.drafterEnabled ? "On" : "Off"}`,
			`Fork gate: ${self.forkGateEnabled ? "On" : "Off"}`,
			`Gate warm-up: ${self.forkGateMinSamples}`,
			`Gate window: ${self.forkGateWindowSize}`,
			`Gate minimum net: ${formatDuration(self.forkGateMinNetBenefitMs)}`,
			`Gate probe interval: ${self.forkGateProbeInterval}`,
			`Gate failure threshold: ${self.forkGateFailureThreshold}`,
			`Candidate bundle: ${self.maxCandidates}`,
			`Draft tokens: ${self.maxDraftTokens}`,
			`Draft format: ${self.draftFormat}`,
			`Draft boundary: ${self.draftBoundary}`,
			`Fork tokens: ${self.forkMaxTokens}`,
			`Fork temperature: ${formatNumber(self.forkTemperature)}`,
			`Decoder: ${self.forkDecoder}`,
			`Forced prefix: ${self.forkForcedPrefix}`,
			`Require logprobs: ${self.requireLogprobs ? "On" : "Off"}`,
			`Control timeout: ${formatDuration(self.timeoutMs)}`,
			`Bearer-token env: ${self.apiKeyEnv ?? "none"}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Enabled:")) updateSelfSpeculation(controller, settings, { enabled: !self.enabled });
		if (choice.startsWith("Endpoint:")) await editSelfSpeculationEndpoint(ctx, controller, settings);
		if (choice.startsWith("Fork transport:")) {
			const selected = await ctx.ui.select("Fork transport", ["provider", "sidecar", BACK]);
			if (selected === "provider" || selected === "sidecar")
				updateSelfSpeculation(controller, settings, { forkTransport: selected });
		}
		if (choice.startsWith("Actor fork:"))
			updateSelfSpeculation(controller, settings, { forkEnabled: !self.forkEnabled });
		if (choice.startsWith("Fork action source:"))
			updateSelfSpeculation(controller, settings, { forkActionEnabled: !self.forkActionEnabled });
		if (choice.startsWith("Fork action confidence:"))
			await editSelfSpeculationProbability(
				ctx,
				controller,
				settings,
				"forkActionMinConfidence",
				"Fork action minimum confidence",
			);
		if (choice.startsWith("Drafter fork:"))
			updateSelfSpeculation(controller, settings, { drafterEnabled: !self.drafterEnabled });
		if (choice.startsWith("Fork gate:"))
			updateSelfSpeculation(controller, settings, { forkGateEnabled: !self.forkGateEnabled });
		if (choice.startsWith("Gate warm-up:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "forkGateMinSamples", "Gate warm-up samples");
		if (choice.startsWith("Gate window:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "forkGateWindowSize", "Gate rolling window");
		if (choice.startsWith("Gate minimum net:"))
			await editSelfSpeculationNonNegativeNumber(
				ctx,
				controller,
				settings,
				"forkGateMinNetBenefitMs",
				"Gate minimum net benefit (ms)",
			);
		if (choice.startsWith("Gate probe interval:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "forkGateProbeInterval", "Gate probe interval");
		if (choice.startsWith("Gate failure threshold:"))
			await editSelfSpeculationInteger(
				ctx,
				controller,
				settings,
				"forkGateFailureThreshold",
				"Gate failure threshold",
			);
		if (choice.startsWith("Candidate bundle:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "maxCandidates", "Candidate bundle size");
		if (choice.startsWith("Draft tokens:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "maxDraftTokens", "Draft tokens");
		if (choice.startsWith("Draft format:"))
			await editSelfSpeculationString(ctx, controller, settings, "draftFormat", "Draft format", false);
		if (choice.startsWith("Draft boundary:"))
			await editSelfSpeculationString(ctx, controller, settings, "draftBoundary", "Draft boundary", false);
		if (choice.startsWith("Fork tokens:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "forkMaxTokens", "Fork tokens");
		if (choice.startsWith("Control timeout:"))
			await editSelfSpeculationInteger(ctx, controller, settings, "timeoutMs", "Control timeout (ms)");
		if (choice.startsWith("Fork temperature:"))
			await editSelfSpeculationTemperature(ctx, controller, settings);
		if (choice.startsWith("Decoder:"))
			await editSelfSpeculationString(ctx, controller, settings, "forkDecoder", "Fork decoder", false);
		if (choice.startsWith("Forced prefix:"))
			await editSelfSpeculationString(ctx, controller, settings, "forkForcedPrefix", "Forced prefix", false);
		if (choice.startsWith("Require logprobs:"))
			updateSelfSpeculation(controller, settings, { requireLogprobs: !self.requireLogprobs });
		if (choice.startsWith("Bearer-token env:"))
			await editSelfSpeculationString(ctx, controller, settings, "apiKeyEnv", "Bearer-token environment variable", true);
	}
}

async function openDrafterSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const choice = await ctx.ui.select("Drafter", [
			`Enabled: ${settings.drafterEnabled ? "On" : "Off"}`,
			`Action utility gate: ${settings.drafterGateEnabled ? "On" : "Off"}`,
			`Model › ${settings.draftModel ?? activeModelReference(ctx)}`,
			`Rollout depth: ${settings.drafterMaxDepth}`,
			`Output tokens: ${settings.drafterMaxTokens ?? "provider default"}`,
			`Deterministic requests: ${settings.drafterDeterministicCandidates}`,
			`Temperature range: ${formatNumber(settings.drafterTemperatureMin)}-${formatNumber(settings.drafterTemperatureMax)}`,
			`Prediction timeout: ${formatDuration(settings.predictionTimeoutMs)}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Enabled:"))
			controller.setSettings({ ...settings, drafterEnabled: !settings.drafterEnabled });
		if (choice.startsWith("Action utility gate:"))
			controller.setSettings({ ...settings, drafterGateEnabled: !settings.drafterGateEnabled });
		if (choice.startsWith("Model")) await editDraftModel(ctx, controller, settings);
		if (choice.startsWith("Rollout depth:")) {
			await editDrafterNonNegativeInteger(ctx, controller, settings, "drafterMaxDepth", "Drafter rollout depth");
		}
		if (choice.startsWith("Output tokens:")) {
			await editPositiveInteger(
				ctx,
				controller,
				settings,
				"drafterMaxTokens",
				"Drafter output tokens (blank for provider default)",
			);
		}
		if (choice.startsWith("Deterministic requests:")) {
			await editDrafterNonNegativeInteger(
				ctx,
				controller,
				settings,
				"drafterDeterministicCandidates",
				"Deterministic Drafter requests",
			);
		}
		if (choice.startsWith("Temperature range:")) {
			await editDrafterTemperatureRange(ctx, controller, settings);
		}
		if (choice.startsWith("Prediction timeout:")) {
			await editPositiveInteger(ctx, controller, settings, "predictionTimeoutMs", "Prediction timeout (ms)");
		}
	}
}

async function openPatternAwareSettings(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const choice = await ctx.ui.select("PatternAware", [
			`Enabled: ${settings.patternAware.enabled ? "On" : "Off"}`,
			`Learning › context ${settings.patternAware.maxContextLength}, promotion ${settings.patternAware.minOccurrences}`,
			`Multi-step prediction › ${settings.patternAware.multiStepEnabled ? "On" : "Off"}, beam/tool ${settings.patternAware.beamWidth}, depth ${settings.patternAware.maxPredictionDepth}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Enabled:")) {
			controller.setSettings({
				...settings,
				patternAware: { ...settings.patternAware, enabled: !settings.patternAware.enabled },
			});
		}
		if (choice.startsWith("Learning")) await openPatternLearning(ctx, controller);
		if (choice.startsWith("Multi-step prediction")) await openPatternMultiStep(ctx, controller);
	}
}

async function openPatternLearning(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const pattern = settings.patternAware;
		const choice = await ctx.ui.select("PatternAware learning", [
			`Context events: ${pattern.maxContextLength}`,
			`Future gap: ${pattern.maxFutureGap}`,
			`Future gap coverage: ${formatPercent(pattern.futureGapCoverage)}`,
			`Decay half-life: ${pattern.decayHalfLifeEvents} events`,
			`Promotion occurrences: ${pattern.minOccurrences}`,
			`Pattern capacity: ${pattern.maxPatterns}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Context events:")) {
			await editPatternInteger(ctx, controller, settings, "maxContextLength", "Pattern context events", false);
		}
		if (choice.startsWith("Future gap:")) {
			await editPatternInteger(ctx, controller, settings, "maxFutureGap", "Pattern future gap", true);
		}
		if (choice.startsWith("Future gap coverage:")) await editFutureGapCoverage(ctx, controller, settings);
		if (choice.startsWith("Decay half-life:")) await editPatternHalfLife(ctx, controller, settings);
		if (choice.startsWith("Promotion occurrences:")) {
			await editPatternInteger(ctx, controller, settings, "minOccurrences", "Promotion occurrences", false);
		}
		if (choice.startsWith("Pattern capacity:")) {
			await editPatternInteger(ctx, controller, settings, "maxPatterns", "Pattern capacity", false);
		}
	}
}

async function openPatternMultiStep(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const pattern = settings.patternAware;
		const choice = await ctx.ui.select("PatternAware multi-step", [
			`Enabled: ${pattern.multiStepEnabled ? "On" : "Off"}`,
			`Beam width per tool: ${pattern.beamWidth}`,
			`Prediction depth: ${pattern.maxPredictionDepth}`,
			`Minimum binding replay: ${formatPercent(pattern.minBindingReplayProbability)}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Enabled:")) {
			controller.setSettings({
				...settings,
				patternAware: { ...pattern, multiStepEnabled: !pattern.multiStepEnabled },
			});
		}
		if (choice.startsWith("Beam width per tool:")) {
			await editPatternInteger(ctx, controller, settings, "beamWidth", "Pattern beam width per tool", false);
		}
		if (choice.startsWith("Prediction depth:")) {
			await editPatternInteger(ctx, controller, settings, "maxPredictionDepth", "Pattern prediction depth", false);
		}
		if (choice.startsWith("Minimum binding replay:")) {
			await editPatternProbability(
				ctx,
				controller,
				settings,
				"minBindingReplayProbability",
				"Minimum binding replay probability",
			);
		}
	}
}

async function openSchedulingAndCache(ctx: ExtensionContext, controller: SpeculativeActionController): Promise<void> {
	while (true) {
		const settings = controller.settings();
		const choice = await ctx.ui.select("Scheduling & cache", [
			`Drafter requests: ${settings.candidateLimit}`,
			`Concurrent actions: ${settings.maxConcurrentActions}`,
			`Resource cache entries: ${settings.resourceCacheMaxEntries}`,
			`Resource cache memory: ${formatBytes(settings.resourceCacheMaxBytes)}`,
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Drafter requests:")) {
			await editPositiveInteger(ctx, controller, settings, "candidateLimit", "Drafter requests per turn");
		}
		if (choice.startsWith("Concurrent actions:")) {
			await editPositiveInteger(ctx, controller, settings, "maxConcurrentActions", "Concurrent actions");
		}
		if (choice.startsWith("Resource cache entries:")) {
			await editPositiveInteger(ctx, controller, settings, "resourceCacheMaxEntries", "Resource cache entries");
		}
		if (choice.startsWith("Resource cache memory:")) await editCacheBytes(ctx, controller, settings);
	}
}

async function openToolsAndExecution(
	ctx: ExtensionContext,
	editor: SpeculativeActionController,
	controller: SpeculativeActionController,
): Promise<void> {
	while (true) {
		const settings = editor.settings();
		const choice = await ctx.ui.select("Tools & execution", [
			`Tool policy › ${enabledToolCount(settings)} enabled`,
			"Execution guarantees",
			BACK,
		]);
		if (!choice || choice === BACK) return;
		if (choice.startsWith("Tool policy"))
			await editToolPolicy(ctx, editor, controller.registeredTools(), controller.toolConflicts());
		if (choice === "Execution guarantees") {
			ctx.ui.notify(
				`Every speculative tool first uses a runtime-wide sandbox when one is available. Otherwise read-only tools use resource snapshots, write/edit use private Git worktrees, and tools without a safe isolation route are matched but executed only by the Actor.\n${controller.executionSummary()}`,
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
			const availability = conflicts.has(tool) ? " · custom override" : registered.has(tool) ? "" : " · unavailable";
			labels.set(`${selected ? "[x]" : "[ ]"} ${tool} · ${toolIsolationLabel(tool)}${availability}`, tool);
		}
		const choice = await ctx.ui.select("Tool policy", [...labels.keys(), BACK]);
		if (!choice || choice === BACK) return;
		const tool = labels.get(choice);
		if (!tool) continue;
		if (!(KEYABLE_TOOLS as readonly string[]).includes(tool)) {
			ctx.ui.notify(`${tool} has no speculative action semantics.`, "warning");
			continue;
		}
		const selected = settings.tools.includes(tool);
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

async function editSelfSpeculationEndpoint(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input("Self-speculation endpoint", settings.selfSpeculation.endpoint);
	if (value === undefined) return;
	const endpoint = value.trim();
	if (!/^https?:\/\/[^\s]+$/u.test(endpoint)) {
		ctx.ui.notify("Endpoint must be an absolute HTTP(S) URL.", "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { endpoint });
}

async function editSelfSpeculationInteger(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field:
		| "maxCandidates"
		| "maxDraftTokens"
		| "forkMaxTokens"
		| "timeoutMs"
		| "forkGateMinSamples"
		| "forkGateWindowSize"
		| "forkGateProbeInterval"
		| "forkGateFailureThreshold",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings.selfSpeculation[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || parsed <= 0) {
		ctx.ui.notify(`${title} must be a positive integer.`, "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { [field]: parsed });
}

async function editSelfSpeculationNonNegativeNumber(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "forkGateMinNetBenefitMs",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings.selfSpeculation[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isFinite(parsed) || parsed < 0) {
		ctx.ui.notify(`${title} must be a non-negative number.`, "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { [field]: parsed });
}

async function editSelfSpeculationProbability(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "forkActionMinConfidence",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings.selfSpeculation[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
		ctx.ui.notify(`${title} must be between 0 and 1.`, "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { [field]: parsed });
}

async function editSelfSpeculationTemperature(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input("Fork temperature", String(settings.selfSpeculation.forkTemperature));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isFinite(parsed) || parsed < 0) {
		ctx.ui.notify("Fork temperature must be a non-negative number.", "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { forkTemperature: parsed });
}

async function editSelfSpeculationString(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "draftFormat" | "draftBoundary" | "forkDecoder" | "forkForcedPrefix" | "apiKeyEnv",
	title: string,
	allowEmpty: boolean,
): Promise<void> {
	const current = settings.selfSpeculation[field] ?? "";
	const value = await ctx.ui.input(title, current);
	if (value === undefined) return;
	const normalized = value.trim();
	if (!allowEmpty && !normalized) {
		ctx.ui.notify(`${title} cannot be empty.`, "warning");
		return;
	}
	updateSelfSpeculation(controller, settings, { [field]: normalized || undefined });
}

async function editPositiveInteger(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field:
		| "candidateLimit"
		| "maxConcurrentActions"
		| "resourceCacheMaxEntries"
		| "predictionTimeoutMs"
		| "drafterMaxTokens",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings[field] ?? ""));
	if (value === undefined) return;
	if (field === "drafterMaxTokens" && value.trim() === "") {
		const { drafterMaxTokens: _removed, ...next } = settings;
		controller.setSettings(next);
		return;
	}
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || parsed <= 0) {
		ctx.ui.notify(`${title} must be a positive integer.`, "warning");
		return;
	}
	controller.setSettings({
		...settings,
		[field]: field === "candidateLimit" || field === "maxConcurrentActions" ? clampCandidateLimit(parsed) : parsed,
	});
}

async function editDrafterNonNegativeInteger(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "drafterMaxDepth" | "drafterDeterministicCandidates",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || parsed < 0) {
		ctx.ui.notify(`${title} must be a non-negative integer.`, "warning");
		return;
	}
	controller.setSettings({ ...settings, [field]: parsed });
}

async function editDrafterTemperatureRange(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input(
		"Drafter temperature range",
		`${formatNumber(settings.drafterTemperatureMin)},${formatNumber(settings.drafterTemperatureMax)}`,
	);
	if (value === undefined) return;
	const [lower, upper, ...extra] = value.split(",").map((item) => Number(item.trim()));
	if (extra.length > 0 || !Number.isFinite(lower) || !Number.isFinite(upper) || lower < 0 || upper < lower) {
		ctx.ui.notify(
			"Drafter temperature range must be two non-negative comma-separated numbers in ascending order.",
			"warning",
		);
		return;
	}
	controller.setSettings({ ...settings, drafterTemperatureMin: lower, drafterTemperatureMax: upper });
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

async function editCacheBytes(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input(
		"Resource cache memory (MiB)",
		String(Math.max(1, Math.round(settings.resourceCacheMaxBytes / (1024 * 1024)))),
	);
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || parsed <= 0) {
		ctx.ui.notify("Resource cache memory must be a positive integer in MiB.", "warning");
		return;
	}
	controller.setSettings({ ...settings, resourceCacheMaxBytes: parsed * 1024 * 1024 });
}

async function editFutureGapCoverage(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input("Future gap coverage (0-1)", String(settings.patternAware.futureGapCoverage));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
		ctx.ui.notify("Future gap coverage must be between 0 and 1.", "warning");
		return;
	}
	controller.setSettings({
		...settings,
		patternAware: { ...settings.patternAware, futureGapCoverage: parsed },
	});
}

async function editPatternHalfLife(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
): Promise<void> {
	const value = await ctx.ui.input("Pattern half-life (events)", String(settings.patternAware.decayHalfLifeEvents));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || parsed <= 0) {
		ctx.ui.notify("Pattern half-life must be a positive integer.", "warning");
		return;
	}
	controller.setSettings({
		...settings,
		patternAware: { ...settings.patternAware, decayHalfLifeEvents: parsed },
	});
}

async function editPatternInteger(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "maxContextLength" | "beamWidth" | "maxPredictionDepth" | "maxFutureGap" | "minOccurrences" | "maxPatterns",
	title: string,
	allowZero: boolean,
): Promise<void> {
	const value = await ctx.ui.input(title, String(settings.patternAware[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isInteger(parsed) || (allowZero ? parsed < 0 : parsed <= 0)) {
		ctx.ui.notify(`${title} must be ${allowZero ? "a non-negative" : "a positive"} integer.`, "warning");
		return;
	}
	controller.setSettings({
		...settings,
		patternAware: { ...settings.patternAware, [field]: parsed },
	});
}

async function editPatternProbability(
	ctx: ExtensionContext,
	controller: SpeculativeActionController,
	settings: EffectiveSpeculativeActionSettings,
	field: "minBindingReplayProbability",
	title: string,
): Promise<void> {
	const value = await ctx.ui.input(`${title} (0-1)`, String(settings.patternAware[field]));
	if (value === undefined) return;
	const parsed = Number(value.trim());
	if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
		ctx.ui.notify(`${title} must be between 0 and 1.`, "warning");
		return;
	}
	controller.setSettings({
		...settings,
		patternAware: { ...settings.patternAware, [field]: parsed },
	});
}

function showRecentEvents(ctx: ExtensionContext, controller: SpeculativeActionController): void {
	const events = controller.recentEvents();
	ctx.ui.notify(events.length > 0 ? events.join("\n") : "No speculative action events recorded yet.", "info");
}

export function formatSpeculativeActionEvent(event: SpeculativeActionEvent<string>): string {
	const parts = [`[${event.type}]`];
	switch (event.type) {
		case "task":
			parts.push(
				`${formatDuration(event.timing.endToEndMs)} actual`,
				`${formatDuration(event.timing.serializedMs)} serialized`,
				`${formatDuration(event.timing.hiddenLatencyMs)} serialized overlap`,
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
		case "candidate":
			parts.push(event.candidate.tool, event.candidate.source, event.state.status);
			if (event.state.status === "running") {
				parts.push(
					`${event.candidate.origin === "actor_preview" ? "previewed" : "predicted"} ${compactEventText(event.candidate.predictedAction)}`,
				);
			} else if (event.state.status === "succeeded") {
				parts.push(formatDuration(event.state.executionMs));
			} else {
				parts.push(causeSummary(event.state.cause), formatDuration(event.state.executionMs));
			}
			break;
		case "actor_action": {
			const sources = [...new Set(event.settlement.matchedPredictions.map((prediction) => prediction.source))];
			parts.push(
				event.settlement.tool,
				sources.join("+") || (event.settlement.provider.kind === "speculative" ? "cache" : "no prediction"),
			);
			if (event.settlement.provider.kind === "speculative") {
				parts.push(
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

function cloneSettings(settings: EffectiveSpeculativeActionSettings): EffectiveSpeculativeActionSettings {
	return {
		...settings,
		patternAware: { ...settings.patternAware },
		selfSpeculation: { ...settings.selfSpeculation },
		tools: [...settings.tools],
	};
}

function sameSettings(left: EffectiveSpeculativeActionSettings, right: EffectiveSpeculativeActionSettings): boolean {
	return JSON.stringify(left) === JSON.stringify(right);
}

function sourceSummary(settings: EffectiveSpeculativeActionSettings): string {
	if (!settings.enabled) return "Inactive";
	const sources = [
		settings.drafterEnabled ? "Drafter" : undefined,
		settings.patternAware.enabled ? "PatternAware" : undefined,
		settings.selfSpeculation.enabled &&
		settings.selfSpeculation.forkTransport === "sidecar" &&
		settings.selfSpeculation.forkEnabled &&
		settings.selfSpeculation.forkActionEnabled
			? "SidecarFork"
			: undefined,
	]
		.filter((source): source is string => source !== undefined)
		.join(" + ");
	return sources || "No source enabled";
}

function selfSpeculationSummary(settings: SelfSpeculationSettings): string {
	return settings.enabled
		? `On, ${settings.forkTransport}, ${settings.maxCandidates}×${settings.maxDraftTokens}`
		: "Off";
}

function activeModelReference(ctx: ExtensionContext): string {
	return ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : "active model";
}

function enabledToolCount(settings: EffectiveSpeculativeActionSettings): number {
	return new Set(settings.tools).size;
}

function toolIsolationLabel(tool: string): string {
	if ((OBSERVATION_ACTION_TOOLS as readonly string[]).includes(tool)) return "resource snapshot fallback";
	if ((WORKSPACE_MUTATION_ACTION_TOOLS as readonly string[]).includes(tool)) return "Git worktree fallback";
	if ((UNBOUNDED_ACTION_TOOLS as readonly string[]).includes(tool)) return "requires runtime sandbox";
	return "unsupported";
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

function executionWorldSummary(worlds: readonly SpeculativeAgentExecutionWorld[]): string {
	const supporting = (isolation: SpeculativeAgentExecutionWorld["isolation"]) =>
		worlds.filter((world) => world.isolation === isolation).map((world) => world.id);
	const snapshots = [...new Set(["resource_version", ...supporting("resource_snapshot")])];
	return `Execution capabilities: runtime sandbox ${toolsSummary(supporting("runtime_sandbox"))}; resource snapshot ${toolsSummary(snapshots)}; workspace branch ${toolsSummary(supporting("workspace_branch"))}`;
}

function countSummary(counts: Readonly<Record<string, number>>): string {
	const entries = Object.entries(counts).sort(([leftKey, left], [rightKey, right]) =>
		right === left ? leftKey.localeCompare(rightKey) : right - left,
	);
	return entries.length > 0 ? entries.map(([key, count]) => `${key}=${count}`).join(", ") : "none";
}

function formatDuration(ms: number): string {
	if (ms < 1000) return `${Math.round(ms)}ms`;
	if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0).replace(/\.0$/, "")}s`;
	const seconds = Math.round(ms / 1000);
	return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function formatBytes(bytes: number): string {
	if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
	if (bytes < 1024) return `${Math.round(bytes)} B`;
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1).replace(/\.0$/, "")} KiB`;
	return `${(bytes / (1024 * 1024)).toFixed(1).replace(/\.0$/, "")} MiB`;
}

function formatPercent(value: number): string {
	return `${Math.round(value * 100)}%`;
}

function formatNumber(value: number): string {
	return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));
}
