import { createHash } from "node:crypto";
import path from "node:path";
import type { AgentTool, AgentToolCall, AgentToolResult } from "@earendil-works/pi-agent-core";
import type {
	Api,
	AssistantMessage,
	Context,
	Model,
	SimpleStreamOptions,
	ToolResultMessage,
} from "@earendil-works/pi-ai";
import { validateToolArguments } from "@earendil-works/pi-ai";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionKey, type ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	drafterRequestTemperature,
	normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection,
	type SpeculativeToolSelectionInput,
	usageTokenCount,
} from "./common.ts";
import { ExecutionWorldRouter, type SpeculativeExecutionRoute } from "./execution-world.ts";
import {
	DrafterUtilityGate,
	type DrafterUtilityBatch,
	type DrafterUtilityGateSnapshot,
} from "./drafter-utility-gate.ts";
import {
	acquirePatternAwareStore,
	asPatternAwareRuntimeContext,
	PATTERN_AWARE_DEFAULTS,
	type PatternAwareCandidate,
	type PatternAwareEventInput,
	type PatternAwareRuntimeContext,
	type PatternAwareSettings,
	type PatternAwareStore,
	type PatternAwareStoreLease,
	patternAwareAnalyzerKey,
	patternAwareRuntimeContext,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "./pattern-aware.ts";
import type { PlanAction, PlanProposal } from "./plan-proposal.ts";
import type {
	CandidatePreflight,
	MaterializedSpeculativeCandidate,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionSettings,
	SpeculativePlanSource,
} from "./runtime.ts";
import type { SelfSpeculationSettingsInput } from "./self-speculation.ts";
import { candidateExecutionMs, candidateToolNames, makeSpeculativeActionRuntime } from "./runtime.ts";
import { stableStringify } from "./stable-json.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";

export interface SpeculativeAgentSettingsInput {
	readonly enabled?: boolean;
	readonly drafterEnabled?: boolean;
	/** Adaptively skip a root Drafter batch when its measured action-side utility is negative. */
	readonly drafterGateEnabled?: boolean;
	/** Output-informed successor actions retained after the first Drafter action. */
	readonly drafterMaxDepth?: number;
	/** Optional hard output cap for each one-action Drafter request; omitted uses the provider default. */
	readonly drafterMaxTokens?: number;
	/** Number of leading Drafter requests sent at temperature zero. */
	readonly drafterDeterministicCandidates?: number;
	/** Inclusive temperature range stratified across the remaining requests. */
	readonly drafterTemperatureMin?: number;
	readonly drafterTemperatureMax?: number;
	readonly candidateLimit?: number;
	readonly maxConcurrentActions?: number;
	readonly resourceCacheMaxEntries?: number;
	readonly resourceCacheMaxBytes?: number;
	readonly predictionTimeoutMs?: number;
	readonly patternAware?: Partial<PatternAwareSettings>;
	readonly selfSpeculation?: SelfSpeculationSettingsInput;
	/** Legacy grouped input is accepted only for configuration migration. */
	readonly tools?: SpeculativeToolSelectionInput;
}

export interface SpeculativeAgentPreflightContext {
	readonly tool: AgentTool;
	readonly toolName: string;
	readonly args: unknown;
	readonly action: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly signal: AbortSignal;
}

export interface DraftOptionsContext {
	readonly actorModel: Model<Api>;
	readonly draftModel: Model<Api>;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly signal: AbortSignal;
}

export interface CreateSpeculativeActionHostOptions {
	/** Workspace root used for action canonicalization and resource validation. */
	readonly cwd: string;
	/** Runtime settings. The feature remains disabled when omitted. */
	readonly getSettings?: () => SpeculativeAgentSettingsInput | Promise<SpeculativeAgentSettingsInput>;
	/** Drafter model. Defaults to the actor model when omitted or unresolved. */
	readonly draftModel?:
		| Model<Api>
		| ((actorModel: Model<Api>) => Model<Api> | undefined | Promise<Model<Api> | undefined>);
	/** Resolve drafter request options, including credentials when using a different provider. */
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	/** Provider completion used by the drafter. */
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	/** Resolve the concrete executor identity used by both speculative and actor calls. */
	readonly resolveInvocation?: (
		tool: string,
		input: unknown,
	) => ToolInvocation | undefined | Promise<ToolInvocation | undefined>;
	/**
	 * Non-interactive permission and policy check for speculative execution.
	 * Candidates are rejected when this callback is absent.
	 */
	readonly preflight?: (
		context: SpeculativeAgentPreflightContext,
	) => boolean | CandidatePreflight | Promise<boolean | CandidatePreflight>;
	/** Canonical K(a), projection, and resource-version semantics for this host. */
	readonly actionSemantics?: ActionSemanticsRegistry;
	/** Lossless Π rules; each rule owns key relation, realized coverage, and output reconstruction. */
	readonly projectionRules?: readonly ActionProjectionRule<ToolSettlement>[];
	/** Ordered execution capabilities. A runtime-wide sandbox takes precedence over local fallbacks. */
	readonly executionWorlds?: readonly SpeculativeAgentExecutionWorld[];
	/** Optional persistence root for workspace-hashed PatternAware state. */
	readonly patternStateDirectory?: string;
	/** Stable logical workspace identity when physical checkout paths are ephemeral. */
	readonly patternWorkspaceIdentity?: string;
	/** Optional injected store, primarily for embedding and deterministic tests. */
	readonly patternStore?: PatternAwareStore | Promise<PatternAwareStore>;
	/** Starts request-scoped inference integration before prediction sources launch. */
	readonly onTurnStarted?: (input: {
		readonly turnID: string;
		readonly actorModel: Model<Api>;
		readonly context: Context;
		readonly decisionSequence: number;
	}) => void | Promise<void>;
	/** Receives every validated K(a) as a concrete tool call, independent of execution isolation. */
	readonly onCandidateMaterialized?: (candidate: MaterializedSpeculativeCandidate<string>) => void | Promise<void>;
	readonly onEvent?: (event: SpeculativeActionEvent<string>) => void | Promise<void>;
}

export interface SpeculativeActionHost {
	readonly sessionID: string;
	readonly runtime: SpeculativeActionRuntime<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentConsumeInput
	>;
	readonly startTurn: (input: Omit<AgentStartInput, "sessionID">, signal?: AbortSignal) => Promise<void>;
	readonly previewActorTool: (
		input: { readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	) => Promise<void>;
	readonly previewActorCall: (input: Omit<AgentConsumeInput, "sessionID">, signal?: AbortSignal) => Promise<void>;
	readonly consume: (
		input: Omit<AgentConsumeInput, "sessionID">,
		signal?: AbortSignal,
	) => Promise<ToolSettlement | undefined>;
	readonly actual: (
		input: Omit<AgentConsumeInput, "sessionID"> & { readonly durationMs: number; readonly output?: ToolSettlement },
	) => Promise<void>;
	readonly finishTurn: (turnID: string, terminal?: boolean) => Promise<void>;
	readonly drafterGateSnapshot: () => ActionDrafterGateSnapshot;
	readonly dispose: () => Promise<void>;
}

export type ActionDrafterGateSnapshot = DrafterUtilityGateSnapshot;

interface AgentStartInput {
	readonly sessionID: string;
	readonly turnID: string;
	readonly actorModel: Model<Api>;
	readonly context: Context;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly tools: readonly AgentTool[];
}

interface AgentConsumeInput {
	readonly sessionID: string;
	readonly turnID: string;
	readonly id?: string;
	readonly tool: string;
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
	readonly terminal?: boolean;
}

interface AgentStateData {
	readonly tools: ReadonlyMap<string, AgentTool>;
	readonly schemaHashes: Readonly<Record<string, string>>;
}

export function patternPlanActionID(actionIdentity: string, parentActionID = "root"): string {
	return `pattern:${stableHash({ actionIdentity, parentActionID }).slice(0, 16)}`;
}

/**
 * Build source-neutral speculative plan execution for a host. The host owns lifecycle and tool interception.
 */
export function createSpeculativeActionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
): SpeculativeActionHost {
	const actionSemantics = options.actionSemantics ?? PI_ACTION_SEMANTICS;
	const projectionRules = (options.projectionRules ?? []).filter((rule) => actionSemantics.supportsProjector(rule.id));
	const executionWorlds = [...new Set(options.executionWorlds ?? [])];
	if (
		!executionWorlds.some(
			(world) =>
				world.id === "resource_version" && world.scope === "fallback" && world.isolation === "resource_snapshot",
		)
	) {
		executionWorlds.push(createResourceSnapshotExecutionWorld(actionSemantics));
	}
	const executionRouter = new ExecutionWorldRouter(executionWorlds);
	const resolveExecutionRoute = (tool: string, signal?: AbortSignal, action?: ActionKey) => {
		const effect = actionSemantics.effect(tool);
		return effect
			? executionRouter.resolve(
					{ tool, effect, ...(action ? { action } : {}) },
					{ cwd: options.cwd, ...(signal ? { signal } : {}) },
				)
			: undefined;
	};
	const patternActionSemantics = {
		namespace: "pi-action-semantics-v1",
		actionKey: (tool: string, input: Readonly<Record<string, unknown>>, schemaHash?: string) =>
			actionSemantics.buildKey(tool, input, options.cwd, schemaHash),
		projectors: projectionRules,
	};
	let openedPatternStore: Promise<PatternAwareStoreLease> | undefined;
	let openedPatternStoreKey: string | undefined;
	const authoritativeBatches = new Map<string, Map<number, PatternAwareEventInput>>();
	const patternRevisions = new Map<string, number>();
	const carriedPatternPredictions = new Map<string, string>();
	type DrafterBatch = {
		readonly model: Model<Api>;
		readonly context: Context;
		readonly options: SimpleStreamOptions;
		readonly utility: DrafterUtilityBatch;
	};
	const drafterBatches = new Map<string, Promise<DrafterBatch>>();
	const drafterGate = new DrafterUtilityGate();
	let patternAnalysisTail: Promise<void> = Promise.resolve();
	const queuePatternAnalysis = (analysis: () => void | Promise<void>) => {
		patternAnalysisTail = patternAnalysisTail
			.then(() => new Promise<void>((resolve) => setTimeout(resolve, 0)))
			.then(analysis)
			.catch(() => {
				// Optional learning cannot poison later observations or the Actor lifecycle.
			});
	};
	const authoritativeBatchKey = (batchSessionID: string, turnID: string) => JSON.stringify([batchSessionID, turnID]);
	const nextPatternRevision = (batchSessionID: string, turnID: string) => {
		const key = authoritativeBatchKey(batchSessionID, turnID);
		const revision = (patternRevisions.get(key) ?? -1) + 1;
		patternRevisions.set(key, revision);
		return revision;
	};
	const patternPredictionSignature = (candidates: readonly PatternAwareCandidate[]) =>
		JSON.stringify(
			candidates
				.map((candidate) => [candidate.actionIdentity, candidate.horizon, candidate.latestHorizon] as const)
				.sort(([left], [right]) => left.localeCompare(right)),
		);
	const clearAuthoritativeSession = (batchSessionID: string) => {
		for (const [key, batch] of authoritativeBatches) {
			if (batch.values().next().value?.sessionID === batchSessionID) authoritativeBatches.delete(key);
		}
	};
	const resolveSettings = async (): Promise<SpeculativeActionSettings> => {
		const settings = (await options.getSettings?.()) ?? {};
		const drafter = normalizeDrafterRequestSettings(settings);
		return {
			enabled: typeof settings.enabled === "boolean" ? settings.enabled : DEFAULTS.enabled,
			drafterEnabled:
				typeof settings.drafterEnabled === "boolean" ? settings.drafterEnabled : DEFAULTS.drafterEnabled,
			candidateLimit: clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
			maxConcurrentActions: clampCandidateLimit(settings.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions),
			resourceCacheMaxEntries: normalizePositiveInteger(
				settings.resourceCacheMaxEntries,
				DEFAULTS.resourceCacheMaxEntries,
			),
			resourceCacheMaxBytes: normalizePositiveInteger(
				settings.resourceCacheMaxBytes,
				DEFAULTS.resourceCacheMaxBytes,
			),
			predictionTimeoutMs: normalizeTimeout(settings.predictionTimeoutMs),
			sourceConfig: {
				...drafter,
				drafterGateEnabled:
					typeof settings.drafterGateEnabled === "boolean"
						? settings.drafterGateEnabled
						: DEFAULTS.drafterGateEnabled,
				patternAware: patternAwareSettings(settings.patternAware ?? PATTERN_AWARE_DEFAULTS),
			},
			tools: normalizeSpeculativeToolSelection(settings.tools, actionSemantics.toolNames()),
		};
	};
	const sourcePatternSettings = (settings: SpeculativeActionSettings): PatternAwareSettings =>
		patternAwareSettings(settings.sourceConfig?.patternAware);
	const sourceDrafterSettings = (settings: SpeculativeActionSettings) =>
		normalizeDrafterRequestSettings(settings.sourceConfig);
	const drafterModelKey = (model: Model<Api>): string => JSON.stringify([model.provider, model.api, model.id]);
	const resolvePatternStore = async (settings: SpeculativeActionSettings): Promise<PatternAwareStore> => {
		if (options.patternStore) {
			return options.patternStore;
		}
		const patternSettings = sourcePatternSettings(settings);
		const configurationKey = patternAwareAnalyzerKey(patternSettings);
		if (openedPatternStore && openedPatternStoreKey !== configurationKey) {
			const previous = await openedPatternStore;
			previous.store.finishSession(sessionID);
			await previous.release();
			openedPatternStore = undefined;
		}
		openedPatternStoreKey = configurationKey;
		openedPatternStore ??= acquirePatternAwareStore(
			options.patternWorkspaceIdentity ?? options.cwd,
			patternSettings,
			options.patternStateDirectory,
			patternActionSemantics,
		);
		return (await openedPatternStore).store;
	};
	const finishPatternSession = async (): Promise<void> => {
		drafterBatches.clear();
		drafterGate.reset();
		patternRevisions.clear();
		carriedPatternPredictions.clear();
		clearAuthoritativeSession(sessionID);
		await patternAnalysisTail;
		const store = options.patternStore
			? await options.patternStore
			: openedPatternStore
				? (await openedPatternStore).store
				: undefined;
		if (!store) return;
		store.finishSession(sessionID);
		try {
			await store.flush();
		} catch {
			// Persistence failure must not change Agent lifecycle semantics.
		}
	};
	const prepareExecutionWorlds = async (tools: readonly string[], signal?: AbortSignal): Promise<void> => {
		await Promise.all([...new Set(tools)].map((tool) => resolveExecutionRoute(tool, signal)));
	};
	type AgentPlanSource = SpeculativePlanSource<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentStateData
	>;
	type DrafterPlanFeedback = {
		readonly kind: "drafter_plan";
		readonly model: Model<Api>;
		readonly context: Context;
		readonly options: SimpleStreamOptions;
		readonly message: AssistantMessage;
		readonly depth: number;
	};
	const asDrafterPlanFeedback = (value: unknown): DrafterPlanFeedback | undefined =>
		value && typeof value === "object" && (value as { kind?: unknown }).kind === "drafter_plan"
			? (value as DrafterPlanFeedback)
			: undefined;
	const drafterPlanAction = (
		id: string,
		call: AgentToolCall,
		feedback: DrafterPlanFeedback,
		dependsOn?: PlanAction["dependsOn"],
	): PlanAction => ({
		id,
		type: "tool_call",
		tool: call.name,
		input: call.arguments,
		diagnostic: JSON.stringify({ toolCallID: call.id, tool: call.name, input: call.arguments }, null, 2),
		depth: feedback.depth,
		feedback,
		...(dependsOn?.length ? { dependsOn } : {}),
	});
	const drafterFeedback = (
		model: Model<Api>,
		context: Context,
		requestOptions: SimpleStreamOptions,
		message: AssistantMessage,
		call: AgentToolCall,
		depth: number,
	): DrafterPlanFeedback => ({
		kind: "drafter_plan",
		model,
		context,
		options: requestOptions,
		message: { ...message, content: message.content.filter((item) => item.type !== "toolCall" || item === call) },
		depth,
	});
	const drafterToolResult = (call: AgentToolCall, output: ToolSettlement): ToolResultMessage => ({
		...output.result,
		role: "toolResult",
		toolCallId: call.id,
		toolName: call.name,
		isError: output.isError,
		timestamp: Date.now(),
	});
	type PatternPlanFeedback = PatternAwareRuntimeContext & { readonly patternIDs: ReadonlyArray<string> };
	const asPatternPlanFeedback = (value: unknown): PatternPlanFeedback | undefined => {
		const context = asPatternAwareRuntimeContext(value);
		const patternIDs = (value as { patternIDs?: unknown })?.patternIDs;
		if (!context || !Array.isArray(patternIDs) || !patternIDs.every((item) => typeof item === "string"))
			return undefined;
		return { ...context, patternIDs };
	};
	const patternPlanAction = (
		candidate: PatternAwareCandidate,
		store: PatternAwareStore,
		id: string,
		dependsOn?: PlanAction["dependsOn"],
	): PlanAction => ({
		id,
		type: "tool_call",
		tool: candidate.tool,
		input: candidate.input,
		diagnostic: candidate.diagnostic,
		horizon: candidate.horizon,
		latestHorizon: candidate.latestHorizon,
		empiricalProbability: candidate.empiricalProbability,
		conditionalProbability: candidate.conditionalProbability,
		expectedDurationMs: candidate.expectedDurationMs,
		expectedLatencyBenefitMs: candidate.expectedLatencyBenefitMs,
		...(candidate.background ? { background: true } : {}),
		depth: candidate.depth,
		...(dependsOn?.length ? { dependsOn } : {}),
		feedback: { ...patternAwareRuntimeContext(store, candidate), patternIDs: candidate.supportingPatternIDs },
	});
	const drafterSource: AgentPlanSource = {
		id: "drafter",
		enabled: (settings) => settings.drafterEnabled ?? DEFAULTS.drafterEnabled,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "actor_decision",
		multiStepEnabled: (settings, feedback) => {
			const maxDepth = sourceDrafterSettings(settings).drafterMaxDepth;
			if (maxDepth === 0) return false;
			if (feedback === undefined) return true;
			const previous = asDrafterPlanFeedback(feedback);
			return previous !== undefined && previous.depth < maxDepth;
		},
		continueOn: ["execution_succeeded"],
		proposalCount: (settings) => clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
		concurrentProposalPolicy: (settings) =>
			clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit) === 2 ? "first_produced" : "all",
		propose: async ({
			startInput: input,
			data,
			candidateNames,
			proposalIndex,
			proposalCount,
			signal,
			settings,
		}): Promise<PlanProposal | undefined> => {
			const proposalID = `drafter:${input.turnID}:${proposalIndex}`;
			const batchKey = authoritativeBatchKey(input.sessionID, input.turnID);
			let batch = drafterBatches.get(batchKey);
			if (!batch) {
				batch = (async () => {
					const configuredDraftModel =
						typeof options.draftModel === "function"
							? await options.draftModel(input.actorModel)
							: options.draftModel;
					const model = configuredDraftModel ?? input.actorModel;
					const utility = drafterGate.start(
						drafterModelKey(model),
						proposalCount,
						settings.sourceConfig?.drafterGateEnabled !== false,
					);
					let configuredDraftOptions: SimpleStreamOptions | undefined;
					if (utility.allowed) {
						configuredDraftOptions = options.getDraftOptions
							? await options.getDraftOptions({
								actorModel: input.actorModel,
								draftModel: model,
								actorOptions: input.actorOptions,
								signal,
								})
							: input.actorOptions;
					}
					return {
						model,
						context: input.context,
						options: configuredDraftOptions ?? {},
						utility,
					};
				})();
				drafterBatches.set(batchKey, batch);
			}
			const prepared = await batch;
			if (!prepared.utility.allowed) return undefined;
			const drafter = sourceDrafterSettings(settings);
			const { maxTokens: _actorMaxTokens, ...requestOptions } = prepared.options;
			const draftOptions: SimpleStreamOptions & { readonly toolChoice: "required" } = {
				...requestOptions,
				temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter),
				...(drafter.drafterMaxTokens ? { maxTokens: drafter.drafterMaxTokens } : {}),
				toolChoice: "required",
				reasoning: undefined,
				deferred: false,
				sessionId: prepared.options.sessionId ?? sessionID,
				cacheRetention: prepared.options.cacheRetention ?? "short",
			};
			const requestStartedAt = performance.now();
			let requestFailed = false;
			let message: AssistantMessage;
			try {
				message = await options.complete(prepared.model, prepared.context, { ...draftOptions, signal });
				if (message.stopReason === "error" || message.stopReason === "aborted") {
					requestFailed = message.stopReason === "error" || !signal.aborted;
					throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
				}
			} catch (error) {
				if (!signal.aborted) requestFailed = true;
				throw error;
			} finally {
				drafterGate.requestSettled(prepared.utility, performance.now() - requestStartedAt, requestFailed);
			}
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			const tool = data.tools.get(call.name);
			if (
				!tool ||
				!candidateNames.includes(call.name) ||
				validateCandidateArguments(tool, call.name, call.arguments, call.id) === undefined
			)
				return undefined;
			const feedback = drafterFeedback(prepared.model, prepared.context, draftOptions, message, call, 0);
			return {
				id: proposalID,
				source: "drafter",
				revision: 0,
				actions: [drafterPlanAction(`${proposalIndex}:${call.id}`, call, feedback)],
				draftTokens: usageTokenCount(message.usage),
			};
		},
		continue: async ({ proposalID, actionID, revision, feedback, output, signal }) => {
			const previous = asDrafterPlanFeedback(feedback);
			if (!previous || signal.aborted) return undefined;
			const previousCall = previous.message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!previousCall) return undefined;
			const context: Context = {
				...previous.context,
				messages: [...previous.context.messages, previous.message, drafterToolResult(previousCall, output)],
			};
			const continuationOptions = { ...previous.options, toolChoice: "auto" as const };
			const message = await options.complete(previous.model, context, { ...continuationOptions, signal });
			if (message.stopReason === "error" || message.stopReason === "aborted") {
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			}
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			const next = drafterFeedback(previous.model, context, continuationOptions, message, call, previous.depth + 1);
			return {
				proposalID,
				source: "drafter",
				revision,
				upsert: [
					drafterPlanAction(`${actionID}/rollout:${next.depth}:${call.id}`, call, next, [
						{ actionID, condition: "execution_succeeded" },
					]),
				],
				draftTokens: usageTokenCount(message.usage),
			};
		},
	};
	const patternSource: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => sourcePatternSettings(settings).enabled,
		multiStepEnabled: (settings) => sourcePatternSettings(settings).multiStepEnabled,
		requestLifetime: "actor_decision",
		propose: async ({ startInput, settings, definitions }) => {
			const patternSettings = sourcePatternSettings(settings);
			if (!patternSettings.enabled) return undefined;
			await patternAnalysisTail;
			const store = await resolvePatternStore(settings);
			const candidates = store.predict(startInput.sessionID, definitionSchemaHashes(definitions), patternSettings);
			const signature = patternPredictionSignature(candidates);
			const carried = carriedPatternPredictions.get(startInput.sessionID);
			carriedPatternPredictions.delete(startInput.sessionID);
			// The previous authoritative observation already issued these candidates early for this decision.
			// Re-issuing an unchanged K(a) set at the provider boundary creates duplicate prediction
			// opportunities and can restart a losing alternative after the shared winner is adopted.
			if (carried === signature) return undefined;
			if (!candidates.length) return undefined;
			return {
				id: `pattern:${startInput.turnID}`,
				source: "pattern_aware",
				revision: nextPatternRevision(startInput.sessionID, startInput.turnID),
				actions: candidates.map((candidate) =>
					patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity)),
				),
			};
		},
		continue: async ({
			startInput,
			data,
			settings,
			candidate,
			adoptedAction,
			proposalID,
			actionID,
			revision,
			feedback,
			output,
			trigger,
			signal,
		}) => {
			if (signal.aborted) return undefined;
			const context = asPatternPlanFeedback(feedback);
			if (!context) return undefined;
			const action = adoptedAction ?? candidate;
			const observation = projectPatternAwareObservation(
				output.result,
				extractOutputPaths(action.key.tool, action.input, output.result),
				options.cwd,
			);
			const next = context.store.continue(
				context.continuation,
				{
					sessionID: startInput.sessionID,
					turnID: startInput.turnID,
					tool: action.key.tool,
					input: structuredClone(action.input) as Record<string, unknown>,
					outcome: output.isError ? "failure" : "success",
					...observation,
					durationMs: candidateExecutionMs(candidate),
					schemaHash: action.key.schemaHash,
					...(typeof action.input.operation === "string" ? { operation: action.input.operation } : {}),
					learnTarget: false,
				},
				data.schemaHashes,
				trigger === "actor_adopted",
				sourcePatternSettings(settings),
			);
			if (!next.length) return undefined;
			return {
				proposalID,
				source: "pattern_aware",
				revision,
				upsert: next.map((item) =>
					patternPlanAction(item, context.store, patternPlanActionID(item.actionIdentity, actionID), [
						{ actionID, condition: "execution_succeeded" },
					]),
				),
			};
		},
		observe: async ({ startInput, data, settings, consumeInput, tool, concrete, output, durationMs, order }) => {
			const patternSettings = sourcePatternSettings(settings);
			if (!patternSettings.enabled) return undefined;
			const definition = startInput.tools.find((item) => item.name === tool);
			const observation = projectPatternAwareObservation(
				output?.result,
				extractOutputPaths(tool, concrete, output?.result),
				options.cwd,
			);
			const key = authoritativeBatchKey(consumeInput.sessionID, consumeInput.turnID);
			const batch = authoritativeBatches.get(key) ?? new Map();
			const event: PatternAwareEventInput = {
				sessionID: consumeInput.sessionID,
				turnID: consumeInput.turnID,
				tool,
				input: structuredClone(concrete),
				outcome: output?.isError ? "failure" : "success",
				...observation,
				durationMs,
				...(typeof concrete.operation === "string" ? { operation: concrete.operation } : {}),
				...(definition ? { schemaHash: stableHash(definition.parameters) } : {}),
				learnTarget: candidateToolNames(settings, actionSemantics).includes(tool),
			};
			batch.set(order, event);
			authoritativeBatches.set(key, batch);
			if (!patternSettings.multiStepEnabled) return undefined;
			await patternAnalysisTail;
			const store = await resolvePatternStore(settings);
			const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
			const candidates = store.predictAfterBatch(
				consumeInput.sessionID,
				ordered,
				data.schemaHashes,
				patternSettings,
			);
			carriedPatternPredictions.set(consumeInput.sessionID, patternPredictionSignature(candidates));
			return {
				id: `pattern:${consumeInput.turnID}`,
				source: "pattern_aware",
				revision: nextPatternRevision(consumeInput.sessionID, consumeInput.turnID),
				actions: candidates.map((candidate) =>
					patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity)),
				),
			};
		},
		onIssued: ({ feedback }) => {
			const context = asPatternPlanFeedback(feedback);
			for (const patternID of context?.patternIDs ?? []) context?.store.issued(patternID);
		},
		onSettled: ({ feedback, settlement }) => {
			const context = asPatternPlanFeedback(feedback);
			for (const patternID of context?.patternIDs ?? []) context?.store.settled(patternID, settlement);
		},
		flush: async () => {
			await patternAnalysisTail;
			if (openedPatternStore) await (await openedPatternStore).store.flush();
			if (options.patternStore) await (await options.patternStore).flush();
		},
	};

	const runtime = makeSpeculativeActionRuntime<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentConsumeInput,
		AgentStateData
	>({
		actionSemantics,
		sources: [patternSource, drafterSource],
		settings: resolveSettings,
		definitions: (input) =>
			input.tools.map((tool) => ({ name: tool.name, description: tool.description, inputSchema: tool.parameters })),
		stateData: (input) => ({
			tools: new Map(input.tools.map((tool) => [tool.name, tool])),
			schemaHashes: definitionSchemaHashes(
				input.tools.map((tool) => ({ name: tool.name, inputSchema: tool.parameters })),
			),
		}),
		actionKey: async (toolName, input, context) => {
			let tool: AgentTool | undefined;
			let validated: unknown;
			if (context.type === "consume") {
				tool = context.consumeInput.tools.find((candidate) => candidate.name === toolName);
				validated = input;
			} else {
				tool = context.data.tools.get(toolName);
				if (!tool) return undefined;
				validated = validateCandidateArguments(tool, toolName, input, "spec_key");
				if (validated === undefined) return undefined;
			}
			if (!tool) return undefined;
			let invocation: ToolInvocation | undefined;
			try {
				invocation = await options.resolveInvocation?.(toolName, validated);
			} catch {
				return undefined;
			}
			const schemaHash =
				context.type === "consume" ? stableHash(tool.parameters ?? null) : context.data.schemaHashes[toolName];
			return actionSemantics.buildKey(
				toolName,
				validated,
				options.cwd,
				schemaHash,
				invocation
					? {
							fingerprint: stableHash(invocation.identity ?? invocation),
							context: invocation,
						}
					: undefined,
			);
		},
		resolveExecution: ({ tool, action, signal }) => resolveExecutionRoute(tool, signal, action),
		actual: (input) => ({ id: input.id, tool: input.tool, input: input.args }),
		preflightCandidate: async ({ data, tool: toolName, concrete, action, route, callID, signal }) => {
			const tool = data.tools.get(toolName);
			if (!tool || !options.preflight) return { ok: false, reason: "permission_or_policy" };
			const args = validateCandidateArguments(tool, toolName, concrete, callID);
			if (args === undefined) return { ok: false, reason: "invalid_tool_call_input" };
			const result = await options.preflight({ tool, toolName, args, action, route, signal });
			return typeof result === "boolean"
				? result
					? { ok: true }
					: { ok: false, reason: "permission_or_policy" }
				: result;
		},
		authorizeCandidate: async ({ stateData, tool: toolName, concrete, action, route, signal }) => {
			const tool = stateData.tools.get(toolName);
			if (!tool || !options.preflight) return { ok: false, reason: "permission_or_policy_changed" };
			const args = validateCandidateArguments(tool, toolName, concrete, "spec_authorize");
			if (args === undefined) return { ok: false, reason: "invalid_tool_call_input" };
			const result = await options.preflight({
				tool,
				toolName,
				args,
				action,
				route,
				signal: signal ?? new AbortController().signal,
			});
			if (typeof result === "boolean") {
				return result ? { ok: true } : { ok: false, reason: "permission_or_policy_changed" };
			}
			return result.ok ? result : { ...result, reason: "permission_or_policy_changed" };
		},
		executeCandidate: async ({ data, tool: toolName, concrete, action, route, callID, signal, parentWorld }) => {
			const tool = data.tools.get(toolName);
			if (!tool) throw new Error(`Tool ${toolName} not found`);
			const args = validateCandidateArguments(tool, toolName, concrete, callID);
			if (args === undefined) throw new Error(`Invalid arguments for tool ${toolName}`);
			return executionRouter.fork(route, {
				cwd: options.cwd,
				tool,
				toolName,
				args,
				action,
				callID,
				signal,
				...(parentWorld?.checkpoint ? { parentCheckpoint: parentWorld.checkpoint } : {}),
			});
		},
		rejectCandidateOutput: ({ output }) => (output.isError ? "tool_error_result" : undefined),
		projectionRules,
		onTurnStarted: async ({ startInput, decisionSequence, settings, signal }) => {
			const key = authoritativeBatchKey(startInput.sessionID, startInput.turnID);
			authoritativeBatches.delete(key);
			patternRevisions.delete(key);
			try {
				await options.onTurnStarted?.({
					turnID: startInput.turnID,
					actorModel: startInput.actorModel,
					context: startInput.context,
					decisionSequence,
				});
			} catch {
				// Optional inference integration cannot prevent source launch or Actor execution.
			}
			void prepareExecutionWorlds(settings.tools, signal).catch(() => {
				// Turn warm-up is best-effort; route resolution remains authoritative.
			});
			if (!settings.enabled || !sourcePatternSettings(settings).enabled) {
				carriedPatternPredictions.delete(startInput.sessionID);
				return;
			}
			queuePatternAnalysis(async () => {
				const store = await resolvePatternStore(settings);
				store.observeTurn();
			});
		},
		onTurnFinished: ({ startInput, settings, terminal }) => {
			const key = authoritativeBatchKey(startInput.sessionID, startInput.turnID);
			const drafterBatch = drafterBatches.get(key);
			drafterBatches.delete(key);
			if (drafterBatch) {
				void drafterBatch
					.then((batch) => {
						drafterGate.finish(batch.utility);
					})
					.catch(() => {
						// Model/auth resolution failures are already represented by source request events.
					});
			}
			const batch = authoritativeBatches.get(key);
			authoritativeBatches.delete(key);
			patternRevisions.delete(key);
			if (terminal) carriedPatternPredictions.delete(startInput.sessionID);
			if (!settings.enabled || !sourcePatternSettings(settings).enabled) {
				carriedPatternPredictions.delete(startInput.sessionID);
				return;
			}
			const events = batch?.size
				? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event)
				: [];
			queuePatternAnalysis(async () => {
				const store = await resolvePatternStore(settings);
				if (events.length) store.observeBatch(events);
				store.observeTurn();
				if (terminal) store.finishSession(startInput.sessionID);
			});
		},
		onCandidateMaterialized: options.onCandidateMaterialized,
		onEvent: async (event) => {
			if (
				event.type === "actor_action" &&
				event.candidate?.source === "drafter" &&
				event.settlement.provider.kind === "speculative" &&
				event.settlement.matchedPredictions.some(
					(prediction) =>
						prediction.source === "drafter" && prediction.proposalID.startsWith(`drafter:${event.turnID}:`),
				)
			) {
				const batch = drafterBatches.get(authoritativeBatchKey(event.sessionID, event.turnID));
				if (batch) {
					try {
						drafterGate.creditExecutionAhead(
							(await batch).utility,
							event.settlement.provider.timing.executionAheadMs,
						);
					} catch {
						// Source resolution failures cannot own an adopted speculative candidate.
					}
				}
			}
			await options.onEvent?.(event);
		},
	});

	return {
		sessionID,
		runtime,
		startTurn: (input, signal) => runtime.startTurn({ ...input, sessionID }, signal),
		previewActorTool: (input, signal) => runtime.previewActorTool({ ...input, sessionID }, signal),
		previewActorCall: (input, signal) => runtime.previewActorCall({ ...input, sessionID }, signal),
		consume: (input, signal) => runtime.consume({ ...input, sessionID }, signal),
		actual: (input) => runtime.actual({ ...input, sessionID }),
		drafterGateSnapshot: () => drafterGate.snapshot(),
		finishTurn: async (turnID, terminal = false) => {
			await runtime.finishTurn({ sessionID, turnID, tool: "", args: {}, tools: [], terminal });
			if (terminal) await finishPatternSession();
		},
		dispose: async () => {
			try {
				await runtime.releaseSession(sessionID);
				await finishPatternSession();
			} finally {
				try {
					if (openedPatternStore) {
						try {
							await (await openedPatternStore).release();
						} catch {
							// Persistence failure must not change Agent uninstall semantics.
						}
					}
				} finally {
					await executionRouter.dispose();
				}
			}
		},
	};
}

function validateCandidateArguments(
	tool: AgentTool,
	toolName: string,
	input: unknown,
	callID: string,
): unknown | undefined {
	try {
		const prepared = tool.prepareArguments ? tool.prepareArguments(input) : input;
		const toolCall: AgentToolCall = {
			type: "toolCall",
			id: callID,
			name: toolName,
			arguments: prepared as Record<string, unknown>,
		};
		return validateToolArguments(tool, toolCall);
	} catch {
		return undefined;
	}
}

function definitionSchemaHashes(
	definitions: readonly { readonly name: string; readonly inputSchema?: unknown }[],
): Readonly<Record<string, string>> {
	return Object.fromEntries(
		definitions.map((definition) => [definition.name, stableHash(definition.inputSchema ?? null)]),
	);
}

function stableHash(value: unknown): string {
	return createHash("sha256").update(stableStringify(value)).digest("hex").slice(0, 32);
}

function extractOutputPaths(
	tool: string,
	input: Readonly<Record<string, unknown>>,
	result: AgentToolResult<unknown> | undefined,
): readonly string[] | undefined {
	if ((tool !== "find" && tool !== "grep") || !result) return undefined;
	const searchRoot = typeof input.path === "string" && input.path ? input.path : ".";
	const text = result.content
		.filter((item): item is Extract<(typeof result.content)[number], { type: "text" }> => item.type === "text")
		.map((item) => item.text)
		.join("\n");
	const paths = text
		.split(/\r?\n/)
		.map((line) => {
			const trimmed = line.trim();
			if (!trimmed || trimmed.startsWith("[") || /^No files found\b/.test(trimmed)) return undefined;
			if (tool === "find") return trimmed;
			return /^(.*?):\d+(?::\d+)?:/.exec(trimmed)?.[1];
		})
		.filter((item): item is string => typeof item === "string" && item.length > 0)
		.map((item) => {
			if (path.isAbsolute(item)) return item;
			if (tool === "grep" && path.basename(searchRoot) === item) return searchRoot;
			return path.join(searchRoot, item);
		});
	return paths.length ? [...new Set(paths)] : undefined;
}

function normalizeTimeout(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0
		? Math.floor(value)
		: DEFAULTS.predictionTimeoutMs;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}
