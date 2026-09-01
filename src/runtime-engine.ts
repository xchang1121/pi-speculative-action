import { isDeepStrictEqual } from "node:util";

import type { ActionProjectionCoverage, ActionProjectionRule } from "./action-key-projection.ts";
import type {
	ActionKey,
	ActionKeyMatch,
	ActionSemanticsRegistry,
	ProjectedActionKeyMatch,
} from "./action-semantics.ts";
import { actionKeyCovers, actionKeyMatch, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { ActorCallAttempt } from "./actor-call-attempt.ts";
import { ActorAction } from "./actor-action.ts";
import { CandidateExecution, type CandidateReservation } from "./candidate-execution.ts";
import { ActionStore, ResultCache, type ResultCacheEvidence, speculativeCacheValue } from "./candidate-stores.ts";
import { clampCandidateLimit, DEFAULTS, type DrafterToolDefinition } from "./common.ts";
import { diagnosticAction } from "./diagnostics.ts";
import type { CandidateEventDescriptor, CandidateExecutionProjection } from "./events.ts";
import { type SpeculativeExecutionRoute, sameSpeculativeExecutionRoute, type WorldBranch } from "./execution-world.ts";
import type { PlanUpdate } from "./plan-proposal.ts";
import { PlanRuntime, type PlanRuntimeNode, type PredictionOpportunity, type RetiredPlanNode } from "./plan-runtime.ts";
import { BoundedEventQueue, PostSettlementQueue } from "./post-settlement.ts";
import { actionResourceProfile, resourceProfile } from "./resource-budget.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type {
	AdoptedAction,
	AuthoritativeResultCapture,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionRuntimeAdapter,
	SpeculativeActionSettings,
	SpeculativeCacheSnapshot,
	SpeculativeCandidate,
	SpeculativeDraftCandidate,
	SpeculativePlanSource,
	SpeculativeRuntimeInspection,
} from "./runtime.ts";
import {
	type PredictionForecast,
	type ServiceTimingIdentity,
	SpeculationScheduler,
} from "./scheduler.ts";
import type {
	ActorActionIdentity,
	PlanActionIdentity,
	PredictionAdoption,
	PredictionSettlement,
	ResolutionCause,
	ResourceValidation,
	SettledSourceRequest,
	SourceRequestKind,
} from "./settlement.ts";
import { cause, zeroValidationMetrics } from "./settlement.ts";
import { runSourceRequest, SourceGeneration, type SourceRequestResult } from "./source-request.ts";
import { measureSpeculativeTask, type TimelineInterval } from "./task-timing.ts";

interface TurnInput<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly terminal?: boolean;
}

class CandidateFailure extends Error {
	readonly failure: ResolutionCause;

	constructor(failure: ResolutionCause) {
		super(failure.detail ?? failure.code);
		this.failure = failure;
	}
}

function uniqueProjectionRules<Output>(
	rules: readonly ActionProjectionRule<Output>[],
	semantics: ActionSemanticsRegistry,
): readonly ActionProjectionRule<Output>[] {
	const unique = new Map<string, ActionProjectionRule<Output>>();
	for (const rule of rules) {
		if (semantics.supportsProjector(rule.id) && !unique.has(rule.id)) unique.set(rule.id, rule);
	}
	return [...unique.values()];
}

function coveringAction<Output>(predicted: ActionKey, rules: readonly ActionProjectionRule<Output>[]): ActionKey {
	for (const rule of rules) {
		try {
			const covering = rule.coveringAction?.(predicted);
			if (covering && actionKeyCovers(covering, predicted, [rule])) return covering;
		} catch {
			// A projection optimization cannot change the predicted action.
		}
	}
	return predicted;
}

function asUpdates(value: PlanUpdate | readonly PlanUpdate[] | undefined): readonly PlanUpdate[] {
	return value === undefined ? [] : Array.isArray(value) ? value : [value as PlanUpdate];
}

function reservationAvailable(reservation: CandidateReservation): boolean {
	return reservation.kind === "shared" ? reservation.owners.length === 0 : reservation.status === "available";
}

function updateSource(update: PlanUpdate): string {
	return "actions" in update ? update.source : update.source;
}

function planUpdateID(update: PlanUpdate): string {
	return "actions" in update ? update.id : update.proposalID;
}

function immediateOnly(update: PlanUpdate): PlanUpdate {
	if ("actions" in update) {
		return {
			...update,
			actions: update.actions.filter(
				(action) => finiteMetric(action.horizon) === 0 && (action.dependsOn?.length ?? 0) === 0,
			),
		};
	}
	return { ...update, upsert: [], remove: update.remove };
}

function concurrentLimit(settings: SpeculativeActionSettings): number {
	const value = settings.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions;
	return Number.isFinite(value) ? Math.max(1, Math.floor(value)) : 1;
}

function cacheEntryLimit(settings: SpeculativeActionSettings): number {
	return Number.isFinite(settings.resourceCacheMaxEntries)
		? Math.max(1, Math.floor(settings.resourceCacheMaxEntries))
		: 1;
}

function cacheByteLimit(settings: SpeculativeActionSettings): number {
	return typeof settings.resourceCacheMaxBytes === "number" && Number.isFinite(settings.resourceCacheMaxBytes)
		? Math.max(1, Math.floor(settings.resourceCacheMaxBytes))
		: DEFAULTS.resourceCacheMaxBytes;
}

function cacheLimits(settings: SpeculativeActionSettings) {
	return { maxEntries: cacheEntryLimit(settings), maxBytes: cacheByteLimit(settings), hotFraction: 0.8 };
}

function forecastFor(
	node: PlanRuntimeNode,
	route: SpeculativeExecutionRoute,
	decisionSequence: number,
	actorPhase?: PredictionForecast["actorPhase"],
): PredictionForecast {
	return {
		tool: node.action.tool,
		execution: route.isolation,
		...(node.actionKey
			? { executionFingerprint: node.actionKey.executionFingerprint, actionKeyHash: node.actionKey.hash }
			: {}),
		...(node.action.expectedDurationMs !== undefined ? { expectedDurationMs: node.action.expectedDurationMs } : {}),
		...(node.action.resourceDemand !== undefined ? { resourceDemand: node.action.resourceDemand } : {}),
		decisionBatchesUntilCall: Math.max(0, node.expectedDecisionSeq - decisionSequence),
		...(actorPhase ? { actorPhase } : {}),
		criticalPathMs: node.criticalPathMs,
		...(node.action.expectedLatencyBenefitMs !== undefined
			? { expectedLatencyBenefitMs: node.action.expectedLatencyBenefitMs }
			: {}),
		...(node.action.background ? { background: true } : {}),
		...((node.action.dependsOn?.length ?? 0) > 0 && (node.action.horizon ?? 0) <= 0
			? { dependenciesResolved: true }
			: {}),
	};
}

function planActionDraft(node: PlanRuntimeNode): SpeculativeDraftCandidate {
	return {
		type: "tool_call",
		tool: node.action.tool,
		input: node.action.input,
		...(node.action.diagnostic ? { diagnostic: node.action.diagnostic } : {}),
		source: node.source,
		proposalID: node.proposalID,
		actionID: node.action.id,
		feedback: node.action.feedback,
		...(node.action.dependsOn ? { dependsOn: node.action.dependsOn } : {}),
		...(node.action.horizon !== undefined ? { horizon: node.action.horizon } : {}),
		...(node.action.latestHorizon !== undefined ? { latestHorizon: node.action.latestHorizon } : {}),
		...(node.action.empiricalProbability !== undefined
			? { empiricalProbability: node.action.empiricalProbability }
			: {}),
		...(node.action.conditionalProbability !== undefined
			? { conditionalProbability: node.action.conditionalProbability }
			: {}),
		...(node.action.expectedDurationMs !== undefined ? { expectedDurationMs: node.action.expectedDurationMs } : {}),
		...(node.action.expectedLatencyBenefitMs !== undefined
			? { expectedLatencyBenefitMs: node.action.expectedLatencyBenefitMs }
			: {}),
		...(node.action.resourceDemand !== undefined ? { resourceDemand: node.action.resourceDemand } : {}),
		...(node.action.depth !== undefined ? { depth: node.action.depth } : {}),
	};
}

function asConcreteInput(value: unknown): Record<string, unknown> | undefined {
	if (value === undefined || value === null) return {};
	return typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}

function publicCandidate<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): SpeculativeCandidate {
	const execution = candidate.work.execution;
	return {
		id: candidate.id,
		key: candidate.key,
		tool: candidate.key.tool,
		input: candidate.key.input,
		...("executionMs" in execution ? { work: { execution: { executionMs: execution.executionMs } } } : {}),
		...(candidate.owner.draft.source ? { source: candidate.owner.draft.source } : {}),
	};
}

function predictionCandidate<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
	node: PlanRuntimeNode,
): SpeculativeCandidate {
	return {
		...publicCandidate(candidate),
		source: node.source,
		empiricalProbability: node.action.empiricalProbability,
		conditionalProbability: node.action.conditionalProbability,
		depth: node.action.depth,
		planDependencies: node.action.dependsOn,
	} as unknown as SpeculativeCandidate;
}

function activeExecution<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): boolean {
	return candidate.work.execution.status !== "failed" && candidate.work.execution.status !== "cancelled";
}

function candidateBranch<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): WorldBranch<Output> | undefined {
	const execution = candidate.work.execution;
	return execution.status === "succeeded" ? execution.output : undefined;
}

function canShareInFlight<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
	actor: ActionKey,
	match: ProjectedActionKeyMatch,
	rules: readonly ActionProjectionRule<Output>[],
): boolean {
	if (!activeExecution(candidate)) return false;
	const rule = rules.find((item) => item.id === match.projector);
	return rule?.canShareInFlight?.(candidate.key, actor) === true;
}

function captureCoverage<Output>(
	action: ActionKey,
	output: Output,
	rules: readonly ActionProjectionRule<Output>[],
): readonly ActionProjectionCoverage[] {
	return rules.flatMap((rule) => {
		try {
			const value = rule.captureCoverage(action, output);
			return value === undefined ? [] : [{ rule: rule.id, value }];
		} catch {
			return [];
		}
	});
}

async function projectOutput<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
	actor: ActionKey,
	output: Output,
	match: ActionKeyMatch,
	rules: readonly ActionProjectionRule<Output>[],
): Promise<ProjectionResult<Output>> {
	if (match.kind === "exact") return { ok: true, output, durationMs: 0 };
	const rule = rules.find((item) => item.id === match.projector);
	if (!rule) return { ok: false, cause: cause("projection", "rule_missing") };
	const coverage = candidate.projectionCoverage.find((item) => item.rule === rule.id);
	if (!coverage) return { ok: false, cause: cause("projection", "coverage_missing") };
	const startedAt = performance.now();
	try {
		const projected = await rule.projectOutput({
			speculative: candidate.key,
			actor,
			output,
			coverage: coverage.value,
			keyMatch: match,
		});
		const durationMs = Math.max(0, performance.now() - startedAt);
		return projected === undefined
			? { ok: false, cause: cause("projection", "view_not_covered") }
			: { ok: true, output: projected, durationMs };
	} catch (error) {
		return { ok: false, cause: cause("projection", "reconstruction_failed", errorDetail(error)) };
	}
}

type CandidateWaitResult<T> =
	| { readonly status: "completed"; readonly value: T }
	| { readonly status: "aborted" }
	| { readonly status: "deadline" };

async function waitForCandidate<T>(
	promise: Promise<T>,
	signal?: AbortSignal,
	waitBudgetMs?: number,
): Promise<CandidateWaitResult<T>> {
	if (signal?.aborted) return { status: "aborted" };
	const bounded = waitBudgetMs !== undefined && Number.isFinite(waitBudgetMs);
	if (!signal && !bounded) return { status: "completed", value: await promise };
	return new Promise((resolve) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const finish = (result: CandidateWaitResult<T>) => {
			if (settled) return;
			settled = true;
			if (timer) clearTimeout(timer);
			signal?.removeEventListener("abort", aborted);
			resolve(result);
		};
		const aborted = () => finish({ status: "aborted" });
		signal?.addEventListener("abort", aborted, { once: true });
		if (bounded) timer = setTimeout(() => finish({ status: "deadline" }), Math.max(0, waitBudgetMs!));
		void promise.then((value) => finish({ status: "completed", value }));
	});
}

function registerActorAction<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
	callID: string | undefined,
	action: ActorAction,
): void {
	if (callID) session.actorCalls.set(callKey(action.identity.turnID, callID), action);
	else session.anonymousActorCalls.push(action);
}

function takeActorAction<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
	turnID: string,
	call: ActualToolCall,
): ActorAction | undefined {
	if (call.id) {
		const key = callKey(turnID, call.id);
		const action = session.actorCalls.get(key);
		session.actorCalls.delete(key);
		return action;
	}
	const index = session.anonymousActorCalls.findIndex(
		(action) =>
			action.identity.turnID === turnID && action.tool === call.tool && action.state.status === "awaiting_fallback",
	);
	return index < 0 ? undefined : session.anonymousActorCalls.splice(index, 1)[0];
}

function forgetActorAction<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
	callID: string | undefined,
	action: ActorAction,
): void {
	disposeActorCapture(session, action);
	if (callID) {
		session.actorCalls.delete(callKey(action.identity.turnID, callID));
		return;
	}
	const index = session.anonymousActorCalls.indexOf(action);
	if (index >= 0) session.anonymousActorCalls.splice(index, 1);
}

function clearActorActions<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
	turnID?: string,
): void {
	if (turnID === undefined) {
		for (const capture of session.authoritativeResultCaptures.values()) disposeAuthoritativeResultCapture(capture);
		session.authoritativeResultCaptures.clear();
		session.actorCalls.clear();
		session.anonymousActorCalls.length = 0;
		return;
	}
	for (const [key, action] of session.actorCalls) {
		if (action.identity.turnID !== turnID) continue;
		disposeActorCapture(session, action);
		session.actorCalls.delete(key);
	}
	for (let index = session.anonymousActorCalls.length - 1; index >= 0; index--) {
		const action = session.anonymousActorCalls[index];
		if (action?.identity.turnID !== turnID) continue;
		disposeActorCapture(session, action);
		session.anonymousActorCalls.splice(index, 1);
	}
}

function disposeActorCapture<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
	action: ActorAction,
): void {
	const capture = session.authoritativeResultCaptures.get(action);
	if (!capture) return;
	session.authoritativeResultCaptures.delete(action);
	disposeAuthoritativeResultCapture(capture);
}

function disposeAuthoritativeResultCapture<Output>(capture: AuthoritativeResultCapture<Output>): void {
	try {
		void Promise.resolve(capture.dispose()).catch(() => undefined);
	} catch {
		// Capture cleanup cannot alter Actor execution or settlement.
	}
}

function callKey(turnID: string, callID: string): string {
	return JSON.stringify([turnID, callID]);
}

function sameActualToolCall(left: ActualToolCall, right: ActualToolCall): boolean {
	return left.id === right.id && left.tool === right.tool && isDeepStrictEqual(left.input, right.input);
}

function closeActorPhase<SessionID, Output, StartInput, StateData>(
	turn: TurnState<SessionID, Output, StartInput, StateData>,
	completedAt: number,
): void {
	if (turn.actorPhaseCompletedAt !== undefined) return;
	turn.actorPhaseCompletedAt = Math.max(turn.startedAt, completedAt);
	turn.session.actorPhaseIntervals.push({ startedAt: turn.startedAt, completedAt: turn.actorPhaseCompletedAt });
}

function enterActorAdmission<SessionID, Output, StartInput, StateData>(
	session: SessionState<SessionID, Output, StartInput, StateData>,
): { readonly ready: Promise<void>; readonly release: () => void } {
	const ready = session.actorAdmissionTail;
	let unlock!: () => void;
	session.actorAdmissionTail = new Promise<void>((resolve) => {
		unlock = resolve;
	});
	let released = false;
	return {
		ready,
		release: () => {
			if (released) return;
			released = true;
			unlock();
		},
	};
}

function turnKey<SessionID>(sessionID: SessionID, turnID: string): string {
	return JSON.stringify([String(sessionID), turnID]);
}

function outputIsError(value: unknown): boolean {
	return Boolean(value && typeof value === "object" && (value as { readonly isError?: unknown }).isError === true);
}

function candidateEventDescriptor<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): CandidateEventDescriptor {
	return {
		source: candidate.owner.draft.source ?? "cache",
		depth: candidate.owner.draft.depth ?? 0,
		id: candidate.id,
		origin: candidate.origin,
		tool: candidate.key.tool,
		actionKeyHash: candidate.key.hash,
		execution: candidate.route.isolation,
		predictedAction: diagnosticAction(candidate.key.tool, candidate.key.input, candidate.key),
		predictionLatencyMs: candidate.predictionLatencyMs,
		draftTokens: candidate.draftTokens,
		totalDraftTokens: candidate.totalDraftTokens,
		expectedDurationMs: candidate.expectedDurationMs,
		estimatedBytes: candidate.estimatedBytes,
		validation: {
			durationMs: candidate.validationMs,
			bytesRead: candidate.validationBytes,
			filesRead: candidate.validationFiles,
			...(candidate.validationMode ? { mode: candidate.validationMode } : {}),
		},
	};
}

function candidateExecutionProjection<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): CandidateExecutionProjection | undefined {
	const state = candidate.work.execution;
	if (state.status === "queued") return undefined;
	if (state.status === "running") return { status: "running", startedAt: state.startedAt };
	if (state.status === "succeeded") {
		return {
			status: "succeeded",
			startedAt: state.startedAt,
			completedAt: state.completedAt,
			executionMs: state.executionMs,
		};
	}
	return {
		status: state.status,
		cause: state.cause,
		...(state.startedAt !== undefined ? { startedAt: state.startedAt } : {}),
		completedAt: state.completedAt,
		executionMs: state.executionMs,
	};
}

function candidateCacheValue<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
	evidence: ResultCacheEvidence,
	now: number,
): number {
	const execution = candidate.work.execution;
	const reuseSamples = Math.max(1, evidence.actorHits);
	return speculativeCacheValue(
		{
			executionMs: "executionMs" in execution ? execution.executionMs : candidate.expectedDurationMs,
			expectedValidationMs: candidate.validationMs / reuseSamples,
			expectedProjectionMs: candidate.projectionMs / reuseSamples,
			bytes: candidate.estimatedBytes,
			actorHits: evidence.actorHits,
			insertedAt: evidence.insertedAt,
			...(evidence.lastActorHitAt ? { lastActorHitAt: evidence.lastActorHitAt } : {}),
		},
		now,
	);
}

function executionDuration<Output, StartInput, StateData>(
	candidate: CandidateRecord<Output, StartInput, StateData>,
): number {
	const execution = candidate.work.execution;
	return "executionMs" in execution ? execution.executionMs : 0;
}

function estimateValueBytes(value: unknown, seen = new WeakSet<object>()): number {
	if (value === null || value === undefined) return 0;
	if (typeof value === "string") return value.length * 2;
	if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") return 8;
	if (typeof value !== "object" || seen.has(value)) return 0;
	seen.add(value);
	if (ArrayBuffer.isView(value)) return value.byteLength;
	if (value instanceof ArrayBuffer) return value.byteLength;
	if (Array.isArray(value)) return value.reduce((sum, item) => sum + estimateValueBytes(item, seen), 0);
	return Object.entries(value).reduce((sum, [key, item]) => sum + key.length * 2 + estimateValueBytes(item, seen), 0);
}

function resourcePathsOverlap(left: string, right: string): boolean {
	const normalize = (value: string) => value.replace(/\\/g, "/").replace(/\/+$/, "").toLowerCase();
	const a = normalize(left);
	const b = normalize(right);
	return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
}

function uniqueCandidates<T extends object>(candidates: readonly T[]): T[] {
	return [...new Set(candidates)];
}

function maybe<T>(value: T | undefined): T[] {
	return value === undefined ? [] : [value];
}

function finiteMetric(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

function actionTimingIdentity(action: ActionKey): ServiceTimingIdentity {
	return {
		tool: action.tool,
		executionFingerprint: action.executionFingerprint,
		actionKeyHash: action.hash,
	};
}

function errorDetail(error: unknown): string {
	return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

interface ActualToolCall {
	readonly id?: string;
	readonly tool: string;
	readonly input: unknown;
}

interface PlanActionContext<StartInput, StateData> {
	readonly identity: PlanActionIdentity;
	readonly opportunity: PredictionOpportunity;
	feedback: unknown;
	readonly startInput: StartInput;
	readonly data: StateData;
	readonly settings: SpeculativeActionSettings;
	readonly attemptStartedAt: number;
	readonly predictionLatencyMs: number;
	readonly draftTokens: number;
	readonly totalDraftTokens: number;
	draft: SpeculativeDraftCandidate;
	readonly admissionSignal: AbortSignal;
	readonly sourceSlot?: SourceRequestSlot;
	readonly continuationSlots: Set<SourceRequestSlot>;
	readonly continuationTriggers: Set<"execution_succeeded" | "actor_adopted">;
	continuationTail: Promise<void>;
	executionRoute?: SpeculativeExecutionRoute;
}

/** One producer request from the bounded budget for a future Actor decision. */
interface SourceRequestSlot {
	readonly source: string;
	readonly targetDecisionSequence: number;
	readonly requestKind: SourceRequestKind;
	readonly expiresAtTarget: boolean;
	readonly generations: Set<SourceGeneration>;
	readonly owners: Set<string>;
	pendingRequests: number;
	active: boolean;
}

interface PlanAdmissionScope<SessionID, Output, StartInput, StateData> {
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly startInput: StartInput;
	readonly data: StateData;
	readonly settings: SpeculativeActionSettings;
	readonly signal: AbortSignal;
	readonly slot?: SourceRequestSlot;
}

interface CandidateRecord<Output, StartInput, StateData> {
	readonly id: string;
	readonly origin: "prediction" | "actor_preview" | "actor_result";
	readonly key: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly work: CandidateExecution<WorldBranch<Output>>;
	readonly worldParent?: CandidateRecord<Output, StartInput, StateData>;
	actorAdopted: boolean;
	readonly owner: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly draft: SpeculativeDraftCandidate;
		readonly index: number;
	};
	readonly createdAt: number;
	readonly attemptStartedAt: number;
	readonly predictionLatencyMs: number;
	readonly draftTokens: number;
	readonly totalDraftTokens: number;
	expectedDurationMs: number;
	expectedDecisionSeq: number;
	criticalPathMs: number;
	priorityMs: number;
	background: boolean;
	estimatedBytes: number;
	projectionCoverage: readonly ActionProjectionCoverage[];
	validationMs: number;
	validationBytes: number;
	validationFiles: number;
	validationMode?: "watcher" | "exact";
	projectionMs: number;
}

type ActorPreviewState =
	| { readonly status: "pending" }
	| { readonly status: "candidate"; readonly candidateID: string; readonly ownership: "existing" | "preview" }
	| { readonly status: "cancelled" };

interface ActorPreviewRecord {
	readonly call: ActualToolCall;
	readonly actionKey: Promise<ActionKey | undefined>;
	readonly actionKeyPending: () => boolean;
	task: Promise<void>;
	state: ActorPreviewState;
}

interface SessionState<SessionID, Output, StartInput, StateData> {
	readonly id: SessionID;
	readonly lifecycle: RuntimeLifecycleLane;
	readonly plan: PlanRuntime;
	readonly scheduler: SpeculationScheduler<CandidateRecord<Output, StartInput, StateData>>;
	readonly effects: PostSettlementQueue;
	readonly events: BoundedEventQueue<SpeculativeActionEvent<SessionID>>;
	readonly actionContexts: Map<string, PlanActionContext<StartInput, StateData>>;
	readonly launchTimers: Map<string, ReturnType<typeof setTimeout>>;
	readonly sourceSlots: Set<SourceRequestSlot>;
	readonly sourceTasks: Set<Promise<void>>;
	readonly actorCalls: Map<string, ActorAction>;
	readonly anonymousActorCalls: ActorAction[];
	readonly authoritativeResultCaptures: Map<ActorAction, AuthoritativeResultCapture<Output>>;
	readonly turns: Set<string>;
	readonly actorPhaseIntervals: TimelineInterval[];
	readonly authoritativeToolIntervals: TimelineInterval[];
	readonly authoritativeCandidateIDs: Set<string>;
	actorAdmissionTail: Promise<void>;
	readonly planAdmissionTails: Map<string, Promise<void>>;
	settings: SpeculativeActionSettings;
	taskStartedAt?: number;
	lastActorArrivedAt?: number;
	sequence: number;
	decisionSequence: number;
	tokenTotal: number;
	candidateSequence: number;
	sourceRequestSequence: number;
	pendingSourceRequests: number;
	pendingAdmissions: number;
}

interface TurnState<SessionID, Output, StartInput, StateData> {
	readonly key: string;
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly startInput: StartInput;
	readonly startedAt: number;
	readonly data: StateData;
	readonly settings: SpeculativeActionSettings;
	readonly definitions: readonly DrafterToolDefinition[];
	readonly candidateNames: readonly string[];
	readonly generation: SourceGeneration;
	readonly decisionSequence: number;
	readonly actorActions: ActorAction[];
	readonly actorToolHints: Set<string>;
	readonly actorPreviews: Map<string, ActorPreviewRecord>;
	actorDecisionStartedAt: number;
	actorArrivedAt?: number;
	actorPhaseCompletedAt?: number;
	lifecycle: "active" | "closing" | "finished";
}

interface RankedCandidate<Output, StartInput, StateData> {
	readonly candidate: CandidateRecord<Output, StartInput, StateData>;
	readonly match: ActionKeyMatch;
	readonly ready: boolean;
	readonly remainingMs: number;
}

interface ClaimedPrediction {
	readonly node: PlanRuntimeNode;
	readonly opportunity: PredictionOpportunity;
}

type ProjectionResult<Output> =
	| { readonly ok: true; readonly output: Output; readonly durationMs: number }
	| { readonly ok: false; readonly cause: ResolutionCause };

const RUNTIME_EVENT_QUEUE_CAPACITY = 256;

/** Structural runtime: plans own predictions, candidates own execution, ActorAction owns adoption. */
export function makeStructuralSpeculativeActionRuntime<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	ConsumeInput extends TurnInput<SessionID>,
	FinishInput extends TurnInput<SessionID>,
	StateData,
>(
	adapter: SpeculativeActionRuntimeAdapter<SessionID, Output, StartInput, ConsumeInput, StateData>,
): SpeculativeActionRuntime<SessionID, Output, StartInput, ConsumeInput, FinishInput> {
	type Source = SpeculativePlanSource<SessionID, Output, StartInput, ConsumeInput, StateData>;
	type Candidate = CandidateRecord<Output, StartInput, StateData>;
	type Session = SessionState<SessionID, Output, StartInput, StateData>;
	type Turn = TurnState<SessionID, Output, StartInput, StateData>;
	type TurnClosure = {
		readonly state: Turn;
		readonly completedAt: number;
		readonly terminal: boolean;
		readonly notifyHost: boolean;
	};
	type SessionClosureMode = "terminal" | "disabled" | "disposed";
	type ActorSelectionInput = {
		readonly state: Turn;
		readonly consumeInput: ConsumeInput;
		readonly actualCall: ActualToolCall;
		readonly actualKey: ActionKey;
		readonly attempt: ActorCallAttempt<Candidate, Output>;
		readonly ranked: readonly RankedCandidate<Output, StartInput, StateData>[];
		readonly actorArrivedAt: number;
		readonly signal?: AbortSignal;
	};

	const semantics = adapter.actionSemantics ?? PI_ACTION_SEMANTICS;
	const sources = adapter.sources ?? [];
	const sourcesByID = new Map<string, Source>();
	for (const source of sources) {
		if (!source.id || source.id.trim() !== source.id) throw new Error(`invalid speculative plan source ${source.id}`);
		if (sourcesByID.has(source.id)) throw new Error(`duplicate speculative plan source ${source.id}`);
		sourcesByID.set(source.id, source);
	}
	const projectionRules = uniqueProjectionRules(adapter.projectionRules ?? [], semantics);
	const projectors = projectionRules;
	const jobs = new ActionStore<SessionID, Candidate>(projectors, true);
	const results = new ResultCache<SessionID, Candidate>(projectors, candidateCacheValue);
	const branches = new ActionStore<SessionID, Candidate>([], true);
	const sessions = new Map<SessionID, Session>();
	const turns = new Map<string, Turn>();
	const disposedBranches = new WeakSet<WorldBranch<Output>>();
	let masterEnabled: boolean | undefined;
	const masterDisabled = (): boolean => masterEnabled === false;
	const disposeBranch = (branch: WorldBranch<Output> | undefined): void => {
		if (!branch || disposedBranches.has(branch)) return;
		disposedBranches.add(branch);
		try {
			void Promise.resolve(branch.dispose()).catch(() => undefined);
		} catch {
			// Cleanup cannot change authoritative settlement.
		}
	};

	const sessionFor = (sessionID: SessionID, settings: SpeculativeActionSettings): Session => {
		const current = sessions.get(sessionID);
		if (current) return current;
		const created: Session = {
			id: sessionID,
			lifecycle: new RuntimeLifecycleLane(),
			plan: new PlanRuntime(),
			scheduler: new SpeculationScheduler<Candidate>(),
			effects: new PostSettlementQueue(),
			events: new BoundedEventQueue(RUNTIME_EVENT_QUEUE_CAPACITY, emit),
			actionContexts: new Map(),
			launchTimers: new Map(),
			sourceSlots: new Set(),
			sourceTasks: new Set(),
			actorCalls: new Map(),
			anonymousActorCalls: [],
			authoritativeResultCaptures: new Map(),
			turns: new Set(),
			actorPhaseIntervals: [],
			authoritativeToolIntervals: [],
			authoritativeCandidateIDs: new Set(),
			actorAdmissionTail: Promise.resolve(),
			planAdmissionTails: new Map(),
			settings,
			sequence: 0,
			decisionSequence: 0,
			tokenTotal: 0,
			candidateSequence: 0,
			sourceRequestSequence: 0,
			pendingSourceRequests: 0,
			pendingAdmissions: 0,
		};
		sessions.set(sessionID, created);
		return created;
	};

	const clearLaunchTimers = (session: Session): void => {
		for (const timer of session.launchTimers.values()) clearTimeout(timer);
		session.launchTimers.clear();
	};

	const candidateNames = (settings: SpeculativeActionSettings): readonly string[] => {
		const known = new Set(semantics.toolNames());
		return [...new Set(settings.tools)].filter((tool) => known.has(tool));
	};

	const startTurn = async (input: StartInput, signal?: AbortSignal): Promise<void> => {
		const settings = await adapter.settings();
		if (!settings.enabled || masterDisabled()) {
			await disableSession(input.sessionID);
			return;
		}
		if (signal?.aborted) return;
		const definitions = adapter.definitions(input);
		const names = candidateNames(settings);
		if (!definitions.length || !names.length) return;
		const key = turnKey(input.sessionID, input.turnID);
		const previous = turns.get(key);
		if (previous) await finishState(previous, false);
		const session = sessionFor(input.sessionID, settings);
		await session.lifecycle.run(async () => {
			if (signal?.aborted || masterDisabled()) return;
			const data = await adapter.stateData(input);
			if (signal?.aborted || masterDisabled() || session.lifecycle.sealed) return;
			session.settings = settings;
			const generation = new SourceGeneration(signal);
			const startedAt = performance.now();
			if (session.taskStartedAt === undefined) {
				session.taskStartedAt = startedAt;
				session.actorPhaseIntervals.length = 0;
				session.authoritativeToolIntervals.length = 0;
				session.authoritativeCandidateIDs.clear();
			}
			const state: Turn = {
				key,
				session,
				sessionID: input.sessionID,
				turnID: input.turnID,
				startInput: input,
				startedAt,
				data,
				settings,
				definitions,
				candidateNames: names,
				generation,
				decisionSequence: session.decisionSequence + 1,
				actorActions: [],
				actorToolHints: new Set(),
				actorPreviews: new Map(),
				actorDecisionStartedAt: startedAt,
				lifecycle: "active",
			};
			turns.set(key, state);
			session.turns.add(key);
			await reconcileStores(state);
			try {
				await adapter.onTurnStarted?.({
					startInput: input,
					decisionSequence: state.decisionSequence,
					settings,
					definitions,
					candidateNames: names,
					...(signal ? { signal } : {}),
				});
			} catch {
				// Host analysis does not own runtime state.
			}
			state.actorDecisionStartedAt = performance.now();
			dispatchReady(session);
			setTimeout(() => {
				if (state.lifecycle === "active" && state.actorArrivedAt === undefined && !state.generation.signal.aborted)
					launchSourceRequests(state);
			}, 0);
		});
	};

	const claimSourceSlot = (
		session: Session,
		source: string,
		targetDecisionSequence: number,
		limit: number,
		requestKind: SourceRequestKind,
		expiresAtTarget = true,
	): SourceRequestSlot | undefined => {
		const used = [...session.sourceSlots].filter(
			(slot) => slot.active && slot.source === source && slot.targetDecisionSequence === targetDecisionSequence,
		).length;
		if (used >= limit) return undefined;
		const slot: SourceRequestSlot = {
			source,
			targetDecisionSequence,
			requestKind,
			expiresAtTarget,
			generations: new Set(),
			owners: new Set(),
			pendingRequests: 0,
			active: true,
		};
		session.sourceSlots.add(slot);
		return slot;
	};

	const releaseSourceSlot = (session: Session, slot: SourceRequestSlot, failure: ResolutionCause): void => {
		if (!slot.active) return;
		slot.active = false;
		session.sourceSlots.delete(slot);
		for (const generation of slot.generations) generation.expire(failure);
		slot.generations.clear();
	};

	const releaseUnusedSourceSlot = (session: Session, slot: SourceRequestSlot): void => {
		if (slot.pendingRequests === 0 && slot.owners.size === 0) {
			releaseSourceSlot(session, slot, cause("control", "source_slot_unused"));
		}
	};

	const retainSourceRequest = (slot: SourceRequestSlot): void => {
		slot.pendingRequests++;
	};

	const releaseSourceRequest = (session: Session, slot: SourceRequestSlot): void => {
		slot.pendingRequests = Math.max(0, slot.pendingRequests - 1);
		releaseUnusedSourceSlot(session, slot);
	};

	const cancelCompetingProposals = (session: Session, winner: SourceRequestSlot): void => {
		for (const slot of [...session.sourceSlots]) {
			if (
				slot === winner ||
				!slot.active ||
				slot.requestKind !== "proposal" ||
				slot.source !== winner.source ||
				slot.targetDecisionSequence !== winner.targetDecisionSequence
			)
				continue;
			releaseSourceSlot(session, slot, cause("source", "proposal_race_lost"));
		}
	};

	const expireSourceHorizon = (session: Session, decisionSequence: number, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) {
			if (slot.expiresAtTarget && slot.targetDecisionSequence <= decisionSequence)
				releaseSourceSlot(session, slot, failure);
		}
	};

	const releaseAllSourceSlots = (session: Session, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) releaseSourceSlot(session, slot, failure);
	};

	const trackSourceTask = (session: Session, task: Promise<void>): void => {
		session.sourceTasks.add(task);
		void task
			.finally(() => session.sourceTasks.delete(task))
			.catch(() => {
				// Request failure is already represented by its source settlement.
			});
	};

	const waitForSourceTasks = async (session: Session): Promise<void> => {
		while (session.sourceTasks.size || session.planAdmissionTails.size) {
			await Promise.allSettled([...session.sourceTasks, ...session.planAdmissionTails.values()]);
		}
	};

	const launchSourceRequests = (state: Turn): void => {
		for (const source of sources) {
			if (!source.enabled(state.settings)) continue;
			const count = clampCandidateLimit(source.proposalCount?.(state.settings));
			for (let index = 0; index < count; index++) {
				const targetDecisionSequence = state.decisionSequence;
				const slot = claimSourceSlot(
					state.session,
					source.id,
					targetDecisionSequence,
					count,
					"proposal",
					source.requestLifetime === "actor_decision",
				);
				if (!slot) break;
				const generation = new SourceGeneration(state.generation.signal);
				slot.generations.add(generation);
				retainSourceRequest(slot);
				state.session.pendingSourceRequests++;
				const pending = runSourceRequest({
					request: {
						source: source.id,
						turnID: state.turnID,
						index: state.session.sourceRequestSequence++,
						kind: "proposal",
						targetDecisionSequence,
					},
					generation,
					timeoutMs: source.timeoutMs?.(state.settings),
					produce: (requestSignal) =>
						source.propose({
							startInput: state.startInput,
							data: state.data,
							settings: state.settings,
							definitions: state.definitions,
							candidateNames: state.candidateNames,
							proposalIndex: index,
							proposalCount: count,
							signal: requestSignal,
						}),
					count: (value) => asUpdates(value).length,
				}).then((request) =>
					sourceRequestFinished(
						{
							session: state.session,
							startInput: state.startInput,
							data: state.data,
							settings: state.settings,
							signal: generation.signal,
							slot,
						},
						state.turnID,
						source,
						slot,
						generation,
						request,
					),
				);
				trackSourceTask(state.session, pending);
			}
		}
	};

	const sourceRequestFinished = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		turnID: string,
		source: Source,
		slot: SourceRequestSlot,
		generation: SourceGeneration,
		request: SourceRequestResult<PlanUpdate | readonly PlanUpdate[] | undefined>,
	): Promise<void> => {
		const session = scope.session;
		session.pendingSourceRequests = Math.max(0, session.pendingSourceRequests - 1);
		try {
			if (
				request.request.kind === "proposal" &&
				request.settlement.status === "produced" &&
				request.value !== undefined &&
				slot.active &&
				!scope.signal.aborted &&
				source.concurrentProposalPolicy?.(scope.settings) === "first_produced"
			) {
				cancelCompetingProposals(session, slot);
			}
			queueSourceRequestEvent(session, turnID, scope.settings, request);
			if (
				request.settlement.status !== "produced" ||
				request.value === undefined ||
				!slot.active ||
				scope.signal.aborted
			) {
				return;
			}
			for (const update of asUpdates(request.value)) {
				if (session.lifecycle.sealed || !slot.active || scope.signal.aborted) break;
				await admitUpdate(scope, source, update, request);
			}
		} finally {
			slot.generations.delete(generation);
			releaseSourceRequest(session, slot);
		}
	};

	const admitUpdate = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		update: PlanUpdate,
		request?: SettledSourceRequest,
	): Promise<void> => {
		const { session } = scope;
		const key = planUpdateID(update);
		const previous = session.planAdmissionTails.get(key) ?? Promise.resolve();
		session.pendingAdmissions++;
		let admission!: Promise<void>;
		admission = previous
			.then(() => applyUpdate(scope, source, update, request))
			.catch(() => {
				// One malformed proposal cannot poison later independent admissions.
			})
			.finally(() => {
				session.pendingAdmissions = Math.max(0, session.pendingAdmissions - 1);
				if (session.planAdmissionTails.get(key) === admission) session.planAdmissionTails.delete(key);
			});
		session.planAdmissionTails.set(key, admission);
		await admission;
	};

	const applyUpdate = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		update: PlanUpdate,
		request?: SettledSourceRequest,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || scope.signal.aborted) return;
		if (updateSource(update) !== source.id) return;
		const acceptedUpdate = source.multiStepEnabled?.(scope.settings) === false ? immediateOnly(update) : update;
		const applied = session.plan.apply(acceptedUpdate, session.decisionSequence);
		if (!applied.accepted) return;
		for (const retired of applied.retired) retirePlanAction(session, retired, cause("plan", "superseded"));
		const draftTokens = finiteMetric(acceptedUpdate.draftTokens);
		session.tokenTotal += draftTokens;
		const materializations: Promise<void>[] = [];
		for (const action of applied.upserted) {
			const node = session.plan.get(applied.plan.id, action.id);
			if (!node || node.predictionState.status !== "pending") continue;
			const issued = !session.actionContexts.has(node.identity.id);
			if (issued) {
				scope.slot?.owners.add(node.identity.id);
				session.actionContexts.set(node.identity.id, {
					identity: node.identity,
					opportunity: session.plan.opportunity(node.proposalID, node.action.id)!,
					feedback: action.feedback,
					startInput: scope.startInput,
					data: scope.data,
					settings: scope.settings,
					attemptStartedAt: request?.startedAt ?? performance.now(),
					predictionLatencyMs: request?.durationMs ?? 0,
					draftTokens,
					totalDraftTokens: session.tokenTotal,
					draft: planActionDraft(node),
					admissionSignal: scope.signal,
					...(scope.slot ? { sourceSlot: scope.slot } : {}),
					continuationTriggers: new Set(),
					continuationSlots: new Set(),
					continuationTail: Promise.resolve(),
				});
				if (source.onIssued) {
					session.effects.enqueue(() =>
						source.onIssued!({
							proposalID: node.identity.proposalID,
							actionID: node.identity.actionID,
							feedback: action.feedback,
						}),
					);
				}
			} else {
				const context = session.actionContexts.get(node.identity.id)!;
				context.feedback = action.feedback;
				context.draft = planActionDraft(node);
			}
			materializations.push(materializeAction(session, node));
		}
		await Promise.all(materializations);
		dispatchReady(session);
	};

	const executionRouteFor = async (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly draft: SpeculativeDraftCandidate;
		readonly action: ActionKey;
		readonly concrete: Record<string, unknown>;
		readonly callID: string;
		readonly index: number;
		readonly signal: AbortSignal;
	}): Promise<
		| { readonly ok: true; readonly route: SpeculativeExecutionRoute }
		| { readonly ok: false; readonly cause: ResolutionCause }
	> => {
		let route: SpeculativeExecutionRoute | undefined;
		try {
			route = await adapter.resolveExecution({
				startInput: input.startInput,
				data: input.data,
				settings: input.settings,
				candidate: input.draft,
				tool: input.action.tool,
				concrete: input.concrete,
				action: input.action,
				signal: input.signal,
			});
		} catch {
			route = undefined;
		}
		if (input.signal.aborted) return { ok: false, cause: cause("source", "generation_expired") };
		if (!route) {
			return {
				ok: false,
				cause: cause("execution", "isolation_unavailable", "No safe speculative execution route is available."),
			};
		}
		try {
			const preflight = await adapter.preflightCandidate({
				startInput: input.startInput,
				data: input.data,
				settings: input.settings,
				candidate: input.draft,
				tool: input.action.tool,
				concrete: input.concrete,
				action: input.action,
				route,
				callID: input.callID,
				index: input.index,
				signal: input.signal,
			});
			if (!preflight.ok) return { ok: false, cause: cause("admission", preflight.reason, preflight.detail) };
		} catch (error) {
			return { ok: false, cause: cause("admission", "preflight_failed", errorDetail(error)) };
		}
		return input.signal.aborted ? { ok: false, cause: cause("source", "generation_expired") } : { ok: true, route };
	};

	const materializeAction = async (session: Session, node: PlanRuntimeNode): Promise<void> => {
		if (node.actionKey || node.execution.status !== "deferred") return;
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return;
		const concrete = asConcreteInput(node.action.input);
		if (!concrete || !context.settings.enabled || !candidateNames(context.settings).includes(node.action.tool)) {
			failUnlaunchable(session, node, cause("admission", concrete ? "tool_disabled" : "invalid_input"));
			return;
		}
		let predictedAction: ActionKey | undefined;
		try {
			predictedAction = await adapter.actionKey(node.action.tool, concrete, {
				type: "start",
				startInput: context.startInput,
				data: context.data,
			});
		} catch {
			predictedAction = undefined;
		}
		if (!predictedAction) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		const executionAction = coveringAction(predictedAction, projectionRules);
		const executionInput = asConcreteInput(executionAction.input);
		if (!executionInput) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		const onCandidateMaterialized = adapter.onCandidateMaterialized;
		if (onCandidateMaterialized) {
			session.effects.enqueue(() =>
				onCandidateMaterialized({
					sessionID: session.id,
					turnID: context.startInput.turnID,
					expectedDecisionSequence: node.expectedDecisionSeq,
					latestDecisionSequence: node.latestDecisionSeq,
					source: node.source,
					proposalID: node.proposalID,
					actionID: node.action.id,
					tool: node.action.tool,
					input: structuredClone(concrete),
					predictedAction,
					executionAction,
					...(node.action.depth !== undefined ? { depth: node.action.depth } : {}),
					...(node.action.horizon !== undefined ? { horizon: node.action.horizon } : {}),
					...(node.action.conditionalProbability !== undefined
						? { conditionalProbability: node.action.conditionalProbability }
						: {}),
					...(node.action.empiricalProbability !== undefined
						? { empiricalProbability: node.action.empiricalProbability }
						: {}),
					...(node.action.expectedLatencyBenefitMs !== undefined
						? { expectedLatencyBenefitMs: node.action.expectedLatencyBenefitMs }
						: {}),
					...(node.action.expectedDurationMs !== undefined
						? { expectedDurationMs: node.action.expectedDurationMs }
						: {}),
				}),
			);
		}
		const admission = await executionRouteFor({
			startInput: context.startInput,
			data: context.data,
			settings: context.settings,
			draft: context.draft,
			action: executionAction,
			concrete: executionInput,
			callID: `spec_${session.candidateSequence + 1}`,
			index: session.candidateSequence,
			signal: context.admissionSignal,
		});
		if (!admission.ok) {
			if (admission.cause.stage !== "execution" || admission.cause.code !== "isolation_unavailable") {
				failUnlaunchable(session, node, admission.cause);
				return;
			}
			session.plan.bindActionKey(node.proposalID, node.action.id, executionAction);
			if (!session.plan.markExecutionBlocked(node.proposalID, node.action.id, admission.cause)) {
				failUnlaunchable(session, node, cause("plan", "execution_route_state_invalid"));
			}
			return;
		}
		context.executionRoute = admission.route;
		session.plan.bindActionKey(node.proposalID, node.action.id, executionAction);
	};

	const releaseActionContext = (session: Session, id: string, keepContinuation = false): void => {
		const context = session.actionContexts.get(id);
		if (!context) return;
		session.actionContexts.delete(id);
		if (context.sourceSlot) {
			context.sourceSlot.owners.delete(id);
			releaseUnusedSourceSlot(session, context.sourceSlot);
		}
		if (!keepContinuation)
			for (const slot of context.continuationSlots)
				releaseSourceSlot(session, slot, cause("control", "parent_prediction_not_adopted"));
	};

	const failUnlaunchable = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const work = new CandidateExecution<never>("shared");
		work.fail(failure, performance.now(), 0);
		session.plan.attachExecution(node.proposalID, node.action.id, `rejected:${node.identity.id}`, work);
		settleUnobserved(session, node, failure);
	};

	const settleBlockedPlanActions = (session: Session): void => {
		for (const node of session.plan.drainBlocked()) {
			const failure = cause("plan", "dependency_impossible");
			if (node.execution.status === "deferred" || node.execution.status === "scheduled") {
				const work = new CandidateExecution<never>("shared");
				work.fail(failure, performance.now(), 0);
				session.plan.attachExecution(node.proposalID, node.action.id, `blocked:${node.identity.id}`, work);
			}
			settleUnobserved(session, node, failure);
		}
	};

	const pendingActorTurn = (session: Session): Turn | undefined =>
		[...session.turns]
			.map((key) => turns.get(key))
			.find(
				(turn) =>
					turn?.lifecycle === "active" &&
					turn.actorArrivedAt === undefined &&
					turn.decisionSequence === session.decisionSequence + 1,
			);

	const actorPhaseFor = (session: Session, now = performance.now()): PredictionForecast["actorPhase"] => {
		const actorTurn = pendingActorTurn(session);
		return actorTurn
			? { kind: "decision", elapsedMs: Math.max(0, now - actorTurn.actorDecisionStartedAt) }
			: session.lastActorArrivedAt === undefined
				? undefined
				: { kind: "cycle", elapsedMs: Math.max(0, now - session.lastActorArrivedAt) };
	};

	const dispatchReady = (session: Session, immediatePredictionID?: string): void => {
		if (session.lifecycle.sealed) return;
		settleBlockedPlanActions(session);
		const now = performance.now();
		const actorPhase = actorPhaseFor(session, now);
		const immediate: PlanRuntimeNode[] = [];
		for (const node of session.plan.launchable()) {
			if (!node.actionKey) continue;
			const existingTimer = session.launchTimers.get(node.prediction.id);
			if (
				existingTimer &&
				node.expectedDecisionSeq > session.decisionSequence + 1 &&
				node.prediction.id !== immediatePredictionID
			) {
				continue;
			}
			if (existingTimer) clearTimeout(existingTimer);
			session.launchTimers.delete(node.prediction.id);
			const context = session.actionContexts.get(node.identity.id);
			if (!context?.executionRoute) continue;
			const forecast = forecastFor(node, context.executionRoute, session.decisionSequence, actorPhase);
			const delay = node.prediction.id === immediatePredictionID ? 0 : session.scheduler.launchDelay(forecast);
			if (delay <= 0) {
				const promoted = session.plan.promote(node.proposalID, node.action.id);
				if (promoted.status === "scheduled") immediate.push(promoted.node);
				continue;
			}
			const timer = setTimeout(() => {
				session.launchTimers.delete(node.prediction.id);
				const promoted = session.plan.promote(node.proposalID, node.action.id);
				if (promoted.status === "scheduled") void launchNode(session, promoted.node);
			}, delay);
			session.launchTimers.set(node.prediction.id, timer);
		}
		const foreground = immediate.filter((node) => !node.action.background);
		const background = immediate.filter((node) => node.action.background);
		const foregroundAdmissions = Promise.allSettled(foreground.map((node) => launchNode(session, node)));
		void foregroundAdmissions.then(() => Promise.allSettled(background.map((node) => launchNode(session, node))));
		startQueuedCandidates(session);
	};

	const launchNode = async (session: Session, node: PlanRuntimeNode): Promise<void> => {
		if (!node.actionKey || node.predictionState.status === "settled") {
			session.plan.defer(node.proposalID, node.action.id);
			return;
		}
		const context = session.actionContexts.get(node.identity.id);
		if (!context) {
			failUnlaunchable(session, node, cause("plan", "context_missing"));
			return;
		}
		const route = context.executionRoute;
		if (!route) {
			failUnlaunchable(session, node, cause("plan", "execution_route_missing"));
			return;
		}
		const parent = dependencyWorld(session, node);
		if (parent === null) {
			failUnlaunchable(session, node, cause("plan", "incompatible_parent_worlds"));
			return;
		}
		if (
			parent &&
			(route.reuse === "shared_result" ||
				!candidateBranch(parent)?.checkpoint ||
				!sameSpeculativeExecutionRoute(parent.route, route))
		) {
			session.plan.defer(node.proposalID, node.action.id);
			return;
		}
		const reusable = await reusableForPrediction(session, node.actionKey, route, parent);
		if (reusable) {
			attachNode(session, node, reusable);
			return;
		}
		const concrete = asConcreteInput(node.action.input);
		if (!concrete) {
			failUnlaunchable(session, node, cause("admission", "invalid_input"));
			return;
		}
		const sequence = ++session.candidateSequence;
		const candidateID = `spec_${sequence}_${node.actionKey.hash.slice(0, 12)}`;
		const reuse = route.reuse === "exclusive_branch" ? "exclusive" : "shared";
		const work = new CandidateExecution<WorldBranch<Output>>(reuse);
		const scheduled = session.scheduler.evaluate([
			forecastFor(node, route, session.decisionSequence, actorPhaseFor(session)),
		]);
		const candidate: Candidate = {
			id: candidateID,
			origin: "prediction",
			key: node.actionKey,
			route,
			work,
			...(parent ? { worldParent: parent } : {}),
			actorAdopted: false,
			owner: {
				startInput: context.startInput,
				data: context.data,
				settings: context.settings,
				draft: context.draft,
				index: sequence - 1,
			},
			createdAt: Date.now(),
			attemptStartedAt: context.attemptStartedAt,
			predictionLatencyMs: context.predictionLatencyMs,
			draftTokens: context.draftTokens,
			totalDraftTokens: context.totalDraftTokens,
			expectedDurationMs: scheduled.expectedDurationMs,
			expectedDecisionSeq: node.expectedDecisionSeq,
			criticalPathMs: scheduled.criticalPathMs,
			priorityMs: scheduled.priorityMs,
			background: scheduled.background,
			estimatedBytes: 0,
			projectionCoverage: [],
			validationMs: 0,
			validationBytes: 0,
			validationFiles: 0,
			projectionMs: 0,
		};
		const insertion = jobs.insertOrGetCompatible(
			session.id,
			candidate,
			(existing, match) =>
				existing.origin === "prediction" &&
				sameSpeculativeExecutionRoute(existing.route, route) &&
				candidateWorld(existing) === parent &&
				canShareInFlight(existing, node.actionKey!, match, projectionRules),
			(existing) =>
				existing.origin === "prediction" &&
				sameSpeculativeExecutionRoute(existing.route, route) &&
				candidateWorld(existing) === parent &&
				activeExecution(existing),
		);
		if (!insertion.inserted) {
			attachNode(session, node, insertion.entry);
			return;
		}
		session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work);
		startQueuedCandidates(session);
	};

	const attachNode = (session: Session, node: PlanRuntimeNode, candidate: Candidate): void => {
		if (!session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work)) return;
		const scheduled = session.scheduler.evaluate(forecastsForCandidate(session, candidate, node));
		candidate.expectedDurationMs = Math.max(candidate.expectedDurationMs, scheduled.expectedDurationMs);
		candidate.expectedDecisionSeq = Math.min(candidate.expectedDecisionSeq, node.expectedDecisionSeq);
		candidate.criticalPathMs = Math.max(candidate.criticalPathMs, scheduled.criticalPathMs);
		candidate.priorityMs = Math.max(candidate.priorityMs, scheduled.priorityMs);
		candidate.background = scheduled.background;
		const execution = candidate.work.execution;
		if (execution.status === "succeeded") {
			queueContinuation(session, node, candidate, execution.output.output, "execution_succeeded");
			return;
		}
		if (execution.status === "queued" || execution.status === "running") {
			session.scheduler.refresh(candidate, forecastsForCandidate(session, candidate, node));
		}
		if (execution.status === "queued") startQueuedCandidates(session);
	};

	const startQueuedCandidates = (session: Session, preferred?: Candidate, authoritative = false): void => {
		const actorToolHints = pendingActorTurn(session)?.actorToolHints;
		const queued = jobs
			.values(session.id)
			.filter(
				(candidate) =>
					candidate.work.execution.status === "queued" &&
					(!actorToolHints?.size || actorToolHints.has(candidate.key.tool)),
			)
			.sort(
				(left, right) =>
					Number(right === preferred) - Number(left === preferred) ||
					Number(!reservationAvailable(right.work.reservation)) -
						Number(!reservationAvailable(left.work.reservation)) ||
					Number(left.background) - Number(right.background) ||
					left.expectedDecisionSeq - right.expectedDecisionSeq ||
					right.priorityMs - left.priorityMs ||
					right.criticalPathMs - left.criticalPathMs ||
					right.expectedDurationMs - left.expectedDurationMs ||
					left.createdAt - right.createdAt,
			);
		for (const candidate of queued) {
			if (candidate.work.execution.status !== "queued") continue;
			let admission = session.scheduler.admit(
				candidate,
				forecastsForCandidate(session, candidate),
				concurrentLimit(session.settings),
			);
			if (!admission.admitted && !candidate.background) {
				for (const victim of session.scheduler.preemptFor(
					admission.work.resource,
					concurrentLimit(session.settings),
					(victim) =>
						reservationAvailable(victim.work.reservation) &&
						(victim.background ||
							candidate.expectedDecisionSeq < victim.expectedDecisionSeq ||
							(candidate.expectedDecisionSeq === victim.expectedDecisionSeq &&
								candidate.priorityMs > victim.priorityMs)),
				)) {
					cancelCandidate(session, victim, cause("admission", "scheduler_preempted"), false);
				}
				admission = session.scheduler.admit(
					candidate,
					forecastsForCandidate(session, candidate),
					concurrentLimit(session.settings),
				);
			}
			if (!admission.admitted && !(authoritative && candidate === preferred)) continue;
			const startedAt = performance.now();
			if (!candidate.work.start(startedAt)) continue;
			queueCandidateEvent(session, candidate);
			void executeCandidate(session, candidate, startedAt);
		}
	};

	const executeCandidate = async (session: Session, candidate: Candidate, startedAt: number): Promise<void> => {
		let branch: WorldBranch<Output> | undefined;
		try {
			const parent = candidateWorld(candidate);
			branch = await adapter.executeCandidate({
				startInput: candidate.owner.startInput,
				data: candidate.owner.data,
				candidate: candidate.owner.draft,
				tool: candidate.key.tool,
				concrete: candidate.key.input as Record<string, unknown>,
				action: candidate.key,
				route: candidate.route,
				callID: candidate.id,
				index: candidate.owner.index,
				signal: candidate.work.controller.signal,
				...(parent ? { parentWorld: candidateBranch(parent)! } : {}),
			});
			const output = branch.output;
			const rejected = adapter.rejectCandidateOutput?.({
				output,
				candidate: publicCandidate(candidate),
			});
			if (rejected) throw new CandidateFailure(cause("execution", "output_rejected", rejected));
			candidate.projectionCoverage = captureCoverage(candidate.key, output, projectionRules);
			candidate.estimatedBytes = estimateValueBytes(output) + branch.capturedBytes;
			const completedAt = performance.now();
			if (!candidate.work.succeed(branch, completedAt, completedAt - startedAt)) {
				disposeBranch(branch);
				return;
			}
			session.scheduler.observeSpeculativeService(
				actionTimingIdentity(candidate.key),
				completedAt - startedAt,
			);
			session.scheduler.complete(candidate);
			jobs.delete(session.id, candidate);
			if (candidate.work.reservation.kind === "shared") results.insert(session.id, candidate);
			else branches.insert(session.id, candidate);
			installWatcher(session, candidate);
			queueCandidateContinuations(
				session,
				nodesForCandidate(session, candidate.id).filter((node) => {
					const status = session.plan.get(node.proposalID, node.action.id)?.predictionState.status;
					return status === "pending" || status === "matching";
				}),
				candidate,
				output,
				"execution_succeeded",
			);
			trimResults(session, candidate.owner.settings);
			queueCandidateEvent(session, candidate);
			dispatchReady(session);
		} catch (error) {
			if (candidate.work.execution.status !== "succeeded") disposeBranch(branch);
			const failure =
				error instanceof CandidateFailure
					? error.failure
					: candidate.work.controller.signal.aborted
						? cause("control", "execution_aborted")
						: cause("execution", "candidate_failed", errorDetail(error));
			const completedAt = performance.now();
			const settled = candidate.work.controller.signal.aborted
				? candidate.work.cancel(failure, completedAt, completedAt - startedAt)
				: candidate.work.fail(failure, completedAt, completedAt - startedAt);
			session.scheduler.complete(candidate);
			removeCandidate(session, candidate);
			if (settled) queueCandidateEvent(session, candidate);
			dispatchReady(session);
		}
	};

	const previewActorTool = async (
		input: { readonly sessionID: SessionID; readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	): Promise<void> => {
		const state = turns.get(turnKey(input.sessionID, input.turnID));
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterEnabled === false) return;
		if (!state.candidateNames.includes(input.tool)) return;
		state.actorToolHints.add(input.tool);
		await Promise.all(
			nearestPredictions(state.session, state.decisionSequence, (node) => node.action.tool === input.tool).map(
				(node) => promoteForActor(state.session, node),
			),
		);
		if (signal?.aborted || state.lifecycle !== "active" || turns.get(state.key) !== state) return;
		startQueuedCandidates(state.session);
	};

	const actorActionKey = async (input: ConsumeInput, call: ActualToolCall): Promise<ActionKey | undefined> => {
		try {
			return await adapter.actionKey(call.tool, call.input, { type: "consume", consumeInput: input });
		} catch {
			return undefined;
		}
	};

	const previewActorCall = (input: ConsumeInput, signal?: AbortSignal): Promise<void> => {
		const state = turns.get(turnKey(input.sessionID, input.turnID));
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterEnabled === false) {
			return Promise.resolve();
		}
		const actualCall = adapter.actual(input) as ActualToolCall;
		if (!actualCall.id) return promoteActorCall(state, input, actualCall, undefined, signal);
		const existing = state.actorPreviews.get(actualCall.id);
		if (existing) return existing.task;
		let actionKeyPending = true;
		const actionKey = actorActionKey(input, actualCall).finally(() => {
			actionKeyPending = false;
		});
		const record: ActorPreviewRecord = {
			call: actualCall,
			actionKey,
			actionKeyPending: () => actionKeyPending,
			task: Promise.resolve(),
			state: { status: "pending" },
		};
		state.actorPreviews.set(actualCall.id, record);
		record.task = promoteActorCall(state, input, actualCall, record, signal);
		return record.task;
	};

	const promoteActorCall = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		record: ActorPreviewRecord | undefined,
		signal?: AbortSignal,
	): Promise<void> => {
		const attemptStartedAt = performance.now();
		const active = () =>
			!signal?.aborted &&
			record?.state.status !== "cancelled" &&
			state.lifecycle === "active" &&
			turns.get(state.key) === state &&
			masterEnabled !== false;
		const action = await (record?.actionKey ?? actorActionKey(input, actualCall));
		if (!action || !active()) return;
		await Promise.all(
			predictionMatches(state.session, action, state.decisionSequence).map(({ node }) =>
				promoteForActor(state.session, node),
			),
		);
		if (!active()) return;
		const preferred = rankCandidates(
			action,
			allCandidates(state.sessionID).filter(
				(candidate) => candidate.origin !== "actor_preview" && candidateWorld(candidate) === undefined,
			),
		)[0]?.candidate;
		if (preferred) {
			if (record) record.state = { status: "candidate", candidateID: preferred.id, ownership: "existing" };
			if (preferred.work.execution.status === "queued") {
				startQueuedCandidates(state.session, preferred, true);
			}
			return;
		}
		if (!record || !state.candidateNames.includes(actualCall.tool)) return;
		const executionSignal = signal ?? state.generation.signal;
		const concrete = asConcreteInput(action.input);
		if (!concrete) return;
		const draft: SpeculativeDraftCandidate = {
			type: "tool_call",
			tool: actualCall.tool,
			input: action.input,
			source: "actor_preview",
		};
		const admission = await executionRouteFor({
			startInput: state.startInput,
			data: state.data,
			settings: state.settings,
			draft,
			action,
			concrete,
			callID: actualCall.id ?? callKey(state.turnID, actualCall.tool),
			index: state.session.candidateSequence,
			signal: executionSignal,
		});
		if (!admission.ok || !active()) return;
		const { route } = admission;
		const sequence = ++state.session.candidateSequence;
		const forecast: PredictionForecast = {
			tool: actualCall.tool,
			execution: route.isolation,
			executionFingerprint: action.executionFingerprint,
			actionKeyHash: action.hash,
			decisionBatchesUntilCall: 0,
			actorPhase: actorPhaseFor(state.session),
		};
		const scheduled = state.session.scheduler.evaluate([forecast]);
		const work = new CandidateExecution<WorldBranch<Output>>(
			route.reuse === "exclusive_branch" ? "exclusive" : "shared",
		);
		const candidate: Candidate = {
			id: `actor_${sequence}_${action.hash.slice(0, 12)}`,
			origin: "actor_preview",
			key: action,
			route,
			work,
			actorAdopted: false,
			owner: {
				startInput: state.startInput,
				data: state.data,
				settings: state.settings,
				draft,
				index: sequence - 1,
			},
			createdAt: Date.now(),
			attemptStartedAt,
			predictionLatencyMs: 0,
			draftTokens: 0,
			totalDraftTokens: state.session.tokenTotal,
			expectedDurationMs: scheduled.expectedDurationMs,
			expectedDecisionSeq: state.decisionSequence,
			criticalPathMs: scheduled.criticalPathMs,
			priorityMs: scheduled.priorityMs,
			background: false,
			estimatedBytes: 0,
			projectionCoverage: [],
			validationMs: 0,
			validationBytes: 0,
			validationFiles: 0,
			projectionMs: 0,
		};
		jobs.insert(state.sessionID, candidate);
		record.state = { status: "candidate", candidateID: candidate.id, ownership: "preview" };
		startQueuedCandidates(state.session, candidate, true);
	};

	const abandonActorPreview = (
		state: Turn,
		record: ActorPreviewRecord | undefined,
		failure: ResolutionCause,
	): void => {
		if (!record) return;
		const current = record.state;
		record.state = { status: "cancelled" };
		if (current.status !== "candidate" || current.ownership !== "preview") return;
		const candidate = candidateByID(state.sessionID, current.candidateID);
		if (!candidate) return;
		discardCandidate(state.session, candidate, failure, false);
	};

	const beginAuthoritativeResultCapture = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction,
		action: ActionKey,
		signal?: AbortSignal,
	): Promise<void> => {
		if (!adapter.captureAuthoritativeResult) return;
		const concrete = asConcreteInput(actualCall.input);
		if (!concrete) return;
		const captureSignal = signal ?? state.generation.signal;
		if (captureSignal.aborted) return;
		let capture: AuthoritativeResultCapture<Output> | undefined;
		try {
			capture = await adapter.captureAuthoritativeResult({
				startInput: state.startInput,
				data: state.data,
				consumeInput: input,
				settings: state.settings,
				tool: actualCall.tool,
				concrete,
				action,
				callID: actualCall.id ?? callKey(state.turnID, actualCall.tool),
				signal: captureSignal,
			});
		} catch {
			return;
		}
		if (!capture) return;
		if (
			capture.route.reuse !== "shared_result" ||
			captureSignal.aborted ||
			state.lifecycle !== "active" ||
			turns.get(state.key) !== state ||
			masterEnabled === false
		) {
			disposeAuthoritativeResultCapture(capture);
			return;
		}
		disposeActorCapture(state.session, actorAction);
		state.session.authoritativeResultCaptures.set(actorAction, capture);
	};

	const selectActorCandidate = async (input: ActorSelectionInput): Promise<void> => {
		const { state, actualCall, actualKey, attempt, ranked, actorArrivedAt, signal } = input;
		const matchingCandidates = ranked.map(({ candidate }) => candidate);
		const stopCandidate = (candidate: Candidate): boolean => {
			const failure = signal?.aborted
				? cause("control", "actor_aborted")
				: masterEnabled === false || state.lifecycle !== "active" || turns.get(state.key) !== state
					? cause("control", "disabled")
					: undefined;
			if (!failure) return false;
			attempt.interruptCandidate(candidate.id, failure);
			return true;
		};

		for (const choice of ranked) {
			const candidate = choice.candidate;
			const executionAtDecision = candidate.work.execution;
			const join = state.session.scheduler.assessCandidateJoin({
				identity: actionTimingIdentity(candidate.key),
				state:
					executionAtDecision.status === "succeeded"
						? "succeeded"
						: executionAtDecision.status === "running"
							? "running"
							: "queued",
				expectedSpeculativeDurationMs: candidate.expectedDurationMs,
				actorElapsedMs: Math.max(0, performance.now() - actorArrivedAt),
				...(executionAtDecision.status === "running"
					? { elapsedMs: Math.max(0, actorArrivedAt - executionAtDecision.startedAt) }
					: {}),
			});
			if (!join.allowed) {
				attempt.rejectCandidate(
					candidate.id,
					choice.match,
					cause(
						"matching",
						"candidate_join_not_profitable",
						JSON.stringify({
							expectedRemainingMs: join.expectedRemainingMs,
							expectedAdoptionMs: join.expectedAdoptionMs,
							expectedActorMs: join.expectedActorMs,
							expectedNetBenefitMs: join.expectedNetBenefitMs,
						}),
					),
				);
				continue;
			}
			const reservation = candidate.work.acquire(attempt.action.identity.id);
			if (!reservation) {
				attempt.rejectCandidate(candidate.id, choice.match, cause("matching", "candidate_reserved"));
				continue;
			}
			try {
				if (candidate.work.execution.status === "queued") {
					preemptForActor(
						state.session,
						resourceProfile(candidate.route.isolation),
						state.settings,
						matchingCandidates,
					);
					startQueuedCandidates(state.session, candidate, true);
				}
				attempt.releaseAdmission();
				const authorization = await authorize(
					state,
					input.consumeInput,
					actualKey,
					actualCall,
					candidate,
					signal,
				);
				if (stopCandidate(candidate)) break;
				if (authorization) {
					attempt.rejectCandidate(candidate.id, choice.match, authorization);
					continue;
				}
				const wasRunning =
					candidate.work.execution.status === "queued" || candidate.work.execution.status === "running";
				if (!wasRunning) {
					const before = await validateCandidate(candidate);
					if (stopCandidate(candidate)) break;
					if (before.status !== "valid") {
						attempt.rejectCandidate(candidate.id, choice.match, before.cause);
						if (before.status === "stale") invalidateCandidates(state.session, [candidate], before.cause);
						continue;
					}
				}
				const waiting = await waitForCandidate(
					candidate.work.completion,
					signal,
					join.reason === "ready" ? undefined : join.waitBudgetMs,
				);
				if (stopCandidate(candidate)) break;
				if (waiting.status === "aborted") {
					attempt.interruptCandidate(candidate.id, cause("control", "actor_aborted"));
					break;
				}
				if (waiting.status === "deadline") {
					attempt.rejectCandidate(
						candidate.id,
						choice.match,
						cause(
							"matching",
							"candidate_join_deadline",
							JSON.stringify({ waitBudgetMs: join.waitBudgetMs, reason: join.reason }),
						),
					);
					continue;
				}
				const execution = waiting.value;
				if (execution.status !== "succeeded") {
					attempt.rejectCandidate(candidate.id, choice.match, execution.cause);
					continue;
				}
				if (wasRunning) {
					const after = await validateCandidate(candidate);
					if (stopCandidate(candidate)) break;
					if (after.status !== "valid") {
						attempt.rejectCandidate(candidate.id, choice.match, after.cause);
						if (after.status === "stale") invalidateCandidates(state.session, [candidate], after.cause);
						continue;
					}
				}
				const branch = execution.output;
				const compatibility = state.session.scheduler.assessCompatibility(
					branch.compatibility,
					actualKey.executionFingerprint,
				);
				if (!compatibility.compatible) {
					const failure = cause("compatibility", compatibility.code, compatibility.detail);
					attempt.rejectCandidate(candidate.id, choice.match, failure);
					discardCandidate(state.session, candidate, failure);
					continue;
				}

				// Projection is pure and must succeed before the irreversible world commit.
				const projection = await projectOutput(candidate, actualKey, branch.output, choice.match, projectionRules);
				if (stopCandidate(candidate)) break;
				if (!projection.ok) {
					attempt.rejectCandidate(candidate.id, choice.match, projection.cause);
					continue;
				}
				candidate.projectionMs += projection.durationMs;
				let output = projection.output;
				try {
					const committed = await branch.commit();
					if (choice.match.kind === "exact") output = committed;
				} catch (error) {
					const failure = cause("commit", "world_commit_failed", errorDetail(error));
					attempt.rejectCandidate(candidate.id, choice.match, failure);
					discardCandidate(state.session, candidate, failure);
					continue;
				}

				const adoptedAt = performance.now();
				state.session.scheduler.observeAdoption(
					actionTimingIdentity(candidate.key),
					Math.max(0, adoptedAt - Math.max(actorArrivedAt, execution.completedAt)),
				);
				const timing = {
					executionAheadMs: Math.min(
						execution.executionMs,
						Math.max(0, actorArrivedAt - execution.startedAt),
					),
					attemptLeadMs: Math.max(0, actorArrivedAt - candidate.attemptStartedAt),
					hitLatencyMs: Math.max(0, adoptedAt - actorArrivedAt),
				};
				reservation.adopt();
				if (reservation.kind === "exclusive") {
					removeCandidate(state.session, candidate);
				} else if (candidate.origin === "actor_preview") {
					removeCandidate(state.session, candidate);
				} else {
					results.recordActorHit(state.sessionID, candidate, cacheLimits(state.settings));
				}
				attempt.select({
					candidate,
					match: choice.match,
					output,
					timing,
					toolExecution: { startedAt: execution.startedAt, completedAt: execution.completedAt },
				});
				break;
			} finally {
				reservation.release();
			}
		}
	};

	const consume = async (input: ConsumeInput, signal?: AbortSignal): Promise<Output | undefined> => {
		const actorArrivedAt = performance.now();
		const state = turns.get(turnKey(input.sessionID, input.turnID));
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterEnabled === false) return undefined;
		const actualCall = adapter.actual(input) as ActualToolCall;
		let preview = actualCall.id ? state.actorPreviews.get(actualCall.id) : undefined;
		if (actualCall.id) state.actorPreviews.delete(actualCall.id);
		const previewActionKey =
			preview?.actionKeyPending() && sameActualToolCall(preview.call, actualCall) ? preview.actionKey : undefined;
		if (preview?.state.status === "pending") {
			preview.state = { status: "cancelled" };
			preview = undefined;
		}
		const previewCandidateID = preview?.state.status === "candidate" ? preview.state.candidateID : undefined;
		expireSourceHorizon(state.session, state.decisionSequence, cause("control", "actor_action_arrived"));
		closeActorPhase(state, actorArrivedAt);
		if (state.actorArrivedAt === undefined) {
			state.actorArrivedAt = actorArrivedAt;
			state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
			clearLaunchTimers(state.session);
			const actorDecisionMs = Math.max(0, actorArrivedAt - state.actorDecisionStartedAt);
			const previousActorArrivedAt = state.session.lastActorArrivedAt;
			state.session.lastActorArrivedAt = actorArrivedAt;
			state.session.scheduler.observeActorTiming(
				actorDecisionMs,
				previousActorArrivedAt === undefined ? undefined : actorArrivedAt - previousActorArrivedAt,
			);
		}
		const candidatesAtArrival = allCandidates(state.sessionID);
		const sequence = ++state.session.sequence;
		const identity: ActorActionIdentity = {
			id: actualCall.id ?? JSON.stringify([input.turnID, sequence]),
			sequence,
			decisionSequence: state.decisionSequence,
			turnID: input.turnID,
		};
		const admission = enterActorAdmission(state.session);
		const actualKey = await (previewActionKey ?? actorActionKey(input, actualCall));
		const actorAction = new ActorAction({
			identity,
			tool: actualCall.tool,
			...(actualKey ? { actionKey: actualKey } : {}),
		});
		const attempt = new ActorCallAttempt<Candidate, Output>({
			action: actorAction,
			fallback: cause("matching", "no_candidate"),
			releaseActorAdmission: admission.release,
		});
		registerActorAction(state.session, actualCall.id, actorAction);
		state.actorActions.push(actorAction);
		const onActorActionMaterialized = adapter.onActorActionMaterialized;
		if (actualKey && onActorActionMaterialized) {
			state.session.effects.enqueue(() =>
				onActorActionMaterialized({
					sessionID: state.session.id,
					turnID: input.turnID,
					identity,
					tool: actualCall.tool,
					input: structuredClone(actualKey.input),
					action: actualKey,
				}),
			);
		}

		await admission.ready;
		try {
			if (!actualKey) {
				const failure = cause("matching", "action_not_keyable");
				abandonActorPreview(state, preview, failure);
				attempt.deferToFallback([], undefined, failure);
				preemptForActor(state.session, { class: "global", units: 1 }, state.settings);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				return undefined;
			}

			const matchingPredictions: ClaimedPrediction[] = predictionMatches(
				state.session,
				actualKey,
				state.decisionSequence,
			).flatMap(({ node, relation }) => {
				const opportunity = state.session.plan.claimMatch(node.proposalID, node.action.id, identity, relation);
				if (!opportunity) return [];
				return [{ node, opportunity }];
			});
			if (!previewCandidateID) {
				await Promise.all(matchingPredictions.map(({ node }) => promoteForActor(state.session, node)));
			}
			const candidates = uniqueCandidates([...candidatesAtArrival, ...allCandidates(state.sessionID)]).filter(
				(candidate) => candidateWorld(candidate) === undefined,
			);
			const ranked = [...rankCandidates(actualKey, candidates)].sort(
				(left, right) =>
					Number(right.candidate.id === previewCandidateID) - Number(left.candidate.id === previewCandidateID),
			);
			const blockedPrediction = matchingPredictions.find(
				({ node }) => node.execution.status === "execution_blocked",
			)?.node;
			attempt.setFallback(
				blockedPrediction?.execution.status === "execution_blocked"
					? blockedPrediction.execution.cause
					: cause("matching", ranked.length ? "candidate_unavailable" : "no_candidate"),
			);
			await selectActorCandidate({
				state,
				consumeInput: input,
				actualCall,
				actualKey,
				attempt,
				ranked,
				actorArrivedAt,
				...(signal ? { signal } : {}),
			});
			const selected = attempt.selection;
			if (selected) {
				const predictionIdentities = matchingPredictions.map(({ opportunity }) => opportunity.identity);
				if (selected.candidate.origin === "actor_preview") {
					const adoption = attempt.settleSelection(predictionIdentities, "preview");
					if (!adoption) return undefined;
					forgetActorAction(state.session, actualCall.id, actorAction);
					confirmPredictions(state.session, matchingPredictions, identity, adoption);
					reconcileAdoptedCandidate(state.session, actualKey, selected.candidate);
					queueActorSettlement(state, input, actualCall, actorAction, selected.output, selected.candidate);
					state.session.effects.enqueue(() => dispatchReady(state.session));
					return selected.output;
				}
				const adoption = attempt.settleSelection(predictionIdentities, "speculative");
				if (!adoption) return undefined;
				forgetActorAction(state.session, actualCall.id, actorAction);
				reconcileAdoptedCandidate(state.session, actualKey, selected.candidate);
				queueCandidateContinuations(
					state.session,
					matchingPredictions.map(({ node }) => node),
					selected.candidate,
					selected.output,
					"actor_adopted",
					{
						key: actualKey,
						input: asConcreteInput(actualCall.input) ?? actualKey.input,
					},
				);
				confirmPredictions(state.session, matchingPredictions, identity, adoption);
				queueActorSettlement(state, input, actualCall, actorAction, selected.output, selected.candidate);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				return selected.output;
			}

			abandonActorPreview(state, preview, attempt.fallback.cause);
			const adoption = attempt.deferToFallback(
				matchingPredictions.map(({ opportunity }) => opportunity.identity),
				executionBlockedAttemptLead(state.session, matchingPredictions, actorArrivedAt),
			);
			if (adoption) confirmPredictions(state.session, matchingPredictions, identity, adoption);
			preemptForActor(state.session, actorResourceProfile(actualKey.tool), state.settings);
			state.session.effects.enqueue(() => dispatchReady(state.session));
			attempt.releaseAdmission();
			if (
				adapter.captureAuthoritativeResult &&
				semantics.effect(actualKey.tool) === "observation" &&
				state.candidateNames.includes(actualCall.tool)
			) {
				await beginAuthoritativeResultCapture(state, input, actualCall, actorAction, actualKey, signal);
			}
			return undefined;
		} finally {
			attempt.close();
		}
	};

	const executionBlockedAttemptLead = (
		session: Session,
		matches: readonly ClaimedPrediction[],
		actorArrivedAt: number,
	): number | undefined => {
		let earliestAttempt: number | undefined;
		for (const { node } of matches) {
			if (node.execution.status !== "execution_blocked") continue;
			const startedAt = session.actionContexts.get(node.identity.id)?.attemptStartedAt;
			if (startedAt === undefined || !Number.isFinite(startedAt)) continue;
			earliestAttempt = earliestAttempt === undefined ? startedAt : Math.min(earliestAttempt, startedAt);
		}
		return earliestAttempt === undefined ? undefined : Math.max(0, actorArrivedAt - earliestAttempt);
	};

	const actorResourceProfile = (tool: string) => actionResourceProfile(semantics.effect(tool));

	const promoteAuthoritativeResult = async (
		state: Turn,
		action: ActionKey,
		output: Output,
		durationMs: number,
		toolExecution: TimelineInterval,
		capture: AuthoritativeResultCapture<Output>,
	): Promise<void> => {
		let branch: WorldBranch<Output> | undefined;
		try {
			branch = await capture.seal(output);
		} catch {
			disposeAuthoritativeResultCapture(capture);
			return;
		}
		let retained = false;
		try {
			const sequence = ++state.session.candidateSequence;
			const work = new CandidateExecution<WorldBranch<Output>>("shared");
			work.start(toolExecution.startedAt);
			if (!work.succeed(branch, toolExecution.completedAt, durationMs)) return;
			const candidate: Candidate = {
				id: `actor_result_${sequence}_${action.hash.slice(0, 12)}`,
				origin: "actor_result",
				key: action,
				route: capture.route,
				work,
				actorAdopted: true,
				owner: {
					startInput: state.startInput,
					data: state.data,
					settings: state.settings,
					draft: { type: "tool_call", tool: action.tool, input: action.input, source: "actor_result" },
					index: sequence - 1,
				},
				createdAt: Date.now(),
				attemptStartedAt: toolExecution.startedAt,
				predictionLatencyMs: 0,
				draftTokens: 0,
				totalDraftTokens: state.session.tokenTotal,
				expectedDurationMs: durationMs,
				expectedDecisionSeq: state.decisionSequence,
				criticalPathMs: durationMs,
				priorityMs: durationMs,
				background: false,
				estimatedBytes: estimateValueBytes(output) + branch.capturedBytes,
				projectionCoverage: captureCoverage(action, output, projectionRules),
				validationMs: 0,
				validationBytes: 0,
				validationFiles: 0,
				projectionMs: 0,
			};
			const rejected = adapter.rejectCandidateOutput?.({ output, candidate: publicCandidate(candidate) });
			if (rejected) return;
			results.insert(state.sessionID, candidate);
			retained = true;
			installWatcher(state.session, candidate);
			trimResults(state.session, state.settings);
		} catch {
			// Optional cache promotion cannot alter an already completed Actor result.
		} finally {
			if (!retained) disposeBranch(branch);
		}
	};

	const actual = async (
		input: ConsumeInput & { readonly durationMs: number; readonly output?: Output },
	): Promise<void> => {
		const state = turns.get(turnKey(input.sessionID, input.turnID));
		if (!state) return;
		const actualCall = adapter.actual(input) as ActualToolCall;
		const actorAction = takeActorAction(state.session, input.turnID, actualCall);
		if (!actorAction) return;
		const capture = state.session.authoritativeResultCaptures.get(actorAction);
		state.session.authoritativeResultCaptures.delete(actorAction);
		const durationMs = finiteMetric(input.durationMs);
		if (!actorAction.settleActor(durationMs, outputIsError(input.output), performance.now())) {
			if (capture) disposeAuthoritativeResultCapture(capture);
			return;
		}
		const key = actorAction.actionKey;
		if (key) state.session.scheduler.observeActorService(actionTimingIdentity(key), durationMs);
		if (key) reconcileAuthoritativeEffects(state.session, key);
		// Authoritative feedback must enter the settlement queue before optional cache work can yield.
		queueActorSettlement(state, input, actualCall, actorAction, input.output);
		if (capture && key && input.output !== undefined && !outputIsError(input.output)) {
			const provider = actorAction.settlement?.provider;
			if (provider?.kind === "actor") {
				await promoteAuthoritativeResult(state, key, input.output, durationMs, provider.toolExecution, capture);
			} else {
				disposeAuthoritativeResultCapture(capture);
			}
		} else if (capture) {
			disposeAuthoritativeResultCapture(capture);
		}
	};

	const queueActorSettlement = (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction,
		output: Output | undefined,
		candidate?: Candidate,
	): void => {
		const settlement = actorAction.settlement;
		if (!settlement) return;
		if (
			settlement.provider.kind === "actor" ||
			!state.session.authoritativeCandidateIDs.has(settlement.provider.candidateID)
		) {
			state.session.authoritativeToolIntervals.push(settlement.provider.toolExecution);
			if (settlement.provider.kind === "speculative") {
				state.session.authoritativeCandidateIDs.add(settlement.provider.candidateID);
			}
		}
		const key = actorAction.actionKey;
		const settledCandidate =
			candidate ??
			(settlement.provider.kind === "speculative"
				? candidateByID(state.sessionID, settlement.provider.candidateID)
				: undefined);
		const settledCandidateDescriptor = settledCandidate
			? Object.freeze(candidateEventDescriptor(settledCandidate))
			: undefined;
		const event: SpeculativeActionEvent<SessionID> = {
			type: "actor_action",
			sessionID: state.sessionID,
			turnID: state.turnID,
			timestamp: Date.now(),
			cache: cacheSnapshot(state.session, state.settings),
			settlement,
			actualAction: diagnosticAction(actorAction.tool, actualCall.input, key),
			...(settledCandidate ? { execution: settledCandidate.route.isolation } : {}),
			...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
		};
		state.session.effects.enqueue(async () => {
			try {
				await adapter.onActorActionSettled?.({
					sessionID: state.sessionID,
					turnID: state.turnID,
					...(key ? { action: key } : {}),
					settlement,
					...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
				});
			} catch {
				// Policy feedback cannot alter authoritative settlement or source learning.
			}
			state.session.events.enqueue(event);
			const concrete = asConcreteInput(actualCall.input) ?? {};
			for (const source of sources) {
				if (!source.observe || !source.enabled(state.settings)) continue;
				try {
					const updates = await source.observe({
						startInput: state.startInput,
						data: state.data,
						settings: state.settings,
						consumeInput: input,
						...(key ? { action: key } : {}),
						tool: actorAction.tool,
						concrete,
						...(output !== undefined ? { output } : {}),
						durationMs:
							settlement.provider.kind === "actor"
								? settlement.provider.durationMs
								: settledCandidate
									? executionDuration(settledCandidate)
									: candidateExecutionDuration(state.session, settlement.provider.candidateID),
						order: settlement.actorAction.sequence,
					});
					for (const update of asUpdates(updates)) {
						if (state.lifecycle === "active") {
							await admitUpdate(
								{
									session: state.session,
									startInput: state.startInput,
									data: state.data,
									settings: state.settings,
									signal: state.generation.signal,
								},
								source,
								update,
							);
						}
					}
				} catch {
					// Learning and continuation never alter an authoritative Actor result.
				}
			}
		});
	};

	const confirmPredictions = (
		session: Session,
		matches: readonly ClaimedPrediction[],
		actorAction: ActorActionIdentity,
		adoption: PredictionAdoption,
	): void => {
		for (const { node, opportunity } of matches) {
			const settlement = session.plan.confirm(opportunity, actorAction, adoption);
			if (settlement) predictionSettled(session, node, settlement);
		}
	};

	const settlePredictionFrontier = (state: Turn): void => {
		if (!state.actorActions.length) return;
		state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
		const observation = state.actorActions.find((action) => action.actionKey);
		for (const node of state.session.plan.due(state.decisionSequence)) {
			if (node.predictionState.status !== "pending") continue;
			if (!observation) {
				settleUnobserved(state.session, node, cause("matching", "actor_action_not_keyable"));
				continue;
			}
			const settlement = state.session.plan.miss(node.proposalID, node.action.id, observation.identity);
			if (settlement) predictionSettled(state.session, node, settlement);
		}
		dispatchReady(state.session);
	};

	const predictionSettled = (session: Session, node: PlanRuntimeNode, settlement: PredictionSettlement): void => {
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return;
		const source = sourcesByID.get(context.identity.source);
		const event: SpeculativeActionEvent<SessionID> = {
			type: "prediction",
			sessionID: session.id,
			turnID: context.startInput.turnID,
			timestamp: Date.now(),
			cache: cacheSnapshot(session, context.settings),
			settlement,
		};
		session.effects.enqueue(async () => {
			try {
				await adapter.onPredictionSettled?.({
					sessionID: session.id,
					turnID: context.startInput.turnID,
					tool: node.action.tool,
					...(node.actionKey ? { action: node.actionKey } : {}),
					settlement,
				});
			} catch {
				// Policy feedback is a projection of settlement, never its owner.
			}
			session.events.enqueue(event);
			try {
				if (source?.onSettled) {
					await source.onSettled({
						proposalID: context.identity.proposalID,
						actionID: context.identity.actionID,
						feedback: context.feedback,
						settlement,
					});
				}
			} catch {
				// Source feedback is a projection of settlement, never its owner.
			}
		});
		const adopted =
			settlement.observation === "observed" &&
			settlement.match.matched &&
			settlement.match.adoption.status === "adopted";
		releaseActionContext(session, node.identity.id, adopted);
		if ("candidateID" in node.execution && node.execution.candidateID) {
			const candidate = candidateByID(session.id, node.execution.candidateID);
			if (
				candidate?.work.reservation.kind === "exclusive" &&
				!nodesForCandidate(session, candidate.id).some((item) => item.predictionState.status !== "settled")
			) {
				discardCandidate(session, candidate, cause("retention", "prediction_horizon_settled"));
			}
		}
	};

	const settleUnobserved = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const settlement = session.plan.unobserve(node.proposalID, node.action.id, failure);
		if (settlement) predictionSettled(session, node, settlement);
	};

	const queueCandidateContinuations = (
		session: Session,
		nodes: readonly PlanRuntimeNode[],
		candidate: Candidate,
		output: Output,
		trigger: "execution_succeeded" | "actor_adopted",
		adoptedAction?: AdoptedAction,
	): void => {
		for (const node of nodes) {
			queueContinuation(session, node, candidate, output, trigger, adoptedAction);
		}
	};

	const queueContinuation = (
		session: Session,
		node: PlanRuntimeNode,
		candidate: Candidate,
		output: Output,
		trigger: "execution_succeeded" | "actor_adopted",
		adoptedAction?: AdoptedAction,
	): void => {
		const current = session.plan.get(node.proposalID, node.action.id);
		if (current?.identity.id !== node.identity.id) return;
		const context = session.actionContexts.get(node.identity.id);
		const source = context ? sourcesByID.get(context.identity.source) : undefined;
		if (!context || !source?.continue) return;
		if (source.multiStepEnabled?.(context.settings, context.feedback) === false) return;
		if (source.continueOn && !source.continueOn.includes(trigger)) return;
		if (context.continuationTriggers.has(trigger)) return;
		context.continuationTriggers.add(trigger);
		const parentDecisionSequence =
			current.predictionState.status === "matching"
				? (current.predictionState.actorAction.decisionSequence ?? current.expectedDecisionSeq)
				: current.expectedDecisionSeq;
		const targetDecisionSequence = parentDecisionSequence + 1;
		const requestLimit = clampCandidateLimit(source.proposalCount?.(context.settings));
		const pending = context.continuationTail
			.then(async () => {
				const slot = claimSourceSlot(session, source.id, targetDecisionSequence, requestLimit, "continuation");
				if (!slot) return;
				context.continuationSlots.add(slot);
				retainSourceRequest(slot);
				if (session.lifecycle.sealed || !slot.active) {
					releaseSourceRequest(session, slot);
					return;
				}
				const revision = session.plan.reserveRevision(node.proposalID);
				if (revision === undefined) {
					releaseSourceRequest(session, slot);
					return;
				}
				const generation = new SourceGeneration();
				slot.generations.add(generation);
				session.pendingSourceRequests++;
				const request = await runSourceRequest({
					request: {
						source: source.id,
						turnID: context.startInput.turnID,
						index: session.sourceRequestSequence++,
						kind: "continuation",
						targetDecisionSequence: slot.targetDecisionSequence,
					},
					generation,
					timeoutMs: source.timeoutMs?.(context.settings),
					produce: (requestSignal) =>
						source.continue!({
							startInput: context.startInput,
							data: context.data,
							settings: context.settings,
							candidate: predictionCandidate(candidate, node),
							...(adoptedAction ? { adoptedAction } : {}),
							proposalID: node.proposalID,
							actionID: node.action.id,
							revision,
							feedback: context.feedback,
							output,
							trigger,
							signal: requestSignal,
						}),
					count: (value) => asUpdates(value).length,
				});
				await sourceRequestFinished(
					{
						session,
						startInput: context.startInput,
						data: context.data,
						settings: context.settings,
						signal: generation.signal,
						slot,
					},
					context.startInput.turnID,
					source,
					slot,
					generation,
					request,
				);
			})
			.catch(() => {
				// Continuation failure cannot revoke completed work or Actor adoption.
			});
		context.continuationTail = pending;
		trackSourceTask(session, pending);
	};

	const retirePlanAction = (session: Session, retired: RetiredPlanNode, failure: ResolutionCause): void => {
		const timer = session.launchTimers.get(retired.node.identity.id);
		if (timer) clearTimeout(timer);
		session.launchTimers.delete(retired.node.identity.id);
		if (retired.opportunity.state.status === "matching") return;
		const finalized = retired.opportunity.unobserve(failure);
		if (finalized) predictionSettled(session, retired.node, finalized);
		else releaseActionContext(session, retired.node.identity.id);
	};

	const reusableForPrediction = async (
		session: Session,
		key: ActionKey,
		route: SpeculativeExecutionRoute,
		parent: Candidate | undefined,
	): Promise<Candidate | undefined> => {
		for (const lookup of [
			...jobs.lookup(session.id, key),
			...results.lookup(session.id, key),
			...branches.lookup(session.id, key),
		]) {
			const candidate = lookup.entry;
			if (
				candidate.origin === "actor_preview" ||
				!activeExecution(candidate) ||
				!sameSpeculativeExecutionRoute(candidate.route, route) ||
				candidateWorld(candidate) !== parent
			)
				continue;
			if (lookup.match.kind === "projected" && candidate.work.execution.status !== "succeeded") {
				if (!canShareInFlight(candidate, key, lookup.match, projectionRules)) continue;
			}
			if (candidate.work.execution.status === "succeeded") {
				const validation = await validateCandidate(candidate);
				if (validation.status === "stale") {
					invalidateCandidates(session, [candidate], validation.cause);
					continue;
				}
				if (validation.status === "indeterminate") continue;
			}
			return candidate;
		}
		return undefined;
	};

	const nodesForCandidate = (session: Session, candidateID: string): readonly PlanRuntimeNode[] =>
		session.plan
			.values()
			.filter((node) => "candidateID" in node.execution && node.execution.candidateID === candidateID);

	const descendsFrom = (candidate: Candidate, ancestor: Candidate): boolean => {
		for (let parent = candidate.worldParent; parent; parent = parent.worldParent) {
			if (parent === ancestor) return true;
		}
		return false;
	};

	const unresolvedWorld = (candidate: Candidate): Candidate | undefined => {
		for (let current: Candidate | undefined = candidate; current; current = current.worldParent) {
			if (candidateBranch(current)?.checkpoint && !current.actorAdopted) return current;
		}
		return undefined;
	};

	const candidateWorld = (candidate: Candidate): Candidate | undefined =>
		candidate.worldParent ? unresolvedWorld(candidate.worldParent) : undefined;

	const dependencyWorld = (session: Session, node: PlanRuntimeNode): Candidate | null | undefined => {
		const parents = new Set<Candidate>();
		for (const dependency of node.action.dependsOn ?? []) {
			const parentNode = session.plan.get(node.proposalID, dependency.actionID);
			if (!parentNode || !("candidateID" in parentNode.execution) || !parentNode.execution.candidateID) continue;
			const candidate = candidateByID(session.id, parentNode.execution.candidateID);
			if (!candidate) continue;
			const parent = unresolvedWorld(candidate);
			if (parent) parents.add(parent);
		}
		if (!parents.size) return undefined;
		return (
			[...parents].find((candidate) =>
				[...parents].every((parent) => parent === candidate || descendsFrom(candidate, parent)),
			) ?? null
		);
	};

	const forecastsForCandidate = (
		session: Session,
		candidate: Candidate,
		additional?: PlanRuntimeNode,
	): readonly PredictionForecast[] => {
		const nodes = nodesForCandidate(session, candidate.id);
		const unique =
			additional && !nodes.some((node) => node.prediction.id === additional.prediction.id)
				? [...nodes, additional]
				: nodes;
		const actorPhase = actorPhaseFor(session);
		if (unique.length) {
			return unique.map((node) => forecastFor(node, candidate.route, session.decisionSequence, actorPhase));
		}
		return [
			{
				tool: candidate.key.tool,
				execution: candidate.route.isolation,
				executionFingerprint: candidate.key.executionFingerprint,
				actionKeyHash: candidate.key.hash,
				expectedDurationMs: candidate.expectedDurationMs,
				decisionBatchesUntilCall: Math.max(0, candidate.expectedDecisionSeq - session.decisionSequence),
				...(actorPhase ? { actorPhase } : {}),
				criticalPathMs: candidate.criticalPathMs,
				background: candidate.background,
			},
		];
	};

	const installWatcher = (session: Session, candidate: Candidate): void => {
		const branch = candidateBranch(candidate);
		if (!branch?.watch) return;
		try {
			branch.watch((changedPath) => {
				invalidateCandidates(session, [candidate], cause("freshness", "resource_changed", changedPath));
			});
		} catch {
			// Exact validation remains authoritative when a backend cannot install a watcher.
		}
	};

	const validateCandidate = async (candidate: Candidate): Promise<ResourceValidation> => {
		const branch = candidateBranch(candidate);
		if (!branch?.validate) return { status: "valid", metrics: zeroValidationMetrics() };
		try {
			const validation = await branch.validate();
			recordValidation(candidate, validation);
			return validation;
		} catch (error) {
			return {
				status: "indeterminate",
				cause: cause("freshness", "validation_failed", errorDetail(error)),
				metrics: zeroValidationMetrics(),
			};
		}
	};

	const authorize = async (
		state: Turn,
		input: ConsumeInput,
		action: ActionKey,
		actualCall: ActualToolCall,
		candidate: Candidate,
		signal?: AbortSignal,
	): Promise<ResolutionCause | undefined> => {
		if (!adapter.authorizeCandidate) return undefined;
		const concrete = asConcreteInput(actualCall.input);
		if (!concrete) return cause("authorization", "invalid_input");
		try {
			const result = await adapter.authorizeCandidate({
				stateData: state.data,
				consumeInput: input,
				settings: state.settings,
				action,
				route: candidate.route,
				candidate: publicCandidate(candidate),
				tool: actualCall.tool,
				concrete,
				...(signal ? { signal } : {}),
			});
			return result.ok ? undefined : cause("authorization", result.reason, result.detail);
		} catch (error) {
			return cause("authorization", "authorization_failed", errorDetail(error));
		}
	};

	const rankCandidates = (
		action: ActionKey,
		candidates: readonly Candidate[],
	): readonly RankedCandidate<Output, StartInput, StateData>[] => {
		const now = performance.now();
		return candidates
			.flatMap((candidate) => {
				const match = actionKeyMatch(candidate.key, action, projectors);
				if (!match || !activeExecution(candidate)) return [];
				const execution = candidate.work.execution;
				const remainingMs =
					execution.status === "running"
						? Math.max(0, candidate.expectedDurationMs - (now - execution.startedAt))
						: execution.status === "queued"
							? candidate.expectedDurationMs
							: 0;
				return [{ candidate, match, ready: execution.status === "succeeded", remainingMs }];
			})
			.sort(
				(left, right) =>
					Number(right.ready) - Number(left.ready) ||
					left.remainingMs - right.remainingMs ||
					left.match.distance - right.match.distance ||
					right.candidate.createdAt - left.candidate.createdAt,
			);
	};

	const nearestPredictions = (
		session: Session,
		decisionSequence: number,
		matches: (node: PlanRuntimeNode) => boolean,
	): readonly PlanRuntimeNode[] => {
		const groups = new Map<string, PlanRuntimeNode[]>();
		for (const node of session.plan.matchable(decisionSequence)) {
			if (!node.actionKey) continue;
			if (!matches(node)) continue;
			const group = groups.get(node.proposalID) ?? [];
			group.push(node);
			groups.set(node.proposalID, group);
		}
		return [...groups.values()].flatMap((group) => {
			const due = group.filter((node) => node.expectedDecisionSeq <= decisionSequence);
			const ranked = (due.length ? due : group).sort((left, right) =>
				due.length
					? right.expectedDecisionSeq - left.expectedDecisionSeq
					: left.expectedDecisionSeq - right.expectedDecisionSeq,
			);
			return ranked[0] ? [ranked[0]] : [];
		});
	};

	const predictionMatches = (
		session: Session,
		action: ActionKey,
		decisionSequence: number,
	): readonly { readonly node: PlanRuntimeNode; readonly relation: ActionKeyMatch }[] =>
		nearestPredictions(
			session,
			decisionSequence,
			(node) => actionKeyMatch(node.actionKey!, action, projectors) !== undefined,
		).flatMap((node) => {
			const relation = actionKeyMatch(node.actionKey!, action, projectors);
			return relation ? [{ node, relation }] : [];
		});

	const promoteForActor = async (session: Session, node: PlanRuntimeNode): Promise<void> => {
		const timer = session.launchTimers.get(node.prediction.id);
		if (timer) clearTimeout(timer);
		session.launchTimers.delete(node.prediction.id);
		const promoted = session.plan.promote(node.proposalID, node.action.id);
		if (promoted.status === "scheduled") await launchNode(session, promoted.node);
	};

	const preemptForActor = (
		session: Session,
		resource: ReturnType<typeof resourceProfile>,
		settings: SpeculativeActionSettings,
		protectedCandidates: readonly Candidate[] = [],
	): void => {
		for (const candidate of session.scheduler.preemptFor(
			resource,
			concurrentLimit(settings),
			(candidate) => !protectedCandidates.includes(candidate) && reservationAvailable(candidate.work.reservation),
		)) {
			cancelCandidate(session, candidate, cause("admission", "preempted_by_actor"));
		}
	};

	const cancelCandidate = (
		session: Session,
		candidate: Candidate,
		failure: ResolutionCause,
		dispatch = true,
	): void => {
		const state = candidate.work.execution;
		const startedAt = state.status === "running" ? state.startedAt : performance.now();
		const completedAt = performance.now();
		const settled = candidate.work.cancel(failure, completedAt, Math.max(0, completedAt - startedAt));
		session.scheduler.discard(candidate);
		removeCandidate(session, candidate);
		if (settled) queueCandidateEvent(session, candidate);
		if (dispatch) dispatchReady(session);
	};

	const discardCandidate = (
		session: Session,
		candidate: Candidate,
		failure: ResolutionCause,
		dispatch = true,
	): void => {
		if (candidate.work.execution.status === "queued" || candidate.work.execution.status === "running") {
			cancelCandidate(session, candidate, failure, dispatch);
			return;
		}
		removeCandidate(session, candidate);
	};

	const invalidateCandidates = (session: Session, candidates: Iterable<Candidate>, failure: ResolutionCause): void => {
		let invalidated = false;
		for (const candidate of new Set(candidates)) {
			if (candidateByID(session.id, candidate.id) !== candidate) continue;
			discardCandidate(session, candidate, failure, false);
			session.plan.rearmExecution(candidate.id);
			invalidated = true;
		}
		if (invalidated) dispatchReady(session);
	};

	const removeCandidate = (session: Session, candidate: Candidate): void => {
		jobs.delete(session.id, candidate);
		results.delete(session.id, candidate);
		branches.delete(session.id, candidate);
		disposeBranch(candidateBranch(candidate));
	};

	const reconcileStores = async (state: Turn): Promise<void> => {
		const enabled = new Set(candidateNames(state.settings));
		for (const candidate of allCandidates(state.sessionID)) {
			if (!enabled.has(candidate.key.tool))
				discardCandidate(state.session, candidate, cause("control", "tool_disabled"));
		}
		trimResults(state.session, state.settings);
	};

	const trimResults = (session: Session, settings: SpeculativeActionSettings): void => {
		for (const candidate of results.trim(
			session.id,
			cacheLimits(settings),
			(entry) => entry.origin !== "actor_preview" && reservationAvailable(entry.work.reservation),
		)) {
			removeCandidate(session, candidate);
		}
	};

	const reconcileAdoptedCandidate = (session: Session, action: ActionKey, adopted: Candidate): void => {
		reconcileAuthoritativeEffects(session, action, adopted);
		adopted.actorAdopted = true;
	};

	const authoritativeMutationResources = (action: ActionKey, adopted?: Candidate): readonly string[] => {
		if (semantics.effect(action.tool) === "observation") return [];
		if (adopted?.work.reservation.kind === "shared") return [];
		return adopted ? (candidateBranch(adopted)?.resources ?? action.resources) : action.resources;
	};

	const reconcileAuthoritativeEffects = (session: Session, action: ActionKey, adopted?: Candidate): void => {
		const changed = authoritativeMutationResources(action, adopted);
		if (!changed.length) return;
		const candidates = allCandidates(session.id);
		const invalid = new Set<Candidate>();
		for (const candidate of candidates) {
			if (candidate === adopted || (adopted && descendsFrom(candidate, adopted))) continue;
			// Once an Actor reserves a candidate, its freshness and compatibility checks
			// are authoritative. Cache invalidation may only retire unclaimed work.
			if (!reservationAvailable(candidate.work.reservation)) continue;
			if (candidate.key.resources.some((resource) => changed.some((path) => resourcePathsOverlap(resource, path)))) {
				for (const descendant of candidates) {
					if (descendant === candidate || descendsFrom(descendant, candidate)) invalid.add(descendant);
				}
			}
		}
		invalidateCandidates(
			session,
			[...invalid].filter((candidate) => reservationAvailable(candidate.work.reservation)),
			cause("freshness", "authoritative_resource_changed"),
		);
	};

	const pruneActionContexts = (session: Session): void => {
		for (const [id, context] of session.actionContexts) {
			if (context.opportunity.state.status !== "matching") releaseActionContext(session, id);
		}
	};

	const resetTaskTimeline = (session: Session): void => {
		session.taskStartedAt = undefined;
		session.lastActorArrivedAt = undefined;
		session.actorPhaseIntervals.length = 0;
		session.authoritativeToolIntervals.length = 0;
		session.authoritativeCandidateIDs.clear();
	};

	const beginTurnClosure = (
		state: Turn,
		input: { readonly failure: ResolutionCause; readonly terminal: boolean; readonly notifyHost: boolean },
	): TurnClosure | undefined => {
		if (state.lifecycle !== "active") return undefined;
		const completedAt = performance.now();
		closeActorPhase(state, completedAt);
		settlePredictionFrontier(state);
		state.lifecycle = "closing";
		state.generation.expire(input.failure);
		for (const preview of state.actorPreviews.values()) {
			abandonActorPreview(state, preview, input.failure);
		}
		state.actorPreviews.clear();
		return { state, completedAt, terminal: input.terminal, notifyHost: input.notifyHost };
	};

	const completeTurnClosure = async (closure: TurnClosure): Promise<void> => {
		const { state } = closure;
		state.lifecycle = "finished";
		if (closure.notifyHost) {
			try {
				await adapter.onTurnFinished?.({
					startInput: state.startInput,
					settings: state.settings,
					terminal: closure.terminal,
					durationMs: Math.max(0, closure.completedAt - state.startedAt),
				});
			} catch {
				// Host lifecycle is outside authoritative settlement.
			}
		}
		turns.delete(state.key);
		state.session.turns.delete(state.key);
		clearActorActions(state.session, state.turnID);
	};

	const flushSources = async (): Promise<void> => {
		for (const source of sources) {
			try {
				await source.flush?.();
			} catch {
				// Persistence failure is isolated per source.
			}
		}
	};

	const closeSessionState = async (session: Session, mode: SessionClosureMode, turnID: string): Promise<void> => {
		const terminal = mode === "terminal";
		if (terminal && session.taskStartedAt === undefined && session.turns.size === 0) return;
		const failure = cause(
			"control",
			terminal ? "terminal_turn" : mode === "disposed" ? "session_disposed" : "disabled",
		);
		const closures = [...session.turns].flatMap((key) => {
			const state = turns.get(key);
			if (!state) return [];
			const closure = beginTurnClosure(state, { failure, terminal, notifyHost: terminal });
			return closure ? [closure] : [];
		});

		releaseAllSourceSlots(session, failure);
		await waitForSourceTasks(session);
		await session.effects.flush();
		for (const closure of closures) await completeTurnClosure(closure);
		for (const key of session.turns) turns.delete(key);
		session.turns.clear();

		const planFailure = terminal ? cause("control", "session_terminal") : failure;
		for (const node of session.plan.unsettled()) settleUnobserved(session, node, planFailure);
		clearLaunchTimers(session);
		for (const candidate of allCandidates(session.id)) {
			if (!terminal || candidate.work.reservation.kind === "exclusive") {
				discardCandidate(session, candidate, planFailure);
			}
		}
		if (terminal) {
			queueTaskEvent(session, turnID, performance.now());
		} else {
			resetTaskTimeline(session);
		}
		await session.effects.flush();
		if (terminal || mode === "disposed") await flushSources();
		session.plan.clear();
		pruneActionContexts(session);
		clearActorActions(session);
	};

	const finishState = (state: Turn, terminal: boolean): Promise<void> =>
		state.session.lifecycle.run(async () => {
			if (terminal) {
				await closeSessionState(state.session, "terminal", state.turnID);
				return;
			}
			const closure = beginTurnClosure(state, {
				failure: cause("control", "turn_finished"),
				terminal: false,
				notifyHost: true,
			});
			if (!closure) return;
			await state.session.effects.flush();
			await completeTurnClosure(closure);
		});

	const finishTurn = async (input: FinishInput): Promise<void> => {
		const session = sessions.get(input.sessionID);
		if (!session) return;
		const state = turns.get(turnKey(input.sessionID, input.turnID));
		if (state) {
			await finishState(state, input.terminal === true);
		} else if (input.terminal === true) {
			await session.lifecycle.run(() => closeSessionState(session, "terminal", input.turnID));
		}
	};

	const settingsChanged = async (settings: SpeculativeActionSettings): Promise<void> => {
		masterEnabled = settings.enabled;
		if (settings.enabled) return;
		await Promise.all(
			[...sessions.keys()].map((sessionID) => disableSession(sessionID)),
		);
	};

	const disableSession = async (sessionID: SessionID): Promise<void> => {
		const session = sessions.get(sessionID);
		if (!session) return;
		await session.lifecycle.run(() => closeSessionState(session, "disabled", String(sessionID)));
	};

	const disposeSession = async (sessionID: SessionID): Promise<void> => {
		const session = sessions.get(sessionID);
		if (!session) return;
		await session.lifecycle.close(async () => {
			await closeSessionState(session, "disposed", String(sessionID));
			await session.effects.close();
			await session.events.close({ drain: false });
			sessions.delete(sessionID);
		});
	};

	const releaseSession = async (sessionID: SessionID): Promise<void> => {
		await disposeSession(sessionID);
	};

	const dispose = async (): Promise<void> => {
		await Promise.all([...sessions.keys()].map((sessionID) => disposeSession(sessionID)));
	};

	const inspect = (sessionID?: SessionID): SpeculativeRuntimeInspection => {
		const selectedSessions = sessionID === undefined ? [...sessions.values()] : maybe(sessions.get(sessionID));
		const selectedTurns = [...turns.values()].filter(
			(turn) => sessionID === undefined || turn.sessionID === sessionID,
		);
		const candidates =
			sessionID === undefined
				? uniqueCandidates([...jobs.allValues(), ...results.allValues(), ...branches.allValues()])
				: allCandidates(sessionID);
		const planNodes = selectedSessions.flatMap((session) => session.plan.values());
		const telemetry = selectedSessions.map((session) => session.events.snapshot());
		return {
			activeTurns: selectedTurns.length,
			exclusiveCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "exclusive").length,
			sharedCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "shared").length,
			pendingPredictions: selectedSessions.reduce(
				(total, session) => total + session.pendingSourceRequests + session.pendingAdmissions,
				0,
			),
			deferredPlanActions: planNodes.filter((node) => node.execution.status === "deferred").length,
			activePlanActions: planNodes.filter(
				(node) =>
					node.execution.status === "scheduled" ||
					node.execution.status === "queued" ||
					node.execution.status === "running",
			).length,
			executionBlockedPlanActions: planNodes.filter((node) => node.execution.status === "execution_blocked").length,
			blockedPlanActions: planNodes.filter((node) => node.readiness === "blocked").length,
			pendingTelemetryEvents: telemetry.reduce((total, item) => total + item.pending, 0),
			droppedTelemetryEvents: telemetry.reduce((total, item) => total + item.dropped, 0),
			oldestTelemetryEventMs: telemetry.reduce((oldest, item) => Math.max(oldest, item.oldestPendingMs), 0),
		};
	};

	const cacheSnapshot = (session: Session, settings: SpeculativeActionSettings): SpeculativeCacheSnapshot => {
		const inFlight = jobs.values(session.id);
		const cached = results.values(session.id);
		const staged = branches.values(session.id);
		const segments = results.snapshot(session.id);
		return {
			cacheCapacity: settings.resourceCacheMaxEntries,
			cacheByteCapacity: cacheByteLimit(settings),
			cacheCold: segments.coldEntries,
			cacheHot: segments.hotEntries,
			inFlightJobs: inFlight.length,
			resultEntries: cached.length,
			resultBytes: cached.reduce((sum, candidate) => sum + candidate.estimatedBytes, 0),
			branchEntries: staged.length,
			branchBytes: staged.reduce((sum, candidate) => sum + candidate.estimatedBytes, 0),
			exclusiveCandidates: [...inFlight, ...staged].filter(
				(candidate) => candidate.work.reservation.kind === "exclusive",
			).length,
			sharedCandidates: [...inFlight, ...cached].filter((candidate) => candidate.work.reservation.kind === "shared")
				.length,
			cacheTools: [...new Set([...inFlight, ...cached, ...staged].map((candidate) => candidate.key.tool))].sort(),
			cacheExecutions: [
				...new Set([...inFlight, ...cached, ...staged].map((candidate) => candidate.route.isolation)),
			].sort(),
		};
	};

	const queueTaskEvent = (session: Session, turnID: string, completedAt: number): void => {
		const startedAt = session.taskStartedAt;
		if (startedAt === undefined) return;
		const timing = measureSpeculativeTask({
			startedAt,
			completedAt,
			actorPhases: session.actorPhaseIntervals,
			authoritativeTools: session.authoritativeToolIntervals,
		});
		resetTaskTimeline(session);
		const event: SpeculativeActionEvent<SessionID> = {
			type: "task",
			sessionID: session.id,
			turnID,
			timestamp: Date.now(),
			cache: cacheSnapshot(session, session.settings),
			timing,
		};
		session.events.enqueue(event);
	};

	const queueSourceRequestEvent = (
		session: Session,
		turnID: string,
		settings: SpeculativeActionSettings,
		result: SettledSourceRequest,
	): void => {
		const request: SettledSourceRequest = {
			request: result.request,
			startedAt: result.startedAt,
			durationMs: result.durationMs,
			settlement: result.settlement,
		};
		const event: SpeculativeActionEvent<SessionID> = {
			type: "source_request",
			sessionID: session.id,
			turnID,
			timestamp: Date.now(),
			cache: cacheSnapshot(session, settings),
			request,
		};
		session.events.enqueue(event);
	};

	const queueCandidateEvent = (session: Session, candidate: Candidate): void => {
		const state = candidateExecutionProjection(candidate);
		if (!state) return;
		const descriptor = candidateEventDescriptor(candidate);
		const event: SpeculativeActionEvent<SessionID> = {
			type: "candidate",
			sessionID: session.id,
			turnID: candidate.owner.startInput.turnID,
			timestamp: Date.now(),
			cache: cacheSnapshot(session, candidate.owner.settings),
			candidate: descriptor,
			state,
		};
		session.events.enqueue(event);
	};

	const emit = async (event: SpeculativeActionEvent<SessionID>): Promise<void> => {
		try {
			await adapter.onEvent?.(event);
		} catch {
			// Events are projections and cannot mutate settlement.
		}
	};

	return {
		startTurn,
		previewActorTool,
		previewActorCall,
		consume,
		actual,
		finishTurn,
		settingsChanged,
		releaseSession,
		disposeSession,
		dispose,
		inspect,
	};

	function allCandidates(sessionID: SessionID): Candidate[] {
		return uniqueCandidates([...jobs.values(sessionID), ...results.values(sessionID), ...branches.values(sessionID)]);
	}

	function candidateByID(sessionID: SessionID, candidateID: string): Candidate | undefined {
		return allCandidates(sessionID).find((candidate) => candidate.id === candidateID);
	}

	function candidateExecutionDuration(session: Session, candidateID: string): number {
		const execution = candidateByID(session.id, candidateID)?.work.execution;
		return execution && "executionMs" in execution ? execution.executionMs : 0;
	}

	function recordValidation(candidate: Candidate, validation: ResourceValidation): void {
		candidate.validationMs += finiteMetric(validation.metrics.durationMs);
		candidate.validationBytes += finiteMetric(validation.metrics.bytesRead);
		candidate.validationFiles += finiteMetric(validation.metrics.filesRead);
		candidate.validationMode = validation.metrics.mode;
	}
}
