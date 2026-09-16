import { type ActionProjectionCoverage, type ActionProjectionRule, resolveActionProjectionRules } from "./action-key-projection.ts";
import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import { actionKeyCovers, actionKeyMatch, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { ActorAction, type ActorCandidateSelection } from "./actor-action.ts";
import { CandidateExecution, type CandidateReservation } from "./candidate-execution.ts";
import {
	CandidateStore,
	type ResultCacheEvidence,
	speculativeCacheValue,
} from "./candidate-stores.ts";
import { candidateToolNames, clampCandidateLimit, DEFAULTS, type DrafterToolDefinition } from "./common.ts";
import { nonNegativeFinite as finiteMetric, positiveCount } from "./number-utils.ts";
import { errorDetail } from "./error-utils.ts";
import { diagnosticAction } from "./diagnostics.ts";
import { effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import type { CandidateEventDescriptor, CandidateExecutionProjection } from "./events.ts";
import { type ExecutionOperationAdoption, type ExecutionOperationBinding, type ExecutionScope, type SpeculativeExecutionRoute, sameSpeculativeExecutionRoute, validateWorldBranch, type WorldBranch } from "./execution-world.ts";
import type { PlanUpdate } from "./plan-proposal.ts";
import { PlanRuntime, type PlanRuntimeNode, type PredictionOpportunity } from "./plan-runtime.ts";
import { BoundedEventQueue, PostSettlementQueue } from "./post-settlement.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { cloneSharedData } from "./stable-json.ts";
import { containsLogicalPath } from "./path-utils.ts";
import type {
	AdoptedAction,
	ActualToolCall,
	AuthoritativeResultCapture,
	PreparedActorCall,
	RuntimeTurnContext,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionRuntimeAdapter,
	SpeculativeActionSettings,
	SpeculativeCacheSnapshot,
	SpeculativeCandidate,
	SpeculativeDraftCandidate,
	SpeculativePlanSource,
	SpeculativeRuntimeInspection,
} from "./runtime-contracts.ts";
import {
	type CandidateJoinDecision,
	type PredictionForecast,
	type ServiceTimingIdentity,
	SpeculationScheduler,
	waitForCandidate,
} from "./scheduler.ts";
import type {
	ActorActionIdentity,
	PlanActionIdentity,
	PredictionAdoption,
	PredictionSettlement,
	ResolutionCause,
	ResourceValidation,
	SettledSourceRequest,
	SourceRequestIdentity,
	SourceRequestKind,
} from "./settlement.ts";
import { cause } from "./settlement.ts";
import { runSourceRequest, SourceGeneration } from "./source-request.ts";
import { TaskTimeline, TimelineInterval } from "./task-timing.ts";

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

function asUpdates(value: PlanUpdate | readonly PlanUpdate[] | undefined): readonly PlanUpdate[] {
	return value === undefined ? [] : Array.isArray(value) ? value : [value as PlanUpdate];
}

function reservationAvailable(reservation: CandidateReservation): boolean {
	return reservation.kind === "shared" ? reservation.owners.length === 0 : reservation.status === "available";
}

function concurrentLimit(settings: SpeculativeActionSettings): number {
	return positiveCount(settings.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions);
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

function definedFields<T, K extends keyof T>(value: T, keys: readonly K[]): Partial<Pick<T, K>> {
	const result: Partial<Pick<T, K>> = {};
	for (const key of keys) if (value[key] !== undefined) result[key] = value[key];
	return result;
}

function forecastFor(
	node: PlanRuntimeNode,
	decisionSequence: number,
	actorPhase?: PredictionForecast["actorPhase"],
): PredictionForecast {
	return {
		tool: node.action.tool,
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
		type: node.action.type,
		...(node.action.operation ? { operation: node.action.operation } : {}),
		tool: node.action.tool,
		input: node.action.input,
		...(node.action.diagnostic ? { diagnostic: node.action.diagnostic } : {}),
		source: node.source,
		proposalID: node.proposalID,
		actionID: node.action.id,
		feedback: node.action.feedback,
		...(node.action.dependsOn ? { dependsOn: node.action.dependsOn } : {}),
		...definedFields(node.action, [
			"horizon", "latestHorizon", "empiricalProbability", "conditionalProbability",
			"expectedDurationMs", "expectedLatencyBenefitMs", "resourceDemand", "depth",
		]),
	};
}

function asConcreteInput(value: unknown): Record<string, unknown> | undefined {
	if (value === undefined || value === null) return {};
	return typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}

function publicCandidate<Output>(
	candidate: CandidateRecord<Output>,
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

function predictionCandidate<Output>(
	candidate: CandidateRecord<Output>,
	node: PlanRuntimeNode,
): SpeculativeCandidate {
	return {
		...publicCandidate(candidate),
		source: node.source,
		empiricalProbability: node.action.empiricalProbability,
		conditionalProbability: node.action.conditionalProbability,
		depth: node.action.depth,
		planDependencies: node.action.dependsOn,
	};
}

function activeExecution<Output>(
	candidate: CandidateRecord<Output>,
): boolean {
	return candidate.work.execution.status !== "failed" && candidate.work.execution.status !== "cancelled";
}

function candidateBranch<Output>(
	candidate: CandidateRecord<Output>,
): WorldBranch<Output> | undefined {
	const execution = candidate.work.execution;
	return execution.status === "succeeded" ? execution.output : undefined;
}

function captureCoverage<Output>(
	action: ActionKey,
	output: Output,
	rules: readonly ActionProjectionRule<Output>[],
): readonly ActionProjectionCoverage[] {
	return rules.flatMap((rule) => {
		try {
			const value = rule.captureCoverage?.(action, output);
			return value === undefined ? [] : [{ rule: rule.id, value: cloneSharedData(value) }];
		} catch {
			return [];
		}
	});
}

async function projectOutput<Output>(
	candidate: CandidateRecord<Output>,
	actor: ActionKey,
	output: Output,
	match: ActionKeyMatch,
	rules: readonly ActionProjectionRule<Output>[],
	request: Parameters<NonNullable<WorldBranch<Output>["reconstruct"]>>[0],
): Promise<ProjectionResult<Output>> {
	if (match.kind === "exact") return { ok: true, output };
	const retained = candidate.resultViews?.get(actor.key);
	if (retained) return { ok: true, output: cloneSharedData(retained.output), execution: retained.execution };
	const reconstruct = candidateBranch(candidate)?.reconstruct;
	const rule = rules.find((item) => item.id === match.projector);
	if (!rule) return { ok: false, cause: cause("projection", "rule_missing") };
	const coverage = candidate.projectionCoverage.find((item) => item.rule === rule.id);
	if (!reconstruct && (!coverage || !rule.projectOutput)) return { ok: false, cause: cause("projection", "coverage_missing") };
	const startedAt = performance.now();
	try {
		let projected = coverage && rule.projectOutput ? cloneSharedData(await rule.projectOutput({
			speculative: candidate.key,
			actor,
			output,
			coverage: cloneSharedData(coverage.value),
			keyMatch: match,
		})) : undefined;
		if (projected === undefined) projected = await reconstruct?.(request);
		if (projected === undefined) return { ok: false, cause: cause("projection", "view_not_covered") };
		const execution = new TimelineInterval(startedAt, performance.now());
		candidate.projectionMs += Math.max(0, execution.completedAt - execution.startedAt);
		return { ok: true, output: projected, execution };
	} catch (error) {
		return { ok: false, cause: cause("projection", "reconstruction_failed", errorDetail(error)) };
	}
}

function callKey(turnID: string, callID: string): string {
	return JSON.stringify([turnID, callID]);
}

function closeActorPhase<SessionID, Output, StartInput, StateData>(
	turn: TurnState<SessionID, Output, StartInput, StateData>,
	completedAt: number,
): void {
	if (turn.actorPhaseCompletedAt !== undefined) return;
	turn.actorPhaseCompletedAt = Math.max(turn.startedAt, completedAt);
	turn.session.timeline?.recordActor(turn.startedAt, turn.actorPhaseCompletedAt);
}

function outputIsError(value: unknown): boolean {
	return Boolean(value && typeof value === "object" && (value as { readonly isError?: unknown }).isError === true);
}

function candidateEventDescriptor<Output>(
	candidate: CandidateRecord<Output>,
): CandidateEventDescriptor {
	const branch = candidateBranch(candidate);
	return {
		source: candidate.owner.draft.source ?? "cache",
		depth: candidate.owner.draft.depth ?? 0,
		id: candidate.id,
		origin: candidate.origin,
		...(candidate.owner.draft.type === "operation" ? { kind: "operation" as const } : {}),
		tool: candidate.key.tool,
		actionKeyHash: candidate.key.hash,
		execution: candidate.route.isolation,
		route: candidate.route,
		...(branch ? { world: { backend: branch.backend, executionMetrics: branch.executionMetrics } } : {}),
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

function candidateExecutionProjection<Output>(
	candidate: CandidateRecord<Output>,
): CandidateExecutionProjection | undefined {
	const state = candidate.work.execution;
	if (state.status === "queued") return undefined;
	if (state.status === "succeeded") {
		return {
			status: "succeeded",
			...state.toolExecution,
			executionMs: state.executionMs,
		};
	}
	return { ...state };
}

function candidateCacheValue<Output>(
	candidate: CandidateRecord<Output>,
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

function executionDuration<Output>(
	candidate: CandidateRecord<Output> | undefined,
): number {
	const execution = candidate?.work.execution;
	return execution && "executionMs" in execution ? execution.executionMs : 0;
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

/** Memoized queries share their sealed candidate's proof, retention budget, and lifetime. */
function retainResultView<Output>(
	candidate: CandidateRecord<Output>, action: ActionKey, projection: Extract<ProjectionResult<Output>, { ok: true }>,
	settings: SpeculativeActionSettings,
): boolean {
	if (!projection.execution || candidate.resultViews?.has(action.key)) return false;
	try {
		const owned = cloneSharedData(projection.output), bytes = estimateValueBytes(owned) + action.key.length * 2 + 64;
		const views = candidate.resultViews ??= new Map();
		while (views.size && (views.size >= cacheEntryLimit(settings) || candidate.estimatedBytes + bytes > cacheByteLimit(settings))) {
			const [key, previous] = views.entries().next().value!;
			views.delete(key); candidate.estimatedBytes -= previous.bytes;
		}
		if (candidate.estimatedBytes + bytes <= cacheByteLimit(settings)) {
			views.set(action.key, { output: owned, bytes, execution: projection.execution }); candidate.estimatedBytes += bytes;
			return true;
		}
	} catch { /* Optional retention cannot alter an already committed result. */ }
	return false;
}

function resourcePathsOverlap(left: string, right: string): boolean {
	return containsLogicalPath(left, right) || containsLogicalPath(right, left);
}

function maybe<T>(value: T | undefined): T[] {
	return value === undefined ? [] : [value];
}

function actionTimingIdentity(action: ActionKey): ServiceTimingIdentity {
	return {
		tool: action.tool,
		executionFingerprint: action.executionFingerprint,
		actionKeyHash: action.hash,
	};
}

interface PlanActionContext<StartInput, StateData> extends RuntimeTurnContext<StartInput, StateData> {
	readonly identity: PlanActionIdentity;
	readonly opportunity: PredictionOpportunity;
	feedback: unknown;
	readonly attemptStartedAt: number;
	readonly predictionLatencyMs: number;
	readonly draftTokens: number;
	readonly totalDraftTokens: number;
	draft: SpeculativeDraftCandidate;
	readonly admissionSignal: AbortSignal;
	readonly admissionController: AbortController;
	readonly sourceSlot: SourceRequestSlot;
	readonly continuationSlots: Set<SourceRequestSlot>;
	readonly continuationTriggers: Set<"execution_succeeded" | "actor_adopted">;
	continuationTail: Promise<void>;
	peerContinuations?: Set<string>;
	executionRoute?: SpeculativeExecutionRoute;
}

/** Producer requests and ordered observations share ownership with their admitted actions. */
interface SourceRequestSlot {
	readonly request: SourceRequestIdentity;
	readonly expiresAtTarget: boolean;
	readonly generation: SourceGeneration;
	readonly owners: Set<string>;
	pending: number;
}

interface PlanAdmissionScope<SessionID, Output, StartInput, StateData> extends RuntimeTurnContext<StartInput, StateData> {
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly slot: SourceRequestSlot;
}

interface CandidateRecord<Output, StartInput = unknown, StateData = unknown> {
	readonly id: string;
	readonly origin: "prediction" | "actor_preview" | "actor_result";
	readonly key: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly work: CandidateExecution<WorldBranch<Output>>;
	readonly worldParent?: CandidateRecord<Output, StartInput, StateData>;
	actorAdopted: boolean;
	readonly owner: RuntimeTurnContext<StartInput, StateData> & {
		readonly draft: SpeculativeDraftCandidate;
		readonly index: number;
	};
	readonly createdAt: number;
	readonly attemptStartedAt: number;
	readonly predictionLatencyMs: number;
	readonly draftTokens: number;
	readonly totalDraftTokens: number;
	expectedDurationMs: number;
	estimatedBytes: number;
	projectionCoverage: readonly ActionProjectionCoverage[];
	resultViews?: Map<string, { readonly output: Output; readonly bytes: number; readonly execution: TimelineInterval }>;
	previews?: Set<ActorPreviewRecord>;
	onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	acceptOperationScope?: (scope: ExecutionScope) => boolean;
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
	readonly actionKey: Promise<ActionKey | undefined>;
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
	readonly sourceTasks: Set<Promise<unknown>>;
	readonly turns: Map<string, TurnState<SessionID, Output, StartInput, StateData>>;
	settings: SpeculativeActionSettings;
	timeline?: TaskTimeline;
	lastActorArrivedAt?: number;
	sequence: number;
	decisionSequence: number;
	tokenTotal: number;
	candidateSequence: number;
	sourceRequestSequence: number;
	pendingSourceRequests: number;
	pendingAdmissions: number;
	pendingLaunch?: Promise<void>;
}

interface TurnState<SessionID, Output, StartInput, StateData> extends RuntimeTurnContext<StartInput, StateData> {
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly startedAt: number;
	readonly definitions: readonly DrafterToolDefinition[];
	readonly candidateNames: readonly string[];
	readonly generation: SourceGeneration;
	readonly signal?: AbortSignal;
	readonly decisionSequence: number;
	readonly actorActions: Set<ActorAction<CandidateRecord<Output, StartInput, StateData>, Output>>;
	actorObservation?: ActorActionIdentity | null;
	readonly actorToolHints: Set<string>;
	readonly actorPreviews: Map<string, ActorPreviewRecord>;
	actorDecisionStartedAt: number;
	actorArrivedAt?: number;
	actorPhaseCompletedAt?: number;
	lifecycle: "active" | "closing" | "finished";
}

interface ClaimedPrediction {
	readonly node: PlanRuntimeNode;
	readonly opportunity: PredictionOpportunity;
}

type ProjectionResult<Output> =
	| { readonly ok: true; readonly output: Output; readonly execution?: TimelineInterval }
	| { readonly ok: false; readonly cause: ResolutionCause };

const RUNTIME_EVENT_QUEUE_CAPACITY = 256;

/** Structural runtime: plans own predictions, candidates own execution, ActorAction owns adoption. */
export function makeSpeculativeActionRuntime<
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
		readonly actorAction: ActorAction<Candidate, Output>;
		readonly ranked: ReturnType<typeof rankCandidates>;
		readonly actorArrivedAt: number;
		readonly preview?: ActorPreviewRecord;
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
	const projectionRules = resolveActionProjectionRules(adapter.projectionRules ?? [], semantics);
	const candidateStore = new CandidateStore<SessionID, Candidate>(projectionRules, candidateCacheValue);
	const sessionStates = new Map<SessionID, Session>();
	let masterEnabled: boolean | undefined;
	const masterDisabled = () => masterEnabled === false;

	const sessionFor = (sessionID: SessionID, settings: SpeculativeActionSettings): Session => {
		const current = sessionStates.get(sessionID);
		if (current) return current;
		const created: Session = {
			id: sessionID,
			lifecycle: new RuntimeLifecycleLane(),
			plan: new PlanRuntime(),
			scheduler: new SpeculationScheduler<Candidate>(),
			effects: new PostSettlementQueue(),
			events: new BoundedEventQueue(RUNTIME_EVENT_QUEUE_CAPACITY, (event) => adapter.onEvent?.(event)),
			actionContexts: new Map(),
			launchTimers: new Map(),
			sourceSlots: new Set(),
			sourceTasks: new Set(),
			turns: new Map(),
			settings,
			sequence: 0,
			decisionSequence: 0,
			tokenTotal: 0,
			candidateSequence: 0,
			sourceRequestSequence: 0,
			pendingSourceRequests: 0,
			pendingAdmissions: 0,
		};
		sessionStates.set(sessionID, created);
		return created;
	};
	const turnContext = ({ startInput, data, settings }: RuntimeTurnContext<StartInput, StateData>) =>
		({ startInput, data, settings });

	const removeCandidate = (sessionID: SessionID, candidate: Candidate): void => {
		candidateStore.delete(sessionID, candidate);
		candidate.resultViews?.clear();
		sessionStates.get(sessionID)?.lifecycle.release(candidateBranch(candidate));
	};

	const acquireCandidate = (session: Session, candidate: Candidate, owner: string) =>
		candidateStore.has(session.id, candidate) ? candidate.work.acquire(owner) : undefined;

	const createCandidate = (
		session: Session,
		context: RuntimeTurnContext<StartInput, StateData>,
		draft: SpeculativeDraftCandidate,
		input: Pick<Candidate, "origin" | "key" | "route" | "attemptStartedAt" | "expectedDurationMs"> & Partial<Pick<Candidate,
			"worldParent" | "predictionLatencyMs" | "draftTokens" | "totalDraftTokens" | "estimatedBytes" | "projectionCoverage">>,
	): Candidate => {
		const sequence = ++session.candidateSequence;
		return {
			id: `${input.origin === "prediction" ? "spec" : input.origin === "actor_preview" ? "actor" : input.origin}_${sequence}_${input.key.hash.slice(0, 12)}`,
			work: new CandidateExecution<WorldBranch<Output>>(input.origin !== "actor_result" && input.route.reuse === "exclusive_branch" ? "exclusive" : "shared"),
			actorAdopted: input.origin === "actor_result",
			owner: { ...turnContext(context), draft, index: sequence - 1 },
			createdAt: Date.now(),
			predictionLatencyMs: 0,
			draftTokens: 0,
			totalDraftTokens: session.tokenTotal,
			estimatedBytes: 0,
			projectionCoverage: [],
			validationMs: 0,
			validationBytes: 0,
			validationFiles: 0,
			projectionMs: 0,
			...input,
		};
	};

	/** Every producer joins or registers work before yielding. Validation never grants a second launch. */
	const admitCandidate = async (
		session: Session,
		input: Pick<Candidate, "key" | "route" | "worldParent"> & { readonly kind?: SpeculativeDraftCandidate["type"] },
		create: () => Candidate,
		active: () => boolean,
		attach: (candidate: Candidate, created: boolean) => void,
	): Promise<void> => {
		let rejected: Set<Candidate> | undefined;
		while (active()) {
			const { entry: candidate, inserted } = candidateStore.getOrCreate(session.id, input.key, create, (existing, match) => {
					const execution = existing.work.execution;
					return existing.owner.draft.type === (input.kind ?? "tool_call") &&
						!rejected?.has(existing) && activeExecution(existing) && candidateWorld(existing) === input.worldParent &&
						(sameSpeculativeExecutionRoute(existing.route, input.route) ||
							(existing.route.reuse === "shared_result" && input.route.reuse === "shared_result" && execution.status === "succeeded" &&
								session.scheduler.assessCompatibility(execution.output.compatibility, input.key.executionFingerprint).compatible)) &&
						(match.kind === "exact" || (existing.route.reuse === "shared_result" || execution.status !== "succeeded") &&
							actionKeyCovers(existing.key, input.key, projectionRules));
				});
			if (!inserted && candidate.work.execution.status === "succeeded") {
				const validation = await validateCandidate(candidate);
				if (validation.status !== "valid" || !candidateStore.has(session.id, candidate)) {
					(rejected ??= new Set()).add(candidate);
					if (validation.status === "stale") invalidateCandidates(session, [candidate], validation.cause);
					continue;
				}
			}
			if (active()) attach(candidate, inserted);
			else if (inserted) discardCandidate(session, candidate, cause("control", "admission_withdrawn"), false);
			return;
		}
	};

	const attachActorPreview = (record: ActorPreviewRecord, candidate: Candidate, ownership: "existing" | "preview"): void => {
		record.state = { status: "candidate", candidateID: candidate.id, ownership };
		(candidate.previews ??= new Set()).add(record);
	};

	const clearLaunchTimers = (session: Session): void => {
		for (const timer of session.launchTimers.values()) clearTimeout(timer);
		session.launchTimers.clear();
	};

	const startTurn = async (input: StartInput, signal?: AbortSignal): Promise<void> => {
		const settings = await adapter.settings();
		if (!settings.enabled || masterDisabled()) {
			await disableSession(input.sessionID);
			return;
		}
		if (signal?.aborted) return;
		const definitions = adapter.definitions(input);
		const names = candidateToolNames(settings, semantics);
		if (!definitions.length) return;
		const session = sessionFor(input.sessionID, settings);
		await session.lifecycle.run(async () => {
			if (signal?.aborted || masterDisabled()) return;
			const previous = session.turns.get(input.turnID);
			if (previous) await closeTurn(previous);
			const data = await adapter.stateData(input);
			if (signal?.aborted || masterDisabled() || session.lifecycle.sealed) return;
			session.settings = settings;
			const generation = new SourceGeneration(signal);
			const startedAt = performance.now();
			session.timeline ??= new TaskTimeline(startedAt);
			const state: Turn = {
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
				signal,
				decisionSequence: session.decisionSequence + 1,
				actorActions: new Set(),
				actorToolHints: new Set(),
				actorPreviews: new Map(),
				actorDecisionStartedAt: startedAt,
				lifecycle: "active",
			};
			session.turns.set(input.turnID, state);
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
		turnID: string,
		targetDecisionSequence: number,
		limit: number,
		requestKind: SourceRequestKind,
		expiresAtTarget = true,
		parent?: AbortSignal,
	): SourceRequestSlot | undefined => {
		const slots = [...session.sourceSlots].filter((slot) => slot.request.source === source &&
			slot.request.targetDecisionSequence === targetDecisionSequence &&
			(slot.request.kind === "observation") === (requestKind === "observation"));
		const observed = requestKind === "observation" && slots.find((slot) => slot.request.turnID === turnID);
		if (observed) {
			observed.pending++;
			return observed;
		}
		if (slots.length >= limit) return undefined;
		const slot: SourceRequestSlot = {
			request: { source, turnID, index: session.sourceRequestSequence++, kind: requestKind, targetDecisionSequence },
			expiresAtTarget,
			generation: new SourceGeneration(parent),
			owners: new Set(),
			pending: 1,
		};
		session.sourceSlots.add(slot);
		return slot;
	};

	const releaseSourceSlot = (session: Session, slot: SourceRequestSlot, failure: ResolutionCause): void => {
		if (!session.sourceSlots.delete(slot)) return;
		slot.generation.expire(failure);
	};

	const releaseUnusedSourceSlot = (session: Session, slot: SourceRequestSlot): void => {
		if (!slot.pending && slot.owners.size === 0) {
			releaseSourceSlot(session, slot, cause("control", "source_slot_unused"));
		}
	};

	const releaseSourceRequest = (session: Session, slot: SourceRequestSlot): void => {
		slot.pending--;
		releaseUnusedSourceSlot(session, slot);
	};

	const cancelCompetingProposals = (session: Session, winner: SourceRequestSlot): void => {
		for (const slot of [...session.sourceSlots]) {
			if (
				slot === winner ||
				!session.sourceSlots.has(slot) ||
				slot.request.kind !== "proposal" ||
				slot.request.source !== winner.request.source ||
				slot.request.targetDecisionSequence !== winner.request.targetDecisionSequence
			)
				continue;
			releaseSourceSlot(session, slot, cause("source", "proposal_race_lost"));
		}
	};

	const expireSourceHorizon = (session: Session, decisionSequence: number, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) {
			if (slot.expiresAtTarget && slot.request.targetDecisionSequence <= decisionSequence)
				releaseSourceSlot(session, slot, failure);
		}
	};

	const releaseAllSourceSlots = (session: Session, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) releaseSourceSlot(session, slot, failure);
	};

	const trackSourceTask = <Value>(session: Session, task: Promise<Value>): Promise<Value> => {
		session.sourceTasks.add(task);
		void task
			.finally(() => session.sourceTasks.delete(task))
			.catch(() => {
				// Request failure is already represented by its source settlement.
			});
		return task;
	};

	const launchSourceRequests = (state: Turn): void => {
		if (!state.candidateNames.length) return;
		for (const source of sources) {
			if (!source.enabled(state.settings)) continue;
			const count = clampCandidateLimit(source.proposalCount?.(state.settings));
			for (let index = 0; index < count; index++) {
				const slot = claimSourceSlot(
					state.session,
					source.id,
					state.turnID,
					state.decisionSequence,
					count,
					"proposal",
					source.requestLifetime === "actor_decision",
					state.generation.signal,
				);
				if (!slot) break;
				const pending = requestSource({ ...turnContext(state), session: state.session, slot }, source, (signal) =>
					source.propose({
						...turnContext(state),
						definitions: state.definitions,
						candidateNames: state.candidateNames,
						proposalIndex: index,
						proposalCount: count,
						signal,
					}));
				trackSourceTask(state.session, pending);
			}
		}
	};

	/** Initial and continuation requests retain the same identity, production and admission owners. */
	const requestSource = (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		produce: (signal: AbortSignal) => ReturnType<NonNullable<Source["continue"]>>,
	): Promise<void> => {
		const { session, slot } = scope;
		session.pendingSourceRequests++;
		return runSourceRequest({
			request: slot.request,
			generation: slot.generation,
			timeoutMs: source.timeoutMs?.(scope.settings),
			produce: (signal) => trackSourceTask(session, Promise.resolve(produce(signal))),
			count: (value) => asUpdates(value).length,
		}).then(async (request) => {
			session.pendingSourceRequests = Math.max(0, session.pendingSourceRequests - 1);
			try {
				queueSourceRequestEvent(session, slot.request.turnID, scope.settings, request);
				if (request.settlement.status === "produced" && request.value !== undefined &&
					session.sourceSlots.has(slot) && slot.generation.active) {
					await admitUpdates(scope, source, request.value, request);
				}
			} finally {
				releaseSourceRequest(session, slot);
			}
		});
	};

	const admitUpdates = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		updates: PlanUpdate | readonly PlanUpdate[] | undefined,
		request?: SettledSourceRequest,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active) return;
		await Promise.allSettled(asUpdates(updates).map(async (update) => {
			const captured = PlanRuntime.capture(update, source.multiStepEnabled?.(scope.settings) !== false);
			if (!("update" in captured)) return;
			session.pendingAdmissions++;
			try {
				// Capture the batch before binding callbacks can mutate producer input.
				await Promise.resolve();
				await applyUpdate(scope, source, captured.update, request);
			} finally {
				session.pendingAdmissions--;
			}
		}));
	};

	const applyUpdate = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		update: PlanUpdate,
		request?: SettledSourceRequest,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active) return;
		if (update.source !== source.id) return;
		const draftTokens = finiteMetric(update.draftTokens);
		const applied = session.plan.apply(update, session.decisionSequence);
		if (!applied.accepted) return;
		for (const retired of applied.retired) retirePlanAction(session, retired, cause("plan", "superseded"));
		session.tokenTotal += draftTokens;
		const materializations: Promise<void>[] = [];
		for (const action of applied.upserted) {
			const node = session.plan.get(applied.plan.id, action.id);
			if (!node || node.predictionState.status !== "pending") continue;
			const context = session.actionContexts.get(node.identity.id);
			const issued = !context;
			if (issued) {
				const admissionController = new AbortController();
				scope.slot.owners.add(node.identity.id);
				session.actionContexts.set(node.identity.id, {
					identity: node.identity,
					opportunity: session.plan.opportunity(node.proposalID, node.action.id)!,
					feedback: action.feedback,
					...turnContext(scope),
					attemptStartedAt: request?.startedAt ?? performance.now(),
					predictionLatencyMs: request?.durationMs ?? 0,
					draftTokens,
					totalDraftTokens: session.tokenTotal,
					draft: planActionDraft(node),
					admissionSignal: AbortSignal.any([scope.slot.generation.signal, admissionController.signal]),
					admissionController,
					sourceSlot: scope.slot,
					continuationTriggers: new Set(),
					continuationSlots: new Set(),
					continuationTail: Promise.resolve(),
				});
			} else {
				context.feedback = action.feedback;
				context.draft = planActionDraft(node);
			}
			for (const notify of [issued ? source.onIssued : undefined, source.onAdmitted]) {
				if (notify) session.effects.enqueue(() => notify.call(source, {
					proposalID: node.identity.proposalID, actionID: node.identity.actionID, feedback: action.feedback,
				}));
			}
			if (issued) materializations.push(materializeAction(session, node).finally(() => dispatchReady(session)));
		}
		await Promise.allSettled(materializations);
		if (!materializations.length) dispatchReady(session);
	};

	const executionRouteFor = async (input: Omit<Parameters<typeof adapter.preflightCandidate>[0], "route">): Promise<
		| { readonly ok: true; readonly route: SpeculativeExecutionRoute }
		| { readonly ok: false; readonly cause: ResolutionCause }
	> => {
		let route: SpeculativeExecutionRoute | undefined;
		try {
			const { callID, index, ...request } = input;
			route = await adapter.resolveExecution(request);
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
			const preflight = await adapter.preflightCandidate({ ...input, route });
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
		if (!concrete || !context.settings.enabled || !candidateToolNames(context.settings, semantics).includes(node.action.tool)) {
			failUnlaunchable(session, node, cause("admission", concrete ? "tool_disabled" : "invalid_input"));
			return;
		}
		let predictedAction: ActionKey | undefined;
		try {
			predictedAction = await adapter.actionKey(node.action.tool, concrete, {
				type: "start",
				startInput: context.startInput,
				data: context.data,
				...(node.action.operation ? { operation: node.action.operation } : {}),
			});
		} catch {
			predictedAction = undefined;
		}
		if (context.admissionSignal.aborted) {
			failUnlaunchable(session, node, cause("source", "generation_expired"));
			return;
		}
		if (!predictedAction) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		const executionInput = asConcreteInput(predictedAction.input);
		if (!executionInput) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		if (!session.plan.bindActionKey(node.identity, predictedAction)) return;
		// Binding owns schema validation and argument preparation; raw proposals cannot win the race.
		const slot = context.sourceSlot;
		if (session.sourceSlots.has(slot) && slot.request.kind === "proposal" &&
			sourcesByID.get(node.source)?.concurrentProposalPolicy?.(context.settings) === "first_produced")
			cancelCompetingProposals(session, slot);
		const onCandidateMaterialized = adapter.onCandidateMaterialized;
		if (onCandidateMaterialized && node.action.type === "tool_call") {
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
					executionAction: predictedAction,
					...definedFields(node.action, [
						"depth", "horizon", "conditionalProbability", "empiricalProbability",
						"expectedLatencyBenefitMs", "expectedDurationMs",
					]),
				}),
			);
		}
		const admission = await executionRouteFor({
			...turnContext(context),
			candidate: context.draft,
			tool: predictedAction.tool,
			action: predictedAction,
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
			session.plan.finishPreparation(node.identity, admission.cause);
			return;
		}
		context.executionRoute = admission.route;
		session.plan.finishPreparation(node.identity);
	};

	const releaseActionContext = (session: Session, id: string, keepContinuation = false): void => {
		const context = session.actionContexts.get(id);
		if (!context) return;
		session.actionContexts.delete(id);
		context.admissionController.abort(cause("control", "prediction_retired"));
		context.sourceSlot.owners.delete(id);
		releaseUnusedSourceSlot(session, context.sourceSlot);
		if (!keepContinuation)
			for (const slot of context.continuationSlots)
				releaseSourceSlot(session, slot, cause("control", "parent_prediction_not_adopted"));
	};

	const failUnlaunchable = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		session.plan.rejectExecution(node.identity, failure);
		settleUnobserved(session, node, failure);
	};

	const settleBlockedPlanActions = (session: Session): void => {
		for (const node of session.plan.drainBlocked()) {
			failUnlaunchable(session, node, cause("plan", "dependency_impossible"));
		}
	};

	const pendingActorTurn = (session: Session): Turn | undefined =>
		[...session.turns.values()]
			.find(
				(turn) =>
					turn.lifecycle === "active" &&
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
			const forecast = forecastFor(node, session.decisionSequence, actorPhase);
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

	const launchNode = (session: Session, node: PlanRuntimeNode): Promise<void> => session.lifecycle.track(Promise.resolve().then(async () => {
		const current = session.plan.get(node.proposalID, node.action.id);
		if (session.lifecycle.sealed || current?.identity.id !== node.identity.id || current.predictionState.status === "settled") return;
		if (!node.actionKey) {
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
		await admitCandidate(session, { key: node.actionKey, route, worldParent: parent, kind: node.action.type }, () => {
			const scheduled = session.scheduler.evaluate([
				forecastFor(node, session.decisionSequence, actorPhaseFor(session)),
			]);
			return createCandidate(session, context, context.draft, {
				origin: "prediction",
				key: node.actionKey!,
				route,
				...(parent ? { worldParent: parent } : {}),
				attemptStartedAt: context.attemptStartedAt,
				predictionLatencyMs: context.predictionLatencyMs,
				draftTokens: context.draftTokens,
				totalDraftTokens: context.totalDraftTokens,
				expectedDurationMs: scheduled.expectedDurationMs,
			});
		}, () => {
			const current = session.plan.get(node.proposalID, node.action.id);
			return !session.lifecycle.sealed && current?.identity.id === node.identity.id &&
				current.predictionState.status !== "settled" && current.execution.status === "scheduled";
		}, (candidate, created) => {
			if (!created) attachNode(session, node, candidate);
			else {
				session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work);
				startQueuedCandidates(session);
			}
		});
	}));

	const attachNode = (session: Session, node: PlanRuntimeNode, candidate: Candidate): void => {
		if (!session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work)) return;
		const forecasts = forecastsForCandidate(session, candidate);
		const scheduled = session.scheduler.refresh(candidate, forecasts) ?? session.scheduler.evaluate(forecasts);
		candidate.expectedDurationMs = Math.max(candidate.expectedDurationMs, scheduled.expectedDurationMs);
		const execution = candidate.work.execution;
		if (execution.status === "succeeded") {
			queueContinuation(session, node, candidate, execution.output.output, "execution_succeeded");
			return;
		}
		if (execution.status === "queued") startQueuedCandidates(session);
	};

	/** Coalesce producer requests behind the current Actor events; a concrete Actor intent stays immediate. */
	const startQueuedCandidates = (session: Session, preferred?: Candidate): void => {
		if (session.lifecycle.sealed) return;
		if (preferred) return launchCandidateBatch(session, preferred);
		if (session.pendingLaunch || !candidateStore.pending(session.id).some((candidate) => candidate.work.execution.status === "queued")) return;
		session.pendingLaunch = session.lifecycle.track(new Promise<void>(setImmediate).then(() => {
			session.pendingLaunch = undefined;
			if (!session.lifecycle.sealed && !masterDisabled()) launchCandidateBatch(session);
		}));
	};

	const launchCandidateBatch = (session: Session, preferred?: Candidate): void => {
		const actorToolHints = pendingActorTurn(session)?.actorToolHints;
		const queued = (preferred ? [preferred] : candidateStore.pending(session.id))
			.filter(
				(candidate) =>
					candidate.work.execution.status === "queued" &&
					(!actorToolHints?.size || actorToolHints.has(candidate.key.tool)),
			)
			.flatMap((candidate) => {
				const forecasts = forecastsForCandidate(session, candidate);
				if (!forecasts.length) {
					retireUndemandedCandidate(session, candidate, cause("retention", "prediction_horizon_settled"));
					return [];
				}
				return [{ candidate, forecasts, work: session.scheduler.evaluate(forecasts) }];
			})
			.sort(
				(left, right) =>
					Number(!reservationAvailable(right.candidate.work.reservation)) -
						Number(!reservationAvailable(left.candidate.work.reservation)) ||
					Number(left.work.background) - Number(right.work.background) ||
					left.work.decisionBatchesUntilCall - right.work.decisionBatchesUntilCall ||
					right.work.priorityMs - left.work.priorityMs ||
					right.work.criticalPathMs - left.work.criticalPathMs ||
					right.work.expectedDurationMs - left.work.expectedDurationMs ||
					left.candidate.createdAt - right.candidate.createdAt,
			);
		for (const { candidate, forecasts, work } of queued) {
			if (candidate.work.execution.status !== "queued") continue;
			const admission = session.scheduler.admit(
				candidate,
				forecasts,
				concurrentLimit(session.settings),
				reservationAvailable(candidate.work.reservation) ? "producer" : "actor",
				work,
				candidate.previews?.size ? undefined : actionTimingIdentity(candidate.key),
			);
			if (!admission.admitted && admission.reason === "budget_exhausted" && !work.background) {
				for (const victim of session.scheduler.preemptFor(
					admission.work.resourceUnits,
					concurrentLimit(session.settings),
					(victim) => {
						if (victim.work.execution.status !== "running" || !reservationAvailable(victim.work.reservation)) return false;
						const other = session.scheduler.evaluate(forecastsForCandidate(session, victim));
						return other.background || work.decisionBatchesUntilCall < other.decisionBatchesUntilCall ||
							(work.decisionBatchesUntilCall === other.decisionBatchesUntilCall && work.priorityMs > other.priorityMs);
					},
				)) {
					discardCandidate(session, victim, cause("admission", "scheduler_preempted"), false);
				}
				// Only executor completion, after cleanup, can admit the next batch.
			}
			if (!admission.admitted) continue;
			candidate.expectedDurationMs = work.expectedDurationMs;
			const startedAt = performance.now();
			if (!candidate.work.start(startedAt)) continue;
			queueCandidateEvent(session, candidate);
			void session.lifecycle.track(executeCandidate(session, candidate, startedAt));
		}
	};

	const executeCandidate = async (session: Session, candidate: Candidate, startedAt: number): Promise<void> => {
		let branch: WorldBranch<Output> | undefined;
		try {
			const parent = candidateWorld(candidate);
			candidate.acceptOperationScope = scope => {
				const turn = session.turns.get(scope.turnID);
				if (session.lifecycle.sealed || masterDisabled() || scope.sessionID !== session.id ||
					turn?.lifecycle !== "active" || candidate.work.controller.signal.aborted || !candidateStore.has(session.id, candidate)) return false;
				return session.plan.matchable(turn.decisionSequence).some(node =>
					"candidateID" in node.execution && node.execution.candidateID === candidate.id);
			};
			if (candidate.owner.draft.type === "operation") candidate.onOperationAdopted = adoption => {
				const turn = session.turns.get(adoption.scope.turnID);
				if (session.lifecycle.sealed || session.id !== adoption.scope.sessionID || !turn ||
					candidate.owner.draft.operation?.identity !== adoption.operationIdentity) return;
				const actorAction: ActorActionIdentity = { id: adoption.id, kind: "operation", sequence: adoption.sequence,
					decisionSequence: turn.decisionSequence, turnID: turn.turnID };
				candidate.actorAdopted = true;
				for (const node of session.plan.consumers(candidate.id)) {
					const opportunity = session.plan.claimMatch(node.proposalID, node.action.id, actorAction, { kind: "exact", distance: 0 });
					const settled = opportunity && session.plan.confirm(opportunity, actorAction, { status: "adopted", candidateID: candidate.id });
					if (settled) predictionSettled(session, node, settled);
				}
			};
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
				onOperationAdopted: candidate.onOperationAdopted,
				acceptOperationScope: candidate.acceptOperationScope,
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
			if (!candidate.work.succeed(branch, new TimelineInterval(startedAt, completedAt, branch.computationDependencies), completedAt - startedAt)) {
				await session.lifecycle.release(branch);
				return;
			}
			session.scheduler.observeSpeculativeService(
				actionTimingIdentity(candidate.key),
				completedAt - startedAt,
			);
			candidateStore.settle(session.id, candidate, candidate.work.reservation.kind === "shared");
			queueCandidateContinuations(
				session,
				session.plan.consumers(candidate.id),
				candidate,
				output,
				"execution_succeeded",
			);
			trimResults(session, candidate.owner.settings);
			queueCandidateEvent(session, candidate);
			if (candidate.owner.draft.type === "operation" && candidate.actorAdopted)
				retireUndemandedCandidate(session, candidate, cause("retention", "operation_adopted"));
		} catch (error) {
			if (candidate.work.execution.status !== "succeeded") await session.lifecycle.release(branch);
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
			if (settled && candidate.work.execution.status === "failed")
				session.scheduler.observeSpeculativeService(actionTimingIdentity(candidate.key), completedAt - startedAt, true);
			removeCandidate(session.id, candidate);
			if (settled) queueCandidateEvent(session, candidate);
		} finally {
			session.scheduler.complete(candidate);
			dispatchReady(session);
		}
	};

	const previewActorTool = async (
		input: { readonly sessionID: SessionID; readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	): Promise<void> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterDisabled()) return;
		state.actorToolHints.add(input.tool);
		await Promise.all(
			nearestPredictions(state.session, state.decisionSequence, (node) => node.action.tool === input.tool ? { node } : undefined).map(
				({ node }) => promoteForActor(state.session, node),
			),
		);
		if (signal?.aborted || state.lifecycle !== "active" || state.session.turns.get(state.turnID) !== state) return;
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
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterDisabled()) {
			return Promise.resolve();
		}
		const actualCall = adapter.actual(input);
		if (!actualCall.id) return state.session.lifecycle.track(promoteActorCall(state, input, actualCall, undefined, signal));
		const existing = state.actorPreviews.get(actualCall.id);
		if (existing) return existing.task;
		const record: ActorPreviewRecord = {
			actionKey: actorActionKey(input, actualCall),
			task: Promise.resolve(),
			state: { status: "pending" },
		};
		state.actorPreviews.set(actualCall.id, record);
		record.task = state.session.lifecycle.track(promoteActorCall(state, input, actualCall, record, signal));
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
			state.session.turns.get(state.turnID) === state &&
			!masterDisabled();
		const action = await (record?.actionKey ?? actorActionKey(input, actualCall));
		if (!action || !active()) return;
		await Promise.all(
			predictionMatches(state.session, action, state.decisionSequence).map(({ node }) =>
				promoteForActor(state.session, node),
			),
		);
		if (!active()) return;
		const preferred = rankCandidates(state.session, action)[0];
		if (preferred) {
			const { candidate, match } = preferred;
			if (record) attachActorPreview(record, candidate, "existing");
			if (candidate.work.execution.status === "queued") startQueuedCandidates(state.session, candidate);
			const execution = candidate.work.execution;
			if (!record || match.kind === "exact" || candidate.route.reuse !== "shared_result" || execution.status !== "succeeded" ||
				candidate.resultViews?.has(action.key) || candidate.estimatedBytes + action.key.length * 2 + 64 >= cacheByteLimit(state.settings)) return;
			await new Promise<void>(setImmediate);
			if (!active() || state.actorPreviews.get(actualCall.id!) !== record) return;
			const lease = acquireCandidate(state.session, candidate, `preview:${callKey(state.turnID, actualCall.id!)}`);
			if (!lease) return;
			try {
				// A streamed intent may prepare sealed data, but grants no freshness or commit authority.
				const projected = await projectOutput(candidate, action, execution.output.output, match,
					projectionRules, { action, args: action.input, callID: actualCall.id!,
						signal: signal ?? state.generation.signal });
				if (projected.ok && active() && candidateStore.get(state.sessionID, candidate.id) === candidate &&
					retainResultView(candidate, action, projected, state.settings)) trimResults(state.session, state.settings);
			} finally {
				lease.release();
			}
			return;
		}
		if (!record) return;
		await new Promise<void>(setImmediate);
		if (!active()) return;
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
			...turnContext(state),
			candidate: draft,
			tool: action.tool,
			action,
			concrete,
			callID: actualCall.id ?? callKey(state.turnID, actualCall.tool),
			index: state.session.candidateSequence,
			signal: executionSignal,
		});
		if (!admission.ok || !active()) return;
		const { route } = admission;
		const forecast: PredictionForecast = {
			tool: actualCall.tool,
			executionFingerprint: action.executionFingerprint,
			actionKeyHash: action.hash,
			decisionBatchesUntilCall: 0,
			actorPhase: actorPhaseFor(state.session),
		};
		await admitCandidate(state.session, { key: action, route }, () => {
			const scheduled = state.session.scheduler.evaluate([forecast]);
			return createCandidate(state.session, state, draft, {
				origin: "actor_preview",
				key: action,
				route,
				attemptStartedAt,
				expectedDurationMs: scheduled.expectedDurationMs,
			});
		}, active, (candidate, created) => {
			attachActorPreview(record, candidate, created ? "preview" : "existing");
			startQueuedCandidates(state.session, candidate);
		});
	};

	const abandonActorPreview = (
		state: Turn,
		record: ActorPreviewRecord | undefined,
		failure: ResolutionCause,
	): void => {
		if (!record) return;
		const current = record.state;
		record.state = { status: "cancelled" };
		if (current.status !== "candidate") return;
		const candidate = candidateStore.get(state.sessionID, current.candidateID);
		if (!candidate) return;
		candidate.previews?.delete(record);
		retireUndemandedCandidate(state.session, candidate, failure);
	};

	/** Results may outlive their consumers; work that has not started still needs an owner. */
	const retireUndemandedCandidate = (session: Session, candidate: Candidate, failure: ResolutionCause): void => {
		if (!reservationAvailable(candidate.work.reservation) || candidate.previews?.size || session.plan.consumers(candidate.id).length) return;
		if (candidate.owner.draft.type === "operation" && candidate.actorAdopted && candidate.work.execution.status === "running") return;
		if (candidate.work.execution.status === "queued" || candidate.work.reservation.kind === "exclusive" ||
			(candidate.origin === "actor_preview" && !candidate.actorAdopted)) discardCandidate(session, candidate, failure, false);
	};

	const beginAuthoritativeResultCapture = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		action: ActionKey,
		signal?: AbortSignal,
	): Promise<void> => {
		if (!adapter.captureAuthoritativeResult || !state.actorActions.has(actorAction)) return;
		const concrete = asConcreteInput(actualCall.input);
		if (!concrete) return;
		const captureSignal = signal ?? state.generation.signal;
		if (captureSignal.aborted) return;
		let capture: AuthoritativeResultCapture<Output> | undefined;
		try {
			capture = await adapter.captureAuthoritativeResult({
				...turnContext(state),
				consumeInput: input,
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
			state.session.turns.get(state.turnID) !== state ||
			masterDisabled() ||
			!state.actorActions.has(actorAction) ||
			!actorAction.capture(capture)
		) {
			state.session.lifecycle.release(capture);
			return;
		}
	};

	const selectActorCandidate = async (input: ActorSelectionInput): Promise<void> => {
		const { state, actualCall, actualKey, actorAction, ranked, actorArrivedAt, preview, signal } = input;
		const matchingCandidates = ranked.map(({ candidate }) => candidate);
		const readyJoins = new Map<string, CandidateJoinDecision>();
		const stopCandidate = (candidate: Candidate): boolean => {
			const failure = signal?.aborted
				? cause("control", "actor_aborted")
				: masterDisabled() ||
					state.lifecycle !== "active" ||
					state.session.turns.get(state.turnID) !== state
					? cause("control", "disabled")
					: undefined;
			if (!failure) return false;
			actorAction.setFallback(failure, candidate.id);
			return true;
		};

		for (const choice of ranked) {
			const candidate = choice.candidate;
			const executionAtDecision = candidate.work.execution;
			const actorIdentity = actionTimingIdentity(actualKey), route = candidate.route;
			const adoptionIdentity = {
				...actorIdentity,
				actionKeyHash: JSON.stringify([candidate.key.hash, actualKey.hash]),
				operation: JSON.stringify([route.backend, route.fingerprint, route.scope, route.isolation, route.reuse,
					choice.match.kind === "exact" ? "exact" : choice.match.projector,
					...(candidate.resultViews?.has(actualKey.key) ? ["retained"] : [])]),
			};
			// Equivalent ready choices share this Actor decision, including its recovery probe.
			const readyKey = executionAtDecision.status === "succeeded" ? JSON.stringify(adoptionIdentity) : undefined;
			const join = (readyKey ? readyJoins.get(readyKey) : undefined) ?? state.session.scheduler.assessCandidateJoin({
				identity: actionTimingIdentity(candidate.key),
				actorIdentity, adoptionIdentity,
				state:
					executionAtDecision.status === "succeeded"
						? "succeeded"
						: executionAtDecision.status === "running"
							? "running"
							: "queued",
				expectedSpeculativeDurationMs: candidate.expectedDurationMs,
				...(executionAtDecision.status === "running"
					? { elapsedMs: Math.max(0, performance.now() - executionAtDecision.startedAt) }
					: {}),
			});
			if (readyKey) readyJoins.set(readyKey, join);
			if (!join.allowed) {
				actorAction.rejectCandidate(
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
			const reservation = acquireCandidate(state.session, candidate, actorAction.identity.id);
			if (!reservation) {
				actorAction.rejectCandidate(candidate.id, choice.match, cause("matching", "candidate_reserved"));
				continue;
			}
			const attemptStartedAt = performance.now();
			let waitMs = 0;
			try {
				if (candidate.work.execution.status === "queued") {
					preemptForActor(
						state.session,
						state.settings,
						matchingCandidates,
					);
					startQueuedCandidates(state.session, candidate);
				}
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
					actorAction.rejectCandidate(candidate.id, choice.match, authorization);
					continue;
				}
				const waitStartedAt = performance.now();
				const waiting = await waitForCandidate(
					candidate.work.completion,
					signal,
					join.reason === "ready" ? undefined : join.waitBudgetMs,
				);
				waitMs = performance.now() - waitStartedAt;
				if (stopCandidate(candidate)) break;
				if (waiting.status === "aborted") {
					actorAction.setFallback(cause("control", "actor_aborted"), candidate.id);
					break;
				}
				if (waiting.status === "deadline") {
					actorAction.rejectCandidate(
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
					actorAction.rejectCandidate(candidate.id, choice.match, execution.cause);
					continue;
				}
				const branch = execution.output;
				const compatibility = state.session.scheduler.assessCompatibility(
					branch.compatibility,
					actualKey.executionFingerprint,
				);
				if (!compatibility.compatible) {
					const failure = cause("compatibility", compatibility.code, compatibility.detail);
					actorAction.rejectCandidate(candidate.id, choice.match, failure);
					discardCandidate(state.session, candidate, failure);
					continue;
				}

				// Join only this exact Actor intent; changed arguments or executors cannot inherit its preparation.
				if (preview?.state.status === "candidate" && preview.state.candidateID === candidate.id &&
					(await preview.actionKey)?.key === actualKey.key) await waitForCandidate(preview.task, signal);
				if (stopCandidate(candidate)) break;
				// Evaluate sealed data first, then prove freshness once immediately before commit.
				const projection = await projectOutput(
					candidate,
					actualKey,
					branch.output,
					choice.match,
					projectionRules,
					{ action: actualKey, args: actualCall.input, callID: actualCall.id ?? actualKey.hash,
						signal: signal ?? state.generation.signal },
				);
				if (stopCandidate(candidate)) break;
				if (!projection.ok) {
					actorAction.rejectCandidate(candidate.id, choice.match, projection.cause);
					continue;
				}
				const validation = await validateCandidate(candidate);
				if (stopCandidate(candidate)) break;
				if (validation.status !== "valid") {
					actorAction.rejectCandidate(candidate.id, choice.match, validation.cause);
					if (validation.status === "stale") invalidateCandidates(state.session, [candidate], validation.cause);
					continue;
				}
				let output = projection.output;
				try {
					const committed = await branch.commit();
					if (choice.match.kind === "exact") output = committed;
				} catch (error) {
					const commitFailure = effectCommitFailure(error, "poisoned");
					if (isPoisonedEffectCommit(commitFailure)) throw commitFailure;
					const failure = commitFailure.resolutionCause ??
						cause("commit", "world_commit_failed", errorDetail(commitFailure));
					actorAction.rejectCandidate(candidate.id, choice.match, failure);
					discardCandidate(state.session, candidate, failure);
					continue;
				}

				reservation.adopt();
				if (reservation.kind === "exclusive") {
					removeCandidate(state.session.id, candidate);
				} else {
					if (preview) candidate.previews?.delete(preview);
					const retained = retainResultView(candidate, actualKey, projection, state.settings);
					candidateStore.recordActorHit(state.sessionID, candidate, cacheLimits(state.settings));
					if (retained) trimResults(state.session, state.settings);
				}
				actorAction.select({
					candidate,
					match: choice.match,
					output,
					timing: {
						executionAheadMs: Math.min(execution.executionMs, Math.max(0, actorArrivedAt - execution.toolExecution.startedAt)),
						attemptLeadMs: Math.max(0, actorArrivedAt - candidate.attemptStartedAt),
						hitLatencyMs: Math.max(0, performance.now() - actorArrivedAt),
						...(join.expectedActorMs === undefined ? {} : { expectedActorMs: join.expectedActorMs }),
					},
					toolExecution: execution.toolExecution,
					...(projection.execution ? { projection: projection.execution } : {}),
				});
				break;
			} finally {
				reservation.release();
				state.session.scheduler.observeAdoption(adoptionIdentity, Math.max(0, performance.now() - attemptStartedAt - waitMs));
			}
		}
	};

	const prepareActorCall = async (input: ConsumeInput, signal?: AbortSignal): Promise<PreparedActorCall<Output> | undefined> => {
		const actorArrivedAt = performance.now();
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || state.lifecycle !== "active" || signal?.aborted || masterDisabled())
			return undefined;
		const actualCall = adapter.actual(input);
		let preview = actualCall.id ? state.actorPreviews.get(actualCall.id) : undefined;
		if (actualCall.id) state.actorPreviews.delete(actualCall.id);
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
		const sequence = ++state.session.sequence;
		const actualKey = await actorActionKey(input, actualCall);
		if (state.lifecycle !== "active" || state.session.turns.get(state.turnID) !== state ||
			signal?.aborted || masterDisabled()) {
			abandonActorPreview(state, preview, cause("control", signal?.aborted ? "actor_aborted" : "disabled"));
			return undefined;
		}
		const actorAction = new ActorAction<Candidate, Output>({
			identity: { id: actualCall.id ?? JSON.stringify([input.turnID, sequence]), sequence,
				decisionSequence: state.decisionSequence, turnID: input.turnID },
			tool: actualCall.tool,
			...(actualKey ? { actionKey: actualKey } : {}),
			fallback: cause("matching", "no_candidate"),
		});
		const identity = actorAction.identity;
		state.actorActions.add(actorAction);
		state.actorObservation ??= actualKey ? identity : null;
		let capturePreparationMs = 0;
		const prepared: { output?: Output; observeOperations: boolean; settle: PreparedActorCall<Output>["settle"] } = {
			observeOperations: state.settings.enabled && sources.some(source => source.observesOperations && source.observe && source.enabled(state.settings)),
			settle: (toolExecution, output, operations) => state.session.lifecycle.track(
				settleActorCall(state, input, actualCall, actorAction, output, capturePreparationMs, toolExecution, operations && Object.freeze([...operations]))),
		};
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

		try {
			if (!actualKey) {
				const failure = cause("matching", "action_not_keyable");
				abandonActorPreview(state, preview, failure);
				actorAction.deferToFallback([], undefined, failure);
				preemptForActor(state.session, state.settings);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				return Object.freeze(prepared);
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
			const ranked = rankCandidates(state.session, actualKey, previewCandidateID);
			const blockedPrediction = matchingPredictions.find(
				({ node }) => node.execution.status === "execution_blocked" || node.execution.status === "preparing",
			)?.node;
			actorAction.setFallback(
				blockedPrediction?.execution.status === "execution_blocked"
					? blockedPrediction.execution.cause
					: blockedPrediction?.execution.status === "preparing" ? cause("admission", "preparation_pending")
					: cause("matching", ranked.length ? "candidate_unavailable" : "no_candidate"),
			);
			await selectActorCandidate({
				state,
				consumeInput: input,
				actualCall,
				actualKey,
				actorAction,
				ranked,
				actorArrivedAt,
				...(preview ? { preview } : {}),
				...(signal ? { signal } : {}),
			});
			const selected = actorAction.selection;
			if (selected) {
				const predictionIdentities = matchingPredictions.map(({ opportunity }) => opportunity.identity);
				const previewed = preview?.state.status === "candidate" && preview.state.ownership === "preview" &&
					preview.state.candidateID === selected.candidate.id;
				const adoption = actorAction.settleSelection(predictionIdentities, previewed ? "preview" : "speculative");
				if (!adoption) return Object.freeze(prepared);
				state.actorActions.delete(actorAction);
				reconcileAdoptedCandidate(state.session, actualKey, selected.candidate);
				if (!previewed) queueCandidateContinuations(
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
				queueActorSettlement(state, input, actualCall, actorAction, selected.output, selected);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				prepared.output = selected.output;
				return Object.freeze(prepared);
			}

			abandonActorPreview(state, preview, actorAction.fallback.cause);
			const adoption = actorAction.deferToFallback(
				matchingPredictions.map(({ opportunity }) => opportunity.identity),
				executionBlockedAttemptLead(state.session, matchingPredictions, actorArrivedAt),
			);
			if (adoption) confirmPredictions(state.session, matchingPredictions, identity, adoption);
			const effect = semantics.effect(actualKey);
			preemptForActor(state.session, state.settings);
			state.session.effects.enqueue(() => dispatchReady(state.session));
			if (adapter.captureAuthoritativeResult && effect === "observation") {
				const startedAt = performance.now();
				await beginAuthoritativeResultCapture(state, input, actualCall, actorAction, actualKey, signal);
				capturePreparationMs = Math.max(0, performance.now() - startedAt);
			}
			return Object.freeze(prepared);
		} finally {
			abandonActorPreview(state, preview, actorAction.fallback.cause);
			actorAction.deferToFallback();
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
			state.session.lifecycle.release(capture);
			return;
		}
		let retained = false;
		try {
			if (state.lifecycle !== "active" || state.session.lifecycle.sealed || masterDisabled()) return;
			const candidate = createCandidate(state.session, state,
				{ type: "tool_call", tool: action.tool, input: action.input, source: "actor_result" }, {
				origin: "actor_result",
				key: action,
				route: capture.route,
				attemptStartedAt: toolExecution.startedAt,
				expectedDurationMs: durationMs,
				estimatedBytes: estimateValueBytes(output) + branch.capturedBytes,
				projectionCoverage: captureCoverage(action, output, projectionRules),
			});
			candidate.work.start(toolExecution.startedAt);
			if (!candidate.work.succeed(branch, toolExecution, durationMs)) return;
			const rejected = adapter.rejectCandidateOutput?.({ output, candidate: publicCandidate(candidate) });
			if (rejected) return;
			candidateStore.settle(state.sessionID, candidate);
			retained = true;
			trimResults(state.session, state.settings);
		} catch {
			// Optional cache promotion cannot alter an already completed Actor result.
		} finally {
			if (!retained) state.session.lifecycle.release(branch);
		}
	};

	const settleActorCall = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		output: Output | undefined,
		capturePreparationMs: number,
		toolExecution: TimelineInterval,
		operations?: readonly ExecutionOperationBinding[],
	): Promise<void> => {
		if (!state.actorActions.delete(actorAction)) return;
		const settlementStartedAt = performance.now();
		const capture = actorAction.takeCapture();
		const settlement = actorAction.settleActor(toolExecution, outputIsError(output));
		if (!settlement) {
			state.session.lifecycle.release(capture);
			return;
		}
		const execution = settlement.provider.toolExecution, durationMs = execution.completedAt - execution.startedAt;
		const key = actorAction.actionKey;
		if (key) reconcileAuthoritativeEffects(state.session, key);
		// Authoritative feedback must enter the settlement queue before optional cache work can yield.
		queueActorSettlement(state, input, actualCall, actorAction, output, undefined, operations);
		if (capture && key && output !== undefined && !outputIsError(output)) {
			await promoteAuthoritativeResult(state, key, output, durationMs, execution, capture);
		} else if (capture) {
			state.session.lifecycle.release(capture);
		}
		// Compare complete alternatives; optional capture waits belong to the native path.
		if (key) state.session.scheduler.observeActorService(actionTimingIdentity(key),
			durationMs + capturePreparationMs + Math.max(0, performance.now() - settlementStartedAt));
	};

	const queueActorSettlement = (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		output: Output | undefined,
		selection?: ActorCandidateSelection<Candidate, Output>,
		operations?: readonly ExecutionOperationBinding[],
	): void => {
		const settlement = actorAction.settlement;
		if (!settlement) return;
		state.session.timeline?.recordTool(settlement.provider.toolExecution);
		if (selection?.projection) state.session.timeline?.recordTool(selection.projection);
		const key = actorAction.actionKey;
		const settledCandidate = selection?.candidate;
		const settledCandidateDescriptor = settledCandidate && (adapter.onActorActionSettled || adapter.onEvent)
			? Object.freeze(candidateEventDescriptor(settledCandidate))
			: undefined;
		const event: SpeculativeActionEvent<SessionID> | undefined = adapter.onEvent ? {
			type: "actor_action",
			...eventEnvelope(state.session, state.turnID, state.settings),
			settlement,
			actualAction: diagnosticAction(actorAction.tool, actualCall.input, key),
			...(settledCandidate ? { execution: settledCandidate.route.isolation } : {}),
			...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
		} : undefined;
		state.session.effects.enqueue(() => adapter.onActorActionSettled?.({
			sessionID: state.sessionID,
			turnID: state.turnID,
			...(key ? { action: key } : {}),
			settlement,
			...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
			candidateFeedback: settledCandidate?.owner.draft.feedback,
		}));
		if (event) state.session.effects.enqueue(() => { state.session.events.enqueue(event); });
		for (const source of sources) {
			try {
				if (!source.observe) continue;
				const observation = cloneSharedData({
					concrete: asConcreteInput(actualCall.input) ?? {}, ...(output !== undefined ? { output } : {}),
				});
				state.session.effects.enqueue(async () => {
					if (!source.enabled(state.settings)) return;
					const updates = await source.observe?.({
						...turnContext(state),
						consumeInput: input,
						...(key ? { action: key } : {}),
						tool: actorAction.tool,
						...observation,
						operations: operations ?? (settledCandidate && candidateBranch(settledCandidate)?.operations),
						durationMs:
							settlement.provider.kind === "actor"
								? settlement.provider.durationMs
								: executionDuration(settledCandidate),
						order: settlement.actorAction.sequence,
					});
					const target = state.decisionSequence + 1;
					if (state.lifecycle !== "active" || !state.generation.active ||
						target <= state.session.decisionSequence || !asUpdates(updates).length) return;
					// Real observations remain ordered; their next-decision preparation survives normal turn closure.
					const slot = claimSourceSlot(state.session, source.id, state.turnID, target, 1, "observation", true, state.signal);
					if (slot) void trackSourceTask(state.session, admitUpdates(
						{ ...turnContext(state), session: state.session, slot }, source, updates,
					).finally(() => releaseSourceRequest(state.session, slot)));
				});
			} catch { /* Unowned data can settle normally but cannot train a source. */ }
		}
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
		const observation = state.actorObservation;
		if (observation === undefined) return;
		state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
		for (const node of state.session.plan.due(state.decisionSequence)) {
			if (node.predictionState.status !== "pending") continue;
			if (node.action.type === "operation") {
				settleUnobserved(state.session, node, cause("matching", "operation_not_observed"));
				continue;
			}
			if (!observation) {
				settleUnobserved(state.session, node, cause("matching", "actor_action_not_keyable"));
				continue;
			}
			const settlement = state.session.plan.miss(node.proposalID, node.action.id, observation);
			if (settlement) predictionSettled(state.session, node, settlement);
		}
		dispatchReady(state.session);
	};

	const predictionSettled = (session: Session, node: PlanRuntimeNode, settlement: PredictionSettlement): void => {
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return;
		const source = sourcesByID.get(context.identity.source);
		const event: SpeculativeActionEvent<SessionID> | undefined = adapter.onEvent ? {
			type: node.action.type === "operation" ? "operation_prediction" : "prediction",
			...eventEnvelope(session, context.startInput.turnID, context.settings),
			settlement,
		} : undefined;
		session.effects.enqueue(() => adapter.onPredictionSettled?.({
			sessionID: session.id,
			turnID: context.startInput.turnID,
			tool: node.action.tool,
			...(node.actionKey ? { action: node.actionKey } : {}),
			settlement,
		}));
		session.effects.enqueue(async () => {
			if (event) session.events.enqueue(event);
			await source?.onSettled?.({
				proposalID: context.identity.proposalID,
				actionID: context.identity.actionID,
				feedback: context.feedback,
				settlement,
			});
		});
		const adopted =
			settlement.observation === "observed" &&
			settlement.match.matched &&
			settlement.match.adoption.status === "adopted";
		releaseActionContext(session, node.identity.id, adopted);
		if ("candidateID" in node.execution && node.execution.candidateID) {
			const candidate = candidateStore.get(session.id, node.execution.candidateID);
			if (candidate) retireUndemandedCandidate(session, candidate, cause("retention", "prediction_horizon_settled"));
		}
	};

	const settleUnobserved = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const settlement = session.actionContexts.get(node.identity.id)?.opportunity.unobserve(failure);
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
		if (node.action.type === "operation") return;
		const current = session.plan.get(node.proposalID, node.action.id);
		if (current?.identity.id !== node.identity.id) return;
		const context = session.actionContexts.get(node.identity.id);
		const source = context ? sourcesByID.get(context.identity.source) : undefined;
		if (context && source?.continuationBatch && trigger === "execution_succeeded") {
			try { queuePeerContinuations(session, current, context, source); } catch { /* Producer feedback is advisory. */ }
		}
		if (!context || !source?.continue) return;
		if (source.multiStepEnabled?.(context.settings, context.feedback) === false) return;
		if (context.continuationTriggers.has(trigger)) return;
		context.continuationTriggers.add(trigger);
		try {
			const filter = source.continueOn;
			if (filter && !(typeof filter === "function"
				? filter({ actionID: node.action.id, feedback: context.feedback, output, trigger })
				: filter.includes(trigger))) return;
		} catch { return; } // Producer feedback cannot alter completed execution.
		const parentDecisionSequence =
			current.predictionState.status === "matching"
				? (current.predictionState.actorAction.decisionSequence ?? current.expectedDecisionSeq)
				: current.expectedDecisionSeq;
		const targetDecisionSequence = parentDecisionSequence + 1;
		const pending = context.continuationTail
			.then(() => requestContinuation(session, source, [context], targetDecisionSequence, (signal) => {
				const revision = session.plan.reserveRevision(node.proposalID);
				if (revision === undefined) return undefined;
				return source.continue!({
					...turnContext(context),
					candidate: predictionCandidate(candidate, node), ...(adoptedAction ? { adoptedAction } : {}),
					proposalID: node.proposalID, actionID: node.action.id, revision,
					feedback: context.feedback, output, trigger, signal,
				});
			}))
			.catch(() => {
				// Continuation failure cannot revoke completed work or Actor adoption.
			});
		context.continuationTail = pending;
		trackSourceTask(session, pending);
	};

	const requestContinuation = async (
		session: Session,
		source: Source,
		parents: readonly PlanActionContext<StartInput, StateData>[],
		targetDecisionSequence: number,
		produce: (signal: AbortSignal) => ReturnType<NonNullable<Source["continue"]>>,
	): Promise<void> => {
		const context = parents[0]!;
		if (session.lifecycle.sealed || targetDecisionSequence <= session.decisionSequence || parents.some(({ identity }) =>
			session.plan.get(identity.proposalID, identity.actionID)?.identity.id !== identity.id)) return;
		const slot = claimSourceSlot(session, source.id, context.startInput.turnID, targetDecisionSequence,
			clampCandidateLimit(source.proposalCount?.(context.settings)), "continuation");
		if (!slot) return;
		for (const parent of parents) parent.continuationSlots.add(slot);
		await requestSource({ ...turnContext(context), session, slot }, source, produce);
	};

	const queuePeerContinuations = (
		session: Session, node: PlanRuntimeNode, context: PlanActionContext<StartInput, StateData>, source: Source,
	): void => {
		const peers = sources.filter((peer) => peer.id !== source.id && peer.continueFrom &&
			peer.enabled(context.settings) && peer.multiStepEnabled?.(context.settings) !== false);
		if (!peers.length) return;
		const ids = source.continuationBatch!({ proposalID: node.proposalID, actionID: node.action.id, feedback: context.feedback });
		if (!ids?.length || !ids.includes(node.action.id) || new Set(ids).size !== ids.length) return;
		const batch: Parameters<NonNullable<Source["continueFrom"]>>[0]["batch"][number][] = [];
		const parents: PlanActionContext<StartInput, StateData>[] = [];
		for (const id of ids) {
			const parent = session.plan.get(node.proposalID, id);
			const owner = parent && session.actionContexts.get(parent.identity.id);
			if (!parent || parent.action.type !== "tool_call" || !owner || owner.admissionSignal.aborted || parent.predictionState.status !== "pending" ||
				parent.expectedDecisionSeq !== session.decisionSequence + 1 || parent.action.dependsOn?.length ||
				parent.execution.status !== "succeeded") return;
			const candidate = candidateStore.get(session.id, parent.execution.candidateID);
			if (candidate?.work.execution.status !== "succeeded") return;
			batch.push({ identity: parent.identity, candidate: predictionCandidate(candidate, parent), output: candidate.work.execution.output.output });
			parents.push(owner);
		}
		const requested = parents[0]!.peerContinuations ??= new Set();
		for (const peer of peers) {
			if (requested.has(peer.id)) continue;
			requested.add(peer.id);
			const pending = requestContinuation(session, peer, parents, node.expectedDecisionSeq + 1, async (requestSignal) => {
				const signal = AbortSignal.any([requestSignal, ...parents.map((parent) => parent.admissionSignal)]);
				if (signal.aborted) return undefined;
				const update = await peer.continueFrom!({ ...turnContext(context), batch, signal });
				return signal.aborted ? undefined : update;
			}).catch(() => { /* Peer prediction cannot revoke the completed parent batch. */ });
			trackSourceTask(session, pending);
		}
	};

	const retirePlanAction = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const timer = session.launchTimers.get(node.identity.id);
		if (timer) clearTimeout(timer);
		session.launchTimers.delete(node.identity.id);
		const opportunity = session.actionContexts.get(node.identity.id)?.opportunity;
		if (opportunity?.state.status === "matching") return;
		const finalized = opportunity?.unobserve(failure);
		if (finalized) predictionSettled(session, node, finalized);
		else releaseActionContext(session, node.identity.id);
	};

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
			const parentNode = session.plan.dependency(node.proposalID, dependency);
			if (parentNode?.action.type === "operation") continue;
			if (!parentNode || !("candidateID" in parentNode.execution) || !parentNode.execution.candidateID) continue;
			const candidate = candidateStore.get(session.id, parentNode.execution.candidateID);
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
	): readonly PredictionForecast[] => {
		const nodes = session.plan.consumers(candidate.id);
		const actorPhase = actorPhaseFor(session);
		if (nodes.length) return nodes.map((node) => forecastFor(node, session.decisionSequence, actorPhase));
		if (!candidate.previews?.size && reservationAvailable(candidate.work.reservation)) return [];
		return [
			{
				tool: candidate.key.tool,
				executionFingerprint: candidate.key.executionFingerprint,
				actionKeyHash: candidate.key.hash,
				expectedDurationMs: candidate.expectedDurationMs,
				decisionBatchesUntilCall: 0,
				...(actorPhase ? { actorPhase } : {}),
			},
		];
	};

	const validateCandidate = async (candidate: Candidate): Promise<ResourceValidation> => {
		const validation = await validateWorldBranch(candidateBranch(candidate), candidate.route.reuse);
		recordValidation(candidate, validation);
		return validation;
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
		session: Session,
		action: ActionKey,
		preferred?: string,
	) => {
		const now = performance.now();
		return candidateStore.lookup(session.id, action, (candidate) => candidate.work.execution.status !== "succeeded")
			.flatMap(({ entry: candidate, match }) => {
				const execution = candidate.work.execution;
				if (candidate.owner.draft.type !== "tool_call" || !activeExecution(candidate) || candidateWorld(candidate) !== undefined) return [];
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
					Number(right.candidate.id === preferred) - Number(left.candidate.id === preferred) ||
					Number(right.ready) - Number(left.ready) ||
					left.remainingMs - right.remainingMs ||
					left.match.distance - right.match.distance ||
					right.candidate.createdAt - left.candidate.createdAt,
			);
	};

	const nearestPredictions = <Selection extends { readonly node: PlanRuntimeNode }>(
		session: Session,
		decisionSequence: number,
		select: (node: PlanRuntimeNode) => Selection | undefined,
	): readonly Selection[] => {
		const selected = new Map<string, Selection>();
		for (const node of session.plan.matchable(decisionSequence)) {
			if (!node.actionKey || node.action.type !== "tool_call") continue;
			const selection = select(node);
			if (!selection) continue;
			const previous = selected.get(node.proposalID)?.node.expectedDecisionSeq;
			const expected = node.expectedDecisionSeq;
			// Prefer the latest due action, otherwise the nearest future action; retain insertion-order ties.
			if (previous === undefined || (expected <= decisionSequence
				? previous > decisionSequence || expected > previous
				: expected < previous)) selected.set(node.proposalID, selection);
		}
		return [...selected.values()];
	};

	const predictionMatches = (
		session: Session,
		action: ActionKey,
		decisionSequence: number,
	): readonly { readonly node: PlanRuntimeNode; readonly relation: ActionKeyMatch }[] =>
		nearestPredictions(session, decisionSequence, (node) => {
			const relation = actionKeyMatch(node.actionKey!, action, projectionRules, true);
			return relation ? { node, relation } : undefined;
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
		settings: SpeculativeActionSettings,
		protectedCandidates: readonly Candidate[] = [],
	): void => {
		for (const candidate of session.scheduler.preemptFor(
			1,
			concurrentLimit(settings),
			(candidate) => candidate.work.execution.status === "running" && !protectedCandidates.includes(candidate) && reservationAvailable(candidate.work.reservation),
		)) {
			discardCandidate(session, candidate, cause("admission", "preempted_by_actor"));
		}
	};

	const discardCandidate = (
		session: Session,
		candidate: Candidate,
		failure: ResolutionCause,
		dispatch = true,
	): void => {
		const state = candidate.work.execution;
		if (state.status !== "queued" && state.status !== "running") {
			removeCandidate(session.id, candidate);
			return;
		}
		const startedAt = state.status === "running" ? state.startedAt : performance.now();
		const completedAt = performance.now();
		const settled = candidate.work.cancel(failure, completedAt, Math.max(0, completedAt - startedAt));
		removeCandidate(session.id, candidate);
		if (settled) queueCandidateEvent(session, candidate);
		if (dispatch) dispatchReady(session);
	};

	const invalidateCandidates = (session: Session, candidates: Iterable<Candidate>, failure: ResolutionCause): void => {
		let invalidated = false;
		for (const candidate of new Set(candidates)) {
			if (candidateStore.get(session.id, candidate.id) !== candidate) continue;
			discardCandidate(session, candidate, failure, false);
			session.plan.rearmExecution(candidate.id);
			invalidated = true;
		}
		if (invalidated) dispatchReady(session);
	};

	const reconcileStores = async (state: Turn): Promise<void> => {
		const available = new Set(state.definitions.map((definition) => definition.name));
		for (const candidate of candidateStore.values(state.sessionID)) {
			if (!available.has(candidate.key.tool) ||
				(candidate.work.execution.status === "queued" && !state.candidateNames.includes(candidate.key.tool)))
				discardCandidate(state.session, candidate, cause("control", "tool_disabled"));
		}
		trimResults(state.session, state.settings);
	};

	const trimResults = (session: Session, settings: SpeculativeActionSettings): void => {
		for (const candidate of candidateStore.trim(
			session.id,
			cacheLimits(settings),
			(entry) => !entry.previews?.size && reservationAvailable(entry.work.reservation),
		)) {
			removeCandidate(session.id, candidate);
		}
	};

	const reconcileAdoptedCandidate = (session: Session, action: ActionKey, adopted: Candidate): void => {
		reconcileAuthoritativeEffects(session, action, adopted);
		adopted.actorAdopted = true;
	};

	const authoritativeMutationResources = (action: ActionKey, adopted?: Candidate): readonly string[] => {
		if (semantics.effect(action) === "observation") return [];
		if (adopted?.work.reservation.kind === "shared") return [];
		return adopted ? (candidateBranch(adopted)?.resources ?? action.resources) : action.resources;
	};

	const reconcileAuthoritativeEffects = (session: Session, action: ActionKey, adopted?: Candidate): void => {
		const changed = authoritativeMutationResources(action, adopted);
		if (!changed.length) return;
		const candidates = candidateStore.values(session.id);
		const invalid = new Set<Candidate>();
		for (const candidate of candidates) {
			if (candidate === adopted || (adopted && descendsFrom(candidate, adopted))) continue;
			// Once an Actor reserves a candidate, its freshness and compatibility checks
			// are authoritative. Cache invalidation may only retire unclaimed work.
			if (!reservationAvailable(candidate.work.reservation)) continue;
			if (candidate.key.resources.some((resource) => changed.some((path) => resourcePathsOverlap(resource, path)))) {
				for (const descendant of candidates) {
					// Completed shared outputs are checked against their sealed evidence at every adoption.
					// Pending work and checkpoint descendants still retain conservative conflict invalidation.
					if (descendant === candidate && candidate.work.execution.status === "succeeded" && candidate.work.reservation.kind === "shared") continue;
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
		session.timeline = undefined;
		session.lastActorArrivedAt = undefined;
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
		for (const node of state.session.plan.pending()) {
			if ((!node.actionKey || node.execution.status === "preparing") &&
				state.session.actionContexts.get(node.identity.id)?.admissionSignal.aborted)
				failUnlaunchable(state.session, node, cause("source", "generation_expired"));
		}
		for (const preview of state.actorPreviews.values()) {
			abandonActorPreview(state, preview, input.failure);
		}
		state.actorPreviews.clear();
		return { state, completedAt, terminal: input.terminal, notifyHost: input.notifyHost };
	};

	const clearActorActions = (state: Turn): void => {
		for (const action of state.actorActions) {
			const capture = action.takeCapture();
			if (capture) state.session.lifecycle.release(capture);
		}
		state.actorActions.clear();
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
		state.session.turns.delete(state.turnID);
		clearActorActions(state);
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

	const closeSessionState = async (session: Session, mode: SessionClosureMode, turnID = ""): Promise<void> => {
		const terminal = mode === "terminal";
		if (terminal && !session.timeline && session.turns.size === 0) return;
		const failure = cause(
			"control",
			terminal ? "terminal_turn" : mode === "disposed" ? "session_disposed" : "disabled",
		);
		const closures = [...session.turns.values()].flatMap((state) => {
			const closure = beginTurnClosure(state, { failure, terminal, notifyHost: terminal });
			return closure ? [closure] : [];
		});

		releaseAllSourceSlots(session, failure);
		const planFailure = terminal ? cause("control", "session_terminal") : failure;
		for (const node of session.plan.unsettled()) settleUnobserved(session, node, planFailure);
		clearLaunchTimers(session);
		session.plan.clear();
		while (session.sourceTasks.size) await Promise.allSettled(session.sourceTasks);
		await session.effects.flush();
		for (const closure of closures) await completeTurnClosure(closure);
		for (const state of session.turns.values()) clearActorActions(state);
		session.turns.clear();

		for (const candidate of candidateStore.values(session.id)) {
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
		pruneActionContexts(session);
		if (!terminal) await session.lifecycle.drain();
	};

	/** Called only inside the owning session's lifecycle lane, including replacement on start. */
	const closeTurn = async (state: Turn): Promise<void> => {
		const closure = beginTurnClosure(state, {
			failure: cause("control", "turn_finished"),
			terminal: false,
			notifyHost: true,
		});
		if (!closure) return;
		await state.session.effects.flush();
		await completeTurnClosure(closure);
	};

	const finishTurn = async (input: FinishInput): Promise<void> => {
		const session = sessionStates.get(input.sessionID);
		if (!session) return;
		await session.lifecycle.run(() => {
			if (input.terminal === true) return closeSessionState(session, "terminal", input.turnID);
			const state = session.turns.get(input.turnID);
			return state && closeTurn(state);
		});
	};

	const settingsChanged = async (settings: SpeculativeActionSettings): Promise<void> => {
		masterEnabled = settings.enabled;
		if (settings.enabled) return;
		await Promise.all(
			[...sessionStates.keys()].map((sessionID) => disableSession(sessionID)),
		);
	};

	const disableSession = async (sessionID: SessionID): Promise<void> => {
		const session = sessionStates.get(sessionID);
		if (!session) return;
		await session.lifecycle.run(() => closeSessionState(session, "disabled"));
	};

	const disposeSession = async (sessionID: SessionID): Promise<void> => {
		const session = sessionStates.get(sessionID);
		if (!session) return;
		await session.lifecycle.close(async () => {
			await closeSessionState(session, "disposed");
			await session.effects.close();
			await session.events.close({ drain: false });
			sessionStates.delete(sessionID);
		});
	};

	const dispose = async (): Promise<void> => {
		await Promise.all([...sessionStates.keys()].map((sessionID) => disposeSession(sessionID)));
	};

	const inspect = (sessionID?: SessionID): SpeculativeRuntimeInspection => {
		const selectedSessions =
			sessionID === undefined ? [...sessionStates.values()] : maybe(sessionStates.get(sessionID));
		const candidates =
			sessionID === undefined
				? candidateStore.allValues()
				: candidateStore.values(sessionID);
		const planNodes = selectedSessions.flatMap((session) => session.plan.values());
		const telemetry = selectedSessions.map((session) => session.events.snapshot());
		return {
			activeTurns: selectedSessions.reduce((total, session) => total + session.turns.size, 0),
			exclusiveCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "exclusive").length,
			sharedCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "shared").length,
			pendingPredictions: selectedSessions.reduce(
				(total, session) => total + session.pendingSourceRequests + session.pendingAdmissions,
				0,
			),
			deferredPlanActions: planNodes.filter((node) => node.execution.status === "deferred" || node.execution.status === "preparing").length,
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
		const candidates = candidateStore.values(session.id);
		const segments = candidateStore.snapshot(session.id);
		const resultEntries = segments.coldEntries + segments.hotEntries;
		let exclusiveCandidates = 0, branchEntries = 0, branchBytes = 0;
		for (const candidate of candidates) {
			if (candidate.work.reservation.kind !== "exclusive") continue;
			exclusiveCandidates++;
			if (candidate.work.execution.status === "succeeded") { branchEntries++; branchBytes += candidate.estimatedBytes; }
		}
		return {
			cacheCapacity: settings.resourceCacheMaxEntries,
			cacheByteCapacity: cacheByteLimit(settings),
			cacheCold: segments.coldEntries,
			cacheHot: segments.hotEntries,
			inFlightJobs: candidates.length - resultEntries - branchEntries,
			resultEntries,
			resultBytes: segments.coldBytes + segments.hotBytes,
			branchEntries,
			branchBytes,
			exclusiveCandidates,
			sharedCandidates: candidates.length - exclusiveCandidates,
			cacheTools: [...new Set(candidates.map((candidate) => candidate.key.tool))].sort(),
			cacheExecutions: [...new Set(candidates.map((candidate) => candidate.route.isolation))].sort(),
		};
	};

	/** Capture at the state transition; policy callbacks may delay delivery but cannot change the snapshot. */
	const eventEnvelope = (session: Session, turnID: string, settings: SpeculativeActionSettings) => ({
		sessionID: session.id,
		turnID,
		timestamp: Date.now(),
		cache: cacheSnapshot(session, settings),
	});

	const queueTaskEvent = (session: Session, turnID: string, completedAt: number): void => {
		const timeline = session.timeline;
		if (!timeline) return;
		resetTaskTimeline(session);
		if (adapter.onEvent) session.events.enqueue({
			type: "task", ...eventEnvelope(session, turnID, session.settings), timing: timeline.measure(completedAt),
		});
	};

	const queueSourceRequestEvent = (
		session: Session,
		turnID: string,
		settings: SpeculativeActionSettings,
		result: SettledSourceRequest,
	): void => {
		if (adapter.onEvent) session.events.enqueue({
			type: "source_request", ...eventEnvelope(session, turnID, settings),
			request: { request: result.request, startedAt: result.startedAt, durationMs: result.durationMs, settlement: result.settlement },
		});
	};

	const queueCandidateEvent = (session: Session, candidate: Candidate): void => {
		if (!adapter.onEvent) return;
		const state = candidateExecutionProjection(candidate);
		if (!state) return;
		const descriptor = candidateEventDescriptor(candidate);
		session.events.enqueue({
			type: "candidate", ...eventEnvelope(session, candidate.owner.startInput.turnID, candidate.owner.settings),
			candidate: descriptor, state,
		});
	};

	return {
		startTurn,
		previewActorTool,
		previewActorCall,
		prepareActorCall: (input, signal) => {
			const task = prepareActorCall(input, signal);
			return sessionStates.get(input.sessionID)?.lifecycle.track(task) ?? task;
		},
		finishTurn,
		settingsChanged,
		disposeSession,
		dispose,
		inspect,
	};

	function recordValidation(candidate: Candidate, validation: ResourceValidation): void {
		candidate.validationMs += finiteMetric(validation.metrics.durationMs);
		candidate.validationBytes += finiteMetric(validation.metrics.bytesRead);
		candidate.validationFiles += finiteMetric(validation.metrics.filesRead);
		candidate.validationMode = validation.metrics.mode;
	}
}
