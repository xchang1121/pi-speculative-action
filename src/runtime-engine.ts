import path from "node:path";
import { type ActionProjectionCoverage, type ActionProjectionRule, resolveActionProjectionRules } from "./action-key-projection.ts";
import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import { actionKeyCovers, actionKeyMatch, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { ActorAction, type ActorCandidateSelection } from "./actor-action.ts";
import { CandidateExecution, type CandidateReservation } from "./candidate-execution.ts";
import { CandidateStore, resultCacheRecency } from "./candidate-stores.ts";
import { candidateToolNames, clampCandidateLimit, DEFAULTS, type DrafterToolDefinition } from "./common.ts";
import { nonNegativeFinite as finiteMetric, positiveCount } from "./number-utils.ts";
import { errorDetail } from "./error-utils.ts";
import { diagnosticAction } from "./diagnostics.ts";
import { effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import type { CandidateEventDescriptor } from "./events.ts";
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
	SpeculativeRuntimeInspection, TurnInput,
} from "./runtime-contracts.ts";
import { type PredictionForecast, type ExecutionIdentity, type ScheduledWork, SpeculationScheduler, waitForCompletion } from "./scheduler.ts";
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
import { TaskTimeline, TimelineInterval, type ComputationReuseShare } from "./task-timing.ts";
import type { HardwareResources } from "./system-resources.ts";
import { normalizeSchedulingSettings, SCHEDULING_DEFAULTS } from "./scheduling-settings.ts";

class CandidateFailure extends Error {
	readonly failure: ResolutionCause;

	constructor(failure: ResolutionCause) { super(failure.detail ?? failure.code); this.failure = failure; }
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

function cacheByteLimit(settings: SpeculativeActionSettings): number {
	return typeof settings.resourceCacheMaxBytes === "number" && Number.isFinite(settings.resourceCacheMaxBytes)
		? Math.max(1, Math.floor(settings.resourceCacheMaxBytes))
		: DEFAULTS.resourceCacheMaxBytes;
}

function cacheLimits(settings: SpeculativeActionSettings) {
	return { maxEntries: positiveCount(settings.resourceCacheMaxEntries), maxBytes: cacheByteLimit(settings), hotFraction: 0.8 };
}

function definedFields<T, K extends keyof T>(value: T, keys: readonly K[]): Partial<Pick<T, K>> {
	const result: Partial<Pick<T, K>> = {};
	for (const key of keys) if (value[key] !== undefined) result[key] = value[key];
	return result;
}

function forecastFor(node: PlanRuntimeNode, decisionSequence: number): PredictionForecast {
	return {
		tool: node.action.tool,
		// Internal producers keep their backend identity separate from their permission anchor.
		...(node.actionKey && !node.action.producesOperations ? actionExecutionIdentity(node.actionKey) : {}),
		...definedFields(node.action, ["expectedDurationMs", "resourceDemand", "adoptionProbability", "confidence"]),
		decisionBatchesUntilCall: Math.max(0, node.expectedDecisionSeq - decisionSequence),
		criticalPathSteps: node.criticalPathSteps,
		...(node.action.empiricalProbability !== undefined ? { hitProbability: node.action.empiricalProbability } : {}),
		...(node.action.background ? { background: true } : {}),
		actorDemand: node.predictionState.status === "matching",
		...(node.dependenciesReady && (node.action.dependsOn?.length ?? 0) > 0 && (node.action.horizon ?? 0) <= 0 ? { dependenciesResolved: true } : {}),
	};
}

function planActionDraft(node: PlanRuntimeNode): SpeculativeDraftCandidate {
	const { id, background: _background, ...draft } = node.action;
	return { ...draft, source: node.source, proposalID: node.proposalID, actionID: id };
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

function predictionCandidate<Output>(candidate: CandidateRecord<Output>, node: PlanRuntimeNode): SpeculativeCandidate {
	return {
		...publicCandidate(candidate),
		source: node.source,
		empiricalProbability: node.action.empiricalProbability,
		adoptionProbability: node.action.adoptionProbability,
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

function captureCoverage<Output>(action: ActionKey, output: Output, rules: readonly ActionProjectionRule<Output>[]): readonly ActionProjectionCoverage[] {
	return rules.flatMap((rule) => {
		try { const value = rule.captureCoverage?.(action, output); return value === undefined ? [] : [{ rule: rule.id, value: cloneSharedData(value) }]; } catch { return []; }
	});
}

async function projectOutput<Output>(candidate: CandidateRecord<Output>, actor: ActionKey, match: ActionKeyMatch,
	rules: readonly ActionProjectionRule<Output>[],
	request: Parameters<NonNullable<WorldBranch<Output>["reconstruct"]>>[0]): Promise<ProjectionResult<Output>> {
	const branch = candidateBranch(candidate)!;
	if ((branch.inputsOnly || candidate.outputStale) && match.kind !== "inputs") return { ok: false, cause: cause("projection", "input_only_branch") };
	if (match.kind === "exact") return { ok: true, output: branch.output };
	const retained = candidate.resultViews?.get(actor.key);
	// A view kept without its proof is served only by a branch that can still commit and validate as a whole.
	if (retained && (retained.validate || !candidate.outputStale && !branch.inputsOnly)) {
		const output = cloneSharedData(retained.output);
		if (retained.resource) retained.resource.references++;
		return { ok: true, ...retained, output, reused: true };
	}
	const reconstruct = branch.reconstruct;
	const rule = match.kind === "projected" ? rules.find((item) => item.id === match.projector) : undefined;
	if (match.kind === "projected" && !rule) return { ok: false, cause: cause("projection", "rule_missing") };
	const coverage = candidate.projectionCoverage.find((item) => item.rule === rule?.id);
	if (!reconstruct && (!coverage || !rule?.projectOutput)) return { ok: false, cause: cause("projection", "coverage_missing") };
	let rebuilt: Awaited<ReturnType<NonNullable<typeof reconstruct>>>;
	let transferred = false;
	try {
		const evaluation = await TimelineInterval.measure(async () => {
			let projected: Output | undefined = match.kind === "projected" && coverage && rule?.projectOutput ? cloneSharedData(await rule.projectOutput({
				speculative: candidate.key, actor, output: branch.output, coverage: cloneSharedData(coverage.value), keyMatch: match,
			})) : undefined;
			if (projected === undefined) { rebuilt = await reconstruct?.(request); if (rebuilt) projected = rebuilt.output; }
			return projected;
		});
		const projected = evaluation.output;
		if ((rebuilt?.requiresQueryValidation || candidate.outputStale) && !rebuilt?.validate) return { ok: false, cause: cause("projection", "query_proof_missing") };
		if (projected === undefined) return { ok: false, cause: cause("projection", "view_not_covered") };
		const execution = evaluation.computation;
		const resource = rebuilt?.dispose ? { dispose: rebuilt.dispose.bind(rebuilt), references: 1 } : undefined;
		transferred = true;
		return { ok: true, output: projected, execution, inputs: !!rebuilt, compatibility: rebuilt?.compatibility,
			validate: rebuilt?.validate, capturedBytes: rebuilt?.capturedBytes, requiresQueryValidation: rebuilt?.requiresQueryValidation, resource };
	} catch (error) {
		return { ok: false, cause: cause("projection", "reconstruction_failed", errorDetail(error)) };
	} finally { if (!transferred) await Promise.resolve().then(() => rebuilt?.dispose?.()).catch(() => {}); }
}

function callKey(turnID: string, callID: string): string { return JSON.stringify([turnID, callID]); }

function outputIsError(value: unknown): boolean {
	return Boolean(value && typeof value === "object" && (value as { readonly isError?: unknown }).isError === true);
}

function candidateEventDescriptor<Output>(
	candidate: CandidateRecord<Output>,
): CandidateEventDescriptor {
	const branch = candidateBranch(candidate);
	return {
		source: candidate.owner.draft.source ?? "cache",
		...(candidate.owner.draft.mode ? { mode: candidate.owner.draft.mode } : {}),
		depth: candidate.owner.draft.depth ?? 0,
		id: candidate.id,
		origin: candidate.origin,
		...(candidate.owner.draft.type === "operation" ? { kind: "operation" as const } : {}),
		tool: candidate.key.tool,
		route: candidate.route,
		...(branch ? { world: { backend: branch.backend, executionMetrics: branch.executionMetrics } } : {}),
		predictedAction: diagnosticAction(candidate.key.tool, candidate.key.input, candidate.key),
	};
}

function executionDuration<Output>(
	candidate: CandidateRecord<Output> | undefined,
): number { const execution = candidate?.work.execution; return execution && "executionMs" in execution ? execution.executionMs : 0; }

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

function inputIndexBytes(branch: WorldBranch<unknown>): number {
	return branch.reconstruct ? branch.inputResources?.reduce((bytes, input) => bytes + input.path.length * 2 + 64, 0) ?? 0 : 0;
}

/** Relative resources mean their key's root, never the host process working directory. */
function rootedResource(key: ActionKey, resource: string): string {
	return key.resourceRoot !== undefined || path.isAbsolute(resource) ? path.resolve(key.resourceRoot ?? "", resource) : resource;
}

function resourcePathsOverlap(left: string, right: string): boolean { return containsLogicalPath(left, right) || containsLogicalPath(right, left); }

function maybe<T>(value: T | undefined): T[] { return value === undefined ? [] : [value]; }

function actionExecutionIdentity(action: ActionKey): ExecutionIdentity {
	return { tool: action.tool, semanticsEpoch: action.semanticsEpoch, executionFingerprint: action.executionFingerprint, actionKeyHash: action.hash };
}

interface PlanActionContext<StartInput, StateData> extends RuntimeTurnContext<StartInput, StateData> {
	readonly identity: PlanActionIdentity;
	readonly opportunity: PredictionOpportunity;
	draft: SpeculativeDraftCandidate;
	readonly admissionController: AbortController;
	readonly sourceSlot: SourceRequestSlot;
	readonly continuationSlots: Set<SourceRequestSlot>;
	readonly continuationTriggers: Set<"execution_succeeded" | "actor_adopted">;
	continuationTail: Promise<void>;
	peerContinuations?: Set<string>;
	executionRoute?: SpeculativeExecutionRoute;
	launch?: Promise<void>;
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
	readonly owner: RuntimeTurnContext<StartInput, StateData> & { readonly draft: SpeculativeDraftCandidate; readonly index: number; };
	expectedDurationMs?: number;
	estimatedBytes: number;
	projectionCoverage: readonly ActionProjectionCoverage[];
	outputStale?: boolean;
	resultViews?: Map<string, RetainedResultView<Output>>;
	previews?: Set<ActorPreviewRecord>;
	onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	operationJoinable?: () => boolean;
	operationConsumers?: Set<object>;
	actorConsumers?: Set<object>;
	admissionValidation?: Promise<ResourceValidation>;
}

type ActorPreviewState =
	| { readonly status: "pending" }
	| { readonly status: "candidate"; readonly candidateID: string; readonly ownership: "existing" | "preview" }
	| { readonly status: "cancelled" };

interface ActorPreviewRecord {
	readonly controller: AbortController; actionKey?: ActionKey; task: Promise<void>; state: ActorPreviewState; actorDemand?: boolean;
}

interface SessionState<SessionID, Output, StartInput, StateData> {
	readonly id: SessionID;
	readonly lifecycle: RuntimeLifecycleLane;
	readonly plan: PlanRuntime;
	readonly effects: PostSettlementQueue;
	readonly events: BoundedEventQueue<SpeculativeActionEvent<SessionID>>;
	readonly actionContexts: Map<string, PlanActionContext<StartInput, StateData>>;
	readonly sourceSlots: Set<SourceRequestSlot>;
	readonly sourceTasks: Set<Promise<unknown>>;
	readonly turns: Map<string, TurnState<SessionID, Output, StartInput, StateData>>;
	/** Fallback execution can outlive turn retirement; only its physical settlement returns these units. */
	readonly actorRequests: Set<object>;
	settings: SpeculativeActionSettings;
	timeline?: TaskTimeline;
	sequence: number;
	decisionSequence: number;
	tokenTotal: number;
	candidateSequence: number;
	sourceRequestSequence: number;
	pendingPredictions: number;
	/** Retained handoffs borrow the live session, independently of prediction and candidate retirement. */
	readonly acceptOperationScope: (scope: ExecutionScope) => boolean;
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
	actorArrivedAt?: number;
	lifecycle: "active" | "closing" | "finished";
}

interface ClaimedPrediction { readonly node: PlanRuntimeNode; readonly opportunity: PredictionOpportunity; }

interface ProjectionResource { readonly dispose: () => void | Promise<void>; references: number; }

type ProjectionResult<Output> =
	| { readonly ok: true; readonly output: Output; readonly execution?: TimelineInterval; readonly reused?: true; readonly inputs?: boolean; readonly compatibility?: WorldBranch<Output>["compatibility"]; readonly validate?: WorldBranch<Output>["validate"]; readonly capturedBytes?: number; readonly requiresQueryValidation?: true; readonly resource?: ProjectionResource }
	| { readonly ok: false; readonly cause: ResolutionCause };

type RetainedResultView<Output> = Omit<Extract<ProjectionResult<Output>, { ok: true }>, "ok" | "execution" | "reused"> & {
	readonly bytes: number;
	readonly execution: TimelineInterval;
};

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
	type TurnClosure = { readonly state: Turn; readonly terminal: boolean; readonly notifyHost: boolean; };
	type SessionClosureMode = "terminal" | "disabled" | "disposed";
	type ActorSelectionInput = {
		readonly state: Turn;
		readonly consumeInput: ConsumeInput;
		readonly actualCall: ActualToolCall;
		readonly actualKey: ActionKey;
		readonly actorAction: ActorAction<Candidate, Output>;
		readonly pending: ReadonlySet<Promise<void>>;
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
	const candidateStore = new CandidateStore<SessionID, Candidate>(projectionRules, (_candidate, evidence) => resultCacheRecency(evidence), Date.now,
		(candidate) => candidate.origin === "prediction" ? candidate.owner.draft.source ?? "prediction" : "actor");
	const releaseProjectionResource = (sessionID: SessionID, resource?: ProjectionResource) => {
		if (resource && --resource.references === 0) sessionStates.get(sessionID)?.lifecycle.release(resource);
	};

	/** One mutation owns a view's storage, budget, lookup membership, and proof reference. */
	function setResultView(sessionID: SessionID, candidate: Candidate, key: string, view?: RetainedResultView<Output>): void {
		const previous = candidate.resultViews?.get(key);
		if (previous === view) return;
		if (view?.resource) view.resource.references++;
		if (view) (candidate.resultViews ??= new Map()).set(key, view);
		else candidate.resultViews?.delete(key);
		candidate.estimatedBytes += (view?.bytes ?? 0) - (previous?.bytes ?? 0);
		candidateStore.indexView(sessionID, candidate, key, view !== undefined);
		releaseProjectionResource(sessionID, previous?.resource);
	}

	/** Memoized queries share their sealed candidate's proof, retention budget, and lifetime. */
	function retainResultView(
		sessionID: SessionID, candidate: Candidate, action: ActionKey, projection: Extract<ProjectionResult<Output>, { ok: true }>,
		settings: SpeculativeActionSettings,
	): boolean {
		if (!projection.execution || candidate.resultViews?.has(action.key) || candidate.outputStale && !projection.validate) return false;
		try {
			const owned = cloneSharedData(projection.output), outputBytes = estimateValueBytes(owned) + action.key.length * 2 + 64;
			let bytes = outputBytes + finiteMetric(projection.capturedBytes), validate = projection.validate, resource = projection.resource;
			if (!projection.requiresQueryValidation && candidate.estimatedBytes + bytes > cacheByteLimit(settings)) { bytes = outputBytes; validate = undefined; resource = undefined; }
			while (candidate.resultViews?.size && (candidate.resultViews.size >= positiveCount(settings.resourceCacheMaxEntries) || candidate.estimatedBytes + bytes > cacheByteLimit(settings)))
				setResultView(sessionID, candidate, candidate.resultViews.keys().next().value!);
			if (candidate.estimatedBytes + bytes <= cacheByteLimit(settings)) {
				setResultView(sessionID, candidate, action.key, { output: owned, bytes, execution: projection.execution, inputs: projection.inputs, compatibility: projection.compatibility, validate, resource,
					capturedBytes: bytes - outputBytes, requiresQueryValidation: projection.requiresQueryValidation });
				return true;
			}
		} catch { /* Optional retention cannot alter an already committed result. */ }
		return false;
	}

	const sessionStates = new Map<SessionID, Session>();
	let masterEnabled: boolean | undefined;
	const masterDisabled = () => masterEnabled === false;
	type Preparation = {
		readonly session: Session; readonly controller: AbortController; readonly forecast: () => PredictionForecast;
		readonly run: () => Promise<void>; readonly fail: (failure: ResolutionCause) => void;
		readonly completion: Promise<void>; readonly complete: () => void;
		active: boolean;
	};
	const preparations = new Map<object, Preparation>();
	let pendingLaunch: Promise<void> | undefined;
	type ScheduledJob = Candidate | Preparation | ActorAction<Candidate, Output>;
	const needsResourceSamples = (): boolean => [...sessionStates.values()].some(session => !session.lifecycle.sealed &&
		(session.turns.size > 0 || candidateStore.pending(session.id).length > 0)) || preparations.size > 0 || scheduler.snapshot().length > 0;
	const scheduler: SpeculationScheduler<ScheduledJob> = new SpeculationScheduler({ resources: adapter.resources, active: needsResourceSamples,
		pollIntervalMs: () => {
			const active = [...sessionStates.values()].filter(session => session.turns.size || candidateStore.pending(session.id).length || scheduler.snapshot(session).length);
			return active.length ? Math.min(...active.map(session => normalizeSchedulingSettings(session.settings.scheduling).resourcePollIntervalMs)) : SCHEDULING_DEFAULTS.resourcePollIntervalMs;
		},
		changed: () => wakeResourceWaiters() });
	const scopeFor = (session: Session) => ({ owner: session, limit: concurrentLimit(session.settings), scheduling: session.settings.scheduling });
	const wakeResourceWaiters = (): void => {
		for (const session of sessionStates.values()) { preemptForActor(session); dispatchReady(session); }
		scheduler.watch();
	};

	const sessionFor = (sessionID: SessionID, settings: SpeculativeActionSettings): Session => {
		const current = sessionStates.get(sessionID);
		if (current) return current;
		const created: Session = {
			id: sessionID,
			lifecycle: new RuntimeLifecycleLane(),
			plan: new PlanRuntime(),
			effects: new PostSettlementQueue(),
			events: new BoundedEventQueue(RUNTIME_EVENT_QUEUE_CAPACITY, (event) => adapter.onEvent?.(event)),
			actionContexts: new Map(),
			sourceSlots: new Set(),
			sourceTasks: new Set(),
			turns: new Map(),
			settings,
			sequence: 0,
			decisionSequence: 0,
			tokenTotal: 0,
			candidateSequence: 0,
			sourceRequestSequence: 0,
			pendingPredictions: 0,
			acceptOperationScope: scope => !created.lifecycle.sealed && !masterDisabled() && scope.sessionID === created.id &&
				created.turns.get(scope.turnID)?.lifecycle === "active",
			actorRequests: new Set(),
		};
		sessionStates.set(sessionID, created);
		return created;
	};
	const turnContext = ({ startInput, data, settings }: RuntimeTurnContext<StartInput, StateData>) => ({ startInput, data, settings });
	const actorTurnActive = (state: Turn): boolean => state.lifecycle === "active" && state.session.turns.get(state.turnID) === state && !masterDisabled();

	const removeCandidate = (sessionID: SessionID, candidate: Candidate): void => {
		candidateStore.delete(sessionID, candidate);
		for (const key of candidate.resultViews?.keys() ?? []) setResultView(sessionID, candidate, key);
		sessionStates.get(sessionID)?.lifecycle.release(candidateBranch(candidate));
	};

	const acquireCandidate = (session: Session, candidate: Candidate, owner: string) => candidateStore.has(session.id, candidate) ? candidate.work.acquire(owner) : undefined;

	const borrowCandidateInputs = (session: Session, source: Candidate | undefined, owner: string) => {
		const leases = new Map<Candidate, NonNullable<ReturnType<typeof acquireCandidate>>>();
		let closed = false;
		return {
			lookup: function* (path: string): Iterable<object> {
				if (closed || session.lifecycle.sealed || masterDisabled()) return;
				for (const candidate of candidateStore.lookupInputs(session.id, [path])) {
					if (candidate === source || candidate.owner.draft.type !== "tool_call" || candidateWorld(candidate) || candidate.route.reuse !== "shared_result" ||
						candidate.work.execution.status !== "succeeded" || !candidate.work.execution.output.inputSource) continue;
					const lease = leases.get(candidate) ?? acquireCandidate(session, candidate, owner);
					if (lease) { leases.set(candidate, lease); yield candidate.work.execution.output.inputSource; }
				}
			},
			dispose: () => { closed = true; for (const lease of leases.values()) lease.release(); leases.clear(); },
		};
	};

	const createCandidate = (
		session: Session,
		context: RuntimeTurnContext<StartInput, StateData>,
		draft: SpeculativeDraftCandidate,
		input: Pick<Candidate, "origin" | "key" | "route" | "expectedDurationMs"> & Partial<Pick<Candidate,
			"worldParent" | "estimatedBytes" | "projectionCoverage">>,
	): Candidate => {
		const sequence = ++session.candidateSequence;
		let resultBytes = input.estimatedBytes ?? 0;
		const candidate: Candidate = {
			id: `${input.origin === "prediction" ? "spec" : input.origin === "actor_preview" ? "actor" : input.origin}_${sequence}_${input.key.hash.slice(0, 12)}`,
			work: new CandidateExecution<WorldBranch<Output>>(input.origin !== "actor_result" && input.route.reuse === "exclusive_branch" ? "exclusive" : "shared"),
			actorAdopted: input.origin === "actor_result",
			owner: { ...turnContext(context), draft, index: sequence - 1 },
			projectionCoverage: [],
			...input,
			get estimatedBytes() { return resultBytes + (candidateBranch(candidate)?.capturedBytes ?? 0); },
			set estimatedBytes(bytes) { resultBytes = bytes - (candidateBranch(candidate)?.capturedBytes ?? 0); },
		};
		return candidate;
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
					return existing.owner.draft.type === (input.kind ?? "tool_call") && !candidateBranch(existing)?.inputsOnly && !existing.outputStale &&
						!rejected?.has(existing) && activeExecution(existing) && candidateWorld(existing) === input.worldParent &&
						(sameSpeculativeExecutionRoute(existing.route, input.route) ||
							(existing.route.reuse === "shared_result" && input.route.reuse === "shared_result" && execution.status === "succeeded" &&
								scheduler.assessCompatibility(execution.output.compatibility, input.key.executionFingerprint).compatible)) &&
						(match.kind === "exact" || (existing.route.reuse === "shared_result" || execution.status !== "succeeded") &&
							actionKeyCovers(existing.key, input.key, projectionRules));
				});
			if (!inserted && candidate.work.execution.status === "succeeded") {
				// Joining producers share only the in-flight check; Actor adoption always validates afresh.
				const validation = await (candidate.admissionValidation ??= validateCandidate(candidate).finally(() => { candidate.admissionValidation = undefined; }));
				if (validation.status !== "valid" || candidate.outputStale || !candidateStore.has(session.id, candidate)) {
					(rejected ??= new Set()).add(candidate);
					if (validation.status === "stale") invalidateCandidates(session, [candidate], validation.cause, true);
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


	const startTurn = async (input: StartInput, signal?: AbortSignal): Promise<void> => {
		const settings = await adapter.settings();
		if (!settings.enabled || masterDisabled()) { await disableSession(input.sessionID); return; }
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
				lifecycle: "active",
			};
			session.turns.set(input.turnID, state);
			scheduler.watch(0);
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
			dispatchReady(session);
			setTimeout(() => { if (state.lifecycle === "active" && state.actorArrivedAt === undefined && !state.generation.signal.aborted) launchSourceRequests(state); }, 0);
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
		const slots = [...session.sourceSlots].filter((slot) => slot.request.source === source && slot.request.targetDecisionSequence === targetDecisionSequence &&
			(slot.request.kind === "observation") === (requestKind === "observation"));
		const observed = requestKind === "observation" && slots.find((slot) => slot.request.turnID === turnID);
		if (observed) { observed.pending++; return observed; }
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
		if (!slot.pending && slot.owners.size === 0) releaseSourceSlot(session, slot, cause("control", "source_slot_unused"));
	};

	const releaseSourceRequest = (session: Session, slot: SourceRequestSlot): void => { slot.pending--; releaseUnusedSourceSlot(session, slot); };

	const cancelCompetingProposals = (session: Session, winner: SourceRequestSlot): void => {
		for (const slot of [...session.sourceSlots]) {
			if (
				slot === winner || !session.sourceSlots.has(slot) || slot.request.kind !== "proposal" || slot.request.source !== winner.request.source ||
				slot.request.targetDecisionSequence !== winner.request.targetDecisionSequence
			)
				continue;
			releaseSourceSlot(session, slot, cause("source", "proposal_race_lost"));
		}
	};

	const expireSourceHorizon = (session: Session, decisionSequence: number, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) {
			if (slot.expiresAtTarget && slot.request.targetDecisionSequence <= decisionSequence) releaseSourceSlot(session, slot, failure);
		}
	};

	const releaseAllSourceSlots = (session: Session, failure: ResolutionCause): void => { for (const slot of [...session.sourceSlots]) releaseSourceSlot(session, slot, failure); };

	const trackSourceTask = <Value>(session: Session, task: Promise<Value>): Promise<Value> => {
		session.sourceTasks.add(task);
		void task.finally(() => session.sourceTasks.delete(task))
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
				const pending = requestSource({ ...turnContext(state), session: state.session, slot }, source, (signal, reportDraftTokens) =>
					source.propose({
						...turnContext(state),
						definitions: state.definitions,
						candidateNames: state.candidateNames,
						proposalIndex: index,
						proposalCount: count,
						signal, reportDraftTokens,
					}));
				trackSourceTask(state.session, pending);
			}
		}
	};

	/** Initial and continuation requests retain the same identity, production and admission owners. */
	const requestSource = (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		produce: (signal: AbortSignal, reportDraftTokens: (tokens: number) => void) => ReturnType<NonNullable<Source["continue"]>>,
	): Promise<void> => {
		const { session, slot } = scope;
		session.pendingPredictions++;
		// Tokens count when spent: empty, failed, aborted and never-admitted requests cost the same as productive ones.
		let draftTokens = 0;
		const reportDraftTokens = (tokens: number) => { const spent = finiteMetric(tokens); draftTokens += spent; session.tokenTotal += spent; };
		return runSourceRequest({
			request: slot.request,
			generation: slot.generation,
			timeoutMs: source.timeoutMs?.(scope.settings),
			produce: (signal) => trackSourceTask(session, Promise.resolve(produce(signal, reportDraftTokens))),
			count: (value) => asUpdates(value).length,
		}).then(async (settled) => {
			const request = { ...settled, draftTokens };
			session.pendingPredictions--;
			try {
				queueSourceRequestEvent(session, slot.request.turnID, scope.settings, request);
				if (source.onRequestSettled && session.sourceSlots.has(slot) && slot.generation.active)
					try { void Promise.resolve(source.onRequestSettled(request)).catch(() => {}); } catch { /* Feedback cannot discard a produced proposal. */ }
				if (request.settlement.status === "produced" && request.value !== undefined && session.sourceSlots.has(slot) && slot.generation.active) {
					await admitUpdates(scope, source, request.value);
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
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active) return;
		await Promise.allSettled(asUpdates(updates).map(async (update) => {
			const captured = PlanRuntime.capture(update, source.multiStepEnabled?.(scope.settings) !== false);
			if (!("update" in captured)) return;
			session.pendingPredictions++;
			try {
				// Capture the batch before binding callbacks can mutate producer input.
				await Promise.resolve();
				await applyUpdate(scope, source, captured.update);
			} finally {
				session.pendingPredictions--;
			}
		}));
	};

	const applyUpdate = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		update: PlanUpdate,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active) return;
		if (update.source !== source.id) return;
		const applied = session.plan.apply(update, session.decisionSequence);
		if (!applied.accepted) return;
		for (const retired of applied.retired) retirePlanAction(session, retired, cause("plan", "superseded"));
		const materializations: Promise<void>[] = [];
		for (const action of applied.upserted) {
			const node = session.plan.get(applied.plan.id, action.id);
			if (!node || node.predictionState.status !== "pending") continue;
			const context = session.actionContexts.get(node.identity.id);
			const issued = !context;
			if (issued) {
				const admissionController = new AbortController();
				const owner = session.turns.get(scope.startInput.turnID);
				scope.slot.owners.add(node.identity.id);
				session.actionContexts.set(node.identity.id, {
					identity: node.identity,
					opportunity: session.plan.opportunity(node.proposalID, node.action.id)!,
					...turnContext(scope),
					draft: planActionDraft(node),
					admissionController,
					sourceSlot: scope.slot,
					continuationTriggers: new Set(),
					continuationSlots: new Set(),
					continuationTail: Promise.resolve(),
				});
				const expired = () => {
					const current = session.plan.get(node.proposalID, node.action.id), failure = scope.slot.generation.expiration;
					// Binding transfers preparation to the plan; ending its source request cannot revoke that work.
					if (current?.identity.id === node.identity.id && current.actionKey &&
						(failure?.code === "actor_action_arrived" || owner?.generation.expiration?.code === "turn_finished")) return;
					admissionController.abort(failure);
				};
				scope.slot.generation.signal.addEventListener("abort", expired, { once: true, signal: admissionController.signal });
				owner?.signal?.addEventListener("abort", () => admissionController.abort(cause("control", "turn_aborted")),
					{ once: true, signal: admissionController.signal });
				if (scope.slot.generation.signal.aborted) expired();
			} else { context.draft = planActionDraft(node); }
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
		try { const { callID, index, ...request } = input; route = await adapter.resolveExecution(request); } catch { route = undefined; }
		if (input.signal.aborted) return { ok: false, cause: cause("source", "generation_expired") };
		if (!route) return { ok: false, cause: cause("execution", "isolation_unavailable", "No safe speculative execution route is available.") };
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
		if (context.admissionController.signal.aborted) { failUnlaunchable(session, node, cause("source", "generation_expired")); return; }
		if (!predictedAction) { failUnlaunchable(session, node, cause("matching", "action_not_keyable")); return; }
		const executionInput = asConcreteInput(predictedAction.input);
		if (!executionInput) { failUnlaunchable(session, node, cause("matching", "action_not_keyable")); return; }
		if (!session.plan.bindActionKey(node.identity, predictedAction)) return;
		// Binding owns schema validation and argument preparation; raw proposals cannot win the race.
		const slot = context.sourceSlot;
		if (session.sourceSlots.has(slot) && slot.request.kind === "proposal" && sourcesByID.get(node.source)?.concurrentProposalPolicy?.(context.settings) === "first_produced")
			cancelCompetingProposals(session, slot);
		const onCandidateMaterialized = adapter.onCandidateMaterialized;
		if (onCandidateMaterialized && node.action.type === "tool_call") {
			session.effects.enqueue(() =>
				onCandidateMaterialized({
					sessionID: session.id,
					turnID: context.startInput.turnID,
					expectedDecisionSequence: node.expectedDecisionSeq, latestDecisionSequence: node.latestDecisionSeq,
					source: node.source, proposalID: node.proposalID, actionID: node.action.id,
					tool: node.action.tool,
					input: structuredClone(concrete),
					predictedAction, executionAction: predictedAction,
					...definedFields(node.action, ["depth", "horizon", "conditionalProbability", "empiricalProbability", "adoptionProbability", "expectedDurationMs"]),
				}),
			);
		}
		await prepareWithinResources(context, { session, controller: context.admissionController,
			forecast: () => ({ ...forecastFor(session.plan.get(node.proposalID, node.action.id) ?? node, session.decisionSequence),
				resourceDemand: resourceDemand(session, predictedAction, undefined, node.action.resourceDemand) }),
			fail: failure => failUnlaunchable(session, node, failure), run: async () => {
				const admission = await executionRouteFor({ ...turnContext(context), candidate: context.draft,
					tool: predictedAction.tool, action: predictedAction, concrete: executionInput,
					callID: `spec_${session.candidateSequence + 1}`, index: session.candidateSequence, signal: context.admissionController.signal });
				if (admission.ok) context.executionRoute = admission.route;
				if (admission.ok || admission.cause.stage === "execution" && admission.cause.code === "isolation_unavailable")
					session.plan.finishPreparation(node.identity, admission.ok ? undefined : admission.cause);
				else failUnlaunchable(session, node, admission.cause);
			} });
	};

	const prepareWithinResources = (owner: object, input: Omit<Preparation, "active" | "completion" | "complete">): Promise<void> => {
		const signal = input.controller.signal;
		if (signal.aborted) return Promise.resolve();
		let resolve!: () => void;
		const entry: Preparation = { ...input, active: false, completion: new Promise<void>(done => { resolve = done; }),
			complete: () => { signal.removeEventListener("abort", aborted); scheduler.complete(entry); preparations.delete(owner); resolve(); wakeResourceWaiters(); } };
		const aborted = () => {
			entry.fail(signal.reason ?? cause("control", "preparation_aborted"));
			if (!entry.active) entry.complete(); // Cancellation does not return units still owned by physical preparation.
		};
		preparations.set(owner, entry); signal.addEventListener("abort", aborted, { once: true });
		startQueuedCandidates(input.session, input.forecast().actorDemand ? entry : undefined); scheduler.watch(0);
		return entry.completion;
	};

	const releaseActionContext = (session: Session, id: string, keepContinuation = false): void => {
		const context = session.actionContexts.get(id);
		if (!context) return;
		session.actionContexts.delete(id);
		context.admissionController.abort(cause("control", "prediction_retired"));
		context.sourceSlot.owners.delete(id);
		releaseUnusedSourceSlot(session, context.sourceSlot);
		if (!keepContinuation) for (const slot of context.continuationSlots) releaseSourceSlot(session, slot, cause("control", "parent_prediction_not_adopted"));
	};

	const failUnlaunchable = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		session.plan.rejectExecution(node.identity, failure);
		settleUnobserved(session, node, failure);
	};

	const settleBlockedPlanActions = (session: Session): void => {
		for (const node of session.plan.drainBlocked()) { failUnlaunchable(session, node, cause("plan", "dependency_impossible")); }
	};

	const pendingActorTurn = (session: Session): Turn | undefined =>
		[...session.turns.values()].find((turn) => turn.lifecycle === "active" && turn.actorArrivedAt === undefined && turn.decisionSequence === session.decisionSequence + 1);

	const dispatchReady = (session: Session): void => {
		if (session.lifecycle.sealed) return;
		settleBlockedPlanActions(session);
		for (const node of session.plan.launchable()) {
			if (!node.actionKey || !session.actionContexts.get(node.identity.id)?.executionRoute) continue;
			void launchNode(session, node);
		}
		startQueuedCandidates(session);
	};

	const launchNode = (session: Session, node: PlanRuntimeNode): Promise<void> => {
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return Promise.resolve();
		const promoted = session.plan.promote(node.proposalID, node.action.id);
		if (promoted.status !== "scheduled") return context.launch ?? Promise.resolve();
		node = promoted.node;
		return context.launch = session.lifecycle.track(Promise.resolve().then(async () => {
			const current = session.plan.get(node.proposalID, node.action.id);
			if (session.lifecycle.sealed || current?.identity.id !== node.identity.id || current.predictionState.status === "settled") return;
			if (!node.actionKey) { session.plan.defer(node.proposalID, node.action.id); return; }
			const route = context.executionRoute;
			if (!route) { failUnlaunchable(session, node, cause("plan", "execution_route_missing")); return; }
			const parent = dependencyWorld(session, node), checkpoint = parent && candidateBranch(parent)?.checkpoint;
			if (parent === null) { failUnlaunchable(session, node, cause("plan", "incompatible_parent_worlds")); return; }
			if (parent && (route.reuse === "shared_result" || !checkpoint ||
				(!sameSpeculativeExecutionRoute(parent.route, route) && route.acceptsCheckpoint?.(checkpoint) !== true))) {
				session.plan.defer(node.proposalID, node.action.id); return;
			}
			await admitCandidate(session, { key: node.actionKey, route, worldParent: parent, kind: node.action.type },
				() => createCandidate(session, context, context.draft, { origin: "prediction", key: node.actionKey!, route, worldParent: parent,
					expectedDurationMs: scheduler.evaluate([forecastFor(node, session.decisionSequence)]).expectedDurationMs }), () => {
					const current = session.plan.get(node.proposalID, node.action.id);
					return !session.lifecycle.sealed && current?.identity.id === node.identity.id &&
						current.predictionState.status !== "settled" && current.execution.status === "scheduled";
				}, (candidate, created) => {
					if (!created) attachNode(session, node, candidate);
					else { session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work); startQueuedCandidates(session); }
				});
		}).finally(() => { context.launch = undefined; }));
	};

	const attachNode = (session: Session, node: PlanRuntimeNode, candidate: Candidate): void => {
		if (!session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work)) return;
		const forecasts = forecastsForCandidate(session, candidate);
		const scheduled = scheduler.refresh(candidate, forecasts) ?? scheduler.evaluate(forecasts);
		candidate.expectedDurationMs = scheduled.expectedDurationMs;
		const execution = candidate.work.execution;
		if (execution.status === "succeeded") { queueContinuation(session, node, candidate, execution.output.output, "execution_succeeded"); return; }
		if (execution.status === "queued") startQueuedCandidates(session);
	};

	/** Coalesce producer requests behind the current Actor events; a concrete Actor intent stays immediate. */
	const startQueuedCandidates = (session: Session, preferred?: Candidate | Preparation): void => {
		if (session.lifecycle.sealed) return;
		if (preferred) return launchCandidateBatch(session, preferred);
		if (pendingLaunch || !candidateStore.pending(session.id).some((candidate) => candidate.work.execution.status === "queued") &&
			![...preparations.values()].some(entry => entry.session === session && !entry.active)) return;
		pendingLaunch = session.lifecycle.track(new Promise<void>(setImmediate).then(() => {
			pendingLaunch = undefined;
			if (!masterDisabled()) launchCandidateBatch();
		}));
	};

	const launchCandidateBatch = (owner?: Session, preferred?: Candidate | Preparation): void => {
		const queued: { job: Candidate | Preparation; session: Session; forecasts: readonly PredictionForecast[]; work: ScheduledWork }[] = [];
		for (const session of preferred ? [owner!] : sessionStates.values()) {
			if (session.lifecycle.sealed) continue;
			for (const { job } of scheduler.snapshot(session)) {
				if ("work" in job) scheduler.refresh(job, forecastsForCandidate(session, job));
				else if ("run" in job) scheduler.refresh(job, [job.forecast()]);
			}
			for (const candidate of candidateStore.pending(session.id)) {
				if (candidate.work.execution.status !== "queued" || preferred && candidate !== preferred) continue;
				const forecasts = forecastsForCandidate(session, candidate);
				if (!forecasts.length) { retireUndemandedCandidate(session, candidate, cause("retention", "prediction_horizon_settled")); continue; }
				queued.push({ job: candidate, session, forecasts, work: scheduler.evaluate(forecasts) });
			}
		}
		for (const entry of preparations.values()) {
			if (entry.active || entry.controller.signal.aborted || entry.session.lifecycle.sealed || preferred && entry !== preferred) continue;
			const forecasts = [entry.forecast()];
			queued.push({ job: entry, session: entry.session, forecasts, work: scheduler.evaluate(forecasts) });
		}
		queued.sort((a, b) => scheduler.compare(a.work, b.work));
		for (const { job, session, forecasts, work } of queued) {
			const preparing = "run" in job;
			if (preparing ? job.controller.signal.aborted : job.work.execution.status !== "queued") continue;
			if (work.actorDemand) preemptForActor(session, [job]);
			const admission = scheduler.admit(job, forecasts, scopeFor(session), preparing ? "preparation" : "execution", work,
				preparing || job.previews?.size ? undefined : actionExecutionIdentity(job.key));
			if (!admission.admitted) {
				if (admission.reason === "budget_exhausted") cancelScheduled(scheduler.preemptFor(scopeFor(session), work,
					other => "run" in other || "work" in other && reservationAvailable(other.work.reservation), drainingJob));
				continue;
			}
			if (preparing) {
				job.active = true;
				void session.lifecycle.track(Promise.resolve().then(job.run).catch(error => {
					job.fail(cause("admission", "preparation_failed", errorDetail(error)));
				}).finally(job.complete));
			} else {
				job.expectedDurationMs = work.expectedDurationMs;
				const startedAt = performance.now();
				if (!job.work.start(startedAt)) { scheduler.complete(job); continue; }
				queueCandidateEvent(session, job);
				void session.lifecycle.track(executeCandidate(session, job, startedAt));
			}
		}
		scheduler.watch();
	};

	const executeCandidate = async (session: Session, candidate: Candidate, startedAt: number): Promise<void> => {
		let branch: WorldBranch<Output> | undefined;
		const inputs = candidate.owner.draft.type === "tool_call" && candidate.route.reuse === "shared_result" && !candidateWorld(candidate)
			? borrowCandidateInputs(session, candidate, `inputs:prediction:${candidate.id}`) : undefined;
		const draft = candidate.owner.draft;
		try {
			const parent = candidateWorld(candidate), owner = candidate.owner.startInput;
			// A root fork outliving its turn runs in the live turn's scope: its own turn's snapshots and handoffs are closed.
			const live = parent || session.turns.get(owner.turnID)?.lifecycle === "active" ? undefined
				: [...session.turns.values()].find((turn) => turn.lifecycle === "active")?.startInput;
			if (draft.type === "operation" || candidate.origin === "prediction") candidate.onOperationAdopted = adoption => {
				const turn = session.turns.get(adoption.scope.turnID);
				if (session.lifecycle.sealed || session.id !== adoption.scope.sessionID || !turn ||
					draft.type === "operation" && draft.operation?.identity !== adoption.operationIdentity) return;
				if (candidate.origin === "prediction" && adoption.computation?.reused)
					TimelineInterval.producedBy(adoption.computation.computation, { source: draft.source ?? "cache", mode: draft.mode, feedback: draft.reuseFeedback });
				if (draft.type !== "operation") return;
				const actorAction: ActorActionIdentity = { id: adoption.id, kind: "operation", sequence: adoption.sequence,
					decisionSequence: turn.decisionSequence, turnID: turn.turnID };
				candidate.actorAdopted = true;
				for (const node of session.plan.consumers(candidate.id)) {
					const opportunity = session.plan.claimMatch(node.proposalID, node.action.id, actorAction, { kind: "exact", distance: 0 });
					const settled = opportunity && session.plan.confirm(opportunity, actorAction, { status: "adopted", candidateID: candidate.id });
					if (settled) predictionSettled(session, node, settled);
				}
			};
			const evaluation = await TimelineInterval.measure(() => adapter.executeCandidate({
				startInput: live ?? owner,
				data: candidate.owner.data,
				candidate: candidate.owner.draft,
				tool: candidate.key.tool,
				concrete: candidate.key.input as Record<string, unknown>,
				action: candidate.key,
				route: candidate.route,
				callID: candidate.id,
				index: candidate.owner.index,
				signal: candidate.work.controller.signal,
				inputs: inputs?.lookup,
				onOperationAdopted: candidate.onOperationAdopted,
				onOperationJoinable: available => { candidate.operationJoinable = available; },
				acceptOperationScope: session.acceptOperationScope,
				...(parent ? { parentWorld: candidateBranch(parent)! } : {}),
			}), branch => branch.computationDependencies ?? []);
			branch = evaluation.output;
			inputs?.dispose(); // The returned branch owns its proof before cache admission can evict sources.
			const output = branch.output;
			const rejected = adapter.rejectCandidateOutput?.({ output, candidate: publicCandidate(candidate) });
			if (rejected) throw new CandidateFailure(cause("execution", "output_rejected", rejected));
			candidate.projectionCoverage = captureCoverage(candidate.key, output, projectionRules);
			candidate.estimatedBytes = estimateValueBytes(output) + inputIndexBytes(branch);
			const completedAt = performance.now();
			const computation = evaluation.computation;
			if (!candidate.work.succeed(branch, computation, completedAt - startedAt)) {
				await session.lifecycle.release(branch);
				return;
			}
			if (candidate.origin === "prediction") TimelineInterval.producedBy(computation, { source: draft.source ?? "cache", mode: draft.mode, feedback: draft.reuseFeedback });
			scheduler.observe(actionExecutionIdentity(candidate.key), false);
			candidateStore.settle(session.id, candidate, candidate.work.reservation.kind === "shared", branch.reconstruct ? branch.inputResources : undefined);
			// An exclusive result waits for its own adoption; meanwhile the unchanged bytes it read can answer other reads.
			const budget = cacheLimits(candidate.owner.settings), preimages = candidate.work.reservation.kind === "exclusive" && budget.maxEntries > 0 && budget.maxBytes > 0
				? await branch.takeReadInputs?.(budget.maxBytes).catch(() => undefined) : undefined;
			if (preimages) await promoteAuthoritativeResult(session, candidate.owner, () => candidateStore.has(session.id, candidate), candidate.key, preimages.output, 0,
				computation, { route: { ...candidate.route, backend: preimages.backend, isolation: "resource_snapshot", reuse: "shared_result" },
					seal: () => preimages, dispose: preimages.dispose });
			for (const node of session.plan.consumers(candidate.id)) queueContinuation(session, node, candidate, output, "execution_succeeded");
			trimResults(session, candidate.owner.settings);
			queueCandidateEvent(session, candidate);
			retireUndemandedCandidate(session, candidate, cause("retention", "execution_settled"));
		} catch (error) {
			if (candidate.work.execution.status !== "succeeded") await session.lifecycle.release(branch);
			const failure =
				error instanceof CandidateFailure
					? error.failure
					: candidate.work.controller.signal.aborted ? cause("control", "execution_aborted") : cause("execution", "candidate_failed", errorDetail(error));
			const executionMs = Math.max(0, performance.now() - startedAt);
			const settled = candidate.work.controller.signal.aborted
				? candidate.work.cancel(failure, executionMs)
				: candidate.work.fail(failure, executionMs);
			if (settled && candidate.work.execution.status === "failed") {
				scheduler.observe(actionExecutionIdentity(candidate.key), true);
				if (candidate.owner.draft.type === "operation") for (const node of session.plan.consumers(candidate.id))
					settleUnobserved(session, node, failure);
			}
			removeCandidate(session.id, candidate);
			if (settled) queueCandidateEvent(session, candidate);
		} finally {
			const execution = candidate.work.execution;
			const source = candidate.origin === "prediction" ? sourcesByID.get(draft.source ?? "cache") : undefined;
			if (source?.onExecutionSettled && execution.status !== "queued" && execution.status !== "running") {
				const feedback = { reuseFeedback: draft.reuseFeedback, status: execution.status, executionMs: execution.executionMs };
				session.effects.enqueue(() => source.onExecutionSettled?.(feedback));
			}
			candidate.operationJoinable = undefined;
			candidate.operationConsumers = undefined;
			inputs?.dispose(); scheduler.complete(candidate); wakeResourceWaiters();
		}
	};

	const previewActorTool = async (
		input: { readonly sessionID: SessionID; readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	): Promise<void> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || signal?.aborted || !actorTurnActive(state)) return;
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
		try { return await adapter.actionKey(call.tool, call.input, { type: "consume", consumeInput: input }); } catch { return undefined; }
	};

	const previewActorCall = (input: ConsumeInput, signal?: AbortSignal): Promise<void> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || signal?.aborted || !actorTurnActive(state)) return Promise.resolve();
		const actualCall = adapter.actual(input);
		if (!actualCall.id) return state.session.lifecycle.track(promoteActorCall(state, input, actualCall, undefined, signal));
		const existing = state.actorPreviews.get(actualCall.id);
		if (existing) return existing.task;
		const record: ActorPreviewRecord = { controller: new AbortController(), task: Promise.resolve(), state: { status: "pending" } };
		const parent = signal ? AbortSignal.any([signal, state.generation.signal]) : state.generation.signal;
		parent.addEventListener("abort", () => record.controller.abort(parent.reason), { once: true, signal: record.controller.signal });
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
		const active = () =>
			!signal?.aborted && !record?.controller.signal.aborted && record?.state.status !== "cancelled" && actorTurnActive(state);
		const action = await actorActionKey(input, actualCall);
		if (!action || !active()) return;
		if (record) record.actionKey = action;
		await Promise.all(predictionMatches(state.session, action, state.decisionSequence).map(({ node }) => promoteForActor(state.session, node)));
		if (!active()) return;
		const preferred = rankCandidates(state.session, action)[0];
		if (preferred) {
			const { candidate, match } = preferred;
			if (record) attachActorPreview(record, candidate, "existing");
			if (candidate.work.execution.status === "queued") startQueuedCandidates(state.session, candidate);
			const execution = candidate.work.execution;
			if (!record || match.kind === "exact" || candidate.route.reuse !== "shared_result" || execution.status !== "succeeded" ||
				candidate.resultViews?.has(action.key) || candidate.estimatedBytes + action.key.length * 2 + 64 >= cacheByteLimit(state.settings)) return;
			if (execution.output.reconstructionScope !== "current_action" &&
				!scheduler.assessCompatibility(execution.output.compatibility, action.executionFingerprint).compatible) return;
			await new Promise<void>(setImmediate);
			if (!active() || state.actorPreviews.get(actualCall.id!) !== record) return;
			const lease = acquireCandidate(state.session, candidate, `preview:${callKey(state.turnID, actualCall.id!)}`);
			if (!lease) return;
			const inputs = execution.output.inputSource
				? borrowCandidateInputs(state.session, candidate, `inputs:preview:${callKey(state.turnID, actualCall.id!)}`) : undefined;
			let projected: ProjectionResult<Output> | undefined;
			try {
				if (await authorize(state, input, action, actualCall, candidate, signal) || !active()) return;
				// A streamed intent may prepare sealed data, but grants no freshness or commit authority.
				projected = await projectOutput(candidate, action, match,
					projectionRules, { action, args: action.input, callID: actualCall.id!, signal: signal ?? state.generation.signal, inputs: inputs?.lookup });
				if (projected.ok && active() && candidateStore.get(state.sessionID, candidate.id) === candidate) {
					const retained = retainResultView(state.sessionID, candidate, action, projected, state.settings);
					if (retained) trimResults(state.session, state.settings);
				}
			} finally {
				if (projected?.ok) releaseProjectionResource(state.sessionID, projected.resource);
				inputs?.dispose();
				lease.release();
			}
			return;
		}
		if (!record) return;
		await new Promise<void>(setImmediate);
		if (!active()) return;
		const concrete = asConcreteInput(action.input);
		if (!concrete) return;
		const draft: SpeculativeDraftCandidate = { type: "tool_call", tool: actualCall.tool, input: action.input, source: "actor_preview" };
		await prepareWithinResources(record, { session: state.session, controller: record.controller,
			forecast: () => ({ ...actionExecutionIdentity(action), decisionBatchesUntilCall: 0, actorHint: true,
				actorDemand: record.actorDemand, resourceDemand: resourceDemand(state.session, action) }),
			fail: () => { record.state = { status: "cancelled" }; }, run: async () => {
				const admission = await executionRouteFor({ ...turnContext(state), candidate: draft, tool: action.tool, action, concrete,
					callID: actualCall.id!, index: state.session.candidateSequence, signal: record.controller.signal });
				if (!admission.ok || !active()) return;
				const route = admission.route;
				await admitCandidate(state.session, { key: action, route }, () => createCandidate(state.session, state, draft, {
					origin: "actor_preview", key: action, route,
				}), active, (candidate, created) => {
					attachActorPreview(record, candidate, created ? "preview" : "existing");
					startQueuedCandidates(state.session, candidate);
				});
			} });
	};

	const abandonActorPreview = (state: Turn, record: ActorPreviewRecord | undefined, failure: ResolutionCause): void => {
		if (!record) return;
		const current = record.state;
		record.state = { status: "cancelled" };
		record.actorDemand = false; record.controller.abort(failure);
		if (current.status !== "candidate") return;
		const candidate = candidateStore.get(state.sessionID, current.candidateID);
		if (!candidate) return;
		candidate.previews?.delete(record);
		retireUndemandedCandidate(state.session, candidate, failure);
	};

	/** Results may outlive their consumers; work that has not started still needs an owner. */
	const retireUndemandedCandidate = (session: Session, candidate: Candidate, failure: ResolutionCause): void => {
		if (!reservationAvailable(candidate.work.reservation) || candidate.previews?.size || hasActorDemand(session, candidate) || session.plan.consumers(candidate.id).length) return;
		// Executed work belongs to the reuse store; prediction retirement cannot invalidate it.
		const state = candidate.work.execution;
		if (state.status === "queued" || candidate.owner.draft.type === "operation" && state.status !== "running") discardCandidate(session, candidate, failure, false);
	};

	const beginAuthoritativeResultCapture = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		action: ActionKey,
		signal?: AbortSignal,
	): Promise<AuthoritativeResultCapture<Output> | undefined> => {
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
			capture.route.reuse !== "shared_result" || captureSignal.aborted || !actorTurnActive(state) ||
			!state.actorActions.has(actorAction) ||
			!actorAction.capture(capture)
		) { state.session.lifecycle.release(capture); return; }
		return capture;
	};

	const selectActorCandidate = async (input: ActorSelectionInput): Promise<void> => {
		const { state, actualCall, actualKey, actorAction, pending, preview } = input;
		const signal = input.signal ? AbortSignal.any([input.signal, state.generation.signal]) : state.generation.signal;
		const rebuilt = new Set<Candidate>(), attempted = new Set<Candidate>();
		let retry: ReturnType<typeof rankCandidates>[number] | undefined;
		const rejectCompatibility = (choice: ReturnType<typeof rankCandidates>[number], compatibility: ReturnType<typeof scheduler.assessCompatibility> | undefined): boolean => {
			if (!compatibility || compatibility.compatible) return false;
			const failure = cause("compatibility", compatibility.code, compatibility.detail);
			actorAction.rejectCandidate(choice.candidate.id, choice.match, failure);
			if (choice.match.kind !== "inputs") discardCandidate(state.session, choice.candidate, failure);
			return true;
		};
		const stopCandidate = (candidate: Candidate): boolean => {
			const failure = signal?.aborted
				? cause("control", "actor_aborted")
				: !actorTurnActive(state) ? cause("control", "disabled") : undefined;
			if (!failure) return false;
			actorAction.setFallback(failure, candidate.id);
			return true;
		};

		for (;;) {
			const ranked = rankCandidates(state.session, actualKey, preview?.state.status === "candidate" ? preview.state.candidateID : undefined)
				.filter(({ candidate }) => !attempted.has(candidate));
			const choice = retry ?? ranked[0]; retry = undefined;
			if (!choice) {
				if (!pending.size) break;
				if ((await waitForCompletion(Promise.race(pending), signal)).status === "aborted") { actorAction.setFallback(cause("control", "actor_aborted")); break; }
				continue;
			}
			const candidate = choice.candidate;
			attempted.add(candidate);
			const reservation = acquireCandidate(state.session, candidate, `actor:${actorAction.identity.sequence}`);
			if (!reservation) { actorAction.rejectCandidate(candidate.id, choice.match, cause("matching", "candidate_reserved")); continue; }
			(candidate.actorConsumers ??= new Set()).add(actorAction);
			let inputs: ReturnType<typeof borrowCandidateInputs> | undefined;
			let projection: ProjectionResult<Output> | undefined;
			try {
				if (candidate.work.execution.status === "queued") {
					preemptForActor(state.session, ranked.map(({ candidate }) => candidate));
					startQueuedCandidates(state.session, candidate);
				}
				const authorization = await authorize(state, input.consumeInput, actualKey, actualCall, candidate, signal);
				if (stopCandidate(candidate)) break;
				if (authorization) { actorAction.rejectCandidate(candidate.id, choice.match, authorization); continue; }
				const waiting = await waitForCompletion(Promise.race([candidate.work.completion, ...[
					...pending, ...ranked.filter(other => other.candidate !== candidate && other.candidate.work.execution.status === "running")
						.map(other => other.candidate.work.completion),
				].map(task => task.then(() => undefined))]), signal);
				if (stopCandidate(candidate)) break;
				if (waiting.status === "aborted") { actorAction.setFallback(cause("control", "actor_aborted"), candidate.id); break; }
				const execution = waiting.value;
				if (!execution) { attempted.delete(candidate); continue; }
				if (execution.status !== "succeeded") { actorAction.rejectCandidate(candidate.id, choice.match, execution.cause); continue; }
				const branch = execution.output;
				const sourceCompatibility = branch.reconstructionScope === "current_action" && choice.match.kind !== "exact" ? undefined
					: scheduler.assessCompatibility(branch.compatibility, actualKey.executionFingerprint);
				if (rejectCompatibility(choice, sourceCompatibility)) continue;
				// Join only this exact Actor intent; changed arguments or executors cannot inherit its preparation.
				if (preview?.state.status === "candidate" && preview.state.candidateID === candidate.id &&
					preview.actionKey?.key === actualKey.key) await waitForCompletion(preview.task, signal);
				if (stopCandidate(candidate)) break;
				// Evaluate sealed data first, then prove freshness once immediately before commit.
				if (choice.match.kind !== "exact" && branch.inputSource && !candidate.resultViews?.has(actualKey.key))
					inputs = borrowCandidateInputs(state.session, candidate, `inputs:actor:${actorAction.identity.sequence}`);
				projection = await projectOutput(
					candidate,
					actualKey,
					choice.match,
					projectionRules,
					{ action: actualKey, args: actualCall.input, callID: actualCall.id ?? actualKey.hash, signal: signal ?? state.generation.signal, inputs: inputs?.lookup },
				);
				if (stopCandidate(candidate)) break;
				if (!projection.ok) { actorAction.rejectCandidate(candidate.id, choice.match, projection.cause); continue; }
				const compatibility = sourceCompatibility ?? scheduler.assessCompatibility(
					projection.compatibility ?? branch.compatibility, actualKey.executionFingerprint);
				if (rejectCompatibility(choice, compatibility)) continue;
				const validation = await validateCandidate(candidate, projection.validate);
				if (stopCandidate(candidate)) break;
				if (validation.status !== "valid") {
					if (validation.status === "stale") {
						setResultView(state.sessionID, candidate, actualKey.key);
						// A query may borrow another owner; its failure does not revoke this owner's other inputs.
						if (!projection.validate) invalidateCandidates(state.session, [candidate], validation.cause, true);
						if (validation.reconstruct && !rebuilt.has(candidate) && branch.reconstructionScope === "current_action" && branch.reconstruct &&
							candidate.route.reuse === "shared_result" && semantics.effect(actualKey) === "observation") {
							rebuilt.add(candidate); retry = { ...choice, match: { kind: "inputs", distance: choice.match.distance } }; continue;
						}
					}
					actorAction.rejectCandidate(candidate.id, choice.match, validation.cause);
					continue;
				}
				let output = projection.output;
				try {
					if (!projection.validate) { const committed = await branch.commit(); if (choice.match.kind === "exact") output = committed; }
				} catch (error) {
					const commitFailure = effectCommitFailure(error, "poisoned");
					if (isPoisonedEffectCommit(commitFailure)) throw commitFailure;
					const failure = commitFailure.resolutionCause ?? cause("commit", "world_commit_failed", errorDetail(commitFailure));
					actorAction.rejectCandidate(candidate.id, choice.match, failure);
					discardCandidate(state.session, candidate, failure);
					continue;
				}

				reservation.adopt();
				reconcileAuthoritativeEffects(state.session, actualKey, candidate);
				candidate.actorAdopted = true;
				if (reservation.kind === "exclusive") {
					const budget = cacheLimits(state.settings);
					if (branch.takeCommittedInputs && budget.maxEntries > 0 && budget.maxBytes > 0) {
						const inputs = await branch.takeCommittedInputs(budget.maxBytes).catch(() => undefined);
						if (inputs) await promoteAuthoritativeResult(state.session, state, () => state.lifecycle === "active", candidate.key, inputs.output, 0, execution.toolExecution, {
							route: { ...candidate.route, backend: inputs.backend, isolation: "resource_snapshot", reuse: "shared_result" },
							seal: () => inputs, dispose: inputs.dispose,
						});
					}
					removeCandidate(state.session.id, candidate);
				} else {
					if (preview) candidate.previews?.delete(preview);
					const retained = retainResultView(state.sessionID, candidate, actualKey, projection, state.settings);
					candidateStore.recordActorHit(state.sessionID, candidate, cacheLimits(state.settings));
					if (retained) trimResults(state.session, state.settings);
				}
				// Re-evaluating inputs adopts this query's computation, not the source tool's unused output work.
				const toolExecution = projection.inputs ? projection.execution! : execution.toolExecution;
				actorAction.select({
					candidate,
					match: projection.inputs ? { kind: "inputs", distance: choice.match.distance } : choice.match,
					output,
					toolExecution,
					...(projection.execution ? { projection: projection.execution } : {}),
					...(projection.reused ? { projectionReused: true } : {}),
				});
				break;
			} finally {
				if (projection?.ok) releaseProjectionResource(state.sessionID, projection.resource);
				inputs?.dispose();
				candidate.actorConsumers?.delete(actorAction);
				reservation.release();
				retireUndemandedCandidate(state.session, candidate, cause("retention", "actor_released"));
				scheduler.refresh(candidate, forecastsForCandidate(state.session, candidate));
			}
		}
	};

	const prepareActorCall = async (input: ConsumeInput, signal?: AbortSignal): Promise<PreparedActorCall<Output> | undefined> => {
		const actorArrivedAt = performance.now();
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || signal?.aborted || !actorTurnActive(state)) return undefined;
		const actualCall = adapter.actual(input);
		let preview = actualCall.id ? state.actorPreviews.get(actualCall.id) : undefined;
		if (actualCall.id) state.actorPreviews.delete(actualCall.id);
		expireSourceHorizon(state.session, state.decisionSequence, cause("control", "actor_action_arrived"));
		if (state.actorArrivedAt === undefined) {
			state.actorArrivedAt = actorArrivedAt;
			state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
			scheduler.advance();
		}
		const sequence = ++state.session.sequence;
		const actualKey = await actorActionKey(input, actualCall);
		if (state.lifecycle !== "active" || state.session.turns.get(state.turnID) !== state || signal?.aborted || masterDisabled()) {
			abandonActorPreview(state, preview, cause("control", signal?.aborted ? "actor_aborted" : "disabled"));
			return undefined;
		}
		const actorAction = new ActorAction<Candidate, Output>({
			identity: { id: actualCall.id ?? JSON.stringify([input.turnID, sequence]), sequence, decisionSequence: state.decisionSequence, turnID: input.turnID },
			tool: actualCall.tool,
			...(actualKey ? { actionKey: actualKey } : {}),
			fallback: cause("matching", "no_candidate"),
		});
		const identity = actorAction.identity;
		state.actorActions.add(actorAction);
		state.actorObservation ??= actualKey ? identity : null;
		let operationLearning: boolean | undefined;
		let captureInputSource: object | undefined;
		const prepared: PreparedActorCall<Output> & { output?: Output } = {
			get observeOperations() { return operationLearning === true; },
			withInputs: async execute => {
				const inputs = borrowCandidateInputs(state.session, undefined, `inputs:actor:${identity.id}`);
				try { return await execute(function* (target) { yield* inputs.lookup(target); if (captureInputSource) yield captureInputSource; }); } finally { inputs.dispose(); }
			},
			settle: (toolExecution, output, operations) => state.session.lifecycle.track(
				settleActorCall(state, input, actualCall, actorAction, output, toolExecution, operations && Object.freeze([...operations]))),
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

		let matchingPredictions: ClaimedPrediction[] = [];
		const pending = new Set<Promise<void>>(), actorSignal = signal ? AbortSignal.any([signal, state.generation.signal]) : state.generation.signal;
		const follow = (task: Promise<void>) => {
			const tracked = task.catch(() => {}).finally(() => { pending.delete(tracked); }); pending.add(tracked);
		};
		try {
			if (!actualKey) {
				const failure = cause("matching", "action_not_keyable");
				abandonActorPreview(state, preview, failure);
				actorAction.deferToFallback([], failure);
				preemptForActor(state.session);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				return Object.freeze(prepared);
			}

			matchingPredictions = predictionMatches(state.session, actualKey, state.decisionSequence).flatMap(({ node, relation }) => {
				const opportunity = state.session.plan.claimMatch(node.proposalID, node.action.id, identity, relation);
				if (!opportunity) return [];
				return [{ node, opportunity }];
			});
			if (preview?.actionKey && actionKeyCovers(preview.actionKey, actualKey, projectionRules)) {
				preview.actorDemand = true;
				const preparation = preparations.get(preview);
				if (preparation) startQueuedCandidates(state.session, preparation);
				if (preview.state.status === "pending") follow(preview.task);
			} else if (preview?.state.status === "pending") {
				abandonActorPreview(state, preview, cause("matching", "preview_intent_changed")); preview = undefined;
			}
			for (const { node } of matchingPredictions) follow(promoteForActor(state.session, node, actorSignal));
			await selectActorCandidate({
				state,
				consumeInput: input,
				actualCall,
				actualKey,
				actorAction,
				pending,
				...(preview ? { preview } : {}),
				...(signal ? { signal } : {}),
			});
			const selected = actorAction.selection;
			if (selected) {
				const predictionIdentities = matchingPredictions.map(({ opportunity }) => opportunity.identity);
				const previewed = preview?.state.status === "candidate" && preview.state.ownership === "preview" && preview.state.candidateID === selected.candidate.id;
				const adoption = actorAction.settleSelection(predictionIdentities, previewed ? "preview" : "speculative");
				if (!adoption) return Object.freeze(prepared);
				state.actorActions.delete(actorAction);
				if (!previewed) for (const { node } of matchingPredictions) queueContinuation(
					state.session,
					node,
					selected.candidate,
					selected.output,
					"actor_adopted",
					{ key: actualKey, input: asConcreteInput(actualCall.input) ?? actualKey.input },
				);
				confirmPredictions(state.session, matchingPredictions, identity, adoption);
				queueActorSettlement(state, input, actualCall, actorAction, selected.output, selected);
				state.session.effects.enqueue(() => dispatchReady(state.session));
				prepared.output = selected.output;
				return Object.freeze(prepared);
			}

			if (actorAction.fallback.cause.code === "no_candidate") for (const { node } of matchingPredictions) {
				const execution = state.session.plan.get(node.proposalID, node.action.id)?.execution;
				if (execution && "cause" in execution) { actorAction.setFallback(execution.cause); break; }
			}
			abandonActorPreview(state, preview, actorAction.fallback.cause);
			const adoption = actorAction.deferToFallback(matchingPredictions.map(({ opportunity }) => opportunity.identity));
			if (adoption) confirmPredictions(state.session, matchingPredictions, identity, adoption);
			const effect = semantics.effect(actualKey);
			operationLearning = state.settings.enabled && sources.some(source => source.observe && source.enabled(state.settings) && source.observesOperations?.(actualKey))
				? true : undefined;
			preemptForActor(state.session);
			state.session.effects.enqueue(() => dispatchReady(state.session));
			if (adapter.captureAuthoritativeResult && (effect === "observation" || effect === "workspace_mutation" || prepared.observeOperations)) {
				captureInputSource = (await beginAuthoritativeResultCapture(state, input, actualCall, actorAction, actualKey, signal))?.inputSource;
			}
			return Object.freeze(prepared);
		} catch (error) {
			if (isPoisonedEffectCommit(error)) throw error;
			return Object.freeze(prepared); // Speculation failed after matching: the native run still settles and is recorded.
		} finally {
			abandonActorPreview(state, preview, actorAction.fallback.cause);
			// Claims still open after a throw settle as matched but not served, instead of staying "matching" forever.
			const adoption = actorAction.deferToFallback(matchingPredictions.map(({ opportunity }) => opportunity.identity));
			if (adoption) confirmPredictions(state.session, matchingPredictions, identity, adoption);
			if (actorAction.state.status === "awaiting_fallback" && !scheduler.has(actorAction)) preemptForActor(state.session);
		}
	};

	const promoteAuthoritativeResult = async (
		session: Session,
		context: RuntimeTurnContext<StartInput, StateData>,
		active: () => boolean,
		action: ActionKey,
		output: Output,
		durationMs: number,
		toolExecution: TimelineInterval,
		capture: AuthoritativeResultCapture<Output>,
	): Promise<void> => {
		let branch: WorldBranch<Output> | undefined;
		try { branch = await capture.seal(output); } catch { session.lifecycle.release(capture); return; }
		let retained = false;
		try {
			if (!active() || session.lifecycle.sealed || masterDisabled()) return;
			const candidate = createCandidate(session, context,
				{ type: "tool_call", tool: action.tool, input: action.input, source: "actor_result" }, {
				origin: "actor_result",
				key: action,
				route: capture.route,
					estimatedBytes: estimateValueBytes(output) + inputIndexBytes(branch),
				projectionCoverage: captureCoverage(action, output, projectionRules),
			});
			candidate.work.start(toolExecution.startedAt);
			if (!candidate.work.succeed(branch, toolExecution, durationMs)) return;
			const rejected = adapter.rejectCandidateOutput?.({ output, candidate: publicCandidate(candidate) });
			if (rejected) return;
			candidateStore.settle(session.id, candidate, true, branch.reconstruct ? branch.inputResources : undefined);
			retained = true;
			trimResults(session, context.settings);
		} catch {
			// Optional cache promotion cannot alter an already completed Actor result.
		} finally {
			if (!retained) session.lifecycle.release(branch);
		}
	};

	const settleActorCall = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		output: Output | undefined,
		toolExecution: TimelineInterval,
		operations?: readonly ExecutionOperationBinding[],
	): Promise<void> => {
		if (state.session.actorRequests.delete(actorAction)) { scheduler.complete(actorAction); state.session.effects.enqueue(wakeResourceWaiters); }
		for (const candidate of candidateStore.values(state.sessionID)) if (candidate.operationConsumers?.delete(actorAction))
			retireUndemandedCandidate(state.session, candidate, cause("retention", "prediction_horizon_settled"));
		if (!state.actorActions.delete(actorAction)) return;
		const capture = actorAction.takeCapture();
		const settlement = actorAction.settleActor(toolExecution, outputIsError(output));
		if (!settlement) { state.session.lifecycle.release(capture); return; }
		const execution = settlement.provider.toolExecution, durationMs = execution.completedAt - execution.startedAt;
		const key = actorAction.actionKey;
		if (key) reconcileAuthoritativeEffects(state.session, key);
		// Authoritative feedback must enter the settlement queue before optional cache work can yield.
		queueActorSettlement(state, input, actualCall, actorAction, output, undefined, operations);
		if (capture && key && output !== undefined && !outputIsError(output)) {
			await promoteAuthoritativeResult(state.session, state, () => state.lifecycle === "active", key, output, durationMs, execution, capture);
		} else if (capture) state.session.lifecycle.release(capture);
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
		let reusedComputations: readonly ComputationReuseShare[] | undefined;
		const computation = state.session.timeline?.recordCall([
			{ computation: settlement.provider.toolExecution, reused: !!selection && (selection.match.kind !== "inputs" || !!selection.projectionReused) },
			...(selection?.projection ? [{ computation: selection.projection, reused: selection.projectionReused }] : []),
		], shares => { reusedComputations = shares; });
		const key = actorAction.actionKey;
		const settledCandidate = selection?.candidate;
		const settledCandidateDescriptor = settledCandidate && (adapter.onActorActionSettled || adapter.onEvent)
			? Object.freeze(candidateEventDescriptor(settledCandidate))
			: undefined;
		const event: SpeculativeActionEvent<SessionID> | undefined = adapter.onEvent ? {
			type: "actor_action",
			...eventEnvelope(state.session, state.turnID, state.settings),
			settlement,
			...(computation ? { computation } : {}),
			actualAction: diagnosticAction(actorAction.tool, actualCall.input, key),
			...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
		} : undefined;
		if (adapter.onActorActionSettled) state.session.effects.enqueue(() => adapter.onActorActionSettled?.({
			sessionID: state.sessionID,
			turnID: state.turnID,
			...(key ? { action: key } : {}),
			settlement,
			...(settledCandidateDescriptor ? { candidate: settledCandidateDescriptor } : {}),
			candidateFeedback: settledCandidate?.owner.draft.feedback,
			computation, reusedComputations,
		}));
		if (event) state.session.effects.enqueue(() => { state.session.events.enqueue(event); });
		for (const source of sources) {
			try {
				if (!source.observe) continue;
				const observation = cloneSharedData({ concrete: asConcreteInput(actualCall.input) ?? {}, ...(output !== undefined ? { output } : {}) });
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
							settlement.provider.kind === "actor" ? settlement.provider.durationMs : executionDuration(settledCandidate),
						order: settlement.actorAction.sequence,
						signal: state.generation.signal,
						reserveRevision: (proposalID, minimum) => state.session.plan.reserveRevision(proposalID, minimum) ?? minimum,
					});
					const target = state.decisionSequence + 1;
					if (state.lifecycle !== "active" || !state.generation.active || target <= state.session.decisionSequence || !asUpdates(updates).length) return;
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
			if (node.action.type === "operation") { settleUnobserved(state.session, node, cause("matching", "operation_not_observed")); continue; }
			if (!observation) { settleUnobserved(state.session, node, cause("matching", "actor_action_not_keyable")); continue; }
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
			tool: node.action.tool, predictedAction: diagnosticAction(node.action.tool, node.action.input, node.actionKey),
			...(node.action.mode ? { mode: node.action.mode } : {}),
			settlement,
		} : undefined;
		if (adapter.onPredictionSettled) session.effects.enqueue(() => adapter.onPredictionSettled?.({
			sessionID: session.id,
			turnID: context.startInput.turnID,
			tool: node.action.tool,
			...(node.actionKey ? { action: node.actionKey } : {}),
			settlement,
		}));
		if (event || source?.onSettled) session.effects.enqueue(async () => {
			if (event) session.events.enqueue(event);
			await source?.onSettled?.({ proposalID: context.identity.proposalID, actionID: context.identity.actionID, feedback: context.draft.feedback, settlement });
		});
		const adopted = settlement.observation === "observed" && settlement.match.matched && settlement.match.adoption.status === "adopted";
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
		if (source.multiStepEnabled?.(context.settings, context.draft.feedback) === false) return;
		if (context.continuationTriggers.has(trigger)) return;
		context.continuationTriggers.add(trigger);
		try {
			const filter = source.continueOn;
			if (filter && !(typeof filter === "function" ? filter({ actionID: node.action.id, feedback: context.draft.feedback, output, trigger }) : filter.includes(trigger))) return;
		} catch { return; } // Producer feedback cannot alter completed execution.
		const parentDecisionSequence =
			current.predictionState.status === "matching" ? (current.predictionState.actorAction.decisionSequence ?? current.expectedDecisionSeq) : current.expectedDecisionSeq;
		const targetDecisionSequence = parentDecisionSequence + 1;
		const pending = context.continuationTail
			.then(() => requestContinuation(session, source, [context], targetDecisionSequence, (signal, reportDraftTokens) => {
				const revision = session.plan.reserveRevision(node.proposalID);
				if (revision === undefined) return undefined;
				return source.continue!({
					...turnContext(context),
					candidate: predictionCandidate(candidate, node), ...(adoptedAction ? { adoptedAction } : {}),
					proposalID: node.proposalID, actionID: node.action.id, revision,
					feedback: context.draft.feedback, output, trigger, signal, reportDraftTokens,
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
		produce: Parameters<typeof requestSource>[2],
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
		const peers = sources.filter((peer) => peer.id !== source.id && peer.continueFrom && peer.enabled(context.settings) && peer.multiStepEnabled?.(context.settings) !== false);
		if (!peers.length) return;
		const ids = source.continuationBatch!({ proposalID: node.proposalID, actionID: node.action.id, feedback: context.draft.feedback });
		if (!ids?.length || !ids.includes(node.action.id) || new Set(ids).size !== ids.length) return;
		const batch: Parameters<NonNullable<Source["continueFrom"]>>[0]["batch"][number][] = [];
		const parents: PlanActionContext<StartInput, StateData>[] = [];
		for (const id of ids) {
			const parent = session.plan.get(node.proposalID, id);
			const owner = parent && session.actionContexts.get(parent.identity.id);
			if (!parent || parent.action.type !== "tool_call" || !owner || owner.admissionController.signal.aborted || parent.predictionState.status !== "pending" ||
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
			const pending = requestContinuation(session, peer, parents, node.expectedDecisionSeq + 1, async (requestSignal, reportDraftTokens) => {
				const signal = AbortSignal.any([requestSignal, ...parents.map((parent) => parent.admissionController.signal)]);
				if (signal.aborted) return undefined;
				const update = await peer.continueFrom!({ ...turnContext(context), batch, signal, reportDraftTokens });
				return signal.aborted ? undefined : update;
			}).catch(() => { /* Peer prediction cannot revoke the completed parent batch. */ });
			trackSourceTask(session, pending);
		}
	};

	const retirePlanAction = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const opportunity = session.actionContexts.get(node.identity.id)?.opportunity;
		if (opportunity?.state.status === "matching") return;
		const finalized = opportunity?.unobserve(failure);
		if (finalized) predictionSettled(session, node, finalized);
		else releaseActionContext(session, node.identity.id);
	};

	const descendsFrom = (candidate: Candidate, ancestor: Candidate): boolean => {
		for (let parent = candidate.worldParent; parent; parent = parent.worldParent) { if (parent === ancestor) return true; }
		return false;
	};

	const unresolvedWorld = (candidate: Candidate): Candidate | undefined => {
		for (let current: Candidate | undefined = candidate; current; current = current.worldParent) {
			if (candidateBranch(current)?.checkpoint && !current.actorAdopted) return current;
		}
		return undefined;
	};

	const candidateWorld = (candidate: Candidate): Candidate | undefined => candidate.worldParent ? unresolvedWorld(candidate.worldParent) : undefined;

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
		return ([...parents].find((candidate) => [...parents].every((parent) => parent === candidate || descendsFrom(candidate, parent))) ?? null);
	};

	const hasActorDemand = (session: Session, candidate: Candidate): boolean =>
		!!candidate.actorConsumers?.size || [...candidate.previews ?? []].some(preview => preview.actorDemand) ||
		[...candidate.operationConsumers ?? []].some(actor => session.actorRequests.has(actor));
	const forecastsForCandidate = (session: Session, candidate: Candidate): readonly PredictionForecast[] => {
		const nodes = session.plan.consumers(candidate.id), execution = candidate.work.execution;
		const current = { elapsedMs: execution.status === "running" ? Math.max(0, performance.now() - execution.startedAt) : 0,
			actorDemand: hasActorDemand(session, candidate),
			actorHint: !!candidate.previews?.size || pendingActorTurn(session)?.actorToolHints.has(candidate.key.tool) === true };
		if (nodes.length) return nodes.map(node => ({ ...forecastFor(node, session.decisionSequence), ...current,
			resourceDemand: resourceDemand(session, candidate.key, candidate.route, node.action.resourceDemand) }));
		if (!current.actorDemand && !candidate.previews?.size) return [];
		return [{ ...actionExecutionIdentity(candidate.key), ...current, expectedDurationMs: candidate.expectedDurationMs,
			resourceDemand: resourceDemand(session, candidate.key, candidate.route), decisionBatchesUntilCall: 0 }];
	};

	const validateCandidate = (candidate: Candidate, validate?: WorldBranch<Output>["validate"]): Promise<ResourceValidation> =>
		validateWorldBranch(validate ? { validate } : candidateBranch(candidate), candidate.route.reuse);

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

	const rankCandidates = (session: Session, action: ActionKey, preferred?: string) => {
		const now = performance.now();
		const choices = candidateStore.lookup(session.id, action, (candidate) => candidate.work.execution.status !== "succeeded", semantics.effect(action) === "observation")
			.flatMap(({ entry: candidate, match }) => {
				const execution = candidate.work.execution;
				if (candidate.owner.draft.type !== "tool_call" || !activeExecution(candidate) || candidateWorld(candidate) !== undefined) return [];
				if (candidateBranch(candidate)?.inputsOnly || candidate.outputStale) {
					if (semantics.effect(action) !== "observation") return [];
					match = { kind: "inputs", distance: match.distance };
				}
				const remainingMs =
					execution.status === "running"
						? candidate.expectedDurationMs === undefined ? Infinity : Math.max(0, candidate.expectedDurationMs - (now - execution.startedAt))
						: execution.status === "queued" ? candidate.expectedDurationMs ?? Infinity : 0;
				return [{ candidate, match, ready: execution.status === "succeeded", remainingMs }];
			});
		if (choices.length < 2) return choices;
		const inputConsumers = new Set(choices.flatMap(({ candidate, match }) => {
			const reservation = candidate.work.reservation;
			return match.kind === "inputs" && reservation.kind === "shared" ? reservation.owners : [];
		}));
		const priority = ({ candidate, match, ready }: typeof choices[number]) =>
			ready && (match.kind !== "inputs" || candidate.resultViews?.has(action.key)) ? 0 :
				match.kind === "exact" && candidate.work.execution.status === "running" &&
				inputConsumers.has(`inputs:prediction:${candidate.id}`) ? 1 : 2;
		// A ready input owner is not a ready query. Join its exact active consumer before rebuilding it.
		return choices.sort(
				(left, right) =>
					priority(left) - priority(right) || Number(right.candidate.id === preferred) - Number(left.candidate.id === preferred) ||
					Number(right.ready) - Number(left.ready) ||
					left.remainingMs - right.remainingMs ||
					left.match.distance - right.match.distance ||
					right.candidate.owner.index - left.candidate.owner.index,
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

	const promoteForActor = async (session: Session, node: PlanRuntimeNode, signal?: AbortSignal): Promise<void> => {
		const context = session.actionContexts.get(node.identity.id), preparation = context && preparations.get(context);
		if (preparation && context.opportunity.state.status === "matching") {
			startQueuedCandidates(session, preparation);
			if ((await waitForCompletion(preparation.completion, signal)).status === "aborted") return;
		}
		await launchNode(session, node);
	};

	const resourceDemand = (session: Session, action: ActionKey | string, route?: SpeculativeExecutionRoute,
		declared?: number | HardwareResources): HardwareResources => {
		const definition = semantics.definition(action);
		const heavy = (route && route.isolation !== "resource_snapshot") || !definition || definition.effect === "unbounded" ||
			definition.resourceScope === "tree_entries" || definition.resourceScope === "tree_content" || definition.resourceScope === "captured_inputs";
		const policy = normalizeSchedulingSettings(session.settings.scheduling);
		return { cpu: Math.min(concurrentLimit(session.settings), adapter.resources?.initial.cpuCount ?? Infinity, heavy ? policy.heavyCpu : policy.lightCpu),
			memory: heavy ? policy.heavyMemoryBytes : policy.lightMemoryBytes, io: heavy ? policy.heavyIo : policy.lightIo,
			...(typeof declared === "number" ? { cpu: declared } : declared) };
	};

	const cancelScheduled = (jobs: readonly ScheduledJob[], actor = false): void => {
		for (const job of jobs) {
			if ("run" in job) {
				const failure = cause("admission", actor ? "preempted_by_actor" : "scheduler_preempted");
				job.controller.abort(failure);
			} else if ("work" in job) {
				const owner = scheduler.snapshot().find(entry => entry.job === job)?.scope.owner as Session | undefined;
				if (owner) { discardCandidate(owner, job, cause("admission", "scheduler_preempted"), false); owner.plan.rearmExecution(job.id); }
			}
		}
	};

	const preemptForActor = (session: Session, protectedCandidates: readonly (Candidate | Preparation)[] = []): void => {
		for (const turn of session.turns.values()) for (const action of turn.actorActions) {
			if (action.state.status !== "awaiting_fallback") continue;
			session.actorRequests.add(action);
			const forecast = [{ tool: action.tool, actorDemand: true, resourceDemand: resourceDemand(session, action.actionKey ?? action.tool) }];
			if (scheduler.has(action)) scheduler.refresh(action, forecast);
			else scheduler.admit(action, forecast, scopeFor(session), "actor");
		}
		for (const candidate of candidateStore.values(session.id)) {
			for (const consumer of candidate.operationConsumers ?? []) if (!session.actorRequests.has(consumer)) candidate.operationConsumers!.delete(consumer);
			try {
				if (candidate.operationJoinable?.()) for (const turn of session.turns.values()) for (const action of turn.actorActions)
					if (action.state.status === "awaiting_fallback" && action.tool === candidate.key.tool) (candidate.operationConsumers ??= new Set()).add(action);
			} catch { /* A failed acquisition hint reserves no hardware. */ }
		}
		const preferred = protectedCandidates.find(candidate => !scheduler.has(candidate));
		const incoming = preferred && scheduler.evaluate(("run" in preferred ? [preferred.forecast()] : forecastsForCandidate(session, preferred))
			.map(f => ({ ...f, actorDemand: true })));
		cancelScheduled(scheduler.preemptFor(scopeFor(session), incoming, job => {
			if ("run" in job) return job.active && !job.controller.signal.aborted && !job.forecast().actorDemand && !protectedCandidates.includes(job);
			if (!("work" in job) || job.work.execution.status !== "running" || protectedCandidates.includes(job) || !reservationAvailable(job.work.reservation)) return false;
			const owner = scheduler.snapshot().find(entry => entry.job === job)?.scope.owner as Session;
			return !hasActorDemand(owner, job);
		}, drainingJob), true);
	};

	const drainingJob = (job: ScheduledJob): boolean => "run" in job ? job.controller.signal.aborted : "work" in job && job.work.execution.status !== "running";

	const discardCandidate = (session: Session, candidate: Candidate, failure: ResolutionCause, dispatch = true): void => {
		const state = candidate.work.execution;
		if (state.status !== "queued" && state.status !== "running") { removeCandidate(session.id, candidate); return; }
		const executionMs = state.status === "running" ? Math.max(0, performance.now() - state.startedAt) : 0;
		const settled = candidate.work.cancel(failure, executionMs);
		removeCandidate(session.id, candidate);
		if (settled) queueCandidateEvent(session, candidate);
		if (dispatch) dispatchReady(session);
	};

	const invalidateCandidates = (session: Session, candidates: Iterable<Candidate>, failure: ResolutionCause, retainInputs = false, dispatch = true): void => {
		let invalidated = false;
		for (const candidate of new Set(candidates)) {
			if (candidateStore.get(session.id, candidate.id) !== candidate) continue;
			const branch = candidateBranch(candidate);
			if (retainInputs && candidate.work.reservation.kind === "shared" && !branch?.checkpoint &&
				branch?.reconstructionScope === "current_action" && branch.inputSource && branch.reconstruct) {
				candidate.outputStale = true;
				// Independent query proofs survive; views backed only by the old output proof do not.
				for (const [key, view] of candidate.resultViews ?? []) if (!view.validate) setResultView(session.id, candidate, key);
			} else discardCandidate(session, candidate, failure, false);
			session.plan.rearmExecution(candidate.id);
			invalidated = true;
		}
		if (invalidated && dispatch) dispatchReady(session);
	};

	const reconcileStores = async (state: Turn): Promise<void> => {
		const available = new Set(state.definitions.map((definition) => definition.name));
		for (const candidate of candidateStore.values(state.sessionID)) {
			if (!available.has(candidate.key.tool) || (candidate.work.execution.status === "queued" && !state.candidateNames.includes(candidate.key.tool)))
				discardCandidate(state.session, candidate, cause("control", "tool_disabled"));
		}
		trimResults(state.session, state.settings);
	};

	const trimResults = (session: Session, settings: SpeculativeActionSettings): void => {
		const parents = new Set(session.plan.unsettled().flatMap(node => (node.action.dependsOn ?? []).flatMap(dependency => {
			const execution = session.plan.dependency(node.proposalID, dependency)?.execution;
			return execution && "candidateID" in execution ? [execution.candidateID] : [];
		})));
		for (const candidate of candidateStore.values(session.id))
			for (let parent = candidate.worldParent; parent; parent = parent.worldParent) if (!parent.actorAdopted) parents.add(parent.id);
		for (const candidate of candidateStore.trim(session.id, cacheLimits(settings), entry => !entry.previews?.size &&
			reservationAvailable(entry.work.reservation) && !parents.has(entry.id))) {
			removeCandidate(session.id, candidate);
		}
	};

	const reconcileAuthoritativeEffects = (session: Session, action: ActionKey, adopted?: Candidate): void => {
		const effect = semantics.effect(action);
		if (effect === "observation" || adopted?.work.reservation.kind === "shared") return;
		const changed = (adopted ? candidateBranch(adopted)?.resources ?? action.resources : action.resources).map(resource => rootedResource(action, resource));
		if (!changed.length) return;
		const paths = (adopted || effect === "workspace_mutation" ? changed : []).filter(resource => path.isAbsolute(resource));
		// A native unbounded call names no write set: running work validates at adoption, finished branches off the Actor's path.
		const unbounded = !adopted && effect === "unbounded", revalidate: Candidate[] = [];
		const candidates = candidateStore.values(session.id);
		const invalid = new Set<Candidate>();
		for (const candidate of candidates) {
			if (candidate === adopted || (adopted && descendsFrom(candidate, adopted))) continue;
			try {
				const removed = paths.length ? candidateBranch(candidate)?.invalidateInputs?.(paths) : undefined;
				if (removed) candidateStore.invalidateInputs(session.id, candidate, removed);
			}
			catch { invalid.add(candidate); }
			// Once an Actor reserves a candidate, its freshness and compatibility checks
			// are authoritative. Cache invalidation may only retire unclaimed work.
			if (!reservationAvailable(candidate.work.reservation)) continue;
			if (candidate.key.resources.some((resource) => changed.some((path) => resourcePathsOverlap(rootedResource(candidate.key, resource), path)))) {
				if (unbounded) { if (candidate.work.execution.status === "succeeded" && candidate.work.reservation.kind !== "shared") revalidate.push(candidate); continue; }
				for (const descendant of candidates) {
					// Completed shared outputs are checked against their sealed evidence at every adoption.
					// Pending work and checkpoint descendants still retain conservative conflict invalidation.
					if (descendant === candidate && candidate.work.execution.status === "succeeded" && candidate.work.reservation.kind === "shared") continue;
					if (descendant === candidate || descendsFrom(descendant, candidate)) invalid.add(descendant);
				}
			}
		}
		// The Actor's settlement relaunches what still has a future; relaunched now, work for the decision it settles would only be retired.
		invalidateCandidates(session, [...invalid].filter((candidate) => reservationAvailable(candidate.work.reservation)), cause("freshness", "authoritative_resource_changed"), false, false);
		if (revalidate.length) trackSourceTask(session, (async () => {
			for (const candidate of revalidate) {
				if (session.lifecycle.sealed || !reservationAvailable(candidate.work.reservation) || !candidateStore.has(session.id, candidate)) continue;
				const validation = await validateCandidate(candidate).catch(() => undefined);
				if (validation?.status === "stale") invalidateCandidates(session, candidateStore.values(session.id).filter((descendant) =>
					reservationAvailable(descendant.work.reservation) && (descendant === candidate || descendsFrom(descendant, candidate))), validation.cause);
			}
		})());
	};

	const pruneActionContexts = (session: Session): void => {
		for (const [id, context] of session.actionContexts) if (context.opportunity.state.status !== "matching") releaseActionContext(session, id);
	};

	const beginTurnClosure = (
		state: Turn,
		input: { readonly failure: ResolutionCause; readonly terminal: boolean; readonly notifyHost: boolean },
	): TurnClosure | undefined => {
		if (state.lifecycle !== "active") return undefined;
		settlePredictionFrontier(state);
		state.lifecycle = "closing";
		state.generation.expire(input.failure);
		for (const node of state.session.plan.pending()) {
			if ((!node.actionKey || node.execution.status === "preparing") && state.session.actionContexts.get(node.identity.id)?.admissionController.signal.aborted)
				failUnlaunchable(state.session, node, cause("source", "generation_expired"));
		}
		for (const preview of state.actorPreviews.values()) { abandonActorPreview(state, preview, input.failure); }
		state.actorPreviews.clear();
		return { state, terminal: input.terminal, notifyHost: input.notifyHost };
	};

	const clearActorActions = (state: Turn): void => {
		for (const action of state.actorActions) { const capture = action.takeCapture(); if (capture) state.session.lifecycle.release(capture); }
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
		const failure = cause("control", terminal ? "terminal_turn" : mode === "disposed" ? "session_disposed" : "disabled");
		const closures = [...session.turns.values()].flatMap((state) => {
			const closure = beginTurnClosure(state, { failure, terminal, notifyHost: terminal });
			return closure ? [closure] : [];
		});

		releaseAllSourceSlots(session, failure);
		const planFailure = terminal ? cause("control", "session_terminal") : failure;
		for (const node of session.plan.unsettled()) settleUnobserved(session, node, planFailure);
		session.plan.clear();
		while (session.sourceTasks.size) await Promise.allSettled(session.sourceTasks);
		await session.effects.flush();
		for (const closure of closures) await completeTurnClosure(closure);
		for (const state of session.turns.values()) clearActorActions(state);
		session.turns.clear();

		for (const candidate of candidateStore.values(session.id)) {
			if (!terminal || candidate.work.reservation.kind === "exclusive") discardCandidate(session, candidate, planFailure);
		}
		if (terminal) { queueTaskEvent(session, turnID, performance.now()); } else { session.timeline = undefined; }
		await session.effects.flush();
		if (terminal || mode === "disposed") await flushSources();
		pruneActionContexts(session);
		if (!terminal) await session.lifecycle.drain();
	};

	/** Called only inside the owning session's lifecycle lane, including replacement on start. */
	const closeTurn = async (state: Turn): Promise<void> => {
		const closure = beginTurnClosure(state, { failure: cause("control", "turn_finished"), terminal: false, notifyHost: true });
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
		await Promise.all([...sessionStates.keys()].map((sessionID) => disableSession(sessionID)));
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
			if (!needsResourceSamples()) scheduler.close();
		});
	};

	const dispose = async (): Promise<void> => { await Promise.all([...sessionStates.keys()].map((sessionID) => disposeSession(sessionID))); };

	const inspect = (sessionID?: SessionID): SpeculativeRuntimeInspection => {
		const selectedSessions = sessionID === undefined ? [...sessionStates.values()] : maybe(sessionStates.get(sessionID));
		const candidates = sessionID === undefined ? candidateStore.allValues() : candidateStore.values(sessionID);
		const planNodes = selectedSessions.flatMap((session) => session.plan.values());
		const telemetry = selectedSessions.map((session) => session.events.snapshot());
		const resources = scheduler.inspect();
		return {
			...(resources ? { resources: { ...resources,
				queuedPreparations: [...preparations.values()].filter(entry => !entry.active).length } } : {}),
			activeTurns: selectedSessions.reduce((total, session) => total + session.turns.size, 0),
			exclusiveCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "exclusive").length,
			sharedCandidates: candidates.filter((candidate) => candidate.work.reservation.kind === "shared").length,
			pendingPredictions: selectedSessions.reduce((total, session) => total + session.pendingPredictions, 0),
			deferredPlanActions: planNodes.filter((node) => node.execution.status === "deferred" || node.execution.status === "preparing").length,
			activePlanActions: planNodes.filter(
				(node) =>
					node.execution.status === "scheduled" || node.execution.status === "queued" || node.execution.status === "running",
			).length,
			executionBlockedPlanActions: planNodes.filter((node) => node.execution.status === "execution_blocked").length,
			blockedPlanActions: planNodes.filter((node) => node.readiness === "blocked").length,
			pendingTelemetryEvents: telemetry.reduce((total, item) => total + item.pending, 0),
			droppedTelemetryEvents: telemetry.reduce((total, item) => total + item.dropped, 0),
		};
	};

	const cacheSnapshot = (session: Session, settings: SpeculativeActionSettings): SpeculativeCacheSnapshot => {
		const candidates = candidateStore.values(session.id);
		const segments = candidateStore.snapshot(session.id, candidate => candidate.work.reservation.kind === "shared");
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
		};
	};

	/** Capture at the state transition; policy callbacks may delay delivery but cannot change the snapshot. */
	const eventEnvelope = (session: Session, turnID: string, settings: SpeculativeActionSettings) => ({
		sessionID: session.id,
		turnID,
		timestamp: performance.timeOrigin + performance.now(),
		cache: cacheSnapshot(session, settings),
	});

	const queueTaskEvent = (session: Session, turnID: string, completedAt: number): void => {
		const timeline = session.timeline;
		if (!timeline) return;
		session.timeline = undefined;
		if (adapter.onEvent) session.events.enqueue({ type: "task", ...eventEnvelope(session, turnID, session.settings), timing: timeline.measure(completedAt) });
	};

	const queueSourceRequestEvent = (session: Session, turnID: string, settings: SpeculativeActionSettings, result: SettledSourceRequest): void => {
		if (adapter.onEvent) session.events.enqueue({
			type: "source_request", ...eventEnvelope(session, turnID, settings),
			request: { request: result.request, durationMs: result.durationMs, settlement: result.settlement,
				...(result.draftTokens ? { draftTokens: result.draftTokens } : {}) },
			totalDraftTokens: session.tokenTotal,
		});
	};

	const queueCandidateEvent = (session: Session, candidate: Candidate): void => {
		if (!adapter.onEvent) return;
		const execution = candidate.work.execution;
		if (execution.status === "queued") return;
		const state = execution.status === "running" ? { status: "running" as const }
			: execution.status === "succeeded" ? { status: "succeeded" as const, executionMs: execution.executionMs } : { ...execution };
		const descriptor = candidateEventDescriptor(candidate);
		session.events.enqueue({ type: "candidate", ...eventEnvelope(session, candidate.owner.startInput.turnID, candidate.owner.settings), candidate: descriptor, state });
	};

	return {
		trackActorTool: async (sessionID, execute) => {
			const finish = sessionStates.get(sessionID)?.timeline?.startToolWait(performance.now());
			try { return await execute(); } finally { finish?.(performance.now()); }
		},
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
}
