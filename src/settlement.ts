import type { AssistantMessageEvent } from "@earendil-works/pi-ai";
import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import type { DrafterToolDefinition } from "./config-schema.ts";
import { nonNegativeNumber } from "./config-schema.ts";
import type {
	CapturedExecutionWorldResult,
	ExecutionScope,
	SpeculativeExecutionRoute,
	WorldExecutionMetrics,
	WorldResultCapture,
} from "./execution-world.ts";
import {
	asRecord,
	errorDetail,
	nonNegativeCount as finiteCount,
	nonNegativeFinite as metric,
} from "./primitives.ts";
import { waitForCompletion } from "./runtime-lifecycle.ts";
import type { SpeculativeTaskTiming, SubcomputeMetrics, ToolComputationTiming } from "./task-timing.ts";
import { TimelineInterval } from "./task-timing.ts";
export type ResolutionStage =
	| "source"
	| "admission"
	| "execution"
	| "matching"
	| "authorization"
	| "freshness"
	| "compatibility"
	| "reconstruction"
	| "commit"
	| "retention"
	| "control";
/** Machine-readable classification. Human diagnostics never participate in policy. */
export interface ResolutionCause {
	readonly stage: ResolutionStage;
	readonly code: string;
	readonly detail?: string;
}
export interface ValidationMetrics {
	readonly bytesRead: number;
	readonly filesRead: number;
	readonly mode: "watcher" | "exact";
}
export type ResourceValidation = {
	readonly metrics: ValidationMetrics;
} & (
	| {
			readonly status: "valid";
	  }
	| ({
			readonly cause: ResolutionCause & {
				readonly stage: "freshness";
			};
	  } & (
			| {
					readonly status: "stale";
					/** Changed inputs were revoked while independent computations survived; one observation rebuild may help. */
					readonly reconstruct?: true;
			  }
			| {
					readonly status: "indeterminate";
			  }
	  ))
);
export interface PredictionIdentity {
	readonly id: string;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
}
export interface ActorActionIdentity {
	readonly id: string;
	/** Unique order of this concrete tool call. */
	readonly sequence: number;
	/** Actor tool batch used by prediction horizons. Defaults to sequence for external adapters. */
	readonly decisionSequence?: number;
	readonly turnID: string;
}
export type PredictionAdoption =
	| {
			readonly status: "adopted";
			readonly candidateID: string;
	  }
	| {
			readonly status: "rejected";
			readonly candidateID?: string;
			readonly cause: ResolutionCause;
	  };
export type PredictionSettlement = {
	readonly prediction: PredictionIdentity;
} & (
	| {
			readonly observation: "unobserved";
			readonly cause: ResolutionCause;
	  }
	| ({
			readonly observation: "observed";
			readonly actorAction: ActorActionIdentity;
	  } & (
			| {
					readonly match: {
						readonly matched: false;
					};
			  }
			| {
					readonly match: {
						readonly matched: true;
						readonly relation: ActionKeyMatch;
						readonly adoption: PredictionAdoption;
					};
			  }
	  ))
);
/** Eligible learning samples: shared work earns adoption without inventing a tool match. */
export function predictionLearning(settlement: PredictionSettlement, served?: boolean) {
	const match = settlement.observation === "observed" ? settlement.match : undefined;
	const adopted = served ?? (match?.matched === true && match.adoption.status === "adopted");
	return {
		matched: match?.matched || !adopted ? match?.matched : undefined,
		adopted: adopted || match?.matched ? adopted : undefined,
	};
}
export type SourceRequestSettlement =
	| {
			readonly status: "produced";
			readonly proposalCount: number;
	  }
	| {
			readonly status: "empty";
			readonly cause?: ResolutionCause & {
				readonly stage: "source";
			};
	  }
	| {
			readonly status: "timeout";
			readonly cause: ResolutionCause & {
				readonly stage: "source";
			};
	  }
	| {
			readonly status: "error";
			readonly cause: ResolutionCause & {
				readonly stage: "source";
			};
	  }
	| {
			readonly status: "aborted";
			readonly cause: ResolutionCause & {
				readonly stage: "source";
			};
	  };
export type SourceRequestKind = "proposal" | "continuation" | "observation";
export interface SourceRequestIdentity {
	readonly source: string;
	readonly turnID: string;
	readonly index: number;
	readonly kind: SourceRequestKind;
	/** Actor decision batch this request is trying to cover. */
	readonly targetDecisionSequence: number;
}
export interface SettledSourceRequest {
	readonly request: SourceRequestIdentity;
	readonly durationMs: number;
	readonly settlement: SourceRequestSettlement;
	/** Provider tokens reported by the time the request settled. */
	readonly draftTokens?: number;
}
export interface CandidateRejection {
	readonly candidateID: string;
	readonly match: ActionKeyMatch;
	readonly cause: ResolutionCause;
}
export type ActorActionProvider = {
	readonly toolExecution: TimelineInterval;
} & (
	| {
			readonly kind: "speculative";
			readonly candidateID: string;
			readonly match: ActionKeyMatch;
	  }
	| ({
			readonly kind: "actor";
			readonly durationMs: number;
	  } & (
			| {
					readonly origin: "fallback";
					readonly cause: ResolutionCause;
					readonly candidateID?: string;
					readonly isError: boolean;
			  }
			| {
					readonly origin: "preview";
					readonly candidateID: string;
					readonly isError: false;
			  }
	  ))
);
export interface ActorActionSettlement {
	readonly actorAction: ActorActionIdentity;
	readonly tool: string;
	readonly actionKeyHash?: string;
	readonly matchedPredictions: readonly PredictionIdentity[];
	readonly rejections: readonly CandidateRejection[];
	readonly provider: ActorActionProvider;
}
export function cause<Stage extends ResolutionStage>(
	stage: Stage,
	code: string,
	detail?: string,
): ResolutionCause & {
	readonly stage: Stage;
} {
	return Object.freeze({ stage, code, ...(detail ? { detail } : {}) });
}
export function zeroValidationMetrics(mode: ValidationMetrics["mode"] = "exact"): ValidationMetrics {
	return { bytesRead: 0, filesRead: 0, mode };
}
type TurnIdentity<SessionID> = Pick<TurnInput<SessionID>, "sessionID" | "turnID">;
export interface SpeculativeActionSettings {
	/** Enable speculative production, including call predictions and observed-operation preparation. */
	readonly enabled: boolean;
	/** Cache and adopt reusable work. Omitted defaults to true; observation and learning stay active. */
	readonly reuseEnabled?: boolean;
	/** Optional prediction scope; omitted means all registered tools. Never a reuse or permission boundary. */
	readonly tools?: readonly string[];
	readonly prepareObservedOperations?: boolean;
	readonly drafterEnabled?: boolean;
	readonly candidateLimit?: number;
	readonly maxConcurrentActions?: number;
	readonly resourceCacheMaxEntries: number;
	readonly resourceCacheMaxBytes?: number;
	readonly predictionTimeoutMs?: number;

	/** Source-owned configuration. The runtime treats every value as opaque. */
	readonly sourceConfig?: Readonly<Record<string, unknown>>;
}
export type CandidatePreflight =
	| {
			readonly ok: true;
	  }
	| {
			readonly ok: false;
			readonly reason: string;
			readonly detail?: string;
	  };
/** A pre-Actor freshness baseline that can promote the Actor's own result into the shared cache. */
export interface AuthoritativeResultCapture<Output>
	extends WorldResultCapture<Output>,
		Omit<CapturedExecutionWorldResult<Output>, "capture"> {}
/** A validated, concrete prediction suitable for a target-model draft verifier. */
export interface MaterializedSpeculativeCandidate<SessionID> extends TurnIdentity<SessionID> {
	readonly tool: string;
	readonly depth?: number;
	readonly horizon?: number;
	readonly conditionalProbability?: number;
	readonly empiricalProbability?: number;
	readonly adoptionProbability?: number;
	/** Absolute Actor decision that this prediction is expected to match. */
	readonly targetDecision: number;
	/** Last Actor decision for which the prediction may still be considered. */
	readonly lastDecision: number;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
	/** Producer-facing arguments, before K(a) canonicalization. */
	readonly input: Readonly<Record<string, unknown>>;
	/** Exact Actor-visible K(a) represented by the prediction and target-decoder draft. */
	readonly predictedAction: ActionKey;
}
/** The exact K(a) Runtime assigned to an authoritative Actor tool call. */
export interface MaterializedActorAction<SessionID>
	extends TurnIdentity<SessionID>,
		Pick<ActionKey, "tool" | "input"> {
	readonly identity: ActorActionIdentity;
	readonly action: ActionKey;
}
/** Policy-facing authoritative settlement, kept separate from diagnostic events. */
export interface ActorActionFeedback<SessionID> extends TurnIdentity<SessionID> {
	readonly action?: ActionKey;
	readonly settlement: ActorActionSettlement;
	/** Frozen candidate attribution supplied without routing policy through the event sink. */
	readonly candidate?: CandidateEventDescriptor;
	readonly computation?: ToolComputationTiming;
}
/** Policy-facing prediction outcome with the tool context omitted from generic settlement identity. */
export interface PredictionFeedback<SessionID> extends TurnIdentity<SessionID> {
	readonly tool: string;
	readonly action?: ActionKey;
	readonly settlement: PredictionSettlement;
}
export interface TurnInput<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly terminal?: boolean;
}
export interface RuntimeTurnContext<StartInput, StateData> {
	readonly startInput: StartInput;
	readonly data: StateData;
	readonly settings: SpeculativeActionSettings;
}
/** Authoritative observations are available to learners even while every prediction source is off. */
export interface ActorObservation<SessionID, Output, StartInput, ConsumeInput, StateData>
	extends ActorActionFeedback<SessionID>,
		RuntimeTurnContext<StartInput, StateData> {
	readonly consumeInput: ConsumeInput;
	readonly tool: string;
	readonly concrete: Record<string, unknown>;
	readonly output?: Output;
	readonly durationMs: number;
	readonly order: number;
	readonly signal?: AbortSignal;
}
/** Typed policy events retain attribution without granting execution, reuse or prediction authority. */
export interface RuntimeEventMap<SessionID, Output, StartInput, ConsumeInput, StateData> {
	readonly turn_started: RuntimeTurnContext<StartInput, StateData> & {
		readonly decisionSequence: number;
		readonly definitions: readonly DrafterToolDefinition[];
		readonly signal?: AbortSignal;
	};
	readonly turn_finished: Pick<RuntimeTurnContext<StartInput, StateData>, "startInput" | "settings"> & {
		readonly terminal: boolean;
	};
	readonly session_closed: {
		readonly sessionID: SessionID;
		readonly terminal: boolean;
	};
	readonly candidate_materialized: MaterializedSpeculativeCandidate<SessionID>;
	readonly actor_materialized: MaterializedActorAction<SessionID>;
	readonly actor_settled: ActorActionFeedback<SessionID>;
	readonly actor_observed: ActorObservation<SessionID, Output, StartInput, ConsumeInput, StateData>;
	readonly prediction_admitted: {
		readonly feedback?: unknown;
		readonly actionID: string;
		readonly source: string;
		readonly issued: boolean;
	};
	readonly prediction_settled: PredictionFeedback<SessionID>;
	/** Delivered after claimed work is consumed or released, independently of source enablement. */
	readonly prediction_feedback: {
		readonly feedback?: unknown;
		readonly actionID: string;
		readonly settlement: PredictionSettlement;
		readonly served: boolean;
	};
	/** Once per physical execution; shared predictions do not incur another production cost. */
	readonly execution_settled: TurnIdentity<SessionID> & {
		readonly candidate: CandidateEventDescriptor;
		readonly reuseFeedback: unknown;
		readonly status: "succeeded" | "failed" | "cancelled";
		readonly executionMs: number;
	};
	readonly source_request: TurnIdentity<SessionID> & {
		readonly request: SettledSourceRequest;
	};
}
export interface ActualToolCall {
	readonly id?: string;
	readonly tool: string;
	readonly input: unknown;
}
export interface SpeculativeCacheSnapshot {
	readonly cacheCapacity: number;
	readonly cacheByteCapacity?: number;
	readonly cacheCold: number;
	readonly cacheHot: number;
	readonly inFlightJobs: number;
	readonly resultEntries: number;
	readonly resultBytes: number;
	readonly branchEntries: number;
	readonly branchBytes: number;
	readonly exclusiveCandidates: number;
	readonly sharedCandidates: number;
}
export interface CandidateEventDescriptor {
	readonly kind?: "operation";
	readonly id: string;
	readonly origin: "prediction" | "actor_preview" | "actor_result" | "backend";
	readonly tool: string;
	readonly route: SpeculativeExecutionRoute;
	readonly world?: {
		readonly backend: string;
		readonly executionMetrics: WorldExecutionMetrics;
	};
	readonly source: string;
	readonly mode?: string;
	readonly depth: number;
	readonly predictedAction: string;
}
export type CandidateExecutionProjection =
	| {
			readonly status: "running";
	  }
	| {
			readonly status: "succeeded";
			readonly executionMs: number;
	  }
	| Extract<
			CandidateExecutionState<never>,
			{
				readonly status: "failed" | "cancelled";
			}
	  >;
interface EventEnvelope<SessionID> {
	readonly subcomputations?: SubcomputeMetrics;
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly timestamp: number; // Epoch milliseconds from performance.timeOrigin + performance.now().
	readonly cache: SpeculativeCacheSnapshot;
}
/** Immutable observability projections. Policy and learning never consume this stream. */
export type SpeculativeActionEvent<SessionID> = EventEnvelope<SessionID> &
	(
		| {
				readonly type: "task";
				readonly timing: SpeculativeTaskTiming;
		  }
		| {
				readonly type: "source_request";
				readonly request: SettledSourceRequest;
				readonly totalDraftTokens: number;
		  }
		| {
				readonly type: "prediction";
				readonly settlement: PredictionSettlement;
				readonly tool?: string;
				readonly predictedAction?: string;
				readonly mode?: string;
		  }
		| {
				readonly type: "candidate";
				readonly candidate: CandidateEventDescriptor;
				readonly state: CandidateExecutionProjection;
		  }
		| {
				readonly type: "actor_action";
				readonly settlement: ActorActionSettlement;
				readonly actualAction: string;
				readonly candidate?: CandidateEventDescriptor;
				/** Actual accepted computation for this Actor call, including consumed preparations. */
				readonly computation?: ToolComputationTiming;
		  }
	);
export class SourceRequestSuppressed extends Error {
	readonly cause: ResolutionCause & {
		readonly stage: "source";
	};
	constructor(cause: SourceRequestSuppressed["cause"]) {
		super(cause.detail ?? cause.code);
		this.cause = Object.freeze({ ...cause });
	}
}
/** Producer timing/cancellation; the caller retains physical production until close, separately from admission. */
export async function runSourceRequest<Value>(input: {
	readonly request: SourceRequestIdentity;
	readonly signal: AbortSignal;
	readonly timeoutMs?: number;
	readonly produce: (signal: AbortSignal) => Value | Promise<Value>;
	readonly count: (value: Value) => number;
}): Promise<
	SettledSourceRequest & {
		readonly value?: Value;
	}
> {
	const startedAt = performance.now();
	const finish = (settlement: SourceRequestSettlement): SettledSourceRequest =>
		Object.freeze({
			request: Object.freeze({ ...input.request }),
			durationMs: Math.max(0, performance.now() - startedAt),
			settlement: Object.freeze(settlement),
		});
	const aborted = () =>
		finish({
			status: "aborted",
			cause: input.signal.reason instanceof SourceRequestSuppressed
				? input.signal.reason.cause : cause("source", "generation_expired"),
		});
	const failed = (code: string, error: unknown) =>
		finish({
			status: "error",
			cause: cause("source", code, errorDetail(error)),
		});
	if (input.signal.aborted) return aborted();
	const controller = new AbortController();
	// Produced proposals can leave preparation in flight until their generation closes.
	const signal = AbortSignal.any([input.signal, controller.signal]);
	const producer = Promise.resolve()
		.then(() => {
			signal.throwIfAborted();
			return input.produce(signal);
		})
		.then(
			(value) => ({ kind: "produced" as const, value }),
			(error) => ({ kind: "error" as const, error }),
		);
	const waited = await waitForCompletion(
		producer,
		input.signal,
		nonNegativeNumber(input.timeoutMs, undefined),
	);
	if (waited.status === "deadline") {
		const expiration = cause("source", "timeout");
		controller.abort(expiration);
		return finish({ status: "timeout", cause: expiration });
	}
	if (waited.status === "aborted" || input.signal.aborted) return aborted();
	const outcome = waited.value;
	if (outcome.kind === "error")
		return outcome.error instanceof SourceRequestSuppressed
			? finish({ status: "empty", cause: outcome.error.cause })
			: failed("producer_error", outcome.error);
	let proposalCount: number;
	try {
		proposalCount = finiteCount(input.count(outcome.value));
	} catch (error) {
		return failed("result_error", error);
	}
	return {
		...finish(proposalCount > 0 ? { status: "produced", proposalCount } : { status: "empty" }),
		value: outcome.value,
	};
}
export type RuntimeEventHandlers<Events> = {
	readonly [Type in keyof Events]?: (event: Events[Type]) => void | Promise<void>;
};
/** Lossless policy subscriptions. The owning session orders delivery outside Actor settlement. */
export class RuntimeSubscriptions<Events> {
	private readonly listeners = new Set<RuntimeEventHandlers<Events>>();
	subscribe(handlers: RuntimeEventHandlers<Events>): () => void {
		const owned = Object.freeze({ ...handlers });
		this.listeners.add(owned);
		return () => {
			this.listeners.delete(owned);
		};
	}
	async publish<Type extends keyof Events>(
		type: Type,
		event: Events[Type],
		own: (event: Events[Type]) => Events[Type] = (event) => event,
	): Promise<void> {
		await Promise.allSettled(
			[...this.listeners].map(async (handlers) => {
				if (this.listeners.has(handlers)) await handlers[type]?.(own(event));
			}),
		);
	}
}
export type CandidateExecutionState<Output> =
	| {
			readonly status: "created";
	  }
	| {
			readonly status: "running";
			readonly startedAt: number;
	  }
	| {
			readonly status: "succeeded";
			readonly output: Output;
			readonly toolExecution: TimelineInterval;
			readonly executionMs: number;
	  }
	| {
			readonly status: "failed" | "cancelled";
			readonly cause: ResolutionCause;
			readonly executionMs: number;
	  };
export type CandidateReservation =
	| {
			readonly kind: "shared";
			readonly owners: readonly string[];
	  }
	| {
			readonly kind: "exclusive";
			readonly status: "available" | "reserved" | "consumed";
			readonly turnID?: string;
	  };
export type CandidateExecutionSettlement<Output> = Extract<
	CandidateExecutionState<Output>,
	{
		readonly status: "succeeded" | "failed" | "cancelled";
	}
>;
type CandidateReservationLeaseState = "active" | "released" | "consumed";
/** One acquired reservation. Every exit can safely call `release`; adoption makes it a no-op. */
export interface CandidateReservationLease {
	readonly owner: string;
	readonly kind: CandidateReservation["kind"];
	readonly state: CandidateReservationLeaseState;
	readonly active: boolean;
	release(): boolean;
	adopt(): boolean;
}
/** Owns execution and reservation as independent, monotonic facts. */
export class CandidateExecution<Output> {
	readonly controller: AbortController;
	private readonly completionState = Promise.withResolvers<CandidateExecutionSettlement<Output>>();
	readonly completion = this.completionState.promise;
	private readonly released?: () => void;
	private executionValue: CandidateExecutionState<Output> = Object.freeze({
		status: "created",
	});
	private reservationValue: CandidateReservation;
	constructor(reuse: "shared" | "exclusive", controller = new AbortController(), released?: () => void) {
		this.controller = controller;
		this.released = released;
		this.reservationValue =
			reuse === "shared"
				? Object.freeze({ kind: "shared", owners: Object.freeze([]) })
				: Object.freeze({ kind: "exclusive", status: "available" });
	}
	get execution(): CandidateExecutionState<Output> {
		return this.executionValue;
	}
	get reservation(): CandidateReservation {
		return this.reservationValue;
	}
	start(startedAt: number): boolean {
		if (this.executionValue.status !== "created") return false;
		this.executionValue = Object.freeze({
			status: "running",
			startedAt: metric(startedAt),
		});
		return true;
	}
	succeed(output: Output, toolExecution: TimelineInterval, executionMs: number): boolean {
		return this.finish({
			status: "succeeded",
			output,
			toolExecution,
			executionMs,
		});
	}
	fail(cause: ResolutionCause, executionMs: number): boolean {
		return this.finish({ status: "failed", cause, executionMs });
	}
	cancel(cause: ResolutionCause, executionMs: number): boolean {
		const changed = this.finish({ status: "cancelled", cause, executionMs });
		if (changed) this.controller.abort(cause);
		return changed;
	}
	acquire(owner: string): CandidateReservationLease | undefined {
		if (this.executionValue.status === "failed" || this.executionValue.status === "cancelled")
			return undefined;
		const reservation = this.reservationValue;
		if (reservation.kind === "shared") {
			if (reservation.owners.includes(owner)) return undefined;
			this.reservationValue = Object.freeze({
				kind: "shared",
				owners: Object.freeze([...reservation.owners, owner]),
			});
		} else {
			if (reservation.status !== "available") return undefined;
			this.reservationValue = Object.freeze({
				kind: "exclusive",
				status: "reserved",
				turnID: owner,
			});
		}
		const kind = reservation.kind;
		let state: CandidateReservationLeaseState = "active";
		const settle = (adopt: boolean) => {
			if (state !== "active") return false;
			const consumed = adopt && kind === "exclusive";
			if (consumed && this.executionValue.status !== "succeeded") return false;
			const current = this.reservationValue;
			this.reservationValue = Object.freeze(
				current.kind === "shared"
					? {
							kind: "shared",
							owners: Object.freeze(current.owners.filter((value) => value !== owner)),
						}
					: { kind: "exclusive", status: consumed ? "consumed" : "available" },
			);
			state = consumed ? "consumed" : "released";
			if (kind === "exclusive") this.released?.();
			return true;
		};
		return {
			owner,
			kind,
			get state() {
				return state;
			},
			get active() {
				return state === "active";
			},
			release: () => settle(false),
			adopt: () => settle(true),
		};
	}
	private finish(settlement: CandidateExecutionSettlement<Output>): boolean {
		if (
			this.executionValue.status !== "running" &&
			(this.executionValue.status !== "created" || settlement.status === "succeeded")
		)
			return false;
		const owned =
			settlement.status === "succeeded"
				? {
						...settlement,
						toolExecution: TimelineInterval.from(settlement.toolExecution),
					}
				: { ...settlement, cause: Object.freeze({ ...settlement.cause }) };
		this.executionValue = Object.freeze({
			...owned,
			executionMs: metric(owned.executionMs),
		});
		this.completionState.resolve(this.executionValue);
		return true;
	}
}
export type ActorStreamPreview = {
	readonly type: "call";
	readonly call: {
		readonly id: string;
		readonly name: string;
		readonly arguments: Record<string, unknown>;
	};
};
type StreamedCall = {
	arguments: string;
	depth: number;
	lexeme: "json" | "string" | "escape";
	ready: boolean;
};
/** Delivers a complete streamed call once its argument object parses. */
export class ActorStreamPreviewTracker {
	private readonly calls = new Map<number, StreamedCall | "delivered">();
	clear(): void {
		this.calls.clear();
	}
	observe(update: AssistantMessageEvent): readonly ActorStreamPreview[] {
		if (update.type === "toolcall_end") {
			const state = this.calls.get(update.contentIndex);
			this.calls.delete(update.contentIndex);
			const { id, name, arguments: args } = update.toolCall;
			return state === "delivered" ? [] : [{ type: "call", call: { id, name, arguments: args } }];
		}
		if (update.type !== "toolcall_start" && update.type !== "toolcall_delta") return [];
		const call = update.partial.content[update.contentIndex];
		if (call?.type !== "toolCall") return [];
		const prior = this.calls.get(update.contentIndex);
		if (prior === "delivered") return [];
		const state: StreamedCall = prior ?? {
			arguments: "",
			depth: 0,
			lexeme: "json",
			ready: false,
		};
		this.calls.set(update.contentIndex, state);
		if (update.type === "toolcall_delta") {
			state.arguments += update.delta;
			// Scan only new characters; strict parsing still decides whether the complete input is an object.
			for (const character of update.delta) {
				if (state.lexeme === "escape") state.lexeme = "string";
				else if (character === "\\" && state.lexeme === "string") state.lexeme = "escape";
				else if (character === '"') state.lexeme = state.lexeme === "string" ? "json" : "string";
				else if (state.lexeme === "json") {
					if (character === "{" || character === "[") state.depth++;
					else if (character === "}" || character === "]") state.ready = --state.depth === 0 || state.ready;
				}
			}
		}
		const result: ActorStreamPreview[] = [];
		if (update.type === "toolcall_delta" && state.ready && call.name && call.id) {
			state.ready = false;
			const parsed = jsonObject(state.arguments);
			if (parsed) {
				this.calls.set(update.contentIndex, "delivered");
				result.push({
					type: "call",
					call: { id: call.id, name: call.name, arguments: parsed },
				});
			}
		}
		return result;
	}
}
function jsonObject(value: string): Record<string, unknown> | undefined {
	try {
		return asRecord(JSON.parse(value));
	} catch {
		return undefined;
	}
}
export function diagnosticAction(tool: string, input: unknown, key?: ActionKey): string {
	return diagnosticJson({
		tool,
		input: key?.input ?? input,
		...(key ? { actionKeyHash: key.hash } : {}),
	});
}
export function diagnosticJson(value: unknown): string {
	try {
		return JSON.stringify(redactDiagnostics(value));
	} catch {
		return "[unserializable]";
	}
}
export function redactDiagnostics(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(redactDiagnostics);
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>).map(([key, item]) => [
			key,
			/(token|secret|password|authorization|cookie|api.?key)/i.test(key)
				? "[redacted]"
				: redactDiagnostics(item),
		]),
	);
}
export interface ToolOperation {
	readonly tool: string;
	/** Absent for capability warm-up before a concrete call exists. */
	readonly callID?: string;
	readonly scope?: ExecutionScope;
	readonly input: unknown;
	readonly signal?: AbortSignal;
	/** Present after the caller has resolved the canonical action identity. */
	readonly action?: ActionKey;
}
