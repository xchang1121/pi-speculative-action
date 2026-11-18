import type { AssistantMessageEvent } from "@earendil-works/pi-ai";
import type {
	ActionKey,
	ActionKeyMatch,
	ActionProjectionRule,
	ActionSemanticsRegistry,
} from "./action-semantics.ts";
import type { DrafterToolDefinition } from "./config-schema.ts";
import { nonNegativeNumber } from "./config-schema.ts";
import type {
	CapturedExecutionWorldResult,
	ExecutionOperationBinding,
	ExecutionResourceContext,
	ExecutionScope,
	SpeculativeExecutionRoute,
	WorldBranch,
	WorldExecutionMetrics,
	WorldResultCapture,
} from "./execution-world.ts";
import {
	asRecord,
	errorDetail,
	nonNegativeCount as finiteCount,
	isSafeCount,
	nonNegativeFinite as metric,
} from "./primitives.ts";
import { waitForCompletion } from "./runtime-lifecycle.ts";
import type { HardwareResources } from "./system-resources.ts";
import type { SpeculativeTaskTiming, ToolComputationTiming } from "./task-timing.ts";
import { TimelineInterval } from "./task-timing.ts";
export type ResolutionStage =
	| "source"
	| "plan"
	| "admission"
	| "execution"
	| "matching"
	| "authorization"
	| "freshness"
	| "compatibility"
	| "projection"
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
export interface PlanActionIdentity {
	readonly kind?: "operation";
	readonly id: string;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
}
export type PredictionIdentity = PlanActionIdentity;
export interface ActorActionIdentity {
	readonly kind?: "operation";
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
	readonly scheduling?: Partial<import("./config-schema.ts").SchedulingSettings>;
	/** Source-owned configuration. The runtime treats every value as opaque. */
	readonly sourceConfig?: Readonly<Record<string, unknown>>;
}
export interface SpeculativeDraftCandidate extends Omit<PlanAction, "id" | "background"> {
	readonly source?: string;
	readonly proposalID?: string;
	readonly actionID?: string;
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
/** Read-only execution view passed to host callbacks. Prediction facts are supplied separately. */
export interface SpeculativeCandidate extends Pick<
	PlanAction,
	"tool" | "empiricalProbability" | "adoptionProbability" | "conditionalProbability" | "depth"
> {
	readonly id: string;
	readonly key?: ActionKey;
	readonly input: Readonly<Record<string, unknown>>;
	/** Elapsed execution time once settled; runtime work and reservation state stay private. */
	readonly executionMs?: number;
	readonly source?: string;
	readonly planDependencies?: PlanAction["dependsOn"];
}
/** A pre-Actor freshness baseline that can promote the Actor's own result into the shared cache. */
export interface AuthoritativeResultCapture<Output>
	extends WorldResultCapture<Output>, Omit<CapturedExecutionWorldResult<Output>, "capture"> {}
/** A validated, concrete prediction suitable for a target-model draft verifier. */
export interface MaterializedSpeculativeCandidate<SessionID>
	extends
		Pick<
			PlanAction,
			| "tool"
			| "depth"
			| "horizon"
			| "conditionalProbability"
			| "empiricalProbability"
			| "adoptionProbability"
			| "expectedDurationMs"
		>,
		TurnIdentity<SessionID> {
	/** Absolute Actor decision that this prediction is expected to match. */
	readonly expectedDecisionSequence: number;
	/** Last Actor decision for which the prediction may still be considered. */
	readonly latestDecisionSequence: number;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
	/** Producer-facing arguments, before K(a) canonicalization or execution projection. */
	readonly input: Readonly<Record<string, unknown>>;
	/** Exact Actor-visible K(a) represented by the prediction and target-decoder draft. */
	readonly predictedAction: ActionKey;
	/** K(a) actually scheduled; it may cover the prediction through a lossless projection. */
	readonly executionAction: ActionKey;
}
/** The exact K(a) Runtime assigned to an authoritative Actor tool call. */
export interface MaterializedActorAction<SessionID>
	extends TurnIdentity<SessionID>, Pick<ActionKey, "tool" | "input"> {
	readonly identity: ActorActionIdentity;
	readonly action: ActionKey;
}
/** Policy-facing authoritative settlement, kept separate from diagnostic events. */
export interface ActorActionFeedback<SessionID> extends TurnIdentity<SessionID> {
	readonly action?: ActionKey;
	readonly settlement: ActorActionSettlement;
	/** Frozen candidate attribution supplied without routing policy through the event sink. */
	readonly candidate?: CandidateEventDescriptor;
	/** Opaque producer-owned feedback of the execution owner, including cross-turn cache adoption. */
	readonly candidateFeedback?: unknown;
	readonly computation?: ToolComputationTiming;
}
/** Policy-facing prediction outcome with the tool context omitted from generic settlement identity. */
export interface PredictionFeedback<SessionID> extends TurnIdentity<SessionID> {
	readonly tool: string;
	readonly action?: ActionKey;
	readonly settlement: PredictionSettlement;
}
/** The concrete Actor action represented by an `actor_adopted` continuation output. */
export type AdoptedAction = Required<Pick<SpeculativeCandidate, "key" | "input">>;
export interface TurnInput<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly terminal?: boolean;
}
type MaybePromise<T> = T | Promise<T>;
export interface RuntimeTurnContext<StartInput, StateData> {
	readonly startInput: StartInput;
	readonly data: StateData;
	readonly settings: SpeculativeActionSettings;
}
interface BoundCandidateCall {
	readonly candidate: SpeculativeDraftCandidate;
	readonly tool: string;
	readonly concrete: Record<string, unknown>;
	readonly action: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly callID: string;
	readonly index: number;
	readonly signal: AbortSignal;
}
/** An independently cancellable continuation that reads only these parent observations. */
export interface PlanContinuation {
	readonly actionIDs: readonly string[];
	readonly feedback: unknown;
}
export interface SpeculativePlanSource<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	StateData,
> {
	readonly id: string;
	/** Preparation reuses plan admission without claiming an Agent tool forecast. */
	readonly kind?: "prediction" | "preparation";
	readonly enabled: (settings: SpeculativeActionSettings) => boolean;
	readonly timeoutMs?: (settings: SpeculativeActionSettings) => number | undefined;
	/** Stop requests that can only predict the next Actor decision once that decision arrives. */
	readonly requestLifetime?: "actor_decision" | "turn";
	readonly multiStepEnabled?: (settings: SpeculativeActionSettings, feedback?: unknown) => boolean;
	readonly proposalCount?: (settings: SpeculativeActionSettings) => number;
	/** Admission policy for concurrent initial proposals targeting one Actor decision. */
	readonly concurrentProposalPolicy?: (settings: SpeculativeActionSettings) => "all" | "first_produced";
	readonly propose: (
		input: RuntimeTurnContext<StartInput, StateData> & {
			readonly definitions: readonly DrafterToolDefinition[];
			readonly proposalIndex: number;
			readonly proposalCount: number;
			readonly signal: AbortSignal;
			/** Provider tokens this request spent, reported whatever it returns; later reports still count. */
			readonly reportDraftTokens?: (tokens: number) => void;
		},
	) => MaybePromise<PlanProposal | readonly PlanProposal[] | undefined>;
	readonly continue?: (
		input: RuntimeTurnContext<StartInput, StateData> &
			PlanActionFeedback & {
				readonly candidate: SpeculativeCandidate;
				readonly adoptedAction?: AdoptedAction;
				readonly revision: number;
				/** Reserve at completion when independent branches can finish out of order. */
				readonly reserveRevision?: () => number;
				readonly output: Output | undefined;
				readonly trigger: "prepared" | "execution_succeeded" | "actor_adopted";
				readonly signal: AbortSignal;
				readonly reportDraftTokens?: (tokens: number) => void;
			},
	) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	/** Declare the original ordered batch; peers receive it only after every root execution succeeds. */
	readonly continuationBatch?: (input: PlanActionFeedback) => readonly string[] | undefined;
	/** Predict from another source's hypothetical batch without recording Actor observations. */
	readonly continueFrom?: (
		input: RuntimeTurnContext<StartInput, StateData> & {
			readonly batch: readonly {
				readonly identity: PlanActionIdentity;
				readonly candidate: SpeculativeCandidate;
				readonly output: Output;
			}[];
			readonly signal: AbortSignal;
			readonly reportDraftTokens?: (tokens: number) => void;
		},
	) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	/** Collect/filter execution feedback before a continuation consumes request capacity. */
	readonly continueOn?:
		| readonly ("prepared" | "execution_succeeded" | "actor_adopted")[]
		| ((input: {
				readonly actionID: string;
				readonly action: ActionKey;
				readonly feedback: unknown;
				readonly output: Output | undefined;
				readonly trigger: "prepared" | "execution_succeeded" | "actor_adopted";
		  }) => boolean | readonly PlanContinuation[]);
}
interface PlanActionFeedback {
	readonly proposalID: string;
	readonly actionID: string;
	readonly feedback: unknown;
}
/** Authoritative observations are available to learners even while every prediction source is off. */
export interface ActorObservation<SessionID, Output, StartInput, ConsumeInput, StateData>
	extends ActorActionFeedback<SessionID>, RuntimeTurnContext<StartInput, StateData> {
	readonly consumeInput: ConsumeInput;
	readonly tool: string;
	readonly concrete: Record<string, unknown>;
	readonly output?: Output;
	readonly operations?: readonly ExecutionOperationBinding[];
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
	readonly prediction_admitted: PlanActionFeedback & {
		readonly source: string;
		readonly issued: boolean;
	};
	readonly prediction_settled: PredictionFeedback<SessionID>;
	/** Delivered after claimed work is consumed or released, independently of source enablement. */
	readonly prediction_feedback: PlanActionFeedback & {
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
export interface SpeculativeActionRuntimeAdapter<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	ConsumeInput extends TurnInput<SessionID>,
	StateData,
> {
	readonly actionSemantics?: ActionSemanticsRegistry;
	/** Optional host resource observations; absent for deterministic runtimes without host telemetry. */
	readonly resources?: import("./system-resources.ts").ExecutionResourceMonitor;
	readonly sources?: readonly SpeculativePlanSource<SessionID, Output, StartInput, StateData>[];
	/** Canonical model-visible observation, used only to verify private prediction ancestry. */
	readonly observationKey?: (output: Output, action?: ActionKey) => string | undefined;
	readonly settings: () => MaybePromise<SpeculativeActionSettings>;
	readonly definitions: (input: StartInput) => readonly DrafterToolDefinition[];
	readonly stateData: (input: StartInput) => MaybePromise<StateData>;
	readonly actionKey: (
		tool: string,
		input: unknown,
		context:
			| {
					readonly type: "start";
					readonly startInput: StartInput;
					readonly data: StateData;
					readonly operation?: ExecutionOperationBinding;
			  }
			| {
					readonly type: "consume";
					readonly consumeInput: ConsumeInput;
			  },
	) => MaybePromise<ActionKey | undefined>;
	/** Resolve the highest-priority safe execution capability for this attempt. */
	readonly resolveExecution: (
		input: RuntimeTurnContext<StartInput, StateData> &
			Omit<BoundCandidateCall, "route" | "callID" | "index"> & {
				readonly parentWorld?: WorldBranch<Output>;
			},
	) => MaybePromise<SpeculativeExecutionRoute | undefined>;
	/** Snapshot freshness before fallback execution; this callback must never execute the tool. */
	readonly captureAuthoritativeResult?: (
		input: RuntimeTurnContext<StartInput, StateData> &
			Omit<BoundCandidateCall, "candidate" | "route" | "index" | "action"> & {
				readonly action?: ActionKey;
				readonly consumeInput: ConsumeInput;
			},
	) => MaybePromise<AuthoritativeResultCapture<Output> | undefined>;
	readonly actual: (input: ConsumeInput) => ActualToolCall;
	readonly preflightCandidate: (
		input: RuntimeTurnContext<StartInput, StateData> & BoundCandidateCall,
	) => MaybePromise<CandidatePreflight>;
	readonly authorizeCandidate?: (
		input: Pick<BoundCandidateCall, "tool" | "concrete" | "action" | "route"> & {
			readonly stateData: StateData;
			readonly consumeInput: ConsumeInput;
			readonly settings: SpeculativeActionSettings;
			readonly candidate: SpeculativeCandidate;
			readonly signal?: AbortSignal;
		},
	) => MaybePromise<CandidatePreflight>;
	readonly executeCandidate: (
		input: Omit<RuntimeTurnContext<StartInput, StateData>, "settings"> &
			BoundCandidateCall &
			ExecutionResourceContext & {
				readonly parentWorld?: WorldBranch<Output>;
			},
	) => MaybePromise<WorldBranch<Output>>;
	readonly projectionRules?: readonly ActionProjectionRule<Output>[];
	readonly rejectCandidateOutput?: (input: {
		readonly output: Output;
		readonly candidate: SpeculativeCandidate;
	}) => string | undefined;
	readonly onEvent?: (event: SpeculativeActionEvent<SessionID>) => MaybePromise<void>;
}
/** One execution owns its reuse result and exactly-once fallback settlement, independent of caller IDs. */
export interface PreparedActorCall<Output> {
	readonly output?: Output;
	readonly observeOperations?: boolean;
	readonly reuseEnabled?: boolean;
	/** Internal Actor operations borrow the same input owners as predicted tool calls. */
	readonly withInputs?: <Value>(
		execute: (inputs?: (path?: string) => Iterable<object>) => Promise<Value>,
	) => Promise<Value>;
	readonly settle: (
		toolExecution: TimelineInterval,
		output?: Output,
		operations?: readonly ExecutionOperationBinding[],
	) => Promise<void>;
}
export interface SpeculativeActionRuntime<
	SessionID,
	Output,
	StartInput,
	ConsumeInput,
	FinishInput,
	StateData = unknown,
> {
	/** Subscribe independently of proposal sources; unsubscribe prevents future delivery. Failures are isolated. */
	readonly subscribe: (
		handlers: RuntimeEventHandlers<RuntimeEventMap<SessionID, Output, StartInput, ConsumeInput, StateData>>,
	) => () => void;
	/** Explicit proposal command. Late or disabled producers cannot publish into a completed decision. */
	readonly submitPlan: (
		turn: StartInput,
		updates: PlanUpdate | readonly PlanUpdate[] | undefined,
	) => Promise<void>;
	readonly reservePlanRevision: (sessionID: SessionID, proposalID: string, minimum: number) => number;
	/** Measure the complete authoritative call against its originating task, including errors and fallback. */
	readonly trackActorTool: <Value>(
		sessionID: SessionID,
		execute: (issuedAt: number) => Promise<Value>,
	) => Promise<Value>;
	readonly startTurn: (input: StartInput, signal?: AbortSignal) => Promise<void>;
	/** Streamed Actor tool identity: prioritize complete predictions for that tool without matching them. */
	readonly previewActorTool: (
		input: TurnIdentity<SessionID> & {
			readonly tool: string;
		},
		signal?: AbortSignal,
	) => Promise<void>;
	/** Complete streamed Actor intent: prioritize matching work or start an isolated preview; never commit it. */
	readonly previewActorCall: (input: ConsumeInput, signal?: AbortSignal) => Promise<void>;
	/** Use the original trackActorTool arrival when preparation follows an asynchronous boundary. */
	readonly prepareActorCall: (
		input: ConsumeInput,
		signal?: AbortSignal,
		issuedAt?: number,
	) => Promise<PreparedActorCall<Output> | undefined>;
	readonly finishTurn: (input: FinishInput) => Promise<void>;
	/** Refresh through the adapter unless the caller supplies a complete runtime policy. */
	readonly settingsChanged: (settings?: SpeculativeActionSettings) => Promise<void>;
	readonly disposeSession: (sessionID: SessionID) => Promise<void>;
	readonly dispose: () => Promise<void>;
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
	readonly origin: "prediction" | "actor_preview" | "actor_result";
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
				readonly type: "operation_prediction";
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
export type PlanActionDependencyCondition =
	"execution_settled" | "execution_succeeded" | "actor_adopted" | "prediction" | "resource";
/** A scheduler-visible edge. The producer may add actions in later deltas. */
export interface PlanActionDependency {
	readonly actionID: string;
	/** Cross-plan edges must pin the parent's immutable identity as well as its proposal. */
	readonly proposalID?: string;
	readonly identity?: string;
	/** Resource edges retain execution ancestry without making its observations prediction inputs. */
	readonly condition?: PlanActionDependencyCondition;
	/** Private forecast of the parent's model-visible result; never execution or reuse evidence. */
	readonly observation?: string;
}
export interface PlanAction {
	/** Stable within one proposal across revisions. */
	readonly id: string;
	readonly type: "tool_call" | "operation";
	/** Source-owned diagnostic label; never part of action identity, scheduling or reuse authority. */
	readonly mode?: string;
	/** Lightweight policy token retained with consumed computation; omit tool inputs, outputs and model context. */
	readonly reuseFeedback?: unknown;
	/** Internal work retains its backend capability and uses tool/input only as the permission anchor. */
	readonly operation?: ExecutionOperationBinding;
	readonly tool: string;
	readonly input: unknown;
	/** Expected Actor tool batches before this action; used to schedule execution. */
	readonly horizon?: number;
	/** Latest Actor tool batches before this prediction becomes a miss. Defaults to horizon. */
	readonly latestHorizon?: number;
	readonly empiricalProbability?: number;
	/** Chance prepared work is consumed, including shared inputs; independent of prediction precision. */
	readonly adoptionProbability?: number;
	readonly conditionalProbability?: number;
	readonly confidence?: number;
	/** Explicit duration for this execution, when supplied by its executor; never extrapolated from historical runs. */
	readonly expectedDurationMs?: number;
	/** Uses only otherwise-idle speculative capacity and yields first under contention. */
	readonly background?: boolean;
	/** Run for the internal operations it leaves later calls as much as for its own result, which an Actor call may still adopt. */
	readonly producesOperations?: true;
	readonly resourceDemand?: number | HardwareResources;
	readonly depth?: number;
	readonly dependsOn?: readonly PlanActionDependency[];
	/** Opaque producer-owned state. The runtime only returns it in feedback/continuation calls. */
	readonly feedback?: unknown;
}
export interface PlanProposal {
	/** Stable producer-chosen identity for the whole evolving plan. */
	readonly id: string;
	/** Opaque source identity; cache and action equivalence never depend on it. */
	readonly source: string;
	readonly revision: number;
	readonly actions: readonly PlanAction[];
	/** Retain descendants of unchanged actions. Omit to replace the whole plan. */
	readonly retainDescendants?: true;
}
/** Incremental update to a proposal. Revisions must be strictly increasing. */
export interface PlanDelta {
	readonly proposalID: string;
	readonly source: string;
	readonly revision: number;
	readonly upsert?: readonly PlanAction[];
	readonly remove?: readonly string[];
}
export type PlanUpdate = PlanProposal | PlanDelta;
export type MaterializedPlan = Omit<PlanProposal, "retainDescendants">;
/** A deliberate source admission decision; it produced no proposal and is not a provider failure. */
export class SourceRequestSuppressed extends Error {
	readonly cause: ResolutionCause & {
		readonly stage: "source";
	};
	constructor(cause: SourceRequestSuppressed["cause"]) {
		super(cause.detail ?? cause.code);
		this.cause = Object.freeze({ ...cause });
	}
}
/** Turn-scoped authority token. Expiration prevents late producer results from entering admission. */
export class SourceGeneration {
	private readonly controller = new AbortController();
	readonly signal: AbortSignal = this.controller.signal;
	constructor(parent?: AbortSignal) {
		if (!parent) return;
		const abort = () => this.expire(cause("control", "turn_aborted"));
		if (parent.aborted) abort();
		else
			parent.addEventListener("abort", abort, {
				once: true,
				signal: this.signal,
			});
	}
	get active(): boolean {
		return !this.signal.aborted;
	}
	get expiration(): ResolutionCause | undefined {
		return this.signal.reason as ResolutionCause | undefined;
	}
	expire(expiration: ResolutionCause): void {
		if (this.signal.aborted) return;
		this.controller.abort(Object.freeze({ ...expiration }));
	}
}
/** Producer timing/cancellation; the caller retains physical production until close, separately from admission. */
export async function runSourceRequest<Value>(input: {
	readonly request: SourceRequestIdentity;
	readonly generation: SourceGeneration;
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
			cause: cause(
				"source",
				input.generation.expiration?.code ?? "generation_expired",
				input.generation.expiration?.detail,
			),
		});
	const failed = (code: string, error: unknown) =>
		finish({
			status: "error",
			cause: cause("source", code, errorDetail(error)),
		});
	if (!input.generation.active) return aborted();
	const controller = new AbortController();
	// Produced proposals can leave preparation in flight until their generation closes.
	const signal = AbortSignal.any([input.generation.signal, controller.signal]);
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
		input.generation.signal,
		nonNegativeNumber(input.timeoutMs, undefined),
	);
	if (waited.status === "deadline") {
		const expiration = cause("source", "timeout");
		controller.abort(expiration);
		return finish({ status: "timeout", cause: expiration });
	}
	if (waited.status === "aborted" || !input.generation.active) return aborted();
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
/**
 * Failure-isolated, bounded delivery for optional observers.
 *
 * Optional observers use a fixed capacity so stalled delivery cannot retain an unbounded backlog.
 * PostSettlementQueue shares the drain machinery with a lossless capacity for source learning.
 */
export class BoundedEventQueue<Event> {
	private readonly capacityValue: number;
	private readonly deliver: (event: Event) => void | Promise<void>;
	private readonly pending = new Set<{
		readonly value: Event;
	}>();
	private idle?: PromiseWithResolvers<void>;
	private closed = false;
	constructor(capacity: number, deliver: (event: Event) => void | Promise<void>) {
		if (!isSafeCount(capacity, 1)) throw new Error("event queue capacity must be positive");
		this.capacityValue = capacity;
		this.deliver = deliver;
	}
	enqueue(event: Event): boolean {
		if (this.closed) return false;
		if (this.pending.size >= this.capacityValue) return false;
		this.pending.add({ value: event });
		if (this.pending.size === 1) void this.drain();
		return true;
	}
	async flush(): Promise<void> {
		if (this.pending.size) await (this.idle ??= Promise.withResolvers<void>()).promise;
	}
	/** Seal delivery; callers may detach from an already bounded backlog during runtime disposal. */
	async close(
		options: {
			readonly drain?: boolean;
		} = {},
	): Promise<void> {
		this.closed = true;
		if (options.drain !== false) await this.flush();
	}
	private async drain(): Promise<void> {
		try {
			// A Set retains insertion order while admitting reentrant work without shifting a backlog.
			for (const next of this.pending) {
				try {
					await this.deliver(next.value);
				} catch {
					/* Optional observers cannot poison later delivery. */
				}
				this.pending.delete(next);
			}
		} finally {
			this.idle?.resolve();
			this.idle = undefined;
		}
	}
}
/** Ordered, failure-isolated work that must never extend the Actor settlement barrier. */
export class PostSettlementQueue extends BoundedEventQueue<() => void | Promise<void>> {
	constructor() {
		// Learning is lossless; defer invocation so enqueue never enters a producer callback.
		super(Number.MAX_SAFE_INTEGER, (task) => Promise.resolve().then(task));
	}
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
	has(type: keyof Events): boolean {
		return [...this.listeners].some((handlers) => handlers[type]);
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
			readonly status: "queued";
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
		status: "queued",
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
		if (this.executionValue.status !== "queued") return false;
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
			(this.executionValue.status !== "queued" || settlement.status === "succeeded")
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
export interface ActorCandidateSelection<
	Candidate extends {
		readonly id: string;
	},
	Output,
> {
	readonly candidate: Candidate;
	readonly match: ActionKeyMatch;
	readonly output: Output;
	readonly toolExecution: TimelineInterval;
	readonly projection?: TimelineInterval;
	/** This projection already existed before the current Actor call; a fresh reconstruction has no receipt. */
	readonly projectionReused?: true;
}
type ActorActionState<
	Candidate extends {
		readonly id: string;
	},
	Output,
> =
	| {
			readonly status: "matching";
	  }
	| {
			readonly status: "selected";
			readonly value: ActorCandidateSelection<Candidate, Output>;
	  }
	| {
			readonly status: "awaiting_fallback";
	  }
	| {
			readonly status: "settled";
			readonly value: ActorActionSettlement;
	  };
/** One owner for matching, the committed selection and exactly-once settlement. */
export class ActorAction<
	Candidate extends {
		readonly id: string;
	} = {
		readonly id: string;
	},
	Output = unknown,
> {
	readonly identity: ActorActionIdentity;
	readonly issuedAt: number;
	readonly tool: string;
	readonly actionKey?: ActionKey;
	private readonly rejections: CandidateRejection[] = [];
	private stateValue: ActorActionState<Candidate, Output> = Object.freeze({
		status: "matching",
	});
	private fallbackValue: {
		readonly cause: ResolutionCause;
		readonly candidateID?: string;
	};
	private resultCapture?: AuthoritativeResultCapture<Output>;
	private matchedPredictions: readonly PredictionIdentity[] = [];
	finishConsumption!: (reused?: ReadonlySet<string>) => void;
	readonly consumption = new Promise<ReadonlySet<string>>((resolve) => {
		this.finishConsumption = (reused = new Set()) => resolve(reused);
	});
	constructor(
		input: Pick<ActorAction<Candidate, Output>, "identity" | "issuedAt" | "tool" | "actionKey"> & {
			readonly fallback: ResolutionCause;
		},
	) {
		this.identity = Object.freeze({ ...input.identity });
		this.issuedAt = input.issuedAt;
		this.tool = input.tool;
		this.actionKey = input.actionKey;
		this.fallbackValue = Object.freeze({ cause: input.fallback });
	}
	get state(): ActorActionState<Candidate, Output> {
		return this.stateValue;
	}
	get settlement(): ActorActionSettlement | undefined {
		return this.stateValue.status === "settled" ? this.stateValue.value : undefined;
	}
	get fallback() {
		return this.fallbackValue;
	}
	get selection() {
		return this.stateValue.status === "selected" ? this.stateValue.value : undefined;
	}
	get inputSource(): object | undefined {
		return this.stateValue.status === "awaiting_fallback" ? this.resultCapture?.inputSource : undefined;
	}
	capture(capture: AuthoritativeResultCapture<Output>): boolean {
		if (this.stateValue.status !== "awaiting_fallback" || this.resultCapture) return false;
		this.resultCapture = capture;
		return true;
	}
	takeCapture(): AuthoritativeResultCapture<Output> | undefined {
		const capture = this.resultCapture;
		this.resultCapture = undefined;
		return capture;
	}
	setFallback(failure: ResolutionCause, candidateID?: string): void {
		if (this.stateValue.status !== "matching") return;
		this.fallbackValue = Object.freeze({
			cause: failure,
			...(candidateID ? { candidateID } : {}),
		});
	}
	rejectCandidate(candidateID: string, match: ActionKeyMatch, failure: ResolutionCause): void {
		if (this.stateValue.status !== "matching") return;
		this.rejections.push(
			Object.freeze({
				candidateID,
				match: Object.freeze({ ...match }),
				cause: Object.freeze({ ...failure }),
			}),
		);
		this.setFallback(failure, candidateID);
	}
	select(selection: ActorCandidateSelection<Candidate, Output>): void {
		if (this.stateValue.status !== "matching") return;
		this.stateValue = Object.freeze({
			status: "selected",
			value: Object.freeze({ ...selection }),
		});
	}
	settleSelection(provider: "speculative" | "preview"): void {
		if (this.stateValue.status !== "selected") return;
		const selected = this.stateValue.value;
		const candidateID = selected.candidate.id;
		const toolExecution = TimelineInterval.from(selected.toolExecution);
		this.finish(
			Object.freeze<ActorActionProvider>({
				candidateID,
				toolExecution,
				...(provider === "preview"
					? {
							kind: "actor",
							origin: "preview",
							durationMs: toolExecution.completedAt - toolExecution.startedAt,
							isError: false,
						}
					: {
							kind: "speculative",
							match: Object.freeze({ ...selected.match }),
						}),
			}),
		);
	}
	match(predictions: readonly PredictionIdentity[], settled: (adoption: PredictionAdoption) => void): void {
		this.matchedPredictions = Object.freeze([
			...new Map(predictions.map((prediction) => [prediction.id, Object.freeze({ ...prediction })])).values(),
		]);
		void this.consumption.then(() => {
			const provider = this.settlement?.provider;
			settled(
				provider?.kind === "speculative"
					? { status: "adopted", candidateID: provider.candidateID }
					: {
							status: "rejected",
							...(provider?.origin === "preview"
								? {
										candidateID: provider.candidateID,
										cause: cause("control", "actor_preview_provider"),
									}
								: this.fallbackValue),
						},
			);
		});
	}
	deferToFallback(): void {
		if (this.stateValue.status === "matching")
			this.stateValue = Object.freeze({ status: "awaiting_fallback" });
	}
	settleActor(execution: TimelineInterval, isError: boolean): ActorActionSettlement | undefined {
		if (this.stateValue.status !== "awaiting_fallback") return undefined;
		const toolExecution = TimelineInterval.from(execution);
		return this.finish(
			Object.freeze({
				kind: "actor",
				origin: "fallback",
				...this.fallbackValue,
				durationMs: toolExecution.completedAt - toolExecution.startedAt,
				isError,
				toolExecution,
			}),
		);
	}
	private finish(provider: ActorActionProvider): ActorActionSettlement {
		const settlement = Object.freeze({
			actorAction: this.identity,
			tool: this.tool,
			provider,
			matchedPredictions: this.matchedPredictions,
			...(this.actionKey ? { actionKeyHash: this.actionKey.hash } : {}),
			rejections: Object.freeze([...this.rejections]),
		});
		this.stateValue = Object.freeze({ status: "settled", value: settlement });
		return settlement;
	}
}
export type ActorStreamPreview =
	| {
			readonly type: "tool";
			readonly tool: string;
	  }
	| {
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
	preview: "none" | "tool";
};
/** Derives non-authoritative scheduling hints from the same Actor stream in every host integration. */
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
			preview: "none",
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
		if (state.preview === "none" && call.name) {
			state.preview = "tool";
			result.push({ type: "tool", tool: call.name });
		}
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
