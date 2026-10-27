import type { ActionProjectionRule } from "./action-key-projection.ts";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import type { DrafterToolDefinition } from "./common.ts";
import type { CandidateEventDescriptor, SpeculativeActionEvent } from "./events.ts";
import type {
	CapturedExecutionWorldResult, ExecutionResourceContext, ExecutionOperationBinding, SpeculativeExecutionRoute, WorldBranch, WorldResultCapture,
} from "./execution-world.ts";
import type { PlanAction, PlanProposal, PlanUpdate } from "./plan-proposal.ts";
import type { ActorActionIdentity, ActorActionSettlement, PlanActionIdentity, PredictionSettlement, SettledSourceRequest } from "./settlement.ts";
import type { TimelineInterval, ToolComputationTiming } from "./task-timing.ts";
import type { RuntimeEventHandlers } from "./post-settlement.ts";

export type { SpeculativeActionEvent, SpeculativeCacheSnapshot } from "./events.ts";

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
	readonly predictionTimeoutMs: number;
	readonly scheduling?: Partial<import("./scheduling-settings.ts").SchedulingSettings>;
	/** Source-owned configuration. The runtime treats every value as opaque. */
	readonly sourceConfig?: Readonly<Record<string, unknown>>;
}

export interface SpeculativeDraftCandidate extends Omit<PlanAction, "id" | "background"> {
	readonly source?: string;
	readonly proposalID?: string;
	readonly actionID?: string;
}

export type CandidatePreflight =
	| { readonly ok: true }
	| { readonly ok: false; readonly reason: string; readonly detail?: string };

/** Read-only execution view passed to host callbacks. Prediction facts are supplied separately. */
export interface SpeculativeCandidate extends Pick<PlanAction, "tool" | "empiricalProbability" | "adoptionProbability" | "conditionalProbability" | "depth"> {
	readonly id: string;
	readonly key?: ActionKey;
	readonly input: Readonly<Record<string, unknown>>;
	/** Elapsed execution time once settled; runtime work and reservation state stay private. */
	readonly executionMs?: number;
	readonly source?: string;
	readonly planDependencies?: PlanAction["dependsOn"];
}

/** A pre-Actor freshness baseline that can promote the Actor's own result into the shared cache. */
export interface AuthoritativeResultCapture<Output> extends WorldResultCapture<Output>, Omit<CapturedExecutionWorldResult<Output>, "capture"> {}

/** A validated, concrete prediction suitable for a target-model draft verifier. */
export interface MaterializedSpeculativeCandidate<SessionID> extends Pick<PlanAction,
	"tool" | "depth" | "horizon" | "conditionalProbability" | "empiricalProbability" | "adoptionProbability" | "expectedDurationMs">, TurnIdentity<SessionID> {
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
export interface MaterializedActorAction<SessionID> extends TurnIdentity<SessionID>, Pick<ActionKey, "tool" | "input"> {
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

export interface TurnInput<SessionID> { readonly sessionID: SessionID; readonly turnID: string; readonly terminal?: boolean; }

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
	readonly propose: (input: RuntimeTurnContext<StartInput, StateData> & {
		readonly definitions: readonly DrafterToolDefinition[];
		readonly proposalIndex: number;
		readonly proposalCount: number;
		readonly signal: AbortSignal;
		/** Provider tokens this request spent, reported whatever it returns; later reports still count. */
		readonly reportDraftTokens?: (tokens: number) => void;
	}) => MaybePromise<PlanProposal | readonly PlanProposal[] | undefined>;
	readonly continue?: (input: RuntimeTurnContext<StartInput, StateData> & PlanActionFeedback & {
		readonly candidate: SpeculativeCandidate;
		readonly adoptedAction?: AdoptedAction;
		readonly revision: number;
		/** Reserve at completion when independent branches can finish out of order. */
		readonly reserveRevision?: () => number;
		readonly output: Output | undefined;
		readonly trigger: "prepared" | "execution_succeeded" | "actor_adopted";
		readonly signal: AbortSignal;
		readonly reportDraftTokens?: (tokens: number) => void;
	}) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	/** Declare the original ordered batch; peers receive it only after every root execution succeeds. */
	readonly continuationBatch?: (input: PlanActionFeedback) => readonly string[] | undefined;
	/** Predict from another source's hypothetical batch without recording Actor observations. */
	readonly continueFrom?: (input: RuntimeTurnContext<StartInput, StateData> & {
		readonly batch: readonly {
			readonly identity: PlanActionIdentity;
			readonly candidate: SpeculativeCandidate;
			readonly output: Output;
		}[];
		readonly signal: AbortSignal;
		readonly reportDraftTokens?: (tokens: number) => void;
	}) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	/** Collect/filter execution feedback before a continuation consumes request capacity. */
	readonly continueOn?: readonly ("prepared" | "execution_succeeded" | "actor_adopted")[] | ((input: {
		readonly actionID: string;
		readonly action: ActionKey;
		readonly feedback: unknown;
		readonly output: Output | undefined;
		readonly trigger: "prepared" | "execution_succeeded" | "actor_adopted";
	}) => boolean | readonly PlanContinuation[]);
}

interface PlanActionFeedback { readonly proposalID: string; readonly actionID: string; readonly feedback: unknown; }

/** Authoritative observations are available to learners even while every prediction source is off. */
export interface ActorObservation<SessionID, Output, StartInput, ConsumeInput, StateData> extends ActorActionFeedback<SessionID>, RuntimeTurnContext<StartInput, StateData> {
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
		readonly decisionSequence: number; readonly definitions: readonly DrafterToolDefinition[]; readonly signal?: AbortSignal;
	};
	readonly turn_finished: Pick<RuntimeTurnContext<StartInput, StateData>, "startInput" | "settings"> & { readonly terminal: boolean };
	readonly session_closed: { readonly sessionID: SessionID; readonly terminal: boolean };
	readonly candidate_materialized: MaterializedSpeculativeCandidate<SessionID>;
	readonly actor_materialized: MaterializedActorAction<SessionID>;
	readonly actor_settled: ActorActionFeedback<SessionID>;
	readonly actor_observed: ActorObservation<SessionID, Output, StartInput, ConsumeInput, StateData>;
	readonly prediction_admitted: PlanActionFeedback & { readonly source: string; readonly issued: boolean };
	readonly prediction_settled: PredictionFeedback<SessionID>;
	/** Delivered after claimed work is consumed or released, independently of source enablement. */
	readonly prediction_feedback: PlanActionFeedback & { readonly settlement: PredictionSettlement; readonly served: boolean };
	/** Once per physical execution; shared predictions do not incur another production cost. */
	readonly execution_settled: TurnIdentity<SessionID> & {
		readonly candidate: CandidateEventDescriptor; readonly reuseFeedback: unknown;
		readonly status: "succeeded" | "failed" | "cancelled"; readonly executionMs: number;
	};
	readonly source_request: TurnIdentity<SessionID> & { readonly request: SettledSourceRequest };
}

export interface ActualToolCall { readonly id?: string; readonly tool: string; readonly input: unknown; }

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
			| { readonly type: "start"; readonly startInput: StartInput; readonly data: StateData; readonly operation?: ExecutionOperationBinding }
			| { readonly type: "consume"; readonly consumeInput: ConsumeInput },
	) => MaybePromise<ActionKey | undefined>;
	/** Resolve the highest-priority safe execution capability for this attempt. */
	readonly resolveExecution: (input: RuntimeTurnContext<StartInput, StateData> &
		Omit<BoundCandidateCall, "route" | "callID" | "index"> & { readonly parentWorld?: WorldBranch<Output> }) => MaybePromise<SpeculativeExecutionRoute | undefined>;
	/** Snapshot freshness before fallback execution; this callback must never execute the tool. */
	readonly captureAuthoritativeResult?: (input: RuntimeTurnContext<StartInput, StateData> &
		Omit<BoundCandidateCall, "candidate" | "route" | "index" | "action"> & {
		readonly action?: ActionKey;
		readonly consumeInput: ConsumeInput;
	}) => MaybePromise<AuthoritativeResultCapture<Output> | undefined>;
	readonly actual: (input: ConsumeInput) => ActualToolCall;
	readonly preflightCandidate: (input: RuntimeTurnContext<StartInput, StateData> & BoundCandidateCall) => MaybePromise<CandidatePreflight>;
	readonly authorizeCandidate?: (input: Pick<BoundCandidateCall, "tool" | "concrete" | "action" | "route"> & {
		readonly stateData: StateData;
		readonly consumeInput: ConsumeInput;
		readonly settings: SpeculativeActionSettings;
		readonly candidate: SpeculativeCandidate;
		readonly signal?: AbortSignal;
	}) => MaybePromise<CandidatePreflight>;
	readonly executeCandidate: (input: Omit<RuntimeTurnContext<StartInput, StateData>, "settings"> & BoundCandidateCall & ExecutionResourceContext & { readonly parentWorld?: WorldBranch<Output> }) => MaybePromise<WorldBranch<Output>>;
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
	readonly withInputs?: <Value>(execute: (inputs?: (path?: string) => Iterable<object>) => Promise<Value>) => Promise<Value>;
	readonly settle: (toolExecution: TimelineInterval, output?: Output, operations?: readonly ExecutionOperationBinding[]) => Promise<void>;
}

export interface SpeculativeActionRuntime<SessionID, Output, StartInput, ConsumeInput, FinishInput, StateData = unknown> {
	/** Subscribe independently of proposal sources; unsubscribe prevents future delivery. Failures are isolated. */
	readonly subscribe: (handlers: RuntimeEventHandlers<RuntimeEventMap<SessionID, Output, StartInput, ConsumeInput, StateData>>) => () => void;
	/** Explicit proposal command. Late or disabled producers cannot publish into a completed decision. */
	readonly submitPlan: (turn: StartInput, updates: PlanUpdate | readonly PlanUpdate[] | undefined) => Promise<void>;
	readonly reservePlanRevision: (sessionID: SessionID, proposalID: string, minimum: number) => number;
	/** Measure the complete authoritative call against its originating task, including errors and fallback. */
	readonly trackActorTool: <Value>(sessionID: SessionID, execute: (issuedAt: number) => Promise<Value>) => Promise<Value>;
	readonly startTurn: (input: StartInput, signal?: AbortSignal) => Promise<void>;
	/** Streamed Actor tool identity: prioritize complete predictions for that tool without matching them. */
	readonly previewActorTool: (
		input: TurnIdentity<SessionID> & { readonly tool: string },
		signal?: AbortSignal,
	) => Promise<void>;
	/** Complete streamed Actor intent: prioritize matching work or start an isolated preview; never commit it. */
	readonly previewActorCall: (input: ConsumeInput, signal?: AbortSignal) => Promise<void>;
	/** Use the original trackActorTool arrival when preparation follows an asynchronous boundary. */
	readonly prepareActorCall: (input: ConsumeInput, signal?: AbortSignal, issuedAt?: number) => Promise<PreparedActorCall<Output> | undefined>;
	readonly finishTurn: (input: FinishInput) => Promise<void>;
	/** Refresh through the adapter unless the caller supplies a complete runtime policy. */
	readonly settingsChanged: (settings?: SpeculativeActionSettings) => Promise<void>;
	readonly disposeSession: (sessionID: SessionID) => Promise<void>;
	readonly dispose: () => Promise<void>;
}
