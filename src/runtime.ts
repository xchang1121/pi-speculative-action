import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionKey, type ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type { DrafterToolDefinition } from "./common.ts";
import type { CandidateEventDescriptor, SpeculativeActionEvent } from "./events.ts";
import type { SpeculativeExecutionRoute, WorldBranch, WorldResultCapture } from "./execution-world.ts";
import type { PlanAction, PlanProposal, PlanUpdate } from "./plan-proposal.ts";
import { makeStructuralSpeculativeActionRuntime } from "./runtime-engine.ts";
import type { ActorActionIdentity, ActorActionSettlement, PredictionSettlement } from "./settlement.ts";

export { diagnosticAction, diagnosticJson, redactDiagnostics } from "./diagnostics.ts";

export type {
	CandidateEventDescriptor,
	CandidateExecutionProjection,
	SpeculativeActionEvent,
	SpeculativeCacheSnapshot,
} from "./events.ts";

export interface SpeculativeActionSettings {
	readonly enabled: boolean;
	readonly drafterEnabled?: boolean;
	readonly candidateLimit?: number;
	readonly maxConcurrentActions?: number;
	readonly resourceCacheMaxEntries: number;
	readonly resourceCacheMaxBytes?: number;
	readonly predictionTimeoutMs: number;
	/** Source-owned configuration. The runtime treats every value as opaque. */
	readonly sourceConfig?: Readonly<Record<string, unknown>>;
	/** Tools eligible for prediction; execution isolation is resolved independently. */
	readonly tools: readonly string[];
}

export interface SpeculativeDraftCandidate {
	readonly type: "tool_call";
	readonly tool: string;
	readonly input: unknown;
	readonly diagnostic?: string;
	readonly source?: string;
	readonly proposalID?: string;
	readonly actionID?: string;
	readonly feedback?: unknown;
	readonly dependsOn?: PlanAction["dependsOn"];
	readonly horizon?: number;
	readonly latestHorizon?: number;
	readonly empiricalProbability?: number;
	readonly conditionalProbability?: number;
	readonly expectedDurationMs?: number;
	readonly expectedLatencyBenefitMs?: number;
	readonly resourceDemand?: number;
	readonly depth?: number;
}

export type CandidatePreflight =
	| { readonly ok: true }
	| { readonly ok: false; readonly reason: string; readonly detail?: string };

/** Read-only execution view passed to host callbacks. Prediction facts are supplied separately. */
export interface SpeculativeCandidate {
	readonly id: string;
	readonly key: ActionKey;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly work?: { readonly execution?: { readonly executionMs?: number } };
	readonly source?: string;
	readonly empiricalProbability?: number;
	readonly conditionalProbability?: number;
	readonly depth?: number;
	readonly planDependencies?: PlanAction["dependsOn"];
}

/** A pre-Actor freshness baseline that can promote the Actor's own result into the shared cache. */
export interface AuthoritativeResultCapture<Output> extends WorldResultCapture<Output> {
	readonly route: SpeculativeExecutionRoute;
}

/** A validated, concrete prediction suitable for a target-model draft verifier. */
export interface MaterializedSpeculativeCandidate<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	/** Absolute Actor decision that this prediction is expected to match. */
	readonly expectedDecisionSequence: number;
	/** Last Actor decision for which the prediction may still be considered. */
	readonly latestDecisionSequence: number;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
	readonly tool: string;
	/** Producer-facing arguments, before K(a) canonicalization or execution projection. */
	readonly input: Readonly<Record<string, unknown>>;
	/** Exact Actor-visible K(a) represented by the prediction and target-decoder draft. */
	readonly predictedAction: ActionKey;
	/** K(a) actually scheduled; it may cover the prediction through a lossless projection. */
	readonly executionAction: ActionKey;
	readonly depth?: number;
	readonly horizon?: number;
	readonly conditionalProbability?: number;
	readonly empiricalProbability?: number;
	readonly expectedLatencyBenefitMs?: number;
	readonly expectedDurationMs?: number;
}

/** The exact K(a) Runtime assigned to an authoritative Actor tool call. */
export interface MaterializedActorAction<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly identity: ActorActionIdentity;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly action: ActionKey;
}

/** Policy-facing authoritative settlement, kept separate from diagnostic events. */
export interface ActorActionFeedback<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly action?: ActionKey;
	readonly settlement: ActorActionSettlement;
	/** Frozen candidate attribution supplied without routing policy through the event sink. */
	readonly candidate?: CandidateEventDescriptor;
}

/** Policy-facing prediction outcome with the tool context omitted from generic settlement identity. */
export interface PredictionFeedback<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly tool: string;
	readonly action?: ActionKey;
	readonly settlement: PredictionSettlement;
}

/** The concrete Actor action represented by an `actor_adopted` continuation output. */
export type AdoptedAction = Pick<SpeculativeCandidate, "key" | "input">;

interface TurnInput<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly terminal?: boolean;
}

type MaybePromise<T> = T | Promise<T>;

export interface SpeculativePlanSource<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	ConsumeInput extends TurnInput<SessionID>,
	StateData,
> {
	readonly id: string;
	readonly enabled: (settings: SpeculativeActionSettings) => boolean;
	readonly timeoutMs?: (settings: SpeculativeActionSettings) => number | undefined;
	/** Stop requests that can only predict the next Actor decision once that decision arrives. */
	readonly requestLifetime?: "actor_decision" | "turn";
	readonly multiStepEnabled?: (settings: SpeculativeActionSettings, feedback?: unknown) => boolean;
	readonly proposalCount?: (settings: SpeculativeActionSettings) => number;
	/** Admission policy for concurrent initial proposals targeting one Actor decision. */
	readonly concurrentProposalPolicy?: (settings: SpeculativeActionSettings) => "all" | "first_produced";
	readonly propose: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly definitions: readonly DrafterToolDefinition[];
		readonly candidateNames: readonly string[];
		readonly proposalIndex: number;
		readonly proposalCount: number;
		readonly signal: AbortSignal;
	}) => MaybePromise<PlanProposal | readonly PlanProposal[] | undefined>;
	readonly continue?: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly candidate: SpeculativeCandidate;
		readonly adoptedAction?: AdoptedAction;
		readonly proposalID: string;
		readonly actionID: string;
		readonly revision: number;
		readonly feedback: unknown;
		readonly output: Output;
		readonly trigger: "execution_succeeded" | "actor_adopted";
		readonly signal: AbortSignal;
	}) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	/** Restrict continuation callbacks without coupling Runtime scheduling to a concrete source. */
	readonly continueOn?: readonly ("execution_succeeded" | "actor_adopted")[];
	readonly observe?: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly consumeInput: ConsumeInput;
		readonly action?: ActionKey;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly output?: Output;
		readonly durationMs: number;
		readonly order: number;
	}) => MaybePromise<PlanUpdate | readonly PlanUpdate[] | undefined>;
	readonly onIssued?: (input: {
		readonly proposalID: string;
		readonly actionID: string;
		readonly feedback: unknown;
	}) => MaybePromise<void>;
	readonly onSettled?: (input: {
		readonly proposalID: string;
		readonly actionID: string;
		readonly feedback: unknown;
		readonly settlement: PredictionSettlement;
	}) => MaybePromise<void>;
	readonly flush?: () => MaybePromise<void>;
}

export interface SpeculativeActionRuntimeAdapter<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	ConsumeInput extends TurnInput<SessionID>,
	StateData,
> {
	readonly actionSemantics?: ActionSemanticsRegistry;
	readonly sources?: readonly SpeculativePlanSource<SessionID, Output, StartInput, ConsumeInput, StateData>[];
	readonly settings: () => MaybePromise<SpeculativeActionSettings>;
	readonly definitions: (input: StartInput) => readonly DrafterToolDefinition[];
	readonly stateData: (input: StartInput) => MaybePromise<StateData>;
	readonly actionKey: (
		tool: string,
		input: unknown,
		context:
			| { readonly type: "start"; readonly startInput: StartInput; readonly data: StateData }
			| { readonly type: "consume"; readonly consumeInput: ConsumeInput },
	) => MaybePromise<ActionKey | undefined>;
	/** Resolve the highest-priority safe execution capability for this attempt. */
	readonly resolveExecution: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly candidate: SpeculativeDraftCandidate;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly action: ActionKey;
		readonly signal: AbortSignal;
	}) => MaybePromise<SpeculativeExecutionRoute | undefined>;
	/** Snapshot freshness before fallback execution; this callback must never execute the tool. */
	readonly captureAuthoritativeResult?: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly consumeInput: ConsumeInput;
		readonly settings: SpeculativeActionSettings;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly action: ActionKey;
		readonly callID: string;
		readonly signal: AbortSignal;
	}) => MaybePromise<AuthoritativeResultCapture<Output> | undefined>;
	readonly actual: (input: ConsumeInput) => { readonly id?: string; readonly tool: string; readonly input: unknown };
	readonly preflightCandidate: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly settings: SpeculativeActionSettings;
		readonly candidate: SpeculativeDraftCandidate;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly action: ActionKey;
		readonly route: SpeculativeExecutionRoute;
		readonly callID: string;
		readonly index: number;
		readonly signal: AbortSignal;
	}) => MaybePromise<CandidatePreflight>;
	readonly authorizeCandidate?: (input: {
		readonly stateData: StateData;
		readonly consumeInput: ConsumeInput;
		readonly settings: SpeculativeActionSettings;
		readonly action: ActionKey;
		readonly route: SpeculativeExecutionRoute;
		readonly candidate: SpeculativeCandidate;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly signal?: AbortSignal;
	}) => MaybePromise<CandidatePreflight>;
	readonly executeCandidate: (input: {
		readonly startInput: StartInput;
		readonly data: StateData;
		readonly candidate: SpeculativeDraftCandidate;
		readonly tool: string;
		readonly concrete: Record<string, unknown>;
		readonly action: ActionKey;
		readonly route: SpeculativeExecutionRoute;
		readonly callID: string;
		readonly index: number;
		readonly signal: AbortSignal;
		readonly parentWorld?: WorldBranch<Output>;
	}) => MaybePromise<WorldBranch<Output>>;
	readonly projectionRules?: readonly ActionProjectionRule<Output>[];
	readonly rejectCandidateOutput?: (input: {
		readonly output: Output;
		readonly candidate: SpeculativeCandidate;
	}) => string | undefined;
	readonly onTurnStarted?: (input: {
		readonly startInput: StartInput;
		readonly decisionSequence: number;
		readonly settings: SpeculativeActionSettings;
		readonly definitions: readonly DrafterToolDefinition[];
		readonly candidateNames: readonly string[];
		readonly signal?: AbortSignal;
	}) => MaybePromise<void>;
	readonly onTurnFinished?: (input: {
		readonly startInput: StartInput;
		readonly settings: SpeculativeActionSettings;
		readonly terminal: boolean;
		readonly durationMs: number;
	}) => MaybePromise<void>;
	/** Best-effort side channel; failures cannot affect candidate admission or Actor behavior. */
	readonly onCandidateMaterialized?: (candidate: MaterializedSpeculativeCandidate<SessionID>) => MaybePromise<void>;
	/** Best-effort identity side channel; failures cannot affect Actor matching or execution. */
	readonly onActorActionMaterialized?: (action: MaterializedActorAction<SessionID>) => MaybePromise<void>;
	/** Best-effort policy feedback; failures cannot affect Actor settlement or source learning. */
	readonly onActorActionSettled?: (feedback: ActorActionFeedback<SessionID>) => MaybePromise<void>;
	/** Best-effort policy feedback; failures cannot affect prediction or source settlement. */
	readonly onPredictionSettled?: (feedback: PredictionFeedback<SessionID>) => MaybePromise<void>;
	readonly onEvent?: (event: SpeculativeActionEvent<SessionID>) => MaybePromise<void>;
}

export interface SpeculativeRuntimeInspection {
	readonly activeTurns: number;
	readonly exclusiveCandidates: number;
	readonly sharedCandidates: number;
	readonly pendingPredictions: number;
	readonly deferredPlanActions: number;
	readonly activePlanActions: number;
	readonly executionBlockedPlanActions: number;
	readonly blockedPlanActions: number;
	readonly pendingTelemetryEvents: number;
	readonly droppedTelemetryEvents: number;
	readonly oldestTelemetryEventMs: number;
}

export interface SpeculativeActionRuntime<SessionID, Output, StartInput, ConsumeInput, FinishInput> {
	readonly startTurn: (input: StartInput, signal?: AbortSignal) => Promise<void>;
	/** Streamed Actor tool identity: prioritize complete predictions for that tool without matching them. */
	readonly previewActorTool: (
		input: { readonly sessionID: SessionID; readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	) => Promise<void>;
	/** Complete streamed Actor intent: prioritize matching work or start an isolated preview; never commit it. */
	readonly previewActorCall: (input: ConsumeInput, signal?: AbortSignal) => Promise<void>;
	readonly consume: (input: ConsumeInput, signal?: AbortSignal) => Promise<Output | undefined>;
	readonly actual: (input: ConsumeInput & { readonly durationMs: number; readonly output?: Output }) => Promise<void>;
	readonly finishTurn: (input: FinishInput) => Promise<void>;
	readonly settingsChanged: (settings: SpeculativeActionSettings) => Promise<void>;
	readonly releaseSession: (sessionID: SessionID) => Promise<void>;
	readonly disposeSession: (sessionID: SessionID) => Promise<void>;
	readonly dispose: () => Promise<void>;
	readonly inspect: (sessionID?: SessionID) => SpeculativeRuntimeInspection;
}

export const makeSpeculativeActionRuntime = makeStructuralSpeculativeActionRuntime;

export function candidateToolNames(
	settings: SpeculativeActionSettings,
	semantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): readonly string[] {
	const known = new Set(semantics.toolNames());
	return [...new Set(settings.tools)].filter((tool) => known.has(tool));
}

export function candidateExecutionMs(candidate: SpeculativeCandidate): number {
	const value = candidate.work?.execution?.executionMs;
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}
