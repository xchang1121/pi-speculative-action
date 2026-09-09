import type { ActionKeyMatch } from "./action-semantics.ts";
import type { TimelineInterval } from "./task-timing.ts";

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
	readonly durationMs: number;
	readonly bytesRead: number;
	readonly filesRead: number;
	readonly mode: "watcher" | "exact";
}

export type ResourceValidation =
	| { readonly status: "valid"; readonly metrics: ValidationMetrics }
	| {
			readonly status: "stale";
			readonly cause: ResolutionCause & { readonly stage: "freshness" };
			readonly metrics: ValidationMetrics;
	  }
	| {
			readonly status: "indeterminate";
			readonly cause: ResolutionCause & { readonly stage: "freshness" };
			readonly metrics: ValidationMetrics;
	  };

export interface PlanActionIdentity {
	readonly id: string;
	readonly source: string;
	readonly proposalID: string;
	readonly actionID: string;
}

export type PredictionIdentity = PlanActionIdentity;

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

export type PredictionSettlement =
	| {
			readonly prediction: PredictionIdentity;
			readonly observation: "unobserved";
			readonly cause: ResolutionCause;
	  }
	| {
			readonly prediction: PredictionIdentity;
			readonly observation: "observed";
			readonly actorAction: ActorActionIdentity;
			readonly match: { readonly matched: false };
	  }
	| {
			readonly prediction: PredictionIdentity;
			readonly observation: "observed";
			readonly actorAction: ActorActionIdentity;
			readonly match: {
				readonly matched: true;
				readonly relation: ActionKeyMatch;
				readonly adoption: PredictionAdoption;
			};
	  };

export type SourceRequestSettlement =
	| { readonly status: "produced"; readonly proposalCount: number }
	| { readonly status: "empty" }
	| { readonly status: "timeout"; readonly cause: ResolutionCause & { readonly stage: "source" } }
	| { readonly status: "error"; readonly cause: ResolutionCause & { readonly stage: "source" } }
	| { readonly status: "aborted"; readonly cause: ResolutionCause & { readonly stage: "source" } };

export type SourceRequestKind = "proposal" | "continuation";

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
	readonly startedAt: number;
	readonly durationMs: number;
	readonly settlement: SourceRequestSettlement;
}

export interface CandidateRejection {
	readonly candidateID: string;
	readonly match: ActionKeyMatch;
	readonly cause: ResolutionCause;
}

export interface ActorHitTiming {
	/** Work completed before Actor interception, capped by execution duration. */
	readonly executionAheadMs: number;
	/** Execution-owning source request to Actor interception. */
	readonly attemptLeadMs: number;
	/** Actor interception through adoption and result retention; session cleanup is separate. */
	readonly hitLatencyMs: number;
	/** Calibrated Actor service estimate, absent until an Actor baseline is observed. */
	readonly expectedActorMs?: number;
}

/** Latency an isolation-blocked match could hide if a safe execution route were available. */
export interface ExecutionBlockedTiming {
	/** Earliest matching prediction intent to Actor interception. */
	readonly attemptLeadMs: number;
	/** Actor execution time covered by that lead, capped by the authoritative duration. */
	readonly executionAheadMs: number;
	/** Authoritative execution time that would remain on the Actor critical path. */
	readonly hitLatencyMs: number;
}

export type ActorActionProvider =
	| {
			readonly kind: "speculative";
			readonly candidateID: string;
			readonly match: ActionKeyMatch;
			readonly timing: ActorHitTiming;
			readonly toolExecution: TimelineInterval;
	  }
	| {
			readonly kind: "actor";
			readonly origin: "fallback";
			readonly durationMs: number;
			readonly isError: boolean;
			readonly toolExecution: TimelineInterval;
			readonly executionBlockedTiming?: ExecutionBlockedTiming;
	  }
	| {
			readonly kind: "actor";
			readonly origin: "preview";
			readonly candidateID: string;
			readonly durationMs: number;
			readonly isError: false;
			readonly toolExecution: TimelineInterval;
	  };

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
): ResolutionCause & { readonly stage: Stage } {
	return Object.freeze({ stage, code, ...(detail ? { detail } : {}) });
}

export function zeroValidationMetrics(mode: ValidationMetrics["mode"] = "exact"): ValidationMetrics {
	return { durationMs: 0, bytesRead: 0, filesRead: 0, mode };
}
