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
export interface ResolutionCause { readonly stage: ResolutionStage; readonly code: string; readonly detail?: string; }

export interface ValidationMetrics {
	readonly bytesRead: number;
	readonly filesRead: number;
	readonly mode: "watcher" | "exact";
}

export type ResourceValidation = { readonly metrics: ValidationMetrics } & (
	| { readonly status: "valid" }
	| ({ readonly cause: ResolutionCause & { readonly stage: "freshness" } } & (
		{
			readonly status: "stale";
			/** Changed inputs were revoked while independent computations survived; one observation rebuild may help. */
			readonly reconstruct?: true;
		} | { readonly status: "indeterminate" }
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
	| { readonly status: "adopted"; readonly candidateID: string; }
	| { readonly status: "rejected"; readonly candidateID?: string; readonly cause: ResolutionCause; };

export type PredictionSettlement = { readonly prediction: PredictionIdentity } & (
	| { readonly observation: "unobserved"; readonly cause: ResolutionCause; }
	| ({
			readonly observation: "observed";
			readonly actorAction: ActorActionIdentity;
	  } & (
			| { readonly match: { readonly matched: false }; }
			| { readonly match: { readonly matched: true; readonly relation: ActionKeyMatch; readonly adoption: PredictionAdoption; }; }
	  ))
);

/** Eligible learning samples: shared work earns adoption without inventing a tool match. */
export function predictionLearning(settlement: PredictionSettlement, served?: boolean) {
	const match = settlement.observation === "observed" ? settlement.match : undefined;
	const adopted = served ?? (match?.matched === true && match.adoption.status === "adopted");
	return { matched: match?.matched || !adopted ? match?.matched : undefined,
		adopted: adopted || match?.matched ? adopted : undefined };
}

export type SourceRequestSettlement =
	| { readonly status: "produced"; readonly proposalCount: number }
	| { readonly status: "empty"; readonly cause?: ResolutionCause & { readonly stage: "source" } }
	| { readonly status: "timeout"; readonly cause: ResolutionCause & { readonly stage: "source" } }
	| { readonly status: "error"; readonly cause: ResolutionCause & { readonly stage: "source" } }
	| { readonly status: "aborted"; readonly cause: ResolutionCause & { readonly stage: "source" } };

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

export interface CandidateRejection { readonly candidateID: string; readonly match: ActionKeyMatch; readonly cause: ResolutionCause; }

export type ActorActionProvider = { readonly toolExecution: TimelineInterval } & (
	| {
			readonly kind: "speculative";
			readonly candidateID: string;
			readonly match: ActionKeyMatch;
	  }
	| ({ readonly kind: "actor"; readonly durationMs: number } & (
		{
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
): ResolutionCause & { readonly stage: Stage } {
	return Object.freeze({ stage, code, ...(detail ? { detail } : {}) });
}

export function zeroValidationMetrics(mode: ValidationMetrics["mode"] = "exact"): ValidationMetrics {
	return { bytesRead: 0, filesRead: 0, mode };
}
