import type { SpeculativeExecutionRoute, WorldExecutionMetrics } from "./execution-world.ts";
import type { ActorActionSettlement, PredictionSettlement, ResolutionCause, SettledSourceRequest } from "./settlement.ts";
import type { SpeculativeTaskTiming } from "./task-timing.ts";

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
	readonly world?: { readonly backend: string; readonly executionMetrics: WorldExecutionMetrics; };
	readonly source: string;
	readonly depth: number;
	readonly predictedAction: string;
}

export type CandidateExecutionProjection =
	| { readonly status: "running"; readonly startedAt: number }
	| { readonly status: "succeeded"; readonly startedAt: number; readonly completedAt: number; readonly executionMs: number; }
	| {
			readonly status: "failed" | "cancelled";
			readonly cause: ResolutionCause;
			readonly startedAt?: number;
			readonly completedAt: number;
			readonly executionMs: number;
	  };

interface EventEnvelope<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly timestamp: number;
	readonly cache: SpeculativeCacheSnapshot;
}

/** Immutable observability projections. Policy and learning never consume this stream. */
export type SpeculativeActionEvent<SessionID> =
	| (EventEnvelope<SessionID> & { readonly type: "task"; readonly timing: SpeculativeTaskTiming; })
	| (EventEnvelope<SessionID> & {
			readonly type: "source_request";
			readonly request: SettledSourceRequest;
			readonly totalDraftTokens: number;
	  })
	| (EventEnvelope<SessionID> & { readonly type: "prediction"; readonly settlement: PredictionSettlement; })
	| (EventEnvelope<SessionID> & { readonly type: "operation_prediction"; readonly settlement: PredictionSettlement; })
	| (EventEnvelope<SessionID> & {
			readonly type: "candidate";
			readonly candidate: CandidateEventDescriptor;
			readonly state: CandidateExecutionProjection;
	  })
	| (EventEnvelope<SessionID> & {
			readonly type: "actor_action";
			readonly settlement: ActorActionSettlement;
			readonly actualAction: string;
			readonly candidate?: CandidateEventDescriptor;
	  });
