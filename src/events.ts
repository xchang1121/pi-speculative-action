import type { CandidateExecutionState } from "./candidate-execution.ts";
import type { SpeculativeExecutionRoute, WorldExecutionMetrics } from "./execution-world.ts";
import type { ActorActionSettlement, PredictionSettlement, SettledSourceRequest } from "./settlement.ts";
import type { SpeculativeTaskTiming, ToolComputationTiming } from "./task-timing.ts";

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
	readonly mode?: string;
	readonly depth: number;
	readonly predictedAction: string;
}

export type CandidateExecutionProjection =
	| { readonly status: "running" }
	| { readonly status: "succeeded"; readonly executionMs: number; }
	| Extract<CandidateExecutionState<never>, { readonly status: "failed" | "cancelled" }>;

interface EventEnvelope<SessionID> {
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly timestamp: number; // Epoch milliseconds from performance.timeOrigin + performance.now().
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
	| (EventEnvelope<SessionID> & { readonly type: "prediction"; readonly settlement: PredictionSettlement; readonly tool?: string; readonly predictedAction?: string; readonly mode?: string; })
	| (EventEnvelope<SessionID> & { readonly type: "operation_prediction"; readonly settlement: PredictionSettlement; readonly tool?: string; readonly predictedAction?: string; readonly mode?: string; })
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
			/** Actual accepted computation for this Actor call, including consumed preparations. */
			readonly computation?: ToolComputationTiming;
	  });
