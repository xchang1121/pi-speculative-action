import type { SpeculativeExecution } from "./execution-world.ts";
import type {
	ActorActionSettlement,
	PredictionSettlement,
	ResolutionCause,
	SettledSourceRequest,
} from "./settlement.ts";
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
	readonly cacheTools: readonly string[];
	readonly cacheExecutions: readonly SpeculativeExecution[];
}

export interface CandidateEventDescriptor {
	readonly id: string;
	readonly origin: "prediction" | "actor_preview" | "actor_result";
	readonly tool: string;
	readonly actionKeyHash: string;
	readonly execution: SpeculativeExecution;
	/** Concrete world identity used for source/tool/backend utility accounting. */
	readonly executionBackend: string;
	/** Route-local identity; a changed sandbox/runtime starts a separate utility history. */
	readonly executionFingerprint: string;
	readonly source: string;
	readonly depth: number;
	readonly predictedAction: string;
	readonly predictionLatencyMs: number;
	readonly draftTokens: number;
	readonly totalDraftTokens: number;
	readonly expectedDurationMs: number;
	readonly estimatedBytes: number;
	readonly validation: {
		readonly durationMs: number;
		readonly bytesRead: number;
		readonly filesRead: number;
		readonly mode?: "watcher" | "exact";
	};
}

export type CandidateExecutionProjection =
	| { readonly status: "running"; readonly startedAt: number }
	| {
			readonly status: "succeeded";
			readonly startedAt: number;
			readonly completedAt: number;
			readonly executionMs: number;
	  }
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
	| (EventEnvelope<SessionID> & {
			readonly type: "task";
			readonly timing: SpeculativeTaskTiming;
	  })
	| (EventEnvelope<SessionID> & {
			readonly type: "source_request";
			readonly request: SettledSourceRequest;
	  })
	| (EventEnvelope<SessionID> & {
			readonly type: "prediction";
			readonly settlement: PredictionSettlement;
	  })
	| (EventEnvelope<SessionID> & {
			readonly type: "candidate";
			readonly candidate: CandidateEventDescriptor;
			readonly state: CandidateExecutionProjection;
	  })
	| (EventEnvelope<SessionID> & {
			readonly type: "actor_action";
			readonly settlement: ActorActionSettlement;
			readonly actualAction: string;
			readonly execution?: SpeculativeExecution;
			readonly candidate?: CandidateEventDescriptor;
	  });
