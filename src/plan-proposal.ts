export type PlanActionDependencyCondition = "execution_settled" | "execution_succeeded" | "actor_adopted";

/** A scheduler-visible edge. The producer may add actions in later deltas. */
export interface PlanActionDependency {
	readonly actionID: string;
	readonly condition?: PlanActionDependencyCondition;
}

export interface PlanAction {
	/** Stable within one proposal across revisions. */
	readonly id: string;
	readonly type: "tool_call";
	readonly tool: string;
	readonly input: unknown;
	readonly diagnostic?: string;
	/** Expected Actor tool batches before this action; used to schedule execution. */
	readonly horizon?: number;
	/** Latest Actor tool batches before this prediction becomes a miss. Defaults to horizon. */
	readonly latestHorizon?: number;
	readonly empiricalProbability?: number;
	readonly conditionalProbability?: number;
	readonly expectedDurationMs?: number;
	readonly expectedLatencyBenefitMs?: number;
	/** Uses only otherwise-idle speculative capacity and yields first under contention. */
	readonly background?: boolean;
	readonly resourceDemand?: number;
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
	readonly draftTokens?: number;
}

/** Incremental update to a proposal. Revisions must be strictly increasing. */
export interface PlanDelta {
	readonly proposalID: string;
	readonly source: string;
	readonly revision: number;
	readonly upsert?: readonly PlanAction[];
	readonly remove?: readonly string[];
	readonly draftTokens?: number;
}

export type PlanUpdate = PlanProposal | PlanDelta;

export interface MaterializedPlan extends Required<PlanProposal> {}
