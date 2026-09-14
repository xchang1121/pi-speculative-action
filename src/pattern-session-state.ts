import type { ActionKey } from "./action-semantics.ts";
import type { BoundedRecencyMap } from "./bounded-recency-map.ts";

export interface PatternPendingValidation {
	readonly patternID: string;
	readonly triggerSequence: number;
	readonly expectedInputs: ReadonlyArray<Record<string, unknown>>;
	remaining: number;
}

export interface PatternRecurrentAction {
	readonly action: ActionKey;
	readonly input: Record<string, unknown>;
	count: number;
	/** Sufficient statistics at lastSeenSequence; count retains the actual support threshold. */
	weightedCount: number;
	weightedDurationMs: number;
	lastSeenSequence: number;
}

export interface PatternSessionState<Event> {
	readonly history: Event[];
	pending: PatternPendingValidation[];
	readonly recurrentActions: BoundedRecencyMap<string, PatternRecurrentAction>;
}

/** Runtime-only state is deliberately much smaller than the persisted pattern corpus. */
export function patternSessionBudgets(maxPatterns: number) {
	const corpusLimit = Math.max(1, Math.floor(maxPatterns));
	return {
		sessions: Math.min(corpusLimit, 64),
		recurrentActionsPerSession: Math.min(corpusLimit, 256),
		pendingValidationsPerSession: Math.min(corpusLimit, 512),
	};
}
