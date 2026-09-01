import type { ActionKey } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";

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
	totalDurationMs: number;
	lastSeenSequence: number;
}

export interface PatternSessionBudgets {
	readonly sessions: number;
	readonly recurrentActionsPerSession: number;
	readonly pendingValidationsPerSession: number;
}

/** Runtime-only state is deliberately much smaller than the persisted pattern corpus. */
export function patternSessionBudgets(maxPatterns: number): PatternSessionBudgets {
	const corpusLimit = Math.max(1, Math.floor(maxPatterns));
	return {
		sessions: Math.min(corpusLimit, 64),
		recurrentActionsPerSession: Math.min(corpusLimit, 256),
		pendingValidationsPerSession: Math.min(corpusLimit, 512),
	};
}

export class PatternSessionState<Event> {
	readonly id: string;
	readonly history: Event[] = [];
	private pendingValue: PatternPendingValidation[] = [];
	private readonly recurrentActionsValue: BoundedRecencyMap<string, PatternRecurrentAction>;
	private readonly pendingLimit: number;

	constructor(id: string, budgets: PatternSessionBudgets) {
		this.id = id;
		this.pendingLimit = budgets.pendingValidationsPerSession;
		this.recurrentActionsValue = new BoundedRecencyMap(budgets.recurrentActionsPerSession);
	}

	get pending(): readonly PatternPendingValidation[] {
		return this.pendingValue;
	}

	get recurrentActions(): Iterable<PatternRecurrentAction> {
		return this.recurrentActionsValue.values();
	}

	recurrentAction(key: string): PatternRecurrentAction | undefined {
		return this.recurrentActionsValue.get(key);
	}

	rememberRecurrentAction(key: string, action: PatternRecurrentAction): void {
		this.recurrentActionsValue.set(key, action);
	}

	replacePending(next: readonly PatternPendingValidation[]): readonly PatternPendingValidation[] {
		const overflow = Math.max(0, next.length - this.pendingLimit);
		const dropped = overflow ? next.slice(0, overflow) : [];
		this.pendingValue = next.slice(overflow);
		return dropped;
	}

	removePattern(patternID: string): void {
		if (!this.pendingValue.some((item) => item.patternID === patternID)) return;
		this.pendingValue = this.pendingValue.filter((item) => item.patternID !== patternID);
	}
}

export class PatternSessionRegistry<Event> {
	private readonly sessions: BoundedRecencyMap<string, PatternSessionState<Event>>;
	private readonly budgets: PatternSessionBudgets;

	constructor(budgets: PatternSessionBudgets) {
		this.budgets = budgets;
		this.sessions = new BoundedRecencyMap(budgets.sessions);
	}

	ensure(sessionID: string): {
		readonly state: PatternSessionState<Event>;
		readonly evicted?: PatternSessionState<Event>;
	} {
		const existing = this.sessions.get(sessionID);
		if (existing) return { state: existing };
		const state = new PatternSessionState<Event>(sessionID, this.budgets);
		const evicted = this.sessions.set(sessionID, state)?.value;
		return { state, ...(evicted ? { evicted } : {}) };
	}

	get(sessionID: string): PatternSessionState<Event> | undefined {
		return this.sessions.get(sessionID);
	}

	finish(sessionID: string): PatternSessionState<Event> | undefined {
		const state = this.sessions.get(sessionID);
		if (state) this.sessions.delete(sessionID);
		return state;
	}

	removePattern(patternID: string): void {
		for (const state of this.sessions.values()) state.removePattern(patternID);
	}
}
