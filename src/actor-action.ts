import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import {
	cause,
	type ActorActionIdentity,
	type ActorActionProvider,
	type ActorActionSettlement,
	type ActorHitTiming,
	type CandidateRejection,
	type ExecutionBlockedTiming,
	type PredictionAdoption,
	type PredictionIdentity,
	type ResolutionCause,
} from "./settlement.ts";
import type { TimelineInterval } from "./task-timing.ts";

export interface ActorCandidateSelection<Candidate extends { readonly id: string }, Output> {
	readonly candidate: Candidate;
	readonly match: ActionKeyMatch;
	readonly output: Output;
	readonly timing: ActorHitTiming;
	readonly toolExecution: TimelineInterval;
	readonly projection?: TimelineInterval;
}

type ActorActionState<Candidate extends { readonly id: string }, Output> =
	| { readonly status: "matching" }
	| { readonly status: "selected"; readonly value: ActorCandidateSelection<Candidate, Output> }
	| {
			readonly status: "awaiting_fallback";
			readonly matchedPredictions: readonly PredictionIdentity[];
			readonly executionBlockedAttemptLeadMs?: number;
	  }
	| { readonly status: "settled"; readonly value: ActorActionSettlement };

/** One owner for matching, the committed selection, admission and exactly-once settlement. */
export class ActorAction<Candidate extends { readonly id: string } = { readonly id: string }, Output = unknown> {
	readonly identity: ActorActionIdentity;
	readonly tool: string;
	readonly actionKey?: ActionKey;
	private readonly rejections: CandidateRejection[] = [];
	private stateValue: ActorActionState<Candidate, Output> = Object.freeze({ status: "matching" });
	private fallbackValue: { readonly cause: ResolutionCause; readonly candidateID?: string };
	private releaseActorAdmission?: () => void;

	constructor(input: {
		readonly identity: ActorActionIdentity;
		readonly tool: string;
		readonly actionKey?: ActionKey;
		readonly fallback: ResolutionCause;
		readonly releaseActorAdmission: () => void;
	}) {
		this.identity = Object.freeze({ ...input.identity });
		this.tool = input.tool;
		this.actionKey = input.actionKey;
		this.fallbackValue = Object.freeze({ cause: input.fallback });
		this.releaseActorAdmission = input.releaseActorAdmission;
	}

	get state(): ActorActionState<Candidate, Output> {
		return this.stateValue;
	}

	get settlement(): ActorActionSettlement | undefined {
		return this.stateValue.status === "settled" ? this.stateValue.value : undefined;
	}

	get fallback() {
		return this.fallbackValue;
	}

	get selection() {
		return this.stateValue.status === "selected" ? this.stateValue.value : undefined;
	}

	setFallback(failure: ResolutionCause, candidateID?: string): boolean {
		if (this.stateValue.status !== "matching") return false;
		this.fallbackValue = Object.freeze({ cause: failure, ...(candidateID ? { candidateID } : {}) });
		return true;
	}

	rejectCandidate(candidateID: string, match: ActionKeyMatch, failure: ResolutionCause): boolean {
		if (
			this.stateValue.status !== "matching" ||
			this.rejections.some((rejection) => rejection.candidateID === candidateID)
		) {
			return false;
		}
		this.rejections.push(
			Object.freeze({
				candidateID,
				match: Object.freeze({ ...match }),
				cause: Object.freeze({ ...failure }),
			}),
		);
		return this.setFallback(failure, candidateID);
	}

	select(selection: ActorCandidateSelection<Candidate, Output>): boolean {
		if (
			this.stateValue.status !== "matching" ||
			this.rejections.some((rejection) => rejection.candidateID === selection.candidate.id)
		) {
			return false;
		}
		this.stateValue = Object.freeze({ status: "selected", value: Object.freeze({ ...selection }) });
		return true;
	}

	settleSelection(
		matchedPredictions: readonly PredictionIdentity[],
		provider: "speculative" | "preview",
	): PredictionAdoption | undefined {
		if (this.stateValue.status !== "selected") return undefined;
		const selected = this.stateValue.value, candidateID = selected.candidate.id;
		const toolExecution = normalizeInterval(selected.toolExecution);
		this.finish(Object.freeze(provider === "preview" ? {
			kind: "actor",
			origin: "preview",
			candidateID,
			toolExecution,
			durationMs: toolExecution.completedAt - toolExecution.startedAt,
			isError: false,
		} : {
			kind: "speculative",
			candidateID,
			toolExecution,
			match: Object.freeze({ ...selected.match }),
			timing: normalizeTiming(selected.timing),
		}), freezePredictions(matchedPredictions));
		return provider === "preview"
			? { status: "rejected", candidateID, cause: cause("control", "actor_preview_provider") }
			: { status: "adopted", candidateID };
	}

	deferToFallback(
		matchedPredictions: readonly PredictionIdentity[] = [],
		executionBlockedAttemptLeadMs?: number,
		fallback?: ResolutionCause,
	): PredictionAdoption | undefined {
		if (this.stateValue.status !== "matching") return undefined;
		if (fallback) this.setFallback(fallback);
		this.stateValue = Object.freeze({
			status: "awaiting_fallback",
			matchedPredictions: freezePredictions(matchedPredictions),
			...(executionBlockedAttemptLeadMs !== undefined
				? { executionBlockedAttemptLeadMs: finite(executionBlockedAttemptLeadMs) }
				: {}),
		});
		return { status: "rejected", ...this.fallbackValue };
	}

	releaseAdmission(): void {
		const release = this.releaseActorAdmission;
		this.releaseActorAdmission = undefined;
		release?.();
	}

	close(): void {
		this.deferToFallback();
		this.releaseAdmission();
	}

	settleActor(
		durationMs: number,
		isError: boolean,
		completedAt = performance.now(),
	): ActorActionSettlement | undefined {
		if (this.stateValue.status !== "awaiting_fallback") return undefined;
		const duration = finite(durationMs);
		const completed = finite(completedAt);
		const executionBlockedTiming =
			this.stateValue.executionBlockedAttemptLeadMs === undefined
				? undefined
				: normalizeExecutionBlockedTiming(this.stateValue.executionBlockedAttemptLeadMs, duration);
		return this.finish(Object.freeze({
			kind: "actor",
			origin: "fallback",
			durationMs: duration,
			isError,
			toolExecution: Object.freeze({ startedAt: Math.max(0, completed - duration), completedAt: completed }),
			...(executionBlockedTiming ? { executionBlockedTiming } : {}),
		}), this.stateValue.matchedPredictions);
	}

	private finish(provider: ActorActionProvider, matchedPredictions: readonly PredictionIdentity[]): ActorActionSettlement {
		const settlement = Object.freeze({
			actorAction: this.identity,
			tool: this.tool,
			provider,
			matchedPredictions,
			...(this.actionKey ? { actionKeyHash: this.actionKey.hash } : {}),
			rejections: Object.freeze([...this.rejections]),
		});
		this.stateValue = Object.freeze({ status: "settled", value: settlement });
		return settlement;
	}
}

function normalizeInterval(interval: TimelineInterval): TimelineInterval {
	const startedAt = finite(interval.startedAt);
	return Object.freeze({ startedAt, completedAt: Math.max(startedAt, finite(interval.completedAt)) });
}

function normalizeTiming(timing: ActorHitTiming): ActorHitTiming {
	return Object.freeze({
		executionAheadMs: finite(timing.executionAheadMs),
		attemptLeadMs: finite(timing.attemptLeadMs),
		hitLatencyMs: finite(timing.hitLatencyMs),
		...(timing.expectedActorMs !== undefined && Number.isFinite(timing.expectedActorMs)
			? { expectedActorMs: finite(timing.expectedActorMs) } : {}),
	});
}

function normalizeExecutionBlockedTiming(attemptLeadMs: number, durationMs: number): ExecutionBlockedTiming {
	const attemptLead = finite(attemptLeadMs);
	const duration = finite(durationMs);
	const executionAheadMs = Math.min(duration, attemptLead);
	return Object.freeze({
		attemptLeadMs: attemptLead,
		executionAheadMs,
		hitLatencyMs: Math.max(0, duration - executionAheadMs),
	});
}

function freezePredictions(predictions: readonly PredictionIdentity[]): readonly PredictionIdentity[] {
	const unique = new Map(predictions.map((prediction) => [prediction.id, Object.freeze({ ...prediction })]));
	return Object.freeze([...unique.values()]);
}

function finite(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}
