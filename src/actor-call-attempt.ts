import type { ActionKeyMatch } from "./action-semantics.ts";
import type { ActorAction } from "./actor-action.ts";
import {
	cause,
	type ActorHitTiming,
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
}

export interface ActorCallFallback {
	readonly cause: ResolutionCause;
	readonly candidateID?: string;
}

/**
 * Owns the mutable matching attempt around an ActorAction.
 *
 * ActorAction remains the exactly-once settlement record. This object owns the temporary facts
 * needed while selecting its provider: the latest fallback, one selection, and the admission
 * lease that must be released on every exit.
 */
export class ActorCallAttempt<Candidate extends { readonly id: string }, Output> {
	readonly action: ActorAction;
	private fallbackValue: ActorCallFallback;
	private selectionValue?: ActorCandidateSelection<Candidate, Output>;
	private admissionReleased = false;
	private finalized = false;
	private readonly releaseActorAdmission: () => void;

	constructor(input: {
		readonly action: ActorAction;
		readonly fallback: ResolutionCause;
		readonly releaseActorAdmission: () => void;
	}) {
		this.action = input.action;
		this.fallbackValue = Object.freeze({ cause: input.fallback });
		this.releaseActorAdmission = input.releaseActorAdmission;
	}

	get fallback(): ActorCallFallback {
		return this.fallbackValue;
	}

	get selection(): ActorCandidateSelection<Candidate, Output> | undefined {
		return this.selectionValue;
	}

	setFallback(failure: ResolutionCause): boolean {
		if (this.finalized || this.selectionValue) return false;
		this.fallbackValue = Object.freeze({ cause: failure });
		return true;
	}

	rejectCandidate(candidateID: string, match: ActionKeyMatch, failure: ResolutionCause): boolean {
		if (this.finalized || !this.action.reject(candidateID, match, failure)) return false;
		this.fallbackValue = Object.freeze({ cause: failure, candidateID });
		return true;
	}

	interruptCandidate(candidateID: string, failure: ResolutionCause): boolean {
		if (this.finalized) return false;
		this.fallbackValue = Object.freeze({ cause: failure, candidateID });
		return true;
	}

	select(selection: ActorCandidateSelection<Candidate, Output>): boolean {
		if (this.finalized || this.selectionValue) return false;
		this.selectionValue = Object.freeze({ ...selection });
		return true;
	}

	settleSelection(
		matchedPredictions: readonly PredictionIdentity[],
		provider: "speculative" | "preview",
	): PredictionAdoption | undefined {
		const selected = this.selectionValue;
		if (!selected || this.finalized) return undefined;
		const settlement =
			provider === "preview"
				? this.action.settlePreview(selected.candidate.id, selected.toolExecution, matchedPredictions)
				: this.action.adopt(
						selected.candidate.id,
						selected.match,
						selected.timing,
						selected.toolExecution,
						matchedPredictions,
					);
		if (!settlement) return undefined;
		this.finalized = true;
		return provider === "preview"
			? {
					status: "rejected",
					candidateID: selected.candidate.id,
					cause: cause("control", "actor_preview_provider"),
				}
			: { status: "adopted", candidateID: selected.candidate.id };
	}

	deferToFallback(
		matchedPredictions: readonly PredictionIdentity[] = [],
		executionBlockedAttemptLeadMs?: number,
		fallback?: ResolutionCause,
	): PredictionAdoption | undefined {
		if (this.finalized || this.selectionValue) return undefined;
		if (fallback) this.fallbackValue = Object.freeze({ cause: fallback });
		if (!this.action.deferToFallback(matchedPredictions, executionBlockedAttemptLeadMs)) return undefined;
		this.finalized = true;
		return {
			status: "rejected",
			...(this.fallbackValue.candidateID ? { candidateID: this.fallbackValue.candidateID } : {}),
			cause: this.fallbackValue.cause,
		};
	}

	releaseAdmission(): boolean {
		if (this.admissionReleased) return false;
		this.admissionReleased = true;
		this.releaseActorAdmission();
		return true;
	}

	close(): void {
		if (!this.finalized && !this.selectionValue) {
			this.action.deferToFallback();
			this.finalized = true;
		}
		this.releaseAdmission();
	}
}
