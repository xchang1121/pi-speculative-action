import { stableStringify } from "./stable-json.ts";

export interface DecoderEvidenceContext {
	readonly model: string;
	readonly endpoint: string;
	readonly format: string;
	readonly boundary: string;
	readonly tool: string;
	readonly source: string;
}

export interface DecoderEvidenceSnapshot {
	readonly contexts: number;
	readonly verificationSteps: number;
	readonly attributedDraftTokens: number;
	readonly attributedAcceptedTokens: number;
}

export interface ActionEvidenceContext {
	readonly model: string;
	readonly tool: string;
	readonly source: string;
}

export interface ActionEvidenceSnapshot {
	readonly contexts: number;
	readonly observations: number;
	readonly adoptions: number;
}

interface DecoderEvidenceState {
	verificationSteps: number;
	draftedTokens: number;
	acceptedTokens: number;
}

interface ActionEvidenceState {
	observations: number;
	adoptions: number;
}

const PRIOR_DRAFT_TOKENS = 4;
const PRIOR_ACCEPTED_TOKENS = 2;
const PRIOR_ACTION_OBSERVATIONS = 2;
const PRIOR_ACTION_ADOPTIONS = 1;

/**
 * Online target-verification evidence. It deliberately knows nothing about
 * action adoption: token compatibility and semantic action utility are
 * different signals and must not train each other implicitly.
 */
export class DecoderVerificationLedger {
	private readonly states = new Map<string, DecoderEvidenceState>();

	observe(context: DecoderEvidenceContext, draftedTokens: number, acceptedTokens: number): void {
		if (!Number.isSafeInteger(draftedTokens) || draftedTokens <= 0) return;
		if (!Number.isSafeInteger(acceptedTokens) || acceptedTokens < 0 || acceptedTokens > draftedTokens) return;
		const key = evidenceKey(context);
		const state = this.states.get(key) ?? { verificationSteps: 0, draftedTokens: 0, acceptedTokens: 0 };
		state.verificationSteps++;
		state.draftedTokens += draftedTokens;
		state.acceptedTokens += acceptedTokens;
		this.states.set(key, state);
	}

	probability(contexts: readonly DecoderEvidenceContext[]): number {
		if (!contexts.length) return PRIOR_ACCEPTED_TOKENS / PRIOR_DRAFT_TOKENS;
		return (
			contexts.reduce((total, context) => total + this.contextProbability(context), 0) / contexts.length
		);
	}

	snapshot(): DecoderEvidenceSnapshot {
		let verificationSteps = 0;
		let attributedDraftTokens = 0;
		let attributedAcceptedTokens = 0;
		for (const state of this.states.values()) {
			verificationSteps += state.verificationSteps;
			attributedDraftTokens += state.draftedTokens;
			attributedAcceptedTokens += state.acceptedTokens;
		}
		return {
			contexts: this.states.size,
			verificationSteps,
			attributedDraftTokens,
			attributedAcceptedTokens,
		};
	}

	private contextProbability(context: DecoderEvidenceContext): number {
		const state = this.states.get(evidenceKey(context));
		return (
			((state?.acceptedTokens ?? 0) + PRIOR_ACCEPTED_TOKENS) /
			((state?.draftedTokens ?? 0) + PRIOR_DRAFT_TOKENS)
		);
	}
}

/** Actor adoption evidence used by decoder ranking, never by token calibration. */
export class ActionAdoptionLedger {
	private readonly states = new Map<string, ActionEvidenceState>();

	observe(context: ActionEvidenceContext, adopted: boolean): void {
		const key = evidenceKey(context);
		const state = this.states.get(key) ?? { observations: 0, adoptions: 0 };
		state.observations++;
		if (adopted) state.adoptions++;
		this.states.set(key, state);
	}

	probability(contexts: readonly ActionEvidenceContext[]): number {
		if (!contexts.length) return PRIOR_ACTION_ADOPTIONS / PRIOR_ACTION_OBSERVATIONS;
		return contexts.reduce((total, context) => total + this.contextProbability(context), 0) / contexts.length;
	}

	snapshot(): ActionEvidenceSnapshot {
		let observations = 0;
		let adoptions = 0;
		for (const state of this.states.values()) {
			observations += state.observations;
			adoptions += state.adoptions;
		}
		return { contexts: this.states.size, observations, adoptions };
	}

	private contextProbability(context: ActionEvidenceContext): number {
		const state = this.states.get(evidenceKey(context));
		return (
			((state?.adoptions ?? 0) + PRIOR_ACTION_ADOPTIONS) /
			((state?.observations ?? 0) + PRIOR_ACTION_OBSERVATIONS)
		);
	}
}

function evidenceKey(context: DecoderEvidenceContext | ActionEvidenceContext): string {
	return stableStringify(context);
}
