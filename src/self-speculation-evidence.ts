import { isSafeCount } from "./number-utils.ts";
import { stableStringify } from "./stable-json.ts";

interface EvidenceState {
	observations: number;
	trials: number;
	successes: number;
}

/** Beta-prior evidence over opaque structural contexts. */
export class EvidenceLedger {
	private readonly states = new Map<string, EvidenceState>();
	private readonly priorTrials: number;
	private readonly priorSuccesses: number;

	constructor(priorTrials: number, priorSuccesses: number) {
		this.priorTrials = priorTrials;
		this.priorSuccesses = priorSuccesses;
	}

	observe(context: object, trials: number, successes: number): void {
		if (!isSafeCount(trials, 1)) return;
		if (!isSafeCount(successes, 0, trials)) return;
		const key = stableStringify(context);
		const state = this.states.get(key) ?? { observations: 0, trials: 0, successes: 0 };
		state.observations++;
		state.trials += trials;
		state.successes += successes;
		this.states.set(key, state);
	}

	probability(contexts: readonly object[]): number {
		if (!contexts.length) return this.priorSuccesses / this.priorTrials;
		return (
			contexts.reduce((total, context) => {
				const state = this.states.get(stableStringify(context));
				return (
					total +
					((state?.successes ?? 0) + this.priorSuccesses) /
						((state?.trials ?? 0) + this.priorTrials)
				);
			}, 0) / contexts.length
		);
	}

	snapshot(): Readonly<EvidenceState & { contexts: number }> {
		const total = { contexts: this.states.size, observations: 0, trials: 0, successes: 0 };
		for (const state of this.states.values()) {
			total.observations += state.observations;
			total.trials += state.trials;
			total.successes += state.successes;
		}
		return total;
	}
}
