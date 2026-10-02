import type { ActorHitTiming } from "./settlement.ts";
import { nonNegativeFinite as metric } from "./number-utils.ts";

export interface BenefitGatePolicy extends Readonly<typeof benefitGateDefaults> {}

const benefitGateDefaults = { enabled: true, minSamples: 4, windowSize: 4, minNetBenefitMs: 25, probeInterval: 4, failureThreshold: 2 };

export const DEFAULT_BENEFIT_GATE_POLICY: BenefitGatePolicy = Object.freeze(benefitGateDefaults);

export interface BenefitObservation {
	readonly costMs: number;
	/** Missing for adopted work whose counterfactual service time was not observed. */
	readonly benefitMs?: number;
	readonly failed?: boolean;
}

/** Historical fallback service is an estimate; a censored hit is neither zero gain nor measured savings. */
export function creditAdoption(utility: { costMs: number; benefitMs?: number }, timing: ActorHitTiming, shares = 1): void {
	utility.costMs += metric(timing.hitLatencyMs) / shares;
	utility.benefitMs = utility.benefitMs === undefined || timing.expectedActorMs === undefined
		? undefined : utility.benefitMs + metric(timing.expectedActorMs) / shares;
}

export type BenefitDecisionReason =
	| "disabled"
	| "warmup"
	| "profitable"
	| "utility_probe"
	| "failure_probe"
	| "calibration_probe"
	| "benefit_unmeasured"
	| "negative_utility"
	| "failure_circuit";

export interface BenefitDecision {
	readonly allowed: boolean;
	readonly reason: BenefitDecisionReason;
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
}

interface GateState {
	readonly samples: Array<{ netBenefit: number | undefined; failed: boolean }>;
	observations: number;
	priorFailures: number;
	suppressedSinceProbe: number;
	totalSuppressed: number;
	/** Each probe into a still-unprofitable window doubles the wait for the next (at most 8x); profit resets it. */
	backoff: number;
}

/** Key-scoped rolling utility gate with bounded exploration and a failure circuit. */
export class BenefitGate {
	private readonly states = new Map<string, GateState>();

	decide(key: string, policy: BenefitGatePolicy): BenefitDecision {
		const state = this.state(key);
		const expected = mean(state.samples);
		const base = { samples: state.samples.length, ...(expected === undefined ? {} : { expectedNetBenefitMs: expected }) };
		if (!policy.enabled) return { allowed: true, reason: "disabled", ...base };
		const failing = consecutiveFailures(state) >= policy.failureThreshold;
		if (!failing) {
			if (state.observations < policy.minSamples) return { allowed: true, reason: "warmup", ...base };
			if (expected !== undefined && expected >= policy.minNetBenefitMs) { state.backoff = 1; return { allowed: true, reason: "profitable", ...base }; }
		}
		if (++state.suppressedSinceProbe >= policy.probeInterval * state.backoff) {
			state.suppressedSinceProbe = 0; state.backoff = Math.min(8, state.backoff * 2);
			return { allowed: true, reason: failing ? "failure_probe" : expected === undefined ? "calibration_probe" : "utility_probe", ...base };
		}
		state.totalSuppressed++;
		return { allowed: false, reason: failing ? "failure_circuit" : expected === undefined ? "benefit_unmeasured" : "negative_utility", ...base };
	}

	observe(key: string, observation: BenefitObservation, policy: BenefitGatePolicy): (observation: BenefitObservation) => void {
		const state = this.state(key);
		const sample: GateState["samples"][number] = { netBenefit: undefined, failed: false };
		state.samples.push(sample);
		state.observations++;
		// Late lineage costs/benefits amend one retained sample, never append another observation.
		const update = (value: BenefitObservation) => {
			if (this.states.get(key) !== state || !state.samples.includes(sample)) return;
			sample.netBenefit = value.benefitMs === undefined ? undefined : metric(value.benefitMs) - metric(value.costMs);
			sample.failed = value.failed === true;
		};
		update(observation);
		for (const evicted of state.samples.splice(0, Math.max(0, state.samples.length - policy.windowSize)))
			state.priorFailures = evicted.failed ? state.priorFailures + 1 : 0;
		state.suppressedSinceProbe = 0;
		return update;
	}

	snapshot(key: string) {
		const state = this.state(key);
		const expected = mean(state.samples);
		const snapshot = { samples: state.samples.length, ...(expected === undefined ? {} : { expectedNetBenefitMs: expected }),
			consecutiveFailures: consecutiveFailures(state), suppressedDecisions: state.totalSuppressed };
		return snapshot as Readonly<typeof snapshot>;
	}

	reset(): void {
		this.states.clear();
	}

	private state(key: string): GateState {
		let state = this.states.get(key);
		if (!state) {
			state = { samples: [], observations: 0, priorFailures: 0, suppressedSinceProbe: 0, totalSuppressed: 0, backoff: 1 };
			this.states.set(key, state);
		}
		return state;
	}
}

function mean(values: GateState["samples"]): number | undefined {
	return values.length && values.every((value) => value.netBenefit !== undefined)
		? values.reduce((total, value) => total + value.netBenefit!, 0) / values.length : undefined;
}

function consecutiveFailures(state: GateState): number {
	return state.samples.reduce((count, sample) => sample.failed ? count + 1 : 0, state.priorFailures);
}
