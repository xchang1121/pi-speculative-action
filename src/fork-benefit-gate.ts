export interface BenefitGatePolicy {
	readonly enabled: boolean;
	readonly minSamples: number;
	readonly windowSize: number;
	readonly minNetBenefitMs: number;
	readonly probeInterval: number;
	readonly failureThreshold: number;
}

export const DEFAULT_BENEFIT_GATE_POLICY: BenefitGatePolicy = Object.freeze({
	enabled: true,
	minSamples: 4,
	windowSize: 4,
	minNetBenefitMs: 25,
	probeInterval: 4,
	failureThreshold: 2,
});

export interface BenefitObservation {
	readonly costMs: number;
	readonly benefitMs: number;
	readonly failed?: boolean;
}

/** Conservative request-budget credit; execution lead alone is not avoided Actor work. */
export function adoptionUtility(timing: ActorHitTiming): BenefitObservation {
	return { costMs: metric(timing.hitLatencyMs), benefitMs: metric(timing.expectedActorMs ?? 0) };
}

export type BenefitDecisionReason =
	| "disabled"
	| "warmup"
	| "profitable"
	| "utility_probe"
	| "failure_probe"
	| "negative_utility"
	| "failure_circuit";

export interface BenefitDecision {
	readonly allowed: boolean;
	readonly reason: BenefitDecisionReason;
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
}

export interface BenefitGateSnapshot {
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
	readonly consecutiveFailures: number;
	readonly suppressedDecisions: number;
}

interface GateState {
	readonly samples: Array<{ netBenefit: number; failed: boolean }>;
	priorFailures: number;
	suppressedSinceProbe: number;
	totalSuppressed: number;
}

/** Key-scoped rolling utility gate with bounded exploration and a failure circuit. */
export class BenefitGate {
	private readonly states = new Map<string, GateState>();

	decide(key: string, policy: BenefitGatePolicy): BenefitDecision {
		const state = this.state(key);
		const expected = mean(state.samples);
		const base = {
			samples: state.samples.length,
			...(expected === undefined ? {} : { expectedNetBenefitMs: expected }),
		};
		if (!policy.enabled) return { allowed: true, reason: "disabled", ...base };
		if (consecutiveFailures(state) >= policy.failureThreshold)
			return this.probeDecision(state, policy, "failure_probe", "failure_circuit", base);
		if (state.samples.length < policy.minSamples) return { allowed: true, reason: "warmup", ...base };
		if ((expected ?? 0) >= policy.minNetBenefitMs) return { allowed: true, reason: "profitable", ...base };
		return this.probeDecision(state, policy, "utility_probe", "negative_utility", base);
	}

	observe(
		key: string,
		observation: BenefitObservation,
		policy: BenefitGatePolicy,
	): (observation: BenefitObservation) => void {
		const state = this.state(key);
		const sample = { netBenefit: 0, failed: false };
		state.samples.push(sample);
		// Late lineage costs/benefits amend one retained sample, never append another observation.
		const update = (value: BenefitObservation) => {
			if (this.states.get(key) !== state || !state.samples.includes(sample)) return;
			sample.netBenefit = metric(value.benefitMs) - metric(value.costMs);
			sample.failed = value.failed === true;
		};
		update(observation);
		for (const evicted of state.samples.splice(0, Math.max(0, state.samples.length - policy.windowSize)))
			state.priorFailures = evicted.failed ? state.priorFailures + 1 : 0;
		state.suppressedSinceProbe = 0;
		return update;
	}

	snapshot(key: string): BenefitGateSnapshot {
		const state = this.state(key);
		const expected = mean(state.samples);
		return {
			samples: state.samples.length,
			...(expected === undefined ? {} : { expectedNetBenefitMs: expected }),
			consecutiveFailures: consecutiveFailures(state),
			suppressedDecisions: state.totalSuppressed,
		};
	}

	reset(): void {
		this.states.clear();
	}

	private probeDecision(
		state: GateState,
		policy: BenefitGatePolicy,
		probeReason: "utility_probe" | "failure_probe",
		skipReason: "negative_utility" | "failure_circuit",
		base: Pick<BenefitDecision, "samples" | "expectedNetBenefitMs">,
	): BenefitDecision {
		state.suppressedSinceProbe++;
		if (state.suppressedSinceProbe >= policy.probeInterval) {
			state.suppressedSinceProbe = 0;
			return { allowed: true, reason: probeReason, ...base };
		}
		state.totalSuppressed++;
		return { allowed: false, reason: skipReason, ...base };
	}

	private state(key: string): GateState {
		const existing = this.states.get(key);
		if (existing) return existing;
		const created: GateState = {
			samples: [],
			priorFailures: 0,
			suppressedSinceProbe: 0,
			totalSuppressed: 0,
		};
		this.states.set(key, created);
		return created;
	}
}

function metric(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function mean(values: GateState["samples"]): number | undefined {
	return values.length ? values.reduce((total, value) => total + value.netBenefit, 0) / values.length : undefined;
}

function consecutiveFailures(state: GateState): number {
	return state.samples.reduce((count, sample) => sample.failed ? count + 1 : 0, state.priorFailures);
}
import type { ActorHitTiming } from "./settlement.ts";
