export interface ForkBenefitGatePolicy {
	readonly enabled: boolean;
	readonly minSamples: number;
	readonly windowSize: number;
	readonly minNetBenefitMs: number;
	readonly probeInterval: number;
	readonly failureThreshold: number;
}

export interface ForkBenefitObservation {
	readonly forkLatencyMs: number;
	readonly exactLeadMs: number;
	readonly failed?: boolean;
}

export type ForkBenefitDecisionReason =
	| "disabled"
	| "warmup"
	| "profitable"
	| "utility_probe"
	| "failure_probe"
	| "negative_utility"
	| "failure_circuit";

export interface ForkBenefitDecision {
	readonly allowed: boolean;
	readonly reason: ForkBenefitDecisionReason;
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
}

export interface ForkBenefitGateSnapshot {
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
	readonly consecutiveFailures: number;
	readonly suppressedDecisions: number;
}

interface GateState {
	readonly netBenefits: number[];
	consecutiveFailures: number;
	suppressedSinceProbe: number;
	totalSuppressed: number;
}

/** Model-scoped rolling utility gate with bounded exploration and a failure circuit. */
export class ForkBenefitGate {
	private readonly states = new Map<string, GateState>();

	decide(key: string, policy: ForkBenefitGatePolicy): ForkBenefitDecision {
		const state = this.state(key);
		const expected = mean(state.netBenefits);
		const base = {
			samples: state.netBenefits.length,
			...(expected === undefined ? {} : { expectedNetBenefitMs: expected }),
		};
		if (!policy.enabled) return { allowed: true, reason: "disabled", ...base };
		if (state.consecutiveFailures >= policy.failureThreshold)
			return this.probeDecision(state, policy, "failure_probe", "failure_circuit", base);
		if (state.netBenefits.length < policy.minSamples) return { allowed: true, reason: "warmup", ...base };
		if ((expected ?? 0) >= policy.minNetBenefitMs) return { allowed: true, reason: "profitable", ...base };
		return this.probeDecision(state, policy, "utility_probe", "negative_utility", base);
	}

	observe(key: string, observation: ForkBenefitObservation, policy: ForkBenefitGatePolicy): void {
		const state = this.state(key);
		const latency = metric(observation.forkLatencyMs);
		const lead = metric(observation.exactLeadMs);
		state.netBenefits.push(lead - latency);
		if (state.netBenefits.length > policy.windowSize) {
			state.netBenefits.splice(0, state.netBenefits.length - policy.windowSize);
		}
		state.consecutiveFailures = observation.failed ? state.consecutiveFailures + 1 : 0;
		state.suppressedSinceProbe = 0;
	}

	snapshot(key: string): ForkBenefitGateSnapshot {
		const state = this.state(key);
		const expected = mean(state.netBenefits);
		return {
			samples: state.netBenefits.length,
			...(expected === undefined ? {} : { expectedNetBenefitMs: expected }),
			consecutiveFailures: state.consecutiveFailures,
			suppressedDecisions: state.totalSuppressed,
		};
	}

	reset(): void {
		this.states.clear();
	}

	private probeDecision(
		state: GateState,
		policy: ForkBenefitGatePolicy,
		probeReason: "utility_probe" | "failure_probe",
		skipReason: "negative_utility" | "failure_circuit",
		base: Pick<ForkBenefitDecision, "samples" | "expectedNetBenefitMs">,
	): ForkBenefitDecision {
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
			netBenefits: [],
			consecutiveFailures: 0,
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

function mean(values: readonly number[]): number | undefined {
	return values.length ? values.reduce((total, value) => total + value, 0) / values.length : undefined;
}
