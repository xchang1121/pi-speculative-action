import { describe, expect, it } from "vitest";
import {
	BenefitGate,
	ForkBenefitGate,
	type ForkBenefitGatePolicy,
	type ForkBenefitObservation,
} from "../src/fork-benefit-gate.ts";

const POLICY: ForkBenefitGatePolicy = {
	enabled: true,
	minSamples: 4,
	windowSize: 4,
	minNetBenefitMs: 25,
	probeInterval: 4,
	failureThreshold: 2,
};

describe("fork benefit gate", () => {
	it("shares the same policy with source-neutral cost and benefit metrics", () => {
		const gate = new BenefitGate();
		for (let index = 0; index < 4; index++) {
			expect(gate.decide("drafter", POLICY).allowed).toBe(true);
			gate.observe("drafter", { costMs: 100, benefitMs: 0 }, POLICY);
		}
		expect(gate.decide("drafter", POLICY)).toMatchObject({
			allowed: false,
			reason: "negative_utility",
			expectedNetBenefitMs: -100,
		});
	});

	it("keeps profitable forks and suppresses a negative rolling window", () => {
		const gate = new ForkBenefitGate();
		for (const observation of [sample(65, 396), sample(80, 0), sample(54, 402), sample(81, 0)]) {
			expect(gate.decide("model", POLICY).allowed).toBe(true);
			gate.observe("model", observation, POLICY);
		}
		expect(gate.decide("model", POLICY)).toMatchObject({ allowed: true, reason: "profitable" });
		gate.observe("model", sample(74, 0), POLICY);
		expect(gate.decide("model", POLICY).allowed).toBe(true);
		gate.observe("model", sample(112, 0), POLICY);
		expect(gate.decide("model", POLICY)).toMatchObject({ allowed: false, reason: "negative_utility" });
	});

	it("periodically probes negative utility and an unhealthy endpoint", () => {
		const gate = new ForkBenefitGate();
		for (let index = 0; index < 4; index++) gate.observe("utility", sample(100, 0), POLICY);
		expect([1, 2, 3, 4].map(() => gate.decide("utility", POLICY).reason)).toEqual([
			"negative_utility",
			"negative_utility",
			"negative_utility",
			"utility_probe",
		]);

		gate.observe("failure", { ...sample(50, 0), failed: true }, POLICY);
		gate.observe("failure", { ...sample(50, 0), failed: true }, POLICY);
		expect(gate.decide("failure", POLICY)).toMatchObject({ allowed: false, reason: "failure_circuit" });
	});

	it("isolates models and bypasses policy when disabled", () => {
		const gate = new ForkBenefitGate();
		for (let index = 0; index < 4; index++) gate.observe("bad", sample(100, 0), POLICY);
		expect(gate.decide("bad", POLICY).allowed).toBe(false);
		expect(gate.decide("fresh", POLICY)).toMatchObject({ allowed: true, reason: "warmup" });
		expect(gate.decide("bad", { ...POLICY, enabled: false })).toMatchObject({ allowed: true, reason: "disabled" });
	});
});

function sample(forkLatencyMs: number, exactLeadMs: number): ForkBenefitObservation {
	return { forkLatencyMs, exactLeadMs };
}
