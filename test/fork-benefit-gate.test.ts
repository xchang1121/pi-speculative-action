import { describe, expect, it } from "vitest";
import { DrafterUtilityGate } from "../src/drafter-utility-gate.ts";
import {
	BenefitGate,
	type BenefitGatePolicy,
	type BenefitObservation,
} from "../src/fork-benefit-gate.ts";

const POLICY: BenefitGatePolicy = {
	enabled: true,
	minSamples: 4,
	windowSize: 4,
	minNetBenefitMs: 25,
	probeInterval: 4,
	failureThreshold: 2,
};

describe("fork benefit gate", () => {
	it("charges request and adoption costs against calibrated Actor service, including late continuations", () => {
		for (const expectedActorMs of [undefined, 50, 300]) {
			const gate = new DrafterUtilityGate();
			gate.finish(gate.start("drafter", true));
			expect(gate.snapshot().samples).toBe(0);
			for (let index = 0; index < 4; index++) {
				const batch = gate.start("drafter", true);
				expect(batch.allowed).toBe(true);
				gate.requestStarted(batch); gate.requestStarted(batch);
				gate.requestSettled(batch, 10); gate.finish(batch);
				expect(gate.snapshot().samples).toBe(index);
				gate.requestSettled(batch, 10);
				gate.requestStarted(batch); gate.requestSettled(batch, 130);
				gate.creditAdoption(batch, { executionAheadMs: 10000, attemptLeadMs: 20000, hitLatencyMs: 100, expectedActorMs });
				expect(gate.snapshot()).toMatchObject({ samples: index + 1, expectedNetBenefitMs: (expectedActorMs ?? 0) - 250 });
			}
			expect(gate.start("drafter", true).allowed).toBe(expectedActorMs === 300);
		}
	});

	it("keeps profitable forks and suppresses a negative rolling window", () => {
		const gate = new BenefitGate();
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
		const gate = new BenefitGate();
		for (let index = 0; index < 4; index++) gate.observe("utility", sample(100, 0), POLICY);
		expect([1, 2, 3, 4].map(() => gate.decide("utility", POLICY).reason)).toEqual([
			"negative_utility",
			"negative_utility",
			"negative_utility",
			"utility_probe",
		]);

		const update = gate.observe("failure", sample(50, 0), POLICY);
		update({ ...sample(50, 0), failed: true });
		gate.observe("failure", { ...sample(50, 0), failed: true }, { ...POLICY, windowSize: 1 });
		expect(gate.decide("failure", POLICY)).toMatchObject({ allowed: false, reason: "failure_circuit" });
		update(sample(0, 500));
		expect(gate.snapshot("failure")).toMatchObject({ samples: 1, expectedNetBenefitMs: -50, consecutiveFailures: 2 });
		gate.reset(); update(sample(0, 500));
		expect(gate.snapshot("failure").samples).toBe(0);
	});

	it("isolates models and bypasses policy when disabled", () => {
		const gate = new BenefitGate();
		for (let index = 0; index < 4; index++) gate.observe("bad", sample(100, 0), POLICY);
		expect(gate.decide("bad", POLICY).allowed).toBe(false);
		expect(gate.decide("fresh", POLICY)).toMatchObject({ allowed: true, reason: "warmup" });
		expect(gate.decide("bad", { ...POLICY, enabled: false })).toMatchObject({ allowed: true, reason: "disabled" });
	});
});

function sample(costMs: number, benefitMs: number): BenefitObservation {
	return { costMs, benefitMs };
}
