import { describe, expect, it } from "vitest";
import { DrafterUtilityGate } from "../src/drafter-utility-gate.ts";
import { BenefitGate, DEFAULT_BENEFIT_GATE_POLICY as POLICY, type BenefitObservation } from "../src/fork-benefit-gate.ts";

describe("fork benefit gate", () => {
	it("keeps censored hit benefit unknown and charges only Actor-visible adoption latency", () => {
		for (const expectedActorMs of [undefined, 50, 300]) {
			const gate = new DrafterUtilityGate();
			gate.finish(gate.start("drafter", true));
			expect(gate.snapshot().samples).toBe(0);
			for (let index = 0; index < 4; index++) {
				const batch = gate.start("drafter", true);
				expect(batch.allowed).toBe(true);
				gate.requestStarted(batch); gate.requestStarted(batch);
				gate.requestSettled(batch); gate.finish(batch);
				expect(gate.snapshot().samples).toBe(index);
				gate.requestSettled(batch);
				gate.requestStarted(batch); gate.requestSettled(batch); // A late continuation runs beside the Actor.
				gate.creditAdoption(batch, { hitLatencyMs: 100, expectedActorMs });
				expect(gate.snapshot().samples).toBe(index + 1);
				expect(gate.snapshot().expectedNetBenefitMs).toBe(expectedActorMs === undefined ? undefined : expectedActorMs - 100);
			}
			expect(gate.start("drafter", true).allowed).toBe(expectedActorMs === 300);
			if (expectedActorMs === undefined) {
				for (let index = 0, probes = 0; index < 128 && probes < 4; index++) {
					const missed = gate.start("drafter", true);
					if (!missed.allowed) continue;
					probes++;
					gate.requestStarted(missed); gate.requestSettled(missed); gate.finish(missed);
				}
				expect(gate.start("drafter", true).allowed).toBe(false);
			}
		}
	});

	it.each([1, 4])("bounds unknown-benefit exploration with a %i-sample window and recovers from late evidence", windowSize => {
		const gate = new BenefitGate(), policy = { ...POLICY, windowSize }, decisions = [];
		const updates: ReturnType<BenefitGate["observe"]>[] = [];
		for (let index = 0; index < 40; index++) {
			const decision = gate.decide("unknown", policy); decisions.push(decision);
			if (decision.allowed) updates.push(gate.observe("unknown", { costMs: 20 }, policy));
		}
		expect(decisions.filter(decision => decision.reason === "warmup")).toHaveLength(4);
		expect(decisions.filter(decision => decision.allowed)).toHaveLength(7);
		expect(decisions.filter(decision => decision.reason === "calibration_probe")).toHaveLength(3);
		expect(gate.snapshot("unknown").expectedNetBenefitMs).toBeUndefined();
		for (const update of updates) update({ costMs: 20, benefitMs: 120 });
		expect(gate.decide("unknown", policy)).toMatchObject({ allowed: true, reason: "profitable", expectedNetBenefitMs: 100 });
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
		const probes = () => Array.from({ length: 32 }, () => gate.decide("utility", POLICY).reason).flatMap((reason, index) => reason === "utility_probe" ? [index + 1] : []);
		expect(probes()).toEqual([4, 12, 28]); // A probe into a still-negative window doubles the next wait.
		gate.observe("utility", sample(0, 1000), POLICY); gate.decide("utility", POLICY); // Profit restores the first interval.
		for (let index = 0; index < 4; index++) gate.observe("utility", sample(100, 0), POLICY);
		expect(probes().slice(0, 1)).toEqual([4]);

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
