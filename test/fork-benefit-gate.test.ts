import { describe, expect, it } from "vitest";
import { DrafterTaskBudget } from "../src/drafter-budget.ts";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { deferred } from "./async.ts";
import { testModel } from "./model.ts";
import { BenefitGate, DEFAULT_BENEFIT_GATE_POLICY as POLICY, type BenefitObservation } from "../src/fork-benefit-gate.ts";

describe("fork benefit gate", () => {
	it("amends one request sample with measured reuse while charging adoption cost separately", async () => {
		for (const reusedExecutionMs of [0, 50, 300]) {
			const budget = new DrafterTaskBudget(), batch = budget.start("drafter", true), pending = deferred<ReturnType<typeof fauxAssistantMessage>>();
			batch.expectedBenefitMs = 1000; // Measured workflow hints may justify overlap before the final Actor outcome arrives.
			const request = { model: testModel(), context: { messages: [] }, policy: { drafterTaskMaxRequests: 1000, drafterTaskMaxTokens: 1000000 },
				options: { maxTokens: 80 }, complete: async () => fauxAssistantMessage([]) };
			budget.finish(budget.start("unused", true)); expect(budget.utilitySnapshot().samples).toBe(0);
			const first = budget.run({ ...request, utility: batch }), second = budget.run({ ...request, utility: batch, complete: () => pending.promise });
			await first; budget.finish(batch); expect(batch.update).toBeUndefined();
			await budget.run({ ...request, utility: batch }); // A continuation after turn closure runs beside the Actor.
			pending.resolve(fauxAssistantMessage([])); await second;
			budget.credit([batch, batch], { reusedExecutionMs, costMs: 100 });
			budget.start("drafter", true);
			expect(budget.utilitySnapshot()).toMatchObject({ samples: 1, expectedNetBenefitMs: reusedExecutionMs - 100 });
			expect(batch).toMatchObject({ benefitMs: reusedExecutionMs, costMs: 100 });
			expect(batch.startedRequests).toBe(3); expect(batch.pendingRequests).toBe(0);
			expect(Boolean(await budget.run({ ...request, utility: budget.start("drafter", true) }))).toBe(reusedExecutionMs === 300);
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
