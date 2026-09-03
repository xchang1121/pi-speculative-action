import { describe, expect, it } from "vitest";
import { type PredictionForecast, SpeculationScheduler } from "../src/scheduler.ts";

describe("SpeculationScheduler", () => {
	it("does not evict foreground work during ordinary admission", () => {
		const scheduler = new SpeculationScheduler<object>();
		const first = {};
		const second = {};
		expect(scheduler.admit(first, [forecast()], 1).admitted).toBe(true);
		expect(scheduler.admit(second, [forecast({ expectedDurationMs: 500 })], 1)).toMatchObject({
			admitted: false,
			reason: "budget_exhausted",
		});
		expect(scheduler.snapshot().map((entry) => entry.job)).toEqual([first]);
	});

	it("merges duplicate K(a) forecasts without source-count inflation", () => {
		const scheduler = new SpeculationScheduler<object>();
		const one = scheduler.evaluate([
			forecast({ expectedDurationMs: 100, decisionBatchesUntilCall: 3, criticalPathMs: 120 }),
		]);
		const duplicate = scheduler.evaluate([
			forecast({ expectedDurationMs: 100, decisionBatchesUntilCall: 3, criticalPathMs: 120 }),
			forecast({ expectedDurationMs: 80, decisionBatchesUntilCall: 4, criticalPathMs: 100 }),
		]);
		expect(duplicate).toEqual(one);
	});

	it("separates the current Actor decision from later tool cycles", () => {
		const scheduler = new SpeculationScheduler<object>();
		scheduler.observeActorTiming(40, 80);
		scheduler.observeActorTiming(50, 100);
		scheduler.observeActorTiming(60, 120);
		scheduler.observeActorTiming(70, 200);
		for (const duration of [20, 40, 60, 100]) scheduler.observeService("read", duration);
		expect(
			scheduler.launchDelay(
				forecast({
					decisionBatchesUntilCall: 3,
					actorPhase: { kind: "decision", elapsedMs: 20 },
					expectedDurationMs: 40,
				}),
				10,
			),
		).toBe(110);
		expect(
			scheduler.launchDelay(
				forecast({ decisionBatchesUntilCall: 3, actorPhase: { kind: "cycle", elapsedMs: 20 } }),
				10,
			),
		).toBe(150);
		expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 3 }), 10)).toBe(170);
		expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 3, dependenciesResolved: true }), 10)).toBe(0);
		expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 1 }))).toBe(0);
	});

	it("prioritizes explicit expected latency benefit without requiring it from every source", () => {
		const scheduler = new SpeculationScheduler<object>();
		const unlikelyLong = {};
		const likelyShort = {};
		scheduler.admit(unlikelyLong, [forecast({ expectedDurationMs: 500, expectedLatencyBenefitMs: 10 })], 2);
		scheduler.admit(likelyShort, [forecast({ expectedDurationMs: 50, expectedLatencyBenefitMs: 40 })], 2);

		expect(scheduler.preemptFor({ class: "filesystem", units: 1 }, 2)).toEqual([unlikelyLong]);
		expect(scheduler.evaluate([forecast({ expectedDurationMs: 50 })])).toMatchObject({
			criticalPathMs: 50,
			priorityMs: 50,
		});
	});

	it("caps expected benefit by observed Actor runway without inventing cold-start timing", () => {
		const scheduler = new SpeculationScheduler<object>();
		const long = forecast({
			expectedDurationMs: 1_000,
			expectedLatencyBenefitMs: 240,
			actorPhase: { kind: "decision", elapsedMs: 0 },
		});
		expect(scheduler.evaluate([long]).priorityMs).toBe(240);

		scheduler.observeActorTiming(20);
		scheduler.observeService("read", 10);
		const short = forecast({
			expectedDurationMs: 80,
			expectedLatencyBenefitMs: 40,
			actorPhase: { kind: "decision", elapsedMs: 0 },
		});
		expect(scheduler.evaluate([long]).priorityMs).toBeCloseTo(4.8);
		expect(scheduler.evaluate([short]).priorityMs).toBe(10);
		expect(scheduler.evaluate([{ ...short, actorPhase: { kind: "cycle", elapsedMs: 500 } }]).priorityMs).toBe(10);
		const { actorPhase: _, ...withoutPhase } = long;
		expect(scheduler.evaluate([withoutPhase]).priorityMs).toBe(240);
	});

	it("keeps Actor, speculative-world, and adoption timing distributions separate", () => {
		const scheduler = new SpeculationScheduler<object>();
		const identity = {
			tool: "bash",
			executionFingerprint: "linux-world:v1",
			actionKeyHash: "action-a",
		};
		scheduler.observeActorService(identity, 380);
		scheduler.observeSpeculativeService(identity, 920);
		scheduler.observeAdoption(identity, 70);

		expect(
			scheduler.evaluate([
				forecast({
					tool: "bash",
					executionFingerprint: identity.executionFingerprint,
					actionKeyHash: identity.actionKeyHash,
					expectedDurationMs: 380,
				}),
			]),
		).toMatchObject({ expectedDurationMs: 920 });
		expect(
			scheduler.assessCandidateJoin({
				identity,
				state: "running",
				expectedSpeculativeDurationMs: 920,
				elapsedMs: 100,
			}),
		).toMatchObject({
			allowed: false,
			reason: "fallback_faster",
			expectedRemainingMs: 820,
			expectedActorMs: 380,
			expectedAdoptionMs: 70,
		});
		expect(
			scheduler.assessCandidateJoin({
				identity,
				state: "succeeded",
				expectedSpeculativeDurationMs: 920,
			}),
		).toMatchObject({ allowed: true, reason: "ready" });

		const tiny = new SpeculationScheduler<object>();
		tiny.observeActorService(identity, 30);
		tiny.observeAdoption(identity, 70);
		expect(
			tiny.assessCandidateJoin({
				identity,
				state: "succeeded",
				expectedSpeculativeDurationMs: 920,
			}),
		).toMatchObject({ allowed: false, reason: "fallback_faster", expectedNetBenefitMs: -40 });
	});

	it("uses measured net latency to retain heavy hits and reject noise-boundary waits", () => {
		const heavy = new SpeculationScheduler<object>();
		const identity = { tool: "bash", executionFingerprint: "linux-world:v1" };
		heavy.observeActorService(identity, 2_687);
		heavy.observeSpeculativeService(identity, 936);
		heavy.observeAdoption(identity, 70);
		const profitable = heavy.assessCandidateJoin({
			identity,
			state: "running",
			expectedSpeculativeDurationMs: 2_687,
			elapsedMs: 0,
		});
		expect(profitable).toMatchObject({
			allowed: true,
			reason: "profitable",
			expectedRemainingMs: 936,
			expectedNetBenefitMs: 1_681,
		});
		expect(profitable.waitBudgetMs).toBeGreaterThan(936);

		const boundary = new SpeculationScheduler<object>();
		boundary.observeActorService(identity, 994);
		boundary.observeSpeculativeService(identity, 973);
		expect(
			boundary.assessCandidateJoin({
				identity,
				state: "running",
				expectedSpeculativeDurationMs: 994,
			}),
		).toMatchObject({
			allowed: false,
			reason: "fallback_faster",
			expectedNetBenefitMs: 21,
		});
	});

	it("uses upper empirical speculative and adoption quantiles against a lower Actor quantile", () => {
		const scheduler = new SpeculationScheduler<object>();
		const identity = { tool: "bash", executionFingerprint: "linux-world:v1" };
		for (const duration of [100, 110, 120]) scheduler.observeActorService(identity, duration);
		for (const duration of [40, 50, 90]) scheduler.observeSpeculativeService(identity, duration);
		for (const duration of [5, 10, 20]) scheduler.observeAdoption(identity, duration);

		expect(
			scheduler.assessCandidateJoin({
				identity,
				state: "running",
				expectedSpeculativeDurationMs: 50,
			}),
		).toMatchObject({
			allowed: false,
			reason: "fallback_faster",
			expectedActorMs: 100,
			expectedRemainingMs: 90,
			expectedAdoptionMs: 20,
			expectedNetBenefitMs: -10,
		});
	});

	it("bounds an uncalibrated join while wider timing classes transfer across exact actions", () => {
		const scheduler = new SpeculationScheduler<object>({
			candidateJoinPolicy: { warmupWaitMs: 17 },
		});
		const first = { tool: "bash", executionFingerprint: "linux-world:v1", actionKeyHash: "parent-a" };
		const second = { ...first, actionKeyHash: "parent-b" };
		expect(
			scheduler.assessCandidateJoin({
				identity: first,
				state: "running",
				expectedSpeculativeDurationMs: 1,
			}),
		).toMatchObject({
			allowed: true,
			reason: "warmup_probe",
			waitBudgetMs: Number.POSITIVE_INFINITY,
			actorSamples: 0,
		});
		scheduler.observeActorService(first, 100);
		expect(
			scheduler.assessCandidateJoin({
				identity: first,
				state: "running",
				expectedSpeculativeDurationMs: 1,
			}),
		).toMatchObject({ allowed: true, reason: "warmup_probe", waitBudgetMs: 18.25 });
		expect(
			scheduler.assessCandidateJoin({
				identity: first,
				state: "running",
				expectedSpeculativeDurationMs: 1,
				actorElapsedMs: 80,
			}),
		).toMatchObject({ allowed: false, reason: "fallback_faster", expectedNetBenefitMs: 19 });

		const cold = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
		cold.observeSpeculativeService(first, 900);
		expect(
			cold.assessCandidateJoin({
				identity: second,
				state: "running",
				expectedSpeculativeDurationMs: 1,
			}),
		).toMatchObject({ allowed: false, reason: "warmup_probe", actorSamples: 0 });
		expect(
			cold.evaluate([
				forecast({
					tool: "bash",
					executionFingerprint: second.executionFingerprint,
					actionKeyHash: second.actionKeyHash,
					expectedDurationMs: 200,
				}),
			]),
		).toMatchObject({ expectedDurationMs: 900 });
	});

	it("promotes shared work on foreground evidence and lets background work yield", () => {
		const scheduler = new SpeculationScheduler<object>();
		expect(scheduler.evaluate([forecast({ background: true }), forecast({ background: true })]).background).toBe(
			true,
		);
		expect(scheduler.evaluate([forecast({ background: true }), forecast()]).background).toBe(false);

		const foreground = {};
		const background = {};
		scheduler.admit(foreground, [forecast({ expectedLatencyBenefitMs: 1 })], 2);
		scheduler.admit(background, [forecast({ background: true, expectedLatencyBenefitMs: 1_000 })], 2);
		expect(scheduler.preemptFor({ class: "filesystem", units: 1 }, 2, (job) => job === background)).toEqual([
			background,
		]);
	});

	it("preempts the latest and least critical work for an authoritative action", () => {
		const scheduler = new SpeculationScheduler<object>();
		const nearCritical = {};
		const farNoncritical = {};
		scheduler.admit(nearCritical, [forecast({ decisionBatchesUntilCall: 1, criticalPathMs: 500 })], 2);
		scheduler.admit(farNoncritical, [forecast({ decisionBatchesUntilCall: 4, criticalPathMs: 10 })], 2);
		expect(scheduler.preemptFor({ class: "filesystem", units: 1 }, 2)).toEqual([farNoncritical]);
		expect(scheduler.snapshot().map((entry) => entry.job)).toEqual([nearCritical]);
	});

	it("does not preempt a candidate already joined by an Actor", () => {
		const scheduler = new SpeculationScheduler<object>();
		const joined = {};
		const idle = {};
		scheduler.admit(joined, [forecast({ decisionBatchesUntilCall: 5 })], 2);
		scheduler.admit(idle, [forecast({ decisionBatchesUntilCall: 1 })], 2);

		expect(scheduler.preemptFor({ class: "filesystem", units: 1 }, 2, (candidate) => candidate !== joined)).toEqual([
			idle,
		]);
		expect(scheduler.snapshot().map((entry) => entry.job)).toEqual([joined]);
	});

	it("accepts world effects only when backend evidence matches the Actor execution world", () => {
		const scheduler = new SpeculationScheduler<object>();
		expect(
			scheduler.assessCompatibility(
				{ status: "compatible", backend: "native", executionFingerprint: "world-a" },
				"world-a",
			),
		).toEqual({ compatible: true });
		expect(
			scheduler.assessCompatibility(
				{ status: "compatible", backend: "native", executionFingerprint: "world-a" },
				"world-b",
			),
		).toEqual({ compatible: false, code: "execution_fingerprint_changed" });
		expect(
			scheduler.assessCompatibility(
				{ status: "indeterminate", backend: "native", code: "attestation_missing" },
				"world-a",
			),
		).toEqual({ compatible: false, code: "backend_indeterminate", detail: "attestation_missing" });
	});
});

function forecast(overrides: Partial<PredictionForecast> = {}): PredictionForecast {
	return {
		tool: "read",
		execution: "resource_snapshot",
		expectedDurationMs: 50,
		decisionBatchesUntilCall: 1,
		...overrides,
	};
}
