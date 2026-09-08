import { afterEach, describe, expect, it, vi } from "vitest";
import {
	type CandidateJoinRequest,
	type PredictionForecast,
	type ServiceTimingIdentity,
	SpeculationScheduler,
	waitForCandidate,
} from "../src/scheduler.ts";

afterEach(() => vi.useRealTimers());

describe("SpeculationScheduler", () => {
	it.each(["resolve", "reject", "pre-abort", "abort", "deadline", "late resolve", "late reject"] as const)(
		"settles candidate waits on %s and cleans every losing path",
		async (winner) => {
			vi.useFakeTimers();
			const pending = deferred<number>();
			const controller = new AbortController();
			const remove = vi.spyOn(controller.signal, "removeEventListener");
			if (winner === "pre-abort") controller.abort();
			const waiting = waitForCandidate(pending.promise, controller.signal, 10);
			const failure = new Error("candidate boundary failed");
			if (winner === "resolve") pending.resolve(7);
			else if (winner === "reject") pending.reject(failure);
			else if (winner === "abort") controller.abort();
			else if (winner !== "pre-abort") await vi.advanceTimersByTimeAsync(10);

			if (winner === "reject") await expect(waiting).rejects.toBe(failure);
			else {
				const result = await waiting;
				expect(result).toEqual(
					winner === "resolve" ? { status: "completed", value: 7 } : { status: winner.includes("abort") ? "aborted" : "deadline" },
				);
			}
			if (winner === "pre-abort") pending.reject(failure);
			if (winner === "late resolve") pending.resolve(7);
			if (winner === "late reject") pending.reject(failure);
			await Promise.resolve();
			expect(vi.getTimerCount()).toBe(0);
			if (winner !== "pre-abort") expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
		},
	);

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
		const actionSpecific = forecast({
			expectedDurationMs: 500,
			decisionBatchesUntilCall: 3,
			actorPhase: { kind: "decision", elapsedMs: 20 },
		});
		expect(scheduler.evaluate([actionSpecific]).expectedDurationMs).toBe(500);
		expect(scheduler.launchDelay(actionSpecific, 10)).toBe(0);
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

	it("separates producer, consumer, and adoption work while retaining exact/class quantiles and bounded history", () => {
		const scheduler = new SpeculationScheduler<object>();
		const identity = { tool: "bash", executionFingerprint: "linux-world:v1", actionKeyHash: "producer" };
		const actorIdentity = { ...identity, actionKeyHash: "consumer" };
		const exact = { ...actorIdentity, operation: "route:exact" }, inputs = { ...actorIdentity, operation: "route:inputs" };
		scheduler.observeActorService(identity, 380);
		scheduler.observeAdoption(identity, 70);
		for (const duration of [40, 50, 90]) scheduler.observeSpeculativeService(identity, duration);
		for (const duration of [100, 110, 120]) scheduler.observeActorService(actorIdentity, duration);
		for (const duration of [5, 10, 20]) scheduler.observeAdoption(exact, duration);
		for (const duration of [150, 160, 200]) scheduler.observeAdoption(inputs, duration);
		expect(scheduler.evaluate([forecast({ ...identity, expectedDurationMs: 1 })]).expectedDurationMs).toBe(50);
		expect(joinDecision(scheduler, identity)).toMatchObject({ expectedRemainingMs: 90, expectedActorMs: 380, expectedAdoptionMs: 70 });
		for (const [adoptionIdentity, expectedAdoptionMs] of [[exact, 20], [inputs, 200]] as const) {
			expect(joinDecision(scheduler, identity, { actorIdentity, adoptionIdentity, state: "succeeded" })).toMatchObject({
				allowed: expectedAdoptionMs < 100, expectedActorMs: 100, expectedAdoptionMs, expectedNetBenefitMs: 100 - expectedAdoptionMs,
			});
		}
		expect(joinDecision(scheduler, identity, { actorIdentity, adoptionIdentity: exact })).toMatchObject({
			allowed: false, expectedActorMs: 100, expectedRemainingMs: 90, expectedAdoptionMs: 20, expectedNetBenefitMs: -10,
		});
		for (const [adoptionIdentity, expectedAdoptionMs] of [
			[{ ...exact, actionKeyHash: "new pair" }, 20],
			[{ ...exact, operation: "other route:exact" }, 0],
			[{ ...exact, executionFingerprint: "other executor" }, 0],
		] as const) {
			expect(joinDecision(scheduler, identity, {
				actorIdentity: { ...actorIdentity, actionKeyHash: "new query" }, adoptionIdentity, state: "succeeded",
			})).toMatchObject({ expectedActorMs: 100, expectedAdoptionMs });
		}

		const tiny = new SpeculationScheduler<object>();
		tiny.observeActorService(identity, 30);
		tiny.observeAdoption(identity, 70);
		expect(joinDecision(tiny, identity, { state: "succeeded", expectedSpeculativeDurationMs: 920 }))
			.toMatchObject({ allowed: false, reason: "fallback_faster", expectedNetBenefitMs: -40 });
		for (let index = 0; index < 1100; index++) {
			const newer = { ...identity, executionFingerprint: `world-${index}` };
			scheduler.observeActorService(newer, 1);
			scheduler.observeSpeculativeService(newer, 2);
			scheduler.observeAdoption(newer, 3);
		}
		expect(joinDecision(scheduler, identity)).toMatchObject({ actorSamples: 0, speculativeSamples: 0, adoptionSamples: 0 });
	});

	it("uses measured net latency to retain heavy hits and reject noise-boundary waits", () => {
		const heavy = new SpeculationScheduler<object>();
		const identity = { tool: "bash", executionFingerprint: "linux-world:v1" };
		heavy.observeActorService(identity, 2_687);
		heavy.observeSpeculativeService(identity, 936);
		heavy.observeAdoption(identity, 70);
		const profitable = joinDecision(heavy, identity, { expectedSpeculativeDurationMs: 2_687 });
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
		expect(joinDecision(boundary, identity, { expectedSpeculativeDurationMs: 994 })).toMatchObject({
			allowed: false,
			reason: "fallback_faster",
			expectedNetBenefitMs: 21,
		});
	});

	it("bounds an uncalibrated join while wider timing classes transfer across exact actions", () => {
		const scheduler = new SpeculationScheduler<object>({
			candidateJoinPolicy: { warmupWaitMs: 17 },
		});
		const first = { tool: "bash", executionFingerprint: "linux-world:v1", actionKeyHash: "parent-a" };
		const second = { ...first, actionKeyHash: "parent-b" };
		expect(joinDecision(scheduler, first)).toMatchObject({
			allowed: true,
			reason: "warmup_probe",
			waitBudgetMs: Number.POSITIVE_INFINITY,
			actorSamples: 0,
		});
		scheduler.observeActorService(first, 100);
		expect(joinDecision(scheduler, first)).toMatchObject({ allowed: true, reason: "warmup_probe", waitBudgetMs: 18.25 });
		expect(joinDecision(scheduler, first, { actorElapsedMs: 80 }))
			.toMatchObject({ allowed: false, reason: "fallback_faster", expectedNetBenefitMs: 19 });

		const cold = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
		cold.observeSpeculativeService(first, 900);
		expect(joinDecision(cold, second)).toMatchObject({ allowed: false, reason: "warmup_probe", actorSamples: 0 });
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

	it("selects unreserved victims but accounts for them until completion, including Actor over-budget work", () => {
		for (const joined of [false, true]) {
			const scheduler = new SpeculationScheduler<object>(), near = {}, far = {}, next = {}, actor = {};
			scheduler.admit(near, [forecast({ decisionBatchesUntilCall: 1, criticalPathMs: 500 })], 2);
			scheduler.admit(far, [forecast({ decisionBatchesUntilCall: 4, criticalPathMs: 10 })], 2);
			const victim = joined ? near : far;
			expect(scheduler.preemptFor({ class: "filesystem", units: 1 }, 2, (job) => !joined || job !== far)).toEqual([victim]);
			expect(scheduler.snapshot().map((entry) => entry.job)).toEqual([near, far]);
			expect(scheduler.admit(next, [forecast()], 2).admitted).toBe(false);
			scheduler.complete(victim);
			expect(scheduler.admit(next, [forecast()], 2).admitted).toBe(true);
			expect(scheduler.admit(actor, [forecast({ resourceDemand: 2 })], 2, "actor").admitted).toBe(true);
			expect(scheduler.snapshot().map((entry) => entry.job)).toContain(actor);
			expect(scheduler.admit({}, [forecast()], 2).admitted).toBe(false);
			scheduler.complete(actor);
		}
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

function joinDecision(
	scheduler: SpeculationScheduler<object>,
	identity: ServiceTimingIdentity,
	overrides: Partial<Omit<CandidateJoinRequest, "identity">> = {},
) {
	return scheduler.assessCandidateJoin({
		identity,
		state: "running",
		expectedSpeculativeDurationMs: 1,
		...overrides,
	});
}

function deferred<Value>() {
	let resolve!: (value: Value) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}
