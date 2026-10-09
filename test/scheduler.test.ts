import { deferred } from "./async.ts";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_BENEFIT_GATE_POLICY } from "../src/fork-benefit-gate.ts";
import { type CandidateJoinRequest, type PredictionForecast, type ServiceTimingIdentity, SpeculationScheduler,
	waitForCandidate } from "../src/scheduler.ts";

afterEach(() => vi.useRealTimers());

describe("SpeculationScheduler", () => {
	it("scopes failed work to its producer and recovers retained candidates without inflating dispatch probes", () => {
		const scheduler = new SpeculationScheduler<object>(), retained = {};
		const identity = { tool: "read", executionFingerprint: "backend", actionKeyHash: "producer" };
		const consumers = ["query-a", "query-b"].map(actionKeyHash => forecast({ ...identity, actionKeyHash }));
		const admit = (job: object, executionIdentity: ServiceTimingIdentity | undefined = identity, role: "producer" | "actor" = "producer", capacity = 8) =>
			scheduler.admit(job, consumers, capacity, role, undefined, executionIdentity);
		for (const duration of [80, 160]) scheduler.observeSpeculativeService(identity, duration);
		for (let failure = 0; failure < DEFAULT_BENEFIT_GATE_POLICY.failureThreshold; failure++)
			scheduler.observeSpeculativeService(identity, 1, true);
		expect(scheduler.evaluate([forecast(identity)]).expectedDurationMs).toBe(80);
		const probes: boolean[] = [];
		for (let decision = 1; decision <= 3 * DEFAULT_BENEFIT_GATE_POLICY.probeInterval; decision++) {
			scheduler.observeActorTiming(50, 50);
			const allowed = admit(retained).admitted;
			probes.push(allowed); scheduler.complete(retained);
			for (let dispatch = 0; dispatch < 12; dispatch++) {
				expect(admit(retained).admitted).toBe(allowed);
				scheduler.complete(retained);
			}
		}
		expect(probes).toEqual(probes.map((_, index) => (index + 1) % DEFAULT_BENEFIT_GATE_POLICY.probeInterval === 0));
		for (const other of [{ ...identity, actionKeyHash: "other" }, { ...identity, executionFingerprint: "other" },
			{ ...identity, actionKeyHash: undefined }]) {
			const job = {};
			expect(admit(job, other).admitted).toBe(true); scheduler.complete(job);
		}
		const actor = {}, preview = {}, blocked = {};
		expect(admit(actor, identity, "actor", 1).admitted).toBe(true);
		expect(admit(blocked, identity, "producer", 1)).toMatchObject({ reason: "failure_circuit" }); // Never budget_exhausted, which preempts.
		expect(scheduler.admit(preview, consumers, 1)).toMatchObject({ admitted: false, reason: "budget_exhausted" });
		scheduler.complete(actor);
		expect(scheduler.admit(preview, consumers, 1).admitted).toBe(true); scheduler.complete(preview);
		expect(admit(blocked)).toMatchObject({ admitted: false, reason: "failure_circuit" });
		scheduler.observeSpeculativeService(identity, 0);
		expect(admit(blocked).admitted).toBe(true); scheduler.complete(blocked);
		expect(scheduler.evaluate([forecast(identity)]).expectedDurationMs).toBe(80);
		expect(scheduler.snapshot()).toEqual([]);
	});

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

	it.each([2, 131_072])("merges %i duplicate K(a) forecasts without source-count inflation", (count) => {
		const scheduler = new SpeculationScheduler<object>();
		const one = scheduler.evaluate([
			forecast({ expectedDurationMs: 100, decisionBatchesUntilCall: 3, criticalPathMs: 120, resourceDemand: 2 }),
		]);
		const duplicate = scheduler.evaluate([
			forecast({ expectedDurationMs: 100, decisionBatchesUntilCall: 3, criticalPathMs: 120, resourceDemand: 2 }),
			...Array.from({ length: count - 1 }, () => forecast({ expectedDurationMs: 80, decisionBatchesUntilCall: 4, criticalPathMs: 100 })),
		]);
		expect(duplicate).toEqual({ ...one, resourceUnits: 2 });
		expect(scheduler.admit({}, [forecast({ resourceDemand: 2 })], 1).admitted).toBe(false);
	});

	it("weighs a model source's work by its calibrated hit probability unless it states a benefit", () => {
		const scheduler = new SpeculationScheduler<object>(), base = { expectedDurationMs: 100, criticalPathMs: 100 };
		expect(scheduler.evaluate([forecast(base)]).priorityMs).toBe(100);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.25 })]).priorityMs).toBe(25);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.5, adoptionProbability: 0.2 })]).priorityMs).toBeCloseTo(10);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.5, adoptionProbability: 0 })]).priorityMs).toBe(0);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.5, adoptionProbability: 0.2, expectedLatencyBenefitMs: 60 })]).priorityMs).toBe(60);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.25, expectedLatencyBenefitMs: 60 })]).priorityMs).toBe(60);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.5 }), forecast({ ...base, hitProbability: 0.5 })]).priorityMs).toBe(50);
		expect(scheduler.evaluate([forecast({ ...base, hitProbability: 0.5 }), forecast({ ...base, hitProbability: 0.5, decisionBatchesUntilCall: 2 })]).priorityMs).toBe(75);
	});

	it("queries measured action and timing-class value without inventing cold benefits or discounting source benefits twice", () => {
		const scheduler = new SpeculationScheduler<object>();
		const identity = { tool: "bash", semanticsEpoch: "shell", executionFingerprint: "world", actionKeyHash: "test" };
		const value = forecast({ ...identity, hitProbability: 0.5, adoptionProbability: 0.2 });
		expect(scheduler.measuredBenefitMs(value)).toBeUndefined();
		scheduler.observeActorService(identity, 1_000);
		expect(scheduler.measuredBenefitMs(value)).toBe(100);
		expect(scheduler.measuredBenefitMs({ ...value, expectedLatencyBenefitMs: 700 })).toBe(700);
		const other = { ...value, actionKeyHash: "other-test" };
		expect(scheduler.measuredBenefitMs(other)).toBe(100);
		scheduler.observeActorService(other, 2_000);
		expect(scheduler.measuredBenefitMs(other)).toBe(200);
		expect(scheduler.measuredBenefitMs(value)).toBe(100);
		for (const scope of [{ tool: "read" }, { semanticsEpoch: "changed" }, { executionFingerprint: "changed" }])
			expect(scheduler.measuredBenefitMs({ ...value, ...scope })).toBeUndefined();
		for (const probability of [-1, NaN, Infinity, 0])
			expect(scheduler.measuredBenefitMs({ ...value, hitProbability: probability })).toBe(0);
		expect(scheduler.measuredBenefitMs({ ...value, hitProbability: 3, adoptionProbability: undefined })).toBe(1_000);
		expect(scheduler.snapshot()).toEqual([]);
	});

	it("adds distinct downstream opportunities once while preserving prerequisite service cost and capacity", () => {
		const scheduler = new SpeculationScheduler<object>();
		const base = forecast({ expectedDurationMs: 10, criticalPathMs: 10, resourceDemand: 2, hitProbability: 0.5 });
		const without = scheduler.evaluate([base]);
		const shared = { opportunity: "future-call", expectedBenefitMs: 5_000 };
		const withWorkflow = scheduler.evaluate([
			{ ...base, downstreamBenefits: [shared, shared, { opportunity: "later-call", expectedBenefitMs: 30 }] },
			{ ...base, downstreamBenefits: [{ ...shared, expectedBenefitMs: 4_000 }, { opportunity: "invalid", expectedBenefitMs: NaN }] },
		]);
		expect(withWorkflow).toEqual({ ...without, priorityMs: 5_035 });
		const short = {}, independent = {}, enriched = { ...base, downstreamBenefits: [shared] };
		expect(scheduler.admit(short, [enriched], 1)).toMatchObject({ admitted: false, reason: "budget_exhausted" });
		expect(scheduler.admit(short, [enriched], 3).admitted).toBe(true);
		expect(scheduler.admit(independent, [forecast({ expectedDurationMs: 100 })], 3).admitted).toBe(true);
		expect(scheduler.preemptFor(1, 3)).toEqual([independent]);
		expect(scheduler.snapshot().find(entry => entry.job === short)?.work).toMatchObject({ expectedDurationMs: 10, resourceUnits: 2 });
	});

	it("does not count correlated forecasts as independent evidence or transfer their probabilities to longer work", () => {
		const scheduler = new SpeculationScheduler<object>();
		const base = { tool: "read", actionKeyHash: "query", expectedDurationMs: 100, hitProbability: 0.8 };
		for (const count of [1, 2, 128]) expect(scheduler.evaluate(Array.from({ length: count }, () => forecast(base))).priorityMs).toBe(80);
		const different = [forecast({ ...base, expectedDurationMs: 20 }), forecast({ ...base, hitProbability: 0.2 })];
		for (const rows of [different, [...different].reverse()]) expect(scheduler.evaluate(rows).priorityMs).toBeCloseTo(20);
		const future = forecast({ ...base, decisionBatchesUntilCall: 2 });
		expect(scheduler.evaluate([forecast(base), future]).priorityMs).toBeCloseTo(96);
		expect(scheduler.evaluate([forecast(base), forecast({ ...base, actionKeyHash: "other-query" })]).priorityMs).toBeCloseTo(96);
		const independent = [different[0]!, { ...different[1]!, actionKeyHash: "other-query" }];
		expect(scheduler.evaluate(independent).priorityMs).toBeCloseTo(32.8);
	});

	it("defers future work only from observed Actor timing and known service cost", () => {
		const scheduler = new SpeculationScheduler<object>();
		const future = forecast({ tool: "bash", decisionBatchesUntilCall: 2,
			actorPhase: { kind: "decision", elapsedMs: 20 } });
		for (const expectedDurationMs of [undefined, 0, -1, NaN, Infinity, 500])
			expect(scheduler.launchDelay({ ...future, expectedDurationMs })).toBe(0);
		for (const [decision, cycle] of [[40, 80], [50, 100], [60, 120], [70, 200]])
			scheduler.observeActorTiming(decision!, cycle);
		for (const expectedDurationMs of [undefined, 0, -1, NaN, Infinity])
			expect(scheduler.launchDelay({ ...future, expectedDurationMs })).toBe(0);
		scheduler.observeSpeculativeService(future, 30);
		expect(scheduler.launchDelay({ ...future, expectedDurationMs: undefined })).toBe(60);
		for (const duration of [20, 40, 60, 100]) scheduler.observeSpeculativeService({ tool: "read" }, duration);
		for (const [phase, expected] of [
			[{ actorPhase: { kind: "decision", elapsedMs: 20 }, expectedDurationMs: 40 }, 110],
			[{ actorPhase: { kind: "cycle", elapsedMs: 20 } }, 150],
			[{}, 170],
		] as const) expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 3, ...phase }), 10)).toBe(expected);
		expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 3, dependenciesResolved: true }), 10)).toBe(0);
		expect(scheduler.launchDelay(forecast({ decisionBatchesUntilCall: 1 }))).toBe(0);
		const actionSpecific = { ...future, expectedDurationMs: 500 };
		expect(scheduler.evaluate([actionSpecific]).expectedDurationMs).toBe(500);
		expect(scheduler.launchDelay(actionSpecific, 10)).toBe(0);
	});

	it("prioritizes explicit expected latency benefit without requiring it from every source", () => {
		const scheduler = new SpeculationScheduler<object>();
		const unlikelyLong = {};
		const likelyShort = {};
		scheduler.admit(unlikelyLong, [forecast({ expectedDurationMs: 500, expectedLatencyBenefitMs: 10 })], 2);
		scheduler.admit(likelyShort, [forecast({ expectedDurationMs: 50, expectedLatencyBenefitMs: 40 })], 2);

		expect(scheduler.preemptFor(1, 2)).toEqual([unlikelyLong]);
		expect(scheduler.evaluate([forecast({ expectedDurationMs: 50 })])).toMatchObject({ criticalPathMs: 50, priorityMs: 50 });
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
		scheduler.observeSpeculativeService({ tool: "read" }, 10);
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

	it("always validates ready work without requesting fallback calibration", () => {
		const scheduler = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
		const identity = { tool: "read", actionKeyHash: "query" };
		for (let index = 0; index < 32; index++) {
			if (index > 8) scheduler.observeActorService(identity, 1);
			expect(joinDecision(scheduler, identity, { state: "succeeded" })).toMatchObject({ allowed: true, reason: "ready", waitBudgetMs: 0 });
		}
	});

	it("uses producer completion evidence for finite joins independently of Actor service", () => {
		const identity = { tool: "bash", executionFingerprint: "world", actionKeyHash: "long" };
		for (const actorMs of [undefined, 1, 100_000]) {
			const scheduler = new SpeculationScheduler<object>();
			if (actorMs !== undefined) scheduler.observeActorService(identity, actorMs);
			for (const duration of [800, 900, 1000]) scheduler.observeSpeculativeService(identity, duration);
			expect(joinDecision(scheduler, identity, { elapsedMs: 800, expectedSpeculativeDurationMs: undefined }))
				.toMatchObject({ allowed: true, reason: "waiting", waitBudgetMs: 275 });
			expect(joinDecision(scheduler, identity, { elapsedMs: 1200, expectedSpeculativeDurationMs: undefined }))
				.toMatchObject({ allowed: true, waitBudgetMs: 25 });
			expect(scheduler.evaluate([forecast({ ...identity, expectedDurationMs: 1 })]).expectedDurationMs).toBe(900);
			const job = {};
			scheduler.observeActorTiming(100, 1100);
			expect(scheduler.admit(job, [forecast({ ...identity, expectedDurationMs: 1500,
				actorPhase: { kind: "cycle", elapsedMs: 0 }, decisionBatchesUntilCall: 2 })], 1).admitted).toBe(true);
			expect(scheduler.snapshot().map(entry => entry.job)).toEqual([job]);
		}
	});

	it("keeps cancelled runs as scheduling floors without treating them as completed producers", () => {
		const scheduler = new SpeculationScheduler<object>(), identity = { tool: "bash", actionKeyHash: "cancelled" };
		for (const duration of [3000, 4000]) scheduler.observeSpeculativeService(identity, duration, "cancelled");
		for (let index = 0; index < 12; index++) expect(joinDecision(scheduler, identity, { expectedSpeculativeDurationMs: undefined, elapsedMs: 4500 }))
			.toMatchObject({ allowed: true, waitBudgetMs: 25 });
		expect(scheduler.evaluate([forecast(identity)]).expectedDurationMs).toBe(4000);
		scheduler.observeSpeculativeService(identity, 100);
		expect(joinDecision(scheduler, identity)).toMatchObject({ waitBudgetMs: 150 });
		for (let index = 0; index < 1100; index++) scheduler.observeSpeculativeService({ ...identity, executionFingerprint: `world-${index}` }, 2);
		expect(joinDecision(scheduler, identity, { expectedSpeculativeDurationMs: undefined })).toMatchObject({ waitBudgetMs: 25 });
	});

	it("keeps a long action's forecast and elapsed time above a short-command timing class", () => {
		const scheduler = new SpeculationScheduler<object>(), ls = { tool: "bash", executionFingerprint: "linux-world", actionKeyHash: "ls" };
		for (const duration of [202, 204, 200, 206]) scheduler.observeSpeculativeService(ls, duration);
		for (const duration of [20.8, 20.1, 20.1, 20.1]) scheduler.observeActorService({ ...ls, actionKeyHash: "git-status" }, duration);
		const npmTest = { ...ls, actionKeyHash: "npm-test" }, running = { state: "running" as const, elapsedMs: 616 };
		expect(joinDecision(scheduler, npmTest, { ...running, expectedSpeculativeDurationMs: 2000 })).toMatchObject({ allowed: true, waitBudgetMs: 1755 });
		expect(joinDecision(scheduler, npmTest, { ...running, expectedSpeculativeDurationMs: undefined })).toMatchObject({ allowed: true, reason: "waiting", waitBudgetMs: 25 });
	});

	it("combines distinct Actor opportunities without duplicating correlated source evidence", () => {
		const scheduler = new SpeculationScheduler<object>(), half = forecast({ expectedDurationMs: 100, expectedLatencyBenefitMs: 50 });
		expect([scheduler.evaluate([half]).priorityMs, scheduler.evaluate([half, { ...half, decisionBatchesUntilCall: 2 }]).priorityMs, scheduler.evaluate([half, forecast({ expectedDurationMs: 100 })]).priorityMs]).toEqual([50, 75, 100]);
	});

	it("uses a configurable cold deadline without borrowing unrelated Actor or producer timings", () => {
		const scheduler = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 17 } });
		const first = { tool: "bash", executionFingerprint: "world", actionKeyHash: "first" }, second = { ...first, actionKeyHash: "second" };
		scheduler.observeActorService(first, 510_000);
		scheduler.observeSpeculativeService(first, 900);
		expect(joinDecision(scheduler, second, { expectedSpeculativeDurationMs: undefined, elapsedMs: 400_000 }))
			.toMatchObject({ allowed: true, reason: "waiting", waitBudgetMs: 17 });
		const stopped = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
		expect(joinDecision(stopped, second, { expectedSpeculativeDurationMs: undefined })).toMatchObject({ allowed: false, reason: "deadline", waitBudgetMs: 0 });
		expect(joinDecision(stopped, second, { state: "succeeded" })).toMatchObject({ allowed: true, reason: "ready" });
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
		expect(scheduler.preemptFor(1, 2, (job) => job === background)).toEqual([
			background,
		]);
	});

	it("selects unreserved victims but accounts for them until completion, including Actor over-budget work", () => {
		for (const joined of [false, true]) {
			const scheduler = new SpeculationScheduler<object>(), near = {}, far = {}, next = {}, actor = {};
			scheduler.admit(near, [forecast({ decisionBatchesUntilCall: 1, criticalPathMs: 500 })], 2);
			scheduler.admit(far, [forecast({ decisionBatchesUntilCall: 4, criticalPathMs: 10 })], 2);
			const victim = joined ? near : far;
			expect(scheduler.preemptFor(1, 2, (job) => !joined || job !== far)).toEqual([victim]);
			expect(scheduler.preemptFor(1, 2, () => true, (job) => job === victim)).toEqual([]); // Draining units are about to return.
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
		for (const [evidence, actor, result] of [
			[{ status: "compatible", backend: "native", executionFingerprint: "world-a" }, "world-a", { compatible: true }],
			[{ status: "compatible", backend: "native", executionFingerprint: "world-a" }, "world-b", { compatible: false, code: "execution_fingerprint_changed" }],
			[{ status: "indeterminate", backend: "native", code: "attestation_missing" }, "world-a", { compatible: false, code: "backend_indeterminate", detail: "attestation_missing" }],
		] as const) expect(scheduler.assessCompatibility(evidence, actor)).toEqual(result);
	});
});

function forecast(overrides: Partial<PredictionForecast> = {}): PredictionForecast {
	return { tool: "read", expectedDurationMs: 50, decisionBatchesUntilCall: 1, ...overrides };
}

function joinDecision(
	scheduler: SpeculationScheduler<object>,
	identity: ServiceTimingIdentity,
	overrides: Partial<Omit<CandidateJoinRequest, "identity">> = {},
) {
	return scheduler.assessCandidateJoin({ identity, state: "running", expectedSpeculativeDurationMs: 1, ...overrides });
}
