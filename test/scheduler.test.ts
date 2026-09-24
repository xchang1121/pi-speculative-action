import { deferred } from "./async.ts";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_BENEFIT_GATE_POLICY } from "../src/fork-benefit-gate.ts";
import {
	type CandidateJoinRequest,
	type PredictionForecast,
	type ServiceTimingIdentity,
	SpeculationScheduler,
	waitForCandidate,
} from "../src/scheduler.ts";

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

	it("separates producer, consumer, and adoption work while retaining exact/class quantiles and bounded history", () => {
		const scheduler = new SpeculationScheduler<object>();
		const identity = { tool: "bash", executionFingerprint: "linux-world", actionKeyHash: "producer" };
		const actorIdentity = { ...identity, actionKeyHash: "consumer" };
		const exact = { ...actorIdentity, operation: "route:exact" }, inputs = { ...actorIdentity, operation: "route:inputs" };
		scheduler.observeActorService(identity, 380, 300);
		scheduler.observeAdoption(identity, 70);
		for (const duration of [40, 50, 90]) scheduler.observeSpeculativeService(identity, duration);
		for (const duration of [100, 110, 120, 130]) scheduler.observeActorService(actorIdentity, duration);
		for (const duration of [5, 10, 15, 20]) scheduler.observeAdoption(exact, duration);
		for (const duration of [150, 160, 180, 200]) scheduler.observeAdoption(inputs, duration);
		expect(scheduler.evaluate([forecast({ ...identity, expectedDurationMs: 1 })]).expectedDurationMs).toBe(50);
		expect(joinDecision(scheduler, identity)).toMatchObject({ expectedRemainingMs: 90, expectedActorMs: 380, expectedNativeMs: 300, expectedAdoptionMs: 70 });
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
			[{ ...inputs, actionKeyHash: "new pair" }, 200],
			[{ ...exact, operation: "other route:exact" }, 0],
			[{ ...exact, executionFingerprint: "other executor" }, 0],
		] as const) {
			expect(joinDecision(scheduler, identity, {
				actorIdentity: { ...actorIdentity, actionKeyHash: "new query" }, adoptionIdentity, state: "succeeded",
			})).toMatchObject({ allowed: true, expectedActorMs: 110, expectedAdoptionMs });
		}

		for (let failure = 0; failure < DEFAULT_BENEFIT_GATE_POLICY.failureThreshold; failure++)
			scheduler.observeSpeculativeService(identity, 1, true);
		for (let index = 0; index < 1100; index++) {
			const newer = { ...identity, executionFingerprint: `world-${index}` };
			scheduler.observeActorService(newer, 1);
			scheduler.observeSpeculativeService(newer, 2);
			scheduler.observeAdoption(newer, 3);
		}
		expect(joinDecision(scheduler, identity)).toMatchObject({ actorSamples: 0, speculativeSamples: 0, adoptionSamples: 0 });
		expect(scheduler.admit({}, [forecast(identity)], 1, "producer", undefined, identity).admitted).toBe(true);
	});

	it("uses measured net latency to retain heavy hits and reject noise-boundary waits", () => {
		const identity = { tool: "bash", executionFingerprint: "linux-world", actionKeyHash: "measured-action" };
		for (const [actorMs, speculativeMs, adoptionMs, state, netMs, allowed] of [
			[2687, 936, 70, "running", 1681, true],
			[994, 973, 0, "running", 21, false],
			[30, 920, 70, "succeeded", -40, false],
		] as const) {
			const scheduler = new SpeculationScheduler<object>();
			for (let sample = 0; sample < 4; sample++) {
				scheduler.observeActorService(identity, actorMs);
				if (state === "running") scheduler.observeSpeculativeService(identity, speculativeMs);
				if (adoptionMs) scheduler.observeAdoption(identity, adoptionMs);
				if (state === "succeeded" && sample < 3) expect(joinDecision(scheduler, identity, { state }).allowed).toBe(true);
			}
			const decision = joinDecision(scheduler, identity, { state, expectedSpeculativeDurationMs: state === "running" ? actorMs : speculativeMs });
			if (state === "succeeded") expect(joinDecision(scheduler, { ...identity, actionKeyHash: undefined }, { state }).allowed).toBe(true);
			expect(decision).toMatchObject({ allowed, reason: allowed ? "profitable" : "fallback_faster",
				expectedRemainingMs: state === "running" ? speculativeMs : 0, expectedNetBenefitMs: netMs });
			if (allowed) expect(decision.waitBudgetMs).toBeGreaterThan(speculativeMs);
		}
		const unmeasured = new SpeculationScheduler<object>();
		unmeasured.observeActorService(identity, 1000);
		unmeasured.observeAdoption(identity, 50);
		const pending = { state: "running" as const, elapsedMs: 800, expectedSpeculativeDurationMs: undefined };
		expect(joinDecision(unmeasured, identity, pending)).toMatchObject({ allowed: true, reason: "warmup_probe",
			expectedRemainingMs: 200, expectedNetBenefitMs: 750, waitBudgetMs: 925 });
		unmeasured.observeSpeculativeService(identity, 1000);
		expect(joinDecision(unmeasured, identity, pending)).toMatchObject({ reason: "profitable", waitBudgetMs: 275 });
		const cold = new SpeculationScheduler<object>();
		cold.observeActorService(identity, 30);
		const run = (cost: number, count: number) => Array.from({ length: count }, () => {
			const decision = joinDecision(cold, identity, { state: "succeeded" });
			if (decision.allowed) cold.observeAdoption(identity, cost);
			else cold.observeActorService(identity, 30);
			return decision;
		});
		const loss = run(70, 32);
		expect(loss.slice(0, 4).every((decision) => decision.allowed)).toBe(true);
		expect(loss.slice(-8).filter((decision) => !decision.allowed).length).toBeGreaterThanOrEqual(4);
		expect(loss.at(-1)!.actorSamples).toBeGreaterThan(1);
		const overlapping = Array.from({ length: 12 }, () => joinDecision(cold, identity, { state: "succeeded" }));
		expect(overlapping.filter((decision) => decision.allowed).length).toBeGreaterThan(0);
		expect(overlapping.filter((decision) => decision.allowed).length).toBeLessThanOrEqual(3);
		expect(run(1, 256).slice(-8).every((decision) => decision.allowed)).toBe(true);
	});

	it.each(["loss", "hidden", "slow-producer", "cancelled", "cancelled-no-forecast", "unknown-clock", "other-action", "no-forecast", "actor"] as const)("aligns producer launch with the expected Actor decision: %s", (mode) => {
		const scheduler = new SpeculationScheduler<object>(), job = {};
		const identity = { tool: "read", executionFingerprint: "reader", actionKeyHash: "image" };
		for (const duration of [1000, 2000]) scheduler.observeActorService(identity, duration);
		if (mode === "slow-producer") for (const duration of [1000, 1500, 4000]) scheduler.observeSpeculativeService(identity, duration);
		const cancelled = mode === "cancelled" || mode === "cancelled-no-forecast";
		if (cancelled) for (const duration of [4000, 3000]) scheduler.observeSpeculativeService(identity, duration, "cancelled");
		if (mode !== "unknown-clock") scheduler.observeActorTiming(100, 1100);
		const hidden = mode === "hidden" || mode === "slow-producer" || cancelled, blocked = mode === "loss" || mode === "slow-producer" || cancelled;
		const request = forecast({ ...identity, expectedDurationMs: mode.endsWith("no-forecast") ? undefined : 1500,
			actionKeyHash: mode === "other-action" ? "unmeasured" : identity.actionKeyHash,
			actorPhase: { kind: hidden ? "cycle" : "decision", elapsedMs: 0 }, decisionBatchesUntilCall: hidden ? 2 : 1 });
		const admission = scheduler.admit(job, [request], 1, mode === "actor" ? "actor" : "producer");
		expect(admission.admitted).toBe(!blocked);
		expect(scheduler.snapshot().map((entry) => entry.job)).toEqual(blocked ? [] : [job]);
		if (cancelled) {
			expect(joinDecision(scheduler, identity)).toMatchObject({ speculativeSamples: 0, expectedRemainingMs: 4000 });
			expect(scheduler.evaluate([forecast({ ...identity, actionKeyHash: "other", expectedDurationMs: undefined })]).expectedDurationMs).toBe(1);
			const probes = Array.from({ length: 2 * DEFAULT_BENEFIT_GATE_POLICY.probeInterval }, () => {
				const join = joinDecision(scheduler, identity, { state: "running", elapsedMs: 4500 });
				expect(join).toMatchObject({ reason: "warmup_probe", speculativeSamples: 0 });
				expect(join.waitBudgetMs).toBe(join.allowed ? 25 : 0);
				return join.allowed;
			});
			expect(probes).toEqual(probes.map((_, index) => (index + 1) % DEFAULT_BENEFIT_GATE_POLICY.probeInterval === 0));
			expect(joinDecision(scheduler, identity, { state: "succeeded" })).toMatchObject({ allowed: true, reason: "ready" });
			expect(scheduler.admit(job, [{ ...request, decisionBatchesUntilCall: 5 }], 1).admitted).toBe(true);
			scheduler.complete(job);
			scheduler.observeSpeculativeService(identity, 100);
			expect(scheduler.admit(job, [request], 1).admitted).toBe(true);
			expect(joinDecision(scheduler, identity)).toMatchObject({ speculativeSamples: 1, expectedRemainingMs: 100 });
		}
		if (mode === "loss") {
			expect(admission).toMatchObject({ reason: "not_profitable" });
			expect(joinDecision(scheduler, identity, { state: "running", expectedSpeculativeDurationMs: 1500, elapsedMs: 100 }).allowed).toBe(false);
			expect(scheduler.admit(job, [request, { ...request, decisionBatchesUntilCall: 3 }], 1).admitted).toBe(true);
		}
	});

	it("keeps a long action's forecast and elapsed time above a short-command timing class", () => {
		const scheduler = new SpeculationScheduler<object>(), ls = { tool: "bash", executionFingerprint: "linux-world", actionKeyHash: "ls" };
		for (const duration of [202, 204, 200, 206]) scheduler.observeSpeculativeService(ls, duration);
		for (const duration of [20.8, 20.1, 20.1, 20.1]) scheduler.observeActorService({ ...ls, actionKeyHash: "git-status" }, duration);
		const npmTest = { ...ls, actionKeyHash: "npm-test" }, running = { state: "running" as const, elapsedMs: 616 };
		expect(joinDecision(scheduler, npmTest, { ...running, expectedSpeculativeDurationMs: 2000 })).toMatchObject({ allowed: true, expectedRemainingMs: 1384, waitBudgetMs: 1755 });
		expect(joinDecision(scheduler, npmTest, { ...running, expectedSpeculativeDurationMs: undefined })).toMatchObject({ allowed: true, waitBudgetMs: 591 });
	});

	it("bounds an uncalibrated join while wider timing classes transfer across exact actions", () => {
		const scheduler = new SpeculationScheduler<object>({
			candidateJoinPolicy: { warmupWaitMs: 17 },
		});
		const first = { tool: "bash", executionFingerprint: "linux-world", actionKeyHash: "parent-a" };
		const second = { ...first, actionKeyHash: "parent-b" };
		expect(joinDecision(scheduler, first)).toMatchObject({
			allowed: true,
			reason: "warmup_probe",
			waitBudgetMs: Number.POSITIVE_INFINITY,
			actorSamples: 0,
		});
		scheduler.observeActorService(first, 100);
		expect(joinDecision(scheduler, first)).toMatchObject({ allowed: true, reason: "warmup_probe", waitBudgetMs: 18.25, expectedNetBenefitMs: 99 });

		const cold = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
		for (const [duration, count] of [[900, 1], [900, 63], [450, 64], [1800, 64]] as const) {
			for (let sample = 0; sample < count; sample++) cold.observeSpeculativeService(first, duration);
			expect(joinDecision(cold, second)).toMatchObject({ allowed: false, reason: "warmup_probe", actorSamples: 0 });
			expect(cold.evaluate([forecast({ ...second, expectedDurationMs: 200 })])).toMatchObject({ expectedDurationMs: duration });
		}
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
