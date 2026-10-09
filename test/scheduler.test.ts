import { afterEach, describe, expect, it, vi } from "vitest";
import { SpeculationScheduler, waitForCompletion, type PredictionForecast } from "../src/scheduler.ts";
import { deferred } from "./async.ts";
import type { ExecutionResourceSnapshot } from "../src/system-resources.ts";

const forecast = (facts: Partial<PredictionForecast> = {}): PredictionForecast => ({ tool: "read", decisionBatchesUntilCall: 1, ...facts });
const scope = (limit = 8) => ({ owner: {}, limit });
const host = (initial: ExecutionResourceSnapshot) => new SpeculationScheduler<object>({ resources: { initial, sample: async () => initial } });
afterEach(() => vi.useRealTimers());

describe("unified speculation scheduling", () => {
	it("shares memory, I/O and GPU capacity across preparation, execution and sessions", () => {
		const scheduler = host({ cpuCount: 8, idleCpuCount: 8, capacity: { memory: 100, io: 1, gpu: 1, gpuMemory: 100 },
			available: { memory: 100, io: 1, gpu: 1, gpuMemory: 100 } });
		const first = scope(), second = scope(), preparation = {}, gpu = {};
		expect(scheduler.admit(preparation, [forecast({ resourceDemand: { cpu: 1, memory: 60, io: 0.5 } })], first, "preparation").admitted).toBe(true);
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 1, memory: 50 } })], second).admitted).toBe(false);
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 1, io: 0.75 } })], second).admitted).toBe(false);
		expect(scheduler.admit(gpu, [forecast({ resourceDemand: { cpu: 1, memory: 10, gpu: 0.5, gpuMemory: 60 } })], second).admitted).toBe(true);
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 1, gpu: 0.5, gpuMemory: 50 } })], first).admitted).toBe(false);
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 1, memory: 10, io: 0.25 } })], first).admitted).toBe(true);
		expect(scheduler.inspect()).toMatchObject({ preparationUnits: 1, executionUnits: 2, reserved: { cpu: 3, memory: 80, gpu: 0.5, gpuMemory: 60 } });
	});

	it("does not invent GPU capacity or a duration for unknown work", () => {
		const scheduler = host({ cpuCount: 8, idleCpuCount: 8 }), owner = scope();
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 1, gpu: 1 } })], owner).admitted).toBe(false);
		scheduler.observe({ tool: "read", actionKeyHash: "old" }, false);
		expect(scheduler.evaluate([forecast()]).expectedDurationMs).toBeUndefined();
	});

	it("retains admitted resources when prediction ownership and live priority change", () => {
		const scheduler = host({ cpuCount: 2, idleCpuCount: 2, capacity: { memory: 100 }, available: { memory: 100 } });
		const job = {}, owner = scope();
		scheduler.admit(job, [forecast({ resourceDemand: { cpu: 1, memory: 80 } })], owner);
		scheduler.refresh(job, []);
		expect(scheduler.inspect()?.reserved).toEqual({ cpu: 1, memory: 80 });
		scheduler.refresh(job, [forecast({ actorDemand: true, resourceDemand: { cpu: 0 } })]);
		expect(scheduler.inspect()?.reserved).toEqual({ cpu: 1, memory: 80 });
		expect(scheduler.snapshot()[0]?.work.actorDemand).toBe(true);
		expect(scheduler.admit({}, [forecast({ resourceDemand: { memory: 30 } })], scope()).admitted).toBe(false);
	});

	it("keeps previews within capacity while admitting a producer selected by an actual Actor", () => {
		const scheduler = host({ cpuCount: 1, idleCpuCount: 1 }), owner = scope(1), job = {};
		const preview = forecast({ actorHint: true, resourceDemand: 2 });
		expect(scheduler.admit(job, [preview], owner).admitted).toBe(false);
		expect(scheduler.admit(job, [{ ...preview, actorDemand: true }], owner).admitted).toBe(true);
		expect(scheduler.inspect()?.reserved.cpu).toBe(2);
	});

	it("treats duration as occupancy and current progress, never as a latency benefit", () => {
		const scheduler = host({ cpuCount: 8, idleCpuCount: 8 });
		const short = scheduler.evaluate([forecast({ expectedDurationMs: 100, hitProbability: 0.8 })]);
		const long = scheduler.evaluate([forecast({ expectedDurationMs: 1000, hitProbability: 0.8 })]);
		expect(short.confidence).toBe(long.confidence);
		expect(scheduler.compare(short, long)).toBeLessThan(0);
		const finishing = scheduler.evaluate([forecast({ expectedDurationMs: 1000, elapsedMs: 950, hitProbability: 0.8 })]);
		expect(scheduler.compare(finishing, short)).toBeLessThan(0);
		const actor = scheduler.evaluate([forecast({ actorDemand: true, expectedDurationMs: 10_000, resourceDemand: 4 })]);
		expect(scheduler.compare(actor, short)).toBeLessThan(0);
	});

	it("opens launch windows from Actor progress and dependency facts without a history clock", () => {
		const scheduler = new SpeculationScheduler<object>();
		expect(scheduler.ready(scheduler.evaluate([forecast({ decisionBatchesUntilCall: 4, expectedDurationMs: 100_000 })]))).toBe(false);
		expect(scheduler.ready(scheduler.evaluate([forecast({ decisionBatchesUntilCall: 4, criticalPathSteps: 4 })]))).toBe(true);
		expect(scheduler.ready(scheduler.evaluate([forecast({ decisionBatchesUntilCall: 4, dependenciesResolved: true })]))).toBe(true);
		expect(scheduler.ready(scheduler.evaluate([forecast({ decisionBatchesUntilCall: 1 })]))).toBe(true);
		expect(scheduler.ready(scheduler.evaluate([forecast({ decisionBatchesUntilCall: 4, actorDemand: true })]))).toBe(true);
	});

	it("does not count correlated forecasts as extra confidence or physical resources", () => {
		const scheduler = new SpeculationScheduler<object>();
		const work = forecast({ hitProbability: 0.25, adoptionProbability: 0.5, resourceDemand: { cpu: 2, memory: 20 } });
		expect(scheduler.evaluate([work, work, work])).toEqual(scheduler.evaluate([work]));
	});

	it("keeps cancelled work charged until physical completion and avoids extra victims", () => {
		const scheduler = host({ cpuCount: 2, idleCpuCount: 2 }), first = scope(), second = scope();
		const cold = {}, foreground = {}, actor = {};
		scheduler.admit(cold, [forecast({ background: true })], first);
		scheduler.admit(foreground, [forecast()], first);
		expect(scheduler.admit(actor, [forecast({ actorDemand: true })], second, "actor").admitted).toBe(true);
		expect(scheduler.preemptFor(second, undefined, () => true, () => false)).toEqual([cold]);
		expect(scheduler.preemptFor(second, undefined, () => true, job => job === cold)).toEqual([]);
		expect(scheduler.inspect()?.reserved.cpu).toBe(3);
		expect(scheduler.admit({}, [forecast()], first).admitted).toBe(false);
		scheduler.complete(cold); scheduler.complete(actor);
		expect(scheduler.admit({}, [forecast()], second).admitted).toBe(true);
	});

	it("protects a producer claimed by a real Actor and keeps unrelated previews lower priority", () => {
		const scheduler = host({ cpuCount: 2, idleCpuCount: 2 }), owner = scope(), claimed = {}, hinted = {}, actor = {};
		scheduler.admit(claimed, [forecast({ actorDemand: true })], owner);
		scheduler.admit(hinted, [forecast({ actorHint: true })], owner);
		scheduler.admit(actor, [forecast({ actorDemand: true })], owner, "actor");
		expect(scheduler.preemptFor(owner, undefined, () => true, () => false)).toEqual([hinted]);
	});

	it("preempts only work that relieves the missing resource or local scope", () => {
		const scheduler = host({ cpuCount: 8, idleCpuCount: 8, capacity: { memory: 100 }, available: { memory: 100 } });
		const first = scope(2), second = scope(), unrelated = {}, memory = {}, local = {};
		scheduler.admit(unrelated, [forecast({ background: true, resourceDemand: { cpu: 1 } })], second);
		scheduler.admit(memory, [forecast({ resourceDemand: { cpu: 1, memory: 80 } })], second);
		const incoming = scheduler.evaluate([forecast({ actorHint: true, resourceDemand: { cpu: 1, memory: 40 } })]);
		expect(scheduler.preemptFor(first, incoming, () => true, () => false)).toEqual([memory]);
		expect(scheduler.preemptFor(first, scheduler.evaluate([forecast({ actorHint: true, resourceDemand: { memory: 101 } })]), () => true, () => false)).toEqual([]);
		scheduler.admit(local, [forecast({ resourceDemand: { cpu: 0 } })], first);
		scheduler.admit({}, [forecast({ resourceDemand: { cpu: 0 }, actorDemand: true })], first, "actor");
		expect(scheduler.admit({}, [forecast({ resourceDemand: { cpu: 0 } })], first).admitted).toBe(false);
		expect(scheduler.preemptFor(first, scheduler.evaluate([forecast({ actorHint: true, resourceDemand: { cpu: 0 } })]), () => true, () => false)).toEqual([local]);
	});

	it("keeps failure recovery independent of elapsed time and repeated dispatch", () => {
		const scheduler = new SpeculationScheduler<object>(), owner = { ...scope(), scheduling: { failureThreshold: 3, failureRetryDecisions: 2 } }, job = {}, identity = { tool: "read", actionKeyHash: "a" };
		for (let count = 0; count < 2; count++) { scheduler.observe(identity, true); expect(scheduler.admit(job, [forecast()], owner, "execution", undefined, identity).admitted).toBe(true); scheduler.complete(job); }
		scheduler.observe(identity, true);
		for (let decision = 0; decision < 1; decision++) {
			for (let dispatch = 0; dispatch < 5; dispatch++) for (const attempt of [job, {}])
				expect(scheduler.admit(attempt, [forecast()], owner, "execution", undefined, identity)).toMatchObject({ admitted: false, reason: "failure_circuit" });
			scheduler.advance();
		}
		expect(scheduler.admit(job, [forecast()], owner, "execution", undefined, identity).admitted).toBe(true);
		expect(scheduler.admit({}, [forecast()], owner, "execution", undefined, { ...identity, actionKeyHash: "b" }).admitted).toBe(true);
	});

	it("waits for completion without a timer and releases cancelled waits", async () => {
		vi.useFakeTimers();
		const result = deferred<number>(), settled = vi.fn(), joined = waitForCompletion(result.promise).then(value => { settled(); return value; });
		await vi.advanceTimersByTimeAsync(86_400_000); expect(settled).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
		result.resolve(42); expect(await joined).toEqual({ status: "completed", value: 42 });
		const controller = new AbortController(), pending = new Promise<void>(() => {});
		const aborted = waitForCompletion(pending, controller.signal);
		controller.abort(); expect(await aborted).toEqual({ status: "aborted" }); expect(vi.getTimerCount()).toBe(0);
		const bounded = waitForCompletion(pending, undefined, 27); // Source production and teardown retain their own deadlines.
		await vi.advanceTimersByTimeAsync(27);
		expect(await bounded).toEqual({ status: "deadline" }); expect(vi.getTimerCount()).toBe(0);
	});

	it("samples off the Actor path at the configured interval", async () => {
		vi.useFakeTimers();
		const sample = vi.fn(async () => ({ cpuCount: 2 })), scheduler = new SpeculationScheduler({
			resources: { initial: { cpuCount: 2 }, sample }, active: () => true, pollIntervalMs: () => 73 });
		scheduler.watch(); await vi.advanceTimersByTimeAsync(72); expect(sample).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1); expect(sample).toHaveBeenCalledTimes(1); scheduler.close();
		expect(vi.getTimerCount()).toBe(0);
	});

	it("preserves execution-world compatibility as an independent adoption requirement", () => {
		const scheduler = new SpeculationScheduler<object>();
		expect(scheduler.assessCompatibility({ status: "compatible", backend: "test", executionFingerprint: "one" }, "one")).toEqual({ compatible: true });
		expect(scheduler.assessCompatibility({ status: "compatible", backend: "test", executionFingerprint: "one" }, "two")).toMatchObject({ compatible: false, code: "execution_fingerprint_changed" });
	});
});
