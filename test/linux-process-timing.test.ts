import * as filesystem from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { HeldExecDecision, HeldExecProcess, HeldExecTiming } from "../src/linux-held-exec.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import { TaskTimeline, TimelineInterval, type TimelineDependency } from "../src/task-timing.ts";
import { processCertificate, processPrototype, SPECULATIVE_PRODUCER } from "./process-fixture.ts";

vi.mock("node:fs/promises", { spy: true });

type TimingReceipt = HeldExecTiming & { readonly pid: number };

/** Inject transport evidence without launching a process or enabling optional native observation. */
async function account(timings: readonly TimingReceipt[]) {
	const sourceRoot = path.join(os.tmpdir(), "held-clock-fixture"), scope = { sessionID: "timing", turnID: "actor" };
	const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(sourceRoot, "store") });
	const image = vi.spyOn(filesystem, "realpath").mockResolvedValue(path.join(sourceRoot, "worker"));
	const available = vi.spyOn(backend.store, "mayHaveCertificates").mockResolvedValue(false);
	const decide = Reflect.get(backend, "decideHeldExec") as (process: HeldExecProcess, executionScope: typeof scope) => Promise<HeldExecDecision>;
	let dependencies: readonly TimelineDependency[] = [];
	try {
		await backend.observeBindings(scope, async () => {
			for (const [index, timing] of timings.entries()) {
				const decision = await decide.call(backend, { id: `clock:${index}`, sequence: index + 1, pid: timing.pid,
					tracerPid: 1, sourceRoot, scope }, scope);
				expect(decision).toMatchObject({ kind: "continue", repeat: "launch" });
				if (decision.kind !== "continue") throw new Error("unexpected replay");
				expect(decision.observeCompletion, "accounting must not request extra native observations").toBeUndefined();
				decision.observeTiming!(timing);
			}
		}, (_bindings, measured) => { dependencies = measured; });
	} finally { image.mockRestore(); available.mockRestore(); }
	const reused = new TimelineInterval(200, 240);
	const timeline = new TaskTimeline(0);
	const computation = new TimelineInterval(0, 120, [...dependencies, { computation: reused, reused: true }]);
	const result = timeline.recordTool(computation, performance.now());
	const later = new TaskTimeline(0).recordTool(computation, performance.now(), true);
	return { dependencies, result, later };
}

describe("held native computation accounting", () => {
	it("keeps validation-only captures out of the enclosing calculation graph", async () => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "validation-clock-fixture") });
		const internal = backend as unknown as { planner: { plan: (...args: unknown[]) => Promise<unknown> };
			plan: (...args: unknown[]) => Promise<unknown> };
		const actualInput = new TimelineInterval(200, 210), proofInput = new TimelineInterval(300, 350);
		const planner = vi.spyOn(internal.planner, "plan").mockImplementation(async () => {
			TimelineInterval.own(new TimelineInterval(10, 20)); TimelineInterval.use(proofInput);
			return { kind: "miss", lookup: { candidateCertificates: 0, pathsetsValidated: 0,
				filesRead: 1, bytesRead: 1, artifactsLoaded: 0, artifactBytesRead: 0 } };
		});
		try {
			const evaluation = await TimelineInterval.collect(async () => {
				TimelineInterval.own(new TimelineInterval(0, 10)); TimelineInterval.use(actualInput);
				await internal.plan("key", "/bin/tool", {}, () => true);
				TimelineInterval.exclude(new TimelineInterval(10, 20));
			});
			expect(new TaskTimeline(0).recordTool(new TimelineInterval(0, 30, evaluation.dependencies), performance.now()))
				.toMatchObject({ toolComputeMs: 30, hiddenComputeMs: 10 });
		} finally { planner.mockRestore(); }
	});

	it("retains shared session preparation once and includes only a prefix's recorded local setup", () => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "preparation-clock-fixture") });
		const internal = backend as unknown as {
			recordSessionPreparation: (session: unknown, creation: TimelineInterval, dispatch: TimelineInterval) => void;
			processComputation: (session: unknown, startedAt: number, completedAt: number) => TimelineInterval;
		};
		const session = { computations: [] as TimelineDependency[] };
		const readiness = new TimelineInterval(0, 2), creation = new TimelineInterval(0, 10, [{ computation: readiness, overhead: true }]);
		internal.recordSessionPreparation(session, creation, new TimelineInterval(30, 40));
		const native = new TimelineInterval(60, 80), prefix = internal.processComputation(session, 50, native.completedAt);
		const sibling = internal.processComputation(session, 70, 100);
		expect([native.startedAt, native.completedAt], "native frontier remains unchanged").toEqual([60, 80]);
		expect(new TaskTimeline(0).recordTool(prefix, performance.now(), true)).toMatchObject({ toolComputeMs: 48, hiddenComputeMs: 48 });
		const restored = [prefix, sibling].map(computation => ({ computation: TimelineInterval.restore(TimelineInterval.serialize(computation))!, reused: true }));
		expect(restored.every(input => !!input.computation)).toBe(true);
		expect(new TaskTimeline(0).recordCall(restored, performance.now())).toMatchObject({ toolComputeMs: 68, hiddenComputeMs: 68 });
		// The outer caller keeps its own work between session creation and dispatch; only readiness is excluded.
		expect(new TaskTimeline(0).recordTool(new TimelineInterval(0, 110, session.computations), performance.now(), true))
			.toMatchObject({ toolComputeMs: 108, hiddenComputeMs: 108 });
	});

	it.each(["live", "stored", "legacy", "malformed"] as const)("credits successful %s replay from original calculation evidence", async kind => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "stored-clock-fixture") });
		const source = new TimelineInterval(200, 250), overhead = new TimelineInterval(20, 40);
		const original = new TimelineInterval(10, 110, [{ computation: overhead, overhead: true }, { computation: source, reused: true }]);
		const serialized = TimelineInterval.serialize(original);
		const certificate = processCertificate(processPrototype(), { result: { replayProfile: "buffered_noninteractive", journal: [],
			exit: { kind: "code", code: 0 }, observedProcessMs: 999, ...(kind === "stored" ? { computation: serialized } : {}) } });
		const plan = { kind: "completed_replay", certificate: kind === "malformed"
			? { ...certificate, result: { ...certificate.result, computation: { version: 999 } } } : certificate, artifacts: { read: vi.fn() } };
		const computations: TimelineDependency[] = [], session = { computations, nestedEvidence: [], metrics: emptyWorldReuseMetrics(),
			workspace: { sandboxRoot: os.tmpdir(), processRoot: os.tmpdir() } };
		const replay = Reflect.get(backend, "replay") as (...args: unknown[]) => Promise<unknown>;
		expect(await replay.call(backend, session, plan, certificate.weakKey, { joined: false,
			...(kind === "live" ? { producer: { computation: original } } : {}) })).toMatchObject({ kind: "hit", exit: { kind: "code", code: 0 } });
		const timing = new TaskTimeline(0).recordCall(computations, performance.now());
		expect(timing).toMatchObject({ toolComputeMs: kind === "live" || kind === "stored" ? 130 : undefined, hiddenComputeMs: kind === "live" || kind === "stored" ? 130 : 0 });
		expect(timing.hiddenComputeIncomplete).toBe(kind === "legacy" || kind === "malformed" ? true : undefined);
		if (kind === "live") expect(computations[0]!.computation).toBe(original);
		expect(plan.artifacts.read).not.toHaveBeenCalled();
	});

	it("publishes top-level preparation and nested work with replay overhead excluded", async () => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "published-clock-fixture") });
		const internal = backend as unknown as { planner: { publishCompleted: (...args: unknown[]) => Promise<boolean> };
			publishTopLevel: (session: unknown, changes: readonly unknown[]) => Promise<void> };
		const overhead = new TimelineInterval(20, 40), source = new TimelineInterval(200, 250);
		const session = { topLevelExecution: { prototype: processPrototype(), outcome: { code: 0, signal: null, output: [] }, observedProcessMs: 40, startedAt: 0 },
			topLevelEvidence: { complete: true, dependencies: [], taints: [] }, producer: SPECULATIVE_PRODUCER,
			computations: [{ computation: overhead, overhead: true }, { computation: source, reused: true }], metrics: emptyWorldReuseMetrics() };
		const publish = vi.spyOn(internal.planner, "publishCompleted").mockResolvedValue(false), clock = vi.spyOn(performance, "now").mockReturnValue(100);
		try {
			await internal.publishTopLevel(session, []);
			const certificate = publish.mock.calls[0]![0] as ReturnType<typeof processCertificate>;
			const restored = TimelineInterval.restore(certificate.result.computation)!;
			expect(restored).toBeDefined();
			expect(new TaskTimeline(0).recordTool(restored, performance.now(), true)).toMatchObject({ toolComputeMs: 130, hiddenComputeMs: 80 });
		} finally { publish.mockRestore(); clock.mockRestore(); }
	});

	it.each([false, true])("excludes speculative replay lookup and delivery while preserving fresh production (hit=%s)", async hit => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "replay-clock-fixture") });
		const internal = backend as unknown as {
			prototype: (...args: unknown[]) => Promise<unknown>;
			acquireProcessResult: (...args: unknown[]) => Promise<unknown>;
			replay: (...args: unknown[]) => Promise<unknown>;
			executeAndPublish: (...args: unknown[]) => Promise<unknown>;
			executeRequest: (session: unknown, request: unknown, executable: string, route: readonly number[], id: number, prototype: unknown) => Promise<unknown>;
			processComputation: (session: unknown, startedAt: number, completedAt: number, pids?: readonly number[], preparation?: TimelineInterval) => TimelineInterval;
		};
		const computations: TimelineDependency[] = [], source = new TimelineInterval(200, 300), native = new TimelineInterval(20, 50);
		const session = { computations, metrics: emptyWorldReuseMetrics(), signal: new AbortController().signal };
		const work = { computation: native };
		let now = 0;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const prototype = vi.spyOn(internal, "prototype").mockImplementation(async () => {
			now = 10; TimelineInterval.own(new TimelineInterval(0, 10)); return processPrototype();
		});
		const acquired = vi.spyOn(internal, "acquireProcessResult").mockImplementation(async () => {
			now = 20; return hit ? { plan: { kind: "completed_replay" } } : { work };
		});
		const replay = vi.spyOn(internal, "replay").mockImplementation(async () => {
			now = 50; computations.push({ computation: source, reused: true }); return { kind: "hit" };
		});
		const fresh = vi.spyOn(internal, "executeAndPublish").mockImplementation(async (...args) => {
			now = 50; work.computation = internal.processComputation(session, 20, 50, undefined, args[10] as TimelineInterval);
			return { kind: "executed" };
		});
		try {
			const outer = await TimelineInterval.collect(() => internal.executeRequest(session, {}, "/bin/tool", [1, 2], 1, undefined));
			expect(replay).toHaveBeenCalledTimes(Number(hit)); expect(fresh).toHaveBeenCalledTimes(Number(!hit));
			const producer = new TimelineInterval(0, 60, [...outer.dependencies, ...computations]);
			expect(new TaskTimeline(0).recordTool(producer, performance.now(), true)).toMatchObject({ toolComputeMs: hit ? 110 : 50, hiddenComputeMs: hit ? 0 : 40 });
			if (!hit) expect(new TaskTimeline(0).recordTool(work.computation, performance.now(), true)).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40 });
		} finally { prototype.mockRestore(); acquired.mockRestore(); replay.mockRestore(); fresh.mockRestore(); clock.mockRestore(); }
	});

	it("does not credit failed certificate lookup as original computation", async () => {
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "failed-clock-fixture") });
		const internal = backend as unknown as { prototype: (...args: unknown[]) => Promise<unknown>; acquireProcessResult: (...args: unknown[]) => Promise<unknown>;
			executeRequest: (...args: unknown[]) => Promise<unknown> };
		const computations: TimelineDependency[] = [], session = { computations, signal: new AbortController().signal };
		let now = 0;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const prototype = vi.spyOn(internal, "prototype").mockImplementation(async () => {
			now = 10; TimelineInterval.own(new TimelineInterval(0, 10)); return processPrototype();
		});
		const acquire = vi.spyOn(internal, "acquireProcessResult").mockImplementation(async () => { now = 20; throw new Error("invalid certificate"); });
		try {
			const evaluation = await TimelineInterval.measure(async () => {
				await expect(internal.executeRequest(session, {}, "/bin/tool", [1, 2], 1)).rejects.toThrow("invalid certificate");
				now = 100;
			}, () => computations);
			expect(new TaskTimeline(0).recordTool(evaluation.computation, performance.now(), true)).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 80 });
		} finally { prototype.mockRestore(); acquire.mockRestore(); clock.mockRestore(); }
	});

	it("excludes a sibling barrier from every observed clock and clips replaced images", async () => {
		const { dependencies, result } = await account([
			{ pid: 10, requestedAt: 10, completedAt: 20, barrier: "inspection", outcome: "continued", native: { startedAt: 20, completedAt: 100 } },
			{ pid: 20, requestedAt: 30, completedAt: 40, barrier: "inspection", outcome: "continued", native: { startedAt: 40, completedAt: 100 } },
			{ pid: 10, requestedAt: 50, committedAt: 65, completedAt: 70, barrier: "inspection", outcome: "adopted" },
		]);
		expect(result).toMatchObject({ toolComputeMs: 120, hiddenComputeMs: 40 });
		expect(dependencies.filter(input => input.owned).map(input => [input.computation.startedAt, input.computation.completedAt]))
			.toEqual([[20, 50], [40, 100]]);
	});

	it.each(["none", "descriptors"] as const)("keeps known hidden computation when %s transport cannot separate sibling computation", async barrier => {
		const { result, later } = await account([
			{ pid: 10, requestedAt: 10, completedAt: 20, barrier: "inspection", outcome: "continued", native: { startedAt: 20, completedAt: 100 } },
			{ pid: 20, requestedAt: 40, committedAt: 50, completedAt: 70, barrier, outcome: "adopted" },
		]);
		expect(result).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40 });
		expect(later, "later reuse cannot credit ambiguous adoption as original computation").toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 120, hiddenComputeIncomplete: true });
	});

	it("does not invent a denominator after bounded timing storage saturates", async () => {
		const { dependencies, result, later } = await account(Array.from({ length: 66 }, (_, index) => ({
			pid: index + 10, requestedAt: index, completedAt: index + 0.5, barrier: "inspection", outcome: "continued",
			...(index === 0 ? { native: { startedAt: 0.5, completedAt: 120 } } : {}),
		})));
		expect(dependencies).toHaveLength(66); // 64 pauses, one native clock and one bounded omission envelope.
		expect(result).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40 });
		expect(later).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 126.5, hiddenComputeIncomplete: true });
	});
});
