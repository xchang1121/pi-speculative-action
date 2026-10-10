import { describe, expect, it, vi } from "vitest";
import { deferred } from "./async.ts";
import { TaskTimeline, TimelineInterval, toolSpeedup, normalizeTimelineComputation } from "../src/task-timing.ts";

describe("consumed calculation and time hidden before Actor issue", () => {
	it("records execution boundaries and diagnoses the remaining control wait", async () => {
		let now = 0;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		try {
			const evaluation = await TimelineInterval.measure(async () => {
				now = 10;
				await TimelineInterval.outside(async () => {
					now = 1000;
					TimelineInterval.use(new TimelineInterval(200, 240));
					const calculation = await TimelineInterval.measure(() => { now = 1010; });
					TimelineInterval.own(calculation.computation);
					now = 1015;
				});
				now = 1025;
			});
			const graph = TimelineInterval.serialize(evaluation.computation)!;
			expect(graph.nodes.flatMap(node => node.inputs ?? []).some(input => input.overhead)).toBe(false);
			expect(new TaskTimeline(0).recordTool(evaluation.computation, 100_000)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 40 });
			expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(graph)!, 100_000, true)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 70 });
			expect(new TaskTimeline(0).recordCall([{ computation: evaluation.computation }], 0, now))
				.toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 0, adoptionWaitMs: 955 });
			clock.mockClear();
			await TimelineInterval.outside(() => { now += 1000; });
			expect(clock).not.toHaveBeenCalled();
		} finally { clock.mockRestore(); }
	});
	it.each([0, 60, 120])("separates control wait from a producer finishing at %s", end => {
		const producer = new TimelineInterval(0, end), timeline = new TaskTimeline(40);
		const roots = [{ computation: new TimelineInterval(40, 140, [{ computation: producer, reused: true },
			{ computation: new TimelineInterval(40, 140), overhead: true }]) }];
		expect(timeline.recordCall(roots, 40, 140)).toMatchObject({ toolComputeMs: end, hiddenComputeMs: Math.min(40, end), adoptionWaitMs: 140 - Math.max(40, end) });
		const restored = TimelineInterval.restore(TimelineInterval.serialize(producer))!;
		expect(timeline.recordCall([{ computation: restored, reused: true }], 50, 150).adoptionWaitMs).toBe(150 - Math.max(50, end));
		expect(timeline.measure(150).adoptionWaitMs).toBe(150 - Math.max(40, end));
		expect(timeline.recordCall([{ computation: TimelineInterval.unknownReuse("old"), reused: true }], 150, 160).adoptionWaitMs).toBeUndefined();
		expect(timeline.measure(160).adoptionWaitMs).toBeUndefined();
	});

	it("keeps nested and overlapping control work outside every enclosing calculation after failure", async () => {
		let now = 0;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		try {
			const evaluation = await TimelineInterval.measure(async () => {
				now = 5;
				const child = await TimelineInterval.measure(async () => {
					now = 10;
					const first = deferred<void>(), second = deferred<void>();
					const failure = TimelineInterval.outside(async () => { await first.promise; throw new Error("invalid proof"); }).catch(() => {});
					now = 20;
					const other = TimelineInterval.outside(() => second.promise);
					now = 30; first.resolve(); await failure;
					now = 40; second.resolve(); await other;
					now = 50;
					expect(new TaskTimeline(0).recordTool(TimelineInterval.current(5, 50), 100_000)).toMatchObject({ toolComputeMs: 15, hiddenComputeMs: 0 });
				});
				TimelineInterval.own(child.computation); now = 60;
			});
			expect(new TaskTimeline(0).recordTool(evaluation.computation, 100_000)).toMatchObject({ toolComputeMs: 30, hiddenComputeMs: 0 });
		} finally { clock.mockRestore(); }
	});

	it("unions complete and interrupted tool waits without using them as the computation denominator", () => {
		const timeline = new TaskTimeline(100);
		timeline.recordTool(new TimelineInterval(0, 100), 100_000);
		timeline.recordTool(new TimelineInterval(110, 130), 100_000, true);
		timeline.startToolWait(140)(170);
		timeline.startToolWait(150)(180);
		const interrupted = timeline.startToolWait(190);
		expect(timeline.measure(200)).toMatchObject({ toolWaitMs: 50, toolComputeMs: 120, hiddenComputeMs: 20 });
		expect(toolSpeedup(timeline.measure(200))).toBe(1.2);
		interrupted(195); interrupted(250);
		expect(timeline.measure(200).toolWaitMs).toBe(45);
		expect(toolSpeedup(timeline.measure(200))).toBe(1.2);
		expect(toolSpeedup(new TaskTimeline(200).measure(300))).toBeNull();
		for (const toolComputeMs of [0, -1, NaN, Infinity]) expect(toolSpeedup({ toolComputeMs, hiddenComputeMs: 20 })).toBeNull();
		for (const hiddenComputeMs of [-1, NaN, Infinity]) expect(toolSpeedup({ toolComputeMs: 50, hiddenComputeMs })).toBeNull();
		expect(toolSpeedup({ toolComputeMs: 150, hiddenComputeMs: 100, hiddenComputeIncomplete: true })).toBeNull();
	});

	it.each([
		{ name: "native work without reuse", computation: new TimelineInterval(0, 100), issuedAt: 0, reused: false,
			expected: { toolComputeMs: 100, hiddenComputeMs: 0 }, speedup: 1 },
		{ name: "a completed result", computation: new TimelineInterval(0, 100), issuedAt: 200, reused: true,
			expected: { toolComputeMs: 100, hiddenComputeMs: 100 }, speedup: null },
		{ name: "an in-flight result", computation: new TimelineInterval(0, 100), issuedAt: 40, reused: true,
			expected: { toolComputeMs: 100, hiddenComputeMs: 40 }, speedup: 100 / 60 },
		{ name: "a result started after issue", computation: new TimelineInterval(10, 110), issuedAt: 5, reused: true,
			expected: { toolComputeMs: 100, hiddenComputeMs: 0 }, speedup: 1 },
		{ name: "a partially executed prefix with a resumed tail", computation: new TimelineInterval(100, 104, [
			{ computation: new TimelineInterval(0, 6), reused: true },
		]), issuedAt: 4, reused: false, expected: { toolComputeMs: 10, hiddenComputeMs: 4 }, speedup: 10 / 6 },
		{ name: "disjoint calculation segments", computation: new TimelineInterval(0, 10, [], [new TimelineInterval(0, 1), new TimelineInterval(9, 10)]), issuedAt: 5, reused: true,
			expected: { toolComputeMs: 2, hiddenComputeMs: 1 }, speedup: 2 },
		{ name: "fresh work using a prepared input", computation: new TimelineInterval(100, 110, [
			{ computation: new TimelineInterval(10, 50), reused: true },
		]), issuedAt: 100, reused: false, expected: { toolComputeMs: 50, hiddenComputeMs: 40 }, speedup: 5 },
	])("measures $name independently of adoption wait", ({ computation, issuedAt, reused, expected, speedup }) => {
		const timeline = new TaskTimeline(100);
		timeline.startToolWait(200)(450);
		expect(timeline.recordTool(computation, issuedAt, reused)).toEqual({ ...expected, ...(reused || expected.hiddenComputeMs > 0 ? { reused: true } : {}) });
		expect(timeline.measure(450)).toMatchObject({ ...expected, toolWaitMs: 250 });
		expect(toolSpeedup(timeline.measure(450))).toBe(speedup);
	});

	it("keeps a producer's post-issue calculation in the denominator and removes overlapping join waits", () => {
		const child = new TimelineInterval(20, 150), timeline = new TaskTimeline(100);
		timeline.recordTool(new TimelineInterval(100, 170, [{ computation: child, reused: true, shared: [
			new TimelineInterval(105, 140), new TimelineInterval(130, 150),
		] }]), 100);
		expect(timeline.measure(170)).toMatchObject({ toolComputeMs: 155, hiddenComputeMs: 80 });
		const repeated = new TaskTimeline(100);
		repeated.recordTool(new TimelineInterval(100, 170, [{ computation: child, reused: true }, { computation: child, reused: true }]), 100);
		expect(repeated.measure(170)).toMatchObject({ toolComputeMs: 200, hiddenComputeMs: 80 });
	});

	it("sums independent native calls even when their clocks overlap", () => {
		const timeline = new TaskTimeline(0);
		for (const end of [100, 90, 70]) timeline.recordTool(new TimelineInterval(40, end), 100_000);
		expect(timeline.measure(100)).toMatchObject({ toolComputeMs: 140, hiddenComputeMs: 0 });
		expect(toolSpeedup(timeline.measure(100))).toBe(1);
	});

	it("snapshots endpoints and deduplicates identity within a call while retaining independent later reuse", () => {
		const input = { startedAt: 120, completedAt: 160 }, first = TimelineInterval.from(input);
		const second = new TimelineInterval(120, 160), timeline = new TaskTimeline(100);
		input.completedAt = 1000;
		expect(first.completedAt).toBe(160);
		expect(TimelineInterval.from(first)).toBe(first);
		expect(Reflect.set(first, "completedAt", 1000)).toBe(false);
		expect(timeline.recordCall([first, first, second].map((computation, index) => ({ computation, reused: index > 0 })), 200))
			.toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 80 });
		for (let repeat = 0; repeat < 3; repeat++) timeline.recordTool(first, 210 + repeat * 10, true);
		expect(timeline.measure(300)).toMatchObject({ toolComputeMs: 200, hiddenComputeMs: 200 });
		expect(new TimelineInterval(Number.NaN, -1)).toEqual({ startedAt: 0, completedAt: 0 });
		expect(timeline.recordTool(new TimelineInterval(140, 120), 140)).toMatchObject({ toolComputeMs: 0, hiddenComputeMs: 0 });
	});

	it("isolates concurrent evaluations and excludes rejected input work from their callers", async () => {
		const used = new TimelineInterval(10, 30), rejected = new TimelineInterval(30, 90), error = new Error("invalid proof");
		const [left, right] = await Promise.all([used, rejected].map(computation => TimelineInterval.collect(async () => {
			await expect(TimelineInterval.collect(() => { TimelineInterval.use(rejected); throw error; })).rejects.toBe(error);
			await Promise.resolve(); TimelineInterval.use(computation); TimelineInterval.use(computation);
		})));
		for (const [evaluation, saved] of [[left, 20], [right, 60]] as const) {
			const timeline = new TaskTimeline(100);
			timeline.startToolWait(100)(110);
			const accepted = new TimelineInterval(100, 110, evaluation.dependencies);
			expect(timeline.recordTool(accepted, 100)).toMatchObject({ toolComputeMs: 10 + saved, hiddenComputeMs: saved });
			expect(timeline.measure(110)).toMatchObject({ toolComputeMs: 10 + saved, hiddenComputeMs: saved, toolWaitMs: 10 });
		}
	});

	it.each([false, true])("deduplicates a retained process and its live partial computation (live first=%s)", liveFirst => {
		const live = TimelineInterval.retained("process", 40, new TimelineInterval(10, 50));
		const cached = TimelineInterval.retained("process", 40), other = TimelineInterval.retained("other", 5);
		const timeline = new TaskTimeline(100);
		timeline.startToolWait(100)(110);
		timeline.recordTool(new TimelineInterval(100, 110, (liveFirst ? [live, cached, cached, other] : [cached, live, other])
			.map(computation => ({ computation, reused: true }))), 100_000);
		expect(timeline.measure(110)).toMatchObject({ toolComputeMs: 55, hiddenComputeMs: 40, hiddenComputeIncomplete: true });
		expect(toolSpeedup(timeline.measure(110))).toBeNull();
	});

	it.each([false, true])("preserves owned parallelism and credits the work independently consumed by later calls (adopted=%s)", async adopted => {
		const captured = await TimelineInterval.collect(() => [new TimelineInterval(20, 60), new TimelineInterval(40, 80)].map(TimelineInterval.own));
		const parent = new TimelineInterval(10, 90, captured.dependencies);
		const borrowed = await TimelineInterval.collect(() => captured.output.forEach(TimelineInterval.use));
		const query = new TimelineInterval(100, 110, borrowed.dependencies);
		for (const childFirst of [false, true]) {
			const measured = new TaskTimeline(0);
			for (const computation of childFirst ? [query, parent] : [parent, query]) measured.recordTool(computation, 100_000, computation === parent && adopted);
			expect(measured.measure(110)).toMatchObject({ toolComputeMs: 150, hiddenComputeMs: adopted ? 140 : 60 });
			measured.recordTool(new TimelineInterval(115, 120, [{ computation: TimelineInterval.retained("external", 7), reused: true }]), 100_000);
			expect(measured.measure(120)).toMatchObject({ toolComputeMs: 162, hiddenComputeMs: adopted ? 140 : 60, hiddenComputeIncomplete: true });
		}
		const partial = new TaskTimeline(0);
		partial.recordTool(query, 100_000);
		expect(partial.measure(110)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 60 });
	});

	it("snapshots dependency joins without double-counting them as fresh computation", () => {
		const child = new TimelineInterval(20, 150), shared = [{ startedAt: 105, completedAt: 150 }];
		const inputs = [{ computation: child, reused: true, shared }], native = new TimelineInterval(100, 170, inputs);
		shared[0]!.completedAt = 170;
		inputs.length = 0;
		expect(JSON.stringify(native)).toBe('{"startedAt":100,"completedAt":170}');
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(native, 100_000)).toMatchObject({ toolComputeMs: 155, hiddenComputeMs: 130 });
		timeline.recordTool(child, 100_000, true);
		expect(timeline.measure(170)).toMatchObject({ toolComputeMs: 285, hiddenComputeMs: 260 });
		const serialChild = new TimelineInterval(100, 150), serial = new TaskTimeline(0);
		serial.recordTool(new TimelineInterval(100, 160, [{ computation: serialChild, reused: true, shared: [serialChild] }]), 100_000);
		expect(serial.measure(160)).toMatchObject({ toolComputeMs: 60, hiddenComputeMs: 50 });
	});

	it.each([false, true])("deduplicates a parent's overlapping input roots while retaining independent work (child first=%s)", childFirst => {
		const left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
		const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, reused: true, shared: [computation] })));
		const timeline = new TaskTimeline(0);
		timeline.recordCall((childFirst ? [left, right, parent] : [parent, left, right]).map(computation => ({ computation, reused: true })), 100_000);
		// The independent children retain their durations; the parent adds only its remaining 20 ms.
		expect(timeline.measure(100)).toMatchObject({ toolComputeMs: 100, hiddenComputeMs: 100 });
	});

	it("credits complete recorded execution on every reuse without deducting adoption costs", () => {
		const computation = new TimelineInterval(1000, 2000), timeline = new TaskTimeline(0);
		timeline.startToolWait(61_000)(61_250);
		timeline.recordCall([{ computation, reused: true }, { computation, reused: true }], 100_000);
		expect(timeline.measure(61_250)).toMatchObject({ toolComputeMs: 1000, hiddenComputeMs: 1000, toolWaitMs: 250 });
		timeline.recordTool(computation, 100_000, true);
		expect(timeline.measure(121_250)).toMatchObject({ toolComputeMs: 2000, hiddenComputeMs: 2000 });
	});

	it.each([false, true])("unions a reused parent's owned children including normalized inputs (plain=%s)", plain => {
		const children = [[20, 60], [40, 80]].map(([startedAt, completedAt]) => plain
			? { startedAt: startedAt!, completedAt: completedAt! } : new TimelineInterval(startedAt!, completedAt!));
		const parent = new TimelineInterval(10, 90, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(parent, 100_000)).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 0 });
		timeline.recordTool(parent, 100_000, true);
		expect(timeline.measure(210)).toMatchObject({ toolComputeMs: 160, hiddenComputeMs: 80 });
		timeline.recordTool(parent, 100_000, true);
		expect(timeline.measure(310)).toMatchObject({ toolComputeMs: 240, hiddenComputeMs: 160 });
	});

	it("credits only the consumed preparation and never its unconsumed owner or siblings", () => {
		const children = [new TimelineInterval(20, 40), new TimelineInterval(30, 80), new TimelineInterval(60, 100)];
		const producer = new TimelineInterval(10, 110, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		const consumer = new TimelineInterval(300, 310, [{ computation: children[0]!, reused: true }]);
		const timeline = new TaskTimeline(0);
		timeline.recordTool(producer, 100_000); timeline.recordTool(consumer, 100_000);
		expect(timeline.measure(310)).toMatchObject({ toolComputeMs: 130, hiddenComputeMs: 20 });
	});

	it("does not credit native misses or newly owned children without a successful reuse receipt", () => {
		const child = new TimelineInterval(20, 60);
		const native = new TimelineInterval(10, 90, [{ computation: child, owned: true, shared: [child] }]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(native, 100_000)).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 0 });
	});

	it("counts reconstruction inputs once across one Actor call's provider and projection", () => {
		const preparation = new TimelineInterval(10, 50);
		const reconstructed = new TimelineInterval(100, 180, [{ computation: preparation, reused: true }]);
		const projected = new TimelineInterval(180, 190, [{ computation: preparation, reused: true }]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordCall([{ computation: reconstructed }, { computation: projected }], 100_000))
			.toMatchObject({ toolComputeMs: 130, hiddenComputeMs: 40 });
	});

	it.each([false, true])("deduplicates retained/live identities and preserves live timing regardless of order (live first=%s)", liveFirst => {
		for (const priorMs of [0, 40]) {
			const live = TimelineInterval.retained("same-process", priorMs, new TimelineInterval(10, 50));
			const retained = TimelineInterval.retained("same-process", priorMs), external = TimelineInterval.retained("external", 7);
			const timeline = new TaskTimeline(0);
			timeline.recordCall((liveFirst ? [live, retained, external] : [retained, live, external]).map(computation => ({ computation, reused: true })), 100_000);
			expect(timeline.measure(210)).toMatchObject({ toolComputeMs: 47, hiddenComputeMs: 40, hiddenComputeIncomplete: true });
		}
		const historical = new TaskTimeline(100);
		historical.recordTool(TimelineInterval.retained("historical", 40), 100_000, true);
		expect(historical.measure(100_010)).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 0, hiddenComputeIncomplete: true });
	});

	it.each([false, true])("excludes only work paused by validation while retaining a known sibling's interval (child paused=%s)", async paused => {
		const sibling = new TimelineInterval(20, 80), validation = new TimelineInterval(110, 140);
		const fresh = new TimelineInterval(100, 150, paused ? [{ computation: validation, overhead: true }] : []);
		const measured = await TimelineInterval.collect(() => {
			TimelineInterval.own(sibling); TimelineInterval.own(fresh); TimelineInterval.exclude(validation);
		});
		const parent = new TimelineInterval(0, 150, measured.dependencies), timeline = new TaskTimeline(0);
		expect(timeline.recordTool(parent, 100_000)).toMatchObject({ toolComputeMs: paused ? 120 : 150, hiddenComputeMs: 0 });
		expect(timeline.recordTool(sibling, 100_000, true)).toMatchObject({ toolComputeMs: 60, hiddenComputeMs: 60 });
	});

	it("keeps unproven fresh computation unknown while preserving successful reuse receipts", () => {
		const prepared = new TimelineInterval(10, 50);
		const root = new TimelineInterval(100, 110, [
			{ computation: prepared, reused: true },
			{ computation: new TimelineInterval(0, 0), overhead: true, computeUncertain: true },
		]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(root, 100_000)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40 });
		timeline.recordTool(new TimelineInterval(120, 130), 100_000);
		expect(timeline.measure(130)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40 });
		expect(toolSpeedup(timeline.measure(130))).toBeNull();
		const adopted = new TaskTimeline(200);
		expect(adopted.recordTool(root, 100_000, true)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 50, hiddenComputeIncomplete: true });
		expect(adopted.measure(300)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 50, hiddenComputeIncomplete: true });
		expect(toolSpeedup(adopted.measure(300))).toBeNull();
	});

	it("attributes owned preparations to their producer while borrowed work keeps its own provenance", () => {
		const owned = new TimelineInterval(20, 40), borrowed = new TimelineInterval(0, 10), unknown = new TimelineInterval(50, 55);
		const producer = new TimelineInterval(10, 50, [
			{ computation: owned, owned: true, shared: [owned] },
			{ computation: borrowed, reused: true }, { computation: unknown, reused: true },
		]);
		TimelineInterval.producedBy(producer, { source: "pattern", mode: "whole" });
		TimelineInterval.producedBy(borrowed, { source: "drafter", mode: "input" });
		TimelineInterval.producedBy(owned, { source: "consumer", mode: "replacement" });
		const timeline = new TaskTimeline(100);
		expect(timeline.recordTool(producer, 100_000, true)).toMatchObject({ toolComputeMs: 55, hiddenComputeMs: 55, hiddenByMode: [
			{ source: "pattern", mode: "whole", hiddenComputeMs: 40 }, { source: "drafter", mode: "input", hiddenComputeMs: 10 },
		] });
		const partial = new TimelineInterval(100, 110, [owned, borrowed, unknown, owned].map(computation => ({ computation, reused: true })));
		for (let repeat = 0; repeat < 2; repeat++) expect(timeline.recordTool(partial, 100_000)).toMatchObject({ toolComputeMs: 45, hiddenComputeMs: 35, hiddenByMode: [
			{ source: "pattern", mode: "whole", hiddenComputeMs: 20 }, { source: "drafter", mode: "input", hiddenComputeMs: 10 },
		] });
	});

	it.each([false, true])("leaves ambiguous owned overlaps unassigned without exceeding gross reuse (reverse=%s)", reverse => {
		const left = new TimelineInterval(10, 50), right = new TimelineInterval(30, 70), unknown = new TimelineInterval(60, 90);
		const children = reverse ? [unknown, right, left] : [left, right, unknown];
		const parent = new TimelineInterval(0, 100, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		TimelineInterval.producedBy(left, { source: "pattern", mode: "left" });
		TimelineInterval.producedBy(right, { source: "pattern", mode: "right" });
		const result = new TaskTimeline(100).recordCall([parent, ...children].map(computation => ({ computation, reused: true })), 100_000);
		expect(result).toMatchObject({ toolComputeMs: 100, hiddenComputeMs: 100 });
		expect(result.hiddenByMode).toEqual([
			{ source: "pattern", mode: "left", hiddenComputeMs: 20 }, { source: "pattern", mode: "right", hiddenComputeMs: 10 },
		]);
		expect(result.hiddenByMode!.reduce((sum, value) => sum + value.hiddenComputeMs, 0)).toBeLessThanOrEqual(result.hiddenComputeMs);
	});

	it.each([false, true])("deduplicates retained attribution, preserves write-once provenance and rejects conflicting aliases (live first=%s)", liveFirst => {
		const live = TimelineInterval.retained("same", 40, new TimelineInterval(10, 50)), retained = TimelineInterval.retained("same", 40);
		const producer = { source: "pattern", mode: "owner" };
		TimelineInterval.producedBy(live, producer);
		producer.mode = "mutated";
		TimelineInterval.producedBy(live, { source: "other", mode: "supporter" });
		const roots = (liveFirst ? [live, retained] : [retained, live]).map(computation => ({ computation, reused: true }));
		expect(new TaskTimeline(100).recordCall(roots, 100_000)).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40,
			hiddenByMode: [{ source: "pattern", mode: "owner", hiddenComputeMs: 40 }] });
		TimelineInterval.producedBy(retained, { source: "other", mode: "conflict" });
		expect(new TaskTimeline(100).recordCall(roots, 100_000)).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40 });
	});

	it("propagates uncertain excluded work through collection without inventing reuse", async () => {
		const preparation = new TimelineInterval(10, 50);
		TimelineInterval.producedBy(preparation, { source: "pattern", mode: "failed" });
		const collected = await TimelineInterval.collect(() => TimelineInterval.exclude(preparation, true));
		expect(new TaskTimeline(0).recordTool(new TimelineInterval(10, 60, collected.dependencies), 100_000))
			.toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 0 });
	});
});

describe("persisted computation evidence", () => {
	const restore = (computation: TimelineInterval): TimelineInterval => {
		const evidence = TimelineInterval.serialize(computation);
		expect(evidence).toBeDefined();
		const restored = TimelineInterval.restore(JSON.parse(JSON.stringify(evidence)));
		expect(restored).toBeDefined();
		return restored!;
	};
	const reused = (computations: readonly TimelineInterval[]) => new TaskTimeline(0).recordCall(computations.map(computation => ({ computation, reused: true })), 100_000);

	it("roundtrips original input graphs, explicit exclusions and physical producer attribution", () => {
		const child = new TimelineInterval(110, 150), borrowed = new TimelineInterval(0, 40);
		const parent = new TimelineInterval(100, 170, [
			{ computation: child, owned: true, shared: [child] }, { computation: borrowed, reused: true },
			{ computation: new TimelineInterval(100, 110), overhead: true },
		]);
		TimelineInterval.producedBy(parent, { source: "pattern", mode: "whole" });
		TimelineInterval.producedBy(borrowed, { source: "drafter", mode: "input" });
		const expected = { toolComputeMs: 100, hiddenComputeMs: 100, hiddenByMode: [
			{ source: "pattern", mode: "whole", hiddenComputeMs: 60 }, { source: "drafter", mode: "input", hiddenComputeMs: 40 },
		] };
		expect(reused([parent])).toMatchObject(expected);
		const left = restore(parent), right = restore(parent);
		expect(left).not.toBe(right);
		expect(reused([left, right])).toMatchObject(expected);
		for (const roots of [[parent, left], [left, parent]]) expect(reused(roots)).toMatchObject(expected);
		const graph = TimelineInterval.serialize(parent)!;
		expect(Object.isFrozen(graph)).toBe(true);
		expect(Object.isFrozen(graph.nodes.find(node => node.id === graph.root)!.inputs)).toBe(true);
	});

	it.each([false, true])("deduplicates independently restored parents sharing a borrowed child (reverse=%s)", reverse => {
		const child = new TimelineInterval(0, 40);
		const left = new TimelineInterval(100, 110, [{ computation: child, reused: true }]);
		const right = new TimelineInterval(200, 220, [{ computation: child, reused: true }]);
		const roots = [restore(left), restore(right), restore(child)];
		expect(reused(reverse ? roots.reverse() : roots)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 70 });
		const timeline = new TaskTimeline(0);
		for (const root of roots) timeline.recordTool(root, 100_000, true);
		expect(timeline.measure(500)).toMatchObject({ hiddenComputeMs: 150 });
	});

	it.each([false, true])("unions persisted owned siblings without selecting their ancestor or other siblings (reverse=%s)", reverse => {
		const left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80), unconsumed = new TimelineInterval(90, 200);
		const parent = new TimelineInterval(10, 210, [left, right, unconsumed].map(computation => ({ computation, owned: true, shared: [computation] })));
		const roots = [restore(left), restore(right)];
		expect(reused(reverse ? roots.reverse() : roots)).toMatchObject({ toolComputeMs: 60, hiddenComputeMs: 60 });
		expect(reused([restore(left)])).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40 });
		const complete = [restore(parent), restore(left), restore(right)];
		expect(reused(reverse ? complete.reverse() : complete)).toMatchObject({ toolComputeMs: 200, hiddenComputeMs: 200 });
	});

	it.each([false, true])("preserves early production groups after the enclosing live interval is created (reverse=%s)", reverse => {
		const production = {}, left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
		TimelineInterval.group(left, production); TimelineInterval.group(right, production);
		const savedLeft = restore(left), savedRight = restore(right);
		const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, owned: true, shared: [computation] })));
		expect(reused([savedLeft, savedRight])).toMatchObject({ toolComputeMs: 60, hiddenComputeMs: 60 });
		for (const enclosing of [parent, restore(parent)]) {
			const roots = [enclosing, savedLeft, savedRight];
			expect(reused(reverse ? roots.reverse() : roots)).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 80 });
		}
		const separate = new TimelineInterval(20, 60);
		TimelineInterval.group(separate, {});
		expect(reused([savedLeft, restore(separate)])).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 80 });
	});

	it("keeps graph identity stable when a live or restored result receives a certificate alias", () => {
		const original = new TimelineInterval(10, 50), saved = restore(original);
		TimelineInterval.retained("later-certificate-alias", 999, original);
		TimelineInterval.retained("another-certificate-alias", 999, saved);
		expect(reused([original, saved, restore(original)])).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40 });
		const measuredLegacy = TimelineInterval.retained("independently-measured", 25);
		const retainedLegacy = restore(measuredLegacy);
		TimelineInterval.retained("later-measured-alias", 999, retainedLegacy);
		expect(reused([measuredLegacy, retainedLegacy])).toMatchObject({ toolComputeMs: 25, hiddenComputeMs: 0, hiddenComputeIncomplete: true });
	});

	it("canonicalizes nested owned aliases when a session records both a parent and its child", () => {
		const child = new TimelineInterval(20, 60);
		const parent = new TimelineInterval(10, 80, [{ computation: child, owned: true, shared: [child] }]);
		const root = new TimelineInterval(0, 100, [parent, child].map(computation => ({ computation, owned: true, shared: [computation] })));
		expect(reused([root])).toMatchObject({ toolComputeMs: 100, hiddenComputeMs: 100 });
		const restored = restore(root);
		expect(reused([restored, restore(child), restore(parent)])).toMatchObject({ toolComputeMs: 100, hiddenComputeMs: 100 });
	});

	it.each([undefined, 0, 1000, -1000])("clips restored computation at Actor issue across clock origins (offset=%s)", offset => {
		const original = new TimelineInterval(100, 200), graph = TimelineInterval.serialize(original)!;
		const foreign = TimelineInterval.restore({ ...graph, nodes: graph.nodes.map(node => ({ ...node,
			clock: "earlier-process", timeOrigin: offset === undefined ? undefined : performance.timeOrigin + offset })) })!;
		const timing = new TaskTimeline(0).recordCall([{ computation: foreign, reused: true }], 150, 250);
		expect(timing).toEqual({ toolComputeMs: 100, hiddenComputeMs: offset === undefined || offset > 0 ? 0 : offset < 0 ? 100 : 50,
			reused: true, ...(offset === undefined ? { hiddenComputeIncomplete: true } : { adoptionWaitMs: offset === 0 ? 50 : 100 }) });
		expect(TimelineInterval.serialize(foreign)?.nodes[0]?.timeOrigin).toBe(offset === undefined ? undefined : performance.timeOrigin + offset);
	});

	it("keeps old monotonic coordinates from cutting unrelated work after a process restart", async () => {
		const graph = TimelineInterval.serialize(new TimelineInterval(100, 140))!;
		const foreign = TimelineInterval.restore({ ...graph, nodes: graph.nodes.map(node => ({ ...node, clock: "earlier-process" })) })!;
		const collected = await TimelineInterval.collect(() => TimelineInterval.use(foreign));
		// The numerical coordinates overlap, but the producer and this Actor have different clocks.
		const current = new TimelineInterval(120, 130, collected.dependencies);
		expect(new TaskTimeline(0).recordTool(current, 100_000)).toMatchObject({ toolComputeMs: 50, hiddenComputeMs: 40 });
		expect(reused([restore(current), restore(foreign)])).toMatchObject({ toolComputeMs: 50, hiddenComputeMs: 50 });
		// An explicit current-process join still removes its measured wait without clipping it to the old clock.
		const joined = new TimelineInterval(200, 210, [{ computation: foreign, reused: true, shared: [new TimelineInterval(202, 207)] }]);
		expect(new TaskTimeline(0).recordTool(joined, 100_000)).toMatchObject({ toolComputeMs: 45, hiddenComputeMs: 40 });
		expect(reused([restore(joined)])).toMatchObject({ toolComputeMs: 45, hiddenComputeMs: 45 });
	});

	it("never unions matching numerical spans from different monotonic clocks", () => {
		const production = {}, live = new TimelineInterval(100, 140);
		TimelineInterval.group(live, production);
		const graph = TimelineInterval.serialize(live)!;
		const foreign = TimelineInterval.restore({ ...graph, root: "foreign-root", nodes: graph.nodes.map(node => ({ ...node,
			id: "foreign-root", clock: "earlier-process" })) })!;
		for (const roots of [[live, foreign], [foreign, live]]) expect(reused(roots)).toMatchObject({ toolComputeMs: 80, hiddenComputeMs: 80 });
	});

	it("retains known spans while marking missing reuse evidence and incomplete producer coverage", () => {
		const missing = TimelineInterval.unknownReuse("unrecorded"), known = new TimelineInterval(10, 50);
		const parent = new TimelineInterval(100, 120, [
			{ computation: missing, reused: true }, { computation: known, reused: true },
			{ computation: new TimelineInterval(100, 105), overhead: true, computeUncertain: true },
		]);
		expect(reused([restore(missing)])).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 0, hiddenComputeIncomplete: true });
		expect(reused([restore(parent)])).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 55, hiddenComputeIncomplete: true });
		const timeline = new TaskTimeline(100);
		expect(timeline.recordTool(parent, 100_000)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40, hiddenComputeIncomplete: true });
		timeline.recordTool(new TimelineInterval(200, 210), 100_000);
		expect(timeline.measure(300)).toMatchObject({ toolComputeMs: undefined, hiddenComputeMs: 40, hiddenComputeIncomplete: true });
		const knownAlias = TimelineInterval.retained("unrecorded", 40, new TimelineInterval(10, 50));
		for (const roots of [[missing, knownAlias], [knownAlias, missing]]) expect(reused(roots)).toMatchObject({ toolComputeMs: 40, hiddenComputeMs: 40 });
	});

	it("rejects malformed or cyclic graphs without issuing a successful receipt", async () => {
		const graph = TimelineInterval.serialize(new TimelineInterval(10, 50))!;
		const root = graph.nodes[0]!;
		const invalid = [null, {}, { ...graph, version: 999 }, { ...graph, root: "absent" },
			{ ...graph, nodes: [{ ...root, spans: [{ startedAt: 0, completedAt: 20 }] }] },
			{ ...graph, nodes: [{ ...root, spans: [{ startedAt: 10, completedAt: 51 }] }] },
			{ ...graph, nodes: [{ ...root, clock: undefined }] },
			{ ...graph, nodes: [root, root] }, { ...graph, nodes: [{ ...root, completedAt: -1 }] },
			{ ...graph, nodes: [{ ...root, startedAt: NaN }] },
			{ ...graph, nodes: [{ ...root, timeOrigin: NaN }] },
			{ ...graph, nodes: [{ ...root, inputs: [{ id: "child" }] }, { ...root, id: "child", timeOrigin: root.timeOrigin! + 1 }] },
			{ ...graph, nodes: [{ ...root, inputs: [{ id: root.id }] }] },
			{ ...graph, nodes: [{ ...root, inputs: [{ id: "absent" }] }] },
			{ ...graph, nodes: [{ ...root, inputs: [{ id: root.id, reused: "yes" }] }] },
		];
		const collected = await TimelineInterval.collect(() => {
			for (const value of invalid) {
				expect(normalizeTimelineComputation(value)).toBeUndefined();
				expect(TimelineInterval.restore(value)).toBeUndefined();
			}
			restore(new TimelineInterval(0, 100));
		});
		expect(collected.dependencies).toEqual([]);
	});

	it("retains large calculation graphs and every excluded span through JSON persistence", () => {
		const children = Array.from({ length: 4096 }, (_, index) => new TimelineInterval(index, index + 1));
		const largeGraph = new TimelineInterval(0, 10_000, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		const child = new TimelineInterval(0, 1);
		const manyEdges = new TimelineInterval(0, 10_000, Array.from({ length: 4096 }, () => ({ computation: child, overhead: true })));
		const spans = Array.from({ length: 131_072 }, (_, index) => new TimelineInterval(index, index + 0.5));
		const manySpans = new TimelineInterval(0, spans.length, [{ computation: new TimelineInterval(0, spans.length), overhead: true, shared: spans }]);
		const activeSpans = new TimelineInterval(0, spans.length, [], spans);
		const groups = TimelineInterval.retained("long-id".repeat(128), 0, new TimelineInterval(0, 10_000));
		for (let index = 0; index < 4096; index++) TimelineInterval.group(groups, {});
		let deep = child, owned = child;
		for (let index = 0; index < 8192; index++) {
			deep = new TimelineInterval(index + 1, index + 2, [{ computation: deep }]);
			owned = new TimelineInterval(index + 1, index + 2, [{ computation: owned, owned: true }]);
		}
		TimelineInterval.producedBy(owned, { source: "pattern", mode: "whole" });
		for (const [computation, total] of [[largeGraph, 10_000], [manyEdges, 9999], [manySpans, 65_536], [activeSpans, 65_536],
			[groups, 10_000], [deep, 8193], [TimelineInterval.attempted(deep), 8193]] as const) {
			const saved = restore(computation);
			expect(new TaskTimeline(0).recordTool(saved, 1_000_000, true)).toMatchObject({ toolComputeMs: total, hiddenComputeMs: total });
			for (const issuedAt of [0, 500, 1_000_000]) expect(new TaskTimeline(0).recordCall([{ computation: saved, reused: true }], issuedAt, 1_000_000))
				.toEqual(new TaskTimeline(0).recordCall([{ computation, reused: true }], issuedAt, 1_000_000));
		}
		expect(Buffer.byteLength(JSON.stringify(TimelineInterval.serialize(largeGraph)))).toBeGreaterThan(128 * 1024);
	});
});
