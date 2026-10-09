import { describe, expect, it, vi } from "vitest";
import { deferred } from "./async.ts";
import { TaskTimeline, TimelineInterval, toolSpeedup, normalizeTimelineComputation, TIMELINE_COMPUTATION_LIMITS, type ComputationReuseShare } from "../src/task-timing.ts";

describe("gross Actor computation and successful reuse", () => {
	it("records calculation segments without clocks or receipts for control work", async () => {
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
			expect(new TaskTimeline(0).recordTool(evaluation.computation)).toEqual({ actorComputeMs: 30, reusedExecutionMs: 40 });
			expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(graph)!, true)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 70 });
			clock.mockClear();
			await TimelineInterval.outside(() => { now += 1000; });
			expect(clock).not.toHaveBeenCalled();
		} finally { clock.mockRestore(); }
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
					expect(new TaskTimeline(0).recordTool(TimelineInterval.current(5, 50))).toEqual({ actorComputeMs: 15, reusedExecutionMs: 0 });
				});
				TimelineInterval.own(child.computation); now = 60;
			});
			expect(new TaskTimeline(0).recordTool(evaluation.computation)).toEqual({ actorComputeMs: 30, reusedExecutionMs: 0 });
		} finally { clock.mockRestore(); }
	});

	it("unions complete and interrupted tool waits without using them as the computation denominator", () => {
		const timeline = new TaskTimeline(100);
		timeline.recordTool(new TimelineInterval(0, 100));
		timeline.recordTool(new TimelineInterval(110, 130), true);
		timeline.startToolWait(140)(170);
		timeline.startToolWait(150)(180);
		const interrupted = timeline.startToolWait(190);
		expect(timeline.measure(200)).toMatchObject({ toolWaitMs: 50, actorComputeMs: 100, reusedExecutionMs: 20 });
		expect(toolSpeedup(timeline.measure(200))).toBe(1.2);
		interrupted(195); interrupted(250);
		expect(timeline.measure(200).toolWaitMs).toBe(45);
		expect(toolSpeedup(timeline.measure(200))).toBe(1.2);
		expect(toolSpeedup(new TaskTimeline(200).measure(300))).toBeNull();
		for (const actorComputeMs of [0, -1, NaN, Infinity]) expect(toolSpeedup({ actorComputeMs, reusedExecutionMs: 20 })).toBeNull();
		for (const reusedExecutionMs of [-1, NaN, Infinity]) expect(toolSpeedup({ actorComputeMs: 50, reusedExecutionMs })).toBeNull();
		expect(toolSpeedup({ actorComputeMs: 50, reusedExecutionMs: 100, reusedExecutionIncomplete: true })).toBeNull();
	});

	it.each([
		{ name: "native work without reuse", computation: new TimelineInterval(0, 100), reused: false,
			expected: { actorComputeMs: 100, reusedExecutionMs: 0 }, speedup: 1 },
		{ name: "a fully adopted result", computation: new TimelineInterval(0, 100), reused: true,
			expected: { actorComputeMs: 0, reusedExecutionMs: 100 }, speedup: null },
		{ name: "fresh work using a prepared input", computation: new TimelineInterval(100, 110, [
			{ computation: new TimelineInterval(10, 50), reused: true },
		]), reused: false, expected: { actorComputeMs: 10, reusedExecutionMs: 40 }, speedup: 5 },
	])("measures $name independently of adoption wait", ({ computation, reused, expected, speedup }) => {
		const timeline = new TaskTimeline(100);
		timeline.startToolWait(200)(450);
		expect(timeline.recordTool(computation, reused)).toEqual(expected);
		expect(timeline.measure(450)).toMatchObject({ ...expected, toolWaitMs: 250 });
		expect(toolSpeedup(timeline.measure(450))).toBe(speedup);
	});

	it("credits a consumed input's full execution while removing its overlapping join from fresh work", () => {
		const child = new TimelineInterval(20, 150), timeline = new TaskTimeline(100);
		timeline.recordTool(new TimelineInterval(100, 170, [{ computation: child, reused: true, shared: [
			new TimelineInterval(105, 140), new TimelineInterval(130, 150),
		] }]));
		expect(timeline.measure(170)).toMatchObject({ actorComputeMs: 25, reusedExecutionMs: 130 });
		const repeated = new TaskTimeline(100);
		repeated.recordTool(new TimelineInterval(100, 170, [{ computation: child, reused: true }, { computation: child, reused: true }]));
		expect(repeated.measure(170)).toMatchObject({ actorComputeMs: 70, reusedExecutionMs: 130 });
	});

	it("sums independent native calls even when their clocks overlap", () => {
		const timeline = new TaskTimeline(0);
		for (const end of [100, 90, 70]) timeline.recordTool(new TimelineInterval(40, end));
		expect(timeline.measure(100)).toMatchObject({ actorComputeMs: 140, reusedExecutionMs: 0 });
		expect(toolSpeedup(timeline.measure(100))).toBe(1);
	});

	it("snapshots endpoints and deduplicates identity within a call while retaining independent later reuse", () => {
		const input = { startedAt: 120, completedAt: 160 }, first = TimelineInterval.from(input);
		const second = new TimelineInterval(120, 160), timeline = new TaskTimeline(100);
		input.completedAt = 1000;
		expect(first.completedAt).toBe(160);
		expect(TimelineInterval.from(first)).toBe(first);
		expect(Reflect.set(first, "completedAt", 1000)).toBe(false);
		expect(timeline.recordCall([first, first, second].map(computation => ({ computation, reused: true }))))
			.toEqual({ actorComputeMs: 0, reusedExecutionMs: 80 });
		for (let repeat = 0; repeat < 3; repeat++) timeline.recordTool(first, true);
		expect(timeline.measure(300)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 200 });
		expect(new TimelineInterval(Number.NaN, -1)).toEqual({ startedAt: 0, completedAt: 0 });
		expect(timeline.recordTool(new TimelineInterval(140, 120))).toEqual({ actorComputeMs: 0, reusedExecutionMs: 0 });
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
			expect(timeline.recordTool(accepted)).toEqual({ actorComputeMs: 10, reusedExecutionMs: saved });
			expect(timeline.measure(110)).toMatchObject({ actorComputeMs: 10, reusedExecutionMs: saved, toolWaitMs: 10 });
		}
	});

	it.each([false, true])("deduplicates a retained process and its live partial computation (live first=%s)", liveFirst => {
		const live = TimelineInterval.retained("process", 40, new TimelineInterval(10, 50));
		const cached = TimelineInterval.retained("process", 40), other = TimelineInterval.retained("other", 5);
		const timeline = new TaskTimeline(100);
		timeline.startToolWait(100)(110);
		timeline.recordTool(new TimelineInterval(100, 110, (liveFirst ? [live, cached, cached, other] : [cached, live, other])
			.map(computation => ({ computation, reused: true }))));
		expect(timeline.measure(110)).toMatchObject({ actorComputeMs: 10, reusedExecutionMs: 45 });
		expect(toolSpeedup(timeline.measure(110))).toBe(5.5);
	});

	it.each([false, true])("preserves owned parallelism and credits the work independently consumed by later calls (adopted=%s)", async adopted => {
		const captured = await TimelineInterval.collect(() => [new TimelineInterval(20, 60), new TimelineInterval(40, 80)].map(TimelineInterval.own));
		const parent = new TimelineInterval(10, 90, captured.dependencies);
		const borrowed = await TimelineInterval.collect(() => captured.output.forEach(TimelineInterval.use));
		const query = new TimelineInterval(100, 110, borrowed.dependencies);
		for (const childFirst of [false, true]) {
			const measured = new TaskTimeline(0);
			for (const computation of childFirst ? [query, parent] : [parent, query]) measured.recordTool(computation, computation === parent && adopted);
			expect(measured.measure(110)).toMatchObject({ actorComputeMs: adopted ? 10 : 90, reusedExecutionMs: adopted ? 140 : 60 });
			measured.recordTool(new TimelineInterval(115, 120, [{ computation: TimelineInterval.retained("external", 7), reused: true }]));
			expect(measured.measure(120)).toMatchObject({ actorComputeMs: adopted ? 15 : 95, reusedExecutionMs: adopted ? 147 : 67 });
		}
		const partial = new TaskTimeline(0);
		partial.recordTool(query);
		expect(partial.measure(110)).toMatchObject({ actorComputeMs: 10, reusedExecutionMs: 60 });
	});

	it("snapshots dependency joins without double-counting them as fresh computation", () => {
		const child = new TimelineInterval(20, 150), shared = [{ startedAt: 105, completedAt: 150 }];
		const inputs = [{ computation: child, reused: true, shared }], native = new TimelineInterval(100, 170, inputs);
		shared[0]!.completedAt = 170;
		inputs.length = 0;
		expect(JSON.stringify(native)).toBe('{"startedAt":100,"completedAt":170}');
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(native)).toEqual({ actorComputeMs: 25, reusedExecutionMs: 130 });
		timeline.recordTool(child, true);
		expect(timeline.measure(170)).toMatchObject({ actorComputeMs: 25, reusedExecutionMs: 260 });
		const serialChild = new TimelineInterval(100, 150), serial = new TaskTimeline(0);
		serial.recordTool(new TimelineInterval(100, 160, [{ computation: serialChild, reused: true, shared: [serialChild] }]));
		expect(serial.measure(160)).toMatchObject({ actorComputeMs: 10, reusedExecutionMs: 50 });
	});

	it.each([false, true])("deduplicates a parent's overlapping input roots while retaining independent work (child first=%s)", childFirst => {
		const left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
		const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, reused: true, shared: [computation] })));
		const timeline = new TaskTimeline(0);
		timeline.recordCall((childFirst ? [left, right, parent] : [parent, left, right]).map(computation => ({ computation, reused: true })));
		// The independent children retain their durations; the parent adds only its remaining 20 ms.
		expect(timeline.measure(100)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 100 });
	});

	it("credits complete recorded execution on every reuse without deducting adoption costs", () => {
		const computation = new TimelineInterval(1000, 2000), timeline = new TaskTimeline(0);
		timeline.startToolWait(61_000)(61_250);
		timeline.recordCall([{ computation, reused: true }, { computation, reused: true }]);
		expect(timeline.measure(61_250)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 1000, toolWaitMs: 250 });
		timeline.recordTool(computation, true);
		expect(timeline.measure(121_250)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 2000 });
	});

	it.each([false, true])("unions a reused parent's owned children including normalized inputs (plain=%s)", plain => {
		const children = [[20, 60], [40, 80]].map(([startedAt, completedAt]) => plain
			? { startedAt: startedAt!, completedAt: completedAt! } : new TimelineInterval(startedAt!, completedAt!));
		const parent = new TimelineInterval(10, 90, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(parent)).toEqual({ actorComputeMs: 80, reusedExecutionMs: 0 });
		timeline.recordTool(parent, true);
		expect(timeline.measure(210)).toMatchObject({ actorComputeMs: 80, reusedExecutionMs: 80 });
		timeline.recordTool(parent, true);
		expect(timeline.measure(310)).toMatchObject({ actorComputeMs: 80, reusedExecutionMs: 160 });
	});

	it("credits only the consumed preparation and never its unconsumed owner or siblings", () => {
		const children = [new TimelineInterval(20, 40), new TimelineInterval(30, 80), new TimelineInterval(60, 100)];
		const producer = new TimelineInterval(10, 110, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		const consumer = new TimelineInterval(300, 310, [{ computation: children[0]!, reused: true }]);
		const timeline = new TaskTimeline(0);
		timeline.recordTool(producer); timeline.recordTool(consumer);
		expect(timeline.measure(310)).toMatchObject({ actorComputeMs: 110, reusedExecutionMs: 20 });
	});

	it("does not credit native misses or newly owned children without a successful reuse receipt", () => {
		const child = new TimelineInterval(20, 60);
		const native = new TimelineInterval(10, 90, [{ computation: child, owned: true, shared: [child] }]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(native)).toEqual({ actorComputeMs: 80, reusedExecutionMs: 0 });
	});

	it("counts reconstruction inputs once across one Actor call's provider and projection", () => {
		const preparation = new TimelineInterval(10, 50);
		const reconstructed = new TimelineInterval(100, 180, [{ computation: preparation, reused: true }]);
		const projected = new TimelineInterval(180, 190, [{ computation: preparation, reused: true }]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordCall([{ computation: reconstructed }, { computation: projected }]))
			.toEqual({ actorComputeMs: 90, reusedExecutionMs: 40 });
	});

	it.each([false, true])("deduplicates retained/live identities and preserves live timing regardless of order (live first=%s)", liveFirst => {
		for (const priorMs of [0, 40]) {
			const live = TimelineInterval.retained("same-process", priorMs, new TimelineInterval(10, 50));
			const retained = TimelineInterval.retained("same-process", priorMs), external = TimelineInterval.retained("external", 7);
			const timeline = new TaskTimeline(0);
			timeline.recordCall((liveFirst ? [live, retained, external] : [retained, live, external]).map(computation => ({ computation, reused: true })));
			expect(timeline.measure(210)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 47 });
		}
		const historical = new TaskTimeline(100);
		historical.recordTool(TimelineInterval.retained("historical", 40), true);
		expect(historical.measure(100_010)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 40 });
	});

	it.each([false, true])("excludes only work paused by validation while retaining a known sibling's interval (child paused=%s)", async paused => {
		const sibling = new TimelineInterval(20, 80), validation = new TimelineInterval(110, 140);
		const fresh = new TimelineInterval(100, 150, paused ? [{ computation: validation, overhead: true }] : []);
		const measured = await TimelineInterval.collect(() => {
			TimelineInterval.own(sibling); TimelineInterval.own(fresh); TimelineInterval.exclude(validation);
		});
		const parent = new TimelineInterval(0, 150, measured.dependencies), timeline = new TaskTimeline(0);
		expect(timeline.recordTool(parent)).toEqual({ actorComputeMs: paused ? 120 : 150, reusedExecutionMs: 0 });
		expect(timeline.recordTool(sibling, true)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 60 });
	});

	it("keeps unproven fresh computation unknown while preserving successful reuse receipts", () => {
		const prepared = new TimelineInterval(10, 50);
		const root = new TimelineInterval(100, 110, [
			{ computation: prepared, reused: true },
			{ computation: new TimelineInterval(0, 0), overhead: true, computeUncertain: true },
		]);
		const timeline = new TaskTimeline(0);
		expect(timeline.recordTool(root)).toEqual({ actorComputeMs: undefined, reusedExecutionMs: 40 });
		timeline.recordTool(new TimelineInterval(120, 130));
		expect(timeline.measure(130)).toMatchObject({ actorComputeMs: undefined, reusedExecutionMs: 40 });
		expect(toolSpeedup(timeline.measure(130))).toBeNull();
		const adopted = new TaskTimeline(200);
		expect(adopted.recordTool(root, true)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 50, reusedExecutionIncomplete: true });
		expect(adopted.measure(300)).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 50, reusedExecutionIncomplete: true });
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
		expect(timeline.recordTool(producer, true)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 55, reusedByMode: [
			{ source: "pattern", mode: "whole", reusedExecutionMs: 40 }, { source: "drafter", mode: "input", reusedExecutionMs: 10 },
		] });
		const partial = new TimelineInterval(100, 110, [owned, borrowed, unknown, owned].map(computation => ({ computation, reused: true })));
		for (let repeat = 0; repeat < 2; repeat++) expect(timeline.recordTool(partial)).toEqual({ actorComputeMs: 10, reusedExecutionMs: 35, reusedByMode: [
			{ source: "pattern", mode: "whole", reusedExecutionMs: 20 }, { source: "drafter", mode: "input", reusedExecutionMs: 10 },
		] });
	});

	it.each([false, true])("leaves ambiguous owned overlaps unassigned without exceeding gross reuse (reverse=%s)", reverse => {
		const left = new TimelineInterval(10, 50), right = new TimelineInterval(30, 70), unknown = new TimelineInterval(60, 90);
		const children = reverse ? [unknown, right, left] : [left, right, unknown];
		const parent = new TimelineInterval(0, 100, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		TimelineInterval.producedBy(left, { source: "pattern", mode: "left" });
		TimelineInterval.producedBy(right, { source: "pattern", mode: "right" });
		const result = new TaskTimeline(100).recordCall([parent, ...children].map(computation => ({ computation, reused: true })));
		expect(result).toMatchObject({ actorComputeMs: 0, reusedExecutionMs: 100 });
		expect(result.reusedByMode).toEqual([
			{ source: "pattern", mode: "left", reusedExecutionMs: 20 }, { source: "pattern", mode: "right", reusedExecutionMs: 10 },
		]);
		expect(result.reusedByMode!.reduce((sum, value) => sum + value.reusedExecutionMs, 0)).toBeLessThanOrEqual(result.reusedExecutionMs);
	});

	it.each([false, true])("deduplicates retained attribution, preserves write-once provenance and rejects conflicting aliases (live first=%s)", liveFirst => {
		const live = TimelineInterval.retained("same", 40, new TimelineInterval(10, 50)), retained = TimelineInterval.retained("same", 40);
		const producer = { source: "pattern", mode: "owner" };
		TimelineInterval.producedBy(live, producer);
		producer.mode = "mutated";
		TimelineInterval.producedBy(live, { source: "other", mode: "supporter" });
		const roots = (liveFirst ? [live, retained] : [retained, live]).map(computation => ({ computation, reused: true }));
		expect(new TaskTimeline(100).recordCall(roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40,
			reusedByMode: [{ source: "pattern", mode: "owner", reusedExecutionMs: 40 }] });
		TimelineInterval.producedBy(retained, { source: "other", mode: "conflict" });
		expect(new TaskTimeline(100).recordCall(roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40 });
	});

	it("propagates uncertain excluded work through collection without inventing reuse", async () => {
		const preparation = new TimelineInterval(10, 50);
		TimelineInterval.producedBy(preparation, { source: "pattern", mode: "failed" });
		const collected = await TimelineInterval.collect(() => TimelineInterval.exclude(preparation, true));
		expect(new TaskTimeline(0).recordTool(new TimelineInterval(10, 60, collected.dependencies)))
			.toEqual({ actorComputeMs: undefined, reusedExecutionMs: 0 });
	});
});

describe("bounded persisted computation evidence", () => {
	const restore = (computation: TimelineInterval): TimelineInterval => {
		const evidence = TimelineInterval.serialize(computation);
		expect(evidence).toBeDefined();
		const restored = TimelineInterval.restore(JSON.parse(JSON.stringify(evidence)));
		expect(restored).toBeDefined();
		return restored!;
	};
	const reused = (computations: readonly TimelineInterval[]) => new TaskTimeline(0).recordCall(computations.map(computation => ({ computation, reused: true })));

	it("roundtrips original input graphs, explicit exclusions and physical producer attribution", () => {
		const child = new TimelineInterval(110, 150), borrowed = new TimelineInterval(0, 40);
		const parent = new TimelineInterval(100, 170, [
			{ computation: child, owned: true, shared: [child] }, { computation: borrowed, reused: true },
			{ computation: new TimelineInterval(100, 110), overhead: true },
		]);
		TimelineInterval.producedBy(parent, { source: "pattern", mode: "whole" });
		TimelineInterval.producedBy(borrowed, { source: "drafter", mode: "input" });
		const expected = { actorComputeMs: 0, reusedExecutionMs: 100, reusedByMode: [
			{ source: "pattern", mode: "whole", reusedExecutionMs: 60 }, { source: "drafter", mode: "input", reusedExecutionMs: 40 },
		] };
		expect(reused([parent])).toEqual(expected);
		const left = restore(parent), right = restore(parent);
		expect(left).not.toBe(right);
		expect(reused([left, right])).toEqual(expected);
		for (const roots of [[parent, left], [left, parent]]) expect(reused(roots)).toEqual(expected);
		const graph = TimelineInterval.serialize(parent)!;
		expect(Object.isFrozen(graph)).toBe(true);
		expect(Object.isFrozen(graph.nodes[0]!.inputs)).toBe(true);
	});

	it.each([false, true])("deduplicates independently restored parents sharing a borrowed child (reverse=%s)", reverse => {
		const child = new TimelineInterval(0, 40);
		const left = new TimelineInterval(100, 110, [{ computation: child, reused: true }]);
		const right = new TimelineInterval(200, 220, [{ computation: child, reused: true }]);
		const roots = [restore(left), restore(right), restore(child)];
		expect(reused(reverse ? roots.reverse() : roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 70 });
		const timeline = new TaskTimeline(0);
		for (const root of roots) timeline.recordTool(root, true);
		expect(timeline.measure(500)).toMatchObject({ reusedExecutionMs: 150 });
	});

	it.each([false, true])("unions persisted owned siblings without selecting their ancestor or other siblings (reverse=%s)", reverse => {
		const left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80), unconsumed = new TimelineInterval(90, 200);
		const parent = new TimelineInterval(10, 210, [left, right, unconsumed].map(computation => ({ computation, owned: true, shared: [computation] })));
		const roots = [restore(left), restore(right)];
		expect(reused(reverse ? roots.reverse() : roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 60 });
		expect(reused([restore(left)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40 });
		const complete = [restore(parent), restore(left), restore(right)];
		expect(reused(reverse ? complete.reverse() : complete)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 200 });
	});

	it.each([false, true])("preserves early production groups after the enclosing live interval is created (reverse=%s)", reverse => {
		const production = {}, left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
		TimelineInterval.group(left, production); TimelineInterval.group(right, production);
		const savedLeft = restore(left), savedRight = restore(right);
		const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, owned: true, shared: [computation] })));
		expect(reused([savedLeft, savedRight])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 60 });
		for (const enclosing of [parent, restore(parent)]) {
			const roots = [enclosing, savedLeft, savedRight];
			expect(reused(reverse ? roots.reverse() : roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 80 });
		}
		const separate = new TimelineInterval(20, 60);
		TimelineInterval.group(separate, {});
		expect(reused([savedLeft, restore(separate)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 80 });
	});

	it("keeps graph identity stable when a live or restored result receives a certificate alias", () => {
		const original = new TimelineInterval(10, 50), saved = restore(original);
		TimelineInterval.retained("later-certificate-alias", 999, original);
		TimelineInterval.retained("another-certificate-alias", 999, saved);
		expect(reused([original, saved, restore(original)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40 });
		const measuredLegacy = TimelineInterval.retained("independently-measured", 25);
		const retainedLegacy = restore(measuredLegacy);
		TimelineInterval.retained("later-measured-alias", 999, retainedLegacy);
		expect(reused([measuredLegacy, retainedLegacy])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 25 });
	});

	it("canonicalizes nested owned aliases when a session records both a parent and its child", () => {
		const child = new TimelineInterval(20, 60);
		const parent = new TimelineInterval(10, 80, [{ computation: child, owned: true, shared: [child] }]);
		const root = new TimelineInterval(0, 100, [parent, child].map(computation => ({ computation, owned: true, shared: [computation] })));
		expect(reused([root])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 100 });
		const restored = restore(root);
		expect(reused([restored, restore(child), restore(parent)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 100 });
	});

	it("keeps old monotonic coordinates from cutting unrelated work after a process restart", async () => {
		const graph = TimelineInterval.serialize(new TimelineInterval(100, 140))!;
		const foreign = TimelineInterval.restore({ ...graph, nodes: graph.nodes.map(node => ({ ...node, clock: "earlier-process" })) })!;
		const collected = await TimelineInterval.collect(() => TimelineInterval.use(foreign));
		// The numerical coordinates overlap, but the producer and this Actor have different clocks.
		const current = new TimelineInterval(120, 130, collected.dependencies);
		expect(new TaskTimeline(0).recordTool(current)).toEqual({ actorComputeMs: 10, reusedExecutionMs: 40 });
		expect(reused([restore(current), restore(foreign)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 50 });
		// An explicit current-process join still removes its measured wait without clipping it to the old clock.
		const joined = new TimelineInterval(200, 210, [{ computation: foreign, reused: true, shared: [new TimelineInterval(202, 207)] }]);
		expect(new TaskTimeline(0).recordTool(joined)).toEqual({ actorComputeMs: 5, reusedExecutionMs: 40 });
		expect(reused([restore(joined)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 45 });
	});

	it("never unions matching numerical spans from different monotonic clocks", () => {
		const production = {}, live = new TimelineInterval(100, 140);
		TimelineInterval.group(live, production);
		const graph = TimelineInterval.serialize(live)!;
		const foreign = TimelineInterval.restore({ ...graph, root: "foreign-root", nodes: graph.nodes.map(node => ({ ...node,
			id: "foreign-root", clock: "earlier-process" })) })!;
		for (const roots of [[live, foreign], [foreign, live]]) expect(reused(roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 80 });
	});

	it("retains known spans while marking missing reuse evidence and incomplete producer coverage", () => {
		const missing = TimelineInterval.unknownReuse("unrecorded"), known = new TimelineInterval(10, 50);
		const parent = new TimelineInterval(100, 120, [
			{ computation: missing, reused: true }, { computation: known, reused: true },
			{ computation: new TimelineInterval(100, 105), overhead: true, computeUncertain: true },
		]);
		expect(reused([restore(missing)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 0, reusedExecutionIncomplete: true });
		expect(reused([restore(parent)])).toEqual({ actorComputeMs: 0, reusedExecutionMs: 55, reusedExecutionIncomplete: true });
		const timeline = new TaskTimeline(100);
		expect(timeline.recordTool(parent)).toEqual({ actorComputeMs: undefined, reusedExecutionMs: 40, reusedExecutionIncomplete: true });
		timeline.recordTool(new TimelineInterval(200, 210));
		expect(timeline.measure(300)).toMatchObject({ actorComputeMs: undefined, reusedExecutionMs: 40, reusedExecutionIncomplete: true });
		const knownAlias = TimelineInterval.retained("unrecorded", 40, new TimelineInterval(10, 50));
		for (const roots of [[missing, knownAlias], [knownAlias, missing]]) expect(reused(roots)).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40 });
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

	it("omits saturated evidence atomically instead of dropping excluded spans", () => {
		const children = Array.from({ length: TIMELINE_COMPUTATION_LIMITS.nodes }, (_, index) => new TimelineInterval(index, index + 1));
		const tooManyNodes = new TimelineInterval(0, 1000, children.map(computation => ({ computation, owned: true, shared: [computation] })));
		expect(TimelineInterval.serialize(tooManyNodes)).toBeUndefined();
		const child = new TimelineInterval(0, 1);
		const tooManyEdges = new TimelineInterval(0, 1000, Array.from({ length: TIMELINE_COMPUTATION_LIMITS.edges + 1 }, () => ({ computation: child, overhead: true })));
		expect(TimelineInterval.serialize(tooManyEdges)).toBeUndefined();
		const tooManySpans = new TimelineInterval(0, 1000, [{ computation: child, overhead: true,
			shared: Array.from({ length: TIMELINE_COMPUTATION_LIMITS.spans + 1 }, () => child) }]);
		expect(TimelineInterval.serialize(tooManySpans)).toBeUndefined();
		const nodes = Array.from({ length: 200 }, (_, index) => ({ id: `node-${index}`, clock: "synthetic", startedAt: 0, completedAt: 1,
			producer: { source: "s".repeat(256), mode: "m".repeat(256) }, groups: ["g".repeat(256)] }));
		expect(normalizeTimelineComputation({ version: 2, root: nodes[0]!.id,
			nodes: nodes.map((node, index) => ({ ...node, ...(index === 0 ? { inputs: nodes.slice(1).map(input => ({ id: input.id })) } : {}) })) })).toBeUndefined();
	});
});

describe("live producer receipts for measured reuse", () => {
	const measure = (computations: readonly TimelineInterval[]) => {
		let shares: readonly ComputationReuseShare[] = [], calls = 0;
		const timing = new TaskTimeline(0).recordCall(computations.map(computation => ({ computation, reused: true })), receipt => {
			shares = receipt; calls++;
		});
		expect(calls).toBe(1);
		expect(Object.isFrozen(shares)).toBe(true);
		for (const share of shares) expect(Object.isFrozen(share)).toBe(true);
		return { timing, shares };
	};

	it("credits one producer's owned union once per accepted call without requiring a mode", () => {
		const feedback = {}, left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
		const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, owned: true, shared: [computation] })));
		TimelineInterval.producedBy(parent, { source: "drafter", feedback });
		for (let repeat = 0; repeat < 2; repeat++) {
			const result = measure([parent, left, right, parent]);
			expect(result.timing).toEqual({ actorComputeMs: 0, reusedExecutionMs: 80 });
			expect(result.shares).toEqual([{ source: "drafter", feedback, reusedExecutionMs: 80 }]);
			expect(result.shares[0]!.feedback).toBe(feedback);
		}
	});

	it.each([false, true])("keeps live feedback when the same computation also has a persisted alias (live first=%s)", liveFirst => {
		const feedback: { privateMarker: string; self?: unknown } = { privateMarker: "must-not-persist" }, live = new TimelineInterval(10, 50);
		feedback.self = feedback;
		TimelineInterval.producedBy(live, { source: "drafter", feedback });
		const graph = TimelineInterval.serialize(live)!;
		expect(JSON.stringify(graph)).not.toContain("must-not-persist");
		expect(graph.nodes[0]!.producer).toEqual({ source: "drafter" });
		const disk = TimelineInterval.restore(JSON.parse(JSON.stringify(graph)))!;
		expect(measure([disk]).shares).toEqual([]);
		const result = measure(liveFirst ? [live, disk] : [disk, live]);
		expect(result.timing).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40 });
		expect(result.shares).toEqual([{ source: "drafter", feedback, reusedExecutionMs: 40 }]);
	});

	it.each([false, true])("partitions overlapping physical feedback separately from a shared mode label (reverse=%s)", reverse => {
		const first = {}, second = {}, left = new TimelineInterval(10, 50), right = new TimelineInterval(30, 70);
		new TimelineInterval(0, 100, [left, right].map(computation => ({ computation, owned: true, shared: [computation] })));
		TimelineInterval.producedBy(left, { source: "drafter", mode: "workflow", feedback: first });
		TimelineInterval.producedBy(right, { source: "drafter", mode: "workflow", feedback: second });
		const result = measure(reverse ? [right, left] : [left, right]);
		expect(result.timing).toEqual({ actorComputeMs: 0, reusedExecutionMs: 60,
			reusedByMode: [{ source: "drafter", mode: "workflow", reusedExecutionMs: 60 }] });
		expect(result.shares).toEqual([
			{ source: "drafter", feedback: first, reusedExecutionMs: 20 }, { source: "drafter", feedback: second, reusedExecutionMs: 20 },
		]);
	});

	it("leaves overlap with unknown lineage unassigned and separates borrowed producers", () => {
		const feedback = {}, otherFeedback = {}, known = new TimelineInterval(10, 50), unknown = new TimelineInterval(30, 70);
		new TimelineInterval(0, 100, [known, unknown].map(computation => ({ computation, owned: true, shared: [computation] })));
		TimelineInterval.producedBy(known, { source: "drafter", feedback });
		const borrowed = new TimelineInterval(100, 130);
		TimelineInterval.producedBy(borrowed, { source: "pattern", mode: "input", feedback: otherFeedback });
		const result = measure([known, unknown, borrowed]);
		expect(result.timing.reusedExecutionMs).toBe(90);
		expect(result.shares).toEqual([
			{ source: "drafter", feedback, reusedExecutionMs: 20 }, { source: "pattern", feedback: otherFeedback, reusedExecutionMs: 30 },
		]);
	});

	it.each([false, true])("leaves conflicting live aliases unassigned while retaining diagnostic mode credit (reverse=%s)", reverse => {
		const left = TimelineInterval.retained("aliased", 40, new TimelineInterval(10, 50));
		const right = TimelineInterval.retained("aliased", 40, new TimelineInterval(10, 50));
		TimelineInterval.producedBy(left, { source: "drafter", mode: "workflow", feedback: {} });
		TimelineInterval.producedBy(right, { source: "drafter", mode: "workflow", feedback: {} });
		const result = measure(reverse ? [right, left] : [left, right]);
		expect(result.timing).toEqual({ actorComputeMs: 0, reusedExecutionMs: 40,
			reusedByMode: [{ source: "drafter", mode: "workflow", reusedExecutionMs: 40 }] });
		expect(result.shares).toEqual([]);
	});

	it("delivers positive known lower-bound shares but never credits native or rejected work", async () => {
		const feedback = {}, known = new TimelineInterval(10, 50, [
			{ computation: new TimelineInterval(10, 20), overhead: true, computeUncertain: true },
		]);
		TimelineInterval.producedBy(known, { source: "drafter", feedback });
		const measured = measure([known]);
		expect(measured.timing).toEqual({ actorComputeMs: 0, reusedExecutionMs: 30, reusedExecutionIncomplete: true });
		expect(measured.shares).toEqual([{ source: "drafter", feedback, reusedExecutionMs: 30 }]);
		let native: readonly ComputationReuseShare[] = [];
		new TaskTimeline(0).recordCall([{ computation: known }], shares => { native = shares; });
		expect(native).toEqual([]);
		await expect(TimelineInterval.collect(() => { TimelineInterval.use(known); throw new Error("rejected"); })).rejects.toThrow("rejected");
		const collected = await TimelineInterval.collect(() => {});
		let rejected: readonly ComputationReuseShare[] = [];
		new TaskTimeline(0).recordCall(collected.dependencies, shares => { rejected = shares; });
		expect(rejected).toEqual([]);
	});
});
