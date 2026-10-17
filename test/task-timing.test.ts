import { describe, expect, it, vi } from "vitest";
import { TaskTimeline, TimelineInterval, toolSpeedup, normalizeTimelineComputation } from "../src/task-timing.ts";
import { restoreNativeDependency, serializeNativeDependency } from "../src/linux-process-sync.ts";
import { emptySpeculativeTraceSummary, summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { benchmarkTraceReport } from "../bench/trace-report.ts";

it("counts arbitrary projector names as trace data", () => {
	for (const projector of ["constructor", "__proto__", "toString", "rows"]) {
		const event: SpeculativeActionEvent<string> = { type: "actor_action", sessionID: "trace", turnID: "turn", timestamp: 0,
			cache: emptySpeculativeTraceSummary().cache, actualAction: "", settlement: {
				actorAction: { id: "call", sequence: 1, turnID: "turn" }, tool: projector, matchedPredictions: [],
				rejections: [0, 1].map(() => ({ candidateID: "rejected", match: { kind: "exact", distance: 0 }, cause: { stage: "matching", code: projector } })),
				provider: { kind: "speculative", candidateID: "candidate", toolExecution: new TimelineInterval(0, 1),
					match: { kind: "projected", distance: 1, projector } },
			} };
		const summary = summarizeSpeculativeTrace([event, event]);
		expect(summary.partialResultReuseByProjector).toEqual({ [projector]: 2 });
		expect(summary.actorCandidateRejections).toEqual({ [`matching:${projector}`]: 4 });
		expect(summary.actorActionsByTool).toEqual({ [projector]: [2, 2] });
		const prediction: SpeculativeActionEvent<string> = { ...event, type: "prediction", settlement: {
			prediction: { id: "prediction", source: projector, proposalID: "proposal", actionID: "action" },
			observation: "unobserved", cause: { stage: "control", code: "turn_closed" },
		} };
		const report = benchmarkTraceReport([event, event, prediction, prediction], { [projector]: 3 }, false);
		expect(report.speculativeHitsByTool).toEqual({ [projector]: 2 });
		expect(report.predictionsBySource).toEqual({ [projector]: { settled: 2, observed: 0, matched: 0, adopted: 0 } });
		expect(report.actorFallbacksByTool).toEqual({ [projector]: 3 });
	}
});

const restore = (computation: TimelineInterval): TimelineInterval => {
	const evidence = TimelineInterval.serialize(computation);
	expect(evidence).toBeDefined();
	const restored = TimelineInterval.restore(JSON.parse(JSON.stringify(evidence)));
	expect(restored).toBeDefined();
	return restored!;
};

describe("consumed calculation and time hidden before Actor issue", () => {
	it("records execution boundaries and diagnoses the remaining control wait", async () => {
		let now = 0;
		const borrowed = new TimelineInterval(200, 240), producer = { source: "pattern", mode: "early" };
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		try {
			const evaluation = await TimelineInterval.measure(async () => {
				now = 10;
				await TimelineInterval.outside(async () => {
					now = 1000;
					for (const unknown of [TimelineInterval.unknownReuse("old"), TimelineInterval.from({ startedAt: 200, completedAt: 240 })]) expect(TimelineInterval.serialize(unknown)?.nodes[0]?.producer).toBeUndefined();
					for (const input of [borrowed, restore(borrowed), TimelineInterval.attempted(borrowed)]) TimelineInterval.use(input);
					const calculation = await TimelineInterval.measure(() => { now = 1010; });
					expect(new TaskTimeline(0).recordTool(restore(calculation.computation), 100_000, true).hiddenByMode)
						.toEqual([{ ...producer, hiddenComputeMs: 10 }]);
					expect([...TimelineInterval.reusedProducers([{ computation: restore(calculation.computation), reused: true }])]).toEqual(["live"]);
					TimelineInterval.own(calculation.computation);
					now = 1015;
				});
				now = 1025;
			}, undefined, { ...producer, id: "live" });
			const graph = TimelineInterval.serialize(evaluation.computation)!;
			expect(graph.nodes.flatMap(node => node.inputs ?? []).some(input => input.overhead)).toBe(false);
			expect(new TaskTimeline(0).recordTool(evaluation.computation, 100_000)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 40 });
			expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(graph)!, 100_000, true)).toMatchObject({ toolComputeMs: 70, hiddenComputeMs: 70,
				hiddenByMode: [{ ...producer, hiddenComputeMs: 30 }] });
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
		const restored = restore(producer);
		expect(timeline.recordCall([{ computation: restored, reused: true }], 50, 150).adoptionWaitMs).toBe(150 - Math.max(50, end));
		expect(timeline.measure(150).adoptionWaitMs).toBe(150 - Math.max(40, end));
		expect(timeline.recordCall([{ computation: TimelineInterval.unknownReuse("old"), reused: true }], 150, 160).adoptionWaitMs).toBeUndefined();
		expect(timeline.measure(160).adoptionWaitMs).toBeUndefined();
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
});

describe("persisted computation evidence", () => {

	const reused = (computations: readonly TimelineInterval[]) => new TaskTimeline(0).recordCall(computations.map(computation => ({ computation, reused: true })), 100_000);

	it("roundtrips original input graphs, explicit exclusions and physical producer attribution", () => {
		const child = new TimelineInterval(110, 150), borrowed = new TimelineInterval(0, 40);
		const parent = new TimelineInterval(100, 170, [
			{ computation: child, owned: true, shared: [child] }, { computation: borrowed, reused: true },
			{ computation: new TimelineInterval(100, 110), overhead: true },
		]);
		TimelineInterval.producedBy(parent, { source: "pattern", mode: "whole", id: "parent" });
		TimelineInterval.producedBy(borrowed, { source: "drafter", mode: "input", id: "borrowed" });
		for (const [input, used, producer] of [[child, true, "parent"], [borrowed, true, "borrowed"], [child, false, undefined]] as const) {
			const consumer = new TimelineInterval(300, 310, [{ computation: input, reused: used }]);
			for (const computation of [consumer, restore(consumer)]) {
				expect([...TimelineInterval.reusedProducers([{ computation }])]).toEqual(producer ? [producer] : []);
				expect(TimelineInterval.reusedProducers([{ computation, overhead: true }]).size).toBe(0);
				expect(TimelineInterval.reusedProducers([{ computation: TimelineInterval.attempted(computation) }]).size).toBe(0);
			}
		}
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

	it.each([undefined, 0, 1000, -1000])("clips restored computation at Actor issue across clock origins (offset=%s)", offset => {
		const original = new TimelineInterval(100, 200), graph = TimelineInterval.serialize(original)!;
		const foreign = TimelineInterval.restore({ ...graph, nodes: graph.nodes.map(node => ({ ...node,
			clock: "earlier-process", timeOrigin: offset === undefined ? undefined : performance.timeOrigin + offset })) })!;
		const timing = new TaskTimeline(0).recordCall([{ computation: foreign, reused: true }], 150, 250);
		expect(timing).toEqual({ toolComputeMs: 100, hiddenComputeMs: offset === undefined || offset > 0 ? 0 : offset < 0 ? 100 : 50,
			reused: true, ...(offset === undefined ? { hiddenComputeIncomplete: true } : { adoptionWaitMs: offset === 0 ? 50 : 100 }) });
		expect(TimelineInterval.serialize(foreign)?.nodes[0]?.timeOrigin).toBe(offset === undefined ? undefined : performance.timeOrigin + offset);
		const prepared = new TimelineInterval(2000, 2100), dependency = serializeNativeDependency({ computation: prepared, shared: [prepared], reused: true })!;
		const translated = (value: typeof graph) => ({ ...value, nodes: value.nodes.map(node => ({ ...node, clock: "other-worker",
			startedAt: node.startedAt - (offset ?? 0), completedAt: node.completedAt - (offset ?? 0), timeOrigin: offset === undefined ? undefined : performance.timeOrigin + offset })) });
		const restored = restoreNativeDependency({ ...dependency, computation: translated(dependency.computation), shared: dependency.shared!.map(translated) })!;
		expect(new TaskTimeline(0).recordTool(new TimelineInterval(1900, 2200, [restored]), 2200)).toMatchObject({
			toolComputeMs: offset === undefined ? undefined : 300, hiddenComputeMs: offset === undefined ? 0 : 100,
			...(offset === undefined ? { hiddenComputeIncomplete: true } : {}) });
		expect(restoreNativeDependency({ ...dependency, shared: [{ ...graph, root: "missing" }] })).toBeUndefined();
	});

	it("rejects malformed or cyclic graphs without issuing a successful receipt", async () => {
		const graph = TimelineInterval.serialize(new TimelineInterval(10, 50))!;
		const root = graph.nodes[0]!;
		const invalid = [null, {}, { ...graph, version: 999 }, { ...graph, root: "absent" }, ...[
			[{ ...root, spans: [{ startedAt: 0, completedAt: 20 }] }],
			[{ ...root, spans: [{ startedAt: 10, completedAt: 51 }] }],
			[{ ...root, clock: undefined }],
			[root, root], [{ ...root, completedAt: -1 }],
			[{ ...root, startedAt: NaN }],
			[{ ...root, timeOrigin: NaN }],
			[{ ...root, inputs: [{ id: "child" }] }, { ...root, id: "child", timeOrigin: root.timeOrigin! + 1 }],
			[{ ...root, inputs: [{ id: root.id }] }],
			[{ ...root, inputs: [{ id: "absent" }] }],
			[{ ...root, inputs: [{ id: root.id, reused: "yes" }] }],
			...["", 1, "invalid\0id"].map(id => [{ ...root, producer: { source: "drafter", id } }]),
		].map(nodes => ({ ...graph, nodes }))];
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
