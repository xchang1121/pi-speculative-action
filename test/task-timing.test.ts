import { describe, expect, it } from "vitest";
import { TaskTimeline, TimelineInterval, toolSpeedup, normalizeTimelineComputation } from "../src/task-timing.ts";
import { restoreNativeDependency, serializeNativeDependency } from "../src/linux-process-sync.ts";

const restore = (computation: TimelineInterval): TimelineInterval => {
	const evidence = TimelineInterval.serialize(computation);
	expect(evidence).toBeDefined();
	const restored = TimelineInterval.restore(JSON.parse(JSON.stringify(evidence)));
	expect(restored).toBeDefined();
	return restored!;
};

describe("consumed calculation and time hidden before Actor issue", () => {
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
			[{ ...root, groups: new Array(1) }],
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

});
