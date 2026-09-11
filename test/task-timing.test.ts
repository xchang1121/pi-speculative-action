import { describe, expect, it } from "vitest";
import { measureSpeculativeTask, TaskTimeline, TimelineInterval } from "../src/task-timing.ts";

describe("single-run serialized counterfactual timing", () => {
	it.each([
		{ name: "already serial", start: 0, end: 300, actor: [[0, 100], [200, 300]], tools: [[100, 200]],
			expected: { endToEndMs: 300, nonToolMs: 200, toolExecutionMs: 100, serializedMs: 300, hiddenLatencyMs: 0 } },
		{ name: "tool overlaps Actor generation", start: 0, end: 100, actor: [[0, 100]], tools: [[10, 60]],
			expected: { endToEndMs: 100, nonToolMs: 100, toolExecutionMs: 50, serializedMs: 150, hiddenLatencyMs: 50 } },
		{ name: "independent overlapping tools", start: 0, end: 200, actor: [[10, 90]], tools: [[40, 120], [70, 150]],
			expected: { endToEndMs: 200, actorPhaseMs: 80, orchestrationMs: 60, nonToolMs: 140, toolExecutionMs: 160,
				serializedMs: 300, hiddenLatencyMs: 100, authoritativeToolCount: 2 } },
		{ name: "previous task's cached execution", start: 100, end: 200, actor: [[100, 200]], tools: [[80, 130]],
			expected: { endToEndMs: 100, toolExecutionMs: 0, serializedMs: 100, hiddenLatencyMs: 0 } },
		{ name: "clip and union Actor phases", start: 100, end: 200, actor: [[50, 160], [140, 250]], tools: [],
			expected: { endToEndMs: 100, actorPhaseMs: 100, nonToolMs: 100, hiddenLatencyMs: 0 } },
		{ name: "floating-point residue", start: 0, end: 100_000, actor: [[0, 100_000]], tools: [[0, 2e-11]],
			expected: { endToEndMs: 100_000, serializedMs: 100_000, hiddenLatencyMs: 0 } },
		{ name: "clip tools and reject empty intervals", start: 100, end: 200, actor: [], tools: [[150, 250], [200, 220], [140, 120]],
			expected: { toolExecutionMs: 50, authoritativeToolCount: 1, serializedMs: 100, hiddenLatencyMs: 0 } },
	])("measures $name", ({ start, end, actor, tools, expected }) => {
		const intervals = (pairs: number[][]) => pairs.map(([startedAt, completedAt]) => ({ startedAt: startedAt!, completedAt: completedAt! }));
		expect(measureSpeculativeTask({ startedAt: start, completedAt: end, actorPhases: intervals(actor), authoritativeTools: intervals(tools) }))
			.toMatchObject(expected);
	});

	it("counts the computation once across adoption and snapshots endpoints independently of mutable input", () => {
		const input = { startedAt: 120, completedAt: 160 }, first = TimelineInterval.from(input);
		const second = new TimelineInterval(120, 160), timeline = new TaskTimeline(100);
		input.completedAt = 1000;
		expect(first.completedAt).toBe(160);
		expect(TimelineInterval.from(first)).toBe(first);
		expect(Reflect.set(first, "completedAt", 1000)).toBe(false);
		timeline.recordActor(50, 190);
		for (const interval of [first, first, second, new TimelineInterval(80, 130)]) timeline.recordTool(interval);
		expect(timeline.measure(150)).toMatchObject({ authoritativeToolCount: 2, toolExecutionMs: 60, hiddenLatencyMs: 60 });
		// A retained computation may be adopted later than a more recent one.
		timeline.recordTool(new TimelineInterval(118, 155));
		expect(timeline.measure(200)).toMatchObject({ authoritativeToolCount: 3, toolExecutionMs: 117, hiddenLatencyMs: 117 });
		const nextTask = new TaskTimeline(200);
		nextTask.recordTool(first);
		expect(nextTask.measure(300)).toMatchObject({ authoritativeToolCount: 0, toolExecutionMs: 0, hiddenLatencyMs: 0 });
		expect(new TimelineInterval(Number.NaN, -1)).toEqual({ startedAt: 0, completedAt: 0 });
	});
});
