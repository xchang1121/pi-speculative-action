import { describe, expect, it } from "vitest";
import { TaskTimeline, TimelineInterval, toolSpeedup } from "../src/task-timing.ts";

describe("accepted computation overlap and Actor tool wait", () => {
	it("unions complete and interrupted tool waits without including model time or double-counting parallel calls", () => {
		const timeline = new TaskTimeline(100);
		timeline.recordActor(100, 140);
		timeline.recordTool(new TimelineInterval(110, 130), true);
		timeline.startToolWait(140)(170);
		timeline.startToolWait(150)(180);
		const interrupted = timeline.startToolWait(190);
		expect(timeline.measure(200)).toMatchObject({ toolWaitMs: 50, hiddenLatencyMs: 20 });
		expect(toolSpeedup(timeline.measure(200))).toBe(1.4);
		interrupted(195); interrupted(250);
		expect(timeline.measure(200).toolWaitMs).toBe(45);
		const next = new TaskTimeline(200);
		expect(toolSpeedup(next.measure(300))).toBeNull();
		for (const toolWaitMs of [0, -1, NaN, Infinity]) expect(toolSpeedup({ toolWaitMs, hiddenLatencyMs: 20 })).toBeNull();
		for (const hiddenLatencyMs of [-1, NaN, Infinity]) expect(toolSpeedup({ toolWaitMs: 50, hiddenLatencyMs })).toBeNull();
	});

	it("excludes previous-task computations even when multiple dependencies share them", () => {
		const child = new TimelineInterval(20, 150), timeline = new TaskTimeline(100);
		timeline.recordTool(new TimelineInterval(100, 170, [{ computation: child, shared: [
			new TimelineInterval(105, 140), new TimelineInterval(130, 150),
		] }]));
		expect(timeline.measure(170)).toMatchObject({ toolExecutionMs: 25, hiddenLatencyMs: 0, authoritativeToolCount: 1 });
		const repeated = new TaskTimeline(100);
		repeated.recordTool(new TimelineInterval(100, 170, [{ computation: child }, { computation: child }]));
		expect(repeated.measure(170)).toMatchObject({ toolExecutionMs: 70, authoritativeToolCount: 1 });
	});

	it.each([
		{ name: "adopted computation overlapping Actor generation", start: 100, end: 200, actor: [[100, 140]], tools: [[110, 130]],
			adopted: true, expected: { toolExecutionMs: 20, hiddenLatencyMs: 20 } },
		{ name: "parallel native batch", start: 0, end: 100, actor: [[0, 40]], tools: [[40, 100], [40, 90], [40, 70]],
			expected: { toolExecutionMs: 140, hiddenLatencyMs: 0 } },
		{ name: "already serial", start: 0, end: 300, actor: [[0, 100], [200, 300]], tools: [[100, 200]],
			expected: { toolExecutionMs: 100, hiddenLatencyMs: 0 } },
		{ name: "tool overlaps Actor generation", start: 0, end: 100, actor: [[0, 100]], tools: [[10, 60]],
			expected: { toolExecutionMs: 50, hiddenLatencyMs: 50 } },
		{ name: "independent overlapping tools", start: 0, end: 200, actor: [[10, 90]], tools: [[40, 120], [70, 150]],
			expected: { toolExecutionMs: 160,
				hiddenLatencyMs: 50, authoritativeToolCount: 2 } },
		{ name: "previous task's cached execution", start: 100, end: 200, actor: [[100, 200]], tools: [[80, 130]],
			expected: { toolExecutionMs: 0, hiddenLatencyMs: 0 } },
		{ name: "clip and union Actor phases", start: 100, end: 200, actor: [[50, 160], [140, 250]], tools: [],
			expected: { hiddenLatencyMs: 0 } },
		{ name: "floating-point residue", start: 0, end: 100_000, actor: [[0, 100_000]], tools: [[0, 2e-11]],
			expected: { hiddenLatencyMs: 0 } },
		{ name: "clip tools and reject empty intervals", start: 100, end: 200, actor: [], tools: [[150, 250], [200, 220], [140, 120]],
			expected: { toolExecutionMs: 50, authoritativeToolCount: 1, hiddenLatencyMs: 0 } },
	])("measures $name", ({ start, end, actor, tools, adopted, expected }) => {
		const timeline = new TaskTimeline(start);
		for (const [from, to] of actor) timeline.recordActor(from!, to!);
		for (const [from, to] of tools) timeline.recordTool(new TimelineInterval(from!, to!), adopted);
		expect(timeline.measure(end)).toMatchObject(expected);
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
		expect(timeline.measure(150)).toMatchObject({ authoritativeToolCount: 2, toolExecutionMs: 60, hiddenLatencyMs: 30 });
		// A retained computation may be adopted later than a more recent one.
		timeline.recordTool(new TimelineInterval(118, 155));
		expect(timeline.measure(200)).toMatchObject({ authoritativeToolCount: 3, toolExecutionMs: 117, hiddenLatencyMs: 42 });
		const nextTask = new TaskTimeline(200);
		nextTask.recordTool(first);
		expect(nextTask.measure(300)).toMatchObject({ authoritativeToolCount: 0, toolExecutionMs: 0, hiddenLatencyMs: 0 });
		for (let repeat = 0; repeat < 3; repeat++) nextTask.recordTool(first, true);
		expect(nextTask.measure(300)).toMatchObject({ hiddenLatencyMs: 0 });
		expect(new TimelineInterval(Number.NaN, -1)).toEqual({ startedAt: 0, completedAt: 0 });
	});

	it("accounts for accepted child computations without counting enclosing work or joining waits twice", () => {
		const child = new TimelineInterval(20, 150);
		const shared = [{ startedAt: 105, completedAt: 150 }];
		const inputs = [{ computation: child, shared }];
		const native = new TimelineInterval(100, 170, inputs);
		shared[0]!.completedAt = 170;
		inputs.length = 0;
		expect(JSON.stringify(native)).toBe('{"startedAt":100,"completedAt":170}');
		const timeline = new TaskTimeline(0);
		timeline.recordActor(0, 100);
		timeline.recordTool(native);
		timeline.recordTool(child);
		expect(timeline.measure(170)).toMatchObject({ toolExecutionMs: 155, hiddenLatencyMs: 85,
			authoritativeToolCount: 2 });
		const serialChild = new TimelineInterval(100, 150), serial = new TaskTimeline(0);
		serial.recordActor(0, 100);
		serial.recordTool(new TimelineInterval(100, 160, [{ computation: serialChild, shared: [serialChild] }]));
		expect(serial.measure(160)).toMatchObject({ toolExecutionMs: 60, hiddenLatencyMs: 0 });

		for (const childFirst of [false, true]) {
			const left = new TimelineInterval(20, 60), right = new TimelineInterval(40, 80);
			const parent = new TimelineInterval(10, 90, [left, right].map(computation => ({ computation, shared: [computation] })));
			const wholeAndPartial = new TaskTimeline(0);
			wholeAndPartial.recordActor(0, 100);
			for (const interval of childFirst ? [left, right, parent] : [parent, left, right]) wholeAndPartial.recordTool(interval);
			// Two distinct overlapping children retain their identities; the parent contributes only its remaining 20 ms.
			expect(wholeAndPartial.measure(100)).toMatchObject({ toolExecutionMs: 100, hiddenLatencyMs: 100, authoritativeToolCount: 3 });
		}
	});
});
