import { describe, expect, it } from "vitest";
import { analyzeBashCorpus, bashCallsFromSWEbenchSummary } from "../bench/bash-corpus-analysis.ts";

describe("SWE-bench Bash corpus analysis", () => {
	it("reports only production-proved tail views and labels bucket coverage as incremental", () => {
		const calls = bashCallsFromSWEbenchSummary([
			{
				instance_id: "task-a",
				tool_call_details: [
					bash("pytest -q 2>&1 | tail -20", 100, "test_execution"),
					bash("pytest -q 2>&1 | tail -40", 200, "test_execution"),
					bash("pytest -q 2>&1 | tail -10", 300, "test_execution"),
					bash("pytest -q 2>&1 | tail -10", 400, "test_execution"),
					bash("pytest -q 2>&1 | grep fail | tail -20", 500, "test_execution"),
					{ tool: "read", args: { path: "notes.txt" }, duration_ms: 1 },
				],
			},
		]);

		expect(calls).toHaveLength(5);
		const result = analyzeBashCorpus(calls, { coveringBuckets: [80] });
		expect(result).toMatchObject({
			tasks: 1,
			calls: 5,
			durationMs: 1500,
			categories: { test_execution: { calls: 5, durationMs: 1500 } },
			exactRepeatUpperBound: {
				opportunities: 1,
				actorMs: 400,
				afterExplicitMutation: 0,
				afterExplicitMutationActorMs: 0,
				sameGeneration: 1,
				sameGenerationActorMs: 400,
				immediatelyAfterExplicitMutation: 0,
				immediatelyAfterExplicitMutationActorMs: 0,
			},
			tailViews: {
				syntacticTerminalTailCalls: 5,
				provableCalls: 4,
				provableDurationMs: 1000,
				rejectedLookalikeCalls: 1,
				widths: {
					"10": { calls: 2, durationMs: 700 },
					"20": { calls: 1, durationMs: 100 },
					"40": { calls: 1, durationMs: 200 },
				},
				variantCoreGroups: 1,
				variantCalls: 4,
				coveringBuckets: [80],
				coverage: {
					coreRepeatOpportunities: 3,
					coreRepeatActorMs: 900,
					directlyCovered: 2,
					directlyCoveredActorMs: 700,
					bucketCovered: 3,
					bucketCoveredActorMs: 900,
					incrementalBucketCovered: 1,
					incrementalBucketCoveredActorMs: 200,
				},
			},
		});
	});

	it("fails closed on malformed summaries and metrics", () => {
		expect(bashCallsFromSWEbenchSummary({})).toEqual([]);
		expect(
			bashCallsFromSWEbenchSummary([
				{ instance_id: "task", tool_call_details: [bash("echo ok", -1), bash("echo ok", 1)] },
				{ tool_call_details: [bash("echo ignored", 1)] },
			]),
		).toEqual([
			{
				task: "task",
				sequence: 1,
				command: "echo ok",
				durationMs: 1,
				workspaceGeneration: 0,
				previousTool: "bash",
			},
		]);
	});

	it("separates exact reruns after an explicit workspace mutation", () => {
		const calls = bashCallsFromSWEbenchSummary([
			{
				instance_id: "task",
				tool_call_details: [
					bash("pytest -q", 100),
					{ tool: "edit", args: {}, duration_ms: 1, is_error: false },
					bash("pytest -q", 250),
				],
			},
		]);

		expect(calls[1]).toMatchObject({
			sequence: 2,
			workspaceGeneration: 1,
			previousTool: "edit",
		});
		expect(analyzeBashCorpus(calls).exactRepeatUpperBound).toEqual({
			opportunities: 1,
			actorMs: 250,
			afterExplicitMutation: 1,
			afterExplicitMutationActorMs: 250,
			sameGeneration: 0,
			sameGenerationActorMs: 0,
			immediatelyAfterExplicitMutation: 1,
			immediatelyAfterExplicitMutationActorMs: 250,
		});
	});
});

function bash(command: string, duration_ms: number, bash_category?: string) {
	return {
		tool: "bash",
		args: { command },
		duration_ms,
		...(bash_category ? { bash_category } : {}),
	};
}
