import { describe, expect, it } from "vitest";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";

describe("actor fork plan source", () => {
	it("owns turn delivery and cancels its probe with the Runtime request", async () => {
		const source = createActorForkPlanSource({ retryStreamUpdates: 1 });
		const input = { path: "a.txt" };
		source.startTurn("turn-1");
		const delta = { type: "thinking_delta" as const, contentIndex: 0, delta: "think", partial: undefined as never };
		expect(source.observeActorDelta("turn-1", delta)).toBeUndefined();
		source.bindActorRequest("turn-1");
		expect(source.observeActorDelta("turn-1", delta)).toEqual({
			attempt: 1,
			generatedText: "think",
			content: "",
			reasoning: "think",
			outputChunks: 1,
		});
		expect(source.observeActorDelta("turn-1", delta)).toBeUndefined();
		expect(source.finishProbe("turn-1")).toBe(false);
		expect(source.claimPendingProbe("turn-1")).toEqual({
			attempt: 2,
			generatedText: "thinkthink",
			content: "",
			reasoning: "thinkthink",
			outputChunks: 2,
		});
		const delivered = source.waitForBatches("turn-1", new AbortController().signal);
		source.publish("turn-1", [
			{
				id: "batch",
				calls: [{ id: "call", index: 0, tool: "read", input }],
				evidence: [],
			},
		]);
		input.path = "changed.txt";
		expect((await delivered)[0]?.calls[0]?.input).toEqual({ path: "a.txt" });

		source.startTurn("turn-2");
		const runtime = new AbortController();
		const cancelled = source.waitForBatches("turn-2", runtime.signal);
		runtime.abort();
		expect(await cancelled).toEqual([]);
		expect(source.probeSignal("turn-2")?.aborted).toBe(true);
	});

	it("bounds retries and only probes a newer Actor snapshot", () => {
		const source = createActorForkPlanSource({ maxAttempts: 2, retryStreamUpdates: 2 });
		const delta = { type: "text_delta" as const, contentIndex: 0, delta: "x", partial: undefined as never };
		source.startTurn("turn-d2");
		source.bindActorRequest("turn-d2");
		expect(source.observeActorDelta("turn-d2", delta)?.attempt).toBe(1);
		expect(source.finishProbe("turn-d2")).toBe(false);
		expect(source.claimPendingProbe("turn-d2")).toBeUndefined();
		expect(source.observeActorDelta("turn-d2", delta)).toBeUndefined();
		expect(source.observeActorDelta("turn-d2", delta)?.attempt).toBe(2);
		expect(source.finishProbe("turn-d2")).toBe(true);
		expect(source.observeActorDelta("turn-d2", delta)).toBeUndefined();
	});

	it("releases waiters and cancels the probe when the Actor stream finishes", async () => {
		const source = createActorForkPlanSource();
		source.startTurn("turn-finished");
		source.bindActorRequest("turn-finished");
		const batches = source.waitForBatches("turn-finished", new AbortController().signal);
		source.finishActorStream("turn-finished");
		await expect(batches).resolves.toEqual([]);
		expect(source.probeSignal("turn-finished")?.aborted).toBe(true);
	});

	it("preserves complete call batches and their evidence", async () => {
		const source = createActorForkPlanSource();
		source.startTurn("turn-batch");
		const delivered = source.waitForBatches("turn-batch", new AbortController().signal);
		source.publish("turn-batch", [
			{
				id: "fork:v1:batch",
				calls: [
					{ id: "0:fork", index: 0, callID: "call-a", format: "structured", tool: "read", input: { path: "a.txt" } },
					{ id: "1:fork", index: 1, tool: "read", input: { path: "b.txt" } },
				],
				evidence: [
					{
						candidateIDs: ["candidate-a"],
						sources: ["self-speculation"],
						provenance: [{ proposalID: "proposal", actionID: "action" }],
						actionIdentities: [],
						draftTokenCount: 12,
						confidence: 0.95,
						score: { joint_speculation_probability: 0.7 },
						fork: { total_ms: 25 },
					},
				],
			},
		]);

		expect(await delivered).toMatchObject([
			{
				calls: [{ callID: "call-a", input: { path: "a.txt" } }, { input: { path: "b.txt" } }],
				evidence: [{ confidence: 0.95, fork: { total_ms: 25 } }],
			},
		]);
	});
});
