import { describe, expect, it } from "vitest";
import { SelfSpeculationActionBridge } from "../src/self-speculation-action-bridge.ts";

describe("self-speculation action bridge", () => {
	it("hands off one cloned batch and contains aborts, closes, and late publishers", async () => {
		const bridge = new SelfSpeculationActionBridge();
		const input = { path: "a.txt" };
		bridge.startTurn("turn-1");
		const delivered = bridge.waitForCandidates("turn-1", new AbortController().signal);
		bridge.publish("turn-1", [{ tool: "read", input }]);
		input.path = "changed.txt";
		expect(await delivered).toEqual([{ tool: "read", input: { path: "a.txt" } }]);

		bridge.startTurn("turn-2");
		const aborted = new AbortController();
		const cancelled = bridge.waitForCandidates("turn-2", aborted.signal);
		aborted.abort();
		expect(await cancelled).toEqual([]);

		const closed = bridge.waitForCandidates("turn-2", new AbortController().signal);
		bridge.closeTurn("turn-2");
		bridge.publish("turn-2", [{ tool: "read", input: { path: "late.txt" } }]);
		expect(await closed).toEqual([]);
		expect(await bridge.waitForCandidates("turn-2", new AbortController().signal)).toEqual([]);
	});

	it("preserves complete call batches and isolates their evidence", async () => {
		const bridge = new SelfSpeculationActionBridge();
		const input = { path: "a.txt" };
		const provenance = { proposalID: "proposal", actionID: "action" };
		bridge.startTurn("turn-batch");
		const delivered = bridge.waitForBatches("turn-batch", new AbortController().signal);
		bridge.publishBatches("turn-batch", [
			{
				id: "fork:v1:batch",
				calls: [
					{ id: "0:fork", index: 0, callID: "call-a", format: "structured", tool: "read", input },
					{ id: "1:fork", index: 1, tool: "read", input: { path: "b.txt" } },
				],
				evidence: [
					{
						candidateIDs: ["candidate-a"],
						sources: ["self-speculation"],
						provenance: [provenance],
						actionIdentities: [],
						draftTokenCount: 12,
						confidence: 0.95,
						score: { joint_speculation_probability: 0.7 },
						fork: { total_ms: 25 },
					},
				],
			},
		]);
		input.path = "changed.txt";
		provenance.actionID = "changed";

		expect(await delivered).toEqual([
			{
				id: "fork:v1:batch",
				calls: [
					{
						id: "0:fork",
						index: 0,
						callID: "call-a",
						format: "structured",
						tool: "read",
						input: { path: "a.txt" },
					},
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
	});
});
