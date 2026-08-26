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
});
