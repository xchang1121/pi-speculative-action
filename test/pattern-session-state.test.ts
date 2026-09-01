import { describe, expect, it } from "vitest";
import {
	patternSessionBudgets,
	PatternSessionRegistry,
} from "../src/pattern-session-state.ts";

describe("PatternSessionRegistry", () => {
	it("bounds abandoned sessions and per-session derived state by recency", () => {
		const registry = new PatternSessionRegistry(patternSessionBudgets(2));
		const first = registry.ensure("first").state;
		registry.ensure("second");
		registry.get("first");

		const third = registry.ensure("third");
		expect(third.evicted?.id).toBe("second");
		expect(registry.get("second")).toBeUndefined();

		first.rememberRecurrentAction("one", recurrent("one", 1));
		first.rememberRecurrentAction("two", recurrent("two", 2));
		first.recurrentAction("one");
		first.rememberRecurrentAction("three", recurrent("three", 3));
		expect(first.recurrentAction("two")).toBeUndefined();
		expect([...first.recurrentActions].map((item) => item.action.key)).toEqual(["one", "three"]);

		const dropped = first.replacePending([
			pending("oldest", 1),
			pending("middle", 2),
			pending("newest", 3),
		]);
		expect(dropped.map((item) => item.patternID)).toEqual(["oldest"]);
		expect(first.pending.map((item) => item.patternID)).toEqual(["middle", "newest"]);
	});
});

function recurrent(key: string, sequence: number) {
	return {
		action: {
			key,
			hash: key,
			tool: "read",
			input: {},
			resources: [],
			semanticsEpoch: "test",
			schemaHash: "test",
			executionFingerprint: "test",
		},
		input: {},
		count: 1,
		totalDurationMs: 1,
		lastSeenSequence: sequence,
	};
}

function pending(patternID: string, triggerSequence: number) {
	return { patternID, triggerSequence, expectedInputs: [], remaining: 1 };
}
