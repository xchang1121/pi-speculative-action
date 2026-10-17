import { afterEach, expect, test } from "vitest";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { temporaryDirectories } from "./filesystem.ts";
import { PpmCountTrie } from "../src/ppm-count-trie.ts";
import { applyBindingsVariants, PatternAwareStore, PATTERN_AWARE_DEFAULTS, type PatternAwareBinding, type PatternAwareEvent } from "../src/pattern-aware.ts";

const directories = temporaryDirectories("pi-pattern-system-");
afterEach(directories.dispose);

test.each(["from", "to"])("rejects workspace paths hidden in a portable rebase's %s", async field => {
	const file = path.join(await directories.create(), "portable.json");
	const pattern = { id: "shared", context: [{ tool: "inspect", outcome: "success" }], targetTool: "custom-read", occurrences: 2,
		historicalOpportunities: 2, historicalMatches: 2, lastSeenSequence: 2, gapCounts: { 0: 2 }, gapLastSeen: { 0: 2 },
		feedback: { ...Object.fromEntries(["issued", "observed", "matched", "adopted", "recentMatchedWeight", "recentMismatchedWeight",
			"recentAdoptedWeight", "recentRejectedWeight", "sequence"].map(key => [key, 0])), rejectedAfterMatch: {}, unobserved: {} } };
	for (const local of [false, true]) {
		const binding = { type: "rebase", source: { type: "event", relativeEvent: -1, field: "input", path: ["path"] },
			from: "src", to: "test", ...(local ? { [field]: "/private/workspace" } : {}) };
		await writeFile(file, JSON.stringify({ patterns: [{ ...pattern, bindings: { '["path"]': binding } }] }));
		const store = new PatternAwareStore(PATTERN_AWARE_DEFAULTS, undefined, undefined, file);
		await store.load();
		expect(store.snapshot()).toHaveLength(local ? 0 : 1);
	}
});

const locations: PatternAwareEvent = { sessionID: "bindings", turnID: "one", tool: "custom-inspect", input: {},
	outcome: "success", durationMs: 1, sequence: 1, outputLocations: [{ path: "src/a.ts", line: 3 }, { path: "src/b.ts", line: 17 }] };
const eachLocation = (field: "path" | "line"): PatternAwareBinding => ({ type: "each", relativeEvent: -1, field: "outputLocations", path: [], itemPath: [field] });
const source = eachLocation("path");

test.each([
	{ type: "transform", operation: "normalize_path", source },
	{ type: "template", source, prefix: "", suffix: "" },
	{ type: "rebase", source, from: "src", to: "src" },
	{ type: "join", operation: "join_path", left: { type: "constant", value: "." }, right: source },
	{ type: "splice", sources: [source], parts: ["", ""] },
	{ type: "coalesce", sources: [source, { type: "constant", value: "fallback" }] },
] satisfies PatternAwareBinding[])("keeps each location's path and line together through $type", binding => {
	expect(applyBindingsVariants({ '["path"]': binding, '["offset"]': { type: "near", source: eachLocation("line"), tolerance: 150 } }, [locations]))
		.toEqual([{ path: "src/a.ts", offset: 3 }, { path: "src/b.ts", offset: 17 }]);
});

test("correlates repeated collection references within a composite and keeps independent collections independent", () => {
	const basename: PatternAwareBinding = { type: "transform", operation: "basename", source };
	const command: PatternAwareBinding = { type: "splice", sources: [source, basename], parts: ["check ", " --name ", ""] };
	expect(applyBindingsVariants({ '["command"]': command }, [locations]))
		.toEqual([{ command: "check src/a.ts --name a.ts" }, { command: "check src/b.ts --name b.ts" }]);
	const independent: PatternAwareBinding = { type: "each", relativeEvent: -1, field: "output", path: ["modes"], itemPath: [] };
	expect(applyBindingsVariants({ '["path"]': source, '["mode"]': independent }, [{ ...locations, output: { modes: ["fast", "full"] } }]))
		.toEqual([{ path: "src/a.ts", mode: "fast" }, { path: "src/a.ts", mode: "full" }, { path: "src/b.ts", mode: "fast" }, { path: "src/b.ts", mode: "full" }]);
});

test.each(["trim", "restore"])("preserves aged evidence while %s discards unrelated contexts", operation => {
	const model = new PpmCountTrie(4);
	model.observe([], "old", 0);
	model.observe([], "new", 8);
	model.setCount(["unrelated"], "other", 1, 8);
	const expected = model.distribution([], 8, 8);
	expect(expected.get("new")).toBeGreaterThan(expected.get("old")!);
	if (operation === "trim") model.trim(1);
	else model.restore(JSON.parse(JSON.stringify(model.snapshot(1))));
	expect(model.size).toBe(1);
	expect(model.distribution([], 8, 8)).toEqual(expected);
});

test.each([1, 8, 64])("keeps bounded learned suffixes through persistence and further learning (capacity %s)", capacity => {
	const model = new PpmCountTrie(4), restored = new PpmCountTrie(4), history: string[] = [];
	for (let sequence = 1; sequence <= 200; sequence++) {
		const target = sequence % 3 ? "inspect" : "render";
		for (const store of [model, restored]) { store.observe(history, target, sequence, 32); store.trim(capacity); }
		expect(model.size).toBeLessThanOrEqual(capacity);
		expect(restored.distribution(history, sequence, 32)).toEqual(model.distribution(history, sequence, 32));
		if (sequence % 17 === 0) restored.restore(JSON.parse(JSON.stringify(model.snapshot())));
		history.push(`context:${sequence % 7}`);
		if (history.length > 4) history.shift();
	}
	const probabilities = [...model.distribution(history, 200, 32).values()];
	expect(probabilities.every(value => Number.isFinite(value) && value > 0 && value < 1)).toBe(true);
	expect(probabilities.reduce((sum, value) => sum + value, 0)).toBeLessThanOrEqual(1);
});
