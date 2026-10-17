import { afterEach, expect, test } from "vitest";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { temporaryDirectories } from "./filesystem.ts";
import { PpmCountTrie } from "../src/ppm-count-trie.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createPatternPlanSource } from "../src/pattern-plan-source.ts";
import { testModel } from "./model.ts";
import { applyBindingsVariants, inferBindings, PatternAwareStore, PATTERN_AWARE_DEFAULTS, type PatternAwareBinding, type PatternAwareEvent } from "../src/pattern-aware.ts";

const directories = temporaryDirectories("pi-pattern-system-");
afterEach(directories.dispose);

test("scopes reserved observation revisions to each PatternAware turn", async () => {
	const settings = { enabled: true, resourceCacheMaxEntries: 1, predictionTimeoutMs: 1000, sourceConfig: { patternAware: { presets: [] } } };
	const turn = { sessionID: "revisions", turnID: "same" };
	const startInput = { ...turn, tools: [], actorModel: testModel("actor"), actorOptions: undefined, context: { messages: [] } };
	const controller = createPatternPlanSource({ sessionID: turn.sessionID, cwd: process.cwd(), actionSemantics: PI_ACTION_SEMANTICS,
		projectionRules: [], store: new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, presets: [] }) });
	try {
		for (let round = 0; round < 2; round++) {
			controller.turnStarted(startInput, settings);
			for (const [extra, revision] of [[0, 0], [5, 6], [0, 7]]) {
				const update = await controller.source.observe!({ startInput, settings, data: { tools: new Map(), schemaHashes: {} },
					consumeInput: { ...turn, id: "call", tool: "inspect", args: {}, tools: [] }, tool: "inspect", concrete: {}, durationMs: 1, order: 0,
					reserveRevision: (_id, minimum) => minimum + extra! });
				expect(update).toMatchObject({ revision });
			}
			controller.turnFinished(startInput, settings, false);
			await controller.source.flush?.();
		}
	} finally { await controller.dispose(); }
});

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
	expect(applyBindingsVariants({ '["path"]': { ...binding, variantCounts: { 1: 8 } },
		'["offset"]': { ...eachLocation("line"), variantCounts: { 1: 8 } } }, [locations], 1)).toEqual([{ path: "src/b.ts", offset: 17 }]);
	expect(applyBindingsVariants({ '["path"]': { ...binding, variantCounts: { 1: 8 } },
		'["offset"]': { type: "near", source: eachLocation("line"), tolerance: 150, variantCounts: { 0: 8 } } }, [locations], 1)).toEqual([{ path: "src/b.ts", offset: 17 }]);
});

test.each([
	["path", "test/a.ts", "test/b.ts"], ["path", "src/a.test.ts", "src/b.test.ts"], ["path", "a", "b"],
	["command", "inspect src/a.ts", "inspect src/b.ts"],
])("infers correlated location rewrites for %s = %s", (field, first, second) => {
	for (const offset of [3, 4]) {
		const bindings = inferBindings([locations], { [field]: first, offset });
		const expected = [{ [field]: first, offset: 3 }, { [field]: second, offset: 17 }];
		expect(applyBindingsVariants(bindings, [locations])).toEqual(expected);
		expect(applyBindingsVariants(bindings, [{ ...locations, outputLocations: [...locations.outputLocations!].reverse() }])).toEqual([...expected].reverse());
	}
});

test.each([false, true])("learns complete argument relations with a single prediction (cross-row %s)", crossRow => {
	const store = new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, presets: [], maxContextLength: 1, maxFutureGap: 0, beamWidth: 1 });
	for (let index = 0; index < 4; index++) {
		const context = { ...locations, sessionID: String(index), outputLocations: [
			{ path: `src/a${index}.ts`, line: 3 + 100 * index }, { path: `src/b${index}.ts`, line: 17 + 100 * index },
		] };
		store.observe(context);
		store.observe({ ...context, tool: "custom-read", input: { path: `test/a${index}.ts`, offset: (crossRow ? 17 : 3) + 100 * index }, outputLocations: undefined });
		store.finishSession(context.sessionID);
	}
	const predicted = store.predictAfterBatch("unseen", [{ ...locations, sessionID: "unseen",
		outputLocations: [{ path: "src/c.ts", line: 5 }, { path: "src/d.ts", line: 21 }] }]);
	expect(predicted.map(({ input }) => input)).toEqual([{ path: "test/c.ts", offset: crossRow ? 21 : 5 }]);
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

test("restores replacement counts without losing probability or retention priority", () => {
	const model = new PpmCountTrie(1), row = (context: string[], count: number) => ({ context, counts: { read: count }, lastSeen: 0 });
	model.restore([row([], Number.MAX_SAFE_INTEGER), row([], 0.25), row(["strong"], Number.MAX_SAFE_INTEGER), row(["strong"], 0.25), row(["weak"], 0.125)]);
	expect(model.distribution([]).get("read")).toBeCloseTo(0.2);
	model.trim(2);
	expect(model.snapshot().map(row => row.context)).toEqual([[], ["strong"]]);
});
