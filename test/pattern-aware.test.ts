import { deferred, nextTurn } from "./async.ts";
import fs from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import path from "node:path";
import { afterEach, describe, expect, test, vi } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { BoundedRecencyMap } from "../src/bounded-recency-map.ts";
import {
	acquirePatternAwareStore,
	patternAwareActionSemantics,
	applyBindings,
	applyBindingsVariants,
	inferBindings,
	PATTERN_AWARE_DEFAULTS,
	PatternAwareStore,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "../src/pattern-aware.ts";
import { adoptedSettlement, rejectedSettlement, unmatchedSettlement, unobservedSettlement } from "./prediction.ts";

const directories = temporaryDirectories("pi-pattern-");

afterEach(directories.dispose);

describe("PatternAware", () => {
	test("late-binds a target input from authoritative structured output paths", () => {
		const paths = ["src/a.ts"], target = { filePath: "src/a.ts", offset: 1 };
		const context = [event("one", "grep", { pattern: "TODO" }, { outputPaths: paths })];
		const bindings = inferBindings(context, target);
		expect(bindings['["filePath"]']).toEqual({
			type: "event",
			relativeEvent: -1,
			field: "outputPaths",
			path: [0],
		});
		expect(bindings['["offset"]']).toEqual({ type: "constant", value: 1 });
		for (const [filePath, offset] of [["src/a.ts", 1], ["src/b.ts", 2]] as const) {
			paths[0] = filePath;
			Object.assign(target, { filePath, offset });
			Object.assign(bindings['["offset"]']!, { value: offset });
			expect(applyBindings(bindings, context)).toEqual(target);
			expect(applyBindings(inferBindings(context, target), context)).toEqual(target);
		}
		Object.assign(bindings['["filePath"]']!, { relativeEvent: -2 });
		expect(applyBindings(inferBindings(context, target), context)).toEqual(target);
	});

	test("keeps learned path joins idempotent when search outputs are already anchored", () => {
		const bindings = {
			'["path"]': {
				type: "join" as const,
				operation: "join_path" as const,
				left: { type: "event" as const, relativeEvent: -1, field: "input" as const, path: ["path"] },
				right: { type: "event" as const, relativeEvent: -1, field: "outputPaths" as const, path: [0] },
			},
		};
		const replay = (root: string, output: string) =>
			applyBindings(bindings, [
				event("join", "grep", { path: root }, { outputPaths: [output] }),
			]);

		expect(replay("src/file.ts", "file.ts")).toEqual({ path: "src/file.ts" });
		expect(replay("src", "src/file.ts")).toEqual({ path: "src/file.ts" });
		expect(replay("src", "nested/file.ts")).toEqual({ path: "src/nested/file.ts" });
	});

	test("merges nested binding paths and rejects incomplete or unsafe mappings", () => {
		const context = [event("one", "seed", { oldText: "before" })];
		const target = {
			range: { start: 1, end: 2 },
			edits: [{ oldText: "before", newText: "after" }],
		};

		expect(applyBindings(inferBindings(context, target), context)).toEqual(target);
		target.edits[0]!.newText = "changed";
		expect(applyBindings(inferBindings(context, target), context)).toEqual(target);
		for (const items of [[], Array(1), [undefined]]) {
			const bindings = inferBindings([event("slots", "seed", { items }, { output: {}, outputPaths: [] })], { value: undefined });
			expect(bindings['["value"]']).toEqual(0 in items
				? { type: "event", relativeEvent: -1, field: "input", path: ["items", 0] }
				: { type: "constant", value: undefined });
		}
		const valid = inferBindings(context, target);
		const invalid: Parameters<typeof applyBindings>[0][] = [
			{ '["missing"]': { type: "event", relativeEvent: -2, field: "input", path: ["value"] } },
			...['[]', '["__proto__"]', '["nested","prototype"]', '["constructor","value"]']
				.map(path => ({ [path]: { type: "constant" as const, value: "unsafe" } })),
		];
		for (const missing of invalid) for (const bindings of [{ ...valid, ...missing }, { ...missing, ...valid }]) {
			expect(applyBindingsVariants(bindings, context)).toEqual([]);
		}
	});

	test("derives adjacent paths and commands through bounded path templates", () => {
		const context = [event("one", "read", { filePath: "services/alpha/config.ts" })];
		const bindings = inferBindings(context, {
			command: "bun test services/alpha/config.test.ts",
			workdir: "services/alpha",
		});

		expect(applyBindings(bindings, context)).toEqual({
			command: "bun test services/alpha/config.test.ts",
			workdir: "services/alpha",
		});
		const next = [event("two", "read", { filePath: "services/beta/config.ts" })];
		for (const name of ["beta", "beta", "gamma"]) {
			next[0]!.input.filePath = `services/${name}/config.ts`;
			expect(applyBindings(bindings, next)).toEqual({
				command: `bun test services/${name}/config.test.ts`, workdir: `services/${name}`,
			});
		}
		expect(
			inferBindings([event("path", "read", { filePath: "/workspace/repo" })], {
				filePath: "repo/src/a.ts",
			})['["filePath"]'],
		).toEqual({ type: "constant", value: "repo/src/a.ts" });
	});

	test("does not treat an opaque shell command as a filesystem path template", () => {
		const context = [
			event("one", "bash", { command: '& "C:\\Users\\dev\\.bun\\bin\\bun.exe" test services/delta/config.test.ts' }),
		];
		const target = { command: '& "C:\\Users\\dev\\.bun\\bin\\bun.exe" test services/epsilon/config.test.ts' };
		const bindings = inferBindings(context, target);

		expect(bindings['["command"]']).toEqual({ type: "constant", value: target.command });
		expect(
			applyBindings(bindings, [
				event("two", "bash", { command: "bun test services/gamma/config.test.ts" }),
			]),
		).toEqual(target);
	});

	test("interpolates one non-path value without inventing case, multi-source, short, or path semantics", () => {
		const context = [
			event("one", "inspect", { left: "Alpha", right: "Beta", short: "xy" }),
		];
		const target = {
			normalized: "alpha",
			command: "run Alpha now",
			joined: "Alpha:Beta",
			query: "pre-xy-post",
			filePath: "out/Alpha.txt",
		};
		const bindings = inferBindings(context, target);

		expect(bindings).toMatchObject({
			'["normalized"]': { type: "constant", value: "alpha" },
			'["command"]': { type: "template", prefix: "run ", suffix: " now" },
			'["joined"]': { type: "template", prefix: "", suffix: ":Beta" },
			'["query"]': { type: "constant", value: "pre-xy-post" },
			'["filePath"]': { type: "constant", value: "out/Alpha.txt" },
		});
		expect(
			applyBindings(bindings, [
				event("two", "inspect", { left: "Gamma", right: "Delta", short: "zz" }),
			]),
		).toEqual({ ...target, command: "run Gamma now", joined: "Gamma:Beta" });
	});

	test("rebases predictions over an authoritative provider batch without learning it early", () => {
		const store = patternStore();
		trainGrepRead(store, "one", "src/a.ts");
		trainGrepRead(store, "two", "src/b.ts");
		const before = store.recent("probe");

		const candidates = store.predictAfterBatch("probe", [
			input("probe", "grep", { pattern: "TODO" }, {
				turnID: "probe:scan",
				outputPaths: ["src/c.ts"],
			}),
		]);

		expect(candidates).toContainEqual(
			expect.objectContaining({
				source: "pattern_aware",
				tool: "read",
				input: { filePath: "src/c.ts" },
			}),
		);
		expect(store.recent("probe")).toEqual(before);
		expect(store.predictAfterBatch("missing-payload", [input("missing-payload", "grep", { pattern: "TODO" })])
			.find((item) => item.tool === "read")).toBeUndefined();
	});

	test.each([
		["tools", ["TODO", "TODO", "TODO"], "src/**/*.ts", false],
		["arguments", ["aaa", "zzz", "xyz"], "mmm", false],
		["unicode", ["caf\u00e9", "caf\u00e9", "caf\u00e9"], "cafe\u0301", true],
	] as const)("learns canonical provider batches across %s without sibling causality", (_name, queries, sibling, sameTool) => {
		const store = patternStore();
		for (const [sessionID, filePath, reverse] of [
			["one", "src/a.ts", false],
			["two", "src/b.ts", true],
		] as const) {
			const batch = scanBatch(sessionID, filePath, [queries[reverse ? 1 : 0], sibling], sameTool);
			store.observeBatch(reverse ? [...batch].reverse() : batch);
			expect(store.recent(sessionID).map((event) => event.input.pattern)).toEqual([sibling, queries[reverse ? 1 : 0]]);
			store.observeBatch([input(sessionID, "read", { filePath }, { turnID: `${sessionID}:read`, })]);
			store.finishSession(sessionID);
		}

		const batch = scanBatch("probe", "src/c.ts", [queries[2], sibling], sameTool).reverse();
		const previews = store.predictAfterBatch("probe", batch).filter((item) => item.tool === "read"), preview = previews[0];
		expect(previews.map((item) => item.input)).toEqual([{ filePath: "src/c.ts" }]);
		const before = store.snapshot(), seed = { visitedPatternIDs: ["foreign-batch"], pathProbability: 0.5 };
		const peer = store.predictAfterBatch("probe", batch, {}, settings(), seed).find((item) => item.tool === "read");
		expect(peer).toMatchObject({ input: preview?.input, depth: 2, conditionalProbability: preview?.conditionalProbability });
		expect(peer?.empiricalProbability).toBeCloseTo(preview!.empiricalProbability * 0.5);
		expect(store.predictAfterBatch("probe", batch, {}, settings({ maxPredictionDepth: 1 }), seed)).toEqual([]);
		expect(store.snapshot()).toEqual(before);
		expect(store.recent("probe")).toEqual([]);
		store.observeBatch(batch);
		for (const input of batch) (input.outputPaths as string[])?.splice(0);
		const candidate = store.predict("probe").find((item) => item.tool === "read");

		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
		expect(preview?.input).toEqual(candidate?.input);
		expect(candidate?.dependencies).toContainEqual(
			expect.objectContaining({
				targetPath: ["filePath"],
				sources: expect.arrayContaining([expect.objectContaining({ field: "outputPaths", path: [0] })]),
			}),
		);
		expect(
			store.snapshot().some(
				(pattern) =>
					["grep", "find"].includes(pattern.targetTool) &&
					pattern.context.some((event) => ["grep", "find"].includes(event.tool)),
			),
		).toBe(false);
	});

	test.each([
		["co-occurring", () => ["find", "grep"] as const, 1],
		["alternative", (index: number) => [index % 2 === 0 ? "find" : "grep"] as const, 0.5],
	] as const)("calibrates %s batch members as marginal events", (_name, targets, expected) => {
		const store = patternStore({ maxContextLength: 1, maxFutureGap: 0 });
		for (let index = 0; index < 8; index++) {
			observeBatchTransition(
				store,
				`sample-${index}`,
				targets(index).map((tool) => ({ tool, input: { pattern: tool === "find" ? "*.ts" : "TODO" } })),
			);
		}
		const probabilities = (() => {
			const sessionID = "probe";
			store.observeBatch([
				input(sessionID, "inspect", { scope: "src" }, { turnID: `${sessionID}:context`, }),
			]);
			return new Map(
				store.predict(sessionID).map((candidate) => [candidate.tool, candidate.conditionalProbability]),
			);
		})();
		for (const tool of ["find", "grep"]) {
			if (expected === 1) expect(probabilities.get(tool)).toBeGreaterThan(0.9);
			else expect(probabilities.get(tool)).toBeCloseTo(expected);
		}
	});

	test("counts repeated same-tool batch members once while sample windows slide", () => {
		const store = patternStore({ maxContextLength: 1, maxFutureGap: 0 });
		for (let index = 0; index < 16; index++)
			observeBatchTransition(
				store,
				`same-tool-${index}`,
				["one.ts", "two.ts"].map((filePath) => ({ tool: "read", input: { filePath } })),
			);
		store.observeBatch([input("probe", "inspect", {}, { turnID: "probe:context", })]);
		const reads = store.predict("probe").filter((candidate) => candidate.tool === "read");
		expect(reads).toHaveLength(2);
		expect(reads.every((candidate) => candidate.conditionalProbability > 0.9)).toBe(true);
	});

	test("learns mappers per gap and merges equivalent actions only at prediction", () => {
		const store = patternStore({ maxContextLength: 1, maxFutureGap: 1, minOccurrences: 2, futureGapCoverage: 0.9 });
		for (const [sessionID, filePath] of [
			["immediate-a", "src/a.ts"],
			["immediate-b", "src/b.ts"],
		] as const) {
			store.observe(input(sessionID, "grep", {}, { outputPaths: [filePath] }));
			store.observe(input(sessionID, "read", { filePath }));
			store.finishSession(sessionID);
		}
		for (const [sessionID, filePath] of [
			["delayed-a", "src/c.ts"],
			["delayed-b", "src/d.ts"],
		] as const) {
			store.observe(input(sessionID, "grep", {}, { outputPaths: [filePath] }));
			store.observe(input(sessionID, "bash", { command: "pwd" }));
			store.observe(input(sessionID, "read", { filePath }));
			store.finishSession(sessionID);
		}

		const patterns = store
			.snapshot()
			.filter((item) => item.targetTool === "read" && item.context.length === 1 && item.context[0]?.tool === "grep");
		expect(patterns.map((pattern) => pattern.gapCounts)).toEqual([{ "0": 2 }, { "1": 2 }]);

		store.observe(input("probe", "grep", {}, { outputPaths: ["src/c.ts"] }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");
		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
		expect(candidate).toMatchObject({ horizon: 1, latestHorizon: 1 });
	});

	test.each([1, 1000])("retains the observed deadline after gap decay at sequence %s", (lastSeenSequence) => {
		const gapSettings = settings({ maxFutureGap: 8, futureGapCoverage: 0.8, decayHalfLifeEvents: 10 });
		const store = new PatternAwareStore(gapSettings);
		const immediate = new PatternAwareStore(gapSettings);
		const pattern = acceptPattern(store, { "0": 9, "5": 1 }, {
			lastSeenSequence, gapLastSeen: { "0": lastSeenSequence, "5": 1 },
		});
		acceptPattern(immediate, { "0": 10 }, { lastSeenSequence, gapLastSeen: { "0": lastSeenSequence } });

		store.observe(input("probe", "grep", { pattern: "TODO" }));
		immediate.observe(input("probe", "grep", { pattern: "TODO" }));

		const candidate = store.predict("probe").find((item) => item.tool === "read");
		const immediateCandidate = immediate.predict("probe").find((item) => item.tool === "read");
		expect(candidate).toMatchObject({
			horizon: 0,
			latestHorizon: 5,
		});
		expect(immediateCandidate).toMatchObject({ horizon: 0, latestHorizon: 0 });
		expect(candidate?.conditionalProbability).toBe(immediateCandidate?.conditionalProbability);
		for (let gap = 0; gap < 5; gap++) {
			store.observe(input("probe", "bash", { command: `step-${gap}` }));
		}
		store.observe(input("probe", "read", { path: "README.md" }));
		expect(store.snapshot().find((item) => item.id === pattern.id)).toMatchObject({
			historicalOpportunities: pattern.historicalOpportunities + 1,
			historicalMatches: pattern.historicalMatches + 1,
		});
	});

	test("derives orthogonal feedback only from authoritative prediction settlements", () => {
		const store = patternStore({ minOccurrences: 2, decayHalfLifeEvents: 1 });
		acceptPattern(store, { "0": 10 }, { id: "attributed" });
		store.observe(input("probe", "grep", { pattern: "TODO" }));
		const beforeUnobserved = store.predict("probe").find((item) => item.patternID === "attributed");
		for (let index = 0; index < 4; index++) store.issued("attributed");
		store.settled("attributed", unobservedSettlement("source", "timeout"));
		const afterUnobserved = store.predict("probe").find((item) => item.patternID === "attributed");
		expect(afterUnobserved?.empiricalProbability).toBe(beforeUnobserved?.empiricalProbability);
		store.settled("attributed", unmatchedSettlement());
		store.settled("attributed", rejectedSettlement("freshness", "resource_changed"));
		const afterFreshnessRejection = store.predict("probe").find((item) => item.patternID === "attributed")!;
		const diagnostic = JSON.parse(afterFreshnessRejection.diagnostic);
		expect(afterFreshnessRejection.adoptionProbability).toBeCloseTo(0.5);
		expect(afterFreshnessRejection.expectedLatencyBenefitMs / afterFreshnessRejection.expectedDurationMs).toBeCloseTo(
			afterFreshnessRejection.empiricalProbability *
				afterFreshnessRejection.adoptionProbability *
				diagnostic.mapperConfidence,
		);
		store.settled("attributed", rejectedSettlement("freshness", "resource_changed"));
		const afterRepeatedRejection = store.predict("probe").find((item) => item.patternID === "attributed")!;
		expect(afterRepeatedRejection.adoptionProbability).toBeCloseTo(1 / 3);
		store.settled("attributed", adoptedSettlement());

		const pattern = store.snapshot().find((item) => item.id === "attributed");
		expect(pattern?.adoptionProbability).toBeCloseTo(0.5);
		expect(pattern).toMatchObject({
			feedback: {
				issued: 4,
				observed: 4,
				matched: 3,
				adopted: 1,
				rejectedAfterMatch: { freshness: 2 },
				unobserved: { "source:timeout": 1 },
			},
		});
		for (let index = 0; index < 4; index++)
			store.observe(input(`decay-${index}`, "lsp", { operation: "symbols" }));
		expect(
			store.predict("probe").find((item) => item.patternID === "attributed")!.adoptionProbability,
		).toBeGreaterThan(pattern!.adoptionProbability);
		const afterObservedMiss = store.predict("probe").find((item) => item.patternID === "attributed");
		expect(afterObservedMiss!.empiricalProbability).toBeLessThan(beforeUnobserved!.empiricalProbability);
	});

	test("discounts old mismatch evidence so fresh matches recover after drift", () => {
		const store = patternStore({ minOccurrences: 2, decayHalfLifeEvents: 2 });
		acceptPattern(store, { "0": 10 }, { id: "drift" });
		store.observe(input("probe", "grep", { pattern: "TODO" }));
		for (let index = 0; index < 2; index++) {
			store.issued("drift");
			store.settled("drift", unmatchedSettlement());
		}
		const afterFailures = store.predict("probe").find((item) => item.patternID === "drift");
		expect(afterFailures).toBeDefined();

		for (let index = 0; index < 8; index++) {
			store.observeTurn();
		}
		for (let index = 0; index < 2; index++) {
			store.issued("drift");
			store.settled("drift", adoptedSettlement());
		}

		const afterRecovery = store.predict("probe").find((item) => item.patternID === "drift");
		expect(afterRecovery).toBeDefined();
		const recoveredPattern = store.snapshot().find((item) => item.id === "drift");
		expect(recoveredPattern!.feedback.recentMatchedWeight).toBeGreaterThan(
			recoveredPattern!.feedback.recentMismatchedWeight,
		);
	});

	test("lets recent gap behavior replace stale high-volume history", () => {
		const store = patternStore({
				maxFutureGap: 8,
				futureGapCoverage: 0.9,
				decayHalfLifeEvents: 10,
			});
		acceptPattern(store, { "0": 1000, "3": 10 }, {
			gapLastSeen: { "0": 0, "3": 1000 },
			lastSeenSequence: 1000,
			occurrences: 1010,
			replayMatches: 1010,
			historicalOpportunities: 1010,
			historicalMatches: 1010,
		});

		store.observe(input("probe", "grep", { pattern: "TODO" }));

		expect(store.predict("probe").find((item) => item.tool === "read")?.horizon).toBe(3);
	});

	test("does not promote multiple gap views of one target into repeated support", () => {
		const store = patternStore({ maxContextLength: 1, maxFutureGap: 1, minOccurrences: 2 });
		store.observe(input("one", "grep", { pattern: "a" }, { outputPaths: ["src"] }));
		store.observe(input("one", "grep", { pattern: "b" }, { outputPaths: ["src/a.ts"] }));
		store.observe(input("one", "read", { filePath: "src/a.ts" }));
		store.observe(input("probe", "grep", { pattern: "TODO" }, { outputPaths: ["src/b.ts"] }));

		expect(store.snapshot().filter((item) => item.targetTool === "read")).toEqual([
			expect.objectContaining({ occurrences: 1, gapCounts: { "0": 1 } }),
		]);
		const candidate = store.predict("probe").find((item) => item.tool === "read")!;
		store.issued(candidate.patternID);
		expect(store.predict("probe").filter((item) => item.tool === "read")).toHaveLength(0);
	});

	test("continues only schema-compatible learned targets", () => {
		const store = patternStore();
		trainGrepRead(store, "one", "src/a.ts", "read-base");
		trainGrepRead(store, "two", "src/b.ts", "read-base");
		store.observe(input("three", "grep", {}, { outputPaths: ["src/c.ts"] }));

		expect(store.predict("three", { read: "read-other" }).filter((item) => item.tool === "read")).toHaveLength(0);
		expect(store.predict("three", { read: "read-base" })).toContainEqual(
			expect.objectContaining({
				type: "tool_call",
				source: "pattern_aware",
				tool: "read",
				input: expect.objectContaining({ filePath: "src/c.ts" }),
			}),
		);
	});

	test("persists a deduplicated learning table and rebuilds its opportunity index", async () => {
		const file = await patternFile();
		const first = patternStore({}, file);
		await first.load();
		trainGrepRead(first, "one", "src/a.ts");
		trainGrepRead(first, "two", "src/b.ts");
		await first.flush();

		const raw = await fs.readFile(file, "utf8");
		expect(raw).not.toContain('"history"');
		const persisted = JSON.parse(raw);
		expect(persisted.version).toBe(20);
		expect(persisted.events.length).toBeGreaterThan(0);
		expect(
			persisted.pools.every((pool: { samples: Array<{ context: number[]; target: number }> }) =>
				pool.samples.every(
					(sample) => sample.context.every((event) => Number.isInteger(event)) && Number.isInteger(sample.target),
				),
			),
		).toBe(true);
		for (const version of [persisted.version - 1, persisted.version + 1]) {
			const unsupported = JSON.stringify({ ...persisted, version });
			await fs.writeFile(file, unsupported);
			const ignored = patternStore({}, file);
			await ignored.load(); await ignored.flush();
			expect(ignored.snapshot()).toEqual([]);
			expect(await fs.readFile(file, "utf8")).toBe(unsupported);
		}
		await fs.writeFile(file, raw);
		const second = patternStore({}, file);
		await second.load();
		second.observe(input("three", "grep", {}, { outputPaths: ["src/c.ts"] }));

		expect(second.predict("three").some((item) => item.tool === "read")).toBe(true);
		second.observe(input("three", "read", { filePath: "src/c.ts" }));
		expect(second.snapshot().find((item) => item.targetTool === "read")?.historicalOpportunities).toBe(3);
	});

	test.each([19, 20])("restores valid patterns and owns public snapshots (version=%s)", async (version) => {
		const file = await patternFile();
		const restoredInput = { path: "README.md", fields: { "\u00e9": 2, "e\u0301": 1 } };
		const valid = validatedGapPattern({ "0": 10 }, { id: "valid-persisted-pattern", bindings: constantBindings(restoredInput) });
		const counters = Object.keys(valid.feedback).filter((key) => typeof valid.feedback[key as keyof typeof valid.feedback] === "number");
		Object.assign(valid.feedback, Object.fromEntries(counters.map((key, index) => [key, index + 1])));
		await fs.writeFile(file, JSON.stringify({ version,
			patterns: [valid,
				{ ...valid, id: "bad-context", context: [{ tool: 7, outcome: "success" }] },
				{ ...valid, id: "bad-target-path", bindings: { "not-json": { type: "constant", value: "x" } } },
				{ ...valid, id: "bad-binding", bindings: { '["filePath"]': { type: "event", relativeEvent: -1, field: "output", path: "not-an-array" } } },
				...counters.flatMap((key) => [undefined, null, -1, "0", Number.NaN, Number.POSITIVE_INFINITY].map((value, index) =>
					({ ...valid, id: `bad-feedback-${key}-${index}`, feedback: { ...valid.feedback, [key]: value } }))),
				...(["rejectedAfterMatch", "unobserved"] as const).map((key) => ({ ...valid, id: `bad-feedback-${key}`, feedback: { ...valid.feedback, [key]: { invalid: -1 } } })),
			],
			events: [event("one", "grep"), { sequence: 2 }, event("one", "read", restoredInput)],
			pools: [1, 99, 2].map((target) => ({ key: `bad-sample-${target}`, context: [{ tool: "grep", outcome: "success" }],
				targetTool: "read", gap: 0, samples: [{ context: [0], target, gap: target === 2 ? 1 : 0 }] })), sequenceCounts: [],
		}));
		const store = patternStore({ minOccurrences: 1 }, file);
		await expect(store.load()).resolves.toBeUndefined();
		if (version === 19) { expect(store.snapshot()).toEqual([]); return; }
		const expected = store.snapshot(), exposed = store.snapshot()[0]!;
		expect(expected.map((pattern) => pattern.id)).toEqual(["valid-persisted-pattern"]);
		expect(exposed.feedback).toEqual(valid.feedback);
		for (const value of [exposed, exposed.context[0]!, exposed.bindings['["path"]']!, exposed.feedback,
			exposed.feedback.rejectedAfterMatch, exposed.feedback.unobserved, exposed.gapCounts, exposed.gapLastSeen!]) Object.assign(value, { external: 99 });
		(exposed.dependencies as unknown[]).push({});
		expect(store.snapshot()).toEqual(expected);
		store.issued(valid.id); await store.flush();
		const persisted = JSON.parse(await fs.readFile(file, "utf8"));
		expect(persisted.pools).toEqual([]);
		expect(persisted.patterns[0].feedback).toEqual({ ...valid.feedback, issued: valid.feedback.issued + 1 });
		const observed = input("restored", "grep", { query: "original" }, { outputPaths: ["a.ts"] });
		store.observe(observed);
		observed.input.query = "changed";
		const recent = store.recent("restored");
		expect(recent[0]!.input).toEqual({ query: "original" });
		Object.assign(recent[0]!.outputPaths!, { 0: "changed.ts" });
		expect(store.recent("restored")[0]!.outputPaths).toEqual(["a.ts"]);
		const prediction = store.predict("restored").find((item) => item.patternID === valid.id)!;
		expect(prediction.input).toEqual(restoredInput);
		Object.assign(prediction.input.fields!, { external: 1 });
		expect(store.predict("restored").find((item) => item.patternID === valid.id)!.input).toEqual(restoredInput);
		expect(store.registerValidatedPattern(valid)).toBe(true);
		const registered = store.snapshot();
		for (const record of [valid.gapCounts, valid.feedback.unobserved, valid.feedback.rejectedAfterMatch]) Object.assign(record, { external: 7 });
		expect(store.snapshot()).toEqual(registered);
		await store.flush();
	});

	test.each([false, true])("shares analyzer state and drains every release caller (flush failure=%s)", async (fails) => {
		const workspace = await directories.create();
		const first = await acquirePatternAwareStore(workspace, settings());
		const second = await acquirePatternAwareStore(workspace, settings());
		const predictorOnly = await acquirePatternAwareStore(
			workspace,
			settings({ beamWidth: PATTERN_AWARE_DEFAULTS.beamWidth + 1, maxPredictionDepth: 1 }),
		);
		const differentAnalyzer = await acquirePatternAwareStore(
			workspace,
			settings({ maxContextLength: PATTERN_AWARE_DEFAULTS.maxContextLength + 1 }),
		);

		try {
			expect(second.store).toBe(first.store);
			expect(predictorOnly.store).toBe(first.store);
			expect(differentAnalyzer.store).not.toBe(first.store);
			const { promise: gate, resolve: resume } = deferred(), failure = new Error("flush failed"), completed = vi.fn();
			const flush = vi.spyOn(first.store, "flush").mockImplementationOnce(async () => { await gate; if (fails) throw failure; });
			const released = Promise.allSettled([first.release().finally(completed), first.release().finally(completed)]);
			try {
				await nextTurn();
				expect(completed).not.toHaveBeenCalled(); expect(flush).toHaveBeenCalledOnce();
			} finally { resume(); await released; flush.mockRestore(); }
			expect(await released).toEqual(Array(2).fill(fails ? { status: "rejected", reason: failure } : { status: "fulfilled", value: undefined }));
			await second.release();
			const third = await acquirePatternAwareStore(workspace, settings());
			try { expect(third.store).toBe(second.store); }
			finally { await third.release(); }

			await predictorOnly.release();
			await differentAnalyzer.release();
			const next = await acquirePatternAwareStore(workspace, settings());
			try { expect(next.store).not.toBe(first.store); }
			finally { await next.release(); }
		} finally { await Promise.allSettled([first.release(), second.release(), predictorOnly.release(), differentAnalyzer.release()]); }
	});

	test("enforces the configured context bound while learning, restoring, and registering patterns", async () => {
		const file = await patternFile();
		const long = validatedGapPattern(
			{ "0": 2 },
			{
				context: [
					{ tool: "grep", outcome: "success" },
					{ tool: "read", outcome: "success" },
				],
			},
		);
		await fs.writeFile(
			file,
			JSON.stringify({ version: 20, patterns: [long], events: [], pools: [], sequenceCounts: [] }),
		);

		const store = patternStore({ maxContextLength: 1 }, file);
		await store.load();
		expect(store.snapshot()).toEqual([]);
		expect(store.registerValidatedPattern(long)).toBe(false);
		for (const sessionID of ["learn-a", "learn-b"]) {
			store.observeBatch([
				input(sessionID, "grep", { pattern: "TODO" }),
				input(sessionID, "read", { filePath: "src/a.ts" }),
			]);
			store.observe(input(sessionID, "write", { filePath: "src/a.ts" }));
			store.finishSession(sessionID);
		}
		expect(store.snapshot()).toEqual([]);
	});

	test("transfers data-flow patterns across processes before global support", async () => {
		const file = await patternFile();
		const first = patternStore({ minOccurrences: 2 }, file);
		await first.load();
		trainGrepRead(first, "one", "src/a.ts");
		first.finishSession("one");
		await first.flush();

		const persisted = JSON.parse(await fs.readFile(file, "utf8"));
		expect(persisted.patterns).toEqual([expect.objectContaining({ targetTool: "read", occurrences: 1 })]);
		expect(persisted.pools.length).toBeGreaterThan(0);

		const second = patternStore({ minOccurrences: 2 }, file);
		await second.load();
		second.observe(input("two", "grep", {}, { outputPaths: ["src/b.ts"] }));
		const candidates = second.predict("two");
		expect(candidates).toContainEqual(expect.objectContaining({ tool: "read", input: { filePath: "src/b.ts" } }));
		second.observe(input("two", "read", { filePath: "src/b.ts" }));

		expect(second.snapshot().some((item) => item.targetTool === "read")).toBe(true);
	});

	test("persists PPM counts so beam ordering survives a process restart", async () => {
		const file = await patternFile();
		const configured = settings({ beamWidth: 1 });
		const first = new PatternAwareStore(configured, file);
		await first.load();
		for (let index = 0; index < 4; index++) {
			first.observe(input(`read-${index}`, "grep"));
			first.observe(input(`read-${index}`, "read", { path: "README.md" }));
		}
		await first.flush();
		const previous = await fs.readFile(file);
		for (let index = 0; index < 2; index++) {
			first.observe(input(`bash-${index}`, "grep"));
			first.observe(input(`bash-${index}`, "bash", { command: "npm test" }));
		}
		const fault = new Error("injected replacement failure"), rename = vi.spyOn(fs, "rename").mockRejectedValue(fault);
		try {
			expect(await Promise.allSettled([first.flush(), first.flush()])).toEqual(Array(2).fill({ status: "rejected", reason: fault }));
			expect(await fs.readFile(file)).toEqual(previous);
			expect(await fs.readdir(path.dirname(file))).toEqual([path.basename(file)]);
		} finally { rename.mockRestore(); }
		await first.flush();

		const persisted = JSON.parse(await fs.readFile(file, "utf8"));
		expect(persisted.version).toBe(20);
		expect(persisted.sequenceCounts.length).toBeGreaterThan(0);
		const restored = new PatternAwareStore(configured, file);
		await restored.load();
		restored.observe(input("probe", "grep"));

		expect(restored.predict("probe")).toEqual([
			expect.objectContaining({ tool: "read", input: { path: "README.md" } }),
		]);
	});

	test("keeps constant patterns task-local until independently supported", async () => {
		const local = patternStore({ minOccurrences: 2 });
		local.observe(input("local", "inspect"));
		local.observe(input("local", "read", { filePath: "README.md" }));
		local.observe(input("local", "inspect"));
		expect(local.predict("local")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { filePath: "README.md" } }),
		);

		const file = await patternFile();
		const train = async (sessionID: string) => {
			const store = patternStore({ minOccurrences: 2 }, file);
			await store.load();
			store.observe(input(sessionID, "inspect", {}, { output: { kind: "path" } }));
			store.observe(input(sessionID, "read", { filePath: "README.md" }));
			store.finishSession(sessionID);
			await store.flush();
		};

		await train("one");
		const isolated = patternStore({ minOccurrences: 2 }, file);
		await isolated.load();
		isolated.observe(input("probe", "inspect", {}, { output: { kind: "path" } }));
		expect(isolated.predict("probe").some((candidate) => candidate.tool === "read")).toBe(false);
		isolated.finishSession("probe");
		await isolated.flush();
		for (const sessionID of ["two", "three", "four"]) {
			await train(sessionID);
		}

		const restored = patternStore({ minOccurrences: 2 }, file);
		await restored.load();
		restored.observe(input("probe", "inspect", {}, { output: { kind: "path" } }));
		const persisted = JSON.parse(await fs.readFile(file, "utf8"));

		expect(restored.predict("probe")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { filePath: "README.md" } }),
		);
		expect(Math.max(...persisted.pools.map((pool: { samples: unknown[] }) => pool.samples.length))).toBe(4);
	});

	test("normalizes partial PatternAware settings", () => {
		expect(
			patternAwareSettings({ maxContextLength: 3, beamWidth: 2, maxPredictionDepth: 4, maxFutureGap: 0 }),
		).toEqual({
			...PATTERN_AWARE_DEFAULTS,
			maxContextLength: 3,
			beamWidth: 2,
			maxPredictionDepth: 4,
			maxFutureGap: 0,
		});
		expect(patternAwareSettings({ beamWidth: 0, maxPredictionDepth: Number.NaN })).toMatchObject({
			beamWidth: PATTERN_AWARE_DEFAULTS.beamWidth,
			maxPredictionDepth: PATTERN_AWARE_DEFAULTS.maxPredictionDepth,
		});
	});

	test("probes an adjacent transition once and preserves feedback until configured promotion", () => {
		const store = patternStore({ minOccurrences: 3 });
		trainGrepRead(store, "one", "src/a.ts");
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 1,
			feedback: { issued: 0 },
		});

		store.observe(input("two", "grep", {}, { outputPaths: ["src/b.ts"] }));
		const candidate = store.predict("two").find((item) => item.tool === "read")!;
		expect(candidate.input).toEqual({ filePath: "src/b.ts" });
		expect(candidate.background).toBe(true);
		store.issued(candidate.patternID);
		store.settled(candidate.patternID, adoptedSettlement());
		expect(store.predict("two").filter((item) => item.tool === "read")).toHaveLength(0);

		store.observe(input("two", "read", { filePath: "src/b.ts" }));
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 2,
			feedback: { issued: 1, matched: 1, adopted: 1 },
		});
		store.observe(input("three", "grep", {}, { outputPaths: ["src/c.ts"] }));
		expect(store.predict("three")).not.toContainEqual(expect.objectContaining({ tool: "read" }));
		store.observe(input("three", "read", { filePath: "src/c.ts" }));
		store.observe(input("four", "grep", {}, { outputPaths: ["src/d.ts"] }));
		const promoted = store.predict("four");
		expect(promoted).toContainEqual(expect.objectContaining({ tool: "read", input: { filePath: "src/d.ts" } }));
		expect(promoted.find((item) => item.tool === "read")?.background).toBeUndefined();
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 3,
			feedback: { issued: 1 },
		});
	});

	test("emits weak control-flow candidates for bounded utility admission", () => {
		const store = patternStore({ minBindingReplayProbability: 0.75 });
		trainGrepRead(store, "one", "src/a.ts");
		trainGrepRead(store, "two", "src/b.ts");

		store.observe(input("miss-one", "grep", {}, { outputPaths: ["src/c.ts"] }));
		store.finishSession("miss-one");
		store.observe(input("miss-two", "grep", {}, { outputPaths: ["src/d.ts"] }));
		store.finishSession("miss-two");

		const pattern = store
			.snapshot()
			.find((item) => item.targetTool === "read" && item.context.at(-1)?.tool === "grep");
		expect(pattern?.historicalMatches).toBe(2);
		expect(pattern?.historicalOpportunities).toBe(4);
		expect(pattern?.empiricalProbability).toBe(0.5);

		store.observe(input("probe", "grep", {}, { outputPaths: ["src/e.ts"] }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");
		expect(candidate?.empiricalProbability).toBeGreaterThan(0);
		expect(candidate?.empiricalProbability).toBeLessThan(0.75);
		expect(store.registerValidatedPattern(validatedGapPattern({ "0": 10 },
			{ id: "unreliable-binding", occurrences: 10, replayMatches: 7 }))).toBe(false);
	});

	test("learns indexed field fallbacks across historical samples", () => {
		const store = patternStore();
		trainOutputRead(store, "one", { primary: "src/a.ts" }, "src/a.ts");
		trainOutputRead(store, "two", { fallback: "src/b.ts" }, "src/b.ts");

		store.observe(input("probe", "grep", {}, { output: { fallback: "src/c.ts" } }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");

		expect(candidate?.type).toBe("tool_call");
		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
	});

	test("learns a stable mapper branch after unrelated evidence", () => {
		const store = patternStore({ maxContextLength: 1, maxFutureGap: 0 });
		for (let index = 0; index < 4; index++) {
			trainOutputRead(store, `noise-${index}`, { path: `src/source-${index}.ts` }, `src/unrelated-${index}.ts`);
		}
		for (let index = 0; index < 2; index++) {
			const file = `src/stable-${index}.ts`;
			trainOutputRead(store, `stable-${index}`, { path: file }, file);
		}

		store.observe(input("probe", "grep", {}, { output: { path: "src/result.ts" } }));
		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			filePath: "src/result.ts",
		});
	});

	test("combines multiple structured fields instead of memorizing a concrete path", () => {
		const store = patternStore();
		trainJoinedRead(store, "one", "services/a", "alpha");
		trainJoinedRead(store, "two", "services/b", "beta");

		store.observe(
			input("probe", "inspect", {}, {
				output: { root: "services/c", name: "gamma" },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			filePath: "services/c/gamma",
		});
	});

	test("does not compose presentation text into a path binding", () => {
		const store = patternStore();
		for (const [sessionID, root, preview] of [
			["one", "services/a", "alpha.ts"],
			["two", "services/b", "beta.ts"],
		]) {
			store.observe(
				input(sessionID, "read", { filePath: root }, {
					output: { preview },
				}),
			);
			store.observe(input(sessionID, "read", { filePath: `${root}/${preview}` }));
			store.finishSession(sessionID);
		}

		store.observe(
			input("probe", "read", { filePath: "services/c" }, {
				output: { preview: "export const value = 1" },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")).toBeUndefined();
	});

	test("expands a structured collection when actor ordering varies", () => {
		const store = patternStore();
		trainResultReads(store, "one", ["src/a.ts", "src/b.ts"], ["src/b.ts", "src/a.ts"]);
		trainResultReads(store, "two", ["src/c.ts", "src/d.ts"], ["src/c.ts", "src/d.ts"]);

		store.observe(
			input("probe", "grep", { pattern: "symbol" }, {
				output: { results: [{ path: "src/e.ts" }, { path: "src/f.ts" }] },
			}),
		);
		const paths = store
			.predict("probe")
			.filter((item) => item.tool === "read" && item.type === "tool_call")
			.map((item) => item.input.filePath);

		expect(paths).toContain("src/e.ts");
		expect(paths).toContain("src/f.ts");
	});

	test("does not dilute a continuation path with unrelated sibling candidates", () => {
		const store = patternStore();
		for (const [id, path] of [
			["read-source", "src/source.ts"],
			["read-test", "test/source.test.ts"],
		] as const) {
			acceptPattern(store, { "0": 10 }, { id, bindings: { '["path"]': { type: "constant", value: path } } });
		}
		acceptPattern(store, { "0": 10 }, {
			id: "read-bash",
			context: [{ tool: "read", outcome: "success" }],
			targetTool: "bash",
			bindings: { '["command"]': { type: "constant", value: "npm test" } },
		});

		store.observe(input("probe", "grep", { pattern: "source" }));
		const source = store.predict("probe").find((item) => item.input.path === "src/source.ts");
		expect(source?.empiricalProbability).toBeGreaterThan(0.9);

		const child = store
			.continue(source!.continuation, input("probe", "read", { path: "src/source.ts" }))
			.find((item) => item.tool === "bash");
		expect(child?.conditionalProbability).toBeGreaterThan(0.9);
		expect(child?.empiricalProbability).toBeGreaterThan(0.8);
	});

	test.each([
		["raw collection frequency", "filePath", ["src/likely.ts", "src/unlikely.ts"], false, 2],
		["path aliases", "path", ["src/a.ts", "./src/a.ts"], true, 1],
		["default offset", "offset", [undefined, 1], true, 1],
		["projectable ranges", "limit", [10, 20], true, 2],
		["unkeyed aliases", "path", ["src/a.ts", "./src/a.ts"], false, 2],
	] as const)("conserves alternative mass and independent evidence for %s", (_name, field, values, keyed, count) => {
		const store = patternStore({ decayHalfLifeEvents: 0 }, undefined, keyed ? piActionSemantics() : undefined);
		const base = field === "offset" || field === "limit" ? { path: "src/a.ts" } : {};
		const pattern = acceptPattern(store, { "0": 10 }, {
			id: "ranked-results",
			bindings: { ...constantBindings(base), ...collectionBindings({ "0": 9, "1": 1 }, field) },
			feedback: patternFeedback({ recentAdoptedWeight: 2, recentRejectedWeight: 1 }),
		});
		store.observe(input("probe", "grep", { pattern: "source" }, { output: { results: values.map((path) => ({ path })) } }));
		const before = store.snapshot(), candidates = store.predict("probe");
		expect(candidates).toHaveLength(count);
		expect(candidates.reduce((sum, item) => sum + item.conditionalProbability, 0)).toBeCloseTo(21 / 22);
		if (count === 2) {
			const likely = candidates.find((item) => item.input[field] === values[0]);
			const unlikely = candidates.find((item) => item.input[field] === values[1]);
			expect(likely?.conditionalProbability).toBeGreaterThan(unlikely?.conditionalProbability ?? 1);
		}
		for (const candidate of candidates) {
			expect(candidate.supportingPatternIDs).toEqual(["ranked-results"]);
			expect(candidate.adoptionProbability).toBe(3 / 4);
			expect(candidate.expectedDurationMs).toBe(100);
		}
		expect(store.snapshot()).toEqual(before);
		if (count === 1) {
			acceptPattern(store, { "0": 30 }, { id: "independent", bindings: constantBindings({ path: "src/a.ts" }),
				occurrences: 30, replayMatches: 30, historicalOpportunities: 30, historicalMatches: 15, averageDurationMs: 300,
				feedback: patternFeedback({ recentRejectedWeight: 3 }),
			});
			const [merged] = store.predict("probe");
			expect(merged?.supportingPatternIDs).toEqual(["independent", "ranked-results"]);
			expect(merged?.conditionalProbability).toBeCloseTo(1 / 2);
			expect(merged?.expectedDurationMs).toBe(250);
			expect(merged?.adoptionProbability).toBe(3 / 7);
		}
		store.observe(input("probe", "read", { ...base, [field]: values[1] }));
		expect(store.snapshot().find((item) => item.id === pattern.id)).toMatchObject({
			historicalOpportunities: pattern.historicalOpportunities + 1, historicalMatches: pattern.historicalMatches + 1,
		});
	});

	test("uses the current branch to rank a later step before backing off to common suffixes", () => {
		const store = patternStore({ beamWidth: 1, maxContextLength: 3, maxFutureGap: 0, decayHalfLifeEvents: 0 });
		for (const [tool, operation, count] of [["ls", "test", 16], ["grep", "lint", 3]] as const) {
			for (let index = 0; index < count; index++) {
				const sessionID = `${tool}-${index}`, filePath = `src/${sessionID}.ts`;
				store.observe(input(sessionID, tool, {}, { outputPaths: [filePath] }));
				store.observe(input(sessionID, "read", { filePath }));
				store.observe(input(sessionID, "bash", { command: `bun ${operation} ${filePath}` }));
				store.finishSession(sessionID);
			}
		}
		store.observe(input("probe", "grep", {}, { outputPaths: ["lib/held-out.ts"] }));
		const parent = store.predict("probe")[0]!;
		expect(parent.input).toEqual({ filePath: "lib/held-out.ts" });
		const before = store.snapshot();
		const child = store.continue(parent.continuation, input("probe", "read", parent.input))[0]!;
		expect(child.input).toEqual({ command: "bun lint lib/held-out.ts" });
		expect(child.depth).toBe(2);
		expect(child.empiricalProbability).toBeLessThanOrEqual(parent.empiricalProbability);
		expect(store.snapshot()).toEqual(before);
		store.observe(input("unknown", "inspect", {}, { learnTarget: false }));
		store.observe(input("unknown", "read", { filePath: "lib/unseen.ts" }, { learnTarget: false }));
		expect(store.predict("unknown")[0]?.input).toEqual({ command: "bun test lib/unseen.ts" });
	});

	test.each<Record<string, number>>([{}, { "1": 10 }, { "0": 10, "1": 10 }])("keeps unsupported future gaps out of contextual backoff: %j", (gaps) => {
		const store = patternStore({ decayHalfLifeEvents: 0 });
		acceptPattern(store, gaps, { id: "future" });
		acceptPattern(store, { "0": 10 }, { id: "immediate", targetTool: "bash",
			context: [{ tool: "ls", outcome: "success" }, { tool: "grep", outcome: "success" }],
			bindings: constantBindings({ command: "npm test" }) });
		store.observe(input("probe", "ls", {}, { learnTarget: false }));
		store.observe(input("probe", "grep", {}, { learnTarget: false }));
		const candidate = store.predict("probe").find((item) => item.tool === "read")!;
		expect(candidate.conditionalProbability).toBe(store.snapshot().find((item) => item.id === "future")!.empiricalProbability);
	});

	test("charges evidence-annealed mapper complexity for transforms and ungrounded payloads", () => {
		const store = patternStore({ beamWidth: 1, maxContextLength: 1, maxFutureGap: 0 });
		const source = { type: "event" as const, relativeEvent: -1, field: "input" as const, path: ["path"] };
		for (const [id, binding, averageDurationMs] of [
			["z-direct", source, 100],
			["a-composite", { type: "template" as const, source, prefix: "wrong/", suffix: "" }, 100],
			["a-memorized", { type: "constant" as const, value: "README.md" }, 120],
		] as const) {
			acceptPattern(store, { "0": 2 }, {
				id,
				occurrences: 2,
				bindings: { '["path"]': binding },
				averageDurationMs,
			});
		}

		store.observe(input("probe", "grep", { path: "src/index.ts" }));
		expect(store.predict("probe")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { path: "src/index.ts" } }),
		);
	});

	test("retains multiple replayable mapper branches for one control context", () => {
		const store = patternStore();
		for (const [sessionID, source, target] of [
			["same-a", "src/a.ts", "src/a.ts"],
			["same-b", "src/b.ts", "src/b.ts"],
			["test-a", "src/c.ts", "src/c.ts.test"],
			["test-b", "src/d.ts", "src/d.ts.test"],
		] as const) {
			store.observe(input(sessionID, "inspect", { value: source }));
			store.observe(input(sessionID, "inspect", { value: target }));
			store.finishSession(sessionID);
		}

		store.observe(input("probe", "inspect", { value: "src/e.ts" }));
		expect(store.predict("probe").map((candidate) => candidate.input)).toEqual(
			expect.arrayContaining([{ value: "src/e.ts" }, { value: "src/e.ts.test" }]),
		);
	});

	test("contains non-finite persisted variant counts instead of emitting invalid probabilities", async () => {
		const file = await patternFile();
		const first = patternStore({}, file);
		await first.load();
		acceptPattern(first, { "0": 10 }, {
			id: "invalid-ranked-results",
			bindings: collectionBindings({ "0": Number.NaN, "1": Number.POSITIVE_INFINITY }),
		});
		await first.flush();
		const store = patternStore({}, file);
		await store.load();
		store.observe(
			input("probe-invalid-counts", "grep", {}, {
				output: { results: [{ path: "src/a.ts" }, { path: "src/b.ts" }] },
			}),
		);

		const probabilities = store
			.predict("probe-invalid-counts")
			.filter((item) => item.tool === "read")
			.map((item) => item.conditionalProbability);
		expect(probabilities).toHaveLength(2);
		expect(probabilities.every(Number.isFinite)).toBe(true);
		expect(probabilities.reduce((sum, value) => sum + value, 0)).toBeLessThanOrEqual(1);
	});

	test("does not let single-sample mappers bypass constant provenance evidence", () => {
		const store = patternStore();
		for (const sessionID of ["one", "two", "three"]) {
			store.observe(input(sessionID, "inspect", {}, { output: { kind: "path" } }));
			store.observe(input(sessionID, "read", { filePath: "README.md" }));
			store.finishSession(sessionID);
		}

		store.observe(input("probe", "inspect", {}, { output: { kind: "path" } }));
		expect(store.predict("probe")).toEqual([]);
		store.finishSession("probe");
		store.observe(input("four", "inspect", {}, { output: { kind: "path" } }));
		store.observe(input("four", "read", { filePath: "README.md" }));
		store.finishSession("four");

		store.observe(input("probe-after-four", "inspect", {}, { output: { kind: "path" } }));
		expect(store.predict("probe-after-four")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { filePath: "README.md" } }),
		);
		for (const [sessionID, overrides, matches] of [
			["wrong-output", { output: { kind: "text" } }, false],
			["unknown-output", { output: {} }, true],
			["wrong-tool", { tool: "other" }, false],
			["wrong-outcome", { outcome: "failure" as const }, false],
			["wrong-operation", { operation: "other" }, false],
		] as const) {
			store.observe(input(sessionID, "inspect", {}, { ...overrides }));
			expect(store.predict(sessionID).some((item) => item.tool === "read"), sessionID).toBe(matches);
		}
	});

	test("learns a reusable read range from varying actor windows", () => {
		const store = patternStore({}, undefined, piActionSemantics());
		for (const [sessionID, filePath, offset, limit] of [
			["one", "src/a.ts", 320, 100],
			["two", "src/b.ts", 840, 100],
		] as const) {
			store.observe(
				input(sessionID, "grep", { pattern: "symbol" }, {
					output: { results: [{ path: filePath, line: offset + 20 }] },
				}),
			);
			store.observe(input(sessionID, "read", { path: filePath, offset, limit }));
		}

		store.observe(
			input("probe", "grep", { pattern: "symbol" }, {
				output: { results: [{ path: "src/c.ts", line: 1_200 }] },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			path: "src/c.ts",
		});
	});

	test.each([
		{
			name: "covered",
			bindings: constantBindings({ path: "src/index.ts", offset: 1, limit: 100 }),
			actor: { path: "src/index.ts", offset: 20, limit: 10 },
			actorSchemaHash: "read-schema",
		},
		{
			name: "reverse coverage",
			bindings: constantBindings({ path: "src/index.ts", offset: 20, limit: 10 }),
			actor: { path: "src/index.ts", offset: 1, limit: 100 },
			actorSchemaHash: "read-schema",
		},
		{
			name: "different resource",
			bindings: constantBindings({ path: "src/index.ts", offset: 1, limit: 100 }),
			actor: { path: "src/other.ts", offset: 20, limit: 10 },
			actorSchemaHash: "read-schema",
		},
		{
			name: "different schema",
			bindings: constantBindings({ path: "src/index.ts", offset: 1, limit: 100 }),
			actor: { path: "src/index.ts", offset: 20, limit: 10 },
			actorSchemaHash: "new-read-schema",
		},
	])("accounts for projected feedback with $name", ({ name, bindings, actor, actorSchemaHash }) => {
		const store = patternStore({}, undefined, piActionSemantics());
		const pattern = validatedGapPattern(
			{ "0": 10 },
			{ id: `projected-negative-${name}`, bindings, targetSchemaHash: "read-schema" },
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(input(name, "grep", { pattern: "symbol" }));
		store.observe(input(name, "read", actor, { schemaHash: actorSchemaHash }));

		const after = store.snapshot().find((item) => item.id === pattern.id);
		expect(after?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		expect(after?.historicalMatches).toBe(pattern.historicalMatches + (name === "covered" ? 1 : 0));
	});

	test("owns the analyzer and action contract while deduplicating and memoizing K(a), including misses", () => {
		const semantics = piActionSemantics();
		let resolutions = 0;
		const contract = { ...semantics, projectors: [...semantics.projectors!],
			actionKey: (...args: Parameters<typeof semantics.actionKey>) => { resolutions++; return semantics.actionKey(...args); } };
		const configuration = settings(), store = new PatternAwareStore(configuration, undefined, contract);
		configuration.maxPatterns = 1;
		contract.actionKey = () => undefined;
		contract.projectors.length = 0;
		for (const [id, bindings] of [
			["default-implicit", constantBindings({ path: "src/index.ts" })],
			["default-offset-explicit", constantBindings({ path: "src/index.ts", offset: 1 })],
			["bounded-explicit", constantBindings({ path: "src/index.ts", offset: 1, limit: 2000 })],
			["disjoint", constantBindings({ path: "src/index.ts", offset: 2200, limit: 10 })],
			["unkeyable", constantBindings({ path: "../outside.ts" })],
		] as const) {
			acceptPattern(store, { "0": 10 }, { id, bindings });
		}

		store.observe(input("dedupe", "grep", { pattern: "symbol" }));
		const reads = store.predict("dedupe").filter((candidate) => candidate.tool === "read");

		expect(reads).toHaveLength(4);
		expect(reads.map((candidate) => candidate.input)).toEqual(
			expect.arrayContaining([
				{ path: "src/index.ts" },
				{ path: "src/index.ts", offset: 1, limit: 2000 },
				{ path: "src/index.ts", offset: 2200, limit: 10 },
				{ path: "../outside.ts" },
			]),
		);
		expect(
			JSON.parse(reads.find((candidate) => candidate.input.offset === undefined)!.diagnostic).supportingPatterns,
		).toEqual(expect.arrayContaining(["default-implicit", "default-offset-explicit"]));
		expect(reads.find((candidate) => candidate.input.offset === undefined)?.supportingPatternIDs).toEqual(
			expect.arrayContaining(["default-implicit", "default-offset-explicit"]),
		);
		const afterFirstPrediction = resolutions;
		store.predict("dedupe");
		expect(resolutions).toBe(afterFirstPrediction);
	});

	test("promotes canonical same-session actions beyond context only with authoritative, schema-compatible support", () => {
		for (const mode of ["command", "path aliases", "stale schema", "non-learning"] as const) {
			const config = settings({ maxContextLength: 1, maxFutureGap: 0, minOccurrences: 2 });
			const store = new PatternAwareStore(config, undefined, piActionSemantics());
			const tool = mode === "command" || mode === "non-learning" ? "bash" : "read";
			const first = tool === "bash" ? { command: "npm test" } : { path: "src/a.ts" };
			const inputs = [first, tool === "read" ? { ...first, offset: 1 } : first];
			for (const [index, value] of inputs.entries()) {
				store.observe(input(mode, tool, value, { schemaHash: "schema-base", learnTarget: mode !== "non-learning",
					outcome: index === 0 ? "failure" : "success", durationMs: index === 0 ? 500 : 700 }));
				for (let noise = 0; noise < 3; noise++)
					store.observe(input(mode, "read", { path: `noise-${index}-${noise}.ts` }));
				const recurrent = store.predict(mode, { [tool]: mode === "stale schema" ? "schema-other" : "schema-base" })
					.find((candidate) => candidate.patternID.startsWith("action-backoff:") && !candidate.background);
				if (index === 0 || mode === "stale schema" || mode === "non-learning") expect(recurrent, mode).toBeUndefined();
				else {
					expect(recurrent, mode).toMatchObject({ tool, input: first, horizon: 0, latestHorizon: 0 });
					expect(recurrent!.expectedDurationMs).toBeCloseTo(700 / (1 + 2 ** (-4 / config.decayHalfLifeEvents)), 10);
					expect(JSON.parse(recurrent!.diagnostic)).toMatchObject({ context: [], mapperConfidence: 1 });
					const patterns = new Set(store.snapshot().map((pattern) => pattern.id));
					expect(recurrent!.supportingPatternIDs.every((id) => patterns.has(id))).toBe(true);
					// Inspect demoted samples even when competing reads displace them from the default beam.
					const forecast = () => store.predict(mode, { [tool]: "schema-base" }, { ...config, beamWidth: 16 })
						.find((candidate) => candidate.actionIdentity === recurrent!.actionIdentity)!;
					const learned = store.snapshot(), history = store.recent(mode), before = forecast();
					const settle = (settlement: Parameters<PatternAwareStore["settled"]>[1]) => {
						const candidate = forecast();
						store.issued(candidate.continuation);
						store.settled(candidate.continuation, settlement);
						return forecast();
					};
					expect(settle(unobservedSettlement("control", "turn_closed"))).toEqual(before);
					const rejected = settle(rejectedSettlement("freshness", "resource_changed"));
					expect(rejected.adoptionProbability).toBeLessThan(recurrent!.adoptionProbability);
					expect(rejected.expectedLatencyBenefitMs).toBeLessThan(recurrent!.expectedLatencyBenefitMs);
					settle(unmatchedSettlement());
					const contradicted = settle(unmatchedSettlement());
					expect(contradicted.background).toBe(true);
					expect(contradicted.empiricalProbability).toBeLessThan(recurrent!.empiricalProbability);
					const adopted = settle(adoptedSettlement());
					expect(adopted.background).not.toBe(true);
					expect(adopted.adoptionProbability).toBeGreaterThan(rejected.adoptionProbability);
					expect(store.snapshot()).toEqual(learned);
					expect(store.recent(mode)).toEqual(history);
				}
			}
			store.finishSession(mode);
			store.observe(input("other", "read", { path: "other.ts" }));
			expect(store.predict("other").some((item) => item.patternID.startsWith("action-backoff:"))).toBe(false);
		}
	});

	test.each([0, 64])("weights recurrent evidence by each observation's age (half-life=%i)", (halfLife) => {
		const store = patternStore({ decayHalfLifeEvents: halfLife, maxContextLength: 1, maxFutureGap: 0, minOccurrences: 2 }, undefined, piActionSemantics());
		const samples: Array<{ path: string; sequence: number; durationMs: number }> = [];
		let sequence = 0;
		const observe = (path: string, durationMs: number, outcome: "success" | "failure" = "success") => {
			store.observe(input("weighted", "read", { path }, { durationMs, outcome }));
			samples.push({ path, sequence: ++sequence, durationMs: outcome === "success" ? durationMs : 0 });
		};
		for (let index = 0; index < 32; index++) observe("old.ts", 100);
		for (let index = 0; index < 256; index++, sequence++) store.observeTurn();
		observe("old.ts", 1);
		observe("fresh.ts", 5_000, "failure");
		observe("fresh.ts", 80); observe("fresh.ts", 80);
		store.observe(input("weighted", "marker", {}, { learnTarget: false })); sequence++;
		const weighted = samples.map((sample) => ({ ...sample, weight: halfLife ? 2 ** (-(sequence - sample.sequence) / halfLife) : 1 }));
		const total = weighted.reduce((sum, sample) => sum + sample.weight, 0);
		const predictions = store.predict("weighted");
		for (const path of ["old.ts", "fresh.ts"]) {
			const evidence = weighted.filter((sample) => sample.path === path), mass = evidence.reduce((sum, sample) => sum + sample.weight, 0);
			const prediction = predictions.find((candidate) => candidate.input.path === path)!;
			expect(prediction.background).not.toBe(true);
			expect(prediction.conditionalProbability).toBeCloseTo(mass / total, 12);
			expect(prediction.expectedDurationMs).toBeCloseTo(evidence.reduce((sum, sample) => sum + sample.durationMs * sample.weight, 0) / mass, 10);
		}
	});

	test.each([false, true])("bounds recurrent action samples and requires Actor evidence to unfold them: %s", (confirmed) => {
		const store = patternStore({ beamWidth: 2, maxContextLength: 1, maxFutureGap: 0, minOccurrences: 2 }, undefined, piActionSemantics());
		const sessionID = "sampled";
		for (let index = 0; index < 2; index++) {
			store.observe(input(sessionID, "bash", { command: "stable" }, { durationMs: 100 }));
			store.observe(input(sessionID, "read", { path: "stable.ts" }, { durationMs: 10 }));
		}
		store.observe(input(sessionID, "bash", { command: "slow-a" }, { durationMs: 900 }));
		store.observe(input(sessionID, "bash", { command: "slow-b" }, { durationMs: 1_000 }));
		store.observe(input(sessionID, "read", { path: "slow.ts" }, { durationMs: 2_000 }));

		const recurrent = store
			.predict(sessionID)
			.filter((candidate) => candidate.patternID.startsWith("action-backoff:"));
		const sampled = recurrent.filter((candidate) => candidate.background);
		expect(sampled).toHaveLength(2);
		expect(new Set(sampled.map((candidate) => candidate.tool))).toEqual(new Set(["bash", "read"]));
		const parent = recurrent[0]!;
		const next = store.continue(parent.continuation,
			input(sessionID, parent.tool, parent.input), {}, confirmed);
		expect(next.some((candidate) => candidate.patternID.startsWith("action-backoff:"))).toBe(confirmed);
	});


	test("merges exact backoff and keeps contradicted patterns from evicting contextual evidence", () => {
		const store = patternStore({ beamWidth: 2, minOccurrences: 2 }, undefined, piActionSemantics());
		const commands = [{ command: "npm test" }, { command: "npm run lint" }, { command: "slow probe" }];
		for (const [index, command] of commands.entries()) {
			acceptPattern(store, { "0": 2 }, {
				id: `contextual-bash-${index}`,
				context: [{ tool: "grep", outcome: "success" }],
				targetTool: "bash",
				bindings: { '["command"]': { type: "constant", value: command.command } },
				...(index === 2
					? {
							averageDurationMs: 10_000,
							feedback: patternFeedback({ observed: 3, recentMismatchedWeight: 3 }),
						}
					: {}),
			});
		}
		for (let index = 0; index < 2; index++) {
			store.observe(input("merged", "bash", commands[0], { durationMs: 100 }));
		}
		store.observe(input("merged", "grep", { pattern: "trigger" }));
		const matches = store.predict("merged").filter((candidate) => candidate.tool === "bash");

		expect(matches.map((candidate) => candidate.input)).toEqual(commands.slice(0, 2));
		expect(matches.some((candidate) => candidate.background)).toBe(false);
		expect(matches[0]?.supportingPatternIDs).toContain("contextual-bash-0");
		expect(matches.map((candidate) => JSON.parse(candidate.diagnostic).beamRank)).toEqual([1, 2]);
	});

	test.each([false, true])("unlocks and retains a multi-step frontier (LLM boundaries=%s)", (turnBoundaries) => {
		const store = patternStore();
		trainFrontier(store, "one", "src/a.ts", "tests/alpha.test.ts", turnBoundaries);
		trainFrontier(store, "two", "src/b.ts", "tests/beta.test.ts", turnBoundaries);
		if (turnBoundaries) store.observeTurn();

		store.observe(input("probe", "grep", {}, { outputPaths: ["src/c.ts"] }));
		const read = store.predict("probe").find((item) => item.tool === "read");
		expect(read?.depth).toBe(1);
		const captured = structuredClone(read!.continuation.history);
		store.observe(input("probe", "inspect", { later: true }, { learnTarget: false }));
		expect(read!.continuation.history).toEqual(captured);

		const lsp = store.continue(read!.continuation, input("probe", "read", { filePath: "src/c.ts" }, {
			output: { nextPath: "tests/gamma.test.ts" },
		})).find((item) => item.tool === "lsp");
		expect(lsp?.input).toEqual({ operation: "diagnostics", filePath: "tests/gamma.test.ts" });
		expect(lsp?.depth).toBe(2);

		const bash = store.continue(lsp!.continuation, input("probe", "lsp", lsp!.input, {
			output: { command: "bun test tests/gamma.test.ts" },
		})).find((item) => item.tool === "bash");
		expect(bash?.input).toEqual({ command: "bun test tests/gamma.test.ts" });
		expect(bash?.depth).toBe(3);
		expect(new Set(bash?.continuation.visitedPatternIDs).size).toBe(3);
	});

	test("does not count tool-level PPM or failed target latency when valuing concrete patterns", () => {
		const store = patternStore({ beamWidth: 1, maxContextLength: 1, maxFutureGap: 0 });
		for (let index = 0; index < 8; index++) {
			store.observe(input(`fast-${index}`, "grep", {}, { durationMs: 1 }));
			store.observe(
				input(`fast-${index}`, "read", { path: "README.md" }, { durationMs: 1 }),
			);
		}
		for (let index = 0; index < 4; index++) {
			store.observe(input(`slow-${index}`, "grep", {}, { durationMs: 1 }));
			store.observe(
				input(`slow-${index}`, "bash", { command: "npm test" }, {
					outcome: index === 0 ? "failure" : "success",
					durationMs: index === 0 ? 10_000 : 100,
				}),
			);
		}

		store.observe(input("probe", "grep"));
		const candidates = store.predict("probe");

		expect(candidates.map((candidate) => candidate.tool)).toEqual(["bash", "read"]);
		expect(candidates[0]).toMatchObject({ tool: "bash", expectedDurationMs: 75 });
	});

	test("unfolds recurrence only through distinct finite-motif contexts", () => {
		const train = (length: number, maxPredictionDepth = 6) => {
			const store = patternStore({ beamWidth: 1, maxContextLength: 3, maxFutureGap: 0, maxPredictionDepth });
			for (const sessionID of ["one", "two"]) {
				for (let depth = 0; depth < length; depth++) {
					store.observe(
						input(sessionID, "inspect", { value: `src/${sessionID}.ts${".test".repeat(depth)}` }),
					);
				}
			}
			return store;
		};
		const unfold = (store: PatternAwareStore, sessionID: string) => {
			store.observe(input(sessionID, "inspect", { value: "src/probe.ts" }));
			const candidates = [];
			let candidate = store.predict(sessionID)[0];
			while (candidate) {
				candidates.push(candidate);
				candidate = store.continue(
					candidate.continuation,
					input(sessionID, "inspect", candidate.input, { learnTarget: false }),
				)[0];
			}
			return candidates;
		};

		expect(unfold(train(2), "shallow").map((candidate) => candidate.input.value)).toEqual(["src/probe.ts.test"]);
		const motif = unfold(train(4), "motif");
		expect(motif.map((candidate) => candidate.input.value)).toEqual(
			[1, 2, 3].map((depth) => `src/probe.ts${".test".repeat(depth)}`),
		);
		expect(new Set(motif.map((candidate) => candidate.patternID)).size).toBe(3);
		expect(unfold(train(4, 2), "bounded")).toHaveLength(2);
	});

	test.each([
		[
			"structured output",
			{ output: { structured: [{ entry: { path: "src/a.ts" }, line: 3, text: "TODO" }] } },
			[],
			{ output: [{ entry: { path: "src/a.ts" }, line: 3, text: "TODO" }], outputPaths: ["src/a.ts"] },
		],
		["explicit paths", undefined, ["src/z.ts", "src/a.ts", "src/z.ts"], { outputPaths: ["src/a.ts", "src/z.ts"] }],
		[
			"metadata",
			{ metadata: { results: [{ path: "C:/repo/src/b.ts", line: 4 }] }, output: "ignored display text" },
			[],
			{ output: { results: [{ path: "C:/repo/src/b.ts", line: 4 }] }, outputPaths: ["C:/repo/src/b.ts"] },
		],
		[
			"details",
			{ content: [{ type: "text", text: "private display-only payload" }], details: { results: [{ path: "src/c.ts" }] } },
			[],
			{ output: { results: [{ path: "src/c.ts" }] }, outputPaths: ["src/c.ts"] },
		],
		[
			"display-only text",
			{ content: [{ type: "text", text: "private display-only payload" }], details: undefined },
			[],
			{},
		],
		[
			"opaque values",
			{
				content: [{ type: "text", text: "tests/value.test.ts::case\nexplanatory display text\nabc1234" }],
				details: undefined,
			},
			[],
			{ output: { values: ["tests/value.test.ts::case", "abc1234"] } },
		],
	] as const)("projects %s without parsing display text", (_name, output, paths, expected) => {
		expect(projectPatternAwareObservation(output, paths)).toEqual(expected);
	});

	test("keeps waiting through a different invocation of the target tool while the gap remains", () => {
		const store = patternStore({ maxFutureGap: 2 });
		const pattern = validatedGapPattern(
			{ "1": 10 },
			{
				id: "same-tool-gap",
				bindings: { '["filePath"]': { type: "constant", value: "src/target.ts" } },
			},
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(input("same-tool", "grep", { pattern: "TODO" }));
		store.observe(input("same-tool", "read", { filePath: "src/intermediate.ts" }));
		store.observe(input("same-tool", "read", { filePath: "src/target.ts" }));

		const after = store.snapshot().find((item) => item.id === pattern.id);
		expect(after?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		expect(after?.historicalMatches).toBe(pattern.historicalMatches + 1);
	});

	test("bounds derived state by recency and releases finished sessions", () => {
		const cache = new BoundedRecencyMap<string, number | null>(2);
		cache.set("first", null);
		cache.set("second", 2);
		expect(cache.get("first")).toBeNull();
		expect(cache.set("third", 3)).toEqual({ key: "second", value: 2 });
		expect([...cache.values()]).toEqual([null, 3]);

		const recurrent = patternStore({ maxPatterns: 2 }, undefined, piActionSemantics());
		for (const filePath of ["one", "two", "one", "three"]) recurrent.observe(input("recurrent", "read", { path: filePath }));
		expect(recurrent.predict("recurrent").map(item => item.input.path).sort()).toEqual(["one", "three"]);

		const pending = patternStore({ maxPatterns: 2, maxFutureGap: 8 });
		const bounded = acceptPattern(pending, { "5": 10 }, { bindings: collectionBindings() });
		for (const filePath of ["oldest", "middle", "newest"]) pending.observe(input("pending", "grep", {}, {
			output: { results: [{ path: filePath }] }, learnTarget: false,
		}));
		for (const [index, filePath] of ["oldest", "middle", "newest"].entries()) {
			pending.observe(input("pending", "read", { filePath }, { learnTarget: false }));
			expect(pending.snapshot().find(item => item.id === bounded.id)?.historicalMatches).toBe(bounded.historicalMatches + index);
		}

		const store = patternStore({ maxPatterns: 2 }), pattern = acceptPattern(store, { "1": 10 });
		for (const sessionID of ["first", "second"]) store.observe(input(sessionID, "grep", {}, { learnTarget: false }));
		expect(store.recent("first")).toHaveLength(1);
		store.observe(input("third", "grep", {}, { learnTarget: false }));
		expect(store.recent("second")).toHaveLength(0);
		expect(store.snapshot().find(item => item.id === pattern.id)?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		store.finishSession("first");
		expect(store.recent("first")).toHaveLength(0);
		expect(store.snapshot().find(item => item.id === pattern.id)?.historicalOpportunities).toBe(pattern.historicalOpportunities + 2);
	});

	test("validates imported binding replay independently of control confidence", () => {
		const learned = patternStore();
		trainGrepRead(learned, "one", "src/a.ts");
		trainGrepRead(learned, "two", "src/b.ts");
		const pattern = learned
			.snapshot()
			.find((item) => item.targetTool === "read" && item.context.length === 1 && item.context[0]?.tool === "grep");
		expect(pattern).toBeDefined();

		const imported = patternStore();
		expect(imported.registerValidatedPattern(pattern!)).toBe(true);
		expect(
			imported.registerValidatedPattern({
				...pattern!,
				id: `${pattern!.id}-weak`,
				historicalOpportunities: 10,
				historicalMatches: 1,
				empiricalProbability: 0.1,
			}),
		).toBe(true);
		imported.observe(input("probe", "grep", {}, { outputPaths: ["src/c.ts"] }));
		expect(imported.predict("probe").some((item) => item.tool === "read" && item.type === "tool_call")).toBe(true);
	});
});

function observeBatchTransition(
	store: PatternAwareStore,
	sessionID: string,
	targets: ReadonlyArray<{ readonly tool: string; readonly input: Record<string, unknown> }>,
) {
	store.observeBatch([input(sessionID, "inspect", { scope: "src" }, { turnID: `${sessionID}:context`, })]);
	store.observeBatch(targets.map((target) => input(sessionID, target.tool, target.input, { turnID: `${sessionID}:targets`, ...target })));
	store.finishSession(sessionID);
}

function scanBatch(sessionID: string, filePath: string, patterns: readonly [string, string], sameTool: boolean) {
	const turnID = `${sessionID}:scan`;
	return [
		input(sessionID, "grep", { pattern: patterns[0] }, { turnID, outputPaths: [filePath] }),
		input(sessionID, sameTool ? "grep" : "find", { pattern: patterns[1] },
			{ turnID, ...(sameTool ? { outputPaths: [filePath.replace("src/", "ignored/").replace(".ts", ".txt")] } : { output: { count: 1 } }) }),
	];
}

function trainGrepRead(store: PatternAwareStore, sessionID: string, filePath: string, schemaHash?: string) {
	store.observe(input(sessionID, "grep", { pattern: "TODO" }, { outputPaths: [filePath] }));
	store.observe(
		input(sessionID, "read", { filePath }, {
			output: { content: `contents of ${filePath}` },
			...(schemaHash ? { schemaHash } : {}),
		}),
	);
}

function trainOutputRead(
	store: PatternAwareStore,
	sessionID: string,
	output: Record<string, unknown>,
	filePath: string,
) {
	store.observe(input(sessionID, "grep", { pattern: "TODO" }, { output }));
	store.observe(input(sessionID, "read", { filePath }));
}

function trainJoinedRead(store: PatternAwareStore, sessionID: string, root: string, name: string) {
	store.observe(input(sessionID, "inspect", {}, { output: { root, name } }));
	store.observe(input(sessionID, "read", { filePath: `${root}/${name}` }));
}

function trainFrontier(store: PatternAwareStore, sessionID: string, sourcePath: string, testPath: string, turnBoundaries = false) {
	const events = [
		input(sessionID, "grep", {}, { outputPaths: [sourcePath] }),
		input(sessionID, "read", { filePath: sourcePath }, {
			output: { nextPath: testPath },
		}),
		input(sessionID, "lsp", { operation: "diagnostics", filePath: testPath }, {
			output: { command: `bun test ${testPath}` },
		}),
		input(sessionID, "bash", { command: `bun test ${testPath}` }),
	];
	for (const [index, event] of events.entries()) {
		if (turnBoundaries) {
			store.observeTurn();
			if (index > 0) store.observeTurn();
		}
		store.observe(event);
	}
}

function trainResultReads(
	store: PatternAwareStore,
	sessionID: string,
	results: ReadonlyArray<string>,
	reads: ReadonlyArray<string>,
) {
	store.observe(
		input(sessionID, "grep", { pattern: "symbol" }, {
			output: { results: results.map((filePath) => ({ path: filePath })) },
		}),
	);
	for (const filePath of reads) store.observe(input(sessionID, "read", { filePath }));
}

function patternStore(
	overrides: Parameters<typeof settings>[0] = {},
	file?: string,
	semantics?: ConstructorParameters<typeof PatternAwareStore>[2],
) {
	return new PatternAwareStore(settings(overrides), file, semantics);
}

function settings(overrides: Partial<typeof PATTERN_AWARE_DEFAULTS> = {}) {
	return { ...PATTERN_AWARE_DEFAULTS, minOccurrences: 2, ...overrides };
}

async function patternFile(): Promise<string> {
	return path.join(await directories.create(), "patterns.json");
}

function piActionSemantics() {
	return patternAwareActionSemantics(PI_ACTION_SEMANTICS, "/workspace", [READ_RANGE_ACTION_KEY_PROJECTOR]);
}

type ValidatedPattern = Parameters<PatternAwareStore["registerValidatedPattern"]>[0];

function validatedGapPattern(
	gapCounts: Readonly<Record<string, number>>,
	overrides: Partial<ValidatedPattern> = {},
): ValidatedPattern {
	return {
		id: "gap-pattern",
		context: [{ tool: "grep", outcome: "success" }],
		targetTool: "read",
		bindings: { '["path"]': { type: "constant", value: "README.md" } },
		gapCounts,
		gapLastSeen: Object.fromEntries(Object.keys(gapCounts).map((gap) => [gap, 1])),
		occurrences: 10,
		replayMatches: 10,
		historicalOpportunities: 10,
		historicalMatches: 10,
		empiricalProbability: 1,
		adoptionProbability: 1,
		feedback: patternFeedback(),
		averageDurationMs: 100,
		lastSeenSequence: 1,
		...overrides,
	};
}

function acceptPattern(
	store: PatternAwareStore,
	gapCounts: Readonly<Record<string, number>>,
	overrides: Partial<ValidatedPattern> = {},
): ValidatedPattern {
	const pattern = validatedGapPattern(gapCounts, overrides);
	expect(store.registerValidatedPattern(pattern)).toBe(true);
	return pattern;
}

function constantBindings(input: Readonly<Record<string, unknown>>): ValidatedPattern["bindings"] {
	return Object.fromEntries(
		Object.entries(input).map(([field, value]) => [JSON.stringify([field]), { type: "constant", value }]),
	);
}

function collectionBindings(
	variantCounts?: Readonly<Record<string, number>>,
	field = "filePath",
): ValidatedPattern["bindings"] {
	return {
		[JSON.stringify([field])]: {
			type: "each",
			relativeEvent: -1,
			field: "output",
			path: ["results"],
			itemPath: ["path"],
			...(variantCounts ? { variantCounts } : {}),
		},
	};
}

function patternFeedback(
	overrides: Partial<ValidatedPattern["feedback"]> = {},
): ValidatedPattern["feedback"] {
	return {
		issued: 0,
		observed: 0,
		matched: 0,
		adopted: 0,
		rejectedAfterMatch: {},
		unobserved: {},
		recentMatchedWeight: 0,
		recentMismatchedWeight: 0,
		recentAdoptedWeight: 0,
		recentRejectedWeight: 0,
		sequence: 1,
		...overrides,
	};
}

function input(
	sessionID: string,
	tool: string,
	concrete: Record<string, unknown> = {},
	overrides: Partial<Parameters<PatternAwareStore["observe"]>[0]> = {},
) {
	return {
		sessionID,
		tool,
		input: concrete,
		turnID: `${sessionID}:turn`,
		outcome: "success" as const,
		durationMs: 10,
		...overrides,
	};
}

function event(...args: Parameters<typeof input>) {
	return { ...input(...args), sequence: 1 };
}
