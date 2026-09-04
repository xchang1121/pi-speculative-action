import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, test } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { buildPiActionKey } from "../src/action-semantics.ts";
import { BoundedRecencyMap } from "../src/bounded-recency-map.ts";
import {
	acquirePatternAwareStore,
	applyBindings,
	inferBindings,
	PATTERN_AWARE_DEFAULTS,
	PatternAwareStore,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "../src/pattern-aware.ts";
import { patternSessionBudgets, PatternSessionRegistry } from "../src/pattern-session-state.ts";
import type { PredictionSettlement, ResolutionStage } from "../src/settlement.ts";

const temporary: string[] = [];

afterEach(async () => {
	await Promise.all(temporary.splice(0).map((item) => fs.rm(item, { recursive: true, force: true })));
});

describe("PatternAware", () => {
	test("late-binds a target input from authoritative structured output paths", () => {
		const context = [
			event({
				sessionID: "one",
				tool: "grep",
				input: { pattern: "TODO" },
				outputPaths: ["src/a.ts"],
			}),
		];
		const bindings = inferBindings(context, { filePath: "src/a.ts", offset: 1 });

		expect(applyBindings(bindings, context)).toEqual({ filePath: "src/a.ts", offset: 1 });
		expect(bindings['["filePath"]']).toEqual({
			type: "event",
			relativeEvent: -1,
			field: "outputPaths",
			path: [0],
		});
		expect(bindings['["offset"]']).toEqual({ type: "constant", value: 1 });
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
				event({ sessionID: "join", tool: "grep", input: { path: root }, outputPaths: [output] }),
			]);

		expect(replay("src/file.ts", "file.ts")).toEqual({ path: "src/file.ts" });
		expect(replay("src", "src/file.ts")).toEqual({ path: "src/file.ts" });
		expect(replay("src", "nested/file.ts")).toEqual({ path: "src/nested/file.ts" });
	});

	test("merges bindings that share nested object and array paths", () => {
		const context = [event({ sessionID: "one", tool: "seed", input: { oldText: "before" } })];
		const target = {
			range: { start: 1, end: 2 },
			edits: [{ oldText: "before", newText: "after" }],
		};

		expect(applyBindings(inferBindings(context, target), context)).toEqual(target);
	});

	test("derives adjacent paths and commands through bounded path templates", () => {
		const context = [event({ sessionID: "one", tool: "read", input: { filePath: "services/alpha/config.ts" } })];
		const bindings = inferBindings(context, {
			command: "bun test services/alpha/config.test.ts",
			workdir: "services/alpha",
		});

		expect(applyBindings(bindings, context)).toEqual({
			command: "bun test services/alpha/config.test.ts",
			workdir: "services/alpha",
		});
		let inputReads = 0;
		const nextEvent = event({ sessionID: "two", tool: "read", input: { filePath: "services/beta/config.ts" } });
		const next = [
			new Proxy(nextEvent, {
				get(target, property, receiver) {
					if (property === "input") inputReads++;
					return Reflect.get(target, property, receiver);
				},
			}),
		];
		expect(applyBindings(bindings, next)).toEqual({
			command: "bun test services/beta/config.test.ts",
			workdir: "services/beta",
		});
		const readsAfterFirstApplication = inputReads;
		expect(readsAfterFirstApplication).toBeGreaterThan(0);
		expect(applyBindings(bindings, next)).toEqual({
			command: "bun test services/beta/config.test.ts",
			workdir: "services/beta",
		});
		expect(inputReads).toBe(readsAfterFirstApplication);
		expect(
			inferBindings([event({ sessionID: "path", tool: "read", input: { filePath: "/workspace/repo" } })], {
				filePath: "repo/src/a.ts",
			})['["filePath"]'],
		).toEqual({ type: "constant", value: "repo/src/a.ts" });
	});

	test("does not treat an opaque shell command as a filesystem path template", () => {
		const context = [
			event({
				sessionID: "one",
				tool: "bash",
				input: { command: '& "C:\\Users\\dev\\.bun\\bin\\bun.exe" test services/delta/config.test.ts' },
			}),
		];
		const target = { command: '& "C:\\Users\\dev\\.bun\\bin\\bun.exe" test services/epsilon/config.test.ts' };
		const bindings = inferBindings(context, target);

		expect(bindings['["command"]']).toEqual({ type: "constant", value: target.command });
		expect(
			applyBindings(bindings, [
				event({ sessionID: "two", tool: "bash", input: { command: "bun test services/gamma/config.test.ts" } }),
			]),
		).toEqual(target);
	});

	test("interpolates one non-path value without inventing case, multi-source, short, or path semantics", () => {
		const context = [
			event({ sessionID: "one", tool: "inspect", input: { left: "Alpha", right: "Beta", short: "xy" } }),
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
				event({ sessionID: "two", tool: "inspect", input: { left: "Gamma", right: "Delta", short: "zz" } }),
			]),
		).toEqual({ ...target, command: "run Gamma now", joined: "Gamma:Beta" });
	});

	test("rebases predictions over an authoritative provider batch without learning it early", () => {
		const store = new PatternAwareStore(settings());
		trainGrepRead(store, "one", "src/a.ts");
		trainGrepRead(store, "two", "src/b.ts");
		const before = store.recent("probe");

		const candidates = store.predictAfterBatch("probe", [
			input({
				sessionID: "probe",
				turnID: "probe:scan",
				tool: "grep",
				input: { pattern: "TODO" },
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
	});

	test("learns provider batches canonically without inventing sibling causality", () => {
		const store = new PatternAwareStore(settings());
		for (const [sessionID, filePath, reverse] of [
			["one", "src/a.ts", false],
			["two", "src/b.ts", true],
		] as const) {
			const batch = scanBatch(sessionID, filePath);
			store.observeBatch(reverse ? [...batch].reverse() : batch);
			store.observeBatch([input({ sessionID, turnID: `${sessionID}:read`, tool: "read", input: { filePath } })]);
			store.finishSession(sessionID);
		}

		store.observeBatch([...scanBatch("probe", "src/c.ts")].reverse());
		const candidate = store.predict("probe").find((item) => item.tool === "read");

		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
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
		const store = new PatternAwareStore(settings({ maxContextLength: 1, maxFutureGap: 0 }));
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
				input({ sessionID, turnID: `${sessionID}:context`, tool: "inspect", input: { scope: "src" } }),
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
		const store = new PatternAwareStore(settings({ maxContextLength: 1, maxFutureGap: 0 }));
		for (let index = 0; index < 16; index++)
			observeBatchTransition(
				store,
				`same-tool-${index}`,
				["one.ts", "two.ts"].map((filePath) => ({ tool: "read", input: { filePath } })),
			);
		store.observeBatch([input({ sessionID: "probe", turnID: "probe:context", tool: "inspect", input: {} })]);
		const reads = store.predict("probe").filter((candidate) => candidate.tool === "read");
		expect(reads).toHaveLength(2);
		expect(reads.every((candidate) => candidate.conditionalProbability > 0.9)).toBe(true);
	});

	test("learns mappers per gap and merges equivalent actions only at prediction", () => {
		const store = new PatternAwareStore(
			settings({ maxContextLength: 1, maxFutureGap: 1, minOccurrences: 2, futureGapCoverage: 0.9 }),
		);
		for (const [sessionID, filePath] of [
			["immediate-a", "src/a.ts"],
			["immediate-b", "src/b.ts"],
		] as const) {
			store.observe(input({ sessionID, tool: "grep", input: {}, outputPaths: [filePath] }));
			store.observe(input({ sessionID, tool: "read", input: { filePath } }));
			store.finishSession(sessionID);
		}
		for (const [sessionID, filePath] of [
			["delayed-a", "src/c.ts"],
			["delayed-b", "src/d.ts"],
		] as const) {
			store.observe(input({ sessionID, tool: "grep", input: {}, outputPaths: [filePath] }));
			store.observe(input({ sessionID, tool: "bash", input: { command: "pwd" } }));
			store.observe(input({ sessionID, tool: "read", input: { filePath } }));
			store.finishSession(sessionID);
		}

		const patterns = store
			.snapshot()
			.filter((item) => item.targetTool === "read" && item.context.length === 1 && item.context[0]?.tool === "grep");
		expect(patterns.map((pattern) => pattern.gapCounts)).toEqual([{ "0": 2 }, { "1": 2 }]);

		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");
		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
		expect(candidate).toMatchObject({ horizon: 1, latestHorizon: 1 });
	});

	test("separates eventual probability from weighted gap timing and retains the observed deadline", () => {
		const gapSettings = settings({ maxFutureGap: 8, futureGapCoverage: 0.8 });
		const store = new PatternAwareStore(gapSettings);
		const immediate = new PatternAwareStore(gapSettings);
		const pattern = acceptPattern(store, { "0": 9, "5": 1 });
		acceptPattern(immediate, { "0": 10 });

		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));
		immediate.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));

		const candidate = store.predict("probe").find((item) => item.tool === "read");
		const immediateCandidate = immediate.predict("probe").find((item) => item.tool === "read");
		expect(candidate).toMatchObject({
			horizon: 0,
			latestHorizon: 5,
		});
		expect(immediateCandidate).toMatchObject({ horizon: 0, latestHorizon: 0 });
		expect(candidate?.conditionalProbability).toBe(immediateCandidate?.conditionalProbability);
		for (let gap = 0; gap < 5; gap++) {
			store.observe(input({ sessionID: "probe", tool: "bash", input: { command: `step-${gap}` } }));
		}
		store.observe(input({ sessionID: "probe", tool: "read", input: { path: "README.md" } }));
		expect(store.snapshot().find((item) => item.id === pattern.id)).toMatchObject({
			historicalOpportunities: pattern.historicalOpportunities + 1,
			historicalMatches: pattern.historicalMatches + 1,
		});
	});

	test("derives orthogonal feedback only from authoritative prediction settlements", () => {
		const store = new PatternAwareStore(settings({ minOccurrences: 2, decayHalfLifeEvents: 1 }));
		acceptPattern(store, { "0": 10 }, { id: "attributed" });
		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));
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
			store.observe(input({ sessionID: `decay-${index}`, tool: "lsp", input: { operation: "symbols" } }));
		expect(
			store.predict("probe").find((item) => item.patternID === "attributed")!.adoptionProbability,
		).toBeGreaterThan(pattern!.adoptionProbability);
		const afterObservedMiss = store.predict("probe").find((item) => item.patternID === "attributed");
		expect(afterObservedMiss!.empiricalProbability).toBeLessThan(beforeUnobserved!.empiricalProbability);
	});

	test("discounts old mismatch evidence so fresh matches recover after drift", () => {
		const store = new PatternAwareStore(settings({ minOccurrences: 2, decayHalfLifeEvents: 2 }));
		acceptPattern(store, { "0": 10 }, { id: "drift" });
		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));
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
		const store = new PatternAwareStore(
			settings({
				maxFutureGap: 8,
				futureGapCoverage: 0.9,
				decayHalfLifeEvents: 10,
			}),
		);
		acceptPattern(store, { "0": 1000, "3": 10 }, {
			gapLastSeen: { "0": 0, "3": 1000 },
			lastSeenSequence: 1000,
			occurrences: 1010,
			replayMatches: 1010,
			historicalOpportunities: 1010,
			historicalMatches: 1010,
		});

		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));

		expect(store.predict("probe").find((item) => item.tool === "read")?.horizon).toBe(3);
	});

	test("does not promote multiple gap views of one target into repeated support", () => {
		const store = new PatternAwareStore(settings({ maxContextLength: 1, maxFutureGap: 1, minOccurrences: 2 }));
		store.observe(input({ sessionID: "one", tool: "grep", input: { pattern: "a" }, outputPaths: ["src"] }));
		store.observe(input({ sessionID: "one", tool: "grep", input: { pattern: "b" }, outputPaths: ["src/a.ts"] }));
		store.observe(input({ sessionID: "one", tool: "read", input: { filePath: "src/a.ts" } }));
		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" }, outputPaths: ["src/b.ts"] }));

		expect(store.snapshot().filter((item) => item.targetTool === "read")).toEqual([
			expect.objectContaining({ occurrences: 1, gapCounts: { "0": 1 } }),
		]);
		const candidate = store.predict("probe").find((item) => item.tool === "read")!;
		store.issued(candidate.patternID);
		expect(store.predict("probe").filter((item) => item.tool === "read")).toHaveLength(0);
	});

	test("continues only schema-compatible learned targets", () => {
		const store = new PatternAwareStore(settings());
		trainGrepRead(store, "one", "src/a.ts", "read-v1");
		trainGrepRead(store, "two", "src/b.ts", "read-v1");
		store.observe(input({ sessionID: "three", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));

		expect(store.predict("three", { read: "read-v2" }).filter((item) => item.tool === "read")).toHaveLength(0);
		expect(store.predict("three", { read: "read-v1" })).toContainEqual(
			expect.objectContaining({
				type: "tool_call",
				source: "pattern_aware",
				tool: "read",
				input: expect.objectContaining({ filePath: "src/c.ts" }),
			}),
		);
	});

	test("persists a deduplicated learning table and rebuilds its opportunity index", async () => {
		const file = await patternFile("learning-table");
		const first = new PatternAwareStore(settings(), file);
		await first.load();
		trainGrepRead(first, "one", "src/a.ts");
		trainGrepRead(first, "two", "src/b.ts");
		await first.flush();

		const raw = await fs.readFile(file, "utf8");
		expect(raw).not.toContain('"history"');
		const persisted = JSON.parse(raw);
		expect(persisted.version).toBe(18);
		expect(persisted.events.length).toBeGreaterThan(0);
		expect(
			persisted.pools.every((pool: { samples: Array<{ context: number[]; target: number }> }) =>
				pool.samples.every(
					(sample) => sample.context.every((event) => Number.isInteger(event)) && Number.isInteger(sample.target),
				),
			),
		).toBe(true);
		const second = new PatternAwareStore(settings(), file);
		await second.load();
		second.observe(input({ sessionID: "three", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));

		expect(second.predict("three").some((item) => item.tool === "read")).toBe(true);
		second.observe(input({ sessionID: "three", tool: "read", input: { filePath: "src/c.ts" } }));
		expect(second.snapshot().find((item) => item.targetTool === "read")?.historicalOpportunities).toBe(3);
	});

	test("skips malformed persisted contexts, binding paths, and binding nodes", async () => {
		const file = await patternFile("corrupt-state");
		const valid = validatedGapPattern({ "0": 10 }, { id: "valid-persisted-pattern" });
		await fs.writeFile(
			file,
			JSON.stringify({
				version: 17,
				patterns: [
					valid,
					{ ...valid, id: "bad-context", context: [{ tool: 7, outcome: "success" }] },
					{ ...valid, id: "bad-target-path", bindings: { "not-json": { type: "constant", value: "x" } } },
					{
						...valid,
						id: "bad-binding",
						bindings: {
							'["filePath"]': { type: "event", relativeEvent: -1, field: "output", path: "not-an-array" },
						},
					},
				],
				pools: [],
				sequenceCounts: [],
			}),
		);

		const store = new PatternAwareStore(settings(), file);
		await expect(store.load()).resolves.toBeUndefined();

		expect(store.snapshot().map((pattern) => pattern.id)).toEqual(["valid-persisted-pattern"]);
	});

	test("rejects indexed pools that reference a missing or malformed event", async () => {
		const file = await patternFile("corrupt-index");
		await fs.writeFile(
			file,
			JSON.stringify({
				version: 17,
				patterns: [],
				events: [event({ sessionID: "one", tool: "grep", input: {} }), { sequence: 2 }],
				pools: [
					{
						key: "bad-reference",
						context: [{ tool: "grep", outcome: "success" }],
						targetTool: "read",
						samples: [{ context: [0], target: 1, gap: 0 }],
					},
				],
				sequenceCounts: [],
			}),
		);

		const store = new PatternAwareStore(settings({ minOccurrences: 1 }), file);
		await expect(store.load()).resolves.toBeUndefined();
		expect(store.snapshot()).toEqual([]);
	});

	test("shares analyzer state across predictor-only settings and isolates analyzer configurations", async () => {
		const workspace = await fs.mkdtemp(path.join(os.tmpdir(), "pi-pattern-lease-"));
		temporary.push(workspace);
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

		expect(second.store).toBe(first.store);
		expect(predictorOnly.store).toBe(first.store);
		expect(differentAnalyzer.store).not.toBe(first.store);
		await first.release();
		const third = await acquirePatternAwareStore(workspace, settings());
		expect(third.store).toBe(second.store);

		await second.release();
		await predictorOnly.release();
		await differentAnalyzer.release();
		await third.release();
		const next = await acquirePatternAwareStore(workspace, settings());
		expect(next.store).not.toBe(first.store);
		await next.release();
	});

	test("discards persisted patterns from a different schema version", async () => {
		const file = await patternFile("schema-version");
		await fs.writeFile(
			file,
			JSON.stringify({
				version: 12,
				patterns: [validatedGapPattern({ "0": 10 })],
				pools: [],
				sequenceCounts: [],
			}),
		);

		const store = new PatternAwareStore(settings(), file);
		await store.load();

		expect(store.snapshot()).toEqual([]);
	});

	test("enforces the configured context bound while learning, restoring, and registering patterns", async () => {
		const file = await patternFile("context-bound");
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
			JSON.stringify({ version: 18, patterns: [long], events: [], pools: [], sequenceCounts: [] }),
		);

		const store = new PatternAwareStore(settings({ maxContextLength: 1 }), file);
		await store.load();
		expect(store.snapshot()).toEqual([]);
		expect(store.registerValidatedPattern(long)).toBe(false);
		for (const sessionID of ["learn-a", "learn-b"]) {
			store.observeBatch([
				input({ sessionID, tool: "grep", input: { pattern: "TODO" } }),
				input({ sessionID, tool: "read", input: { filePath: "src/a.ts" } }),
			]);
			store.observe(input({ sessionID, tool: "write", input: { filePath: "src/a.ts" } }));
			store.finishSession(sessionID);
		}
		expect(store.snapshot()).toEqual([]);
	});

	test.each([13, 14])("migrates v%s evidence by gap without loading its mixed mapper patterns", async (version) => {
		const file = await patternFile("gap-migration");
		const sample = (sessionID: string, filePath: string, gap: number) => ({
			context: [event({ sessionID, tool: "grep", input: {}, outputPaths: [filePath] })],
			target: event({ sessionID, tool: "read", input: { filePath } }),
			gap,
		});
		await fs.writeFile(
			file,
			JSON.stringify({
				version,
				patterns: [validatedGapPattern({ "0": 10 }, { id: "stale-legacy" })],
				pools: [
					{
						key: "legacy-mixed-gap",
						context: [{ tool: "grep", outcome: "success" }],
						targetTool: "read",
						samples: [sample("immediate", "src/a.ts", 0), sample("delayed", "src/b.ts", 1)],
						inferred: { '["filePath"]': { type: "constant", value: "src/c.ts" } },
						observations: 2,
						nextInferenceAt: 100,
					},
				],
				sequenceCounts: [],
			}),
		);

		const store = new PatternAwareStore(settings(), file);
		await store.load();
		expect(store.snapshot()).toEqual([]);
		trainGrepRead(store, "fresh", "src/c.ts");

		expect(store.snapshot()).toEqual([
			expect.objectContaining({ targetTool: "read", occurrences: 2, gapCounts: { "0": 2 } }),
		]);
	});

	test("transfers data-flow patterns across processes before global support", async () => {
		const file = await patternFile("pool");
		const first = new PatternAwareStore(settings({ minOccurrences: 2 }), file);
		await first.load();
		trainGrepRead(first, "one", "src/a.ts");
		first.finishSession("one");
		await first.flush();

		const persisted = JSON.parse(await fs.readFile(file, "utf8"));
		expect(persisted.patterns).toEqual([expect.objectContaining({ targetTool: "read", occurrences: 1 })]);
		expect(persisted.pools.length).toBeGreaterThan(0);

		const second = new PatternAwareStore(settings({ minOccurrences: 2 }), file);
		await second.load();
		second.observe(input({ sessionID: "two", tool: "grep", input: {}, outputPaths: ["src/b.ts"] }));
		const candidates = second.predict("two");
		expect(candidates).toContainEqual(expect.objectContaining({ tool: "read", input: { filePath: "src/b.ts" } }));
		second.observe(input({ sessionID: "two", tool: "read", input: { filePath: "src/b.ts" } }));

		expect(second.snapshot().some((item) => item.targetTool === "read")).toBe(true);
	});

	test("persists PPM counts so beam ordering survives a process restart", async () => {
		const file = await patternFile("ppm");
		const configured = settings({ beamWidth: 1 });
		const first = new PatternAwareStore(configured, file);
		await first.load();
		for (let index = 0; index < 4; index++) {
			first.observe(input({ sessionID: `read-${index}`, tool: "grep", input: {} }));
			first.observe(input({ sessionID: `read-${index}`, tool: "read", input: { path: "README.md" } }));
		}
		for (let index = 0; index < 2; index++) {
			first.observe(input({ sessionID: `bash-${index}`, tool: "grep", input: {} }));
			first.observe(input({ sessionID: `bash-${index}`, tool: "bash", input: { command: "npm test" } }));
		}
		await first.flush();

		const persisted = JSON.parse(await fs.readFile(file, "utf8"));
		expect(persisted.version).toBe(18);
		expect(persisted.sequenceCounts.length).toBeGreaterThan(0);
		const restored = new PatternAwareStore(configured, file);
		await restored.load();
		restored.observe(input({ sessionID: "probe", tool: "grep", input: {} }));

		expect(restored.predict("probe")).toEqual([
			expect.objectContaining({ tool: "read", input: { path: "README.md" } }),
		]);
	});

	test("keeps constant patterns task-local until independently supported", async () => {
		const local = new PatternAwareStore(settings({ minOccurrences: 2 }));
		local.observe(input({ sessionID: "local", tool: "inspect", input: {} }));
		local.observe(input({ sessionID: "local", tool: "read", input: { filePath: "README.md" } }));
		local.observe(input({ sessionID: "local", tool: "inspect", input: {} }));
		expect(local.predict("local")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { filePath: "README.md" } }),
		);

		const file = await patternFile("constant-pool");
		const train = async (sessionID: string) => {
			const store = new PatternAwareStore(settings({ minOccurrences: 2 }), file);
			await store.load();
			store.observe(input({ sessionID, tool: "inspect", input: {}, output: { kind: "path" } }));
			store.observe(input({ sessionID, tool: "read", input: { filePath: "README.md" } }));
			store.finishSession(sessionID);
			await store.flush();
		};

		await train("one");
		const isolated = new PatternAwareStore(settings({ minOccurrences: 2 }), file);
		await isolated.load();
		isolated.observe(input({ sessionID: "probe", tool: "inspect", input: {}, output: { kind: "path" } }));
		expect(isolated.predict("probe").some((candidate) => candidate.tool === "read")).toBe(false);
		isolated.finishSession("probe");
		await isolated.flush();
		for (const sessionID of ["two", "three", "four"]) {
			await train(sessionID);
		}

		const restored = new PatternAwareStore(settings({ minOccurrences: 2 }), file);
		await restored.load();
		restored.observe(input({ sessionID: "probe", tool: "inspect", input: {}, output: { kind: "path" } }));
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
		const store = new PatternAwareStore(settings({ minOccurrences: 3 }));
		trainGrepRead(store, "one", "src/a.ts");
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 1,
			feedback: { issued: 0 },
		});

		store.observe(input({ sessionID: "two", tool: "grep", input: {}, outputPaths: ["src/b.ts"] }));
		const candidate = store.predict("two").find((item) => item.tool === "read")!;
		expect(candidate.input).toEqual({ filePath: "src/b.ts" });
		expect(candidate.background).toBe(true);
		store.issued(candidate.patternID);
		store.settled(candidate.patternID, adoptedSettlement());
		expect(store.predict("two").filter((item) => item.tool === "read")).toHaveLength(0);

		store.observe(input({ sessionID: "two", tool: "read", input: { filePath: "src/b.ts" } }));
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 2,
			feedback: { issued: 1, matched: 1, adopted: 1 },
		});
		store.observe(input({ sessionID: "three", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		expect(store.predict("three")).not.toContainEqual(expect.objectContaining({ tool: "read" }));
		store.observe(input({ sessionID: "three", tool: "read", input: { filePath: "src/c.ts" } }));
		store.observe(input({ sessionID: "four", tool: "grep", input: {}, outputPaths: ["src/d.ts"] }));
		const promoted = store.predict("four");
		expect(promoted).toContainEqual(expect.objectContaining({ tool: "read", input: { filePath: "src/d.ts" } }));
		expect(promoted.find((item) => item.tool === "read")?.background).toBeUndefined();
		expect(store.snapshot().find((item) => item.targetTool === "read")).toMatchObject({
			occurrences: 3,
			feedback: { issued: 1 },
		});
	});

	test("emits weak control-flow candidates for bounded utility admission", () => {
		const store = new PatternAwareStore(settings({ minBindingReplayProbability: 0.75 }));
		trainGrepRead(store, "one", "src/a.ts");
		trainGrepRead(store, "two", "src/b.ts");

		store.observe(input({ sessionID: "miss-one", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		store.finishSession("miss-one");
		store.observe(input({ sessionID: "miss-two", tool: "grep", input: {}, outputPaths: ["src/d.ts"] }));
		store.finishSession("miss-two");

		const pattern = store
			.snapshot()
			.find((item) => item.targetTool === "read" && item.context.at(-1)?.tool === "grep");
		expect(pattern?.historicalMatches).toBe(2);
		expect(pattern?.historicalOpportunities).toBe(4);
		expect(pattern?.empiricalProbability).toBe(0.5);

		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, outputPaths: ["src/e.ts"] }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");
		expect(candidate?.empiricalProbability).toBeGreaterThan(0);
		expect(candidate?.empiricalProbability).toBeLessThan(0.75);
	});

	test("still rejects unreliable argument mappers", () => {
		const store = new PatternAwareStore(settings({ minBindingReplayProbability: 0.75 }));
		expect(
			store.registerValidatedPattern(
				validatedGapPattern({ "0": 10 }, { id: "unreliable-binding", occurrences: 10, replayMatches: 7 }),
			),
		).toBe(false);
	});

	test("does not predict an action before all bound payloads are available", () => {
		const store = new PatternAwareStore(settings());
		trainGrepRead(store, "one", "src/a.ts");
		trainGrepRead(store, "two", "src/b.ts");

		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "TODO" } }));
		expect(store.predict("probe").find((item) => item.tool === "read")).toBeUndefined();
	});

	test("learns indexed field fallbacks across historical samples", () => {
		const store = new PatternAwareStore(settings());
		trainOutputRead(store, "one", { primary: "src/a.ts" }, "src/a.ts");
		trainOutputRead(store, "two", { fallback: "src/b.ts" }, "src/b.ts");

		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, output: { fallback: "src/c.ts" } }));
		const candidate = store.predict("probe").find((item) => item.tool === "read");

		expect(candidate?.type).toBe("tool_call");
		expect(candidate?.input).toEqual({ filePath: "src/c.ts" });
	});

	test("learns a stable mapper branch after unrelated evidence", () => {
		const store = new PatternAwareStore(settings({ maxContextLength: 1, maxFutureGap: 0 }));
		for (let index = 0; index < 4; index++) {
			trainOutputRead(store, `noise-${index}`, { path: `src/source-${index}.ts` }, `src/unrelated-${index}.ts`);
		}
		for (let index = 0; index < 2; index++) {
			const file = `src/stable-${index}.ts`;
			trainOutputRead(store, `stable-${index}`, { path: file }, file);
		}

		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, output: { path: "src/result.ts" } }));
		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			filePath: "src/result.ts",
		});
	});

	test("combines multiple structured fields instead of memorizing a concrete path", () => {
		const store = new PatternAwareStore(settings());
		trainJoinedRead(store, "one", "services/a", "alpha");
		trainJoinedRead(store, "two", "services/b", "beta");

		store.observe(
			input({
				sessionID: "probe",
				tool: "inspect",
				input: {},
				output: { root: "services/c", name: "gamma" },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			filePath: "services/c/gamma",
		});
	});

	test("does not compose presentation text into a path binding", () => {
		const store = new PatternAwareStore(settings());
		for (const [sessionID, root, preview] of [
			["one", "services/a", "alpha.ts"],
			["two", "services/b", "beta.ts"],
		]) {
			store.observe(
				input({
					sessionID,
					tool: "read",
					input: { filePath: root },
					output: { preview },
				}),
			);
			store.observe(input({ sessionID, tool: "read", input: { filePath: `${root}/${preview}` } }));
			store.finishSession(sessionID);
		}

		store.observe(
			input({
				sessionID: "probe",
				tool: "read",
				input: { filePath: "services/c" },
				output: { preview: "export const value = 1" },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")).toBeUndefined();
	});

	test("matches learned contexts by semantic output kind", () => {
		const store = new PatternAwareStore(settings());
		for (const sessionID of ["one", "two"]) {
			store.observe(
				input({
					sessionID,
					tool: "inspect",
					input: { path: `src/${sessionID}.ts` },
					output: { kind: "path", value: `src/${sessionID}.test.ts` },
				}),
			);
			store.observe(input({ sessionID, tool: "read", input: { filePath: "README.md" } }));
			store.finishSession(sessionID);
		}

		store.observe(
			input({
				sessionID: "wrong-output",
				tool: "inspect",
				input: { path: "src/value.ts" },
				output: { kind: "text", value: "src/value.test.ts" },
			}),
		);
		expect(store.predict("wrong-output").find((item) => item.tool === "read")).toBeUndefined();
	});

	test("expands a structured collection when actor ordering varies", () => {
		const store = new PatternAwareStore(settings());
		trainResultReads(store, "one", ["src/a.ts", "src/b.ts"], ["src/b.ts", "src/a.ts"]);
		trainResultReads(store, "two", ["src/c.ts", "src/d.ts"], ["src/c.ts", "src/d.ts"]);

		store.observe(
			input({
				sessionID: "probe",
				tool: "grep",
				input: { pattern: "symbol" },
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
		const store = new PatternAwareStore(settings());
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

		store.observe(input({ sessionID: "probe", tool: "grep", input: { pattern: "source" } }));
		const source = store.predict("probe").find((item) => item.input.path === "src/source.ts");
		expect(source?.empiricalProbability).toBeGreaterThan(0.9);

		const child = store
			.continue(source!.continuation, input({ sessionID: "probe", tool: "read", input: { path: "src/source.ts" } }))
			.find((item) => item.tool === "bash");
		expect(child?.conditionalProbability).toBeGreaterThan(0.9);
		expect(child?.empiricalProbability).toBeGreaterThan(0.8);
	});

	test("allocates collection variants by observed actor choice frequency", () => {
		const store = new PatternAwareStore(settings());
		acceptPattern(store, { "0": 10 }, {
			id: "ranked-results",
			bindings: collectionBindings({ "0": 9, "1": 1 }),
		});

		store.observe(
			input({
				sessionID: "probe",
				tool: "grep",
				input: { pattern: "source" },
				output: { results: [{ path: "src/likely.ts" }, { path: "src/unlikely.ts" }] },
			}),
		);
		const candidates = store.predict("probe").filter((item) => item.tool === "read");
		const likely = candidates.find((item) => item.input.filePath === "src/likely.ts");
		const unlikely = candidates.find((item) => item.input.filePath === "src/unlikely.ts");

		expect(likely?.conditionalProbability).toBeGreaterThan(unlikely?.conditionalProbability ?? 1);
		expect(candidates.reduce((sum, item) => sum + item.conditionalProbability, 0)).toBeLessThanOrEqual(1);
	});

	test("charges evidence-annealed mapper complexity for transforms and ungrounded payloads", () => {
		const store = new PatternAwareStore(settings({ beamWidth: 1, maxContextLength: 1, maxFutureGap: 0 }));
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

		store.observe(input({ sessionID: "probe", tool: "grep", input: { path: "src/index.ts" } }));
		expect(store.predict("probe")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { path: "src/index.ts" } }),
		);
	});

	test("retains multiple replayable mapper branches for one control context", () => {
		const store = new PatternAwareStore(settings());
		for (const [sessionID, source, target] of [
			["same-a", "src/a.ts", "src/a.ts"],
			["same-b", "src/b.ts", "src/b.ts"],
			["test-a", "src/c.ts", "src/c.ts.test"],
			["test-b", "src/d.ts", "src/d.ts.test"],
		] as const) {
			store.observe(input({ sessionID, tool: "inspect", input: { value: source } }));
			store.observe(input({ sessionID, tool: "inspect", input: { value: target } }));
			store.finishSession(sessionID);
		}

		store.observe(input({ sessionID: "probe", tool: "inspect", input: { value: "src/e.ts" } }));
		expect(store.predict("probe").map((candidate) => candidate.input)).toEqual(
			expect.arrayContaining([{ value: "src/e.ts" }, { value: "src/e.ts.test" }]),
		);
	});

	test("contains non-finite persisted variant counts instead of emitting invalid probabilities", async () => {
		const file = await patternFile("invalid-variants");
		const first = new PatternAwareStore(settings(), file);
		await first.load();
		acceptPattern(first, { "0": 10 }, {
			id: "invalid-ranked-results",
			bindings: collectionBindings({ "0": Number.NaN, "1": Number.POSITIVE_INFINITY }),
		});
		await first.flush();
		const store = new PatternAwareStore(settings(), file);
		await store.load();
		store.observe(
			input({
				sessionID: "probe-invalid-counts",
				tool: "grep",
				input: {},
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
		const store = new PatternAwareStore(settings());
		for (const sessionID of ["one", "two", "three"]) {
			store.observe(input({ sessionID, tool: "inspect", input: {}, output: { kind: "path" } }));
			store.observe(input({ sessionID, tool: "read", input: { filePath: "README.md" } }));
			store.finishSession(sessionID);
		}

		store.observe(input({ sessionID: "probe", tool: "inspect", input: {}, output: { kind: "path" } }));
		expect(store.predict("probe")).toEqual([]);
		store.finishSession("probe");
		store.observe(input({ sessionID: "four", tool: "inspect", input: {}, output: { kind: "path" } }));
		store.observe(input({ sessionID: "four", tool: "read", input: { filePath: "README.md" } }));
		store.finishSession("four");

		store.observe(input({ sessionID: "probe-after-four", tool: "inspect", input: {}, output: { kind: "path" } }));
		expect(store.predict("probe-after-four")).toContainEqual(
			expect.objectContaining({ tool: "read", input: { filePath: "README.md" } }),
		);
	});

	test("learns a reusable read range from varying actor windows", () => {
		const store = new PatternAwareStore(settings(), undefined, piActionSemantics());
		for (const [sessionID, filePath, offset, limit] of [
			["one", "src/a.ts", 320, 100],
			["two", "src/b.ts", 840, 100],
		] as const) {
			store.observe(
				input({
					sessionID,
					tool: "grep",
					input: { pattern: "symbol" },
					output: { results: [{ path: filePath, line: offset + 20 }] },
				}),
			);
			store.observe(input({ sessionID, tool: "read", input: { path: filePath, offset, limit } }));
		}

		store.observe(
			input({
				sessionID: "probe",
				tool: "grep",
				input: { pattern: "symbol" },
				output: { results: [{ path: "src/c.ts", line: 1_200 }] },
			}),
		);

		expect(store.predict("probe").find((item) => item.tool === "read")?.input).toEqual({
			path: "src/c.ts",
		});
	});

	test("uses directed K(a) coverage when validating a predicted action", () => {
		const store = new PatternAwareStore(settings(), undefined, piActionSemantics());
		const pattern = validatedGapPattern(
			{ "0": 10 },
			{
				id: "projected-feedback",
				bindings: constantBindings({ path: "src/index.ts", offset: 1, limit: 100 }),
				targetSchemaHash: "read-schema",
			},
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(input({ sessionID: "projected", tool: "grep", input: { pattern: "symbol" } }));
		store.observe(
			input({
				sessionID: "projected",
				tool: "read",
				input: { path: "src/index.ts", offset: 20, limit: 10 },
				schemaHash: "read-schema",
			}),
		);

		const after = store.snapshot().find((item) => item.id === pattern.id);
		expect(after?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		expect(after?.historicalMatches).toBe(pattern.historicalMatches + 1);
	});

	test("memoizes repeated deterministic K(a) resolution, including misses", () => {
		const semantics = piActionSemantics();
		let resolutions = 0;
		const store = new PatternAwareStore(settings(), undefined, {
			...semantics,
			actionKey: (...args) => {
				resolutions++;
				return semantics.actionKey(...args);
			},
		});
		store.registerValidatedPattern(
			validatedGapPattern(
				{ "0": 10 },
				{ id: "memoized-action-key", bindings: constantBindings({ path: "src/index.ts" }) },
			),
		);
		store.registerValidatedPattern(
			validatedGapPattern(
				{ "0": 10 },
				{ id: "memoized-missing-key", bindings: constantBindings({ path: "../outside.ts" }) },
			),
		);

		store.observe(input({ sessionID: "memoized", tool: "grep", input: { pattern: "symbol" } }));
		const first = store.predict("memoized");
		const afterFirstPrediction = resolutions;
		expect(first.find((candidate) => candidate.tool === "read")?.input).toEqual({ path: "src/index.ts" });
		store.predict("memoized");
		expect(resolutions).toBe(afterFirstPrediction);
	});

	test("validates any emitted binding variant instead of only the top-ranked input", () => {
		const store = new PatternAwareStore(settings());
		const pattern = validatedGapPattern(
			{ "0": 10 },
			{ id: "variant-feedback", bindings: collectionBindings() },
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(
			input({
				sessionID: "variant-probe",
				tool: "grep",
				input: { pattern: "TODO" },
				output: { results: [{ path: "src/first.ts" }, { path: "src/second.ts" }] },
			}),
		);
		expect(store.predict("variant-probe").filter((candidate) => candidate.tool === "read")).toHaveLength(2);
		store.observe(input({ sessionID: "variant-probe", tool: "read", input: { filePath: "src/second.ts" } }));

		const after = store.snapshot().find((item) => item.id === pattern.id);
		expect(after?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		expect(after?.historicalMatches).toBe(pattern.historicalMatches + 1);
	});

	test.each([
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
	])("does not validate projected feedback with $name", ({ name, bindings, actor, actorSchemaHash }) => {
		const store = new PatternAwareStore(settings(), undefined, piActionSemantics());
		const pattern = validatedGapPattern(
			{ "0": 10 },
			{ id: `projected-negative-${name}`, bindings, targetSchemaHash: "read-schema" },
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(input({ sessionID: name, tool: "grep", input: { pattern: "symbol" } }));
		store.observe(input({ sessionID: name, tool: "read", input: actor, schemaHash: actorSchemaHash }));

		const after = store.snapshot().find((item) => item.id === pattern.id);
		expect(after?.historicalOpportunities).toBe(pattern.historicalOpportunities + 1);
		expect(after?.historicalMatches).toBe(pattern.historicalMatches);
	});

	test("deduplicates syntactic variants that resolve to the same canonical K(a)", () => {
		const store = new PatternAwareStore(settings(), undefined, piActionSemantics());
		for (const [id, bindings] of [
			["default-implicit", constantBindings({ path: "src/index.ts" })],
			["default-offset-explicit", constantBindings({ path: "src/index.ts", offset: 1 })],
			["bounded-explicit", constantBindings({ path: "src/index.ts", offset: 1, limit: 2000 })],
			["disjoint", constantBindings({ path: "src/index.ts", offset: 2200, limit: 10 })],
		] as const) {
			acceptPattern(store, { "0": 10 }, { id, bindings });
		}

		store.observe(input({ sessionID: "dedupe", tool: "grep", input: { pattern: "symbol" } }));
		const reads = store.predict("dedupe").filter((candidate) => candidate.tool === "read");

		expect(reads).toHaveLength(3);
		expect(reads.map((candidate) => candidate.input)).toEqual(
			expect.arrayContaining([
				{ path: "src/index.ts" },
				{ path: "src/index.ts", offset: 1, limit: 2000 },
				{ path: "src/index.ts", offset: 2200, limit: 10 },
			]),
		);
		expect(
			JSON.parse(reads.find((candidate) => candidate.input.offset === undefined)!.diagnostic).supportingPatterns,
		).toEqual(expect.arrayContaining(["default-implicit", "default-offset-explicit"]));
		expect(reads.find((candidate) => candidate.input.offset === undefined)?.supportingPatternIDs).toEqual(
			expect.arrayContaining(["default-implicit", "default-offset-explicit"]),
		);
	});

	test("retains promoted exact actions beyond bounded context but not beyond the session", () => {
		const store = new PatternAwareStore(
			settings({ maxContextLength: 1, maxFutureGap: 0, minOccurrences: 2 }),
			undefined,
			piActionSemantics(),
		);
		const sessionID = "long-recurrence";
		const command = { command: "npm test -- src/slow.test.ts" };
		store.observe(input({ sessionID, tool: "bash", input: command, outcome: "failure", durationMs: 500 }));
		for (let index = 0; index < 3; index++) {
			store.observe(input({ sessionID, tool: "read", input: { path: `src/before-${index}.ts` } }));
		}
		expect(store.predict(sessionID).some((candidate) => candidate.patternID.startsWith("action-backoff:"))).toBe(
			false,
		);

		store.observe(input({ sessionID, tool: "bash", input: command, durationMs: 700 }));
		for (let index = 0; index < 3; index++) {
			store.observe(input({ sessionID, tool: "read", input: { path: `src/after-${index}.ts` } }));
		}
		const recurrent = store.predict(sessionID).find((candidate) => candidate.patternID.startsWith("action-backoff:"));
		expect(recurrent).toMatchObject({
			tool: "bash",
			input: command,
			horizon: 0,
			latestHorizon: 0,
			expectedDurationMs: 350,
		});
		expect(JSON.parse(recurrent!.diagnostic)).toMatchObject({ context: [], mapperConfidence: 1 });
		const learnedPatternIDs = new Set(store.snapshot().map((pattern) => pattern.id));
		expect(recurrent!.supportingPatternIDs.every((patternID) => learnedPatternIDs.has(patternID))).toBe(true);

		store.finishSession(sessionID);
		store.observe(input({ sessionID: "other", tool: "read", input: { path: "src/other.ts" } }));
		expect(store.predict("other").some((candidate) => candidate.patternID.startsWith("action-backoff:"))).toBe(false);
	});

	test("bounds background exact-action samples by the configured per-tool beam", () => {
		const store = new PatternAwareStore(
			settings({ beamWidth: 2, maxContextLength: 1, maxFutureGap: 0, minOccurrences: 2 }),
			undefined,
			piActionSemantics(),
		);
		const sessionID = "sampled";
		for (let index = 0; index < 2; index++) {
			store.observe(input({ sessionID, tool: "bash", input: { command: "stable" }, durationMs: 100 }));
			store.observe(input({ sessionID, tool: "read", input: { path: "stable.ts" }, durationMs: 10 }));
		}
		store.observe(input({ sessionID, tool: "bash", input: { command: "slow-a" }, durationMs: 900 }));
		store.observe(input({ sessionID, tool: "bash", input: { command: "slow-b" }, durationMs: 1_000 }));
		store.observe(input({ sessionID, tool: "read", input: { path: "slow.ts" }, durationMs: 2_000 }));

		const recurrent = store
			.predict(sessionID)
			.filter((candidate) => candidate.patternID.startsWith("action-backoff:"));
		const sampled = recurrent.filter((candidate) => candidate.background);
		expect(sampled).toHaveLength(2);
		expect(new Set(sampled.map((candidate) => candidate.tool))).toEqual(new Set(["bash", "read"]));
	});

	test("uses canonical K(a) identity and rejects stale schemas or non-learning observations", () => {
		const store = new PatternAwareStore(settings({ minOccurrences: 2 }), undefined, piActionSemantics());
		store.observe(
			input({ sessionID: "canonical", tool: "read", input: { path: "src/a.ts" }, schemaHash: "read-v1" }),
		);
		store.observe(input({ sessionID: "canonical", tool: "grep", input: { pattern: "separator" } }));
		store.observe(
			input({
				sessionID: "canonical",
				tool: "read",
				input: { path: "src/a.ts", offset: 1 },
				schemaHash: "read-v1",
			}),
		);
		const canonical = store
			.predict("canonical", { read: "read-v1" })
			.find((candidate) => candidate.patternID.startsWith("action-backoff:"));
		expect(canonical).toMatchObject({ tool: "read", input: { path: "src/a.ts" } });
		expect(
			store
				.predict("canonical", { read: "read-v2" })
				.some((candidate) => candidate.patternID.startsWith("action-backoff:")),
		).toBe(false);

		for (let index = 0; index < 2; index++) {
			store.observe(
				input({
					sessionID: "not-learned",
					tool: "bash",
					input: { command: "npm test" },
					learnTarget: false,
				}),
			);
		}
		expect(store.predict("not-learned").some((candidate) => candidate.patternID.startsWith("action-backoff:"))).toBe(
			false,
		);
	});

	test("merges exact backoff and keeps contradicted patterns from evicting contextual evidence", () => {
		const store = new PatternAwareStore(
			settings({ beamWidth: 2, minOccurrences: 2 }),
			undefined,
			piActionSemantics(),
		);
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
			store.observe(input({ sessionID: "merged", tool: "bash", input: commands[0], durationMs: 100 }));
		}
		store.observe(input({ sessionID: "merged", tool: "grep", input: { pattern: "trigger" } }));
		const matches = store.predict("merged").filter((candidate) => candidate.tool === "bash");

		expect(matches.map((candidate) => candidate.input)).toEqual(commands.slice(0, 2));
		expect(matches.some((candidate) => candidate.background)).toBe(false);
		expect(matches[0]?.supportingPatternIDs).toContain("contextual-bash-0");
		expect(matches.map((candidate) => JSON.parse(candidate.diagnostic).beamRank)).toEqual([1, 2]);
	});

	test("unlocks a multi-step frontier from speculative structured outputs", () => {
		const store = new PatternAwareStore(settings());
		trainFrontier(store, "one", "src/a.ts", "tests/alpha.test.ts");
		trainFrontier(store, "two", "src/b.ts", "tests/beta.test.ts");

		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		const read = store.predict("probe").find((item) => item.tool === "read");
		expect(read?.depth).toBe(1);

		const lsp = store
			.continue(
				read!.continuation,
				input({
					sessionID: "probe",
					tool: "read",
					input: { filePath: "src/c.ts" },
					output: { nextPath: "tests/gamma.test.ts" },
				}),
			)
			.find((item) => item.tool === "lsp");
		expect(lsp?.input).toEqual({ operation: "diagnostics", filePath: "tests/gamma.test.ts" });
		expect(lsp?.depth).toBe(2);

		const bash = store
			.continue(
				lsp!.continuation,
				input({
					sessionID: "probe",
					tool: "lsp",
					input: lsp!.input,
					output: { command: "bun test tests/gamma.test.ts" },
				}),
			)
			.find((item) => item.tool === "bash");
		expect(bash?.input).toEqual({ command: "bun test tests/gamma.test.ts" });
		expect(bash?.depth).toBe(3);
		expect(new Set(bash?.continuation.visitedPatternIDs).size).toBe(3);
	});

	test("does not count tool-level PPM or failed target latency when valuing concrete patterns", () => {
		const store = new PatternAwareStore(settings({ beamWidth: 1, maxContextLength: 1, maxFutureGap: 0 }));
		for (let index = 0; index < 8; index++) {
			store.observe(input({ sessionID: `fast-${index}`, tool: "grep", input: {}, durationMs: 1 }));
			store.observe(
				input({ sessionID: `fast-${index}`, tool: "read", input: { path: "README.md" }, durationMs: 1 }),
			);
		}
		for (let index = 0; index < 4; index++) {
			store.observe(input({ sessionID: `slow-${index}`, tool: "grep", input: {}, durationMs: 1 }));
			store.observe(
				input({
					sessionID: `slow-${index}`,
					tool: "bash",
					input: { command: "npm test" },
					outcome: index === 0 ? "failure" : "success",
					durationMs: index === 0 ? 10_000 : 100,
				}),
			);
		}

		store.observe(input({ sessionID: "probe", tool: "grep", input: {} }));
		const candidates = store.predict("probe");

		expect(candidates.map((candidate) => candidate.tool)).toEqual(["bash", "read"]);
		expect(candidates[0]).toMatchObject({ tool: "bash", expectedDurationMs: 75 });
	});

	test("unfolds recurrence only through distinct finite-motif contexts", () => {
		const train = (length: number, maxPredictionDepth = 6) => {
			const store = new PatternAwareStore(
				settings({ beamWidth: 1, maxContextLength: 3, maxFutureGap: 0, maxPredictionDepth }),
			);
			for (const sessionID of ["one", "two"]) {
				for (let depth = 0; depth < length; depth++) {
					store.observe(
						input({
							sessionID,
							tool: "inspect",
							input: { value: `src/${sessionID}.ts${".test".repeat(depth)}` },
						}),
					);
				}
			}
			return store;
		};
		const unfold = (store: PatternAwareStore, sessionID: string) => {
			store.observe(input({ sessionID, tool: "inspect", input: { value: "src/probe.ts" } }));
			const candidates = [];
			let candidate = store.predict(sessionID)[0];
			while (candidate) {
				candidates.push(candidate);
				candidate = store.continue(
					candidate.continuation,
					input({ sessionID, tool: "inspect", input: candidate.input, learnTarget: false }),
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

	test("keeps LLM turn boundaries transparent to multi-step continuation", () => {
		const store = new PatternAwareStore(settings());
		for (const [sessionID, sourcePath, testPath] of [
			["one", "src/a.ts", "tests/a.test.ts"],
			["two", "src/b.ts", "tests/b.test.ts"],
		] as const) {
			store.observeTurn();
			store.observe(input({ sessionID, tool: "grep", input: {}, outputPaths: [sourcePath] }));
			store.observeTurn();
			store.observeTurn();
			store.observe(
				input({
					sessionID,
					tool: "read",
					input: { filePath: sourcePath },
					output: { nextPath: testPath },
				}),
			);
			store.observeTurn();
			store.observeTurn();
			store.observe(
				input({
					sessionID,
					tool: "lsp",
					input: { operation: "diagnostics", filePath: testPath },
				}),
			);
		}

		store.observeTurn();
		store.observe(input({ sessionID: "probe", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		const read = store.predict("probe").find((item) => item.tool === "read");
		const lsp = store
			.continue(
				read!.continuation,
				input({
					sessionID: "probe",
					tool: "read",
					input: { filePath: "src/c.ts" },
					output: { nextPath: "tests/c.test.ts" },
				}),
			)
			.find((item) => item.tool === "lsp");

		expect(read?.depth).toBe(1);
		expect(lsp?.depth).toBe(2);
		expect(lsp?.input).toEqual({ operation: "diagnostics", filePath: "tests/c.test.ts" });
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
			{ output: { values: ["abc1234", "tests/value.test.ts::case"] } },
		],
	] as const)("projects %s without parsing display text", (_name, output, paths, expected) => {
		expect(projectPatternAwareObservation(output, paths)).toEqual(expected);
	});

	test("keeps waiting through a different invocation of the target tool while the gap remains", () => {
		const store = new PatternAwareStore(settings({ maxFutureGap: 2 }));
		const pattern = validatedGapPattern(
			{ "1": 10 },
			{
				id: "same-tool-gap",
				bindings: { '["filePath"]': { type: "constant", value: "src/target.ts" } },
			},
		);
		expect(store.registerValidatedPattern(pattern)).toBe(true);

		store.observe(input({ sessionID: "same-tool", tool: "grep", input: { pattern: "TODO" } }));
		store.observe(input({ sessionID: "same-tool", tool: "read", input: { filePath: "src/intermediate.ts" } }));
		store.observe(input({ sessionID: "same-tool", tool: "read", input: { filePath: "src/target.ts" } }));

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

		const registry = new PatternSessionRegistry(patternSessionBudgets(2));
		const first = registry.ensure("first").state;
		registry.ensure("second");
		registry.get("first");
		expect(registry.ensure("third").evicted?.id).toBe("second");
		first.rememberRecurrentAction("one", recurrentAction("one", 1));
		first.rememberRecurrentAction("two", recurrentAction("two", 2));
		first.recurrentAction("one");
		first.rememberRecurrentAction("three", recurrentAction("three", 3));
		expect([...first.recurrentActions].map((item) => item.action.key)).toEqual(["one", "three"]);
		expect(
			first.replacePending([pendingPattern("oldest", 1), pendingPattern("middle", 2), pendingPattern("newest", 3)]),
		).toEqual([expect.objectContaining({ patternID: "oldest" })]);

		const store = new PatternAwareStore(settings());
		store.observe(input({ sessionID: "finished", tool: "read", input: { filePath: "README.md" } }));
		expect(store.recent("finished")).toHaveLength(1);

		store.finishSession("finished");

		expect(store.recent("finished")).toHaveLength(0);
	});

	test("validates imported binding replay independently of control confidence", () => {
		const learned = new PatternAwareStore(settings());
		trainGrepRead(learned, "one", "src/a.ts");
		trainGrepRead(learned, "two", "src/b.ts");
		const pattern = learned
			.snapshot()
			.find((item) => item.targetTool === "read" && item.context.length === 1 && item.context[0]?.tool === "grep");
		expect(pattern).toBeDefined();

		const imported = new PatternAwareStore(settings());
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
		imported.observe(input({ sessionID: "probe", tool: "grep", input: {}, outputPaths: ["src/c.ts"] }));
		expect(imported.predict("probe").some((item) => item.tool === "read" && item.type === "tool_call")).toBe(true);
	});
});

function observeBatchTransition(
	store: PatternAwareStore,
	sessionID: string,
	targets: ReadonlyArray<{ readonly tool: string; readonly input: Record<string, unknown> }>,
) {
	store.observeBatch([input({ sessionID, turnID: `${sessionID}:context`, tool: "inspect", input: { scope: "src" } })]);
	store.observeBatch(targets.map((target) => input({ sessionID, turnID: `${sessionID}:targets`, ...target })));
	store.finishSession(sessionID);
}

function scanBatch(sessionID: string, filePath: string) {
	const turnID = `${sessionID}:scan`;
	return [
		input({ sessionID, turnID, tool: "grep", input: { pattern: "TODO" }, outputPaths: [filePath] }),
		input({ sessionID, turnID, tool: "find", input: { pattern: "src/**/*.ts" }, output: { count: 1 } }),
	];
}

function trainGrepRead(store: PatternAwareStore, sessionID: string, filePath: string, schemaHash?: string) {
	store.observe(input({ sessionID, tool: "grep", input: { pattern: "TODO" }, outputPaths: [filePath] }));
	store.observe(
		input({
			sessionID,
			tool: "read",
			input: { filePath },
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
	store.observe(input({ sessionID, tool: "grep", input: { pattern: "TODO" }, output }));
	store.observe(input({ sessionID, tool: "read", input: { filePath } }));
}

function trainJoinedRead(store: PatternAwareStore, sessionID: string, root: string, name: string) {
	store.observe(input({ sessionID, tool: "inspect", input: {}, output: { root, name } }));
	store.observe(input({ sessionID, tool: "read", input: { filePath: `${root}/${name}` } }));
}

function trainFrontier(store: PatternAwareStore, sessionID: string, sourcePath: string, testPath: string) {
	store.observe(input({ sessionID, tool: "grep", input: {}, outputPaths: [sourcePath] }));
	store.observe(
		input({
			sessionID,
			tool: "read",
			input: { filePath: sourcePath },
			output: { nextPath: testPath },
		}),
	);
	store.observe(
		input({
			sessionID,
			tool: "lsp",
			input: { operation: "diagnostics", filePath: testPath },
			output: { command: `bun test ${testPath}` },
		}),
	);
	store.observe(input({ sessionID, tool: "bash", input: { command: `bun test ${testPath}` } }));
}

function trainResultReads(
	store: PatternAwareStore,
	sessionID: string,
	results: ReadonlyArray<string>,
	reads: ReadonlyArray<string>,
) {
	store.observe(
		input({
			sessionID,
			tool: "grep",
			input: { pattern: "symbol" },
			output: { results: results.map((filePath) => ({ path: filePath })) },
		}),
	);
	for (const filePath of reads) store.observe(input({ sessionID, tool: "read", input: { filePath } }));
}

function settings(overrides: Partial<typeof PATTERN_AWARE_DEFAULTS> = {}) {
	return { ...PATTERN_AWARE_DEFAULTS, minOccurrences: 2, ...overrides };
}

async function patternFile(label: string): Promise<string> {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), `pi-pattern-${label}-`));
	temporary.push(directory);
	return path.join(directory, "patterns.json");
}

function piActionSemantics() {
	return {
		actionKey: (tool: string, actionInput: Readonly<Record<string, unknown>>, schemaHash?: string) =>
			buildPiActionKey(tool, actionInput, "/workspace", schemaHash),
		projectors: [READ_RANGE_ACTION_KEY_PROJECTOR],
	};
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
): ValidatedPattern["bindings"] {
	return {
		'["filePath"]': {
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

type PredictionSettlementBody<Settlement> = Settlement extends unknown ? Omit<Settlement, "prediction"> : never;

function predictionSettlement(settlement: PredictionSettlementBody<PredictionSettlement>): PredictionSettlement {
	return {
		prediction: {
			id: "prediction",
			source: "pattern_aware",
			proposalID: "proposal",
			actionID: "action",
		},
		...settlement,
	} as PredictionSettlement;
}

function unobservedSettlement(stage: ResolutionStage, code: string): PredictionSettlement {
	return predictionSettlement({ observation: "unobserved", cause: { stage, code } });
}

function unmatchedSettlement(): PredictionSettlement {
	return predictionSettlement({
		observation: "observed",
		actorAction: { id: "actor", sequence: 1, turnID: "turn" },
		match: { matched: false },
	});
}

function rejectedSettlement(stage: ResolutionStage, code: string): PredictionSettlement {
	return predictionSettlement({
		observation: "observed",
		actorAction: { id: "actor", sequence: 1, turnID: "turn" },
		match: {
			matched: true,
			relation: { kind: "exact", distance: 0 },
			adoption: { status: "rejected", candidateID: "candidate", cause: { stage, code } },
		},
	});
}

function adoptedSettlement(): PredictionSettlement {
	return predictionSettlement({
		observation: "observed",
		actorAction: { id: "actor", sequence: 1, turnID: "turn" },
		match: {
			matched: true,
			relation: { kind: "exact", distance: 0 },
			adoption: {
				status: "adopted",
				candidateID: "candidate",
			},
		},
	});
}

function input(
	overrides: Partial<Parameters<PatternAwareStore["observe"]>[0]> &
		Pick<Parameters<PatternAwareStore["observe"]>[0], "sessionID" | "tool" | "input">,
) {
	return {
		turnID: `${overrides.sessionID}:turn`,
		outcome: "success" as const,
		durationMs: 10,
		...overrides,
	};
}

function event(overrides: Parameters<typeof input>[0]) {
	return { ...input(overrides), sequence: 1 };
}

function recurrentAction(key: string, sequence: number) {
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

function pendingPattern(patternID: string, triggerSequence: number) {
	return { patternID, triggerSequence, expectedInputs: [], remaining: 1 };
}
