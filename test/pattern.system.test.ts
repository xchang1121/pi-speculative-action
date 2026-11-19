import { afterEach, expect, test, vi } from "vitest";
import path from "node:path";
import { existsSync, globSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { temporaryDirectories } from "./filesystem.ts";
import {
	PATTERN_AWARE_DEFAULTS,
	PatternAwareStore,
	PpmCountTrie,
	patternAwareActionSemantics,
	projectPatternAwareObservation,
	type PatternAwareEvent,
	type PatternAwareEventInput,
} from "../src/pattern-aware.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { PredictionSettlement } from "../src/settlement.ts";
import {
	createReadTool,
	createGrepTool,
	createFindTool,
	createLsTool,
	createBashTool,
} from "@earendil-works/pi-coding-agent";
import { systemHost } from "./agent-system-fixture.ts";

const directories = temporaryDirectories("pi-pattern-system-");
afterEach(directories.dispose);

test.each([1, 2])("preserves constant prediction support when another session chose %s", async (mode) => {
	const file = path.join(await directories.create(), "support.json");
	const settings = {
		...PATTERN_AWARE_DEFAULTS,
		presets: [],
		maxContextLength: 1,
		maxFutureGap: 0,
	};
	let store = new PatternAwareStore(settings, file);
	await store.load();
	const event = (sessionID: string, turnID: string, tool = "inspect", input = {}) => ({
		sessionID,
		turnID,
		tool,
		input,
		outcome: "success" as const,
		durationMs: 1,
	});
	for (const [session, choice, count] of [
		["first", 1, 4],
		["second", mode, 1],
	] as const)
		for (let index = 0; index < count; index++) {
			store.observe(event(session, String(index)));
			store.observe(event(session, String(index), "choose", { mode: choice }));
			store.finishSession(session);
		}
	const assertSupport = () => {
		for (const session of ["first", "unseen"])
			expect(
				store.predictAfterBatch(session, [event(session, "new")]).map((candidate) => candidate.input),
			).toEqual(session === "first" || mode === 1 ? [{ mode: 1 }] : []);
	};
	assertSupport();
	await store.flush();
	store = new PatternAwareStore(settings, file);
	const loading = store.load();
	await store.load();
	assertSupport();
	await loading;
});

test("scopes reserved observation revisions to each PatternAware turn", async () => {
	const f = systemHost(
		process.cwd(),
		[createReadTool(process.cwd())],
		{
			patternStore: new PatternAwareStore({
				...PATTERN_AWARE_DEFAULTS,
				presets: [],
			}),
		},
		{ patternAware: { enabled: true, presets: [] } },
	);
	let extra = 0;
	let prior: Parameters<typeof f.host.runtime.submitPlan>[0] | undefined;
	const admitted: string[] = [];
	f.host.runtime.subscribe({
		prediction_admitted: (event) => {
			admitted.push(event.proposalID);
		},
	});
	const reserve = vi
		.spyOn(f.host.runtime, "reservePlanRevision")
		.mockImplementation((_session, _id, minimum) => minimum + extra);
	const submit = vi.spyOn(f.host.runtime, "submitPlan");
	try {
		for (let round = 0; round < 2; round++) {
			await f.start("same");
			if (prior)
				await f.host.runtime.submitPlan(prior, {
					id: "stale-turn",
					source: "pattern_aware",
					revision: 0,
					actions: [
						{
							id: "read",
							type: "tool_call",
							tool: "read",
							input: { path: "package.json" },
						},
					],
				});
			for (const [offset, revision] of [
				[0, 0],
				[5, 6],
				[0, 7],
			]) {
				extra = offset!;
				submit.mockClear();
				await f.host.execute(
					{ turnID: "same", tool: "inspect", args: {}, tools: [] },
					undefined,
					async () => ({ content: [], details: {} }),
				);
				await expect.poll(() => submit.mock.calls.at(-1)?.[1]).toMatchObject({ revision });
				prior = submit.mock.calls.at(-1)?.[0];
			}
			await f.finish();
		}
		expect(admitted).toEqual([]);
	} finally {
		await f.dispose();
		reserve.mockRestore();
		submit.mockRestore();
	}
});

const locations: PatternAwareEvent = {
	sessionID: "bindings",
	turnID: "one",
	tool: "custom-inspect",
	input: {},
	outcome: "success",
	durationMs: 1,
	sequence: 1,
	outputLocations: [
		{ path: "src/a.ts", line: 3 },
		{ path: "src/b.ts", line: 17 },
	],
};

test.each([false, true])(
	"learns exact structured field bindings with a single prediction (cross-row %s)",
	(crossRow) => {
		const store = new PatternAwareStore({
			...PATTERN_AWARE_DEFAULTS,
			presets: [],
			maxContextLength: 1,
			maxFutureGap: 0,
			beamWidth: 1,
		});
		for (let index = 0; index < 4; index++) {
			const context = {
				...locations,
				sessionID: String(index),
				outputLocations: [
					{ path: `src/a${index}.ts`, line: 3 + 100 * index },
					{ path: `src/b${index}.ts`, line: 17 + 100 * index },
				],
			};
			store.observe(context);
			store.observe({
				...context,
				tool: "custom-read",
				input: {
					path: `src/a${index}.ts`,
					offset: (crossRow ? 17 : 3) + 100 * index,
				},
				outputLocations: undefined,
			});
			store.finishSession(context.sessionID);
		}
		const predicted = store.predictAfterBatch("unseen", [
			{
				...locations,
				sessionID: "unseen",
				outputLocations: [
					{ path: "src/c.ts", line: 5 },
					{ path: "src/d.ts", line: 21 },
				],
			},
		]);
		expect(predicted.map(({ input }) => input)).toEqual([{ path: "src/c.ts", offset: crossRow ? 21 : 5 }]);
	},
);

test.each([1, 8, 64])(
	"keeps bounded learned suffixes through persistence and further learning (capacity %s)",
	(capacity) => {
		const model = new PpmCountTrie(4);
		const restored = new PpmCountTrie(4);
		const history: string[] = [];
		for (let sequence = 1; sequence <= 200; sequence++) {
			const target = sequence % 3 ? "inspect" : "render";
			for (const store of [model, restored]) {
				store.observe(history, target, sequence, 32);
				store.trim(capacity);
			}
			expect(model.size).toBeLessThanOrEqual(capacity);
			expect(restored.distribution(history, sequence, 32)).toEqual(model.distribution(history, sequence, 32));
			if (sequence % 17 === 0) restored.restore(JSON.parse(JSON.stringify(model.snapshot())));
			history.push(`context:${sequence % 7}`);
			if (history.length > 4) history.shift();
		}
		const probabilities = [...model.distribution(history, 200, 32).values()];
		expect(probabilities.every((value) => Number.isFinite(value) && value > 0 && value < 1)).toBe(true);
		expect(probabilities.reduce((sum, value) => sum + value, 0)).toBeLessThanOrEqual(1);
	},
);

function event(
	sessionID: string,
	tool: string,
	input: Record<string, unknown> = {},
	facts: Partial<PatternAwareEventInput> = {},
): PatternAwareEventInput {
	return { sessionID, turnID: tool, tool, input, outcome: "success", durationMs: 1, ...facts };
}
const immediate = { ...PATTERN_AWARE_DEFAULTS, maxContextLength: 1, maxFutureGap: 0 };
const textResult = (text: string, details?: unknown) => ({
	content: [{ type: "text" as const, text }],
	details,
});

test.each(["grep", "find", "ls", "bash"] as const)(
	"learns read arguments from the %s tool result on the host observation path",
	async (tool) => {
		const root = await directories.create();
		const reporters = {
			grep: createGrepTool(root),
			find: createFindTool(root, {
				operations: { exists: existsSync, glob: async (pattern, cwd) => globSync(pattern, { cwd }) },
			}),
			ls: createLsTool(root),
			bash: createBashTool(root, {
				operations: {
					exec: async (command, cwd, { onData }) => {
						const child = spawnSync(process.execPath, [command.slice("node ".length)], { cwd });
						if (child.error) throw child.error;
						onData(Buffer.concat([child.stdout, child.stderr]));
						return { exitCode: child.status };
					},
				},
			}),
		};
		const tools = [createReadTool(root), reporters[tool]];
		const store = new PatternAwareStore(immediate);
		const h = systemHost(root, tools, { patternStore: store }, { patternAware: immediate });
		try {
			let readSchema: string | undefined;
			for (let i = 0; i < 4; i++) {
				const dir = "src/unit-" + i;
				const name = "file" + i + ".cjs";
				const target = dir + "/" + name;
				const line = i + 2;
				await mkdir(path.join(root, dir), { recursive: true });
				const contents = tool === "bash" ? 'throw new TypeError("failure");' : "// needle" + i;
				await writeFile(path.join(root, target), "\n".repeat(line - 1) + contents);
				const args =
					tool === "bash"
						? { command: "node " + target }
						: tool === "ls"
							? { path: dir }
							: { path: dir, pattern: tool === "find" ? "*.cjs" : "needle" + i };
				const turnID = "report-" + i;
				await h.start(turnID);
				const reported = h.host.execute({ turnID, tool, args, tools }, undefined, () =>
					reporters[tool].execute(turnID, args as never, undefined),
				);
				if (tool === "bash") await expect(reported).rejects.toThrow("Command exited with code 1");
				else await reported;
				await h.finish();
				const expected = tool === "grep" || tool === "bash" ? { path: target, offset: line } : { path: target };
				const observation = store.recent("system").at(-1);
				expect(observation?.outputPaths).toEqual([target]);
				if (tool === "bash")
					expect(observation).toMatchObject({ outcome: "failure", errorClass: "exit:1 TypeError" });
				if (i === 3) {
					expect(
						store.predict("system", readSchema ? { read: readSchema } : {}).map((action) => action.input),
					).toContainEqual(expected);
					break;
				}
				await h.start("read-" + i);
				await h.host.execute(
					{ turnID: "read-" + i, tool: "read", args: expected, tools },
					undefined,
					async () => textResult("file content"),
				);
				await h.finish();
				readSchema = store.recent("system").at(-1)?.schemaHash;
			}
		} finally {
			await h.dispose();
		}
	},
);

test("separates list cardinality and failure classes and bounds text facts to the workspace", async () => {
	const root = await directories.create();
	const project = (tool: string, text: string, details?: unknown) =>
		projectPatternAwareObservation(textResult(text, details), { tool, input: {} }, root);
	expect(
		project("find", "../outside.ts\n" + path.join(root, "a.ts") + "\nhttps://example.test/file"),
	).toMatchObject({
		cardinality: "singleton",
		outputPaths: ["a.ts"],
	});
	expect(
		project("bash", 'src/a.ts(12,3): error TS1234\n    at run (src/b.ts:20:4)\n  File "src/c.py", line 5'),
	).toMatchObject({
		outputLocations: [
			{ path: "src/a.ts", line: 12 },
			{ path: "src/b.ts", line: 20 },
			{ path: "src/c.py", line: 5 },
		],
	});
	expect(project("grep", "x.ts:8: match\nx.ts-9- context")).toMatchObject({
		cardinality: "singleton",
		outputLocations: [{ path: "x.ts", line: 8 }],
	});
	expect(project("ls", " leading.ts\nnormal name.ts")).toMatchObject({
		outputPaths: [" leading.ts", "normal name.ts"],
	});
	expect(project("find", "x".repeat(8192))).toEqual({ cardinality: "truncated" });
	expect(project("bash", "at src/a.ts:4:8\nat node:internal/test.js:10:6\n--> src/lib.rs:7:3")).toMatchObject({
		outputLocations: [
			{ path: "src/a.ts", line: 4 },
			{ path: "src/lib.rs", line: 7 },
		],
	});
	expect(
		projectPatternAwareObservation(
			textResult("TypeError: lost"),
			{ tool: "bash", input: {}, failed: true },
			root,
		).errorClass,
	).toBe("TypeError");
	const store = new PatternAwareStore(immediate);
	for (const [report, target] of [
		[project("grep", "a.ts:1: match"), "inspect"],
		[project("grep", "No matches found"), "broaden"],
		[project("grep", "a.ts:1: match", { matchLimitReached: 1 }), "narrow"],
		[{ outcome: "failure" as const, errorClass: "exit:1" }, "retry"],
		[{ outcome: "failure" as const, errorClass: "exit:2" }, "fix"],
	] as const) {
		for (let i = 0; i < 6; i++) {
			const id = target + i;
			store.observe(event(id, "grep", {}, report));
			store.observe(event(id, target, {}));
			store.finishSession(id);
		}
		expect(
			store.predictAfterBatch("held", [event("held", "grep", {}, report)]).map((action) => action.tool),
		).toEqual([target]);
	}
	expect(
		store
			.predictAfterBatch("new", [event("new", "grep", {}, project("grep", "No matches found"))])
			.map((action) => action.tool),
	).toEqual(["broaden"]);
});

test("admits repeated transitions, prunes redundant prefixes and counts alternatives at the correct gap", async () => {
	const file = path.join(await directories.create(), "patterns.json");
	const settings = {
		...PATTERN_AWARE_DEFAULTS,
		maxContextLength: 3,
		maxFutureGap: 1,
		maxPatterns: 24,
		decayHalfLifeEvents: 0,
	};
	const store = new PatternAwareStore(settings, file);
	await store.load();
	for (let i = 0; i < 12; i++) {
		const id = String(i),
			name = "p" + i + ".ts";
		store.observe(event(id, "start"));
		store.observe(event(id, "locate", {}, { outputPaths: [name] }));
		store.observe(event(id, "intervening"));
		store.observe(
			i % 2
				? event(id, "read", { path: name })
				: event(id, "unrelated", { label: "other" }, { learnTarget: i % 4 === 0 }),
		);
		store.finishSession(id);
	}
	for (let i = 0; i < 120; i++) {
		const id = "noise-" + i;
		store.observe(event(id, "explore", {}, { operation: id }));
		store.observe(event(id, "one-off", { value: id }));
		store.finishSession(id);
	}
	await store.flush();
	const { pools, events } = JSON.parse(await readFile(file, "utf8"));
	expect(pools.length).toBeLessThan(8);
	expect(events.some((item: PatternAwareEvent) => item.sessionID.startsWith("noise-"))).toBe(false);
	expect(
		store
			.snapshot()
			.filter((p) => p.targetTool === "read")
			.map((p) => [p.gap, ...p.context.map((e) => e.tool)]),
	).toEqual([
		[0, "locate", "intervening"],
		[1, "locate"],
	]);
	for (const candidateStore of [store, new PatternAwareStore(settings, file)]) {
		await candidateStore.load();
		const next = candidateStore.predictAfterBatch("evaluation", [
			event("evaluation", "locate", {}, { outputPaths: ["held.ts"] }),
		]);
		const read = next.find((action) => action.tool === "read");
		expect(read).toMatchObject({ horizon: 1, input: { path: "held.ts" } });
		expect(read!.conditionalProbability).toBeCloseTo(0.5);
		expect(next.some((action) => action.horizon === 1 && action.tool === "unrelated")).toBe(false);
	}
});

test("generalizes correlated collection rows across sessions despite an initial outlier and favors the head", () => {
	const store = new PatternAwareStore(immediate);
	const hits = (id: string) =>
		[0, 1, 2].map((index) => ({ path: id + "-" + index + ".ts", line: 10 * Number(id) + index + 1 }));
	for (let index = 1; index <= 12; index++) {
		const id = String(index),
			rows = hits(id);
		store.observe(event(id, "diagnose", {}, { outputLocations: rows }));
		const chosen = index === 1 ? { path: "unreported", line: 42 } : rows[index % 3 === 0 ? 1 : 0]!;
		store.observe(event(id, "read", { path: chosen.path, offset: chosen.line }));
		store.finishSession(id);
	}
	const rows = hits("30");
	const next = store.predictAfterBatch("30", [event("30", "diagnose", {}, { outputLocations: rows })]);
	expect(next.map((action) => action.input)).toEqual(rows.map(({ path, line }) => ({ path, offset: line })));
	expect(next[0]!.conditionalProbability).toBeGreaterThan(next[1]!.conditionalProbability);
	expect(next[1]!.conditionalProbability).toBeGreaterThan(next[2]!.conditionalProbability);
	expect(Object.values(store.snapshot()[0]!.bindings).map((binding) => binding.type)).toEqual(["each", "each"]);
});

test("withdraws a previously adopted portable pattern after observed mismatches and rejections", async () => {
	const root = await directories.create(),
		file = path.join(root, "workspace.json"),
		portable = path.join(root, "portable.json");
	const store = new PatternAwareStore(immediate, file, undefined, portable);
	await store.load();
	for (let i = 0; i < 6; i++) {
		const id = String(i);
		store.observe(event(id, "search", {}, { outputPaths: ["p" + id] }));
		store.observe(event(id, "read", { path: "p" + id }));
		store.finishSession(id);
	}
	const proposal = () =>
		store.predictAfterBatch("fresh", [event("fresh", "search", {}, { outputPaths: ["new"] })])[0]!;
	const original = proposal();
	const feedback = (kind: "adopted" | "rejected" | "mismatched"): PredictionSettlement => {
		const observed = {
			prediction: { id: "p", proposalID: "proposal", actionID: "a", source: "pattern_aware" },
			actorAction: { id: "actor", turnID: "turn", sequence: 1 },
			observation: "observed" as const,
		};
		if (kind === "mismatched") return { ...observed, match: { matched: false } };
		return {
			...observed,
			match: {
				matched: true,
				relation: { kind: "exact", distance: 0 },
				adoption:
					kind === "adopted"
						? { status: "adopted", candidateID: "candidate" }
						: { status: "rejected", cause: { stage: "freshness", code: "changed" } },
			},
		};
	};
	for (let i = 0; i < 3; i++) store.settled(original.patternID, feedback("adopted"));
	await store.flush();
	expect(JSON.parse(await readFile(portable, "utf8")).patterns).toHaveLength(1);
	for (let i = 0; i < 8; i++) store.settled(original.patternID, feedback(i % 2 ? "mismatched" : "rejected"));
	const penalized = proposal();
	expect(penalized.background).toBe(true);
	expect(penalized.confidence).toBeLessThan(original.confidence);
	await store.flush();
	expect(JSON.parse(await readFile(portable, "utf8")).patterns).toEqual([]);
	const other = new PatternAwareStore(immediate, undefined, undefined, portable);
	await other.load();
	expect(other.snapshot()).toEqual([]);
});

test("promotes a longer context only while it improves on the suffix and credits one provider", () => {
	const store = new PatternAwareStore({ ...immediate, maxContextLength: 2, decayHalfLifeEvents: 0 });
	for (let i = 0; i < 24; i++) {
		const id = String(i);
		store.observe(event(id, i % 2 ? "compile" : "lint"));
		store.observe(event(id, "inspect"));
		store.observe(event(id, i % 2 ? "report" : "format", {}));
		store.finishSession(id);
	}
	store.observe(event("held", "compile"));
	store.observe(event("held", "inspect"));
	const report = store.predict("held").find((candidate) => candidate.tool === "report")!;
	expect(report.conditionalProbability).toBeCloseTo(1);
	expect(report.supportingPatternIDs).toEqual([report.patternID]);
	expect(
		store
			.snapshot()
			.find((p) => p.id === report.patternID)
			?.context.map((s) => s.tool),
	).toEqual(["compile", "inspect"]);
});

test("counts confirmed stops and still foregrounds a prefix with a real successor after reload", async () => {
	const file = path.join(await directories.create(), "patterns.json");
	const settings = { ...immediate, maxContextLength: 2, decayHalfLifeEvents: 0 };
	const store = new PatternAwareStore(settings, file);
	await store.load();
	for (let i = 0; i < 16; i++) {
		const id = String(i),
			input = { path: id + ".ts" };
		store.observe(event(id, i % 4 === 0 ? "grep" : "find"));
		store.observe(event(id, "read", input));
		if (i % 4 === 0) store.observe(event(id, "edit", input));
		store.finishSession(id, true);
	}
	// Settings changes and host disposal can clear a session without observing its outcome.
	store.observe(event("unclosed", "read", { path: "unclosed.ts" }));
	store.finishSession("unclosed");
	await store.flush();
	const loaded = new PatternAwareStore(settings, file);
	await loaded.load();
	const input = { path: "held.ts" };
	const onlyRead = loaded.predictAfterBatch("held", [event("held", "read", input)]);
	expect(onlyRead.find((candidate) => candidate.tool === "edit")).toMatchObject({
		conditionalProbability: 0.25,
		background: true,
	});
	loaded.observe(event("held", "grep"));
	const afterSearch = loaded.predictAfterBatch("held", [event("held", "read", input)]);
	const edit = afterSearch.find((candidate) => candidate.tool === "edit");
	expect(edit).toMatchObject({ input, conditionalProbability: 1 });
	expect(edit?.background).toBeFalsy();
});

function symbolReport(session: string, tool: string, count: number, text = false, identity?: string) {
	const rows = Array.from({ length: count }, (_, i) => ({
		uri: "lib/" + session + "-" + i + ".go",
		line: 301 + i,
	}));
	const output = textResult(
		rows.map((row) => row.uri + "#" + row.line).join("\n"),
		text ? undefined : { rows },
	);
	const report = event(
		session,
		tool,
		{},
		{
			...projectPatternAwareObservation(output, { tool, input: {} }, process.cwd()),
			patternIdentity: identity,
		},
	);
	const input = (index: number) => ({ path: rows[index]!.uri, offset: rows[index]!.line });
	return { report, input };
}
const accepted: PredictionSettlement = {
	prediction: { id: "p", proposalID: "proposal", actionID: "a", source: "pattern_aware" },
	observation: "observed",
	actorAction: { id: "actor", turnID: "turn", sequence: 1 },
	match: {
		matched: true,
		relation: { kind: "exact", distance: 0 },
		adoption: { status: "adopted", candidateID: "p" },
	},
};

test("learns an unissued row beyond the beam from Actor arguments, adapts and restores its ranking", async () => {
	const file = path.join(await directories.create(), "ranking.json");
	const settings = { ...PATTERN_AWARE_DEFAULTS, maxFutureGap: 0 };
	const store = new PatternAwareStore(settings, file);
	await store.load();
	const train = (i: number, index: number) => {
		const id = "rank" + i;
		const { report, input } = symbolReport(id, "symbols", 40);
		store.observe(report);
		store.observe(event(id, "open", input(index)));
		store.finishSession(id, true);
	};
	for (let i = 0; i < 20; i++) train(i, i % 4 === 3 ? 1 : 0);
	const held = symbolReport("held", "symbols", 40);
	const predict = (s: PatternAwareStore) => s.predictAfterBatch("held", [held.report]).map((p) => p.input);
	expect(predict(store)[0]).toEqual(held.input(0));
	expect(predict(store)).not.toContainEqual(held.input(39));
	for (let i = 20; i < 40; i++) train(i, i % 4 === 3 ? 0 : 39);
	expect(predict(store)[0]).toEqual(held.input(39));
	await store.flush();
	const restored = new PatternAwareStore(settings, file);
	await restored.load();
	expect(predict(restored)).toEqual(predict(store));
});

test("learns aligned text spans and numeric arguments, rejects ambiguous copies and partial rows", async () => {
	const file = path.join(await directories.create(), "spans.json");
	const store = new PatternAwareStore(immediate, file);
	await store.load();
	for (let i = 0; i < 12; i++) {
		const id = "text" + i;
		const { report, input } = symbolReport(id, "catalog", 3, true);
		store.observe(report);
		store.observe(event(id, "open", input(i % 4 === 3 ? 1 : 0)));
		store.finishSession(id, true);
	}
	await store.flush();
	const fresh = new PatternAwareStore(immediate, file);
	await fresh.load();
	const held = symbolReport("unseen", "catalog", 3, true);
	expect(fresh.predictAfterBatch("unseen", [held.report]).map((p) => p.input)).toEqual(
		[0, 1, 2].map(held.input),
	);
	const project = (value: string) =>
		projectPatternAwareObservation(textResult(value), { tool: "opaque", input: {} }, process.cwd());
	expect(project("good#1\n" + "z".repeat(6000)).textLines).toEqual(["good#1"]);
	expect(
		projectPatternAwareObservation(textResult("done\npart", { truncation: { truncated: true } }), {
			tool: "opaque",
			input: {},
		}).textLines,
	).toEqual(["done"]);
	const ambiguous = new PatternAwareStore(immediate);
	for (let i = 0; i < 8; i++) {
		const id = String(i),
			value = "sym-" + i;
		ambiguous.observe(event(id, "opaque", {}, project("primary=" + value + "|backup=" + value)));
		ambiguous.observe(event(id, "navigate", { symbol: value }));
		ambiguous.finishSession(id);
	}
	expect(
		ambiguous.predictAfterBatch("later", [event("later", "opaque", {}, project("primary=aa|backup=bb"))]),
	).toEqual([]);
});

test("reuses adopted row selectors with a new tool pair after local confirmation", async () => {
	const root = await directories.create(),
		portable = path.join(root, "shared.json");
	const a = new PatternAwareStore(immediate, path.join(root, "a.json"), undefined, portable);
	await a.load();
	for (let i = 0; i < 12; i++) {
		const id = "a" + i,
			{ report, input } = symbolReport(id, "catalog", 3);
		a.observe(report);
		a.observe(event(id, "inspect", input(i % 2)));
		a.finishSession(id);
	}
	const [original] = a.snapshot();
	for (let i = 0; i < 3; i++) a.settled(original!.id, accepted);
	await a.flush();
	const b = new PatternAwareStore(immediate, path.join(root, "b.json"), undefined, portable);
	await b.load();
	const held = symbolReport("held", "query", 3);
	expect(b.predictAfterBatch("held", [held.report])).toEqual([]);
	for (let i = 0; i < 2; i++) {
		const id = "b" + i,
			{ report, input } = symbolReport(id, "query", 3);
		b.observe(report);
		b.observe(event(id, "reveal", input(0)));
		b.finishSession(id);
	}
	expect(b.predictAfterBatch("held", [held.report]).map((p) => [p.tool, p.input])).toEqual(
		[0, 1, 2].map((i) => ["reveal", held.input(i)]),
	);
});

test("routes a declared rename to the registered target and refuses ambiguous roles or a changed parameter schema", async () => {
	const root = await directories.create(),
		portable = path.join(root, "shared.json");
	const a = new PatternAwareStore(immediate, path.join(root, "a.json"), undefined, portable);
	await a.load();
	for (let i = 0; i < 10; i++) {
		const id = "old" + i,
			{ report, input } = symbolReport(id, "old_symbols", 3, true, "symbols");
		a.observe(report);
		a.observe(
			event(id, "old_open", input(i % 3 === 1 ? 1 : 0), {
				patternIdentity: "open-location",
				schemaHash: "location",
			}),
		);
		a.finishSession(id);
	}
	const [p] = a.snapshot();
	for (let i = 0; i < 3; i++) a.settled(p!.id, accepted);
	await a.flush();
	expect(await readFile(portable, "utf8")).not.toContain("lib/old");
	const b = new PatternAwareStore(immediate, undefined, undefined, portable);
	await b.load();
	const { report, input } = symbolReport("held", "new_symbols", 3, true, "symbols");
	const aliases = { new_open: "open-location", new_symbols: "symbols" };
	const predict = (routing: Record<string, string>, hash = "location") =>
		b.predictAfterBatch("held", [report], { new_open: hash }, immediate, undefined, routing);
	expect(predict(aliases)[0]).toMatchObject({ tool: "new_open", input: input(0) });
	expect(predict({ ...aliases, duplicate: "open-location" })).toEqual([]);
	expect(predict(aliases, "other")).toEqual([]);
	expect(predict({})).toEqual([]);
});

test("checks workspace confinement for read arguments learned from a custom text result", () => {
	const cwd = process.cwd();
	const store = new PatternAwareStore(
		immediate,
		undefined,
		patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd),
	);
	const listing = (id: string, path: string) =>
		event(
			id,
			"catalog",
			{},
			projectPatternAwareObservation(textResult(path), { tool: "catalog", input: {} }, cwd),
		);
	for (let i = 0; i < 8; i++) {
		const id = String(i),
			file = "src/" + id + ".ts";
		store.observe(listing(id, file));
		store.observe(event(id, "read", { path: file }));
		store.finishSession(id, true);
	}
	const next = (file: string) => store.predictAfterBatch("held", [listing("held", file)]).map((p) => p.input);
	expect(next("src/new.ts")).toEqual([{ path: "src/new.ts" }]);
	expect(next("../outside.ts")).toEqual([]);
});

test("learns absolute Actor paths from relative output and preserves non-round-tripping tool arguments", async () => {
	const cwd = await directories.create();
	const semantics = patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd, PI_ACTION_SEMANTICS.projectors());
	const store = new PatternAwareStore(immediate, undefined, semantics);
	for (let i = 0; i < 6; i++) {
		const id = "paths-" + i;
		const file = "src/" + i + ".ts";
		const args = { path: path.join(cwd, file) };
		store.observe(event(id, "report", {}, { outputPaths: [file] }));
		store.observe(event(id, "read", args));
		expect(args.path).toBe(path.join(cwd, file));
		expect(store.recent(id).at(-1)?.input.path).toBe(file);
		store.finishSession(id, true);
	}
	const candidates = store.predictAfterBatch("later", [
		event("later", "report", {}, { outputPaths: ["src/new.ts"] }),
	]);
	expect(candidates.map((p) => p.input)).toContainEqual({ path: "src/new.ts" });
	const raw = { path: path.join(cwd, "src", "abs-only.ts") };
	const absoluteOnly = new PatternAwareStore(immediate, undefined, {
		...semantics,
		actionKey: (tool, input, schema) =>
			typeof input.path === "string" && path.isAbsolute(input.path)
				? semantics.actionKey(tool, input, schema)
				: undefined,
	});
	absoluteOnly.observe(event("other", "read", raw));
	expect(absoluteOnly.recent("other")[0]?.input).toEqual(raw);
});

test("calibrates repeated choices per session and preserves the mass of a collection", () => {
	const store = new PatternAwareStore(immediate);
	for (let i = 0; i < 12; i++) {
		const id = "train-" + i,
			{ report, input } = symbolReport(id, "catalog", 3);
		store.observe(report);
		store.observe(event(id, "open", input(i % 4 === 3 ? 1 : 0)));
		store.finishSession(id, true);
	}
	const { report, input } = symbolReport("rows", "catalog", 3);
	const predict = (id: string) =>
		store.predictAfterBatch(id, [{ ...report, sessionID: id }]).filter((p) => p.tool === "open");
	const total = (values: ReturnType<typeof predict>) =>
		values.reduce((sum, p) => sum + p.conditionalProbability, 0);
	const initial = predict("cold");
	expect(initial[0]?.input).toEqual(input(0));
	for (let i = 0; i < 10; i++) {
		store.observe(event("explore", "open", i < 2 ? input(i) : { path: String(i), offset: i }));
		store.observe(event("repeat", "open", input(0)));
	}
	const explore = predict("explore"),
		repeat = predict("repeat");
	expect(explore[0]?.input).toEqual(input(2));
	expect(repeat[0]?.input).toEqual(input(0));
	expect(total(explore)).toBeCloseTo(total(initial));
	expect(total(repeat)).toBeCloseTo(total(initial));
	store.finishSession("explore", true);
	expect(predict("explore")[0]?.input).toEqual(input(0));
});

test("ranks lazy target counts at the current half-life when pruning and saving", () => {
	const model = new PpmCountTrie(1);
	for (const name of ["old", "mixed"]) model.setCount([name], "past", 128, 1);
	model.observeContext(["mixed"], "now", 129, 8);
	model.observeContext(["fresh"], "now", 129, 8);
	model.trim(2);
	expect(model.evidence(["fresh"], "now", 129, 8)?.hits).toBe(1);
	model.observeContext(["fresh"], "now", 130, 8);
	const [strongest] = model.snapshot(1);
	expect(strongest?.context).toEqual(["fresh"]);
	const restored = new PpmCountTrie(1);
	restored.restore(model.snapshot());
	expect(restored.evidence(["fresh"], "now", 130, 8)?.hits).toBeCloseTo(1 + 2 ** (-1 / 8));
	expect(restored.evidence(["mixed"], "past", 130, 8)?.hits).toBeCloseTo(128 * 2 ** (-129 / 8), 10);
});

test("admits new transitions after decay and retains every pooled suffix through noise and reload", async () => {
	const file = path.join(await directories.create(), "transitions.json");
	const settings = { ...immediate, maxContextLength: 3, maxPatterns: 2, decayHalfLifeEvents: 16 };
	const store = new PatternAwareStore(settings, file);
	await store.load();
	const noise = (tool: string, id: string) => {
		store.observe(event(id, tool));
		store.observe(event(id, "ignored", {}, { learnTarget: false }));
		store.finishSession(id);
	};
	for (let i = 0; i < 24; i++) noise("old" + (i % 2), "old" + i);
	for (let i = 0; i < 160; i++) store.observeTurn();
	const source = (id: string) => event(id, "locate", {}, { outputPaths: [id + ".ts"] });
	const train = (i: number) => {
		const id = String(i);
		store.observe(source(id));
		store.observe(event(id, "middle", {}, { learnTarget: false }));
		store.observe(event(id, "bridge", {}, { learnTarget: false }));
		store.observe(event(id, "open", { path: id + ".ts" }));
		store.finishSession(id);
	};
	const evidence = () =>
		store.snapshot().find((p) => p.targetTool === "open" && p.context.length === 3)!.historicalMatches;
	for (let i = 0; i < 4; i++) train(i);
	let expected = evidence();
	expect(expected).toBeGreaterThan(2);
	for (let i = 4; i < 8; i++) {
		train(i);
		expected = expected * 2 ** (-4 / settings.decayHalfLifeEvents) + 1;
		expect(evidence()).toBeCloseTo(expected, 10);
	}
	for (let i = 0; i < 24; i++) noise("noise" + (i % 3), "new-noise" + i);
	await store.flush();
	const saved: { transitions: { context: string[] }[] } = JSON.parse(await readFile(file, "utf8"));
	const contexts = saved.transitions.map(({ context }) =>
		context.slice(1).map((token) => JSON.parse(token)[0]),
	);
	expect(contexts).toHaveLength(3);
	for (const suffix of [["locate", "middle", "bridge"], ["middle", "bridge"], ["bridge"]])
		expect(contexts).toContainEqual(suffix);
	const loaded = new PatternAwareStore(settings, file);
	await loaded.load();
	expect(loaded.snapshot()).toEqual(store.snapshot());
	loaded.observe(source("held"));
	loaded.observe(event("held", "middle"));
	expect(loaded.predictAfterBatch("held", [event("held", "bridge")])).toContainEqual(
		expect.objectContaining({ tool: "open", input: { path: "held.ts" } }),
	);
	await loaded.flush();
});

test("merges per-gap probability into the covered window while crediting a single provider", () => {
	const settings = { ...immediate, maxFutureGap: 1, decayHalfLifeEvents: 0 };
	const store = new PatternAwareStore(settings);
	const report = (id: string) => event(id, "locate", {}, { outputPaths: [id + ".ts"] });
	const train = (id: string, steps: readonly string[]) => {
		store.observe(report(id));
		for (const step of steps)
			store.observe(
				step === "open" ? event(id, step, { path: id + ".ts" }) : event(id, step, {}, { learnTarget: false }),
			);
		store.finishSession(id);
	};
	for (let i = 0; i < 12; i++) train(String(i), i % 3 ? ["open", "wait"] : ["wait", "open"]);
	const open = (coverage: number) =>
		store
			.predictAfterBatch(
				"held",
				[report("held")],
				{},
				{
					...settings,
					futureGapCoverage: coverage,
				},
				{ visitedPatternIDs: [], pathProbability: 0.5 },
			)
			.find((p) => p.tool === "open")!;
	const patterns = store.snapshot().filter((p) => p.targetTool === "open");
	const early = patterns.find((p) => p.gap === 0)!;
	const late = patterns.find((p) => p.gap === 1)!;
	store.settled(late.id, accepted);
	const lateAdoption = store.snapshot().find((p) => p.id === late.id)!.adoptionProbability;
	const weightedAdoption = (2 * early.adoptionProbability + lateAdoption) / 3;
	for (const [coverage, horizon] of [
		[0, 0],
		[0.5, 0],
		[0.8, 1],
		[1, 1],
	] as const) {
		const p = open(coverage);
		expect(p).toMatchObject({
			horizon,
			latestHorizon: 1,
			patternID: early.id,
			supportingPatternIDs: [early.id],
		});
		expect(p.conditionalProbability).toBeCloseTo(1);
		expect(p.empiricalProbability).toBeCloseTo(0.5);
		expect(p.adoptionProbability).toBeCloseTo(weightedAdoption);
		expect(p.confidence).toBeCloseTo(0.5 * weightedAdoption);
		expect(p.continuation).toMatchObject({
			visitedPatternIDs: [early.id],
			pathProbability: p.empiricalProbability,
		});
	}
	for (let i = 0; i < 6; i++) train("both" + i, ["open", "open"]);
	expect(open(1).conditionalProbability).toBe(1);
	for (let i = 0; i < 5; i++) store.settled(late.id, { ...accepted, match: { matched: false } });
	expect(open(1)).toMatchObject({ horizon: 0, latestHorizon: 0, supportingPatternIDs: [early.id] });
});

function learnPortablePattern(store: PatternAwareStore, tool: string, count: number) {
	for (let i = 0; i < count; i++) {
		const id = tool + i;
		const target = id + ".ts";
		store.observe(event(id, tool, {}, { outputPaths: [target] }));
		store.observe(event(id, "open", { path: target }));
		store.finishSession(id);
	}
	return store.snapshot().find((p) => p.context[0]?.tool === tool)!.id;
}

test("ranks portable evidence on its source clock under shared pool pressure", async () => {
	const root = await directories.create();
	const portable = path.join(root, "portable.json");
	const settings = { ...immediate, maxPatterns: 1, decayHalfLifeEvents: 16 };
	const low = new PatternAwareStore(settings, path.join(root, "low.json"), undefined, portable);
	const high = new PatternAwareStore(settings, path.join(root, "high.json"), undefined, portable);
	await Promise.all([low.load(), high.load()]);
	for (let i = 0; i < 512; i++) high.observeTurn();

	const strong = learnPortablePattern(low, "frequent", 8);
	for (let i = 0; i < 3; i++) low.settled(strong, accepted);
	await low.flush();
	const weak = learnPortablePattern(high, "occasional", 3);
	for (let i = 0; i < 3; i++) high.settled(weak, accepted);
	await high.flush();
	const reader = new PatternAwareStore(settings, undefined, undefined, portable);
	await reader.load();
	expect(reader.snapshot().map((p) => p.id)).toEqual([strong]);
});

test("checks portable confusion on the source clock and rebases aged feedback on import", async () => {
	const root = await directories.create();
	const workspace = path.join(root, "source.json");
	const portable = path.join(root, "portable.json");
	const settings = { ...immediate, decayHalfLifeEvents: 8 };
	const source = new PatternAwareStore(settings, workspace);
	await source.load();
	const id = learnPortablePattern(source, "locate", 6);
	for (let i = 0; i < 8; i++) source.settled(id, accepted);
	for (let i = 0; i < 80; i++) source.observeTurn();
	for (let i = 0; i < 3; i++) source.settled(id, { ...accepted, match: { matched: false } });
	const copyEvidence = async () => {
		await source.flush();
		const { patterns } = JSON.parse(await readFile(workspace, "utf8"));
		await writeFile(portable, JSON.stringify({ patterns }));
	};
	await copyEvidence();
	const high = new PatternAwareStore(settings, path.join(root, "high.json"), undefined, portable);
	for (let i = 0; i < 512; i++) high.observeTurn();
	await high.load();
	expect(high.snapshot()).toEqual([]);
	const local = learnPortablePattern(high, "inspect", 4);
	for (let i = 0; i < 3; i++) high.settled(local, accepted);
	await high.flush();
	const merged = JSON.parse(await readFile(portable, "utf8")).patterns;
	expect(merged.map((p: { id: string }) => p.id)).toEqual([local]);

	for (let i = 0; i < 80; i++) source.observeTurn();
	learnPortablePattern(source, "locate", 4);
	await copyEvidence();
	const fresh = new PatternAwareStore(settings, undefined, undefined, portable);
	await fresh.load();
	const candidate = fresh.predictAfterBatch("held", [
		event("held", "locate", {}, { outputPaths: ["new.ts"] }),
	])[0];
	expect(candidate?.patternID).toBe(id);
	expect(candidate?.background).toBeFalsy();
});
