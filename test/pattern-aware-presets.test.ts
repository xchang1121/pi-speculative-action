import { describe, expect, test } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareActionSemantics, patternAwareSettings, PATTERN_AWARE_DEFAULTS, PATTERN_AWARE_PRESETS,
	type PatternAwareEventInput, type PatternAwarePresetID } from "../src/pattern-aware.ts";
import { createPatternPlanSource } from "../src/pattern-plan-source.ts";
import type { AgentPlanSource } from "../src/agent-runtime-types.ts";
import type { ExecutionOperationBinding } from "../src/execution-world.ts";
import { testModel } from "./model.ts";
import { textResult } from "./result.ts";
import { unmatchedSettlement } from "./prediction.ts";

function fixture(preset: PatternAwarePresetID, exists?: (target: string) => boolean) {
	const settings = patternAwareSettings({ presets: [preset] });
	const store = new PatternAwareStore(settings, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, "/workspace", [], exists));
	let turn = 0;
	return {
		store, settings,
		observe(tool: string, input: Record<string, unknown>, extra: Partial<PatternAwareEventInput> = {}) {
			store.observe({ sessionID: "session", turnID: `turn-${++turn}`, tool, input, outcome: "success", durationMs: 10, ...extra });
		},
		predict(schemaHashes: Record<string, string> = {}) { return store.predict("session", schemaHashes).filter(item => item.presetID === preset); },
	};
}

function sourceFixture(preset: PatternAwarePresetID) {
	const observed = fixture(preset, () => true);
	const controller = createPatternPlanSource({ sessionID: "session", cwd: "/workspace", store: observed.store,
		actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
	const request: Parameters<AgentPlanSource["propose"]>[0] = {
		startInput: { sessionID: "session", turnID: "producer", actorModel: testModel(), actorOptions: undefined,
			context: { systemPrompt: "", messages: [], tools: [] }, tools: [] },
		data: { tools: new Map(), schemaHashes: {} },
		settings: { enabled: true, resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000, tools: ["read", "bash"],
			sourceConfig: { patternAware: { ...observed.settings, multiStepEnabled: false } } },
		definitions: [], candidateNames: ["read", "bash"], proposalIndex: 0, proposalCount: 1, signal: new AbortController().signal,
	};
	return { ...observed, controller, request };
}

describe("observed search presets", () => {
	test("adds independently selectable modes without changing saved selections or defaults", () => {
		expect(PATTERN_AWARE_DEFAULTS.presets).toEqual(["reported-files", "edited-file", "recent-reads", "recent-command"]);
		for (const id of ["recheck-search", "result-neighbors"] as const) {
			expect(PATTERN_AWARE_PRESETS.find(preset => preset.id === id)?.defaultOff).toBe(true);
			expect(patternAwareSettings({ presets: [id] }).presets).toEqual([id]);
		}
		expect(patternAwareSettings({ presets: ["reported-lines", "recent-command"] }).presets).toEqual(["recent-command", "reported-lines"]);
	});

	test("rechecks an observed match with the original query, flags, schema and measured cost", () => {
		const { store, settings, observe, predict } = fixture("recheck-search");
		const query = { pattern: "old.name", path: "src", glob: "*.ts", literal: true, ignoreCase: true, context: 3, limit: 23 };
		observe("grep", query, { schemaHash: "grep-v1", outputPaths: ["./src/../src/a.ts"], durationMs: 1500 });
		observe("edit", { path: "src/a.ts", oldText: "old.name", newText: "new.name" }, { learnTarget: false });
		const candidate = predict({ grep: "grep-v1" })[0]!;
		expect(candidate).toMatchObject({ tool: "grep", input: query, presetID: "recheck-search", patternID: "structural:recheck-search" });
		expect(predict()).toEqual([]);
		expect(predict({ grep: "grep-v2" })).toEqual([]);
		expect(store.predict("session", { grep: "grep-v1" }, { ...settings, presets: [] }).some(item => item.presetID)).toBe(false);
		store.issued(candidate.continuation);
		store.settled(candidate.continuation, unmatchedSettlement());
		expect(predict({ grep: "grep-v1" })[0]!.conditionalProbability).toBeLessThan(candidate.conditionalProbability);
		// The proposal owns its input and cannot mutate the observed query used by subsequent proposals.
		candidate.input.pattern = "changed";
		expect(predict({ grep: "grep-v1" })[0]!.input).toEqual(query);
	});

	test("requires a successful local edit and an unshortened, successful search reporting that file", () => {
		for (const mode of ["failed-edit", "failed-search", "other-file", "other-session", "unreported", "shortened", "unlearnable"] as const) {
			const { observe, predict } = fixture("recheck-search");
			observe("grep", { pattern: mode === "shortened" ? "x".repeat(5000) : "name", path: "src" }, {
				outputPaths: mode === "unreported" ? [] : ["src/a.ts"],
				outcome: mode === "failed-search" ? "failure" : "success",
				...(mode === "other-session" ? { sessionID: "other" } : {}),
				...(mode === "unlearnable" ? { learnTarget: false } : {}),
			});
			observe("edit", { path: mode === "other-file" ? "src/b.ts" : "src/a.ts" }, {
				outcome: mode === "failed-edit" ? "failure" : "success", learnTarget: false,
			});
			expect(predict(), mode).toEqual([]);
		}
	});

	test("selects the latest matching search, emits one, and consumes a real repeat after the edit", () => {
		for (const outcome of ["success", "failure"] as const) {
			const { observe, predict } = fixture("recheck-search");
			observe("grep", { pattern: "older", path: "src" }, { outputPaths: ["src/a.ts"] });
			observe("grep", { pattern: "newer", path: "src" }, { outputLocations: [{ path: "src/a.ts", line: 20 }] });
			observe("write", { path: "src/a.ts", content: "newer name" }, { turnID: "edited", learnTarget: false });
			expect(predict().map(item => item.input)).toEqual([{ pattern: "newer", path: "src" }]);
			observe("grep", { pattern: "newer", path: "./src" }, { turnID: "edited", outcome });
			expect(predict()).toEqual([]);
		}
	});

	test("reads only two unread canonical peers from the latest result containing the current read", () => {
		const { observe, predict, store, settings } = fixture("result-neighbors");
		observe("find", { pattern: "*.ts" }, { outputPaths: ["src/a.ts", "src/old.ts"] });
		observe("grep", { pattern: "name" }, { outputPaths: ["src/a.ts", "./src/a.ts", "src/b.ts", "src/c.ts", "./src/c.ts", "src/d.ts", "src/e.ts"] });
		observe("read", { path: "src/b.ts" }, { learnTarget: false });
		observe("read", { path: "./src/a.ts" }, { learnTarget: false });
		expect(predict().map(item => item.input)).toEqual([{ path: "src/c.ts" }, { path: "src/d.ts" }]);
		expect(store.predict("session", {}, { ...settings, presets: ["reported-files"] }).some(item => item.presetID === "reported-files")).toBe(false);
		expect(store.predict("session", {}, { ...settings, presets: [] }).some(item => item.presetID)).toBe(false);
	});

	test("requires same-session successful search evidence and a successful anchor read", () => {
		for (const mode of ["failed-search", "failed-read", "other-session", "unrelated", "command-output"] as const) {
			const { observe, predict } = fixture("result-neighbors");
			observe(mode === "command-output" ? "bash" : "find", { pattern: "*.ts" }, {
				outputPaths: [mode === "unrelated" ? "src/other.ts" : "src/a.ts", "src/b.ts"],
				outcome: mode === "failed-search" ? "failure" : "success", ...(mode === "other-session" ? { sessionID: "other" } : {}),
			});
			observe("read", { path: "src/a.ts" }, { outcome: mode === "failed-read" ? "failure" : "success", learnTarget: false });
			expect(predict(), mode).toEqual([]);
		}
	});

	test("uses existing file eligibility and stops after peers have been read", () => {
		const { observe, predict } = fixture("result-neighbors", target => !target.endsWith("missing.ts"));
		observe("grep", { pattern: "name" }, { outputLocations: [{ path: "src/a.ts", line: 1 }, { path: "src/missing.ts", line: 2 }, { path: "src/b.ts", line: 3 }] });
		observe("read", { path: "src/a.ts" }, { learnTarget: false });
		expect(predict().map(item => item.input)).toEqual([{ path: "src/b.ts" }]);
		observe("read", { path: "src/b.ts" }, { learnTarget: false });
		expect(predict()).toEqual([]);
	});
});

describe("preset command preparation", () => {


	test.for(["recent-command", "retry-failed-command"] as const)("prepares %s from current workspace changes", async preset => {
		const { controller, request } = sourceFixture(preset), command = { command: "build" };
		const schemaHashes: Readonly<Record<string, string>> = { bash: "schema", write: "schema" };
		const native = preset === "recent-command", action = PI_ACTION_SEMANTICS.buildKey("bash", command, "/workspace", schemaHashes.bash)!;
		const operation: ExecutionOperationBinding = { backend: "test", identity: "worker", permissionHash: action.hash, available: true,
			executionMs: 500, preparation: "current_workspace", stale: async () => true };
		const observe = (tool: string, concrete: Record<string, unknown>, isError = false) => controller.source.observe!({ ...request, data: { ...request.data, schemaHashes },
			consumeInput: { sessionID: "session", turnID: "producer", tool, args: concrete, tools: [] },
			action: PI_ACTION_SEMANTICS.buildKey(tool, concrete, "/workspace", schemaHashes[tool])!, tool, concrete, order: 0, durationMs: 500,
			output: { result: textResult(isError ? "src/a.ts:1: build failed" : "done"), isError }, ...(tool === "bash" && native ? { operations: [operation] } : {}) });
		try {
			await observe("bash", command, !native);
			const first = await observe("write", { path: "src/a.ts", content: "changed" });
			if (!first || !("actions" in first)) throw new Error("missing command preparation");
			const prepared = first.actions[0]!;
			expect(prepared.mode).toBe(preset);
			await controller.source.onAdmitted!({ proposalID: first.id, actionID: prepared.id, feedback: prepared.feedback });
			const next = await observe("write", { path: "src/a.ts", content: "changed again" });
			if (!next || !("actions" in next)) throw new Error("missing next command preparation");
			expect(next.actions[0]!.mode).toBe(preset);
		} finally { await controller.dispose(); }
	});
});
