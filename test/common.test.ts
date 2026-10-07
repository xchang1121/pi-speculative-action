import path from "node:path";
import { describe, expect, it } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { actionKeyCovers, actionKeyMatch, actionKeyMatches, actionKeyProjectionPartitions,
	buildActionKey, buildPiActionKey, inferredActionEffect } from "../src/action-semantics.ts";
import { clampCandidateLimit, DEFAULTS, drafterRequestTemperature, normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection } from "../src/common.ts";
import { candidateToolNames } from "../src/runtime.ts";

describe("speculative action common", () => {
	it("normalizes configured request counts without hidden upper bounds", () => {
		expect(DEFAULTS.candidateLimit).toBe(2);
		expect(clampCandidateLimit(0)).toBe(1);
		expect(clampCandidateLimit(4.9)).toBe(4);
		expect(clampCandidateLimit(100)).toBe(100);
		expect(clampCandidateLimit("4")).toBe(1);
	});

	it("stratifies configurable Drafter sampling for arbitrary candidate counts", () => {
		const baseline = normalizeDrafterRequestSettings(undefined);
		expect(baseline.drafterMaxTokens).toBe(4096);
		expect([0, 1, 2].map((index) => drafterRequestTemperature(index, 3, baseline))).toEqual([0, 0.7, 0.7]);
		const diverse = normalizeDrafterRequestSettings({
			drafterMaxDepth: 3,
			drafterMaxTokens: 256,
			drafterDeterministicCandidates: 1,
			drafterTemperatureMin: 0.4,
			drafterTemperatureMax: 1.6,
		});
		expect(diverse.drafterMaxDepth).toBe(3);
		expect(diverse.drafterMaxTokens).toBe(256);
		expect(drafterRequestTemperature(0, 1, diverse)).toBe(0);
		expect([0, 1, 2].map((index) => drafterRequestTemperature(index, 3, diverse))).toEqual([0, 0.4, 1.6]);
		expect(drafterRequestTemperature(1, 2, diverse)).toBe(1);
		expect(drafterRequestTemperature(100, 101, diverse)).toBeCloseTo(1.6);
		expect(
			normalizeDrafterRequestSettings({
				drafterMaxDepth: -1,
				drafterMaxTokens: 0,
				drafterDeterministicCandidates: -1,
				drafterTemperatureMin: 2,
				drafterTemperatureMax: 0.5,
				drafterPatternHints: "yes",
			}),
		).toEqual({
			drafterMaxDepth: DEFAULTS.drafterMaxDepth,
			drafterMaxTokens: DEFAULTS.drafterMaxTokens,
			drafterTaskMaxRequests: DEFAULTS.drafterTaskMaxRequests,
			drafterTaskMaxTokens: DEFAULTS.drafterTaskMaxTokens,
			drafterDeterministicCandidates: DEFAULTS.drafterDeterministicCandidates,
			drafterTemperatureMin: 0.5,
			drafterTemperatureMax: 2,
			drafterPatternHints: false,
		});
	});

	it("builds stable, conflict-sensitive keys independently of isolation routing", () => {
		const bash = buildPiActionKey("bash", { command: "npm test", timeout: 30 }, "/workspace/a");
		const otherCwd = buildPiActionKey("bash", { command: "npm test", timeout: 30 }, "/workspace/b");
		const write = buildPiActionKey("write", { path: "src/out.ts", content: "one\n" }, "/workspace");
		const otherWrite = buildPiActionKey("write", { path: "src/out.ts", content: "two\n" }, "/workspace");
		const edit = buildPiActionKey("edit", { path: "src/out.ts", edits: [{ oldText: "one", newText: "two" }] }, "/workspace");
		const sameEdit = buildPiActionKey("edit", { path: "src/out.ts", edits: [{ newText: "two", oldText: "one" }] }, "/workspace");

		expect(bash).toMatchObject({ tool: "bash", resources: [path.resolve("/workspace/a").replaceAll("\\", "/")] });
		expect(bash?.key).not.toBe(otherCwd?.key);
		expect(write).toMatchObject({ tool: "write", resources: ["src/out.ts"] });
		expect(write?.key).not.toBe(otherWrite?.key);
		expect(edit?.key).toBe(sameEdit?.key);
		expect(buildPiActionKey("write", { path: "../outside", content: "no" }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("edit", { path: "file", edits: [] }, "/workspace")).toBeUndefined();
	});

	it("namespaces K(a) by semantics and schema while normalizing only equivalent inputs", () => {
		const input = { tool: "custom", resources: ["resource"], input: { alpha: 1, beta: 2 }, schemaHash: "schema-a" };
		const base = buildActionKey(input);
		expect(buildActionKey({ ...input, input: { beta: 2, alpha: 1 } }).key).toBe(base.key);
		expect(new Set([
			base.key, buildActionKey({ ...input, semanticsEpoch: "custom-other" }).key,
			buildActionKey({ ...input, schemaHash: "schema-b" }).key,
		]).size).toBe(3);
		expect(buildActionKey({ ...input, schemaHash: undefined }).key).toBe(buildActionKey({ ...input, schemaHash: "" }).key);
		expect(base.schemaHash).toBe("schema-a");
	});

	it("matches directed read coverage only inside the same execution contract", () => {
		const projectors = [READ_RANGE_ACTION_KEY_PROJECTOR];
		const read = (input: Record<string, unknown>, schema = "schema-a") =>
			buildPiActionKey("read", { path: "src/runtime.ts", ...input }, "/workspace", schema)!;
		const implicit = read({}), explicit = read({ offset: 1, limit: 2000 });
		expect(implicit.key).not.toBe(explicit.key);
		expect(actionKeyMatch(implicit, explicit, projectors)).toMatchObject({ kind: "projected", projector: "read.range" });
		expect(implicit).toMatchObject({ tool: "read", semanticsEpoch: "pi.read", schemaHash: "schema-a", resources: ["src/runtime.ts"] });
		expect(implicit.input).not.toHaveProperty("limit");
		expect(explicit.input).toHaveProperty("limit", 2000);
		expect(implicit.key).toContain('"semanticsEpoch":"pi.read"');
		for (const value of [implicit, implicit.input, implicit.resources]) expect(Object.isFrozen(value)).toBe(true);
		const broad = read({ offset: 100, limit: 160 }), narrow = read({ offset: 220, limit: 30 });
		for (const [input, matches, covers] of [
			[{ offset: 220, limit: 30 }, true, true],
			[{ offset: 250, limit: 30 }, true, false],
			[{ offset: 261, limit: 1 }, false, false],
			[{ offset: 80, limit: 30 }, false, false],
		] as const) {
			const actor = read(input);
			expect(actionKeyMatches(broad, actor, projectors)).toBe(matches);
			expect(actionKeyCovers(broad, actor, projectors)).toBe(covers);
		}
		expect(actionKeyMatches(broad, narrow)).toBe(false);
		expect(actionKeyMatches(narrow, narrow)).toBe(true);
		expect(actionKeyCovers(read({}), read({ limit: 2 }), projectors)).toBe(true);
		expect(actionKeyCovers(read({ limit: 2 }), read({}), projectors)).toBe(false);
		expect(actionKeyMatch(broad, narrow, projectors)).toEqual({ kind: "projected", projector: "read.range", distance: 130 });
		expect(actionKeyMatch(narrow, broad, projectors)).toBeUndefined();
		expect(actionKeyProjectionPartitions(broad, projectors)).toEqual(actionKeyProjectionPartitions(narrow, projectors));
		const otherPath = read({ path: "secret-name.ts", offset: 220, limit: 30 });
		const otherSchema = read(narrow.input, "schema-b");
		const otherExecutor = buildActionKey({ tool: "read", resources: narrow.resources, input: narrow.input,
			schemaHash: "schema-a", semanticsEpoch: broad.semanticsEpoch, executionFingerprint: "other-executor" });
		for (const key of [otherPath, otherSchema, otherExecutor, buildPiActionKey("grep", { pattern: "private-pattern" }, "/workspace", "schema-a")!])
			expect(actionKeyMatch(broad, key, projectors)).toBeUndefined();
		expect(actionKeyMatches(broad, otherSchema, projectors)).toBe(false);
		expect(actionKeyProjectionPartitions(broad, projectors)).not.toEqual(actionKeyProjectionPartitions(otherSchema, projectors));
		for (const tool of ["grep", "find"]) {
			const wide = buildPiActionKey(tool, { pattern: "TODO", limit: 100 }, "/workspace", "schema-a")!;
			const short = buildPiActionKey(tool, { pattern: "TODO", limit: 10 }, "/workspace", "schema-a")!;
			expect(actionKeyMatches(wide, short, projectors)).toBe(false);
		}
		expect(actionKeyMatches({ ...broad, key: "opaque" }, narrow, projectors)).toBe(true);
		expect(actionKeyMatches({ ...broad, key: "opaque", input: { ...broad.input, path: "other.ts" } }, narrow, projectors)).toBe(false);
	});

	it("selects prediction tools independently from their execution route", () => {
		expect(DEFAULTS.tools).toEqual(["read", "grep", "find", "ls", "bash", "write", "edit"]);
		expect(candidateToolNames({ ...DEFAULTS, enabled: true, tools: ["read", "bash", "write"] })).toEqual(["read", "bash", "write"]);
		expect(inferredActionEffect("read")).toBe("observation");
		expect(inferredActionEffect("bash")).toBe("unbounded");
		expect(inferredActionEffect("edit")).toBe("workspace_mutation");
		expect(normalizeSpeculativeToolSelection(["bash", "read", "bash", "unknown"])).toEqual(["bash", "read"]);
		expect(normalizeSpeculativeToolSelection(undefined)).toEqual(DEFAULTS.tools);
		expect(normalizeSpeculativeToolSelection(["custom", "read", "bash", "custom"], ["read", "custom"]))
			.toEqual(["custom", "read"]);
		for (const input of [[], { resourceCached: ["read"], sandbox: [], predictionOnly: [] }, ["read", 1], null]) {
			expect(normalizeSpeculativeToolSelection(input)).toEqual([]);
		}
	});

});
