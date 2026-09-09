import path from "node:path";
import { describe, expect, it } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import {
	type ActionKeyProjector,
	actionKeyCovers,
	actionKeyMatch,
	actionKeyMatches,
	actionKeyMismatchReason,
	actionKeyProjectionPartitions,
	buildActionKey,
	buildPiActionKey,
	inferredActionEffect,
} from "../src/action-semantics.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	drafterRequestTemperature,
	normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection,
} from "../src/common.ts";
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
		expect(baseline.drafterMaxTokens).toBeUndefined();
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
			}),
		).toEqual({
			drafterMaxDepth: DEFAULTS.drafterMaxDepth,
			drafterDeterministicCandidates: DEFAULTS.drafterDeterministicCandidates,
			drafterTemperatureMin: 0.5,
			drafterTemperatureMax: 2,
		});
	});

	it("builds stable, conflict-sensitive keys independently of isolation routing", () => {
		const bash = buildPiActionKey("bash", { command: "npm test", timeout: 30 }, "/workspace/a");
		const otherCwd = buildPiActionKey("bash", { command: "npm test", timeout: 30 }, "/workspace/b");
		const write = buildPiActionKey("write", { path: "src/out.ts", content: "one\n" }, "/workspace");
		const otherWrite = buildPiActionKey("write", { path: "src/out.ts", content: "two\n" }, "/workspace");
		const edit = buildPiActionKey(
			"edit",
			{ path: "src/out.ts", edits: [{ oldText: "one", newText: "two" }] },
			"/workspace",
		);
		const sameEdit = buildPiActionKey(
			"edit",
			{ path: "src/out.ts", edits: [{ newText: "two", oldText: "one" }] },
			"/workspace",
		);

		expect(bash).toMatchObject({
			tool: "bash",
			resources: [path.resolve("/workspace/a").replaceAll("\\", "/")],
		});
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
			base.key, buildActionKey({ ...input, semanticsEpoch: "custom.v2" }).key,
			buildActionKey({ ...input, schemaHash: "schema-b" }).key,
		]).size).toBe(3);
		expect(buildActionKey({ ...input, schemaHash: undefined }).key).toBe(buildActionKey({ ...input, schemaHash: "" }).key);
		expect(base.schemaHash).toBe("schema-a");
	});

	it("matches directed read coverage and classifies incompatible keys without exposing inputs", () => {
		const projectors = [READ_RANGE_ACTION_KEY_PROJECTOR];
		const read = (input: Record<string, unknown>, schema = "schema-a") =>
			buildPiActionKey("read", { path: "src/runtime.ts", ...input }, "/workspace", schema)!;
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
		expect(actionKeyMatch(broad, narrow, projectors)).toEqual({ kind: "projected", projector: "read.range", distance: 130 });
		expect(actionKeyMismatchReason(broad, narrow, projectors)).toBeUndefined();
		expect(actionKeyMismatchReason(narrow, narrow, projectors)).toBeUndefined();
		expect(actionKeyMismatchReason(narrow, broad, projectors)).toBe("projection_not_applicable");
		expect(actionKeyMismatchReason(narrow, broad)).toBe("different_core");
		expect(actionKeyProjectionPartitions(broad, projectors)).toEqual(actionKeyProjectionPartitions(narrow, projectors));
		const otherPath = read({ path: "secret-name.ts", offset: 220, limit: 30 });
		const otherSchema = read(narrow.input, "schema-b");
		const otherExecutor = buildActionKey({ tool: "read", resources: narrow.resources, input: narrow.input,
			schemaHash: "schema-a", semanticsEpoch: broad.semanticsEpoch, executionFingerprint: "other-executor" });
		for (const [key, reason] of [
			[otherPath, "different_core"], [otherSchema, "different_schema"], [otherExecutor, "different_executor"],
			[buildPiActionKey("grep", { pattern: "private-pattern" }, "/workspace", "schema-a")!, "different_tool"],
		] as const) expect(actionKeyMismatchReason(broad, key, projectors)).toBe(reason);
		expect(actionKeyMatches(broad, otherSchema, projectors)).toBe(false);
		expect(actionKeyProjectionPartitions(broad, projectors)).not.toEqual(actionKeyProjectionPartitions(otherSchema, projectors));
		expect(JSON.stringify(actionKeyMismatchReason(broad, otherPath, projectors))).not.toContain("secret-name");
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
		expect(
			candidateToolNames({
				enabled: true,
				candidateLimit: 4,
				maxConcurrentActions: 4,
				resourceCacheMaxEntries: 8,
				predictionTimeoutMs: 100,
				tools: ["read", "bash", "write"],
			}),
		).toEqual(["read", "bash", "write"]);
		expect(inferredActionEffect("read")).toBe("observation");
		expect(inferredActionEffect("bash")).toBe("unbounded");
		expect(inferredActionEffect("edit")).toBe("workspace_mutation");
		expect(normalizeSpeculativeToolSelection(["bash", "read", "bash", "unknown"])).toEqual(["bash", "read"]);
		expect(
			normalizeSpeculativeToolSelection({
				resourceCached: ["read"],
				sandbox: ["write"],
				predictionOnly: ["bash"],
			}),
		).toEqual(["read", "write", "bash"]);
	});

	it("defines equivalence through an injected projection without changing K(a)", () => {
		const projector: ActionKeyProjector = {
			id: "custom.subset",
			partition: (action) =>
				action.tool === "custom"
					? JSON.stringify([action.schemaHash, action.resources, action.input.namespace])
					: undefined,
			project: (speculative, actor) => {
				const speculativeValues = Array.isArray(speculative.input.values) ? speculative.input.values : undefined;
				const actorValues = Array.isArray(actor.input.values) ? actor.input.values : undefined;
				if (!speculativeValues || !actorValues) return undefined;
				if (!actorValues.every((value) => speculativeValues.includes(value))) return undefined;
				return {
					action: buildActionKey({
						tool: speculative.tool,
						resources: speculative.resources,
						schemaHash: speculative.schemaHash,
						input: { ...speculative.input, values: actorValues },
					}),
					distance: speculativeValues.length - actorValues.length,
				};
			},
			canShareInFlight: (speculative, actor) => {
				const speculativeValues = Array.isArray(speculative.input.values) ? speculative.input.values : undefined;
				const actorValues = Array.isArray(actor.input.values) ? actor.input.values : undefined;
				return (
					!!speculativeValues && !!actorValues && actorValues.every((value) => speculativeValues.includes(value))
				);
			},
		};
		const speculative = buildActionKey({
			tool: "custom",
			resources: ["set"],
			input: { namespace: "items", values: ["a", "b", "c"] },
		});
		const actor = buildActionKey({
			tool: "custom",
			resources: ["set"],
			input: { namespace: "items", values: ["b", "c"] },
		});

		expect(actionKeyMatch(speculative, actor, [projector])).toEqual({
			kind: "projected",
			projector: "custom.subset",
			distance: 1,
		});
		expect(actionKeyCovers(speculative, actor, [projector])).toBe(true);
		const unguarded: ActionKeyProjector = {
			id: "unguarded",
			partition: projector.partition,
			project: projector.project,
		};
		expect(actionKeyCovers(speculative, actor, [unguarded])).toBe(false);
		expect(
			actionKeyCovers(speculative, actor, [{ ...projector, id: "guarded", canShareInFlight: () => false }]),
		).toBe(false);
		expect(
			actionKeyCovers(speculative, actor, [
				{
					...projector,
					id: "throwing-guard",
					canShareInFlight: () => {
						throw new Error("coverage failed");
					},
				},
			]),
		).toBe(false);
		const broken: ActionKeyProjector = {
			id: "broken",
			partition: () => {
				throw new Error("partition failed");
			},
			project: () => {
				throw new Error("projection failed");
			},
		};
		expect(actionKeyMatch(speculative, actor, [broken])).toBeUndefined();
		expect(actionKeyProjectionPartitions(speculative, [broken])).toEqual([]);
	});


});
