import { describe, expect, it } from "vitest";
import {
	type ActionKeyProjector,
	type ActionSemanticsDefinition,
	ActionSemanticsRegistry,
	actionKeyMatch,
	actionKeyMismatchReason,
	buildActionKey,
	buildPiActionKey,
	isProjectedActionKeyMatch,
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
	PI_ACTION_SEMANTICS,
	READ_RANGE_ACTION_KEY_PROJECTOR,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "../src/action-semantics.ts";

describe("ActionSemanticsRegistry", () => {
	it("defines K(a), resource evidence, and effects without choosing an execution backend", () => {
		expect(PI_ACTION_SEMANTICS.toolNames()).toEqual(["read", "grep", "find", "ls", "bash", "write", "edit"]);
		expect(KEYABLE_TOOLS).toEqual(PI_ACTION_SEMANTICS.toolNames());
		expect(OBSERVATION_ACTION_TOOLS).toEqual(["read", "grep", "find", "ls"]);
		expect(WORKSPACE_MUTATION_ACTION_TOOLS).toEqual(["write", "edit"]);
		expect(UNBOUNDED_ACTION_TOOLS).toEqual(["bash"]);

		expect(PI_ACTION_SEMANTICS.definition("read")).toMatchObject({
			effect: "observation",
			resourceScope: "content",
		});
		expect(PI_ACTION_SEMANTICS.definition("ls")).toMatchObject({
			effect: "observation",
			resourceScope: "tree_entries",
		});
		expect(PI_ACTION_SEMANTICS.definition("bash")).toMatchObject({
			effect: "unbounded",
		});
		expect(PI_ACTION_SEMANTICS.definition("write")).toMatchObject({
			effect: "workspace_mutation",
		});
		expect(PI_ACTION_SEMANTICS.resourceScope("write")).toBeUndefined();
	});

	it("canonicalizes equivalent ls defaults and rejects unstable views", () => {
		const implicit = buildPiActionKey("ls", {}, "/workspace");
		const explicit = buildPiActionKey("ls", { path: ".", limit: 500 }, "/workspace");

		expect(implicit?.key).toBe(explicit?.key);
		expect(implicit).toMatchObject({
			tool: "ls",
			semanticsEpoch: "pi.ls.v1",
			resources: ["."],
			input: { path: ".", limit: 500 },
		});
		expect(buildPiActionKey("ls", { path: "../outside" }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("ls", { limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("ls", { limit: 1.5 }, "/workspace")).toBeUndefined();
	});

	it("keeps read's omitted-limit view distinct inside its versioned K(a)", () => {
		const implicit = buildPiActionKey("read", { path: "src/a.ts" }, "/workspace", "schema-v1");
		const explicit = buildPiActionKey(
			"read",
			{ path: "src/a.ts", offset: 1, limit: 2000 },
			"/workspace",
			"schema-v1",
		);

		expect(implicit?.key).not.toBe(explicit?.key);
		const relation = implicit && explicit ? actionKeyMatch(implicit, explicit, [READ_RANGE_ACTION_KEY_PROJECTOR]) : undefined;
		expect(relation).toMatchObject({ kind: "projected", projector: "read.range" });
		expect(relation && isProjectedActionKeyMatch(relation)).toBe(true);
		expect(isProjectedActionKeyMatch({ kind: "exact", distance: 0 })).toBe(false);
		expect(implicit).toMatchObject({
			tool: "read",
			semanticsEpoch: "pi.read.v2",
			schemaHash: "schema-v1",
			resources: ["src/a.ts"],
		});
		expect(implicit?.input).not.toHaveProperty("limit");
		expect(explicit?.input).toHaveProperty("limit", 2000);
		expect(implicit?.key).toContain('"semanticsEpoch":"pi.read.v2"');
		expect(Object.isFrozen(implicit)).toBe(true);
		expect(Object.isFrozen(implicit?.input)).toBe(true);
		expect(Object.isFrozen(implicit?.resources)).toBe(true);
	});

	it("fails closed instead of folding unsupported numeric query views into valid keys", () => {
		expect(buildPiActionKey("read", { path: "a.ts", offset: 1.5 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("read", { path: "a.ts", limit: -1 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("grep", { pattern: "x", context: 0.5 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("grep", { pattern: "x", limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("find", { pattern: "*", limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("find", { pattern: "*", limit: 1.5 }, "/workspace")).toBeUndefined();
	});

	it("never equates identical inputs across different tool-semantics epochs", () => {
		const oldAction = buildActionKey({
			tool: "read",
			resources: ["a.ts"],
			input: { path: "a.ts", offset: 1, limit: 20 },
			schemaHash: "schema",
			semanticsEpoch: "read.v1",
		});
		const newAction = buildActionKey({
			tool: "read",
			resources: ["a.ts"],
			input: { path: "a.ts", offset: 1, limit: 20 },
			schemaHash: "schema",
			semanticsEpoch: "read.v2",
		});

		expect(actionKeyMatch(oldAction, newAction, [READ_RANGE_ACTION_KEY_PROJECTOR])).toBeUndefined();
		expect(actionKeyMismatchReason(oldAction, newAction, [READ_RANGE_ACTION_KEY_PROJECTOR])).toBe(
			"different_semantics",
		);
	});

	it("never equates identical actions resolved to different execution backends", () => {
		const first = PI_ACTION_SEMANTICS.buildKey("bash", { command: "echo $VALUE" }, "/workspace", "schema", {
			fingerprint: "local:env-a",
		});
		const second = PI_ACTION_SEMANTICS.buildKey("bash", { command: "echo $VALUE" }, "/workspace", "schema", {
			fingerprint: "local:env-b",
		});

		expect(first).toBeDefined();
		expect(second).toBeDefined();
		if (!first || !second) throw new Error("Expected keyed bash actions");
		expect(actionKeyMatch(first, second)).toBeUndefined();
		expect(actionKeyMismatchReason(first, second)).toBe("different_executor");
	});

	it("keeps every projection inside the immutable semantic envelope", () => {
		const permissive: ActionKeyProjector = {
			id: "permissive",
			partition: () => "all",
			project: (_speculative, actor) => ({ action: actor, distance: 1 }),
		};
		const base = buildActionKey({
			tool: "read",
			resources: ["a.ts"],
			input: { path: "a.ts", offset: 1 },
			semanticsEpoch: "read.v1",
			schemaHash: "schema.v1",
			executionFingerprint: "executor.v1",
		});
		const sameEnvelope = buildActionKey({
			...base,
			resources: ["a.ts"],
			input: { path: "a.ts", offset: 2 },
		});
		expect(actionKeyMatch(base, sameEnvelope, [permissive])).toMatchObject({
			kind: "projected",
			projector: "permissive",
		});

		for (const actor of [
			buildActionKey({ ...base, tool: "grep", resources: ["a.ts"], input: { path: "a.ts", offset: 2 } }),
			buildActionKey({
				...base,
				semanticsEpoch: "read.v2",
				resources: ["a.ts"],
				input: { path: "a.ts", offset: 2 },
			}),
			buildActionKey({ ...base, schemaHash: "schema.v2", resources: ["a.ts"], input: { path: "a.ts", offset: 2 } }),
			buildActionKey({
				...base,
				executionFingerprint: "executor.v2",
				resources: ["a.ts"],
				input: { path: "a.ts", offset: 2 },
			}),
		]) {
			expect(actionKeyMatch(base, actor, [permissive])).toBeUndefined();
		}
	});

	it("supports a new host tool with one semantics definition", () => {
		const registry = new ActionSemanticsRegistry([
			resourceDefinition("stat", "host.stat.v1", (input) => {
				if (!input || typeof input !== "object" || !("path" in input) || typeof input.path !== "string") {
					return undefined;
				}
				return { input: { path: input.path }, resources: [input.path] };
			}),
		]);

		expect(registry.toolNames()).toEqual(["stat"]);
		expect(registry.effect("stat")).toBe("observation");
		expect(registry.resourceScope("stat")).toBe("content");
		expect(registry.buildKey("stat", { path: "a.ts" }, "/workspace", "schema")).toMatchObject({
			tool: "stat",
			input: { path: "a.ts" },
			resources: ["a.ts"],
			semanticsEpoch: "host.stat.v1",
		});
		expect(registry.buildKey("unknown", {}, "/workspace")).toBeUndefined();
	});

	it("fails closed when canonicalization rejects, throws, or returns malformed resources", () => {
		const registry = new ActionSemanticsRegistry([
			resourceDefinition("reject", "reject.v1", () => undefined),
			resourceDefinition("throw", "throw.v1", () => {
				throw new Error("bad normalizer");
			}),
			resourceDefinition(
				"malformed",
				"malformed.v1",
				() =>
					({ input: {}, resources: [42] }) as unknown as {
						input: Record<string, never>;
						resources: string[];
					},
			),
		]);

		expect(registry.buildKey("reject", {}, "/workspace")).toBeUndefined();
		expect(registry.buildKey("throw", {}, "/workspace")).toBeUndefined();
		expect(registry.buildKey("malformed", {}, "/workspace")).toBeUndefined();
	});

	it("rejects duplicate tools and incoherent effect evidence", () => {
		const definition = resourceDefinition("read", "read.v1", () => ({ input: {}, resources: ["."] }));
		expect(() => new ActionSemanticsRegistry([definition, definition])).toThrow(
			"duplicate action semantics for read",
		);
		expect(
			() =>
				new ActionSemanticsRegistry([
					{
						...definition,
						tool: "write",
						effect: "workspace_mutation",
					},
				]),
		).toThrow("non-observation action write cannot declare resource evidence");
		expect(
			() =>
				new ActionSemanticsRegistry([
					{
						...definition,
						tool: "missing_scope",
						resourceScope: undefined,
					},
				]),
		).toThrow("observation action missing_scope requires resource evidence");
		expect(
			() =>
				new ActionSemanticsRegistry([
					{
						...definition,
						tool: "bad_observer",
						effect: "unbounded",
						resourceScope: undefined,
					},
				]),
		).not.toThrow();
		expect(() => new ActionSemanticsRegistry([{ ...definition, tool: "bad_none", effect: "unbounded" }])).toThrow(
			"non-observation action bad_none cannot declare resource evidence",
		);
	});

	it("rejects conflicting projector implementations with one identifier", () => {
		const first = projector("same");
		const second = projector("same");
		expect(
			() =>
				new ActionSemanticsRegistry([
					{ ...resourceDefinition("one", "one.v1", canonicalEmpty), projectors: [first] },
					{ ...resourceDefinition("two", "two.v1", canonicalEmpty), projectors: [second] },
				]),
		).toThrow("conflicting action projector same");
	});

	it("allows one projector instance to be shared and exposes only registered projector IDs", () => {
		const shared = projector("shared");
		const registry = new ActionSemanticsRegistry([
			{ ...resourceDefinition("one", "one.v1", canonicalEmpty), projectors: [shared] },
			{ ...resourceDefinition("two", "two.v1", canonicalEmpty), projectors: [shared] },
		]);

		expect(registry.projectors()).toEqual([shared]);
		expect(registry.supportsProjector("shared")).toBe(true);
		expect(registry.supportsProjector("unknown")).toBe(false);
	});

	it("defensively snapshots definitions, projector lists, and tool-name results", () => {
		const projectors = [projector("kept")];
		const source = { ...resourceDefinition("one", "one.v1", canonicalEmpty), projectors };
		const registry = new ActionSemanticsRegistry([source]);
		projectors.push(projector("late"));
		(source as { epoch: string }).epoch = "mutated";
		const names = registry.toolNames() as string[];
		names.push("outside");

		expect(registry.definition("one")?.epoch).toBe("one.v1");
		expect(registry.projectors().map((item) => item.id)).toEqual(["kept"]);
		expect(registry.toolNames()).toEqual(["one"]);
		expect(() =>
			(registry.definition("one")?.projectors as ActionKeyProjector[]).push(projector("blocked")),
		).toThrow();
	});
});

function resourceDefinition(
	tool: string,
	epoch: string,
	canonicalize: ActionSemanticsDefinition["canonicalize"],
): ActionSemanticsDefinition {
	return {
		tool,
		epoch,
		effect: "observation",
		resourceScope: "content",
		canonicalize,
	};
}

function canonicalEmpty() {
	return { input: {}, resources: ["."] };
}

function projector(id: string): ActionKeyProjector {
	return {
		id,
		partition: () => undefined,
		project: () => undefined,
	};
}
