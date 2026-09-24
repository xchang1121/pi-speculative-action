import path from "node:path";
import { describe, expect, it } from "vitest";
import { resourceDependencies } from "../src/resource-version.ts";
import {
	type ActionKeyProjector,
	type ActionSemanticsDefinition,
	ActionSemanticsRegistry,
	actionKeyCovers,
	actionKeyMatch,
	actionKeyMismatchReason,
	BASH_TIMEOUT_ACTION_KEY_PROJECTOR,
	buildActionKey,
	buildPiActionKey,
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
	PI_ACTION_SEMANTICS,
	READ_RANGE_ACTION_KEY_PROJECTOR,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "../src/action-semantics.ts";
import { PI_BASH_TIMEOUT_PROJECTION_RULE } from "../src/pi-tool-invocation.ts";
import {
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";

describe("ActionSemanticsRegistry", () => {
	it("defines K(a), resource evidence, and effects without choosing an execution backend", () => {
		expect(PI_ACTION_SEMANTICS.toolNames()).toEqual(["read", "grep", "find", "ls", "bash", "write", "edit"]);
		expect(KEYABLE_TOOLS).toEqual(PI_ACTION_SEMANTICS.toolNames());
		expect(OBSERVATION_ACTION_TOOLS).toEqual(["read", "ls"]);
		expect(WORKSPACE_MUTATION_ACTION_TOOLS).toEqual(["write", "edit"]);
		expect(UNBOUNDED_ACTION_TOOLS).toEqual(["grep", "find", "bash"]);

		expect(PI_ACTION_SEMANTICS.definition("read")).toMatchObject({ effect: "observation", resourceScope: "content" });
		expect(PI_ACTION_SEMANTICS.definition("ls")).toMatchObject({ effect: "observation", resourceScope: "entries" });
		expect(PI_ACTION_SEMANTICS.definition("bash")).toMatchObject({ effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS });
		expect(PI_ACTION_SEMANTICS.definition("write")).toMatchObject({
			effect: "workspace_mutation",
			requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
		});
		expect(PI_ACTION_SEMANTICS.definition("write")?.resourceScope).toBeUndefined();
	});

	it("canonicalizes equivalent ls defaults and rejects unstable views", () => {
		const implicit = buildPiActionKey("ls", {}, "/workspace");
		const explicit = buildPiActionKey("ls", { path: ".", limit: 500 }, "/workspace");

		expect(implicit?.key).toBe(explicit?.key);
		expect(implicit).toMatchObject({ tool: "ls", semanticsEpoch: "pi.ls", resources: ["."], input: { path: ".", limit: 500 } });
		expect(buildPiActionKey("ls", { path: "../outside" }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("ls", { path: "..cache" }, "/workspace")).toBeDefined();
		if (process.platform === "win32") for (const [cwd, target] of [["c:\\Work", "C:\\Work\\a.ts"], ["C:\\Work", "c:/Work/a.ts"], ["C:\\Work", "/c/Work/a.ts"]])
			expect(buildPiActionKey("read", { path: target }, cwd)?.key).toBe(buildPiActionKey("read", { path: "a.ts" }, cwd)?.key); // Drive-letter spellings.
		expect(buildPiActionKey("ls", { limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("ls", { limit: 1.5 }, "/workspace")).toBeUndefined();
		for (const tool of ["grep", "find"] as const) {
			const args = { pattern: "*" };
			expect(buildPiActionKey(tool, args, "/workspace")?.key).toBe(buildPiActionKey(tool, { ...args, path: "@.", limit: tool === "grep" ? 100 : 1000 }, "/workspace")?.key);
		}
	});

	it("keeps read's omitted-limit view distinct inside its versioned K(a)", () => {
		const implicit = buildPiActionKey("read", { path: "src/a.ts" }, "/workspace", "schema-base");
		const explicit = buildPiActionKey("read", { path: "src/a.ts", offset: 1, limit: 2000 }, "/workspace", "schema-base");

		expect(implicit?.key).not.toBe(explicit?.key);
		const relation = implicit && explicit ? actionKeyMatch(implicit, explicit, [READ_RANGE_ACTION_KEY_PROJECTOR]) : undefined;
		expect(relation).toMatchObject({ kind: "projected", projector: "read.range" });
		expect(implicit).toMatchObject({ tool: "read", semanticsEpoch: "pi.read", schemaHash: "schema-base", resources: ["src/a.ts"] });
		expect(implicit?.input).not.toHaveProperty("limit");
		expect(explicit?.input).toHaveProperty("limit", 2000);
		expect(implicit?.key).toContain('"semanticsEpoch":"pi.read"');
		expect(Object.isFrozen(implicit)).toBe(true);
		expect(Object.isFrozen(implicit?.input)).toBe(true);
		expect(Object.isFrozen(implicit?.resources)).toBe(true);
	});

	it("projects a finished Bash command onto any longer or absent timeout", async () => {
		const key = (timeout?: number, command = "npm test") => buildPiActionKey("bash", { command, ...(timeout === undefined ? {} : { timeout }) }, "/workspace")!;
		const match = (speculative?: number, actor?: number) => actionKeyMatch(key(speculative), key(actor), [BASH_TIMEOUT_ACTION_KEY_PROJECTOR])?.kind;
		expect([match(60, 120), match(60, undefined), match(120, 60), match(undefined, 60), match(undefined, undefined)]).toEqual(["projected", "projected", undefined, undefined, "exact"]);
		const operation = (identity: string) => buildActionKey({ ...key(), input: { operation: identity } });
		for (const [left, right] of [[key(60), key(120, "npm run build")], [operation("launch"), operation("worker")]] as const)
			expect(actionKeyMatch(left, right, [BASH_TIMEOUT_ACTION_KEY_PROJECTOR])).toBeUndefined();
		const passed = { result: { content: [{ type: "text" as const, text: "pass" }], details: undefined }, isError: false }, failed = { ...passed, isError: true };
		expect([passed, failed].map((output) => PI_BASH_TIMEOUT_PROJECTION_RULE.captureCoverage!(key(60), output))).toEqual([true, undefined]);
		expect(await PI_BASH_TIMEOUT_PROJECTION_RULE.projectOutput!({ speculative: key(60), actor: key(120), output: passed, coverage: true,
			keyMatch: { kind: "projected", projector: "bash.timeout", distance: 1 } })).toBe(passed);
	});

	it("fails closed instead of folding unsupported numeric query views into valid keys", () => {
		expect(buildPiActionKey("read", { path: "a.ts", offset: 1.5 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("read", { path: "a.ts", limit: -1 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("grep", { pattern: "x", context: 0.5 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("grep", { pattern: "x", limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("find", { pattern: "*", limit: 0 }, "/workspace")).toBeUndefined();
		expect(buildPiActionKey("find", { pattern: "*", limit: 1.5 }, "/workspace")).toBeUndefined();
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
			input: { path: "a.ts", offset: 1, fields: { "\u00e9": 2, "e\u0301": 1 } },
			semanticsEpoch: "read-base",
			schemaHash: "schema-base",
			executionFingerprint: "executor-base",
		});
		expect(actionKeyMatch(base, buildActionKey({
			...base, input: { ...base.input, fields: { "e\u0301": 1, "\u00e9": 2 } },
		}))).toMatchObject({ kind: "exact", distance: 0 });
		const sameEnvelope = buildActionKey({ ...base, resources: ["a.ts"], input: { path: "a.ts", offset: 2 } });
		expect(actionKeyMatch(base, sameEnvelope, [permissive])).toMatchObject({ kind: "projected", projector: "permissive" });
		const covering = { ...permissive, id: "covering", canShareInFlight: () => true };
		expect(actionKeyCovers(base, sameEnvelope, [permissive])).toBe(false);
		expect(actionKeyCovers(base, sameEnvelope, [permissive, covering])).toBe(true);
		expect(actionKeyMatch(base, sameEnvelope, [permissive, covering], true)).toMatchObject({ kind: "projected", projector: "covering" });

		for (const [field, value, reason] of [["tool", "grep", "different_tool"], ["semanticsEpoch", "read-other", "different_semantics"],
			["schemaHash", "schema-other", "different_schema"], ["executionFingerprint", "executor-other", "different_executor"]]) {
			for (const input of [base.input, { path: "a.ts", offset: 2 }]) {
				const actor = buildActionKey({ ...base, [field!]: value, input });
				expect(actionKeyMatch(base, actor, [permissive])).toBeUndefined();
				expect(actionKeyMismatchReason(base, actor, [permissive])).toBe(reason);
			}
		}
	});

	it("binds the selected profile before canonicalization without mutating the native registry", () => {
		for (const tool of ["grep", "find"]) for (const scope of ["tree_content", "captured_inputs"] as const) {
			const profile = { ...PI_ACTION_SEMANTICS.definition(tool)!, epoch: "closed-base",
				effect: "observation", requirements: { capabilities: [...RESOURCE_OBSERVATION_EFFECTS.capabilities] }, resourceScope: scope } satisfies ActionSemanticsDefinition;
			const context = {}, execution = { fingerprint: "profile-base", context, semantics: profile as ActionSemanticsDefinition };
			const canonicalize = profile.canonicalize;
			profile.canonicalize = function (input, cwd) {
				profile.epoch = "closed-other";
				profile.requirements.capabilities.push("filesystem.write");
				(profile as { resourceScope: string }).resourceScope = "entries";
				execution.fingerprint = "profile-other"; execution.context = { changed: true };
				execution.semantics = { ...profile, canonicalize: () => undefined };
				return canonicalize(input, cwd);
			};
			const args = { pattern: "needle", path: "." };
			const native = PI_ACTION_SEMANTICS.buildKey(tool, args, "/workspace")!;
			const closed = PI_ACTION_SEMANTICS.buildKey(tool, args, "/workspace", "", execution)!;
			expect(closed).toMatchObject({ semanticsEpoch: "closed-base", executionFingerprint: "profile-base", semantics: { resourceScope: scope } });
			expect(closed.executionContext).toBe(context);
			expect(closed.semantics?.requirements).toEqual(RESOURCE_OBSERVATION_EFFECTS);
			expect(PI_ACTION_SEMANTICS.definition(native)?.effect).toBe("unbounded");
			expect(PI_ACTION_SEMANTICS.definition(closed)?.effect).toBe("observation");
			expect(resourceDependencies(native, "/workspace")).toEqual([]);
			expect(resourceDependencies(closed, "/workspace")).toEqual(scope === "captured_inputs" ? [] : [{ path: path.resolve("/workspace"), scope }]);
			expect(actionKeyMatch(native, closed, PI_ACTION_SEMANTICS.projectors())).toBeUndefined();
			expect(buildActionKey(closed).semantics).toBe(closed.semantics);
			expect(closed.semantics?.canonicalize).toBe(profile.canonicalize);
			expect(() => buildActionKey({ ...closed, tool: "unrelated" })).toThrow("contract identity mismatch");
			expect(Object.isFrozen(closed.semantics?.requirements)).toBe(true);
			expect(PI_ACTION_SEMANTICS.buildKey(tool, args, "/workspace", "", execution)).toBeUndefined();
			expect(PI_ACTION_SEMANTICS.buildKey(tool, args, "/workspace", "", { ...execution, semantics: profile }))
				.toMatchObject({ semanticsEpoch: "closed-other", executionFingerprint: "profile-other", semantics: { resourceScope: "entries" } });
		}
	});

	it("supports a new host tool with one semantics definition", () => {
		const registry = new ActionSemanticsRegistry([
			{ ...resourceDefinition("stat", "host.stat", (input) => {
				if (!input || typeof input !== "object" || !("path" in input) || typeof input.path !== "string") {
					return undefined;
				}
				return { input: { path: input.path }, resources: [input.path] };
			}), resourceScope: "tree_entries" },
			{ ...PI_ACTION_SEMANTICS.definition("write")!, tool: "custom_write" },
		]);
		const key = registry.buildKey("stat", { path: "a.ts" }, "/workspace", "schema")!;
		expect(resourceDependencies(key, "/workspace", registry)).toEqual([{ path: path.resolve("/workspace/a.ts"), scope: "tree_entries" }]);
		expect(resourceDependencies({ ...key, tool: "custom_write" }, "/workspace", registry)).toEqual([]);
		expect(key).toMatchObject({ tool: "stat", input: { path: "a.ts" }, resources: ["a.ts"], semanticsEpoch: "host.stat" });
		expect(registry.buildKey("unknown", {}, "/workspace")).toBeUndefined();
	});

	it("fails closed when canonicalization rejects, throws, or returns unkeyable data", () => {
		const cycle: Record<string, unknown> = {}; cycle.self = cycle;
		const child = { value: 0 };
		for (const value of [new Date(0), new Map([["value", 0]]), new Set([0]), cycle, -0, NaN, { omitted: undefined }, { left: child, right: child }]) {
			const input = { value }, contract = resourceDefinition("opaque", "opaque", () => ({ input, resources: [] }));
			expect(() => buildActionKey({ tool: "opaque", input, resources: [] })).toThrow("immutable data identity");
			expect(new ActionSemanticsRegistry([contract]).buildKey("opaque", {}, "/workspace")).toBeUndefined();
		}
		const registry = new ActionSemanticsRegistry([
			resourceDefinition("reject", "reject", () => undefined),
			resourceDefinition("throw", "throw", () => {
				throw new Error("bad normalizer");
			}),
			resourceDefinition(
				"malformed",
				"malformed",
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
		const definition = resourceDefinition("read", "read-base", () => ({ input: {}, resources: ["."] }));
		expect(() => new ActionSemanticsRegistry([definition, definition])).toThrow("duplicate action semantics for read");
		for (const [override, message] of [
			[{ tool: "write", effect: "workspace_mutation" }, "non-observation action write cannot declare resource evidence"],
			[{ tool: "missing_scope", resourceScope: undefined }, "observation action missing_scope requires resource evidence"],
			[{ tool: "bad_none", effect: "unbounded" }, "non-observation action bad_none cannot declare resource evidence"],
			[{ tool: "  " }, "action semantics tool must not be empty"],
			[{ epoch: "  " }, "action semantics epoch must not be empty for read"],
		] as const) {
			expect(() => new ActionSemanticsRegistry([Object.freeze({ ...definition, ...override })])).toThrow(message);
		}
		expect(() => new ActionSemanticsRegistry([{ ...definition, tool: "observer", effect: "unbounded",
			requirements: UNRESTRICTED_PROCESS_EFFECTS, resourceScope: undefined }])).not.toThrow();
	});

	it("owns immutable definitions and shares registered result projectors", () => {
		const projectors = [projector("kept")];
		const source = { ...resourceDefinition("one", "one", canonicalEmpty), projectors };
		const registry = new ActionSemanticsRegistry([source, { ...source, tool: "two" }]);
		const registered = registry.projectors()[0]!;
		expect(() => new ActionSemanticsRegistry([source, { ...source, tool: "conflict", projectors: [projector("kept")] }]))
			.toThrow("conflicting action projector kept");
		projectors.push(projector("late"));
		Object.assign(projectors[0]!, { id: "changed", partition: () => "changed", project: () => { throw new Error("changed"); } });
		(source as { epoch: string }).epoch = "mutated";
		const names = registry.toolNames() as string[];
		names.push("outside");

		expect(registry.definition("one")?.epoch).toBe("one");
		expect(registry.projectors()).toEqual([registered]);
		expect(registered.id).toBe("kept");
		expect(Object.isFrozen(registered)).toBe(true);
		expect(Object.isFrozen(projectors[0])).toBe(false);
		expect(new ActionSemanticsRegistry([source]).projectors()[0]?.id).toBe("changed");
		expect(registered.partition(buildPiActionKey("read", { path: "a.ts" }, "/workspace")!)).toBeUndefined();
		expect(registry.supportsProjector("kept")).toBe(true);
		expect(registry.supportsProjector("late")).toBe(false);
		expect(registry.toolNames()).toEqual(["one", "two"]);
		expect(registry.definition("one")?.projectors).toHaveLength(1);
		expect(new ActionSemanticsRegistry([registry.definition("one")!]).definition("one")).toBe(registry.definition("one"));
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
		requirements: RESOURCE_OBSERVATION_EFFECTS,
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
