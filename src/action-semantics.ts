import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { nonNegativeInteger, positiveInteger, speculativeConfig } from "./config-schema.ts";
import type { EffectRequirements } from "./execution-world.ts";
import {
	effectRequirements,
	normalizeEffectRequirements,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "./execution-world.ts";
import {
	asRecord,
	finiteNumber,
	immutableSnapshot,
	isImmutableSnapshot,
	isSafeCount,
	relativeFilesystemPath,
	sameFilesystemPath,
	slash,
	stableStringify,
} from "./primitives.ts";
export type ActionEffect = "observation" | "workspace_mutation" | "unbounded";
export type ResourceDependencyScope = "content" | "entries" | "tree_entries" | "tree_content";
export interface ActionKey extends CanonicalAction {
	readonly key: string;
	readonly hash: string;
	readonly tool: string;
	/** Root for resolving logical resource names during retrieval; not equivalence evidence. */
	readonly resourceRoot?: string;
	/** Identity of the canonicalization and execution contract, independent of the input schema. */
	readonly semanticsEpoch: string;
	/** Stable hash of the validated input schema used by both producer and consumer. */
	readonly schemaHash: string;
	/** Opaque digest of the concrete executor, shell, cwd, and visible environment. */
	readonly executionFingerprint: string;
	/** In-memory execution descriptor. It is deliberately excluded from diagnostics and persisted keys. */
	readonly executionContext?: unknown;
	/** An explicitly selected executor contract; absent for the host registry's default tools. */
	readonly semantics?: ActionSemanticsDefinition;
}
export interface ExactActionKeyMatch {
	readonly kind: "exact";
	readonly distance: 0;
}
/** Resource retrieval is not action equivalence or a correct prediction. */
export interface ResourceInputMatch {
	readonly kind: "inputs";
	readonly distance: number;
}
export type ActionKeyMatch = ExactActionKeyMatch | ResourceInputMatch;
export interface CanonicalAction {
	readonly input: Readonly<Record<string, unknown>>;
	readonly resources: readonly string[];
}
export interface ActionSemanticsDefinition {
	/** Explicit name shared by compatible prediction histories across tool renames. */
	readonly patternIdentity?: string;
	readonly tool: string;
	readonly epoch: string;
	/** Effects an isolation backend must contain or validate. */
	readonly effect: ActionEffect;
	/** Atomic execution guarantees required independently of any concrete backend. */
	readonly requirements: EffectRequirements;
	/** Eager filesystem scope, or supplied inputs only (never proof of an ambient Actor window). */
	readonly resourceScope?: ResourceDependencyScope | "captured_inputs";
	readonly canonicalize: (input: unknown, cwd: string) => CanonicalAction | undefined;
}
// Only issued definitions may skip normalization; mutable provider records are never memoized.
const normalizedDefinitions = new WeakSet<ActionSemanticsDefinition>();
export class ActionSemanticsRegistry {
	private readonly definitionsByTool = new Map<string, ActionSemanticsDefinition>();
	constructor(definitions: readonly ActionSemanticsDefinition[]) {
		for (const source of definitions) {
			const definition = normalizeDefinition(source);
			const { tool } = definition;
			if (this.definitionsByTool.has(tool)) throw new Error(`duplicate action semantics for ${tool}`);
			this.definitionsByTool.set(tool, definition);
		}
	}
	definition(action: string | ActionKey): ActionSemanticsDefinition | undefined {
		if (typeof action !== "string" && action.semantics) return action.semantics;
		return this.definitionsByTool.get(typeof action === "string" ? action : action.tool);
	}
	toolNames(effect?: ActionEffect): readonly string[] {
		return [...this.definitionsByTool.values()]
			.filter((definition) => effect === undefined || definition.effect === effect)
			.map((definition) => definition.tool);
	}
	effect(action: string | ActionKey): ActionEffect | undefined {
		return this.definition(action)?.effect;
	}
	requirements(action: string | ActionKey): EffectRequirements | undefined {
		return this.definition(action)?.requirements;
	}
	buildKey(
		tool: string,
		input: unknown,
		cwd: string,
		schemaHash = "",
		execution?: {
			readonly fingerprint: string;
			readonly context?: unknown;
			readonly semantics?: ActionSemanticsDefinition;
		},
	): ActionKey | undefined {
		const { fingerprint, context, semantics } = execution ?? {};
		const definition = semantics ? normalizeDefinition(semantics) : this.definition(tool);
		if (!definition) return undefined;
		try {
			const canonical = definition.canonicalize(input, cwd);
			if (!canonical || !canonical.resources.every((resource) => typeof resource === "string"))
				return undefined;
			return buildActionKey({
				tool,
				resources: canonical.resources,
				resourceRoot: cwd,
				input: canonical.input,
				schemaHash,
				semanticsEpoch: definition.epoch,
				executionFingerprint: fingerprint,
				executionContext: context,
				semantics: semantics ? definition : undefined,
			});
		} catch {
			return undefined;
		}
	}
}
// Stock query tools launch ambient executables/configuration (rg can even invoke --pre).
// A static workspace tree is not their dependency closure or their isolation authority.
const HOST_PROCESS_EFFECTS = effectRequirements(
	"invocation.host_function",
	...UNRESTRICTED_PROCESS_EFFECTS.capabilities,
);
export const PI_ACTION_SEMANTICS = new ActionSemanticsRegistry(
	(
		[
			{
				tool: "read",
				effect: "observation",
				requirements: RESOURCE_OBSERVATION_EFFECTS,
				resourceScope: "content",
			},
			{
				tool: "grep",
				effect: "unbounded",
				requirements: HOST_PROCESS_EFFECTS,
			},
			{ tool: "find", effect: "unbounded", requirements: HOST_PROCESS_EFFECTS },
			{
				tool: "ls",
				effect: "observation",
				requirements: RESOURCE_OBSERVATION_EFFECTS,
				resourceScope: "entries",
			},
			{
				tool: "bash",
				effect: "unbounded",
				requirements: UNRESTRICTED_PROCESS_EFFECTS,
			},
			{
				tool: "write",
				effect: "workspace_mutation",
				requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
			},
			{
				tool: "edit",
				effect: "workspace_mutation",
				requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
			},
		] satisfies Omit<ActionSemanticsDefinition, "epoch" | "canonicalize">[]
	).map((definition): ActionSemanticsDefinition => ({
		...definition,
		epoch: `pi.${definition.tool}`,
		canonicalize: (input, cwd) => canonicalPiAction(definition.tool, input, cwd),
	})),
);
export function buildActionKey(
	input: Pick<ActionKey, "tool" | "resources" | "input"> & Partial<Omit<ActionKey, "key" | "hash">>,
): ActionKey {
	const schemaHash = input.schemaHash ?? "";
	const semanticsEpoch = input.semanticsEpoch ?? "";
	const executionFingerprint = input.executionFingerprint ?? "";
	const semantics = input.semantics ? normalizeDefinition(input.semantics) : undefined;
	if (semantics && (semantics.tool !== input.tool || semantics.epoch !== semanticsEpoch))
		throw new Error("action contract identity mismatch");
	const canonicalInput = immutableSnapshot(input.input);
	if (!isImmutableSnapshot(canonicalInput)) throw new Error("Action input has no immutable data identity");
	const key = stableStringify({
		tool: input.tool,
		semanticsEpoch,
		schemaHash,
		executionFingerprint,
		input: canonicalInput,
	});
	return Object.freeze({
		key,
		hash: fastHash(key),
		tool: input.tool,
		input: canonicalInput,
		resources: Object.freeze([...input.resources]),
		...(input.resourceRoot !== undefined ? { resourceRoot: path.resolve(input.resourceRoot) } : {}),
		semanticsEpoch,
		schemaHash,
		executionFingerprint,
		...(input.executionContext !== undefined ? { executionContext: input.executionContext } : {}),
		...(semantics ? { semantics } : {}),
	});
}
export function actionKeyMatch(speculative: ActionKey, actor: ActionKey): ExactActionKeyMatch | undefined {
	return speculative.key === actor.key ? { kind: "exact", distance: 0 } : undefined;
}
export function normalizeRelativeRoot(value: unknown, cwd: string): string | undefined {
	return value !== undefined && typeof value !== "string"
		? undefined
		: normalizeWorkspacePath(value ?? ".", cwd);
}
export function normalizeReadOffset(value: unknown): number {
	return positiveInteger(value, speculativeConfig().search.readOffset);
}
export function normalizeReadLimit(value: unknown): number {
	const limit = finiteNumber(value);
	return limit === undefined ? speculativeConfig().search.readLimit : Math.max(0, Math.floor(limit));
}
function canonicalPiAction(tool: string, input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record) return undefined;
	if (tool === "bash") {
		if (typeof record.command !== "string") return undefined;
		const resource = slash(path.resolve(cwd));
		return {
			resources: [resource],
			input: {
				command: record.command,
				cwd: resource,
				...(finiteNumber(record.timeout) !== undefined ? { timeout: record.timeout } : {}),
			},
		};
	}
	const query = tool === "grep" || tool === "find" || tool === "ls";
	if (!query && typeof record.path !== "string") return undefined;
	let fields: Record<string, unknown>;
	if (query) {
		if (!validOptionalInteger(record.limit, 1) || (tool !== "ls" && typeof record.pattern !== "string"))
			return undefined;
		fields = {
			...(tool !== "ls" ? { pattern: record.pattern } : {}),
			path: record.path,
		};
		if (tool === "grep") {
			if (!validOptionalInteger(record.context, 0)) return undefined;
			Object.assign(fields, {
				...(typeof record.glob === "string" ? { glob: record.glob } : {}),
				ignoreCase: record.ignoreCase === true,
				literal: record.literal === true,
				context: nonNegativeInteger(record.context, 0),
			});
		}
		fields.limit = positiveInteger(
			record.limit,
			tool === "grep"
				? speculativeConfig().search.grepLimit
				: tool === "find"
					? speculativeConfig().search.findLimit
					: speculativeConfig().search.lsLimit,
		);
	} else if (tool === "read") {
		if (!validOptionalInteger(record.offset, 1) || !validOptionalInteger(record.limit, 0)) return undefined;
		fields = {
			path: record.path,
			offset: normalizeReadOffset(record.offset),
			...(record.limit !== undefined ? { limit: normalizeReadLimit(record.limit) } : {}),
		};
	} else if (tool === "write") {
		if (typeof record.content !== "string") return undefined;
		fields = { path: record.path, content: record.content };
	} else if (tool === "edit") {
		if (!Array.isArray(record.edits) || !record.edits.length) return undefined;
		const edits: Array<{
			readonly oldText: string;
			readonly newText: string;
		}> = [];
		for (const value of record.edits) {
			const edit = asRecord(value);
			if (!edit || typeof edit.oldText !== "string" || typeof edit.newText !== "string") return undefined;
			edits.push({ oldText: edit.oldText, newText: edit.newText });
		}
		fields = { path: record.path, edits };
	} else return undefined;
	const resource = query
		? normalizeRelativeRoot(record.path, cwd)
		: normalizeWorkspacePath(record.path as string, cwd);
	if (resource === undefined || ((tool === "write" || tool === "edit") && resource === ".")) return undefined;
	fields.path = resource;
	return { resources: [resource], input: fields };
}
function normalizeDefinition(source: ActionSemanticsDefinition): ActionSemanticsDefinition {
	if (normalizedDefinitions.has(source)) return source;
	const tool = source.tool.trim();
	const epoch = source.epoch.trim();
	if (!tool) throw new Error("action semantics tool must not be empty");
	if (source.patternIdentity !== undefined && !source.patternIdentity.trim())
		throw new Error("Empty tool pattern identity");
	if (!epoch) throw new Error(`action semantics epoch must not be empty for ${tool}`);
	const definition = Object.freeze({
		...source,
		tool,
		epoch,
		requirements: normalizeEffectRequirements(source.requirements),
	});
	assertDefinitionCoherence(definition);
	normalizedDefinitions.add(definition);
	return definition;
}
function assertDefinitionCoherence(definition: ActionSemanticsDefinition): void {
	const has = (capability: EffectRequirements["capabilities"][number]) =>
		definition.requirements.capabilities.includes(capability);
	if (definition.effect === "observation") {
		if (definition.resourceScope === undefined)
			throw new Error(`observation action ${definition.tool} requires resource evidence`);
		if (!has("validation.resource_snapshot"))
			throw new Error(`observation action ${definition.tool} requires snapshot-validation capability`);
		return;
	}
	if (definition.resourceScope !== undefined)
		throw new Error(`non-observation action ${definition.tool} cannot declare resource evidence`);
	if (
		definition.effect === "workspace_mutation" &&
		(!has("filesystem.write") || !has("invocation.workspace_path"))
	)
		throw new Error(`workspace mutation ${definition.tool} requires path-mutation capabilities`);
	if (definition.effect === "unbounded" && (!has("invocation.process") || !has("output.gate")))
		throw new Error(`unbounded action ${definition.tool} requires process and external-output capabilities`);
}
const require = createRequire(import.meta.url);
let piPaths: {
	resolveToCwd: (value: string, cwd: string) => string;
};
/** Pi's pure path normalization; runtime filename selection belongs to the resource observation. */
export function resolvePiToolPath(value: string, cwd: string): string {
	piPaths ??= require(
		fileURLToPath(
			new URL("./core/tools/path-utils.js", import.meta.resolve("@earendil-works/pi-coding-agent")),
		),
	);
	return piPaths.resolveToCwd(value, cwd);
}
function normalizeWorkspacePath(value: string, cwd: string): string | undefined {
	// An incompatible package layout makes buildKey decline reuse, never invent an identity.
	const root = path.resolve(cwd);
	const target = resolvePiToolPath(value, root);
	const relative = relativeFilesystemPath(root, target);
	if (relative === undefined) return undefined;
	const resource = slash(relative || ".");
	return sameFilesystemPath(resolvePiToolPath(resource, root), target) ? resource : `./${resource}`;
}
function validOptionalInteger(value: unknown, minimum: number): boolean {
	return value === undefined || isSafeCount(value, minimum);
}
function fastHash(value: string): string {
	let hash = 0x811c9dc5;
	for (let index = 0; index < value.length; index++) {
		hash ^= value.charCodeAt(index);
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0).toString(16).padStart(8, "0");
}
