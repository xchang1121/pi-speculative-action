import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { relativeFilesystemPath, slash } from "./path-utils.ts";
import {
	type EffectRequirements,
	normalizeEffectRequirements,
	effectRequirements,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "./effect-model.ts";
import { stableStringify } from "./stable-json.ts";

/** Observable effects of an action, independent of any concrete isolation backend. */
export type ActionEffect = "observation" | "workspace_mutation" | "unbounded";
export type ResourceDependencyScope = "content" | "entries" | "tree_entries" | "tree_content";

export interface ReadActionRange {
	readonly path: string;
	readonly offset: number;
	readonly limit: number;
	readonly end: number;
}

export interface ActionKey {
	readonly key: string;
	readonly hash: string;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly resources: readonly string[];
	/** Version of the canonicalization and execution contract, independent of the input schema. */
	readonly semanticsEpoch: string;
	/** Stable hash of the validated input schema used by both producer and consumer. */
	readonly schemaHash: string;
	/** Opaque digest of the concrete executor, shell, cwd, and visible environment. */
	readonly executionFingerprint: string;
	/** In-memory execution descriptor. It is deliberately excluded from diagnostics and persisted keys. */
	readonly executionContext?: unknown;
}

export interface ProjectedActionKey {
	readonly action: ActionKey;
	/** Information discarded by the projection; lower is a more specific match. */
	readonly distance: number;
}

/** A partial projection π that can map one canonical K(a) into another. */
export interface ActionKeyProjector {
	readonly id: string;
	/** Coarse cache partition; every pair accepted by project must return the same value. */
	readonly partition: (action: ActionKey) => string | undefined;
	readonly project: (speculative: ActionKey, actor: ActionKey) => ProjectedActionKey | undefined;
	/** Whether the speculative request itself covers the actor request before output coverage is known. */
	readonly canShareInFlight?: (speculative: ActionKey, actor: ActionKey) => boolean;
}

export interface ExactActionKeyMatch {
	readonly kind: "exact";
	readonly distance: 0;
}

export interface ProjectedActionKeyMatch {
	/** A lossless result projection may satisfy the Actor action if execution and adoption later succeed. */
	readonly kind: "projected";
	readonly distance: number;
	readonly projector: string;
}

export type ActionKeyMatch = ExactActionKeyMatch | ProjectedActionKeyMatch;

export type ActionKeyMismatchReason =
	| "different_tool"
	| "different_semantics"
	| "different_schema"
	| "different_executor"
	| "different_core"
	| "projection_not_applicable";

export interface CanonicalAction {
	readonly input: Readonly<Record<string, unknown>>;
	readonly resources: readonly string[];
}

export interface ActionSemanticsDefinition {
	readonly tool: string;
	readonly epoch: string;
	/** Effects an isolation backend must contain or validate. */
	readonly effect: ActionEffect;
	/** Atomic execution guarantees required independently of any concrete backend. */
	readonly requirements: EffectRequirements;
	/** Filesystem evidence required to prove that a completed action is still current. */
	readonly resourceScope?: ResourceDependencyScope;
	readonly canonicalize: (input: unknown, cwd: string) => CanonicalAction | undefined;
	readonly projectors?: readonly ActionKeyProjector[];
}

/** Immutable source of truth for K(a), projection, resource evidence, and safe local fallback capability. */
export class ActionSemanticsRegistry {
	private readonly definitionsByTool = new Map<string, ActionSemanticsDefinition>();
	private readonly projectorsByID = new Map<string, ActionKeyProjector>();

	constructor(definitions: readonly ActionSemanticsDefinition[]) {
		for (const source of definitions) {
			const tool = source.tool.trim();
			const epoch = source.epoch.trim();
			if (!tool) throw new Error("action semantics tool must not be empty");
			if (!epoch) throw new Error(`action semantics epoch must not be empty for ${tool}`);
			if (this.definitionsByTool.has(tool)) throw new Error(`duplicate action semantics for ${tool}`);
			const definition: ActionSemanticsDefinition = Object.freeze({
				...source,
				tool,
				epoch,
				requirements: normalizeEffectRequirements(source.requirements),
				projectors: Object.freeze([...new Set([...(source.projectors ?? []), ...(source.resourceScope ? [RESOURCE_INPUT_ACTION_KEY_PROJECTOR] : [])])]),
			});
			assertDefinitionCoherence(definition);
			this.definitionsByTool.set(tool, definition);
			for (const projector of definition.projectors ?? []) {
				const existing = this.projectorsByID.get(projector.id);
				if (existing && existing !== projector) {
					throw new Error(`conflicting action projector ${projector.id}`);
				}
				this.projectorsByID.set(projector.id, projector);
			}
		}
	}

	definition(tool: string): ActionSemanticsDefinition | undefined {
		return this.definitionsByTool.get(tool);
	}

	toolNames(effect?: ActionEffect): readonly string[] {
		return [...this.definitionsByTool.values()]
			.filter((definition) => effect === undefined || definition.effect === effect)
			.map((definition) => definition.tool);
	}

	effect(tool: string): ActionEffect | undefined {
		return this.definition(tool)?.effect;
	}

	requirements(tool: string): EffectRequirements | undefined {
		return this.definition(tool)?.requirements;
	}

	resourceScope(tool: string): ResourceDependencyScope | undefined {
		return this.definition(tool)?.resourceScope;
	}

	projectors(): readonly ActionKeyProjector[] {
		return [...this.projectorsByID.values()];
	}

	supportsProjector(id: string): boolean {
		return this.projectorsByID.has(id);
	}

	buildKey(
		tool: string,
		input: unknown,
		cwd: string,
		schemaHash = "",
		execution?: { readonly fingerprint: string; readonly context?: unknown },
	): ActionKey | undefined {
		const definition = this.definition(tool);
		if (!definition) return undefined;
		let canonical: CanonicalAction | undefined;
		try {
			canonical = definition.canonicalize(input, cwd);
		} catch {
			return undefined;
		}
		if (!canonical || !canonical.resources.every((resource) => typeof resource === "string")) return undefined;
		return buildActionKey({
			tool,
			resources: canonical.resources,
			input: canonical.input,
			schemaHash,
			semanticsEpoch: definition.epoch,
			executionFingerprint: execution?.fingerprint,
			executionContext: execution?.context,
		});
	}
}

export const READ_DEFAULT_OFFSET = 1;
export const READ_DEFAULT_LIMIT = 2000;
export const GREP_DEFAULT_LIMIT = 100;
export const FIND_DEFAULT_LIMIT = 1000;
export const LS_DEFAULT_LIMIT = 500;

/** Lookup hint only: different queries require re-evaluation over the branch's sealed inputs.
 * No running join is inferred before a branch proves that it owns those inputs. */
export const RESOURCE_INPUT_ACTION_KEY_PROJECTOR: ActionKeyProjector = {
	id: "resource.inputs",
	partition: (action) => action.resources.length ? JSON.stringify([
		action.tool, action.semanticsEpoch, action.schemaHash, action.executionFingerprint, action.resources,
	]) : undefined,
	project: (speculative, actor) => {
		const partition = RESOURCE_INPUT_ACTION_KEY_PROJECTOR.partition(speculative);
		return partition !== undefined && partition === RESOURCE_INPUT_ACTION_KEY_PROJECTOR.partition(actor)
			? { action: actor, distance: Number.MAX_SAFE_INTEGER } : undefined;
	},
};

/** π_read narrows a cached read action to the actor's requested interval. */
export const READ_RANGE_ACTION_KEY_PROJECTOR: ActionKeyProjector = {
	id: "read.range",
	partition: readProjectionPartition,
	project: (speculative, actor) => {
		const speculativeRange = readActionRange(speculative);
		const actorRange = readActionRange(actor);
		if (!speculativeRange || !actorRange) return undefined;
		if (readProjectionPartition(speculative) !== readProjectionPartition(actor)) return undefined;
		if (
			speculativeRange.limit === 0 ||
			speculativeRange.offset > actorRange.offset ||
			actorRange.offset > speculativeRange.end + 1
		) {
			return undefined;
		}
		return {
			action: actor,
			distance: actorRange.offset - speculativeRange.offset + Math.abs(speculativeRange.end - actorRange.end),
		};
	},
	canShareInFlight: readRangesShareInFlight,
};

// Stock query tools launch ambient executables/configuration (rg can even invoke --pre).
// A static workspace tree is not their dependency closure or their isolation authority.
const HOST_PROCESS_EFFECTS = effectRequirements("invocation.host_function", ...UNRESTRICTED_PROCESS_EFFECTS.capabilities);

export const PI_ACTION_SEMANTICS = new ActionSemanticsRegistry([
	{
		tool: "read",
		epoch: "pi.read.v3",
		effect: "observation",
		requirements: RESOURCE_OBSERVATION_EFFECTS,
		resourceScope: "content",
		canonicalize: canonicalRead,
		projectors: [READ_RANGE_ACTION_KEY_PROJECTOR],
	},
	{
		tool: "grep",
		epoch: "pi.grep.v4",
		effect: "unbounded",
		requirements: HOST_PROCESS_EFFECTS,
		canonicalize: canonicalGrep,
	},
	{
		tool: "find",
		epoch: "pi.find.v4",
		effect: "unbounded",
		requirements: HOST_PROCESS_EFFECTS,
		canonicalize: canonicalFind,
	},
	{
		tool: "ls",
		epoch: "pi.ls.v3",
		effect: "observation",
		requirements: RESOURCE_OBSERVATION_EFFECTS,
		resourceScope: "entries",
		canonicalize: canonicalLs,
	},
	{
		tool: "bash",
		epoch: "pi.bash.v3",
		effect: "unbounded",
		requirements: UNRESTRICTED_PROCESS_EFFECTS,
		canonicalize: canonicalBash,
	},
	{
		tool: "write",
		epoch: "pi.write.v2",
		effect: "workspace_mutation",
		requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
		canonicalize: canonicalWrite,
	},
	{
		tool: "edit",
		epoch: "pi.edit.v2",
		effect: "workspace_mutation",
		requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
		canonicalize: canonicalEdit,
	},
]);

export const OBSERVATION_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("observation"));
export const WORKSPACE_MUTATION_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("workspace_mutation"));
export const UNBOUNDED_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("unbounded"));
export const KEYABLE_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames());

export function buildActionKey(input: {
	readonly tool: string;
	readonly resources: readonly string[];
	readonly input: Readonly<Record<string, unknown>>;
	readonly schemaHash?: string;
	readonly semanticsEpoch?: string;
	readonly executionFingerprint?: string;
	readonly executionContext?: unknown;
}): ActionKey {
	const schemaHash = input.schemaHash ?? "";
	const semanticsEpoch = input.semanticsEpoch ?? "";
	const executionFingerprint = input.executionFingerprint ?? "";
	const canonicalInput = freezeCanonicalValue(structuredClone(input.input));
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
		semanticsEpoch,
		schemaHash,
		executionFingerprint,
		...(input.executionContext !== undefined ? { executionContext: input.executionContext } : {}),
	});
}

function freezeCanonicalValue<Value>(value: Value, seen = new WeakSet<object>()): Value {
	if (!value || typeof value !== "object" || Object.isFrozen(value) || seen.has(value)) return value;
	seen.add(value);
	for (const child of Object.values(value)) freezeCanonicalValue(child, seen);
	return Object.freeze(value);
}

/** Build K(a) from the default Pi action semantics registry. */
export function buildPiActionKey(tool: string, input: unknown, cwd: string, schemaHash = ""): ActionKey | undefined {
	return PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, schemaHash);
}

export function actionKeyMatches(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[] = [],
): boolean {
	return actionKeyMatch(speculative, actor, projectors) !== undefined;
}

/** K(a_s) covers K(a) without relying on completed-output coverage. */
export function actionKeyCovers(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[] = [],
): boolean {
	const match = actionKeyMatch(speculative, actor, projectors);
	if (!match) return false;
	if (match.kind === "exact") return true;
	const projector = projectors.find((candidate) => candidate.id === match.projector);
	if (!projector?.canShareInFlight) return false;
	try {
		return projector.canShareInFlight(speculative, actor);
	} catch {
		return false;
	}
}

/** K(a_s) can satisfy K(a) exactly, or when some π maps K(a_s) to K(a). */
export function actionKeyMatch(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[] = [],
): ActionKeyMatch | undefined {
	if (speculative.key === actor.key) return { kind: "exact", distance: 0 };
	if (
		speculative.tool !== actor.tool ||
		speculative.semanticsEpoch !== actor.semanticsEpoch ||
		speculative.schemaHash !== actor.schemaHash ||
		speculative.executionFingerprint !== actor.executionFingerprint
	) {
		return undefined;
	}
	let best: ActionKeyMatch | undefined;
	for (const projector of projectors) {
		let projected: ProjectedActionKey | undefined;
		try {
			projected = projector.project(speculative, actor);
		} catch {
			continue;
		}
		if (!projected || projected.action.key !== actor.key) continue;
		if (!Number.isFinite(projected.distance) || projected.distance < 0) continue;
		if (best && best.distance <= projected.distance) continue;
		best = { kind: "projected", projector: projector.id, distance: projected.distance };
	}
	return best;
}

/** Explain why K(a_s) cannot satisfy K(a) without exposing either action's input. */
export function actionKeyMismatchReason(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[] = [],
): ActionKeyMismatchReason | undefined {
	if (actionKeyMatch(speculative, actor, projectors)) return undefined;
	if (speculative.tool !== actor.tool) return "different_tool";
	if (speculative.semanticsEpoch !== actor.semanticsEpoch) return "different_semantics";
	if (speculative.schemaHash !== actor.schemaHash) return "different_schema";
	if (speculative.executionFingerprint !== actor.executionFingerprint) return "different_executor";

	const speculativePartitions = new Set(actionKeyProjectionPartitions(speculative, projectors));
	if (actionKeyProjectionPartitions(actor, projectors).some((partition) => speculativePartitions.has(partition))) {
		return "projection_not_applicable";
	}
	return "different_core";
}

/** Projection partitions used only as an indexed lookup optimization. */
export function actionKeyProjectionPartitions(
	action: ActionKey,
	projectors: readonly ActionKeyProjector[],
): readonly string[] {
	const partitions = new Set<string>();
	for (const projector of projectors) {
		let partition: string | undefined;
		try {
			partition = projector.partition(action);
		} catch {
			continue;
		}
		if (partition !== undefined) partitions.add(JSON.stringify([projector.id, partition]));
	}
	return [...partitions];
}

export function readActionRange(action: ActionKey): ReadActionRange | undefined {
	if (action.tool !== "read") return undefined;
	const input = asRecord(action.input);
	if (!input || typeof input.path !== "string") return undefined;
	const offset = normalizeReadOffset(input.offset);
	const limit = normalizeReadLimit(input.limit);
	return { path: input.path, offset, limit, end: offset + limit - 1 };
}

export function readRangesShareInFlight(speculative: ActionKey, actor: ActionKey): boolean {
	const speculativeRange = readActionRange(speculative);
	const actorRange = readActionRange(actor);
	return (
		!!speculativeRange &&
		!!actorRange &&
		!(actor.input.limit === undefined && speculative.input.limit !== undefined) &&
		readProjectionPartition(speculative) === readProjectionPartition(actor) &&
		speculativeRange.offset <= actorRange.offset &&
		speculativeRange.end >= actorRange.end
	);
}

export function normalizeRelativeRoot(value: unknown, cwd: string): string | undefined {
	if (value !== undefined && typeof value !== "string") return undefined;
	return normalizeWorkspacePath(value ?? ".", cwd);
}

export function inferredActionEffect(tool: string): ActionEffect | undefined {
	return PI_ACTION_SEMANTICS.effect(tool);
}

export function normalizeReadOffset(value: unknown): number {
	return normalizePositiveInteger(value, READ_DEFAULT_OFFSET);
}

export function normalizeReadLimit(value: unknown): number {
	const limit = finiteOrUndefined(value);
	return limit === undefined ? READ_DEFAULT_LIMIT : Math.max(0, Math.floor(limit));
}

function finiteOrUndefined(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function asRecord(value: unknown): Record<string, unknown> | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	return value as Record<string, unknown>;
}

function canonicalRead(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.path !== "string") return undefined;
	if (!validOptionalInteger(record.offset, 1) || !validOptionalInteger(record.limit, 0)) {
		return undefined;
	}
	const resource = normalizeWorkspacePath(record.path, cwd, true);
	if (resource === undefined) return undefined;
	return {
		resources: [resource],
		input: {
			path: resource,
			offset: normalizeReadOffset(record.offset),
			...(record.limit !== undefined ? { limit: normalizeReadLimit(record.limit) } : {}),
		},
	};
}

function canonicalGrep(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.pattern !== "string") return undefined;
	if (!validOptionalInteger(record.context, 0) || !validOptionalInteger(record.limit, 1)) return undefined;
	const root = normalizeRelativeRoot(record.path, cwd);
	if (root === undefined) return undefined;
	return {
		resources: [root],
		input: {
			pattern: record.pattern,
			path: root,
			glob: typeof record.glob === "string" ? record.glob : undefined,
			ignoreCase: record.ignoreCase === true,
			literal: record.literal === true,
			context: normalizeNonNegativeInteger(record.context, 0),
			limit: normalizePositiveInteger(record.limit, GREP_DEFAULT_LIMIT),
		},
	};
}

function canonicalFind(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.pattern !== "string") return undefined;
	if (!validOptionalInteger(record.limit, 1)) return undefined;
	const root = normalizeRelativeRoot(record.path, cwd);
	if (root === undefined) return undefined;
	return {
		resources: [root],
		input: {
			pattern: record.pattern,
			path: root,
			limit: normalizePositiveInteger(record.limit, FIND_DEFAULT_LIMIT),
		},
	};
}

function canonicalLs(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || !validOptionalInteger(record.limit, 1)) return undefined;
	const root = normalizeRelativeRoot(record.path, cwd);
	if (root === undefined) return undefined;
	return {
		resources: [root],
		input: { path: root, limit: normalizePositiveInteger(record.limit, LS_DEFAULT_LIMIT) },
	};
}

function canonicalBash(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.command !== "string") return undefined;
	const normalizedCwd = slash(path.resolve(cwd));
	return {
		resources: [normalizedCwd],
		input: {
			command: record.command,
			cwd: normalizedCwd,
			timeout: finiteOrUndefined(record.timeout),
		},
	};
}

function canonicalWrite(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.path !== "string" || typeof record.content !== "string") return undefined;
	const resource = normalizeWorkspacePath(record.path, cwd);
	if (resource === undefined || resource === ".") return undefined;
	return { resources: [resource], input: { path: resource, content: record.content } };
}

function canonicalEdit(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.path !== "string" || !Array.isArray(record.edits) || record.edits.length === 0) {
		return undefined;
	}
	const edits: Array<{ readonly oldText: string; readonly newText: string }> = [];
	for (const value of record.edits) {
		const edit = asRecord(value);
		if (!edit || typeof edit.oldText !== "string" || typeof edit.newText !== "string") return undefined;
		edits.push({ oldText: edit.oldText, newText: edit.newText });
	}
	const resource = normalizeWorkspacePath(record.path, cwd);
	if (resource === undefined || resource === ".") return undefined;
	return { resources: [resource], input: { path: resource, edits } };
}

function readProjectionPartition(action: ActionKey): string | undefined {
	const range = readActionRange(action);
	if (!range) return undefined;
	return JSON.stringify([
		action.semanticsEpoch,
		action.schemaHash,
		action.executionFingerprint,
		action.resources,
		range.path,
	]);
}

function assertDefinitionCoherence(definition: ActionSemanticsDefinition): void {
	if (definition.effect === "observation") {
		if (definition.resourceScope === undefined) {
			throw new Error(`observation action ${definition.tool} requires resource evidence`);
		}
		if (!definition.requirements.capabilities.includes("validation.resource_snapshot")) {
			throw new Error(`observation action ${definition.tool} requires snapshot-validation capability`);
		}
		return;
	}
	if (definition.resourceScope !== undefined) {
		throw new Error(`non-observation action ${definition.tool} cannot declare resource evidence`);
	}
	if (
		definition.effect === "workspace_mutation" &&
		(!definition.requirements.capabilities.includes("filesystem.write") ||
			!definition.requirements.capabilities.includes("invocation.workspace_path"))
	) {
		throw new Error(`workspace mutation ${definition.tool} requires path-mutation capabilities`);
	}
	if (
		definition.effect === "unbounded" &&
		(!definition.requirements.capabilities.includes("invocation.process") ||
			!definition.requirements.capabilities.includes("output.gate"))
	) {
		throw new Error(`unbounded action ${definition.tool} requires process and external-output capabilities`);
	}
}

const require = createRequire(import.meta.url);
let piPaths: { resolveToCwd: (value: string, cwd: string) => string; resolveReadPath: (value: string, cwd: string) => string };

function normalizeWorkspacePath(value: string, cwd: string, reading = false): string | undefined {
	// Pi does not export its path resolver. Use its installed implementation lazily;
	// an incompatible package layout makes buildKey decline reuse, never invent an identity.
	piPaths ??= require(fileURLToPath(new URL("./core/tools/path-utils.js", import.meta.resolve("@earendil-works/pi-coding-agent"))));
	const root = path.resolve(cwd);
	const target = piPaths.resolveToCwd(value, root);
	// Filename guessing needs a proof of the whole search, not only the chosen file.
	if (reading && piPaths.resolveReadPath(value, root) !== target) return undefined;
	const relative = relativeFilesystemPath(root, target);
	if (relative === undefined) return undefined;
	const resource = slash(relative || ".");
	return piPaths.resolveToCwd(resource, root) === target ? resource : `./${resource}`;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
	const number = finiteOrUndefined(value);
	return number !== undefined && number > 0 ? Math.floor(number) : fallback;
}

function normalizeNonNegativeInteger(value: unknown, fallback: number): number {
	const number = finiteOrUndefined(value);
	return number !== undefined && number >= 0 ? Math.floor(number) : fallback;
}

function validOptionalInteger(value: unknown, minimum: number): boolean {
	return value === undefined || (typeof value === "number" && Number.isSafeInteger(value) && value >= minimum);
}

function fastHash(value: string): string {
	let hash = 0x811c9dc5;
	for (let index = 0; index < value.length; index++) {
		hash ^= value.charCodeAt(index);
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0).toString(16).padStart(8, "0");
}
