import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { relativeFilesystemPath, sameFilesystemPath, slash } from "./path-utils.ts";
import {
	type EffectRequirements,
	normalizeEffectRequirements,
	effectRequirements,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "./effect-model.ts";
import { asRecord, immutableSnapshot, isImmutableSnapshot, stableStringify } from "./stable-json.ts";
import { finiteNumber } from "./number-utils.ts";
import { positiveInteger, nonNegativeInteger } from "./setting-input.ts";

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

/** Resource retrieval is not action equivalence or a correct prediction. */
export interface ResourceInputMatch {
	readonly kind: "inputs";
	readonly distance: number;
}

export type ActionKeyMatch = ExactActionKeyMatch | ProjectedActionKeyMatch | ResourceInputMatch;

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
	/** Eager filesystem scope, or supplied inputs only (never proof of an ambient Actor window). */
	readonly resourceScope?: ResourceDependencyScope | "captured_inputs";
	readonly canonicalize: (input: unknown, cwd: string) => CanonicalAction | undefined;
	readonly projectors?: readonly ActionKeyProjector[];
}

// Only issued definitions may skip normalization; mutable provider records are never memoized.
const normalizedDefinitions = new WeakSet<ActionSemanticsDefinition>();
const projectorSources = new WeakMap<ActionKeyProjector, ActionKeyProjector>();

/** Immutable source of truth for K(a), projection, resource evidence, and safe local fallback capability. */
export class ActionSemanticsRegistry {
	private readonly definitionsByTool = new Map<string, ActionSemanticsDefinition>();
	private readonly projectorsByID = new Map<string, ActionKeyProjector>();

	constructor(definitions: readonly ActionSemanticsDefinition[]) {
		for (const source of definitions) {
			const definition = normalizeDefinition(source), { tool } = definition;
			if (this.definitionsByTool.has(tool)) throw new Error(`duplicate action semantics for ${tool}`);
			this.definitionsByTool.set(tool, definition);
			for (const projector of definition.projectors ?? []) {
				const existing = this.projectorsByID.get(projector.id);
				if (existing && projectorSources.get(existing) !== projectorSources.get(projector)) {
					throw new Error(`conflicting action projector ${projector.id}`);
				}
				this.projectorsByID.set(projector.id, projector);
			}
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
		execution?: { readonly fingerprint: string; readonly context?: unknown; readonly semantics?: ActionSemanticsDefinition },
	): ActionKey | undefined {
		const { fingerprint, context, semantics } = execution ?? {};
		const definition = semantics ? normalizeDefinition(semantics) : this.definition(tool);
		if (!definition) return undefined;
		try {
			const canonical = definition.canonicalize(input, cwd);
			if (!canonical || !canonical.resources.every((resource) => typeof resource === "string")) return undefined;
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

export const READ_DEFAULT_OFFSET = 1;
export const READ_DEFAULT_LIMIT = 2000;
export const GREP_DEFAULT_LIMIT = 100;
export const FIND_DEFAULT_LIMIT = 1000;
export const LS_DEFAULT_LIMIT = 500;

/** π_read narrows a cached read action to the actor's requested interval. */
export const READ_RANGE_ACTION_KEY_PROJECTOR: ActionKeyProjector = ownActionKeyProjector({
	id: "read.range",
	partition: readProjectionPartition,
	project: (speculative, actor) => {
		const speculativeRange = readActionRange(speculative);
		const actorRange = readActionRange(actor);
		if (!speculativeRange || !actorRange) return undefined;
		if (readProjectionPartition(speculative) !== readProjectionPartition(actor)) return undefined;
		if (speculativeRange.limit === 0 || speculativeRange.offset > actorRange.offset || actorRange.offset > speculativeRange.end + 1) return undefined;
		return { action: actor, distance: actorRange.offset - speculativeRange.offset + Math.abs(speculativeRange.end - actorRange.end) };
	},
	canShareInFlight: readRangesShareInFlight,
});

// Stock query tools launch ambient executables/configuration (rg can even invoke --pre).
// A static workspace tree is not their dependency closure or their isolation authority.
const HOST_PROCESS_EFFECTS = effectRequirements("invocation.host_function", ...UNRESTRICTED_PROCESS_EFFECTS.capabilities);

/** A command that finished within its timeout finishes, with the same output, under any longer or no timeout. */
export const BASH_TIMEOUT_ACTION_KEY_PROJECTOR: ActionKeyProjector = ownActionKeyProjector({
	id: "bash.timeout",
	partition: bashTimeoutPartition,
	project: (speculative, actor) => bashTimeoutCovers(speculative, actor) ? { action: actor, distance: 1 } : undefined,
	canShareInFlight: bashTimeoutCovers,
});

export const PI_ACTION_SEMANTICS = new ActionSemanticsRegistry(([
	{ tool: "read", effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "content", projectors: [READ_RANGE_ACTION_KEY_PROJECTOR] },
	{ tool: "grep", effect: "unbounded", requirements: HOST_PROCESS_EFFECTS },
	{ tool: "find", effect: "unbounded", requirements: HOST_PROCESS_EFFECTS },
	{ tool: "ls", effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "entries" },
	{ tool: "bash", effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS, projectors: [BASH_TIMEOUT_ACTION_KEY_PROJECTOR] },
	{ tool: "write", effect: "workspace_mutation", requirements: WORKSPACE_PATH_MUTATION_EFFECTS },
	{ tool: "edit", effect: "workspace_mutation", requirements: WORKSPACE_PATH_MUTATION_EFFECTS },
] satisfies Omit<ActionSemanticsDefinition, "epoch" | "canonicalize">[]).map((definition): ActionSemanticsDefinition => ({
	...definition,
	epoch: `pi.${definition.tool}`,
	canonicalize: (input, cwd) => canonicalPiAction(definition.tool, input, cwd),
})));

export const OBSERVATION_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("observation"));
export const WORKSPACE_MUTATION_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("workspace_mutation"));
export const UNBOUNDED_ACTION_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames("unbounded"));
export const KEYABLE_TOOLS = Object.freeze(PI_ACTION_SEMANTICS.toolNames());

export function buildActionKey(input: {
	readonly tool: string;
	readonly resources: readonly string[];
	readonly resourceRoot?: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly schemaHash?: string;
	readonly semanticsEpoch?: string;
	readonly executionFingerprint?: string;
	readonly executionContext?: unknown;
	readonly semantics?: ActionSemanticsDefinition;
}): ActionKey {
	const schemaHash = input.schemaHash ?? "";
	const semanticsEpoch = input.semanticsEpoch ?? "";
	const executionFingerprint = input.executionFingerprint ?? "";
	const semantics = input.semantics ? normalizeDefinition(input.semantics) : undefined;
	if (semantics && (semantics.tool !== input.tool || semantics.epoch !== semanticsEpoch)) throw new Error("action contract identity mismatch");
	const canonicalInput = immutableSnapshot(input.input);
	if (!isImmutableSnapshot(canonicalInput)) throw new Error("Action input has no immutable data identity");
	const key = stableStringify({ tool: input.tool, semanticsEpoch, schemaHash, executionFingerprint, input: canonicalInput });
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
	return actionKeyMatch(speculative, actor, projectors, true) !== undefined;
}

/** Lookup relation only; adoption still requires realized output/input coverage and branch evidence. */
export function actionKeyMatch(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[] = [],
	/** Prediction matching requires request containment, not merely overlapping reusable inputs. */
	requireCoverage = false,
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
			if (requireCoverage && projector.canShareInFlight?.(speculative, actor) !== true) continue;
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
	return positiveInteger(value, READ_DEFAULT_OFFSET);
}

export function normalizeReadLimit(value: unknown): number {
	const limit = finiteNumber(value);
	return limit === undefined ? READ_DEFAULT_LIMIT : Math.max(0, Math.floor(limit));
}

function canonicalPiAction(tool: string, input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record) return undefined;
	if (tool === "bash") {
		if (typeof record.command !== "string") return undefined;
		const resource = slash(path.resolve(cwd));
		return { resources: [resource], input: { command: record.command, cwd: resource,
			...(finiteNumber(record.timeout) !== undefined ? { timeout: record.timeout } : {}) } };
	}
	const query = tool === "grep" || tool === "find" || tool === "ls";
	if (!query && typeof record.path !== "string") return undefined;
	let fields: Record<string, unknown>;
	if (query) {
		if (!validOptionalInteger(record.limit, 1) || (tool !== "ls" && typeof record.pattern !== "string")) return undefined;
		fields = { ...(tool !== "ls" ? { pattern: record.pattern } : {}), path: record.path };
		if (tool === "grep") {
			if (!validOptionalInteger(record.context, 0)) return undefined;
			Object.assign(fields, {
				...(typeof record.glob === "string" ? { glob: record.glob } : {}),
				ignoreCase: record.ignoreCase === true, literal: record.literal === true,
				context: nonNegativeInteger(record.context, 0),
			});
		}
		fields.limit = positiveInteger(record.limit, tool === "grep" ? GREP_DEFAULT_LIMIT : tool === "find" ? FIND_DEFAULT_LIMIT : LS_DEFAULT_LIMIT);
	} else if (tool === "read") {
		if (!validOptionalInteger(record.offset, 1) || !validOptionalInteger(record.limit, 0)) return undefined;
		fields = { path: record.path, offset: normalizeReadOffset(record.offset),
			...(record.limit !== undefined ? { limit: normalizeReadLimit(record.limit) } : {}) };
	} else if (tool === "write") {
		if (typeof record.content !== "string") return undefined;
		fields = { path: record.path, content: record.content };
	} else if (tool === "edit") {
		if (!Array.isArray(record.edits) || !record.edits.length) return undefined;
		const edits: Array<{ readonly oldText: string; readonly newText: string }> = [];
		for (const value of record.edits) {
			const edit = asRecord(value);
			if (!edit || typeof edit.oldText !== "string" || typeof edit.newText !== "string") return undefined;
			edits.push({ oldText: edit.oldText, newText: edit.newText });
		}
		fields = { path: record.path, edits };
	} else return undefined;
	const resource = query ? normalizeRelativeRoot(record.path, cwd) : normalizeWorkspacePath(record.path as string, cwd, tool === "read");
	if (resource === undefined || ((tool === "write" || tool === "edit") && resource === ".")) return undefined;
	fields.path = resource;
	return { resources: [resource], input: fields };
}

/** Everything but the timeout: internal process operations share a Bash key shape without a command, and never project. */
function bashTimeoutPartition(action: ActionKey): string | undefined {
	return action.tool === "bash" && typeof action.input.command === "string" ? stableStringify([action.semanticsEpoch, action.schemaHash,
		action.executionFingerprint, action.resources, Object.entries(action.input).filter(([name]) => name !== "timeout")]) : undefined;
}

function bashTimeoutCovers(speculative: ActionKey, actor: ActionKey): boolean {
	const limit = (action: ActionKey) => typeof action.input.timeout === "number" ? action.input.timeout : Number.POSITIVE_INFINITY;
	return bashTimeoutPartition(speculative) !== undefined && bashTimeoutPartition(speculative) === bashTimeoutPartition(actor) && limit(actor) >= limit(speculative);
}

function readProjectionPartition(action: ActionKey): string | undefined {
	const range = readActionRange(action);
	if (!range) return undefined;
	return JSON.stringify([ action.semanticsEpoch, action.schemaHash, action.executionFingerprint, action.resources, range.path]);
}

/** Capture rule slots once; retain source identity only for conflicting-registration checks. */
export function ownActionKeyProjector<Projector extends ActionKeyProjector>(source: Projector): Projector {
	if (projectorSources.has(source)) return source;
	const owned = Object.freeze({ ...source });
	projectorSources.set(owned, source);
	return owned;
}

function normalizeDefinition(source: ActionSemanticsDefinition): ActionSemanticsDefinition {
	if (normalizedDefinitions.has(source)) return source;
	const tool = source.tool.trim(), epoch = source.epoch.trim();
	if (!tool) throw new Error("action semantics tool must not be empty");
	if (!epoch) throw new Error(`action semantics epoch must not be empty for ${tool}`);
	const definition = Object.freeze({ ...source, tool, epoch,
		requirements: normalizeEffectRequirements(source.requirements),
		projectors: Object.freeze([...new Set(source.projectors ?? [])].map(ownActionKeyProjector)),
	});
	assertDefinitionCoherence(definition);
	normalizedDefinitions.add(definition);
	return definition;
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

/** Pi's resolution of a tool path; `reading` adds its filename guessing. Pi does not export the resolver, so use its installed one lazily. */
export function resolvePiToolPath(value: string, cwd: string, reading = false): string {
	piPaths ??= require(fileURLToPath(new URL("./core/tools/path-utils.js", import.meta.resolve("@earendil-works/pi-coding-agent"))));
	return (reading ? piPaths.resolveReadPath : piPaths.resolveToCwd)(value, cwd);
}

function normalizeWorkspacePath(value: string, cwd: string, reading = false): string | undefined {
	// An incompatible package layout makes buildKey decline reuse, never invent an identity.
	const root = path.resolve(cwd), target = resolvePiToolPath(value, root);
	// Filename guessing needs a proof of the whole search, not only the chosen file.
	if (reading && resolvePiToolPath(value, root, true) !== target) return undefined;
	const relative = relativeFilesystemPath(root, target);
	if (relative === undefined) return undefined;
	const resource = slash(relative || ".");
	return sameFilesystemPath(resolvePiToolPath(resource, root), target) ? resource : `./${resource}`;
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
