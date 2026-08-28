import path from "node:path";
import { stableStringify } from "./stable-json.ts";

/** Observable effects of an action, independent of any concrete isolation backend. */
export type ActionEffect = "observation" | "workspace_mutation" | "unbounded";
export type ResourceDependencyScope = "content" | "tree_entries" | "tree_query" | "tree_content";

export interface ReadActionRange {
	readonly path: string;
	readonly offset: number;
	readonly limit: number;
	readonly end: number;
}

export interface BashTailLinesView {
	/** Exact output-producing command before the final `tail` stage. */
	readonly core: string;
	readonly lines: number;
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
			assertDefinitionCoherence({ ...source, tool, epoch });
			const definition: ActionSemanticsDefinition = Object.freeze({
				...source,
				tool,
				epoch,
				projectors: Object.freeze([...(source.projectors ?? [])]),
			});
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

/** π_bash_tail narrows one complete stdout/stderr suffix to a shorter suffix. */
export const BASH_TAIL_LINES_ACTION_KEY_PROJECTOR: ActionKeyProjector = {
	id: "bash.tail_lines",
	partition: bashTailProjectionPartition,
	project: (speculative, actor) => {
		const speculativeView = bashTailLinesView(speculative);
		const actorView = bashTailLinesView(actor);
		if (!speculativeView || !actorView) return undefined;
		if (bashTailProjectionPartition(speculative) !== bashTailProjectionPartition(actor)) return undefined;
		if (speculativeView.lines < actorView.lines) return undefined;
		return { action: actor, distance: speculativeView.lines - actorView.lines };
	},
	canShareInFlight: (speculative, actor) =>
		BASH_TAIL_LINES_ACTION_KEY_PROJECTOR.project(speculative, actor) !== undefined,
};

export const PI_ACTION_SEMANTICS = new ActionSemanticsRegistry([
	{
		tool: "read",
		epoch: "pi.read.v2",
		effect: "observation",
		resourceScope: "content",
		canonicalize: canonicalRead,
		projectors: [READ_RANGE_ACTION_KEY_PROJECTOR],
	},
	{
		tool: "grep",
		epoch: "pi.grep.v2",
		effect: "observation",
		resourceScope: "tree_content",
		canonicalize: canonicalGrep,
	},
	{
		tool: "find",
		epoch: "pi.find.v2",
		effect: "observation",
		resourceScope: "tree_query",
		canonicalize: canonicalFind,
	},
	{
		tool: "ls",
		epoch: "pi.ls.v1",
		effect: "observation",
		resourceScope: "tree_entries",
		canonicalize: canonicalLs,
	},
	{
		tool: "bash",
		epoch: "pi.bash.v3",
		effect: "unbounded",
		canonicalize: canonicalBash,
		projectors: [BASH_TAIL_LINES_ACTION_KEY_PROJECTOR],
	},
	{
		tool: "write",
		epoch: "pi.write.v1",
		effect: "workspace_mutation",
		canonicalize: canonicalWrite,
	},
	{
		tool: "edit",
		epoch: "pi.edit.v1",
		effect: "workspace_mutation",
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

export function actionKeyProjects(
	speculative: ActionKey,
	actor: ActionKey,
	projectors: readonly ActionKeyProjector[],
): boolean {
	return actionKeyMatch(speculative, actor, projectors)?.kind === "projected";
}

/** Narrow an action-key relation without implying that execution or reuse has completed. */
export function isProjectedActionKeyMatch(match: ActionKeyMatch): match is ProjectedActionKeyMatch {
	return match.kind === "projected";
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

/**
 * Parse the only shell form whose full observable output is a deterministic suffix view.
 *
 * The accepted command is one pipeline whose producer redirects stderr into stdout, optionally
 * preceded by a silent `cd` to an absolute path. Quoted operators are ignored; command lists,
 * substitutions, nested pipelines, and unredirected stderr fail closed.
 */
export function bashTailLinesView(action: ActionKey): BashTailLinesView | undefined {
	if (action.tool !== "bash") return undefined;
	const input = asRecord(action.input);
	if (!input || typeof input.command !== "string") return undefined;
	const command = input.command;
	if (action.executionContext !== undefined) {
		const invocation = asRecord(action.executionContext);
		const process = asRecord(invocation?.process);
		if (process?.command !== command) return undefined;
	}
	const operators = shellOperators(command);
	if (!operators) return undefined;
	const pipelines = operators.filter((operator) => operator.kind === "pipe");
	const conjunctions = operators.filter((operator) => operator.kind === "and");
	if (
		pipelines.length !== 1 ||
		conjunctions.length > 1 ||
		operators.length !== pipelines.length + conjunctions.length
	) {
		return undefined;
	}
	const pipe = pipelines[0]!.index;
	const suffix = command.slice(pipe + 1).trim();
	const tail = /^tail\s+(?:-(\d+)|-n\s+(\d+))$/.exec(suffix);
	const lines = Number(tail?.[1] ?? tail?.[2]);
	if (!tail || !Number.isSafeInteger(lines) || lines <= 0) return undefined;

	let producerStart = 0;
	const conjunction = conjunctions[0];
	if (conjunction) {
		if (conjunction.index > pipe || !silentAbsoluteCd(command.slice(0, conjunction.index))) return undefined;
		producerStart = conjunction.index + 2;
	}
	const producer = command.slice(producerStart, pipe).trim();
	if (!/\s2>&1$/.test(producer)) return undefined;
	return { core: command.slice(0, pipe).trimEnd(), lines };
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

export function finiteOrUndefined(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function asRecord(value: unknown): Record<string, unknown> | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	return value as Record<string, unknown>;
}

export function slash(value: string): string {
	return value.replaceAll("\\", "/");
}

export function contains(root: string, target: string): boolean {
	const relative = path.relative(root, target);
	return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function canonicalRead(input: unknown, cwd: string): CanonicalAction | undefined {
	const record = asRecord(input);
	if (!record || typeof record.path !== "string") return undefined;
	if (!validOptionalInteger(record.offset, 1) || !validOptionalInteger(record.limit, 0)) {
		return undefined;
	}
	const resource = normalizeWorkspacePath(record.path, cwd);
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

function bashTailProjectionPartition(action: ActionKey): string | undefined {
	const view = bashTailLinesView(action);
	const input = asRecord(action.input);
	if (!view || !input) return undefined;
	return stableStringify({
		core: view.core,
		cwd: input.cwd,
		timeout: input.timeout,
		resources: action.resources,
		executionFingerprint: action.executionFingerprint,
	});
}

type ShellOperator = { readonly kind: "pipe" | "and" | "other"; readonly index: number };

function shellOperators(command: string): readonly ShellOperator[] | undefined {
	if (command.includes("\n") || command.includes("\r") || command.includes("\0")) return undefined;
	const operators: ShellOperator[] = [];
	let quote: "single" | "double" | undefined;
	let escaped = false;
	for (let index = 0; index < command.length; index++) {
		const character = command[index]!;
		if (escaped) {
			escaped = false;
			continue;
		}
		if (quote === "single") {
			if (character === "'") quote = undefined;
			continue;
		}
		if (quote === "double") {
			if (character === "\\") escaped = true;
			else if (character === '"') quote = undefined;
			continue;
		}
		if (character === "\\") {
			escaped = true;
			continue;
		}
		if (character === "'") {
			quote = "single";
			continue;
		}
		if (character === '"') {
			quote = "double";
			continue;
		}
		if (character === "`" || character === "(" || character === ")" || character === "{" || character === "}") {
			return undefined;
		}
		if (character === "$" && (command[index + 1] === "(" || command[index + 1] === "{")) return undefined;
		if (character === "|") {
			if (command[index + 1] === "|" || command[index + 1] === "&") {
				operators.push({ kind: "other", index });
				index++;
			} else operators.push({ kind: "pipe", index });
			continue;
		}
		if (character === "&") {
			if (command[index - 1] === ">") continue;
			if (command[index + 1] === "&") {
				operators.push({ kind: "and", index });
				index++;
			} else operators.push({ kind: "other", index });
			continue;
		}
		if (character === ";") operators.push({ kind: "other", index });
	}
	return quote || escaped ? undefined : operators;
}

function silentAbsoluteCd(command: string): boolean {
	const match = /^\s*cd\s+(?:--\s+)?(?:'([^']+)'|"([^"$`\\]+)"|([^\s'"$`\\;&|(){}]+))\s*$/.exec(command);
	const target = match?.[1] ?? match?.[2] ?? match?.[3];
	return typeof target === "string" && target.startsWith("/");
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
		return;
	}
	if (definition.resourceScope !== undefined) {
		throw new Error(`non-observation action ${definition.tool} cannot declare resource evidence`);
	}
}

function normalizeWorkspacePath(value: string, cwd: string): string | undefined {
	const root = path.resolve(cwd);
	const absolute = path.resolve(root, value);
	if (!contains(root, absolute)) return undefined;
	return slash(path.relative(root, absolute) || ".");
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
