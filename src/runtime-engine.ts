import { serialize } from "node:v8";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type {
	ActionKey,
	ActionKeyMatch,
	ActionKeyProjector,
	ActionProjectionCoverage,
	ActionProjectionRule,
	ActionSemanticsRegistry,
} from "./action-semantics.ts";
import {
	actionKeyCovers,
	actionKeyMatch,
	actionKeyProjectionPartitions,
	ownActionKeyProjector,
	PI_ACTION_SEMANTICS,
	resolveActionProjectionRules,
} from "./action-semantics.ts";
import type { AgentPlanSource, AgentRuntime } from "./agent-integration.ts";
import type { DrafterToolDefinition } from "./config-schema.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	normalizeSchedulingSettings,
	SCHEDULING_DEFAULTS,
	speculativeConfig,
} from "./config-schema.ts";
import type {
	ExecutionOperationAdoption,
	ExecutionOperationBinding,
	ExecutionScope,
	SpeculativeExecutionRoute,
	WorldBranch,
} from "./execution-world.ts";
import {
	effectCommitFailure,
	isPoisonedEffectCommit,
	sameSpeculativeExecutionRoute,
	validateWorldBranch,
} from "./execution-world.ts";
import {
	asRecord,
	BoundedRecencyMap,
	cloneSharedData,
	containsLogicalPath,
	dependencyOrder,
	errorDetail,
	filesystemPathKey,
	nonNegativeCount as finiteLimit,
	finiteNumber,
	nonNegativeFinite as finiteValue,
	immutableSnapshot,
	isFiniteNumber,
	isImmutableSnapshot,
	isSafeCount,
	positiveCount,
} from "./primitives.ts";
import type { ResourceVersionToken } from "./resource-version.ts";
import { observeResourceChanges } from "./resource-version.ts";
import { PendingTasks, RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import type {
	ActorActionIdentity,
	ActorCandidateSelection,
	ActualToolCall,
	AdoptedAction,
	CandidateEventDescriptor,
	CandidateReservation,
	MaterializedPlan,
	PlanAction,
	PlanActionDependency,
	PlanActionIdentity,
	PlanContinuation,
	PlanUpdate,
	PredictionAdoption,
	PredictionIdentity,
	PredictionSettlement,
	PreparedActorCall,
	ResolutionCause,
	ResourceValidation,
	RuntimeEventMap,
	RuntimeTurnContext,
	SettledSourceRequest,
	SourceRequestIdentity,
	SourceRequestKind,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionRuntimeAdapter,
	SpeculativeActionSettings,
	SpeculativeCacheSnapshot,
	SpeculativeCandidate,
	SpeculativeDraftCandidate,
	SpeculativePlanSource,
	TurnInput,
} from "./settlement.ts";
import {
	ActorAction,
	BoundedEventQueue,
	CandidateExecution,
	cause,
	diagnosticAction,
	PostSettlementQueue,
	predictionLearning,
	runSourceRequest,
	RuntimeSubscriptions,
	SourceGeneration,
} from "./settlement.ts";
import type {
	ExecutionIdentity,
	HardwareResources,
	PredictionForecast,
	ScheduledWork,
} from "./system-resources.ts";
import { RESOURCE_DIMENSIONS, SpeculationScheduler } from "./system-resources.ts";
import { TaskTimeline, TimelineInterval } from "./task-timing.ts";
export interface CandidateStoreEntry {
	readonly id: string;
	readonly key?: ActionKey;
	readonly estimatedBytes: number;
}
export interface CandidateLookup<Entry> {
	readonly entry: Entry;
	readonly match: ActionKeyMatch;
}
interface IndexedEntry<Entry> {
	readonly entry: Entry;
	readonly memberships: Set<string>;
	inputs?: boolean;
	recency: number;
	result?: ResultCacheEvidence;
}
interface IndexedScope<Entry> {
	readonly entries: Map<string, IndexedEntry<Entry>>;
	/** Pending work, tagged exact/input/view keys and JSON projections occupy disjoint namespaces. */
	readonly partitions: Map<string, Set<IndexedEntry<Entry>>>;
}
/** One registration survives execution, settlement and cache retention. Keys identify reuse, IDs own work. */
export class CandidateStore<Scope, Entry extends CandidateStoreEntry> {
	private readonly scopes = new Map<Scope, IndexedScope<Entry>>();
	private readonly waiters = new Map<Scope, Set<(changed: boolean) => void>>();
	private readonly projectors: readonly ActionKeyProjector[];
	private readonly score: (entry: Entry, evidence: ResultCacheEvidence, now: number) => number;
	private readonly now: () => number;
	private sequence = 0;
	constructor(
		projectors: readonly ActionKeyProjector[] = [],
		score: (entry: Entry, evidence: ResultCacheEvidence, now: number) => number = () => 0,
		now: () => number = Date.now,
	) {
		this.projectors = projectors.map(ownActionKeyProjector);
		this.score = score;
		this.now = now;
	}
	insert(scope: Scope, entry: Entry): Entry | undefined {
		return (
			this.get(scope, entry.id) ??
			this.add(scope, entry, entry.key ? actionKeyProjectionPartitions(entry.key, this.projectors) : [])
		);
	}
	/** Observe before yielding: a later producer or sealed view may satisfy an existing consumer. */
	observe(scope: Scope): {
		signal: AbortSignal;
		changed: boolean;
		dispose: () => void;
	} {
		const waiters = this.waiters.get(scope) ?? new Set<(changed: boolean) => void>();
		const controller = new AbortController();
		const observation = {
			signal: controller.signal,
			changed: false,
			dispose: () => {
				waiters.delete(wake);
				if (!waiters.size) this.waiters.delete(scope);
			},
		};
		const wake = (changed: boolean) => {
			observation.changed ||= changed;
			controller.abort();
		};
		waiters.add(wake);
		this.waiters.set(scope, waiters);
		return observation;
	}
	/** Availability wakes waiters; only new result evidence invalidates a search pass. */
	notify(scope: Scope, changed = true): void {
		for (const wake of this.waiters.get(scope) ?? []) wake(changed);
	}
	getOrCreate(
		scope: Scope,
		key: ActionKey,
		create: () => Entry,
		canReuse: (existing: Entry, match: ActionKeyMatch) => boolean = (_entry, match) => match.kind === "exact",
	): CandidateLookup<Entry> & {
		readonly inserted: boolean;
	} {
		const partitions = actionKeyProjectionPartitions(key, this.projectors);
		for (const { entry: existing, match, indexed } of this.lookupRecords(scope, key, partitions).sort(
			(left, right) => Number(right.match.kind === "exact") - Number(left.match.kind === "exact"),
		)) {
			if (
				this.record(scope, existing) !== indexed ||
				!canReuse(existing, match) ||
				this.record(scope, existing) !== indexed
			)
				continue;
			return { entry: existing, match, inserted: false };
		}
		const entry = create();
		const existing = this.add(scope, entry, partitions);
		return {
			entry: existing ?? entry,
			match: { kind: "exact", distance: 0 },
			inserted: !existing,
		};
	}
	get(scope: Scope, id: string): Entry | undefined {
		return this.scopes.get(scope)?.entries.get(id)?.entry;
	}
	has(scope: Scope, entry: Entry): boolean {
		return this.get(scope, entry.id) === entry;
	}
	lookup(
		scope: Scope,
		action: ActionKey,
		requireCoverage?: (entry: Entry) => boolean,
		includeInputs = true,
	): readonly CandidateLookup<Entry>[] {
		return this.lookupRecords(
			scope,
			action,
			actionKeyProjectionPartitions(action, this.projectors),
			requireCoverage,
			includeInputs,
		).map(({ entry, match }) => ({ entry, match }));
	}
	/** Path lookup and dynamic query discovery share the same bounded input owners. */
	lookupInputs(scope: Scope, resources?: readonly string[]): readonly Entry[] {
		return [...this.inputRecords(scope, resources)]
			.sort(([left, a], [right, b]) => a - b || right.recency - left.recency)
			.map(([{ entry }]) => entry);
	}
	/** Backend-confirmed name revocation does not erase the owner's output evidence or other inputs. */
	invalidateInputs(scope: Scope, entry: Entry, resources: readonly string[]): void {
		const indexed = this.record(scope, entry);
		const state = this.scopes.get(scope);
		if (!indexed || !state || !resources.length) return;
		const keys = new Set(
			resources.flatMap((resource) => [inputPartition(resource), inputPartition(resource, true)]),
		);
		this.unindex(state, indexed, keys);
	}
	/** Sealed queries remain addressable after their reconstruction inputs are revoked. */
	indexView(scope: Scope, entry: Entry, actionKey: string, retained: boolean): void {
		const indexed = this.record(scope, entry);
		const state = this.scopes.get(scope);
		if (!indexed || !state) return;
		const key = `view:${actionKey}`;
		if (retained) this.index(state, indexed, [key]);
		else this.unindex(state, indexed, [key]);
		this.notify(scope);
	}
	delete(scope: Scope, entry: Entry): boolean {
		const indexed = this.record(scope, entry);
		const state = this.scopes.get(scope);
		if (!indexed || !state) return false;
		state.entries.delete(entry.id);
		this.unindex(state, indexed);
		if (!state.entries.size) this.scopes.delete(scope);
		this.notify(scope);
		return true;
	}
	values(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.entries.values() ?? [])].map(({ entry }) => entry);
	}
	pending(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.partitions.get("pending") ?? [])].map(({ entry }) => entry);
	}
	/** A present input capability may own calculations without paths; retention preserves its existing identity. */
	settle(scope: Scope, entry: Entry, branch: WorldBranch<unknown>, shared = true): void {
		this.insert(scope, entry);
		const indexed = this.record(scope, entry);
		if (!indexed) return;
		this.unindex(this.scopes.get(scope)!, indexed, ["pending"]);
		indexed.result ??= {
			segment: "cold",
			insertedAt: this.now(),
			actorHits: 0,
		};
		if (shared && !indexed.inputs && (branch.inputSource || branch.reconstruct)) {
			this.index(
				this.scopes.get(scope)!,
				indexed,
				branch.inputResources?.map((input) => inputPartition(input.path, input.descendants)) ?? [],
			);
			indexed.inputs = true;
		}
		this.notify(scope);
	}
	cached(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.entries.values() ?? [])]
			.filter(({ result }) => result)
			.map(({ entry }) => entry);
	}
	recordActorHit(scope: Scope, entry: Entry, limits?: ResultCacheLimits): readonly Entry[] {
		const indexed = this.record(scope, entry);
		if (!indexed?.result) return [];
		indexed.result = {
			...indexed.result,
			segment: "hot",
			actorHits: indexed.result.actorHits + 1,
			lastActorHitAt: this.now(),
		};
		indexed.recency = this.sequence++;
		if (!limits) return [];
		const fraction = finiteFraction(limits.hotFraction ?? 0.8);
		const capacity = finiteLimit(limits.maxEntries);
		return this.retireExcess(
			scope,
			this.cached(scope).filter((item) => this.record(scope, item)!.result!.segment === "hot"),
			{
				maxEntries: !capacity || !fraction ? 0 : Math.max(1, Math.floor(capacity * fraction)),
				maxBytes: Math.floor(finiteLimit(limits.maxBytes) * fraction),
			},
			(record) => {
				record.result = { ...record.result!, segment: "cold" };
			},
		);
	}
	snapshot(scope: Scope, include: (entry: Entry) => boolean = () => true): ResultCacheSnapshot {
		const snapshot = {
			coldEntries: 0,
			hotEntries: 0,
			coldBytes: 0,
			hotBytes: 0,
		};
		for (const { entry, result } of this.scopes.get(scope)?.entries.values() ?? []) {
			if (result && include(entry)) {
				snapshot[result.segment === "hot" ? "hotEntries" : "coldEntries"]++;
				snapshot[result.segment === "hot" ? "hotBytes" : "coldBytes"] += finiteValue(entry.estimatedBytes);
			}
		}
		return snapshot;
	}
	trim(scope: Scope, limits: ResultCacheLimits, canEvict: (entry: Entry) => boolean = () => true): Entry[] {
		return this.retireExcess(
			scope,
			this.cached(scope),
			limits,
			({ entry }) => {
				this.delete(scope, entry);
			},
			canEvict,
		);
	}
	private record(scope: Scope, entry: Entry): IndexedEntry<Entry> | undefined {
		const record = this.scopes.get(scope)?.entries.get(entry.id);
		return record?.entry === entry ? record : undefined;
	}
	private index(state: IndexedScope<Entry>, indexed: IndexedEntry<Entry>, keys: Iterable<string>): void {
		for (const key of keys) {
			const members = state.partitions.get(key) ?? new Set();
			members.add(indexed);
			state.partitions.set(key, members);
			indexed.memberships.add(key);
		}
	}
	private unindex(
		state: IndexedScope<Entry>,
		indexed: IndexedEntry<Entry>,
		keys: Iterable<string> = indexed.memberships,
	): void {
		for (const key of keys) {
			const members = state.partitions.get(key);
			members?.delete(indexed);
			if (!members?.size) state.partitions.delete(key);
			indexed.memberships.delete(key);
		}
	}
	private inputRecords(scope: Scope, resources?: readonly string[]) {
		const state = this.scopes.get(scope);
		const inputs = new Map<IndexedEntry<Entry>, number>();
		if (state)
			for (const indexed of state.entries.values()) {
				if (indexed.inputs) inputs.set(indexed, speculativeConfig().runtime.inputDistanceRange);
			}
		if (state)
			for (const resource of resources ?? []) {
				let current = path.resolve(resource);
				let depth = 0;
				for (;;) {
					const keys = depth
						? [inputPartition(current, true)]
						: [inputPartition(current), inputPartition(current, true)];
					for (const key of keys)
						for (const indexed of state.partitions.get(key) ?? [])
							inputs.set(indexed, Math.min(inputs.get(indexed) ?? depth, depth));
					const parent = path.dirname(current);
					if (parent === current) break;
					current = parent;
					depth++;
				}
			}
		return inputs;
	}
	private lookupRecords(
		scope: Scope,
		action: ActionKey,
		partitions: readonly string[],
		requireCoverage?: (entry: Entry) => boolean,
		includeInputs = false,
	) {
		const state = this.scopes.get(scope);
		if (!state) return [];
		const inputs = includeInputs
			? this.inputRecords(
					scope,
					action.resources.flatMap((resource) =>
						action.resourceRoot !== undefined || path.isAbsolute(resource)
							? [path.resolve(action.resourceRoot ?? "", resource)]
							: [],
					),
				)
			: new Map<IndexedEntry<Entry>, number>();
		const views = includeInputs ? state.partitions.get(`view:${action.key}`) : undefined;
		const candidates = new Set([
			...(state.partitions.get(`exact:${action.key}`) ?? []),
			...inputs.keys(),
			...(views ?? []),
		]);
		for (const key of partitions)
			for (const indexed of state.partitions.get(key) ?? []) candidates.add(indexed);
		const ranked: (CandidateLookup<Entry> & {
			readonly indexed: IndexedEntry<Entry>;
		})[] = [];
		for (const indexed of candidates) {
			if (this.record(scope, indexed.entry) !== indexed) continue;
			const coverage = requireCoverage?.(indexed.entry) ?? false;
			const match =
				(indexed.entry.key && actionKeyMatch(indexed.entry.key, action, this.projectors, coverage)) ??
				(!coverage && (inputs.has(indexed) || views?.has(indexed))
					? {
							kind: "inputs" as const,
							distance:
								Number.MAX_SAFE_INTEGER -
								speculativeConfig().runtime.inputDistanceRange +
								Math.min(inputs.get(indexed) ?? 0, speculativeConfig().runtime.inputDistanceRange),
						}
					: undefined);
			if (match) ranked.push({ entry: indexed.entry, match, indexed });
		}
		// Provider callbacks may retire or replace a registration, even with the same object and ID.
		return ranked
			.filter(({ entry, indexed }) => this.record(scope, entry) === indexed)
			.sort(
				(left, right) =>
					left.match.distance - right.match.distance || right.indexed.recency - left.indexed.recency,
			);
	}
	private add(scope: Scope, entry: Entry, partitions: readonly string[]): Entry | undefined {
		const state: IndexedScope<Entry> = this.scopes.get(scope) ?? {
			entries: new Map(),
			partitions: new Map(),
		};
		const existing = state.entries.get(entry.id);
		if (existing) return existing.entry;
		const indexed: IndexedEntry<Entry> = {
			entry,
			memberships: new Set(),
			recency: this.sequence++,
		};
		state.entries.set(entry.id, indexed);
		this.index(state, indexed, ["pending", ...(entry.key ? [`exact:${entry.key.key}`] : []), ...partitions]);
		this.scopes.set(scope, state);
		this.notify(scope);
		return undefined;
	}
	/** Rank once; protected results still occupy the budget, and callbacks cannot retire a replacement. */
	private retireExcess(
		scope: Scope,
		entries: readonly Entry[],
		limits: ResultCacheLimits,
		retire: (record: IndexedEntry<Entry>) => void,
		canRetire: (entry: Entry) => boolean = () => true,
	): Entry[] {
		let count = entries.length;
		let bytes = entries.reduce((total, entry) => total + finiteValue(entry.estimatedBytes), 0);
		const maxEntries = finiteLimit(limits.maxEntries);
		const maxBytes = finiteLimit(limits.maxBytes);
		const withinBudget = () => count <= maxEntries && bytes <= maxBytes;
		if (withinBudget()) return [];
		const now = this.now();
		const ranked = [];
		for (const entry of entries) {
			const indexed = this.record(scope, entry);
			const evidence = indexed?.result;
			if (evidence && canRetire(entry))
				ranked.push({
					entry,
					indexed,
					evidence,
					hot: Number(evidence.segment === "hot"),
					value: finiteValue(this.score(entry, { ...evidence }, now)),
				});
		}
		ranked.sort((left, right) => left.hot - right.hot || left.value - right.value);
		const retired: Entry[] = [];
		for (const { entry, indexed, evidence } of ranked) {
			if (withinBudget()) break;
			if (!canRetire(entry) || this.record(scope, entry) !== indexed || indexed?.result !== evidence)
				continue;
			retired.push(entry);
			count--;
			bytes -= finiteValue(entry.estimatedBytes);
			retire(indexed);
		}
		return retired;
	}
}
const inputPartition = (resource: string, descendants = false) =>
	`inputs:${descendants ? "tree" : "path"}:` + filesystemPathKey(resource);
export type ResultCacheSegment = "cold" | "hot";
export interface ResultCacheLimits {
	readonly maxEntries: number;
	readonly maxBytes: number;
	/** Maximum hot share; lower-value hot entries return to cold before eviction pressure. */
	readonly hotFraction?: number;
}
export function resultCacheRecency(
	evidence: Pick<ResultCacheEvidence, "insertedAt" | "lastActorHitAt">,
): number {
	return evidence.lastActorHitAt ?? evidence.insertedAt;
}
export interface ResultCacheEvidence {
	readonly segment: ResultCacheSegment;
	readonly insertedAt: number;
	readonly actorHits: number;
	readonly lastActorHitAt?: number;
}
export interface ResultCacheSnapshot {
	readonly coldEntries: number;
	readonly hotEntries: number;
	readonly coldBytes: number;
	readonly hotBytes: number;
}
function finiteFraction(value: number): number {
	return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : speculativeConfig().runtime.hotFraction;
}
type Operation = {
	readonly key: string;
	readonly parent: string;
	readonly binding: ExecutionOperationBinding;
};
type Request = {
	readonly revision: number;
	readonly kind: "observed" | "workspace" | "edit";
};
type Command = {
	readonly parent: string;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly schemaHash?: string;
	request?: Request;
};
type Preparation = {
	changes?: Promise<ResourceVersionToken | undefined>;
	preparing?: Promise<PlanAction[]>;
};
/** Backend-issued capabilities retain their real parent call and use the existing runtime admission and resource ledger. */
export function createExecutionPreparation(cwd: string, semantics: ActionSemanticsRegistry) {
	const lifetime = new RuntimeLifecycleLane();
	const operations = new BoundedRecencyMap<string, Operation>(speculativeConfig().runtime.preparedOperations);
	const commands = new BoundedRecencyMap<string, Command>(speculativeConfig().runtime.preparedCommands);
	const feedback = new WeakMap<
		object,
		{
			command: Command;
			request: Request;
			operation?: Operation;
		}
	>();
	let state: Preparation | undefined;
	let revision = 0;
	const current = (binding: ExecutionOperationBinding) =>
		binding.available !== false &&
		binding.preparation === "current_workspace" &&
		!binding.fed &&
		!!binding.stale;
	const operationID = (binding: ExecutionOperationBinding) => `${binding.backend}:${binding.identity}`;
	const release = (owner = state) => {
		return owner?.changes && lifetime.track(owner.changes.then((token) => token?.release()));
	};
	const configure = (settings: SpeculativeActionSettings) => {
		if (
			lifetime.sealed ||
			!settings.enabled ||
			settings.reuseEnabled === false ||
			!(settings.prepareObservedOperations ?? DEFAULTS.prepareObservedOperations)
		) {
			release();
			state = undefined;
		} else state ??= {};
		return state;
	};
	const children = (parent: string) =>
		[...operations.values()].filter((operation) => {
			if (operation.binding.available !== false) return operation.parent === parent;
			operations.delete(operation.key);
			return false;
		});
	const observeWorkspace = async (owner: Preparation) => {
		if (state !== owner || ![...operations.values()].some((operation) => current(operation.binding))) return;
		// Cursor notifications schedule work; every execution and adoption still proves its inputs.
		owner.changes = (owner.changes ?? Promise.resolve(undefined)).then(async (previous) => {
			const next = await observeResourceChanges(cwd).catch(() => undefined);
			const changed = previous?.manager.changesSince(previous);
			await previous?.release();
			if (state !== owner) {
				await next?.release();
				return undefined;
			}
			if (
				changed &&
				!changed.uncertain &&
				changed.paths.some((target) => !path.relative(cwd, target).split(path.sep).includes(".git"))
			)
				for (const command of commands.values())
					command.request = {
						revision: ++revision,
						kind: command.request?.kind ?? "workspace",
					};
			return next;
		});
		await owner.changes;
	};
	const prepare = async (
		owner: Preparation,
		schemas: Readonly<Record<string, string>>,
	): Promise<PlanAction[]> => {
		const pending = (owner.preparing ?? Promise.resolve())
			.catch(() => {})
			.then(async () => {
				const actions = new Map<string, PlanAction | undefined>();
				for (const command of [...commands.values()].reverse()) {
					const request = command.request;
					if (state !== owner) return [];
					if (!request || command.schemaHash !== schemas[command.tool]) continue;
					const observedOnly = request.kind === "workspace";
					const bindings = children(command.parent).filter(
						(operation) => !observedOnly || current(operation.binding),
					);
					const stale = (
						await Promise.all(
							bindings.map(async (operation) => {
								const changed = await operation.binding.stale?.();
								return (observedOnly ? changed === true : changed !== false) ? operation : undefined;
							}),
						)
					).filter(
						(operation): operation is Operation => !!operation && operation.binding.available !== false,
					);
					if (state !== owner) return [];
					if (command.request !== request || ![...commands.values()].includes(command)) continue;
					if (!stale.length) {
						command.request = undefined;
						continue;
					}
					const selections = stale.filter(
						({ binding }) =>
							request.kind === "observed" || (binding.preparation === "current_workspace" && !binding.fed),
					);
					// Replaying the parent establishes captured inputs in order and covers its independent children.
					if (!selections.length)
						for (const { binding } of bindings)
							if (!binding.fed) actions.set(operationID(binding), undefined);
					for (const selected of selections.length ? selections : [undefined]) {
						const operation = selected?.binding;
						const key = operation ? operationID(operation) : command.parent;
						if (operation && actions.has(key)) continue;
						const token = {};
						feedback.set(token, { command, request, operation: selected });
						actions.set(key, {
							id: `prepare:${request.revision}:${key}`,
							type: operation ? "operation" : "tool_call",
							...(operation ? { operation } : {}),
							tool: command.tool,
							input: command.input,
							horizon: 0,
							producesOperations: true,
							mode: "observed-work",
							feedback: token,
							...(operation?.fed ? { background: true } : {}),
						});
					}
				}
				return [...actions.values()].filter((action): action is PlanAction => !!action);
			});
		owner.preparing = pending;
		try {
			return await pending;
		} finally {
			if (owner.preparing === pending) owner.preparing = undefined;
		}
	};
	const proposal = async (
		owner: Preparation,
		turn: string,
		schemas: Readonly<Record<string, string>>,
		signal?: AbortSignal,
		reserve?: (id: string, minimum: number) => number,
	) => {
		await owner.changes;
		const actions = signal?.aborted ? [] : await prepare(owner, schemas);
		if (state !== owner || signal?.aborted || !actions.length) return;
		const id = `prepare:${turn}`;
		const next = ++revision;
		return {
			id,
			source: "execution_preparation",
			revision: reserve?.(id, next) ?? next,
			actions,
		};
	};
	const source: AgentPlanSource = {
		id: "execution_preparation",
		kind: "preparation",
		requestLifetime: "actor_decision",
		enabled: (settings) => !!configure(settings),
		propose: ({ settings, startInput, data, signal }) => {
			const owner = configure(settings);
			return owner
				? lifetime.admit(async () => {
						if (!owner.changes) await observeWorkspace(owner);
						return proposal(owner, startInput.turnID, data.schemaHashes, signal);
					})
				: undefined;
		},
	};
	const clear = async () => {
		await lifetime.drain();
		const owner = state;
		state = undefined;
		await release(owner);
		operations.clear();
		commands.clear();
	};
	return {
		source,
		connect: (runtime: AgentRuntime) =>
			runtime.subscribe({
				actor_observed: (event) =>
					lifetime.admit(async () => {
						const {
							settings,
							action,
							tool,
							concrete,
							operations: observed,
							consumeInput,
							data,
							signal,
						} = event;
						const owner = configure(settings);
						const bindings =
							observed?.filter(
								(binding) => binding.available !== false && binding.permissionHash === action?.hash,
							) ?? [];
						if (bindings.length) {
							const schemaHash = action!.schemaHash;
							const parent = action!.hash;
							for (const binding of bindings) {
								const key = `${parent}:${operationID(binding)}`;
								operations.set(key, { key, parent, binding });
							}
							commands.set(parent, {
								parent,
								tool,
								input: structuredClone(concrete),
								schemaHash,
								request: { revision: ++revision, kind: "observed" },
							});
						}
						if (action && semantics.effect(action) === "workspace_mutation")
							for (const command of commands.values())
								command.request = { revision: ++revision, kind: "edit" };
						if (owner) {
							await observeWorkspace(owner);
							await runtime.submitPlan(
								event.startInput,
								await proposal(
									owner,
									`${consumeInput.turnID}:after`,
									data.schemaHashes,
									signal,
									(id, minimum) => runtime.reservePlanRevision(event.sessionID, id, minimum),
								),
							);
						}
					}),
				prediction_admitted: ({ feedback: token }) => {
					const prepared = feedback.get(token as object);
					if (prepared && prepared.command.request === prepared.request) prepared.command.request = undefined;
				},
				prediction_feedback: ({ feedback: token, settlement }) => {
					const operation = feedback.get(token as object)?.operation;
					if (
						operation &&
						settlement.observation === "unobserved" &&
						settlement.cause.stage === "execution" &&
						!current(operation.binding) &&
						operations.get(operation.key) === operation
					)
						operations.delete(operation.key);
				},
				session_closed: () => lifetime.run(clear),
			}),
		dispose: () => lifetime.close(clear),
	};
}
type PlanNodeExecution =
	| {
			readonly status: "deferred" | "preparing" | "scheduled";
	  }
	| {
			readonly status: "execution_blocked" | "failed";
			readonly cause: ResolutionCause;
	  }
	| {
			readonly status: "queued" | "running" | "succeeded";
			readonly candidateID: string;
	  }
	| {
			readonly status: "failed" | "cancelled";
			readonly cause: ResolutionCause;
			readonly candidateID: string;
	  };
export type PredictionOpportunityState =
	| {
			readonly status: "pending";
	  }
	| {
			readonly status: "matching";
			readonly actorAction: ActorActionIdentity;
			readonly relation: ActionKeyMatch;
	  }
	| {
			readonly status: "settled";
			readonly settlement: PredictionSettlement;
	  };
export type PlanNodeReadiness = "ready" | "waiting" | "blocked" | "settled";
export interface PlanRuntimeNode {
	readonly identity: PlanActionIdentity;
	readonly proposalID: string;
	readonly source: string;
	readonly revision: number;
	readonly action: PlanAction;
	readonly actionKey?: ActionKey;
	readonly anchorDecisionSeq: number;
	readonly earliestDecisionSeq: number;
	readonly expectedDecisionSeq: number;
	readonly latestDecisionSeq: number;
	readonly criticalPathSteps: number;
	readonly dependenciesReady: boolean;
	readonly execution: PlanNodeExecution;
	readonly readiness: PlanNodeReadiness;
	readonly prediction: PredictionIdentity;
	readonly predictionState: PredictionOpportunityState;
}
export interface ClaimedPrediction {
	readonly node: PlanRuntimeNode;
	readonly opportunity: PredictionOpportunity;
}
export type PlanRuntimePromotion =
	| {
			readonly status: "scheduled";
			readonly node: PlanRuntimeNode;
	  }
	| {
			readonly status: "waiting" | "blocked" | "settled" | "already_dispatched" | "missing";
	  };
export type PlanRuntimeUpdateResult =
	| {
			readonly accepted: true;
			readonly plan: MaterializedPlan;
			readonly upserted: readonly PlanAction[];
			readonly removed: readonly string[];
			readonly retired: readonly PlanRuntimeNode[];
	  }
	| {
			readonly accepted: false;
			readonly reason:
				| "invalid_identity"
				| "invalid_revision"
				| "source_mismatch"
				| "proposal_missing"
				| "stale_revision"
				| "duplicate_action"
				| "invalid_action"
				| "invalid_dependency";
	  };
/** Exactly-once owner of one source prediction's Actor-visible outcome. */
export class PredictionOpportunity {
	readonly identity: PredictionIdentity;
	private stateValue: PredictionOpportunityState = { status: "pending" };
	constructor(identity: PredictionIdentity) {
		this.identity = Object.freeze({ ...identity });
	}
	get state(): PredictionOpportunityState {
		return this.stateValue;
	}
	get settlement(): PredictionSettlement | undefined {
		return this.stateValue.status === "settled" ? this.stateValue.settlement : undefined;
	}
	claim(actorAction: ActorActionIdentity, relation: ActionKeyMatch): boolean {
		if (this.stateValue.status !== "pending") return false;
		this.stateValue = Object.freeze({
			status: "matching",
			actorAction: Object.freeze({ ...actorAction }),
			relation: Object.freeze({ ...relation }),
		});
		return true;
	}
	confirm(actorAction: ActorActionIdentity, adoption: PredictionAdoption): PredictionSettlement | undefined {
		if (this.stateValue.status !== "matching" || !sameActorAction(this.stateValue.actorAction, actorAction))
			return undefined;
		return this.finish({
			prediction: this.identity,
			observation: "observed",
			actorAction: this.stateValue.actorAction,
			match: Object.freeze({
				matched: true,
				relation: this.stateValue.relation,
				adoption: freezeAdoption(adoption),
			}),
		});
	}
	miss(actorAction: ActorActionIdentity): PredictionSettlement | undefined {
		if (this.stateValue.status !== "pending") return undefined;
		return this.finish({
			prediction: this.identity,
			observation: "observed",
			actorAction: Object.freeze({ ...actorAction }),
			match: Object.freeze({ matched: false }),
		});
	}
	unobserve(cause: ResolutionCause): PredictionSettlement | undefined {
		if (this.stateValue.status !== "pending") return undefined;
		return this.finish({
			prediction: this.identity,
			observation: "unobserved",
			cause: Object.freeze({ ...cause }),
		});
	}
	private finish(settlement: PredictionSettlement): PredictionSettlement {
		const value = Object.freeze(settlement);
		this.stateValue = Object.freeze({ status: "settled", settlement: value });
		return value;
	}
}
type MutablePlan = {
	id: string;
	source: string;
	revision: number;
	nextRevision: number;
	nodes: Map<string, MutableNode>;
	graph: PlanGraph;
};
type PlanGraph = {
	readonly plans: Set<MutablePlan>;
	ordered: readonly MutableNode[];
};
type PlanExecutionOwner = Pick<CandidateExecution<unknown>, "execution" | "completion">;
type MutableNodeExecution =
	| PlanNodeExecution
	| {
			readonly status: "attached";
			readonly candidateID: string;
			readonly owner: PlanExecutionOwner;
	  };
type PlanNodeFields = Pick<
	PlanRuntimeNode,
	| "identity"
	| "action"
	| "actionKey"
	| "anchorDecisionSeq"
	| "earliestDecisionSeq"
	| "expectedDecisionSeq"
	| "latestDecisionSeq"
	| "criticalPathSteps"
>;
type MutableNode = {
	-readonly [Key in keyof PlanNodeFields]: PlanNodeFields[Key];
} & {
	validDependencies: boolean;
	execution: MutableNodeExecution;
	opportunity: PredictionOpportunity;
	observation?: string;
};
const capturedUpdates = new WeakSet<PlanUpdate>();
/** Owns plan materialization and prediction opportunities; execution is attached, never embedded. */
export class PlanRuntime {
	private readonly plans = new Map<string, MutablePlan>();
	/** Capture once at handoff, before asynchronous binding can invoke producer callbacks. */
	static capture(
		update: PlanUpdate,
		multiStep = true,
	):
		| {
				readonly update: PlanUpdate;
		  }
		| Extract<
				PlanRuntimeUpdateResult,
				{
					accepted: false;
				}
		  > {
		const captured = capturedUpdates.has(update);
		if (captured && multiStep) return { update };
		update = { ...update };
		const id = "actions" in update ? update.id : update.proposalID;
		if (!(validToken(id) && validToken(update.source)))
			return { accepted: false, reason: "invalid_identity" };
		if (!isSafeCount(update.revision)) return { accepted: false, reason: "invalid_revision" };
		const remove = "actions" in update ? [] : [...(update.remove ?? [])];
		const offered =
			"actions" in update
				? multiStep
					? update.actions
					: update.actions.filter(
							(action) => finiteValue(action.horizon) === 0 && (action.dependsOn?.length ?? 0) === 0,
						)
				: multiStep
					? (update.upsert ?? [])
					: [];
		const validated = captured
			? { ok: true as const, actions: Object.freeze(offered) }
			: validateActions(offered);
		if (!validated.ok) return { accepted: false, reason: validated.reason };
		if (remove.some((id) => !validToken(id))) return { accepted: false, reason: "invalid_action" };
		update = Object.freeze(
			"actions" in update
				? {
						...update,
						actions: validated.actions,
						retainDescendants: multiStep ? update.retainDescendants : undefined,
					}
				: {
						...update,
						upsert: validated.actions,
						remove: Object.freeze(remove),
					},
		);
		capturedUpdates.add(update);
		return { update };
	}
	apply(update: PlanUpdate, anchorDecisionSeq: number): PlanRuntimeUpdateResult {
		const captured = PlanRuntime.capture(update);
		if (!("update" in captured)) return captured;
		const owned = captured.update;
		const proposal = "actions" in owned;
		const id = proposal ? owned.id : owned.proposalID;
		const current = this.plans.get(id);
		if (!proposal && !current) return { accepted: false, reason: "proposal_missing" };
		if (current && current.source !== owned.source) return { accepted: false, reason: "source_mismatch" };
		if (current && owned.revision <= current.revision) return { accepted: false, reason: "stale_revision" };
		const upserted = proposal ? owned.actions : (owned.upsert ?? []);
		const actions = new Map(
			proposal ? [] : [...current!.nodes].map(([id, node]) => [id, node.action] as const),
		);
		if (!proposal) for (const id of owned.remove ?? []) actions.delete(id);
		for (const action of upserted) actions.set(action.id, action);
		if (proposal && owned.retainDescendants === true && current) {
			const retained = new Set<string>();
			for (const node of current.graph.ordered) {
				if (node.identity.proposalID !== id) continue;
				const { action } = node;
				const dependencies = action.dependsOn ?? [];
				const replacement = actions.get(action.id);
				if (
					dependencies.some((dependency) =>
						dependency.proposalID ? !this.parent(id, dependency) : !retained.has(dependency.actionID),
					) ||
					(replacement
						? !samePlanActionExecution(action, replacement)
						: !dependencies.some((dependency) => !dependency.proposalID))
				)
					continue;
				actions.set(action.id, replacement ?? action);
				retained.add(action.id);
			}
		}
		const ordered = dependencyOrder(actions.values(), (action) =>
			(action.dependsOn ?? []).flatMap((dependency) =>
				dependency.proposalID
					? dependency.proposalID !== id && this.parent(id, dependency)
						? []
						: [undefined]
					: [actions.get(dependency.actionID)],
			),
		);
		if (!ordered) return { accepted: false, reason: "invalid_dependency" };
		return this.commit({
			id,
			source: owned.source,
			revision: owned.revision,
			actions,
			upserted,
			ordered,
			anchorDecisionSeq,
		});
	}
	plan(proposalID: string): MaterializedPlan | undefined {
		const plan = this.plans.get(proposalID);
		return plan ? planSnapshot(plan) : undefined;
	}
	reserveRevision(proposalID: string, minimum = 0): number | undefined {
		const plan = this.plans.get(proposalID);
		if (!plan) return undefined;
		const revision = Math.max(plan.nextRevision, plan.revision + 1, minimum);
		plan.nextRevision = revision + 1;
		return revision;
	}
	launchable(): readonly PlanRuntimeNode[] {
		return this.select(
			(node) => node.execution.status === "deferred" && node.opportunity.state.status !== "settled",
			"ready",
		);
	}
	promote(proposalID: string, actionID: string): PlanRuntimePromotion {
		const node = this.mutable(proposalID, actionID);
		if (!node) return { status: "missing" };
		const snapshot = this.snapshot(node);
		if (snapshot.readiness === "settled") return { status: "settled" };
		if (snapshot.readiness === "blocked") return { status: "blocked" };
		if (node.execution.status !== "deferred") return { status: "already_dispatched" };
		if (snapshot.readiness === "waiting") return { status: "waiting" };
		node.execution = { status: "scheduled" };
		return { status: "scheduled", node: this.snapshot(node) };
	}
	defer(proposalID: string, actionID: string): boolean {
		const node = this.mutable(proposalID, actionID);
		if (node?.execution.status !== "scheduled") return false;
		node.execution = { status: "deferred" };
		return true;
	}
	rearmExecution(candidateID: string): boolean {
		let rearmed = false;
		for (const plan of this.plans.values())
			for (const node of plan.nodes.values()) {
				if (
					!("candidateID" in node.execution) ||
					node.execution.candidateID !== candidateID ||
					node.opportunity.state.status !== "pending" ||
					!executionSettled(executionProjection(node.execution))
				) {
					continue;
				}
				node.execution = { status: "deferred" };
				rearmed = true;
			}
		return rearmed;
	}
	bindActionKey(identity: PlanActionIdentity, actionKey: ActionKey): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID);
		if (
			node?.identity.id !== identity.id ||
			node.actionKey ||
			node.execution.status !== "deferred" ||
			node.opportunity.state.status === "settled"
		)
			return false;
		node.actionKey = actionKey;
		node.execution = { status: "preparing" };
		return true;
	}
	finishPreparation(identity: PlanActionIdentity, blocked?: ResolutionCause): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID);
		if (
			node?.identity.id !== identity.id ||
			node.execution.status !== "preparing" ||
			node.opportunity.state.status === "settled"
		)
			return false;
		node.execution = blocked
			? { status: "execution_blocked", cause: Object.freeze({ ...blocked }) }
			: { status: "deferred" };
		return true;
	}
	attachExecution(
		proposalID: string,
		actionID: string,
		candidateID: string,
		owner: PlanExecutionOwner,
	): boolean {
		const node = this.mutable(proposalID, actionID);
		if (!node || (node.execution.status !== "deferred" && node.execution.status !== "scheduled"))
			return false;
		const attachment = { status: "attached" as const, candidateID, owner };
		node.execution = attachment;
		void owner.completion.then((state) => {
			if (node.execution === attachment) node.execution = projectExecution(state, candidateID);
		});
		return true;
	}
	/** Preparation and dependency rejection own no physical execution or candidate ID. */
	rejectExecution(identity: PlanActionIdentity, failure: ResolutionCause): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID);
		if (
			node?.identity.id !== identity.id ||
			!["deferred", "preparing", "scheduled"].includes(node.execution.status)
		)
			return false;
		node.execution = Object.freeze({
			status: "failed",
			cause: Object.freeze({ ...failure }),
		});
		return true;
	}
	claimMatch(
		node: PlanRuntimeNode,
		actorAction: ActorActionIdentity,
		relation: ActionKeyMatch,
	): ClaimedPrediction | undefined {
		const current = this.mutable(node.proposalID, node.action.id);
		if (
			current?.identity.id !== node.identity.id ||
			!this.isMatchable(current, actorDecisionSequence(actorAction))
		)
			return undefined;
		const opportunity = current.opportunity;
		return opportunity.claim(actorAction, relation) ? { node, opportunity } : undefined;
	}
	confirm(
		opportunity: PredictionOpportunity,
		actorAction: ActorActionIdentity,
		adoption: PredictionAdoption,
	): PredictionSettlement | undefined {
		const finalized = opportunity.confirm(actorAction, adoption);
		if (!finalized) return undefined;
		const plan = this.plans.get(opportunity.identity.proposalID);
		if (plan?.nodes.get(opportunity.identity.actionID)?.opportunity === opportunity)
			this.recompute(plan.graph);
		return finalized;
	}
	miss(
		proposalID: string,
		actionID: string,
		actorAction: ActorActionIdentity,
	): PredictionSettlement | undefined {
		return this.mutable(proposalID, actionID)?.opportunity.miss(actorAction);
	}
	get(proposalID: string, actionID: string): PlanRuntimeNode | undefined {
		const node = this.mutable(proposalID, actionID);
		return node ? this.snapshot(node) : undefined;
	}
	observe(identity: PlanActionIdentity, observation: string): void {
		const node = this.mutable(identity.proposalID, identity.actionID);
		if (node?.identity.id === identity.id) node.observation = observation;
	}
	dependency(proposalID: string, dependency: PlanActionDependency): PlanRuntimeNode | undefined {
		const node = this.parent(proposalID, dependency);
		return node ? this.get(node.identity.proposalID, node.action.id) : undefined;
	}
	opportunity(proposalID: string, actionID: string): PredictionOpportunity | undefined {
		return this.mutable(proposalID, actionID)?.opportunity;
	}
	pending(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status === "pending");
	}
	matchable(decisionSequence: number): readonly PlanRuntimeNode[] {
		const sequence = Math.max(0, Math.floor(decisionSequence));
		return this.select((node) => this.isMatchable(node, sequence));
	}
	unsettled(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status !== "settled");
	}
	consumers(candidateID: string): readonly PlanRuntimeNode[] {
		return this.select(
			(node) =>
				node.opportunity.state.status !== "settled" &&
				"candidateID" in node.execution &&
				node.execution.candidateID === candidateID,
		);
	}
	due(settledDecisionSeq: number): readonly PlanRuntimeNode[] {
		return this.select(
			(node) => node.opportunity.state.status === "pending" && node.latestDecisionSeq <= settledDecisionSeq,
		);
	}
	drainBlocked(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status !== "settled", "blocked");
	}
	clear(): void {
		this.plans.clear();
	}
	private commit(input: {
		readonly id: string;
		readonly source: string;
		readonly revision: number;
		readonly actions: ReadonlyMap<string, PlanAction>;
		readonly upserted: readonly PlanAction[];
		readonly ordered: readonly PlanAction[];
		readonly anchorDecisionSeq: number;
	}): PlanRuntimeUpdateResult {
		const current = this.plans.get(input.id);
		const touched = new Set(input.upserted.map((action) => action.id));
		const replaced = new Set<string>();
		// A changed ancestor changes the child's execution context even when its own input is unchanged.
		for (const action of input.ordered) {
			const previous = current?.nodes.get(action.id)?.action;
			if (
				previous &&
				((touched.has(action.id) && !samePlanActionExecution(previous, action)) ||
					action.dependsOn?.some((dependency) => !dependency.proposalID && replaced.has(dependency.actionID)))
			)
				replaced.add(action.id);
		}
		const removed = current ? [...current.nodes.keys()].filter((id) => !input.actions.has(id)) : [];
		const retiredIDs = new Set([...removed, ...replaced]);
		const retired: PlanRuntimeNode[] = [];
		if (current) {
			for (const id of retiredIDs) {
				const node = current.nodes.get(id);
				if (node) retired.push(this.snapshot(node));
			}
		}
		const anchor = finiteLimit(input.anchorDecisionSeq);
		const nodes = new Map<string, MutableNode>();
		for (const [id, action] of input.actions) {
			const previous = current?.nodes.get(id);
			if (previous && !replaced.has(id)) {
				previous.action = action;
				if (
					touched.has(id) &&
					(previous.execution.status === "deferred" || previous.execution.status === "preparing")
				)
					previous.anchorDecisionSeq = anchor;
				nodes.set(id, previous);
				continue;
			}
			nodes.set(id, newNode(input.id, input.source, input.revision, action, anchor));
		}
		const connected = new Set(current ? [current.graph] : []);
		for (const action of input.actions.values())
			for (const dependency of action.dependsOn ?? []) {
				if (dependency.proposalID) connected.add(this.plans.get(dependency.proposalID)!.graph);
			}
		const graph: PlanGraph = { plans: new Set(), ordered: [] };
		for (const previous of connected)
			for (const plan of previous.plans) if (plan !== current) graph.plans.add(plan);
		const next: MutablePlan = {
			id: input.id,
			source: input.source,
			revision: input.revision,
			nextRevision: Math.max(current?.nextRevision ?? 0, input.revision + 1),
			nodes,
			graph,
		};
		this.plans.set(next.id, next);
		graph.plans.add(next);
		for (const plan of graph.plans) plan.graph = graph;
		// Only plans sharing dependency ancestry need a common order and settlement pass.
		graph.ordered = dependencyOrder(
			[...graph.plans].flatMap((plan) => [...plan.nodes.values()]),
			(node) =>
				(node.action.dependsOn ?? []).flatMap(
					(dependency) => this.parent(node.identity.proposalID, dependency) ?? [],
				),
		)!;
		this.recompute(graph);
		return {
			accepted: true,
			plan: planSnapshot(next),
			upserted: Object.freeze([
				...input.upserted,
				...input.ordered.filter((action) => replaced.has(action.id) && !touched.has(action.id)),
			]),
			removed,
			retired: Object.freeze(retired),
		};
	}
	private mutable(proposalID: string, actionID: string): MutableNode | undefined {
		return this.plans.get(proposalID)?.nodes.get(actionID);
	}
	private parent(proposalID: string, dependency: PlanActionDependency): MutableNode | undefined {
		const node = this.mutable(dependency.proposalID ?? proposalID, dependency.actionID);
		return !dependency.proposalID || node?.identity.id === dependency.identity ? node : undefined;
	}
	private select(
		include: (node: MutableNode) => boolean = () => true,
		readiness?: PlanNodeReadiness,
	): readonly PlanRuntimeNode[] {
		const selected: PlanRuntimeNode[] = [];
		for (const plan of this.plans.values())
			for (const node of plan.nodes.values()) {
				if (!include(node)) continue;
				const current = this.snapshot(node);
				if (!readiness || current.readiness === readiness) selected.push(current);
			}
		return selected;
	}
	private snapshot(node: MutableNode): PlanRuntimeNode {
		const plan = this.plans.get(node.identity.proposalID)!;
		const dependencies = this.dependencyReadiness(node);
		const {
			opportunity,
			execution,
			observation: _observation,
			validDependencies: _validDependencies,
			...fields
		} = node;
		return Object.freeze({
			...fields,
			proposalID: plan.id,
			source: plan.source,
			revision: plan.revision,
			dependenciesReady: dependencies === "ready",
			execution: executionProjection(execution),
			readiness:
				opportunity.state.status === "settled"
					? "settled"
					: dependencies === "ready" && execution.status !== "deferred"
						? "waiting"
						: dependencies,
			prediction: opportunity.identity,
			predictionState: opportunity.state,
		});
	}
	private dependencyReadiness(node: MutableNode, matching = false): "ready" | "waiting" | "blocked" {
		const pending = [node];
		const visited = new Set<MutableNode>();
		let readiness: "ready" | "waiting" = "ready";
		while (pending.length) {
			const current = pending.pop()!;
			if (visited.has(current)) continue;
			visited.add(current);
			if (!current.validDependencies) return "blocked";
			for (const dependency of current.action.dependsOn ?? []) {
				if (dependency.condition === "resource") continue;
				const parent = this.parent(current.identity.proposalID, dependency);
				const state = parent
					? dependencyReadiness(parent, dependency, matching && current === node)
					: "blocked";
				if (state === "blocked") return state;
				if (state === "waiting" && current === node) readiness = state;
				pending.push(parent!);
			}
		}
		return readiness;
	}
	private isMatchable(node: MutableNode, decisionSequence: number): boolean {
		return (
			node.opportunity.state.status === "pending" &&
			node.earliestDecisionSeq <= decisionSequence &&
			this.dependencyReadiness(node, true) === "ready"
		);
	}
	private recompute(graph: PlanGraph): void {
		// The accepted graph is immutable between revisions; reuse its validated topological order.
		for (const node of graph.ordered) {
			const settlement = node.opportunity.settlement;
			const matched = predictionMatched(settlement)
				? actorDecisionSequence(settlement.actorAction)
				: undefined;
			node.earliestDecisionSeq = matched ?? node.anchorDecisionSeq + 1;
			node.expectedDecisionSeq = matched ?? node.anchorDecisionSeq + horizon(node.action) + 1;
			node.latestDecisionSeq = matched ?? node.anchorDecisionSeq + latestHorizon(node.action) + 1;
			node.criticalPathSteps = 0;
			node.validDependencies = true;
			for (const dependency of node.action.dependsOn ?? []) {
				const parent = this.parent(node.identity.proposalID, dependency);
				if (!parent || !parent.validDependencies) node.validDependencies = false;
				if (!parent) continue;
				const batchAdvance = parent.action.type === "operation" ? 0 : 1;
				node.earliestDecisionSeq = Math.max(
					node.earliestDecisionSeq,
					parent.earliestDecisionSeq + batchAdvance,
				);
				node.expectedDecisionSeq = Math.max(
					node.expectedDecisionSeq,
					parent.expectedDecisionSeq + batchAdvance,
				);
				node.latestDecisionSeq = Math.max(node.latestDecisionSeq, parent.latestDecisionSeq + batchAdvance);
			}
		}
		for (let index = graph.ordered.length - 1; index >= 0; index--) {
			const node = graph.ordered[index]!;
			node.criticalPathSteps += 1;
			for (const dependency of node.action.dependsOn ?? []) {
				const parent = this.parent(node.identity.proposalID, dependency);
				if (parent) parent.criticalPathSteps = Math.max(parent.criticalPathSteps, node.criticalPathSteps);
			}
		}
	}
}
function newNode(
	proposalID: string,
	source: string,
	revision: number,
	action: PlanAction,
	anchorDecisionSeq: number,
): MutableNode {
	const identity: PlanActionIdentity = Object.freeze({
		...(action.type === "operation" ? { kind: "operation" as const } : {}),
		id: JSON.stringify([source, proposalID, action.id, revision]),
		source,
		proposalID,
		actionID: action.id,
	});
	return {
		identity,
		action,
		anchorDecisionSeq,
		earliestDecisionSeq: anchorDecisionSeq + 1,
		expectedDecisionSeq: anchorDecisionSeq + horizon(action) + 1,
		latestDecisionSeq: anchorDecisionSeq + latestHorizon(action) + 1,
		criticalPathSteps: 1,
		validDependencies: true,
		execution: { status: "deferred" },
		opportunity: new PredictionOpportunity(identity),
	};
}
function dependencyReadiness(
	node: MutableNode,
	dependency: PlanActionDependency,
	matching: boolean,
): "ready" | "waiting" | "blocked" {
	const { condition, observation } = dependency;
	if (condition === "prediction") {
		const settlement = node.opportunity.settlement;
		if (
			(settlement && !predictionMatched(settlement)) ||
			(node.observation !== undefined && node.observation !== observation)
		)
			return "blocked";
		return matching && (!settlement || node.observation === undefined) ? "waiting" : "ready";
	}
	if (condition === "actor_adopted") {
		const settlement = node.opportunity.settlement;
		return settlement === undefined
			? "waiting"
			: predictionLearning(settlement).adopted
				? "ready"
				: "blocked";
	}
	const execution = executionProjection(node.execution);
	if (!executionSettled(execution)) return "waiting";
	return condition === "execution_succeeded" && execution.status !== "succeeded" ? "blocked" : "ready";
}
function predictionMatched(settlement: PredictionSettlement | undefined): settlement is Extract<
	PredictionSettlement,
	{
		readonly observation: "observed";
	}
> & {
	readonly match: {
		readonly matched: true;
	};
} {
	return settlement?.observation === "observed" && settlement.match.matched;
}
function freezeAdoption(adoption: PredictionAdoption): PredictionAdoption {
	return adoption.status === "adopted"
		? Object.freeze({ ...adoption })
		: Object.freeze({
				...adoption,
				cause: Object.freeze({ ...adoption.cause }),
			});
}
function sameActorAction(left: ActorActionIdentity, right: ActorActionIdentity): boolean {
	return (
		left.kind === right.kind &&
		left.id === right.id &&
		left.sequence === right.sequence &&
		left.turnID === right.turnID
	);
}
function executionSettled(execution: PlanNodeExecution): boolean {
	return (
		execution.status === "succeeded" || execution.status === "failed" || execution.status === "cancelled"
	);
}
function executionProjection(execution: MutableNodeExecution): PlanNodeExecution {
	return execution.status === "attached"
		? projectExecution(execution.owner.execution, execution.candidateID)
		: Object.freeze(execution);
}
function projectExecution(state: PlanExecutionOwner["execution"], candidateID: string): PlanNodeExecution {
	return Object.freeze(
		state.status === "failed" || state.status === "cancelled"
			? { status: state.status, cause: state.cause, candidateID }
			: { status: state.status, candidateID },
	);
}
function planSnapshot(plan: MutablePlan): MaterializedPlan {
	return Object.freeze({
		id: plan.id,
		source: plan.source,
		revision: plan.revision,
		actions: Object.freeze([...plan.nodes.values()].map((node) => node.action)),
	});
}
function validateActions(actions: readonly PlanAction[]):
	| {
			readonly ok: true;
			readonly actions: readonly PlanAction[];
	  }
	| {
			readonly ok: false;
			readonly reason: "duplicate_action" | "invalid_action" | "invalid_dependency";
	  } {
	// Capture scheduling records before cloning input graphs, which may invoke producer accessors.
	const result = actions.map((source) => ({ ...source }));
	const ids = new Set<string>();
	for (const source of result) {
		if (
			!validToken(source.id) ||
			!validToken(source.tool) ||
			(source.type !== "tool_call" && source.type !== "operation") ||
			(source.type === "operation"
				? !source.operation || !Object.isFrozen(source.operation)
				: source.operation !== undefined)
		) {
			return { ok: false, reason: "invalid_action" };
		}
		if (ids.has(source.id)) return { ok: false, reason: "duplicate_action" };
		ids.add(source.id);
		if (source.dependsOn) {
			source.dependsOn = Object.freeze(
				source.dependsOn.map(
					({ actionID, proposalID, identity, condition = "execution_settled", observation }) =>
						Object.freeze({
							actionID,
							...(proposalID !== undefined || identity !== undefined ? { proposalID, identity } : {}),
							condition,
							...(condition === "prediction" ? { observation } : {}),
						}),
				),
			);
			if (
				source.dependsOn.some(
					(dependency) =>
						!validToken(dependency.actionID) ||
						!["execution_settled", "execution_succeeded", "actor_adopted", "prediction", "resource"].includes(
							dependency.condition!,
						) ||
						(dependency.condition === "prediction" && !validToken(dependency.observation!)) ||
						("proposalID" in dependency &&
							(!validToken(dependency.proposalID!) || !validToken(dependency.identity!))),
				)
			) {
				return { ok: false, reason: "invalid_dependency" };
			}
		}
	}
	try {
		for (const source of result) {
			if (source.resourceDemand && typeof source.resourceDemand === "object") {
				const entries = Object.entries(source.resourceDemand);
				if (
					entries.some(
						([key, value]) =>
							!RESOURCE_DIMENSIONS.includes(key as (typeof RESOURCE_DIMENSIONS)[number]) ||
							!isFiniteNumber(value) ||
							value < 0,
					)
				)
					return { ok: false, reason: "invalid_action" };
				source.resourceDemand = Object.freeze(Object.fromEntries(entries));
			}
			source.input = immutableSnapshot(source.input);
			if (!isImmutableSnapshot(source.input)) return { ok: false, reason: "invalid_action" };
			Object.freeze(source);
		}
	} catch {
		return { ok: false, reason: "invalid_action" };
	}
	return { ok: true, actions: Object.freeze(result) };
}
function samePlanActionExecution(left: PlanAction, right: PlanAction): boolean {
	if (
		left.type !== right.type ||
		left.operation !== right.operation ||
		left.tool !== right.tool ||
		!isDeepStrictEqual(left.input, right.input)
	)
		return false;
	// Owned dependency records have canonical fields; opaque IDs must not use locale ordering.
	const counts = new Map<string, number>();
	for (const [dependencies, delta] of [
		[left.dependsOn, 1],
		[right.dependsOn, -1],
	] as const) {
		for (const dependency of dependencies ?? []) {
			const key = JSON.stringify(dependency);
			const count = (counts.get(key) ?? 0) + delta;
			if (count) counts.set(key, count);
			else counts.delete(key);
		}
	}
	return counts.size === 0;
}
function horizon(action: PlanAction): number {
	return finiteLimit(action.horizon ?? 0);
}
function latestHorizon(action: PlanAction): number {
	return Math.max(horizon(action), finiteLimit(action.latestHorizon ?? action.horizon ?? 0));
}
function actorDecisionSequence(action: ActorActionIdentity): number {
	return finiteLimit(action.decisionSequence ?? action.sequence);
}
function validToken(value: string): boolean {
	return typeof value === "string" && value.length > 0 && value.trim() === value;
}
class CandidateFailure extends Error {
	readonly failure: ResolutionCause;
	constructor(failure: ResolutionCause) {
		super(failure.detail ?? failure.code);
		this.failure = failure;
	}
}
function asUpdates(value: PlanUpdate | readonly PlanUpdate[] | undefined): readonly PlanUpdate[] {
	return value === undefined ? [] : Array.isArray(value) ? value : [value as PlanUpdate];
}
function reservationAvailable(reservation: CandidateReservation): boolean {
	return reservation.kind === "shared" ? reservation.owners.length === 0 : reservation.status === "available";
}
function concurrentLimit(settings: SpeculativeActionSettings): number {
	return positiveCount(settings.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions);
}
function cacheByteLimit(settings: SpeculativeActionSettings): number {
	return positiveCount(finiteNumber(settings.resourceCacheMaxBytes) ?? DEFAULTS.resourceCacheMaxBytes);
}
function cacheLimits(settings: SpeculativeActionSettings) {
	return {
		maxEntries: settings.reuseEnabled === false ? 0 : positiveCount(settings.resourceCacheMaxEntries),
		maxBytes: cacheByteLimit(settings),
		hotFraction: 0.8,
	};
}
function definedFields<T, K extends keyof T>(value: T, keys: readonly K[]): Partial<Pick<T, K>> {
	const result: Partial<Pick<T, K>> = {};
	for (const key of keys) if (value[key] !== undefined) result[key] = value[key];
	return result;
}
function forecastFor(node: PlanRuntimeNode, decisionSequence: number): PredictionForecast {
	return {
		tool: node.action.tool,
		// Internal producers keep their backend identity separate from their permission anchor.
		...(node.actionKey && !node.action.producesOperations ? actionExecutionIdentity(node.actionKey) : {}),
		...definedFields(node.action, [
			"expectedDurationMs",
			"resourceDemand",
			"adoptionProbability",
			"confidence",
		]),
		decisionBatchesUntilCall: Math.max(0, node.expectedDecisionSeq - decisionSequence),
		criticalPathSteps: node.criticalPathSteps,
		...(node.action.empiricalProbability !== undefined
			? { hitProbability: node.action.empiricalProbability }
			: {}),
		...(node.action.background ? { background: true } : {}),
		actorDemand: node.predictionState.status === "matching",
		...(node.dependenciesReady && (node.action.dependsOn?.length ?? 0) > 0 && (node.action.horizon ?? 0) <= 0
			? { dependenciesResolved: true }
			: {}),
	};
}
function planActionDraft(node: PlanRuntimeNode): SpeculativeDraftCandidate {
	const { id, background: _background, ...draft } = node.action;
	return {
		...draft,
		source: node.source,
		proposalID: node.proposalID,
		actionID: id,
	};
}
function asConcreteInput(value: unknown): Record<string, unknown> | undefined {
	return value === undefined || value === null ? {} : asRecord(value);
}
function publicCandidate<Output>(candidate: CandidateRecord<Output>): SpeculativeCandidate {
	const execution = candidate.work.execution;
	return {
		id: candidate.id,
		key: candidate.key,
		tool: candidate.owner.draft.tool,
		input: candidate.key?.input ?? asConcreteInput(candidate.owner.draft.input) ?? {},
		...("executionMs" in execution ? { executionMs: execution.executionMs } : {}),
		...(candidate.owner.draft.source ? { source: candidate.owner.draft.source } : {}),
	};
}
function predictionCandidate<Output>(
	candidate: CandidateRecord<Output>,
	node: PlanRuntimeNode,
): SpeculativeCandidate {
	return {
		...publicCandidate(candidate),
		source: node.source,
		empiricalProbability: node.action.empiricalProbability,
		adoptionProbability: node.action.adoptionProbability,
		conditionalProbability: node.action.conditionalProbability,
		depth: node.action.depth,
		planDependencies: node.action.dependsOn,
	};
}
function activeExecution<Output>(candidate: CandidateRecord<Output>): boolean {
	return candidate.work.execution.status !== "failed" && candidate.work.execution.status !== "cancelled";
}
function candidateBranch<Output>(candidate: CandidateRecord<Output>): WorldBranch<Output> | undefined {
	const execution = candidate.work.execution;
	return execution.status === "succeeded" ? execution.output : undefined;
}
function captureCoverage<Output>(
	action: ActionKey,
	output: Output,
	rules: readonly ActionProjectionRule<Output>[],
): readonly ActionProjectionCoverage[] {
	return rules.flatMap((rule) => {
		try {
			const value = rule.captureCoverage?.(action, output);
			return value === undefined ? [] : [{ rule: rule.id, value: cloneSharedData(value) }];
		} catch {
			return [];
		}
	});
}
async function projectOutput<Output>(
	candidate: CandidateRecord<Output>,
	actor: ActionKey,
	match: ActionKeyMatch,
	rules: readonly ActionProjectionRule<Output>[],
	request: Parameters<NonNullable<WorldBranch<Output>["reconstruct"]>>[0],
	mode: "cached" | "output" | "inputs" = "cached",
): Promise<ProjectionResult<Output>> {
	const branch = candidateBranch(candidate)!;
	if (branch.inputsOnly && match.kind !== "inputs")
		return { ok: false, cause: cause("projection", "input_only_branch") };
	const retained = mode === "cached" && candidate.resultViews?.get(actor.key);
	// A view kept without its proof is served only by a branch that can still commit and validate as a whole.
	if (retained && (retained.validate || !branch.inputsOnly)) {
		const output = cloneSharedData(retained.output);
		if (retained.resource) retained.resource.references++;
		return { ok: true, ...retained, output, reused: true };
	}
	if (!branch.inputsOnly && match.kind === "exact" && mode !== "inputs")
		return { ok: true, output: branch.output };
	const reconstruct = branch.reconstruct;
	const rule =
		match.kind === "projected" && mode !== "inputs"
			? rules.find((item) => item.id === match.projector)
			: undefined;
	const coverage = candidate.projectionCoverage.find((item) => item.rule === rule?.id);
	let rebuilt: Awaited<ReturnType<NonNullable<typeof reconstruct>>>;
	try {
		const { output: projected, computation: execution } = await TimelineInterval.measure(async () => {
			const projected: Output | undefined =
				!branch.inputsOnly && candidate.key && match.kind === "projected" && coverage && rule?.projectOutput
					? cloneSharedData(
							await rule.projectOutput({
								speculative: candidate.key,
								actor,
								output: branch.output,
								coverage: cloneSharedData(coverage.value),
								keyMatch: match,
							}),
						)
					: undefined;
			return projected !== undefined ? projected : (rebuilt = await reconstruct?.(request))?.output;
		});
		if ((rebuilt?.requiresQueryValidation || mode === "inputs") && !rebuilt?.validate)
			return { ok: false, cause: cause("projection", "query_proof_missing") };
		if (projected === undefined) return { ok: false, cause: cause("projection", "view_not_covered") };
		const resource = rebuilt?.dispose ? { dispose: rebuilt.dispose.bind(rebuilt), references: 1 } : undefined;
		const result: ProjectionResult<Output> = {
			ok: true,
			output: projected,
			execution,
			inputs: !!rebuilt,
			compatibility: rebuilt?.compatibility,
			validate: rebuilt?.validate,
			capturedBytes: rebuilt?.capturedBytes,
			requiresQueryValidation: rebuilt?.requiresQueryValidation,
			resource,
		};
		rebuilt = undefined;
		return result;
	} catch (error) {
		return {
			ok: false,
			cause: cause("projection", "reconstruction_failed", errorDetail(error)),
		};
	} finally {
		if (rebuilt?.dispose)
			try {
				await rebuilt.dispose();
			} catch {
				/* A discarded reconstruction cannot change the selected result. */
			}
	}
}
function callKey(turnID: string, callID: string): string {
	return JSON.stringify([turnID, callID]);
}
function outputIsError(value: unknown): boolean {
	return Boolean(
		value &&
		typeof value === "object" &&
		(
			value as {
				readonly isError?: unknown;
			}
		).isError === true,
	);
}
function candidateEventDescriptor<Output>(candidate: CandidateRecord<Output>): CandidateEventDescriptor {
	const branch = candidateBranch(candidate);
	return {
		source: candidate.owner.draft.source ?? "cache",
		...(candidate.owner.draft.mode ? { mode: candidate.owner.draft.mode } : {}),
		depth: candidate.owner.draft.depth ?? 0,
		id: candidate.id,
		origin: candidate.origin,
		...(candidate.owner.draft.type === "operation" ? { kind: "operation" as const } : {}),
		tool: candidate.owner.draft.tool,
		route: candidate.route,
		...(branch
			? {
					world: {
						backend: branch.backend,
						executionMetrics: branch.executionMetrics,
					},
				}
			: {}),
		predictedAction: diagnosticAction(
			candidate.owner.draft.tool,
			candidate.key?.input ?? candidate.owner.draft.input,
			candidate.key,
		),
	};
}
function estimateValueBytes(value: unknown): number {
	try {
		return serialize(value).byteLength;
	} catch {
		return Infinity;
	}
}
function inputIndexBytes(branch: WorldBranch<unknown>): number {
	return estimateValueBytes(branch.inputResources ?? []);
}
/** Relative resources mean their key's root, never the host process working directory. */
function rootedResource(key: ActionKey, resource: string): string {
	return key.resourceRoot !== undefined || path.isAbsolute(resource)
		? path.resolve(key.resourceRoot ?? "", resource)
		: resource;
}
function resourcePathsOverlap(left: string, right: string): boolean {
	return containsLogicalPath(left, right) || containsLogicalPath(right, left);
}
function maybe<T>(value: T | undefined): T[] {
	return value === undefined ? [] : [value];
}
function actionExecutionIdentity(action: ActionKey): ExecutionIdentity {
	return {
		tool: action.tool,
		semanticsEpoch: action.semanticsEpoch,
		executionFingerprint: action.executionFingerprint,
		actionKeyHash: action.hash,
	};
}
interface PlanActionContext<StartInput, StateData> extends RuntimeTurnContext<StartInput, StateData> {
	readonly identity: PlanActionIdentity;
	readonly opportunity: PredictionOpportunity;
	draft: SpeculativeDraftCandidate;
	served?: boolean;
	// Rearming replaces execution, but earlier work can still serve an Actor.
	readonly producers: Set<string>;
	readonly admissionController: AbortController;
	readonly sourceSlot: SourceRequestSlot;
	readonly continuationSlots: Set<SourceRequestSlot>;
	readonly continuationTriggers: Set<"prepared" | "execution_succeeded" | "actor_adopted">;
	peerContinuations?: Set<string>;
	executionRoute?: SpeculativeExecutionRoute;
	launch?: Promise<void>;
}
/** Producer requests and ordered observations share ownership with their admitted actions. */
interface SourceRequestSlot {
	readonly request: SourceRequestIdentity;
	readonly proposalID?: string;
	readonly expiresAtTarget: boolean;
	readonly generation: SourceGeneration;
	readonly owners: Set<string>;
	pending: number;
}
interface PlanAdmissionScope<SessionID, Output, StartInput, StateData> extends RuntimeTurnContext<
	StartInput,
	StateData
> {
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly slot: SourceRequestSlot;
}
interface CandidateRecord<Output, StartInput = unknown, StateData = unknown> {
	readonly id: string;
	readonly origin: "prediction" | "actor_preview" | "actor_result";
	readonly key?: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly work: CandidateExecution<WorldBranch<Output>>;
	forkParent?: CandidateRecord<Output, StartInput, StateData>;
	actorAdopted: boolean;
	readonly owner: RuntimeTurnContext<StartInput, StateData> & {
		readonly draft: SpeculativeDraftCandidate;
		readonly index: number;
	};
	expectedDurationMs?: number;
	estimatedBytes: number;
	projectionCoverage: readonly ActionProjectionCoverage[];
	inputSource?: object;
	resultViews?: Map<string, RetainedResultView<Output>>;
	previews?: Set<ActorPreviewRecord>;
	onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	operationJoinable?: () => boolean;
	operationConsumers?: Set<object>;
	actorConsumers?: Set<object>;
	admissionValidation?: Promise<ResourceValidation>;
}
type ActorPreviewState =
	| {
			readonly status: "pending";
	  }
	| {
			readonly status: "candidate";
			readonly candidateID: string;
			readonly ownership: "existing" | "preview";
	  }
	| {
			readonly status: "cancelled";
	  };
interface ActorPreviewRecord {
	readonly controller: AbortController;
	actionKey?: ActionKey;
	task: Promise<void>;
	state: ActorPreviewState;
	actorDemand?: boolean;
}
interface SessionState<SessionID, Output, StartInput, StateData> {
	readonly id: SessionID;
	readonly lifecycle: RuntimeLifecycleLane;
	readonly plan: PlanRuntime;
	readonly effects: PostSettlementQueue;
	readonly events: BoundedEventQueue<SpeculativeActionEvent<SessionID>>;
	readonly actionContexts: Map<string, PlanActionContext<StartInput, StateData>>;
	readonly sourceSlots: Set<SourceRequestSlot>;
	readonly sourceTasks: PendingTasks;
	readonly turns: Map<string, TurnState<SessionID, Output, StartInput, StateData>>;
	/** Fallback execution can outlive turn retirement; only its physical settlement returns these units. */
	readonly actorRequests: Set<object>;
	settings: SpeculativeActionSettings;
	timeline?: TaskTimeline;
	sequence: number;
	decisionSequence: number;
	tokenTotal: number;
	candidateSequence: number;
	sourceRequestSequence: number;
	/** Retained handoffs borrow the live session, independently of prediction and candidate retirement. */
	readonly acceptOperationScope: (scope: ExecutionScope) => boolean;
}
interface TurnState<SessionID, Output, StartInput, StateData> extends RuntimeTurnContext<
	StartInput,
	StateData
> {
	settings: SpeculativeActionSettings;
	readonly session: SessionState<SessionID, Output, StartInput, StateData>;
	readonly sessionID: SessionID;
	readonly turnID: string;
	readonly startedAt: number;
	readonly definitions: readonly DrafterToolDefinition[];
	readonly generation: SourceGeneration;
	readonly signal?: AbortSignal;
	readonly decisionSequence: number;
	readonly actorActions: Set<ActorAction<CandidateRecord<Output, StartInput, StateData>, Output>>;
	actorObservation?: ActorActionIdentity | null;
	readonly actorToolHints: Set<string>;
	readonly actorPreviews: Map<string, ActorPreviewRecord>;
	actorArrivedAt?: number;
	lifecycle: "active" | "closing" | "finished";
}
interface ProjectionResource {
	readonly dispose: () => void | Promise<void>;
	references: number;
}
type ProjectionResult<Output> =
	| {
			readonly ok: true;
			readonly output: Output;
			readonly execution?: TimelineInterval;
			readonly reused?: true;
			readonly inputs?: boolean;
			readonly compatibility?: WorldBranch<Output>["compatibility"];
			readonly validate?: WorldBranch<Output>["validate"];
			readonly capturedBytes?: number;
			readonly requiresQueryValidation?: true;
			readonly resource?: ProjectionResource;
	  }
	| {
			readonly ok: false;
			readonly cause: ResolutionCause;
	  };
type RetainedResultView<Output> = Omit<
	Extract<
		ProjectionResult<Output>,
		{
			ok: true;
		}
	>,
	"ok" | "execution" | "reused"
> & {
	readonly bytes: number;
	readonly execution: TimelineInterval;
};
/** Structural runtime: plans own predictions, candidates own execution, ActorAction owns adoption. */
export function makeSpeculativeActionRuntime<
	SessionID,
	Output,
	StartInput extends TurnInput<SessionID>,
	ConsumeInput extends TurnInput<SessionID>,
	FinishInput extends TurnInput<SessionID>,
	StateData,
>(
	adapter: SpeculativeActionRuntimeAdapter<SessionID, Output, StartInput, ConsumeInput, StateData>,
): SpeculativeActionRuntime<SessionID, Output, StartInput, ConsumeInput, FinishInput, StateData> {
	type Source = SpeculativePlanSource<SessionID, Output, StartInput, StateData>;
	type Candidate = CandidateRecord<Output, StartInput, StateData>;
	type Session = SessionState<SessionID, Output, StartInput, StateData>;
	type Turn = TurnState<SessionID, Output, StartInput, StateData>;
	type TurnClosure = {
		readonly state: Turn;
		readonly terminal: boolean;
		readonly notifyHost: boolean;
	};
	type SessionClosureMode = "terminal" | "disposed";
	type ActorSelectionInput = {
		readonly state: Turn;
		readonly consumeInput: ConsumeInput;
		readonly actualCall: ActualToolCall;
		readonly actualKey: ActionKey;
		readonly actorAction: ActorAction<Candidate, Output>;
		readonly pending: ReadonlySet<Promise<void>>;
		readonly preview?: ActorPreviewRecord;
		readonly signal?: AbortSignal;
	};
	const semantics = adapter.actionSemantics ?? PI_ACTION_SEMANTICS;
	const subscriptions = new RuntimeSubscriptions<
		RuntimeEventMap<SessionID, Output, StartInput, ConsumeInput, StateData>
	>();
	const sources = adapter.sources ?? [];
	const sourcesByID = new Map<string, Source>();
	for (const source of sources) {
		if (!source.id || source.id.trim() !== source.id)
			throw new Error(`invalid speculative plan source ${source.id}`);
		if (sourcesByID.has(source.id)) throw new Error(`duplicate speculative plan source ${source.id}`);
		sourcesByID.set(source.id, source);
	}
	const projectionRules = resolveActionProjectionRules(adapter.projectionRules ?? [], semantics);
	const candidateStore = new CandidateStore<SessionID, Candidate>(projectionRules, (_candidate, evidence) =>
		resultCacheRecency(evidence),
	);
	const sourceEnabled = (source: Source | undefined, settings: SpeculativeActionSettings): boolean => {
		try {
			return !!source && source.enabled(settings) && settings.enabled;
		} catch {
			return false;
		}
	};
	const releaseProjectionResource = (sessionID: SessionID, resource?: ProjectionResource) => {
		if (resource && --resource.references === 0) sessionStates.get(sessionID)?.lifecycle.release(resource);
	};
	/** One mutation owns a view's storage, budget, lookup membership, and proof reference. */
	function setResultView(
		sessionID: SessionID,
		candidate: Candidate,
		key: string,
		view?: RetainedResultView<Output>,
	): void {
		const previous = candidate.resultViews?.get(key);
		if (previous === view) return;
		if (view?.resource) view.resource.references++;
		if (view) (candidate.resultViews ??= new Map()).set(key, view);
		else candidate.resultViews?.delete(key);
		candidate.estimatedBytes += (view?.bytes ?? 0) - (previous?.bytes ?? 0);
		candidateStore.indexView(sessionID, candidate, key, view !== undefined);
		releaseProjectionResource(sessionID, previous?.resource);
	}
	/** Memoized queries share their sealed candidate's proof, retention budget, and lifetime. */
	function retainResultView(
		sessionID: SessionID,
		candidate: Candidate,
		action: ActionKey,
		projection: Extract<
			ProjectionResult<Output>,
			{
				ok: true;
			}
		>,
		settings: SpeculativeActionSettings,
	): boolean {
		if (settings.reuseEnabled === false || !projection.execution || projection.reused) return false;
		try {
			const owned = cloneSharedData(projection.output);
			const outputBytes = estimateValueBytes(owned) + Buffer.byteLength(action.key);
			setResultView(sessionID, candidate, action.key);
			let bytes = outputBytes + finiteValue(projection.capturedBytes);
			let validate = projection.validate;
			let resource = projection.resource;
			if (
				!projection.requiresQueryValidation &&
				candidate.estimatedBytes + bytes > cacheByteLimit(settings)
			) {
				bytes = outputBytes;
				validate = undefined;
				resource = undefined;
			}
			while (
				candidate.resultViews?.size &&
				(candidate.resultViews.size >= positiveCount(settings.resourceCacheMaxEntries) ||
					candidate.estimatedBytes + bytes > cacheByteLimit(settings))
			)
				setResultView(sessionID, candidate, candidate.resultViews.keys().next().value!);
			if (candidate.estimatedBytes + bytes <= cacheByteLimit(settings)) {
				setResultView(sessionID, candidate, action.key, {
					output: owned,
					bytes,
					execution: projection.execution,
					inputs: projection.inputs,
					compatibility: projection.compatibility,
					validate,
					resource,
					capturedBytes: bytes - outputBytes,
					requiresQueryValidation: projection.requiresQueryValidation,
				});
				return true;
			}
		} catch {
			/* Optional retention cannot alter an already committed result. */
		}
		return false;
	}
	const sessionStates = new Map<SessionID, Session>();
	const continuationTails = new WeakMap<object, Promise<void>>();
	type Preparation = {
		readonly session: Session;
		readonly controller: AbortController;
		readonly forecast: () => PredictionForecast;
		readonly run: () => Promise<void>;
		readonly fail: (failure: ResolutionCause) => void;
		readonly completion: Promise<void>;
		readonly complete: () => void;
		active: boolean;
	};
	const preparations = new Map<object, Preparation>();
	let pendingLaunch: Promise<void> | undefined;
	type ScheduledJob = Candidate | Preparation | ActorAction<Candidate, Output>;
	const needsResourceSamples = (): boolean =>
		[...sessionStates.values()].some(
			(session) =>
				!session.lifecycle.sealed &&
				(session.turns.size > 0 || candidateStore.pending(session.id).length > 0),
		) ||
		preparations.size > 0 ||
		scheduler.snapshot().length > 0;
	const scheduler: SpeculationScheduler<ScheduledJob> = new SpeculationScheduler({
		resources: adapter.resources,
		active: needsResourceSamples,
		pollIntervalMs: () => {
			const active = [...sessionStates.values()].filter(
				(session) =>
					session.turns.size ||
					candidateStore.pending(session.id).length ||
					scheduler.snapshot(session).length,
			);
			return active.length
				? Math.min(
						...active.map(
							(session) => normalizeSchedulingSettings(session.settings.scheduling).resourcePollIntervalMs,
						),
					)
				: SCHEDULING_DEFAULTS.resourcePollIntervalMs;
		},
		changed: () => wakeResourceWaiters(),
	});
	const scopeFor = (session: Session) => ({
		owner: session,
		limit: concurrentLimit(session.settings),
		scheduling: session.settings.scheduling,
	});
	const wakeResourceWaiters = (): void => {
		for (const session of sessionStates.values()) {
			preemptForActor(session);
			startQueuedCandidates(session);
		}
		scheduler.watch();
	};
	const sessionFor = (sessionID: SessionID, settings: SpeculativeActionSettings): Session => {
		const current = sessionStates.get(sessionID);
		if (current) return current;
		const created: Session = {
			id: sessionID,
			lifecycle: new RuntimeLifecycleLane(),
			plan: new PlanRuntime(),
			effects: new PostSettlementQueue(),
			events: new BoundedEventQueue(speculativeConfig().runtime.eventHistory, (event) =>
				adapter.onEvent?.(event),
			),
			actionContexts: new Map(),
			sourceSlots: new Set(),
			sourceTasks: new PendingTasks(),
			turns: new Map(),
			settings,
			sequence: 0,
			decisionSequence: 0,
			tokenTotal: 0,
			candidateSequence: 0,
			sourceRequestSequence: 0,
			acceptOperationScope: (scope) =>
				!created.lifecycle.sealed &&
				scope.sessionID === created.id &&
				created.turns.get(scope.turnID)?.lifecycle === "active",
			actorRequests: new Set(),
		};
		sessionStates.set(sessionID, created);
		return created;
	};
	const turnContext = ({ startInput, data, settings }: RuntimeTurnContext<StartInput, StateData>) => ({
		startInput,
		data,
		settings,
	});
	const actorTurnActive = (state: Turn): boolean =>
		state.lifecycle === "active" && state.session.turns.get(state.turnID) === state;
	const removeCandidate = (sessionID: SessionID, candidate: Candidate): void => {
		candidateStore.delete(sessionID, candidate);
		for (const key of candidate.resultViews?.keys() ?? []) setResultView(sessionID, candidate, key);
		sessionStates.get(sessionID)?.lifecycle.release(candidateBranch(candidate));
	};
	const acquireCandidate = (session: Session, candidate: Candidate, owner: string) =>
		candidateStore.has(session.id, candidate) ? candidate.work.acquire(owner) : undefined;
	const borrowResourceInputs = (session: Session, source: Candidate | undefined, owner: string) => {
		let leases: Map<Candidate, NonNullable<ReturnType<typeof acquireCandidate>>> | undefined = new Map();
		return {
			lookup: function* (path?: string): Iterable<object> {
				if (!leases || session.lifecycle.sealed || session.settings.reuseEnabled === false) return;
				for (const candidate of [
					...candidateStore.lookupInputs(session.id, path === undefined ? undefined : [path]),
					...candidateStore.pending(session.id),
				]) {
					if (!leases) return;
					const inputs = candidateBranch(candidate)?.inputSource ?? candidate.inputSource;
					if (
						candidate === source ||
						candidate.owner.draft.type !== "tool_call" ||
						candidate.route.reuse !== "shared_result" ||
						!inputs
					)
						continue;
					const lease = leases.get(candidate) ?? acquireCandidate(session, candidate, owner);
					if (lease) {
						leases.set(candidate, lease);
						yield inputs;
					}
				}
				if (!leases) return;
				for (const state of session.turns.values())
					if (actorTurnActive(state))
						for (const action of state.actorActions)
							if (leases && action.inputSource) yield action.inputSource;
			},
			dispose: () => {
				const owned = leases;
				leases = undefined;
				for (const lease of owned?.values() ?? []) lease.release();
			},
		};
	};
	const createCandidate = (
		session: Session,
		context: RuntimeTurnContext<StartInput, StateData>,
		draft: SpeculativeDraftCandidate,
		input: Pick<Candidate, "origin" | "key" | "route" | "expectedDurationMs"> &
			Partial<Pick<Candidate, "forkParent" | "estimatedBytes" | "projectionCoverage">>,
	): Candidate => {
		const sequence = ++session.candidateSequence;
		let resultBytes = input.estimatedBytes ?? 0;
		const candidate: Candidate = {
			id: `${input.origin === "prediction" ? "spec" : input.origin === "actor_preview" ? "actor" : input.origin}_${sequence}_${input.key?.hash.slice(0, 12) ?? "inputs"}`,
			work: new CandidateExecution<WorldBranch<Output>>(
				input.origin !== "actor_result" && input.route.reuse === "exclusive_branch" ? "exclusive" : "shared",
				undefined,
				() => candidateStore.notify(session.id, false),
			),
			actorAdopted: input.origin === "actor_result",
			owner: { ...turnContext(context), draft, index: sequence - 1 },
			projectionCoverage: [],
			...input,
			get estimatedBytes() {
				return resultBytes + (candidateBranch(candidate)?.capturedBytes ?? 0);
			},
			set estimatedBytes(bytes) {
				resultBytes = bytes - (candidateBranch(candidate)?.capturedBytes ?? 0);
			},
		};
		return candidate;
	};
	const candidateProducer = (session: Session, candidate: Candidate) => ({
		source: candidate.owner.draft.source ?? candidate.origin,
		mode: candidate.owner.draft.mode,
		id: TimelineInterval.producerID(session, candidate.id),
	});
	/** Every producer joins or registers work before yielding. Validation never grants a second launch. */
	const admitCandidate = async (
		session: Session,
		input: Pick<Candidate, "route" | "forkParent"> & {
			readonly key: ActionKey;
			readonly kind?: SpeculativeDraftCandidate["type"];
		},
		create: () => Candidate,
		active: () => boolean,
		attach: (candidate: Candidate, created: boolean) => void,
	): Promise<void> => {
		let rejected: Set<Candidate> | undefined;
		while (active()) {
			const { entry: candidate, inserted } = candidateStore.getOrCreate(
				session.id,
				input.key,
				create,
				(existing, match) => {
					const execution = existing.work.execution;
					return (
						!!existing.key &&
						existing.owner.draft.type === (input.kind ?? "tool_call") &&
						!candidateBranch(existing)?.inputsOnly &&
						!rejected?.has(existing) &&
						activeExecution(existing) &&
						forkWorld(existing.forkParent) === input.forkParent &&
						(sameSpeculativeExecutionRoute(existing.route, input.route) ||
							(existing.route.reuse === "shared_result" &&
								input.route.reuse === "shared_result" &&
								execution.status === "succeeded" &&
								scheduler.assessCompatibility(execution.output.compatibility, input.key.executionFingerprint)
									.compatible)) &&
						(match.kind === "exact" ||
							((existing.route.reuse === "shared_result" || execution.status !== "succeeded") &&
								actionKeyCovers(existing.key, input.key, projectionRules)))
					);
				},
			);
			if (!inserted && candidate.work.execution.status === "succeeded") {
				// Joining producers share only the in-flight check; Actor adoption always validates afresh.
				const validation = await (candidate.admissionValidation ??= validateWorldBranch(
					candidateBranch(candidate),
					candidate.route.reuse,
				).finally(() => {
					candidate.admissionValidation = undefined;
				}));
				if (validation.status !== "valid" || !candidateStore.has(session.id, candidate)) {
					(rejected ??= new Set()).add(candidate);
					continue;
				}
			}
			if (active()) attach(candidate, inserted);
			else if (inserted) discardCandidate(session, candidate, cause("control", "admission_withdrawn"), false);
			return;
		}
	};
	const attachActorPreview = (
		record: ActorPreviewRecord,
		candidate: Candidate,
		ownership: "existing" | "preview",
	): void => {
		record.state = {
			status: "candidate",
			candidateID: candidate.id,
			ownership,
		};
		(candidate.previews ??= new Set()).add(record);
	};
	const startTurn = async (input: StartInput, signal?: AbortSignal): Promise<void> => {
		const settings = await adapter.settings();
		if (signal?.aborted) return;
		const definitions = adapter.definitions(input);
		if (!definitions.length) return;
		const session = sessionFor(input.sessionID, settings);
		await session.lifecycle.run(async () => {
			if (signal?.aborted) return;
			const previous = session.turns.get(input.turnID);
			if (previous) await closeTurn(previous);
			const data = await adapter.stateData(input);
			if (signal?.aborted || session.lifecycle.sealed) return;
			applySettings(session, settings);
			const generation = new SourceGeneration(signal);
			const startedAt = performance.now();
			session.timeline ??= new TaskTimeline(startedAt);
			const state: Turn = {
				session,
				sessionID: input.sessionID,
				turnID: input.turnID,
				startInput: input,
				startedAt,
				data,
				settings,
				definitions,
				generation,
				signal,
				decisionSequence: session.decisionSequence + 1,
				actorActions: new Set(),
				actorToolHints: new Set(),
				actorPreviews: new Map(),
				lifecycle: "active",
			};
			session.turns.set(input.turnID, state);
			scheduler.watch(0);
			await reconcileStores(state);
			await subscriptions.publish("turn_started", {
				...turnContext(state),
				decisionSequence: state.decisionSequence,
				definitions,
				signal,
			});
			startQueuedCandidates(session);
			setTimeout(() => {
				if (
					state.lifecycle === "active" &&
					state.actorArrivedAt === undefined &&
					!state.generation.signal.aborted
				)
					launchSourceRequests(state);
			}, 0);
		});
	};
	const claimSourceSlot = (
		session: Session,
		source: string,
		turnID: string,
		targetDecisionSequence: number,
		limit: number,
		requestKind: SourceRequestKind,
		expiresAtTarget = true,
		parent?: AbortSignal,
		proposalID?: string,
	): SourceRequestSlot | undefined => {
		const slots = [...session.sourceSlots].filter(
			(slot) =>
				slot.request.source === source &&
				slot.request.targetDecisionSequence === targetDecisionSequence &&
				(slot.request.kind === "observation") === (requestKind === "observation"),
		);
		const observed = requestKind === "observation" && slots.find((slot) => slot.request.turnID === turnID);
		if (observed) {
			observed.pending++;
			return observed;
		}
		// Branches of one proposal share alternative capacity, but retain independent cancellation owners.
		if (
			!slots.some((slot) => proposalID !== undefined && slot.proposalID === proposalID) &&
			new Set(slots.map((slot) => slot.proposalID ?? slot)).size >= limit
		)
			return undefined;
		const slot: SourceRequestSlot = {
			request: {
				source,
				turnID,
				index: session.sourceRequestSequence++,
				kind: requestKind,
				targetDecisionSequence,
			},
			proposalID,
			expiresAtTarget,
			generation: new SourceGeneration(parent),
			owners: new Set(),
			pending: 1,
		};
		session.sourceSlots.add(slot);
		return slot;
	};
	const releaseSourceSlot = (session: Session, slot: SourceRequestSlot, failure: ResolutionCause): void => {
		if (!session.sourceSlots.delete(slot)) return;
		slot.generation.expire(failure);
	};
	const releaseUnusedSourceSlot = (session: Session, slot: SourceRequestSlot): void => {
		if (!slot.pending && slot.owners.size === 0)
			releaseSourceSlot(session, slot, cause("control", "source_slot_unused"));
	};
	const releaseSourceRequest = (session: Session, slot: SourceRequestSlot): void => {
		slot.pending--;
		releaseUnusedSourceSlot(session, slot);
	};
	const cancelCompetingProposals = (session: Session, winner: SourceRequestSlot): void => {
		for (const slot of [...session.sourceSlots]) {
			if (
				slot === winner ||
				!session.sourceSlots.has(slot) ||
				slot.request.kind !== "proposal" ||
				slot.request.source !== winner.request.source ||
				slot.request.targetDecisionSequence !== winner.request.targetDecisionSequence
			)
				continue;
			releaseSourceSlot(session, slot, cause("source", "proposal_race_lost"));
		}
	};
	const expireSourceHorizon = (
		session: Session,
		decisionSequence: number,
		failure: ResolutionCause,
	): void => {
		for (const slot of [...session.sourceSlots]) {
			if (slot.expiresAtTarget && slot.request.targetDecisionSequence <= decisionSequence)
				releaseSourceSlot(session, slot, failure);
		}
	};
	const releaseAllSourceSlots = (session: Session, failure: ResolutionCause): void => {
		for (const slot of [...session.sourceSlots]) releaseSourceSlot(session, slot, failure);
	};
	const launchSourceRequests = (state: Turn): void => {
		for (const source of sources) {
			if (!sourceEnabled(source, state.session.settings)) continue;
			const count = clampCandidateLimit(source.proposalCount?.(state.settings));
			for (let index = 0; index < count; index++) {
				const slot = claimSourceSlot(
					state.session,
					source.id,
					state.turnID,
					state.decisionSequence,
					count,
					"proposal",
					source.requestLifetime === "actor_decision",
					state.generation.signal,
				);
				if (!slot) break;
				const pending = requestSource(
					{ ...turnContext(state), session: state.session, slot },
					source,
					(signal, reportDraftTokens) =>
						source.propose({
							...turnContext(state),
							definitions: state.definitions,
							proposalIndex: index,
							proposalCount: count,
							signal,
							reportDraftTokens,
						}),
				);
				state.session.sourceTasks.track(pending);
			}
		}
	};
	/** Initial and continuation requests retain the same identity, production and admission owners. */
	const requestSource = (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		produce: (
			signal: AbortSignal,
			reportDraftTokens: (tokens: number) => void,
		) => ReturnType<NonNullable<Source["continue"]>>,
	): Promise<void> => {
		const { session, slot } = scope;
		// Tokens count when spent: empty, failed, aborted and never-admitted requests cost the same as productive ones.
		let draftTokens = 0;
		const reportDraftTokens = (tokens: number) => {
			const spent = finiteValue(tokens);
			draftTokens += spent;
			session.tokenTotal += spent;
		};
		return runSourceRequest({
			request: slot.request,
			generation: slot.generation,
			timeoutMs: source.timeoutMs?.(scope.settings),
			produce: (signal) => session.sourceTasks.track(Promise.resolve(produce(signal, reportDraftTokens))),
			count: (value) => asUpdates(value).length,
		}).then(async ({ value, ...settled }) => {
			const request = Object.freeze({ ...settled, draftTokens });
			try {
				queueSourceRequestEvent(session, slot.request.turnID, scope.settings, request);
				session.effects.enqueue(() =>
					subscriptions.publish("source_request", {
						sessionID: session.id,
						turnID: slot.request.turnID,
						request,
					}),
				);
				if (
					request.settlement.status === "produced" &&
					value !== undefined &&
					session.sourceSlots.has(slot) &&
					slot.generation.active
				) {
					await admitUpdates(scope, source, value);
				}
			} finally {
				releaseSourceRequest(session, slot);
			}
		});
	};
	const admitUpdates = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		updates: PlanUpdate | readonly PlanUpdate[] | undefined,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active || !sourceEnabled(source, session.settings))
			return;
		await Promise.allSettled(
			asUpdates(updates).map(async (update) => {
				const captured = PlanRuntime.capture(update, source.multiStepEnabled?.(scope.settings) !== false);
				if (!("update" in captured)) return;
				// Capture the batch before binding callbacks can mutate producer input.
				await Promise.resolve();
				await applyUpdate(scope, source, captured.update);
			}),
		);
	};
	const applyUpdate = async (
		scope: PlanAdmissionScope<SessionID, Output, StartInput, StateData>,
		source: Source,
		update: PlanUpdate,
	): Promise<void> => {
		const { session } = scope;
		if (session.lifecycle.sealed || !scope.slot.generation.active) return;
		if (update.source !== source.id) return;
		const applied = session.plan.apply(update, session.decisionSequence);
		if (!applied.accepted) return;
		for (const retired of applied.retired) retirePlanAction(session, retired, cause("plan", "superseded"));
		const materializations: Promise<void>[] = [];
		for (const action of applied.upserted) {
			const node = session.plan.get(applied.plan.id, action.id);
			if (!node || node.predictionState.status !== "pending") continue;
			const context = session.actionContexts.get(node.identity.id);
			const issued = !context;
			if (issued) {
				const admissionController = new AbortController();
				const owner = session.turns.get(scope.startInput.turnID);
				scope.slot.owners.add(node.identity.id);
				session.actionContexts.set(node.identity.id, {
					identity: node.identity,
					opportunity: session.plan.opportunity(node.proposalID, node.action.id)!,
					...turnContext(scope),
					draft: planActionDraft(node),
					producers: new Set(),
					admissionController,
					sourceSlot: scope.slot,
					continuationTriggers: new Set(),
					continuationSlots: new Set(),
				});
				const expired = () => {
					const current = session.plan.get(node.proposalID, node.action.id);
					const failure = scope.slot.generation.expiration;
					// Binding transfers preparation to the plan; ending its source request cannot revoke that work.
					if (
						current?.identity.id === node.identity.id &&
						current.actionKey &&
						(failure?.code === "actor_action_arrived" ||
							owner?.generation.expiration?.code === "turn_finished")
					)
						return;
					admissionController.abort(failure);
				};
				scope.slot.generation.signal.addEventListener("abort", expired, {
					once: true,
					signal: admissionController.signal,
				});
				owner?.signal?.addEventListener(
					"abort",
					() => admissionController.abort(cause("control", "turn_aborted")),
					{ once: true, signal: admissionController.signal },
				);
				if (scope.slot.generation.signal.aborted) expired();
			} else {
				context.draft = planActionDraft(node);
			}
			session.effects.enqueue(() =>
				subscriptions.publish("prediction_admitted", {
					source: node.source,
					proposalID: node.identity.proposalID,
					actionID: node.identity.actionID,
					feedback: action.feedback,
					issued,
				}),
			);
			if (issued)
				materializations.push(materializeAction(session, node).finally(() => startQueuedCandidates(session)));
		}
		await Promise.allSettled(materializations);
		if (!materializations.length) startQueuedCandidates(session);
	};
	const executionRouteFor = async (
		input: Omit<Parameters<typeof adapter.preflightCandidate>[0], "route"> & {
			readonly parentWorld?: WorldBranch<Output>;
		},
	): Promise<
		| {
				readonly ok: true;
				readonly route: SpeculativeExecutionRoute;
		  }
		| {
				readonly ok: false;
				readonly cause: ResolutionCause;
		  }
	> => {
		let route: SpeculativeExecutionRoute | undefined;
		try {
			const { callID, index, ...request } = input;
			route = await adapter.resolveExecution(request);
		} catch {
			route = undefined;
		}
		if (input.signal.aborted) return { ok: false, cause: cause("source", "generation_expired") };
		if (!route)
			return {
				ok: false,
				cause: cause(
					"execution",
					"isolation_unavailable",
					"No safe speculative execution route is available.",
				),
			};
		try {
			const preflight = await adapter.preflightCandidate({ ...input, route });
			if (!preflight.ok)
				return {
					ok: false,
					cause: cause("admission", preflight.reason, preflight.detail),
				};
		} catch (error) {
			return {
				ok: false,
				cause: cause("admission", "preflight_failed", errorDetail(error)),
			};
		}
		return input.signal.aborted
			? { ok: false, cause: cause("source", "generation_expired") }
			: { ok: true, route };
	};
	const materializeAction = async (session: Session, node: PlanRuntimeNode): Promise<void> => {
		if (node.actionKey || node.execution.status !== "deferred") return;
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return;
		const concrete = asConcreteInput(node.action.input);
		const prediction = sourcesByID.get(node.source)?.kind !== "preparation";
		if (
			!concrete ||
			!sourceEnabled(sourcesByID.get(node.source), session.settings) ||
			!adapter.definitions(context.startInput).some((tool) => tool.name === node.action.tool) ||
			(prediction &&
				node.action.type === "tool_call" &&
				context.settings.tools &&
				!context.settings.tools.includes(node.action.tool))
		) {
			failUnlaunchable(session, node, cause("admission", concrete ? "tool_disabled" : "invalid_input"));
			return;
		}
		let predictedAction: ActionKey | undefined;
		try {
			predictedAction = await adapter.actionKey(node.action.tool, concrete, {
				type: "start",
				startInput: context.startInput,
				data: context.data,
				...(node.action.operation ? { operation: node.action.operation } : {}),
			});
		} catch {
			predictedAction = undefined;
		}
		if (context.admissionController.signal.aborted) {
			failUnlaunchable(session, node, cause("source", "generation_expired"));
			return;
		}
		if (!predictedAction) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		const executionInput = asConcreteInput(predictedAction.input);
		if (!executionInput) {
			failUnlaunchable(session, node, cause("matching", "action_not_keyable"));
			return;
		}
		if (!session.plan.bindActionKey(node.identity, predictedAction)) return;
		// Binding owns schema validation and argument preparation; raw proposals cannot win the race.
		const slot = context.sourceSlot;
		if (
			session.sourceSlots.has(slot) &&
			slot.request.kind === "proposal" &&
			sourcesByID.get(node.source)?.concurrentProposalPolicy?.(context.settings) === "first_produced"
		)
			cancelCompetingProposals(session, slot);
		if (subscriptions.has("candidate_materialized") && prediction && node.action.type === "tool_call") {
			session.effects.enqueue(() =>
				subscriptions.publish("candidate_materialized", {
					sessionID: session.id,
					turnID: context.startInput.turnID,
					expectedDecisionSequence: node.expectedDecisionSeq,
					latestDecisionSequence: node.latestDecisionSeq,
					source: node.source,
					proposalID: node.proposalID,
					actionID: node.action.id,
					tool: node.action.tool,
					input: structuredClone(concrete),
					predictedAction,
					executionAction: predictedAction,
					...definedFields(node.action, [
						"depth",
						"horizon",
						"conditionalProbability",
						"empiricalProbability",
						"adoptionProbability",
						"expectedDurationMs",
					]),
				}),
			);
		}
		if (session.settings.reuseEnabled === false)
			session.plan.finishPreparation(node.identity, cause("control", "reuse_disabled"));
		else await prepareNodeRoute(session, node, context, predictedAction, executionInput);
		if (context.executionRoute) void launchNode(session, node);
	};
	const prepareNodeRoute = (
		session: Session,
		node: PlanRuntimeNode,
		context: PlanActionContext<StartInput, StateData>,
		action: ActionKey,
		concrete: Record<string, unknown>,
		parentWorld?: WorldBranch<Output>,
	): Promise<void> =>
		prepareWithinResources(context, {
			session,
			controller: context.admissionController,
			forecast: () => ({
				...forecastFor(session.plan.get(node.proposalID, node.action.id) ?? node, session.decisionSequence),
				resourceDemand: resourceDemand(session, action, undefined, node.action.resourceDemand),
			}),
			fail: (failure) => failUnlaunchable(session, node, failure),
			run: async () => {
				const admission = await executionRouteFor({
					...turnContext(context),
					candidate: context.draft,
					tool: action.tool,
					action,
					concrete,
					parentWorld,
					callID: `spec_${session.candidateSequence + 1}`,
					index: session.candidateSequence,
					signal: context.admissionController.signal,
				});
				if (admission.ok) context.executionRoute = admission.route;
				if (
					admission.ok ||
					(admission.cause.stage === "execution" && admission.cause.code === "isolation_unavailable")
				)
					session.plan.finishPreparation(node.identity, admission.ok ? undefined : admission.cause);
				else failUnlaunchable(session, node, admission.cause);
				if (admission.ok) queueContinuation(session, node, undefined, undefined, "prepared");
			},
		});
	const prepareWithinResources = (
		owner: object,
		input: Omit<Preparation, "active" | "completion" | "complete">,
	): Promise<void> => {
		const signal = input.controller.signal;
		if (signal.aborted) return Promise.resolve();
		const { promise: completion, resolve } = Promise.withResolvers<void>();
		const entry: Preparation = {
			...input,
			active: false,
			completion,
			complete: () => {
				signal.removeEventListener("abort", aborted);
				scheduler.complete(entry);
				preparations.delete(owner);
				resolve();
				wakeResourceWaiters();
			},
		};
		const aborted = () => {
			entry.fail(signal.reason ?? cause("control", "preparation_aborted"));
			if (!entry.active) entry.complete(); // Cancellation does not return units still owned by physical preparation.
		};
		preparations.set(owner, entry);
		signal.addEventListener("abort", aborted, { once: true });
		startQueuedCandidates(input.session, input.forecast().actorDemand ? entry : undefined);
		scheduler.watch(0);
		return entry.completion;
	};
	const releaseActionContext = (session: Session, id: string, keepContinuation = false): void => {
		const context = session.actionContexts.get(id);
		if (!context) return;
		session.actionContexts.delete(id);
		context.admissionController.abort(cause("control", "prediction_retired"));
		context.sourceSlot.owners.delete(id);
		releaseUnusedSourceSlot(session, context.sourceSlot);
		if (!keepContinuation)
			for (const slot of context.continuationSlots)
				releaseSourceSlot(session, slot, cause("control", "parent_prediction_not_adopted"));
	};
	const failUnlaunchable = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		session.plan.rejectExecution(node.identity, failure);
		settleUnobserved(session, node, failure);
	};
	const settleBlockedPlanActions = (session: Session): void => {
		for (const node of session.plan.drainBlocked()) {
			failUnlaunchable(session, node, cause("plan", "dependency_impossible"));
		}
	};
	const pendingActorTurn = (session: Session): Turn | undefined =>
		[...session.turns.values()].find(
			(turn) =>
				turn.lifecycle === "active" &&
				turn.actorArrivedAt === undefined &&
				turn.decisionSequence === session.decisionSequence + 1,
		);
	const launchNode = (session: Session, node: PlanRuntimeNode): Promise<void> => {
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return Promise.resolve();
		const promoted = session.plan.promote(node.proposalID, node.action.id);
		if (promoted.status !== "scheduled") return context.launch ?? Promise.resolve();
		node = promoted.node;
		return (context.launch = session.lifecycle.track(
			Promise.resolve()
				.then(async () => {
					const current = session.plan.get(node.proposalID, node.action.id);
					if (
						session.lifecycle.sealed ||
						session.settings.reuseEnabled === false ||
						current?.identity.id !== node.identity.id ||
						current.predictionState.status === "settled"
					)
						return;
					if (!node.actionKey) {
						session.plan.defer(node.proposalID, node.action.id);
						return;
					}
					let route = context.executionRoute;
					if (!route) {
						failUnlaunchable(session, node, cause("plan", "execution_route_missing"));
						return;
					}
					const parent = await dependencyWorld(session, node);
					if (parent === "waiting" || parent === null) {
						session.plan.defer(node.proposalID, node.action.id);
						return;
					}
					const checkpoint = parent && candidateBranch(parent)?.checkpoint;
					if (
						parent &&
						checkpoint &&
						(route.reuse === "shared_result" ||
							(!sameSpeculativeExecutionRoute(parent.route, route) &&
								route.acceptsCheckpoint?.(checkpoint) !== true))
					) {
						await prepareNodeRoute(
							session,
							node,
							context,
							node.actionKey,
							node.actionKey.input as Record<string, unknown>,
							candidateBranch(parent),
						);
						route = context.executionRoute!;
					}
					if (
						parent &&
						(route.reuse === "shared_result" ||
							!checkpoint ||
							(!sameSpeculativeExecutionRoute(parent.route, route) &&
								route.acceptsCheckpoint?.(checkpoint) !== true))
					) {
						session.plan.defer(node.proposalID, node.action.id);
						return;
					}
					await admitCandidate(
						session,
						{
							key: node.actionKey,
							route,
							forkParent: parent,
							kind: node.action.type,
						},
						() =>
							createCandidate(session, context, context.draft, {
								origin: "prediction",
								key: node.actionKey!,
								route,
								forkParent: parent,
								expectedDurationMs: scheduler.evaluate([forecastFor(node, session.decisionSequence)])
									.expectedDurationMs,
							}),
						() => {
							const current = session.plan.get(node.proposalID, node.action.id);
							return (
								!session.lifecycle.sealed &&
								current?.identity.id === node.identity.id &&
								current.predictionState.status !== "settled" &&
								current.execution.status === "scheduled"
							);
						},
						(candidate) => attachNode(session, node, candidate),
					);
				})
				.finally(() => {
					context.launch = undefined;
				}),
		));
	};
	const attachNode = (session: Session, node: PlanRuntimeNode, candidate: Candidate): void => {
		if (!session.plan.attachExecution(node.proposalID, node.action.id, candidate.id, candidate.work)) return;
		session.actionContexts
			.get(node.identity.id)
			?.producers.add(TimelineInterval.producerID(session, candidate.id));
		const forecasts = forecastsForCandidate(session, candidate);
		const scheduled = scheduler.refresh(candidate, forecasts) ?? scheduler.evaluate(forecasts);
		candidate.expectedDurationMs = scheduled.expectedDurationMs;
		const execution = candidate.work.execution;
		if (execution.status === "succeeded" && !execution.output.inputsOnly) {
			queueContinuation(session, node, candidate, execution.output.output, "execution_succeeded");
			return;
		}
		if (execution.status === "queued") startQueuedCandidates(session);
	};
	/** Coalesce plan and producer dispatch behind current Actor events; a concrete Actor intent stays immediate. */
	const startQueuedCandidates = (session: Session, preferred?: Candidate | Preparation) => {
		if (session.lifecycle.sealed) return;
		if (preferred) return launchCandidateBatch(session, preferred);
		return (pendingLaunch ??= session.lifecycle.track(
			new Promise<void>(setImmediate).then(() => {
				pendingLaunch = undefined;
				launchCandidateBatch();
			}),
		));
	};
	const launchCandidateBatch = (owner?: Session, preferred?: Candidate | Preparation): void => {
		const queued: {
			job: Candidate | Preparation;
			session: Session;
			forecasts: readonly PredictionForecast[];
			work: ScheduledWork;
		}[] = [];
		for (const session of preferred ? [owner!] : sessionStates.values()) {
			if (session.lifecycle.sealed) continue;
			if (!preferred) {
				settleBlockedPlanActions(session);
				for (const node of session.plan.launchable()) {
					if (node.actionKey && session.actionContexts.get(node.identity.id)?.executionRoute)
						void launchNode(session, node);
				}
			}
			for (const { job } of scheduler.snapshot(session)) {
				if ("work" in job) scheduler.refresh(job, forecastsForCandidate(session, job));
				else if ("run" in job) scheduler.refresh(job, [job.forecast()]);
			}
			for (const candidate of candidateStore.pending(session.id)) {
				if (candidate.work.execution.status !== "queued" || (preferred && candidate !== preferred)) continue;
				const forecasts = forecastsForCandidate(session, candidate);
				if (!forecasts.length) {
					retireUndemandedCandidate(session, candidate, cause("retention", "prediction_horizon_settled"));
					continue;
				}
				queued.push({
					job: candidate,
					session,
					forecasts,
					work: scheduler.evaluate(forecasts),
				});
			}
		}
		for (const entry of preparations.values()) {
			if (
				entry.active ||
				entry.controller.signal.aborted ||
				entry.session.lifecycle.sealed ||
				(preferred && entry !== preferred)
			)
				continue;
			const forecasts = [entry.forecast()];
			queued.push({
				job: entry,
				session: entry.session,
				forecasts,
				work: scheduler.evaluate(forecasts),
			});
		}
		queued.sort((a, b) => scheduler.compare(a.work, b.work));
		for (const { job, session, forecasts, work } of queued) {
			const preparing = "run" in job;
			if (
				session.settings.reuseEnabled === false ||
				(preparing ? job.controller.signal.aborted : job.work.execution.status !== "queued")
			)
				continue;
			if (work.actorDemand) preemptForActor(session, [job]);
			const admission = scheduler.admit(
				job,
				forecasts,
				scopeFor(session),
				preparing ? "preparation" : "execution",
				work,
				preparing || job.previews?.size || !job.key ? undefined : actionExecutionIdentity(job.key),
			);
			if (!admission.admitted) {
				if (admission.reason === "budget_exhausted")
					cancelScheduled(
						scheduler.preemptFor(
							scopeFor(session),
							work,
							(other) => "run" in other || ("work" in other && reservationAvailable(other.work.reservation)),
							drainingJob,
						),
					);
				continue;
			}
			if (preparing) {
				job.active = true;
				void session.lifecycle.track(
					Promise.resolve()
						.then(job.run)
						.catch((error) => {
							job.fail(cause("admission", "preparation_failed", errorDetail(error)));
						})
						.finally(job.complete),
				);
			} else {
				job.expectedDurationMs = work.expectedDurationMs;
				const startedAt = performance.now();
				if (!job.work.start(startedAt)) {
					scheduler.complete(job);
					continue;
				}
				queueCandidateEvent(session, job);
				void session.lifecycle.track(executeCandidate(session, job, startedAt));
			}
		}
		scheduler.watch();
	};
	const executeCandidate = async (
		session: Session,
		candidate: Candidate,
		startedAt: number,
	): Promise<void> => {
		const action = candidate.key;
		if (!action) return; // Input-only owners are already sealed and cannot launch a tool.
		let branch: WorldBranch<Output> | undefined;
		const inputs =
			candidate.owner.draft.type === "tool_call" &&
			candidate.route.reuse === "shared_result" &&
			!forkWorld(candidate.forkParent)
				? borrowResourceInputs(session, candidate, `inputs:prediction:${candidate.id}`)
				: undefined;
		const draft = candidate.owner.draft;
		try {
			const parent = forkWorld(candidate.forkParent);
			const owner = candidate.owner.startInput;
			// A root fork outliving its turn runs in the live turn's scope: its own turn's snapshots and handoffs are closed.
			const live =
				parent || session.turns.get(owner.turnID)?.lifecycle === "active"
					? undefined
					: [...session.turns.values()].find((turn) => turn.lifecycle === "active")?.startInput;
			candidate.onOperationAdopted = (adoption) => {
				const turn = session.turns.get(adoption.scope.turnID);
				if (session.lifecycle.sealed || session.id !== adoption.scope.sessionID || !turn) return;
				const actorAction: ActorActionIdentity = {
					id: adoption.id,
					kind: "operation",
					sequence: adoption.sequence,
					decisionSequence: turn.decisionSequence,
					turnID: turn.turnID,
				};
				const matches = session.plan.matchable(turn.decisionSequence).flatMap((node) => {
					const operation = node.action.operation;
					if (node.action.type !== "operation" || !node.actionKey) return [];
					const exact =
						operation?.backend === adoption.backend && operation.identity === adoption.operationIdentity;
					return exact || node.actionKey.key === action.key
						? maybe(
								session.plan.claimMatch(
									node,
									actorAction,
									exact
										? { kind: "exact", distance: 0 }
										: {
												kind: "projected",
												distance: 1,
												projector: "owned-operation",
											},
								),
							)
						: [];
				});
				confirmPredictions(session, matches, actorAction, {
					status: "adopted",
					candidateID: candidate.id,
				});
			};
			const evaluation = await TimelineInterval.measure(
				() =>
					adapter.executeCandidate({
						startInput: live ?? owner,
						data: candidate.owner.data,
						candidate: candidate.owner.draft,
						tool: action.tool,
						concrete: action.input as Record<string, unknown>,
						action,
						route: candidate.route,
						callID: candidate.id,
						index: candidate.owner.index,
						signal: candidate.work.controller.signal,
						inputs: inputs?.lookup,
						onInputSource: inputs && ((source) => (candidate.inputSource = source)),
						onOperationAdopted: candidate.onOperationAdopted,
						onOperationJoinable: (available) => {
							candidate.operationJoinable = available;
						},
						acceptOperationScope: session.acceptOperationScope,
						...(parent ? { parentWorld: candidateBranch(parent)! } : {}),
					}),
				(branch) => branch.computationDependencies ?? [],
				candidateProducer(session, candidate),
			);
			branch = evaluation.output;
			inputs?.dispose(); // The returned branch owns its proof before cache admission can evict sources.
			if (!branch.inputsOnly && adapter.observationKey) {
				for (const node of session.plan.consumers(candidate.id))
					session.plan.observe(node.identity, adapter.observationKey(branch.output, action) ?? "");
				settleBlockedPlanActions(session);
			}
			const rejected =
				!branch.inputsOnly &&
				adapter.rejectCandidateOutput?.({
					output: branch.output,
					candidate: publicCandidate(candidate),
				});
			if (rejected) throw new CandidateFailure(cause("execution", "output_rejected", rejected));
			candidate.projectionCoverage = branch.inputsOnly
				? []
				: captureCoverage(action, branch.output, projectionRules);
			candidate.estimatedBytes = estimateValueBytes(branch.output) + inputIndexBytes(branch);
			const completedAt = performance.now();
			const computation = evaluation.computation;
			if (!candidate.work.succeed(branch, computation, completedAt - startedAt)) {
				await session.lifecycle.release(branch);
				return;
			}
			scheduler.observe(actionExecutionIdentity(action), false);
			candidateStore.settle(session.id, candidate, branch, candidate.work.reservation.kind === "shared");
			// An exclusive result waits for its own adoption; meanwhile the unchanged bytes it read can answer other reads.
			const budget = cacheLimits(session.settings);
			// An exclusive result waits for its own adoption; meanwhile the unchanged bytes it read can answer other reads.
			const preimages =
				candidate.work.reservation.kind === "exclusive" && budget.maxEntries > 0 && budget.maxBytes > 0
					? await branch.takeReadInputs?.(budget.maxBytes).catch(() => undefined)
					: undefined;
			if (preimages)
				await retainAuthoritativeBranch(
					session,
					candidate.owner,
					() => candidateStore.has(session.id, candidate),
					candidate.owner.draft,
					action,
					preimages,
					0,
					computation,
					{
						...candidate.route,
						backend: preimages.backend,
						isolation: "resource_snapshot",
						reuse: "shared_result",
					},
				);
			if (!branch.inputsOnly)
				for (const node of session.plan.consumers(candidate.id))
					queueContinuation(session, node, candidate, branch.output, "execution_succeeded");
			trimResults(session, session.settings);
			queueCandidateEvent(session, candidate);
			retireUndemandedCandidate(session, candidate, cause("retention", "execution_settled"));
		} catch (error) {
			if (candidate.work.execution.status !== "succeeded") await session.lifecycle.release(branch);
			const failure =
				error instanceof CandidateFailure
					? error.failure
					: candidate.work.controller.signal.aborted
						? cause("control", "execution_aborted")
						: cause("execution", "candidate_failed", errorDetail(error));
			const executionMs = Math.max(0, performance.now() - startedAt);
			const settled = candidate.work.controller.signal.aborted
				? candidate.work.cancel(failure, executionMs)
				: candidate.work.fail(failure, executionMs);
			if (settled && candidate.work.execution.status === "failed") {
				scheduler.observe(actionExecutionIdentity(action), true);
				if (candidate.owner.draft.type === "operation")
					for (const node of session.plan.consumers(candidate.id)) settleUnobserved(session, node, failure);
			}
			removeCandidate(session.id, candidate);
			if (settled) queueCandidateEvent(session, candidate);
		} finally {
			const execution = candidate.work.execution;
			if (execution.status !== "queued" && execution.status !== "running") {
				const feedback = {
					sessionID: session.id,
					turnID: candidate.owner.startInput.turnID,
					candidate: Object.freeze(candidateEventDescriptor(candidate)),
					reuseFeedback: draft.reuseFeedback,
					status: execution.status,
					executionMs: execution.executionMs,
				};
				session.effects.enqueue(() => subscriptions.publish("execution_settled", feedback));
			}
			candidate.inputSource = candidate.operationJoinable = undefined;
			candidate.operationConsumers = undefined;
			inputs?.dispose();
			scheduler.complete(candidate);
			wakeResourceWaiters();
		}
	};
	const previewActorTool = async (
		input: {
			readonly sessionID: SessionID;
			readonly turnID: string;
			readonly tool: string;
		},
		signal?: AbortSignal,
	): Promise<void> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (
			!state ||
			!state.settings.enabled ||
			state.settings.reuseEnabled === false ||
			signal?.aborted ||
			!actorTurnActive(state)
		)
			return;
		state.actorToolHints.add(input.tool);
		await Promise.all(
			nearestPredictions(state.session, state.decisionSequence, (node) =>
				node.action.tool === input.tool ? { node } : undefined,
			).map(({ node }) => promoteForActor(state.session, node)),
		);
		if (signal?.aborted || state.lifecycle !== "active" || state.session.turns.get(state.turnID) !== state)
			return;
		startQueuedCandidates(state.session);
	};
	const actorActionKey = async (
		input: ConsumeInput,
		call: ActualToolCall,
	): Promise<ActionKey | undefined> => {
		try {
			return await adapter.actionKey(call.tool, call.input, {
				type: "consume",
				consumeInput: input,
			});
		} catch {
			return undefined;
		}
	};
	const previewActorCall = (input: ConsumeInput, signal?: AbortSignal): Promise<void> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (
			!state ||
			!state.settings.enabled ||
			state.settings.reuseEnabled === false ||
			signal?.aborted ||
			!actorTurnActive(state)
		)
			return Promise.resolve();
		const actualCall = adapter.actual(input);
		if (!actualCall.id)
			return state.session.lifecycle.track(promoteActorCall(state, input, actualCall, undefined, signal));
		const existing = state.actorPreviews.get(actualCall.id);
		if (existing) return existing.task;
		const record: ActorPreviewRecord = {
			controller: new AbortController(),
			task: Promise.resolve(),
			state: { status: "pending" },
		};
		const parent = signal ? AbortSignal.any([signal, state.generation.signal]) : state.generation.signal;
		parent.addEventListener("abort", () => record.controller.abort(parent.reason), {
			once: true,
			signal: record.controller.signal,
		});
		state.actorPreviews.set(actualCall.id, record);
		record.task = state.session.lifecycle.track(promoteActorCall(state, input, actualCall, record, signal));
		return record.task;
	};
	const promoteActorCall = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		record: ActorPreviewRecord | undefined,
		signal?: AbortSignal,
	): Promise<void> => {
		const active = () =>
			state.settings.enabled &&
			state.settings.reuseEnabled !== false &&
			!signal?.aborted &&
			!record?.controller.signal.aborted &&
			record?.state.status !== "cancelled" &&
			actorTurnActive(state);
		const action = await actorActionKey(input, actualCall);
		if (!action || !active()) return;
		if (record) record.actionKey = action;
		await Promise.all(
			predictionMatches(state.session, action, state.decisionSequence).map(({ node }) =>
				promoteForActor(state.session, node),
			),
		);
		if (!active()) return;
		for (const { candidate, match } of rankCandidates(state.session, action)) {
			const execution = candidate.work.execution;
			if (!record || execution.status !== "succeeded") {
				if (record) attachActorPreview(record, candidate, "existing");
				if (execution.status === "queued") startQueuedCandidates(state.session, candidate);
				return;
			}
			if (!active()) return;
			const lease = acquireCandidate(
				state.session,
				candidate,
				`preview:${callKey(state.turnID, actualCall.id!)}`,
			);
			if (!lease) continue;
			let projected: ProjectionResult<Output> | undefined;
			let prepared = false;
			attachActorPreview(record, candidate, "existing");
			try {
				if (
					(await authorize(
						{ state, consumeInput: input, actualKey: action, actualCall },
						candidate,
						record.controller.signal,
					)) ||
					!active()
				)
					continue;
				projected = await prepareCandidateOutput(
					state,
					candidate,
					action,
					match,
					{ ...actualCall, input: action.input },
					record.controller.signal,
				);
				if (projected.ok && active() && candidateStore.get(state.sessionID, candidate.id) === candidate) {
					prepared = true;
					if (retainResultView(state.sessionID, candidate, action, projected, state.settings))
						trimResults(state.session, state.settings);
					return;
				}
			} finally {
				if (!prepared && record.state.status === "candidate") {
					candidate.previews?.delete(record);
					record.state = { status: "pending" };
				}
				if (projected?.ok) releaseProjectionResource(state.sessionID, projected.resource);
				lease.release();
			}
		}
		if (!record) return;
		await new Promise<void>(setImmediate);
		if (!active()) return;
		const concrete = asConcreteInput(action.input);
		if (!concrete) return;
		const draft: SpeculativeDraftCandidate = {
			type: "tool_call",
			tool: actualCall.tool,
			input: action.input,
			source: "actor_preview",
		};
		await prepareWithinResources(record, {
			session: state.session,
			controller: record.controller,
			forecast: () => ({
				...actionExecutionIdentity(action),
				decisionBatchesUntilCall: 0,
				actorHint: true,
				actorDemand: record.actorDemand,
				resourceDemand: resourceDemand(state.session, action),
			}),
			fail: () => {
				record.state = { status: "cancelled" };
			},
			run: async () => {
				const admission = await executionRouteFor({
					...turnContext(state),
					candidate: draft,
					tool: action.tool,
					action,
					concrete,
					callID: actualCall.id!,
					index: state.session.candidateSequence,
					signal: record.controller.signal,
				});
				if (!admission.ok || !active()) return;
				const route = admission.route;
				await admitCandidate(
					state.session,
					{ key: action, route },
					() =>
						createCandidate(state.session, state, draft, {
							origin: "actor_preview",
							key: action,
							route,
						}),
					active,
					(candidate, created) => {
						attachActorPreview(record, candidate, created ? "preview" : "existing");
						startQueuedCandidates(state.session, candidate);
					},
				);
			},
		});
	};
	const abandonActorPreview = (
		state: Turn,
		record: ActorPreviewRecord | undefined,
		failure: ResolutionCause,
	): void => {
		if (!record) return;
		const current = record.state;
		record.state = { status: "cancelled" };
		record.actorDemand = false;
		record.controller.abort(failure);
		if (current.status !== "candidate") return;
		const candidate = candidateStore.get(state.sessionID, current.candidateID);
		if (!candidate) return;
		candidate.previews?.delete(record);
		retireUndemandedCandidate(state.session, candidate, failure);
	};
	/** Results may outlive their consumers; work that has not started still needs an owner. */
	const retireUndemandedCandidate = (
		session: Session,
		candidate: Candidate,
		failure: ResolutionCause,
	): void => {
		if (
			!reservationAvailable(candidate.work.reservation) ||
			candidate.previews?.size ||
			hasActorDemand(session, candidate) ||
			session.plan.consumers(candidate.id).length
		)
			return;
		// Executed work belongs to the reuse store; prediction retirement cannot invalidate it.
		const state = candidate.work.execution;
		if (
			state.status === "queued" ||
			(candidate.owner.draft.type === "operation" && state.status !== "running")
		)
			discardCandidate(session, candidate, failure, false);
	};
	/** Preparation proves the current query; every later adoption repeats this proof before committing. */
	const prepareCandidateOutput = async (
		state: Turn,
		candidate: Candidate,
		action: ActionKey,
		match: ActionKeyMatch,
		call: ActualToolCall,
		signal: AbortSignal,
	): Promise<ProjectionResult<Output>> => {
		const branch = candidateBranch(candidate)!;
		const sourceCompatibility =
			branch.reconstructionScope === "current_action" && match.kind !== "exact"
				? undefined
				: scheduler.assessCompatibility(branch.compatibility, action.executionFingerprint);
		if (sourceCompatibility && !sourceCompatibility.compatible)
			return {
				ok: false,
				cause: cause("compatibility", sourceCompatibility.code, sourceCompatibility.detail),
			};
		let mode: "cached" | "output" | "inputs" = "cached";
		let inputs: ReturnType<typeof borrowResourceInputs> | undefined;
		try {
			while (!signal.aborted) {
				if (
					(match.kind !== "exact" || mode === "inputs") &&
					branch.inputSource &&
					(mode !== "cached" || !candidate.resultViews?.has(action.key))
				)
					inputs ??= borrowResourceInputs(
						state.session,
						candidate,
						`inputs:query:${callKey(state.turnID, call.id ?? action.hash)}`,
					);
				const projection = await projectOutput(
					candidate,
					action,
					mode === "inputs" ? { kind: "inputs", distance: match.distance } : match,
					projectionRules,
					{
						action,
						args: call.input,
						callID: call.id ?? action.hash,
						signal,
						inputs: inputs?.lookup,
					},
					mode,
				);
				if (!projection.ok) return projection;
				const compatibility =
					sourceCompatibility ??
					scheduler.assessCompatibility(
						projection.compatibility ?? branch.compatibility,
						action.executionFingerprint,
					);
				const validation = compatibility.compatible
					? await validateWorldBranch(
							projection.validate ? { validate: projection.validate } : branch,
							candidate.route.reuse,
						)
					: {
							status: "incompatible" as const,
							cause: cause("compatibility", compatibility.code, compatibility.detail),
						};
				if (validation.status === "valid") return projection;
				releaseProjectionResource(state.sessionID, projection.resource);
				if (validation.status === "stale") {
					if (projection.reused) {
						mode = "output";
						continue;
					}
					if (
						mode !== "inputs" &&
						branch.reconstructionScope === "current_action" &&
						branch.reconstruct &&
						candidate.route.reuse === "shared_result" &&
						semantics.effect(action) === "observation"
					) {
						mode = "inputs";
						continue;
					}
				}
				return { ok: false, cause: validation.cause };
			}
			return { ok: false, cause: cause("control", "actor_aborted") };
		} finally {
			inputs?.dispose();
		}
	};
	const selectActorCandidate = async (input: ActorSelectionInput): Promise<void> => {
		const { state, actualCall, actualKey, actorAction, pending, preview } = input;
		const signal = input.signal
			? AbortSignal.any([input.signal, state.generation.signal])
			: state.generation.signal;
		const attempted = new Set<Candidate>();
		const stopCandidate = (candidate: Candidate): boolean => {
			const failure = signal?.aborted
				? cause("control", "actor_aborted")
				: !actorTurnActive(state)
					? cause("control", "disabled")
					: state.settings.reuseEnabled === false
						? cause("control", "reuse_disabled")
						: undefined;
			if (!failure) return false;
			actorAction.setFallback(failure, candidate.id);
			return true;
		};
		for (;;) {
			const observation = candidateStore.observe(state.sessionID);
			const stop = AbortSignal.any([signal, observation.signal]);
			try {
				const ranked = rankCandidates(
					state.session,
					actualKey,
					preview?.state.status === "candidate" ? preview.state.candidateID : undefined,
				).filter(({ candidate }) => !attempted.has(candidate));
				const choice = ranked.find(({ candidate }) => {
					const reservation = candidate.work.reservation;
					return reservation.kind === "shared" || reservation.status === "available";
				});
				if (!choice) {
					if (!pending.size && !ranked.length) break;
					await waitForCompletion(Promise.race(pending), stop);
					if (signal.aborted) {
						actorAction.setFallback(cause("control", "actor_aborted"));
						break;
					}
					continue;
				}
				const candidate = choice.candidate;
				attempted.add(candidate);
				const reservation = acquireCandidate(
					state.session,
					candidate,
					`actor:${actorAction.identity.sequence}`,
				);
				if (!reservation) {
					actorAction.rejectCandidate(candidate.id, choice.match, cause("matching", "candidate_reserved"));
					continue;
				}
				(candidate.actorConsumers ??= new Set()).add(actorAction);
				let projection: ProjectionResult<Output> | undefined;
				try {
					if (candidate.work.execution.status === "queued") {
						preemptForActor(
							state.session,
							ranked.map(({ candidate }) => candidate),
						);
						startQueuedCandidates(state.session, candidate);
					}
					const authorization = await authorize(input, candidate, signal);
					if (stopCandidate(candidate)) break;
					if (authorization) {
						actorAction.rejectCandidate(candidate.id, choice.match, authorization);
						continue;
					}
					const waiting = await waitForCompletion(
						Promise.race([candidate.work.completion, ...pending]),
						stop,
					);
					if (stopCandidate(candidate)) break;
					if (waiting.status === "aborted" || !waiting.value) {
						attempted.delete(candidate);
						continue;
					}
					const execution = waiting.value;
					if (execution.status !== "succeeded") {
						actorAction.rejectCandidate(candidate.id, choice.match, execution.cause);
						continue;
					}
					const branch = execution.output;
					// Join only this exact Actor intent; changed arguments or executors cannot inherit its preparation.
					if (
						preview?.state.status === "candidate" &&
						preview.state.candidateID === candidate.id &&
						preview.actionKey?.key === actualKey.key
					)
						await waitForCompletion(preview.task, signal);
					if (stopCandidate(candidate)) break;
					projection = await prepareCandidateOutput(
						state,
						candidate,
						actualKey,
						choice.match,
						actualCall,
						signal,
					);
					if (stopCandidate(candidate)) break;
					if (!projection.ok) {
						actorAction.rejectCandidate(candidate.id, choice.match, projection.cause);
						continue;
					}
					let output = projection.output;
					try {
						if (!projection.validate) {
							const committed = await branch.commit();
							if (choice.match.kind === "exact") output = committed;
						}
					} catch (error) {
						const commitFailure = effectCommitFailure(error, "poisoned");
						if (isPoisonedEffectCommit(commitFailure)) throw commitFailure;
						const failure =
							commitFailure.resolutionCause ??
							cause("commit", "world_commit_failed", errorDetail(commitFailure));
						actorAction.rejectCandidate(candidate.id, choice.match, failure);
						continue;
					}
					reservation.adopt();
					reconcileAuthoritativeEffects(state.session, actualKey, candidate);
					candidate.actorAdopted = true;
					candidate.forkParent = undefined;
					if (reservation.kind === "exclusive") {
						const budget = cacheLimits(state.settings);
						if (branch.takeCommittedInputs && budget.maxEntries > 0 && budget.maxBytes > 0) {
							const inputs = await branch.takeCommittedInputs(budget.maxBytes).catch(() => undefined);
							if (inputs)
								await retainAuthoritativeBranch(
									state.session,
									state,
									() => state.lifecycle === "active",
									candidate.owner.draft,
									candidate.key,
									inputs,
									0,
									execution.toolExecution,
									{
										...candidate.route,
										backend: inputs.backend,
										isolation: "resource_snapshot",
										reuse: "shared_result",
									},
								);
						}
						removeCandidate(state.session.id, candidate);
					} else {
						if (preview) candidate.previews?.delete(preview);
						const retained = retainResultView(
							state.sessionID,
							candidate,
							actualKey,
							projection,
							state.settings,
						);
						candidateStore.recordActorHit(state.sessionID, candidate, cacheLimits(state.settings));
						if (retained) trimResults(state.session, state.settings);
					}
					// Re-evaluating inputs adopts this query's computation, not the source tool's unused output work.
					const toolExecution = projection.inputs ? projection.execution! : execution.toolExecution;
					actorAction.select({
						candidate,
						match: projection.inputs ? { kind: "inputs", distance: choice.match.distance } : choice.match,
						output,
						toolExecution,
						...(projection.execution ? { projection: projection.execution } : {}),
						...(projection.reused ? { projectionReused: true } : {}),
					});
					break;
				} finally {
					if (projection?.ok) releaseProjectionResource(state.sessionID, projection.resource);
					candidate.actorConsumers?.delete(actorAction);
					reservation.release();
					retireUndemandedCandidate(state.session, candidate, cause("retention", "actor_released"));
					scheduler.refresh(candidate, forecastsForCandidate(state.session, candidate));
				}
			} finally {
				observation.dispose();
				if (observation.changed) attempted.clear();
			}
		}
	};
	const prepareActorCall = async (
		input: ConsumeInput,
		signal?: AbortSignal,
		actorArrivedAt = performance.now(),
	): Promise<PreparedActorCall<Output> | undefined> => {
		const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
		if (!state || signal?.aborted || !actorTurnActive(state)) return undefined;
		const actualCall = adapter.actual(input);
		let preview = actualCall.id ? state.actorPreviews.get(actualCall.id) : undefined;
		if (actualCall.id) state.actorPreviews.delete(actualCall.id);
		expireSourceHorizon(state.session, state.decisionSequence, cause("control", "actor_action_arrived"));
		if (state.actorArrivedAt === undefined) {
			state.actorArrivedAt = actorArrivedAt;
			state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
			scheduler.advance();
		}
		const sequence = ++state.session.sequence;
		const actualKey = await actorActionKey(input, actualCall);
		if (state.lifecycle !== "active" || state.session.turns.get(state.turnID) !== state || signal?.aborted) {
			abandonActorPreview(state, preview, cause("control", signal?.aborted ? "actor_aborted" : "disabled"));
			return undefined;
		}
		const actorAction = new ActorAction<Candidate, Output>({
			identity: {
				id: actualCall.id ?? JSON.stringify([input.turnID, sequence]),
				sequence,
				decisionSequence: state.decisionSequence,
				turnID: input.turnID,
			},
			tool: actualCall.tool,
			issuedAt: actorArrivedAt,
			...(actualKey ? { actionKey: actualKey } : {}),
			fallback: cause("matching", "no_candidate"),
		});
		const identity = actorAction.identity;
		state.actorActions.add(actorAction);
		state.actorObservation ??= actualKey ? identity : null;
		const prepared: PreparedActorCall<Output> & {
			output?: Output;
		} = {
			observeOperations: true,
			get reuseEnabled() {
				return state.settings.reuseEnabled !== false;
			},
			withInputs: async (execute) => {
				if (state.settings.reuseEnabled === false) return execute(undefined);
				const inputs = borrowResourceInputs(state.session, undefined, `inputs:actor:${identity.id}`);
				try {
					return await execute(inputs.lookup);
				} finally {
					inputs.dispose();
				}
			},
			settle: (toolExecution, output, operations) =>
				state.session.lifecycle.track(
					settleActorCall(
						state,
						input,
						actualCall,
						actorAction,
						output,
						toolExecution,
						operations && Object.freeze([...operations]),
						matchingPredictions,
					),
				),
		};
		if (actualKey && subscriptions.has("actor_materialized")) {
			state.session.effects.enqueue(() =>
				subscriptions.publish("actor_materialized", {
					sessionID: state.session.id,
					turnID: input.turnID,
					identity,
					tool: actualCall.tool,
					input: structuredClone(actualKey.input),
					action: actualKey,
				}),
			);
		}
		let matchingPredictions: ClaimedPrediction[] = [];
		const pending = new Set<Promise<void>>();
		const actorSignal = signal ? AbortSignal.any([signal, state.generation.signal]) : state.generation.signal;
		const follow = (task: Promise<void>) => {
			const tracked = task
				.catch(() => {})
				.finally(() => {
					pending.delete(tracked);
				});
			pending.add(tracked);
		};
		try {
			if (!actualKey) {
				actorAction.setFallback(cause("matching", "action_not_keyable"));
				return Object.freeze(prepared);
			}
			matchingPredictions = predictionMatches(state.session, actualKey, state.decisionSequence).flatMap(
				({ node, relation }) => maybe(state.session.plan.claimMatch(node, identity, relation)),
			);
			actorAction.match(
				matchingPredictions.map(({ opportunity }) => opportunity.identity),
				(adoption) => confirmPredictions(state.session, matchingPredictions, identity, adoption),
			);
			if (state.settings.reuseEnabled === false) {
				actorAction.setFallback(cause("control", "reuse_disabled"));
				return Object.freeze(prepared);
			}
			if (preview?.actionKey && actionKeyCovers(preview.actionKey, actualKey, projectionRules)) {
				preview.actorDemand = true;
				const preparation = preparations.get(preview);
				if (preparation) startQueuedCandidates(state.session, preparation);
				if (preview.state.status === "pending") follow(preview.task);
			} else if (preview?.state.status === "pending") {
				abandonActorPreview(state, preview, cause("matching", "preview_intent_changed"));
				preview = undefined;
			}
			// Reserve existing work before new preparation can preempt it for the same Actor demand.
			for (const { node } of matchingPredictions)
				follow(Promise.resolve().then(() => promoteForActor(state.session, node, actorSignal)));
			await selectActorCandidate({
				state,
				consumeInput: input,
				actualCall,
				actualKey,
				actorAction,
				pending,
				...(preview ? { preview } : {}),
				...(signal ? { signal } : {}),
			});
			const selected = actorAction.selection;
			if (selected) {
				const previewed =
					preview?.state.status === "candidate" &&
					preview.state.ownership === "preview" &&
					preview.state.candidateID === selected.candidate.id;
				actorAction.settleSelection(previewed ? "preview" : "speculative");
				state.actorActions.delete(actorAction);
				queueActorSettlement(
					state,
					input,
					actualCall,
					actorAction,
					selected.output,
					selected,
					undefined,
					matchingPredictions,
				);
				state.session.effects.enqueue(() => startQueuedCandidates(state.session));
				prepared.output = selected.output;
				return Object.freeze(prepared);
			}
			if (actorAction.fallback.cause.code === "no_candidate")
				for (const { node } of matchingPredictions) {
					const execution = state.session.plan.get(node.proposalID, node.action.id)?.execution;
					if (execution && "cause" in execution) {
						actorAction.setFallback(execution.cause);
						break;
					}
				}
			return Object.freeze(prepared);
		} catch (error) {
			if (isPoisonedEffectCommit(error)) {
				state.actorActions.delete(actorAction);
				actorAction.finishConsumption();
				throw error;
			}
			return Object.freeze(prepared); // Speculation failed after matching: the native run still settles and is recorded.
		} finally {
			abandonActorPreview(state, preview, actorAction.fallback.cause);
			actorAction.deferToFallback();
			if (actorAction.state.status === "awaiting_fallback" && state.actorActions.has(actorAction)) {
				if (!scheduler.has(actorAction)) preemptForActor(state.session);
				state.session.effects.enqueue(() => startQueuedCandidates(state.session));
				try {
					const concrete = asConcreteInput(actualCall.input);
					const captureSignal = signal ?? state.generation.signal;
					if (
						state.settings.reuseEnabled !== false &&
						adapter.captureAuthoritativeResult &&
						concrete &&
						!captureSignal.aborted
					) {
						const capture = await adapter.captureAuthoritativeResult({
							...turnContext(state),
							consumeInput: input,
							tool: actualCall.tool,
							concrete,
							action: actualKey,
							callID: actualCall.id ?? callKey(state.turnID, actualCall.tool),
							signal: captureSignal,
						});
						if (capture) {
							if (!(
								state.session.settings.reuseEnabled !== false &&
								(actualKey || capture.inputsOnly) &&
								capture.route.reuse === "shared_result" &&
								!captureSignal.aborted &&
								actorTurnActive(state) &&
								state.actorActions.has(actorAction) &&
								actorAction.capture(capture)
							))
								state.session.lifecycle.release(capture);
						}
					}
				} catch {
					/* Optional capture cannot prevent authoritative execution. */
				}
			}
		}
	};
	const retainAuthoritativeBranch = async (
		session: Session,
		context: RuntimeTurnContext<StartInput, StateData>,
		active: () => boolean,
		actualCall: ActualToolCall,
		action: ActionKey | undefined,
		branch: WorldBranch<Output>,
		durationMs: number,
		toolExecution: TimelineInterval,
		route: SpeculativeExecutionRoute,
	): Promise<void> => {
		let retained = false;
		try {
			if (
				!active() ||
				session.lifecycle.sealed ||
				session.settings.reuseEnabled === false ||
				(!action && !branch.inputsOnly)
			)
				return;
			const candidate = createCandidate(
				session,
				context,
				{
					type: "tool_call",
					tool: actualCall.tool,
					input: action?.input ?? asConcreteInput(actualCall.input) ?? {},
					source: "actor_result",
				},
				{
					origin: "actor_result",
					key: action,
					route,
					estimatedBytes: estimateValueBytes(branch.output) + inputIndexBytes(branch),
					projectionCoverage:
						action && !branch.inputsOnly ? captureCoverage(action, branch.output, projectionRules) : [],
				},
			);
			TimelineInterval.producedBy(toolExecution, candidateProducer(session, candidate));
			candidate.work.start(toolExecution.startedAt);
			if (!candidate.work.succeed(branch, toolExecution, durationMs)) return;
			const rejected =
				!branch.inputsOnly &&
				adapter.rejectCandidateOutput?.({
					output: branch.output,
					candidate: publicCandidate(candidate),
				});
			if (rejected) return;
			candidateStore.settle(session.id, candidate, branch);
			retained = true;
			trimResults(session, session.settings);
		} catch {
			/* Optional cache retention cannot alter the Actor outcome. */
		} finally {
			if (!retained) session.lifecycle.release(branch);
		}
	};
	const settleActorCall = async (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		output: Output | undefined,
		toolExecution: TimelineInterval,
		operations?: readonly ExecutionOperationBinding[],
		matches: readonly ClaimedPrediction[] = [],
	): Promise<void> => {
		if (state.session.actorRequests.delete(actorAction)) {
			scheduler.complete(actorAction);
			state.session.effects.enqueue(wakeResourceWaiters);
		}
		for (const candidate of candidateStore.values(state.sessionID))
			if (candidate.operationConsumers?.delete(actorAction))
				retireUndemandedCandidate(state.session, candidate, cause("retention", "prediction_horizon_settled"));
		if (!state.actorActions.delete(actorAction)) return;
		const capture = actorAction.takeCapture();
		// Captured inputs add no second tool clock.
		if (capture?.computationDependencies?.length)
			toolExecution = new TimelineInterval(
				toolExecution.startedAt,
				toolExecution.completedAt,
				[{ computation: toolExecution, owned: true }, ...capture.computationDependencies],
				[],
				null,
			);
		const settlement = actorAction.settleActor(toolExecution, outputIsError(output));
		if (!settlement) {
			state.session.lifecycle.release(capture);
			return;
		}
		const execution = settlement.provider.toolExecution;
		const durationMs = execution.completedAt - execution.startedAt;
		const key = actorAction.actionKey;
		if (key) reconcileAuthoritativeEffects(state.session, key);
		// Authoritative feedback must enter the settlement queue before optional cache work can yield.
		queueActorSettlement(state, input, actualCall, actorAction, output, undefined, operations, matches);
		if (
			capture &&
			(capture.inputsOnly || capture.inputSource || (output !== undefined && !outputIsError(output)))
		) {
			try {
				await retainAuthoritativeBranch(
					state.session,
					state,
					() => state.lifecycle === "active",
					actualCall,
					key,
					await capture.seal(output),
					durationMs,
					execution,
					capture.route,
				);
			} catch {
				state.session.lifecycle.release(capture);
			}
		} else if (capture) state.session.lifecycle.release(capture);
	};
	const queueActorSettlement = (
		state: Turn,
		input: ConsumeInput,
		actualCall: ActualToolCall,
		actorAction: ActorAction<Candidate, Output>,
		output: Output | undefined,
		selection?: ActorCandidateSelection<Candidate, Output>,
		operations?: readonly ExecutionOperationBinding[],
		matches: readonly ClaimedPrediction[] = [],
	): void => {
		const settlement = actorAction.settlement;
		if (!settlement) return;
		const consumed = [
			{
				computation: settlement.provider.toolExecution,
				reused: !!selection && (selection.match.kind !== "inputs" || !!selection.projectionReused),
			},
			...(selection?.projection
				? [
						{
							computation: selection.projection,
							reused: selection.projectionReused,
						},
					]
				: []),
		];
		const reused = TimelineInterval.reusedProducers(consumed);
		actorAction.finishConsumption(reused);
		for (const context of state.session.actionContexts.values())
			context.served ||= [...context.producers].some((producer) => reused.has(producer));
		const computation = state.session.timeline?.recordCall(consumed, actorAction.issuedAt, performance.now());
		const key = actorAction.actionKey;
		const candidate = selection?.candidate;
		// Credit consumed work before new observations or continuations can retire its prediction.
		if (candidate && key && settlement.provider.kind === "speculative")
			for (const { node } of matches)
				queueContinuation(state.session, node, candidate, output, "actor_adopted", {
					key,
					input: asConcreteInput(actualCall.input) ?? key.input,
				});
		if (output !== undefined && adapter.observationKey) {
			const observation = adapter.observationKey(output, key) ?? "";
			for (const prediction of settlement.matchedPredictions)
				state.session.plan.observe(prediction, observation);
			startQueuedCandidates(state.session);
		}
		const descriptor =
			candidate &&
			(subscriptions.has("actor_settled") || subscriptions.has("actor_observed") || adapter.onEvent)
				? Object.freeze(candidateEventDescriptor(candidate))
				: undefined;
		const event: SpeculativeActionEvent<SessionID> | undefined = adapter.onEvent
			? {
					type: "actor_action",
					...eventEnvelope(state.session, state.turnID, state.settings),
					settlement,
					...(computation ? { computation } : {}),
					actualAction: diagnosticAction(actorAction.tool, actualCall.input, key),
					...(descriptor ? { candidate: descriptor } : {}),
				}
			: undefined;
		const feedback = {
			sessionID: state.sessionID,
			turnID: state.turnID,
			...(key ? { action: key } : {}),
			settlement,
			...(descriptor ? { candidate: descriptor } : {}),
			candidateFeedback: candidate?.owner.draft.feedback,
			computation,
		};
		state.session.effects.enqueue(() => subscriptions.publish("actor_settled", feedback));
		if (event)
			state.session.effects.enqueue(() => {
				state.session.events.enqueue(event);
			});
		if (subscriptions.has("actor_observed")) {
			try {
				const observation = cloneSharedData({
					concrete: asConcreteInput(actualCall.input) ?? {},
					...(output !== undefined ? { output } : {}),
				});
				const execution = candidate?.work.execution;
				const observed = {
					...turnContext(state),
					...feedback,
					...observation,
					consumeInput: input,
					tool: actorAction.tool,
					operations: operations ?? (candidate && candidateBranch(candidate)?.operations),
					durationMs:
						settlement.provider.kind === "actor"
							? settlement.provider.durationMs
							: execution && "executionMs" in execution
								? execution.executionMs
								: 0,
					order: settlement.actorAction.sequence,
					signal: state.generation.signal,
				};
				state.session.effects.enqueue(() =>
					subscriptions.publish("actor_observed", observed, (event) => ({
						...event,
						...cloneSharedData({
							concrete: event.concrete,
							...(event.output !== undefined ? { output: event.output } : {}),
						}),
					})),
				);
			} catch {
				/* Unowned data can settle normally but cannot train a source. */
			}
		}
	};
	const confirmPredictions = (
		session: Session,
		matches: readonly ClaimedPrediction[],
		actorAction: ActorActionIdentity,
		adoption: PredictionAdoption,
	): void => {
		for (const { node, opportunity } of matches) {
			const settlement = session.plan.confirm(opportunity, actorAction, adoption);
			if (settlement) predictionSettled(session, node, settlement);
		}
	};
	const settlePredictionFrontier = (state: Turn): void => {
		const observation = state.actorObservation;
		if (observation === undefined) return;
		state.session.decisionSequence = Math.max(state.session.decisionSequence, state.decisionSequence);
		for (const node of state.session.plan.due(state.decisionSequence)) {
			if (node.predictionState.status !== "pending") continue;
			if (node.action.type === "operation") {
				settleUnobserved(state.session, node, cause("matching", "operation_not_observed"));
				continue;
			}
			if (!observation) {
				settleUnobserved(state.session, node, cause("matching", "actor_action_not_keyable"));
				continue;
			}
			const settlement = state.session.plan.miss(node.proposalID, node.action.id, observation);
			if (settlement) predictionSettled(state.session, node, settlement);
		}
		startQueuedCandidates(state.session);
	};
	const predictionSettled = (
		session: Session,
		node: PlanRuntimeNode,
		settlement: PredictionSettlement,
	): void => {
		const context = session.actionContexts.get(node.identity.id);
		if (!context) return;
		const source = sourcesByID.get(context.identity.source);
		const candidateID = "candidateID" in node.execution ? node.execution.candidateID : undefined;
		const event: SpeculativeActionEvent<SessionID> | undefined = adapter.onEvent
			? {
					type:
						node.action.type === "operation" || source?.kind === "preparation"
							? "operation_prediction"
							: "prediction",
					...eventEnvelope(session, context.startInput.turnID, context.settings),
					tool: node.action.tool,
					predictedAction: diagnosticAction(node.action.tool, node.action.input, node.actionKey),
					...(node.action.mode ? { mode: node.action.mode } : {}),
					settlement,
				}
			: undefined;
		session.effects.enqueue(() =>
			subscriptions.publish("prediction_settled", {
				sessionID: session.id,
				turnID: context.startInput.turnID,
				tool: node.action.tool,
				...(node.actionKey ? { action: node.actionKey } : {}),
				settlement,
			}),
		);
		if (event)
			session.effects.enqueue(() => {
				session.events.enqueue(event);
			});
		if (subscriptions.has("prediction_feedback")) {
			const pending =
				!context.served && context.producers.size
					? [...session.turns.values()].flatMap((turn) =>
							[...turn.actorActions].map((action) => action.consumption),
						)
					: [];
			const deliver = (served: boolean) =>
				session.effects.enqueue(() =>
					subscriptions.publish("prediction_feedback", {
						proposalID: context.identity.proposalID,
						actionID: context.identity.actionID,
						feedback: context.draft.feedback,
						settlement,
						served,
					}),
				);
			// Outside the effects queue, settle once on actual consumption or after every current consumer misses.
			if (pending.length)
				void new Promise<boolean>((resolve) => {
					let remaining = pending.length;
					for (const receipt of pending)
						void receipt.then((reused) => {
							const served = [...context.producers].some((producer) => reused.has(producer));
							if (served || !--remaining) resolve(served);
						});
				}).then(deliver);
			else deliver(context.served === true);
		}
		const matched = settlement.observation === "observed" && settlement.match.matched;
		releaseActionContext(session, node.identity.id, matched);
		const candidate = candidateID && candidateStore.get(session.id, candidateID);
		if (candidate)
			retireUndemandedCandidate(session, candidate, cause("retention", "prediction_horizon_settled"));
	};
	const settleUnobserved = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const settlement = session.actionContexts.get(node.identity.id)?.opportunity.unobserve(failure);
		if (settlement) predictionSettled(session, node, settlement);
	};
	const queueContinuation = (
		session: Session,
		node: PlanRuntimeNode,
		candidate: Candidate | undefined,
		output: Output | undefined,
		trigger: "prepared" | "execution_succeeded" | "actor_adopted",
		adoptedAction?: AdoptedAction,
	): void => {
		if (node.action.type === "operation") return;
		const current = session.plan.get(node.proposalID, node.action.id);
		if (current?.identity.id !== node.identity.id) return;
		if (output !== undefined && adapter.observationKey)
			session.plan.observe(
				node.identity,
				adapter.observationKey(output, adoptedAction?.key ?? current.actionKey) ?? "",
			);
		const context = session.actionContexts.get(node.identity.id);
		const source = context ? sourcesByID.get(context.identity.source) : undefined;
		if (!sourceEnabled(source, session.settings)) return;
		if (context && source?.continuationBatch && trigger === "execution_succeeded") {
			try {
				queuePeerContinuations(session, current, context, source);
			} catch {
				/* Producer feedback is advisory. */
			}
		}
		if (
			!context ||
			!source?.continue ||
			source.multiStepEnabled?.(context.settings, context.draft.feedback) === false ||
			context.continuationTriggers.has(trigger)
		)
			return;
		context.continuationTriggers.add(trigger);
		let continuations: readonly PlanContinuation[];
		let scoped = false;
		try {
			const filter = source.continueOn;
			if (trigger === "prepared" && !filter) return;
			const selected =
				typeof filter === "function"
					? filter({
							actionID: node.action.id,
							action: adoptedAction?.key ?? current.actionKey!,
							feedback: context.draft.feedback,
							output,
							trigger,
						})
					: (filter?.includes(trigger) ?? true);
			if (!selected) return;
			scoped = typeof selected !== "boolean";
			continuations =
				typeof selected === "boolean"
					? [
							{
								feedback: context.draft.feedback,
								actionIDs: source.continuationBatch?.({
									proposalID: node.proposalID,
									actionID: node.action.id,
									feedback: context.draft.feedback,
								}) ?? [node.action.id],
							},
						]
					: selected;
		} catch {
			return;
		} // Producer feedback cannot alter completed execution.
		const parentDecisionSequence =
			current.predictionState.status === "matching"
				? (current.predictionState.actorAction.decisionSequence ?? current.expectedDecisionSeq)
				: current.expectedDecisionSeq;
		const targetDecisionSequence = parentDecisionSequence + 1;
		for (const continuation of continuations) {
			const parents = continuation.actionIDs.map((id) => {
				const identity = session.plan.get(node.proposalID, id)?.identity.id;
				return identity ? session.actionContexts.get(identity) : undefined;
			});
			if (
				!parents.length ||
				new Set(continuation.actionIDs).size !== parents.length ||
				parents.some((parent) => !parent)
			)
				continue;
			const pending = Promise.all([
				startQueuedCandidates(session),
				...(scoped ? [] : parents.map((parent) => continuationTails.get(parent!))),
			])
				.then(() =>
					requestContinuation(
						session,
						source,
						parents as (typeof context)[],
						targetDecisionSequence,
						(signal, reportDraftTokens) => {
							const revision = session.plan.reserveRevision(node.proposalID);
							if (revision === undefined) return undefined;
							return source.continue!({
								...turnContext(context),
								candidate: candidate
									? predictionCandidate(candidate, node)
									: {
											...node.action,
											id: node.identity.id,
											key: current.actionKey,
											input: current.actionKey!.input,
											source: node.source,
										},
								...(adoptedAction ? { adoptedAction } : {}),
								proposalID: node.proposalID,
								actionID: node.action.id,
								revision,
								reserveRevision: () => session.plan.reserveRevision(node.proposalID) ?? revision,
								feedback: continuation.feedback,
								output,
								trigger,
								signal,
								reportDraftTokens,
							});
						},
					),
				)
				.catch(() => {
					// Continuation failure cannot revoke completed work or Actor adoption.
				});
			if (!scoped) for (const parent of parents) continuationTails.set(parent!, pending);
			session.sourceTasks.track(pending);
		}
	};
	const requestContinuation = async (
		session: Session,
		source: Source,
		parents: readonly PlanActionContext<StartInput, StateData>[],
		targetDecisionSequence: number,
		produce: Parameters<typeof requestSource>[2],
	): Promise<void> => {
		const context = parents[0]!;
		if (
			session.lifecycle.sealed ||
			!sourceEnabled(source, session.settings) ||
			targetDecisionSequence <= session.decisionSequence ||
			parents.some(
				({ identity }) =>
					session.plan.get(identity.proposalID, identity.actionID)?.identity.id !== identity.id,
			)
		)
			return;
		const slot = claimSourceSlot(
			session,
			source.id,
			context.startInput.turnID,
			targetDecisionSequence,
			clampCandidateLimit(source.proposalCount?.(context.settings)),
			"continuation",
			true,
			undefined,
			context.identity.proposalID,
		);
		if (!slot) return;
		for (const parent of parents) parent.continuationSlots.add(slot);
		await requestSource({ ...turnContext(context), session, slot }, source, produce);
	};
	const queuePeerContinuations = (
		session: Session,
		node: PlanRuntimeNode,
		context: PlanActionContext<StartInput, StateData>,
		source: Source,
	): void => {
		const peers = sources.filter(
			(peer) =>
				peer.id !== source.id &&
				peer.continueFrom &&
				sourceEnabled(peer, session.settings) &&
				peer.multiStepEnabled?.(context.settings) !== false,
		);
		if (!peers.length) return;
		const ids = source.continuationBatch!({
			proposalID: node.proposalID,
			actionID: node.action.id,
			feedback: context.draft.feedback,
		});
		if (!ids?.length || !ids.includes(node.action.id) || new Set(ids).size !== ids.length) return;
		const batch: Parameters<NonNullable<Source["continueFrom"]>>[0]["batch"][number][] = [];
		const parents: PlanActionContext<StartInput, StateData>[] = [];
		for (const id of ids) {
			const parent = session.plan.get(node.proposalID, id);
			const owner = parent && session.actionContexts.get(parent.identity.id);
			if (
				!parent ||
				parent.action.type !== "tool_call" ||
				!owner ||
				owner.admissionController.signal.aborted ||
				parent.predictionState.status !== "pending" ||
				parent.expectedDecisionSeq !== session.decisionSequence + 1 ||
				parent.action.dependsOn?.length ||
				parent.execution.status !== "succeeded"
			)
				return;
			const candidate = candidateStore.get(session.id, parent.execution.candidateID);
			if (candidate?.work.execution.status !== "succeeded" || candidate.work.execution.output.inputsOnly)
				return;
			batch.push({
				identity: parent.identity,
				candidate: predictionCandidate(candidate, parent),
				output: candidate.work.execution.output.output,
			});
			parents.push(owner);
		}
		const requested = (parents[0]!.peerContinuations ??= new Set());
		for (const peer of peers) {
			if (requested.has(peer.id)) continue;
			requested.add(peer.id);
			const pending = requestContinuation(
				session,
				peer,
				parents,
				node.expectedDecisionSeq + 1,
				async (requestSignal, reportDraftTokens) => {
					const signal = AbortSignal.any([
						requestSignal,
						...parents.map((parent) => parent.admissionController.signal),
					]);
					if (signal.aborted) return undefined;
					const update = await peer.continueFrom!({
						...turnContext(context),
						batch,
						signal,
						reportDraftTokens,
					});
					return signal.aborted ? undefined : update;
				},
			).catch(() => {});
			session.sourceTasks.track(pending);
		}
	};
	const retirePlanAction = (session: Session, node: PlanRuntimeNode, failure: ResolutionCause): void => {
		const opportunity = session.actionContexts.get(node.identity.id)?.opportunity;
		if (opportunity?.state.status === "matching") return;
		const finalized = opportunity?.unobserve(failure);
		if (finalized) predictionSettled(session, node, finalized);
		else releaseActionContext(session, node.identity.id);
	};
	const descendsFrom = (candidate: Candidate, ancestor: Candidate): boolean => {
		for (let parent = candidate.forkParent; parent; parent = parent.forkParent) {
			if (parent === ancestor) return true;
		}
		return false;
	};
	/** Lineage constructs future executions; a sealed result proves its own adoption. */
	const forkWorld = (candidate?: Candidate): Candidate | undefined => {
		for (let current = candidate; current && !current.actorAdopted; current = current.forkParent) {
			if (candidateBranch(current)?.checkpoint) return current;
		}
	};
	const dependencyWorld = async (
		session: Session,
		node: PlanRuntimeNode,
	): Promise<Candidate | null | "waiting" | undefined> => {
		const parents = new Set<Candidate>();
		const visited = new Set<string>();
		let waiting = false;
		const visit = async (child: PlanRuntimeNode): Promise<void> => {
			for (const dependency of child.action.dependsOn ?? []) {
				const parentNode = session.plan.dependency(child.proposalID, dependency);
				if (!parentNode || parentNode.action.type === "operation" || visited.has(parentNode.identity.id))
					continue;
				visited.add(parentNode.identity.id);
				const candidate =
					"candidateID" in parentNode.execution
						? candidateStore.get(session.id, parentNode.execution.candidateID)
						: undefined;
				if (dependency.condition === "prediction" || dependency.condition === "resource") {
					await visit(parentNode);
					if (dependency.condition === "resource" && !parentNode.actionKey) {
						if (!["failed", "cancelled"].includes(parentNode.execution.status)) waiting = true;
						continue;
					}
					const effect = parentNode.actionKey && semantics.effect(parentNode.actionKey);
					if (effect === "observation" || candidate?.actorAdopted) continue;
					if (effect === "workspace_mutation") {
						if (
							(await session.actionContexts
								.get(node.identity.id)
								?.executionRoute?.dependsOn?.(parentNode.actionKey!)) === false
						)
							continue;
						if (!["succeeded", "failed", "cancelled"].includes(parentNode.execution.status)) {
							waiting = true;
							continue;
						}
					}
				}
				const parent = forkWorld(candidate);
				if (parent) parents.add(parent);
			}
		};
		await visit(node);
		if (waiting) return "waiting";
		if (!parents.size) return undefined;
		return (
			[...parents].find((candidate) =>
				[...parents].every((parent) => parent === candidate || descendsFrom(candidate, parent)),
			) ?? null
		);
	};
	const hasActorDemand = (session: Session, candidate: Candidate): boolean =>
		!!candidate.actorConsumers?.size ||
		[...(candidate.previews ?? [])].some((preview) => preview.actorDemand) ||
		[...(candidate.operationConsumers ?? [])].some((actor) => session.actorRequests.has(actor));
	const forecastsForCandidate = (session: Session, candidate: Candidate): readonly PredictionForecast[] => {
		const action = candidate.key;
		if (!action) return [];
		const nodes = session.plan.consumers(candidate.id);
		const execution = candidate.work.execution;
		const current = {
			elapsedMs: execution.status === "running" ? Math.max(0, performance.now() - execution.startedAt) : 0,
			actorDemand: hasActorDemand(session, candidate),
			actorHint:
				!!candidate.previews?.size || pendingActorTurn(session)?.actorToolHints.has(action.tool) === true,
		};
		if (nodes.length)
			return nodes.map((node) => ({
				...forecastFor(node, session.decisionSequence),
				...current,
				resourceDemand: resourceDemand(session, action, candidate.route, node.action.resourceDemand),
			}));
		if (!current.actorDemand && !candidate.previews?.size) return [];
		return [
			{
				...actionExecutionIdentity(action),
				...current,
				expectedDurationMs: candidate.expectedDurationMs,
				resourceDemand: resourceDemand(session, action, candidate.route),
				decisionBatchesUntilCall: 0,
			},
		];
	};
	const authorize = async (
		input: Pick<ActorSelectionInput, "state" | "consumeInput" | "actualKey" | "actualCall">,
		candidate: Candidate,
		signal?: AbortSignal,
	): Promise<ResolutionCause | undefined> => {
		if (!adapter.authorizeCandidate) return undefined;
		const { state, consumeInput, actualKey: action, actualCall } = input;
		const concrete = asConcreteInput(actualCall.input);
		if (!concrete) return cause("authorization", "invalid_input");
		try {
			const result = await adapter.authorizeCandidate({
				stateData: state.data,
				consumeInput,
				settings: state.settings,
				action,
				route: candidate.route,
				candidate: publicCandidate(candidate),
				tool: actualCall.tool,
				concrete,
				...(signal ? { signal } : {}),
			});
			return result.ok ? undefined : cause("authorization", result.reason, result.detail);
		} catch (error) {
			return cause("authorization", "authorization_failed", errorDetail(error));
		}
	};
	const rankCandidates = (session: Session, action: ActionKey, preferred?: string) => {
		const now = performance.now();
		const choices = candidateStore
			.lookup(
				session.id,
				action,
				(candidate) => candidate.work.execution.status !== "succeeded",
				semantics.effect(action) === "observation",
			)
			.flatMap(({ entry: candidate, match }) => {
				const execution = candidate.work.execution;
				const reservation = candidate.work.reservation;
				if (
					candidate.owner.draft.type !== "tool_call" ||
					!activeExecution(candidate) ||
					(reservation.kind === "exclusive" && reservation.status === "consumed")
				)
					return [];
				if (candidateBranch(candidate)?.inputsOnly) {
					if (semantics.effect(action) !== "observation") return [];
					match = { kind: "inputs", distance: match.distance };
				}
				const remainingMs =
					execution.status === "running"
						? candidate.expectedDurationMs === undefined
							? Infinity
							: Math.max(0, candidate.expectedDurationMs - (now - execution.startedAt))
						: execution.status === "queued"
							? (candidate.expectedDurationMs ?? Infinity)
							: 0;
				return [
					{
						candidate,
						match,
						ready: execution.status === "succeeded",
						remainingMs,
					},
				];
			});
		if (choices.length < 2) return choices;
		const inputConsumers = new Set(
			choices.flatMap(({ candidate, match }) => {
				const reservation = candidate.work.reservation;
				return match.kind === "inputs" && reservation.kind === "shared" ? reservation.owners : [];
			}),
		);
		const priority = ({ candidate, match, ready }: (typeof choices)[number]) =>
			ready && (match.kind !== "inputs" || candidate.resultViews?.has(action.key))
				? 0
				: match.kind === "exact" &&
					  candidate.work.execution.status === "running" &&
					  inputConsumers.has(`inputs:prediction:${candidate.id}`)
					? 1
					: 2;
		// A ready input owner is not a ready query. Join its exact active consumer before rebuilding it.
		return choices.sort(
			(left, right) =>
				priority(left) - priority(right) ||
				Number(right.candidate.id === preferred) - Number(left.candidate.id === preferred) ||
				Number(right.ready) - Number(left.ready) ||
				left.remainingMs - right.remainingMs ||
				left.match.distance - right.match.distance ||
				right.candidate.owner.index - left.candidate.owner.index,
		);
	};
	const nearestPredictions = <
		Selection extends {
			readonly node: PlanRuntimeNode;
		},
	>(
		session: Session,
		decisionSequence: number,
		select: (node: PlanRuntimeNode) => Selection | undefined,
	): readonly Selection[] => {
		const selected = new Map<string, Selection>();
		for (const node of session.plan.matchable(decisionSequence)) {
			if (!node.actionKey || node.action.type !== "tool_call") continue;
			const selection = select(node);
			if (!selection) continue;
			const previous = selected.get(node.proposalID)?.node.expectedDecisionSeq;
			const expected = node.expectedDecisionSeq;
			// Prefer the latest due action, otherwise the nearest future action; retain insertion-order ties.
			if (
				previous === undefined ||
				(expected <= decisionSequence
					? previous > decisionSequence || expected > previous
					: expected < previous)
			)
				selected.set(node.proposalID, selection);
		}
		return [...selected.values()];
	};
	const predictionMatches = (
		session: Session,
		action: ActionKey,
		decisionSequence: number,
	): readonly {
		readonly node: PlanRuntimeNode;
		readonly relation: ActionKeyMatch;
	}[] =>
		nearestPredictions(session, decisionSequence, (node) => {
			const relation = actionKeyMatch(node.actionKey!, action, projectionRules, true);
			return relation ? { node, relation } : undefined;
		});
	const promoteForActor = async (
		session: Session,
		node: PlanRuntimeNode,
		signal?: AbortSignal,
	): Promise<void> => {
		const context = session.actionContexts.get(node.identity.id);
		const preparation = context && preparations.get(context);
		if (preparation && context.opportunity.state.status === "matching") {
			startQueuedCandidates(session, preparation);
			if ((await waitForCompletion(preparation.completion, signal)).status === "aborted") return;
		}
		await launchNode(session, node);
	};
	const resourceDemand = (
		session: Session,
		action: ActionKey | string,
		route?: SpeculativeExecutionRoute,
		declared?: number | HardwareResources,
	): HardwareResources => {
		const definition = semantics.definition(action);
		const heavy =
			(route && route.isolation !== "resource_snapshot") ||
			!definition ||
			definition.effect === "unbounded" ||
			definition.resourceScope === "tree_entries" ||
			definition.resourceScope === "tree_content" ||
			definition.resourceScope === "captured_inputs";
		const policy = normalizeSchedulingSettings(session.settings.scheduling);
		return {
			cpu: Math.min(
				concurrentLimit(session.settings),
				adapter.resources?.initial.cpuCount ?? Infinity,
				heavy ? policy.heavyCpu : policy.lightCpu,
			),
			memory: heavy ? policy.heavyMemoryBytes : policy.lightMemoryBytes,
			io: heavy ? policy.heavyIo : policy.lightIo,
			...(typeof declared === "number" ? { cpu: declared } : declared),
		};
	};
	const cancelScheduled = (jobs: readonly ScheduledJob[], actor = false): void => {
		for (const job of jobs) {
			if ("run" in job) {
				const failure = cause("admission", actor ? "preempted_by_actor" : "scheduler_preempted");
				job.controller.abort(failure);
			} else if ("work" in job) {
				const owner = scheduler.snapshot().find((entry) => entry.job === job)?.scope.owner as
					Session | undefined;
				if (owner) {
					discardCandidate(owner, job, cause("admission", "scheduler_preempted"), false);
					owner.plan.rearmExecution(job.id);
				}
			}
		}
	};
	const preemptForActor = (
		session: Session,
		protectedCandidates: readonly (Candidate | Preparation)[] = [],
	): void => {
		for (const turn of session.turns.values())
			for (const action of turn.actorActions) {
				if (action.state.status !== "awaiting_fallback") continue;
				session.actorRequests.add(action);
				const forecast = [
					{
						tool: action.tool,
						actorDemand: true,
						resourceDemand: resourceDemand(session, action.actionKey ?? action.tool),
					},
				];
				if (scheduler.has(action)) scheduler.refresh(action, forecast);
				else scheduler.admit(action, forecast, scopeFor(session), "actor");
			}
		for (const candidate of candidateStore.values(session.id)) {
			for (const consumer of candidate.operationConsumers ?? [])
				if (!session.actorRequests.has(consumer)) candidate.operationConsumers!.delete(consumer);
			try {
				if (candidate.operationJoinable?.())
					for (const turn of session.turns.values())
						for (const action of turn.actorActions)
							if (
								action.state.status === "awaiting_fallback" &&
								(!action.actionKey ||
									semantics.requirements(action.actionKey)?.capabilities.includes("invocation.process"))
							)
								(candidate.operationConsumers ??= new Set()).add(action);
			} catch {
				/* A failed acquisition hint reserves no hardware. */
			}
		}
		const preferred = protectedCandidates.find((candidate) => !scheduler.has(candidate));
		const incoming =
			preferred &&
			scheduler.evaluate(
				("run" in preferred ? [preferred.forecast()] : forecastsForCandidate(session, preferred)).map(
					(f) => ({ ...f, actorDemand: true }),
				),
			);
		cancelScheduled(
			scheduler.preemptFor(
				scopeFor(session),
				incoming,
				(job) => {
					if ("run" in job)
						return (
							job.active &&
							!job.controller.signal.aborted &&
							!job.forecast().actorDemand &&
							!protectedCandidates.includes(job)
						);
					if (
						!("work" in job) ||
						job.work.execution.status !== "running" ||
						protectedCandidates.includes(job) ||
						!reservationAvailable(job.work.reservation)
					)
						return false;
					const owner = scheduler.snapshot().find((entry) => entry.job === job)?.scope.owner as Session;
					return !hasActorDemand(owner, job);
				},
				drainingJob,
			),
			true,
		);
	};
	const drainingJob = (job: ScheduledJob): boolean =>
		"run" in job ? job.controller.signal.aborted : "work" in job && job.work.execution.status !== "running";
	const discardCandidate = (
		session: Session,
		candidate: Candidate,
		failure: ResolutionCause,
		dispatch = true,
	): void => {
		const state = candidate.work.execution;
		if (state.status !== "queued" && state.status !== "running") {
			removeCandidate(session.id, candidate);
			return;
		}
		const executionMs = state.status === "running" ? Math.max(0, performance.now() - state.startedAt) : 0;
		const settled = candidate.work.cancel(failure, executionMs);
		removeCandidate(session.id, candidate);
		if (settled) queueCandidateEvent(session, candidate);
		if (dispatch) startQueuedCandidates(session);
	};
	const reconcileStores = async (state: Turn): Promise<void> => {
		const available = new Set(state.definitions.map((definition) => definition.name));
		for (const candidate of candidateStore.pending(state.sessionID)) {
			if (candidate.work.execution.status === "queued" && !available.has(candidate.owner.draft.tool))
				discardCandidate(state.session, candidate, cause("control", "tool_disabled"));
		}
		trimResults(state.session, state.settings);
	};
	const trimResults = (session: Session, settings: SpeculativeActionSettings): void => {
		const parents = new Set(
			session.plan.unsettled().flatMap((node) =>
				(node.action.dependsOn ?? []).flatMap((dependency) => {
					const execution = session.plan.dependency(node.proposalID, dependency)?.execution;
					return execution && "candidateID" in execution ? [execution.candidateID] : [];
				}),
			),
		);
		for (const candidate of candidateStore.values(session.id))
			for (let parent = candidate.forkParent; parent && !parent.actorAdopted; parent = parent.forkParent)
				parents.add(parent.id);
		for (const candidate of candidateStore.trim(
			session.id,
			cacheLimits(settings),
			(entry) =>
				!entry.previews?.size && reservationAvailable(entry.work.reservation) && !parents.has(entry.id),
		)) {
			removeCandidate(session.id, candidate);
		}
	};
	const reconcileAuthoritativeEffects = (session: Session, action: ActionKey, adopted?: Candidate): void => {
		const effect = semantics.effect(action);
		if (effect === "observation" || adopted?.work.reservation.kind === "shared") return;
		const changed = (
			adopted ? (candidateBranch(adopted)?.resources ?? action.resources) : action.resources
		).map((resource) => rootedResource(action, resource));
		if (!changed.length) return;
		const paths = (adopted || effect === "workspace_mutation" ? changed : []).filter((resource) =>
			path.isAbsolute(resource),
		);
		// A native unbounded call names no write set; its neighbours validate at adoption.
		const unbounded = !adopted && effect === "unbounded";
		const candidates = candidateStore.values(session.id);
		const invalid = new Set<Candidate>();
		for (const candidate of candidates) {
			if (candidate === adopted || (adopted && descendsFrom(candidate, adopted))) continue;
			try {
				const removed = paths.length ? candidateBranch(candidate)?.invalidateInputs?.(paths) : undefined;
				if (removed) candidateStore.invalidateInputs(session.id, candidate, removed);
			} catch {
				invalid.add(candidate);
			}
			// Once an Actor reserves a candidate, its freshness and compatibility checks
			// are authoritative. Cache invalidation may only retire unclaimed work.
			if (!reservationAvailable(candidate.work.reservation)) continue;
			// Path hints retire pending captures, not sealed results with their own adoption proof.
			if (
				unbounded ||
				(candidate.work.execution.status === "succeeded" &&
					(candidate.work.reservation.kind === "shared" || candidateBranch(candidate)?.validate))
			)
				continue;
			const key = candidate.key;
			if (
				key?.resources.some((resource) =>
					changed.some((path) => resourcePathsOverlap(rootedResource(key, resource), path)),
				)
			)
				invalid.add(candidate);
		}
		const parents = [...invalid].filter((candidate) => reservationAvailable(candidate.work.reservation));
		for (const candidate of candidates)
			if (parents.some((parent) => descendsFrom(candidate, parent))) invalid.add(candidate);
		// The Actor's settlement relaunches what still has a future; relaunched now, work for the decision it settles would only be retired.
		for (const candidate of invalid)
			if (reservationAvailable(candidate.work.reservation)) {
				discardCandidate(session, candidate, cause("freshness", "authoritative_resource_changed"), false);
				session.plan.rearmExecution(candidate.id);
			}
	};
	const pruneActionContexts = (session: Session): void => {
		for (const [id, context] of session.actionContexts)
			if (context.opportunity.state.status !== "matching") releaseActionContext(session, id);
	};
	const beginTurnClosure = (
		state: Turn,
		input: {
			readonly failure: ResolutionCause;
			readonly terminal: boolean;
			readonly notifyHost: boolean;
		},
	): TurnClosure | undefined => {
		if (state.lifecycle !== "active") return undefined;
		settlePredictionFrontier(state);
		state.lifecycle = "closing";
		state.generation.expire(input.failure);
		for (const node of state.session.plan.pending()) {
			if (
				(!node.actionKey || node.execution.status === "preparing") &&
				state.session.actionContexts.get(node.identity.id)?.admissionController.signal.aborted
			)
				failUnlaunchable(state.session, node, cause("source", "generation_expired"));
		}
		for (const preview of state.actorPreviews.values()) {
			abandonActorPreview(state, preview, input.failure);
		}
		state.actorPreviews.clear();
		return { state, terminal: input.terminal, notifyHost: input.notifyHost };
	};
	const clearActorActions = (state: Turn): void => {
		for (const action of state.actorActions) {
			action.finishConsumption();
			state.session.lifecycle.release(action.takeCapture());
		}
		state.actorActions.clear();
	};
	const completeTurnClosure = async (closure: TurnClosure): Promise<void> => {
		const { state } = closure;
		state.lifecycle = "finished";
		if (closure.notifyHost)
			await subscriptions.publish("turn_finished", {
				startInput: state.startInput,
				settings: state.settings,
				terminal: closure.terminal,
			});
		state.session.turns.delete(state.turnID);
		clearActorActions(state);
	};
	const closeSessionState = async (
		session: Session,
		mode: SessionClosureMode,
		turnID = "",
	): Promise<void> => {
		const terminal = mode === "terminal";
		if (terminal && !session.timeline && session.turns.size === 0) return;
		const failure = cause("control", terminal ? "terminal_turn" : "session_disposed");
		const closures = [...session.turns.values()].flatMap((state) => {
			const closure = beginTurnClosure(state, {
				failure,
				terminal,
				notifyHost: terminal,
			});
			return closure ? [closure] : [];
		});
		releaseAllSourceSlots(session, failure);
		const planFailure = terminal ? cause("control", "session_terminal") : failure;
		for (const node of session.plan.unsettled()) settleUnobserved(session, node, planFailure);
		session.plan.clear();
		await session.sourceTasks.drain();
		await session.effects.flush();
		for (const closure of closures) await completeTurnClosure(closure);
		for (const candidate of candidateStore.values(session.id)) {
			if (!terminal || candidate.work.execution.status !== "succeeded")
				discardCandidate(session, candidate, planFailure);
		}
		if (terminal) trimResults(session, session.settings);
		if (terminal) {
			queueTaskEvent(session, turnID, performance.now());
		} else {
			session.timeline = undefined;
		}
		await session.effects.flush();
		if (terminal || mode === "disposed")
			await subscriptions.publish("session_closed", {
				sessionID: session.id,
				terminal,
			});
		pruneActionContexts(session);
		if (!terminal) await session.lifecycle.drain();
	};
	/** Called only inside the owning session's lifecycle lane, including replacement on start. */
	const closeTurn = async (state: Turn): Promise<void> => {
		const closure = beginTurnClosure(state, {
			failure: cause("control", "turn_finished"),
			terminal: false,
			notifyHost: true,
		});
		if (!closure) return;
		await state.session.effects.flush();
		await completeTurnClosure(closure);
		await state.session.effects.flush();
	};
	const finishTurn = async (input: FinishInput): Promise<void> => {
		const session = sessionStates.get(input.sessionID);
		if (!session) return;
		await session.lifecycle.run(() => {
			if (input.terminal === true) return closeSessionState(session, "terminal", input.turnID);
			const state = session.turns.get(input.turnID);
			return state && closeTurn(state);
		});
	};
	const stopDisabledSources = (session: Session): void => {
		const failure = cause("control", "source_disabled");
		const settings = session.settings;
		for (const slot of [...session.sourceSlots])
			if (!sourceEnabled(sourcesByID.get(slot.request.source), settings))
				releaseSourceSlot(session, slot, failure);
		for (const node of session.plan.pending()) {
			if (sourceEnabled(sourcesByID.get(node.source), settings)) continue;
			const candidate =
				"candidateID" in node.execution && candidateStore.get(session.id, node.execution.candidateID);
			// Already-produced work keeps its attribution and matching opportunity until the Actor decides.
			if (candidate && ["running", "succeeded"].includes(candidate.work.execution.status)) continue;
			settleUnobserved(session, node, failure);
		}
	};
	const applySettings = (session: Session, settings: SpeculativeActionSettings): void => {
		session.settings = settings;
		for (const turn of session.turns.values()) turn.settings = settings;
		stopDisabledSources(session);
		if (!settings.enabled || settings.reuseEnabled === false) {
			for (const turn of session.turns.values())
				for (const preview of turn.actorPreviews.values())
					abandonActorPreview(turn, preview, cause("control", "preparation_disabled"));
			if (settings.reuseEnabled === false)
				for (const node of session.plan.pending())
					if (node.execution.status !== "running" && node.execution.status !== "succeeded")
						settleUnobserved(session, node, cause("control", "reuse_disabled"));
		}
		trimResults(session, settings);
	};
	const settingsChanged = async (input?: SpeculativeActionSettings): Promise<void> => {
		const settings = input ?? (await adapter.settings());
		await Promise.all(
			[...sessionStates.values()].map((session) =>
				session.lifecycle.run(() => applySettings(session, settings)),
			),
		);
	};
	const disposeSession = async (sessionID: SessionID): Promise<void> => {
		const session = sessionStates.get(sessionID);
		if (!session) return;
		await session.lifecycle.close(async () => {
			await closeSessionState(session, "disposed");
			await session.effects.close();
			await session.events.close({ drain: false });
			sessionStates.delete(sessionID);
			if (!needsResourceSamples()) scheduler.close();
		});
	};
	const dispose = async (): Promise<void> => {
		await Promise.all([...sessionStates.keys()].map((sessionID) => disposeSession(sessionID)));
	};
	const cacheSnapshot = (session: Session, settings: SpeculativeActionSettings): SpeculativeCacheSnapshot => {
		const candidates = candidateStore.values(session.id);
		const segments = candidateStore.snapshot(
			session.id,
			(candidate) => candidate.work.reservation.kind === "shared",
		);
		const resultEntries = segments.coldEntries + segments.hotEntries;
		let exclusiveCandidates = 0;
		let branchEntries = 0;
		let branchBytes = 0;
		for (const candidate of candidates) {
			if (candidate.work.reservation.kind !== "exclusive") continue;
			exclusiveCandidates++;
			if (candidate.work.execution.status === "succeeded") {
				branchEntries++;
				branchBytes += candidate.estimatedBytes;
			}
		}
		return {
			cacheCapacity: settings.resourceCacheMaxEntries,
			cacheByteCapacity: cacheByteLimit(settings),
			cacheCold: segments.coldEntries,
			cacheHot: segments.hotEntries,
			inFlightJobs: candidates.length - resultEntries - branchEntries,
			resultEntries,
			resultBytes: segments.coldBytes + segments.hotBytes,
			branchEntries,
			branchBytes,
			exclusiveCandidates,
			sharedCandidates: candidates.length - exclusiveCandidates,
		};
	};
	/** Capture at the state transition; policy callbacks may delay delivery but cannot change the snapshot. */
	const eventEnvelope = (session: Session, turnID: string, settings: SpeculativeActionSettings) => ({
		sessionID: session.id,
		turnID,
		timestamp: performance.timeOrigin + performance.now(),
		cache: cacheSnapshot(session, settings),
	});
	const queueTaskEvent = (session: Session, turnID: string, completedAt: number): void => {
		const timeline = session.timeline;
		if (!timeline) return;
		session.timeline = undefined;
		if (adapter.onEvent)
			session.events.enqueue({
				type: "task",
				...eventEnvelope(session, turnID, session.settings),
				timing: timeline.measure(completedAt),
			});
	};
	const queueSourceRequestEvent = (
		session: Session,
		turnID: string,
		settings: SpeculativeActionSettings,
		result: SettledSourceRequest,
	): void => {
		if (adapter.onEvent)
			session.events.enqueue({
				type: "source_request",
				...eventEnvelope(session, turnID, settings),
				request: {
					request: result.request,
					durationMs: result.durationMs,
					settlement: result.settlement,
					...(result.draftTokens ? { draftTokens: result.draftTokens } : {}),
				},
				totalDraftTokens: session.tokenTotal,
			});
	};
	const queueCandidateEvent = (session: Session, candidate: Candidate): void => {
		if (!adapter.onEvent) return;
		const execution = candidate.work.execution;
		if (execution.status === "queued") return;
		const state =
			execution.status === "running"
				? { status: "running" as const }
				: execution.status === "succeeded"
					? { status: "succeeded" as const, executionMs: execution.executionMs }
					: { ...execution };
		const descriptor = candidateEventDescriptor(candidate);
		session.events.enqueue({
			type: "candidate",
			...eventEnvelope(session, candidate.owner.startInput.turnID, candidate.owner.settings),
			candidate: descriptor,
			state,
		});
	};
	return {
		subscribe: (handlers) => subscriptions.subscribe(handlers),
		reservePlanRevision: (sessionID, proposalID, minimum) =>
			sessionStates.get(sessionID)?.plan.reserveRevision(proposalID, minimum) ?? minimum,
		submitPlan: async (input, updates) => {
			const state = sessionStates.get(input.sessionID)?.turns.get(input.turnID);
			if (!state || state.startInput !== input || !actorTurnActive(state) || !state.generation.active) return;
			const target = state.decisionSequence + 1;
			if (target <= state.session.decisionSequence) return;
			for (const update of asUpdates(updates)) {
				const source = sourcesByID.get(update.source);
				if (!source || !sourceEnabled(source, state.session.settings)) continue;
				const slot = claimSourceSlot(
					state.session,
					source.id,
					state.turnID,
					target,
					1,
					"observation",
					true,
					state.signal,
				);
				if (slot)
					void state.session.sourceTasks.track(
						admitUpdates({ ...turnContext(state), session: state.session, slot }, source, update).finally(
							() => releaseSourceRequest(state.session, slot),
						),
					);
			}
		},
		trackActorTool: async (sessionID, execute) => {
			const issuedAt = performance.now();
			const finish = sessionStates.get(sessionID)?.timeline?.startToolWait(issuedAt);
			try {
				return await execute(issuedAt);
			} finally {
				finish?.(performance.now());
			}
		},
		startTurn,
		previewActorTool,
		previewActorCall,
		prepareActorCall: (input, signal, issuedAt) => {
			const task = prepareActorCall(input, signal, issuedAt);
			return sessionStates.get(input.sessionID)?.lifecycle.track(task) ?? task;
		},
		finishTurn,
		settingsChanged,
		disposeSession,
		dispose,
	};
}
export { diagnosticAction, diagnosticJson, redactDiagnostics } from "./settlement.ts";
export type * from "./settlement.ts";
