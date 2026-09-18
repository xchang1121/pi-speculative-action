import { nonNegativeCount as finiteLimit, nonNegativeFinite as finiteValue } from "./number-utils.ts";
import { filesystemPathKey } from "./path-utils.ts";
import path from "node:path";
import type { WorldBranch } from "./execution-world.ts";
import {
	type ActionKey, type ActionKeyMatch, type ActionKeyProjector,
	actionKeyMatch, actionKeyProjectionPartitions, ownActionKeyProjector,
} from "./action-semantics.ts";

export interface CandidateStoreEntry {
	readonly id: string;
	readonly key: ActionKey;
	readonly estimatedBytes: number;
}

export interface CandidateLookup<Entry> {
	readonly entry: Entry;
	readonly match: ActionKeyMatch;
}

interface IndexedEntry<Entry> {
	readonly entry: Entry;
	readonly memberships: Array<readonly [Map<string, Set<IndexedEntry<Entry>>>, string]>;
	inputs?: boolean;
	recency: number;
	result?: ResultCacheEvidence;
}

interface IndexedScope<Entry> {
	readonly entries: Map<string, IndexedEntry<Entry>>;
	readonly pending: Set<IndexedEntry<Entry>>;
	readonly exact: Map<string, Set<IndexedEntry<Entry>>>;
	readonly partitions: Map<string, Set<IndexedEntry<Entry>>>;
}

/** One registration survives execution, settlement and cache retention. Keys identify reuse, IDs own work. */
export class CandidateStore<Scope, Entry extends CandidateStoreEntry> {
	private readonly scopes = new Map<Scope, IndexedScope<Entry>>();
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
		return this.get(scope, entry.id) ?? this.add(scope, entry, actionKeyProjectionPartitions(entry.key, this.projectors));
	}

	getOrCreate(
		scope: Scope,
		key: ActionKey,
		create: () => Entry,
		canReuse: (existing: Entry, match: ActionKeyMatch) => boolean = (_entry, match) => match.kind === "exact",
	): CandidateLookup<Entry> & { readonly inserted: boolean } {
		const exact = [...(this.scopes.get(scope)?.exact.get(key.key) ?? [])].reverse();
		for (const indexed of exact) {
			if (this.record(scope, indexed.entry) !== indexed) continue;
			if (canReuse(indexed.entry, { kind: "exact", distance: 0 }) && this.record(scope, indexed.entry) === indexed)
				return { entry: indexed.entry, match: { kind: "exact", distance: 0 }, inserted: false };
		}
		const partitions = actionKeyProjectionPartitions(key, this.projectors);
		for (const { entry: existing, match, indexed } of this.lookupRecords(scope, key, partitions)) {
			if ((match.kind === "exact" && exact.includes(indexed)) || this.record(scope, existing) !== indexed ||
				!canReuse(existing, match) || this.record(scope, existing) !== indexed) continue;
			return { entry: existing, match, inserted: false };
		}
		const entry = create();
		const existing = this.add(scope, entry, partitions);
		return { entry: existing ?? entry, match: { kind: "exact", distance: 0 }, inserted: !existing };
	}

	get(scope: Scope, id: string): Entry | undefined {
		return this.scopes.get(scope)?.entries.get(id)?.entry;
	}

	has(scope: Scope, entry: Entry): boolean {
		return this.get(scope, entry.id) === entry;
	}

	lookup(scope: Scope, action: ActionKey, requireCoverage?: (entry: Entry) => boolean, includeInputs = true): readonly CandidateLookup<Entry>[] {
		return this.scopes.has(scope)
			? this.lookupRecords(scope, action, actionKeyProjectionPartitions(action, this.projectors), requireCoverage, includeInputs).map(({ entry, match }) => ({ entry, match }))
			: [];
	}

	/** The same owned-name index serves dynamically discovered query dependencies. */
	lookupInputs(scope: Scope, resources: readonly string[]): readonly Entry[] {
		return [...this.inputRecords(scope, resources)].sort(([left, a], [right, b]) => a - b || right.recency - left.recency)
			.map(([{ entry }]) => entry);
	}

	/** Backend-confirmed name revocation does not erase the owner's output evidence or other inputs. */
	invalidateInputs(scope: Scope, entry: Entry, resources: readonly string[]): void {
		const indexed = this.record(scope, entry), state = this.scopes.get(scope);
		if (!indexed || !state || !resources.length) return;
		const keys = new Set(resources.flatMap(resource => [inputPartition(resource), inputPartition(resource, true)]));
		this.unindex(indexed, (index, key) => index === state.partitions && keys.has(key));
	}

	/** Sealed queries remain addressable after their reconstruction inputs are revoked. */
	indexView(scope: Scope, entry: Entry, actionKey: string, retained: boolean): void {
		const indexed = this.record(scope, entry), state = this.scopes.get(scope);
		if (!indexed || !state) return;
		const key = `view:${actionKey}`, members = state.partitions.get(key) ?? new Set();
		if (!retained) this.unindex(indexed, (index, name) => index === state.partitions && name === key);
		else if (!members.has(indexed)) {
			members.add(indexed); state.partitions.set(key, members); indexed.memberships.push([state.partitions, key]);
		}
	}

	touch(scope: Scope, entry: Entry): boolean {
		const indexed = this.record(scope, entry), state = this.scopes.get(scope);
		if (!indexed || !state) return false;
		state.entries.delete(entry.id);
		indexed.recency = this.sequence++;
		state.entries.set(entry.id, indexed);
		const exact = state.exact.get(indexed.memberships[0]![1])!;
		exact.delete(indexed);
		exact.add(indexed);
		return true;
	}

	delete(scope: Scope, entry: Entry): boolean {
		const indexed = this.record(scope, entry), state = this.scopes.get(scope);
		if (!indexed || !state) return false;
		state.entries.delete(entry.id);
		state.pending.delete(indexed);
		this.unindex(indexed);
		if (!state.entries.size) this.scopes.delete(scope);
		return true;
	}

	values(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.entries.values() ?? [])].map(({ entry }) => entry);
	}

	allValues(): Entry[] {
		return [...this.scopes.keys()].flatMap((scope) => this.values(scope));
	}

	pending(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.pending ?? [])].map(({ entry }) => entry);
	}

	/** Retention adds evidence to the existing owner; it neither reinserts nor changes identity. */
	settle(scope: Scope, entry: Entry, shared = true, inputs: NonNullable<WorldBranch<unknown>["inputResources"]> = []): void {
		this.insert(scope, entry);
		const indexed = this.record(scope, entry);
		if (!indexed) return;
		this.scopes.get(scope)!.pending.delete(indexed);
		if (shared) indexed.result ??= { segment: "cold", insertedAt: this.now(), actorHits: 0 };
		if (shared && !indexed.inputs && inputs.length) {
			const partitions = this.scopes.get(scope)!.partitions;
			for (const key of new Set(inputs.map(input => inputPartition(input.path, input.descendants)))) {
				const members = partitions.get(key) ?? new Set();
				members.add(indexed); partitions.set(key, members);
				indexed.memberships.push([partitions, key]);
			}
			indexed.inputs = true;
		}
	}

	cached(scope: Scope): Entry[] {
		return [...(this.scopes.get(scope)?.entries.values() ?? [])].filter(({ result }) => result).map(({ entry }) => entry);
	}

	evidenceOf(scope: Scope, entry: Entry): ResultCacheEvidence | undefined {
		const evidence = this.record(scope, entry)?.result;
		return evidence ? { ...evidence } : undefined;
	}

	recordActorHit(scope: Scope, entry: Entry, limits?: ResultCacheLimits): readonly Entry[] {
		const indexed = this.record(scope, entry);
		if (!indexed?.result) return [];
		indexed.result = { ...indexed.result, segment: "hot", actorHits: indexed.result.actorHits + 1, lastActorHitAt: this.now() };
		this.touch(scope, entry);
		if (!limits) return [];
		const fraction = finiteFraction(limits.hotFraction ?? 0.8), capacity = finiteLimit(limits.maxEntries);
		return this.retireExcess(scope, this.cached(scope).filter((item) => this.record(scope, item)!.result!.segment === "hot"), {
			maxEntries: !capacity || !fraction ? 0 : Math.max(1, Math.floor(capacity * fraction)),
			maxBytes: Math.floor(finiteLimit(limits.maxBytes) * fraction),
		}, (record) => { record.result = { ...record.result!, segment: "cold" }; });
	}

	snapshot(scope: Scope): ResultCacheSnapshot {
		const snapshot = { coldEntries: 0, hotEntries: 0, coldBytes: 0, hotBytes: 0 };
		for (const { entry, result } of this.scopes.get(scope)?.entries.values() ?? []) {
			if (result) { snapshot[result.segment === "hot" ? "hotEntries" : "coldEntries"]++;
				snapshot[result.segment === "hot" ? "hotBytes" : "coldBytes"] += entryBytes(entry); }
		}
		return snapshot;
	}

	trim(scope: Scope, limits: ResultCacheLimits, canEvict: (entry: Entry) => boolean = () => true): Entry[] {
		return this.retireExcess(scope, this.cached(scope), limits, ({ entry }) => { this.delete(scope, entry); }, canEvict);
	}

	private record(scope: Scope, entry: Entry): IndexedEntry<Entry> | undefined {
		const record = this.scopes.get(scope)?.entries.get(entry.id);
		return record?.entry === entry ? record : undefined;
	}

	private unindex(indexed: IndexedEntry<Entry>, remove?: (index: Map<string, Set<IndexedEntry<Entry>>>, key: string) => boolean): void {
		let kept = 0;
		for (const membership of indexed.memberships) {
			const [index, key] = membership;
			if (remove && !remove(index, key)) { indexed.memberships[kept++] = membership; continue; }
			const members = index.get(key)!;
			members.delete(indexed);
			if (!members.size) index.delete(key);
		}
		indexed.memberships.length = kept;
	}

	private inputRecords(scope: Scope, resources: readonly string[]) {
		const state = this.scopes.get(scope);
		const inputs = new Map<IndexedEntry<Entry>, number>();
		if (state) for (const resource of resources) {
			let current = path.resolve(resource), depth = 0;
			for (;;) {
				const keys = depth ? [inputPartition(current, true)] : [inputPartition(current), inputPartition(current, true)];
				for (const key of keys) {
					for (const indexed of state.partitions.get(key) ?? []) {
						inputs.set(indexed, Math.min(inputs.get(indexed) ?? depth, depth));
					}
				}
				const parent = path.dirname(current); if (parent === current) break;
				current = parent; depth++;
			}
		}
		return inputs;
	}

	private lookupRecords(scope: Scope, action: ActionKey, partitions: readonly string[], requireCoverage?: (entry: Entry) => boolean, includeInputs = false) {
		const state = this.scopes.get(scope);
		if (!state) return [];
		const inputs = this.inputRecords(scope, includeInputs ? action.resources.flatMap(resource =>
			action.resourceRoot !== undefined || path.isAbsolute(resource) ? [path.resolve(action.resourceRoot ?? "", resource)] : []) : []);
		const views = includeInputs ? state.partitions.get(`view:${action.key}`) : undefined;
		const candidates = new Set([...(state.exact.get(action.key) ?? []), ...inputs.keys(), ...(views ?? [])]);
		for (const key of partitions) for (const indexed of state.partitions.get(key) ?? []) candidates.add(indexed);
		const ranked: (CandidateLookup<Entry> & { readonly indexed: IndexedEntry<Entry> })[] = [];
		for (const indexed of candidates) {
			if (this.record(scope, indexed.entry) !== indexed) continue;
			const coverage = requireCoverage?.(indexed.entry) ?? false;
			const match = actionKeyMatch(indexed.entry.key, action, this.projectors, coverage) ??
				(!coverage && (inputs.has(indexed) || views?.has(indexed))
					? { kind: "inputs" as const, distance: Number.MAX_SAFE_INTEGER - 1024 + Math.min(inputs.get(indexed) ?? 0, 1024) } : undefined);
			if (match) ranked.push({ entry: indexed.entry, match, indexed });
		}
		// Provider callbacks may retire or replace a registration, even with the same object and ID.
		return ranked.filter(({ entry, indexed }) => this.record(scope, entry) === indexed)
			.sort((left, right) => left.match.distance - right.match.distance || right.indexed.recency - left.indexed.recency);
	}

	private add(scope: Scope, entry: Entry, partitions: readonly string[]): Entry | undefined {
		const state: IndexedScope<Entry> = this.scopes.get(scope) ?? { entries: new Map(), pending: new Set(), exact: new Map(), partitions: new Map() };
		const existing = state.entries.get(entry.id);
		if (existing) return existing.entry;
		const memberships = [[state.exact, entry.key.key] as const, ...partitions.map((key) => [state.partitions, key] as const)];
		const indexed: IndexedEntry<Entry> = { entry, memberships, recency: this.sequence++ };
		state.entries.set(entry.id, indexed);
		state.pending.add(indexed);
		for (const [index, key] of memberships) {
			const members = index.get(key) ?? new Set();
			members.add(indexed);
			index.set(key, members);
		}
		this.scopes.set(scope, state);
		return undefined;
	}

	/** Rank once; protected results still occupy the budget, and callbacks cannot retire a replacement. */
	private retireExcess(
		scope: Scope, entries: readonly Entry[], limits: ResultCacheLimits,
		retire: (record: IndexedEntry<Entry>) => void, canRetire: (entry: Entry) => boolean = () => true,
	): Entry[] {
		let count = entries.length, bytes = entries.reduce((total, entry) => total + entryBytes(entry), 0);
		const maxEntries = finiteLimit(limits.maxEntries), maxBytes = finiteLimit(limits.maxBytes);
		const withinBudget = () => count <= maxEntries && bytes <= maxBytes;
		if (withinBudget()) return [];
		const now = this.now(), ranked = [];
		for (const entry of entries) {
			const indexed = this.record(scope, entry), evidence = indexed?.result;
			if (evidence && canRetire(entry)) ranked.push({
				entry, indexed, evidence, hot: Number(evidence.segment === "hot"), value: finiteValue(this.score(entry, { ...evidence }, now)),
			});
		}
		ranked.sort((left, right) => left.hot - right.hot || left.value - right.value);
		const retired: Entry[] = [];
		for (const { entry, indexed, evidence } of ranked) {
			if (withinBudget()) break;
			if (!canRetire(entry) || this.record(scope, entry) !== indexed || indexed?.result !== evidence) continue;
			retired.push(entry); count--; bytes -= entryBytes(entry); retire(indexed);
		}
		return retired;
	}
}

const inputPartition = (resource: string, descendants = false) => `inputs:${descendants ? "tree" : "path"}:` + filesystemPathKey(resource);

export type ResultCacheSegment = "cold" | "hot";

export interface ResultCacheLimits {
	readonly maxEntries: number;
	readonly maxBytes: number;
	/** Maximum hot share; lower-value hot entries return to cold before eviction pressure. */
	readonly hotFraction?: number;
}

export interface SpeculativeCacheValueMetrics {
	readonly executionMs: number;
	readonly expectedValidationMs: number;
	readonly expectedProjectionMs: number;
	readonly bytes: number;
	readonly actorHits: number;
	readonly insertedAt: number;
	readonly lastActorHitAt?: number;
}

const CACHE_HIT_HALF_LIFE_MS = 30 * 60 * 1000;

export function speculativeCacheValue(
	metrics: SpeculativeCacheValueMetrics,
	now = Date.now(),
	halfLifeMs = CACHE_HIT_HALF_LIFE_MS,
): number {
	const reusableWorkMs = Math.max(
		0,
		finiteValue(metrics.executionMs) -
			finiteValue(metrics.expectedValidationMs) -
			finiteValue(metrics.expectedProjectionMs),
	);
	const referenceAt = metrics.actorHits > 0 ? (metrics.lastActorHitAt ?? metrics.insertedAt) : metrics.insertedAt;
	const ageMs = Math.max(0, finiteValue(now - referenceAt));
	const decay = halfLifeMs > 0 ? 2 ** (-ageMs / halfLifeMs) : 0;
	const reuseWeight = metrics.actorHits > 0 ? 1 + finiteValue(metrics.actorHits) * decay : 0.1 * decay;
	return (reuseWeight * reusableWorkMs) / (finiteValue(metrics.bytes) + 4096);
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
	return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0.8;
}

function entryBytes(entry: CandidateStoreEntry): number {
	return Number.isFinite(entry.estimatedBytes) ? Math.max(0, entry.estimatedBytes) : 0;
}
