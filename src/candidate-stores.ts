import {
	type ActionKey,
	type ActionKeyMatch,
	type ActionKeyProjector,
	actionKeyMatch,
	actionKeyProjectionPartitions,
	ownActionKeyProjector,
	type ProjectedActionKeyMatch,
} from "./action-semantics.ts";

export interface ActionStoreEntry {
	readonly key: ActionKey;
}

export interface SizedActionStoreEntry extends ActionStoreEntry {
	readonly estimatedBytes: number;
}

export interface ActionStoreLookup<Entry> {
	readonly entry: Entry;
	readonly match: ActionKeyMatch;
}

export interface ActionStoreInsertResult<Entry> extends ActionStoreLookup<Entry> {
	readonly inserted: boolean;
}

interface IndexedActionStoreLookup<Entry> extends ActionStoreLookup<Entry> {
	readonly indexed: IndexedEntry<Entry>;
}

interface IndexedEntry<Entry> {
	readonly memberships: ReadonlyArray<readonly [Map<string, Set<Entry>>, string]>;
	recency: number;
}

interface IndexedScope<Entry> {
	readonly entries: Map<Entry, IndexedEntry<Entry>>;
	readonly exact: Map<string, Set<Entry>>;
	readonly partitions: Map<string, Set<Entry>>;
}

/** Scoped action identity, projection lookup, recency, and bounded storage. */
export class ActionStore<Scope, Entry extends ActionStoreEntry> {
	private readonly scopesByID = new Map<Scope, IndexedScope<Entry>>();
	private readonly projectors: readonly ActionKeyProjector[];
	private readonly allowDuplicateExact: boolean;
	private sequence = 0;

	constructor(projectors: readonly ActionKeyProjector[] = [], allowDuplicateExact = false) {
		this.projectors = projectors.map(ownActionKeyProjector);
		this.allowDuplicateExact = allowDuplicateExact;
	}

	insert(scope: Scope, entry: Entry): Entry | undefined {
		if (this.has(scope, entry)) return entry;
		const existing = this.getExact(scope, entry.key);
		if (existing && !this.allowDuplicateExact) return existing;
		return this.add(scope, entry, actionKeyProjectionPartitions(entry.key, this.projectors));
	}

	insertOrGetCompatible(
		scope: Scope,
		entry: Entry,
		canReuseProjected: (existing: Entry, match: ProjectedActionKeyMatch) => boolean = () => false,
		canReuseExact: (existing: Entry) => boolean = () => true,
	): ActionStoreInsertResult<Entry> {
		const state = this.scopesByID.get(scope);
		const exact = [...(state?.exact.get(entry.key.key) ?? [])].reverse();
		for (const existing of exact) {
			const indexed = state!.entries.get(existing);
			if (!indexed) continue;
			if ((!this.allowDuplicateExact || canReuseExact(existing)) && state!.entries.get(existing) === indexed) {
				return { entry: existing, match: { kind: "exact", distance: 0 }, inserted: false };
			}
		}
		const partitions = actionKeyProjectionPartitions(entry.key, this.projectors);
		for (const { entry: existing, match, indexed } of this.lookupRecords(scope, entry.key, partitions)) {
			if (match.kind === "exact" || this.scopesByID.get(scope)?.entries.get(existing) !== indexed) continue;
			if (!canReuseProjected(existing, match) || this.scopesByID.get(scope)?.entries.get(existing) !== indexed) continue;
			return { entry: existing, match, inserted: false };
		}
		const existing = this.add(scope, entry, partitions);
		return { entry: existing ?? entry, match: { kind: "exact", distance: 0 }, inserted: !existing };
	}

	getExact(scope: Scope, action: ActionKey): Entry | undefined {
		return last(this.scopesByID.get(scope)?.exact.get(action.key));
	}

	has(scope: Scope, entry: Entry): boolean {
		return this.scopesByID.get(scope)?.entries.has(entry) === true;
	}

	lookup(scope: Scope, action: ActionKey): readonly ActionStoreLookup<Entry>[] {
		if (!this.scopesByID.has(scope)) return [];
		return this.lookupRecords(scope, action, actionKeyProjectionPartitions(action, this.projectors)).map(
			({ entry, match }) => ({ entry, match }),
		);
	}

	touch(scope: Scope, entry: Entry): boolean {
		const state = this.scopesByID.get(scope);
		const indexed = state?.entries.get(entry);
		if (!state || !indexed) return false;
		state.entries.delete(entry);
		indexed.recency = this.sequence++;
		state.entries.set(entry, indexed);
		const exact = state.exact.get(indexed.memberships[0]![1])!;
		exact.delete(entry);
		exact.add(entry);
		return true;
	}

	delete(scope: Scope, entry: Entry): boolean {
		const state = this.scopesByID.get(scope);
		const indexed = state?.entries.get(entry);
		if (!state || !indexed) return false;
		state.entries.delete(entry);
		for (const [index, key] of indexed.memberships) {
			const members = index.get(key)!;
			members.delete(entry);
			if (members.size === 0) index.delete(key);
		}
		if (state.entries.size === 0) this.scopesByID.delete(scope);
		return true;
	}

	values(scope: Scope): readonly Entry[] {
		return [...(this.scopesByID.get(scope)?.entries.keys() ?? [])];
	}

	allValues(): readonly Entry[] {
		return [...this.scopesByID.values()].flatMap((state) => [...state.entries.keys()]);
	}

	private lookupRecords(
		scope: Scope,
		action: ActionKey,
		partitions: readonly string[],
	): readonly IndexedActionStoreLookup<Entry>[] {
		const state = this.scopesByID.get(scope);
		if (!state) return [];
		const candidates = new Set<Entry>();
		for (const exact of state.exact.get(action.key) ?? []) candidates.add(exact);
		for (const key of partitions) {
			for (const entry of state.partitions.get(key) ?? []) candidates.add(entry);
		}
		const ranked: IndexedActionStoreLookup<Entry>[] = [];
		for (const entry of candidates) {
			const indexed = state.entries.get(entry);
			if (!indexed) continue;
			const match = actionKeyMatch(entry.key, action, this.projectors);
			if (match) ranked.push({ entry, match, indexed });
		}
		// A later provider callback can retire an already-ranked registration, including delete/reinsert of the same entry.
		return ranked
			.filter(({ entry, indexed }) => state.entries.get(entry) === indexed)
			.sort((left, right) =>
				left.match.distance - right.match.distance || right.indexed.recency - left.indexed.recency,
			);
	}

	private add(scope: Scope, entry: Entry, partitions: readonly string[]): Entry | undefined {
		// Resolve the current scope only after provider callbacks; they may have retired or replaced it.
		const state: IndexedScope<Entry> = this.scopesByID.get(scope) ?? {
			entries: new Map(),
			exact: new Map(),
			partitions: new Map(),
		};
		const existing = this.allowDuplicateExact
			? (state.entries.has(entry) ? entry : undefined)
			: last(state.exact.get(entry.key.key));
		if (existing) return existing;
		const memberships = [
			[state.exact, entry.key.key] as const,
			...partitions.map((key) => [state.partitions, key] as const),
		];
		state.entries.set(entry, { memberships, recency: this.sequence++ });
		for (const [index, key] of memberships) {
			const members = index.get(key) ?? new Set<Entry>();
			members.add(entry);
			index.set(key, members);
		}
		this.scopesByID.set(scope, state);
		return undefined;
	}
}

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

export interface ResultCacheLookup<Entry> extends ActionStoreLookup<Entry> {
	readonly evidence: ResultCacheEvidence;
}

export interface ResultCacheSnapshot {
	readonly coldEntries: number;
	readonly hotEntries: number;
	readonly coldBytes: number;
	readonly hotBytes: number;
}

/** Completed shareable results. Exact freshness generations may coexist until validation or retention retires them. */
export class ResultCache<Scope, Entry extends SizedActionStoreEntry> {
	private readonly index: ActionStore<Scope, Entry>;
	private readonly metadata = new Map<Scope, Map<Entry, ResultCacheEvidence>>();
	private readonly score: (entry: Entry, evidence: ResultCacheEvidence, now: number) => number;
	private readonly now: () => number;

	constructor(
		projectors: readonly ActionKeyProjector[] = [],
		score: (entry: Entry, evidence: ResultCacheEvidence, now: number) => number = () => 0,
		now: () => number = Date.now,
	) {
		this.index = new ActionStore(projectors, true);
		this.score = score;
		this.now = now;
	}

	insert(scope: Scope, entry: Entry): Entry | undefined {
		const existing = this.index.insert(scope, entry);
		if (!existing) {
			const metadata = this.metadata.get(scope) ?? new Map<Entry, ResultCacheEvidence>();
			metadata.set(entry, { segment: "cold", insertedAt: this.now(), actorHits: 0 });
			this.metadata.set(scope, metadata);
		}
		return existing;
	}

	lookup(scope: Scope, action: ActionKey): readonly ResultCacheLookup<Entry>[] {
		return this.index.lookup(scope, action).map((item) => ({
			entry: item.entry,
			match: item.match,
			evidence: this.evidenceOf(scope, item.entry)!,
		}));
	}

	recordActorHit(scope: Scope, entry: Entry, limits?: ResultCacheLimits): readonly Entry[] {
		const current = this.evidenceOf(scope, entry);
		if (!current) return [];
		const now = this.now();
		this.metadata.get(scope)!.set(entry, {
			...current,
			segment: "hot",
			actorHits: current.actorHits + 1,
			lastActorHitAt: now,
		});
		this.index.touch(scope, entry);
		return limits ? this.rebalanceHot(scope, limits) : [];
	}

	evidenceOf(scope: Scope, entry: Entry): ResultCacheEvidence | undefined {
		if (!this.index.has(scope, entry)) return undefined;
		const evidence = this.metadata.get(scope)?.get(entry);
		return evidence ? { ...evidence } : undefined;
	}

	segmentOf(scope: Scope, entry: Entry): ResultCacheSegment | undefined {
		return this.evidenceOf(scope, entry)?.segment;
	}

	delete(scope: Scope, entry: Entry): boolean {
		if (!this.index.delete(scope, entry)) return false;
		const metadata = this.metadata.get(scope);
		metadata?.delete(entry);
		if (metadata?.size === 0) this.metadata.delete(scope);
		return true;
	}

	values(scope: Scope): readonly Entry[] {
		return this.index.values(scope);
	}

	allValues(): readonly Entry[] {
		return this.index.allValues();
	}

	snapshot(scope: Scope): ResultCacheSnapshot {
		let coldEntries = 0;
		let hotEntries = 0;
		let coldBytes = 0;
		let hotBytes = 0;
		for (const entry of this.index.values(scope)) {
			const bytes = entryBytes(entry);
			if (this.segmentOf(scope, entry) === "hot") {
				hotEntries++;
				hotBytes += bytes;
			} else {
				coldEntries++;
				coldBytes += bytes;
			}
		}
		return { coldEntries, hotEntries, coldBytes, hotBytes };
	}

	trim(scope: Scope, limits: ResultCacheLimits, canEvict: (entry: Entry) => boolean = () => true): Entry[] {
		return this.retireExcess(scope, this.index.values(scope), limits, (entry) => this.delete(scope, entry), canEvict);
	}

	private rebalanceHot(scope: Scope, limits: ResultCacheLimits): readonly Entry[] {
		const fraction = finiteFraction(limits.hotFraction ?? 0.8);
		const entryCapacity = finiteLimit(limits.maxEntries);
		const byteCapacity = finiteLimit(limits.maxBytes);
		const hotEntryLimit =
			entryCapacity === 0 || fraction === 0 ? 0 : Math.max(1, Math.floor(entryCapacity * fraction));
		const hotByteLimit = Math.floor(byteCapacity * fraction);
		return this.retireExcess(scope,
			this.index.values(scope).filter((entry) => this.segmentOf(scope, entry) === "hot"),
			{ maxEntries: hotEntryLimit, maxBytes: hotByteLimit },
			(entry) => this.metadata.get(scope)!.set(entry, { ...this.evidenceOf(scope, entry)!, segment: "cold" }));
	}

	/** Rank once at one observation time; protected entries still occupy the shared budget. */
	private retireExcess(
		scope: Scope,
		entries: readonly Entry[],
		limits: ResultCacheLimits,
		retire: (entry: Entry) => void,
		canRetire: (entry: Entry) => boolean = () => true,
	): Entry[] {
		let count = entries.length, bytes = entries.reduce((total, entry) => total + entryBytes(entry), 0);
		const maxEntries = finiteLimit(limits.maxEntries), maxBytes = finiteLimit(limits.maxBytes);
		const withinBudget = () => count <= maxEntries && bytes <= maxBytes;
		if (withinBudget()) return [];
		const now = this.now(), ranked = [];
		for (const entry of entries) {
			const evidence = this.metadata.get(scope)?.get(entry);
			if (evidence && canRetire(entry)) ranked.push({
				entry, evidence, hot: Number(evidence.segment === "hot"), value: finiteValue(this.score(entry, { ...evidence }, now)),
			});
		}
		ranked.sort((left, right) => left.hot - right.hot || left.value - right.value);
		const retired: Entry[] = [];
		for (const { entry, evidence } of ranked) {
			if (withinBudget()) break;
			if (!canRetire(entry) || this.metadata.get(scope)?.get(entry) !== evidence) continue;
			retired.push(entry);
			count--;
			bytes -= entryBytes(entry);
			retire(entry);
		}
		return retired;
	}
}

function finiteLimit(value: number): number {
	return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

function finiteFraction(value: number): number {
	return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0.8;
}

function finiteValue(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function entryBytes(entry: SizedActionStoreEntry): number {
	return Number.isFinite(entry.estimatedBytes) ? Math.max(0, entry.estimatedBytes) : 0;
}

function last<T>(values: Iterable<T> | undefined): T | undefined {
	let result: T | undefined;
	for (const value of values ?? []) result = value;
	return result;
}
