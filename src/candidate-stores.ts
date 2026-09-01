import {
	type ActionKey,
	type ActionKeyMatch,
	type ActionKeyProjector,
	actionKeyMatch,
	actionKeyProjectionPartitions,
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
	readonly recency: number;
}

interface IndexedScope<Entry> {
	readonly entries: Set<Entry>;
	readonly exact: Map<string, Set<Entry>>;
	readonly partitions: Map<string, Set<Entry>>;
	readonly recency: Map<Entry, number>;
}

/** Scoped action identity, projection lookup, recency, and bounded storage. */
export class ActionStore<Scope, Entry extends ActionStoreEntry> {
	private readonly scopesByID = new Map<Scope, IndexedScope<Entry>>();
	private readonly projectors: readonly ActionKeyProjector[];
	private readonly allowDuplicateExact: boolean;
	private sequence = 0;

	constructor(projectors: readonly ActionKeyProjector[] = [], allowDuplicateExact = false) {
		this.projectors = [...projectors];
		this.allowDuplicateExact = allowDuplicateExact;
	}

	insert(scope: Scope, entry: Entry): Entry | undefined {
		const state = this.ensureScope(scope);
		const existing = last(state.exact.get(entry.key.key));
		if (existing && !this.allowDuplicateExact) return existing;
		this.add(state, entry);
		return undefined;
	}

	insertOrGetCompatible(
		scope: Scope,
		entry: Entry,
		canReuseProjected: (existing: Entry, match: ProjectedActionKeyMatch) => boolean = () => false,
		canReuseExact: (existing: Entry) => boolean = () => true,
	): ActionStoreInsertResult<Entry> {
		const state = this.ensureScope(scope);
		const exact = [...(state.exact.get(entry.key.key) ?? [])].reverse();
		for (const existing of exact) {
			if (!this.allowDuplicateExact || canReuseExact(existing)) {
				return { entry: existing, match: { kind: "exact", distance: 0 }, inserted: false };
			}
		}
		for (const candidate of this.lookupRecords(state, entry.key)) {
			if (candidate.match.kind === "exact") continue;
			if (!canReuseProjected(candidate.entry, candidate.match)) continue;
			return { entry: candidate.entry, match: candidate.match, inserted: false };
		}
		this.add(state, entry);
		return { entry, match: { kind: "exact", distance: 0 }, inserted: true };
	}

	getExact(scope: Scope, action: ActionKey): Entry | undefined {
		return last(this.scopesByID.get(scope)?.exact.get(action.key));
	}

	has(scope: Scope, entry: Entry): boolean {
		return this.scopesByID.get(scope)?.entries.has(entry) === true;
	}

	lookup(scope: Scope, action: ActionKey): readonly ActionStoreLookup<Entry>[] {
		const state = this.scopesByID.get(scope);
		return state ? this.lookupRecords(state, action).map(({ entry, match }) => ({ entry, match })) : [];
	}

	touch(scope: Scope, entry: Entry): boolean {
		const state = this.scopesByID.get(scope);
		if (!state?.entries.has(entry)) return false;
		state.entries.delete(entry);
		state.entries.add(entry);
		const exact = state.exact.get(entry.key.key)!;
		exact.delete(entry);
		exact.add(entry);
		state.recency.set(entry, this.sequence++);
		return true;
	}

	delete(scope: Scope, entry: Entry): boolean {
		const state = this.scopesByID.get(scope);
		if (!state?.entries.delete(entry)) return false;
		const exact = state.exact.get(entry.key.key);
		exact?.delete(entry);
		if (exact?.size === 0) state.exact.delete(entry.key.key);
		state.recency.delete(entry);
		this.removeFromPartitions(state, entry);
		if (state.entries.size === 0) this.scopesByID.delete(scope);
		return true;
	}

	values(scope: Scope): readonly Entry[] {
		return [...(this.scopesByID.get(scope)?.entries ?? [])];
	}

	allValues(): readonly Entry[] {
		return [...this.scopesByID.values()].flatMap((state) => [...state.entries]);
	}

	private lookupRecords(state: IndexedScope<Entry>, action: ActionKey): readonly IndexedActionStoreLookup<Entry>[] {
		const candidates = new Set<Entry>();
		for (const exact of state.exact.get(action.key) ?? []) candidates.add(exact);
		for (const key of actionKeyProjectionPartitions(action, this.projectors)) {
			for (const entry of state.partitions.get(key) ?? []) candidates.add(entry);
		}
		const ranked: IndexedActionStoreLookup<Entry>[] = [];
		for (const entry of candidates) {
			const match = actionKeyMatch(entry.key, action, this.projectors);
			if (!match) continue;
			ranked.push({ entry, match, recency: state.recency.get(entry) ?? 0 });
		}
		ranked.sort((left, right) => left.match.distance - right.match.distance || right.recency - left.recency);
		return ranked;
	}

	private ensureScope(scope: Scope): IndexedScope<Entry> {
		const existing = this.scopesByID.get(scope);
		if (existing) return existing;
		const created: IndexedScope<Entry> = {
			entries: new Set(),
			exact: new Map(),
			partitions: new Map(),
			recency: new Map(),
		};
		this.scopesByID.set(scope, created);
		return created;
	}

	private add(state: IndexedScope<Entry>, entry: Entry): void {
		if (state.entries.has(entry)) return;
		state.entries.add(entry);
		state.recency.set(entry, this.sequence++);
		const exact = state.exact.get(entry.key.key) ?? new Set<Entry>();
		exact.add(entry);
		state.exact.set(entry.key.key, exact);
		this.addToPartitions(state, entry);
	}

	private addToPartitions(state: IndexedScope<Entry>, entry: Entry): void {
		for (const key of actionKeyProjectionPartitions(entry.key, this.projectors)) {
			const partition = state.partitions.get(key) ?? new Set<Entry>();
			partition.add(entry);
			state.partitions.set(key, partition);
		}
	}

	private removeFromPartitions(state: IndexedScope<Entry>, entry: Entry): void {
		for (const key of actionKeyProjectionPartitions(entry.key, this.projectors)) {
			const partition = state.partitions.get(key);
			if (!partition) continue;
			partition.delete(entry);
			if (partition.size === 0) state.partitions.delete(key);
		}
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
		if (!existing) this.placeCold(scope, entry);
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
		this.metadataFor(scope).set(entry, {
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
		const maxEntries = finiteLimit(limits.maxEntries);
		const maxBytes = finiteLimit(limits.maxBytes);
		let entries = this.index.values(scope);
		let bytes = entries.reduce((total, entry) => total + entryBytes(entry), 0);
		const evicted: Entry[] = [];
		while (entries.length > maxEntries || bytes > maxBytes) {
			const victim =
				this.lowestValue(
					scope,
					entries.filter((entry) => this.segmentOf(scope, entry) === "cold" && canEvict(entry)),
				) ??
				this.lowestValue(
					scope,
					entries.filter((entry) => this.segmentOf(scope, entry) === "hot" && canEvict(entry)),
				);
			if (!victim) break;
			bytes -= entryBytes(victim);
			this.delete(scope, victim);
			evicted.push(victim);
			entries = this.index.values(scope);
		}
		return evicted;
	}

	private metadataFor(scope: Scope): Map<Entry, ResultCacheEvidence> {
		const existing = this.metadata.get(scope);
		if (existing) return existing;
		const created = new Map<Entry, ResultCacheEvidence>();
		this.metadata.set(scope, created);
		return created;
	}

	private placeCold(scope: Scope, entry: Entry): void {
		const now = this.now();
		this.metadataFor(scope).set(entry, {
			segment: "cold",
			insertedAt: now,
			actorHits: 0,
		});
	}

	private rebalanceHot(scope: Scope, limits: ResultCacheLimits): readonly Entry[] {
		const fraction = finiteFraction(limits.hotFraction ?? 0.8);
		const entryCapacity = finiteLimit(limits.maxEntries);
		const byteCapacity = finiteLimit(limits.maxBytes);
		const hotEntryLimit =
			entryCapacity === 0 || fraction === 0 ? 0 : Math.max(1, Math.floor(entryCapacity * fraction));
		const hotByteLimit = Math.floor(byteCapacity * fraction);
		const demoted: Entry[] = [];
		const now = this.now();
		while (true) {
			const hotEntries = this.index.values(scope).filter((entry) => this.segmentOf(scope, entry) === "hot");
			const hotBytes = hotEntries.reduce((total, entry) => total + entryBytes(entry), 0);
			if (hotEntries.length <= hotEntryLimit && hotBytes <= hotByteLimit) break;
			const victim = this.lowestValue(scope, hotEntries, now);
			if (!victim) break;
			const current = this.evidenceOf(scope, victim)!;
			this.metadataFor(scope).set(victim, {
				...current,
				segment: "cold",
			});
			demoted.push(victim);
		}
		return demoted;
	}

	private lowestValue(scope: Scope, entries: readonly Entry[], now = this.now()): Entry | undefined {
		return entries.reduce<Entry | undefined>((lowest, entry) => {
			if (!lowest) return entry;
			const evidence = this.evidenceOf(scope, entry);
			const lowestEvidence = this.evidenceOf(scope, lowest);
			if (!evidence) return lowest;
			if (!lowestEvidence) return entry;
			return finiteValue(this.score(entry, evidence, now)) < finiteValue(this.score(lowest, lowestEvidence, now))
				? entry
				: lowest;
		}, undefined);
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
