import { clampProbability, nonNegativeCount } from "./number-utils.ts";

export type PpmCountTrieRow = {
	readonly context: readonly string[];
	readonly counts: Readonly<Record<string, number>>;
	readonly lastSeen: number;
	/** Older snapshots use the context timestamp for every target. */
	readonly targetLastSeen?: Readonly<Record<string, number>>;
};

type TargetCount = { count: number; lastSeen: number };
type ContextCounts = {
	readonly context: readonly string[];
	readonly targets: Map<string, TargetCount>;
	total: number;
	lastSeen: number;
};

/** Bounded suffix evidence with interpolated PPM escape. Only populated contexts own state. */
export class PpmCountTrie {
	private readonly counts = new Map<string, ContextCounts>();
	private readonly order: number;

	constructor(maxOrder: number) {
		this.order = nonNegativeCount(maxOrder);
	}

	get size(): number { return this.counts.size; }

	observe(history: readonly string[], target: string, sequence = 0, halfLife = 0): void {
		if (!target) return;
		const lastSeen = nonNegativeCount(sequence);
		for (const context of this.suffixes(history)) {
			const current = this.context(context);
			// Advance all sufficient statistics before adding the new observation.
			if (halfLife > 0 && lastSeen > current.lastSeen) {
				current.total = 0;
				for (const value of current.targets.values()) {
					value.count = safeCount(decayedCount(value, lastSeen, halfLife));
					value.lastSeen = lastSeen;
					current.total = safeCount(current.total + value.count, Number.MAX_VALUE / 2);
				}
			}
			const previous = current.targets.get(target);
			this.put(current, target, safeCount((previous?.count ?? 0) + 1), Math.max(previous?.lastSeen ?? 0, lastSeen));
		}
	}

	/** Replace one exact context's evidence, without changing any suffix counts. */
	setCount(context: readonly string[], target: string, count: number, lastSeen = 0): void {
		if (target && context.length <= this.order && Number.isFinite(count) && count > 0)
			this.put(this.context(context), target, safeCount(count), nonNegativeCount(lastSeen));
	}

	distribution(history: readonly string[], sequence = 0, halfLife = 0): ReadonlyMap<string, number> {
		const estimates = new Map<string, number>();
		if (!this.counts.get("[]")?.total) return estimates;
		const suffixes = [...this.suffixes(history)].flatMap(context => this.counts.get(JSON.stringify(context)) ?? []);
		// Prefer the shortest deterministic suffix to its equally deterministic extensions.
		const deterministic = suffixes.findIndex((suffix, index) => index > 0 && suffix.targets.size === 1);
		if (deterministic >= 0) suffixes.length = deterministic + 1;
		let remaining = 1;
		for (const suffix of suffixes.reverse()) {
			const weighted = [...suffix.targets].map(([target, value]) => [target, decayedCount(value, sequence, halfLife)] as const);
			const total = weighted.reduce((sum, [, count]) => sum + count, 0);
			const distinct = weighted.filter(([, count]) => count > 0).length;
			if (total <= 0 || distinct <= 0) continue;
			for (const [target, count] of weighted) if (count > 0)
				estimates.set(target, (estimates.get(target) ?? 0) + remaining * (count / (total + distinct)));
			remaining *= distinct / (total + distinct);
		}
		for (const [target, probability] of estimates) estimates.set(target, clampProbability(probability));
		return estimates;
	}

	snapshot(maxContexts = Number.POSITIVE_INFINITY): readonly PpmCountTrieRow[] {
		const limit = Number.isFinite(maxContexts) ? Math.max(1, Math.floor(maxContexts)) : Number.POSITIVE_INFINITY;
		return this.ranked().slice(0, limit).map(([, row]) => ({
			context: [...row.context],
			counts: Object.fromEntries([...row.targets].sort(([left], [right]) => left.localeCompare(right)).map(([target, value]) => [target, value.count])),
			lastSeen: row.lastSeen,
			...([...row.targets.values()].some(value => value.lastSeen !== row.lastSeen)
				? { targetLastSeen: Object.fromEntries([...row.targets].map(([target, value]) => [target, value.lastSeen])) } : {}),
		}));
	}

	restore(rows: readonly unknown[]): void {
		this.counts.clear();
		for (const value of rows) {
			const row = value as Partial<PpmCountTrieRow> | null;
			if (!row || !Array.isArray(row.context) || !row.context.every(token => typeof token === "string") ||
				!row.counts || typeof row.counts !== "object" || Array.isArray(row.counts)) continue;
			for (const [target, count] of Object.entries(row.counts))
				this.setCount(row.context, target, count, typeof row.targetLastSeen?.[target] === "number"
					? row.targetLastSeen[target] : typeof row.lastSeen === "number" ? row.lastSeen : 0);
		}
	}

	trim(maxContexts: number): void {
		const limit = Math.max(1, Math.floor(maxContexts));
		if (this.size <= limit) return;
		if (!Number.isFinite(limit)) { this.restore(this.snapshot(limit)); return; }
		for (const [key] of this.ranked().slice(limit)) this.counts.delete(key);
	}

	private *suffixes(history: readonly string[]) {
		for (let length = 0; length <= Math.min(history.length, this.order); length++)
			yield history.slice(history.length - length);
	}

	private context(context: readonly string[]): ContextCounts {
		const key = JSON.stringify(context);
		let current = this.counts.get(key);
		if (!current) this.counts.set(key, current = { context: [...context], targets: new Map(), total: 0, lastSeen: 0 });
		return current;
	}

	private put(current: ContextCounts, target: string, count: number, lastSeen: number): void {
		const previous = current.targets.get(target);
		current.total = safeCount(current.total + count - (previous?.count ?? 0), Number.MAX_VALUE / 2);
		current.targets.set(target, { count, lastSeen });
		current.lastSeen = Math.max(current.lastSeen, lastSeen);
	}

	private ranked() {
		return [...this.counts].sort(([leftKey, left], [rightKey, right]) =>
			Number(right.context.length === 0) - Number(left.context.length === 0) ||
			right.total - left.total || right.context.length - left.context.length ||
			right.lastSeen - left.lastSeen || leftKey.localeCompare(rightKey));
	}
}

function safeCount(value: number, maximum = Number.MAX_SAFE_INTEGER): number {
	return Math.min(maximum, Math.max(0, Number.isFinite(value) ? value : maximum));
}

function decayedCount(value: TargetCount, sequence: number, halfLife: number): number {
	return !Number.isFinite(sequence) || !Number.isFinite(halfLife) || halfLife <= 0 ? value.count
		: value.count * 2 ** (-Math.max(0, sequence - value.lastSeen) / halfLife);
}
