export type PpmCountTrieRow = {
	readonly context: readonly string[];
	readonly counts: Readonly<Record<string, number>>;
	readonly lastSeen: number;
};

export type PpmProbabilityEstimate = {
	readonly probability: number;
	/** Longest suffix order that contributed evidence for the target. */
	readonly order: number;
	readonly evidence: number;
	readonly escapeMass: number;
};

type TargetCount = {
	count: number;
	lastSeen: number;
};

type CountNode = {
	readonly children: Map<string, CountNode>;
	readonly targets: Map<string, TargetCount>;
	total: number;
	lastSeen: number;
};

/**
 * Bounded-order context count trie with interpolated PPM escape.
 *
 * Context edges are stored newest-event first, so querying every suffix costs
 * O(maxOrder). Counts belong to exact context nodes; observing one transition
 * updates the root and every suffix order exactly once.
 */
export class PpmCountTrie {
	private root = node();
	private populatedContexts = 0;
	private order: number;

	constructor(maxOrder: number) {
		this.order = nonNegativeInteger(maxOrder);
	}

	get maxOrder(): number {
		return this.order;
	}

	get size(): number {
		return this.populatedContexts;
	}

	observe(history: readonly string[], target: string, sequence = 0, halfLife = 0): void {
		if (!target) return;
		const lastSeen = nonNegativeInteger(sequence);
		this.increment(this.root, target, 1, lastSeen, halfLife);
		let current = this.root;
		for (let index = history.length - 1; index >= Math.max(0, history.length - this.order); index--) {
			const token = history[index];
			if (token === undefined) continue;
			const child = current.children.get(token) ?? node();
			current.children.set(token, child);
			current = child;
			this.increment(current, target, 1, lastSeen, halfLife);
		}
	}

	/** Set evidence for one exact context without implicitly changing its suffixes. */
	setCount(context: readonly string[], target: string, count: number, lastSeen = 0): void {
		if (!target || context.length > this.order) return;
		const normalizedCount = positiveCount(count);
		if (normalizedCount === undefined) return;
		let current = this.root;
		for (let index = context.length - 1; index >= 0; index--) {
			const token = context[index];
			if (token === undefined) continue;
			const child = current.children.get(token) ?? node();
			current.children.set(token, child);
			current = child;
		}
		const wasEmpty = current.total === 0;
		const previous = current.targets.get(target)?.count ?? 0;
		current.targets.set(target, { count: normalizedCount, lastSeen: nonNegativeInteger(lastSeen) });
		current.total = safeTotal(current.total + normalizedCount - previous);
		current.lastSeen = Math.max(current.lastSeen, nonNegativeInteger(lastSeen));
		if (wasEmpty && current.total > 0) this.populatedContexts++;
	}

	estimate(
		history: readonly string[],
		target: string,
		sequence = 0,
		halfLife = 0,
	): PpmProbabilityEstimate | undefined {
		return target ? this.distribution(history, sequence, halfLife).get(target) : undefined;
	}

	/** Compute the suffix evidence once for every competing target in this prediction frontier. */
	distribution(history: readonly string[], sequence = 0, halfLife = 0): ReadonlyMap<string, PpmProbabilityEstimate> {
		const estimates = new Map<string, PpmProbabilityEstimate>();
		if (this.root.total <= 0) return estimates;
		const suffixNodes: Array<{ readonly node: CountNode; readonly order: number }> = [{ node: this.root, order: 0 }];
		let current = this.root;
		for (let order = 1; order <= Math.min(history.length, this.order); order++) {
			const token = history[history.length - order];
			if (token === undefined) continue;
			const child = current.children.get(token);
			if (!child) break;
			current = child;
			if (current.total > 0) suffixNodes.push({ node: current, order });
		}
		// PPM*: a shorter deterministic suffix has more evidence than its equally deterministic extensions.
		const deterministic = suffixNodes.findIndex(({ node, order }) => order > 0 && node.targets.size === 1);
		if (deterministic >= 0) suffixNodes.length = deterministic + 1;

		let remaining = 1;
		for (const item of suffixNodes.reverse()) {
			const weighted = [...item.node.targets].map(([target, value]) => [target, decayedCount(value, sequence, halfLife)] as const);
			const total = weighted.reduce((sum, [, count]) => sum + count, 0);
			const distinct = weighted.filter(([, count]) => count > 0).length;
			if (total <= 0 || distinct <= 0) continue;
			const denominator = total + distinct;
			for (const [target, count] of weighted) {
				if (count <= 0) continue;
				const previous = estimates.get(target);
				estimates.set(target, {
					probability: (previous?.probability ?? 0) + remaining * (count / denominator),
					order: previous?.order ?? item.order,
					evidence: previous?.evidence ?? count,
					escapeMass: 0,
				});
			}
			remaining *= distinct / denominator;
		}
		for (const [target, estimate] of estimates) {
			estimates.set(target, { ...estimate, probability: clampProbability(estimate.probability), escapeMass: clampProbability(remaining) });
		}
		return estimates;
	}

	probability(history: readonly string[], target: string, sequence = 0, halfLife = 0): number | undefined {
		return this.estimate(history, target, sequence, halfLife)?.probability;
	}

	snapshot(maxContexts = Number.POSITIVE_INFINITY): readonly PpmCountTrieRow[] {
		const rows: Array<PpmCountTrieRow & { readonly total: number }> = [];
		const visit = (current: CountNode, reverseContext: readonly string[]): void => {
			if (current.total > 0) {
				rows.push({
					context: [...reverseContext].reverse(),
					counts: Object.fromEntries(
						[...current.targets.entries()]
							.sort(([left], [right]) => left.localeCompare(right))
							.map(([target, value]) => [target, value.count]),
					),
					lastSeen: current.lastSeen,
					total: current.total,
				});
			}
			for (const [token, child] of [...current.children.entries()].sort(([left], [right]) =>
				left.localeCompare(right),
			)) {
				visit(child, [...reverseContext, token]);
			}
		};
		visit(this.root, []);
		const root = rows.find((row) => row.context.length === 0);
		const descendants = rows
			.filter((row) => row.context.length > 0)
			.sort(
				(left, right) =>
					right.total - left.total ||
					right.context.length - left.context.length ||
					right.lastSeen - left.lastSeen ||
					contextKey(left.context).localeCompare(contextKey(right.context)),
			);
		const limit = Number.isFinite(maxContexts) ? Math.max(1, Math.floor(maxContexts)) : Number.POSITIVE_INFINITY;
		return [...(root ? [root] : []), ...descendants].slice(0, limit).map(({ total: _, ...row }) => row);
	}

	restore(rows: readonly unknown[]): void {
		this.root = node();
		this.populatedContexts = 0;
		for (const value of rows) {
			const row = countRow(value);
			if (!row || row.context.length > this.order) continue;
			for (const [target, count] of Object.entries(row.counts)) {
				if (typeof count !== "number") continue;
				this.setCount(row.context, target, count, row.lastSeen);
			}
		}
	}

	reconfigure(maxOrder: number, maxContexts: number): void {
		const rows = this.snapshot(maxContexts).filter((row) => row.context.length <= nonNegativeInteger(maxOrder));
		this.order = nonNegativeInteger(maxOrder);
		this.restore(rows);
	}

	trim(maxContexts: number): void {
		const limit = Math.max(1, Math.floor(maxContexts));
		if (this.size <= limit) return;
		if (!Number.isFinite(limit)) {
			this.restore(this.snapshot(limit));
			return;
		}
		const descendants: Array<{
			readonly node: CountNode;
			readonly depth: number;
			readonly key: string;
		}> = [];
		const collect = (current: CountNode, reverseContext: readonly string[]): void => {
			if (current !== this.root && current.total > 0) {
				const context = [...reverseContext].reverse();
				descendants.push({ node: current, depth: context.length, key: contextKey(context) });
			}
			for (const [token, child] of current.children) collect(child, [...reverseContext, token]);
		};
		collect(this.root, []);
		descendants.sort(
			(left, right) =>
				right.node.total - left.node.total ||
				right.depth - left.depth ||
				right.node.lastSeen - left.node.lastSeen ||
				left.key.localeCompare(right.key),
		);
		const retainedCount = Math.max(0, limit - Number(this.root.total > 0));
		const discarded = new Set(descendants.slice(retainedCount).map((item) => item.node));
		const prune = (current: CountNode): void => {
			if (discarded.has(current)) {
				current.targets.clear();
				current.total = 0;
				current.lastSeen = 0;
			} else if (current.total > 0) {
				// Preserve snapshot/restore's target order and shared context timestamp.
				const lastSeen = current.lastSeen;
				const targets = [...current.targets].sort(([left], [right]) => left.localeCompare(right));
				current.targets.clear();
				current.total = 0;
				for (const [target, value] of targets) {
					value.lastSeen = lastSeen;
					current.targets.set(target, value);
					current.total = safeTotal(current.total + value.count);
				}
			}
			for (const [token, child] of current.children) {
				prune(child);
				if (child.total === 0 && child.children.size === 0) current.children.delete(token);
			}
		};
		prune(this.root);
		this.populatedContexts -= discarded.size;
	}

	private increment(current: CountNode, target: string, count: number, lastSeen: number, halfLife: number): void {
		const wasEmpty = current.total === 0;
		// Move every sufficient statistic to the new event time before adding evidence.
		if (halfLife > 0 && lastSeen > current.lastSeen) {
			current.total = 0;
			for (const value of current.targets.values()) {
				value.count = safeCount(decayedCount(value, lastSeen, halfLife));
				value.lastSeen = lastSeen;
				current.total = safeTotal(current.total + value.count);
			}
		}
		const previous = current.targets.get(target);
		const nextCount = safeCount((previous?.count ?? 0) + count);
		current.targets.set(target, {
			count: nextCount,
			lastSeen: Math.max(previous?.lastSeen ?? 0, lastSeen),
		});
		current.total = safeTotal(current.total + nextCount - (previous?.count ?? 0));
		current.lastSeen = Math.max(current.lastSeen, lastSeen);
		if (wasEmpty) this.populatedContexts++;
	}
}

function node(): CountNode {
	return { children: new Map(), targets: new Map(), total: 0, lastSeen: 0 };
}

function nonNegativeInteger(value: number): number {
	return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

function positiveCount(value: number): number | undefined {
	return Number.isFinite(value) && value > 0 ? safeCount(value) : undefined;
}

function safeCount(value: number): number {
	return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Number.isFinite(value) ? value : Number.MAX_SAFE_INTEGER));
}

function safeTotal(value: number): number {
	const maximum = Number.MAX_VALUE / 2;
	return Math.min(maximum, Math.max(0, Number.isFinite(value) ? value : maximum));
}

function clampProbability(value: number): number {
	return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}

function decayedCount(value: TargetCount | undefined, sequence: number, halfLife: number): number {
	if (!value) return 0;
	if (!Number.isFinite(sequence) || !Number.isFinite(halfLife) || halfLife <= 0) return value.count;
	return value.count * 2 ** (-Math.max(0, sequence - value.lastSeen) / halfLife);
}

function validContext(value: unknown): value is readonly string[] {
	return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function countRow(value: unknown): PpmCountTrieRow | undefined {
	if (!value || typeof value !== "object") return undefined;
	const row = value as { context?: unknown; counts?: unknown; lastSeen?: unknown };
	if (!validContext(row.context) || !row.counts || typeof row.counts !== "object" || Array.isArray(row.counts)) {
		return undefined;
	}
	return {
		context: row.context,
		counts: row.counts as Record<string, number>,
		lastSeen: typeof row.lastSeen === "number" ? row.lastSeen : 0,
	};
}

function contextKey(context: readonly string[]): string {
	return JSON.stringify(context);
}
