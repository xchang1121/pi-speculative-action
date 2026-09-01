/** Fixed-entry LRU map used for derived state that is always safe to recompute. */
export class BoundedRecencyMap<Key, Value> {
	private readonly entriesValue = new Map<Key, Value>();
	readonly capacity: number;

	constructor(capacity: number) {
		this.capacity = Math.max(1, Math.floor(capacity));
	}

	get size(): number {
		return this.entriesValue.size;
	}

	get(key: Key): Value | undefined {
		const value = this.entriesValue.get(key);
		if (value === undefined && !this.entriesValue.has(key)) return undefined;
		this.entriesValue.delete(key);
		this.entriesValue.set(key, value as Value);
		return value;
	}

	set(key: Key, value: Value): { readonly key: Key; readonly value: Value } | undefined {
		this.entriesValue.delete(key);
		this.entriesValue.set(key, value);
		if (this.entriesValue.size <= this.capacity) return undefined;
		const oldest = this.entriesValue.entries().next().value as [Key, Value] | undefined;
		if (!oldest) return undefined;
		this.entriesValue.delete(oldest[0]);
		return { key: oldest[0], value: oldest[1] };
	}

	delete(key: Key): boolean {
		return this.entriesValue.delete(key);
	}

	values(): IterableIterator<Value> {
		return this.entriesValue.values();
	}
}
