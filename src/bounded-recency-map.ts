/** Fixed-entry LRU map used for derived state that is always safe to recompute. */
export class BoundedRecencyMap<Key, Value> extends Map<Key, Value> {
	private readonly capacity: number;
	private readonly evicted?: (value: Value) => void;

	constructor(capacity: number, evicted?: (value: Value) => void) {
		super();
		this.capacity = Math.max(1, Math.floor(capacity));
		this.evicted = evicted;
	}

	get(key: Key): Value | undefined {
		const value = super.get(key);
		if (value === undefined && !this.has(key)) return undefined;
		super.delete(key);
		super.set(key, value as Value);
		return value;
	}

	/** Cache factory results, including undefined; factory errors propagate. */
	getOrCreate(key: Key, create: () => Value): Value {
		const cached = this.get(key);
		if (cached !== undefined || this.has(key)) return cached as Value;
		const value = create();
		this.set(key, value);
		return value;
	}

	set(key: Key, value: Value): this {
		super.delete(key);
		super.set(key, value);
		if (!(this.size <= this.capacity)) {
			const [oldest, removed] = this.entries().next().value!;
			super.delete(oldest);
			this.evicted?.(removed);
		}
		return this;
	}
}
