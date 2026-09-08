/**
 * Serializes lifecycle mutations for one reusable runtime session.
 *
 * Normal operations remain reusable after they settle. `close` is different: it seals the lane
 * synchronously, runs exactly one final operation after already-admitted work, and makes later
 * callers join that same completion instead of starting a second teardown.
 */
export class RuntimeLifecycleLane {
	private tail: Promise<void> = Promise.resolve();
	private closeTask?: Promise<void>;
	private sealedValue = false;
	private readonly work = new Set<Promise<unknown>>();
	private readonly released = new WeakMap<object, Promise<void>>();

	get sealed(): boolean {
		return this.sealedValue;
	}

	run(operation: () => void | Promise<void>): Promise<void> {
		if (this.sealedValue) return this.closeTask ?? this.tail;
		return this.enqueue(operation);
	}

	close(operation: () => void | Promise<void>): Promise<void> {
		if (this.closeTask) return this.closeTask;
		this.sealedValue = true;
		this.closeTask = this.enqueue(async () => {
			try { await operation(); } finally { await this.drain(); }
		});
		return this.closeTask;
	}

	/** New borrowers require an open owner; tracked continuations may still drain after sealing. */
	admit<Value>(operation: () => Promise<Value>): Promise<Value> {
		if (this.sealedValue) return Promise.reject(new Error("execution lifetime is closed"));
		return this.track(Promise.resolve().then(operation));
	}

	track<Value>(task: Promise<Value>): Promise<Value> {
		this.work.add(task);
		void task.finally(() => this.work.delete(task)).catch(() => {});
		return task;
	}

	release(resource?: { readonly dispose: () => void | Promise<void> }): Promise<void> {
		if (!resource) return Promise.resolve();
		const existing = this.released.get(resource);
		if (existing) return existing;
		const task = this.track(Promise.resolve().then(async () => {
			try { await resource.dispose(); } catch { /* Cleanup cannot replace authoritative settlement. */ }
		}));
		this.released.set(resource, task);
		return task;
	}

	async drain(): Promise<void> {
		while (this.work.size) await Promise.allSettled(this.work);
	}

	private enqueue(operation: () => void | Promise<void>): Promise<void> {
		const task = this.tail.then(operation, operation);
		this.tail = task.catch(() => {
			// A failed lifecycle callback is visible to its caller but cannot poison later cleanup.
		});
		return task;
	}
}
