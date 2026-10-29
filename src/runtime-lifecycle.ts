export type CompletionWaitResult<T> = { readonly status: "completed"; readonly value: T } | { readonly status: "aborted" } | { readonly status: "deadline" };

/** Actor joins supply cancellation only; source production and teardown may impose their own deadline. */
export function waitForCompletion<T>(promise: Promise<T>, signal?: AbortSignal): Promise<Exclude<CompletionWaitResult<T>, { status: "deadline" }>>;
export function waitForCompletion<T>(promise: Promise<T>, signal: AbortSignal | undefined, timeoutMs: number | undefined): Promise<CompletionWaitResult<T>>;
export async function waitForCompletion<T>(promise: Promise<T>, signal?: AbortSignal, timeoutMs?: number): Promise<CompletionWaitResult<T>> {
	if (signal?.aborted) { void promise.catch(() => undefined); return { status: "aborted" }; }
	const bounded = timeoutMs !== undefined && Number.isFinite(timeoutMs);
	if (!signal && !bounded) return { status: "completed", value: await promise };
	return new Promise((resolve, reject) => {
		let settled = false, timer: ReturnType<typeof setTimeout> | undefined;
		const finish = (complete: () => void) => {
			if (settled) return;
			settled = true; clearTimeout(timer); signal?.removeEventListener("abort", aborted); complete();
		};
		const aborted = () => finish(() => resolve({ status: "aborted" }));
		signal?.addEventListener("abort", aborted, { once: true });
		if (bounded) timer = setTimeout(() => finish(() => resolve({ status: "deadline" })), Math.max(0, timeoutMs));
		void promise.then(value => finish(() => resolve({ status: "completed", value })), error => finish(() => reject(error)));
	});
}

/** Tracks owned asynchronous work, including tasks added while earlier work settles. */
export class PendingTasks {
	private readonly work = new Set<Promise<unknown>>();

	track<Value>(task: Promise<Value>): Promise<Value> {
		if (this.work.has(task)) return task;
		this.work.add(task);
		const settled = () => { this.work.delete(task); };
		void task.then(settled, settled);
		return task;
	}

	async drain(): Promise<void> {
		while (this.work.size) await Promise.allSettled(this.work);
	}
}

/**
 * Serializes lifecycle mutations for one reusable runtime session.
 *
 * Normal operations remain reusable after they settle. `close` is different: it seals the lane
 * synchronously, runs exactly one final operation after already-admitted work, and makes later
 * callers join that same completion instead of starting a second teardown.
 */
export class RuntimeLifecycleLane extends PendingTasks {
	private tail: Promise<void> = Promise.resolve();
	private closeTask?: Promise<void>;
	private readonly released = new WeakMap<object, Promise<void>>();
	private owned?: Set<{ readonly dispose: () => void | Promise<void> }> = new Set();

	get sealed(): boolean {
		return this.closeTask !== undefined;
	}

	run(operation: () => void | Promise<void>): Promise<void> {
		return this.closeTask ?? this.enqueue(operation);
	}

	close(operation: () => void | Promise<void>): Promise<void> {
		this.closeTask ??= this.enqueue(async () => {
			try { await operation(); } finally { await this.drain(); }
		});
		return this.closeTask;
	}

	/** New borrowers require an open owner; tracked continuations may still drain after sealing. */
	admit<Value>(operation: () => Promise<Value>): Promise<Value> {
		if (this.sealed) return Promise.reject(new Error("execution lifetime is closed"));
		return this.track(Promise.resolve().then(operation));
	}

	/** Fresh proofs run in order; closing waits for proofs already admitted to this lane. */
	serialize<Value>(operation: () => Value | Promise<Value>): Promise<Value> {
		if (this.sealed) return Promise.reject(new Error("execution lifetime is closed"));
		return this.track(this.enqueue(operation));
	}

	/** Register an owned resource once; admitted work may finish acquiring it during shutdown. */
	own<Resource extends { readonly dispose: () => void | Promise<void> }>(resource: Resource): Resource {
		if (!this.released.has(resource)) {
			if (this.owned) this.owned.add(resource);
			else void this.release(resource);
		}
		return resource;
	}

	/** Drain borrowers, release their resources, then finalize the owner's metadata. */
	dispose(finalize: () => void | Promise<void> = () => {}): Promise<void> {
		return this.close(async () => {
			await this.drain();
			const resources = this.owned; this.owned = undefined;
			await Promise.all([...resources ?? []].map(resource => this.release(resource)));
			await this.drain();
			await finalize();
		});
	}

	release(resource?: { readonly dispose: () => void | Promise<void> }): Promise<void> {
		if (!resource) return Promise.resolve();
		this.owned?.delete(resource);
		const existing = this.released.get(resource);
		if (existing) return existing;
		const task = this.track(Promise.resolve().then(async () => {
			try { await resource.dispose(); } catch { /* Cleanup cannot replace authoritative settlement. */ }
		}));
		this.released.set(resource, task);
		return task;
	}

	private enqueue<Value>(operation: () => Value | Promise<Value>): Promise<Value> {
		const task = this.tail.then(operation, operation);
		this.tail = task.then(() => {}, () => {
			// A failed lifecycle callback is visible to its caller but cannot poison later cleanup.
		});
		return task;
	}
}

/** A shared operation lives while any borrower still needs it. */
export class SharedAbortScope extends AbortController {
	private readonly borrowers = new Set<() => void>();
	acquire(signal?: AbortSignal): () => void {
		const release = () => {
			signal?.removeEventListener("abort", release);
			if (this.borrowers.delete(release) && !this.borrowers.size) this.abort(signal?.reason);
		};
		this.borrowers.add(release);
		signal?.addEventListener("abort", release, { once: true, signal: this.signal });
		if (signal?.aborted) release();
		return release;
	}
}
