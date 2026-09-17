export type PostSettlementFailureHandler = (error: unknown) => void;

export interface BoundedEventQueueSnapshot {
	readonly capacity: number;
	readonly pending: number;
	readonly dropped: number;
	readonly oldestPendingMs: number;
}

/** Ordered, failure-isolated work that must never extend the Actor settlement barrier. */
export class PostSettlementQueue {
	private readonly queue: BoundedEventQueue<() => void | Promise<void>>;

	constructor(onFailure: PostSettlementFailureHandler = () => {}) {
		// Learning is lossless; defer invocation so enqueue never enters a producer callback.
		this.queue = new BoundedEventQueue(Number.MAX_SAFE_INTEGER, task => Promise.resolve().then(task), onFailure);
	}

	enqueue(task: () => void | Promise<void>): boolean {
		return this.queue.enqueue(task);
	}

	flush(): Promise<void> {
		return this.queue.flush();
	}

	close(): Promise<void> {
		return this.queue.close();
	}
}

interface PendingEvent<Event> {
	readonly value: Event;
	readonly enqueuedAt: number;
}

/**
 * Failure-isolated, bounded delivery for optional observers.
 *
 * Optional observers use a fixed capacity so stalled delivery cannot retain an unbounded backlog.
 * PostSettlementQueue shares the drain machinery with a lossless capacity for source learning.
 */
export class BoundedEventQueue<Event> {
	private readonly capacityValue: number;
	private readonly deliver: (event: Event) => void | Promise<void>;
	private readonly onFailure: PostSettlementFailureHandler;
	private readonly pending = new Set<PendingEvent<Event>>();
	private readonly idleWaiters = new Set<() => void>();
	private active = false;
	private closed = false;
	private droppedValue = 0;

	constructor(
		capacity: number,
		deliver: (event: Event) => void | Promise<void>,
		onFailure: PostSettlementFailureHandler = () => {},
	) {
		if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error("event queue capacity must be positive");
		this.capacityValue = capacity;
		this.deliver = deliver;
		this.onFailure = onFailure;
	}

	enqueue(event: Event): boolean {
		if (this.closed) return false;
		if (this.pending.size >= this.capacityValue) {
			this.droppedValue++;
			return false;
		}
		this.pending.add({ value: event, enqueuedAt: performance.now() });
		if (!this.active) void this.drain();
		return true;
	}

	snapshot(now = performance.now()): BoundedEventQueueSnapshot {
		const oldest = this.pending.values().next().value?.enqueuedAt;
		return Object.freeze({
			capacity: this.capacityValue,
			pending: this.pending.size,
			dropped: this.droppedValue,
			oldestPendingMs: oldest === undefined ? 0 : Math.max(0, now - oldest),
		});
	}

	async flush(): Promise<void> {
		if (!this.pending.size) return;
		await new Promise<void>((resolve) => { this.idleWaiters.add(resolve); });
	}

	/** Seal delivery; callers may detach from an already bounded backlog during runtime disposal. */
	async close(options: { readonly drain?: boolean } = {}): Promise<void> {
		this.closed = true;
		if (options.drain !== false) await this.flush();
	}

	private async drain(): Promise<void> {
		this.active = true;
		try {
			// A Set retains insertion order while admitting reentrant work without shifting a backlog.
			for (const next of this.pending) {
				try {
					await this.deliver(next.value);
				} catch (error) {
					try {
						this.onFailure(error);
					} catch {
						// Diagnostics cannot poison later observer delivery.
					}
				}
				this.pending.delete(next);
			}
		} finally {
			// The empty-queue check and release are synchronous; enqueued callbacks drain in the loop.
			this.active = false;
			for (const resolve of this.idleWaiters) resolve();
			this.idleWaiters.clear();
		}
	}
}
