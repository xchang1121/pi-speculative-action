export type PostSettlementFailureHandler = (error: unknown) => void;

export interface BoundedEventQueueSnapshot {
	readonly capacity: number;
	readonly pending: number;
	readonly dropped: number;
	readonly oldestPendingMs: number;
}

/** Ordered, failure-isolated work that must never extend the Actor settlement barrier. */
export class PostSettlementQueue {
	private tail: Promise<void> = Promise.resolve();
	private closed = false;
	private readonly onFailure: PostSettlementFailureHandler;

	constructor(onFailure: PostSettlementFailureHandler = () => {}) {
		this.onFailure = onFailure;
	}

	enqueue(task: () => void | Promise<void>): boolean {
		if (this.closed) return false;
		this.tail = this.tail.then(task).catch((error) => {
			try {
				this.onFailure(error);
			} catch {
				// Diagnostics cannot poison the ordered effects chain.
			}
		});
		return true;
	}

	async flush(): Promise<void> {
		while (true) {
			const observed = this.tail;
			await observed;
			if (observed === this.tail) return;
		}
	}

	async close(): Promise<void> {
		this.closed = true;
		await this.flush();
	}
}

interface PendingEvent<Event> {
	readonly value: Event;
	readonly enqueuedAt: number;
}

/**
 * Failure-isolated, bounded delivery for optional observers.
 *
 * Runtime state transitions and source learning must not enter this queue. If an observer stalls,
 * delivery drops new events after the fixed capacity instead of retaining an unbounded Promise
 * chain for the rest of a long-running session.
 */
export class BoundedEventQueue<Event> {
	private readonly capacityValue: number;
	private readonly deliver: (event: Event) => void | Promise<void>;
	private readonly onFailure: PostSettlementFailureHandler;
	private readonly pending: PendingEvent<Event>[] = [];
	private readonly idleWaiters = new Set<() => void>();
	private active?: PendingEvent<Event>;
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
		const active = this.active ? 1 : 0;
		if (active + this.pending.length >= this.capacityValue) {
			this.droppedValue++;
			return false;
		}
		this.pending.push({ value: event, enqueuedAt: performance.now() });
		if (!this.active) void this.drain();
		return true;
	}

	snapshot(now = performance.now()): BoundedEventQueueSnapshot {
		const oldest = this.active?.enqueuedAt ?? this.pending[0]?.enqueuedAt;
		return Object.freeze({
			capacity: this.capacityValue,
			pending: this.pending.length + (this.active ? 1 : 0),
			dropped: this.droppedValue,
			oldestPendingMs: oldest === undefined ? 0 : Math.max(0, now - oldest),
		});
	}

	async flush(): Promise<void> {
		if (!this.active && this.pending.length === 0) return;
		await new Promise<void>((resolve) => { this.idleWaiters.add(resolve); });
	}

	/** Seal delivery; callers may detach from an already bounded backlog during runtime disposal. */
	async close(options: { readonly drain?: boolean } = {}): Promise<void> {
		this.closed = true;
		if (options.drain !== false) await this.flush();
	}

	private async drain(): Promise<void> {
		try {
			while (true) {
				const next = this.pending.shift();
				if (!next) return;
				this.active = next;
				try {
					await this.deliver(next.value);
				} catch (error) {
					try {
						this.onFailure(error);
					} catch {
						// Diagnostics cannot poison later observer delivery.
					}
				}
			}
		} finally {
			// The empty-queue check and release are synchronous; enqueued callbacks drain in the loop.
			this.active = undefined;
			for (const resolve of this.idleWaiters) resolve();
			this.idleWaiters.clear();
		}
	}
}
