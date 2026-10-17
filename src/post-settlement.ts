/// <reference lib="es2024.promise" />
import { isSafeCount } from "./number-utils.ts";
export type PostSettlementFailureHandler = (error: unknown) => void;

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
	private readonly pending = new Set<{ readonly value: Event }>();
	private idle?: PromiseWithResolvers<void>;
	private closed = false;
	private droppedValue = 0;

	constructor(
		capacity: number,
		deliver: (event: Event) => void | Promise<void>,
		onFailure: PostSettlementFailureHandler = () => {},
	) {
		if (!isSafeCount(capacity, 1)) throw new Error("event queue capacity must be positive");
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
		this.pending.add({ value: event });
		if (this.pending.size === 1) void this.drain();
		return true;
	}

	snapshot() {
		return Object.freeze({
			capacity: this.capacityValue,
			pending: this.pending.size,
			dropped: this.droppedValue,
		});
	}

	async flush(): Promise<void> {
		if (this.pending.size) await (this.idle ??= Promise.withResolvers<void>()).promise;
	}

	/** Seal delivery; callers may detach from an already bounded backlog during runtime disposal. */
	async close(options: { readonly drain?: boolean } = {}): Promise<void> {
		this.closed = true;
		if (options.drain !== false) await this.flush();
	}

	private async drain(): Promise<void> {
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
			this.idle?.resolve();
			this.idle = undefined;
		}
	}
}

/** Ordered, failure-isolated work that must never extend the Actor settlement barrier. */
export class PostSettlementQueue extends BoundedEventQueue<() => void | Promise<void>> {
	constructor(onFailure: PostSettlementFailureHandler = () => {}) {
		// Learning is lossless; defer invocation so enqueue never enters a producer callback.
		super(Number.MAX_SAFE_INTEGER, task => Promise.resolve().then(task), onFailure);
	}
}
