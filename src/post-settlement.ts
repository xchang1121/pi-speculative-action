/// <reference lib="es2024.promise" />
import { isSafeCount } from "./number-utils.ts";

/**
 * Failure-isolated, bounded delivery for optional observers.
 *
 * Optional observers use a fixed capacity so stalled delivery cannot retain an unbounded backlog.
 * PostSettlementQueue shares the drain machinery with a lossless capacity for source learning.
 */
export class BoundedEventQueue<Event> {
	private readonly capacityValue: number;
	private readonly deliver: (event: Event) => void | Promise<void>;
	private readonly pending = new Set<{ readonly value: Event }>();
	private idle?: PromiseWithResolvers<void>;
	private closed = false;

	constructor(
		capacity: number,
		deliver: (event: Event) => void | Promise<void>,
	) {
		if (!isSafeCount(capacity, 1)) throw new Error("event queue capacity must be positive");
		this.capacityValue = capacity;
		this.deliver = deliver;
	}

	enqueue(event: Event): boolean {
		if (this.closed) return false;
		if (this.pending.size >= this.capacityValue) return false;
		this.pending.add({ value: event });
		if (this.pending.size === 1) void this.drain();
		return true;
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
				} catch { /* Optional observers cannot poison later delivery. */ }
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
	constructor() {
		// Learning is lossless; defer invocation so enqueue never enters a producer callback.
		super(Number.MAX_SAFE_INTEGER, task => Promise.resolve().then(task));
	}
}

export type RuntimeEventHandlers<Events> = { readonly [Type in keyof Events]?: (event: Events[Type]) => void | Promise<void> };

/** Lossless policy subscriptions. The owning session orders delivery outside Actor settlement. */
export class RuntimeSubscriptions<Events> {
	private readonly listeners = new Set<RuntimeEventHandlers<Events>>();

	subscribe(handlers: RuntimeEventHandlers<Events>): () => void {
		const owned = Object.freeze({ ...handlers });
		this.listeners.add(owned);
		return () => { this.listeners.delete(owned); };
	}

	has(type: keyof Events): boolean { return [...this.listeners].some(handlers => handlers[type]); }

	async publish<Type extends keyof Events>(type: Type, event: Events[Type], own: (event: Events[Type]) => Events[Type] = event => event): Promise<void> {
		await Promise.allSettled([...this.listeners].map(async handlers => {
			if (this.listeners.has(handlers)) await handlers[type]?.(own(event));
		}));
	}
}
