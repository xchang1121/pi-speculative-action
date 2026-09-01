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
		this.closeTask = this.enqueue(operation);
		return this.closeTask;
	}

	private enqueue(operation: () => void | Promise<void>): Promise<void> {
		const task = this.tail.then(operation, operation);
		this.tail = task.catch(() => {
			// A failed lifecycle callback is visible to its caller but cannot poison later cleanup.
		});
		return task;
	}
}
