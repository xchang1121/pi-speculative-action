/** One elapsed-time budget covers every candidate, validation and reconstruction for a formal tool call. */
export class ReuseDeadline {
	private readonly controller = new AbortController();
	private readonly timer: ReturnType<typeof setTimeout>;
	readonly signal: AbortSignal;
	constructor(timeoutMs: number, parent?: AbortSignal) {
		this.signal = parent ? AbortSignal.any([parent, this.controller.signal]) : this.controller.signal;
		this.timer = setTimeout(() => this.controller.abort(new Error("reuse_lookup_deadline")), timeoutMs);
	}
	get expired(): boolean { return this.controller.signal.aborted; }
	dispose(): void { clearTimeout(this.timer); }
}

/** Failures never prove freshness. They only skip the same immutable owner/projector until a bounded cooldown. */
export class ProjectionFailureMemory {
	private readonly owners = new WeakMap<object, Map<string, number>>();
	private readonly now: () => number;
	constructor(now = () => performance.now()) { this.now = now; }
	remember(owner: object, projector: string, cooldownMs: number): void {
		let failures = this.owners.get(owner); if (!failures) this.owners.set(owner, failures = new Map());
		for (const [key, until] of failures) if (until <= this.now()) failures.delete(key);
		if (failures.size >= 32) failures.delete(failures.keys().next().value!);
		failures.set(projector, this.now() + cooldownMs);
	}
	blocked(owner: object, projector: string): boolean {
		const failures = this.owners.get(owner), until = failures?.get(projector);
		if (until === undefined) return false;
		if (until <= this.now()) { failures!.delete(projector); return false; }
		return true;
	}
}
