import type { ExecutionScope } from "./execution-world.ts";
import type { ProcessProvenanceCertificate, Sha256Digest } from "./provenance-certificate.ts";

export interface ProcessHandoff {
	readonly completion: Promise<void>;
	readonly scope: ExecutionScope | undefined;
	readonly startedAt: number;
}

type HandoffState =
	| { readonly status: "running" }
	| { readonly status: "completed"; readonly candidate?: ProcessProvenanceCertificate }
	| { readonly status: "claimed"; readonly candidate: ProcessProvenanceCertificate };

interface HandoffRecord extends ProcessHandoff {
	state: HandoffState;
	readonly settle: () => void;
}

export type ProcessHandoffAcquisition<Plan> =
	| { readonly kind: "hit"; readonly plan: Plan; readonly joined: boolean }
	| { readonly kind: "work"; readonly work: ProcessHandoff; readonly joined: boolean }
	| { readonly kind: "miss"; readonly joined: boolean };

interface AcquireBase<Plan> {
	readonly key: Sha256Digest;
	readonly scope?: ExecutionScope;
	readonly lookup: (candidate?: ProcessProvenanceCertificate) => Promise<Plan | undefined>;
}

type AcquireOptions<Plan> = AcquireBase<Plan> & (
	| { readonly role: "producer" }
	| {
			readonly role: "actor";
			readonly admitCompleted: (joined: boolean) => boolean;
			readonly waitForRunning: (handoff: ProcessHandoff) => Promise<"completed" | "miss">;
	  }
);

/** Owns every legal transition and selection of same-scope process handoffs. */
export class ProcessHandoffRegistry {
	private readonly byKey = new Map<Sha256Digest, HandoffRecord[]>();
	private maxCompleted: number;
	private disposed = false;

	constructor(maxCompleted: number) {
		this.maxCompleted = maxCompleted;
	}

	configure(maxCompleted: number): void {
		this.maxCompleted = maxCompleted;
		this.trim();
	}

	async acquire<Plan>(options: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		let joined = false;
		while (true) {
			const selected = await this.lookup(options, joined);
			if (selected.kind === "hit") return { ...selected, joined };
			if (selected.kind === "blocked") return this.miss(options, joined);
			if (options.role === "producer" || !selected.running) return this.miss(options, joined);
			if ((await options.waitForRunning(selected.running)) !== "completed") return this.miss(options, joined);
			joined = true;
		}
	}

	/** Makes the candidate visible before persistence begins; persistence outcome never retracts it. */
	async publish(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		candidate: ProcessProvenanceCertificate,
		persist: () => Promise<boolean>,
	): Promise<boolean> {
		if (!this.complete(key, handoff, candidate)) throw new Error("process handoff is no longer running");
		return persist();
	}

	complete(key: Sha256Digest, handoff: ProcessHandoff, candidate?: ProcessProvenanceCertificate): boolean {
		const record = this.record(key, handoff);
		if (!record || record.state.status !== "running") return false;
		record.state = { status: "completed", ...(candidate ? { candidate } : {}) };
		record.settle();
		if (!candidate || !record.scope) this.remove(key, record);
		else this.trim();
		return true;
	}

	clearCompleted(): void {
		for (const [key, records] of this.byKey) {
			const retained = records.filter((record) => record.state.status === "running");
			if (retained.length) this.byKey.set(key, retained);
			else this.byKey.delete(key);
		}
	}

	dispose(): void {
		this.disposed = true;
		for (const [key, records] of this.byKey) {
			for (const record of records) this.complete(key, record);
		}
		this.byKey.clear();
	}

	private async lookup<Plan>(
		options: AcquireOptions<Plan>,
		joined: boolean,
	): Promise<
		| { readonly kind: "hit"; readonly plan: Plan }
		| { readonly kind: "blocked" }
		| { readonly kind: "miss"; readonly running?: ProcessHandoff }
	> {
		const persisted = await options.lookup();
		if (persisted) return this.admitted(options, joined) ? { kind: "hit", plan: persisted } : { kind: "blocked" };
		const records = this.byKey.get(options.key) ?? [];
		for (const record of [...records].reverse()) {
			if (record.state.status !== "completed" || !record.state.candidate || !sameScope(record.scope, options.scope)) continue;
			const candidate = record.state.candidate;
			const plan = await options.lookup(candidate);
			if (!plan || record.state.status !== "completed" || record.state.candidate !== candidate) continue;
			if (!this.admitted(options, joined)) return { kind: "blocked" };
			record.state = { status: "claimed", candidate };
			this.remove(options.key, record);
			return { kind: "hit", plan };
		}
		return {
			kind: "miss",
			running: records.find((record) => record.state.status === "running" && sameScope(record.scope, options.scope)),
		};
	}

	private miss<Plan>(options: AcquireOptions<Plan>, joined: boolean): ProcessHandoffAcquisition<Plan> {
		return options.role === "producer"
			? { kind: "work", work: this.reserve(options.key, options.scope), joined }
			: { kind: "miss", joined };
	}

	private admitted<Plan>(options: AcquireOptions<Plan>, joined: boolean): boolean {
		return options.role === "producer" || options.admitCompleted(joined);
	}

	private reserve(key: Sha256Digest, scope?: ExecutionScope): ProcessHandoff {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => {
			settle = resolve;
		});
		const record: HandoffRecord = {
			completion,
			scope,
			startedAt: performance.now(),
			state: { status: "running" },
			settle,
		};
		const records = this.byKey.get(key) ?? [];
		records.push(record);
		this.byKey.set(key, records);
		return record;
	}

	private record(key: Sha256Digest, handoff: ProcessHandoff): HandoffRecord | undefined {
		return this.byKey.get(key)?.find((record) => record === handoff);
	}

	private remove(key: Sha256Digest, record: HandoffRecord): void {
		const retained = this.byKey.get(key)?.filter((candidate) => candidate !== record) ?? [];
		if (retained.length) this.byKey.set(key, retained);
		else this.byKey.delete(key);
	}

	private trim(): void {
		let excess = [...this.byKey.values()].flat().filter((record) => record.state.status === "completed").length - this.maxCompleted;
		if (excess <= 0) return;
		for (const [key, records] of this.byKey) {
			const retained = records.filter((record) => record.state.status === "running" || excess-- <= 0);
			if (retained.length) this.byKey.set(key, retained);
			else this.byKey.delete(key);
		}
	}
}

function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
