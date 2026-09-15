import { snapshotExecutionScope, type ExecutionScope } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import type { ProcessProvenanceCertificate, Sha256Digest } from "./provenance-certificate.ts";

/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private state: "available" | "partial" | "whole" = "available";

	get wholeClaimed(): boolean { return this.state === "whole"; }

	claimChild(): boolean {
		if (this.wholeClaimed) return false;
		this.state = "partial";
		return true;
	}

	async commit<T>(apply: () => Promise<T>): Promise<T> {
		if (this.state === "partial") throw effectCommitFailure(new Error("process execution was partially consumed"), "recoverable");
		this.state = "whole";
		try { return await apply(); } catch (error) {
			if (error instanceof EffectCommitFailure && error.disposition === "recoverable") this.state = "available";
			throw error;
		}
	}
}

export interface ProcessHandoff {
	readonly completion: Promise<void>;
	readonly scope: ExecutionScope | undefined;
	readonly startedAt: number;
}

type HandoffState =
	| { readonly status: "running" }
	| { readonly status: "completed"; readonly candidate?: ProcessProvenanceCertificate };

interface HandoffRecord extends ProcessHandoff {
	state: HandoffState;
	readonly executablePath: string;
	readonly ownership: ProcessHandoffOwnership;
	readonly settle: () => void;
}

export type ProcessHandoffAcquisition<Plan> =
	| { readonly kind: "hit"; readonly plan: Plan; readonly joined: boolean; readonly producer?: ProcessHandoff }
	| { readonly kind: "work"; readonly work: ProcessHandoff; readonly joined: boolean }
	| { readonly kind: "miss"; readonly joined: boolean };

export type ProcessHandoffLookup<Plan> = (
	candidates?: readonly ProcessProvenanceCertificate[],
	excludedCertificates?: ReadonlySet<Sha256Digest>,
) => Promise<Plan | undefined>;

type AcquireOptions<Plan> = {
	readonly key: Sha256Digest;
	readonly scope?: ExecutionScope;
	readonly lookup: ProcessHandoffLookup<Plan>;
} & (
	| { readonly role: "producer"; readonly ownership: ProcessHandoffOwnership; readonly executablePath: string }
	| {
			readonly role: "actor";
			readonly waitForRunning: (handoff: ProcessHandoff) => Promise<"completed" | "miss">;
	  }
);

/** Owns process evidence selection and the scope of one-shot transfers. */
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

	/** Conservative availability hint; scope, ownership and evidence still decide acquisition. */
	get hasResults(): boolean { return this.byKey.size > 0; }

	/** Retrieval hint for both running and completed records; it grants no adoption authority. */
	mayHaveExecutable(executablePath: string): boolean {
		for (const records of this.byKey.values()) if (records.some(record => record.executablePath === executablePath)) return true;
		return false;
	}

	async acquire<Plan extends { readonly certificate: ProcessProvenanceCertificate }>({ scope, ...request }: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		scope = snapshotExecutionScope(scope);
		let joined = false, historyChecked = false;
		const considered = new Map<HandoffRecord, Sha256Digest>();
		while (true) {
			if (this.disposed) return { kind: "miss", joined };
			const records = this.byKey.get(request.key) ?? [];
			const completed = [...records].reverse().flatMap((record) => {
				const state = record.state;
				if (state.status !== "completed" || !state.candidate || considered.has(record)) return [];
				const oneShot = state.candidate.dependencyCertificate.taints.length > 0;
				return oneShot && (!sameScope(record.scope, scope) || record.ownership.wholeClaimed)
					? [] : [{ record, state, candidate: state.candidate, oneShot }];
			});
			if (completed.length) {
				const plan = await request.lookup(completed.map(({ candidate }) => candidate));
				const selected = completed.find(({ candidate }) => candidate === plan?.certificate);
				for (const { record, candidate } of selected ? [selected] : completed) considered.set(record, candidate.id);
				if (plan && selected && selected.record.state === selected.state && this.byKey.get(request.key)?.includes(selected.record) &&
					(!selected.oneShot || selected.record.ownership.claimChild())) {
					if (selected.oneShot) this.remove(request.key, selected.record);
					return { kind: "hit", plan, joined, producer: selected.record };
				}
				continue;
			}
			if (!historyChecked) {
				// A failed live attempt also rules out its immutable disk copy for this acquisition.
				const plan = await request.lookup(undefined, new Set(considered.values()));
				if (plan && !this.disposed) return { kind: "hit", plan, joined };
				historyChecked = true;
				continue; // A candidate may have completed while history was being read.
			}
			if (request.role === "producer") return { kind: "work", work: this.reserve(request.key, request.executablePath, request.ownership, scope), joined };
			// Waiting grants no transfer authority; only repeatable sealed evidence may cross turns.
			const running = records.find((record) => record.state.status === "running" && sameScope(record.scope, scope)) ??
				records.find((record) => record.state.status === "running" && scope && record.scope?.sessionID === scope.sessionID);
			if (!running || (await request.waitForRunning(running)) !== "completed") return { kind: "miss", joined };
			joined = true;
			historyChecked = false;
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
		const record = this.byKey.get(key)?.find((record) => record === handoff);
		if (!record || record.state.status !== "running") return false;
		record.state = { status: "completed", ...(candidate ? { candidate } : {}) };
		record.settle();
		if (!candidate || !record.scope) this.remove(key, record);
		else this.trim();
		return true;
	}

	clearCompleted(): void {
		this.trim(0);
	}

	dispose(): void {
		this.disposed = true;
		for (const [key, records] of this.byKey) {
			for (const record of records) this.complete(key, record);
		}
		this.byKey.clear();
	}

	private reserve(key: Sha256Digest, executablePath: string, ownership: ProcessHandoffOwnership, scope?: ExecutionScope): ProcessHandoff {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const record: HandoffRecord = {
			completion,
			executablePath,
			scope,
			ownership,
			startedAt: performance.now(),
			state: { status: "running" },
			settle,
		};
		const records = this.byKey.get(key) ?? [];
		records.push(record);
		this.byKey.set(key, records);
		return record;
	}

	private remove(key: Sha256Digest, record: HandoffRecord): void {
		const retained = this.byKey.get(key)?.filter((candidate) => candidate !== record) ?? [];
		if (retained.length) this.byKey.set(key, retained);
		else this.byKey.delete(key);
	}

	private trim(limit = this.maxCompleted): void {
		let excess = [...this.byKey.values()].flat().filter((record) => record.state.status === "completed").length - limit;
		if (excess <= 0) return;
		for (const [key, records] of this.byKey) {
			const retained = records.filter((record) => record.state.status === "running" || excess-- <= 0);
			if (retained.length) this.byKey.set(key, retained);
			else this.byKey.delete(key);
		}
	}
}

export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
