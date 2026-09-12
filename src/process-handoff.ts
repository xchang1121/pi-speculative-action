import type { ExecutionScope } from "./execution-world.ts";
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
	| { readonly status: "completed"; readonly candidate?: ProcessProvenanceCertificate }
	| { readonly status: "claimed" };

interface HandoffRecord extends ProcessHandoff {
	state: HandoffState;
	readonly ownership: ProcessHandoffOwnership;
	readonly settle: () => void;
}

export type ProcessHandoffAcquisition<Plan> =
	| { readonly kind: "hit"; readonly plan: Plan; readonly joined: boolean }
	| { readonly kind: "work"; readonly work: ProcessHandoff; readonly joined: boolean }
	| { readonly kind: "miss"; readonly joined: boolean };

interface AcquireBase<Plan> {
	readonly key: Sha256Digest;
	readonly scope?: ExecutionScope;
	readonly lookup: (candidates?: readonly ProcessProvenanceCertificate[]) => Promise<Plan | undefined>;
}

type AcquireOptions<Plan> = AcquireBase<Plan> & (
	| { readonly role: "producer"; readonly ownership: ProcessHandoffOwnership }
	| {
			readonly role: "actor";
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

	async acquire<Plan extends { readonly certificate: ProcessProvenanceCertificate }>(options: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		let joined = false, historyChecked = false;
		const considered = new Set<HandoffRecord>();
		while (true) {
			const records = this.byKey.get(options.key) ?? [];
			const completed = [...records].reverse().flatMap((record) => {
				const state = record.state;
				if (state.status !== "completed" || !state.candidate || considered.has(record) || !sameScope(record.scope, options.scope)) return [];
				const oneShot = state.candidate.dependencyCertificate.taints.length > 0;
				return oneShot && record.ownership.wholeClaimed ? [] : [{ record, state, candidate: state.candidate, oneShot }];
			});
			if (completed.length) {
				const plan = await options.lookup(completed.map(({ candidate }) => candidate));
				const selected = completed.find(({ candidate }) => candidate === plan?.certificate);
				for (const { record } of selected ? [selected] : completed) considered.add(record);
				if (plan && selected && selected.record.state === selected.state &&
					(!selected.oneShot || selected.record.ownership.claimChild())) {
					selected.record.state = { status: "claimed" };
					this.remove(options.key, selected.record);
					return { kind: "hit", plan, joined };
				}
				continue;
			}
			if (!historyChecked) {
				const plan = await options.lookup();
				if (plan) return { kind: "hit", plan, joined };
				historyChecked = true;
				continue; // A candidate may have completed while history was being read.
			}
			if (options.role === "producer") return { kind: "work", work: this.reserve(options.key, options.ownership, options.scope), joined };
			const running = records.find((record) => record.state.status === "running" && sameScope(record.scope, options.scope));
			if (!running || (await options.waitForRunning(running)) !== "completed") return { kind: "miss", joined };
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

	private reserve(key: Sha256Digest, ownership: ProcessHandoffOwnership, scope?: ExecutionScope): ProcessHandoff {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const record: HandoffRecord = {
			completion,
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

function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
