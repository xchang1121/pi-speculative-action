import { snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import type { ProcessProvenanceCertificate, Sha256Digest } from "./provenance-certificate.ts";
import { immutableSnapshot, isImmutableSnapshot } from "./stable-json.ts";
import { TimelineInterval } from "./task-timing.ts";

/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private state: "available" | "partial" | "whole" = "available";
	private readonly observer?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;

	constructor(observer?: (adoption: ExecutionOperationAdoption) => void) {
		if (observer) this.observer = new WeakRef(observer);
	}

	/** Observational only; a retained certificate must not keep an expired runtime alive. */
	adopted(adoption: ExecutionOperationAdoption): void { this.observer?.deref()?.(adoption); }

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
	readonly ownership: ProcessHandoffOwnership;
	readonly binding?: ProcessExecutionBinding;
	readonly computation?: TimelineInterval;
	readonly completion: Promise<void>;
	readonly scope: ExecutionScope | undefined;
	readonly startedAt: number;
}

/** In-memory capability for another isolated execution, never a proof of result equivalence. */
export interface ProcessExecutionBinding {
	readonly key: Sha256Digest;
	readonly certificate: ProcessProvenanceCertificate;
	readonly scope: ExecutionScope;
	readonly available: boolean;
}

type HandoffState =
	| { readonly status: "running" }
	| { readonly status: "completed" | "consumed"; readonly candidate?: ProcessProvenanceCertificate };

interface HandoffRecord extends ProcessHandoff {
	state: HandoffState;
	binding?: ProcessExecutionBinding;
	computation?: TimelineInterval;
	readonly executablePath: string;
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
export class ProcessHandoffRegistry<Invocation = never> {
	private readonly byKey = new Map<Sha256Digest, HandoffRecord[]>();
	private readonly invocations = new WeakMap<ProcessExecutionBinding, { readonly value: Invocation; readonly bytes: number }>();
	private maxCompleted: number;
	private maxBindingBytes: number;
	private bindingBytes = 0;
	private disposed = false;

	constructor(maxCompleted: number, maxBindingBytes = 0) {
		this.maxCompleted = maxCompleted;
		this.maxBindingBytes = maxBindingBytes;
	}

	configure(maxCompleted: number, maxBindingBytes = this.maxBindingBytes): void {
		this.maxCompleted = maxCompleted;
		this.maxBindingBytes = maxBindingBytes;
		this.trim();
	}

	/** Keep secrets only with their existing handoff owner; the returned capability contains no raw arguments. */
	bind(key: Sha256Digest, handoff: ProcessHandoff, invocation: Invocation): ProcessExecutionBinding | undefined {
		const record = this.byKey.get(key)?.find(candidate => candidate === handoff);
		if (!record?.scope || record.binding || record.state.status === "running" || record.state.candidate?.weakKey !== key ||
			!record.state.candidate.dependencyCertificate.complete || this.maxBindingBytes <= 0) return;
		const value = immutableSnapshot(invocation);
		if (!isImmutableSnapshot(value)) return;
		const bytes = Buffer.byteLength(JSON.stringify(value));
		if (bytes > this.maxBindingBytes) return;
		const owner = new WeakRef(this.invocations);
		const binding = Object.freeze({ key, certificate: record.state.candidate, scope: record.scope,
			get available(): boolean { return owner.deref()?.has(this) ?? false; } });
		this.invocations.set(binding, { value, bytes });
		record.binding = binding;
		this.bindingBytes += bytes;
		this.trim();
		return this.invocations.has(binding) ? binding : undefined;
	}

	bindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return Object.freeze([...this.byKey.values()].flatMap(records => records.flatMap(record =>
			record.binding?.scope.sessionID === scope.sessionID ? [record.binding] : [])));
	}

	/** Copying a digest/descriptor cannot mint a capability. Revocation affects subsequent admissions. */
	resolveBinding(binding: ProcessExecutionBinding, scope: ExecutionScope | undefined): Invocation | undefined {
		return scope?.sessionID === binding.scope.sessionID ? this.invocations.get(binding)?.value : undefined;
	}

	/** Conservative availability hint; scope, ownership and evidence still decide acquisition. */
	get hasResults(): boolean {
		for (const records of this.byKey.values()) if (records.some(record => record.state.status !== "consumed")) return true;
		return false;
	}

	/** Retrieval hint for both running and completed records; it grants no adoption authority. */
	mayHaveExecutable(executablePath: string): boolean {
		for (const records of this.byKey.values()) if (records.some(record => record.state.status !== "consumed" && record.executablePath === executablePath)) return true;
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
					// Retain bounded launch parameters without granting another transfer of this result.
					if (selected.oneShot) selected.record.state = { ...selected.state, status: "consumed" };
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
		if (candidate) record.computation = new TimelineInterval(record.startedAt, performance.now());
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
			for (const record of records) {
				this.revokeBinding(record);
				this.complete(key, record);
			}
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
		this.revokeBinding(record);
		const retained = this.byKey.get(key)?.filter((candidate) => candidate !== record) ?? [];
		if (retained.length) this.byKey.set(key, retained);
		else this.byKey.delete(key);
	}

	private trim(limit = this.maxCompleted): void {
		let excess = [...this.byKey.values()].flat().filter((record) => record.state.status !== "running").length - limit;
		if (excess <= 0 && this.bindingBytes <= this.maxBindingBytes) return;
		for (const [key, records] of this.byKey) {
			for (const record of records) {
				if (record.state.status !== "running" && excess-- > 0) this.remove(key, record);
				else if (this.bindingBytes > this.maxBindingBytes) this.revokeBinding(record);
			}
		}
	}

	private revokeBinding(record: HandoffRecord): void {
		if (!record.binding) return;
		this.bindingBytes -= this.invocations.get(record.binding)?.bytes ?? 0;
		this.invocations.delete(record.binding);
		record.binding = undefined;
	}
}

export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
