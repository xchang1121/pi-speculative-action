import { snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import type { ProcessProvenanceCertificate, Sha256Digest } from "./provenance-certificate.ts";
import { immutableSnapshot, isImmutableSnapshot, stableEqual } from "./stable-json.ts";
import { TimelineInterval } from "./task-timing.ts";

/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private state: "available" | "partial" | "whole" = "available";
	private readonly observer?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
	private readonly scopeOwner?: WeakRef<(scope: ExecutionScope) => boolean>;

	constructor(observer?: (adoption: ExecutionOperationAdoption) => void, acceptScope?: (scope: ExecutionScope) => boolean) {
		if (observer) this.observer = new WeakRef(observer);
		if (acceptScope) this.scopeOwner = new WeakRef(acceptScope);
	}

	/** A live plan consumer may own this one-shot computation beyond its production turn. */
	acceptsScope(producer: ExecutionScope | undefined, consumer: ExecutionScope | undefined): boolean {
		if (!producer || !consumer || producer.sessionID !== consumer.sessionID) return false;
		return sameScope(producer, consumer) || this.scopeOwner?.deref()?.(consumer) === true;
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
	/** A running owner's bounded negative lookup; false never authorizes adoption. */
	readonly inputsChanged?: () => Promise<boolean>;
}

/** In-memory capability for another isolated execution, never a proof of result equivalence. */
export interface ProcessExecutionBinding {
	readonly key: Sha256Digest;
	readonly executionMs: number;
	readonly scope: ExecutionScope;
	readonly available: boolean;
}

type HandoffState =
	| { readonly status: "running"; inputsChanged?: () => Promise<boolean> }
	| { readonly status: "completed" | "retained"; readonly candidate?: ProcessProvenanceCertificate };

interface HandoffRecord extends ProcessHandoff {
	readonly key: Sha256Digest;
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
			readonly waitForRunning: (handoff: ProcessHandoff) => Promise<"completed" | "miss" | "rejected">;
	  }
);

/** Owns process evidence selection and the scope of one-shot transfers. */
export class ProcessHandoffRegistry<Invocation = never> {
	private readonly byKey = new Map<Sha256Digest, Map<ProcessHandoff, HandoffRecord>>();
	private readonly invocations = new WeakMap<ProcessExecutionBinding, { readonly value: Invocation; readonly bytes: number }>();
	private maxCompleted: number;
	private maxBindingBytes: number;
	private bindingBytes = 0;
	private completedCount = 0;
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
		const record = this.byKey.get(key)?.get(handoff);
		if (!record || record.state.status === "running" || record.state.candidate?.weakKey !== key ||
			!record.state.candidate.dependencyCertificate.complete) return;
		return this.retainBinding(key, record, invocation, record.state.candidate.result.observedProcessMs ?? 0);
	}

	/** A completed native launch teaches preparation only; it has no result or adoption authority. */
	observe(key: Sha256Digest, executablePath: string, scope: ExecutionScope, invocation: Invocation, executionMs: number): ProcessExecutionBinding | undefined {
		if (this.disposed || !Number.isFinite(executionMs) || executionMs < 0) return;
		const record = this.reserve(key, executablePath, new ProcessHandoffOwnership(), snapshotExecutionScope(scope));
		record.state = { status: "retained" };
		this.completedCount++;
		record.settle();
		let binding: ProcessExecutionBinding | undefined;
		try { return binding = this.retainBinding(key, record, invocation, executionMs); }
		finally { if (!binding) this.remove(record); }
	}

	private retainBinding(key: Sha256Digest, record: HandoffRecord, invocation: Invocation, executionMs: number): ProcessExecutionBinding | undefined {
		if (!record.scope || record.binding || this.maxBindingBytes <= 0) return;
		const value = immutableSnapshot(invocation);
		if (!isImmutableSnapshot(value)) return;
		const bytes = Buffer.byteLength(JSON.stringify(value));
		if (bytes > this.maxBindingBytes) return;
		// Repeated native learning shares its launch capability; result owners remain distinct.
		if (record.state.status === "retained" && !record.state.candidate) for (const previous of this.byKey.get(key)?.values() ?? []) {
			if (previous !== record && previous.state.status === "retained" && !previous.state.candidate &&
				previous.scope?.sessionID === record.scope.sessionID && previous.executablePath === record.executablePath &&
				previous.binding && stableEqual(this.invocations.get(previous.binding)?.value, value)) {
				this.remove(record); return previous.binding;
			}
		}
		const owner = new WeakRef(this.invocations);
		const binding = Object.freeze({ key, executionMs, scope: record.scope,
			get available(): boolean { return owner.deref()?.has(this) ?? false; } });
		this.invocations.set(binding, { value, bytes });
		record.binding = binding;
		this.bindingBytes += bytes;
		this.trim();
		return this.invocations.has(binding) ? binding : undefined;
	}

	bindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return Object.freeze([...this.records()].flatMap(record => record.binding?.scope.sessionID === scope.sessionID ? [record.binding] : []));
	}

	/** Copying a digest/descriptor cannot mint a capability. Revocation affects subsequent admissions. */
	resolveBinding(binding: ProcessExecutionBinding, scope: ExecutionScope | undefined): Invocation | undefined {
		return scope?.sessionID === binding.scope.sessionID ? this.invocations.get(binding)?.value : undefined;
	}

	/** Conservative availability hint; scope, ownership and evidence still decide acquisition. */
	get hasResults(): boolean {
		for (const record of this.records()) if (record.state.status !== "retained") return true;
		return false;
	}

	/** Retrieval hint for both running and completed records; it grants no adoption authority. */
	mayHaveExecutable(executablePath: string): boolean {
		for (const record of this.records()) if (record.state.status !== "retained" && record.executablePath === executablePath) return true;
		return false;
	}

	/** The running state owns lookup access; completion or release revokes even a borrowed callback. */
	observeInputs(key: Sha256Digest, handoff: ProcessHandoff, changed: () => Promise<boolean>): () => void {
		const record = this.byKey.get(key)?.get(handoff), state = record?.state;
		if (!record || state?.status !== "running") return () => {};
		const check = () => record.state === state && state.inputsChanged === check ? changed() : Promise.resolve(false);
		state.inputsChanged = check;
		return () => { if (state.inputsChanged === check) state.inputsChanged = undefined; };
	}

	async acquire<Plan extends { readonly certificate: ProcessProvenanceCertificate }>({ scope, ...request }: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		scope = snapshotExecutionScope(scope);
		let joined = false, historyChecked = false;
		const considered = new Map<HandoffRecord, Sha256Digest | undefined>();
		while (true) {
			if (this.disposed) return { kind: "miss", joined };
			const records = [...this.byKey.get(request.key)?.values() ?? []];
			const completed = [...records].reverse().flatMap((record) => {
				const state = record.state;
				if (state.status !== "completed" || !state.candidate || considered.has(record)) return [];
				const oneShot = state.candidate.dependencyCertificate.taints.length > 0;
				return oneShot && (!record.ownership.acceptsScope(record.scope, scope) || record.ownership.wholeClaimed)
					? [] : [{ record, state, candidate: state.candidate, oneShot }];
			});
			if (completed.length) {
				const plan = await request.lookup(completed.map(({ candidate }) => candidate));
				const selected = completed.find(({ candidate }) => candidate === plan?.certificate);
				for (const { record, candidate } of selected ? [selected] : completed) considered.set(record, candidate.id);
				if (plan && selected && selected.record.state === selected.state && this.byKey.get(request.key)?.has(selected.record) &&
					(!selected.oneShot || (selected.record.ownership.acceptsScope(selected.record.scope, scope) && selected.record.ownership.claimChild()))) {
					// Retain bounded launch parameters without granting another transfer of this result.
					if (selected.oneShot) selected.record.state = { ...selected.state, status: "retained" };
					return { kind: "hit", plan, joined, producer: selected.record };
				}
				continue;
			}
			if (!historyChecked) {
				// A failed live attempt also rules out its immutable disk copy for this acquisition.
				const plan = await request.lookup(undefined, new Set([...considered.values()].flatMap(id => id ? [id] : [])));
				if (plan && !this.disposed) return { kind: "hit", plan, joined };
				historyChecked = true;
				continue; // A candidate may have completed while history was being read.
			}
			if (request.role === "producer") return { kind: "work", work: this.reserve(request.key, request.executablePath, request.ownership, scope), joined };
			// Waiting grants no transfer authority; acquisition rechecks the live consumer after validation.
			const running = records.find((record) => !considered.has(record) && record.state.status === "running" && record.ownership.acceptsScope(record.scope, scope)) ??
				records.find((record) => !considered.has(record) && record.state.status === "running" && scope && record.scope?.sessionID === scope.sessionID);
			if (!running) return { kind: "miss", joined };
			const decision = await request.waitForRunning(running);
			if (decision === "rejected") { considered.set(running, undefined); continue; }
			if (decision !== "completed") return { kind: "miss", joined };
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
		const record = this.byKey.get(key)?.get(handoff);
		if (!record || record.state.status !== "running") return false;
		record.state = { status: "completed", ...(candidate ? { candidate } : {}) };
		this.completedCount++;
		if (candidate) record.computation = new TimelineInterval(record.startedAt, performance.now());
		record.settle();
		if (!candidate || !record.scope) this.remove(record);
		else this.trim();
		return true;
	}

	clearCompleted(): void {
		this.trim(0);
	}

	dispose(): void {
		this.disposed = true;
		for (const record of this.records()) {
			this.revokeBinding(record);
			this.complete(record.key, record);
		}
		this.byKey.clear();
		this.completedCount = 0;
	}

	private reserve(key: Sha256Digest, executablePath: string, ownership: ProcessHandoffOwnership, scope?: ExecutionScope): HandoffRecord {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const record: HandoffRecord = {
			key,
			completion,
			executablePath,
			scope,
			ownership,
			startedAt: performance.now(),
			state: { status: "running" },
			get inputsChanged() { return record.state.status === "running" ? record.state.inputsChanged : undefined; },
			settle,
		};
		const records = this.byKey.get(key) ?? new Map();
		records.set(record, record);
		this.byKey.set(key, records);
		return record;
	}

	private *records(): IterableIterator<HandoffRecord> {
		for (const records of this.byKey.values()) yield* records.values();
	}

	private remove(record: HandoffRecord): void {
		this.revokeBinding(record);
		const records = this.byKey.get(record.key);
		if (records?.delete(record) && record.state.status !== "running") this.completedCount--;
		if (!records?.size) this.byKey.delete(record.key);
	}

	private trim(limit = this.maxCompleted): void {
		if (this.completedCount <= limit && this.bindingBytes <= this.maxBindingBytes) return;
		for (const record of this.records()) {
			if (record.state.status !== "running" && this.completedCount > limit) this.remove(record);
			else if (this.bindingBytes > this.maxBindingBytes) this.revokeBinding(record);
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
