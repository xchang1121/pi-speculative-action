import { snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import { referencedArtifacts, type ProcessProvenanceCertificate, type Sha256Digest } from "./provenance-certificate.ts";
import type { ProvenanceCertificateStore, VerifiedArtifactClosure } from "./reuse-store.ts";
import { immutableSnapshot, isImmutableSnapshot, stableEqual } from "./stable-json.ts";
import { TimelineInterval } from "./task-timing.ts";
import { waitForCompletion } from "./scheduler.ts";

/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private state: "available" | "partial" | "whole" = "available";
	private reservations = 0;
	private transfer?: { readonly apply: WeakRef<() => Promise<unknown>>; readonly result: Promise<unknown> };
	private committing?: Promise<unknown>;
	private readonly observer?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
	private readonly scopeOwner?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean>;

	constructor(observer?: (adoption: ExecutionOperationAdoption) => void, acceptScope?: (scope: ExecutionScope, salvage?: boolean) => boolean) {
		if (observer) this.observer = new WeakRef(observer);
		if (acceptScope) this.scopeOwner = new WeakRef(acceptScope);
	}

	/** A live owner may transfer this computation beyond its production turn; age grants no reuse authority. */
	acceptsScope(producer: ExecutionScope | undefined, consumer: ExecutionScope | undefined, salvage = false): boolean {
		if (!producer || !consumer || producer.sessionID !== consumer.sessionID) return false;
		return sameScope(producer, consumer) || this.scopeOwner?.deref()?.(consumer, salvage) === true;
	}

	/** Observational only; a retained certificate must not keep an expired runtime alive. */
	adopted(adoption: ExecutionOperationAdoption): void { this.observer?.deref()?.(adoption); }

	get wholeClaimed(): boolean { return this.state === "whole"; }
	get pendingWhole(): Promise<unknown> | undefined { return this.committing; }

	claimChild(): ((consumed: boolean) => void) | undefined {
		if (this.wholeClaimed) return;
		this.reservations++;
		let pending = true;
		return consumed => { if (pending) { pending = false; this.reservations--; if (consumed) this.state = "partial"; } };
	}

	async commit<T>(apply: () => Promise<T>): Promise<T> {
		if (this.transfer?.apply.deref() === apply) return this.transfer.result as Promise<T>;
		if (this.state !== "available" || this.reservations) throw effectCommitFailure(new Error(this.state === "partial" ? "process execution was partially consumed" : "process execution was already claimed"), "recoverable");
		this.state = "whole";
		const result = Promise.resolve().then(apply).catch(error => {
			if (error instanceof EffectCommitFailure && error.disposition === "recoverable") { this.state = "available"; this.transfer = undefined; }
			throw error;
		}).finally(() => { this.committing = undefined; });
		this.committing = result;
		this.transfer = { apply: new WeakRef(apply), result };
		return result;
	}
}

export interface ProcessHandoff {
	/** The registry owns production; disposal revokes work as well as its lookup capability. */
	readonly signal: AbortSignal;
	readonly ownership: ProcessHandoffOwnership;
	readonly binding?: ProcessExecutionBinding;
	readonly computation?: TimelineInterval;
	readonly completion: Promise<void>;
	readonly scope: ExecutionScope | undefined;
	readonly startedAt: number;
	/** A running owner's bounded negative lookup; false never authorizes adoption. */
	readonly inputsChanged?: () => Promise<boolean>;
	/** Owned producer may freeze a proved frontier; completion still seals its evidence. */
	readonly suspend?: (signal?: AbortSignal) => Promise<void>;
}

export interface ProcessContinuation { readonly image: Buffer; readonly physicalRoot: string; readonly computation: TimelineInterval; }

/** In-memory capability for another isolated execution, never a proof of result equivalence. */
export interface ProcessExecutionBinding {
	readonly key: Sha256Digest;
	/** Latest completed native observation, or the owned result's execution time. */
	readonly executionMs: number;
	readonly scope: ExecutionScope;
	readonly available: boolean;
}

type HandoffState =
	| { readonly status: "running"; inputsChanged?: () => Promise<boolean>; suspend?: () => Promise<void> }
	| { readonly status: "completed" | "reserved" | "retained"; readonly result?: { readonly candidate: ProcessProvenanceCertificate; readonly artifacts: VerifiedArtifactClosure } };

interface HandoffRecord extends ProcessHandoff {
	borrowers: number;
	readonly controller: AbortController;
	readonly key: Sha256Digest;
	state: HandoffState;
	binding?: ProcessExecutionBinding;
	computation?: TimelineInterval;
	readonly executablePath: string;
	readonly settle: () => void;
	continuation?: ProcessContinuation;
}

export type ProcessHandoffAcquisition<Plan> =
	| { readonly kind: "hit"; readonly plan: Plan; readonly joined: boolean; readonly producer?: ProcessHandoff; readonly continuation?: ProcessContinuation;
		readonly settle?: (consumed: boolean) => void }
	| { readonly kind: "work"; readonly work: ProcessHandoff; readonly joined: boolean }
	| { readonly kind: "miss"; readonly joined: boolean };

export type ProcessHandoffLookup<Plan> = (
	candidates?: readonly ProcessProvenanceCertificate[],
	excludedCertificates?: ReadonlySet<Sha256Digest>,
	artifacts?: ReadonlyMap<Sha256Digest, VerifiedArtifactClosure>,
) => Promise<Plan | undefined>;

type AcquireOptions<Plan> = {
	readonly key: Sha256Digest;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	readonly lookup: ProcessHandoffLookup<Plan>;
} & (
	| { readonly role: "producer"; readonly ownership: ProcessHandoffOwnership; readonly executablePath: string }
	| { readonly role: "actor"; readonly waitForRunning: (handoff: ProcessHandoff, signal: AbortSignal) => Promise<"completed" | "miss" | "rejected">; }
);

/** Owns process evidence selection and the scope of one-shot transfers. */
export class ProcessHandoffRegistry<Invocation = never> {
	private readonly byKey = new Map<Sha256Digest, Map<ProcessHandoff, HandoffRecord>>();
	private readonly waiters = new Map<Sha256Digest, Set<AbortController>>();
	private readonly invocations = new WeakMap<ProcessExecutionBinding, { readonly value: Invocation; readonly bytes: number; executionMs: number }>();
	private readonly store: ProvenanceCertificateStore;
	private maxCompleted: number;
	private maxRetainedBytes: number;
	private retainedBytes = 0;
	private completedCount = 0;
	private disposed = false;

	constructor(store: ProvenanceCertificateStore, maxRetainedBytes: number) { this.store = store; this.maxCompleted = store.limits.maxCertificates; this.maxRetainedBytes = maxRetainedBytes; }

	configure(maxCompleted: number, maxRetainedBytes = this.maxRetainedBytes): void {
		this.maxCompleted = maxCompleted;
		this.maxRetainedBytes = maxRetainedBytes;
		this.trim();
	}

	/** Keep secrets only with their existing handoff owner; the returned capability contains no raw arguments. */
	bind(key: Sha256Digest, handoff: ProcessHandoff, invocation: Invocation): ProcessExecutionBinding | undefined {
		const record = this.byKey.get(key)?.get(handoff);
		if (!record || record.state.status === "running" || record.state.result?.candidate.weakKey !== key || !record.state.result.candidate.dependencyCertificate.complete) return;
		return this.retainBinding(key, record, invocation, record.state.result.candidate.result.observedProcessMs ?? 0);
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
		if (!record.scope || record.binding || this.maxRetainedBytes <= 0) return;
		const value = immutableSnapshot(invocation);
		if (!isImmutableSnapshot(value)) return;
		const bytes = Buffer.byteLength(JSON.stringify(value));
		if (bytes > this.maxRetainedBytes) return;
		// Repeated native learning shares its launch capability; result owners remain distinct.
		if (record.state.status === "retained" && !record.state.result) for (const previous of this.byKey.get(key)?.values() ?? []) {
			if (previous !== record && previous.state.status === "retained" && !previous.state.result &&
				previous.scope?.sessionID === record.scope.sessionID && previous.executablePath === record.executablePath &&
				previous.binding && stableEqual(this.invocations.get(previous.binding)?.value, value)) {
				this.invocations.get(previous.binding)!.executionMs = executionMs;
				this.remove(record); return previous.binding;
			}
		}
		const owner = new WeakRef(this.invocations);
		const binding = Object.freeze({ key, scope: record.scope,
			get executionMs(): number { return owner.deref()?.get(this)?.executionMs ?? 0; },
			get available(): boolean { return owner.deref()?.has(this) ?? false; } });
		this.invocations.set(binding, { value, bytes, executionMs });
		record.binding = binding;
		this.retainedBytes += bytes;
		this.trim();
		return this.invocations.has(binding) ? binding : undefined;
	}

	/** Results of `key` this session produced, newest first: what a run of it could still reuse, never an adoption. */
	results(key: Sha256Digest, scope: ExecutionScope): readonly ProcessProvenanceCertificate[] {
		return [...this.byKey.get(key)?.values() ?? []].reverse().flatMap(record =>
				record.state.status === "completed" && record.state.result && record.scope?.sessionID === scope.sessionID ? [record.state.result.candidate] : []);
	}

	bindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return Object.freeze([...this.records()].flatMap(record => record.binding?.scope.sessionID === scope.sessionID ? [record.binding] : []));
	}

	/** Copying a digest/descriptor cannot mint a capability. Revocation affects subsequent admissions. */
	resolveBinding(binding: ProcessExecutionBinding, scope: ExecutionScope | undefined): Invocation | undefined {
		return binding.available && scope?.sessionID === binding.scope.sessionID ? this.invocations.get(binding)?.value : undefined;
	}

	/** Conservative availability hint; scope, ownership and evidence still decide acquisition. */
	get hasResults(): boolean { for (const record of this.records()) if (record.state.status !== "retained") return true; return false; }
	/** Acquisition can attempt this running work; inputs, timing, scope and commit proofs are still checked at the native boundary. */
	needsContinuationTracking(scope?: ExecutionScope): boolean {
		for (const record of this.records()) if ((record.state.status === "running" || record.continuation) &&
			!record.ownership.wholeClaimed && record.ownership.acceptsScope(record.scope, scope, true)) return true;
		return false;
	}

	hasJoinableWork(ownership: ProcessHandoffOwnership): boolean {
		for (const record of this.records()) if (record.ownership === ownership && !record.signal.aborted &&
			record.state.status === "running" && (record.inputsChanged || record.suspend)) return true;
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

	observeSuspension(key: Sha256Digest, handoff: ProcessHandoff, suspend: NonNullable<ProcessHandoff["suspend"]>): () => void {
		const record = this.byKey.get(key)?.get(handoff), state = record?.state;
		if (!record || state?.status !== "running") return () => {};
		const owned: NonNullable<ProcessHandoff["suspend"]> = signal => record.state === state && state.suspend === owned ? suspend(signal) : Promise.resolve();
		state.suspend = owned;
		return () => { if (state.suspend === owned) state.suspend = undefined; };
	}

	async acquire<Plan extends { readonly certificate: ProcessProvenanceCertificate }>({ scope, ...request }: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		scope = snapshotExecutionScope(scope);
		let joined = false, historyChecked = false;
		const considered = new Map<HandoffRecord, Sha256Digest | undefined>();
		const borrowed = new Set<HandoffRecord>();
		const retain = (record: HandoffRecord) => { if (!borrowed.has(record)) { borrowed.add(record); record.borrowers++; } };
		try {
			while (true) {
				if (this.disposed || request.signal?.aborted) return { kind: "miss", joined };
				const records = [...this.byKey.get(request.key)?.values() ?? []];
				const completed = [...records].reverse().flatMap((record) => {
					const state = record.state;
					if (state.status !== "completed" || !state.result || considered.has(record)) return [];
					const { candidate, artifacts } = state.result, oneShot = candidate.dependencyCertificate.taints.length > 0 || !!candidate.result.continuation;
					return oneShot && (!record.ownership.acceptsScope(record.scope, scope, request.role === "actor") || record.ownership.wholeClaimed)
						? [] : [{ record, state, candidate, artifacts, oneShot }];
				});
				if (completed.length) {
					for (const { record } of completed) retain(record);
					const plan = await request.lookup(completed.map(({ candidate }) => candidate), undefined, new Map(completed.map(({ candidate, artifacts }) => [candidate.id, artifacts])));
					if (request.signal?.aborted) return { kind: "miss", joined };
					const selected = completed.find(({ candidate }) => candidate === plan?.certificate);
					if (!selected) for (const { record, candidate } of completed) considered.set(record, candidate.id);
					if (plan && selected && selected.record.state === selected.state && this.byKey.get(request.key)?.has(selected.record)) {
						const { record, state, oneShot } = selected;
						const claim = oneShot && record.ownership.acceptsScope(record.scope, scope, request.role === "actor") ? record.ownership.claimChild() : undefined;
						if (oneShot && !claim) { considered.set(record, selected.candidate.id); continue; }
						if (oneShot) record.state = { ...state, status: "reserved" };
						record.borrowers++;
						let pending = true;
						return { kind: "hit", plan, joined, producer: record, continuation: record.continuation, settle: consumed => {
							if (!pending) return;
							pending = false; claim?.(consumed); record.borrowers--;
							if (oneShot && this.byKey.get(request.key)?.has(record)) {
								if (consumed) this.releaseResult(record); else record.state = state; this.wake(request.key);
							}
							this.trim();
						} };
					}
					continue;
				}
				if (!historyChecked) {
					// A failed live attempt also rules out its immutable disk copy for this acquisition.
					const plan = await request.lookup(undefined, new Set([...considered.values()].flatMap(id => id ? [id] : [])));
					if (plan && !this.disposed && !request.signal?.aborted) return { kind: "hit", plan, joined };
					historyChecked = true;
					continue; // A candidate may have completed while history was being read.
				}
				if (request.role === "producer") return { kind: "work", work: this.reserve(request.key, request.executablePath, request.ownership, scope), joined };
				// Waiting grants no transfer authority; acquisition rechecks the live consumer after validation.
				const running = records.find((record) => !considered.has(record) && record.state.status === "running" && record.ownership.acceptsScope(record.scope, scope, true)) ??
					records.find((record) => !considered.has(record) && record.state.status === "running" && scope && record.scope?.sessionID === scope.sessionID);
				const waitingOn = running ?? records.find(record => (record.state.status === "reserved" || record.ownership.pendingWhole) && record.ownership.acceptsScope(record.scope, scope, true));
				if (!waitingOn) return { kind: "miss", joined };
				retain(waitingOn);
				const changed = new AbortController(), waiters = this.waiters.get(request.key) ?? new Set<AbortController>();
				waiters.add(changed); this.waiters.set(request.key, waiters);
				const stop = request.signal ? AbortSignal.any([changed.signal, request.signal]) : changed.signal;
				let decision: "completed" | "miss" | "rejected";
				try {
					// Any newly available result may win, including a producer registered after this wait began.
					const completion = running ? request.waitForRunning(running, stop) : waitingOn.ownership.pendingWhole?.then(() => "completed" as const, () => "completed" as const) ?? new Promise<never>(() => {});
					const waiting = await waitForCompletion(completion, stop);
					if (waiting.status === "aborted") { joined = true; historyChecked = false; continue; }
					decision = waiting.value;
				} finally {
					waiters.delete(changed); if (!waiters.size) this.waiters.delete(request.key);
					changed.abort();
				}
				if (decision !== "completed") { considered.set(waitingOn, undefined); continue; }
				joined = true;
				historyChecked = false;
			}
		} finally { for (const record of borrowed) record.borrowers--; this.trim(); }
	}

	/** Seal owned bytes before visibility; disk maintenance and persistence cannot revoke a live result. */
	async publish(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		candidate: ProcessProvenanceCertificate,
		persist: () => Promise<boolean>,
		continuation?: ProcessContinuation,
		computation?: TimelineInterval,
	): Promise<boolean> {
		if (this.byKey.get(key)?.get(handoff)?.state.status !== "running") throw new Error("process handoff is no longer running");
		const references = referencedArtifacts(candidate), bytes = [...new Map(references.map(reference => [reference.digest, reference.size])).values()].reduce((sum, size) => sum + size, continuation?.image.length ?? 0);
		if (bytes > this.maxRetainedBytes) { this.complete(key, handoff); return persist(); }
		const artifacts = await this.store.loadArtifacts(references, handoff.signal);
		if (!artifacts || !this.complete(key, handoff, { candidate, artifacts }, continuation, computation)) throw new Error("process handoff has no complete owned result");
		return persist();
	}

	complete(key: Sha256Digest, handoff: ProcessHandoff, result?: Extract<HandoffState, { status: "completed" | "reserved" | "retained" }>["result"], continuation?: ProcessContinuation,
		computation?: TimelineInterval): boolean {
		const record = this.byKey.get(key)?.get(handoff);
		if (!record || record.state.status !== "running") return false;
		if (!!result?.candidate.result.continuation !== !!continuation || continuation && continuation.image.length !== result!.candidate.result.continuation!.imageBytes) return false;
		record.continuation = continuation; this.retainedBytes += (result?.artifacts.bytes ?? 0) + (continuation?.image.length ?? 0);
		record.state = { status: "completed", result };
		this.completedCount++;
		if (result) record.computation = computation ?? continuation?.computation ?? new TimelineInterval(record.startedAt, performance.now());
		record.settle();
		this.wake(key);
		if (!result || !record.scope) this.remove(record);
		else this.trim();
		return true;
	}

	clearCompleted(): void { for (const record of this.records()) if (record.state.status !== "running") this.remove(record); }

	dispose(): void {
		this.disposed = true;
		for (const key of this.waiters.keys()) this.wake(key);
		for (const record of this.records()) {
			if (record.state.status === "running") record.controller.abort(new Error("process handoff disposed"));
			this.complete(record.key, record);
			this.remove(record);
		}
		this.byKey.clear();
		this.completedCount = 0;
	}

	private reserve(key: Sha256Digest, executablePath: string, ownership: ProcessHandoffOwnership, scope?: ExecutionScope): HandoffRecord {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const controller = new AbortController();
		const record: HandoffRecord = {
			controller, signal: controller.signal, borrowers: 0,
			key,
			completion,
			executablePath,
			scope,
			ownership,
			startedAt: performance.now(),
			state: { status: "running" },
			get inputsChanged() { return record.state.status === "running" ? record.state.inputsChanged : undefined; },
			get suspend() { return record.state.status === "running" ? record.state.suspend : undefined; },
			settle,
		};
		const records = this.byKey.get(key) ?? new Map();
		records.set(record, record);
		this.byKey.set(key, records);
		return record;
	}

	private *records(): IterableIterator<HandoffRecord> { for (const records of this.byKey.values()) yield* records.values(); }
	private wake(key: Sha256Digest): void { for (const waiter of this.waiters.get(key) ?? []) waiter.abort(); }

	private remove(record: HandoffRecord): void {
		this.revokeBinding(record);
		const records = this.byKey.get(record.key);
		if (records?.delete(record)) { if (record.state.status !== "running") this.completedCount--; this.wake(record.key); }
		if (!records?.size) this.byKey.delete(record.key);
	}

	private trim(limit = this.maxCompleted): void {
		for (const record of this.records()) {
			if (this.completedCount <= limit && this.retainedBytes <= this.maxRetainedBytes) break;
			if (!record.borrowers && record.state.status !== "running") this.remove(record);
		}
	}

	private releaseResult(record: HandoffRecord): void {
		if (record.state.status !== "running") { this.retainedBytes -= record.state.result?.artifacts.bytes ?? 0; record.state = { status: "retained" }; }
		if (record.continuation) { this.retainedBytes -= record.continuation.image.length; record.continuation = undefined; }
	}

	private revokeBinding(record: HandoffRecord): void {
		this.releaseResult(record);
		if (!record.binding) return;
		this.retainedBytes -= this.invocations.get(record.binding)?.bytes ?? 0;
		this.invocations.delete(record.binding);
		record.binding = undefined;
	}
}

export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
