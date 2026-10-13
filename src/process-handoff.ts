import { snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import { referencedArtifacts, type ArtifactReference, type ProcessProvenanceCertificate, type Sha256Digest } from "./provenance-certificate.ts";
import type { ProvenanceCertificateStore, VerifiedArtifactClosure } from "./reuse-store.ts";
import { immutableSnapshot, isImmutableSnapshot, stableEqual } from "./stable-json.ts";
import { TimelineInterval, type SerializedTimelineComputation } from "./task-timing.ts";
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
	/** Advance owned work toward a result. False declines this join; true still needs a sealed, validated result. */
	readonly advance?: () => Promise<boolean>;
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
	| { readonly status: "running"; advance?: () => Promise<boolean> }
	| { readonly status: "completed" | "reserved" | "retained"; readonly result?: { readonly candidate: ProcessProvenanceCertificate; readonly artifacts: VerifiedArtifactClosure;
		readonly bytes: number; readonly computation: TimelineInterval; readonly continuation?: ProcessContinuation } };

interface HandoffRecord extends ProcessHandoff {
	borrowers: number;
	readonly controller: AbortController;
	readonly key: Sha256Digest;
	state: HandoffState;
	binding?: ProcessExecutionBinding;
	readonly executablePath: string;
	readonly settle: () => void;
	readonly continuation?: ProcessContinuation;
	/** A worker's reservation remains observable even if its result message is lost. */
	claimState?: Int32Array;
}

export interface ProcessHandoffTransfer {
	readonly claims: SharedArrayBuffer;
	readonly results: readonly {
		readonly key: Sha256Digest; readonly executablePath: string; readonly scope?: ExecutionScope;
		readonly candidate: ProcessProvenanceCertificate; readonly bytes: number; readonly computation: SerializedTimelineComputation;
		readonly artifacts: readonly { readonly reference: ArtifactReference; readonly bytes: Uint8Array }[];
		readonly continuation?: { readonly image: Uint8Array; readonly physicalRoot: string; readonly computation: SerializedTimelineComputation };
	}[];
}

export type ProcessHandoffAcquisition<Plan> =
	| { readonly kind: "hit"; readonly plan: Plan; readonly joined: boolean; readonly producer?: ProcessHandoff; readonly continuation?: ProcessContinuation;
		readonly waiting?: readonly TimelineInterval[];
		readonly settle?: (consumed: boolean, forward?: boolean) => void }
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
	| { readonly role: "actor" }
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
		for (const record of this.records()) if (record.ownership === ownership && !record.signal.aborted && record.advance) return true;
		return false;
	}

	/** Retrieval hint for both running and completed records; it grants no adoption authority. */
	mayHaveExecutable(executablePath: string): boolean {
		for (const record of this.records()) if (record.state.status !== "retained" && record.executablePath === executablePath) return true;
		return false;
	}

	/** Running capabilities are observable state; completion or withdrawal revokes even a borrowed callback. */
	observeRunning(key: Sha256Digest, handoff: ProcessHandoff, invoke: NonNullable<ProcessHandoff["advance"]>): () => void {
		const record = this.byKey.get(key)?.get(handoff), state = record?.state;
		if (!record || state?.status !== "running") return () => {};
		const owned = () => record.state === state && state.advance === owned ? invoke() : Promise.resolve(false);
		state.advance = owned;
		this.wake(key);
		return () => { if (state.advance === owned) { state.advance = undefined; this.wake(key); } };
	}

	async acquire<Plan extends { readonly certificate: ProcessProvenanceCertificate }>({ scope, ...request }: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		scope = snapshotExecutionScope(scope);
		const waits: { readonly handoff: ProcessHandoff; readonly interval: TimelineInterval }[] = [];
		const borrowed = new Set<HandoffRecord>();
		const retain = (record: HandoffRecord) => { if (!borrowed.has(record)) { borrowed.add(record); record.borrowers++; } };
		try {
			while (true) {
				if (this.disposed || request.signal?.aborted) return { kind: "miss", joined: waits.length > 0 };
				// Observe before any asynchronous lookup; negative evidence belongs only to this unchanged pass.
				const changed = new AbortController(), waiters = this.waiters.get(request.key) ?? new Set<AbortController>();
				waiters.add(changed); this.waiters.set(request.key, waiters);
				const stop = request.signal ? AbortSignal.any([changed.signal, request.signal]) : changed.signal;
				const considered = new Map<HandoffRecord, Sha256Digest>();
				let historyChecked = false;
				try {
					while (!stop.aborted) {
						const records = [...this.byKey.get(request.key)?.values() ?? []];
						const completed = [...records].reverse().flatMap((record) => {
							const state = record.state;
							if (state.status !== "completed" || !state.result || considered.has(record)) return [];
							const { candidate, artifacts } = state.result, oneShot = oneShotResult(candidate);
							return oneShot && !this.accepts(record, scope, request.role === "actor")
								? [] : [{ record, state, candidate, artifacts, oneShot }];
						});
						if (completed.length || !historyChecked) {
							historyChecked ||= !completed.length;
							for (const { record } of completed) retain(record);
							const plan = completed.length
								? await request.lookup(completed.map(({ candidate }) => candidate), undefined, new Map(completed.map(({ candidate, artifacts }) => [candidate.id, artifacts])))
								: await request.lookup(undefined, new Set(considered.values()));
							if (this.disposed || request.signal?.aborted) return { kind: "miss", joined: waits.length > 0 };
							if (plan && !completed.length) return { kind: "hit", plan, joined: waits.length > 0 };
							const selected = completed.find(({ candidate }) => candidate === plan?.certificate);
							if (!selected) for (const { record, candidate } of completed) considered.set(record, candidate.id);
							if (plan && selected && selected.record.state === selected.state && this.byKey.get(request.key)?.has(selected.record)) {
								const { record, oneShot } = selected;
								const settle = this.claim(record, oneShot, request.role === "producer" ? request : undefined, scope);
								if (!settle) { considered.set(record, selected.candidate.id); continue; }
								return { kind: "hit", plan, joined: waits.length > 0, producer: record, continuation: record.continuation,
									waiting: waits.filter(wait => wait.handoff === record).map(wait => wait.interval), settle };
							}
							continue;
						}
						if (request.role === "producer") return { kind: "work", work: this.reserve(request.key, request.executablePath, request.ownership, scope), joined: waits.length > 0 };
						// Each producer advances independently; readiness never grants transfer authority.
						await Promise.allSettled(records.filter(record => record.ownership.acceptsScope(record.scope, scope, true) &&
							(record.state.status === "running" || record.state.status === "reserved" || record.ownership.pendingWhole)).map(async record => {
							retain(record);
							const running = record.state.status === "running", startedAt = performance.now();
							if (running) {
								const advanced = await waitForCompletion(record.advance?.() ?? Promise.resolve(true), stop);
								if (advanced.status === "completed" && !advanced.value) return;
							}
							await waitForCompletion(running ? record.completion :
								record.ownership.pendingWhole?.then(() => {}, () => {}) ?? new Promise<never>(() => {}), stop);
							waits.push({ handoff: record, interval: new TimelineInterval(startedAt, performance.now()) });
							changed.abort();
						}));
						if (!stop.aborted) return { kind: "miss", joined: waits.length > 0 };
						break;
					}
				} finally {
					waiters.delete(changed); if (!waiters.size) this.waiters.delete(request.key);
					changed.abort();
				}
			}
		} finally { for (const record of borrowed) record.borrowers--; this.trim(); }
	}

	private accepts(record: HandoffRecord, scope: ExecutionScope | undefined, salvage: boolean): boolean {
		return !record.ownership.wholeClaimed && record.ownership.acceptsScope(record.scope, scope, salvage);
	}

	private claim(record: HandoffRecord, oneShot: boolean,
		forwardOwner?: { readonly ownership: ProcessHandoffOwnership; readonly executablePath: string }, scope?: ExecutionScope): ((consumed: boolean, forward?: boolean) => void) | undefined {
		const state = record.state;
		if (state.status !== "completed") return;
		const claim = oneShot ? record.ownership.claimChild() : undefined;
		if (oneShot && !claim) return;
		if (oneShot) record.state = { ...state, status: "reserved" };
		record.borrowers++;
		if (record.claimState) Atomics.compareExchange(record.claimState, 0, 0, 1);
		let pending = true;
		return (consumed, forward) => {
			if (!pending) return;
			pending = false; claim?.(consumed); record.borrowers--;
			if (record.claimState) {
				if (consumed) Atomics.store(record.claimState, 0, 2); else Atomics.compareExchange(record.claimState, 0, 1, 0);
			}
			if (oneShot && this.byKey.get(record.key)?.has(record)) {
				if (consumed && forward && forwardOwner) {
					const next = this.reserve(record.key, forwardOwner.executablePath, forwardOwner.ownership, scope);
					next.binding = record.binding; record.binding = undefined;
					this.remove(record); this.complete(record.key, next, state.result);
				} else if (consumed) this.releaseResult(record); else record.state = state;
				this.wake(record.key);
			}
			this.trim();
		};
	}

	/** Loan only completed evidence; the original owner remains claimed until the caller can run again. */
	lend(scope?: ExecutionScope) {
		const results: ProcessHandoffTransfer["results"][number][] = [], records: HandoffRecord[] = [], settlements: ((consumed: boolean) => void)[] = [];
		let claims: Int32Array | undefined;
		const settle = () => settlements.forEach((release, index) => release(!!claims && Atomics.load(claims, index) !== 0));
		try {
			for (const record of this.records()) {
				const state = record.state, result = state.status === "completed" ? state.result : undefined;
				if (!result || oneShotResult(result.candidate) && !this.accepts(record, scope, true)) continue;
				const computation = TimelineInterval.serialize(result.computation), continuation = result.continuation;
				const resumed = continuation && TimelineInterval.serialize(continuation.computation);
				if (!computation || continuation && !resumed) continue;
				const release = this.claim(record, oneShotResult(result.candidate));
				if (!release) continue;
				settlements.push(release); records.push(record);
				results.push({ key: record.key, executablePath: record.executablePath, scope: record.scope,
					candidate: result.candidate, bytes: result.bytes, computation,
					artifacts: [...new Map(referencedArtifacts(result.candidate).map(reference => [reference.digest, reference])).values()]
						.map(reference => ({ reference, bytes: new Uint8Array(result.artifacts.read(reference)) })),
					...(continuation ? { continuation: { image: new Uint8Array(continuation.image), physicalRoot: continuation.physicalRoot, computation: resumed! } } : {}) });
			}
			claims = new Int32Array(new SharedArrayBuffer(results.length * 4));
			return { transfer: { results, claims: claims.buffer as SharedArrayBuffer }, settle, binding: (index: number) => records[index]?.binding,
				adopted: (index: number, adoption: ExecutionOperationAdoption) => records[index]?.ownership.adopted(adoption) };
		} catch (error) { settle(); throw error; }
	}

	/** Revalidate transferred bytes through the same artifact loader; a copy never extends its loan's scope. */
	async receive(transfer: ProcessHandoffTransfer, scope: ExecutionScope | undefined,
		adopted: (index: number, adoption: ExecutionOperationAdoption) => void): Promise<() => void> {
		const callbacks: object[] = [], received: HandoffRecord[] = [];
		for (const [index, result] of transfer.results.entries()) {
			const computation = TimelineInterval.restore(result.computation), resumed = result.continuation && TimelineInterval.restore(result.continuation.computation);
			if (!computation || result.continuation && !resumed) continue;
			const images = new Map(result.artifacts.map(({ reference, bytes }) => [reference.digest, Buffer.from(bytes)]));
			const artifacts = await this.store.loadArtifacts(referencedArtifacts(result.candidate), undefined, {
				artifacts: images.size, bytes: [...images.values()].reduce((sum, bytes) => sum + bytes.length, 0),
				read: reference => Buffer.from(images.get(reference.digest) ?? []),
			});
			if (!artifacts) continue;
			const observe = (adoption: ExecutionOperationAdoption) => adopted(index, adoption);
			const accept = (consumer: ExecutionScope) => sameScope(scope, consumer) && scope?.callID === consumer.callID;
			callbacks.push(observe, accept);
			const record = this.reserve(result.key, result.executablePath, new ProcessHandoffOwnership(observe, accept), result.scope);
			received.push(record);
			record.claimState = new Int32Array(transfer.claims, index * 4, 1);
			this.complete(result.key, record, { candidate: result.candidate, artifacts, bytes: result.bytes, computation,
				...(result.continuation ? { continuation: { ...result.continuation, image: Buffer.from(result.continuation.image), computation: resumed! } } : {}) });
		}
		return () => { callbacks.length = 0; for (const record of received) this.remove(record); };
	}

	/** Seal owned bytes before visibility; disk maintenance and persistence cannot revoke a live result. */
	async publish(key: Sha256Digest, handoff: ProcessHandoff, candidate: ProcessProvenanceCertificate, persist: () => Promise<boolean>,
		continuation?: ProcessContinuation, computation?: TimelineInterval): Promise<boolean> {
		if (this.byKey.get(key)?.get(handoff)?.state.status !== "running") throw new Error("process handoff is no longer running");
		computation ??= continuation?.computation ?? new TimelineInterval(handoff.startedAt, performance.now());
		const graphs = [...new Set([computation, ...(continuation ? [continuation.computation] : [])])].map(value => TimelineInterval.serialize(value));
		const references = referencedArtifacts(candidate), bytes = Buffer.byteLength(JSON.stringify([candidate, graphs])) +
			[...new Map(references.map(reference => [reference.digest, reference.size])).values()].reduce((sum, size) => sum + size, continuation?.image.length ?? 0);
		if (bytes > this.maxRetainedBytes) { this.complete(key, handoff); return persist(); }
		const artifacts = await this.store.loadArtifacts(references, handoff.signal);
		if (!artifacts || !this.complete(key, handoff, { candidate, artifacts, bytes, computation, continuation })) throw new Error("process handoff has no complete owned result");
		return persist();
	}

	complete(key: Sha256Digest, handoff: ProcessHandoff, result?: Extract<HandoffState, { status: "completed" | "reserved" | "retained" }>["result"]): boolean {
		const record = this.byKey.get(key)?.get(handoff);
		if (!record || record.state.status !== "running") return false;
		const continuation = result?.continuation;
		if (!!result?.candidate.result.continuation !== !!continuation || continuation && continuation.image.length !== result!.candidate.result.continuation!.imageBytes) return false;
		this.retainedBytes += result?.bytes ?? 0;
		record.state = { status: "completed", result };
		this.completedCount++;
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
			if (!this.complete(record.key, record)) this.remove(record);
		}
		this.completedCount = 0;
	}

	private reserve(key: Sha256Digest, executablePath: string, ownership: ProcessHandoffOwnership, scope?: ExecutionScope): HandoffRecord {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const controller = new AbortController();
		const record: HandoffRecord = {
			key, completion, executablePath, scope, ownership, settle, startedAt: performance.now(), state: { status: "running" },
			controller, signal: controller.signal, borrowers: 0,
			get advance() { return record.state.status === "running" ? record.state.advance : undefined; },
			get computation() { return record.state.status !== "running" ? record.state.result?.computation : undefined; },
			get continuation() { return record.state.status !== "running" ? record.state.result?.continuation : undefined; },
		};
		const records = this.byKey.get(key) ?? new Map();
		records.set(record, record);
		this.byKey.set(key, records);
		return record;
	}

	private *records(): IterableIterator<HandoffRecord> { for (const records of this.byKey.values()) yield* records.values(); }
	private wake(key: Sha256Digest): void { for (const waiter of this.waiters.get(key) ?? []) waiter.abort(); }

	private remove(record: HandoffRecord): void {
		this.releaseResult(record);
		if (record.binding) {
			this.retainedBytes -= this.invocations.get(record.binding)?.bytes ?? 0;
			this.invocations.delete(record.binding);
			record.binding = undefined;
		}
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
		if (record.state.status !== "running") { this.retainedBytes -= record.state.result?.bytes ?? 0; record.state = { status: "retained" }; }
	}
}

export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}

function oneShotResult(candidate: ProcessProvenanceCertificate): boolean {
	return candidate.dependencyCertificate.taints.length > 0 || !!candidate.result.continuation;
}
