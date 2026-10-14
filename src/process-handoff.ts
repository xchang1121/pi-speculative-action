import { snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import { referencedArtifacts, type ProcessProvenanceCertificate, type Sha256Digest } from "./provenance-certificate.ts";
import type { ProvenanceCertificateStore, VerifiedArtifactClosure } from "./reuse-store.ts";
import { immutableSnapshot, isImmutableSnapshot, stableEqual } from "./stable-json.ts";
import { TimelineInterval } from "./task-timing.ts";
import { waitForCompletion } from "./scheduler.ts";

const CLAIM = { available: 0, reserved: 1, consumed: 2 } as const;
const LOAN = { closed: 1, consumed: 2, reader: 4 } as const;

/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private state: "available" | "partial" | "whole" = "available";
	private reservations = 0;
	private transfer?: { readonly apply: WeakRef<() => Promise<unknown>>; readonly result: Promise<unknown> };
	private committing?: Promise<unknown>;
	private readonly observer?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
	private readonly scopeOwner?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean>;
	private readonly consumer?: ExecutionScope;

	constructor(observer?: (adoption: ExecutionOperationAdoption) => void, acceptScope?: (scope: ExecutionScope, salvage?: boolean) => boolean,
		consumer?: ExecutionScope) {
		this.consumer = snapshotExecutionScope(consumer);
		if (observer) this.observer = new WeakRef(observer);
		if (acceptScope) this.scopeOwner = new WeakRef(acceptScope);
	}

	/** A live owner may transfer this computation beyond its production turn; age grants no reuse authority. */
	acceptsScope(producer: ExecutionScope | undefined, consumer: ExecutionScope | undefined, salvage = false): boolean {
		if (!producer || !consumer || producer.sessionID !== consumer.sessionID) return false;
		if (this.consumer) return sameScope(this.consumer, consumer) && this.consumer.callID === consumer.callID;
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

type HandoffRecord = ReturnType<ProcessHandoffRegistry["reserve"]>;

export interface ProcessHandoffTransfer {
	readonly results: Readonly<ReturnType<ProcessHandoffRegistry["lend"]>["transfer"]["results"]>;
}

export type ProcessHandoffBorrow = (key: Sha256Digest, excluded: ReadonlySet<Sha256Digest>, signal?: AbortSignal) =>
	Promise<{ readonly records: readonly ProcessHandoff[]; readonly joined: boolean } | undefined>;

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
	readonly borrow?: ProcessHandoffBorrow;
	readonly excludeHandoff?: (handoff: ProcessHandoff) => boolean;
} & (
	| { readonly role: "producer"; readonly ownership: ProcessHandoffOwnership; readonly executablePath: string }
	| { readonly role: "actor" }
);

/** Owns process evidence selection and the scope of one-shot transfers. */
export class ProcessHandoffRegistry<Invocation = never> {
	private readonly byKey = new Map<Sha256Digest, Map<ProcessHandoff, HandoffRecord>>();
	private readonly waiters = new Map<Sha256Digest, Set<() => void>>();
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
		for (const record of this.records()) if ((record.state.status === "running" || record.continuation) && this.accepts(record, scope, true)) return true;
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
		const considered = new Map<HandoffRecord, Sha256Digest>();
		let changed = new AbortController(), historyChecked = false;
		const wake = () => changed.abort(), waiters = this.waiters.get(request.key) ?? new Set<() => void>();
		waiters.add(wake); this.waiters.set(request.key, waiters);
		try {
			while (!this.disposed && !request.signal?.aborted) {
				// One subscription spans the lookup; a change invalidates only this pass's negative evidence.
				if (changed.signal.aborted) { changed = new AbortController(); historyChecked = false; considered.clear(); }
				const stop = request.signal ? AbortSignal.any([changed.signal, request.signal]) : changed.signal;
				const records = [...this.byKey.get(request.key)?.values() ?? []].filter(record => !request.excludeHandoff?.(record));
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
					if (this.disposed || request.signal?.aborted) break;
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
				await Promise.allSettled([...records.filter(record => record.ownership.acceptsScope(record.scope, scope, true) &&
					(record.state.status === "running" || record.state.status === "reserved" || record.ownership.pendingWhole)).map(async record => {
					retain(record);
					const running = record.state.status === "running", startedAt = performance.now();
					const ready = running ? (record.advance?.() ?? Promise.resolve(true)).then(advanced => advanced && record.completion.then(() => true))
						: record.ownership.pendingWhole?.then(() => true, () => true) ?? new Promise<never>(() => {});
					const advanced = await waitForCompletion(ready, stop);
					if (advanced.status === "completed" && !advanced.value) return;
					waits.push({ handoff: record, interval: new TimelineInterval(startedAt, performance.now()) });
					changed.abort();
				}), ...(request.borrow ? [(async () => {
					const startedAt = performance.now(), imported = await request.borrow!(request.key, new Set(considered.values()), stop);
					if (imported?.joined) for (const handoff of imported.records) waits.push({ handoff, interval: new TimelineInterval(startedAt, performance.now()) });
					if (imported?.records.length) changed.abort();
				})()] : [])]);
				if (!stop.aborted) break;
			}
			return { kind: "miss", joined: waits.length > 0 };
		} finally {
			waiters.delete(wake); if (!waiters.size) this.waiters.delete(request.key); changed.abort();
			for (const record of borrowed) record.borrowers--; this.trim();
		}
	}

	private accepts(record: HandoffRecord, scope: ExecutionScope | undefined, salvage: boolean): boolean {
		return !record.ownership.wholeClaimed && record.ownership.acceptsScope(record.scope, scope, salvage);
	}

	private claim(record: HandoffRecord, oneShot: boolean,
		forwardOwner?: { readonly ownership: ProcessHandoffOwnership; readonly executablePath: string }, scope?: ExecutionScope, loan?: Int32Array): ((consumed: boolean, forward?: boolean) => void) | undefined {
		const state = record.state;
		if (state.status !== "completed" || record.receipts.some(receipt => Atomics.load(receipt, 0) & LOAN.closed)) return;
		const claim = oneShot ? record.ownership.claimChild() : undefined;
		if (oneShot && !claim) return;
		if (oneShot && !loan && record.claimState && Atomics.compareExchange(record.claimState, 0, CLAIM.available, CLAIM.reserved) !== CLAIM.available) { claim?.(false); return; }
		if (oneShot && !loan) record.state = { ...state, status: "reserved" };
		record.borrowers++;
		const held: Int32Array[] = [];
		let pending = true;
		const settle = (consumed: boolean, forward?: boolean) => {
			if (!pending) return;
			pending = false; claim?.(consumed); record.borrowers--;
			if (consumed) for (const receipt of loan ? [...record.receipts, loan] : record.receipts) Atomics.or(receipt, 0, LOAN.consumed);
			for (const receipt of held) Atomics.sub(receipt, 0, LOAN.reader);
			if (record.claimState && (!loan || consumed)) {
				if (consumed) Atomics.store(record.claimState, 0, CLAIM.consumed); else Atomics.compareExchange(record.claimState, 0, CLAIM.reserved, CLAIM.available);
			}
			if (oneShot && this.byKey.get(record.key)?.has(record)) {
				if (consumed && forward && forwardOwner) {
					const next = this.reserve(record.key, forwardOwner.executablePath, forwardOwner.ownership, scope);
					next.binding = record.binding; record.binding = undefined;
					this.remove(record); this.complete(record.key, next, state.result);
				} else if (consumed) this.releaseResult(record); else if (!loan) record.state = state;
				this.wake(record.key);
			}
			this.trim();
		};
		if (!loan) for (const receipt of record.receipts) {
			held.push(receipt); if (Atomics.add(receipt, 0, LOAN.reader) & LOAN.closed) { settle(false); return; }
		}
		return settle;
	}

	/** One lending lifetime owns initial evidence and later publications until the caller settles it. */
	lend(scope?: ExecutionScope) {
		scope = snapshotExecutionScope(scope);
		const loans: { readonly record: HandoffRecord; readonly release: (consumed: boolean) => void; readonly claims: Int32Array }[] = [];
		const lifetime = new AbortController();
		const settle = () => { lifetime.abort(); for (const { release, claims } of loans) release(!!(Atomics.or(claims, 0, LOAN.closed) & ~LOAN.closed)); };
		const offer = (record: HandoffRecord) => {
			const state = record.state, result = state.status === "completed" ? state.result : undefined;
			if (!result || oneShotResult(result.candidate) && !this.accepts(record, scope, true)) return;
			const computation = TimelineInterval.serialize(result.computation), continuation = result.continuation;
			const resumed = continuation && TimelineInterval.serialize(continuation.computation);
			if (!computation || continuation && !resumed) return;
			const id = loans.length, claims = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
			if (oneShotResult(result.candidate)) record.claimState ??= new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
			const transferred = { id, claims: [...record.receipts, claims].map(receipt => receipt.buffer as SharedArrayBuffer),
				reservation: record.claimState?.buffer as SharedArrayBuffer | undefined, key: record.key, executablePath: record.executablePath, scope: record.scope,
				candidate: result.candidate, bytes: result.bytes, computation,
				artifacts: Object.freeze([...new Map(referencedArtifacts(result.candidate).map(reference => [reference.digest, reference])).values()]
					.map(reference => ({ reference, bytes: new Uint8Array(result.artifacts.read(reference)) } as const))),
				...(continuation ? { continuation: { image: new Uint8Array(continuation.image), physicalRoot: continuation.physicalRoot, computation: resumed! } as const } : {}) } as const;
			const release = this.claim(record, oneShotResult(result.candidate), undefined, undefined, claims);
			if (!release) return;
			loans.push({ record, release, claims }); return transferred;
		};
		try {
			return { transfer: { results: [...this.records()].flatMap(record => offer(record) ?? []) }, settle, binding: (index: number) => loans[index]?.record.binding,
				adopted: (index: number, adoption: ExecutionOperationAdoption) => loans[index]?.record.ownership.adopted(adoption),
				claimed: () => loans.some(({ claims }) => Atomics.load(claims, 0) & ~LOAN.closed),
				borrow: async (key: Sha256Digest, excluded: ReadonlySet<Sha256Digest>, signal?: AbortSignal) => {
					if (lifetime.signal.aborted || signal?.aborted) return;
					const acquired = await this.acquire({ key, scope, signal: signal ? AbortSignal.any([signal, lifetime.signal]) : lifetime.signal, role: "actor",
						excludeHandoff: handoff => loans.some(({ record }) => record === handoff), lookup: async candidates => {
						const certificate = candidates?.find(candidate => !excluded.has(candidate.id)); return certificate && { certificate };
					} });
					if (acquired.kind !== "hit" || !acquired.producer) return;
					acquired.settle?.(false);
					if (lifetime.signal.aborted || signal?.aborted) return;
					const record = this.byKey.get(key)?.get(acquired.producer), result = record && offer(record);
					return result ? { transfer: { results: [result] }, joined: acquired.joined } : undefined;
				} };
		} catch (error) { settle(); throw error; }
	}

	/** Revalidate transferred bytes through the same artifact loader; a copy never extends its loan's scope. */
	async receive(transfer: ProcessHandoffTransfer, scope: ExecutionScope | undefined,
		adopted: (index: number, adoption: ExecutionOperationAdoption) => void) {
		const callbacks: object[] = [], received: HandoffRecord[] = [];
		const close = () => { callbacks.length = 0; for (const record of received) this.remove(record); };
		try { for (const result of transfer.results) {
			const computation = TimelineInterval.restore(result.computation), resumed = result.continuation && TimelineInterval.restore(result.continuation.computation);
			if (!computation || result.continuation && !resumed) continue;
			const images = new Map(result.artifacts.map(({ reference, bytes }) => [reference.digest, Buffer.from(bytes)]));
			const artifacts = await this.store.loadArtifacts(referencedArtifacts(result.candidate), undefined, {
				artifacts: images.size, bytes: [...images.values()].reduce((sum, bytes) => sum + bytes.length, 0),
				read: reference => Buffer.from(images.get(reference.digest) ?? []),
			});
			if (!artifacts) continue;
			const observe = (adoption: ExecutionOperationAdoption) => adopted(result.id, adoption);
			callbacks.push(observe);
			const record = this.reserve(result.key, result.executablePath, new ProcessHandoffOwnership(observe, undefined, scope), result.scope);
			received.push(record);
			record.claimState = result.reservation && new Int32Array(result.reservation);
			record.receipts = result.claims.map(claims => new Int32Array(claims));
			this.complete(result.key, record, { candidate: result.candidate, artifacts, bytes: result.bytes, computation,
				...(result.continuation ? { continuation: { ...result.continuation, image: Buffer.from(result.continuation.image), computation: resumed! } } : {}) });
		} return Object.assign(close, { records: received }); } catch (error) { close(); throw error; }
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

	private reserve(key: Sha256Digest, executablePath: string, ownership: ProcessHandoffOwnership, scope?: ExecutionScope) {
		if (this.disposed) throw new Error("process handoff registry is disposed");
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => { settle = resolve; });
		const controller: AbortController = new AbortController();
		const record = {
			key, completion, executablePath, scope, ownership, settle, startedAt: performance.now(), state: { status: "running" } as HandoffState,
			controller, signal: controller.signal as AbortSignal, borrowers: 0,
			binding: undefined as ProcessExecutionBinding | undefined,
			// All aliases share one reservation; every loan in their ancestry can revoke future claims.
			claimState: undefined as Int32Array | undefined,
			receipts: [] as readonly Int32Array[],
			get advance(): ProcessHandoff["advance"] { return record.state.status === "running" ? record.state.advance : undefined; },
			get computation(): TimelineInterval | undefined { return record.state.status !== "running" ? record.state.result?.computation : undefined; },
			get continuation(): ProcessContinuation | undefined { return record.state.status !== "running" ? record.state.result?.continuation : undefined; },
		};
		const records = this.byKey.get(key) ?? new Map();
		records.set(record, record);
		this.byKey.set(key, records);
		return record;
	}

	private *records(): IterableIterator<HandoffRecord> { for (const records of this.byKey.values()) yield* records.values(); }
	private wake(key: Sha256Digest): void { for (const waiter of this.waiters.get(key) ?? []) waiter(); }

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

export type ProcessHandoffLender = ReturnType<ProcessHandoffRegistry["lend"]>["borrow"];

export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}

function oneShotResult(candidate: ProcessProvenanceCertificate): boolean {
	return candidate.dependencyCertificate.taints.length > 0 || !!candidate.result.continuation;
}
