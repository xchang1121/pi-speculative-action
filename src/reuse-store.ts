import { createHash, randomUUID } from "node:crypto";
import { readSync } from "node:fs";
import { AsyncLocalStorage } from "node:async_hooks";
import { mkdir, open, readFile, readdir, realpath, rename, rm, stat, utimes, writeFile, type FileHandle } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from "node:sqlite";
import { setTimeout as delay } from "node:timers/promises";
import {
	certificateReplayable, parseProcessCertificate, type ProcessProvenanceCertificate, type ProvenanceTaint,
	referencedArtifacts,
} from "./provenance-certificate.ts";
import { CONTENT_CHUNK_BYTES as ARTIFACT_BLOCK_BYTES, readChunkRange, type ArtifactReference, validateArtifact } from "./artifact-reference.ts";
import { digestObject, isSha256Digest, sha256Digest, sha256DigestAsync, type Sha256Digest } from "./stable-value-hash.ts";
import { stableStringify } from "./stable-json.ts";
import { nonNegativeNumber, positiveInteger } from "./setting-input.ts";
import { isMissing as missing } from "./error-utils.ts";
import { FilesystemObjectLease, mapFilesystem, openAnonymousFilesystemFile, openFilesystemFile, withFilesystemReadView, type FilesystemObjectTransfer } from "./filesystem-evidence.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import { isSafeCount } from "./number-utils.ts";
import { SQLiteReadOwner } from "./sqlite-read.ts";
import { normalizeTimelineComputation, type SerializedTimelineComputation } from "./timeline-computation.ts";

/** maxCertificates is the shared metadata-record limit, including persistent input versions. */
export interface ProvenanceStoreLimits { readonly maxCertificates: number; readonly maxBytes: number; }

export interface ProvenanceStoreOptions extends Partial<ProvenanceStoreLimits> {
	readonly gcIntervalMs?: number; readonly orphanGraceMs?: number;
	/** Configuration already reserved this root; runtime publication must never recreate its ancestors. */
	readonly reservedRoot?: boolean;
	/** Independent of retention: over-budget stores must still be readable for collection. */
	readonly maxReadBytes?: number;
	/** Taints the publisher accepted: retention and stats keep what its lookups can still replay. */
	readonly acceptedTaints?: readonly ProvenanceTaint[];
}

export interface ProvenanceStoreStats {
	readonly certificates: number; readonly artifacts: number; readonly orphanArtifacts: number;
	readonly inputVersions: number;
	readonly totalBytes: number; readonly overBudget: boolean;
}

export interface ProvenanceStoreGCResult { readonly removedCertificates: number; readonly removedArtifacts: number; readonly removedBytes: number; readonly removedInputVersions: number; }

export interface StoredInputVersion {
	readonly id: Sha256Digest; readonly createdAt: number; readonly schema: 1;
	readonly payload: unknown; readonly artifacts: readonly ArtifactReference[];
}

/** A restart candidate carries evidence, never the old callback's live authority. */
export interface StoredProcessCheckpoint {
	readonly schema: 1; readonly id: Sha256Digest; readonly ticket: string; readonly createdAt: number;
	readonly sessionID: string; readonly sourceRoot: string; readonly physicalRoot: string; readonly context: Sha256Digest;
	readonly producer: { readonly pid: number; readonly start: string };
	readonly certificate: ProcessProvenanceCertificate; readonly computation: SerializedTimelineComputation; readonly image: ArtifactReference;
}

export const DEFAULT_PROVENANCE_STORE_LIMITS: ProvenanceStoreLimits = Object.freeze({ maxCertificates: 4_096, maxBytes: 2 * 1024 * 1024 * 1024 });

const DEFAULT_GC_INTERVAL_MS = 5 * 60_000;
const DEFAULT_ORPHAN_GRACE_MS = 5 * 60_000;

/** Immutable effects; use store.artifacts to participate in a shared store's publication and retention lifecycle. */
export class ArtifactCAS {
	readonly root: string;
	private readonly publish: typeof publishImmutable;
	private readonly readers = new Set<ArtifactReadView>();

	constructor(root: string, publish = publishImmutable) { this.root = path.resolve(root); this.publish = publish; }

	async put(value: string | Uint8Array): Promise<ArtifactReference> {
		const bytes = Buffer.from(value);
		const reference = Object.freeze({ digest: await sha256DigestAsync(bytes), size: bytes.byteLength }), target = this.artifactPath(reference.digest);
		await this.publish(target, bytes);
		return reference;
	}

	async get(reference: ArtifactReference, signal?: AbortSignal): Promise<Buffer | undefined> {
		validateArtifact(reference);
		return verifyArtifact(reference, await readOptional(this.artifactPath(reference.digest), reference.size, signal));
	}

	/** Availability hint only; publication and adoption verify content. */
	async has(reference: ArtifactReference): Promise<boolean> {
		const info = await withFilesystemReadView(undefined, view => view.stat(this.artifactPath(reference.digest), true)
			.catch((error): undefined => { if (!missing(error)) throw error; }));
		return info?.isFile() === true && info.size === BigInt(reference.size);
	}

	/** Load and integrity-check a complete effect closure before any replay side effect begins. */
	async load(references: readonly ArtifactReference[], signal?: AbortSignal, retained?: VerifiedArtifactClosure, limits?: Partial<ArtifactReadLimits>): Promise<VerifiedArtifactClosure | undefined> {
		const view = new ArtifactReadView(this, limits, () => this.readers.delete(view)), loan = retained?.lend(); let complete = false;
		this.readers.add(view);
		try { complete = await view.restore(references, loan?.transfer, signal); return complete ? view : undefined; }
		finally { loan?.release(); if (!complete) await view.dispose(); }
	}
	async dispose(): Promise<void> { await Promise.all([...this.readers].map(view => view.dispose())); }

	artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest); return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}

export interface VerifiedArtifactClosure {
	readonly artifacts: number; readonly bytes: number;
	/** Each consumer owns its copy; callbacks cannot mutate a retained result's verified bytes. */
	readonly read: (reference: ArtifactReference) => Buffer;
	readonly readRange: (reference: ArtifactReference, offset: number, length: number) => Promise<Buffer>;
	readonly memoryBytes: number;
	readonly lend: () => { readonly transfer: readonly ArtifactTransfer[]; readonly release: () => void };
	readonly dispose: () => Promise<void>;
}

export type ArtifactTransfer = { readonly reference: ArtifactReference } & ({ readonly object: FilesystemObjectTransfer } | { readonly bytes: Uint8Array });
interface ArtifactReadLimits { readonly maxReadBytes: number; readonly maxStoredBytes: number; readonly maxOpenChunks: number; }

/** One verified content lease, shared by effect closures and input chunks. Linux owns objects; other adapters retain their existing byte images. */
export class ArtifactReadView implements VerifiedArtifactClosure {
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly values = new Map<Sha256Digest, { readonly reference: ArtifactReference; readonly blocks: readonly Sha256Digest[];
		readonly object?: FilesystemObjectLease; readonly image?: Buffer }>();
	private readonly cas: ArtifactCAS;
	private readonly released?: () => void;
	private readonly limits: ArtifactReadLimits;
	constructor(cas: ArtifactCAS, limits: Partial<ArtifactReadLimits> = {}, released?: () => void) {
		this.cas = cas; this.released = released;
		this.limits = Object.freeze({ maxReadBytes: limits.maxReadBytes ?? 64 * ARTIFACT_BLOCK_BYTES,
			maxStoredBytes: limits.maxStoredBytes ?? 2 ** 31, maxOpenChunks: limits.maxOpenChunks ?? 1024 });
		if (Object.values(this.limits).some(value => !isSafeCount(value)) || this.limits.maxReadBytes > 2 ** 30 || this.limits.maxOpenChunks > 65536)
			throw new Error("invalid artifact lease budget");
	}
	get maxReadBytes(): number { return this.limits.maxReadBytes; }
	get artifacts(): number { return this.values.size; }
	get bytes(): number { return [...this.values.values()].reduce((sum, value) => sum + value.reference.size, 0); }
	get memoryBytes(): number { return [...this.values.values()].reduce((sum, value) => sum + 160 + value.blocks.length * 80 + (value.image?.length ?? 0), 0); }
	has(reference: ArtifactReference): boolean { return this.values.get(reference.digest)?.reference.size === reference.size; }
	checkBudget(reference: ArtifactReference): void {
		validateArtifact(reference);
		if (!this.has(reference) && (this.bytes + reference.size > this.limits.maxStoredBytes || this.artifacts >= this.limits.maxOpenChunks))
			throw new Error("artifact stored-byte or descriptor budget exceeded");
	}
	async restore(references: readonly ArtifactReference[], transfers?: readonly ArtifactTransfer[], signal?: AbortSignal): Promise<boolean> {
		const sizes = new Map<Sha256Digest, number>(); for (const reference of references) validateArtifact(reference, sizes);
		const supplied = transfers && new Map(transfers.map(value => [value.reference.digest, value]));
		if (supplied && supplied.size !== transfers!.length) throw new Error("duplicate artifact transfer");
		for (const [digest, size] of sizes) {
			const retained = supplied?.get(digest);
			if (supplied && !retained || !await this.add({ digest, size }, signal, retained)) return false;
		}
		signal?.throwIfAborted(); return true;
	}
	add(reference: ArtifactReference, signal?: AbortSignal, supplied?: ArtifactTransfer): Promise<boolean> {
		return this.lifetime.serialize(async () => {
			signal?.throwIfAborted(); validateArtifact(reference);
			if (this.values.has(reference.digest)) { if (!this.has(reference)) throw new Error("conflicting artifact size"); return true; }
			this.checkBudget(reference);
			if (supplied && (supplied.reference.digest !== reference.digest || supplied.reference.size !== reference.size)) throw new Error("artifact transfer identity mismatch");
			let object: FilesystemObjectLease | undefined, image: Buffer | undefined, snapshot: FileHandle | undefined;
			try {
				if (supplied && "bytes" in supplied || process.platform !== "linux") {
					image = supplied && "bytes" in supplied ? Buffer.from(supplied.bytes) : await this.cas.get(reference, signal);
					if (!image) return false;
					if (image.length !== reference.size) throw new Error("artifact integrity check failed: length");
					if (process.platform === "linux") snapshot = await openAnonymousFilesystemFile(this.cas.root);
				} else {
					const imported = supplied && "object" in supplied ? FilesystemObjectLease.receive(supplied.object, true) : undefined;
					if (supplied && !imported) return false;
					const handle = imported?.handle ?? await openArtifactFile(this.cas.artifactPath(reference.digest));
					if (!handle) return false;
					try {
						const stat = imported?.stat ?? await handle.stat({ bigint: true });
						if (!stat.isFile() || stat.size !== BigInt(reference.size)) throw new Error("artifact integrity check failed: length");
						object = new FilesystemObjectLease(handle, stat, true);
					} catch (error) { await handle.close(); throw error; }
					// A named CAS inode can be damaged in place. A lease owns verified bytes independently of that name and object.
					if (!imported) snapshot = await openAnonymousFilesystemFile(this.cas.root);
				}
				const hash = createHash("sha256"), blocks: Sha256Digest[] = [];
				for (let offset = 0; offset < reference.size; offset += ARTIFACT_BLOCK_BYTES) {
					signal?.throwIfAborted();
					const block = await this.block({ reference, object, image, blocks: [] }, offset / ARTIFACT_BLOCK_BYTES, false);
					hash.update(block); blocks.push(await sha256DigestAsync(block));
					if (snapshot) for (let written = 0; written < block.length;) {
						const { bytesWritten } = await snapshot.write(block, written, block.length - written, offset + written);
						if (!bytesWritten) throw new Error("artifact snapshot write made no progress"); written += bytesWritten;
					}
				}
				if (`sha256:${hash.digest("hex")}` !== reference.digest) throw new Error(`artifact integrity check failed for ${reference.digest}`);
				if (snapshot) {
					const handle = await open(`/proc/self/fd/${snapshot.fd}`, "r"), source = object;
					try { object = new FilesystemObjectLease(handle, await handle.stat({ bigint: true }), true); }
					catch (error) { await handle.close(); throw error; }
					await source?.dispose(); image = undefined;
				}
				signal?.throwIfAborted(); this.values.set(reference.digest, { reference: Object.freeze({ ...reference }), object, image, blocks }); object = undefined;
				return true;
			} finally { try { await snapshot?.close(); } finally { await object?.dispose(); } }
		});
	}
	read(reference: ArtifactReference): Buffer {
		if (this.lifetime.sealed) throw new Error("artifact lease is closed");
		const value = this.value(reference); this.range(reference, 0, reference.size);
		const result = Buffer.allocUnsafe(reference.size);
		for (let offset = 0; offset < result.length; offset += ARTIFACT_BLOCK_BYTES) {
			const block = result.subarray(offset, Math.min(result.length, offset + ARTIFACT_BLOCK_BYTES));
			if (value.image) value.image.copy(block, 0, offset, offset + block.length);
			else for (let count = 0; count < block.length;) { const read = readSync(value.object!.handle.fd, block, count, block.length - count, offset + count); if (!read) throw new Error("artifact truncated"); count += read; }
			if (sha256Digest(block) !== value.blocks[offset / ARTIFACT_BLOCK_BYTES]) throw new Error("artifact integrity check failed");
		}
		return result;
	}
	readRange(reference: ArtifactReference, offset: number, length: number): Promise<Buffer> {
		return this.lifetime.serialize(async () => {
			const value = this.value(reference); this.range(reference, offset, length);
			return readChunkRange(offset, length, index => this.block(value, index));
		});
	}
	lend() {
		if (this.lifetime.sealed) throw new Error("artifact lease is closed");
		const loans: { readonly release: () => void }[] = [], transfer: ArtifactTransfer[] = [];
		const release = () => { for (const loan of loans) loan.release(); };
		try { for (const { reference, object, image } of this.values.values()) {
			if (image) transfer.push({ reference, bytes: Uint8Array.from(image) });
			else { const loan = object!.lend(); if (!loan) throw new Error("artifact object lease is closed"); loans.push(loan); transfer.push({ reference, object: loan.transfer }); }
		} return { transfer, release }; } catch (error) { release(); throw error; }
	}
	dispose(): Promise<void> { return this.lifetime.close(async () => { await this.lifetime.drain(); await Promise.allSettled([...this.values.values()].map(value => value.object?.dispose())); this.values.clear(); this.released?.(); }); }
	private value(reference: ArtifactReference) {
		const value = this.values.get(reference.digest);
		if (!value || value.reference.size !== reference.size) throw new Error(`artifact is outside the verified closure: ${reference.digest}`); return value;
	}
	private range(reference: ArtifactReference, offset: number, length: number): void {
		if (!isSafeCount(offset) || !isSafeCount(length) || offset + length > reference.size || length > this.maxReadBytes) throw new Error("artifact read budget exceeded");
	}
	private async block(value: ArtifactReadView["values"] extends Map<unknown, infer Value> ? Value : never, index: number, verify = true): Promise<Buffer> {
		const offset = index * ARTIFACT_BLOCK_BYTES, size = Math.min(ARTIFACT_BLOCK_BYTES, value.reference.size - offset), buffer = Buffer.allocUnsafe(size);
		if (value.image) value.image.copy(buffer, 0, offset, offset + size);
		else await value.object!.borrow(async handle => {
			for (let count = 0; count < size;) { const { bytesRead } = await handle.read!(buffer, count, size - count, offset + count); if (!bytesRead) throw new Error("artifact truncated"); count += bytesRead; }
		});
		if (verify && await sha256DigestAsync(buffer) !== value.blocks[index]) throw new Error("artifact integrity check failed"); return buffer;
	}
}

/** Certificate metadata and its indexes share one cross-process transaction; immutable payloads remain in the CAS. */
export class ProvenanceCertificateStore {
	readonly root: string;
	readonly artifacts: ArtifactCAS;
	private limitsValue = DEFAULT_PROVENANCE_STORE_LIMITS;
	private readonly gcIntervalMs: number;
	private readonly orphanGraceMs: number;
	private readonly acceptedTaints: readonly ProvenanceTaint[];
	private readonly maxReadBytes: number;
	private readonly reservedRoot: boolean;
	private readonly lifecycle = new RuntimeLifecycleLane();
	private readonly stopped = new AbortController();
	private collection?: Promise<ProvenanceStoreGCResult>;
	private maintenance?: NodeJS.Timeout;
	private readonly publication = new AsyncLocalStorage<{ database: DatabaseSync; touched: Set<string>; active: boolean }>();
	private control?: DatabaseSync;
	private reader?: SQLiteReadOwner;

	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.root = path.resolve(root); this.configure(options);
		this.reservedRoot = options.reservedRoot === true;
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"), (target, bytes) =>
			this.exclusive(() => publishImmutable(target, bytes, this.reservedRoot ? this.root : undefined), [this.objectKey(target)]));
		this.gcIntervalMs = nonNegativeNumber(options.gcIntervalMs, DEFAULT_GC_INTERVAL_MS);
		this.orphanGraceMs = nonNegativeNumber(options.orphanGraceMs, DEFAULT_ORPHAN_GRACE_MS);
		this.acceptedTaints = Object.freeze([...options.acceptedTaints ?? []]);
		this.maxReadBytes = positiveInteger(options.maxReadBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes);
	}

	get limits(): ProvenanceStoreLimits { return this.limitsValue; }
	get signal(): AbortSignal { return this.stopped.signal; }
	/** Own the complete read, including input proofs and their handle cleanup. */
	read<T>(operation: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal): Promise<T> {
		const execute = () => {
			const combined = AbortSignal.any([...(this.publication.getStore()?.active ? [] : [this.signal]), ...(signal ? [signal] : [])]);
			combined.throwIfAborted(); return operation(combined);
		};
		// An admitted publication owns its remaining finite reads through shutdown.
		return this.publication.getStore()?.active ? this.lifecycle.track(Promise.resolve().then(execute)) : this.lifecycle.admit(execute);
	}
	loadArtifacts(references: readonly ArtifactReference[], signal?: AbortSignal, retained?: VerifiedArtifactClosure): Promise<VerifiedArtifactClosure | undefined> {
		return this.read(signal => this.artifacts.load(references, signal, retained, { maxStoredBytes: this.limits.maxBytes }), signal);
	}
	/** A finite publication owns its payloads until sealing; other processes cannot collect between its writes. */
	withPublication<T>(operation: () => Promise<T>): Promise<T> { return this.exclusive(() => operation()); }
	configure(limits: Partial<ProvenanceStoreLimits>): void {
		this.limitsValue = Object.freeze({ maxCertificates: positiveInteger(limits.maxCertificates, this.limits.maxCertificates), maxBytes: positiveInteger(limits.maxBytes, this.limits.maxBytes) });
	}

	async put(certificate: ProcessProvenanceCertificate): Promise<boolean> {
		if (certificate.result.continuation) return false;
		const owned = parseProcessCertificate(certificate);
		if (!owned) throw new Error("invalid process provenance certificate");
		const references = referencedArtifacts(owned), touched = [`certificate:${owned.id}`, ...references.map(reference => this.objectKey(this.artifacts.artifactPath(reference.digest)))];
		return this.exclusive(async database => {
			await mapFilesystem(references, async reference => { if (!await this.artifacts.has(reference)) throw new Error(`certificate references missing artifact ${reference.digest}`); });
			const stored = decodeCertificate(database.prepare("SELECT value FROM certificates WHERE id = ?").get(owned.id)?.value);
			const valid = stored?.id === owned.id;
			if (!valid) writeCertificate(database, owned);
			return !valid;
		}, touched);
	}

	async get(id: Sha256Digest, signal?: AbortSignal): Promise<ProcessProvenanceCertificate | undefined> {
		digestHex(id);
		const value = (await this.query("SELECT value FROM certificates WHERE id = ?", [id], true, signal))[0]?.value, certificate = decodeCertificate(value);
		if (value !== undefined && certificate?.id !== id) throw new Error(`certificate integrity check failed for ${id}`);
		return certificate;
	}

	/** Reserve once inside the capture's publication. Clear and collection never reset the issuer. */
	issueProcessCheckpoint(): Promise<string> {
		return this.exclusive(async database => {
			const origin = await realpath(this.root);
			database.prepare("INSERT OR IGNORE INTO checkpoint_owner VALUES (1, ?, 0, ?)").run(randomUUID(), origin);
			const owner = database.prepare("SELECT issuer, sequence, origin FROM checkpoint_owner WHERE singleton=1").get()!;
			if (owner.origin !== origin || !isSafeCount(Number(owner.sequence) + 1)) throw new Error("checkpoint issuer is unavailable");
			const sequence = Number(owner.sequence) + 1, ticket = `${owner.issuer}:${sequence}`;
			database.prepare("UPDATE checkpoint_owner SET sequence=? WHERE singleton=1").run(sequence);
			database.prepare("INSERT INTO process_checkpoints(ticket, createdAt) VALUES (?, ?)").run(ticket, Date.now());
			return ticket;
		});
	}
	async putProcessCheckpoint(ticket: string, input: Omit<StoredProcessCheckpoint, "schema" | "id" | "createdAt" | "ticket">): Promise<StoredProcessCheckpoint> {
		const body = { ...input, schema: 1 as const, ticket, createdAt: Date.now() }, record = decodeProcessCheckpoint(stableStringify({ ...body, id: digestObject(body) }));
		if (!record) throw new Error("invalid process checkpoint");
		const references = checkpointArtifacts(record);
		return this.exclusive(async database => {
			const owner = database.prepare("SELECT issuer, origin FROM checkpoint_owner WHERE singleton=1").get();
			const issued = database.prepare("SELECT value FROM process_checkpoints WHERE ticket=?").get(ticket);
			if (owner?.origin !== await realpath(this.root) || !ticket.startsWith(`${owner.issuer}:`) || !issued || issued.value !== null)
				throw new Error("checkpoint ticket is no longer publishable");
			const closure = await this.loadArtifacts(references);
			if (!closure) throw new Error("checkpoint artifacts are unavailable");
			await closure.dispose();
			database.prepare("UPDATE process_checkpoints SET value=?, weakKey=?, executable=?, sessionID=?, createdAt=? WHERE ticket=?")
				.run(stableStringify(record), record.certificate.weakKey, record.certificate.prototype.executablePath, record.sessionID, record.createdAt, ticket);
			return record;
		}, [`checkpoint:${ticket}`, ...references.map(reference => this.objectKey(this.artifacts.artifactPath(reference.digest)))]);
	}
	async mayHaveProcessCheckpoints(sessionID: string, executablePath?: string, signal?: AbortSignal): Promise<boolean> {
		try { return !!(await this.query(`SELECT 1 FROM process_checkpoints WHERE sessionID=? AND claim IS NULL${executablePath ? " AND executable=?" : ""} LIMIT 1`,
			[sessionID, ...(executablePath ? [executablePath] : [])], false, signal)).length; } catch { return false; }
	}
	async *processCheckpoints(weakKey: Sha256Digest, executablePath: string, sessionID: string, signal?: AbortSignal): AsyncGenerator<StoredProcessCheckpoint> {
		const rows = await this.query("SELECT ticket, value FROM process_checkpoints WHERE weakKey=? AND executable=? AND sessionID=? AND claim IS NULL AND EXISTS (SELECT 1 FROM checkpoint_owner WHERE origin=?) ORDER BY createdAt DESC LIMIT ?",
			[weakKey, executablePath, sessionID, await realpath(this.root), this.limits.maxCertificates], false, signal);
		for (const row of rows) { signal?.throwIfAborted(); const record = decodeProcessCheckpoint(row.value); if (record?.ticket === row.ticket) yield record; }
	}
	/** Claim is durable before any native proposal. An unknown outcome stays claimed after a crash. */
	async claimProcessCheckpoint(ticket: string, certificate: Sha256Digest): Promise<((consumed: boolean) => Promise<void>) | undefined> {
		const claim = randomUUID();
		const acquired = await this.exclusive(async database => {
			const owner = database.prepare("SELECT origin FROM checkpoint_owner WHERE singleton=1").get();
			const row = database.prepare("SELECT value, claim FROM process_checkpoints WHERE ticket=?").get(ticket), record = decodeProcessCheckpoint(row?.value);
			if (owner?.origin !== await realpath(this.root) || !record || record.certificate.id !== certificate || row?.claim !== null) return false;
			return !!database.prepare("UPDATE process_checkpoints SET claim=? WHERE ticket=? AND claim IS NULL").run(claim, ticket).changes;
		}, [`checkpoint:${ticket}`]);
		if (!acquired) return;
		let settled = false;
		return async consumed => {
			if (settled) return; settled = true;
			await this.exclusive(async database => { database.prepare(consumed ? "DELETE FROM process_checkpoints WHERE ticket=? AND claim=?" :
				"UPDATE process_checkpoints SET claim=NULL WHERE ticket=? AND claim=?").run(ticket, claim); }, [`checkpoint:${ticket}`]);
		};
	}

	/** Input metadata shares certificate publication, retention, and crash recovery; it grants no execution authority. */
	async putInputVersion(payload: unknown, artifacts: readonly ArtifactReference[]): Promise<StoredInputVersion> {
		const body = { schema: 1 as const, createdAt: Date.now(), payload, artifacts }, id = digestObject(body);
		const record = decodeInputVersion(stableStringify({ ...body, id }));
		if (!record) throw new Error("invalid persistent input version");
		return this.exclusive(async database => {
			await mapFilesystem(record.artifacts, async reference => {
				if (!await this.artifacts.has(reference)) throw new Error(`input version references missing artifact ${reference.digest}`);
			});
			database.prepare("INSERT OR REPLACE INTO input_versions VALUES (?, ?)").run(id, stableStringify(record)); return record;
		}, [`input:${id}`, ...record.artifacts.map(reference => this.objectKey(this.artifacts.artifactPath(reference.digest)))]);
	}
	async getInputVersion(id: Sha256Digest, signal?: AbortSignal): Promise<StoredInputVersion | undefined> {
		digestHex(id);
		const value = (await this.query("SELECT value FROM input_versions WHERE id = ?", [id], false, signal))[0]?.value, record = decodeInputVersion(value);
		if (value !== undefined && record?.id !== id) throw new Error(`input version integrity check failed for ${id}`);
		return record;
	}
	/** Finite discovery hints, newest publication first; every row still needs current input and CAS validation. */
	async *candidatesInputVersions(limit: number, signal?: AbortSignal): AsyncGenerator<StoredInputVersion> {
		if (!Number.isSafeInteger(limit) || limit < 1 || limit > 65536) throw new Error("invalid input discovery limit");
		const candidates = await this.query("SELECT id FROM input_versions ORDER BY rowid DESC LIMIT ?", [limit], false, signal);
		for (const { id } of candidates) {
			signal?.throwIfAborted(); this.signal.throwIfAborted();
			if (!isSha256Digest(id)) continue;
			const record = await this.getInputVersion(id, signal).catch(() => {
				signal?.throwIfAborted(); this.signal.throwIfAborted(); return undefined;
			});
			signal?.throwIfAborted(); this.signal.throwIfAborted();
			if (record) yield record;
		}
	}

	async findByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): Promise<readonly ProcessProvenanceCertificate[]> {
		const certificates: ProcessProvenanceCertificate[] = [];
		for await (const certificate of this.candidatesByWeakKey(weakKey, executablePath, excludedCertificates)) certificates.push(certificate);
		return certificates;
	}

	/** Indexes only select candidates; every yielded certificate still verifies its complete identity. */
	async *candidatesByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>, signal?: AbortSignal): AsyncGenerator<ProcessProvenanceCertificate> {
		const candidates = await this.query("SELECT id FROM certificates WHERE weakKey = ? AND executable = ? ORDER BY createdAt DESC, id DESC", [weakKey, executablePath], true, signal);
		for (const { id } of candidates) {
			signal?.throwIfAborted();
			if (!isSha256Digest(id) || excludedCertificates?.has(id)) continue;
			const certificate = await this.get(id, signal).catch(() => undefined);
			if (certificate?.weakKey === weakKey) yield certificate;
		}
	}

	/** An unavailable optional cache is a miss; live handoffs have their own availability check. */
	async mayHaveCertificates(executablePath?: string, signal?: AbortSignal): Promise<boolean> {
		try { return Boolean((await this.query(executablePath === undefined ? "SELECT 1 FROM certificates LIMIT 1" : "SELECT 1 FROM certificates WHERE executable = ? LIMIT 1", executablePath === undefined ? [] : [executablePath], true, signal)).length); }
		catch { return false; }
	}

	/** Physical snapshots include uncommitted crash debris; a metadata revision cannot cache them. */
	async stats(_refresh = false): Promise<ProvenanceStoreStats> {
		const limits = this.limits;
		const inventory = await this.inventory(), referenced = new Set(inventory.roots.flatMap(record => record.references?.map(reference => reference.digest) ?? []));
		const totalBytes = inventory.objects.reduce((total, record) => total + record.bytes, 0), artifacts = inventory.objects.filter(record => record.path).length;
		return Object.freeze({ certificates: inventory.roots.filter(record => record.kind !== "input").length, inputVersions: inventory.roots.filter(record => record.kind === "input").length,
			artifacts, totalBytes, orphanArtifacts: artifacts - [...referenced].filter(digest => inventory.artifacts.has(digest)).length, overBudget: inventory.roots.length > limits.maxCertificates || totalBytes > limits.maxBytes });
	}

	gc(): Promise<ProvenanceStoreGCResult> {
		// Later publications are marked in the shared transaction log, so this finite inventory never restarts.
		return this.collection ??= this.publication.exit(() => this.lifecycle.admit(async () => {
			const revision = Number((await this.query("SELECT revision FROM changes WHERE path = ''", [], true))[0]?.revision ?? 0);
			return this.collect(await this.inventory(), revision);
		}).finally(() => { this.collection = undefined; }));
	}

	/** Flush and release the connection; subsequent operations may reopen it. */
	async drain(): Promise<void> {
		clearTimeout(this.maintenance); this.maintenance = undefined;
		await this.lifecycle.drain();
		if (this.lifecycle.sealed) return this.dispose();
		await this.lifecycle.serialize(() => this.closeDatabase());
	}
	dispose(): Promise<void> {
		this.stopped.abort(new Error("provenance store is closed"));
		clearTimeout(this.maintenance); this.maintenance = undefined;
		return this.lifecycle.close(async () => { await this.lifecycle.drain(); await this.artifacts.dispose(); await this.closeDatabase(); });
	}
	private async closeDatabase(): Promise<void> {
		clearTimeout(this.maintenance); this.maintenance = undefined;
		const reader = this.reader; this.reader = undefined;
		await reader?.dispose(); this.control?.close(); this.control = undefined;
	}
	private maintain(): void {
		if (!this.control || this.maintenance || this.lifecycle.sealed) return;
		this.publication.exit(() => { this.maintenance = setInterval(() => { void this.gc().catch(() => undefined); }, Math.max(100, this.gcIntervalMs)).unref(); });
	}

	async clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(async database => {
			const before = await this.inventory(database);
			await mapFilesystem(await this.payloadRoots(), root => rm(root, { recursive: true, force: true }));
			database.exec("DELETE FROM certificates; DELETE FROM input_versions; DELETE FROM process_checkpoints; DELETE FROM changes WHERE path <> ''");
			return { removedCertificates: before.roots.filter(record => record.kind !== "input").length, removedInputVersions: before.roots.filter(record => record.kind === "input").length,
				removedArtifacts: before.objects.filter(record => record.path).length, removedBytes: before.objects.reduce((total, record) => total + record.bytes, 0) };
		}, [".clear"]);
	}

	private async collect(inventory: StoreInventory, revision: number): Promise<ProvenanceStoreGCResult> {
		const retained = new Set<string>(), result = { removedCertificates: 0, removedInputVersions: 0, removedArtifacts: 0, removedBytes: 0 }, cutoff = Date.now() - this.orphanGraceMs;
		let retainedBytes = 0, retainedCertificates = 0;
		for (const record of [...inventory.roots].sort((left, right) => right.createdAt - left.createdAt)) {
			if (!record.references) continue;
			const references = record.references.map(reference => inventory.artifacts.get(reference.digest)?.bytes === reference.size ? inventory.artifacts.get(reference.digest) : undefined);
			if (references.some(reference => !reference)) continue;
			const addedBytes = record.bytes + references.reduce((total, reference) => total + (retained.has(reference!.key) ? 0 : reference!.bytes), 0);
			if (retainedCertificates >= this.limits.maxCertificates || retainedBytes + addedBytes > this.limits.maxBytes) continue;
			retainedBytes += addedBytes; retainedCertificates++; retained.add(record.key);
			for (const reference of references) retained.add(reference!.key);
		}
		// Each certificate (including its index) or payload is one transaction. Publishers can run between objects.
		for (const record of inventory.objects) if (!retained.has(record.key)) await this.exclusive(async database => {
			if ([".clear", record.key].some(key => changeRevision(database, key) > revision)) return;
			if (record.path) {
				const current = await stat(record.path).catch(error => { if (!missing(error)) throw error; });
				if (record.digest && (record.modifiedAt > cutoff || (current?.mtimeMs ?? 0) > cutoff)) return;
				if (current) {
					await rm(record.path, { force: true }); result.removedArtifacts++; result.removedBytes += current.size;
				}
			} else if (/^(certificate|input|checkpoint):/.test(record.key)) {
				const input = record.key.startsWith("input:"), checkpoint = record.key.startsWith("checkpoint:"), table = input ? "input_versions" : checkpoint ? "process_checkpoints" : "certificates";
				const removed = Number(database.prepare(`DELETE FROM ${table} WHERE ${checkpoint ? "ticket" : "id"} = ?`).run(record.key.slice(record.key.indexOf(":") + 1)).changes);
				if (input) result.removedInputVersions += removed; else result.removedCertificates += removed;
				result.removedBytes += removed ? record.bytes : 0;
			}
			database.prepare("DELETE FROM changes WHERE path = ?").run(record.key);
		});
		await mapFilesystem(inventory.retiredRoots, root => rm(root, { recursive: true, force: true }));
		return result;
	}

	private async inventory(control?: DatabaseSync): Promise<StoreInventory> {
		const read = (sql: string) => control ? Promise.resolve(control.prepare(sql).all()) : this.query(sql, [], true);
		const records: StoredRoot[] = (await read("SELECT 'certificate' AS kind, id, value FROM certificates UNION ALL SELECT 'input', id, value FROM input_versions UNION ALL SELECT 'checkpoint', ticket, value FROM process_checkpoints"))
			.map(({ kind: storedKind, id, value }) => {
				const kind = storedKind === "checkpoint" ? "checkpoint" : storedKind === "certificate" ? "certificate" : "input";
				if (kind === "checkpoint") {
					const record = decodeProcessCheckpoint(value);
					return { key: `checkpoint:${id}`, kind, bytes: Buffer.byteLength(String(value)), modifiedAt: 0, createdAt: record?.createdAt ?? 0,
						references: record?.ticket === id ? checkpointArtifacts(record) : undefined };
				}
				const record = kind === "certificate" ? decodeCertificate(value) : decodeInputVersion(value), valid = record?.id === id;
				const references = !valid ? undefined : kind === "input" ? (record as StoredInputVersion).artifacts
					: certificateReplayable(record as ProcessProvenanceCertificate, this.acceptedTaints) ? referencedArtifacts(record as ProcessProvenanceCertificate) : undefined;
				return { key: `${kind}:${id}`, kind, bytes: Buffer.byteLength(String(value)), modifiedAt: 0, createdAt: record?.createdAt ?? 0, references };
			});
		const keys = await read("SELECT path FROM changes WHERE path NOT IN ('', '.clear')");
		const roots = await this.payloadRoots(), paths = (await mapFilesystem(roots, filesUnder)).flat(), artifacts = new Map<Sha256Digest, StoredFile>();
		const objects: StoredFile[] = [...records, ...(await mapFilesystem(paths, async target => {
			const value = await stat(target).catch(error => { if (!missing(error)) throw error; }), key = this.objectKey(target), match = /^cas\/sha256\/([0-9a-f]{2})\/([0-9a-f]{62})$/.exec(key);
			if (!value) return [];
			const digest = match ? `sha256:${match[1]}${match[2]}` as Sha256Digest : undefined, record = { key, path: target, bytes: value.size, modifiedAt: value.mtimeMs, digest };
			if (digest) artifacts.set(digest, record); return [record];
		})).flat()];
		const present = new Set(objects.map(record => record.key));
		for (const { path: key } of keys) if (typeof key === "string" && !present.has(key)) objects.push({ key, bytes: 0, modifiedAt: 0 });
		return { roots: records, artifacts, objects, retiredRoots: roots.slice(1) };
	}
	private async payloadRoots(): Promise<readonly string[]> {
		return [this.artifacts.root, ...(await readdir(this.root)).filter(name => /^\.clear-[0-9a-f-]{36}$/.test(name)).map(name => path.join(this.root, name))];
	}

	private objectKey(target: string): string { return path.relative(this.root, target).split(path.sep).join("/"); }
	/** Every committed read owns fresh SQLite state. Legacy migration remains an explicit writer operation. */
	private query(sql: string, parameters: SQLInputValue[], migrate: boolean, signal?: AbortSignal): Promise<Record<string, SQLOutputValue>[]> {
		return this.read(async signal => {
			const publication = this.publication.getStore();
			if (publication?.active) return publication.database.prepare(sql).all(...parameters);
			const reader = this.reader ??= new SQLiteReadOwner(this.root);
			let rows = await reader.read(sql, parameters, this.maxReadBytes, signal);
			if (rows === undefined && migrate) {
				await this.exclusive(async () => {}, [], false, signal);
				rows = await reader.read(sql, parameters, this.maxReadBytes, signal);
				if (rows === undefined) throw new Error("provenance store metadata is unavailable");
			}
			signal.throwIfAborted(); return rows ?? [];
		}, signal);
	}
	private async exclusive<T>(operation: (database: DatabaseSync) => Promise<T>, touched: readonly string[] = [], mutate = true, signal?: AbortSignal): Promise<T> {
		const publication = this.publication.getStore();
		if (publication?.active) {
			const value = await operation(publication.database);
			for (const key of touched) publication.touched.add(key);
			return value;
		}
		const admission = AbortSignal.any([this.stopped.signal, AbortSignal.timeout(1_000), ...(signal ? [signal] : [])]);
		let admitted = false;
		const task = this.lifecycle.serialize(async () => {
			admission.throwIfAborted(); const database = this.control ??= await this.open(admission);
			this.maintain();
			if (!mutate) { admission.throwIfAborted(); return operation(database); }
			await sqliteReady(() => { database.exec("BEGIN IMMEDIATE"); admitted = true; }, admission);
			const publication = { database, touched: new Set(touched), active: true };
			// Cancellation ends at admission. Finish owned filesystem work before releasing the transaction.
			try {
				const value = await this.publication.run(publication, () => operation(database));
				const version = changeRevision(database) + 1, update = database.prepare("INSERT INTO changes VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET revision=excluded.revision");
				for (const key of ["", ...publication.touched]) update.run(key, version);
				database.exec("COMMIT"); return value;
			} catch (error) { if (database.isTransaction) database.exec("ROLLBACK"); throw error; }
			finally { publication.active = false; }
		});
		const result = await waitForCompletion(task, admission);
		if (result.status !== "completed") { if (admitted) return task; throw admission.reason; }
		return result.value;
	}

	private async open(signal: AbortSignal): Promise<DatabaseSync> {
		if (!this.reservedRoot) await mkdir(this.root, { recursive: true });
		const database = new DatabaseSync(path.join(this.root, "metadata.sqlite"));
		try {
			database.exec("PRAGMA synchronous=NORMAL");
			const version = Number(await sqliteReady(() => database.prepare("PRAGMA user_version").get()?.user_version, signal));
			if (version === 4) return database;
			if (version > 4) throw new Error(`unsupported provenance store version ${version}`);
			await sqliteReady(() => database.exec("PRAGMA auto_vacuum=FULL; PRAGMA journal_mode=WAL"), signal);
			await sqliteReady(() => database.exec("BEGIN IMMEDIATE"), signal);
			database.exec("CREATE TABLE IF NOT EXISTS changes(path TEXT PRIMARY KEY, revision INTEGER NOT NULL); INSERT OR IGNORE INTO changes VALUES ('', 0); CREATE TABLE IF NOT EXISTS certificates(id TEXT PRIMARY KEY, value TEXT NOT NULL, weakKey TEXT NOT NULL, executable TEXT NOT NULL, createdAt REAL NOT NULL); CREATE INDEX IF NOT EXISTS candidates ON certificates(executable, weakKey, createdAt DESC, id DESC)");
			if (database.prepare("PRAGMA user_version").get()?.user_version === 0) {
				const root = path.join(this.root, "certificates");
				for (const file of await filesUnder(root)) {
					const value = await readFile(file, "utf8").catch(() => undefined), certificate = decodeCertificate(value);
					if (certificate && path.relative(root, file).split(path.sep).join("") === `${digestHex(certificate.id)}.json`) writeCertificate(database, certificate);
				}
				database.exec("PRAGMA user_version=1");
			}
			database.exec("COMMIT"); // Imported metadata is durable before any legacy file is removed.
			if (database.prepare("PRAGMA user_version").get()?.user_version === 1) {
				await sqliteReady(() => database.exec("BEGIN IMMEDIATE"), signal);
				await mapFilesystem(["certificates", "indexes"], segment => rm(path.join(this.root, segment), { recursive: true, force: true }));
				database.exec("PRAGMA user_version=2; COMMIT");
			}
			await sqliteReady(() => database.exec("BEGIN IMMEDIATE"), signal);
			database.exec("CREATE TABLE IF NOT EXISTS input_versions(id TEXT PRIMARY KEY, value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS checkpoint_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1), issuer TEXT NOT NULL, sequence INTEGER NOT NULL, origin TEXT NOT NULL); CREATE TABLE IF NOT EXISTS process_checkpoints(ticket TEXT PRIMARY KEY, value TEXT, weakKey TEXT, executable TEXT, sessionID TEXT, createdAt REAL NOT NULL, claim TEXT); CREATE INDEX IF NOT EXISTS checkpoint_candidates ON process_checkpoints(sessionID, executable, weakKey, createdAt DESC); PRAGMA user_version=4; COMMIT");
			return database;
		} catch (error) { database.close(); throw error; }
	}
}

interface StoredFile { readonly key: string; readonly path?: string; readonly bytes: number; readonly modifiedAt: number; readonly digest?: Sha256Digest; }
interface StoredRoot extends StoredFile { readonly kind: "certificate" | "input" | "checkpoint"; readonly references?: readonly ArtifactReference[]; readonly createdAt: number; }
interface StoreInventory { readonly roots: readonly StoredRoot[]; readonly artifacts: ReadonlyMap<Sha256Digest, StoredFile>; readonly objects: readonly StoredFile[]; readonly retiredRoots: readonly string[]; }

async function filesUnder(root: string): Promise<readonly string[]> {
	try { return (await readdir(root, { recursive: true, withFileTypes: true })).filter(entry => entry.isFile()).map(entry => path.join(entry.parentPath, entry.name)); }
	catch (error) { if (missing(error)) return []; throw error; }
}
function changeRevision(database: DatabaseSync, key = ""): number { return Number(database.prepare("SELECT revision FROM changes WHERE path = ?").get(key)?.revision ?? 0); }
function writeCertificate(database: DatabaseSync, certificate: ProcessProvenanceCertificate): void {
	database.prepare("INSERT OR REPLACE INTO certificates VALUES (?, ?, ?, ?, ?)").run(certificate.id, stableStringify(certificate), certificate.weakKey, certificate.prototype.executablePath, certificate.createdAt);
}
/** Native transactions recover on process exit. Busy acquisition yields; filesystem work stays asynchronous. */
async function sqliteReady<T>(run: () => T, signal: AbortSignal): Promise<T> {
	for (let wait = 1; ; wait = Math.min(wait * 2, 16)) { signal.throwIfAborted(); try { return run(); } catch (error) {
		if (![5, 6].includes(Number((error as { errcode?: number }).errcode) & 0xff)) throw error; await delay(wait);
	} }
}
/** Preserve verified objects; a new complete publication atomically replaces a corrupt copy. */
async function publishImmutable(target: string, bytes: Uint8Array, reservedRoot?: string): Promise<boolean> {
	if ((await readOptional(target, bytes.byteLength))?.equals(bytes)) {
		const now = new Date(); // Referencing the same object restarts its orphan grace.
		try { await utimes(target, now, now); return false; } catch (error) { if (!missing(error)) throw error; }
	}
	if (!reservedRoot) await mkdir(path.dirname(target), { recursive: true });
	else {
		const relative = path.relative(reservedRoot, path.dirname(target));
		if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new Error("artifact publication escapes reserved cache");
		let directory = reservedRoot;
		for (const segment of relative.split(path.sep)) {
			directory = path.join(directory, segment);
			await mkdir(directory).catch(error => { if (error.code !== "EEXIST") throw error; });
		}
	}
	const temporary = path.join(path.dirname(target), `.${path.basename(target)}.${process.pid}.${randomUUID()}.tmp`);
	// Exclusive creation grants cleanup ownership; an existing temporary belongs to another publisher.
	const file = await open(temporary, "wx");
	try {
		try { await writeFile(file, bytes); } finally { await file.close(); }
		await rename(temporary, target); return true;
	} finally {
		await rm(temporary, { force: true });
	}
}

function decodeCertificate(value: unknown): ProcessProvenanceCertificate | undefined {
	try { return typeof value === "string" ? parseProcessCertificate(JSON.parse(value)) : undefined; } catch { return undefined; }
}

function checkpointArtifacts(record: StoredProcessCheckpoint): readonly ArtifactReference[] { return [...referencedArtifacts(record.certificate), record.image]; }
function decodeProcessCheckpoint(value: unknown): StoredProcessCheckpoint | undefined {
	try {
		if (typeof value !== "string" || Buffer.byteLength(value) > 8 * 1024 * 1024) return;
		const record = JSON.parse(value) as StoredProcessCheckpoint, { id, ...body } = record, certificate = parseProcessCertificate(record.certificate);
		if (record.schema !== 1 || !isSha256Digest(id) || digestObject(body) !== id || !Number.isFinite(record.createdAt) ||
			!isSha256Digest(record.context) || !/^[0-9a-f-]{36}:[1-9][0-9]*$/.test(record.ticket) || typeof record.sessionID !== "string" || !record.sessionID || record.sessionID.length > 4096 ||
			![record.sourceRoot, record.physicalRoot].every(value => typeof value === "string" && path.isAbsolute(value) && !value.includes("\0")) ||
			!isSafeCount(record.producer.pid, 1) || !/^[0-9]+$/.test(record.producer.start) || !normalizeTimelineComputation(record.computation) ||
			!certificate?.result.continuation || certificate.result.continuation.imageDigest !== record.image.digest ||
			certificate.result.continuation.imageBytes !== record.image.size) return;
		validateArtifact(record.image); return Object.freeze({ ...record, certificate });
	} catch { return; }
}

function decodeInputVersion(value: unknown): StoredInputVersion | undefined {
	try {
		if (typeof value !== "string" || Buffer.byteLength(value) > 8 * 1024 * 1024) return;
		const record = JSON.parse(value) as StoredInputVersion, { id, ...body } = record;
		if (record.schema !== 1 || !Number.isFinite(record.createdAt) || !isSha256Digest(id) || digestObject(body) !== id ||
			!Array.isArray(record.artifacts) || record.artifacts.length > 65536) return;
		const sizes = new Map<Sha256Digest, number>(); for (const reference of record.artifacts) validateArtifact(reference, sizes);
		if (sizes.size !== record.artifacts.length) return;
		return record;
	} catch { return undefined; }
}

/** Named cache objects may alias source inputs. The returned reader retains the verified noatime mount. */
async function openArtifactFile(target: string): Promise<FileHandle | undefined> {
	return withFilesystemReadView(undefined, async view => {
		const opened = await openFilesystemFile(target, false, view).catch((error): undefined => { if (!missing(error)) throw error; });
		if (!opened) return;
		try { await opened.binding?.close(); return opened.handle; }
		catch (error) { await opened.handle.close(); throw error; }
	}, async handle => { await handle?.close(); });
}

/** One extra byte proves a size mismatch without loading an arbitrarily enlarged corrupt object. */
async function readOptional(target: string, size: number, signal?: AbortSignal): Promise<Buffer | undefined> {
	signal?.throwIfAborted();
	const file = await openArtifactFile(target);
	if (!file) return;
	try {
		const bytes = Buffer.allocUnsafe(size + 1); let offset = 0;
		while (offset < bytes.length) {
			signal?.throwIfAborted();
			const { bytesRead } = await file.read(bytes, offset, bytes.length - offset, offset);
			if (!bytesRead) break; offset += bytesRead;
		}
		signal?.throwIfAborted(); return bytes.subarray(0, offset);
	} finally { await file.close(); }
}

async function verifyArtifact(reference: ArtifactReference, bytes?: Buffer): Promise<Buffer | undefined> {
	if (bytes && (bytes.byteLength !== reference.size || await sha256DigestAsync(bytes) !== reference.digest)) throw new Error(`artifact integrity check failed for ${reference.digest}`);
	return bytes;
}

function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}
