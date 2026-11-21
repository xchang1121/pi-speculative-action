import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import { readSync } from "node:fs";
import {
	mkdir,
	open,
	readdir,
	rename,
	rm,
	stat,
	utimes,
	writeFile,
	type FileHandle,
} from "node:fs/promises";
import path from "node:path";
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from "node:sqlite";
import { setTimeout as delay } from "node:timers/promises";
import { serialize } from "node:v8";
import {
	currentSpeculativeConfiguration,
	nonNegativeNumber,
	positiveInteger,
	speculativeConfig,
} from "./config-schema.ts";
import {
	CONTENT_CHUNK_BYTES as ARTIFACT_BLOCK_BYTES,
	validateArtifact,
	type ArtifactReference,
} from "./content-chunks.ts";
import {
	FilesystemObjectLease,
	mapFilesystem,
	openArtifactImage,
	openFilesystemFile,
	sealArtifactImage,
	withFilesystemReadView,
	type FilesystemObjectHandle,
	type FilesystemObjectTransfer,
} from "./filesystem-evidence.ts";
import {
	hasErrorCode,
	ignoreMissing,
	digestObject,
	filesystemPathKey,
	isSafeCount,
	isSha256Digest,
	isMissing as missing,
	sha256Digest,
	sha256DigestAsync,
	stableStringify,
	type Sha256Digest,
} from "./primitives.ts";
import {
	certificateReplayable,
	parseProcessCertificate,
	referencedArtifacts,
	type ProcessProvenanceCertificate,
	type ProvenanceTaint,
} from "./provenance-certificate.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
/** maxCertificates is the shared metadata-record limit, including persistent input versions. */
export interface ProvenanceStoreLimits {
	readonly maxCertificates: number;
	readonly maxBytes: number;
}
export interface ProvenanceStoreOptions extends Partial<ProvenanceStoreLimits> {
	readonly gcIntervalMs?: number;
	readonly orphanGraceMs?: number;
	/** Configuration already reserved this root; runtime publication must never recreate its ancestors. */
	readonly reservedRoot?: boolean;
	/** Independent of retention: over-budget stores must still be readable for collection. */
	readonly maxReadBytes?: number;
	/** Taints the publisher accepted: retention and stats keep what its lookups can still replay. */
	readonly acceptedTaints?: readonly ProvenanceTaint[];
}
export interface ProvenanceStoreStats {
	readonly certificates: number;
	readonly artifacts: number;
	readonly orphanArtifacts: number;
	readonly inputVersions: number;
	readonly totalBytes: number;
	readonly overBudget: boolean;
}
export interface ProvenanceStoreGCResult {
	readonly removedCertificates: number;
	readonly removedArtifacts: number;
	readonly removedBytes: number;
	readonly removedInputVersions: number;
}
export interface StoredInputVersion {
	readonly id: Sha256Digest;
	readonly createdAt: number;
	readonly payload: unknown;
	readonly artifacts: readonly ArtifactReference[];
}
const metadataDDL = {
	changes: "CREATE TABLE changes(path TEXT PRIMARY KEY, revision INTEGER NOT NULL)",
	certificates: "CREATE TABLE certificates(id TEXT PRIMARY KEY, value TEXT NOT NULL, weakKey TEXT NOT NULL, executable TEXT NOT NULL, createdAt REAL NOT NULL)",
	candidates: "CREATE INDEX candidates ON certificates(executable, weakKey, createdAt DESC, id DESC)",
	input_versions: "CREATE TABLE input_versions(id TEXT PRIMARY KEY, root TEXT NOT NULL, value TEXT NOT NULL)",
	input_versions_root: "CREATE INDEX input_versions_root ON input_versions(root)",
	input_preparations: `CREATE TABLE input_preparations(
		root TEXT NOT NULL, binding TEXT NOT NULL, prep_key TEXT NOT NULL,
		input_id TEXT NOT NULL REFERENCES input_versions(id) ON DELETE CASCADE, updated_at INTEGER NOT NULL,
		PRIMARY KEY (root, binding, prep_key, input_id)
	) WITHOUT ROWID`,
	input_preparations_recent: "CREATE INDEX input_preparations_recent ON input_preparations(root, binding, prep_key, updated_at DESC, input_id DESC)",
	input_preparations_owner: "CREATE INDEX input_preparations_owner ON input_preparations(input_id)",
};
/** An empty database may be initialized. Any existing schema must match in full. */
function hasMetadataSchema(database: DatabaseSync): boolean {
	const objects = database.prepare("SELECT name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").all();
	if (!objects.length) return false;
	const matches = objects.length === Object.keys(metadataDDL).length && objects.every(({ name, sql }) =>
		Object.hasOwn(metadataDDL, String(name)) && metadataDDL[name as keyof typeof metadataDDL] === sql
	);
	if (!matches)
		throw Object.assign(new Error("incompatible provenance store schema"), { code: "ERR_PROVENANCE_SCHEMA" });
	return true;
}
/** Immutable effects; use store.artifacts to participate in a shared store's publication and retention lifecycle. */
export class ArtifactCAS {
	readonly root: string;
	private readonly publish: typeof publishImmutable;
	private readonly readers = new Set<ArtifactReadView>();
	private readonly capture = new AsyncLocalStorage<ArtifactReadView>();
	constructor(root: string, publish = publishImmutable) {
		this.root = path.resolve(root);
		this.publish = publish;
	}
	get capturing(): boolean {
		return !!this.capture.getStore();
	}
	async put(value: string | Uint8Array): Promise<ArtifactReference> {
		const bytes = Buffer.from(value);
		const reference = Object.freeze({
			digest: await sha256DigestAsync(bytes),
			size: bytes.byteLength,
		});
		const target = this.artifactPath(reference.digest);
		const captured = this.capture.getStore();
		if (captured) await captured.add(reference, undefined, { reference, bytes });
		else await this.publish(target, bytes);
		return reference;
	}
	/** A producer owns its versions directly. Only an accepted persistent root writes them to history. */
	async withCapture<T>(operation: () => Promise<T>, limits: Partial<ArtifactReadLimits>): Promise<T> {
		if (this.capture.getStore()) return operation();
		const view = new ArtifactReadView(this, limits);
		try {
			return await this.capture.run(view, operation);
		} finally {
			await view.dispose();
		}
	}
	async materialize(
		references: readonly ArtifactReference[],
		source = this.capture.getStore(),
	): Promise<void> {
		for (const reference of new Map(references.map((reference) => [reference.digest, reference])).values())
			if (source?.has(reference)) {
				const blocks: Buffer[] = [];
				for (let offset = 0; offset < reference.size; offset += ARTIFACT_BLOCK_BYTES)
					blocks.push(
						await source.readRange(
							reference,
							offset,
							Math.min(ARTIFACT_BLOCK_BYTES, reference.size - offset),
						),
					);
				await this.publish(this.artifactPath(reference.digest), Buffer.concat(blocks, reference.size));
			}
	}
	async share(references: readonly ArtifactReference[], source: ArtifactReadView): Promise<void> {
		const captured = this.capture.getStore();
		if (captured) {
			if (!(await captured.restore(references, source))) throw new Error("owned artifact is unavailable");
		} else await this.materialize(references, source);
	}
	async get(reference: ArtifactReference, signal?: AbortSignal): Promise<Buffer | undefined> {
		validateArtifact(reference);
		return verifyArtifact(
			reference,
			await readOptional(this.artifactPath(reference.digest), reference.size, signal),
		);
	}
	/** Availability hint only; publication and adoption verify content. */
	async has(reference: ArtifactReference): Promise<boolean> {
		const info = await withFilesystemReadView(undefined, (view) =>
			view.stat(this.artifactPath(reference.digest), true).catch((error): undefined => {
				if (!missing(error)) throw error;
			}),
		);
		return info?.isFile() === true && info.size === BigInt(reference.size);
	}
	/** Load and integrity-check a complete effect closure before any replay side effect begins. */
	async load(
		references: readonly ArtifactReference[],
		signal?: AbortSignal,
		retained?: ArtifactSource,
		limits?: Partial<ArtifactReadLimits>,
	): Promise<VerifiedArtifactClosure | undefined> {
		const view = new ArtifactReadView(this, limits, () => this.readers.delete(view));
		let complete = false;
		this.readers.add(view);
		try {
			complete = await view.restore(references, retained ?? this.capture.getStore(), signal);
			return complete ? view : undefined;
		} finally {
			if (!complete) await view.dispose();
		}
	}
	async dispose(): Promise<void> {
		await Promise.all([...this.readers].map((view) => view.dispose()));
	}
	artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest);
		return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}
export interface VerifiedArtifactClosure {
	readonly artifacts: number;
	readonly bytes: number;
	/** Each consumer owns its copy; callbacks cannot mutate a retained result's verified bytes. */
	readonly read: (reference: ArtifactReference) => Buffer;
	readonly readRange: (reference: ArtifactReference, offset: number, length: number) => Promise<Buffer>;
	readonly memoryBytes: number;
	readonly lend: (retain?: boolean) => {
		readonly transfer: readonly ArtifactTransfer[];
		readonly release: () => void;
	};
	readonly dispose: () => Promise<void>;
}
export type ArtifactTransfer = {
	readonly reference: ArtifactReference;
} & (
	| {
			readonly object: FilesystemObjectTransfer;
	  }
	| {
			readonly bytes: Uint8Array;
	  }
);
export type ArtifactSource = VerifiedArtifactClosure | readonly ArtifactTransfer[];
interface ArtifactReadLimits {
	readonly maxReadBytes: number;
	readonly maxStoredBytes: number;
	readonly maxOpenChunks: number;
}
/** One verified content lease, shared by effect closures and input chunks. Linux owns objects; other adapters retain their existing byte images. */
export class ArtifactReadView implements VerifiedArtifactClosure {
	private storedBytes = 0;
	private descriptors = 0;
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly values = new Map<
		Sha256Digest,
		{
			readonly reference: ArtifactReference;
			readonly object?: FilesystemObjectLease;
			readonly image?: Buffer;
			owners: number;
		}
	>();
	private readonly cas: ArtifactCAS;
	private readonly released?: () => void;
	private readonly limits: ArtifactReadLimits;
	constructor(cas: ArtifactCAS, limits: Partial<ArtifactReadLimits> = {}, released?: () => void) {
		this.cas = cas;
		this.released = released;
		this.limits = Object.freeze({
			maxReadBytes: limits.maxReadBytes ?? speculativeConfig().filesystem.maxReadBytes,
			maxStoredBytes: limits.maxStoredBytes ?? speculativeConfig().filesystem.maxStoredBytes,
			maxOpenChunks: limits.maxOpenChunks ?? speculativeConfig().filesystem.maxOpenChunks,
		});
		if (Object.values(this.limits).some((value) => !isSafeCount(value)))
			throw new Error("invalid artifact lease budget");
	}
	get maxReadBytes(): number {
		return this.limits.maxReadBytes;
	}
	get artifacts(): number {
		return this.values.size;
	}
	get bytes(): number {
		return this.storedBytes;
	}
	get memoryBytes(): number {
		return [...this.values.values()].reduce(
			(sum, { reference, image, owners }) =>
				sum + serialize({ reference, owners }).byteLength + (image?.length ?? 0),
			0,
		);
	}
	has(reference: ArtifactReference): boolean {
		return this.values.get(reference.digest)?.reference.size === reference.size;
	}
	checkBudget(reference: ArtifactReference): void {
		validateArtifact(reference);
		if (!this.has(reference) && this.bytes + reference.size > this.limits.maxStoredBytes)
			throw Object.assign(new Error("artifact stored-byte budget exceeded"), { code: "ERR_ARTIFACT_LEASE_BUDGET" });
	}
	async restore(
		references: readonly ArtifactReference[],
		transfers?: ArtifactSource,
		signal?: AbortSignal,
	): Promise<boolean> {
		if (transfers && !(transfers instanceof ArtifactReadView) && "lend" in transfers) {
			const loan = transfers.lend();
			try {
				return await this.restore(references, loan.transfer, signal);
			} finally {
				loan.release();
			}
		}
		const sizes = new Map<Sha256Digest, number>();
		for (const reference of references) validateArtifact(reference, sizes);
		const source = transfers instanceof ArtifactReadView ? transfers : undefined;
		const supplied =
			transfers && !source
				? new Map((transfers as readonly ArtifactTransfer[]).map((value) => [value.reference.digest, value]))
				: undefined;
		if (supplied && supplied.size !== (transfers as readonly ArtifactTransfer[]).length)
			throw new Error("duplicate artifact transfer");
		for (const [digest, size] of sizes) {
			const retained = supplied?.get(digest);
			if ((supplied && !retained) || !(await this.add({ digest, size }, signal, source ?? retained)))
				return false;
		}
		signal?.throwIfAborted();
		return true;
	}
	add(
		reference: ArtifactReference,
		signal?: AbortSignal,
		supplied?: ArtifactTransfer | ArtifactReadView,
	): Promise<boolean> {
		return this.lifetime.serialize(async () => {
			signal?.throwIfAborted();
			validateArtifact(reference);
			if (supplied instanceof ArtifactReadView && supplied.lifetime.sealed)
				throw new Error("artifact lease is closed");
			if (this.values.has(reference.digest)) {
				if (!this.has(reference)) throw new Error("conflicting artifact size");
				return true;
			}
			this.checkBudget(reference);
			if (supplied instanceof ArtifactReadView) {
				if (supplied.has(reference)) {
					const value = supplied.value(reference);
					if (!value.object || this.descriptors < this.limits.maxOpenChunks) {
						value.owners++;
						if (value.object) this.descriptors++;
						this.values.set(reference.digest, value);
						this.storedBytes += reference.size;
						return true;
					}
					const bytes = Buffer.allocUnsafe(reference.size);
					await value.object.borrow((handle) => readArtifactRange(handle, bytes, 0, signal));
					supplied = { reference, bytes };
				} else {
					supplied = undefined;
				}
			}
			if (
				supplied &&
				(supplied.reference.digest !== reference.digest || supplied.reference.size !== reference.size)
			)
				throw new Error("artifact transfer identity mismatch");
			let handle: FilesystemObjectHandle | undefined;
			let snapshot: FileHandle | undefined;
			try {
				if (supplied && "object" in supplied)
					throw new Error("descriptor transfers are unsupported; use CAS artifacts");
				const pin = process.platform === "linux" && this.descriptors < this.limits.maxOpenChunks;
				let image =
					supplied && "bytes" in supplied
						? Buffer.from(supplied.bytes)
						: !supplied && !pin
							? await readOptional(this.cas.artifactPath(reference.digest), reference.size, signal)
							: undefined;
				if (!image && pin)
					handle = await openArtifactFile(this.cas.artifactPath(reference.digest));
				if (!image && !handle) return false;
				const stat = handle && (await handle.stat({ bigint: true }));
				if (
					(image && image.length !== reference.size) ||
					(stat && (!stat.isFile() || stat.size !== BigInt(reference.size)))
				)
					throw new Error("artifact integrity check failed: length");
				if (pin) snapshot = await openArtifactImage();
				const hash = createHash("sha256");
				for (let offset = 0; offset < reference.size; offset += ARTIFACT_BLOCK_BYTES) {
					signal?.throwIfAborted();
					const block = image
						? image.subarray(offset, offset + ARTIFACT_BLOCK_BYTES)
						: Buffer.allocUnsafe(Math.min(ARTIFACT_BLOCK_BYTES, reference.size - offset));
					if (!image) await readArtifactRange(handle!, block, offset, signal);
					hash.update(block);
					await snapshot?.writeFile(block);
				}
				if (`sha256:${hash.digest("hex")}` !== reference.digest)
					throw new Error(`artifact integrity check failed for ${reference.digest}`);
				if (snapshot) {
					if (!sealArtifactImage(snapshot.fd, true)) throw new Error("artifact image sealing failed");
					await handle?.close();
					handle = snapshot;
					snapshot = undefined;
					image = undefined;
				}
				signal?.throwIfAborted();
				const object = handle && new FilesystemObjectLease(handle, await handle.stat({ bigint: true }), true);
				if (object) this.descriptors++;
				this.values.set(reference.digest, {
					reference: Object.freeze({ ...reference }),
					object,
					image,
					owners: 1,
				});
				this.storedBytes += reference.size;
				handle = undefined;
				return true;
			} finally {
				try {
					await snapshot?.close();
				} finally {
					await handle?.close();
				}
			}
		});
	}
	read(reference: ArtifactReference): Buffer {
		if (this.lifetime.sealed) throw new Error("artifact lease is closed");
		const value = this.value(reference);
		const result = this.range(reference, 0, reference.size);
		if (value.image) value.image.copy(result);
		else
			for (let count = 0; count < result.length;) {
				const read = readSync(value.object!.handle.fd, result, count, result.length - count, count);
				if (!read) throw new Error("artifact truncated");
				count += read;
			}
		return result;
	}
	readRange(reference: ArtifactReference, offset: number, length: number): Promise<Buffer> {
		return this.lifetime.serialize(async () => {
			const value = this.value(reference);
			const result = this.range(reference, offset, length);
			if (value.image) value.image.copy(result, 0, offset, offset + length);
			else await readArtifactRange(value.object!.handle, result, offset);
			return result;
		});
	}
	lend(_retain = true) {
		const transfer: ArtifactTransfer[] = [...this.values.values()].map(({ reference }) => ({
			reference,
			bytes: this.read(reference),
		}));
		return { transfer, release: () => {} };
	}
	dispose(): Promise<void> {
		return this.lifetime.dispose(async () => {
			await Promise.all(
				[...this.values.values()].map((value) => (--value.owners ? undefined : value.object?.dispose())),
			);
			this.values.clear();
			this.descriptors = 0;
			this.storedBytes = 0;
			this.released?.();
		});
	}
	private value(reference: ArtifactReference) {
		const value = this.values.get(reference.digest);
		if (!value || value.reference.size !== reference.size)
			throw new Error(`artifact is outside the verified closure: ${reference.digest}`);
		return value;
	}
	private range(reference: ArtifactReference, offset: number, length: number): Buffer {
		if (
			!isSafeCount(offset) ||
			!isSafeCount(length) ||
			offset + length > reference.size ||
			length > this.maxReadBytes
		)
			throw new Error("artifact read budget exceeded");
		return Buffer.allocUnsafe(length);
	}
}
/** Certificate metadata and its indexes share one cross-process transaction; immutable payloads remain in the CAS. */
export class ProvenanceCertificateStore {
	readonly configuration = currentSpeculativeConfiguration();
	readonly root: string;
	readonly artifacts: ArtifactCAS;
	private limitsValue: Partial<ProvenanceStoreLimits> = {};
	private readonly options: ProvenanceStoreOptions;
	private readonly acceptedTaints?: readonly ProvenanceTaint[];
	private readonly reservedRoot: boolean;
	private readonly lifecycle = new RuntimeLifecycleLane();
	private readonly stopped = new AbortController();
	private collection?: Promise<ProvenanceStoreGCResult>;
	private maintenance?: NodeJS.Timeout;
	private maintenanceIntervalMs?: number;
	private readonly publication = new AsyncLocalStorage<{
		database: DatabaseSync;
		touched: Set<string>;
		verifiedPaths: Set<string>;
		statements: Map<string, ReturnType<DatabaseSync["prepare"]>>;
		active: boolean;
	}>();
	private control?: DatabaseSync;
	private readonlyDb?: {
		readonly db: DatabaseSync;
		readonly dev: bigint;
		readonly ino: bigint;
	};
	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.options = options;
		this.root = path.resolve(root);
		this.configure(options);
		this.reservedRoot = options.reservedRoot === true;
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"), (target, bytes) =>
			this.exclusive(
				async () => {
					const created = await publishImmutable(target, bytes, this.reservedRoot ? this.root : undefined);
					this.publication.getStore()?.verifiedPaths.add(target);
					return created;
				},
				[this.objectKey(target)],
			),
		);
		this.acceptedTaints = options.acceptedTaints && Object.freeze([...options.acceptedTaints]);
		this.configuration.bind(this);
	}
	get limits(): ProvenanceStoreLimits {
		const defaults = this.configuration.current.storage;
		return {
			maxCertificates: this.limitsValue.maxCertificates ?? defaults.maxCertificates,
			maxBytes: this.limitsValue.maxBytes ?? defaults.maxBytes,
		};
	}
	private get gcIntervalMs(): number {
		return nonNegativeNumber(this.options.gcIntervalMs, this.configuration.current.storage.gcIntervalMs);
	}
	private get orphanGraceMs(): number {
		return nonNegativeNumber(this.options.orphanGraceMs, this.configuration.current.storage.orphanGraceMs);
	}
	private get maxReadBytes(): number {
		return positiveInteger(this.options.maxReadBytes, this.configuration.current.storage.maxBytes);
	}
	get signal(): AbortSignal {
		return this.stopped.signal;
	}
	/** Own the complete read, including input proofs and their handle cleanup. */
	read<T>(operation: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal): Promise<T> {
		const execute = () => {
			const combined = AbortSignal.any([
				...(this.publication.getStore()?.active ? [] : [this.signal]),
				...(signal ? [signal] : []),
			]);
			combined.throwIfAborted();
			return operation(combined);
		};
		// An admitted publication owns its remaining finite reads through shutdown.
		return this.publication.getStore()?.active
			? this.lifecycle.track(Promise.resolve().then(execute))
			: this.lifecycle.admit(execute);
	}
	loadArtifacts(
		references: readonly ArtifactReference[],
		signal?: AbortSignal,
		retained?: ArtifactSource,
	): Promise<VerifiedArtifactClosure | undefined> {
		return this.read(
			(signal) =>
				this.artifacts.load(references, signal, retained, {
					maxStoredBytes: this.limits.maxBytes,
				}),
			signal,
		);
	}
	/** A finite publication owns its payloads until sealing; other processes cannot collect between its writes. */
	withPublication<T>(operation: () => Promise<T>, owned = false): Promise<T> {
		return this.artifacts.capturing
			? operation()
			: owned
				? this.read(() =>
						this.artifacts.withCapture(operation, {
							maxStoredBytes: this.limits.maxBytes,
						}),
					)
				: this.exclusive(() => operation());
	}
	configure(limits: Partial<ProvenanceStoreLimits>): void {
		this.limitsValue = Object.freeze({
			maxCertificates: positiveInteger(limits.maxCertificates, this.limitsValue.maxCertificates),
			maxBytes: positiveInteger(limits.maxBytes, this.limitsValue.maxBytes),
		});
		this.maintain();
	}
	async put(certificate: ProcessProvenanceCertificate): Promise<boolean> {
		const owned = parseProcessCertificate(certificate);
		if (!owned) throw new Error("invalid process provenance certificate");
		return this.publishRoot(`certificate:${owned.id}`, referencedArtifacts(owned), (database) => {
			const stored = decodeCertificate(
				database.prepare("SELECT value FROM certificates WHERE id = ?").get(owned.id)?.value,
			);
			const valid = stored?.id === owned.id;
			if (!valid) writeCertificate(database, owned);
			return !valid;
		});
	}
	get(id: Sha256Digest, signal?: AbortSignal): Promise<ProcessProvenanceCertificate | undefined> {
		return this.record("certificates", id, decodeCertificate, signal);
	}
	/** Input metadata shares certificate publication, retention, and crash recovery; it grants no execution authority. */
	async putInputVersion(
		payload: unknown,
		artifacts: readonly ArtifactReference[],
	): Promise<StoredInputVersion> {
		const body = {
			createdAt: Date.now(),
			payload,
			artifacts,
		};
		const canonical = stableStringify(body);
		const id = sha256Digest(canonical);
		const encoded = `${canonical.slice(0, -1)},"id":"${id}"}`;
		const record = decodeInputVersion(encoded, id);
		if (!record) throw new Error("invalid persistent input version");
		return this.publishRoot(`input:${id}`, record.artifacts, (database) => {
			const input = record.payload as { root: string; preparations: readonly { binding: string; key: string }[] };
			if (typeof input?.root !== "string" || !Array.isArray(input.preparations))
				throw new Error("invalid persistent input scope");
			this.statement(database, "INSERT OR REPLACE INTO input_versions(id, root, value) VALUES (?, ?, ?)")
				.run(id, input.root, encoded);
			const index = this.statement(database, "INSERT OR IGNORE INTO input_preparations VALUES (?, ?, ?, ?, ?)");
			for (const { binding, key } of input.preparations) {
				if (typeof binding !== "string" || typeof key !== "string") throw new Error("invalid preparation key");
				index.run(input.root, binding, digestObject(key), id, record.createdAt);
			}
			return record;
		});
	}
	getInputVersion(id: Sha256Digest, signal?: AbortSignal): Promise<StoredInputVersion | undefined> {
		return this.record("input_versions", id, decodeInputVersion, signal);
	}
	async findPreparedInputCandidates(query: {
		root: string;
		binding: string;
		prepKey: string;
		limit?: number;
	}, signal?: AbortSignal): Promise<readonly StoredInputVersion[]> {
		const limit = query.limit ?? this.configuration.current.filesystem.maxHistoryEntries;
		if (!isSafeCount(limit, 1, this.configuration.current.filesystem.maxHistoryEntries))
			throw new Error("invalid input discovery limit");
		const candidates: StoredInputVersion[] = [];
		for await (const record of this.records(
			`SELECT i.id, i.value FROM input_preparations p JOIN input_versions i ON i.id = p.input_id
			 WHERE p.root = ? AND p.binding = ? AND p.prep_key = ?
			 ORDER BY p.updated_at DESC, p.input_id DESC LIMIT ?`,
			[query.root, query.binding, query.prepKey, limit], decodeInputVersion, signal,
		)) candidates.push(record);
		return candidates;
	}
	/** Finite discovery hints, newest publication first; every row still needs current input and CAS validation. */
	async *candidatesInputVersions(root: string, limit: number, signal?: AbortSignal): AsyncGenerator<StoredInputVersion> {
		if (!isSafeCount(limit, 1, this.configuration.current.filesystem.maxHistoryEntries))
			throw new Error("invalid input discovery limit");
		yield* this.records(
			"SELECT id, value FROM input_versions WHERE root = ? ORDER BY rowid DESC LIMIT ?",
			[root, limit],
			decodeInputVersion,
			signal,
		);
	}
	/** One read snapshot supplies path hints for a whole resource query, including existing stores. */
	async inputPathIndex(root: string, limit: number, signal: AbortSignal) {
		if (!isSafeCount(limit, 1, this.configuration.current.filesystem.maxHistoryEntries))
			throw new Error("invalid input discovery limit");
		const rows = await this.query(
			`SELECT i.id, e.value ->> '$[0]' AS name, e.value ->> '$[1].type' AS kind
			FROM input_versions AS i
			CROSS JOIN json_each(CASE WHEN json_valid(i.value) THEN i.value END, '$.payload.entries') AS e
			WHERE e.type = 'array' AND i.rowid IN (
				SELECT rowid FROM input_versions WHERE root = ? ORDER BY rowid DESC LIMIT ?
			) ORDER BY i.rowid DESC`,
			[root, limit], signal,
		);
		const paths = new Map<string, Sha256Digest[]>();
		for (const { id, name, kind } of rows) {
			if (!isSha256Digest(id) || typeof name !== "string") continue;
			const key = kind === "alias" ? `tree:${name}` : name;
			const ids = paths.get(key);
			if (ids) ids.push(id);
			else paths.set(key, [id]);
		}
		return (target: string) => {
			const ids = new Set(paths.get(target));
			for (let current = target; ; ) {
				for (const id of paths.get(`tree:${current}`) ?? []) ids.add(id);
				const parent = filesystemPathKey(path.dirname(current));
				if (current === parent) return ids;
				current = parent;
			}
		};
	}
	async findByWeakKey(
		weakKey: Sha256Digest,
		executablePath: string,
		excludedCertificates?: ReadonlySet<Sha256Digest>,
	): Promise<readonly ProcessProvenanceCertificate[]> {
		const certificates: ProcessProvenanceCertificate[] = [];
		for await (const certificate of this.candidatesByWeakKey(weakKey, executablePath, excludedCertificates))
			certificates.push(certificate);
		return certificates;
	}
	/** Indexes only select candidates; every yielded certificate still verifies its complete identity. */
	async *candidatesByWeakKey(
		weakKey: Sha256Digest,
		executablePath: string,
		excludedCertificates?: ReadonlySet<Sha256Digest>,
		signal?: AbortSignal,
	): AsyncGenerator<ProcessProvenanceCertificate> {
		for await (const certificate of this.records(
			"SELECT id, value FROM certificates WHERE weakKey = ? AND executable = ? ORDER BY createdAt DESC, id DESC",
			[weakKey, executablePath],
			decodeCertificate,
			signal,
		))
			if (certificate.weakKey === weakKey && !excludedCertificates?.has(certificate.id)) yield certificate;
	}
	async hasWeakKey(weakKey: Sha256Digest, executablePath: string, signal?: AbortSignal): Promise<boolean> {
		try {
			return Boolean(
				(
					await this.query(
						"SELECT 1 FROM certificates WHERE weakKey = ? AND executable = ? LIMIT 1",
						[weakKey, executablePath],
						signal,
					)
				).length,
			);
		} catch (error) {
			if (hasErrorCode(error, "ERR_PROVENANCE_SCHEMA")) throw error;
			return false;
		}
	}
	/** An unavailable optional cache is a miss; live handoffs have their own availability check. */
	async mayHaveCertificates(executablePath?: string, signal?: AbortSignal): Promise<boolean> {
		try {
			return Boolean(
				(
					await this.query(
						executablePath === undefined
							? "SELECT 1 FROM certificates LIMIT 1"
							: "SELECT 1 FROM certificates WHERE executable = ? LIMIT 1",
						executablePath === undefined ? [] : [executablePath],
						signal,
					)
				).length,
			);
		} catch (error) {
			if (hasErrorCode(error, "ERR_PROVENANCE_SCHEMA")) throw error;
			return false;
		}
	}
	/** Physical snapshots include uncommitted crash debris; a metadata revision cannot cache them. */
	async stats(_refresh = false): Promise<ProvenanceStoreStats> {
		const limits = this.limits;
		const inventory = await this.inventory();
		const referenced = new Set(
			inventory.roots.flatMap((record) => record.references?.map((reference) => reference.digest) ?? []),
		);
		const totalBytes = inventory.objects.reduce((total, record) => total + record.bytes, 0);
		const artifacts = inventory.objects.filter((record) => record.path).length;
		return Object.freeze({
			certificates: inventory.roots.filter((record) => record.kind !== "input").length,
			inputVersions: inventory.roots.filter((record) => record.kind === "input").length,
			artifacts,
			totalBytes,
			orphanArtifacts: artifacts - [...referenced].filter((digest) => inventory.artifacts.has(digest)).length,
			overBudget: inventory.roots.length > limits.maxCertificates || totalBytes > limits.maxBytes,
		});
	}
	gc(): Promise<ProvenanceStoreGCResult> {
		// Later publications are marked in the shared transaction log, so this finite inventory never restarts.
		return (this.collection ??= this.publication.exit(() =>
			this.lifecycle
				.admit(async () => {
					const revision = Number(
						(await this.query("SELECT revision FROM changes WHERE path = ''", []))[0]?.revision ?? 0,
					);
					return this.collect(await this.inventory(), revision);
				})
				.finally(() => {
					this.collection = undefined;
				}),
		));
	}
	/** Flush and release the connection; subsequent operations may reopen it. */
	async drain(): Promise<void> {
		clearTimeout(this.maintenance);
		this.maintenance = undefined;
		await this.lifecycle.drain();
		if (this.lifecycle.sealed) return this.dispose();
		await this.lifecycle.serialize(() => this.closeDatabase());
	}
	dispose(): Promise<void> {
		this.stopped.abort(new Error("provenance store is closed"));
		clearTimeout(this.maintenance);
		this.maintenance = undefined;
		return this.lifecycle.dispose(async () => {
			await this.artifacts.dispose();
			await this.closeDatabase();
		});
	}
	private async closeDatabase(): Promise<void> {
		clearTimeout(this.maintenance);
		this.maintenance = undefined;
		this.readonlyDb?.db.close();
		this.readonlyDb = undefined;
		this.control?.close();
		this.control = undefined;
	}
	private maintain(): void {
		const interval = this.gcIntervalMs;
		if (this.maintenanceIntervalMs !== interval) {
			clearInterval(this.maintenance);
			this.maintenance = undefined;
		}
		if (!this.control || this.maintenance || !interval || this.lifecycle.sealed) return;
		this.maintenanceIntervalMs = interval;
		this.publication.exit(() => {
			this.maintenance = setInterval(() => {
				void this.gc().catch(() => undefined);
			}, interval).unref();
		});
	}
	async clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(
			async (database) => {
				const before = await this.inventory(database);
				await mapFilesystem(await this.payloadRoots(), (root) => rm(root, { recursive: true, force: true }));
				database.exec(
					"DELETE FROM certificates; DELETE FROM input_versions; DELETE FROM changes WHERE path <> ''",
				);
				return {
					removedCertificates: before.roots.filter((record) => record.kind !== "input").length,
					removedInputVersions: before.roots.filter((record) => record.kind === "input").length,
					removedArtifacts: before.objects.filter((record) => record.path).length,
					removedBytes: before.objects.reduce((total, record) => total + record.bytes, 0),
				};
			},
			[".clear"],
		);
	}
	private async collect(inventory: StoreInventory, revision: number): Promise<ProvenanceStoreGCResult> {
		const retained = new Set<string>();
		const result = {
			removedCertificates: 0,
			removedInputVersions: 0,
			removedArtifacts: 0,
			removedBytes: 0,
		};
		const cutoff = Date.now() - this.orphanGraceMs;
		let retainedBytes = 0;
		let retainedCertificates = 0;
		for (const record of [...inventory.roots].sort((left, right) => right.createdAt - left.createdAt)) {
			if (!record.references) continue;
			const references = record.references.map((reference) =>
				inventory.artifacts.get(reference.digest)?.bytes === reference.size
					? inventory.artifacts.get(reference.digest)
					: undefined,
			);
			if (references.some((reference) => !reference)) continue;
			const addedBytes =
				record.bytes +
				references.reduce(
					(total, reference) => total + (retained.has(reference!.key) ? 0 : reference!.bytes),
					0,
				);
			if (
				retainedCertificates >= this.limits.maxCertificates ||
				retainedBytes + addedBytes > this.limits.maxBytes
			)
				continue;
			retainedBytes += addedBytes;
			retainedCertificates++;
			retained.add(record.key);
			for (const reference of references) retained.add(reference!.key);
		}
		// Each certificate (including its index) or payload is one transaction. Publishers can run between objects.
		for (const record of inventory.objects)
			if (!retained.has(record.key))
				await this.exclusive(async (database) => {
					if ([".clear", record.key].some((key) => changeRevision(database, key) > revision)) return;
					if (record.path) {
						const current = await stat(record.path).catch(ignoreMissing);
						if (record.digest && (record.modifiedAt > cutoff || (current?.mtimeMs ?? 0) > cutoff)) return;
						if (current) {
							await rm(record.path, { force: true });
							result.removedArtifacts++;
							result.removedBytes += current.size;
						}
					} else if (/^(certificate|input):/.test(record.key)) {
						const table = record.key.startsWith("input:") ? "input_versions" : "certificates";
						const removed = Number(
							database
								.prepare(`DELETE FROM ${table} WHERE id = ?`)
								.run(record.key.slice(record.key.indexOf(":") + 1)).changes,
						);
						if (table === "input_versions") result.removedInputVersions += removed;
						else result.removedCertificates += removed;
						result.removedBytes += removed ? record.bytes : 0;
					}
					database.prepare("DELETE FROM changes WHERE path = ?").run(record.key);
				});
		await mapFilesystem(inventory.retiredRoots, (root) => rm(root, { recursive: true, force: true }));
		return result;
	}
	private async inventory(control?: DatabaseSync): Promise<StoreInventory> {
		const read = (sql: string) =>
			control ? Promise.resolve(control.prepare(sql).all()) : this.query(sql, []);
		const records: StoredRoot[] = (
			await read(
				"SELECT 'certificate' AS kind, id, value FROM certificates UNION ALL SELECT 'input', id, value FROM input_versions",
			)
		).map(({ kind: storedKind, id, value }) => {
			const kind = storedKind === "certificate" ? "certificate" : "input";
			const record = kind === "certificate" ? decodeCertificate(value) : decodeInputVersion(value);
			const valid = record?.id === id;
			const references = !valid
				? undefined
				: kind === "input"
					? (record as StoredInputVersion).artifacts
					: certificateReplayable(
								record as ProcessProvenanceCertificate,
								this.acceptedTaints ?? speculativeConfig().processReuse.replayAcceptedTaints,
						  )
						? referencedArtifacts(record as ProcessProvenanceCertificate)
						: undefined;
			return {
				key: `${kind}:${id}`,
				kind,
				bytes: Buffer.byteLength(String(value)),
				modifiedAt: 0,
				createdAt: record?.createdAt ?? 0,
				references,
			};
		});
		const keys = await read("SELECT path FROM changes WHERE path NOT IN ('', '.clear')");
		const roots = await this.payloadRoots();
		const paths = (await mapFilesystem(roots, filesUnder)).flat();
		const artifacts = new Map<Sha256Digest, StoredFile>();
		const objects: StoredFile[] = [
			...records,
			...(
				await mapFilesystem(paths, async (target) => {
					const value = await stat(target).catch(ignoreMissing);
					const key = this.objectKey(target);
					const match = /^cas\/sha256\/([0-9a-f]{2})\/([0-9a-f]{62})$/.exec(key);
					if (!value) return [];
					const digest = match ? (`sha256:${match[1]}${match[2]}` as Sha256Digest) : undefined;
					const record = {
						key,
						path: target,
						bytes: value.size,
						modifiedAt: value.mtimeMs,
						digest,
					};
					if (digest) artifacts.set(digest, record);
					return [record];
				})
			).flat(),
		];
		const present = new Set(objects.map((record) => record.key));
		for (const { path: key } of keys)
			if (typeof key === "string" && !present.has(key)) objects.push({ key, bytes: 0, modifiedAt: 0 });
		return { roots: records, artifacts, objects, retiredRoots: roots.slice(1) };
	}
	private async payloadRoots(): Promise<readonly string[]> {
		return [
			this.artifacts.root,
			...((await readdir(this.root).catch(ignoreMissing)) ?? [])
				.filter((name) => /^\.clear-[0-9a-f-]{36}$/.test(name))
				.map((name) => path.join(this.root, name)),
		];
	}
	private objectKey(target: string): string {
		return path.relative(this.root, target).split(path.sep).join("/");
	}
	private statement(database: DatabaseSync, sql: string) {
		const active = this.publication.getStore();
		if (!active || active.database !== database) return database.prepare(sql);
		let prepared = active.statements.get(sql);
		if (!prepared) {
			prepared = database.prepare(sql);
			active.statements.set(sql, prepared);
		}
		return prepared;
	}
	/** A persistent root adopts existing versions under the same metadata transaction. */
	private publishRoot<T>(
		key: string,
		references: readonly ArtifactReference[],
		publish: (database: DatabaseSync) => T | Promise<T>,
	): Promise<T> {
		return this.exclusive(
			async (database) => {
				const result = await publish(database);
				if (references.length) {
					await this.artifacts.materialize(references);
					const verified = this.publication.getStore()?.verifiedPaths;
					const unverified = verified
						? references.filter((reference) => !verified.has(this.artifacts.artifactPath(reference.digest)))
						: references;
					if (unverified.length) {
						const closure = await this.read((signal) =>
							this.artifacts.load(unverified, signal, undefined, {
								maxStoredBytes: this.limits.maxBytes,
								maxOpenChunks: 0,
							}),
						);
						if (!closure) throw new Error("persistent root artifacts are unavailable");
						await closure.dispose();
						for (const reference of unverified)
							verified?.add(this.artifacts.artifactPath(reference.digest));
					}
				}
				return result;
			},
			[key, ...references.map((reference) => this.objectKey(this.artifacts.artifactPath(reference.digest)))],
		);
	}
	private async record<
		T extends {
			readonly id: Sha256Digest;
		},
	>(
		table: "certificates" | "input_versions",
		id: Sha256Digest,
		decode: (value: unknown) => T | undefined,
		signal?: AbortSignal,
	): Promise<T | undefined> {
		digestHex(id);
		const value = (
			await this.query(`SELECT value FROM ${table} WHERE id = ?`, [id], signal)
		)[0]?.value;
		const record = decode(value);
		if (value !== undefined && record?.id !== id) throw new Error(`record integrity check failed for ${id}`);
		return record;
	}
	/** One read snapshot supplies each finite candidate set; exact dependency validation still grants adoption. */
	private async *records<
		T extends {
			readonly id: Sha256Digest;
		},
	>(
		sql: string,
		parameters: SQLInputValue[],
		decode: (value: unknown) => T | undefined,
		signal?: AbortSignal,
	): AsyncGenerator<T> {
		for (const { id, value } of await this.query(sql, parameters, signal)) {
			signal?.throwIfAborted();
			this.signal.throwIfAborted();
			const record = decode(value);
			if (record?.id === id) yield record;
		}
	}
	/** A lookup opens existing metadata read-only. */
	private query(
		sql: string,
		parameters: SQLInputValue[],
		signal?: AbortSignal,
	): Promise<Record<string, SQLOutputValue>[]> {
		return this.read(async (signal) => {
			const publication = this.publication.getStore();
			if (publication?.active) return publication.database.prepare(sql).all(...parameters);
			const rows = await this.queryReadonly(sql, parameters, signal);
			signal.throwIfAborted();
			return rows ?? [];
		}, signal);
	}
	private async queryReadonly(
		sql: string,
		parameters: SQLInputValue[],
		signal: AbortSignal,
	): Promise<Record<string, SQLOutputValue>[] | undefined> {
		const target = path.join(this.root, "metadata.sqlite");
		const present = await stat(target, { bigint: true }).catch(ignoreMissing);
		if (!present) {
			this.readonlyDb?.db.close();
			this.readonlyDb = undefined;
			return;
		}
		signal.throwIfAborted();
		let database =
			this.readonlyDb?.dev === present.dev && this.readonlyDb?.ino === present.ino
				? this.readonlyDb.db
				: undefined;
		if (!database) {
			this.readonlyDb?.db.close();
			this.readonlyDb = undefined;
			const opened = new DatabaseSync(target, { readOnly: true });
			opened.exec("PRAGMA query_only=ON");
			this.readonlyDb = { db: opened, dev: present.dev, ino: present.ino };
			database = opened;
		}
		database.exec("BEGIN");
		try {
			if (!hasMetadataSchema(database)) return;
			const rows: Record<string, SQLOutputValue>[] = [];
			let bytes = 0;
			for (const row of database.prepare(sql).iterate(...parameters)) {
				signal.throwIfAborted();
				bytes += serialize(row).byteLength;
				if (bytes > this.maxReadBytes) throw new Error("SQLite read result budget exceeded");
				rows.push(row);
			}
			return rows;
		} finally {
			if (database.isTransaction) database.exec("ROLLBACK");
		}
	}
	private async exclusive<T>(
		operation: (database: DatabaseSync) => Promise<T>,
		touched: readonly string[] = [],
		signal?: AbortSignal,
	): Promise<T> {
		const publication = this.publication.getStore();
		if (publication?.active) {
			const value = await operation(publication.database);
			for (const key of touched) publication.touched.add(key);
			return value;
		}
		const admission = AbortSignal.any([
			this.stopped.signal,
			AbortSignal.timeout(speculativeConfig().storage.sqliteTimeoutMs),
			...(signal ? [signal] : []),
		]);
		let admitted = false;
		const task = this.lifecycle.serialize(async () => {
			admission.throwIfAborted();
			const database = (this.control ??= await this.open(admission));
			this.maintain();
			await sqliteReady(() => {
				database.exec("BEGIN IMMEDIATE");
				admitted = true;
			}, admission);
			const publication = {
				database,
				touched: new Set(touched),
				verifiedPaths: new Set<string>(),
				statements: new Map<string, ReturnType<DatabaseSync["prepare"]>>(),
				active: true,
			};
			// Cancellation ends at admission. Finish owned filesystem work before releasing the transaction.
			try {
				const value = await this.publication.run(publication, () => operation(database));
				const version = changeRevision(database) + 1;
				const update = this.statement(
					database,
					"INSERT INTO changes VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET revision=excluded.revision",
				);
				for (const key of ["", ...publication.touched]) update.run(key, version);
				database.exec("COMMIT");
				return value;
			} catch (error) {
				if (database.isTransaction) database.exec("ROLLBACK");
				throw error;
			} finally {
				publication.active = false;
			}
		});
		const result = await waitForCompletion(task, admission);
		if (result.status !== "completed") {
			if (admitted) return task;
			throw admission.reason;
		}
		return result.value;
	}
	private async open(signal: AbortSignal): Promise<DatabaseSync> {
		if (!this.reservedRoot) await mkdir(this.root, { recursive: true });
		const database = new DatabaseSync(path.join(this.root, "metadata.sqlite"));
		try {
			await sqliteReady(() => database.exec("PRAGMA auto_vacuum=FULL; PRAGMA journal_mode=WAL"), signal);
			database.exec("PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON");
			await sqliteReady(() => database.exec("BEGIN IMMEDIATE"), signal);
			if (!hasMetadataSchema(database))
				for (const ddl of Object.values(metadataDDL))
					database.exec(ddl.replace(/^(CREATE (?:TABLE|INDEX)) /, "$1 IF NOT EXISTS "));
			database.exec("INSERT OR IGNORE INTO changes VALUES ('', 0); COMMIT");
			return database;
		} catch (error) {
			database.close();
			throw error;
		}
	}
}
interface StoredFile {
	readonly key: string;
	readonly path?: string;
	readonly bytes: number;
	readonly modifiedAt: number;
	readonly digest?: Sha256Digest;
}
interface StoredRoot extends StoredFile {
	readonly kind: "certificate" | "input";
	readonly references?: readonly ArtifactReference[];
	readonly createdAt: number;
}
interface StoreInventory {
	readonly roots: readonly StoredRoot[];
	readonly artifacts: ReadonlyMap<Sha256Digest, StoredFile>;
	readonly objects: readonly StoredFile[];
	readonly retiredRoots: readonly string[];
}
async function filesUnder(root: string): Promise<readonly string[]> {
	try {
		return (await readdir(root, { recursive: true, withFileTypes: true }))
			.filter((entry) => entry.isFile())
			.map((entry) => path.join(entry.parentPath, entry.name));
	} catch (error) {
		if (missing(error)) return [];
		throw error;
	}
}
function changeRevision(database: DatabaseSync, key = ""): number {
	return Number(database.prepare("SELECT revision FROM changes WHERE path = ?").get(key)?.revision ?? 0);
}
function writeCertificate(database: DatabaseSync, certificate: ProcessProvenanceCertificate): void {
	database
		.prepare("INSERT OR REPLACE INTO certificates VALUES (?, ?, ?, ?, ?)")
		.run(
			certificate.id,
			stableStringify(certificate),
			certificate.weakKey,
			certificate.prototype.executablePath,
			certificate.createdAt,
		);
}
/** Native transactions recover on process exit. Busy acquisition yields; filesystem work stays asynchronous. */
async function sqliteReady<T>(run: () => T, signal: AbortSignal): Promise<T> {
	const policy = speculativeConfig().storage;
	for (
		let wait = policy.sqliteRetryInitialMs;
		;
		wait = Math.min(wait * policy.sqliteRetryMultiplier, policy.sqliteRetryMaxMs)
	) {
		signal.throwIfAborted();
		try {
			return run();
		} catch (error) {
			if (
				![5, 6].includes(
					Number(
						(
							error as {
								errcode?: number;
							}
						).errcode,
					) & 0xff,
				)
			)
				throw error;
			await delay(wait);
		}
	}
}
/** Preserve verified objects; a new complete publication atomically replaces a corrupt copy. */
async function publishImmutable(target: string, bytes: Uint8Array, reservedRoot?: string): Promise<boolean> {
	if ((await readOptional(target, bytes.byteLength))?.equals(bytes)) {
		const now = new Date(); // Referencing the same object restarts its orphan grace.
		try {
			await utimes(target, now, now);
			return false;
		} catch (error) {
			if (!missing(error)) throw error;
		}
	}
	if (!reservedRoot) await mkdir(path.dirname(target), { recursive: true });
	else {
		const relative = path.relative(reservedRoot, path.dirname(target));
		if (!relative || relative.startsWith("..") || path.isAbsolute(relative))
			throw new Error("artifact publication escapes reserved cache");
		let directory = reservedRoot;
		for (const segment of relative.split(path.sep)) {
			directory = path.join(directory, segment);
			await mkdir(directory).catch((error) => {
				if (error.code !== "EEXIST") throw error;
			});
		}
	}
	const temporary = path.join(
		path.dirname(target),
		`.${path.basename(target)}.${process.pid}.${randomUUID()}.tmp`,
	);
	// Exclusive creation grants cleanup ownership; an existing temporary belongs to another publisher.
	const file = await open(temporary, "wx");
	try {
		try {
			await writeFile(file, bytes);
		} finally {
			await file.close();
		}
		await rename(temporary, target);
		return true;
	} finally {
		await rm(temporary, { force: true });
	}
}
function decodeCertificate(value: unknown): ProcessProvenanceCertificate | undefined {
	try {
		return typeof value === "string" ? parseProcessCertificate(JSON.parse(value)) : undefined;
	} catch {
		return undefined;
	}
}
function decodeInputVersion(value: unknown, writtenID?: Sha256Digest): StoredInputVersion | undefined {
	try {
		if (typeof value !== "string" || Buffer.byteLength(value) > speculativeConfig().storage.maxRecordBytes)
			return;
		const record = JSON.parse(value) as StoredInputVersion;
		const { id, ...body } = record;
		if (
			Object.keys(record).some((key) => !["id", "createdAt", "payload", "artifacts"].includes(key)) ||
			!Number.isFinite(record.createdAt) ||
			!isSha256Digest(id) ||
			(writtenID ?? digestObject(body)) !== id ||
			!Array.isArray(record.artifacts) ||
			record.artifacts.length > speculativeConfig().filesystem.maxManifestChunks
		)
			return;
		const sizes = new Map<Sha256Digest, number>();
		for (const reference of record.artifacts) validateArtifact(reference, sizes);
		if (sizes.size !== record.artifacts.length) return;
		return record;
	} catch {
		return undefined;
	}
}
/** Named cache objects may alias source inputs. The returned reader retains the verified noatime mount. */
async function openArtifactFile(target: string): Promise<FileHandle | undefined> {
	return withFilesystemReadView(
		undefined,
		async (view) => {
			const opened = await openFilesystemFile(target, false, view).catch((error): undefined => {
				if (!missing(error)) throw error;
			});
			if (!opened) return;
			try {
				await opened.binding?.close();
				return opened.handle;
			} catch (error) {
				await opened.handle.close();
				throw error;
			}
		},
		async (handle) => {
			await handle?.close();
		},
	);
}
async function readArtifactRange(
	file: FilesystemObjectHandle,
	bytes: Buffer,
	position: number,
	signal?: AbortSignal,
	exact = true,
): Promise<Buffer> {
	let offset = 0;
	while (offset < bytes.length) {
		signal?.throwIfAborted();
		const { bytesRead } = await file.read!(bytes, offset, bytes.length - offset, position + offset);
		if (!bytesRead) break;
		offset += bytesRead;
	}
	signal?.throwIfAborted();
	if (exact && offset !== bytes.length) throw new Error("artifact truncated");
	return offset === bytes.length ? bytes : bytes.subarray(0, offset);
}
/** One extra byte proves a size mismatch without loading an arbitrarily enlarged corrupt object. */
async function readOptional(target: string, size: number, signal?: AbortSignal): Promise<Buffer | undefined> {
	signal?.throwIfAborted();
	await using file = await openArtifactFile(target);
	return file && (await readArtifactRange(file, Buffer.allocUnsafe(size + 1), 0, signal, false));
}
async function verifyArtifact(reference: ArtifactReference, bytes?: Buffer): Promise<Buffer | undefined> {
	if (bytes && (bytes.byteLength !== reference.size || (await sha256DigestAsync(bytes)) !== reference.digest))
		throw new Error(`artifact integrity check failed for ${reference.digest}`);
	return bytes;
}
function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}
