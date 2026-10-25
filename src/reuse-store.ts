import { randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { mkdir, open, readFile, readdir, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { setTimeout as delay } from "node:timers/promises";
import {
	certificateReplayable, parseProcessCertificate, type ProcessProvenanceCertificate, type ProvenanceTaint,
	referencedArtifacts,
} from "./provenance-certificate.ts";
import { type ArtifactReference, validateArtifact } from "./artifact-reference.ts";
import { digestObject, isSha256Digest, sha256DigestAsync, type Sha256Digest } from "./stable-value-hash.ts";
import { stableStringify } from "./stable-json.ts";
import { nonNegativeNumber, positiveInteger } from "./setting-input.ts";
import { isMissing as missing } from "./error-utils.ts";
import { mapFilesystem } from "./filesystem-evidence.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";

/** maxCertificates is the shared metadata-record limit, including persistent input versions. */
export interface ProvenanceStoreLimits { readonly maxCertificates: number; readonly maxBytes: number; }

export interface ProvenanceStoreOptions extends Partial<ProvenanceStoreLimits> {
	readonly gcIntervalMs?: number; readonly orphanGraceMs?: number;
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

export const DEFAULT_PROVENANCE_STORE_LIMITS: ProvenanceStoreLimits = Object.freeze({ maxCertificates: 4_096, maxBytes: 2 * 1024 * 1024 * 1024 });

const DEFAULT_GC_INTERVAL_MS = 5 * 60_000;
const DEFAULT_ORPHAN_GRACE_MS = 5 * 60_000;

/** Immutable effects; use store.artifacts to participate in a shared store's publication and retention lifecycle. */
export class ArtifactCAS {
	readonly root: string;
	private readonly publish: typeof publishImmutable;

	constructor(root: string, publish = publishImmutable) { this.root = path.resolve(root); this.publish = publish; }

	async put(value: string | Uint8Array): Promise<ArtifactReference> {
		const bytes = Buffer.from(value);
		const reference = Object.freeze({ digest: await sha256DigestAsync(bytes), size: bytes.byteLength }), target = this.artifactPath(reference.digest);
		await this.publish(target, bytes);
		return reference;
	}

	async get(reference: ArtifactReference, signal?: AbortSignal): Promise<Buffer | undefined> {
		validateArtifact(reference);
		return verifyArtifact(reference, await readOptional(this.artifactPath(reference.digest), signal));
	}

	/** Availability hint only; publication and adoption verify content. */
	async has(reference: ArtifactReference): Promise<boolean> {
		const info = await stat(this.artifactPath(reference.digest)).catch((error): undefined => { if (!missing(error)) throw error; });
		return info?.isFile() === true && info.size === reference.size;
	}

	/** Load and integrity-check a complete effect closure before any replay side effect begins. */
	async load(references: readonly ArtifactReference[], signal?: AbortSignal, retained?: VerifiedArtifactClosure): Promise<VerifiedArtifactClosure | undefined> {
		signal?.throwIfAborted();
		const sizes = new Map<Sha256Digest, number>();
		for (const reference of references) validateArtifact(reference, sizes);
		const values = new Map<Sha256Digest, Buffer>();
		for (const [digest, size] of sizes) {
			signal?.throwIfAborted();
			const reference = { digest, size }, value = retained ? await verifyArtifact(reference, Buffer.from(retained.read(reference))) : await this.get(reference, signal);
			if (!value) return undefined;
			values.set(digest, value);
		}
		signal?.throwIfAborted();
		return Object.freeze({ artifacts: values.size, bytes: [...values.values()].reduce((total, value) => total + value.byteLength, 0),
			read: ({ digest, size }: ArtifactReference): Buffer => {
				const value = values.get(digest);
				if (!value || value.byteLength !== size) throw new Error(`artifact is outside the verified closure: ${digest}`);
				return Buffer.from(value);
			},
		});
	}

	artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest); return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}

export interface VerifiedArtifactClosure {
	readonly artifacts: number; readonly bytes: number;
	/** Each consumer owns its copy; callbacks cannot mutate a retained result's verified bytes. */
	readonly read: (reference: ArtifactReference) => Buffer;
}

/** Certificate metadata and its indexes share one cross-process transaction; immutable payloads remain in the CAS. */
export class ProvenanceCertificateStore {
	readonly root: string;
	readonly artifacts: ArtifactCAS;
	private limitsValue = DEFAULT_PROVENANCE_STORE_LIMITS;
	private readonly gcIntervalMs: number;
	private readonly orphanGraceMs: number;
	private readonly acceptedTaints: readonly ProvenanceTaint[];
	private readonly lifecycle = new RuntimeLifecycleLane();
	private readonly stopped = new AbortController();
	private collection?: Promise<ProvenanceStoreGCResult>;
	private maintenance?: NodeJS.Timeout;
	private readonly publication = new AsyncLocalStorage<{ database: DatabaseSync; touched: Set<string>; active: boolean }>();
	private control?: DatabaseSync;
	private reader?: DatabaseSync;

	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.root = path.resolve(root); this.configure(options);
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"), (target, bytes) =>
			this.exclusive(() => publishImmutable(target, bytes), [this.objectKey(target)]));
		this.gcIntervalMs = nonNegativeNumber(options.gcIntervalMs, DEFAULT_GC_INTERVAL_MS);
		this.orphanGraceMs = nonNegativeNumber(options.orphanGraceMs, DEFAULT_ORPHAN_GRACE_MS);
		this.acceptedTaints = Object.freeze([...options.acceptedTaints ?? []]);
	}

	get limits(): ProvenanceStoreLimits { return this.limitsValue; }
	get signal(): AbortSignal { return this.stopped.signal; }
	/** Own the complete read, including input proofs and their handle cleanup. */
	read<T>(operation: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal): Promise<T> {
		return this.lifecycle.admit(() => {
			const combined = AbortSignal.any([this.signal, ...(signal ? [signal] : [])]); combined.throwIfAborted(); return operation(combined);
		});
	}
	loadArtifacts(references: readonly ArtifactReference[], signal?: AbortSignal, retained?: VerifiedArtifactClosure): Promise<VerifiedArtifactClosure | undefined> {
		return this.read(signal => this.artifacts.load(references, signal, retained), signal);
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

	get(id: Sha256Digest, signal?: AbortSignal): Promise<ProcessProvenanceCertificate | undefined> {
		digestHex(id);
		return this.exclusive(async database => {
			const value = database.prepare("SELECT value FROM certificates WHERE id = ?").get(id)?.value, certificate = decodeCertificate(value);
			if (value !== undefined && certificate?.id !== id) throw new Error(`certificate integrity check failed for ${id}`);
			return certificate;
		}, [], false, signal);
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
	getInputVersion(id: Sha256Digest): Promise<StoredInputVersion | undefined> {
		digestHex(id);
		return this.exclusive(async database => {
			const value = database.prepare("SELECT value FROM input_versions WHERE id = ?").get(id)?.value, record = decodeInputVersion(value);
			if (value !== undefined && record?.id !== id) throw new Error(`input version integrity check failed for ${id}`);
			return record;
		}, [], false);
	}

	async findByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): Promise<readonly ProcessProvenanceCertificate[]> {
		const certificates: ProcessProvenanceCertificate[] = [];
		for await (const certificate of this.candidatesByWeakKey(weakKey, executablePath, excludedCertificates)) certificates.push(certificate);
		return certificates;
	}

	/** Indexes only select candidates; every yielded certificate still verifies its complete identity. */
	async *candidatesByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>, signal?: AbortSignal): AsyncGenerator<ProcessProvenanceCertificate> {
		const candidates = await this.exclusive(async database => database.prepare("SELECT id FROM certificates WHERE weakKey = ? AND executable = ? ORDER BY createdAt DESC, id DESC").all(weakKey, executablePath), [], false, signal);
		for (const { id } of candidates) {
			signal?.throwIfAborted();
			if (!isSha256Digest(id) || excludedCertificates?.has(id)) continue;
			const certificate = await this.get(id, signal).catch(() => undefined);
			if (certificate?.weakKey === weakKey) yield certificate;
		}
	}

	/** An unavailable optional cache is a miss; live handoffs have their own availability check. */
	async mayHaveCertificates(executablePath?: string, signal?: AbortSignal): Promise<boolean> {
		try { return await this.exclusive(async database => Boolean(executablePath === undefined ? database.prepare("SELECT 1 FROM certificates LIMIT 1").get() : database.prepare("SELECT 1 FROM certificates WHERE executable = ? LIMIT 1").get(executablePath)), [], false, signal); }
		catch { return false; }
	}

	/** Physical snapshots include uncommitted crash debris; a metadata revision cannot cache them. */
	async stats(_refresh = false): Promise<ProvenanceStoreStats> {
		const limits = this.limits;
		const inventory = await this.inventory(), referenced = new Set(inventory.roots.flatMap(record => record.references?.map(reference => reference.digest) ?? []));
		const totalBytes = inventory.objects.reduce((total, record) => total + record.bytes, 0), artifacts = inventory.objects.filter(record => record.path).length;
		return Object.freeze({ certificates: inventory.roots.filter(record => record.kind === "certificate").length, inputVersions: inventory.roots.filter(record => record.kind === "input").length,
			artifacts, totalBytes, orphanArtifacts: artifacts - [...referenced].filter(digest => inventory.artifacts.has(digest)).length, overBudget: inventory.roots.length > limits.maxCertificates || totalBytes > limits.maxBytes });
	}

	gc(): Promise<ProvenanceStoreGCResult> {
		// Later publications are marked in the shared transaction log, so this finite inventory never restarts.
		return this.collection ??= this.publication.exit(() => this.lifecycle.admit(async () => {
			const revision = await this.exclusive(async database => changeRevision(database), [], false); return this.collect(await this.inventory(), revision);
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
		return this.lifecycle.close(async () => { await this.lifecycle.drain(); this.closeDatabase(); });
	}
	private closeDatabase(): void {
		clearTimeout(this.maintenance); this.maintenance = undefined;
		this.reader?.close(); this.reader = undefined; this.control?.close(); this.control = undefined;
	}
	private maintain(): void {
		if (!this.control || this.maintenance || this.lifecycle.sealed) return;
		this.publication.exit(() => { this.maintenance = setInterval(() => { void this.gc().catch(() => undefined); }, Math.max(100, this.gcIntervalMs)).unref(); });
	}

	async clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(async database => {
			const before = await this.inventory(database);
			await mapFilesystem(await this.payloadRoots(), root => rm(root, { recursive: true, force: true }));
			database.exec("DELETE FROM certificates; DELETE FROM input_versions; DELETE FROM changes WHERE path <> ''");
			return { removedCertificates: before.roots.filter(record => record.kind === "certificate").length, removedInputVersions: before.roots.filter(record => record.kind === "input").length,
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
			} else if (/^(certificate|input):/.test(record.key)) {
				const certificate = record.key.startsWith("certificate:"), table = certificate ? "certificates" : "input_versions";
				const removed = Number(database.prepare(`DELETE FROM ${table} WHERE id = ?`).run(record.key.slice(record.key.indexOf(":") + 1)).changes);
				if (certificate) result.removedCertificates += removed; else result.removedInputVersions += removed;
				result.removedBytes += removed ? record.bytes : 0;
			}
			database.prepare("DELETE FROM changes WHERE path = ?").run(record.key);
		});
		await mapFilesystem(inventory.retiredRoots, root => rm(root, { recursive: true, force: true }));
		return result;
	}

	private async inventory(control?: DatabaseSync): Promise<StoreInventory> {
		const read = (database: DatabaseSync): StoredRoot[] => (["certificate", "input"] as const).flatMap(kind =>
			database.prepare(`SELECT id, value FROM ${kind === "certificate" ? "certificates" : "input_versions"}`).all().map(({ id, value }) => {
				const record = kind === "certificate" ? decodeCertificate(value) : decodeInputVersion(value), valid = record?.id === id;
				const references = !valid ? undefined : kind === "input" ? (record as StoredInputVersion).artifacts
					: certificateReplayable(record as ProcessProvenanceCertificate, this.acceptedTaints) ? referencedArtifacts(record as ProcessProvenanceCertificate) : undefined;
				return { key: `${kind}:${id}`, kind, bytes: Buffer.byteLength(String(value)), modifiedAt: 0, createdAt: record?.createdAt ?? 0, references };
			}));
		const metadata = (database: DatabaseSync) => ({ roots: read(database), keys: database.prepare("SELECT path FROM changes WHERE path NOT IN ('', '.clear')").all() });
		const { roots: records, keys } = control ? metadata(control) : await this.exclusive(async database => metadata(database), [], false);
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
	private async exclusive<T>(operation: (database: DatabaseSync) => Promise<T>, touched: readonly string[] = [], mutate = true, signal?: AbortSignal): Promise<T> {
		const publication = this.publication.getStore();
		if (publication?.active) {
			const value = await operation(publication.database);
			for (const key of touched) publication.touched.add(key);
			return value;
		}
		// WAL readers see committed history without waiting behind a producer that an Actor may need to join.
		if (!mutate && this.control) return this.read(() => operation(this.reader ??= new DatabaseSync(path.join(this.root, "metadata.sqlite"), { readOnly: true })), signal);
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
		await mkdir(this.root, { recursive: true }); const database = new DatabaseSync(path.join(this.root, "metadata.sqlite"));
		try {
			database.exec("PRAGMA synchronous=NORMAL");
			const version = Number(await sqliteReady(() => database.prepare("PRAGMA user_version").get()?.user_version, signal));
			if (version === 3) return database;
			if (version > 3) throw new Error(`unsupported provenance store version ${version}`);
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
			database.exec("CREATE TABLE IF NOT EXISTS input_versions(id TEXT PRIMARY KEY, value TEXT NOT NULL); PRAGMA user_version=3; COMMIT");
			return database;
		} catch (error) { database.close(); throw error; }
	}
}

interface StoredFile { readonly key: string; readonly path?: string; readonly bytes: number; readonly modifiedAt: number; readonly digest?: Sha256Digest; }
interface StoredRoot extends StoredFile { readonly kind: "certificate" | "input"; readonly references?: readonly ArtifactReference[]; readonly createdAt: number; }
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
async function publishImmutable(target: string, bytes: Uint8Array): Promise<boolean> {
	if ((await readOptional(target))?.equals(bytes)) {
		const now = new Date(); // Referencing the same object restarts its orphan grace.
		try { await utimes(target, now, now); return false; } catch (error) { if (!missing(error)) throw error; }
	}
	await mkdir(path.dirname(target), { recursive: true });
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

function readOptional(target: string, signal?: AbortSignal) { return readFile(target, { signal }).catch((error): undefined => { if (!missing(error)) throw error; }); }

async function verifyArtifact(reference: ArtifactReference, bytes?: Buffer): Promise<Buffer | undefined> {
	if (bytes && (bytes.byteLength !== reference.size || await sha256DigestAsync(bytes) !== reference.digest)) throw new Error(`artifact integrity check failed for ${reference.digest}`);
	return bytes;
}

function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}
