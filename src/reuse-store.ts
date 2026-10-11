import { randomUUID } from "node:crypto";
import { link, mkdir, open, readFile, readdir, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { setTimeout as delay } from "node:timers/promises";
import { type ArtifactReference, certificateReplayable, isSha256Digest, parseProcessCertificate, type ProcessProvenanceCertificate,
	type ProvenanceTaint, referencedArtifacts, sha256DigestAsync, type Sha256Digest, validateArtifact } from "./provenance-certificate.ts";
import { stableStringify } from "./stable-json.ts";
import { nonNegativeNumber, positiveInteger } from "./setting-input.ts";
import { hasErrorCode, isMissing as missing } from "./error-utils.ts";
import { mapFilesystem } from "./filesystem-evidence.ts";

export interface ProvenanceStoreLimits { readonly maxCertificates: number; readonly maxBytes: number; }

export interface ProvenanceStoreOptions extends Partial<ProvenanceStoreLimits> {
	readonly gcIntervalMs?: number; readonly orphanGraceMs?: number;
	/** Taints the publisher accepted: retention and stats keep what its lookups can still replay. */
	readonly acceptedTaints?: readonly ProvenanceTaint[];
}

export interface ProvenanceStoreStats {
	readonly certificates: number; readonly artifacts: number; readonly orphanArtifacts: number;
	readonly totalBytes: number; readonly overBudget: boolean;
}

export interface ProvenanceStoreGCResult { readonly removedCertificates: number; readonly removedArtifacts: number; readonly removedBytes: number; }

export const DEFAULT_PROVENANCE_STORE_LIMITS: ProvenanceStoreLimits = Object.freeze({ maxCertificates: 4_096, maxBytes: 2 * 1024 * 1024 * 1024 });

const DEFAULT_GC_INTERVAL_MS = 5 * 60_000;
const DEFAULT_ORPHAN_GRACE_MS = 5 * 60_000;

/** Immutable content-addressed effect storage; it never stores decoded Runtime result objects. */
export class ArtifactCAS {
	readonly root: string;
	private readonly publish: typeof publishImmutable;

	constructor(root: string, publish = publishImmutable) { this.root = path.resolve(root); this.publish = publish; }

	async put(value: string | Uint8Array): Promise<ArtifactReference> {
		const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : value;
		const reference = Object.freeze({ digest: await sha256DigestAsync(bytes), size: bytes.byteLength }), target = this.artifactPath(reference.digest);
		await this.publish(target, bytes);
		return reference;
	}

	async get(reference: ArtifactReference): Promise<Buffer | undefined> {
		validateArtifact(reference);
		const bytes = await readOptional(this.artifactPath(reference.digest));
		if (bytes && (bytes.byteLength !== reference.size || await sha256DigestAsync(bytes) !== reference.digest)) throw new Error(`artifact integrity check failed for ${reference.digest}`);
		return bytes;
	}

	/** Availability hint only; publication and adoption verify content. */
	async has(reference: ArtifactReference): Promise<boolean> {
		const info = await stat(this.artifactPath(reference.digest)).catch((error): undefined => { if (!missing(error)) throw error; });
		return info?.isFile() === true && info.size === reference.size;
	}

	/** Load and integrity-check a complete effect closure before any replay side effect begins. */
	async load(references: readonly ArtifactReference[]): Promise<VerifiedArtifactClosure | undefined> {
		const sizes = new Map<Sha256Digest, number>();
		for (const reference of references) validateArtifact(reference, sizes);
		const values = new Map<Sha256Digest, Buffer>();
		for (const [digest, size] of sizes) {
			const value = await this.get({ digest, size });
			if (!value) return undefined;
			values.set(digest, value);
		}
		return Object.freeze({ artifacts: values.size, bytes: [...values.values()].reduce((total, value) => total + value.byteLength, 0),
			read: ({ digest, size }: ArtifactReference): Buffer => {
				const value = values.get(digest);
				if (!value || value.byteLength !== size) throw new Error(`artifact is outside the verified closure: ${digest}`);
				return value;
			},
		});
	}

	artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest); return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}

export interface VerifiedArtifactClosure {
	readonly artifacts: number; readonly bytes: number;
	/** Borrow verified bytes. Trusted replay consumers must treat the returned buffer as read-only. */
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
	private maintenance: Promise<void> = Promise.resolve();
	private collection?: Promise<ProvenanceStoreGCResult>;
	private control?: Promise<DatabaseSync>;
	private gcDueAt: number;
	private statsValue?: { revision: number; value: ProvenanceStoreStats; limits: ProvenanceStoreLimits };

	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.root = path.resolve(root); this.configure(options);
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"), (target, bytes, verify) =>
			this.exclusive(() => publishImmutable(target, bytes, verify), [this.objectKey(target)]));
		this.gcIntervalMs = nonNegativeNumber(options.gcIntervalMs, DEFAULT_GC_INTERVAL_MS);
		this.orphanGraceMs = nonNegativeNumber(options.orphanGraceMs, DEFAULT_ORPHAN_GRACE_MS);
		this.acceptedTaints = Object.freeze([...options.acceptedTaints ?? []]);
		this.gcDueAt = Date.now() + this.gcIntervalMs;
	}

	get limits(): ProvenanceStoreLimits { return this.limitsValue; }
	configure(limits: Partial<ProvenanceStoreLimits>): void {
		this.limitsValue = Object.freeze({ maxCertificates: positiveInteger(limits.maxCertificates, this.limits.maxCertificates), maxBytes: positiveInteger(limits.maxBytes, this.limits.maxBytes) });
		this.gcDueAt = 0; this.statsValue = undefined;
	}

	async put(certificate: ProcessProvenanceCertificate): Promise<boolean> {
		if (certificate.result.continuation) return false;
		const owned = parseProcessCertificate(certificate);
		if (!owned) throw new Error("invalid process provenance certificate");
		const references = referencedArtifacts(owned), touched = [`certificate:${owned.id}`, ...references.map(reference => this.objectKey(this.artifacts.artifactPath(reference.digest)))];
		const published = await this.exclusive(async database => {
			await mapFilesystem(references, async reference => { if (!await this.artifacts.has(reference)) throw new Error(`certificate references missing artifact ${reference.digest}`); });
			const stored = decodeCertificate(database.prepare("SELECT value FROM certificates WHERE id = ?").get(owned.id)?.value);
			const valid = stored?.id === owned.id;
			if (!valid) writeCertificate(database, owned);
			return !valid;
		}, touched);
		if (Date.now() >= this.gcDueAt) { this.gcDueAt = Date.now() + this.gcIntervalMs; void this.gc().catch(() => undefined); }
		return published;
	}

	get(id: Sha256Digest): Promise<ProcessProvenanceCertificate | undefined> {
		digestHex(id);
		return this.exclusive(async database => {
			const value = database.prepare("SELECT value FROM certificates WHERE id = ?").get(id)?.value, certificate = decodeCertificate(value);
			if (value !== undefined && certificate?.id !== id) throw new Error(`certificate integrity check failed for ${id}`);
			return certificate;
		}, [], false);
	}

	async findByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): Promise<readonly ProcessProvenanceCertificate[]> {
		const certificates: ProcessProvenanceCertificate[] = [];
		for await (const certificate of this.candidatesByWeakKey(weakKey, executablePath, excludedCertificates)) certificates.push(certificate);
		return certificates.sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id));
	}

	/** Indexes only select candidates; every yielded certificate still verifies its complete identity. */
	async *candidatesByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): AsyncGenerator<ProcessProvenanceCertificate> {
		const candidates = await this.exclusive(async database => database.prepare("SELECT id FROM certificates WHERE weakKey = ? AND executable = ? ORDER BY createdAt DESC, id DESC").all(weakKey, executablePath), [], false);
		for (const { id } of candidates) {
			if (!isSha256Digest(id) || excludedCertificates?.has(id)) continue;
			const certificate = await this.get(id).catch(() => undefined);
			if (certificate?.weakKey === weakKey) yield certificate;
		}
	}

	/** IO uncertainty retains the replay route; an empty metadata index has no leftover directory hints. */
	async mayHaveCertificates(executablePath?: string): Promise<boolean> {
		try { return await this.exclusive(async database => Boolean(executablePath === undefined ? database.prepare("SELECT 1 FROM certificates LIMIT 1").get() : database.prepare("SELECT 1 FROM certificates WHERE executable = ? LIMIT 1").get(executablePath)), [], false); }
		catch { return true; }
	}

	async stats(refresh = false): Promise<ProvenanceStoreStats> {
		await this.drain(); const revision = await this.revision(), limits = this.limits;
		if (!refresh && this.statsValue?.revision === revision && this.statsValue.limits === limits) return this.statsValue.value;
		const inventory = await this.inventory(), referenced = new Set(inventory.certificates.flatMap(({ certificate }) => certificate && certificateReplayable(certificate, this.acceptedTaints) ? referencedArtifacts(certificate).map(reference => reference.digest) : []));
		const totalBytes = [...inventory.certificates, ...inventory.artifacts.values()].reduce((total, record) => total + record.bytes, 0);
		const value = Object.freeze({ certificates: inventory.certificates.length, artifacts: inventory.artifacts.size, totalBytes, orphanArtifacts: [...inventory.artifacts.keys()].filter(digest => !referenced.has(digest)).length, overBudget: inventory.certificates.length > limits.maxCertificates || totalBytes > limits.maxBytes });
		if (await this.revision() === revision && this.limits === limits) this.statsValue = { revision, limits, value };
		return value;
	}

	gc(): Promise<ProvenanceStoreGCResult> {
		// Later publications are marked in the shared transaction log, so this finite inventory never restarts.
		return this.collection ??= this.revision().then(async revision => this.collect(await this.inventory(), revision)).finally(() => { this.collection = undefined; });
	}

	async drain(): Promise<void> {
		await this.maintenance; await this.collection?.catch(() => undefined);
		const pending = this.maintenance; await pending;
		if (pending === this.maintenance && !this.collection) { const control = this.control; this.control = undefined; (await control)?.close(); }
	}

	async clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(async database => {
			const before = await this.inventory(database);
			const tomb = path.join(this.root, `.clear-${randomUUID()}`);
			await rename(this.artifacts.root, tomb).catch(error => { if (!missing(error)) throw error; });
			await rm(tomb, { recursive: true, force: true });
			database.exec("DELETE FROM certificates; DELETE FROM changes WHERE path <> ''");
			return { removedCertificates: before.certificates.length, removedArtifacts: before.artifacts.size, removedBytes: [...before.certificates, ...before.artifacts.values()].reduce((total, record) => total + record.bytes, 0) };
		}, [".clear"]);
	}

	private async collect(inventory: StoreInventory, revision: number): Promise<ProvenanceStoreGCResult> {
		const retained = new Set<string>(), result = { removedCertificates: 0, removedArtifacts: 0, removedBytes: 0 }, cutoff = Date.now() - this.orphanGraceMs;
		let retainedBytes = 0, retainedCertificates = 0;
		for (const record of [...inventory.certificates].sort((left, right) => (right.certificate?.createdAt ?? 0) - (left.certificate?.createdAt ?? 0))) {
			const certificate = record.certificate;
			if (!certificate || !certificateReplayable(certificate, this.acceptedTaints)) continue;
			const references = referencedArtifacts(certificate).map(reference => inventory.artifacts.get(reference.digest)?.bytes === reference.size ? inventory.artifacts.get(reference.digest) : undefined);
			if (references.some(reference => !reference)) continue;
			const addedBytes = record.bytes + references.reduce((total, reference) => total + (retained.has(reference!.key) ? 0 : reference!.bytes), 0);
			if (retainedCertificates >= this.limits.maxCertificates || retainedBytes + addedBytes > this.limits.maxBytes) continue;
			retainedBytes += addedBytes; retainedCertificates++; retained.add(record.key);
			for (const reference of references) retained.add(reference!.key);
		}
		// Each certificate (including its index) or payload is one transaction. Publishers can run between objects.
		for (const record of [...inventory.certificates, ...inventory.artifacts.values()]) if (!retained.has(record.key)) await this.exclusive(async database => {
			if ([".clear", record.key].some(key => changeRevision(database, key) > revision)) return;
			if (record.path) {
				if (record.modifiedAt > cutoff || ((await stat(record.path).catch(() => undefined))?.mtimeMs ?? Infinity) > cutoff) return;
				await rm(record.path, { force: true }); result.removedArtifacts++;
			} else { database.prepare("DELETE FROM certificates WHERE id = ?").run(record.key.slice("certificate:".length)); result.removedCertificates++; }
			database.prepare("DELETE FROM changes WHERE path = ?").run(record.key); result.removedBytes += record.bytes;
		});
		return result;
	}

	private async inventory(control?: DatabaseSync): Promise<StoreInventory> {
		const read = (database: DatabaseSync) => database.prepare("SELECT id, value FROM certificates").all().map(({ id, value }) => {
			const certificate = decodeCertificate(value);
			return { key: `certificate:${id}`, bytes: Buffer.byteLength(String(value)), modifiedAt: 0, certificate: certificate?.id === id ? certificate : undefined };
		});
		const certificates = control ? read(control) : await this.exclusive(async database => read(database), [], false);
		const root = path.join(this.artifacts.root, "sha256"), paths = await filesUnder(root);
		const artifacts = new Map<Sha256Digest, StoredFile>((await mapFilesystem(paths, async target => {
			const value = await stat(target).catch(() => undefined), hex = path.relative(root, target).split(path.sep).join("");
			return value && /^[0-9a-f]{64}$/.test(hex) ? [[`sha256:${hex}` as Sha256Digest, { key: this.objectKey(target), path: target, bytes: value.size, modifiedAt: value.mtimeMs }] as const] : [];
		})).flat());
		return { certificates, artifacts };
	}

	private revision(): Promise<number> { return this.exclusive(async database => changeRevision(database), [], false); }
	private objectKey(target: string): string { return path.relative(this.root, target).split(path.sep).join("/"); }
	private exclusive<T>(operation: (database: DatabaseSync) => Promise<T>, touched: readonly string[] = [], mutate = true): Promise<T> {
		const result = this.maintenance.then(async () => {
			const database = await (this.control ??= this.open().catch(error => { this.control = undefined; throw error; }));
			if (!mutate) return operation(database);
			await sqliteReady(() => database.exec("BEGIN IMMEDIATE"));
			try { return await operation(database); }
			finally {
				try {
					const version = changeRevision(database) + 1, update = database.prepare("INSERT INTO changes VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET revision=excluded.revision");
					for (const key of ["", ...touched]) update.run(key, version);
					database.exec("COMMIT");
				} catch (error) { database.exec("ROLLBACK"); throw error; }
			}
		});
		this.maintenance = result.then(() => undefined, () => undefined); return result;
	}

	private async open(): Promise<DatabaseSync> {
		await mkdir(this.root, { recursive: true }); const database = new DatabaseSync(path.join(this.root, "metadata.sqlite"));
		try {
			await sqliteReady(() => database.exec("PRAGMA auto_vacuum=FULL; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL"));
			await sqliteReady(() => database.exec("BEGIN IMMEDIATE"));
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
				await sqliteReady(() => database.exec("BEGIN IMMEDIATE"));
				await mapFilesystem(["certificates", "indexes"], segment => rm(path.join(this.root, segment), { recursive: true, force: true }));
				database.exec("PRAGMA user_version=2; COMMIT");
			}
			return database;
		} catch (error) { database.close(); throw error; }
	}
}

interface StoredFile { readonly key: string; readonly path?: string; readonly bytes: number; readonly modifiedAt: number; }
interface StoreInventory { readonly certificates: readonly (StoredFile & { readonly certificate: ProcessProvenanceCertificate | undefined })[]; readonly artifacts: ReadonlyMap<Sha256Digest, StoredFile>; }

async function filesUnder(root: string): Promise<readonly string[]> {
	try { return (await readdir(root, { recursive: true, withFileTypes: true })).filter(entry => entry.isFile()).map(entry => path.join(entry.parentPath, entry.name)); }
	catch (error) { if (missing(error)) return []; throw error; }
}
function changeRevision(database: DatabaseSync, key = ""): number { return Number(database.prepare("SELECT revision FROM changes WHERE path = ?").get(key)?.revision ?? 0); }
function writeCertificate(database: DatabaseSync, certificate: ProcessProvenanceCertificate): void {
	database.prepare("INSERT OR REPLACE INTO certificates VALUES (?, ?, ?, ?, ?)").run(certificate.id, stableStringify(certificate), certificate.weakKey, certificate.prototype.executablePath, certificate.createdAt);
}
/** Native transactions recover on process exit. Busy acquisition yields; filesystem work stays asynchronous. */
async function sqliteReady<T>(run: () => T): Promise<T> {
	for (;;) { try { return run(); } catch (error) { if (![5, 6].includes(Number((error as { errcode?: number }).errcode) & 0xff)) throw error; await delay(1); } }
}
/** Preserve verified objects; a new complete publication atomically replaces a corrupt copy. */
async function publishImmutable(target: string, bytes: Uint8Array, verify = (stored: Buffer) => stored.equals(bytes)): Promise<boolean> {
	const present = async () => {
		const stored = await readOptional(target);
		try { if (!stored || !verify(stored)) return false; } catch { return false; }
		const now = new Date(); // Referencing the same object restarts its orphan grace.
		return utimes(target, now, now).then(() => true, (error): false => { if (!missing(error)) throw error; return false; });
	};
	if (await present()) return false;
	await mkdir(path.dirname(target), { recursive: true });
	const temporary = path.join(path.dirname(target), `.${path.basename(target)}.${process.pid}.${randomUUID()}.tmp`);
	// Exclusive creation grants cleanup ownership; an existing temporary belongs to another publisher.
	const file = await open(temporary, "wx");
	try {
		try { await writeFile(file, bytes); } finally { await file.close(); }
		try { await link(temporary, target); return true; } catch (error) {
			if (!hasErrorCode(error, "EEXIST")) throw error;
			if (await present()) return false;
			await rename(temporary, target); return true;
		}
	} finally {
		await rm(temporary, { force: true });
	}
}

function decodeCertificate(value: unknown): ProcessProvenanceCertificate | undefined {
	try { return typeof value === "string" ? parseProcessCertificate(JSON.parse(value)) : undefined; } catch { return undefined; }
}

function readOptional(target: string) { return readFile(target).catch((error): undefined => { if (!missing(error)) throw error; }); }

function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}
