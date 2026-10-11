import { randomUUID } from "node:crypto";
import { link, mkdir, open, readFile, readdir, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import path from "node:path";
import { type ArtifactReference, certificateReplayable, isSha256Digest, parseProcessCertificate, type ProcessProvenanceCertificate,
	type ProvenanceTaint, referencedArtifacts, sha256Digest, sha256DigestAsync, type Sha256Digest, validateArtifact } from "./provenance-certificate.ts";
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
const STORE_SEGMENTS = ["indexes", "certificates", "cas"] as const;

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

/** Persistent certificate/pathset index kept separate from the Runtime's live ResultCache. */
export class ProvenanceCertificateStore {
	readonly root: string;
	readonly artifacts: ArtifactCAS;
	private limitsValue: ProvenanceStoreLimits;
	private readonly gcIntervalMs: number;
	private readonly orphanGraceMs: number;
	private readonly acceptedTaints: readonly ProvenanceTaint[];
	private maintenance: Promise<void> = Promise.resolve();
	private collection?: Promise<ProvenanceStoreGCResult>;
	private protectedObjects?: Set<string>;
	private gcDueAt: number;
	private statsValue?: ProvenanceStoreStats;

	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.root = path.resolve(root);
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"), (target, bytes, verify) => this.exclusive(() => {
			this.protectedObjects?.add(target); return publishImmutable(target, bytes, verify);
		}));
		this.limitsValue = Object.freeze({
			maxCertificates: positiveInteger(options.maxCertificates, DEFAULT_PROVENANCE_STORE_LIMITS.maxCertificates),
			maxBytes: positiveInteger(options.maxBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes),
		});
		this.gcIntervalMs = nonNegativeNumber(options.gcIntervalMs, DEFAULT_GC_INTERVAL_MS);
		this.orphanGraceMs = nonNegativeNumber(options.orphanGraceMs, DEFAULT_ORPHAN_GRACE_MS);
		this.acceptedTaints = Object.freeze([...options.acceptedTaints ?? []]);
		this.gcDueAt = Date.now() + this.gcIntervalMs;
	}

	get limits(): ProvenanceStoreLimits { return this.limitsValue; }

	configure(limits: Partial<ProvenanceStoreLimits>): void {
		this.limitsValue = Object.freeze({
			maxCertificates: positiveInteger(limits.maxCertificates, this.limits.maxCertificates),
			maxBytes: positiveInteger(limits.maxBytes, this.limits.maxBytes),
		});
		this.gcDueAt = 0;
		this.statsValue = undefined;
	}

	async put(certificate: ProcessProvenanceCertificate): Promise<boolean> {
		if (certificate.result.continuation) return false;
		const owned = parseProcessCertificate(certificate);
		if (!owned) throw new Error("invalid process provenance certificate");
		const published = await this.exclusive(async () => {
			let indexed: ProcessProvenanceCertificate | undefined;
			this.protectedObjects?.add(this.certificatePath(owned.id));
			await mapFilesystem(referencedArtifacts(owned), async reference => {
				this.protectedObjects?.add(this.artifacts.artifactPath(reference.digest));
				if (!(await this.artifacts.has(reference))) throw new Error(`certificate references missing artifact ${reference.digest}`);
			});
			const published = await publishImmutable(this.certificatePath(owned.id), Buffer.from(stableStringify(owned), "utf8"),
				bytes => (indexed = decodeCertificate(bytes))?.id === owned.id);
			await publishImmutable(this.weakReferencePath(published ? owned : indexed ?? owned), new Uint8Array());
			return published;
		});
		if (Date.now() >= this.gcDueAt) { this.gcDueAt = Date.now() + this.gcIntervalMs; void this.gc().catch(() => undefined); }
		return published;
	}

	async get(id: Sha256Digest): Promise<ProcessProvenanceCertificate | undefined> {
		const bytes = await readOptional(this.certificatePath(id)), certificate = bytes && decodeCertificate(bytes);
		if (bytes && certificate?.id !== id) throw new Error(`certificate integrity check failed for ${id}`);
		return certificate;
	}

	async findByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): Promise<readonly ProcessProvenanceCertificate[]> {
		const certificates: ProcessProvenanceCertificate[] = [];
		for await (const certificate of this.candidatesByWeakKey(weakKey, executablePath, excludedCertificates)) certificates.push(certificate);
		return certificates.sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id));
	}

	/** Newest publication hints first; every yielded certificate still verifies its full identity. */
	async *candidatesByWeakKey(weakKey: Sha256Digest, executablePath: string, excludedCertificates?: ReadonlySet<Sha256Digest>): AsyncGenerator<ProcessProvenanceCertificate> {
		const directory = this.weakIndexDirectory(weakKey, executablePath);
		const names = await readdir(directory).catch((error): string[] => { if (missing(error)) return []; throw error; });
		const references = names.flatMap(name => {
			const match = /^(?:(.+)-)?([0-9a-f]{64})\.ref$/.exec(name), createdAt = Number(match?.[1] ?? 0);
			return match && Number.isFinite(createdAt) && createdAt >= 0 ? [{ createdAt, id: `sha256:${match[2]}` as Sha256Digest }] : [];
		}).sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id));
		const checked = new Set(excludedCertificates);
		for (const { id } of references) {
			if (checked.has(id)) continue;
			checked.add(id);
			const certificate = await this.get(id).catch(() => undefined); // An unreadable candidate cannot suppress another verified result.
			if (certificate?.weakKey === weakKey) yield certificate;
		}
	}

	/** Cheap conservative hint: empty shards and IO uncertainty still retain the replay route. */
	async mayHaveCertificates(executablePath?: string): Promise<boolean> {
		try { return (await readdir(executablePath === undefined ? this.managedPath("certificates") : this.executableIndexDirectory(executablePath))).length > 0; }
		catch (error) { return !missing(error); }
	}

	async stats(refresh = false): Promise<ProvenanceStoreStats> {
		await this.drain();
		if (refresh || !this.statsValue) this.statsValue = inventoryStats(await this.inventory(), this.limits, this.acceptedTaints);
		return this.statsValue;
	}

	gc(): Promise<ProvenanceStoreGCResult> {
		if (this.collection) return this.collection;
		const protectedObjects = this.protectedObjects = new Set<string>();
		// One finite snapshot. Publications during this cycle survive until the next; clear invalidates the old namespace.
		return this.collection = this.inventory().then(inventory => this.collect(inventory, protectedObjects))
			.finally(() => { this.collection = undefined; this.protectedObjects = undefined; });
	}

	/** Wait for admitted publication and optional maintenance before disposing the store owner. */
	async drain(): Promise<void> {
		await this.maintenance;
		await this.collection?.catch(() => undefined);
	}

	clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(async () => {
			this.protectedObjects = undefined;
			const before = inventoryStats(await this.inventory(), this.limits, this.acceptedTaints);
			await mkdir(this.root, { recursive: true });
			const tomb = path.join(this.root, `.clear-${randomUUID()}`);
			await mkdir(tomb);
			for (const segment of STORE_SEGMENTS) {
				await rename(this.managedPath(segment), path.join(tomb, segment)).catch((error) => { if (!missing(error)) throw error; });
			}
			await rm(tomb, { recursive: true, force: true });
			return { removedCertificates: before.certificates, removedArtifacts: before.artifacts, removedBytes: before.totalBytes };
		});
	}

	private async collect(inventory: StoreInventory, protectedObjects: ReadonlySet<string>): Promise<ProvenanceStoreGCResult> {
		const cutoff = Date.now() - this.orphanGraceMs, result = { removedCertificates: 0, removedArtifacts: 0, removedBytes: 0 };
		const retained = new Set<Sha256Digest>(), retainedArtifacts = new Set<Sha256Digest>();
		let retainedBytes = 0;
		for (const record of [...inventory.certificates].sort((left, right) => (right.certificate?.createdAt ?? 0) - (left.certificate?.createdAt ?? 0))) {
			const certificate = record.certificate;
			if (!certificate || !certificateReplayable(certificate, this.acceptedTaints)) continue;
			const references = referencedArtifacts(certificate);
			if (!references.every((reference) => inventory.artifacts.get(reference.digest)?.bytes === reference.size)) continue;
			const addedArtifacts = references.filter((reference) => !retainedArtifacts.has(reference.digest));
			const addedBytes = record.bytes + addedArtifacts.reduce((total, reference) => total + reference.size, 0);
			if (retained.size >= this.limits.maxCertificates || retainedBytes + addedBytes > this.limits.maxBytes) continue;
			retained.add(certificate.id);
			retainedBytes += addedBytes;
			for (const reference of references) retainedArtifacts.add(reference.digest);
		}
		// Admit one removal at a time: a publisher can wait for one object, never the remainder of the collection.
		const remove = (record: StoredFile, certificate?: ProcessProvenanceCertificate) => this.exclusive(async () => {
			if (protectedObjects !== this.protectedObjects || protectedObjects.has(record.path)) return;
			if (!record.path.endsWith(".json") && ((await stat(record.path).catch(() => undefined))?.mtimeMs ?? Infinity) > cutoff) return;
			await mapFilesystem([record.path, ...certificate ? [false, true].map(legacy => this.weakReferencePath(certificate, legacy)) : []], target => rm(target, { force: true }));
			result[record.path.endsWith(".json") ? "removedCertificates" : "removedArtifacts"]++; result.removedBytes += record.bytes;
		});
		for (const record of inventory.certificates) if (!record.certificate || !retained.has(record.certificate.id)) await remove(record, record.certificate);
		for (const [digest, artifact] of inventory.artifacts) if (!retainedArtifacts.has(digest) && artifact.modifiedAt <= cutoff) await remove(artifact);
		return result;
	}

	private async inventory(): Promise<StoreInventory> {
		const [certificatePaths, artifactPaths] = await mapFilesystem([this.managedPath("certificates"), path.join(this.artifacts.root, "sha256")], filesUnder);
		const certificates = (await mapFilesystem(certificatePaths.filter((target) => target.endsWith(".json")), async (target) => {
			const bytes = await readFile(target).catch(() => undefined);
			if (!bytes) return undefined;
			return { path: target, bytes: bytes.byteLength, modifiedAt: 0, certificate: decodeCertificate(bytes) };
		})).filter((record): record is StoredCertificateFile => Boolean(record));
		const artifactRoot = path.join(this.artifacts.root, "sha256");
		const artifacts = new Map<Sha256Digest, StoredFile>((await mapFilesystem(artifactPaths, async (target): Promise<readonly (readonly [Sha256Digest, StoredFile])[]> => {
			const value = await stat(target).catch(() => undefined);
			const hex = path.relative(artifactRoot, target).split(path.sep).join("");
			return value && /^[0-9a-f]{64}$/.test(hex) ? [[`sha256:${hex}`, { path: target, bytes: value.size, modifiedAt: value.mtimeMs }]] : [];
		})).flat());
		return { certificates, artifacts };
	}

	private exclusive<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.maintenance.then(operation).finally(() => { this.statsValue = undefined; });
		this.maintenance = result.then(() => undefined, () => undefined);
		return result;
	}

	private certificatePath(id: Sha256Digest): string {
		const hex = digestHex(id); return path.join(this.root, "certificates", hex.slice(0, 2), `${hex.slice(2)}.json`);
	}

	private executableIndexDirectory(executablePath: string): string { return path.join(this.root, "indexes", "executable", digestHex(sha256Digest(executablePath))); }

	private weakIndexDirectory(weakKey: Sha256Digest, executablePath: string): string { return path.join(this.executableIndexDirectory(executablePath), digestHex(weakKey)); }

	private weakReferencePath(certificate: ProcessProvenanceCertificate, legacy = false): string {
		return path.join(this.weakIndexDirectory(certificate.weakKey, certificate.prototype.executablePath), `${legacy ? "" : `${certificate.createdAt}-`}${digestHex(certificate.id)}.ref`);
	}

	private managedPath(segment: typeof STORE_SEGMENTS[number]): string { return path.join(this.root, segment); }
}

interface StoredFile { readonly path: string; readonly bytes: number; readonly modifiedAt: number; }

interface StoredCertificateFile extends StoredFile { readonly certificate: ProcessProvenanceCertificate | undefined; }
interface StoreInventory { readonly certificates: readonly StoredCertificateFile[]; readonly artifacts: ReadonlyMap<Sha256Digest, StoredFile>; }

function inventoryStats(inventory: StoreInventory, limits: ProvenanceStoreLimits, acceptedTaints: readonly ProvenanceTaint[]): ProvenanceStoreStats {
	const referenced = new Set(inventory.certificates.flatMap(({ certificate }) => certificate && certificateReplayable(certificate, acceptedTaints)
		? referencedArtifacts(certificate).map(reference => reference.digest) : []));
	const totalBytes = [...inventory.certificates, ...inventory.artifacts.values()].reduce((total, record) => total + record.bytes, 0);
	return Object.freeze({ certificates: inventory.certificates.length, artifacts: inventory.artifacts.size, totalBytes,
		orphanArtifacts: [...inventory.artifacts.keys()].filter(digest => !referenced.has(digest)).length,
		overBudget: inventory.certificates.length > limits.maxCertificates || totalBytes > limits.maxBytes });
}

async function filesUnder(root: string): Promise<readonly string[]> {
	try {
		return (await readdir(root, { recursive: true, withFileTypes: true })).filter((entry) => entry.isFile()).map((entry) => path.join(entry.parentPath, entry.name));
	} catch (error) { if (missing(error)) return []; throw error; }
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

function decodeCertificate(bytes: Buffer): ProcessProvenanceCertificate | undefined {
	try { return parseProcessCertificate(JSON.parse(bytes.toString("utf8"))); } catch { return undefined; }
}

function readOptional(target: string) { return readFile(target).catch((error): undefined => { if (!missing(error)) throw error; }); }

function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}
