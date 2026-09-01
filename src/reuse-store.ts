import { randomUUID } from "node:crypto";
import { link, mkdir, readFile, readdir, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	type ArtifactReference,
	certificateReplayable,
	isSha256Digest,
	parseProcessCertificate,
	type ProcessProvenanceCertificate,
	referencedArtifacts,
	sha256Digest,
	type Sha256Digest,
} from "./provenance-certificate.ts";
import { stableStringify } from "./stable-json.ts";

export interface ProvenanceStoreLimits {
	readonly maxCertificates: number;
	readonly maxBytes: number;
}

export interface ProvenanceStoreOptions extends Partial<ProvenanceStoreLimits> {
	readonly gcIntervalMs?: number;
	readonly orphanGraceMs?: number;
}

export interface ProvenanceStoreStats {
	readonly certificates: number;
	readonly weakKeys: number;
	readonly artifacts: number;
	readonly orphanArtifacts: number;
	readonly certificateBytes: number;
	readonly artifactBytes: number;
	readonly totalBytes: number;
	readonly overBudget: boolean;
	readonly lastGCAt?: number;
	readonly limits: ProvenanceStoreLimits;
}

export interface ProvenanceStoreGCResult {
	readonly removedCertificates: number;
	readonly removedArtifacts: number;
	readonly removedBytes: number;
	readonly stats: ProvenanceStoreStats;
}

export const DEFAULT_PROVENANCE_STORE_LIMITS: ProvenanceStoreLimits = Object.freeze({
	maxCertificates: 4_096,
	maxBytes: 2 * 1024 * 1024 * 1024,
});

const DEFAULT_GC_INTERVAL_MS = 5 * 60_000;
const DEFAULT_ORPHAN_GRACE_MS = 5 * 60_000;
const STORE_SEGMENTS = ["indexes", "certificates", "cas"] as const;

/** Immutable content-addressed effect storage; it never stores decoded Runtime result objects. */
export class ArtifactCAS {
	readonly root: string;

	constructor(root: string) {
		this.root = path.resolve(root);
	}

	async put(value: string | Uint8Array): Promise<ArtifactReference> {
		const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value);
		const digest = sha256Digest(bytes);
		const target = this.artifactPath(digest);
		await publishImmutable(target, bytes);
		const reference = Object.freeze({ digest, size: bytes.byteLength });
		if (!(await this.has(reference))) throw new Error(`artifact publication failed for ${digest}`);
		return reference;
	}

	async get(reference: ArtifactReference): Promise<Buffer | undefined> {
		if (!isSha256Digest(reference.digest) || !Number.isSafeInteger(reference.size) || reference.size < 0) {
			throw new Error("invalid artifact reference");
		}
		let bytes: Buffer;
		try {
			bytes = await readFile(this.artifactPath(reference.digest));
		} catch (error) {
			if (missing(error)) return undefined;
			throw error;
		}
		if (bytes.byteLength !== reference.size || sha256Digest(bytes) !== reference.digest) {
			throw new Error(`artifact integrity check failed for ${reference.digest}`);
		}
		return bytes;
	}

	async has(reference: ArtifactReference): Promise<boolean> {
		return (await this.get(reference)) !== undefined;
	}

	/** Load and integrity-check a complete effect closure before any replay side effect begins. */
	async load(references: readonly ArtifactReference[]): Promise<VerifiedArtifactClosure | undefined> {
		const expected = new Map<Sha256Digest, ArtifactReference>();
		for (const reference of references) {
			const previous = expected.get(reference.digest);
			if (previous && previous.size !== reference.size) {
				throw new Error(`conflicting artifact sizes for ${reference.digest}`);
			}
			expected.set(reference.digest, reference);
		}
		const values = new Map<Sha256Digest, Buffer>();
		for (const reference of expected.values()) {
			const value = await this.get(reference);
			if (!value) return undefined;
			values.set(reference.digest, value);
		}
		return new LoadedArtifactClosure(values);
	}

	private artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest);
		return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}

export interface VerifiedArtifactClosure {
	readonly artifacts: number;
	readonly bytes: number;
	/** Borrow verified bytes. Trusted replay consumers must treat the returned buffer as read-only. */
	readonly read: (reference: ArtifactReference) => Buffer;
}

class LoadedArtifactClosure implements VerifiedArtifactClosure {
	readonly artifacts: number;
	readonly bytes: number;
	private readonly values: ReadonlyMap<Sha256Digest, Buffer>;

	constructor(values: ReadonlyMap<Sha256Digest, Buffer>) {
		this.values = new Map(values);
		this.artifacts = values.size;
		this.bytes = [...values.values()].reduce((total, value) => total + value.byteLength, 0);
		Object.freeze(this);
	}

	readonly read = (reference: ArtifactReference): Buffer => {
		if (!isSha256Digest(reference.digest) || !Number.isSafeInteger(reference.size) || reference.size < 0) {
			throw new Error("invalid artifact reference");
		}
		const value = this.values.get(reference.digest);
		if (!value || value.byteLength !== reference.size) {
			throw new Error(`artifact is outside the verified closure: ${reference.digest}`);
		}
		return value;
	};
}

/** Persistent L2 certificate/pathset index kept separate from the Runtime's in-memory ResultCache. */
export class ProvenanceCertificateStore {
	readonly root: string;
	readonly artifacts: ArtifactCAS;
	private limitsValue: ProvenanceStoreLimits;
	private readonly gcIntervalMs: number;
	private readonly orphanGraceMs: number;
	private maintenance: Promise<void> = Promise.resolve();
	private gcDueAt: number;
	private lastGCAt?: number;

	constructor(root: string, options: ProvenanceStoreOptions = {}) {
		this.root = path.resolve(root);
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"));
		this.limitsValue = Object.freeze({
			maxCertificates: positiveInteger(options.maxCertificates, DEFAULT_PROVENANCE_STORE_LIMITS.maxCertificates),
			maxBytes: positiveInteger(options.maxBytes, DEFAULT_PROVENANCE_STORE_LIMITS.maxBytes),
		});
		this.gcIntervalMs = nonNegativeNumber(options.gcIntervalMs, DEFAULT_GC_INTERVAL_MS);
		this.orphanGraceMs = nonNegativeNumber(options.orphanGraceMs, DEFAULT_ORPHAN_GRACE_MS);
		this.gcDueAt = Date.now() + this.gcIntervalMs;
	}

	get limits(): ProvenanceStoreLimits {
		return this.limitsValue;
	}

	configure(limits: Partial<ProvenanceStoreLimits>): void {
		this.limitsValue = Object.freeze({
			maxCertificates: positiveInteger(limits.maxCertificates, this.limits.maxCertificates),
			maxBytes: positiveInteger(limits.maxBytes, this.limits.maxBytes),
		});
		this.gcDueAt = 0;
	}

	put(certificate: ProcessProvenanceCertificate): Promise<boolean> {
		const publication = this.exclusive(async () => {
			const parsed = parseProcessCertificate(certificate);
			if (!parsed || parsed.id !== certificate.id) throw new Error("invalid process provenance certificate");
			for (const reference of referencedArtifacts(certificate)) {
				if (!(await this.artifacts.has(reference))) {
					throw new Error(`certificate references missing artifact ${reference.digest}`);
				}
			}
			const published = await publishImmutable(
				this.certificatePath(certificate.id),
				Buffer.from(stableStringify(certificate), "utf8"),
			);
			if ((await this.get(certificate.id))?.id !== certificate.id) throw new Error("certificate publication failed");
			const reference = this.weakReferencePath(certificate.weakKey, certificate.id);
			await mkdir(path.dirname(reference), { recursive: true });
			try {
				await writeFile(reference, "", { flag: "wx" });
			} catch (error) {
				if (!alreadyExists(error)) throw error;
			}
			return published;
		});
		return publication.then((published) => {
			if (Date.now() >= this.gcDueAt) {
				this.gcDueAt = Date.now() + this.gcIntervalMs;
				void this.gc().catch(() => undefined);
			}
			return published;
		});
	}

	async get(id: Sha256Digest): Promise<ProcessProvenanceCertificate | undefined> {
		let bytes: Buffer;
		try {
			bytes = await readFile(this.certificatePath(id));
		} catch (error) {
			if (missing(error)) return undefined;
			throw error;
		}
		let value: unknown;
		try {
			value = JSON.parse(bytes.toString("utf8"));
		} catch {
			throw new Error(`certificate ${id} is not valid JSON`);
		}
		const certificate = parseProcessCertificate(value);
		if (!certificate || certificate.id !== id) throw new Error(`certificate integrity check failed for ${id}`);
		return certificate;
	}

	async findByWeakKey(weakKey: Sha256Digest): Promise<readonly ProcessProvenanceCertificate[]> {
		let names: string[];
		try {
			names = await readdir(this.weakIndexDirectory(weakKey));
		} catch (error) {
			if (missing(error)) return [];
			throw error;
		}
		const certificates: ProcessProvenanceCertificate[] = [];
		for (const name of names) {
			const match = /^([0-9a-f]{64})\.ref$/.exec(name);
			if (!match) continue;
			const reference = path.join(this.weakIndexDirectory(weakKey), name);
			const certificate = await this.get(`sha256:${match[1]}` as Sha256Digest);
			if (certificate?.weakKey === weakKey) certificates.push(certificate);
			else await unlink(reference).catch((error) => {
				if (!missing(error)) throw error;
			});
		}
		certificates.sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id));
		return certificates;
	}

	async stats(): Promise<ProvenanceStoreStats> {
		await this.maintenance;
		return inventoryStats(await this.inventory(), this.limits, this.lastGCAt);
	}

	gc(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(() => this.collect());
	}

	clear(): Promise<ProvenanceStoreGCResult> {
		return this.exclusive(async () => {
			const before = inventoryStats(await this.inventory(), this.limits, this.lastGCAt);
			await mkdir(this.root, { recursive: true });
			const tomb = path.join(this.root, `.clear-${randomUUID()}`);
			await mkdir(tomb);
			for (const segment of STORE_SEGMENTS) {
				await rename(this.managedPath(segment), path.join(tomb, segment)).catch((error) => {
					if (!missing(error)) throw error;
				});
			}
			await rm(tomb, { recursive: true, force: true });
			this.lastGCAt = Date.now();
			const stats = inventoryStats(await this.inventory(), this.limits, this.lastGCAt);
			return {
				removedCertificates: Math.max(0, before.certificates - stats.certificates),
				removedArtifacts: Math.max(0, before.artifacts - stats.artifacts),
				removedBytes: Math.max(0, before.totalBytes - stats.totalBytes),
				stats,
			};
		});
	}

	private async collect(): Promise<ProvenanceStoreGCResult> {
		const inventory = await this.inventory();
		const now = Date.now();
		const retained = new Set<Sha256Digest>();
		const retainedArtifacts = new Set<Sha256Digest>();
		let retainedBytes = 0;
		for (const record of [...inventory.certificates].sort(
			(left, right) => (right.certificate?.createdAt ?? 0) - (left.certificate?.createdAt ?? 0),
		)) {
			const certificate = record.certificate;
			if (!certificate || !certificateReplayable(certificate)) continue;
			const references = referencedArtifacts(certificate);
			if (!references.every((reference) => inventory.artifacts.get(reference.digest)?.bytes === reference.size)) continue;
			const addedArtifacts = references.filter((reference) => !retainedArtifacts.has(reference.digest));
			const addedBytes = record.bytes + addedArtifacts.reduce((total, reference) => total + reference.size, 0);
			if (retained.size >= this.limits.maxCertificates || retainedBytes + addedBytes > this.limits.maxBytes) continue;
			retained.add(certificate.id);
			retainedBytes += addedBytes;
			for (const reference of references) retainedArtifacts.add(reference.digest);
		}
		const removed = inventory.certificates.filter((record) => !record.certificate || !retained.has(record.certificate.id));
		const orphans = [...inventory.artifacts].flatMap(([digest, artifact]) =>
			!retainedArtifacts.has(digest) && now - artifact.modifiedAt >= this.orphanGraceMs ? [artifact] : [],
		);
		await Promise.all([
			...removed.map((record) => rm(record.path, { force: true })),
			...removed.flatMap((record) =>
				record.certificate
					? [rm(this.weakReferencePath(record.certificate.weakKey, record.certificate.id), { force: true })]
					: [],
			),
			...orphans.map((artifact) => rm(artifact.path, { force: true })),
		]);
		this.lastGCAt = now;
		return {
			removedCertificates: removed.length,
			removedArtifacts: orphans.length,
			removedBytes:
				removed.reduce((total, record) => total + record.bytes, 0) +
				orphans.reduce((total, artifact) => total + artifact.bytes, 0),
			stats: inventoryStats(await this.inventory(), this.limits, this.lastGCAt),
		};
	}

	private async inventory(): Promise<StoreInventory> {
		const [certificatePaths, artifactPaths] = await Promise.all([
			filesUnder(this.managedPath("certificates")),
			filesUnder(path.join(this.artifacts.root, "sha256")),
		]);
		const certificates = (await Promise.all(certificatePaths.filter((target) => target.endsWith(".json")).map(async (target) => {
			const bytes = await readFile(target).catch(() => undefined);
			if (!bytes) return undefined;
			let value: unknown;
			try { value = JSON.parse(bytes.toString("utf8")); } catch { value = undefined; }
			return { path: target, bytes: bytes.byteLength, modifiedAt: 0, certificate: parseProcessCertificate(value) };
		}))).filter((record): record is StoredCertificateFile => Boolean(record));
		const artifacts = new Map<Sha256Digest, StoredFile>();
		const artifactRoot = path.join(this.artifacts.root, "sha256");
		for (const record of (await Promise.all(artifactPaths.map(async (target) => {
			const value = await stat(target).catch(() => undefined);
			return value ? { path: target, bytes: value.size, modifiedAt: value.mtimeMs } : undefined;
		}))).filter((record): record is StoredFile => Boolean(record))) {
			const target = record.path;
			const hex = path.relative(artifactRoot, target).split(path.sep).join("");
			if (/^[0-9a-f]{64}$/.test(hex)) artifacts.set(`sha256:${hex}` as Sha256Digest, record);
		}
		return { certificates, artifacts };
	}

	private exclusive<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.maintenance.then(operation, operation);
		this.maintenance = result.then(() => undefined, () => undefined);
		return result;
	}

	private certificatePath(id: Sha256Digest): string {
		const hex = digestHex(id);
		return path.join(this.root, "certificates", hex.slice(0, 2), `${hex.slice(2)}.json`);
	}

	private weakIndexDirectory(weakKey: Sha256Digest): string {
		const hex = digestHex(weakKey);
		return path.join(this.root, "indexes", "weak", hex.slice(0, 2), hex.slice(2));
	}

	private weakReferencePath(weakKey: Sha256Digest, id: Sha256Digest): string {
		return path.join(this.weakIndexDirectory(weakKey), `${digestHex(id)}.ref`);
	}

	private managedPath(segment: typeof STORE_SEGMENTS[number]): string {
		const target = path.resolve(this.root, segment);
		if (path.dirname(target) !== this.root) throw new Error("invalid provenance store root");
		return target;
	}
}

interface StoredFile {
	readonly path: string;
	readonly bytes: number;
	readonly modifiedAt: number;
}

interface StoredCertificateFile extends StoredFile {
	readonly certificate: ProcessProvenanceCertificate | undefined;
}

interface StoreInventory {
	readonly certificates: readonly StoredCertificateFile[];
	readonly artifacts: ReadonlyMap<Sha256Digest, StoredFile>;
}

function inventoryStats(
	inventory: StoreInventory,
	limits: ProvenanceStoreLimits,
	lastGCAt?: number,
): ProvenanceStoreStats {
	const replayable = inventory.certificates.flatMap((record) =>
		record.certificate && certificateReplayable(record.certificate) ? [record.certificate] : [],
	);
	const referenced = new Set(replayable.flatMap(referencedArtifacts).map((reference) => reference.digest));
	const certificateBytes = inventory.certificates.reduce((total, record) => total + record.bytes, 0);
	const artifactBytes = [...inventory.artifacts.values()].reduce((total, record) => total + record.bytes, 0);
	return Object.freeze({
		certificates: inventory.certificates.length,
		weakKeys: new Set(replayable.map((certificate) => certificate.weakKey)).size,
		artifacts: inventory.artifacts.size,
		orphanArtifacts: [...inventory.artifacts.keys()].filter((digest) => !referenced.has(digest)).length,
		certificateBytes,
		artifactBytes,
		totalBytes: certificateBytes + artifactBytes,
		overBudget:
			inventory.certificates.length > limits.maxCertificates ||
			certificateBytes + artifactBytes > limits.maxBytes,
		...(lastGCAt === undefined ? {} : { lastGCAt }),
		limits,
	});
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

async function publishImmutable(target: string, bytes: Uint8Array): Promise<boolean> {
	await mkdir(path.dirname(target), { recursive: true });
	const temporary = path.join(path.dirname(target), `.${path.basename(target)}.${process.pid}.${randomUUID()}.tmp`);
	await writeFile(temporary, bytes, { flag: "wx" });
	let published = true;
	try {
		await link(temporary, target);
	} catch (error) {
		if (!alreadyExists(error)) throw error;
		published = false;
	} finally {
		await unlink(temporary).catch((error) => {
			if (!missing(error)) throw error;
		});
	}
	return published;
}

function digestHex(digest: Sha256Digest): string {
	if (!isSha256Digest(digest)) throw new Error("invalid sha256 digest");
	return digest.slice("sha256:".length);
}

function missing(error: unknown): boolean {
	return hasCode(error, "ENOENT");
}

function alreadyExists(error: unknown): boolean {
	return hasCode(error, "EEXIST");
}

function hasCode(error: unknown, code: string): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === code);
}

function positiveInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function nonNegativeNumber(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : fallback;
}
