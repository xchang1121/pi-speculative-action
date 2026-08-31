import { randomUUID } from "node:crypto";
import { link, mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	type ArtifactReference,
	isSha256Digest,
	parseProcessCertificate,
	type ProcessProvenanceCertificate,
	referencedArtifacts,
	sha256Digest,
	type Sha256Digest,
} from "./provenance-certificate.ts";
import { stableStringify } from "./stable-json.ts";

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

	private artifactPath(digest: Sha256Digest): string {
		const hex = digestHex(digest);
		return path.join(this.root, "sha256", hex.slice(0, 2), hex.slice(2));
	}
}

/** Persistent L2 certificate/pathset index kept separate from the Runtime's in-memory ResultCache. */
export class ProvenanceCertificateStore {
	readonly root: string;
	readonly artifacts: ArtifactCAS;

	constructor(root: string) {
		this.root = path.resolve(root);
		this.artifacts = new ArtifactCAS(path.join(this.root, "cas"));
	}

	async put(certificate: ProcessProvenanceCertificate): Promise<void> {
		const parsed = parseProcessCertificate(certificate);
		if (!parsed || parsed.id !== certificate.id) throw new Error("invalid process provenance certificate");
		for (const reference of referencedArtifacts(certificate)) {
			if (!(await this.artifacts.has(reference))) {
				throw new Error(`certificate references missing artifact ${reference.digest}`);
			}
		}
		await publishImmutable(
			this.certificatePath(certificate.id),
			Buffer.from(stableStringify(certificate), "utf8"),
		);
		if ((await this.get(certificate.id))?.id !== certificate.id) throw new Error("certificate publication failed");
		const index = this.weakIndexDirectory(certificate.weakKey);
		await mkdir(index, { recursive: true });
		try {
			await writeFile(path.join(index, `${digestHex(certificate.id)}.ref`), "", { flag: "wx" });
		} catch (error) {
			if (!alreadyExists(error)) throw error;
		}
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
			const certificate = await this.get(`sha256:${match[1]}` as Sha256Digest);
			if (certificate?.weakKey === weakKey) certificates.push(certificate);
		}
		certificates.sort((left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id));
		return certificates;
	}

	private certificatePath(id: Sha256Digest): string {
		const hex = digestHex(id);
		return path.join(this.root, "certificates", hex.slice(0, 2), `${hex.slice(2)}.json`);
	}

	private weakIndexDirectory(weakKey: Sha256Digest): string {
		const hex = digestHex(weakKey);
		return path.join(this.root, "indexes", "weak", hex.slice(0, 2), hex.slice(2));
	}
}

async function publishImmutable(target: string, bytes: Uint8Array): Promise<void> {
	await mkdir(path.dirname(target), { recursive: true });
	const temporary = path.join(path.dirname(target), `.${path.basename(target)}.${process.pid}.${randomUUID()}.tmp`);
	await writeFile(temporary, bytes, { flag: "wx" });
	try {
		await link(temporary, target);
	} catch (error) {
		if (!alreadyExists(error)) throw error;
	} finally {
		await unlink(temporary).catch((error) => {
			if (!missing(error)) throw error;
		});
	}
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
