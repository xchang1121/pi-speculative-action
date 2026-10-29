import { isSha256Digest, type Sha256Digest } from "./stable-value-hash.ts";
import { isSafeCount } from "./number-utils.ts";

export interface ArtifactReference { readonly digest: Sha256Digest; readonly size: number; }

/** The same immutable content identity for input versions and effect closures. */
export function validateArtifact(reference: ArtifactReference, sizes?: Map<Sha256Digest, number>): void {
	if (!reference || !isSha256Digest(reference.digest) || !isSafeCount(reference.size)) throw new Error("invalid content artifact");
	const previousSize = sizes?.get(reference.digest);
	if (previousSize !== undefined && previousSize !== reference.size) throw new Error("conflicting content artifact sizes");
	sizes?.set(reference.digest, reference.size);
}

export const CONTENT_CHUNK_BYTES = 1024 * 1024;

/** Read a validated range from fixed-size content blocks, including a short final block. */
export async function readChunkRange(offset: number, length: number, block: (index: number) => Promise<Buffer>): Promise<Buffer> {
	const result = Buffer.allocUnsafe(length);
	for (let copied = 0; copied < length;) {
		const position = offset + copied, index = Math.floor(position / CONTENT_CHUNK_BYTES), start = position % CONTENT_CHUNK_BYTES;
		const bytes = await block(index), count = Math.min(length - copied, bytes.length - start);
		bytes.copy(result, copied, start, start + count); copied += count;
	}
	return result;
}

export interface ChunkedContentManifest {
	readonly size: number; readonly digest: Sha256Digest; readonly chunks: readonly (ArtifactReference | null)[];
}

/** Zero blocks describe bytes, not physical holes. No runtime descriptor is serializable. */
export function parseContentManifest(value: unknown): ChunkedContentManifest {
	const manifest = value as ChunkedContentManifest;
	if (!manifest || !isSafeCount(manifest.size) || !isSha256Digest(manifest.digest) || !Array.isArray(manifest.chunks) ||
		manifest.chunks.length > 65536 || manifest.chunks.length !== Math.ceil(manifest.size / CONTENT_CHUNK_BYTES)) throw new Error("invalid content manifest");
	const sizes = new Map<Sha256Digest, number>();
	return Object.freeze({ size: manifest.size, digest: manifest.digest, chunks: Object.freeze(manifest.chunks.map((reference, index) => {
		if (reference === null) return null;
		validateArtifact(reference, sizes);
		if (reference.size !== Math.min(CONTENT_CHUNK_BYTES, manifest.size - index * CONTENT_CHUNK_BYTES)) throw new Error("invalid content chunk size");
		return Object.freeze({ digest: reference.digest, size: reference.size });
	})) });
}
