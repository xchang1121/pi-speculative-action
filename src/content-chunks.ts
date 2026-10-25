import { createHash } from "node:crypto";
import { open, type FileHandle } from "node:fs/promises";
import { type ArtifactReference, validateArtifact } from "./artifact-reference.ts";
import type { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { isSafeCount } from "./number-utils.ts";
import { isSha256Digest, sha256DigestAsync, type Sha256Digest } from "./stable-value-hash.ts";

export const CONTENT_CHUNK_BYTES = 1024 * 1024;
export interface ContentChunkLimits {
	readonly maxLogicalBytes: number; readonly maxStoredBytes: number; readonly maxOpenChunks: number; readonly maxReadBytes: number;
}
export interface ChunkedContentManifest {
	readonly size: number; readonly digest: Sha256Digest; readonly chunks: readonly (ArtifactReference | null)[];
}
const DEFAULT_LIMITS: ContentChunkLimits = { maxLogicalBytes: 2 ** 31, maxStoredBytes: 2 ** 30, maxOpenChunks: 1024, maxReadBytes: 16 * CONTENT_CHUNK_BYTES };
let zeroChunk: Buffer | undefined;

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

/** One view owns one bounded, serialized I/O lane and pins each distinct CAS object once. */
export class ContentChunkPool {
	readonly limits: ContentChunkLimits;
	private readonly store: ProvenanceCertificateStore;
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly handles = new Map<Sha256Digest, { readonly handle: FileHandle; readonly size: number }>();
	private storedBytes = 0;
	constructor(store: ProvenanceCertificateStore, limits: Partial<ContentChunkLimits> = {}) {
		if (process.platform !== "linux") throw new Error("chunked input leases require qualified Linux filesystem semantics");
		this.store = store; this.limits = Object.freeze({ ...DEFAULT_LIMITS, ...limits });
		if (Object.values(this.limits).some(value => !isSafeCount(value)) || this.limits.maxLogicalBytes > 65536 * CONTENT_CHUNK_BYTES ||
			this.limits.maxReadBytes > 2 ** 30 || this.limits.maxOpenChunks > 65536) throw new Error("invalid content chunk budget");
	}
	async capture<T extends { readonly bytesRead: number; readonly hash: string }>(read: (sink: (chunk: Buffer) => Promise<void>) => Promise<T>): Promise<{ capture: T; content: ChunkedContent }> {
		return this.store.withPublication(() => this.lifetime.serialize(async () => {
			const chunks: (ArtifactReference | null)[] = [], buffer = Buffer.allocUnsafe(CONTENT_CHUNK_BYTES);
			let offset = 0, size = 0;
			const flush = async () => {
				const bytes = buffer.subarray(0, offset);
				if (bytes.equals((zeroChunk ??= Buffer.alloc(CONTENT_CHUNK_BYTES)).subarray(0, offset))) chunks.push(null);
				else {
					const reference = { digest: await sha256DigestAsync(bytes), size: bytes.length };
					this.checkBudget(reference);
					await this.store.artifacts.put(bytes); await this.pin(reference); chunks.push(reference);
				}
				offset = 0;
			};
			const capture = await read(async chunk => {
				if (size + chunk.length > this.limits.maxLogicalBytes) throw new Error("content logical budget exceeded");
				size += chunk.length;
				for (let cursor = 0; cursor < chunk.length;) {
					const count = Math.min(chunk.length - cursor, buffer.length - offset);
					chunk.copy(buffer, offset, cursor, cursor + count); cursor += count; offset += count;
					if (offset === buffer.length) await flush();
				}
			});
			if (offset) await flush();
			if (size !== capture.bytesRead) throw new Error("content capture size mismatch");
			return { capture, content: new ChunkedContent(this, parseContentManifest({ size, digest: `sha256:${capture.hash}`, chunks })) };
		}));
	}
	/** Reopen and verify every block and the complete digest before granting a cold input lease. */
	restore(value: unknown): Promise<ChunkedContent> {
		return this.store.withPublication(() => this.lifetime.serialize(async () => {
			const manifest = parseContentManifest(value);
			if (manifest.size > this.limits.maxLogicalBytes) throw new Error("content logical budget exceeded");
			const digest = createHash("sha256");
			for (let index = 0; index < manifest.chunks.length; index++) {
				const reference = manifest.chunks[index]; if (reference) await this.pin(reference);
				digest.update(await this.block(manifest, index));
			}
			if (`sha256:${digest.digest("hex")}` !== manifest.digest) throw new Error("content manifest integrity check failed");
			return new ChunkedContent(this, manifest);
		}));
	}
	/** Renew roots under the caller's publication transaction, even after GC unlinked our pinned blocks. */
	publish(manifest: ChunkedContentManifest): Promise<void> {
		return this.store.withPublication(() => this.lifetime.serialize(async () => {
			const published = new Set<Sha256Digest>();
			for (let index = 0; index < manifest.chunks.length; index++) {
				const reference = manifest.chunks[index];
				if (reference && !published.has(reference.digest)) {
					await this.store.artifacts.put(await this.block(manifest, index)); published.add(reference.digest);
				}
			}
		}));
	}
	read(manifest: ChunkedContentManifest, maxBytes?: number, offset = 0): Promise<Buffer> {
		return this.lifetime.serialize(async () => {
			if (!isSafeCount(offset) || maxBytes !== undefined && !isSafeCount(maxBytes)) throw new Error("invalid content read budget");
			const size = Math.min(Math.max(0, manifest.size - offset), maxBytes ?? manifest.size);
			if (size > this.limits.maxReadBytes) throw new Error("content read budget exceeded; request a bounded prefix");
			const result = Buffer.allocUnsafe(size);
			for (let copied = 0; copied < size;) {
				const position = offset + copied, index = Math.floor(position / CONTENT_CHUNK_BYTES), start = position % CONTENT_CHUNK_BYTES;
				const block = await this.block(manifest, index), count = Math.min(block.length - start, size - copied);
				block.copy(result, copied, start, start + count); copied += count;
			}
			return result;
		});
	}
	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			await this.lifetime.drain(); await Promise.allSettled([...this.handles.values()].map(entry => entry.handle.close())); this.handles.clear();
		});
	}
	private checkBudget(reference: ArtifactReference): void {
		const owned = this.handles.get(reference.digest);
		if (owned && owned.size !== reference.size) throw new Error("content chunk size conflict");
		if (!owned && (this.storedBytes + reference.size > this.limits.maxStoredBytes || this.handles.size >= this.limits.maxOpenChunks)) throw new Error("content stored-byte or descriptor budget exceeded");
	}
	private async pin(reference: ArtifactReference): Promise<void> {
		this.checkBudget(reference); if (this.handles.has(reference.digest)) return;
		const handle = await open(this.store.artifacts.artifactPath(reference.digest), "r");
		try {
			const stat = await handle.stat();
			if (!stat.isFile() || stat.size !== reference.size) throw new Error("content chunk identity changed");
			this.handles.set(reference.digest, { handle, size: reference.size }); this.storedBytes += reference.size;
		} catch (error) { await handle.close(); throw error; }
	}
	private async block(manifest: ChunkedContentManifest, index: number): Promise<Buffer> {
		const reference = manifest.chunks[index], size = Math.min(CONTENT_CHUNK_BYTES, manifest.size - index * CONTENT_CHUNK_BYTES);
		if (!reference) return Buffer.alloc(size);
		const pinned = this.handles.get(reference.digest);
		if (!pinned || pinned.size !== size) throw new Error("content block is outside the input lease");
		const buffer = Buffer.allocUnsafe(size);
		for (let offset = 0; offset < size;) {
			const { bytesRead } = await pinned.handle.read(buffer, offset, size - offset, offset);
			if (!bytesRead) throw new Error("content chunk truncated"); offset += bytesRead;
		}
		if (await sha256DigestAsync(buffer) !== reference.digest) throw new Error("content chunk integrity check failed");
		return buffer;
	}
}

export class ChunkedContent {
	readonly manifest: ChunkedContentManifest;
	private readonly pool: ContentChunkPool;
	constructor(pool: ContentChunkPool, manifest: ChunkedContentManifest) { this.pool = pool; this.manifest = manifest; }
	get length(): number { return this.manifest.size; }
	get memoryBytes(): number { return 96 + this.manifest.chunks.length * 112; }
	read(maxBytes?: number): Promise<Buffer> { return this.pool.read(this.manifest, maxBytes); }
	readRange(offset: number, length: number): Promise<Buffer> { return this.pool.read(this.manifest, length, offset); }
	publish(): Promise<void> { return this.pool.publish(this.manifest); }
}
