import { createHash } from "node:crypto";
import type { FileHandle } from "node:fs/promises";
import { CONTENT_CHUNK_BYTES, parseContentManifest, type ChunkedContentManifest, type ArtifactReference } from "./artifact-reference.ts";
export { CONTENT_CHUNK_BYTES, parseContentManifest, type ChunkedContentManifest } from "./artifact-reference.ts";
import { ArtifactReadView, type ArtifactTransfer, type ProvenanceCertificateStore, type VerifiedArtifactClosure } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { isSafeCount } from "./number-utils.ts";
import { sha256Digest, sha256DigestAsync, type Sha256Digest } from "./stable-value-hash.ts";

export interface ContentChunkLimits {
	readonly maxLogicalBytes: number; readonly maxStoredBytes: number; readonly maxOpenChunks: number; readonly maxReadBytes: number;
}
export const DEFAULT_CONTENT_LIMITS: ContentChunkLimits = Object.freeze({ maxLogicalBytes: 2 ** 31, maxStoredBytes: 2 ** 30, maxOpenChunks: 1024, maxReadBytes: 16 * CONTENT_CHUNK_BYTES });
let zeroChunk: Buffer | undefined;

/** One view owns one bounded, serialized I/O lane and retains each distinct content block once. */
export class ContentChunkPool {
	readonly limits: ContentChunkLimits;
	private readonly store: ProvenanceCertificateStore;
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly artifacts: ArtifactReadView;
	private owners = 0;
	constructor(store: ProvenanceCertificateStore, limits: Partial<ContentChunkLimits> = {}) {
		if (process.platform !== "linux") throw new Error("chunked input leases require qualified Linux filesystem semantics");
		this.store = store; this.limits = Object.freeze({ ...DEFAULT_CONTENT_LIMITS, ...limits });
		if (!isSafeCount(this.limits.maxLogicalBytes) || this.limits.maxLogicalBytes > 65536 * CONTENT_CHUNK_BYTES) throw new Error("invalid content chunk budget");
		this.artifacts = new ArtifactReadView(store.artifacts, this.limits);
	}
	get storedBytes(): number { return this.artifacts.bytes; }
	get openChunks(): number { return this.artifacts.artifacts; }
	get closed(): boolean { return this.lifetime.sealed; }
	get storeRoot(): string { return this.store.root; }
	retain(): () => Promise<void> {
		if (this.lifetime.sealed) throw new Error("content lease is closed"); this.owners++; let held = true;
		return () => { if (held) { held = false; if (!--this.owners) return this.dispose(); } return Promise.resolve(); };
	}
	lend() { return this.artifacts.lend(); }
	async capture<T extends { readonly bytesRead: number; readonly hash: string }>(read: (sink: (chunk: Buffer) => Promise<void>) => Promise<T>): Promise<{ capture: T; content: ChunkedContent }> {
		return this.lifetime.serialize(async () => {
			const chunks: (ArtifactReference | null)[] = [], buffer = Buffer.allocUnsafe(CONTENT_CHUNK_BYTES);
			let offset = 0, size = 0;
			const flush = async () => {
				const bytes = buffer.subarray(0, offset);
				if (bytes.equals((zeroChunk ??= Buffer.alloc(CONTENT_CHUNK_BYTES)).subarray(0, offset))) chunks.push(null);
				else {
					const reference = { digest: await sha256DigestAsync(bytes), size: bytes.length };
					this.artifacts.checkBudget(reference);
					await this.store.withPublication(async () => { await this.store.artifacts.put(bytes); await this.pin(reference); }); chunks.push(reference);
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
		});
	}
	/** Reopen and verify every block and the complete digest before granting a cold input lease. */
	restore(value: unknown, transfers?: readonly ArtifactTransfer[]): Promise<ChunkedContent> {
		return this.lifetime.serialize(async () => {
			const manifest = parseContentManifest(value);
			if (manifest.size > this.limits.maxLogicalBytes) throw new Error("content logical budget exceeded");
			if (transfers && !await this.artifacts.restore(manifest.chunks.filter(reference => reference !== null), transfers)) {
				throw new Error("content transfer is unavailable");
			}
			const digest = createHash("sha256");
			for (let index = 0; index < manifest.chunks.length; index++) {
				const reference = manifest.chunks[index]; if (reference) await this.store.withPublication(() => this.pin(reference));
				digest.update(await this.block(manifest, index));
			}
			if (`sha256:${digest.digest("hex")}` !== manifest.digest) throw new Error("content manifest integrity check failed");
			return new ChunkedContent(this, manifest);
		});
	}
	/** Renew roots under the caller's publication transaction, even after GC unlinked our pinned blocks. */
	publish(manifest: ChunkedContentManifest, store = this.store): Promise<void> {
		return this.lifetime.admit(() => store.withPublication(async () => {
			const published = new Set<Sha256Digest>();
			for (let index = 0; index < manifest.chunks.length; index++) {
				const reference = manifest.chunks[index];
				if (reference && !published.has(reference.digest)) {
					await store.artifacts.put(await this.block(manifest, index)); published.add(reference.digest);
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
			await this.lifetime.drain(); await this.artifacts.dispose();
		});
	}
	private async pin(reference: ArtifactReference): Promise<void> {
		if (!await this.artifacts.add(reference)) throw new Error("content chunk is unavailable");
	}
	private async block(manifest: ChunkedContentManifest, index: number): Promise<Buffer> {
		const reference = manifest.chunks[index], size = Math.min(CONTENT_CHUNK_BYTES, manifest.size - index * CONTENT_CHUNK_BYTES);
		if (!reference) return (zeroChunk ??= Buffer.alloc(CONTENT_CHUNK_BYTES)).subarray(0, size);
		return this.artifacts.readRange(reference, 0, size);
	}
}

export class ChunkedContent {
	readonly manifest: ChunkedContentManifest;
	readonly pool: ContentChunkPool;
	constructor(pool: ContentChunkPool, manifest: ChunkedContentManifest) { this.pool = pool; this.manifest = manifest; }
	get length(): number { return this.manifest.size; }
	get byteLength(): number { return this.manifest.size; }
	get memoryBytes(): number { return 96 + this.manifest.chunks.length * 112; }
	read(maxBytes?: number): Promise<Buffer> { return this.pool.read(this.manifest, maxBytes); }
	readRange(offset: number, length: number): Promise<Buffer> { return this.pool.read(this.manifest, length, offset); }
	publish(): Promise<void> { return this.pool.publish(this.manifest); }
}

/** Byte arguments remain an adapter boundary; captured Linux file versions use the same immutable chunks as inputs. */
export type ContentVersion = Uint8Array | ChunkedContent;
export const contentDigest = (value: ContentVersion): Sha256Digest => value instanceof ChunkedContent ? value.manifest.digest : sha256Digest(value);
export const sameContent = (left: ContentVersion | undefined, right: ContentVersion | undefined): boolean =>
	left === undefined || right === undefined ? left === right : left.byteLength === right.byteLength && contentDigest(left) === contentDigest(right);
export const ownContent = (value: ContentVersion | undefined): ContentVersion | undefined => value instanceof ChunkedContent ? value : value && Buffer.from(value);
export const contentMemory = (value: ContentVersion | undefined): number => value instanceof ChunkedContent ? value.memoryBytes : value?.byteLength ?? 0;
export const readContent = (value: ContentVersion, offset = 0, length = value.byteLength - offset): Promise<Buffer> =>
	value instanceof ChunkedContent ? value.readRange(offset, length) : Promise.resolve(Buffer.from(value.subarray(offset, offset + length)));
export async function captureContent(pool: ContentChunkPool, value: ContentVersion | string): Promise<ChunkedContent> {
	if (value instanceof ChunkedContent && value.pool === pool) return value;
	return (await pool.capture(async sink => {
		const digest = createHash("sha256"); let bytesRead = 0;
		const size = typeof value === "string" ? value.length : value.byteLength;
		for (let offset = 0; offset < size;) {
			let end = Math.min(size, offset + CONTENT_CHUNK_BYTES);
			if (typeof value === "string" && end < size && value.charCodeAt(end - 1) >= 0xd800 && value.charCodeAt(end - 1) <= 0xdbff) end--;
			const block = typeof value === "string" ? Buffer.from(value.slice(offset, end)) : await readContent(value, offset, end - offset);
			digest.update(block); bytesRead += block.length; await sink(block); offset = end;
		}
		return { bytesRead, hash: digest.digest("hex") };
	})).content;
}
export async function restoreArtifactContent(pool: ContentChunkPool, value: ArtifactReference | ChunkedContentManifest, artifacts: VerifiedArtifactClosure): Promise<ChunkedContent> {
	if ("chunks" in value) { const loan = artifacts.lend(); try { return await pool.restore(value, loan.transfer); } finally { loan.release(); } }
	return (await pool.capture(async sink => {
		for (let offset = 0; offset < value.size; offset += CONTENT_CHUNK_BYTES) await sink(await artifacts.readRange(value, offset, Math.min(CONTENT_CHUNK_BYTES, value.size - offset)));
		return { bytesRead: value.size, hash: value.digest.slice(7) };
	})).content;
}
/** Every new file effect publishes a manifest into the certificate owner's existing CAS transaction. */
export function publishContent(value: ContentVersion, store: ProvenanceCertificateStore): Promise<ChunkedContentManifest> {
	if (value instanceof ChunkedContent) return value.pool.publish(value.manifest, store).then(() => value.manifest);
	return store.withPublication(async () => {
		const chunks: (ArtifactReference | null)[] = [];
		for (let offset = 0; offset < value.byteLength; offset += CONTENT_CHUNK_BYTES) {
			const length = Math.min(CONTENT_CHUNK_BYTES, value.byteLength - offset);
			const bytes = await readContent(value, offset, length);
			if (bytes.equals((zeroChunk ??= Buffer.alloc(CONTENT_CHUNK_BYTES)).subarray(0, length))) chunks.push(null);
			else chunks.push(await store.artifacts.put(bytes));
		}
		return parseContentManifest({ size: value.byteLength, digest: contentDigest(value), chunks });
	});
}
/** Apply changed logical ranges to the existing object, keeping its aliases and open descriptions attached. */
export async function writeContent(handle: FileHandle, after: ContentVersion, before?: ContentVersion): Promise<void> {
	const previous = before instanceof ChunkedContent ? before.manifest : undefined, next = after instanceof ChunkedContent ? after.manifest : undefined;
	let written = false;
	if (!before || before.byteLength < after.byteLength) { await handle.truncate(after.byteLength); written = true; }
	for (let offset = 0; offset < after.byteLength; offset += CONTENT_CHUNK_BYTES) {
		const index = offset / CONTENT_CHUNK_BYTES, length = Math.min(CONTENT_CHUNK_BYTES, after.byteLength - offset);
		if (next && (previous && previous.size >= offset + length && next.chunks[index]?.digest === previous.chunks[index]?.digest ||
			!before && next.chunks[index] === null || before && offset >= before.byteLength && next.chunks[index] === null)) continue;
		const block = await readContent(after, offset, length);
		for (let written = 0; written < block.length;) { const { bytesWritten } = await handle.write(block, written, block.length - written, offset + written); if (!bytesWritten) throw new Error("content write made no progress"); written += bytesWritten; }
		written = true;
	}
	if (!written || before && before.byteLength > after.byteLength) await handle.truncate(after.byteLength);
}

/** The ordinary record stays cloneable; content manifests borrow the same object leases as effect closures. */
export interface ContentChangesTransfer<Change> {
	readonly changes: readonly Change[];
	readonly contents: readonly { readonly index: number; readonly before?: ChunkedContentManifest; readonly after?: ChunkedContentManifest }[];
	readonly artifacts: readonly ArtifactTransfer[];
}
export function lendContentChanges<Change extends { readonly before?: unknown; readonly after?: unknown }>(changes: readonly Change[]) {
	const pools = new Set<ContentChunkPool>(), contents: ContentChangesTransfer<Change>["contents"][number][] = [];
	const records = changes.map((change, index) => {
		const before = change.before instanceof ChunkedContent ? change.before : undefined, after = change.after instanceof ChunkedContent ? change.after : undefined;
		if (!before && !after) return change;
		for (const value of [before, after]) if (value) pools.add(value.pool);
		contents.push({ index, before: before?.manifest, after: after?.manifest });
		return { ...change, before: before ? undefined : change.before, after: after ? undefined : change.after };
	});
	const loans: ReturnType<ContentChunkPool["lend"]>[] = [], release = () => { for (const loan of loans) loan.release(); };
	try {
		for (const pool of pools) loans.push(pool.lend());
		const artifacts = [...new Map(loans.flatMap(loan => loan.transfer).map(value => [value.reference.digest, value])).values()];
		return { transfer: { changes: records, contents, artifacts } as ContentChangesTransfer<Change>, release };
	} catch (error) { release(); throw error; }
}
export async function receiveContentChanges<Change extends { readonly before?: unknown; readonly after?: unknown }>(value: ContentChangesTransfer<Change>, pool?: ContentChunkPool): Promise<Change[]> {
	const changes = [...value.changes], restored = new Set<number>();
	if (!pool && value.contents.length) throw new Error("content receiver has no storage owner");
	for (const content of value.contents) {
		if (!isSafeCount(content.index) || !changes[content.index] || restored.has(content.index)) throw new Error("invalid content change transfer");
		restored.add(content.index);
		const change = changes[content.index]!;
		if (content.before && change.before !== undefined || content.after && change.after !== undefined) throw new Error("conflicting content change transfer");
		changes[content.index] = { ...change,
			before: content.before ? await pool!.restore(content.before, value.artifacts) : change.before,
			after: content.after ? await pool!.restore(content.after, value.artifacts) : change.after };
	}
	return changes;
}
