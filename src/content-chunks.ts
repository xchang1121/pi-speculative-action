import { createHash } from "node:crypto";
import { serialize } from "node:v8";
import type { FileHandle } from "node:fs/promises";
import { DEFAULT_SPECULATIVE_CONFIG, speculativeConfig } from "./config-schema.ts";
import type { StableFilesystemCapture } from "./filesystem-evidence.ts";
import { captureStableFile } from "./filesystem-evidence.ts";
import type { Sha256Digest } from "./primitives.ts";
import { isSafeCount, isSha256Digest, sha256Digest, sha256DigestAsync } from "./primitives.ts";
import type { ArtifactSource, ProvenanceCertificateStore, VerifiedArtifactClosure } from "./reuse-store.ts";
import { ArtifactReadView } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
export const CONTENT_CHUNK_BYTES = DEFAULT_SPECULATIVE_CONFIG.filesystem.chunkBytes;
export interface ContentChunkLimits {
	readonly maxLogicalBytes: number;
	readonly maxStoredBytes: number;
	readonly maxOpenChunks: number;
	readonly maxReadBytes: number;
}
export const DEFAULT_CONTENT_LIMITS: ContentChunkLimits = Object.freeze({
	maxLogicalBytes: DEFAULT_SPECULATIVE_CONFIG.filesystem.maxLogicalBytes,
	maxStoredBytes: DEFAULT_SPECULATIVE_CONFIG.filesystem.maxStoredBytes,
	maxOpenChunks: DEFAULT_SPECULATIVE_CONFIG.filesystem.maxOpenChunks,
	maxReadBytes: DEFAULT_SPECULATIVE_CONFIG.filesystem.maxReadBytes,
});
const contentBlockBytes = (manifest: ChunkedContentManifest): number =>
	manifest.chunkBytes ?? CONTENT_CHUNK_BYTES;
const verifiedManifests = new WeakSet<ChunkedContentManifest>();
/** One view owns one bounded, serialized I/O lane and retains each distinct content block once. */
export class ContentChunkPool {
	readonly limits: ContentChunkLimits;
	private readonly store: ProvenanceCertificateStore;
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly artifacts: ArtifactReadView;
	private owners = 0;
	constructor(store: ProvenanceCertificateStore, limits: Partial<ContentChunkLimits> = {}) {
		this.store = store;
		this.limits = Object.freeze({
			maxLogicalBytes: speculativeConfig().filesystem.maxLogicalBytes,
			maxStoredBytes: speculativeConfig().filesystem.maxStoredBytes,
			maxOpenChunks: speculativeConfig().filesystem.maxOpenChunks,
			maxReadBytes: speculativeConfig().filesystem.maxReadBytes,
			...limits,
		});
		if (!isSafeCount(this.limits.maxLogicalBytes)) throw new Error("invalid content chunk budget");
		this.artifacts = this.lifetime.own(new ArtifactReadView(store.artifacts, this.limits));
	}
	get storedBytes(): number {
		return this.artifacts.bytes;
	}
	get openChunks(): number {
		return this.artifacts.artifacts;
	}
	get closed(): boolean {
		return this.lifetime.sealed;
	}
	get storeRoot(): string {
		return this.store.root;
	}
	retain(): () => Promise<void> {
		if (this.lifetime.sealed) throw new Error("content lease is closed");
		this.owners++;
		let held = true;
		return () => {
			if (held) {
				held = false;
				if (!--this.owners) return this.dispose();
			}
			return Promise.resolve();
		};
	}
	lend() {
		return this.artifacts.lend();
	}
	async capture<
		T extends {
			readonly bytesRead: number;
		},
	>(
		read: (sink: (chunk: Buffer) => Promise<void>) => Promise<T>,
	): Promise<{
		capture: T & {
			readonly hash: string;
		};
		content: ChunkedContent;
	}> {
		return this.store.read((signal) =>
			this.lifetime.serialize(async () => {
				const { capture, manifest } = await captureContentBlocks(
					read,
					async (bytes) => {
						const reference = {
							digest: await sha256DigestAsync(bytes),
							size: bytes.length,
						};
						await this.artifacts.add(reference, signal, { reference, bytes });
						return reference;
					},
					this.limits.maxLogicalBytes,
					signal,
				);
				return { capture, content: new ChunkedContent(this, manifest) };
			}),
		);
	}
	/** Reopen and verify every block and the complete digest before granting a cold input lease. */
	restore(
		value: unknown,
		transfers?: ArtifactSource | ContentChunkPool,
		signal?: AbortSignal,
	): Promise<ChunkedContent> {
		return this.store.read(
			(signal) =>
				this.lifetime.serialize(async () => {
					const manifest = verifiedManifests.has(value as ChunkedContentManifest)
						? (value as ChunkedContentManifest)
						: parseContentManifest(value);
					if (transfers instanceof ContentChunkPool && transfers.closed)
						throw new Error("content lease is closed");
					if (manifest.size > this.limits.maxLogicalBytes) throw new Error("content logical budget exceeded");
					if (
						!(await this.artifacts.restore(
							manifest.chunks.filter((reference) => reference !== null),
							transfers instanceof ContentChunkPool ? transfers.artifacts : transfers,
							signal,
						))
					) {
						throw new Error("content transfer is unavailable");
					}
					if (!verifiedManifests.has(manifest)) {
						const digest = createHash("sha256");
						for (let index = 0; index < manifest.chunks.length; index++) {
							signal.throwIfAborted();
							digest.update(await this.block(manifest, index));
						}
						if (`sha256:${digest.digest("hex")}` !== manifest.digest)
							throw new Error("content manifest integrity check failed");
						verifiedManifests.add(manifest);
					}
					signal.throwIfAborted();
					return new ChunkedContent(this, manifest);
				}),
			signal,
		);
	}
	/** Renew roots under the caller's publication transaction, even after GC unlinked our pinned blocks. */
	publish(manifest: ChunkedContentManifest, store = this.store): Promise<void> {
		return this.lifetime.admit(() =>
			store.withPublication(() =>
				store.artifacts.share(
					manifest.chunks.filter((reference) => reference !== null),
					this.artifacts,
				),
			),
		);
	}
	read(manifest: ChunkedContentManifest, maxBytes?: number, offset = 0): Promise<Buffer> {
		return this.lifetime.serialize(async () => {
			if (!isSafeCount(offset) || (maxBytes !== undefined && !isSafeCount(maxBytes)))
				throw new Error("invalid content read budget");
			const size = Math.min(Math.max(0, manifest.size - offset), maxBytes ?? manifest.size);
			if (size > this.limits.maxReadBytes)
				throw new Error("content read budget exceeded; request a bounded prefix");
			const result = Buffer.allocUnsafe(size);
			const chunkBytes = contentBlockBytes(manifest);
			for (let copied = 0; copied < size;) {
				const position = offset + copied;
				const start = position % chunkBytes;
				const count = Math.min(size - copied, chunkBytes - start);
				const reference = manifest.chunks[Math.floor(position / chunkBytes)];
				if (reference) (await this.artifacts.readRange(reference, start, count)).copy(result, copied);
				else result.fill(0, copied, copied + count);
				copied += count;
			}
			return result;
		});
	}
	dispose(): Promise<void> {
		return this.lifetime.dispose();
	}
	private async block(manifest: ChunkedContentManifest, index: number): Promise<Buffer> {
		const chunkBytes = contentBlockBytes(manifest);
		const reference = manifest.chunks[index];
		const size = Math.min(chunkBytes, manifest.size - index * chunkBytes);
		if (!reference) return Buffer.alloc(size);
		return this.artifacts.readRange(reference, 0, size);
	}
}
export class ChunkedContent {
	readonly manifest: ChunkedContentManifest;
	readonly pool: ContentChunkPool;
	constructor(pool: ContentChunkPool, manifest: ChunkedContentManifest) {
		this.pool = pool;
		this.manifest = manifest;
	}
	get length(): number {
		return this.manifest.size;
	}
	get byteLength(): number {
		return this.manifest.size;
	}
	get memoryBytes(): number {
		return serialize(this.manifest).byteLength;
	}
	read(maxBytes?: number): Promise<Buffer> {
		return this.pool.read(this.manifest, maxBytes);
	}
	readRange(offset: number, length: number): Promise<Buffer> {
		return this.pool.read(this.manifest, length, offset);
	}
	publish(): Promise<void> {
		return this.pool.publish(this.manifest);
	}
}
/** Byte arguments remain an adapter boundary; captured Linux file versions use the same immutable chunks as inputs. */
export type ContentVersion = Uint8Array | ChunkedContent;
export type FileContentVersion = Omit<StableFilesystemCapture, "content"> & {
	readonly content: ContentVersion;
};
/** A private preimage and a workspace input retain the same sealed blocks. */
export async function captureFileContent(
	target: string,
	maxBytes: number,
	pool?: ContentChunkPool,
	observed?: Omit<NonNullable<Parameters<typeof captureStableFile>[3]>, "onChunk">,
): Promise<FileContentVersion> {
	if (pool) {
		const { capture, content } = await pool.capture((onChunk) =>
			captureStableFile(target, Math.min(maxBytes, pool.limits.maxLogicalBytes), false, {
				...observed,
				digest: false,
				onChunk,
			}),
		);
		return { ...capture, content };
	}
	const captured = await captureStableFile(target, maxBytes, true, observed);
	return { ...captured, content: captured.content! };
}
export const contentDigest = (value: ContentVersion): Sha256Digest =>
	value instanceof ChunkedContent ? value.manifest.digest : sha256Digest(value);
export const sameContent = (left: ContentVersion | undefined, right: ContentVersion | undefined): boolean =>
	left === undefined || right === undefined
		? left === right
		: left.byteLength === right.byteLength && contentDigest(left) === contentDigest(right);
export const ownContent = (value: ContentVersion | undefined): ContentVersion | undefined =>
	value instanceof ChunkedContent ? value : value && Buffer.from(value);
export const contentMemory = (value: ContentVersion | undefined): number =>
	value instanceof ChunkedContent ? value.memoryBytes : (value?.byteLength ?? 0);
export const readContent = (
	value: ContentVersion,
	offset = 0,
	length = value.byteLength - offset,
): Promise<Buffer> =>
	value instanceof ChunkedContent
		? value.readRange(offset, length)
		: Promise.resolve(Buffer.from(value.subarray(offset, offset + length)));
export async function captureContent(
	pool: ContentChunkPool,
	value: ContentVersion | string,
): Promise<ChunkedContent> {
	if (value instanceof ChunkedContent)
		return value.pool === pool ? value : pool.restore(value.manifest, value.pool);
	return (
		await pool.capture(async (sink) => {
			let bytesRead = 0;
			const size = typeof value === "string" ? value.length : value.byteLength;
			for (let offset = 0; offset < size;) {
				let end = Math.min(size, offset + speculativeConfig().filesystem.chunkBytes);
				if (
					typeof value === "string" &&
					end < size &&
					value.charCodeAt(end - 1) >= 0xd800 &&
					value.charCodeAt(end - 1) <= 0xdbff
				)
					end--;
				const block =
					typeof value === "string"
						? Buffer.from(value.slice(offset, end))
						: await readContent(value, offset, end - offset);
				bytesRead += block.length;
				await sink(block);
				offset = end;
			}
			return { bytesRead };
		})
	).content;
}
export async function restoreArtifactContent(
	pool: ContentChunkPool,
	value: ArtifactReference | ChunkedContentManifest,
	artifacts: VerifiedArtifactClosure,
): Promise<ChunkedContent> {
	if ("chunks" in value) return pool.restore(value, artifacts);
	return (
		await pool.capture(async (sink) => {
			for (let offset = 0; offset < value.size; offset += speculativeConfig().filesystem.chunkBytes)
				await sink(
					await artifacts.readRange(
						value,
						offset,
						Math.min(speculativeConfig().filesystem.chunkBytes, value.size - offset),
					),
				);
			return { bytesRead: value.size };
		})
	).content;
}
/** Every new file effect publishes a manifest into the certificate owner's existing CAS transaction. */
export function publishContent(
	value: ContentVersion,
	store: ProvenanceCertificateStore,
): Promise<ChunkedContentManifest> {
	if (value instanceof ChunkedContent)
		return value.pool.publish(value.manifest, store).then(() => value.manifest);
	return store.withPublication(
		async () =>
			(
				await captureContentBlocks(
					async (sink) => {
						await sink(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
						return { bytesRead: value.byteLength };
					},
					(bytes) => store.artifacts.put(bytes),
					value.byteLength,
				)
			).manifest,
	);
}
/** Capture and persistence share one bounded chunker, including zero blocks and partial final blocks. */
async function captureContentBlocks<
	T extends {
		readonly bytesRead: number;
	},
>(
	read: (sink: (chunk: Buffer) => Promise<void>) => Promise<T>,
	put: (bytes: Buffer) => Promise<ArtifactReference>,
	maxBytes: number,
	signal?: AbortSignal,
) {
	const chunks: (ArtifactReference | null)[] = [];
	const chunkBytes = speculativeConfig().filesystem.chunkBytes;
	const buffer = Buffer.allocUnsafe(chunkBytes);
	const digest = createHash("sha256");
	let offset = 0;
	let size = 0;
	const flush = async () => {
		const bytes = buffer.subarray(0, offset);
		chunks.push(bytes.every((value) => value === 0) ? null : await put(bytes));
		offset = 0;
	};
	const capture = await read(async (chunk) => {
		signal?.throwIfAborted();
		if (size + chunk.length > maxBytes) throw new Error("content logical budget exceeded");
		size += chunk.length;
		digest.update(chunk);
		for (let cursor = 0; cursor < chunk.length;) {
			const count = Math.min(chunk.length - cursor, buffer.length - offset);
			chunk.copy(buffer, offset, cursor, cursor + count);
			cursor += count;
			offset += count;
			if (offset === buffer.length) await flush();
		}
	});
	if (offset) await flush();
	signal?.throwIfAborted();
	if (size !== capture.bytesRead) throw new Error("content capture size mismatch");
	const hash = digest.digest("hex");
	const manifest = parseContentManifest({
		size,
		chunkBytes,
		digest: `sha256:${hash}`,
		chunks,
	});
	verifiedManifests.add(manifest);
	return { capture: { ...capture, hash }, manifest };
}
/** Apply changed logical ranges to the existing object, keeping its aliases and open descriptions attached. */
export async function writeContent(
	handle: FileHandle,
	after: ContentVersion,
	before?: ContentVersion,
): Promise<void> {
	const previous = before instanceof ChunkedContent ? before.manifest : undefined;
	const next = after instanceof ChunkedContent ? after.manifest : undefined;
	const chunkBytes = next ? contentBlockBytes(next) : speculativeConfig().filesystem.chunkBytes;
	let written = false;
	if (!before || before.byteLength < after.byteLength) {
		await handle.truncate(after.byteLength);
		written = true;
	}
	for (let offset = 0; offset < after.byteLength; offset += chunkBytes) {
		const index = offset / chunkBytes;
		const length = Math.min(chunkBytes, after.byteLength - offset);
		if (
			next &&
			((previous &&
				contentBlockBytes(previous) === chunkBytes &&
				previous.size >= offset + length &&
				next.chunks[index]?.digest === previous.chunks[index]?.digest) ||
				(!before && next.chunks[index] === null) ||
				(before && offset >= before.byteLength && next.chunks[index] === null))
		)
			continue;
		const block = await readContent(after, offset, length);
		for (let written = 0; written < block.length;) {
			const { bytesWritten } = await handle.write(block, written, block.length - written, offset + written);
			if (!bytesWritten) throw new Error("content write made no progress");
			written += bytesWritten;
		}
		written = true;
	}
	if (!written || (before && before.byteLength > after.byteLength)) await handle.truncate(after.byteLength);
}
/** The ordinary record stays cloneable; content manifests borrow the same object leases as effect closures. */
export interface ArtifactReference {
	readonly digest: Sha256Digest;
	readonly size: number;
}
/** The same immutable content identity for input versions and effect closures. */
export function validateArtifact(reference: ArtifactReference, sizes?: Map<Sha256Digest, number>): void {
	if (!reference || !isSha256Digest(reference.digest) || !isSafeCount(reference.size))
		throw new Error("invalid content artifact");
	const previousSize = sizes?.get(reference.digest);
	if (previousSize !== undefined && previousSize !== reference.size)
		throw new Error("conflicting content artifact sizes");
	sizes?.set(reference.digest, reference.size);
}
export interface ChunkedContentManifest {
	readonly chunkBytes?: number;
	readonly size: number;
	readonly digest: Sha256Digest;
	readonly chunks: readonly (ArtifactReference | null)[];
}
/** Zero blocks describe bytes, not physical holes. No runtime descriptor is serializable. */
export function parseContentManifest(value: unknown): ChunkedContentManifest {
	const manifest = value as ChunkedContentManifest;
	const chunkBytes = manifest && contentBlockBytes(manifest);
	if (
		!manifest ||
		!isSafeCount(chunkBytes, 1) ||
		!isSafeCount(manifest.size) ||
		!isSha256Digest(manifest.digest) ||
		!Array.isArray(manifest.chunks) ||
		manifest.chunks.length > speculativeConfig().filesystem.maxManifestChunks ||
		manifest.chunks.length !== Math.ceil(manifest.size / chunkBytes)
	)
		throw new Error("invalid content manifest");
	const sizes = new Map<Sha256Digest, number>();
	return Object.freeze({
		size: manifest.size,
		...(manifest.chunkBytes ? { chunkBytes } : {}),
		digest: manifest.digest,
		chunks: Object.freeze(
			manifest.chunks.map((reference, index) => {
				if (reference === null) return null;
				validateArtifact(reference, sizes);
				if (reference.size !== Math.min(chunkBytes, manifest.size - index * chunkBytes))
					throw new Error("invalid content chunk size");
				return Object.freeze({
					digest: reference.digest,
					size: reference.size,
				});
			}),
		),
	});
}
