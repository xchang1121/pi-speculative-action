import { digestObject, type Sha256Digest } from "./stable-value-hash.ts";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, constants, fstatSync, openSync, read, readlinkSync, type BigIntStats, type Stats } from "node:fs";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, slash } from "./path-utils.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { TimelineInterval, type SerializedTimelineComputation } from "./task-timing.ts";
import { ensureNativeFilesystem, nativeFilesystem } from "./process-startup.mjs";
import { windowsFilesystem } from "./windows-filesystem.mjs";
import { getSystemErrorName } from "node:util";

/** An unaccepted observation crossed an object version and may be captured again as a whole. */
export class FilesystemCaptureChangedError extends Error {}

export interface FilesystemTypeEvidence extends Readonly<Pick<Stats,
	"isFile" | "isDirectory" | "isSymbolicLink" | "isSocket" | "isFIFO" | "isCharacterDevice" | "isBlockDevice">> {}

export interface FilesystemMetadataEvidence extends Readonly<Pick<Stats | BigIntStats,
	"mode" | "uid" | "gid" | "size" | "nlink">> {}

/** Native metadata images use this field order. */
export const FILESYSTEM_METADATA_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "blksize", "blocks", "atimeNs", "mtimeNs", "ctimeNs", "birthtimeNs"] as const;
/** Every field returned by stat is observable, including access time changed by reads. */
export const FILESYSTEM_OBSERVATION_FIELDS = Object.freeze(FILESYSTEM_METADATA_FIELDS.filter(field => field !== "birthtimeNs"));
export type FilesystemObservationField = typeof FILESYSTEM_OBSERVATION_FIELDS[number];
export type FilesystemObservationEvidence = { readonly [Field in FilesystemObservationField]: bigint };

const FILESYSTEM_TYPES = [["file", "isFile", 8], ["directory", "isDirectory", 4], ["symlink", "isSymbolicLink", 10],
	["socket", "isSocket", 12], ["fifo", "isFIFO", 1], ["char", "isCharacterDevice", 2], ["block", "isBlockDevice", 6]] as const;
export function filesystemEntryType(entry: FilesystemTypeEvidence): string { return FILESYSTEM_TYPES.find(([, check]) => entry[check]())?.[0] ?? "other"; }

export function filesystemMetadataDigest(stat: FilesystemMetadataEvidence): Sha256Digest {
	const type = Number(stat.mode) & 0o170000;
	return digestObject({
		mode: Number(stat.mode),
		uid: Number(stat.uid),
		gid: Number(stat.gid),
		...(type === 0o100000 ? { size: Number(stat.size), links: Number(stat.nlink) } : {}),
		type: type === 0o100000 ? "file" : type === 0o040000 ? "directory" : type === 0o120000 ? "symlink" : "other",
	});
}

export function filesystemObservationDigest(stat: Partial<FilesystemObservationEvidence>, fields: readonly FilesystemObservationField[] = FILESYSTEM_OBSERVATION_FIELDS): Sha256Digest {
	return digestObject(Object.fromEntries(fields.map((field) => [field, String(stat[field])])));
}

const IDENTITY_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "mtimeNs", "ctimeNs"] as const;
type FilesystemIdentity = Pick<BigIntStats, typeof IDENTITY_FIELDS[number]>;
export type FilesystemObjectHandle = { readonly fd: number; stat(options: { bigint: true }): Promise<BigIntStats>; close(): Promise<void>;
	read?(buffer: Buffer, offset: number, length: number, position: number): Promise<{ readonly bytesRead: number }> };

/** Open an unnamed regular inode in private backing storage without exposing a control path. */
export async function openAnonymousFilesystemFile(root: string): Promise<FileHandle> {
	if (process.platform !== "linux") throw new Error("anonymous filesystem files require Linux");
	return fs.open(root, constants.O_WRONLY | 0o20200000, 0o600); // __O_TMPFILE | O_DIRECTORY
}

/** Retain identity without retaining a pathname or copying bytes. The caller owns the returned lease. */
export async function pinFilesystemObject(target: string): Promise<{ readonly stat: BigIntStats; close(): Promise<void> }> {
	target = path.resolve(target);
	if (process.platform === "win32") {
		if (!/^[a-z]:[\\/]/i.test(target)) throw new Error("object leases require a local NTFS path");
		const provider = windowsFilesystem(), lease = provider.pin(path.toNamespacedPath(target));
		try {
			const stat = await fs.lstat(target, { bigint: true }), identity = provider.identity(lease);
			const inode = BigInt(identity.readUInt32LE(4)) << 32n | BigInt(identity.readUInt32LE(8));
			if (stat.dev !== BigInt(identity.readUInt32LE(0)) || stat.ino !== inode || stat.isSymbolicLink()) throw new Error("filesystem_object_binding_changed");
			return { stat, close: async () => provider.close(lease) };
		} catch (error) { provider.close(lease); throw error; }
	}
	const handle = await fs.open(target, process.platform === "linux" ? 0x200000 | constants.O_NOFOLLOW : constants.O_RDONLY);
	try { return { stat: await handle.stat({ bigint: true }), close: () => handle.close() }; }
	catch (error) { await handle.close(); throw error; }
}
/** One capture owns one detached read-only root. Cloning changes mount identity, never inode identity. */
export class FilesystemReadView {
	private descriptor: number | undefined;
	private readonly mounts: string;
	private readonly observed = new Set<string>(["/"]);
	private closed = false;
	private readonly reader: typeof nativeFilesystem;
	private constructor(descriptor: number | undefined, mounts: string, reader = nativeFilesystem) { this.descriptor = descriptor; this.mounts = mounts; this.reader = reader; }
	static ambient(): FilesystemReadView { return new FilesystemReadView(undefined, ""); }
	static async open(): Promise<FilesystemReadView> {
		const reader = await ensureNativeFilesystem();
		if (!reader) return FilesystemReadView.ambient();
		const mounts = await fs.readFile("/proc/self/mountinfo", "utf8");
		const descriptor = reader(-5, Buffer.alloc(0), fd => adoptFilesystemDescriptor(fd, 0x200000));
		if (!Number.isInteger(descriptor)) throw new Error("native read-only filesystem view unavailable");
		if (descriptor < 0) throw filesystemReadError("read-only filesystem view unavailable", descriptor);
		const view = new FilesystemReadView(descriptor, mounts, reader);
		try { await view.verify(); return view; } catch (error) { await view.close(); throw error; }
	}
	/** Caller owns the returned raw descriptor. Only use while this view is open. */
	open(target: string, flags: number): number {
		if (this.closed || this.descriptor === undefined || !this.reader) throw new Error("read-only filesystem view is closed or unavailable");
		const absolute = path.isAbsolute(target) ? target : `${process.cwd()}/${target}`;
		if (absolute.includes("\0")) throw new Error("invalid filesystem pathname");
		this.observed.add(absolute);
		const descriptor = openFilesystemDescriptor(this.reader, this.descriptor, absolute, flags);
		try {
			this.observed.add(readlinkSync(`/proc/self/fd/${descriptor}`));
			return descriptor;
		} catch (error) { closeSync(descriptor); throw error; }
	}
	async pin(target: string, follow = false): Promise<FilesystemObjectHandle> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return fs.open(target, 0x200000 | (follow ? 0 : constants.O_NOFOLLOW));
		const descriptor = this.open(target, 0x200000 | (follow ? 0 : constants.O_NOFOLLOW));
		return filesystemDescriptorHandle(descriptor);
	}
	async stat(target: string, follow = false): Promise<BigIntStats> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return (follow ? fs.stat : fs.lstat)(target, { bigint: true });
		const handle = await this.pin(target, follow);
		try { return await handle.stat({ bigint: true }); } finally { await handle.close(); }
	}
	async realpath(target: string): Promise<string> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return fs.realpath(target);
		const handle = await this.pin(target, true);
		try { return readlinkSync(`/proc/self/fd/${handle.fd}`); } finally { await handle.close(); }
	}
	async readlink(target: string): Promise<string> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return fs.readlink(target);
		const handle = await this.pin(target);
		try {
			for (let capacity = 4096; capacity <= 512 * 1024; capacity *= 2) {
				const buffer = Buffer.allocUnsafe(capacity + 4); buffer.writeInt32LE(handle.fd);
				const count = this.reader!(-7, buffer);
				if (!Number.isInteger(count) || count > capacity) throw new Error("native read-only filesystem readlink unavailable");
				if (count < 0) throw filesystemReadError("read-only filesystem readlink failed", count);
				if (count < capacity) {
					const bytes = buffer.subarray(4, 4 + count), link = bytes.toString("utf8");
					if (!Buffer.from(link).equals(bytes)) throw new Error("unrepresentable_symlink_target");
					return link;
				}
			}
			throw new Error("filesystem symlink target is too large");
		} finally { await handle.close(); }
	}
	async readDirectory(target: string): Promise<FileHandle> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return fs.open(target, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOATIME).catch(error => {
			if (error.code !== "EPERM") throw error;
			return fs.open(target, constants.O_RDONLY | constants.O_DIRECTORY);
		});
		const descriptor = this.open(target, constants.O_RDONLY | constants.O_DIRECTORY);
		try { return await fs.open(`/proc/self/fd/${descriptor}`, constants.O_RDONLY | constants.O_DIRECTORY); } finally { closeSync(descriptor); }
	}
	/** Observe lookup ancestors by default; explicit roots also include mounts below them. */
	async verify(targets?: Iterable<string>): Promise<void> {
		if (this.closed) throw new Error("read-only filesystem view is closed");
		if (this.descriptor === undefined) return;
		const paths = [...(targets ?? this.observed)].map(target => path.resolve(target));
		const relevant = (image: string) => image.split("\n").filter(line => {
			const mount = (line.split(" ")[4] ?? "").replace(/\\([0-7]{3})/g, (_match, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));
			return mount && paths.some(target => containsFilesystemPath(mount, target) || targets !== undefined && containsFilesystemPath(target, mount));
		}).sort().join("\n");
		if (relevant(this.mounts) !== relevant(await fs.readFile("/proc/self/mountinfo", "utf8"))) throw new Error("filesystem mount topology changed during capture");
	}
	async close(): Promise<void> { if (!this.closed) { this.closed = true; if (this.descriptor !== undefined) closeSync(this.descriptor); this.descriptor = undefined; } }
	[Symbol.asyncDispose](): Promise<void> { return this.close(); }
}

function filesystemDescriptorHandle(fd: number): FilesystemObjectHandle {
	let closed = false;
	return { fd, stat: async () => {
		if (closed) throw filesystemReadError("filesystem object lease is closed", -9);
		return fstatSync(fd, { bigint: true });
	}, close: async () => { if (!closed) { closed = true; closeSync(fd); } } };
}

function openFilesystemDescriptor(reader: NonNullable<typeof nativeFilesystem>, root: number, target: string, flags: number): number {
	const request = Buffer.alloc(9 + Buffer.byteLength(target));
	request.writeInt32LE(root); request.writeUInt32LE(flags, 4); request.write(target, 8);
	const descriptor = reader(-6, request, fd => adoptFilesystemDescriptor(fd, flags));
	if (!Number.isInteger(descriptor)) throw new Error("native filesystem open unavailable");
	if (descriptor < 0) throw filesystemReadError(`filesystem open failed: ${target} (flags=${flags})`, descriptor);
	return descriptor;
}

/** Node owns the adopted descriptor, including worker teardown. The native callback closes its
 * temporary descriptor before returning, including when this callback throws. Reopening a fresh
 * OFD here loses no read offset, and following the proc magic link keeps an O_PATH link. */
function adoptFilesystemDescriptor(nativeDescriptor: number, flags: number): number {
	let descriptor: number | undefined;
	try {
		descriptor = openSync(`/proc/self/fd/${nativeDescriptor}`, flags & ~constants.O_NOFOLLOW);
		if (!sameFilesystemIdentity(fstatSync(nativeDescriptor, { bigint: true }), fstatSync(descriptor, { bigint: true }))) throw new Error("filesystem descriptor adoption changed identity");
	} catch (error) { if (descriptor !== undefined) { closeSync(descriptor); descriptor = undefined; } throw error; }
	return descriptor;
}

function filesystemReadError(message: string, error: number): NodeJS.ErrnoException {
	const code = getSystemErrorName(error);
	return Object.assign(new Error(`${message}: ${code}`), { code });
}

export const openFilesystemReadView = () => FilesystemReadView.open();

/** Reuse a caller's view, or create and verify exactly one view for this capture. */
export async function withFilesystemReadView<T>(view: FilesystemReadView | undefined, capture: (view: FilesystemReadView) => Promise<T>, discard?: (result: T) => Promise<unknown>): Promise<T> {
	if (view) return capture(view);
	await using owned = await openFilesystemReadView();
	const result = await capture(owned);
	try { await owned.verify(); return result; } catch (error) { try { await discard?.(result); } finally { throw error; } }
}

export type CapturedFilesystemTransfer = Omit<StableFilesystemCapture, "stat" | "object" | "content" | "computation"> & FilesystemObjectTransfer & {
	readonly stat: FilesystemIdentity; readonly content?: Uint8Array; readonly computation?: SerializedTimelineComputation;
};
export const FILESYSTEM_CONCURRENCY = 12;

/** Bound independent filesystem work and drain every admitted operation before propagating failure. */
export async function mapFilesystem<Input, Output>(values: ReadonlyArray<Input>, run: (value: Input) => Promise<Output>, signal?: AbortSignal) {
	const output: Output[] = [];
	let cursor = 0;
	const pending = Array.from({ length: Math.min(FILESYSTEM_CONCURRENCY, values.length) }, async () => {
		while (cursor < values.length) { signal?.throwIfAborted(); const index = cursor++; output[index] = await run(values[index]); }
	});
	try { await Promise.all(pending); signal?.throwIfAborted(); }
	catch (error) { cursor = values.length; await Promise.allSettled(pending); throw error; }
	return output;
}

export type StableFilesystemCapture = {
	readonly computation?: TimelineInterval;
	readonly hash: string;
	readonly bytesRead: number;
	readonly realPath: string;
	readonly stat: BigIntStats;
	readonly content?: Buffer;
	/** Directory names use the same byte order as libuv scandir; content retains the kernel image. */
	readonly entries?: readonly string[];
	/** Content bytes came from another capture's retained buffer; bytesRead still describes logical size. */
	readonly shared?: true;
	/** Optional real open file description, owned by the input version rather than its pathname. */
	readonly object?: CapturedFilesystemObject;
	/** Git's object id for exactly these bytes, when the caller stages them. */
	readonly blob?: string;
};

export interface FilesystemObjectTransfer {
	readonly fd: number; readonly active: SharedArrayBuffer; readonly process: number;
	readonly identity: Readonly<Pick<BigIntStats, "dev" | "ino">>; readonly readable: boolean;
}

/** Kernel-object ownership is independent of content and observation proofs. Loans only cross this process's thread boundary. */
export class FilesystemObjectLease {
	private readonly lifetime = new RuntimeLifecycleLane();
	readonly handle: FilesystemObjectHandle;
	private readonly identity: Readonly<Pick<BigIntStats, "dev" | "ino">>;
	private readonly readable: boolean;
	constructor(handle: FilesystemObjectHandle, identity: Readonly<Pick<BigIntStats, "dev" | "ino">>, readable = false) {
		this.handle = handle; this.identity = identity; this.readable = readable;
	}
	borrow<T>(consume: (handle: FilesystemObjectHandle) => Promise<T>): Promise<T> { return this.lifetime.admit(() => consume(this.handle)); }
	lend(): { readonly transfer: FilesystemObjectTransfer; readonly release: () => void } | undefined {
		if (this.lifetime.sealed || process.platform !== "linux") return;
		const active = new Int32Array(new SharedArrayBuffer(4)); Atomics.store(active, 0, 1);
		const transfer = { fd: this.handle.fd, active: active.buffer, process: process.pid, readable: this.readable,
			identity: { dev: this.identity.dev, ino: this.identity.ino } };
		const { promise, resolve } = Promise.withResolvers<void>(); this.lifetime.track(promise);
		return { transfer, release: () => { Atomics.store(active, 0, 0); resolve(); } };
	}
	/** Reopen independently, then check both object identity and the still-active borrowing authority. */
	static receive(transfer: FilesystemObjectTransfer, readable = false): { readonly handle: FilesystemObjectHandle; readonly stat: BigIntStats } | undefined {
		let fd: number | undefined;
		try {
			if (process.platform !== "linux" || transfer.process !== process.pid || readable && !transfer.readable) return;
			const active = new Int32Array(transfer.active); if (!Atomics.load(active, 0)) return;
			fd = openSync(`/proc/self/fd/${transfer.fd}`, readable ? constants.O_RDONLY : 0x200000);
			const stat = fstatSync(fd, { bigint: true });
			if (!Atomics.load(active, 0) || transfer.identity.dev !== stat.dev || transfer.identity.ino !== stat.ino) return;
			const owned = fd; fd = undefined;
			return { stat, handle: { fd: owned, stat: async () => fstatSync(owned, { bigint: true }), close: async () => closeSync(owned),
				read: (buffer, offset, length, position) => new Promise((resolve, reject) => read(owned, buffer, offset, length, position,
					(error, bytesRead) => error ? reject(error) : resolve({ bytesRead }))) } };
		} catch { return undefined; } finally { if (fd !== undefined) closeSync(fd); }
	}
	dispose(): Promise<void> { return this.lifetime.close(async () => { await this.lifetime.drain(); await this.handle.close(); }); }
}

/** A source observation owns a kernel-object lease; the observation alone never grants borrowing authority. */
export class CapturedFilesystemObject {
	private readonly object: FilesystemObjectLease;
	private readonly capture: StableFilesystemCapture;
	constructor(handle: FilesystemObjectHandle, capture: StableFilesystemCapture) { this.object = new FilesystemObjectLease(handle, capture.stat); this.capture = capture; }
	borrow<T>(consume: (capture: StableFilesystemCapture, handle: FilesystemObjectHandle) => Promise<T>): Promise<T> { return this.object.borrow(handle => consume(this.capture, handle)); }
	lend(computation = this.capture.computation): { readonly transfer: CapturedFilesystemTransfer; readonly release: () => void } | undefined {
		const loan = this.object.lend(); if (!loan) return;
		const { object: _object, stat, content, computation: _computation, ...capture } = this.capture;
		return { release: loan.release, transfer: { ...capture, ...loan.transfer, stat: Object.fromEntries(IDENTITY_FIELDS.map(key => [key, stat[key]])) as FilesystemIdentity,
			content: content && Uint8Array.from(content), computation: computation && TimelineInterval.serialize(computation) } };
	}
	static receive(transfer: CapturedFilesystemTransfer): StableFilesystemCapture | undefined {
		const object = FilesystemObjectLease.receive(transfer); if (!object) return;
		try {
			if (!sameFilesystemIdentity(transfer.stat, object.stat)) throw new Error("source object changed");
			const { fd: _fd, active: _active, process: _process, readable: _readable, identity: _identity, computation, content, ...capture } = transfer;
			const result = { ...capture, stat: object.stat, content: content && Buffer.from(content), computation: computation && TimelineInterval.restore(computation) };
			return { ...result, object: new CapturedFilesystemObject(object.handle, result) };
		} catch { void object.handle.close(); return; }
	}
	dispose(): Promise<void> { return this.object.dispose(); }
}

export function sameFilesystemIdentity(left: FilesystemIdentity, right: FilesystemIdentity): boolean {
	return IDENTITY_FIELDS.every((field) => left[field] === right[field]);
}

/** Fence workspace timestamps with a private descriptor; elapsed budgets must not use wall time. */
export async function advanceFilesystemClock(clock: FileHandle, boundary: number, identity: Pick<Stats, "dev" | "ino" | "nlink">): Promise<void> {
	let deadline: number | undefined;
	const stamp = async () => {
		const current = await clock.stat();
		if (!current.isFile() || current.dev !== identity.dev || current.ino !== identity.ino || current.nlink !== identity.nlink) {
			throw new Error("workspace transaction clock identity changed");
		}
		return current.ctimeMs;
	};
	for (let sequence = 0; ; sequence++) {
		await stamp();
		await clock.truncate(0);
		await clock.write(`${sequence}\n`, 0, "utf8");
		const current = await stamp();
		if (current > boundary) return;
		// A host that stepped its clock back (WSL resynchronizing) leaves the boundary ahead: wait out that gap, within 2 s.
		deadline ??= performance.now() + Math.min(2000, Math.max(100, boundary - current + 100));
		if (performance.now() >= deadline) throw new Error(`filesystem change clock did not advance: boundary=${boundary}, clock=${current}`);
		await new Promise<void>((resolve) => setTimeout(resolve, 1));
	}
}

/** File digests by kernel identity and change times (userspace cannot set a ctime). An entry is trusted once the file's last change
 * precedes its digest by more than one timestamp tick, so a same-size rewrite inside that tick cannot reuse it (the racy-git rule). */
const fileDigests = new Map<string, { readonly hash: string; readonly blob?: string; readonly takenAtMs: number }>();
export const fileIdentity = (stat: BigIntStats) => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;

/** A capture standing for re-reading this file, by its identity (`blob` also needs its git object id). */
export function cachedCapture(stat: BigIntStats, realPath: string, blob = false): StableFilesystemCapture | undefined {
	const cached = stat.isFile() ? fileDigests.get(fileIdentity(stat)) : undefined;
	if (!cached || blob && !cached.blob || !settledIdentity(stat, cached.takenAtMs)) return undefined;
	return { hash: cached.hash, bytesRead: Number(stat.size), realPath, stat, shared: true, ...(cached.blob ? { blob: cached.blob } : {}) };
}

/** A file's identity, once its bytes were taken more than one timestamp tick after its last change (a same-size rewrite within it keeps it):
 * the kernel's (at most 10 ms) on a block device's ext4, XFS, Btrfs or F2FS volume; 2 s elsewhere (network, FUSE, FAT, reusable device numbers). */
export function settledIdentity(stat: BigIntStats, takenAtMs: number): string | undefined {
	return stat.isFile() && takenAtMs - Number((stat.mtimeNs > stat.ctimeNs ? stat.mtimeNs : stat.ctimeNs) / 1_000_000n) >= (kernelTicks.get(stat.dev) ? 100 : 2000) ? fileIdentity(stat) : undefined;
}
const KERNEL_TICK_FILESYSTEMS = new Set([0xef53, 0x58465342, 0x9123683e, 0xf2f52010]), kernelTicks = new Map<bigint, boolean>();

/** `takenAtMs`: when the read began. An identity string stands for bytes already hashed (see `settledIdentity`). */
export function rememberCapture(capture: StableFilesystemCapture | { readonly identity: string; readonly hash: string; readonly blob?: undefined }, takenAtMs: number): void {
	fileDigests.set("identity" in capture ? capture.identity : fileIdentity(capture.stat), { hash: capture.hash, ...(capture.blob ? { blob: capture.blob } : {}), takenAtMs });
	if (fileDigests.size > 65536) fileDigests.delete(fileDigests.keys().next().value!);
}

/** Follow executable aliases (including /proc/PID/exe), then hash the complete pinned image. */
export async function hashExecutableFile(target: string, signal?: AbortSignal): Promise<`sha256:${string}`> {
	return `sha256:${(await captureFileDigest(target, Infinity, signal, false)).hash}`;
}

/** A dependency's content digest: an unchanged file validates by its identity (a runtime's image and libraries on every reuse). */
export async function captureFileDigest(target: string, maxBytes = Number.POSITIVE_INFINITY, signal?: AbortSignal, verifyPath = true, view?: FilesystemReadView): Promise<StableFilesystemCapture> {
	view ??= FilesystemReadView.ambient();
	signal?.throwIfAborted();
	const stat = await view.stat(target, !verifyPath), cached = Number(stat.size) <= maxBytes ? cachedCapture(stat, target) : undefined;
	signal?.throwIfAborted(); if (cached) return cached;
	return captureStableFile(target, maxBytes, false, { verifyPath, view }, signal);
}

function directoryImage(content: Buffer, cursor?: { cookie: bigint }) {
	const entries: (FilesystemTypeEvidence & { readonly name: string })[] = [];
	for (let offset = 0; offset < content.length;) {
		if (content.length - offset < 24) throw new Error("invalid_directory_image");
		const length = content.readUInt16LE(offset + 16), end = offset + length, zero = content.indexOf(0, offset + 19);
		if (length < 24 || length % 8 || end > content.length || zero < offset + 20 || zero >= end) throw new Error("invalid_directory_image");
		const name = content.subarray(offset + 19, zero), decoded = name.toString("utf8");
		if (decoded.includes("/") || !Buffer.from(decoded).equals(name)) throw new Error("unrepresentable_directory_name");
		if (cursor) {
			const cookie = content.readBigInt64LE(offset + 8);
			if (cookie <= cursor.cookie) throw new Error("invalid_directory_image");
			cursor.cookie = cookie; content.fill(0, zero + 1, end);
		}
		const type = content[offset + 18];
		if (decoded !== "." && decoded !== "..") entries.push({ name: decoded,
			...Object.fromEntries(FILESYSTEM_TYPES.map(([, check, code]) => [check, () => code === type])) } as typeof entries[number]);
		offset = end;
	}
	return entries.sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)));
}

/** Observe a directory through an independent OFD without advancing owned input access times. */
export async function readFilesystemDirectory(target: string, view?: FilesystemReadView): Promise<(FilesystemTypeEvidence & { readonly name: string })[]> {
	if (!nativeFilesystem) return fs.readdir(target, { withFileTypes: true });
	view ??= FilesystemReadView.ambient();
	const handle = await view.readDirectory(target);
	try {
		const entries: ReturnType<typeof directoryImage> = [];
		for await (const content of readDirectoryChunks(handle)) entries.push(...await mapFilesystem(directoryImage(content), async entry =>
			filesystemEntryType(entry) === "other" ? Object.assign(await view.stat(path.join(target, entry.name)), { name: entry.name }) : entry));
		return entries.sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)));
	} finally { await handle.close(); }
}

/** The yielded buffer is borrowed until the next read; callers retain only admitted bytes. */
async function* readDirectoryChunks(handle: FileHandle, maxBytes = Infinity, signal?: AbortSignal) {
	const buffer = Buffer.allocUnsafe(32 * 1024); let used = 0;
	for (;;) {
		signal?.throwIfAborted();
		const count = nativeFilesystem!(handle.fd, buffer);
		if (!Number.isInteger(count) || count > buffer.length) throw new Error("invalid_directory_read");
		if (count < 0) throw filesystemReadError("directory read failed", count);
		if (!count) return;
		used += count; if (used > maxBytes) throw new Error("directory image exceeds capture budget");
		yield buffer.subarray(0, count);
	}
}

/** Preserve the source filesystem's raw cookies and inode numbers using an independent read-only OFD. */
export async function readFilesystemDirectoryImage(target: string, maxBytes: number, signal?: AbortSignal, view?: FilesystemReadView): Promise<Buffer> {
	signal?.throwIfAborted();
	if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) throw new Error("invalid directory capture budget");
	return withFilesystemReadView(view, async source => {
		if (!nativeFilesystem) throw new Error("native directory image unavailable");
		const handle = await source.readDirectory(target);
		try {
			const before = await handle.stat({ bigint: true }), chunks: Buffer[] = [], names = new Set<string>(), cursor = { cookie: 0n };
			for await (const content of readDirectoryChunks(handle, maxBytes, signal)) {
				for (const entry of directoryImage(content, cursor)) {
					if (names.has(entry.name)) throw new Error("invalid_directory_image");
					names.add(entry.name);
				}
				chunks.push(Buffer.from(content));
			}
			if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true })) ||
				!sameFilesystemIdentity(before, await source.stat(target, true))) throw new FilesystemCaptureChangedError("directory_changed_during_capture");
			return Buffer.concat(chunks);
		} finally { await handle.close(); }
	});
}

/** Import the helper's complete getdents64 image without enumerating the directory again. */
export async function captureHeldDirectory(pid: number, fd: number, content: Buffer, stat: BigIntStats, realPath: string, view?: FilesystemReadView): Promise<StableFilesystemCapture> {
	view ??= FilesystemReadView.ambient();
	const startedAt = performance.now(), entries = directoryImage(content).map(entry => entry.name);
	if (new Set(entries).size !== entries.length || !stat.isDirectory()) throw new Error("invalid_directory_image");
	const handle = await fs.open(`/proc/${pid}/fd/${fd}`, 0x200000);
	try {
		if (!sameFilesystemIdentity(stat, await handle.stat({ bigint: true })) ||
			!sameFilesystemIdentity(stat, await view.stat(realPath, true))) throw new FilesystemCaptureChangedError("directory_changed_during_capture");
		const capture = { content, entries, stat, realPath, bytesRead: content.length, hash: createHash("sha256").update(content).digest("hex"), computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
		return { ...capture, object: new CapturedFilesystemObject(handle, capture) };
	} catch (error) { await handle.close(); throw error; }
}

/** Open an independent reader on one regular inode without changing owned inputs' access times. */
export async function openFilesystemFile(target: string, verifyPath = true, view?: FilesystemReadView): Promise<{ handle: FileHandle; binding: FilesystemObjectHandle | undefined; stat: BigIntStats }> {
	view ??= FilesystemReadView.ambient();
	// O_PATH pins even executable aliases without admitting I/O on a raced-in FIFO or device.
	const binding = process.platform === "linux" && (process.arch === "x64" || process.arch === "arm64")
		? await view.pin(target, !verifyPath) : undefined;
	let handle: FileHandle | undefined;
	try {
		const before = binding ? await binding.stat({ bigint: true }) : await view.stat(target, !verifyPath);
		if (!before.isFile()) throw new Error("not_regular_file");
		handle = await fs.open(binding ? `/proc/self/fd/${binding.fd}` : target,
			constants.O_RDONLY | (binding || !verifyPath ? 0 : constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0)
			| (binding && before.uid === BigInt(process.getuid!()) ? constants.O_NOATIME : 0));
		if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true }))) throw new FilesystemCaptureChangedError("file_changed_during_capture");
		return { handle, binding, stat: before };
	} catch (error) { try { await handle?.close(); } finally { await binding?.close(); } throw error; }
}

/** Pin and hash one file, verifying its final identity before cache publication or transfer. */
export async function captureStableFile(target: string, maxBytes = Number.POSITIVE_INFINITY, retainContent = false,
	observed?: Partial<Pick<StableFilesystemCapture, "stat" | "realPath">> & { readonly retainObject?: boolean; readonly gitBlob?: boolean; readonly digest?: false; readonly verifyPath?: boolean; readonly view?: FilesystemReadView;
		readonly onChunk?: (bytes: Buffer) => Promise<void> },
	signal?: AbortSignal): Promise<StableFilesystemCapture> {
	const view = observed?.view ?? FilesystemReadView.ambient();
	if (retainContent && observed?.onChunk) throw new Error("content capture requires one storage strategy");
	signal?.throwIfAborted(); const startedAt = performance.now(), takenAtMs = Date.now(), verifyPath = observed?.verifyPath !== false;
	let { handle, binding, stat: before } = await openFilesystemFile(target, verifyPath, view);
	try {
		if (binding && ((before.dev >> 8n & 0xfffn) | (before.dev >> 32n & ~0xfffn)) && !kernelTicks.has(before.dev)) // A major number: not a reusable anonymous one.
			kernelTicks.set(before.dev, KERNEL_TICK_FILESYSTEMS.has((await fs.statfs(`/proc/self/fd/${binding.fd}`)).type));
		if (observed?.stat && !sameFilesystemIdentity(observed.stat, before)) throw new FilesystemCaptureChangedError("file_changed_during_capture");
		const beforePath = verifyPath ? observed?.realPath ?? await view.realpath(target) : target;
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes))) throw new Error(`file_too_large:${before.size}`);
		// Another pending read may precede a same-tick rewrite; read under this identity.
		const hash = observed?.digest !== false ? createHash("sha256") : undefined, blob = observed?.gitBlob ? createHash("sha1").update(`blob ${before.size}\0`) : undefined;
		const content = retainContent ? Buffer.allocUnsafe(Number(before.size)) : undefined;
		const buffer = Buffer.allocUnsafe(content ? 1 : Math.max(1, Math.min(Number(before.size), 1024 * 1024)));
		let bytesRead = 0;
		for (;;) {
			signal?.throwIfAborted();
			const chunk = content && bytesRead < content.length ? content.subarray(bytesRead) : buffer;
			const { bytesRead: size } = await handle.read(chunk);
			if (size === 0) break;
			bytesRead += size;
			if (bytesRead > maxBytes) throw new Error(`file_too_large:${bytesRead}`);
			if (bytesRead > Number(before.size)) throw new FilesystemCaptureChangedError("file_changed_during_capture");
			hash?.update(chunk.subarray(0, size)); blob?.update(chunk.subarray(0, size));
			if (observed?.onChunk) await observed.onChunk(chunk.subarray(0, size));
		}
		const after = await handle.stat({ bigint: true });
		signal?.throwIfAborted();
		if (bytesRead !== Number(before.size) || !sameFilesystemIdentity(before, after)) throw new FilesystemCaptureChangedError("file_changed_during_capture");
		if (verifyPath) {
			const afterPath = await view.realpath(target);
			const pathStat = await view.stat(target);
			if (beforePath !== afterPath || !sameFilesystemIdentity(after, pathStat)) throw new FilesystemCaptureChangedError("file_changed_during_capture");
		}
		const result = { hash: hash?.digest("hex") ?? "", bytesRead, stat: after, ...(content ? { content } : {}), ...(blob ? { blob: blob.digest("hex") } : {}),
			realPath: beforePath, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
		if (hash) rememberCapture(result, takenAtMs);
		if (observed?.retainObject && retainContent && binding) {
			const object = new CapturedFilesystemObject(binding, result); binding = undefined;
			return { ...result, object };
		}
		return result;
	} catch (error) { signal?.throwIfAborted(); throw error; } finally {
		try { await handle.close(); } finally { await binding?.close(); }
	}
}

/** Own a directory listing or link target together with its stable entry identity. */
export async function captureFilesystemEntry(target: string, read?: "directory" | "identity", view?: FilesystemReadView): Promise<{
	info: BigIntStats; link: string | undefined; entries: Awaited<ReturnType<typeof readFilesystemDirectory>> | undefined; computation: TimelineInterval;
}> {
	view ??= FilesystemReadView.ambient();
	const startedAt = performance.now();
	const before = await view.stat(target);
	const link = before.isSymbolicLink() ? await view.readlink(target) : undefined;
	const entries = read === "directory" && before.isDirectory() ? await readFilesystemDirectory(target, view) : undefined;
	const info = link !== undefined || read !== undefined ? await view.stat(target) : before;
	if (!sameFilesystemIdentity(before, info)) throw new FilesystemCaptureChangedError(`${link !== undefined ? "symlink" : "directory"}_changed_during_capture`);
	return { info, link, entries, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
}

/** Resolve link targets component by component, retaining each stable namespace observation. */
export async function* walkFilesystemPath(target: string, options: { readonly start?: string; readonly followFinal?: boolean; readonly capture?: typeof captureFilesystemEntry; readonly view?: FilesystemReadView } = {}) {
	await using owned = options.view ? undefined : FilesystemReadView.ambient();
	const view = options.view ?? owned!;
	const start = options.start ?? path.parse(target).root;
	let current = start, links = 0;
	const pending = target.slice(start.length).split(path.sep).filter(Boolean);
	for (;;) {
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>> | undefined;
		try { captured = await (options.capture ?? captureFilesystemEntry)(current, undefined, view); }
		catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
		const { info, link } = captured ?? {}, terminal = pending.length === 0 && (link === undefined || options.followFinal === false);
		if (links && (!info || terminal)) {
			// Magic links name kernel handles; their displayed pathname need not identify that object.
			const actual = await view.stat(target, options.followFinal !== false).catch((error: NodeJS.ErrnoException) => {
				if (error.code !== "ENOENT") throw error;
			});
			if (info ? !actual || !sameFilesystemIdentity(info, actual) : actual) throw new Error("filesystem_link_resolution_changed");
		}
		await view.verify();
		yield { path: current, info, link, terminal };
		if (!info || terminal) return;
		if (link !== undefined) {
			if (++links > 40) throw new Error(`resource_symlink_cycle:${target}`);
			const root = path.parse(link).root;
			pending.unshift(...link.slice(root.length).split(path.sep));
			current = root || path.dirname(current);
			continue;
		}
		const component = pending.shift();
		if (component === undefined) return;
		if (!info.isDirectory()) throw new Error("filesystem path component is not a directory");
		current = path.resolve(current, component);
	}
}

/** Walks taken at one moment share the entries they pass: each is captured once. */
export function sharedWalk<Capture, Input = string>(capture: (target: Input) => Capture, key: (target: Input) => string = String) {
	const walked = new Map<string, Capture>();
	return (target: Input) => {
		const name = key(target);
		return walked.get(name) ?? walked.set(name, capture(target)).get(name)!;
	};
}

export async function assertNoSymlinkPath(root: string, target: string, capture?: typeof captureFilesystemEntry): Promise<void> {
	const resolvedRoot = path.resolve(root), resolvedTarget = path.resolve(target);
	if (!containsFilesystemPath(resolvedRoot, resolvedTarget)) throw new Error(`sandbox path escapes workspace: ${resolvedTarget}`);
	for await (const entry of walkFilesystemPath(resolvedTarget, { start: resolvedRoot, ...(capture ? { capture } : {}) })) {
		const first = entry.path === resolvedRoot, info = entry.info;
		if (!info) { if (first) throw new Error(`sandbox workspace root does not exist: ${resolvedRoot}`); break; }
		if (first && (info.isSymbolicLink() || !info.isDirectory())) throw new Error("sandbox workspace root must be a real directory");
		if (info.isSymbolicLink()) throw new Error(`sandbox path contains symlink: ${slash(path.relative(resolvedRoot, entry.path))}`);
		if (!info.isFile() && !info.isDirectory()) throw new Error("sandbox path contains a special file");
	}
}

/** Publish an owned JSON stage without deleting the previous snapshot on failure. */
export async function writeJsonFile(file: string, value: unknown, space?: number, mode?: number): Promise<void> {
	if (value === undefined) return fs.rm(file, { force: true });
	await fs.mkdir(path.dirname(file), { recursive: true });
	const temporary = `${file}.${randomUUID()}.tmp`;
	const handle = await fs.open(temporary, "wx", mode);
	try {
		try { await handle.writeFile(`${JSON.stringify(value, null, space)}\n`, "utf8"); }
		finally { await handle.close(); }
		await fs.rename(temporary, file);
	} finally { await fs.rm(temporary, { force: true }).catch(() => undefined); }
}
