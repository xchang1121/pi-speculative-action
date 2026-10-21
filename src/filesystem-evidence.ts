import { digestObject, type Sha256Digest } from "./stable-value-hash.ts";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, constants, fstatSync, openSync, type BigIntStats, type Stats } from "node:fs";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, slash } from "./path-utils.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { TimelineInterval, type SerializedTimelineComputation } from "./task-timing.ts";

export interface FilesystemTypeEvidence extends Readonly<Pick<Stats,
	"isFile" | "isDirectory" | "isSymbolicLink" | "isSocket" | "isFIFO" | "isCharacterDevice" | "isBlockDevice">> {}

export interface FilesystemMetadataEvidence extends Readonly<Pick<Stats | BigIntStats,
	"mode" | "uid" | "gid" | "size" | "nlink">> {}

/** Native metadata images use this field order. */
export const FILESYSTEM_METADATA_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "blksize", "blocks", "atimeNs", "mtimeNs", "ctimeNs", "birthtimeNs"] as const;
/** Reading changes atime under relatime; stat's observed fields also exclude birth time. */
export const FILESYSTEM_OBSERVATION_FIELDS = Object.freeze(FILESYSTEM_METADATA_FIELDS.filter(field => field !== "atimeNs" && field !== "birthtimeNs"));
export type FilesystemObservationField = typeof FILESYSTEM_OBSERVATION_FIELDS[number];
export type FilesystemObservationEvidence = { readonly [Field in FilesystemObservationField]: bigint };

export function filesystemEntryType(entry: FilesystemTypeEvidence): string {
	return entry.isFile()
		? "file"
		: entry.isDirectory()
			? "directory"
			: entry.isSymbolicLink()
				? "symlink"
				: entry.isSocket() ? "socket" : entry.isFIFO() ? "fifo" : entry.isCharacterDevice() ? "char" : entry.isBlockDevice() ? "block" : "other";
}

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
export type FilesystemObjectHandle = { readonly fd: number; stat(options: { bigint: true }): Promise<BigIntStats>; close(): Promise<void> };
export type CapturedFilesystemTransfer = Omit<StableFilesystemCapture, "stat" | "object" | "content" | "computation"> & {
	readonly fd: number; readonly active: SharedArrayBuffer; readonly stat: FilesystemIdentity; readonly content?: Uint8Array; readonly computation?: SerializedTimelineComputation;
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

/** A content version and its kernel object share one revocable borrowing lifetime. */
export class CapturedFilesystemObject {
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly handle: FilesystemObjectHandle;
	private readonly capture: StableFilesystemCapture;
	constructor(handle: FilesystemObjectHandle, capture: StableFilesystemCapture) { this.handle = handle; this.capture = capture; }
	borrow<T>(consume: (capture: StableFilesystemCapture, handle: FilesystemObjectHandle) => Promise<T>): Promise<T> {
		return this.lifetime.admit(() => consume(this.capture, this.handle));
	}
	/** Threads share the descriptor table; the sender keeps this pin until the receiver acknowledges its own copy. */
	lend(computation = this.capture.computation): { readonly transfer: CapturedFilesystemTransfer; readonly release: () => void } | undefined {
		if (this.lifetime.sealed) return;
		const active = new Int32Array(new SharedArrayBuffer(4)); Atomics.store(active, 0, 1);
		const { object: _object, stat, content, computation: _computation, ...capture } = this.capture;
		const transfer = { ...capture, fd: this.handle.fd, active: active.buffer, stat: Object.fromEntries(IDENTITY_FIELDS.map(key => [key, stat[key]])) as FilesystemIdentity,
			content: content && Uint8Array.from(content), computation: computation && TimelineInterval.serialize(computation) };
		const { promise, resolve: release } = Promise.withResolvers<void>(); this.lifetime.track(promise);
		return { transfer, release: () => { Atomics.store(active, 0, 0); release(); } };
	}
	/** Synchronous import cannot dispatch caller callbacks; a fresh O_PATH descriptor preserves independent ownership. */
	static receive(transfer: CapturedFilesystemTransfer): StableFilesystemCapture | undefined {
		let fd: number | undefined;
		try {
			const active = new Int32Array(transfer.active); if (!Atomics.load(active, 0)) return;
			fd = openSync(`/proc/self/fd/${transfer.fd}`, 0x200000);
			const stat = fstatSync(fd, { bigint: true });
			if (!Atomics.load(active, 0) || !sameFilesystemIdentity(transfer.stat, stat)) return;
			const { fd: _fd, active: _active, computation, content, ...capture } = transfer, owned = fd;
			const result = { ...capture, stat, content: content && Buffer.from(content), computation: computation && TimelineInterval.restore(computation) };
			const object = new CapturedFilesystemObject({ fd: owned, stat: async () => fstatSync(owned, { bigint: true }), close: async () => closeSync(owned) }, result);
			fd = undefined; return { ...result, object };
		} catch { return undefined; } finally { if (fd !== undefined) closeSync(fd); }
	}
	dispose(): Promise<void> { return this.lifetime.close(async () => { await this.lifetime.drain(); await this.handle.close(); }); }
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
export async function captureFileDigest(target: string, maxBytes = Number.POSITIVE_INFINITY, signal?: AbortSignal, verifyPath = true): Promise<StableFilesystemCapture> {
	signal?.throwIfAborted();
	const stat = await (verifyPath ? fs.lstat : fs.stat)(target, { bigint: true }), cached = Number(stat.size) <= maxBytes ? cachedCapture(stat, target) : undefined;
	signal?.throwIfAborted(); if (cached) return cached;
	return captureStableFile(target, maxBytes, false, { verifyPath }, signal);
}

/** Import the helper's complete getdents64 image without enumerating the directory again. */
export async function captureHeldDirectory(pid: number, fd: number, content: Buffer, stat: BigIntStats, realPath: string): Promise<StableFilesystemCapture> {
	const startedAt = performance.now();
	const names: Buffer[] = [];
	for (let offset = 0; offset < content.length;) {
		if (content.length - offset < 24) throw new Error("invalid_directory_image");
		const length = content.readUInt16LE(offset + 16), end = offset + length, zero = content.indexOf(0, offset + 19);
		if (length < 24 || length % 8 || end > content.length || zero < offset + 20 || zero >= end) throw new Error("invalid_directory_image");
		const name = content.subarray(offset + 19, zero), decoded = name.toString("utf8");
		if (decoded.includes("/") || !Buffer.from(decoded).equals(name)) throw new Error("unrepresentable_directory_name");
		if (decoded !== "." && decoded !== "..") names.push(name);
		offset = end;
	}
	names.sort(Buffer.compare);
	const entries = names.map(name => name.toString("utf8"));
	if (new Set(entries).size !== entries.length || !stat.isDirectory()) throw new Error("invalid_directory_image");
	const handle = await fs.open(`/proc/${pid}/fd/${fd}`, 0x200000);
	try {
		if (!sameFilesystemIdentity(stat, await handle.stat({ bigint: true })) ||
			!sameFilesystemIdentity(stat, await fs.stat(realPath, { bigint: true }))) throw new Error("directory_changed_during_capture");
		const capture = { content, entries, stat, realPath, bytesRead: content.length, hash: createHash("sha256").update(content).digest("hex"), computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
		return { ...capture, object: new CapturedFilesystemObject(handle, capture) };
	} catch (error) { await handle.close(); throw error; }
}

/** Pin and hash one file, verifying its final identity before cache publication or transfer. */
export async function captureStableFile(target: string, maxBytes = Number.POSITIVE_INFINITY, retainContent = false,
	observed?: Partial<Pick<StableFilesystemCapture, "stat" | "realPath">> & { readonly retainObject?: boolean; readonly gitBlob?: boolean; readonly digest?: false; readonly verifyPath?: boolean },
	signal?: AbortSignal): Promise<StableFilesystemCapture> {
	signal?.throwIfAborted(); const startedAt = performance.now(), takenAtMs = Date.now(), verifyPath = observed?.verifyPath !== false;
	// O_PATH pins even executable aliases without admitting I/O on a raced-in FIFO or device.
	let binding = process.platform === "linux" && (process.arch === "x64" || process.arch === "arm64")
		? await fs.open(target, 0x200000 | (verifyPath ? constants.O_NOFOLLOW : 0)) : undefined;
	let handle: FileHandle | undefined;
	try {
		const before = binding ? await binding.stat({ bigint: true }) : observed?.stat ?? await (verifyPath ? fs.lstat : fs.stat)(target, { bigint: true });
		if (!before.isFile()) throw new Error("not_regular_file");
		if (binding && ((before.dev >> 8n & 0xfffn) | (before.dev >> 32n & ~0xfffn)) && !kernelTicks.has(before.dev)) // A major number: not a reusable anonymous one.
			kernelTicks.set(before.dev, KERNEL_TICK_FILESYSTEMS.has((await fs.statfs(`/proc/self/fd/${binding.fd}`)).type));
		if (observed?.stat && !sameFilesystemIdentity(observed.stat, before)) throw new Error("file_changed_during_capture");
		const beforePath = verifyPath ? observed?.realPath ?? await fs.realpath(target) : target;
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes))) throw new Error(`file_too_large:${before.size}`);
		handle = await fs.open(binding ? `/proc/self/fd/${binding.fd}` : target,
			constants.O_RDONLY | (binding || !verifyPath ? 0 : constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
		if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true }))) throw new Error("file_changed_during_capture");
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
			if (bytesRead > Number(before.size)) throw new Error("file_changed_during_capture");
			hash?.update(chunk.subarray(0, size)); blob?.update(chunk.subarray(0, size));
		}
		const after = await handle.stat({ bigint: true });
		signal?.throwIfAborted();
		if (bytesRead !== Number(before.size) || !sameFilesystemIdentity(before, after)) throw new Error("file_changed_during_capture");
		if (verifyPath) {
			const [afterPath, pathStat] = await Promise.all([fs.realpath(target), fs.lstat(target, { bigint: true })]);
			if (beforePath !== afterPath || !sameFilesystemIdentity(after, pathStat)) throw new Error("file_changed_during_capture");
		}
		const result = { hash: hash?.digest("hex") ?? "", bytesRead, stat: after, ...(content ? { content } : {}), ...(blob ? { blob: blob.digest("hex") } : {}),
			realPath: beforePath, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
		if (hash) rememberCapture(result, takenAtMs);
		if (observed?.retainObject && retainContent && binding) {
			await handle.close(); handle = undefined;
			const object = new CapturedFilesystemObject(binding, result); binding = undefined;
			return { ...result, object };
		}
		return result;
	} catch (error) { signal?.throwIfAborted(); throw error; } finally {
		try { await handle?.close(); } finally { await binding?.close(); }
	}
}

/** Own a directory listing or link target together with its stable entry identity. */
export async function captureFilesystemEntry(target: string, read?: "directory" | "identity") {
	const startedAt = performance.now();
	const before = await fs.lstat(target, { bigint: true });
	const link = before.isSymbolicLink() ? await fs.readlink(target) : undefined;
	const entries = read === "directory" && before.isDirectory() ? await fs.readdir(target, { withFileTypes: true }) : undefined;
	const info = link !== undefined || read !== undefined ? await fs.lstat(target, { bigint: true }) : before;
	if (!sameFilesystemIdentity(before, info)) throw new Error(`${link !== undefined ? "symlink" : "directory"}_changed_during_capture`);
	return { info, link, entries, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) };
}

/** Resolve link targets component by component, retaining each stable namespace observation. */
export async function* walkFilesystemPath(target: string, options: { readonly start?: string; readonly followFinal?: boolean; readonly capture?: typeof captureFilesystemEntry } = {}) {
	const start = options.start ?? path.parse(target).root;
	let current = start, links = 0;
	const pending = target.slice(start.length).split(path.sep).filter(Boolean);
	for (;;) {
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>> | undefined;
		try { captured = await (options.capture ?? captureFilesystemEntry)(current); }
		catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
		const { info, link } = captured ?? {}, terminal = pending.length === 0 && (link === undefined || options.followFinal === false);
		if (links && (!info || terminal)) {
			// Magic links name kernel handles; their displayed pathname need not identify that object.
			const actual = await (options.followFinal === false ? fs.lstat : fs.stat)(target, { bigint: true }).catch((error: NodeJS.ErrnoException) => {
				if (error.code !== "ENOENT") throw error;
			});
			if (info ? !actual || !sameFilesystemIdentity(info, actual) : actual) throw new Error("filesystem_link_resolution_changed");
		}
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
