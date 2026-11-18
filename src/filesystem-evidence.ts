import { createHash, randomUUID } from "node:crypto";
import {
	closeSync,
	constants,
	fstatSync,
	openSync,
	read,
	readlinkSync,
	statfsSync,
	type BigIntStats,
	type Stats,
} from "node:fs";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import { getSystemErrorName } from "node:util";
import { speculativeConfig } from "./config-schema.ts";
import { containsFilesystemPath, digestObject, slash, type Sha256Digest } from "./primitives.ts";
import { ensureNativeFilesystem, nativeFilesystem } from "./process-startup.mjs";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { TimelineInterval } from "./task-timing.ts";
import { windowsFilesystem } from "./windows-filesystem.ts";
/** An unaccepted observation crossed an object version and may be captured again as a whole. */
export class FilesystemCaptureChangedError extends Error {}
export interface FilesystemTypeEvidence extends Readonly<
	Pick<
		Stats,
		| "isFile"
		| "isDirectory"
		| "isSymbolicLink"
		| "isSocket"
		| "isFIFO"
		| "isCharacterDevice"
		| "isBlockDevice"
	>
> {}
export interface FilesystemMetadataEvidence extends Readonly<
	Pick<Stats | BigIntStats, "mode" | "uid" | "gid" | "size" | "nlink">
> {}
/** Native metadata images use this field order. */
export const FILESYSTEM_METADATA_FIELDS = [
	"dev",
	"ino",
	"mode",
	"nlink",
	"uid",
	"gid",
	"rdev",
	"size",
	"blksize",
	"blocks",
	"atimeNs",
	"mtimeNs",
	"ctimeNs",
	"birthtimeNs",
] as const;
/** Every field returned by stat is observable, including access time changed by reads. */
export const FILESYSTEM_OBSERVATION_FIELDS = Object.freeze(
	FILESYSTEM_METADATA_FIELDS.filter((field) => field !== "birthtimeNs"),
);
export type FilesystemObservationField = (typeof FILESYSTEM_OBSERVATION_FIELDS)[number];
export type FilesystemObservationEvidence = {
	readonly [Field in FilesystemObservationField]: bigint;
};
const FILESYSTEM_TYPES = [
	["file", "isFile", 8],
	["directory", "isDirectory", 4],
	["symlink", "isSymbolicLink", 10],
	["socket", "isSocket", 12],
	["fifo", "isFIFO", 1],
	["char", "isCharacterDevice", 2],
	["block", "isBlockDevice", 6],
] as const;
export function filesystemEntryType(entry: FilesystemTypeEvidence): string {
	return FILESYSTEM_TYPES.find(([, check]) => entry[check]())?.[0] ?? "other";
}
export function filesystemModeType(mode: number | bigint) {
	switch (Number(mode) & 0o170000) {
		case 0o100000:
			return "file";
		case 0o040000:
			return "directory";
		case 0o120000:
			return "symlink";
		default:
			return "other";
	}
}
export function filesystemMetadataDigest(stat: FilesystemMetadataEvidence): Sha256Digest {
	const type = Number(stat.mode) & 0o170000;
	return digestObject({
		mode: Number(stat.mode),
		uid: Number(stat.uid),
		gid: Number(stat.gid),
		...(type === 0o100000 ? { size: Number(stat.size), links: Number(stat.nlink) } : {}),
		type: filesystemModeType(stat.mode),
	});
}
export function filesystemObservationDigest(
	stat: Partial<FilesystemObservationEvidence>,
	fields: readonly FilesystemObservationField[] = FILESYSTEM_OBSERVATION_FIELDS,
): Sha256Digest {
	return digestObject(Object.fromEntries(fields.map((field) => [field, String(stat[field])])));
}
const IDENTITY_FIELDS = [
	"dev",
	"ino",
	"mode",
	"nlink",
	"uid",
	"gid",
	"rdev",
	"size",
	"mtimeNs",
	"ctimeNs",
] as const;
type FilesystemIdentity = Pick<BigIntStats, (typeof IDENTITY_FIELDS)[number]>;
export type FilesystemObjectHandle = {
	readonly fd: number;
	stat(options: { bigint: true }): Promise<BigIntStats>;
	close(): Promise<void>;
	read?(
		buffer: Buffer,
		offset: number,
		length: number,
		position: number,
	): Promise<{
		readonly bytesRead: number;
	}>;
};
/** Open an unnamed regular inode in private backing storage without exposing a control path. */
export async function openAnonymousFilesystemFile(root: string): Promise<FileHandle> {
	if (process.platform !== "linux") throw new Error("anonymous filesystem files require Linux");
	return withFilesystemReadView(
		undefined,
		async (view) => {
			const { ancestor } = await resolveFilesystemDestination(root, view);
			return fs.open(ancestor, constants.O_WRONLY | 0o20200000, 0o600); // __O_TMPFILE | O_DIRECTORY
		},
		(handle) => handle.close(),
	);
}
/** Kernel-sealed content versions are independent of both mutable names and their producers. */
export async function openArtifactImage(): Promise<FileHandle> {
	const reader = await ensureNativeFilesystem();
	const fd = reader!(-13, Buffer.alloc(0), (native) => adoptFilesystemDescriptor(native, constants.O_RDWR));
	if (!Number.isInteger(fd) || fd < 0)
		throw new Error("immutable artifact images unavailable; rebuild the Linux backend");
	try {
		return await fs.open(`/proc/self/fd/${fd}`, "r+");
	} finally {
		closeSync(fd);
	}
}
export function sealArtifactImage(fd: number, seal = false): boolean {
	const request = Buffer.alloc(8);
	request.writeInt32LE(fd);
	request.writeInt32LE(Number(seal), 4);
	return nativeFilesystem?.(-14, request) === 1;
}
/** Resolve a proposed destination through its existing ancestors without creating any names. */
export async function resolveFilesystemDestination(
	target: string,
	view: FilesystemReadView,
): Promise<{
	path: string;
	ancestor: string;
}> {
	target = path.resolve(target);
	let ancestor = target;
	for (;;) {
		try {
			const resolved = await view.realpath(ancestor);
			return {
				path: path.resolve(resolved, path.relative(ancestor, target)),
				ancestor: resolved,
			};
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT" || ancestor === path.dirname(ancestor))
				throw error;
			ancestor = path.dirname(ancestor);
		}
	}
}
/** Retain identity without retaining a pathname or copying bytes. The caller owns the returned lease. */
export async function pinFilesystemObject(target: string): Promise<{
	readonly stat: BigIntStats;
	close(): Promise<void>;
}> {
	target = path.resolve(target);
	if (process.platform === "win32") {
		if (!/^[a-z]:[\\/]/i.test(target)) throw new Error("object leases require a local drive path");
		const provider = windowsFilesystem();
		const lease = await provider.pin(path.toNamespacedPath(target));
		try {
			const stat = await fs.lstat(target, { bigint: true });
			const identity = await provider.identity(lease);
			const inode = (BigInt(identity.readUInt32LE(4)) << 32n) | BigInt(identity.readUInt32LE(8));
			if (stat.dev !== BigInt(identity.readUInt32LE(0)) || stat.ino !== inode || stat.isSymbolicLink())
				throw new Error("filesystem_object_binding_changed");
			return { stat, close: async () => provider.close(lease) };
		} catch (error) {
			await provider.close(lease);
			throw error;
		}
	}
	const handle = await fs.open(
		target,
		process.platform === "linux" ? 0x200000 | constants.O_NOFOLLOW : constants.O_RDONLY,
	);
	try {
		return {
			stat: await handle.stat({ bigint: true }),
			close: () => handle.close(),
		};
	} catch (error) {
		await handle.close();
		throw error;
	}
}
/** One capture owns one detached read-only root. Cloning changes mount identity, never inode identity. */
export class FilesystemReadView {
	private descriptor: number | "ambient" | "closed";
	private readonly mounts: string;
	private readonly observed = new Set<string>(["/"]);
	private readonly reader: typeof nativeFilesystem;
	private constructor(descriptor: number | "ambient", mounts: string, reader = nativeFilesystem) {
		this.descriptor = descriptor;
		this.mounts = mounts;
		this.reader = reader;
	}
	private get ambient(): boolean {
		if (this.descriptor === "closed") throw new Error("read-only filesystem view is closed");
		return this.descriptor === "ambient";
	}
	static ambient(): FilesystemReadView {
		return new FilesystemReadView("ambient", "");
	}
	static async open(): Promise<FilesystemReadView> {
		const reader = await ensureNativeFilesystem();
		if (!reader) return FilesystemReadView.ambient();
		const mounts = await fs.readFile("/proc/self/mountinfo", "utf8");
		const descriptor = reader(-5, Buffer.alloc(0), (fd) => adoptFilesystemDescriptor(fd, 0x200000));
		if (!Number.isInteger(descriptor)) throw new Error("native read-only filesystem view unavailable");
		if (descriptor < 0) throw filesystemReadError("read-only filesystem view unavailable", descriptor);
		const view = new FilesystemReadView(descriptor, mounts, reader);
		try {
			await view.verify();
			return view;
		} catch (error) {
			await view.close();
			throw error;
		}
	}
	/** Without a synchronous observer, Node adopts and owns the returned descriptor. */
	open(
		target: string,
		flags: number,
		observe = (fd: number) => adoptFilesystemDescriptor(fd, flags),
	): number {
		if (typeof this.descriptor !== "number" || !this.reader)
			throw new Error("read-only filesystem view is closed or unavailable");
		const absolute = path.isAbsolute(target) ? target : `${process.cwd()}/${target}`;
		if (absolute.includes("\0")) throw new Error("invalid filesystem pathname");
		this.observed.add(absolute);
		const request = Buffer.alloc(9 + Buffer.byteLength(absolute));
		request.writeInt32LE(this.descriptor);
		request.writeUInt32LE(flags, 4);
		request.write(absolute, 8);
		const descriptor = this.reader(-6, request, (fd) => {
			this.observed.add(readlinkSync(`/proc/self/fd/${fd}`));
			return observe(fd);
		});
		if (!Number.isInteger(descriptor)) throw new Error("native filesystem open unavailable");
		if (descriptor < 0)
			throw filesystemReadError(`filesystem open failed: ${target} (flags=${flags})`, descriptor);
		return descriptor;
	}
	private observe<Value>(target: string, follow: boolean, read: (fd: number) => Value): Value {
		let value!: Value;
		this.open(target, 0x200000 | (follow ? 0 : constants.O_NOFOLLOW), (fd) => {
			value = read(fd);
			return 0;
		});
		return value;
	}
	async pin(target: string, follow = false): Promise<FilesystemObjectHandle> {
		if (this.ambient) return fs.open(target, 0x200000 | (follow ? 0 : constants.O_NOFOLLOW));
		const descriptor = this.open(target, 0x200000 | (follow ? 0 : constants.O_NOFOLLOW));
		return filesystemDescriptorHandle(descriptor);
	}
	async stat(target: string, follow = false): Promise<BigIntStats> {
		if (this.ambient) return (follow ? fs.stat : fs.lstat)(target, { bigint: true });
		return this.observe(target, follow, (fd) => fstatSync(fd, { bigint: true }));
	}
	async realpath(target: string): Promise<string> {
		if (this.ambient) return fs.realpath(target);
		return this.observe(target, true, (fd) => readlinkSync(`/proc/self/fd/${fd}`));
	}
	async readlink(target: string): Promise<string> {
		if (this.ambient) return fs.readlink(target);
		return this.observe(target, false, (fd) => {
			for (
				let capacity = Math.min(
					speculativeConfig().filesystem.directoryReadBytes,
					speculativeConfig().filesystem.directoryMetadataBytes,
				);
				capacity <= speculativeConfig().filesystem.directoryMetadataBytes;
				capacity *= 2
			) {
				const buffer = Buffer.allocUnsafe(capacity + 4);
				buffer.writeInt32LE(fd);
				const count = this.reader!(-7, buffer);
				if (!Number.isInteger(count) || count > capacity)
					throw new Error("native read-only filesystem readlink unavailable");
				if (count < 0) throw filesystemReadError("read-only filesystem readlink failed", count);
				if (count < capacity) {
					const bytes = buffer.subarray(4, 4 + count);
					const link = bytes.toString("utf8");
					if (!Buffer.from(link).equals(bytes)) throw new Error("unrepresentable_symlink_target");
					return link;
				}
			}
			throw new Error("filesystem symlink target is too large");
		});
	}
	async readDirectory(target: string): Promise<FileHandle> {
		if (this.ambient)
			return fs
				.open(target, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOATIME)
				.catch((error) => {
					if (error.code !== "EPERM") throw error;
					return fs.open(target, constants.O_RDONLY | constants.O_DIRECTORY);
				});
		const descriptor = this.open(target, constants.O_RDONLY | constants.O_DIRECTORY);
		try {
			return await fs.open(`/proc/self/fd/${descriptor}`, constants.O_RDONLY | constants.O_DIRECTORY);
		} finally {
			closeSync(descriptor);
		}
	}
	/** Observe lookup ancestors by default; explicit roots also include mounts below them. */
	async verify(targets?: Iterable<string>): Promise<void> {
		if (this.ambient) return;
		const current = await fs.readFile("/proc/self/mountinfo", "utf8");
		if (current === this.mounts) return;
		const paths = [...(targets ?? this.observed)].map((target) => path.resolve(target));
		const relevant = (image: string) => filesystemMountTopology(image, paths, targets !== undefined);
		if (relevant(this.mounts) !== relevant(current))
			throw new Error("filesystem mount topology changed during capture");
	}
	async close(): Promise<void> {
		const descriptor = this.descriptor;
		this.descriptor = "closed";
		if (typeof descriptor === "number") closeSync(descriptor);
	}
	[Symbol.asyncDispose](): Promise<void> {
		return this.close();
	}
}
/** Mount records affecting these lookups; optional roots also qualify their descendants. */
export function filesystemMountTopology(
	image: string,
	paths: readonly string[],
	descendants = false,
): string {
	return image
		.split("\n")
		.filter((line) => {
			const mount = (line.split(" ")[4] ?? "").replace(/\\([0-7]{3})/g, (_match, octal: string) =>
				String.fromCharCode(Number.parseInt(octal, 8)),
			);
			return (
				mount &&
				paths.some(
					(target) =>
						containsFilesystemPath(mount, target) || (descendants && containsFilesystemPath(target, mount)),
				)
			);
		})
		.sort()
		.join("\n");
}
function filesystemDescriptorHandle(fd: number, readable = false): FilesystemObjectHandle {
	let closed = false;
	const assertOpen = () => {
		if (closed) throw filesystemReadError("filesystem object lease is closed", -9);
	};
	return {
		fd,
		stat: async () => {
			assertOpen();
			return fstatSync(fd, { bigint: true });
		},
		read: readable
			? (buffer, offset, length, position) =>
					new Promise((resolve, reject) => {
						assertOpen();
						read(fd, buffer, offset, length, position, (error, bytesRead) =>
							error ? reject(error) : resolve({ bytesRead }),
						);
					})
			: undefined,
		close: async () => {
			if (!closed) {
				closed = true;
				closeSync(fd);
			}
		},
	};
}
/** Node owns the adopted descriptor, including worker teardown. The native callback closes its
 * temporary descriptor before returning, including when this callback throws. Reopening a fresh
 * OFD here loses no read offset, and following the proc magic link keeps an O_PATH link. */
function adoptFilesystemDescriptor(nativeDescriptor: number, flags: number): number {
	let descriptor: number | undefined;
	try {
		descriptor = openSync(`/proc/self/fd/${nativeDescriptor}`, flags & ~constants.O_NOFOLLOW);
		// The native descriptor pins this inode. Mutable metadata belongs to the caller's
		// capture validation; a concurrent write cannot turn this reopened descriptor into another object.
		const native = fstatSync(nativeDescriptor, { bigint: true });
		const adopted = fstatSync(descriptor, { bigint: true });
		if (
			native.dev !== adopted.dev ||
			native.ino !== adopted.ino ||
			(native.mode & 61440n) !== (adopted.mode & 61440n)
		)
			throw new Error("filesystem descriptor adoption changed identity");
	} catch (error) {
		if (descriptor !== undefined) {
			closeSync(descriptor);
			descriptor = undefined;
		}
		throw error;
	}
	return descriptor;
}
function filesystemReadError(message: string, error: number): NodeJS.ErrnoException {
	const code = getSystemErrorName(error);
	return Object.assign(new Error(`${message}: ${code}`), { code });
}
export const openFilesystemReadView = () => FilesystemReadView.open();
let sharedReadView:
	| {
			view: Promise<FilesystemReadView>;
			references: number;
	  }
	| undefined;
/** Overlapping captures share a mount tree, each retaining its own reads and topology verification. */
export async function withFilesystemReadView<T>(
	view: FilesystemReadView | undefined,
	capture: (view: FilesystemReadView) => Promise<T>,
	discard?: (result: T) => Promise<unknown>,
): Promise<T> {
	if (view) return capture(view);
	const shared = (sharedReadView ??= {
		view: openFilesystemReadView(),
		references: 0,
	});
	shared.references++;
	try {
		const owned = await shared.view;
		const result = await capture(owned);
		try {
			await owned.verify();
			return result;
		} catch (error) {
			try {
				await discard?.(result);
			} finally {
				throw error;
			}
		}
	} finally {
		if (!--shared.references) {
			sharedReadView = undefined;
			await shared.view.then(
				(view) => view.close(),
				() => {},
			);
		}
	}
}
/** Bound independent filesystem work and drain every admitted operation before propagating failure. */
export async function mapFilesystem<Input, Output>(
	values: ReadonlyArray<Input>,
	run: (value: Input) => Promise<Output>,
	signal?: AbortSignal,
) {
	const output: Output[] = [];
	let cursor = 0;
	const pending = Array.from(
		{
			length: Math.min(speculativeConfig().filesystem.concurrency, values.length),
		},
		async () => {
			while (cursor < values.length) {
				signal?.throwIfAborted();
				const index = cursor++;
				output[index] = await run(values[index]);
			}
		},
	);
	try {
		await Promise.all(pending);
		signal?.throwIfAborted();
	} catch (error) {
		cursor = values.length;
		await Promise.allSettled(pending);
		throw error;
	}
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
};
export interface FilesystemObjectTransfer {
	readonly fd: number;
	readonly active: SharedArrayBuffer;
	readonly process: number;
	readonly identity: Readonly<Pick<BigIntStats, "dev" | "ino">>;
	readonly readable: boolean;
}
/** Kernel-object ownership is independent of content and observation proofs. Loans only cross this process's thread boundary. */
export class FilesystemObjectLease {
	private readonly lifetime = new RuntimeLifecycleLane();
	readonly handle: FilesystemObjectHandle;
	constructor(
		handle: FilesystemObjectHandle,
		_identity: Readonly<Pick<BigIntStats, "dev" | "ino">>,
		_readable = false,
	) {
		this.handle = handle;
	}
	borrow<T>(consume: (handle: FilesystemObjectHandle) => Promise<T>): Promise<T> {
		return this.lifetime.admit(() => consume(this.handle));
	}
	dispose(): Promise<void> {
		return this.lifetime.dispose(() => this.handle.close());
	}
}
/** A source observation owns a kernel-object lease; the observation alone never grants borrowing authority. */
export class CapturedFilesystemObject {
	private readonly object: FilesystemObjectLease;
	private readonly capture: StableFilesystemCapture;
	constructor(handle: FilesystemObjectHandle, capture: StableFilesystemCapture) {
		this.object = new FilesystemObjectLease(handle, capture.stat);
		this.capture = capture;
	}
	borrow<T>(
		consume: (capture: StableFilesystemCapture, handle: FilesystemObjectHandle) => Promise<T>,
	): Promise<T> {
		return this.object.borrow((handle) => consume(this.capture, handle));
	}
	dispose(): Promise<void> {
		return this.object.dispose();
	}
}
export function sameFilesystemIdentity(left: FilesystemIdentity, right: FilesystemIdentity): boolean {
	return IDENTITY_FIELDS.every((field) => left[field] === right[field]);
}
/** Fence workspace timestamps with a private descriptor; elapsed budgets must not use wall time. */
export async function advanceFilesystemClock(
	clock: FileHandle,
	boundary: number,
	identity: Pick<Stats, "dev" | "ino" | "nlink">,
): Promise<void> {
	let deadline: number | undefined;
	const stamp = async () => {
		const current = await clock.stat();
		if (
			!current.isFile() ||
			current.dev !== identity.dev ||
			current.ino !== identity.ino ||
			current.nlink !== identity.nlink
		) {
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
		deadline ??= performance.now() + speculativeConfig().filesystem.clockAdvanceTimeoutMs;
		if (performance.now() >= deadline)
			throw new Error(`filesystem change clock did not advance: boundary=${boundary}, clock=${current}`);
		await new Promise<void>((resolve) => setTimeout(resolve, speculativeConfig().filesystem.clockPollMs));
	}
}
/** File digests by kernel identity and change times (userspace cannot set a ctime). An entry is trusted once the file's last change
 * precedes its digest by more than one timestamp tick, so a same-size rewrite inside that tick cannot reuse it (the racy-git rule). */
const fileDigests = new Map<
	string,
	{
		readonly hash: string;
		readonly takenAtMs: number;
	}
>();
export const fileIdentity = (stat: BigIntStats) =>
	`${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
const filesystemTypes = new Map<bigint, bigint>();
/** A capture standing for re-reading this file, by its identity. */
export function cachedCapture(stat: BigIntStats, realPath: string): StableFilesystemCapture | undefined {
	const cached = stat.isFile() ? fileDigests.get(fileIdentity(stat)) : undefined;
	if (!cached) return undefined;
	let filesystemType = filesystemTypes.get(stat.dev);
	if (
		filesystemType === undefined &&
		Object.keys(speculativeConfig().filesystem.timestampQuietByFilesystem).length
	) {
		try {
			filesystemType = statfsSync(realPath, { bigint: true }).type;
			filesystemTypes.set(stat.dev, filesystemType);
		} catch {
			return undefined;
		}
	}
	if (!settledIdentity(stat, cached.takenAtMs, filesystemType)) return undefined;
	return {
		hash: cached.hash,
		bytesRead: Number(stat.size),
		realPath,
		stat,
		shared: true,
	};
}
/** Reuse a digest after the configured settlement window for the observed filesystem type. */
export function settledIdentity(
	stat: BigIntStats,
	takenAtMs: number,
	filesystemType?: bigint,
): string | undefined {
	const settings = speculativeConfig().filesystem;
	const override =
		filesystemType === undefined
			? undefined
			: Object.entries(settings.timestampQuietByFilesystem).find(
					([type]) => BigInt(type) === filesystemType,
				)?.[1];
	return stat.isFile() &&
		takenAtMs - Number((stat.mtimeNs > stat.ctimeNs ? stat.mtimeNs : stat.ctimeNs) / 1000000n) >=
			(override ?? settings.timestampQuietMs)
		? fileIdentity(stat)
		: undefined;
}
/** `takenAtMs`: when the read began. An identity string stands for bytes already hashed (see `settledIdentity`). */
export function rememberCapture(
	capture:
		| StableFilesystemCapture
		| {
				readonly identity: string;
				readonly hash: string;
		  },
	takenAtMs: number,
): void {
	fileDigests.set("identity" in capture ? capture.identity : fileIdentity(capture.stat), {
		hash: capture.hash,
		takenAtMs,
	});
	if (fileDigests.size > speculativeConfig().filesystem.maxDigestEntries)
		fileDigests.delete(fileDigests.keys().next().value!);
}
/** Follow executable aliases (including /proc/PID/exe), then hash the complete pinned image. */
export async function hashExecutableFile(target: string, signal?: AbortSignal): Promise<`sha256:${string}`> {
	return `sha256:${(await captureFileDigest(target, Infinity, signal, false)).hash}`;
}
/** A dependency's content digest: an unchanged file validates by its identity (a runtime's image and libraries on every reuse). */
export async function captureFileDigest(
	target: string,
	maxBytes = Number.POSITIVE_INFINITY,
	signal?: AbortSignal,
	verifyPath = true,
	view?: FilesystemReadView,
): Promise<StableFilesystemCapture> {
	view ??= FilesystemReadView.ambient();
	signal?.throwIfAborted();
	const stat = await view.stat(target, !verifyPath);
	const cached = Number(stat.size) <= maxBytes ? cachedCapture(stat, target) : undefined;
	signal?.throwIfAborted();
	if (cached) return cached;
	return captureStableFile(target, maxBytes, false, { verifyPath, view }, signal);
}
function directoryImage(
	content: Buffer,
	cursor?: {
		cookie: bigint;
	},
) {
	const entries: (FilesystemTypeEvidence & {
		readonly name: string;
	})[] = [];
	for (let offset = 0; offset < content.length;) {
		if (content.length - offset < 24) throw new Error("invalid_directory_image");
		const length = content.readUInt16LE(offset + 16);
		const end = offset + length;
		const zero = content.indexOf(0, offset + 19);
		if (length < 24 || length % 8 || end > content.length || zero < offset + 20 || zero >= end)
			throw new Error("invalid_directory_image");
		const name = content.subarray(offset + 19, zero);
		const decoded = name.toString("utf8");
		if (decoded.includes("/") || !Buffer.from(decoded).equals(name))
			throw new Error("unrepresentable_directory_name");
		if (cursor) {
			const cookie = content.readBigInt64LE(offset + 8);
			if (cookie <= cursor.cookie) throw new Error("invalid_directory_image");
			cursor.cookie = cookie;
			content.fill(0, zero + 1, end);
		}
		const type = content[offset + 18];
		if (decoded !== "." && decoded !== "..")
			entries.push({
				name: decoded,
				...Object.fromEntries(FILESYSTEM_TYPES.map(([, check, code]) => [check, () => code === type])),
			} as (typeof entries)[number]);
		offset = end;
	}
	return entries.sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)));
}
/** Observe a directory through an independent OFD without advancing owned input access times. */
export async function readFilesystemDirectory(
	target: string,
	view?: FilesystemReadView,
): Promise<
	(FilesystemTypeEvidence & {
		readonly name: string;
	})[]
> {
	if (!nativeFilesystem) return fs.readdir(target, { withFileTypes: true });
	view ??= FilesystemReadView.ambient();
	const handle = await view.readDirectory(target);
	try {
		const entries: ReturnType<typeof directoryImage> = [];
		for await (const content of readDirectoryChunks(handle))
			entries.push(
				...(await mapFilesystem(directoryImage(content), async (entry) =>
					filesystemEntryType(entry) === "other"
						? Object.assign(await view.stat(path.join(target, entry.name)), {
								name: entry.name,
							})
						: entry,
				)),
			);
		return entries.sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)));
	} finally {
		await handle.close();
	}
}
/** The yielded buffer is borrowed until the next read; callers retain only admitted bytes. */
async function* readDirectoryChunks(handle: FileHandle, maxBytes = Infinity, signal?: AbortSignal) {
	const buffer = Buffer.allocUnsafe(speculativeConfig().filesystem.directoryReadBytes);
	let used = 0;
	for (;;) {
		signal?.throwIfAborted();
		const count = nativeFilesystem!(handle.fd, buffer);
		if (!Number.isInteger(count) || count > buffer.length) throw new Error("invalid_directory_read");
		if (count < 0) throw filesystemReadError("directory read failed", count);
		if (!count) return;
		used += count;
		if (used > maxBytes) throw new Error("directory image exceeds capture budget");
		yield buffer.subarray(0, count);
	}
}
/** Open an independent reader on one regular inode without changing owned inputs' access times. */
export async function openFilesystemFile(
	target: string,
	verifyPath = true,
	view?: FilesystemReadView,
): Promise<{
	handle: FileHandle;
	binding: FilesystemObjectHandle | undefined;
	stat: BigIntStats;
}> {
	view ??= FilesystemReadView.ambient();
	// O_PATH pins even executable aliases without admitting I/O on a raced-in FIFO or device.
	const binding =
		process.platform === "linux" && (process.arch === "x64" || process.arch === "arm64")
			? await view.pin(target, !verifyPath)
			: undefined;
	let handle: FileHandle | undefined;
	try {
		const before = binding ? await binding.stat({ bigint: true }) : await view.stat(target, !verifyPath);
		if (!before.isFile()) throw new Error("not_regular_file");
		handle = await fs.open(
			binding ? `/proc/self/fd/${binding.fd}` : target,
			constants.O_RDONLY |
				(binding || !verifyPath ? 0 : (constants.O_NOFOLLOW ?? 0)) |
				(constants.O_NONBLOCK ?? 0) |
				(binding && before.uid === BigInt(process.getuid!()) ? constants.O_NOATIME : 0),
		);
		if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true })))
			throw new FilesystemCaptureChangedError("file_changed_during_capture");
		return { handle, binding, stat: before };
	} catch (error) {
		try {
			await handle?.close();
		} finally {
			await binding?.close();
		}
		throw error;
	}
}
/** Pin and hash one file, verifying its final identity before cache publication or transfer. */
export async function captureStableFile(
	target: string,
	maxBytes = Number.POSITIVE_INFINITY,
	retainContent = false,
	observed?: Partial<Pick<StableFilesystemCapture, "stat" | "realPath">> & {
		readonly retainObject?: boolean;
		readonly digest?: false;
		readonly verifyPath?: boolean;
		readonly view?: FilesystemReadView;
		readonly onChunk?: (bytes: Buffer) => Promise<void>;
	},
	signal?: AbortSignal,
): Promise<StableFilesystemCapture> {
	const view = observed?.view ?? FilesystemReadView.ambient();
	if (retainContent && observed?.onChunk) throw new Error("content capture requires one storage strategy");
	signal?.throwIfAborted();
	const startedAt = performance.now();
	const takenAtMs = Date.now();
	const verifyPath = observed?.verifyPath !== false;
	let { handle, binding, stat: before } = await openFilesystemFile(target, verifyPath, view);
	try {
		if (observed?.stat && !sameFilesystemIdentity(observed.stat, before))
			throw new FilesystemCaptureChangedError("file_changed_during_capture");
		const beforePath = verifyPath ? (observed?.realPath ?? (await view.realpath(target))) : target;
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes)))
			throw new Error(`file_too_large:${before.size}`);
		// Another pending read may precede a same-tick rewrite; read under this identity.
		const hash = observed?.digest !== false ? createHash("sha256") : undefined;
		const content = retainContent ? Buffer.allocUnsafe(Number(before.size)) : undefined;
		const buffer = Buffer.allocUnsafe(
			content ? 1 : Math.max(1, Math.min(Number(before.size), speculativeConfig().filesystem.chunkBytes)),
		);
		let bytesRead = 0;
		for (;;) {
			signal?.throwIfAborted();
			const chunk = content && bytesRead < content.length ? content.subarray(bytesRead) : buffer;
			const { bytesRead: size } = await handle.read(chunk);
			if (size === 0) break;
			bytesRead += size;
			if (bytesRead > maxBytes) throw new Error(`file_too_large:${bytesRead}`);
			if (bytesRead > Number(before.size))
				throw new FilesystemCaptureChangedError("file_changed_during_capture");
			hash?.update(chunk.subarray(0, size));
			if (observed?.onChunk) await observed.onChunk(chunk.subarray(0, size));
		}
		const after = await handle.stat({ bigint: true });
		signal?.throwIfAborted();
		if (bytesRead !== Number(before.size) || !sameFilesystemIdentity(before, after))
			throw new FilesystemCaptureChangedError("file_changed_during_capture");
		if (verifyPath) {
			const afterPath = await view.realpath(target);
			const pathStat = await view.stat(target);
			if (beforePath !== afterPath || !sameFilesystemIdentity(after, pathStat))
				throw new FilesystemCaptureChangedError("file_changed_during_capture");
		}
		const result = {
			hash: hash?.digest("hex") ?? "",
			bytesRead,
			stat: after,
			...(content ? { content } : {}),
			realPath: beforePath,
			computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())),
		};
		if (hash) rememberCapture(result, takenAtMs);
		if (observed?.retainObject && retainContent && binding) {
			const object = new CapturedFilesystemObject(binding, result);
			binding = undefined;
			return { ...result, object };
		}
		return result;
	} catch (error) {
		signal?.throwIfAborted();
		throw error;
	} finally {
		try {
			await handle.close();
		} finally {
			await binding?.close();
		}
	}
}
/** Observe children inside the same directory capture. I/O admission never spans the child traversal. */
export async function captureFilesystemDirectory<Value = undefined>(
	target: string,
	options: {
		readonly view?: FilesystemReadView;
		readonly stat?: BigIntStats;
		readonly io?: <Result>(read: () => Promise<Result>) => Promise<Result>;
		readonly visit?: (
			entries: readonly (FilesystemTypeEvidence & {
				readonly name: string;
			})[],
		) => Promise<Value>;
	} = {},
) {
	return withFilesystemReadView(options.view, async (view) => {
		const io = options.io ?? (<Result>(read: () => Promise<Result>) => read());
		const before = options.stat ?? (await io(() => view.stat(target)));
		if (!before.isDirectory()) throw new Error("not_directory");
		const startedAt = performance.now();
		const entries = await io(() => readFilesystemDirectory(target, view));
		const computation = TimelineInterval.own(new TimelineInterval(startedAt, performance.now()));
		const identity = (
			entry: FilesystemTypeEvidence & {
				readonly name: string;
			},
		) => `${filesystemEntryType(entry)}\0${entry.name}`;
		const image = entries.map(identity);
		const value = await options.visit?.(entries);
		const afterEntries = await io(() => readFilesystemDirectory(target, view));
		const info = await io(() => view.stat(target));
		if (
			!info.isDirectory() ||
			!sameFilesystemIdentity(before, info) ||
			image.length !== afterEntries.length ||
			image.some((entry, index) => entry !== identity(afterEntries[index]!))
		) {
			throw new FilesystemCaptureChangedError("directory_changed_during_capture");
		}
		return { info, entries, computation, value };
	});
}
/** Own a directory listing or link target together with its stable entry identity. */
export async function captureFilesystemEntry(
	target: string,
	read?: "directory" | "identity",
	view?: FilesystemReadView,
): Promise<{
	info: BigIntStats;
	link: string | undefined;
	entries: Awaited<ReturnType<typeof readFilesystemDirectory>> | undefined;
	computation: TimelineInterval;
}> {
	view ??= FilesystemReadView.ambient();
	const startedAt = performance.now();
	const before = await view.stat(target);
	if (read === "directory" && before.isDirectory()) {
		const { info, entries, computation } = await captureFilesystemDirectory(target, { view, stat: before });
		return { info, entries, link: undefined, computation };
	}
	const link = before.isSymbolicLink() ? await view.readlink(target) : undefined;
	const info = link !== undefined || read !== undefined ? await view.stat(target) : before;
	if (!sameFilesystemIdentity(before, info))
		throw new FilesystemCaptureChangedError(
			`${link !== undefined ? "symlink" : "directory"}_changed_during_capture`,
		);
	return {
		info,
		link,
		entries: undefined,
		computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())),
	};
}
/** Resolve link targets within the caller's view; its owner verifies the completed capture. */
export async function* walkFilesystemPath(
	target: string,
	options: {
		readonly start?: string;
		readonly followFinal?: boolean;
		readonly capture?: typeof captureFilesystemEntry;
		readonly view?: FilesystemReadView;
	} = {},
) {
	await using owned = options.view ? undefined : FilesystemReadView.ambient();
	const view = options.view ?? owned!;
	const start = options.start ?? path.parse(target).root;
	let current = start;
	let links = 0;
	const pending = target.slice(start.length).split(path.sep).filter(Boolean);
	for (;;) {
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>> | undefined;
		try {
			captured = await (options.capture ?? captureFilesystemEntry)(current, undefined, view);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const { info, link } = captured ?? {};
		const terminal = pending.length === 0 && (link === undefined || options.followFinal === false);
		if (links && (!info || terminal)) {
			// Magic links name kernel handles; their displayed pathname need not identify that object.
			const actual = await view
				.stat(target, options.followFinal !== false)
				.catch((error: NodeJS.ErrnoException) => {
					if (error.code !== "ENOENT") throw error;
				});
			if (info ? !actual || !sameFilesystemIdentity(info, actual) : actual)
				throw new Error("filesystem_link_resolution_changed");
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
export function sharedWalk<Capture, Input = string>(
	capture: (target: Input) => Capture,
	key: (target: Input) => string = String,
) {
	const walked = new Map<string, Capture>();
	return (target: Input) => {
		const name = key(target);
		return walked.get(name) ?? walked.set(name, capture(target)).get(name)!;
	};
}
export async function assertNoSymlinkPath(
	root: string,
	target: string,
	capture?: typeof captureFilesystemEntry,
): Promise<void> {
	const resolvedRoot = path.resolve(root);
	const resolvedTarget = path.resolve(target);
	if (!containsFilesystemPath(resolvedRoot, resolvedTarget))
		throw new Error(`sandbox path escapes workspace: ${resolvedTarget}`);
	for await (const entry of walkFilesystemPath(resolvedTarget, {
		start: resolvedRoot,
		...(capture ? { capture } : {}),
	})) {
		const first = entry.path === resolvedRoot;
		const info = entry.info;
		if (!info) {
			if (first) throw new Error(`sandbox workspace root does not exist: ${resolvedRoot}`);
			break;
		}
		if (first && (info.isSymbolicLink() || !info.isDirectory()))
			throw new Error("sandbox workspace root must be a real directory");
		if (info.isSymbolicLink())
			throw new Error(`sandbox path contains symlink: ${slash(path.relative(resolvedRoot, entry.path))}`);
		if (!info.isFile() && !info.isDirectory()) throw new Error("sandbox path contains a special file");
	}
}
/** Publish an owned JSON stage without deleting the previous snapshot on failure. */
export async function writeJsonFile(
	file: string,
	value: unknown,
	space?: number,
	mode?: number,
	reservedRoot = false,
): Promise<void> {
	if (value === undefined) return fs.rm(file, { force: true });
	if (!reservedRoot) await fs.mkdir(path.dirname(file), { recursive: true });
	const temporary = `${file}.${randomUUID()}.tmp`;
	const handle = await fs.open(temporary, "wx", mode);
	try {
		try {
			await handle.writeFile(`${JSON.stringify(value, null, space)}\n`, "utf8");
		} finally {
			await handle.close();
		}
		await fs.rename(temporary, file);
	} finally {
		await fs.rm(temporary, { force: true }).catch(() => undefined);
	}
}
