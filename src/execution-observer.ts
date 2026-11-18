import path from "node:path";
import { speculativeConfig } from "./config-schema.ts";
import {
	FILESYSTEM_OBSERVATION_FIELDS,
	type FilesystemObservationField,
	filesystemObservationDigest,
} from "./filesystem-evidence.ts";
import { type Sha256Digest, containsLogicalPath, sha256Digest, stableEqual } from "./primitives.ts";
import { type DependencyRole, type ProvenanceTaint } from "./provenance-certificate.ts";
// Linux new_utsname contains six 65-byte fields, including their padding.
export const UTSNAME_BYTES = 6 * 65;

export type ObservedProcessPath =
	/** `listed` when its entries reached the run: it read them, or a removal or rename found some. */
	| {
			readonly path: string;
			readonly role: DependencyRole;
			readonly listed?: true;
			/** Uniform non-writing lookup error; the dependency walker must independently prove the failing component. */
			readonly lookupFailure?: "ENOENT" | "ENOTDIR";
	  }
	| ({
			readonly path: string;
			readonly role: "metadata";
			readonly followSymlinks: boolean;
			readonly afterWrite?: true;
	  } & StatObservation);
export interface ExecutionObservation {
	readonly streamMetadataObserved?: boolean;
	readonly output?: readonly {
		readonly fd: 1 | 2;
		readonly data: Buffer;
	}[];
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	/** Every process its trace holds, a brokered run's launcher among them. */
	readonly pids?: readonly number[];
	readonly incompleteReasons: readonly string[];
	/** Conflicting observations require an enclosing transaction to prove the private object vanished. */
	readonly metadataChanges?: readonly string[];
	/** Bounded diagnostic names only; never dependency evidence or a completeness condition. */
	readonly unsupportedSyscalls?: readonly string[];
	readonly unsupportedSyscallsTruncated?: true;
	/** Workspace paths, relative to it, the traced processes created, removed, renamed or opened to write. */
	readonly written?: readonly string[];
	/** Paths outside the workspace it wrote, which a private branch took. */
	readonly external?: readonly string[];
	/** Locks it took without waiting, or found free: each holds while no one else holds one. */
	readonly locks?: readonly {
		readonly path: string;
		readonly exclusive: boolean;
	}[];
}
export interface ExecutionObservationOptions {
	/** Authenticated host subcomputation channel; its input evidence is imported by the caller. */
	readonly controlledSocket?: string;
	readonly dispatcherSocket?: string;
	readonly platformFingerprint?: string;
	/** A native exec boundary in this trace; the original execution owns all records. */
	readonly execution?: {
		readonly pid: number;
		readonly start: number;
	};
	readonly captureOutput?: true;
	/** Original output OFDs stay in the domain; their observed status must end as it began. */
	readonly outputFlags?: readonly [number, number];
	/** Exact root launch alias; the prototype separately binds the installed executable bytes. */
	readonly launchPath?: string;
	/** Writes owned by a brokered launcher and its descendants: relative workspace names or absolute external paths. */
	readonly brokeredWrites?: (pid: number) => readonly string[] | undefined;
	/** These original records already belong to child evidence imported by the caller. */
	readonly fileEvidence?: (record: object) => boolean;
	/** The session's private branch, where a descriptor on a copy shows the copy: it stands for the host path it shadows. */
	readonly privateUpper?: string;
	/** Bounded prefix lookup for running work; its evidence is always incomplete. */
	readonly previewBytes?: number;
	/**
	 * Workspace roots whose driver-specific unsupported errors must invalidate adoption. This keeps
	 * a COW substrate from changing a command result when the Actor filesystem supports the syscall.
	 */
	readonly guardFilesystemSemanticsWithin?: readonly string[];
	/** The capture sockets serving the traced command's output (`socket:[inode]`): naming them reveals no outside peer. */
	readonly outputEndpoints?: readonly string[];
}
// Linux x86-64 syscall numbers carried by the collector ABI.
// Each row starts at its first Linux x86-64 syscall number. Gaps have no entry.
const syscallNames: Readonly<Record<number, string>> = Object.fromEntries(
	`0 read write open close stat fstat lstat
8 lseek mmap mprotect
16 ioctl pread64 pwrite64 readv writev access pipe
28 madvise shmget shmat shmctl dup dup2
39 getpid sendfile socket connect accept sendto recvfrom sendmsg recvmsg
48 shutdown bind listen getsockname getpeername socketpair setsockopt getsockopt
56 clone fork vfork execve
61 wait4 kill uname
64 semget semop semctl shmdt msgget msgsnd msgrcv msgctl fcntl flock
76 truncate ftruncate getdents
80 chdir fchdir rename mkdir rmdir creat link unlink symlink readlink chmod
91 fchmod chown fchown lchown
97 getrlimit getrusage sysinfo
101 ptrace
105 setuid setgid
110 getppid getpgrp
113 setreuid setregid
116 setgroups setresuid
119 setresgid
121 getpgid setfsuid setfsgid getsid capget capset
132 utime mknod
135 personality
137 statfs fstatfs
155 pivot_root
157 prctl
160 setrlimit chroot
163 acct
165 mount umount2 swapon swapoff reboot sethostname setdomainname
175 init_module delete_module
179 quotactl
186 gettid
188 setxattr lsetxattr fsetxattr getxattr lgetxattr fgetxattr listxattr
195 llistxattr flistxattr removexattr lremovexattr fremovexattr tkill
202 futex
204 sched_getaffinity
206 io_setup
209 io_submit
212 lookup_dcookie
217 getdents64
220 semtimedop fadvise64
234 tgkill utimes
240 mq_open
242 mq_timedsend mq_timedreceive
246 kexec_load
248 add_key request_key keyctl
257 openat mkdirat mknodat fchownat futimesat newfstatat unlinkat renameat linkat
266 symlinkat readlinkat fchmodat faccessat
272 unshare
275 splice tee
278 vmsplice
280 utimensat
285 fallocate
288 accept4
292 dup3 pipe2
295 preadv pwritev
298 perf_event_open recvmmsg
302 prlimit64 name_to_handle_at open_by_handle_at
307 sendmmsg setns
310 process_vm_readv process_vm_writev
313 finit_module
316 renameat2 seccomp
321 bpf execveat userfaultfd
326 copy_file_range
327 preadv2 pwritev2 pkey_mprotect
332 statx
424 pidfd_send_signal io_uring_setup io_uring_enter io_uring_register open_tree
429 move_mount fsopen fsconfig fsmount fspick pidfd_open clone3 close_range
437 openat2 pidfd_getfd faccessat2
442 mount_setattr
452 fchmodat2`
		.split("\n")
		.flatMap((row) => {
			const [first, ...names] = row.split(" ");
			return names.map((name, i) => [Number(first) + i, name]);
		}),
);

interface FileIdentity {
	inode: bigint;
	device: number;
	mode: number;
	flags: number;
}
function identity(data: Buffer, offset: number): FileIdentity | undefined {
	const inode = data.readBigUInt64LE(offset);
	if (!inode) return;
	return {
		inode,
		device: data.readUInt32LE(offset + 8),
		mode: data.readUInt16LE(offset + 12),
		flags: data.readUInt32LE(offset + 16),
	};
}
function objectKey(file?: FileIdentity) {
	return file && file.device + ":" + file.inode;
}
function endpoint(file?: FileIdentity) {
	const type = file && file.mode & 0xf000;
	if (type === 0x1000 || type === 0xc000)
		return (type === 0x1000 ? "pipe" : "socket") + ":[" + file!.inode + "]";
}
type BinaryRecord = {
	pid: number;
	at: number;
	start: number;
	order: number;
	name: string;
	args: bigint[];
	layout: number;
	operands: (string | undefined)[];
	anchors: (FileIdentity | undefined)[];
	resolved: { path: string; file?: FileIdentity }[];
	result?: number;
	file?: FileIdentity;
	second?: FileIdentity;
	lock?: number;
	paths?: string[];
	child?: number;
	childTgid?: number;
	parentTgid?: number;
	sharedFiles?: boolean;
	sharedFS?: boolean;
	previous?: number;
	broker?: boolean;
	image?: string;
	socket?: string;
	addressFamily?: number;
	output?: Buffer[];
	stat?: StatObservation;
	nullDevice?: boolean;
	capabilityTarget?: number;
	statusFlags?: number;
	pathErrors?: number;
	incomplete?: string;
	pending?: boolean;
	notified?: boolean;
	response?: Buffer;
	returned?: boolean;
	virtual?: string;
	descriptorPath?: string;
	unsupported?: { name?: string };
	utsFingerprint?: Sha256Digest;
	beforeExec?: BinaryRecord;
	exec?: boolean;
};
type TraceFile = { pid: number; lines: BinaryRecord[] };
type StatObservation = {
	readonly digest: Sha256Digest;
	readonly fields?: readonly FilesystemObservationField[];
};
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
function pathname(bytes: Buffer) {
	const value = decoder.decode(bytes.at(-1) === 0 ? bytes.subarray(0, -1) : bytes);
	if (value.includes("\0")) throw new Error("invalid execution pathname");
	return value;
}
function isStat(name: string) {
	return ["stat", "lstat", "fstat", "newfstatat", "statx"].includes(name);
}
function isOpen(name: string) {
	return ["open", "openat", "openat2", "creat"].includes(name);
}
function isExec(name: string) {
	return name === "execve" || name === "execveat";
}
function physicalName(value: string, upper?: string) {
	return upper && value.startsWith(upper + "/") ? value.slice(upper.length) : value;
}
function appendGrouped<K, V>(map: Map<K, V[]>, key: K, value: V) {
	const values = map.get(key);
	if (values) values.push(value);
	else map.set(key, [value]);
}
export function executionRecords(read: () => Buffer, privateUpper?: string) {
	const journal: BinaryRecord[] = [];
	const unsupportedCalls = new Map<bigint, { name?: string }>();
	const calls = new Map<bigint, BinaryRecord>();
	const executing = new Map<number, BinaryRecord>();
	const inputs = new Map<
		string,
		{ image?: string; sealed: boolean; links: readonly (readonly [string, string])[] }
	>();
	const inputFrames = new Map<bigint, { chunks: Buffer[]; bytes: number }>();
	const epoch = Date.now() - Number(process.hrtime.bigint()) / 1e6;
	let failure: Error | undefined;
	let domain: bigint | undefined;
	const complete = (id: bigint, record: BinaryRecord) => {
		if (!record.returned || (record.notified && !record.response)) return;
		if (record.response) {
			const flags = record.response.readUInt32LE(20);
			const error = record.response.readInt32LE(16);
			if (
				flags > 1 ||
				error > 0 ||
				(!(flags & 1) && record.result !== (error || Number(record.response.readBigInt64LE(8))))
			)
				record.incomplete = "native_metadata_response_changed";
		}
		record.pending = false;
		calls.delete(id);
	};
	return {
		journal,
		get inputs(): ReadonlyMap<string, ReturnType<typeof inputs.get>> {
			if (failure) throw failure;
			if (inputFrames.size) throw new Error("incomplete execution input record");
			return inputs;
		},
		read() {
			if (failure) throw failure;
			try {
				const bytes = read();
				for (let offset = 0; offset < bytes.length; ) {
					if (bytes.length - offset < 48) throw new Error("truncated execution record");
					const owner = bytes.readBigUInt64LE(offset);
					const task = bytes.readBigUInt64LE(offset + 8);
					const stamp = Number(bytes.readBigUInt64LE(offset + 16)) / 1e6;
					const id = bytes.readBigUInt64LE(offset + 24);
					const kind = Number(bytes.readBigUInt64LE(offset + 32));
					const size = bytes.readUInt32LE(offset + 40);
					const error = bytes.readInt32LE(offset + 44);
					const end = offset + ((48 + size + 7) & ~7);
					domain ??= owner;
					if (
						!id ||
						!task ||
						owner !== domain ||
						size > speculativeConfig().nativeBuild.domainEventBytes ||
						end > bytes.length
					)
						throw new Error("invalid execution record envelope");
					const body = bytes.subarray(offset + 48, offset + 48 + size);
					offset = end;
					const pid = Number(task & 0xffffffffn);
					if ([10, 11, 138, 139].includes(kind)) {
						let frame = inputFrames.get(id);
						if (error || !size || !!frame !== ((kind & 127) === 11))
							throw new Error("invalid native input frame");
						if (!frame) inputFrames.set(id, (frame = { chunks: [], bytes: 0 }));
						frame.bytes += size;
						frame.chunks.push(body);
						if (
							frame.bytes > speculativeConfig().nativeBuild.domainInputBytes ||
							inputFrames.size > speculativeConfig().nativeBuild.domainInputFrames
						)
							throw new Error("native input record exceeds its bound");
						if (!(kind & 128)) continue;
						const row = JSON.parse(decoder.decode(Buffer.concat(frame.chunks))) as {
							path: string;
							image?: string;
							sealed?: boolean;
							ambiguous?: boolean;
							links?: [string, string][];
						};
						inputFrames.delete(id);
						if (typeof row.path !== "string" || !path.posix.isAbsolute(row.path) || row.path.includes("\0"))
							throw new Error("invalid native input pathname");
						const previous = inputs.get(row.path);
						if (row.ambiguous === true) {
							if (previous) inputs.set(row.path, { sealed: false, links: [] });
							continue;
						}
						if (
							typeof row.image !== "string" ||
							!/^[0-9a-f]{16}$/.test(row.image) ||
							typeof row.sealed !== "boolean" ||
							!Array.isArray(row.links) ||
							row.links.length > speculativeConfig().filesystem.maxSymlinkDepth ||
							row.links.some(
								(link) =>
									!Array.isArray(link) ||
									link.length !== 2 ||
									typeof link[0] !== "string" ||
									!path.posix.isAbsolute(link[0]) ||
									typeof link[1] !== "string" ||
									link.some((value) => value.includes("\0")),
							)
						)
							throw new Error("invalid native input index");
						if (!previous && inputs.size >= speculativeConfig().processReuse.maxObservedInputs)
							throw new Error("native input index exceeds its bound");
						const agreeing = previous?.image === row.image && stableEqual(previous.links, row.links);
						if (agreeing && previous!.sealed === row.sealed) continue;
						inputs.set(
							row.path,
							previous && !agreeing
								? { sealed: false, links: [] }
								: { image: row.image, sealed: row.sealed, links: row.links },
						);
						continue;
					}
					const append = (name: string): BinaryRecord => {
						const record: BinaryRecord = {
							pid,
							start: stamp,
							order: stamp,
							at: epoch + stamp,
							name,
							layout: 0,
							args: [],
							anchors: [],
							operands: [],
							resolved: [],
						};
						journal.push(record);
						return record;
					};
					if (kind === 26 || kind === 27) {
						if (error || size !== (kind === 26 ? 8 : 0)) throw new Error("invalid unsupported syscall record");
						let call = unsupportedCalls.get(id);
						if (!call) unsupportedCalls.set(id, (call = {}));
						if (kind === 26) {
							if (call.name) throw new Error("duplicate unsupported syscall definition");
							const nr = body.readBigUInt64LE();
							call.name = syscallNames[Number(nr)] ?? "syscall_" + nr;
						}
						append("unsupported_syscall").unsupported = call;
						continue;
					}
					if (kind === 24 && !error && size === 32) {
						const record = append(body.readUInt32LE(28) ? "descriptor_write" : "descriptor_read");
						record.anchors = [identity(body, 0)];
						record.args = [BigInt(body.readUInt32LE(24))];
						record.result = 0;
						continue;
					}
					if (kind === 13 || kind === 14 || kind === 15 || kind === 12) {
						if (error) throw new Error("unreadable execution lifecycle record");
						if (kind === 13 && size === 16) {
							const event = append("forked");
							event.child = body.readUInt32LE();
							event.parentTgid = Number(task >> 32n);
							event.childTgid = body.readUInt32LE(4);
							event.sharedFiles = Boolean(body.readUInt32LE(12));
							event.sharedFS = Boolean(body.readUInt32LE(8));
						} else if (kind === 14 && size > 32) {
							const event = append("image");
							event.file = identity(body, 0);
							event.previous = body.readUInt32LE(24);
							event.broker = Boolean(body.readUInt32LE(28));
							event.image = physicalName(pathname(body.subarray(32)), privateUpper);
							event.beforeExec = executing.get(event.previous);
							if (event.beforeExec) {
								event.beforeExec.exec = true;
								event.incomplete = event.beforeExec.incomplete;
							}
							executing.delete(event.previous);
						} else if (kind === 15 && size === 4) {
							append("terminated").result = body.readInt32LE();
							executing.delete(pid);
						} else if (kind === 12 && size === 16) {
							const name = syscallNames[Number(body.readBigUInt64LE())];
							if (!name) throw new Error("unknown PID observation");
							append(name).result = Number(body.readBigInt64LE(8));
						} else throw new Error("invalid execution lifecycle record");
						continue;
					}
					if (kind === 16) {
						if (error || size !== 104 || calls.has(id)) throw new Error("invalid execution call");
						const name = syscallNames[body.readUInt32LE()];
						if (!name) throw new Error("unknown execution syscall " + body.readUInt32LE());
						const record = append(name);
						record.pending = true;
						record.layout = body.readUInt32LE(4);
						record.args = Array.from({ length: 6 }, (_, index) => body.readBigUInt64LE(8 + 8 * index));
						record.anchors = [identity(body, 56), identity(body, 80)];
						calls.set(id, record);
						if (isExec(name)) executing.set(pid, record);
						continue;
					}
					const record = calls.get(id);
					if (!record) throw new Error("execution record has no matching entry");
					if (error) {
						if (kind === 17 || kind === 18) {
							record.pathErrors = (record.pathErrors ?? 0) | (1 << (kind - 17));
							continue;
						}
						record.incomplete = "unreadable_execution_record:" + record.name;
						if (kind === 20 || kind === 6) {
							record.returned = true;
							complete(id, record);
						}
						continue;
					}
					if (kind === 17 || kind === 18) {
						record.operands[kind - 17] = pathname(body);
						record.pathErrors = (record.pathErrors ?? 0) & ~(1 << (kind - 17));
					} else if (kind === 23 && size > 24)
						record.resolved.push({
							path: physicalName(pathname(body.subarray(24)), privateUpper),
							file: identity(body, 0),
						});
					else if (kind === 22 && size >= 2) {
						record.addressFamily = body.readUInt16LE();
						if (record.addressFamily === 1) {
							const end = body.indexOf(0, 2);
							record.socket = pathname(body.subarray(2, end < 0 ? size : end));
						}
					} else if (kind === 21) (record.output ??= []).push(body);
					else if (kind === 25 && !size && isExec(record.name))
						record.incomplete = "exec_projection_unproven";
					else if (
						kind === 0 &&
						size === 80 &&
						body.readUInt32LE(8) === record.pid &&
						syscallNames[body.readUInt32LE(16)] === record.name &&
						!record.notified
					)
						record.notified = true;
					else if (kind === 1 || kind === 7) {
						const value = physicalName(pathname(body), privateUpper);
						if (kind === 1) record.virtual = value;
						else record.descriptorPath = value;
					} else if (kind === 4 && size === 24 && !record.response) {
						record.response = body;
						complete(id, record);
					} else if (kind === 5 || kind === 9) {
						if (kind === 5) record.incomplete = "native_metadata_rejected";
						else if (!record.exec) record.incomplete = "interrupted_execution_call:" + record.name;
						record.pending = false;
						calls.delete(id);
					} else if (kind === 20 && size === 64 && !record.returned) {
						record.result = Number(body.readBigInt64LE());
						record.file = identity(body, 8);
						record.second = identity(body, 32);
						record.lock = body.readInt32LE(56);
						if (record.name === "capget") record.capabilityTarget = body.readUInt32LE(60);
						if (record.name === "ioctl" || record.name === "fcntl")
							record.statusFlags = body.readUInt32LE(60);
						record.returned = true;
						record.order = stamp;
						complete(id, record);
					} else if (
						kind === 6 &&
						size >= 32 &&
						!record.returned &&
						syscallNames[Number(body.readBigUInt64LE())] === record.name &&
						body.readUInt32LE(24) === size - 32
					) {
						record.result = Number(body.readBigInt64LE(8));
						if (record.result === 0 && record.name === "uname") {
							if (size !== 32 + UTSNAME_BYTES) throw new Error("invalid uname result");
							record.utsFingerprint = sha256Digest(body.subarray(32));
						}
						if (record.result === 0 && isStat(record.name)) {
							const stat = body.subarray(32);
							const extended = record.name === "statx";
							if (stat.length !== (extended ? 256 : 144)) throw new Error("invalid stat ABI");
							record.stat = nativeStatObservation(stat, extended);
							record.nullDevice =
								((extended ? stat.readUInt16LE(28) : stat.readUInt32LE(24)) & 0xf000) === 0x2000 &&
								(extended
									? stat.readUInt32LE(128) === 1 && stat.readUInt32LE(132) === 3
									: stat.readBigUInt64LE(40) === 259n);
						}
						record.returned = true;
						record.order = stamp;
						complete(id, record);
					} else throw new Error("unexpected execution frame " + kind);
				}
			} catch (error) {
				throw (failure = error instanceof Error ? error : new Error(String(error)));
			}
		},
	};
}

function nativeStatObservation(bytes: Buffer, extended: boolean): StatObservation {
	const integer = (offset: number, size = 8, signed = false) => {
		if (size === 2) return BigInt(bytes.readUInt16LE(offset));
		if (size === 4) return BigInt(bytes.readUInt32LE(offset));
		return signed ? bytes.readBigInt64LE(offset) : bytes.readBigUInt64LE(offset);
	};
	const time = (offset: number) =>
		integer(offset, 8, true) * 1000000000n + integer(offset + 8, extended ? 4 : 8);
	const device = (offset: number) => linuxDevice(integer(offset, 4), integer(offset + 4, 4));
	const values = extended
		? {
				dev: device(136),
				ino: integer(32),
				mode: integer(28, 2),
				nlink: integer(16, 4),
				uid: integer(20, 4),
				gid: integer(24, 4),
				rdev: device(128),
				size: integer(40),
				blksize: integer(4, 4),
				blocks: integer(48),
				atimeNs: time(64),
				mtimeNs: time(112),
				ctimeNs: time(96),
			}
		: {
				dev: integer(0),
				ino: integer(8),
				nlink: integer(16),
				mode: integer(24, 4),
				uid: integer(28, 4),
				gid: integer(32, 4),
				rdev: integer(40),
				size: integer(48, 8, true),
				blksize: integer(56, 8, true),
				blocks: integer(64, 8, true),
				atimeNs: time(72),
				mtimeNs: time(88),
				ctimeNs: time(104),
			};
	const mask = extended ? bytes.readUInt32LE(0) : 0x7ff;
	const masks: Partial<Record<FilesystemObservationField, number>> = {
		mode: 3,
		nlink: 4,
		uid: 8,
		gid: 16,
		atimeNs: 32,
		mtimeNs: 64,
		ctimeNs: 128,
		ino: 256,
		size: 512,
		blocks: 1024,
	};
	const fields =
		mask & ~0x3ffff || (mask & 0x7ff) === 0x7ff
			? FILESYSTEM_OBSERVATION_FIELDS
			: FILESYSTEM_OBSERVATION_FIELDS.filter((name) => mask & (masks[name] ?? 0));
	return {
		digest: filesystemObservationDigest(values, fields),
		...(fields.length < FILESYSTEM_OBSERVATION_FIELDS.length ? { fields } : {}),
	};
}
function linuxDevice(major: bigint, minor: bigint): bigint {
	return ((major & 0xfffn) << 8n) | (minor & 0xffn) | ((minor & ~0xffn) << 12n) | ((major & ~0xfffn) << 32n);
}
export interface TracedWrite {
	readonly paths?: readonly string[];
	readonly at: number;
	readonly opened: boolean;
	readonly mapped?: true;
	readonly pid?: number;
}
export interface TraceTail extends AsyncDisposable {
	readonly writes: readonly TracedWrite[];
	readonly seen: ReadonlyMap<string, number>;
	readonly unsafe: boolean;
	readonly records: readonly TraceFile[];
	journal(pid: number): readonly BinaryRecord[] | undefined;
	read(): Promise<TraceTail>;
}
function writesFile(record: BinaryRecord) {
	if (record.name === "descriptor_write") return (record.anchors[0]?.mode ?? 0) >> 12 === 8;
	if (isOpen(record.name)) {
		const flags = Number(record.args[record.name === "open" ? 1 : 2] ?? 0n);
		return record.name === "creat" || !!(flags & (3 | 64 | 512));
	}
	return writeCalls.has(record.name);
}
const writeCalls = new Set(
	"truncate ftruncate rename renameat renameat2 link linkat symlink symlinkat mknod mknodat unlink unlinkat mkdir mkdirat rmdir utime utimes utimensat futimesat chmod fchmod fchmodat fchmodat2 chown lchown fchown fchownat descriptor_write write writev pwrite64 pwritev pwritev2 fallocate sendfile copy_file_range splice tee mmap".split(
		" ",
	),
);
function fileIoctl(record: BinaryRecord) {
	return (
		record.name === "ioctl" &&
		(record.result ?? -1) >= 0 &&
		(record.anchors[0]?.mode ?? 0) >> 12 === 8 &&
		![0x5450n, 0x5451n].includes(record.args[1]!)
	);
}
function filePath(name?: string) {
	return name?.startsWith("/") && !name.startsWith("/memfd:") && !name.endsWith(" (deleted)")
		? name
		: undefined;
}
function lookupPath(name: string, base?: string) {
	const value = name.startsWith("/") ? name : base && base + "/" + name;
	// '..' must be walked against the symlink evidence.
	return (
		value &&
		"/" +
			value
				.split("/")
				.filter((piece) => piece && piece !== ".")
				.join("/")
	);
}
export function traceTail(
	source: ReturnType<typeof executionRecords> | TraceTail,
	cwd: string,
	options: {
		execution?: { pid: number; start: number };
		rootPid?: () => number | undefined;
		launchPath?: string;
	} = {},
): TraceTail {
	const positions = new Map<number, number>();
	const files = new Map<number, BinaryRecord[]>();
	const seen = new Map<string, number>();
	const writes: TracedWrite[] = [];
	const cwds = new Map<number, { value: string }>();
	const names = new Map<string, string>();
	const members = new Set<number>();
	const pending = new Set<BinaryRecord>();
	let offset = 0;
	let closed = false;
	let unsafe = false;
	let reading: Promise<TraceTail> | undefined;
	if (options.execution) positions.set(options.execution.pid, options.execution.start);
	function paths(record: BinaryRecord, current: string): string[] | undefined {
		if (record.name === "descriptor_read" || record.name === "descriptor_write") {
			const name = names.get(objectKey(record.anchors[0]) ?? "");
			return name ? [name] : [];
		}
		if (record.name === "image")
			return [
				...new Set(
					[...(record.beforeExec ? (paths(record.beforeExec, current) ?? []) : []), record.image].filter(
						(x): x is string => !!filePath(x),
					),
				),
			];
		if (record.virtual && !(record.layout & 0xf00))
			return filePath(record.virtual) ? [lookupPath(record.virtual)!] : [];
		const found: string[] = [];
		for (let slot = 0; slot < 2; slot++) {
			const name = record.operands[slot];
			const arg = (record.layout >> (12 + 4 * slot)) & 15;
			const fd = arg ? Number(BigInt.asIntN(32, record.args[arg - 1]!)) : -100;
			const key = objectKey(record.anchors[slot]);
			const base = fd === -100 ? current : key && names.get(key);
			if (name) {
				const value = lookupPath(name, base);
				if (!value) {
					if ((record.result ?? -1) < 0 && fd !== -100 && !key) continue;
					return;
				}
				found.push(value);
			} else if (
				arg &&
				(record.name === "fstat" ||
					record.name === "fchdir" ||
					record.name.startsWith("getdents") ||
					isStat(record.name) ||
					isExec(record.name) ||
					writesFile(record) ||
					record.name === "flock" ||
					record.name === "fcntl")
			) {
				const value =
					(record.name.startsWith("getdents") && base) ||
					filePath(record.descriptorPath) ||
					(fd >= 0 ? base : undefined);
				if (value) found.push(value);
			}
		}
		if (!found.length && record.descriptorPath && filePath(record.descriptorPath))
			found.push(record.descriptorPath);
		return found;
	}
	function consume(record: BinaryRecord) {
		let current = cwds.get(record.pid);
		if (!current) cwds.set(record.pid, (current = { value: cwd }));
		if (Array.isArray(source.journal)) record.paths = paths(record, current.value);
		if (record.child) cwds.set(record.child, record.sharedFS ? current : { ...current });
		const ok = record.result !== undefined && record.result >= 0;
		if (ok && (record.name === "chdir" || record.name === "fchdir")) {
			if (record.paths?.[0]) current.value = record.paths[0];
			else unsafe = true;
		}
		if (ok && isOpen(record.name)) {
			const key = objectKey(record.file);
			const value =
				record.paths?.[0] ??
				record.resolved.find((entry) => filePath(entry.path) && objectKey(entry.file) === key)?.path;
			if (key && value) names.set(key, value);
		}
		for (const name of record.paths ?? []) seen.set(name, Math.max(seen.get(name) ?? 0, record.at));
		if (ok && writesFile(record) && (record.paths?.length || !endpoint(record.anchors[0]))) {
			writes.push({
				paths: record.paths?.length ? record.paths : undefined,
				at: record.at,
				pid: record.pid,
				opened: isOpen(record.name) || record.name === "mmap",
				...(record.name === "mmap" ? { mapped: true } : {}),
			});
		}
		unsafe ||=
			!!record.unsupported || !!record.incomplete || (ok && (fileIoctl(record) || unsafeCalls.has(record.name)));
	}
	async function advance() {
		if (closed) throw new Error("execution trace closed");
		let fresh: BinaryRecord[];
		if (Array.isArray(source.journal)) {
			const native = source as ReturnType<typeof executionRecords>;
			native.read();
			fresh = native.journal.slice(offset);
			offset = native.journal.length;
		} else {
			const parent = await (source as TraceTail).read();
			fresh = [];
			for (const [pid, start] of positions) {
				const items = parent.journal(pid)?.slice(start) ?? [];
				positions.set(pid, start + items.length);
				for (const record of items) {
					fresh.push(record);
					if (record.child && !positions.has(record.child)) positions.set(record.child, 0);
				}
			}
			fresh.sort((a, b) => a.start - b.start);
		}
		const owner = options.rootPid?.();
		if (owner && !positions.has(owner)) positions.set(owner, 0);
		const ready: BinaryRecord[] = [];
		for (const record of fresh) {
			if (owner && !positions.has(record.pid)) continue;
			if (owner && record.child) positions.set(record.child, 0);
			if (options.launchPath) {
				if (
					!members.size &&
					!record.broker &&
					record.name === "image" &&
					(record.image === options.launchPath || record.beforeExec?.operands.includes(options.launchPath))
				)
					members.add(record.pid);
				if (!members.has(record.pid)) continue;
				if (record.child) members.add(record.child);
			}
			appendGrouped(files, record.pid, record);
			if (!record.pending) ready.push(record);
			else pending.add(record);
		}
		for (const record of pending)
			if (!record.pending) {
				pending.delete(record);
				ready.push(record);
			}
		for (const record of ready.sort((a, b) => a.order - b.order)) consume(record);
		return tail;
	}
	const tail: TraceTail = {
		writes,
		seen,
		get unsafe() {
			return (
				unsafe ||
				[...pending].some(
					(record) => writesFile(record) || unsafeCalls.has(record.name) || record.name === "ioctl",
				)
			);
		},
		get records() {
			return [...files].map(([pid, lines]) => ({ pid, lines }));
		},
		journal(pid) {
			return files.get(pid);
		},
		read() {
			return (reading ??= advance().finally(() => {
				reading = undefined;
			}));
		},
		async [Symbol.asyncDispose]() {
			closed = true;
			await reading;
		},
	};
	return tail;
}
const unsafeCalls = new Set(
	"mount umount2 move_mount pivot_root chroot setns unshare io_uring_setup io_submit setxattr lsetxattr fsetxattr removexattr lremovexattr fremovexattr".split(
		" ",
	),
);

const networkCalls = new Set(
	"socket socketpair connect bind listen shutdown accept accept4 sendto recvfrom sendmsg recvmsg sendmmsg recvmmsg getsockname getpeername getsockopt setsockopt".split(
		" ",
	),
);
const ipcCalls = new Set(
	"msgget msgsnd msgrcv msgctl semget semctl semop semtimedop shmat shmget shmdt shmctl mq_open mq_timedsend mq_timedreceive".split(
		" ",
	),
);
const confinedCalls = new Set(
	"setuid setgid setreuid setregid setgroups setresuid setresgid setfsuid setfsgid fsopen fsconfig fsmount fspick open_tree mount_setattr seccomp capget capset mount umount2 move_mount pivot_root chroot swapon swapoff reboot sethostname setdomainname kexec_load init_module finit_module delete_module unshare setns perf_event_open bpf userfaultfd keyctl add_key request_key ptrace process_vm_readv process_vm_writev open_by_handle_at name_to_handle_at quotactl acct lookup_dcookie io_setup io_submit io_uring_setup io_uring_enter io_uring_register personality".split(
		" ",
	),
);
const unmodeled = new Set(
	"fallocate splice tee fgetxattr flistxattr fremovexattr fsetxattr getxattr lgetxattr listxattr llistxattr lremovexattr lsetxattr removexattr setxattr".split(
		" ",
	),
);
export async function observeExecution(
	source: TraceTail,
	executable: string,
	cwd: string,
	options: ExecutionObservationOptions = {},
): Promise<ExecutionObservation> {
	const { records } = await source.read();
	const parents = new Map<number, number>();
	const byPid = new Map(records.map((file) => [file.pid, file.lines]));
	const replaced = new Set(
		records.flatMap(({ lines }) =>
			lines.flatMap((record) =>
				record.name === "image" && record.previous && record.previous !== record.pid ? [record.previous] : [],
			),
		),
	);
	for (const file of records)
		for (const record of file.lines) if (record.child) parents.set(record.child, file.pid);
	const target = options.launchPath ?? path.posix.resolve(cwd, executable);
	let root = options.execution;
	if (!root) {
		const candidates = records
			.flatMap(({ pid, lines }) => {
				const start = lines.findIndex(
					(record) =>
						record.name === "image" &&
						!record.broker &&
						(record.paths?.includes(target) || record.image === target),
				);
				let parent = pid;
				const visited = new Set<number>();
				while (parents.has(parent) && !visited.has(parent)) {
					visited.add(parent);
					parent = parents.get(parent)!;
				}
				return start < 0 ? [] : [{ pid, start, depth: visited.size }];
			})
			.sort((a, b) => a.depth - b.depth);
		if (candidates[0]?.depth !== candidates[1]?.depth) root = candidates[0];
	}
	if (!root || !byPid.has(root.pid))
		return {
			complete: false,
			paths: [],
			taints: ["trace_incomplete"],
			tracedProcesses: 0,
			incompleteReasons: [root ? "execution_trace_missing" : "target_exec_not_found"],
		};
	const selected = new Map([[root.pid, root.start]]);
	const incomplete = new Set<string>(options.previewBytes === undefined ? [] : ["preview_only"]);
	const ordered: BinaryRecord[] = [];
	for (const [pid, start] of selected) {
		const lines = byPid.get(pid);
		if (!replaced.has(pid) && !lines?.some((line) => line.name === "terminated"))
			incomplete.add("process_exit_unproven:" + pid);
		for (const record of lines?.slice(start) ?? []) {
			if (record.child && !selected.has(record.child)) selected.set(record.child, 0);
			ordered.push(record);
		}
	}
	ordered.sort((a, b) => a.order - b.order);
	const taints = new Set<ProvenanceTaint>(["clock", "random"]);
	const diagnostics = new Set<string>();
	let truncated = false;
	const unsupported = (name: string) => {
		taints.add("unsupported_syscall");
		if (diagnostics.size < speculativeConfig().processReuse.diagnosticSyscalls)
			diagnostics.add(name.slice(0, speculativeConfig().processReuse.syscallNameCharacters));
		else if (!diagnostics.has(name)) truncated = true;
	};
	const roots = (options.guardFilesystemSemanticsWithin ?? []).map((name) => path.posix.resolve(name));
	const relative = (name: string) => {
		const root = roots.find((root) => containsLogicalPath(root, name));
		return root === undefined ? undefined : path.posix.relative(root, name);
	};
	const written = new Set<string>();
	const external = new Set<string>();
	const mapped = new Map<string, DependencyRole>();
	const failures = new Map<string, "ENOENT" | "ENOTDIR" | undefined>();
	const listed = new Set<string>();
	const changed = new Set<string>();
	const metadata = new Map<string, Extract<ObservedProcessPath, { role: "metadata" }>>();
	const locks = new Map<string, boolean>();
	const privateChannels = new Set<string>();
	const opened = new Map<string, string | undefined>();
	const output: { fd: 1 | 2; data: Buffer }[] = [];
	let outputBytes = 0;
	let lastOutputEnd = -Infinity;
	let streamMetadataObserved = false;
	const add = (name: string, role: DependencyRole, failure?: "ENOENT" | "ENOTDIR") => {
		if (mapped.get(name) !== "executable") mapped.set(name, role);
		failures.set(name, failures.has(name) && failures.get(name) !== failure ? undefined : failure);
	};
	const mutation = (name: string) => {
		const local = relative(name);
		if (local !== undefined) written.add(local);
		else external.add(name);
	};
	const controls = new Set([options.controlledSocket, options.dispatcherSocket].filter(Boolean));
	const streamFlags = new Map(options.outputEndpoints?.map((name, i) => [name, options.outputFlags?.[i]]));
	for (const record of ordered) {
		const { name, args, pid, result, anchors } = record;
		const ok = result !== undefined && result >= 0;
		if (record.unsupported) {
			if (!record.unsupported.name) incomplete.add("unresolved_syscall_reference");
			unsupported(record.unsupported.name ?? "unresolved_syscall_reference");
			continue;
		}
		if (record.pathErrors && (!record.virtual || record.layout & 0xf00))
			incomplete.add("unreadable_path:" + name);
		if (record.incomplete || (record.pending && !record.exec)) {
			incomplete.add(record.incomplete ?? "unfinished:" + name);
			continue;
		}
		if (result !== undefined && [-512, -513, -514, -516].includes(result)) continue;
		if (record.exec || name === "forked" || name === "terminated") continue;
		if (name === "image") {
			if (record !== byPid.get(root.pid)?.[root.start] || !options.launchPath) {
				for (const file of record.paths ?? []) add(file, "executable");
			}
			continue;
		}
		const fd = Number(BigInt.asIntN(32, args[0] ?? 0n));
		const file = anchors[0];
		// A broker can confirm an earlier open after a live reader has consumed the fd use.
		const writes = writesFile(record);
		const bound =
			writes && !isOpen(name) && !record.operands.some(Boolean) && opened.get(objectKey(file) ?? "");
		const paths = record.paths?.length || !bound ? record.paths : [bound];
		if (ok && (writes || (name === "descriptor_write" && paths?.length))) {
			if (!paths?.length && !endpoint(file)) {
				taints.add("untracked_fd");
				incomplete.add("unresolved_fd_write");
			}
			for (const target of paths ?? []) mutation(target);
		}
		const channel = endpoint(file) ?? (isStat(name) ? record.descriptorPath : undefined);
		const destination = channel ? (options.outputEndpoints?.indexOf(channel) ?? -1) : -1;
		if (name === "descriptor_read" || name === "descriptor_write") {
			if (channel && !privateChannels.has(channel) && destination < 0 && fd > 2) {
				taints.add((file!.mode & 0xf000) === 0xc000 ? "network" : "ipc");
			} else if (file && (file.mode & 0xf000) === 0x8000 && !opened.has(objectKey(file)!)) {
				taints.add("untracked_fd");
			}
			continue;
		}
		const socketQuery = ["getsockname", "getpeername", "getsockopt"].includes(name);
		if (
			(destination >= 0 || (!options.outputEndpoints && (fd === 1 || fd === 2))) &&
			(isStat(name) || ["ioctl", "fcntl", "lseek", "setsockopt", "fadvise64"].includes(name) || socketQuery)
		)
			streamMetadataObserved = true;
		if (isOpen(name) && ok) {
			const key = objectKey(record.file);
			if (key) opened.set(key, paths?.[0]);
		}
		if ((name === "pipe" || name === "pipe2" || name === "socketpair") && ok)
			for (const file of [record.file, record.second]) {
				const key = endpoint(file);
				if (key) privateChannels.add(key);
			}
		if (name === "connect" && record.addressFamily === 1 && record.socket?.startsWith("/")) {
			if (ok && controls.has(record.socket) && channel) {
				privateChannels.add(channel);
				continue;
			}
			if (result === -2 || result === -111 || result === -13) {
				add(record.socket, "input", result === -2 ? "ENOENT" : undefined);
				continue;
			}
		}
		if (
			networkCalls.has(name) &&
			!((name === "socket" || name === "socketpair") && args[0] === 1n) &&
			!(socketQuery && (destination >= 0 || (result === -88 && file))) &&
			!(
				channel &&
				privateChannels.has(channel) &&
				!["connect", "bind", "listen", "accept", "accept4", "sendmmsg"].includes(name) &&
				(name !== "sendto" || (args[4] === 0n && args[5] === 0n)) &&
				(name !== "sendmsg" || args[5] === 0n)
			)
		)
			taints.add("network");
		if (ipcCalls.has(name)) taints.add("ipc");
		if (["kill", "tkill", "tgkill", "pidfd_send_signal"].includes(name)) {
			taints.add("pid_observation");
			if (name === "pidfd_send_signal" || !selected.has(Number(BigInt.asIntN(32, args[0]!))))
				taints.add("ipc");
		}
		if ((name === "clone" || name === "clone3") && (args[0] ?? 0n) & 0x7e020080n)
			taints.add("confinement_observation");
		if (["getpid", "getppid", "gettid", "getpgrp", "getsid", "getpgid", "pidfd_open", "pidfd_getfd"].includes(name))
			taints.add("pid_observation");
		if (name === "wait4" && ok && result > 0) {
			taints.add("pid_observation");
			if (!selected.has(result)) taints.add("ipc");
		}
		if (
			name === "uname" &&
			ok &&
			(!options.platformFingerprint || record.utsFingerprint !== options.platformFingerprint)
		)
			unsupported(name);
		// The same live memory, load and CPU counters are exposed through procfs and sysfs.
		if (name === "sysinfo" || name === "getrusage" || name === "sched_getaffinity") taints.add("clock");
		if (name === "sched_getaffinity" && args[0] !== 0n) {
			taints.add("pid_observation");
			if (!selected.has(Number(args[0]))) taints.add("ipc");
		}
		if (
			name === "fadvise64" &&
			!opened.has(objectKey(file) ?? "") &&
			!(channel && (fd <= 2 || privateChannels.has(channel)))
		)
			taints.add("untracked_fd");
		if (record.capabilityTarget && record.capabilityTarget !== pid) taints.add("pid_observation");
		if (name === "io_uring_setup" && result === -1) continue;
		if (
			!(name === "capget" && record.lock === 0 && ok) &&
			(confinedCalls.has(name) ||
				result === -1 ||
				result === -13 ||
				(name === "prctl" && ![15n, 16n, 0x53564d41n].includes(args[0]!)) ||
				((name === "prlimit64" || name === "getrlimit") && args[name === "prlimit64" ? 1 : 0] === 4n && ok))
		)
			taints.add("confinement_observation");
		if (unmodeled.has(name) || fileIoctl(record)) unsupported(name);
		if (
			roots.length &&
			[-18, -95, -38].includes(result ?? 0) &&
			paths?.some((x) => relative(x) !== undefined)
		) {
			unsupported(name);
			incomplete.add("filesystem_semantics:" + name + ":" + pid);
		}
		if (
			options.captureOutput &&
			destination >= 0 &&
			ok &&
			result > 0 &&
			["write", "writev", "sendto", "sendmsg", "sendfile", "splice", "vmsplice"].includes(name)
		) {
			if (record.start < lastOutputEnd) incomplete.add("concurrent_output_order_unproven");
			lastOutputEnd = Math.max(lastOutputEnd, record.order);
			const chunks = record.output ?? [];
			const bytes = chunks.reduce((sum, b) => sum + b.length, 0);
			if (bytes !== result || (outputBytes += bytes) > speculativeConfig().processReuse.traceOutputBytes)
				incomplete.add("output_bytes_unproven");
			else
				output.push({
					fd: destination === 0 ? 1 : 2,
					data: chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks),
				});
		}
		const cmd = Number(args[1] ?? 0n);
		const lockCommand = name === "fcntl" && [5, 6, 7, 36, 37, 38].includes(cmd);
		if (lockCommand || name === "flock") {
			const query = cmd === 5 || cmd === 36;
			const release = name === "flock" ? !!(cmd & 8) : !query && record.lock === 2;
			if (!release && ((name === "flock" && cmd & 4) || (lockCommand && cmd !== 7 && cmd !== 38))) {
				const target = paths?.[0];
				if (ok && target && (!query || record.lock === 2))
					locks.set(target, !!locks.get(target) || (name === "flock" ? !!(cmd & 2) : record.lock !== 0));
				else taints.add("ipc");
			}
		}
		if (destination >= 0 && channel && ok && name === "ioctl" && cmd === 0x5421)
			streamFlags.set(channel, record.statusFlags);
		if (name === "fcntl" && !lockCommand && ![0, 1, 2, 1030].includes(cmd)) {
			const known =
				opened.has(objectKey(file) ?? "") ||
				fd <= 2 ||
				destination >= 0 ||
				(channel && privateChannels.has(channel));
			if (![3, 4].includes(cmd) || !known) unsupported(name);
			if (destination >= 0 && channel && ok) {
				const flags = streamFlags.get(channel);
				if (cmd === 3 && flags !== undefined && (flags & 0xc00) !== (result & 0xc00))
					incomplete.add("output_status_changed_outside_execution");
				if (cmd === 4) streamFlags.set(channel, record.statusFlags);
			}
		}
		if (name === "getdents" || name === "getdents64") {
			if (ok && paths?.length) {
				listed.add(paths[0]!);
				add(paths[0]!, "input");
				taints.add("descriptor_observation");
			} else if (ok) {
				unsupported(name);
				incomplete.add("unmodeled_metadata:" + name + ":" + pid);
			}
			continue;
		}
		if (options.fileEvidence?.(record)) continue;
		if (isStat(name) && ok) {
			if (
				record.nullDevice ||
				(!paths?.length && file && (file.mode & 0xf000) !== 0x8000 && (file.mode & 0xf000) !== 0x4000)
			) {
				taints.add("descriptor_observation");
				continue;
			}
			if (!record.stat || !paths?.length) {
				unsupported(name);
				incomplete.add("unparsed_metadata:" + name + ":" + pid);
				continue;
			}
			const followSymlinks = name !== "lstat" && !(Number(args[name === "statx" ? 2 : 3] ?? 0n) & 0x100);
			for (const path of paths) {
				const key = [path, followSymlinks, record.stat.fields?.join(",")].join("\0");
				const afterWrite = external.has(path) || written.has(relative(path) ?? "\0");
				const before = metadata.get(key);
				if (afterWrite && before) continue;
				if (before && before.digest !== record.stat.digest) changed.add(path);
				metadata.set(key, {
					path,
					role: "metadata",
					followSymlinks,
					...record.stat,
					...(afterWrite ? { afterWrite: true } : {}),
				});
			}
			continue;
		}
		if (record.operands.length && !paths) incomplete.add("unresolved_pathname:" + name + ":" + pid);
		const failure =
			paths?.length === 1 && !writesFile(record)
				? result === -2
					? "ENOENT"
					: result === -20
						? "ENOTDIR"
						: undefined
				: undefined;
		if (name === "fcntl" || name === "flock" || name === "statfs" || name === "fstatfs") continue;
		for (const file of paths ?? []) {
			add(file, isExec(name) ? "executable" : "input", failure);
			if (
				(result === -39 || result === -17) &&
				["rmdir", "unlinkat", "rename", "renameat", "renameat2"].includes(name)
			)
				listed.add(file);
		}
	}
	if (options.outputFlags)
		for (const [i, name] of (options.outputEndpoints ?? []).entries())
			if (((streamFlags.get(name) ?? 0) & 0xc00) !== (options.outputFlags[i]! & 0xc00))
				incomplete.add("output_status_changed_during_execution");
	for (const pid of selected.keys())
		for (const item of options.brokeredWrites?.(pid) ?? []) {
			if (path.posix.isAbsolute(item)) external.add(item);
			else written.add(item);
		}
	const touched = new Set(
		[
			...external,
			...[...written].flatMap((name) => roots.map((root) => path.posix.join(root, name))),
		].flatMap((name) => [name, path.posix.dirname(name)]),
	);
	const metadataChanges = [...changed].filter((name) => !touched.has(name)).sort();
	if (incomplete.size) taints.add("trace_incomplete");
	return {
		complete: !incomplete.size,
		paths: [
			...[...mapped].map(([name, role]) => ({
				path: name,
				role: sharedObjectRole(name, role),
				...(listed.has(name) ? { listed: true as const } : {}),
				...(failures.get(name) ? { lookupFailure: failures.get(name)! } : {}),
			})),
			...metadata.values(),
		].sort((a, b) => a.path.localeCompare(b.path)),
		taints: [...taints].sort(),
		pids: [...selected.keys()],
		tracedProcesses: selected.size,
		incompleteReasons: [...incomplete].sort(),
		streamMetadataObserved,
		...(options.captureOutput ? { output } : {}),
		...(diagnostics.size ? { unsupportedSyscalls: [...diagnostics].sort() } : {}),
		...(truncated ? { unsupportedSyscallsTruncated: true } : {}),
		...(metadataChanges.length ? { metadataChanges } : {}),
		written: [...written].sort(),
		external: [...external].sort(),
		locks: [...locks].map(([path, exclusive]) => ({ path, exclusive })),
	};
}

/** Whether writes reached `targets` within [since, until], or held one open to write by `until`. An unresolvable write counts. */
export function writesWithin(
	writes: readonly TracedWrite[],
	targets: ReadonlySet<string>,
	since: number,
	until: number,
): boolean {
	return writes.some(
		({ paths, at, opened }) =>
			at <= until && (opened || at >= since) && (!paths || paths.some((target) => targets.has(target))),
	);
}
function sharedObjectRole(observedPath: string, role: DependencyRole): DependencyRole {
	return role === "input" && /(?:^|\/)lib[^/]*\.so(?:\.|$)/.test(observedPath) ? "shared_object" : role;
}
