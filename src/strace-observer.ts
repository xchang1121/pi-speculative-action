import { type FileHandle, open } from "node:fs/promises";
import path from "node:path";
import { getSystemErrorName } from "node:util";
import { speculativeConfig } from "./config-schema.ts";
import {
	FILESYSTEM_OBSERVATION_FIELDS,
	type FilesystemObservationField,
	filesystemObservationDigest,
} from "./filesystem-evidence.ts";
import { type Sha256Digest, containsLogicalPath, isSafeCount, stableEqual } from "./primitives.ts";
import { type DependencyRole, type ProvenanceTaint } from "./provenance-certificate.ts";
const CONFINEMENT_SENSITIVE_SYSCALLS = new Set([
	"seccomp",
	"capget",
	"capset",
	"mount",
	"umount2",
	"pivot_root",
	"swapon",
	"swapoff",
	"reboot",
	"sethostname",
	"setdomainname",
	"kexec_load",
	"init_module",
	"finit_module",
	"delete_module",
	"unshare",
	"setns",
	"perf_event_open",
	"bpf",
	"userfaultfd",
	"keyctl",
	"add_key",
	"request_key",
	"ptrace",
	"process_vm_readv",
	"process_vm_writev",
	"open_by_handle_at",
	"name_to_handle_at",
	"quotactl",
	"acct",
	"lookup_dcookie",
	"io_setup",
	"io_submit",
	"io_uring_setup",
	"io_uring_enter",
	"io_uring_register",
	"personality",
]);
const SYSCALL_FILTER = `trace=%file,%process,%network,%ipc,%desc,%signal,clock_gettime,gettimeofday,time,getrandom,sysinfo,times,getrusage,getrlimit,setrlimit,prlimit64,prctl,mmap,${[...CONFINEMENT_SENSITIVE_SYSCALLS].join(",")}`;
const STREAM_ENDPOINT = /^(\d+)<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->(\d+))?(?:,"(?:\\.|[^"\\])*")?\]>$/;
/** One production trace shape shared by execution and dependency-ablation paths. */
export function straceCommand(
	strace: string,
	tracePrefix: string,
	command: readonly string[],
	_streams = false,
	_continuation = false,
	kernelMetadata = false,
): readonly [string, ...string[]] {
	// Its sandbox launcher's threads would pay a ptrace stop per call they serve; a file's bytes are captured from the filesystem, a stream's only here.
	return [
		strace,
		"--kill-on-exit",
		"--trace-children-only",
		"--elide-path-data",
		...(kernelMetadata ? ["--seccomp-bpf"] : []),
		"-f",
		"-q",
		"-ttt",
		"-yy",
		"-v",
		"-s",
		String(speculativeConfig().processReuse.traceOutputBytes + 1),
		"-e",
		SYSCALL_FILTER,
		"-o",
		`${tracePrefix}.stream`,
		...command,
	];
}
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
export interface StraceObservation {
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
export interface StraceObservationOptions {
	/** Authenticated host subcomputation channel; its input evidence is imported by the caller. */
	readonly controlledSocket?: string;
	readonly metadata?: ReturnType<typeof nativeMetadataTrace>;
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
interface TraceFile {
	readonly pid: number;
	readonly lines: readonly TraceRecord[];
}
type TraceRoot = TraceFile & {
	readonly start: number;
};
interface TraceProcess extends TraceRoot {
	readonly cwd: string | undefined;
	readonly fs: {
		shared: boolean;
		changed: boolean;
	};
}
interface TraceLine {
	readonly native?: {
		readonly entered: number;
		readonly paths: readonly string[] | undefined;
		readonly observation: StatObservation | undefined;
		readonly descriptor: boolean;
		readonly nullDevice: boolean;
	};
	readonly order?: number;
	readonly name: string;
	readonly args: readonly string[];
	readonly result: string;
	readonly failure?: string;
}
const pathObservations = new WeakMap<
	TraceLine,
	{
		readonly cwd: string;
		readonly paths: readonly string[] | undefined;
	}
>();
const TRACE_DELIMITERS: Readonly<Record<string, string>> = {
	"(": ")",
	"[": "]",
	"{": "}",
	"<": ">",
};
const QUOTED_RUN = /(?:[^"\\]|\\.)+/sy;
const ANNOTATION_RUN = /(?:[^<>\\]|\\.)+/sy;
const PLAIN_RUN = /[^"\\()[\]{}<>,/]+/y;
/** Delimit once: quoted paths and descriptor annotations are data, never syscall syntax; what means nothing where it stands is passed whole. */
function parseTraceLine(line: string): TraceLine {
	if (
		/^\+\+\+ (?:exited with \d+|killed by SIG[A-Z0-9]+(?: \(core dumped\))?) \+\+\+\s*$|^pi_domain_exit\(\d+\) = 0$/.test(
			line,
		)
	)
		return { name: "terminated", args: [], result: "0" };
	const head = /^\s*([a-zA-Z0-9_]+)\(/.exec(line);
	const empty = { name: head?.[1] ?? "", args: [], result: "" };
	if (!head && (!line.trim() || /^(?:\+\+\+|---)/.test(line))) return empty;
	const failure = { ...empty, failure: `syscall_unparsed:${empty.name}` };
	if (!head) return failure;
	const args: string[] = [];
	const stack: string[] = [];
	let start = head[0].length;
	for (let index = start; index < line.length; index++) {
		const character = line[index]!;
		const context = stack.at(-1);
		const quoted = context === '"';
		if (character === "\\" && (quoted || context?.endsWith(">"))) {
			index++;
			continue;
		}
		const run = quoted ? QUOTED_RUN : context?.endsWith(">") ? ANNOTATION_RUN : PLAIN_RUN;
		run.lastIndex = index;
		if (run.test(line)) index = run.lastIndex - 1;
		if (quoted) {
			if (character === '"') stack.pop();
			continue;
		}
		// -yy sockets use -> for peers; filesystem paths escape literal angle brackets.
		if (context?.endsWith(">")) {
			if (character === "<") stack.push(">");
			if (character === ">" && (context === "/>" || line[index - 1] !== "-")) stack.pop();
			continue;
		}
		if (character === '"') {
			stack.push('"');
			continue;
		}
		if (character === "/" && line[index + 1] === "*") {
			const end = line.indexOf("*/", index + 2);
			if (end < 0) return failure;
			index = end + 1;
			continue;
		}
		const closing = TRACE_DELIMITERS[character];
		if (closing) {
			stack.push(character === "<" && line[index + 1] === "/" ? "/>" : closing);
			continue;
		}
		if (character === ")" && !stack.length) {
			const result = /^\s+=\s+(.*)$/.exec(line.slice(index + 1))?.[1];
			if (!result) return failure;
			if (index > start || args.length) args.push(line.slice(start, index).trim());
			return { name: head[1]!, args, result };
		}
		if (")]}".includes(character)) {
			if (stack.pop() !== character) return failure;
		} else if (character === "," && !stack.length) {
			args.push(line.slice(start, index).trim());
			start = index + 1;
		}
	}
	return failure;
}
type TraceRecord = TraceLine & {
	readonly pid: number;
	readonly at: number;
	readonly text: string;
};
/** The kernel owns each invocation. Views retain this record while its broker acknowledgement arrives. */
export function nativeMetadataTrace(read: () => Buffer, privateUpper: string) {
	type Call = {
		readonly line: {
			-readonly [Key in keyof TraceRecord]: TraceRecord[Key];
		};
		readonly number: number;
		readonly arguments: readonly bigint[];
		returned?: Buffer;
		response?: Buffer;
		requested: boolean;
		failed: boolean;
		error?: string;
		ended?: number;
		seen: number;
		path?: string;
		pathRank: number;
		observation?: StatObservation;
	};
	const calls = new Map<bigint, Call>();
	const files = new Map<number, TraceRecord[]>();
	const inputs = new Map<
		string,
		{
			readonly image?: string;
			readonly sealed: boolean;
			readonly links: readonly (readonly [string, string])[];
		}
	>();
	const inputFrames = new Map<
		bigint,
		{
			readonly chunks: Buffer[];
			bytes: number;
		}
	>();
	const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
	let failure: Error | undefined;
	return {
		files,
		get inputs(): ReadonlyMap<string, ReturnType<typeof inputs.get>> {
			if (failure) throw failure;
			if (inputFrames.size) throw new Error("native input record is incomplete");
			return inputs;
		},
		read() {
			if (failure) throw failure;
			try {
				const bytes = read();
				const changed = new Map<bigint, Call>();
				for (let offset = 0; offset < bytes.length;) {
					if (bytes.length - offset < 48) throw new Error("truncated native record");
					const task = bytes.readBigUInt64LE(offset + 8);
					const at = Number(bytes.readBigUInt64LE(offset + 16)) / 1e6;
					const id = bytes.readBigUInt64LE(offset + 24);
					const kind = Number(bytes.readBigUInt64LE(offset + 32));
					const size = bytes.readUInt32LE(offset + 40);
					const error = bytes.readInt32LE(offset + 44);
					if (
						!id ||
						size > speculativeConfig().nativeBuild.domainEventBytes ||
						offset + 48 + size > bytes.length
					)
						throw new Error("invalid native record");
					const body = bytes.subarray(offset + 48, offset + 48 + size);
					offset += (48 + size + 7) & ~7;
					if (kind === 12) {
						if (error || size !== 16) throw new Error("invalid native process record");
						const name = (
							{
								39: "getpid",
								110: "getppid",
								121: "getpgid",
								124: "getsid",
							} as Record<number, string>
						)[Number(body.readBigUInt64LE(0))];
						if (!name) throw new Error("unknown native process observation");
						const pid = Number(task & 0xffffffffn);
						appendGrouped(
							files,
							pid,
							Object.freeze({
								pid,
								at: Date.now(),
								text: "",
								order: at,
								name,
								args: [],
								result: String(body.readBigInt64LE(8)),
								native: {
									entered: at,
									paths: [],
									observation: undefined,
									descriptor: false,
									nullDevice: false,
								},
							}),
						);
						continue;
					}
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
						const row = JSON.parse(text.decode(Buffer.concat(frame.chunks))) as {
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
					let call = calls.get(id);
					if (kind === 8) {
						if (call || error || size !== 56) throw new Error("invalid native entry");
						const number = Number(body.readBigUInt64LE(8));
						const name = (
							{
								4: "stat",
								5: "fstat",
								6: "lstat",
								17: "pread64",
								18: "pwrite64",
								262: "newfstatat",
								332: "statx",
							} as Record<number, string>
						)[number];
						if (!name) throw new Error("unknown native metadata call");
						const args = Array.from({ length: 5 }, (_, index) => body.readBigUInt64LE(16 + index * 8));
						const pid = Number(task & 0xffffffffn);
						const descriptor =
							number === 5 ||
							number === 17 ||
							number === 18 ||
							!!((number === 262 || number === 332) && args[number === 262 ? 3 : 2]! & 0x1000n);
						const state: Call = {
							number,
							arguments: args,
							requested: false,
							failed: false,
							pathRank: 0,
							seen: Date.now(),
							line: {
								pid,
								at: Date.now(),
								text: "",
								order: at,
								name,
								args: [],
								result: "",
								failure: "native_metadata_pending",
								native: {
									entered: at,
									paths: undefined,
									observation: undefined,
									descriptor,
									nullDevice: false,
								},
							},
						};
						calls.set(id, state);
						changed.set(id, state);
						appendGrouped(files, pid, state.line);
						continue;
					}
					if (!call) throw new Error("native record has no kernel entry");
					call.seen = Date.now();
					changed.set(id, call);
					if (error) {
						call.error = "native_metadata_unreadable";
						continue;
					}
					if (kind === 0) {
						if (
							call.requested ||
							size !== 80 ||
							body.readUInt32LE(8) !== call.line.pid ||
							body.readInt32LE(16) !== call.number
						)
							throw new Error("native request changed");
						call.requested = true;
					} else if (kind === 4) {
						if (call.response || size !== 24) throw new Error("invalid native response");
						call.response = body;
					} else if (kind === 5) call.failed = true;
					else if (kind === 9) {
						call.line.name = "";
						call.ended = at;
					} else if (kind === 6) {
						if (
							call.returned ||
							size < 32 ||
							body.readBigInt64LE(0) !== BigInt(call.number) ||
							body.readUInt32LE(24) !== size - 32 ||
							size - 32 !==
								(body.readBigInt64LE(8) < 0n || call.number === 17 || call.number === 18
									? 0
									: call.number === 332
										? 256
										: 144)
						)
							throw new Error("invalid native return");
						call.returned = body;
						call.ended = at;
					} else if (kind === 1 || kind === 7) {
						let value = text.decode(body).replace(/\0$/, "");
						if (value.includes("\0")) throw new Error("invalid native pathname");
						if (value.startsWith(`${privateUpper}/`)) value = value.slice(privateUpper.length);
						const rank = kind === 1 ? 3 : Number(task & 0xffffffffn) !== call.line.pid ? 2 : 1;
						if (rank === call.pathRank && value !== call.path) call.error = "native_metadata_path_changed";
						if (rank > call.pathRank) {
							call.path = value;
							call.pathRank = rank;
						}
					} else throw new Error("unknown native metadata record");
				}
				// Complete the original record once; its many consumers share these parsed values.
				for (const [id, state] of changed) {
					const { line, number, arguments: args, returned, response } = state;
					const result = returned?.readBigInt64LE(8);
					const flags = number === 262 ? 3 : 2;
					const positional = number === 17 || number === 18;
					line.at = state.seen;
					line.order = state.ended ?? line.native!.entered;
					if (result !== undefined && [-512n, -513n, -514n, -516n].includes(result)) line.name = "";
					line.result =
						result === undefined
							? ""
							: result >= 0n
								? String(result)
								: `-1 ${getSystemErrorName(Number(result))}`;

					line.failure = undefined;
					if (line.name) {
						if (state.error) line.failure = state.error;
						else if (positional) {
							if (!returned) line.failure = "native_metadata_pending";
							else if (result! >= 0n && !state.path) line.failure = "native_io_path_missing";
						} else if (!returned || !state.requested || !response) {
							line.failure = state.failed ? "native_metadata_rejected" : "native_metadata_pending";
						} else {
							const error = response.readInt32LE(16);
							const flags = response.readUInt32LE(20);
							const expected = error ? BigInt(error) : response.readBigInt64LE(8);
							if (flags > 1 || error > 0 || (!(flags & 1) && result !== expected))
								line.failure = "native_metadata_response_changed";
						}
					}

					const arguments_ = args.map(String);
					const data = returned?.subarray(32);
					if (positional) arguments_[3] = String(BigInt.asIntN(64, args[3]!));
					if (number === 262 || number === 332)
						arguments_[flags] = args[flags]! & 0x100n ? "AT_SYMLINK_NOFOLLOW" : "0";
					if (line.native!.descriptor) arguments_[0] = `${args[0]}<${state.path ?? "anonymous"}>`;
					line.args = arguments_;
					line.native = {
						...line.native!,
						paths:
							state.path?.startsWith("/") &&
							!state.path.startsWith("/memfd:") &&
							!state.path.endsWith(" (deleted)")
								? [walkedPath(state.path)]
								: state.path || (positional && result !== undefined && result < 0n)
									? []
									: undefined,
						observation:
							!positional && !line.failure && result === 0n
								? (state.observation ??= nativeStatObservation(data!, number === 332))
								: undefined,
						nullDevice:
							!!data?.length &&
							(number === 332 ? data.readUInt16LE(28) & 0xf000 : data.readUInt32LE(24) & 0xf000) === 0x2000 &&
							(number === 332
								? data.readUInt32LE(128) === 1 && data.readUInt32LE(132) === 3
								: data.readBigUInt64LE(40) === 259n),
					};
					if (returned && (positional || (state.requested && response))) {
						Object.freeze(line);
						calls.delete(id);
					}
				}
			} catch (error) {
				throw (failure =
					error instanceof Error ? error : new Error("native execution record failed", { cause: error }));
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
/** Decode the verbose stat ABI record emitted when an Actor runs without the sandbox metadata broker. */
function textStatObservation(record: string | undefined): StatObservation | undefined {
	if (!record?.startsWith("{st_dev=")) return;
	const integer = (field: string) => {
		const match = new RegExp(`(?:^|[,{]\\s*)st_${field}=(-?\\d+)(?=[ ,}])`).exec(record);
		if (!match) throw new Error("incomplete_stat_record");
		return BigInt(match[1]!);
	};
	const device = (field: string) => {
		const match = new RegExp(`st_${field}=makedev\\((0x[\\da-f]+|\\d+), (0x[\\da-f]+|\\d+)\\)`).exec(record);
		if (!match) throw new Error("incomplete_device_record");
		return linuxDevice(BigInt(match[1]!), BigInt(match[2]!));
	};
	try {
		const mode = /st_mode=(S_IFREG|S_IFDIR|S_IFLNK|S_IFIFO|S_IFSOCK)\|([0-7]+)/.exec(record);
		if (!mode) return;
		const kinds: Record<string, bigint> = {
			S_IFREG: 0o100000n,
			S_IFDIR: 0o040000n,
			S_IFLNK: 0o120000n,
			S_IFIFO: 0o010000n,
			S_IFSOCK: 0o140000n,
		};
		const values = {
			dev: device("dev"),
			ino: integer("ino"),
			mode: kinds[mode[1]!]! | BigInt("0o" + mode[2]),
			nlink: integer("nlink"),
			uid: integer("uid"),
			gid: integer("gid"),
			rdev: 0n,
			size: integer("size"),
			blksize: integer("blksize"),
			blocks: integer("blocks"),
			atimeNs: integer("atime") * 1000000000n + integer("atime_nsec"),
			mtimeNs: integer("mtime") * 1000000000n + integer("mtime_nsec"),
			ctimeNs: integer("ctime") * 1000000000n + integer("ctime_nsec"),
		};
		return { digest: filesystemObservationDigest(values) };
	} catch {
		return;
	}
}
function appendGrouped<Key, Value>(groups: Map<Key, Value[]>, key: Key, value: Value): void {
	(groups.get(key) ?? groups.set(key, []).get(key)!).push(value);
}
/** One decoder for complete transcripts and growing tails; unfinished calls retain their entry time across reads. */
function traceDecoder() {
	const pending = new Map<
		string,
		{
			text: string;
			pid: number;
			at: number;
			order?: number;
		}[]
	>();
	const times = new Map<number, number>();
	let sequence = 0;
	return Object.assign(
		(contents: string) => {
			const rows = contents.split(/\r?\n/);
			const complete = new Map<number, TraceRecord[]>();
			for (const [index, raw] of rows.entries()) {
				const order = sequence++;
				if (!raw.trim()) continue;
				const process = /^(\d+)\s+(.*)$/.exec(raw);
				if (!process) throw new Error("invalid ordered trace record");
				const stamp = /^(\d+\.\d+)(?:@(\d+\.\d+))? (.*)$/.exec(process[2]!);
				const text = stamp?.[3] ?? process[2]!;
				const current = {
					pid: Number(process[1]),
					at: stamp ? Number(stamp[1]) * 1000 : NaN,
					order: stamp?.[2] ? Number(stamp[2]) * 1000 : order,
				};
				const fragment = index === rows.length - 1;
				current.at = Math.max(times.get(current.pid) ?? 0, current.at);
				times.set(current.pid, current.at);
				const unfinished = /^\s*([a-zA-Z0-9_]+)\(.*\s<unfinished \.\.\.>\s*$/.exec(text);
				const resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>(.*)$/.exec(text);
				const name = unfinished?.[1] ?? resumed?.[1];
				const key = `${current.pid}:${name}`;
				if (unfinished && !fragment) {
					appendGrouped(pending, key, {
						...current,
						text: text.replace(/\s*<unfinished \.\.\.>\s*$/, ""),
					});
					continue;
				}
				const prefix = !fragment && resumed && pending.get(key)?.shift();
				if (pending.get(key)?.length === 0) pending.delete(key);
				const line = resumed
					? prefix
						? parseTraceLine(`${prefix.text}${resumed[2]}`)
						: {
								name: name!,
								args: [],
								result: "",
								failure: `resumed_without_unfinished:${key}`,
							}
					: parseTraceLine(text);
				// Restart codes stay inside the kernel; only the eventual return is visible to the program.
				if (/^\? ERESTART(?:SYS|NOINTR|NOHAND|_RESTARTBLOCK)\b/.test(line.result)) continue;
				appendGrouped(complete, current.pid, {
					...current,
					...line,
					...(fragment ? { failure: "partial" } : {}),
					text,
					...(prefix
						? {
								at: prefix.at,
								order: /^(?:write|writev|sendto|sendmsg|fork|vfork|clone|clone3)$/.test(name!)
									? prefix.order
									: current.order,
							}
						: {}),
				});
			}
			return complete;
		},
		{
			pending: (): TraceRecord[] =>
				[...pending].flatMap(([key, calls]) =>
					calls.map((call) => ({
						...call,
						name: key.slice(key.indexOf(":") + 1),
						args: [],
						result: "",
						failure: `unfinished:${key}`,
					})),
				),
		},
	);
}
/** The vector and control envelope must be decoded completely; truncated/unknown
 * control messages retain their ordinary unsupported-syscall outcome. */
function streamMessage(text: string | undefined) {
	const match =
		/^\{msg_name=NULL, msg_namelen=0, msg_iov=(\[.*\]), msg_iovlen=(\d+), (?:msg_control=(.*), )?msg_controllen=(\d+), msg_flags=((?:0|MSG_CMSG_CLOEXEC|MSG_TRUNC)(?:\|MSG_CMSG_CLOEXEC|\|MSG_TRUNC)*)\}$/.exec(
			text ?? "",
		);
	if (!match) return;
	const captured = queueVectors(match[1], Number(match[2]));
	if (!captured) return;
	const vector = { ...captured, truncated: /MSG_TRUNC/.test(match[5]!) };
	if ((match[3] === undefined || match[3] === "NULL") && match[4] === "0")
		return { ...vector, rights: [] as number[] };
	const control =
		/^\[\{cmsg_len=(\d+), cmsg_level=SOL_SOCKET, cmsg_type=SCM_RIGHTS, cmsg_data=\[(.*)\]\}\]$/.exec(
			match[3]!,
		);
	if (!control) return;
	const fds = control[2]!.split(", ");
	if (
		fds.length > speculativeConfig().processReuse.maxInheritedDescriptors ||
		!fds.every((fd) => /^\d+<.*>$/.test(fd)) ||
		Number(control[1]) !== 16 + 4 * fds.length ||
		Number(match[4]) < Number(control[1]) ||
		Number(match[4]) > 16 + 8 * Math.ceil(fds.length / 2)
	)
		return;
	return { ...vector, rights: fds.map((fd) => Number.parseInt(fd, 10)) };
}
const FILE_FLAGS: Readonly<Record<string, number>> = {
	"0": 0,
	O_RDONLY: 0,
	O_WRONLY: 1,
	O_RDWR: 2,
	O_APPEND: 0x400,
	O_NONBLOCK: 0x800,
	O_NDELAY: 0x800,
	O_LARGEFILE: 0x8000,
};
function symbolicMask(text: string | undefined, flags: Readonly<Record<string, number>>): number | undefined {
	return text
		?.split("|")
		.reduce<number | undefined>(
			(mask, flag) => (mask === undefined || flags[flag] === undefined ? undefined : mask | flags[flag]),
			0,
		);
}
function selectTraceRoot(
	files: readonly TraceFile[],
	target: string,
	initialCwd: string,
):
	| TraceRoot
	| {
			readonly reason: string;
	  } {
	const parent = new Map<number, number>();
	const children = new Map<number, number[]>();
	for (const file of files) {
		for (const line of file.lines) {
			const child = spawnedPID(line);
			if (!child) continue;
			const existing = parent.get(child);
			if (existing !== undefined && existing !== file.pid)
				return { reason: `process_parent_ambiguous:${child}` };
			parent.set(child, file.pid);
			appendGrouped(children, file.pid, child);
		}
	}
	const roots = files.filter((file) => !parent.has(file.pid));
	if (roots.length !== 1)
		return {
			reason: `trace_root_ambiguous:${roots
				.map(({ pid }) => pid)
				.sort()
				.join(",")}`,
		};
	const depth = new Map<number, number>([[roots[0]!.pid, 0]]);
	for (const [pid, level] of depth)
		for (const child of children.get(pid) ?? []) if (!depth.has(child)) depth.set(child, level + 1);
	const candidates: Array<
		TraceRoot & {
			readonly depth: number;
		}
	> = [];
	for (const file of files) {
		const processDepth = depth.get(file.pid);
		if (processDepth === undefined) continue;
		const start = file.lines.findIndex(
			(line) =>
				successfulExec(line) &&
				syscallPaths(line, initialCwd)?.some((name) => path.posix.resolve(name) === target),
		);
		if (start >= 0) candidates.push({ ...file, start, depth: processDepth });
	}
	if (!candidates.length) return { reason: "target_exec_not_found" };
	const shallowest = Math.min(...candidates.map((candidate) => candidate.depth));
	const matches = candidates.filter((candidate) => candidate.depth === shallowest);
	if (matches.length !== 1)
		return {
			reason: `target_exec_ambiguous:${matches
				.map((file) => file.pid)
				.sort()
				.join(",")}`,
		};
	const match = matches[0]!;
	// exec does not unshare CLONE_FS. An excluded task must not retain authority over the target cwd.
	if (
		files.some((file) =>
			file.lines.some(
				(line, index) =>
					spawnedPID(line) &&
					sharesFilesystem(line) !== false &&
					(spawnedPID(line) === match.pid || (file.pid === match.pid && index < match.start)),
			),
		)
	) {
		return { reason: "target_filesystem_context_unproven" };
	}
	return match;
}
/**
 * Decode the execution's ordered transcript. The parser deliberately fails closed: only the target
 * exec and its recursively identified descendants contribute a replayable certificate.
 */
export async function observeStrace(
	tracePrefix: string | TraceTail,
	executablePath: string,
	initialCwd: string,
	options: StraceObservationOptions = {},
): Promise<StraceObservation> {
	await using owned =
		typeof tracePrefix === "string" ? traceTail(tracePrefix, initialCwd, options) : undefined;
	const source = owned ?? (tracePrefix as TraceTail);
	const { records: files } = await source.read();
	const target = path.posix.resolve(initialCwd, executablePath);
	const file = options.execution && files.find((file) => file.pid === options.execution!.pid);
	const root = options.execution
		? file
			? { ...file, start: options.execution.start }
			: { reason: "execution_trace_missing" }
		: selectTraceRoot(files, options.launchPath ?? target, initialCwd);
	if ("reason" in root)
		return {
			complete: false,
			paths: [],
			taints: ["trace_incomplete"],
			tracedProcesses: 0,
			incompleteReasons: [root.reason],
		};
	const byPID = new Map(files.map((file) => [file.pid, file]));
	const selected = new Map<number, TraceProcess>([
		[
			root.pid,
			{
				...root,
				cwd: path.posix.resolve(initialCwd),
				fs: { shared: false, changed: false },
			},
		],
	]);
	let streamMetadataObserved = false;
	const incomplete = new Set<string>(options.previewBytes === undefined ? [] : ["preview_only"]);
	for (const [pid, process] of selected) {
		if (process.lines.at(-1)?.name !== "terminated") incomplete.add(`process_exit_unproven:${pid}`);
		let cwd = process.cwd;
		for (const line of process.lines.slice(process.start)) {
			if (process.fs.shared && (line.name === "chdir" || line.name === "fchdir") && syscallSucceeded(line))
				process.fs.changed = true;
			cwd = tracedCwd(line, cwd);
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) {
				incomplete.add(`child_trace_missing:${child}`);
				continue;
			}
			const shared = sharesFilesystem(line);
			if (shared === undefined) incomplete.add(`clone_flags_unparsed:${pid}`);
			if (shared !== false) process.fs.shared = true;
			selected.set(child, {
				...childFile,
				start: 0,
				cwd,
				fs: shared === false ? { shared: false, changed: false } : process.fs,
			});
		}
	}
	const paths = new Map<string, DependencyRole>();
	const listedPaths = new Set<string>();
	const metadata = new Map<
		string,
		Extract<
			ObservedProcessPath,
			{
				role: "metadata";
			}
		>
	>();
	const lookupFailures = new Map<string, "ENOENT" | "ENOTDIR" | undefined>();
	const observePath = (target: string, role: DependencyRole, failure?: "ENOENT" | "ENOTDIR") => {
		if (paths.get(target) !== "executable") paths.set(target, role);
		lookupFailures.set(
			target,
			lookupFailures.has(target) && lookupFailures.get(target) !== failure ? undefined : failure,
		);
	};
	// A transcript cannot prove untraced clock/random inputs repeatable; it permits only a one-shot transfer.
	const taints = new Set<ProvenanceTaint>(["clock", "random"]);
	const unsupportedSyscalls = new Set<string>();
	let unsupportedSyscallsTruncated = false;
	const unsupported = (syscall: string) => {
		taints.add("unsupported_syscall");
		const name = syscall.slice(0, speculativeConfig().processReuse.syscallNameCharacters);
		if (unsupportedSyscalls.size < speculativeConfig().processReuse.diagnosticSyscalls)
			unsupportedSyscalls.add(name);
		else if (!unsupportedSyscalls.has(name)) unsupportedSyscallsTruncated = true;
	};
	const opened = new Map<number, Set<number>>(); // Descriptors a process opened itself, whose status flags it chose.
	const streamAliases = new Map<number, Set<number>>([[root.pid, new Set([1, 2])]]);
	const capturedOutput: Array<{
		fd: 1 | 2;
		data: Buffer;
		order: number;
	}> = [];
	let outputBytes = 0;
	const ownPipes = new Set<string>(); // Pipes and socket pairs the traced processes created, by inode.
	// The semantic roots are one workspace seen from the sandbox and from its source: name a file by its place in it.
	const locks = new Map<string, boolean>();
	const changedMetadata = new Set<string>();
	const written = new Set<string>();
	const writable = new Set<string>();
	const external = new Set<string>();
	const workspaceName = (target: string) =>
		semanticRoots.flatMap((root) =>
			containsLogicalPath(root, target) ? [`//${path.posix.relative(root, target)}`] : [],
		)[0];
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) =>
		path.posix.resolve(value),
	);
	const brokered = brokeredProcessSegments(selected, options);
	const outputPipes = new Set<string>();
	if (options.outputFlags)
		for (const [index, endpoint] of options.outputEndpoints?.entries() ?? []) {
			if (!endpoint.startsWith("pipe:")) continue;
			const initial = options.outputFlags[index]!;
			const mask = 0xc00;
			let flags: number | undefined = initial;
			for (const line of [...selected.values()]
				.flatMap((process) => process.lines.slice(process.start))
				.filter((line) => /^fcntl(?:64)?$/.test(line.name) && (line.args[0] ?? "").endsWith(`<${endpoint}>`))
				.sort((a, b) => a.order! - b.order!)) {
				if (line.args[1] === "F_SETFL" && syscallSucceeded(line))
					flags = symbolicMask(line.args[2], FILE_FLAGS);
				if (
					line.args[1] === "F_GETFL" &&
					syscallSucceeded(line) &&
					(flags === undefined || (Number.parseInt(line.result) & mask) !== (flags & mask))
				) {
					incomplete.add("output_status_changed_outside_execution");
				}
			}
			if (flags !== undefined && (flags & mask) === (initial & mask)) outputPipes.add(endpoint);
			else incomplete.add("output_status_changed_during_execution");
		}
	const observeMetadata = (
		observedPath: string,
		followSymlinks: boolean,
		{ digest, fields }: StatObservation,
	) => {
		lookupFailures.set(observedPath, undefined);
		const identity = `metadata:${followSymlinks}:${fields?.join(",") ?? ""}:${observedPath}`;
		const afterWrite = external.has(observedPath) || written.has(workspaceName(observedPath) ?? "");
		if (afterWrite && metadata.has(identity)) return;
		const previous = metadata.get(identity);
		if (previous && previous.digest !== digest) changedMetadata.add(observedPath);
		metadata.set(identity, {
			path: observedPath,
			role: "metadata",
			followSymlinks,
			digest,
			...(fields ? { fields } : {}),
			...(afterWrite ? { afterWrite: true } : {}),
		});
	};
	const cwds = new Map([...selected].map(([pid, process]) => [pid, process.cwd]));
	// One domain order preserves a child's read before a later parent write and FD inheritance at the actual fork.
	const ordered = [...selected]
		.flatMap(([pid, process]) => {
			if (process.fs.changed) {
				incomplete.add("shared_cwd_mutation");
				return [];
			}
			return process.lines
				.slice(process.start, brokered.get(pid))
				.map((line, index) => ({ line, initial: index === 0 }));
		})
		.sort((left, right) => left.line.order! - right.line.order!);
	for (const { line, initial } of ordered) {
		const { pid } = line;
		if (pid === root.pid && initial && options.launchPath && !options.execution) continue;
		let cwd = cwds.get(pid);
		if (!cwd) {
			incomplete.add(`cwd_unknown:${pid}`);
			cwd = path.posix.resolve(initialCwd);
		}
		const spawned = spawnedPID(line);
		if (spawned) {
			opened.set(spawned, new Set(opened.get(pid)));
			streamAliases.set(spawned, new Set(streamAliases.get(pid)));
		}
		const aliases = streamAliases.get(pid) ?? new Set<number>();
		streamAliases.set(pid, aliases);
		const own = opened.get(pid) ?? opened.set(pid, new Set()).get(pid)!;
		const result = Number.parseInt(line.result ?? "", 10);
		// Files and pipes the traced processes created stay private to them, through duplication and inheritance.
		if (/^(?:open|openat|openat2|creat)$/.test(line.name) && isSafeCount(result)) {
			own.add(result);
			if (writesPath(line)) writable.add(absoluteDescriptorPath(line.result) ?? "");
		} else if (/^pipe2?$/.test(line.name) && result === 0)
			for (const pipe of (line.args[0] ?? "").matchAll(/<pipe:\[(\d+)\]>/g)) ownPipes.add(pipe[1]!);
		else if (line.name === "socketpair" && result === 0)
			for (const end of (line.args[3] ?? "").matchAll(/<UNIX(?:-[A-Z]+)?:\[(\d+)/g)) ownPipes.add(end[1]!);
		else if (
			/^dup[23]?$|^fcntl(?:64)?$/.test(line.name) &&
			(line.name.startsWith("dup") || /^F_DUPFD/.test(line.args[1] ?? "")) &&
			isSafeCount(result)
		) {
			if (own.has(Number.parseInt(line.args[0] ?? "", 10))) own.add(result);
			else own.delete(result);
		} else if (line.name === "close") own.delete(Number.parseInt(line.args[0] ?? "", 10));
		else if (line.name === "close_range" && result === 0 && !line.args[2]?.includes("CLOSE_RANGE_CLOEXEC"))
			for (const fd of own)
				if (
					fd >= Number(line.args[0]) &&
					fd <= (/^~0/.test(line.args[1] ?? "") ? Infinity : Number(line.args[1]))
				)
					own.delete(fd);
		if (line.failure) {
			incomplete.add(line.failure);
			continue;
		}
		const { name: syscall, args } = line;
		const succeeded = syscallSucceeded(line);
		const descriptor = Number.parseInt(args[0] ?? "", 10);
		if (
			(aliases.has(descriptor) &&
				/^(?:ioctl|fstat|fstat64|newfstatat|statx|getsockopt|setsockopt|getsockname|getpeername|fcntl|fcntl64|lseek|_llseek)$/.test(
					syscall,
				)) ||
			/^(?:rt_sigaction|sigaction|rt_sigprocmask|sigprocmask)$/.test(syscall)
		)
			streamMetadataObserved = true;
		if (succeeded && /^dup[23]?$/.test(syscall)) {
			if (aliases.has(descriptor)) aliases.add(result);
			else aliases.delete(result);
		} else if (succeeded && /^fcntl(?:64)?$/.test(syscall) && /^F_DUPFD/.test(args[1] ?? "")) {
			if (aliases.has(descriptor)) aliases.add(result);
			else aliases.delete(result);
		} else if (succeeded && syscall === "close") aliases.delete(descriptor);
		else if (succeeded && /^(?:open|openat|openat2|creat|socket|accept|accept4)$/.test(syscall))
			aliases.delete(result);
		else if (succeeded && syscall === "close_range" && !args[2]?.includes("CLOSE_RANGE_CLOEXEC")) {
			for (const alias of aliases)
				if (alias >= Number(args[0]) && alias <= (/^~0/.test(args[1] ?? "") ? Infinity : Number(args[1])))
					aliases.delete(alias);
		}
		if (!syscall) continue;
		if (
			options.captureOutput &&
			succeeded &&
			/^(?:write|writev|sendto|sendmsg|sendfile|splice|vmsplice)$/.test(syscall)
		) {
			const endpoint = STREAM_ENDPOINT.exec(args[syscall === "splice" ? 2 : 0] ?? "")?.[2];
			const channel =
				endpoint === undefined
					? -1
					: (options.outputEndpoints?.findIndex((value) => value.endsWith(`:[${endpoint}]`)) ?? -1);
			if (channel >= 0 && result > 0) {
				const bytes = /^"((?:\\.|[^"\\])*)"$/.exec(args[1] ?? "");
				const vector =
					syscall === "writev"
						? queueVectors(args[1], Number(args[2]))
						: syscall === "sendmsg"
							? streamMessage(args[1])
							: undefined;
				const data = bytes ? decodeCBytes(bytes[1]!) : vector?.data;
				if (
					!data ||
					data.length < result ||
					line.order === undefined ||
					(outputBytes += result) > speculativeConfig().processReuse.traceOutputBytes
				)
					incomplete.add("output_bytes_unproven");
				else
					capturedOutput.push({
						fd: channel === 0 ? 1 : 2,
						data: data.subarray(0, result),
						order: line.order,
					});
			}
		}
		const observedPaths = syscallPaths(line, cwd);
		if (syscall === "getpid" || syscall === "getppid" || syscall === "getsid" || syscall === "getpgid") {
			taints.add("pid_observation");
		}
		const endpoint = STREAM_ENDPOINT.exec(args[0] ?? "")?.[2];
		if (
			syscall === "connect" &&
			succeeded &&
			endpoint &&
			options.controlledSocket &&
			/\bsun_path="(\/[^"]+)"/.exec(args[1] ?? "")?.[1] === options.controlledSocket
		) {
			ownPipes.add(endpoint);
			continue;
		}
		// A local socket reaches another process only once connected: a refused path (an NSS cache that is absent here) is a
		// pathname dependency, validated absent where the Actor runs.
		const refused =
			syscall === "connect" && /^-1 (?:ENOENT|ECONNREFUSED|EACCES)\b/.test(line.result)
				? /\bsun_path="(\/[^"]+)"/.exec(args[1] ?? "")?.[1]
				: undefined;
		if (refused) {
			observePath(refused, "input", /^-1 ENOENT\b/.test(line.result) ? "ENOENT" : undefined);
			continue;
		}
		const output = !!options.outputEndpoints?.includes(
			`socket:[${/^\d+<UNIX-STREAM:\[(\d+)/.exec(args[0] ?? "")?.[1]}]`,
		);
		const outputQuery = output && /^(?:getsockname|getpeername|getsockopt)$/.test(syscall);
		// Created pairs stay within the tree only while they neither open another connection nor send to another address.
		const localCall =
			endpoint &&
			ownPipes.has(endpoint) &&
			(syscall === "sendto"
				? args[4] === "NULL" && args[5] === "0"
				: syscall === "sendmsg"
					? streamMessage(args[1]) !== undefined
					: !/^(?:connect|bind|listen|accept|accept4|sendmmsg)$/.test(syscall));
		if (
			NETWORK_SYSCALLS.has(syscall) &&
			!nonSocketQuery(line) &&
			!outputQuery &&
			!(/^socket(?:pair)?$/.test(syscall) && /^AF_UNIX\b/.test(args[0] ?? "")) &&
			!localCall
		)
			taints.add("network");
		if (IPC_SYSCALLS.has(syscall)) taints.add("ipc");
		// Descriptor-local state is internal; reproduced OFD flags are sealed with their final offsets. A lock taken without waiting,
		// or a query that found the range free, depends only on no one else holding one, which a replay probes; a refusal, or a
		// holder found, saw another. One taken by waiting ends the same way natively, however long it waited; a release reveals nothing.
		const record = /^fcntl(?:64)?$/.test(syscall)
			? /^F_(?:OFD_)?(GETLK|SETLK|SETLKW)(?:64)?$/.exec(args[1] ?? "")?.[1]
			: undefined;
		const lock =
			syscall === "flock"
				? /\bLOCK_NB\b/.test(args[1] ?? "") && /\bLOCK_(SH|EX)\b/.exec(args[1] ?? "")?.[1]
				: record && record !== "SETLKW" && /\bl_type=F_(RD|WR|UN)LCK\b/.exec(args[2] ?? "")?.[1];
		const locked = absoluteDescriptorPath(args[0]);
		if (lock && (record === "GETLK" || lock !== "UN")) {
			if (locked && succeeded && (record === "GETLK") === (lock === "UN"))
				locks.set(locked, locks.get(locked) || (lock !== "SH" && lock !== "RD"));
			else taints.add("ipc");
		}
		if (/^fcntl(?:64)?$/.test(syscall) && !record) {
			const command = args[1] ?? "";
			if (
				!/^F_(?:GETFD|SETFD|DUPFD|DUPFD_CLOEXEC)$/.test(command) &&
				// A descriptor this process opened reports the flags it chose; an inherited one's flags need evidence.
				// Standard streams' status flags are part of the pinned execution context.
				!(
					command === "F_GETFL" &&
					(own.has(Number.parseInt(args[0] ?? "", 10)) || /^[012]</.test(args[0] ?? ""))
				) &&
				!(
					(command === "F_GETFL" ||
						(command === "F_SETFL" &&
							succeeded &&
							(args[2] ?? "")
								.split("|")
								.every((flag) =>
									/^(?:O_(?:RDONLY|WRONLY|RDWR|APPEND|NONBLOCK|NDELAY|LARGEFILE|DIRECTORY|DSYNC|SYNC|NOFOLLOW)|0)$/.test(
										flag,
									),
								))) &&
					(output ||
						outputPipes.has(/^\d+<(pipe:\[\d+\])>$/.exec(args[0] ?? "")?.[1] ?? "") ||
						own.has(Number.parseInt(args[0] ?? "", 10)) ||
						ownPipes.has(endpoint ?? ""))
				)
			)
				unsupported(syscall);
		}
		// An unprivileged process holds no capabilities, sandboxed or not (the observer runs as the Actor's user).
		const noCapabilities =
			syscall === "capget" &&
			process.getuid?.() !== 0 &&
			/\{effective=0, permitted=0, inheritable=0\}\) = 0$/.test(`${args.join(", ")}) = ${line.result}`);
		const limit =
			syscall === "prlimit64" ? args[1] : /^(?:getrlimit|setrlimit)$/.test(syscall) ? args[0] : undefined;
		// A refused io_uring leaves libuv on epoll and its thread pool: the same results, every file call traced.
		if (syscall === "io_uring_setup" && /^-1 EPERM\b/.test(line.result)) continue;
		if (
			!noCapabilities &&
			(CONFINEMENT_SENSITIVE_SYSCALLS.has(syscall) ||
				/^-1 (?:EACCES|EPERM)\b/.test(line.result) ||
				(syscall === "prctl" && !/^PR_(?:GET_NAME|SET_NAME|SET_VMA)$/.test(args[0] ?? "")) ||
				(["clone", "clone3", "fork", "vfork"].includes(syscall) && /^-1 EAGAIN\b/.test(line.result)) ||
				(limit === "RLIMIT_CORE" && syscall !== "setrlimit" && succeeded))
		) {
			taints.add("confinement_observation");
		}
		// A time set on a file the tree opened for writing reaches its effects, which carry each file's modification time.
		if (
			/^(?:utime|utimes|utimensat|futimesat)$/.test(syscall) &&
			!(observedPaths ?? [""]).every((target) => writable.has(target))
		)
			unsupported(syscall);
		if (
			// Even a failed setter can mutate a limit; sandbox caps and foreign limits are unmodeled.
			((syscall === "setrlimit" || (syscall === "prlimit64" && args[2] !== "NULL")) &&
				((syscall === "prlimit64" && args[0] !== "0") ||
					limit === "RLIMIT_NPROC" ||
					limit === "RLIMIT_CORE")) ||
			UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) ||
			(syscall === "pipe2" && /O_DIRECT|O_EXCL/.test(args[1] ?? "")) ||
			(syscall === "ioctl" && unmodeledFileIoctl(line))
		) {
			unsupported(syscall);
		}
		// A filesystem's statistics vary over time like the clock, whose taint every trace carries; the sandbox reports the one a
		// native run sees, the workspace's own included.
		if (/^f?statfs$/.test(syscall)) continue;
		const listed = /^getdents(?:64)?$/.test(syscall) ? absoluteDescriptorPath(args[0]) : undefined;
		// A listed directory's entry set is its dependency; readdir order and d_ino are volatile identity, as a descriptor's is.
		const listing = !!listed && succeeded;
		if (listing) {
			taints.add("descriptor_observation");
			listedPaths.add(listed!);
			if (paths.get(listed!) !== "executable") paths.set(listed!, "input");
		}
		if (UNMODELED_METADATA_SYSCALLS.has(syscall) && !listing && succeeded) {
			unsupported(syscall);
			incomplete.add(`unmodeled_metadata:${syscall}:${pid}`);
		}
		if (semanticRoots.length && workspaceDriverSemanticGap(line, observedPaths, semanticRoots)) {
			unsupported(syscall);
			incomplete.add(`filesystem_semantics:${syscall}:${pid}`);
		}
		if (succeeded && writesPath(line))
			for (const target of observedPaths ?? []) {
				const name = workspaceName(target);
				if (name) written.add(name);
				else external.add(target);
			}
		if (options.fileEvidence?.(line)) {
			const changed = tracedCwd(line, cwd);
			if (changed) cwds.set(pid, changed);
			else incomplete.add(`cwd_unknown:${pid}`);
			continue;
		}
		const [structure, flags] = MODELED_METADATA_SYSCALLS.get(syscall) ?? [];
		if (structure && succeeded) {
			// A recreated null device has the same I/O semantics, but may have a different device-node inode.
			if (line.native?.nullDevice || /<char 1:3>>$/.test(args[0] ?? "")) {
				taints.add("descriptor_observation");
				continue;
			}
			// The returned ABI fields are the observation. Program names and argv cannot prove a field unused.
			const observed = line.native?.observation ?? textStatObservation(args[structure]);
			if (!observedPaths?.length || !observed) {
				// fstat, or an empty *at name, of a pipe or socket
				if (line.native?.descriptor || (descriptorTarget(line) && !quotedArgument(args[1])))
					taints.add("descriptor_observation");
				else {
					unsupported(syscall);
					incomplete.add(`unparsed_metadata:${syscall}:${pid}`);
				}
			}
			if (observed && observed.fields?.length !== 0) {
				const followSymlinks =
					syscall !== "lstat" && !(flags && /\bAT_SYMLINK_NOFOLLOW\b/.test(args[flags] ?? ""));
				for (const metadataPath of observedPaths ?? [])
					observeMetadata(metadataPath, followSymlinks, observed);
			}
			continue;
		}
		if (!PATH_ARGUMENTS[syscall]) continue;
		const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
		if (!observedPaths) incomplete.add(`unresolved_pathname:${syscall}:${pid}`);
		const nonEmpty =
			/^(?:rmdir|unlinkat|rename(?:at2?)?)$/.test(syscall) && /^-1 (?:ENOTEMPTY|EEXIST)\b/.test(line.result);
		// Creating a name can fail on its parent; a multi-path syscall does not identify the failed operand.
		const failure =
			observedPaths?.length === 1 && !writesPath(line)
				? (/^-1 (ENOENT|ENOTDIR)\b/.exec(line.result)?.[1] as "ENOENT" | "ENOTDIR" | undefined)
				: undefined;
		for (const observed of observedPaths ?? []) {
			observePath(observed, role, failure);
			if (nonEmpty) listedPaths.add(observed);
		}
		if ((syscall === "chdir" || syscall === "fchdir") && succeeded) {
			const changed = tracedCwd(line, cwd);
			if (changed) cwds.set(pid, changed);
			else incomplete.add(`${syscall}_unparsed:${pid}`);
		}
	}
	// A workspace file the traced processes wrote changes under their own hands; its later state is their effect, not an input.
	// A run brokered from its tree, whose launcher the trace holds, wrote on its behalf.
	for (const pid of selected.keys())
		for (const name of options.brokeredWrites?.(pid) ?? []) {
			if (path.posix.isAbsolute(name)) external.add(name);
			else written.add(`//${name}`);
		}
	// What it wrote outside the workspace, and the directories holding those names, change by its own hand.
	const touched = new Set(
		[
			...external,
			...[...written].flatMap((name) => semanticRoots.map((root) => path.posix.join(root, name.slice(2)))),
		].flatMap((target) => [target, path.posix.dirname(target)]),
	);
	const metadataChanges = [...changedMetadata].filter((observed) => !touched.has(observed)).sort();
	if (incomplete.size) taints.add("trace_incomplete");
	return {
		complete: !incomplete.size,
		streamMetadataObserved,
		...(options.captureOutput
			? {
					output: capturedOutput
						.sort((left, right) => left.order - right.order)
						.map(({ fd, data }) => ({ fd, data })),
				}
			: {}),
		paths: Object.freeze(
			[
				...[...paths].map(([observedPath, role]) => ({
					path: observedPath,
					role: sharedObjectRole(observedPath, role),
					...(lookupFailures.get(observedPath) ? { lookupFailure: lookupFailures.get(observedPath)! } : {}),
					...(listedPaths.has(observedPath) ? { listed: true as const } : {}),
				})),
				...metadata.values(),
			].sort((left, right) => pathOrder(left).localeCompare(pathOrder(right))),
		),
		taints: Object.freeze([...taints].sort()),
		pids: [...selected.keys()],
		tracedProcesses: [...selected].filter(
			([pid, { lines, start }]) => start < Math.min(lines.length, brokered.get(pid) ?? Infinity),
		).length,
		incompleteReasons: Object.freeze([...incomplete].sort()),
		...(metadataChanges.length ? { metadataChanges: Object.freeze(metadataChanges) } : {}),
		...(unsupportedSyscalls.size
			? { unsupportedSyscalls: Object.freeze([...unsupportedSyscalls].sort()) }
			: {}),
		...(unsupportedSyscallsTruncated ? { unsupportedSyscallsTruncated: true as const } : {}),
		written: [...written].map((name) => name.slice(2)).sort(),
		external: [...external].sort(),
		locks: [...locks].map(([target, exclusive]) => ({
			path: target,
			exclusive,
		})),
	};
}
function pathOrder(item: ObservedProcessPath): string {
	return `${item.role}:${item.role === "metadata" ? item.followSymlinks : ""}:${item.path}`;
}
/** Kernel argument positions own pathname identity; each *at operand has its own directory binding.
 * An empty or NULL name of an operand that accepts AT_EMPTY_PATH names its descriptor. */
const PATH_ARGUMENTS: Readonly<
	Record<string, readonly (readonly [pathname: number | undefined, dirfd?: number, descriptorPath?: true])[]>
> = Object.fromEntries(
	(
		[
			[
				"access chdir chmod chown creat execve getxattr lgetxattr listxattr llistxattr mkdir mknod open readlink removexattr lremovexattr rmdir setxattr lsetxattr truncate unlink utime utimes stat lstat statfs",
				[[0]],
			],
			["rename link mount", [[0], [1]]],
			["symlink", [[1]]],
			["faccessat fchmodat mkdirat mknodat openat openat2 unlinkat", [[1, 0]]],
			["execveat faccessat2 fchownat newfstatat readlinkat statx utimensat", [[1, 0, true]]],
			[
				"renameat renameat2",
				[
					[1, 0],
					[3, 2],
				],
			],
			[
				"linkat",
				[
					[1, 0, true],
					[3, 2],
				],
			],
			["symlinkat", [[2, 1]]],
			["fchdir fstat fstatfs", [[undefined, 0]]],
			["write writev pwrite64 pwritev pwritev2 ftruncate fchmod fchown fallocate sendfile", [[undefined, 0]]],
			["copy_file_range splice", [[undefined, 2]]],
			["tee", [[undefined, 1]]],
			["mmap", [[undefined, 4]]],
		] as const
	).flatMap(([names, positions]) => names.split(" ").map((name) => [name, positions])),
);
const MODELED_METADATA_SYSCALLS = new Map<string, readonly [structure: number, flags?: number]>([
	["stat", [1]],
	["lstat", [1]],
	["fstat", [1]],
	["newfstatat", [2, 3]],
	["statx", [4, 2]],
]);
const UNMODELED_METADATA_SYSCALLS = new Set(["getdents", "getdents64"]);
/** Persistent metadata not represented by the typed workspace transaction must never be replayed. */
const UNMODELED_FILE_SEMANTICS_SYSCALLS = new Set([
	"fallocate",
	"splice",
	"tee",
	"fgetxattr",
	"flistxattr",
	"fremovexattr",
	"fsetxattr",
	"getxattr",
	"lgetxattr",
	"listxattr",
	"llistxattr",
	"lremovexattr",
	"lsetxattr",
	"removexattr",
	"setxattr",
]);
const UNMODELED_MUTATING_IOCTL =
	/\b(?:FICLONE|FICLONERANGE|FIDEDUPERANGE|FS_IOC_SETFLAGS|FS_IOC_SETVERSION|FS_IOC_FSSETXATTR)\b/;
const DRIVER_SEMANTIC_GAP_RESULT = /^-1\s+(?:EXDEV|EOPNOTSUPP|ENOTSUP|ENOSYS)\b/;
function unmodeledFileIoctl(line: TraceLine): boolean {
	// Close-on-exec is the descriptor's own flag, like F_SETFD (Python sets it on every file it opens).
	if (!syscallSucceeded(line) || /^FIO(?:N?CLEX)$/.test(line.args[1] ?? "")) return false;
	return (
		UNMODELED_MUTATING_IOCTL.test(line.args[1] ?? "") || absoluteDescriptorPath(line.args[0]) !== undefined
	);
}
function workspaceDriverSemanticGap(
	line: TraceLine,
	paths: readonly string[] | undefined,
	roots: readonly string[],
): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line.result)) return false;
	const referenced = new Set(paths);
	for (const argument of line.args) {
		const target = absoluteDescriptorPath(argument);
		if (target) referenced.add(target);
	}
	return [...referenced].some((candidate) => roots.some((root) => containsLogicalPath(root, candidate)));
}
const NETWORK_SYSCALLS = new Set([
	"getsockname",
	"getpeername",
	"getsockopt",
	"setsockopt",
	"listen",
	"shutdown",
	"accept",
	"accept4",
	"bind",
	"connect",
	"recvfrom",
	"recvmmsg",
	"recvmsg",
	"sendmmsg",
	"sendmsg",
	"sendto",
	"socket",
	"socketpair",
]);
const IPC_SYSCALLS = new Set(["mq_open", "msgget", "semget", "shmat", "shmget"]);
/** A failed query of a proven file/pipe reveals no socket state; unknown descriptor types stay tainted. */
function nonSocketQuery(line: TraceLine): boolean {
	return (
		/^(?:getsockname|getpeername|getsockopt)$/.test(line.name) &&
		/^-1 ENOTSOCK\b/.test(line.result) &&
		Boolean(absoluteDescriptorPath(line.args[0]) || /^\d+<pipe:\[\d+\]>$/.test(line.args[0] ?? ""))
	);
}
/** A traced execution's workspace writes so far, stamped in wall-clock ms; an unresolvable name is undefined. */
export interface TracedWrite {
	readonly paths?: readonly string[];
	readonly at: number;
	readonly opened: boolean;
	readonly mapped?: true;
	readonly pid?: number;
}
/** A trace read as it grows, each record parsed once however often it is asked: its writes so far, and when it last named each absolute path (by name, through a
 * descriptor, or against the cwd its process followed from `cwd`, unknown until its fork is on record; a failed lookup names one too), in wall-clock milliseconds.
 * Native metadata is conservatively seen when collected; its independent monotonic stamp owns ordering, without converting filesystem timestamps. */
export interface TraceTail extends AsyncDisposable {
	readonly writes: readonly TracedWrite[];
	readonly seen: ReadonlyMap<string, number>;
	readonly unsafe: boolean;
	readonly records: readonly TraceFile[];
	journal(pid: number): readonly TraceRecord[] | undefined;
	readonly frontier: boolean;
	read(): Promise<TraceTail>;
}
export function traceTail(
	tracePrefix: string | TraceTail,
	cwd: string,
	options: Pick<StraceObservationOptions, "privateUpper" | "previewBytes" | "execution" | "metadata"> = {},
): TraceTail {
	const writes: TracedWrite[] = [];
	const seen = new Map<string, number>();
	const files = new Map<number, TraceRecord[]>();
	const cwds = new Map<
		number,
		{
			cwd?: string;
		}
	>();
	const decode = traceDecoder();
	const ordered = new Map<number, TraceFile>();
	const positions = new Map(options.execution ? [[options.execution.pid, options.execution.start]] : []);
	const nativePositions = new Map<number, number>();
	const births = new Map<number, number>();
	const pendingMetadata = new Set<TraceRecord>();
	let reading: Promise<unknown> = Promise.resolve();
	let queued: Promise<TraceTail> | undefined;
	let root = options.execution?.pid;
	let unsafe = false;
	let partial: TraceRecord[] = [];
	let remaining = options.previewBytes;
	let closed = false;
	let offset = 0;
	let stream: FileHandle | undefined;
	if (remaining !== undefined && !isSafeCount(remaining)) throw new Error("invalid trace preview budget");
	const advance = async () => {
		if (closed) throw new Error("trace owner closed");
		for (const line of partial) ordered.delete(line.pid);
		if (typeof tracePrefix === "string" && !stream)
			stream = await open(`${tracePrefix}.stream`, "r").catch((error) => {
				if (error?.code !== "ENOENT") throw error;
				return undefined;
			});
		const records: TraceRecord[] = [];
		partial = [];
		if (typeof tracePrefix !== "string") {
			await tracePrefix.read();
			for (const [pid, start] of positions)
				for (const line of tracePrefix.journal(pid)?.slice(start) ?? []) {
					if (line.failure === "partial" || line.failure?.startsWith("unfinished:")) {
						partial.push(line);
						continue;
					}
					positions.set(pid, (positions.get(pid) ?? 0) + 1);
					records.push(line);
					appendGrouped(files, pid, line);
					const child = spawnedPID(line);
					if (child && !positions.has(child)) positions.set(child, 0);
				}
		} else if (stream && remaining !== 0) {
			const info = await stream.stat();
			if (!info.isFile() || info.size < offset) throw new Error("trace shrank or is not a regular file");
			const bytes = Buffer.allocUnsafe(Math.min(remaining ?? Infinity, info.size - offset));
			if ((await stream.read(bytes, 0, bytes.length, offset)).bytesRead !== bytes.length)
				throw new Error("trace changed while reading");
			offset += bytes.lastIndexOf(10) + 1; // A partial final record is reread on the next advance.
			if (remaining !== undefined) remaining -= bytes.length;
			let contents = bytes.toString("utf8");
			if (options.privateUpper) contents = contents.replaceAll(`<${options.privateUpper}/`, "</");
			for (const [pid, lines] of decode(contents)) {
				for (const line of lines)
					if (line.failure === "partial") partial.push(line);
					else {
						root ??= pid;
						records.push(line);
						appendGrouped(files, pid, line);
						const child = spawnedPID(line);
						if (child) births.set(child, line.order!);
					}
			}
		}
		if (typeof tracePrefix === "string" && options.metadata) {
			options.metadata.read();
			for (const [pid, lines] of files) {
				const native = options.metadata.files.get(pid) ?? [];
				const first = births.get(pid) ?? (pid === root ? lines[0]!.order! : undefined);
				// A child may make only kernel-collected calls before its first text record (even its exit).
				if (first === undefined) continue;
				for (let index = nativePositions.get(pid) ?? 0; index < native.length; index++) {
					const line = native[index]!;
					if (line.native!.entered < first) continue;
					lines.push(line);
					records.push(line);
				}
				nativePositions.set(pid, native.length);
			}
		}
		for (const line of pendingMetadata)
			if (!line.failure || line.failure !== "native_metadata_pending") {
				pendingMetadata.delete(line);
				records.push(line);
			}
		for (const line of records) ordered.delete(line.pid);
		records.sort((a, b) => a.at - b.at);
		root ??= records[0]?.pid; // A fork precedes its child's records, a chdir the forks after it; the first is the root's.
		for (const line of records) {
			const { pid, at } = line;
			if (line.native && line.failure === "native_metadata_pending") {
				pendingMetadata.add(line);
				continue;
			}
			// Path mutations are journaled below. Waiting or installing an image cannot itself mutate file data or names.
			unsafe ||=
				!Number.isFinite(at) ||
				(!!line.failure && !/^(?:wait(?:4|id|pid)|execve(?:at)?)$/.test(line.name)) ||
				(syscallSucceeded(line) &&
					(/^(?:mount|umount2?|move_mount|pivot_root|chroot|setns|unshare|io_uring_setup|io_submit|[fl]?setxattr|[fl]?removexattr)$/.test(
						line.name,
					) ||
						(line.name === "ioctl" && unmodeledFileIoctl(line))));
			if (line.failure) continue;
			const process = cwds.get(pid) ?? (pid === root ? cwds.set(pid, { cwd }).get(pid) : undefined);
			const paths = syscallPaths(line, process?.cwd ?? "");
			const child = spawnedPID(line);
			const mapped = line.name === "mmap" && /\bMAP_SHARED(?:_VALIDATE)?\b/.test(line.args[3] ?? "");
			if (syscallSucceeded(line) && (writesPath(line) || mapped))
				writes.push({
					...(paths?.every((target) => target.startsWith("/")) ? { paths } : {}),
					at,
					pid,
					opened: /^open(?:at2?)?$/.test(line.name) || line.name === "mmap",
					...(line.name === "mmap" ? { mapped: true } : {}),
				});
			for (const target of [
				...(paths ?? []),
				...line.args.flatMap((arg) => absoluteDescriptorPath(arg) ?? []),
			])
				if (target.startsWith("/")) seen.set(target, Math.max(seen.get(target) ?? 0, at));
			if (process) process.cwd = tracedCwd(line, process.cwd);
			if (child && !cwds.has(child)) {
				const shared = sharesFilesystem(line);
				cwds.set(child, shared && process ? process : { cwd: shared === false ? process?.cwd : undefined });
			}
		}
	};
	return {
		async [Symbol.asyncDispose]() {
			closed = true;
			await reading;
			await stream?.close();
			stream = undefined;
		},
		writes,
		seen,
		get unsafe() {
			return (
				unsafe ||
				[...partial, ...decode.pending()].some(
					(line) => !/^(?:wait(?:4|id|pid)|execve(?:at)?)$/.test(line.name),
				)
			);
		},
		get frontier() {
			return false;
		},
		journal(pid) {
			const pending = [...partial, ...decode.pending()].filter((line) => line.pid === pid);
			return pending.length ? [...(files.get(pid) ?? []), ...pending] : files.get(pid);
		},
		get records() {
			const pending = [...partial, ...decode.pending()];
			for (const line of [...pending, ...pendingMetadata]) ordered.delete(line.pid);
			return [...new Set([...files.keys(), ...pending.map((line) => line.pid)])].map((pid) => {
				const entry = ordered.get(pid) ?? {
					pid,
					lines: [...this.journal(pid)!].sort((a, b) => (a.order ?? Infinity) - (b.order ?? Infinity)),
				};
				ordered.set(pid, entry);
				return entry;
			});
		},
		read() {
			if (queued) return queued;
			// A batch starts after every caller it serves. Requests arriving during a read join the next batch.
			const next = reading.then(() => {
				queued = undefined;
				return advance();
			});
			reading = next.catch(() => {
				unsafe = true;
			});
			return (queued = next.then(() => this));
		},
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
function writesPath(line: TraceLine): boolean {
	return (
		/^(?:creat|truncate|rename|renameat2?|link|linkat|symlink|symlinkat|mknod|mknodat|unlink|unlinkat|mkdir|mkdirat|rmdir|utimes?|utimensat|futimesat|chmod|fchmodat|chown|fchownat|writev?|pwrite(?:64|v2?)|ftruncate|fchmod|fchown|fallocate|sendfile|copy_file_range|splice|tee)$/.test(
			line.name,
		) ||
		(line.name === "mmap" &&
			/\bPROT_WRITE\b/.test(line.args[2] ?? "") &&
			/\bMAP_SHARED(?:_VALIDATE)?\b/.test(line.args[3] ?? "")) ||
		(/^open(?:at2?)?$/.test(line.name) && /\bO_(?:WRONLY|RDWR|CREAT|TRUNC)\b/.test(line.args.join(" ")))
	);
}
/** Only the owned tracer can write this marker, after successful adoption and before resuming. */
function brokeredProcessSegments(
	selected: ReadonlyMap<number, TraceProcess>,
	options: StraceObservationOptions,
) {
	const brokered = new Map<number, number>();
	for (const [pid, { lines, start }] of selected)
		for (let index = start; index < lines.length; index++) {
			const line = lines[index]!;
			if (line.name !== "pi_brokered_exec") continue;
			if (
				!syscallSucceeded(line) ||
				index <= start ||
				!successfulExec(lines[index - 1]!) ||
				brokered.has(pid) ||
				options.brokeredWrites?.(pid) === undefined ||
				lines
					.slice(index + 1)
					.some((next) => next.failure || (next.name && !/^(?:exit|exit_group|terminated)$/.test(next.name)))
			)
				throw new Error(`unproven brokered exec: ${pid}`);
			brokered.set(pid, index - 1);
		}
	return brokered;
}
function tracedCwd(line: TraceLine, cwd: string | undefined): string | undefined {
	return syscallSucceeded(line) && /^(?:chdir|fchdir)$/.test(line.name)
		? syscallPaths(line, cwd ?? "")?.[0]
		: cwd;
}
function successfulExec(line: TraceLine): boolean {
	return (line.name === "execve" || line.name === "execveat") && syscallSucceeded(line);
}
function spawnedPID(line: TraceLine): number | undefined {
	if (!["clone", "clone3", "fork", "vfork"].includes(line.name)) return undefined;
	const pid = Number(line.result);
	return isSafeCount(pid, 1) ? pid : undefined;
}
function sharesFilesystem(line: TraceLine): boolean | undefined {
	if (line.name === "fork" || line.name === "vfork") return false;
	const flags =
		line.name === "clone3"
			? /^\{flags=([^,}]+)(?:,|})/.exec(line.args[0] ?? "")?.[1]
			: line.args.find((argument) => argument.startsWith("flags="))?.slice(6);
	if (!flags) return undefined;
	let shared = false;
	for (const flag of flags.split("|")) {
		const number = parseInteger(flag);
		if (number !== undefined) shared ||= (number & 0x200n) !== 0n;
		else if (flag === "CLONE_FS") shared = true;
		else if (!/^(?:CLONE_[A-Z0-9_]+|SIG[A-Z0-9]+)$/.test(flag)) return undefined;
	}
	return shared;
}
function syscallSucceeded(line: TraceLine): boolean {
	return /^(?:0x[0-9a-f]+|[0-9]+)(?:\b|<)/i.test(line.result);
}
function syscallPaths(line: TraceLine, cwd: string): readonly string[] | undefined {
	if (line.native) return line.native.paths;
	const previous = pathObservations.get(line);
	if (previous?.cwd === cwd) return previous.paths;
	const paths = (PATH_ARGUMENTS[line.name] ?? []).map(([pathname, dirfd, descriptorPath]) => {
		// strace names AT_FDCWD by the kernel's cwd, which a sandbox serving chdir itself never moves: the traced cwd is the process's.
		const descriptor = dirfd === undefined ? undefined : line.args[dirfd];
		const current = dirfd === undefined || /^(?:AT_FDCWD|-100)(?:<|$)/.test(descriptor ?? "");
		if (pathname === undefined || (descriptorPath && /^(?:""|NULL)$/.test(line.args[pathname] ?? ""))) {
			const target = current ? cwd : absoluteDescriptorPath(descriptor);
			return target ? [target] : current || /^\d+$/.test(descriptor ?? "") ? undefined : [];
		}
		const value = quotedArgument(line.args[pathname]);
		if (value?.startsWith("/")) return [walkedPath(value)]; // Absolute names ignore dirfd, even an invalid one.
		const base = current ? cwd : absoluteDescriptorPath(descriptor);
		// Without a name or a directory, a failed call looked nothing up and a successful one is unresolved.
		return value && base ? [walkedPath(`${base}/${value}`)] : syscallSucceeded(line) ? undefined : [];
	});
	const observed = paths.includes(undefined) ? undefined : (paths.flat() as string[]);
	pathObservations.set(line, { cwd, paths: observed });
	return observed;
}
/** Keep `..`: only a walk over the recorded tree knows whether it leaves a symlinked directory. */
function walkedPath(value: string): string {
	return `/${value
		.split("/")
		.filter((segment) => segment && segment !== ".")
		.join("/")}`;
}
function absoluteDescriptorPath(descriptor: string | undefined): string | undefined {
	const target = /^\d+<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
	return target?.startsWith("/") && !target.endsWith(" (deleted)")
		? path.posix.normalize(decodeCString(target))
		: undefined;
}
/** Non-path descriptors, and memfds such as Sandlock's virtual /proc/mounts (its content comes from the opened path), are
 * already typed in the process key, but their kernel identity is volatile. */
function descriptorTarget(line: TraceLine): boolean {
	const target = /^\d+<(.+)>(?:\(deleted\))?$/.exec(line.args[0] ?? "")?.[1]; // strace marks an unlinked memfd after its bracket
	return Boolean(target && (!target.startsWith("/") || target.startsWith("/memfd:")));
}
type StatObservation = {
	readonly digest: Sha256Digest;
	readonly fields?: readonly FilesystemObservationField[];
};
function parseInteger(value: string | undefined): bigint | undefined {
	if (!value) return undefined;
	const normalized = value.trim();
	try {
		if (/^-?0x[0-9a-f]+$/i.test(normalized)) return BigInt(normalized);
		if (/^-?0[0-7]+$/.test(normalized)) {
			const negative = normalized.startsWith("-");
			const magnitude = BigInt(`0o${normalized.replace(/^-?0/, "") || "0"}`);
			return negative ? -magnitude : magnitude;
		}
		return /^-?[0-9]+$/.test(normalized) ? BigInt(normalized) : undefined;
	} catch {
		return undefined;
	}
}
/** Linux's userspace-compatible new_encode_dev layout. */
function linuxDevice(major: bigint, minor: bigint): bigint {
	return ((major & 0xfffn) << 8n) | (minor & 0xffn) | ((minor & ~0xffn) << 12n) | ((major & ~0xfffn) << 32n);
}
function quotedArgument(argument: string | undefined): string | undefined {
	const match = /^"((?:\\.|[^"\\])*)"$/.exec(argument ?? "");
	return match ? decodeCString(match[1]!) : undefined;
}
function decodeCString(value: string): string {
	// Paths must round-trip through Node's UTF-8 APIs; queue payloads remain bytes.
	return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(decodeCBytes(value));
}
function queueVectors(
	value: string | undefined,
	count: number,
):
	| {
			data: Buffer;
			length: number;
	  }
	| undefined {
	if (!value || !/^\[(?:\{iov_base="(?:\\.|[^"\\])*", iov_len=\d+\}(?:, )?)*\]$/.test(value)) return;
	const vectors = [...value.matchAll(/\{iov_base="((?:\\.|[^"\\])*)", iov_len=(\d+)\}/g)];
	if (vectors.length !== count) return;
	return {
		data: Buffer.concat(vectors.map((vector) => decodeCBytes(vector[1]!))),
		length: vectors.reduce((sum, vector) => sum + Number(vector[2]), 0),
	};
}
function decodeCBytes(value: string): Buffer {
	const chunks: Buffer[] = [];
	let start = 0;
	for (const match of value.matchAll(/\\(?:x([0-9a-fA-F]{2})|([0-7]{1,3})|(.))/g)) {
		chunks.push(Buffer.from(value.slice(start, match.index), "utf8"));
		const byte = match[1]
			? Number.parseInt(match[1], 16)
			: match[2]
				? Number.parseInt(match[2], 8)
				: (({ a: 7, b: 8, f: 12, n: 10, r: 13, t: 9, v: 11 } as Record<string, number>)[match[3]!] ??
					match[3]!.charCodeAt(0));
		chunks.push(Buffer.from([byte]));
		start = match.index + match[0].length;
	}
	chunks.push(Buffer.from(value.slice(start), "utf8"));
	return Buffer.concat(chunks);
}
function sharedObjectRole(observedPath: string, role: DependencyRole): DependencyRole {
	return role === "input" && /(?:^|\/)lib[^/]*\.so(?:\.|$)/.test(observedPath) ? "shared_object" : role;
}
