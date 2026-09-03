import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import {
	type DependencyRole,
	filesystemObservationDigest,
	type ProvenanceTaint,
	type Sha256Digest,
} from "./provenance-certificate.ts";

const CONFINEMENT_SENSITIVE_SYSCALLS = new Set([
	"seccomp", "capget", "capset", "mount", "umount2", "pivot_root", "swapon", "swapoff", "reboot",
	"sethostname", "setdomainname", "kexec_load", "init_module", "finit_module", "delete_module", "unshare", "setns",
	"perf_event_open", "bpf", "userfaultfd", "keyctl", "add_key", "request_key", "ptrace", "process_vm_readv",
	"process_vm_writev", "open_by_handle_at", "name_to_handle_at", "quotactl", "acct", "lookup_dcookie",
	"io_uring_setup", "io_uring_enter", "io_uring_register", "personality",
]);
const SYSCALL_FILTER = `trace=%file,%process,%network,%ipc,getpid,getppid,getsid,getpgid,clock_gettime,gettimeofday,time,getrandom,sysinfo,times,getrusage,getrlimit,setrlimit,prlimit64,fchdir,fallocate,ioctl,prctl,fstat,fstatfs,getdents,getdents64,${[...CONFINEMENT_SENSITIVE_SYSCALLS].join(",")}`;

/** One production trace shape shared by execution and dependency-ablation paths. */
export function straceCommand(
	strace: string,
	tracePrefix: string,
	command: readonly string[],
): readonly string[] {
	return [strace, "-ff", "-qq", "-yy", "-v", "-s", "65535", "-e", SYSCALL_FILTER, "-o", tracePrefix, ...command];
}

export type ObservedProcessPath =
	| { readonly path: string; readonly role: DependencyRole }
	| {
			readonly path: string;
			readonly role: "metadata";
			readonly followSymlinks: boolean;
			readonly digest: Sha256Digest;
	  };

export interface StraceObservation {
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	readonly incompleteReasons: readonly string[];
}

export interface StraceObservationOptions {
	/** Intercepted path to native target; a direct second exec proves descriptor-preserving bypass. */
	readonly interposedExecutables?: readonly (readonly [intercepted: string, original: string])[];
	/**
	 * Workspace roots whose driver-specific unsupported errors must invalidate adoption. This keeps
	 * a COW substrate from changing a command result when the Actor filesystem supports the syscall.
	 */
	readonly guardFilesystemSemanticsWithin?: readonly string[];
}

interface TraceFile {
	readonly pid: number;
	readonly lines: readonly string[];
}

/**
 * Decode a bounded strace -ff transcript. The parser deliberately fails closed: only the target
 * exec and its recursively identified descendants contribute a replayable certificate.
 */
export async function observeStrace(
	tracePrefix: string,
	executablePath: string,
	initialCwd: string,
	options: StraceObservationOptions = {},
): Promise<StraceObservation> {
	const directory = path.dirname(tracePrefix);
	const prefix = `${path.basename(tracePrefix)}.`;
	const files: TraceFile[] = [];
	for (const name of await readdir(directory)) {
		if (!name.startsWith(prefix)) continue;
		const pid = Number.parseInt(name.slice(prefix.length), 10);
		if (!Number.isSafeInteger(pid) || pid <= 0) continue;
		const contents = await readFile(path.join(directory, name), "utf8");
		files.push({ pid, lines: contents.split(/\r?\n/) });
	}
	const target = path.posix.resolve(executablePath);
	let root: { readonly file: TraceFile; readonly start: number } | undefined;
	for (const file of files) {
		for (let index = 0; index < file.lines.length; index++) {
			const line = file.lines[index]!;
			if (!successfulExec(line)) continue;
			const first = quotedStrings(line)[0];
			if (first && path.posix.resolve(first) === target) {
				root = { file, start: index };
				break;
			}
		}
		if (root) break;
	}
	if (!root) {
		return {
			complete: false,
			paths: [],
			taints: ["trace_incomplete"],
			tracedProcesses: 0,
			incompleteReasons: ["target_exec_not_found"],
		};
	}

	const byPID = new Map(files.map((file) => [file.pid, file]));
	const selected = new Map<number, number>([[root.file.pid, root.start]]);
	const initialCwds = new Map<number, string | undefined>([[root.file.pid, path.posix.resolve(initialCwd)]]);
	const queue = [root.file.pid];
	let complete = true;
	const incompleteReasons = new Set<string>();
	while (queue.length) {
		const pid = queue.shift()!;
		const file = byPID.get(pid);
		if (!file) {
			complete = false;
			incompleteReasons.add(`trace_file_missing:${pid}`);
			continue;
		}
		let cwd = initialCwds.get(pid);
		for (const line of file.lines.slice(selected.get(pid) ?? 0)) {
			cwd = tracedCwd(line, cwd);
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) {
				complete = false;
				incompleteReasons.add(`child_trace_missing:${child}`);
				continue;
			}
			selected.set(child, 0);
			initialCwds.set(child, cwd);
			queue.push(child);
		}
	}

	const paths = new Map<string, DependencyRole>();
	const metadata = new Map<string, Extract<ObservedProcessPath, { role: "metadata" }>>();
	const taints = new Set<ProvenanceTaint>();
	const interposedExecutables = new Map(
		(options.interposedExecutables ?? []).map(([intercepted, original]) => [
			path.posix.resolve(intercepted), path.posix.resolve(original),
		]),
	);
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) => path.posix.resolve(value));
	const ignoredSegments = ignoredProcessSegments(selected, byPID, interposedExecutables, initialCwds);
	const observeMetadata = (observedPath: string, followSymlinks: boolean, digest: Sha256Digest) => {
		const identity = `metadata:${followSymlinks}:${observedPath}`;
		if (metadata.get(identity)?.digest !== undefined && metadata.get(identity)?.digest !== digest) {
			taints.add("mutable_input");
			incompleteReasons.add(`metadata_changed:${observedPath}`);
		}
		metadata.set(identity, {
			path: observedPath,
			role: "metadata",
			followSymlinks,
			digest,
		});
	};
	for (const [pid, start] of selected) {
		const file = byPID.get(pid);
		if (!file) continue;
		let cwd = initialCwds.get(pid);
		if (!cwd) {
			complete = false;
			incompleteReasons.add(`cwd_unknown:${pid}`);
			cwd = path.posix.resolve(initialCwd);
		}
		const unfinished = new Map<string, number>();
		for (let index = start; index < file.lines.length; index++) {
			if (ignoredSegments.get(pid)?.some(([from, to]) => index >= from && index < to)) continue;
			const line = file.lines[index]!;
			if (!line) continue;
			if (line.includes("<unfinished ...>")) {
				const name = syscallName(line);
				if (name) unfinished.set(name, (unfinished.get(name) ?? 0) + 1);
				else {
					complete = false;
					incompleteReasons.add(`unfinished_unparsed:${pid}`);
				}
				continue;
			}
			const resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>/.exec(line)?.[1];
			if (resumed) {
				const count = unfinished.get(resumed) ?? 0;
				if (count <= 1) unfinished.delete(resumed);
				else unfinished.set(resumed, count - 1);
				continue;
			}
			if (line.startsWith("+++ killed by") || line.startsWith("--- SIG")) {
				continue;
			}
			const syscall = syscallName(line);
			if (!syscall) continue;
			if (syscall === "getpid" || syscall === "getppid" || syscall === "getsid" || syscall === "getpgid") {
				taints.add("pid_observation");
			}
			if (NETWORK_SYSCALLS.has(syscall)) taints.add("network");
			if (IPC_SYSCALLS.has(syscall)) taints.add("ipc");
			if (CLOCK_SYSCALLS.has(syscall)) taints.add("clock");
			if (RANDOM_SYSCALLS.has(syscall)) taints.add("random");
			if (CONFINEMENT_SENSITIVE_SYSCALLS.has(syscall) || prctlConfinementSensitive(line, syscall) || confinementDenied(line) || processLimitDenied(line, syscall)) {
				taints.add("confinement_observation");
			}
			if (
				resourceLimitMutation(line, syscall) ||
				UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) ||
				(syscall === "ioctl" && unmodeledFileIoctl(line))
			) {
				taints.add("unsupported_syscall");
			}
			if (UNMODELED_METADATA_SYSCALLS.has(syscall) && syscallSucceeded(line)) {
				taints.add("unsupported_syscall");
				incompleteReasons.add(`unmodeled_metadata:${syscall}:${pid}`);
			}
			if (semanticRoots.length && workspaceDriverSemanticGap(line, syscall, cwd, semanticRoots)) {
				complete = false;
				taints.add("unsupported_syscall");
				incompleteReasons.add(`filesystem_semantics:${syscall}:${pid}`);
			}
			if ((syscall === "chdir" || syscall === "fchdir") && syscallSucceeded(line)) {
				const changed = tracedCwd(line, cwd);
				if (changed) cwd = changed;
				else {
					complete = false;
					incompleteReasons.add(`${syscall}_unparsed:${pid}`);
				}
			}
			if (MODELED_METADATA_SYSCALLS.has(syscall)) {
				if (syscallSucceeded(line)) {
					const metadataPaths = metadataSyscallPaths(line, syscall, cwd);
					const digest = statObservationDigest(line);
					if (!metadataPaths.length || !digest) {
						if (syscall === "fstat" && descriptorTarget(line)) taints.add("descriptor_observation");
						else {
							taints.add("unsupported_syscall");
							incompleteReasons.add(`unparsed_metadata:${syscall}:${pid}`);
						}
					}
					if (digest) {
						for (const observed of metadataPaths) observeMetadata(observed.path, observed.followSymlinks, digest);
					}
				} else {
					for (const observed of syscallPaths(line, syscall, cwd)) {
						if (paths.get(observed) !== "executable") paths.set(observed, "input");
					}
				}
				continue;
			}
			if (!FILE_SYSCALLS.has(syscall)) continue;
			const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
			for (const observed of syscallPaths(line, syscall, cwd)) {
				if (paths.get(observed) !== "executable") paths.set(observed, role);
			}
		}
		if (unfinished.size) {
			complete = false;
			for (const name of unfinished.keys()) incompleteReasons.add(`unfinished:${pid}:${name}`);
		}
	}
	if (!complete) taints.add("trace_incomplete");
	return {
		complete,
		paths: Object.freeze(
			[
				...[...paths].map(([observedPath, role]) => ({ path: observedPath, role: sharedObjectRole(observedPath, role) })),
				...metadata.values(),
			]
				.sort((left, right) =>
					`${left.role}:${left.role === "metadata" ? left.followSymlinks : ""}:${left.path}`.localeCompare(
						`${right.role}:${right.role === "metadata" ? right.followSymlinks : ""}:${right.path}`,
					),
				),
		),
		taints: Object.freeze([...taints].sort()),
		tracedProcesses: [...selected].filter(([pid, start]) =>
			byPID.get(pid)?.lines.slice(start).some((_, offset) =>
				!ignoredSegments.get(pid)?.some(([from, to]) => start + offset >= from && start + offset < to),
			),
		).length,
		incompleteReasons: Object.freeze([...incompleteReasons].sort()),
	};
}

const FILE_SYSCALLS = new Set([
	"access",
	"chdir",
	"chmod",
	"chown",
	"creat",
	"execve",
	"execveat",
	"fchdir",
	"faccessat",
	"faccessat2",
	"fchmodat",
	"fchownat",
	"getxattr",
	"lgetxattr",
	"listxattr",
	"llistxattr",
	"link",
	"linkat",
	"mkdir",
	"mkdirat",
	"mknod",
	"mknodat",
	"mount",
	"open",
	"openat",
	"openat2",
	"readlink",
	"readlinkat",
	"removexattr",
	"lremovexattr",
	"rename",
	"renameat",
	"renameat2",
	"rmdir",
	"setxattr",
	"lsetxattr",
	"symlink",
	"symlinkat",
	"truncate",
	"unlink",
	"unlinkat",
	"utime",
	"utimensat",
	"utimes",
]);

const MODELED_METADATA_SYSCALLS = new Set(["stat", "lstat", "fstat", "newfstatat"]);
const UNMODELED_METADATA_SYSCALLS = new Set(["statx", "statfs", "fstatfs", "getdents", "getdents64"]);

/** Persistent metadata not represented by the typed workspace transaction must never be replayed. */
const UNMODELED_FILE_SEMANTICS_SYSCALLS = new Set([
	"fallocate",
	"fgetxattr",
	"flistxattr",
	"fremovexattr",
	"fsetxattr",
	"futimesat",
	"getxattr",
	"lgetxattr",
	"listxattr",
	"llistxattr",
	"lremovexattr",
	"lsetxattr",
	"removexattr",
	"setxattr",
	"utime",
	"utimensat",
	"utimes",
]);

const UNMODELED_MUTATING_IOCTL = /\b(?:FICLONE|FICLONERANGE|FIDEDUPERANGE|FS_IOC_SETFLAGS|FS_IOC_SETVERSION|FS_IOC_FSSETXATTR)\b/;
const DRIVER_SEMANTIC_GAP_RESULT = /=\s*-1\s+(?:EXDEV|EOPNOTSUPP|ENOTSUP|ENOSYS)\b/;

function unmodeledFileIoctl(line: string): boolean {
	if (!syscallSucceeded(line)) return false;
	if (UNMODELED_MUTATING_IOCTL.test(line)) return true;
	const descriptorPath = /^\s*ioctl\(\d+<([^>]+)>/.exec(line)?.[1];
	return descriptorPath?.startsWith("/") ?? false;
}

function workspaceDriverSemanticGap(
	line: string,
	syscall: string,
	cwd: string,
	roots: readonly string[],
): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line)) return false;
	const referenced = new Set(syscallPaths(line, syscall, cwd));
	for (const match of line.matchAll(/\d+<(\/[^>]+)>/g)) referenced.add(path.posix.normalize(match[1]!));
	return [...referenced].some((candidate) => roots.some((root) => posixContains(root, candidate)));
}

function posixContains(root: string, candidate: string): boolean {
	const relative = path.posix.relative(path.posix.resolve(root), path.posix.resolve(candidate));
	return relative === "" || (relative !== ".." && !relative.startsWith("../") && !path.posix.isAbsolute(relative));
}

const NETWORK_SYSCALLS = new Set([
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

const IPC_SYSCALLS = new Set([
	"mq_open",
	"msgget",
	"semget",
	"shmat",
	"shmget",
]);

const CLOCK_SYSCALLS = new Set(["clock_gettime", "gettimeofday", "time", "sysinfo", "times", "getrusage"]);
const RANDOM_SYSCALLS = new Set(["getrandom"]);

function resourceLimitMutation(line: string, syscall: string): boolean {
	if (!syscallSucceeded(line)) return false;
	return syscall === "setrlimit" || (syscall === "prlimit64" && !/^\s*prlimit64\([^,]+,[^,]+,\s*NULL\s*,/.test(line));
}

function ignoredProcessSegments(
	selected: ReadonlyMap<number, number>,
	byPID: ReadonlyMap<number, TraceFile>,
	interposedExecutables: ReadonlyMap<string, string>,
	initialCwds: ReadonlyMap<number, string | undefined>,
): Map<number, Array<readonly [number, number]>> {
	const ignored = new Map<number, Array<readonly [number, number]>>();
	if (!interposedExecutables.size) return ignored;
	const queue: Array<readonly [number, number]> = [];
	const fullyIgnored = new Set<number>();
	for (const [pid, start] of selected) {
		const file = byPID.get(pid);
		if (!file) continue;
		let cwd = initialCwds.get(pid);
		for (let index = start; index < file.lines.length; index++) {
			const line = file.lines[index]!;
			cwd = tracedCwd(line, cwd);
			if (!successfulExec(line)) continue;
			const executable = quotedStrings(line)[0];
			const original = executable && interposedExecutables.get(tracedPath(executable, cwd) ?? "");
			if (!original) continue;
			let resumed = -1, resumedExecutable: string | undefined, resumedCwd = cwd;
			for (let candidateIndex = index + 1; candidateIndex < file.lines.length; candidateIndex++) {
				const candidate = file.lines[candidateIndex]!;
				resumedCwd = tracedCwd(candidate, resumedCwd);
				if (!successfulExec(candidate)) continue;
				resumed = candidateIndex;
				resumedExecutable = quotedStrings(candidate)[0];
				break;
			}
			if (resumedExecutable && tracedPath(resumedExecutable, resumedCwd) === original) {
				(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, resumed]);
				index = resumed - 1;
				continue;
			}
			(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, file.lines.length]);
			fullyIgnored.add(pid);
			queue.push([pid, index]);
			break;
		}
	}
	while (queue.length) {
		const [pid, start] = queue.shift()!;
		const file = byPID.get(pid);
		if (!file) continue;
		for (const line of file.lines.slice(start)) {
			const child = spawnedPID(line);
			if (!child || fullyIgnored.has(child)) continue;
			ignored.set(child, [[0, byPID.get(child)?.lines.length ?? Number.POSITIVE_INFINITY]]);
			fullyIgnored.add(child);
			queue.push([child, 0]);
		}
	}
	return ignored;
}

function tracedCwd(line: string, cwd: string | undefined): string | undefined {
	if (!syscallSucceeded(line)) return cwd;
	const syscall = syscallName(line);
	if (syscall === "fchdir") return fchdirPath(line);
	if (syscall !== "chdir") return cwd;
	const target = quotedStrings(line)[0];
	return target ? tracedPath(target, cwd) : undefined;
}

function tracedPath(target: string, cwd: string | undefined): string | undefined {
	return path.posix.isAbsolute(target) ? path.posix.resolve(target) : cwd ? path.posix.resolve(cwd, target) : undefined;
}

function successfulExec(line: string): boolean {
	return /\bexecve(?:at)?\(/.test(line) && syscallSucceeded(line);
}

function spawnedPID(line: string): number | undefined {
	if (!/^\s*(?:clone|clone3|fork|vfork)\(/.test(line)) return undefined;
	const match = /=\s*(\d+)\s*$/.exec(line);
	if (!match) return undefined;
	const pid = Number.parseInt(match[1]!, 10);
	return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
}

function syscallName(line: string): string | undefined {
	return /^\s*([a-zA-Z0-9_]+)\(/.exec(line)?.[1];
}

function syscallSucceeded(line: string): boolean {
	const result = /=\s*([^\s]+)/.exec(line)?.[1];
	return result !== undefined && result !== "-1" && result !== "?";
}

function confinementDenied(line: string): boolean {
	return /\)\s+= -1 (?:EACCES|EPERM)\b/.test(line);
}

function prctlConfinementSensitive(line: string, syscall: string): boolean {
	return syscall === "prctl" && !/^prctl\(PR_SET_(?:NAME|VMA)\b/.test(line);
}

function processLimitDenied(line: string, syscall: string): boolean {
	return ["clone", "clone3", "fork", "vfork"].includes(syscall) && /\)\s+= -1 EAGAIN\b/.test(line);
}

function syscallPaths(line: string, syscall: string, cwd: string): readonly string[] {
	if (syscall === "fchdir") {
		const target = fchdirPath(line);
		return target ? [target] : [];
	}
	const quoted = quotedStrings(line);
	if (!quoted.length) return [];
	let values: readonly string[];
	if (syscall === "symlink" || syscall === "symlinkat") values = quoted.slice(-1);
	else if (["rename", "renameat", "renameat2", "link", "linkat"].includes(syscall)) values = quoted.slice(0, 2);
	else values = quoted.slice(0, 1);
	const dirfd = /(?:openat2?|newfstatat|statx|faccessat2?|readlinkat|mkdirat|unlinkat|execveat)\(([^,]+),/.exec(line)?.[1];
	let base = cwd;
	if (dirfd && dirfd !== "AT_FDCWD") {
		const descriptorPath = /^\d+<([^>]+)>$/.exec(dirfd.trim())?.[1];
		if (descriptorPath) base = descriptorPath;
	}
	return values
		.filter((value) => value.length > 0)
		.map((value) => resolveObservedPath(value, base));
}

function fchdirPath(line: string): string | undefined {
	const descriptor = /^\s*fchdir\((.+)\)\s+=/.exec(line)?.[1];
	return absoluteDescriptorPath(descriptor);
}

function absoluteDescriptorPath(descriptor: string | undefined): string | undefined {
	const target = /^\d+<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
	return target?.startsWith("/") && !target.endsWith(" (deleted)") ? path.posix.normalize(target) : undefined;
}

function metadataSyscallPaths(
	line: string,
	syscall: string,
	cwd: string,
): readonly { readonly path: string; readonly followSymlinks: boolean }[] {
	const followSymlinks = syscall !== "lstat" && !/\bAT_SYMLINK_NOFOLLOW\b/.test(line);
	const paths = syscall === "fstat" ? [] : syscallPaths(line, syscall, cwd);
	if (paths.length) return paths.map((observedPath) => ({ path: observedPath, followSymlinks }));
	if (syscall !== "fstat" && syscall !== "newfstatat") return [];
	const descriptorPath = absoluteDescriptorPath(/^\s*(?:fstat|newfstatat)\((\d+<.+?>),/.exec(line)?.[1]);
	return descriptorPath ? [{ path: descriptorPath, followSymlinks }] : [];
}

/** Non-path descriptors are already typed in the process key, but their kernel identity is volatile. */
function descriptorTarget(line: string): boolean {
	const target = /^\s*fstat\(\d+<(.+?)>,/.exec(line)?.[1];
	return Boolean(target && !target.startsWith("/"));
}

const STAT_MODE_BITS: Readonly<Record<string, bigint>> = {
	S_IFSOCK: 0o140000n,
	S_IFLNK: 0o120000n,
	S_IFREG: 0o100000n,
	S_IFBLK: 0o060000n,
	S_IFDIR: 0o040000n,
	S_IFCHR: 0o020000n,
	S_IFIFO: 0o010000n,
	S_ISUID: 0o004000n,
	S_ISGID: 0o002000n,
	S_ISVTX: 0o001000n,
};

/** Normalize the successful kernel stat structure printed by strace -v. */
function statObservationDigest(line: string): Sha256Digest | undefined {
	const field = (name: string): bigint | undefined => parseInteger(new RegExp(`\\b${name}=(-?(?:0x[0-9a-f]+|0[0-7]+|[0-9]+))`, "i").exec(line)?.[1]);
	const device = (name: string): bigint | undefined => {
		const match = new RegExp(`\\b${name}=makedev\\(([^,]+),\\s*([^\\)]+)\\)`).exec(line);
		if (!match) return field(name) ?? (name === "st_rdev" ? 0n : undefined);
		const major = parseInteger(match[1]);
		const minor = parseInteger(match[2]);
		return major === undefined || minor === undefined ? undefined : linuxDevice(major, minor);
	};
	const modeText = /\bst_mode=([^,}]+)/.exec(line)?.[1]?.trim();
	const mode = modeText?.split("|").reduce<bigint | undefined>((combined, token) => {
		const bits = STAT_MODE_BITS[token] ?? parseInteger(token);
		return bits === undefined || combined === undefined ? undefined : combined | bits;
	}, 0n);
	const evidence = {
		dev: device("st_dev"),
		ino: field("st_ino"),
		mode,
		nlink: field("st_nlink"),
		uid: field("st_uid"),
		gid: field("st_gid"),
		rdev: device("st_rdev"),
		size: field("st_size"),
		blksize: field("st_blksize"),
		blocks: field("st_blocks"),
		atime: field("st_atime"),
		atimeNsec: field("st_atime_nsec"),
		mtime: field("st_mtime"),
		mtimeNsec: field("st_mtime_nsec"),
		ctime: field("st_ctime"),
		ctimeNsec: field("st_ctime_nsec"),
	};
	if (Object.values(evidence).some((value) => value === undefined)) return undefined;
	return filesystemObservationDigest({
		dev: evidence.dev!,
		ino: evidence.ino!,
		mode: evidence.mode!,
		nlink: evidence.nlink!,
		uid: evidence.uid!,
		gid: evidence.gid!,
		rdev: evidence.rdev!,
		size: evidence.size!,
		blksize: evidence.blksize!,
		blocks: evidence.blocks!,
		atimeNs: evidence.atime! * 1_000_000_000n + evidence.atimeNsec!,
		mtimeNs: evidence.mtime! * 1_000_000_000n + evidence.mtimeNsec!,
		ctimeNs: evidence.ctime! * 1_000_000_000n + evidence.ctimeNsec!,
	});
}

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
	return ((major & 0xfffn) << 8n) |
		(minor & 0xffn) |
		((minor & ~0xffn) << 12n) |
		((major & ~0xfffn) << 32n);
}

function resolveObservedPath(value: string, cwd: string): string {
	if (value.startsWith("/")) return path.posix.normalize(value);
	return path.posix.resolve(cwd, value);
}

function quotedStrings(line: string): string[] {
	const values: string[] = [];
	for (const match of line.matchAll(/"((?:\\.|[^"\\])*)"/g)) values.push(decodeCString(match[1]!));
	return values;
}

function decodeCString(value: string): string {
	let decoded = "";
	for (let index = 0; index < value.length; index++) {
		const character = value[index]!;
		if (character !== "\\") {
			decoded += character;
			continue;
		}
		const next = value[++index];
		if (next === undefined) break;
		if (next === "x") {
			const hex = value.slice(index + 1, index + 3);
			if (/^[0-9a-fA-F]{2}$/.test(hex)) {
				decoded += String.fromCharCode(Number.parseInt(hex, 16));
				index += 2;
				continue;
			}
		}
		if (/[0-7]/.test(next)) {
			const octal = `${next}${value.slice(index + 1).match(/^[0-7]{0,2}/)?.[0] ?? ""}`;
			decoded += String.fromCharCode(Number.parseInt(octal, 8));
			index += octal.length - 1;
			continue;
		}
		decoded += next === "n" ? "\n" : next === "r" ? "\r" : next === "t" ? "\t" : next;
	}
	return decoded;
}

function sharedObjectRole(observedPath: string, role: DependencyRole): DependencyRole {
	return role === "input" && /(?:^|\/)lib[^/]*\.so(?:\.|$)/.test(observedPath) ? "shared_object" : role;
}
