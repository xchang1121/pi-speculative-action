import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { containsLogicalPath } from "./path-utils.ts";
import {
	type DependencyRole,
	type FilesystemObservationEvidence,
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
	readonly lines: readonly TraceLine[];
}

type TraceRoot = { readonly file: TraceFile; readonly start: number };
interface TraceLine {
	readonly name: string;
	readonly args: readonly string[];
	readonly result: string;
	readonly failure?: string;
}
const TRACE_DELIMITERS: Readonly<Record<string, string>> = { "(": ")", "[": "]", "{": "}", "<": ">" };

/** Delimit once: quoted paths and descriptor annotations are data, never syscall syntax. */
function parseTraceLine(line: string): TraceLine {
	const head = /^\s*([a-zA-Z0-9_]+)\(/.exec(line);
	const empty = { name: head?.[1] ?? "", args: [], result: "" };
	if (!head && (!line.trim() || /^(?:\+\+\+|---)/.test(line))) return empty;
	const failure = { ...empty, failure: `syscall_unparsed:${empty.name}` };
	if (!head) return failure;
	const args: string[] = [], stack: string[] = [];
	let start = head[0].length, quoted = false;
	for (let index = start; index < line.length; index++) {
		const character = line[index]!;
		const context = stack.at(-1);
		if (character === "\\" && (quoted || context?.endsWith(">"))) { index++; continue; }
		if (quoted) { if (character === '"') quoted = false; continue; }
		// -yy sockets use -> for peers; filesystem paths escape literal angle brackets.
		if (context?.endsWith(">")) {
			if (character === "<") stack.push(">");
			if (character === ">" && (context === "/>" || line[index - 1] !== "-")) stack.pop();
			continue;
		}
		if (character === '"') { quoted = true; continue; }
		if (line.startsWith("/*", index)) {
			const end = line.indexOf("*/", index + 2);
			if (end < 0) return failure;
			index = end + 1;
			continue;
		}
		const closing = TRACE_DELIMITERS[character];
		if (closing) { stack.push(character === "<" && line[index + 1] === "/" ? "/>" : closing); continue; }
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

function reassembleSyscalls(lines: readonly string[], pid: number): readonly TraceLine[] {
	const pending = new Map<string, string[]>();
	const complete: TraceLine[] = [];
	for (const line of lines) {
		const unfinished = /^\s*([a-zA-Z0-9_]+)\(.*\s<unfinished \.\.\.>\s*$/.exec(line);
		if (unfinished) {
			const name = unfinished[1]!;
			(pending.get(name) ?? pending.set(name, []).get(name)!).push(line.replace(/\s*<unfinished \.\.\.>\s*$/, ""));
			continue;
		}
		const resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>(.*)$/.exec(line);
		if (!resumed) {
			complete.push(parseTraceLine(line));
			continue;
		}
		const name = resumed[1]!;
		const queue = pending.get(name);
		const prefix = queue?.shift();
		if (!prefix) complete.push({ name, args: [], result: "", failure: `resumed_without_unfinished:${pid}:${name}` });
		else complete.push(parseTraceLine(`${prefix}${resumed[2]}`));
		if (queue?.length === 0) pending.delete(name);
	}
	for (const name of pending.keys()) complete.push({ name, args: [], result: "", failure: `unfinished:${pid}:${name}` });
	return complete;
}

function selectTraceRoot(files: readonly TraceFile[], target: string): TraceRoot | { readonly reason: string } {
	const byPID = new Map(files.map((file) => [file.pid, file]));
	const parent = new Map<number, number>();
	const children = new Map<number, number[]>();
	for (const file of files) {
		for (const line of file.lines) {
			const child = spawnedPID(line);
			if (!child) continue;
			const existing = parent.get(child);
			if (existing !== undefined && existing !== file.pid) return { reason: `process_parent_ambiguous:${child}` };
			parent.set(child, file.pid);
			(children.get(file.pid) ?? children.set(file.pid, []).get(file.pid)!).push(child);
		}
	}
	const roots = files.filter((file) => !parent.has(file.pid));
	if (roots.length !== 1) return { reason: `trace_root_ambiguous:${roots.map(({ pid }) => pid).sort().join(",")}` };

	const depth = new Map<number, number>([[roots[0]!.pid, 0]]);
	const queue = [roots[0]!.pid];
	while (queue.length) {
		const pid = queue.shift()!;
		for (const child of children.get(pid) ?? []) {
			if (!byPID.has(child) || depth.has(child)) continue;
			depth.set(child, depth.get(pid)! + 1);
			queue.push(child);
		}
	}
	const candidates: Array<TraceRoot & { readonly depth: number }> = [];
	for (const file of files) {
		const processDepth = depth.get(file.pid);
		if (processDepth === undefined) continue;
		const start = file.lines.findIndex((line) => {
			if (!successfulExec(line)) return false;
			const executable = quotedStrings(line)[0];
			return executable !== undefined && path.posix.resolve(executable) === target;
		});
		if (start >= 0) candidates.push({ file, start, depth: processDepth });
	}
	if (!candidates.length) return { reason: "target_exec_not_found" };
	const shallowest = Math.min(...candidates.map((candidate) => candidate.depth));
	const matches = candidates.filter((candidate) => candidate.depth === shallowest);
	return matches.length === 1 ? matches[0]! : { reason: `target_exec_ambiguous:${matches.map(({ file }) => file.pid).sort().join(",")}` };
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
		files.push({ pid, lines: reassembleSyscalls(contents.split(/\r?\n/), pid) });
	}
	const target = path.posix.resolve(executablePath);
	const root = selectTraceRoot(files, target);
	if ("reason" in root) {
		return {
			complete: false,
			paths: [],
			taints: ["trace_incomplete"],
			tracedProcesses: 0,
			incompleteReasons: [root.reason],
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
		for (let index = start; index < file.lines.length; index++) {
			if (ignoredSegments.get(pid)?.some(([from, to]) => index >= from && index < to)) continue;
			const line = file.lines[index]!;
			if (!line) continue;
			const traceFailure = line.failure;
			if (traceFailure) {
				complete = false;
				incompleteReasons.add(traceFailure);
				continue;
			}
			const syscall = line.name;
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
					const digest = statObservationDigest(line.args[syscall === "newfstatat" ? 2 : 1] ?? "");
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
const DRIVER_SEMANTIC_GAP_RESULT = /^-1\s+(?:EXDEV|EOPNOTSUPP|ENOTSUP|ENOSYS)\b/;

function unmodeledFileIoctl(line: TraceLine): boolean {
	if (!syscallSucceeded(line)) return false;
	return UNMODELED_MUTATING_IOCTL.test(line.args[1] ?? "") || absoluteDescriptorPath(line.args[0]) !== undefined;
}

function workspaceDriverSemanticGap(
	line: TraceLine,
	syscall: string,
	cwd: string,
	roots: readonly string[],
): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line.result)) return false;
	const referenced = new Set(syscallPaths(line, syscall, cwd));
	for (const argument of line.args) {
		const target = absoluteDescriptorPath(argument);
		if (target) referenced.add(target);
	}
	return [...referenced].some((candidate) => roots.some((root) => containsLogicalPath(root, candidate)));
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

function resourceLimitMutation(line: TraceLine, syscall: string): boolean {
	if (!syscallSucceeded(line)) return false;
	return syscall === "setrlimit" || (syscall === "prlimit64" && line.args[2] !== "NULL");
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

function tracedCwd(line: TraceLine, cwd: string | undefined): string | undefined {
	if (!syscallSucceeded(line)) return cwd;
	if (line.name === "fchdir") return absoluteDescriptorPath(line.args[0]);
	if (line.name !== "chdir") return cwd;
	const target = quotedStrings(line)[0];
	return target ? tracedPath(target, cwd) : undefined;
}

function tracedPath(target: string, cwd: string | undefined): string | undefined {
	return path.posix.isAbsolute(target) ? path.posix.resolve(target) : cwd ? path.posix.resolve(cwd, target) : undefined;
}

function successfulExec(line: TraceLine): boolean {
	return (line.name === "execve" || line.name === "execveat") && syscallSucceeded(line);
}

function spawnedPID(line: TraceLine): number | undefined {
	if (!["clone", "clone3", "fork", "vfork"].includes(line.name)) return undefined;
	const pid = Number(line.result);
	return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
}

function syscallSucceeded(line: TraceLine): boolean {
	return /^(?:0x[0-9a-f]+|[0-9]+)(?:\b|<)/i.test(line.result);
}

function confinementDenied(line: TraceLine): boolean {
	return /^-1 (?:EACCES|EPERM)\b/.test(line.result);
}

function prctlConfinementSensitive(line: TraceLine, syscall: string): boolean {
	return syscall === "prctl" && !/^PR_SET_(?:NAME|VMA)\b/.test(line.args[0] ?? "");
}

function processLimitDenied(line: TraceLine, syscall: string): boolean {
	return ["clone", "clone3", "fork", "vfork"].includes(syscall) && /^-1 EAGAIN\b/.test(line.result);
}

function syscallPaths(line: TraceLine, syscall: string, cwd: string): readonly string[] {
	if (syscall === "fchdir") {
		const target = absoluteDescriptorPath(line.args[0]);
		return target ? [target] : [];
	}
	const quoted = quotedStrings(line);
	if (!quoted.length) return [];
	let values: readonly string[];
	if (syscall === "symlink" || syscall === "symlinkat") values = quoted.slice(-1);
	else if (["rename", "renameat", "renameat2", "link", "linkat"].includes(syscall)) values = quoted.slice(0, 2);
	else values = quoted.slice(0, 1);
	const dirfd = /^(?:openat2?|newfstatat|statx|faccessat2?|readlinkat|mkdirat|unlinkat|execveat)$/.test(syscall) ? line.args[0] : undefined;
	const base = absoluteDescriptorPath(dirfd) ?? cwd;
	return values
		.filter((value) => value.length > 0)
		.map((value) => resolveObservedPath(value, base));
}

function absoluteDescriptorPath(descriptor: string | undefined): string | undefined {
	const target = /^\d+<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
	return target?.startsWith("/") && !target.endsWith(" (deleted)") ? path.posix.normalize(decodeCString(target)) : undefined;
}

function metadataSyscallPaths(
	line: TraceLine,
	syscall: string,
	cwd: string,
): readonly { readonly path: string; readonly followSymlinks: boolean }[] {
	const followSymlinks = syscall !== "lstat" && !(syscall === "newfstatat" && /\bAT_SYMLINK_NOFOLLOW\b/.test(line.args[3] ?? ""));
	const paths = syscall === "fstat" ? [] : syscallPaths(line, syscall, cwd);
	if (paths.length) return paths.map((observedPath) => ({ path: observedPath, followSymlinks }));
	if (syscall !== "fstat" && syscall !== "newfstatat") return [];
	const descriptorPath = absoluteDescriptorPath(line.args[0]);
	return descriptorPath ? [{ path: descriptorPath, followSymlinks }] : [];
}

/** Non-path descriptors are already typed in the process key, but their kernel identity is volatile. */
function descriptorTarget(line: TraceLine): boolean {
	const target = /^\d+<(.+)>$/.exec(line.args[0] ?? "")?.[1];
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
	const time = (name: string): bigint | undefined => {
		const seconds = field(name), nanos = field(`${name}_nsec`);
		return seconds === undefined || nanos === undefined ? undefined : seconds * 1_000_000_000n + nanos;
	};
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
		atimeNs: time("st_atime"),
		mtimeNs: time("st_mtime"),
		ctimeNs: time("st_ctime"),
	};
	if (Object.values(evidence).some((value) => value === undefined)) return undefined;
	return filesystemObservationDigest(evidence as FilesystemObservationEvidence);
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

function quotedStrings(line: TraceLine): string[] {
	return line.args.flatMap((argument) => {
		const match = /^"((?:\\.|[^"\\])*)"$/.exec(argument);
		return match ? [decodeCString(match[1]!)] : [];
	});
}

function decodeCString(value: string): string {
	const chunks: Buffer[] = [];
	let start = 0;
	for (const match of value.matchAll(/\\(?:x([0-9a-fA-F]{2})|([0-7]{1,3})|(.))/g)) {
		chunks.push(Buffer.from(value.slice(start, match.index), "utf8"));
		const byte = match[1] ? Number.parseInt(match[1], 16) : match[2] ? Number.parseInt(match[2], 8) :
			({ a: 7, b: 8, f: 12, n: 10, r: 13, t: 9, v: 11 } as Record<string, number>)[match[3]!] ?? match[3]!.charCodeAt(0);
		chunks.push(Buffer.from([byte]));
		start = match.index + match[0].length;
	}
	chunks.push(Buffer.from(value.slice(start), "utf8"));
	// strace escapes bytes, not Unicode code points. Refuse identities Node cannot represent losslessly.
	return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Buffer.concat(chunks));
}

function sharedObjectRole(observedPath: string, role: DependencyRole): DependencyRole {
	return role === "input" && /(?:^|\/)lib[^/]*\.so(?:\.|$)/.test(observedPath) ? "shared_object" : role;
}
