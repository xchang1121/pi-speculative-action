import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import type { DependencyRole, ProvenanceTaint } from "./provenance-certificate.ts";

interface ObservedProcessPath {
	readonly path: string;
	readonly role: DependencyRole;
}

export interface StraceObservation {
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	readonly incompleteReasons: readonly string[];
}

export interface StraceObservationOptions {
	/** Exec paths replaced by the process outlet. Their implementation subtree is not workload provenance. */
	readonly ignoredExecutablePaths?: readonly string[];
	/**
	 * Workspace roots whose driver-specific unsupported errors must invalidate adoption. This keeps
	 * a COW substrate from changing a command result when the Actor filesystem supports the syscall.
	 */
	readonly guardFilesystemSemanticsWithin?: readonly string[];
	/** Nondeterminism denied or virtualized by the execution provider, never by strace itself. */
	readonly coveredTaints?: readonly ProvenanceTaint[];
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
		for (const line of file.lines.slice(selected.get(pid) ?? 0)) {
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) {
				complete = false;
				incompleteReasons.add(`child_trace_missing:${child}`);
				continue;
			}
			selected.set(child, 0);
			queue.push(child);
		}
	}

	const paths = new Map<string, DependencyRole>();
	const taints = new Set<ProvenanceTaint>();
	const ignoredExecutables = new Set(
		(options.ignoredExecutablePaths ?? []).map((value) => path.posix.resolve(value)),
	);
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) => path.posix.resolve(value));
	const ignoredAfter = ignoredProcessSegments(selected, byPID, ignoredExecutables);
	for (const [pid, start] of selected) {
		const file = byPID.get(pid);
		if (!file) continue;
		let cwd = path.posix.resolve(initialCwd);
		const unfinished = new Map<string, number>();
		const end = ignoredAfter.get(pid) ?? file.lines.length;
		for (const line of file.lines.slice(start, end)) {
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
			if (
				UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) ||
				(syscall === "ioctl" && unmodeledFileIoctl(line))
			) {
				taints.add("unsupported_syscall");
			}
			if (semanticRoots.length && workspaceDriverSemanticGap(line, syscall, cwd, semanticRoots)) {
				complete = false;
				taints.add("unsupported_syscall");
				incompleteReasons.add(`filesystem_semantics:${syscall}:${pid}`);
			}
			if (syscall === "chdir" && syscallSucceeded(line)) {
				const value = quotedStrings(line)[0];
				if (value) cwd = resolveObservedPath(value, cwd);
				else {
					complete = false;
					incompleteReasons.add(`chdir_unparsed:${pid}`);
				}
			}
			if (syscall === "fchdir") {
				complete = false;
				incompleteReasons.add(`fchdir:${pid}`);
			}
			if (!FILE_SYSCALLS.has(syscall)) continue;
			const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
			for (const observed of syscallPaths(line, syscall, cwd)) {
				const existing = paths.get(observed);
				if (existing !== "executable") paths.set(observed, role);
			}
		}
		if (unfinished.size) {
			complete = false;
			for (const name of unfinished.keys()) incompleteReasons.add(`unfinished:${pid}:${name}`);
		}
	}
	if (!complete) taints.add("trace_incomplete");
	for (const covered of options.coveredTaints ?? []) taints.delete(covered);
	return {
		complete,
		paths: Object.freeze(
			[...paths].sort(([left], [right]) => left.localeCompare(right)).map(([observedPath, role]) => ({
				path: observedPath,
				role: sharedObjectRole(observedPath, role),
			})),
		),
		taints: Object.freeze([...taints].sort()),
		tracedProcesses: [...selected].filter(([pid, start]) => (ignoredAfter.get(pid) ?? Number.POSITIVE_INFINITY) > start)
			.length,
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
	"lstat",
	"mkdir",
	"mkdirat",
	"mknod",
	"mknodat",
	"mount",
	"newfstatat",
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
	"stat",
	"statfs",
	"statx",
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

function ignoredProcessSegments(
	selected: ReadonlyMap<number, number>,
	byPID: ReadonlyMap<number, TraceFile>,
	ignoredExecutables: ReadonlySet<string>,
): Map<number, number> {
	const ignoredAfter = new Map<number, number>();
	if (!ignoredExecutables.size) return ignoredAfter;
	const queue: number[] = [];
	for (const [pid, start] of selected) {
		const file = byPID.get(pid);
		if (!file) continue;
		for (let index = start; index < file.lines.length; index++) {
			const line = file.lines[index]!;
			if (!successfulExec(line)) continue;
			const executable = quotedStrings(line)[0];
			if (!executable || !ignoredExecutables.has(path.posix.resolve(executable))) continue;
			ignoredAfter.set(pid, index);
			queue.push(pid);
			break;
		}
	}
	while (queue.length) {
		const pid = queue.shift()!;
		const file = byPID.get(pid);
		if (!file) continue;
		const start = ignoredAfter.get(pid) ?? 0;
		for (const line of file.lines.slice(start)) {
			const child = spawnedPID(line);
			if (!child || ignoredAfter.has(child)) continue;
			ignoredAfter.set(child, 0);
			queue.push(child);
		}
	}
	return ignoredAfter;
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

function syscallPaths(line: string, syscall: string, cwd: string): readonly string[] {
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
