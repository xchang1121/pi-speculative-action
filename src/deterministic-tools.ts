import path from "node:path";
import { FILESYSTEM_OBSERVATION_FIELDS, type FilesystemObservationField } from "./provenance-certificate.ts";

interface ToolRule {
	/** Orders a directory listing itself, so readdir order never reaches its output. */
	readonly sortsListings?: true;
	/** Arguments that read the clock or randomness, or reorder output by scheduling. */
	readonly rejected?: RegExp;
	readonly subcommands?: ReadonlySet<string>;
}

/**
 * Read-only tools whose output is a function of their observed inputs: the clock, random and pid reads every process makes,
 * and a descriptor's volatile identity, never reach it. `ls -l` switches its date format for files six months old.
 */
const TOOLS: Readonly<Record<string, ToolRule>> = {
	...Object.fromEntries(["cat", "head", "tail", "wc", "grep", "egrep", "fgrep", "sed", "cut", "tr", "nl", "uniq", "diff", "cmp", "tac", "rev", "paste",
		"comm", "join", "fold", "expand", "column", "basename", "dirname", "echo", "printf", "true", "false", "test", "[", "pwd", "readlink", "realpath", "env"]
		.map((name) => [name, {}])),
	sort: { rejected: /^(?:-[a-zA-Z]*R|--random-)/ },
	xargs: { rejected: /^(?:-P|--max-procs)/ },
	ls: { sortsListings: true, rejected: /^(?:-[a-zA-Z]*[fUlgon]|--sort=none|--format=(?:long|verbose))/ },
	find: { rejected: /^-(?:[acm](?:min|time)|used|newer[aBcmt]t|f?printf|f?ls)$/ },
	git: { sortsListings: true, rejected: /^--(?:relative-date|date=relative|since|until|after|before|min-age|max-age)\b/,
		subcommands: new Set(["status", "diff", "rev-parse", "ls-files", "grep", "cat-file"]) },
};
export const SHELLS: ReadonlySet<string> = new Set(["bash", "sh", "dash"]);
/** Programs whose output is a function of file contents: they stat a file only for its type, size hints or same-file checks. */
const CONTENT_READERS = new Set([...Object.keys(TOOLS).filter((name) => !["env", "test", "[", "ls", "find", "git"].includes(name)), "rg", "awk", "gawk", "mawk"]);
const WITHOUT_DEVICE = FILESYSTEM_OBSERVATION_FIELDS.filter((field) => field !== "dev");
/** find predicates that read metadata beyond a file's type, with the fields they read; any other listing predicate reads all. */
const FIND_FIELDS: ReadonlyArray<readonly [RegExp, readonly FilesystemObservationField[]]> = [[/^-(?:size|empty)$/, ["size"]], [/^-perm$/, ["mode"]],
	[/^-(?:user|group|uid|gid|nouser|nogroup)$/, ["uid", "gid"]], [/^-links$/, ["nlink"]], [/^-(?:inum|samefile)$/, ["ino"]]];
/** The fields each of stat's format directives prints; any other directive, its default and terse output and file system mode print more. */
const STAT_DIRECTIVES: Readonly<Record<string, readonly FilesystemObservationField[]>> = Object.fromEntries(([["aAfF", ["mode"]], ["s", ["size"]], ["b", ["blocks"]],
	["o", ["blksize"]], ["h", ["nlink"]], ["i", ["ino"]], ["dDm", ["dev"]], ["uU", ["uid"]], ["gG", ["gid"]], ["yY", ["mtimeNs"]], ["zZ", ["ctimeNs"]], ["tT", ["rdev"]],
	["nNB%", []]] as const).flatMap(([directives, fields]) => [...directives].map((directive) => [directive, fields])));

/**
 * The stat fields of a workspace file that can reach a program's output. A sandbox serves the workspace from another device,
 * and a snapshot's inode numbers and times differ: only what the program reveals is a dependency. Undefined keeps every field.
 */
export function workspaceStatFields(image: string, argv: readonly string[]): readonly FilesystemObservationField[] | undefined {
	if (CONTENT_READERS.has(image)) return ["mode"];
	if (image === "find") {
		const fields = new Set<FilesystemObservationField>(["mode"]);
		if (argv.some((argument) => argument.includes("%D"))) return undefined;
		for (const argument of argv.slice(1)) {
			if (/^-(?:[acm](?:min|time)|used|newer\w*|f?printf|f?ls)$/.test(argument)) return WITHOUT_DEVICE;
			for (const [predicate, read] of FIND_FIELDS) if (predicate.test(argument)) for (const field of read) fields.add(field);
		}
		return [...fields];
	}
	if (image === "ls") return argv.slice(1).some((argument) => /^-[a-zA-Z]*[lgonsiStcu]|^--(?:full-time|size|inode|sort|time)/.test(argument)) ? WITHOUT_DEVICE : ["mode"];
	if (image === "stat") {
		const at = argv.findIndex((argument) => /^(?:-c|--format|--printf)(?:=|$)|^-c./.test(argument)), option = argv[at] ?? "";
		const format = at < 0 || argv.some((argument) => /^(?:-[a-zA-Z]*[ft]|--file-system|--terse)$/.test(argument)) ? undefined
			: /^-c./.test(option) ? option.slice(2) : option.includes("=") ? option.slice(option.indexOf("=") + 1) : argv[at + 1];
		const read = format === undefined ? [undefined] : [...format.matchAll(/%[-#+ 0-9.']*(.)/g)].map(([, directive]) => STAT_DIRECTIVES[directive!]);
		return read.every(Boolean) ? FILESYSTEM_OBSERVATION_FIELDS.filter((field) => field === "mode" || read.some((fields) => fields!.includes(field))) : undefined;
	}
	// du needs device and inode to distinguish hard links from different files with equal sizes and link counts.
	if (image === "du") return argv.some((argument) => argument.startsWith("--time")) ? undefined : ["mode", "dev", "ino", "nlink", "size", "blocks"];
	return image === "git" ? FILESYSTEM_OBSERVATION_FIELDS.filter((field) => !["dev", "ino", "blksize", "blocks", "ctimeNs"].includes(field)) : undefined;
}

/** Directory traversal contracts apply only to known readers; unknown programs may reveal every field. */
const DIRECTORY_METADATA_READERS = new Set(["ls", "find", "stat", "du", "tree"]);
export function directoryStatFields(image: string, fields: readonly FilesystemObservationField[] | undefined, workspace: boolean) {
	return image !== "git" && !CONTENT_READERS.has(image) && !SHELLS.has(image) || DIRECTORY_METADATA_READERS.has(image) ? fields : (fields ?? FILESYSTEM_OBSERVATION_FIELDS)
		.filter((field) => ["mode", "uid", "gid", ...workspace ? [] : ["dev", "ino"]].includes(field));
}

/** The stat fields git reads of a directory outside the workspace while it discovers the repository: its device (the
 * filesystem boundary), owner (safe.directory) and type; never the times or link count its siblings change. */
export function hostStatFields(image: string): readonly FilesystemObservationField[] | undefined {
	return image === "git" ? ["dev", "mode", "uid", "gid"] : undefined;
}

/** Shell text that reads what differs between runs: special parameters, time formats, the time keyword and job pids. */
const VOLATILE_SHELL = /\$\{?(?:RANDOM|SRANDOM|BASHPID|SECONDS|EPOCHSECONDS|EPOCHREALTIME|PPID|\$|!)(?![A-Za-z0-9_])|%\(|(?:^|[\s;&|(])(?:times?|jobs)(?=[\s;&|)]|$)/;

export interface TracedExecution {
	readonly pid: number;
	readonly path?: string;
	readonly argv: readonly string[];
}

/** Contracts apply to the platform's system tools, never a user executable sharing their basename. */
export function systemToolName(image: string | undefined, workspaceRoots: readonly string[]): string {
	return image && /^\/(?:usr\/)?bin\/[^/]+$/.test(image) && !workspaceRoots.some(root => image === root || image.startsWith(`${root}/`)) ? path.posix.basename(image) : "";
}

/** Whether a complete transcript ran only system tools whose output cannot depend on the one-shot inputs they read. */
export function repeatableExecutions(executions: readonly TracedExecution[], listingPIDs: ReadonlySet<number>, workspaceRoots: readonly string[]): boolean {
	if (!workspaceRoots.length || !executions.length) return false;
	const names = new Map<number, string[]>();
	for (const { pid, path: image, argv } of executions) {
		// A workspace file named like a tool is user code.
		if (!image || !path.posix.isAbsolute(image) || workspaceRoots.some((root) => image === root || image.startsWith(`${root}/`))) return false;
		const name = systemToolName(image, workspaceRoots), rule = TOOLS[name], script = argv.indexOf("-c");
		if (SHELLS.has(name) ? script < 1 || script + 1 >= argv.length || VOLATILE_SHELL.test(argv[script + 1]!)
			: !rule || argv.slice(1).some((argument) => rule.rejected?.test(argument)) || rule.subcommands && !rule.subcommands.has(gitSubcommand(argv) ?? "")) return false;
		names.set(pid, [...(names.get(pid) ?? []), name]);
	}
	// Readdir order is volatile: only a shell's sorted glob or a tool sorting its listing may read a directory.
	return [...listingPIDs].every((pid) => (names.get(pid) ?? ["sh"]).every((name) => SHELLS.has(name) || TOOLS[name]?.sortsListings));
}

function gitSubcommand(argv: readonly string[]): string | undefined {
	for (let index = 1; index < argv.length; index++) {
		if (argv[index] === "-C" || argv[index] === "-c") index++;
		else if (!argv[index]!.startsWith("-")) return argv[index];
	}
	return undefined;
}
