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
	ls: { sortsListings: true, rejected: /^(?:-[a-zA-Z]*[fU]|--sort=none)/ },
	find: { rejected: /^-(?:[acm](?:min|time)|used|newer[aBcmt]t|f?printf|f?ls)$/ },
	git: { sortsListings: true, rejected: /^--(?:relative-date|date=relative|since|until|after|before|min-age|max-age)\b/,
		subcommands: new Set(["status", "diff", "log", "show", "rev-parse", "ls-files", "branch", "grep", "blame", "cat-file"]) },
};
export const SHELLS: ReadonlySet<string> = new Set(["bash", "sh", "dash"]);
/** Programs whose output is a function of file contents: they stat a file only for its type, size hints or same-file checks. */
const CONTENT_READERS = new Set([...Object.keys(TOOLS).filter((name) => !["env", "test", "[", "ls", "find"].includes(name)), "rg", "awk", "gawk", "mawk"]);
const WITHOUT_DEVICE = FILESYSTEM_OBSERVATION_FIELDS.filter((field) => field !== "dev");
/** find predicates that read metadata beyond a file's type, with the fields they read; any other listing predicate reads all. */
const FIND_FIELDS: ReadonlyArray<readonly [RegExp, readonly FilesystemObservationField[]]> = [[/^-(?:size|empty)$/, ["size"]], [/^-perm$/, ["mode"]],
	[/^-(?:user|group|uid|gid|nouser|nogroup)$/, ["uid", "gid"]], [/^-links$/, ["nlink"]], [/^-(?:inum|samefile)$/, ["ino"]]];

/**
 * The stat fields of a workspace file that can reach a program's output. A sandbox serves the workspace from another device,
 * and a snapshot's inode numbers and times differ: only what the program reveals is a dependency. Undefined keeps every field.
 */
export function workspaceStatFields(image: string, argv: readonly string[]): readonly FilesystemObservationField[] | undefined {
	if (CONTENT_READERS.has(image) || image === "git") return ["mode"];
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
	return SHELLS.has(image) || ["test", "[", "make", "xargs", "cp", "mv", "rm", "ln", "mkdir", "rmdir", "touch", "chmod"].includes(image) ? WITHOUT_DEVICE : undefined;
}

/** The stat fields git reads of a directory outside the workspace while it discovers the repository: its device (the
 * filesystem boundary), owner (safe.directory) and type; never the times or link count its siblings change. */
export function hostStatFields(image: string): readonly FilesystemObservationField[] | undefined {
	return image === "git" ? ["dev", "mode", "uid", "gid"] : undefined;
}

/** Walkers whose output never depends on the filesystem type fts reads to pick its traversal. */
export const FILESYSTEM_TYPE_BLIND: ReadonlySet<string> = new Set(["find", "rm", "grep", "egrep", "fgrep"]);
/** Shell text that reads what differs between runs: special parameters, time formats, the time keyword and job pids. */
const VOLATILE_SHELL = /\$\{?(?:RANDOM|SRANDOM|BASHPID|SECONDS|EPOCHSECONDS|EPOCHREALTIME|PPID|\$|!)(?![A-Za-z0-9_])|%\(|(?:^|[\s;&|(])(?:times?|jobs)(?=[\s;&|)]|$)/;

export interface TracedExecution {
	readonly pid: number;
	readonly path?: string;
	readonly argv: readonly string[];
}

/** Whether a complete transcript ran only system tools whose output cannot depend on the one-shot inputs they read. */
export function repeatableExecutions(executions: readonly TracedExecution[], listingPIDs: ReadonlySet<number>, workspaceRoots: readonly string[]): boolean {
	if (!workspaceRoots.length || !executions.length) return false;
	const names = new Map<number, string[]>();
	for (const { pid, path: image, argv } of executions) {
		// A workspace file named like a tool is user code.
		if (!image || !path.posix.isAbsolute(image) || workspaceRoots.some((root) => image === root || image.startsWith(`${root}/`))) return false;
		const name = path.posix.basename(image), rule = TOOLS[name], script = argv.indexOf("-c");
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
