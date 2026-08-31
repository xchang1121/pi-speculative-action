import { createHash } from "node:crypto";
import { lstat, readFile, readdir, readlink } from "node:fs/promises";
import path from "node:path";
import type { DynamicDependency, Sha256Digest } from "./provenance-certificate.ts";
import { digestObject, sha256Digest } from "./provenance-certificate.ts";

export interface ExecutionPathProjectionOptions {
	readonly sourceRoot: string;
	readonly workspaceRoot: string;
	readonly privateRoot?: string;
}

/** Stable logical paths keep certificates independent of disposable worktree names. */
export class ExecutionPathProjection {
	readonly sourceRoot: string;
	readonly workspaceRoot: string;
	readonly privateRoot?: string;

	constructor(options: ExecutionPathProjectionOptions) {
		this.sourceRoot = path.resolve(options.sourceRoot);
		this.workspaceRoot = path.resolve(options.workspaceRoot);
		this.privateRoot = options.privateRoot ? path.resolve(options.privateRoot) : undefined;
	}

	toLogical(physicalPath: string): string {
		const physical = path.resolve(physicalPath);
		if (!pathContains(this.workspaceRoot, physical)) return slash(physical);
		return slash(path.join(this.sourceRoot, path.relative(this.workspaceRoot, physical)));
	}

	toPhysical(logicalPath: string): string | undefined {
		const logical = path.resolve(logicalPath);
		if (!pathContains(this.sourceRoot, logical)) return logical;
		const physical = path.resolve(this.workspaceRoot, path.relative(this.sourceRoot, logical));
		return pathContains(this.workspaceRoot, physical) ? physical : undefined;
	}

	normalizeValue(value: string): string {
		let normalized = replacePath(value, this.workspaceRoot, this.sourceRoot);
		if (this.privateRoot) normalized = replacePath(normalized, this.privateRoot, "/.pi-private-world");
		return slash(normalized);
	}

	isWorkspacePhysical(physicalPath: string): boolean {
		return pathContains(this.workspaceRoot, path.resolve(physicalPath));
	}
}

export type WorkspaceTreeEntry =
	| {
			readonly kind: "file";
			readonly digest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
			readonly size: number;
			readonly content?: Buffer;
	  }
	| {
			readonly kind: "directory";
			readonly entriesDigest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
	  }
	| {
			readonly kind: "symlink";
			readonly target: string;
			readonly targetDigest: Sha256Digest;
	  }
	| { readonly kind: "unsupported"; readonly type: string };

export interface WorkspaceTreeSnapshot {
	readonly root: string;
	readonly entries: ReadonlyMap<string, WorkspaceTreeEntry>;
	readonly files: number;
	readonly bytesRead: number;
	readonly complete: boolean;
}

export interface WorkspaceTreeCaptureOptions {
	readonly includeFileContent?: boolean;
	readonly maxFiles?: number;
	readonly maxBytes?: number;
	readonly exclude?: readonly string[];
}

/** Bounded, symlink-preserving snapshot used only for nested-process effect attribution. */
export async function captureWorkspaceTree(
	root: string,
	options: WorkspaceTreeCaptureOptions = {},
): Promise<WorkspaceTreeSnapshot> {
	const absoluteRoot = path.resolve(root);
	const entries = new Map<string, WorkspaceTreeEntry>();
	const excludes = new Set(options.exclude ?? [".git"]);
	const maxFiles = Math.max(1, options.maxFiles ?? 100_000);
	const maxBytes = Math.max(0, options.maxBytes ?? 512 * 1024 * 1024);
	let files = 0;
	let bytesRead = 0;
	let complete = true;
	const rootStat = await lstat(absoluteRoot);
	const rootChildren = await readdir(absoluteRoot, { withFileTypes: true });
	entries.set("", {
		kind: "directory",
		entriesDigest: directoryEntriesDigest(rootChildren.filter((entry) => !excludes.has(entry.name))),
		metadataDigest: statMetadataDigest(rootStat),
		mode: rootStat.mode & 0o777,
	});

	const visit = async (directory: string, relativeDirectory: string): Promise<void> => {
		if (!complete) return;
		const children = await readdir(directory, { withFileTypes: true });
		for (const child of children.sort((left, right) => left.name.localeCompare(right.name))) {
			const relative = relativeDirectory ? path.join(relativeDirectory, child.name) : child.name;
			if (!relativeDirectory && excludes.has(child.name)) continue;
			const target = path.join(directory, child.name);
			const stat = await lstat(target);
			if (++files > maxFiles) {
				complete = false;
				return;
			}
			if (stat.isSymbolicLink()) {
				const linkTarget = await readlink(target);
				entries.set(relative, {
					kind: "symlink",
					target: linkTarget,
					targetDigest: sha256Digest(Buffer.from(linkTarget, "utf8")),
				});
				continue;
			}
			if (stat.isDirectory()) {
				const names = await readdir(target, { withFileTypes: true });
				entries.set(relative, {
					kind: "directory",
					entriesDigest: directoryEntriesDigest(names),
					metadataDigest: statMetadataDigest(stat),
					mode: stat.mode & 0o777,
				});
				await visit(target, relative);
				continue;
			}
			if (stat.isFile()) {
				if (bytesRead + stat.size > maxBytes) {
					complete = false;
					return;
				}
				const content = await readFile(target);
				bytesRead += content.byteLength;
				entries.set(relative, {
					kind: "file",
					digest: sha256Digest(content),
					metadataDigest: statMetadataDigest(stat),
					mode: stat.mode & 0o777,
					size: content.byteLength,
					...(options.includeFileContent ? { content } : {}),
				});
				continue;
			}
			entries.set(relative, { kind: "unsupported", type: specialFileType(stat) });
		}
	};

	await visit(absoluteRoot, "");
	return Object.freeze({ root: absoluteRoot, entries, files, bytesRead, complete });
}

export interface WorkspaceRegularEffect {
	readonly kind: "write" | "delete";
	readonly logicalPath: string;
	readonly relativePath: string;
	readonly before?: WorkspaceTreeEntry;
	readonly after?: Extract<WorkspaceTreeEntry, { kind: "file" }>;
}

export interface WorkspaceEffectDiff {
	readonly effects: readonly WorkspaceRegularEffect[];
	readonly complete: boolean;
	readonly reason?: string;
}

/** Final-state effect bundle. Unsupported inode semantics make the certificate non-replayable. */
export function diffWorkspaceTrees(
	before: WorkspaceTreeSnapshot,
	after: WorkspaceTreeSnapshot,
	projection: ExecutionPathProjection,
): WorkspaceEffectDiff {
	if (!before.complete || !after.complete) return { effects: [], complete: false, reason: "snapshot_limit" };
	const effects: WorkspaceRegularEffect[] = [];
	const names = [...new Set([...before.entries.keys(), ...after.entries.keys()])].sort();
	for (const relativePath of names) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		if (sameTreeEntry(previous, current)) continue;
		const logicalPath = projection.toLogical(path.join(after.root, relativePath));
		if (!current) {
			if (previous?.kind === "file") {
				effects.push({ kind: "delete", logicalPath, relativePath, before: previous });
				continue;
			}
			if (previous?.kind === "directory" && directoryHasChildren(before, relativePath)) continue;
			return { effects: [], complete: false, reason: `unsupported_delete:${relativePath}` };
		}
		if (current.kind === "directory") {
			if (!previous && directoryHasChildren(after, relativePath)) continue;
			if (previous?.kind === "directory") continue;
			return { effects: [], complete: false, reason: `unsupported_directory:${relativePath}` };
		}
		if (current.kind !== "file" || !current.content) {
			return { effects: [], complete: false, reason: `unsupported_inode:${relativePath}` };
		}
		if (previous && previous.kind !== "file") {
			return { effects: [], complete: false, reason: `inode_type_changed:${relativePath}` };
		}
		effects.push({ kind: "write", logicalPath, relativePath, ...(previous ? { before: previous } : {}), after: current });
	}
	return { effects: Object.freeze(effects), complete: true };
}

export function snapshotDependency(
	logicalPath: string,
	entry: WorkspaceTreeEntry | undefined,
	parent: WorkspaceTreeEntry | undefined,
	role: Extract<DynamicDependency, { kind: "file" }>["role"] = "input",
): DynamicDependency | undefined {
	if (!entry) {
		return {
			kind: "absence",
			path: logicalPath,
			...(parent?.kind === "directory" ? { parentEntriesDigest: parent.entriesDigest } : {}),
		};
	}
	switch (entry.kind) {
		case "file":
			return {
				kind: "file",
				path: logicalPath,
				role,
				contentDigest: entry.digest,
				metadataDigest: entry.metadataDigest,
			};
		case "directory":
			return {
				kind: "directory",
				path: logicalPath,
				entriesDigest: entry.entriesDigest,
				metadataDigest: entry.metadataDigest,
			};
		case "symlink":
			return { kind: "symlink", path: logicalPath, target: entry.target, targetDigest: entry.targetDigest };
		case "unsupported":
			return undefined;
	}
}

export function relativeSnapshotEntry(snapshot: WorkspaceTreeSnapshot, physicalPath: string): WorkspaceTreeEntry | undefined {
	const relative = path.relative(snapshot.root, path.resolve(physicalPath));
	if (relative.startsWith("..") || path.isAbsolute(relative)) return undefined;
	return snapshot.entries.get(relative);
}

export function parentSnapshotEntry(snapshot: WorkspaceTreeSnapshot, physicalPath: string): WorkspaceTreeEntry | undefined {
	return relativeSnapshotEntry(snapshot, path.dirname(physicalPath));
}

function sameTreeEntry(left: WorkspaceTreeEntry | undefined, right: WorkspaceTreeEntry | undefined): boolean {
	if (!left || !right || left.kind !== right.kind) return left === right;
	switch (left.kind) {
		case "file": {
			const value = right as Extract<WorkspaceTreeEntry, { kind: "file" }>;
			return left.digest === value.digest && left.metadataDigest === value.metadataDigest;
		}
		case "directory": {
			const value = right as Extract<WorkspaceTreeEntry, { kind: "directory" }>;
			return left.entriesDigest === value.entriesDigest && left.metadataDigest === value.metadataDigest;
		}
		case "symlink":
			return left.targetDigest === (right as Extract<WorkspaceTreeEntry, { kind: "symlink" }>).targetDigest;
		case "unsupported":
			return left.type === (right as Extract<WorkspaceTreeEntry, { kind: "unsupported" }>).type;
	}
}

function directoryHasChildren(snapshot: WorkspaceTreeSnapshot, relativePath: string): boolean {
	const prefix = `${relativePath}${path.sep}`;
	return [...snapshot.entries.keys()].some((name) => name.startsWith(prefix));
}

function directoryEntriesDigest(entries: readonly { readonly name: string; isFile(): boolean; isDirectory(): boolean; isSymbolicLink(): boolean; isSocket(): boolean; isFIFO(): boolean; isCharacterDevice(): boolean; isBlockDevice(): boolean }[]): Sha256Digest {
	return digestObject(
		entries
			.map((entry) => `${direntType(entry)}\0${entry.name}`)
			.sort(),
	);
}

function direntType(entry: Parameters<typeof directoryEntriesDigest>[0][number]): string {
	return entry.isFile()
		? "file"
		: entry.isDirectory()
			? "directory"
			: entry.isSymbolicLink()
				? "symlink"
				: entry.isSocket()
					? "socket"
					: entry.isFIFO()
						? "fifo"
						: entry.isCharacterDevice()
							? "char"
							: entry.isBlockDevice()
								? "block"
								: "other";
}

function statMetadataDigest(stat: Awaited<ReturnType<typeof lstat>>): Sha256Digest {
	return digestObject({
		mode: stat.mode,
		uid: stat.uid,
		gid: stat.gid,
		size: stat.size,
		type: stat.isFile() ? "file" : stat.isDirectory() ? "directory" : stat.isSymbolicLink() ? "symlink" : "other",
	});
}

function specialFileType(stat: Awaited<ReturnType<typeof lstat>>): string {
	return stat.isSocket()
		? "socket"
		: stat.isFIFO()
			? "fifo"
			: stat.isCharacterDevice()
				? "char"
				: stat.isBlockDevice()
					? "block"
					: "other";
}

function replacePath(value: string, from: string, to: string): string {
	const variants = [from, slash(from)];
	let replaced = value;
	for (const variant of variants) replaced = replaced.split(variant).join(to);
	return replaced;
}

function pathContains(root: string, target: string): boolean {
	const relative = path.relative(root, target);
	return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function slash(value: string): string {
	return value.replaceAll("\\", "/");
}

/** Streaming helper used by tests and trace readers without buffering a second copy. */
export function digestBytes(chunks: readonly Uint8Array[]): Sha256Digest {
	const hash = createHash("sha256");
	for (const chunk of chunks) hash.update(chunk);
	return `sha256:${hash.digest("hex")}`;
}
