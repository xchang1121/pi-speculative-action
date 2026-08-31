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
			readonly links: number;
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

export type WorkspaceStructureEntry =
	| {
			readonly kind: "file";
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
			readonly size: number;
			readonly links: number;
	  }
	| Exclude<WorkspaceTreeEntry, { readonly kind: "file" }>;

interface WorkspaceSnapshot<Entry> {
	readonly root: string;
	readonly entries: ReadonlyMap<string, Entry>;
	readonly files: number;
	readonly bytesRead: number;
	readonly complete: boolean;
}

export interface WorkspaceTreeSnapshot extends WorkspaceSnapshot<WorkspaceTreeEntry> {}

export interface WorkspaceStructureSnapshot extends WorkspaceSnapshot<WorkspaceStructureEntry> {}

export interface WorkspaceTreeCaptureOptions {
	readonly includeFileContent?: boolean;
	readonly maxFiles?: number;
	readonly maxBytes?: number;
	readonly exclude?: readonly string[];
}

export type WorkspaceStructureCaptureOptions = Pick<WorkspaceTreeCaptureOptions, "maxFiles" | "exclude">;

/** Bounded, symlink-preserving snapshot used only for nested-process effect attribution. */
export async function captureWorkspaceTree(
	root: string,
	options: WorkspaceTreeCaptureOptions = {},
): Promise<WorkspaceTreeSnapshot> {
	return captureWorkspaceSnapshot(root, options, options.includeFileContent ? "content" : "digest") as Promise<WorkspaceTreeSnapshot>;
}

/** Capture inode and directory semantics without reading regular-file contents. */
export async function captureWorkspaceStructure(
	root: string,
	options: WorkspaceStructureCaptureOptions = {},
): Promise<WorkspaceStructureSnapshot> {
	return captureWorkspaceSnapshot(root, options, "none") as Promise<WorkspaceStructureSnapshot>;
}

async function captureWorkspaceSnapshot(
	root: string,
	options: WorkspaceTreeCaptureOptions,
	fileCapture: "none" | "digest" | "content",
): Promise<WorkspaceSnapshot<WorkspaceTreeEntry | WorkspaceStructureEntry>> {
	const absoluteRoot = path.resolve(root);
	const entries = new Map<string, WorkspaceTreeEntry | WorkspaceStructureEntry>();
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
				if (fileCapture === "none") {
					entries.set(relative, {
						kind: "file",
						metadataDigest: statMetadataDigest(stat),
						mode: stat.mode & 0o777,
						size: stat.size,
						links: stat.nlink,
					});
					continue;
				}
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
					links: stat.nlink,
					...(fileCapture === "content" ? { content } : {}),
				});
				continue;
			}
			entries.set(relative, { kind: "unsupported", type: specialFileType(stat) });
		}
	};

	await visit(absoluteRoot, "");
	return Object.freeze({ root: absoluteRoot, entries, files, bytesRead, complete });
}

export interface WorkspaceRegularDelta {
	readonly relativePath: string;
	readonly before?: Uint8Array;
	readonly after?: Uint8Array;
	readonly beforeMode?: number;
	readonly afterMode?: number;
}

/**
 * Join a content-addressed transaction delta with content-free inode snapshots. The delta is the
 * authority for regular-file bytes; the snapshots prove that no unsupported inode or metadata
 * transition was hidden by a content-only change detector.
 */
export function diffWorkspaceStructures(
	before: WorkspaceStructureSnapshot,
	after: WorkspaceStructureSnapshot,
	deltas: readonly WorkspaceRegularDelta[],
	projection: ExecutionPathProjection,
): WorkspaceEffectDiff {
	if (!before.complete || !after.complete) return { effects: [], complete: false, reason: "snapshot_limit" };
	const rootReason = changedRootMetadata(before.entries.get(""), after.entries.get(""));
	if (rootReason) return { effects: [], complete: false, reason: rootReason };
	const byPath = new Map<string, WorkspaceRegularDelta>();
	for (const delta of deltas) {
		const relativePath = path.normalize(delta.relativePath);
		if (!relativePath || path.isAbsolute(relativePath) || relativePath === ".." || relativePath.startsWith(`..${path.sep}`)) {
			return { effects: [], complete: false, reason: `invalid_delta:${delta.relativePath}` };
		}
		if (byPath.has(relativePath)) return { effects: [], complete: false, reason: `duplicate_delta:${delta.relativePath}` };
		byPath.set(relativePath, delta);
	}

	const effects: WorkspaceRegularEffect[] = [];
	const names = [...new Set([...before.entries.keys(), ...after.entries.keys(), ...byPath.keys()])].sort();
	for (const relativePath of names) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		const delta = byPath.get(relativePath);
		if (delta) {
			const joined = joinRegularDelta(relativePath, previous, current, delta, projection, after.root);
			if ("reason" in joined) return { effects: [], complete: false, reason: joined.reason };
			effects.push(joined.effect);
			continue;
		}
		if (sameStructureEntry(previous, current)) continue;
		if (previous?.kind === "directory" || current?.kind === "directory") {
			if (previous?.kind === "directory" && current?.kind === "directory") {
				if (previous.metadataDigest !== current.metadataDigest) {
					return { effects: [], complete: false, reason: `unsupported_directory_metadata:${relativePath}` };
				}
				continue;
			}
			// Git cannot represent empty directories or preserve created-directory metadata. Until a
			// directory delta driver is installed, fail closed instead of silently approximating it.
			return { effects: [], complete: false, reason: `unsupported_directory_transition:${relativePath}` };
		}
		return { effects: [], complete: false, reason: `untracked_inode_transition:${relativePath}` };
	}
	return { effects: Object.freeze(effects), complete: true };
}

export function hydrateWorkspaceFileEntry(
	entry: Extract<WorkspaceStructureEntry, { readonly kind: "file" }>,
	bytes: Uint8Array,
	includeContent = false,
): Extract<WorkspaceTreeEntry, { readonly kind: "file" }> | undefined {
	if (bytes.byteLength !== entry.size) return undefined;
	const content = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	return {
		...entry,
		digest: sha256Digest(content),
		...(includeContent ? { content } : {}),
	};
}

function joinRegularDelta(
	relativePath: string,
	previous: WorkspaceStructureEntry | undefined,
	current: WorkspaceStructureEntry | undefined,
	delta: WorkspaceRegularDelta,
	projection: ExecutionPathProjection,
	root: string,
): { readonly effect: WorkspaceRegularEffect } | { readonly reason: string } {
	let beforeEntry: Extract<WorkspaceTreeEntry, { readonly kind: "file" }> | undefined;
	if (delta.before === undefined) {
		if (previous !== undefined) return { reason: `delta_before_missing:${relativePath}` };
	} else {
		if (previous?.kind !== "file") return { reason: `delta_before_type:${relativePath}` };
		if (previous.links !== 1) return { reason: `unsupported_hardlink:${relativePath}` };
		if (delta.beforeMode !== undefined && delta.beforeMode !== previous.mode) {
			return { reason: `delta_before_mode:${relativePath}` };
		}
		beforeEntry = hydrateWorkspaceFileEntry(previous, delta.before);
		if (!beforeEntry) return { reason: `delta_before_size:${relativePath}` };
	}

	const logicalPath = projection.toLogical(path.join(root, relativePath));
	if (delta.after === undefined) {
		if (!beforeEntry || current !== undefined) return { reason: `delta_delete_shape:${relativePath}` };
		return { effect: { kind: "delete", logicalPath, relativePath, before: beforeEntry } };
	}
	if (current?.kind !== "file") return { reason: `delta_after_type:${relativePath}` };
	if (current.links !== 1) return { reason: `unsupported_hardlink:${relativePath}` };
	if (delta.afterMode !== undefined && delta.afterMode !== current.mode) {
		return { reason: `delta_after_mode:${relativePath}` };
	}
	const afterEntry = hydrateWorkspaceFileEntry(current, delta.after, true);
	if (!afterEntry) return { reason: `delta_after_size:${relativePath}` };
	return {
		effect: {
			kind: "write",
			logicalPath,
			relativePath,
			...(beforeEntry ? { before: beforeEntry } : {}),
			after: afterEntry,
		},
	};
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
	const rootReason = changedRootMetadata(before.entries.get(""), after.entries.get(""));
	if (rootReason) return { effects: [], complete: false, reason: rootReason };
	const effects: WorkspaceRegularEffect[] = [];
	const names = [...new Set([...before.entries.keys(), ...after.entries.keys()])].sort();
	for (const relativePath of names) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		if (sameTreeEntry(previous, current)) continue;
		if (
			(previous?.kind === "file" && previous.links !== 1) ||
			(current?.kind === "file" && current.links !== 1)
		) {
			return { effects: [], complete: false, reason: `unsupported_hardlink:${relativePath}` };
		}
		const logicalPath = projection.toLogical(path.join(after.root, relativePath));
		if (!current) {
			if (previous?.kind === "file") {
				effects.push({ kind: "delete", logicalPath, relativePath, before: previous });
				continue;
			}
			return { effects: [], complete: false, reason: `unsupported_delete:${relativePath}` };
		}
		if (current.kind === "directory") {
			if (previous?.kind === "directory") {
				if (previous.metadataDigest !== current.metadataDigest) {
					return { effects: [], complete: false, reason: `unsupported_directory_metadata:${relativePath}` };
				}
				continue;
			}
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
	options: {
		readonly excludedEntries?: readonly string[];
		readonly parentExcludedEntries?: readonly string[];
	} = {},
): DynamicDependency | undefined {
	if (!entry) {
		return {
			kind: "absence",
			path: logicalPath,
			...(parent?.kind === "directory" ? { parentEntriesDigest: parent.entriesDigest } : {}),
			...(parent?.kind === "directory" && options.parentExcludedEntries?.length
				? { parentExcludedEntries: Object.freeze([...options.parentExcludedEntries].sort()) }
				: {}),
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
				...(options.excludedEntries?.length
					? { excludedEntries: Object.freeze([...options.excludedEntries].sort()) }
					: {}),
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

function sameStructureEntry(
	left: WorkspaceStructureEntry | undefined,
	right: WorkspaceStructureEntry | undefined,
): boolean {
	if (!left || !right || left.kind !== right.kind) return left === right;
	switch (left.kind) {
		case "file": {
			const value = right as Extract<WorkspaceStructureEntry, { kind: "file" }>;
			return left.size === value.size && left.metadataDigest === value.metadataDigest;
		}
		case "directory": {
			const value = right as Extract<WorkspaceStructureEntry, { kind: "directory" }>;
			return left.entriesDigest === value.entriesDigest && left.metadataDigest === value.metadataDigest;
		}
		case "symlink":
			return left.targetDigest === (right as Extract<WorkspaceStructureEntry, { kind: "symlink" }>).targetDigest;
		case "unsupported":
			return left.type === (right as Extract<WorkspaceStructureEntry, { kind: "unsupported" }>).type;
	}
}

function changedRootMetadata(
	before: WorkspaceTreeEntry | WorkspaceStructureEntry | undefined,
	after: WorkspaceTreeEntry | WorkspaceStructureEntry | undefined,
): string | undefined {
	if (before?.kind !== "directory" || after?.kind !== "directory") return "unsupported_workspace_root_transition";
	return before.metadataDigest === after.metadataDigest ? undefined : "unsupported_workspace_root_metadata";
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
		...(stat.isFile() ? { size: stat.size, links: stat.nlink } : {}),
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
