import type { Stats } from "node:fs";
import { lstat, readdir, readlink } from "node:fs/promises";
import path from "node:path";
import { contains, slash } from "./path-utils.ts";
import type { DynamicDependency, Sha256Digest } from "./provenance-certificate.ts";
import {
	digestObject,
	filesystemEntryType,
	filesystemMetadataDigest,
	sha256Digest,
} from "./provenance-certificate.ts";
import type {
	WorkspaceStructureEntry,
	WorkspaceStructureSnapshot,
	WorkspaceTreeEntry,
} from "./workspace-state.ts";
import type { WorkspaceRegularDelta } from "./workspace-transaction.ts";

export type {
	WorkspaceStructureEntry,
	WorkspaceStructureSnapshot,
	WorkspaceTreeEntry,
} from "./workspace-state.ts";

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
		if (!contains(this.workspaceRoot, physical)) return slash(physical);
		return slash(path.join(this.sourceRoot, path.relative(this.workspaceRoot, physical)));
	}

	toPhysical(logicalPath: string): string | undefined {
		const logical = path.resolve(logicalPath);
		if (!contains(this.sourceRoot, logical)) return logical;
		const physical = path.resolve(this.workspaceRoot, path.relative(this.sourceRoot, logical));
		return contains(this.workspaceRoot, physical) ? physical : undefined;
	}

	normalizeValue(value: string): string {
		let normalized = replacePath(value, this.workspaceRoot, this.sourceRoot);
		if (this.privateRoot) normalized = replacePath(normalized, this.privateRoot, "/.pi-private-world");
		return slash(normalized);
	}

	isWorkspacePhysical(physicalPath: string): boolean {
		return contains(this.workspaceRoot, path.resolve(physicalPath));
	}
}

export interface WorkspaceStructureCaptureOptions {
	readonly maxFiles?: number;
	readonly exclude?: readonly string[];
}

/** Capture inode and directory semantics without reading regular-file contents. */
export async function captureWorkspaceStructure(
	root: string,
	options: WorkspaceStructureCaptureOptions = {},
): Promise<WorkspaceStructureSnapshot> {
	const absoluteRoot = path.resolve(root);
	const entries = new Map<string, WorkspaceStructureEntry>();
	const excludes = new Set(options.exclude ?? [".git"]);
	const maxFiles = Math.max(1, options.maxFiles ?? 100_000);
	let files = 0;
	let complete = true;
	const rootStat = await lstat(absoluteRoot);
	entries.set("", await captureExistingWorkspaceStructureEntry(absoluteRoot, rootStat, [...excludes]));

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
			entries.set(relative, await captureExistingWorkspaceStructureEntry(target, stat));
			if (stat.isDirectory()) await visit(target, relative);
		}
	};

	await visit(absoluteRoot, "");
	return Object.freeze({ root: absoluteRoot, entries, files, bytesRead: 0, complete });
}

/** Capture one path without walking its descendants; used by typed mutation-frontier drivers. */
export async function captureWorkspaceStructureEntry(
	target: string,
	excludeEntries: readonly string[] = [],
): Promise<WorkspaceStructureEntry | undefined> {
	let stat: Stats;
	try {
		stat = await lstat(target);
	} catch (error) {
		if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return undefined;
		throw error;
	}
	return captureExistingWorkspaceStructureEntry(target, stat, excludeEntries);
}

async function captureExistingWorkspaceStructureEntry(
	target: string,
	stat: Stats,
	excludeEntries: readonly string[] = [],
): Promise<WorkspaceStructureEntry> {
	if (stat.isSymbolicLink()) {
		const linkTarget = await readlink(target);
		return {
			kind: "symlink",
			target: linkTarget,
			targetDigest: sha256Digest(Buffer.from(linkTarget, "utf8")),
			changeDigest: statChangeDigest(stat),
			changeTimeMs: stat.ctimeMs,
		};
	}
	if (stat.isDirectory()) {
		const excluded = new Set(excludeEntries);
		const entries = (await readdir(target, { withFileTypes: true })).filter((entry) => !excluded.has(entry.name));
		return {
			kind: "directory",
			entriesDigest: directoryEntriesDigest(entries),
			metadataDigest: filesystemMetadataDigest(stat),
			changeDigest: statChangeDigest(stat),
			changeTimeMs: stat.ctimeMs,
			mode: stat.mode & 0o777,
			uid: stat.uid,
			gid: stat.gid,
		};
	}
	if (stat.isFile()) {
		return {
			kind: "file",
			metadataDigest: filesystemMetadataDigest(stat),
			changeDigest: statChangeDigest(stat),
			changeTimeMs: stat.ctimeMs,
			mode: stat.mode & 0o777,
			size: stat.size,
			links: stat.nlink,
		};
	}
	return {
		kind: "unsupported",
		type: specialFileType(stat),
		changeDigest: statChangeDigest(stat),
		changeTimeMs: stat.ctimeMs,
	};
}

/** Effect shape used only to seal input evidence; replay bytes remain owned by the transaction. */
type WorkspaceTransactionEffect =
	| {
			readonly kind: "write" | "delete";
			readonly logicalPath: string;
			readonly relativePath: string;
			readonly before?: WorkspaceTreeEntry;
	  }
	| {
			readonly kind: "mkdir";
			readonly logicalPath: string;
			readonly relativePath: string;
			readonly after: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>;
	  }
	| {
			readonly kind: "rmdir";
			readonly logicalPath: string;
			readonly relativePath: string;
			readonly before: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>;
	  };

export interface WorkspaceTransactionDiff {
	readonly effects: readonly WorkspaceTransactionEffect[];
	readonly complete: boolean;
	readonly reason?: string;
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
): WorkspaceTransactionDiff {
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

	const effects: WorkspaceTransactionEffect[] = [];
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
			const logicalPath = projection.toLogical(path.join(after.root, relativePath));
			if (previous === undefined && current?.kind === "directory") {
				effects.push({ kind: "mkdir", logicalPath, relativePath, after: current });
				continue;
			}
			if (previous?.kind === "directory" && current === undefined) {
				effects.push({ kind: "rmdir", logicalPath, relativePath, before: previous });
				continue;
			}
			return { effects: [], complete: false, reason: `unsupported_directory_type_change:${relativePath}` };
		}
		return { effects: [], complete: false, reason: `untracked_inode_transition:${relativePath}` };
	}
	return { effects: Object.freeze(orderWorkspaceTransactionEffects(effects)), complete: true };
}

function orderWorkspaceTransactionEffects(
	effects: readonly WorkspaceTransactionEffect[],
): WorkspaceTransactionEffect[] {
	const phase = (effect: WorkspaceTransactionEffect): number =>
		effect.kind === "delete" ? 0 : effect.kind === "rmdir" ? 1 : effect.kind === "mkdir" ? 2 : 3;
	const depth = (effect: WorkspaceTransactionEffect): number =>
		effect.relativePath.split(path.sep).filter(Boolean).length;
	return [...effects].sort((left, right) => {
		const phaseDifference = phase(left) - phase(right);
		if (phaseDifference !== 0) return phaseDifference;
		const depthDifference = depth(left) - depth(right);
		if (left.kind === "delete" || left.kind === "rmdir") {
			if (depthDifference !== 0) return -depthDifference;
		} else if (depthDifference !== 0) return depthDifference;
		return left.relativePath.localeCompare(right.relativePath);
	});
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
): { readonly effect: WorkspaceTransactionEffect } | { readonly reason: string } {
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
	if (delta.after.byteLength !== current.size) return { reason: `delta_after_size:${relativePath}` };
	return {
		effect: {
			kind: "write",
			logicalPath,
			relativePath,
			...(beforeEntry ? { before: beforeEntry } : {}),
		},
	};
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

export function directoryEntriesDigest(entries: readonly { readonly name: string; isFile(): boolean; isDirectory(): boolean; isSymbolicLink(): boolean; isSocket(): boolean; isFIFO(): boolean; isCharacterDevice(): boolean; isBlockDevice(): boolean }[]): Sha256Digest {
	return digestObject(
		entries
			.map((entry) => `${filesystemEntryType(entry)}\0${entry.name}`)
			.sort(),
	);
}

/** Kernel-maintained identity/change fields detect writes without making timestamps replay semantics. */
function statChangeDigest(stat: Stats): Sha256Digest {
	return digestObject({
		dev: stat.dev,
		ino: stat.ino,
		ctimeMs: stat.ctimeMs,
		mtimeMs: stat.mtimeMs,
		mode: stat.mode,
		size: stat.size,
		links: stat.nlink,
		type: stat.isFile() ? "file" : stat.isDirectory() ? "directory" : stat.isSymbolicLink() ? "symlink" : "other",
	});
}

function specialFileType(stat: Stats): string {
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
