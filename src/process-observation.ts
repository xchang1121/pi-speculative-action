import type { Dirent, Stats } from "node:fs";
import { lstat, readdir, readlink } from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, relativeFilesystemPath, slash } from "./path-utils.ts";
import { isMissing } from "./error-utils.ts";
import type { DynamicDependency, FilesystemTypeEvidence, Sha256Digest } from "./provenance-certificate.ts";
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
		const relative = relativeFilesystemPath(this.workspaceRoot, physical);
		return relative === undefined ? slash(physical) : slash(path.join(this.sourceRoot, relative));
	}

	toPhysical(logicalPath: string): string | undefined {
		const logical = path.resolve(logicalPath);
		const relative = relativeFilesystemPath(this.sourceRoot, logical);
		if (relative === undefined) return logical;
		const physical = path.resolve(this.workspaceRoot, relative);
		return containsFilesystemPath(this.workspaceRoot, physical) ? physical : undefined;
	}

	normalizeValue(value: string): string {
		let normalized = replacePath(value, this.workspaceRoot, this.sourceRoot);
		if (this.privateRoot) normalized = replacePath(normalized, this.privateRoot, "/.pi-private-world");
		return slash(normalized);
	}

	isWorkspacePhysical(physicalPath: string): boolean {
		return containsFilesystemPath(this.workspaceRoot, physicalPath);
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
	const visit = async (directory: string, relativeDirectory: string, stat: Stats): Promise<void> => {
		const children = !relativeDirectory || stat.isDirectory() ? await readdir(directory, { withFileTypes: true }) : [];
		entries.set(relativeDirectory, await captureExistingWorkspaceStructureEntry(
			directory, stat, relativeDirectory ? [] : [...excludes], children,
		));
		for (const child of children.sort((left, right) => left.name.localeCompare(right.name))) {
			const relative = relativeDirectory ? path.join(relativeDirectory, child.name) : child.name;
			if (!relativeDirectory && excludes.has(child.name)) continue;
			const target = path.join(directory, child.name);
			const stat = await lstat(target);
			if (++files > maxFiles) {
				complete = false;
				return;
			}
			await visit(target, relative, stat);
		}
	};

	await visit(absoluteRoot, "", await lstat(absoluteRoot));
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
		if (isMissing(error)) return undefined;
		throw error;
	}
	return captureExistingWorkspaceStructureEntry(target, stat, excludeEntries);
}

async function captureExistingWorkspaceStructureEntry(
	target: string,
	stat: Stats,
	excludeEntries: readonly string[] = [],
	children?: readonly Dirent[],
): Promise<WorkspaceStructureEntry> {
	const change = { changeDigest: statChangeDigest(stat), changeTimeMs: stat.ctimeMs };
	if (stat.isSymbolicLink()) {
		const linkTarget = await readlink(target);
		return {
			kind: "symlink",
			target: linkTarget,
			targetDigest: sha256Digest(Buffer.from(linkTarget, "utf8")),
			...change,
		};
	}
	if (stat.isDirectory()) {
		const excluded = new Set(excludeEntries);
		const entries = (children ?? await readdir(target, { withFileTypes: true })).filter((entry) => !excluded.has(entry.name));
		return {
			kind: "directory",
			entriesDigest: directoryEntriesDigest(entries),
			metadataDigest: filesystemMetadataDigest(stat),
			...change,
			mode: stat.mode & 0o777,
			uid: stat.uid,
			gid: stat.gid,
		};
	}
	if (stat.isFile()) {
		return {
			kind: "file",
			metadataDigest: filesystemMetadataDigest(stat),
			...change,
			mode: stat.mode & 0o777,
			size: stat.size,
			links: stat.nlink,
		};
	}
	return {
		kind: "unsupported",
		type: filesystemEntryType(stat),
		...change,
	};
}

/** Validated transaction changes, ordered for replay without copying or hashing their bytes. */
interface WorkspaceTransactionEffect {
	readonly logicalPath: string;
	readonly relativePath: string;
	readonly change:
		| (WorkspaceRegularDelta & { readonly kind?: "file" })
		| {
				readonly kind: "directory";
				readonly before?: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>;
				readonly after?: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>;
		  };
}

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
		if (!relativePath || path.isAbsolute(relativePath) || relativeFilesystemPath(".", relativePath) === undefined) {
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
			const reason = regularDeltaFailure(relativePath, previous, current, delta);
			if (reason) return { effects: [], complete: false, reason };
			effects.push({ logicalPath: projection.toLogical(path.join(after.root, relativePath)), relativePath, change: delta });
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
				effects.push({ logicalPath, relativePath, change: { kind: "directory", after: current } });
				continue;
			}
			if (previous?.kind === "directory" && current === undefined) {
				effects.push({ logicalPath, relativePath, change: { kind: "directory", before: previous } });
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
		effect.change.kind === "directory" ? (effect.change.after === undefined ? 1 : 2) : (effect.change.after === undefined ? 0 : 3);
	const depth = (effect: WorkspaceTransactionEffect): number =>
		effect.relativePath.split(path.sep).filter(Boolean).length;
	return [...effects].sort((left, right) => {
		const phaseDifference = phase(left) - phase(right);
		if (phaseDifference !== 0) return phaseDifference;
		const depthDifference = depth(left) - depth(right);
		if (left.change.after === undefined) {
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

function regularDeltaFailure(
	relativePath: string,
	previous: WorkspaceStructureEntry | undefined,
	current: WorkspaceStructureEntry | undefined,
	delta: WorkspaceRegularDelta,
): string | undefined {
	if (delta.before === undefined) {
		if (previous !== undefined) return `delta_before_missing:${relativePath}`;
	} else {
		if (previous?.kind !== "file") return `delta_before_type:${relativePath}`;
		if (previous.links !== 1) return `unsupported_hardlink:${relativePath}`;
		if (delta.beforeMode !== undefined && delta.beforeMode !== previous.mode) {
			return `delta_before_mode:${relativePath}`;
		}
		if (delta.before.byteLength !== previous.size) return `delta_before_size:${relativePath}`;
	}

	if (delta.after === undefined) {
		return delta.before === undefined || current !== undefined ? `delta_delete_shape:${relativePath}` : undefined;
	}
	if (current?.kind !== "file") return `delta_after_type:${relativePath}`;
	if (current.links !== 1) return `unsupported_hardlink:${relativePath}`;
	if (delta.afterMode !== undefined && delta.afterMode !== current.mode) {
		return `delta_after_mode:${relativePath}`;
	}
	if (delta.after.byteLength !== current.size) return `delta_after_size:${relativePath}`;
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

export function directoryEntriesDigest(entries: readonly (FilesystemTypeEvidence & { readonly name: string })[]): Sha256Digest {
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

function replacePath(value: string, from: string, to: string): string {
	const variants = [from, slash(from)];
	let replaced = value;
	for (const variant of variants) replaced = replaced.split(variant).join(to);
	return replaced;
}
