import type { BigIntStats, Dirent, Stats } from "node:fs";
import { lstat, readdir, readlink } from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, relativeFilesystemPath, slash } from "./path-utils.ts";
import { isMissing } from "./error-utils.ts";
import type { StableFilesystemCapture } from "./filesystem-evidence.ts";
import { FILESYSTEM_CONCURRENCY, mapFilesystem } from "./filesystem-evidence.ts";
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
import { orderWorkspaceChanges, type WorkspaceRegularDelta } from "./workspace-transaction.ts";

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
	const pending = [""];
	let files = 0, complete = true;
	for (let cursor = 0; cursor < pending.length;) {
		const batch = pending.slice(cursor, cursor + FILESYSTEM_CONCURRENCY);
		cursor += batch.length;
		const captured = await mapFilesystem(batch, async (relative) => {
			const target = path.join(absoluteRoot, relative), stat = await lstat(target, { bigint: true });
			const children = !relative || stat.isDirectory() ? await readdir(target, { withFileTypes: true }) : [];
			const entry = await captureExistingWorkspaceStructureEntry(target, stat, relative ? [] : [...excludes], children);
			return { relative, entry, children };
		});
		for (const { relative, entry, children } of captured) {
			entries.set(relative, entry);
			for (const child of children.sort((left, right) => left.name.localeCompare(right.name))) {
				if (!relative && excludes.has(child.name)) continue;
				if (files + 1 > maxFiles) { complete = false; continue; }
				files++;
				pending.push(path.join(relative, child.name));
			}
		}
	}
	return workspaceStructureSnapshot(absoluteRoot, entries, complete);
}

/** Resolve namespace aliases in the same captured structure, including layered mutation frontiers. */
export function workspaceStructureSnapshot(root: string, entries: Map<string, WorkspaceStructureEntry>, complete: boolean): WorkspaceStructureSnapshot {
	const objects = new Map<string, string[]>();
	for (const [name, entry] of entries) if (entry.kind === "file" && entry.links > 1 && entry.object) {
		const aliases = objects.get(entry.object) ?? []; aliases.push(name); objects.set(entry.object, aliases);
	}
	for (const names of objects.values()) {
		const aliases = Object.freeze(names.map(name => path.join(root, name)).sort());
		for (const name of names) {
			const entry = entries.get(name)! as Extract<WorkspaceStructureEntry, { kind: "file" }>;
			if (entry.links !== names.length) complete = false;
			entries.set(name, { ...entry, aliases });
		}
	}
	return Object.freeze({ root, entries, files: Math.max(0, entries.size - 1), bytesRead: 0, complete });
}

/** Capture one path without walking its descendants; used by typed mutation-frontier drivers. */
export async function captureWorkspaceStructureEntry(
	target: string,
	excludeEntries: readonly string[] = [],
): Promise<WorkspaceStructureEntry | undefined> {
	let stat: BigIntStats;
	try {
		stat = await lstat(target, { bigint: true });
	} catch (error) {
		if (isMissing(error)) return undefined;
		throw error;
	}
	return captureExistingWorkspaceStructureEntry(target, stat, excludeEntries);
}

async function captureExistingWorkspaceStructureEntry(
	target: string,
	stat: BigIntStats,
	excludeEntries: readonly string[] = [],
	children?: readonly Dirent[],
): Promise<WorkspaceStructureEntry> {
	const change = { changeDigest: statChangeDigest(stat), changeTimeMs: statMilliseconds(stat, "ctime") };
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
			mode: Number(stat.mode & 0o777n),
			uid: Number(stat.uid),
			gid: Number(stat.gid),
		};
	}
	if (stat.isFile()) {
		return {
			kind: "file",
			metadataDigest: filesystemMetadataDigest(stat),
			...change,
			mode: Number(stat.mode & 0o777n),
			size: Number(stat.size),
			links: Number(stat.nlink),
			object: `${stat.dev}:${stat.ino}`,
			modified: String(stat.mtimeNs),
			ownership: `${stat.uid}:${stat.gid}:${stat.mode & 0o7000n}`,
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
	const anchors = (snapshot: WorkspaceStructureSnapshot) => {
		const result = new Map<string, string>();
		for (const name of names) {
			const entry = snapshot.entries.get(name);
			if (entry?.kind === "file" && entry.object && !result.has(entry.object)) result.set(entry.object, name);
		}
		return result;
	};
	const originals = anchors(before), results = anchors(after);
	for (const relativePath of names) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		const delta = byPath.get(relativePath);
		if (delta) {
			const reason = regularDeltaFailure(relativePath, previous, current, delta);
			if (reason) return { effects: [], complete: false, reason };
			let change: WorkspaceRegularDelta = delta;
			if (previous?.kind === "file" && previous.aliases) change = { ...change, aliases: previous.aliases.map(name => projection.toLogical(name)).sort() };
			if (current?.kind === "file" && current.object) {
				const original = originals.get(current.object), anchor = original ?? results.get(current.object)!;
				if (original !== undefined || anchor !== relativePath) {
					if (!byPath.has(anchor)) return { effects: [], complete: false, reason: `object_anchor_missing:${anchor}` };
					change = { ...change, object: { path: projection.toLogical(path.join(after.root, anchor)), before: original !== undefined } };
				}
				const source = original === undefined ? undefined : before.entries.get(original);
				if (source?.kind === "file" &&
					(source.modified !== current.modified || !Buffer.from(byPath.get(anchor)!.before!).equals(Buffer.from(delta.after!)))) {
					change = { ...change, operation: "write_contents" };
				}
			}
			effects.push({ logicalPath: projection.toLogical(path.join(after.root, relativePath)), relativePath, change });
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
	return {
		effects: Object.freeze(orderWorkspaceChanges(effects, effect => ({
			change: effect.change,
			depth: effect.relativePath.split(path.sep).filter(Boolean).length,
			key: effect.relativePath,
		}))),
		complete: true,
	};
}

export function hydrateWorkspaceFileEntry(
	entry: Extract<WorkspaceStructureEntry, { readonly kind: "file" }>,
	content: Uint8Array | StableFilesystemCapture,
): Extract<WorkspaceTreeEntry, { readonly kind: "file" }> | undefined {
	if ("hash" in content) {
		if (statChangeDigest(content.stat) !== entry.changeDigest) return undefined;
		return { ...entry, digest: `sha256:${content.hash}` };
	}
	return content.byteLength === entry.size ? { ...entry, digest: sha256Digest(content) } : undefined;
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
		if (previous.links > 1 && previous.aliases?.length !== previous.links) return `unproven_aliases:${relativePath}`;
		if (delta.beforeMode !== undefined && delta.beforeMode !== previous.mode) {
			return `delta_before_mode:${relativePath}`;
		}
		if (delta.before.byteLength !== previous.size) return `delta_before_size:${relativePath}`;
	}

	if (delta.after === undefined) {
		return delta.before === undefined || current !== undefined ? `delta_delete_shape:${relativePath}` : undefined;
	}
	if (current?.kind !== "file") return `delta_after_type:${relativePath}`;
	if (previous?.kind === "file" && previous.ownership !== current.ownership) return `unsupported_file_ownership:${relativePath}`;
	if (current.links > 1 && current.aliases?.length !== current.links) return `unproven_aliases:${relativePath}`;
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
				...(entry.aliases ? { aliases: entry.aliases } : {}),
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
			return left.size === value.size && left.metadataDigest === value.metadataDigest && left.object === value.object &&
				JSON.stringify(left.aliases) === JSON.stringify(value.aliases);
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
	before: WorkspaceStructureEntry | undefined,
	after: WorkspaceStructureEntry | undefined,
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
function statMilliseconds(stat: Stats | BigIntStats, field: "ctime" | "mtime"): number {
	const ns = (stat as BigIntStats)[`${field}Ns`];
	if (ns === undefined) return Number(stat[`${field}Ms`]);
	const remainder = (ns % 1_000_000_000n + 1_000_000_000n) % 1_000_000_000n;
	return Number((ns - remainder) / 1_000_000_000n) * 1_000 + Number(remainder) / 1_000_000;
}

function statChangeDigest(stat: Stats | BigIntStats): Sha256Digest {
	return digestObject({
		dev: Number(stat.dev),
		ino: Number(stat.ino),
		ctimeMs: statMilliseconds(stat, "ctime"),
		mtimeMs: statMilliseconds(stat, "mtime"),
		mode: Number(stat.mode),
		size: Number(stat.size),
		links: Number(stat.nlink),
		type: stat.isFile() ? "file" : stat.isDirectory() ? "directory" : stat.isSymbolicLink() ? "symlink" : "other",
	});
}

function replacePath(value: string, from: string, to: string): string {
	const variants = [from, slash(from)];
	let replaced = value;
	for (const variant of variants) replaced = replaced.split(variant).join(to);
	return replaced;
}
