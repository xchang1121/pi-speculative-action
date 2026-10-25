import type { BigIntStats, Stats } from "node:fs";
import { contentDigest, sameContent, type ContentVersion } from "./content-chunks.ts";
import path from "node:path";
import { containsFilesystemPath, relativeFilesystemPath, slash } from "./path-utils.ts";
import { isMissing } from "./error-utils.ts";
import {
	type StableFilesystemCapture, FILESYSTEM_CONCURRENCY, mapFilesystem, type FilesystemTypeEvidence, FILESYSTEM_OBSERVATION_FIELDS,
	filesystemEntryType, filesystemMetadataDigest, type FilesystemObservationEvidence, captureFilesystemEntry, type FilesystemReadView,
} from "./filesystem-evidence.ts";
import type { DynamicDependency } from "./provenance-certificate.ts";
import { type Sha256Digest, digestObject, sha256Digest } from "./stable-value-hash.ts";
import { workspaceAliasIdentity, type WorkspaceStructureEntry, type WorkspaceStructureSnapshot, type WorkspaceTreeEntry } from "./workspace-state.ts";
import { orderWorkspaceChanges, type WorkspaceRegularDelta } from "./workspace-transaction.ts";

export type { WorkspaceStructureEntry, WorkspaceStructureSnapshot, WorkspaceTreeEntry } from "./workspace-state.ts";

export interface ExecutionPathProjectionOptions { readonly sourceRoot: string; readonly workspaceRoot: string; }

/** Stable logical paths keep certificates independent of disposable worktree names. */
export class ExecutionPathProjection {
	readonly sourceRoot: string;
	readonly workspaceRoot: string;

	constructor(options: ExecutionPathProjectionOptions) {
		this.sourceRoot = path.resolve(options.sourceRoot);
		this.workspaceRoot = path.resolve(options.workspaceRoot);
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

	isWorkspacePhysical(physicalPath: string): boolean { return containsFilesystemPath(this.workspaceRoot, physicalPath); }
}

export interface WorkspaceStructureCaptureOptions { readonly maxFiles?: number; readonly exclude?: readonly string[]; readonly observeStat?: (relative: string, stat: BigIntStats) => void | Promise<void>; readonly visitEntry?: (relative: string, entry: FilesystemTypeEvidence) => boolean | Promise<boolean>; readonly view?: FilesystemReadView; }

/** Capture inode and directory semantics without reading regular-file contents. */
export async function captureWorkspaceStructure(root: string, options: WorkspaceStructureCaptureOptions = {}): Promise<WorkspaceStructureSnapshot> {
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
			const captured = await captureFilesystemEntry(path.join(absoluteRoot, relative), "directory", options.view);
			if (!relative && !captured.info.isDirectory()) throw Object.assign(new Error("workspace root is not a directory"), { code: "ENOTDIR" });
			await options.observeStat?.(relative, captured.info);
			const children = captured.entries ?? [];
			const entry = captureExistingWorkspaceStructureEntry(captured, relative ? [] : [...excludes]);
			return { relative, entry, children };
		});
		for (const { relative, entry, children } of captured) {
			entries.set(relative, entry);
			for (const child of children.sort((left, right) => left.name.localeCompare(right.name))) {
				if (!relative && excludes.has(child.name)) continue;
				if (files + 1 > maxFiles) { complete = false; continue; }
				files++;
				if (options.visitEntry && !await options.visitEntry(path.join(relative, child.name), child)) continue;
				pending.push(path.join(relative, child.name));
			}
		}
	}
	return workspaceStructureSnapshot(absoluteRoot, entries, complete);
}

export function captureWorkspaceFileAliases(root?: string) {
	let namespace: ReturnType<typeof captureWorkspaceStructure> | undefined;
	return async (target: string, stat: BigIntStats): Promise<readonly string[] | undefined> => {
		if (stat.nlink <= 1n) return undefined;
		if (!root || !containsFilesystemPath(root, target)) throw new Error("file alias namespace is unavailable");
		const snapshot = await (namespace ??= captureWorkspaceStructure(root)), entry = snapshot.entries.get(path.relative(root, target));
		if (!snapshot.complete || entry?.kind !== "file" || entry.object !== `${stat.dev}:${stat.ino}` || entry.aliases?.length !== Number(stat.nlink))
			throw new Error("file alias namespace is not closed");
		return entry.aliases;
	};
}

/** Resolve namespace aliases in the same captured structure, including layered mutation frontiers. */
export function workspaceStructureSnapshot(root: string, entries: Map<string, WorkspaceStructureEntry>, complete: boolean): WorkspaceStructureSnapshot {
	const objects = new Map<string, string[]>();
	for (const [name, entry] of entries) if (entry.kind === "file" && entry.object) {
		const aliases = objects.get(entry.object) ?? []; aliases.push(name); objects.set(entry.object, aliases);
	}
	for (const names of objects.values()) {
		// A FUSE overlay reports each name's own link count: one below the names found sharing its object is stale, not a hidden name.
		const links = Math.max(...names.map(name => (entries.get(name) as Extract<WorkspaceStructureEntry, { kind: "file" }>).links));
		if (links < 2) continue;
		if (links !== names.length) complete = false;
		const aliases = Object.freeze(names.map(name => path.join(root, name)).sort());
		for (const name of names) {
			const entry = entries.get(name)! as Extract<WorkspaceStructureEntry, { kind: "file" }>;
			entries.set(name, { ...entry, aliases });
		}
	}
	return Object.freeze({ root, entries, complete });
}

/** Capture one path without walking its descendants; used by typed mutation-frontier drivers. */
export async function captureWorkspaceStructureEntry(
	target: string,
	excludeEntries: readonly string[] = [],
	view?: FilesystemReadView,
): Promise<WorkspaceStructureEntry | undefined> {
	try { return captureExistingWorkspaceStructureEntry(await captureFilesystemEntry(target, "directory", view), excludeEntries); }
	catch (error) { if (isMissing(error)) return undefined; throw error; }
}

function captureExistingWorkspaceStructureEntry(
	{ info: stat, link: linkTarget, entries: children }: Awaited<ReturnType<typeof captureFilesystemEntry>>,
	excludeEntries: readonly string[] = [],
): WorkspaceStructureEntry {
	const change = { changeDigest: statChangeDigest(stat), changeTimeMs: statMilliseconds(stat, "ctime") };
	if (stat.isSymbolicLink()) {
		return { kind: "symlink", target: linkTarget!, targetDigest: sha256Digest(linkTarget!), ...change };
	}
	if (stat.isDirectory()) {
		const excluded = new Set(excludeEntries);
		const entries = children!.filter((entry) => !excluded.has(entry.name));
		return {
			kind: "directory",
			observed: Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field, stat[field]])) as FilesystemObservationEvidence,
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
			mode: process.platform === "win32" ? 0 : Number(stat.mode & 0o777n),
			size: Number(stat.size),
			links: Number(stat.nlink),
			object: `${stat.dev}:${stat.ino}`,
			modified: String(stat.mtimeNs),
			ownership: `${stat.uid}:${stat.gid}:${stat.mode & 0o7000n}`,
		};
	}
	return { kind: "unsupported", type: filesystemEntryType(stat), ...change };
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
			let change: WorkspaceRegularDelta = current?.kind === "file" ? { ...delta, afterModified: current.modified,
				afterAliases: (current.aliases ?? [path.join(after.root, relativePath)]).map(name => projection.toLogical(name)).sort() } : delta;
			if (previous?.kind === "file") change = { ...change,
				aliases: (previous.aliases ?? [path.join(before.root, relativePath)]).map(name => projection.toLogical(name)).sort() };
			if (current?.kind === "file" && current.object) {
				const original = originals.get(current.object), anchor = original ?? results.get(current.object)!;
				if (original !== undefined || anchor !== relativePath) {
					if (!byPath.has(anchor)) return { effects: [], complete: false, reason: `object_anchor_missing:${anchor}` };
					change = { ...change, object: { path: projection.toLogical(path.join(after.root, anchor)), before: original !== undefined } };
				}
				const source = original === undefined ? undefined : before.entries.get(original);
				if (source?.kind === "file" && (source.modified !== current.modified || !sameContent(byPath.get(anchor)!.before, delta.after))) {
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

export async function hydrateWorkspaceFileEntry(entry: Extract<WorkspaceStructureEntry, { readonly kind: "file" }>,
	content: ContentVersion | StableFilesystemCapture): Promise<Extract<WorkspaceTreeEntry, { readonly kind: "file" }> | undefined> {
	if ("hash" in content) {
		if (statChangeDigest(content.stat) !== entry.changeDigest) return undefined;
		return { ...entry, digest: `sha256:${content.hash}` };
	}
	return content.byteLength === entry.size ? { ...entry, digest: contentDigest(content) } : undefined;
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
		if (delta.beforeMode !== undefined && delta.beforeMode !== previous.mode) return `delta_before_mode:${relativePath}`;
		if (delta.before.byteLength !== previous.size) return `delta_before_size:${relativePath}`;
	}

	if (delta.after === undefined) return delta.before === undefined || current !== undefined ? `delta_delete_shape:${relativePath}` : undefined;
	if (current?.kind !== "file") return `delta_after_type:${relativePath}`;
	if (previous?.kind === "file" && previous.ownership !== current.ownership) return `unsupported_file_ownership:${relativePath}`;
	if (current.links > 1 && current.aliases?.length !== current.links) return `unproven_aliases:${relativePath}`;
	if (delta.afterMode !== undefined && delta.afterMode !== current.mode) return `delta_after_mode:${relativePath}`;
	if (delta.after.byteLength !== current.size) return `delta_after_size:${relativePath}`;
}

export function snapshotDependency(
	logicalPath: string,
	entry: WorkspaceTreeEntry | undefined,
	parent: WorkspaceTreeEntry | undefined,
	role: Extract<DynamicDependency, { kind: "file" }>["role"] = "input",
	options: { readonly excludedEntries?: readonly string[]; readonly parentExcludedEntries?: readonly string[]; readonly listed?: boolean } = {},
): DynamicDependency | undefined {
	if (!entry) {
		return {
			kind: "absence",
			path: logicalPath,
			...(parent?.kind === "directory" ? { parentEntriesDigest: parent.entriesDigest } : {}),
			...(parent?.kind === "directory" && options.parentExcludedEntries?.length ? { parentExcludedEntries: Object.freeze([...options.parentExcludedEntries].sort()) } : {}),
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
			if (options.listed === false) return { kind: "directory", path: logicalPath, metadataDigest: entry.metadataDigest };
			return {
				kind: "directory",
				path: logicalPath,
				entriesDigest: entry.entriesDigest,
				metadataDigest: entry.metadataDigest,
				...(options.excludedEntries?.length ? { excludedEntries: Object.freeze([...options.excludedEntries].sort()) } : {}),
			};
		case "symlink":
			return { kind: "symlink", path: logicalPath, target: entry.target, targetDigest: entry.targetDigest };
		case "unsupported":
			return undefined;
	}
}

const STRUCTURE_IDENTITY_FIELDS = {
	file: ["size", "metadataDigest", "object"], directory: ["entriesDigest", "metadataDigest"], symlink: ["targetDigest"], unsupported: ["type"],
} satisfies { [Kind in WorkspaceStructureEntry["kind"]]: readonly (keyof Extract<WorkspaceStructureEntry, { kind: Kind }>)[] };

export function sameStructureEntry(left: WorkspaceStructureEntry | undefined, right: WorkspaceStructureEntry | undefined, identity: "semantic" | "change" = "semantic"): boolean {
	if (left === right) return true;
	if (!left || !right || left.kind !== right.kind) return false;
	return (identity === "change" ? left.changeDigest === right.changeDigest : STRUCTURE_IDENTITY_FIELDS[left.kind].every(field => Reflect.get(left, field) === Reflect.get(right, field))) &&
		(left.kind !== "file" || workspaceAliasIdentity(left.aliases) === workspaceAliasIdentity((right as typeof left).aliases));
}

function changedRootMetadata(
	before: WorkspaceStructureEntry | undefined,
	after: WorkspaceStructureEntry | undefined,
): string | undefined {
	if (before?.kind !== "directory" || after?.kind !== "directory") return "unsupported_workspace_root_transition";
	return before.metadataDigest === after.metadataDigest ? undefined : "unsupported_workspace_root_metadata";
}

export function directoryEntriesDigest(entries: readonly (FilesystemTypeEvidence & { readonly name: string })[]): Sha256Digest {
	return digestObject(entries .map((entry) => `${filesystemEntryType(entry)}\0${entry.name}`).sort());
}

/** Kernel-maintained identity/change fields detect writes without making timestamps replay semantics. */
function statMilliseconds(stat: Stats | BigIntStats, field: "ctime" | "mtime"): number {
	const ns = (stat as BigIntStats)[`${field}Ns`];
	if (ns === undefined) return Number(stat[`${field}Ms`]);
	const remainder = (ns % 1_000_000_000n + 1_000_000_000n) % 1_000_000_000n;
	return Number((ns - remainder) / 1_000_000_000n) * 1_000 + Number(remainder) / 1_000_000;
}

export function statChangeDigest(stat: Stats | BigIntStats): Sha256Digest {
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
