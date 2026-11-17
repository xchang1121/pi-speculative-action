import { serialize } from "node:v8";
import { createHash, randomUUID } from "node:crypto";
import type { BigIntStats, Stats } from "node:fs";
import { constants as fsConstants } from "node:fs";
import type { FileHandle } from "node:fs/promises";
import {
	access,
	chmod,
	link,
	lstat,
	lutimes,
	mkdir,
	mkdtemp,
	open,
	readFile,
	realpath,
	rename,
	rm,
	rmdir,
	symlink,
	unlink,
	utimes,
	writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type {
	SpeculativeAgentExecutionWorld,
	SpeculativeToolExecutionContext,
} from "./agent-execution-world.ts";
import {
	createCommittedResourceInputs,
	evaluateResourceQuery,
	filesystemExecutionDependsOn,
	resourceSnapshotBranch,
} from "./agent-execution-world.ts";
import { currentSpeculativeConfiguration, speculativeConfig } from "./config-schema.ts";
import type { ContentVersion, FileContentVersion } from "./content-chunks.ts";
import {
	captureContent,
	captureFileContent,
	ChunkedContent,
	ContentChunkPool,
	contentMemory,
	ownContent,
	readContent,
	sameContent,
	writeContent,
} from "./content-chunks.ts";
import type {
	ToolInvocation,
	ToolSettlement,
	WorldBranch,
	WorldCheckpoint,
	WorldExecutionMetrics,
} from "./execution-world.ts";
import {
	EffectCommit,
	effectCommitFailure,
	isPoisonedEffectCommit,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "./execution-world.ts";
import type { FilesystemObservationEvidence, FilesystemTypeEvidence } from "./filesystem-evidence.ts";
import {
	advanceFilesystemClock,
	assertNoSymlinkPath,
	captureFilesystemEntry,
	fileIdentity,
	FILESYSTEM_OBSERVATION_FIELDS,
	FilesystemCaptureChangedError,
	filesystemEntryType,
	filesystemMetadataDigest,
	FilesystemReadView,
	mapFilesystem,
	openAnonymousFilesystemFile,
	openFilesystemReadView,
	pinFilesystemObject,
	readFilesystemDirectory,
	sameFilesystemIdentity,
	sharedWalk,
	FILESYSTEM_METADATA_FIELDS as SNAPSHOT_METADATA_FIELDS,
	withFilesystemReadView,
} from "./filesystem-evidence.ts";
import type { LinuxOverlayfsMount, LinuxOverlayfsOptions } from "./linux-overlayfs.ts";
import { LinuxOverlayfsCapabilityRegistry, mountLinuxOverlayfs } from "./linux-overlayfs.ts";
import type { Sha256Digest } from "./primitives.ts";
import {
	containsFilesystemPath,
	digestObject,
	errorMessage,
	execHostFile,
	filesystemPathKey,
	hasErrorCode,
	isMissing,
	isSafeCount,
	relativeFilesystemPath,
	sha256Digest,
	slash,
} from "./primitives.ts";
import { nativeFilesystem } from "./process-startup.mjs";
import type { DynamicDependency } from "./provenance-certificate.ts";
import type { ResourceChangeSet, ResourceInput } from "./resource-version.ts";
import {
	computeStepInView,
	ResourceInputSession,
	ResourceVersionManager,
	type ResourceVersionToken,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { ResourceValidation } from "./settlement.ts";
interface WorkspaceEntryChange {
	readonly changeDigest: Sha256Digest;
	readonly changeTimeMs: number;
}
export interface WorkspaceDirectoryState {
	readonly entriesDigest: Sha256Digest;
	readonly mode: number;
	readonly uid: number;
	readonly gid: number;
}
export type WorkspaceTreeEntry = WorkspaceEntryChange &
	(
		| {
				readonly kind: "file";
				/** Owned byte source when an immutable lower entry is carried into a layered view. */
				readonly contentPath?: string;
				readonly digest: Sha256Digest;
				readonly metadataDigest: Sha256Digest;
				readonly mode: number;
				readonly size: number;
				readonly links: number;
				/** Snapshot-local object identity and its closed set of names. */
				readonly object?: string;
				readonly aliases?: readonly string[];
				readonly modified?: string;
				readonly ownership?: string;
		  }
		| ({
				readonly kind: "directory";
				/** Kernel prestate for observations made after this operation changed the directory's entries. */
				readonly observed?: FilesystemObservationEvidence;
				readonly metadataDigest: Sha256Digest;
		  } & WorkspaceDirectoryState)
		| {
				readonly kind: "symlink";
				readonly target: string;
				readonly targetDigest: Sha256Digest;
		  }
		| {
				readonly kind: "unsupported";
				readonly type: string;
		  }
	);
export type WorkspaceStructureEntry =
	| Omit<
			Extract<
				WorkspaceTreeEntry,
				{
					readonly kind: "file";
				}
			>,
			"digest"
	  >
	| Exclude<
			WorkspaceTreeEntry,
			{
				readonly kind: "file";
			}
	  >;
export interface WorkspaceStructureSnapshot {
	readonly root: string;
	readonly entries: ReadonlyMap<string, WorkspaceStructureEntry>;
	readonly complete: boolean;
}
const aliasIdentities = new WeakMap<readonly string[], Sha256Digest>();
/** A captured alias group is shared by its names; compare its immutable namespace once. */
export function workspaceAliasIdentity(aliases: readonly string[] | undefined): Sha256Digest | undefined {
	if (!aliases) return;
	const identity = aliasIdentities.get(aliases) ?? digestObject(aliases);
	if (Object.isFrozen(aliases)) aliasIdentities.set(aliases, identity);
	return identity;
}
/** A name refers to an object from either endpoint of the same workspace transaction. */
export interface WorkspaceFileMutation {
	readonly operation?: "write_contents";
	readonly object?: {
		readonly path: string;
		readonly before: boolean;
	};
	readonly aliases?: readonly string[];
	/** Closed names of the resulting object, independent of its predecessor's names. */
	readonly afterAliases?: readonly string[];
}
export function projectFileMutation(
	{ operation, object, aliases, afterAliases }: WorkspaceFileMutation,
	project = (name: string) => name,
): WorkspaceFileMutation {
	return Object.fromEntries(
		Object.entries({
			operation,
			object: object && { ...object, path: project(object.path) },
			aliases: aliases?.map(project),
			afterAliases: afterAliases?.map(project),
		}).filter(([, value]) => value !== undefined),
	);
}
/** Apply removals child first and creations parent first, preserving each caller's path order. */
export function orderWorkspaceChanges<Value>(
	values: readonly Value[],
	order: (value: Value) => {
		readonly change: {
			readonly kind?: "file" | "directory";
			readonly after?: unknown;
		};
		readonly depth: number;
		readonly key: string;
	},
): Value[] {
	if (values.length < 2) return [...values];
	return values
		.map((value) => {
			const { change, depth, key } = order(value);
			const removed = change.after === undefined;
			return {
				value,
				key,
				depth: removed ? -depth : depth,
				phase: change.kind === "directory" ? (removed ? 1 : 2) : removed ? 0 : 3,
			};
		})
		.sort(
			(left, right) =>
				left.phase - right.phase || left.depth - right.depth || left.key.localeCompare(right.key),
		)
		.map(({ value }) => value);
}
/** Exact regular-file transition captured around one workspace operation. */
export interface WorkspaceRegularDelta extends WorkspaceFileMutation {
	readonly relativePath: string;
	readonly before?: ContentVersion;
	readonly after?: ContentVersion;
	readonly beforeMode?: number;
	readonly afterMode?: number;
	readonly afterModified?: string;
	/** The identity that stands for the before bytes (see `settledIdentity`): an unchanged file proves its baseline by it. */
	readonly beforeIdentity?: string;
}
export type WorkspaceTransactionDelta =
	| {
			readonly complete: true;
			readonly changes: readonly WorkspaceRegularDelta[];
			readonly before: WorkspaceStructureSnapshot;
			readonly after: WorkspaceStructureSnapshot;
	  }
	| {
			readonly complete: false;
			readonly changes: readonly WorkspaceRegularDelta[];
			readonly reason: string;
			readonly before?: WorkspaceStructureSnapshot;
			readonly after?: WorkspaceStructureSnapshot;
	  };
/**
 * One mutation interval in a workspace. A driver may reject attribution when another interval
 * overlaps, but it must never alter or re-execute the operation itself.
 */
export interface WorkspaceTransactionCapture {
	/** Borrow a bounded copy of an active interval's original regular-file bytes. */
	readonly readBefore?: (relativePath: string, maxBytes: number) => Promise<Uint8Array | undefined>;
	/** An overlapped interval is attributed only by its writer's own account; without one it fails closed. */
	readonly finish: (
		ownership?: () => Promise<WorkspaceTransactionOwnership | undefined>,
	) => Promise<WorkspaceTransactionDelta>;
	/** `unchanged` is valid only when the operation never started mutating this workspace. */
	readonly abort: (unchanged?: true) => Promise<void>;
}
/** What one interval's writer did, from its own trace, in workspace-relative paths. */
export interface WorkspaceTransactionOwnership {
	readonly written: ReadonlySet<string>;
	/** Paths it read, stat'ed, probed or listed, each with the wall-clock milliseconds it last did. */
	readonly observed: ReadonlyMap<string, number>;
	/** Wall-clock milliseconds by which all its processes had exited. */
	readonly endedAt: number;
	/** Whether another writer touched any of these paths while this interval ran, up to `until`. */
	readonly interfered: (paths: ReadonlySet<string>, until: number) => Promise<boolean>;
}
/** Generic mutation journal installed by the concrete workspace implementation. */
export interface WorkspaceTransactionDriver {
	readonly begin: () => Promise<WorkspaceTransactionCapture>;
	/** Release driver-owned journal handles before the enclosing workspace is removed. */
	readonly dispose: () => Promise<void>;
}
/** Content-free structure view supplied by the concrete workspace storage driver. */
export interface WorkspaceStructureDriver {
	readonly capture: () => Promise<WorkspaceStructureSnapshot>;
	readonly retain?: (versions: FilesystemVersionSource) => () => void;
}
/**
 * Keep transaction machinery off read-only/replay paths. Concurrent first users share one driver,
 * and a failed construction remains failed rather than silently changing observation policy.
 */
export function deferredWorkspaceTransactionDriver(
	create: () => Promise<WorkspaceTransactionDriver>,
): WorkspaceTransactionDriver {
	let driver: Promise<WorkspaceTransactionDriver> | undefined;
	let disposal: Promise<void> | undefined;
	return {
		begin: async () => {
			if (!disposal) {
				const resolved = await (driver ??= Promise.resolve().then(create));
				if (!disposal) return resolved.begin();
				await disposal;
			}
			throw new Error("workspace transaction driver is disposed");
		},
		dispose: () => (disposal ??= driver ? driver.then((resolved) => resolved.dispose()) : Promise.resolve()),
	};
}
async function gitOutput(cwd: string, args: readonly string[]): Promise<string> {
	const { stdout } = await execHostFile("git", ["--no-optional-locks", "-C", cwd, ...args], {
		encoding: "utf8",
		windowsHide: true,
	});
	return stdout.trim();
}
/**
 * Keep learned patterns attached to the repository, even when runners reuse the
 * same checkout path for unrelated projects. Raw remotes never leave this scope.
 */
export async function resolvePatternWorkspaceIdentity(cwd: string): Promise<string> {
	const absoluteCwd = path.resolve(cwd);
	try {
		const layout = await gitOutput(absoluteCwd, [
			"rev-parse",
			"--path-format=absolute",
			"--show-toplevel",
			"--git-common-dir",
		]);
		const [root, commonDirectory] = layout.split(/\r?\n/u);
		if (!root || !commonDirectory) return absoluteCwd;
		let repository = commonDirectory;
		try {
			repository =
				(await gitOutput(absoluteCwd, ["config", "--get", "remote.origin.url"])) || commonDirectory;
		} catch {
			// Local repositories still have a stable common Git directory.
		}
		const relativeCwd = path.relative(root, absoluteCwd);
		const digest = createHash("sha256").update(repository).update("\0").update(relativeCwd).digest("hex");
		return path.join(path.parse(absoluteCwd).root, ".pi-pattern-repositories", digest);
	} catch {
		return absoluteCwd;
	}
}
export interface ExecutionPathProjectionOptions {
	readonly sourceRoot: string;
	readonly workspaceRoot: string;
}
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
	isWorkspacePhysical(physicalPath: string): boolean {
		return containsFilesystemPath(this.workspaceRoot, physicalPath);
	}
}
export interface WorkspaceStructureCaptureOptions {
	readonly maxFiles?: number;
	readonly exclude?: readonly string[];
	readonly observeStat?: (relative: string, stat: BigIntStats, unchanged?: boolean) => void | Promise<void>;
	readonly visitEntry?: (relative: string, entry: FilesystemTypeEvidence) => boolean | Promise<boolean>;
	readonly view?: FilesystemReadView;
	readonly layered?: true;
	readonly content?: {
		readonly pool?: ContentChunkPool;
		readonly maxFileBytes: number;
		readonly maxBytes: number;
	};
}
interface WorkspaceCapturedVersion {
	readonly captured: Awaited<ReturnType<typeof captureFilesystemEntry>>;
	readonly entry: WorkspaceStructureEntry;
}
export interface WorkspaceContentSnapshot {
	readonly snapshot: WorkspaceStructureSnapshot;
	readonly files: ReadonlyMap<string, FileContentVersion | undefined>;
}
interface WorkspaceCapturedVersions extends WorkspaceContentSnapshot {
	readonly entries: ReadonlyMap<string, WorkspaceCapturedVersion>;
}
/** A boundary retains original file versions; unchanged names share them across all later intervals. */
export async function captureWorkspaceContent(
	snapshot: WorkspaceStructureSnapshot,
	previous: WorkspaceContentSnapshot | undefined,
	read: (
		relative: string,
		entry: Extract<
			WorkspaceStructureEntry,
			{
				kind: "file";
			}
		>,
	) => Promise<FileContentVersion | undefined>,
	options: {
		readonly maxBytes: number;
		readonly memory?: true;
		readonly required?: true;
		readonly baseline?: WorkspaceStructureSnapshot;
		readonly baselineMemory?: (relative: string) => number;
		readonly maxFileBytes?: number;
	},
): Promise<WorkspaceContentSnapshot> {
	if (
		previous?.snapshot.entries === snapshot.entries &&
		(!options.required || [...previous.files.values()].every(Boolean))
	)
		return previous;
	const files = new Map<string, FileContentVersion | undefined>();
	let remaining = options.maxBytes;
	await mapFilesystem([...snapshot.entries], async ([relative, entry]) => {
		if (entry.kind !== "file") return;
		if (
			!previous?.files.has(relative) &&
			options.baseline &&
			sameStructureEntry(entry, options.baseline.entries.get(relative), "change")
		) {
			remaining -= options.baselineMemory?.(relative) ?? 0;
			if (remaining < 0) throw new Error("workspace transaction after-state exceeds capture limit");
			return;
		}
		const reused = previous?.files.get(relative);
		const prior =
			reused && sameStructureEntry(entry, previous!.snapshot.entries.get(relative), "change")
				? reused
				: undefined;
		if (entry.size > (options.maxFileBytes ?? Infinity) || (!options.memory && entry.size > remaining)) {
			if (options.required) throw new Error("workspace transaction after-state exceeds capture limit");
			files.set(relative, undefined);
			return;
		}
		if (!options.memory) remaining -= entry.size;
		const content = prior ?? (await read(relative, entry));
		if (content && statChangeDigest(content.stat) !== entry.changeDigest)
			throw new Error(`workspace file version changed: ${relative}`);
		if (options.memory && content) {
			remaining -= contentMemory(content.content);
			if (remaining < 0) throw new Error("workspace transaction after-state exceeds capture limit");
		}
		files.set(relative, content);
	});
	return { snapshot, files };
}
export interface FilesystemVersionSource {
	readonly watch: (roots: readonly string[]) => void;
	readonly admit?: (
		objects: readonly {
			readonly dev: bigint;
			readonly ino: bigint;
			readonly path: string;
		}[],
	) => boolean;
	readonly version: () => bigint | undefined;
	readonly changes: (
		since: bigint,
		inputs: readonly {
			readonly dev: bigint;
			readonly ino: bigint;
		}[],
	) =>
		| {
				readonly version: bigint;
				readonly objects: ReadonlySet<string>;
				readonly unstable?: ReadonlySet<string>;
		  }
		| undefined;
}
/** Capture inode and directory semantics without reading regular-file contents. */
export async function captureWorkspaceStructure(
	root: string,
	options: WorkspaceStructureCaptureOptions = {},
): Promise<WorkspaceStructureSnapshot> {
	return new WorkspaceStructureCapture(root, options).capture();
}
/** Original observations survive only while the kernel proves their objects and names unchanged. */
export class WorkspaceStructureCapture {
	private retained?: WorkspaceCapturedVersions & {
		readonly version: bigint;
		readonly objects: readonly {
			readonly dev: bigint;
			readonly ino: bigint;
		}[];
	};
	private readonly root: string;
	private readonly options: WorkspaceStructureCaptureOptions;
	private readonly versions?: FilesystemVersionSource;
	constructor(
		root: string,
		options: WorkspaceStructureCaptureOptions = {},
		versions?: FilesystemVersionSource,
	) {
		this.root = root;
		this.options = options;
		this.versions = versions;
	}
	clear(): void {
		this.retained = undefined;
	}
	async capture(
		observeStat?: WorkspaceStructureCaptureOptions["observeStat"],
	): Promise<WorkspaceStructureSnapshot> {
		return (await this.captureVersions(observeStat)).snapshot;
	}
	/** Each boundary borrows the original entries and content versions; a later capture replaces only changed entries. */
	async captureVersions(
		observeStat?: WorkspaceStructureCaptureOptions["observeStat"],
	): Promise<WorkspaceCapturedVersions> {
		const options = this.options;
		const absoluteRoot = path.resolve(this.root);
		const retained = this.retained;
		const since = retained?.version ?? this.versions?.version();
		const changes = since === undefined ? undefined : this.versions?.changes(since, retained?.objects ?? []);
		const version = changes?.version;
		const objects = changes?.objects;
		const previous = changes ? retained?.entries : undefined;
		if (retained && objects && !objects.size) {
			this.retained = { ...retained, version: changes!.version };
			if (observeStat)
				await mapFilesystem([...retained.entries], async ([relative, { captured }]) =>
					observeStat(relative, captured.info, true),
				);
			return retained;
		}
		const capturedEntries = new Map<string, WorkspaceCapturedVersion>();
		const admission: {
			dev: bigint;
			ino: bigint;
			path: string;
		}[] = [];
		const entries = new Map<string, WorkspaceStructureEntry>();
		const excludes = new Set(options.exclude ?? speculativeConfig().filesystem.excludes);
		const maxFiles = options.maxFiles ?? speculativeConfig().filesystem.maxTreeEntries;
		const pending = [{ relative: "", replaced: false }];
		let files = 0;
		let complete = !this.versions || changes !== undefined;
		for (let cursor = 0; cursor < pending.length;) {
			const batch = pending.slice(cursor, cursor + speculativeConfig().filesystem.concurrency);
			cursor += batch.length;
			const captured = await mapFilesystem(batch, async ({ relative, replaced }) => {
				const prior = previous?.get(relative);
				const reused =
					prior && !replaced && !objects!.has(`${prior.captured.info.dev}:${prior.captured.info.ino}`);
				const captured = reused
					? prior.captured
					: await captureFilesystemEntry(path.join(absoluteRoot, relative), "directory", options.view);
				if (!reused)
					admission.push({
						dev: captured.info.dev,
						ino: captured.info.ino,
						path: path.join(absoluteRoot, relative),
					});
				if (!relative && !captured.info.isDirectory())
					throw Object.assign(new Error("workspace root is not a directory"), { code: "ENOTDIR" });
				await (observeStat ?? (!reused ? options.observeStat : undefined))?.(
					relative,
					captured.info,
					previous ? !!reused : undefined,
				);
				const children = captured.entries ?? [];
				const entry = reused ? prior.entry : captureWorkspaceStructureEntry(captured, [...excludes]);
				capturedEntries.set(relative, reused ? prior : { captured, entry });
				const moved =
					replaced ||
					(!!prior &&
						(captured.info.dev !== prior.captured.info.dev ||
							captured.info.ino !== prior.captured.info.ino)) ||
					(captured.info.isDirectory() &&
						!!changes?.unstable?.has(`${captured.info.dev}:${captured.info.ino}`));
				return { relative, entry, children, replaced: moved };
			});
			for (const { relative, entry, children, replaced } of captured) {
				entries.set(relative, entry);
				for (const child of children.sort((left, right) => left.name.localeCompare(right.name))) {
					if (excludes.has(child.name)) continue;
					if (files + 1 > maxFiles) {
						complete = false;
						continue;
					}
					files++;
					if (options.visitEntry && !(await options.visitEntry(path.join(relative, child.name), child)))
						continue;
					pending.push({ relative: path.join(relative, child.name), replaced });
				}
			}
		}
		complete &&= this.versions?.admit?.(admission) !== false;
		const snapshot = options.layered
			? { root: absoluteRoot, entries, complete }
			: workspaceStructureSnapshot(absoluteRoot, entries, complete);
		const content = options.content
			? await captureWorkspaceContent(
					snapshot,
					retained,
					(relative, entry) =>
						captureFileContent(path.join(absoluteRoot, relative), entry.size, options.content!.pool, {
							stat: capturedEntries.get(relative)!.captured.info,
						}),
					options.content,
				)
			: { snapshot, files: new Map<string, FileContentVersion | undefined>() };
		// Keep the starting epoch: writes during this walk are visited again on the next capture.
		// A temporarily busy writer cannot revoke the older observations of unrelated objects.
		if (snapshot.complete && version !== undefined)
			this.retained = {
				version,
				...content,
				entries: capturedEntries,
				objects: [
					...new Map(
						[...capturedEntries.values()].map(({ captured: { info } }) => [
							`${info.dev}:${info.ino}`,
							{ dev: info.dev, ino: info.ino },
						]),
					).values(),
				],
			};
		return { ...content, entries: capturedEntries };
	}
}
/** Resolve namespace aliases in the same captured structure, including layered mutation frontiers. */
export function workspaceStructureSnapshot(
	root: string,
	entries: Map<string, WorkspaceStructureEntry>,
	complete: boolean,
): WorkspaceStructureSnapshot {
	const objects = new Map<string, string[]>();
	for (const [name, entry] of entries)
		if (entry.kind === "file" && entry.object) {
			const aliases = objects.get(entry.object) ?? [];
			aliases.push(name);
			objects.set(entry.object, aliases);
		}
	for (const names of objects.values()) {
		// A FUSE overlay reports each name's own link count: one below the names found sharing its object is stale, not a hidden name.
		const links = Math.max(
			...names.map(
				(name) =>
					(
						entries.get(name) as Extract<
							WorkspaceStructureEntry,
							{
								kind: "file";
							}
						>
					).links,
			),
		);
		if (links < 2) continue;
		if (links !== names.length) complete = false;
		const aliases = Object.freeze(names.map((name) => path.join(root, name)).sort());
		for (const name of names) {
			const entry = entries.get(name)! as Extract<
				WorkspaceStructureEntry,
				{
					kind: "file";
				}
			>;
			entries.set(name, { ...entry, aliases });
		}
	}
	return Object.freeze({ root, entries, complete });
}
export function captureWorkspaceStructureEntry(
	{ info: stat, link: linkTarget, entries: children }: Awaited<ReturnType<typeof captureFilesystemEntry>>,
	excludeEntries: readonly string[] = [],
): WorkspaceStructureEntry {
	const change = {
		changeDigest: statChangeDigest(stat),
		changeTimeMs: statMilliseconds(stat, "ctime"),
	};
	if (stat.isSymbolicLink()) {
		return {
			kind: "symlink",
			target: linkTarget!,
			targetDigest: sha256Digest(linkTarget!),
			...change,
		};
	}
	if (stat.isDirectory()) {
		const excluded = new Set(excludeEntries);
		const entries = children!.filter((entry) => !excluded.has(entry.name));
		return {
			kind: "directory",
			observed: Object.fromEntries(
				FILESYSTEM_OBSERVATION_FIELDS.map((field) => [field, stat[field]]),
			) as FilesystemObservationEvidence,
			entriesDigest: directoryEntriesDigest(entries),
			metadataDigest: filesystemMetadataDigest(stat),
			...change,
			mode: Number(stat.mode & 511n),
			uid: Number(stat.uid),
			gid: Number(stat.gid),
		};
	}
	if (stat.isFile()) {
		return {
			kind: "file",
			metadataDigest: filesystemMetadataDigest(stat),
			...change,
			mode: process.platform === "win32" ? 0 : Number(stat.mode & 511n),
			size: Number(stat.size),
			links: Number(stat.nlink),
			object: `${stat.dev}:${stat.ino}`,
			modified: String(stat.mtimeNs),
			ownership: `${stat.uid}:${stat.gid}:${stat.mode & 3584n}`,
		};
	}
	return { kind: "unsupported", type: filesystemEntryType(stat), ...change };
}
/** Validated transaction changes, ordered for replay without copying or hashing their bytes. */
interface WorkspaceTransactionEffect {
	readonly logicalPath: string;
	readonly relativePath: string;
	readonly change:
		| (WorkspaceRegularDelta & {
				readonly kind?: "file";
		  })
		| {
				readonly kind: "directory";
				readonly before?: Extract<
					WorkspaceStructureEntry,
					{
						readonly kind: "directory";
					}
				>;
				readonly after?: Extract<
					WorkspaceStructureEntry,
					{
						readonly kind: "directory";
					}
				>;
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
		if (
			!relativePath ||
			path.isAbsolute(relativePath) ||
			relativeFilesystemPath(".", relativePath) === undefined
		) {
			return {
				effects: [],
				complete: false,
				reason: `invalid_delta:${delta.relativePath}`,
			};
		}
		if (byPath.has(relativePath))
			return {
				effects: [],
				complete: false,
				reason: `duplicate_delta:${delta.relativePath}`,
			};
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
	const originals = anchors(before);
	const results = anchors(after);
	for (const relativePath of names) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		const delta = byPath.get(relativePath);
		if (delta) {
			const reason = regularDeltaFailure(relativePath, previous, current, delta);
			if (reason) return { effects: [], complete: false, reason };
			let change: WorkspaceRegularDelta =
				current?.kind === "file"
					? {
							...delta,
							afterModified: current.modified,
							afterAliases: (current.aliases ?? [path.join(after.root, relativePath)])
								.map((name) => projection.toLogical(name))
								.sort(),
						}
					: delta;
			if (previous?.kind === "file")
				change = {
					...change,
					aliases: (previous.aliases ?? [path.join(before.root, relativePath)])
						.map((name) => projection.toLogical(name))
						.sort(),
				};
			if (current?.kind === "file" && current.object) {
				const original = originals.get(current.object);
				const anchor = original ?? results.get(current.object)!;
				if (original !== undefined || anchor !== relativePath) {
					if (!byPath.has(anchor))
						return {
							effects: [],
							complete: false,
							reason: `object_anchor_missing:${anchor}`,
						};
					change = {
						...change,
						object: {
							path: projection.toLogical(path.join(after.root, anchor)),
							before: original !== undefined,
						},
					};
				}
				const source = original === undefined ? undefined : before.entries.get(original);
				if (
					source?.kind === "file" &&
					(source.modified !== current.modified || !sameContent(byPath.get(anchor)!.before, delta.after))
				) {
					change = { ...change, operation: "write_contents" };
				}
			}
			effects.push({
				logicalPath: projection.toLogical(path.join(after.root, relativePath)),
				relativePath,
				change,
			});
			continue;
		}
		if (sameStructureEntry(previous, current)) continue;
		if (previous?.kind === "directory" || current?.kind === "directory") {
			if (previous?.kind === "directory" && current?.kind === "directory") {
				if (previous.metadataDigest !== current.metadataDigest) {
					return {
						effects: [],
						complete: false,
						reason: `unsupported_directory_metadata:${relativePath}`,
					};
				}
				continue;
			}
			const logicalPath = projection.toLogical(path.join(after.root, relativePath));
			if (previous === undefined && current?.kind === "directory") {
				effects.push({
					logicalPath,
					relativePath,
					change: { kind: "directory", after: current },
				});
				continue;
			}
			if (previous?.kind === "directory" && current === undefined) {
				effects.push({
					logicalPath,
					relativePath,
					change: { kind: "directory", before: previous },
				});
				continue;
			}
			return {
				effects: [],
				complete: false,
				reason: `unsupported_directory_type_change:${relativePath}`,
			};
		}
		return {
			effects: [],
			complete: false,
			reason: `untracked_inode_transition:${relativePath}`,
		};
	}
	return {
		effects: Object.freeze(
			orderWorkspaceChanges(effects, (effect) => ({
				change: effect.change,
				depth: effect.relativePath.split(path.sep).filter(Boolean).length,
				key: effect.relativePath,
			})),
		),
		complete: true,
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
		if (previous.links > 1 && previous.aliases?.length !== previous.links)
			return `unproven_aliases:${relativePath}`;
		if (delta.beforeMode !== undefined && delta.beforeMode !== previous.mode)
			return `delta_before_mode:${relativePath}`;
		if (delta.before.byteLength !== previous.size) return `delta_before_size:${relativePath}`;
	}
	if (delta.after === undefined)
		return delta.before === undefined || current !== undefined
			? `delta_delete_shape:${relativePath}`
			: undefined;
	if (current?.kind !== "file") return `delta_after_type:${relativePath}`;
	if (previous?.kind === "file" && previous.ownership !== current.ownership)
		return `unsupported_file_ownership:${relativePath}`;
	if (current.links > 1 && current.aliases?.length !== current.links)
		return `unproven_aliases:${relativePath}`;
	if (delta.afterMode !== undefined && delta.afterMode !== current.mode)
		return `delta_after_mode:${relativePath}`;
	if (delta.after.byteLength !== current.size) return `delta_after_size:${relativePath}`;
}
export function snapshotDependency(
	logicalPath: string,
	entry: WorkspaceTreeEntry | undefined,
	parent: WorkspaceTreeEntry | undefined,
	role: Extract<
		DynamicDependency,
		{
			kind: "file";
		}
	>["role"] = "input",
	options: {
		readonly excludedEntries?: readonly string[];
		readonly parentExcludedEntries?: readonly string[];
		readonly listed?: boolean;
	} = {},
): DynamicDependency | undefined {
	if (!entry) {
		return {
			kind: "absence",
			path: logicalPath,
			...(parent?.kind === "directory" ? { parentEntriesDigest: parent.entriesDigest } : {}),
			...(parent?.kind === "directory" && options.parentExcludedEntries?.length
				? {
						parentExcludedEntries: Object.freeze([...options.parentExcludedEntries].sort()),
					}
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
			if (options.listed === false)
				return {
					kind: "directory",
					path: logicalPath,
					metadataDigest: entry.metadataDigest,
				};
			return {
				kind: "directory",
				path: logicalPath,
				entriesDigest: entry.entriesDigest,
				metadataDigest: entry.metadataDigest,
				...(options.excludedEntries?.length
					? {
							excludedEntries: Object.freeze([...options.excludedEntries].sort()),
						}
					: {}),
			};
		case "symlink":
			return {
				kind: "symlink",
				path: logicalPath,
				target: entry.target,
				targetDigest: entry.targetDigest,
			};
		case "unsupported":
			return undefined;
	}
}
const STRUCTURE_IDENTITY_FIELDS = {
	file: ["size", "metadataDigest", "object"],
	directory: ["entriesDigest", "metadataDigest"],
	symlink: ["targetDigest"],
	unsupported: ["type"],
} satisfies {
	[Kind in WorkspaceStructureEntry["kind"]]: readonly (keyof Extract<
		WorkspaceStructureEntry,
		{
			kind: Kind;
		}
	>)[];
};
export function sameStructureEntry(
	left: WorkspaceStructureEntry | undefined,
	right: WorkspaceStructureEntry | undefined,
	identity: "semantic" | "change" = "semantic",
): boolean {
	if (left === right) return true;
	if (!left || !right || left.kind !== right.kind) return false;
	return (
		(identity === "change"
			? left.changeDigest === right.changeDigest
			: STRUCTURE_IDENTITY_FIELDS[left.kind].every(
					(field) => Reflect.get(left, field) === Reflect.get(right, field),
				)) &&
		(left.kind !== "file" ||
			workspaceAliasIdentity(left.aliases) === workspaceAliasIdentity((right as typeof left).aliases))
	);
}
function changedRootMetadata(
	before: WorkspaceStructureEntry | undefined,
	after: WorkspaceStructureEntry | undefined,
): string | undefined {
	if (before?.kind !== "directory" || after?.kind !== "directory")
		return "unsupported_workspace_root_transition";
	return before.metadataDigest === after.metadataDigest ? undefined : "unsupported_workspace_root_metadata";
}
export function directoryEntriesDigest(
	entries: readonly (FilesystemTypeEvidence & {
		readonly name: string;
	})[],
): Sha256Digest {
	return digestObject(entries.map((entry) => `${filesystemEntryType(entry)}\0${entry.name}`).sort());
}
/** Kernel-maintained identity/change fields detect writes without making timestamps replay semantics. */
function statMilliseconds(stat: Stats | BigIntStats, field: "ctime" | "mtime"): number {
	const ns = (stat as BigIntStats)[`${field}Ns`];
	if (ns === undefined) return Number(stat[`${field}Ms`]);
	const remainder = ((ns % 1000000000n) + 1000000000n) % 1000000000n;
	return Number((ns - remainder) / 1000000000n) * 1000 + Number(remainder) / 1000000;
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

		type:
			(
				{
					[0o100000]: "file",
					[0o040000]: "directory",
					[0o120000]: "symlink",
				} as Record<number, string>
			)[Number(stat.mode) & 0o170000] ?? "other",
	});
}
interface SandboxChangeTarget {
	readonly root: string;
	readonly target: string;
	readonly resource: string;
	/** Validate captured file bytes or directory existence in the same lock, without writing. */
	readonly validationOnly?: true;
	/** Successful access checks on existing inputs must still hold at adoption. */
	readonly accessMode?: number;
}
export interface SandboxFileChange extends SandboxChangeTarget, Omit<WorkspaceRegularDelta, "relativePath"> {
	readonly kind?: "file";
}
export type SandboxDirectoryState = import("./workspace-sandbox.ts").WorkspaceDirectoryState;
export interface SandboxDirectoryChange extends SandboxChangeTarget {
	readonly kind: "directory";
	/** Preserve native creation policy; private directory permissions are not Actor metadata. */
	readonly operation?: "mkdir";
	readonly before?: SandboxDirectoryState;
	readonly after?: SandboxDirectoryState;
}
export type SandboxWorkspaceChange = SandboxFileChange | SandboxDirectoryChange;
interface RegularFileState {
	readonly content: ContentVersion;
	readonly mode: number;
	readonly identity?: BigIntStats;
	readonly settled?: string;
}
export interface SandboxExecutionDelta {
	readonly output: ToolSettlement;
	readonly changes: readonly SandboxWorkspaceChange[];
}
export interface WorkspaceExecutionSnapshot extends SandboxExecutionDelta {
	readonly readInputs?: WorldBranch<ToolSettlement> & {
		readonly inputsOnly: true;
	};
	readonly executionMetrics: WorldExecutionMetrics;
	readonly dispose?: () => Promise<void>;
}
export type WorkspaceSandboxDriver = "auto" | "git" | "overlayfs" | "in_memory";
export interface WorkspaceSandboxOptions extends LinuxOverlayfsOptions {
	readonly gitBinary?: string;
	/** Auto is portable Git unless a trace-guarded runtime explicitly qualifies a COW driver. */
	readonly driver?: WorkspaceSandboxDriver;
	/** Root edits and writes run in memory over the workspace itself (default); false always allocates a private workspace. */
	readonly inPlaceMutations?: boolean;
	/** OverlayFS layers over the workspace itself rather than a snapshot, so its files keep their own times and inodes. */
	readonly liveLower?: boolean;
}
export interface SandboxWorkspaceContext {
	readonly contents?: ContentChunkPool;
	readonly sourceRoot: string;
	readonly sandboxRoot: string;
	readonly processRoot: string;
	/** Native metadata for pinned snapshot objects; shared by every process in this private workspace. */
	readonly metadataImage?: string;
	/** Mount policy used to project source access times; adoption must revalidate it. */
	readonly metadataMounts?: string;
	readonly metadataObjects?: ReadonlyMap<string, string>;
	readonly projectMetadata?: (stat: FilesystemObservationEvidence) => FilesystemObservationEvidence;
	/** Root entry names owned by the isolation substrate and invisible to effect observation. */
	readonly observationExcludes: readonly string[];
	/** Driver-native content-free structure view shared by outer and nested process observers. */
	readonly structure: WorkspaceStructureDriver;
	/** Content-addressed mutation intervals, independent of any process or tool implementation. */
	readonly transactions: WorkspaceTransactionDriver;
	/** Capture named object/namespace transitions omitted by a content-only change index. */
	readonly captureChanges?: (frontier: readonly string[]) => Promise<readonly SandboxFileChange[]>;
	/** Source notifications since this fork's baseline; lookup hints, never freshness authority. */
	readonly sourceChanges?: () => ResourceChangeSet;
}
/** Storage remains pinned by its allocator; only the observation owner reconstructs its journal. */
export interface SandboxWorkspaceObservation {
	readonly id: string;
	readonly sourceRoot: string;
	readonly sandboxRoot: string;
	readonly processRoot: string;
	readonly baseline: WorkspaceContentSnapshot;
	readonly baselineFrontier: Map<string, RegularFileState | undefined>;
	readonly sourceDevice: bigint;
	readonly initialStructure: WorkspaceStructureSnapshot;
	readonly metadata?: Awaited<ReturnType<typeof preserveSnapshotMetadata>>;
	readonly modes?: ReadonlyMap<string, number>;
	readonly liveBase?: WorkspaceStructureSnapshot;
	readonly overlay?: {
		readonly upperRoot: string;
		readonly device: string;
		readonly baseline: WorkspaceStructureSnapshot;
	};
	readonly clock: {
		readonly path: string;
		readonly links: 0 | 1;
		readonly roots: readonly string[];
	};
	readonly hints: ResourceChangeSet;
}
interface WorkspaceObservation extends SandboxWorkspaceContext, SandboxWorkspaceObservation {
	readonly readBase: (resource: string, maxBytes?: number) => Promise<RegularFileState | undefined>;
}
const workspaceVersions = new WeakMap<
	WorkspaceStructureSnapshot,
	(resource: string) => Promise<ContentVersion | undefined>
>();
/** The interval borrows its original version, including after a later writer replaces that name. */
export const workspaceInputVersion = (snapshot: WorkspaceStructureSnapshot, resource: string) =>
	workspaceVersions.get(snapshot)?.(resource);
function createWorkspaceObservation(
	data: SandboxWorkspaceObservation,
	sourceChanges: () => ResourceChangeSet,
	contents?: ContentChunkPool,
): WorkspaceObservation {
	let workspace!: WorkspaceObservation;
	const readBase = async (resource: string, maxBytes: number) => {
		const file = data.baseline.files.get(resource);
		if (!file) return undefined;
		if (file.content.byteLength > maxBytes) throw new Error("workspace input exceeds capture limit");
		return {
			content: file.content,
			mode: process.platform === "win32" ? 0 : Number(file.stat.mode & 511n),
			identity: file.stat,
		};
	};
	const versions =
		contents && sharedWalk((resource: string) => readBase(resource, contents.limits.maxLogicalBytes));
	let retainedStructure: (() => Promise<WorkspaceStructureSnapshot>) | undefined;
	const structure: WorkspaceStructureDriver = {
		capture: () => retainedStructure?.() ?? captureSandboxStructure(data),
		retain: (versions) => {
			versions.watch(data.overlay ? data.clock.roots : [data.sandboxRoot]);
			const retained = new WorkspaceStructureCapture(
				data.sandboxRoot,
				{
					maxFiles: speculativeConfig().filesystem.maxTreeEntries,
					exclude: speculativeConfig().filesystem.excludes,
				},
				versions,
			);
			const overlay = data.overlay && createOverlayStructureCapture(data.sandboxRoot, data.overlay, versions);
			const capture = overlay ?? (() => retained.capture());
			retainedStructure = capture;
			return () => {
				retained.clear();
				if (retainedStructure === capture) retainedStructure = undefined;
			};
		},
	};
	const transactions = deferredWorkspaceTransactionDriver(() => createWorkspaceTransactionDriver(workspace));
	workspace = {
		...data,
		contents,
		structure,
		transactions,
		sourceChanges,
		observationExcludes: speculativeConfig().filesystem.excludes,
		metadataImage: data.metadata?.image,
		metadataMounts: data.metadata?.mounts,
		metadataObjects: data.metadata?.image ? data.metadata.objects : undefined,
		projectMetadata: (stat: FilesystemObservationEvidence): FilesystemObservationEvidence => {
			const row = data.metadata?.mappings.get(`${stat.dev}:${stat.ino}`);
			if (!row) return { ...stat, dev: data.sourceDevice };
			const [physical, source, atimePolicy] = row;
			const links = source.nlink + stat.nlink - physical.nlink;
			const unchanged =
				stat.size === physical.size && stat.mtimeNs === physical.mtimeNs && stat.ctimeNs === physical.ctimeNs;
			// Copying gives an object a new ctime, which can spuriously trigger relatime on its first read.
			const copiedAccess =
				unchanged &&
				stat.atimeNs > physical.atimeNs &&
				(atimePolicy === 1 ||
					(atimePolicy === 2 &&
						source.atimeNs > source.mtimeNs &&
						source.atimeNs > source.ctimeNs &&
						stat.atimeNs / 1000000000n - source.atimeNs / 1000000000n < 86400n));
			return {
				...Object.fromEntries(
					FILESYSTEM_OBSERVATION_FIELDS.map((field) => [
						field,
						(stat[field] === physical[field] && (field !== "blocks" || unchanged)) ||
						(field === "atimeNs" && copiedAccess)
							? source[field]
							: stat[field],
					]),
				),
				dev: data.sourceDevice,
				ino: source.ino,
				nlink: links < 0n ? 0n : links,
			} as FilesystemObservationEvidence;
		},
		captureChanges: (frontier: readonly string[]) => collectSandboxChanges(workspace, frontier),
		readBase: async (
			resource: string,
			maxBytes = contents?.limits.maxLogicalBytes ?? speculativeConfig().filesystem.maxTreeBytes,
		) => {
			const state = await (versions ? versions(resource) : readBase(resource, maxBytes));
			if (state && state.content.byteLength > maxBytes)
				throw new Error("workspace input exceeds capture limit");
			const mode = data.liveBase
				? undefined
				: (data.metadata?.modes.get(slash(resource)) ?? data.modes?.get(slash(resource)));
			return state && mode !== undefined ? { ...state, mode } : state;
		},
	};
	return workspace;
}
async function captureSandboxStructure(
	data: Pick<SandboxWorkspaceObservation, "sandboxRoot" | "overlay" | "clock">,
	fence = false,
): Promise<WorkspaceStructureSnapshot> {
	const snapshot = await (data.overlay
		? createOverlayStructureCapture(data.sandboxRoot, data.overlay)()
		: captureWorkspaceStructure(data.sandboxRoot, {
				maxFiles: speculativeConfig().filesystem.maxTreeEntries,
				exclude: speculativeConfig().filesystem.excludes,
			}));
	if (fence) {
		const clock = await openWorkspaceChangeClock(data);
		try {
			await advanceWorkspaceChangeClock(clock, snapshot);
		} finally {
			await clock.handle.close();
		}
	}
	return snapshot;
}
async function openWorkspaceChangeClock({
	sandboxRoot,
	clock,
}: Pick<SandboxWorkspaceObservation, "sandboxRoot" | "clock">) {
	const handle = await (clock.links === 0
		? openAnonymousFilesystemFile(clock.path)
		: open(clock.path, fsConstants.O_WRONLY | fsConstants.O_CREAT | (fsConstants.O_NOFOLLOW ?? 0), 0o600));
	try {
		const [workspaceInfo, clockInfo, ...rootInfo] = await Promise.all([
			lstat(sandboxRoot),
			handle.stat(),
			...clock.roots.map((root) => lstat(root)),
		]);
		if (
			!workspaceInfo.isDirectory() ||
			!clockInfo.isFile() ||
			clockInfo.nlink !== clock.links ||
			rootInfo.length === 0 ||
			rootInfo.some((root) => !root.isDirectory() || root.dev !== clockInfo.dev)
		) {
			throw new Error("workspace transaction clock is not private or its backing timestamp domain changed");
		}
		return { handle, identity: clockInfo };
	} catch (error) {
		await handle.close();
		throw error;
	}
}
async function advanceWorkspaceChangeClock(
	clock: Awaited<ReturnType<typeof openWorkspaceChangeClock>> | undefined,
	snapshot: WorkspaceStructureSnapshot,
): Promise<void> {
	let boundary = Number.NEGATIVE_INFINITY;
	for (const entry of snapshot.entries.values()) boundary = Math.max(boundary, entry.changeTimeMs);
	if (!Number.isFinite(boundary)) throw new Error("workspace change clock boundary is unavailable");
	if (!clock) throw new Error("workspace transaction clock is unavailable");
	await advanceFilesystemClock(clock.handle, boundary, clock.identity);
}
export interface SandboxWorkspaceBranchOptions extends WorkspaceSandboxOptions {
	readonly cwd: string;
	readonly action: SpeculativeToolExecutionContext["action"];
	readonly parentCheckpoint?: WorldCheckpoint;
	/** Borrow an immutable workspace when validate is supplied; preparation never proves current inputs. */
	readonly preparation?: QualifiedWorkspaceSandboxDriver;
	/** An operation boundary may supply its complete input/effect delta, avoiding a second tree scan. */
	readonly execute: (workspace: SandboxWorkspaceContext) => Promise<ToolSettlement | SandboxExecutionDelta>;
	/** Optional backend metrics collected during execute/capture and sealed into the branch. */
	readonly executionMetrics?: () => WorldExecutionMetrics;
	/** Seal evidence after generic capture; return a complete delta when refining its operation semantics. */
	readonly afterCapture?: (
		workspace: SandboxWorkspaceContext,
		capture: SandboxExecutionDelta,
	) => Promise<readonly SandboxWorkspaceChange[] | void>;
	/** Optional exact freshness proof captured by the operation-specific execution substrate. */
	readonly validate?: () => Promise<ResourceValidation>;
}
export interface PrepareSandboxWorkspaceOptions extends WorkspaceSandboxOptions {
	readonly signal?: AbortSignal;
}
type PrivateSandboxWorkspace = Awaited<ReturnType<WorkspaceSandboxService["createPrivateSandboxWorkspace"]>>;
function regularStructureTransitions(
	before: WorkspaceStructureSnapshot,
	after: WorkspaceStructureSnapshot,
): readonly string[] {
	if (!before.complete || !after.complete) throw new Error("workspace_structure_limit");
	if (before.entries === after.entries) return [];
	const paths: string[] = [];
	for (const relativePath of [...new Set([...before.entries.keys(), ...after.entries.keys()])].sort()) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		const kind = previous?.kind ?? current?.kind;
		if (sameStructureEntry(previous, current, "change")) continue;
		if ((previous && current && previous.kind !== current.kind) || (kind !== "file" && kind !== "directory"))
			throw new Error(`unsupported_workspace_transition:${relativePath}`);
		if (kind === "file") paths.push(relativePath);
	}
	return Object.freeze(paths);
}
function workspaceAncestors(resource: string): string[] {
	const names: string[] = [];
	for (let current = path.normalize(resource); ; current = path.dirname(current)) {
		if (path.parse(current).root || current === "..")
			throw new Error("workspace ownership path escapes workspace");
		names.push(current === "." ? "" : current);
		if (current === ".") return names;
	}
}
function sameWorkspaceChangeSnapshot(
	left: WorkspaceStructureSnapshot,
	right: WorkspaceStructureSnapshot,
): boolean {
	if (!left.complete || !right.complete || left.entries.size !== right.entries.size) return false;
	if (left.entries === right.entries) return true;
	for (const [relativePath, entry] of left.entries) {
		if (!sameStructureEntry(entry, right.entries.get(relativePath), "change")) return false;
	}
	return true;
}
// The execution world mirrors everything the actor can read below cwd. Git metadata is
// replaced by the private repository and commit's own temporary files are internal.
const SANDBOX_STAGING_FILE_PREFIX = ".pi-speculative-";
export interface WorkspaceCheckpoint {
	readonly token: WorldCheckpoint;
	readonly sourceRoot: string;
	parent?: WorkspaceCheckpoint;
	readonly changes: readonly SandboxWorkspaceChange[];
	committed: boolean;
}
// A handed-out checkpoint outlives its branch. Service disposal also closes every pool.
const retiredWorkspaceCheckpoints = new FinalizationRegistry<() => Promise<void>>((release) => {
	void release().catch(() => {});
});
export interface QualifiedWorkspaceSandboxDriver {
	readonly driver: Exclude<WorkspaceSandboxDriver, "auto">;
	readonly fingerprint: string;
}
/** Bound filesystem operations validate all captured inputs and effects in the commit transaction. */
const capturedWorkspaceInputs = Symbol("captured workspace inputs");
type SandboxPreparation = QualifiedWorkspaceSandboxDriver | typeof capturedWorkspaceInputs;
/** Owns workspace repositories and commit serialization for one extension/runtime lifecycle. */
export class WorkspaceSandboxService {
	readonly configuration = currentSpeculativeConfiguration();
	private store?: ProvenanceCertificateStore;
	private storage?: Promise<{
		readonly store: ProvenanceCertificateStore;
		readonly ownedRoot?: string;
	}>;
	private readonly trees = new Map<
		string,
		Promise<{
			capture: WorkspaceStructureCapture;
			contents?: ContentChunkPool;
			release?: () => Promise<void>;
		}>
	>();
	private readonly contents = new Set<ContentChunkPool>();
	/** Checkpoints belong to this service, including its sibling execution worlds. */
	private readonly checkpoints = new WeakMap<WorldCheckpoint, WorkspaceCheckpoint>();
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly overlayfsCapabilities = new LinuxOverlayfsCapabilityRegistry();
	constructor(store?: ProvenanceCertificateStore) {
		this.store = store;
		this.configuration.bind(this);
	}
	useContentStore(store: ProvenanceCertificateStore): void {
		if (!this.storage) this.store = store;
	}
	async fingerprint(options: WorkspaceSandboxOptions = {}, sourceRoot?: string): Promise<string> {
		this.assertOpen();
		return (await this.resolveDriver(options, sourceRoot)).fingerprint;
	}
	async qualify(
		options: WorkspaceSandboxOptions,
		sourceRoot: string,
	): Promise<QualifiedWorkspaceSandboxDriver> {
		this.assertOpen();
		return await this.resolveDriver(options, sourceRoot);
	}
	branch(
		options: Pick<SandboxWorkspaceBranchOptions, "cwd" | "action" | "parentCheckpoint" | "validate">,
		snapshot: WorkspaceExecutionSnapshot,
	): WorldBranch<ToolSettlement> {
		this.assertOpen();
		return this.createBranch(
			snapshot,
			path.resolve(options.cwd),
			options.action,
			this.checkpoint(options.parentCheckpoint, options.cwd),
			options.validate,
		);
	}
	async withWorkspace<T>(
		cwd: string,
		run: (workspace: SandboxWorkspaceContext) => Promise<T>,
		options: Pick<WorkspaceSandboxOptions, "gitBinary" | "liveLower"> | string = {},
	): Promise<T> {
		this.assertOpen();
		const config = typeof options === "string" ? { gitBinary: options } : options;
		return await this.withPrivateWorkspace(cwd, config.gitBinary ?? "git", "git", config, run);
	}
	async commitDelta(delta: SandboxExecutionDelta, privateView?: FilesystemReadView): Promise<ToolSettlement> {
		this.assertOpen();
		const changes = ownSandboxChanges(delta.changes);
		const release = retainWorkspaceChanges(changes);
		try {
			return await this.commit({ output: delta.output, changes }, undefined, privateView);
		} finally {
			await release();
		}
	}
	dispose(): Promise<void> {
		return this.lifetime.dispose(async () => {
			try {
				await this.closePools();
			} finally {
				this.overlayfsCapabilities.dispose();
				await Promise.allSettled([...this.contents].map((pool) => pool.dispose()));
				this.contents.clear();
				const storage = await this.storage;
				if (storage?.ownedRoot) {
					await storage.store.dispose();
					await removeOwnedTree(storage.ownedRoot);
				}
			}
		});
	}
	acceptsCheckpoint(checkpoint: WorldCheckpoint, cwd: string): boolean {
		const owned = this.checkpoints.get(checkpoint);
		return (
			!this.lifetime.sealed &&
			!!owned &&
			filesystemPathKey(owned.sourceRoot) === filesystemPathKey(path.resolve(cwd))
		);
	}
	checkpoint(checkpoint: WorldCheckpoint | undefined, cwd: string): WorkspaceCheckpoint | undefined {
		if (checkpoint === undefined) return undefined;
		const owned = this.checkpoints.get(checkpoint);
		if (!owned) throw new Error("Execution world checkpoint belongs to another backend.");
		if (filesystemPathKey(owned.sourceRoot) !== filesystemPathKey(path.resolve(cwd)))
			throw new Error("Execution world checkpoint belongs to another workspace.");
		return owned;
	}
	private async createContentPool(): Promise<ContentChunkPool | undefined> {
		this.assertOpen();
		for (const pool of this.contents) if (pool.closed) this.contents.delete(pool);
		const { store } = await (this.storage ??= this.store
			? Promise.resolve({ store: this.store })
			: mkdtemp(path.join(os.tmpdir(), "pi-workspace-content-")).then((ownedRoot) => ({
					store: new ProvenanceCertificateStore(ownedRoot),
					ownedRoot,
				})));
		const pool = new ContentChunkPool(store);
		this.contents.add(pool);
		return pool;
	}
	private async resolveDriver(
		options: WorkspaceSandboxOptions,
		_sourceRoot?: string,
	): Promise<QualifiedWorkspaceSandboxDriver> {
		const driver = options.driver === "auto" || options.driver === undefined ? "in_memory" : options.driver;
		if (driver !== "overlayfs") return { driver, fingerprint: "immutable-cas-workspace:1" };
		const capability = await this.overlayfsCapabilities.capability(options);
		if (!capability.available) throw new Error(capability.detail);
		return {
			driver,
			fingerprint: `immutable-cas-overlayfs:${capability.fingerprint}`,
		};
	}
	createExecutionWorld(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
		this.assertOpen();
		// Generic mutations retain Git unless OverlayFS was explicitly requested. Process routes
		// qualify a live lower from their invocation cwd to preserve native metadata.
		const resolvedOptions: WorkspaceSandboxOptions =
			options.driver === "overlayfs" ? options : { ...options, driver: "git" };
		const roots = new Set<string>();
		return {
			id: "git_worktree",
			scope: "fallback",
			isolation: "workspace_branch",
			speculation: {
				capabilities: [...WORKSPACE_PATH_MUTATION_EFFECTS.capabilities, "validation.resource_snapshot"],
				dependsOn: filesystemExecutionDependsOn,
				acceptsCheckpoint: (checkpoint, cwd) => this.acceptsCheckpoint(checkpoint, cwd),
				fingerprint: async ({ action, effect, parentCheckpoint }) => {
					this.assertOpen();
					if (effect === "observation" && !parentCheckpoint)
						throw new Error("Reads use captured inputs unless based on a private checkpoint");
					if (action && !(action.executionContext as ToolInvocation | undefined)?.filesystem)
						throw new Error("Workspace execution requires an explicitly bound filesystem operation");
					return (await this.resolveDriver(resolvedOptions)).fingerprint;
				},
				prepare: async ({ cwd, signal }) => {
					this.assertOpen();
					throwIfAborted(signal);
					if (resolvedOptions.inPlaceMutations !== false) return;
					roots.add(path.resolve(cwd));
					await this.prepare(cwd, { ...resolvedOptions, signal });
				},
				execute: async (context) => {
					this.assertOpen();
					const sourceRoot = path.resolve(context.cwd);
					roots.add(sourceRoot);
					return this.executeMutation(context, resolvedOptions);
				},
			},
			dispose: async () => {
				const ownedRoots = [...roots];
				roots.clear();
				await this.lifetime.run(() => this.closePools(ownedRoots));
			},
		};
	}
	private createBranch(
		snapshot: WorkspaceExecutionSnapshot,
		sourceRoot: string,
		action: SpeculativeToolExecutionContext["action"],
		parent?: WorkspaceCheckpoint,
		validate?: () => Promise<ResourceValidation>,
	): WorldBranch<ToolSettlement> {
		const validateParent = validate;
		if (snapshot.readInputs)
			validate = async () => {
				const inputs = await snapshot.readInputs?.validate?.();
				if (inputs && inputs.status !== "valid") return inputs;
				return (await validateParent?.()) ?? inputs!;
			};
		const { changes } = snapshot;
		const { executionFingerprint } = action;
		const backend = "git_worktree";
		const id = randomUUID();
		const checkpoint = Object.freeze({
			backend,
			id,
			lineage: parent?.token.lineage ?? id,
			depth: (parent?.token.depth ?? -1) + 1,
		});
		const retained: WorkspaceCheckpoint = {
			token: checkpoint,
			sourceRoot,
			parent,
			changes,
			committed: false,
		};
		retiredWorkspaceCheckpoints.register(checkpoint, retainWorkspaceChanges(changes));
		this.checkpoints.set(checkpoint, retained);
		const commit = new EffectCommit<ToolSettlement>();
		const lifetime = new RuntimeLifecycleLane();
		const inputs = new Map<string, ResourceInput>();
		const release = snapshot.dispose ?? retainWorkspaceChanges(changes);
		let reading = false;
		let readsTransferred = false;
		let transferred = false;
		return {
			backend,
			checkpoint,
			output: snapshot.output,
			validate,
			resources: Object.freeze([
				...new Set(changes.filter((change) => !change.validationOnly).map((change) => change.resource)),
			]),
			capturedBytes: changes.reduce((total, change) => total + sandboxChangeBytes(change), 0),
			executionMetrics: Object.freeze({ ...snapshot.executionMetrics }),
			compatibility: Object.freeze({
				status: "compatible" as const,
				backend,
				executionFingerprint,
			}),
			commit: () =>
				lifetime.track(
					commit.run(async () => {
						if (lifetime.sealed)
							throw effectCommitFailure(new Error("workspace branch is disposed"), "recoverable");
						const current = await validate?.();
						if (current && current.status !== "valid")
							throw effectCommitFailure(
								new Error("workspace inputs changed before commit"),
								"recoverable",
								undefined,
								current.cause,
							);
						return this.commit(snapshot, inputs).then((output) => {
							// A proven commit anchors future forks in the live workspace, independently of unadopted ancestors.
							retained.committed = true;
							retained.parent = undefined;
							if (lifetime.sealed) inputs.clear();
							return output;
						});
					}),
				),
			takeReadInputs: (maxBytes) => {
				if (reading || lifetime.sealed || commit.result) return Promise.resolve(undefined);
				reading = true;
				return lifetime.admit(async () => {
					if (snapshot.readInputs && snapshot.readInputs.capturedBytes <= maxBytes) {
						const validity = await snapshot.readInputs.validate?.();
						if (validity?.status === "valid") {
							readsTransferred = true;
							return snapshot.readInputs;
						}
					}
					// Every file this branch read keeps its pre-image until commit; each serves reads while it stays unchanged.
					const preimages = new Map<string, ResourceInput>();
					for (const change of changes)
						if (change.kind !== "directory" && change.before) preimages.set(change.target, change.before);
					if (!preimages.size || lifetime.sealed) return undefined;
					const inputs = await createCommittedResourceInputs(action, sourceRoot, preimages, maxBytes).catch(
						() => undefined,
					);
					// Supplied pre-images use the same exact proof as every other retained input.
					const current = await inputs?.validate?.();
					if (inputs && !lifetime.sealed && current?.status === "valid") return inputs;
					await inputs?.dispose();
					return undefined;
				});
			},
			takeCommittedInputs: async (maxBytes) => {
				if (lifetime.sealed || !retained.committed || transferred) return undefined;
				transferred = true;
				return lifetime.admit(async () => {
					if (!inputs.size) return undefined;
					try {
						const branch = await createCommittedResourceInputs(action, sourceRoot, inputs, maxBytes);
						if (!lifetime.sealed) return branch;
						await branch.dispose();
						return undefined;
					} finally {
						inputs.clear();
					}
				});
			},
			dispose() {
				inputs.clear();
				return lifetime.dispose(async () => {
					if (!readsTransferred) await snapshot.readInputs?.dispose();
					await release();
				});
			},
		};
	}
	private async commit(
		execution: SandboxExecutionDelta,
		inputs?: Map<string, ResourceInput>,
		privateView?: FilesystemReadView,
	): Promise<ToolSettlement> {
		this.assertOpen();
		const { changes } = execution;
		let release: (() => Promise<void>) | undefined;
		const commit = (async () => {
			const contents = await this.createContentPool().catch((error) => {
				throw effectCommitFailure(error, "recoverable");
			});
			release = contents?.retain();
			await using source = privateView
				? undefined
				: await openFilesystemReadView().catch((error) => {
						throw effectCommitFailure(error, "recoverable");
					});
			const view = privateView ?? source!;
			const readonlySource = !privateView && process.platform === "linux";
			return await withCommitLocks(
				changes,
				async () => {
					const staged = new Map<SandboxFileChange, string>();
					const descriptors = new Map<SandboxFileChange, FileHandle>();
					const baselines = new Map<
						SandboxWorkspaceChange,
						RegularFileState | SandboxDirectoryState | undefined
					>();
					const applied: SandboxWorkspaceChange[] = [];
					const createdDirectories: string[] = [];
					const parents = new Set<string>();
					const objects = new Map<
						string,
						{
							source: SandboxFileChange;
							write?: SandboxFileChange;
							handle?: FileHandle;
							mode?: number;
						}
					>();
					const openDescriptors = (phase: readonly SandboxWorkspaceChange[]) =>
						mapFilesystem(phase, async (change) => {
							const group = objects.get("1:" + change.target);
							const writing =
								change.kind !== "directory" && !change.validationOnly && change.operation && !change.object;
							if (writing && change.after === undefined)
								throw new Error("A content write cannot delete a file");
							if (change.kind !== "directory" && (group || (writing && change.before !== undefined))) {
								if (!(await view.stat(change.target)).isFile())
									throw new Error("file object predecessor is not regular");
								const descriptor = await open(
									change.target,
									(writing || group?.write ? fsConstants.O_RDWR : fsConstants.O_RDONLY) |
										(fsConstants.O_NOFOLLOW ?? 0) |
										(fsConstants.O_NONBLOCK ?? 0),
								);
								descriptors.set(change, descriptor);
								if (group) group.handle = descriptor;
							}
						});
					const verifyDescriptor = async (change: SandboxWorkspaceChange) => {
						const descriptor = change.kind !== "directory" && descriptors.get(change);
						if (descriptor) {
							const before = baselines.get(change) as RegularFileState | undefined;
							const identity = await descriptor.stat({ bigint: true });
							if (
								!before?.identity ||
								identity.nlink !== BigInt(change.aliases?.length ?? 1) ||
								!sameFilesystemIdentity(before.identity, identity)
							)
								throw new Error("file object predecessor changed: " + change.resource);
						}
					};
					let nativeStarted = false;
					try {
						// Nothing has moved yet: every target is checked at this one moment, over one walk of the directories they share.
						const capture = sharedWalk((target) => captureFilesystemEntry(target, undefined, view));
						await mapFilesystem(changes, (change) => assertCommitTarget(change, capture));
						for (const change of changes)
							if (change.kind !== "directory" && change.object) {
								const reference = change.object;
								const key = `${Number(reference.before)}:${reference.path}`;
								const source = changes.find((candidate) => candidate.target === reference.path);
								if (
									!source ||
									source.kind === "directory" ||
									change.after === undefined ||
									(reference.before
										? source.before === undefined
										: source.after === undefined || source.object)
								)
									throw new Error("file object anchor is unavailable");
								const group = objects.get(key) ?? { source };
								if (reference.before) {
									if (change.operation) {
										if (
											group.write &&
											(!sameContent(group.write.after, change.after) ||
												group.write.afterMode !== change.afterMode)
										)
											throw new Error("inconsistent object contents");
										group.write = change;
									} else if (!sameContent(source.before, change.after))
										throw new Error("object change lacks a write");
									if (source.beforeMode !== change.afterMode) {
										if (group.mode !== undefined && group.mode !== change.afterMode)
											throw new Error("inconsistent object permissions");
										group.mode = change.afterMode;
									}
								} else if (!sameContent(source.after, change.after) || source.afterMode !== change.afterMode)
									throw new Error("inconsistent linked contents");
								objects.set(key, group);
							}
						// Capture directories before writable file handles trigger OverlayFS copy-up and change their times.
						for (const directories of [true, false]) {
							const phase = changes.filter((change) => (change.kind === "directory") === directories);
							// Ambient adapters settle copy-up or read metadata with their handles held; qualified source views need no such preparation.
							if (!directories && !readonlySource) await openDescriptors(phase);
							await mapFilesystem(phase, async (change) => {
								// A file whose identity still stands for the bytes this change replaces needs no rereading to prove them.
								const unchanged =
									change.kind !== "directory" && change.beforeIdentity && change.before
										? await view.stat(change.target).then(
												(info) => (fileIdentity(info) === change.beforeIdentity ? info : undefined),
												() => undefined,
											)
										: undefined;
								const current = unchanged
									? {
											content: (change as SandboxFileChange).before!,
											mode: Number(unchanged.mode & 511n),
											identity: unchanged,
										}
									: change.kind === "directory"
										? await readSandboxDirectoryState(change.target, undefined, view)
										: await readRegularState(change.target, undefined, view, contents);
								baselines.set(change, current);
								if (
									change.kind === "directory" && change.validationOnly
										? !!current !== !!change.before
										: !sameSandboxState(
												current,
												change.kind === "directory"
													? change.before
													: change.before && {
															content: change.before,
															mode: change.beforeMode ?? 0,
														},
											)
								)
									throw new Error(`resource changed before commit: ${change.resource}`);
								if (change.accessMode) await access(await view.realpath(change.target), change.accessMode);
								if (change.kind !== "directory" && change.aliases)
									await assertFileAliases(
										change.root,
										change.target,
										change.aliases,
										(current as RegularFileState | undefined)?.identity,
										view,
									);
								await verifyDescriptor(change);
							});
						}
						await view.verify();
						// Read-only source proofs finish before opening writable objects; every adapter validates before staging names.
						if (readonlySource) {
							await openDescriptors(changes.filter((change) => change.kind !== "directory"));
							await mapFilesystem(changes, verifyDescriptor);
						}
						await mapFilesystem(changes, async (change) => {
							if (
								!change.validationOnly &&
								change.kind !== "directory" &&
								!change.operation &&
								!change.object &&
								change.after !== undefined
							) {
								staged.set(change, await stageAtomicWrite(change.after, change.afterMode, change.root));
							}
						});
						await restoreModifiedTimes(
							[...staged].map(([change, temporary]) => [temporary, change.afterModified]),
						);
						// Pin every source name before any destination is removed or replaced; cycles use the same transaction.
						for (const change of changes)
							if (change.kind !== "directory" && change.object) {
								const reference = change.object;
								const group = objects.get(`${Number(reference.before)}:${reference.path}`)!;
								const prior = baselines.get(change) as RegularFileState | undefined;
								const sourceState = baselines.get(group.source) as RegularFileState | undefined;
								if (
									reference.before &&
									prior?.identity &&
									sourceState?.identity &&
									prior.identity.dev === sourceState.identity.dev &&
									prior.identity.ino === sourceState.identity.ino
								)
									continue;
								const source = reference.before ? reference.path : staged.get(group.source)!;
								const temporary = path.join(change.root, `${SANDBOX_STAGING_FILE_PREFIX}${randomUUID()}.tmp`);
								if (reference.before) nativeStarted = true;
								await link(source, temporary);
								staged.set(change, temporary);
								if (reference.before) {
									const linked = await view.stat(temporary);
									if (linked.dev !== sourceState!.identity!.dev || linked.ino !== sourceState!.identity!.ino)
										throw new Error("file object changed while linking");
								}
							}
						for (const group of objects.values())
							if (group.write || group.mode !== undefined) {
								nativeStarted = true;
								if (group.write) await writeContent(group.handle!, group.write.after!, group.source.before);
								if (group.mode !== undefined && process.platform !== "win32")
									await group.handle!.chmod(group.mode);
							}
						for (const change of orderWorkspaceChanges(
							changes.filter((change) => !change.validationOnly),
							(change) => ({
								change,
								depth: slash(change.resource).split("/").filter(Boolean).length,
								key: filesystemPathKey(change.target),
							}),
						)) {
							await assertCommitTarget(change, (target, read) => captureFilesystemEntry(target, read, view));
							if (change.kind === "directory") {
								if (!change.after) {
									await rmdir(change.target);
									applied.push(change);
								} else if (!change.before) {
									await createParentDirectories(
										change.root,
										change.target,
										view,
										createdDirectories,
										parents,
									);
									await mkdir(change.target, change.operation ? undefined : { mode: change.after.mode });
									applied.push(change);
									if (!change.operation && process.platform !== "win32")
										await chmod(change.target, change.after.mode);
								} else if (process.platform !== "win32" && change.before.mode !== change.after.mode) {
									await chmod(change.target, change.after.mode);
									applied.push(change);
								}
								continue;
							}
							if (change.object && !staged.has(change)) continue;
							if (change.operation && !change.object) {
								// Native writes are authoritative from their first file effect; created parents still roll back.
								await createParentDirectories(change.root, change.target, view, createdDirectories, parents);
								let descriptor = descriptors.get(change);
								if (descriptor) {
									applied.push(change);
								} else {
									// Exclusive creation already gives an empty file; only existing files need truncation.
									descriptor = await open(change.target, "wx", 0o666);
									descriptors.set(change, descriptor);
									applied.push(change);
								}
								await writeContent(descriptor, change.after!, change.before);
								continue;
							}
							applied.push(change);
							const temporary = staged.get(change);
							if (temporary) {
								await createParentDirectories(change.root, change.target, view, createdDirectories, parents);
								const mode = resolveCommitMode(baselines.get(change) as RegularFileState | undefined, change);
								try {
									await replaceFile(temporary, change.target, mode);
								} catch (error) {
									if (
										process.platform !== "win32" ||
										!(change.object || change.aliases) ||
										!hasErrorCode(error, "EPERM")
									)
										throw error;
									// NTFS can deny replacement of an open name while allowing its unlink. Retained handles stay on the old object.
									nativeStarted = true;
									await unlink(change.target);
									await replaceFile(temporary, change.target, mode);
								}
								staged.delete(change);
							} else {
								await rm(change.target, { force: true });
							}
						}
						await restoreModifiedTimes(
							changes.flatMap((change) =>
								change.kind !== "directory" && change.operation
									? [[change.target, change.afterModified] as const]
									: [],
							),
						);
						let directoryBytes = 0;
						const createdEntries = new Map(
							(inputs ? changes : [])
								.filter((change) => change.kind === "directory" && change.operation && change.after)
								.map((change) => [change.target, [] as string[]]),
						);
						if (inputs)
							for (const change of changes)
								if (!change.validationOnly && change.after)
									createdEntries.get(path.dirname(change.target))?.push(path.basename(change.target));
						// Directories are read at once; what their listings retain is decided in order.
						const listed = await mapFilesystem(
							changes.filter(
								(change): change is SandboxDirectoryChange =>
									!change.validationOnly && change.kind === "directory" && !!change.after,
							),
							async (change) => ({
								change,
								names: await verifySandboxDirectoryAfter(change, view),
							}),
						);
						for (const { change, names: observed } of listed) {
							if (!inputs) continue;
							const names = observed ?? createdEntries.get(change.target);
							const bytes = names ? serialize(names).byteLength : 0;
							const retain = directoryBytes + bytes <= speculativeConfig().filesystem.maxTreeBytes;
							if (retain) directoryBytes += bytes;
							inputs.set(change.target, { names: retain ? names : undefined });
						}
						try {
							await view.verify();
						} catch (error) {
							throw effectCommitFailure(error, nativeStarted || applied.length ? "poisoned" : "recoverable");
						}
					} catch (error) {
						inputs?.clear();
						if (
							nativeStarted ||
							isPoisonedEffectCommit(error) ||
							applied.some((change) => change.kind !== "directory" && (change.operation || change.aliases))
						) {
							throw effectCommitFailure(
								error,
								"poisoned",
								"native file write began; its effects cannot be safely replayed or rolled back",
							);
						}
						try {
							await restoreChanges(applied, baselines, view, contents);
							for (const directory of [...new Set(createdDirectories)].sort(
								(left, right) => right.length - left.length,
							)) {
								try {
									await rmdir(directory);
								} catch (error) {
									if (!isMissing(error)) throw error;
								}
							}
						} catch (rollbackError) {
							throw effectCommitFailure(
								new AggregateError([error, rollbackError], "sandbox commit and rollback both failed", {
									cause: error,
								}),
								"poisoned",
								"sandbox commit failed and the original workspace could not be fully restored",
							);
						}
						throw effectCommitFailure(error, "recoverable");
					} finally {
						const closed = await Promise.allSettled(
							[...descriptors.values()].map((descriptor) => descriptor.close()),
						);
						await Promise.all(
							[...staged.values()].map((temporary) => rm(temporary, { force: true }).catch(() => undefined)),
						);
						const failure = closed.find((result) => result.status === "rejected");
						if (failure)
							throw effectCommitFailure(
								failure.reason,
								"poisoned",
								"native file descriptor cleanup failed; completion is unknown",
							);
					}
					if (inputs)
						for (const change of changes)
							if (!change.validationOnly && (change.kind !== "directory" || !change.after))
								inputs.set(change.target, change.kind === "directory" ? null : (change.after ?? null));
					return execution.output;
				},
				view,
			);
		})()
			.catch((error) => {
				throw effectCommitFailure(error, "poisoned");
			})
			.finally(() => release?.());
		return this.lifetime.track(commit);
	}
	async fork(
		options: SandboxWorkspaceBranchOptions,
		preparation: SandboxPreparation | undefined = options.validate ? options.preparation : undefined,
	): Promise<WorldBranch<ToolSettlement>> {
		this.assertOpen();
		const sourceRoot = path.resolve(options.cwd);
		const parent = this.checkpoint(options.parentCheckpoint, sourceRoot);
		return this.createBranch(
			await this.capture(options, parent, preparation),
			sourceRoot,
			options.action,
			parent,
			options.validate,
		);
	}
	async capture(
		options: Omit<SandboxWorkspaceBranchOptions, "action" | "parentCheckpoint">,
		parent?: WorkspaceCheckpoint,
		preparation: SandboxPreparation | undefined = options.validate ? options.preparation : undefined,
	): Promise<WorkspaceExecutionSnapshot> {
		this.assertOpen();
		const sourceRoot = path.resolve(options.cwd);
		const ancestors: SandboxWorkspaceChange[] = [];
		for (let checkpoint = parent; checkpoint; checkpoint = checkpoint.parent)
			ancestors.push(...checkpoint.changes);
		const releaseParent = retainWorkspaceChanges(ancestors);
		let capturedSnapshot: WorkspaceExecutionSnapshot | undefined;
		try {
			const resolvedDriver = await this.resolveDriver(
				options.driver === "auto" || options.driver === undefined ? { ...options, driver: "git" } : options,
			);
			return await this.withPrivateWorkspace(
				sourceRoot,
				options.gitBinary ?? "git",
				resolvedDriver.driver,
				options,
				async (workspace) => {
					const before = parent ? await captureSandboxStructure(workspace, true) : workspace.initialStructure;
					const result = await options.execute(workspace);
					const captured =
						"output" in result
							? result
							: {
									output: result,
									changes: await captureWorkspaceChanges(workspace, before),
								};
					const changes = ownSandboxChanges(
						(await options.afterCapture?.(workspace, captured)) ?? captured.changes,
					);
					return (capturedSnapshot = {
						output: captured.output,
						changes,
						executionMetrics: { ...options.executionMetrics?.() },
						dispose: retainWorkspaceChanges([...changes, ...ancestors]),
					});
				},
				parent,
				preparation,
			);
		} catch (error) {
			await capturedSnapshot?.dispose?.();
			throw error;
		} finally {
			await releaseParent();
		}
	}
	async prepare(
		cwd: string,
		options: PrepareSandboxWorkspaceOptions = {},
	): Promise<QualifiedWorkspaceSandboxDriver> {
		this.assertOpen();
		options.signal?.throwIfAborted();
		const qualified = await this.resolveDriver(options, cwd);
		await (await this.tree(path.resolve(cwd))).capture.captureVersions();
		options.signal?.throwIfAborted();
		return Object.freeze(qualified);
	}
	private async executeMutation(
		context: SpeculativeToolExecutionContext,
		options: WorkspaceSandboxOptions,
	): Promise<WorldBranch<ToolSettlement>> {
		const execute = (context.action.executionContext as ToolInvocation | undefined)?.filesystem;
		if (!execute) throw new Error("Workspace execution requires an explicitly bound filesystem operation");
		const sourceRoot = path.resolve(context.cwd);
		// Root mutations keep writes in memory; hard links and chained actions need a private workspace.
		if (!context.parentCheckpoint && options.inPlaceMutations !== false)
			try {
				return this.createBranch(
					await this.executeFilesystemMutation(context, execute),
					sourceRoot,
					context.action,
				);
			} catch (error) {
				if (error !== LINKED_INPUT) throw error;
			}
		return this.fork(
			{
				cwd: sourceRoot,
				action: context.action,
				parentCheckpoint: context.parentCheckpoint,
				...options,
				execute: (workspace) => this.executeFilesystemMutation(context, execute, workspace),
			},
			capturedWorkspaceInputs,
		);
	}
	/** Both substrates capture the same operation delta; only a private workspace performs speculative writes. */
	private async executeFilesystemMutation(
		context: SpeculativeToolExecutionContext,
		execute: NonNullable<ToolInvocation["filesystem"]>,
		workspace?: SandboxWorkspaceContext,
	): Promise<WorkspaceExecutionSnapshot> {
		const contents = workspace?.contents ?? (await this.createContentPool());
		const release = contents?.retain();
		const readVersions: ResourceVersionToken[] = [];
		const store = (await this.storage)?.store;
		const session = store
			? new ResourceInputSession({ store }, speculativeConfig().runtime.resourceCacheMaxBytes)
			: undefined;
		let transferred = false;
		return withFilesystemReadView(undefined, async (source) => {
			const sourceRoot = path.resolve(context.cwd);
			await using privateReads = workspace ? FilesystemReadView.ambient() : undefined;
			const view = privateReads ?? source;
			const changes = new Map<string, SandboxWorkspaceChange>();
			const lifetime = new RuntimeLifecycleLane();
			let bytes = 0;
			let failure:
				| {
						error: unknown;
				  }
				| undefined;
			let namespace: Promise<WorkspaceStructureSnapshot> | undefined;
			// A request the tool swallowed still fails the run; requests after it returns are refused.
			const track = <T>(run: () => Promise<T>): Promise<T> =>
				lifetime.admit(async () => {
					try {
						context.signal.throwIfAborted();
						return await run();
					} catch (error) {
						failure ??= { error };
						throw error;
					}
				});
			const record = (key: string, change: SandboxWorkspaceChange) => {
				const retained = bytes - sandboxChangeBytes(changes.get(key)) + sandboxChangeBytes(change);
				if (
					retained > speculativeConfig().filesystem.maxTreeBytes ||
					(!changes.has(key) && changes.size >= speculativeConfig().filesystem.maxTreeEntries)
				)
					throw new Error("Workspace operation exceeds its capture budget");
				changes.set(key, change);
				bytes = retained;
			};
			const located = async (logical: string) => {
				const relative = relativeFilesystemPath(sourceRoot, logical);
				if (relative === undefined || isSnapshotExcluded(slash(relative)))
					throw new Error("Filesystem operation is outside the workspace view");
				const target = path.resolve(sourceRoot, relative);
				const file = path.resolve(workspace?.sandboxRoot ?? sourceRoot, relative);
				await assertNoSymlinkPath(sourceRoot, target, (target, read) =>
					captureFilesystemEntry(target, read, source),
				);
				if (workspace)
					await assertNoSymlinkPath(workspace.sandboxRoot, file, (target, read) =>
						captureFilesystemEntry(target, read, view),
					);
				context.signal.throwIfAborted();
				return {
					file,
					key: filesystemPathKey(target),
					root: sourceRoot,
					target,
					resource: slash(relative),
				};
			};
			const fileInput = async (logical: string) => {
				const { file, key, ...identity } = await located(logical);
				const previous = changes.get(key);
				if (previous?.kind === "directory") throw new Error("Workspace file input changed type");
				// Reuse the preimage until a private write requires reading the new physical state.
				const before =
					previous && (!workspace || previous.validationOnly)
						? previous.before === undefined
							? undefined
							: { content: previous.before, mode: previous.beforeMode! }
						: await readRegularState(file, undefined, view, contents);
				let aliases: readonly string[] | undefined;
				if (!previous && before && "identity" in before && before.identity && before.identity.nlink > 1n) {
					if (!workspace) throw LINKED_INPUT;
					const snapshot = await (namespace ??= workspace.structure.capture());
					const entry = snapshot.entries.get(path.relative(workspace.sandboxRoot, file));
					if (
						!snapshot.complete ||
						entry?.kind !== "file" ||
						entry.aliases?.length !== Number(before.identity.nlink)
					)
						throw new Error("workspace file alias namespace is not closed");
					aliases = entry.aliases.map((name) =>
						path.resolve(sourceRoot, path.relative(workspace.sandboxRoot, name)),
					);
				}
				const captured: SandboxFileChange = previous ?? {
					...identity,
					validationOnly: true,
					before: before?.content,
					beforeMode: before?.mode,
					...(aliases ? { aliases } : {}),
				};
				record(key, captured);
				return {
					file,
					key,
					captured,
					content: !workspace && !captured.validationOnly ? captured.after : before?.content,
				};
			};
			const directoryInput = async (logical: string) => {
				const { file, key, ...identity } = await located(logical);
				const previous = changes.get(key);
				if (previous && previous.kind !== "directory")
					throw new Error("Workspace directory input changed type");
				const before =
					previous && !workspace
						? (previous.before ?? previous.after)
						: await readSandboxDirectoryState(file, undefined, view);
				// An in-memory mkdir needs only existence; a private mkdir captures its actual final directory state below.
				if (!previous)
					record(key, {
						...identity,
						kind: "directory",
						before,
						...(before
							? { validationOnly: true }
							: {
									operation: "mkdir",
									...(!workspace
										? {
												after: {
													entriesDigest: directoryEntriesDigest([]),
													mode: 0o755,
													uid: 0,
													gid: 0,
												},
											}
										: {}),
								}),
					});
				return { file, key, before };
			};
			const input = async (logical: string) => {
				const { file, key } = await located(logical);
				const known = changes.get(key);
				const directory =
					!workspace && known
						? known.kind === "directory"
						: (
								await view.stat(file).catch((error) => {
									if (!isMissing(error)) throw error;
								})
							)?.isDirectory();
				const exists = directory
					? (await directoryInput(logical)).before !== undefined
					: (await fileInput(logical)).content !== undefined;
				return { file, key, exists };
			};
			let output: ToolSettlement;
			const operations: ToolFilesystemOperations = {
				readFile: (logical, limit) =>
					track(async () => {
						const { content } = await fileInput(logical);
						if (content === undefined) throw new Error("Workspace input does not exist");
						return readContent(content, 0, Math.min(content.byteLength, limit ?? content.byteLength));
					}),
				access: (logical, writable) =>
					track(async () => {
						const { file, key } = await input(logical);
						const mode = fsConstants.R_OK | (writable ? fsConstants.W_OK : 0);
						const captured = changes.get(key)!;
						assertExistingInputPolicy(captured);
						await access(await view.realpath(file), mode);
						if (captured.before !== undefined)
							record(key, {
								...captured,
								accessMode: (captured.accessMode ?? 0) | mode,
							});
					}),
				exists: (logical) => track(async () => (await input(logical)).exists),
				writeFile: (logical, content) =>
					track(async () => {
						const { file, key, captured } = await fileInput(logical);
						// Even an equal-content native write performs permission checks and touches the inode.
						record(key, {
							...captured,
							accessMode: changes.get(key)?.accessMode,
							validationOnly: undefined,
							after: contents ? await captureContent(contents, content) : Buffer.from(content),
							operation: "write_contents",
						});
						if (workspace) await writeFile(file, content, "utf8");
					}),
				mkdir: (logical) =>
					track(async () => {
						for (let directory = path.resolve(logical); ; directory = path.dirname(directory)) {
							if ((await directoryInput(directory)).before || directory === sourceRoot) break;
						}
						if (workspace) await mkdir((await located(logical)).file, { recursive: true });
					}),
				stat: (logical, fields, includeSize = true) =>
					track(async () => {
						const { file, key, exists } = await input(logical);
						if (!exists)
							throw Object.assign(new Error("Workspace input does not exist"), {
								code: "ENOENT",
							});
						const known = changes.get(key)!;
						if (!workspace && known.kind !== "directory" && !known.validationOnly)
							return {
								isDirectory: () => false,
								type: "file",
								size: includeSize ? known.after?.byteLength : undefined,
							};
						const info = await view.stat(file, fields !== "entry");
						let type: "file" | "directory" | "symlink" | "special";
						if (info.isDirectory()) {
							type = "directory";
						} else {
							if (info.isFile()) {
								type = "file";
							} else {
								if (info.isSymbolicLink()) {
									type = "symlink";
								} else {
									type = "special";
								}
							}
						}
						return {
							isDirectory: () => info.isDirectory(),
							type,
							size: includeSize ? Number(info.size) : undefined,
							...(type === "symlink" ? { link: await view.readlink(file) } : { realPath: logical }),
						};
					}),
				readdir: (logical) =>
					track(async () => {
						const { file } = await directoryInput(logical);
						const names = new Set(
							await readFilesystemDirectory(file, view)
								.then((entries) => entries.map((entry) => entry.name))
								.catch((error) => {
									if (isMissing(error) && !workspace) return [];
									throw error;
								}),
						);
						if (!workspace)
							for (const change of changes.values())
								if (!change.validationOnly && path.dirname(change.target) === path.resolve(logical))
									names.add(path.basename(change.target));
						return [...names].sort();
					}),
				prepare: async (binding, key, build, consume, hint, signal = context.signal) => {
					let baseline = true;
					const written = (logical: string, scope: string) => {
						if (scope === "binding" || scope === "resolution") return false;
						const target = path.resolve(sourceRoot, logical);
						const writes = [...changes.values()];
						for (
							let parent = this.checkpoint(context.parentCheckpoint, sourceRoot);
							parent;
							parent = parent.parent
						)
							if (!parent.committed) writes.push(...parent.changes);
						return writes.some(
							(change) =>
								!change.validationOnly &&
								(filesystemPathKey(change.target) === filesystemPathKey(target) ||
									(change.kind === "directory" && containsFilesystemPath(change.target, target)) ||
									(change.kind !== "directory" &&
										change.aliases?.some(
											(alias) => filesystemPathKey(alias) === filesystemPathKey(target),
										)) ||
									(["names", "entries", "tree_entries", "tree_content"].includes(scope) &&
										containsFilesystemPath(target, change.target))),
						);
					};
					const transactionView = (captured: ToolFilesystemOperations): ToolFilesystemOperations => {
						const select = (logical: string, scope: string) => {
							if (!written(logical, scope)) return captured;
							baseline = false;
							return operations;
						};
						const overlay: ToolFilesystemOperations = {
							readFile: (target, limit) => select(target, "content").readFile(target, limit),
							access: (target, writable) => select(target, "entry").access(target, writable),
							exists: (target) => select(target, "exists").exists!(target),
							stat: (target, fields, size) => select(target, fields ?? "stat").stat!(target, fields, size),
							readdir: (target) => select(target, "names").readdir!(target),
							prepare: (binding, key, build, use, hint, signal) =>
								captured.prepare!(
									binding,
									key,
									async (view, signal) => {
										const result = await build(transactionView(view), signal);
										return { ...result, cacheable: baseline && result.cacheable !== false };
									},
									use,
									hint,
									signal,
								),
							computeStep: (request, calculate, signal) =>
								computeStepInView(overlay, request, calculate, signal),
						};
						return overlay;
					};
					const run = () =>
						evaluateResourceQuery(
							undefined,
							{ ...context, signal },
							sourceRoot,
							(captured) =>
								transactionView(captured).prepare!(binding, key, build, async (value) => value, hint, signal),
							PI_ACTION_SEMANTICS,
							speculativeConfig().runtime.resourceCacheMaxBytes,
							true,
							"output",
							(versions) => {
								readVersions.push(...versions);
								return true;
							},
							session,
							(observation) => !written(observation.path, observation.scope),
						);
					const query = await (session ? session.run(run) : run());
					if (!query) throw new Error("workspace_preparation_budget_exceeded");
					return consume(query.output);
				},
				computeStep: (request, calculate, signal) =>
					computeStepInView(operations, request, calculate, signal ?? context.signal),
			};
			try {
				output = await execute(operations, context);
			} finally {
				await lifetime.close(() => {});
				if (failure) throw failure.error;
				context.signal.throwIfAborted();
			}
			if (workspace)
				for (const [key, change] of changes) {
					if (change.kind === "directory" && !change.validationOnly)
						record(key, {
							...change,
							after: await readSandboxDirectoryState((await located(change.target)).file, undefined, view),
						});
					else if (
						change.kind !== "directory" &&
						!change.validationOnly &&
						!sameContent(
							(await readRegularState((await located(change.target)).file, undefined, view, contents))
								?.content,
							change.after,
						)
					) {
						throw new Error("Workspace writes did not settle to their declared bytes");
					}
				}
			const captured = [...changes.values()];
			const readInputs = readVersions.some((token) => token.view?.hasPreparedInputs)
				? resourceSnapshotBranch(
						{ inputsOnly: true as const },
						readVersions,
						context.action,
						PI_ACTION_SEMANTICS,
						undefined,
						session?.configuration,
					)
				: undefined;
			transferred = !!readInputs;
			return {
				output,
				changes: workspace ? captured : ownSandboxChanges(captured),
				readInputs,
				executionMetrics: {},
				dispose: workspace ? undefined : retainWorkspaceChanges(captured),
			};
		}).finally(async () => {
			if (!transferred) await Promise.allSettled(readVersions.map((token) => token.release()));
			await session?.dispose();
			await release?.();
		});
	}
	private async createPrivateSandboxWorkspace(
		cwd: string,
		_gitBinary: string,
		driver: Exclude<WorkspaceSandboxDriver, "auto">,
		options: WorkspaceSandboxOptions,
		_preparation?: SandboxPreparation,
	) {
		const sourceRoot = path.resolve(cwd);
		const tree = await this.tree(sourceRoot);
		const releaseContents = tree.contents?.retain();
		const versions = new ResourceVersionManager(sourceRoot);
		const cursor = await versions.observeChanges();
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-workspace-"));
		const processRoot = path.join(root, "process");
		const lowerRoot = path.join(root, "tree");
		const pins: Awaited<ReturnType<typeof pinFilesystemObject>>[] = [];
		const pinObject = async (target: string) => {
			const handle = await pinFilesystemObject(target);
			pins.push(handle);
			return handle.stat;
		};
		let overlay: LinuxOverlayfsMount | undefined;
		let workspace: WorkspaceObservation | undefined;
		let disposal: Promise<void> | undefined;
		const dispose = () =>
			(disposal ??= (async () => {
				try {
					await workspace?.transactions.dispose();
					await overlay?.close();
					await removeOwnedTree(root);
				} finally {
					await Promise.all(pins.map((handle) => handle.close()));
					await cursor.release();
					versions.close();
					await releaseContents?.();
				}
			})());
		try {
			const captured = await tree.capture.captureVersions();
			if (!captured.snapshot.complete || [...captured.files.values()].some((file) => !file))
				throw new Error("workspace snapshot exceeds configured limits");
			const original = new Map([...captured.entries].map(([name, entry]) => [name, entry.captured.info]));
			await mkdir(processRoot);
			const live = driver === "overlayfs" && options.liveLower;
			if (!live) await materializeWorkspaceTree(captured, lowerRoot);
			let sandboxRoot = lowerRoot;
			if (driver === "overlayfs") {
				overlay = await mountLinuxOverlayfs({
					lowerRoot: live ? sourceRoot : lowerRoot,
					privateRoot: root,
					options,
					capabilityRegistry: this.overlayfsCapabilities,
				});
				sandboxRoot = overlay.root;
			}
			const metadata = await preserveSnapshotMetadata(
				sourceRoot,
				live ? overlay!.upperRoot : sandboxRoot,
				processRoot,
				process.platform === "linux" && !!options.liveLower,
				original,
				pinObject,
			);
			const clock = overlay
				? {
						path: overlay.upperRoot,
						links: 0 as const,
						roots: [overlay.upperRoot, overlay.workRoot],
					}
				: {
						path: path.join(processRoot, "workspace-transaction.clock"),
						links: 1 as const,
						roots: [sandboxRoot, processRoot],
					};
			const storage = { sandboxRoot, clock };
			const sourceChanges = () => versions.changesSince(cursor);
			const initialStructure = await captureSandboxStructure(storage, true);
			const data: SandboxWorkspaceObservation = {
				...storage,
				id: randomUUID(),
				sourceRoot,
				processRoot,
				baseline: captured,
				baselineFrontier: new Map(),
				metadata,
				initialStructure,
				...(overlay
					? {
							overlay: {
								baseline: initialStructure,
								device: String((await lstat(sandboxRoot, { bigint: true })).dev),
								upperRoot: overlay.upperRoot,
							},
						}
					: {}),
				sourceDevice: original.get("")!.dev,
				hints: sourceChanges(),
			};
			workspace = createWorkspaceObservation(data, sourceChanges, tree.contents);
			return { ...workspace, dispose, pinObject };
		} catch (error) {
			await dispose();
			throw error;
		}
	}
	private tree(sourceRoot: string) {
		let tree = this.trees.get(sourceRoot);
		if (!tree) {
			tree = this.createContentPool().then((contents) => {
				const release = contents?.retain();
				const capture = new WorkspaceStructureCapture(sourceRoot, {
					maxFiles: speculativeConfig().filesystem.maxTreeEntries,
					exclude: speculativeConfig().filesystem.excludes,
					content: {
						pool: contents,
						maxBytes: speculativeConfig().filesystem.maxTreeBytes,
						maxFileBytes: speculativeConfig().filesystem.maxTreeBytes,
					},
				});
				return { capture, contents, release };
			});
			this.trees.set(sourceRoot, tree);
			void tree.catch(() => this.trees.delete(sourceRoot));
		}
		return tree;
	}
	private assertOpen(): void {
		if (this.lifetime.sealed) throw new Error("Workspace sandbox service is disposed");
	}
	private async withPrivateWorkspace<T>(
		cwd: string,
		gitBinary: string,
		driver: Exclude<WorkspaceSandboxDriver, "auto">,
		overlayOptions: WorkspaceSandboxOptions,
		run: (workspace: PrivateSandboxWorkspace) => Promise<T>,
		checkpoint?: WorkspaceCheckpoint,
		preparation?: SandboxPreparation,
	): Promise<T> {
		const workspace = await this.createPrivateSandboxWorkspace(
			cwd,
			gitBinary,
			driver,
			overlayOptions,
			preparation,
		);
		try {
			if (checkpoint) await this.materializeCheckpoint(workspace, checkpoint);
			return await run(workspace);
		} finally {
			await workspace.dispose();
		}
	}
	private async closePools(roots?: readonly string[]): Promise<void> {
		for (const [root, pending] of this.trees) {
			if (roots && !roots.includes(root)) continue;
			this.trees.delete(root);
			const tree = await pending;
			tree.capture.clear();
			await tree.release?.();
		}
	}
	private async materializeCheckpoint(
		workspace: PrivateSandboxWorkspace,
		checkpoint: WorkspaceCheckpoint,
	): Promise<void> {
		await using view = FilesystemReadView.ambient();
		const lineage: WorkspaceCheckpoint[] = [];
		for (let current: WorkspaceCheckpoint | undefined = checkpoint; current; current = current.parent) {
			lineage.push(current);
		}
		for (const ancestor of lineage.reverse()) {
			const changes = ownSandboxChanges(ancestor.changes, (name) => {
				const relative = relativeFilesystemPath(ancestor.sourceRoot, name);
				if (relative === undefined) throw new Error("checkpoint object escapes workspace");
				return path.resolve(workspace.sandboxRoot, relative);
			});
			// Allocation may precede commit: prove the resulting bytes, namespace and times before skipping an adopted root.
			if (
				ancestor.committed &&
				(
					await mapFilesystem(
						changes.filter((change) => !change.validationOnly),
						async (change) => {
							await assertCommitTarget(change);
							if (change.kind === "directory") {
								await verifySandboxDirectoryAfter(change);
								return true;
							}
							const current = await readRegularState(change.target, undefined, view, workspace.contents);
							if (change.after !== undefined)
								await assertFileAliases(
									change.root,
									change.target,
									change.afterAliases ?? (change.operation ? change.aliases : undefined) ?? [change.target],
									current?.identity,
								);
							return (
								sameSandboxState(
									current,
									change.after === undefined
										? undefined
										: {
												content: change.after,
												mode: change.afterMode ?? change.beforeMode ?? 0,
											},
								) &&
								(process.platform !== "linux" ||
									change.afterModified === undefined ||
									String(current?.identity?.mtimeNs) === change.afterModified)
							);
						},
					).catch(() => [false])
				).every(Boolean)
			)
				continue;
			await this.commit(
				{
					output: { result: { content: [], details: {} }, isError: false },
					changes,
				},
				undefined,
				view,
			);
			for (const change of changes)
				if (!change.validationOnly && change.kind !== "directory") {
					if (change.after !== undefined) await workspace.pinObject(change.target);
					workspace.baselineFrontier.set(
						path.normalize(change.resource),
						await readRegularState(change.target, undefined, view, workspace.contents),
					);
				}
		}
	}
}
function retainWorkspaceChanges(changes: readonly SandboxWorkspaceChange[]): () => Promise<void> {
	const pools = new Set(
		changes
			.flatMap((change) => (change.kind === "directory" ? [] : [change.before, change.after]))
			.filter((value): value is ChunkedContent => value instanceof ChunkedContent)
			.map((value) => value.pool),
	);
	const releases = [...pools].map((pool) => pool.retain());
	return async () => {
		await Promise.all(releases.map((release) => release()));
	};
}
async function captureWorkspaceChanges(
	workspace: WorkspaceObservation,
	before: WorkspaceStructureSnapshot,
): Promise<readonly SandboxWorkspaceChange[]> {
	const after = await workspace.structure.capture();
	const transitions = regularStructureTransitions(before, after);
	const deltas = (await collectSandboxChanges(workspace, transitions)).map(
		({ resource: relativePath, ...change }) => ({ ...change, relativePath }),
	);
	const effects = diffWorkspaceStructures(
		before,
		after,
		deltas,
		new ExecutionPathProjection({
			sourceRoot: workspace.sourceRoot,
			workspaceRoot: workspace.sandboxRoot,
		}),
	);
	if (!sameWorkspaceChangeSnapshot(after, await workspace.structure.capture()))
		throw new Error("workspace changed while capturing effects");
	return sandboxWorkspaceChanges(workspace.sourceRoot, effects);
}
/** Both raw operations and native traces commit the same proven object transitions. */
export function sandboxWorkspaceChanges(
	root: string,
	effects: WorkspaceTransactionDiff,
): SandboxWorkspaceChange[] {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	const directory = ({ entriesDigest, mode, uid, gid }: SandboxDirectoryState) => ({
		entriesDigest,
		mode,
		uid,
		gid,
	});
	return effects.effects.map(({ logicalPath: target, relativePath, change }): SandboxWorkspaceChange => {
		const address = { root, target, resource: slash(relativePath) };
		return change.kind !== "directory"
			? { ...change, ...address }
			: {
					kind: "directory",
					...address,
					before: change.before && directory(change.before),
					after: change.after && directory(change.after),
				};
	});
}
const LINKED_INPUT = new Error("linked workspace input");
/** Git owns bytes and links, while an OS snapshot also owns empty directories and object metadata.
 * Pin every translated object until disposal: unlink/recreate must never reuse a saved inode mapping. */
async function captureSnapshotMetadata(sourceRoot: string, copiedRoot = sourceRoot) {
	return withFilesystemReadView(undefined, async (view) => {
		const original = new Map<string, BigIntStats>();
		if (
			!(
				await captureWorkspaceStructure(copiedRoot, {
					maxFiles: speculativeConfig().filesystem.maxTreeEntries,
					exclude: speculativeConfig().filesystem.excludes,
					view,
					observeStat: async (resource, stat) => {
						original.set(
							resource,
							copiedRoot === sourceRoot ? stat : await view.stat(path.join(sourceRoot, resource)),
						);
					},
				})
			).complete
		)
			throw new Error("snapshot metadata namespace is incomplete");
		return original;
	});
}
async function preserveSnapshotMetadata(
	sourceRoot: string,
	sandboxRoot: string,
	processRoot: string,
	native: boolean,
	captured?: ReadonlyMap<string, BigIntStats>,
	pin?: (target: string) => Promise<BigIntStats>,
) {
	const original = captured ?? (await captureSnapshotMetadata(sourceRoot));
	const names = [...original.keys()].sort((a, b) => a.length - b.length);
	const mounts = native ? await readFile("/proc/self/mountinfo", "utf8") : undefined;
	const policies = mounts
		? mounts
				.trim()
				.split("\n")
				.map((line) => {
					const fields = line.split(" ");
					return {
						root: fields[4]!.replace(/\\([0-7]{3})/g, (_, octal: string) =>
							String.fromCharCode(Number.parseInt(octal, 8)),
						),
						flags: fields[5]!.split(","),
					};
				})
				.sort((a, b) => b.root.length - a.root.length)
		: [];
	const atimePolicy = (target: string, directory: boolean) => {
		const flags = policies.find((mount) => containsFilesystemPath(mount.root, target))?.flags ?? [];
		return flags.includes("noatime") || flags.includes("ro") || (directory && flags.includes("nodiratime"))
			? 1
			: flags.includes("relatime")
				? 2
				: 0;
	};
	// Check each parent before populating its children: a stale checkout may contain a symlink in its place.
	for (const resource of names)
		if (original.get(resource)!.isDirectory()) {
			const target = path.join(sandboxRoot, resource);
			const current = await lstat(target).catch((error) => {
				if (isMissing(error)) return undefined;
				throw error;
			});
			if (!current) await mkdir(target);
			else if (!current.isDirectory()) throw new Error(`snapshot directory type changed: ${resource}`);
		}
	const translated: [string, BigIntStats][] = [];
	const rows: [BigIntStats, BigIntStats, number][] = [];
	const objects = new Map<string, string>();
	// Set parent modes last so a read-only directory cannot prevent its own snapshot from being populated.
	for (const resource of names.reverse()) {
		const source = original.get(resource)!;
		const target = path.join(sandboxRoot, resource);
		const current = await lstat(target, { bigint: true }).catch((error) => {
			if (isMissing(error)) return undefined;
			throw error;
		});
		if (!current) continue; // A warmup can predate this name; exact input validation still decides adoption.
		if (objects.has(`${current.dev}:${current.ino}`)) continue;
		if ((source.mode & 61440n) !== (current.mode & 61440n))
			throw new Error(`snapshot object type changed: ${resource}`);
		if (source.uid !== current.uid || source.gid !== current.gid)
			throw new Error(`snapshot object ownership differs: ${resource}`);
		if (!source.isSymbolicLink()) await chmod(target, Number(source.mode & 4095n));
		if (process.platform === "linux" || !source.isSymbolicLink())
			await (source.isSymbolicLink() ? lutimes : utimes)(target, source.atime, source.mtime);
		objects.set(`${current.dev}:${current.ino}`, `${source.dev}:${source.ino}`);
		translated.push([target, source]);
	}
	await restoreModifiedTimes(translated.map(([target, source]) => [target, String(source.mtimeNs)]));
	for (const [target, source] of translated) {
		if (!native && (!pin || !source.isFile())) continue;
		const physical = await pin!(target);
		if (native)
			rows.push([
				physical,
				source,
				atimePolicy(path.join(sourceRoot, path.relative(sandboxRoot, target)), source.isDirectory()),
			]);
	}
	const image = native ? path.join(processRoot, "metadata.json") : undefined;
	if (image) {
		rows.push([
			await pin!(path.dirname(sandboxRoot)),
			await withFilesystemReadView(undefined, (view) => view.stat(path.dirname(sourceRoot))),
			atimePolicy(path.dirname(sourceRoot), true),
		]);
		await writeFile(
			image,
			JSON.stringify(
				rows.map(([physical, source, policy]) => [
					...[physical, source].map((stat) => SNAPSHOT_METADATA_FIELDS.map((field) => String(stat[field]))),
					policy,
				]),
			),
			{ flag: "wx", mode: 0o600 },
		);
	}
	const mappings = new Map(rows.map((row) => [`${row[0].dev}:${row[0].ino}`, row]));
	return {
		image,
		mounts,
		objects,
		mappings,
		modes: new Map(
			[...original].map(([name, stat]) => [
				slash(name),
				process.platform === "win32" ? 0 : Number(stat.mode & 511n),
			]),
		),
	};
}
async function createWorkspaceTransactionDriver(
	workspace: ReturnType<typeof createWorkspaceObservation>,
): Promise<WorkspaceTransactionDriver> {
	// Each interval borrows the domain's original versions. Its writer later attributes overlapping changes.
	interface Capture {
		contaminated: boolean;
		overlapped: boolean;
		readonly before?: WorkspaceStructureSnapshot;
		readonly versions?: WorkspaceContentSnapshot;
		readonly racing: readonly string[];
	}
	const { sandboxRoot } = workspace;
	// Discard the entire unaccepted snapshot if a name disappears or an observed object changes during capture.
	const captureStructure = async (attempt = 1): Promise<WorkspaceStructureSnapshot> => {
		const last = attempt >= speculativeConfig().filesystem.stabilityAttempts;
		const snapshot = await workspace.structure.capture().catch((error: unknown) => {
			if (last || !(isMissing(error) || error instanceof FilesystemCaptureChangedError)) throw error;
		});
		return snapshot && (snapshot.complete || last) ? snapshot : captureStructure(attempt + 1);
	};
	let versions: WorkspaceContentSnapshot = {
		snapshot: workspace.initialStructure,
		files: new Map(),
	};
	let fenced = false;
	const active = new Set<Capture>();
	const lock = new RuntimeLifecycleLane();
	let poisonReason = versions.snapshot.complete ? undefined : "workspace_structure_limit";
	let clock: Awaited<ReturnType<typeof openWorkspaceChangeClock>> | undefined;
	const retain = (snapshot: WorkspaceStructureSnapshot, capture = true) =>
		captureWorkspaceContent(
			snapshot,
			versions,
			async (resource, entry) =>
				capture
					? captureFileContent(
							path.resolve(sandboxRoot, resource),
							Math.min(
								entry.size,
								workspace.contents?.limits.maxLogicalBytes ?? speculativeConfig().filesystem.maxTreeBytes,
							),
							workspace.contents,
							{ digest: false },
						)
					: undefined,
			{
				maxBytes: speculativeConfig().filesystem.maxTreeBytes,
				memory: workspace.contents ? true : undefined,
				required: true,
				baseline: workspace.initialStructure,
				baselineMemory: (resource) => contentMemory(workspace.baselineFrontier.get(resource)?.content),
			},
		);
	const readVersion = async (
		original: WorkspaceContentSnapshot,
		resource: string,
		current = false,
	): Promise<RegularFileState | undefined> => {
		if (original.snapshot.entries.get(resource)?.kind !== "file") return;
		if (!original.files.has(resource))
			return workspace.baselineFrontier.has(resource)
				? workspace.baselineFrontier.get(resource)
				: workspace.readBase(resource);
		const file = original.files.get(resource);
		if (file)
			return {
				content: file.content,
				identity: file.stat,
				mode: process.platform === "win32" ? 0 : Number(file.stat.mode & 511n),
			};
		if (current)
			return readRegularState(path.resolve(sandboxRoot, resource), undefined, undefined, workspace.contents);
		throw new Error("workspace transaction input is unavailable");
	};
	if (!poisonReason) {
		try {
			clock = await openWorkspaceChangeClock(workspace);
		} catch (error) {
			poisonReason = `workspace_transaction_clock:${errorMessage(error)}`;
		}
	}
	const begin = (): Promise<WorkspaceTransactionCapture> =>
		lock.serialize(async () => {
			const overlapped = active.size > 0;
			for (const other of active) other.overlapped = true;
			const start = poisonReason
				? undefined
				: await captureBefore(overlapped).catch((error) => {
						poisonReason = `workspace_transaction_sync:${errorMessage(error)}`;
						return undefined;
					});
			const capture: Capture = {
				contaminated: false,
				overlapped,
				before: start && { ...start.versions.snapshot },
				versions: start?.versions,
				racing: start?.racing ?? [],
			};
			const input = async (resource: string, maxBytes = Infinity) => {
				if (capture.contaminated || !capture.before)
					throw new Error("workspace transaction input is unavailable");
				const entry = capture.before.entries.get(resource);
				if (entry?.kind === "file" && entry.size > maxBytes)
					throw new Error("workspace transaction input exceeds capture limit");
				return (await readVersion(capture.versions!, resource))?.content;
			};
			if (capture.before) workspaceVersions.set(capture.before, input);
			active.add(capture);
			return {
				readBefore: (resource, maxBytes) =>
					lock.serialize(async () => {
						if (!active.has(capture)) throw new Error("workspace transaction input is unavailable");
						if (!isSafeCount(maxBytes)) throw new Error("workspace transaction input budget is invalid");
						const content = await input(resource, maxBytes);
						if (content && content.byteLength > maxBytes)
							throw new Error("workspace transaction input exceeds capture limit");
						return content && readContent(content, 0, content.byteLength);
					}),
				finish: (ownership) => finish(capture, ownership),
				abort: (unchanged) => abort(capture, unchanged),
			};
		});
	/**
	 * Attribute an overlapped interval by its writer's trace: its changes are the paths it wrote, other changes are foreign and
	 * must not reach what it observed, and nothing may touch its own paths after it ended or while it ran.
	 */
	async function finish(
		capture: Capture,
		ownership?: () => Promise<WorkspaceTransactionOwnership | undefined>,
	): Promise<WorkspaceTransactionDelta> {
		const settled = await lock.serialize(async () => {
			if (!active.delete(capture))
				return {
					complete: false,
					changes: [],
					reason: "transaction_already_settled",
				};
			if (capture.contaminated || (capture.overlapped && !ownership))
				return {
					complete: false,
					changes: [],
					reason: "overlapping_workspace_transaction",
				};
			if (!capture.before)
				return {
					complete: false,
					changes: [],
					reason: poisonReason ?? "workspace_transaction_unavailable",
				};
			try {
				const { versions } = await captureBefore(capture.overlapped);
				const after = versions.snapshot;
				return {
					after,
					versions,
					capturedAt: Date.now(),
					paths: regularStructureTransitions(capture.before, after),
				};
			} catch (error) {
				return {
					complete: false,
					changes: [],
					reason: `workspace_transaction_capture:${errorMessage(error)}`,
					before: capture.before,
				};
			}
		});
		if (settled.reason !== undefined)
			return {
				complete: false,
				changes: [],
				reason: settled.reason,
				before: settled.before,
			};
		const { after, capturedAt, paths } = settled;
		const before = capture.before!;
		const owned: WorkspaceTransactionOwnership | undefined = capture.overlapped
			? await ownership!().catch(() => undefined)
			: {
					written: new Set(paths),
					observed: new Map(),
					endedAt: capturedAt,
					interfered: async () => false,
				};
		const incomplete = (reason: string) => ({
			complete: false,
			changes: [],
			reason,
			before,
			after,
		});
		if (!owned) return incomplete("overlapping_workspace_transaction");
		// A writer owns the objects it named at either endpoint, including every alias and moved descendant.
		const named = (resource: string) =>
			workspaceAncestors(resource).some((name) => owned.written.has(name) || owned.written.has(slash(name)));
		const objects = new Set<string>();
		for (const snapshot of [before, after])
			for (const [name, entry] of snapshot.entries)
				if (entry.kind === "file" && entry.object && named(name)) objects.add(entry.object);
		const mine = (resource: string) =>
			named(resource) ||
			[before, after].some((snapshot) => {
				const entry = snapshot.entries.get(resource);
				return entry?.kind === "file" && !!entry.object && objects.has(entry.object);
			});
		const own = paths.filter(mine);
		const ownNames = new Set(own);
		const racing = new Set(capture.racing);
		// A foreign change reaches what it saw only if it came no later than its last look at the name or at the directory holding it.
		const seen = (resource: string) =>
			Math.max(
				owned.observed.get(resource) ?? -Infinity,
				owned.observed.get(path.dirname(resource).replace(/^\.$/, "")) ?? -Infinity,
			);
		for (const foreign of new Set([...racing, ...paths.filter((resource) => !ownNames.has(resource))])) {
			const changed = racing.has(foreign) ? undefined : after.entries.get(foreign)?.changeTimeMs;
			if (seen(foreign) > -Infinity && (changed === undefined || Math.floor(changed) <= seen(foreign)))
				return incomplete(`overlapping_workspace_input:${foreign}`);
		}
		const late = own.find(
			(resource) => Math.floor(after.entries.get(resource)?.changeTimeMs ?? -Infinity) > owned.endedAt,
		);
		if (late !== undefined) return incomplete(`overlapping_workspace_write:${late}`);
		const targets = new Set([...owned.written, ...own].flatMap(workspaceAncestors));
		if (targets.size && (await owned.interfered(targets, capturedAt)))
			return incomplete("overlapping_workspace_write");
		let changes: WorkspaceRegularDelta[];
		const entries = new Map(before.entries);
		try {
			let beforeBytes = 0;
			let afterBytes = 0;
			// Its own paths' endpoints are independent reads; they are judged in order, the first that fails deciding.
			const captured = await mapFilesystem(
				own,
				async (relativePath): Promise<string | WorkspaceRegularDelta> => {
					if (
						capture.versions!.files.has(relativePath) &&
						!capture.versions!.files.get(relativePath) &&
						before.entries.get(relativePath)?.kind === "file"
					)
						return `overlapping_workspace_before:${relativePath}`;
					const previous = await readVersion(capture.versions!, relativePath);
					const entry = after.entries.get(relativePath);
					const current = await readVersion(settled.versions, relativePath, true);
					beforeBytes += contentMemory(previous?.content);
					afterBytes += contentMemory(current?.content);
					if (Math.max(beforeBytes, afterBytes) > speculativeConfig().filesystem.maxTreeBytes)
						return "workspace transaction exceeds memory budget";
					if (
						entry?.kind === "file" &&
						(!current?.identity || statChangeDigest(current.identity) !== entry.changeDigest)
					)
						return `overlapping_workspace_write:${relativePath}`;
					return {
						relativePath,
						...(previous ? { before: previous.content, beforeMode: previous.mode } : {}),
						...(current ? { after: current.content, afterMode: current.mode } : {}),
					};
				},
			);
			const failure = captured.find((change): change is string => typeof change === "string");
			if (failure !== undefined) return incomplete(failure);
			changes = captured as WorkspaceRegularDelta[];
		} catch (error) {
			return incomplete(`workspace_transaction_capture:${errorMessage(error)}`);
		}
		// Both object names and their owned directories advance together; everything else stays as it began.
		for (const name of new Set([...before.entries.keys(), ...after.entries.keys()])) {
			const entry = after.entries.get(name);
			const previous = before.entries.get(name);
			if (!ownNames.has(name) && ((entry ?? previous)?.kind !== "directory" || !mine(name))) continue;
			if (entry) entries.set(name, entry);
			else entries.delete(name);
		}
		return {
			complete: true,
			changes,
			before,
			after: capture.overlapped ? workspaceStructureSnapshot(after.root, entries, after.complete) : after,
		};
	}
	async function captureBefore(overlapped = false): Promise<{
		readonly versions: WorkspaceContentSnapshot;
		readonly racing: readonly string[];
	}> {
		try {
			for (let attempt = 0; attempt < speculativeConfig().filesystem.stabilityAttempts; attempt++) {
				const current = await captureStructure();
				if (fenced && sameWorkspaceChangeSnapshot(current, versions.snapshot))
					return { versions, racing: [] };
				await advanceWorkspaceChangeClock(clock, current);
				const checkpoint = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(current, checkpoint)) continue;
				regularStructureTransitions(versions.snapshot, checkpoint);
				fenced = false;
				versions = await retain(checkpoint);
				const verified = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(checkpoint, verified)) continue;
				fenced = true;
				return { versions, racing: [] };
			}
			throw new Error("workspace did not stabilize before transaction execution");
		} catch (error) {
			if (!overlapped) throw error;
			// A still-moving start keeps its racing names unread until their writers can prove ownership.
			const current = await captureStructure();
			await advanceWorkspaceChangeClock(clock, current);
			const before = await captureStructure();
			const racing = regularStructureTransitions(current, before);
			regularStructureTransitions(versions.snapshot, before);
			fenced = false;
			versions = await retain(before, false);
			return { versions, racing };
		}
	}
	const abort = (capture: Capture, unchanged?: true): Promise<void> =>
		lock.serialize(async () => {
			if (active.delete(capture) && !unchanged) for (const other of active) other.contaminated = true;
		});
	const dispose = (): Promise<void> =>
		lock.serialize(async () => {
			for (const capture of active) capture.contaminated = true;
			active.clear();
			const closingClock = clock;
			clock = undefined;
			await closingClock?.handle.close();
		});
	return { begin, dispose };
}
/** Remove a tree this process owns, whatever modes a command left in it: a directory without owner write or search keeps its entries. */
const removeOwnedTree = (target: string): Promise<void> =>
	rm(target, { recursive: true, force: true }).catch(async (error: unknown) => {
		if (!hasErrorCode(error, "EACCES")) throw error;
		const grant = async (directory: string): Promise<void> => {
			if (!(await lstat(directory).catch(() => undefined))?.isDirectory()) return;
			await chmod(directory, 0o700).catch(() => {});
			for (const entry of await readFilesystemDirectory(directory).catch(() => []))
				if (entry.isDirectory()) await grant(path.join(directory, entry.name));
		};
		await grant(target);
		await rm(target, { recursive: true, force: true });
	});
function throwIfAborted(signal?: AbortSignal): void {
	if (!signal?.aborted) return;
	throw signal.reason instanceof Error ? signal.reason : new Error("sandbox preparation aborted");
}
/** Case-insensitive volumes, and win32 trailing dots and spaces, spell the same excluded entry differently. */
const snapshotSegment =
	process.platform === "win32"
		? (segment: string) => segment.replace(/[. ]+$/u, "").toLowerCase()
		: process.platform === "darwin"
			? (segment: string) => segment.toLowerCase()
			: (segment: string) => segment;
function isSnapshotExcluded(relative: string): boolean {
	return relative
		.split("/")
		.map(snapshotSegment)
		.some(
			(segment) =>
				(speculativeConfig().filesystem.excludes as readonly string[]).includes(segment) ||
				(segment.startsWith(SANDBOX_STAGING_FILE_PREFIX) && segment.endsWith(".tmp")),
		);
}
async function collectSandboxChanges(
	workspace: ReturnType<typeof createWorkspaceObservation>,
	frontier: readonly string[],
): Promise<readonly SandboxFileChange[]> {
	return withFilesystemReadView(undefined, async (view) => {
		const resources = [...new Set(frontier)]
			.filter((resource) => !isSnapshotExcluded(slash(resource)))
			.sort();
		// Changed files share their directories: each is walked once, and the files are read concurrently.
		const capture = sharedWalk(captureFilesystemEntry);
		// Changed files share their directories: each is walked once, and the files are read concurrently.
		const source = sharedWalk((target) => captureFilesystemEntry(target, undefined, view));
		return mapFilesystem(resources, async (resource): Promise<SandboxFileChange> => {
			if (!resource || path.isAbsolute(resource) || resource.split("/").includes(".."))
				throw new Error(`invalid sandbox change path: ${resource}`);
			const target = path.resolve(workspace.sourceRoot, resource);
			const sandboxTarget = path.resolve(workspace.sandboxRoot, resource);
			if (
				!containsFilesystemPath(workspace.sourceRoot, target) ||
				!containsFilesystemPath(workspace.sandboxRoot, sandboxTarget)
			)
				throw new Error(`sandbox change escapes workspace: ${resource}`);
			await assertNoSymlinkPath(workspace.sourceRoot, target, source);
			await assertNoSymlinkPath(workspace.sandboxRoot, sandboxTarget, capture);
			const before = workspace.baselineFrontier.has(resource)
				? workspace.baselineFrontier.get(resource)
				: await workspace.readBase(resource);
			const after = await readRegularState(sandboxTarget, undefined, undefined, workspace.contents);
			return {
				root: workspace.sourceRoot,
				target,
				resource,
				before: before?.content,
				after: after?.content,
				beforeMode: before?.mode,
				afterMode: after?.mode,
				afterModified: after?.identity && String(after.identity.mtimeNs),
				...(before?.settled ? { beforeIdentity: before.settled } : {}),
			};
		});
	});
}
/** Changed upper objects select the merged paths to read; actual directory names
 * decide visibility without a second implementation of OverlayFS whiteouts. */
function createOverlayStructureCapture(
	sandboxRoot: string,
	{ baseline, device, upperRoot }: NonNullable<SandboxWorkspaceObservation["overlay"]>,
	versions?: FilesystemVersionSource,
) {
	const capture = new WorkspaceStructureCapture(
		upperRoot,
		{
			maxFiles: speculativeConfig().filesystem.maxTreeEntries,
			layered: true,
			visitEntry: (relative) => !isSnapshotExcluded(slash(relative)),
			observeStat: (relative, stat) => {
				if (!stat.isDirectory() && !stat.isFile() && !(stat.isCharacterDevice() && stat.rdev === 0n))
					throw new Error(`unsupported OverlayFS upper inode: ${relative}`);
			},
		},
		versions,
	);
	let previous:
		| {
				readonly upper: WorkspaceStructureSnapshot;
				readonly merged: WorkspaceStructureSnapshot;
				readonly listings: ReadonlyMap<string, ReadonlySet<string>>;
		  }
		| undefined;
	return async (): Promise<WorkspaceStructureSnapshot> => {
		const retained = previous;
		const upper = await capture.capture();
		if (retained?.upper === upper) return retained.merged;
		if (!upper.complete) throw new Error("OverlayFS structure frontier is incomplete");
		const entries = new Map(
			[...baseline.entries].map(([name, entry]) => [
				name,
				entry.kind === "file" && entry.object
					? { ...entry, object: `${device}:${entry.object.split(":")[1]}` }
					: entry,
			]),
		);
		const refresh = new Set<string>();
		const dirty = new Set<string>();
		const listings = new Map<string, ReadonlySet<string>>();
		for (const [resource, entry] of upper.entries)
			for (const name of workspaceAncestors(resource)) {
				refresh.add(name);
				if (retained?.upper.entries.get(resource) !== entry) dirty.add(name);
			}
		await mapFilesystem([...refresh], async (resource) => {
			let entry: WorkspaceStructureEntry | undefined;
			if (retained && !dirty.has(resource)) {
				entry = retained.merged.entries.get(resource);
				listings.set(resource, retained.listings.get(resource)!);
			} else {
				const captured = await captureFilesystemEntry(path.join(sandboxRoot, resource), "directory").catch(
					(error) => {
						if (!isMissing(error)) throw error;
					},
				);
				entry = captured
					? captureWorkspaceStructureEntry(captured, resource ? [] : speculativeConfig().filesystem.excludes)
					: undefined;
				listings.set(resource, new Set(captured?.entries?.map((child) => child.name)));
			}
			if (entry) entries.set(resource, entry);
			else entries.delete(resource);
		});
		for (const name of entries.keys())
			for (let current = name; current; current = path.dirname(current).replace(/^\.$/, "")) {
				const names = listings.get(path.dirname(current).replace(/^\.$/, ""));
				if (names && !names.has(path.basename(current))) {
					entries.delete(name);
					break;
				}
			}
		const merged = workspaceStructureSnapshot(
			sandboxRoot,
			entries,
			baseline.complete && entries.size - 1 <= speculativeConfig().filesystem.maxTreeEntries,
		);
		previous = { upper, merged, listings };
		return merged;
	};
}
async function assertCommitTarget(
	change: SandboxWorkspaceChange,
	capture?: typeof captureFilesystemEntry,
): Promise<void> {
	const root = path.resolve(change.root);
	const target = path.resolve(change.target);
	if (
		!containsFilesystemPath(root, target) ||
		(target === root && !change.validationOnly) ||
		target !== path.resolve(root, change.resource) ||
		isSnapshotExcluded(slash(change.resource))
	) {
		throw new Error(`sandbox commit path escapes workspace: ${change.resource}`);
	}
	if (change.kind === "directory" && change.operation && (change.before || !change.after)) {
		throw new Error("Native directory creation requires an absent baseline and a sealed result");
	}
	if (change.kind !== "directory")
		for (const name of [
			...(change.aliases ?? []),
			...(change.afterAliases ?? []),
			...(change.object ? [change.object.path] : []),
		]) {
			if (!path.isAbsolute(name) || !containsFilesystemPath(root, name) || path.resolve(name) === root)
				throw new Error("file object name escapes workspace");
		}
	await assertNoSymlinkPath(root, target, capture);
}
async function readRegularState(
	target: string,
	maxBytes?: number,
	view?: FilesystemReadView,
	contents?: ContentChunkPool,
): Promise<RegularFileState | undefined> {
	try {
		const limit = Math.min(
			maxBytes ?? Infinity,
			contents?.limits.maxLogicalBytes ?? speculativeConfig().filesystem.maxTreeBytes,
		);
		const captured = await captureFileContent(target, limit, contents, {
			digest: false,
			view,
		});
		return {
			content: captured.content,
			mode: process.platform === "win32" ? 0 : Number(captured.stat.mode & 511n),
			identity: captured.stat,
		};
	} catch (error) {
		if (isMissing(error)) return undefined;
		throw error;
	}
}
async function assertFileAliases(
	root: string,
	target: string,
	aliases: readonly string[],
	identity?: BigIntStats,
	view = FilesystemReadView.ambient(),
): Promise<void> {
	if (!identity || identity.nlink !== BigInt(aliases.length) || !aliases.includes(target))
		throw new Error("file alias set changed");
	for (const alias of aliases) {
		await assertNoSymlinkPath(root, alias, (target, read) => captureFilesystemEntry(target, read, view));
		if (!sameFilesystemIdentity(identity, await view.stat(alias)))
			throw new Error("file alias identity changed");
	}
}
/** Capture a directory without following links and reject concurrent namespace changes. */
export async function readSandboxDirectoryState(
	target: string,
	captureNames?: (names: readonly string[]) => void,
	view?: FilesystemReadView,
): Promise<SandboxDirectoryState | undefined> {
	try {
		const { info, entries } = await captureFilesystemEntry(target, "directory", view);
		if (!entries) throw new Error(`sandbox resource is not a real directory: ${target}`);
		captureNames?.(entries.map((entry) => entry.name));
		return {
			entriesDigest: directoryEntriesDigest(entries),
			mode: Number(info.mode & 511n),
			uid: Number(info.uid),
			gid: Number(info.gid),
		};
	} catch (error) {
		if (isMissing(error)) return undefined;
		throw error;
	}
}
export function sameSandboxState(
	left: RegularFileState | SandboxDirectoryState | undefined,
	right: RegularFileState | SandboxDirectoryState | undefined,
): boolean {
	if (!left || !right) return left === right;
	if ("content" in left || "content" in right)
		return (
			"content" in left &&
			"content" in right &&
			sameContent(left.content, right.content) &&
			(right.mode === 0 || sameExecutableMode(left.mode, right.mode))
		);
	return (
		left.entriesDigest === right.entriesDigest &&
		left.mode === right.mode &&
		left.uid === right.uid &&
		left.gid === right.gid
	);
}
async function verifySandboxDirectoryAfter(
	change: SandboxDirectoryChange,
	view = FilesystemReadView.ambient(),
): Promise<readonly string[] | undefined> {
	// Native mkdir observes existence; subsequent new-entry reads/access need their own proof.
	let names: readonly string[] | undefined;
	const same = change.operation
		? (await view.stat(change.target)).isDirectory()
		: sameSandboxState(
				await readSandboxDirectoryState(
					change.target,
					(captured) => {
						names = captured;
					},
					view,
				),
				change.after,
			);
	if (!same) throw new Error(`directory changed while committing: ${change.resource}`);
	return names;
}
function assertExistingInputPolicy(change: SandboxWorkspaceChange): void {
	// Default ACLs and other inherited creation policy are not observable through this binding.
	if (!change.validationOnly && change.before === undefined)
		throw new Error("Created input permissions require authoritative execution");
}
function sandboxChangeBytes(change: SandboxWorkspaceChange | undefined): number {
	return !change || change.kind === "directory"
		? 0
		: contentMemory(change.before) + contentMemory(change.after);
}
/** Take ownership once, before cleanup or commit locks can yield to a retained caller reference. */
function ownSandboxChanges(
	changes: readonly SandboxWorkspaceChange[],
	project = (name: string) => path.resolve(name),
): SandboxWorkspaceChange[] {
	const result = new Map<string, SandboxWorkspaceChange>();
	for (const change of changes) {
		const key = filesystemPathKey(change.target);
		const previous = result.get(key);
		if (!previous) {
			result.set(key, change);
			continue;
		}
		if (
			filesystemPathKey(previous.root) !== filesystemPathKey(change.root) ||
			(previous.kind === "directory") !== (change.kind === "directory") ||
			previous.validationOnly !== change.validationOnly ||
			previous.operation !== change.operation ||
			(previous.kind !== "directory" &&
				change.kind !== "directory" &&
				JSON.stringify(projectFileMutation(previous)) !== JSON.stringify(projectFileMutation(change)))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		const same =
			previous.kind === "directory" && change.kind === "directory"
				? sameSandboxState(previous.before, change.before)
				: previous.kind !== "directory" &&
					change.kind !== "directory" &&
					sameContent(previous.before, change.before) &&
					(previous.beforeMode === undefined ||
						change.beforeMode === undefined ||
						sameExecutableMode(previous.beforeMode, change.beforeMode));
		if (!same) throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		const baseline =
			previous.kind === "directory"
				? { before: previous.before }
				: {
						before: previous.before,
						beforeMode: previous.beforeMode,
						beforeIdentity: previous.beforeIdentity,
					};
		result.set(key, {
			...change,
			...baseline,
			accessMode: (previous.accessMode ?? 0) | (change.accessMode ?? 0),
		} as SandboxWorkspaceChange);
	}
	return [...result.values()]
		.map((change): SandboxWorkspaceChange => {
			const target = {
				root: project(change.root),
				target: project(change.target),
			};
			return change.kind === "directory"
				? {
						...change,
						...target,
						before: change.before && { ...change.before },
						after: change.after && { ...change.after },
					}
				: {
						...change,
						...target,
						...projectFileMutation(change, project),
						before: ownContent(change.before),
						after: ownContent(change.after),
					};
		})
		.sort((left, right) => filesystemPathKey(left.target).localeCompare(filesystemPathKey(right.target)));
}
async function restoreChanges(
	changes: readonly SandboxWorkspaceChange[],
	baselines: ReadonlyMap<SandboxWorkspaceChange, RegularFileState | SandboxDirectoryState | undefined>,
	view: FilesystemReadView,
	contents?: ContentChunkPool,
): Promise<void> {
	const errors: unknown[] = [];
	for (const change of [...changes].reverse()) {
		try {
			await assertCommitTarget(change, (target, read) => captureFilesystemEntry(target, read, view));
			const baseline = baselines.get(change);
			if (change.kind === "directory") {
				const directory = baseline as SandboxDirectoryState | undefined;
				if (!directory) {
					try {
						await rmdir(change.target);
					} catch (error) {
						if (!isMissing(error)) throw error;
					}
				} else {
					const current = await readSandboxDirectoryState(change.target, undefined, view);
					if (!current) {
						await createParentDirectories(change.root, change.target, view);
						await mkdir(change.target, { mode: directory.mode });
					}
					if (process.platform !== "win32") await chmod(change.target, directory.mode);
				}
				continue;
			}
			const file = baseline as RegularFileState | undefined;
			if (!file) await rm(change.target, { force: true });
			else {
				const temporary = await stageAtomicWrite(file.content, file.mode, change.root);
				try {
					await createParentDirectories(change.root, change.target, view);
					await replaceFile(temporary, change.target, file.mode);
				} finally {
					await rm(temporary, { force: true }).catch(() => undefined);
				}
			}
		} catch (error) {
			errors.push(error);
		}
	}
	for (const change of changes) {
		try {
			const baseline = baselines.get(change);
			const current =
				change.kind === "directory"
					? await readSandboxDirectoryState(change.target, undefined, view)
					: await readRegularState(change.target, undefined, view, contents);
			if (!sameSandboxState(current, baseline))
				throw new Error(`sandbox rollback did not restore: ${change.resource}`);
		} catch (error) {
			errors.push(error);
		}
	}
	if (errors.length > 0) throw new AggregateError(errors, "failed to restore sandbox commit changes");
}
function resolveCommitMode(
	current: RegularFileState | undefined,
	change: SandboxFileChange,
): number | undefined {
	if (process.platform === "win32") return undefined;
	if (change.afterMode === undefined) return current?.mode ?? 0o644;
	if (!current || change.beforeMode === undefined) return change.afterMode;
	if (sameExecutableMode(change.beforeMode, change.afterMode)) return current.mode;
	return isExecutableMode(change.afterMode)
		? current.mode | (change.afterMode & 0o111)
		: current.mode & ~0o111;
}
function sameExecutableMode(left: number, right: number): boolean {
	return isExecutableMode(left) === isExecutableMode(right);
}
function isExecutableMode(mode: number): boolean {
	return (mode & 0o111) !== 0;
}
async function stageAtomicWrite(
	content: ContentVersion,
	mode: number | undefined,
	stagingDirectory: string,
): Promise<string> {
	await mkdir(stagingDirectory, { recursive: true });
	const temporary = path.join(stagingDirectory, `${SANDBOX_STAGING_FILE_PREFIX}${randomUUID()}.tmp`);
	const fileMode = process.platform === "win32" ? undefined : mode;
	const handle = await open(temporary, "wx", fileMode ?? 0o600);
	let staged = false;
	try {
		await writeContent(handle, content);
		await handle.sync();
		if (fileMode !== undefined) await handle.chmod(fileMode);
		staged = true;
		return temporary;
	} finally {
		await handle.close().catch(() => undefined);
		if (!staged) await rm(temporary, { force: true }).catch(() => undefined);
	}
}
async function createParentDirectories(
	sourceRoot: string,
	target: string,
	view: FilesystemReadView,
	created?: string[],
	ensured?: Set<string>,
): Promise<void> {
	const root = path.resolve(sourceRoot);
	const parent = path.dirname(path.resolve(target));
	const relative = relativeFilesystemPath(root, parent);
	if (relative === undefined) throw new Error(`sandbox commit path escapes workspace: ${target}`);
	let current = root;
	for (const segment of relative.split(path.sep).filter(Boolean)) {
		current = path.join(current, segment);
		if (ensured?.has(current)) continue; // This commit made or found it real; each change's own path check precedes this.
		try {
			await mkdir(current);
			created?.push(current);
		} catch (error) {
			if (!hasErrorCode(error, "EEXIST")) throw error;
			const info = await view.stat(current);
			if (info.isSymbolicLink() || !info.isDirectory()) {
				throw new Error(`sandbox commit parent is not a real directory: ${current}`, { cause: error });
			}
		}
		ensured?.add(current);
	}
}
/** Give files back the modification times their producers left, which later readers may have observed. Node sets only
 * microseconds, so Linux uses the shared native binding or coreutils; elsewhere a file keeps its commit's time. */
export async function restoreModifiedTimes(
	files: readonly (readonly [target: string, modified: string | undefined])[],
): Promise<void> {
	const times = files.flatMap(([target, modified]) =>
		/^\d+$/.test(modified ?? "")
			? [[modified!.padStart(10, "0").slice(0, -9), modified!.slice(-9).padStart(9, "0"), target]]
			: [],
	);
	const pairs = times.flatMap(([seconds, nanoseconds, target]) => {
		if (!nativeFilesystem) return [`@${seconds}.${nanoseconds}`, target!];
		const record = Buffer.concat([Buffer.alloc(16), Buffer.from(`${target}\0`)]);
		record.writeBigInt64LE(BigInt(seconds!), 0);
		record.writeBigInt64LE(BigInt(nanoseconds!), 8);
		if (nativeFilesystem(-3, record) !== 0) throw new Error(`could not restore modification time: ${target}`);
		return [];
	});
	for (
		let start = 0;
		process.platform === "linux" && start < pairs.length;
		start += speculativeConfig().filesystem.timestampRestoreBatch
	)
		await execHostFile("sh", [
			"-c",
			'while [ $# -gt 0 ]; do touch -chm -d "$1" -- "$2" || exit; shift 2; done',
			"sh",
			...pairs.slice(start, start + speculativeConfig().filesystem.timestampRestoreBatch),
		]);
}
async function replaceFile(temporary: string, target: string, mode?: number): Promise<void> {
	await rename(temporary, target);
	if (mode !== undefined && process.platform !== "win32") await chmod(target, mode);
}
/** Root and every proven file name share the native queue; resolved keys give all transactions one strict order. */
async function withCommitLocks<T>(
	changes: readonly SandboxWorkspaceChange[],
	run: () => Promise<T>,
	view: FilesystemReadView,
): Promise<T> {
	if (!changes.length) return run();
	try {
		const names = [
			...new Set(
				changes
					.flatMap((change) => [
						change.root,
						change.target,
						...(change.kind === "directory"
							? []
							: [...(change.aliases ?? []), ...(change.object ? [change.object.path] : [])]),
					])
					.map((name) => path.resolve(name)),
			),
		];
		const resolve = () =>
			mapFilesystem(names, (name) =>
				view.realpath(name).catch((error) => {
					if (isMissing(error) || hasErrorCode(error, "ENOTDIR")) return name;
					throw error;
				}),
			);
		const keys = await resolve();
		const { withFileMutationQueue } = await import("@earendil-works/pi-coding-agent");
		const apply = async () => {
			if ((await resolve()).some((key, index) => key !== keys[index]))
				throw new Error("commit file names changed while waiting");
			return run();
		};
		return await [...new Set(keys)]
			.sort()
			.reduceRight((next, key) => () => withFileMutationQueue(key, next), apply)();
	} catch (error) {
		throw effectCommitFailure(error, "recoverable");
	}
}
/** Materialize the same immutable content records used by reads and transaction deltas. */
async function materializeWorkspaceTree(tree: WorkspaceContentSnapshot, root: string): Promise<void> {
	await mkdir(root);
	const objects = new Map<string, string>();
	for (const [relative, entry] of [...tree.snapshot.entries].sort(([a], [b]) => a.length - b.length)) {
		if (!relative) continue;
		const target = path.resolve(root, relative);
		if (!containsFilesystemPath(root, target)) throw new Error("snapshot entry escapes workspace");
		if (entry.kind === "directory") {
			await mkdir(target, { recursive: true });
		} else if (entry.kind === "file") {
			const captured = tree.files.get(relative);
			if (!captured) throw new Error(`snapshot file unavailable: ${relative}`);
			if (entry.links > 1 && entry.aliases?.length !== entry.links)
				throw new Error("workspace hardlink namespace is not closed");
			const existing = entry.object && objects.get(entry.object);
			if (existing) await link(existing, target);
			else {
				await using handle = await open(target, "wx", entry.mode);
				await writeContent(handle, captured.content);
				if (entry.object) objects.set(entry.object, target);
			}
		} else if (entry.kind === "symlink") {
			const resolved = path.resolve(path.dirname(path.join(tree.snapshot.root, relative)), entry.target);
			if (path.isAbsolute(entry.target) || !containsFilesystemPath(tree.snapshot.root, resolved))
				throw new Error("snapshot symlink escapes workspace");
			const destination = await realpath(path.join(tree.snapshot.root, relative)).catch((error) => {
				if (isMissing(error)) return resolved;
				throw error;
			});
			if (!containsFilesystemPath(tree.snapshot.root, destination))
				throw new Error("snapshot symlink escapes workspace");
			await symlink(entry.target, target);
		} else throw new Error(`unsupported snapshot entry: ${relative}`);
	}
}
