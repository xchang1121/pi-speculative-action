import { spawn } from "node:child_process";
import { ChunkedContent, ContentChunkPool, captureContent, contentMemory, ownContent, readContent, sameContent, writeContent,
	lendContentChanges, receiveContentChanges, type ContentChangesTransfer, type ContentVersion } from "./content-chunks.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { isSafeCount } from "./number-utils.ts";
import { execHostFile } from "./executable-path.ts";
import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants, type BigIntStats } from "node:fs";
import {
	access, chmod, type FileHandle, link, lstat, lutimes, mkdir, mkdtemp, open, readFile, rename, rm, rmdir, unlink, utimes, writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { containsFilesystemPath, filesystemPathKey, relativeFilesystemPath, slash } from "./path-utils.ts";
import { errorMessage, hasErrorCode, isMissing } from "./error-utils.ts";
import {
	createCommittedResourceInputs, filesystemExecutionDependsOn, type SpeculativeAgentExecutionWorld, type SpeculativeToolExecutionContext,
} from "./agent-execution-world.ts";
import type { WorldBranch, WorldCheckpoint, WorldExecutionMetrics } from "./execution-world.ts";
import {
	advanceFilesystemClock, assertNoSymlinkPath, captureFilesystemEntry, captureStableFile, fileIdentity, mapFilesystem, sameFilesystemIdentity, readFilesystemDirectory, openFilesystemFile,
	withFilesystemReadView, openFilesystemReadView, FilesystemReadView, FilesystemCaptureChangedError,
	settledIdentity, sharedWalk, pinFilesystemObject, openAnonymousFilesystemFile, FILESYSTEM_OBSERVATION_FIELDS, FILESYSTEM_METADATA_FIELDS as SNAPSHOT_METADATA_FIELDS, type FilesystemObservationEvidence,
} from "./filesystem-evidence.ts";
import { WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import { EffectCommit, effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import {
	LinuxOverlayfsCapabilityRegistry, LinuxOverlayfsUnsafeCleanupError, mountLinuxOverlayfs,
	type LinuxOverlayfsMount, type LinuxOverlayfsOptions,
} from "./linux-overlayfs.ts";
import {
	captureWorkspaceStructure, captureWorkspaceStructureEntry, statChangeDigest, sameStructureEntry, workspaceStructureSnapshot, directoryEntriesDigest, diffWorkspaceStructures, ExecutionPathProjection,
	type WorkspaceStructureSnapshot, type WorkspaceTransactionDiff,
} from "./process-observation.ts";
import { ResourceVersionManager, type ResourceChangeSet, type ResourceVersionToken, type ResourceInput } from "./resource-version.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { ResourceValidation } from "./settlement.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import {
	deferredWorkspaceTransactionDriver, orderWorkspaceChanges, type WorkspaceRegularDelta, type WorkspaceStructureDriver,
	type WorkspaceTransactionCapture, type WorkspaceTransactionDelta, type WorkspaceTransactionDriver, type WorkspaceTransactionOwnership,
} from "./workspace-transaction.ts";
import { connectNativeOwner, ownsNativeProcesses } from "./native-owner.ts";
import { nativeFilesystem } from "./process-startup.mjs";
import { projectFileMutation } from "./workspace-state.ts";

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

export type SandboxDirectoryState = import("./workspace-state.ts").WorkspaceDirectoryState;

export interface SandboxDirectoryChange extends SandboxChangeTarget {
	readonly kind: "directory";
	/** Preserve native creation policy; private directory permissions are not Actor metadata. */
	readonly operation?: "mkdir";
	readonly before?: SandboxDirectoryState;
	readonly after?: SandboxDirectoryState;
}

export type SandboxWorkspaceChange = SandboxFileChange | SandboxDirectoryChange;

interface RegularFileState { readonly content: ContentVersion; readonly mode: number; readonly identity?: BigIntStats; readonly settled?: string; }

export interface SandboxExecutionDelta { readonly output: ToolSettlement; readonly changes: readonly SandboxWorkspaceChange[]; }

export interface WorkspaceExecutionSnapshot extends SandboxExecutionDelta { readonly executionMetrics: WorldExecutionMetrics; readonly dispose?: () => Promise<void>; }

export type WorkspaceSandboxDriver = "auto" | "git" | "overlayfs";

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
	readonly id: string; readonly sourceRoot: string; readonly sandboxRoot: string; readonly processRoot: string;
	readonly gitBinary: string; readonly repository: string; readonly commit: string;
	readonly baselineFrontier: Map<string, RegularFileState | undefined>; readonly sourceDevice: bigint;
	readonly initialStructure: WorkspaceStructureSnapshot;
	readonly metadata?: Awaited<ReturnType<typeof preserveSnapshotMetadata>>;
	readonly modes?: ReadonlyMap<string, number>;
	readonly liveBase?: WorkspaceStructureSnapshot;
	readonly overlay?: { readonly upperRoot: string; readonly device: string; readonly baseline: WorkspaceStructureSnapshot };
	readonly clock: { readonly path: string; readonly links: 0 | 1; readonly roots: readonly string[] };
	readonly hints: ResourceChangeSet;
}

export interface SandboxWorkspaceTransfer {
	readonly data: Omit<SandboxWorkspaceObservation, "baselineFrontier">;
	readonly baseline: ContentChangesTransfer<{ readonly resource: string; readonly before?: ContentVersion; readonly mode?: number; readonly settled?: string }>;
	readonly storeRoot?: string;
}

const workspaceOwners = new Map<string, { readonly capability: readonly unknown[]; readonly observation: ReturnType<typeof ownWorkspaceObservation> }>();
export function nativeWorkspaceOwner(workspace: SandboxWorkspaceContext): Promise<string> | undefined {
	const owner = workspaceOwners.get(workspace.sandboxRoot);
	return owner && workspaceCapability(workspace).every((value, index) => value === owner.capability[index]) ? owner.observation.ready() : undefined;
}
export function nativeWorkspaceObservation(workspace: SandboxWorkspaceContext) { return workspaceOwners.get(workspace.sandboxRoot)?.observation; }
const workspaceCapability = (workspace: SandboxWorkspaceContext) => [workspace.sourceRoot, workspace.sandboxRoot, workspace.processRoot,
	workspace.metadataImage, workspace.metadataMounts, workspace.metadataObjects, workspace.projectMetadata, workspace.observationExcludes, workspace.structure, workspace.structure.capture,
	workspace.transactions, workspace.transactions.begin, workspace.transactions.dispose, workspace.captureChanges, workspace.sourceChanges];

interface WorkspaceObservation extends SandboxWorkspaceContext, SandboxWorkspaceObservation {
	readonly readBase: (resource: string, maxBytes?: number) => Promise<RegularFileState | undefined>;
}

function createWorkspaceObservation(data: SandboxWorkspaceObservation, sourceChanges: () => ResourceChangeSet, contents?: ContentChunkPool): WorkspaceObservation {
	let workspace!: WorkspaceObservation;
	const baseGit = bindGit(data.gitBinary, path.dirname(data.repository), ["--git-dir", data.repository]);
	const structure: WorkspaceStructureDriver = { capture: () => captureSandboxStructure(data) };
	const transactions = deferredWorkspaceTransactionDriver(() => createGitWorkspaceTransactionDriver(workspace));
	workspace = {
		...data, contents, structure, transactions, sourceChanges, observationExcludes: SNAPSHOT_EXCLUDES,
		metadataImage: data.metadata?.image, metadataMounts: data.metadata?.mounts, metadataObjects: data.metadata?.image ? data.metadata.objects : undefined,
		projectMetadata: (stat: FilesystemObservationEvidence): FilesystemObservationEvidence => {
			const row = data.metadata?.mappings.get(`${stat.dev}:${stat.ino}`);
			if (!row) return { ...stat, dev: data.sourceDevice };
			const [physical, source, atimePolicy] = row, links = source.nlink + stat.nlink - physical.nlink, unchanged = stat.size === physical.size && stat.mtimeNs === physical.mtimeNs && stat.ctimeNs === physical.ctimeNs;
			// Copying gives an object a new ctime, which can spuriously trigger relatime on its first read.
			const copiedAccess = unchanged && stat.atimeNs > physical.atimeNs && (atimePolicy === 1 || atimePolicy === 2 &&
				source.atimeNs > source.mtimeNs && source.atimeNs > source.ctimeNs && stat.atimeNs / 1_000_000_000n - source.atimeNs / 1_000_000_000n < 86400n);
			return { ...Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field,
				stat[field] === physical[field] && (field !== "blocks" || unchanged) || field === "atimeNs" && copiedAccess ? source[field] : stat[field]])),
				dev: data.sourceDevice, ino: source.ino, nlink: links < 0n ? 0n : links } as FilesystemObservationEvidence;
		},
		captureChanges: (frontier: readonly string[]) => collectSandboxChanges(workspace, frontier),
		readBase: async (resource: string, maxBytes = contents?.limits.maxLogicalBytes ?? WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES) => {
			if (data.liveBase) return readLiveBase(data.liveBase, resource, maxBytes, contents);
			const state = await readGitTreeRegularState(baseGit, data.commit, resource, maxBytes, contents), mode = data.metadata?.modes.get(slash(resource)) ?? data.modes?.get(slash(resource));
			return state && mode !== undefined ? { ...state, mode } : state;
		},
	};
	return workspace;
}

async function captureSandboxStructure(data: Pick<SandboxWorkspaceObservation, "sandboxRoot" | "overlay" | "clock">, fence = false): Promise<WorkspaceStructureSnapshot> {
	const snapshot = await (data.overlay ? captureOverlayWorkspaceStructure(data.sandboxRoot, data.overlay)
		: captureWorkspaceStructure(data.sandboxRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: SNAPSHOT_EXCLUDES }));
	if (fence) {
		const clock = await openWorkspaceChangeClock(data);
		try { await advanceWorkspaceChangeClock(clock, snapshot); } finally { await clock.handle.close(); }
	}
	return snapshot;
}

async function openWorkspaceChangeClock({ sandboxRoot, clock }: Pick<SandboxWorkspaceObservation, "sandboxRoot" | "clock">) {
	const handle = await (clock.links === 0 ? openAnonymousFilesystemFile(clock.path)
		: open(clock.path, fsConstants.O_WRONLY | fsConstants.O_CREAT | (fsConstants.O_NOFOLLOW ?? 0), 0o600));
	try {
		const [workspaceInfo, clockInfo, ...rootInfo] = await Promise.all([lstat(sandboxRoot), handle.stat(), ...clock.roots.map(root => lstat(root))]);
		if (!workspaceInfo.isDirectory() || !clockInfo.isFile() || clockInfo.nlink !== clock.links || rootInfo.length === 0 ||
			rootInfo.some(root => !root.isDirectory() || root.dev !== clockInfo.dev)) {
			throw new Error("workspace transaction clock is not private or its backing timestamp domain changed");
		}
		return { handle, identity: clockInfo };
	} catch (error) { await handle.close(); throw error; }
}

async function advanceWorkspaceChangeClock(clock: Awaited<ReturnType<typeof openWorkspaceChangeClock>> | undefined, snapshot: WorkspaceStructureSnapshot): Promise<void> {
	let boundary = Number.NEGATIVE_INFINITY;
	for (const entry of snapshot.entries.values()) boundary = Math.max(boundary, entry.changeTimeMs);
	if (!Number.isFinite(boundary)) throw new Error("workspace change clock boundary is unavailable");
	if (!clock) throw new Error("workspace transaction clock is unavailable");
	await advanceFilesystemClock(clock.handle, boundary, clock.identity);
}

export async function restoreWorkspaceObservation(transfer: SandboxWorkspaceTransfer, hints: () => Promise<ResourceChangeSet>) {
	const versions = new ResourceVersionManager(transfer.data.sourceRoot), cursor = await versions.observeChanges();
	const store = transfer.storeRoot ? new ProvenanceCertificateStore(transfer.storeRoot) : undefined, contents = store && new ContentChunkPool(store);
	const release = contents?.retain();
	let workspace: WorkspaceObservation | undefined, disposal: Promise<void> | undefined;
	// Start the new notification window before reading the allocator's existing one, leaving no transfer gap.
	const dispose = () => disposal ??= (async () => { try { await workspace?.transactions.dispose(); } finally {
		cursor.release(); versions.close(); await release?.(); await store?.dispose();
	} })();
	try {
		const baseline = await receiveContentChanges(transfer.baseline, contents);
		const data: SandboxWorkspaceObservation = { ...transfer.data, hints: await hints(), baselineFrontier: new Map(baseline.map(entry =>
			[entry.resource, entry.before === undefined ? undefined : { content: entry.before, mode: entry.mode!, settled: entry.settled }])) };
		workspace = createWorkspaceObservation(data, () => {
			const changes = versions.changesSince(cursor); return { uncertain: changes.uncertain || data.hints.uncertain, paths: [...new Set([...data.hints.paths, ...changes.paths])] };
		}, contents);
		return { workspace, dispose };
	} catch (error) { await dispose(); throw error; }
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
	readonly afterCapture?: (workspace: SandboxWorkspaceContext, capture: SandboxExecutionDelta) => Promise<readonly SandboxWorkspaceChange[] | void>;
	/** Optional exact freshness proof captured by the operation-specific execution substrate. */
	readonly validate?: () => Promise<ResourceValidation>;
}

export interface PrepareSandboxWorkspaceOptions extends WorkspaceSandboxOptions { readonly signal?: AbortSignal; }

type PrivateSandboxWorkspace = Awaited<ReturnType<WorkspaceSandboxService["createPrivateSandboxWorkspace"]>>;

type PooledGitRepository = Awaited<ReturnType<typeof createSandboxRepository>> & {
	baseline?: { readonly commit: string; readonly tree: string; readonly version: ResourceVersionToken; readonly aliases: readonly (readonly string[])[] };
	/** The workspace's own structure, reused while its version reports no change. */
	liveBase?: { readonly version: ResourceVersionToken; readonly structure: WorkspaceStructureSnapshot };
	/** One idle checkout, either warming or returned with its filesystem writes recorded. */
	prepared?: Promise<PreparedGitWorkspace>;
	autoDriverDecision?: AutoWorkspaceDriverDecision;
	registration?: Promise<PooledGitRepository>;
	idleTimer?: ReturnType<typeof setTimeout>;
	prunedAt?: number;
};

interface PreparedGitWorkspace {
	readonly sandboxRoot: string;
	readonly processRoot: string;
	readonly commit: string;
	readonly aliases: readonly (readonly string[])[];
	readonly written?: readonly string[];
	readonly dispose: () => Promise<void>;
}

interface SharedOverlayBaseline extends PreparedGitWorkspace { structure?: Promise<WorkspaceStructureSnapshot>; active: number; readonly modes: ReadonlyMap<string, number>; }

interface AutoWorkspaceDriverDecision {
	readonly baseline: string | WorkspaceStructureSnapshot;
	readonly capabilityFingerprint: string;
	readonly resolved: QualifiedWorkspaceSandboxDriver;
}


function regularStructureTransitions(before: WorkspaceStructureSnapshot, after: WorkspaceStructureSnapshot): readonly string[] {
	if (!before.complete || !after.complete) throw new Error("workspace_structure_limit");
	const paths: string[] = [];
	for (const relativePath of [...new Set([...before.entries.keys(), ...after.entries.keys()])].sort()) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath), current = after.entries.get(relativePath), kind = previous?.kind ?? current?.kind;
		if (sameStructureEntry(previous, current, "change")) continue;
		if (previous && current && previous.kind !== current.kind || kind !== "file" && kind !== "directory")
			throw new Error(`unsupported_workspace_transition:${relativePath}`);
		if (kind === "file") paths.push(relativePath);
	}
	return Object.freeze(paths);
}

function workspaceAncestors(resource: string): string[] {
	const names: string[] = [];
	for (let current = path.normalize(resource); ; current = path.dirname(current)) {
		if (path.parse(current).root || current === "..") throw new Error("workspace ownership path escapes workspace");
		names.push(current === "." ? "" : current);
		if (current === ".") return names;
	}
}

function sameWorkspaceChangeSnapshot(left: WorkspaceStructureSnapshot, right: WorkspaceStructureSnapshot): boolean {
	if (!left.complete || !right.complete || left.entries.size !== right.entries.size) return false;
	for (const [relativePath, entry] of left.entries) { if (!sameStructureEntry(entry, right.entries.get(relativePath), "change")) return false; }
	return true;
}

// The execution world mirrors everything the actor can read below cwd. Git metadata is
// replaced by the private repository and commit's own temporary files are internal.
const SNAPSHOT_EXCLUDES = [".git"] as const;
const SANDBOX_REPOSITORY_IDLE_MS = 5 * 60 * 1000;
/** Replaced baselines stay referenced only by live workspaces; expired unreferenced objects are collected. */
const SANDBOX_PRUNE_INTERVAL_MS = 5 * 60 * 1000;
const WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES = 512 * 1024 * 1024;
const WORKSPACE_TRANSACTION_MAX_FILES = 100_000;
const WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS = 3;
const SANDBOX_STAGING_FILE_PREFIX = ".pi-speculative-";
const GIT_WORKSPACE_FINGERPRINT = "git-worktree-metadata";
// Small-tree gains remain host-sensitive and carry one-time FUSE preparation cost, while the
// 500/1,000-file A/B is material. Use a conservative power-of-two boundary and exact baseline.
const AUTO_OVERLAY_MIN_TREE_ENTRIES = 256;
const SANDBOX_AUTHOR_ENVIRONMENT = {
	GIT_AUTHOR_NAME: "Pi Speculative Action",
	GIT_AUTHOR_EMAIL: "speculative-action@localhost",
	GIT_COMMITTER_NAME: "Pi Speculative Action",
	GIT_COMMITTER_EMAIL: "speculative-action@localhost",
} as const;

export interface WorkspaceCheckpoint {
	readonly token: WorldCheckpoint;
	readonly sourceRoot: string;
	parent?: WorkspaceCheckpoint;
	readonly changes: readonly SandboxWorkspaceChange[];
	committed: boolean;
}

// A handed-out checkpoint outlives its branch. Service disposal also closes every pool.
const retiredWorkspaceCheckpoints = new FinalizationRegistry<() => Promise<void>>(release => { void release().catch(() => {}); });

export interface QualifiedWorkspaceSandboxDriver { readonly driver: Exclude<WorkspaceSandboxDriver, "auto">; readonly fingerprint: string; }

// Preparation owns no additional handles. A retired/replaced pool cannot lend its old snapshot.
const preparedWorkspaceBaselines = new WeakMap<QualifiedWorkspaceSandboxDriver, {
	readonly repository: PooledGitRepository;
	readonly baseline: NonNullable<PooledGitRepository["baseline"]>;
}>();

/** Bound filesystem operations validate all captured inputs and effects in the commit transaction. */
const capturedWorkspaceInputs = Symbol("captured workspace inputs");
type SandboxPreparation = QualifiedWorkspaceSandboxDriver | typeof capturedWorkspaceInputs;

/** Owns workspace repositories and commit serialization for one extension/runtime lifecycle. */
export class WorkspaceSandboxService {
	private store?: ProvenanceCertificateStore;
	private storage?: Promise<{ readonly store: ProvenanceCertificateStore; readonly ownedRoot?: string }>;
	private readonly repositories = new Map<string, Promise<PooledGitRepository>>();
	private readonly contents = new Set<ContentChunkPool>();
	/** Checkpoints belong to this service, including its sibling execution worlds. */
	private readonly checkpoints = new WeakMap<WorldCheckpoint, WorkspaceCheckpoint>();
	private readonly lifetime = new RuntimeLifecycleLane();
	private readonly overlayfsCapabilities = new LinuxOverlayfsCapabilityRegistry();

	constructor(store?: ProvenanceCertificateStore) { this.store = store; }
	useContentStore(store: ProvenanceCertificateStore): void { if (!this.storage) this.store = store; }

	async fingerprint(options: WorkspaceSandboxOptions = {}, sourceRoot?: string): Promise<string> {
		this.assertOpen();
		return (await this.resolveDriver(options, sourceRoot)).fingerprint;
	}

	async qualify(options: WorkspaceSandboxOptions, sourceRoot: string): Promise<QualifiedWorkspaceSandboxDriver> {
		this.assertOpen();
		return await this.resolveDriver(options, sourceRoot);
	}
	branch(options: Pick<SandboxWorkspaceBranchOptions, "cwd" | "action" | "parentCheckpoint" | "validate">, snapshot: WorkspaceExecutionSnapshot): WorldBranch<ToolSettlement> {
		this.assertOpen();
		return this.createBranch(snapshot, path.resolve(options.cwd), options.action, this.checkpoint(options.parentCheckpoint, options.cwd), options.validate);
	}

	async withWorkspace<T>(cwd: string, run: (workspace: SandboxWorkspaceContext) => Promise<T>, options: Pick<WorkspaceSandboxOptions, "gitBinary" | "liveLower"> | string = {}): Promise<T> {
		this.assertOpen();
		const config = typeof options === "string" ? { gitBinary: options } : options;
		return await this.withPrivateWorkspace(cwd, config.gitBinary ?? "git", "git", config, run);
	}

	async commitDelta(delta: SandboxExecutionDelta, privateView?: FilesystemReadView): Promise<ToolSettlement> {
		this.assertOpen();
		const changes = ownSandboxChanges(delta.changes), release = retainWorkspaceChanges(changes);
		try { return await this.commit({ output: delta.output, changes }, undefined, privateView); }
		finally { await release(); }
	}

	dispose(): Promise<void> {
		return this.lifetime.close(async () => {
			await this.lifetime.drain();
			try { await this.closePools(); }
			finally {
				this.overlayfsCapabilities.dispose();
				await Promise.allSettled([...this.contents].map(pool => pool.dispose())); this.contents.clear();
				const storage = await this.storage;
				if (storage?.ownedRoot) { await storage.store.dispose(); await removeOwnedTree(storage.ownedRoot); }
			}
		});
	}

	acceptsCheckpoint(checkpoint: WorldCheckpoint, cwd: string): boolean {
		const owned = this.checkpoints.get(checkpoint);
		return !this.lifetime.sealed && !!owned && filesystemPathKey(owned.sourceRoot) === filesystemPathKey(path.resolve(cwd));
	}

	checkpoint(checkpoint: WorldCheckpoint | undefined, cwd: string): WorkspaceCheckpoint | undefined {
		if (checkpoint === undefined) return undefined;
		const owned = this.checkpoints.get(checkpoint);
		if (!owned) throw new Error("Execution world checkpoint belongs to another backend.");
		if (filesystemPathKey(owned.sourceRoot) !== filesystemPathKey(path.resolve(cwd))) throw new Error("Execution world checkpoint belongs to another workspace.");
		return owned;
	}

	private async createContentPool(): Promise<ContentChunkPool | undefined> {
		this.assertOpen();
		if (process.platform !== "linux") return undefined;
		for (const pool of this.contents) if (pool.closed) this.contents.delete(pool);
		const { store } = await (this.storage ??= this.store ? Promise.resolve({ store: this.store }) :
			mkdtemp(path.join(os.tmpdir(), "pi-workspace-content-")).then(ownedRoot => ({ store: new ProvenanceCertificateStore(ownedRoot), ownedRoot })));
		const pool = new ContentChunkPool(store);
		this.contents.add(pool); return pool;
	}

	private async resolveDriver(options: WorkspaceSandboxOptions, sourceRoot?: string, acquiredRepository?: PooledGitRepository): Promise<QualifiedWorkspaceSandboxDriver> {
		const requested = options.driver ?? "auto";
		if (requested === "git") return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
		const capability = await this.overlayfsCapabilities.capability({
			...(options.overlayfsBinary ? { overlayfsBinary: options.overlayfsBinary } : {}),
			...(options.fusermountBinary ? { fusermountBinary: options.fusermountBinary } : {}),
		});
		if (!capability.available) {
			if (requested === "overlayfs") throw new Error(capability.detail);
			return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
		}
		const overlay = { driver: "overlayfs", fingerprint: `linux-overlayfs:${capability.fingerprint}` } as const;
		if (requested === "overlayfs") return overlay;
		if (!sourceRoot) return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };

		const repository = acquiredRepository ?? await this.acquireRepository(path.resolve(sourceRoot), options.gitBinary ?? "git");
		try {
			// Driver choice is preparation; actual workspace allocation still validates its captured inputs.
			const captured = options.liveLower ? await captureLiveBase(repository) : undefined;
			const cached = repository.autoDriverDecision;
			if (captured && cached?.baseline === captured && cached.capabilityFingerprint === capability.fingerprint) return cached.resolved;
			const entries = captured && [...captured.entries].filter(([resource]) => !isSnapshotExcluded(slash(resource)));
			const live = entries?.every(([, entry]) => entry.kind !== "unsupported" && (entry.kind !== "file" ||
				!entry.aliases?.some(alias => isSnapshotExcluded(slash(path.relative(repository.sourceRoot, alias)))))) ? captured : undefined;
			const baseline = live ?? (await acquireSandboxBaseline(repository, true)).commit;
			if (options.liveLower && !live) return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
			if (cached?.baseline === baseline && cached.capabilityFingerprint === capability.fingerprint) { return cached.resolved; }
			// Structure captures link text only. Follow links with the same containment/cycle guards as Git baselines.
			const links = live && entries!.flatMap(([resource, entry]) => entry.kind === "symlink"
				? [{ path: path.join(repository.sourceRoot, resource), scope: "tree_entries" as const }] : []);
			if (links?.length) await (await repository.versions.capture(links)).release();
			// A live lower preserves process-observed metadata even in small workspaces.
			const resolved = live || parseNullList(await repository.git(["ls-tree", "-r", "-z", "--name-only", baseline as string])).length >= AUTO_OVERLAY_MIN_TREE_ENTRIES
				? overlay : { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT } as const;
			repository.autoDriverDecision = { baseline, capabilityFingerprint: capability.fingerprint, resolved };
			return resolved;
		} finally {
			if (!acquiredRepository) this.releaseRepository(repository);
		}
	}

	createExecutionWorld(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
		this.assertOpen();
		// Generic mutations retain Git unless OverlayFS was explicitly requested. Process routes
		// qualify a live lower from their invocation cwd to preserve native metadata.
		const resolvedOptions: WorkspaceSandboxOptions = options.driver === "overlayfs" ? options : { ...options, driver: "git" };
		const roots = new Set<string>();
		return { id: "git_worktree", scope: "fallback", isolation: "workspace_branch", speculation: {
				capabilities: [...WORKSPACE_PATH_MUTATION_EFFECTS.capabilities, "validation.resource_snapshot"],
				dependsOn: filesystemExecutionDependsOn,
				acceptsCheckpoint: (checkpoint, cwd) => this.acceptsCheckpoint(checkpoint, cwd),
				fingerprint: async ({ action, effect, parentCheckpoint }) => {
					this.assertOpen();
					if (effect === "observation" && !parentCheckpoint) throw new Error("Reads use captured inputs unless based on a private checkpoint");
					if (action && !(action.executionContext as ToolInvocation | undefined)?.filesystem) throw new Error("Workspace execution requires an explicitly bound filesystem operation");
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
			dispose: async () => { const ownedRoots = [...roots]; roots.clear(); await this.lifetime.run(() => this.closePools(ownedRoots)); },
		};
	}

	private createBranch(snapshot: WorkspaceExecutionSnapshot, sourceRoot: string, action: SpeculativeToolExecutionContext["action"], parent?: WorkspaceCheckpoint, validate?: () => Promise<ResourceValidation>): WorldBranch<ToolSettlement> {
		const { changes } = snapshot, { executionFingerprint } = action, backend = "git_worktree", id = randomUUID();
		const checkpoint = Object.freeze({ backend, id, lineage: parent?.token.lineage ?? id, depth: (parent?.token.depth ?? -1) + 1 });
		const retained: WorkspaceCheckpoint = { token: checkpoint, sourceRoot, parent, changes, committed: false };
		retiredWorkspaceCheckpoints.register(checkpoint, retainWorkspaceChanges(changes));
		this.checkpoints.set(checkpoint, retained);
		const commit = new EffectCommit<ToolSettlement>();
		const inputs = new Map<string, ResourceInput>();
		const release = snapshot.dispose ?? retainWorkspaceChanges(changes);
		let disposed = false, readInputs = false, transferred: ReturnType<NonNullable<WorldBranch<ToolSettlement>["takeCommittedInputs"]>> | undefined;
		let disposal: Promise<void> | undefined, reading: ReturnType<NonNullable<WorldBranch<ToolSettlement>["takeReadInputs"]>> | undefined;
		return {
			backend, checkpoint, output: snapshot.output, validate,
			resources: Object.freeze([...new Set(changes.filter((change) => !change.validationOnly).map((change) => change.resource))]),
			capturedBytes: changes.reduce((total, change) => total + sandboxChangeBytes(change), 0),
			executionMetrics: Object.freeze({ ...snapshot.executionMetrics }),
			compatibility: Object.freeze({ status: "compatible" as const, backend, executionFingerprint }),
			commit: () => commit.run(() => {
				if (disposed) throw effectCommitFailure(new Error("workspace branch is disposed"), "recoverable");
				return this.commit(snapshot, inputs).then(output => {
				// A proven commit anchors future forks in the live workspace, independently of unadopted ancestors.
				retained.committed = true; retained.parent = undefined; if (disposed) inputs.clear(); return output;
			}); }),
			takeReadInputs: maxBytes => {
				if (reading) return Promise.resolve(undefined);
				return reading = (async () => {
				// Every file this branch read keeps its pre-image until commit; each serves reads while it stays unchanged.
				if (disposed || commit.result || readInputs) return undefined;
				readInputs = true;
				const preimages = new Map<string, ResourceInput>();
				for (const change of changes) if (change.kind !== "directory" && change.before && !change.aliases && await textual(change.before)) preimages.set(change.target, change.before);
				if (!preimages.size || disposed) return undefined;
				const inputs = await createCommittedResourceInputs(action, sourceRoot, preimages, maxBytes).catch(() => undefined);
				// Stamped only now: each file must still hold the bytes it was read with, or the owner would vouch for stale ones.
				const current = inputs && await withFilesystemReadView(undefined, view => mapFilesystem([...preimages], async ([target, before]) => {
					const content = before as ContentVersion;
					return sameContent((await readRegularState(target, undefined, view, content instanceof ChunkedContent ? content.pool : undefined))?.content, content);
				}))
					.catch(async error => { await inputs.dispose(); throw error; });
				if (inputs && !disposed && current?.every(Boolean)) return inputs;
				await inputs?.dispose(); return undefined;
				})();
			},
			takeCommittedInputs: async (maxBytes) => {
				if (disposed || !retained.committed || transferred) return undefined;
				return transferred = (async () => {
					if (!inputs.size) return undefined;
					try {
						const branch = await createCommittedResourceInputs(action, sourceRoot, inputs, maxBytes);
						if (!disposed) return branch;
						await branch.dispose(); return undefined;
					} finally { inputs.clear(); }
				})();
			},
			dispose() { disposed = true; inputs.clear();
				return disposal ??= Promise.allSettled([commit.result, reading, transferred]).then(release); },
		};
	}

	private async commit(execution: SandboxExecutionDelta, inputs?: Map<string, ResourceInput>, privateView?: FilesystemReadView): Promise<ToolSettlement> {
		this.assertOpen();
		const { changes } = execution;
		let release: (() => Promise<void>) | undefined;
		const commit = (async () => {
			const contents = await this.createContentPool().catch(error => { throw effectCommitFailure(error, "recoverable"); });
			release = contents?.retain();
			await using source = privateView ? undefined : await openFilesystemReadView().catch(error => { throw effectCommitFailure(error, "recoverable"); });
			const view = privateView ?? source!, readonlySource = !privateView && process.platform === "linux";
			return await withCommitLocks(
				changes,
				async () => {
					const staged = new Map<SandboxFileChange, string>(), descriptors = new Map<SandboxFileChange, FileHandle>();
					const baselines = new Map<SandboxWorkspaceChange, RegularFileState | SandboxDirectoryState | undefined>();
					const applied: SandboxWorkspaceChange[] = [], createdDirectories: string[] = [], parents = new Set<string>();
					const objects = new Map<string, { source: SandboxFileChange; write?: SandboxFileChange; handle?: FileHandle; mode?: number }>();
					const openDescriptors = (phase: readonly SandboxWorkspaceChange[]) => mapFilesystem(phase, async change => {
						const group = objects.get("1:" + change.target), writing = change.kind !== "directory" && !change.validationOnly && change.operation && !change.object;
						if (writing && change.after === undefined) throw new Error("A content write cannot delete a file");
						if (change.kind !== "directory" && (group || writing && change.before !== undefined)) {
							if (!(await view.stat(change.target)).isFile()) throw new Error("file object predecessor is not regular");
							const descriptor = await open(change.target, (writing || group?.write ? fsConstants.O_RDWR : fsConstants.O_RDONLY) |
								(fsConstants.O_NOFOLLOW ?? 0) | (fsConstants.O_NONBLOCK ?? 0));
							descriptors.set(change, descriptor); if (group) group.handle = descriptor;
						}
					});
					const verifyDescriptor = async (change: SandboxWorkspaceChange) => {
						const descriptor = change.kind !== "directory" && descriptors.get(change);
						if (descriptor) {
							const before = baselines.get(change) as RegularFileState | undefined, identity = await descriptor.stat({ bigint: true });
							if (!before?.identity || identity.nlink !== BigInt(change.aliases?.length ?? 1) || !sameFilesystemIdentity(before.identity, identity))
								throw new Error("file object predecessor changed: " + change.resource);
						}
					};
					let nativeStarted = false;
					try {
						// Nothing has moved yet: every target is checked at this one moment, over one walk of the directories they share.
						const capture = sharedWalk(target => captureFilesystemEntry(target, undefined, view)); await mapFilesystem(changes, change => assertCommitTarget(change, capture));

						for (const change of changes) if (change.kind !== "directory" && change.object) {
							const reference = change.object, key = `${Number(reference.before)}:${reference.path}`;
							const source = changes.find(candidate => candidate.target === reference.path);
							if (!source || source.kind === "directory" || change.after === undefined ||
								(reference.before ? source.before === undefined : source.after === undefined || source.object)) throw new Error("file object anchor is unavailable");
							const group = objects.get(key) ?? { source };
							if (reference.before) {
								if (change.operation) {
									if (group.write && (!sameContent(group.write.after, change.after) || group.write.afterMode !== change.afterMode)) throw new Error("inconsistent object contents");
									group.write = change;
								} else if (!sameContent(source.before, change.after)) throw new Error("object change lacks a write");
								if (source.beforeMode !== change.afterMode) {
									if (group.mode !== undefined && group.mode !== change.afterMode) throw new Error("inconsistent object permissions");
									group.mode = change.afterMode;
								}
							} else if (!sameContent(source.after, change.after) || source.afterMode !== change.afterMode) throw new Error("inconsistent linked contents");
							objects.set(key, group);
						}
						// Capture directories before writable file handles trigger OverlayFS copy-up and change their times.
						for (const directories of [true, false]) {
							const phase = changes.filter(change => (change.kind === "directory") === directories);
							// Ambient adapters settle copy-up or read metadata with their handles held; qualified source views need no such preparation.
							if (!directories && !readonlySource) await openDescriptors(phase);
							await mapFilesystem(phase, async change => {
								// A file whose identity still stands for the bytes this change replaces needs no rereading to prove them.
								const unchanged = change.kind !== "directory" && change.beforeIdentity && change.before
									? await view.stat(change.target).then(info => fileIdentity(info) === change.beforeIdentity ? info : undefined, () => undefined) : undefined;
								const current = unchanged ? { content: (change as SandboxFileChange).before!, mode: Number(unchanged.mode & 0o777n), identity: unchanged }
									: change.kind === "directory" ? await readSandboxDirectoryState(change.target, undefined, view) : await readRegularState(change.target, undefined, view, contents);
								baselines.set(change, current);
								if (change.kind === "directory" && change.validationOnly ? !!current !== !!change.before : !sameSandboxState(current,
									change.kind === "directory" ? change.before : change.before && { content: change.before, mode: change.beforeMode ?? 0 }))
									throw new Error(`resource changed before commit: ${change.resource}`);
								if (change.accessMode) await access(await view.realpath(change.target), change.accessMode);
								if (change.kind !== "directory" && change.aliases) await assertFileAliases(change.root, change.target, change.aliases, (current as RegularFileState | undefined)?.identity, view);
								await verifyDescriptor(change);
							});
						}
						await view.verify();
						// Read-only source proofs finish before opening writable objects; every adapter validates before staging names.
						if (readonlySource) {
							await openDescriptors(changes.filter(change => change.kind !== "directory"));
							await mapFilesystem(changes, verifyDescriptor);
						}
						await mapFilesystem(changes, async (change) => {
							if (!change.validationOnly && change.kind !== "directory" && !change.operation && !change.object && change.after !== undefined) {
								staged.set(change, await stageAtomicWrite(change.after, change.afterMode, change.root));
							}
						});
						await restoreModifiedTimes([...staged].map(([change, temporary]) => [temporary, change.afterModified]));
						// Pin every source name before any destination is removed or replaced; cycles use the same transaction.
						for (const change of changes) if (change.kind !== "directory" && change.object) {
							const reference = change.object, group = objects.get(`${Number(reference.before)}:${reference.path}`)!;
							const prior = baselines.get(change) as RegularFileState | undefined;
							const sourceState = baselines.get(group.source) as RegularFileState | undefined;
							if (reference.before && prior?.identity && sourceState?.identity && prior.identity.dev === sourceState.identity.dev && prior.identity.ino === sourceState.identity.ino) continue;
							const source = reference.before ? reference.path : staged.get(group.source)!;
							const temporary = path.join(change.root, `${SANDBOX_STAGING_FILE_PREFIX}${randomUUID()}.tmp`);
							if (reference.before) nativeStarted = true;
							await link(source, temporary); staged.set(change, temporary);
							if (reference.before) {
								const linked = await view.stat(temporary);
								if (linked.dev !== sourceState!.identity!.dev || linked.ino !== sourceState!.identity!.ino) throw new Error("file object changed while linking");
							}
						}
						for (const group of objects.values()) if (group.write || group.mode !== undefined) {
							nativeStarted = true;
							if (group.write) await writeContent(group.handle!, group.write.after!, group.source.before);
							if (group.mode !== undefined && process.platform !== "win32") await group.handle!.chmod(group.mode);
						}
						for (const change of orderWorkspaceChanges(changes.filter(change => !change.validationOnly), change => ({
							change, depth: slash(change.resource).split("/").filter(Boolean).length, key: filesystemPathKey(change.target),
						}))) {
							await assertCommitTarget(change, (target, read) => captureFilesystemEntry(target, read, view));
							if (change.kind === "directory") {
								if (!change.after) { await rmdir(change.target); applied.push(change); }
								else if (!change.before) {
									await createParentDirectories(change.root, change.target, view, createdDirectories, parents);
									await mkdir(change.target, change.operation ? undefined : { mode: change.after.mode });
									applied.push(change);
									if (!change.operation && process.platform !== "win32") await chmod(change.target, change.after.mode);
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
								if (descriptor) { applied.push(change); }
								else {
									// Exclusive creation already gives an empty file; only existing files need truncation.
									descriptor = await open(change.target, "wx", 0o666);
									descriptors.set(change, descriptor); applied.push(change);
								}
								await writeContent(descriptor, change.after!, change.before);
								continue;
							}
							applied.push(change);
							const temporary = staged.get(change);
							if (temporary) {
								await createParentDirectories(change.root, change.target, view, createdDirectories, parents);
								const mode = resolveCommitMode(baselines.get(change) as RegularFileState | undefined, change);
								try { await replaceFile(temporary, change.target, mode); }
								catch (error) {
									if (process.platform !== "win32" || !(change.object || change.aliases) || !hasErrorCode(error, "EPERM")) throw error;
									// NTFS can deny replacement of an open name while allowing its unlink. Retained handles stay on the old object.
									nativeStarted = true; await unlink(change.target); await replaceFile(temporary, change.target, mode);
								}
								staged.delete(change);
							} else {
								await rm(change.target, { force: true });
							}
						}
						await restoreModifiedTimes(changes.flatMap(change => change.kind !== "directory" && change.operation ? [[change.target, change.afterModified] as const] : []));
						let directoryBytes = 0;
						const createdEntries = new Map((inputs ? changes : []).filter(change => change.kind === "directory" && change.operation && change.after)
							.map(change => [change.target, [] as string[]]));
						if (inputs) for (const change of changes) if (!change.validationOnly && change.after)
							createdEntries.get(path.dirname(change.target))?.push(path.basename(change.target));
						// Directories are read at once; what their listings retain is decided in order.
						const listed = await mapFilesystem(changes.filter((change): change is SandboxDirectoryChange => !change.validationOnly && change.kind === "directory" && !!change.after),
							async change => ({ change, names: await verifySandboxDirectoryAfter(change, view) }));
						for (const { change, names: observed } of listed) {
							if (!inputs) continue;
							const names = observed ?? createdEntries.get(change.target);
							const bytes = names?.reduce((sum, name) => sum + name.length * 2 + 16, 0) ?? 0, retain = directoryBytes + bytes <= WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES;
							if (retain) directoryBytes += bytes;
							inputs.set(change.target, { names: retain ? names : undefined });
						}
						try { await view.verify(); } catch (error) { throw effectCommitFailure(error, nativeStarted || applied.length ? "poisoned" : "recoverable"); }
					} catch (error) {
						inputs?.clear();
						if (nativeStarted || isPoisonedEffectCommit(error) || applied.some((change) => change.kind !== "directory" && (change.operation || change.aliases))) {
							throw effectCommitFailure(error, "poisoned", "native file write began; its effects cannot be safely replayed or rolled back");
						}
						try {
							await restoreChanges(applied, baselines, view, contents);
							for (const directory of [...new Set(createdDirectories)].sort((left, right) => right.length - left.length)) {
								try { await rmdir(directory); } catch (error) { if (!isMissing(error)) throw error; }
							}
						} catch (rollbackError) {
							throw effectCommitFailure(
								new AggregateError([error, rollbackError], "sandbox commit and rollback both failed", { cause: error }),
								"poisoned",
								"sandbox commit failed and the original workspace could not be fully restored",
							);
						}
						throw effectCommitFailure(error, "recoverable");
					} finally {
						const closed = await Promise.allSettled([...descriptors.values()].map((descriptor) => descriptor.close()));
						await Promise.all([...staged.values()].map((temporary) => rm(temporary, { force: true }).catch(() => undefined)));
						const failure = closed.find((result) => result.status === "rejected");
						if (failure) throw effectCommitFailure(failure.reason, "poisoned", "native file descriptor cleanup failed; completion is unknown");
					}
					if (inputs) for (const change of changes) if (!change.validationOnly && (change.kind === "directory" ? !change.after : await textual(change.after)))
						inputs.set(change.target, change.kind === "directory" ? null : change.after ?? null);
					return execution.output;
				},
				view,
			);
		})().catch(error => { throw effectCommitFailure(error, "poisoned"); }).finally(() => release?.());
		return this.lifetime.track(commit);
	}

	async fork(options: SandboxWorkspaceBranchOptions, preparation: SandboxPreparation | undefined = options.validate ? options.preparation : undefined): Promise<WorldBranch<ToolSettlement>> {
		this.assertOpen();
		const sourceRoot = path.resolve(options.cwd), parent = this.checkpoint(options.parentCheckpoint, sourceRoot);
		return this.createBranch(await this.capture(options, parent, preparation), sourceRoot, options.action, parent, options.validate);
	}

	async capture(options: Omit<SandboxWorkspaceBranchOptions, "action" | "parentCheckpoint">, parent?: WorkspaceCheckpoint, preparation: SandboxPreparation | undefined = options.validate ? options.preparation : undefined): Promise<WorkspaceExecutionSnapshot> {
		this.assertOpen();
		const sourceRoot = path.resolve(options.cwd);
		const ancestors: SandboxWorkspaceChange[] = [];
		for (let checkpoint = parent; checkpoint; checkpoint = checkpoint.parent) ancestors.push(...checkpoint.changes);
		const releaseParent = retainWorkspaceChanges(ancestors);
		let capturedSnapshot: WorkspaceExecutionSnapshot | undefined;
		try {
			const resolvedDriver = await this.resolveDriver(options.driver === "auto" || options.driver === undefined ? { ...options, driver: "git" } : options);
			return await this.withPrivateWorkspace(sourceRoot, options.gitBinary ?? "git", resolvedDriver.driver, options,
				async (workspace) => {
					const before = parent ? await captureSandboxStructure(workspace, true) : workspace.initialStructure;
					const result = await options.execute(workspace);
					const captured = "output" in result ? result : { output: result, changes: await captureWorkspaceChanges(workspace, before) };
					const changes = ownSandboxChanges((await options.afterCapture?.(workspace, captured)) ?? captured.changes);
					if (workspace.recycle && "output" in result) { workspace.recycle.written.push(...writtenPaths(changes)); workspace.recycle.settled = true; }
					return capturedSnapshot = { output: captured.output, changes, executionMetrics: { ...options.executionMetrics?.() }, dispose: retainWorkspaceChanges([...changes, ...ancestors]) };
				},
				parent, preparation);
		} catch (error) { await capturedSnapshot?.dispose?.(); throw error; }
		finally { await releaseParent(); }
	}

	async prepare(cwd: string, options: PrepareSandboxWorkspaceOptions = {}): Promise<QualifiedWorkspaceSandboxDriver> {
		this.assertOpen();
		throwIfAborted(options.signal);
		const sourceRoot = path.resolve(cwd);
		await withFilesystemReadView(undefined, view => assertNoSymlinkPath(sourceRoot, sourceRoot, (target, read) => captureFilesystemEntry(target, read, view)));
		throwIfAborted(options.signal);
		const repository = await this.acquireRepository(sourceRoot, options.gitBinary ?? "git");
		try {
			throwIfAborted(options.signal);
			const resolved = await this.resolveDriver(options.driver === "overlayfs" ? options : { ...options, driver: "git" }, sourceRoot, repository);
			throwIfAborted(options.signal);
			const live = resolved.driver === "overlayfs" && options.liveLower && await captureLiveBase(repository);
			const baseline = live ? undefined : await acquireSandboxBaseline(repository, true);
			throwIfAborted(options.signal);
			if (resolved.driver !== "overlayfs") await prepareNextSandbox(repository, baseline!);
			else if (baseline) {
				const overlay = await acquireOverlayBaseline(repository, baseline);
				try { throwIfAborted(options.signal); await (overlay.structure ??= captureOwnedWorkspaceStructure(overlay.sandboxRoot)); } finally { releaseOverlayBaseline(overlay); }
			}
			throwIfAborted(options.signal);
			const prepared = Object.freeze({ ...resolved });
			if (baseline) preparedWorkspaceBaselines.set(prepared, { repository, baseline });
			return prepared;
		} finally {
			this.releaseRepository(repository);
		}
	}

	private async executeMutation(context: SpeculativeToolExecutionContext, options: WorkspaceSandboxOptions): Promise<WorldBranch<ToolSettlement>> {
		const execute = (context.action.executionContext as ToolInvocation | undefined)?.filesystem;
		if (!execute) throw new Error("Workspace execution requires an explicitly bound filesystem operation");
		const sourceRoot = path.resolve(context.cwd);
		// Root mutations keep writes in memory; hard links and chained actions need a private workspace.
		if (!context.parentCheckpoint && options.inPlaceMutations !== false) try {
			return this.createBranch(await this.executeFilesystemMutation(context, execute), sourceRoot, context.action);
		} catch (error) { if (error !== LINKED_INPUT) throw error; }
		return this.fork({
			cwd: sourceRoot, action: context.action, parentCheckpoint: context.parentCheckpoint, ...options,
			execute: workspace => this.executeFilesystemMutation(context, execute, workspace),
		}, capturedWorkspaceInputs);
	}

	/** Both substrates capture the same operation delta; only a private workspace performs speculative writes. */
	private async executeFilesystemMutation(context: SpeculativeToolExecutionContext, execute: NonNullable<ToolInvocation["filesystem"]>, workspace?: SandboxWorkspaceContext): Promise<WorkspaceExecutionSnapshot> {
		const contents = workspace?.contents ?? await this.createContentPool(), release = contents?.retain();
		return withFilesystemReadView(undefined, async source => {
			const sourceRoot = path.resolve(context.cwd);
			await using privateReads = workspace ? FilesystemReadView.ambient() : undefined;
			const view = privateReads ?? source;
			const changes = new Map<string, SandboxWorkspaceChange>(), lifetime = new RuntimeLifecycleLane();
			let bytes = 0, failure: { error: unknown } | undefined, namespace: Promise<WorkspaceStructureSnapshot> | undefined;
			// A request the tool swallowed still fails the run; requests after it returns are refused.
			const track = <T>(run: () => Promise<T>): Promise<T> => lifetime.admit(async () => {
				try { context.signal.throwIfAborted(); return await run(); } catch (error) { failure ??= { error }; throw error; }
			});
			const record = (key: string, change: SandboxWorkspaceChange) => {
				const retained = bytes - sandboxChangeBytes(changes.get(key)) + sandboxChangeBytes(change);
				if (retained > WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES || (!changes.has(key) && changes.size >= WORKSPACE_TRANSACTION_MAX_FILES))
					throw new Error("Workspace operation exceeds its capture budget");
				changes.set(key, change); bytes = retained;
			};
			const located = async (logical: string) => {
				const relative = relativeFilesystemPath(sourceRoot, logical);
				if (relative === undefined || isSnapshotExcluded(slash(relative))) throw new Error("Filesystem operation is outside the workspace view");
				const target = path.resolve(sourceRoot, relative), file = path.resolve(workspace?.sandboxRoot ?? sourceRoot, relative);
				await assertNoSymlinkPath(sourceRoot, target, (target, read) => captureFilesystemEntry(target, read, source));
				if (workspace) await assertNoSymlinkPath(workspace.sandboxRoot, file, (target, read) => captureFilesystemEntry(target, read, view));
				context.signal.throwIfAborted();
				return { file, key: filesystemPathKey(target), root: sourceRoot, target, resource: slash(relative) };
			};
			const fileInput = async (logical: string) => {
				const { file, key, ...identity } = await located(logical), previous = changes.get(key);
				if (previous?.kind === "directory") throw new Error("Workspace file input changed type");
				// Reuse the preimage until a private write requires reading the new physical state.
				const before = previous && (!workspace || previous.validationOnly)
					? previous.before === undefined ? undefined : { content: previous.before, mode: previous.beforeMode! }
					: await readRegularState(file, undefined, view, contents);
				let aliases: readonly string[] | undefined;
				if (!previous && before && "identity" in before && before.identity && before.identity.nlink > 1n) {
					if (!workspace) throw LINKED_INPUT;
					const snapshot = await (namespace ??= workspace.structure.capture()), entry = snapshot.entries.get(path.relative(workspace.sandboxRoot, file));
					if (!snapshot.complete || entry?.kind !== "file" || entry.aliases?.length !== Number(before.identity.nlink))
						throw new Error("workspace file alias namespace is not closed");
					aliases = entry.aliases.map(name => path.resolve(sourceRoot, path.relative(workspace.sandboxRoot, name)));
				}
				const captured: SandboxFileChange = previous ?? { ...identity, validationOnly: true, before: before?.content, beforeMode: before?.mode, ...(aliases ? { aliases } : {}) };
				record(key, captured);
				return { file, key, captured, content: !workspace && !captured.validationOnly ? captured.after : before?.content };
			};
			const directoryInput = async (logical: string) => {
				const { file, key, ...identity } = await located(logical), previous = changes.get(key);
				if (previous && previous.kind !== "directory") throw new Error("Workspace directory input changed type");
				const before = previous && !workspace ? previous.before ?? previous.after : await readSandboxDirectoryState(file, undefined, view);
				// An in-memory mkdir needs only existence; a private mkdir captures its actual final directory state below.
				if (!previous) record(key, { ...identity, kind: "directory", before, ...(before ? { validationOnly: true } : {
					operation: "mkdir", ...(!workspace ? { after: { entriesDigest: directoryEntriesDigest([]), mode: 0o755, uid: 0, gid: 0 } } : {}) }) });
				return { file, key, before };
			};
			const input = async (logical: string) => {
				const { file, key } = await located(logical), known = changes.get(key);
				const directory = !workspace && known ? known.kind === "directory"
					: (await view.stat(file).catch(error => { if (!isMissing(error)) throw error; }))?.isDirectory();
				const exists = directory ? (await directoryInput(logical)).before !== undefined : (await fileInput(logical)).content !== undefined;
				return { file, key, exists };
			};
			let output: ToolSettlement;
			try {
				output = await execute({
					readFile: (logical, limit) => track(async () => {
						const { captured, content } = await fileInput(logical);
						if (content === undefined) throw new Error("Workspace input does not exist");
						assertExistingInputPolicy(captured);
						return readContent(content, 0, Math.min(content.byteLength, limit ?? content.byteLength));
					}),
					access: (logical, writable) => track(async () => {
						const { file, key } = await input(logical), mode = fsConstants.R_OK | (writable ? fsConstants.W_OK : 0);
						const captured = changes.get(key)!;
						assertExistingInputPolicy(captured);
						await access(await view.realpath(file), mode);
						if (captured.before !== undefined) record(key, { ...captured, accessMode: (captured.accessMode ?? 0) | mode });
					}),
					exists: logical => track(async () => (await input(logical)).exists),
					writeFile: (logical, content) => track(async () => {
						const { file, key, captured } = await fileInput(logical);
						// Even an equal-content native write performs permission checks and touches the inode.
						record(key, { ...captured, accessMode: changes.get(key)?.accessMode, validationOnly: undefined, after: contents ? await captureContent(contents, content) : Buffer.from(content), operation: "write_contents" });
						if (workspace) await writeFile(file, content, "utf8");
					}),
					mkdir: (logical) => track(async () => {
						for (let directory = path.resolve(logical); ; directory = path.dirname(directory)) {
							if ((await directoryInput(directory)).before || directory === sourceRoot) break;
						}
						if (workspace) await mkdir((await located(logical)).file, { recursive: true });
					}),
				}, context);
			} finally { await lifetime.close(() => {}); if (failure) throw failure.error; context.signal.throwIfAborted(); }
			if (workspace) for (const [key, change] of changes) {
				if (change.kind === "directory" && !change.validationOnly) record(key, { ...change, after: await readSandboxDirectoryState((await located(change.target)).file, undefined, view) });
				else if (change.kind !== "directory" && !change.validationOnly &&
					!sameContent((await readRegularState((await located(change.target)).file, undefined, view, contents))?.content, change.after)) {
					throw new Error("Workspace writes did not settle to their declared bytes");
				}
			}
			const captured = [...changes.values()];
			return { output, changes: workspace ? captured : ownSandboxChanges(captured), executionMetrics: {}, dispose: workspace ? undefined : retainWorkspaceChanges(captured) };
		}).finally(release);
	}

	private async createPrivateSandboxWorkspace(cwd: string, gitBinary: string, driver: Exclude<WorkspaceSandboxDriver, "auto">, overlayOptions: WorkspaceSandboxOptions, preparation?: SandboxPreparation) {
		const sourceRoot = path.resolve(cwd);
		await withFilesystemReadView(undefined, view => assertNoSymlinkPath(sourceRoot, sourceRoot, (target, read) => captureFilesystemEntry(target, read, view)));
		const pool = await this.acquireRepository(sourceRoot, gitBinary);
		let contents: ContentChunkPool | undefined, releaseContents: (() => Promise<void>) | undefined;
		let attached: PreparedGitWorkspace | undefined;
		let sharedBaseline: SharedOverlayBaseline | undefined;
		let overlay: LinuxOverlayfsMount | undefined;
		let overlayStorageRoot: string | undefined, cursor: ResourceVersionToken | undefined;
		let metadata: Awaited<ReturnType<typeof preserveSnapshotMetadata>> | undefined;
		// Baselines and checkpoints share one owner; Linux O_PATH also retains unreadable objects without reading them.
		const pins: Awaited<ReturnType<typeof pinFilesystemObject>>[] = [];
		const pinObject = async (target: string) => {
			const lease = await pinFilesystemObject(target);
			pins.push(lease); return lease.stat;
		};
		let workspace!: PrivateSandboxWorkspace;
		let disposal: Promise<void> | undefined;
		const dispose = (unsafe = false): Promise<void> => disposal ??= (async () => {
			const failures: unknown[] = [];
			await workspace?.transactions.dispose().catch((error) => failures.push(error));
			await releaseContents?.().catch(error => failures.push(error));
			if (workspace) workspaceOwners.delete(workspace.sandboxRoot);
			const closed = await Promise.allSettled(pins.splice(0).map(handle => handle.close()));
			failures.push(...closed.flatMap(result => result.status === "rejected" ? [result.reason] : []));
			await overlay?.close().catch((error) => { unsafe = true; failures.push(error); });
			if (unsafe) {
				// A live mount retains upper/work/lower storage; quarantine it rather than recycling its roots.
				this.quarantineRepository(pool);
			} else {
				if (attached && workspace?.recycle?.settled) {
					await retireSandboxWorkspace(pool, attached, workspace.recycle.written).catch((error) => failures.push(error));
					if (pool.baseline) void prepareNextSandbox(pool, pool.baseline).catch(() => {});
				} else await attached?.dispose().catch((error) => failures.push(error));
				if (overlayStorageRoot) await removeOwnedTree(overlayStorageRoot).catch((error) => failures.push(error));
				if (sharedBaseline) releaseOverlayBaseline(sharedBaseline);
				cursor?.release();
				this.releaseRepository(pool);
			}
			if (failures.length) throw new AggregateError(failures, "sandbox workspace cleanup failed");
		})();
		try {
			contents = await this.createContentPool(); releaseContents = contents?.retain();
			const prepared = typeof preparation === "object" ? preparedWorkspaceBaselines.get(preparation) : undefined;
			// A live lower is the workspace itself: what changed since now stands in for a baseline of its bytes.
			cursor = driver === "overlayfs" && overlayOptions.liveLower ? await pool.versions.observeChanges() : undefined;
			const liveBase = cursor && await captureLiveBase(pool);
			const baseline = liveBase ? { commit: "", tree: "", version: cursor!, aliases: [] } : prepared?.repository === pool && typeof preparation === "object" &&
				preparation.driver === driver ? prepared.baseline : await acquireSandboxBaseline(pool, preparation === capturedWorkspaceInputs);
			const { commit } = baseline;
			let sandboxRoot: string, processRoot: string, transactionClockPath: string;
			let transactionClockLinks: 0 | 1, transactionClockRoots: readonly string[], overlayDevice: string | undefined;
			if (driver === "overlayfs") {
				// Only a snapshot lower needs the baseline checked out.
				if (!liveBase) sharedBaseline = await acquireOverlayBaseline(pool, baseline);
				overlayStorageRoot = await mkdtemp(path.join(pool.parent, "overlay-storage-"));
				processRoot = path.join(overlayStorageRoot, "process");
				await mkdir(processRoot);
				const lowerRoot = liveBase ? sourceRoot : sharedBaseline!.sandboxRoot, upper = path.join(overlayStorageRoot, "upper");
				const mounted = overlay = await mountLinuxOverlayfs({ lowerRoot,
					privateRoot: overlayStorageRoot, options: overlayOptions, capabilityRegistry: this.overlayfsCapabilities });
				sandboxRoot = overlay.root;
				metadata = await preserveSnapshotMetadata(lowerRoot, liveBase ? sandboxRoot : upper, processRoot, !!liveBase,
					await captureSnapshotMetadata(lowerRoot, upper), pinObject);
				overlayDevice = String((await lstat(overlay.root, { bigint: true })).dev);
				transactionClockPath = mounted.upperRoot; transactionClockLinks = 0;
				// A live lower may sit on another filesystem; its files' own identities, checked on every read, fence it instead.
				transactionClockRoots = Object.freeze([...liveBase ? [] : [sharedBaseline!.sandboxRoot], mounted.upperRoot, mounted.workRoot]);
			} else {
				const prepared = (await takePreparedSandbox(pool, baseline)) ?? (await attachSandboxWorkspace(pool, baseline));
				// The next speculated action's checkout (the whole tree, ignored dependencies included) runs while this one does;
				// a filesystem-tool worktree is itself reset for the next one when it returns.
				if ((overlayOptions as Partial<SandboxWorkspaceBranchOptions>).action && preparation !== capturedWorkspaceInputs) void prepareNextSandbox(pool, baseline).catch(() => {});
				attached = prepared;
				({ sandboxRoot, processRoot } = prepared);
				metadata = await preserveSnapshotMetadata(sourceRoot, sandboxRoot, processRoot, process.platform === "linux" && !!overlayOptions.liveLower, undefined, pinObject);
				transactionClockPath = path.join(prepared.processRoot, "workspace-transaction.clock");
				transactionClockLinks = 1;
				transactionClockRoots = Object.freeze([prepared.sandboxRoot, prepared.processRoot]);
			}
			const sourceChanges = () => pool.versions.changesSince(baseline.version);
			const storage = { sandboxRoot, overlay: overlay && { upperRoot: overlay.upperRoot, device: overlayDevice!, baseline: liveBase || await (sharedBaseline!.structure ??= captureOwnedWorkspaceStructure(sharedBaseline!.sandboxRoot)) },
				clock: { path: transactionClockPath, links: transactionClockLinks, roots: transactionClockRoots } };
			const data: SandboxWorkspaceObservation = {
				...storage, initialStructure: await captureSandboxStructure(storage, true), id: randomUUID(), sourceRoot, processRoot, gitBinary, repository: path.join(pool.parent, "snapshot.git"), commit,
				baselineFrontier: new Map(), sourceDevice: (await withFilesystemReadView(undefined, view => view.stat(sourceRoot))).dev,
				metadata, modes: sharedBaseline?.modes,
				liveBase: liveBase || undefined,
				hints: sourceChanges(),
			};
			const observation = createWorkspaceObservation(data, sourceChanges, contents);
			const remote = process.platform === "linux" && !ownsNativeProcesses ? ownWorkspaceObservation(() => ({ ...data, hints: sourceChanges() }), contents) : undefined;
			const allocated = {
				...observation, transactions: remote?.transactions ?? observation.transactions, pool, dispose, pinObject,
				// Bound filesystem writes record their paths so the pool can reset this worktree.
				...(attached && preparation === capturedWorkspaceInputs ? { recycle: { written: [] as string[], settled: false } } : {}),
			};
			workspace = allocated;
			if (remote) workspaceOwners.set(sandboxRoot, { capability: workspaceCapability(allocated), observation: remote });
			return allocated;
		} catch (error) {
			try {
				await dispose(error instanceof LinuxOverlayfsUnsafeCleanupError);
			} catch (cleanupError) {
				throw new AggregateError([error, cleanupError], "sandbox creation and cleanup both failed");
			}
			throw error;
		}
	}

	private async acquireRepository(sourceRoot: string, gitBinary: string): Promise<PooledGitRepository> {
		this.assertOpen();
		const key = `${filesystemPathKey(sourceRoot)}\0${gitBinary}`;
		let pending = this.repositories.get(key);
		if (!pending) {
			pending = createSandboxRepository(sourceRoot, gitBinary);
			this.repositories.set(key, pending);
			void pending.catch(() => { if (this.repositories.get(key) === pending) this.repositories.delete(key); });
		}
		const repository = await pending;
		repository.registration ??= pending;
		if (repository.quarantined) {
			if (this.repositories.get(key) === pending) this.repositories.delete(key);
			return this.acquireRepository(sourceRoot, gitBinary);
		}
		if (repository.idleTimer) { clearTimeout(repository.idleTimer); repository.idleTimer = undefined; }
		repository.active++;
		return repository;
	}

	private assertOpen(): void {
		if (this.lifetime.sealed) throw new Error("Workspace sandbox service is disposed");
	}

	private async withPrivateWorkspace<T>(cwd: string, gitBinary: string, driver: Exclude<WorkspaceSandboxDriver, "auto">, overlayOptions: WorkspaceSandboxOptions, run: (workspace: PrivateSandboxWorkspace) => Promise<T>, checkpoint?: WorkspaceCheckpoint, preparation?: SandboxPreparation): Promise<T> {
		const workspace = await this.createPrivateSandboxWorkspace(cwd, gitBinary, driver, overlayOptions, preparation);
		try { if (checkpoint) await this.materializeCheckpoint(workspace, checkpoint); return await run(workspace); } finally { await workspace.dispose(); }
	}

	private releaseRepository(repository: PooledGitRepository): void {
		repository.active = Math.max(0, repository.active - 1);
		if (repository.active === 0) { for (const resolve of repository.idleWaiters) resolve(); repository.idleWaiters.clear(); }
		if (repository.quarantined || repository.lock.sealed || repository.active > 0 || repository.idleTimer) return;
		repository.idleTimer = setTimeout(() => {
			// A sealed service owns the final pool sweep; keep its registration until that sweep.
			if (repository.active > 0 || this.lifetime.sealed) return;
			const key = `${filesystemPathKey(repository.sourceRoot)}\0${repository.gitBinary}`;
			if (this.repositories.get(key) === repository.registration) this.repositories.delete(key);
			// Detach this generation now, but keep its asynchronous retirement inside the service lifecycle.
			void this.lifetime.run(() => closeSandboxRepository(repository)).catch(() => undefined);
		}, SANDBOX_REPOSITORY_IDLE_MS);
		repository.idleTimer.unref?.();
	}

	private quarantineRepository(repository: PooledGitRepository): void {
		repository.quarantined = true;
		const key = `${filesystemPathKey(repository.sourceRoot)}\0${repository.gitBinary}`;
		if (this.repositories.get(key) === repository.registration) this.repositories.delete(key);
		if (repository.idleTimer) { clearTimeout(repository.idleTimer); repository.idleTimer = undefined; }
		repository.baseline?.version.release();
		repository.baseline = undefined;
		repository.versions.close();
		this.releaseRepository(repository);
	}

	private async closePools(roots?: readonly string[]): Promise<void> {
		const rootKeys = roots ? new Set(roots.map(filesystemPathKey)) : undefined;
		const pending = [...this.repositories.entries()].filter(([key]) => {
			if (!rootKeys) return true;
			const separator = key.indexOf("\0");
			return rootKeys.has(separator === -1 ? key : key.slice(0, separator));
		});
		const closed = await Promise.allSettled(pending.map(async ([key, item]) => {
			if (this.repositories.get(key) === item) this.repositories.delete(key);
			const repository = await item.catch(() => undefined);
			if (repository) await closeSandboxRepository(repository);
		}));
		const failure = closed.find(result => result.status === "rejected");
		if (failure) throw failure.reason;
	}

	private async materializeCheckpoint(workspace: PrivateSandboxWorkspace, checkpoint: WorkspaceCheckpoint): Promise<void> {
		await using view = FilesystemReadView.ambient();
		const lineage: WorkspaceCheckpoint[] = [];
		for (let current: WorkspaceCheckpoint | undefined = checkpoint; current; current = current.parent) { lineage.push(current); }
		for (const ancestor of lineage.reverse()) {
			const changes = ownSandboxChanges(ancestor.changes, name => {
				const relative = relativeFilesystemPath(ancestor.sourceRoot, name);
				if (relative === undefined) throw new Error("checkpoint object escapes workspace");
				return path.resolve(workspace.sandboxRoot, relative);
			});
			// Allocation may precede commit: prove the resulting bytes, namespace and times before skipping an adopted root.
			if (ancestor.committed && (await mapFilesystem(changes.filter(change => !change.validationOnly), async change => {
				await assertCommitTarget(change);
				if (change.kind === "directory") { await verifySandboxDirectoryAfter(change); return true; }
				const current = await readRegularState(change.target, undefined, view, workspace.contents);
				if (change.after !== undefined) await assertFileAliases(change.root, change.target,
					change.afterAliases ?? (change.operation ? change.aliases : undefined) ?? [change.target], current?.identity);
				return sameSandboxState(current, change.after === undefined ? undefined : { content: change.after, mode: change.afterMode ?? change.beforeMode ?? 0 }) &&
					(process.platform !== "linux" || change.afterModified === undefined || String(current?.identity?.mtimeNs) === change.afterModified);
			}).catch(() => [false])).every(Boolean)) continue;
			workspace.recycle?.written.push(...writtenPaths(changes));
			await this.commit({ output: { result: { content: [], details: {} }, isError: false }, changes }, undefined, view);
			for (const change of changes) if (!change.validationOnly && change.kind !== "directory") {
				if (change.after !== undefined) await workspace.pinObject(change.target);
				workspace.baselineFrontier.set(path.normalize(change.resource), await readRegularState(change.target, undefined, view, workspace.contents));
			}
		}
	}
}

function retainWorkspaceChanges(changes: readonly SandboxWorkspaceChange[]): () => Promise<void> {
	const pools = new Set(changes.flatMap(change => change.kind === "directory" ? [] : [change.before, change.after])
		.filter((value): value is ChunkedContent => value instanceof ChunkedContent).map(value => value.pool));
	const releases = [...pools].map(pool => pool.retain());
	return async () => { await Promise.all(releases.map(release => release())); };
}

async function captureWorkspaceChanges(workspace: WorkspaceObservation, before: WorkspaceStructureSnapshot): Promise<readonly SandboxWorkspaceChange[]> {
	const after = await workspace.structure.capture(), transitions = regularStructureTransitions(before, after);
	const deltas = (await collectSandboxChanges(workspace, transitions)).map(({ resource: relativePath, ...change }) => ({ ...change, relativePath }));
	const effects = diffWorkspaceStructures(before, after, deltas, new ExecutionPathProjection({ sourceRoot: workspace.sourceRoot, workspaceRoot: workspace.sandboxRoot }));
	if (!sameWorkspaceChangeSnapshot(after, await workspace.structure.capture())) throw new Error("workspace changed while capturing effects");
	return sandboxWorkspaceChanges(workspace.sourceRoot, effects);
}

/** Both raw operations and native traces commit the same proven object transitions. */
export function sandboxWorkspaceChanges(root: string, effects: WorkspaceTransactionDiff): SandboxWorkspaceChange[] {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	const directory = ({ entriesDigest, mode, uid, gid }: SandboxDirectoryState) => ({ entriesDigest, mode, uid, gid });
	return effects.effects.map(({ logicalPath: target, relativePath, change }): SandboxWorkspaceChange => {
		const address = { root, target, resource: slash(relativePath) };
		return change.kind !== "directory" ? { ...change, ...address } : { kind: "directory", ...address,
			before: change.before && directory(change.before), after: change.after && directory(change.after) };
	});
}

const LINKED_INPUT = new Error("linked workspace input");

/** Git owns bytes and links, while an OS snapshot also owns empty directories and object metadata.
 * Pin every translated object until disposal: unlink/recreate must never reuse a saved inode mapping. */
async function captureSnapshotMetadata(sourceRoot: string, copiedRoot = sourceRoot) {
	return withFilesystemReadView(undefined, async view => {
		const original = new Map<string, BigIntStats>();
		if (!(await captureWorkspaceStructure(copiedRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: SNAPSHOT_EXCLUDES, view,
			observeStat: async (resource, stat) => { original.set(resource, copiedRoot === sourceRoot ? stat : await view.stat(path.join(sourceRoot, resource))); }
		})).complete) throw new Error("snapshot metadata namespace is incomplete");
		return original;
	});
}
async function preserveSnapshotMetadata(sourceRoot: string, sandboxRoot: string, processRoot: string, native: boolean,
	captured?: ReadonlyMap<string, BigIntStats>, pin?: (target: string) => Promise<BigIntStats>) {
	const original = captured ?? await captureSnapshotMetadata(sourceRoot), names = [...original.keys()].sort((a, b) => a.length - b.length);
	const mounts = native ? await readFile("/proc/self/mountinfo", "utf8") : undefined;
	const policies = mounts ? mounts.trim().split("\n").map(line => {
		const fields = line.split(" ");
		return { root: fields[4]!.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(Number.parseInt(octal, 8))), flags: fields[5]!.split(",") };
	}).sort((a, b) => b.root.length - a.root.length) : [];
	const atimePolicy = (target: string, directory: boolean) => {
		const flags = policies.find(mount => containsFilesystemPath(mount.root, target))?.flags ?? [];
		return flags.includes("noatime") || flags.includes("ro") || directory && flags.includes("nodiratime") ? 1 : flags.includes("relatime") ? 2 : 0;
	};
	// Check each parent before populating its children: a stale checkout may contain a symlink in its place.
	for (const resource of names) if (original.get(resource)!.isDirectory()) {
		const target = path.join(sandboxRoot, resource), current = await lstat(target).catch(error => { if (isMissing(error)) return undefined; throw error; });
		if (!current) await mkdir(target);
		else if (!current.isDirectory()) throw new Error(`snapshot directory type changed: ${resource}`);
	}
	const translated: [string, BigIntStats][] = [], rows: [BigIntStats, BigIntStats, number][] = [], objects = new Map<string, string>();
	// Set parent modes last so a read-only directory cannot prevent its own snapshot from being populated.
	for (const resource of names.reverse()) {
		const source = original.get(resource)!, target = path.join(sandboxRoot, resource);
		const current = await lstat(target, { bigint: true }).catch(error => { if (isMissing(error)) return undefined; throw error; });
		if (!current) continue; // A warmup can predate this name; exact input validation still decides adoption.
		if (objects.has(`${current.dev}:${current.ino}`)) continue;
		if ((source.mode & 0o170000n) !== (current.mode & 0o170000n)) throw new Error(`snapshot object type changed: ${resource}`);
		if (source.uid !== current.uid || source.gid !== current.gid) throw new Error(`snapshot object ownership differs: ${resource}`);
		if (!source.isSymbolicLink()) await chmod(target, Number(source.mode & 0o7777n));
		if (process.platform === "linux" || !source.isSymbolicLink()) await (source.isSymbolicLink() ? lutimes : utimes)(target, source.atime, source.mtime);
		objects.set(`${current.dev}:${current.ino}`, `${source.dev}:${source.ino}`);
		translated.push([target, source]);
	}
	await restoreModifiedTimes(translated.map(([target, source]) => [target, String(source.mtimeNs)]));
	for (const [target, source] of translated) {
		if (!native && (!pin || !source.isFile())) continue;
		const physical = await pin!(target);
		if (native) rows.push([physical, source, atimePolicy(path.join(sourceRoot, path.relative(sandboxRoot, target)), source.isDirectory())]);
	}
	const image = native ? path.join(processRoot, "metadata.json") : undefined;
	if (image) {
		rows.push([await pin!(path.dirname(sandboxRoot)), await withFilesystemReadView(undefined, view => view.stat(path.dirname(sourceRoot))), atimePolicy(path.dirname(sourceRoot), true)]);
		await writeFile(image, JSON.stringify(rows.map(([physical, source, policy]) => [
			...[physical, source].map(stat => SNAPSHOT_METADATA_FIELDS.map(field => String(stat[field]))), policy])), { flag: "wx", mode: 0o600 });
	}
	const mappings = new Map(rows.map(row => [`${row[0].dev}:${row[0].ino}`, row]));
	return { image, mounts, objects, mappings, modes: new Map([...original].map(([name, stat]) => [slash(name), process.platform === "win32" ? 0 : Number(stat.mode & 0o777n)])) };
}

async function createGitWorkspaceTransactionDriver(workspace: ReturnType<typeof createWorkspaceObservation>): Promise<WorkspaceTransactionDriver> {
	// An interval that overlapped another keeps its own before-state: the frontier it began with, what was unreadable then,
	// and what changed while its start was being fenced. Its writer's trace later names which changes are its own.
	interface Capture {
		contaminated: boolean; overlapped: boolean; readonly before?: WorkspaceStructureSnapshot;
		readonly frontier: ReadonlyMap<string, RegularFileState | undefined>; readonly unknown: ReadonlySet<string>; readonly racing: readonly string[];
	}
	const { sandboxRoot } = workspace;
	// Discard the entire unaccepted snapshot if a name disappears or an observed object changes during capture.
	const captureStructure = async (attempt = 1): Promise<WorkspaceStructureSnapshot> => {
		const last = attempt >= WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS, snapshot = await workspace.structure.capture().catch((error: unknown) => { if (last || !(isMissing(error) || error instanceof FilesystemCaptureChangedError)) throw error; });
		return snapshot && (snapshot.complete || last) ? snapshot : captureStructure(attempt + 1);
	}, frontier = new Map(workspace.baselineFrontier);
	let lastStructure = workspace.initialStructure;
	let retainedBytes = [...frontier.values()].reduce((total, state) => total + contentMemory(state?.content), 0);
	const active = new Set<Capture>(), lock = new RuntimeLifecycleLane(), unknown = new Set<string>(); // Bytes last seen while others wrote.
	let poisonReason = lastStructure.complete ? undefined : "workspace_structure_limit";
	let clock: Awaited<ReturnType<typeof openWorkspaceChangeClock>> | undefined;

	if (!poisonReason) {
		try {
			clock = await openWorkspaceChangeClock(workspace);
			await captureBefore();
		} catch (error) {
			poisonReason = `workspace_transaction_clock:${errorMessage(error)}`;
		}
	}

	const begin = (): Promise<WorkspaceTransactionCapture> =>
		lock.serialize(async () => {
			const overlapped = active.size > 0;
			for (const other of active) other.overlapped = true;
			const start = poisonReason ? undefined : await captureBefore(overlapped).catch(error => {
				poisonReason = `workspace_transaction_sync:${errorMessage(error)}`;
				return undefined;
			});
			const capture: Capture = { contaminated: false, overlapped, before: start?.before, frontier: new Map(frontier), unknown: new Set(unknown), racing: start?.racing ?? [] };
			active.add(capture);
			return {
				readBefore: (resource, maxBytes) => lock.serialize(async () => {
					if (!active.has(capture) || capture.contaminated || !capture.before || capture.unknown.has(resource)) throw new Error("workspace transaction input is unavailable");
					if (!isSafeCount(maxBytes)) throw new Error("workspace transaction input budget is invalid");
					if (capture.before.entries.get(resource)?.kind !== "file") return undefined;
					const state = capture.frontier.has(resource) ? capture.frontier.get(resource) : await workspace.readBase(resource, maxBytes);
					if (state && state.content.byteLength > maxBytes) throw new Error("workspace transaction input exceeds capture limit");
					return state && readContent(state.content, 0, state.content.byteLength);
				}),
				finish: (ownership) => finish(capture, ownership), abort: () => abort(capture),
			};
		});

	function markUnread(current: WorkspaceStructureSnapshot): void {
		for (const resource of regularStructureTransitions(lastStructure, current)) unknown.add(resource);
		lastStructure = current;
	}

	/**
	 * Attribute an overlapped interval by its writer's trace: its changes are the paths it wrote, other changes are foreign and
	 * must not reach what it observed, and nothing may touch its own paths after it ended or while it ran.
	 */
	async function finish(capture: Capture, ownership?: () => Promise<WorkspaceTransactionOwnership | undefined>): Promise<WorkspaceTransactionDelta> {
		const settled = await lock.serialize(async (): Promise<WorkspaceTransactionDelta | { readonly after: WorkspaceStructureSnapshot; readonly capturedAt: number; readonly paths: readonly string[] }> => {
			if (!active.delete(capture)) return { complete: false, changes: [], reason: "transaction_already_settled" };
			if (capture.contaminated || capture.overlapped && !ownership) return { complete: false, changes: [], reason: "overlapping_workspace_transaction" };
			if (!capture.before) return { complete: false, changes: [], reason: poisonReason ?? "workspace_transaction_unavailable" };
			try {
				const after = await captureStructure();
				if (!capture.overlapped) {
					await advanceWorkspaceChangeClock(clock, after);
					if (!sameWorkspaceChangeSnapshot(after, await captureStructure())) throw new Error("workspace changed while fencing transaction endpoint");
				}
				const capturedAt = Date.now();
				markUnread(after);
				return { after, capturedAt, paths: regularStructureTransitions(capture.before, after) };
			} catch (error) {
				return { complete: false, changes: [], reason: `workspace_transaction_capture:${errorMessage(error)}`, before: capture.before };
			}
		});
		if ("complete" in settled) return settled;
		const { after, capturedAt, paths } = settled, before = capture.before!, owned: WorkspaceTransactionOwnership | undefined = capture.overlapped
			? await ownership!().catch(() => undefined) : { written: new Set(paths), observed: new Map(), endedAt: capturedAt, interfered: async () => false };
		const incomplete = (reason: string) => ({ complete: false, changes: [], reason, before, after });
		if (!owned) return incomplete("overlapping_workspace_transaction");
		// A writer owns the objects it named at either endpoint, including every alias and moved descendant.
		const named = (resource: string) => workspaceAncestors(resource).some(name => owned.written.has(name) || owned.written.has(slash(name)));
		const objects = new Set<string>();
		for (const snapshot of [before, after]) for (const [name, entry] of snapshot.entries)
			if (entry.kind === "file" && entry.object && named(name)) objects.add(entry.object);
		const mine = (resource: string) => named(resource) || [before, after].some(snapshot => {
			const entry = snapshot.entries.get(resource); return entry?.kind === "file" && !!entry.object && objects.has(entry.object);
		});
		const own = paths.filter(mine), ownNames = new Set(own), racing = new Set(capture.racing);
		// A foreign change reaches what it saw only if it came no later than its last look at the name or at the directory holding it.
		const seen = (resource: string) => Math.max(owned.observed.get(resource) ?? -Infinity, owned.observed.get(path.dirname(resource).replace(/^\.$/, "")) ?? -Infinity);
		for (const foreign of new Set([...racing, ...paths.filter(resource => !ownNames.has(resource))])) {
			const changed = racing.has(foreign) ? undefined : after.entries.get(foreign)?.changeTimeMs;
			if (seen(foreign) > -Infinity && (changed === undefined || Math.floor(changed) <= seen(foreign))) return incomplete(`overlapping_workspace_input:${foreign}`);
		}
		const late = own.find(resource => Math.floor(after.entries.get(resource)?.changeTimeMs ?? -Infinity) > owned.endedAt);
		if (late !== undefined) return incomplete(`overlapping_workspace_write:${late}`);
		const targets = new Set([...owned.written, ...own].flatMap(workspaceAncestors));
		if (targets.size && await owned.interfered(targets, capturedAt)) return incomplete("overlapping_workspace_write");
		let changes: WorkspaceRegularDelta[];
		const entries = new Map(before.entries);
		try {
			let beforeBytes = 0, afterBytes = 0;
			// Its own paths' endpoints are independent reads; they are judged in order, the first that fails deciding.
			const captured = await mapFilesystem(own, async (relativePath): Promise<string | WorkspaceRegularDelta> => {
				if (capture.unknown.has(relativePath) && before.entries.get(relativePath)?.kind === "file") return `overlapping_workspace_before:${relativePath}`;
				// Bytes last seen stand for a file only while it was there when this interval began.
				const previous = before.entries.get(relativePath)?.kind !== "file" ? undefined
					: capture.frontier.has(relativePath) ? capture.frontier.get(relativePath) : await workspace.readBase(relativePath);
				const entry = after.entries.get(relativePath);
				const current = entry?.kind === "file" ? await readRegularState(path.resolve(sandboxRoot, relativePath), undefined, undefined, workspace.contents) : undefined;
				beforeBytes += contentMemory(previous?.content); afterBytes += contentMemory(current?.content);
				if (Math.max(beforeBytes, afterBytes) > WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES) return "workspace transaction exceeds memory budget";
				if (entry?.kind === "file" && (!current?.identity || statChangeDigest(current.identity) !== entry.changeDigest)) return `overlapping_workspace_write:${relativePath}`;
				return { relativePath, ...(previous ? { before: previous.content, beforeMode: previous.mode } : {}),
					...(current ? { after: current.content, afterMode: current.mode } : {}) };
			});
			const failure = captured.find((change): change is string => typeof change === "string");
			if (failure !== undefined) return incomplete(failure);
			changes = captured as WorkspaceRegularDelta[];
			if (!capture.overlapped && !sameWorkspaceChangeSnapshot(after, await captureStructure())) return incomplete("workspace changed while sealing transaction endpoint");
		} catch (error) { return incomplete(`workspace_transaction_capture:${errorMessage(error)}`); }
		// Both object names and their owned directories advance together; everything else stays as it began.
		for (const name of new Set([...before.entries.keys(), ...after.entries.keys()])) {
			const entry = after.entries.get(name), previous = before.entries.get(name);
			if (!ownNames.has(name) && ((entry ?? previous)?.kind !== "directory" || !mine(name))) continue;
			if (entry) entries.set(name, entry); else entries.delete(name);
		}
		return { complete: true, changes, before, after: capture.overlapped ? workspaceStructureSnapshot(after.root, entries, after.complete) : after };
	}

	async function captureBefore(overlapped = false): Promise<{ readonly before: WorkspaceStructureSnapshot; readonly racing: readonly string[] }> {
		try {
			for (let attempt = 0; attempt < WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS; attempt++) {
				const current = await captureStructure();
				await advanceWorkspaceChangeClock(clock, current);
				const fenced = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(current, fenced)) continue;
				markUnread(fenced);
				// Quiet again: retain every endpoint that moved since the last known frontier.
				await captureTransitions([...unknown], fenced);
				unknown.clear();
				const verified = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(fenced, verified)) continue;
				lastStructure = verified;
				return { before: verified, racing: [] };
			}
			throw new Error("workspace did not stabilize before transaction execution");
		} catch (error) {
			if (!overlapped) throw error;
			// A still-moving start keeps its racing names unread until their writers can prove ownership.
			const current = await captureStructure();
			await advanceWorkspaceChangeClock(clock, current);
			const before = await captureStructure(), racing = regularStructureTransitions(current, before);
			markUnread(before);
			return { before, racing };
		}
	}

	const abort = (capture: Capture): Promise<void> => lock.serialize(async () => { if (active.delete(capture)) for (const other of active) other.contaminated = true; });

	const dispose = (): Promise<void> => lock.serialize(async () => {
		for (const capture of active) capture.contaminated = true;
		active.clear();
		const closingClock = clock;
		clock = undefined;
		await closingClock?.handle.close();
	});

	async function captureTransitions(paths: readonly string[], after: WorkspaceStructureSnapshot): Promise<void> {
		// The lock and overlap rejection keep this frontier unchanged, and each file the size its snapshot took, throughout the interval: read those at once.
		const sizes = paths.map(relativePath => { const entry = after.entries.get(relativePath); return entry?.kind === "file" ? entry.size : undefined; });
		if (!workspace.contents && sizes.reduce<number>((total, size) => total + (size ?? 0), 0) > WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES) throw new Error("workspace transaction after-state exceeds capture limit");
		const currents = await mapFilesystem([...paths.keys()], async index => sizes[index] === undefined ? undefined : readRegularState(path.resolve(sandboxRoot, paths[index]!), sizes[index], undefined, workspace.contents));
		for (const [index, relativePath] of paths.entries()) {
			const current = currents[index], unchangedBytes = retainedBytes - contentMemory(frontier.get(relativePath)?.content);
			if (unchangedBytes + contentMemory(current?.content) > WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES) throw new Error("workspace transaction after-state exceeds capture limit");
			retainedBytes = unchangedBytes + contentMemory(current?.content);
			frontier.set(relativePath, current);
		}
	}

	return { begin, dispose };
}

async function createSandboxRepository(sourceRoot: string, gitBinary: string) {
	const parent = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-action-pool-"));
	const repository = path.join(parent, "snapshot.git");
	const git = bindGit(gitBinary, parent, ["--git-dir", repository]);
	try {
		await bindGit(gitBinary, parent)(["init", "--bare", "--template=", repository]);
		// This private snapshot stores raw bytes. Keep blobs packed so Git reads use bounded mapping windows.
		await mkdir(path.join(repository, "info"), { recursive: true });
		await writeFile(path.join(repository, "config"), "\n[core]\n\tautocrlf = false\n\tlongpaths = true\n\tattributesFile = /dev/null\n" +
			"\tbigFileThreshold = 16m\n\tpackedGitWindowSize = 16m\n\tpackedGitLimit = 32m\n" +
			"[fastimport]\n\tunpackLimit = 0\n[pack]\n\tthreads = 1\n\twindowMemory = 16m\n", { flag: "a" });
		await writeFile(path.join(repository, "info", "attributes"), "* -text -eol -filter -ident -working-tree-encoding\n");
		return {
			sourceRoot, parent, gitBinary, git,
			versions: new ResourceVersionManager(sourceRoot, { snapshotExcludes: SNAPSHOT_EXCLUDES, gitObjects: true }),
			active: 0, idleWaiters: new Set<() => void>(), lock: new RuntimeLifecycleLane(), overlayBaselines: new Map<string, Promise<SharedOverlayBaseline>>(),
			// Unsafe live-mount storage is detached from allocation and retained for OS-level recovery.
			quarantined: false,
		};
	} catch (error) { await rm(parent, { recursive: true, force: true }); throw error; }
}

async function acquireSandboxBaseline(repository: PooledGitRepository, warmup = false): Promise<NonNullable<PooledGitRepository["baseline"]>> {
	// The pool owns this shared baseline; callers cancel before private workspace allocation.
	return repository.lock.serialize(async () => {
		const baseline = repository.baseline;
		if (baseline) {
			// Quiet notifications reuse preparation; fresh allocations check exact evidence below.
			const changes = warmup ? repository.versions.changesSince(baseline.version) : undefined;
			if (changes && !changes.uncertain && !changes.paths.length) return baseline;
			if (!(await repository.versions.validate(baseline.version)).expired) return baseline;
		}
		for (let attempt = 0; attempt < WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS; attempt++) {
			// Preparation owns immutable bytes, not source freshness. Actual allocations capture exact
			// evidence; borrowed process preparations validate their observed inputs and effects at adoption.
			let version: ResourceVersionToken | undefined;
			try {
				version = await repository.versions.capture([{ path: repository.sourceRoot, scope: "tree_content" }]);
				const aliases = [...version.observations.values()].flatMap(entry => entry.aliases ?? []).map(group => {
					if (group.paths.length !== group.links) throw new Error("workspace hardlink namespace is not closed");
					return group.paths.map(target => {
						const relative = relativeFilesystemPath(repository.sourceRoot, target);
						if (!relative || isSnapshotExcluded(slash(relative))) throw new Error("workspace hardlink escapes snapshot");
						return relative;
					});
				});
				if (!(await stageTreeObjects(repository, version))) continue;
				const tree = (await repository.git(["write-tree"])).toString("utf8").trim();
				// Baselines are independent snapshots: a parent chain would keep every replaced copy reachable.
				const commit = tree === baseline?.tree && JSON.stringify(aliases) === JSON.stringify(baseline.aliases) ? baseline.commit
					: (await repository.git(["commit-tree", tree, "-m", "speculative baseline"], { environment: SANDBOX_AUTHOR_ENVIRONMENT })).toString("utf8").trim();
				if (!warmup && (await repository.versions.validate(version)).expired) continue;
				if (commit !== baseline?.commit) await repository.git(["update-ref", "refs/heads/baseline", commit]);
				if (commit !== baseline?.commit && Date.now() >= (repository.prunedAt ?? 0) + SANDBOX_PRUNE_INTERVAL_MS) {
					repository.prunedAt = Date.now(); // The grace keeps objects that concurrent workspace staging has not referenced yet.
					await repository.git(["gc", "--cruft", "--quiet", "--prune=5.minutes.ago"]).catch(() => undefined);
				}
				repository.baseline = { commit, tree, version, aliases };
				baseline?.version.release();
				return repository.baseline;
			} catch (error) {
				if (!(error instanceof FilesystemCaptureChangedError || isMissing(error))) throw error;
			} finally {
				if (repository.baseline?.version !== version) version?.release();
			}
		}
		throw new Error("workspace changed repeatedly while preparing sandbox baseline");
	});
}

/** Stage only changed objects through the shared inode reader, verifying Git imported the exact captured bytes. */
async function stageTreeObjects(repository: PooledGitRepository, version: ResourceVersionToken): Promise<boolean> {
	return withFilesystemReadView(undefined, async view => {
		const staged = gitTreeEntries(await repository.git(["ls-files", "-s", "-z"])), desired = capturedGitTree(version, staged);
		const changed = changedGitEntries(staged, desired).flatMap(name => desired.has(name) ? [[name, desired.get(name)!] as const] : []);
		async function* objects() {
			for (const [index, [name, entry]] of changed.entries()) {
				const link = entry.link === undefined ? undefined : Buffer.from(entry.link), reader = link ? undefined : await openFilesystemFile(path.join(repository.sourceRoot, name), true, view);
				try {
					const size = link?.length ?? Number(reader!.stat.size);
					if (!Number.isSafeInteger(size)) throw new Error("Git input size is not representable");
					yield Buffer.from(`blob\nmark :${index + 1}\ndata ${size}\n`);
					if (link) yield link;
					else if (size) yield* reader!.handle.createReadStream({ autoClose: false, end: size - 1 });
					if (reader && !sameFilesystemIdentity(reader.stat, await reader.handle.stat({ bigint: true }))) throw new FilesystemCaptureChangedError("Git input changed while staging");
					yield Buffer.from(`\nget-mark :${index + 1}\n`);
				} finally { try { await reader?.handle.close(); } finally { await reader?.binding?.close(); } }
			}
			yield Buffer.from("done\n");
		}
		const written = changed.length ? (await repository.git(["fast-import", "--quiet", "--done"], { input: objects() })).toString("utf8").trim().split("\n") : [];
		if (written.length !== changed.length || written.some((blob, index) => blob !== changed[index]![1].blob)) return false;
		const updates = [...changed.map(([name, entry]) => `${entry.mode} ${entry.blob}\t${name}\0`),
			...[...staged.keys()].filter((name) => !desired.has(name)).map((name) => `0 ${"0".repeat(40)}\t${name}\0`)];
		if (updates.length) await repository.git(["update-index", "-z", "--index-info"], { input: Buffer.from(updates.join("")) });
		return true;
	});
}

function gitBlobID(bytes: Buffer): string {
	return createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
}

type GitTreeEntry = { readonly mode: string; readonly blob: string; readonly link?: string };
function gitTreeEntries(output: Buffer): Map<string, GitTreeEntry> {
	if (output.length && output.at(-1) !== 0) throw new Error("unterminated Git tree entry");
	return new Map(parseNullList(output).map(record => {
		const match = /^(\d{6}) (?:blob |commit )?([0-9a-f]{40})(?: 0)?\t(.+)$/su.exec(record);
		if (!match) throw new Error("invalid Git tree entry");
		return [match[3]!, { mode: match[1]!, blob: match[2]! }];
	}));
}
function capturedGitTree(version: ResourceVersionToken, baseline: ReadonlyMap<string, GitTreeEntry>): Map<string, GitTreeEntry> {
	if (!version.treeObjects) throw new Error("Git input tree is unavailable");
	return new Map([...version.treeObjects].flatMap(([target, object]) => {
		const relative = relativeFilesystemPath(version.root, target), name = relative && slash(relative);
		return !name || isSnapshotExcluded(name) ? [] : [[name, object.link !== undefined
			? { mode: "120000", blob: gitBlobID(Buffer.from(object.link)), link: object.link }
			: { mode: process.platform === "win32" ? baseline.get(name)?.mode ?? "100644" : object.mode & 0o100 ? "100755" : "100644", blob: object.blob }]];
	}));
}
function changedGitEntries(before: ReadonlyMap<string, GitTreeEntry>, after: ReadonlyMap<string, GitTreeEntry>): string[] {
	return [...new Set([...before.keys(), ...after.keys()])].filter(name => before.get(name)?.mode !== after.get(name)?.mode || before.get(name)?.blob !== after.get(name)?.blob);
}

/** Claim before waiting: allocation, warm-up and shutdown cannot own the same checkout. */
async function takePreparedSandbox(repository: PooledGitRepository, baseline?: NonNullable<PooledGitRepository["baseline"]>): Promise<PreparedGitWorkspace | undefined> {
	const pending = repository.prepared;
	repository.prepared = undefined;
	const workspace = await pending?.catch(() => undefined);
	if (!workspace || !baseline) return workspace;
	if (!workspace.written && workspace.commit === baseline.commit && JSON.stringify(workspace.aliases) === JSON.stringify(baseline.aliases)) return workspace;
	try { return await resetSandboxWorkspace(repository, workspace, baseline); }
	catch { await workspace.dispose().catch(() => undefined); return undefined; }
}

async function attachSandboxWorkspace(repository: PooledGitRepository, baseline: NonNullable<PooledGitRepository["baseline"]>,
	ownedProcessRoot?: string): Promise<PreparedGitWorkspace> {
	const { commit, aliases } = baseline;
	const processRoot = ownedProcessRoot ?? (await mkdtemp(path.join(repository.parent, "action-")));
	const sandboxRoot = path.join(processRoot, "workspace");
	try {
		if (ownedProcessRoot) await mkdir(processRoot, { recursive: true });
		await repository.git(["worktree", "add", "--detach", sandboxRoot, commit], { cwd: processRoot });
		await linkSandboxAliases(sandboxRoot, aliases);
		const gitDirectory = (await bindGit(repository.gitBinary, sandboxRoot, ["-C", sandboxRoot])(["rev-parse", "--absolute-git-dir"])).toString("utf8").trim();
		if (!path.isAbsolute(gitDirectory) || filesystemPathKey(path.dirname(processRoot)) !== filesystemPathKey(repository.parent)
			|| filesystemPathKey(path.dirname(gitDirectory)) !== filesystemPathKey(path.join(repository.parent, "snapshot.git", "worktrees"))) {
			throw new Error("private Git workspace ownership is unavailable");
		}
		let disposal: Promise<void> | undefined;
		return { sandboxRoot, processRoot, commit, aliases, dispose: () => disposal ??= (async () => {
			// Keep the Git name reserved until its workspace is gone. A retired owner must never delete a reused name.
			await removeOwnedTree(processRoot); await rm(gitDirectory, { recursive: true, force: true });
		})() };
	} catch (error) {
		await repository.git(["worktree", "remove", "--force", sandboxRoot]).catch(() => undefined);
		if (!ownedProcessRoot) await rm(processRoot, { recursive: true, force: true }).catch(() => undefined);
		throw error;
	}
}

async function linkSandboxAliases(sandboxRoot: string, groups: readonly (readonly string[])[]): Promise<void> {
	for (const paths of groups.map(group => group.map(relative => path.resolve(sandboxRoot, relative)))) {
		for (const target of paths) await assertNoSymlinkPath(sandboxRoot, target);
		for (const target of paths.slice(1)) { await unlink(target); await link(paths[0]!, target); }
	}
}

/** Remove a tree this process owns, whatever modes a command left in it: a directory without owner write or search keeps its entries. */
const removeOwnedTree = (target: string): Promise<void> => rm(target, { recursive: true, force: true }).catch(async (error: unknown) => {
	if (!hasErrorCode(error, "EACCES")) throw error;
	const grant = async (directory: string): Promise<void> => { if (!(await lstat(directory).catch(() => undefined))?.isDirectory()) return; await chmod(directory, 0o700).catch(() => {});
		for (const entry of await readFilesystemDirectory(directory).catch(() => [])) if (entry.isDirectory()) await grant(path.join(directory, entry.name)); };
	await grant(target); await rm(target, { recursive: true, force: true });
});

/** Paths a settled change wrote, relative to the root it wrote under. */
function writtenPaths(changes: readonly SandboxWorkspaceChange[]): string[] {
	return changes.flatMap(change => change.validationOnly ? [] : [change.target, ...change.kind !== "directory" ? change.aliases ?? [] : []]
		.map(target => path.relative(change.root, target)));
}

/** Warm and returned checkouts share one slot; replacement resets that same owned workspace. */
function prepareNextSandbox(pool: PooledGitRepository, baseline: NonNullable<PooledGitRepository["baseline"]>): Promise<unknown> {
	if (pool.lock.sealed || pool.quarantined) return Promise.resolve();
	const next = pool.prepared = takePreparedSandbox(pool, baseline).then(workspace => workspace ?? attachSandboxWorkspace(pool, baseline));
	void next.catch(() => { if (pool.prepared === next) pool.prepared = undefined; });
	return next;
}

function retireSandboxWorkspace(repository: PooledGitRepository, workspace: PreparedGitWorkspace, written: readonly string[] = []): Promise<void> {
	if (repository.prepared || repository.lock.sealed || repository.quarantined) return workspace.dispose();
	repository.prepared = Promise.resolve({ ...workspace, written }); return Promise.resolve();
}

/**
 * Only filesystem tools wrote here, each path recorded: those paths and the old link groups are removed so Git restores them
 * with checkout modes, Git resets and cleans the rest, and a clean status (ignored files included) proves the baseline.
 */
async function resetSandboxWorkspace(repository: PooledGitRepository, workspace: PreparedGitWorkspace,
	baseline: NonNullable<PooledGitRepository["baseline"]>): Promise<PreparedGitWorkspace> {
	const { sandboxRoot } = workspace, git = bindGit(repository.gitBinary, sandboxRoot, ["-C", sandboxRoot]);
	for (const relative of [...workspace.written ?? [], ...workspace.aliases.flat()]) {
		const target = path.resolve(sandboxRoot, relative);
		if (!relativeFilesystemPath(sandboxRoot, target)) throw new Error("recycled path escapes its workspace");
		await assertNoSymlinkPath(sandboxRoot, target); await removeOwnedTree(target);
	}
	await git(["reset", "--hard", "--quiet", baseline.commit]); await git(["clean", "-ffdxq"]);
	await linkSandboxAliases(sandboxRoot, baseline.aliases);
	if ((await git(["status", "--porcelain", "--ignored", "--untracked-files=all", "-z"])).length) throw new Error("recycled workspace differs from its baseline");
	return { ...workspace, commit: baseline.commit, aliases: baseline.aliases, written: undefined };
}

async function acquireOverlayBaseline(
	repository: PooledGitRepository,
	snapshot: NonNullable<PooledGitRepository["baseline"]>,
): Promise<SharedOverlayBaseline> {
	const { commit } = snapshot;
	return repository.lock.serialize(async () => {
		const original = await captureSnapshotMetadata(repository.sourceRoot), identity = createHash("sha256").update(commit);
		for (const [name, stat] of [...original].sort(([a], [b]) => a.localeCompare(b)))
			identity.update(JSON.stringify([name, ...SNAPSHOT_METADATA_FIELDS.map(field => String(stat[field]))]));
		const key = identity.digest("hex");
		for (const [candidateCommit, pending] of repository.overlayBaselines) {
			if (candidateCommit === key) continue;
			const candidate = await pending.catch(() => undefined);
			if (!candidate || candidate.active > 0) continue;
			repository.overlayBaselines.delete(candidateCommit);
			await candidate.dispose().catch(() => undefined);
		}
		let pending = repository.overlayBaselines.get(key);
		if (!pending) {
			pending = attachSandboxWorkspace(repository, snapshot, path.join(repository.parent, `overlay-baseline-${key}`))
				.then(async workspace => {
					try { return { ...workspace, active: 0, modes: (await preserveSnapshotMetadata(repository.sourceRoot, workspace.sandboxRoot, workspace.processRoot, false, original)).modes }; }
					catch (error) { await workspace.dispose(); throw error; }
				});
			repository.overlayBaselines.set(key, pending);
			void pending.catch(() => { if (repository.overlayBaselines.get(key) === pending) repository.overlayBaselines.delete(key); });
		}
		const baseline = await pending;
		baseline.active++;
		return baseline;
	});
}

async function captureOwnedWorkspaceStructure(root: string): Promise<WorkspaceStructureSnapshot> {
	const snapshot = await withFilesystemReadView(undefined, view => captureWorkspaceStructure(root, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: SNAPSHOT_EXCLUDES, view }));
	return Object.freeze({ ...snapshot,
		// The lower stat identity belongs to this owned resource, not the merged mount's device.
		entries: new Map([...snapshot.entries].map(([resource, entry]) => [resource,
			entry.kind === "file" ? { ...entry, contentPath: path.join(snapshot.root, resource) } : entry])),
	});
}

function releaseOverlayBaseline(baseline: SharedOverlayBaseline): void { baseline.active = Math.max(0, baseline.active - 1); }

function closeSandboxRepository(repository: PooledGitRepository): Promise<void> {
	return repository.lock.close(async () => {
		if (repository.active > 0) await new Promise<void>(resolve => repository.idleWaiters.add(resolve));
		if (repository.quarantined) return;
		if (repository.idleTimer) clearTimeout(repository.idleTimer);
		const prepared = await takePreparedSandbox(repository);
		await prepared?.dispose().catch(() => undefined);
		repository.baseline?.version.release();
		repository.baseline = undefined;
		repository.versions.close();
		await removeOwnedTree(repository.parent);
	});
}

function throwIfAborted(signal?: AbortSignal): void {
	if (!signal?.aborted) return;
	throw signal.reason instanceof Error ? signal.reason : new Error("sandbox preparation aborted");
}

/** Case-insensitive volumes, and win32 trailing dots and spaces, spell the same excluded entry differently. */
const snapshotSegment = process.platform === "win32" ? (segment: string) => segment.replace(/[. ]+$/u, "").toLowerCase()
	: process.platform === "darwin" ? (segment: string) => segment.toLowerCase() : (segment: string) => segment;

function isSnapshotExcluded(relative: string): boolean {
	return relative.split("/").map(snapshotSegment).some((segment) => (SNAPSHOT_EXCLUDES as readonly string[]).includes(segment) ||
		segment.startsWith(SANDBOX_STAGING_FILE_PREFIX) && segment.endsWith(".tmp"));
}

async function collectSandboxChanges(workspace: ReturnType<typeof createWorkspaceObservation>, frontier: readonly string[]): Promise<readonly SandboxFileChange[]> {
	return withFilesystemReadView(undefined, async view => {
		const resources = [...new Set(frontier)].filter((resource) => !isSnapshotExcluded(slash(resource))).sort();
		// Changed files share their directories: each is walked once, and the files are read concurrently.
		const capture = sharedWalk(captureFilesystemEntry), source = sharedWalk(target => captureFilesystemEntry(target, undefined, view));
		return mapFilesystem(resources, async (resource): Promise<SandboxFileChange> => {
			if (!resource || path.isAbsolute(resource) || resource.split("/").includes("..")) throw new Error(`invalid sandbox change path: ${resource}`);
			const target = path.resolve(workspace.sourceRoot, resource), sandboxTarget = path.resolve(workspace.sandboxRoot, resource);
			if (!containsFilesystemPath(workspace.sourceRoot, target) || !containsFilesystemPath(workspace.sandboxRoot, sandboxTarget)) throw new Error(`sandbox change escapes workspace: ${resource}`);
			await assertNoSymlinkPath(workspace.sourceRoot, target, source);
			await assertNoSymlinkPath(workspace.sandboxRoot, sandboxTarget, capture);
			const before = workspace.baselineFrontier.has(resource) ? workspace.baselineFrontier.get(resource) : await workspace.readBase(resource);
			const after = await readRegularState(sandboxTarget, undefined, undefined, workspace.contents);
			return { root: workspace.sourceRoot, target, resource, before: before?.content, after: after?.content, beforeMode: before?.mode,
				afterMode: after?.mode, afterModified: after?.identity && String(after.identity.mtimeNs), ...(before?.settled ? { beforeIdentity: before.settled } : {}) };
		});
	});
}

interface OverlayStructureRemoval { readonly resource: string; readonly descendantsOnly: boolean; }

/**
 * Reconstruct the complete logical structure from one immutable lower snapshot plus the typed upper
 * journal. Only upper paths and their ancestor directories require fresh merged-view syscalls.
 */
async function captureOverlayWorkspaceStructure(sandboxRoot: string, { baseline, device, upperRoot }: NonNullable<SandboxWorkspaceObservation["overlay"]>): Promise<WorkspaceStructureSnapshot> {
	const frontier = await inspectOverlayStructureFrontier(upperRoot);
	const entries = new Map([...baseline.entries].map(([name, entry]) => [name, entry.kind === "file" && entry.object
		? { ...entry, object: `${device}:${entry.object.split(":")[1]}` } : entry]));
	for (const removal of frontier.removals) {
		const normalized = path.normalize(removal.resource);
		const prefix = normalized ? `${normalized}${path.sep}` : "";
		for (const candidate of entries.keys()) {
			if (prefix && candidate.startsWith(prefix) || !removal.descendantsOnly && candidate === normalized) entries.delete(candidate);
		}
	}
	const refresh = [...frontier.refresh], refreshed = await mapFilesystem(refresh, resource => {
		const target = resource ? path.resolve(sandboxRoot, resource) : sandboxRoot;
		if (!containsFilesystemPath(sandboxRoot, target)) throw new Error(`OverlayFS frontier escapes workspace: ${resource}`);
		return captureWorkspaceStructureEntry(target, resource ? [] : SNAPSHOT_EXCLUDES);
	});
	refresh.forEach((resource, index) => { const entry = refreshed[index]; if (entry) entries.set(resource, entry); else entries.delete(resource); });
	const files = Math.max(0, entries.size - 1);
	return workspaceStructureSnapshot(sandboxRoot, entries, baseline.complete && files <= WORKSPACE_TRANSACTION_MAX_FILES);
}

async function inspectOverlayStructureFrontier(upperRoot: string) {
	const refresh = new Set<string>([""]), removals: OverlayStructureRemoval[] = [];
	const addAncestors = (resource: string) => workspaceAncestors(resource).forEach(name => refresh.add(name));
	const snapshot = await captureWorkspaceStructure(upperRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, visitEntry: async (relative, entry) => {
		const resource = slash(relative), name = path.basename(resource), parent = path.dirname(resource);
		if (isSnapshotExcluded(resource)) return false;
		if (name === ".wh..wh..opq") {
			removals.push({ resource: parent === "." ? "" : parent, descendantsOnly: true }); addAncestors(parent); return false;
		}
		if (name.startsWith(".wh.")) throw new Error(`unsupported OverlayFS whiteout encoding: ${name}`);
		if (entry.isCharacterDevice()) {
			if ((await lstat(path.join(upperRoot, relative))).rdev !== 0) throw new Error(`unsupported OverlayFS device entry: ${resource}`);
			removals.push({ resource, descendantsOnly: false }); addAncestors(parent); return false;
		}
		if (!entry.isDirectory() && !entry.isFile()) throw new Error(`unsupported OverlayFS upper inode: ${resource}`);
		addAncestors(resource);
		return entry.isDirectory();
	} }).catch(error => { if (isMissing(error) && error.path === upperRoot) return undefined; throw error; });
	if (snapshot && !snapshot.complete) throw new Error("OverlayFS structure frontier is incomplete");
	return { refresh, removals: Object.freeze(removals) };
}

/** The workspace's own structure for a live lower; undefined keeps the snapshot when a truncated walk cannot vouch for what it skipped. */
async function captureLiveBase(pool: PooledGitRepository): Promise<WorkspaceStructureSnapshot | undefined> {
	const cached = pool.liveBase, changes = cached && pool.versions.changesSince(cached.version);
	if (cached && changes && !changes.uncertain && !changes.paths.length) return cached.structure;
	const version = await pool.versions.observeChanges();
	const live = await captureOwnedWorkspaceStructure(pool.sourceRoot);
	if (!live.complete) { version.release(); return undefined; }
	if (pool.liveBase !== cached) version.release();
	else { cached?.version.release(); pool.liveBase = { version, structure: live }; }
	return live;
}

/** A lower file read now is the one the workspace started from only while its identity is unchanged. */
async function readLiveBase(base: WorkspaceStructureSnapshot, resource: string, maxBytes: number, contents?: ContentChunkPool): Promise<RegularFileState | undefined> {
	const entry = base.entries.get(resource);
	if (entry?.kind !== "file") return undefined;
	const takenAtMs = Date.now(), captured = await withFilesystemReadView(undefined, view => readRegularState(entry.contentPath ?? path.join(base.root, resource), maxBytes, view, contents));
	if (!captured?.identity || statChangeDigest(captured.identity) !== entry.changeDigest) throw new Error(`workspace changed since the sandbox started: ${resource}`);
	return { ...captured, settled: settledIdentity(captured.identity, takenAtMs) };
}

async function readGitTreeRegularState(git: ReturnType<typeof bindGit>, tree: string, resource: string,
	maxBytes = WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES, contents?: ContentChunkPool): Promise<RegularFileState | undefined> {
	const entry = gitTreeEntries(await git(["ls-tree", "-z", tree, "--", resource])).get(slash(resource));
	if (!entry) return undefined;
	if (entry.mode !== "100644" && entry.mode !== "100755") throw new Error(`workspace transaction resource is not a regular file: ${resource}`);
	const content = contents ? (await contents.capture(async sink => {
		const hash = createHash("sha256"); let bytesRead = 0;
		await git(["cat-file", "blob", entry.blob], { maxBuffer: maxBytes, onChunk: async chunk => { hash.update(chunk); bytesRead += chunk.length; await sink(chunk); } });
		return { bytesRead, hash: hash.digest("hex") };
	})).content : await git(["cat-file", "blob", entry.blob], { maxBuffer: Math.max(1, Math.min(WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES, maxBytes) + 1) });
	if (content.byteLength > maxBytes) throw new Error(`Git transaction blob exceeds capture limit: ${resource}`);
	return { content, mode: process.platform === "win32" ? 0 : entry.mode === "100755" ? 0o755 : 0o644 };
}


async function assertCommitTarget(change: SandboxWorkspaceChange, capture?: typeof captureFilesystemEntry): Promise<void> {
	const root = path.resolve(change.root);
	const target = path.resolve(change.target);
	if (!containsFilesystemPath(root, target) || (target === root && !change.validationOnly) || target !== path.resolve(root, change.resource) ||
		isSnapshotExcluded(slash(change.resource))) {
		throw new Error(`sandbox commit path escapes workspace: ${change.resource}`);
	}
	if (change.kind === "directory" && change.operation && (change.before || !change.after)) {
		throw new Error("Native directory creation requires an absent baseline and a sealed result");
	}
	if (change.kind !== "directory") for (const name of [...(change.aliases ?? []), ...(change.afterAliases ?? []), ...(change.object ? [change.object.path] : [])]) {
		if (!path.isAbsolute(name) || !containsFilesystemPath(root, name) || path.resolve(name) === root) throw new Error("file object name escapes workspace");
	}
	await assertNoSymlinkPath(root, target, capture);
}

async function readRegularState(target: string, maxBytes?: number, view?: FilesystemReadView, contents?: ContentChunkPool): Promise<RegularFileState | undefined> {
	try {
		const limit = Math.min(maxBytes ?? Infinity, contents?.limits.maxLogicalBytes ?? WORKSPACE_TRANSACTION_MAX_MEMORY_BYTES);
		const streamed = contents && await contents.capture(onChunk => captureStableFile(target, limit, false, { view, onChunk }));
		const captured = streamed ? streamed.capture : await captureStableFile(target, limit, true, { digest: false, view });
		return { content: streamed ? streamed.content : captured.content!, mode: process.platform === "win32" ? 0 : Number(captured.stat.mode & 0o777n), identity: captured.stat };
	} catch (error) { if (isMissing(error)) return undefined; throw error; }
}

async function assertFileAliases(root: string, target: string, aliases: readonly string[], identity?: BigIntStats, view = FilesystemReadView.ambient()): Promise<void> {
	if (!identity || identity.nlink !== BigInt(aliases.length) || !aliases.includes(target)) throw new Error("file alias set changed");
	for (const alias of aliases) {
		await assertNoSymlinkPath(root, alias, (target, read) => captureFilesystemEntry(target, read, view));
		if (!sameFilesystemIdentity(identity, await view.stat(alias))) throw new Error("file alias identity changed");
	}
}

/** Capture a directory without following links and reject concurrent namespace changes. */
export async function readSandboxDirectoryState(target: string, captureNames?: (names: readonly string[]) => void, view?: FilesystemReadView): Promise<SandboxDirectoryState | undefined> {
	try {
		const { info, entries } = await captureFilesystemEntry(target, "directory", view);
		if (!entries) throw new Error(`sandbox resource is not a real directory: ${target}`);
		captureNames?.(entries.map(entry => entry.name));
		return { entriesDigest: directoryEntriesDigest(entries), mode: Number(info.mode & 0o777n), uid: Number(info.uid), gid: Number(info.gid) };
	} catch (error) { if (isMissing(error)) return undefined; throw error; }
}


export function sameSandboxState(
	left: RegularFileState | SandboxDirectoryState | undefined,
	right: RegularFileState | SandboxDirectoryState | undefined,
): boolean {
	if (!left || !right) return left === right;
	if ("content" in left || "content" in right) return "content" in left && "content" in right &&
		sameContent(left.content, right.content) && (right.mode === 0 || sameExecutableMode(left.mode, right.mode));
	return left.entriesDigest === right.entriesDigest && left.mode === right.mode && left.uid === right.uid && left.gid === right.gid;
}

async function verifySandboxDirectoryAfter(change: SandboxDirectoryChange, view = FilesystemReadView.ambient()): Promise<readonly string[] | undefined> {
	// Native mkdir observes existence; subsequent new-entry reads/access need their own proof.
	let names: readonly string[] | undefined;
	const same = change.operation ? (await view.stat(change.target)).isDirectory()
		: sameSandboxState(await readSandboxDirectoryState(change.target, captured => { names = captured; }, view), change.after);
	if (!same) throw new Error(`directory changed while committing: ${change.resource}`);
	return names;
}

function assertExistingInputPolicy(change: SandboxWorkspaceChange): void {
	// Default ACLs and other inherited creation policy are not observable through this binding.
	if (!change.validationOnly && change.before === undefined) throw new Error("Created input permissions require authoritative execution");
}



/** Tool reads serve text: bytes holding a NUL among their first 8000 (git's binary test) are never kept as an input. */
const textual = async (content: ContentVersion | undefined) => !content || !(await readContent(content, 0, Math.min(8000, content.byteLength))).includes(0);

function sandboxChangeBytes(change: SandboxWorkspaceChange | undefined): number {
	return !change || change.kind === "directory" ? 0 : contentMemory(change.before) + contentMemory(change.after);
}

/** Take ownership once, before cleanup or commit locks can yield to a retained caller reference. */
function ownSandboxChanges(changes: readonly SandboxWorkspaceChange[], project = (name: string) => path.resolve(name)): SandboxWorkspaceChange[] {
	const result = new Map<string, SandboxWorkspaceChange>();
	for (const change of changes) {
		const key = filesystemPathKey(change.target);
		const previous = result.get(key);
		if (!previous) { result.set(key, change); continue; }
		if (
			filesystemPathKey(previous.root) !== filesystemPathKey(change.root) ||
			(previous.kind === "directory") !== (change.kind === "directory") || previous.validationOnly !== change.validationOnly ||
			previous.operation !== change.operation || previous.kind !== "directory" && change.kind !== "directory" &&
				JSON.stringify(projectFileMutation(previous)) !== JSON.stringify(projectFileMutation(change))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		const same = previous.kind === "directory" && change.kind === "directory" ? sameSandboxState(previous.before, change.before)
			: previous.kind !== "directory" && change.kind !== "directory" && sameContent(previous.before, change.before) &&
				(previous.beforeMode === undefined || change.beforeMode === undefined || sameExecutableMode(previous.beforeMode, change.beforeMode));
		if (!same) throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		const baseline = previous.kind === "directory" ? { before: previous.before }
			: { before: previous.before, beforeMode: previous.beforeMode, beforeIdentity: previous.beforeIdentity };
		result.set(key, { ...change, ...baseline, accessMode: (previous.accessMode ?? 0) | (change.accessMode ?? 0) } as SandboxWorkspaceChange);
	}
	return [...result.values()].map((change): SandboxWorkspaceChange => {
		const target = { root: project(change.root), target: project(change.target) };
		return change.kind === "directory"
			? { ...change, ...target, before: change.before && { ...change.before }, after: change.after && { ...change.after } }
			: { ...change, ...target, ...projectFileMutation(change, project),
				before: ownContent(change.before), after: ownContent(change.after) };
	}).sort((left, right) =>
		filesystemPathKey(left.target).localeCompare(filesystemPathKey(right.target)),
	);
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
					try { await rmdir(change.target); } catch (error) { if (!isMissing(error)) throw error; }
				} else {
					const current = await readSandboxDirectoryState(change.target, undefined, view);
					if (!current) { await createParentDirectories(change.root, change.target, view); await mkdir(change.target, { mode: directory.mode }); }
					if (process.platform !== "win32") await chmod(change.target, directory.mode);
				}
				continue;
			}
			const file = baseline as RegularFileState | undefined;
			if (!file) await rm(change.target, { force: true });
			else {
				const temporary = await stageAtomicWrite(file.content, file.mode, change.root);
				try { await createParentDirectories(change.root, change.target, view); await replaceFile(temporary, change.target, file.mode); }
				finally { await rm(temporary, { force: true }).catch(() => undefined); }
			}
		} catch (error) {
			errors.push(error);
		}
	}
	for (const change of changes) {
		try {
			const baseline = baselines.get(change);
			const current = change.kind === "directory" ? await readSandboxDirectoryState(change.target, undefined, view) : await readRegularState(change.target, undefined, view, contents);
			if (!sameSandboxState(current, baseline)) throw new Error(`sandbox rollback did not restore: ${change.resource}`);
		} catch (error) {
			errors.push(error);
		}
	}
	if (errors.length > 0) throw new AggregateError(errors, "failed to restore sandbox commit changes");
}

function resolveCommitMode(current: RegularFileState | undefined, change: SandboxFileChange): number | undefined {
	if (process.platform === "win32") return undefined;
	if (change.afterMode === undefined) return current?.mode ?? 0o644;
	if (!current || change.beforeMode === undefined) return change.afterMode;
	if (sameExecutableMode(change.beforeMode, change.afterMode)) return current.mode;
	return isExecutableMode(change.afterMode) ? current.mode | (change.afterMode & 0o111) : current.mode & ~0o111;
}

function sameExecutableMode(left: number, right: number): boolean { return isExecutableMode(left) === isExecutableMode(right); }

function isExecutableMode(mode: number): boolean { return (mode & 0o111) !== 0; }

async function stageAtomicWrite(content: ContentVersion, mode: number | undefined, stagingDirectory: string): Promise<string> {
	await mkdir(stagingDirectory, { recursive: true });
	const temporary = path.join(stagingDirectory, `${SANDBOX_STAGING_FILE_PREFIX}${randomUUID()}.tmp`);
	const fileMode = process.platform === "win32" ? undefined : mode;
	const handle = await open(temporary, "wx", fileMode ?? 0o600);
	let staged = false;
	try {
		await writeContent(handle, content);
		await handle.sync();
		if (fileMode !== undefined) await handle.chmod(fileMode);
		staged = true; return temporary;
	} finally {
		await handle.close().catch(() => undefined);
		if (!staged) await rm(temporary, { force: true }).catch(() => undefined);
	}
}

async function createParentDirectories(sourceRoot: string, target: string, view: FilesystemReadView, created?: string[], ensured?: Set<string>): Promise<void> {
	const root = path.resolve(sourceRoot);
	const parent = path.dirname(path.resolve(target));
	const relative = relativeFilesystemPath(root, parent);
	if (relative === undefined) throw new Error(`sandbox commit path escapes workspace: ${target}`);
	let current = root;
	for (const segment of relative.split(path.sep).filter(Boolean)) {
		current = path.join(current, segment);
		if (ensured?.has(current)) continue; // This commit made or found it real; each change's own path check precedes this.
		try { await mkdir(current); created?.push(current); } catch (error) {
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
export async function restoreModifiedTimes(files: readonly (readonly [target: string, modified: string | undefined])[]): Promise<void> {
	const times = files.flatMap(([target, modified]) => /^\d+$/.test(modified ?? "") ? [[modified!.padStart(10, "0").slice(0, -9), modified!.slice(-9).padStart(9, "0"), target]] : []);
	const pairs = times.flatMap(([seconds, nanoseconds, target]) => {
		if (!nativeFilesystem) return [`@${seconds}.${nanoseconds}`, target!];
		const record = Buffer.concat([Buffer.alloc(16), Buffer.from(`${target}\0`)]);
		record.writeBigInt64LE(BigInt(seconds!), 0); record.writeBigInt64LE(BigInt(nanoseconds!), 8);
		if (nativeFilesystem(-3, record) !== 0) throw new Error(`could not restore modification time: ${target}`);
		return [];
	});
	for (let start = 0; process.platform === "linux" && start < pairs.length; start += 1024) await execHostFile("sh",
		["-c", 'while [ $# -gt 0 ]; do touch -chm -d "$1" -- "$2" || exit; shift 2; done', "sh", ...pairs.slice(start, start + 1024)]);
}

async function replaceFile(temporary: string, target: string, mode?: number): Promise<void> {
	await rename(temporary, target);
	if (mode !== undefined && process.platform !== "win32") await chmod(target, mode);
}

/** Root and every proven file name share the native queue; resolved keys give all transactions one strict order. */
async function withCommitLocks<T>(changes: readonly SandboxWorkspaceChange[], run: () => Promise<T>, view: FilesystemReadView): Promise<T> {
	if (!changes.length) return run();
	try {
		const names = [...new Set(changes.flatMap(change => [change.root, change.target, ...change.kind === "directory" ? [] :
			[...(change.aliases ?? []), ...change.object ? [change.object.path] : []]]).map(name => path.resolve(name)))];
		const resolve = () => mapFilesystem(names, name => view.realpath(name).catch(error => {
			if (isMissing(error) || hasErrorCode(error, "ENOTDIR")) return name;
			throw error;
		}));
		const keys = await resolve(), { withFileMutationQueue } = await import("@earendil-works/pi-coding-agent");
		const apply = async () => {
			if ((await resolve()).some((key, index) => key !== keys[index])) throw new Error("commit file names changed while waiting");
			return run();
		};
		return await [...new Set(keys)].sort().reduceRight((next, key) => () => withFileMutationQueue(key, next), apply)();
	} catch (error) { throw effectCommitFailure(error, "recoverable"); }
}


function parseNullList(value: Uint8Array): string[] { return value.toString().split("\0").filter(Boolean).map((item) => slash(item)); }

/** The existing process owner binds each private repository/index once, preserving per-call cwd and limits. */
function bindGit(command: string, cwd: string, prefix: readonly string[] = []) {
	return async (input: readonly string[], options: { cwd?: string; environment?: Readonly<Record<string, string>>; maxBuffer?: number;
		input?: Buffer | AsyncIterable<Buffer>; onChunk?: (chunk: Buffer) => Promise<void> } = {}): Promise<Buffer> => {
		const args = [...prefix, ...input];
		const child = spawn(command, args, { cwd: options.cwd ?? cwd, windowsHide: true, stdio: ["pipe", "pipe", "pipe"], env: {
			...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("GIT_"))),
			GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1", GIT_ATTR_NOSYSTEM: "1", ...options.environment,
		} });
		let stderr = "";
		child.stderr.on("data", chunk => { if (stderr.length < 65536) stderr += String(chunk).slice(0, 65536 - stderr.length); });
		const exited = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", code =>
			code === 0 ? resolve() : reject(new Error(`${command} ${args.slice(0, 16).join(" ")} failed (${code}): ${stderr.trim()}`))); });
		const inputFinished = options.input && !Buffer.isBuffer(options.input) ? pipeline(Readable.from(options.input), child.stdin)
			: new Promise<void>((resolve, reject) => { child.stdin.once("error", reject); child.stdin.end(options.input, resolve); });
		const output = (async () => {
			const chunks: Buffer[] = []; let size = 0;
			for await (const value of child.stdout) {
				const chunk = value as Buffer; size += chunk.length;
				if (size > (options.maxBuffer ?? 64 * 1024 * 1024)) throw new Error("Git output exceeds capture budget");
				if (options.onChunk) await options.onChunk(chunk); else chunks.push(chunk);
			}
			return Buffer.concat(chunks);
		})();
		try { return (await Promise.all([output, inputFinished, exited]))[0]; }
		catch (error) { child.kill(); await Promise.allSettled([output, inputFinished, exited]); throw error; }
	};
}

function ownWorkspaceObservation(description: () => SandboxWorkspaceObservation, contents?: ContentChunkPool): { readonly ready: () => Promise<string>;
	readonly call: <Value>(method: string, args?: unknown) => Promise<Value>; readonly transactions: WorkspaceTransactionDriver } {
	let connection: ReturnType<typeof connectNativeOwner> | undefined, initialized: Promise<string> | undefined, disposal: Promise<void> | undefined;
	const owners = new Map<number, { readonly get: () => Promise<WorkspaceTransactionOwnership | undefined>; proof?: WorkspaceTransactionOwnership }>();
	const ready = (): Promise<string> => {
		if (disposal) return Promise.reject(new Error("workspace transaction driver is disposed"));
		if (!connection) {
			const { baselineFrontier, ...data } = description(), loan = lendContentChanges([...baselineFrontier].map(([resource, entry]) =>
				({ resource, before: entry?.content, mode: entry?.mode, settled: entry?.settled })));
			try { connection = connectNativeOwner("workspace", { data, baseline: loan.transfer, storeRoot: contents?.storeRoot } satisfies SandboxWorkspaceTransfer, async (method, args: { id: number; paths: ReadonlySet<string>; until: number }) => {
			if (method === "hints") return description().hints;
			const owner = owners.get(args.id); if (!owner) throw new Error("workspace interval owner retired");
			if (method === "ownership") { const proof = owner.proof = await owner.get(); return proof && { ...proof, interfered: undefined }; }
			if (method === "interfered" && owner.proof) return owner.proof.interfered(args.paths, args.until);
			throw new Error("workspace interval ownership unavailable");
			});
				initialized = connection.rpc.call<string>("ready").finally(loan.release);
			} catch (error) { loan.release(); throw error; }
		}
		return initialized!;
	};
	const call = async <Value>(method: string, args?: unknown, decode?: (value: unknown) => Promise<Value>): Promise<Value> => {
		await ready(); return connection!.rpc.call<Value>(method, args, undefined, undefined, decode);
	};
	return { ready, call, transactions: {
		begin: async () => {
			const id = await call<number>("begin"); let settled = false;
			return { readBefore: (resource, maxBytes) => call("readBefore", { id, resource, maxBytes }), abort: async () => { if (!settled) { settled = true; await call("abort", { id }); } },
				finish: async get => { if (settled) return { complete: false, changes: [], reason: "transaction_already_settled" }; settled = true; if (get) owners.set(id, { get });
					try { return await call("finish", { id, owned: !!get }, async value => {
						const delta = value as Omit<WorkspaceTransactionDelta, "changes"> & { readonly changes: ContentChangesTransfer<WorkspaceRegularDelta> };
						return { ...delta, changes: await receiveContentChanges(delta.changes, contents) } as WorkspaceTransactionDelta;
					}); } finally { owners.delete(id); } } };
		},
		dispose: () => disposal ??= connection ? connection.rpc.call<void>("dispose").finally(() => connection!.close()) : Promise.resolve(),
	} };
}
