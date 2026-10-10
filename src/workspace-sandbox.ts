import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants, type BigIntStats, type Stats } from "node:fs";
import { access, chmod, copyFile, type FileHandle, link, lstat, mkdir, mkdtemp, open, readdir, rename, rm, rmdir, unlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { withFileMutationQueue } from "@earendil-works/pi-coding-agent";
import { containsFilesystemPath, filesystemPathKey, relativeFilesystemPath, slash } from "./path-utils.ts";
import { errorMessage, hasErrorCode, isMissing } from "./error-utils.ts";
import { createCommittedResourceInputs, type SpeculativeAgentExecutionWorld, type SpeculativeToolExecutionContext } from "./agent-execution-world.ts";
import type { WorldBranch, WorldCheckpoint, WorldExecutionMetrics } from "./execution-world.ts";
import { advanceFilesystemClock, assertNoSymlinkPath, captureFilesystemEntry, captureStableFile, fileIdentity, mapFilesystem, sameFilesystemIdentity,
	settledIdentity, sharedWalk } from "./filesystem-evidence.ts";
import { WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import { effectCommitFailure } from "./effect-transaction.ts";
import { LinuxOverlayfsCapabilityRegistry, LinuxOverlayfsUnsafeCleanupError, mountLinuxOverlayfs, openLinuxAnonymousWorkspaceFile,
	type LinuxOverlayfsMount, type LinuxOverlayfsOptions } from "./linux-overlayfs.ts";
import { captureWorkspaceStructure, captureWorkspaceStructureEntry, statChangeDigest, workspaceStructureSnapshot, directoryEntriesDigest,
	type WorkspaceStructureEntry, type WorkspaceStructureSnapshot } from "./process-observation.ts";
import { ResourceVersionManager, type ResourceChangeSet, type ResourceVersionToken, type ResourceInput } from "./resource-version.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { FILESYSTEM_OBSERVATION_FIELDS, type FilesystemObservationEvidence } from "./provenance-certificate.ts";
import type { ResourceValidation } from "./settlement.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import { deferredWorkspaceTransactionDriver, orderWorkspaceChanges, type WorkspaceRegularDelta, type WorkspaceStructureDriver,
	type WorkspaceTransactionCapture, type WorkspaceTransactionDelta, type WorkspaceTransactionDriver, type WorkspaceTransactionOwnership } from "./workspace-transaction.ts";

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

export interface SandboxDirectoryState {
	readonly entriesDigest: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>["entriesDigest"];
	readonly mode: number;
	readonly uid: number;
	readonly gid: number;
}

export interface SandboxDirectoryChange extends SandboxChangeTarget {
	readonly kind: "directory";
	/** Preserve native creation policy; private directory permissions are not Actor metadata. */
	readonly operation?: "mkdir";
	readonly before?: SandboxDirectoryState;
	readonly after?: SandboxDirectoryState;
}

export type SandboxWorkspaceChange = SandboxFileChange | SandboxDirectoryChange;

interface RegularFileState { readonly content: Uint8Array; readonly mode: number; readonly identity?: BigIntStats; readonly settled?: string; }

export interface SandboxExecutionDelta { readonly output: ToolSettlement; readonly changes: readonly SandboxWorkspaceChange[]; }

interface WorkspaceExecutionSnapshot extends SandboxExecutionDelta { readonly executionMetrics: WorldExecutionMetrics; }

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
	readonly sourceRoot: string;
	readonly sandboxRoot: string;
	readonly processRoot: string;
	/** Native metadata for pinned snapshot objects; shared by every process in this private workspace. */
	readonly metadataImage?: string;
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

interface PrivateSandboxWorkspace extends SandboxWorkspaceContext {
	readonly indexGit: ReturnType<typeof bindGit>;
	readonly pool: PooledGitRepository;
	readonly commit: string;
	readonly baselineFrontier: Map<string, RegularFileState | undefined>;
	/** A resource's state in the lower layer the workspace started from. */
	readonly readBase: (resource: string, maxBytes: number) => Promise<RegularFileState | undefined>;
	/** A live lower's structure as the workspace started from it, in place of a baseline commit. */
	readonly liveBase?: WorkspaceStructureSnapshot;
	readonly openTransactionClock: () => Promise<FileHandle>;
	readonly transactionClockLinks: 0 | 1;
	/** Native roots whose timestamp domain is projected through the workspace view. */
	readonly transactionClockRoots: readonly string[];
	readonly dispose: () => Promise<void>;
	readonly overlay?: LinuxOverlayfsMount;
	readonly sharedBaseline?: SharedOverlayBaseline;
	/** Filesystem-tool worktrees record every path they write; once settled, the pool resets them instead of checking out anew. */
	readonly recycle?: { readonly written: string[]; settled: boolean };
}

interface WorkspaceSandboxState {
	readonly repositories: Map<string, Promise<PooledGitRepository>>;
	/** Checkpoint identity and bytes belong to one service, including its sibling execution worlds. */
	readonly checkpoints: WeakMap<WorldCheckpoint, WorkspaceCheckpoint>;
	readonly lifetime: RuntimeLifecycleLane;
	readonly overlayfsCapabilities: LinuxOverlayfsCapabilityRegistry;
}

interface PooledGitRepository {
	readonly owner: WorkspaceSandboxState;
	readonly sourceRoot: string;
	readonly parent: string;
	readonly gitBinary: string;
	readonly git: ReturnType<typeof bindGit>;
	readonly index: ReturnType<typeof bindGit>;
	readonly versions: ResourceVersionManager;
	baseline?: { readonly commit: string; readonly tree: string; readonly version: ResourceVersionToken; readonly aliases: readonly (readonly string[])[] };
	/** The workspace's own structure, reused while its version reports no change. */
	liveBase?: { readonly version: ResourceVersionToken; readonly structure: WorkspaceStructureSnapshot };
	active: number;
	readonly idleWaiters: Set<() => void>;
	lock: Promise<void>;
	/** A checked-out baseline: its commit and hard-link groups (Git trees record neither the groups nor their absence). */
	prepared?: { readonly commit: string; readonly aliases: string; readonly workspace: Promise<PreparedGitWorkspace> };
	spare?: { readonly workspace: PreparedGitWorkspace; readonly written: readonly string[] };
	readonly overlayBaselines: Map<string, Promise<SharedOverlayBaseline>>;
	autoDriverDecision?: AutoWorkspaceDriverDecision;
	/** Unsafe live-mount storage is detached from allocation and retained for OS-level recovery. */
	quarantined: boolean;
	registration?: Promise<PooledGitRepository>;
	idleTimer?: ReturnType<typeof setTimeout>;
	disposal?: Promise<void>;
	prunedAt?: number;
}

interface PreparedGitWorkspace {
	readonly sandboxRoot: string;
	readonly processRoot: string;
	readonly commit: string;
	readonly gitDirectory: string;
	readonly aliases: readonly (readonly string[])[];
	readonly dispose: () => Promise<void>;
}

interface SharedOverlayBaseline extends PreparedGitWorkspace { structure?: Promise<WorkspaceStructureSnapshot>; active: number; readonly modes: ReadonlyMap<string, number>; }

interface AutoWorkspaceDriverDecision {
	readonly baseline: string | WorkspaceStructureSnapshot;
	readonly capabilityFingerprint: string;
	readonly resolved: QualifiedWorkspaceSandboxDriver;
}


function regularStructureTransitions(before: WorkspaceStructureSnapshot, after: WorkspaceStructureSnapshot): { readonly complete: true; readonly paths: readonly string[] } | { readonly complete: false; readonly reason: string } {
	if (!before.complete || !after.complete) return { complete: false, reason: "workspace_structure_limit" };
	const paths: string[] = [];
	for (const relativePath of [...new Set([...before.entries.keys(), ...after.entries.keys()])].sort()) {
		if (!relativePath) continue;
		const previous = before.entries.get(relativePath);
		const current = after.entries.get(relativePath);
		if (sameChangeIdentity(previous, current)) continue;
		if ((previous === undefined || previous.kind === "file") && (current === undefined || current.kind === "file")) {
			paths.push(relativePath);
			continue;
		}
		if ((previous === undefined || previous.kind === "directory") && (current === undefined || current.kind === "directory")) { continue; }
		return { complete: false, reason: `unsupported_workspace_transition:${relativePath}` };
	}
	return { complete: true, paths: Object.freeze(paths) };
}

function sameChangeIdentity(left: WorkspaceStructureEntry | undefined, right: WorkspaceStructureEntry | undefined): boolean {
	return left === right || (!!left && !!right && left.kind === right.kind && left.changeDigest === right.changeDigest);
}

function sameWorkspaceChangeSnapshot(left: WorkspaceStructureSnapshot, right: WorkspaceStructureSnapshot): boolean {
	if (!left.complete || !right.complete || left.entries.size !== right.entries.size) return false;
	for (const [relativePath, entry] of left.entries) { if (!sameChangeIdentity(entry, right.entries.get(relativePath))) return false; }
	return true;
}

// The execution world mirrors everything the actor can read below cwd. Git metadata is
// replaced by the private repository and commit's own temporary files are internal.
const SNAPSHOT_EXCLUDES = [".git"] as const;
const SANDBOX_REPOSITORY_IDLE_MS = 5 * 60 * 1000;
/** Replaced baselines stay referenced only by live workspaces; aged loose objects are the rest of their copies. */
const SANDBOX_PRUNE_INTERVAL_MS = 5 * 60 * 1000;
const WORKSPACE_TRANSACTION_MAX_BYTES = 512 * 1024 * 1024;
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

interface WorkspaceCheckpoint {
	readonly token: WorldCheckpoint;
	readonly sourceRoot: string;
	readonly parent?: WorkspaceCheckpoint;
	readonly changes: readonly SandboxWorkspaceChange[];
	committed: boolean;
}

function acceptsWorkspaceCheckpoint(state: WorkspaceSandboxState, checkpoint: WorldCheckpoint, cwd: string): boolean {
	const owned = state.checkpoints.get(checkpoint);
	return !state.lifetime.sealed && !!owned && filesystemPathKey(owned.sourceRoot) === filesystemPathKey(path.resolve(cwd));
}

function resolveWorkspaceCheckpoint(state: WorkspaceSandboxState, checkpoint: WorldCheckpoint | undefined, sourceRoot: string): WorkspaceCheckpoint | undefined {
	if (checkpoint === undefined) return undefined;
	const owned = state.checkpoints.get(checkpoint);
	if (!owned) throw new Error("Execution world checkpoint belongs to another backend.");
	if (filesystemPathKey(owned.sourceRoot) !== filesystemPathKey(sourceRoot)) throw new Error("Execution world checkpoint belongs to another workspace.");
	return owned;
}

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
	private readonly state: WorkspaceSandboxState = {
		repositories: new Map(),
		checkpoints: new WeakMap(),
		lifetime: new RuntimeLifecycleLane(),
		overlayfsCapabilities: new LinuxOverlayfsCapabilityRegistry(),
	};

	async fingerprint(options: WorkspaceSandboxOptions = {}, sourceRoot?: string): Promise<string> {
		assertWorkspaceSandboxOpen(this.state);
		return (await resolveWorkspaceDriver(this.state, options, sourceRoot)).fingerprint;
	}

	async qualify(options: WorkspaceSandboxOptions, sourceRoot: string): Promise<QualifiedWorkspaceSandboxDriver> {
		assertWorkspaceSandboxOpen(this.state);
		return await resolveWorkspaceDriver(this.state, options, sourceRoot);
	}

	createExecutionWorld(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
		assertWorkspaceSandboxOpen(this.state);
		return createWorkspaceSandboxFor(this.state, options);
	}

	acceptsCheckpoint(checkpoint: WorldCheckpoint, cwd: string): boolean { return acceptsWorkspaceCheckpoint(this.state, checkpoint, cwd); }

	async prepare(cwd: string, options: PrepareSandboxWorkspaceOptions = {}): Promise<QualifiedWorkspaceSandboxDriver> {
		assertWorkspaceSandboxOpen(this.state);
		return await prepareSandboxWorkspaceFor(this.state, cwd, options);
	}

	async fork(options: SandboxWorkspaceBranchOptions): Promise<WorldBranch<ToolSettlement>> {
		assertWorkspaceSandboxOpen(this.state);
		return await forkSandboxWorkspaceFor(this.state, options);
	}

	async withWorkspace<T>(cwd: string, run: (workspace: SandboxWorkspaceContext) => Promise<T>, options: Pick<WorkspaceSandboxOptions, "gitBinary" | "liveLower"> | string = {}): Promise<T> {
		assertWorkspaceSandboxOpen(this.state);
		const config = typeof options === "string" ? { gitBinary: options } : options;
		return await withPrivateSandboxWorkspace(this.state, cwd, config.gitBinary ?? "git", "git", config, run);
	}

	async commitDelta(delta: SandboxExecutionDelta): Promise<ToolSettlement> {
		assertWorkspaceSandboxOpen(this.state);
		return commitSandboxExecution(this.state, { output: delta.output, changes: ownSandboxChanges(delta.changes) });
	}

	closePools(roots?: readonly string[]): Promise<void> { return closeWorkspaceSandboxPoolsFor(this.state, roots); }

	dispose(): Promise<void> {
		return this.state.lifetime.close(async () => {
			await this.state.lifetime.drain();
			try { await closeWorkspaceSandboxPoolsNow(this.state); }
			finally { this.state.overlayfsCapabilities.dispose(); }
		});
	}
}

async function resolveWorkspaceDriver(state: WorkspaceSandboxState, options: WorkspaceSandboxOptions, sourceRoot?: string,
	acquiredRepository?: PooledGitRepository): Promise<QualifiedWorkspaceSandboxDriver> {
	const requested = options.driver ?? "auto";
	if (requested === "git") return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
	const capability = await state.overlayfsCapabilities.capability({
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

	const repository = acquiredRepository ?? await acquireSandboxRepository(state, path.resolve(sourceRoot), options.gitBinary ?? "git");
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
		if (!acquiredRepository) releaseSandboxRepository(repository);
	}
}

function createWorkspaceSandboxFor(state: WorkspaceSandboxState, options: WorkspaceSandboxOptions): SpeculativeAgentExecutionWorld {
	// Generic mutations retain Git unless OverlayFS was explicitly requested. Process routes
	// qualify a live lower from their invocation cwd to preserve native metadata.
	const resolvedOptions: WorkspaceSandboxOptions = options.driver === "overlayfs" ? options : { ...options, driver: "git" };
	const roots = new Set<string>();
	return { id: "git_worktree", scope: "fallback", isolation: "workspace_branch", speculation: {
			capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
			acceptsCheckpoint: (checkpoint, cwd) => acceptsWorkspaceCheckpoint(state, checkpoint, cwd),
			fingerprint: async ({ action }) => {
				assertWorkspaceSandboxOpen(state);
				if (action && !(action.executionContext as ToolInvocation | undefined)?.filesystem) throw new Error("Workspace execution requires an explicitly bound filesystem operation");
				return (await resolveWorkspaceDriver(state, resolvedOptions)).fingerprint;
			},
			prepare: async ({ cwd, signal }) => {
				assertWorkspaceSandboxOpen(state);
				throwIfAborted(signal);
				if (resolvedOptions.inPlaceMutations !== false) return;
				roots.add(path.resolve(cwd));
				await prepareSandboxWorkspaceFor(state, cwd, { ...resolvedOptions, signal });
			},
			execute: async (context) => {
				assertWorkspaceSandboxOpen(state);
				const sourceRoot = path.resolve(context.cwd);
				roots.add(sourceRoot);
				return executeMutation(state, context, resolvedOptions);
			},
		},
		dispose: async () => { const ownedRoots = [...roots]; roots.clear(); await closeWorkspaceSandboxPoolsFor(state, ownedRoots); },
	};
}

function workspaceBranch(snapshot: WorkspaceExecutionSnapshot, sourceRoot: string, action: SpeculativeToolExecutionContext["action"],
	owner: WorkspaceSandboxState, parent?: WorkspaceCheckpoint, validate?: () => Promise<ResourceValidation>): WorldBranch<ToolSettlement> {
	const { changes } = snapshot, { executionFingerprint } = action, backend = "git_worktree", id = randomUUID();
	const checkpoint = Object.freeze({ backend, id, lineage: parent?.token.lineage ?? id, depth: (parent?.token.depth ?? -1) + 1 });
	const retained = { token: checkpoint, sourceRoot, parent, changes, committed: false };
	owner.checkpoints.set(checkpoint, retained);
	let commitPromise: Promise<ToolSettlement> | undefined;
	const inputs = new Map<string, ResourceInput>();
	let disposed = false, readInputs = false, transferred: ReturnType<NonNullable<WorldBranch<ToolSettlement>["takeCommittedInputs"]>> | undefined;
	return {
		backend, checkpoint, output: snapshot.output, validate,
		resources: Object.freeze([...new Set(changes.filter((change) => !change.validationOnly).map((change) => change.resource))]),
		capturedBytes: changes.reduce((total, change) => total + sandboxChangeBytes(change), 0),
		executionMetrics: Object.freeze({ ...snapshot.executionMetrics }),
		compatibility: Object.freeze({ status: "compatible" as const, backend, executionFingerprint }),
		commit: () => commitPromise ??= commitSandboxExecution(owner, snapshot, inputs).then(output => { retained.committed = true; if (disposed) inputs.clear(); return output; }),
		takeReadInputs: async (maxBytes) => {
			// Every file this branch read keeps its pre-image until commit; each serves reads while it stays unchanged.
			const preimages = new Map<string, ResourceInput>(changes.flatMap((change) => change.kind !== "directory" && change.before && !change.aliases && textual(change.before) ? [[change.target, change.before]] : []));
			if (disposed || commitPromise || readInputs || !preimages.size) return undefined;
			readInputs = true;
			const inputs = await createCommittedResourceInputs(snapshot.output, action, sourceRoot, preimages, maxBytes).catch(() => undefined);
			// Stamped only now: each file must still hold the bytes it was read with, or the owner would vouch for stale ones.
			const current = inputs && await Promise.all([...preimages].map(async ([target, before]) => sameOptionalBytes((await readRegularState(target))?.content, before as Uint8Array)));
			if (inputs && current?.every(Boolean)) return inputs;
			await inputs?.dispose(); return undefined;
		},
		takeCommittedInputs: async (maxBytes) => {
			if (disposed || !retained.committed || transferred) return undefined;
			return transferred = (async () => {
				if (!inputs.size) return undefined;
				try {
					const branch = await createCommittedResourceInputs(snapshot.output, action, sourceRoot, inputs, maxBytes);
					if (!disposed) return branch;
					await branch.dispose(); return undefined;
				} finally { inputs.clear(); }
			})();
		},
		dispose() { disposed = true; inputs.clear(); return transferred?.then(() => {}, () => {}); },
	};
}

async function commitSandboxExecution(state: WorkspaceSandboxState, execution: SandboxExecutionDelta,
	inputs?: Map<string, ResourceInput>): Promise<ToolSettlement> {
	assertWorkspaceSandboxOpen(state);
	const { changes } = execution;
	const commit = withCommitLocks(
		commitLockTargets(changes),
		async () => {
			const staged = new Map<SandboxFileChange, string>(), descriptors = new Map<SandboxFileChange, FileHandle>();
			const baselines = new Map<SandboxWorkspaceChange, RegularFileState | SandboxDirectoryState | undefined>();
			const applied: SandboxWorkspaceChange[] = [], createdDirectories: string[] = [], parents = new Set<string>();
			const objects = new Map<string, { source: SandboxFileChange; write?: SandboxFileChange; handle?: FileHandle; mode?: number }>();
			let nativeStarted = false;
			try {
				// Nothing has moved yet: every target is checked at this one moment, over one walk of the directories they share.
				const capture = sharedWalk(); await mapFilesystem(changes, change => assertCommitTarget(change, capture));
				await mapFilesystem(changes, async (change) => {
					if (!change.validationOnly && change.kind !== "directory" && !change.operation && !change.object && change.after !== undefined) {
						staged.set(change, await stageAtomicWrite(change.after, change.afterMode, change.root));
					}
				});
				// The staged files take their times while every target is checked, concurrently; nothing moves before both finish.
				const timed = restoreModifiedTimes([...staged].map(([change, temporary]) => [temporary, change.afterModified]));
				timed.catch(() => undefined);
				await mapFilesystem(changes, async (change) => {
					// A file whose identity still stands for the bytes this change replaces needs no rereading to prove them.
					const unchanged = change.kind !== "directory" && change.beforeIdentity && change.before
						? await lstat(change.target, { bigint: true }).then(info => fileIdentity(info) === change.beforeIdentity ? info : undefined, () => undefined) : undefined;
					const current = unchanged ? { content: (change as SandboxFileChange).before!, mode: Number(unchanged.mode & 0o777n), identity: unchanged }
						: change.kind === "directory" ? await readSandboxDirectoryState(change.target) : await readRegularState(change.target);
					baselines.set(change, current);
					if (!sameSandboxBaseline(current, change)) throw new Error(`resource changed before commit: ${change.resource}`);
					if (change.accessMode) await access(change.target, change.accessMode);
					if (change.kind !== "directory" && change.aliases) {
						const identity = (current as RegularFileState | undefined)?.identity;
						if (!identity || identity.nlink !== BigInt(change.aliases.length) || !change.aliases.includes(change.target)) throw new Error("file alias set changed");
						for (const alias of change.aliases) {
							await assertNoSymlinkPath(change.root, alias);
							if (!sameFilesystemIdentity(identity, await lstat(alias, { bigint: true }))) throw new Error("file alias identity changed");
						}
					}
					if (!change.validationOnly && change.kind !== "directory" && change.operation && !change.object) {
						if (change.after === undefined) throw new Error("A content write cannot delete a file");
						const before = current as RegularFileState | undefined;
						if (before) {
							const descriptor = await open(change.target, fsConstants.O_RDWR | (fsConstants.O_NOFOLLOW ?? 0) | (fsConstants.O_NONBLOCK ?? 0));
							descriptors.set(change, descriptor);
							const identity = await descriptor.stat({ bigint: true });
							if (identity.nlink !== BigInt(change.aliases?.length ?? 1) || !before.identity || !sameFilesystemIdentity(before.identity, identity)) {
								throw new Error(`content write identity is not representable: ${change.resource}`);
							}
						}
					}
				});
				await timed;
				for (const change of changes) if (change.kind !== "directory" && change.object) {
					const reference = change.object, key = `${Number(reference.before)}:${reference.path}`;
					const source = changes.find(candidate => candidate.target === reference.path);
					if (!source || source.kind === "directory" || change.after === undefined ||
						(reference.before ? source.before === undefined : source.after === undefined || source.object)) throw new Error("file object anchor is unavailable");
					const group = objects.get(key) ?? { source };
					if (reference.before) {
						if (change.operation) {
							if (group.write && (!sameOptionalBytes(group.write.after, change.after) || group.write.afterMode !== change.afterMode)) throw new Error("inconsistent object contents");
							group.write = change;
						} else if (!sameOptionalBytes(source.before, change.after)) throw new Error("object change lacks a write");
						if (source.beforeMode !== change.afterMode) {
							if (group.mode !== undefined && group.mode !== change.afterMode) throw new Error("inconsistent object permissions");
							group.mode = change.afterMode;
						}
					} else if (!sameOptionalBytes(source.after, change.after) || source.afterMode !== change.afterMode) throw new Error("inconsistent linked contents");
					objects.set(key, group);
				}
				for (const [key, group] of objects) if (key.startsWith("1:")) {
					const before = baselines.get(group.source) as RegularFileState;
					group.handle = await open(group.source.target, (group.write ? fsConstants.O_RDWR : fsConstants.O_RDONLY) | (fsConstants.O_NOFOLLOW ?? 0) | (fsConstants.O_NONBLOCK ?? 0));
					descriptors.set(group.source, group.handle);
					const identity = await group.handle.stat({ bigint: true });
					if (!before.identity || identity.nlink !== BigInt(group.source.aliases?.length ?? 1) || !sameFilesystemIdentity(before.identity, identity)) throw new Error("file object predecessor changed");
				}
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
						const linked = await lstat(temporary, { bigint: true });
						if (linked.dev !== sourceState!.identity!.dev || linked.ino !== sourceState!.identity!.ino) throw new Error("file object changed while linking");
					}
				}
				for (const group of objects.values()) if (group.write || group.mode !== undefined) {
					nativeStarted = true;
					if (group.write) { await group.handle!.truncate(0); await group.handle!.writeFile(group.write.after!); }
					if (group.mode !== undefined && process.platform !== "win32") await group.handle!.chmod(group.mode);
				}
				for (const change of orderSandboxChanges(changes)) {
					await assertCommitTarget(change);
					if (change.kind === "directory") {
						if (!change.after) { await rmdir(change.target); applied.push(change); }
						else if (!change.before) {
							await createParentDirectories(change.root, change.target, createdDirectories, parents);
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
						await createParentDirectories(change.root, change.target, createdDirectories, parents);
						let descriptor = descriptors.get(change);
						if (descriptor) { applied.push(change); await descriptor.truncate(0); }
						else {
							// Exclusive creation already gives an empty file; only existing files need truncation.
							descriptor = await open(change.target, "wx", 0o666);
							descriptors.set(change, descriptor); applied.push(change);
						}
						await descriptor.writeFile(change.after!);
						continue;
					}
					applied.push(change);
					const temporary = staged.get(change);
					if (temporary) {
						await createParentDirectories(change.root, change.target, createdDirectories, parents);
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
				const directories = changes.filter((change): change is SandboxDirectoryChange => !change.validationOnly && change.kind === "directory" && !!change.after);
				const listed = await mapFilesystem(directories, async change => {
					let listing: { names?: readonly string[] } | undefined;
					return { same: await sameDirectoryAfter(change.target, change, inputs && (names => { listing = { names: names ?? createdEntries.get(change.target) }; })), listing };
				});
				directories.forEach((change, index) => {
					const { same, listing } = listed[index]!;
					if (!same) throw new Error(`directory changed while committing: ${change.resource}`);
					if (!inputs || !listing) return;
					const bytes = listing.names?.reduce((sum, name) => sum + name.length * 2 + 16, 0) ?? 0, retain = directoryBytes + bytes <= WORKSPACE_TRANSACTION_MAX_BYTES;
					if (retain) directoryBytes += bytes;
					inputs.set(change.target, { names: retain ? listing.names : undefined });
				});
			} catch (error) {
				inputs?.clear();
				if (nativeStarted || applied.some((change) => change.kind !== "directory" && (change.operation || change.aliases))) {
					throw effectCommitFailure(error, "poisoned", "native file write began; its effects cannot be safely replayed or rolled back");
				}
				try {
					await restoreChanges(applied, baselines);
					await removeCreatedDirectories(createdDirectories);
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
			if (inputs) for (const change of changes) if (!change.validationOnly && (change.kind === "directory" ? !change.after : textual(change.after)))
				inputs.set(change.target, change.kind === "directory" ? null : change.after ?? null);
			return execution.output;
		},
	);
	return state.lifetime.track(commit);
}

async function forkSandboxWorkspaceFor(state: WorkspaceSandboxState, options: SandboxWorkspaceBranchOptions,
	preparation: SandboxPreparation | undefined = options.validate ? options.preparation : undefined): Promise<WorldBranch<ToolSettlement>> {
	const sourceRoot = path.resolve(options.cwd);
	const parent = resolveWorkspaceCheckpoint(state, options.parentCheckpoint, sourceRoot);
	const resolvedDriver = await resolveWorkspaceDriver(state, options.driver === "auto" || options.driver === undefined ? { ...options, driver: "git" } : options);
	const snapshot = await withPrivateSandboxWorkspace(state, sourceRoot, options.gitBinary ?? "git", resolvedDriver.driver, options,
		async (workspace) => {
			const result = await options.execute(workspace);
			const captured = "output" in result ? result : { output: result, changes: await collectSandboxChanges(workspace) };
			const changes = ownSandboxChanges((await options.afterCapture?.(workspace, captured)) ?? captured.changes);
			if (workspace.recycle && "output" in result) { workspace.recycle.written.push(...writtenPaths(changes)); workspace.recycle.settled = true; }
			return { output: captured.output, changes, executionMetrics: { ...options.executionMetrics?.() } };
		},
		parent, preparation);
	return workspaceBranch(snapshot, sourceRoot, options.action, state, parent, options.validate);
}

async function prepareSandboxWorkspaceFor(state: WorkspaceSandboxState, cwd: string,
	options: PrepareSandboxWorkspaceOptions): Promise<QualifiedWorkspaceSandboxDriver> {
	throwIfAborted(options.signal);
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	throwIfAborted(options.signal);
	const repository = await acquireSandboxRepository(state, sourceRoot, options.gitBinary ?? "git");
	try {
		throwIfAborted(options.signal);
		const resolved = await resolveWorkspaceDriver(state, options.driver === "overlayfs" ? options : { ...options, driver: "git" }, sourceRoot, repository);
		throwIfAborted(options.signal);
		const live = resolved.driver === "overlayfs" && options.liveLower && await captureLiveBase(repository);
		const baseline = live ? undefined : await acquireSandboxBaseline(repository, true);
		throwIfAborted(options.signal);
		if (resolved.driver !== "overlayfs") await ensurePreparedSandbox(repository, baseline!, options.signal);
		else if (baseline) {
			const overlay = await acquireOverlayBaseline(repository, baseline);
			try { throwIfAborted(options.signal); await overlayBaselineStructure(overlay); } finally { releaseOverlayBaseline(overlay); }
		}
		throwIfAborted(options.signal);
		const prepared = Object.freeze({ ...resolved });
		if (baseline) preparedWorkspaceBaselines.set(prepared, { repository, baseline });
		return prepared;
	} finally {
		releaseSandboxRepository(repository);
	}
}

async function executeMutation(state: WorkspaceSandboxState, context: SpeculativeToolExecutionContext,
	options: WorkspaceSandboxOptions): Promise<WorldBranch<ToolSettlement>> {
	const execute = (context.action.executionContext as ToolInvocation | undefined)?.filesystem;
	if (!execute) throw new Error("Workspace execution requires an explicitly bound filesystem operation");
	const sourceRoot = path.resolve(context.cwd);
	// Root mutations keep writes in memory; hard links and chained actions need a private workspace.
	if (!context.parentCheckpoint && options.inPlaceMutations !== false) try {
		return workspaceBranch(await executeFilesystemMutation(context, execute), sourceRoot, context.action, state);
	} catch (error) { if (error !== LINKED_INPUT) throw error; }
	return forkSandboxWorkspaceFor(state, {
		cwd: sourceRoot, action: context.action, parentCheckpoint: context.parentCheckpoint, ...options,
		execute: workspace => executeFilesystemMutation(context, execute, workspace),
	}, capturedWorkspaceInputs);
}

const LINKED_INPUT = new Error("linked workspace input");

/** Both substrates capture the same operation delta; only a private workspace performs speculative writes. */
async function executeFilesystemMutation(context: SpeculativeToolExecutionContext, execute: NonNullable<ToolInvocation["filesystem"]>,
	workspace?: SandboxWorkspaceContext): Promise<WorkspaceExecutionSnapshot> {
	const sourceRoot = path.resolve(context.cwd);
	const changes = new Map<string, SandboxWorkspaceChange>(), lifetime = new RuntimeLifecycleLane();
	let bytes = 0, failure: { error: unknown } | undefined, namespace: Promise<WorkspaceStructureSnapshot> | undefined;
	// A request the tool swallowed still fails the run; requests after it returns are refused.
	const track = <T>(run: () => Promise<T>): Promise<T> => lifetime.admit(async () => {
		try { context.signal.throwIfAborted(); return await run(); } catch (error) { failure ??= { error }; throw error; }
	});
	const record = (key: string, change: SandboxWorkspaceChange) => {
		const retained = bytes - sandboxChangeBytes(changes.get(key)) + sandboxChangeBytes(change);
		if (retained > WORKSPACE_TRANSACTION_MAX_BYTES || (!changes.has(key) && changes.size >= WORKSPACE_TRANSACTION_MAX_FILES))
			throw new Error("Workspace operation exceeds its capture budget");
		changes.set(key, change); bytes = retained;
	};
	const located = async (logical: string) => {
		const relative = relativeFilesystemPath(sourceRoot, logical);
		if (relative === undefined || isSnapshotExcluded(slash(relative))) throw new Error("Filesystem operation is outside the workspace view");
		const target = path.resolve(sourceRoot, relative), file = path.resolve(workspace?.sandboxRoot ?? sourceRoot, relative);
		await Promise.all([assertNoSymlinkPath(sourceRoot, target), ...(workspace ? [assertNoSymlinkPath(workspace.sandboxRoot, file)] : [])]);
		context.signal.throwIfAborted();
		return { file, key: filesystemPathKey(target), root: sourceRoot, target, resource: slash(relative) };
	};
	const fileInput = async (logical: string) => {
		const { file, key, ...identity } = await located(logical), previous = changes.get(key);
		if (previous?.kind === "directory") throw new Error("Workspace file input changed type");
		// Reuse the preimage until a private write requires reading the new physical state.
		const before = previous && (!workspace || previous.validationOnly)
			? previous.before === undefined ? undefined : { content: previous.before, mode: previous.beforeMode! }
			: await readRegularState(file, WORKSPACE_TRANSACTION_MAX_BYTES);
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
		const before = previous && !workspace ? previous.before ?? previous.after : await readSandboxDirectoryState(file);
		// An in-memory mkdir needs only existence; a private mkdir captures its actual final directory state below.
		if (!previous) record(key, { ...identity, kind: "directory", before, ...(before ? { validationOnly: true } : {
			operation: "mkdir", ...(!workspace ? { after: { entriesDigest: directoryEntriesDigest([]), mode: 0o755, uid: 0, gid: 0 } } : {}) }) });
		return { file, key, before };
	};
	let output: ToolSettlement;
	try {
		output = await execute({
			readFile: (logical, limit) => track(async () => {
				const { captured, content } = await fileInput(logical);
				if (content === undefined) throw new Error("Workspace input does not exist");
				assertExistingInputPolicy(captured);
				return Buffer.from(content.subarray(0, limit));
			}),
			access: (logical, writable) => track(async () => {
				const { file, key } = await located(logical), known = changes.get(key), mode = fsConstants.R_OK | (writable ? fsConstants.W_OK : 0);
				const directory = !workspace && known ? known.kind === "directory" : (await lstat(file)).isDirectory();
				await (directory ? directoryInput(logical) : fileInput(logical));
				const captured = changes.get(key)!;
				assertExistingInputPolicy(captured);
				await access(file, mode);
				if (captured.before !== undefined) record(key, { ...captured, accessMode: (captured.accessMode ?? 0) | mode });
			}),
			writeFile: (logical, content) => track(async () => {
				const { file, key, captured } = await fileInput(logical);
				// Even an equal-content native write performs permission checks and touches the inode.
				record(key, { ...captured, accessMode: changes.get(key)?.accessMode, validationOnly: undefined, after: Buffer.from(content), operation: "write_contents" });
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
		if (change.kind === "directory" && !change.validationOnly) record(key, { ...change, after: await readSandboxDirectoryState((await located(change.target)).file) });
		else if (change.kind !== "directory" && !change.validationOnly &&
			!sameOptionalBytes((await readRegularState((await located(change.target)).file))?.content, change.after)) {
			throw new Error("Workspace writes did not settle to their declared bytes");
		}
	}
	const captured = [...changes.values()];
	return { output, changes: workspace ? captured : ownSandboxChanges(captured), executionMetrics: {} };
}

async function createPrivateSandboxWorkspace(state: WorkspaceSandboxState, cwd: string, gitBinary: string,
	driver: Exclude<WorkspaceSandboxDriver, "auto">, overlayOptions: WorkspaceSandboxOptions,
	preparation?: SandboxPreparation): Promise<PrivateSandboxWorkspace> {
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	const pool = await acquireSandboxRepository(state, sourceRoot, gitBinary);
	let attached: PreparedGitWorkspace | undefined;
	let sharedBaseline: SharedOverlayBaseline | undefined;
	let overlay: LinuxOverlayfsMount | undefined;
	let overlayStorageRoot: string | undefined, cursor: ResourceVersionToken | undefined;
	let metadata: Awaited<ReturnType<typeof preserveSnapshotMetadata>> | undefined;
	let workspace!: PrivateSandboxWorkspace;
	let disposal: Promise<void> | undefined;
	const dispose = (unsafe = false): Promise<void> => disposal ??= (async () => {
		const failures: unknown[] = [];
		await workspace?.transactions.dispose().catch((error) => failures.push(error));
		await metadata?.dispose().catch(error => failures.push(error));
		await overlay?.close().catch((error) => { unsafe = true; failures.push(error); });
		if (unsafe) {
			// A live mount retains upper/work/lower storage; quarantine it rather than recycling its roots.
			quarantineSandboxRepository(pool);
		} else {
			if (attached && workspace?.recycle?.settled) {
				await retireSandboxWorkspace(pool, attached, workspace.recycle.written).catch((error) => failures.push(error));
				if (pool.baseline) prepareNextSandbox(pool, pool.baseline);
			} else await attached?.dispose().catch((error) => failures.push(error));
			if (overlayStorageRoot) await removeOwnedTree(overlayStorageRoot).catch((error) => failures.push(error));
			if (sharedBaseline) releaseOverlayBaseline(sharedBaseline);
			cursor?.release();
			releaseSandboxRepository(pool);
		}
		if (failures.length) throw new AggregateError(failures, "sandbox workspace cleanup failed");
	})();
	try {
		const prepared = typeof preparation === "object" ? preparedWorkspaceBaselines.get(preparation) : undefined;
		// A live lower is the workspace itself: what changed since now stands in for a baseline of its bytes.
		cursor = driver === "overlayfs" && overlayOptions.liveLower ? await pool.versions.observeChanges() : undefined;
		const liveBase = cursor && await captureLiveBase(pool);
		const baseline = liveBase ? { commit: "", tree: "", version: cursor!, aliases: [] } : prepared?.repository === pool && typeof preparation === "object" &&
			preparation.driver === driver ? prepared.baseline : await acquireSandboxBaseline(pool, preparation === capturedWorkspaceInputs);
		const { commit } = baseline;
		let sandboxRoot: string, processRoot: string, gitDirectory: string, openTransactionClock: () => Promise<FileHandle>;
		let transactionClockLinks: 0 | 1, transactionClockRoots: readonly string[], overlayDevice: string | undefined;
		const observationExcludes: readonly string[] = SNAPSHOT_EXCLUDES;
		if (driver === "overlayfs") {
			// Only a snapshot lower needs the baseline checked out.
			if (!liveBase) sharedBaseline = await acquireOverlayBaseline(pool, baseline);
			overlayStorageRoot = await mkdtemp(path.join(pool.parent, "overlay-storage-"));
			processRoot = path.join(overlayStorageRoot, "process");
			await mkdir(processRoot);
			// Lower-layer copy-up can split hardlinks. Materialize only shared objects in the private upper layer, with the times
			// the lower shows: build tools compare them.
			const directories = new Set<string>(), lowerRoot = liveBase ? sourceRoot : sharedBaseline!.sandboxRoot;
			// A FUSE mount root has the private upper's identity. Keep a live workspace below it so root stat observations
			// see the original directory, just as its children do, until the operation actually changes that directory.
			const lowerName = liveBase ? path.basename(lowerRoot) : "", upper = path.join(overlayStorageRoot, "upper", lowerName);
			const aliasGroups = liveBase ? [...new Map([...liveBase.entries.values()].flatMap(entry => entry.kind === "file" && entry.aliases
				? [[entry.aliases.join("\0"), entry.aliases.map(name => path.relative(liveBase!.root, name))] as const] : [])).values()] : baseline.aliases;
			for (const aliases of aliasGroups) {
				for (const name of aliases) {
					for (let parent = path.dirname(name); parent !== "."; parent = path.dirname(parent)) directories.add(parent);
					await mkdir(path.dirname(path.join(upper, name)), { recursive: true });
				}
				await copyFile(path.join(lowerRoot, aliases[0]!), path.join(upper, aliases[0]!), fsConstants.COPYFILE_FICLONE);
				for (const alias of aliases.slice(1)) await link(path.join(upper, aliases[0]!), path.join(upper, alias));
			}
			const copied = ["", ...directories, ...aliasGroups.flat()];
			if (!liveBase && aliasGroups.length) await preserveSnapshotMetadata(lowerRoot, upper, processRoot, false, await captureSnapshotMetadata(lowerRoot, copied));
			const mounted = await mountLinuxOverlayfs({ lowerRoot: lowerName ? path.dirname(lowerRoot) : lowerRoot,
				privateRoot: overlayStorageRoot, options: overlayOptions, capabilityRegistry: state.overlayfsCapabilities });
			overlay = { ...mounted, root: path.join(mounted.root, lowerName), upperRoot: upper };
			// FUSE overlays count a file's links by the names they have loaded: list each alias's directory before anything stats one.
			for (const directory of new Set(aliasGroups.flat().map(name => path.dirname(name)))) await readdir(path.join(overlay.root, directory));
			overlayDevice = String((await lstat(overlay.root, { bigint: true })).dev);
			sandboxRoot = overlay.root;
			if (liveBase && aliasGroups.length) metadata = await preserveSnapshotMetadata(sourceRoot, sandboxRoot, processRoot, true,
				await captureSnapshotMetadata(sourceRoot, copied));
			gitDirectory = sharedBaseline?.gitDirectory ?? path.join(overlayStorageRoot, "no-index"); // OverlayFS journals its own changes.
			openTransactionClock = () => openLinuxAnonymousWorkspaceFile(mounted.upperRoot); transactionClockLinks = 0;
			// A live lower may sit on another filesystem; its files' own identities, checked on every read, fence it instead.
			transactionClockRoots = Object.freeze([...liveBase ? [] : [sharedBaseline!.sandboxRoot], mounted.upperRoot, mounted.workRoot]);
		} else {
			const aliases = JSON.stringify(baseline.aliases);
			const prepared = (await takePreparedSandbox(pool, commit, aliases)) ?? (await attachSandboxWorkspace(pool, baseline));
			// The next speculated action's checkout (the whole tree, ignored dependencies included) runs while this one does;
			// a filesystem-tool worktree is itself reset for the next one when it returns.
			if ((overlayOptions as Partial<SandboxWorkspaceBranchOptions>).action && preparation !== capturedWorkspaceInputs) prepareNextSandbox(pool, baseline);
			attached = prepared;
			({ sandboxRoot, processRoot, gitDirectory } = prepared);
			metadata = await preserveSnapshotMetadata(sourceRoot, sandboxRoot, processRoot, process.platform === "linux" && !!overlayOptions.liveLower);
			const transactionClockPath = path.join(prepared.processRoot, "workspace-transaction.clock");
			const noFollow = typeof fsConstants.O_NOFOLLOW === "number" ? fsConstants.O_NOFOLLOW : 0;
			openTransactionClock = () => open(transactionClockPath, fsConstants.O_WRONLY | fsConstants.O_CREAT | noFollow, 0o600);
			transactionClockLinks = 1;
			transactionClockRoots = Object.freeze([prepared.sandboxRoot, prepared.processRoot]);
		}
		const baselineFrontier = new Map<string, RegularFileState | undefined>();
		const structure: WorkspaceStructureDriver = {
			capture: () => {
				if (!workspace.overlay) {
					return captureWorkspaceStructure(workspace.sandboxRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: workspace.observationExcludes });
				}
				if (!liveBase && !workspace.sharedBaseline) throw new Error("OverlayFS shared baseline is unavailable");
				return (liveBase ? Promise.resolve(liveBase) : overlayBaselineStructure(workspace.sharedBaseline!)).then((baseline) =>
					captureOverlayWorkspaceStructure(workspace, baseline, overlayDevice!),
				);
			},
		};
		const transactions = deferredWorkspaceTransactionDriver(() => createGitWorkspaceTransactionDriver(workspace));
		const sourceDevice = (await lstat(sourceRoot, { bigint: true })).dev;
		workspace = {
			sourceRoot, sandboxRoot, processRoot, observationExcludes, structure, transactions, pool, commit, baselineFrontier,
			...(metadata ? { metadataImage: metadata.image } : {}),
			...(metadata?.image ? { metadataObjects: metadata.objects } : {}),
			projectMetadata: (stat) => ({ ...metadata?.project(stat) ?? stat, dev: sourceDevice }),
			captureChanges: frontier => collectSandboxChanges(workspace, frontier),
			sourceChanges: () => pool.versions.changesSince(baseline.version),
			indexGit: bindGit(gitBinary, sandboxRoot, ["--git-dir", gitDirectory, "--work-tree", sandboxRoot]),
			readBase: liveBase ? (resource, maxBytes) => readLiveBase(liveBase!, resource, maxBytes) : async (resource, maxBytes) => {
				const state = await readGitTreeRegularState(pool.git, commit, resource, maxBytes), mode = (metadata?.modes ?? sharedBaseline?.modes)?.get(slash(resource));
				return state && mode !== undefined ? { ...state, mode } : state;
			},
			...(liveBase ? { liveBase } : {}),
			openTransactionClock, transactionClockLinks, transactionClockRoots, dispose,
			...(overlay ? { overlay } : {}),
			...(sharedBaseline ? { sharedBaseline } : {}),
			...(attached && preparation === capturedWorkspaceInputs ? { recycle: { written: [], settled: false } } : {}),
		};
		return workspace;
	} catch (error) {
		try {
			await dispose(error instanceof LinuxOverlayfsUnsafeCleanupError);
		} catch (cleanupError) {
			throw new AggregateError([error, cleanupError], "sandbox creation and cleanup both failed");
		}
		throw error;
	}
}

/** Git owns bytes and links, while an OS snapshot also owns empty directories and object metadata.
 * Pin every translated object until disposal: unlink/recreate must never reuse a saved inode mapping. */
const SNAPSHOT_METADATA_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "blksize", "blocks", "atimeNs", "mtimeNs", "ctimeNs", "birthtimeNs"] as const;
async function captureSnapshotMetadata(sourceRoot: string, copied?: readonly string[]) {
	const original = new Map<string, BigIntStats>();
	if (copied) await mapFilesystem([...new Set(copied)], async resource => { original.set(resource, await lstat(path.join(sourceRoot, resource), { bigint: true })); });
	else if (!(await captureWorkspaceStructure(sourceRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: SNAPSHOT_EXCLUDES,
		observeStat: (resource, stat) => { original.set(resource, stat); } })).complete) throw new Error("snapshot metadata namespace is incomplete");
	return original;
}
async function preserveSnapshotMetadata(sourceRoot: string, sandboxRoot: string, processRoot: string, native: boolean, captured?: ReadonlyMap<string, BigIntStats>) {
	const original = captured ?? await captureSnapshotMetadata(sourceRoot), handles: FileHandle[] = [];
	const dispose = async () => {
		const closed = await Promise.allSettled(handles.splice(0).map(handle => handle.close()));
		const failed = closed.flatMap(result => result.status === "rejected" ? [result.reason] : []);
		if (failed.length) throw new AggregateError(failed, "snapshot metadata pins did not close");
	};
	try {
		const names = [...original.keys()].sort((a, b) => a.length - b.length);
		// Check each parent before populating its children: a stale checkout may contain a symlink in its place.
		for (const resource of names) if (original.get(resource)!.isDirectory()) {
			const target = path.join(sandboxRoot, resource), current = await lstat(target).catch(error => { if (isMissing(error)) return undefined; throw error; });
			if (!current) await mkdir(target);
			else if (!current.isDirectory()) throw new Error(`snapshot directory type changed: ${resource}`);
		}
		const rows: [BigIntStats, BigIntStats][] = [], objects = new Map<string, string>();
		const fields = SNAPSHOT_METADATA_FIELDS;
		// Set parent modes last so a read-only directory cannot prevent its own snapshot from being populated.
		for (const resource of names.reverse()) {
			const source = original.get(resource)!, target = path.join(sandboxRoot, resource);
			const current = await lstat(target, { bigint: true }).catch(error => { if (isMissing(error)) return undefined; throw error; });
			if (!current) continue; // A warmup can predate this name; exact input validation still decides adoption.
			if (objects.has(`${current.dev}:${current.ino}`)) continue;
			if ((source.mode & 0o170000n) !== (current.mode & 0o170000n)) throw new Error(`snapshot object type changed: ${resource}`);
			if (source.uid !== current.uid || source.gid !== current.gid) throw new Error(`snapshot object ownership differs: ${resource}`);
			if (!source.isSymbolicLink()) { await chmod(target, Number(source.mode & 0o7777n)); await utimes(target, source.atime, source.mtime); }
			objects.set(`${current.dev}:${current.ino}`, `${source.dev}:${source.ino}`);
			if (!native) continue;
			const handle = await open(target, 0x200000 | fsConstants.O_NOFOLLOW); // Linux O_PATH also pins symlinks and unreadable objects.
			handles.push(handle);
			const physical = await handle.stat({ bigint: true });
			rows.push([physical, source]);
		}
		const image = native ? path.join(processRoot, "metadata.json") : undefined;
		if (image) {
			const parent = await open(path.dirname(sandboxRoot), 0x200000 | fsConstants.O_NOFOLLOW); handles.push(parent);
			const physicalParent = await parent.stat({ bigint: true }), sourceParent = await lstat(path.dirname(sourceRoot), { bigint: true });
			rows.push([physicalParent, sourceParent]);
			await writeFile(image, JSON.stringify(rows.map(row => row.map(stat => fields.map(field => String(stat[field]))))), { flag: "wx", mode: 0o600 });
		}
		const mappings = new Map(rows.map(row => [`${row[0].dev}:${row[0].ino}`, row]));
		return { image, objects, project: (stat: FilesystemObservationEvidence): FilesystemObservationEvidence => {
			const row = mappings.get(`${stat.dev}:${stat.ino}`); if (!row) return stat;
			const [physical, source] = row, links = source.nlink + stat.nlink - physical.nlink, unchanged = stat.size === physical.size && stat.mtimeNs === physical.mtimeNs && stat.ctimeNs === physical.ctimeNs;
			return { ...Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field,
				stat[field] === physical[field] && (field !== "blocks" || unchanged) ? source[field] : stat[field]])),
				dev: source.dev, ino: source.ino, nlink: links < 0n ? 0n : links } as FilesystemObservationEvidence;
		}, modes: new Map([...original].map(([name, stat]) => [slash(name), process.platform === "win32" ? 0 : Number(stat.mode & 0o777n)])), dispose };
	} catch (error) { await dispose(); throw error; }
}

async function createGitWorkspaceTransactionDriver(workspace: PrivateSandboxWorkspace): Promise<WorkspaceTransactionDriver> {
	// An interval that overlapped another keeps its own before-state: the frontier it began with, what was unreadable then,
	// and what changed while its start was being fenced. Its writer's trace later names which changes are its own.
	interface Capture {
		contaminated: boolean; overlapped: boolean; readonly before?: WorkspaceStructureSnapshot;
		readonly frontier: ReadonlyMap<string, RegularFileState | undefined>; readonly unknown: ReadonlySet<string>; readonly racing: readonly string[];
	}
	const { sandboxRoot, openTransactionClock: openClock, transactionClockLinks: expectedClockLinks, transactionClockRoots: clockRoots } = workspace;
	// Another process linking or removing a file can leave one capture short of its names, or without a name it listed: capture again.
	const captureStructure = async (attempt = 1): Promise<WorkspaceStructureSnapshot> => {
		const last = attempt >= WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS, snapshot = await workspace.structure.capture().catch((error: unknown) => { if (last || !isMissing(error)) throw error; });
		return snapshot && (snapshot.complete || last) ? snapshot : captureStructure(attempt + 1);
	}, frontier = new Map(workspace.baselineFrontier);
	let lastStructure = await captureStructure();
	let retainedBytes = [...frontier.values()].reduce((total, state) => total + (state?.content.byteLength ?? 0), 0);
	const active = new Set<Capture>(), lock = { lock: Promise.resolve() }, unknown = new Set<string>(); // Bytes last seen while others wrote.
	let poisonReason = lastStructure.complete ? undefined : "workspace_structure_limit";
	let clock: { readonly handle: FileHandle; readonly identity: Stats } | undefined;

	if (!poisonReason) {
		try {
			await assertChangeClockFilesystem();
			await advanceChangeClock(lastStructure);
			await captureTransitions((await (workspace.overlay ? collectOverlayChangeResources(workspace) : collectGitChangeResources(workspace)))
				.filter(resource => !isSnapshotExcluded(slash(resource))),
				lastStructure, false);
			const verified = await captureStructure();
			if (!sameWorkspaceChangeSnapshot(lastStructure, verified)) { throw new Error("workspace changed while initializing transaction clock"); }
			lastStructure = verified;
		} catch (error) {
			poisonReason = `workspace_transaction_clock:${errorMessage(error)}`;
		}
	}

	const begin = (): Promise<WorkspaceTransactionCapture> =>
		withWorkspaceLock(lock, async () => {
			const overlapped = active.size > 0;
			for (const other of active) other.overlapped = true;
			let before: WorkspaceStructureSnapshot | undefined, racing: readonly string[] = [];
			if (!poisonReason) {
				try {
					if (!overlapped) before = await captureFencedBefore();
					else ({ before, racing } = await captureOpenBefore());
				} catch (error) {
					poisonReason = `workspace_transaction_sync:${errorMessage(error)}`;
				}
			}
			const capture: Capture = { contaminated: false, overlapped, before, frontier: new Map(frontier), unknown: new Set(unknown), racing };
			active.add(capture);
			return {
				readBefore: (resource, maxBytes) => withWorkspaceLock(lock, async () => {
					if (!active.has(capture) || capture.contaminated || !capture.before || capture.unknown.has(resource)) throw new Error("workspace transaction input is unavailable");
					if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) throw new Error("workspace transaction input budget is invalid");
					if (capture.before.entries.get(resource)?.kind !== "file") return undefined;
					const state = capture.frontier.has(resource) ? capture.frontier.get(resource) : await workspace.readBase(resource, maxBytes);
					if (state && state.content.byteLength > maxBytes) throw new Error("workspace transaction input exceeds capture limit");
					return state && Uint8Array.from(state.content);
				}),
				finish: (ownership) => capture.overlapped ? finishOverlapped(capture, ownership) : finish(capture), abort: () => abort(capture),
			};
		});

	async function finish(capture: Capture): Promise<WorkspaceTransactionDelta> {
		return withWorkspaceLock(lock, async () => {
			if (!active.delete(capture)) return { complete: false, changes: [], reason: "transaction_already_settled" };
			if (capture.contaminated) return { complete: false, changes: [], reason: "overlapping_workspace_transaction" };
			if (!capture.before) return { complete: false, changes: [], reason: poisonReason ?? "workspace_transaction_unavailable" };
			try {
				const observed = await captureStructure();
				await advanceChangeClock(observed);
				const after = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(observed, after)) throw new Error("workspace changed while fencing transaction endpoint");
				const transitions = regularStructureTransitions(capture.before, after);
				lastStructure = after;
				if (!transitions.complete) {
					poisonReason = transitions.reason;
					return { complete: false, changes: [], reason: transitions.reason, before: capture.before, after };
				}
				const changes = await captureTransitions(transitions.paths, after);
				const verified = await captureStructure();
				if (!sameWorkspaceChangeSnapshot(after, verified)) throw new Error("workspace changed while sealing transaction endpoint");
				lastStructure = verified;
				return { complete: true, changes, before: capture.before, after: verified };
			} catch (error) {
				const reason = `workspace_transaction_capture:${errorMessage(error)}`;
				poisonReason = reason;
				return { complete: false, changes: [], reason, before: capture.before };
			}
		});
	}

	/** While other intervals write, no start is stable: what changes around the fence is foreign to this one, and unread. */
	async function captureOpenBefore(): Promise<{ readonly before: WorkspaceStructureSnapshot; readonly racing: readonly string[] }> {
		const current = await captureStructure();
		await advanceChangeClock(current);
		const before = await captureStructure(), racing = regularStructureTransitions(current, before);
		if (!racing.complete) throw new Error(racing.reason);
		markUnread(before);
		return { before, racing: racing.paths };
	}

	function markUnread(current: WorkspaceStructureSnapshot): void {
		const moved = regularStructureTransitions(lastStructure, current);
		if (!moved.complete) { poisonReason = moved.reason; return; }
		for (const resource of moved.paths) unknown.add(resource);
		lastStructure = current;
	}

	/**
	 * Attribute an overlapped interval by its writer's trace: its changes are the paths it wrote, other changes are foreign and
	 * must not reach what it observed, and nothing may touch its own paths after it ended or while it ran.
	 */
	async function finishOverlapped(capture: Capture, ownership?: () => Promise<WorkspaceTransactionOwnership | undefined>): Promise<WorkspaceTransactionDelta> {
		const settled = await withWorkspaceLock(lock, async (): Promise<WorkspaceTransactionDelta | { readonly after: WorkspaceStructureSnapshot; readonly capturedAt: number; readonly paths: readonly string[] }> => {
			if (!active.delete(capture)) return { complete: false, changes: [], reason: "transaction_already_settled" };
			if (capture.contaminated || !ownership) return { complete: false, changes: [], reason: "overlapping_workspace_transaction" };
			if (!capture.before) return { complete: false, changes: [], reason: poisonReason ?? "workspace_transaction_unavailable" };
			try {
				const after = await captureStructure(), capturedAt = Date.now();
				markUnread(after);
				const transitions = regularStructureTransitions(capture.before, after);
				return transitions.complete ? { after, capturedAt, paths: transitions.paths }
					: { complete: false, changes: [], reason: transitions.reason, before: capture.before, after };
			} catch (error) {
				return { complete: false, changes: [], reason: `workspace_transaction_capture:${errorMessage(error)}`, before: capture.before };
			}
		});
		if ("complete" in settled) return settled;
		const { after, capturedAt, paths } = settled, before = capture.before!, owned = await ownership!().catch(() => undefined);
		const incomplete = (reason: string) => ({ complete: false, changes: [], reason, before, after });
		if (!owned) return incomplete("overlapping_workspace_transaction");
		// What lies under a directory it made or moved in (a session directory renamed on completion) is its own too.
		const mine = (resource: string): boolean => owned.written.has(resource) || resource.includes("/") && mine(path.posix.dirname(resource));
		// A foreign change reaches what it saw only if it came no later than its last look at the name or at the directory holding it.
		const seen = (resource: string) => Math.max(owned.observed.get(resource) ?? -Infinity, owned.observed.get(path.dirname(resource).replace(/^\.$/, "")) ?? -Infinity);
		for (const foreign of [...capture.racing, ...paths.filter(resource => !mine(resource))]) {
			const changed = capture.racing.includes(foreign) ? undefined : after.entries.get(foreign)?.changeTimeMs;
			if (seen(foreign) > -Infinity && (changed === undefined || Math.floor(changed) <= seen(foreign))) return incomplete(`overlapping_workspace_input:${foreign}`);
		}
		const own = paths.filter(mine);
		const late = own.find(resource => Math.floor(after.entries.get(resource)?.changeTimeMs ?? -Infinity) > owned.endedAt);
		if (late !== undefined) return incomplete(`overlapping_workspace_write:${late}`);
		if (own.length && await owned.interfered(new Set(own), capturedAt)) return incomplete("overlapping_workspace_write");
		const changes: WorkspaceRegularDelta[] = [], entries = new Map(before.entries);
		try {
			// Its own paths' endpoints are independent reads; they are judged in order, the first that fails deciding.
			const endpoints = await mapFilesystem(own, async (relativePath): Promise<string | { readonly previous?: RegularFileState; readonly current?: RegularFileState }> => {
				if (capture.unknown.has(relativePath) && before.entries.get(relativePath)?.kind === "file") return `overlapping_workspace_before:${relativePath}`;
				// Bytes last seen stand for a file only while it was there when this interval began.
				const previous = before.entries.get(relativePath)?.kind !== "file" ? undefined
					: capture.frontier.has(relativePath) ? capture.frontier.get(relativePath) : await workspace.readBase(relativePath, WORKSPACE_TRANSACTION_MAX_BYTES);
				const entry = after.entries.get(relativePath);
				if (entry?.kind !== "file") return { previous };
				const captured = await captureStableFile(path.resolve(sandboxRoot, relativePath), WORKSPACE_TRANSACTION_MAX_BYTES, true, { digest: false });
				return statChangeDigest(captured.stat) !== entry.changeDigest ? `overlapping_workspace_write:${relativePath}`
					: { previous, current: { content: captured.content!, mode: Number(captured.stat.mode & 0o777n) } };
			});
			for (const [index, relativePath] of own.entries()) {
				const endpoint = endpoints[index]!;
				if (typeof endpoint === "string") return incomplete(endpoint);
				const { previous, current } = endpoint;
				if (current) entries.set(relativePath, after.entries.get(relativePath)!); else entries.delete(relativePath);
				changes.push({ relativePath, ...(previous ? { before: previous.content, beforeMode: previous.mode } : {}),
					...(current ? { after: current.content, afterMode: current.mode } : {}) });
			}
		} catch (error) { return incomplete(`workspace_transaction_capture:${errorMessage(error)}`); }
		// Directories this interval made or removed are its own too; everything else stays as it began.
		for (const name of new Set([...before.entries.keys(), ...after.entries.keys()])) {
			const entry = after.entries.get(name), previous = before.entries.get(name);
			if (!mine(name) || (entry ?? previous)?.kind !== "directory") continue;
			if (entry) entries.set(name, entry); else entries.delete(name);
		}
		return { complete: true, changes, before, after: workspaceStructureSnapshot(after.root, entries, after.complete) };
	}

	async function captureFencedBefore(): Promise<WorkspaceStructureSnapshot> {
		for (let attempt = 0; attempt < WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS; attempt++) {
			const current = await captureStructure();
			await advanceChangeClock(current);
			const fenced = await captureStructure();
			if (!sameWorkspaceChangeSnapshot(current, fenced)) continue;
			await synchronizeFrontier(fenced);
			if (poisonReason) throw new Error(poisonReason);
			const verified = await captureStructure();
			if (!sameWorkspaceChangeSnapshot(fenced, verified)) continue;
			lastStructure = verified;
			return verified;
		}
		throw new Error("workspace did not stabilize before transaction execution");
	}

	async function assertChangeClockFilesystem(): Promise<void> {
		const handle = await openClock();
		try {
			const [workspaceInfo, clockInfo, ...rootInfo] = await Promise.all([lstat(sandboxRoot), handle.stat(), ...clockRoots.map((root) => lstat(root))]);
			if (!workspaceInfo.isDirectory() || !clockInfo.isFile() || clockInfo.nlink !== expectedClockLinks || rootInfo.length === 0 ||
				rootInfo.some((root) => !root.isDirectory() || root.dev !== clockInfo.dev)) {
				throw new Error("workspace transaction clock is not private or its backing timestamp domain changed");
			}
			clock = { handle, identity: clockInfo };
		} catch (error) { await handle.close(); throw error; }
	}

	async function advanceChangeClock(snapshot: WorkspaceStructureSnapshot): Promise<void> {
		let boundary = Number.NEGATIVE_INFINITY;
		for (const entry of snapshot.entries.values()) boundary = Math.max(boundary, entry.changeTimeMs);
		if (!Number.isFinite(boundary)) throw new Error("workspace change clock boundary is unavailable");
		if (!clock) throw new Error("workspace transaction clock is unavailable");
		await advanceFilesystemClock(clock.handle, boundary, clock.identity);
	}

	const abort = (capture: Capture): Promise<void> => withWorkspaceLock(lock, async () => { if (active.delete(capture)) for (const other of active) other.contaminated = true; });

	const dispose = (): Promise<void> => withWorkspaceLock(lock, async () => {
		for (const capture of active) capture.contaminated = true;
		active.clear();
		const closingClock = clock;
		clock = undefined;
		await closingClock?.handle.close();
	});

	async function synchronizeFrontier(current: WorkspaceStructureSnapshot): Promise<void> {
		const transitions = regularStructureTransitions(lastStructure, current);
		lastStructure = current;
		if (!transitions.complete) { poisonReason = transitions.reason; return; }
		// Quiet again: what was written while intervals overlapped is read now.
		await captureTransitions([...new Set([...transitions.paths, ...unknown])], current, false);
		unknown.clear();
	}

	async function captureTransitions(paths: readonly string[], after: WorkspaceStructureSnapshot, captureBefore = true): Promise<readonly WorkspaceRegularDelta[]> {
		// The lock and overlap rejection keep this frontier unchanged, and each file the size its snapshot took, throughout the interval: read those at once.
		const sizes = paths.map(relativePath => { const entry = after.entries.get(relativePath); return entry?.kind === "file" ? entry.size : undefined; });
		if (sizes.reduce<number>((total, size) => total + (size ?? 0), 0) > WORKSPACE_TRANSACTION_MAX_BYTES) throw new Error("workspace transaction after-state exceeds capture limit");
		const currents = await mapFilesystem([...paths.keys()], async index => sizes[index] === undefined ? undefined : readRegularState(path.resolve(sandboxRoot, paths[index]!), sizes[index]));
		const changes: WorkspaceRegularDelta[] = [];
		let beforeBytes = 0;
		for (const [index, relativePath] of paths.entries()) {
			const previous = !captureBefore ? undefined : frontier.has(relativePath) ? frontier.get(relativePath) : await workspace.readBase(relativePath, WORKSPACE_TRANSACTION_MAX_BYTES - beforeBytes);
			beforeBytes += previous?.content.byteLength ?? 0;
			if (beforeBytes > WORKSPACE_TRANSACTION_MAX_BYTES) throw new Error("workspace transaction before-state exceeds capture limit");
			const current = currents[index], unchangedBytes = retainedBytes - (frontier.get(relativePath)?.content.byteLength ?? 0);
			if (unchangedBytes + (current?.content.byteLength ?? 0) > WORKSPACE_TRANSACTION_MAX_BYTES) throw new Error("workspace transaction after-state exceeds capture limit");
			retainedBytes = unchangedBytes + (current?.content.byteLength ?? 0);
			frontier.set(relativePath, current);
			if (captureBefore) changes.push({ relativePath, ...(previous ? { before: previous.content, beforeMode: previous.mode } : {}),
				...(current ? { after: current.content, afterMode: current.mode } : {}) });
		}
		return Object.freeze(changes);
	}

	return { begin, dispose };
}

async function acquireSandboxRepository(state: WorkspaceSandboxState, sourceRoot: string, gitBinary: string): Promise<PooledGitRepository> {
	assertWorkspaceSandboxOpen(state);
	const key = `${filesystemPathKey(sourceRoot)}\0${gitBinary}`;
	let pending = state.repositories.get(key);
	if (!pending) {
		pending = createSandboxRepository(state, sourceRoot, gitBinary);
		state.repositories.set(key, pending);
		void pending.catch(() => { if (state.repositories.get(key) === pending) state.repositories.delete(key); });
	}
	const repository = await pending;
	repository.registration ??= pending;
	if (repository.quarantined) {
		if (state.repositories.get(key) === pending) state.repositories.delete(key);
		return acquireSandboxRepository(state, sourceRoot, gitBinary);
	}
	if (repository.idleTimer) { clearTimeout(repository.idleTimer); repository.idleTimer = undefined; }
	repository.active++;
	return repository;
}

async function createSandboxRepository(owner: WorkspaceSandboxState, sourceRoot: string, gitBinary: string): Promise<PooledGitRepository> {
	const parent = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-action-pool-"));
	const repository = path.join(parent, "snapshot.git");
	const git = bindGit(gitBinary, parent, ["--git-dir", repository]);
	try {
		await bindGit(gitBinary, parent)(["init", "--bare", "--template=", repository]);
		// This private snapshot stores raw bytes; caller configuration and attributes cannot transform them.
		await mkdir(path.join(repository, "info"), { recursive: true });
		await writeFile(path.join(repository, "config"), "\n[core]\n\tautocrlf = false\n\tlongpaths = true\n\tattributesFile = /dev/null\n", { flag: "a" });
		await writeFile(path.join(repository, "info", "attributes"), "* -text -eol -filter -ident -working-tree-encoding\n");
		return {
			owner, sourceRoot, parent, gitBinary, git, index: bindGit(gitBinary, sourceRoot, ["--git-dir", repository, "--work-tree", sourceRoot]),
			versions: new ResourceVersionManager(sourceRoot, { snapshotExcludes: SNAPSHOT_EXCLUDES, gitObjects: true }),
			active: 0, idleWaiters: new Set(), lock: Promise.resolve(), overlayBaselines: new Map(), quarantined: false,
		};
	} catch (error) { await rm(parent, { recursive: true, force: true }); throw error; }
}

async function acquireSandboxBaseline(repository: PooledGitRepository, warmup = false): Promise<NonNullable<PooledGitRepository["baseline"]>> {
	// The pool owns this shared baseline; callers cancel before private workspace allocation.
	return withWorkspaceLock(repository, async () => {
		const baseline = repository.baseline;
		if (baseline) {
			// Quiet notifications reuse preparation; fresh allocations check exact evidence below.
			const changes = warmup ? repository.versions.changesSince(baseline.version) : undefined;
			if (changes && !changes.uncertain && !changes.paths.length) return baseline;
			// A preparation owns an immutable namespace. Borrowers prove the captured alias set at adoption.
			if (warmup && !(await sandboxIndexChanges(repository)).length) return baseline;
			if (!warmup && [...baseline.version.observations.values()].some(entry => entry.scope === "tree_content")) {
				const [version, paths] = await Promise.all([repository.versions.validate(baseline.version), sandboxIndexChanges(repository)]);
				if (!version.expired && !paths.length) return baseline;
			}
		}
		for (let attempt = 0, rebuild = false; attempt < 3; attempt++) {
			// Preparation owns immutable bytes, not source freshness. Actual allocations capture exact
			// evidence; borrowed process preparations validate their observed inputs and effects at adoption.
			const version = await repository.versions.capture([{ path: repository.sourceRoot, scope: warmup ? "tree_entries" : "tree_content" }]);
			try {
				const aliases = [...version.observations.values()].flatMap(entry => entry.aliases ?? []).map(group => {
					if (group.paths.length !== group.links) throw new Error("workspace hardlink namespace is not closed");
					return group.paths.map(target => {
						const relative = relativeFilesystemPath(repository.sourceRoot, target);
						if (!relative || isSnapshotExcluded(slash(relative))) throw new Error("workspace hardlink escapes snapshot");
						return relative;
					});
				});
				// Events and Git stat data can both miss changes: a changed baseline stages the captured objects, or owns a fresh index.
				// A preparation proves nothing (adoption checks what it read), so it keeps the index and lets Git's stat check stage.
				if (rebuild || !baseline || !(await stageTreeObjects(repository, version))) {
					if (rebuild || !baseline || !warmup) await repository.index(["read-tree", "--empty"]);
					await repository.index(["add", "-f", "-A", "--", ...snapshotPathspecs()]);
				}
				const tree = (await repository.index(["write-tree"])).toString("utf8").trim();
				// Baselines are independent snapshots: a parent chain would keep every replaced copy reachable.
				const commit = tree === baseline?.tree && JSON.stringify(aliases) === JSON.stringify(baseline.aliases) ? baseline.commit
					: (await repository.git(["commit-tree", tree, "-m", "speculative baseline"], { environment: SANDBOX_AUTHOR_ENVIRONMENT })).toString("utf8").trim();
				if (!warmup && (await repository.versions.validate(version)).expired) continue;
				// An ABA during staging can restore captured source bytes after Git copied different bytes.
				if (!warmup && (await sandboxIndexChanges(repository)).length) { rebuild = true; continue; }
				if (commit !== baseline?.commit) await repository.git(["update-ref", "refs/heads/baseline", commit]);
				if (commit !== baseline?.commit && Date.now() >= (repository.prunedAt ?? 0) + SANDBOX_PRUNE_INTERVAL_MS) {
					repository.prunedAt = Date.now(); // The grace keeps objects that concurrent workspace staging has not referenced yet.
					await repository.git(["prune", "--expire=5.minutes.ago"]).catch(() => undefined);
				}
				repository.baseline = { commit, tree, version, aliases };
				baseline?.version.release();
				return repository.baseline;
			} finally {
				if (repository.baseline?.version !== version) version.release();
			}
		}
		throw new Error("workspace changed repeatedly while preparing sandbox baseline");
	});
}

/**
 * Stage only what differs from the previous baseline's index, by the Git objects the exact capture hashed from the same bytes.
 * Whatever `add -f -A` might stage otherwise (a gitlink, a Windows link, bytes that moved on since capture) rebuilds instead.
 */
async function stageTreeObjects(repository: PooledGitRepository, version: ResourceVersionToken): Promise<boolean> {
	if (!version.treeObjects) return false;
	const staged = new Map((await repository.index(["ls-files", "-s", "-z"])).toString("utf8").split("\0").flatMap((record) => {
		const match = /^(\d{6}) ([0-9a-f]{40}) \d\t(.+)$/su.exec(record);
		return match ? [[match[3]!, { mode: match[1]!, blob: match[2]! }] as const] : [];
	}));
	const desired = new Map<string, { readonly mode: string; readonly blob: string; readonly link?: string }>();
	for (const [target, object] of version.treeObjects) {
		const relative = relativeFilesystemPath(repository.sourceRoot, target), name = relative && slash(relative);
		if (!name || isSnapshotExcluded(name)) continue;
		if (object.link !== undefined && process.platform === "win32" || name.includes("\n")) return false;
		desired.set(name, object.link !== undefined ? { mode: "120000", blob: gitBlobID(Buffer.from(object.link)), link: object.link }
			: { mode: process.platform === "win32" ? staged.get(name)?.mode ?? "100644" : object.mode & 0o100 ? "100755" : "100644", blob: object.blob });
	}
	if ([...staged.values()].some((entry) => entry.mode === "160000")) return false;
	const changed = [...desired].filter(([name, entry]) => staged.get(name)?.mode !== entry.mode || staged.get(name)?.blob !== entry.blob);
	const files = changed.flatMap(([name, entry]) => entry.link === undefined ? [name] : []);
	const written = files.length ? (await repository.index(["hash-object", "-w", "--no-filters", "--stdin-paths"], { input: Buffer.from(`${files.join("\n")}\n`) }))
		.toString("utf8").trim().split("\n") : [];
	if (written.some((blob, index) => blob !== desired.get(files[index]!)!.blob)) return false;
	for (const [, { link }] of changed) if (link !== undefined) await repository.index(["hash-object", "-w", "--stdin"], { input: Buffer.from(link) });
	const updates = [...changed.map(([name, entry]) => `${entry.mode} ${entry.blob}\t${name}\0`),
		...[...staged.keys()].filter((name) => !desired.has(name)).map((name) => `0 ${"0".repeat(40)}\t${name}\0`)];
	if (updates.length) await repository.index(["update-index", "-z", "--index-info"], { input: Buffer.from(updates.join("")) });
	return true;
}

function gitBlobID(bytes: Buffer): string {
	return createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
}

async function ensurePreparedSandbox(repository: PooledGitRepository, baseline: NonNullable<PooledGitRepository["baseline"]>, signal?: AbortSignal): Promise<void> {
	const { commit } = baseline, aliases = JSON.stringify(baseline.aliases);
	const existing = repository.prepared;
	if (existing?.commit === commit && existing.aliases === aliases) { await existing.workspace; return; }
	const stale = await takePreparedSandbox(repository);
	if (stale) await retireSandboxWorkspace(repository, stale);
	throwIfAborted(signal);
	const pending = repository.prepared ??= { commit, aliases, workspace: attachSandboxWorkspace(repository, baseline) };
	try { await pending.workspace; } catch (error) { if (repository.prepared === pending) repository.prepared = undefined; throw error; }
}

async function takePreparedSandbox(repository: PooledGitRepository, commit?: string, aliases?: string): Promise<PreparedGitWorkspace | undefined> {
	const pending = repository.prepared;
	if (!pending) return undefined;
	// Claim the slot before waiting: execution, replacement and shutdown cannot retire the same workspace.
	repository.prepared = undefined;
	try {
		const prepared = await pending.workspace;
		if (commit === undefined || prepared.commit === commit && (aliases === undefined || pending.aliases === aliases)) return prepared;
		await retireSandboxWorkspace(repository, prepared);
	} catch {
		// A failed or stale warm-up falls back to a fresh per-action workspace.
	}
	return undefined;
}

async function attachSandboxWorkspace(repository: PooledGitRepository, baseline: NonNullable<PooledGitRepository["baseline"]>,
	ownedProcessRoot?: string): Promise<PreparedGitWorkspace> {
	const { commit, aliases } = baseline, spare = ownedProcessRoot ? undefined : repository.spare;
	if (spare) {
		repository.spare = undefined;
		try { return await resetSandboxWorkspace(repository, spare, baseline); } catch { await spare.workspace.dispose().catch(() => undefined); }
	}
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
		return { sandboxRoot, processRoot, commit, gitDirectory, aliases, dispose: () => disposal ??= (async () => {
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
		for (const entry of await readdir(directory, { withFileTypes: true }).catch(() => [])) if (entry.isDirectory()) await grant(path.join(directory, entry.name)); };
	await grant(target); await rm(target, { recursive: true, force: true });
});

/** Paths a settled change wrote, relative to the root it wrote under. */
function writtenPaths(changes: readonly SandboxWorkspaceChange[]): string[] {
	return changes.flatMap(change => change.validationOnly ? [] : [change.target, ...change.kind !== "directory" ? change.aliases ?? [] : []]
		.map(target => path.relative(change.root, target)));
}

function prepareNextSandbox(pool: PooledGitRepository, baseline: NonNullable<PooledGitRepository["baseline"]>): void {
	if (pool.prepared || pool.disposal || pool.quarantined) return;
	const next = pool.prepared = { commit: baseline.commit, aliases: JSON.stringify(baseline.aliases), workspace: attachSandboxWorkspace(pool, baseline) };
	void next.workspace.catch(() => { if (pool.prepared === next) pool.prepared = undefined; });
}

/** Keep one filesystem-tool worktree for reset; any other goes. */
function retireSandboxWorkspace(repository: PooledGitRepository, workspace: PreparedGitWorkspace, written: readonly string[] = []): Promise<void> {
	if (repository.spare || repository.disposal || repository.quarantined) return workspace.dispose();
	repository.spare = { workspace, written }; return Promise.resolve();
}

/**
 * Only filesystem tools wrote here, each path recorded: those paths and the old link groups are removed so Git restores them
 * with checkout modes, Git resets and cleans the rest, and a clean status (ignored files included) proves the baseline.
 */
async function resetSandboxWorkspace(repository: PooledGitRepository, { workspace, written }: NonNullable<PooledGitRepository["spare"]>,
	baseline: NonNullable<PooledGitRepository["baseline"]>): Promise<PreparedGitWorkspace> {
	const { sandboxRoot } = workspace, git = bindGit(repository.gitBinary, sandboxRoot, ["-C", sandboxRoot]);
	for (const relative of [...written, ...workspace.aliases.flat()]) {
		const target = path.resolve(sandboxRoot, relative);
		if (!relativeFilesystemPath(sandboxRoot, target)) throw new Error("recycled path escapes its workspace");
		await assertNoSymlinkPath(sandboxRoot, target); await removeOwnedTree(target);
	}
	await git(["reset", "--hard", "--quiet", baseline.commit]); await git(["clean", "-ffdxq"]);
	await linkSandboxAliases(sandboxRoot, baseline.aliases);
	if ((await git(["status", "--porcelain", "--ignored", "--untracked-files=all", "-z"])).length) throw new Error("recycled workspace differs from its baseline");
	return { ...workspace, commit: baseline.commit, aliases: baseline.aliases };
}

async function acquireOverlayBaseline(
	repository: PooledGitRepository,
	snapshot: NonNullable<PooledGitRepository["baseline"]>,
): Promise<SharedOverlayBaseline> {
	const { commit } = snapshot;
	return withWorkspaceLock(repository, async () => {
		const original = await captureSnapshotMetadata(repository.sourceRoot), identity = createHash("sha256").update(commit);
		for (const [name, stat] of [...original].sort(([a], [b]) => a.localeCompare(b)))
			identity.update(JSON.stringify([name, ...SNAPSHOT_METADATA_FIELDS.filter(field => field !== "atimeNs").map(field => String(stat[field]))]));
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

function overlayBaselineStructure(baseline: SharedOverlayBaseline): Promise<WorkspaceStructureSnapshot> {
	baseline.structure ??= captureWorkspaceStructure(baseline.sandboxRoot, {
		maxFiles: WORKSPACE_TRANSACTION_MAX_FILES,
		exclude: SNAPSHOT_EXCLUDES,
	}).then(snapshot => Object.freeze({ ...snapshot,
		// The lower stat identity belongs to this owned resource, not the merged mount's device.
		entries: new Map([...snapshot.entries].map(([resource, entry]) => [resource,
			entry.kind === "file" ? { ...entry, contentPath: path.join(snapshot.root, resource) } : entry])),
	}));
	return baseline.structure;
}

function releaseOverlayBaseline(baseline: SharedOverlayBaseline): void { baseline.active = Math.max(0, baseline.active - 1); }

async function sandboxIndexChanges(repository: PooledGitRepository): Promise<string[]> {
	const [tracked, untracked] = await Promise.all([
		// Porcelain refreshes stat-only changes; fresh allocations also validate exact resources.
		repository.index(["diff", "--name-only", "--no-renames", "--no-ext-diff", "--no-textconv", "--ignore-submodules=none", "-z", "--"]),
		repository.index(["ls-files", "--others", "-z", "--"]),
	]);
	return [...new Set([...parseNullList(tracked), ...parseNullList(untracked)])].filter((file) => !isSnapshotExcluded(slash(file)))
		.map((file) => path.resolve(repository.sourceRoot, file));
}

function withWorkspaceLock<T>(owner: { lock: Promise<void> }, run: () => Promise<T>): Promise<T> {
	const pending = owner.lock.then(run);
	owner.lock = pending.then(() => undefined, () => undefined);
	return pending;
}

function releaseSandboxRepository(repository: PooledGitRepository): void {
	repository.active = Math.max(0, repository.active - 1);
	if (repository.active === 0) { for (const resolve of repository.idleWaiters) resolve(); repository.idleWaiters.clear(); }
	if (repository.quarantined || repository.disposal || repository.active > 0 || repository.idleTimer) return;
	repository.idleTimer = setTimeout(() => {
		// A sealed service owns the final pool sweep; keep its registration until that sweep.
		if (repository.active > 0 || repository.owner.lifetime.sealed) return;
		const { owner } = repository, key = `${filesystemPathKey(repository.sourceRoot)}\0${repository.gitBinary}`;
		if (owner.repositories.get(key) === repository.registration) owner.repositories.delete(key);
		// Detach this generation now, but keep its asynchronous retirement inside the service lifecycle.
		void owner.lifetime.run(() => closeSandboxRepository(repository)).catch(() => undefined);
	}, SANDBOX_REPOSITORY_IDLE_MS);
	repository.idleTimer.unref?.();
}

/**
 * Stop allocating a pool that still backs an unverified live mount. Logical users may finish and
 * release their leases, but neither idle cleanup nor global disposal may reclaim its filesystem.
 */
function quarantineSandboxRepository(repository: PooledGitRepository): void {
	repository.quarantined = true;
	const key = `${filesystemPathKey(repository.sourceRoot)}\0${repository.gitBinary}`;
	if (repository.owner.repositories.get(key) === repository.registration) repository.owner.repositories.delete(key);
	if (repository.idleTimer) { clearTimeout(repository.idleTimer); repository.idleTimer = undefined; }
	repository.baseline?.version.release();
	repository.baseline = undefined;
	repository.versions.close();
	releaseSandboxRepository(repository);
}

function closeWorkspaceSandboxPoolsFor(state: WorkspaceSandboxState, roots?: readonly string[]): Promise<void> {
	return state.lifetime.run(() => closeWorkspaceSandboxPoolsNow(state, roots));
}

async function closeWorkspaceSandboxPoolsNow(state: WorkspaceSandboxState, roots?: readonly string[]): Promise<void> {
	const rootKeys = roots ? new Set(roots.map(filesystemPathKey)) : undefined;
	const pending = [...state.repositories.entries()].filter(([key]) => {
		if (!rootKeys) return true;
		const separator = key.indexOf("\0");
		return rootKeys.has(separator === -1 ? key : key.slice(0, separator));
	});
	const closed = await Promise.allSettled(pending.map(async ([key, item]) => {
		if (state.repositories.get(key) === item) state.repositories.delete(key);
		const repository = await item.catch(() => undefined);
		if (repository) await closeSandboxRepository(repository);
	}));
	const failure = closed.find(result => result.status === "rejected");
	if (failure) throw failure.reason;
}

function closeSandboxRepository(repository: PooledGitRepository): Promise<void> {
	return repository.disposal ??= (async () => {
		if (repository.active > 0) await new Promise<void>(resolve => repository.idleWaiters.add(resolve));
		if (repository.quarantined) return;
		if (repository.idleTimer) clearTimeout(repository.idleTimer);
		const prepared = await takePreparedSandbox(repository);
		await prepared?.dispose().catch(() => undefined);
		repository.baseline?.version.release();
		repository.baseline = undefined;
		repository.versions.close();
		await removeOwnedTree(repository.parent);
	})();
}

function throwIfAborted(signal?: AbortSignal): void {
	if (!signal?.aborted) return;
	throw signal.reason instanceof Error ? signal.reason : new Error("sandbox preparation aborted");
}

function snapshotPathspecs(): string[] {
	return [
		".",
		...SNAPSHOT_EXCLUDES.flatMap((item) => [`:(glob,exclude)**/${item}`, `:(glob,exclude)**/${item}/**`]),
		`:(glob,exclude)**/${SANDBOX_STAGING_FILE_PREFIX}*.tmp`,
	];
}

/** Case-insensitive volumes, and win32 trailing dots and spaces, spell the same excluded entry differently. */
const snapshotSegment = process.platform === "win32" ? (segment: string) => segment.replace(/[. ]+$/u, "").toLowerCase()
	: process.platform === "darwin" ? (segment: string) => segment.toLowerCase() : (segment: string) => segment;

function isSnapshotExcluded(relative: string): boolean {
	return relative.split("/").map(snapshotSegment).some((segment) => (SNAPSHOT_EXCLUDES as readonly string[]).includes(segment) ||
		segment.startsWith(SANDBOX_STAGING_FILE_PREFIX) && segment.endsWith(".tmp"));
}

function assertWorkspaceSandboxOpen(state: WorkspaceSandboxState): void {
	if (state.lifetime.sealed) throw new Error("Workspace sandbox service is disposed");
}

async function withPrivateSandboxWorkspace<T>(state: WorkspaceSandboxState, cwd: string, gitBinary: string,
	driver: Exclude<WorkspaceSandboxDriver, "auto">, overlayOptions: WorkspaceSandboxOptions, run: (workspace: PrivateSandboxWorkspace) => Promise<T>,
	checkpoint?: WorkspaceCheckpoint, preparation?: SandboxPreparation): Promise<T> {
	const workspace = await createPrivateSandboxWorkspace(state, cwd, gitBinary, driver, overlayOptions, preparation);
	try { if (checkpoint) await materializeCheckpoint(workspace, checkpoint); return await run(workspace); } finally { await workspace.dispose(); }
}

async function materializeCheckpoint(workspace: PrivateSandboxWorkspace, checkpoint: WorkspaceCheckpoint): Promise<void> {
	const lineage: WorkspaceCheckpoint[] = [];
	for (let current: WorkspaceCheckpoint | undefined = checkpoint; current; current = current.parent) { lineage.push(current); }
	lineage.reverse();
	// Allocation can finish after Actor adoption. Skip an adopted prefix only when this private baseline proves its final state.
	let adopted = 0;
	while (lineage[adopted]?.committed) adopted++;
	for (; adopted; adopted--) {
		const final = new Map(lineage.slice(0, adopted).flatMap(ancestor => ancestor.changes.filter(change => !change.validationOnly).map(change => [change.resource, change] as const)));
		if ((await mapFilesystem([...final.values()], async change => {
			const target = path.resolve(workspace.sandboxRoot, change.resource);
			await assertNoSymlinkPath(workspace.sandboxRoot, target);
			if (change.kind === "directory") return sameDirectoryAfter(target, change).catch(() => false);
			if (change.object || change.aliases) return false;
			const current = await readRegularState(target);
			return sameSandboxState(current, change.after === undefined ? undefined : { content: change.after, mode: change.afterMode ?? change.beforeMode ?? 0 }) &&
				(change.afterModified === undefined || String(current?.identity?.mtimeNs) === change.afterModified);
		})).every(Boolean)) break;
	}
	for (const ancestor of lineage.slice(adopted)) {
		const project = (name: string) => {
			const relative = relativeFilesystemPath(ancestor.sourceRoot, name);
			if (relative === undefined) throw new Error("checkpoint object escapes workspace");
			return path.resolve(workspace.sandboxRoot, relative);
		};
		const changes = ownSandboxChanges(ancestor.changes.map(change => ({ ...change, root: workspace.sandboxRoot, target: project(change.target),
			...(change.kind !== "directory" ? { ...(change.object ? { object: { ...change.object, path: project(change.object.path) } } : {}),
				...(change.aliases ? { aliases: change.aliases.map(project) } : {}) } : {}) })));
		workspace.recycle?.written.push(...writtenPaths(changes));
		await commitSandboxExecution(workspace.pool.owner, { output: { result: { content: [], details: {} }, isError: false }, changes });
		for (const change of changes) if (!change.validationOnly && change.kind !== "directory") {
			workspace.baselineFrontier.set(change.resource, await readRegularState(change.target));
		}
	}
}

async function collectSandboxChanges(workspace: PrivateSandboxWorkspace, frontier?: readonly string[]): Promise<readonly SandboxFileChange[]> {
	const detected = frontier ?? (workspace.overlay ? await collectOverlayChangeResources(workspace) : await collectGitChangeResources(workspace));
	const resources = [...new Set([...detected, ...(frontier ? [] : workspace.baselineFrontier.keys())])].filter((resource) => !isSnapshotExcluded(slash(resource))).sort();
	// Changed files share their directories: each is walked once, and the files are read concurrently.
	const capture = sharedWalk();
	return (await mapFilesystem(resources, async (resource): Promise<readonly SandboxFileChange[]> => {
		if (!resource || path.isAbsolute(resource) || resource.split("/").includes("..")) throw new Error(`invalid sandbox change path: ${resource}`);
		const target = path.resolve(workspace.sourceRoot, resource), sandboxTarget = path.resolve(workspace.sandboxRoot, resource);
		if (!containsFilesystemPath(workspace.sourceRoot, target) || !containsFilesystemPath(workspace.sandboxRoot, sandboxTarget)) throw new Error(`sandbox change escapes workspace: ${resource}`);
		await Promise.all([assertNoSymlinkPath(workspace.sourceRoot, target, capture), assertNoSymlinkPath(workspace.sandboxRoot, sandboxTarget, capture)]);
		const [before, after] = await Promise.all([workspace.baselineFrontier.has(resource) ? workspace.baselineFrontier.get(resource)
			: workspace.readBase(resource, 64 * 1024 * 1024), readRegularState(sandboxTarget)]);
		return frontier || !sameSandboxState(before, after) ? [{ root: workspace.sourceRoot, target, resource, before: before?.content, after: after?.content, beforeMode: before?.mode,
			afterMode: after?.mode, afterModified: after?.identity && String(after.identity.mtimeNs), ...(before?.settled ? { beforeIdentity: before.settled } : {}) }] : [];
	})).flat();
}

async function collectGitChangeResources(workspace: PrivateSandboxWorkspace): Promise<readonly string[]> {
	const options = { environment: { GIT_OPTIONAL_LOCKS: "0" } };
	const tracked = await workspace.indexGit(["diff", "--name-only", "--no-renames", "-z", workspace.commit, "--"], options);
	const untracked = await workspace.indexGit(["ls-files", "--others", "-z", "--"], options);
	if (process.platform === "win32") {
		const untrackedRoots = await workspace.indexGit(["ls-files", "--others", "--directory", "-z", "--"], options);
		for (const resource of parseNullList(untrackedRoots)) {
			if (slash(resource).endsWith("/")) await assertNoDirectoryLinks(workspace.sandboxRoot, resource);
		}
	}
	return Object.freeze([...new Set([...parseNullList(tracked), ...parseNullList(untracked)])]);
}

interface OverlayStructureRemoval { readonly resource: string; readonly descendantsOnly: boolean; }

interface OverlayStructureFrontier { readonly refresh: ReadonlySet<string>; readonly removals: readonly OverlayStructureRemoval[]; }

type OverlayUpperEntry =
	| { readonly kind: "opaque" | "directory" | "whiteout"; readonly resource: string }
	| { readonly kind: "leaf"; readonly resource: string; readonly regular: boolean };

/**
 * Reconstruct the complete logical structure from one immutable lower snapshot plus the typed upper
 * journal. Only upper paths and their ancestor directories require fresh merged-view syscalls.
 */
async function captureOverlayWorkspaceStructure(workspace: PrivateSandboxWorkspace, baseline: WorkspaceStructureSnapshot,
	device: string): Promise<WorkspaceStructureSnapshot> {
	if (!workspace.overlay) throw new Error("OverlayFS structure frontier is unavailable");
	const frontier = await inspectOverlayStructureFrontier(workspace.overlay.upperRoot);
	const entries = new Map([...baseline.entries].map(([name, entry]) => [name, entry.kind === "file" && entry.object
		? { ...entry, object: `${device}:${entry.object.split(":")[1]}` } : entry]));
	for (const removal of frontier.removals) {
		const normalized = path.normalize(removal.resource);
		const prefix = normalized ? `${normalized}${path.sep}` : "";
		for (const candidate of [...entries.keys()]) {
			if (
				(removal.descendantsOnly && prefix && candidate.startsWith(prefix)) ||
				(!removal.descendantsOnly && (candidate === normalized || (prefix && candidate.startsWith(prefix))))
			) {
				entries.delete(candidate);
			}
		}
	}
	const refresh = [...frontier.refresh].sort(comparePathDepth), refreshed = await mapFilesystem(refresh, resource => {
		const target = resource ? path.resolve(workspace.sandboxRoot, resource) : workspace.sandboxRoot;
		if (!containsFilesystemPath(workspace.sandboxRoot, target)) throw new Error(`OverlayFS frontier escapes workspace: ${resource}`);
		return captureWorkspaceStructureEntry(target, resource ? [] : workspace.observationExcludes);
	});
	refresh.forEach((resource, index) => { const entry = refreshed[index]; if (entry) entries.set(resource, entry); else entries.delete(resource); });
	const files = Math.max(0, entries.size - 1);
	return workspaceStructureSnapshot(workspace.sandboxRoot, entries, baseline.complete && files <= WORKSPACE_TRANSACTION_MAX_FILES);
}

async function inspectOverlayStructureFrontier(upperRoot: string): Promise<OverlayStructureFrontier> {
	const refresh = new Set<string>([""]);
	const removals: OverlayStructureRemoval[] = [];
	const addAncestors = (resource: string, includeSelf: boolean) => {
		let current = includeSelf ? path.normalize(resource) : path.dirname(path.normalize(resource));
		for (;;) { const relative = current === "." ? "" : current; refresh.add(relative); if (!relative) break; current = path.dirname(relative); }
	};
	await walkOverlayUpper(upperRoot, "structure frontier", (entry) => {
		if (entry.kind === "opaque" || entry.kind === "whiteout") {
			removals.push({ resource: entry.resource, descendantsOnly: entry.kind === "opaque" });
			addAncestors(entry.resource, entry.kind === "opaque");
			return;
		}
		if (entry.kind === "leaf" && !entry.regular) throw new Error(`unsupported OverlayFS upper inode: ${entry.resource}`);
		addAncestors(entry.resource, true);
	});
	return { refresh, removals: Object.freeze(removals) };
}

async function walkOverlayUpper(upperRoot: string, journal: string, observe: (entry: OverlayUpperEntry) => void | Promise<void>): Promise<void> {
	let entries = 0;
	const visit = async (directory: string, relativeDirectory: string): Promise<void> => {
		const subdirectories: (readonly [string, string])[] = [];
		const children = await readdir(directory, { withFileTypes: true }).catch(error => {
			if (directory === upperRoot && isMissing(error)) return []; // A live lower has no upper until the first copy-up.
			throw error;
		});
		for (const child of children) {
			if (++entries > WORKSPACE_TRANSACTION_MAX_FILES) throw new Error(`OverlayFS ${journal} exceeds file limit`);
			if (child.name === ".wh..wh..opq") { await observe({ kind: "opaque", resource: relativeDirectory }); continue; }
			if (child.name.startsWith(".wh.")) throw new Error(`unsupported OverlayFS whiteout encoding: ${child.name}`);
			const resource = slash(relativeDirectory ? path.join(relativeDirectory, child.name) : child.name);
			if (isSnapshotExcluded(resource)) continue;
			// The entry's own type decides; only a whiteout's device number needs its inode.
			const target = path.join(directory, child.name);
			if (child.isDirectory()) { await observe({ kind: "directory", resource }); subdirectories.push([target, resource]); }
			else if (child.isCharacterDevice()) {
				if ((await lstat(target)).rdev !== 0) throw new Error(`unsupported OverlayFS device entry: ${resource}`);
				await observe({ kind: "whiteout", resource });
			} else await observe({ kind: "leaf", resource, regular: child.isFile() });
		}
		await Promise.all(subdirectories.map(([target, resource]) => visit(target, resource))); // Sibling directories are read concurrently.
	};
	await visit(upperRoot, "");
}

function comparePathDepth(left: string, right: string): number {
	const depth = (value: string) => value.split(path.sep).filter(Boolean).length;
	return depth(left) - depth(right) || left.localeCompare(right);
}

/**
 * OverlayFS is itself the mutation journal. Only copy-ups, creations, and whiteout/opaque
 * boundaries can differ from the immutable lower tree, so an unchanged lower tree is never
 * rescanned. Final bytes are still read through the merged mount and compared with the exact
 * checkpoint baseline before a branch can be sealed.
 */
async function collectOverlayChangeResources(
	workspace: PrivateSandboxWorkspace,
): Promise<readonly string[]> {
	if (!workspace.overlay) throw new Error("OverlayFS change journal is unavailable");
	const resources = new Set<string>();
	const addBaselineSubtree = async (resource: string) => {
		const prefix = resource || ".", within = resource ? `${path.normalize(resource)}${path.sep}` : "";
		const tree = workspace.liveBase ? [...workspace.liveBase.entries].flatMap(([name, entry]) => entry.kind !== "directory" && name.startsWith(within) ? [`\t${slash(name)}`] : [])
			: parseNullList(await workspace.pool.git(["ls-tree", "-r", "-z", "--full-tree", workspace.commit, "--", prefix], { environment: { GIT_OPTIONAL_LOCKS: "0" } }));
		for (const record of tree) {
			const separator = record.indexOf("\t");
			if (separator === -1) throw new Error(`invalid Git subtree entry: ${resource}`);
			const candidate = record.slice(separator + 1);
			if (candidate && !isSnapshotExcluded(slash(candidate))) resources.add(candidate);
		}
		const normalized = resource ? `${path.normalize(resource)}${path.sep}` : "";
		for (const candidate of workspace.baselineFrontier.keys()) {
			if (candidate === resource || (!resource || path.normalize(candidate).startsWith(normalized))) { resources.add(candidate); }
		}
	};
	await walkOverlayUpper(workspace.overlay.upperRoot, "change journal", async (entry) => {
		if (entry.kind === "opaque" || entry.kind === "whiteout") await addBaselineSubtree(entry.resource);
		else if (entry.kind === "leaf") resources.add(entry.resource);
	});
	return Object.freeze([...resources]);
}

/** The workspace's own structure for a live lower; undefined keeps the snapshot (copy-up splits hard links, and a
 * truncated walk cannot vouch for what it skipped). */
async function captureLiveBase(pool: PooledGitRepository): Promise<WorkspaceStructureSnapshot | undefined> {
	const cached = pool.liveBase, changes = cached && pool.versions.changesSince(cached.version);
	if (cached && changes && !changes.uncertain && !changes.paths.length) return cached.structure;
	const version = await pool.versions.observeChanges();
	const structure = await captureWorkspaceStructure(pool.sourceRoot, { maxFiles: WORKSPACE_TRANSACTION_MAX_FILES, exclude: SNAPSHOT_EXCLUDES });
	if (!structure.complete) { version.release(); return undefined; }
	const live = Object.freeze({ ...structure, entries: new Map([...structure.entries].map(([resource, entry]) =>
		[resource, entry.kind === "file" ? { ...entry, contentPath: path.join(pool.sourceRoot, resource) } : entry])) });
	if (pool.liveBase !== cached) version.release();
	else { cached?.version.release(); pool.liveBase = { version, structure: live }; }
	return live;
}

/** A lower file read now is the one the workspace started from only while its identity is unchanged. */
async function readLiveBase(base: WorkspaceStructureSnapshot, resource: string, maxBytes: number): Promise<RegularFileState | undefined> {
	const entry = base.entries.get(resource);
	if (entry?.kind !== "file") return undefined;
	const takenAtMs = Date.now(), captured = await captureStableFile(entry.contentPath ?? path.join(base.root, resource), maxBytes, true, { digest: false });
	if (statChangeDigest(captured.stat) !== entry.changeDigest) throw new Error(`workspace changed since the sandbox started: ${resource}`);
	return { content: captured.content!, mode: Number(captured.stat.mode & 0o777n), settled: settledIdentity(captured.stat, takenAtMs) };
}

async function readGitTreeRegularState(git: ReturnType<typeof bindGit>, tree: string, resource: string,
	maxBytes = WORKSPACE_TRANSACTION_MAX_BYTES): Promise<RegularFileState | undefined> {
	const entry = await git(["ls-tree", "-z", tree, "--", resource]);
	if (entry.length === 0) return undefined;
	const terminator = entry.indexOf(0);
	if (terminator === -1) throw new Error(`invalid Git transaction entry: ${resource}`);
	const metadata = entry.subarray(0, terminator).toString("utf8").split("\t", 1)[0];
	const [mode, kind, hash] = metadata.split(" ");
	if ((mode !== "100644" && mode !== "100755") || kind !== "blob") {
		throw new Error(`workspace transaction resource is not a regular file: ${resource}`);
	}
	if (!hash) throw new Error(`invalid Git transaction blob: ${resource}`);
	const content = await git(["cat-file", "blob", hash], { maxBuffer: Math.max(1, Math.min(WORKSPACE_TRANSACTION_MAX_BYTES, maxBytes) + 1) });
	if (content.byteLength > maxBytes) throw new Error(`Git transaction blob exceeds capture limit: ${resource}`);
	return { content, mode: process.platform === "win32" ? 0 : mode === "100755" ? 0o755 : 0o644 };
}


async function assertNoDirectoryLinks(root: string, relative: string): Promise<void> {
	const normalized = slash(relative).replace(/\/+$/, "");
	if (!normalized || isSnapshotExcluded(normalized)) return;
	const target = path.resolve(root, normalized);
	await assertNoSymlinkPath(root, target);
	for (const entry of await readdir(target, { withFileTypes: true })) {
		const child = slash(path.join(normalized, entry.name));
		if (isSnapshotExcluded(child)) continue;
		if (entry.isSymbolicLink()) throw new Error(`sandbox path contains symlink: ${child}`);
		if (entry.isDirectory()) await assertNoDirectoryLinks(root, child);
	}
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
	if (change.kind !== "directory") for (const name of [...(change.aliases ?? []), ...(change.object ? [change.object.path] : [])]) {
		if (!path.isAbsolute(name) || !containsFilesystemPath(root, name) || path.resolve(name) === root) throw new Error("file object name escapes workspace");
	}
	await assertNoSymlinkPath(root, target, capture);
}

async function readRegularState(target: string, maxBytes = Number.POSITIVE_INFINITY): Promise<RegularFileState | undefined> {
	try {
		const captured = await captureStableFile(target, maxBytes, true, { digest: false });
		return { content: captured.content!, mode: process.platform === "win32" ? 0 : Number(captured.stat.mode & 0o777n), identity: captured.stat };
	} catch (error) { if (isMissing(error)) return undefined; throw error; }
}

/** Capture a directory without following links and reject concurrent namespace changes. */
export async function readSandboxDirectoryState(target: string, captureNames?: (names: readonly string[]) => void): Promise<SandboxDirectoryState | undefined> {
	try {
		const { info, entries } = await captureFilesystemEntry(target, "directory");
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
		Buffer.compare(left.content, right.content) === 0 && (right.mode === 0 || sameExecutableMode(left.mode, right.mode));
	return left.entriesDigest === right.entriesDigest && left.mode === right.mode && left.uid === right.uid && left.gid === right.gid;
}

function sameSandboxBaseline(current: RegularFileState | SandboxDirectoryState | undefined, change: SandboxWorkspaceChange): boolean {
	if (change.kind === "directory" && change.validationOnly) return Boolean(current) === Boolean(change.before);
	return sameSandboxState(current, change.kind === "directory" ? change.before : change.before === undefined
		? undefined : { content: change.before, mode: change.beforeMode ?? 0 });
}

async function sameDirectoryAfter(target: string, change: SandboxDirectoryChange, captureNames?: (names: readonly string[] | undefined) => void): Promise<boolean> {
	// Native mkdir observes existence; subsequent new-entry reads/access need their own proof.
	if (!change.operation) return sameSandboxState(await readSandboxDirectoryState(target, captureNames), change.after);
	const directory = (await lstat(target)).isDirectory();
	if (directory) captureNames?.(undefined);
	return directory;
}

function assertExistingInputPolicy(change: SandboxWorkspaceChange): void {
	// Default ACLs and other inherited creation policy are not observable through this binding.
	if (!change.validationOnly && change.before === undefined) throw new Error("Created input permissions require authoritative execution");
}



/** Tool reads serve text: bytes holding a NUL among their first 8000 (git's binary test) are never kept as an input. */
const textual = (content: Uint8Array | undefined) => !content?.subarray(0, 8000).includes(0);

function sandboxChangeBytes(change: SandboxWorkspaceChange | undefined): number {
	return !change || change.kind === "directory" ? 0 : (change.before?.byteLength ?? 0) + (change.after?.byteLength ?? 0);
}

/** Take ownership once, before cleanup or commit locks can yield to a retained caller reference. */
function ownSandboxChanges(changes: readonly SandboxWorkspaceChange[]): SandboxWorkspaceChange[] {
	const result = new Map<string, SandboxWorkspaceChange>();
	for (const change of changes) {
		const key = filesystemPathKey(change.target);
		const previous = result.get(key);
		if (!previous) { result.set(key, change); continue; }
		if (
			filesystemPathKey(previous.root) !== filesystemPathKey(change.root) ||
			(previous.kind === "directory") !== (change.kind === "directory") || previous.validationOnly !== change.validationOnly ||
			previous.operation !== change.operation || previous.kind !== "directory" && change.kind !== "directory" &&
				(JSON.stringify(previous.object) !== JSON.stringify(change.object) || JSON.stringify(previous.aliases) !== JSON.stringify(change.aliases))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		if (previous.kind === "directory" && change.kind === "directory") {
			if (!sameSandboxState(previous.before, change.before)) throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
			result.set(key, { ...change, before: previous.before, accessMode: (previous.accessMode ?? 0) | (change.accessMode ?? 0) });
			continue;
		}
		const previousFile = previous as SandboxFileChange;
		const changeFile = change as SandboxFileChange;
		if (
			!sameOptionalBytes(previousFile.before, changeFile.before) ||
			(previousFile.beforeMode !== undefined && changeFile.beforeMode !== undefined && !sameExecutableMode(previousFile.beforeMode, changeFile.beforeMode))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		result.set(key, { ...changeFile, before: previousFile.before, beforeMode: previousFile.beforeMode, beforeIdentity: previousFile.beforeIdentity,
			accessMode: (previous.accessMode ?? 0) | (change.accessMode ?? 0) });
	}
	return [...result.values()].map((change): SandboxWorkspaceChange => {
		const target = { root: path.resolve(change.root), target: path.resolve(change.target) };
		return change.kind === "directory"
			? { ...change, ...target, before: change.before && { ...change.before }, after: change.after && { ...change.after } }
			: { ...change, ...target, ...(change.object ? { object: { ...change.object, path: path.resolve(change.object.path) } } : {}),
				...(change.aliases ? { aliases: change.aliases.map(name => path.resolve(name)) } : {}),
				before: change.before && Buffer.from(change.before), after: change.after && Buffer.from(change.after) };
	}).sort((left, right) =>
		filesystemPathKey(left.target).localeCompare(filesystemPathKey(right.target)),
	);
}

async function restoreChanges(
	changes: readonly SandboxWorkspaceChange[],
	baselines: ReadonlyMap<SandboxWorkspaceChange, RegularFileState | SandboxDirectoryState | undefined>,
): Promise<void> {
	const errors: unknown[] = [];
	for (const change of [...changes].reverse()) {
		try {
			await assertCommitTarget(change);
			const baseline = baselines.get(change);
			if (change.kind === "directory") {
				const directory = baseline as SandboxDirectoryState | undefined;
				if (!directory) {
					try { await rmdir(change.target); } catch (error) { if (!isMissing(error)) throw error; }
				} else {
					const current = await readSandboxDirectoryState(change.target);
					if (!current) { await createParentDirectories(change.root, change.target); await mkdir(change.target, { mode: directory.mode }); }
					if (process.platform !== "win32") await chmod(change.target, directory.mode);
				}
				continue;
			}
			const file = baseline as RegularFileState | undefined;
			if (!file) await rm(change.target, { force: true });
			else await atomicWrite(change.target, file.content, file.mode, change.root);
		} catch (error) {
			errors.push(error);
		}
	}
	for (const change of changes) {
		try {
			const baseline = baselines.get(change);
			const current = change.kind === "directory" ? await readSandboxDirectoryState(change.target) : await readRegularState(change.target);
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

async function atomicWrite(target: string, content: Uint8Array, mode: number | undefined, sourceRoot: string) {
	const temporary = await stageAtomicWrite(content, mode, sourceRoot);
	try { await createParentDirectories(sourceRoot, target); await replaceFile(temporary, target, mode); } finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
}

async function stageAtomicWrite(content: Uint8Array, mode: number | undefined, stagingDirectory: string): Promise<string> {
	await mkdir(stagingDirectory, { recursive: true });
	const temporary = path.join(stagingDirectory, `${SANDBOX_STAGING_FILE_PREFIX}${randomUUID()}.tmp`);
	const fileMode = process.platform === "win32" ? undefined : mode;
	const handle = await open(temporary, "wx", fileMode ?? 0o600);
	try {
		await handle.writeFile(content);
		await handle.sync();
		if (fileMode !== undefined) await handle.chmod(fileMode);
	} catch (error) {
		await handle.close().catch(() => undefined);
		await rm(temporary, { force: true }).catch(() => undefined);
		throw error;
	} finally {
		await handle.close().catch(() => undefined);
	}
	return temporary;
}

async function createParentDirectories(sourceRoot: string, target: string, created?: string[], ensured?: Set<string>): Promise<void> {
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
			const info = await lstat(current);
			if (info.isSymbolicLink() || !info.isDirectory()) {
				throw new Error(`sandbox commit parent is not a real directory: ${current}`, { cause: error });
			}
		}
		ensured?.add(current);
	}
}

async function removeCreatedDirectories(directories: readonly string[]): Promise<void> {
	for (const directory of [...new Set(directories)].sort((left, right) => right.length - left.length)) {
		try { await rmdir(directory); } catch (error) { if (!isMissing(error)) throw error; }
	}
}

let modifiedTimesHelper: string | undefined;
/** A native helper that sets them all in one process (`--set-modified-times`, linux-held-exec.c). */
export const useModifiedTimesHelper = (binary: string) => { modifiedTimesHelper = binary; };

/** Give files back the modification times their producers left, which later readers may have observed. Node sets only
 * microseconds, so on Linux, whose traces record them, the helper or else coreutils touch sets them; elsewhere a file keeps its commit's time. */
export async function restoreModifiedTimes(files: readonly (readonly [target: string, modified: string | undefined])[]): Promise<void> {
	const times = files.flatMap(([target, modified]) => /^\d+$/.test(modified ?? "") ? [[modified!.padStart(10, "0").slice(0, -9), modified!.slice(-9).padStart(9, "0"), target]] : []);
	if (process.platform === "linux" && modifiedTimesHelper && times.length) return new Promise<void>((resolve, reject) => execFile(modifiedTimesHelper!, ["--set-modified-times"],
		error => error ? reject(error) : resolve()).stdin?.end(times.map(([seconds, nanoseconds, target]) => `${seconds} ${nanoseconds} ${target}\0`).join("")));
	const pairs = times.flatMap(([seconds, nanoseconds, target]) => [`@${seconds}.${nanoseconds}`, target!]);
	for (let start = 0; process.platform === "linux" && start < pairs.length; start += 1024) await new Promise<void>((resolve, reject) => execFile("sh",
		["-c", 'while [ $# -gt 0 ]; do touch -chm -d "$1" -- "$2" || exit; shift 2; done', "sh", ...pairs.slice(start, start + 1024)], error => error ? reject(error) : resolve()));
}

async function replaceFile(temporary: string, target: string, mode?: number): Promise<void> {
	await rename(temporary, target);
	if (mode !== undefined && process.platform !== "win32") await chmod(target, mode);
}

function orderSandboxChanges(changes: readonly SandboxWorkspaceChange[]): SandboxWorkspaceChange[] {
	return orderWorkspaceChanges(changes.filter(change => !change.validationOnly), change => ({
		change,
		depth: slash(change.resource).split("/").filter(Boolean).length,
		key: filesystemPathKey(change.target),
	}));
}

/** Root locks make namespace changes conflict with every file commit below the same workspace. */
function commitLockTargets(changes: readonly SandboxWorkspaceChange[]): string[] {
	return [...new Set(changes.flatMap((change) => [path.resolve(change.root), path.resolve(change.target)]))].sort(
		(left, right) => filesystemPathKey(left).localeCompare(filesystemPathKey(right)),
	);
}

function withCommitLocks<T>(targets: readonly string[], run: () => Promise<T>, index = 0): Promise<T> {
	const target = targets[index];
	return target ? withFileMutationQueue(target, () => withCommitLocks(targets, run, index + 1)) : run();
}

function sameOptionalBytes(left: Uint8Array | undefined, right: Uint8Array | undefined): boolean {
	if (left === undefined || right === undefined) return left === right;
	return Buffer.compare(left, right) === 0;
}

function parseNullList(value: Uint8Array): string[] { return value.toString().split("\0").filter(Boolean).map((item) => slash(item)); }

/** The existing process owner binds each private repository/index once, preserving per-call cwd and limits. */
function bindGit(command: string, cwd: string, prefix: readonly string[] = []) {
	const bound = [...prefix];
	return (input: readonly string[], options: { cwd?: string; environment?: Readonly<Record<string, string>>; maxBuffer?: number; input?: Buffer } = {}): Promise<Buffer> => new Promise((resolve, reject) => {
		const args = [...bound, ...input];
		execFile(
			command,
			[...args],
			{
				cwd: options.cwd ?? cwd,
				env: {
					...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("GIT_"))),
					GIT_CONFIG_GLOBAL: "/dev/null",
					GIT_CONFIG_NOSYSTEM: "1",
					GIT_ATTR_NOSYSTEM: "1",
					...options.environment,
				},
				encoding: "buffer",
				maxBuffer: options.maxBuffer ?? 64 * 1024 * 1024,
			},
			(error, stdout, stderr) => {
				if (error) {
					const detail = Buffer.from(stderr).toString("utf8").trim() || error.message;
					const shown = args.slice(0, 16).join(" ");
					const omitted = Math.max(0, args.length - 16);
					reject(
						new Error(`${command} ${shown}${omitted ? ` … (${omitted} args omitted)` : ""} failed: ${detail}`),
					);
					return;
				}
				resolve(Buffer.from(stdout));
			},
		).stdin?.end(options.input);
	});
}
