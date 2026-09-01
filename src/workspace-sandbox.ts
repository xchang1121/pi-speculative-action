import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { chmod, type FileHandle, lstat, mkdir, mkdtemp, open, readdir, rename, rm, rmdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { asRecord, contains, slash } from "./action-semantics.ts";
import type { SpeculativeAgentExecutionWorld, SpeculativeToolExecutionContext } from "./agent-execution-world.ts";
import type {
	WorldBranch,
	WorldCheckpoint,
	WorldCommitMetrics,
	WorldCompatibilityEvidence,
	WorldExecutionMetrics,
} from "./execution-world.ts";
import { WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import {
	LinuxOverlayfsUnsafeCleanupError,
	linuxOverlayfsCapability,
	mountLinuxOverlayfs,
	openLinuxAnonymousWorkspaceFile,
	type LinuxOverlayfsMount,
	type LinuxOverlayfsOptions,
} from "./linux-overlayfs.ts";
import {
	captureWorkspaceStructure,
	captureWorkspaceStructureEntry,
	directoryEntriesDigest,
	type WorkspaceStructureEntry,
	type WorkspaceStructureSnapshot,
} from "./process-observation.ts";
import { ResourceVersionManager, type ResourceVersionToken } from "./resource-version.ts";
import type { ResourceValidation } from "./settlement.ts";
import type { ToolSettlement } from "./tool-settlement.ts";
import {
	deferredWorkspaceTransactionDriver,
	type WorkspaceRegularDelta,
	type WorkspaceStructureDriver,
	type WorkspaceTransactionCapture,
	type WorkspaceTransactionDelta,
	type WorkspaceTransactionDriver,
} from "./workspace-transaction.ts";

export interface SandboxFileChange {
	readonly kind?: "file";
	readonly root: string;
	readonly target: string;
	readonly resource: string;
	readonly before?: Uint8Array;
	readonly after?: Uint8Array;
	readonly beforeMode?: number;
	readonly afterMode?: number;
}

export interface SandboxDirectoryState {
	readonly entriesDigest: Extract<WorkspaceStructureEntry, { readonly kind: "directory" }>["entriesDigest"];
	readonly mode: number;
	readonly uid: number;
	readonly gid: number;
}

export interface SandboxDirectoryChange {
	readonly kind: "directory";
	readonly root: string;
	readonly target: string;
	readonly resource: string;
	readonly before?: SandboxDirectoryState;
	readonly after?: SandboxDirectoryState;
}

export type SandboxWorkspaceChange = SandboxFileChange | SandboxDirectoryChange;

interface RegularFileState {
	readonly content: Uint8Array;
	readonly mode: number;
}

export interface SandboxExecutionDelta {
	readonly output: ToolSettlement;
	readonly changes: readonly SandboxWorkspaceChange[];
}

interface WorkspaceExecutionSnapshot extends SandboxExecutionDelta {
	readonly executionMetrics: WorldExecutionMetrics;
}

export type WorkspaceSandboxDriver = "auto" | "git" | "overlayfs";

export interface WorkspaceSandboxOptions extends LinuxOverlayfsOptions {
	readonly gitBinary?: string;
	/** Auto is portable Git unless a trace-guarded runtime explicitly qualifies a COW driver. */
	readonly driver?: WorkspaceSandboxDriver;
}

export interface SandboxWorkspaceContext {
	readonly sourceRoot: string;
	readonly sandboxRoot: string;
	readonly processRoot: string;
	/** Root entry names owned by the isolation substrate and invisible to effect observation. */
	readonly observationExcludes: readonly string[];
	/** Driver-native content-free structure view shared by outer and nested process observers. */
	readonly structure: WorkspaceStructureDriver;
	/** Content-addressed mutation intervals, independent of any process or tool implementation. */
	readonly transactions: WorkspaceTransactionDriver;
}

export interface SandboxWorkspaceBranchOptions {
	readonly cwd: string;
	readonly action: SpeculativeToolExecutionContext["action"];
	readonly parentCheckpoint?: WorldCheckpoint;
	readonly gitBinary?: string;
	readonly driver?: WorkspaceSandboxDriver;
	readonly overlayfsBinary?: string;
	readonly fusermountBinary?: string;
	readonly execute: (workspace: SandboxWorkspaceContext) => Promise<ToolSettlement>;
	/** Seal operation-specific evidence after the generic transaction has captured its exact delta. */
	readonly afterCapture?: (
		workspace: SandboxWorkspaceContext,
		capture: SandboxExecutionDelta,
	) => Promise<readonly SandboxDirectoryChange[] | void>;
	/** Optional exact freshness proof captured by the operation-specific execution substrate. */
	readonly validate?: () => Promise<ResourceValidation>;
}

export interface PrepareSandboxWorkspaceOptions extends LinuxOverlayfsOptions {
	readonly gitBinary?: string;
	readonly driver?: WorkspaceSandboxDriver;
	readonly signal?: AbortSignal;
}

interface PrivateSandboxWorkspace extends SandboxWorkspaceContext {
	readonly repository: string;
	readonly gitDirectory: string;
	readonly baselineRoot: string;
	readonly gitBinary: string;
	readonly pool: PooledGitRepository;
	readonly commit: string;
	readonly driver: Exclude<WorkspaceSandboxDriver, "auto">;
	readonly baselineFrontier: Map<string, RegularFileState | undefined>;
	readonly openTransactionClock: () => Promise<FileHandle>;
	readonly transactionClockLinks: 0 | 1;
	/** Native roots whose timestamp domain is projected through the workspace view. */
	readonly transactionClockRoots: readonly string[];
	readonly overlay?: LinuxOverlayfsMount;
	readonly overlayStorageRoot?: string;
	readonly sharedBaseline?: SharedOverlayBaseline;
}

interface WorkspaceSandboxState {
	readonly repositories: Map<string, Promise<PooledGitRepository>>;
	readonly targetLocks: Map<string, Promise<void>>;
	cleanupTail: Promise<void>;
	disposed: boolean;
}

interface PooledGitRepository {
	readonly owner: WorkspaceSandboxState;
	readonly sourceRoot: string;
	readonly parent: string;
	readonly repository: string;
	readonly gitBinary: string;
	readonly versions: ResourceVersionManager;
	commit?: string;
	version?: ResourceVersionToken;
	active: number;
	readonly idleWaiters: Set<() => void>;
	lock: Promise<void>;
	prepared?: Promise<PreparedGitWorkspace>;
	readonly overlayBaselines: Map<string, Promise<SharedOverlayBaseline>>;
	autoDriverDecision?: AutoWorkspaceDriverDecision;
	/** Unsafe live-mount storage is detached from allocation and retained for OS-level recovery. */
	quarantined: boolean;
	registration?: Promise<PooledGitRepository>;
	idleTimer?: ReturnType<typeof setTimeout>;
}

interface PreparedGitWorkspace {
	readonly sandboxRoot: string;
	readonly processRoot: string;
	readonly commit: string;
	readonly gitDirectory: string;
}

interface SharedOverlayBaseline {
	readonly root: string;
	readonly privateRoot: string;
	readonly gitDirectory: string;
	readonly commit: string;
	structure?: Promise<WorkspaceStructureSnapshot>;
	active: number;
}

interface AutoWorkspaceDriverDecision {
	readonly commit: string;
	readonly capabilityFingerprint: string;
	readonly treeEntries: number;
	readonly resolved: QualifiedWorkspaceSandboxDriver;
}

class GitWorkspaceTransactionCapture implements WorkspaceTransactionCapture {
	contaminated = false;
	settled = false;
	readonly owner: GitWorkspaceTransactionDriver;
	readonly before?: WorkspaceStructureSnapshot;
	readonly frontier?: ReadonlyMap<string, RegularFileState | undefined>;

	constructor(
		owner: GitWorkspaceTransactionDriver,
		before?: WorkspaceStructureSnapshot,
		frontier?: ReadonlyMap<string, RegularFileState | undefined>,
	) {
		this.owner = owner;
		this.before = before;
		this.frontier = frontier;
	}

	readonly finish = (): Promise<WorkspaceTransactionDelta> => this.owner.finish(this);
	readonly abort = (): Promise<void> => this.owner.abort(this);
}

class GitWorkspaceTransactionDriver implements WorkspaceTransactionDriver {
	private readonly active = new Set<GitWorkspaceTransactionCapture>();
	private lock: Promise<void> = Promise.resolve();
	private readonly gitBinary: string;
	private readonly gitRoot: string;
	private readonly sandboxRoot: string;
	private readonly baselineTree: string;
	private readonly captureStructure: () => Promise<WorkspaceStructureSnapshot>;
	private readonly openClock: () => Promise<FileHandle>;
	private readonly expectedClockLinks: 0 | 1;
	private readonly clockRoots: readonly string[];
	private lastStructure: WorkspaceStructureSnapshot;
	private readonly frontier: Map<string, RegularFileState | undefined>;
	private poisonReason?: string;
	private clockSequence = 0;
	private clockDevice?: number;
	private clockHandle?: FileHandle;
	private disposed = false;

	constructor(
		gitBinary: string,
		gitRoot: string,
		sandboxRoot: string,
		baselineTree: string,
		captureStructure: () => Promise<WorkspaceStructureSnapshot>,
		openClock: () => Promise<FileHandle>,
		expectedClockLinks: 0 | 1,
		clockRoots: readonly string[],
		initialStructure: WorkspaceStructureSnapshot,
		initialFrontier: ReadonlyMap<string, RegularFileState | undefined>,
	) {
		this.gitBinary = gitBinary;
		this.gitRoot = gitRoot;
		this.sandboxRoot = sandboxRoot;
		this.baselineTree = baselineTree;
		this.captureStructure = captureStructure;
		this.openClock = openClock;
		this.expectedClockLinks = expectedClockLinks;
		this.clockRoots = clockRoots;
		this.lastStructure = initialStructure;
		this.frontier = new Map(initialFrontier);
		if (!initialStructure.complete) this.poisonReason = "workspace_structure_limit";
	}

	async initialize(): Promise<void> {
		if (this.poisonReason) return;
		try {
			await this.assertChangeClockFilesystem();
			await this.advanceChangeClock(this.lastStructure);
			const verified = await this.captureStructure();
			if (!sameWorkspaceChangeSnapshot(this.lastStructure, verified)) {
				throw new Error("workspace changed while initializing transaction clock");
			}
			this.lastStructure = verified;
		} catch (error) {
			this.poisonReason = `workspace_transaction_clock:${error instanceof Error ? error.message : String(error)}`;
		}
	}

	readonly begin = (): Promise<WorkspaceTransactionCapture> =>
		this.withLock(async () => {
			if (this.disposed) throw new Error("workspace transaction driver is disposed");
			if (this.active.size > 0) {
				for (const capture of this.active) capture.contaminated = true;
				const capture = new GitWorkspaceTransactionCapture(this);
				capture.contaminated = true;
				this.active.add(capture);
				return capture;
			}
			if (this.poisonReason) {
				const capture = new GitWorkspaceTransactionCapture(this);
				this.active.add(capture);
				return capture;
			}
			let before: WorkspaceStructureSnapshot | undefined;
			try {
				before = await this.captureFencedBefore();
			} catch (error) {
				this.poisonReason = `workspace_transaction_sync:${error instanceof Error ? error.message : String(error)}`;
			}
			const capture = this.poisonReason || !before
				? new GitWorkspaceTransactionCapture(this)
				: new GitWorkspaceTransactionCapture(this, before, new Map(this.frontier));
			this.active.add(capture);
			return capture;
		});

	async finish(capture: GitWorkspaceTransactionCapture): Promise<WorkspaceTransactionDelta> {
		return this.withLock(async () => {
			if (capture.settled || !this.active.has(capture)) {
				return { complete: false, changes: [], reason: "transaction_already_settled" };
			}
			capture.settled = true;
			this.active.delete(capture);
			if (capture.contaminated) {
				return { complete: false, changes: [], reason: "overlapping_workspace_transaction" };
			}
			if (!capture.before || !capture.frontier) {
				return { complete: false, changes: [], reason: this.poisonReason ?? "workspace_transaction_unavailable" };
			}
			try {
				const observed = await this.captureStructure();
				await this.advanceChangeClock(observed);
				const after = await this.captureStructure();
				if (!sameWorkspaceChangeSnapshot(observed, after)) {
					throw new Error("workspace changed while fencing transaction endpoint");
				}
				const transitions = regularStructureTransitions(capture.before, after);
				this.lastStructure = after;
				if (!transitions.complete) {
					this.poisonReason = transitions.reason;
					return { complete: false, changes: [], reason: transitions.reason, before: capture.before, after };
				}
				const changes = await this.captureTransitions(transitions.paths, capture.frontier, after);
				const verified = await this.captureStructure();
				if (!sameWorkspaceChangeSnapshot(after, verified)) {
					throw new Error("workspace changed while sealing transaction endpoint");
				}
				this.lastStructure = verified;
				return {
					complete: true,
					changes,
					before: capture.before,
					after: verified,
				};
			} catch (error) {
				const reason = `workspace_transaction_capture:${error instanceof Error ? error.message : String(error)}`;
				this.poisonReason = reason;
				return {
					complete: false,
					changes: [],
					reason,
					before: capture.before,
				};
			}
		});
	}

	private async captureFencedBefore(): Promise<WorkspaceStructureSnapshot> {
		for (let attempt = 0; attempt < WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS; attempt++) {
			const current = await this.captureStructure();
			await this.advanceChangeClock(current);
			const fenced = await this.captureStructure();
			if (!sameWorkspaceChangeSnapshot(current, fenced)) continue;
			await this.synchronizeFrontier(fenced);
			if (this.poisonReason) throw new Error(this.poisonReason);
			const verified = await this.captureStructure();
			if (!sameWorkspaceChangeSnapshot(fenced, verified)) continue;
			this.lastStructure = verified;
			return verified;
		}
		throw new Error("workspace did not stabilize before transaction execution");
	}

	private async assertChangeClockFilesystem(): Promise<void> {
		const handle = await this.openClock();
		let retained = false;
		try {
			const [workspace, clock, ...clockRoots] = await Promise.all([
				lstat(this.sandboxRoot),
				handle.stat(),
				...this.clockRoots.map((root) => lstat(root)),
			]);
			if (
				!workspace.isDirectory() ||
				!clock.isFile() ||
				clock.nlink !== this.expectedClockLinks ||
				clockRoots.length === 0 ||
				clockRoots.some((root) => !root.isDirectory() || root.dev !== clock.dev)
			) {
				throw new Error("workspace transaction clock is not private or its backing timestamp domain changed");
			}
			this.clockDevice = clock.dev;
			this.clockHandle = handle;
			retained = true;
		} finally {
			if (!retained) await handle.close();
		}
	}

	private async advanceChangeClock(snapshot: WorkspaceStructureSnapshot): Promise<void> {
		let boundary = Number.NEGATIVE_INFINITY;
		for (const entry of snapshot.entries.values()) boundary = Math.max(boundary, entry.changeTimeMs);
		if (!Number.isFinite(boundary)) throw new Error("workspace change clock boundary is unavailable");
		const deadline = Date.now() + WORKSPACE_TRANSACTION_CLOCK_TIMEOUT_MS;
		for (;;) {
			const handle = this.clockHandle;
			if (!handle) throw new Error("workspace transaction clock is unavailable");
			let changedAt: number;
			const identity = await handle.stat();
			if (
				!identity.isFile() ||
				identity.nlink !== this.expectedClockLinks ||
				this.clockDevice === undefined ||
				identity.dev !== this.clockDevice
			) {
				throw new Error("workspace transaction clock identity changed");
			}
			await handle.truncate(0);
			await handle.write(`${++this.clockSequence}\n`, 0, "utf8");
			changedAt = (await handle.stat()).ctimeMs;
			if (changedAt > boundary) return;
			if (Date.now() >= deadline) throw new Error("filesystem change clock did not advance");
			await new Promise<void>((resolve) => setTimeout(resolve, 1));
		}
	}

	async abort(capture: GitWorkspaceTransactionCapture): Promise<void> {
		await this.withLock(async () => {
			if (capture.settled) return;
			capture.settled = true;
			this.active.delete(capture);
			for (const active of this.active) active.contaminated = true;
		});
	}

	readonly dispose = (): Promise<void> =>
		this.withLock(async () => {
			if (this.disposed) return;
			this.disposed = true;
			for (const capture of this.active) capture.contaminated = true;
			this.active.clear();
			const clock = this.clockHandle;
			this.clockHandle = undefined;
			await clock?.close();
		});

	private async synchronizeFrontier(current: WorkspaceStructureSnapshot): Promise<void> {
		const transitions = regularStructureTransitions(this.lastStructure, current);
		this.lastStructure = current;
		if (!transitions.complete) {
			this.poisonReason = transitions.reason;
			return;
		}
		let retainedBytes = stateMapBytes(this.frontier);
		for (const relativePath of transitions.paths) {
			const entry = current.entries.get(relativePath);
			retainedBytes -= this.frontier.get(relativePath)?.content.byteLength ?? 0;
			const state =
				entry?.kind === "file"
					? await readRegularState(
							path.resolve(this.sandboxRoot, relativePath),
							WORKSPACE_TRANSACTION_MAX_BYTES - retainedBytes,
						)
					: undefined;
			retainedBytes += state?.content.byteLength ?? 0;
			this.frontier.set(relativePath, state);
		}
	}

	private async captureTransitions(
		paths: readonly string[],
		beforeFrontier: ReadonlyMap<string, RegularFileState | undefined>,
		after: WorkspaceStructureSnapshot,
	): Promise<readonly WorkspaceRegularDelta[]> {
		const changes: WorkspaceRegularDelta[] = [];
		let beforeBytes = 0;
		let afterBytes = 0;
		let retainedBytes = stateMapBytes(this.frontier);
		for (const relativePath of paths) {
			const previous = beforeFrontier.has(relativePath)
				? beforeFrontier.get(relativePath)
				: await readGitTreeRegularState(
						this.gitBinary,
						this.gitRoot,
						this.baselineTree,
						relativePath,
						WORKSPACE_TRANSACTION_MAX_BYTES - beforeBytes,
					);
			beforeBytes += previous?.content.byteLength ?? 0;
			if (beforeBytes > WORKSPACE_TRANSACTION_MAX_BYTES) {
				throw new Error("workspace transaction before-state exceeds capture limit");
			}
			const entry = after.entries.get(relativePath);
			retainedBytes -= this.frontier.get(relativePath)?.content.byteLength ?? 0;
			const current =
				entry?.kind === "file"
					? await readRegularState(
							path.resolve(this.sandboxRoot, relativePath),
							Math.min(
								WORKSPACE_TRANSACTION_MAX_BYTES - afterBytes,
								WORKSPACE_TRANSACTION_MAX_BYTES - retainedBytes,
							),
						)
					: undefined;
			afterBytes += current?.content.byteLength ?? 0;
			retainedBytes += current?.content.byteLength ?? 0;
			this.frontier.set(relativePath, current);
			if (!sameOptionalState(previous, current)) {
				changes.push({
					relativePath,
					...(previous ? { before: previous.content, beforeMode: previous.mode } : {}),
					...(current ? { after: current.content, afterMode: current.mode } : {}),
				});
			}
		}
		return Object.freeze(changes);
	}

	private async withLock<T>(run: () => Promise<T>): Promise<T> {
		const previous = this.lock;
		let release: () => void = () => {};
		this.lock = new Promise<void>((resolve) => {
			release = resolve;
		});
		await previous;
		try {
			return await run();
		} finally {
			release();
		}
	}
}

function regularStructureTransitions(
	before: WorkspaceStructureSnapshot,
	after: WorkspaceStructureSnapshot,
): { readonly complete: true; readonly paths: readonly string[] } | { readonly complete: false; readonly reason: string } {
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
		if (
			(previous === undefined || previous.kind === "directory") &&
			(current === undefined || current.kind === "directory")
		) {
			continue;
		}
		return { complete: false, reason: `unsupported_workspace_transition:${relativePath}` };
	}
	return { complete: true, paths: Object.freeze(paths) };
}

function sameChangeIdentity(
	left: WorkspaceStructureEntry | undefined,
	right: WorkspaceStructureEntry | undefined,
): boolean {
	return left === right || (!!left && !!right && left.kind === right.kind && left.changeDigest === right.changeDigest);
}

function sameWorkspaceChangeSnapshot(left: WorkspaceStructureSnapshot, right: WorkspaceStructureSnapshot): boolean {
	if (!left.complete || !right.complete || left.entries.size !== right.entries.size) return false;
	for (const [relativePath, entry] of left.entries) {
		if (!sameChangeIdentity(entry, right.entries.get(relativePath))) return false;
	}
	return true;
}

function stateMapBytes(states: ReadonlyMap<string, RegularFileState | undefined>): number {
	let total = 0;
	for (const state of states.values()) total += state?.content.byteLength ?? 0;
	return total;
}

// The execution world mirrors everything the actor can read below cwd. Git metadata is
// replaced by the private repository and commit's own temporary files are internal.
const SNAPSHOT_EXCLUDES = [".git"] as const;
const SANDBOX_REPOSITORY_IDLE_MS = 5 * 60 * 1000;
const GIT_PATHSPEC_BATCH_BYTES = 32 * 1024;
const WORKSPACE_TRANSACTION_MAX_BYTES = 512 * 1024 * 1024;
const WORKSPACE_TRANSACTION_MAX_FILES = 100_000;
const WORKSPACE_TRANSACTION_CLOCK_TIMEOUT_MS = 100;
const WORKSPACE_TRANSACTION_STABILITY_ATTEMPTS = 3;
const SANDBOX_STAGING_FILE_PREFIX = ".pi-speculative-";
const GIT_WORKSPACE_FINGERPRINT = "git-worktree:v1";
// Small-tree gains remain host-sensitive and carry one-time FUSE preparation cost, while the
// 500/1,000-file A/B is material. Use a conservative power-of-two boundary and exact baseline.
const AUTO_OVERLAY_MIN_TREE_ENTRIES = 256;
const SANDBOX_AUTHOR_ENVIRONMENT = {
	GIT_AUTHOR_NAME: "Pi Speculative Action",
	GIT_AUTHOR_EMAIL: "speculative-action@localhost",
	GIT_COMMITTER_NAME: "Pi Speculative Action",
	GIT_COMMITTER_EMAIL: "speculative-action@localhost",
} as const;

class GitWorldCheckpoint implements WorldCheckpoint {
	readonly backend = "git_worktree" as const;
	readonly id = randomUUID();
	readonly lineage: string;
	readonly depth: number;
	readonly sourceRoot: string;
	readonly parent?: GitWorldCheckpoint;
	readonly changes: readonly SandboxWorkspaceChange[];

	constructor(sourceRoot: string, parent: GitWorldCheckpoint | undefined, changes: readonly SandboxWorkspaceChange[]) {
		this.sourceRoot = path.resolve(sourceRoot);
		this.lineage = parent?.lineage ?? this.id;
		this.depth = (parent?.depth ?? -1) + 1;
		this.parent = parent;
		this.changes = changes;
	}
}

function resolveWorkspaceCheckpoint(
	checkpoint: WorldCheckpoint | undefined,
	sourceRoot: string,
): GitWorldCheckpoint | undefined {
	if (checkpoint === undefined) return undefined;
	if (!(checkpoint instanceof GitWorldCheckpoint))
		throw new Error("Execution world checkpoint belongs to another backend.");
	if (pathKey(checkpoint.sourceRoot) !== pathKey(sourceRoot)) {
		throw new Error("Execution world checkpoint belongs to another workspace.");
	}
	return checkpoint;
}

export interface QualifiedWorkspaceSandboxDriver {
	readonly driver: Exclude<WorkspaceSandboxDriver, "auto">;
	readonly fingerprint: string;
}

/** Owns workspace repositories and commit serialization for one extension/runtime lifecycle. */
export class WorkspaceSandboxService {
	private readonly state: WorkspaceSandboxState = {
		repositories: new Map(),
		targetLocks: new Map(),
		cleanupTail: Promise.resolve(),
		disposed: false,
	};

	async fingerprint(options: WorkspaceSandboxOptions = {}, sourceRoot?: string): Promise<string> {
		assertWorkspaceSandboxOpen(this.state);
		return await workspaceSandboxFingerprintFor(this.state, options, sourceRoot);
	}

	async qualify(
		options: WorkspaceSandboxOptions,
		sourceRoot: string,
	): Promise<QualifiedWorkspaceSandboxDriver> {
		assertWorkspaceSandboxOpen(this.state);
		return await resolveWorkspaceDriver(this.state, options, sourceRoot);
	}

	createExecutionWorld(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
		assertWorkspaceSandboxOpen(this.state);
		return createWorkspaceSandboxFor(this.state, options);
	}

	async prepare(cwd: string, options: PrepareSandboxWorkspaceOptions = {}): Promise<void> {
		assertWorkspaceSandboxOpen(this.state);
		await prepareSandboxWorkspaceFor(this.state, cwd, options);
	}

	async fork(options: SandboxWorkspaceBranchOptions): Promise<WorldBranch<ToolSettlement>> {
		assertWorkspaceSandboxOpen(this.state);
		return await forkSandboxWorkspaceFor(this.state, options);
	}

	async withWorkspace<T>(
		cwd: string,
		run: (workspace: SandboxWorkspaceContext) => Promise<T>,
		gitBinary = "git",
	): Promise<T> {
		assertWorkspaceSandboxOpen(this.state);
		return await withSandboxWorkspaceFor(this.state, cwd, run, gitBinary);
	}

	async commitDelta(delta: SandboxExecutionDelta): Promise<ToolSettlement> {
		assertWorkspaceSandboxOpen(this.state);
		return (await commitSandboxExecution(this.state, delta)).output;
	}

	closePools(roots?: readonly string[]): Promise<void> {
		return closeWorkspaceSandboxPoolsFor(this.state, roots);
	}

	async dispose(): Promise<void> {
		if (this.state.disposed) return;
		this.state.disposed = true;
		await Promise.allSettled([...this.state.targetLocks.values()]);
		await closeWorkspaceSandboxPoolsFor(this.state);
	}
}

const defaultWorkspaceSandboxService = new WorkspaceSandboxService();

/** Concrete storage identity is part of route compatibility, never an implicit implementation detail. */
export async function workspaceSandboxFingerprint(
	options: WorkspaceSandboxOptions = {},
	sourceRoot?: string,
): Promise<string> {
	return defaultWorkspaceSandboxService.fingerprint(options, sourceRoot);
}

async function workspaceSandboxFingerprintFor(
	state: WorkspaceSandboxState,
	options: WorkspaceSandboxOptions,
	sourceRoot?: string,
): Promise<string> {
	return sourceRoot
		? (await resolveWorkspaceDriver(state, options, sourceRoot)).fingerprint
		: (await resolveWorkspaceDriver(state, options)).fingerprint;
}

/**
 * Qualify the capability- and cost-selected COW driver for a runtime that traces driver-specific
 * filesystem errors and rejects adoption. Generic host-function branches deliberately do not call
 * this function and retain portable Git semantics.
 */
export function qualifyWorkspaceSandboxDriver(
	options: WorkspaceSandboxOptions,
	sourceRoot: string,
): Promise<QualifiedWorkspaceSandboxDriver> {
	return defaultWorkspaceSandboxService.qualify(options, sourceRoot);
}

async function resolveWorkspaceDriver(
	state: WorkspaceSandboxState,
	options: WorkspaceSandboxOptions,
	sourceRoot?: string,
	acquiredRepository?: PooledGitRepository,
): Promise<QualifiedWorkspaceSandboxDriver> {
	const requested = options.driver ?? "auto";
	if (requested === "git") return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
	const capability = await linuxOverlayfsCapability({
		...(options.overlayfsBinary ? { overlayfsBinary: options.overlayfsBinary } : {}),
		...(options.fusermountBinary ? { fusermountBinary: options.fusermountBinary } : {}),
	});
	if (!capability.available) {
		if (requested === "overlayfs") throw new Error(capability.detail);
		return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };
	}
	const overlay = { driver: "overlayfs", fingerprint: `linux-overlayfs:v1:${capability.fingerprint}` } as const;
	if (requested === "overlayfs") return overlay;
	if (!sourceRoot) return { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT };

	const ownedRepository = acquiredRepository
		? undefined
		: await acquireSandboxRepository(state, path.resolve(sourceRoot), options.gitBinary ?? "git");
	const repository = acquiredRepository ?? ownedRepository;
	if (!repository) throw new Error("workspace repository is unavailable");
	try {
		const commit = await acquireSandboxBaseline(repository, SANDBOX_AUTHOR_ENVIRONMENT);
		const cached = repository.autoDriverDecision;
		if (cached?.commit === commit && cached.capabilityFingerprint === capability.fingerprint) {
			return cached.resolved;
		}
		const treeEntries = await countGitBaselineEntries(repository, commit);
		const resolved = treeEntries >= AUTO_OVERLAY_MIN_TREE_ENTRIES
			? overlay
			: { driver: "git", fingerprint: GIT_WORKSPACE_FINGERPRINT } as const;
		repository.autoDriverDecision = {
			commit,
			capabilityFingerprint: capability.fingerprint,
			treeEntries,
			resolved,
		};
		return resolved;
	} finally {
		if (ownedRepository) releaseSandboxRepository(ownedRepository);
	}
}

/** Create a copy-on-write execution world with transactional multi-file commit. */
export function createWorkspaceSandbox(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
	return defaultWorkspaceSandboxService.createExecutionWorld(options);
}

function createWorkspaceSandboxFor(
	state: WorkspaceSandboxState,
	options: WorkspaceSandboxOptions,
): SpeculativeAgentExecutionWorld {
	// Generic mutation routes have no workspace root at fingerprint time. Their short, targeted
	// branches retain Git unless OverlayFS was explicitly requested; Linux process routes can make
	// the exact baseline-qualified auto decision from their invocation context.
	const resolvedOptions: WorkspaceSandboxOptions =
		options.driver === "overlayfs" ? options : { ...options, driver: "git" };
	const roots = new Set<string>();
	return {
		id: "git_worktree",
		scope: "fallback",
		isolation: "workspace_branch",
		capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
		fingerprint: () => {
			assertWorkspaceSandboxOpen(state);
			return workspaceSandboxFingerprintFor(state, resolvedOptions);
		},
		prepare: async ({ cwd, signal }) => {
			assertWorkspaceSandboxOpen(state);
			roots.add(path.resolve(cwd));
			await prepareSandboxWorkspaceFor(state, cwd, { ...resolvedOptions, signal });
		},
		fork: async (context) => {
			assertWorkspaceSandboxOpen(state);
			const sourceRoot = path.resolve(context.cwd);
			roots.add(sourceRoot);
			return executeMutation(state, context, resolvedOptions);
		},
		dispose: async () => {
			const ownedRoots = [...roots];
			roots.clear();
			await closeWorkspaceSandboxPoolsFor(state, ownedRoots);
		},
	};
}

class GitWorldBranch implements WorldBranch<ToolSettlement> {
	readonly backend = "git_worktree" as const;
	readonly checkpoint: GitWorldCheckpoint;
	readonly output: ToolSettlement;
	readonly resources: readonly string[];
	readonly capturedBytes: number;
	readonly executionMetrics: WorkspaceExecutionSnapshot["executionMetrics"];
	readonly compatibility: WorldCompatibilityEvidence;
	readonly validate?: () => Promise<ResourceValidation>;
	private readonly changes: readonly SandboxWorkspaceChange[];
	private readonly owner: WorkspaceSandboxState;
	private commitMetricsValue?: WorldCommitMetrics;
	private commitPromise?: Promise<ToolSettlement>;

	constructor(
		snapshot: WorkspaceExecutionSnapshot,
		sourceRoot: string,
		executionFingerprint: string,
		owner: WorkspaceSandboxState,
		parent?: GitWorldCheckpoint,
		validate?: () => Promise<ResourceValidation>,
	) {
		this.output = snapshot.output;
		this.changes = Object.freeze([...snapshot.changes]);
		this.checkpoint = new GitWorldCheckpoint(sourceRoot, parent, this.changes);
		this.resources = Object.freeze([...new Set(this.changes.map((change) => change.resource))]);
		this.capturedBytes = this.changes.reduce(
			(total, change) =>
				change.kind === "directory"
					? total
					: total + (change.before?.byteLength ?? 0) + (change.after?.byteLength ?? 0),
			0,
		);
		this.executionMetrics = Object.freeze({ ...snapshot.executionMetrics });
		this.owner = owner;
		this.compatibility = Object.freeze({
			status: "compatible" as const,
			backend: this.backend,
			executionFingerprint,
		});
		this.validate = validate;
	}

	get commitMetrics(): WorldCommitMetrics | undefined {
		return this.commitMetricsValue;
	}

	readonly commit = (): Promise<ToolSettlement> => {
		if (this.commitPromise) return this.commitPromise;
		this.commitPromise = commitSandboxExecution(this.owner, { output: this.output, changes: this.changes }).then(
			({ output, metrics }) => {
				this.commitMetricsValue = metrics;
				return output;
			},
		);
		return this.commitPromise;
	};

	dispose(): void {
		// The private worktree is sealed and removed during fork; this branch owns only immutable bytes.
	}
}

/** Low-level transactional commit primitive for execution-world implementations. */
export async function commitSandboxDelta(delta: SandboxExecutionDelta): Promise<ToolSettlement> {
	return defaultWorkspaceSandboxService.commitDelta(delta);
}

async function commitSandboxExecution(
	state: WorkspaceSandboxState,
	execution: SandboxExecutionDelta,
): Promise<{ readonly output: ToolSettlement; readonly metrics: WorldCommitMetrics }> {
	assertWorkspaceSandboxOpen(state);
	const started = performance.now();
	const changes = deduplicateChanges(execution.changes);
	return withTargetLocks(
		state.targetLocks,
		commitLockTargets(changes),
		async () => {
			const staged = new Map<SandboxFileChange, string>();
			const baselines = new Map<SandboxWorkspaceChange, RegularFileState | SandboxDirectoryState | undefined>();
			const commitModes = new Map<SandboxFileChange, number | undefined>();
			const applied: SandboxWorkspaceChange[] = [];
			const createdDirectories: string[] = [];
			let bytesValidated = 0;
			let validationMs = 0;
			let resourcesCommitted = 0;
			try {
				for (const change of changes) await assertCommitTarget(change);
				for (const change of changes) {
					if (change.kind !== "directory" && change.after !== undefined) {
						staged.set(change, await stageAtomicWrite(change.after, change.afterMode, change.root));
					}
				}
				const validationStarted = performance.now();
				for (const change of changes) {
					const current =
						change.kind === "directory"
							? await readSandboxDirectoryState(change.target)
							: await readRegularState(change.target);
					baselines.set(change, current);
					if (change.kind !== "directory") bytesValidated += (current as RegularFileState | undefined)?.content.byteLength ?? 0;
					if (!sameSandboxBaseline(current, change)) {
						throw new Error(`resource changed before commit: ${change.resource}`);
					}
					if (change.kind !== "directory") {
						commitModes.set(change, resolveCommitMode(current as RegularFileState | undefined, change));
					}
				}
				validationMs = Math.max(0, performance.now() - validationStarted);
				for (const change of orderSandboxChanges(changes)) {
					await assertCommitTarget(change);
					if (change.kind === "directory") {
						if (!change.after) {
							await rmdir(change.target);
							applied.push(change);
						} else if (!change.before) {
							createdDirectories.push(...(await createParentDirectories(change.root, change.target)));
							await mkdir(change.target, { mode: change.after.mode });
							applied.push(change);
							if (process.platform !== "win32") await chmod(change.target, change.after.mode);
						} else if (process.platform !== "win32" && change.before.mode !== change.after.mode) {
							await chmod(change.target, change.after.mode);
							applied.push(change);
						}
						resourcesCommitted++;
						continue;
					}
					applied.push(change);
					const temporary = staged.get(change);
					if (temporary) {
						createdDirectories.push(...(await createParentDirectories(change.root, change.target)));
						await replaceFile(temporary, change.target, commitModes.get(change));
						staged.delete(change);
					} else {
						await rm(change.target, { force: true });
					}
					resourcesCommitted++;
				}
				for (const change of changes) {
					if (change.kind !== "directory" || !change.after) continue;
					if (!sameDirectoryState(await readSandboxDirectoryState(change.target), change.after)) {
						throw new Error(`directory changed while committing: ${change.resource}`);
					}
				}
			} catch (error) {
				try {
					await restoreChanges(applied, baselines);
					await removeCreatedDirectories(createdDirectories);
				} catch (rollbackError) {
					throw new AggregateError(
						[error, rollbackError],
						"sandbox commit failed and the original workspace could not be fully restored",
						{ cause: error },
					);
				}
				throw error;
			} finally {
				await Promise.all(
					[...staged.values()].map((temporary) => rm(temporary, { force: true }).catch(() => undefined)),
				);
			}
			return {
				output: execution.output,
				metrics: {
					durationMs: Math.max(0, performance.now() - started),
					validationMs,
					bytesValidated,
					resourcesValidated: changes.length,
					resourcesCommitted,
				},
			};
		},
	);
}

export async function withSandboxWorkspace<T>(
	cwd: string,
	run: (workspace: SandboxWorkspaceContext) => Promise<T>,
	gitBinary = "git",
): Promise<T> {
	return defaultWorkspaceSandboxService.withWorkspace(cwd, run, gitBinary);
}

async function withSandboxWorkspaceFor<T>(
	state: WorkspaceSandboxState,
	cwd: string,
	run: (workspace: SandboxWorkspaceContext) => Promise<T>,
	gitBinary: string,
): Promise<T> {
	const workspace = await createPrivateSandboxWorkspace(state, cwd, gitBinary, "git", {});
	try {
		return await run(workspace);
	} finally {
		await cleanupPrivateSandboxWorkspace(workspace);
	}
}

/**
 * Fork one generic operation into a private workspace and seal its output together with the
 * complete regular-file delta. Process and host-function worlds share this primitive.
 */
export async function forkSandboxWorkspace(options: SandboxWorkspaceBranchOptions): Promise<WorldBranch<ToolSettlement>> {
	return defaultWorkspaceSandboxService.fork(options);
}

async function forkSandboxWorkspaceFor(
	state: WorkspaceSandboxState,
	options: SandboxWorkspaceBranchOptions,
): Promise<WorldBranch<ToolSettlement>> {
	const sourceRoot = path.resolve(options.cwd);
	const parent = resolveWorkspaceCheckpoint(options.parentCheckpoint, sourceRoot);
	const resolvedDriver = await resolveWorkspaceDriver(
		state,
		options.driver === "auto" || options.driver === undefined ? { ...options, driver: "git" } : options,
	);
	const setupStarted = performance.now();
	const snapshot = await withPrivateSandboxWorkspace(
		state,
		sourceRoot,
		options.gitBinary ?? "git",
		resolvedDriver.driver,
		options,
		async (workspace) => {
			const setupMs = Math.max(0, performance.now() - setupStarted);
			const output = await options.execute(workspace);
			const captureStarted = performance.now();
			const fileChanges = await collectSandboxChanges(workspace);
			const directoryChanges = (await options.afterCapture?.(workspace, { output, changes: fileChanges })) ?? [];
			const changes = deduplicateChanges([...fileChanges, ...directoryChanges]);
			return {
				output,
				changes,
				executionMetrics: {
					setupMs,
					captureMs: Math.max(0, performance.now() - captureStarted),
				},
			};
		},
		parent,
	);
	return new GitWorldBranch(
		snapshot,
		sourceRoot,
		options.action.executionFingerprint,
		state,
		parent,
		options.validate,
	);
}

export async function prepareSandboxWorkspace(
	cwd: string,
	options: PrepareSandboxWorkspaceOptions = {},
): Promise<void> {
	return defaultWorkspaceSandboxService.prepare(cwd, options);
}

async function prepareSandboxWorkspaceFor(
	state: WorkspaceSandboxState,
	cwd: string,
	options: PrepareSandboxWorkspaceOptions,
): Promise<void> {
	throwIfAborted(options.signal);
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	const repository = await acquireSandboxRepository(state, sourceRoot, options.gitBinary ?? "git");
	try {
		const concreteOptions =
			options.driver === "auto" || options.driver === undefined ? { ...options, driver: "git" as const } : options;
		const resolved = await resolveWorkspaceDriver(state, concreteOptions, sourceRoot, repository);
		if (resolved.driver === "overlayfs") {
			const commit = await acquireSandboxBaseline(repository, SANDBOX_AUTHOR_ENVIRONMENT);
			const baseline = await acquireOverlayBaseline(repository, commit);
			try {
				await overlayBaselineStructure(baseline);
			} finally {
				releaseOverlayBaseline(baseline);
			}
		} else await ensurePreparedSandbox(repository);
		throwIfAborted(options.signal);
	} finally {
		releaseSandboxRepository(repository);
	}
}

async function executeMutation(
	state: WorkspaceSandboxState,
	context: SpeculativeToolExecutionContext,
	options: WorkspaceSandboxOptions,
): Promise<WorldBranch<ToolSettlement>> {
	const args = asRecord(context.args);
	if (!args || typeof args.path !== "string") throw new Error(`${context.toolName}.path must be a string`);
	const sourceRoot = path.resolve(context.cwd);
	const target = path.resolve(sourceRoot, args.path);
	if (!contains(sourceRoot, target) || target === sourceRoot) {
		throw new Error(`sandbox mutation path escapes workspace: ${args.path}`);
	}
	await assertNoSymlinkPath(sourceRoot, target);
	const resource = slash(path.relative(sourceRoot, target));
	const requestedPath = args.path;
	return forkSandboxWorkspaceFor(state, {
		cwd: sourceRoot,
		action: context.action,
		...(context.parentCheckpoint ? { parentCheckpoint: context.parentCheckpoint } : {}),
		...options,
		execute: async (workspace) => {
			const sandboxTarget = path.resolve(workspace.sandboxRoot, resource);
			await assertNoSymlinkPath(workspace.sandboxRoot, sandboxTarget);
			const redirected = { ...args, path: sandboxTarget };
			const result = await context.tool.execute(context.callID, redirected as never, context.signal);
			return {
				result: replacePaths(result, [
					[sandboxTarget, requestedPath],
					[workspace.sandboxRoot, sourceRoot],
				]),
				isError: false,
			};
		},
	});
}

async function createPrivateSandboxWorkspace(
	state: WorkspaceSandboxState,
	cwd: string,
	gitBinary: string,
	driver: Exclude<WorkspaceSandboxDriver, "auto">,
	overlayOptions: LinuxOverlayfsOptions,
): Promise<PrivateSandboxWorkspace> {
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	const pool = await acquireSandboxRepository(state, sourceRoot, gitBinary);
	let attached: PreparedGitWorkspace | undefined;
	let sharedBaseline: SharedOverlayBaseline | undefined;
	let overlay: LinuxOverlayfsMount | undefined;
	let processRoot: string | undefined;
	let overlayStorageRoot: string | undefined;
	try {
		const commit = await acquireSandboxBaseline(pool, SANDBOX_AUTHOR_ENVIRONMENT);
		let sandboxRoot: string;
		let baselineRoot: string;
		let gitDirectory: string;
		let openTransactionClock: () => Promise<FileHandle>;
		let transactionClockLinks: 0 | 1;
		let transactionClockRoots: readonly string[];
		const observationExcludes: readonly string[] = SNAPSHOT_EXCLUDES;
		if (driver === "overlayfs") {
			sharedBaseline = await acquireOverlayBaseline(pool, commit);
			[processRoot, overlayStorageRoot] = await Promise.all([
				mkdtemp(path.join(pool.parent, "action-")),
				mkdtemp(path.join(pool.parent, "overlay-storage-")),
			]);
			const mounted = await mountLinuxOverlayfs({
				lowerRoot: sharedBaseline.root,
				privateRoot: overlayStorageRoot,
				options: overlayOptions,
			});
			overlay = mounted;
			sandboxRoot = mounted.root;
			baselineRoot = sharedBaseline.root;
			gitDirectory = sharedBaseline.gitDirectory;
			openTransactionClock = () => openLinuxAnonymousWorkspaceFile(mounted.upperRoot);
			transactionClockLinks = 0;
			transactionClockRoots = Object.freeze([sharedBaseline.root, mounted.upperRoot, mounted.workRoot]);
		} else {
			const prepared = (await takePreparedSandbox(pool, commit)) ?? (await attachSandboxWorkspace(pool, commit));
			attached = prepared;
			sandboxRoot = prepared.sandboxRoot;
			processRoot = prepared.processRoot;
			baselineRoot = prepared.sandboxRoot;
			gitDirectory = prepared.gitDirectory;
			const transactionClockPath = path.join(prepared.processRoot, "workspace-transaction.clock");
			openTransactionClock = () => {
				const noFollow = typeof fsConstants.O_NOFOLLOW === "number" ? fsConstants.O_NOFOLLOW : 0;
				return open(
					transactionClockPath,
					fsConstants.O_WRONLY | fsConstants.O_CREAT | noFollow,
					0o600,
				);
			};
			transactionClockLinks = 1;
			transactionClockRoots = Object.freeze([prepared.sandboxRoot, prepared.processRoot]);
		}
		const baselineFrontier = new Map<string, RegularFileState | undefined>();
		let workspace!: PrivateSandboxWorkspace;
		const structure: WorkspaceStructureDriver = {
			capture: () => {
				if (!workspace.overlay) {
					return captureWorkspaceStructure(workspace.sandboxRoot, {
						maxFiles: WORKSPACE_TRANSACTION_MAX_FILES,
						exclude: workspace.observationExcludes,
					});
				}
				if (!workspace.sharedBaseline) throw new Error("OverlayFS shared baseline is unavailable");
				return overlayBaselineStructure(workspace.sharedBaseline).then((baseline) =>
					captureOverlayWorkspaceStructure(workspace, baseline),
				);
			},
		};
		const transactions = deferredWorkspaceTransactionDriver(() => createGitWorkspaceTransactionDriver(workspace));
		workspace = {
			sourceRoot,
			sandboxRoot,
			processRoot,
			observationExcludes,
			structure,
			transactions,
			repository: pool.repository,
			gitDirectory,
			baselineRoot,
			gitBinary,
			pool,
			commit,
			driver,
			baselineFrontier,
			openTransactionClock,
			transactionClockLinks,
			transactionClockRoots,
			...(overlay ? { overlay } : {}),
			...(overlayStorageRoot ? { overlayStorageRoot } : {}),
			...(sharedBaseline ? { sharedBaseline } : {}),
		};
		return workspace;
	} catch (error) {
		let safeToRelease = !(error instanceof LinuxOverlayfsUnsafeCleanupError);
		let closeError: unknown;
		if (overlay) {
			try {
				await overlay.close();
			} catch (failure) {
				safeToRelease = false;
				closeError = failure;
			}
		}
		if (safeToRelease) {
			if (processRoot && !attached) await rm(processRoot, { recursive: true, force: true }).catch(() => undefined);
			if (overlayStorageRoot) await rm(overlayStorageRoot, { recursive: true, force: true }).catch(() => undefined);
			if (sharedBaseline) releaseOverlayBaseline(sharedBaseline);
			if (attached) await discardPreparedSandbox(pool, attached).catch(() => undefined);
			releaseSandboxRepository(pool);
		} else {
			quarantineSandboxRepository(pool);
		}
		if (closeError) {
			throw new AggregateError([error, closeError], "sandbox creation and safe OverlayFS cleanup both failed");
		}
		throw error;
	}
}

async function createGitWorkspaceTransactionDriver(workspace: PrivateSandboxWorkspace): Promise<WorkspaceTransactionDriver> {
	const baselineTree = (
		await git(
			workspace.gitBinary,
			["--git-dir", workspace.repository, "rev-parse", `${workspace.commit}^{tree}`],
			workspace.processRoot,
		)
	)
		.toString("utf8")
		.trim();
	if (!baselineTree) throw new Error("Git workspace transaction baseline is unavailable");
	const initialStructure = await workspace.structure.capture();
	const driver = new GitWorkspaceTransactionDriver(
		workspace.gitBinary,
		workspace.baselineRoot,
		workspace.sandboxRoot,
		baselineTree,
		workspace.structure.capture,
		workspace.openTransactionClock,
		workspace.transactionClockLinks,
		workspace.transactionClockRoots,
		initialStructure,
		workspace.baselineFrontier,
	);
	await driver.initialize();
	return driver;
}

async function acquireSandboxRepository(
	state: WorkspaceSandboxState,
	sourceRoot: string,
	gitBinary: string,
): Promise<PooledGitRepository> {
	assertWorkspaceSandboxOpen(state);
	const key = `${pathKey(sourceRoot)}\0${gitBinary}`;
	let pending = state.repositories.get(key);
	if (!pending) {
		pending = createSandboxRepository(state, sourceRoot, gitBinary);
		state.repositories.set(key, pending);
		void pending.catch(() => {
			if (state.repositories.get(key) === pending) state.repositories.delete(key);
		});
	}
	const repository = await pending;
	repository.registration ??= pending;
	if (repository.quarantined) {
		if (state.repositories.get(key) === pending) state.repositories.delete(key);
		return acquireSandboxRepository(state, sourceRoot, gitBinary);
	}
	if (repository.idleTimer) {
		clearTimeout(repository.idleTimer);
		repository.idleTimer = undefined;
	}
	repository.active++;
	return repository;
}

async function createSandboxRepository(
	owner: WorkspaceSandboxState,
	sourceRoot: string,
	gitBinary: string,
): Promise<PooledGitRepository> {
	const parent = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-action-pool-"));
	const repository = path.join(parent, "snapshot.git");
	try {
		await git(gitBinary, ["init", "--bare", repository], parent);
		await git(gitBinary, ["--git-dir", repository, "config", "core.autocrlf", "false"], parent);
		await git(gitBinary, ["--git-dir", repository, "config", "core.longpaths", "true"], parent);
		return {
			owner,
			sourceRoot,
			parent,
			repository,
			gitBinary,
			versions: new ResourceVersionManager(sourceRoot),
			active: 0,
			idleWaiters: new Set(),
			lock: Promise.resolve(),
			overlayBaselines: new Map(),
			quarantined: false,
		};
	} catch (error) {
		await rm(parent, { recursive: true, force: true });
		throw error;
	}
}

async function acquireSandboxBaseline(
	repository: PooledGitRepository,
	authorEnvironment: Readonly<Record<string, string>>,
): Promise<string> {
	return withRepositoryLock(repository, async () => {
		if (repository.commit && repository.version) {
			const [current, indexed] = await Promise.all([
				repository.versions.validate(repository.version),
				sandboxIndexChanges(repository),
			]);
			if (!current.expired && indexed.length === 0) return repository.commit;
		}
		for (let attempt = 0; attempt < 3; attempt++) {
			const version = await repository.versions.capture([{ path: repository.sourceRoot, scope: "tree_content" }]);
			let retained = false;
			try {
				const changes = repository.version ? repository.versions.changesSince(repository.version) : undefined;
				const indexed = repository.commit ? await sandboxIndexChanges(repository) : [];
				const changedPaths = [...new Set([...(changes?.paths ?? []), ...indexed])];
				const changedPathspecs =
					repository.commit && changes && !changes.uncertain
						? incrementalPathspecs(repository.sourceRoot, changedPaths)
						: undefined;
				if (repository.commit && changedPathspecs) {
					await git(
						repository.gitBinary,
						[
							"--git-dir",
							repository.repository,
							"--work-tree",
							repository.sourceRoot,
							"read-tree",
							repository.commit,
						],
						repository.sourceRoot,
					);
					if (changedPathspecs.length) {
						await stageSandboxPaths(repository, changedPathspecs);
					}
				} else {
					await git(
						repository.gitBinary,
						["--git-dir", repository.repository, "--work-tree", repository.sourceRoot, "read-tree", "--empty"],
						repository.sourceRoot,
					);
					await git(
						repository.gitBinary,
						[
							"--git-dir",
							repository.repository,
							"--work-tree",
							repository.sourceRoot,
							"add",
							"-f",
							"-A",
							"--",
							...snapshotPathspecs(),
						],
						repository.sourceRoot,
					);
				}
				const tree = (
					await git(
						repository.gitBinary,
						["--git-dir", repository.repository, "--work-tree", repository.sourceRoot, "write-tree"],
						repository.sourceRoot,
					)
				)
					.toString("utf8")
					.trim();
				if (repository.commit) {
					const previousTree = (
						await git(
							repository.gitBinary,
							["--git-dir", repository.repository, "show", "-s", "--format=%T", repository.commit],
							repository.parent,
						)
					)
						.toString("utf8")
						.trim();
					if (tree === previousTree) {
						if ((await repository.versions.validate(version)).expired) continue;
						replaceSandboxVersion(repository, version);
						retained = true;
						return repository.commit;
					}
				}
				const commit = (
					await git(
						repository.gitBinary,
						[
							"--git-dir",
							repository.repository,
							"commit-tree",
							tree,
							...(repository.commit ? ["-p", repository.commit] : []),
							"-m",
							"speculative baseline",
						],
						repository.parent,
						authorEnvironment,
					)
				)
					.toString("utf8")
					.trim();
				if ((await repository.versions.validate(version)).expired) continue;
				await git(
					repository.gitBinary,
					["--git-dir", repository.repository, "update-ref", "refs/heads/baseline", commit],
					repository.parent,
				);
				repository.commit = commit;
				replaceSandboxVersion(repository, version);
				retained = true;
				return commit;
			} finally {
				if (!retained) version.release();
			}
		}
		throw new Error("workspace changed repeatedly while preparing sandbox baseline");
	});
}

async function countGitBaselineEntries(repository: PooledGitRepository, commit: string): Promise<number> {
	const tree = await git(
		repository.gitBinary,
		["--git-dir", repository.repository, "ls-tree", "-r", "-z", "--name-only", commit],
		repository.parent,
	);
	return parseNullList(tree).length;
}

async function stageSandboxPaths(repository: PooledGitRepository, pathspecs: readonly string[]): Promise<void> {
	for (const batch of batchPathspecs(pathspecs)) {
		let pending = batch;
		for (let attempt = 0; attempt < 3 && pending.length; attempt++) {
			const tracked = new Set(
				parseNullList(
					await git(
						repository.gitBinary,
						["--git-dir", repository.repository, "ls-files", "-z", "--"],
						repository.sourceRoot,
					),
				),
			);
			pending = (
				await Promise.all(
					pending.map(async (pathspec) =>
						tracked.has(pathspec) || (await exists(path.join(repository.sourceRoot, pathspec)))
							? pathspec
							: undefined,
					),
				)
			).filter((pathspec): pathspec is string => pathspec !== undefined);
			if (!pending.length) break;
			try {
				await git(
					repository.gitBinary,
					[
						"--git-dir",
						repository.repository,
						"--work-tree",
						repository.sourceRoot,
						"add",
						"-f",
						"-A",
						"--",
						...pending,
					],
					repository.sourceRoot,
				);
				break;
			} catch (error) {
				if (!(error instanceof Error) || !error.message.includes("did not match any files") || attempt === 2) {
					throw error;
				}
			}
		}
	}
}

function batchPathspecs(pathspecs: readonly string[]): string[][] {
	const batches: string[][] = [];
	let batch: string[] = [];
	let bytes = 0;
	for (const pathspec of pathspecs) {
		const size = Buffer.byteLength(pathspec) + 1;
		if (batch.length && bytes + size > GIT_PATHSPEC_BATCH_BYTES) {
			batches.push(batch);
			batch = [];
			bytes = 0;
		}
		batch.push(pathspec);
		bytes += size;
	}
	if (batch.length) batches.push(batch);
	return batches;
}

async function ensurePreparedSandbox(repository: PooledGitRepository): Promise<void> {
	const commit = await acquireSandboxBaseline(repository, SANDBOX_AUTHOR_ENVIRONMENT);
	const existing = repository.prepared;
	if (existing) {
		const prepared = await existing;
		if (prepared.commit === commit) return;
		if (repository.prepared === existing) repository.prepared = undefined;
		await discardPreparedSandbox(repository, prepared);
	}
	if (repository.prepared) {
		await repository.prepared;
		return;
	}
	const pending = attachSandboxWorkspace(repository, commit);
	repository.prepared = pending;
	try {
		await pending;
	} catch (error) {
		if (repository.prepared === pending) repository.prepared = undefined;
		throw error;
	}
}

async function takePreparedSandbox(
	repository: PooledGitRepository,
	commit: string,
): Promise<PreparedGitWorkspace | undefined> {
	const pending = repository.prepared;
	if (!pending) return undefined;
	repository.prepared = undefined;
	try {
		const prepared = await pending;
		if (prepared.commit === commit) return prepared;
		await discardPreparedSandbox(repository, prepared);
	} catch {
		// A failed or stale warm-up falls back to a fresh per-action workspace.
	}
	return undefined;
}

async function attachSandboxWorkspace(
	repository: PooledGitRepository,
	commit: string,
	ownedProcessRoot?: string,
): Promise<PreparedGitWorkspace> {
	const processRoot = ownedProcessRoot ?? (await mkdtemp(path.join(repository.parent, "action-")));
	const sandboxRoot = path.join(processRoot, "workspace");
	try {
		if (ownedProcessRoot) await mkdir(processRoot, { recursive: true });
		await git(
			repository.gitBinary,
			["--git-dir", repository.repository, "worktree", "add", "--detach", sandboxRoot, commit],
			processRoot,
		);
		const gitDirectory = (
			await git(repository.gitBinary, ["-C", sandboxRoot, "rev-parse", "--absolute-git-dir"], sandboxRoot)
		)
			.toString("utf8")
			.trim();
		if (!path.isAbsolute(gitDirectory)) throw new Error("private Git directory is unavailable");
		return { sandboxRoot, processRoot, commit, gitDirectory };
	} catch (error) {
		await git(
			repository.gitBinary,
			["--git-dir", repository.repository, "worktree", "remove", "--force", sandboxRoot],
			repository.parent,
		).catch(() => undefined);
		if (!ownedProcessRoot) await rm(processRoot, { recursive: true, force: true }).catch(() => undefined);
		throw error;
	}
}

async function acquireOverlayBaseline(
	repository: PooledGitRepository,
	commit: string,
): Promise<SharedOverlayBaseline> {
	return withRepositoryLock(repository, async () => {
		for (const [candidateCommit, pending] of repository.overlayBaselines) {
			if (candidateCommit === commit) continue;
			const candidate = await pending.catch(() => undefined);
			if (!candidate || candidate.active > 0) continue;
			repository.overlayBaselines.delete(candidateCommit);
			await discardOverlayBaseline(repository, candidate).catch(() => undefined);
		}
		let pending = repository.overlayBaselines.get(commit);
		if (!pending) {
			pending = createOverlayBaseline(repository, commit);
			repository.overlayBaselines.set(commit, pending);
			void pending.catch(() => {
				if (repository.overlayBaselines.get(commit) === pending) repository.overlayBaselines.delete(commit);
			});
		}
		const baseline = await pending;
		baseline.active++;
		return baseline;
	});
}

async function createOverlayBaseline(
	repository: PooledGitRepository,
	commit: string,
): Promise<SharedOverlayBaseline> {
	const privateRoot = path.join(repository.parent, `overlay-baseline-${commit}`);
	const prepared = await attachSandboxWorkspace(repository, commit, privateRoot);
	return {
		root: prepared.sandboxRoot,
		privateRoot,
		gitDirectory: prepared.gitDirectory,
		commit,
		active: 0,
	};
}

function overlayBaselineStructure(baseline: SharedOverlayBaseline): Promise<WorkspaceStructureSnapshot> {
	baseline.structure ??= captureWorkspaceStructure(baseline.root, {
		maxFiles: WORKSPACE_TRANSACTION_MAX_FILES,
		exclude: SNAPSHOT_EXCLUDES,
	});
	return baseline.structure;
}

function releaseOverlayBaseline(baseline: SharedOverlayBaseline): void {
	baseline.active = Math.max(0, baseline.active - 1);
}

async function discardOverlayBaseline(
	repository: PooledGitRepository,
	baseline: SharedOverlayBaseline,
): Promise<void> {
	if (baseline.active > 0) throw new Error("cannot discard an active OverlayFS lower directory");
	await git(
		repository.gitBinary,
		["--git-dir", repository.repository, "worktree", "remove", "--force", baseline.root],
		repository.parent,
	).catch(() => undefined);
	await rm(baseline.privateRoot, { recursive: true, force: true });
}

async function discardPreparedSandbox(repository: PooledGitRepository, workspace: PreparedGitWorkspace): Promise<void> {
	await git(
		repository.gitBinary,
		["--git-dir", repository.repository, "worktree", "remove", "--force", workspace.sandboxRoot],
		repository.parent,
	).catch(() => undefined);
	await rm(workspace.processRoot, { recursive: true, force: true });
}

async function sandboxIndexChanges(repository: PooledGitRepository): Promise<string[]> {
	const prefix = ["--git-dir", repository.repository, "--work-tree", repository.sourceRoot];
	const [tracked, untracked] = await Promise.all([
		git(
			repository.gitBinary,
			[...prefix, "diff-files", "--name-only", "--no-renames", "-z", "--"],
			repository.sourceRoot,
		),
		git(repository.gitBinary, [...prefix, "ls-files", "--others", "-z", "--"], repository.sourceRoot),
	]);
	return [...new Set([...parseNullList(tracked), ...parseNullList(untracked)])]
		.filter((file) => !isSnapshotExcluded(slash(file)))
		.map((file) => path.resolve(repository.sourceRoot, file));
}

async function withRepositoryLock<T>(repository: PooledGitRepository, run: () => Promise<T>): Promise<T> {
	const previous = repository.lock;
	let release: () => void = () => {};
	repository.lock = new Promise<void>((resolve) => {
		release = resolve;
	});
	await previous;
	try {
		return await run();
	} finally {
		release();
	}
}

function releaseSandboxRepository(repository: PooledGitRepository): void {
	repository.active = Math.max(0, repository.active - 1);
	if (repository.active === 0) {
		for (const resolve of repository.idleWaiters) resolve();
		repository.idleWaiters.clear();
	}
	if (repository.quarantined || repository.active > 0 || repository.idleTimer) return;
	repository.idleTimer = setTimeout(() => {
		if (repository.active > 0) return;
		repository.owner.repositories.delete(`${pathKey(repository.sourceRoot)}\0${repository.gitBinary}`);
		const prepared = repository.prepared;
		repository.prepared = undefined;
		void (async () => {
			if (prepared) {
				const workspace = await prepared.catch(() => undefined);
				if (workspace) await discardPreparedSandbox(repository, workspace).catch(() => undefined);
			}
			repository.version?.release();
			repository.version = undefined;
			repository.versions.close();
			await rm(repository.parent, { recursive: true, force: true }).catch(() => undefined);
		})();
	}, SANDBOX_REPOSITORY_IDLE_MS);
	repository.idleTimer.unref?.();
}

/**
 * Stop allocating a pool that still backs an unverified live mount. Logical users may finish and
 * release their leases, but neither idle cleanup nor global disposal may reclaim its filesystem.
 */
function quarantineSandboxRepository(repository: PooledGitRepository): void {
	repository.quarantined = true;
	const key = `${pathKey(repository.sourceRoot)}\0${repository.gitBinary}`;
	if (repository.owner.repositories.get(key) === repository.registration) repository.owner.repositories.delete(key);
	if (repository.idleTimer) {
		clearTimeout(repository.idleTimer);
		repository.idleTimer = undefined;
	}
	repository.version?.release();
	repository.version = undefined;
	repository.versions.close();
	releaseSandboxRepository(repository);
}

export async function closeWorkspaceSandboxPools(roots?: readonly string[]): Promise<void> {
	return defaultWorkspaceSandboxService.closePools(roots);
}

function closeWorkspaceSandboxPoolsFor(
	state: WorkspaceSandboxState,
	roots?: readonly string[],
): Promise<void> {
	const close = state.cleanupTail.then(
		() => closeWorkspaceSandboxPoolsNow(state, roots),
		() => closeWorkspaceSandboxPoolsNow(state, roots),
	);
	state.cleanupTail = close.catch(() => undefined);
	return close;
}

async function closeWorkspaceSandboxPoolsNow(
	state: WorkspaceSandboxState,
	roots?: readonly string[],
): Promise<void> {
	const rootKeys = roots ? new Set(roots.map(pathKey)) : undefined;
	const pending = [...state.repositories.entries()].filter(([key]) => {
		if (!rootKeys) return true;
		const separator = key.indexOf("\0");
		return rootKeys.has(separator === -1 ? key : key.slice(0, separator));
	});
	for (const [key, item] of pending) {
		if (state.repositories.get(key) === item) state.repositories.delete(key);
		const repository = await item.catch(() => undefined);
		if (!repository) continue;
		await waitForSandboxRepositoryIdle(repository);
		if (repository.quarantined) continue;
		if (repository.idleTimer) clearTimeout(repository.idleTimer);
		const prepared = repository.prepared;
		repository.prepared = undefined;
		if (prepared) {
			const workspace = await prepared.catch(() => undefined);
			if (workspace) await discardPreparedSandbox(repository, workspace).catch(() => undefined);
		}
		repository.version?.release();
		repository.version = undefined;
		repository.versions.close();
		await rm(repository.parent, { recursive: true, force: true });
	}
}

async function waitForSandboxRepositoryIdle(repository: PooledGitRepository): Promise<void> {
	if (repository.active === 0) return;
	await new Promise<void>((resolve) => {
		repository.idleWaiters.add(resolve);
		if (repository.active === 0 && repository.idleWaiters.delete(resolve)) resolve();
	});
}

function replaceSandboxVersion(repository: PooledGitRepository, next: ResourceVersionToken): void {
	const previous = repository.version;
	repository.version = next;
	if (previous !== next) previous?.release();
}

function throwIfAborted(signal?: AbortSignal): void {
	if (!signal?.aborted) return;
	throw signal.reason instanceof Error ? signal.reason : new Error("sandbox preparation aborted");
}

function incrementalPathspecs(root: string, changedPaths: readonly string[]): string[] | undefined {
	const result = new Set<string>();
	for (const changedPath of changedPaths) {
		const relative = slash(path.relative(root, path.resolve(changedPath)) || ".");
		if (relative === ".") return undefined;
		if (relative === ".." || relative.startsWith("../") || path.isAbsolute(relative)) return undefined;
		if (isSnapshotExcluded(relative)) continue;
		result.add(relative);
	}
	return [...result].sort();
}

function snapshotPathspecs(): string[] {
	return [
		".",
		...SNAPSHOT_EXCLUDES.flatMap((item) => [`:(glob,exclude)**/${item}`, `:(glob,exclude)**/${item}/**`]),
		`:(glob,exclude)**/${SANDBOX_STAGING_FILE_PREFIX}*.tmp`,
	];
}

function isSnapshotExcluded(relative: string): boolean {
	const segments = relative.split("/");
	return (
		segments.some((segment) => (SNAPSHOT_EXCLUDES as readonly string[]).includes(segment)) ||
		segments.some((segment) => segment.startsWith(SANDBOX_STAGING_FILE_PREFIX) && segment.endsWith(".tmp"))
	);
}

function pathKey(value: string): string {
	const normalized = path.resolve(value).replaceAll("\\", "/");
	return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function assertWorkspaceSandboxOpen(state: WorkspaceSandboxState): void {
	if (state.disposed) throw new Error("Workspace sandbox service is disposed");
}

async function withPrivateSandboxWorkspace<T>(
	state: WorkspaceSandboxState,
	cwd: string,
	gitBinary: string,
	driver: Exclude<WorkspaceSandboxDriver, "auto">,
	overlayOptions: LinuxOverlayfsOptions,
	run: (workspace: PrivateSandboxWorkspace) => Promise<T>,
	checkpoint?: GitWorldCheckpoint,
): Promise<T> {
	const workspace = await createPrivateSandboxWorkspace(state, cwd, gitBinary, driver, overlayOptions);
	try {
		if (checkpoint) await materializeCheckpoint(workspace, checkpoint);
		return await run(workspace);
	} finally {
		await cleanupPrivateSandboxWorkspace(workspace);
	}
}

async function materializeCheckpoint(workspace: PrivateSandboxWorkspace, checkpoint: GitWorldCheckpoint): Promise<void> {
	const lineage: GitWorldCheckpoint[] = [];
	for (let current: GitWorldCheckpoint | undefined = checkpoint; current; current = current.parent) {
		lineage.push(current);
	}
	for (const ancestor of lineage.reverse()) {
		for (const change of orderSandboxChanges(ancestor.changes)) {
			const target = path.resolve(workspace.sandboxRoot, change.resource);
			if (!contains(workspace.sandboxRoot, target) || target === workspace.sandboxRoot) {
				throw new Error(`execution checkpoint escapes workspace: ${change.resource}`);
			}
			await assertNoSymlinkPath(workspace.sandboxRoot, target);
			if (change.kind === "directory") {
				if (!change.after) await rmdir(target);
				else if (!change.before) {
					await createParentDirectories(workspace.sandboxRoot, target);
					await mkdir(target, { mode: change.after.mode });
					if (process.platform !== "win32") await chmod(target, change.after.mode);
				} else if (process.platform !== "win32" && change.before.mode !== change.after.mode) {
					await chmod(target, change.after.mode);
				}
				continue;
			}
			if (change.after === undefined) await rm(target, { force: true });
			else await atomicWrite(target, change.after, change.afterMode, workspace.sandboxRoot);
			workspace.baselineFrontier.set(change.resource, await readRegularState(target));
		}
		for (const change of ancestor.changes) {
			if (change.kind !== "directory") continue;
			const target = path.resolve(workspace.sandboxRoot, change.resource);
			if (!sameDirectoryState(await readSandboxDirectoryState(target), change.after)) {
				throw new Error(`execution checkpoint directory mismatch: ${change.resource}`);
			}
		}
	}
}

async function cleanupPrivateSandboxWorkspace(workspace: PrivateSandboxWorkspace): Promise<void> {
	let safeToRelease = true;
	const failures: unknown[] = [];
	try {
		await workspace.transactions.dispose();
	} catch (error) {
		failures.push(error);
	}
	if (workspace.overlay) {
		try {
			await workspace.overlay.close();
		} catch (error) {
			safeToRelease = false;
			failures.push(error);
		}
	} else {
		await git(
			workspace.gitBinary,
			["--git-dir", workspace.repository, "worktree", "remove", "--force", workspace.sandboxRoot],
			workspace.processRoot,
		).catch(() => undefined);
	}
	if (safeToRelease) {
		await rm(workspace.processRoot, { recursive: true, force: true }).catch((error) => failures.push(error));
		if (workspace.overlayStorageRoot) {
			await rm(workspace.overlayStorageRoot, { recursive: true, force: true }).catch((error) => failures.push(error));
		}
		if (workspace.sharedBaseline) releaseOverlayBaseline(workspace.sharedBaseline);
		releaseSandboxRepository(workspace.pool);
	} else {
		quarantineSandboxRepository(workspace.pool);
	}
	// A still-mounted FUSE view retains direct references to upper/work/lower. Leak those
	// resources deliberately rather than deleting or recycling storage under a live mount.
	if (failures.length) throw new AggregateError(failures, "sandbox workspace cleanup failed");
}

async function collectSandboxChanges(workspace: PrivateSandboxWorkspace): Promise<readonly SandboxFileChange[]> {
	const detected = workspace.overlay
		? await collectOverlayChangeResources(workspace)
		: await collectGitChangeResources(workspace);
	const resources = [...new Set([...detected, ...workspace.baselineFrontier.keys()])]
		.filter((resource) => !isSnapshotExcluded(slash(resource)))
		.sort();
	const changes: SandboxFileChange[] = [];
	for (const resource of resources) {
		if (!resource || path.isAbsolute(resource) || resource.split("/").includes("..")) {
			throw new Error(`invalid sandbox change path: ${resource}`);
		}
		const target = path.resolve(workspace.sourceRoot, resource);
		const sandboxTarget = path.resolve(workspace.sandboxRoot, resource);
		if (!contains(workspace.sourceRoot, target) || !contains(workspace.sandboxRoot, sandboxTarget)) {
			throw new Error(`sandbox change escapes workspace: ${resource}`);
		}
		await assertNoSymlinkPath(workspace.sourceRoot, target);
		await assertNoSymlinkPath(workspace.sandboxRoot, sandboxTarget);
		const before = await readBaselineState(workspace, resource);
		const after = await readRegularState(sandboxTarget);
		if (!sameOptionalState(before, after)) {
			changes.push({
				root: workspace.sourceRoot,
				target,
				resource,
				before: before?.content,
				after: after?.content,
				beforeMode: before?.mode,
				afterMode: after?.mode,
			});
		}
	}
	return changes;
}

async function collectGitChangeResources(workspace: PrivateSandboxWorkspace): Promise<readonly string[]> {
	const prefix = ["--git-dir", workspace.gitDirectory, "--work-tree", workspace.sandboxRoot];
	const readOnlyGitEnvironment = { GIT_OPTIONAL_LOCKS: "0" };
	const tracked = await git(
		workspace.gitBinary,
		[...prefix, "diff", "--name-only", "--no-renames", "-z", workspace.commit, "--"],
		workspace.sandboxRoot,
		readOnlyGitEnvironment,
	);
	const untracked = await git(
		workspace.gitBinary,
		[...prefix, "ls-files", "--others", "-z", "--"],
		workspace.sandboxRoot,
		readOnlyGitEnvironment,
	);
	if (process.platform === "win32") {
		const untrackedRoots = await git(
			workspace.gitBinary,
			[...prefix, "ls-files", "--others", "--directory", "-z", "--"],
			workspace.sandboxRoot,
			readOnlyGitEnvironment,
		);
		for (const resource of parseNullList(untrackedRoots)) {
			if (slash(resource).endsWith("/")) await assertNoDirectoryLinks(workspace.sandboxRoot, resource);
		}
	}
	return Object.freeze([...new Set([...parseNullList(tracked), ...parseNullList(untracked)])]);
}

interface OverlayStructureRemoval {
	readonly resource: string;
	readonly descendantsOnly: boolean;
}

interface OverlayStructureFrontier {
	readonly refresh: ReadonlySet<string>;
	readonly removals: readonly OverlayStructureRemoval[];
}

/**
 * Reconstruct the complete logical structure from one immutable lower snapshot plus the typed upper
 * journal. Only upper paths and their ancestor directories require fresh merged-view syscalls.
 */
async function captureOverlayWorkspaceStructure(
	workspace: PrivateSandboxWorkspace,
	baseline: WorkspaceStructureSnapshot,
): Promise<WorkspaceStructureSnapshot> {
	if (!workspace.overlay) throw new Error("OverlayFS structure frontier is unavailable");
	const frontier = await inspectOverlayStructureFrontier(workspace.overlay.upperRoot);
	const entries = new Map(baseline.entries);
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
	for (const resource of [...frontier.refresh].sort(comparePathDepth)) {
		const target = resource ? path.resolve(workspace.sandboxRoot, resource) : workspace.sandboxRoot;
		if (!contains(workspace.sandboxRoot, target)) throw new Error(`OverlayFS frontier escapes workspace: ${resource}`);
		const entry = await captureWorkspaceStructureEntry(
			target,
			resource ? [] : workspace.observationExcludes,
		);
		if (entry) entries.set(resource, entry);
		else entries.delete(resource);
	}
	const files = Math.max(0, entries.size - 1);
	return Object.freeze({
		root: workspace.sandboxRoot,
		entries,
		files,
		bytesRead: 0,
		complete: baseline.complete && files <= WORKSPACE_TRANSACTION_MAX_FILES,
	});
}

async function inspectOverlayStructureFrontier(upperRoot: string): Promise<OverlayStructureFrontier> {
	const refresh = new Set<string>([""]);
	const removals: OverlayStructureRemoval[] = [];
	let entries = 0;
	const addAncestors = (resource: string, includeSelf: boolean) => {
		let current = includeSelf ? path.normalize(resource) : path.dirname(path.normalize(resource));
		for (;;) {
			const relative = current === "." ? "" : current;
			refresh.add(relative);
			if (!relative) break;
			current = path.dirname(relative);
		}
	};
	const visit = async (directory: string, relativeDirectory: string): Promise<void> => {
		for (const child of await readdir(directory, { withFileTypes: true })) {
			if (++entries > WORKSPACE_TRANSACTION_MAX_FILES) throw new Error("OverlayFS structure frontier exceeds file limit");
			if (child.name === ".wh..wh..opq") {
				removals.push({ resource: relativeDirectory, descendantsOnly: true });
				addAncestors(relativeDirectory, true);
				continue;
			}
			if (child.name.startsWith(".wh.")) {
				throw new Error(`unsupported OverlayFS whiteout encoding: ${child.name}`);
			}
			const resource = slash(relativeDirectory ? path.join(relativeDirectory, child.name) : child.name);
			if (isSnapshotExcluded(resource)) continue;
			const target = path.join(directory, child.name);
			const stats = await lstat(target);
			if (stats.isDirectory()) {
				addAncestors(resource, true);
				await visit(target, resource);
				continue;
			}
			if (stats.isCharacterDevice()) {
				if (stats.rdev !== 0) throw new Error(`unsupported OverlayFS device entry: ${resource}`);
				removals.push({ resource, descendantsOnly: false });
				addAncestors(resource, false);
				continue;
			}
			if (!stats.isFile()) throw new Error(`unsupported OverlayFS upper inode: ${resource}`);
			addAncestors(resource, true);
		}
	};
	await visit(upperRoot, "");
	return { refresh, removals: Object.freeze(removals) };
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
	let entries = 0;
	const addBaselineSubtree = async (resource: string) => {
		const prefix = resource || ".";
		const tree = await git(
			workspace.gitBinary,
			["-C", workspace.baselineRoot, "ls-tree", "-r", "-z", "--full-tree", workspace.commit, "--", prefix],
			workspace.baselineRoot,
			{ GIT_OPTIONAL_LOCKS: "0" },
		);
		for (const record of parseNullList(tree)) {
			const separator = record.indexOf("\t");
			if (separator === -1) throw new Error(`invalid Git subtree entry: ${resource}`);
			const candidate = record.slice(separator + 1);
			if (candidate && !isSnapshotExcluded(slash(candidate))) resources.add(candidate);
		}
		const normalized = resource ? `${path.normalize(resource)}${path.sep}` : "";
		for (const candidate of workspace.baselineFrontier.keys()) {
			if (candidate === resource || (!resource || path.normalize(candidate).startsWith(normalized))) {
				resources.add(candidate);
			}
		}
	};
	const visit = async (directory: string, relativeDirectory: string): Promise<void> => {
		for (const child of await readdir(directory, { withFileTypes: true })) {
			if (++entries > WORKSPACE_TRANSACTION_MAX_FILES) throw new Error("OverlayFS change journal exceeds file limit");
			if (child.name === ".wh..wh..opq") {
				await addBaselineSubtree(relativeDirectory);
				continue;
			}
			if (child.name.startsWith(".wh.")) {
				throw new Error(`unsupported OverlayFS whiteout encoding: ${child.name}`);
			}
			const resource = slash(relativeDirectory ? path.join(relativeDirectory, child.name) : child.name);
			if (isSnapshotExcluded(resource)) continue;
			const target = path.join(directory, child.name);
			const stats = await lstat(target);
			if (stats.isDirectory()) {
				await visit(target, resource);
				continue;
			}
			if (stats.isCharacterDevice()) {
				if (stats.rdev !== 0) throw new Error(`unsupported OverlayFS device entry: ${resource}`);
				await addBaselineSubtree(resource);
				continue;
			}
			resources.add(resource);
		}
	};
	await visit(workspace.overlay.upperRoot, "");
	return Object.freeze([...resources]);
}

async function readBaselineState(
	workspace: PrivateSandboxWorkspace,
	resource: string,
): Promise<RegularFileState | undefined> {
	if (workspace.baselineFrontier.has(resource)) return workspace.baselineFrontier.get(resource);
	const entry = await git(
		workspace.gitBinary,
		["-C", workspace.baselineRoot, "ls-tree", "-z", workspace.commit, "--", resource],
		workspace.baselineRoot,
	);
	if (entry.length === 0) return undefined;
	const metadata = entry.subarray(0, entry.indexOf(0)).toString("utf8").split("\t", 1)[0];
	const [mode, kind, hash] = metadata.split(" ");
	if ((mode !== "100644" && mode !== "100755") || kind !== "blob") {
		throw new Error(`sandbox baseline resource is not a regular file: ${resource}`);
	}
	if (!hash) throw new Error(`invalid Git baseline entry: ${resource}`);
	return {
		content: await git(
			workspace.gitBinary,
			["-C", workspace.baselineRoot, "cat-file", "blob", hash],
			workspace.baselineRoot,
		),
		mode: process.platform === "win32" ? 0 : mode === "100755" ? 0o755 : 0o644,
	};
}

async function readGitTreeRegularState(
	gitBinary: string,
	gitRoot: string,
	tree: string,
	resource: string,
	maxBytes = WORKSPACE_TRANSACTION_MAX_BYTES,
): Promise<RegularFileState | undefined> {
	const entry = await git(gitBinary, ["-C", gitRoot, "ls-tree", "-z", tree, "--", resource], gitRoot);
	if (entry.length === 0) return undefined;
	const terminator = entry.indexOf(0);
	if (terminator === -1) throw new Error(`invalid Git transaction entry: ${resource}`);
	const metadata = entry.subarray(0, terminator).toString("utf8").split("\t", 1)[0];
	const [mode, kind, hash] = metadata.split(" ");
	if ((mode !== "100644" && mode !== "100755") || kind !== "blob") {
		throw new Error(`workspace transaction resource is not a regular file: ${resource}`);
	}
	if (!hash) throw new Error(`invalid Git transaction blob: ${resource}`);
	const content = await git(
			gitBinary,
			["-C", gitRoot, "cat-file", "blob", hash],
			gitRoot,
			{},
			Math.max(1, Math.min(WORKSPACE_TRANSACTION_MAX_BYTES, maxBytes) + 1),
		);
	if (content.byteLength > maxBytes) throw new Error(`Git transaction blob exceeds capture limit: ${resource}`);
	return {
		content,
		mode: process.platform === "win32" ? 0 : mode === "100755" ? 0o755 : 0o644,
	};
}

async function assertNoSymlinkPath(root: string, target: string): Promise<void> {
	const resolvedRoot = path.resolve(root);
	const resolvedTarget = path.resolve(target);
	if (!contains(resolvedRoot, resolvedTarget) && resolvedRoot !== resolvedTarget) {
		throw new Error(`sandbox path escapes workspace: ${resolvedTarget}`);
	}
	try {
		const rootInfo = await lstat(resolvedRoot);
		if (rootInfo.isSymbolicLink() || !rootInfo.isDirectory()) {
			throw new Error("sandbox workspace root must be a real directory");
		}
	} catch (error) {
		if (isMissing(error)) throw new Error(`sandbox workspace root does not exist: ${resolvedRoot}`, { cause: error });
		throw error;
	}
	const relative = path.relative(resolvedRoot, resolvedTarget);
	let current = resolvedRoot;
	for (const segment of relative === "" ? [] : relative.split(path.sep)) {
		current = path.join(current, segment);
		try {
			const stats = await lstat(current);
			if (stats.isSymbolicLink()) {
				throw new Error(`sandbox path contains symlink: ${slash(path.relative(resolvedRoot, current))}`);
			}
		} catch (error) {
			if (isMissing(error)) break;
			throw error;
		}
	}
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

async function assertCommitTarget(change: SandboxWorkspaceChange): Promise<void> {
	const root = path.resolve(change.root);
	const target = path.resolve(change.target);
	if (!contains(root, target) || target === root || target !== path.resolve(root, change.resource)) {
		throw new Error(`sandbox commit path escapes workspace: ${change.resource}`);
	}
	await assertNoSymlinkPath(root, target);
}

async function readRegularState(target: string, maxBytes = Number.POSITIVE_INFINITY): Promise<RegularFileState | undefined> {
	let handle: FileHandle;
	try {
		const noFollow = typeof fsConstants.O_NOFOLLOW === "number" ? fsConstants.O_NOFOLLOW : 0;
		handle = await open(target, fsConstants.O_RDONLY | noFollow);
	} catch (error) {
		if (isMissing(error)) return undefined;
		if (error && typeof error === "object" && "code" in error && error.code === "ELOOP") {
			throw new Error(`symbolic links are not committable sandbox resources: ${target}`, { cause: error });
		}
		throw error;
	}
	try {
		const before = await handle.stat();
		if (!before.isFile()) throw new Error(`sandbox resource is not a regular file: ${target}`);
		if (before.size > maxBytes) throw new Error(`sandbox resource exceeds capture limit: ${target}`);
		const content = await handle.readFile();
		const after = await handle.stat();
		if (content.byteLength !== before.size || !sameOpenFileIdentity(before, after)) {
			throw new Error(`sandbox resource changed while being captured: ${target}`);
		}
		return {
			content,
			mode: process.platform === "win32" ? 0 : before.mode & 0o777,
		};
	} finally {
		await handle.close();
	}
}

/** Capture a directory without following links and reject concurrent namespace changes. */
export async function readSandboxDirectoryState(target: string): Promise<SandboxDirectoryState | undefined> {
	let before: Awaited<ReturnType<typeof lstat>>;
	try {
		before = await lstat(target);
	} catch (error) {
		if (isMissing(error)) return undefined;
		throw error;
	}
	if (before.isSymbolicLink() || !before.isDirectory()) {
		throw new Error(`sandbox resource is not a real directory: ${target}`);
	}
	const entries = await readdir(target, { withFileTypes: true });
	const after = await lstat(target);
	if (!after.isDirectory() || after.isSymbolicLink() || !sameDirectoryCaptureIdentity(before, after)) {
		throw new Error(`sandbox directory changed while being captured: ${target}`);
	}
	return {
		entriesDigest: directoryEntriesDigest(entries),
		mode: before.mode & 0o777,
		uid: before.uid,
		gid: before.gid,
	};
}

function sameDirectoryCaptureIdentity(
	left: Awaited<ReturnType<typeof lstat>>,
	right: Awaited<ReturnType<typeof lstat>>,
): boolean {
	return (
		left.dev === right.dev &&
		left.ino === right.ino &&
		left.mode === right.mode &&
		left.uid === right.uid &&
		left.gid === right.gid &&
		left.nlink === right.nlink &&
		left.size === right.size &&
		left.mtimeMs === right.mtimeMs &&
		left.ctimeMs === right.ctimeMs
	);
}

function sameOpenFileIdentity(left: Awaited<ReturnType<FileHandle["stat"]>>, right: Awaited<ReturnType<FileHandle["stat"]>>): boolean {
	return (
		left.dev === right.dev &&
		left.ino === right.ino &&
		left.mode === right.mode &&
		left.nlink === right.nlink &&
		left.size === right.size &&
		left.mtimeMs === right.mtimeMs &&
		left.ctimeMs === right.ctimeMs
	);
}

function sameBaselineState(current: RegularFileState | undefined, change: SandboxFileChange): boolean {
	if (current === undefined || change.before === undefined) {
		return current === undefined && change.before === undefined;
	}
	if (!sameBytes(current.content, change.before)) return false;
	return (
		change.beforeMode === undefined || change.beforeMode === 0 || sameExecutableMode(current.mode, change.beforeMode)
	);
}

function sameDirectoryState(
	left: SandboxDirectoryState | undefined,
	right: SandboxDirectoryState | undefined,
): boolean {
	if (!left || !right) return left === right;
	return (
		left.entriesDigest === right.entriesDigest &&
		left.mode === right.mode &&
		left.uid === right.uid &&
		left.gid === right.gid
	);
}

function sameSandboxBaseline(
	current: RegularFileState | SandboxDirectoryState | undefined,
	change: SandboxWorkspaceChange,
): boolean {
	return change.kind === "directory"
		? sameDirectoryState(current as SandboxDirectoryState | undefined, change.before)
		: sameBaselineState(current as RegularFileState | undefined, change);
}

function sameOptionalState(left: RegularFileState | undefined, right: RegularFileState | undefined): boolean {
	if (!left || !right) return left === right;
	if (!sameBytes(left.content, right.content)) return false;
	return right.mode === 0 || sameExecutableMode(left.mode, right.mode);
}

function deduplicateChanges(changes: readonly SandboxWorkspaceChange[]): SandboxWorkspaceChange[] {
	const result = new Map<string, SandboxWorkspaceChange>();
	for (const change of changes) {
		const key = pathKey(change.target);
		const previous = result.get(key);
		if (!previous) {
			result.set(key, change);
			continue;
		}
		if (
			pathKey(previous.root) !== pathKey(change.root) ||
			(previous.kind === "directory") !== (change.kind === "directory")
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		if (previous.kind === "directory" && change.kind === "directory") {
			if (!sameDirectoryState(previous.before, change.before)) {
				throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
			}
			result.set(key, { ...change, before: previous.before });
			continue;
		}
		const previousFile = previous as SandboxFileChange;
		const changeFile = change as SandboxFileChange;
		if (
			!sameOptionalBytes(previousFile.before, changeFile.before) ||
			(previousFile.beforeMode !== undefined &&
				changeFile.beforeMode !== undefined &&
				!sameExecutableMode(previousFile.beforeMode, changeFile.beforeMode))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		result.set(key, { ...changeFile, before: previousFile.before, beforeMode: previousFile.beforeMode });
	}
	return [...result.values()].sort((left, right) => pathKey(left.target).localeCompare(pathKey(right.target)));
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
					try {
						await rmdir(change.target);
					} catch (error) {
						if (!isMissing(error)) throw error;
					}
				} else {
					const current = await readSandboxDirectoryState(change.target);
					if (!current) {
						await createParentDirectories(change.root, change.target);
						await mkdir(change.target, { mode: directory.mode });
					}
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
			const current =
				change.kind === "directory"
					? await readSandboxDirectoryState(change.target)
					: await readRegularState(change.target);
			if (
				change.kind === "directory"
					? !sameDirectoryState(current as SandboxDirectoryState | undefined, baseline as SandboxDirectoryState | undefined)
					: !sameOptionalState(current as RegularFileState | undefined, baseline as RegularFileState | undefined)
			) {
				throw new Error(`sandbox rollback did not restore: ${change.resource}`);
			}
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

function sameExecutableMode(left: number, right: number): boolean {
	return isExecutableMode(left) === isExecutableMode(right);
}

function isExecutableMode(mode: number): boolean {
	return (mode & 0o111) !== 0;
}

async function atomicWrite(target: string, content: Uint8Array, mode: number | undefined, sourceRoot: string) {
	const temporary = await stageAtomicWrite(content, mode, sourceRoot);
	try {
		await createParentDirectories(sourceRoot, target);
		await replaceFile(temporary, target, mode);
	} finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
}

async function stageAtomicWrite(
	content: Uint8Array,
	mode: number | undefined,
	stagingDirectory: string,
): Promise<string> {
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

async function createParentDirectories(sourceRoot: string, target: string): Promise<string[]> {
	const root = path.resolve(sourceRoot);
	const parent = path.dirname(path.resolve(target));
	const relative = path.relative(root, parent);
	if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
		throw new Error(`sandbox commit path escapes workspace: ${target}`);
	}
	const created: string[] = [];
	let current = root;
	for (const segment of relative.split(path.sep).filter(Boolean)) {
		current = path.join(current, segment);
		try {
			await mkdir(current);
			created.push(current);
		} catch (error) {
			if (!(error && typeof error === "object" && "code" in error && error.code === "EEXIST")) throw error;
			const info = await lstat(current);
			if (info.isSymbolicLink() || !info.isDirectory()) {
				throw new Error(`sandbox commit parent is not a real directory: ${current}`, { cause: error });
			}
		}
	}
	return created;
}

async function removeCreatedDirectories(directories: readonly string[]): Promise<void> {
	for (const directory of [...new Set(directories)].sort((left, right) => right.length - left.length)) {
		try {
			await rmdir(directory);
		} catch (error) {
			const code = error && typeof error === "object" && "code" in error ? error.code : undefined;
			if (code !== "ENOENT" && code !== "ENOTEMPTY" && code !== "EEXIST") throw error;
		}
	}
}

async function replaceFile(temporary: string, target: string, mode?: number): Promise<void> {
	await rename(temporary, target);
	if (mode !== undefined && process.platform !== "win32") await chmod(target, mode);
}

function orderSandboxChanges(changes: readonly SandboxWorkspaceChange[]): SandboxWorkspaceChange[] {
	const phase = (change: SandboxWorkspaceChange): number => {
		if (change.kind !== "directory") return change.after === undefined ? 0 : 3;
		return change.after === undefined ? 1 : 2;
	};
	const depth = (change: SandboxWorkspaceChange): number =>
		slash(change.resource).split("/").filter(Boolean).length;
	return [...changes].sort((left, right) => {
		const phaseDifference = phase(left) - phase(right);
		if (phaseDifference !== 0) return phaseDifference;
		const depthDifference = depth(left) - depth(right);
		if (phase(left) <= 1) {
			if (depthDifference !== 0) return -depthDifference;
		} else if (depthDifference !== 0) return depthDifference;
		return pathKey(left.target).localeCompare(pathKey(right.target));
	});
}

/** Root locks make namespace changes conflict with every file commit below the same workspace. */
function commitLockTargets(changes: readonly SandboxWorkspaceChange[]): string[] {
	return [...new Set(changes.flatMap((change) => [path.resolve(change.root), path.resolve(change.target)]))];
}

async function withTargetLocks<T>(
	targetLocks: Map<string, Promise<void>>,
	targets: readonly string[],
	run: () => Promise<T>,
): Promise<T> {
	const releases: Array<() => void> = [];
	try {
		for (const target of [...new Set(targets.map(pathKey))].sort()) {
			releases.push(await acquireTargetLock(targetLocks, target));
		}
		return await run();
	} finally {
		for (const release of releases.reverse()) release();
	}
}

async function acquireTargetLock(targetLocks: Map<string, Promise<void>>, key: string): Promise<() => void> {
	const previous = targetLocks.get(key) ?? Promise.resolve();
	let release: () => void = () => undefined;
	const current = new Promise<void>((resolve) => {
		release = resolve;
	});
	const tail = previous.then(() => current);
	targetLocks.set(key, tail);
	await previous;
	return () => {
		release();
		if (targetLocks.get(key) === tail) targetLocks.delete(key);
	};
}

function isMissing(error: unknown): boolean {
	return !!error && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

async function exists(target: string): Promise<boolean> {
	try {
		await lstat(target);
		return true;
	} catch (error) {
		if (isMissing(error)) return false;
		throw error;
	}
}

function sameOptionalBytes(left: Uint8Array | undefined, right: Uint8Array | undefined): boolean {
	if (left === undefined || right === undefined) return left === right;
	return sameBytes(left, right);
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
	if (left.length !== right.length) return false;
	for (let index = 0; index < left.length; index++) if (left[index] !== right[index]) return false;
	return true;
}

function parseNullList(value: Uint8Array): string[] {
	return value
		.toString()
		.split("\0")
		.filter(Boolean)
		.map((item) => slash(item));
}

function replacePaths<T>(value: T, replacements: readonly (readonly [string, string])[]): T {
	if (typeof value === "string") {
		let result: string = value;
		for (const [from, to] of replacements) result = result.replaceAll(from, to);
		return result as T;
	}
	if (Array.isArray(value)) return value.map((item) => replacePaths(item, replacements)) as T;
	if (!value || typeof value !== "object" || value instanceof Uint8Array) return value;
	if (Object.getPrototypeOf(value) !== Object.prototype) return value;
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, replacePaths(item, replacements)]),
	) as T;
}

function git(
	command: string,
	args: readonly string[],
	cwd: string,
	environment: Readonly<Record<string, string>> = {},
	maxBuffer = 64 * 1024 * 1024,
): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		execFile(
			command,
			[...args],
			{
				cwd,
				env: { ...process.env, ...environment },
				encoding: "buffer",
				maxBuffer,
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
		);
	});
}
