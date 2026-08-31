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
	WorldBranchState,
	WorldCheckpoint,
	WorldCommitMetrics,
	WorldCompatibilityEvidence,
	WorldExecutionMetrics,
} from "./execution-world.ts";
import { WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import { ResourceVersionManager, type ResourceVersionToken } from "./resource-version.ts";
import type { ResourceValidation } from "./settlement.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

export interface SandboxFileChange {
	readonly root: string;
	readonly target: string;
	readonly resource: string;
	readonly before?: Uint8Array;
	readonly after?: Uint8Array;
	readonly beforeMode?: number;
	readonly afterMode?: number;
}

interface RegularFileState {
	readonly content: Uint8Array;
	readonly mode: number;
}

export interface SandboxExecutionDelta {
	readonly output: ToolSettlement;
	readonly changes: readonly SandboxFileChange[];
}

interface WorkspaceExecutionSnapshot extends SandboxExecutionDelta {
	readonly executionMetrics: WorldExecutionMetrics;
}

export interface WorkspaceSandboxOptions {
	readonly gitBinary?: string;
}

export interface SandboxWorkspaceContext {
	readonly sourceRoot: string;
	readonly sandboxRoot: string;
	readonly processRoot: string;
}

export interface SandboxWorkspaceBranchOptions {
	readonly cwd: string;
	readonly action: SpeculativeToolExecutionContext["action"];
	readonly parentCheckpoint?: WorldCheckpoint;
	readonly gitBinary?: string;
	readonly execute: (workspace: SandboxWorkspaceContext) => Promise<ToolSettlement>;
	/** Optional exact freshness proof captured by the operation-specific execution substrate. */
	readonly validate?: () => Promise<ResourceValidation>;
}

export interface PrepareSandboxWorkspaceOptions {
	readonly gitBinary?: string;
	readonly signal?: AbortSignal;
}

interface PrivateGitWorkspace extends SandboxWorkspaceContext {
	readonly repository: string;
	readonly gitBinary: string;
	readonly pool: PooledGitRepository;
}

interface PooledGitRepository {
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
	idleTimer?: ReturnType<typeof setTimeout>;
}

interface PreparedGitWorkspace {
	readonly sandboxRoot: string;
	readonly processRoot: string;
	readonly commit: string;
}

// The execution world mirrors everything the actor can read below cwd. Git metadata is
// replaced by the private repository and commit's own temporary files are internal.
const SNAPSHOT_EXCLUDES = [".git"] as const;
const SANDBOX_REPOSITORY_IDLE_MS = 5 * 60 * 1000;
const GIT_PATHSPEC_BATCH_BYTES = 32 * 1024;
const SANDBOX_STAGING_FILE_PREFIX = ".pi-speculative-";
const sandboxRepositories = new Map<string, Promise<PooledGitRepository>>();
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
	readonly changes: readonly SandboxFileChange[];

	constructor(sourceRoot: string, parent: GitWorldCheckpoint | undefined, changes: readonly SandboxFileChange[]) {
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

/** Create a copy-on-write execution world with transactional multi-file commit. */
export function createWorkspaceSandbox(options: WorkspaceSandboxOptions = {}): SpeculativeAgentExecutionWorld {
	const roots = new Set<string>();
	return {
		id: "git_worktree",
		scope: "fallback",
		isolation: "workspace_branch",
		capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
		fingerprint: () => "git-worktree:v1",
		prepare: async ({ cwd, signal }) => {
			roots.add(path.resolve(cwd));
			await prepareSandboxWorkspace(cwd, { ...(options.gitBinary ? { gitBinary: options.gitBinary } : {}), signal });
		},
		fork: async (context) => {
			const sourceRoot = path.resolve(context.cwd);
			roots.add(sourceRoot);
			return executeMutation(context, options.gitBinary);
		},
		dispose: async () => {
			const ownedRoots = [...roots];
			roots.clear();
			await closeWorkspaceSandboxPools(ownedRoots);
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
	private readonly changes: readonly SandboxFileChange[];
	private stateValue: WorldBranchState = "sealed";
	private commitMetricsValue?: WorldCommitMetrics;
	private commitPromise?: Promise<ToolSettlement>;

	constructor(
		snapshot: WorkspaceExecutionSnapshot,
		sourceRoot: string,
		executionFingerprint: string,
		parent?: GitWorldCheckpoint,
		validate?: () => Promise<ResourceValidation>,
	) {
		this.output = snapshot.output;
		this.changes = Object.freeze([...snapshot.changes]);
		this.checkpoint = new GitWorldCheckpoint(sourceRoot, parent, this.changes);
		this.resources = Object.freeze([...new Set(this.changes.map((change) => change.resource))]);
		this.capturedBytes = this.changes.reduce(
			(total, change) => total + (change.before?.byteLength ?? 0) + (change.after?.byteLength ?? 0),
			0,
		);
		this.executionMetrics = Object.freeze({ ...snapshot.executionMetrics });
		this.compatibility = Object.freeze({
			status: "compatible" as const,
			backend: this.backend,
			executionFingerprint,
		});
		this.validate = validate;
	}

	get state(): WorldBranchState {
		return this.stateValue;
	}

	get commitMetrics(): WorldCommitMetrics | undefined {
		return this.commitMetricsValue;
	}

	readonly commit = (): Promise<ToolSettlement> => {
		if (this.commitPromise) return this.commitPromise;
		this.stateValue = "committing";
		this.commitPromise = commitSandboxExecution({ output: this.output, changes: this.changes }).then(
			({ output, metrics }) => {
				this.commitMetricsValue = metrics;
				this.stateValue = "committed";
				return output;
			},
			(error) => {
				this.stateValue = "failed";
				throw error;
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
	return (await commitSandboxExecution(delta)).output;
}

async function commitSandboxExecution(
	execution: SandboxExecutionDelta,
): Promise<{ readonly output: ToolSettlement; readonly metrics: WorldCommitMetrics }> {
	const started = performance.now();
	const changes = deduplicateChanges(execution.changes);
	return withTargetLocks(
		changes.map((change) => change.target),
		async () => {
			const staged = new Map<SandboxFileChange, string>();
			const baselines = new Map<SandboxFileChange, RegularFileState | undefined>();
			const commitModes = new Map<SandboxFileChange, number | undefined>();
			const applied: SandboxFileChange[] = [];
			const createdDirectories: string[] = [];
			let bytesValidated = 0;
			let validationMs = 0;
			let resourcesCommitted = 0;
			try {
				for (const change of changes) await assertCommitTarget(change);
				for (const change of changes) {
					if (change.after !== undefined) {
						staged.set(change, await stageAtomicWrite(change.after, change.afterMode, change.root));
					}
				}
				const validationStarted = performance.now();
				for (const change of changes) {
					const current = await readRegularState(change.target);
					baselines.set(change, current);
					bytesValidated += current?.content.byteLength ?? 0;
					if (!sameBaselineState(current, change)) {
						throw new Error(`resource changed before commit: ${change.resource}`);
					}
					commitModes.set(change, resolveCommitMode(current, change));
				}
				validationMs = Math.max(0, performance.now() - validationStarted);
				for (const change of changes) {
					await assertCommitTarget(change);
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
	const workspace = await createPrivateGitWorkspace(cwd, gitBinary);
	try {
		return await run(workspace);
	} finally {
		await cleanupPrivateGitWorkspace(workspace);
	}
}

/**
 * Fork one generic operation into a private workspace and seal its output together with the
 * complete regular-file delta. Process and host-function worlds share this primitive.
 */
export async function forkSandboxWorkspace(options: SandboxWorkspaceBranchOptions): Promise<WorldBranch<ToolSettlement>> {
	const sourceRoot = path.resolve(options.cwd);
	const parent = resolveWorkspaceCheckpoint(options.parentCheckpoint, sourceRoot);
	const setupStarted = performance.now();
	const snapshot = await withPrivateGitWorkspace(
		sourceRoot,
		options.gitBinary ?? "git",
		async (workspace) => {
			const setupMs = Math.max(0, performance.now() - setupStarted);
			const output = await options.execute(workspace);
			const captureStarted = performance.now();
			const changes = await collectSandboxChanges(workspace);
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
	return new GitWorldBranch(snapshot, sourceRoot, options.action.executionFingerprint, parent, options.validate);
}

export async function prepareSandboxWorkspace(
	cwd: string,
	options: PrepareSandboxWorkspaceOptions = {},
): Promise<void> {
	throwIfAborted(options.signal);
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	const repository = await acquireSandboxRepository(sourceRoot, options.gitBinary ?? "git");
	try {
		await ensurePreparedSandbox(repository);
		throwIfAborted(options.signal);
	} finally {
		releaseSandboxRepository(repository);
	}
}

async function executeMutation(
	context: SpeculativeToolExecutionContext,
	gitBinary?: string,
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
	return forkSandboxWorkspace({
		cwd: sourceRoot,
		action: context.action,
		...(context.parentCheckpoint ? { parentCheckpoint: context.parentCheckpoint } : {}),
		...(gitBinary ? { gitBinary } : {}),
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

async function createPrivateGitWorkspace(cwd: string, gitBinary: string): Promise<PrivateGitWorkspace> {
	const sourceRoot = path.resolve(cwd);
	await assertNoSymlinkPath(sourceRoot, sourceRoot);
	const pool = await acquireSandboxRepository(sourceRoot, gitBinary);
	try {
		const commit = await acquireSandboxBaseline(pool, SANDBOX_AUTHOR_ENVIRONMENT);
		const workspace = (await takePreparedSandbox(pool, commit)) ?? (await attachSandboxWorkspace(pool, commit));
		return {
			sourceRoot,
			sandboxRoot: workspace.sandboxRoot,
			processRoot: workspace.processRoot,
			repository: pool.repository,
			gitBinary,
			pool,
		};
	} catch (error) {
		releaseSandboxRepository(pool);
		throw error;
	}
}

async function acquireSandboxRepository(sourceRoot: string, gitBinary: string): Promise<PooledGitRepository> {
	const key = `${pathKey(sourceRoot)}\0${gitBinary}`;
	let pending = sandboxRepositories.get(key);
	if (!pending) {
		pending = createSandboxRepository(sourceRoot, gitBinary);
		sandboxRepositories.set(key, pending);
		void pending.catch(() => {
			if (sandboxRepositories.get(key) === pending) sandboxRepositories.delete(key);
		});
	}
	const repository = await pending;
	if (repository.idleTimer) {
		clearTimeout(repository.idleTimer);
		repository.idleTimer = undefined;
	}
	repository.active++;
	return repository;
}

async function createSandboxRepository(sourceRoot: string, gitBinary: string): Promise<PooledGitRepository> {
	const parent = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-action-pool-"));
	const repository = path.join(parent, "snapshot.git");
	try {
		await git(gitBinary, ["init", "--bare", repository], parent);
		await git(gitBinary, ["--git-dir", repository, "config", "core.autocrlf", "false"], parent);
		await git(gitBinary, ["--git-dir", repository, "config", "core.longpaths", "true"], parent);
		return {
			sourceRoot,
			parent,
			repository,
			gitBinary,
			versions: new ResourceVersionManager(sourceRoot),
			active: 0,
			idleWaiters: new Set(),
			lock: Promise.resolve(),
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
		return { sandboxRoot, processRoot, commit };
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
	if (repository.active > 0 || repository.idleTimer) return;
	repository.idleTimer = setTimeout(() => {
		if (repository.active > 0) return;
		sandboxRepositories.delete(`${pathKey(repository.sourceRoot)}\0${repository.gitBinary}`);
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

export async function closeWorkspaceSandboxPools(roots?: readonly string[]): Promise<void> {
	const rootKeys = roots ? new Set(roots.map(pathKey)) : undefined;
	const pending = [...sandboxRepositories.entries()].filter(([key]) => {
		if (!rootKeys) return true;
		const separator = key.indexOf("\0");
		return rootKeys.has(separator === -1 ? key : key.slice(0, separator));
	});
	for (const [key, item] of pending) {
		if (sandboxRepositories.get(key) === item) sandboxRepositories.delete(key);
		const repository = await item.catch(() => undefined);
		if (!repository) continue;
		await waitForSandboxRepositoryIdle(repository);
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
	const basename = path.posix.basename(relative);
	return (
		relative.split("/").some((segment) => (SNAPSHOT_EXCLUDES as readonly string[]).includes(segment)) ||
		(basename.startsWith(SANDBOX_STAGING_FILE_PREFIX) && basename.endsWith(".tmp"))
	);
}

function pathKey(value: string): string {
	const normalized = path.resolve(value).replaceAll("\\", "/");
	return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

async function withPrivateGitWorkspace<T>(
	cwd: string,
	gitBinary: string,
	run: (workspace: PrivateGitWorkspace) => Promise<T>,
	checkpoint?: GitWorldCheckpoint,
): Promise<T> {
	const workspace = await createPrivateGitWorkspace(cwd, gitBinary);
	try {
		if (checkpoint) await materializeCheckpoint(workspace, checkpoint);
		return await run(workspace);
	} finally {
		await cleanupPrivateGitWorkspace(workspace);
	}
}

async function materializeCheckpoint(workspace: PrivateGitWorkspace, checkpoint: GitWorldCheckpoint): Promise<void> {
	const lineage: GitWorldCheckpoint[] = [];
	for (let current: GitWorldCheckpoint | undefined = checkpoint; current; current = current.parent) {
		lineage.push(current);
	}
	for (const ancestor of lineage.reverse()) {
		for (const change of ancestor.changes) {
			const target = path.resolve(workspace.sandboxRoot, change.resource);
			if (!contains(workspace.sandboxRoot, target) || target === workspace.sandboxRoot) {
				throw new Error(`execution checkpoint escapes workspace: ${change.resource}`);
			}
			await assertNoSymlinkPath(workspace.sandboxRoot, target);
			if (change.after === undefined) await rm(target, { force: true });
			else await atomicWrite(target, change.after, change.afterMode, workspace.sandboxRoot);
		}
	}
	if (!lineage.some((ancestor) => ancestor.changes.length > 0)) return;
	await git(workspace.gitBinary, ["-C", workspace.sandboxRoot, "add", "--all", "--", "."], workspace.sandboxRoot);
	await git(
		workspace.gitBinary,
		["-C", workspace.sandboxRoot, "commit", "--allow-empty", "--no-gpg-sign", "-m", "speculative lineage"],
		workspace.sandboxRoot,
		SANDBOX_AUTHOR_ENVIRONMENT,
	);
}

async function cleanupPrivateGitWorkspace(workspace: PrivateGitWorkspace): Promise<void> {
	try {
		await git(
			workspace.gitBinary,
			["--git-dir", workspace.repository, "worktree", "remove", "--force", workspace.sandboxRoot],
			workspace.processRoot,
		);
	} catch {
		// The private parent removal below is the final cleanup boundary.
	}
	try {
		await rm(workspace.processRoot, { recursive: true, force: true });
	} finally {
		releaseSandboxRepository(workspace.pool);
	}
}

async function collectSandboxChanges(workspace: PrivateGitWorkspace): Promise<readonly SandboxFileChange[]> {
	const tracked = await git(
		workspace.gitBinary,
		["-C", workspace.sandboxRoot, "diff", "--name-only", "--no-renames", "-z", "HEAD", "--"],
		workspace.sandboxRoot,
	);
	const untracked = await git(
		workspace.gitBinary,
		["-C", workspace.sandboxRoot, "ls-files", "--others", "-z", "--"],
		workspace.sandboxRoot,
	);
	if (process.platform === "win32") {
		const untrackedRoots = await git(
			workspace.gitBinary,
			["-C", workspace.sandboxRoot, "ls-files", "--others", "--directory", "-z", "--"],
			workspace.sandboxRoot,
		);
		for (const resource of parseNullList(untrackedRoots)) {
			if (slash(resource).endsWith("/")) await assertNoDirectoryLinks(workspace.sandboxRoot, resource);
		}
	}
	const resources = [...new Set([...parseNullList(tracked), ...parseNullList(untracked)])]
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

async function readBaselineState(
	workspace: PrivateGitWorkspace,
	resource: string,
): Promise<RegularFileState | undefined> {
	const entry = await git(
		workspace.gitBinary,
		["-C", workspace.sandboxRoot, "ls-tree", "-z", "HEAD", "--", resource],
		workspace.sandboxRoot,
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
			["-C", workspace.sandboxRoot, "cat-file", "blob", hash],
			workspace.sandboxRoot,
		),
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

async function assertCommitTarget(change: SandboxFileChange): Promise<void> {
	const root = path.resolve(change.root);
	const target = path.resolve(change.target);
	if (!contains(root, target) || target === root || target !== path.resolve(root, change.resource)) {
		throw new Error(`sandbox commit path escapes workspace: ${change.resource}`);
	}
	await assertNoSymlinkPath(root, target);
}

async function readRegularState(target: string): Promise<RegularFileState | undefined> {
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
		const info = await handle.stat();
		if (!info.isFile()) throw new Error(`sandbox resource is not a regular file: ${target}`);
		return {
			content: await handle.readFile(),
			mode: process.platform === "win32" ? 0 : info.mode & 0o777,
		};
	} finally {
		await handle.close();
	}
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

function sameOptionalState(left: RegularFileState | undefined, right: RegularFileState | undefined): boolean {
	if (!left || !right) return left === right;
	if (!sameBytes(left.content, right.content)) return false;
	return right.mode === 0 || sameExecutableMode(left.mode, right.mode);
}

function deduplicateChanges(changes: readonly SandboxFileChange[]): SandboxFileChange[] {
	const result = new Map<string, SandboxFileChange>();
	for (const change of changes) {
		const key = pathKey(change.target);
		const previous = result.get(key);
		if (!previous) {
			result.set(key, change);
			continue;
		}
		if (
			pathKey(previous.root) !== pathKey(change.root) ||
			!sameOptionalBytes(previous.before, change.before) ||
			(previous.beforeMode !== undefined &&
				change.beforeMode !== undefined &&
				!sameExecutableMode(previous.beforeMode, change.beforeMode))
		) {
			throw new Error(`inconsistent sandbox baseline: ${change.resource}`);
		}
		result.set(key, {
			...change,
			before: previous.before,
			beforeMode: previous.beforeMode,
		});
	}
	return [...result.values()].sort((left, right) => pathKey(left.target).localeCompare(pathKey(right.target)));
}

async function restoreChanges(
	changes: readonly SandboxFileChange[],
	baselines: ReadonlyMap<SandboxFileChange, RegularFileState | undefined>,
): Promise<void> {
	const errors: unknown[] = [];
	for (const change of [...changes].reverse()) {
		try {
			await assertCommitTarget(change);
			const baseline = baselines.get(change);
			if (!baseline) await rm(change.target, { force: true });
			else await atomicWrite(change.target, baseline.content, baseline.mode, change.root);
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

const targetLocks = new Map<string, Promise<void>>();

async function withTargetLocks<T>(targets: readonly string[], run: () => Promise<T>): Promise<T> {
	const releases: Array<() => void> = [];
	try {
		for (const target of [...new Set(targets.map(pathKey))].sort()) releases.push(await acquireTargetLock(target));
		return await run();
	} finally {
		for (const release of releases.reverse()) release();
	}
}

async function acquireTargetLock(key: string): Promise<() => void> {
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
): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		execFile(
			command,
			[...args],
			{
				cwd,
				env: { ...process.env, ...environment },
				encoding: "buffer",
				maxBuffer: 64 * 1024 * 1024,
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
