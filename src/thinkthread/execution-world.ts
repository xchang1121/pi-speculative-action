import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
	AgentPosixClient,
	CONTRACT_FINGERPRINT,
	type FsDependency,
	type FsRunKeyParamsV1,
	type FsRunOutputChunkV1,
	type FsRunV1,
	type FsRunWrites,
	type FsSnapshotId,
} from "@thinkthread/agent-posix";
import type { SpeculativeAgentExecutionWorld, SpeculativeToolExecutionContext } from "../agent-execution-world.ts";
import {
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../effect-model.ts";
import {
	type ExecutionWorldRequest,
	type WorldBranch,
	type WorldCommitMetrics,
	WorldCommitRejectedError,
	type WorldCompatibilityEvidence,
	type WorldResultCapture,
} from "../execution-world.ts";
import { cause, type ResourceValidation } from "../settlement.ts";
import type { ToolInvocation, ToolSettlement } from "../tool-settlement.ts";
import { DurableFsExecutor } from "./durable-fs.ts";
import { ThinkThreadDurableError } from "./errors.ts";
import { type SnapshotLease, type ThinkThreadCheckpoint, ThinkThreadSnapshotPool } from "./snapshot-pool.ts";
import {
	decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest,
	THINKTHREAD_TOOL_RUNNER_VERSION,
	type ThinkThreadToolName,
} from "./tool-runner-protocol.ts";

const WORLD_ID = "ThinkThread";
const LINUX_EXECUTION_BACKEND_EPOCH = "linux-execution-v10";
const RUNNER_MAX_OUTPUT_BYTES = 512 * 1024;
const DEFAULT_RUN_TIMEOUT_MS = 120_000;
const MAX_RUN_TIMEOUT_MS = 300_000;
const BASH_TIMEOUT_BUFFER_MS = 5_000;
const DIFF_PAGE_LIMIT = 256;
const TOOL_NAMES = new Set<ThinkThreadToolName>(["read", "grep", "find", "ls", "write", "edit", "bash"]);
const OBSERVATION_TOOLS = new Set(["read", "grep", "find", "ls"]);

export interface ThinkThreadExecutionWorldOptions {
	readonly clientFactory?: () => AgentPosixClient;
	readonly runnerPath?: string;
	readonly runnerFingerprint?: string;
	readonly nodePath?: string;
	readonly autoResizeImages?: boolean;
}

export type ThinkThreadExecutionWorld = SpeculativeAgentExecutionWorld & {
	readonly beginTurn: (turnID: string) => Promise<void>;
	readonly actorFallbackSettled: () => Promise<void>;
	readonly finishTurn: (turnID: string) => Promise<void>;
};

export function createThinkThreadExecutionWorld(
	options: ThinkThreadExecutionWorldOptions = {},
): ThinkThreadExecutionWorld {
	const runnerPath = options.runnerPath ?? fileURLToPath(new URL("./tool-runner.js", import.meta.url));
	const nodePath = options.nodePath ?? process.execPath;
	const autoResizeImages = options.autoResizeImages ?? true;
	let prepared: Promise<PreparedWorld> | undefined;
	let runnerFingerprint: Promise<string> | undefined;
	const lifetime = new AbortController();
	const pending = new Set<Promise<unknown>>();
	let disposal: Promise<void> | undefined;

	const prepare = (cwd: string): Promise<PreparedWorld> => {
		lifetime.signal.throwIfAborted();
		if (!prepared) {
			const attempt = prepareWorld(cwd, options.clientFactory).catch((error) => {
				if (prepared === attempt) prepared = undefined;
				throw error;
			});
			prepared = attempt;
		}
		return prepared;
	};
	const fingerprint = async (request?: ExecutionWorldRequest): Promise<string> => {
		if (request?.action) toolName(request.action.tool);
		if (!runnerFingerprint) {
			const attempt = (options.runnerFingerprint
				? Promise.resolve(options.runnerFingerprint)
				: hashFile(runnerPath)).catch((error) => {
				if (runnerFingerprint === attempt) runnerFingerprint = undefined;
				throw error;
			});
			runnerFingerprint = attempt;
		}
		return [
			"thinkthread-fs-run",
			CONTRACT_FINGERPRINT,
			LINUX_EXECUTION_BACKEND_EPOCH,
			THINKTHREAD_TOOL_RUNNER_VERSION,
			await runnerFingerprint,
		].join(":");
	};
	const execute = async <Result>(
		context: SpeculativeToolExecutionContext,
		operation: (world: PreparedWorld, context: SpeculativeToolExecutionContext) => Promise<Result>,
	): Promise<Result> => {
		const signal = AbortSignal.any([context.signal, lifetime.signal]);
		signal.throwIfAborted();
		const task = prepare(context.cwd).then((world) => {
			signal.throwIfAborted();
			return operation(world, { ...context, signal });
		});
		pending.add(task);
		try {
			return await task;
		} finally {
			pending.delete(task);
		}
	};

	return {
		id: WORLD_ID,
		scope: "runtime",
		isolation: "runtime_sandbox",
		speculation: {
			capabilities: [...new Set([
				...RESOURCE_OBSERVATION_EFFECTS.capabilities,
				...WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
				...UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			])],
			fingerprint,
			prepare: async ({ cwd }) => { await prepare(cwd); },
			diagnostics: async ({ cwd }) => {
				await prepare(cwd);
				await fingerprint();
				return {
					state: "ready",
					detail: "ThinkThread fs.run; seven stock tools; inherited profile policy; no process-certificate replay",
				};
			},
			execute: (context) => execute(context, (world, input) =>
				forkThinkThreadWorld(world, input, runnerPath, nodePath, autoResizeImages)),
		},
		observation: {
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			fingerprint: (request) => {
				if (request.action && !OBSERVATION_TOOLS.has(request.action.tool)) {
					throw new Error(`ThinkThread cannot capture authoritative ${request.action.tool} results`);
				}
				return `thinkthread-fs-observation:v1:${CONTRACT_FINGERPRINT}`;
			},
			prepare: async ({ cwd }) => { await prepare(cwd); },
			diagnostics: async ({ cwd }) => {
				await prepare(cwd);
				return { state: "ready", detail: "ThinkThread snapshot/verify for Actor read, grep, find, and ls results" };
			},
			capture: (context) => execute(context, captureThinkThreadResult),
		},
		beginTurn: async (turnID) => {
			const world = await prepared;
			if (!world) throw new Error("ThinkThread execution world must be prepared before a turn starts");
			await world.pool.beginTurn(turnID);
		},
		actorFallbackSettled: async () => {
			const world = await prepared;
			await world?.pool.invalidate();
		},
		finishTurn: async (turnID) => {
			const world = await prepared;
			await world?.pool.finishTurn(turnID);
		},
		dispose: () => {
			disposal ??= (async () => {
				lifetime.abort();
				await Promise.allSettled([...pending]);
				const world = await prepared;
				await world?.pool.dispose();
			})();
			return disposal;
		},
	};
}

interface PreparedWorld {
	readonly client: AgentPosixClient;
	readonly durable: DurableFsExecutor;
	readonly pool: ThinkThreadSnapshotPool;
}

async function prepareWorld(cwd: string, clientFactory: (() => AgentPosixClient) | undefined): Promise<PreparedWorld> {
	if (process.platform !== "linux" && !clientFactory) {
		throw new Error("ThinkThread speculative execution is supported only on Linux");
	}
	const configuredFs = process.env.THINKTHREAD_FS;
	if (configuredFs && path.resolve(configuredFs) !== path.resolve(cwd)) {
		throw new Error(`Pi cwd ${cwd} does not match THINKTHREAD_FS ${configuredFs}`);
	}
	const client = clientFactory?.() ?? AgentPosixClient.fromEnv();
	const self = await client.selfView();
	if (!self.capabilities.some((capability) => capability.id === "thinkthread.fs.self" && capability.version === 1)) {
		throw new Error("ThinkThread profile does not delegate thinkthread.fs.self@1");
	}
	await client.fs.stat();
	const durable = new DurableFsExecutor(client);
	return { client, durable, pool: new ThinkThreadSnapshotPool(durable) };
}

async function forkThinkThreadWorld(
	world: PreparedWorld,
	context: SpeculativeToolExecutionContext,
	runnerPath: string,
	nodePath: string,
	autoResizeImages: boolean,
): Promise<WorldBranch<ToolSettlement>> {
	const setupStarted = performance.now();
	const source = context.parentCheckpoint
		? world.pool.acquireCheckpoint(context.parentCheckpoint)
		: await world.pool.acquireRoot();
	let target: SnapshotLease | undefined;
	try {
		const tool = toolName(context.toolName);
		const processInvocation = toolInvocation(context.action.executionContext)?.process;
		const request = encodeThinkThreadToolRunnerRequest({
			version: THINKTHREAD_TOOL_RUNNER_VERSION,
			tool,
			callID: context.callID,
			args: context.args,
			autoResizeImages,
			...(tool === "bash" && processInvocation?.shell ? { shellPath: processInvocation.shell } : {}),
			...(tool === "bash" ? shellCommandPrefix(context.action.executionContext) : {}),
		});
		const writes: FsRunWrites =
			context.action.tool === "read" ||
			context.action.tool === "grep" ||
			context.action.tool === "find" ||
			context.action.tool === "ls"
				? "deny"
				: "snapshot";
		const environment = invocationEnvironment(context.action.executionContext);
		const runParams: FsRunKeyParamsV1 = {
			snapshotId: source.lease.id,
			writes,
			invocation: {
				argv: [nodePath, runnerPath],
				cwd: ".",
				...(Object.keys(environment).length > 0 ? { environment } : {}),
			},
			limits: {
				timeoutMs: runTimeout(context),
				maxOutputBytes: RUNNER_MAX_OUTPUT_BYTES,
			},
		};
		const expectedKey = await world.durable.runKeyWithInput(runParams, request);
		const run = await world.durable.runWithInput(runParams, request, context.signal);
		if (run.targetSnapshotId) {
			target = world.pool.ownSnapshot({
				snapshotId: run.targetSnapshotId,
				logicalBytes: Math.max(0, run.changedBytes ?? 0),
			});
		}
		context.signal.throwIfAborted();
		if (run.runKey !== expectedKey.runKey) throw new Error("ThinkThread fs.run returned an unexpected run key");
		assertSuccessfulRun(run);
		const output = decodeThinkThreadToolRunnerResponse(outputBytes(run.outputChunks, "stdout"));
		const resources = target
			? await changedResources(world.client, source.lease.id, target.id)
			: [...context.action.resources];
		const setupMs = Math.max(0, performance.now() - setupStarted - run.metrics.executeMs - run.metrics.sealMs);
		const dependencies = actionDependencies(context);
		return new ThinkThreadWorldBranch({
			output,
			source: source.lease,
			target,
			lineage: source.lineage,
			depth: source.depth,
			resources,
			capturedBytes: Math.max(0, run.changedBytes ?? 0),
			setupMs,
			captureMs: run.metrics.sealMs,
			executionFingerprint: context.action.executionFingerprint,
			client: world.client,
			durable: world.durable,
			pool: world.pool,
			dependencies,
		});
	} catch (error) {
		await target?.release().catch(() => undefined);
		await source.lease.release().catch(() => undefined);
		throw error;
	}
}

async function captureThinkThreadResult(
	world: PreparedWorld,
	context: SpeculativeToolExecutionContext,
): Promise<WorldResultCapture<ToolSettlement>> {
	if (!OBSERVATION_TOOLS.has(context.toolName)) {
		throw new Error(`ThinkThread cannot capture authoritative ${context.toolName} results`);
	}
	const dependencies = actionDependencies(context);
	const started = performance.now();
	// Actor capture must observe the live workspace now, not an earlier speculative turn BASE.
	const source = world.pool.ownSnapshot(await world.durable.snapshotCreate());
	try {
		context.signal.throwIfAborted();
		return new ThinkThreadResultCapture({
			source,
			lineage: `actor:${source.id}`,
			depth: 0,
			resources: [...context.action.resources],
			capturedBytes: 0,
			setupMs: Math.max(0, performance.now() - started),
			captureMs: 0,
			executionFingerprint: context.action.executionFingerprint,
			...world,
			dependencies,
		});
	} catch (error) {
		await source.release().catch(() => undefined);
		throw error;
	}
}

interface ThinkThreadBranchInput {
	readonly output: ToolSettlement;
	readonly source: SnapshotLease;
	readonly target?: SnapshotLease;
	readonly lineage: string;
	readonly depth: number;
	readonly resources: readonly string[];
	readonly capturedBytes: number;
	readonly setupMs: number;
	readonly captureMs: number;
	readonly executionFingerprint: string;
	readonly client: AgentPosixClient;
	readonly durable: DurableFsExecutor;
	readonly pool: ThinkThreadSnapshotPool;
	readonly dependencies: readonly FsDependency[];
}

class ThinkThreadResultCapture implements WorldResultCapture<ToolSettlement> {
	private readonly input: Omit<ThinkThreadBranchInput, "output">;
	private state: "open" | "sealed" | "disposed" = "open";

	constructor(input: Omit<ThinkThreadBranchInput, "output">) {
		this.input = input;
	}

	seal(output: ToolSettlement): WorldBranch<ToolSettlement> {
		if (this.state !== "open") throw new Error(`ThinkThread result capture is already ${this.state}`);
		const branch = new ThinkThreadWorldBranch({ ...this.input, output });
		this.state = "sealed";
		return branch;
	}

	async dispose(): Promise<void> {
		if (this.state !== "open") return;
		this.state = "disposed";
		await this.input.source.release();
	}
}

class ThinkThreadWorldBranch implements WorldBranch<ToolSettlement> {
	readonly output: ToolSettlement;
	readonly backend = WORLD_ID;
	readonly checkpoint: ThinkThreadCheckpoint;
	readonly resources: readonly string[];
	readonly capturedBytes: number;
	readonly executionMetrics: { readonly setupMs: number; readonly captureMs: number };
	readonly compatibility: WorldCompatibilityEvidence;
	private readonly source: SnapshotLease;
	private readonly target?: SnapshotLease;
	private readonly client: AgentPosixClient;
	private readonly durable: DurableFsExecutor;
	private readonly pool: ThinkThreadSnapshotPool;
	private readonly dependencies: readonly FsDependency[];
	private metrics?: WorldCommitMetrics;
	private commitPromise?: Promise<ToolSettlement>;
	private disposed = false;

	constructor(input: ThinkThreadBranchInput) {
		this.output = input.output;
		this.source = input.source;
		this.target = input.target;
		this.resources = Object.freeze([...input.resources]);
		this.capturedBytes = input.capturedBytes;
		this.executionMetrics = Object.freeze({ setupMs: input.setupMs, captureMs: input.captureMs });
		this.compatibility = Object.freeze({
			status: "compatible",
			backend: WORLD_ID,
			executionFingerprint: input.executionFingerprint,
		});
		this.client = input.client;
		this.durable = input.durable;
		this.pool = input.pool;
		this.dependencies = input.dependencies;
		this.checkpoint = input.pool.checkpoint(input.target ?? input.source, input.lineage, input.depth + 1);
	}

	get commitMetrics(): WorldCommitMetrics | undefined {
		return this.metrics;
	}

	async validate(): Promise<ResourceValidation> {
		const result = await this.client.fs.verify({
			snapshotId: this.source.id,
			dependencies: [...this.dependencies],
		});
		const metrics = {
			durationMs: result.durationMs,
			bytesRead: result.comparedBytes,
			filesRead: result.comparedEntries,
			mode: "exact" as const,
		};
		return result.status === "matched"
			? { status: "valid", metrics }
			: { status: "stale", cause: cause("freshness", "thinkthread_dependency_changed"), metrics };
	}

	commit(): Promise<ToolSettlement> {
		if (this.disposed) return Promise.reject(new Error("ThinkThread branch is disposed"));
		this.commitPromise ??= this.commitOnce();
		return this.commitPromise;
	}

	async dispose(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		await this.commitPromise?.catch(() => undefined);
		await Promise.allSettled([this.target?.release(), this.source.release()]);
	}

	private async commitOnce(): Promise<ToolSettlement> {
		const started = performance.now();
		try {
			if (!this.target) {
				const validation = await this.validate();
				if (validation.status !== "valid") {
					throw new WorldCommitRejectedError(validation.cause, "ThinkThread speculative observation is stale");
				}
				this.metrics = {
					durationMs: Math.max(0, performance.now() - started),
					validationMs: validation.metrics.durationMs,
					bytesValidated: validation.metrics.bytesRead,
					resourcesValidated: validation.metrics.filesRead,
					resourcesCommitted: 0,
				};
			} else {
				const apply = await this.durable.apply({
					baseSnapshotId: this.source.id,
					targetSnapshotId: this.target.id,
					dependencies: [...this.dependencies],
					policyId: "safe_content_v1",
				});
				this.metrics = {
					durationMs: Math.max(0, performance.now() - started),
					validationMs: 0,
					bytesValidated: 0,
					resourcesValidated: this.dependencies.length,
					resourcesCommitted: apply.changedPaths,
				};
				await this.pool.invalidate();
			}
			return this.output;
		} catch (error) {
			if (error instanceof ThinkThreadDurableError && error.code === "FsApplyConflict") {
				throw new WorldCommitRejectedError(
					cause("freshness", "thinkthread_apply_conflict", error.message),
					"ThinkThread workspace changed before speculative adoption",
					{ cause: error },
				);
			}
			throw error;
		}
	}
}

function actionDependencies(context: SpeculativeToolExecutionContext): readonly FsDependency[] {
	if (context.toolName === "bash") return [{ path: ".", scope: "tree_content" }];
	const scope = dependencyScope(context.toolName);
	return context.action.resources.map((resource) => ({ path: resource, scope }));
}

function dependencyScope(tool: string): FsDependency["scope"] {
	switch (tool) {
		case "read":
		case "write":
		case "edit":
			return "content";
		case "grep":
			return "tree_content";
		case "find":
		case "ls":
			return "tree_entries";
		default:
			throw new Error(`ThinkThread has no dependency scope for ${tool}`);
	}
}

function toolName(tool: string): ThinkThreadToolName {
	if (!TOOL_NAMES.has(tool as ThinkThreadToolName))
		throw new Error(`ThinkThread tool runner does not support ${tool}`);
	return tool as ThinkThreadToolName;
}

function toolInvocation(value: unknown): ToolInvocation | undefined {
	return value !== null &&
		typeof value === "object" &&
		!Array.isArray(value) &&
		typeof (value as { executor?: unknown }).executor === "string"
		? (value as ToolInvocation)
		: undefined;
}

function shellCommandPrefix(value: unknown): { readonly shellCommandPrefix?: string } {
	const invocation = toolInvocation(value);
	const identity = invocation?.identity;
	if (!identity || typeof identity !== "object" || Array.isArray(identity)) return {};
	const commandPrefix = (identity as { commandPrefix?: unknown }).commandPrefix;
	return typeof commandPrefix === "string" ? { shellCommandPrefix: commandPrefix } : {};
}

function invocationEnvironment(value: unknown): Record<string, string> {
	const environment = toolInvocation(value)?.process?.environment ?? {};
	return Object.fromEntries(Object.entries(environment).filter(([key]) => !key.startsWith("THINKTHREAD_")));
}

function runTimeout(context: SpeculativeToolExecutionContext): number {
	if (context.toolName !== "bash") return DEFAULT_RUN_TIMEOUT_MS;
	const timeoutSeconds = toolInvocation(context.action.executionContext)?.process?.timeout;
	if (timeoutSeconds === undefined) return DEFAULT_RUN_TIMEOUT_MS;
	return Math.min(MAX_RUN_TIMEOUT_MS, Math.max(1, Math.ceil(timeoutSeconds * 1000) + BASH_TIMEOUT_BUFFER_MS));
}

function assertSuccessfulRun(run: FsRunV1): void {
	if (run.outputTruncated) throw new Error("ThinkThread tool runner output exceeded 512 KiB");
	if (run.exit.kind !== "code" || run.exit.code !== 0) {
		const diagnostic = new TextDecoder().decode(outputBytes(run.outputChunks, "stderr"));
		throw new Error(`ThinkThread tool runner failed (${run.exit.kind})${diagnostic ? `: ${diagnostic}` : ""}`);
	}
}

function outputBytes(chunks: readonly FsRunOutputChunkV1[], stream: FsRunOutputChunkV1["stream"]): Uint8Array {
	const buffers = chunks
		.filter((chunk) => chunk.stream === stream)
		.sort((left, right) => left.sequence - right.sequence)
		.map((chunk) => Buffer.from(chunk.dataBase64, "base64"));
	return Buffer.concat(buffers);
}

async function changedResources(
	client: AgentPosixClient,
	baseSnapshotID: FsSnapshotId,
	targetSnapshotID: FsSnapshotId,
): Promise<readonly string[]> {
	const resources: string[] = [];
	let cursor: string | undefined;
	do {
		const page = await client.fs.snapshotDiff({
			baseSnapshotId: baseSnapshotID,
			targetSnapshotId: targetSnapshotID,
			limit: DIFF_PAGE_LIMIT,
			...(cursor ? { cursor } : {}),
		});
		for (const change of page.changes) {
			if (change.path.utf8 === undefined || change.path.utf8 === null) {
				throw new Error("ThinkThread changed path is not valid UTF-8");
			}
			resources.push(change.path.utf8);
		}
		cursor = page.hasMore ? (page.nextCursor ?? undefined) : undefined;
		if (page.hasMore && !cursor) throw new Error("ThinkThread snapshot diff omitted its continuation cursor");
	} while (cursor);
	return Object.freeze(resources);
}

async function hashFile(file: string): Promise<string> {
	return createHash("sha256")
		.update(await readFile(file))
		.digest("hex");
}
