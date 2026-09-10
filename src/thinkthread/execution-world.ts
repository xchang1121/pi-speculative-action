import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
	type AgentPosixClient,
	CONTRACT_FINGERPRINT,
	type FsDependency,
	type FsRunKeyParamsV1,
	type FsRunOutputChunkV1,
	type FsRunV1,
	type FsRunWrites,
	type FsSnapshotId,
} from "@thinkthread/agent-posix";
import type { SpeculativeAgentExecutionWorld, SpeculativeToolExecutionContext } from "../agent-execution-world.ts";
import { asRecord, type ActionKey, PI_ACTION_SEMANTICS } from "../action-semantics.ts";
import {
	effectCapabilitiesCover,
	RESOURCE_OBSERVATION_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../effect-model.ts";
import {
	type ExecutionWorldRequest,
	type WorldBranch,
	type WorldCommitMetrics,
} from "../execution-world.ts";
import { effectCommitFailure } from "../effect-transaction.ts";
import { assertNoSymlinkPath } from "../filesystem-evidence.ts";
import { relativeFilesystemPath, slash } from "../path-utils.ts";
import { ResourceReadView, ResourceVersionManager, resourceDependencies, type ResourceVersionToken } from "../resource-version.ts";
import { cause, type ResourceValidation } from "../settlement.ts";
import { toolErrorSettlement, type ToolSettlement } from "../tool-settlement.ts";
import { DurableFsExecutor } from "./durable-fs.ts";
import { createThinkThreadClient } from "./control-transport.ts";
import { ThinkThreadDurableError } from "./errors.ts";
import { type SnapshotLease, ThinkThreadSnapshotPool } from "./snapshot-pool.ts";
import {
	decodeThinkThreadToolRunnerRequest, decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_NAMES,
	THINKTHREAD_TOOL_RUNNER_VERSION,
	type ThinkThreadToolName,
	type ThinkThreadToolRunnerRequest,
} from "./tool-runner-protocol.ts";

const WORLD_ID = "ThinkThread";
const RUNNER_MAX_OUTPUT_BYTES = 512 * 1024;
const MAX_INPUT_BYTES = 8 * 1024 * 1024;
const DEFAULT_RUN_TIMEOUT_MS = 120_000;
const DIFF_PAGE_LIMIT = 256;
const CAPABILITIES = [...new Set([...RESOURCE_OBSERVATION_EFFECTS.capabilities, ...WORKSPACE_PATH_MUTATION_EFFECTS.capabilities])];
const TOOL_NAMES = THINKTHREAD_TOOL_NAMES.filter((tool) => effectCapabilitiesCover(CAPABILITIES, PI_ACTION_SEMANTICS.requirements(tool)!));

export interface ThinkThreadExecutionWorldOptions {
	readonly clientFactory?: () => AgentPosixClient;
	readonly runnerPath?: string;
	readonly runnerFingerprint?: string;
	readonly nodePath?: string;
	readonly autoResizeImages?: boolean;
}

export type ThinkThreadExecutionWorld = SpeculativeAgentExecutionWorld & {
	readonly actorFallbackSettled: () => Promise<void>;
	readonly finishTurn: (turnID: string) => Promise<void>;
};

export function createThinkThreadExecutionWorld(
	options: ThinkThreadExecutionWorldOptions = {},
): ThinkThreadExecutionWorld {
	const runnerPath = options.runnerPath ?? fileURLToPath(new URL("./tool-runner.js", import.meta.url));
	const nodePath = options.nodePath ?? process.execPath;
	const autoResizeImages = options.autoResizeImages ?? true;
	const snapshotInputs = process.platform === "linux" && options.runnerPath === undefined &&
		options.runnerFingerprint === undefined && options.nodePath === undefined;
	let prepared: Promise<PreparedWorld> | undefined;
	let runnerFingerprint: Promise<string> | undefined;
	const lifetime = new AbortController();
	const pending = new Set<Promise<unknown>>();
	let disposal: Promise<void> | undefined;

	const prepare = async (cwd: string): Promise<PreparedWorld> => {
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
		const settings = runnerSettings(request?.action, autoResizeImages);
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
			"thinkthread-fs-v2", snapshotInputs ? "sealed-inputs" : "runner",
			CONTRACT_FINGERPRINT,
			THINKTHREAD_TOOL_RUNNER_VERSION,
			await runnerFingerprint,
			nodePath, settings.autoResizeImages, settings.modelSupportsImages,
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
			capabilities: CAPABILITIES,
			tools: TOOL_NAMES,
			fingerprint,
			prepare: async ({ cwd }) => { await (await prepare(cwd)).client.fs.stat(); },
			diagnostics: async ({ cwd, refresh }) => {
				if (!prepared && !refresh) return { state: "registered", detail: "ThinkThread is checked on first use or refresh" };
				const world = await prepare(cwd);
				if (refresh) await world.client.fs.stat();
				await fingerprint();
				return {
					state: "ready",
					detail: `ThinkThread ${snapshotInputs ? "sealed regular inputs; " : ""}fs.run: ${TOOL_NAMES.join(", ")}; ambient process tools require a complete process proof`,
				};
			},
			execute: (context) => execute(context, (world, input) =>
				forkThinkThreadWorld(world, input, runnerPath, nodePath, autoResizeImages, snapshotInputs)),
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
	const client = clientFactory?.() ?? createThinkThreadClient();
	const self = await client.selfView();
	if (!self.capabilities.some((capability) => capability.id === "thinkthread.fs.self" && capability.version === 1)) {
		throw new Error("ThinkThread profile does not delegate thinkthread.fs.self@1");
	}
	const durable = new DurableFsExecutor(client);
	return { client, durable, pool: new ThinkThreadSnapshotPool(durable) };
}

async function forkThinkThreadWorld(
	world: PreparedWorld,
	context: SpeculativeToolExecutionContext,
	runnerPath: string,
	nodePath: string,
	autoResizeImages: boolean,
	snapshotInputs: boolean,
): Promise<WorldBranch<ToolSettlement>> {
	const setupStarted = performance.now();
	const tool = toolName(context.toolName);
	const settings = runnerSettings(context.action, autoResizeImages, context.cwd);
	const dependencies = actionDependencies(context);
	const source = context.parentCheckpoint
		? world.pool.acquireCheckpoint(context.parentCheckpoint)
		: await world.pool.acquireRoot(context.executionScope ?? { sessionID: context.cwd, turnID: context.callID });
	let target: SnapshotLease | undefined;
	let inputs: Awaited<ReturnType<typeof captureSnapshotInputs>>;
	try {
		context.signal.throwIfAborted();
		const request = encodeThinkThreadToolRunnerRequest({
			version: THINKTHREAD_TOOL_RUNNER_VERSION,
			tool,
			callID: context.callID,
			args: context.args,
			...settings,
		});
		if (snapshotInputs && !context.parentCheckpoint && PI_ACTION_SEMANTICS.effect(tool) === "observation" &&
			dependencies.length && dependencies.every((dependency) => dependency.scope === "content")) {
			inputs = await captureSnapshotInputs(world, context, source.lease.id, dependencies, decodeThinkThreadToolRunnerRequest(request));
			context.signal.throwIfAborted();
			if (inputs) return thinkThreadWorldBranch({
				output: inputs.output, source: source.lease, lineage: source.lineage, depth: source.depth,
				resources: context.action.resources, capturedBytes: 0,
				setupMs: inputs.setupFinishedAt - setupStarted, captureMs: inputs.captureMs, nativeInputs: inputs.version,
				executionFingerprint: context.action.executionFingerprint, ...world, dependencies,
			});
		}
		const writes: FsRunWrites = PI_ACTION_SEMANTICS.effect(tool) === "observation" ? "deny" : "snapshot";
		const runParams: FsRunKeyParamsV1 = {
			snapshotId: source.lease.id,
			writes,
			invocation: {
				argv: [nodePath, runnerPath],
				cwd: ".",
			},
			limits: {
				timeoutMs: DEFAULT_RUN_TIMEOUT_MS,
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
		return thinkThreadWorldBranch({
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
		await Promise.allSettled([inputs?.version.release(), target?.release(), source.lease.release()]);
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
	readonly nativeInputs?: ResourceVersionToken;
}

function thinkThreadWorldBranch(input: ThinkThreadBranchInput): WorldBranch<ToolSettlement> {
	const { output, source, target, client, durable, pool, dependencies, nativeInputs } = input;
	let metrics: WorldCommitMetrics | undefined, commitPromise: Promise<ToolSettlement> | undefined;
	let validating: Promise<ResourceValidation> | undefined, disposal: Promise<void> | undefined, disposed = false;
	const nativeCause = async () => {
		try { if (nativeInputs) await assertInputAuthority(nativeInputs); }
		catch (error) { return cause("freshness", "thinkthread_input_authority_changed", error instanceof Error ? error.message : String(error)); }
		return undefined;
	};
	const validateOnce = async (): Promise<ResourceValidation> => {
		const started = performance.now(), before = await nativeCause();
		const result = before ? undefined : await client.fs.verify({ snapshotId: source.id, dependencies: [...dependencies] });
		const changed = before ?? await nativeCause();
		const metrics = { durationMs: performance.now() - started, bytesRead: result?.comparedBytes ?? 0,
			filesRead: result?.comparedEntries ?? 0, mode: "exact" as const };
		return changed || result?.status !== "matched"
			? { status: "stale", cause: changed ?? cause("freshness", "thinkthread_dependency_changed"), metrics }
			: { status: "valid", metrics };
	};
	const validate = (): Promise<ResourceValidation> => {
		if (disposed) return Promise.reject(new Error("ThinkThread branch is disposed"));
		return validating ??= validateOnce().finally(() => { validating = undefined; });
	};
	async function commitOnce(onValidation?: (validation: ResourceValidation) => void): Promise<ToolSettlement> {
		const started = performance.now();
		try {
			if (!target) {
				const validation = await validate();
				onValidation?.(validation);
				if (validation.status !== "valid") {
					throw effectCommitFailure(
						new Error("ThinkThread speculative observation is stale"),
						"recoverable",
						"ThinkThread speculative observation is stale",
						validation.cause,
					);
				}
				metrics = {
					durationMs: Math.max(0, performance.now() - started),
					validationMs: validation.metrics.durationMs,
					bytesValidated: validation.metrics.bytesRead,
					resourcesValidated: validation.metrics.filesRead,
					resourcesCommitted: 0,
				};
			} else {
				const apply = await durable.apply({
					baseSnapshotId: source.id,
					targetSnapshotId: target.id,
					dependencies: [...dependencies],
					policyId: "safe_content_v1",
				});
				metrics = {
					durationMs: Math.max(0, performance.now() - started),
					validationMs: 0,
					bytesValidated: 0,
					resourcesValidated: dependencies.length,
					resourcesCommitted: apply.changedPaths,
				};
				await pool.invalidate();
			}
			return output;
		} catch (error) {
			if (error instanceof ThinkThreadDurableError && error.code === "FsApplyConflict") {
				throw effectCommitFailure(
					error,
					"recoverable",
					"ThinkThread workspace changed before speculative adoption",
					cause("freshness", "thinkthread_apply_conflict", error.message),
				);
			}
			throw error;
		}
	}
	return {
		output, backend: WORLD_ID, resources: Object.freeze([...input.resources]), capturedBytes: input.capturedBytes,
		checkpoint: pool.checkpoint(target ?? source, input.lineage, input.depth + 1),
		executionMetrics: Object.freeze({ setupMs: input.setupMs, captureMs: input.captureMs }),
		compatibility: Object.freeze({ status: "compatible", backend: WORLD_ID, executionFingerprint: input.executionFingerprint }),
		get commitMetrics() { return metrics; },
		validate,
		validateAndCommit: target ? undefined : async () => {
			if (commitPromise) {
				await commitPromise;
				return validate();
			}
			if (disposed) throw new Error("ThinkThread branch is disposed");
			let validation: ResourceValidation | undefined;
			const pending = commitOnce((proof) => { validation = proof; });
			commitPromise = pending;
			try { await pending; }
			catch (error) {
				// A failed proof did not commit; later validation may retry an indeterminate observation.
				if (validation?.status === "valid") throw error;
				if (commitPromise === pending) commitPromise = undefined;
				if (!validation) throw error;
			}
			return validation!;
		},
		commit: () => disposed ? Promise.reject(new Error("ThinkThread branch is disposed")) : (commitPromise ??= commitOnce()),
		dispose: () => {
			disposed = true;
			return disposal ??= (async () => {
				await Promise.allSettled([commitPromise, validating]);
				await Promise.allSettled([target?.release(), source.release(), nativeInputs?.release()]);
			})();
		},
	};
}

/** Consume immutable regular inputs through the stock operation seam; other shapes retain fs.run. */
async function captureSnapshotInputs(world: PreparedWorld, context: SpeculativeToolExecutionContext,
	snapshotId: FsSnapshotId, dependencies: readonly FsDependency[], request: ThinkThreadToolRunnerRequest) {
	if (typeof world.client.fs.snapshotStat !== "function" || typeof world.client.fs.snapshotPread !== "function") return undefined;
	const files = new Map<string, { path: string; bytes: number }>();
	let totalBytes = 0;
	try {
		for (const resource of context.action.resources) {
			context.signal.throwIfAborted();
			const target = path.resolve(context.cwd, resource), relative = relativeFilesystemPath(context.cwd, target);
			if (!relative) return undefined;
			const parts = slash(relative).split("/");
			for (let end = 1; end < parts.length; end++) {
				const parent = await world.client.fs.snapshotStat({ snapshotId, path: parts.slice(0, end).join("/") });
				if (!("kind" in parent) || parent.kind !== "directory") return undefined;
			}
			const entry = await world.client.fs.snapshotStat({ snapshotId, path: slash(relative) });
			if (!("kind" in entry) || entry.kind !== "file" || !Number.isSafeInteger(entry.len) || entry.len < 0 ||
				(totalBytes += entry.len + 2 * Buffer.byteLength(target) + 64) > MAX_INPUT_BYTES) return undefined;
			files.set(target, { path: slash(relative), bytes: entry.len });
		}
	} catch { return undefined; } // Missing entries or unavailable snapshot metadata retain the isolated runner.
	if (!files.size) return undefined;
	const manager = new ResourceVersionManager(context.cwd, { watch: false, onIdle: () => manager.close() });
	const version = await manager.capture([...files.keys()].map((target) => ({ path: target, scope: "stat" })));
	const loaded = new Set<string>();
	let view: ResourceReadView | undefined, retained = false;
	try {
		for (const target of files.keys()) await assertNoSymlinkPath(context.cwd, target);
		await assertInputAuthority(version);
		const matched = await world.client.fs.verify({ snapshotId, dependencies: [...dependencies] });
		if (matched.status !== "matched") throw new Error("ThinkThread snapshot inputs changed before execution");
		await assertInputAuthority(version);
		view = new ResourceReadView(MAX_INPUT_BYTES, async (dependency) => {
			context.signal.throwIfAborted();
			const target = path.resolve(dependency.path), file = files.get(target);
			if (!file || !["content", "stat", "type"].includes(dependency.scope)) throw new Error("ThinkThread input operation is unproven");
			if (loaded.has(target)) return;
			view!.reserve(file.bytes);
			const chunks: Buffer[] = [];
			let offset = 0;
			for (;;) {
				context.signal.throwIfAborted();
				const result = await world.client.fs.snapshotPread({ snapshotId, path: file.path, offset, length: 65536 });
				const bytes = Buffer.from(result.dataBase64, "base64");
				if (result.offset !== offset || result.bytesRead !== bytes.length || bytes.length > 65536 || offset + bytes.length > file.bytes)
					throw new Error("ThinkThread snapshot input length is unproven");
				chunks.push(bytes); offset += bytes.length;
				if (result.eof) break;
				if (!bytes.length) throw new Error("ThinkThread snapshot input did not advance");
			}
			if (offset !== file.bytes) throw new Error("ThinkThread snapshot input is incomplete");
			view!.capture(target, { type: "file", content: Buffer.concat(chunks, offset), realPath: target });
			loaded.add(target);
		});
		const { resolvePiToolInvocation } = await import("../pi-tool-invocation.ts");
		const execute = resolvePiToolInvocation(request.tool, request.args, { cwd: context.cwd, environment: {},
			autoResizeImages: request.autoResizeImages, modelSupportsImages: request.modelSupportsImages })?.filesystem;
		if (!execute) throw new Error("ThinkThread input execution requires its qualified stock Pi version");
		const setupFinishedAt = performance.now();
		let output: ToolSettlement;
		try { output = await execute(view, { ...request, signal: context.signal }); }
		catch (error) { output = toolErrorSettlement(error); }
		const captureStarted = performance.now();
		context.signal.throwIfAborted(); view.seal();
		await assertInputAuthority(version);
		const frame = Buffer.from(encodeThinkThreadToolRunnerResponse(output));
		if (frame.length > RUNNER_MAX_OUTPUT_BYTES) throw new Error("ThinkThread tool runner output exceeded 512 KiB");
		context.signal.throwIfAborted();
		output = decodeThinkThreadToolRunnerResponse(frame);
		retained = true;
		return { output, setupFinishedAt, captureMs: performance.now() - captureStarted, version };
	} finally {
		await view?.dispose();
		if (!retained) await version.release();
	}
}

/** Snapshot metadata omits ACL/owner authority. Fence current access and all ancestor bindings as well. */
async function assertInputAuthority(version: ResourceVersionToken): Promise<void> {
	for (const observation of version.observations.values()) if (observation.scope === "stat") await access(observation.path, constants.R_OK);
	const validation = await version.manager.seal(version);
	if (validation.expired) throw new Error(validation.reason ?? "ThinkThread input authority changed");
}

/** The stock runner may consume only its declared executor contract, including both image options. */
function runnerSettings(action: ActionKey | undefined, autoResizeImages: boolean, cwd?: string) {
	if (!action) return { autoResizeImages, modelSupportsImages: true }; // Capability preparation has no invocation yet.
	const invocation = asRecord(action.executionContext), identity = asRecord(invocation?.identity);
	if (invocation?.executor !== "pi.filesystem.local.v2" || typeof invocation.filesystem !== "function" ||
		identity?.executor !== invocation.executor || identity.version !== "0.84.1" || action.semantics ||
		action.semanticsEpoch !== PI_ACTION_SEMANTICS.definition(action.tool)?.epoch ||
		typeof identity.cwd !== "string" || !path.isAbsolute(identity.cwd) || (cwd !== undefined && identity.cwd !== cwd) ||
		typeof identity.autoResizeImages !== "boolean" || typeof identity.modelSupportsImages !== "boolean") {
		throw new Error("ThinkThread runner requires the bound stock Pi filesystem contract");
	}
	return { autoResizeImages: identity.autoResizeImages, modelSupportsImages: identity.modelSupportsImages };
}

function actionDependencies(context: SpeculativeToolExecutionContext): readonly FsDependency[] {
	const observed = resourceDependencies(context.action, context.cwd);
	const dependencies = observed.length ? observed : context.action.resources.map((resource) => ({
		path: path.resolve(context.cwd, resource), scope: "content" as const,
	}));
	return dependencies.map((dependency) => {
		const relative = relativeFilesystemPath(context.cwd, dependency.path);
		if (relative === undefined) throw new Error("ThinkThread dependency escapes its snapshot");
		return { path: slash(relative) || ".", scope: dependency.scope };
	});
}

function toolName(tool: string): ThinkThreadToolName {
	if (!TOOL_NAMES.includes(tool as ThinkThreadToolName))
		throw new Error(`ThinkThread tool runner does not support ${tool}`);
	return tool as ThinkThreadToolName;
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
