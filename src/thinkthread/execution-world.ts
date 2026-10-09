import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { access, lstat, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type {
	AgentPosixClient,
	FsDependency,
	FsEntryV1,
	FsRunKeyParamsV1,
	FsRunOutputChunkV1,
	FsRunV1,
	FsRunWrites,
	FsSnapshotId,
} from "@thinkthread/agent-posix";
import { createCommittedResourceInputs, type SpeculativeAgentExecutionWorld, type SpeculativeToolExecutionContext } from "../agent-execution-world.ts";
import { type ActionKey, PI_ACTION_SEMANTICS } from "../action-semantics.ts";
import { asRecord } from "../stable-json.ts";
import {
	effectCapabilitiesCover,
	RESOURCE_OBSERVATION_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../effect-model.ts";
import {
	type ExecutionWorldRequest,
	type WorldBranch,
} from "../execution-world.ts";
import { effectCommitFailure } from "../effect-transaction.ts";
import { assertNoSymlinkPath } from "../filesystem-evidence.ts";
import { relativeFilesystemPath, slash } from "../path-utils.ts";
import { ResourceReadView, ResourceVersionManager, resourceDependencies, type ResourceInput, type ResourceVersionToken } from "../resource-version.ts";
import { RuntimeLifecycleLane } from "../runtime-lifecycle.ts";
import { cause, type ResourceValidation } from "../settlement.ts";
import { TimelineInterval } from "../task-timing.ts";
import { toolErrorSettlement, type ToolSettlement } from "../tool-settlement.ts";
import type { DurableFsExecutor } from "./durable-fs.ts";
import { ThinkThreadDurableError } from "./errors.ts";
import { type SnapshotLease, ThinkThreadSnapshotPool } from "./snapshot-pool.ts";
import {
	decodeThinkThreadToolRunnerRequest, decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_NAMES,
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
	const operations = new RuntimeLifecycleLane();

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
		return (await Promise.all([
			"thinkthread-fs", snapshotInputs ? "sealed-inputs" : "runner",
			import("@thinkthread/agent-posix").then((sdk) => sdk.CONTRACT_FINGERPRINT),
			runnerFingerprint,
			nodePath, settings.autoResizeImages, settings.modelSupportsImages,
		])).join(":");
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
			execute: (context) => operations.admit(async () => {
				const signal = AbortSignal.any([context.signal, lifetime.signal]);
				signal.throwIfAborted();
				const world = await prepare(context.cwd);
				signal.throwIfAborted();
				return forkThinkThreadWorld(world, { ...context, signal }, runnerPath, nodePath, autoResizeImages, snapshotInputs);
			}),
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
			lifetime.abort();
			return operations.close(async () => {
				await operations.drain();
				const world = await prepared;
				await world?.pool.dispose();
			});
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
	const [{ DurableFsExecutor }, { createThinkThreadClient }] = await Promise.all([
		import("./durable-fs.ts"), import("./control-transport.ts"),
	]);
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
	const tool = toolName(context.toolName);
	const settings = runnerSettings(context.action, autoResizeImages, context.cwd);
	const dependencies = actionDependencies(context);
	const source = context.parentCheckpoint
		? world.pool.acquireCheckpoint(context.parentCheckpoint)
		: await world.pool.acquireRoot(context.executionScope ?? { sessionID: context.cwd, turnID: context.callID });
	TimelineInterval.use(source.lease.computation);
	let target: SnapshotLease | undefined;
	let inputs: Awaited<ReturnType<typeof captureSnapshotInputs>>;
	try {
		context.signal.throwIfAborted();
		const request = encodeThinkThreadToolRunnerRequest({
			tool,
			callID: context.callID,
			args: context.args,
			...settings,
		});
		if (snapshotInputs && !context.parentCheckpoint && PI_ACTION_SEMANTICS.effect(tool) === "observation" &&
			dependencies.length && dependencies.every((dependency) => dependency.scope === "content" || dependency.scope === "entries")) {
			inputs = await captureSnapshotInputs(world, context, source.lease, dependencies, decodeThinkThreadToolRunnerRequest(request));
			context.signal.throwIfAborted();
			if (inputs) return thinkThreadWorldBranch({
				output: inputs.output, source: source.lease, lineage: source.lineage, depth: source.depth,
				resources: context.action.resources, capturedBytes: 0,
				nativeInputs: inputs.version, reusableInputs: inputs.branch,
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
		return thinkThreadWorldBranch({
			output,
			source: source.lease,
			target,
			lineage: source.lineage,
			depth: source.depth,
			resources,
			capturedBytes: Math.max(0, run.changedBytes ?? 0),
			executionFingerprint: context.action.executionFingerprint,
			client: world.client,
			durable: world.durable,
			pool: world.pool,
			dependencies,
		});
	} catch (error) {
		await Promise.allSettled([inputs?.branch.dispose(), inputs?.version.release(), target?.release(), source.lease.release()]);
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
	readonly executionFingerprint: string;
	readonly client: AgentPosixClient;
	readonly durable: DurableFsExecutor;
	readonly pool: ThinkThreadSnapshotPool;
	readonly dependencies: readonly FsDependency[];
	readonly nativeInputs?: ResourceVersionToken;
	readonly reusableInputs?: WorldBranch<ToolSettlement>;
}

function thinkThreadWorldBranch(input: ThinkThreadBranchInput): WorldBranch<ToolSettlement> {
	const { output, source, target, client, durable, pool, dependencies, nativeInputs, reusableInputs } = input;
	let commitPromise: Promise<ToolSettlement> | undefined;
	const lifecycle = new RuntimeLifecycleLane();
	const nativeCause = async () => {
		try { if (nativeInputs) await assertInputAuthority(nativeInputs); }
		catch (error) { return cause("freshness", "thinkthread_input_authority_changed", error instanceof Error ? error.message : String(error)); }
		return undefined;
	};
	const validate = (): Promise<ResourceValidation> => lifecycle.serialize(() => TimelineInterval.outside(async () => {
		const before = await nativeCause();
		const result = before ? undefined : await client.fs.verify({ snapshotId: source.id, dependencies: [...dependencies] });
		const changed = before ?? await nativeCause();
		const metrics = { bytesRead: result?.comparedBytes ?? 0,
			filesRead: result?.comparedEntries ?? 0, mode: "exact" as const };
		return changed || result?.status !== "matched"
			? { status: "stale", cause: changed ?? cause("freshness", "thinkthread_dependency_changed"), metrics }
			: { status: "valid", metrics };
	}));
	async function commitOnce(onValidation?: (validation: ResourceValidation) => void): Promise<ToolSettlement> {
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
			} else {
				await durable.apply({
					baseSnapshotId: source.id,
					targetSnapshotId: target.id,
					dependencies: [...dependencies],
					policyId: "safe_content_v1",
				});
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
		output, backend: WORLD_ID, resources: Object.freeze([...input.resources]),
		get capturedBytes() { return input.capturedBytes + (reusableInputs?.capturedBytes ?? 0); },
		inputSource: reusableInputs?.inputSource, inputResources: reusableInputs?.inputResources,
		invalidateInputs: reusableInputs?.invalidateInputs, reconstructionScope: reusableInputs?.reconstructionScope,
		...(reusableInputs?.reconstruct ? { reconstruct: (request: Parameters<NonNullable<WorldBranch<ToolSettlement>["reconstruct"]>>[0]) => lifecycle.admit(async () => {
			const settings = runnerSettings(request.action, true, nativeInputs!.root);
			const { resolvePiToolInvocation } = await import("../pi-tool-invocation.ts");
			const invocation = resolvePiToolInvocation(request.action.tool, request.args, { cwd: nativeInputs!.root, environment: {}, ...settings });
			if (!invocation?.filesystem) return undefined;
			// The stock executor consumes the shared sealed-data protocol; its proof belongs only to this query.
			return reusableInputs.reconstruct!({ ...request, action: { ...request.action, executionContext: invocation } });
		}) } : {}),
		checkpoint: pool.checkpoint(target ?? source, input.lineage, input.depth + 1),
		executionMetrics: Object.freeze({}),
		compatibility: Object.freeze({ status: "compatible", backend: WORLD_ID, executionFingerprint: input.executionFingerprint }),
		validate,
		validateAndCommit: target || reusableInputs ? undefined : async () => {
			if (commitPromise) {
				await commitPromise;
				return validate();
			}
			if (lifecycle.sealed) throw new Error("ThinkThread branch is disposed");
			let validation: ResourceValidation | undefined;
			const pending = lifecycle.track(commitOnce((proof) => { validation = proof; }));
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
		commit: () => lifecycle.sealed ? Promise.reject(new Error("ThinkThread branch is disposed")) : (commitPromise ??= lifecycle.track(commitOnce())),
		dispose: () => lifecycle.close(async () => {
			await lifecycle.drain();
			await Promise.allSettled([target?.release(), source.release(), nativeInputs?.release(), reusableInputs?.dispose()]);
		}),
	};
}

/** Consume immutable regular files and directory entries through the stock operation seam. */
async function captureSnapshotInputs(world: PreparedWorld, context: SpeculativeToolExecutionContext,
	source: SnapshotLease, dependencies: readonly FsDependency[], request: ThinkThreadToolRunnerRequest) {
	const snapshotId = source.id;
	if (typeof world.client.fs.snapshotStat !== "function") return undefined;
	const files = new Map<string, { path: string } & ({ kind: "file"; bytes: number } |
		{ kind: "directory"; entries: readonly FsEntryV1[]; computation: TimelineInterval })>();
	let totalBytes = 0;
	try {
		for (const resource of context.action.resources) {
			context.signal.throwIfAborted();
			const target = path.resolve(context.cwd, resource), relative = relativeFilesystemPath(context.cwd, target);
			if (relative === undefined) return undefined;
			const parts = slash(relative).split("/");
			for (let end = 1; end < parts.length; end++) {
				const parent = await world.client.fs.snapshotStat({ snapshotId, path: parts.slice(0, end).join("/") });
				if (!("kind" in parent) || parent.kind !== "directory") return undefined;
			}
			const name = slash(relative) || ".", entry = await world.client.fs.snapshotStat({ snapshotId, path: name });
			if (!("kind" in entry)) return undefined;
			if (entry.kind === "file" && dependencies.some(dependency => dependency.path === name && dependency.scope === "content")) {
				if (typeof world.client.fs.snapshotPread !== "function" || !Number.isSafeInteger(entry.len) || entry.len < 0 ||
					(totalBytes += entry.len + 2 * Buffer.byteLength(target) + 64) > MAX_INPUT_BYTES) return undefined;
				files.set(target, { kind: "file", path: name, bytes: entry.len });
			} else if (entry.kind === "directory" && dependencies.some(dependency => dependency.path === name && dependency.scope === "entries")) {
				if (typeof world.client.fs.snapshotReaddir !== "function") return undefined;
				const startedAt = performance.now(), entries: FsEntryV1[] = [], names = new Set<string>(), cursors = new Set<string>();
				let cursor: string | undefined, total: number | undefined;
				do {
					context.signal.throwIfAborted();
					const page = await world.client.fs.snapshotReaddir({ snapshotId, path: name, limit: DIFF_PAGE_LIMIT, ...(cursor ? { cursor } : {}) });
					if (!Number.isSafeInteger(page.totalEntries) || page.totalEntries < 0 || total !== undefined && total !== page.totalEntries) return undefined;
					total = page.totalEntries;
					for (const child of page.entries) {
						const childPath = child.path.utf8, raw = Buffer.from(child.path.bytesBase64, "base64");
						if (typeof childPath !== "string" || !Buffer.from(childPath).equals(raw) || childPath.includes("\0") ||
							path.posix.dirname(childPath) !== name || childPath !== path.posix.normalize(childPath) ||
							[".", ".."].includes(path.posix.basename(childPath)) || !["file", "directory"].includes(child.kind) || names.has(childPath)) return undefined;
						names.add(childPath); entries.push(child);
						if ((totalBytes += 3 * Buffer.byteLength(path.resolve(context.cwd, childPath)) + 256) > MAX_INPUT_BYTES) return undefined;
					}
					if (entries.length > total || page.hasMore && (!page.entries.length || !page.nextCursor || cursors.has(page.nextCursor))) return undefined;
					cursor = page.hasMore ? page.nextCursor! : undefined;
					if (cursor) cursors.add(cursor);
				} while (cursor);
				if (entries.length !== total) return undefined;
				files.set(target, { kind: "directory", path: name, entries, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now(),
					source.computation ? [{ computation: source.computation, reused: true }] : [])) });
			} else return undefined;
		}
	} catch { return undefined; } // Missing entries or unavailable snapshot metadata retain the isolated runner.
	if (!files.size) return undefined;
	const manager = new ResourceVersionManager(context.cwd, { watch: false, onIdle: () => manager.close() });
	const version = await manager.capture([...files.keys()].map((target) => ({ path: target, scope: "stat" })));
	const loaded = new Set<string>();
	const retainedInputs = new Map<string, ResourceInput>();
	let view: ResourceReadView | undefined, retained = false;
	try {
		for (const target of files.keys()) await assertNoSymlinkPath(context.cwd, target);
		await assertInputAuthority(version);
		const matched = await TimelineInterval.outside(() => world.client.fs.verify({ snapshotId, dependencies: [...dependencies] }));
		if (matched.status !== "matched") throw new Error("ThinkThread snapshot inputs changed before execution");
		await assertInputAuthority(version);
		view = new ResourceReadView(MAX_INPUT_BYTES, async (dependency) => {
			context.signal.throwIfAborted();
			const target = path.resolve(dependency.path), file = files.get(target);
			if (!file || !(file.kind === "file" ? ["content", "stat", "type"] : ["names", "stat", "type"]).includes(dependency.scope)) throw new Error("ThinkThread input operation is unproven");
			if (loaded.has(target)) return;
			if (file.kind === "directory") {
				const names = file.entries.map(entry => path.posix.basename(entry.path.utf8!));
				view!.capture(target, { type: "directory", entries: names, realPath: target, computation: file.computation });
				retainedInputs.set(target, { names, computation: file.computation });
				for (const entry of file.entries) {
					const child = path.resolve(context.cwd, entry.path.utf8!), type = entry.kind as "file" | "directory";
					view!.capture(child, { type, realPath: child, computation: file.computation });
					retainedInputs.set(child, { type, computation: file.computation });
				}
				loaded.add(target); return;
			}
			const startedAt = performance.now();
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
			const content = Buffer.concat(chunks, offset), computation = TimelineInterval.own(new TimelineInterval(startedAt, performance.now(),
				source.computation ? [{ computation: source.computation, reused: true }] : []));
			view!.capture(target, { type: "file", content, realPath: target, computation }, file.bytes);
			retainedInputs.set(target, { content, computation });
			loaded.add(target);
		});
		const { resolvePiToolInvocation } = await import("../pi-tool-invocation.ts");
		const execute = resolvePiToolInvocation(request.tool, request.args, { cwd: context.cwd, environment: {},
			autoResizeImages: request.autoResizeImages, modelSupportsImages: request.modelSupportsImages })?.filesystem;
		if (!execute) throw new Error("ThinkThread input execution requires its qualified stock Pi version");
		let output: ToolSettlement;
		try { output = await execute(view, { ...request, signal: context.signal }); }
		catch (error) { output = toolErrorSettlement(error); }
		context.signal.throwIfAborted(); view.seal();
		await assertInputAuthority(version);
		const frame = Buffer.from(encodeThinkThreadToolRunnerResponse(output));
		if (frame.length > RUNNER_MAX_OUTPUT_BYTES) throw new Error("ThinkThread tool runner output exceeded 512 KiB");
		context.signal.throwIfAborted();
		output = decodeThinkThreadToolRunnerResponse(frame);
		// Imported bytes are proposals until the common exact content proof validates them at consumption.
		const branch = await createCommittedResourceInputs(output, context.action, context.cwd, retainedInputs, MAX_INPUT_BYTES);
		retained = true;
		return { output, version, branch };
	} finally {
		await view?.dispose();
		if (!retained) await version.release();
	}
}

/** Snapshot metadata omits ACL/owner authority. Fence current access and all ancestor bindings as well. */
async function assertInputAuthority(version: ResourceVersionToken): Promise<void> {
	return TimelineInterval.outside(async () => {
		for (const observation of version.observations.values()) if (observation.scope === "stat")
			await access(observation.path, constants.R_OK | ((await lstat(observation.path)).isDirectory() ? constants.X_OK : 0));
		const validation = await version.manager.seal(version);
		if (validation.expired) throw new Error(validation.reason ?? "ThinkThread input authority changed");
	});
}

/** The stock runner may consume only its declared executor contract, including both image options. */
function runnerSettings(action: ActionKey | undefined, autoResizeImages: boolean, cwd?: string) {
	if (!action) return { autoResizeImages, modelSupportsImages: true }; // Capability preparation has no invocation yet.
	const invocation = asRecord(action.executionContext), identity = asRecord(invocation?.identity);
	if (invocation?.executor !== "pi.filesystem.local" || typeof invocation.filesystem !== "function" ||
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
