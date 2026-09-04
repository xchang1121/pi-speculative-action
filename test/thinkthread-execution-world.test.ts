import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import {
	type AgentPosixClient,
	parseFsPayloadId,
	parseFsSnapshotId,
	parseRequestId,
	parseThinkThreadId,
} from "@thinkthread/agent-posix";
import { describe, expect, it, vi } from "vitest";
import { buildPiActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { EffectCommitFailure } from "../src/effect-transaction.ts";
import {
	effectCapabilitiesCover,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
} from "../src/effect-model.ts";
import { ExecutionWorldRouter } from "../src/execution-world.ts";
import { ThinkThreadDurableError } from "../src/thinkthread/errors.ts";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { encodeThinkThreadToolRunnerResponse } from "../src/thinkthread/tool-runner-protocol.ts";

const ownerID = parseThinkThreadId("tt-00000000-0000-4000-8000-000000000001");

describe("ThinkThread execution world", () => {
	it("advertises runtime-wide routing with the current Linux execution epoch", async () => {
		const world = createThinkThreadExecutionWorld({
			clientFactory: () => fakeClient().client,
			runnerPath: "/opt/pi-speculative-action/tool-runner.js",
			runnerFingerprint: "runner-v1",
		});

		expect(world.speculation.capabilities).toEqual(expect.arrayContaining([...RESOURCE_OBSERVATION_EFFECTS.capabilities]));
		expect(effectCapabilitiesCover(world.speculation.capabilities, UNRESTRICTED_PROCESS_EFFECTS)).toBe(false);
		expect(world.speculation.tools).toEqual(["read", "grep", "find", "ls", "write", "edit"]);
		expect(world.observation?.tools).toEqual(["read", "grep", "find", "ls"]);
		await expect(
			world.speculation.fingerprint?.({ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS }),
		).resolves.toContain("linux-execution-v10");
	});

	it("falls through an unsupported or unavailable ThinkThread route", async () => {
		const fixture = fakeClient();
		const primary = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerFingerprint: "test" });
		const nativePrepare = vi.fn(async () => undefined);
		const native = {
			id: "linux_process_reuse", scope: "runtime", isolation: "runtime_sandbox",
			speculation: {
				capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
				tools: PI_ACTION_SEMANTICS.toolNames("unbounded"),
				prepare: nativePrepare,
				execute: vi.fn(),
			},
		} as unknown as SpeculativeAgentExecutionWorld;
		const portable = {
			id: "resource_fallback", scope: "fallback", isolation: "resource_snapshot",
			speculation: { capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities, tools: ["read"], execute: vi.fn() },
		} as unknown as SpeculativeAgentExecutionWorld;
		const router = new ExecutionWorldRouter([primary, native, portable]);
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		const bash = buildPiActionKey("bash", { command: "printf ok" }, cwd, "schema")!;

		await expect(router.resolve({
			effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS, action: bash,
		}, { cwd })).resolves.toMatchObject({ backend: "linux_process_reuse" });
		expect(fixture.selfView).not.toHaveBeenCalled();
		expect(nativePrepare).toHaveBeenCalledOnce();

		fixture.selfView.mockRejectedValueOnce(new Error("ThinkThread unavailable"));
		const read = buildPiActionKey("read", { path: "notes.txt" }, cwd, "schema")!;
		await expect(router.resolve({
			effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, action: read,
		}, { cwd })).resolves.toMatchObject({ backend: "resource_fallback" });
		expect(fixture.selfView).toHaveBeenCalledOnce();
		await router.dispose();
	});

	it("shares one BASE across eight sealed root executions and cleans it after the turn", async () => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture, "turn-1");

		const branches = await Promise.all(
			Array.from({ length: 8 }, (_, index) =>
				world.speculation.execute(context("read", { path: "notes.txt" }, cwd, `read-${index}`)),
			),
		);

		expect(fixture.snapshotCreate).toHaveBeenCalledOnce();
		expect(fixture.run).toHaveBeenCalledTimes(8);
		expect(fixture.run.mock.calls.every(([params]) => params.writes === "deny")).toBe(true);
		expect(new Set(branches.map((branch) => branch.checkpoint?.id)).size).toBe(1);

		await world.finishTurn("turn-1");
		expect(fixture.snapshotRemove).not.toHaveBeenCalled();
		await Promise.all(branches.map((branch) => branch.dispose()));
		expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		await world.dispose?.();
	});

	it("seals workspace mutations and joins one conflict-checked apply", async () => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture, "turn-2");
		const branch = await world.speculation.execute(
			context("write", { path: "generated.txt", content: "generated\n" }, cwd, "write-1"),
		);

		expect(fixture.run.mock.calls[0]?.[0]).toMatchObject({ writes: "snapshot" });
		expect(branch.resources).toEqual(["generated.txt"]);
		expect(branch.capturedBytes).toBe(10);
		const [first, second] = await Promise.all([branch.commit(), branch.commit()]);

		expect(first).toEqual(second);
		expect(fixture.apply).toHaveBeenCalledOnce();
		expect(fixture.apply.mock.calls[0]?.[0]).toMatchObject({
			policyId: "safe_content_v1",
			dependencies: [{ path: "generated.txt", scope: "content" }],
		});
		expect(branch.commitMetrics?.resourcesCommitted).toBe(1);
		await branch.dispose();
		await world.finishTurn("turn-2");
		await world.dispose?.();
	});

	it.each([
		["read", { path: "notes.txt" }, "deny", { path: "notes.txt", scope: "content" }],
		["grep", { pattern: "alpha", path: "." }, "deny", { path: ".", scope: "tree_content" }],
		["find", { pattern: "*.txt", path: "." }, "deny", { path: ".", scope: "tree_entries" }],
		["ls", { path: "." }, "deny", { path: ".", scope: "tree_entries" }],
		[
			"write",
			{ path: "generated.txt", content: "generated\n" },
			"snapshot",
			{ path: "generated.txt", scope: "content" },
		],
		[
			"edit",
			{ path: "notes.txt", edits: [{ oldText: "alpha", newText: "beta" }] },
			"snapshot",
			{ path: "notes.txt", scope: "content" },
		],
	] as const)("maps %s to its exact fs.run and dependency policy", async (tool, args, writes, dependency) => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture, `turn-${tool}`);
		const branch = await world.speculation.execute(context(tool, args, cwd, `${tool}-1`));

		expect(fixture.run.mock.calls[0]?.[0]).toMatchObject({ writes });
		await expect(branch.validate?.()).resolves.toMatchObject({ status: "valid" });
		expect(fixture.verify).toHaveBeenCalledWith({
			snapshotId: expect.any(String),
			dependencies: [dependency],
		});

		await branch.dispose();
		await world.finishTurn(`turn-${tool}`);
		await world.dispose?.();
	});

	it("rejects a run whose returned key does not match the preflight key", async () => {
		const fixture = fakeClient({ returnedRunKey: "unexpected-run-key" });
		const { world, cwd } = await startWorld(fixture, "turn-run-key");

		await expect(world.speculation.execute(context("read", { path: "notes.txt" }, cwd, "read-key"))).rejects.toThrow(
			"unexpected run key",
		);
		await world.finishTurn("turn-run-key");
		await world.dispose?.();
	});

	it("rejects a truncated tool settlement instead of adopting partial output", async () => {
		const fixture = fakeClient({ outputTruncated: true });
		const { world, cwd } = await startWorld(fixture, "turn-truncated");

		await expect(world.speculation.execute(context("read", { path: "notes.txt" }, cwd, "read-truncated"))).rejects.toThrow(
			"exceeded 512 KiB",
		);
		await world.finishTurn("turn-truncated");
		await world.dispose?.();
	});

	it("maps a stale apply to a backend-independent commit rejection", async () => {
		const conflict = new ThinkThreadDurableError(
			"fs.apply",
			parseRequestId("req-00000000-0000-4000-8000-000000000099"),
			"FsApplyConflict",
			"workspace changed",
		);
		const fixture = fakeClient({ applyError: conflict });
		const { world, cwd } = await startWorld(fixture, "turn-conflict");
		const branch = await world.speculation.execute(
			context("write", { path: "generated.txt", content: "generated\n" }, cwd, "write-conflict"),
		);

		const rejection = await branch.commit().catch((error: unknown) => error);
		expect(rejection).toBeInstanceOf(EffectCommitFailure);
		expect((rejection as EffectCommitFailure).resolutionCause).toMatchObject({
			stage: "freshness",
			code: "thinkthread_apply_conflict",
		});
		await expect(branch.commit()).rejects.toBe(rejection);
		expect(fixture.apply).toHaveBeenCalledOnce();

		await branch.dispose();
		await world.finishTurn("turn-conflict");
		await world.dispose?.();
	});

	it("captures a fresh Actor baseline without executing a tool or reusing the turn BASE", async () => {
		const fixture = fakeClient();
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerFingerprint: "test" });
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		await world.beginTurn("capture-turn");
		await world.speculation.prepare?.({ cwd });
		const input = context("read", { path: "notes.txt" }, cwd, "actor-read");
		const speculative = await world.speculation.execute(input);
		const capture = await world.observation!.capture(input);
		const output = { result: { content: [{ type: "text" as const, text: "Actor output" }], details: {} }, isError: false };
		const branch = await capture.seal(output);

		expect(fixture.snapshotCreate).toHaveBeenCalledTimes(2);
		expect(fixture.run).toHaveBeenCalledOnce();
		expect(branch.checkpoint?.id).not.toBe(speculative.checkpoint?.id);
		await expect(branch.validate?.()).resolves.toMatchObject({ status: "valid" });
		await expect(branch.commit()).resolves.toBe(output);
		expect(fixture.apply).not.toHaveBeenCalled();
		expect(() => capture.seal(output)).toThrow("already sealed");
		await capture.dispose();
		expect(fixture.snapshotRemove).not.toHaveBeenCalled();
		await branch.dispose();
		await speculative.dispose();
		await world.finishTurn("capture-turn");
		expect(fixture.snapshotRemove).toHaveBeenCalledTimes(2);
		await world.dispose?.();
	});

	it("rejects stale Actor observations at adoption and releases unsealed captures", async () => {
		const fixture = fakeClient({ verifyStatus: "stale" });
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client });
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		const input = context("read", { path: "notes.txt" }, cwd, "actor-read");
		const capture = await world.observation!.capture(input);
		const branch = await capture.seal({ result: { content: [], details: {} }, isError: false });
		await expect(branch.validate?.()).resolves.toMatchObject({ status: "stale" });
		await expect(branch.commit()).rejects.toMatchObject({
			resolutionCause: { stage: "freshness", code: "thinkthread_dependency_changed" },
		});
		await branch.dispose();
		const unused = await world.observation!.capture(input);
		await unused.dispose();
		await unused.dispose();
		expect(() => unused.seal(branch.output)).toThrow("already disposed");
		expect(fixture.snapshotRemove).toHaveBeenCalledTimes(2);
		expect(fixture.run).not.toHaveBeenCalled();
		await world.dispose?.();
	});

	it("qualifies Actor observation independently of the speculative runner", async () => {
		const fixture = fakeClient();
		const world = createThinkThreadExecutionWorld({
			clientFactory: () => fixture.client,
			runnerPath: "/missing-thinkthread-runner/does-not-exist.js",
		});
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		await expect(world.speculation.diagnostics?.({ cwd })).rejects.toThrow();
		await expect(world.observation!.diagnostics?.({ cwd })).resolves.toMatchObject({ state: "ready" });
		await expect(world.observation!.capture(context("write", { path: "a", content: "b" }, cwd, "write")))
			.rejects.toThrow("cannot capture authoritative write");
		expect(fixture.snapshotCreate).not.toHaveBeenCalled();
		await world.dispose?.();
	});

	it("releases a late Actor snapshot when cancelled during capture", async () => {
		const fixture = fakeClient();
		let release!: (snapshot: ReturnType<typeof snapshotView>) => void;
		fixture.snapshotCreate.mockImplementationOnce(() => new Promise((resolve) => { release = resolve; }));
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client });
		const controller = new AbortController();
		const input = { ...context("read", { path: "notes.txt" }, process.env.THINKTHREAD_FS ?? "/workspace", "read"), signal: controller.signal };
		const capture = world.observation!.capture(input);
		const rejected = expect(capture).rejects.toThrow();
		await vi.waitFor(() => expect(fixture.snapshotCreate).toHaveBeenCalledOnce());
		controller.abort();
		release(snapshotView(1));
		await rejected;
		expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		await world.dispose?.();
	});

	it("can requalify speculation after the runner becomes available", async () => {
		const directory = await mkdtemp(path.join(os.tmpdir(), "thinkthread-runner-probe-"));
		const runnerPath = path.join(directory, "tool-runner.js");
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fakeClient().client, runnerPath });
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		try {
			await expect(world.speculation.diagnostics?.({ cwd })).rejects.toThrow();
			await writeFile(runnerPath, "// runner\n");
			await expect(world.speculation.diagnostics?.({ cwd })).resolves.toMatchObject({ state: "ready" });
		} finally {
			await world.dispose?.();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("drains in-flight capture before world disposal and rejects later execution", async () => {
		const fixture = fakeClient();
		let release!: (snapshot: ReturnType<typeof snapshotView>) => void;
		fixture.snapshotCreate.mockImplementationOnce(() => new Promise((resolve) => { release = resolve; }));
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client });
		const input = context("read", { path: "notes.txt" }, process.env.THINKTHREAD_FS ?? "/workspace", "read");
		const capture = world.observation!.capture(input);
		const rejected = expect(capture).rejects.toThrow();
		await vi.waitFor(() => expect(fixture.snapshotCreate).toHaveBeenCalledOnce());
		let disposed = false;
		const disposal = world.dispose!().then(() => { disposed = true; });
		await Promise.resolve();
		expect(disposed).toBe(false);
		release(snapshotView(1));
		await rejected;
		await disposal;
		expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		await expect(world.observation!.capture(input)).rejects.toThrow();
		expect(fixture.snapshotCreate).toHaveBeenCalledOnce();
	});

	it("reclaims a mutation TARGET even when its response cannot be adopted", async () => {
		const fixture = fakeClient({ outputTruncated: true });
		const { world, cwd } = await startWorld(fixture, "truncated-write");
		await expect(world.speculation.execute(context("write", { path: "a", content: "b" }, cwd, "write")))
			.rejects.toThrow("exceeded 512 KiB");
		await world.finishTurn("truncated-write");
		expect(fixture.snapshotRemove).toHaveBeenCalledTimes(2);
		await world.dispose?.();
	});
});

function context(toolName: string, args: unknown, cwd: string, callID: string) {
	const action = buildPiActionKey(toolName, args, cwd, "schema");
	if (!action) throw new Error(`could not build ${toolName} action`);
	return {
		cwd,
		tool: { name: toolName } as AgentTool,
		toolName,
		args,
		action,
		callID,
		signal: new AbortController().signal,
	};
}

async function startWorld(fixture: ReturnType<typeof fakeClient>, turnID: string) {
	const world = createThinkThreadExecutionWorld({
		clientFactory: () => fixture.client,
		runnerPath: "/opt/pi-speculative-action/tool-runner.js",
		runnerFingerprint: "runner-v1",
		nodePath: "/usr/bin/node",
	});
	const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
	await world.beginTurn(turnID);
	await world.speculation.prepare?.({ cwd });
	return { world, cwd };
}

function fakeClient(
	options: {
		readonly returnedRunKey?: string;
		readonly outputTruncated?: boolean;
		readonly applyError?: Error;
		readonly verifyStatus?: "matched" | "stale";
	} = {},
) {
	let snapshotSequence = 0;
	let payloadSequence = 0;
	let runSequence = 0;
	const snapshotCreate = vi.fn(async () => snapshotView(++snapshotSequence));
	const snapshotRemove = vi.fn(async () => ({}));
	const payloadCreate = vi.fn(async () => ({
		payloadId: payloadID(++payloadSequence),
		expectedBytes: 1,
		currentBytes: 0,
		state: "open" as const,
		lifecycle: "available" as const,
	}));
	const payloadWrite = vi.fn(async ({ payloadId }: { readonly payloadId: ReturnType<typeof payloadID> }) => ({
		payloadId,
		expectedBytes: 1,
		currentBytes: 1,
		state: "open" as const,
		lifecycle: "available" as const,
	}));
	const payloadSeal = vi.fn(async ({ payloadId }: { readonly payloadId: ReturnType<typeof payloadID> }) => ({
		payloadId,
		expectedBytes: 1,
		currentBytes: 1,
		state: "sealed" as const,
		lifecycle: "available" as const,
	}));
	const runKey = vi.fn(async () => ({ runKey: "run-key" }));
	const run = vi.fn(async (params: { readonly writes: string }) => {
		const sequence = ++runSequence;
		const response = encodeThinkThreadToolRunnerResponse({
			result: { content: [{ type: "text", text: `result-${sequence}` }], details: {} },
			isError: false,
		});
		const target = params.writes === "snapshot" ? snapshotID(++snapshotSequence) : undefined;
		return {
			exit: { kind: "code" as const, code: 0 },
			outputChunks: [
				{
					sequence: 0,
					stream: "stdout" as const,
					dataBase64: Buffer.from(response).toString("base64"),
				},
			],
			outputTruncated: options.outputTruncated ?? false,
			retainedOutputBytes: response.length,
			observedOutputBytes: response.length,
			runKey: options.returnedRunKey ?? "run-key",
			...(target
				? { targetSnapshotId: target, changedPaths: 1, changedBytes: 10 }
				: { targetSnapshotId: null, changedPaths: null, changedBytes: null }),
			metrics: { setupMs: 1, executeMs: 2, sealMs: target ? 3 : 0, cleanupMs: 1 },
		};
	});
	const snapshotDiff = vi.fn(async () => ({
		changes: [
			{
				path: { utf8: "generated.txt", bytesBase64: Buffer.from("generated.txt").toString("base64") },
				kind: "added" as const,
			},
		],
		changedPaths: 1,
		nextCursor: null,
		hasMore: false,
	}));
	const apply = vi.fn(
		async (params: {
			readonly baseSnapshotId: ReturnType<typeof snapshotID>;
			readonly targetSnapshotId: ReturnType<typeof snapshotID>;
		}) => {
			if (options.applyError) throw options.applyError;
			return {
				status: "applied" as const,
				baseSnapshotId: params.baseSnapshotId,
				targetSnapshotId: params.targetSnapshotId,
				changedPaths: 1,
				changedBytes: 10,
			};
		},
	);
	const verify = vi.fn(async () => ({
		status: options.verifyStatus ?? "matched",
		durationMs: 1,
		comparedEntries: 1,
		comparedBytes: 10,
	}));
	const fs = {
		stat: vi.fn(async () => ({
			kind: "direct" as const,
			thinkthreadId: ownerID,
			state: "attached" as const,
			storage: {},
		})),
		snapshotCreate,
		snapshotRemove,
		payloadCreate,
		payloadWrite,
		payloadSeal,
		runKey,
		run,
		snapshotDiff,
		verify,
		apply,
		requestClose: vi.fn(async () => ({})),
		requestStatus: vi.fn(),
		requestCancel: vi.fn(async () => ({ accepted: true, state: "running" as const })),
	};
	const selfView = vi.fn(async () => ({
			schemaVersion: 1,
			thinkthreadId: ownerID,
			capabilities: [{ id: "thinkthread.fs.self" as const, version: 1 }],
			profiles: [],
		}));
	const client = {
		selfView,
		fs,
	} as unknown as AgentPosixClient;
	return { client, selfView, snapshotCreate, snapshotRemove, run, apply, verify };
}

function snapshotID(sequence: number) {
	return parseFsSnapshotId(`fsnap-00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`);
}

function payloadID(sequence: number) {
	return parseFsPayloadId(`fspayload-00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`);
}

function snapshotView(sequence: number) {
	return {
		snapshotId: snapshotID(sequence),
		ownerThinkthreadId: ownerID,
		createdAtUnixMs: sequence,
		logicalBytes: 100,
	};
}
