import { deferred, nextTurn } from "./async.ts";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import {
	type AgentPosixClient,
	parseFsPayloadId,
	parseFsSnapshotId,
	parseRequestId,
	parseThinkThreadId,
} from "@thinkthread/agent-posix";
import { describe, expect, it, vi } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { resolvePiToolInvocation, type PiToolInvocationOptions } from "../src/pi-tool-invocation.ts";
import { stableValueHash } from "../src/stable-value-hash.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { EffectCommitFailure, EffectTransactionCoordinator } from "../src/effect-transaction.ts";
import {
	effectCapabilitiesCover,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import { ExecutionWorldRouter } from "../src/execution-world.ts";
import { ThinkThreadDurableError } from "../src/thinkthread/errors.ts";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { encodeThinkThreadToolRunnerResponse } from "../src/thinkthread/tool-runner-protocol.ts";

const ownerID = parseThinkThreadId("tt-00000000-0000-4000-8000-000000000001");

describe("ThinkThread execution world", () => {
	it("binds the stock runner and execution options without inventing a Runtime epoch", async () => {
		const fixture = fakeClient(), cwd = process.env.THINKTHREAD_FS ?? path.resolve("/workspace");
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerFingerprint: "runner-v1" });
		const fallback = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 4096 });
		const router = new ExecutionWorldRouter([world, fallback]);
		expect(world.speculation.capabilities).toEqual(expect.arrayContaining([...RESOURCE_OBSERVATION_EFFECTS.capabilities]));
		expect(effectCapabilitiesCover(world.speculation.capabilities, UNRESTRICTED_PROCESS_EFFECTS)).toBe(false);
		expect(world.speculation.tools).toEqual(["read", "ls", "write", "edit"]);
		expect(world.observation).toBeUndefined();
		const fingerprint = await world.speculation.fingerprint?.({ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS });
		expect(fingerprint).toContain("runner-v1");
		const resized = createThinkThreadExecutionWorld({ runnerFingerprint: "runner-v1", autoResizeImages: false });
		expect(await resized.speculation.fingerprint?.({ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS })).not.toBe(fingerprint);
		const fingerprints = new Set<string | undefined>();
		try {
			for (const autoResizeImages of [false, true]) for (const modelSupportsImages of [false, true]) {
				const input = context("read", { path: "image.png" }, cwd, "read", { autoResizeImages, modelSupportsImages });
				const request = { action: input.action, effect: "observation" as const, requirements: RESOURCE_OBSERVATION_EFFECTS };
				fingerprints.add(await world.speculation.fingerprint?.(request));
				const branch = await world.speculation.execute(input);
				try {
					const payload = vi.mocked(fixture.client.fs.payloadWrite).mock.calls.at(-1)![0].dataBase64;
					expect(JSON.parse(Buffer.from(payload, "base64").toString())).toMatchObject({ autoResizeImages, modelSupportsImages });
				} finally { await branch.dispose(); await world.finishTurn("turn"); }
				const invocation = input.action.executionContext as ReturnType<typeof resolvePiToolInvocation>;
				for (const executionContext of [undefined, { ...invocation, executor: "different" },
					{ ...invocation, identity: { ...(invocation!.identity as object), modelSupportsImages: undefined } }]) {
					const unqualified = { ...request, action: { ...input.action, executionContext } };
					await expect(world.speculation.fingerprint?.(unqualified)).rejects.toThrow("bound stock Pi");
					expect((await router.resolve(unqualified, { cwd }))?.backend).toBe(executionContext ? fallback.id : undefined);
				}
				await expect(world.speculation.execute({ ...input, cwd: path.join(cwd, "other") })).rejects.toThrow("bound stock Pi");
			}
			expect(fingerprints.size).toBe(4);
		} finally { await router.dispose(); await resized.dispose?.(); }
	});

	it.each([false, true])("routes every stock tool through the same capability layers (warmup=%s)", async (warmup) => {
		const fixture = fakeClient();
		const primary = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerFingerprint: "test" });
		const nativePrepare = vi.fn(async () => undefined);
		const processWorld = {
			id: "linux_process_reuse", scope: "runtime", isolation: "runtime_sandbox",
			speculation: {
				capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
				tools: PI_ACTION_SEMANTICS.toolNames("unbounded"),
				prepare: nativePrepare,
				execute: vi.fn(),
			},
		} as unknown as SpeculativeAgentExecutionWorld;
		const workspace = {
			id: "git_worktree", scope: "fallback", isolation: "workspace_branch",
			speculation: {
				capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
				tools: PI_ACTION_SEMANTICS.toolNames("workspace_mutation"),
				execute: vi.fn(),
			},
		} as unknown as SpeculativeAgentExecutionWorld;
		let enabled: (backend: string) => boolean = () => true;
		const router = new ExecutionWorldRouter([primary, processWorld, workspace], (backend) => enabled(backend));
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		const cases = [
			["read", { path: "notes.txt" }, "ThinkThread", undefined],
			["grep", { pattern: "alpha", path: "." }, undefined, undefined],
			["find", { pattern: "*.txt", path: "." }, undefined, undefined],
			["ls", { path: "." }, "ThinkThread", undefined],
			["write", { path: "generated.txt", content: "generated\n" }, "ThinkThread", "git_worktree"],
			["edit", { path: "notes.txt", edits: [{ oldText: "alpha", newText: "beta" }] }, "ThinkThread", "git_worktree"],
			["bash", { command: "printf ok" }, "linux_process_reuse", "linux_process_reuse"],
		] as const;

		for (const [tool, args, allLayers, nativeOnly] of cases) {
			const definition = PI_ACTION_SEMANTICS.definition(tool)!;
			const action = context(tool, args, cwd, "route").action;
			const request = { effect: definition.effect, requirements: definition.requirements, tool, action: warmup ? undefined : action };
			enabled = () => true;
			expect((await router.resolve(request, { cwd }))?.backend).toBe(allLayers);
			enabled = (backend) => backend !== "ThinkThread";
			const native = await router.resolve(request, { cwd });
			expect(native?.backend).toBe(nativeOnly);
			enabled = (backend) => backend === "ThinkThread";
			const unified = await router.resolve(request, { cwd });
			expect(unified?.backend).toBe(allLayers === "ThinkThread" ? "ThinkThread" : undefined);
			enabled = () => false;
			await expect(router.resolve(request, { cwd })).resolves.toBeUndefined();
		}

		expect(nativePrepare).toHaveBeenCalled();
		expect(fixture.selfView).toHaveBeenCalled();
		await router.dispose();
	});

	it.each(["startup", "disconnected"])("falls through %s failure without inventing a read fallback", async (failure) => {
		const fixture = fakeClient();
		const primary = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerFingerprint: "test" });
		const workspace = {
			id: "git_worktree", scope: "fallback", isolation: "workspace_branch",
			speculation: { capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities, execute: vi.fn() },
		} as unknown as SpeculativeAgentExecutionWorld;
		const router = new ExecutionWorldRouter([primary, workspace]);
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		if (failure === "startup") fixture.selfView.mockRejectedValue(new Error("ThinkThread unavailable"));
		else {
			await primary.speculation.prepare?.({ cwd });
			vi.mocked(fixture.client.fs.stat).mockRejectedValue(new Error("Runtime disconnected"));
		}
		const route = async (tool: "read" | "write", args: unknown) => {
			const definition = PI_ACTION_SEMANTICS.definition(tool)!;
			return router.resolve({
				effect: definition.effect,
				requirements: definition.requirements,
				action: context(tool, args, cwd, "route").action,
			}, { cwd });
		};

		await expect(route("read", { path: "notes.txt" })).resolves.toBeUndefined();
		await expect(route("write", { path: "generated.txt", content: "generated\n" }))
			.resolves.toMatchObject({ backend: "git_worktree" });
		await router.dispose();
	});

	it("shares one BASE across eight sealed root executions and cleans it after the turn", async () => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture);

		const branches = await Promise.all(
			Array.from({ length: 8 }, (_, index) =>
				world.speculation.execute(context("read", { path: "notes.txt" }, cwd, `read-${index}`)),
			),
		);

		expect(fixture.snapshotCreate).toHaveBeenCalledOnce();
		expect(fixture.run).toHaveBeenCalledTimes(8);
		expect(fixture.run.mock.calls.every(([params]) => params.writes === "deny")).toBe(true);
		expect(new Set(branches.map((branch) => branch.checkpoint?.id)).size).toBe(1);

		await world.finishTurn("turn");
		expect(fixture.snapshotRemove).not.toHaveBeenCalled();
		await Promise.all(branches.map((branch) => branch.dispose()));
		expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		await world.dispose?.();
	});

	it("seals workspace mutations and joins one conflict-checked apply", async () => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture);
		const branch = await world.speculation.execute(
			context("write", { path: "generated.txt", content: "generated\n" }, cwd, "write-1"),
		);

		expect(fixture.run.mock.calls[0]?.[0]).toMatchObject({ writes: "snapshot" });
		expect(branch.validateAndCommit).toBeUndefined();
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
		await world.finishTurn("turn");
		await world.dispose?.();
	});

	it.each([
		["read", { path: "notes.txt" }, "deny", { path: "notes.txt", scope: "content" }],
		["ls", { path: "." }, "deny", { path: ".", scope: "entries" }],
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
	] as const)("maps %s to sealed inputs or its exact fs.run and dependency policy", async (tool, args, writes, dependency) => {
		const fixture = fakeClient();
		const { world, cwd } = await startWorld(fixture);
		const branch = await world.speculation.execute(context(tool, args, cwd, `${tool}-1`));

		expect(fixture.run.mock.calls[0]?.[0]).toMatchObject({ writes });
		await expect(branch.validate?.()).resolves.toMatchObject({ status: "valid" });
		expect(fixture.verify).toHaveBeenCalledWith({
			snapshotId: expect.any(String),
			dependencies: [dependency],
		});

		await branch.dispose();
		await world.finishTurn("turn");
		await world.dispose?.();
		if (tool === "read" && process.platform === "linux") await verifySnapshotInputs();
	});

	it("rejects a run whose returned key does not match the preflight key", async () => {
		const fixture = fakeClient({ returnedRunKey: "unexpected-run-key" });
		const { world, cwd } = await startWorld(fixture);

		await expect(world.speculation.execute(context("read", { path: "notes.txt" }, cwd, "read-key"))).rejects.toThrow(
			"unexpected run key",
		);
		await world.finishTurn("turn");
		await world.dispose?.();
	});

	it("rejects a truncated tool settlement instead of adopting partial output", async () => {
		const fixture = fakeClient({ outputTruncated: true });
		const { world, cwd } = await startWorld(fixture);

		await expect(world.speculation.execute(context("read", { path: "notes.txt" }, cwd, "read-truncated"))).rejects.toThrow(
			"exceeded 512 KiB",
		);
		await world.finishTurn("turn");
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
		const { world, cwd } = await startWorld(fixture);
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
		await world.finishTurn("turn");
		await world.dispose?.();
	});

	it("joins read validation and commit without losing direct freshness, retries, or cleanup ownership", async () => {
		for (const mode of ["direct", "shared", "retry", "dispose", "direct-dispose", "direct-overlap"] as const) {
			const fixture = fakeClient(), { world, cwd } = await startWorld(fixture);
			const source = await world.speculation.execute(context("read", { path: "notes.txt" }, cwd, "read"));
			const coordinator = new EffectTransactionCoordinator<typeof source.output>();
			const branch = mode.startsWith("direct") ? source : await coordinator.execute(coordinator.begin({ tool: "read", route: {
				isolation: "runtime_sandbox", reuse: "shared_result", scope: "runtime", backend: world.id, fingerprint: "test",
			} }), async () => source);
			const verify = fixture.verify.getMockImplementation()!;
			try {
				if (mode === "retry") {
					fixture.verify.mockRejectedValueOnce(new Error("disconnected"));
					await expect(branch.validate?.()).resolves.toMatchObject({ status: "indeterminate" });
					fixture.verify.mockResolvedValueOnce({ ...await verify(), status: "stale" });
					await expect(branch.validate?.()).resolves.toMatchObject({ status: "stale" });
					await expect(branch.commit()).rejects.toThrow("requires successful validation");
				}
				if (mode === "direct-overlap") {
					await branch.commit();
					const entered = deferred(), gate = deferred(); let changed = false;
					fixture.verify.mockImplementation(async () => {
						const result = { ...await verify(), status: changed ? "stale" as const : "matched" as const };
						entered.resolve(); await gate.promise; return result;
					});
					const first = branch.validate!(); await entered.promise; changed = true;
					const second = branch.validate!(); gate.resolve();
					expect((await Promise.all([first, second])).map(proof => proof.status)).toEqual(["valid", "stale"]);
					expect(fixture.verify).toHaveBeenCalledTimes(3);
				} else if (mode === "dispose" || mode === "direct-dispose") {
					const { promise: entered, resolve: enter } = deferred(), { promise: gate, resolve: release } = deferred();
					fixture.verify.mockImplementationOnce(async () => { enter(); await gate; return verify(); });
					const validating = branch.validate!(); await entered;
					const disposal = branch.dispose();
					const repeated = branch.dispose(); let released = false;
					void Promise.resolve(repeated).then(() => { released = true; });
					await world.finishTurn("turn"); expect(fixture.snapshotRemove).not.toHaveBeenCalled();
					expect(released).toBe(false);
					release(); await validating; await disposal; await repeated;
					if (mode === "direct-dispose") await expect(branch.commit()).rejects.toThrow("disposed");
					else await expect(branch.commit()).rejects.toMatchObject({ disposition: "recoverable" });
				} else {
					await expect(branch.validate?.()).resolves.toMatchObject({ status: "valid" });
					if (mode !== "direct") {
						const results = await Promise.all([branch.commit(), branch.commit()]);
						expect(results).toEqual([source.output, source.output]);
						expect(fixture.verify).toHaveBeenCalledTimes(mode === "retry" ? 3 : 1);
					}
					fixture.verify.mockResolvedValueOnce({ ...await verify(), status: "stale" });
					if (mode === "direct") await expect(branch.commit()).rejects.toMatchObject({
						resolutionCause: { stage: "freshness", code: "thinkthread_dependency_changed" },
					});
					else await expect(branch.validate?.()).resolves.toMatchObject({ status: "stale" });
					expect(fixture.verify).toHaveBeenCalledTimes(mode === "retry" ? 4 : 2);
				}
				expect(fixture.apply).not.toHaveBeenCalled();
			} finally { await branch.dispose(); await world.dispose?.(); }
			expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		}
	});

	it("can requalify speculation after the runner becomes available", async () => {
		const directory = await mkdtemp(path.join(os.tmpdir(), "thinkthread-runner-probe-"));
		const runnerPath = path.join(directory, "tool-runner.js");
		const fixture = fakeClient();
		const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client, runnerPath });
		let enabled = true;
		const router = new ExecutionWorldRouter([world], () => enabled);
		const request = { effect: "observation" as const, requirements: RESOURCE_OBSERVATION_EFFECTS };
		const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
		try {
			await expect(router.diagnostics({ cwd })).resolves.toMatchObject([{ state: "registered" }]);
			expect(fixture.selfView).not.toHaveBeenCalled();
			await expect(router.resolve(request, { cwd })).resolves.toBeUndefined();
			await writeFile(runnerPath, "// runner\n");
			await expect(router.diagnostics({ cwd, refresh: true })).resolves.toMatchObject([{ state: "ready" }]);
			enabled = false;
			await expect(router.diagnostics({ cwd })).resolves.toMatchObject([{ state: "unavailable", tools: world.speculation.tools }]);
			enabled = true;
			await expect(router.diagnostics({ cwd })).resolves.toMatchObject([{ state: "ready" }]);
			await expect(router.resolve(request, { cwd })).resolves.toMatchObject({ backend: world.id });
			vi.mocked(fixture.client.fs.stat).mockRejectedValueOnce(new Error("Runtime disconnected"));
			await expect(router.diagnostics({ cwd, refresh: true })).resolves.toMatchObject([{ state: "unavailable", detail: "Runtime disconnected" }]);
			await expect(router.diagnostics({ cwd, refresh: true })).resolves.toMatchObject([{ state: "ready" }]);
		} finally {
			await world.dispose?.();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it.each(["abort", "dispose"])("drains a late BASE after %s without starting the runner", async (operation) => {
		const fixture = fakeClient();
		const { promise: entered, resolve: enter } = deferred();
		const { promise: snapshot, resolve: release } = deferred<ReturnType<typeof snapshotView>>();
		fixture.snapshotCreate.mockImplementationOnce(() => { enter(); return snapshot; });
		const { world, cwd } = await startWorld(fixture);
		const controller = new AbortController();
		const input = { ...context("read", { path: "notes.txt" }, cwd, "read"), signal: controller.signal };
		const rejected = expect(world.speculation.execute(input)).rejects.toThrow();
		await entered;
		if (operation === "abort") controller.abort();
		const disposal = operation === "dispose" ? world.dispose!() : undefined;
		release(snapshotView(1));
		await rejected;
		await (disposal ?? world.dispose!());
		expect(fixture.run).not.toHaveBeenCalled();
		expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		await expect(world.speculation.execute(input)).rejects.toThrow();
	});

	it.each(["truncated", "cancelled"])("reclaims a mutation TARGET after %s", async (exit) => {
		const fixture = fakeClient({ outputTruncated: exit === "truncated", cancelled: exit === "cancelled" });
		const { world, cwd } = await startWorld(fixture);
		await expect(world.speculation.execute(context("write", { path: "a", content: "b" }, cwd, "write")))
			.rejects.toThrow(exit === "truncated" ? "exceeded 512 KiB" : "failed (cancelled)");
		await world.finishTurn("turn");
		expect(fixture.snapshotRemove).toHaveBeenCalledTimes(2);
		await world.dispose?.();
	});
});

async function verifySnapshotInputs() {
	const directory = await mkdtemp(path.join(process.env.THINKTHREAD_FS ?? process.cwd(), "thinkthread-inputs-"));
	const cwd = process.env.THINKTHREAD_FS ?? directory, file = path.join(directory, "notes.txt"), relative = path.relative(cwd, file);
	const bytes = Buffer.from("alpha\nbeta\n");
	try {
		for (const mode of ["complete", "malformed", "stale", "changed", "large", "runner", "node", "fingerprint", "cancel"]) {
			await writeFile(file, bytes);
			const fixture = fakeClient({ verifyStatus: mode === "stale" ? "stale" : "matched" });
			const { promise: entered, resolve: enter } = deferred(), { promise: gate, resolve: release } = deferred();
			const snapshotPread = vi.fn(async ({ offset = 0, length = 65536 }) => {
				if (mode === "cancel") { enter(); await gate; }
				if (mode === "changed") await writeFile(file, "changed\n");
				const content = bytes.subarray(offset, offset + length);
				return { offset, bytesRead: content.length + (mode === "malformed" ? 1 : 0), dataBase64: content.toString("base64"), eof: true };
			});
			Object.assign(fixture.client.fs, { snapshotPread,
				snapshotStat: vi.fn(async ({ path: name }: { path: string }) => ({ kind: name === relative ? "file" : "directory",
					len: mode === "large" ? 8 * 1024 * 1024 : bytes.length, mode: 0o644 })),
			});
			const world = createThinkThreadExecutionWorld({ clientFactory: () => fixture.client,
				...(mode === "runner" ? { runnerPath: "/custom/runner.js" } : mode === "node" ? { nodePath: "/custom/node" } :
					mode === "fingerprint" ? { runnerFingerprint: "custom" } : {}) });
			const controller = new AbortController(), input = { ...context("read", { path: relative }, cwd, "read"), signal: controller.signal };
			const supplied = vi.fn(async () => { throw new Error("supplied host function must not execute"); });
			input.action = { ...input.action, executionContext: { ...(input.action.executionContext as object), filesystem: supplied } };
			let branch: Awaited<ReturnType<typeof world.speculation.execute>> | undefined;
			try {
				const executing = world.speculation.execute(input);
				if (mode === "cancel") {
					const rejected = expect(executing).rejects.toThrow(); await entered;
					controller.abort(); let disposed = false;
					const disposal = world.dispose!().then(() => { disposed = true; });
					await nextTurn();
					expect(disposed).toBe(false); expect(fixture.snapshotRemove).not.toHaveBeenCalled();
					release(); await rejected; await disposal;
				} else if (["malformed", "stale", "changed"].includes(mode)) await expect(executing).rejects.toThrow();
				else {
					branch = await executing;
					if (mode === "complete") {
						const expected = { result: await createReadTool(cwd).execute("read", { path: relative }), isError: false };
						const first = branch.validate!(), second = branch.validate!(); await Promise.all([first, second]);
						await expect(branch.commit()).resolves.toEqual(expected);
					} else expect(snapshotPread).not.toHaveBeenCalled();
				}
				expect(supplied).not.toHaveBeenCalled();
				expect(fixture.run).toHaveBeenCalledTimes(["large", "runner", "node", "fingerprint"].includes(mode) ? 1 : 0);
			} finally { release(); await branch?.dispose(); await world.dispose!(); }
			expect(fixture.snapshotRemove).toHaveBeenCalledOnce();
		}
	} finally { await rm(directory, { recursive: true, force: true }); }
}

function context(toolName: string, args: unknown, cwd: string, callID: string, settings: Partial<PiToolInvocationOptions> = {}) {
	const invocation = ["read", "ls", "write", "edit"].includes(toolName)
		? resolvePiToolInvocation(toolName, args, { ...settings, cwd, environment: {} }) : undefined;
	const action = PI_ACTION_SEMANTICS.buildKey(toolName, args, cwd, "schema", invocation
		? { fingerprint: stableValueHash(invocation.identity), context: invocation } : undefined);
	if (!action) throw new Error(`could not build ${toolName} action`);
	return {
		cwd,
		tool: { name: toolName } as AgentTool,
		toolName,
		args,
		action,
		callID,
		signal: new AbortController().signal,
		executionScope: { sessionID: "session", turnID: "turn" },
	};
}

async function startWorld(fixture: ReturnType<typeof fakeClient>) {
	const world = createThinkThreadExecutionWorld({
		clientFactory: () => fixture.client,
		runnerPath: "/opt/pi-speculative-action/tool-runner.js",
		runnerFingerprint: "runner-v1",
		nodePath: "/usr/bin/node",
	});
	const cwd = process.env.THINKTHREAD_FS ?? "/workspace";
	await world.speculation.prepare?.({ cwd });
	return { world, cwd };
}

function fakeClient(
	options: {
		readonly returnedRunKey?: string;
		readonly outputTruncated?: boolean;
		readonly cancelled?: boolean;
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
	const payloadWrite = vi.fn(async ({ payloadId }: Parameters<AgentPosixClient["fs"]["payloadWrite"]>[0]) => ({
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
			exit: options.cancelled ? { kind: "cancelled" as const } : { kind: "code" as const, code: 0 },
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
