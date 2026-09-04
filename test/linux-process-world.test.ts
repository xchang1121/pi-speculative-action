import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { describe, expect, test, vi } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { workspaceSandboxFingerprint } from "../src/workspace-sandbox.ts";
import {
	createLinuxProcessBenchmark,
	forkReusableBash,
	prepareLinuxProcessReuse,
} from "../bench/linux-process-harness.ts";

describe("Linux process ExecutionWorld", () => {
	test("skips completed replay lookup when Actor execution is cheaper", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-admission-");
		const host = { execute: vi.fn(async () => ({ exitCode: 0 })) };
		const planner = vi.spyOn(fixture.backend.planner, "plan");
		const admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin").mockReturnValue({
			allowed: false, reason: "fallback_faster", waitBudgetMs: 0,
			speculativeSamples: 1, actorSamples: 1, adoptionSamples: 1,
			expectedRemainingMs: 0, expectedAdoptionMs: 100,
			expectedActorMs: 10, expectedNetBenefitMs: -90,
		});
		try {
			const command = ":";
			const executor = fixture.backend.completedReplayExecutor(host, {
				sourceRoot: fixture.workspace,
				invocation: () => resolvePiToolInvocation("bash", { command }, {
					cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath,
				})?.process,
			});
			await executor.execute({
				command, cwd: fixture.workspace, environment: fixture.environment, onData: () => undefined,
			});
			expect(host.execute).toHaveBeenCalledOnce();
			expect(planner).not.toHaveBeenCalled();
		} finally {
			admission.mockRestore();
			await fixture.dispose();
		}
	});

	test("keeps eligible producers concurrent and classifies an ineligible sibling", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-concurrency-");
		let branch: Awaited<ReturnType<typeof forkReusableBash>> | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			await writeFile(path.join(fixture.workspace, "barrier-worker"), [
				"#!/bin/sh", "set -C",
				"if : > \"$1/slot\" 2>/dev/null; then self=one other=two; else self=two other=one; fi",
				": > \"$1/$self\"", "while [ ! -e \"$1/$other\" ]; do :; done",
			].join("\n"));
			await chmod(path.join(fixture.workspace, "barrier-worker"), 0o755);
			await writeFile(path.join(fixture.workspace, "redirect-worker"), "#!/bin/sh\nprintf 'redirected\\n'\n");
			await chmod(path.join(fixture.workspace, "redirect-worker"), 0o755);
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			branch = await forkReusableBash(fixture, {
				label: "concurrency",
				command: "mkdir barrier; barrier-worker barrier & barrier-worker barrier & wait; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; }",
				actionNamespace: "process-concurrency-test.v1",
				executionFingerprint,
			});
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(2);
			expect(branch.executionMetrics.reuse?.bypasses).toBe(1);
			expect(JSON.stringify(await branch.validate?.())).toContain("broker_bypass:redirect-worker:output_endpoint_mismatch");
		} finally {
			await branch?.dispose();
			await fixture.dispose();
		}
	}, 15_000);

	test("defers native health and storage work until an explicit refresh", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-health-"));
		const storeRoot = path.join(root, "store");
		const backend = new LinuxProcessReuseBackend({ storeRoot });
		const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(createLocalBashOperations()));
		const world = createLinuxProcessExecutionWorld({ coordinator, backend, storeRoot });
		try {
			const lazy = await world.speculation.diagnostics?.({ cwd: root });
			expect(lazy).toEqual({ state: "registered", detail: "Checked on first process fork" });
			await expect(stat(storeRoot)).rejects.toThrow();
			const expected = await backend.check(true);
			const actual = await world.speculation.diagnostics?.({ cwd: root, refresh: true });
			expect(actual?.state).toBe(expected.state === "ready" ? "ready" : "unavailable");
			expect(actual?.detail).toContain(expected.detail);
		} finally {
			await world.dispose?.();
			await rm(root, { recursive: true, force: true });
		}
	});

	test("rejects adoption when the COW driver forces a handled cross-device rename", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-driver-semantics-"));
		const workspace = path.join(root, "workspace");
		const storeRoot = path.join(root, "store");
		await mkdir(path.join(workspace, "source"), { recursive: true });
		await writeFile(path.join(workspace, "source", "value.txt"), "value\n", "utf8");
		const shellPath = "/bin/bash";
		const environment = Object.freeze({
			PATH: `/home/${os.userInfo().username}/.local/bin:/usr/local/bin:/usr/bin:/bin`,
			HOME: os.homedir(),
			LANG: "C.UTF-8",
		});
		const operations = createLocalBashOperations({ shellPath });
		const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(operations));
		const backend = new LinuxProcessReuseBackend({ storeRoot });
		const world = createLinuxProcessExecutionWorld({ coordinator, backend, storeRoot, driver: "overlayfs" });
		const tool = createBashTool(workspace, {
			operations: coordinator.operations,
			shellPath,
			exposeSessionEnvironment: false,
			spawnHook: (context) => ({ ...context, env: { ...environment } }),
		});
		let branch: Awaited<ReturnType<typeof world.speculation.execute>> | undefined;
		try {
			const status = await backend.check(true);
			if (status.state !== "ready") throw new Error(status.detail);
			await world.speculation.prepare?.({ cwd: workspace });
			const args = { command: "mv source moved" };
			const invocation = resolvePiToolInvocation("bash", args, { cwd: workspace, environment, shellPath });
			if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
			const executionFingerprint = `${await backend.fingerprint()}:${await workspaceSandboxFingerprint(
				{ driver: "overlayfs" },
				workspace,
			)}`;
			const action = PI_ACTION_SEMANTICS.buildKey("bash", args, workspace, "driver-semantics-test.v1", {
				fingerprint: executionFingerprint,
				context: invocation,
			});
			if (!action) throw new Error("Pi Bash action could not be keyed");
			branch = await world.speculation.execute({
				cwd: workspace,
				tool,
				toolName: "bash",
				args,
				action,
				callID: "driver-semantics-test",
				signal: new AbortController().signal,
			});
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			const validation = await branch.validate?.();
			expect(validation?.status).toBe("indeterminate");
			expect(JSON.stringify(validation)).toContain("filesystem_semantics");
			expect(branch.executionMetrics.reuse?.requests).toBeGreaterThan(0);
			expect(branch.executionMetrics.reuse?.executionMs).toBeGreaterThan(0);
			expect(backend.metrics().tainted).toBeGreaterThan(0);
			expect(backend.metrics().published).toBe(0);
			expect((await backend.store.stats()).certificates).toBe(0);
			expect((await stat(path.join(workspace, "source"))).isDirectory()).toBe(true);
			await expect(stat(path.join(workspace, "moved"))).rejects.toThrow();
		} finally {
			await branch?.dispose();
			await world.dispose?.();
			await rm(root, { recursive: true, force: true });
		}
	});
});
