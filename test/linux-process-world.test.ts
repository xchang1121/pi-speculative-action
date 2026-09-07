import { execFileSync } from "node:child_process";
import * as childProcess from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { describe, expect, test, vi } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { LinuxHeldExecBoundary } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
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

vi.mock("node:child_process", { spy: true });

describe("Linux process ExecutionWorld", () => {
	test("owns the entire Actor call when a held child crosses the adoption boundary", async ({ skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-held-transaction-"));
		const binary = path.join(root, "helper");
		execFileSync("cc", ["-O2", "-Wall", "-Wextra", "-Werror", fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), "-o", binary]);
		const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary });
		try {
			for (const disposition of [undefined, "recoverable", "poisoned"] as const) {
				const after = path.join(root, `after-${disposition}`);
				const commit = vi.fn(async () => {
					if (disposition) throw effectCommitFailure(new Error("injected commit failure"), disposition);
				});
				const executor = boundary.executor(adaptProcessToolOperations(createLocalBashOperations({ shellPath: binary })), {
					sourceRoot: root, realShell: "/bin/bash",
					decide: async () => ({ kind: "replay", output: [], exitCode: 0, commit }),
				});
				const run = executor.execute({ command: `/bin/true; printf continued > '${after}'`, cwd: root,
					environment: { PATH: "/usr/bin:/bin" }, onData: () => {}, timeout: 5 });
				if (disposition) {
					await expect(run).rejects.toMatchObject({ disposition: "poisoned" });
					await expect(stat(after)).rejects.toThrow();
				} else {
					expect(await run).toEqual({ exitCode: 0 });
					expect(await readFile(after, "utf8")).toBe("continued");
				}
				expect(commit).toHaveBeenCalledOnce();
			}
		} finally {
			await boundary.close();
			await rm(root, { recursive: true, force: true });
		}
	});
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

	test("waits for descriptor publication, keeps producers concurrent and classifies an ineligible sibling", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-concurrency-");
		const { spawn: nativeSpawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const spawning = vi.spyOn(childProcess, "spawn").mockImplementation((...args: Parameters<typeof childProcess.spawn>) => {
			const child = nativeSpawn(...args), pid = child.pid;
			Object.defineProperty(child, "pid", { configurable: true, value: process.pid });
			child.prependOnceListener("spawn", () => Object.defineProperty(child, "pid", { value: pid }));
			return child;
		});
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
			expect(spawning).toHaveBeenCalled();
			expect(JSON.stringify(await branch.validate?.())).toContain("broker_bypass:redirect-worker:output_endpoint_mismatch");
		} finally {
			spawning.mockRestore();
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
			expect(world.speculation.tools).toEqual(PI_ACTION_SEMANTICS.toolNames("unbounded"));
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
