import { execFileSync } from "node:child_process";
import * as childProcess from "node:child_process";
import * as filesystem from "node:fs/promises";
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
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import {
	createLinuxProcessBenchmark,
	forkReusableBash,
	prepareLinuxProcessReuse,
} from "../bench/linux-process-harness.ts";

vi.mock("node:child_process", { spy: true });
vi.mock("node:fs/promises", { spy: true });

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

	test("owns output independently of tracer descriptors, preserves concurrency and rejects an internal pipe", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-concurrency-");
		const { readlink } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const { spawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const spawning = vi.mocked(childProcess.spawn);
		const allocations = vi.mocked(filesystem.mkdtemp);
		const sampling = vi.spyOn(filesystem, "readlink").mockImplementation((...args) => {
			const tracer = spawning.mock.results.some(({ value }) => value?.pid && String(args[0]) === `/proc/${value.pid}/fd/1`);
			return (tracer ? Promise.resolve("pipe:[0]") : readlink(...args)) as ReturnType<typeof readlink>;
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
				command: "mkdir barrier; barrier-worker barrier & barrier-worker barrier & wait; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; }; printf '%32768s:end' ''",
				actionNamespace: "process-concurrency-test.v1",
				executionFingerprint,
			});
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			const text = branch.output.result.content[0];
			expect(text?.type === "text" && text.text.endsWith(" ".repeat(32768) + ":end")).toBe(true);
			expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(2);
			expect(branch.executionMetrics.reuse?.bypasses).toBe(1);
			expect(JSON.stringify(await branch.validate?.())).toContain("broker_bypass:redirect-worker:output_endpoint_mismatch");
			await branch.dispose();
			branch = undefined;
			for (const failure of ["spawn", "abort"] as const) {
				const controller = new AbortController();
				const args = { command: "while :; do :; done" };
				const context = resolvePiToolInvocation("bash", args, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath });
				const action = PI_ACTION_SEMANTICS.buildKey("bash", args, fixture.workspace, "output-lifetime", { fingerprint: executionFingerprint, context })!;
				spawning.mockImplementation((...input) => {
					const top = JSON.stringify(input[1]).includes("top-trace-");
					const child = spawn(...(top && failure === "spawn" ? [path.join(fixture.root, "missing-executable"), input[1], input[2]] : input) as Parameters<typeof spawn>);
					if (top && failure === "abort") child.once("spawn", () => controller.abort());
					return child;
				});
				await expect(fixture.world.speculation.execute({ cwd: fixture.workspace, tool: fixture.tool, toolName: "bash", args, action,
					callID: failure, signal: controller.signal })).rejects.toThrow("top-level workspace capture is missing");
			}
			for (const root of await Promise.all(allocations.mock.results.map(({ value }) => value))) {
				if (typeof root === "string" && path.basename(root).startsWith("pi-process-output-")) await expect(stat(root)).rejects.toMatchObject({ code: "ENOENT" });
			}
		} finally {
			spawning.mockRestore();
			sampling.mockRestore();
			await branch?.dispose();
			await fixture.dispose();
		}
	}, 15_000);

	test("defers native initialization and preserves opaque process output without path rewriting", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-health-"));
		const storeRoot = path.join(root, "store");
		const backend = new LinuxProcessReuseBackend({ storeRoot });
		const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(createLocalBashOperations()));
		const world = createLinuxProcessExecutionWorld({ coordinator, tools: PI_OPERATION_TOOLS.process, backend, storeRoot });
		let payload = "";
		const close = vi.fn(async () => {});
		vi.spyOn(backend, "open").mockImplementation(async ({ workspace }) => ({
			executor: { execute: async (request) => { payload = `opaque bytes: ${workspace.sandboxRoot}`; request.onData(Buffer.from(payload)); return { exitCode: 0 }; } },
			metrics: emptyWorldReuseMetrics, seal: async () => [], close,
			validate: async () => ({ status: "valid", metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } }),
		}));
		try {
			expect(await world.speculation.diagnostics?.({ cwd: root })).toMatchObject({ state: "registered" });
			await expect(stat(storeRoot)).rejects.toThrow();
			const args = { command: "opaque" };
			const invocation = resolvePiToolInvocation("bash", args, { cwd: root, environment: {} })!;
			const action = PI_ACTION_SEMANTICS.buildKey("bash", args, root, "", { fingerprint: "fake-process", context: invocation })!;
			const branch = await world.speculation.execute({ cwd: root, toolName: "bash", args, action, callID: "opaque",
				tool: createBashTool(root, { operations: coordinator.operations }), signal: new AbortController().signal });
			try { expect(branch.output.result.content).toEqual([{ type: "text", text: payload }]); }
			finally { await branch.dispose(); }
			expect(close).toHaveBeenCalledOnce();
		} finally {
			await world.dispose?.();
			await rm(root, { recursive: true, force: true });
		}
	});

	test("rejects adoption when the COW driver forces a handled cross-device rename", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const fixture = await createLinuxProcessBenchmark("pi-process-driver-semantics-", "overlayfs");
		const { workspace, backend } = fixture;
		let branch: Awaited<ReturnType<typeof forkReusableBash>> | undefined;
		try {
			await mkdir(path.join(workspace, "source"));
			await writeFile(path.join(workspace, "source", "value.txt"), "value\n", "utf8");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { workspaceDriver: "overlayfs", includeWorkspaceFingerprint: true });
			branch = await forkReusableBash(fixture, { command: "mv source moved", label: "driver-semantics-test",
				actionNamespace: "driver-semantics-test.v1", executionFingerprint });
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
			await fixture.dispose();
		}
	});
});
