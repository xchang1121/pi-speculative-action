import { mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { describe, expect, test } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { workspaceSandboxFingerprint } from "../src/workspace-sandbox.ts";

describe("Linux process ExecutionWorld", () => {
	test("reports backend health without claiming that registration is availability", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-health-"));
		const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(root, "store") });
		const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(createLocalBashOperations()));
		const world = createLinuxProcessExecutionWorld({ coordinator, backend, storeRoot: path.join(root, "store") });
		try {
			const expected = await backend.check(true);
			const actual = await world.diagnostics?.({ cwd: root, refresh: true });
			expect(actual?.state).toBe(expected.state === "ready" ? "ready" : "unavailable");
			expect(actual?.detail).toContain(expected.detail);
		} finally {
			await world.dispose?.();
			await rm(root, { recursive: true, force: true });
		}
	});

	test("rejects adoption when the COW driver forces a handled cross-device rename", async () => {
		if (process.platform !== "linux" || !(await linuxOverlayfsCapability()).available) return;
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
		let branch: Awaited<ReturnType<typeof world.fork>> | undefined;
		try {
			const status = await backend.check(true);
			if (status.state !== "ready") throw new Error(status.detail);
			await world.prepare?.({ cwd: workspace });
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
			branch = await world.fork({
				cwd: workspace,
				tool,
				toolName: "bash",
				args,
				action,
				callID: "driver-semantics-test",
				signal: new AbortController().signal,
			});
			expect(branch.output.isError).toBe(false);
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
