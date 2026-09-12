import { deferred, nextTurn } from "./async.ts";
import { execFileSync } from "node:child_process";
import * as childProcess from "node:child_process";
import { existsSync } from "node:fs";
import * as filesystem from "node:fs/promises";
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { describe, expect, test, vi } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { LinuxHeldExecBoundary } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { ProcessHandoffOwnership } from "../src/process-handoff.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import type { WorkspaceTransactionCapture } from "../src/workspace-transaction.ts";
import {
	createLinuxProcessBenchmark,
	executeReusableBash,
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
			const completed = path.join(root, "descendant-completed"), pidFile = path.join(root, "descendant-pid");
			const command = `/usr/bin/setsid /bin/sh -c 'echo $$ > ${pidFile}; sleep 0.1; echo done > ${completed}' </dev/null >/dev/null 2>&1 & exit 7`;
			expect(childProcess.spawnSync(binary, ["/bin/bash", "-c", command]).status).toBe(7);
			expect(await readFile(completed, "utf8"), "parent exit must drain detached descendants").toBe("done\n");
			await rm(pidFile);
			const tracer = childProcess.spawn(binary, ["/bin/bash", "-c", command.replace("sleep 0.1", "sleep 10").replace("exit 7", "wait")]);
			const stopped = new Promise<void>((resolve) => tracer.once("close", () => resolve()));
			let pid = 0;
			try {
				for (let retry = 0; retry < 100 && !pid; retry++) {
					pid = Number(await readFile(pidFile, "utf8").catch(() => ""));
					if (!pid) await new Promise((resolve) => setTimeout(resolve, 10));
				}
				expect(pid).toBeGreaterThan(0);
				tracer.kill("SIGKILL");
				await stopped;
				let alive = true;
				for (let retry = 0; retry < 100 && alive; retry++) {
					const state = (await readFile(`/proc/${pid}/stat`, "utf8").catch(() => "")).match(/\) (\w) /)?.[1];
					alive = state !== undefined && state !== "Z";
					if (alive) await new Promise((resolve) => setTimeout(resolve, 10));
				}
				expect(alive, "killed tracer must not release its tracees to run").toBe(false);
			} finally {
				tracer.kill("SIGKILL");
				if (pid) try { process.kill(-pid, "SIGKILL"); } catch { /* Already reaped. */ }
				await stopped;
			}
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
			let closed = false;
			const { promise: entered, resolve: started } = deferred();
			const { promise: completion, resolve: release } = deferred();
			const executor = boundary.executor({ execute: async () => {
				started(); await completion; return { exitCode: 0 };
			} }, { sourceRoot: root, realShell: "/bin/bash", decide: async () => ({ kind: "continue" }) });
			const running = executor.execute({ command: ":", cwd: root, environment: {}, onData: () => {} });
			await entered;
			const closing = boundary.close().then(() => { closed = true; });
			try {
				await nextTurn();
				expect(closed, "close must wait for the owned executor and concurrent callers").toBe(false);
				const concurrent = boundary.close().then(() => { expect(closed).toBe(true); });
				release();
				await Promise.all([closing, concurrent, running]);
			} finally { release(); await Promise.allSettled([closing, running]); }
		} finally {
			await boundary.close();
			await rm(root, { recursive: true, force: true });
		}
	});
	test("defers empty replay, retains later evidence and drains lazy preparation before refresh", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-admission-");
		const host = { execute: vi.fn(async () => ({ exitCode: 0 })) };
		const held = { execute: vi.fn(async () => ({ exitCode: 0 })) }, close = vi.fn(async () => {});
		const { promise: pending, resolve: release } = deferred();
		const opening = vi.spyOn(LinuxHeldExecBoundary, "open").mockImplementation(async () => {
			await pending;
			return { shellPath: fixture.shellPath, executor: () => held, close } as unknown as LinuxHeldExecBoundary;
		});
		const planner = vi.spyOn(fixture.backend.planner, "plan");
		const observed = vi.spyOn(SpeculationScheduler.prototype, "observeActorService");
		const admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin").mockReturnValue({
			allowed: false, reason: "fallback_faster", waitBudgetMs: 0,
			speculativeSamples: 1, actorSamples: 1, adoptionSamples: 1,
			expectedRemainingMs: 0, expectedAdoptionMs: 100,
			expectedActorMs: 10, expectedNetBenefitMs: -90,
		});
		const invocation = vi.fn(() => resolvePiToolInvocation("bash", { command: ":" }, {
			cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath,
		})?.process);
		const coordinator = new ProcessExecutionCoordinator(host, {
			enabled: () => true,
			prepare: (refresh) => fixture.backend.prepareActorReplay(host, {
				sourceRoot: fixture.workspace, invocation,
				held: { realShell: fixture.shellPath, executor: () => held },
			}, refresh),
			reset: () => fixture.backend.resetActorReplay(),
		});
		const invoke = () => coordinator.operations.exec(":", fixture.workspace, { env: fixture.environment, onData: () => {} });
		let calls: Promise<unknown> | undefined, refreshing: Promise<unknown> | undefined;
		try {
			await invoke();
			expect(host.execute).toHaveBeenCalledOnce();
			expect(invocation).not.toHaveBeenCalled(); expect(opening).not.toHaveBeenCalled(); expect(observed).not.toHaveBeenCalled();
			expect(coordinator.actorDiagnostics().state).toBe("degraded");
			await mkdir(path.join(fixture.storeRoot, "certificates", "00"), { recursive: true });
			calls = Promise.all([invoke(), invoke()]);
			await vi.waitFor(() => expect(opening).toHaveBeenCalledOnce());
			await fixture.backend.store.clear();
			refreshing = coordinator.refreshActorRoute();
			await invoke();
			expect(host.execute).toHaveBeenCalledTimes(2); expect(close).not.toHaveBeenCalled();
			expect(coordinator.actorDiagnostics().state).toBe("probing");
			release(); await Promise.all([calls, refreshing]);
			expect(held.execute).toHaveBeenCalledTimes(2); expect(invocation).toHaveBeenCalledTimes(2);
			expect(planner).not.toHaveBeenCalled(); expect(admission).toHaveBeenCalledTimes(2);
			expect(observed).toHaveBeenCalledTimes(2); expect(opening).toHaveBeenCalledTimes(2); expect(close).toHaveBeenCalledOnce();
			expect(coordinator.actorDiagnostics().state).toBe("ready");
			await invoke(); // Clearing evidence is rechecked even after the helper was initialized.
			expect(host.execute).toHaveBeenCalledTimes(3); expect(invocation).toHaveBeenCalledTimes(2);
			const availability = vi.spyOn(fixture.backend.store, "mayHaveCertificates").mockImplementationOnce(async () => {
				await fixture.backend.check(); return false; // A producer can begin while the empty lookup awaits IO.
			});
			try { await invoke(); expect(held.execute).toHaveBeenCalledTimes(3); } finally { availability.mockRestore(); }
			opening.mockRejectedValueOnce(new Error("held-exec functional probe failed"));
			await coordinator.refreshActorRoute();
			expect(coordinator.actorDiagnostics()).toMatchObject({ state: "degraded", detail: expect.stringContaining("functional probe failed") });
			await invoke(); expect(host.execute).toHaveBeenCalledTimes(4);
		} finally {
			release(); await Promise.allSettled([calls, refreshing]);
			await coordinator.dispose(); opening.mockRestore(); admission.mockRestore(); observed.mockRestore();
			await fixture.dispose();
		}
	});

	test("owns output and capture lifetimes, preserves concurrency and rejects an internal pipe", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-concurrency-");
		const { readlink, readFile: readTrace, rm: removeFile, mkdtemp: allocateRoot } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const { spawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const kill = process.kill.bind(process), killing = vi.spyOn(process, "kill");
		const open = fixture.backend.open.bind(fixture.backend);
		let closeSession: (() => Promise<void>) | undefined;
		let onClose: ((workspace: string, first: Promise<void>, close: () => Promise<void>) => void) | undefined;
		const captures: WorkspaceTransactionCapture[] = [];
		let restoreTransactions: (() => void) | undefined;
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(async (input) => {
			if (!restoreTransactions) {
				const begin = input.workspace.transactions.begin;
				const recording = vi.spyOn(input.workspace.transactions, "begin").mockImplementation(async () => {
					const capture = await begin(), recorded = { finish: vi.fn(capture.finish), abort: vi.fn(capture.abort) };
					captures.push(recorded);
					return recorded;
				});
				restoreTransactions = () => recording.mockRestore();
			}
			const session = await open({ ...input, signal: AbortSignal.any([AbortSignal.timeout(8000), ...(input.signal ? [input.signal] : [])]) });
			const wrapped = { ...session, close: () => { const first = session.close(); onClose?.(input.workspace.sandboxRoot, first, session.close); return first; } };
			closeSession = wrapped.close;
			return wrapped;
		});
		const createServer = net.createServer;
		let broker: { server: net.Server; socket: net.Socket; path: string } | undefined;
		const servers = vi.spyOn(net, "createServer").mockImplementation((...args) => {
			const server = createServer(...args);
			server.on("connection", (socket) => {
				const address = server.address();
				if (typeof address === "string" && path.basename(address).startsWith("broker-")) broker = { server, socket, path: address };
			});
			return server;
		});
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
				"if ( : > \"$1/slot\" ) 2>/dev/null; then self=one other=two; else self=two other=one; fi",
				": > \"$1/$self\"", "while [ ! -e \"$1/$other\" ]; do :; done",
			].join("\n"));
			await chmod(path.join(fixture.workspace, "barrier-worker"), 0o755);
			await writeFile(path.join(fixture.workspace, "redirect-worker"), "#!/bin/sh\nprintf 'redirected\\n'\n");
			await chmod(path.join(fixture.workspace, "redirect-worker"), 0o755);
			await fixture.world.speculation.prepare?.({ cwd: fixture.workspace });
			const executionFingerprint = await fixture.backend.fingerprint();
			let allocationFailed = false;
			allocations.mockImplementation((...args) => {
				if (!allocationFailed && path.basename(String(args[0])) === "trace-") {
					allocationFailed = true;
					return Promise.reject(Object.assign(new Error("injected trace allocation failure"), { code: "ENOSPC" }));
				}
				return allocateRoot(...args);
			});
			branch = await forkReusableBash(fixture, {
				label: "concurrency",
				command: "set -e; /usr/bin/printf 'trace-root-fallback\\n'; mkdir barrier; barrier-worker barrier & first=$!; barrier-worker barrier & second=$!; wait \"$first\"; wait \"$second\"; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; printf '%s\\n' \"$line\"; }; printf '%32768s:end' ''",
				actionNamespace: "process-concurrency-test.v1",
				executionFingerprint,
			});
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			const text = branch.output.result.content[0];
			expect(text?.type === "text" && text.text).toBe("trace-root-fallback\nredirected\n" + " ".repeat(32768) + ":end");
			const nextCapture = await vi.mocked(captures[1]!.finish).mock.results[0]!.value;
			expect({ allocationFailed, aborts: vi.mocked(captures[0]!.abort).mock.calls.length, nextComplete: nextCapture.complete },
				nextCapture.complete ? undefined : nextCapture.reason).toEqual({ allocationFailed: true, aborts: 1, nextComplete: true });
			expect(captures[0]!.finish).not.toHaveBeenCalled();
			expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(3);
			expect(branch.executionMetrics.reuse?.bypasses).toBe(1);
			expect(JSON.stringify(await branch.validate?.())).toContain("broker_bypass:redirect-worker:output_endpoint_mismatch");
			await branch.dispose();
			branch = undefined;
			for (const failure of ["spawn", "abort", "nested-abort", "nested-seal", "session-close"] as const) {
				const controller = new AbortController();
				const args = { command: failure === "nested-seal" ? "/bin/true" : failure === "spawn" || failure === "abort" ? "while :; do :; done" : "/bin/sleep 1" };
				let nested: childProcess.ChildProcess | undefined, nestedClosed: Promise<void> | undefined, ownedAtClose = false;
				const closeTasks: Promise<void>[] = [];
				const closing = new Promise<void>((resolve) => { onClose = (workspace, first, close) => {
					ownedAtClose = existsSync(workspace);
					if (failure === "nested-seal") closeTasks.push(first, close());
					resolve();
				}; });
				let traceRoot: string | undefined, socketRemoved = false;
				const { promise: captureStarted, resolve: reachCapture } = deferred();
				const { promise: captureGate, resolve: releaseCapture } = deferred();
				const { promise: captureCleanup, resolve: captureCleaned } = deferred();
				const reading = vi.spyOn(filesystem, "readFile").mockImplementation((...args) => {
					if (failure === "nested-seal" && !traceRoot && String(args[0]).includes("/trace-")) {
						traceRoot = path.dirname(String(args[0])); reachCapture();
						return captureGate.then(() => readTrace(...args)) as ReturnType<typeof readTrace>;
					}
					return readTrace(...args);
				});
				const removing = vi.spyOn(filesystem, "rm").mockImplementation((...args) => {
					if (String(args[0]) === broker?.path) socketRemoved = true;
					const removed = removeFile(...args);
					if (String(args[0]) === traceRoot) void removed.then(captureCleaned, captureCleaned);
					return removed;
				});
				let restoreServerClose: (() => void) | undefined;
				const context = resolvePiToolInvocation("bash", args, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath });
				const action = PI_ACTION_SEMANTICS.buildKey("bash", args, fixture.workspace, "output-lifetime", { fingerprint: executionFingerprint, context })!;
				spawning.mockImplementation((...input) => {
					const top = JSON.stringify(input[1]).includes("top-trace-");
					const child = spawn(...(top && failure === "spawn" ? [path.join(fixture.root, "missing-executable"), input[1], input[2]] : input) as Parameters<typeof spawn>);
					if (top && failure === "abort") child.once("spawn", () => controller.abort());
					if ((failure === "session-close" && top) || (failure.startsWith("nested-") && JSON.stringify(input[1]).includes("/trace-"))) {
						nested = child;
						nestedClosed = new Promise((resolve) => child.once("close", () => resolve()));
						if (failure === "nested-abort") child.once("spawn", () => { kill(-child.pid!, "SIGSTOP"); controller.abort(); });
						if (failure === "session-close") child.once("spawn", () => { kill(-child.pid!, "SIGSTOP"); closeTasks.push(closeSession!()); });
					}
					return child;
				});
				const running = fixture.world.speculation.execute({ cwd: fixture.workspace, tool: fixture.tool, toolName: "bash", args, action,
					callID: failure, signal: controller.signal });
				void running.catch(() => undefined);
				try {
					if (failure === "nested-abort" || failure === "session-close") {
						await closing;
						expect(nested?.pid).toBeTypeOf("number");
						expect({ ownedAtClose, childCancelled: killing.mock.calls.some(([pid, signal]) => pid === -nested!.pid! && signal === "SIGKILL") })
							.toEqual({ ownedAtClose: true, childCancelled: true });
						}
					if (failure === "nested-seal") {
						await captureStarted;
						const transport = broker!, closeServer = transport.server.close.bind(transport.server);
						const { promise: serverStopped, resolve: reached } = deferred();
						const stopping = vi.spyOn(transport.server, "close").mockImplementation((callback) => closeServer((error) => { callback?.(error); reached(); }));
						restoreServerClose = () => stopping.mockRestore();
						transport.socket.destroy(); controller.abort();
						await serverStopped; // Production's close callback resumes before this observation, without a time threshold.
						expect({ ownedAtClose, sharedClose: closeTasks[0] === closeTasks[1], socketRemoved })
							.toEqual({ ownedAtClose: true, sharedClose: true, socketRemoved: false });
						releaseCapture();
					}
					await expect(running).rejects.toThrow("top-level workspace capture is missing");
				} finally {
					releaseCapture();
					if (nested?.pid && nested.exitCode === null && nested.signalCode === null) try { kill(-nested.pid, "SIGKILL"); } catch { /* Already reaped. */ }
					await nestedClosed;
					if (traceRoot) await captureCleanup;
					await Promise.allSettled(closeTasks);
					await running.then((value) => value.dispose(), () => undefined);
					restoreServerClose?.(); reading.mockRestore(); removing.mockRestore();
					onClose = undefined;
				}
			}
			for (const result of await Promise.allSettled(allocations.mock.results.map(({ value }) => value))) {
				const root = result.status === "fulfilled" ? result.value : undefined;
				if (typeof root === "string" && path.basename(root).startsWith("pi-process-output-")) await expect(stat(root)).rejects.toMatchObject({ code: "ENOENT" });
			}
		} finally {
			restoreTransactions?.();
			servers.mockRestore();
			opening.mockRestore();
			killing.mockRestore();
			spawning.mockRestore();
			allocations.mockRestore();
			sampling.mockRestore();
			await branch?.dispose();
			await fixture.dispose();
		}
	}, 15_000);

	test.for(["trace", "transaction"] as const)("drains both capture owners when %s sealing fails", { timeout: 15_000 }, async (failure, { skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-capture-failure-");
		const { readFile: readTrace, rm: removeFile } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const { spawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const entered = deferred(), failed = deferred(), gate = deferred();
		const error = new Error(`injected ${failure} capture failure`);
		let traceRoot: string | undefined, released = false, cleanupBeforeRelease = false, returned = false, executions = 0;
		let restoreTransactions: (() => void) | undefined;
		const open = fixture.backend.open.bind(fixture.backend);
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(async (input) => {
			const begin = input.workspace.transactions.begin;
			const recording = vi.spyOn(input.workspace.transactions, "begin").mockImplementation(async () => {
				const capture = await begin();
				return { abort: capture.abort, finish: async () => {
					if (failure === "trace") { entered.resolve(); await gate.promise; return capture.finish(); }
					await entered.promise; await capture.abort(); failed.resolve(); throw error;
				} };
			});
			restoreTransactions = () => recording.mockRestore();
			return open(input);
		});
		const reading = vi.spyOn(filesystem, "readFile").mockImplementation(async (...args) => {
			if (String(args[0]).includes("/trace-")) {
				traceRoot = path.dirname(String(args[0]));
				if (failure === "trace") { await entered.promise; failed.resolve(); throw error; }
				entered.resolve(); await gate.promise;
			}
			return readTrace(...args);
		});
		const removing = vi.spyOn(filesystem, "rm").mockImplementation((...args) => {
			if (String(args[0]) === traceRoot && !released) cleanupBeforeRelease = true;
			return removeFile(...args);
		});
		const spawning = vi.mocked(childProcess.spawn).mockImplementation((...args) => {
			if (JSON.stringify(args[1]).includes("/trace-")) executions++;
			return spawn(...args);
		});
		let running: ReturnType<typeof forkReusableBash> | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			running = forkReusableBash(fixture, { command: "/usr/bin/printf capture-once", label: failure,
				actionNamespace: "capture-owners.v1", executionFingerprint });
			void running.then(() => { returned = true; }, () => { returned = true; });
			await failed.promise; await nextTurn();
			expect({ cleanupBeforeRelease, returned }, "a failed capture must drain its pending sibling").toEqual({ cleanupBeforeRelease: false, returned: false });
			released = true; gate.resolve();
			const branch = await running;
			await expect(stat(traceRoot!)).rejects.toMatchObject({ code: "ENOENT" });
			expect(branch.output.result.content).toEqual([{ type: "text", text: "capture-once" }]);
			expect({ executions, published: fixture.backend.metrics().published }).toEqual({ executions: 1, published: 0 });
			const validation = await branch.validate?.();
			expect(validation?.status).toBe("indeterminate");
			expect(JSON.stringify(validation)).toContain(`nested_capture:${error.message}`);
		} finally {
			released = true; gate.resolve();
			await running?.then((branch) => branch.dispose(), () => undefined);
			restoreTransactions?.(); opening.mockRestore(); reading.mockRestore(); removing.mockRestore(); spawning.mockRestore();
			await fixture.dispose();
		}
	});

	test("defers native initialization and preserves opaque process output without path rewriting", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-health-"));
		const storeRoot = path.join(root, "store");
		const backend = new LinuxProcessReuseBackend({ storeRoot });
		const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(createLocalBashOperations()));
		const world = createLinuxProcessExecutionWorld({ coordinator, tools: PI_OPERATION_TOOLS.process, backend, storeRoot });
		let payload = "", ownedAtClose = false;
		const ownership = new ProcessHandoffOwnership();
		const close = vi.fn(async (workspace: string) => { ownedAtClose = existsSync(workspace); });
		vi.spyOn(backend, "open").mockImplementation(async ({ workspace }) => ({
			ownership,
			executor: { execute: async (request) => { payload = `opaque bytes: ${workspace.sandboxRoot}`; request.onData(Buffer.from(payload)); return { exitCode: 0 }; } },
			metrics: emptyWorldReuseMetrics, seal: async () => [], close: () => close(workspace.sandboxRoot),
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
			try {
				expect(branch.output.result.content).toEqual([{ type: "text", text: payload }]);
				await expect(branch.commit()).resolves.toEqual(branch.output);
				await expect(branch.commit()).resolves.toEqual(branch.output);
				expect(branch.commitMetrics).toBeDefined();
				expect(ownership.claimChild()).toBe(false);
				const entered = deferred(), released = deferred();
				const clock = vi.spyOn(performance, "now").mockReturnValue(0);
				const execute = vi.spyOn(world.speculation, "execute").mockResolvedValue(branch);
				const dispose = vi.spyOn(branch, "dispose").mockImplementation(() => { entered.resolve(); return released.promise; });
				let delivered = false;
				const measured = executeReusableBash({ backend, world, workspace: root, environment: {}, shellPath: invocation.process!.shell,
					tool: createBashTool(root) }, { label: "cleanup", command: "opaque", actionNamespace: "", executionFingerprint: "fake-process" })
					.then(result => { delivered = true; return result; });
				try {
					await Promise.race([entered.promise, measured]); await nextTurn(); expect(delivered).toBe(false);
					clock.mockReturnValue(100); released.resolve();
					expect((await measured).measurement.totalMs).toBe(100);
				} finally {
					released.resolve(); await measured.catch(() => undefined); clock.mockRestore(); execute.mockRestore(); dispose.mockRestore();
				}
			}
			finally { await branch.dispose(); }
			expect(close).toHaveBeenCalledOnce();
			expect(ownedAtClose).toBe(true);
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
