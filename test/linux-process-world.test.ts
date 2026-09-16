import { gated, deferred, nextTurn } from "./async.ts";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import { AsyncLocalStorage } from "node:async_hooks";
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
import { inspectHeldExecProcess, LinuxHeldExecBoundary, type HeldExecProcess } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend, validateTransferredProcessEvidence } from "../src/linux-process-backend.ts";
import { ProcessHandoffOwnership, type ProcessHandoffRegistry, type ProcessHandoff, type ProcessExecutionBinding } from "../src/process-handoff.ts";
import { sha256Digest } from "../src/provenance-certificate.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { TaskTimeline } from "../src/task-timing.ts";
import { testModel } from "./model.ts";
import type { WorkspaceTransactionCapture } from "../src/workspace-transaction.ts";
import {
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeReusableBash,
	forkReusableBash,
	prepareLinuxProcessReuse,
} from "../bench/linux-process-harness.ts";

vi.mock("node:child_process", { spy: true });
vi.mock("node:fs/promises", { spy: true });

describe("Linux process ExecutionWorld", () => {
	test.for(["completed", "running", "native", "native-merged"] as const)("reexecutes an owned child binding across turns without replaying its parent or stale input (%s)", { timeout: 20_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-binding-");
		const publishing = vi.spyOn(fixture.backend.planner, "publishCompleted");
		const native = mode.startsWith("native");
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		const release = deferred();
		let restoreJoin: (() => void) | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			await writeFile(path.join(fixture.workspace, "input.txt"), "before\n");
			await writeFile(path.join(fixture.workspace, "worker.c"), `#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc != 2 || strcmp(argv[0], "bound-name") || strcmp(argv[1], "private argument") ||
		!getenv("BOUND_SECRET") || strcmp(getenv("BOUND_SECRET"), "private value")) return 71;
	for (volatile unsigned long iteration = 0; iteration < ${mode === "running" ? 500000000 : native ? 50000000 : 0}ul; ++iteration) {}
	char text[32]; int fd = open("input.txt", O_RDONLY); ssize_t size = read(fd, text, sizeof(text));
	if (size <= 0 || write(1, text, (size_t)size) != size) return 1;
	return ${mode === "native-merged" ? 'write(2, "stderr\\n", 7) != 7' : "0"};
}
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			await commitBenchmarkFixture(fixture.workspace, "Bound process invocation");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const command = "export BOUND_SECRET='private value'; printf 'parent\\n'; exec -a bound-name worker 'private argument'" + (mode === "native-merged" ? " 2>&1" : "");
			const route = await fixture.backend.prepareActorReplay(adaptProcessToolOperations(createLocalBashOperations()), {
				sourceRoot: fixture.workspace, invocation: () => undefined, held: { realShell: fixture.shellPath,
					executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) },
			}, !native);
			if (!("executor" in route)) throw new Error(route.detail);
			const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
			let binding: ProcessExecutionBinding | undefined;
			if (!native) {
				const branch = await forkReusableBash(fixture, { command, label: "recorded", actionNamespace: "binding", executionFingerprint, executionScope: scope });
				try {
					expect(branch.output).toMatchObject({ isError: false, result: { content: [{ text: "parent\nbefore\n" }] } });
					const invocation = resolvePiToolInvocation("bash", { command }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!;
					const permission = PI_ACTION_SEMANTICS.buildKey("bash", { command }, fixture.workspace, "binding", { fingerprint: executionFingerprint, context: invocation })!;
					const binding = branch.operations![0]!;
					for (const operation of [{ binding: Object.freeze({ ...binding }), permission }, { binding, permission: { ...permission, key: "different action with colliding hash" } }]) {
						await expect(fixture.world.speculation.fingerprint!({ effect: "unbounded", requirements: PI_ACTION_SEMANTICS.definition("bash")!.requirements, tool: "bash",
							action: { ...permission, executionContext: { ...invocation, operation } } })).rejects.toThrow("binding is unavailable");
					}
				}
				finally { await branch.dispose(); }
				[binding] = fixture.backend.executionBindings(later);
				expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
				const certificate = publishing.mock.calls.find(([certificate]) => certificate.weakKey === binding!.key)?.[0];
				expect(certificate!.producer.execution.authority).toBe("speculative");
				expect(certificate!.dependencyCertificate.taints).toEqual(["clock", "random"]);
				expect(await fixture.backend.store.findByWeakKey(binding!.key, path.join(fixture.workspace, "worker"))).toEqual([]);
				expect(JSON.stringify(binding)).not.toContain("private argument");
				expect(JSON.stringify(binding)).not.toContain("private value");
				await writeFile(path.join(fixture.workspace, "input.txt"), "after\n");
				await expect(validateTransferredProcessEvidence(certificate!.dependencyCertificate)).resolves.toMatchObject({ status: "stale" });

				await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
					const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
					try {
						const result = await session.executeBinding(binding!);
						expect(result.exit).toEqual({ kind: "code", code: 0 });
						expect(result.output.map(({ fd, data }) => [fd, data.toString()])).toEqual([[1, "after\n"]]);
						await session.seal([]);
						const validation = await session.validate();
						expect(validation, JSON.stringify({ validation, metrics: session.metrics() })).toMatchObject({ status: "valid" });
						await expect(session.executeBinding(binding!)).rejects.toThrow("already consumed");
						expect(existsSync(path.join(workspace.processRoot, "process-interposition"))).toBe(false);
						expect((await filesystem.readdir(workspace.processRoot)).filter(name => name.startsWith("broker-"))).toEqual([]);
					} finally { await session.close(); }
				});

				let output = "";
				const result = await route.executor.execute({ command: command.replace("parent", "other-parent"), cwd: fixture.workspace,
					environment: fixture.environment, scope: { ...scope, turnID: "actor" }, onData: data => { output += data.toString(); } });
				expect(result).toEqual({ exitCode: 0 }); expect(output).toBe("other-parent\nafter\n");
				expect(fixture.backend.actorMetrics()).toMatchObject({ hits: 0, crossTurnHits: 0 });
			} else {
				expect(fixture.backend.executionBindings(later)).toEqual([]);
				await writeFile(path.join(fixture.workspace, "input.txt"), "after\n");
			}

			const patternSettings = patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 });
			const patternStore = new PatternAwareStore(patternSettings, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, fixture.workspace));
			const events: SpeculativeActionEvent<string>[] = [], tools = [fixture.tool];
			host = createSpeculativeActionHost(scope.sessionID, { cwd: fixture.workspace, patternStore,
				complete: async () => { throw new Error("unexpected inference"); },
				getSettings: () => ({ enabled: true, drafterEnabled: false, candidateLimit: 4, maxConcurrentActions: 4, tools: ["bash"], patternAware: patternSettings }),
				preflight: ({ args, action }) => { expect(args).toHaveProperty("command"); expect(action.input.command).toBe((args as { command: string }).command); return true; }, executionWorlds: [fixture.world],
				resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath }),
				onEvent: event => { events.push(event); },
			});
			const start = (turnID: string) => host!.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined,
				context: { systemPrompt: "unchanged", messages: [], tools } });
			const call = (turnID: string, command: string) => ({ turnID, id: turnID, tool: "bash", args: { command }, tools });
			for (const turnID of ["common-1", "common-2"]) {
				await start(turnID);
				await host.execute(call(turnID, "printf common"), undefined, () => fixture.tool.execute(turnID, { command: "printf common" }));
				await host.finishTurn(turnID);
			}
			await start("seed");
			if (!native) {
				await host.previewActorCall(call("seed", command));
				await expect.poll(() => events.some(event => event.type === "candidate" && event.turnID === "seed" &&
					event.candidate.origin === "actor_preview" && event.state.status === "succeeded"), { timeout: 5000 }).toBe(true);
			}

			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common"]);
			const seedFallback = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { ...scope, turnID: "seed" } }) }, () => fixture.tool.execute("seed", { command })));
			const suffix = mode === "native-merged" ? "stderr\n" : "";
			expect((await host.execute(call("seed", command), undefined, seedFallback)).content).toEqual([{ type: "text", text: `parent\nafter\n${suffix}` }]);
			await host.finishTurn("seed");
			expect(seedFallback).toHaveBeenCalledOnce(); // The whole Bash metadata proof remains rejected; the child can still be adopted.
			expect(fixture.backend.actorMetrics().hits).toBe(native ? 0 : 1);
			binding ??= fixture.backend.executionBindings(later).at(-1);
			expect(binding, "a real native miss must retain its launch without publishing a result").toBeDefined();
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", command]);
			await writeFile(path.join(fixture.workspace, "input.txt"), "newest\n");
			const before = fixture.backend.metrics();
			let sealing = false, joining: boolean | undefined;
			let joinEvidence: unknown;
			if (mode === "running") {
				const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
				const publish = handoffs.publish.bind(handoffs);
				const publication = vi.spyOn(handoffs, "publish").mockImplementation(async (...args) => {
					sealing = true; await release.promise; return publish(...args);
				});
				const scheduler = Reflect.get(fixture.backend, "processScheduler") as SpeculationScheduler<object>;
				const assess = scheduler.assessCandidateJoin.bind(scheduler);
				const assessment = vi.spyOn(scheduler, "assessCandidateJoin").mockImplementation(request => {
					const decision = assess(request);
					if (request.state === "running") {
						joining = decision.allowed; joinEvidence = { request, decision };
						if (joining) queueMicrotask(() => release.resolve());
					}
					return decision;
				});
				restoreJoin = () => { publication.mockRestore(); assessment.mockRestore(); };
			}
			await start("prepared");
			if (mode === "running") await expect.poll(() => sealing, { timeout: 5000 }).toBe(true);
			else await expect.poll(() => events.filter(event => event.turnID === "prepared" && (event.type === "candidate" || event.type === "operation_prediction"))
				.map(event => event.type === "candidate" ? [event.candidate.kind, event.state.status] : event.type === "operation_prediction" ? event.settlement : undefined), { timeout: 5000 }).toContainEqual(["operation", "succeeded"]);
			expect(fixture.backend.metrics().misses).toBeGreaterThan(before.misses);
			const changedParent = command.replace("parent", "automatic-parent");
			if (mode === "running") {
				let output = "";
				await route.executor.execute({ command: changedParent, cwd: fixture.workspace, environment: fixture.environment,
					scope: { ...scope, turnID: "foreign" }, onData: bytes => { output += bytes.toString(); } });
				expect(output).toBe("automatic-parent\nnewest\n");
				expect(joining, "another turn cannot wait for this one-shot producer").toBeUndefined();
			}
			const actor = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { ...scope, turnID: "prepared" } }) }, () => fixture.tool.execute("prepared", { command: changedParent })));
			const nativeExecution = host.execute(call("prepared", changedParent), undefined, actor);
			void nativeExecution.catch(() => undefined);
			if (mode === "running") {
				await expect.poll(() => joining).toBeDefined();
				expect(joining, JSON.stringify(joinEvidence)).toBe(true);
			}
			expect((await nativeExecution).content).toEqual([{ type: "text", text: `automatic-parent\nnewest\n${suffix}` }]);
			expect(actor).toHaveBeenCalledOnce();
			expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify({ joinEvidence, metrics: fixture.backend.actorMetrics() })).toBe(Number(mode === "running"));
			await host.finishTurn("prepared");
			const execution = events.filter(event => event.type === "actor_action")
				.find(event => event.turnID === "prepared")!.settlement.provider.toolExecution;
			const timeline = new TaskTimeline(0), laterTask = new TaskTimeline(execution.startedAt);
			for (const clock of [timeline, laterTask]) clock.recordTool(execution);
			expect(timeline.measure(execution.completedAt).authoritativeToolCount,
				JSON.stringify({ execution, metrics: fixture.backend.actorMetrics(), operations: events.filter(event => event.type === "operation_prediction") })).toBe(2);
			expect(laterTask.measure(execution.completedAt)).toMatchObject({ authoritativeToolCount: 1, hiddenLatencyMs: 0 });
			expect(events.filter(event => event.type === "operation_prediction")).toMatchObject([{ settlement: {
				prediction: { source: "pattern_aware", kind: "operation" }, observation: "observed", match: { matched: true, adoption: { status: "adopted" } },
			} }]);
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", command, changedParent]);
			expect(JSON.stringify(events.filter(event => event.type === "candidate" && event.candidate.kind === "operation"))).not.toContain("private value");
			await fixture.backend.storage.maintain("clear");
			expect(fixture.backend.executionBindings(later)).toEqual([]);
			await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
				try { await expect(session.executeBinding(binding!)).rejects.toThrow("unavailable in this scope"); }
				finally { await session.close(); }
			});
		} finally { release.resolve(); restoreJoin?.(); publishing.mockRestore(); await host?.dispose(); await fixture.dispose(); }
	});

	test("owns the entire Actor call when a held child crosses the adoption boundary", async ({ skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-held-transaction-"));
		const binary = path.join(root, "helper");
		execFileSync("cc", ["-pthread", "-O2", "-Wall", "-Wextra", "-Werror", fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), "-o", binary]);
		const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary });
		try {
			const permissions = new Error("socket permission failure"), retained = await filesystem.readdir(root);
			vi.mocked(filesystem.chmod).mockResolvedValueOnce().mockRejectedValueOnce(permissions);
			await expect(LinuxHeldExecBoundary.open({ storeRoot: root, binary })).rejects.toBe(permissions);
			expect(await filesystem.readdir(root)).toEqual(retained);
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
			const native = adaptProcessToolOperations(createLocalBashOperations({ shellPath: binary }));
			for (const [redirection, route] of [["", [1, 2]], ["2>&1", [1, 1]], ["3>&1", undefined], ["0<&-", undefined], ["1>/dev/null", undefined]] as const) {
				let inspected = 0;
				let inspection: ReturnType<typeof inspectHeldExecProcess> | undefined;
				const inspecting = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", decide: async ({ pid }) => {
					inspected++;
					inspection = inspectHeldExecProcess(pid, await filesystem.readlink(`/proc/${pid}/exe`));
					await inspection.catch(() => undefined);
					return { kind: "continue" };
				} });
				expect(await inspecting.execute({ command: `exec /bin/true ${redirection}`, cwd: root,
					environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: () => {} })).toEqual({ exitCode: 0 });
				expect(inspected).toBe(1);
				// Assert outside the advisory callback, whose failures intentionally preserve native execution.
				if (route) expect((await inspection!).outputRoute).toEqual(route);
				else await expect(inspection).rejects.toThrow(/descriptors/);
			}
			for (const killed of [false, true]) {
				const waiting = deferred(), nativeDone = deferred();
				let callbacks = 0, observed = 0, closed = 0, output = "", heldPid = 0;
				const concurrent = boundary.executor({ execute: request => native.execute(request).finally(nativeDone.resolve) }, {
					sourceRoot: root, realShell: "/bin/bash", decide: async process => {
						if (++callbacks === 1) { heldPid = process.pid; await waiting.promise; }
						return { kind: "continue", observeCompletion: async durationMs => {
							await nextTurn(); closed++; if (durationMs !== undefined) observed++;
						} };
					},
				});
				const siblings = concurrent.execute({
					command: `/bin/true & while [[ ! -e start-second-${killed} ]]; do :; done; /bin/echo sibling; wait`,
					cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: data => { output += data.toString(); },
				});
				try {
					await vi.waitFor(() => expect(callbacks).toBe(1));
					await writeFile(path.join(root, `start-second-${killed}`), "ready");
					await vi.waitFor(() => { expect(output).toBe("sibling\n"); expect(observed).toBe(1); });
					expect(callbacks).toBe(2);
					if (killed) { process.kill(heldPid, "SIGKILL"); await nativeDone.promise; }
					waiting.resolve(); expect(await siblings).toEqual({ exitCode: 0 }); expect(observed).toBe(killed ? 1 : 2);
					expect(closed, "native return must drain both successful and interrupted observations").toBe(2);
				} finally { waiting.resolve(); await Promise.allSettled([siblings]); }
			}
			const threaded = path.join(root, "thread-exec");
			await writeFile(`${threaded}.c`, `#include <pthread.h>
#include <unistd.h>
static void *replace(void *unused) {
	(void)unused;
	execl("/bin/true", "true", (char *)0);
	_exit(127);
}
int main(void) {
	pthread_t worker;
	if (pthread_create(&worker, 0, replace, 0)) return 70;
	pthread_exit(0);
}
`);
			execFileSync("cc", ["-pthread", "-Wall", "-Wextra", "-Werror", `${threaded}.c`, "-o", threaded]);
			for (const command of ["exec /bin/sh -c 'exec /bin/true'", `exec ${threaded}`]) {
				const visited: string[] = [], completed: string[] = [];
				const chained = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", decide: async ({ pid }) => {
					const image = `${pid}:${await filesystem.readlink(`/proc/${pid}/exe`)}`;
					visited.push(image);
					return { kind: "continue", observeCompletion: () => { completed.push(image); } };
				} });
				expect(await chained.execute({ command, cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: () => {} }))
					.toEqual({ exitCode: 0 });
				expect(visited).toHaveLength(2);
				expect(completed.sort(), "every exec image must retain its completion owner through replacement").toEqual(visited.sort());
			}
			let output = "";
			const committed = vi.fn(async () => { await writeFile(path.join(root, "producer-armed"), "ready"); });
			const delivery = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", decide: async process => {
				if (path.basename(await filesystem.readlink(`/proc/${process.pid}/exe`)) === "true")
					return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.alloc(2 * 1024 * 1024, 97) }], commit: committed };
				return { kind: "continue" };
			} });
			expect(await delivery.execute({
				command: "/bin/true | (while [[ ! -e producer-armed ]]; do :; done; /usr/bin/wc -c)",
				cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: data => { output += data.toString(); },
			})).toEqual({ exitCode: 0 });
			expect(output).toBe("2097152\n"); expect(committed).toHaveBeenCalledOnce();
			const actorContext = new AsyncLocalStorage<string>(), execIDs = new Set<string>();
			for (const disposition of [undefined, "recoverable", "poisoned", "killed"] as const) {
				const after = path.join(root, `after-${disposition}`);
				const scope = { sessionID: "session", turnID: "original" };
				const nativeDone = deferred();
				const adopted = vi.fn(() => { throw new Error("advisory feedback failed"); });
				let heldPid = 0;
				const commit = vi.fn(async () => {
					if (disposition === "killed") { process.kill(heldPid, "SIGKILL"); await nativeDone.promise; }
					else if (disposition) throw effectCommitFailure(new Error("injected commit failure"), disposition);
				});
				const decide = vi.fn(async (process: HeldExecProcess) => {
					expect(actorContext.getStore()).toBe("original");
					expect(process.id).toMatch(/^[a-f0-9]{48}:1$/); expect(process.sequence).toBe(1);
					execIDs.add(process.id);
					heldPid = process.pid; return { kind: "replay" as const, output: [], exitCode: 0, commit, adopted };
				});
				const executor = boundary.executor({ execute: request => native.execute(request).finally(nativeDone.resolve) }, {
					sourceRoot: root, realShell: "/bin/bash",
					decide,
				});
				const run = actorContext.run("original", () => executor.execute({ command: `/bin/true; printf continued > '${after}'`, cwd: root,
					environment: { PATH: "/usr/bin:/bin" }, onData: () => {}, timeout: 5, scope }));
				scope.turnID = "later";
				if (disposition) {
					await expect(run).rejects.toMatchObject({ disposition: "poisoned" });
					await expect(stat(after)).rejects.toThrow();
				} else {
					expect(await run).toEqual({ exitCode: 0 });
					expect(await readFile(after, "utf8")).toBe("continued");
				}
				expect(commit).toHaveBeenCalledOnce();
				expect(adopted).toHaveBeenCalledTimes(disposition ? 0 : 1);
				expect(decide).toHaveBeenCalledOnce();
				expect(decide.mock.calls[0]![0].scope).toEqual({ sessionID: "session", turnID: "original" });
				expect(Object.isFrozen(decide.mock.calls[0]![0].scope)).toBe(true);
			}
			expect(execIDs.size).toBe(4);
			let closed = false;
			const gate = gated();
			const executor = boundary.executor({ execute: async () => {
				await gate.wait(); return { exitCode: 0 };
			} }, { sourceRoot: root, realShell: "/bin/bash", decide: async () => ({ kind: "continue" }) });
			const running = executor.execute({ command: ":", cwd: root, environment: {}, onData: () => {} });
			await gate.entered;
			const closing = boundary.close().then(() => { closed = true; });
			try {
				await nextTurn();
				expect(closed, "close must wait for the owned executor and concurrent callers").toBe(false);
				const concurrent = boundary.close().then(() => { expect(closed).toBe(true); });
				gate.release();
				await Promise.all([closing, concurrent, running]);
			} finally { gate.release(); await Promise.allSettled([closing, running]); }
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
		let decide!: Parameters<LinuxHeldExecBoundary["executor"]>[1]["decide"];
		const { promise: pending, resolve: release } = deferred();
		const opening = vi.spyOn(LinuxHeldExecBoundary, "open").mockImplementation(async () => {
			await pending;
			return { shellPath: fixture.shellPath, executor: (_host: unknown, options: Parameters<LinuxHeldExecBoundary["executor"]>[1]) => {
				decide = options.decide; return held;
			}, close } as unknown as LinuxHeldExecBoundary;
		});
		const planner = vi.spyOn(fixture.backend.planner, "plan");
		const observed = vi.spyOn(SpeculationScheduler.prototype, "observeActorService");
		const admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin").mockReturnValue({
			allowed: false, reason: "fallback_faster", waitBudgetMs: 0,
			speculativeSamples: 1, actorSamples: 1, adoptionSamples: 1,
			expectedRemainingMs: 0, expectedAdoptionMs: 100,
			expectedActorMs: 10, expectedNetBenefitMs: -90,
		});
		const processInvocation = resolvePiToolInvocation("bash", { command: ":" }, {
			cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath,
		})!.process!;
		const invocation = vi.fn(() => processInvocation);
		const coordinator = new ProcessExecutionCoordinator(host, {
			enabled: () => true,
			prepare: (refresh) => fixture.backend.prepareActorReplay(host, {
				sourceRoot: fixture.workspace, invocation,
				held: { realShell: fixture.shellPath, executor: () => held },
			}, refresh),
			reset: () => fixture.backend.resetActorReplay(),
		});
		const invoke = () => coordinator.operations.exec(":", fixture.workspace, { env: fixture.environment, onData: () => {} });
		const gates = [deferred(), deferred()], sessionsReady = deferred(), captureGate = gated();
		const producers: Promise<unknown>[] = [];
		let sessions = 0;
		let calls: Promise<unknown> | undefined, refreshing: Promise<unknown> | undefined;
		try {
			await fixture.backend.observeBindings({ sessionID: "session", turnID: "turn" }, invoke,
				bindings => { expect(bindings).toEqual([]); }, false);
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
			expect(await fixture.backend.check()).toMatchObject({ state: "ready" }); await invoke();
			expect(host.execute, "backend preparation alone cannot produce a replay result").toHaveBeenCalledTimes(4);
			expect(held.execute).toHaveBeenCalledTimes(2);
			const availability = vi.spyOn(fixture.backend.store, "mayHaveCertificates").mockImplementationOnce(async () => {
				producers.push(...gates.map(gate => fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
					const session = await fixture.backend.open({ workspace, sourceRoot: fixture.workspace, invocation: processInvocation });
					try { if (++sessions === gates.length) sessionsReady.resolve(); await gate.promise; }
					finally { const first = session.close(), second = session.close(); await first; expect(second).toBe(first); }
				})));
				await Promise.race([sessionsReady.promise, Promise.all(producers)]);
				return false; // Actual sessions appeared while the empty history lookup was awaiting IO.
			});
			try { await invoke(); expect(held.execute).toHaveBeenCalledTimes(3); } finally { availability.mockRestore(); }
			const cancelled = new AbortController(); cancelled.abort(new Error("cancelled preparation"));
			await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				await expect(fixture.backend.open({ workspace, sourceRoot: fixture.workspace, invocation: processInvocation, signal: cancelled.signal }))
					.rejects.toThrow("cancelled preparation");
			});
			gates[0]!.resolve(); await producers[0]; await invoke();
			expect(held.execute, "one closed or failed producer cannot retire its live sibling").toHaveBeenCalledTimes(4);
			gates[1]!.resolve(); await producers[1]; await invoke(); expect(host.execute).toHaveBeenCalledTimes(5);

			const executable = path.join(fixture.workspace, "lookup-worker"), key = sha256Digest("different execution identity");
			const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
			const scan = vi.spyOn(await import("../src/linux-held-exec.ts"), "inspectHeldExecProcess").mockRejectedValue(new Error("process context required"));
			const image = vi.spyOn(filesystem, "realpath").mockResolvedValue(executable);
			const inspect = () => decide({ id: "lookup:1", sequence: 1, pid: process.pid, tracerPid: process.pid, sourceRoot: fixture.workspace });
			let work: ProcessHandoff | undefined;
			const history = vi.spyOn(fixture.backend.store, "mayHaveCertificates");
			try {
				await expect(inspect()).resolves.toEqual({ kind: "continue" }); expect(scan).not.toHaveBeenCalled();
				history.mockImplementationOnce(async () => {
					const result = await handoffs.acquire({ key, executablePath: executable, role: "producer",
						ownership: new ProcessHandoffOwnership(), lookup: async () => undefined });
					if (result.kind !== "work") throw new Error("expected concurrent producer registration");
					work = result.work; return false;
				});
				await expect(inspect()).resolves.toEqual({ kind: "continue" });
				expect(history).toHaveBeenLastCalledWith(executable);
				expect(scan).toHaveBeenCalledExactlyOnceWith(process.pid, executable);
				expect(fixture.backend.actorMetrics()).toMatchObject({ hits: 0, lastError: "actor_child:process context required" });
			} finally {
				if (work) handoffs.complete(key, work);
				history.mockRestore(); image.mockRestore(); scan.mockRestore();
			}

			const fork = fixture.workspaceSandbox.fork.bind(fixture.workspaceSandbox);
			const forking = vi.spyOn(fixture.workspaceSandbox, "fork").mockImplementation(options => fork({ ...options,
				afterCapture: async (workspace, capture) => {
					await captureGate.wait();
					return options.afterCapture!(workspace, capture); // The process is closed; final evidence can still be published.
				},
			}));
			try {
				const producing = forkReusableBash(fixture, { command: ":", label: "sealing", actionNamespace: "readiness", executionFingerprint: "readiness" })
					.then(async branch => { try { expect(branch.output.isError).toBe(false); } finally { await branch.dispose(); } });
				producers.push(producing);
				await Promise.race([captureGate.entered, producing]); await invoke();
				expect(held.execute, "outer evidence capture still owns possible publication").toHaveBeenCalledTimes(5);
				captureGate.release(); await producing;
			} finally { captureGate.release(); forking.mockRestore(); }
			await fixture.backend.store.clear(); await invoke(); expect(host.execute).toHaveBeenCalledTimes(6);
			opening.mockRejectedValueOnce(new Error("held-exec functional probe failed"));
			await coordinator.refreshActorRoute();
			expect(coordinator.actorDiagnostics()).toMatchObject({ state: "degraded", detail: expect.stringContaining("functional probe failed") });
			await invoke(); expect(host.execute).toHaveBeenCalledTimes(7);
		} finally {
			release(); gates.forEach(gate => gate.resolve()); captureGate.release();
			await Promise.allSettled([calls, refreshing, ...producers]);
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
			await writeFile(path.join(fixture.workspace, "fd-check.c"), "#include <fcntl.h>\nint main(void) {\n\tint mask = 0;\n\tfor (int fd = 0; fd < 3; fd++) if (fcntl(fd, F_GETFD) >= 0) mask |= 1 << fd;\n\treturn mask;\n}\n");
			await compileBenchmarkHelper(fixture.workspace, { source: "fd-check.c", output: "fd-check" });
			const streamProbes = ["", "0<&-", "1>&-", "2>&-", "0<&- 1>&- 2>&-", "3>&1"].map((redirection) =>
				"status=0; fd-check " + redirection + " || status=$?; printf 'fds:%s\\n' \"$status\"").join("; ");
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
				command: "set -e; /usr/bin/printf 'trace-root-fallback\\n'; mkdir barrier; barrier-worker barrier & first=$!; barrier-worker barrier & second=$!; wait \"$first\"; wait \"$second\"; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; printf '%s\\n' \"$line\"; }; " +
					"/usr/bin/printf 'file-fallback\\n' > redirected-file.txt; /usr/bin/cat < redirected-file.txt; printf 'pipe-fallback\\n' | /usr/bin/cat; " + streamProbes + "; printf '%32768s:end' ''",
				actionNamespace: "process-concurrency-test",
				executionFingerprint,
			});
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			const text = branch.output.result.content[0];
			expect(text?.type === "text" && text.text).toBe("trace-root-fallback\nredirected\nfile-fallback\npipe-fallback\nfds:7\nfds:6\nfds:5\nfds:3\nfds:0\nfds:7\n" + " ".repeat(32768) + ":end");
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
				const running = forkReusableBash(fixture, { ...args, label: failure, actionNamespace: "output-lifetime",
					executionFingerprint, signal: controller.signal });
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

	test.for(["trace", "transaction", "dependency", "host_parent", "publication"] as const)("preserves output and capture ownership when %s fails", { timeout: 15_000 }, async (failure, { skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-capture-failure-");
		const { readFile: readTrace, rm: removeFile, lstat: readStat } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const { spawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const entered = deferred(), failed = deferred(), gate = deferred();
		const error = new Error(failure === "dependency" ? "transaction baseline changed: input.txt"
			: failure === "host_parent" ? "mutable_input" : `injected ${failure} capture failure`);
		let traceRoot: string | undefined, released = false, cleanupBeforeRelease = false, returned = false, executions = 0;
		let processContext = {};
		let restoreTransactions: (() => void) | undefined;
		const open = fixture.backend.open.bind(fixture.backend);
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(async (input) => {
			processContext = { workspace: input.workspace.sandboxRoot, scope: input.scope };
			const begin = input.workspace.transactions.begin;
			const recording = vi.spyOn(input.workspace.transactions, "begin").mockImplementation(async () => {
				const capture = await begin();
				if (failure === "publication" || failure === "host_parent") return capture;
				return { abort: capture.abort, finish: async () => {
					if (failure === "dependency") {
						const result = await capture.finish(), next = await begin();
						await writeFile(path.join(input.workspace.sandboxRoot, "input.txt"), "changed-once");
						await next.finish(); failed.resolve(); await gate.promise; return result;
					}
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
				if (failure === "publication" || failure === "dependency" || failure === "host_parent") return readTrace(...args);
				if (failure === "trace") { await entered.promise; failed.resolve(); throw error; }
				entered.resolve(); await gate.promise;
			}
			return readTrace(...args);
		});
		const probing = vi.spyOn(filesystem, "lstat").mockImplementation(async (...args) => {
			const info = await readStat(...args);
			if (failure === "host_parent" && String(args[0]) === "/etc" && typeof info.mode === "bigint") {
				failed.resolve(); await gate.promise; info.mode |= 0o022n;
			}
			return info;
		});
		const removing = vi.spyOn(filesystem, "rm").mockImplementation((...args) => {
			if (String(args[0]) === traceRoot && !released) cleanupBeforeRelease = true;
			return removeFile(...args);
		});
		const spawning = vi.mocked(childProcess.spawn).mockImplementation((...args) => {
			if (JSON.stringify(args[1]).includes("/trace-")) executions++;
			return spawn(...args);
		});
		const planner = fixture.backend.planner;
		const publish = planner.publishCompleted.bind(planner);
		let publicationFailed = false;
		const publishing = vi.spyOn(planner, "publishCompleted").mockImplementation(async (...args) => {
			if (failure === "publication" && !publicationFailed) {
				publicationFailed = true; failed.resolve(); await gate.promise; throw error;
			}
			return publish(...args);
		});
		let running: ReturnType<typeof forkReusableBash> | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			await writeFile(path.join(fixture.workspace, "input.txt"), "capture-once");
			await writeFile(path.join(fixture.workspace, "emit.c"), '#include <fcntl.h>\n#include <unistd.h>\nint main(void) { char text[12]; return read(open("input.txt", O_RDONLY), text, 12) != 12 || write(1, text, 12) != 12; }\n');
			await compileBenchmarkHelper(fixture.workspace, { source: "emit.c", output: "emit" });
			await commitBenchmarkFixture(fixture.workspace, "Process capture failure");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			running = forkReusableBash(fixture, { command: "emit", label: failure,
				actionNamespace: "capture-owners", executionFingerprint });
			void running.then(() => { returned = true; }, () => { returned = true; });
			await Promise.race([failed.promise, running.then(() => { throw new Error(`failure injection was not reached: ${JSON.stringify(fixture.backend.metrics())}`); })]);
			await nextTurn();
			expect({ cleanupBeforeRelease, returned }, "a failed capture must drain its pending sibling").toEqual({ cleanupBeforeRelease: false, returned: false });
			released = true; gate.resolve();
			const branch = await running;
			await expect(stat(traceRoot!)).rejects.toMatchObject({ code: "ENOENT" });
			expect(branch.output.result.content).toEqual([{ type: "text", text: "capture-once" }]);
			expect({ executions, published: fixture.backend.metrics().published }).toEqual({ executions: 1, published: 0 });
			const lastError = branch.executionMetrics.reuse?.lastError ?? "";
			expect(lastError).toContain(error.message);
			const detail = failure === "host_parent" ? undefined : JSON.parse(lastError.split("; process=")[1]!);
			if (failure !== "host_parent") {
				expect(detail).toMatchObject({ ...processContext, requestID: 1,
					stage: failure === "publication" ? "history_publication" : failure === "dependency" ? "dependencies" : `${failure}_capture` });
				expect(detail.weakKey).toMatch(/^sha256:[a-f0-9]{64}$/);
			}
			const validation = await branch.validate?.();
			if (failure === "publication") {
				await expect(validateTransferredProcessEvidence(publishing.mock.calls[0]![0].dependencyCertificate)).resolves.toMatchObject({ status: "valid" });
				expect(lastError).toContain(`nested_publish:${error.message}`);
				expect(detail).toMatchObject({ certificateID: publishing.mock.calls[0]![0].id, complete: true, taints: ["clock", "random"] });
				// The parent's independent directory identity proof must still reject this private root.
				expect(validation).toMatchObject({ status: "stale", cause: { code: "process_dependency_changed", detail: fixture.workspace } });
				expect((await fixture.backend.store.stats()).certificates).toBe(0);
			} else {
				expect(validation?.status).toBe("indeterminate");
				expect(JSON.stringify(validation)).toContain(failure === "host_parent" ? "top_evidence:mutable:/etc/ld.so.cache" : `nested_capture:${error.message}`);
			}
		} finally {
			released = true; gate.resolve();
			await running?.then((branch) => branch.dispose(), () => undefined);
			restoreTransactions?.(); opening.mockRestore(); reading.mockRestore(); probing.mockRestore(); removing.mockRestore(); spawning.mockRestore(); publishing.mockRestore();
			await fixture.dispose();
		}
	});

	test.for([false, true, "close"] as const)("replenishes shared PATH alias probes while preserving mappings and owned cancellation (cancel=%s)", { timeout: 15_000 }, async (cancel, { skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-interposition-cancel-");
		const { realpath: resolvePath } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const controller = new AbortController(), entered = deferred(), gate = deferred();
		let activeRoot: string | undefined, held = false, returned = false, probesAfterAbort = 0;
		const probes = new Map<string, number>();
		let closing: (() => Promise<void>) | undefined, closed: Promise<void> | undefined, cancelled = false;
		const open = fixture.backend.open.bind(fixture.backend);
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(async (input) => {
			activeRoot = input.workspace.sandboxRoot;
			const session = await open(input); closing = session.close; return session;
		});
		const resolving = vi.spyOn(filesystem, "realpath").mockImplementation((...args) => {
			const target = String(args[0]);
			if (activeRoot && path.dirname(target) === path.join(activeRoot, "bin") && path.basename(target).startsWith("probe-")) {
				probes.set(path.basename(target), (probes.get(path.basename(target)) ?? 0) + 1);
				if (cancelled) probesAfterAbort++;
				if (!held) { held = true; entered.resolve(); return gate.promise.then(() => resolvePath(...args)); }
			}
			return resolvePath(...args);
		});
		const listening = vi.spyOn(net.Server.prototype, "listen");
		let running: ReturnType<typeof forkReusableBash> | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			await mkdir(path.join(fixture.workspace, "bin"));
			for (let index = 0; index < 33; index++) {
				const target = path.join(fixture.workspace, "bin", `probe-${index}`);
				await writeFile(target, "#!/bin/sh\nexit 0\n"); await chmod(target, 0o755);
			}
			await filesystem.symlink("bin", path.join(fixture.workspace, "alias"));
			const environment = { ...fixture.environment, PATH: `${fixture.workspace}/bin:${fixture.workspace}/alias:${fixture.environment.PATH}` };
			const tool = createBashTool(fixture.workspace, { shellPath: fixture.shellPath, operations: fixture.coordinator.operations,
				exposeSessionEnvironment: false, spawnHook: (context) => ({ ...context, env: environment }) });
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			const args = { command: ":" };
			const expected = await tool.execute("oracle", args);
			running = forkReusableBash({ ...fixture, tool, environment }, { ...args, label: "cancel-interposition",
				actionNamespace: "cancel-interposition", executionFingerprint, signal: controller.signal });
			void running.then(() => { returned = true; }, () => { returned = true; });
			await Promise.race([entered.promise, running]);
			if (cancel) { cancelled = true; if (cancel === "close") closed = closing!(); else controller.abort(); }
			await nextTurn();
			if (!cancel) await expect.poll(() => probes.size, { timeout: 1000 }).toBe(33);
			expect({ returned, owned: existsSync(activeRoot!) }).toEqual({ returned: false, owned: true });
			gate.resolve();
			if (cancel) await expect(running).rejects.toThrow();
			else {
				expect((await running).output).toEqual({ result: expected, isError: false });
				expect(probes.size).toBe(33); expect([...probes.values()]).toEqual(Array(33).fill(1));
				const mounts = vi.mocked(childProcess.spawn).mock.calls.flatMap(([, values]) => Array.isArray(values)
					? values.filter((value, index) => values[index - 1] === "--exec-mount" && value.includes("/probe-"))
						.map((value) => value.split(":")[0]) : []);
				expect(mounts.sort()).toEqual([`${fixture.workspace}/bin`, `${fixture.workspace}/alias`]
					.flatMap((directory) => Array.from({ length: 33 }, (_, index) => `${directory}/probe-${index}`)).sort());
			}
			const brokerStarted = listening.mock.calls.some(([address]) => typeof address === "string" && path.basename(address).startsWith("broker-"));
			const afterAbort = { probesAfterAbort, brokerStarted };
			expect(afterAbort, `preparation ownership: ${JSON.stringify(afterAbort)}`).toEqual({ probesAfterAbort: 0, brokerStarted: !cancel });
			await expect(stat(activeRoot!)).rejects.toMatchObject({ code: "ENOENT" });
		} finally {
			gate.resolve(); await running?.then((branch) => branch.dispose(), () => undefined); await closed;
			opening.mockRestore(); resolving.mockRestore(); listening.mockRestore(); await fixture.dispose();
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
			executeBinding: async () => { throw new Error("unexpected process binding"); },
			executionBindings: () => [],
			computationDependencies: () => [],
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
				const gate = gated();
				const clock = vi.spyOn(performance, "now").mockReturnValue(0);
				const execute = vi.spyOn(world.speculation, "execute").mockResolvedValue(branch);
				const dispose = vi.spyOn(branch, "dispose").mockImplementation(gate.wait);
				let delivered = false;
				const measured = executeReusableBash({ backend, world, workspace: root, environment: {}, shellPath: invocation.process!.shell,
					tool: createBashTool(root) }, { label: "cleanup", command: "opaque", actionNamespace: "", executionFingerprint: "fake-process" })
					.then(result => { delivered = true; return result; });
				try {
					await Promise.race([gate.entered, measured]); await nextTurn(); expect(delivered).toBe(false);
					clock.mockReturnValue(100); gate.release();
					expect((await measured).measurement.totalMs).toBe(100);
				} finally {
					gate.release(); await measured.catch(() => undefined); clock.mockRestore(); execute.mockRestore(); dispose.mockRestore();
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

	test.for(["rename", "posix-lock", "ofd-lock", "flock", "rdtsc", "auxv-random"] as const)("rejects reuse when isolated resource semantics differ from native (%s)", { timeout: 20_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const instanceInput = mode === "rdtsc" || mode === "auxv-random";
		if (instanceInput && process.arch !== "x64") return skip("x86-64 ELF input probe");
		if (mode === "rename") {
			const overlay = await linuxOverlayfsCapability();
			if (!overlay.available) return skip(overlay.detail);
		}
		const fixture = await createLinuxProcessBenchmark("pi-process-driver-semantics-", mode === "rename" ? "overlayfs" : undefined);
		const { workspace, backend } = fixture;
		let branch: Awaited<ReturnType<typeof forkReusableBash>> | undefined;
		let locker: childProcess.ChildProcessWithoutNullStreams | undefined, lockerClosed: Promise<unknown> | undefined;
		try {
			await mkdir(path.join(workspace, "source"));
			await writeFile(path.join(workspace, "source", "value.txt"), "value\n", "utf8");
			if (mode !== "rename") {
				await writeFile(path.join(workspace, "probe.c"), instanceInput ? `#include <elf.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
int main(int argc, char **argv, char **envp) {
	(void)argc; (void)argv; (void)envp; unsigned long long value;
	${mode === "rdtsc" ? 'unsigned lo, hi; __asm__ volatile ("rdtsc" : "=a"(lo), "=d"(hi)); value = ((unsigned long long)hi << 32) | lo;' : `
	while (*envp) envp++;
	Elf64_auxv_t *aux = (Elf64_auxv_t *)(envp + 1); const unsigned char *bytes = 0;
	for (; aux->a_type != AT_NULL; aux++) if (aux->a_type == AT_RANDOM) bytes = (const unsigned char *)(uintptr_t)aux->a_un.a_val;
	if (!bytes) return 2;
	value = 14695981039346656037ull;
	for (unsigned i = 0; i < 16; i++) value = (value ^ bytes[i]) * 1099511628211ull;`}
	char output[80]; int size = snprintf(output, sizeof(output), "%llu\\n", value);
	return write(1, output, (size_t)size) != size;
}
` : `#define _GNU_SOURCE
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
int main(int argc, char **argv) {
	(void)argv; int fd = open("source/value.txt", O_RDWR); if (fd < 0) return 2;
	struct flock lock = {.l_type=F_WRLCK, .l_whence=SEEK_SET}; char value;
	if (argc > 1) {
		if (${mode === "flock" ? "flock(fd, LOCK_EX)" : "fcntl(fd, F_SETLK, &lock)"} < 0) return 3;
		if (write(1, "R", 1) != 1) return 4;
		return read(0, &value, 1) < 0;
	}
	${mode === "flock" ? "(void)lock; value = flock(fd, LOCK_EX|LOCK_NB) == 0 ? 'U' : 'L';" :
		`if (fcntl(fd, ${mode === "ofd-lock" ? "F_OFD_GETLK" : "F_GETLK"}, &lock) < 0) return 5; value = lock.l_type == F_UNLCK ? 'U' : 'L';`}
	return write(1, &value, 1) != 1;
}
`);
				await compileBenchmarkHelper(workspace, { source: "probe.c", output: "probe" });
				await commitBenchmarkFixture(workspace, "Resource control observation");
				if (!instanceInput) {
					locker = childProcess.spawn(path.join(workspace, "probe"), ["hold"], { cwd: workspace, env: fixture.environment, stdio: ["pipe", "pipe", "pipe"] });
					lockerClosed = once(locker, "close");
					expect((await Promise.race([once(locker.stdout, "data"), lockerClosed.then(() => { throw new Error("lock holder exited"); })]))[0].toString()).toBe("R");
					expect(execFileSync(path.join(workspace, "probe"), [], { cwd: workspace, env: fixture.environment, encoding: "utf8" })).toBe("L");
				}
			}
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { includeWorkspaceFingerprint: true });
			branch = await forkReusableBash(fixture, { command: mode === "rename" ? "mv source moved" : "probe", label: "driver-semantics-test",
				actionNamespace: "driver-semantics-test", executionFingerprint });
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			if (mode !== "rename") {
				expect(branch.output.result.content).toEqual([{ type: "text", text: instanceInput ? expect.stringMatching(/^\d+\n$/) : "U" }]);
				const route = await backend.prepareActorReplay(adaptProcessToolOperations(createLocalBashOperations()), {
					sourceRoot: workspace, invocation: () => undefined, held: { realShell: fixture.shellPath,
						executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) },
				}, true);
				if (!("executor" in route)) throw new Error(route.detail);
				let output = "";
				await route.executor.execute({ command: ": changed-parent; probe", cwd: workspace, environment: fixture.environment,
					scope: { sessionID: "benchmark", turnID: "later" }, onData: data => { output += data.toString(); } });
				expect(output, JSON.stringify(backend.actorMetrics())).toEqual(instanceInput ? expect.stringMatching(/^\d+\n$/) : "L");
				expect(backend.actorMetrics().hits).toBe(0);
			}
			if (!instanceInput) {
				const validation = await branch.validate?.();
				expect(validation?.status).toBe("indeterminate");
				expect(JSON.stringify(validation)).toContain(mode === "rename" ? "filesystem_semantics" : "ipc");
			}
			expect(branch.executionMetrics.reuse?.requests).toBeGreaterThan(0);
			expect(branch.executionMetrics.reuse?.executionMs).toBeGreaterThan(0);
			expect(backend.metrics().tainted).toBeGreaterThan(0);
			expect(backend.metrics().published).toBe(0);
			expect((await backend.store.stats()).certificates).toBe(0);
			expect((await stat(path.join(workspace, "source"))).isDirectory()).toBe(true);
			await expect(stat(path.join(workspace, "moved"))).rejects.toThrow();
		} finally {
			locker?.stdin.end(); await lockerClosed;
			await branch?.dispose();
			await fixture.dispose();
		}
	});
});
