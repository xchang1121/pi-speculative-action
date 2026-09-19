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
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff, type ProcessExecutionBinding } from "../src/process-handoff.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { sha256Digest } from "../src/provenance-certificate.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { emptyWorldReuseMetrics, type ExecutionOperationBinding } from "../src/execution-world.ts";
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
	forkReusableBash,
	prepareLinuxProcessReuse,
	holdProcessPublication,
} from "./linux-process-fixture.ts";

vi.mock("node:child_process", { spy: true });
vi.mock("node:fs/promises", { spy: true });

describe("Linux process ExecutionWorld", () => {
	test.for(["git", "auto"] as const)("selects changed running inputs after unrelated hints and compares the predecessor state (%s)", { timeout: 20_000 }, async (driver, { skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-running-inputs-", driver);
		let pending: ReturnType<typeof forkReusableBash> | undefined, sandboxRoot: string | undefined;
		let check: (() => Promise<boolean>) | undefined;
		const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
		const observe = registry.observeInputs.bind(registry), open = fixture.backend.open.bind(fixture.backend);
		const observing = vi.spyOn(registry, "observeInputs").mockImplementation((key, owner, changed) => {
			check = changed; return observe(key, owner, changed);
		});
		const hints = ["ignored.txt", "stable.txt", "input.txt"].map(name => path.join(fixture.workspace, name));
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(input => {
			sandboxRoot = input.workspace.sandboxRoot;
			return open({ ...input, workspace: { ...input.workspace, sourceChanges: () => ({ uncertain: false, paths: hints }) } });
		});
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			for (const file of hints) await writeFile(file, "baseline");
			await writeFile(path.join(fixture.workspace, "worker.c"), `#include <fcntl.h>
#include <unistd.h>
int main(void) {
	char value[32];
	int stable = open("stable.txt", O_RDONLY);
	if (stable < 0 || read(stable, value, sizeof(value)) <= 0) return 1;
	close(stable);
	int input = open("input.txt", O_RDONLY);
	ssize_t length = input < 0 ? -1 : read(input, value, sizeof(value));
	if (length <= 0) return 2;
	close(input);
	int ready = open("ready", O_WRONLY | O_CREAT, 0600);
	if (ready < 0) return 3;
	close(ready);
	while (access("release", F_OK) != 0) usleep(1000);
	return write(1, value, (size_t)length) == length ? 0 : 4;
}
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			await commitBenchmarkFixture(fixture.workspace, "Running predecessor inputs");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			pending = forkReusableBash(fixture, { label: "inputs", command: "printf predecessor > input.txt; worker",
				actionNamespace: "running-inputs", executionFingerprint });
			void pending.catch(() => {});
			await expect.poll(() => Boolean(sandboxRoot && existsSync(path.join(sandboxRoot, "ready"))), { timeout: 5000 }).toBe(true);
			expect(check).toBeDefined();
			await writeFile(hints[0]!, "unrelated"); await writeFile(hints[2]!, "predecessor");
			const opened = vi.spyOn(filesystem, "open"), reads = opened.mock.calls.length;
			try {
				expect(await check!()).toBe(false); // The child's transaction starts after the shell's write.
				await writeFile(hints[2]!, "after");
				expect(await check!()).toBe(true);
				expect(opened.mock.calls.slice(reads).filter(([file]) => String(file) === hints[0])).toHaveLength(0);
				expect(fixture.backend.actorMetrics().lastError).toContain("actor_running_input_changed:");
			} finally { opened.mockRestore(); }
			await writeFile(path.join(sandboxRoot!, "release"), "");
			expect((await pending).output.result.content).toEqual([{ type: "text", text: "predecessor" }]);
		} finally {
			if (sandboxRoot) await writeFile(path.join(sandboxRoot, "release"), "").catch(() => {});
			await (await pending?.catch(() => undefined))?.dispose();
			opening.mockRestore(); observing.mockRestore(); await fixture.dispose();
		}
	});

	test.for(["completed", "running", "native", "native-merged", "native-closed-input", "native-descriptors", "native-null", "native-null-stdin", "native-status", "native-directory", "native-directory-prepared-stale", "native-directory-opath", "native-directory-opath-prepared-stale", "native-pipe", "native-pipe-prepared-stale", "native-pipe-live", "native-pipe-live-prepared-stale", "native-pipe-live-mixed", "native-pipe-live-transfer", "native-pipe-producer", "native-socket", "native-socket-running", "native-socket-duplex", "native-socket-concurrent", "native-socket-half-closed", "native-socket-prepared-stale", "native-shared-table", "native-unshare", "native-prepared-stale", "native-prepared-restored"] as const)("reexecutes an owned child binding across turns without replaying its parent or stale input (%s)", { timeout: 20_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-binding-");
		const publishing = vi.spyOn(fixture.backend.planner, "publishCompleted");
		const errors = vi.spyOn(fixture.backend as unknown as { setError(session: unknown, message: string): void }, "setError");
		const native = mode.startsWith("native"), running = mode.endsWith("running");
		const mixed = mode.endsWith("mixed"), socket = mode.includes("socket"), duplex = mode.endsWith("duplex") || mode.endsWith("concurrent"), concurrent = mode.endsWith("concurrent"), pipe = mode.includes("pipe"), opath = mode.includes("opath") || mixed, launcher = pipe || socket || mode === "native-shared-table" || mode === "native-unshare" || opath;
		const nullDevice = mixed || mode === "native-null" || mode === "native-null-stdin" || mode === "native-status";
		const directory = mode.includes("directory") || mixed;
		const descriptors = mode === "native-descriptors" || launcher || nullDevice || directory;
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		let publication: ReturnType<typeof holdProcessPublication> | undefined;
		let restorePreparation: (() => void) | undefined;
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") return skip(status.detail);
			await writeFile(path.join(fixture.workspace, "input.txt"), "before\n");
			if (descriptors) await writeFile(path.join(fixture.workspace, "fd.txt"), "abcdef");
			await writeFile(path.join(fixture.workspace, "worker.c"), `#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc != 2 || strcmp(argv[0], "bound-name") || strcmp(argv[1], "private argument") ||
		!getenv("BOUND_SECRET") || strcmp(getenv("BOUND_SECRET"), "private value")) return 71;
	for (volatile unsigned long iteration = 0; iteration < ${running ? 500000000 : native ? 50000000 : 0}ul; ++iteration) {}
	${mode === "native-closed-input" ? 'char probe; if (read(0, &probe, 1) != -1 || errno != EBADF) return 72;' : ""}
	${socket ? 'char preview; if (recv(3, &preview, 1, MSG_PEEK) != 1 || preview != \'b\') return 106;' : ""}
	${descriptors ? 'char a, b, c; if (read(3, &a, 1) != 1 || read(4, &b, 1) != 1 || read(8, &c, 1) != 1 || a != \'b\' || b != \'c\' || c != \'a\') return 72;' : ""}
	${descriptors ? 'int alias = dup(4); if (alias < 0 || fcntl(alias, F_GETFL) != fcntl(3, F_GETFL) || (fcntl(8, F_GETFL) & O_ACCMODE) != ' + (socket ? 'O_RDWR' : 'O_RDONLY') + ') return 74; close(alias);' : ""}
	${socket ? (duplex ? 'char peer[5]; if (read(14, peer, 4) != 4 || memcmp(peer, "peer", 4)) return 100; ' + (concurrent ? 'pid_t child = fork(); if (child < 0) return 103; if (!child) { size_t length = 0; while (length < 5) { ssize_t size = read(14, peer + length, 5 - length); if (size <= 0) return 104; length += (size_t)size; } return memcmp(peer, "reply", 5) || write(14, "back", 4) != 4 || shutdown(14, SHUT_WR); } ' : '') : '') + 'if (write(3, "re", 2) != 2 || write(4, "ply", 3) != 3 || shutdown(4, SHUT_WR)) return 94;' + (duplex ? concurrent ? ' int status; if (waitpid(child, &status, 0) != child || status) return 105;' : ' if (read(14, peer, 5) != 5 || memcmp(peer, "reply", 5) || write(14, "back", 4) != 4 || shutdown(14, SHUT_WR)) return 101;' : '') : mode === "native-pipe-producer" ? 'if (write(14, "re", 2) != 2 || write(14, "ply", 3) != 3) return 94;' : ""}
	${nullDevice ? 'char byte; if (write(6, "discard", 7) != 7 || read(7, &byte, 1) != 0 || lseek(6, 100, SEEK_SET) != 0 || fcntl(6, F_GETFL) != fcntl(7, F_GETFL)) return 75;' : ""}
	${mode === "native-status" || mixed ? 'if (fcntl(3, F_SETFL, fcntl(3, F_GETFL) | O_NONBLOCK) || fcntl(6, F_SETFL, fcntl(6, F_GETFL) | O_APPEND | O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK)) return 77;' : ""}
	${mode === "native-null-stdin" ? 'if (read(0, &byte, 1) != 0) return 76;' : ""}
	${directory ? 'if (fchdir(10) || fcntl(10, F_GETFL) != fcntl(11, F_GETFL)) return 78;' : ""}
	${mixed ? 'if (pwrite(16, "Q", 1, 2) != 1 || lseek(17, 4, SEEK_SET) != 4 || lseek(18, 0, SEEK_CUR) != 0) return 86;' : ""}
	${opath ? 'char probe; if (fcntl(12, F_GETFL) != O_PATH || read(12, &probe, 1) != -1 || errno != EBADF) return 79;' : ""}
	char text[32]; int fd = ${directory ? 'openat(11, "input.txt", O_RDONLY)' : 'open("input.txt", O_RDONLY)'}; ssize_t size = read(fd, text, sizeof(text));
	${mode === "native-closed-input" ? 'if (fd != 0) return 73;' : ""}
	if (size <= 0 || write(1, text, (size_t)size) != size) return 1;
	return ${mode === "native-merged" ? 'write(2, "stderr\\n", 7) != 7' : "0"};
}
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			if (launcher) {
				await writeFile(path.join(fixture.workspace, "fd-launch.c"), `#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
static void *duplicate(void *unused) { (void)unused; if (dup2(3, 9) < 0) _exit(73); return 0; }
int main(int argc, char **argv) {
	if (argc != 2) return 74;
	pthread_t worker; if (pthread_create(&worker, 0, duplicate, 0) || pthread_join(worker, 0)) return 75;
	${mode === "native-unshare" ? "if (unshare(CLONE_FILES)) return 77;" : ""}
	${opath ? 'int directory = open(".", O_PATH | O_DIRECTORY), file = open("fd.txt", O_PATH);\n\tif (directory < 0 || file < 0 || dup2(directory, 10) < 0 || dup2(directory, 11) < 0 || dup2(file, 12) < 0) return 79;\n\tclose(directory); if (file != 12) close(file);' : ""}
	${pipe ? 'int fds[2]; if (pipe2(fds, O_CLOEXEC) || write(fds[1], "bcaXYZ", 6) != 6) return 80; ' + (mode.includes("pipe-live") ? '' : 'close(fds[1]); ') + 'if (dup2(fds[0], 0) < 0) return 81; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0) return 82; int reopened = open("/proc/self/fd/0", O_RDONLY); if (reopened < 0 || dup2(reopened, 8) < 0) return 83; if (reopened != 8) close(reopened);' : ""}
	${mixed ? 'int writable = open("anonymous", O_RDWR | O_CREAT | O_EXCL, 0600); if (writable < 0 || unlink("anonymous") || write(writable, "abcdef", 6) != 6 || lseek(writable, 0, SEEK_SET) != 0 || dup2(writable, 16) < 0 || dup2(writable, 17) < 0) return 87; close(writable); int reader = open("/proc/self/fd/16", O_RDONLY); if (reader < 0 || dup2(reader, 18) < 0) return 88; close(reader);' : ""}
	${socket ? 'int fds[2]; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) || write(fds[1], "bcaXYZ", 6) != 6 || dup2(fds[0], 0) < 0) return 95; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0 || dup2(0, 8) < 0) return 96;' + (mode === "native-socket-half-closed" ? ' if (shutdown(fds[1], SHUT_WR)) return 97;' : '') : ""}
	${duplex ? 'if (dup2(fds[1], 14) < 0 || write(0, "peer", 4) != 4) return 100;' : ""}
	${mode === "native-pipe-producer" ? 'int output[2]; if (pipe2(output, O_CLOEXEC) || dup2(output[1], 14) < 0 || dup3(output[0], 15, O_CLOEXEC) < 0) return 95; close(output[0]); close(output[1]);' : ""}
	puts(argv[1]); fflush(stdout);
	${mode === "native-pipe-live-transfer" ? `int sockets[2], passed[] = {0, 3, 4, 8}; char byte = 'x', control[CMSG_SPACE(sizeof(passed))] = {0};
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets)) return 89;
	struct iovec vector = {&byte, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed));
	memcpy(CMSG_DATA(header), passed, sizeof(passed)); if (sendmsg(sockets[0], &message, 0) != 1) return 90; close(sockets[0]);` : ""}
	${pipe || socket ? 'pid_t child = fork(); if (child < 0) return 84; if (child) { int status; char tail[3]; if (waitpid(child, &status, 0) != child || status || ' + (mixed ? 'lseek(16, 0, SEEK_CUR) != 4 || lseek(18, 0, SEEK_CUR) != 0 || pread(18, tail, 1, 2) != 1 || tail[0] != 81 || !(fcntl(3, F_GETFL) & O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK) || ' : '') + 'read(3, tail, 1) != 1 || read(4, tail + 1, 1) != 1 || read(8, tail + 2, 1) != 1 || tail[0] != \'X\' || tail[1] != \'Y\' || tail[2] != \'Z\') return 85; ' + (socket || mode === "native-pipe-producer" ? 'char reply[6]; ' + (duplex ? 'if (read(0, reply, 4) != 4 || memcmp(reply, "back", 4) || read(0, reply, 1) != 0) return 102; ' : 'if (read(' + (socket ? 'fds[1]' : '15') + ', reply, 5) != 5 || memcmp(reply, "reply", 5)) return 98; ') + (socket ? 'if (read(fds[1], reply, 1) != 0) return 99; ' : '') : '') + 'return 0; }' : ""}
	${mode === "native-pipe-live-transfer" ? `for (unsigned index = 0; index < 4; index++) close(passed[index]);
	if (recvmsg(sockets[1], &message, MSG_CMSG_CLOEXEC) != 1) return 91;
	int received[4], saved[4]; memcpy(received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(received));
	for (unsigned index = 0; index < 4; index++) { saved[index] = fcntl(received[index], F_DUPFD_CLOEXEC, 100); if (saved[index] < 0) return 92; close(received[index]); }
	for (unsigned index = 0; index < 4; index++) { if (dup2(saved[index], passed[index]) < 0) return 93; close(saved[index]); }
	close(sockets[1]);` : ""}
	char *command[] = {"bound-name", "private argument", 0}; execv("./worker", command); return 76;
}
`);
				execFileSync("cc", ["-pthread", "-O2", "-Wall", "-Wextra", "-Werror", "fd-launch.c", "-o", "fd-launch"], { cwd: fixture.workspace });
			}
			await commitBenchmarkFixture(fixture.workspace, "Bound process invocation");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const command = (descriptors ? "exec 3<fd.txt; exec 4<&3; exec 8<fd.txt; IFS= read -r -N 1 discarded <&3; " : "") +
				(directory ? "exec 10<.; exec 11<&10; " : "") +
				(nullDevice ? (mode === "native-null-stdin" ? "exec 0<>/dev/null; exec 6<&0; " : "exec 6<>/dev/null; ") + "exec 7<&6; " : "") +
				"export BOUND_SECRET='private value'; " + (launcher ? "exec fd-launch parent" : "printf 'parent\\n'; exec -a bound-name worker 'private argument'") +
				(mode === "native-merged" ? " 2>&1" : mode === "native-closed-input" ? " 0<&-" : "");
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
			expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics() })).toBe(native ? 0 : 1);
			binding ??= fixture.backend.executionBindings(later).at(-1);
			expect(binding, "a real native miss must retain its launch without publishing a result").toBeDefined();
			let retainedOperation: ExecutionOperationBinding | undefined;
			if (mode === "native") for (const turnID of ["repeated-native-1", "repeated-native-2"]) {
				const repeatedScope = { ...scope, turnID }; let output = "";
				const previousMs = binding!.executionMs;
				const action = PI_ACTION_SEMANTICS.buildKey("bash", { command }, fixture.workspace, "binding")!;
				await fixture.world.observeOperations!({ action, scope: repeatedScope, learn: true }, () => route.executor.execute({
					command: command.replace("parent", turnID), cwd: fixture.workspace, environment: fixture.environment,
					scope: repeatedScope, onData: data => { output += data.toString(); },
				}), bindings => { retainedOperation ??= bindings.find(item => item.identity === binding!.key); });
				expect(binding!.executionMs).not.toBe(previousMs);
				expect(retainedOperation).toMatchObject({ executionMs: binding!.executionMs, expectedDurationMs: binding!.executionMs });
				expect(output).toBe(`${turnID}\nafter\n`);
				expect(fixture.backend.executionBindings(later).filter(item => item.key === binding!.key)).toEqual([binding]);
			}
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", command]);
			await writeFile(path.join(fixture.workspace, "input.txt"), "newest\n");
			const before = fixture.backend.metrics();
			const publicationStart = publishing.mock.calls.length;
			if (running) publication = holdProcessPublication(fixture.backend);
			if (mode.includes("prepared-")) {
				const fork = fixture.workspaceSandbox.fork.bind(fixture.workspaceSandbox);
				const borrowing = vi.spyOn(fixture.workspaceSandbox, "fork").mockImplementation(async options => {
					if (options.preparation) await writeFile(path.join(fixture.workspace, "input.txt"), "changed after preparation\n");
					return fork(options);
				});
				restorePreparation = () => borrowing.mockRestore();
			}
			await start("prepared");
			if (running) await expect.poll(publication!.reached, { timeout: 5000 }).toBe(true);
			else await expect.poll(() => events.filter(event => event.turnID === "prepared" && (event.type === "candidate" || event.type === "operation_prediction"))
				.map(event => event.type === "candidate" ? [event.candidate.kind, event.state.status] : event.type === "operation_prediction" ? event.settlement : undefined), { timeout: 5000 }).toContainEqual(["operation", "succeeded"]);
			if (launcher && !running) await expect.poll(() => publishing.mock.calls.slice(publicationStart).some(([certificate]) =>
				certificate.prototype.executablePath === path.join(fixture.workspace, "worker")), { timeout: 5000 }).toBe(true).catch(error => {
				throw new Error(JSON.stringify({ metrics: fixture.backend.metrics(), actor: fixture.backend.actorMetrics(), errors: errors.mock.calls.map(call => call[1]) }), { cause: error });
			});
			if (mode.includes("prepared-")) {
				expect(await readFile(path.join(fixture.workspace, "input.txt"), "utf8")).toBe("changed after preparation\n");
				restorePreparation?.(); restorePreparation = undefined;
				if (mode === "native-prepared-restored") await writeFile(path.join(fixture.workspace, "input.txt"), "newest\n");
			}
			expect(fixture.backend.metrics().misses).toBeGreaterThan(before.misses);
			const changedParent = command.replace("parent", "automatic-parent");
			if (running) {
				let output = "";
				await route.executor.execute({ command: changedParent, cwd: fixture.workspace, environment: fixture.environment,
					scope: { ...scope, turnID: "foreign" }, onData: bytes => { output += bytes.toString(); } });
				expect(output).toBe("automatic-parent\nnewest\n");
				expect(publication!.evidence(), "another turn cannot wait for this one-shot producer").toBeUndefined();
			}
			const actor = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { ...scope, turnID: "prepared" } }) }, () => fixture.tool.execute("prepared", { command: changedParent })));
			const nativeExecution = host.execute(call("prepared", changedParent), undefined, actor);
			void nativeExecution.catch(() => undefined);
			if (running) {
				await expect.poll(() => publication!.evidence()?.decision.allowed).toBe(true);
			}
			const stalePreparation = mode.endsWith("prepared-stale");
			expect((await nativeExecution).content).toEqual([{ type: "text", text: `automatic-parent\n${stalePreparation ? "changed after preparation\n" : "newest\n"}${suffix}` }]);
			expect(actor).toHaveBeenCalledOnce();
			expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify({ joinEvidence: publication?.evidence(), metrics: fixture.backend.actorMetrics() })).toBe(Number(running));
			await host.finishTurn("prepared");
			const execution = events.filter(event => event.type === "actor_action")
				.find(event => event.turnID === "prepared")!.settlement.provider.toolExecution;
			const timeline = new TaskTimeline(0), laterTask = new TaskTimeline(execution.startedAt);
			for (const clock of [timeline, laterTask]) clock.recordTool(execution);
			expect(timeline.measure(execution.completedAt).authoritativeToolCount,
				JSON.stringify({ execution, metrics: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(), bindings: fixture.backend.executionBindings(later), operations: events.filter(event => event.type === "operation_prediction") })).toBe(stalePreparation ? 1 : 2);
			expect(laterTask.measure(execution.completedAt)).toMatchObject({ authoritativeToolCount: 1, hiddenLatencyMs: 0 });
			expect(events.filter(event => event.type === "operation_prediction").filter(event => !launcher || stalePreparation || event.settlement.observation === "observed")).toMatchObject(Array.from({ length: launcher && stalePreparation ? 2 : 1 }, () => ({ settlement: stalePreparation ? { observation: "unobserved" } : {
				prediction: { source: "pattern_aware", kind: "operation" }, observation: "observed", match: { matched: true, adoption: { status: "adopted" } },
			} })));
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", command, changedParent]);
			expect(JSON.stringify(events.filter(event => event.type === "candidate" && event.candidate.kind === "operation"))).not.toContain("private value");
			await fixture.backend.storage.maintain("clear");
			expect(fixture.backend.executionBindings(later)).toEqual([]);
			await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
				try { await expect(session.executeBinding(binding!)).rejects.toThrow("unavailable in this scope"); }
				finally { await session.close(); }
			});
		} finally { publication?.close(); restorePreparation?.(); publishing.mockRestore(); errors.mockRestore(); await host?.dispose(); await fixture.dispose(); }
	});

	test.for(["read", "write", "path-write", "unlinked"] as const)("learns and adopts regular OFDs with shared positions and predecessor inputs (%s)", { timeout: 30_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-fd-binding-");
		const writable = mode === "write" || mode === "unlinked";
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") throw new Error(status.detail);
			const input = path.join(fixture.workspace, "input.txt");
			await writeFile(input, "abcdef");
			await writeFile(path.join(fixture.workspace, "worker.c"), `#include <unistd.h>
#include <fcntl.h>
int main(void) {
	char bytes[6] = {0, 0, 0, ':', 0, '\\n'};
	if (read(3, bytes, 2) != 2 || read(4, bytes + 2, 1) != 1 || read(8, bytes + 4, 1) != 1) return 71;
	${writable ? 'if (write(4, "XY", 2) != 2) return 72;' : mode === "path-write" ? 'int fd = open("input.txt", O_WRONLY); if (pwrite(fd, "Z", 1, 2) != 1) return 72; close(fd);' : ""}
	for (volatile unsigned long i = 0; i < 50000000ul; ++i) {}
	return write(1, bytes, sizeof(bytes)) != sizeof(bytes);
}
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			await commitBenchmarkFixture(fixture.workspace, "Inherited FD binding");
			await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "fd-binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const route = await fixture.backend.prepareActorReplay(adaptProcessToolOperations(createLocalBashOperations()), {
				sourceRoot: fixture.workspace, invocation: () => undefined, held: { realShell: fixture.shellPath,
					executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) },
			});
			if (!("executor" in route)) throw new Error(route.detail);
			const command = `exec 3<${writable ? ">" : ""}input.txt; exec 4<&3; exec 8<input.txt; IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; ` +
				(mode === "unlinked" ? "rm input.txt; " : "") +
				`printf 'parent\\n'; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"`;
			const execute = async (scope: { sessionID: string; turnID: string }, command: string) => {
				let output = "";
				const result = await route.executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment, scope,
					timeout: 10, onData: data => { output += data.toString(); } });
				expect(result).toEqual({ exitCode: 0 }); return output;
			};
			const tail = writable ? ":c" : mode === "path-write" ? "e:Z" : "e:c";
			expect(await fixture.backend.observeBindings(scope, () => execute(scope, command), () => {}, true)).toBe(`parent\nbcd:b\ntail:${tail}\n`);
			const binding = fixture.backend.executionBindings(later).at(-1);
			expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
			const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace,
				environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
			for (const changed of [false, true]) {
				await writeFile(input, "abcdef");
				await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
					const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
					const before = await readFile(path.join(workspace.sandboxRoot, "input.txt"));
					const beforeMode = (await stat(path.join(workspace.sandboxRoot, "input.txt"))).mode & 0o777;
					try {
						const result = await session.executeBinding(binding!);
						expect(result.exit).toEqual({ kind: "code", code: 0 });
						expect(result.output.map(({ data }) => data.toString()).join("")).toBe("bcd:b\n");
						const after = await readFile(path.join(workspace.sandboxRoot, "input.txt"));
						await session.seal(before.equals(after) ? [] : [{ root: fixture.workspace, target: input, resource: "input.txt",
							before, after, beforeMode, afterMode: beforeMode }]);
						expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: "valid" });
						const hits = fixture.backend.actorMetrics().hits;
						if (changed) await writeFile(input, "uvwxyz");
						expect(await execute(later, command.replace("parent", "changed-parent"))).toBe(changed
							? `changed-parent\nvwx:v\ntail:${writable ? ":w" : mode === "path-write" ? "y:Z" : "y:w"}\n` : `changed-parent\nbcd:b\ntail:${tail}\n`);
						expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() }))
							.toBe(hits + Number(!changed));
					} finally { await session.close(); }
				});
			}
		} finally { await fixture.dispose(); }
	});

	test("owns the entire Actor call when a held child crosses the adoption boundary", async ({ skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-held-transaction-"));
		const binary = path.join(root, "helper");
		execFileSync("cc", ["-pthread", "-O2", "-Wall", "-Wextra", "-Werror", fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), "-o", binary]);
		const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary });
		try {
			const run = (...args: string[]) => childProcess.spawnSync(binary, args, { encoding: "utf8", timeout: 1_000 });
			const passThrough = "printf out; printf err >&2; exit 7";
			const direct = childProcess.spawnSync("/bin/bash", ["-c", passThrough], { encoding: "utf8" });
			expect(run("/bin/bash", "-c", passThrough)).toMatchObject({ status: direct.status, signal: direct.signal, stdout: direct.stdout, stderr: direct.stderr });
			expect(run("--skip-code", "42", "/bin/bash", "-c", "exec /bin/sleep 5").status).toBe(42);
			expect(childProcess.spawnSync("/bin/bash", ["-c", "grep '^TracerPid:' /proc/self/status"], { encoding: "utf8" }).stdout).toMatch(/\t0\n$/);
			expect(run("/bin/bash", "-c", "grep '^TracerPid:' /proc/self/status").stdout).not.toMatch(/\t0\n$/);
			expect(run("/bin/bash", "-c", "(sleep 0.05; kill -CONT $$) & kill -STOP $$; printf resumed"))
				.toMatchObject({ status: 0, stdout: "resumed" });
			const text = `bound-name\nliteral ' $ value\nprivate value\n${root}\nstdin\n`;
			for (const [route, stdout, stderr] of [["12", text, "stderr"], ["11", text + "stderr", ""],
				["21", "stderr", text], ["22", "", text + "stderr"]]) {
				const result = childProcess.spawnSync(binary, ["--exec", route!, "bound-name", "/bin/sh", "-c",
					'IFS= read -r value; printf "%s\\n" "$0" "$1" "$BOUND_EXEC" "$PWD" "$value"; printf stderr >&2; exit 23',
					"bound-name", "literal ' $ value"], { encoding: "utf8", cwd: root,
					env: { ...process.env, BOUND_EXEC: "private value" }, input: "stdin\n" });
				expect([result.status, result.stdout, result.stderr]).toEqual([23, stdout, stderr]);
				expect(childProcess.spawnSync(binary, ["--exec", route!, "clean", binary, "--probe-clean-fds"]).status).toBe(0);
			}
			for (const args of [["--exec"], ["--exec", "13", "invalid", "/bin/true"]])
				expect(childProcess.spawnSync(binary, args).status).toBe(64);
			const queueImage = path.join(root, "queue"), queueManifest = path.join(root, "queue-queueManifest"), queueReport = path.join(root, "queue-queueReport");
			await writeFile(queueImage, "abc");
			for (const producer of [1, 2, 4, 5]) {
				await writeFile(queueReport, "");
				await writeFile(queueManifest, `FD2 1 0 ${Number(producer >= 4)}\n0 0 ${producer >= 4 ? 2 : 0} 0 ${Buffer.byteLength(queueImage)} ${producer} ${producer >= 4 ? 212992 : 4096} 0 ${producer === 5 ? 3 : 0} -1\n${queueImage}\n`);
				const child = childProcess.spawn(binary, ["--exec-fds", "12", queueManifest, queueReport, "consumer", "/bin/sh", "-c", "echo $$ >&2; exec /bin/cat"], { detached: true });
				let output = "", childPID = "", completed = false;
				child.stdout.on("data", data => { output += data.toString(); });
				child.stderr.on("data", data => { childPID += data.toString(); });
				const closed = once(child, "close").then(value => { completed = true; return value; });
				try {
					await expect.poll(() => output).toBe("abc");
					if (producer === 1 || producer === 5) {
						expect(await closed).toEqual([0, null]);
						expect(await readFile(queueReport, "utf8")).toMatch(producer === 1 ? /^FD2 1\n0 0 3 / : /^FD2 1\n0 2 0 /);
					} else {
						await new Promise(resolve => setTimeout(resolve, 30));
						expect(completed, "exhausted active input must block, never fabricate EOF").toBe(false);
						process.kill(-child.pid!, "SIGKILL"); await closed;
						await expect.poll(async () => (await readFile(`/proc/${Number(childPID)}/stat`, "utf8").catch(() => "")).split(") ")[1]?.[0] ?? "").not.toMatch(/[RSDT]/);
						expect(await readFile(queueReport, "utf8")).toBe("");
					}
				} finally { if (!completed) { process.kill(-child.pid!, "SIGKILL"); await closed; } }
			}
			expect(childProcess.spawnSync(binary, ["--exec", "12", "missing", path.join(root, "missing")]).status).toBe(127);
			for (const signal of ["TERM", "PIPE", "XFSZ"])
				expect(childProcess.spawnSync("/bin/bash", ["-c", `trap '' ${signal}; exec "$@"`, "outer", binary,
					"--exec", "12", "signals", "/bin/sh", "-c", `kill -${signal} $$; exit 99`]).signal).toBe(`SIG${signal}`);
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
			const compat = path.join(root, "compat32");
			await writeFile(`${compat}.s`, ".global _start\n_start: movl $1, %eax; movl $7, %ebx; int $0x80\n");
			execFileSync("cc", ["-nostdlib", "-m32", "-static", `${compat}.s`, "-o", compat]);
			if (childProcess.spawnSync(compat).status === 7) for (const descriptors of [false, true]) {
				const decide = vi.fn(async () => ({ kind: "continue" as const }));
				const executor = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", descriptors, decide });
				expect(await executor.execute({ command: `exec '${compat}'`, cwd: root, environment: { PATH: "/usr/bin:/bin" },
					timeout: 5, onData: () => {} })).toEqual({ exitCode: 7 });
				expect(decide).not.toHaveBeenCalled();
			}
			for (const [redirection, route] of [["", [1, 2]], ["2>&1", [1, 1]], ["3>&1", undefined], ["0<&-", [1, 2]], ["1>/dev/null", undefined]] as const) {
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
				if (route) {
					expect((await inspection!).outputRoute).toEqual(route);
					expect((await inspection!).context.descriptorTypes[0]).toBe(redirection === "0<&-" ? "closed" : "device");
				} else await expect(inspection).rejects.toThrow(/descriptors/);
			}
			const directoryInput = path.join(root, "anchor"); await mkdir(directoryInput);
			const input = path.join(root, "ofd-input");
			await writeFile(input, "abcdef");
			const manifest = path.join(root, "fd-plan"), report = path.join(root, "fd-report");
			await writeFile(report, "");
			await writeFile(manifest, `FD2 3 0 0\n0 0 32768 1 ${Buffer.byteLength(input)} 0 0 0 0 -1\n${input}\n3 0 32768 1 0 0 0 0 0 -1\n\n8 8 32768 1 ${Buffer.byteLength(input)} 0 0 0 0 -1\n${input}\n`);
			const reproduced = run("--exec-fds", "12", manifest, report, "fd-worker", "/bin/bash", "-c",
				`IFS= read -r -N 1 a; IFS= read -r -N 1 b <&3; IFS= read -r -N 1 c <&8; printf '%s:%s:%s' "$a" "$b" "$c"; ` +
				`(sleep 0.02; IFS= read -r -N 1 d <&3) & exit 7`);
			expect(reproduced).toMatchObject({ status: 7, stdout: "b:c:b", stderr: "" });
			const reportLines = (await readFile(report, "utf8")).trimEnd().split("\n");
			expect([reportLines[0], ...reportLines.slice(1).map(line => line.split(" ").slice(0, 3).join(" "))])
				.toEqual(["FD2 3", "0 32768 4", "3 32768 4", "8 32768 2"]);
			const external = path.join(root, "external-fd");
			await writeFile(external, `#!/bin/sh\nexec 9<'${input}'\nexec '${binary}' "$@"\n`, { mode: 0o700 });
			for (const shell of [binary, external]) {
				const snapshots: NonNullable<HeldExecProcess["descriptors"]>[] = [], failures: unknown[] = [];
				const inspecting = boundary.executor(adaptProcessToolOperations(createLocalBashOperations({ shellPath: shell })), {
					sourceRoot: root, realShell: "/bin/bash", descriptors: true, decide: async process => {
						try {
							const descriptors = process.descriptors!;
							expect(descriptors).toBeDefined(); snapshots.push(descriptors);
							const snapshot = await inspectHeldExecProcess(process.pid, await filesystem.readlink(`/proc/${process.pid}/exe`), descriptors);
							expect(snapshot.context.regularDescriptors).toEqual(descriptors);
							const first = descriptors.find(({ fd }) => fd === 3)!;
							expect(first).toMatchObject({ fd: 3, alias: 3, owned: true });
							expect(descriptors.find(({ fd }) => fd === 4)).toEqual({ ...first, fd: 4 });
							expect(descriptors.find(({ fd }) => fd === 5)).toMatchObject({ fd: 5, alias: 5, owned: true, inode: first.inode });
							if (shell === external) expect(descriptors.find(({ fd }) => fd === 9)).toMatchObject({ fd: 9, alias: 9, owned: false });
							await new Promise(resolve => setTimeout(resolve, 30));
							expect(await readFile(`/proc/${process.pid}/fdinfo/3`, "utf8")).toMatch(new RegExp(`^pos:\\s*${first.offset}$`, "m"));
						} catch (error) { failures.push(error); }
						return { kind: "continue" };
					},
				});
				expect(await inspecting.execute({ command: `exec 3<'${input}'; exec 4<&3; exec 5<'${input}'; ` +
					`(while IFS= read -r -N 1 value <&4; do :; done) & /bin/true; wait; /bin/true`,
					cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: () => {} })).toEqual({ exitCode: 0 });
				expect(failures).toEqual([]); expect(snapshots).toHaveLength(2);
			}
			const descriptorProbe = path.join(root, "descriptor-probe");
			await writeFile(`${descriptorProbe}.c`, `#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
static int aliases(void *unused) {
	(void)unused;
	if (dup2(3, 65) < 0 || dup3(3, 66, O_CLOEXEC) < 0 || fcntl(3, F_DUPFD, 67) != 67) return 75;
	return 0;
}
static void *thread(void *execute) {
	if (aliases(0)) _exit(75);
	if (execute) { char *command[] = {"true", 0}; execv("/bin/true", command); _exit(76); }
	return 0;
}
static int shared_exec(void *unused) { (void)unused; thread((void *)1); return 76; }
static int split_exec(void *argument) {
	const char *mode = argument;
	if (!strcmp(mode, "unshare")) {
		if (unshare(CLONE_FILES) || close(65) || dup2(3, 68) < 0) return 85;
	} else if (!strcmp(mode, "unshare-noop")) {
		if (unshare(0) || dup2(3, 68) < 0) return 86;
	} else if (!strcmp(mode, "range-invalid")) {
		if (close_range(67, 65, CLOSE_RANGE_UNSHARE) != -1 || dup2(3, 68) < 0) return 87;
	} else if (close_range(65, 67, CLOSE_RANGE_UNSHARE | (!strcmp(mode, "range-cloexec") ? CLOSE_RANGE_CLOEXEC : 0))) return 88;
	char *command[] = {"true", 0}; execv("/bin/true", command); return 76;
}
static void *blocked_open(void *file) { int fd = open(file, O_RDONLY); if (fd < 0) _exit(79); close(fd); return 0; }
int main(int argc, char **argv) {
	if (argc != 3) return 70;
	if (!strncmp(argv[1], "rights", 6) || !strcmp(argv[1], "pidfd")) {
		int sockets[2], status, copying = !strcmp(argv[1], "pidfd"); char byte = 'x';
		if (socketpair(AF_UNIX, strstr(argv[1], "batch") ? SOCK_SEQPACKET : SOCK_STREAM, 0, sockets)) return 93;
		pid_t child = fork(); if (child < 0) return 94;
		if ((child != 0) != copying) {
			close(sockets[1]); int fd;
			if (strstr(argv[1], "pipe")) {
				int stream[2]; if (pipe(stream) || write(stream[1], "abcdef", 6) != 6) return 110;
				close(stream[1]); fd = stream[0];
			} else fd = strstr(argv[1], "unowned") ? dup(9) : open(argv[2], O_RDONLY);
			if (fd < 0 || dup2(fd, 8) < 0) return 95;
			if (fd != 8) close(fd);
			if (copying) {
				if (write(sockets[0], &byte, 1) != 1 || read(sockets[0], &byte, 1) != 1) return 96;
				return 0;
			}
			int passed[] = {3, 3, 8}; char control[CMSG_SPACE(sizeof(passed))] = {0};
			struct iovec vector = {&byte, 1}; struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}};
			struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed));
			memcpy(CMSG_DATA(header), passed, sizeof(passed));
			if ((strstr(argv[1], "batch") ? sendmmsg(sockets[0], &message, 1, 0) : sendmsg(sockets[0], &message.msg_hdr, 0)) != 1) return 97;
			if (strstr(argv[1], "orphan")) { close(8); if (shutdown(sockets[0], SHUT_WR)) return 108; }
			close(sockets[0]); if (waitpid(child, &status, 0) != child || status) return 98;
			return 0;
		}
		close(sockets[0]); close(3); close(9); int received[3], saved[3];
		if (copying) {
			if (read(sockets[1], &byte, 1) != 1) return 99;
			int target = (int)syscall(SYS_pidfd_open, child, 0); if (target < 0) return 100;
			for (int index = 0; index < 3; index++) if ((received[index] = (int)syscall(SYS_pidfd_getfd, target, index == 2 ? 8 : 3, 0)) < 0) return 101;
			close(target);
		} else {
			if (strstr(argv[1], "orphan")) { struct pollfd ready = {.fd = sockets[1], .events = POLLRDHUP}; if (poll(&ready, 1, -1) != 1) return 109; }
			char control[CMSG_SPACE(sizeof(received))] = {0}; struct iovec vector = {&byte, 1};
			struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}};
			if (strstr(argv[1], "failed") && recvmsg(-1, &message.msg_hdr, 0) != -1) return 102;
			int flags = MSG_CMSG_CLOEXEC;
			if ((strstr(argv[1], "batch") ? recvmmsg(sockets[1], &message, 1, flags, 0) : recvmsg(sockets[1], &message.msg_hdr, flags)) != 1) return 103;
			struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr);
			if (!header || header->cmsg_len != CMSG_LEN(sizeof(received))) return 104;
			memcpy(received, CMSG_DATA(header), sizeof(received));
		}
		for (int index = 0; index < 3; index++) {
			if (fcntl(received[index], F_GETFD) != FD_CLOEXEC || (saved[index] = fcntl(received[index], F_DUPFD_CLOEXEC, 100)) < 0) return 105;
			close(received[index]);
		}
		if (copying && (write(sockets[1], &byte, 1) != 1 || waitpid(child, &status, 0) != child || status)) return 106;
		close(sockets[1]);
		if (dup2(saved[0], 3) < 0 || dup2(saved[1], 65) < 0 || dup2(saved[2], 67) < 0 || dup3(saved[0], 66, O_CLOEXEC) < 0) return 107;
		for (int index = 0; index < 3; index++) close(saved[index]);
		char *command[] = {"true", 0}; execv("/bin/true", command); return 76;
	}
	if (!strcmp(argv[1], "opath")) {
		int fd = open(argv[2], O_PATH); if (fd < 0 || dup2(fd, 3) < 0 || dup2(fd, 4) < 0 || dup2(fd, 5) < 0) return 92;
		if (fd > 5) close(fd);
		char *command[] = {"true", 0}; execv("/bin/true", command); return 76;
	}
	if (!strcmp(argv[1], "status")) {
		if (fcntl(3, F_SETFL, fcntl(3, F_GETFL) | O_APPEND | O_NONBLOCK)) return 89;
		char *command[] = {"true", 0}; execv("/bin/true", command); return 76;
	}
	if (!strcmp(argv[1], "lock")) {
		int fd = open(argv[2], O_RDWR); if (flock(fd, LOCK_EX) < 0) return 71; close(fd);
		fd = open(argv[2], O_RDWR); return flock(fd, LOCK_EX | LOCK_NB) < 0 ? 72 : 0;
	}
	if (!strcmp(argv[1], "export")) {
		int sockets[2]; char byte = 'x', control[CMSG_SPACE(sizeof(int))] = {0};
		if (socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) < 0) return 73;
		struct iovec vector = {&byte, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
		struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(int));
		int fd = 3; memcpy(CMSG_DATA(header), &fd, sizeof(fd)); if (sendmsg(sockets[0], &message, 0) < 0) return 74;
		close(sockets[0]); close(sockets[1]);
	} else if (!strcmp(argv[1], "thread") || !strcmp(argv[1], "thread-exec")) {
		pthread_t worker;
		if (pthread_create(&worker, 0, thread, !strcmp(argv[1], "thread-exec") ? argv : 0) || pthread_join(worker, 0)) return 77;
	} else if (!strcmp(argv[1], "shared-table") || !strcmp(argv[1], "shared-exec")) {
		char *stack = malloc(65536); int status;
		pid_t child = stack ? clone(!strcmp(argv[1], "shared-exec") ? shared_exec : aliases, stack + 65536, CLONE_FILES | SIGCHLD, 0) : -1;
		if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) return 78;
		if (fcntl(66, F_GETFD) != FD_CLOEXEC) return 80;
		free(stack);
	} else if (!strncmp(argv[1], "unshare", 7) || !strncmp(argv[1], "range-", 6)) {
		char *stack = malloc(65536); int status;
		if (aliases(0)) return 75;
		pid_t child = stack ? clone(split_exec, stack + 65536, CLONE_FILES | SIGCHLD, argv[1]) : -1;
		if (child < 0 || waitpid(child, &status, 0) != child || status) return 89;
		if (fcntl(65, F_GETFD) != 0 || fcntl(66, F_GETFD) != FD_CLOEXEC || fcntl(67, F_GETFD) != 0) return 90;
		if ((fcntl(68, F_GETFD) >= 0) != (!strcmp(argv[1], "unshare-noop") || !strcmp(argv[1], "range-invalid"))) return 91;
		free(stack);
	} else if (!strcmp(argv[1], "overlap")) {
		pthread_t worker; int status; const char *fifo = "overlap-fifo";
		if (mkfifo(fifo, 0600) || pthread_create(&worker, 0, blocked_open, (void *)fifo)) return 81;
		usleep(30000); int writer = open(fifo, O_WRONLY);
		if (writer < 0 || pthread_join(worker, 0)) return 82;
		close(writer); unlink(fifo);
		pid_t child = fork(); if (!child) { char *command[] = {"true", 0}; execv("/bin/true", command); _exit(76); }
		if (child < 0 || waitpid(child, &status, 0) != child || status) return 83;
		int fd = open(argv[2], O_RDONLY); if (fd < 0 || dup2(fd, 3) < 0) return 84; if (fd != 3) close(fd);
	} else if (aliases(0)) return 75;
	char *command[] = {"true", 0}; execv("/bin/true", command); return 76;
}
`);
			execFileSync("cc", ["-pthread", "-O2", "-Wall", "-Wextra", "-Werror", `${descriptorProbe}.c`, "-o", descriptorProbe]);
			for (const mode of ["lock", "export", "rights", "rights-batch", "rights-failed", "rights-unowned", "rights-orphan", "rights-pipe-orphan", "pidfd", "table", "thread", "thread-exec", "shared-table", "shared-exec", "overlap",
				"unshare", "unshare-noop", "range-close", "range-cloexec", "range-invalid"]) {
				let snapshot: HeldExecProcess["descriptors"], output = "";
				const split = mode.startsWith("unshare") || mode.startsWith("range-");
				const imported = mode.startsWith("rights") || mode === "pidfd";
				const unknown = mode === "rights-unowned";
				const shared = mode === "unshare-noop" || mode === "range-invalid", slots: number[][] = [];
				const ownership: boolean[] = [];
				const commit = vi.fn(async () => {});
				const executor = boundary.executor(mode === "rights-unowned" ? adaptProcessToolOperations(createLocalBashOperations({ shellPath: external })) : native, { sourceRoot: root, realShell: "/bin/bash", descriptors: true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					snapshot = process.descriptors;
					slots.push(snapshot!.map(({ fd }) => fd));
					ownership.push(snapshot!.find(({ fd }) => fd === 3)!.owned);
					return mode === "export" || split || imported ? { kind: "replay", descriptorOffsets: snapshot!.map(({ offset, ...descriptor }) =>
						({ ...descriptor, before: offset, after: offset + 1, ...(descriptor.type === "pipe" ? { content: Buffer.from(descriptor.queueHex!, "hex") } : {}) })), exitCode: 0, output: [], commit } : { kind: "continue" };
				} });
				expect(await executor.execute({ command: `exec 3<'${input}'; '${descriptorProbe}' ${mode} '${input}'; result=$?; ` +
					`IFS= read -r -N 1 byte <&3; printf '%s' "$byte"; exit "$result"`, cwd: root, environment: { PATH: "/usr/bin:/bin" },
					timeout: 5, onData: data => { output += data.toString(); } })).toEqual({ exitCode: 0 });
				expect(output, mode).toBe(split ? "c" : imported && !unknown ? "b" : "a"); expect(commit).toHaveBeenCalledTimes(split ? 2 : imported && !unknown ? 1 : 0);
				if (!["lock", "export", "overlap"].includes(mode)) expect(snapshot?.map(({ fd, alias, owned }) => ({ fd, alias, owned })), mode).toEqual([
					{ fd: 3, alias: 3, owned: true }, { fd: 65, alias: 3, owned: true }, { fd: 67, alias: imported ? 67 : 3, owned: !unknown },
					...(shared ? [{ fd: 68, alias: 3, owned: true }] : []),
				]);
				if (split) {
					expect(ownership, mode).toEqual([true, true]);
					expect(slots[0], mode).toEqual(shared ? [3, 65, 67, 68] : mode === "unshare" ? [3, 67, 68] : [3]);
				}
				if (mode === "export") expect(snapshot).toMatchObject([{ fd: 3, owned: false }]);
				if (mode === "overlap") expect(ownership).toEqual([false, true]);
			}
			let pipeOutput = "";
			const piped = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", descriptors: true, decide: async process => {
				if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
				return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.alloc(1024 * 1024, "x") }],
					descriptorOffsets: process.descriptors!.map(({ offset, ...descriptor }) => ({ ...descriptor, before: offset, after: offset + 1 })), commit: async () => {} };
			} });
			expect(await piped.execute({ command: `exec 3<'${input}'; /bin/true | /usr/bin/wc -c; IFS= read -r -N 1 byte <&3; printf '%s' "$byte"`,
				cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: data => { pipeOutput += data.toString(); } })).toEqual({ exitCode: 0 });
			expect(pipeOutput).toBe("1048576\nb");
			const pipeProbe = path.join(root, "pipe-probe");
			await writeFile(`${pipeProbe}.c`, `#define _GNU_SOURCE
#include <fcntl.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc != 2) return 70;
	int fds[2]; if (pipe2(fds, O_CLOEXEC | (!strcmp(argv[1], "packet") ? O_DIRECT : 0))) return 71;
	if (strcmp(argv[1], "empty") && write(fds[1], "abcdef", 6) != 6) return 72;
	int writer = fcntl(fds[1], F_DUPFD_CLOEXEC, 20); close(fds[1]);
	if (dup2(fds[0], 0) < 0) return 73;
	close(fds[0]);
	if (dup2(0, 3) < 0) return 74;
	int fd = open("/proc/self/fd/0", O_RDONLY); if (fd < 0 || dup2(fd, 8) < 0) return 75; close(fd);
	if (strcmp(argv[1], "live")) { close(writer); writer = -1; }
	pid_t child = fork(); if (child < 0) return 76;
	if (!child) { char *command[] = {"true", 0}; execv("/bin/true", command); _exit(77); }
	int status; if (waitpid(child, &status, 0) != child || status) return 78; close(writer);
	if (!strcmp(argv[1], "flags") && (!(fcntl(3, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK))) return 79;
	char bytes[6]; ssize_t size = read(0, bytes, sizeof(bytes));
	return size < 0 || write(1, bytes, (size_t)size) != size;
}
`);
			execFileSync("cc", ["-O2", "-Wall", "-Wextra", "-Werror", `${pipeProbe}.c`, "-o", pipeProbe]);
			for (const mode of ["partial", "empty", "flags", "queue-conflict", "contents", "overrun", "stale", "live", "packet", "inspect", "commit-failure", "journal", "journal-conflict", "journal-write-readonly", "journal-shutdown-pipe", "journal-commit-failure"]) {
				const accepted = ["partial", "empty", "flags", "journal"].includes(mode), journal = mode.startsWith("journal"), failedCommit = mode.endsWith("commit-failure");
				let output = "", descriptors: HeldExecProcess["descriptors"];
				const commit = vi.fn(async () => { if (failedCommit) throw new Error("pipe commit failure"); });
				const executor = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", descriptors: mode === "inspect" ? () => "inspect" : true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					descriptors = process.descriptors;
					if (mode === "stale") {
						const reader = await filesystem.open(`/proc/${process.pid}/fd/0`, "r");
						try { await reader.read(Buffer.alloc(1)); } finally { await reader.close(); }
					}
					const descriptorOffsets = await Promise.all([0, 3, 8].map(async fd => {
						const state = await filesystem.stat(`/proc/${process.pid}/fd/${fd}`, { bigint: true });
						const flags = fd === 8 ? 32768 : 0;
						return { fd, flags, device: String(state.dev), inode: String(state.ino), before: 0, capacity: descriptors?.find(descriptor => descriptor.fd === fd)?.capacity,
							after: journal || mode === "empty" ? 0 : mode === "overrun" ? 7 : mode === "queue-conflict" && fd === 8 ? 2 : 3,
							...(mode === "flags" && fd !== 8 ? { afterFlags: 2048 } : {}), content: Buffer.from(mode === "empty" ? "" : mode === "contents" ? "xxxxxx" : "abcdef") };
					}));
					return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.from("replayed:") }], descriptorOffsets, commit,
						...(journal ? { resourceEvents: [{ fd: 3, kind: "peek" as const, data: Buffer.from("abc") }, { fd: 0, kind: mode === "journal-write-readonly" ? "produce" as const : mode === "journal-shutdown-pipe" ? "shutdown" as const : "consume" as const, data: Buffer.from("ab") },
							{ fd: 8, kind: "consume" as const, data: Buffer.from(mode === "journal-conflict" ? "x" : "c") }] } : {}) };
				} });
				const running = executor.execute({ command: `'${pipeProbe}' ${mode}`, cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: data => { output += data.toString(); } });
				if (failedCommit) await expect(running).rejects.toMatchObject({ disposition: "poisoned" });
				else {
					expect(await running, mode).toEqual({ exitCode: 0 });
					expect(output, mode).toBe(accepted ? `replayed:${mode === "empty" ? "" : "def"}` : mode === "stale" ? "bcdef" : "abcdef");
				}
				expect(commit, mode).toHaveBeenCalledTimes(Number(accepted || failedCommit));
				if (accepted) expect(descriptors, mode).toMatchObject([{ fd: 0, alias: 0, type: "pipe", owned: true }, { fd: 3, alias: 0, type: "pipe", owned: true }, { fd: 8, alias: 8, type: "pipe", owned: true }]);
			}
			for (const mode of ["shared", "unlinked", "offset", "identity", "flags", "closed", "alias-conflict", "invalid", "commit-failure",
				"null", "null-offset", "null-content", "zero", "status-set", "status-clear", "status-conflict", "status-unsupported", "directory", "directory-offset", "directory-content", "directory-replaced", "directory-no-path", "opath", "opath-offset", "opath-content", "opath-flags", "directory-opath", "directory-symlink"]) {
				await writeFile(input, "abcdef");
				const directory = mode.startsWith("directory"), opath = mode.includes("opath"), device = directory || opath || mode.startsWith("null") || mode === "zero";
				const target = directory ? directoryInput : opath ? input : device ? mode === "zero" ? "/dev/zero" : "/dev/null" : input;
				const accepted = mode === "shared" || mode === "unlinked" || mode === "null" || mode === "status-set" || mode === "status-clear" || mode === "directory" || mode === "opath" || mode === "directory-opath";
				let output = "", heldPid = 0;
				const commit = vi.fn(async () => {
					expect(await readFile(`/proc/${heldPid}/fdinfo/3`, "utf8")).toMatch(/^pos:\s*0$/m);
					if (mode === "commit-failure") throw new Error("injected offset commit failure");
				}), adopted = vi.fn();
				const executor = boundary.executor(native, { sourceRoot: root, realShell: "/bin/bash", decide: async ({ pid }) => {
					if (await filesystem.readlink(`/proc/${pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					heldPid = pid;
					const descriptorOffsets = await Promise.all([3, 4, 5].map(async fd => {
						const info = await filesystem.stat(`/proc/${pid}/fd/${fd}`, { bigint: true });
						const text = await readFile(`/proc/${pid}/fdinfo/${fd}`, "utf8");
						return { fd, device: info.dev.toString(), inode: info.ino.toString(),
							flags: Number.parseInt(/^flags:\s*([0-7]+)/m.exec(text)![1]!, 8), before: 0, after: device ? 0 : fd === 5 ? 1 : 3,
							...(mode.startsWith("status-") && fd !== 5 ? { afterFlags: 32768 | (mode === "status-clear" ? 0 : mode === "status-unsupported" ? 0x2000 : mode === "status-conflict" && fd === 4 ? 0 : 0xc00) } : {}),
							...(directory && mode !== "directory-no-path" ? { path: directoryInput } : {}),
							...((mode === "null-content" || mode === "directory-content" || mode === "opath-content") && fd === 3 ? { content: Buffer.from("x") } : {}) };
					}));
					const first = descriptorOffsets[0]!;
					if (mode === "unlinked") await rm(input);
					if (mode === "offset") first.before = 1;
					if (mode === "identity") first.inode = "0";
					if (mode === "flags") first.flags ^= 0x800;
					if (mode === "closed") descriptorOffsets[1]!.fd = 1000;
					if (mode === "alias-conflict") descriptorOffsets[1]!.after = 4;
					if (mode === "invalid") first.after = -1;
					if (mode === "null-offset" || mode === "directory-offset" || mode === "opath-offset") first.after = 1;
					if (mode === "opath-flags") first.afterFlags = first.flags ^ 0x800;
					if (mode === "directory-replaced") { await filesystem.rename(directoryInput, `${directoryInput}-old`); await mkdir(directoryInput); }
					if (mode === "directory-symlink") { await filesystem.rename(directoryInput, `${directoryInput}-target`); await filesystem.symlink(`${directoryInput}-target`, directoryInput); }
					return { kind: "replay", descriptorOffsets, exitCode: 0,
						output: [{ fd: 1, data: Buffer.from("replayed:") }], commit, adopted };
				} });
				const running = executor.execute({ command: `exec 3<'${target}'; exec 4<&3; exec 5<'${target}'; ${opath ? `'${descriptorProbe}' opath '${target}'` : mode === "status-clear" ? `'${descriptorProbe}' status '${input}'` : "/bin/true"}; ` +
					(mode.startsWith("status-") ? `for fd in 3 4 5; do while read -r key value; do if [[ $key == flags: ]]; then (( (8#$value & 3072) == (${mode === "status-set" ? 3072 : 0} * (fd != 5)) )) || exit 90; fi; done </proc/self/fdinfo/$fd; done; ` : "") +
					(device ? "printf native" : `IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&5; IFS= read -r -N 1 c <&3; printf '%s:%s:%s' "$a" "$b" "$c"`),
					cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5, onData: data => { output += data.toString(); } });
				if (mode === "commit-failure") {
					await expect(running).rejects.toMatchObject({ disposition: "poisoned" }); expect(output).toBe("");
				} else {
					expect(await running).toEqual({ exitCode: 0 });
					expect(output, mode).toBe(device ? (accepted ? "replayed:native" : "native") : accepted ? "replayed:d:b:e" : "a:a:b");
				}
				expect(commit).toHaveBeenCalledTimes(Number(accepted || mode === "commit-failure"));
				expect(adopted).toHaveBeenCalledTimes(Number(accepted));
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
				expect(scan).toHaveBeenCalledExactlyOnceWith(process.pid, executable, undefined);
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
		const registry = new ProcessHandoffRegistry<null>(1, 16), scope = { sessionID: "cost", turnID: "first" };
		const binding = registry.observe(sha256Digest("cost"), "/worker", scope, null, 10)!;
		const close = vi.fn(async (workspace: string) => { ownedAtClose = existsSync(workspace); });
		vi.spyOn(backend, "open").mockImplementation(async ({ workspace }) => ({
			ownership,
			executeBinding: async () => { throw new Error("unexpected process binding"); },
			executionBindings: () => [binding],
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
				const operation = branch.operations![0]!, preparedMs = operation.expectedDurationMs;
				expect(operation.executionMs).toBe(10);
				expect(registry.observe(binding.key, "/worker", scope, null, 30)).toBe(binding);
				expect(operation.executionMs).toBe(30);
				expect(operation.expectedDurationMs).toBeCloseTo(preparedMs + 20);
				await expect(branch.commit()).resolves.toEqual(branch.output);
				await expect(branch.commit()).resolves.toEqual(branch.output);
				expect(branch.commitMetrics).toBeDefined();
				expect(ownership.claimChild()).toBe(false);

			}
			finally { await branch.dispose(); }
			expect(close).toHaveBeenCalledOnce();
			expect(ownedAtClose).toBe(true);
		} finally {
			await world.dispose?.();
			registry.dispose();
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
