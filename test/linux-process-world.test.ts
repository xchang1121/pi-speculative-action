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
import { createBashTool, createLocalBashOperations, createReadTool, createGrepTool, createLsTool, createFindTool } from "@earendil-works/pi-coding-agent";
import { describe, expect, test, vi } from "vitest";
import { buildPiActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { inspectHeldExecProcess, LinuxHeldExecBoundary, type HeldExecProcess } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend, validateTransferredProcessEvidence } from "../src/linux-process-backend.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff, type ProcessExecutionBinding } from "../src/process-handoff.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { sha256Digest } from "../src/provenance-certificate.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation, createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
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
	test.for(["pipe", "socket", "eventfd", "readv", "recvmsg", "writev", "mmap", "runtime", "handles", "eof", "early", "changed", "identity", "cancel"] as const)("resumes a learned running process with future input (%s)", { timeout: 60_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-live-process-");
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		try {
			await prepareLinuxProcessReuse(fixture);
			if (!(await Reflect.get(fixture.backend, "ready")).imageLibrary) return skip("native process image capture is unavailable");
			await writeFile(path.join(fixture.workspace, "worker.c"), `#define _GNU_SOURCE
#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#define INPUT_FD ${mode === "handles" ? 8 : 3}
#define ACK_FD ${mode === "handles" ? 3 : 4}
${mode === "runtime" ? `static __thread int local = 17;
static char alternate[65536]; static volatile sig_atomic_t delivered;
static void receive_signal(int signal) { char here; uintptr_t address = (uintptr_t)&here;
 delivered = signal == SIGUSR1 && address >= (uintptr_t)alternate && address < (uintptr_t)alternate + sizeof(alternate); }
static __attribute__((noinline)) int future_stack(int depth) { volatile unsigned char bytes[65536];
 for (unsigned i = 0; i < sizeof(bytes); i += 4096) bytes[i] = (unsigned char)depth;
 int total = depth ? future_stack(depth - 1) : 0;
 for (unsigned i = 0; i < sizeof(bytes); i += 4096) total += bytes[i]; return total; }
static void *future_thread(void *unused) { (void)unused; return (void *)(uintptr_t)(local == 17); }` : ""}
static ssize_t next_input(void *bytes, size_t length) {
 struct iovec parts[] = {{bytes, length / 2}, {(char *)bytes + length / 2, length - length / 2}};
 ${mode === "recvmsg" ? "struct msghdr message = {.msg_iov = parts, .msg_iovlen = 2}; return recvmsg(INPUT_FD, &message, 0);" : mode === "readv" ? "return readv(INPUT_FD, parts, 2);" : "(void)parts; return read(INPUT_FD, bytes, length);"}
}
int main(void) {
 ${mode === "identity" ? "pid_t identity = getpid();" : ""}
 ${mode === "runtime" ? `char *heap = malloc(65536); if (!heap) return 78; memset(heap, 'H', 65536); local = 23;
 stack_t stack = {.ss_sp = alternate, .ss_size = sizeof(alternate)};
 struct sigaction action = {.sa_handler = receive_signal, .sa_flags = SA_ONSTACK};
 if (sigaltstack(&stack, NULL) || sigaction(SIGUSR1, &action, NULL)) return 79;` : ""}
 ${mode === "mmap" ? `volatile char *mapping = mmap(NULL, 12288, PROT_READ | PROT_WRITE, MAP_PRIVATE, 5, 0);
 if (mapping == MAP_FAILED || mapping[0] != 'A') return 75; mapping[4096] = 'A'; mapping[8192] = 'Z';` : ""}
 char input[4] = {0}; uint64_t counter = 0;
 if (${mode === "eventfd" ? "read(3, &counter, 8) != 8 || counter != 1" : "read(3, input, 1) != 1"}) return 71;
 ${mode === "handles" ? `char bytes[2]; if (read(5, bytes, 2) != 2 || memcmp(bytes, "ab", 2) || lseek(6, 0, SEEK_CUR) != 2 || lseek(7, 0, SEEK_CUR)) return 83;
 if (fcntl(5, F_DUPFD_CLOEXEC, 12) != 12 || close(5) || close(6) || dup2(7, 6) != 6 || close(7) ||
     dup2(3, 8) != 8 || close(3) || dup2(4, 3) != 3 || close(4) || fcntl(8, F_SETFD, FD_CLOEXEC) ||
     fcntl(12, F_SETFL, O_APPEND)) return 84;` : ""}
 volatile uint64_t hash = 1; for (unsigned i = 0; i < ${mode === "early" ? 1500000000 : 250000000}; i++) hash = hash * 33 + i;
 ${mode === "eof" ? `char fill[131072]; memset(fill, 'P', sizeof(fill));
 if (close(4) || write(1, fill, sizeof(fill)) != sizeof(fill)) return 72;` : mode === "writev" ? `char fill[4096]; for (unsigned i = 0; i < sizeof(fill); i++) fill[i] = 'P';
 if (write(4, fill, sizeof(fill)) != sizeof(fill)) return 72;
 struct iovec ready[] = {{"R", 1}}; if (writev(4, ready, 1) != 1) return 72;` : `if (write(ACK_FD, "R", 1) != 1) return 72;`}
 ${mode === "eventfd" ? `if (read(3, &counter, 8) != 8 || counter != 2 || write(4, "S", 1) != 1 || read(3, &counter, 8) != 8 || counter != 3) return 73;
 input[0] = 'A';` : `
 unsigned used = 1; ssize_t n; while ((n = next_input(input + used, sizeof(input) - used)) > 0) used += n;
 if (n || used != 3 || (input[0] != 'A' && input[0] != 'D') || input[1] != 'B' || input[2] != 'C') return 73;`}
 if (getpid() != syscall(SYS_gettid) ${mode === "identity" ? "|| identity != getpid()" : ""}) return 74;
 ${mode === "handles" ? `if (fcntl(4, F_GETFD) != -1 || errno != EBADF || fcntl(8, F_GETFD) != FD_CLOEXEC ||
     fcntl(12, F_GETFD) != FD_CLOEXEC || !(fcntl(12, F_GETFL) & O_APPEND) || read(12, bytes, 1) != 1 || bytes[0] != 'c' ||
     lseek(6, 0, SEEK_CUR) || read(6, bytes, 1) != 1 || bytes[0] != 'a') return 85;` : ""}
 ${mode === "runtime" ? `heap = realloc(heap, 262144); if (!heap || local != 23 || pthread_kill(pthread_self(), SIGUSR1) || !delivered || future_stack(32) != 8448) return 80;
 for (unsigned i = 0; i < 65536; i++) if (heap[i] != 'H') return 81; free(heap);
 pthread_t thread; void *returned; if (pthread_create(&thread, NULL, future_thread, NULL) || pthread_join(thread, &returned) || returned != (void *)1) return 82;` : ""}
 ${mode === "mmap" ? `char value; if (mapping[0] != 'B' || mapping[4096] != 'A' || mapping[8192] != 'Z') return 76;
 for (unsigned i = 0; i < 3; i++) if (pread(5, &value, 1, i * 4096) != 1 || value != 'B') return 77;` : ""}
 printf("%cBC:%llu\\n", input[0], (unsigned long long)hash); return 37;
}
`);
			await writeFile(path.join(fixture.workspace, "launch.c"), `#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/wait.h>
int main(int argc, char **argv) {
 if (argc != 2) return 60;
 int in[2], ack[2]; if (${mode === "eventfd" ? "(in[0] = eventfd(0, 0)) < 0 || (in[1] = dup(in[0])) < 0" : mode === "socket" || mode === "recvmsg" ? "socketpair(AF_UNIX, SOCK_STREAM, 0, in)" : "pipe(in)"} || pipe(ack)) return 61;
 ${mode === "eof" ? "int output[2]; if (pipe(output)) return 61;" : ""}
 ${mode === "writev" ? "if (fcntl(ack[1], F_SETPIPE_SZ, 4096) != 4096) return 61;" : ""}
 ${mode === "mmap" ? `int backing = memfd_create("live-backing", 0); if (backing < 0 || ftruncate(backing, 12288)) return 61;
 for (unsigned i = 0; i < 3; i++) if (pwrite(backing, "A", 1, i * 4096) != 1) return 61;` : ""}
 ${mode === "handles" ? `int backing = memfd_create("live-ofd", 0); if (backing < 0 || write(backing, "abcd", 4) != 4 || lseek(backing, 0, SEEK_SET)) return 61;
 char pathname[64]; snprintf(pathname, sizeof(pathname), "/proc/self/fd/%d", backing); int independent = open(pathname, O_RDONLY); if (independent < 0) return 61;` : ""}
 pid_t child = fork(); if (child < 0) return 62;
 if (!child) { int a = fcntl(in[0], F_DUPFD_CLOEXEC, 10), b = fcntl(ack[1], F_DUPFD_CLOEXEC, 10);
  if (a < 0 || b < 0 || dup2(a, 3) != 3 || dup2(b, 4) != 4) _exit(63);
  ${mode === "eof" ? "if (dup2(output[1], 1) != 1) _exit(63);" : ""}
  ${mode === "handles" ? "if (dup2(backing, 5) != 5 || dup2(5, 6) != 6 || dup2(independent, 7) != 7) _exit(63); close_range(8, ~0U, 0);" : mode === "mmap" ? "if (dup2(backing, 5) != 5) _exit(63); close_range(6, ~0U, 0);" : "close_range(5, ~0U, 0);"} execlp("worker", "worker", NULL); _exit(64); }
 close(in[0]); close(ack[1]); char ready; uint64_t counter = 1;
 ${mode === "eof" ? "close(output[1]);" : ""}
 if (${mode === "eventfd" ? "write(in[1], &counter, 8) != 8" : `write(in[1], argv[1][0] == 'c' ? "D" : "A", 1) != 1`}) return 65;
 ${mode === "writev" ? `char fill[4096]; unsigned used = 0; ssize_t size;
 while (used < sizeof(fill) && (size = read(ack[0], fill + used, sizeof(fill) - used)) > 0) used += size;
 if (used != sizeof(fill)) return 65; for (unsigned i = 0; i < used; i++) if (fill[i] != 'P') return 65;` : ""}
 ${mode === "eof" ? `if (read(ack[0], &ready, 1) != 0) return 65;
 char chunk[4096]; unsigned total = 0; while (total < 131072) { ssize_t size = read(output[0], chunk, sizeof(chunk));
 if (size <= 0) return 65; for (ssize_t i = 0; i < size; i++) if (chunk[i] != 'P') return 65; total += (unsigned)size; }
 if (total != 131072) return 65;` : "if (read(ack[0], &ready, 1) != 1 || ready != 'R') return 65;"}
 ${mode === "mmap" ? `for (unsigned i = 0; i < 3; i++) if (pwrite(backing, "B", 1, i * 4096) != 1) return 65;` : ""}
 ${mode === "eventfd" ? `counter = 2; if (write(in[1], &counter, 8) != 8 || read(ack[0], &ready, 1) != 1 || ready != 'S') return 66;
 counter = 3; if (write(in[1], &counter, 8) != 8) return 67;` : `if (write(in[1], "B", 1) != 1) return 66;
 usleep(1000); if (write(in[1], "C", 1) != 1) return 67;`}
 close(in[1]); close(ack[0]);
 ${mode === "eof" ? "ssize_t size; while ((size = read(output[0], chunk, sizeof(chunk))) > 0) if (write(1, chunk, (size_t)size) != size) return 68; if (size) return 68; close(output[0]);" : ""}
 int status; return waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 37
 ${mode === "handles" ? "|| lseek(backing, 0, SEEK_CUR) != 3 || lseek(independent, 0, SEEK_CUR) != 1 || !(fcntl(backing, F_GETFL) & O_APPEND)" : ""};
}
`);
			for (const name of ["worker", "launch"]) await compileBenchmarkHelper(fixture.workspace, { source: `${name}.c`, output: name, arguments: ["-pthread"] });
			await commitBenchmarkFixture(fixture.workspace, "Live process continuation");
			const route = await fixture.prepareActorReplay();
			if (!("executor" in route)) throw new Error(route.detail);
			const tools = [fixture.tool], events: SpeculativeActionEvent<string>[] = [], scope = { sessionID: "live", turnID: "seed" };
			const settings = patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 });
			const patternStore = new PatternAwareStore(settings, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, fixture.workspace));
			const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
			const publishing = vi.spyOn(registry, "publish"), errors = vi.spyOn(fixture.backend as any, "setError");
			host = createSpeculativeActionHost(scope.sessionID, { cwd: fixture.workspace, patternStore, complete: async () => { throw new Error("no inference"); },
				getSettings: () => ({ enabled: true, drafterEnabled: false, candidateLimit: 4, maxConcurrentActions: 4, tools: ["bash"], patternAware: settings }),
				preflight: () => true, executionWorlds: [fixture.world],
				resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath }),
				onEvent: event => { events.push(event); },
			});
			const start = (turnID: string) => host!.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined,
				context: { systemPrompt: "continuation", messages: [], tools } });
			const execute = (turnID: string, command: string) => host!.execute({ turnID, id: turnID, tool: "bash", args: { command }, tools }, undefined,
				() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID } }) },
					() => fixture.tool.execute(turnID, { command })));
			for (const turnID of ["common-1", "common-2"]) { await start(turnID); await execute(turnID, "printf common"); await host.finishTurn(turnID); }
			await start("seed"); const seed = await execute("seed", "printf seed; launch seed"); await host.finishTurn("seed");
			expect(seed.content).toMatchObject([{ type: "text", text: expect.stringMatching(/^seedABC:\d+\n$/) }]);
			const spawnStart = vi.mocked(childProcess.spawn).mock.calls.length;
			await start("prepared");
			const diagnostic = () => JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(), errors: errors.mock.calls.map(call => call[1]),
				certificates: publishing.mock.calls.map(([, , certificate]) => certificate && { complete: certificate.dependencyCertificate, continuation: certificate.result.continuation }),
				operations: events.filter(event => event.type === "operation_prediction") });
			let privatePid = 0;
			await expect.poll(async () => {
				for (const [, args] of vi.mocked(childProcess.spawn).mock.calls.slice(spawnStart)) {
					const image = Array.isArray(args) && args.find(arg => arg.startsWith("--handoff-image="));
					if (!image) continue;
					const report = await readFile(path.join(path.dirname(image.slice("--handoff-image=".length)), "fd-offsets"), "utf8").catch(() => "");
					const ready = /^RUNNING (\d+)\n$/.exec(report); if (!ready) continue;
					const pid = Number(ready[1]), state = await readFile(`/proc/${pid}/syscall`, "utf8").catch(() => "");
					if (mode === "early") {
						const usage = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => ""), fields = usage.slice(usage.lastIndexOf(")") + 2).split(" ");
						if (/^(?:running|-1 )/.test(state) && Number(fields[11]) + Number(fields[12]) >= 40) { privatePid = pid; return true; }
						continue;
					}
					const frontier = mode === "handles" ? "0 0x8 " : mode === "writev" ? "20 0x4 " : mode === "readv" ? "19 0x3 " : mode === "recvmsg" ? "47 0x3 " : "0 0x3 ";
					if (state.startsWith(frontier)) { privatePid = pid; return true; }
				}
				return false;
			}, { timeout: 10_000 }).toBe(true).catch(error => { throw new Error(diagnostic(), { cause: error }); });
			if (mode === "cancel") { await host.finishTurn("prepared", true); await expect.poll(() => existsSync(`/proc/${privatePid}`)).toBe(false); }
			const command = `printf actual; launch ${mode === "changed" ? "changed" : "actual"}`;
			const actual = mode === "cancel" ? await fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID: "later" } }) },
				() => fixture.tool.execute("later", { command })) : await execute("prepared", command);
			expect(actual.content, JSON.stringify(actual.content)).toEqual(seed.content.map(item => item.type === "text" ? { ...item,
				text: item.text.replace(/^seedA/, `actual${mode === "changed" ? "D" : "A"}`) } : item));
			const resumed = mode !== "changed" && mode !== "identity" && mode !== "cancel";
			expect(fixture.backend.actorMetrics().joinedHits, diagnostic()).toBe(Number(resumed));
			if (resumed) expect(publishing.mock.calls.some(([, , certificate]) => certificate?.result.continuation), diagnostic()).toBe(true);
			if (mode !== "cancel") await host.finishTurn("prepared");
			expect(existsSync(`/proc/${privatePid}`)).toBe(false);
			if (resumed) expect(events.filter(event => event.type === "operation_prediction"), diagnostic()).toContainEqual(expect.objectContaining({ settlement: expect.objectContaining({
				observation: "observed", match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }) }) }));
		} finally { await host?.dispose(); await fixture.dispose(); }
	});

	test("transfers native Bash FD inputs to filesystem tools across turns with exact invalidation", { timeout: 30_000 }, async ({ skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-native-resource-inputs-");
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		const search = await createClosedSearchProfile(fixture.workspace);
		try {
			if (!search.invocations.has("grep") || !search.invocations.has("find")) return skip("qualified search executors are unavailable");
			const file = path.join(fixture.workspace, "input.txt"); await writeFile(file, "one\ntwo\n");
			await mkdir(path.join(fixture.workspace, "entries"));
			for (const name of ["z.txt", "a.txt", "\ue000.txt", "\u{10000}.txt"]) await writeFile(path.join(fixture.workspace, "entries", name), name);
			await writeFile(path.join(fixture.workspace, "worker.c"), `#include <unistd.h>
#include <stdio.h>
#include <sys/syscall.h>
int main(void) { char b[1024]; return syscall(SYS_getdents64, 4, b, sizeof(b)) < 0 || pread(3, b, 8, 0) != 8 || write(1, b, 8) != 8; }
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			await commitBenchmarkFixture(fixture.workspace, "Native resource input bridge"); await prepareLinuxProcessReuse(fixture);
			const route = await fixture.prepareActorReplay();
			if (!("executor" in route)) throw new Error(route.detail);
			const tools = [fixture.tool, createReadTool(fixture.workspace), createGrepTool(fixture.workspace), createLsTool(fixture.workspace), createFindTool(fixture.workspace)], events: SpeculativeActionEvent<string>[] = [];
			host = createSpeculativeActionHost("native-inputs", { cwd: fixture.workspace, complete: async () => { throw new Error("no inference"); },
				getSettings: () => ({ enabled: true, drafterEnabled: false, tools: tools.map(tool => tool.name), resourceCacheMaxEntries: 8,
					resourceCacheMaxBytes: 1024 * 1024, patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false }) }),
				preflight: () => true, executionWorlds: [fixture.world,
					createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read", "grep", "ls", "find"], maxBytes: () => 1024 * 1024 })],
				resolveInvocation: (tool, input) => search.invocations.get(tool) ?? resolvePiToolInvocation(tool, input,
					{ cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath }),
				onEvent: event => { events.push(event); },
			});
			const start = (turnID: string) => host!.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined,
				context: { systemPrompt: "inputs", messages: [], tools } });
			const command = "exec 3<input.txt 4<entries; worker; worker", bash = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { sessionID: "native-inputs", turnID: "seed" } }) }, () => fixture.tool.execute("seed", { command })));
			await start("seed");
			expect((await host.execute({ turnID: "seed", id: "seed", tool: "bash", args: { command }, tools }, undefined, bash)).content)
				.toEqual([{ type: "text", text: "one\ntwo\none\ntwo\n" }]);
			await host.finishTurn("seed"); expect(bash).toHaveBeenCalledOnce();
			await start("queries");
			for (const [tool, args] of [["read", { path: "input.txt", offset: 2 }], ["grep", { path: "input.txt", pattern: "two" }],
				["ls", { path: "entries" }], ["find", { path: "entries", pattern: "*.txt" }]] as const) {
				const native = tools.find(value => value.name === tool)!;
				const invocation = search.invocations.get(tool);
				const authoritative = (callID: string) => invocation?.authoritative
					? invocation.authoritative({ callID, args, signal: new AbortController().signal }).then(value => value.result) : native.execute(callID, args);
				const execute = vi.fn(() => authoritative("fallback"));
				expect(await host.execute({ turnID: "queries", id: tool, tool, args, tools }, undefined, execute)).toEqual(await authoritative("oracle"));
				expect(execute, JSON.stringify(summarizeSpeculativeTrace(events))).not.toHaveBeenCalled();
			}
			const read = tools[1]!, args = { path: "input.txt" }, execute = vi.fn(() => read.execute("fallback", args));
			await writeFile(file, "new\nvalue\n");
			expect(await host.execute({ turnID: "queries", id: "stale", tool: "read", args, tools }, undefined, execute)).toEqual(await read.execute("oracle", args));
			expect(execute).toHaveBeenCalledOnce();
			await writeFile(path.join(fixture.workspace, "entries", "new.txt"), "new");
			const ls = tools[3]!, directory = { path: "entries" }, list = vi.fn(() => ls.execute("fallback", directory));
			expect(await host.execute({ turnID: "queries", id: "stale-directory", tool: "ls", args: directory, tools }, undefined, list)).toEqual(await ls.execute("oracle", directory));
			expect(list).toHaveBeenCalledOnce();
			await host.finishTurn("queries", true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 4, exactReuseHits: 0 });
		} finally { await host?.dispose(); await search.pool.dispose(); await fixture.dispose(); }
	});

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
				// A bounded trace preview can lag the separate filesystem readiness signal.
				await expect.poll(() => check!(), { timeout: 2000 }).toBe(true);
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

	test.for(["completed", "running", "native", "native-merged", "native-closed-input", "native-descriptors", "native-null", "native-null-stdin", "native-status", "native-directory", "native-directory-entries", "native-directory-prepared-stale", "native-directory-opath", "native-directory-opath-prepared-stale", "native-pipe", "native-pipe-prepared-stale", "native-pipe-live", "native-pipe-live-prepared-stale", "native-pipe-live-mixed", "native-pipe-live-transfer", "native-pipe-producer", "native-pipe-producer-queued", "native-pipe-producer-short", "native-pipe-producer-splice", "native-socket", "native-socket-running", "native-socket-duplex", "native-socket-lifetime", "native-socket-io", "native-socket-messages", "native-socket-export", "native-socket-counter-export", "native-socket-prequeued", "native-socket-segmented", "native-socket-prefixed", "native-socket-orphan", "native-eventfd", "native-eventfd-semaphore", "native-socket-concurrent", "native-socket-half-closed", "native-socket-prepared-stale", "native-shared-table", "native-unshare", "native-prepared-stale", "native-prepared-restored"] as const)("reexecutes an owned child binding across turns without replaying its parent or stale input (%s)", { timeout: 20_000 }, async (mode, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const fixture = await createLinuxProcessBenchmark("pi-process-binding-");
		const publishing = vi.spyOn(fixture.backend.planner, "publishCompleted");
		const errors = vi.spyOn(fixture.backend as unknown as { setError(session: unknown, message: string): void }, "setError");
		const native = mode.startsWith("native"), running = mode.endsWith("running"), enumerate = mode.includes("entries");
		const orphan = mode.includes("orphan"), segmented = mode.includes("segmented"), prefixed = mode.endsWith("prefixed"), queueCounter = mode === "native-socket-counter-export", prequeued = orphan || segmented || prefixed || mode.endsWith("prequeued") || queueCounter;
		const discovery = segmented || orphan;
		const counter = mode.includes("eventfd"), semaphore = mode.endsWith("semaphore");
		const exporting = mode.endsWith("-export"), messages = mode.endsWith("messages"), transferIO = mode.endsWith("-splice");
		const shortWrite = mode.endsWith("short"), io = mode.endsWith("-io"), lifetime = mode.endsWith("lifetime"), pipeOutput = mode.startsWith("native-pipe-producer"), queuedOutput = mode === "native-pipe-producer-queued";
		const mixed = mode.endsWith("mixed"), socket = mode.includes("socket"), duplex = prequeued || exporting || lifetime || io || messages || mode.endsWith("duplex") || mode.endsWith("concurrent"), concurrent = mode.endsWith("concurrent"), pipe = mode.includes("pipe"), opath = mode.includes("opath") || mixed, launcher = counter || pipe || socket || mode === "native-shared-table" || mode === "native-unshare" || opath || enumerate;
		const nullDevice = mixed || mode === "native-null" || mode === "native-null-stdin" || mode === "native-status";
		const directory = mode.includes("directory") || mixed;
		const descriptors = mode === "native-descriptors" || launcher || nullDevice || directory;
		const rightsHelpers = orphan ? `static int pass(int fd, const char *text, int length, const int *rights, int count) {
	char control[CMSG_SPACE(8 * sizeof(int))] = {0}; struct iovec vector = {(void *)text, (size_t)length};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = CMSG_SPACE((size_t)count * sizeof(int))};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN((size_t)count * sizeof(int));
	memcpy(CMSG_DATA(header), rights, (size_t)count * sizeof(int)); return sendmsg(fd, &message, 0) != length;
}
static int take(int fd, char expected, int *rights, int count, int peek) {
	char byte, control[CMSG_SPACE(8 * sizeof(int))]; struct iovec vector = {&byte, 1};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	if (recvmsg(fd, &message, MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0)) != 1 || byte != expected || message.msg_flags & MSG_CTRUNC) return 1;
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); if (!header || header->cmsg_len != CMSG_LEN((size_t)count * sizeof(int))) return 1;
	memcpy(rights, CMSG_DATA(header), (size_t)count * sizeof(int));
	for (int index = 0; index < count; index++) if (fcntl(rights[index], F_GETFD) != FD_CLOEXEC) return 1;
	return 0;
}` : "";
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
#include <poll.h>
#include <sys/select.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
${rightsHelpers}
${enumerate ? `static int count_directory(int fd) {
	char entries[64]; int count = 0; ssize_t length;
	while ((length = syscall(SYS_getdents64, fd, entries, sizeof(entries))) > 0) {
		for (ssize_t offset = 0; offset < length;) { unsigned short size; memcpy(&size, entries + offset + 16, 2); if (size < 24 || size > length - offset) return -1; offset += size; count++; }
	}
	return length < 0 ? -1 : count;
}` : ""}
int main(int argc, char **argv) {
	if (argc != 2 || strcmp(argv[0], "bound-name") || strcmp(argv[1], "private argument") ||
		!getenv("BOUND_SECRET") || strcmp(getenv("BOUND_SECRET"), "private value")) return 71;
	for (volatile unsigned long iteration = 0; iteration < ${running ? 500000000 : native ? 50000000 : 0}ul; ++iteration) {}
	${orphan ? `{ if (fcntl(9, F_GETFD) != -1 || errno != EBADF || fcntl(15, F_GETFD) != -1 || errno != EBADF) return 187;
	int received[7]; char byte;
	for (int peek = 1; peek >= 0; peek--) {
		if (take(3, 'q', received, 7, peek) || read(received[3], &byte, 1) != -1 || errno != EAGAIN) return 189;
		if (peek) for (int index = 0; index < 7; index++) close(received[index]);
	}
	if (read(received[0], &byte, 1) != 1 || byte != 'b' || lseek(received[1], 0, SEEK_CUR) != 2 ||
		fcntl(received[0], F_SETFL, O_NONBLOCK) || !(fcntl(received[1], F_GETFL) & O_NONBLOCK)) return 190;
	close(received[0]); close(received[1]);
	uint64_t count; if (read(received[2], &count, 8) != 8 || count != 5) return 191;
	count = 7; if (write(received[2], &count, 8) != 8 || read(received[2], &count, 8) != 8 || count != 7) return 192; close(received[2]);
	if (write(received[4], "x", 1) != 1) return 193;
	close(received[4]);
	if (read(received[3], &byte, 1) != 1 || byte != 'x' || read(received[3], &byte, 1) != 0) return 194;
	close(received[3]);
	int peer, alias;
	if (take(received[5], 'D', &peer, 1, 0) || take(peer, 'C', &alias, 1, 0) || pass(alias, "Z", 1, &received[6], 1)) return 195;
	int root; if (take(peer, 'Z', &root, 1, 0) || recv(root, &byte, 1, MSG_PEEK) != 1 || byte != 'b') return 196;
	close(root); close(alias); close(received[5]);
	if (read(peer, &byte, 1) != 0) return 197;
	close(peer); close(received[6]); }` : ""}
	${prefixed ? 'char gap; if (read(3, &gap, 1) != 1 || gap != 76) return 180;' : ""}
	${segmented ? 'char gap; if (read(3, &gap, 1) != 1 || gap != 76) return 180; int received[2]; char byte, control[CMSG_SPACE(sizeof(received))]; struct iovec vector = {&byte, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control}; for (int segment = 0; segment < 2; segment++) { for (int peek = 1; peek >= 0; peek--) { message.msg_controllen = sizeof(control); if (recvmsg(3, &message, MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0)) != 1 || byte != (segment ? 116 : 113) || message.msg_flags & MSG_CTRUNC) return 181; struct cmsghdr *header = CMSG_FIRSTHDR(&message); if (!header || header->cmsg_len != CMSG_LEN(sizeof(received))) return 182; memcpy(received, CMSG_DATA(header), sizeof(received)); if (!peek && !segment && (read(received[0], &byte, 1) != 1 || byte != 98)) return 183; close(received[0]); close(received[1]); } if (!segment && (read(3, &gap, 1) != 1 || gap != 73)) return 184; }' : prequeued && !orphan ? 'int received[2]; char byte, control[CMSG_SPACE(sizeof(received))]; struct iovec vector = {&byte, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control}; for (int peek = 1; peek >= 0; peek--) { message.msg_controllen = sizeof(control); if (recvmsg(3, &message, MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0)) != 1 || byte != 113 || message.msg_flags & MSG_CTRUNC) return 153; struct cmsghdr *header = CMSG_FIRSTHDR(&message); if (!header || header->cmsg_len != CMSG_LEN(sizeof(received))) return 154; memcpy(received, CMSG_DATA(header), sizeof(received)); if (fcntl(received[0], F_GETFD) != FD_CLOEXEC || recv(received[1], &byte, 1, MSG_PEEK) != 1 || byte != (peek ? 113 : 98)) return 155; ' + (queueCounter ? 'if (!peek) { uint64_t count; if (read(received[0], &count, 8) != 8 || count != 5) return 158; count = 7; if (write(21, &count, 8) != 8) return 159; }' : 'if (!peek && (read(received[0], &byte, 1) != 1 || byte != 98)) return 156;') + ' close(received[0]); close(received[1]); }' : ""}
	${exporting ? `{ int passed = 3; char value = 120, control[CMSG_SPACE(sizeof(passed))] = {0}; struct iovec vector = {&value, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed)); memcpy(CMSG_DATA(header), &passed, sizeof(passed)); if (fcntl(3, F_SETFL, O_NONBLOCK) || sendmsg(3, &message, 0) != 1) return 132; close(0); close(3); close(4); close(8); char text[32]; int fd = open("input.txt", O_RDONLY); ssize_t length = read(fd, text, sizeof(text)); return length <= 0 || write(1, text, (size_t)length) != length; }` : ""}
	${mode === "native-closed-input" ? 'char probe; if (read(0, &probe, 1) != -1 || errno != EBADF) return 72;' : ""}
	${io ? 'struct pollfd ready = {.fd = 3, .events = POLLIN | POLLOUT}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT) || fcntl(3, F_SETFL, O_NONBLOCK)) return 114; fd_set rd, wr; FD_ZERO(&rd); FD_ZERO(&wr); FD_SET(3, &rd); FD_SET(3, &wr); struct timeval timeout = {0}; if (select(4, &rd, &wr, 0, &timeout) != 2 || !FD_ISSET(3, &rd) || !FD_ISSET(3, &wr)) return 118; int ep = epoll_create1(EPOLL_CLOEXEC); struct epoll_event registration = {.events = EPOLLIN | EPOLLONESHOT, .data.u64 = 42}, readyEvents[2]; if (ep < 0 || epoll_ctl(ep, EPOLL_CTL_ADD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || readyEvents[0].events != EPOLLIN || readyEvents[0].data.u64 != 42 || epoll_wait(ep, readyEvents, 2, 0) != 0 || epoll_ctl(ep, EPOLL_CTL_MOD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || epoll_ctl(ep, EPOLL_CTL_DEL, 3, 0)) return 119; registration.events = EPOLLIN | EPOLLET; if (epoll_ctl(ep, EPOLL_CTL_ADD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || epoll_wait(ep, readyEvents, 2, 0) != 0 || epoll_ctl(ep, EPOLL_CTL_MOD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1) return 125; close(ep);' : ""}
	${counter ? `uint64_t count[2] = {0};
	if (read(20, count, sizeof(count)) != 8 || count[0] != ${semaphore ? 1 : 5}) return 137;
	count[0] = 2; if (write(21, count, 8) != 8) return 138;
	struct pollfd ready = {.fd = 20, .events = POLLIN | POLLOUT}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT)) return 139;
	int ep = epoll_create1(EPOLL_CLOEXEC); struct epoll_event registration = {.events = EPOLLIN | EPOLLONESHOT, .data.u64 = 42}, readyEvent;
	if (ep < 0 || epoll_ctl(ep, EPOLL_CTL_ADD, 21, &registration) || epoll_wait(ep, &readyEvent, 1, 0) != 1 || readyEvent.events != EPOLLIN || readyEvent.data.u64 != 42 || epoll_wait(ep, &readyEvent, 1, 0) != 0) return 140; close(ep);
	if (fcntl(20, F_SETFL, 0) || (fcntl(21, F_GETFL) & O_NONBLOCK) || fcntl(21, F_SETFL, O_NONBLOCK) || read(21, count, 8) != 8 || count[0] != ${semaphore ? 1 : 2}) return 141;
	${semaphore ? 'pid_t forked = fork(); if (forked < 0) return 142; if (!forked) return read(20, count, 8) != 8 || count[0] != 1; int waited; if (waitpid(forked, &waited, 0) != forked || waited) return 143;' : ''}
	count[0] = 7; if (write(20, count, 8) != 8) return 144;
	count[0] = 1; if (write(22, count, 8) != 8) return 145;
	ready.fd = 22; if (poll(&ready, 1, 0) != 1 || ready.revents != POLLIN || write(22, count, 8) != -1 || errno != EAGAIN) return 146;
	count[0] = UINT64_MAX; if (write(22, count, 8) != -1 || errno != EINVAL || read(22, count, 7) != -1 || errno != EINVAL) return 147;
	if (read(22, count, 8) != 8 || count[0] != UINT64_MAX - 1 || read(22, count, 8) != -1 || errno != EAGAIN) return 148;
	` : ""}

	${socket ? 'char preview; if (recv(3, &preview, 1, MSG_PEEK) != 1 || preview != \'b\') return 106;' : ""}
	${transferIO ? 'if (tee(3, 14, 3, SPLICE_F_NONBLOCK) != 3 || splice(3, 0, 14, 0, 3, SPLICE_F_NONBLOCK) != 3) return 130;' : descriptors ? 'char a, b, c; if (read(3, &a, 1) != 1 || read(4, &b, 1) != 1 || read(8, &c, 1) != 1 || a != \'b\' || b != \'c\' || c != \'a\') return 72;' : ""}
	${descriptors ? 'int alias = dup(4); if (alias < 0 || fcntl(alias, F_GETFL) != fcntl(3, F_GETFL) || (fcntl(8, F_GETFL) & O_ACCMODE) != ' + (socket ? 'O_RDWR' : 'O_RDONLY') + ') return 74; close(alias);' : ""}
	${socket ? (duplex ? 'char peer[5]; if (read(14, peer, 4) != 4 || memcmp(peer, "peer", 4)) return 100; ' + (messages ? 'int passed[2] = {9, 4}; char value = 77, control[CMSG_SPACE(sizeof(passed))] = {0}; struct iovec vector = {&value, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed)); memcpy(CMSG_DATA(header), passed, sizeof(passed)); if (sendmsg(3, &message, MSG_NOSIGNAL) != 1) return 126; close(9); int received[2]; for (int peek = 1; peek >= 0; peek--) { message.msg_controllen = sizeof(control); if (recvmsg(14, &message, MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0)) != 1 || value != 77) return 127; memcpy(received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(received)); if (fcntl(received[0], F_GETFD) != FD_CLOEXEC || recv(received[1], &value, 1, MSG_PEEK) != 1 || value != 88) return 128; if (!peek && (read(received[0], &value, 1) != 1 || value != 98)) return 129; close(received[0]); close(received[1]); value = 77; } ' : '') + (io ? 'char absent; if (recv(14, &absent, 1, MSG_DONTWAIT) != -1 || errno != EAGAIN) return 115; struct pollfd empty = {.fd = 14, .events = POLLIN}; if (poll(&empty, 1, 0) != 0 || empty.revents) return 116; ' : '') + (concurrent ? 'pid_t child = fork(); if (child < 0) return 103; if (!child) { size_t length = 0; while (length < 5) { ssize_t size = read(14, peer + length, 5 - length); if (size <= 0) return 104; length += (size_t)size; } return memcmp(peer, "reply", 5) || write(14, "back", 4) != 4 || shutdown(14, SHUT_WR); } ' : '') : '') + 'if (write(3, "re", 2) != 2 || write(4, "ply", 3) != 3 || ' + (lifetime ? '0' : 'shutdown(4, SHUT_WR)') + ') return 94;' + (duplex ? concurrent ? ' int status; if (waitpid(child, &status, 0) != child || status) return 105;' : ' if (read(14, peer, 5) != 5 || memcmp(peer, "reply", 5) || write(14, "back", 4) != 4 || ' + (lifetime ? '0' : 'shutdown(14, SHUT_WR)') + ') return 101;' : '') : pipeOutput ? shortWrite ? 'char payload[8192]; memset(payload, 90, sizeof(payload)); if (write(14, payload, sizeof(payload)) != 4096 || write(14, payload, 1) != -1 || errno != EAGAIN) return 120;' : 'if (write(14, "re", 2) != 2 || write(14, "ply", 3) != 3) return 94;' : ""}
	${io ? 'ready = (struct pollfd){.fd = 14, .events = POLLIN | POLLOUT | POLLRDHUP}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT | POLLRDHUP | POLLHUP)) return 117; if (send(3, "!", 1, MSG_NOSIGNAL) != -1 || errno != EPIPE) return 124;' : ""}
	${lifetime ? 'char rest[8]; if (read(3, rest, 7) != 7 || memcmp(rest, "XYZback", 7)) return 107; close(0); close(3); close(4); close(8); if (read(14, rest, 1) != 0) return 108;' : ""}
	${nullDevice ? 'char byte; if (write(6, "discard", 7) != 7 || read(7, &byte, 1) != 0 || lseek(6, 100, SEEK_SET) != 0 || fcntl(6, F_GETFL) != fcntl(7, F_GETFL)) return 75;' : ""}
	${mode === "native-status" || mixed ? 'if (fcntl(3, F_SETFL, fcntl(3, F_GETFL) | O_NONBLOCK) || fcntl(6, F_SETFL, fcntl(6, F_GETFL) | O_APPEND | O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK)) return 77;' : ""}
	${mode === "native-null-stdin" ? 'if (read(0, &byte, 1) != 0) return 76;' : ""}
	${directory ? 'if (fchdir(10) || fcntl(10, F_GETFL) != fcntl(11, F_GETFL)) return 78;' : ""}
	${enumerate ? `char entries[512], repeated[512]; off_t start = lseek(10, 0, SEEK_CUR);
	if (start <= 0 || lseek(11, 0, SEEK_CUR) != start || lseek(13, 0, SEEK_CUR) != 0) return 161;
	memset(entries, 90, sizeof(entries)); memset(repeated, 90, sizeof(repeated));
	ssize_t length = syscall(SYS_getdents64, 10, entries, 48);
	if (length <= 0 || lseek(13, start, SEEK_SET) != start || syscall(SYS_getdents64, 13, repeated, 48) != length || memcmp(entries, repeated, (size_t)length)) return 162;
	if (lseek(11, 0, SEEK_SET) != 0) return 163;
	pid_t reader = fork(); if (reader < 0) return 164;
	if (!reader) return syscall(SYS_getdents64, 11, entries, 24) != 24;
	int waited; if (waitpid(reader, &waited, 0) != reader || waited || lseek(10, 0, SEEK_CUR) != start) return 165;
	if (lseek(13, 0, SEEK_SET) != 0 || lseek(10, 0, SEEK_SET) != 0 || syscall(SYS_getdents, 13, repeated, 24) != 24 || syscall(SYS_getdents64, 10, entries, 24) != 24 || memcmp(entries, repeated, 18) || strcmp(entries + 19, repeated + 18) || entries[18] != repeated[23]) return 166;
	const char *captured = getenv("BOUND_DIRECTORY_ENTRY"); if (!captured || strlen(captured) != 38) return 176;
	for (int index = 0; index < 19; index++) { char hex[] = {captured[2 * index], captured[2 * index + 1], 0}; if ((unsigned char)entries[index] != strtoul(hex, 0, 16)) return 177; }
	if (lseek(13, 0, SEEK_SET) != 0 || lseek(10, 0, SEEK_SET) != 0) return 167;
	int expected = count_directory(13); if (expected <= 0 || lseek(13, start, SEEK_SET) != start) return 171;
	reader = fork(); if (reader < 0) return 172; if (!reader) return count_directory(10);
	int consumed = count_directory(11); if (waitpid(reader, &waited, 0) != reader || !WIFEXITED(waited) || consumed < 0 || WEXITSTATUS(waited) + consumed != expected) return 173;
	if (syscall(SYS_getdents64, 10, entries, sizeof(entries)) != 0 || lseek(10, 0, SEEK_CUR) <= 9007199254740991LL || lseek(13, 0, SEEK_CUR) != start || fcntl(6, F_GETFL) != (O_PATH | O_DIRECTORY)) return 174;
	` : ""}
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
#include <sys/syscall.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include <sys/eventfd.h>
#include <sched.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
static void *duplicate(void *unused) { (void)unused; if (dup2(3, 9) < 0) _exit(73); return 0; }
${rightsHelpers}
int main(int argc, char **argv) {
	if (argc != 2) return 74;
	pthread_t worker; if (pthread_create(&worker, 0, duplicate, 0) || pthread_join(worker, 0)) return 75;
	${mode === "native-unshare" ? "if (unshare(CLONE_FILES)) return 77;" : ""}
	${opath ? 'int directory = open(".", O_PATH | O_DIRECTORY), file = open("fd.txt", O_PATH);\n\tif (directory < 0 || file < 0 || dup2(directory, 10) < 0 || dup2(directory, 11) < 0 || dup2(file, 12) < 0) return 79;\n\tclose(directory); if (file != 12) close(file);' : ""}
	${pipe ? 'int fds[2]; if (pipe2(fds, O_CLOEXEC) || write(fds[1], "bcaXYZ", 6) != 6) return 80; ' + (mode.includes("pipe-live") ? '' : 'close(fds[1]); ') + 'if (dup2(fds[0], 0) < 0) return 81; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0) return 82; int reopened = open("/proc/self/fd/0", O_RDONLY); if (reopened < 0 || dup2(reopened, 8) < 0) return 83; if (reopened != 8) close(reopened);' : ""}
	${mixed ? 'int writable = open("anonymous", O_RDWR | O_CREAT | O_EXCL, 0600); if (writable < 0 || unlink("anonymous") || write(writable, "abcdef", 6) != 6 || lseek(writable, 0, SEEK_SET) != 0 || dup2(writable, 16) < 0 || dup2(writable, 17) < 0) return 87; close(writable); int reader = open("/proc/self/fd/16", O_RDONLY); if (reader < 0 || dup2(reader, 18) < 0) return 88; close(reader);' : ""}
	${socket ? 'int fds[2]; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) || ' + (prequeued ? '0' : 'write(fds[1], "bcaXYZ", 6) != 6') + ' || dup2(fds[0], 0) < 0) return 95; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0 || dup2(0, 8) < 0) return 96;' + (mode === "native-socket-half-closed" ? ' if (shutdown(fds[1], SHUT_WR)) return 97;' : '') : ""}
	${queueCounter ? 'int counter = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC); if (counter < 0 || dup2(counter, 20) < 0 || dup2(counter, 21) < 0) return 160; close(counter);' : ""}
	${orphan ? `int queue[2], channel[2], counter = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC);
	if (counter < 0 || pipe2(queue, O_CLOEXEC | O_NONBLOCK) || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, channel)) return 198;
	int rights[] = {9, 9, counter, queue[0], queue[1], channel[0], 3};
	if (pass(channel[0], "C", 1, &channel[0], 1) || pass(channel[1], "D", 1, &channel[1], 1) ||
		pass(fds[1], "qbcaXYZ", 7, rights, 7) || pass(fds[1], "T", 1, rights, 1)) return 199;
	close(counter); close(queue[0]); close(queue[1]); close(channel[0]); close(channel[1]);
	(void)take;` : ""}
	${prequeued && !orphan ? 'int passed[2] = {' + (queueCounter ? '20' : '9') + ', 3}; char control[CMSG_SPACE(sizeof(passed))] = {0}; struct iovec vector = {"qbcaXYZ", 7}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed)); memcpy(CMSG_DATA(header), passed, sizeof(passed)); ' + (segmented ? 'vector.iov_base = "qI"; vector.iov_len = 2; if (write(fds[1], "L", 1) != 1 || sendmsg(fds[1], &message, 0) != 2) return 185; vector.iov_len = 1; vector.iov_base = "t"; if (sendmsg(fds[1], &message, 0) != 1 || write(fds[1], "bcaXYZ", 6) != 6) return 186;' : (prefixed ? 'if (write(fds[1], "L", 1) != 1) return 186; ' : '') + 'if (sendmsg(fds[1], &message, 0) != 7) return 157;') : ""}
	${duplex ? 'if (dup2(fds[1], 14) < 0 || write(0, "peer", 4) != 4) return 100;' : ""}
	${orphan ? 'if (close(9)) return 188;' : ""}
	${pipeOutput ? 'int output[2]; if (pipe2(output, O_CLOEXEC) || dup2(output[1], 14) < 0 || dup3(output[0], 15, O_CLOEXEC) < 0) return 95; ' + (shortWrite ? 'if (fcntl(14, F_SETPIPE_SZ, 4096) != 4096 || fcntl(14, F_SETFL, O_NONBLOCK)) return 121; ' : '') + (queuedOutput ? 'if (write(output[1], "start", 5) != 5) return 112; ' : '') + 'close(output[0]); close(output[1]);' : ""}
	${counter ? 'int counter = eventfd(5, EFD_CLOEXEC | EFD_NONBLOCK' + (semaphore ? ' | EFD_SEMAPHORE' : '') + '), full = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); uint64_t count = UINT64_MAX - 2; if (counter < 0 || full < 0 || write(full, &count, 8) != 8 || dup2(counter, 20) < 0 || dup2(counter, 21) < 0 || dup2(full, 22) < 0) return 149; close(counter); close(full);' : ""}
	${lifetime || exporting ? 'int gate[2]; if (pipe2(gate, O_CLOEXEC)) return 109;' : ""}
	puts(argv[1]); fflush(stdout);
	${enumerate ? 'int anchor = open(".", O_PATH | O_DIRECTORY); if (anchor < 0 || dup2(anchor, 6) < 0) return 175; if (anchor != 6) close(anchor); char entries[24]; int independent = open(".", O_RDONLY | O_DIRECTORY); if (independent < 0 || dup2(independent, 13) < 0 || syscall(SYS_getdents64, 10, entries, sizeof(entries)) != 24) return 168; if (independent != 13) close(independent); char captured[39]; for (int index = 0; index < 19; index++) snprintf(captured + 2 * index, 3, "%02x", (unsigned char)entries[index]); if (setenv("BOUND_DIRECTORY_ENTRY", captured, 1)) return 178; pid_t reader = fork(); if (reader < 0) return 169; if (reader) { int waited; if (waitpid(reader, &waited, 0) != reader || waited || lseek(10, 0, SEEK_CUR) <= 9007199254740991LL || lseek(11, 0, SEEK_CUR) != lseek(10, 0, SEEK_CUR) || lseek(13, 0, SEEK_CUR) <= 0 || lseek(13, 0, SEEK_CUR) == lseek(10, 0, SEEK_CUR)) return 170; return 0; }' : ""}
	${mode === "native-pipe-live-transfer" ? `int sockets[2], passed[] = {0, 3, 4, 8}; char byte = 'x', control[CMSG_SPACE(sizeof(passed))] = {0};
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets)) return 89;
	struct iovec vector = {&byte, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed));
	memcpy(CMSG_DATA(header), passed, sizeof(passed)); if (sendmsg(sockets[0], &message, 0) != 1) return 90; close(sockets[0]);` : ""}
	${counter ? 'pid_t child = fork(); if (child < 0) return 150; if (child) { int status; if (waitpid(child, &status, 0) != child || status) return 151; uint64_t count, total = 0; while (read(21, &count, 8) == 8) total += count; if (errno != EAGAIN || total != ' + (semaphore ? 11 : 7) + ' || !(fcntl(20, F_GETFL) & O_NONBLOCK) || read(22, &count, 8) != -1 || errno != EAGAIN) return 152; return 0; }' : ""}
	${pipe || socket ? 'pid_t child = fork(); if (child < 0) return 84; ' + (exporting ? 'if (child) { close(0); close(3); close(4); close(8); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 133; close(gate[1]); int status; if (waitpid(child, &status, 0) != child || status) return 134; char text[6], control[CMSG_SPACE(sizeof(int))] = {0}; struct iovec vector = {text, 5}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; if (recvmsg(14, &message, MSG_CMSG_CLOEXEC) != 5 || memcmp(text, "peerx", 5)) return 135; int received; memcpy(&received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(received)); if (!(fcntl(received, F_GETFL) & O_NONBLOCK) || read(received, text, 6) != 6 || memcmp(text, "bcaXYZ", 6)) return 136; close(received); ' + (queueCounter ? 'uint64_t count; if (read(20, &count, 8) != 8 || count != 7) return 161; ' : '') + 'return 0; } ' : '') + (lifetime ? 'if (child) { close(0); close(3); close(4); close(8); close(14); close(fds[1]); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 110; close(gate[1]); int status; return waitpid(child, &status, 0) != child || status; } ' : '') + 'if (child) { int status; char tail[3]; if (waitpid(child, &status, 0) != child || status || ' + (mixed ? 'lseek(16, 0, SEEK_CUR) != 4 || lseek(18, 0, SEEK_CUR) != 0 || pread(18, tail, 1, 2) != 1 || tail[0] != 81 || !(fcntl(3, F_GETFL) & O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK) || ' : '') + 'read(3, tail, 1) != 1 || read(4, tail + 1, 1) != 1 || read(8, tail + 2, 1) != 1 || tail[0] != \'X\' || tail[1] != \'Y\' || tail[2] != \'Z\') return 85; ' + (orphan ? 'int remaining; if (take(3, 84, &remaining, 1, 0) || lseek(remaining, 0, SEEK_CUR) != 2 || !(fcntl(remaining, F_GETFL) & O_NONBLOCK)) return 200; close(remaining); ' : '') + (socket || pipeOutput ? 'char reply[6]; ' + (shortWrite ? 'char head[4091]; if (read(15, head, sizeof(head)) != sizeof(head)) return 122; for (unsigned index = 0; index < sizeof(head); index++) if (head[index] != 90) return 123; ' : '') + (transferIO ? 'if (read(15, reply, 6) != 6 || memcmp(reply, "bcabca", 6)) return 131; ' : '') + (queuedOutput ? 'if (read(15, reply, 5) != 5 || memcmp(reply, "start", 5)) return 113; ' : '') + (duplex ? 'if (read(0, reply, 4) != 4 || memcmp(reply, "back", 4) || read(0, reply, 1) != 0) return 102; ' : 'if (read(' + (socket ? 'fds[1]' : '15') + ', reply, 5) != 5 || memcmp(reply, "' + (shortWrite ? 'ZZZZZ' : 'reply') + '", 5)) return 98; ') + (socket ? 'if (read(fds[1], reply, 1) != 0) return 99; ' : '') : '') + 'return 0; }' : ""}
	${lifetime || exporting ? 'char ready; close(gate[1]); if (read(gate[0], &ready, 1) != 1) return 111; close(gate[0]);' : ""}
	${exporting ? 'close(14);' : ""}
	${mode === "native-pipe-live-transfer" ? `for (unsigned index = 0; index < 4; index++) close(passed[index]);
	if (recvmsg(sockets[1], &message, MSG_CMSG_CLOEXEC) != 1) return 91;
	int received[4], saved[4]; memcpy(received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(received));
	for (unsigned index = 0; index < 4; index++) { saved[index] = fcntl(received[index], F_DUPFD_CLOEXEC, 100); if (saved[index] < 0) return 92; close(received[index]); }
	for (unsigned index = 0; index < 4; index++) { if (dup2(saved[index], passed[index]) < 0) return 93; close(saved[index]); }
	close(sockets[1]);` : ""}
	char *command[] = {"bound-name", "private argument", 0}; execv("./worker", command); return 76;
}
`);
				await compileBenchmarkHelper(fixture.workspace, { source: "fd-launch.c", output: "fd-launch", arguments: ["-pthread", "-Werror"] });
			}
			await commitBenchmarkFixture(fixture.workspace, "Bound process invocation");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const command = (descriptors ? "exec 3<fd.txt; exec 4<&3; exec 8<fd.txt; IFS= read -r -N 1 discarded <&3; " : "") +
				(directory ? "exec 10<.; exec 11<&10; " : "") +
				(nullDevice ? (mode === "native-null-stdin" ? "exec 0<>/dev/null; exec 6<&0; " : "exec 6<>/dev/null; ") + "exec 7<&6; " : "") +
				"export BOUND_SECRET='private value'; " + (launcher ? "exec fd-launch parent" : "printf 'parent\\n'; exec -a bound-name worker 'private argument'") +
				(mode === "native-merged" ? " 2>&1" : mode === "native-closed-input" ? " 0<&-" : "");
			const route = await fixture.prepareActorReplay(!native);
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
			if (discovery) {
				const coldScope = { ...scope, turnID: "cold-queue" };
				await start(coldScope.turnID);
				const output = await host.execute(call(coldScope.turnID, command), undefined, () => fixture.coordinator.runWith({
					execute: request => route.executor.execute({ ...request, scope: coldScope }) }, () => fixture.tool.execute(coldScope.turnID, { command })));
				expect(output.content).toEqual([{ type: "text", text: "parent\nafter\n" }]);
				await host.finishTurn(coldScope.turnID);
				const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
				expect(registry.bindings(coldScope).length).toBeGreaterThan(fixture.backend.executionBindings(coldScope).length);
			}
			await start("seed");
			if (!native) {
				await host.previewActorCall(call("seed", command));
				await expect.poll(() => events.some(event => event.type === "candidate" && event.turnID === "seed" &&
					event.candidate.origin === "actor_preview" && event.state.status === "succeeded"), { timeout: 5000 }).toBe(true);
			}

			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : [])]);
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
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : []), command]);
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
				.map(event => event.type === "candidate" ? [event.candidate.kind, event.state.status] : event.type === "operation_prediction" ? event.settlement : undefined), { timeout: 5000 }).toContainEqual(["operation", "succeeded"]).catch(error => {
				throw new Error(JSON.stringify({ metrics: fixture.backend.metrics(), errors: errors.mock.calls.map(call => call[1]) }), { cause: error });
			});
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
			if (segmented || orphan) expect(fixture.backend.actorMetrics().hits).toBe(1);
			if (orphan) expect(publishing.mock.calls.some(([certificate]) => certificate.prototype.inheritedFDs.some(fd => fd.installed === false))).toBe(true);
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
			await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : []), command, changedParent]);
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

	test.for((["git", "overlayfs"] as const).flatMap(driver => ["read", "write", "locks", "locks-close", "locks-named", "path-write", "unlinked", "hardlink-read", "hardlink-write", "hardlink-path-write", "hardlink-rename", "hardlink-split", "hardlink-unlink", "hardlink-detached"].map(mode => [driver, mode] as const)))("learns and adopts regular OFDs with shared positions and predecessor inputs (%s, %s)", { timeout: 30_000 }, async ([driver, mode], { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		if (mode === "locks-named" && await filesystem.readlink("/proc/self/ns/pid") === "pid:[4026531836]") return skip("nested PID namespace visibility case");
		const fixture = await createLinuxProcessBenchmark("pi-fd-binding-", driver);
		const hardlink = mode.startsWith("hardlink"), pathWrite = mode.endsWith("path-write"), locks = mode.startsWith("locks");
		const memory = locks && mode !== "locks-named", closing = mode === "locks-close";
		const writable = !pathWrite && mode !== "read" && mode !== "hardlink-read";
		try {
			const status = await fixture.backend.check(true);
			if (status.state !== "ready") throw new Error(status.detail);
			const input = path.join(fixture.workspace, "input.txt");
			const reset = async () => {
				if (hardlink) for (const name of ["input.txt", "alias.txt", "moved.txt"]) await rm(path.join(fixture.workspace, name), { force: true });
				await writeFile(input, "abcdef");
				if (hardlink) await filesystem.link(input, path.join(fixture.workspace, "alias.txt"));
			};
			await reset();
			const lockHelpers = `#define _GNU_SOURCE
#include <errno.h>
#include <sys/file.h>
#include <fcntl.h>
static int lock(int fd,int type,int start,int length) { struct flock range={.l_type=type,.l_whence=SEEK_SET,.l_start=start,.l_len=length};return fcntl(fd,F_OFD_SETLK,&range); }
static int query(int fd,int start,int length,int type,int foundStart,int foundLength) {
	struct flock range={.l_type=F_WRLCK,.l_whence=SEEK_SET,.l_start=start,.l_len=length};
	return fcntl(fd,F_OFD_GETLK,&range) || range.l_type!=type || range.l_start!=foundStart || range.l_len!=foundLength || (type!=F_UNLCK && range.l_pid!=-1);
}
`;
			if (locks) {
				await writeFile(path.join(fixture.workspace, "locker.c"), lockHelpers + `int main(int argc,char **argv) {
	if(argc!=2)return 70;
	if(argv[1][0]=='i')return lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB);
	return query(8,2,4,F_UNLCK,2,4) || query(8,10,3,F_RDLCK,10,3) || flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN;
}`);
				await compileBenchmarkHelper(fixture.workspace, { source: "locker.c", output: "locker" });
			}
			await writeFile(path.join(fixture.workspace, "worker.c"), (locks ? lockHelpers : "") + `#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
int main(void) {
	char bytes[6] = {0, 0, 0, ':', 0, '\\n'};
	if (read(3, bytes, 2) != 2 || read(4, bytes + 2, 1) != 1 || read(8, bytes + 4, 1) != 1) return 71;
	${locks ? `if(query(8,0,1,F_UNLCK,0,1) || query(8,2,1,F_WRLCK,2,4) || lock(4,F_UNLCK,3,1) || query(8,3,1,F_UNLCK,3,1))return 74;
	if(lock(8,F_WRLCK,2,1)!=-1 || errno!=EAGAIN || lock(8,F_WRLCK,3,1) || lock(3,F_RDLCK,3,1)!=-1 || errno!=EAGAIN || lock(8,F_UNLCK,0,0) || lock(4,F_WRLCK,3,1))return 75;
	if(flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN || flock(4,LOCK_UN) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN) || flock(3,LOCK_EX|LOCK_NB) || lock(4,F_UNLCK,0,0) || lock(4,F_RDLCK,10,3))return 76;` : ""}
	${mode === "hardlink-rename" ? 'if (rename("input.txt", "moved.txt")) return 73;' : mode === "hardlink-unlink" ? 'if (unlink("input.txt")) return 73;' : mode === "hardlink-split" ? 'if (unlink("input.txt")) return 73; int replacement = open("input.txt", O_WRONLY | O_CREAT, 0600); if (replacement < 0 || write(replacement, "new", 3) != 3) return 73; close(replacement);' : ""}
	${mode === "hardlink-detached" ? 'if (unlink("input.txt") || unlink("alias.txt")) return 73;' : ""}
	${writable ? 'if (write(4, "XY", 2) != 2) return 72;' : pathWrite ? `int fd = open("${hardlink ? "alias.txt" : "input.txt"}", O_WRONLY); if (pwrite(fd, "Z", 1, 2) != 1) return 72; close(fd);` : ""}
	${closing ? `int gate[2];char byte;if(pipe2(gate,O_CLOEXEC))return 77;pid_t child=fork();if(child<0)return 77;
	if(!child){close(gate[1]);if(read(gate[0],&byte,1)!=1)_exit(78);close(gate[0]);close(3);close(4);_exit(0);}
	close(gate[0]);if(close(3) || close(4) || query(8,10,3,F_RDLCK,10,3) || flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN)return 77;
	if(write(gate[1],"x",1)!=1 || close(gate[1]))return 77;int status;
	if(waitpid(child,&status,0)!=child || status || query(8,10,3,F_UNLCK,10,3) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN))return 77;` : ""}
	for (volatile unsigned long i = 0; i < 50000000ul; ++i) {}
	return write(1, bytes, sizeof(bytes)) != sizeof(bytes);
}
`);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
			const body = `IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; locker init; printf '%s\\n' "$1"; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"; locker check`;
			if (memory) {
				await writeFile(path.join(fixture.workspace, "fd-memory.c"), lockHelpers + `
#include <sys/mman.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
int main(int argc,char **argv) {
	char bytes[6];int input=open("input.txt",O_RDONLY);if(argc!=2 || input<0 || read(input,bytes,6)!=6)return 80;close(input);
	int fd=memfd_create("lock-input",0);if(fd<0 || write(fd,bytes,6)!=6 || lseek(fd,0,SEEK_SET)!=0 || dup2(fd,3)<0)return 81;
	if(fd!=3)close(fd);if(dup2(3,4)<0)return 82;fd=open("/proc/self/fd/3",O_RDWR);if(fd<0 || dup2(fd,8)<0)return 83;if(fd!=8)close(fd);
	${closing ? `int gate[2];char byte;if(pipe2(gate,O_CLOEXEC) || lseek(3,1,SEEK_SET)!=1 || lseek(8,1,SEEK_SET)!=1 || lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB))return 84;
	puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 85;
	if(!child){close(gate[1]);if(read(gate[0],&byte,1)!=1)_exit(86);close(gate[0]);execl("./worker","worker",(char *)0);_exit(86);}
	close(gate[0]);close(3);close(4);if(write(gate[1],"x",1)!=1)return 86;close(gate[1]);int status;
	if(waitpid(child,&status,0)!=child || status || read(8,&byte,1)!=1 || query(8,10,3,F_UNLCK,10,3) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN))return 87;
	printf("tail::%c\\n",byte);return 0;` : `execl("/bin/bash","bash","-c",${JSON.stringify(body)},"fd-memory",argv[1],(char *)0);return 84;`}
}`);
				await compileBenchmarkHelper(fixture.workspace, { source: "fd-memory.c", output: "fd-memory" });
			}
			await commitBenchmarkFixture(fixture.workspace, "Inherited FD binding");
			await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "fd-binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const route = await fixture.prepareActorReplay();
			if (!("executor" in route)) throw new Error(route.detail);
			const command = memory ? "fd-memory parent" : `exec 3<${writable ? ">" : ""}input.txt; exec 4<&3; exec 8<${locks ? ">" : ""}${hardlink ? "alias.txt" : "input.txt"}; IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; ` +
				(mode === "unlinked" ? "rm input.txt; " : "") +
				(locks ? "locker init; " : "") + `printf 'parent\\n'; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"` + (locks ? "; locker check" : "");
			const execute = async (scope: { sessionID: string; turnID: string }, command: string) => {
				let output = "";
				const result = await route.executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment, scope,
					timeout: 10, onData: data => { output += data.toString(); } });
				expect(result).toEqual({ exitCode: 0 }); return output;
			};
			const tail = writable ? ":c" : pathWrite ? "e:Z" : "e:c";
			let binding: ProcessExecutionBinding | undefined;
			const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable?: string }>;
			expect(await fixture.backend.observeBindings(scope, () => execute(scope, command), bindings => {
				binding = bindings.find(candidate => path.basename(handoffs.resolveBinding(candidate, scope)?.executable ?? "") === "worker");
			}, true)).toBe(`parent\nbcd:b\ntail:${tail}\n`);
			expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
			const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace,
				environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
			for (const changed of [false, true]) {
				await reset();
				const branch = await fixture.workspaceSandbox.fork({ cwd: fixture.workspace, driver,
					action: buildPiActionKey("bash", { command }, fixture.workspace)!, execute: async workspace => {
					const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
					try {
						const result = await session.executeBinding(binding!);
						expect(result.exit).toEqual({ kind: "code", code: 0 });
						expect(result.output.map(({ data }) => data.toString()).join("")).toBe("bcd:b\n");
						await session.seal([]);
						expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: "valid" });
						const hits = fixture.backend.actorMetrics().hits;
						if (changed) await writeFile(input, "uvwxyz");
						expect(await execute(later, command.replace("parent", "changed-parent"))).toBe(changed
							? `changed-parent\nvwx:v\ntail:${writable ? ":w" : pathWrite ? "y:Z" : "y:w"}\n` : `changed-parent\nbcd:b\ntail:${tail}\n`);
						if (mode === "hardlink-detached") { expect(existsSync(input)).toBe(false); expect(existsSync(path.join(fixture.workspace, "alias.txt"))).toBe(false); }
						else if (hardlink) {
							const alias = path.join(fixture.workspace, "alias.txt");
							expect(await readFile(alias, "utf8")).toBe(writable ? `${changed ? "uvwx" : "abcd"}XY` : pathWrite ? `${changed ? "uv" : "ab"}Z${changed ? "xyz" : "def"}` : changed ? "uvwxyz" : "abcdef");
							if (mode === "hardlink-rename" || mode === "hardlink-unlink") expect(existsSync(input)).toBe(false);
							if (mode === "hardlink-rename") expect((await stat(path.join(fixture.workspace, "moved.txt"))).ino).toBe((await stat(alias)).ino);
							if (mode === "hardlink-split") expect(await readFile(input, "utf8")).toBe("new");
						}
						expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() }))
							.toBe(hits + Number(!changed && mode !== "locks-named"));
					} finally { await session.close(); }
					return { output: { result: { content: [], details: {} }, isError: false }, changes: [] };
				} });
				await branch.dispose();
			}
		} finally { await fixture.dispose(); }
	});

	test.for(["SOCK_DGRAM", "SOCK_SEQPACKET", "SOCK_DGRAM:close", "SOCK_SEQPACKET:close"])("adopts packet boundaries, empty messages, truncation and queued OFDs (%s)", { timeout: 30_000 }, async (scenario, { skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const [type, closing] = scenario.split(":");
		const fixture = await createLinuxProcessBenchmark("pi-packet-binding-");
		try {
			await writeFile(path.join(fixture.workspace, "input.txt"), "abcdef");
			const helpers = `#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/wait.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
static int pass(const char *bytes, int length) {
	char control[CMSG_SPACE(sizeof(int))] = {0}; struct iovec vector = {(void *)bytes, length};
	struct msghdr message = {.msg_iov=&vector,.msg_iovlen=1,.msg_control=control,.msg_controllen=sizeof(control)};
	struct cmsghdr *header=CMSG_FIRSTHDR(&message);header->cmsg_level=SOL_SOCKET;header->cmsg_type=SCM_RIGHTS;header->cmsg_len=CMSG_LEN(sizeof(int));
	int fd=9;memcpy(CMSG_DATA(header),&fd,sizeof(fd));return sendmsg(20,&message,MSG_NOSIGNAL)!=length;
}
static int take(int length,int peek,int truncated,char expected) {
	char bytes[4],control[CMSG_SPACE(sizeof(int))];struct iovec vector={bytes,length ? (size_t)length : 2};
	struct msghdr message={.msg_iov=&vector,.msg_iovlen=1,.msg_control=control,.msg_controllen=sizeof(control)};
	if(recvmsg(22,&message,MSG_CMSG_CLOEXEC|(peek?MSG_PEEK:0))!=length || !!(message.msg_flags&MSG_TRUNC)!=truncated || (message.msg_flags&MSG_CTRUNC))return 1;
	if((length==2 && memcmp(bytes,"pq",2)) || (length==4 && memcmp(bytes,"tail",4)))return 1;
	struct cmsghdr *header=CMSG_FIRSTHDR(&message);int fd;char byte;
	if(!header || header->cmsg_level!=SOL_SOCKET || header->cmsg_type!=SCM_RIGHTS || header->cmsg_len!=CMSG_LEN(sizeof(fd)))return 2;
	memcpy(&fd,CMSG_DATA(header),sizeof(fd));int failed= !peek && (read(fd,&byte,1)!=1 || byte!=expected);
	close(fd);return failed;
}
`;
			await writeFile(path.join(fixture.workspace, "worker.c"), helpers + `int main(void) {
	char bytes[8];struct pollfd ready={.fd=22,.events=POLLIN};
	if(pass("",0) || read(22,bytes,0)!=0 || poll(&ready,1,0)!=1 || take(0,1,0,0) || take(0,0,0,'a'))return 70;
	if(write(21,"abcdef",6)!=6 || read(22,bytes,2)!=2 || memcmp(bytes,"ab",2))return 71;
	if(send(20,"XY",2,0)!=2 || recv(22,bytes,8,0)!=2 || memcmp(bytes,"XY",2))return 72;
	struct iovec vector={bytes,0};struct msghdr message={.msg_iov=&vector,.msg_iovlen=1};
	if(send(20,"truncate",8,0)!=8 || recv(22,bytes,2,MSG_PEEK|MSG_TRUNC)!=8 || memcmp(bytes,"tr",2) || recvmsg(22,&message,MSG_TRUNC)!=8 || !(message.msg_flags&MSG_TRUNC))return 74;
	if(pass("pqrs",4) || take(2,0,1,'b') || recv(22,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN || pass("tail",4))return 73;
	if(shutdown(20,SHUT_WR) || send(20,"x",1,MSG_NOSIGNAL)!=-1 || errno!=EPIPE || send(22,"back",4,0)!=4 || recv(20,bytes,8,0)!=4 || memcmp(bytes,"back",4) || shutdown(20,SHUT_RD))return 75;
	if(${type === "SOCK_DGRAM" ? "recv(20,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(20,bytes,1,0)!=0"})return 75;
	${closing ? "if(close(20) || close(21))return 76;" : ""}
	ready.revents=0;if(poll(&ready,1,0)!=1 || ready.revents!=${type === "SOCK_DGRAM" ? "POLLIN" : "(POLLIN|POLLHUP)"})return 77;
	for(volatile unsigned long i=0;i<200000000ul;i++){}
	return write(1,"packets\\n",8)!=8;
}
`);
			await writeFile(path.join(fixture.workspace, "fd-launch.c"), helpers + `int main(int argc,char **argv) {
	if(argc!=2)return 80;int pair[2],gate[2];char byte;
	if(socketpair(AF_UNIX,${type}|SOCK_CLOEXEC|SOCK_NONBLOCK,0,pair) || dup2(pair[0],20)<0 || dup2(pair[0],21)<0 || dup2(pair[1],22)<0)return 81;
	close(pair[0]);close(pair[1]);int fd=open("input.txt",O_RDONLY);if(fd<0 || dup2(fd,9)<0)return 82;if(fd!=9)close(fd);
	if(pipe2(gate,O_CLOEXEC))return 83;puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 83;
	if(!child){close(gate[1]);if(read(gate[0],&byte,1)!=1)_exit(84);close(gate[0]);execl("./worker","worker",(char *)0);_exit(84);}
	close(gate[0]);${closing ? "close(20);close(21);" : ""}if(write(gate[1],"x",1)!=1)return 84;close(gate[1]);
	int status;if(waitpid(child,&status,0)!=child || status || lseek(9,0,SEEK_CUR)!=2 || take(4,0,0,'c') || lseek(9,0,SEEK_CUR)!=3)return 85;
	if(${type === "SOCK_DGRAM" ? "recv(22,&byte,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(22,&byte,1,0)!=0"})return 86;
	return 0;
}
`);
			for (const name of ["worker", "fd-launch"]) await compileBenchmarkHelper(fixture.workspace, { source: `${name}.c`, output: name });
			await commitBenchmarkFixture(fixture.workspace, "Packet resource graph"); await prepareLinuxProcessReuse(fixture);
			const scope = { sessionID: "packet-binding", turnID: "seed" }, later = { ...scope, turnID: "prepared" };
			const route = await fixture.prepareActorReplay();
			if (!("executor" in route)) throw new Error(route.detail);
			const command = "fd-launch parent";
			const execute = async (scope: { sessionID: string; turnID: string }, command: string) => {
				let output = ""; const result = await route.executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment, scope,
					timeout: 10, onData: data => { output += data.toString(); } }); expect(result).toEqual({ exitCode: 0 }); return output;
			};
			let binding: ProcessExecutionBinding | undefined;
			expect(await fixture.backend.observeBindings(scope, () => execute(scope, command), bindings => { binding = bindings.at(-1); }, true)).toBe("parent\npackets\n");
			expect(fixture.backend.actorMetrics().bypasses, JSON.stringify(fixture.backend.actorMetrics())).toBe(0);
			expect(binding).toBeDefined();
			const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
			await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
				try {
					const result = await session.executeBinding(binding!);expect(result.exit).toEqual({ kind: "code", code: 0 });
					expect(result.output.map(({ data }) => data.toString()).join("")).toBe("packets\n");
					await session.seal([]);expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: "valid" });
					expect(await execute(later, command.replace("parent", "changed-parent"))).toBe("changed-parent\npackets\n");
					expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() })).toBe(1);
				} finally { await session.close(); }
			});
		} finally { await fixture.dispose(); }
	});

	test("owns the entire Actor call when a held child crosses the adoption boundary", async ({ skip }) => {
		if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-held-transaction-"));
		const binary = path.join(root, "helper");
		await compileBenchmarkHelper(root, { source: fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), output: "helper", arguments: ["-pthread", "-Werror"] });
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
				await writeFile(queueManifest, `INPUTS 1 0 ${Number(producer >= 4)}\n0 0 ${producer >= 4 ? 2 : 0} 0 ${Buffer.byteLength(queueImage)} ${producer} ${producer >= 4 ? 212992 : 4096} 0 ${producer === 5 ? 3 : 0} -1 3 1 ${Number(producer >= 4)}\n${queueImage}\n`);
				const child = childProcess.spawn(binary, ["--exec-fds", "12", queueManifest, queueReport, "consumer", "/bin/sh", "-c", "echo $$ >&2; exec /bin/cat"], { detached: true });
				let output = "", childPID = "", completed = false;
				child.stdout.on("data", data => { output += data.toString(); });
				child.stderr.on("data", data => { childPID += data.toString(); });
				const closed = once(child, "close").then(value => { completed = true; return value; });
				try {
					await expect.poll(() => output).toBe("abc");
					if (producer === 1 || producer === 5) {
						expect(await closed).toEqual([0, null]);
					expect(await readFile(queueReport, "utf8")).toMatch(producer === 1 ? /^OFD 1\n0 0 3 / : /^OFD 1\n0 2 0 /);
					} else {
						await new Promise(resolve => setTimeout(resolve, 30));
						expect(completed, "exhausted active input must block, never fabricate EOF").toBe(false);
						process.kill(-child.pid!, "SIGKILL"); await closed;
						await expect.poll(async () => (await readFile(`/proc/${Number(childPID)}/stat`, "utf8").catch(() => "")).split(") ")[1]?.[0] ?? "").not.toMatch(/[RSDT]/);
						expect(await readFile(queueReport, "utf8")).toMatch(/^RUNNING \d+\n$/);
					}
				} finally { if (!completed) { process.kill(-child.pid!, "SIGKILL"); await closed; } }
			}
			await writeFile(queueImage, "");
			const peerImage = path.join(root, "peer-queue"); await writeFile(peerImage, "");
			for (const socket of [false, true]) {
				const readFD = socket ? 4 : 3, writeFD = socket ? 3 : 4;
				const handles = socket ? [[3, 3, 2, 4, 4, queueImage], [4, 4, 2, 4, 3, peerImage], [5, 3, 2, 4, 4, ""]] :
					[[3, 3, 0, 2, -1, queueImage], [4, 4, 1, 3, -1, queueImage], [5, 4, 1, 3, -1, ""]];
				await writeFile(queueManifest, "INPUTS 3 0 1\n" + handles.map(([fd, alias, flags, stream, peer, image]) =>
					`${fd} ${alias} ${flags} 0 ${Buffer.byteLength(String(image))} ${stream} ${socket ? 212992 : 4096} 0 0 ${peer} 0 1 ${Number(socket)}\n${image}\n`).join(""));
				const closed = childProcess.spawnSync("/usr/bin/timeout", ["--kill-after=1", "2", binary, "--exec-fds", "12", queueManifest, queueReport, "lifetime", "/bin/bash", "-c",
					`exec ${writeFD}>&-; printf x >&5; (sleep 0.03; printf y >&5) & exec 5>&-; /bin/cat <&${readFD}; wait`], { encoding: "utf8" });
				expect(closed, "aliases and fork references must drain before last-close EOF").toMatchObject({ status: 0, stdout: "xy", stderr: "" });
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
			const held = (options: Pick<Parameters<LinuxHeldExecBoundary["executor"]>[1], "decide" | "descriptors">, executor = native) => {
				const wrapped = boundary.executor(executor, { sourceRoot: root, realShell: "/bin/bash", ...options });
				let output = "";
				return {
					get output() { return output; },
					execute: (command: string, request: Pick<Parameters<typeof native.execute>[0], "signal" | "scope"> = {}) =>
						wrapped.execute({ command, cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5,
							onData: data => { output += data.toString(); }, ...request }),
				};
			};
			await writeFile(path.join(root, "queue-owners.c"), `#define _GNU_SOURCE
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc != 2) return 1;
	int data[2], channel[2], gate[2];
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, data) || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, channel) || dup2(data[0], 8) < 0 || pipe2(gate, O_CLOEXEC)) return 2;
	char byte = 'x', control[CMSG_SPACE(sizeof(int))] = {0}; struct iovec vector = {&byte, 1};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(int));
	memcpy(CMSG_DATA(header), &data[0], sizeof(int)); if (sendmsg(channel[0], &message, 0) != 1) return 3;
	if (strcmp(argv[1], "queued")) { if (recvmsg(channel[1], &message, MSG_CMSG_CLOEXEC) != 1) return 4; int received; memcpy(&received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(int)); close(received); }
	pid_t child = fork(); if (child < 0) return 5;
	if (!child) { close(gate[1]); if (read(gate[0], &byte, 1) != 1) return 6; close(gate[0]); char *args[] = {"/bin/true", 0}; execv(args[0], args); return 7; }
	close(8); close(data[0]); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 8; close(gate[1]);
	int status; if (waitpid(child, &status, 0) != child || status) return 9;
	struct pollfd ready = {.fd = data[1], .events = POLLIN | POLLRDHUP}; if (poll(&ready, 1, 0) < 0) return 10;
	if (!strcmp(argv[1], "queued")) { if (ready.revents || recvmsg(channel[1], &message, MSG_CMSG_CLOEXEC) != 1) return 11; int received; memcpy(&received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(int)); close(received); }
	return read(data[1], &byte, 1) != 0;
}
`);
			await compileBenchmarkHelper(root, { source: "queue-owners.c", output: "queue-owners" });
			for (const state of ["queued", "consumed"]) {
				let outside: number | undefined;
				const executor = held({ descriptors: true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) === "/usr/bin/true") outside = process.descriptors?.find(({ fd }) => fd === 8)?.outside;
					return { kind: "continue" };
				} });
				expect(await executor.execute(`exec './queue-owners' ${state}`)).toEqual({ exitCode: 0 });
					expect(outside, state).toBe(state === "queued" ? 3 : 0);
				}
			await writeFile(path.join(root, "packet-capture.c"), `#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main(int argc,char **argv) {
	if(argc!=2)return 1;int pair[2],offset=0;char bytes[4];
	if(socketpair(AF_UNIX,atoi(argv[1])|SOCK_NONBLOCK,0,pair) || send(pair[0],"",0,0)!=0 || send(pair[0],"tail",4,0)!=4)return 2;
	pid_t child=fork();if(child<0)return 3;if(!child){execl("/bin/true","true",(char *)0);_exit(4);}
	int status;if(waitpid(child,&status,0)!=child || status)return 5;
	/* If capture peeked the zero-length skb, the first offset peek skips it. */
	if(setsockopt(pair[1],SOL_SOCKET,SO_PEEK_OFF,&offset,sizeof(offset)) || recv(pair[1],bytes,1,MSG_PEEK)!=0 || recv(pair[1],bytes,1,MSG_PEEK)!=1 || bytes[0]!='t')return 6;
	offset=-1;if(setsockopt(pair[1],SOL_SOCKET,SO_PEEK_OFF,&offset,sizeof(offset)) || recv(pair[1],bytes,4,0)!=0 || recv(pair[1],bytes,4,0)!=4 || memcmp(bytes,"tail",4))return 7;
	return 0;
}
`);
			await compileBenchmarkHelper(root, { source: "packet-capture.c", output: "packet-capture" });
			for (const type of [2, 5]) {
				const executor = held({ descriptors: true, decide: async () => ({ kind: "continue" }) });
				expect(await executor.execute(`exec './packet-capture' ${type}`)).toEqual({ exitCode: 0 });
			}
			const compat = path.join(root, "compat32");
			await writeFile(`${compat}.s`, ".global _start\n_start: movl $1, %eax; movl $7, %ebx; int $0x80\n");
			execFileSync("cc", ["-nostdlib", "-m32", "-static", `${compat}.s`, "-o", compat]);
			if (childProcess.spawnSync(compat).status === 7) for (const descriptors of [false, true]) {
				const decide = vi.fn(async () => ({ kind: "continue" as const }));
				const executor = held({ descriptors, decide });
				expect(await executor.execute(`exec '${compat}'`)).toEqual({ exitCode: 7 });
				expect(decide).not.toHaveBeenCalled();
			}
			for (const [redirection, route] of [["", [1, 2]], ["2>&1", [1, 1]], ["3>&1", undefined], ["0<&-", [1, 2]], ["1>/dev/null", undefined]] as const) {
				let inspected = 0;
				let inspection: ReturnType<typeof inspectHeldExecProcess> | undefined;
				const inspecting = held({ decide: async ({ pid }) => {
					inspected++;
					inspection = inspectHeldExecProcess(pid, await filesystem.readlink(`/proc/${pid}/exe`));
					await inspection.catch(() => undefined);
					return { kind: "continue" };
				} });
				expect(await inspecting.execute(`exec /bin/true ${redirection}`)).toEqual({ exitCode: 0 });
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
			await writeFile(manifest, `INPUTS 3 0 0\n0 0 32768 1 ${Buffer.byteLength(input)} 0 0 0 0 -1 3 1 0\n${input}\n3 0 32768 1 0 0 0 0 0 -1 3 1 0\n\n8 8 32768 1 ${Buffer.byteLength(input)} 0 0 0 0 -1 3 1 0\n${input}\n`);
			const reproduced = run("--exec-fds", "12", manifest, report, "fd-worker", "/bin/bash", "-c",
				`IFS= read -r -N 1 a; IFS= read -r -N 1 b <&3; IFS= read -r -N 1 c <&8; printf '%s:%s:%s' "$a" "$b" "$c"; ` +
				`(sleep 0.02; IFS= read -r -N 1 d <&3) & exit 7`);
			expect(reproduced).toMatchObject({ status: 7, stdout: "b:c:b", stderr: "" });
			const reportLines = (await readFile(report, "utf8")).trimEnd().split("\n");
			expect([reportLines[0], ...reportLines.slice(1).map(line => line.split(" ").slice(0, 3).join(" "))])
				.toEqual(["OFD 3", "0 32768 4", "3 32768 4", "8 32768 2"]);
			const external = path.join(root, "external-fd");
			await writeFile(external, `#!/bin/sh\nexec 9<'${input}'\nexec '${binary}' "$@"\n`, { mode: 0o700 });
			for (const shell of [binary, external]) {
				const snapshots: NonNullable<HeldExecProcess["descriptors"]>[] = [], failures: unknown[] = [];
				const inspecting = held({ descriptors: true, decide: async process => {
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
				} }, adaptProcessToolOperations(createLocalBashOperations({ shellPath: shell })));
				expect(await inspecting.execute(`exec 3<'${input}'; exec 4<&3; exec 5<'${input}'; ` +
					`(while IFS= read -r -N 1 value <&4; do :; done) & /bin/true; wait; /bin/true`)).toEqual({ exitCode: 0 });
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
			await compileBenchmarkHelper(root, { source: `${descriptorProbe}.c`, output: "descriptor-probe", arguments: ["-pthread", "-Werror"] });
			for (const mode of ["lock", "export", "rights", "rights-batch", "rights-failed", "rights-unowned", "rights-orphan", "rights-pipe-orphan", "pidfd", "table", "thread", "thread-exec", "shared-table", "shared-exec", "overlap",
				"unshare", "unshare-noop", "range-close", "range-cloexec", "range-invalid"]) {
				let snapshot: HeldExecProcess["descriptors"];
				const split = mode.startsWith("unshare") || mode.startsWith("range-");
				const imported = mode.startsWith("rights") || mode === "pidfd";
				const unknown = mode === "rights-unowned";
				const shared = mode === "unshare-noop" || mode === "range-invalid", slots: number[][] = [];
				const ownership: boolean[] = [];
				const commit = vi.fn(async () => {});
				const executor = held({ descriptors: true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					snapshot = process.descriptors;
					slots.push(snapshot!.map(({ fd }) => fd));
					ownership.push(snapshot!.find(({ fd }) => fd === 3)!.owned);
					return mode === "export" || split || imported ? { kind: "replay", descriptorOffsets: snapshot!.map(({ offset, ...descriptor }) =>
						({ ...descriptor, before: offset, after: Number(offset) + 1, ...(descriptor.type === "pipe" ? { content: Buffer.from(descriptor.queueHex!, "hex") } : {}) })), exitCode: 0, output: [], commit } : { kind: "continue" };
				} }, mode === "rights-unowned" ? adaptProcessToolOperations(createLocalBashOperations({ shellPath: external })) : native);
				expect(await executor.execute(`exec 3<'${input}'; '${descriptorProbe}' ${mode} '${input}'; result=$?; ` +
					`IFS= read -r -N 1 byte <&3; printf '%s' "$byte"; exit "$result"`)).toEqual({ exitCode: 0 });
				expect(executor.output, mode).toBe(split ? "c" : imported && !unknown ? "b" : "a"); expect(commit).toHaveBeenCalledTimes(split ? 2 : imported && !unknown ? 1 : 0);
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
			const piped = held({ descriptors: true, decide: async process => {
				if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
				return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.alloc(1024 * 1024, "x") }],
					descriptorOffsets: process.descriptors!.map(({ offset, ...descriptor }) => ({ ...descriptor, before: offset, after: Number(offset) + 1 })), commit: async () => {} };
			} });
			expect(await piped.execute(`exec 3<'${input}'; /bin/true | /usr/bin/wc -c; IFS= read -r -N 1 byte <&3; printf '%s' "$byte"`)).toEqual({ exitCode: 0 });
			expect(piped.output).toBe("1048576\nb");
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
			await compileBenchmarkHelper(root, { source: `${pipeProbe}.c`, output: "pipe-probe", arguments: ["-Werror"] });
			for (const mode of ["partial", "empty", "flags", "queue-conflict", "contents", "overrun", "stale", "live", "packet", "inspect", "commit-failure", "journal", "journal-conflict", "journal-write-readonly", "journal-shutdown-pipe", "journal-commit-failure"]) {
				const accepted = ["partial", "empty", "flags", "journal"].includes(mode), journal = mode.startsWith("journal"), failedCommit = mode.endsWith("commit-failure");
				let descriptors: HeldExecProcess["descriptors"];
				const commit = vi.fn(async () => { if (failedCommit) throw new Error("pipe commit failure"); });
				const executor = held({ descriptors: mode === "inspect" ? () => "inspect" : true, decide: async process => {
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
				const running = executor.execute(`'${pipeProbe}' ${mode}`);
				if (failedCommit) await expect(running).rejects.toMatchObject({ disposition: "poisoned" });
				else {
					expect(await running, mode).toEqual({ exitCode: 0 });
					expect(executor.output, mode).toBe(accepted ? `replayed:${mode === "empty" ? "" : "def"}` : mode === "stale" ? "bcdef" : "abcdef");
				}
				expect(commit, mode).toHaveBeenCalledTimes(Number(accepted || failedCommit));
				if (accepted) expect(descriptors, mode).toMatchObject([{ fd: 0, alias: 0, type: "pipe", owned: true }, { fd: 3, alias: 0, type: "pipe", owned: true }, { fd: 8, alias: 8, type: "pipe", owned: true }]);
			}
			const messageProbe = path.join(root, "message-probe");
			await writeFile(messageProbe + ".c", `#define _GNU_SOURCE
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <string.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc != 3) return 70;
	int fds[2]; if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) || fds[0] != 3 || fds[1] != 4) return 71;
	int input = open(argv[1], O_RDONLY); if (input < 0 || dup2(input, 9) < 0) return 72; close(input);
	int passed[2] = {9, 3}; char control[CMSG_SPACE(sizeof(passed))] = {0};
	struct iovec vector = {"qI", 2}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS;
	header->cmsg_len = CMSG_LEN(sizeof(passed)); memcpy(CMSG_DATA(header), passed, sizeof(passed));
	if (write(4, "L", 1) != 1 || sendmsg(4, &message, 0) != 2) return 73;
	vector.iov_base = "t"; vector.iov_len = 1; if (sendmsg(4, &message, 0) != 1 || write(4, "bcaXYZ", 6) != 6) return 74;
	if (*argv[2] == '1' && close(9)) return 79;
	for (int repeat = 0; repeat < 2; repeat++) {
		pid_t child = fork(); if (child < 0) return 75;
		if (!child) { execl("/bin/true", "true", (char *)0); _exit(76); }
		int status, cursor; socklen_t size = sizeof(cursor);
		if (waitpid(child, &status, 0) != child || status || getsockopt(3, SOL_SOCKET, SO_PEEK_OFF, &cursor, &size) || cursor != -1) return 77;
	}
	for (int left = 10; left;) { char bytes[10]; ssize_t size = read(3, bytes, left); if (size <= 0 || write(1, bytes, size) != size) return 78; left -= size; }
	return 0;
}
`);
			await compileBenchmarkHelper(root, { source: messageProbe + ".c", output: "message-probe", arguments: ["-Werror"] });
			for (const orphan of [false, true]) for (const mode of ["messages", "start", "end", "rights", "commit-failure", "cancel"]) {
				const controller = new AbortController();
				const pinCounts: number[] = [];
				const commit = vi.fn(async () => { if (mode === "commit-failure") throw new Error("message commit failure"); });
				const executor = held({ descriptors: true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					const descriptorOffsets = process.descriptors!.map(fd => ({ ...fd, before: fd.offset, after: fd.offset,
						...(fd.queueHex !== undefined ? { content: Buffer.from(fd.queueHex, "hex") } : {}) }));
					const receiver = descriptorOffsets.find(fd => fd.fd === 3)!;
					const reference = orphan ? descriptorOffsets.find(fd => fd.pin !== undefined)! : descriptorOffsets.find(fd => fd.fd === 9)!;
					expect(reference).toBeDefined();
					if (orphan) {
						expect(await filesystem.readdir(`/proc/${process.pid}/fd`)).not.toContain(String(reference.fd));
						pinCounts.push((await filesystem.readdir(`/proc/${process.tracerPid}/fd`)).length);
					}
					expect(receiver.messages).toEqual([{ start: 1, end: 3, rights: [reference.fd, 3] }, { start: 3, end: 4, rights: [reference.fd, 3] }]);
					receiver.messages = receiver.messages!.map((message, index) => index ? message : { ...message,
						...(mode === "start" ? { start: 0 } : mode === "end" ? { end: 2 } : mode === "rights" ? { rights: [3, reference.fd] } : {}) });
					if (mode === "cancel") controller.abort();
					return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.from("replayed:") }], descriptorOffsets, commit };
				} });
				const running = executor.execute(`'${messageProbe}' '${input}' ${Number(orphan)}`, { signal: controller.signal });
				if (mode === "commit-failure") await expect(running).rejects.toMatchObject({ disposition: "poisoned" });
				else if (mode === "cancel") await expect(running).rejects.toThrow();
				else { expect(await running, mode).toEqual({ exitCode: 0 }); expect(executor.output, mode).toBe(`${mode === "messages" ? "replayed:replayed:" : ""}LqItbcaXYZ`); }
				expect(commit, mode).toHaveBeenCalledTimes(mode === "messages" ? 2 : Number(mode === "commit-failure"));
				if (pinCounts.length === 2) expect(pinCounts[1]).toBe(pinCounts[0]);
				if (mode === "cancel" || mode === "commit-failure") expect(executor.output).toBe("");
			}
			const counterProbe = path.join(root, "counter-probe");
			await writeFile(counterProbe + ".c", `#include <sys/eventfd.h>
#include <sys/wait.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
int main(void) {
	int fd = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC); if (fd < 0 || dup2(fd, 20) < 0 || dup2(fd, 21) < 0) return 70; close(fd);
	pid_t child = fork(); if (child < 0) return 71;
	if (!child) { char *args[] = {"true", 0}; execv("/bin/true", args); return 72; }
	int status; uint64_t value; if (waitpid(child, &status, 0) != child || status || read(20, &value, 8) != 8) return 73;
	printf("%llu", (unsigned long long)value); return 0;
}
`);
			await compileBenchmarkHelper(root, { source: counterProbe + ".c", output: "counter-probe", arguments: ["-Werror"] });
			for (const mode of ["counter", "value", "identity", "alias", "overflow", "commit-failure"]) {
				const commit = vi.fn(async () => { if (mode === "commit-failure") throw new Error("counter commit failure"); });
				const executor = held({ descriptors: true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					const descriptorOffsets = process.descriptors!.filter(fd => fd.type === "eventfd").map(fd => {
						const content = Buffer.alloc(9); content.writeBigUInt64LE(BigInt(mode === "value" || mode === "alias" && fd.fd === 21 ? 6 : fd.counter!.value));
						return { ...fd, event: fd.counter!.id + (mode === "identity" ? 2 : 1), before: 0, after: 0, content };
					});
					const data = Buffer.alloc(8); data.writeBigUInt64LE(mode === "overflow" ? 0xffffffffffffffffn : 2n);
					return { kind: "replay", exitCode: 0, output: [], descriptorOffsets, resourceEvents: [{ fd: 21, kind: "produce", data }], commit };
				} });
				const running = executor.execute(`'${counterProbe}'`);
				if (mode === "commit-failure") await expect(running).rejects.toMatchObject({ disposition: "poisoned" });
				else { expect(await running, mode).toEqual({ exitCode: 0 }); expect(executor.output, mode).toBe(mode === "counter" ? "7" : "5"); }
				expect(commit, mode).toHaveBeenCalledTimes(Number(mode === "counter" || mode === "commit-failure"));
			}
			for (const mode of ["shared", "unlinked", "offset", "identity", "flags", "closed", "alias-conflict", "invalid", "commit-failure",
				"null", "null-offset", "null-content", "zero", "status-set", "status-clear", "status-conflict", "status-unsupported", "directory", "directory-offset", "directory-content", "directory-stale", "directory-replaced", "directory-no-path", "opath", "opath-offset", "opath-content", "opath-flags", "directory-opath", "directory-symlink"]) {
				await writeFile(input, "abcdef");
				const directory = mode.startsWith("directory"), opath = mode.includes("opath"), device = directory || opath || mode.startsWith("null") || mode === "zero";
				const target = directory ? directoryInput : opath ? input : device ? mode === "zero" ? "/dev/zero" : "/dev/null" : input;
				const accepted = mode === "shared" || mode === "unlinked" || mode === "null" || mode === "status-set" || mode === "status-clear" || mode === "directory" || mode === "opath" || mode === "opath-content" || mode === "directory-opath";
				let heldPid = 0;
				const commit = vi.fn(async () => {
					expect(await readFile(`/proc/${heldPid}/fdinfo/3`, "utf8")).toMatch(/^pos:\s*0$/m);
					if (mode === "commit-failure") throw new Error("injected offset commit failure");
				}), adopted = vi.fn();
				const executor = held({ descriptors: directory, decide: async ({ pid, descriptors }) => {
					if (await filesystem.readlink(`/proc/${pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					heldPid = pid;
					const descriptorOffsets = await Promise.all([3, 4, 5].map(async fd => {
						const info = await filesystem.stat(`/proc/${pid}/fd/${fd}`, { bigint: true });
						const text = await readFile(`/proc/${pid}/fdinfo/${fd}`, "utf8");
						return { fd, device: info.dev.toString(), inode: info.ino.toString(),
							flags: Number.parseInt(/^flags:\s*([0-7]+)/m.exec(text)![1]!, 8), before: 0, after: device ? 0 : fd === 5 ? 1 : 3,
							...(mode.startsWith("status-") && fd !== 5 ? { afterFlags: 32768 | (mode === "status-clear" ? 0 : mode === "status-unsupported" ? 0x2000 : mode === "status-conflict" && fd === 4 ? 0 : 0xc00) } : {}),
							...(directory && mode !== "directory-no-path" ? { path: directoryInput } : {}),
							...(directory && !opath ? { content: Buffer.from(descriptors!.find(item => item.fd === fd)!.directoryHex!, "hex") } : {}),
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
					if (mode === "directory-stale") await writeFile(path.join(directoryInput, "created-after-capture"), "");
					if (mode === "directory-replaced") { await filesystem.rename(directoryInput, `${directoryInput}-old`); await mkdir(directoryInput); }
					if (mode === "directory-symlink") { await filesystem.rename(directoryInput, `${directoryInput}-target`); await filesystem.symlink(`${directoryInput}-target`, directoryInput); }
					return { kind: "replay", descriptorOffsets, exitCode: 0,
						output: [{ fd: 1, data: Buffer.from("replayed:") }], commit, adopted };
				} });
				const running = executor.execute(`exec 3<'${target}'; exec 4<&3; exec 5<'${target}'; ${opath ? `'${descriptorProbe}' opath '${target}'` : mode === "status-clear" ? `'${descriptorProbe}' status '${input}'` : "/bin/true"}; ` +
					(mode.startsWith("status-") ? `for fd in 3 4 5; do while read -r key value; do if [[ $key == flags: ]]; then (( (8#$value & 3072) == (${mode === "status-set" ? 3072 : 0} * (fd != 5)) )) || exit 90; fi; done </proc/self/fdinfo/$fd; done; ` : "") +
					(device ? "printf native" : `IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&5; IFS= read -r -N 1 c <&3; printf '%s:%s:%s' "$a" "$b" "$c"`));
				if (mode === "commit-failure") {
					await expect(running).rejects.toMatchObject({ disposition: "poisoned" }); expect(executor.output).toBe("");
				} else {
					expect(await running).toEqual({ exitCode: 0 });
					expect(executor.output, mode).toBe(device ? (accepted ? "replayed:native" : "native") : accepted ? "replayed:d:b:e" : "a:a:b");
				}
				expect(commit).toHaveBeenCalledTimes(Number(accepted || mode === "commit-failure"));
				expect(adopted).toHaveBeenCalledTimes(Number(accepted));
			}
			for (const killed of [false, true]) {
				const waiting = deferred(), nativeDone = deferred();
				let callbacks = 0, observed = 0, closed = 0, heldPid = 0;
				const concurrent = held({ decide: async process => {
					if (++callbacks === 1) { heldPid = process.pid; await waiting.promise; }
					return { kind: "continue", observeCompletion: async durationMs => {
						await nextTurn(); closed++; if (durationMs !== undefined) observed++;
					} };
				} }, { execute: request => native.execute(request).finally(nativeDone.resolve) });
				const siblings = concurrent.execute(`/bin/true & while [[ ! -e start-second-${killed} ]]; do :; done; /bin/echo sibling; wait`);
				try {
					await vi.waitFor(() => expect(callbacks).toBe(1));
					await writeFile(path.join(root, `start-second-${killed}`), "ready");
					await vi.waitFor(() => { expect(concurrent.output).toBe("sibling\n"); expect(observed).toBe(1); });
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
				const chained = held({ decide: async ({ pid }) => {
					const image = `${pid}:${await filesystem.readlink(`/proc/${pid}/exe`)}`;
					visited.push(image);
					return { kind: "continue", observeCompletion: () => { completed.push(image); } };
				} });
				expect(await chained.execute(command))
					.toEqual({ exitCode: 0 });
				expect(visited).toHaveLength(2);
				expect(completed.sort(), "every exec image must retain its completion owner through replacement").toEqual(visited.sort());
			}
			const committed = vi.fn(async () => { await writeFile(path.join(root, "producer-armed"), "ready"); });
			const delivery = held({ decide: async process => {
				if (path.basename(await filesystem.readlink(`/proc/${process.pid}/exe`)) === "true")
					return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.alloc(2 * 1024 * 1024, 97) }], commit: committed };
				return { kind: "continue" };
			} });
			expect(await delivery.execute("/bin/true | (while [[ ! -e producer-armed ]]; do :; done; /usr/bin/wc -c)")).toEqual({ exitCode: 0 });
			expect(delivery.output).toBe("2097152\n"); expect(committed).toHaveBeenCalledOnce();
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
				const executor = held({ decide }, { execute: request => native.execute(request).finally(nativeDone.resolve) });
				const run = actorContext.run("original", () => executor.execute(`/bin/true; printf continued > '${after}'`, { scope }));
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
			expect(branch.output.isError, JSON.stringify({ output: branch.output, metrics: fixture.backend.metrics() })).toBe(false);
			const text = branch.output.result.content[0];
			expect(text?.type === "text" && text.text).toBe("trace-root-fallback\nredirected\nfile-fallback\npipe-fallback\nfds:7\nfds:6\nfds:5\nfds:3\nfds:0\nfds:7\n" + " ".repeat(32768) + ":end");
			const nextCapture = await vi.mocked(captures[1]!.finish).mock.results[0]!.value;
			expect({ allocationFailed, aborts: vi.mocked(captures[0]!.abort).mock.calls.length, nextComplete: nextCapture.complete },
				nextCapture.complete ? undefined : nextCapture.reason).toEqual({ allocationFailed: true, aborts: 1, nextComplete: true });
			expect(captures[0]!.finish).not.toHaveBeenCalled();
			expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(3);
			expect(branch.executionMetrics.reuse?.bypasses).toBe(5); // Capture failure, internal pipeline and three unsupported stdio contexts.
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
				const route = await fixture.prepareActorReplay(true);
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
