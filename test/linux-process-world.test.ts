import { fauxText } from "@earendil-works/pi-ai";
import { readContent } from "../src/content-chunks.ts";
import { readText } from "./filesystem.ts";
import { disposable, onExit, gated, deferred, nextTurn } from "./async.ts";
import { once } from "node:events";
import { AsyncResource } from "node:async_hooks";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";
import * as childProcess from "node:child_process";
import { existsSync } from "node:fs";
import * as filesystem from "node:fs/promises";
import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { createLocalBashOperations, createReadTool, createGrepTool, createLsTool, createFindTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { describe, expect, vi } from "vitest";
import { buildActionKey, buildPiActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { LinuxHeldExecBoundary, resolveLinuxExecHelper, type HeldExecProcess, type HeldExecTiming } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend, validateTransferredProcessEvidence, type LinuxProcessSession } from "../src/linux-process-backend.ts";
import { ProcessHandoffRegistry, type ProcessExecutionBinding } from "../src/process-handoff.ts";
import { resolvePiToolInvocation, createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { expectHiddenCompute, resourceWorld } from "./agent-system-fixture.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";
import { waitForCompletion } from "../src/runtime-lifecycle.ts";
import { currentExecutionScope, type ExecutionOperationBinding } from "../src/execution-world.ts";
import { patternAwareSettings } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { TaskTimeline, TimelineInterval, type TimelineDependency } from "../src/task-timing.ts";
import { ResourceObjectInputs } from "../src/agent-execution-world.ts";
import { captureStableFile, FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";
import { ResourceReadView } from "../src/resource-version.ts";
import {
	compileBenchmarkHelper, cHeaders, processTest as test, executeProcess, holdOwnedProcessPublication, patternProcessHost,
	recordProcessPublications, textOutput, type LinuxProcessBenchmark,
} from "./linux-process-fixture.ts";

vi.mock("node:child_process", { spy: true });
vi.mock("node:fs/promises", { spy: true });
const nativeTest = process.arch === "x64" ? test : test.skip;

const processChecks = `${cHeaders("sys/wait.h string.h unistd.h")}
static inline int wait_child(pid_t child) {
	int status; return waitpid(child, &status, 0) != child || status;
}
static inline int read_text(int fd, void *bytes, const char *expected) {
	size_t length = strlen(expected);
	return read(fd, bytes, length) != (ssize_t)length || memcmp(bytes, expected, length);
}
static inline int move_fd(int source, int target) {
	if (source < 0 || dup2(source, target) < 0) return 1;
	if (source != target) close(source);
	return 0;
}
static inline int wait_gate(int gate[2]) {
	char byte; close(gate[1]);
	if (read(gate[0], &byte, 1) != 1) return 1;
	close(gate[0]); return 0;
}
static inline void burn_cpu(unsigned long iterations) {
	for (volatile unsigned long i = 0; i < iterations; ++i) {}
}`;

const rightsHelpers = `${processChecks}
${cHeaders("fcntl.h sys/socket.h")}
static inline int pass(int fd, const char *text, int length, const int *rights, int count, int flags) {
	char control[CMSG_SPACE(8 * sizeof(int))] = {0}; struct iovec vector = {(void *)text, (size_t)length};
	struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = CMSG_SPACE((size_t)count * sizeof(int))}};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN((size_t)count * sizeof(int));
	memcpy(CMSG_DATA(header), rights, (size_t)count * sizeof(int)); return (flags < 0 ? sendmmsg(fd, &message, 1, 0) : sendmsg(fd, &message.msg_hdr, flags)) != (flags < 0 ? 1 : length);
}
static inline int take_text(int fd, const char *expected, int length, int *rights, int count, int peek, int batch) {
	char bytes[8], control[CMSG_SPACE(8 * sizeof(int))]; struct iovec vector = {bytes, (size_t)length};
	if (length < 1 || (size_t)length > sizeof(bytes)) return 1;
	struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}};
	if (batch < 0 && recvmsg(-1, &message.msg_hdr, 0) != -1) return 1;
	int flags = MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0);
	if ((batch > 0 ? recvmmsg(fd, &message, 1, flags, 0) : recvmsg(fd, &message.msg_hdr, flags)) != (batch > 0 ? 1 : length) || memcmp(bytes, expected, (size_t)length) || message.msg_hdr.msg_flags & MSG_CTRUNC) return 1;
	struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr); if (!header || header->cmsg_len != CMSG_LEN((size_t)count * sizeof(int))) return 1;
	memcpy(rights, CMSG_DATA(header), (size_t)count * sizeof(int));
	for (int index = 0; index < count; index++) if (fcntl(rights[index], F_GETFD) != FD_CLOEXEC) return 1;
	return 0;
}
static inline int take(int fd, char expected, int *rights, int count, int peek, int batch) {
	return take_text(fd, &expected, 1, rights, count, peek, batch);
}
static inline int wrap_queue(int reference, int cycle) {
	int pair[2]; if (reference < 0 || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair)) return -1;
	int rights[2] = {reference, pair[1]}, failed = pass(pair[0], "x", 1, rights, cycle ? 2 : 1, 0);
	close(reference); close(pair[0]); if (failed) { close(pair[1]); return -1; } return pair[1];
}`;

async function learnWorker(fixture: LinuxProcessBenchmark, scope: { sessionID: string; turnID: string }, command: string) {
	await fixture.prepare();
	const route = await fixture.prepareActorReplay(), handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string }>;
	let binding: ProcessExecutionBinding | undefined;
	const execute = async (current = scope, input = command, signal = AbortSignal.timeout(20_000)) => {
		const { output, ...result } = await executeProcess(fixture, route.executor, input, { scope: current, signal, timeout: 10 });
		expect(result, output).toEqual({ exitCode: 0 }); return output;
	};
	const actor = (signal?: AbortSignal, synchronous = false) => {
		let computations: readonly TimelineDependency[] = [];
		return TimelineInterval.measure(() => fixture.backend.observeBindings(scope, () => synchronous ? fixture.backend.observeNative(fixture.workspace, scope, signal,
			async () => childProcess.execFileSync(fixture.shellPath, ["-c", `printf own:; ${command}; printf once >> marker`],
				{ cwd: fixture.workspace, env: { ...fixture.environment }, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] })) : execute(scope, command, signal), (bindings, observed) => {
			computations = observed; binding ??= bindings.find(candidate => path.basename(handoffs.resolveBinding(candidate, scope)?.executable ?? "") === "worker");
		}, true), () => computations);
	};
	const seed = (await actor()).output; expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
	return { actor, execute, seed, binding: binding!, handoffs, invocation: binding! };
}

async function runningProcess(fixture: LinuxProcessBenchmark, ready: (pid: number, syscall: string) => boolean | Promise<boolean>, start = 0) {
	let pid = 0;
	await expect.poll(async () => {
		const reports = await fixture.inOwner((state, start) => state.spawn.mock.calls.slice(start).flatMap(({ arguments: [, args] }: any) => {
			const file = args.find((arg: string) => arg.endsWith("/fd-offsets")); return file ? [[file, state.captured?.get(file)] as const] : [];
		}) as readonly (readonly [string, number | undefined])[], start);
		for (const [reportPath, sealed] of reports) {
			if (sealed) { pid = sealed; return true; }
			const report = await readText(reportPath).catch(() => "");
			const match = /\nRUNNING (\d+)\n$/.exec(report); if (!match) continue;
			const current = Number(match[1]), syscall = await readText(`/proc/${current}/syscall`).catch(() => "");
			if (await ready(current, syscall)) { pid = current; return true; }
		}
		return false;
	}, { timeout: 10_000 }).toBe(true);
	return pid;
}

function consumedTiming(source: readonly SpeculativeActionEvent<string>[] | TimelineInterval, turnID?: string) {
	const computation = source instanceof TimelineInterval ? source : source.filter(event => event.type === "actor_action").find(event => event.turnID === turnID)!.settlement.provider.toolExecution;
	const timings = [0, computation.startedAt].map(start => new TaskTimeline(start).recordCall([{ computation }], computation.startedAt, computation.completedAt));
	expect(timings[1]!.hiddenComputeMs).toBe(timings[0]!.hiddenComputeMs);
	expect(timings[0]!.toolComputeMs).toBeGreaterThanOrEqual(timings[0]!.hiddenComputeMs);
	return timings[0]!;
}

async function withPreparedWorker(fixture: LinuxProcessBenchmark, input: Parameters<LinuxProcessReuseBackend["open"]>[0], binding: ProcessExecutionBinding,
	output: string, run: (session: LinuxProcessSession) => Promise<void>, validate = true) {
	await using session = disposable(await fixture.backend.open(input));
	const result = await session.executeBinding(binding); expect(result.exit).toEqual({ kind: "code", code: 0 });
	expect(result.output.map(({ fd, data }) => [fd, data.toString()])).toEqual([[1, output]]);
	if (validate) { await session.seal([]); expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: "valid" }); }
	await run(session);
}

describe("Linux process ExecutionWorld", () => {
nativeTest("keeps output endpoint ownership distinct from another process with the same directory descriptor", async ({ benchmark }) => {
	const fixture = await benchmark("pi-output-namespace-", "git");
	await fixture.compile("foreign-output", { code: `${cHeaders("sys/un.h sys/socket.h unistd.h stdio.h stdlib.h fcntl.h")}
int main(int argc, char **argv) {
 if (argc != 3) return 61;
 int directory = open(argv[1], O_RDONLY | O_DIRECTORY), target = atoi(argv[2]);
 if (directory < 0 || dup2(directory, target) != target) return 62;
 struct sockaddr_un address = {.sun_family = AF_UNIX};
 snprintf(address.sun_path, sizeof(address.sun_path), "/proc/self/fd/%d/1", target);
 int server = socket(AF_UNIX, SOCK_STREAM, 0), client = socket(AF_UNIX, SOCK_STREAM, 0);
 if (server < 0 || client < 0 || bind(server, (void *)&address, sizeof(address)) || listen(server, 1) ||
     connect(client, (void *)&address, sizeof(address)) || accept(server, NULL, NULL) < 0 || write(1, "ready", 5) != 5) return 63;
 char byte; return read(0, &byte, 1) < 0;
}` });
	const foreign = path.join(fixture.root, "foreign"); await mkdir(foreign);
	await fixture.commit("Output endpoint ownership"); await fixture.prepare();
	await fixture.inOwner(async (state, binary, foreign) => {
		const listen = state.net.Server.prototype.listen, read = state.fs.readFile;
		state.method(state.net.Server.prototype, "listen", function(this: unknown, ...args: any[]) {
			const socket = typeof args[0] === "string" ? args[0] : args[0]?.path;
			if (/^\/proc\/self\/fd\/\d+\//.test(socket ?? "")) state.outputDirectory = socket.split("/")[4];
			return listen.apply(this, args);
		});
		state.method(state.fs, "readFile", async (...args: any[]) => {
			if (args[0] === "/proc/net/unix" && state.outputDirectory && !state.foreign) {
				const { spawn } = await state.load("node:child_process"), { once } = await state.load("node:events");
				state.foreign = spawn(binary, [foreign, state.outputDirectory]); state.foreignClosed = once(state.foreign, "close");
				await once(state.foreign.stdout, "data");
			}
			return read(...args);
		});
	}, path.join(fixture.workspace, "foreign-output"), foreign);
	await using _cleanup = onExit(() => fixture.inOwner(async state => { state.foreign?.stdin.end(); await state.foreignClosed; }));
	await using branch = disposable(await fixture.fork("printf correct-output"));
	expect(await fixture.inOwner(state => !!state.foreign)).toBe(true);
	expect(textOutput(branch.output.result)).toBe("correct-output");
});

nativeTest.for(["argv", "MODE", "__proto__"])("preserves literal workspace text in %s", async (field, { benchmark }) => {
	const fixture = await benchmark("pi-literal-input-", "git"); await fixture.prepare();
	await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
		const sessions: LinuxProcessSession[] = []; await using _sessions = onExit(async () => { for (const session of sessions) await session.close(); });
		for (const literal of [workspace.sandboxRoot, fixture.workspace, workspace.processRoot, "/.pi-private-world"]) {
			const environment = { ...fixture.environment, ...(field === "argv" ? {} : { [field]: literal }) };
			const command = field === "argv" ? `printf '%s\\n' '${literal}'` : `printf own:; /usr/bin/printenv ${field}; true`;
			const invocation = { ...fixture.invocation({ command }).process!, environment };
			const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: { sessionID: "literal", turnID: "one" } }); sessions.push(session);
			const actual = await executeProcess({ ...fixture, environment }, session.executor, command);
			expect(actual, JSON.stringify({ literal, actual })).toEqual({ exitCode: 0, output: `${field === "argv" ? "" : "own:"}${literal}\n` });
		}
	});
});

nativeTest.for(["path", "nested", "script", "alias", "absolute", "argv0", "fd", "unlinked", "thread", "failed"])("exposes and adopts a first unseen child (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-first-child-", "git"), directory = mode === "path" ? "" : "nested";
	await mkdir(path.join(fixture.workspace, "nested"));
	const executable = path.join(fixture.workspace, directory, "worker"), relative = `./${directory ? directory + "/" : ""}worker`;
	const launched = ["argv0", "fd", "unlinked", "thread", "failed"].includes(mode);
	const fd = mode === "fd" || mode === "unlinked", invoked = fd ? "/dev/fd/100" : mode === "absolute" ? executable : mode === "alias" ? "./alias" : relative, argv0 = launched ? "fixed-zero" : invoked;
	if (mode === "script") await writeFile(executable, '#!/bin/sh\nprintf "%s|%s\\n" "$0" "$1"\n', { mode: 0o755 });
	else {
		await fixture.compile(path.join(directory, "worker"), { source: "worker.c", code: `${cHeaders("unistd.h string.h sys/auxv.h sys/prctl.h")}\nint main(int argc, char **argv) { (void)argc; char comm[17]={0}; if(prctl(PR_GET_NAME,comm))return 70; const char *name=(char *)getauxval(AT_EXECFN); write(1,argv[0],strlen(argv[0])); write(1,"|",1); write(1,name,strlen(name)); write(1,"|",1); write(1,comm,strlen(comm)); write(1,"\\n",1); return 0; }\n` });
		await fixture.compile("launch", { arguments: ["-pthread"], code: `#define _GNU_SOURCE\n${cHeaders("unistd.h fcntl.h sys/syscall.h pthread.h stdio.h errno.h string.h")}\nstatic char **command; static void *execute(void *unused) { (void)unused; execv(command[2],command+3); _exit(127); }\nint main(int argc, char **argv) { if(argc<5)return 64; command=argv; if(!strncmp(argv[1],"thread:",7)){pthread_t thread; if(pthread_create(&thread,0,execute,0))return 68;for(;;)pause();} if(!strncmp(argv[1],"failed:",7)){execv("./missing",argv+3);dprintf(1,"failure=%d\\n",errno);}
${fd ? `int input=open(argv[2],O_RDONLY); if(input<0)return 65;
${mode === "unlinked" ? 'int output=open("transient",O_WRONLY|O_CREAT|O_TRUNC,0700); char buffer[8192]; ssize_t n; while((n=read(input,buffer,sizeof(buffer)))>0)if(write(output,buffer,n)!=n)return 66; close(output);close(input);input=open("transient",O_RDONLY);unlink("transient");' : ''}
if(dup3(input,100,O_CLOEXEC)<0)return 67; close(input); syscall(SYS_execveat,100,"",argv+3,environ,AT_EMPTY_PATH);` : 'execv(argv[2],argv+3);'} return 127; }\n` });
	}
	if (mode === "alias") await filesystem.symlink(path.join(directory, "worker"), path.join(fixture.workspace, "alias"));
	await fixture.prepare(); const publications = await recordProcessPublications(fixture);
	const command = (owner: string) => `${launched ? `./launch ${mode}:${owner} ${fd ? relative : invoked} ${argv0}` : invoked} ${invoked}`;
	const expected = `${mode === "failed" ? "failure=2\n" : ""}${argv0}|${invoked}${mode === "script" ? "" : "|" + (mode === "alias" ? "alias" : mode === "unlinked" ? "transient" : "worker")}\n`;
	const producer = { source: "drafter", id: TimelineInterval.producerID(fixture, mode) };
	const evaluated = await TimelineInterval.collect(() => fixture.fork(`printf own:; ${command("own")}`), producer);
	await using branch = disposable(evaluated.output);
	expect(textOutput(branch.output.result)).toBe(`own:${expected}`);
	const published = (await publications()).map(certificate => certificate.prototype.executablePath);
	expect(published, JSON.stringify({ published, operations: branch.operations, metrics: fixture.backend.metrics() })).toContain(mode === "script" ? await filesystem.realpath("/bin/sh") : mode === "unlinked" ? path.join(fixture.workspace, "transient") : executable);
	const { executor } = await fixture.prepareActorReplay(true), before = fixture.backend.actorMetrics().hits;
	const scope = { sessionID: "benchmark", turnID: "benchmark" };
	let consumed: readonly TimelineDependency[] = [];
	const actor = await fixture.backend.observeBindings(scope, () => executeProcess(fixture, executor, `printf actor:; ${command("actor")}`, { scope }), (_, computations) => { consumed = computations; });
	expect(actor.output).toBe(`actor:${expected}`);
	expect([...TimelineInterval.reusedProducers(consumed)]).toEqual([producer.id]);
	expect(fixture.backend.actorMetrics().hits, JSON.stringify({ metrics: fixture.backend.actorMetrics(), certificates: (await publications()).map(({ prototype, dependencyCertificate }) => ({ prototype, taints: dependencyCertificate.taints })) })).toBe(before + 1);
	if (mode !== "script" && !fd) {
		const other = mode === "absolute" ? relative : executable;
		expect((await executeProcess(fixture, executor, `./launch other ${other} ${argv0} ${invoked}`, { scope })).output).toBe(`${argv0}|${other}|worker\n`);
		expect(fixture.backend.actorMetrics().hits).toBe(before + 1);
	}
});

nativeTest.for(["workspace", "backend-first", "workspace-first"])("drains the shared owner in a standalone process (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-owner-shutdown-", "git");
	await fixture.prepare();
	const source = (name: string) => JSON.stringify(new URL(`../src/${name}.ts`, import.meta.url).href);
	const result = childProcess.spawnSync(process.execPath, ["--input-type=module", "-e", `
		import { WorkspaceSandboxService } from ${source("workspace-sandbox")};
		import { LinuxProcessReuseBackend } from ${source("linux-process-backend")};
		import { resolvePiToolInvocation } from ${source("pi-tool-invocation")};
		const service = new WorkspaceSandboxService(), backend = new LinuxProcessReuseBackend({ storeRoot: ${JSON.stringify(path.join(fixture.root, "standalone-store"))} });
		await service.withWorkspace(${JSON.stringify(fixture.workspace)}, async workspace => {
			const capture = await workspace.transactions.begin(); await capture.abort();
			if (${JSON.stringify(mode)} === "workspace") return;
			const command = "printf own", invocation = resolvePiToolInvocation("bash", { command }, { cwd: workspace.sourceRoot, environment: process.env }).process;
			const session = await backend.open({ sourceRoot: workspace.sourceRoot, workspace, invocation });
			await session.executor.execute({ command, cwd: workspace.sourceRoot, environment: process.env, onData: data => process.stdout.write(data) });
			await session.close();
			if (${JSON.stringify(mode)} === "backend-first") await backend.dispose();
		});
		await service.dispose(); await backend.dispose(); process.stdout.write("closed");
	`], { encoding: "utf8", timeout: 20_000 });
	expect({ code: result.status, signal: result.signal, error: result.error, stderr: result.stderr }).toEqual({ code: 0, signal: null, error: undefined, stderr: "" });
	expect(result.stdout).toBe(mode === "workspace" ? "closed" : "ownclosed");
});

nativeTest.for(["journal", "whole-released", "whole-consumed", "custom", "dispose"])("keeps one workspace owner across public sessions (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-public-owner-", "git"), second = new LinuxProcessReuseBackend({ storeRoot: path.join(fixture.root, "other-store") });
	await using _second = onExit(() => second.dispose());
	await fixture.write("input.txt", "before");
	await fixture.inOwner(async state => {
		state.opened = state.method((await state.load("../src/linux-process-backend.ts")).LinuxProcessReuseBackend.prototype, "open");
	});
	const command = "printf own", invocation = fixture.invocation({ command }).process!;
	const running = fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
		if (mode === "journal") await chmod(path.join(workspace.sandboxRoot, "input.txt"), 0o600);
		const interval = await workspace.transactions.begin(), begin = workspace.transactions.begin, captured = vi.fn(begin);
		const context = mode === "custom" ? { ...workspace, transactions: { ...workspace.transactions, begin: captured } } : workspace;
		await using session = disposable(await fixture.backend.open({ sourceRoot: fixture.workspace, workspace: context, invocation }));
		await using _sibling = disposable(await second.open({ sourceRoot: fixture.workspace, workspace, invocation }));
		if (mode === "journal") {
			await fixture.inOwner(async state => { state.intervals = await Promise.all(state.opened.mock.calls.map((call: any) => call.arguments[0].workspace.transactions.begin())); await state.intervals[0].abort(); });
			expect(await interval.finish()).toMatchObject({ complete: false, reason: "overlapping_workspace_transaction" });
			expect(await fixture.inOwner(state => state.intervals[1].finish())).toMatchObject({ complete: false, reason: "overlapping_workspace_transaction" });
			const next = await workspace.transactions.begin();
			expect(Buffer.from((await next.readBefore!("input.txt", 6))!).toString()).toBe("before");
			await fixture.inOwner(async state => { state.other = await state.opened.mock.calls[1].arguments[0].workspace.transactions.begin(); });
			await writeFile(path.join(workspace.sandboxRoot, "input.txt"), "after");
			const interfered = vi.fn(async () => false), delta = await next.finish(async () => ({ written: new Set(["input.txt"]), observed: new Map(), endedAt: Date.now(), interfered }));
			expect(delta.complete).toBe(true); expect(await Promise.all(delta.changes.map(async change => [change.relativePath, (await readContent(change.before!)).toString(), (await readContent(change.after!)).toString()]))).toEqual([["input.txt", "before", "after"]]);
			expect(delta.changes.map(change => [change.beforeMode, change.afterMode])).toEqual([[0o600, 0o600]]);
			expect(interfered).toHaveBeenCalledOnce(); await fixture.inOwner(state => state.other.abort());
			for (const phase of ["once", "always", "io"]) {
				const capture = await workspace.transactions.begin();
				await fixture.inOwner(async (state, phase) => {
					const { FilesystemReadView } = await state.load("../src/filesystem-evidence.ts"), readDirectory = FilesystemReadView.prototype.readDirectory;
					const root = state.opened.mock.calls[0].arguments[0].workspace.sandboxRoot;
					state.reads = state.mutations = 0;
					state.captureRead = state.method(FilesystemReadView.prototype, "readDirectory", async function(this: unknown, ...args: unknown[]) {
						if (args[0] === root) {
							state.reads++;
							if (phase === "io") throw Object.assign(new Error("directory capture I/O sentinel"), { code: "EIO" });
							// A real namespace change invalidates the before/after identity, independent of timestamp resolution.
							if (phase === "always" || state.reads === 1) await state.fs.mkdir(`${root}/capture-${phase}-${++state.mutations}`);
						}
						return readDirectory.apply(this, args);
					});
				}, phase);
				const result = await capture.finish().finally(() => fixture.inOwner(state => state.captureRead.mock.restore()));
				const counts = await fixture.inOwner(state => ({ reads: state.reads, mutations: state.mutations }));
				if (phase === "once") {
					expect(result.complete, result.complete ? undefined : result.reason).toBe(true); expect(result.after?.entries.has("capture-once-1")).toBe(true);
					expect(counts.mutations).toBe(1); expect(counts.reads).toBeGreaterThanOrEqual(2);
				} else {
					expect(result).toMatchObject({ complete: false, changes: [], reason: `workspace_transaction_capture:${phase === "io" ? "directory capture I/O sentinel" : "directory_changed_during_capture"}` });
					// Three capture attempts each compare two complete directory images.
					expect(counts).toEqual(phase === "io" ? { reads: 1, mutations: 0 } : { reads: 6, mutations: 6 });
				}
			}
		} else {
			await interval.abort();
			if (mode.startsWith("whole-")) {
				const gate = gated(), value = { own: () => "local result" }, released = mode === "whole-released";
				const apply = vi.fn(async () => { await gate.wait(); if (released && apply.mock.calls.length === 1) throw effectCommitFailure(new Error("unchanged"), "recoverable"); return value; });
				const result = session.ownership.commit(apply); void result.catch(() => undefined); await gate.entered;
				expect(await fixture.inOwner(async state => { const { ownership } = await state.opened.mock.calls[0].result; return [!!ownership.pendingWhole, !!ownership.claimChild()]; })).toEqual([true, false]);
				await expect(session.ownership.commit(async () => value)).rejects.toMatchObject({ disposition: "recoverable" });
				gate.release();
				if (released) { await expect(result).rejects.toMatchObject({ disposition: "recoverable" }); expect(await session.ownership.commit(apply)).toBe(value); }
				else expect(await result).toBe(value);
				expect(await session.ownership.commit(apply)).toBe(value); expect(apply).toHaveBeenCalledTimes(released ? 2 : 1);
			} else if (mode === "custom") {
				expect(await executeProcess(fixture, session.executor, command)).toEqual({ exitCode: 0, output: "own" }); expect(captured).toHaveBeenCalledTimes(2);
			} else await fixture.inOwner(state => {
				const driver = state.opened.mock.calls[0].arguments[0].workspace.transactions, dispose = driver.dispose;
				driver.dispose = async () => { await dispose(); throw new Error("journal cleanup sentinel"); };
			});
		}
	});
	if (mode === "dispose") await expect(running).rejects.toMatchObject({ errors: [{ message: "journal cleanup sentinel" }] }); else await running;
});

nativeTest.for(["conflict", "disjoint", "alias-conflict", "alias-disjoint", "custom-conflict", "custom-disjoint", "foreign-conflict", "foreign-disjoint", "live-conflict", "live-disjoint", "sealing-disjoint", "retired-conflict", "retired-disjoint", "custom-retired-conflict", "custom-retired-disjoint"])("attributes %s writes across public workspace sessions", { timeout: 30_000 }, async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-public-writer-", "git");
	await fixture.write("input.txt", "base");
	await fixture.write("other.txt", "base");
	await fixture.write("other-pid", "");
	if (mode.startsWith("alias-")) await filesystem.link(path.join(fixture.workspace, "input.txt"), path.join(fixture.workspace, "z-alias.txt"));
	if (mode.includes("retired-")) await fixture.write("pid", "");
	await fixture.compile("writer", { code: `${cHeaders("fcntl.h signal.h stdio.h unistd.h")}
int main(int argc, char **argv) { int fd = open(argv[2], O_WRONLY | O_TRUNC); if (fd < 0 || write(fd, argv[1], 1) != 1) return 1; close(fd);
 if (argv[1][0] == 'A' || argc > 3) { fd = open(argv[1][0] == 'A' ? "pid" : "other-pid", O_WRONLY | O_CREAT | O_TRUNC, 0600); if (fd < 0) return 2; dprintf(fd, "%d\\n", getpid()); close(fd); raise(SIGSTOP); } return 0; }` });
	await fixture.prepare();
	const second = new LinuxProcessReuseBackend({ storeRoot: path.join(fixture.root, "other-store") });
	await using _second = onExit(() => second.dispose());
	await fixture.inOwner(async state => {
		const { AsyncLocalStorage } = await state.load("node:async_hooks"), { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts");
		const owner = new AsyncLocalStorage(), open = LinuxProcessReuseBackend.prototype.open, execute = LinuxProcessReuseBackend.prototype.executeRequest;
		state.captures = []; state.patched = new WeakSet();
		state.method(LinuxProcessReuseBackend.prototype, "executeRequest", function(this: any, ...args: any[]) { return owner.run(args[0].invocation?.command, () => execute.apply(this, args)); });
		state.method(LinuxProcessReuseBackend.prototype, "open", function(this: any, input: any) {
			const driver = input.workspace.transactions;
			if (!state.patched.has(driver)) { state.patched.add(driver); const begin = driver.begin;
				driver.begin = async () => { const label = owner.getStore(), capture = await begin(); return { ...capture, finish: async (...args: any[]) => {
					const delta = await capture.finish(...args); state.captures.push({ label, complete: delta.complete, reason: delta.reason }); return delta;
				} }; };
			} return open.call(this, input);
		});
	});
	await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
		const invocation = (command: string) => fixture.invocation({ command }).process!;
		const disjoint = mode.endsWith("disjoint"), aliased = mode.startsWith("alias-"), custom = mode.startsWith("custom-"), live = mode.startsWith("live-"), foreign = live || mode.startsWith("foreign-"), retired = mode.includes("retired-");
		const command = "exec ./writer A input.txt", other = `exec ./writer B ${disjoint ? "other.txt" : aliased ? "z-alias.txt" : "input.txt"}${live ? " hold" : ""}`;
		let captureCalls = 0;
		const context = { ...workspace, transactions: { ...workspace.transactions, begin: async () => {
			captureCalls++; return workspace.transactions.begin();
		} } };
		await using first = disposable(await fixture.backend.open({ sourceRoot: fixture.workspace, workspace: custom ? context : workspace, invocation: invocation(command) }));
		await using sibling = disposable(await second.open({ sourceRoot: fixture.workspace, workspace: foreign ? context : workspace, invocation: invocation(other) }));
		const stopped = async (name: string) => {
			let pid = 0;
			await expect.poll(async () => { pid = Number(await readText(workspace.sandboxRoot, name).catch(() => "")); return pid; }, { timeout: 10_000 }).toBeGreaterThan(0);
			await expect.poll(() => readText(`/proc/${pid}/status`).then(text => /^State:\s+[Tt]/m.test(text))).toBe(true); return pid;
		};
		const running = first.executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment, signal: AbortSignal.timeout(15_000), onData: () => {} });
		void running.catch(() => undefined); const pid = await stopped("pid");
		await using _resume = onExit(async () => { if (!retired) { try { process.kill(pid, "SIGCONT"); } catch {} await running; } });
		if (retired) {
			await first.close(); await expect(running).rejects.toThrow("aborted");
			if (custom) expect(captureCalls).toBeGreaterThan(0);
			expect(await executeProcess(fixture, sibling.executor, other)).toEqual({ exitCode: 0, output: "" });
			const changes = await sibling.seal([]);
			expect(changes.map(change => change.resource), JSON.stringify({ captures: await fixture.inOwner(state => state.captures), validation: await sibling.validate() })).toEqual([disjoint ? "other.txt" : "input.txt"]);
			expect(await Promise.all(changes.map(async change => change.kind === "directory" ? [] : [(await readContent(change.before!)).toString(), (await readContent(change.after!)).toString()]))).toEqual([[disjoint ? "base" : "A", "B"]]);
			if (!disjoint) { expect(await sibling.validate()).toMatchObject({ status: "stale" }); await fixture.write("input.txt", "A"); }
			const validation = await sibling.validate(); expect(validation, JSON.stringify(validation)).toMatchObject({ status: "valid" });
			return;
		}
		let sealed = false;
		const sealing = mode.startsWith("sealing-") ? first.seal([]).then(changes => { sealed = true; return changes; }) : undefined;
		void sealing?.catch(() => undefined);
		const another = executeProcess(fixture, sibling.executor, other); void another.catch(() => undefined);
		const otherPid = live ? await stopped("other-pid") : undefined;
		await using _resumeOther = onExit(async () => { if (otherPid) try { process.kill(otherPid, "SIGCONT"); } catch {} expect(await another).toEqual({ exitCode: 0, output: "" }); });
		if (!live) expect(await another).toEqual({ exitCode: 0, output: "" });
		if (sealing) expect(sealed).toBe(false);
		process.kill(pid, "SIGCONT"); expect(await running).toEqual({ exitCode: 0 });
		if (custom || foreign) expect(captureCalls).toBeGreaterThan(0);
		const captures = await fixture.inOwner(state => state.captures as { label: string; complete: boolean; reason?: string }[]);
		const own = captures.filter(capture => capture.label === command), changes = await (sealing ?? first.seal([]));
		if (sealing) await expect(executeProcess(fixture, first.executor, command)).rejects.toThrow("consumed");
		if (!disjoint) {
			if (!custom) expect(own, JSON.stringify(captures)).toContainEqual({ label: command, complete: false, reason: "overlapping_workspace_write" });
			expect(changes).toEqual([]); expect(await first.validate()).toMatchObject({ status: "indeterminate" });
		} else {
			if (!custom) expect(own.length).toBeGreaterThan(0); expect(own.every(capture => capture.complete), JSON.stringify(captures)).toBe(true);
			expect(changes.map(change => change.resource).sort()).toEqual(["input.txt", "pid", ...aliased ? ["z-alias.txt"] : []]);
			expect(await first.validate()).toMatchObject({ status: "valid" });
		}
	});
});

nativeTest("preserves access syscall arguments and empty-path descriptors", { timeout: 30_000 }, async ({ benchmark }) => {
	const fixture = await benchmark("pi-access-", "git");
	await fixture.write("input", "readable");
	await filesystem.symlink("missing", path.join(fixture.workspace, "dangling"));
	await fixture.compile("access-check", { code: String.raw`#define _GNU_SOURCE
${cHeaders("errno.h fcntl.h stdio.h sys/syscall.h unistd.h")}
static void check(long call, int fd, const char *path, int mode, int flags) {
 errno = 0; long result = syscall(call, fd, path, mode, flags); printf("%ld %d\n", result, errno);
}
int main(void) {
 int fd = open("input", O_PATH), held = open("held", O_CREAT | O_RDWR, 0600); if (fd < 0 || held < 0 || unlink("held")) return 1;
 const char *paths[] = {"input", "missing", NULL}; int fds[] = {fd, held, -1};
 for (unsigned i = 0; i < 3; i++) {
  check(SYS_faccessat2, AT_FDCWD, paths[i], 8, 0); check(SYS_faccessat2, AT_FDCWD, paths[i], F_OK, 0x40000000);
  check(SYS_faccessat2, fds[i], "", R_OK, AT_EMPTY_PATH); check(SYS_faccessat2, fds[i], "", F_OK, 0);
  check(SYS_faccessat2, fds[i], "child", F_OK, 0);
 }
 check(SYS_faccessat, AT_FDCWD, "input", R_OK, 0x40000000); check(SYS_faccessat, AT_FDCWD, "missing", -1, 0);
 check(SYS_faccessat2, AT_FDCWD, "dangling", F_OK, 0); check(SYS_faccessat2, AT_FDCWD, "dangling", F_OK, AT_SYMLINK_NOFOLLOW);
 check(SYS_faccessat2, AT_FDCWD, "input", R_OK | W_OK, AT_EACCESS); check(SYS_faccessat2, AT_FDCWD, "input", X_OK, AT_EMPTY_PATH);
 close(fd); close(held); return 0;
}` });
	const expected = childProcess.execFileSync(path.join(fixture.workspace, "access-check"), { cwd: fixture.workspace, encoding: "utf8" });
	await fixture.prepare();
	await using branch = disposable(await fixture.fork("./access-check"));
	expect(textOutput(branch.output.result)).toBe(expected);
});

nativeTest("proves external reads from their opened image and never commits a read copy", { timeout: 30_000 }, async ({ benchmark }) => {
		const fixture = await benchmark("pi-read-image-", "git"), outside = await mkdtemp(path.join(os.tmpdir(), "pi-read-image-host-"));
		const target = path.join(outside, "input"), alias = path.join(outside, "alias");
		await using _cleanup = onExit(async () => { await fixture.dispose(); await rm(outside, { recursive: true, force: true }); });
		await writeFile(target, "old\n"); await filesystem.symlink("input", alias);
		await fixture.compile("permissions", { code: `${cHeaders("stdio.h unistd.h dirent.h string.h sys/stat.h fcntl.h")}
int main(int argc, char **argv) {
 if (argc > 3) { int fd = open(argv[1], O_RDONLY); struct stat before; if (!strcmp(argv[3], "read-stat") && fstat(fd, &before)) return 5;
  char bytes[32]; ssize_t n = read(fd, bytes, sizeof(bytes)); close(fd);
  if (argc > 4) { int output = open(argv[4], O_WRONLY | O_CREAT | O_TRUNC, 0600); if (output < 0 || write(output, bytes, n) != n || close(output)) return 4; }
  return n < 0 || write(1, bytes, n) != n; }
 FILE *f = fopen(argc > 1 ? argv[1] : "/etc/issue", "r"); if (!f) return 1; fgetc(f);
 if (argc > 1) { struct stat st; if (fstat(fileno(f), &st)) return 2; DIR *d = opendir(argv[2]); if (!d) return 3; struct dirent *e;
  while ((e = readdir(d))) if (!strcmp(e->d_name, "input")) { puts(e->d_ino == st.st_ino ? "identical" : "different"); break; } closedir(d);
 } else printf("%d %d %d\\n", access("/etc/issue", R_OK), access("/etc/issue", W_OK), access("/etc/issue", X_OK)); fclose(f); }\n` });
		await fixture.compile("waiter", { code: `${processChecks}
int main(int argc, char **argv) { if (argc < 3) return 1; pid_t child = fork();
 if (!child) { execv(argv[2], argv + 2); _exit(2); } return wait_child(child); }\n` });
		await fixture.prepare(); const route = await fixture.prepareActorReplay(), scope = { sessionID: "read-image", turnID: "actor" };
		for (const args of [[], [target, outside]]) {
			await using permissionBranch = disposable(await fixture.fork(`./permissions ${args.map(arg => `'${arg}'`).join(" ")}`));
			expect(textOutput(permissionBranch.output.result)).toBe(childProcess.execFileSync(path.join(fixture.workspace, "permissions"), args, { encoding: "utf8" }));
		}
		for (const [label, pathname] of [["direct", target], ["alias", alias], ["predecessor", target], ["created", `${target}-new`], ["sibling", `${target}-sibling`], ["waited-sibling", `${target}-waited`], ["changed-waited-sibling", `${target}-waited-changed`], ["changed-sibling", `${target}-changed`], ["changed", target]] as const) {
			const prepared = !["direct", "alias"].includes(label), sibling = label.endsWith("sibling");
			const command = prepared ? `${label.includes("waited") ? "./waiter producer " : ""}./permissions '${pathname}' unused read${sibling ? ` '${pathname}-out'` : ""}` : `cat '${pathname}'`;
			const prefix = prepared ? `printf 'prepared\\n' > '${pathname}'; ` : "", expected = prepared ? "prepared\n" : "old\n", changed = label.startsWith("changed");
			await using branch = disposable(await fixture.fork(`: '${label}'; ${prefix}${command}`, { executionScope: scope }));
			expect(textOutput(branch.output.result), label).toBe(expected);
			const validation = await branch.validate?.();
			// The shell inspected its write without prior metadata; the independent byte-reading child remains reusable.
			expect(validation, JSON.stringify({ label, validation, metrics: fixture.backend.metrics() })).toMatchObject({ status: prepared ? "indeterminate" : "valid" });
			if (prepared) expect(JSON.stringify(validation)).toContain(`metadata_after_write:metadata:follow:${pathname}`);
			const before = fixture.backend.actorMetrics().hits;
			expect((await executeProcess(fixture, route.executor, `${changed ? prefix.replace("prepared", "different") : prefix}true; ${command.replace("./waiter producer", "./waiter actor")}`, { scope })).output).toBe(changed ? "different\n" : expected);
			expect(fixture.backend.actorMetrics().hits - before, JSON.stringify({ label, metrics: fixture.backend.metrics() })).toBe(changed ? 0 : 1);
			expect(await readText(pathname)).toBe(changed ? "different\n" : expected);
			if (sibling) expect(await readText(`${pathname}-out`)).toBe(changed ? "different\n" : expected);
		}
		await writeFile(target, "old\n");
		const modified = (await filesystem.stat(target, { bigint: true })).mtimeNs, stamp = `${modified / 1_000_000_000n}.${String(modified % 1_000_000_000n).padStart(9, "0")}`;
		await using restored = disposable(await fixture.fork(`./permissions '${target}' unused read-stat; printf 'old\\n' > '${target}'; touch -m -d '@${stamp}' '${target}'`, { executionScope: scope }));
		expect(textOutput(restored.output.result)).toBe("old\n");
		expect(await restored.validate?.()).toMatchObject({ status: "valid" });
		await writeFile(target, "new\n");
		expect(await restored.validate?.()).toMatchObject({ status: "stale" });
		await writeFile(target, "old\n");
		await fixture.inOwner((state, target) => {
			const read = state.fs.readFile; state.changed = false;
			state.method(state.fs, "readFile", async (...args: unknown[]) => {
				const data = await read(...args);
				if (!state.changed && String(args[0]).endsWith("/proof.json") && JSON.parse(String(data)).path === target) { state.changed = true; await state.fs.writeFile(target, "new\n"); }
				return data;
			});
		}, target);
		await using branch = disposable(await fixture.fork(`cat '${target}'`));
		expect(await fixture.inOwner(state => state.changed)).toBe(true); expect(textOutput(branch.output.result)).toBe("old\n");
		expect(await branch.validate?.()).toMatchObject({ status: "stale" });
		expect(textOutput(await fixture.tool.execute("raced-actor", { command: `cat '${target}'` }))).toBe("new\n");
		await branch.commit(); expect(await readText(target)).toBe("new\n");
	});

nativeTest("reuses private overwrites against the child's actual prestate", { timeout: 30_000 }, async ({ benchmark }) => {
	const fixture = await benchmark("pi-private-prestate-", "git"), outside = await mkdtemp(path.join(os.tmpdir(), "pi-private-prestate-host-"));
	await using _cleanup = onExit(async () => { await fixture.dispose(); await rm(outside, { recursive: true, force: true }); });
	await fixture.compile("overwrite", { code: `${cHeaders("fcntl.h unistd.h string.h sys/stat.h stdio.h")}
static int identity(struct stat s) { char text[32]; int n = snprintf(text, sizeof(text), "%llu:", (unsigned long long)s.st_ino); return write(1, text, n) != n; }
int main(int argc, char **argv) { if (argc < 2) return 1; int fd; char bytes[64]; struct stat s;
 if (argc == 3 && argv[2][0] == '=') { fd = open(argv[1], O_WRONLY | O_CREAT | O_TRUNC, 0600); return fd < 0 || write(fd, argv[2]+1, strlen(argv[2]+1)) != (ssize_t)strlen(argv[2]+1) || close(fd); }
 if (argc == 4) { if (!strcmp(argv[2], "race")) { fd = open(argv[1], O_WRONLY); if (fd < 0) return 6;
   char ready[4096]; snprintf(ready, sizeof(ready), "%s.ready", argv[3]); int signal = open(ready, O_WRONLY | O_CREAT, 0600); if (signal < 0 || close(signal)) return 7;
   while (access(argv[3], F_OK)) usleep(1000); return write(fd, "raced", 5) != 5 || ftruncate(fd, 5) || close(fd); }
  fd = open(argv[3], O_WRONLY | O_CREAT, 0600); if (fd < 0 || close(fd)) return 2; usleep(200000); }
 if (argc == 3 && !strcmp(argv[2], "stat") && (stat(argv[1], &s) || identity(s))) return 4;
 fd = open(argv[1], O_RDONLY); if (fd >= 0) { ssize_t n = read(fd, bytes, sizeof(bytes)); if (n < 0 || write(1, bytes, n) != n || close(fd)) return 3; }
 if (argc == 3 && !strcmp(argv[2], "read")) { usleep(2100000); return 0; }
 if (argc == 3 && !strcmp(argv[2], "delete")) return unlink(argv[1]) || write(1, "done\\n", 5) != 5;
 fd = open(argv[1], O_WRONLY | O_CREAT | O_TRUNC, 0600);
 return fd < 0 || write(fd, "after\\n", 6) != 6 || (argc == 3 && fstat(fd, &s)) ||
  (argc == 3 && !strcmp(argv[2], "stat-after") && identity(s)) || close(fd) || write(1, "done\\n", 5) != 5; }\n` });
	await fixture.prepare(); const route = await fixture.prepareActorReplay(), scope = { sessionID: "private-prestate", turnID: "actor" };
	const pooled = path.join(outside, "pooled"), read = `./overwrite '${pooled}' read; `;
	await fixture.inOwner(state => { state.opened = state.method(state.fs, "open"); });
	await using snapshot = disposable(await fixture.fork(`./overwrite '${pooled}' =prepared; ${read.repeat(3)}:`));
	expect(textOutput(snapshot.output.result)).toBe("prepared".repeat(3)); expect(await snapshot.validate?.()).toMatchObject({ status: "valid" });
	await snapshot.commit(); expect(await readText(pooled)).toBe("prepared");
	expect(await fixture.inOwner((state, target) => state.opened.mock.calls.filter((call: any) =>
		String(call.arguments[0]).includes("/writes/upper/") && String(call.arguments[0]).endsWith(target) && call.stack.stack.includes("observeStat")).length, pooled)).toBeLessThanOrEqual(2);
	await fixture.inOwner(state => state.opened.mock.restore());
	for (const mode of ["existing", "created", "changed", "deleted", "deleted-changed", "absent", "appeared"]) {
		const target = path.join(outside, mode), removed = mode.startsWith("deleted"), absent = ["created", "absent", "appeared"].includes(mode), changed = mode.endsWith("changed") || mode === "appeared";
		const command = `./overwrite '${target}'${removed ? " delete" : ""}`, prefix = ["absent", "appeared"].includes(mode) ? "" : `./overwrite '${target}' =prepared; `;
		if (!absent) await writeFile(target, "old");
		await using branch = disposable(await fixture.fork(`: producer; ${prefix}${command}`, { executionScope: scope }));
		expect(textOutput(branch.output.result)).toBe(`${prefix ? "prepared" : ""}done\n`);
		expect(await readText(target).catch(() => "absent")).toBe(absent ? "absent" : "old");
		const validation = await branch.validate?.(); expect(validation, JSON.stringify({ mode, validation, metrics: fixture.backend.metrics() })).toMatchObject({ status: "valid" });
		const before = fixture.backend.actorMetrics().hits;
		if (mode === "appeared") await writeFile(target, "changed");
		expect(await executeProcess(fixture, route.executor, `: actor; ${changed ? prefix.replace("prepared", "changed") : prefix}${command}`, { scope })).toEqual({ exitCode: 0, output: `${changed ? "changed" : prefix ? "prepared" : ""}done\n` });
		expect(await readText(target).catch(() => "absent")).toBe(removed ? "absent" : "after\n");
		expect(fixture.backend.actorMetrics().hits - before, JSON.stringify({ mode, metrics: fixture.backend.metrics() })).toBe(changed ? 0 : prefix ? 2 : 1);
	}
	for (const mode of ["stat", "stat-after", "stat-created", "stat-created-child", "stat-created-child-workspace"]) {
		const inspected = path.join(mode.endsWith("workspace") ? fixture.workspace : outside, mode), replacement = path.join(outside, "replacement"), absent = mode.startsWith("stat-created");
		const child = mode.includes("child"), inspect = `${child ? `./overwrite '${inspected}' =same; ` : ""}./overwrite '${inspected}' ${mode === "stat" || child ? "stat" : "stat-after"}`;
		if (!absent) await writeFile(inspected, "same");
		await using metadata = disposable(await fixture.fork(`: producer; ${inspect}`, { executionScope: scope }));
		const oldIdentity = await filesystem.stat(inspected).then(info => info.ino, () => undefined);
		const expected = mode === "stat" ? `${oldIdentity}:samedone\n` : expect.stringMatching(child ? /^\d+:samedone\n$/ : absent ? /^\d+:done\n$/ : /^same\d+:done\n$/);
		expect(textOutput(metadata.output.result)).toEqual(expected);
		expect(await metadata.validate?.()).toMatchObject(mode === "stat" ? { status: "valid" } : child ? { status: "stale", cause: { detail: expect.stringContaining(inspected) } }
			: { status: "indeterminate", cause: { detail: expect.stringContaining(`metadata_after_write:metadata:follow:${inspected}`) } });
		if (!absent) { await writeFile(replacement, "same"); await filesystem.rename(replacement, inspected); }
		const replacementIdentity = await filesystem.stat(inspected).then(info => info.ino, () => undefined); if (!absent) expect(replacementIdentity).not.toBe(oldIdentity);
		const metadataHits = fixture.backend.actorMetrics().hits, actual = await executeProcess(fixture, route.executor, `: actor; ${inspect}`, { scope });
		const newIdentity = replacementIdentity ?? (await filesystem.stat(inspected)).ino;
		expect(actual).toEqual({ exitCode: 0, output: mode === "stat" || child ? `${newIdentity}:samedone\n` : `${absent ? "" : "same"}${newIdentity}:done\n` });
		expect(fixture.backend.actorMetrics().hits - metadataHits, mode).toBe(child ? 1 : 0);
	}
	const target = path.join(outside, "raced"), marker = path.join(outside, "marker"), command = `./overwrite '${target}' slow '${marker}'`, prefix = `printf prepared > '${target}'; `;
	await writeFile(target, "old");
	await using racing = disposable(await fixture.fork(`${prefix}./overwrite '${target}' race '${marker}' & while [ ! -e '${marker}.ready' ]; do sleep 0.01; done; ${command}; wait`, { executionScope: scope }));
	expect(textOutput(racing.output.result)).toBe("raceddone\n"); expect(await readText(target)).toBe("old");
	const before = fixture.backend.actorMetrics().hits;
	expect(await executeProcess(fixture, route.executor, `${prefix}${command}`, { scope })).toEqual({ exitCode: 0, output: "prepareddone\n" });
	expect(fixture.backend.actorMetrics().hits - before, JSON.stringify(fixture.backend.metrics())).toBe(0);
});

nativeTest("admits expensive late launches by completed work within a bounded learning budget", { timeout: 30_000 }, async ({ benchmark }) => {
		const fixture = await benchmark("pi-learning-admission-", "git");
		await fixture.prepare();
		const route = await fixture.prepareActorReplay(), scope = { sessionID: "admission", turnID: "native" };
		const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string; args: readonly string[] }>;
		const command = [...Array.from({ length: 66 }, (_, index) => `/bin/true ${index}`), "/bin/sleep 0.12",
			...Array.from({ length: 66 }, (_, index) => `/bin/true ${index + 66}`)].join("; ");
		await fixture.backend.observeBindings(scope, () => route.executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment,
			scope, onData: () => {} }), bindings => {
			expect(bindings.length).toBeGreaterThan(0); expect(bindings.length).toBeLessThanOrEqual(64);
			const slow = bindings.find(binding => handoffs.resolveBinding(binding, scope)?.executable.endsWith("/sleep"));
			expect(slow, JSON.stringify(fixture.backend.actorMetrics())).toMatchObject({ available: true });
			expect(slow!.executionMs).toBeGreaterThanOrEqual(100);
		}, true);
	});

nativeTest.for(["file", "symlink", "lexical-parent", "normalized-blocker"] as const)("proves ENOTDIR from the blocking file and invalidates a changed walk (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
		const fixture = await benchmark("pi-enotdir-", "overlayfs", {}, os.homedir());
		const blocker = path.join(fixture.workspace, "module.js");
		await writeFile(blocker, "source\n");
		await fixture.write("package.json", "{}\n");
		await filesystem.symlink(mode === "normalized-blocker" ? "inner/dir" : "module.js", path.join(fixture.workspace, "alias"));
		if (mode === "normalized-blocker") {
			await mkdir(path.join(fixture.workspace, "inner/dir"), { recursive: true });
			await mkdir(path.join(fixture.workspace, "inner/module.js"));
			await filesystem.symlink("../../module.js", path.join(fixture.workspace, "inner/module.js/package.json"));
		}
		await fixture.commit("blocked module lookup");
		await fixture.prepare();
		const target = mode === "normalized-blocker" ? "alias/../module.js/package.json" : mode === "symlink" ? "alias/package.json" : mode === "lexical-parent" ? "module.js/../package.json" : "module.js/package.json";
		const script = `import os\ntry:\n os.stat(${JSON.stringify(target)})\n print(0)\nexcept OSError as error:\n print(error.errno)`;
		const command = `if [ -e '${target}' ]; then printf present; else printf blocked; fi`;
		expect(childProcess.execFileSync("/usr/bin/python3", ["-c", script], { cwd: fixture.workspace, encoding: "utf8" })).toBe(mode === "normalized-blocker" ? "0\n" : "20\n");
		await using branch = disposable(await fixture.fork(command));
		const native = childProcess.execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: fixture.environment, encoding: "utf8" });
		expect(native).toBe(mode === "normalized-blocker" ? "present" : "blocked");
		const validation = await branch.validate?.();
		expect(textOutput(branch.output.result)).toBe(native);
		if (mode === "normalized-blocker") {
			expect(validation).toMatchObject({ status: "indeterminate" });
			expect(JSON.stringify(validation)).toContain("pathname_walk:");
		} else {
			expect(validation, JSON.stringify(validation)).toMatchObject({ status: "valid" });
			const changed = mode === "symlink" ? path.join(fixture.workspace, "alias") : blocker;
			await rm(changed); await mkdir(changed);
			await writeFile(path.join(changed, "package.json"), "{}\n");
			expect(await branch.validate?.()).toMatchObject({ status: expect.stringMatching(/^(stale|indeterminate)$/) });
			expect(childProcess.execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: fixture.environment, encoding: "utf8" })).toBe("present");
		}
	});


nativeTest.for(["missing", "appeared", "file", "blocked", "symlink", "lexical", "lexical-leaf", "lexical-written", "lexical-directory", "lexical-file", "lexical-symlink", "socket", "socket-file", "socket-blocked", "socket-listening", "socket-private", "socket-private-blocked", "socket-mapped", "socket-relative", "socket-link"] as const)("proves only absent host lookups and cleans its long-path broker (%s)", { timeout: 20_000 }, async (mode, { benchmark }) => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-long-broker-")), previous = process.env.TMPDIR;
		const temporary = path.join(root, "nested-temporary-directory-".repeat(3));
		await mkdir(temporary); process.env.TMPDIR = temporary;
		const prefix = path.join(root, "candidate"), absent = ["missing", "socket", "lexical", "lexical-leaf", "lexical-directory"].includes(mode);
		let target = `${prefix}${mode.endsWith("blocked") ? "/child" : mode.startsWith("lexical") ? "/../other" : ""}`;
		if (mode.endsWith("file") || mode === "blocked" || mode === "socket-blocked") await writeFile(prefix, "prepared");
		if (mode === "lexical-directory") await mkdir(prefix);
		if (mode === "symlink" || mode === "socket-link" || mode === "lexical-symlink") await filesystem.symlink(mode === "lexical-symlink" ? temporary : "absent", prefix);
		let connections = 0;
		const listener = mode === "socket-listening" ? net.createServer(socket => { connections++; socket.destroy(); }) : undefined;
		if (listener) { listener.listen(target); await once(listener, "listening"); }
		let fixture: Awaited<ReturnType<typeof benchmark>> | undefined;
		await using _cleanup = onExit(async () => {
			if (listener) await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
			await fixture?.dispose();
			if (previous === undefined) delete process.env.TMPDIR; else process.env.TMPDIR = previous;
			await rm(root, { recursive: true, force: true });
		});
		fixture = await benchmark("command-", "git");
		await fixture.inOwner(async (state, mode, target) => {
			const { FilesystemReadView } = await state.load("../src/filesystem-evidence.ts"), stat = FilesystemReadView.prototype.stat;
			state.listening = state.method(state.net.Server.prototype, "listen"); state.permissions = state.method(state.fs, "chmod");
			state.method(FilesystemReadView.prototype, "stat", async function(this: unknown, ...args: unknown[]) {
				if (mode === "appeared" && String(args[0]) === target) await state.fs.writeFile(target, "appeared");
				return Reflect.apply(stat, this, args);
			});
		}, mode, target);
		await fixture.write("input.txt", "input\n");
		if (mode.startsWith("socket")) {
			await fixture.compile("connect", { code: String.raw`${cHeaders("sys/socket.h sys/un.h errno.h stdio.h string.h unistd.h")}
int main(int argc, char **argv) {
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	if (argc != 2 || strlen(argv[1]) >= sizeof(address.sun_path)) return 2;
	strcpy(address.sun_path, argv[1]); int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	int result = connect(fd, (struct sockaddr *)&address, sizeof(address));
	printf("%d\n", result < 0 ? errno : 0); close(fd); return 0;
}
` });
		}
		if (mode === "socket-mapped") target = `${fixture.workspace}/candidate`;
		await fixture.commit("long temporary path");
		await fixture.prepare();
		const privateWrite = mode.startsWith("socket-private") || mode === "socket-mapped";
		const command = mode.startsWith("socket") ? `${privateWrite ? `printf private > '${mode === "socket-private-blocked" ? path.dirname(target) : target}'; ` : ""}${mode === "socket-relative" ? `cd '${root}'; ` : ""}'${fixture.workspace}/connect' '${mode === "socket-relative" || mode === "socket-mapped" ? "candidate" : target}'` : `${mode === "lexical-written" ? `printf private > '${prefix}'; ` : ""}/bin/cat '${target}' 2>/dev/null || printf prepared`;
		{
			await using branch = disposable(await fixture.fork(command));
			expect(textOutput(branch.output.result)).toBe(mode.startsWith("socket") ? `${mode.endsWith("blocked") ? 20 : privateWrite || mode === "socket-listening" || mode === "socket-file" ? 13 : 2}\n` : "prepared");
			if (!privateWrite) expect(await branch.validate?.()).toMatchObject({ status: absent || mode === "file" ? "valid" : "indeterminate" });
			if (mode === "appeared") expect(await readText(target)).toBe("appeared");
			if (privateWrite) await expect(filesystem.access(target)).rejects.toMatchObject({ code: "ENOENT" });
			expect(connections).toBe(0);
			if (absent) {
				if (mode === "lexical-directory") await filesystem.rmdir(prefix);
				if (mode === "lexical") await mkdir(prefix);
				else await writeFile(mode === "lexical-directory" ? prefix : path.resolve(target), "appeared");
				expect(await branch.validate?.()).toMatchObject({ status: mode === "lexical-directory" ? "indeterminate" : "stale" });
			}
		}
		const sockets = await fixture.inOwner(state => state.listening.mock.calls.flatMap(({ arguments: [target] }: { arguments: unknown[] }) =>
			typeof target === "string" && target.startsWith("/tmp/pi-broker-") ? [target] : []) as string[]);
		expect(sockets).toHaveLength(1);
		expect(await fixture.inOwner(state => state.permissions.mock.calls.map(({ arguments: args }: { arguments: unknown[] }) => args))).toContainEqual([sockets[0], 0o600]);
		await expect(filesystem.access(sockets[0]!)).rejects.toMatchObject({ code: "ENOENT" });
	});

nativeTest.for(["unusable", "one-shot", "descendant", "unusable-descendant"] as const)("rechecks failed native preparation when its observed inputs change (%s)", { timeout: 20_000 }, async (mode, { benchmark }) => {
		const fixture = await benchmark("pi-preparation-evidence-", undefined, {}, os.homedir());
		const publications = await recordProcessPublications(fixture);
		const failedChild = mode === "unusable-descendant", print = 'printf "prepared\\n"\n';
		const { write } = fixture;
		await write("input.txt", "prepared\n");
		await write("body.sh", 'ulimit -c 0\n' + (failedChild ? "" : print));
		if (failedChild) await write("unusable-child.sh",
			'i=0; while [ "$i" -lt 10000 ]; do i=$((i + 1)); done\nulimit -c 0\n' + print);
		if (mode === "one-shot") {
			await fixture.compile("one-shot", { code: '#include <stdio.h>\n#include <unistd.h>\nint main(void) { usleep(20000); printf("prepared:%ld\\n", (long)getpid()); return 0; }\n' });
		}
		await fixture.commit("Preparation evidence");
		// This failure-cache case keeps script metadata unchanged by the child's own reads.
		const scriptPath = path.join(fixture.workspace, "unusable-child.sh");
		if (failedChild) await filesystem.utimes(scriptPath, Date.now() / 1000 + 3600, (await stat(scriptPath)).mtime);
		await fixture.prepare();
		const route = await fixture.prepareActorReplay(), scope = { sessionID: "preparation-evidence", turnID: "native" };
		// Changing a process limit is an existing unsupported syscall observation. Only this child changes its limit.
		const script = 'i=0; while [ "$i" -lt 10000 ]; do i=$((i + 1)); done; ' +
			(mode === "descendant" ? "ulimit -c 0; /bin/cat input.txt; :" :
				failedChild ? ". ./body.sh; /bin/bash ./unusable-child.sh; :" : ". ./body.sh");
		const command = mode === "one-shot" ? "exec ./one-shot" : `exec /bin/sh -c '${script}'`;
		const expectedOutput = mode === "one-shot" ? expect.stringMatching(/^prepared:\d+\n$/) : "prepared\n";
		const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ readonly args: readonly string[]; readonly executable: string }>;
		let binding: ProcessExecutionBinding | undefined, childBinding: ProcessExecutionBinding | undefined;
		const expectStale = (value: boolean, current = binding!) => expect(fixture.backend.bindingStale(current)).resolves.toBe(value);
		const observe = () => fixture.backend.observeBindings(scope, () => executeProcess(fixture, route.executor, command, { scope }), bindings => {
			binding = bindings.find(candidate => {
				const invocation = handoffs.resolveBinding(candidate, scope);
				return mode === "one-shot" ? invocation?.executable.endsWith("/one-shot") : invocation?.args.includes(script);
			}) ?? binding;
			childBinding = bindings.find(candidate => handoffs.resolveBinding(candidate, scope)?.args.includes("./unusable-child.sh")) ?? childBinding;
		}, true);
		expect((await observe()).output).toEqual(expectedOutput);
		expect(binding, JSON.stringify(fixture.backend.actorMetrics())).toMatchObject({ available: true });
		expect(fixture.backend.operationHints(binding!).preparation).toBe("current_workspace");
		// The session excludes its shell from interposition. Keep the selected parent excluded, not the distinct child under test.
		const invocation = resolvePiToolInvocation("bash", { command: "exit 97" }, { cwd: fixture.workspace, environment: fixture.environment,
			shellPath: failedChild ? "/bin/sh" : fixture.shellPath })!.process!;
		const prepare = () => fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
			withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: { ...scope, turnID: "prepared" } }, binding!, expectedOutput, async session => {
				if (mode === "descendant") expect(session.executionBindings().some(child => child.available && child.key !== binding!.key)).toBe(true);
			}, false), { liveLower: true });
		const expectUnusable = () => expect(prepare()).rejects.toThrow("bound process preparation produced no reusable result");
		if (mode === "unusable" || failedChild) {
			const before = fixture.backend.metrics().requests;
			await expectUnusable();
			expect(binding!.available).toBe(true);
			await expectStale(false);
			expect(fixture.backend.metrics().lastError).toContain("unsupported_syscall");
			if (failedChild) {
				expect(fixture.backend.metrics().requests - before).toBe(2);
				expect((await publications()).map(certificate => certificate.dependencyCertificate.taints))
					.toEqual([expect.arrayContaining(["unsupported_syscall"]), expect.arrayContaining(["unsupported_syscall"])]);
				expect(childBinding).toBeDefined();
				await expectStale(false, childBinding!);
				await mkdir(path.join(fixture.root, "sibling"));
				await expectStale(true); await expectStale(true, childBinding!);
				await expectUnusable(); await expectStale(false); await expectStale(false, childBinding!);
			}
			await write("input.txt", "arbitrary edit\n");
			expect((await observe()).output).toBe("prepared\n");
			await expectStale(false); // An unrelated edit cannot revive unchanged failure inputs.
			await write("body.sh", failedChild ? "" : print);
			if (childBinding) await write("unusable-child.sh", print);
			await expectStale(true);
			if (childBinding) await expectStale(true, childBinding);
			await prepare();
			await expectStale(false);
		} else {
			await prepare();
			expect(binding!.available).toBe(true);
			if (mode === "one-shot") {
				expect(fixture.backend.metrics().published).toBe(0);
				expect((await publications()).find(certificate => certificate.weakKey === binding!.key)?.dependencyCertificate.taints)
					.toEqual(expect.arrayContaining(["clock", "random", "pid_observation"]));
				await using branch = disposable(await fixture.fork("./one-shot; ./one-shot; :"));
				const lines = textOutput(branch.output.result).trim().split("\n");
				expect(lines).toEqual([expect.stringMatching(/^prepared:\d+$/), expect.stringMatching(/^prepared:\d+$/)]);
				expect(new Set(lines).size).toBe(2);
				await branch.commit();
			}
		}
	});

nativeTest.for([false, true])("prepares a failed command after its reported input is edited and validates before committing (stale=%s)", { timeout: 20_000 }, async (stale, { benchmark }) => {
		const fixture = await benchmark("pi-failed-command-");
		await fixture.write("source.txt", "broken\n");
		await fixture.commit("failed command input");
		await fixture.prepare();
		const writer = createWriteTool(fixture.workspace), tools = [fixture.tool, writer], sessionID = "failed-command";
		const command = 'IFS= read -r value < source.txt; if [ "$value" != fixed ]; then printf "source.txt:1: expected fixed, got %s\\n" "$value"; exit 1; fi; printf "%s\\n" "$value" > generated.txt; printf "verified\\n"';
		const prepared = patternProcessHost(fixture, sessionID, { prepareObservedOperations: false, patternAware: patternAwareSettings({ presets: ["retry-failed-command"] }) }, {}, tools), { events, start } = prepared;
		await using host = disposable(prepared.host);
		const call = (turnID: string) => prepared.call(turnID, command);
		await start("failure");
		await expect(host.execute(call("failure"), undefined, () => fixture.tool.execute("failure", { command }))).rejects.toThrow("got broken");
		await host.finishTurn("failure");
		expect(fixture.backend.executionBindings({ sessionID, turnID: "failure" })).toEqual([]);
		await start("repair");
		const args = { path: "source.txt", content: "fixed\n" };
		await host.execute({ turnID: "repair", id: "repair", tool: "write", args, tools }, undefined, () => writer.execute("repair", args));
		await prepared.waitFor(() => events.some(event => event.type === "candidate" && event.candidate.source === "pattern_aware" &&
			event.candidate.tool === "bash" && event.state.status === "succeeded"));
		expect(existsSync(path.join(fixture.workspace, "generated.txt"))).toBe(false);
		await host.finishTurn("repair");
		if (stale) await fixture.write("source.txt", "changed\n");
		await start("retry");
		const fallback = vi.fn(() => fixture.tool.execute("retry", { command })), result = host.execute(call("retry"), undefined, fallback);
		if (stale) {
			await expect(result).rejects.toThrow("got changed");
			expect(existsSync(path.join(fixture.workspace, "generated.txt"))).toBe(false);
		} else {
			expect((await result).content).toEqual([fauxText("verified\n")]);
			expect(await fixture.read("generated.txt")).toBe("fixed\n");
		}
		await host.finishTurn("retry");
		expect(fallback).toHaveBeenCalledTimes(1);
		expect(events.filter(event => event.type === "prediction")).toContainEqual(expect.objectContaining({ settlement:
			expect.objectContaining({ observation: "observed", match: expect.objectContaining({ matched: true, adoption:
			expect.objectContaining({ status: "rejected", cause: expect.objectContaining({ stage: "freshness", ...(stale ? {} : { detail: expect.stringContaining("metadata_after_write") }) }) }) }) }) }));
	});

nativeTest.for(["bash", "write", "stale", "disabled", "bash:shared", "write:shared", "stale:shared", "disabled:shared", "bash:parents", "bash:aliases", "bash:both", "bash:repeat", "write:repeat", "bash:race"])("reprepares changed children beyond fresh beam entries (%s)", { timeout: 20_000 }, async (scenario, { benchmark }) => {
	const [mode, layout] = scenario.split(":"), target = layout === "parents" ? "first" : "last", both = layout === "both";
	const fixture = await benchmark("pi-rerun-frontier-", "git"), scope = { sessionID: "frontier", turnID: "seed" };
	for (const name of ["first", "last"]) await fixture.write(name, "old\n");
	await fixture.compile("worker", { code: `${cHeaders("fcntl.h")}
${processChecks}
int main(int argc, char **argv) { if (argc != 2) return 1; int fd = open(${layout ? '"last"' : "argv[1]"}, O_RDONLY); char bytes[16];
 int length = read(fd, bytes, sizeof(bytes)); if (length < 0 || close(fd)) return 2;
 burn_cpu(40000000ul); return write(1, bytes, (size_t)length) != length; }
` });
	await fixture.commit("changed child frontier"); await fixture.prepare();
	const route = await fixture.prepareActorReplay(), writer = createWriteTool(fixture.workspace), tools = [fixture.tool, writer];
	const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string; args: readonly string[] }>;
	const settings = { enabled: true, drafterEnabled: false, candidateLimit: 4, maxConcurrentActions: 4, tools: [], prepareObservedOperations: false, patternAware: patternAwareSettings({ enabled: false }) };
	const interrupted = layout === "race" ? new AbortController() : undefined, gate = interrupted && gated(2);
	const prepared = patternProcessHost(fixture, scope.sessionID, settings, {}, tools), { events, start, call } = prepared;
	await using host = disposable(prepared.host);
	await using _gates = onExit(async () => { gate?.release(); });
	const execute = (turnID: string, command: string) => host!.execute({ ...call(turnID, command), id: `${turnID}:${command}` }, AbortSignal.timeout(10_000),
		() => fixture.coordinator.runWith(route.executor, () => fixture.tool.execute(turnID, { command })));
	await start("seed");
	const commands = layout === "parents" ? ["printf parent >> marker; worker first", "worker last"] : ["printf parent >> marker; worker first; worker last", ...layout === "aliases" ? ["printf ''; worker first; worker last"] : []];
	for (const command of commands) expect(textOutput(await execute("seed", command))).toBe("old\n".repeat(command.includes("; worker last") ? 2 : 1));
	await host.finishTurn("seed");
	const bindings = fixture.backend.executionBindings(scope).filter(binding => registry.resolveBinding(binding, scope)?.executable.endsWith("/worker"));
	expect(bindings).toHaveLength(2);
	const consumed = bindings.find(binding => registry.resolveBinding(binding, scope)?.args.includes(target))!;
	settings.prepareObservedOperations = true;
	await start("prepared");
	await prepared.waitFor(() => bindings.every(binding => events.some(event => event.type === "candidate" && event.candidate.kind === "operation" &&
		event.candidate.predictedAction.includes(binding.key) && event.state.status === "succeeded")));
	expect(await Promise.all(bindings.map(binding => fixture.backend.bindingStale(binding)))).toEqual([false, false]); await host.finishTurn("prepared");
	const preparations = (turnID: string) => events.filter(event => event.type === "candidate" && event.turnID === turnID &&
		event.candidate.mode === "observed-work" && bindings.some(binding => event.candidate.predictedAction.includes(binding.key)));
	const lookup = fixture.backend.bindingStale.bind(fixture.backend), resources = await import("../src/resource-version.ts"), observeChanges = resources.observeResourceChanges;
	const publications = await recordProcessPublications(fixture);
	using changes = vi.spyOn(resources, "observeResourceChanges").mockImplementationOnce(async (...args) => {
		const token = await observeChanges(...args); interrupted?.abort(); return token;
	});
	using checking = vi.spyOn(fixture.backend, "bindingStale").mockImplementation(async binding => {
		const stale = await lookup(binding); if (!stale && gate && bindings.includes(binding)) await gate.wait(); return stale;
	});
	await start("unrelated", interrupted?.signal); await execute("unrelated", "printf ignored > unrelated");
	if (interrupted) await expect.poll(() => interrupted.signal.aborted).toBe(true);
	else {
		await expect.poll(() => checking.mock.calls.length).toBeGreaterThan(0); await Promise.all(checking.mock.results.map(result => result.value)); await nextTurn();
	}
	await host.finishTurn("unrelated");
	expect(preparations("unrelated")).toEqual([]);
	if (mode === "disabled") settings.prepareObservedOperations = false;
	await start("edit");
	if (gate) await gate.entered;
	for (const value of layout === "repeat" ? ["new", "two"] : ["new"]) {
		if (mode === "write") {
			const args = { path: "last", content: `${value}\n` };
			await host.execute({ turnID: "edit", id: `edit:${value}`, tool: "write", args, tools }, undefined, () => writer.execute("edit", args));
		} else await execute("edit", `printf '${value}\\n' > last`);
		if (gate) {
			await expect.poll(() => changes.mock.calls.length).toBe(2); await Promise.all(changes.mock.results.map(result => result.value)); await nextTurn(); gate.release();
		}
		if (mode !== "disabled") await prepared.waitFor(() => preparations("edit").filter(event => event.type === "candidate" && event.state.status === "succeeded").length ===
			(layout ? 2 : 1) * (value === "two" ? 2 : 1));
	}
	expect((await publications()).map(certificate => certificate.weakKey).sort()).toEqual(mode === "disabled" ? [] : (layout ? bindings : [consumed]).flatMap(binding => layout === "repeat" ? [binding.key, binding.key] : [binding.key]).sort());
	if (mode === "stale") await fixture.write("last", "bad\n");
	const before = fixture.backend.actorMetrics().hits, actual = await execute("edit", `printf actual; ${both ? "worker first; " : ""}worker ${target}`);
	await host.finishTurn("edit", true);
	const computation = events.filter(event => event.type === "actor_action").at(-1)!.settlement.provider.toolExecution, timing = consumedTiming(computation);
	expect(textOutput(actual)).toBe(`actual${`${mode === "stale" ? "bad" : layout === "repeat" ? "two" : "new"}\n`.repeat(both ? 2 : 1)}`);
	expect(await fixture.read("marker")).toBe("parent");
	const reused = mode === "bash" || mode === "write";
	expect([fixture.backend.actorMetrics().hits - before, timing.hiddenComputeMs > 0]).toEqual([reused ? both ? 2 : 1 : 0, reused]);
	const receipts = new Set(TimelineInterval.serialize(computation)!.nodes.flatMap(node => node.inputs?.filter(input => input.reused).map(input => input.id) ?? []));
	expect((await publications()).filter(certificate => receipts.has(certificate.result.computation!.root)).map(certificate => certificate.weakKey).sort())
		.toEqual(reused ? (both ? bindings : [consumed]).map(binding => binding.key).sort() : []);
	if (reused) expect(timing.hiddenByMode).toEqual([{ source: "execution_preparation", mode: "observed-work", hiddenComputeMs: timing.hiddenComputeMs }]);
	if (mode === "disabled") expect(preparations("edit")).toEqual([]);
});

nativeTest.for(["async", "sync"])("credits borrowed descriptor inputs only after native adoption (%s)", async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-borrowed-descriptor-", "git"), scope = { sessionID: "borrowed-input", turnID: "actor" };
	await fixture.write("input", "original\n");
	await fixture.compile("worker", { code: `${processChecks}\nint main(void) { char bytes[16]; ssize_t n = read(3, bytes, sizeof(bytes));
 burn_cpu(40000000ul); return n < 0 || write(1, bytes, n) != n; }\n` });
	await fixture.prepare(); const route = await fixture.prepareActorReplay(), target = path.join(fixture.workspace, "input"), command = "./worker 3<input; :";
	const view = new ResourceReadView(4096), producer = { source: "pattern_aware", mode: "edited-file", id: TimelineInterval.producerID(fixture, "input") };
	await using _cleanup = onExit(async () => { await view.dispose(); await fixture.dispose(); });
	const captured = await TimelineInterval.collect(() => captureStableFile(target, 4096, true, { retainObject: true }), producer);
	view.captureObject(target, captured.output); view.seal();
	await fixture.inOwner(async state => {
		state.borrowedInputs = state.method((await state.load("../src/agent-execution-world.ts")).ResourceObjectInputs.prototype, "borrow");
	});
	const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string }>;
	let binding: ProcessExecutionBinding | undefined;
	const actor = async (reused: boolean) => {
		await fixture.inOwner(state => state.borrowedInputs.mock.resetCalls());
		let dependencies: readonly TimelineDependency[] = [];
		const before = fixture.backend.actorMetrics().hits, actual = await TimelineInterval.measure(() => fixture.backend.observeBindings(scope,
			() => mode === "sync" ? fixture.backend.observeNative(fixture.workspace, scope, undefined, async () => childProcess.execFileSync(fixture.shellPath, ["-c", command],
				{ cwd: fixture.workspace, env: { ...fixture.environment }, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }))
				: executeProcess(fixture, route.executor, command, { scope }).then(result => result.output),
			(bindings, observed) => { dependencies = observed; binding = bindings.find(candidate => registry.resolveBinding(candidate, scope)?.executable === path.join(fixture.workspace, "worker")) ?? binding; },
			true, new ResourceObjectInputs(view)), () => dependencies);
		expect(actual.output).toBe("original\n"); expect(fixture.backend.actorMetrics().hits - before,
			JSON.stringify({ reused, actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics() })).toBe(Number(reused));
		expect(await fixture.inOwner(async (state, target) => (await Promise.all(state.borrowedInputs.mock.calls.filter((call: any) => call.arguments[0] === target).map((call: any) => call.result))).filter(Boolean).length, target)).toBeGreaterThan(0);
		expect(consumedTiming(actual.computation).hiddenComputeMs > 0).toBe(reused);
		expect(TimelineInterval.reusedProducers(dependencies).has(producer.id)).toBe(reused);
	};
	// Discovery enables full descriptor tracking; prepare the ownership observed with that tracking active.
	await actor(false); await actor(false); expect(binding).toBeDefined();
	await fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace => withPreparedWorker(fixture,
		{ sourceRoot: fixture.workspace, workspace, invocation: binding!, scope }, binding!, "original\n", () => actor(true)), { liveLower: true });
});

nativeTest.for(["async", "sync"])("keeps native reuse and learning across production and cache switches (%s)", async (mode, { benchmark }) => {
	const fixture = await benchmark("pi-native-switches-", "git"), sessionID = "switches";
	await fixture.write("first", "one\n"); await fixture.write("last", "two\n");
	await fixture.compile("worker", { code: `${cHeaders("fcntl.h")}\n${processChecks}
int main(int argc, char **argv) { if (argc != 2) return 1; int fd = open(argv[1], O_RDONLY); char bytes[16];
 int length = read(fd, bytes, sizeof(bytes)); if (length < 0 || close(fd)) return 2;
 burn_cpu(40000000ul); return write(1, bytes, (size_t)length) != length; }
` });
	await fixture.commit("native switches"); await fixture.prepare();
	const route = await fixture.prepareActorReplay();
	const settings = { enabled: true, reuseEnabled: true, drafterEnabled: false, tools: [], prepareObservedOperations: true, patternAware: patternAwareSettings({ enabled: false }) };
	const prepared = patternProcessHost(fixture, sessionID, settings), { events, start, call } = prepared;
	await using host = disposable(prepared.host);
	const refresh = () => host.runtime.settingsChanged({ ...settings, resourceCacheMaxEntries: 512, predictionTimeoutMs: 10_000,
		sourceConfig: { patternAware: settings.patternAware } });
	const execute = async (turn: string, id: string, input: string, hits: number) => {
		const command = `printf '${id}:'; ./worker ${input}; printf x >> marker`, before = fixture.backend.actorMetrics().hits;
		const result = await host.execute({ ...call(turn, command), id }, AbortSignal.timeout(10_000), async () => mode === "sync"
			? { content: [fauxText(childProcess.execFileSync(fixture.shellPath, ["-c", command], {
				cwd: fixture.workspace, env: { ...fixture.environment }, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
			}))], details: {} }
			: fixture.coordinator.runWith(route.executor, () => fixture.tool.execute(id, { command })));
		expect(textOutput(result)).toBe(`${id}:${input === "first" ? "one" : "two"}\n`);
		expect(fixture.backend.actorMetrics().hits - before).toBe(hits);
	};
	const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string; args: readonly string[] }>;
	const bindingFor = (input: string) => fixture.backend.executionBindings({ sessionID, turnID: input === "first" ? "seed" : "uncached" }).find(binding => {
		const invocation = registry.resolveBinding(binding, binding.scope);
		return invocation?.executable.endsWith("/worker") && invocation.args.includes(input);
	});
	const ready = (input: string, turn: string) => prepared.waitFor(() => {
		const binding = bindingFor(input);
		return !!binding && events.some(event => event.type === "candidate" && event.turnID === turn && event.candidate.kind === "operation" &&
			event.candidate.predictedAction.includes(binding.key) && event.state.status === "succeeded");
	});
	await start("seed"); await execute("seed", "seed", "first", 0); await ready("first", "seed");
	settings.enabled = false; await refresh(); await host.finishTurn("seed");
	await start("reuse"); await execute("reuse", "reuse", "first", 1); await host.finishTurn("reuse");
	settings.reuseEnabled = false; await refresh();
	await start("uncached"); await execute("uncached", "uncached", "last", 0); await host.finishTurn("uncached");
	expect(bindingFor("last")).toBeDefined();
	expect(events.some(event => event.type === "operation_prediction" && event.settlement.prediction.source === "execution_preparation" &&
		event.settlement.observation === "observed" && event.settlement.match.matched && event.settlement.match.adoption.status === "adopted")).toBe(true);
	settings.enabled = settings.reuseEnabled = true;
	await start("learned"); await ready("last", "learned");
	settings.enabled = false; await refresh();
	await execute("learned", "again", "last", 1); await host.finishTurn("learned", true);
	expect(await fixture.read("marker")).toBe("xxxx");
});

nativeTest.for(["sample", "file", "dup", "threads", "limit", "limit-stack", "limit-stack:failed", "limit-stack:unlimited", "limit-nofile:hard", "limit-cpu", "metadata", "identity", "identity:sync", "pipe", "socket", "eventfd", "pipe:unavailable", "socket:unavailable", "eventfd:unavailable", "eventfd:unavailable:destroyed", "eventfd::inherited", "eventfd:unavailable:external", "eventfd::external", "eventfd:unavailable:discard", "eventfd::discard", "eventfd:unavailable:discard-nested", "eventfd::discard-nested", "eventfd:unavailable:discard-cycle"])("adopts a closed-input calculation through capture or completion (%s)", { timeout: 30_000 }, async (scenario, { benchmark }) => {
		const [mode, transport, queue] = scenario.split(":"), unavailable = transport === "unavailable", synchronous = transport === "sync", fixture = await benchmark("pi-prefix-process-", "git", { processImages: unavailable ? false : undefined });
		const descendant = mode === "pipe" || mode === "socket" || mode === "eventfd", complete = descendant || mode === "identity" || mode === "limit-cpu", discarded = queue?.startsWith("discard"), cyclic = queue === "discard-cycle", capturable = !unavailable && mode !== "limit-cpu";
		const resource = `RLIMIT_${mode.slice(6).toUpperCase()}`, limitValue = transport === "unlimited" ? "RLIM_INFINITY" : mode === "limit-stack" ? 67108864 : 64;
		await fixture.compile("worker", { arguments: ["-pthread"], code: `#define _GNU_SOURCE
${cHeaders("sys/syscall.h sys/random.h sys/resource.h sys/stat.h sys/prctl.h pthread.h stdio.h stdint.h fcntl.h errno.h")}
${processChecks}
static void *thread(void *unused) { return unused; }
int main(void) {
 ${mode === "sample" ? 'char name[17]={0}; if(prctl(PR_GET_NAME,name) || strcmp(name,"worker")) return 80;' : ""}
 ${mode === "identity" ? "pid_t prior = getpid();" : ""}
 ${mode.startsWith("limit-") ? `struct rlimit limit, actual; if (getrlimit(${resource}, &limit)) return 73; limit.rlim_cur = ${limitValue}; ${transport === "hard" ? "limit.rlim_max = limit.rlim_cur;" : ""} if (${transport === "failed" ? `syscall(SYS_prlimit64, 0, ${resource}, &limit, (void *)1) != -1 || errno != EFAULT` : `setrlimit(${resource}, &limit)`}) return 73;` : ""}
 int fd = open("prefix-ready", O_WRONLY|O_CREAT|O_TRUNC, 0600); if (fd < 0 || write(fd, "ready", 5) != 5) return 71;
 ${mode === "dup" ? "int copy = dup(fd); if (copy < 0 || close(fd)) return 77; fd = copy;" : mode === "file" ? "" : "if (close(fd)) return 71;"}
 ${descendant ? "pid_t child = fork(); if (child < 0) return 78; if (!child) { if (close(3)) _exit(79);" : ""}
 volatile uint64_t hash = 1; for (unsigned i = 0; i < ${synchronous ? 1500000000 : 800000000}; i++) hash = hash * 33 + i;
 ${descendant ? `if (write(4, &hash, sizeof(hash)) != sizeof(hash) || close(4)) _exit(79); _exit(0); }
 uint64_t hash; if (close(4) || read(3, &hash, sizeof(hash)) != sizeof(hash) || close(3) || wait_child(child)) return 79;` : ""}
 ${mode === "file" || mode === "dup" ? "if (close(fd)) return 77;" : ""}
 ${mode === "metadata" ? "struct stat state; if (fstat(STDOUT_FILENO, &state) || !state.st_ino) return 76;" : ""}
 ${mode === "threads" ? "pthread_t worker; void *result; if (pthread_create(&worker, NULL, thread, (void *)37) || pthread_join(worker, &result) || result != (void *)37) return 72;" : "(void)thread;"}
 ${mode === "limit" ? "struct rlimit limit = {0, 0}; if (setrlimit(RLIMIT_CORE, &limit)) return 73;" : ""}
 uint64_t value; if (getrandom(&value, sizeof(value), 0) != sizeof(value)) return 74;
 ${mode.startsWith("limit-") ? `if (getrlimit(${resource}, &actual) || memcmp(&actual, &limit, sizeof(limit))) return 75;` : ""}
 printf("%ld:%ld:%llu:%llu\\n", (long)getpid(), (long)syscall(SYS_gettid), (unsigned long long)hash, (unsigned long long)value);
 ${mode === "identity" ? "if (prior != getpid()) return 75;" : ""} return 0;
}
` });
		if (descendant) await fixture.compile("launch", { code: `#define _GNU_SOURCE
${cHeaders("sys/eventfd.h sys/mman.h")}
${rightsHelpers}
int main(void) { int fds[2]; if (${mode === "eventfd" ? "(fds[0] = eventfd(0, 0)) < 0 || (fds[1] = dup(fds[0])) < 0" : mode === "socket" ? "socketpair(AF_UNIX, SOCK_STREAM, 0, fds)" : "pipe(fds)"} || fds[0] != 3 || fds[1] != 4) return 61;
 ${queue && queue !== "external" ? `int queues[2]; if (socketpair(AF_UNIX, SOCK_STREAM, 0, queues)) return 63;
 int reference = ${discarded ? queue !== "discard" ? `wrap_queue(memfd_create("queued-file", MFD_CLOEXEC), ${Number(cyclic)})` : 'memfd_create("queued-file", MFD_CLOEXEC)' : "-1"};
 if (${discarded ? "reference < 0 || " : ""}pass(queues[0], "x", 1, &reference, ${discarded ? 1 : 0}, 0)${discarded ? " || close(reference)" : ""}) return 63;` : ""}
 ${queue === "external" ? `int gate[2]; if (wrap_queue(wrap_queue(eventfd(5, EFD_CLOEXEC), 0), 0) < 0 || pipe2(gate, O_CLOEXEC)) return 64;
 pid_t child = fork(); if (child < 0) return 65;
 if (child) { close(3); close(4); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 66; close(gate[1]); return wait_child(child); }
 if (wait_gate(gate)) return 66;` : ""}
 close_range(${queue === "inherited" || discarded ? 7 : 5}, ~0U, 0); execlp("worker", "worker", NULL); return 62; }
` });
		await fixture.commit("Closed input prefix");
		const scope = { sessionID: "prefix", turnID: "same" };
		// The synchronous Actor keeps its shell to write the marker; its seed must inherit the same SHLVL.
		const command = `${synchronous ? "" : "exec "}./${descendant ? "launch" : "worker"}${synchronous ? "; :" : ""}`;
		const { actor, seed, binding, invocation } = await learnWorker(fixture, scope, command);
		await rm(path.join(fixture.workspace, "prefix-ready"));
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
			const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope });
			await fixture.inOwner(async (state, mode) => {
				const { ProcessHandoffRegistry } = await state.load("../src/process-handoff.ts"), { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { gated } = await state.load("./async.ts");
				const registry = ProcessHandoffRegistry.prototype, acquire = registry.acquire, observe = registry.observeRunning, request = LinuxProcessReuseBackend.prototype.requestProcessImageAtFrontier;
				state.starting = gated(); state.frontier = gated(); state.cancel = state.deferred(); state.cancelled = state.deferred(); state.admissions = []; state.joins = 0;
				state.spawn = state.method((await state.load("node:child_process")).default, "spawn"); state.publishing = state.method(registry, "publish");
				state.method(registry, "acquire", async function (this: unknown, options: any) {
					if (mode === "sample" && !options.producer) state.starting.release();
					const result = await acquire.call(this, options); if (mode === "sample" && result.kind === "work") await state.starting.wait(); return result;
				});
				state.method(registry, "observeRunning", function (this: unknown, key: unknown, work: unknown, invoke: () => unknown) { return observe.call(this, key, work, async () => {
					if (mode === "threads" && ++state.joins === 2) { state.cancel.resolve(); await state.cancelled.promise; state.frontier.release(); } return invoke();
				}); });
				state.method(LinuxProcessReuseBackend.prototype, "requestProcessImageAtFrontier", async function (this: unknown, ...args: any[]) {
					const admit = args[4]; args[4] = async (pid: number, reply: Buffer, report: Buffer) => {
						const accepted = await admit(pid, reply, report); state.admissions.push({ syscall: reply.readInt32LE(8), accepted }); return accepted;
					};
					if (mode === "threads") await state.frontier.wait(); return request.apply(this, args);
				});
			}, mode);
			const controller = new AbortController(); let cancelled: Promise<unknown> | undefined;
			const cancel = mode === "threads" ? fixture.inOwner(state => state.cancel.promise).then(async () => { controller.abort(); await fixture.inOwner(state => state.cancelled.resolve()); }) : undefined;
			const pending = session.executeBinding(binding), settled = pending.catch(() => undefined);
			await using _cleanup = onExit(async () => {
				controller.abort(); await fixture.inOwner(state => { state.cancel.resolve(); state.starting.release(); state.frontier.release(); }); await session.close(); await settled; await cancelled; await cancel;
			});
			if (mode === "sample") await fixture.inOwner(state => state.starting.entered);
			else if (descendant || synchronous) await runningProcess(fixture, async (pid, syscall) => {
				if (!synchronous) return syscall.startsWith("0 0x3 ");
				const stat = await readText(`/proc/${pid}/stat`), fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
				return /^(?:running|-1 )/.test(syscall) && Number(fields[11]) + Number(fields[12]) >= 40;
			});
			else await expect.poll(() => existsSync(path.join(workspace.sandboxRoot, "prefix-ready"))).toBe(true);
			expect(existsSync(path.join(fixture.workspace, "prefix-ready"))).toBe(false);
			if (mode === "threads") { cancelled = actor(controller.signal).catch(() => undefined); await fixture.inOwner(state => state.frontier.entered); }
			const issuedAt = performance.now(), actual = await actor(undefined, synchronous), fields = actual.output.replace(/^own:/, "").trim().split(":");
			const timing = new TaskTimeline(issuedAt).recordCall([{ computation: actual.computation }], issuedAt, performance.now());
			expect(fields).toHaveLength(4); expect(fields[0]).toBe(fields[1]); expect(fields[2]).toBe(seed.trim().split(":")[2]);
			expect(fields[3]).not.toBe(seed.trim().split(":")[3]);
			expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify(fixture.backend.metrics())).toBe(Number(!cyclic));
			const { admissions, publications } = await fixture.inOwner(async state => {
				const { TimelineInterval } = await state.load("../src/task-timing.ts");
				return { admissions: state.admissions as { syscall: number; accepted: boolean | undefined }[], publications: state.publishing.mock.calls.map(({ arguments: [, , certificate, , continuation] }: any) =>
					({ certificate, computation: continuation && TimelineInterval.serialize(continuation.computation) })) as { certificate: import("../src/provenance-certificate.ts").ProcessProvenanceCertificate; computation?: unknown }[] };
			});
			if (mode === "file" || mode === "dup") expect(admissions).toEqual([{ syscall: 3, accepted: undefined }, { syscall: 318, accepted: true }]);
			const produced = await pending; expect(produced).toMatchObject(complete ? { exit: { kind: "code", code: 0 } } : { suspended: true });
			if (synchronous) { expect(actual.output).toBe("own:" + Buffer.concat(produced.output.map(event => event.data)).toString()); expect(await fixture.read("marker")).toBe("once"); }
			const continuations = publications.flatMap(({ computation }) => computation ? [TimelineInterval.restore(computation)!] : []);
			expect(continuations).toEqual(complete ? [] : [expect.anything()]); expect(timing.reused).toBe(cyclic ? undefined : true);
			expect(timing.toolComputeMs).toBeGreaterThanOrEqual(timing.hiddenComputeMs);
			if (complete) {
				expect(admissions.length > 0).toBe(capturable); expect(admissions.every(result => !result.accepted)).toBe(true);
				if (descendant && capturable) expect(admissions).toContainEqual({ syscall: 0, accepted: false });
				if (cyclic) expect(publications).toEqual([]);
				else if (discarded) {
					const certificate = publications.find(({ certificate }) => certificate?.dependencyCertificate.complete)!.certificate;
					const file = certificate.prototype.inheritedFDs.find(fd => fd.type === "regular")!;
					expect(certificate.result.resources!.transitions!.at(-1)).toMatchObject({ id: file.alias, kind: "release" });
				}
				if (cyclic) expect(timing.hiddenComputeMs).toBe(0); else expect(timing.hiddenComputeMs).toBeGreaterThan(0);
			} else if (mode === "sample") expect(new TaskTimeline(issuedAt).recordTool(continuations[0]!, issuedAt, true).hiddenComputeMs).toBe(0);
		}, { liveLower: true });
	});

	nativeTest.for(["pipe", "socket", "peer", "eventfd", "readv", "recvmsg", "writev", "mmap", "runtime", "handles", "eof", "early", "changed", "identity", "file", "cancel", "pipe:unavailable", "peer:unavailable", "eventfd:unavailable", "queue-drop", "queue-deferred", "queue-received", "queue-closed", "queue-peeked", "queue-changed", "queue-cancel", "queue-deferred:unavailable", "queue-alias", "queue-counter", "queue-stream", "queue-nested", "queue-peek-kept", "queue-stale", "queue-received-cancel", "queue-received:unavailable",
		"pipe:sync", "changed:sync", "cancel:sync", "peer:sync", "queue-received:sync", "queue-stale:sync"])("resumes a learned process with future input (%s)", { timeout: 60_000 }, async (scenario, { skip, benchmark }) => {
		const [mode, transport] = scenario.split(":"), unavailable = transport === "unavailable", synchronous = transport === "sync";
		const fixture = await benchmark("pi-live-process-", undefined, { processImages: unavailable ? false : undefined });
		const queued = mode!.startsWith("queue-"), stale = mode === "queue-stale", changed = stale || mode!.endsWith("changed"), cancelled = mode!.endsWith("cancel");
		const sealable = !unavailable && !["identity", "peer", "file", "queue-peeked"].includes(mode!), resumed = sealable && !changed && !cancelled;
		const received = queued && !["queue-drop", "queue-deferred", "queue-changed", "queue-cancel"].includes(mode!), alias = mode === "queue-alias";
		await fixture.inOwner(async state => {
			const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { ProcessHandoffRegistry } = await state.load("../src/process-handoff.ts");
			state.spawn = state.method((await state.load("node:child_process")).default, "spawn");
			state.captured = new Map();
			state.inspections = state.method(LinuxProcessReuseBackend.prototype, "decideHeldExec");
			state.publications = state.method(ProcessHandoffRegistry.prototype, "publish");
			const capture = LinuxProcessReuseBackend.prototype.requestProcessImageAtFrontier;
			LinuxProcessReuseBackend.prototype.requestProcessImageAtFrontier = async function (...args: unknown[]) {
				await state.captureGate?.promise; const result = await capture.apply(this, args); if (result) state.captured.set(args[0], result.pid); return result;
			};
		});
		const observations = new Map<string, { bindings: readonly ProcessExecutionBinding[]; computations: readonly TimelineDependency[] }>();
		const observeBindings = fixture.backend.observeBindings.bind(fixture.backend);
		using _observing = vi.spyOn(fixture.backend, "observeBindings").mockImplementation((scope, execute, observe, learn, inputs) =>
			observeBindings(scope, execute, (bindings, computations) => { observations.set(scope.turnID, { bindings, computations }); observe(bindings, computations); }, learn, inputs));
		await fixture.prepare();
		if (unavailable) expect((await Reflect.get(fixture.backend, "ready")).imageLibrary).toBeUndefined();
		if (!unavailable && !(await Reflect.get(fixture.backend, "ready")).imageLibrary) return skip("native process image capture is unavailable");
		await fixture.compile("worker", { arguments: ["-pthread"], code: `#define _GNU_SOURCE
${cHeaders("stdio.h stdint.h sys/syscall.h sys/uio.h sys/mman.h pthread.h signal.h stdlib.h errno.h")}
${rightsHelpers}
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
int main(${alias ? "int argc, char **argv" : "void"}) {
 ${alias ? `if (argc > 1) { char byte; return strcmp(argv[1], "verify") || fcntl(12, F_GETFD) != -1 || errno != EBADF || fcntl(13, F_GETFD) || lseek(5, 0, SEEK_CUR) != 2 || lseek(13, 0, SEEK_CUR) != 2 || read_text(5, &byte, "c") || lseek(13, 0, SEEK_CUR) != 3 ? 86 : 37; }` : ""}
 ${mode === "identity" || mode === "peer" ? "pid_t identity = getpid();" : mode === "file" ? "int temporary = open(\"/dev/null\", O_RDONLY); if (temporary < 0) return 86;" : ""}
 ${mode === "runtime" ? `char *heap = malloc(65536); if (!heap) return 78; memset(heap, 'H', 65536); local = 23;
 stack_t stack = {.ss_sp = alternate, .ss_size = sizeof(alternate)};
 struct sigaction action = {.sa_handler = receive_signal, .sa_flags = SA_ONSTACK};
 if (sigaltstack(&stack, NULL) || sigaction(SIGUSR1, &action, NULL)) return 79;` : ""}
 ${mode === "mmap" ? `volatile char *mapping = mmap(NULL, 12288, PROT_READ | PROT_WRITE, MAP_PRIVATE, 5, 0);
 if (mapping == MAP_FAILED || mapping[0] != 'A') return 75; mapping[4096] = 'A'; mapping[8192] = 'Z';` : ""}
 char input[4] = {0}, byte; uint64_t counter = 0; int received = -1;
 ${mode === "queue-nested" ? "if (take(5, 'x', &received, 1, 0, 0) || close(5) || dup2(received, 5) != 5 || close(received)) return 86;" : ""}
 ${mode === "queue-drop" ? "if (close(5)) return 86;" : received ? `if (take(5, 'x', &received, 1, ${Number(mode!.startsWith("queue-peek"))}, 0)) return 86;
 ${mode === "queue-counter" ? "if (read(received, &counter, 8) != 8 || counter != 3) return 86; counter = 7; if (write(received, &counter, 8) != 8) return 86;" : `if (read(received, &byte, 1) != 1 || (byte != 'a'${stale ? " && byte != 'x'" : ""})) return 86;`}
 ${["queue-closed", "queue-peeked"].includes(mode!) ? `if (close(received)${mode === "queue-closed" ? " || close(5)" : ""}) return 86; received = -1;` : mode === "queue-peek-kept" ? "" : "if (close(5)) return 86;"}
 ${alias ? "if (dup3(received, 5, 0) != 5 || fcntl(received, F_DUPFD_CLOEXEC, 12) != 12 || dup3(received, 13, 0) != 13 || close(received) || fcntl(5, F_SETFL, O_APPEND)) return 86; received = 5;" : ""}` : ""}
 if (${mode === "eventfd" ? "read(3, &counter, 8) != 8 || counter != 1" : "read(3, input, 1) != 1"}) return 71;
 ${stale ? "if (byte == 'x') input[0] = 'D';" : ""}
 ${mode === "handles" ? `char bytes[2]; if (read_text(5, bytes, "ab") || lseek(6, 0, SEEK_CUR) != 2 || lseek(7, 0, SEEK_CUR)) return 83;
 if (fcntl(5, F_DUPFD_CLOEXEC, 12) != 12 || close(5) || close(6) || dup2(7, 6) != 6 || close(7) ||
     dup2(3, 8) != 8 || close(3) || dup2(4, 3) != 3 || close(4) || fcntl(8, F_SETFD, FD_CLOEXEC) ||
     fcntl(12, F_SETFL, O_APPEND)) return 84;` : ""}
 volatile uint64_t hash = 1; for (unsigned i = 0; i < ${mode === "early" || mode === "identity" ? 1500000000 : 250000000}; i++) hash = hash * 33 + i;
 ${mode === "eof" ? `char fill[131072]; memset(fill, 'P', sizeof(fill));
 if (close(4) || write(1, fill, sizeof(fill)) != sizeof(fill)) return 72;` : mode === "writev" ? `char fill[4096]; for (unsigned i = 0; i < sizeof(fill); i++) fill[i] = 'P';
 if (write(4, fill, sizeof(fill)) != sizeof(fill)) return 72;
 struct iovec ready[] = {{"R", 1}}; if (writev(4, ready, 1) != 1) return 72;` : `if (write(ACK_FD, "R", 1) != 1) return 72;`}
 ${mode === "eventfd" ? `if (read(3, &counter, 8) != 8 || counter != 2 || write(4, "S", 1) != 1 || read(3, &counter, 8) != 8 || counter != 3) return 73;
 input[0] = 'A';` : `
 unsigned used = 1; ssize_t n; while ((n = next_input(input + used, sizeof(input) - used)) > 0) used += n;
 if (n || used != 3 || (input[0] != 'A' && input[0] != 'D') || input[1] != 'B' || input[2] != 'C') return 73;`}
 ${mode === "queue-peek-kept" ? "int queued_alias; if (take(5, 'x', &queued_alias, 1, 0, 0) || close(5) || lseek(queued_alias, 0, SEEK_CUR) != 1 || close(queued_alias)) return 86;" : ""}
 ${queued && !["queue-drop", "queue-closed"].includes(mode!) ? `if ((received < 0 && (take(5, 'x', &received, 1, 0, 0) || close(5))) || ${received && mode !== "queue-counter" ? 'read_text(received, &byte, "b")' : `read(received, &counter, 8) != 8 || counter != ${received ? 7 : 3}`} ${alias ? "|| fcntl(5, F_GETFD) || fcntl(12, F_GETFD) != FD_CLOEXEC || !(fcntl(5, F_GETFL) & O_APPEND) || !(fcntl(12, F_GETFL) & O_APPEND) || lseek(12, 0, SEEK_CUR) != 2" : "|| close(received)"}) return 86;` : "(void)byte; (void)received;"}
 ${alias ? "for (int fd = 6; fd < 64; fd++) if (fd != 12 && fd != 13 && (fcntl(fd, F_GETFD) != -1 || errno != EBADF)) return 86;" : ""}
 if (getpid() != syscall(SYS_gettid) ${mode === "identity" || mode === "peer" ? "|| identity != getpid()" : mode === "file" ? "|| close(temporary)" : ""}) return 74;
 ${mode === "handles" ? `if (fcntl(4, F_GETFD) != -1 || errno != EBADF || fcntl(8, F_GETFD) != FD_CLOEXEC ||
     fcntl(12, F_GETFD) != FD_CLOEXEC || !(fcntl(12, F_GETFL) & O_APPEND) || read_text(12, bytes, "c") ||
     lseek(6, 0, SEEK_CUR) || read_text(6, bytes, "a")) return 85;` : ""}
 ${mode === "runtime" ? `heap = realloc(heap, 262144); if (!heap || local != 23 || pthread_kill(pthread_self(), SIGUSR1) || !delivered || future_stack(32) != 8448) return 80;
 for (unsigned i = 0; i < 65536; i++) if (heap[i] != 'H') return 81; free(heap);
 pthread_t thread; void *returned; if (pthread_create(&thread, NULL, future_thread, NULL) || pthread_join(thread, &returned) || returned != (void *)1) return 82;` : ""}
 ${mode === "mmap" ? `char value; if (mapping[0] != 'B' || mapping[4096] != 'A' || mapping[8192] != 'Z') return 76;
 for (unsigned i = 0; i < 3; i++) if (pread(5, &value, 1, i * 4096) != 1 || value != 'B') return 77;` : ""}
 printf("%cBC:%llu\\n", input[0], (unsigned long long)hash); ${alias ? 'fflush(stdout); if (close(3) || close(4)) return 86; execlp("worker", "worker", "verify", NULL); return 86;' : "return 37;"}
}
` });
		await fixture.compile("launch", { code: `#define _GNU_SOURCE
${cHeaders("stdio.h stdlib.h stdint.h sys/eventfd.h sys/mman.h")}
${rightsHelpers}
int main(int argc, char **argv) {
 if (argc != 2) return 60;
 int in[2], ack[2]; if (${mode === "eventfd" ? "(in[0] = eventfd(0, 0)) < 0 || (in[1] = dup(in[0])) < 0" : mode === "socket" || mode === "peer" || mode === "recvmsg" ? "socketpair(AF_UNIX, SOCK_STREAM, 0, in)" : "pipe(in)"} || ${mode === "peer" ? "(ack[0] = dup(in[1])) < 0 || (ack[1] = dup(in[0])) < 0" : "pipe(ack)"}) return 61;
 ${mode === "eof" ? "int output[2]; if (pipe(output)) return 61;" : ""}
 ${mode === "writev" ? "if (fcntl(ack[1], F_SETPIPE_SZ, 4096) != 4096) return 61;" : ""}
 ${mode === "mmap" ? `int backing = memfd_create("live-backing", 0); if (backing < 0 || ftruncate(backing, 12288)) return 61;
 for (unsigned i = 0; i < 3; i++) if (pwrite(backing, "A", 1, i * 4096) != 1) return 61;` : ""}
 ${mode === "handles" ? `int backing = memfd_create("live-ofd", 0); if (backing < 0 || write(backing, "abcd", 4) != 4 || lseek(backing, 0, SEEK_SET)) return 61;
 char pathname[64]; snprintf(pathname, sizeof(pathname), "/proc/self/fd/%d", backing); int independent = open(pathname, O_RDONLY); if (independent < 0) return 61;` : ""}
 ${queued ? mode === "queue-stream" ? 'int stream[2]; if (pipe(stream) || write(stream[1], "ab", 2) != 2 || close(stream[1])) return 61; int reference = stream[0];' : `int reference = ${received && mode !== "queue-counter" || mode === "queue-drop" ? 'memfd_create("queued-file", 0)' : "eventfd(3, 0)"}; if (reference < 0${received && mode !== "queue-counter" ? ` || write(reference, ${stale ? 'argv[1][0] == \'c\' ? "xb" : "ab"' : alias ? '"abc"' : '"ab"'}, ${alias ? 3 : 2}) != ${alias ? 3 : 2} || lseek(reference, 0, SEEK_SET)` : ""}) return 61;` : ""}
 ${queued ? `int queued = wrap_queue(${mode === "queue-nested" ? "wrap_queue(reference, 0)" : "reference"}, 0); if (queued < 0) return 61;` : ""}
 // The prefix and parent endpoints must be stable before exec, independent of fork scheduling.
 uint64_t counter = 1;
 if (${mode === "eventfd" ? "write(in[1], &counter, 8) != 8" : `write(in[1], ${stale ? '"A"' : 'argv[1][0] == \'c\' ? "D" : "A"'}, 1) != 1`}) return 65;
 int start[2]; if (pipe(start)) return 61;
 pid_t child = fork(); if (child < 0) return 62;
 if (!child) { close(start[1]); char go; if (read(start[0], &go, 1) != 1 || close(start[0])) _exit(63);
  int a = fcntl(in[0], F_DUPFD_CLOEXEC, 10), b = fcntl(ack[1], F_DUPFD_CLOEXEC, 10);
  if (a < 0 || b < 0 || dup2(a, 3) != 3 || dup2(b, 4) != 4) _exit(63);
  ${mode === "eof" ? "if (dup2(output[1], 1) != 1) _exit(63);" : ""}
  ${mode === "handles" ? "if (dup2(backing, 5) != 5 || dup2(5, 6) != 6 || dup2(independent, 7) != 7) _exit(63); close_range(8, ~0U, 0);" : mode === "mmap" || queued ? `if (dup2(${queued ? "queued" : "backing"}, 5) != 5) _exit(63); close_range(6, ~0U, 0);` : "close_range(5, ~0U, 0);"} execlp("worker", "worker", NULL); _exit(64); }
 close(in[0]); close(ack[1]); ${queued ? "close(queued);" : ""} char ready;
 ${mode === "eof" ? "close(output[1]);" : ""}
 close(start[0]); if (write(start[1], "S", 1) != 1 || close(start[1])) return 65;
 ${mode === "writev" ? `char fill[4096]; unsigned used = 0; ssize_t size;
 while (used < sizeof(fill) && (size = read(ack[0], fill + used, sizeof(fill) - used)) > 0) used += size;
 if (used != sizeof(fill)) return 65; for (unsigned i = 0; i < used; i++) if (fill[i] != 'P') return 65;` : ""}
 ${mode === "eof" ? `if (read(ack[0], &ready, 1) != 0) return 65;
 char chunk[4096]; unsigned total = 0; while (total < 131072) { ssize_t size = read(output[0], chunk, sizeof(chunk));
 if (size <= 0) return 65; for (ssize_t i = 0; i < size; i++) if (chunk[i] != 'P') return 65; total += (unsigned)size; }
 if (total != 131072) return 65;` : 'if (read_text(ack[0], &ready, "R")) return 65;'}
 ${mode === "mmap" ? `for (unsigned i = 0; i < 3; i++) if (pwrite(backing, "B", 1, i * 4096) != 1) return 65;` : ""}
 ${mode === "eventfd" ? `counter = 2; if (write(in[1], &counter, 8) != 8 || read_text(ack[0], &ready, "S")) return 66;
 counter = 3; if (write(in[1], &counter, 8) != 8) return 67;` : `if (write(in[1], "B", 1) != 1) return 66;
 usleep(1000); if (write(in[1], "C", 1) != 1) return 67;`}
 close(in[1]); close(ack[0]);
 ${mode === "eof" ? "ssize_t size; while ((size = read(output[0], chunk, sizeof(chunk))) > 0) if (write(1, chunk, (size_t)size) != size) return 68; if (size) return 68; close(output[0]);" : ""}
 int status; return waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 37
 ${mode === "handles" ? "|| lseek(backing, 0, SEEK_CUR) != 3 || lseek(independent, 0, SEEK_CUR) != 1 || !(fcntl(backing, F_GETFL) & O_APPEND)" : ""};
}
` });
		await fixture.commit("Live process continuation");
		const route = await fixture.prepareActorReplay(), scope = { sessionID: "live", turnID: "seed" };
		const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string }>;
		const publishing = vi.spyOn(registry, "publish"), errors = vi.spyOn(fixture.backend as any, "setError");
		const prepared = patternProcessHost(fixture, scope.sessionID, { candidateLimit: 4, maxConcurrentActions: 4,
			patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 }) }), { events, start, call } = prepared;
		await using host = disposable(prepared.host);
		const runSync = async (command: string) => ({ details: {}, content: [fauxText(childProcess.execFileSync(fixture.shellPath, ["-c", command],
			{ cwd: fixture.workspace, env: fixture.environment, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }))] });
		const execute = (turnID: string, command: string, signal = AbortSignal.timeout(10_000)) => host!.execute(call(turnID, command), signal,
				() => synchronous && turnID === "prepared" ? runSync(command) : fixture.coordinator.runWith(route.executor, () => fixture.tool.execute(turnID, { command }, signal)));
		for (const turnID of ["common-1", "common-2"]) { await start(turnID); await execute(turnID, "printf common"); await host.finishTurn(turnID); }
		await start("seed"); const seed = await execute("seed", "printf seed; launch seed"); await host.finishTurn("seed");
		expect(seed.content).toMatchObject([fauxText(expect.stringMatching(/^seedABC:\d+\n$/))]);
		await fixture.inOwner(state => state.spawn.mock.resetCalls());
		await start("prepared");
		const diagnostic = () => JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(), errors: errors.mock.calls.map(call => call[1]),
			certificates: publishing.mock.calls.map(([, , certificate]) => certificate && { complete: certificate.dependencyCertificate, continuation: certificate.result.continuation }),
			operations: events.filter(event => event.type === "operation_prediction") });
		const privatePid = await runningProcess(fixture, async (pid, syscall) => {
			if (mode === "early" || mode === "identity") {
				const usage = await readText(`/proc/${pid}/stat`).catch(() => ""), fields = usage.slice(usage.lastIndexOf(")") + 2).split(" ");
				return /^(?:running|-1 )/.test(syscall) && Number(fields[11]) + Number(fields[12]) >= 40;
			}
			return syscall.startsWith(mode === "handles" ? "0 0x8 " : mode === "writev" ? "20 0x4 " : mode === "readv" ? "19 0x3 " : mode === "recvmsg" ? "47 0x3 " : "0 0x3 ");
		}).catch(error => { throw new Error(diagnostic(), { cause: error }); });
		const ownerBinding = (turnID = "prepared") => fixture.inOwner((state, scope, turnID) => { const registry = state.publications.mock.calls.at(-1)?.this; return registry?.bindings(scope).find((binding: any) => binding.scope.turnID === turnID && registry.results(binding.key, scope).some((result: any) => result.result.continuation))?.key; }, scope, turnID);
		if (sealable && mode !== "early") await expect.poll(() => ownerBinding(), { timeout: 5_000 }).toBeDefined().catch(error => { throw new Error(diagnostic(), { cause: error }); });
		const preparedKey = await ownerBinding();
		if (cancelled) { await host.finishTurn("prepared", true); await expect.poll(() => existsSync(`/proc/${privatePid}`)).toBe(false); }
		const command = `printf actual; launch ${changed ? "changed" : "actual"}`;
		const inspectionStart = await fixture.inOwner(state => state.inspections.mock.calls.length);
		const actual = cancelled && synchronous ? await fixture.backend.observeNative(fixture.workspace, { ...scope, turnID: "later" }, undefined, () => runSync(command)) : cancelled ? await fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID: "later" } }) },
			() => fixture.tool.execute("later", { command })) : await execute("prepared", command);
		expect(actual.content, JSON.stringify(actual.content)).toEqual(seed.content.map(item => item.type === "text" ? { ...item,
			text: item.text.replace(/^seedA/, `actual${changed ? "D" : "A"}`) } : item));
		if (!alias) expect(fixture.backend.actorMetrics().hits, diagnostic()).toBe(Number(resumed));
		if (!alias) expect(fixture.backend.actorMetrics().joinedHits, diagnostic()).toBe(Number(resumed && mode === "early"));
		const ownerEvidence = await fixture.inOwner(state => ({ inspections: state.inspections.mock.calls.map(({ arguments: [held] }: any) => held.descriptors),
			publications: state.publications.mock.calls.map(({ arguments: [, , certificate, , continuation, computation] }: any) => ({ certificate, continuation: continuation?.computation, computation })) }));
		if (alias) expect(ownerEvidence.inspections.slice(inspectionStart).some((descriptors: any[]) => [5, 13].every(slot => descriptors?.some(fd => fd.fd === slot && fd.alias === 5 && fd.offset === 2 && fd.owned)) && !descriptors?.some(fd => fd.fd === 12))).toBe(true);
		if (queued && resumed) expect(ownerEvidence.publications.some(({ certificate }: any) => certificate?.prototype.inheritedFDs.some((fd: any) => fd.installed === false))).toBe(true);
		if (resumed) expect(ownerEvidence.publications.some(({ certificate, continuation, computation }: any) => certificate?.result.continuation &&
			continuation && computation && computation.completedAt > continuation.completedAt), diagnostic()).toBe(true);
		if (!cancelled) { if (!sealable) expect(existsSync(`/proc/${privatePid}`)).toBe(true); await host.finishTurn("prepared", true); }
		await expect.poll(() => existsSync(`/proc/${privatePid}`), { timeout: 5_000 }).toBe(false); // Retirement kills asynchronously, as for cancel.
		if (!cancelled) expect(consumedTiming(events, "prepared").hiddenComputeMs > 0, diagnostic()).toBe(resumed);
		if (resumed) expect(events.filter(event => event.type === "operation_prediction"), diagnostic()).toContainEqual(expect.objectContaining({ settlement: expect.objectContaining({
			observation: "observed", match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }) }) }));
		const binding = observations.get("prepared")?.bindings.find(candidate => registry.resolveBinding(candidate, scope)?.executable === path.join(fixture.workspace, "worker"));
		if (resumed) expect(binding, diagnostic()).toMatchObject({ available: true });
		if (resumed && preparedKey) {
			expect(binding?.key, diagnostic()).toBe(preparedKey);
			expect(binding).toBe(fixture.backend.executionBindings(scope).find(candidate => candidate.key === preparedKey && candidate.scope.turnID === "prepared"));
		}
		if (resumed && ["pipe", "queue-received"].includes(mode!)) {
			const invocation = fixture.invocation({ command: "exit 92" }).process!;
			for (const phase of ["foreign", "fresh", "changed", "cancel", "closed", "revoked"]) await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				if (phase === "revoked") { await fixture.backend.storage.maintain("clear"); expect(binding!.available).toBe(false); }
				const later = { sessionID: phase === "foreign" ? "foreign" : scope.sessionID, turnID: phase }, fresh = phase === "fresh" || phase === "closed";
				await fixture.inOwner((state, cancel) => { state.captureGate = cancel ? state.deferred() : undefined; state.spawn.mock.resetCalls(); }, phase === "cancel");
				const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
				const preparing = session.executeBinding(binding!);
				void preparing.catch(() => undefined);
				await using _cleanup = onExit(async () => { await fixture.inOwner(state => state.captureGate?.resolve()); await session.close(); await preparing.catch(() => undefined); });
				if (phase === "foreign" || phase === "revoked") { await expect(preparing).rejects.toThrow("unavailable in this scope"); return; }
				const pid = await runningProcess(fixture, (_pid, syscall) => syscall.startsWith("0 0x3 "));
				if (phase !== "cancel") await expect.poll(() => ownerBinding(phase), { timeout: 5_000 }).toBeDefined().catch(error => { throw new Error(phase + diagnostic(), { cause: error }); });
				if (phase === "closed") await preparing;
				if (phase === "cancel" || phase === "closed") { await fixture.inOwner(state => state.captureGate?.resolve()); await session.close(); }
				const before = fixture.backend.actorMetrics().hits;
				const repeated = await TimelineInterval.measure(() => fixture.backend.observeBindings(later,
					() => executeProcess(fixture, route.executor, `launch ${phase === "changed" ? "changed" : "actual"}`, { scope: later, signal: AbortSignal.timeout(10_000) }), () => {}, false),
					() => observations.get(phase)!.computations);
				const timing = consumedTiming(repeated.computation);
				expect(repeated.output).toEqual({ output: `${phase === "changed" ? "D" : "A"}BC:7107042143181080257\n`, exitCode: 0 });
				expect([fixture.backend.actorMetrics().hits - before, timing.hiddenComputeMs > 0], phase + diagnostic()).toEqual([Number(fresh), fresh]);
				if (fresh) {
					expect(await preparing).toEqual({ output: [], suspended: true });
					expect(observations.get(phase)!.bindings).toEqual([session.executionBindings()[0]]);
				} else expect(observations.get(phase)!.bindings).toEqual([]);
				expect(existsSync(`/proc/${pid}`)).toBe(false);
			}, { liveLower: true });
		}
	});

nativeTest.for(["keyed", "unkeyed", "keyed-large", "unkeyed-opaque", "keyed-failed", "unkeyed-failed", "keyed-exit", "unkeyed-exit",
	"keyed-sync", "unkeyed-sync", "keyed-sync-failed", "unkeyed-sync-exit", "keyed-mixed"])("transfers native tool FD inputs to filesystem tools across turns with exact invalidation (%s)", { timeout: 30_000 }, async (mode, { skip, benchmark }) => {
		const name = "render_rows";
		const fixture = await benchmark("pi-native-resource-inputs-"), observing = vi.spyOn(fixture.backend, "observeBindings");
		const unkeyed = mode.startsWith("unkeyed"), parentOutput = { transcript: mode.endsWith("large") ? "x".repeat(128 * 1024) : "", ...(mode.endsWith("opaque") ? { callback: () => "parent-only" } : {}) };
		let host: ReturnType<typeof patternProcessHost>["host"] | undefined;
		const search = await createClosedSearchProfile(fixture.workspace);
		await using _cleanup = onExit(async () => { observing.mockRestore(); await host?.dispose(); await search.pool.dispose(); await fixture.dispose(); });
		if (!search.invocations.has("grep") || !search.invocations.has("find")) return skip("qualified search executors are unavailable");
		const file = path.join(fixture.workspace, "input.txt"); await writeFile(file, "one\ntwo\n");
		await mkdir(path.join(fixture.workspace, "entries"));
		for (const name of ["z.txt", "a.txt", "\ue000.txt", "\u{10000}.txt"]) await fixture.write(path.join("entries", name), name);
		await fixture.compile("worker", { code: `${cHeaders("unistd.h stdio.h sys/syscall.h")}
int main(void) { char b[1024]; return syscall(SYS_getdents64, 4, b, sizeof(b)) < 0 || pread(3, b, 8, 0) != 8 || write(1, b, 8) != 8; }
` });
		await fixture.commit("Native resource input bridge"); await fixture.prepare();
		const tools: (typeof fixture.tool)[] = [name, "consume_rows"].map(name => ({ ...fixture.tool, name }));
		tools.push(createReadTool(fixture.workspace), createGrepTool(fixture.workspace), createLsTool(fixture.workspace), createFindTool(fixture.workspace));
		const prepared = patternProcessHost(fixture, "native-inputs", { resourceCacheMaxEntries: 8,
			resourceCacheMaxBytes: 32 * 1024, patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false }) }, {
			executionWorlds: [fixture.world, resourceWorld()],
			resolveInvocation: (tool, input) => {
				const process = tool === name || tool === "consume_rows";
				const invocation = search.invocations.get(tool) ?? resolvePiToolInvocation(process ? "bash" : tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath });
				return unkeyed && process ? { ...invocation!, semantics: { ...PI_ACTION_SEMANTICS.definition("bash")!, canonicalize: () => undefined } } : invocation;
			},
		}, tools), { events, start } = prepared;
		host = prepared.host;
		const error = new Error("native producer failed after completed work"), command = "exec 3<input.txt 4<entries; worker; worker" + (mode.endsWith("exit") ? "; exit 17" : "");
		const bash = vi.fn(async () => {
			const options = { cwd: fixture.workspace, env: fixture.environment, uid: process.getuid!(), gid: process.getgid!() };
			const output = mode.includes("sync") ? childProcess.execFileSync(fixture.shellPath, ["-c", command], options).toString() : await new Promise<string>((resolve, reject) =>
				childProcess.execFile(fixture.shellPath, ["-c", command], options, (error, stdout) => error ? reject(error) : resolve(stdout)));
			if (mode.endsWith("failed")) throw error; return { content: [fauxText(output)], details: parentOutput };
		});
		await start("seed");
		const result = host.execute({ turnID: "seed", id: "seed", tool: name, args: { command }, tools }, undefined, bash);
		if (mode.endsWith("failed")) await expect(result).rejects.toBe(error);
		else if (mode.endsWith("exit")) await expect(result).rejects.toThrow(/17/);
		else expect(await result).toEqual({ content: [fauxText("one\ntwo\none\ntwo\n")], details: parentOutput });
		await host.finishTurn("seed"); expect(bash).toHaveBeenCalledOnce();
		expect(observing.mock.calls[0]![0]).toEqual({ sessionID: "native-inputs", turnID: "seed", callID: "seed" });
		if (unkeyed) expect(events.find(event => event.type === "actor_action")).toMatchObject({ settlement: { provider: { kind: "actor", cause: { code: "action_not_keyable" } } } });
		await start("queries");
		if (mode.includes("sync") || mode === "keyed-mixed") {
			const command = "exec 3<input.txt 4<entries; worker remaining", before = fixture.backend.actorMetrics().hits;
			const consumer = vi.fn(async () => ({ content: [fauxText("remaining:" + childProcess.execFileSync(fixture.shellPath, ["-c", command],
				{ cwd: fixture.workspace, env: fixture.environment, encoding: "utf8" }))], details: {} }));
			expect(await host.execute({ turnID: "queries", id: "consumer", tool: "consume_rows", args: { command }, tools }, undefined, consumer))
				.toMatchObject({ content: [fauxText("remaining:one\ntwo\n")] });
			expect(consumer).toHaveBeenCalledOnce(); expect(fixture.backend.actorMetrics().hits).toBe(before);
			// Borrowing an input for an unsuccessful process lookup is validation, not consumed calculation.
			expect(events.filter(event => event.type === "actor_action").at(-1)?.computation?.hiddenComputeMs).toBe(0);
		}
		for (const [tool, args, change] of [["read", { path: "input.txt", offset: 2 }, undefined], ["grep", { path: "input.txt", pattern: "two" }, undefined],
			["ls", { path: "entries" }, undefined], ["find", { path: "entries", pattern: "*.txt" }, undefined], ["read", { path: "input.txt" }, "new\nvalue\n"], ["ls", { path: "entries" }, "new.txt"]] as const) {
			if (change) await writeFile(tool === "read" ? file : path.join(fixture.workspace, "entries", change), change);
			const native = tools.find(value => value.name === tool)!, invocation = search.invocations.get(tool);
			const authoritative = (callID: string) => invocation?.authoritative
				? invocation.authoritative({ callID, args, signal: new AbortController().signal }).then(value => value.result) : native.execute(callID, args);
			const execute = vi.fn(() => authoritative("fallback"));
			expect(await host.execute({ turnID: "queries", id: change ? `stale-${tool}` : tool, tool, args, tools }, undefined, execute)).toEqual(await authoritative("oracle"));
			expect(execute, JSON.stringify(summarizeSpeculativeTrace(events))).not.toHaveBeenCalled();
			if (tool === "read" && !change) expect(events.filter(event => event.type === "actor_action").at(-1)?.computation?.hiddenComputeMs).toBeGreaterThan(0);
		}
		await host.finishTurn("queries", true);
		expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 6, exactReuseHits: 0 });
	});

	test.for(["git", "auto"] as const)("selects changed running inputs after unrelated hints and compares the predecessor state (%s)", { timeout: 20_000 }, async (driver, { skip, benchmark }) => {
		const fixture = await benchmark("pi-running-inputs-", driver);
		let pending: ReturnType<typeof fixture.fork> | undefined;
		const hints = ["ignored.txt", "stable.txt", "input.txt"].map(name => path.join(fixture.workspace, name));
		await fixture.inOwner(async (state, hints) => {
			const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { ProcessHandoffRegistry } = await state.load("../src/process-handoff.ts");
			const observe = ProcessHandoffRegistry.prototype.observeRunning, open = LinuxProcessReuseBackend.prototype.open;
			ProcessHandoffRegistry.prototype.observeRunning = function (key: unknown, owner: any, invoke: unknown) {
				const release = observe.call(this, key, owner, invoke); state.check = owner.advance; return release;
			};
			LinuxProcessReuseBackend.prototype.open = function (input: any) { state.workspace = input.workspace; state.backend = this;
				return open.call(this, { ...input, workspace: { ...input.workspace, sourceChanges: () => ({ uncertain: false, paths: hints }) } });
			};
		}, hints);
		await using _cleanup = onExit(async () => {
			await fixture.inOwner(state => state.workspace && state.fs.writeFile(state.workspace.sandboxRoot + "/release", "").catch(() => {}));
			await (await pending?.catch(() => undefined))?.dispose();
		});
		for (const file of hints) await writeFile(file, "baseline");
		await fixture.compile("worker", { code: `${cHeaders("fcntl.h unistd.h")}
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
` });
		await fixture.commit("Running predecessor inputs");
		await fixture.prepare({ skip });
		pending = fixture.fork("printf predecessor > input.txt; worker");
		void pending.catch(() => {});
		await expect.poll(() => fixture.inOwner(state => state.workspace ? state.fs.access(state.workspace.sandboxRoot + "/ready").then(() => true, () => false) : false), { timeout: 5000 }).toBe(true);
		expect(await fixture.inOwner(state => typeof state.check)).toBe("function");
		await writeFile(hints[0]!, "unrelated"); await writeFile(hints[2]!, "predecessor");
		await fixture.inOwner(state => { state.opened = state.method(state.fs, "open"); });
		expect(await fixture.inOwner(state => state.check())).toBe(true); // The child's transaction starts after the shell's write.
		await writeFile(hints[2]!, "after");
		// A bounded trace preview can lag the separate filesystem readiness signal.
		await expect.poll(() => fixture.inOwner(state => state.check()), { timeout: 2000 }).toBe(false);
		expect(await fixture.inOwner((state, ignored) => state.opened.mock.calls.filter(({ arguments: [file] }: any) => String(file) === ignored).length, hints[0])).toBe(0);
		expect(await fixture.inOwner(state => state.backend.actorMetrics().lastError)).toContain("actor_running_input_changed:");
		await fixture.inOwner(state => state.fs.writeFile(state.workspace.sandboxRoot + "/release", ""));
		if (await fixture.inOwner(state => !!state.workspace.liveBase)) await expect(pending).rejects.toThrow("workspace changed since the sandbox started: input.txt");
		else expect((await pending).output.result.content).toEqual([fauxText("predecessor")]);
	});

nativeTest.for(["completed", "running", ...["", "-merged", "-closed-input", "-descriptors", "-descriptors-merged", "-null", "-null-stdin", "-status", "-directory", "-directory-entries", "-directory-prepared-stale", "-directory-opath", "-directory-opath-prepared-stale", "-pipe", "-pipe-prepared-stale", "-pipe-live", "-pipe-live-prepared-stale", "-pipe-live-mixed", "-pipe-live-transfer", "-pipe-producer", "-pipe-producer-queued", "-pipe-producer-short", "-pipe-producer-splice", "-socket", "-socket-running", "-socket-duplex", "-socket-lifetime", "-socket-io", "-socket-messages", "-socket-export", "-socket-counter-export", "-socket-prequeued", "-socket-segmented", "-socket-prefixed", "-socket-orphan", "-eventfd", "-eventfd-semaphore", "-socket-concurrent", "-socket-half-closed", "-socket-prepared-stale", "-shared-table", "-unshare", "-prepared-stale", "-prepared-restored"].map(mode => `native${mode}`)] as const)("reexecutes an owned child binding across turns without replaying its parent or stale input (%s)", { timeout: 20_000 }, async (mode, { skip, benchmark }) => {
		const fixture = await benchmark("pi-process-binding-", mode.includes("prepared-") ? "git" : undefined); // Borrowed immutable preparations.
		const publications = await recordProcessPublications(fixture);
		using errors = vi.spyOn(fixture.backend as unknown as { setError(session: unknown, message: string): void }, "setError");
		const native = mode.startsWith("native"), running = mode.endsWith("running"), enumerate = mode.includes("entries");
		const reopened = mode.startsWith("native-descriptors") || ["native-directory-entries", "native-directory-opath"].includes(mode);
		const orphan = mode.includes("orphan"), segmented = mode.includes("segmented"), prefixed = mode.endsWith("prefixed"), queueCounter = mode === "native-socket-counter-export", prequeued = orphan || segmented || prefixed || mode.endsWith("prequeued") || queueCounter;
		const discovery = segmented || orphan;
		const counter = mode.includes("eventfd"), semaphore = mode.endsWith("semaphore");
		const exporting = mode.endsWith("-export"), messages = mode.endsWith("messages"), transferIO = mode.endsWith("-splice");
		const shortWrite = mode.endsWith("short"), io = mode.endsWith("-io"), lifetime = mode.endsWith("lifetime"), pipeOutput = mode.startsWith("native-pipe-producer"), queuedOutput = mode === "native-pipe-producer-queued";
		const mixed = mode.endsWith("mixed"), socket = mode.includes("socket"), duplex = prequeued || exporting || lifetime || io || messages || mode.endsWith("duplex") || mode.endsWith("concurrent"), concurrent = mode.endsWith("concurrent"), pipe = mode.includes("pipe"), opath = mode.includes("opath") || mixed, launcher = counter || pipe || socket || mode === "native-shared-table" || mode === "native-unshare" || opath || enumerate;
		const nullDevice = mixed || mode === "native-null" || mode === "native-null-stdin" || mode === "native-status";
		const directory = mode.includes("directory") || mixed;
		const descriptors = mode.startsWith("native-descriptors") || launcher || nullDevice || directory;
		let host: ReturnType<typeof patternProcessHost>["host"] | undefined;
		let publication: Awaited<ReturnType<typeof holdOwnedProcessPublication>> | undefined;
		let restorePreparation: (() => Promise<void>) | undefined;
		const release = deferred();
		await using _cleanup = onExit(async () => { release.resolve(); await publication?.close(); await restorePreparation?.(); await host?.dispose(); });
		await fixture.write("input.txt", "before\n");
		if (descriptors) await fixture.write("fd.txt", "abcdef");
		if (mode === "native-descriptors-merged") await fixture.write("fd-other.txt", "abcdef");
		await fixture.compile("worker", { code: `#define _GNU_SOURCE
${cHeaders("errno.h poll.h sys/select.h sys/eventfd.h sys/syscall.h stdint.h sys/epoll.h stdlib.h")}
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
	${mode === "native" ? `{ int pair[2]; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair)) return 201;
	pid_t child = fork(); if (child < 0) return 202;
	if (!child) _exit(fcntl(pair[0], F_SETFL, O_RDWR | O_NONBLOCK) ? 203 : 0);
	if (wait_child(child)) return 204;
	int flags = fcntl(pair[0], F_GETFL), peer = fcntl(pair[1], F_GETFL);
	if (flags < 0 || peer < 0 || !(flags & O_NONBLOCK) || (peer & O_NONBLOCK) || close(pair[0]) || close(pair[1])) return 205;
	}` : ""}
	burn_cpu(${running ? 500000000 : native ? 50000000 : 0}ul);
	${orphan ? `{ if (fcntl(9, F_GETFD) != -1 || errno != EBADF || fcntl(15, F_GETFD) != -1 || errno != EBADF) return 187;
	int received[7]; char byte;
	for (int peek = 1; peek >= 0; peek--) {
		if (take(3, 'q', received, 7, peek, 0) || read(received[3], &byte, 1) != -1 || errno != EAGAIN) return 189;
		if (peek) for (int index = 0; index < 7; index++) close(received[index]);
	}
	if (read_text(received[0], &byte, "b") || lseek(received[1], 0, SEEK_CUR) != 2 ||
		fcntl(received[0], F_SETFL, O_NONBLOCK) || !(fcntl(received[1], F_GETFL) & O_NONBLOCK)) return 190;
	close(received[0]); close(received[1]);
	uint64_t count; if (read(received[2], &count, 8) != 8 || count != 5) return 191;
	count = 7; if (write(received[2], &count, 8) != 8 || read(received[2], &count, 8) != 8 || count != 7) return 192; close(received[2]);
	if (write(received[4], "x", 1) != 1) return 193;
	close(received[4]);
	if (read_text(received[3], &byte, "x") || read(received[3], &byte, 1) != 0) return 194;
	close(received[3]);
	int peer, alias;
	if (take(received[5], 'D', &peer, 1, 0, 0) || take(peer, 'C', &alias, 1, 0, 0) || pass(alias, "Z", 1, &received[6], 1, 0)) return 195;
	int root; if (take(peer, 'Z', &root, 1, 0, 0) || recv(root, &byte, 1, MSG_PEEK) != 1 || byte != 'b') return 196;
	close(root); close(alias); close(received[5]);
	if (read(peer, &byte, 1) != 0) return 197;
	close(peer); close(received[6]); }` : ""}
	${prefixed ? 'char gap; if (read_text(3, &gap, "L")) return 180;' : ""}
	${segmented ? 'char gap; if (read_text(3, &gap, "L")) return 180; int received[2]; char byte; for (int segment = 0; segment < 2; segment++) { for (int peek = 1; peek >= 0; peek--) { if (take(3, segment ? 116 : 113, received, 2, peek, 0)) return 181; if (!peek && !segment && read_text(received[0], &byte, "b")) return 183; close(received[0]); close(received[1]); } if (!segment && read_text(3, &gap, "I")) return 184; }' : prequeued && !orphan ? 'int received[2]; char byte; for (int peek = 1; peek >= 0; peek--) { if (take(3, 113, received, 2, peek, 0)) return 153; if (recv(received[1], &byte, 1, MSG_PEEK) != 1 || byte != (peek ? 113 : 98)) return 155; ' + (queueCounter ? 'if (!peek) { uint64_t count; if (read(received[0], &count, 8) != 8 || count != 5) return 158; count = 7; if (write(21, &count, 8) != 8) return 159; }' : 'if (!peek && read_text(received[0], &byte, "b")) return 156;') + ' close(received[0]); close(received[1]); }' : ""}
	${exporting ? `{ int passed = 3; if (fcntl(3, F_SETFL, O_NONBLOCK) || pass(3, "x", 1, &passed, 1, 0)) return 132; close(0); close(3); close(4); close(8); char text[32]; int fd = open("input.txt", O_RDONLY); ssize_t length = read(fd, text, sizeof(text)); return length <= 0 || write(1, text, (size_t)length) != length; }` : ""}
	${mode === "native-closed-input" ? 'char probe; if (read(0, &probe, 1) != -1 || errno != EBADF) return 72;' : ""}
	${io ? 'struct pollfd ready = {.fd = 3, .events = POLLIN | POLLOUT}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT) || fcntl(3, F_SETFL, O_NONBLOCK)) return 114; fd_set rd, wr; FD_ZERO(&rd); FD_ZERO(&wr); FD_SET(3, &rd); FD_SET(3, &wr); struct timeval timeout = {0}; if (select(4, &rd, &wr, 0, &timeout) != 2 || !FD_ISSET(3, &rd) || !FD_ISSET(3, &wr)) return 118; int ep = epoll_create1(EPOLL_CLOEXEC); struct epoll_event registration = {.events = EPOLLIN | EPOLLONESHOT, .data.u64 = 42}, readyEvents[2]; if (ep < 0 || epoll_ctl(ep, EPOLL_CTL_ADD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || readyEvents[0].events != EPOLLIN || readyEvents[0].data.u64 != 42 || epoll_wait(ep, readyEvents, 2, 0) != 0 || epoll_ctl(ep, EPOLL_CTL_MOD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || epoll_ctl(ep, EPOLL_CTL_DEL, 3, 0)) return 119; registration.events = EPOLLIN | EPOLLET; if (epoll_ctl(ep, EPOLL_CTL_ADD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1 || epoll_wait(ep, readyEvents, 2, 0) != 0 || epoll_ctl(ep, EPOLL_CTL_MOD, 3, &registration) || epoll_wait(ep, readyEvents, 2, 0) != 1) return 125; close(ep);' : ""}
	${counter ? `uint64_t count[2] = {0};
	if (read(20, count, sizeof(count)) != 8 || count[0] != ${semaphore ? 1 : 5}) return 137;
	count[0] = 2; if (write(21, count, 8) != 8) return 138;
	struct pollfd ready = {.fd = 20, .events = POLLIN | POLLOUT}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT)) return 139;
	int ep = epoll_create1(EPOLL_CLOEXEC); struct epoll_event registration = {.events = EPOLLIN | EPOLLONESHOT, .data.u64 = 42}, readyEvent;
	if (ep < 0 || epoll_ctl(ep, EPOLL_CTL_ADD, 21, &registration) || epoll_wait(ep, &readyEvent, 1, 0) != 1 || readyEvent.events != EPOLLIN || readyEvent.data.u64 != 42 || epoll_wait(ep, &readyEvent, 1, 0) != 0) return 140; close(ep);
	if (fcntl(20, F_SETFL, 0) || (fcntl(21, F_GETFL) & O_NONBLOCK) || fcntl(21, F_SETFL, O_NONBLOCK) || read(21, count, 8) != 8 || count[0] != ${semaphore ? 1 : 2}) return 141;
	${semaphore ? 'pid_t forked = fork(); if (forked < 0) return 142; if (!forked) return read(20, count, 8) != 8 || count[0] != 1; if (wait_child(forked)) return 143;' : ''}
	count[0] = 7; if (write(20, count, 8) != 8) return 144;
	count[0] = 1; if (write(22, count, 8) != 8) return 145;
	ready.fd = 22; if (poll(&ready, 1, 0) != 1 || ready.revents != POLLIN || write(22, count, 8) != -1 || errno != EAGAIN) return 146;
	count[0] = UINT64_MAX; if (write(22, count, 8) != -1 || errno != EINVAL || read(22, count, 7) != -1 || errno != EINVAL) return 147;
	if (read(22, count, 8) != 8 || count[0] != UINT64_MAX - 1 || read(22, count, 8) != -1 || errno != EAGAIN) return 148;
	` : ""}

	${socket ? 'char preview; if (recv(3, &preview, 1, MSG_PEEK) != 1 || preview != \'b\') return 106;' : ""}
	${transferIO ? 'if (tee(3, 14, 3, SPLICE_F_NONBLOCK) != 3 || splice(3, 0, 14, 0, 3, SPLICE_F_NONBLOCK) != 3) return 130;' : descriptors ? 'char a, b, c; if (read(3, &a, 1) != 1 || read(4, &b, 1) != 1 || read(8, &c, 1) != 1 || a != \'b\' || b != \'c\' || c != \'a\') return 72;' : ""}
	${descriptors ? 'int alias = dup(4); if (alias < 0 || fcntl(alias, F_GETFL) != fcntl(3, F_GETFL) || (fcntl(8, F_GETFL) & O_ACCMODE) != ' + (socket ? 'O_RDWR' : 'O_RDONLY') + ') return 74; close(alias);' : ""}
	${socket ? (duplex ? 'char peer[5]; if (read_text(14, peer, "peer")) return 100; ' + (messages ? 'int passed[2] = {9, 4}; char value; if (pass(3, "M", 1, passed, 2, MSG_NOSIGNAL)) return 126; close(9); int received[2]; for (int peek = 1; peek >= 0; peek--) { if (take(14, 77, received, 2, peek, 0)) return 127; if (recv(received[1], &value, 1, MSG_PEEK) != 1 || value != 88) return 128; if (!peek && (read(received[0], &value, 1) != 1 || value != 98)) return 129; close(received[0]); close(received[1]); } ' : '') + (io ? 'char absent; if (recv(14, &absent, 1, MSG_DONTWAIT) != -1 || errno != EAGAIN) return 115; struct pollfd empty = {.fd = 14, .events = POLLIN}; if (poll(&empty, 1, 0) != 0 || empty.revents) return 116; ' : '') + (concurrent ? 'pid_t child = fork(); if (child < 0) return 103; if (!child) { size_t length = 0; while (length < 5) { ssize_t size = read(14, peer + length, 5 - length); if (size <= 0) return 104; length += (size_t)size; } return memcmp(peer, "reply", 5) || write(14, "back", 4) != 4 || shutdown(14, SHUT_WR); } ' : '') : '') + 'if (write(3, "re", 2) != 2 || write(4, "ply", 3) != 3 || ' + (lifetime ? '0' : 'shutdown(4, SHUT_WR)') + ') return 94;' + (duplex ? concurrent ? ' if (wait_child(child)) return 105;' : ' if (read_text(14, peer, "reply") || write(14, "back", 4) != 4 || ' + (lifetime ? '0' : 'shutdown(14, SHUT_WR)') + ') return 101;' : '') : pipeOutput ? shortWrite ? 'char payload[8192]; memset(payload, 90, sizeof(payload)); if (write(14, payload, sizeof(payload)) != 4096 || write(14, payload, 1) != -1 || errno != EAGAIN) return 120;' : 'if (write(14, "re", 2) != 2 || write(14, "ply", 3) != 3) return 94;' : ""}
	${io ? 'ready = (struct pollfd){.fd = 14, .events = POLLIN | POLLOUT | POLLRDHUP}; if (poll(&ready, 1, 0) != 1 || ready.revents != (POLLIN | POLLOUT | POLLRDHUP | POLLHUP)) return 117; if (send(3, "!", 1, MSG_NOSIGNAL) != -1 || errno != EPIPE) return 124;' : ""}
	${lifetime ? 'char rest[8]; if (read_text(3, rest, "XYZback")) return 107; close(0); close(3); close(4); close(8); if (read(14, rest, 1) != 0) return 108;' : ""}
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
	int waited; if (wait_child(reader) || lseek(10, 0, SEEK_CUR) != start) return 165;
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
` });
		if (launcher) {
			await fixture.compile("fd-launch", { arguments: ["-pthread", "-Werror"], code: `#define _GNU_SOURCE
${cHeaders("pthread.h sys/syscall.h stdlib.h errno.h stdint.h sys/eventfd.h sched.h stdio.h")}
${rightsHelpers}
static void *duplicate(void *unused) { (void)unused; if (dup2(3, 9) < 0) _exit(73); return 0; }
int main(int argc, char **argv) {
	if (argc != 2) return 74;
	pthread_t worker; if (pthread_create(&worker, 0, duplicate, 0) || pthread_join(worker, 0)) return 75;
	${mode === "native-unshare" ? "if (unshare(CLONE_FILES)) return 77;" : ""}
	${opath ? 'int directory = open(".", O_PATH | O_DIRECTORY), file = open("fd.txt", O_PATH);\n\tif (directory < 0 || file < 0 || dup2(directory, 10) < 0 || dup2(directory, 11) < 0 || dup2(file, 12) < 0) return 79;\n\tclose(directory); if (file != 12) close(file);' : ""}
	${pipe ? 'int fds[2]; if (pipe2(fds, O_CLOEXEC) || write(fds[1], "bcaXYZ", 6) != 6) return 80; ' + (mode.includes("pipe-live") ? '' : 'close(fds[1]); ') + 'if (dup2(fds[0], 0) < 0) return 81; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0) return 82; int reopened = open("/proc/self/fd/0", O_RDONLY); if (move_fd(reopened, 8)) return 83;' : ""}
	${mixed ? 'int writable = open("anonymous", O_RDWR | O_CREAT | O_EXCL, 0600); if (writable < 0 || unlink("anonymous") || write(writable, "abcdef", 6) != 6 || lseek(writable, 0, SEEK_SET) != 0 || dup2(writable, 16) < 0 || dup2(writable, 17) < 0) return 87; close(writable); int reader = open("/proc/self/fd/16", O_RDONLY); if (reader < 0 || dup2(reader, 18) < 0) return 88; close(reader);' : ""}
	${socket ? 'int fds[2]; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) || ' + (prequeued ? '0' : 'write(fds[1], "bcaXYZ", 6) != 6') + ' || dup2(fds[0], 0) < 0) return 95; close(fds[0]); if (dup2(0, 3) < 0 || dup2(0, 4) < 0 || dup2(0, 8) < 0) return 96;' + (mode === "native-socket-half-closed" ? ' if (shutdown(fds[1], SHUT_WR)) return 97;' : '') : ""}
	${queueCounter ? 'int counter = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC); if (counter < 0 || dup2(counter, 20) < 0 || dup2(counter, 21) < 0) return 160; close(counter);' : ""}
	${orphan ? `int queue[2], channel[2], counter = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC);
	if (counter < 0 || pipe2(queue, O_CLOEXEC | O_NONBLOCK) || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, channel)) return 198;
	int rights[] = {9, 9, counter, queue[0], queue[1], channel[0], 3};
	if (pass(channel[0], "C", 1, &channel[0], 1, 0) || pass(channel[1], "D", 1, &channel[1], 1, 0) ||
		pass(fds[1], "qbcaXYZ", 7, rights, 7, 0) || pass(fds[1], "T", 1, rights, 1, 0)) return 199;
	close(counter); close(queue[0]); close(queue[1]); close(channel[0]); close(channel[1]);` : ""}
	${prequeued && !orphan ? 'int passed[2] = {' + (queueCounter ? '20' : '9') + ', 3}; ' + (segmented ? 'if (write(fds[1], "L", 1) != 1 || pass(fds[1], "qI", 2, passed, 2, 0)) return 185; if (pass(fds[1], "t", 1, passed, 2, 0) || write(fds[1], "bcaXYZ", 6) != 6) return 186;' : (prefixed ? 'if (write(fds[1], "L", 1) != 1) return 186; ' : '') + 'if (pass(fds[1], "qbcaXYZ", 7, passed, 2, 0)) return 157;') : ""}
	${duplex ? 'if (dup2(fds[1], 14) < 0 || write(0, "peer", 4) != 4) return 100;' : ""}
	${orphan ? 'if (close(9)) return 188;' : ""}
	${pipeOutput ? 'int output[2]; if (pipe2(output, O_CLOEXEC) || dup2(output[1], 14) < 0 || dup3(output[0], 15, O_CLOEXEC) < 0) return 95; ' + (shortWrite ? 'if (fcntl(14, F_SETPIPE_SZ, 4096) != 4096 || fcntl(14, F_SETFL, O_NONBLOCK)) return 121; ' : '') + (queuedOutput ? 'if (write(output[1], "start", 5) != 5) return 112; ' : '') + 'close(output[0]); close(output[1]);' : ""}
	${counter ? 'int counter = eventfd(5, EFD_CLOEXEC | EFD_NONBLOCK' + (semaphore ? ' | EFD_SEMAPHORE' : '') + '), full = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); uint64_t count = UINT64_MAX - 2; if (counter < 0 || full < 0 || write(full, &count, 8) != 8 || dup2(counter, 20) < 0 || dup2(counter, 21) < 0 || dup2(full, 22) < 0) return 149; close(counter); close(full);' : ""}
	${lifetime || exporting ? 'int gate[2]; if (pipe2(gate, O_CLOEXEC)) return 109;' : ""}
	puts(argv[1]); fflush(stdout);
	${enumerate ? 'int anchor = open(".", O_PATH | O_DIRECTORY); if (move_fd(anchor, 6)) return 175; char entries[24]; int independent = open(".", O_RDONLY | O_DIRECTORY); if (independent < 0 || dup2(independent, 13) < 0 || syscall(SYS_getdents64, 10, entries, sizeof(entries)) != 24) return 168; if (independent != 13) close(independent); char captured[39]; for (int index = 0; index < 19; index++) snprintf(captured + 2 * index, 3, "%02x", (unsigned char)entries[index]); if (setenv("BOUND_DIRECTORY_ENTRY", captured, 1)) return 178; pid_t reader = fork(); if (reader < 0) return 169; if (reader) { if (wait_child(reader) || lseek(10, 0, SEEK_CUR) <= 9007199254740991LL || lseek(11, 0, SEEK_CUR) != lseek(10, 0, SEEK_CUR) || lseek(13, 0, SEEK_CUR) <= 0 || lseek(13, 0, SEEK_CUR) == lseek(10, 0, SEEK_CUR)) return 170; return 0; }' : ""}
	${mode === "native-pipe-live-transfer" ? `int sockets[2], passed[] = {0, 3, 4, 8};
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) || pass(sockets[0], "x", 1, passed, 4, 0)) return 89;
	close(sockets[0]);` : ""}
	${counter ? 'pid_t child = fork(); if (child < 0) return 150; if (child) { if (wait_child(child)) return 151; uint64_t count, total = 0; while (read(21, &count, 8) == 8) total += count; if (errno != EAGAIN || total != ' + (semaphore ? 11 : 7) + ' || !(fcntl(20, F_GETFL) & O_NONBLOCK) || read(22, &count, 8) != -1 || errno != EAGAIN) return 152; return 0; }' : ""}
	${pipe || socket ? 'pid_t child = fork(); if (child < 0) return 84; ' + (exporting ? 'if (child) { close(0); close(3); close(4); close(8); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 133; close(gate[1]); if (wait_child(child)) return 134; char text[6]; int received; if (take_text(14, "peerx", 5, &received, 1, 0, 0)) return 135; if (!(fcntl(received, F_GETFL) & O_NONBLOCK) || read_text(received, text, "bcaXYZ")) return 136; close(received); ' + (queueCounter ? 'uint64_t count; if (read(20, &count, 8) != 8 || count != 7) return 161; ' : '') + 'return 0; } ' : '') + (lifetime ? 'if (child) { close(0); close(3); close(4); close(8); close(14); close(fds[1]); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 110; close(gate[1]); return wait_child(child); } ' : '') + 'if (child) { char tail[3]; if (wait_child(child) || ' + (mixed ? 'lseek(16, 0, SEEK_CUR) != 4 || lseek(18, 0, SEEK_CUR) != 0 || pread(18, tail, 1, 2) != 1 || tail[0] != 81 || !(fcntl(3, F_GETFL) & O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK) || ' : '') + 'read(3, tail, 1) != 1 || read(4, tail + 1, 1) != 1 || read(8, tail + 2, 1) != 1 || tail[0] != \'X\' || tail[1] != \'Y\' || tail[2] != \'Z\') return 85; ' + (orphan ? 'int remaining; if (take(3, 84, &remaining, 1, 0, 0) || lseek(remaining, 0, SEEK_CUR) != 2 || !(fcntl(remaining, F_GETFL) & O_NONBLOCK)) return 200; close(remaining); ' : '') + (socket || pipeOutput ? 'char reply[6]; ' + (shortWrite ? 'char head[4091]; if (read(15, head, sizeof(head)) != sizeof(head)) return 122; for (unsigned index = 0; index < sizeof(head); index++) if (head[index] != 90) return 123; ' : '') + (transferIO ? 'if (read_text(15, reply, "bcabca")) return 131; ' : '') + (queuedOutput ? 'if (read_text(15, reply, "start")) return 113; ' : '') + (duplex ? 'if (read_text(0, reply, "back") || read(0, reply, 1) != 0) return 102; ' : 'if (read_text(' + (socket ? 'fds[1]' : '15') + ', reply, "' + (shortWrite ? 'ZZZZZ' : 'reply') + '")) return 98; ') + (socket ? 'if (read(fds[1], reply, 1) != 0) return 99; ' : '') : '') + 'return 0; }' : ""}
	${lifetime || exporting ? 'if (wait_gate(gate)) return 111;' : ""}
	${exporting ? 'close(14);' : ""}
	${mode === "native-pipe-live-transfer" ? `for (unsigned index = 0; index < 4; index++) close(passed[index]);
	int received[4], saved[4]; if (take(sockets[1], 'x', received, 4, 0, 0)) return 91;
	for (unsigned index = 0; index < 4; index++) { saved[index] = fcntl(received[index], F_DUPFD_CLOEXEC, 100); if (saved[index] < 0) return 92; close(received[index]); }
	for (unsigned index = 0; index < 4; index++) { if (dup2(saved[index], passed[index]) < 0) return 93; close(saved[index]); }
	close(sockets[1]);` : ""}
	char *command[] = {"bound-name", "private argument", 0}; execv("./worker", command); return 76;
}
` });
		}
		await fixture.commit("Bound process invocation");
		const { executionFingerprint } = await fixture.prepare({ skip });
		const scope = { sessionID: "binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
		const command = (descriptors ? `exec 3<fd.txt; exec 4<&3; exec 8<${mode === "native-descriptors-merged" ? "fd-other.txt" : "fd.txt"}; IFS= read -r -N 1 discarded <&3; ` : "") +
			(directory ? "exec 10<.; exec 11<&10; " : "") +
			(nullDevice ? (mode === "native-null-stdin" ? "exec 0<>/dev/null; exec 6<&0; " : "exec 6<>/dev/null; ") + "exec 7<&6; " : "") +
			"export BOUND_SECRET='private value'; " + (launcher ? "exec fd-launch parent" : "printf 'parent\\n'; exec -a bound-name worker 'private argument'") +
			(mode === "native-merged" ? " 2>&1" : mode === "native-closed-input" ? " 0<&-" : "");
		const route = await fixture.prepareActorReplay(!native);
		const invocation = fixture.invocation({ command: "exit 92" }).process!;
		let binding: ProcessExecutionBinding | undefined;
		if (!native) {
			for (const value of ["before", ...Array.from({ length: 8 }, (_, version) => `history-${version}`)]) {
				await fixture.write("input.txt", `${value}\n`);
				await using branch = disposable(await fixture.fork(command, { namespace: "binding", executionScope: scope }));
				expect(branch.output).toMatchObject({ isError: false, result: { content: [{ text: `parent\n${value}\n` }] } });
				const invocation = fixture.invocation({ command });
				const permission = PI_ACTION_SEMANTICS.buildKey("bash", { command }, fixture.workspace, "binding", { fingerprint: executionFingerprint, context: invocation })!;
				const binding = branch.operations![0]!;
				for (const operation of [{ binding: Object.freeze({ ...binding }), permission }, { binding, permission: { ...permission, key: "different action with colliding hash" } }]) {
					await expect(fixture.world.speculation.fingerprint!({ effect: "unbounded", requirements: PI_ACTION_SEMANTICS.definition("bash")!.requirements, tool: "bash",
						action: { ...permission, executionContext: { ...invocation, operation } } })).rejects.toThrow("binding is unavailable");
				}
			}
			[binding] = fixture.backend.executionBindings(later);
			expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
			const history = (await publications()).filter(certificate => certificate.weakKey === binding!.key), [certificate] = history;
			expect(certificate!.producer.execution.authority).toBe("speculative");
			expect(certificate!.dependencyCertificate.taints).toEqual(["clock", "random"]);
			expect(await fixture.backend.store.findByWeakKey(binding!.key, path.join(fixture.workspace, "worker"))).toEqual([]);
			expect(JSON.stringify(binding)).not.toContain("private argument");
			expect(JSON.stringify(binding)).not.toContain("private value");
			expect(new Set(history.map(certificate => certificate.id)).size).toBe(9);
			for (const certificate of history) await fixture.backend.store.put(certificate);
			await fixture.write("input.txt", "after\n");
			await expect(validateTransferredProcessEvidence(certificate!.dependencyCertificate)).resolves.toMatchObject({ status: "stale" });
			await fixture.inOwner(state => {
				const open = state.fs.open; state.validationPaths = [];
				state.method(state.fs, "open", async (...args: unknown[]) => {
					const handle = await open(...args); state.validationPaths.push(await state.fs.readlink(`/proc/self/fd/${handle.fd}`)); return handle;
				});
			});
			for (const value of ["after", "before", "history-3", "after"]) {
				await fixture.write("input.txt", `${value}\n`); await fixture.inOwner(state => { state.validationPaths = []; });
				expect(await fixture.backend.bindingStale(binding!)).toBe(value === "after");
				expect(await fixture.inOwner((state, target) => state.validationPaths.filter((file: string) => file === target).length,
					path.join(fixture.workspace, "input.txt"))).toBe(1); // History shares one pinned read; each lookup captures it again.
			}

			let operationRoot = "";
			await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
				operationRoot = workspace.processRoot;
				await withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: later }, binding!, "after\n",
					async session => { await expect(session.executeBinding(binding!)).rejects.toThrow("already consumed"); });
				expect((await filesystem.readdir(workspace.processRoot)).filter(name => name.startsWith("broker-"))).toEqual([]);
			});
			expect(existsSync(operationRoot)).toBe(false);

			expect(await executeProcess(fixture, route.executor, command.replace("parent", "other-parent"), { scope: { ...scope, turnID: "actor" } }))
				.toEqual({ exitCode: 0, output: "other-parent\nafter\n" });
			expect(fixture.backend.actorMetrics()).toMatchObject({ hits: 0, crossTurnHits: 0 });
		} else {
			expect(fixture.backend.executionBindings(later)).toEqual([]);
			await fixture.write("input.txt", "after\n");
		}

		const prepared = patternProcessHost(fixture, scope.sessionID, { tools: [], candidateLimit: 4, maxConcurrentActions: 6,
			patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 }) }, {
			// This boundary fixture budgets two native preparations plus the Actor; single-CPU live transfers are tested above.
			resources: { initial: { cpuCount: 6, idleCpuCount: 6 }, sample: async () => ({ cpuCount: 6, idleCpuCount: 6 }) },
			preflight: ({ args, action }) => { expect(args).toHaveProperty("command"); expect(action.input.command).toBe((args as { command: string }).command); return true; },
			executionWorlds: [{ ...fixture.world, speculation: { ...fixture.world.speculation, execute: async context => {
				if (mode === "completed" && context.executionScope?.turnID === "prepared" && typeof context.action.input.operation === "string") await TimelineInterval.outside(() => release.promise);
				return fixture.world.speculation.execute(context);
			} } }],
		}), { events, patternStore, start, call } = prepared;
		host = prepared.host;
		const operations = () => events.filter(event => event.type === "operation_prediction");
		const succeeded = (turnID: string, kind: "operation" | "actor_preview") => events.filter(event => event.type === "candidate" &&
			event.turnID === turnID && event.state.status === "succeeded" && (kind === "operation" ? event.candidate.kind : event.candidate.origin) === kind);
		const diagnostic = () => JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(),
			errors: errors.mock.calls.map(call => call[1]), operations: operations(), bindings: fixture.backend.executionBindings(later) });
		for (const turnID of ["common-1", "common-2"]) {
			await start(turnID);
			await host.execute(call(turnID, "printf common"), undefined, () => fixture.tool.execute(turnID, { command: "printf common" }));
			await host.finishTurn(turnID);
		}
		if (discovery) {
			const coldScope = { ...scope, turnID: "cold-queue" };
			await fixture.inOwner(async state => {
				const { ProcessHandoffRegistry } = await state.load("../src/process-handoff.ts"), observe = ProcessHandoffRegistry.prototype.observe;
				state.method(ProcessHandoffRegistry.prototype, "observe", function (this: unknown, ...args: unknown[]) { state.registry = this; return observe.apply(this, args); });
			});
			await start(coldScope.turnID);
			const output = await host.execute(call(coldScope.turnID, command), undefined, () => fixture.coordinator.runWith(route.executor, () => fixture.tool.execute(coldScope.turnID, { command })));
			expect(output.content).toEqual([fauxText("parent\nafter\n")]);
			await host.finishTurn(coldScope.turnID);
			expect(await fixture.inOwner((state, scope) => state.registry.bindings(scope).some((binding: ProcessExecutionBinding) =>
				state.registry.resolveBinding(binding, scope)?.trackingOnly), coldScope)).toBe(true);
		}
		await start("seed");
		if (!native) {
			await host.previewActorCall(call("seed", command));
			await prepared.waitFor(() => succeeded("seed", "actor_preview").length > 0);
		}

		await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : [])]);
		const seedFallback = vi.fn(() => fixture.coordinator.runWith(route.executor, () => fixture.tool.execute("seed", { command })));
		const suffix = mode === "native-merged" ? "stderr\n" : "";
		expect((await host.execute(call("seed", command), undefined, seedFallback)).content).toEqual([fauxText(`parent\nafter\n${suffix}`)]);
		await host.finishTurn("seed");
		// The previewed whole Bash command is adopted outright once its evidence validates; a native run still binds its child.
		expect(seedFallback).toHaveBeenCalledTimes(native ? 1 : 0);
		expect(fixture.backend.actorMetrics().hits, diagnostic()).toBe(0);
		binding ??= fixture.backend.executionBindings(later).at(-1);
		if (reopened || launcher) {
			const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ readonly executable: string }>;
			binding = fixture.backend.executionBindings(later).find(candidate => registry.resolveBinding(candidate, later)?.executable === path.join(fixture.workspace, "worker"));
		}
		expect(binding, "a real native miss must retain its launch without publishing a result").toBeDefined();
		if (["completed", "native", "native-merged", "native-descriptors", "native-pipe-live"].includes(mode))
			expect(fixture.backend.operationHints(binding!).preparation).toBe(mode === "native" || mode === "native-merged" || mode === "native-descriptors" ? "current_workspace" : undefined);
		if (mode === "native-pipe-live") expect(fixture.backend.operationHints(binding!).fed).toBe(true);
		let retainedOperation: ExecutionOperationBinding | undefined;
		if (mode === "native") for (const turnID of ["repeated-native-1", "repeated-native-2"]) {
			const repeatedScope = { ...scope, turnID };
			const previousMs = binding!.executionMs;
			const action = PI_ACTION_SEMANTICS.buildKey("bash", { command }, fixture.workspace, "binding")!;
			const result = await fixture.world.observeOperations!({ action, scope: repeatedScope, learn: true },
				() => executeProcess(fixture, route.executor, command.replace("parent", turnID), { scope: repeatedScope }), bindings => { retainedOperation ??= bindings.find(item => item.identity === binding!.key); });
			expect(binding!.executionMs).not.toBe(previousMs);
			expect(retainedOperation).toMatchObject({ executionMs: binding!.executionMs, preparation: "current_workspace" });
			expect(result.output).toBe(`${turnID}\nafter\n`);
			expect(fixture.backend.executionBindings(later).filter(item => item.key === binding!.key)).toEqual([binding]);
		}
		await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : []), command]);
		await fixture.write("input.txt", "newest\n");
		if (reopened) {
			await fixture.write("fd.txt", "abcdeZ"); // Same size, another full object version.
			await filesystem.link(path.join(fixture.workspace, "fd.txt"), path.join(fixture.workspace, "fd-alias.txt"));
			if (mode === "native-descriptors-merged") {
				await rm(path.join(fixture.workspace, "fd-other.txt"));
				await filesystem.link(path.join(fixture.workspace, "fd.txt"), path.join(fixture.workspace, "fd-other.txt"));
			}
			expect(await fixture.backend.bindingStale(binding!)).toBe(true);
		}
		const before = fixture.backend.metrics();
		const publicationStart = (await publications()).length;
		if (running) publication = await holdOwnedProcessPublication(fixture, undefined, path.join(fixture.workspace, "worker"));
		if (mode.includes("prepared-")) {
			const capture = fixture.workspaceSandbox.capture;
			const fault = vi.spyOn(fixture.workspaceSandbox, "capture").mockImplementation(async (options, parent) => {
				if (options.preparation) await fixture.write("input.txt", "changed during preparation\n");
				return capture.call(fixture.workspaceSandbox, options, parent);
			});
			restorePreparation = async () => { fault.mockRestore(); };
		}
		const sourceMetadata = async () => Promise.all(["", "fd.txt", "fd-alias.txt"].map(async name => {
			const entry = await filesystem.lstat(path.join(fixture.workspace, name), { bigint: true });
			return Object.fromEntries(FILESYSTEM_METADATA_FIELDS.map(field => [field, String(entry[field])]));
		}));
		// Input-graph capture must preserve its source objects before the Actor uses them.
		if (enumerate) await filesystem.utimes(fixture.workspace, 1, (await filesystem.stat(fixture.workspace)).mtime);
		const sourceBeforePreparation = enumerate ? await sourceMetadata() : undefined;
		await start("prepared");
		if (mode === "completed") {
			await host.previewActorCall(call("prepared", command));
			await prepared.waitFor(() => succeeded("prepared", "actor_preview").length > 0);
		} else if (running) await publication!.ready();
		else await prepared.waitFor(() => succeeded("prepared", "operation").length > 0);
		if (launcher && !running) await expect.poll(async () => (await publications()).slice(publicationStart).some(certificate =>
			certificate.prototype.executablePath === path.join(fixture.workspace, "worker")), { timeout: 5000 }).toBe(true).catch(error => {
			throw new Error(diagnostic(), { cause: error });
		});
		if (mode.includes("prepared-")) {
			await prepared.waitFor(() => succeeded("prepared", "operation").length === (launcher ? 2 : 1));
			expect(await fixture.read("input.txt")).toBe("changed during preparation\n");
			await restorePreparation?.(); restorePreparation = undefined;
			// Every preparation is sealed before this change; no later producer can capture the replacement value.
			await fixture.write("input.txt", mode === "native-prepared-restored" ? "newest\n" : "changed after preparation\n");
		}
		if (reopened) {
			await expect.poll(() => fixture.backend.bindingStale(binding!), { timeout: 5000 }).toBe(false);
			expect((await publications()).slice(publicationStart).some(certificate => certificate.weakKey !== binding!.key &&
				certificate.prototype.inheritedFDs.some(fd => fd.resourceAliases?.includes(path.join(fixture.workspace, "fd-alias.txt"))))).toBe(true);
		}
		if (enumerate) expect(await sourceMetadata()).toEqual(sourceBeforePreparation);
		const changedParent = command.replace("parent", "automatic-parent");
		if (running) {
			expect((await executeProcess(fixture, route.executor, changedParent, { scope: { ...scope, turnID: "foreign" } })).output).toBe("automatic-parent\nnewest\n");
			expect(await publication!.joined(), "another turn cannot wait for this one-shot producer").toBe(false);
		}
		const actor = vi.fn(() => fixture.coordinator.runWith(route.executor, () => fixture.tool.execute("prepared", { command: changedParent })));
		const stalePreparation = mode.endsWith("prepared-stale");
		expect((await host.execute(call("prepared", changedParent), undefined, actor)).content).toEqual([fauxText(`automatic-parent\n${stalePreparation ? "changed after preparation\n" : "newest\n"}${suffix}`)]);
		if (running) expect(await publication!.joined(), diagnostic()).toBe(true);
		expect(actor).toHaveBeenCalledOnce();
		if (segmented || orphan) expect(fixture.backend.actorMetrics().hits).toBe(1);
		if (orphan) expect((await publications()).some(certificate => certificate.prototype.inheritedFDs.some(fd => fd.installed === false))).toBe(true);
		expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify({ joined: await publication?.joined(), metrics: fixture.backend.actorMetrics() })).toBe(Number(running));
		expect(fixture.backend.metrics().misses).toBeGreaterThan(before.misses);
		release.resolve(); await host.finishTurn("prepared");
		expectHiddenCompute(consumedTiming(events, "prepared"), !stalePreparation);
		expect(operations().filter(event => !launcher || stalePreparation || event.predictedAction?.includes(binding!.key)), diagnostic()).toMatchObject(Array.from({ length: launcher && stalePreparation ? 2 : 1 }, () => ({ settlement: stalePreparation ? { observation: "unobserved" } : {
			prediction: { source: "execution_preparation", kind: "operation" }, observation: "observed", match: { matched: true, adoption: { status: "adopted" } },
		} })));
		await expect.poll(() => patternStore.recent(scope.sessionID).map(event => event.input.command)).toEqual(["printf common", "printf common", ...(discovery ? [command] : []), command, changedParent]);
		expect(JSON.stringify(events.filter(event => event.type === "candidate" && event.candidate.kind === "operation"))).not.toContain("private value");
		await fixture.backend.storage.maintain("clear");
		expect(fixture.backend.executionBindings(later)).toEqual([]);
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
			await using session = disposable(await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later }));
			await expect(session.executeBinding(binding!)).rejects.toThrow("unavailable in this scope");
		});
	});

nativeTest.for(["temporary", "persistent", "descendant", "merged", "stale", "stack", "stack-unlimited"])("seals output pipe status before reusing a child (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
		const fixture = await benchmark("pi-output-status-", "git", {});
		await fixture.write("input.txt", "before\n");
		await fixture.compile("worker", { code: `${cHeaders("fcntl.h sys/resource.h")}
${processChecks}
int main(void) {
	${mode.startsWith("stack") ? `struct rlimit limit; if (getrlimit(RLIMIT_STACK, &limit) || limit.rlim_cur != ${mode === "stack" ? "67108864" : "RLIM_INFINITY"}) return 75;` : ""}
	burn_cpu(20000000ul);
	int fd = dup(${mode === "merged" ? 2 : 1}), flags = fcntl(fd, F_GETFL);
	if (fd < 0 || flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK)) return 71;
	${mode === "descendant" ? 'int child = fork(); if (child < 0) return 72; if (child) return 0;' : ""}
	${mode !== "persistent" ? 'if (fcntl(fd, F_SETFL, flags)) return 73;' : ""}
	char bytes[32]; int input = open("input.txt", O_RDONLY); if (input < 0) return 74;
	int length = read(input, bytes, sizeof(bytes)); return length < 0 || write(fd, bytes, length) != length;
}` });
		await fixture.commit("output status");
		const scope = { sessionID: "output-status", turnID: "learned" }, later = { ...scope, turnID: "prepared" };
		const { execute, binding, invocation, seed } = await learnWorker(fixture, scope, `${mode.startsWith("stack") ? `ulimit -S -s ${mode === "stack" ? "65536" : "unlimited"}; ` : ""}worker${mode === "merged" ? " 2>&1" : ""} | cat`);
		expect(seed).toBe("before\n");
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
			withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: later }, binding!, "before\n", async session => {
				await session.seal([]);
				if (mode === "stale") await fixture.write("input.txt", "after\n");
				expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: mode === "persistent" ? "indeterminate" : mode === "stale" ? "stale" : "valid" });
				if (mode === "persistent") expect(session.metrics().lastError).toContain("output pipe status changed");
				expect(await execute(later)).toBe(mode === "stale" ? "after\n" : "before\n");
				expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() })).toBe(Number(!["persistent", "stale"].includes(mode)));
			}, false));
	});

nativeTest.for((["git", "overlayfs"] as const).flatMap(driver => ["read", "write", "locks", "locks-close", "locks-close-queues", "locks-close-nested", "locks-named", "path-write", "unlinked", "hardlink-read", "hardlink-write", "hardlink-path-write", "hardlink-rename", "hardlink-split", "hardlink-unlink", "hardlink-detached"].map(mode => [driver, mode] as const)))("learns and adopts regular OFDs with shared positions and predecessor inputs (%s, %s)", { timeout: 30_000 }, async ([driver, mode], { skip, benchmark }) => {
		if (mode === "locks-named" && await filesystem.readlink("/proc/self/ns/pid") === "pid:[4026531836]") return skip("nested PID namespace visibility case");
		const fixture = await benchmark("pi-fd-binding-", driver);
		const hardlink = mode.startsWith("hardlink"), pathWrite = mode.endsWith("path-write"), locks = mode.startsWith("locks");
		const memory = locks && mode !== "locks-named", closing = mode.startsWith("locks-close");
		const writable = !pathWrite && mode !== "read" && mode !== "hardlink-read";
		const input = path.join(fixture.workspace, "input.txt");
		const reset = async () => {
			if (hardlink) for (const name of ["input.txt", "alias.txt", "moved.txt"]) await rm(path.join(fixture.workspace, name), { force: true });
			await writeFile(input, "abcdef");
			if (hardlink) await filesystem.link(input, path.join(fixture.workspace, "alias.txt"));
		};
		await reset();
		const lockHelpers = `#define _GNU_SOURCE
${cHeaders("errno.h sys/file.h fcntl.h")}
static int lock(int fd,int type,int start,int length) { struct flock range={.l_type=type,.l_whence=SEEK_SET,.l_start=start,.l_len=length};return fcntl(fd,F_OFD_SETLK,&range); }
static int query(int fd,int start,int length,int type,int foundStart,int foundLength) {
	struct flock range={.l_type=F_WRLCK,.l_whence=SEEK_SET,.l_start=start,.l_len=length};
	return fcntl(fd,F_OFD_GETLK,&range) || range.l_type!=type || range.l_start!=foundStart || range.l_len!=foundLength || (type!=F_UNLCK && range.l_pid!=-1);
}
`;
		if (locks) {
			await fixture.compile("locker", { code: lockHelpers + `int main(int argc,char **argv) {
	if(argc!=2)return 70;
	if(argv[1][0]=='i')return lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB);
	return query(8,2,4,F_UNLCK,2,4) || query(8,10,3,F_RDLCK,10,3) || flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN;
}` });
		}
		await fixture.compile("worker", { code: (locks ? lockHelpers : "") + `${cHeaders("fcntl.h")}
${processChecks}
int main(void) {
	char bytes[6] = {0, 0, 0, ':', 0, '\\n'};
	if (read(3, bytes, 2) != 2 || read(4, bytes + 2, 1) != 1 || read(8, bytes + 4, 1) != 1) return 71;
	${locks ? `if(query(8,0,1,F_UNLCK,0,1) || query(8,2,1,F_WRLCK,2,4) || lock(4,F_UNLCK,3,1) || query(8,3,1,F_UNLCK,3,1))return 74;
	if(lock(8,F_WRLCK,2,1)!=-1 || errno!=EAGAIN || lock(8,F_WRLCK,3,1) || lock(3,F_RDLCK,3,1)!=-1 || errno!=EAGAIN || lock(8,F_UNLCK,0,0) || lock(4,F_WRLCK,3,1))return 75;
	if(flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN || flock(4,LOCK_UN) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN) || flock(3,LOCK_EX|LOCK_NB) || lock(4,F_UNLCK,0,0) || lock(4,F_RDLCK,10,3))return 76;` : ""}
	${mode === "hardlink-rename" ? 'if (rename("input.txt", "moved.txt")) return 73;' : mode === "hardlink-unlink" ? 'if (unlink("input.txt")) return 73;' : mode === "hardlink-split" ? 'if (unlink("input.txt")) return 73; int replacement = open("input.txt", O_WRONLY | O_CREAT, 0600); if (replacement < 0 || write(replacement, "new", 3) != 3) return 73; close(replacement);' : ""}
	${mode === "hardlink-detached" ? 'if (unlink("input.txt") || unlink("alias.txt")) return 73;' : ""}
	${writable ? 'if (write(4, "XY", 2) != 2) return 72;' : pathWrite ? `int fd = open("${hardlink ? "alias.txt" : "input.txt"}", O_WRONLY); if (pwrite(fd, "Z", 1, 2) != 1) return 72; close(fd);` : ""}
	${closing ? `int gate[2];if(pipe2(gate,O_CLOEXEC))return 77;pid_t child=fork();if(child<0)return 77;
	if(!child){if(wait_gate(gate))_exit(78);close(3);close(4);_exit(0);}
	close(gate[0]);if(close(3) || close(4) || query(8,10,3,F_RDLCK,10,3) || flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN)return 77;
	if(write(gate[1],"x",1)!=1 || close(gate[1]))return 77;
	if(wait_child(child) || query(8,10,3,F_UNLCK,10,3) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN))return 77;` : ""}
	burn_cpu(50000000ul);
	return write(1, bytes, sizeof(bytes)) != sizeof(bytes);
}
` });
		const body = `IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; locker init; printf '%s\\n' "$1"; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"; locker check`;
		if (memory) {
			await fixture.compile("fd-memory", { code: lockHelpers + `
${cHeaders("sys/mman.h stdio.h")}
${rightsHelpers}
int main(int argc,char **argv) {
	char bytes[6];int input=open("input.txt",O_RDONLY);if(argc!=2 || input<0 || read(input,bytes,6)!=6)return 80;close(input);
	${mode === "locks-close-queues" ? 'int queues[2]; struct iovec vector = {bytes, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1}; if (socketpair(AF_UNIX, SOCK_STREAM, 0, queues) || sendmsg(queues[0], &message, 0) != 1 || close(queues[0]) || close(queues[1])) return 88;' : ""}
	int fd=memfd_create("lock-input",0);if(fd<0 || write(fd,bytes,6)!=6 || lseek(fd,0,SEEK_SET)!=0 || move_fd(fd,3))return 81;
	if(dup2(3,4)<0)return 82;fd=open("/proc/self/fd/3",O_RDWR);if(move_fd(fd,8))return 83;
	${mode === "locks-close-nested" ? 'if (wrap_queue(wrap_queue(memfd_create("unrelated", MFD_CLOEXEC), 0), 0) < 0) return 88;' : ""}
	${closing ? `int gate[2];char byte;if(pipe2(gate,O_CLOEXEC) || lseek(3,1,SEEK_SET)!=1 || lseek(8,1,SEEK_SET)!=1 || lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB))return 84;
	puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 85;
	if(!child){if(wait_gate(gate))_exit(86);execl("./worker","worker",(char *)0);_exit(86);}
	close(gate[0]);close(3);close(4);if(write(gate[1],"x",1)!=1)return 86;close(gate[1]);
	if(wait_child(child) || read(8,&byte,1)!=1 || query(8,10,3,F_UNLCK,10,3) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN))return 87;
	printf("tail::%c\\n",byte);return 0;` : `execl("/bin/bash","bash","-c",${JSON.stringify(body)},"fd-memory",argv[1],(char *)0);return 84;`}
}` });
		}
		await fixture.commit("Inherited FD binding");
		const scope = { sessionID: "fd-binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
		const command = memory ? "fd-memory parent" : `exec 3<${writable ? ">" : ""}input.txt; exec 4<&3; exec 8<${locks ? ">" : ""}${hardlink ? "alias.txt" : "input.txt"}; IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; ` +
			(mode === "unlinked" ? "rm input.txt; " : "") +
			(locks ? "locker init; " : "") + `printf 'parent\\n'; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"` + (locks ? "; locker check" : "");
		const tail = writable ? ":c" : pathWrite ? "e:Z" : "e:c";
		const { execute, binding, invocation, seed } = await learnWorker(fixture, scope, command);
		expect(seed).toBe(`parent\nbcd:b\ntail:${tail}\n`);
		for (const changed of [false, true]) {
			await reset();
			const branch = await fixture.workspaceSandbox.fork({ cwd: fixture.workspace, driver, liveLower: true,
				action: buildPiActionKey("bash", { command }, fixture.workspace)!, execute: async workspace => {
				await withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: later }, binding!, "bcd:b\n", async session => {
					const hits = fixture.backend.actorMetrics().hits;
					if (changed) await writeFile(input, "uvwxyz");
					expect(await execute(later, command.replace("parent", "changed-parent"))).toBe(changed
						? `changed-parent\nvwx:v\ntail:${writable ? ":w" : pathWrite ? "y:Z" : "y:w"}\n` : `changed-parent\nbcd:b\ntail:${tail}\n`);
					if (mode === "hardlink-detached") { expect(existsSync(input)).toBe(false); expect(existsSync(path.join(fixture.workspace, "alias.txt"))).toBe(false); }
					else if (hardlink) {
						const alias = path.join(fixture.workspace, "alias.txt");
						expect(await readText(alias)).toBe(writable ? `${changed ? "uvwx" : "abcd"}XY` : pathWrite ? `${changed ? "uv" : "ab"}Z${changed ? "xyz" : "def"}` : changed ? "uvwxyz" : "abcdef");
						if (mode === "hardlink-rename" || mode === "hardlink-unlink") expect(existsSync(input)).toBe(false);
						if (mode === "hardlink-rename") expect((await stat(path.join(fixture.workspace, "moved.txt"))).ino).toBe((await stat(alias)).ino);
						if (mode === "hardlink-split") expect(await readText(input)).toBe("new");
					}
					expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() }))
						.toBe(hits + Number(!changed && mode !== "locks-named"));
				});
				return { output: { result: { content: [], details: {} }, isError: false }, changes: [] };
			} });
			await branch.dispose();
		}
	});

nativeTest.for(["SOCK_DGRAM", "SOCK_SEQPACKET", "SOCK_DGRAM:close", "SOCK_SEQPACKET:close"])("adopts packet boundaries, empty messages, truncation and queued OFDs (%s)", { timeout: 30_000 }, async (scenario, { benchmark }) => {
		const [type, closing] = scenario.split(":");
		const fixture = await benchmark("pi-packet-binding-");
		await fixture.write("input.txt", "abcdef");
		const helpers = `#define _GNU_SOURCE
${cHeaders("poll.h errno.h stdio.h")}
${rightsHelpers}
static int passPacket(const char *bytes, int length) { int fd = 9; return pass(20, bytes, length, &fd, 1, MSG_NOSIGNAL); }
static int takePacket(int length,int peek,int truncated,char expected) {
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
		await fixture.compile("worker", { code: helpers + `int main(void) {
	char bytes[8];struct pollfd ready={.fd=22,.events=POLLIN};
	if(passPacket("",0) || read(22,bytes,0)!=0 || poll(&ready,1,0)!=1 || takePacket(0,1,0,0) || takePacket(0,0,0,'a'))return 70;
	if(write(21,"abcdef",6)!=6 || read_text(22, bytes, "ab"))return 71;
	if(send(20,"XY",2,0)!=2 || recv(22,bytes,8,0)!=2 || memcmp(bytes,"XY",2))return 72;
	struct iovec vector={bytes,0};struct msghdr message={.msg_iov=&vector,.msg_iovlen=1};
	if(send(20,"truncate",8,0)!=8 || recv(22,bytes,2,MSG_PEEK|MSG_TRUNC)!=8 || memcmp(bytes,"tr",2) || recvmsg(22,&message,MSG_TRUNC)!=8 || !(message.msg_flags&MSG_TRUNC))return 74;
	if(passPacket("pqrs",4) || takePacket(2,0,1,'b') || recv(22,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN || passPacket("tail",4))return 73;
	if(shutdown(20,SHUT_WR) || send(20,"x",1,MSG_NOSIGNAL)!=-1 || errno!=EPIPE || send(22,"back",4,0)!=4 || recv(20,bytes,8,0)!=4 || memcmp(bytes,"back",4) || shutdown(20,SHUT_RD))return 75;
	if(${type === "SOCK_DGRAM" ? "recv(20,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(20,bytes,1,0)!=0"})return 75;
	${closing ? "if(close(20) || close(21))return 76;" : ""}
	ready.revents=0;if(poll(&ready,1,0)!=1 || ready.revents!=${type === "SOCK_DGRAM" ? "POLLIN" : "(POLLIN|POLLHUP)"})return 77;
	burn_cpu(200000000ul);
	return write(1,"packets\\n",8)!=8;
}
` });
		await fixture.compile("fd-launch", { code: helpers + `int main(int argc,char **argv) {
	if(argc!=2)return 80;int pair[2],gate[2];char byte;
	if(socketpair(AF_UNIX,${type}|SOCK_CLOEXEC|SOCK_NONBLOCK,0,pair) || dup2(pair[0],20)<0 || dup2(pair[0],21)<0 || dup2(pair[1],22)<0)return 81;
	close(pair[0]);close(pair[1]);int fd=open("input.txt",O_RDONLY);if(move_fd(fd,9))return 82;
	if(pipe2(gate,O_CLOEXEC))return 83;puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 83;
	if(!child){if(wait_gate(gate))_exit(84);execl("./worker","worker",(char *)0);_exit(84);}
	close(gate[0]);${closing ? "close(20);close(21);" : ""}if(write(gate[1],"x",1)!=1)return 84;close(gate[1]);
	if(wait_child(child) || lseek(9,0,SEEK_CUR)!=2 || takePacket(4,0,0,'c') || lseek(9,0,SEEK_CUR)!=3)return 85;
	if(${type === "SOCK_DGRAM" ? "recv(22,&byte,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(22,&byte,1,0)!=0"})return 86;
	return 0;
}
` });
		await fixture.commit("Packet resource graph");
		const scope = { sessionID: "packet-binding", turnID: "seed" }, later = { ...scope, turnID: "prepared" };
		const command = "fd-launch parent";
		const { execute, binding, invocation, seed } = await learnWorker(fixture, scope, command);
		expect(seed).toBe("parent\npackets\n");
		expect(fixture.backend.actorMetrics().bypasses, JSON.stringify(fixture.backend.actorMetrics())).toBe(0);
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
			withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: later }, binding!, "packets\n", async session => {
				expect(await execute(later, command.replace("parent", "changed-parent"))).toBe("changed-parent\npackets\n");
				expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: session.metrics() })).toBe(1);
			}));
	});

	nativeTest("owns the entire Actor call when a held child crosses the adoption boundary", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-held-transaction-"));
		const binary = path.join(root, "helper");
		for (const suffix of ["", ".node"]) await compileBenchmarkHelper(root, "helper" + suffix, { source: fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), arguments: ["-pthread", "-Werror", ...(suffix ? ["-shared", "-fPIC"] : [])] });
		await filesystem.symlink((await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC)) + ".sqlite", binary + ".sqlite");
		const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary });
		await using _cleanup = onExit(async () => { await boundary.close(); await rm(root, { recursive: true, force: true }); });
		const actual = await vi.importActual<typeof childProcess>("node:child_process"), original = Reflect.get(actual.ChildProcess.prototype, "spawn");
		const observations: HeldExecProcess[] = [], completions: (number | undefined)[] = [];
		const collect = (child: childProcess.ChildProcess) => {
			let stdout = "", stderr = "", error: NodeJS.ErrnoException | undefined;
			child.stdout?.on("data", data => { stdout += data; }); child.stderr?.on("data", data => { stderr += data; });
			child.once("error", value => { error = value; });
			return new Promise<{ stdout: string; stderr: string; error?: NodeJS.ErrnoException; code: number | null; signal: NodeJS.Signals | null }>(resolve =>
				child.once("close", (code, signal) => resolve({ stdout, stderr, error, code, signal })));
		};
		const observe = <Value>(callID: string, execute: () => Promise<Value>, signal?: AbortSignal, descriptors?: boolean | "inspect") => boundary.observe({ sourceRoot: root, signal, descriptors,
			scope: { sessionID: "raw", turnID: "round", callID }, decide: async held => {
				observations.push(held); await nextTurn(); return { kind: "continue", observeCompletion: duration => { completions.push(duration); } };
			} }, execute);
		const code = `console.log(JSON.stringify({pid:process.pid,ppid:process.ppid,argv0:process.argv0,uid:process.getuid(),gid:process.getgid(),env:process.env.PI_SPEC_HELD_EXEC_ID,
				raw:require('fs').readFileSync('/proc/self/environ').includes('PI_SPEC_HELD_EXEC_ID=caller-owned')}));console.error('stderr');`;
		const credentials = { uid: process.getuid!(), gid: process.getgid!() }, launches = [[], ["uid"], ["gid"], ["uid", "gid"]] as const;
		await writeFile(path.join(root, "no-permission"), "x", { mode: 0o600 });
		// Native observation has no workspace commit to lock and must not initialize the entire SDK in its worker.
		const argvLength = process.execArgv.length; process.execArgv.push("--import", `data:text/javascript,import {registerHooks} from 'node:module'; registerHooks({resolve(name,context,next){if(name==='@earendil-works/pi-coding-agent')throw new Error('unexpected SDK initialization');return next(name,context);}});`);
		await using _observerImports = onExit(async () => { process.execArgv.splice(argvLength); });
		await using synchronous = disposable(new LinuxProcessReuseBackend({ storeRoot: path.join(root, "sync-store"), heldExecBinary: binary }));
		const syncScope = { sessionID: "raw", turnID: "synchronous" }, learned: ProcessExecutionBinding[] = [];
		const observeSync = <Value>(execute: () => Value) => synchronous.observeBindings(syncScope,
			() => synchronous.observeNative(root, syncScope, undefined, async () => execute()), bindings => { learned.push(...bindings); }, true);
		for (const sync of [false, true]) await Promise.all(launches.map(async fields => {
			const callID = `credentials:${fields}`, options = { ...Object.fromEntries(fields.map(field => [field, credentials[field]])), argv0: callID, env: { PI_SPEC_HELD_EXEC_ID: "caller-owned" } };
			const result = sync ? await observeSync(() => { const child = childProcess.spawnSync(process.execPath, ["-e", code], { ...options, encoding: "utf8" }); return { ...child, code: child.status }; }) :
				await observe(callID, async () => { await nextTurn(); const child = childProcess.spawn(process.execPath, ["-e", code], options);
					expect(child).toBeInstanceOf(actual.ChildProcess); return { ...await collect(child), pid: child.pid }; });
			expect(JSON.parse(result.stdout)).toEqual({ ...credentials, pid: result.pid, ppid: process.pid, argv0: callID, env: "caller-owned", raw: true });
			expect({ ...result, error: result.error }).toMatchObject({ code: 0, signal: null, stderr: "stderr\n", error: undefined });
		}));
		expect(observations.map(held => held.scope!.callID).sort()).toEqual(launches.map(fields => `credentials:${fields}`).sort());
		expect(new Set(observations.map(held => held.id.split(":")[0])).size).toBe(4);
		const options = { cwd: root, encoding: "utf8" as const, stdio: ["pipe", "pipe", "pipe"] as ["pipe", "pipe", "pipe"] };
		const outcome = (execute: () => unknown) => {
			let value: unknown; try { value = execute(); } catch (error) { value = error; }
			if (typeof value === "string" || Buffer.isBuffer(value)) return value;
			const result = value as childProcess.SpawnSyncReturns<string> & NodeJS.ErrnoException;
			return { status: result.status, signal: result.signal, code: (result.error as NodeJS.ErrnoException | undefined)?.code ?? result.code,
				stdout: result.stdout, stderr: result.stderr, syscall: (result.error as NodeJS.ErrnoException | undefined)?.syscall ?? result.syscall };
		};
		const syncCalls = ["printf out; printf err >&2; exit 17", "cat", "kill -TERM $$", "sleep 1", "printf '%4096s' ''; exec /bin/sleep 1"].flatMap((command, index) => {
			const configured = { ...options, ...(index === 1 ? { input: "stdin\n" } : index === 3 ? { timeout: 10 } : index === 4 ? { maxBuffer: 32 } : {}) };
			return [() => childProcess.spawnSync("/bin/sh", ["-c", command], configured), () => childProcess.execFileSync("/bin/sh", ["-c", command], configured),
				() => childProcess.execSync(command, configured)];
		});
		for (const name of ["missing", "no-permission"]) syncCalls.push(() => childProcess.spawnSync(path.join(root, name), [], options), () => childProcess.execFileSync(path.join(root, name), [], options));
		const expected = syncCalls.map(outcome), bindingBefore = Reflect.get(process, "binding")("spawn_sync").spawn;
		await observeSync(() => {
			let interleaved = false; const timer = setTimeout(() => { interleaved = true; }, 0); queueMicrotask(() => { interleaved = true; });
			try { for (const [index, call] of syncCalls.entries()) { expect(outcome(call), `synchronous case ${index}`).toEqual(expected[index]); expect(interleaved).toBe(false); } }
			finally { clearTimeout(timer); }
		});
		expect(learned.length).toBeGreaterThan(0); expect(Reflect.get(process, "binding")("spawn_sync").spawn).toBe(bindingBefore);
		learned.length = 0;
		await observeSync(async () => {
			expect(childProcess.execFileSync("/bin/echo", ["before"], options)).toBe("before\n");
			expect((await collect(childProcess.spawn("/bin/echo", ["middle"], options))).stdout).toBe("middle\n");
			expect(childProcess.execFileSync("/bin/echo", ["after"], options)).toBe("after\n");
		});
		const registry = Reflect.get(synchronous, "handoffs") as ProcessHandoffRegistry<{ args: readonly string[] }>;
		expect(learned.map(binding => registry.resolveBinding(binding, syncScope)?.args)).toEqual([["before"], ["middle"], ["after"]]);
		await Reflect.get(Reflect.get(synchronous, "synchronous"), "connection").worker.terminate();
		const requests = synchronous.actorMetrics().requests;
		for (const restarted of [false, true]) {
			expect(await observeSync(() => childProcess.execFileSync("/bin/echo", ["alive"], options))).toBe("alive\n");
			expect(synchronous.actorMetrics().requests > requests).toBe(restarted);
		}
		for (const [name, expected] of [["missing", "ENOENT"], ["no-permission", "EACCES"]]) await observe(name!, async () => {
			const command = path.join(root, name!), child = childProcess.spawn(command);
			expect(await collect(child)).toMatchObject({ error: { code: expected, syscall: `spawn ${command}` } }); expect(child.pid).toBeUndefined();
		});
		for (const shell of [false, true]) expect(await observe(`exec-${shell}`, () => new Promise(resolve => {
			const callback = (error: childProcess.ExecFileException | null, stdout: string, stderr: string) => resolve({ code: error?.code, stdout, stderr });
			if (shell) childProcess.exec("printf file; printf err >&2; exit 17", callback);
			else childProcess.execFile("/bin/sh", ["-c", "printf file; printf err >&2; exit 17"], callback);
		}))).toEqual({ code: 17, stdout: "file", stderr: "err" });
		await writeFile(path.join(root, "ipc.cjs"), "process.on('message',value=>{process.send({value,pid:process.pid,ppid:process.ppid});process.disconnect();});");
		await observe("ipc", async () => {
			const child = childProcess.fork(path.join(root, "ipc.cjs"), [], { silent: true }), closed = collect(child), received = once(child, "message");
			child.send("hello"); expect((await received)[0]).toEqual({ value: "hello", pid: child.pid, ppid: process.pid }); expect((await closed).code).toBe(0);
		});
		const cancelled = new AbortController();
		const cancelledRun = observe("cancelled", () => collect(childProcess.spawn("/bin/sleep", ["10"], { signal: cancelled.signal })), cancelled.signal);
		const surviving = observe("surviving", () => collect(childProcess.spawn("/bin/echo", ["survived"])));
		cancelled.abort();
		expect(await cancelledRun).toMatchObject({ signal: "SIGTERM", error: { code: "ABORT_ERR" } }); expect((await surviving).stdout).toBe("survived\n");
		await compileBenchmarkHelper(root, "lifetime", { code: `${processChecks}
int main(int n,char **v) { if(n!=2)return 90; pid_t child=!strcmp(v[1],"vfork")?vfork():fork(); if(child<0)return 91;
	if(!child) { char byte; if(write(1,"ready",5)!=5 || read(0,&byte,1)!=1 || write(1,"done",4)!=4)_exit(92); _exit(0); }
	return wait_child(child) || write(1,"parent",6)!=6;
}` });
		for (const kind of ["fork", "vfork", "stopped"]) for (const descriptors of [undefined, "inspect", true] as const) for (const killed of [false, true]) {
			const before = observations.length, completedBefore = completions.length, background = gated(); let late!: () => Promise<unknown>, child!: childProcess.ChildProcess, finished!: ReturnType<typeof collect>;
			const pending = observe("background", async () => {
				child = childProcess.spawn(path.join(root, "lifetime"), [kind], { detached: true }); finished = collect(child); child.stdin!.on("error", () => {});
				late = AsyncResource.bind(() => collect(childProcess.spawn("/bin/true"))); await once(child.stdout!, "data");
				if (kind === "stopped") { process.kill(-child.pid!, "SIGSTOP"); await expect.poll(() => readText(`/proc/${child.pid}/status`), { timeout: 500 }).toMatch(/^State:\s+[Tt]/m); }
				if (killed) process.kill(observations.at(-1)!.tracerPid, "SIGKILL"); return { resource: child, output: "own" };
			}, undefined, descriptors);
			await using _child = onExit(async () => { if (child?.exitCode === null && child.signalCode === null) try { process.kill(-child.pid!, "SIGKILL"); } catch {} await finished; await pending; });
			expect(await waitForCompletion(pending, undefined, 1000), `${kind}/${descriptors}/${killed}`).toMatchObject({ status: "completed", value: { resource: child, output: "own" } });
			if (killed) { expect(await waitForCompletion(finished, undefined, 1000)).toMatchObject({ status: "completed", value: { signal: "SIGKILL", stdout: "ready" } }); continue; }
			expect(child.exitCode).toBeNull(); expect(child.signalCode).toBeNull(); expect(completions.slice(completedBefore)).toEqual([undefined]);
			const keeper = observe("keeper", () => background.wait()); await background.entered;
			await late(); expect(observations.length).toBe(before + 1); background.release(); await keeper;
			if (kind === "stopped") { expect(await readText(`/proc/${child.pid}/status`)).toMatch(/^State:\s+T/m); process.kill(-child.pid!, "SIGCONT"); }
			child.stdin!.end("go\n"); expect(await finished).toMatchObject({ code: 0, signal: null, stdout: "readydoneparent" }); expect(completions.slice(completedBefore)).toEqual([undefined]);
			await expect.poll(() => { try { process.kill(observations.at(-1)!.tracerPid, 0); return true; } catch { return false; } }).toBe(false);
		}
		expect(Reflect.get(actual.ChildProcess.prototype, "spawn")).toBe(original);
		const count = observations.length; await collect(childProcess.spawn("/bin/true")); expect(observations).toHaveLength(count);
		for (const stop of ["waiter", "preparing", "owner"]) {
			const gate = path.join(root, `probe-${stop}`), marker = gate + "-pid", helper = gate + "-helper", storeRoot = gate + "-store";
			await writeFile(helper, `#!/bin/sh\nif [ "$1" = "--attach-skip-code" ]; then echo $$ > '${marker}'; while [ ! -f '${gate}' ]; do sleep .01; done; fi\nexec '${Reflect.get(boundary, "observerBinary")}' "$@"\n`, { mode: 0o700 });
			for (const suffix of [".node", ".sqlite"]) await filesystem.symlink(binary + suffix, helper + suffix);
			const backend = new LinuxProcessReuseBackend({ storeRoot, heldExecBinary: helper, heldExecObserverBinary: helper }), host = adaptProcessToolOperations(createLocalBashOperations({ shellPath: "/bin/bash" }));
			const controller = new AbortController();
			const execute = async (signal?: AbortSignal) => { let output = "";
				try { await backend.observeNative(root, { sessionID: "preparing", turnID: stop }, signal, () => host.execute({ command: "printf ready", cwd: root, signal, environment: {}, onData: data => { output += data.toString(); } })); return output; }
				catch (error) { if (signal?.aborted) return "aborted"; throw error; }
			};
			try {
				await backend.store.put(PROCESS_CERTIFICATE); const warming = stop === "preparing" ? backend.refreshNativeObservation() : undefined, first = execute(controller.signal);
				await expect.poll(() => existsSync(marker)).toBe(true); const pid = Number(await readText(marker)), second = execute();
				if (stop !== "owner") {
					controller.abort(); expect(await waitForCompletion(first, undefined, 500)).toEqual({ status: "completed", value: "aborted" });
					expect((await waitForCompletion(second, undefined, 30)).status).toBe("deadline"); expect(() => process.kill(pid, 0)).not.toThrow();
					await writeFile(gate, ""); expect(await second).toBe("ready");
				} else {
					expect((await waitForCompletion(backend.resetNativeObservation(), undefined, 500)).status).toBe("completed");
					expect(await Promise.all([first, second])).toEqual(["ready", "ready"]); await writeFile(gate, "");
				}
				await warming; expect(() => process.kill(pid, 0)).toThrow(); await backend.refreshNativeObservation(); expect(backend.nativeDiagnostics().state).toBe("ready"); expect(await execute()).toBe("ready");
			} finally { await backend.dispose(); }
			expect((await filesystem.readdir(storeRoot)).filter(name => name.endsWith(".sock"))).toEqual([]);
		}
		const run = (command: string) => observe("transport", () => collect(childProcess.spawn("/bin/bash", ["-c", command], { cwd: root })));
		const passThrough = "printf out; printf err >&2; exit 7";
		const direct = childProcess.spawnSync("/bin/bash", ["-c", passThrough], { encoding: "utf8" });
		expect(await run(passThrough)).toMatchObject({ code: direct.status, signal: direct.signal, stdout: direct.stdout, stderr: direct.stderr });
		expect(childProcess.spawnSync("/bin/bash", ["-c", "grep '^TracerPid:' /proc/self/status"], { encoding: "utf8" }).stdout).toMatch(/\t0\n$/);
		expect((await run("grep '^TracerPid:' /proc/self/status")).stdout).not.toMatch(/\t0\n$/);
		expect(await run("(sleep 0.05; kill -CONT $$) & kill -STOP $$; printf resumed")).toMatchObject({ code: 0, stdout: "resumed" });
		const text = `bound-name\nliteral ' $ value\nprivate value\n${root}\nstdin\n`;
		for (const [route, stdout, stderr] of [["12", text, "stderr"], ["11", text + "stderr", ""], ["21", "stderr", text], ["22", "", text + "stderr"]]) {
			const result = childProcess.spawnSync(binary, ["--exec", route!, "bound-name", "/bin/sh", "-c",
				'IFS= read -r value; printf "%s\\n" "$0" "$1" "$BOUND_EXEC" "$PWD" "$value"; printf stderr >&2; exit 23',
				"bound-name", "literal ' $ value"], { encoding: "utf8", cwd: root,
				env: { ...process.env, BOUND_EXEC: "private value" }, input: "stdin\n" });
			expect([result.status, result.stdout, result.stderr]).toEqual([23, stdout, stderr]);
			expect(childProcess.spawnSync(binary, ["--exec", route!, "clean", binary, "--probe-clean-fds"]).status).toBe(0);
		}
		for (const args of [["--exec"], ...["13", "12:", "12:-1", "12:unknown", "12:2x", "12:18446744073709551616"].map(route => ["--exec", route, "invalid", "/bin/true"])])
			expect(childProcess.spawnSync(binary, args).status).toBe(64);
		expect(childProcess.spawnSync(binary, ["--exec", "12", "missing", path.join(root, "missing")]).status).toBe(127);
		for (const signal of ["TERM", "PIPE", "XFSZ"])
			expect(childProcess.spawnSync("/bin/bash", ["-c", `trap '' ${signal}; exec "$@"`, "outer", binary,
				"--exec", "12", "signals", "/bin/sh", "-c", `kill -${signal} $$; exit 99`]).signal).toBe(`SIG${signal}`);
		const permissions = new Error("socket permission failure"), retained = await filesystem.readdir(root);
		vi.mocked(filesystem.chmod).mockResolvedValueOnce().mockRejectedValueOnce(permissions);
		await expect(LinuxHeldExecBoundary.open({ storeRoot: root, binary })).rejects.toBe(permissions);
		expect(await filesystem.readdir(root)).toEqual(retained);
		const completed = path.join(root, "descendant-completed"), pidFile = path.join(root, "descendant-pid");
		const command = `/usr/bin/setsid /bin/sh -c 'echo $$ > ${pidFile}; sleep 1; echo done > ${completed}' </dev/null >/dev/null 2>&1 & exit 7`;
		expect(await waitForCompletion(run(command), undefined, 900), "the shell returns without its detached descendants").toMatchObject({ status: "completed", value: { code: 7 } });
		await expect.poll(() => readText(completed).catch(() => ""), { timeout: 5000 }).toBe("done\n"); // Released, not killed with the tracer.
		await rm(pidFile);
		for (const skipRoot of [false, true]) {
			let armed!: ReturnType<typeof collect>;
			await expect(boundary.observe({ sourceRoot: root, skipRoot, decide: async held => ({ kind: "replay", exitCode: 0, output: [],
				commit: async () => { process.kill(held.tracerPid, "SIGKILL"); await armed; throw new Error("observer died after arming"); },
			}) }, () => armed = collect(childProcess.spawn("/bin/sh", ["-c", `/bin/true; printf unsafe > '${root}/after-observer-death'`])))).rejects.toMatchObject({ disposition: "poisoned" });
			expect(await armed, "armed failure must terminate the whole tree").toMatchObject({ code: null, signal: "SIGKILL", stdout: "" });
			expect(existsSync(path.join(root, "after-observer-death"))).toBe(false);
		}
		const native = adaptProcessToolOperations(createLocalBashOperations({ shellPath: "/bin/bash" }));
		const held = (options: Pick<Parameters<LinuxHeldExecBoundary["observe"]>[0], "decide" | "descriptors">, executor = native) => {
			const wrapped = new ProcessExecutionCoordinator({ execute: request => boundary.observe({ sourceRoot: root, ...options,
				scope: request.scope, signal: request.signal, skipRoot: true }, () => executor.execute(request)) });
			let output = "";
			return {
				get output() { return output; },
				execute: (command: string, request: Pick<Parameters<typeof native.execute>[0], "signal" | "scope"> = {}) =>
					wrapped.execute({ command, cwd: root, environment: { PATH: "/usr/bin:/bin" }, timeout: 5,
						onData: data => { output += data.toString(); }, ...request }),
			};
		};
		// A vfork parent cannot stop until its child execs: a sibling's exec decision must not wait for it (cargo build scripts hung).
		await compileBenchmarkHelper(root, "spawner", { code: `${cHeaders("sys/syscall.h time.h")}
${processChecks}
int main(void) { struct timespec pause = {0, 300000000}; pid_t sibling = fork(), child;
	if (!sibling) { usleep(50000); execl("/bin/true", "true", (char *)0); _exit(1); }
	if (!(child = vfork())) { syscall(SYS_nanosleep, &pause, 0); execl("/bin/true", "true", (char *)0); _exit(1); }
	return wait_child(child) || wait_child(sibling); }\n` });
		expect(await held({ descriptors: true, decide: async () => ({ kind: "continue" }) }).execute("exec ./spawner")).toEqual({ exitCode: 0 });
		const compat = path.join(root, "compat32");
		await writeFile(`${compat}.s`, ".global _start\n_start: movl $1, %eax; movl $7, %ebx; int $0x80\n");
		childProcess.execFileSync("cc", ["-nostdlib", "-m32", "-static", `${compat}.s`, "-o", compat]);
		if (childProcess.spawnSync(compat).status === 7) for (const descriptors of [false, true]) {
			const decide = vi.fn(async () => ({ kind: "continue" as const }));
			const executor = held({ descriptors, decide });
			expect(await executor.execute(`exec '${compat}'`)).toEqual({ exitCode: 7 });
			expect(decide).not.toHaveBeenCalled();
		}
		for (const killed of [false, true]) {
			const waiting = deferred(), nativeDone = deferred();
			const timings: HeldExecTiming[] = [];
			let callbacks = 0, observed = 0, closed = 0, heldPid = 0;
			const concurrent = held({ decide: async process => {
				if (++callbacks === 1) { heldPid = process.pid; await waiting.promise; }
				return { kind: "continue", observeTiming: timing => { timings.push(timing); }, observeCompletion: async (durationMs, clock) => {
					expect(clock ? clock.completedAt - clock.startedAt : undefined).toBe(durationMs);
					await nextTurn(); closed++; if (durationMs !== undefined) observed++;
				} };
			} }, { execute: request => native.execute(request).finally(nativeDone.resolve) });
			const siblings = concurrent.execute(`/bin/true & while [[ ! -e start-second-${killed} ]]; do :; done; /bin/echo sibling; wait`);
			await using _cleanup = onExit(async () => { waiting.resolve(); await Promise.allSettled([siblings]); });
			await vi.waitFor(() => expect(callbacks).toBe(1));
			await writeFile(path.join(root, `start-second-${killed}`), "ready");
			await vi.waitFor(() => { expect(concurrent.output).toBe("sibling\n"); expect(observed).toBe(1); });
			expect(callbacks).toBe(2);
			if (killed) { process.kill(heldPid, "SIGKILL"); await nativeDone.promise; }
			waiting.resolve(); expect(await siblings).toEqual({ exitCode: 0 }); expect(observed).toBe(killed ? 1 : 2);
			expect(closed, "native return must drain both successful and interrupted observations").toBe(2);
			expect(timings).toHaveLength(2);
			for (const timing of timings) {
				expect(timing.barrier).toBe("none");
				expect(timing.completedAt).toBeGreaterThanOrEqual(timing.requestedAt);
				if (timing.native) expect(timing.completedAt).toBe(timing.native.startedAt);
			}
		}
		{
			const visited: string[] = [], completed: string[] = [];
			const chained = held({ decide: async ({ pid }) => {
				const image = `${pid}:${await filesystem.readlink(`/proc/${pid}/exe`)}`;
				visited.push(image);
				return { kind: "continue", observeCompletion: () => { completed.push(image); } };
			} });
			expect(await chained.execute("exec /bin/sh -c 'exec /bin/true'")).toEqual({ exitCode: 0 });
			expect(visited).toHaveLength(2);
			expect(completed.sort(), "every exec image must retain its completion owner through replacement").toEqual(visited.sort());
		}
		const deliveryCommit = vi.fn(() => writeFile(path.join(root, "producer-armed"), "ready"));
		const delivery = held({ descriptors: false, decide: async ({ pid }) =>
			await filesystem.readlink(`/proc/${pid}/exe`) === "/usr/bin/true"
				? { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.alloc(2 * 1024 * 1024, 97) }], commit: deliveryCommit }
				: { kind: "continue" } });
		expect(await delivery.execute("/bin/true | (while [[ ! -e producer-armed ]]; do :; done; /usr/bin/wc -c)")).toEqual({ exitCode: 0 });
		expect(delivery.output).toBe("2097152\n");
		expect(deliveryCommit).toHaveBeenCalledOnce();
		const gateway = new ToolExecutionGateway<never, never>([]), execIDs = new Set<string>();
		await Promise.all(([undefined, "recoverable", "poisoned", "killed"] as const).map(async disposition => {
			const after = path.join(root, `after-${disposition}`);
			const scope = { sessionID: "session", turnID: "original", callID: String(disposition) }, expected = { ...scope };
			const nativeDone = deferred();
			const adopted = vi.fn(() => { throw new Error("advisory feedback failed"); });
			const observeTiming = vi.fn<(timing: HeldExecTiming) => void>();
			let heldPid = 0;
			const commit = vi.fn(async () => {
				if (disposition === "killed") { process.kill(heldPid, "SIGKILL"); await nativeDone.promise; }
				else if (disposition) throw effectCommitFailure(new Error("injected commit failure"), disposition);
			});
			const decide = vi.fn(async (process: HeldExecProcess) => {
				expect(currentExecutionScope()).toEqual(expected);
				expect(process.id).toMatch(/^[a-f0-9]{48}:1$/); expect(process.sequence).toBe(1);
				execIDs.add(process.id);
				heldPid = process.pid; return { kind: "replay" as const, output: [], exitCode: 0, commit, adopted, observeTiming };
			});
			const executor = held({ decide }, { execute: request => native.execute(request).finally(nativeDone.resolve) });
			const run = gateway.executeAuthoritative({ tool: "custom", input: {}, scope }, async () => {
				await nextTurn(); return executor.execute(`/bin/true; printf continued > '${after}'`);
			}); scope.turnID = scope.callID = "later";
			if (disposition) {
				await expect(run).rejects.toMatchObject({ disposition: "poisoned" });
				await expect(stat(after)).rejects.toThrow();
			} else {
				expect(await run).toEqual({ exitCode: 0 });
				expect(await readText(after)).toBe("continued");
			}
			expect(commit).toHaveBeenCalledOnce();
			expect(adopted).toHaveBeenCalledTimes(disposition ? 0 : 1);
			expect(observeTiming).toHaveBeenCalledOnce();
			const timing = observeTiming.mock.calls[0]![0];
			expect(timing).toMatchObject({ barrier: "none", outcome: disposition ? "failed" : "adopted" });
			expect(timing.native).toBeUndefined();
			if (!disposition) {
				expect(timing.committedAt).toBeGreaterThanOrEqual(timing.requestedAt);
				expect(timing.completedAt).toBeGreaterThanOrEqual(timing.committedAt!);
			}
			expect(decide).toHaveBeenCalledOnce();
			expect(decide.mock.calls[0]![0].scope).toEqual(expected);
			expect(Object.isFrozen(decide.mock.calls[0]![0].scope)).toBe(true);
		}));
		expect(execIDs.size).toBe(4); await gateway.dispose();
		let closed = false;
		const gate = gated();
		const running = observe("empty", async () => {
			await gate.wait(); return { exitCode: 0 };
		});
		await gate.entered;
		const closing = boundary.close().then(() => { closed = true; });
		await using _running = onExit(async () => { gate.release(); await Promise.allSettled([closing, running]); });
		await nextTurn();
		expect(closed, "close must wait for the owned executor and concurrent callers").toBe(false);
		const concurrent = boundary.close().then(() => { expect(closed).toBe(true); });
		gate.release();
		await Promise.all([closing, concurrent, running]);
	});

	test("commits what it wrote outside the workspace on adoption, over the host state it replaced", async ({ benchmark }) => {
		const fixture = await benchmark("pi-private-writes-"), outside = await mkdtemp(path.join(os.tmpdir(), "pi-private-outside-"));
		await using _cleanup = onExit(async () => { await fixture.dispose(); await rm(outside, { recursive: true, force: true }); });
		for (const name of ["kept", "stay"]) await writeFile(path.join(outside, name), "old\n");
		const { executionFingerprint } = await fixture.prepare();
		const scratch = "t=$(mktemp -d) && echo t > $t/x && cat $t/x && rm -r $t", host = async (name: string) => readText(outside, name).catch(() => "absent");
		// Written, replaced and scratch paths in one command: only the net changes commit, and only on adoption, with the times
		// they were left. The directory holding them lists the host's entries with the branch's, and is the host's own.
		const branch = await fixture.fork(`mkdir ${outside}/d && echo private > ${outside}/d/f && echo new > ${outside}/kept && cat ${outside}/d/f && ` +
			`touch -d @981173106.123456789 ${outside}/d/f && echo $(ls -a ${outside}) $(stat -c %i ${outside}) && ${scratch}`);
		try {
			expect(textOutput(branch.output.result)).toBe(`private\n. .. d kept stay ${(await stat(outside)).ino}\nt\n`);
			expect([await host("d/f"), await host("kept")], "a private write must not reach the host before adoption").toEqual(["absent", "old\n"]);
			await branch.commit();
			expect([await host("d/f"), await host("kept"), (await stat(path.join(outside, "d/f"), { bigint: true })).mtimeNs]).toEqual(["private\n", "new\n", 981173106123456789n]);
		} finally { await branch.dispose?.(); }
		// A child that read another's scratch file saw the command's own doing: the command stands on the workspace as it found it.
		const scratched = await fixture.fork("mkdir w && echo t > w/x && cat w/x && rm -r w");
		try { const validation = await scratched.validate?.(); expect([textOutput(scratched.output.result), validation], JSON.stringify({ validation, metrics: fixture.backend.metrics() })).toMatchObject(["t\n", { status: "valid" }]); } finally { await scratched.dispose?.(); }
		// A host object someone changed meanwhile is no baseline: the Actor must run the command instead.
		const raced = await fixture.fork(`echo again > ${outside}/kept`);
		try {
			await writeFile(path.join(outside, "kept"), "raced\n");
			await expect(raced.commit()).rejects.toThrow();
			expect(await host("kept")).toBe("raced\n");
		} finally { await raced.dispose?.(); }
		// A successor that also stats a new object keeps its identity constraint; the Actor settles that object natively.
		for (const label of ["prepared", "fresh"]) {
			const before = fixture.backend.metrics(), command = `/bin/sh -c 'printf "g\\n" > "$1"' sh ${outside}/g && cat ${outside}/g`;
			await using child = disposable(await fixture.fork(command));
			expect([textOutput(child.output.result), await host("g"), fixture.backend.metrics().hits > before.hits], JSON.stringify(fixture.backend.metrics())).toEqual(["g\n", "absent", false]);
			const validation = await child.validate?.(); expect(validation, JSON.stringify({ validation, metrics: fixture.backend.metrics() })).toMatchObject({ status: "stale", cause: { detail: `${outside}/g` } });
			if (label === "fresh") expect([textOutput(await fixture.tool.execute("created-actor", { command })), await host("g")]).toEqual(["g\n", "g\n"]);
		}
		// An independently prepared native operation has the same external-write baseline as a whole tool.
		const args = { command: `/bin/sh -c 'printf "bound\\n" > "$1"' sh ${outside}/kept` };
		const invocation = fixture.invocation(args);
		const permission = PI_ACTION_SEMANTICS.buildKey("bash", args, fixture.workspace, "private-bound", { fingerprint: executionFingerprint, context: invocation })!;
		const scope = { sessionID: "private-bound", turnID: "native" }, route = await fixture.prepareActorReplay();
		let operation: ExecutionOperationBinding | undefined;
		await fixture.world.observeOperations!({ action: permission, scope, learn: true }, () => route.executor.execute({
			command: args.command, cwd: fixture.workspace, environment: fixture.environment, scope, onData: () => {},
		}), bindings => { operation = bindings.at(-1); });
		expect(await host("kept")).toBe("bound\n");
		expect(operation).toMatchObject({ preparation: "current_workspace", permissionHash: permission.hash });
		await writeFile(path.join(outside, "kept"), "old\n");
		const definition = PI_ACTION_SEMANTICS.definition(permission)!, epoch = `${definition.epoch}:operation:${operation!.backend}`;
		const action = buildActionKey({ ...permission, input: { operation: operation!.identity }, semanticsEpoch: epoch,
			semantics: { ...definition, epoch, projectors: [] }, executionContext: { ...invocation, operation: { binding: operation!, permission } } });
		const prepareBound = () => fixture.world.speculation.execute({ cwd: fixture.workspace, tool: fixture.tool, toolName: "bash", args, action,
			callID: "private-bound", signal: new AbortController().signal, executionScope: scope });
		const prepared = await prepareBound();
		try {
			expect(await host("kept")).toBe("old\n");
			expect(await prepared.validate?.()).toMatchObject({ status: "valid" });
		} finally { await prepared.dispose(); }
		await fixture.inOwner(async (state, target) => {
			const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { writeFile } = await state.load("node:fs/promises");
			const open = LinuxProcessReuseBackend.prototype.open;
			LinuxProcessReuseBackend.prototype.open = async function (input: unknown) {
				LinuxProcessReuseBackend.prototype.open = open;
				const session = await open.call(this, input);
				return { ...session, executeBinding: async (binding: unknown) => {
					const result = await session.executeBinding(binding); await writeFile(target, "raced\n"); return result;
				} };
			};
		}, path.join(outside, "kept"));
		await expect(prepareBound()).rejects.toThrow(/unrepresentable writes outside the workspace.*external_write/);
		expect(await host("kept")).toBe("raced\n");
	});

	test("settles a brokered child's inherited pipe as the child left its queue, executed and replayed", async ({ benchmark }) => {
		const fixture = await benchmark("pi-pipe-settle-");
		await fixture.compile("pipe-child", { code: "#include <unistd.h>\nint main(void) { char byte; return read(3, &byte, 1) != 1 || byte != 'a' || write(4, \"cX\", 2) != 2; }\n" });
		// Both ends reach the child; the parent reads what the queue holds once it exits.
		await fixture.compile("piper", { code: `${cHeaders("stdio.h")}
${processChecks}
int main(void) { int fds[2]; char queue[8]; if (pipe(fds) || fds[0] != 3 || write(4, "ab", 2) != 2) return 90; pid_t child = fork();
	if (!child) { execlp("pipe-child", "pipe-child", (char *)0); _exit(91); }
	if (wait_child(child)) return 92; close(4); ssize_t length = read(3, queue, sizeof(queue));
	return length < 0 || printf("%.*s\\n", (int)length, queue) < 0; }\n` });
		await fixture.prepare();
		for (const label of ["executed", "replayed"]) {
			const before = fixture.backend.metrics(); await using branch = disposable(await fixture.fork(`printf '${label}: '; ./piper`));
			const after = fixture.backend.metrics();
			expect({ text: textOutput(branch.output.result), bypasses: after.bypasses - before.bypasses, hits: after.hits > before.hits }, JSON.stringify(after))
				.toEqual({ text: `${label}: bcX\n`, bypasses: 0, hits: label === "replayed" });
		}
	});

	test("owns output and capture lifetimes, preserves concurrency and brokers an internal pipe", async ({ skip, benchmark }) => {
		const fixture = await benchmark("pi-process-concurrency-");
		await fixture.inOwner(async (state, root) => {
			const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { default: path } = await state.load("node:path");
			const { default: childProcess } = await state.load("node:child_process"), { existsSync } = await state.load("node:fs");
			const { readlink, open: openFile, rm, mkdtemp } = state.fs, spawn = childProcess.spawn, kill = process.kill.bind(process), open = LinuxProcessReuseBackend.prototype.open, createServer = state.net.createServer;
			state.captures = []; state.allocated = []; state.killed = []; state.traces = [];
			state.method(process, "kill", (...args: Parameters<typeof process.kill>) => { state.killed.push(args); return kill(...args); });
			state.method(LinuxProcessReuseBackend.prototype, "open", async function (this: unknown, input: any) {
				if (!state.recorded) {
					state.recorded = true; const begin = input.workspace.transactions.begin;
					state.method(input.workspace.transactions, "begin", async () => {
						const capture = await begin(), recorded = { finish: capture.finish, abort: capture.abort };
						state.method(recorded, "finish"); state.method(recorded, "abort"); state.captures.push(recorded); return recorded;
					});
				}
				const session = await open.call(this, { ...input, signal: AbortSignal.any([AbortSignal.timeout(8000),
					...(input.signal ? [input.signal] : []), ...(state.controller ? [state.controller.signal] : [])]) });
				state.closeSession = () => {
					const first = session.close(); state.ownedAtClose = existsSync(input.workspace.sandboxRoot);
					if (state.mode === "nested-seal") state.closeTasks.push(first, session.close());
					state.closing?.resolve(); return first;
				};
				return { ...session, close: state.closeSession };
			});
			state.method(state.net, "createServer", (...args: unknown[]) => {
				const server = createServer(...args);
				server.on("connection", (socket: any) => { const address = server.address();
					if (typeof address === "string" && /^(?:pi-)?broker-/.test(path.basename(address))) state.broker = { server, socket, path: address }; });
				return server;
			});
			state.spawned = state.method(childProcess, "spawn", (...args: any[]) => {
				const trace = JSON.stringify(args[1]).includes("/trace-"), top = trace && ++state.launches === 1, mode = state.mode;
				const child = spawn(...(top && mode === "spawn" ? [path.join(root, "missing-executable"), args[1], args[2]] : args));
				if (top && mode === "abort") child.once("spawn", () => state.controller.abort());
				if (top && mode === "session-close" || trace && !top && mode?.startsWith("nested-")) {
					state.child = child; state.childClosed = new Promise<void>(resolve => child.once("close", () => resolve()));
					if (mode === "nested-abort" || mode === "session-close") child.once("spawn", () => {
						kill(-child.pid!, "SIGSTOP");
						if (mode === "nested-abort") state.controller.abort(); else state.closeTasks.push(state.closeSession());
					});
				}
				return child;
			});
			state.method(state.fs, "mkdtemp", async (...args: unknown[]) => {
				const trace = path.basename(String(args[0])) === "trace-";
				if (trace && state.inject && ++state.traceAllocations === 2) {
					state.allocationFailed = true; throw Object.assign(new Error("injected trace allocation failure"), { code: "ENOSPC" });
				}
				const value = await mkdtemp(...args); state.allocated.push(value); if (trace) state.traces.push(value); return value;
			});
			state.method(state.fs, "readlink", (...args: unknown[]) => state.spawned.mock.calls.some(({ result: child }: any) => child?.pid && String(args[0]) === `/proc/${child.pid}/fd/1`)
				? Promise.resolve("pipe:[0]") : readlink(...args));
			state.method(state.fs, "open", (...args: unknown[]) => {
				if (state.mode === "nested-seal" && !state.traceRoot && String(args[0]).startsWith(state.traces[1] + "/process.")) {
					state.traceRoot = state.traces[1]; state.captureStarted.resolve(); return state.captureGate.promise.then(() => openFile(...args));
				}
				return openFile(...args);
			});
			state.method(state.fs, "rm", (...args: unknown[]) => {
				if (String(args[0]) === state.broker?.path) state.socketRemoved = true;
				const removed = rm(...args); if (String(args[0]) === state.traceRoot) void removed.then(state.captureCleanup.resolve, state.captureCleanup.resolve); return removed;
			});
			state.cleanupCapture = async () => {
				state.captureGate.resolve();
				if (state.child?.pid && state.child.exitCode === null && state.child.signalCode === null) try { kill(-state.child.pid, "SIGKILL"); } catch { /* Already reaped. */ }
				await state.childClosed; if (state.traceRoot) await state.captureCleanup.promise; await Promise.allSettled(state.closeTasks);
			};
		}, fixture.root);
		let branch: Awaited<ReturnType<typeof fixture.fork>> | undefined;
		await using _cleanup = onExit(async () => { await branch?.dispose(); });
		await fixture.write("barrier-worker", [
			"#!/bin/sh", "set -C",
			"if ( : > \"$1/slot\" ) 2>/dev/null; then self=one other=two; else self=two other=one; fi",
			": > \"$1/$self\"", "while [ ! -e \"$1/$other\" ]; do :; done",
		].join("\n"));
		const barrierWorker = path.join(fixture.workspace, "barrier-worker");
		await chmod(barrierWorker, 0o755);
		await filesystem.utimes(barrierWorker, Date.now() / 1000 + 3600, (await stat(barrierWorker)).mtime); // Keep concurrent readers' metadata stable.
		// Scripts keep their own path as $0 and resolve relative modules, directly or through a bypass's shadow.
		for (const [name, script] of [["redirect-worker", "#!/bin/sh\nprintf 'redirected %s\\n' \"$0\""], ["route-worker", "#!/bin/sh\nprintf 'out\\n'; printf 'err\\n' >&2"],
			["bin.js", "#!/usr/bin/env node\nconsole.log(require('./lib.js'));"], ["lib.js", "module.exports = 'lib';"]]) {
			await fixture.write(name, `${script}\n`);
			await chmod(path.join(fixture.workspace, name), 0o755);
		}
		await fixture.compile("fd-check", { code: "#include <fcntl.h>\nint main(void) {\n\tint mask = 0;\n\tfor (int fd = 0; fd < 3; fd++) if (fcntl(fd, F_GETFD) >= 0) mask |= 1 << fd;\n\treturn mask;\n}\n" });
		const streamProbes = ["", "0<&-", "1>&-", "2>&-", "0<&- 1>&- 2>&-", "3>&1"].map((redirection) =>
			"status=0; fd-check " + redirection + " || status=$?; printf 'fds:%s\\n' \"$status\"").join("; ");
		await fixture.prepare({ skip });
		await fixture.inOwner(state => { state.inject = true; state.traceAllocations = state.launches = 0; });
		branch = await fixture.fork("set -e; /bin/bash -c '(/bin/sleep 0.05; echo x > escaped.txt) 2>/dev/null & exit 0'; until [ -e escaped.txt ]; do :; done; /usr/bin/printf 'trace-root-fallback\\n'; mkdir barrier; barrier-worker barrier & first=$!; barrier-worker barrier & second=$!; wait \"$first\"; wait \"$second\"; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; printf '%s\\n' \"$line\"; }; " +
				"route-worker 2>/dev/null; route-worker 2>&1 1>/dev/null; ./bin.js; /usr/bin/printf 'file-fallback\\n' > redirected-file.txt; /usr/bin/cat < redirected-file.txt; printf 'pipe-fallback\\n' | /usr/bin/cat; " + streamProbes + "; printf '%32768s:end' ''");
		expect(branch.output.isError, JSON.stringify({ output: branch.output, metrics: fixture.backend.metrics() })).toBe(false);
		const text = branch.output.result.content[0];
		// Discarded streams route into /dev/null; the stderr kept by `2>&1 1>/dev/null` still reaches stdout.
		expect(text?.type === "text" && text.text).toBe(`trace-root-fallback\nredirected ${path.join(fixture.workspace, "redirect-worker")}\nout\nerr\nlib\nfile-fallback\npipe-fallback\nfds:7\nfds:6\nfds:5\nfds:3\nfds:0\nfds:7\n` + " ".repeat(32768) + ":end");
		const captured = await fixture.inOwner(async state => ({ allocationFailed: state.allocationFailed,
			aborts: state.captures[2].abort.mock.calls.length, finishes: state.captures[2].finish.mock.calls.length,
			next: await state.captures[3].finish.mock.calls[0].result }));
		expect({ allocationFailed: captured.allocationFailed, aborts: captured.aborts, finishes: captured.finishes, nextComplete: captured.next.complete }, captured.next.reason)
			.toEqual({ allocationFailed: true, aborts: 1, finishes: 0, nextComplete: true });
		expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(3);
		expect(branch.executionMetrics.reuse?.bypasses).toBe(4); // Capture failure and three unsupported stdio contexts; a pipeline writer is brokered.
		expect(branch.executionMetrics.reuse?.lastError).toBe("process_bypass:process_context_unsupported");
		// A failed preparation leaves the installed image in its enclosing trace, including all of its native effects.
		const validation = JSON.stringify(await branch.validate?.());
		expect([validation.includes("broker_bypass:"), validation.includes("injected trace allocation failure")], validation)
			.toEqual([false, false]);
		expect(existsSync(path.join(fixture.workspace, "escaped.txt")), "an orphan's write, adopted by the supervisor, stays in the sandbox").toBe(false);
		await branch.dispose();
		branch = undefined;
		for (const failure of ["spawn", "abort", "nested-abort", "nested-seal", "session-close"] as const) {
			await fixture.inOwner((state, mode) => {
				state.mode = mode; state.inject = false; state.launches = 0; state.traces = []; state.killed = []; state.closeTasks = [];
				state.child = state.childClosed = state.traceRoot = undefined; state.ownedAtClose = state.socketRemoved = false;
				state.controller = new AbortController();
				for (const name of ["closing", "captureStarted", "captureGate", "captureCleanup"]) state[name] = state.deferred();
			}, failure);
			const running = fixture.fork(failure === "nested-seal" ? "/bin/true" : failure === "spawn" || failure === "abort" ? "while :; do :; done" : "/bin/sleep 1");
			void running.catch(() => undefined);
			await using _cleanup = onExit(async () => {
				await fixture.inOwner(state => state.cleanupCapture()); await running.then(value => value.dispose(), () => undefined);
			});
			if (failure === "nested-abort" || failure === "session-close") {
				const closed = await fixture.inOwner(async state => {
					await state.closing.promise;
					return { pid: state.child?.pid, ownedAtClose: state.ownedAtClose,
						childCancelled: state.killed.some(([pid, signal]: [number, string]) => pid === -state.child.pid && signal === "SIGKILL") };
				});
				expect(closed.pid).toBeTypeOf("number");
				expect({ ownedAtClose: closed.ownedAtClose, childCancelled: closed.childCancelled }).toEqual({ ownedAtClose: true, childCancelled: true });
			}
			if (failure === "nested-seal") {
				const closed = await fixture.inOwner(async state => {
					await state.captureStarted.promise;
					const { server, socket } = state.broker, close = server.close.bind(server), stopped = state.deferred();
					state.method(server, "close", (callback: (error: unknown) => void) => close((error: unknown) => { callback?.(error); stopped.resolve(); }));
					socket.destroy(); state.controller.abort(); await stopped.promise;
					return { ownedAtClose: state.ownedAtClose, sharedClose: state.closeTasks[0] === state.closeTasks[1], socketRemoved: state.socketRemoved };
				});
				expect(closed).toEqual({ ownedAtClose: true, sharedClose: true, socketRemoved: false });
				await fixture.inOwner(state => state.captureGate.resolve());
			}
			await expect(running).rejects.toThrow(failure === "spawn" ? "missing-executable ENOENT" : "aborted");
		}
		for (const root of await fixture.inOwner(state => state.allocated as string[]))
			if (path.basename(root).startsWith("pi-process-output-")) await expect(stat(root)).rejects.toMatchObject({ code: "ENOENT" });

	}, 15_000);

	test.for(["trace", "transaction", "dependency", "input_image", "publication"] as const)("preserves output and capture ownership when %s fails", { timeout: 15_000 }, async (failure, { skip, benchmark }) => {
		const fixture = await benchmark("pi-process-capture-failure-", failure === "dependency" ? "git" : undefined);
		const error = failure === "dependency" ? "transaction baseline changed: input.txt" : `injected ${failure} capture failure`;
		await fixture.inOwner(async (state, failure, message) => {
			const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { ProcessReusePlanner } = await state.load("../src/reuse-planner.ts");
			const { default: childProcess } = await state.load("node:child_process"), { default: path } = await state.load("node:path");
			const { readFile, open: openFile, rm, mkdtemp } = state.fs, open = LinuxProcessReuseBackend.prototype.open, publish = ProcessReusePlanner.prototype.publishCompleted;
			const error = new Error(message), entered = state.deferred(), failed = state.failed = state.deferred(), gate = state.gate = state.deferred();
			state.spawn = state.method(childProcess, "spawn"); state.released = state.cleanupBeforeRelease = false; state.published = [];
			state.method(LinuxProcessReuseBackend.prototype, "open", async function (this: unknown, input: any) {
				state.processContext = { workspace: input.workspace.sandboxRoot, scope: input.scope };
				const begin = input.workspace.transactions.begin; let captures = 0;
				state.method(input.workspace.transactions, "begin", async () => {
					const capture = await begin();
					if (++captures <= 2 || failure === "publication" || failure === "input_image") return capture;
					return { abort: capture.abort, finish: async (...args: unknown[]) => {
						if (failure === "dependency") {
							const result = await capture.finish(...args), next = await begin();
							await state.fs.writeFile(path.join(input.workspace.sandboxRoot, "input.txt"), "changed-once");
							await next.finish(); failed.resolve(); await gate.promise; return result;
						}
						if (failure === "trace") { entered.resolve(); await gate.promise; return capture.finish(...args); }
						await entered.promise; await capture.abort(); failed.resolve(); throw error;
					} };
				});
				return open.call(this, input);
			});
			let traces = 0;
			state.method(state.fs, "mkdtemp", async (...args: unknown[]) => {
				const root = await mkdtemp(...args);
				if (path.basename(String(args[0])) === "trace-" && ++traces === 2) state.traceRoot = root;
				return root;
			});
			state.method(state.fs, "readFile", async (...args: unknown[]) => {
				if (failure === "input_image" && String(args[0]).endsWith("/proof.json")) { failed.resolve(); await gate.promise; throw error; }
				return readFile(...args);
			});
			state.method(state.fs, "open", async (...args: unknown[]) => {
				if (state.traceRoot && String(args[0]).startsWith(state.traceRoot + "/process.")) {
					if (["publication", "dependency", "input_image"].includes(failure)) return openFile(...args);
					if (failure === "trace") { await entered.promise; failed.resolve(); throw error; }
					entered.resolve(); await gate.promise;
				}
				return openFile(...args);
			});
			state.method(state.fs, "rm", (...args: unknown[]) => {
				if (String(args[0]) === state.traceRoot && !state.released) state.cleanupBeforeRelease = true;
				return rm(...args);
			});
			state.method(ProcessReusePlanner.prototype, "publishCompleted", async function (this: unknown, ...args: unknown[]) {
				state.published.push(args[0]);
				if (failure === "publication" && state.published.length === 1) { failed.resolve(); await gate.promise; throw error; }
				return publish.apply(this, args);
			});
		}, failure, error);
		let running: ReturnType<typeof fixture.fork> | undefined, returned = false;
		await using _cleanup = onExit(async () => {
			await fixture.inOwner(state => { state.released = true; state.gate.resolve(); });
			await running?.then(branch => branch.dispose(), () => undefined);
		});
		const inputName = failure === "input_image" ? path.join(fixture.root, "input.txt") : "input.txt";
		await writeFile(path.resolve(fixture.workspace, inputName), "capture-once");
		await fixture.compile("emit", { code: `#include <fcntl.h>\n#include <unistd.h>\nint main(void) { char text[12]; return read(open(${JSON.stringify(inputName)}, O_RDONLY), text, 12) != 12 || write(1, text, 12) != 12; }\n` });
		await fixture.commit("Process capture failure");
		await fixture.prepare({ skip });
		running = fixture.fork("emit");
		void running.then(() => { returned = true; }, () => { returned = true; });
		await Promise.race([fixture.inOwner(state => state.failed.promise), running.then(() => { throw new Error(`failure injection was not reached: ${JSON.stringify(fixture.backend.metrics())}`); })]);
		await nextTurn();
		expect({ cleanupBeforeRelease: await fixture.inOwner(state => state.cleanupBeforeRelease), returned }, "a failed capture must drain its pending sibling")
			.toEqual({ cleanupBeforeRelease: false, returned: false });
		await fixture.inOwner(state => { state.released = true; state.gate.resolve(); });
		const branch = await running, captured = await fixture.inOwner(state => ({ traceRoot: state.traceRoot, executions: state.spawn.mock.calls.filter(({ arguments: [, args] }: any) => JSON.stringify(args).includes(state.traceRoot)).length, context: state.processContext, certificate: state.published[0] }));
		await expect(stat(captured.traceRoot)).rejects.toMatchObject({ code: "ENOENT" });
		expect(branch.output.result.content).toEqual([fauxText("capture-once")]);
		expect({ executions: captured.executions, published: fixture.backend.metrics().published }).toEqual({ executions: 1, published: 0 });
		const lastError = branch.executionMetrics.reuse?.lastError ?? "";
		expect(lastError).toContain(error);
		const detail = failure === "input_image" ? undefined : JSON.parse(lastError.split("; process=")[1]!);
		if (failure !== "input_image") {
			expect(detail).toMatchObject({ ...captured.context, requestID: 1,
				stage: failure === "publication" ? "history_publication" : failure === "dependency" ? "dependencies" : `${failure}_capture` });
			expect(detail.weakKey).toMatch(/^sha256:[a-f0-9]{64}$/);
		}
		const validation = await branch.validate?.();
		if (failure === "publication") {
			await expect(validateTransferredProcessEvidence(captured.certificate.dependencyCertificate)).resolves.toMatchObject({ status: "valid" });
			expect(lastError).toContain(`nested_publish:${error}`);
			expect(detail).toMatchObject({ certificateID: captured.certificate.id, complete: true, taints: ["clock", "random"] });
			expect(validation).toMatchObject({ status: "valid" });
			expect((await fixture.backend.store.stats()).certificates).toBe(0);
		} else {
			expect(validation?.status).toBe("indeterminate");
			expect(JSON.stringify(validation)).toContain(`nested_capture:${error}`);
		}
	});

	test.for(["rename", "posix-lock", "ofd-lock", "flock", "external-posix-lock", "external-ofd-lock", "external-flock", "rdtsc", "auxv-random"] as const)("rejects reuse when isolated resource semantics differ from native (%s)", { timeout: 20_000 }, async (mode, { skip, benchmark }) => {
		const instanceInput = mode === "rdtsc" || mode === "auxv-random";
		if (instanceInput && process.arch !== "x64") return skip("x86-64 ELF input probe");
		if (mode === "rename") {
			const overlay = await linuxOverlayfsCapability();
			if (!overlay.available) return skip(overlay.detail);
		}
		const fixture = await benchmark("pi-process-driver-semantics-", mode === "rename" ? "overlayfs" : undefined);
		const { workspace, backend } = fixture;
		const external = mode.startsWith("external-"), controlRoot = external ? fixture.root : workspace, target = path.join(controlRoot, "source", "value.txt");
		let branch: Awaited<ReturnType<typeof fixture.fork>> | undefined;
		let locker: childProcess.ChildProcessWithoutNullStreams | undefined, lockerClosed: Promise<unknown> | undefined;
		await using _cleanup = onExit(async () => {
			locker?.stdin.end(); await lockerClosed;
			await branch?.dispose();
		});
		await mkdir(path.join(controlRoot, "source"));
		await writeFile(target, "value\n", "utf8");
		if (mode !== "rename") {
			await fixture.compile("probe", { code: instanceInput ? `${cHeaders("elf.h stdint.h stdio.h unistd.h")}
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
${cHeaders("fcntl.h sys/file.h unistd.h")}
int main(int argc, char **argv) {
	(void)argv; int fd = open(${JSON.stringify(external ? target : "source/value.txt")}, O_RDWR); if (fd < 0) return 2;
	if (argc > 2) return write(fd, "prepared\\n", 9) != 9 || ftruncate(fd, 9) ? 6 : 0;
	struct flock lock = {.l_type=F_WRLCK, .l_whence=SEEK_SET}; char value;
	if (argc == 2) {
		if (${mode.endsWith("flock") ? "flock(fd, LOCK_EX)" : "fcntl(fd, F_SETLK, &lock)"} < 0) return 3;
		if (write(1, "R", 1) != 1) return 4;
		return read(0, &value, 1) < 0;
	}
	${mode.endsWith("flock") ? "(void)lock; value = flock(fd, LOCK_EX|LOCK_NB) == 0 ? 'U' : 'L';" :
		`if (fcntl(fd, ${mode.endsWith("ofd-lock") ? "F_OFD_GETLK" : "F_GETLK"}, &lock) < 0) return 5; value = lock.l_type == F_UNLCK ? 'U' : 'L';`}
	return write(1, &value, 1) != 1;
}
` });
			await fixture.commit("Resource control observation");
			if (!instanceInput) {
				locker = childProcess.spawn(path.join(workspace, "probe"), ["hold"], { cwd: workspace, env: fixture.environment, stdio: ["pipe", "pipe", "pipe"] });
				lockerClosed = once(locker, "close");
				expect((await Promise.race([once(locker.stdout, "data"), lockerClosed.then(() => { throw new Error("lock holder exited"); })]))[0].toString()).toBe("R");
				expect(childProcess.execFileSync(path.join(workspace, "probe"), [], { cwd: workspace, env: fixture.environment, encoding: "utf8" })).toBe("L");
			}
		}
		await fixture.prepare({ includeWorkspaceFingerprint: true });
		const command = mode === "rename" ? "mv source moved && mkdir shut && chmod 600 shut" : `${external ? "probe prepare private && " : ""}probe`;
		branch = await fixture.fork(command);
		expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
		// Check before the Actor writes: the host's holder alone must invalidate the granted lock.
		if (!instanceInput) {
			const validation = await branch.validate?.();
			expect(validation?.status, JSON.stringify({ mode, validation, metrics: backend.metrics() })).toBe(mode === "rename" ? "indeterminate" : "stale");
			expect(JSON.stringify(validation)).toContain(mode === "rename" ? "filesystem_semantics" : "value.txt");
		}
		if (mode !== "rename") {
			expect(branch.output.result.content).toEqual([fauxText(instanceInput ? expect.stringMatching(/^\d+\n$/) : "U")]);
			const route = await fixture.prepareActorReplay(true);
			const { output } = await executeProcess(fixture, route.executor, `: changed-parent; ${command}`, { scope: { sessionID: "benchmark", turnID: "later" } });
			expect(output, JSON.stringify(backend.actorMetrics())).toEqual(instanceInput ? expect.stringMatching(/^\d+\n$/) : "L");
			expect(backend.actorMetrics().hits).toBe(0);
		}
		expect(branch.executionMetrics.reuse?.requests).toBeGreaterThan(0);
		expect(branch.executionMetrics.reuse?.executionMs).toBeGreaterThan(0);
		if (!mode.endsWith("lock")) {
			expect(backend.metrics().tainted).toBeGreaterThan(0);
			expect(backend.metrics().published).toBe(0);
			expect((await backend.store.stats()).certificates).toBe(0);
		}
		expect((await stat(path.join(controlRoot, "source"))).isDirectory()).toBe(true);
		await expect(stat(path.join(workspace, "moved"))).rejects.toThrow();
	});
});
nativeTest.for(["git", "auto"] as const)("preserves native metadata across stat families and rejects volatile or changed results (%s)", { timeout: 120_000 }, async (driver, { benchmark }) => {
	const fixture = await benchmark("pi-native-metadata-", driver, {}, os.homedir());
	const actual = await vi.importActual<typeof filesystem>("node:fs/promises"), pins: filesystem.FileHandle[] = [];
	const pinning = vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
		const handle = await actual.open(...args);
		if (typeof args[1] === "number" && args[1] & 0x200000) pins.push(handle);
		return handle;
	});
	await using _cleanup = onExit(async () => {
		try { expect(pins.every(handle => handle.fd === -1)).toBe(true); }
		finally { pinning.mockRestore(); await fixture.dispose(); }
	});
	const bin = path.join(fixture.root, "host-bin"); await mkdir(bin);
	await compileBenchmarkHelper(bin, "cat", { source: "pid.c", code: '#include <stdio.h>\n#include <unistd.h>\nint main(void) { printf("%ld\\n", (long)getpid()); return 0; }\n' });
	await fixture.write("README", "metadata fixture\n");
	await filesystem.chmod(path.join(fixture.workspace, "README"), 0o640);
	await filesystem.link(path.join(fixture.workspace, "README"), path.join(fixture.workspace, "README-alias"));
	await fixture.write(".gitignore", "dist/\n");
	await mkdir(path.join(fixture.workspace, "dist"));
	await fixture.write(path.join("dist", "module.cjs"), "module.exports = 42;\n");
	await mkdir(path.join(fixture.workspace, "many"));
	for (let index = 0; index < 300; index++) await mkdir(path.join(fixture.workspace, "many", `d${index}`));
	await fixture.write(path.join("many", "file"), "regular\n");
	await filesystem.symlink("d0", path.join(fixture.workspace, "many", "alias"));
	await fixture.compile("metadata", { code: String.raw`#define _GNU_SOURCE
${cHeaders("dirent.h fcntl.h stdio.h string.h sys/mman.h sys/stat.h sys/syscall.h sys/sysmacros.h unistd.h")}
static void emit(struct stat *s) { printf("%llu %llu %lld %o %llu %lld:%ld\n", (unsigned long long)s->st_dev, (unsigned long long)s->st_ino, (long long)s->st_size, s->st_mode, (unsigned long long)s->st_nlink, (long long)s->st_atim.tv_sec, s->st_atim.tv_nsec); }
int main(int argc, char **argv) {
	const char *name = argc > 1 ? argv[1] : "README";
	struct stat s; struct statx x;
	int fd = open(name, O_RDONLY);
	if (fd < 0) { perror("open"); return 1; }
	if (argc > 2 && !strcmp(argv[2], "legacy")) {
		char target[32] = {0};
		if (syscall(SYS_mkdir, "legacy-dir", 0750)) return 20;
		int output = syscall(SYS_open, "legacy-dir/input", O_CREAT|O_RDWR, 0640);
		if (output < 0 || write(output, "legacy", 6) != 6 || close(output)) return 21;
		if (syscall(SYS_link, "legacy-dir/input", "legacy-dir/alias") ||
			syscall(SYS_rename, "legacy-dir/input", "legacy-result") ||
			syscall(SYS_symlink, "../legacy-result", "legacy-dir/link")) return 22;
		if (syscall(SYS_readlink, "legacy-dir/link", target, sizeof(target)) != 16 || strcmp(target, "../legacy-result") ||
			syscall(SYS_access, "legacy-dir/link", R_OK) || syscall(SYS_chmod, "legacy-result", 0440) ||
			syscall(SYS_chown, "legacy-result", getuid(), getgid()) || syscall(SYS_lchown, "legacy-dir/link", getuid(), getgid())) return 23;
		if (syscall(SYS_unlink, "legacy-dir/link") || syscall(SYS_unlink, "legacy-dir/alias") || syscall(SYS_rmdir, "legacy-dir")) return 24;
		puts("legacy-ok"); return close(fd);
	}
	if (argc > 2 && !strcmp(argv[2], "enumerate")) {
		DIR *dir = opendir(name); struct dirent *entry; if (!dir) return 7;
		while ((entry = readdir(dir))) { struct stat child; if (fstatat(dirfd(dir), entry->d_name, &child, AT_SYMLINK_NOFOLLOW) || child.st_ino != entry->d_ino) return 8; }
		if (closedir(dir)) return 9;
		char *buffer = mmap(NULL, 8192, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		if (buffer == MAP_FAILED || mprotect(buffer+4096, 4096, PROT_NONE)) return 10;
		int empty = open("many/d1", O_RDONLY|O_DIRECTORY); if (empty < 0) return 11;
		for (int legacy = 0; legacy < 2; legacy++) {
			if (lseek(empty, 0, SEEK_SET) || syscall(legacy ? SYS_getdents : SYS_getdents64, empty, buffer+4096-128, 4096) <= 0) return 12;
		}
		if (close(empty) || munmap(buffer, 8192)) return 13;
	}
	char held[64];
	if (argc > 2 && !strcmp(argv[2], "proc")) { snprintf(held, sizeof(held), "/proc/self/fd/%d", fd); name = held; }
	if (syscall(SYS_stat, name, &s)) { perror("stat"); return 1; } emit(&s);
	if (syscall(SYS_lstat, name, &s)) return 2; emit(&s);
	if (syscall(SYS_newfstatat, AT_FDCWD, name, &s, AT_EMPTY_PATH)) return 3; emit(&s);
	if (syscall(SYS_newfstatat, fd, "", &s, AT_EMPTY_PATH)) return 4; emit(&s);
	if (syscall(SYS_fstat, fd, &s)) return 5; emit(&s);
	for (int mode = 0; mode < 3; mode++) {
		if (statx(mode == 1 ? fd : AT_FDCWD, mode == 1 ? "" : name, mode == 1 ? AT_EMPTY_PATH : mode == 2 ? AT_SYMLINK_NOFOLLOW : 0, STATX_BASIC_STATS, &x)) return 6;
		printf("%llu %llu %lld %o %u %lld:%u\n", (unsigned long long)makedev(x.stx_dev_major, x.stx_dev_minor), (unsigned long long)x.stx_ino, (long long)x.stx_size, x.stx_mode, x.stx_nlink, (long long)x.stx_atime.tv_sec, x.stx_atime.tv_nsec);
	}
	return close(fd);
}
` });
	await filesystem.symlink("README", path.join(fixture.workspace, "metadata-link"));
	await fixture.commit("metadata");
	const stamp = `${Math.floor(Date.now() / 1000) - 30} +0000`;
	childProcess.execFileSync("git", ["commit", "--amend", "--no-edit", "--date", stamp], { cwd: fixture.workspace, env: { ...process.env, GIT_COMMITTER_DATE: stamp } });
	await fixture.prepare();
	let turn = 0;
	// Positive metadata cases keep access times stable across both executions and their directory reads.
	for (const name of ["", ...await filesystem.readdir(fixture.workspace, { recursive: true })]) {
		const target = path.join(fixture.workspace, name), info = await filesystem.lstat(target);
		await filesystem.lutimes(target, Date.now() / 1000 + 3600, info.mtime);
	}
	const produce = (command: string) => fixture.fork(command, { executionScope: { sessionID: "metadata", turnID: String(turn++) } });
	const native = (command: string) => childProcess.execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: { ...fixture.environment }, encoding: "utf8" });
	const publication = vi.spyOn(filesystem, "writeFile").mockImplementation(async (...args) => {
		if (String(args[0]).endsWith("/metadata.json")) throw new Error("injected metadata publication failure");
		return actual.writeFile(...args);
	});
	try {
		await expect(produce("true")).rejects.toThrow("injected metadata publication failure");
		expect(pins.length).toBeGreaterThan(0);
		expect(pins.every(handle => handle.fd === -1)).toBe(true);
	} finally { publication.mockRestore(); }
	for (const mutation of ["", "mkdir many/new", "rmdir many/d0", "./metadata README legacy"] as const) {
		const command = `${mutation ? `${mutation}; ` : ""}./metadata many enumerate`, candidate = await produce(command);
		try {
			expect(candidate.output.isError, textOutput(candidate.output.result)).toBe(false);
			if (mutation.endsWith("legacy")) {
				expect(textOutput(candidate.output.result)).toContain("legacy-ok\n");
				await expect(filesystem.lstat(path.join(fixture.workspace, "legacy-result"))).rejects.toMatchObject({ code: "ENOENT" });
			}
			expect(textOutput(candidate.output.result)).toBe(native(command));
			if (!mutation) {
				expect(await candidate.validate?.()).toMatchObject({ status: "valid" });
				await mkdir(path.join(fixture.workspace, "many", "changed"));
				expect((await candidate.validate?.())?.status).toBe("stale");
				await rm(path.join(fixture.workspace, "many", "changed"), { recursive: true });
			}
		} finally { await candidate.dispose(); }
		if (mutation.startsWith("mkdir")) await rm(path.join(fixture.workspace, "many", "new"), { recursive: true });
		if (mutation.startsWith("rmdir")) await mkdir(path.join(fixture.workspace, "many", "d0"));
		if (mutation.endsWith("legacy")) { expect(await fixture.read("legacy-result")).toBe("legacy"); await rm(path.join(fixture.workspace, "legacy-result")); }
	}
	for (const [name, mode] of [["README", "proc"], ["many", "proc"], [".", ""], ["metadata-link", ""], ["dist/module.cjs", "module"], ["README", "replace"], [path.join(fixture.workspace, "README"), "atime"], [`${bin}/cat`, "atime"]]) {
		if (mode === "atime") await filesystem.utimes(name!, 0, 0);
		const command = mode === "module" ? `node -e "console.log(require('./${name}'))"` : `./metadata '${name}' ${mode === "proc" ? mode : ""}`;
		await using candidate = disposable(await produce(command));
		expect(candidate.output.isError, textOutput(candidate.output.result)).toBe(false);
		// A proc link names this caller's descriptor; only its followed metadata compares across processes.
		const output = (text: string) => mode === "proc" ? text.trimEnd().split("\n").filter((_, index) => index !== 1 && index !== 7) : text;
		expect(output(textOutput(candidate.output.result))).toEqual(output(native(command)));
		if (mode === "proc") continue;
		expect((await candidate.validate?.())?.status).toBe("valid");
		if (mode === "module") await fixture.write(name!, "module.exports = 43;\n");
		else if (mode === "atime") await filesystem.readFile(name!);
		else if (name === ".") { const info = await filesystem.stat(fixture.workspace); await filesystem.utimes(fixture.workspace, info.atime, new Date(info.mtimeMs - 2000)); }
		else if (mode === "replace") {
			childProcess.execFileSync("python3", ["-c", 'import os; p="README"; s=os.stat(p); b=open(p,"rb").read(); q=p+".swap"; open(q,"wb").write(b); os.chmod(q,s.st_mode & 0o7777); os.utime(q, ns=(s.st_atime_ns,s.st_mtime_ns)); os.replace(q,p)'], { cwd: fixture.workspace });
		} else continue;
		if (mode === "replace" || mode === "atime") expect(textOutput(candidate.output.result)).not.toBe(native(command));
		expect((await candidate.validate?.())?.status).toBe("stale");
	}
	for (const command of [`${bin}/cat`, "git log -1 --format=%cr", 'printf "%s\\n" constant', 'printf "%s\\n" "$RANDOM"',
		'printf "%s\\n" "$((RANDOM))"', 'v=RANDOM; printf "%s\\n" "${!v}"', 'declare -n v=RANDOM; printf "%s\\n" "$v"']) {
		for (const actor of [false, true]) {
			const metrics = () => actor ? fixture.backend.actorMetrics() : fixture.backend.metrics(), before = metrics().wholeCommandHits, outputs: string[] = [];
			for (let index = 0; index < 3; index++) {
				await using candidate = actor ? undefined : disposable(await produce(command));
				if (candidate) expect(candidate.output.isError).toBe(false);
				const output = textOutput(candidate ? candidate.output.result : await fixture.tool.execute(`actor-${index}`, { command })); outputs.push(output);
				if (command.includes("printf")) expect(output).toMatch(command.includes("constant") ? /^constant\n$/ : /^\d+\n$/);
				if (command.startsWith("git") && index === 0) await new Promise(resolve => setTimeout(resolve, 1500));
			}
			expect(metrics().wholeCommandHits - before, command).toBe(0);
			if (!command.includes("printf")) expect(outputs[0], JSON.stringify({ command, actor, outputs, metrics: metrics() })).not.toBe(outputs[1]);
		}
	}
});
