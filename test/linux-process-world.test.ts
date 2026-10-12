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
import { createLocalBashOperations, createReadTool, createGrepTool, createLsTool, createFindTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { describe, expect, vi } from "vitest";
import { buildActionKey, buildPiActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { inspectHeldExecProcess, LinuxHeldExecBoundary, type HeldExecDecision, type HeldExecProcess, type HeldExecTiming } from "../src/linux-held-exec.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend, validateTransferredProcessEvidence, type LinuxProcessSession } from "../src/linux-process-backend.ts";
import { ProcessHandoffRegistry, type ProcessExecutionBinding } from "../src/process-handoff.ts";
import { resolvePiToolInvocation, createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";
import * as scheduling from "../src/scheduler.ts";
import type { ExecutionOperationBinding } from "../src/execution-world.ts";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { TaskTimeline, TimelineInterval, type TimelineDependency } from "../src/task-timing.ts";
import { testModel } from "./model.ts";
import type { WorkspaceTransactionCapture } from "../src/workspace-transaction.ts";
import { commitBenchmarkFixture, compileBenchmarkHelper, cHeaders, processTest as test, executeProcess, forkReusableBash, prepareLinuxProcessReuse,
	holdProcessPublication, textOutput, type LinuxProcessBenchmark } from "./linux-process-fixture.ts";

vi.mock("node:child_process", { spy: true });
vi.mock("node:fs/promises", { spy: true });
const nativeTest = process.arch === "x64" ? test : test.skip;

const rightsHelpers = `${cHeaders("fcntl.h sys/socket.h string.h unistd.h")}
static inline int pass(int fd, const char *text, int length, const int *rights, int count, int flags) {
	char control[CMSG_SPACE(8 * sizeof(int))] = {0}; struct iovec vector = {(void *)text, (size_t)length};
	struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = CMSG_SPACE((size_t)count * sizeof(int))}};
	struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN((size_t)count * sizeof(int));
	memcpy(CMSG_DATA(header), rights, (size_t)count * sizeof(int)); return (flags < 0 ? sendmmsg(fd, &message, 1, 0) : sendmsg(fd, &message.msg_hdr, flags)) != (flags < 0 ? 1 : length);
}
static inline int take(int fd, char expected, int *rights, int count, int peek, int batch) {
	char byte, control[CMSG_SPACE(8 * sizeof(int))]; struct iovec vector = {&byte, 1};
	struct mmsghdr message = {.msg_hdr = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}};
	if (batch < 0 && recvmsg(-1, &message.msg_hdr, 0) != -1) return 1;
	int flags = MSG_CMSG_CLOEXEC | (peek ? MSG_PEEK : 0);
	if ((batch > 0 ? recvmmsg(fd, &message, 1, flags, 0) : recvmsg(fd, &message.msg_hdr, flags)) != 1 || byte != expected || message.msg_hdr.msg_flags & MSG_CTRUNC) return 1;
	struct cmsghdr *header = CMSG_FIRSTHDR(&message.msg_hdr); if (!header || header->cmsg_len != CMSG_LEN((size_t)count * sizeof(int))) return 1;
	memcpy(rights, CMSG_DATA(header), (size_t)count * sizeof(int));
	for (int index = 0; index < count; index++) if (fcntl(rights[index], F_GETFD) != FD_CLOEXEC) return 1;
	return 0;
}
static inline int wrap_queue(int reference, int cycle) {
	int pair[2]; if (reference < 0 || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair)) return -1;
	int rights[2] = {reference, pair[1]}, failed = pass(pair[0], "x", 1, rights, cycle ? 2 : 1, 0);
	close(reference); close(pair[0]); if (failed) { close(pair[1]); return -1; } return pair[1];
}`;

function patternProcessHost(fixture: LinuxProcessBenchmark, sessionID: string, settings: SpeculativeAgentSettingsInput,
	options: Partial<CreateSpeculativeActionHostOptions> = {}, tools = [fixture.tool]) {
	const events: SpeculativeActionEvent<string>[] = [], patternStore = new PatternAwareStore(patternAwareSettings(settings.patternAware), undefined,
		patternAwareActionSemantics(PI_ACTION_SEMANTICS, fixture.workspace));
	const host = createSpeculativeActionHost(sessionID, { cwd: fixture.workspace, patternStore,
		complete: async () => { throw new Error("unexpected inference"); }, preflight: () => true, executionWorlds: [fixture.world],
		getSettings: () => ({ enabled: true, drafterEnabled: false, tools: ["bash"], ...settings }),
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath }),
		onEvent: event => { events.push(event); }, ...options,
	});
	return { host, events, patternStore,
		start: (turnID: string) => host.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined, context: { systemPrompt: "unchanged", messages: [], tools } }),
		call: (turnID: string, command: string) => ({ turnID, id: turnID, tool: "bash", args: { command }, tools }),
	};
}

async function learnWorker(fixture: LinuxProcessBenchmark, scope: { sessionID: string; turnID: string }, command: string) {
	await prepareLinuxProcessReuse(fixture);
	const route = await fixture.prepareActorReplay(), handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string }>;
	let binding: ProcessExecutionBinding | undefined;
	const execute = async (current = scope, input = command, signal = AbortSignal.timeout(20_000)) => {
		const { output, ...result } = await executeProcess(fixture, route.executor, input, { scope: current, signal, timeout: 10 });
		expect(result).toEqual({ exitCode: 0 }); return output;
	};
	const actor = (signal?: AbortSignal) => {
		let computations: readonly TimelineDependency[] = [];
		return TimelineInterval.measure(() => fixture.backend.observeBindings(scope, () => execute(scope, command, signal), (bindings, observed) => {
			computations = observed; binding ??= bindings.find(candidate => path.basename(handoffs.resolveBinding(candidate, scope)?.executable ?? "") === "worker");
		}, true), () => computations);
	};
	const seed = (await actor()).output; expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
	const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
	return { actor, execute, seed, binding: binding!, handoffs, invocation };
}

async function runningProcess(start: number, ready: (pid: number, syscall: string) => boolean | Promise<boolean>) {
	let pid = 0;
	await expect.poll(async () => {
		for (const [, args] of vi.mocked(childProcess.spawn).mock.calls.slice(start)) {
			const reportPath = Array.isArray(args) && args.find(arg => arg.endsWith("/fd-offsets")); if (!reportPath) continue;
			const report = await readFile(reportPath, "utf8").catch(() => "");
			const match = /\nRUNNING (\d+)\n$/.exec(report); if (!match) continue;
			const current = Number(match[1]), syscall = await readFile(`/proc/${current}/syscall`, "utf8").catch(() => "");
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
	const session = await fixture.backend.open(input);
	try {
		const result = await session.executeBinding(binding); expect(result.exit).toEqual({ kind: "code", code: 0 });
		expect(result.output.map(({ fd, data }) => [fd, data.toString()])).toEqual([[1, output]]);
		if (validate) { await session.seal([]); expect(await session.validate(), JSON.stringify(session.metrics())).toMatchObject({ status: "valid" }); }
		await run(session);
	} finally { await session.close(); }
}

describe("Linux process ExecutionWorld", () => {
nativeTest("preserves fresh invocation inputs without classifying shell syntax", { timeout: 30_000 }, async ({ benchmark }) => {
		const fixture = await benchmark("pi-shell-proof-", "git");
		const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
		for (const command of ['printf "%s\\n" constant', 'printf "%s\\n" "$RANDOM"', 'printf "%s\\n" "$((RANDOM))"',
			'v=RANDOM; printf "%s\\n" "${!v}"', 'declare -n v=RANDOM; printf "%s\\n" "$v"']) {
			const constant = command.includes("constant"), before = fixture.backend.metrics().wholeCommandHits;
			for (let turn = 0; turn < 3; turn++) {
				const branch = await forkReusableBash(fixture, { command, label: String(turn), executionFingerprint, actionNamespace: "shell-proof",
					executionScope: { sessionID: "shell-proof", turnID: String(turn) } });
				try { expect(textOutput(branch.output.result)).toMatch(constant ? /^constant\n$/ : /^\d+\n$/); }
				finally { await branch.dispose(); }
			}
			expect(fixture.backend.metrics().wholeCommandHits - before, command).toBe(0);
			const actorBefore = fixture.backend.actorMetrics().wholeCommandHits;
			for (let turn = 0; turn < 3; turn++)
				expect(textOutput(await fixture.tool.execute(`actor-${turn}`, { command }))).toMatch(constant ? /^constant\n$/ : /^\d+\n$/);
			expect(fixture.backend.actorMetrics().wholeCommandHits - actorBefore, command).toBe(0);
		}
	});

nativeTest("proves external reads from their opened image and never commits a read copy", { timeout: 30_000 }, async ({ benchmark }) => {
		const fixture = await benchmark("pi-read-image-", "git"), outside = await mkdtemp(path.join(os.tmpdir(), "pi-read-image-host-"));
		const target = path.join(outside, "input"), alias = path.join(outside, "alias");
		try {
			await writeFile(target, "old\n"); await filesystem.symlink("input", alias);
			await compileBenchmarkHelper(fixture.workspace, { source: "permissions.c", output: "permissions", code: `${cHeaders("stdio.h unistd.h dirent.h string.h sys/stat.h")}
int main(int argc, char **argv) { FILE *f = fopen(argc > 1 ? argv[1] : "/etc/issue", "r"); if (!f) return 1; fgetc(f);
 if (argc > 1) { struct stat st; if (fstat(fileno(f), &st)) return 2; DIR *d = opendir(argv[2]); if (!d) return 3; struct dirent *e;
  while ((e = readdir(d))) if (!strcmp(e->d_name, "input")) { puts(e->d_ino == st.st_ino ? "identical" : "different"); break; } closedir(d);
 } else printf("%d %d %d\\n", access("/etc/issue", R_OK), access("/etc/issue", W_OK), access("/etc/issue", X_OK)); fclose(f); }\n` });
			const prepared = await prepareLinuxProcessReuse(fixture), route = await fixture.prepareActorReplay(), scope = { sessionID: "read-image", turnID: "actor" };
			for (const args of [[], [target, outside]]) {
				const permissionBranch = await forkReusableBash(fixture, { command: `./permissions ${args.map(arg => `'${arg}'`).join(" ")}`, label: "permissions", actionNamespace: "read-image", ...prepared });
				try {
					expect(textOutput(permissionBranch.output.result)).toBe(execFileSync(path.join(fixture.workspace, "permissions"), args, { encoding: "utf8" }));
				} finally { await permissionBranch.dispose(); }
			}
			for (const [label, pathname] of [["direct", target], ["alias", alias]] as const) {
				const command = `cat '${pathname}'`, branch = await forkReusableBash(fixture, { command, label, executionScope: scope, actionNamespace: "read-image", ...prepared });
				try {
					expect(textOutput(branch.output.result)).toBe("old\n");
					const validation = await branch.validate?.();
					expect(validation, JSON.stringify({ validation, metrics: fixture.backend.metrics() })).toMatchObject({ status: "valid" });
					const before = fixture.backend.actorMetrics().hits;
					expect((await executeProcess(fixture, route.executor, command, { scope })).output).toBe("old\n");
					expect(fixture.backend.actorMetrics().hits - before, JSON.stringify(fixture.backend.actorMetrics())).toBe(1);
					expect(await readFile(target, "utf8")).toBe("old\n");
				} finally { await branch.dispose(); }
			}
			const { readFile: read } = await vi.importActual<typeof filesystem>("node:fs/promises"); let changed = false;
			const reading = vi.spyOn(filesystem, "readFile").mockImplementation(async (...args) => {
				const bytes = await read(...args);
				if (!changed && String(args[0]).includes("/trace-")) { changed = true; await writeFile(target, "new\n"); }
				return bytes;
			});
			try {
				const branch = await forkReusableBash(fixture, { command: `cat '${target}'`, label: "raced", actionNamespace: "read-image-race", ...prepared });
				try {
					expect(changed).toBe(true); expect(textOutput(branch.output.result)).toBe("old\n");
					expect(await branch.validate?.()).toMatchObject({ status: "stale" });
					expect(textOutput(await fixture.tool.execute("raced-actor", { command: `cat '${target}'` }))).toBe("new\n");
					await branch.commit(); expect(await readFile(target, "utf8")).toBe("new\n");
				} finally { await branch.dispose(); }
			} finally { reading.mockRestore(); }
		} finally { await fixture.dispose(); await rm(outside, { recursive: true, force: true }); }
	});

nativeTest("admits expensive late launches by completed work within a bounded learning budget", { timeout: 30_000 }, async ({ benchmark }) => {
		const fixture = await benchmark("pi-learning-admission-", "git");
		await prepareLinuxProcessReuse(fixture);
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
		await writeFile(path.join(fixture.workspace, "package.json"), "{}\n");
		await filesystem.symlink(mode === "normalized-blocker" ? "inner/dir" : "module.js", path.join(fixture.workspace, "alias"));
		if (mode === "normalized-blocker") {
			await mkdir(path.join(fixture.workspace, "inner/dir"), { recursive: true });
			await mkdir(path.join(fixture.workspace, "inner/module.js"));
			await filesystem.symlink("../../module.js", path.join(fixture.workspace, "inner/module.js/package.json"));
		}
		await commitBenchmarkFixture(fixture.workspace, "blocked module lookup");
		const prepared = await prepareLinuxProcessReuse(fixture);
		const target = mode === "normalized-blocker" ? "alias/../module.js/package.json" : mode === "symlink" ? "alias/package.json" : mode === "lexical-parent" ? "module.js/../package.json" : "module.js/package.json";
		const script = `import os\ntry:\n os.stat(${JSON.stringify(target)})\n print(0)\nexcept OSError as error:\n print(error.errno)`;
		const command = `if [ -e '${target}' ]; then printf present; else printf blocked; fi`;
		expect(execFileSync("/usr/bin/python3", ["-c", script], { cwd: fixture.workspace, encoding: "utf8" })).toBe(mode === "normalized-blocker" ? "0\n" : "20\n");
		const branch = await forkReusableBash(fixture, { command, label: "enotdir", actionNamespace: "enotdir", ...prepared });
		try {
			const native = execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: fixture.environment, encoding: "utf8" });
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
				expect(execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: fixture.environment, encoding: "utf8" })).toBe("present");
			}
		} finally { await branch.dispose(); }
	});


nativeTest.for(["missing", "appeared", "file", "blocked", "symlink", "lexical", "socket", "socket-file", "socket-blocked", "socket-listening", "socket-private", "socket-private-blocked", "socket-mapped", "socket-relative", "socket-link"] as const)("proves only absent host lookups and cleans its long-path broker (%s)", { timeout: 20_000 }, async (mode, { benchmark }) => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-long-broker-")), previous = process.env.TMPDIR;
		const temporary = path.join(root, "nested-temporary-directory-".repeat(3));
		await mkdir(temporary); process.env.TMPDIR = temporary;
		const listening = vi.spyOn(net.Server.prototype, "listen");
		let target = `${root}/candidate${mode.endsWith("blocked") ? "/child" : mode === "lexical" ? "/../other" : ""}`;
		if (mode.endsWith("file") || mode === "blocked" || mode === "socket-blocked") await writeFile(path.join(root, "candidate"), "prepared");
		if (mode === "symlink" || mode === "socket-link") await filesystem.symlink("absent", target);
		let connections = 0;
		const listener = mode === "socket-listening" ? net.createServer(socket => { connections++; socket.destroy(); }) : undefined;
		if (listener) { listener.listen(target); await once(listener, "listening"); }
		const { lstat } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const probing = vi.spyOn(filesystem, "lstat").mockImplementation(async (...args) => {
			if (mode === "appeared" && String(args[0]) === target) await writeFile(target, "appeared");
			return lstat(...args);
		});
		let fixture: Awaited<ReturnType<typeof benchmark>> | undefined;
		try {
			fixture = await benchmark("command-", "git");
			await writeFile(path.join(fixture.workspace, "input.txt"), "input\n");
			if (mode.startsWith("socket")) {
				await compileBenchmarkHelper(fixture.workspace, { source: "connect.c", output: "connect", code: String.raw`${cHeaders("sys/socket.h sys/un.h errno.h stdio.h string.h unistd.h")}
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
			await commitBenchmarkFixture(fixture.workspace, "long temporary path");
			const prepared = await prepareLinuxProcessReuse(fixture);
			const privateWrite = mode.startsWith("socket-private") || mode === "socket-mapped";
			const command = mode.startsWith("socket") ? `${privateWrite ? `printf private > '${mode === "socket-private-blocked" ? path.dirname(target) : target}'; ` : ""}${mode === "socket-relative" ? `cd '${root}'; ` : ""}'${fixture.workspace}/connect' '${mode === "socket-relative" || mode === "socket-mapped" ? "candidate" : target}'` : `/bin/cat '${target}' 2>/dev/null || printf prepared`;
			const branch = await forkReusableBash(fixture, { label: "long-path", command, actionNamespace: "test", ...prepared });
			try {
				expect(textOutput(branch.output.result)).toBe(mode.startsWith("socket") ? `${mode.endsWith("blocked") ? 20 : privateWrite || mode === "socket-listening" || mode === "socket-file" ? 13 : 2}\n` : "prepared");
				if (!privateWrite) expect(await branch.validate?.()).toMatchObject({ status: mode === "missing" || mode === "socket" || mode === "file" ? "valid" : "indeterminate" });
				if (privateWrite) await expect(filesystem.access(target)).rejects.toMatchObject({ code: "ENOENT" });
				expect(connections).toBe(0);
				if (mode === "missing" || mode === "socket") {
					await writeFile(target, "appeared");
					expect(await branch.validate?.()).toMatchObject({ status: "stale" });
				}
			}
			finally { await branch.dispose(); }
			const sockets = listening.mock.calls.flatMap(([target]) => typeof target === "string" && target.startsWith("/tmp/pi-broker-") ? [target] : []);
			expect(sockets).toHaveLength(1);
			expect(filesystem.chmod).toHaveBeenCalledWith(sockets[0], 0o600);
			await expect(filesystem.access(sockets[0]!)).rejects.toMatchObject({ code: "ENOENT" });
		} finally {
			if (listener) await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
			await fixture?.dispose(); listening.mockRestore(); probing.mockRestore();
			if (previous === undefined) delete process.env.TMPDIR; else process.env.TMPDIR = previous;
			await rm(root, { recursive: true, force: true });
		}
	});

nativeTest.for(["unusable", "one-shot", "descendant", "unusable-descendant"] as const)("rechecks failed native preparation when its observed inputs change (%s)", { timeout: 20_000 }, async (mode, { benchmark }) => {
		const fixture = await benchmark("pi-preparation-evidence-");
		const publishing = vi.spyOn(fixture.backend.planner, "publishCompleted");
		try {
			await writeFile(path.join(fixture.workspace, "input.txt"), "prepared\n");
			await writeFile(path.join(fixture.workspace, "body.sh"), 'ulimit -c 0\n' + (mode === "unusable-descendant" ? "" : 'printf "prepared\\n"\n'));
			if (mode === "unusable-descendant") await writeFile(path.join(fixture.workspace, "unusable-child.sh"),
				'i=0; while [ "$i" -lt 10000 ]; do i=$((i + 1)); done\nulimit -c 0\nprintf "prepared\\n"\n');
			if (mode === "one-shot") {
				await compileBenchmarkHelper(fixture.workspace, { source: "one-shot.c", output: "one-shot", code: '#include <stdio.h>\n#include <unistd.h>\nint main(void) { usleep(20000); printf("prepared:%ld\\n", (long)getpid()); return 0; }\n' });
			}
			await commitBenchmarkFixture(fixture.workspace, "Preparation evidence");
			await prepareLinuxProcessReuse(fixture);
			const route = await fixture.prepareActorReplay(), scope = { sessionID: "preparation-evidence", turnID: "native" };
			// Changing a process limit is an existing unsupported syscall observation. Only this child changes its limit.
			const script = 'i=0; while [ "$i" -lt 10000 ]; do i=$((i + 1)); done; ' +
				(mode === "descendant" ? "ulimit -c 0; /bin/cat input.txt; :" :
					mode === "unusable-descendant" ? ". ./body.sh; /bin/bash ./unusable-child.sh; :" : ". ./body.sh");
			const command = mode === "one-shot" ? "exec ./one-shot" : `exec /bin/sh -c '${script}'`;
			const expectedOutput = mode === "one-shot" ? expect.stringMatching(/^prepared:\d+\n$/) : "prepared\n";
			const handoffs = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ readonly args: readonly string[]; readonly executable: string }>;
			let binding: ProcessExecutionBinding | undefined, childBinding: ProcessExecutionBinding | undefined;
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
				shellPath: mode === "unusable-descendant" ? "/bin/sh" : fixture.shellPath })!.process!;
			const prepare = () => fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
				withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: { ...scope, turnID: "prepared" } }, binding!, expectedOutput, async session => {
					if (mode === "descendant") expect(session.executionBindings().some(child => child.available && child.key !== binding!.key)).toBe(true);
				}, false), { liveLower: true });
			if (mode === "unusable" || mode === "unusable-descendant") {
				const before = fixture.backend.metrics().requests;
				await expect(prepare()).rejects.toThrow("bound process preparation produced no reusable result");
				expect(binding!.available).toBe(true);
				expect(await fixture.backend.bindingStale(binding!)).toBe(false);
				expect(fixture.backend.metrics().lastError).toContain("unsupported_syscall");
				if (mode === "unusable-descendant") {
					expect(fixture.backend.metrics().requests - before).toBe(2);
					expect(publishing.mock.calls.map(([certificate]) => certificate.dependencyCertificate.taints))
						.toEqual([expect.arrayContaining(["unsupported_syscall"]), expect.arrayContaining(["unsupported_syscall"])]);
					expect(childBinding).toBeDefined();
					expect(await fixture.backend.bindingStale(childBinding!)).toBe(false);
				}
				await writeFile(path.join(fixture.workspace, "input.txt"), "arbitrary edit\n");
				expect((await observe()).output).toBe("prepared\n");
				expect(await fixture.backend.bindingStale(binding!)).toBe(false); // An unrelated edit cannot revive unchanged failure inputs.
				await writeFile(path.join(fixture.workspace, "body.sh"), mode === "unusable-descendant" ? "" : 'printf "prepared\\n"\n');
				if (childBinding) await writeFile(path.join(fixture.workspace, "unusable-child.sh"), 'printf "prepared\\n"\n');
				expect(await fixture.backend.bindingStale(binding!)).toBe(true);
				if (childBinding) expect(await fixture.backend.bindingStale(childBinding)).toBe(true);
				await prepare();
				expect(await fixture.backend.bindingStale(binding!)).toBe(false);
			} else {
				await prepare();
				expect(binding!.available).toBe(true);
				if (mode === "one-shot") {
					expect(fixture.backend.metrics().published).toBe(0);
					expect(publishing.mock.calls.find(([certificate]) => certificate.weakKey === binding!.key)?.[0].dependencyCertificate.taints)
						.toEqual(expect.arrayContaining(["clock", "random", "pid_observation"]));
				}
			}
		} finally { publishing.mockRestore(); await fixture.dispose(); }
	});

nativeTest.for([false, true])("prepares a failed command after its reported input is edited and validates before committing (stale=%s)", { timeout: 20_000 }, async (stale, { benchmark }) => {
		const fixture = await benchmark("pi-failed-command-");
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		try {
			await writeFile(path.join(fixture.workspace, "source.txt"), "broken\n");
			await commitBenchmarkFixture(fixture.workspace, "failed command input");
			await prepareLinuxProcessReuse(fixture);
			const writer = createWriteTool(fixture.workspace), tools = [fixture.tool, writer], sessionID = "failed-command";
			const command = 'IFS= read -r value < source.txt; if [ "$value" != fixed ]; then printf "source.txt:1: expected fixed, got %s\\n" "$value"; exit 1; fi; printf "%s\\n" "$value" > generated.txt; printf "verified\\n"';
			const prepared = patternProcessHost(fixture, sessionID, { patternAware: patternAwareSettings({ presets: ["retry-failed-command"] }) }, {}, tools), { events, start } = prepared;
			host = prepared.host;
			const call = (turnID: string) => prepared.call(turnID, command);
			await start("failure");
			await expect(host.execute(call("failure"), undefined, () => fixture.tool.execute("failure", { command }))).rejects.toThrow("got broken");
			await host.finishTurn("failure");
			expect(fixture.backend.executionBindings({ sessionID, turnID: "failure" })).toEqual([]);
			await start("repair");
			const args = { path: "source.txt", content: "fixed\n" };
			await host.execute({ turnID: "repair", id: "repair", tool: "write", args, tools }, undefined, () => writer.execute("repair", args));
			await expect.poll(() => events.some(event => event.type === "candidate" && event.candidate.source === "pattern_aware" &&
				event.candidate.tool === "bash" && event.state.status === "succeeded"), { timeout: 5000 }).toBe(true);
			expect(existsSync(path.join(fixture.workspace, "generated.txt"))).toBe(false);
			await host.finishTurn("repair");
			if (stale) await writeFile(path.join(fixture.workspace, "source.txt"), "changed\n");
			await start("retry");
			const fallback = vi.fn(() => fixture.tool.execute("retry", { command })), result = host.execute(call("retry"), undefined, fallback);
			if (stale) {
				await expect(result).rejects.toThrow("got changed");
				expect(existsSync(path.join(fixture.workspace, "generated.txt"))).toBe(false);
			} else {
				expect((await result).content).toEqual([{ type: "text", text: "verified\n" }]);
				expect(await readFile(path.join(fixture.workspace, "generated.txt"), "utf8")).toBe("fixed\n");
			}
			await host.finishTurn("retry");
			expect(fallback).toHaveBeenCalledTimes(stale ? 1 : 0);
			expect(events.filter(event => event.type === "prediction")).toContainEqual(expect.objectContaining({ settlement:
				expect.objectContaining({ observation: "observed", match: expect.objectContaining({ matched: true, adoption:
					expect.objectContaining(stale ? { status: "rejected", cause: expect.objectContaining({ stage: "freshness" }) } : { status: "adopted" }) }) }) }));
		} finally { await host?.dispose(); await fixture.dispose(); }
	});

nativeTest.for(["bash", "write", "stale", "disabled", "bash:shared", "write:shared", "stale:shared", "disabled:shared", "bash:parents", "bash:aliases", "bash:both", "bash:repeat", "write:repeat"])("reprepares changed children beyond fresh beam entries (%s)", { timeout: 20_000 }, async (scenario, { benchmark }) => {
	const [mode, layout] = scenario.split(":"), target = layout === "parents" ? "first" : "last", both = layout === "both";
	const fixture = await benchmark("pi-rerun-frontier-", "git"), scope = { sessionID: "frontier", turnID: "seed" };
	let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
	try {
		for (const name of ["first", "last"]) await writeFile(path.join(fixture.workspace, name), "old\n");
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: `${cHeaders("fcntl.h unistd.h")}
int main(int argc, char **argv) { if (argc != 2) return 1; int fd = open(${layout ? '"last"' : "argv[1]"}, O_RDONLY); char bytes[16];
 int length = read(fd, bytes, sizeof(bytes)); if (length < 0 || close(fd)) return 2;
 for (volatile unsigned long i = 0; i < 40000000ul; ++i) {} return write(1, bytes, (size_t)length) != length; }
` });
		await commitBenchmarkFixture(fixture.workspace, "changed child frontier"); await prepareLinuxProcessReuse(fixture);
		const route = await fixture.prepareActorReplay(), writer = createWriteTool(fixture.workspace), tools = [fixture.tool, writer];
		const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string; args: readonly string[] }>;
		const settings = { candidateLimit: 4, maxConcurrentActions: 4, patternAware: patternAwareSettings({ multiStepEnabled: false, beamWidth: 1, presets: ["recent-command"] }) };
		const prepared = patternProcessHost(fixture, scope.sessionID, settings, {}, tools), { events, start, call } = prepared; host = prepared.host;
		const execute = (turnID: string, command: string) => host!.execute({ ...call(turnID, command), id: `${turnID}:${command}` }, AbortSignal.timeout(10_000),
			() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID } }) }, () => fixture.tool.execute(turnID, { command })));
		await start("seed");
		const commands = layout === "parents" ? ["printf parent >> marker; worker first", "worker last"] : ["printf parent >> marker; worker first; worker last", ...layout === "aliases" ? ["printf ''; worker first; worker last"] : []];
		for (const command of commands) expect(textOutput(await execute("seed", command))).toBe("old\n".repeat(command.includes("; worker last") ? 2 : 1));
		await host.finishTurn("seed");
		const bindings = fixture.backend.executionBindings(scope).filter(binding => registry.resolveBinding(binding, scope)?.executable.endsWith("/worker"));
		expect(bindings).toHaveLength(2);
		const consumed = bindings.find(binding => registry.resolveBinding(binding, scope)?.args.includes(target))!;
		const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
		for (const binding of bindings) await fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
			withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: { ...scope, turnID: "warm" } }, binding, "old\n", async () => {}), { liveLower: true });
		expect(await Promise.all(bindings.map(binding => fixture.backend.bindingStale(binding)))).toEqual([false, false]);
		const publishing = vi.spyOn(fixture.backend.planner, "publishCompleted"), checking = vi.spyOn(fixture.backend, "bindingStale");
		await start("unrelated"); await execute("unrelated", "printf ignored > unrelated");
		await expect.poll(() => checking.mock.calls.length).toBeGreaterThan(0);
		await Promise.all(checking.mock.results.map(result => result.value)); await nextTurn();
		expect(events.filter(event => event.type === "candidate" && event.candidate.mode === "recent-command")).toEqual([]); await host.finishTurn("unrelated");
		if (mode === "disabled") settings.patternAware = patternAwareSettings({ enabled: false });
		await start("edit");
		for (const value of layout === "repeat" ? ["new", "two"] : ["new"]) {
			if (mode === "write") {
				const args = { path: "last", content: `${value}\n` };
				await host.execute({ turnID: "edit", id: `edit:${value}`, tool: "write", args, tools }, undefined, () => writer.execute("edit", args));
			} else await execute("edit", `printf '${value}\\n' > last`);
			if (mode !== "disabled") await expect.poll(() => events.filter(event => event.type === "candidate" && event.turnID === "edit" &&
				event.candidate.mode === "recent-command" && event.candidate.kind === "operation" && event.state.status === "succeeded").length, { timeout: 5000 }).toBe((layout ? 2 : 1) * (value === "two" ? 2 : 1));
		}
		expect(publishing.mock.calls.map(([certificate]) => certificate.weakKey).sort()).toEqual(mode === "disabled" ? [] : (layout ? bindings : [consumed]).flatMap(binding => layout === "repeat" ? [binding.key, binding.key] : [binding.key]).sort());
		if (mode === "stale") await writeFile(path.join(fixture.workspace, "last"), "bad\n");
		const before = fixture.backend.actorMetrics().hits, actual = await execute("edit", `printf actual; ${both ? "worker first; " : ""}worker ${target}`);
		const computation = events.filter(event => event.type === "actor_action").at(-1)!.settlement.provider.toolExecution, timing = consumedTiming(computation);
		expect(textOutput(actual)).toBe(`actual${`${mode === "stale" ? "bad" : layout === "repeat" ? "two" : "new"}\n`.repeat(both ? 2 : 1)}`);
		expect(await readFile(path.join(fixture.workspace, "marker"), "utf8")).toBe("parent");
		const reused = mode === "bash" || mode === "write";
		expect([fixture.backend.actorMetrics().hits - before, timing.hiddenComputeMs > 0]).toEqual([reused ? both ? 2 : 1 : 0, reused]);
		const receipts = new Set(TimelineInterval.serialize(computation)!.nodes.flatMap(node => node.inputs?.filter(input => input.reused).map(input => input.id) ?? []));
		expect(publishing.mock.calls.filter(([certificate]) => receipts.has(certificate.result.computation!.root)).map(([certificate]) => certificate.weakKey).sort())
			.toEqual(reused ? (both ? bindings : [consumed]).map(binding => binding.key).sort() : []);
		if (reused) expect(timing.hiddenByMode).toEqual([{ source: "pattern_aware", mode: "recent-command", hiddenComputeMs: timing.hiddenComputeMs }]);
		await host.finishTurn("edit", true);
		if (mode === "disabled") expect(events.filter(event => event.type === "candidate" && event.candidate.mode === "recent-command")).toEqual([]);
	} finally { await host?.dispose(); await fixture.dispose(); }
});

nativeTest.for(["sample", "file", "dup", "threads", "limit", "metadata", "identity", "pipe", "socket", "eventfd", "pipe:unavailable", "socket:unavailable", "eventfd:unavailable", "eventfd:unavailable:destroyed", "eventfd::inherited", "eventfd:unavailable:external", "eventfd::external", "eventfd:unavailable:discard", "eventfd::discard", "eventfd:unavailable:discard-nested", "eventfd::discard-nested", "eventfd:unavailable:discard-cycle"])("adopts a closed-input calculation through capture or completion (%s)", { timeout: 30_000 }, async (scenario, { benchmark }) => {
		const [mode, unavailable, queue] = scenario.split(":"), fixture = await benchmark("pi-prefix-process-", "git", { processImages: unavailable ? false : undefined });
		const descendant = mode === "pipe" || mode === "socket" || mode === "eventfd", complete = descendant || mode === "identity", discarded = queue?.startsWith("discard"), cyclic = queue === "discard-cycle", capturable = !unavailable;
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", arguments: ["-pthread"], code: `#define _GNU_SOURCE
${cHeaders("unistd.h sys/syscall.h sys/random.h sys/resource.h sys/stat.h sys/wait.h pthread.h stdio.h stdint.h fcntl.h")}
static void *thread(void *unused) { return unused; }
int main(void) {
 ${mode === "identity" ? "pid_t prior = getpid();" : ""}
 int fd = open("prefix-ready", O_WRONLY|O_CREAT|O_TRUNC, 0600); if (fd < 0 || write(fd, "ready", 5) != 5) return 71;
 ${mode === "dup" ? "int copy = dup(fd); if (copy < 0 || close(fd)) return 77; fd = copy;" : mode === "file" ? "" : "if (close(fd)) return 71;"}
 ${descendant ? "pid_t child = fork(); if (child < 0) return 78; if (!child) { if (close(3)) _exit(79);" : ""}
 volatile uint64_t hash = 1; for (unsigned i = 0; i < 800000000; i++) hash = hash * 33 + i;
 ${descendant ? `if (write(4, &hash, sizeof(hash)) != sizeof(hash) || close(4)) _exit(79); _exit(0); }
 uint64_t hash; int status; if (close(4) || read(3, &hash, sizeof(hash)) != sizeof(hash) || close(3) || waitpid(child, &status, 0) != child || status) return 79;` : ""}
 ${mode === "file" || mode === "dup" ? "if (close(fd)) return 77;" : ""}
 ${mode === "metadata" ? "struct stat state; if (fstat(STDOUT_FILENO, &state) || !state.st_ino) return 76;" : ""}
 ${mode === "threads" ? "pthread_t worker; void *result; if (pthread_create(&worker, NULL, thread, (void *)37) || pthread_join(worker, &result) || result != (void *)37) return 72;" : "(void)thread;"}
 ${mode === "limit" ? "struct rlimit limit = {0, 0}; if (setrlimit(RLIMIT_CORE, &limit)) return 73;" : ""}
 uint64_t value; if (getrandom(&value, sizeof(value), 0) != sizeof(value)) return 74;
 printf("%ld:%ld:%llu:%llu\\n", (long)getpid(), (long)syscall(SYS_gettid), (unsigned long long)hash, (unsigned long long)value);
 ${mode === "identity" ? "if (prior != getpid()) return 75;" : ""} return 0;
}
` });
		if (descendant) await compileBenchmarkHelper(fixture.workspace, { source: "launch.c", output: "launch", code: `#define _GNU_SOURCE
${cHeaders("sys/eventfd.h sys/wait.h sys/mman.h")}
${rightsHelpers}
int main(void) { int fds[2]; if (${mode === "eventfd" ? "(fds[0] = eventfd(0, 0)) < 0 || (fds[1] = dup(fds[0])) < 0" : mode === "socket" ? "socketpair(AF_UNIX, SOCK_STREAM, 0, fds)" : "pipe(fds)"} || fds[0] != 3 || fds[1] != 4) return 61;
 ${queue && queue !== "external" ? `int queues[2]; if (socketpair(AF_UNIX, SOCK_STREAM, 0, queues)) return 63;
 int reference = ${discarded ? queue !== "discard" ? `wrap_queue(memfd_create("queued-file", MFD_CLOEXEC), ${Number(cyclic)})` : 'memfd_create("queued-file", MFD_CLOEXEC)' : "-1"};
 if (${discarded ? "reference < 0 || " : ""}pass(queues[0], "x", 1, &reference, ${discarded ? 1 : 0}, 0)${discarded ? " || close(reference)" : ""}) return 63;` : ""}
 ${queue === "external" ? `int gate[2]; char byte; if (wrap_queue(wrap_queue(eventfd(5, EFD_CLOEXEC), 0), 0) < 0 || pipe2(gate, O_CLOEXEC)) return 64;
 pid_t child = fork(); if (child < 0) return 65;
 if (child) { close(3); close(4); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 66; close(gate[1]); int status; return waitpid(child, &status, 0) != child || status; }
 close(gate[1]); if (read(gate[0], &byte, 1) != 1) return 66; close(gate[0]);` : ""}
 close_range(${queue === "inherited" || discarded ? 7 : 5}, ~0U, 0); execlp("worker", "worker", NULL); return 62; }
` });
		await commitBenchmarkFixture(fixture.workspace, "Closed input prefix");
		const scope = { sessionID: "prefix", turnID: "same" };
		const { handoffs, actor, seed, binding, invocation } = await learnWorker(fixture, scope, `exec ./${descendant ? "launch" : "worker"}`);
		const publishing = vi.spyOn(handoffs, "publish");
		await rm(path.join(fixture.workspace, "prefix-ready"));
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
			const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope });
			const starting = gated(), frontier = gated(), controller = new AbortController();
			const acquire = handoffs.acquire.bind(handoffs), observe = handoffs.observeRunning.bind(handoffs), wait = scheduling.waitForCompletion;
			let completion: Promise<void> | undefined, joins = 0, cancelled: Promise<unknown> | undefined;
			const admissions: Array<{ syscall: number; accepted: boolean | undefined }> = [];
			const acquiring = vi.spyOn(handoffs, "acquire").mockImplementation(async request => {
				const result = await acquire(request);
				if (mode === "sample" && result.kind === "work") { completion = result.work.completion; await starting.wait(); }
				return result;
			});
			const joining = vi.spyOn(scheduling, "waitForCompletion").mockImplementation((promise, signal, timeout) => {
				if (promise === completion) starting.release(); return wait(promise, signal, timeout);
			});
			const observing = vi.spyOn(handoffs, "observeRunning").mockImplementation((key, work, invoke) => observe(key, work, async () => {
				if (mode === "threads" && ++joins === 2) { controller.abort(); frontier.release(); } return invoke();
			}));
			const target = fixture.backend as unknown as { requestProcessImageAtFrontier(...args: unknown[]): Promise<unknown> }, request = target.requestProcessImageAtFrontier.bind(target);
			const requesting = vi.spyOn(target, "requestProcessImageAtFrontier").mockImplementation(async (...args) => {
				const admit = args[4] as (pid: number, reply: Buffer, report: Buffer) => Promise<boolean | undefined>;
				args[4] = async (pid: number, reply: Buffer, report: Buffer) => {
					const accepted = await admit(pid, reply, report); admissions.push({ syscall: reply.readInt32LE(8), accepted }); return accepted;
				};
				if (mode === "threads") await frontier.wait(); return request(...args);
			});
			const spawnStart = vi.mocked(childProcess.spawn).mock.calls.length, pending = session.executeBinding(binding), settled = pending.catch(() => undefined);
			try {
				if (mode === "sample") await starting.entered;
				else if (descendant) await runningProcess(spawnStart, (_pid, syscall) => syscall.startsWith("0 0x3 "));
				else await expect.poll(() => existsSync(path.join(workspace.sandboxRoot, "prefix-ready"))).toBe(true);
				expect(existsSync(path.join(fixture.workspace, "prefix-ready"))).toBe(false);
				if (mode === "threads") { cancelled = actor(controller.signal).catch(() => undefined); await frontier.entered; }
				const issuedAt = performance.now(), actual = await actor(), fields = actual.output.trim().split(":");
				const timing = new TaskTimeline(issuedAt).recordCall([{ computation: actual.computation }], issuedAt, performance.now());
				expect(fields).toHaveLength(4); expect(fields[0]).toBe(fields[1]); expect(fields[2]).toBe(seed.trim().split(":")[2]);
				expect(fields[3]).not.toBe(seed.trim().split(":")[3]);
				expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify(fixture.backend.metrics())).toBe(Number(!cyclic));
				if (mode === "file" || mode === "dup") expect(admissions).toEqual([{ syscall: 3, accepted: undefined }, { syscall: 318, accepted: true }]);
				expect(await pending).toMatchObject(complete ? { exit: { kind: "code", code: 0 } } : { suspended: true });
				const continuations = publishing.mock.calls.flatMap(([, , certificate, , continuation]) => certificate?.result.continuation ? [continuation] : []);
				expect(continuations).toEqual(complete ? [] : [expect.anything()]); expect(timing.reused).toBe(cyclic ? undefined : true);
				expect(timing.toolComputeMs).toBeGreaterThanOrEqual(timing.hiddenComputeMs);
				if (complete) {
					expect(admissions.length > 0).toBe(capturable); expect(admissions.every(result => !result.accepted)).toBe(true);
					if (descendant && capturable) expect(admissions).toContainEqual({ syscall: 0, accepted: false });
					if (cyclic) expect(publishing).not.toHaveBeenCalled();
					else if (discarded) {
						const certificate = publishing.mock.calls.find(([, , certificate]) => certificate?.dependencyCertificate.complete)?.[2]!;
						const file = certificate.prototype.inheritedFDs.find(fd => fd.type === "regular")!;
						expect(certificate.result.resources!.transitions!.at(-1)).toMatchObject({ id: file.alias, kind: "release" });
					}
					if (cyclic) expect(timing.hiddenComputeMs).toBe(0); else expect(timing.hiddenComputeMs).toBeGreaterThan(0);
				} else if (mode === "sample") expect(new TaskTimeline(issuedAt).recordTool(continuations[0]!.computation, issuedAt, true).hiddenComputeMs).toBe(0);
			} finally {
				controller.abort(); starting.release(); frontier.release(); await session.close(); await settled; await cancelled;
				acquiring.mockRestore(); joining.mockRestore(); observing.mockRestore(); requesting.mockRestore();
			}
		}, { liveLower: true });
		publishing.mockRestore();
	});

	nativeTest.for(["pipe", "socket", "peer", "eventfd", "readv", "recvmsg", "writev", "mmap", "runtime", "handles", "eof", "early", "changed", "identity", "file", "cancel", "pipe:unavailable", "peer:unavailable", "eventfd:unavailable", "queue-drop", "queue-deferred", "queue-received", "queue-closed", "queue-peeked", "queue-changed", "queue-cancel", "queue-deferred:unavailable", "queue-alias", "queue-counter", "queue-stream", "queue-nested", "queue-peek-kept", "queue-stale", "queue-received-cancel", "queue-received:unavailable"])("resumes a learned running process with future input (%s)", { timeout: 60_000 }, async (scenario, { skip, benchmark }) => {
		const [mode, unavailable] = scenario.split(":"), fixture = await benchmark("pi-live-process-", undefined, { processImages: unavailable ? false : undefined });
		const queued = mode!.startsWith("queue-"), stale = mode === "queue-stale", changed = stale || mode!.endsWith("changed"), cancelled = mode!.endsWith("cancel");
		const received = queued && !["queue-drop", "queue-deferred", "queue-changed", "queue-cancel"].includes(mode!), alias = mode === "queue-alias";
		const inspecting = vi.spyOn(await import("../src/linux-held-exec.ts"), "inspectHeldExecProcess");
		const observations = new Map<string, { bindings: readonly ProcessExecutionBinding[]; computations: readonly TimelineDependency[] }>();
		const observeBindings = fixture.backend.observeBindings.bind(fixture.backend);
		const observing = vi.spyOn(fixture.backend, "observeBindings").mockImplementation((scope, execute, observe, learn, inputs) =>
			observeBindings(scope, execute, (bindings, computations) => { observations.set(scope.turnID, { bindings, computations }); observe(bindings, computations); }, learn, inputs));
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		try {
			await prepareLinuxProcessReuse(fixture);
			if (unavailable) expect((await Reflect.get(fixture.backend, "ready")).imageLibrary).toBeUndefined();
			if (!unavailable && !(await Reflect.get(fixture.backend, "ready")).imageLibrary) return skip("native process image capture is unavailable");
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", arguments: ["-pthread"], code: `#define _GNU_SOURCE
${cHeaders("unistd.h stdio.h stdint.h sys/syscall.h sys/uio.h sys/socket.h sys/mman.h pthread.h signal.h stdlib.h string.h fcntl.h errno.h")}
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
 ${alias ? `if (argc > 1) { char byte; return strcmp(argv[1], "verify") || fcntl(12, F_GETFD) != -1 || errno != EBADF || fcntl(13, F_GETFD) || lseek(5, 0, SEEK_CUR) != 2 || lseek(13, 0, SEEK_CUR) != 2 || read(5, &byte, 1) != 1 || byte != 'c' || lseek(13, 0, SEEK_CUR) != 3 ? 86 : 37; }` : ""}
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
 ${mode === "handles" ? `char bytes[2]; if (read(5, bytes, 2) != 2 || memcmp(bytes, "ab", 2) || lseek(6, 0, SEEK_CUR) != 2 || lseek(7, 0, SEEK_CUR)) return 83;
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
 ${queued && !["queue-drop", "queue-closed"].includes(mode!) ? `if ((received < 0 && (take(5, 'x', &received, 1, 0, 0) || close(5))) || ${received && mode !== "queue-counter" ? "read(received, &byte, 1) != 1 || byte != 'b'" : `read(received, &counter, 8) != 8 || counter != ${received ? 7 : 3}`} ${alias ? "|| fcntl(5, F_GETFD) || fcntl(12, F_GETFD) != FD_CLOEXEC || !(fcntl(5, F_GETFL) & O_APPEND) || !(fcntl(12, F_GETFL) & O_APPEND) || lseek(12, 0, SEEK_CUR) != 2" : "|| close(received)"}) return 86;` : "(void)byte; (void)received;"}
 ${alias ? "for (int fd = 6; fd < 64; fd++) if (fd != 12 && fd != 13 && (fcntl(fd, F_GETFD) != -1 || errno != EBADF)) return 86;" : ""}
 if (getpid() != syscall(SYS_gettid) ${mode === "identity" || mode === "peer" ? "|| identity != getpid()" : mode === "file" ? "|| close(temporary)" : ""}) return 74;
 ${mode === "handles" ? `if (fcntl(4, F_GETFD) != -1 || errno != EBADF || fcntl(8, F_GETFD) != FD_CLOEXEC ||
     fcntl(12, F_GETFD) != FD_CLOEXEC || !(fcntl(12, F_GETFL) & O_APPEND) || read(12, bytes, 1) != 1 || bytes[0] != 'c' ||
     lseek(6, 0, SEEK_CUR) || read(6, bytes, 1) != 1 || bytes[0] != 'a') return 85;` : ""}
 ${mode === "runtime" ? `heap = realloc(heap, 262144); if (!heap || local != 23 || pthread_kill(pthread_self(), SIGUSR1) || !delivered || future_stack(32) != 8448) return 80;
 for (unsigned i = 0; i < 65536; i++) if (heap[i] != 'H') return 81; free(heap);
 pthread_t thread; void *returned; if (pthread_create(&thread, NULL, future_thread, NULL) || pthread_join(thread, &returned) || returned != (void *)1) return 82;` : ""}
 ${mode === "mmap" ? `char value; if (mapping[0] != 'B' || mapping[4096] != 'A' || mapping[8192] != 'Z') return 76;
 for (unsigned i = 0; i < 3; i++) if (pread(5, &value, 1, i * 4096) != 1 || value != 'B') return 77;` : ""}
 printf("%cBC:%llu\\n", input[0], (unsigned long long)hash); ${alias ? 'fflush(stdout); if (close(3) || close(4)) return 86; execlp("worker", "worker", "verify", NULL); return 86;' : "return 37;"}
}
` });
			await compileBenchmarkHelper(fixture.workspace, { source: "launch.c", output: "launch", code: `#define _GNU_SOURCE
${cHeaders("unistd.h fcntl.h stdio.h string.h stdlib.h stdint.h sys/socket.h sys/eventfd.h sys/mman.h sys/wait.h")}
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
` });
			await commitBenchmarkFixture(fixture.workspace, "Live process continuation");
			const route = await fixture.prepareActorReplay(), scope = { sessionID: "live", turnID: "seed" };
			const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry<{ executable: string }>;
			const publishing = vi.spyOn(registry, "publish"), errors = vi.spyOn(fixture.backend as any, "setError");
			const prepared = patternProcessHost(fixture, scope.sessionID, { candidateLimit: 4, maxConcurrentActions: 4,
				patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 }) }), { events, start, call } = prepared;
			host = prepared.host;
			const execute = (turnID: string, command: string, signal = AbortSignal.timeout(10_000)) => host!.execute(call(turnID, command), signal,
				() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID }, signal }) },
					() => fixture.tool.execute(turnID, { command })));
			for (const turnID of ["common-1", "common-2"]) { await start(turnID); await execute(turnID, "printf common"); await host.finishTurn(turnID); }
			await start("seed"); const seed = await execute("seed", "printf seed; launch seed"); await host.finishTurn("seed");
			expect(seed.content).toMatchObject([{ type: "text", text: expect.stringMatching(/^seedABC:\d+\n$/) }]);
			const spawnStart = vi.mocked(childProcess.spawn).mock.calls.length;
			await start("prepared");
			const diagnostic = () => JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(), errors: errors.mock.calls.map(call => call[1]),
				certificates: publishing.mock.calls.map(([, , certificate]) => certificate && { complete: certificate.dependencyCertificate, continuation: certificate.result.continuation }),
				operations: events.filter(event => event.type === "operation_prediction") });
			const privatePid = await runningProcess(spawnStart, async (pid, syscall) => {
				if (mode === "early" || mode === "identity") {
					const usage = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => ""), fields = usage.slice(usage.lastIndexOf(")") + 2).split(" ");
					return /^(?:running|-1 )/.test(syscall) && Number(fields[11]) + Number(fields[12]) >= 40;
				}
				return syscall.startsWith(mode === "handles" ? "0 0x8 " : mode === "writev" ? "20 0x4 " : mode === "readv" ? "19 0x3 " : mode === "recvmsg" ? "47 0x3 " : "0 0x3 ");
			}).catch(error => { throw new Error(diagnostic(), { cause: error }); });
			if (cancelled) { await host.finishTurn("prepared", true); await expect.poll(() => existsSync(`/proc/${privatePid}`)).toBe(false); }
			const command = `printf actual; launch ${changed ? "changed" : "actual"}`;
			const inspectionStart = inspecting.mock.calls.length;
			const actual = cancelled ? await fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { ...scope, turnID: "later" } }) },
				() => fixture.tool.execute("later", { command })) : await execute("prepared", command);
			expect(actual.content, JSON.stringify(actual.content)).toEqual(seed.content.map(item => item.type === "text" ? { ...item,
				text: item.text.replace(/^seedA/, `actual${changed ? "D" : "A"}`) } : item));
			const resumed = !unavailable && !changed && !cancelled && !["identity", "peer", "file", "queue-peeked"].includes(mode!);
			expect(fixture.backend.actorMetrics().joinedHits, diagnostic()).toBe(Number(resumed));
			if (alias) expect(inspecting.mock.calls.slice(inspectionStart).some(([, , descriptors]) => [5, 13].every(slot => descriptors?.some(fd => fd.fd === slot && fd.alias === 5 && fd.offset === 2 && fd.owned)) && !descriptors?.some(fd => fd.fd === 12))).toBe(true);
			if (queued && resumed) expect(publishing.mock.calls.some(([, , certificate]) => certificate?.prototype.inheritedFDs.some(fd => fd.installed === false))).toBe(true);
			if (resumed) expect(publishing.mock.calls.some(([, , certificate, , continuation, computation]) => certificate?.result.continuation &&
				continuation && computation && computation.completedAt > continuation.computation.completedAt), diagnostic()).toBe(true);
			if (!cancelled) { if (!resumed) expect(existsSync(`/proc/${privatePid}`)).toBe(true); await host.finishTurn("prepared", true); }
			await expect.poll(() => existsSync(`/proc/${privatePid}`), { timeout: 5_000 }).toBe(false); // Retirement kills asynchronously, as for cancel.
			if (!cancelled) expect(consumedTiming(events, "prepared").hiddenComputeMs > 0, diagnostic()).toBe(resumed);
			if (resumed) expect(events.filter(event => event.type === "operation_prediction"), diagnostic()).toContainEqual(expect.objectContaining({ settlement: expect.objectContaining({
				observation: "observed", match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }) }) }));
			const binding = observations.get("prepared")?.bindings.find(candidate => registry.resolveBinding(candidate, scope)?.executable === path.join(fixture.workspace, "worker"));
			if (resumed) expect(binding, diagnostic()).toMatchObject({ available: true });
			if (resumed && ["pipe", "queue-received"].includes(mode!)) {
				const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
				for (const phase of ["foreign", "fresh", "changed", "cancel", "revoked"]) await fixture.workspaceSandbox.withWorkspace(fixture.workspace, async workspace => {
					if (phase === "revoked") { await fixture.backend.storage.maintain("clear"); expect(binding!.available).toBe(false); }
					const later = { sessionID: phase === "foreign" ? "foreign" : scope.sessionID, turnID: phase }, fresh = phase === "fresh";
					const session = await fixture.backend.open({ sourceRoot: fixture.workspace, workspace, invocation, scope: later });
					const spawnStart = vi.mocked(childProcess.spawn).mock.calls.length, preparing = session.executeBinding(binding!);
					void preparing.catch(() => undefined);
					try {
						if (phase === "foreign" || phase === "revoked") { await expect(preparing).rejects.toThrow("unavailable in this scope"); return; }
						const pid = await runningProcess(spawnStart, (_pid, syscall) => syscall.startsWith("0 0x3 "));
						if (phase === "cancel") await session.close();
						const before = fixture.backend.actorMetrics().joinedHits;
						const repeated = await TimelineInterval.measure(() => fixture.backend.observeBindings(later,
							() => executeProcess(fixture, route.executor, `launch ${phase === "changed" ? "changed" : "actual"}`, { scope: later, signal: AbortSignal.timeout(10_000) }), () => {}, false),
							() => observations.get(phase)!.computations);
						const timing = consumedTiming(repeated.computation);
						expect(repeated.output).toEqual({ output: `${phase === "changed" ? "D" : "A"}BC:7107042143181080257\n`, exitCode: 0 });
						expect([fixture.backend.actorMetrics().joinedHits - before, timing.hiddenComputeMs > 0], diagnostic()).toEqual([Number(fresh), fresh]);
						if (fresh) {
							expect(await preparing).toEqual({ output: [], suspended: true });
							expect(observations.get(phase)!.bindings).toEqual([session.executionBindings()[0]]);
						} else expect(observations.get(phase)!.bindings).toEqual([]);
						expect(existsSync(`/proc/${pid}`)).toBe(phase === "changed");
					} finally { await session.close(); await preparing.catch(() => undefined); }
				}, { liveLower: true });
			}
		} finally { observing.mockRestore(); inspecting.mockRestore(); await host?.dispose(); await fixture.dispose(); }
	});

nativeTest.for(["keyed", "unkeyed", "keyed-large", "unkeyed-opaque", "keyed-failed", "unkeyed-failed", "keyed-exit", "unkeyed-exit"])("transfers native Bash FD inputs to filesystem tools across turns with exact invalidation (%s)", { timeout: 30_000 }, async (mode, { skip, benchmark }) => {
		const fixture = await benchmark("pi-native-resource-inputs-");
		const unkeyed = mode.startsWith("unkeyed"), parentOutput = { transcript: mode.endsWith("large") ? "x".repeat(128 * 1024) : "", ...(mode.endsWith("opaque") ? { callback: () => "parent-only" } : {}) };
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		const search = await createClosedSearchProfile(fixture.workspace);
		try {
			if (!search.invocations.has("grep") || !search.invocations.has("find")) return skip("qualified search executors are unavailable");
			const file = path.join(fixture.workspace, "input.txt"); await writeFile(file, "one\ntwo\n");
			await mkdir(path.join(fixture.workspace, "entries"));
			for (const name of ["z.txt", "a.txt", "\ue000.txt", "\u{10000}.txt"]) await writeFile(path.join(fixture.workspace, "entries", name), name);
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: `${cHeaders("unistd.h stdio.h sys/syscall.h")}
int main(void) { char b[1024]; return syscall(SYS_getdents64, 4, b, sizeof(b)) < 0 || pread(3, b, 8, 0) != 8 || write(1, b, 8) != 8; }
` });
			await commitBenchmarkFixture(fixture.workspace, "Native resource input bridge"); await prepareLinuxProcessReuse(fixture);
			const route = await fixture.prepareActorReplay();
			const tools = [fixture.tool, createReadTool(fixture.workspace), createGrepTool(fixture.workspace), createLsTool(fixture.workspace), createFindTool(fixture.workspace)];
			const prepared = patternProcessHost(fixture, "native-inputs", { tools: tools.map(tool => tool.name), resourceCacheMaxEntries: 8,
				resourceCacheMaxBytes: 32 * 1024, patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false }) }, {
				executionWorlds: [fixture.world, createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { maxBytes: () => 1024 * 1024 })],
				resolveInvocation: (tool, input) => {
					const invocation = search.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath });
					return unkeyed && tool === "bash" ? { ...invocation!, semantics: { ...PI_ACTION_SEMANTICS.definition("bash")!, canonicalize: () => undefined } } : invocation;
				},
			}, tools), { events, start } = prepared;
			host = prepared.host;
			const error = new Error("native producer failed after completed work"), command = "exec 3<input.txt 4<entries; worker; worker" + (mode.endsWith("exit") ? "; exit 17" : "");
			const bash = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request, scope: { sessionID: "native-inputs", turnID: "seed" } }) }, async () => {
				const output = await fixture.tool.execute("seed", { command }); if (mode.endsWith("failed")) throw error; return { ...output, details: parentOutput };
			}));
			await start("seed");
			const result = host.execute({ turnID: "seed", id: "seed", tool: "bash", args: { command }, tools }, undefined, bash);
			if (mode.endsWith("failed")) await expect(result).rejects.toBe(error);
			else if (mode.endsWith("exit")) await expect(result).rejects.toThrow(/17/);
			else expect(await result).toEqual({ content: [{ type: "text", text: "one\ntwo\none\ntwo\n" }], details: parentOutput });
			await host.finishTurn("seed"); expect(bash).toHaveBeenCalledOnce();
			if (unkeyed) expect(events.find(event => event.type === "actor_action")).toMatchObject({ settlement: { provider: { kind: "actor", cause: { code: "action_not_keyable" } } } });
			await start("queries");
			for (const [tool, args, change] of [["read", { path: "input.txt", offset: 2 }, undefined], ["grep", { path: "input.txt", pattern: "two" }, undefined],
				["ls", { path: "entries" }, undefined], ["find", { path: "entries", pattern: "*.txt" }, undefined], ["read", { path: "input.txt" }, "new\nvalue\n"], ["ls", { path: "entries" }, "new.txt"]] as const) {
				if (change) await writeFile(tool === "read" ? file : path.join(fixture.workspace, "entries", change), change);
				const native = tools.find(value => value.name === tool)!, invocation = search.invocations.get(tool);
				const authoritative = (callID: string) => invocation?.authoritative
					? invocation.authoritative({ callID, args, signal: new AbortController().signal }).then(value => value.result) : native.execute(callID, args);
				const execute = vi.fn(() => authoritative("fallback"));
				expect(await host.execute({ turnID: "queries", id: change ? `stale-${tool}` : tool, tool, args, tools }, undefined, execute)).toEqual(await authoritative("oracle"));
				expect(execute, JSON.stringify(summarizeSpeculativeTrace(events))).not.toHaveBeenCalled();
			}
			await host.finishTurn("queries", true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 6, exactReuseHits: 0 });
		} finally { await host?.dispose(); await search.pool.dispose(); await fixture.dispose(); }
	});

	test.for(["git", "auto"] as const)("selects changed running inputs after unrelated hints and compares the predecessor state (%s)", { timeout: 20_000 }, async (driver, { skip, benchmark }) => {
		const fixture = await benchmark("pi-running-inputs-", driver);
		let pending: ReturnType<typeof forkReusableBash> | undefined, workspace: Parameters<typeof fixture.backend.open>[0]["workspace"] | undefined;
		let check: (() => Promise<boolean>) | undefined;
		const registry = Reflect.get(fixture.backend, "handoffs") as ProcessHandoffRegistry;
		const observe = registry.observeRunning.bind(registry), open = fixture.backend.open.bind(fixture.backend);
		const observing = vi.spyOn(registry, "observeRunning").mockImplementation((key, owner, invoke) => {
			const release = observe(key, owner, invoke); check = owner.advance; return release;
		});
		const hints = ["ignored.txt", "stable.txt", "input.txt"].map(name => path.join(fixture.workspace, name));
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(input => {
			workspace = input.workspace;
			return open({ ...input, workspace: { ...input.workspace, sourceChanges: () => ({ uncertain: false, paths: hints }) } });
		});
		try {
			for (const file of hints) await writeFile(file, "baseline");
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: `${cHeaders("fcntl.h unistd.h")}
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
			await commitBenchmarkFixture(fixture.workspace, "Running predecessor inputs");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { skip });
			pending = forkReusableBash(fixture, { label: "inputs", command: "printf predecessor > input.txt; worker",
				actionNamespace: "running-inputs", executionFingerprint });
			void pending.catch(() => {});
			await expect.poll(() => Boolean(workspace && existsSync(path.join(workspace.sandboxRoot, "ready"))), { timeout: 5000 }).toBe(true);
			expect(check).toBeDefined();
			await writeFile(hints[0]!, "unrelated"); await writeFile(hints[2]!, "predecessor");
			const opened = vi.spyOn(filesystem, "open"), reads = opened.mock.calls.length;
			try {
				expect(await check!()).toBe(true); // The child's transaction starts after the shell's write.
				await writeFile(hints[2]!, "after");
				// A bounded trace preview can lag the separate filesystem readiness signal.
				await expect.poll(() => check!(), { timeout: 2000 }).toBe(false);
				expect(opened.mock.calls.slice(reads).filter(([file]) => String(file) === hints[0])).toHaveLength(0);
				expect(fixture.backend.actorMetrics().lastError).toContain("actor_running_input_changed:");
			} finally { opened.mockRestore(); }
			await writeFile(path.join(workspace!.sandboxRoot, "release"), "");
			if (workspace && Reflect.get(workspace, "liveBase")) await expect(pending).rejects.toThrow("workspace changed since the sandbox started: input.txt");
			else expect((await pending).output.result.content).toEqual([{ type: "text", text: "predecessor" }]);
		} finally {
			if (workspace) await writeFile(path.join(workspace.sandboxRoot, "release"), "").catch(() => {});
			await (await pending?.catch(() => undefined))?.dispose();
			opening.mockRestore(); observing.mockRestore(); await fixture.dispose();
		}
	});

nativeTest.for(["completed", "running", "native", "native-merged", "native-closed-input", "native-descriptors", "native-null", "native-null-stdin", "native-status", "native-directory", "native-directory-entries", "native-directory-prepared-stale", "native-directory-opath", "native-directory-opath-prepared-stale", "native-pipe", "native-pipe-prepared-stale", "native-pipe-live", "native-pipe-live-prepared-stale", "native-pipe-live-mixed", "native-pipe-live-transfer", "native-pipe-producer", "native-pipe-producer-queued", "native-pipe-producer-short", "native-pipe-producer-splice", "native-socket", "native-socket-running", "native-socket-duplex", "native-socket-lifetime", "native-socket-io", "native-socket-messages", "native-socket-export", "native-socket-counter-export", "native-socket-prequeued", "native-socket-segmented", "native-socket-prefixed", "native-socket-orphan", "native-eventfd", "native-eventfd-semaphore", "native-socket-concurrent", "native-socket-half-closed", "native-socket-prepared-stale", "native-shared-table", "native-unshare", "native-prepared-stale", "native-prepared-restored"] as const)("reexecutes an owned child binding across turns without replaying its parent or stale input (%s)", { timeout: 20_000 }, async (mode, { skip, benchmark }) => {
		const fixture = await benchmark("pi-process-binding-", mode.includes("prepared-") ? "git" : undefined); // Borrowed immutable preparations.
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
		let host: ReturnType<typeof createSpeculativeActionHost> | undefined;
		let publication: ReturnType<typeof holdProcessPublication> | undefined;
		let restorePreparation: (() => void) | undefined;
		try {
			await writeFile(path.join(fixture.workspace, "input.txt"), "before\n");
			if (descriptors) await writeFile(path.join(fixture.workspace, "fd.txt"), "abcdef");
			await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: `#define _GNU_SOURCE
${cHeaders("errno.h poll.h sys/select.h sys/eventfd.h sys/syscall.h stdint.h sys/epoll.h fcntl.h stdlib.h string.h sys/socket.h sys/wait.h unistd.h")}
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
	${mode === "native" ? `{ int pair[2], status; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair)) return 201;
	pid_t child = fork(); if (child < 0) return 202;
	if (!child) _exit(fcntl(pair[0], F_SETFL, O_RDWR | O_NONBLOCK) ? 203 : 0);
	if (waitpid(child, &status, 0) != child || status) return 204;
	int flags = fcntl(pair[0], F_GETFL), peer = fcntl(pair[1], F_GETFL);
	if (flags < 0 || peer < 0 || !(flags & O_NONBLOCK) || (peer & O_NONBLOCK) || close(pair[0]) || close(pair[1])) return 205;
	}` : ""}
	for (volatile unsigned long iteration = 0; iteration < ${running ? 500000000 : native ? 50000000 : 0}ul; ++iteration) {}
	${orphan ? `{ if (fcntl(9, F_GETFD) != -1 || errno != EBADF || fcntl(15, F_GETFD) != -1 || errno != EBADF) return 187;
	int received[7]; char byte;
	for (int peek = 1; peek >= 0; peek--) {
		if (take(3, 'q', received, 7, peek, 0) || read(received[3], &byte, 1) != -1 || errno != EAGAIN) return 189;
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
	if (take(received[5], 'D', &peer, 1, 0, 0) || take(peer, 'C', &alias, 1, 0, 0) || pass(alias, "Z", 1, &received[6], 1, 0)) return 195;
	int root; if (take(peer, 'Z', &root, 1, 0, 0) || recv(root, &byte, 1, MSG_PEEK) != 1 || byte != 'b') return 196;
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
` });
			if (launcher) {
				await compileBenchmarkHelper(fixture.workspace, { source: "fd-launch.c", output: "fd-launch", arguments: ["-pthread", "-Werror"], code: `#define _GNU_SOURCE
${cHeaders("fcntl.h pthread.h sys/syscall.h stdlib.h errno.h stdint.h sys/eventfd.h sched.h string.h sys/socket.h sys/wait.h stdio.h unistd.h")}
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
	if (pass(channel[0], "C", 1, &channel[0], 1, 0) || pass(channel[1], "D", 1, &channel[1], 1, 0) ||
		pass(fds[1], "qbcaXYZ", 7, rights, 7, 0) || pass(fds[1], "T", 1, rights, 1, 0)) return 199;
	close(counter); close(queue[0]); close(queue[1]); close(channel[0]); close(channel[1]);` : ""}
	${prequeued && !orphan ? 'int passed[2] = {' + (queueCounter ? '20' : '9') + ', 3}; char control[CMSG_SPACE(sizeof(passed))] = {0}; struct iovec vector = {"qbcaXYZ", 7}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; struct cmsghdr *header = CMSG_FIRSTHDR(&message); header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(passed)); memcpy(CMSG_DATA(header), passed, sizeof(passed)); ' + (segmented ? 'vector.iov_base = "qI"; vector.iov_len = 2; if (write(fds[1], "L", 1) != 1 || sendmsg(fds[1], &message, 0) != 2) return 185; vector.iov_len = 1; vector.iov_base = "t"; if (sendmsg(fds[1], &message, 0) != 1 || write(fds[1], "bcaXYZ", 6) != 6) return 186;' : (prefixed ? 'if (write(fds[1], "L", 1) != 1) return 186; ' : '') + 'if (sendmsg(fds[1], &message, 0) != 7) return 157;') : ""}
	${duplex ? 'if (dup2(fds[1], 14) < 0 || write(0, "peer", 4) != 4) return 100;' : ""}
	${orphan ? 'if (close(9)) return 188;' : ""}
	${pipeOutput ? 'int output[2]; if (pipe2(output, O_CLOEXEC) || dup2(output[1], 14) < 0 || dup3(output[0], 15, O_CLOEXEC) < 0) return 95; ' + (shortWrite ? 'if (fcntl(14, F_SETPIPE_SZ, 4096) != 4096 || fcntl(14, F_SETFL, O_NONBLOCK)) return 121; ' : '') + (queuedOutput ? 'if (write(output[1], "start", 5) != 5) return 112; ' : '') + 'close(output[0]); close(output[1]);' : ""}
	${counter ? 'int counter = eventfd(5, EFD_CLOEXEC | EFD_NONBLOCK' + (semaphore ? ' | EFD_SEMAPHORE' : '') + '), full = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); uint64_t count = UINT64_MAX - 2; if (counter < 0 || full < 0 || write(full, &count, 8) != 8 || dup2(counter, 20) < 0 || dup2(counter, 21) < 0 || dup2(full, 22) < 0) return 149; close(counter); close(full);' : ""}
	${lifetime || exporting ? 'int gate[2]; if (pipe2(gate, O_CLOEXEC)) return 109;' : ""}
	puts(argv[1]); fflush(stdout);
	${enumerate ? 'int anchor = open(".", O_PATH | O_DIRECTORY); if (anchor < 0 || dup2(anchor, 6) < 0) return 175; if (anchor != 6) close(anchor); char entries[24]; int independent = open(".", O_RDONLY | O_DIRECTORY); if (independent < 0 || dup2(independent, 13) < 0 || syscall(SYS_getdents64, 10, entries, sizeof(entries)) != 24) return 168; if (independent != 13) close(independent); char captured[39]; for (int index = 0; index < 19; index++) snprintf(captured + 2 * index, 3, "%02x", (unsigned char)entries[index]); if (setenv("BOUND_DIRECTORY_ENTRY", captured, 1)) return 178; pid_t reader = fork(); if (reader < 0) return 169; if (reader) { int waited; if (waitpid(reader, &waited, 0) != reader || waited || lseek(10, 0, SEEK_CUR) <= 9007199254740991LL || lseek(11, 0, SEEK_CUR) != lseek(10, 0, SEEK_CUR) || lseek(13, 0, SEEK_CUR) <= 0 || lseek(13, 0, SEEK_CUR) == lseek(10, 0, SEEK_CUR)) return 170; return 0; }' : ""}
	${mode === "native-pipe-live-transfer" ? `int sockets[2], passed[] = {0, 3, 4, 8};
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) || pass(sockets[0], "x", 1, passed, 4, 0)) return 89;
	close(sockets[0]);` : ""}
	${counter ? 'pid_t child = fork(); if (child < 0) return 150; if (child) { int status; if (waitpid(child, &status, 0) != child || status) return 151; uint64_t count, total = 0; while (read(21, &count, 8) == 8) total += count; if (errno != EAGAIN || total != ' + (semaphore ? 11 : 7) + ' || !(fcntl(20, F_GETFL) & O_NONBLOCK) || read(22, &count, 8) != -1 || errno != EAGAIN) return 152; return 0; }' : ""}
	${pipe || socket ? 'pid_t child = fork(); if (child < 0) return 84; ' + (exporting ? 'if (child) { close(0); close(3); close(4); close(8); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 133; close(gate[1]); int status; if (waitpid(child, &status, 0) != child || status) return 134; char text[6], control[CMSG_SPACE(sizeof(int))] = {0}; struct iovec vector = {text, 5}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)}; if (recvmsg(14, &message, MSG_CMSG_CLOEXEC) != 5 || memcmp(text, "peerx", 5)) return 135; int received; memcpy(&received, CMSG_DATA(CMSG_FIRSTHDR(&message)), sizeof(received)); if (!(fcntl(received, F_GETFL) & O_NONBLOCK) || read(received, text, 6) != 6 || memcmp(text, "bcaXYZ", 6)) return 136; close(received); ' + (queueCounter ? 'uint64_t count; if (read(20, &count, 8) != 8 || count != 7) return 161; ' : '') + 'return 0; } ' : '') + (lifetime ? 'if (child) { close(0); close(3); close(4); close(8); close(14); close(fds[1]); close(gate[0]); if (write(gate[1], "x", 1) != 1) return 110; close(gate[1]); int status; return waitpid(child, &status, 0) != child || status; } ' : '') + 'if (child) { int status; char tail[3]; if (waitpid(child, &status, 0) != child || status || ' + (mixed ? 'lseek(16, 0, SEEK_CUR) != 4 || lseek(18, 0, SEEK_CUR) != 0 || pread(18, tail, 1, 2) != 1 || tail[0] != 81 || !(fcntl(3, F_GETFL) & O_NONBLOCK) || !(fcntl(4, F_GETFL) & O_NONBLOCK) || (fcntl(8, F_GETFL) & O_NONBLOCK) || ' : '') + 'read(3, tail, 1) != 1 || read(4, tail + 1, 1) != 1 || read(8, tail + 2, 1) != 1 || tail[0] != \'X\' || tail[1] != \'Y\' || tail[2] != \'Z\') return 85; ' + (orphan ? 'int remaining; if (take(3, 84, &remaining, 1, 0, 0) || lseek(remaining, 0, SEEK_CUR) != 2 || !(fcntl(remaining, F_GETFL) & O_NONBLOCK)) return 200; close(remaining); ' : '') + (socket || pipeOutput ? 'char reply[6]; ' + (shortWrite ? 'char head[4091]; if (read(15, head, sizeof(head)) != sizeof(head)) return 122; for (unsigned index = 0; index < sizeof(head); index++) if (head[index] != 90) return 123; ' : '') + (transferIO ? 'if (read(15, reply, 6) != 6 || memcmp(reply, "bcabca", 6)) return 131; ' : '') + (queuedOutput ? 'if (read(15, reply, 5) != 5 || memcmp(reply, "start", 5)) return 113; ' : '') + (duplex ? 'if (read(0, reply, 4) != 4 || memcmp(reply, "back", 4) || read(0, reply, 1) != 0) return 102; ' : 'if (read(' + (socket ? 'fds[1]' : '15') + ', reply, 5) != 5 || memcmp(reply, "' + (shortWrite ? 'ZZZZZ' : 'reply') + '", 5)) return 98; ') + (socket ? 'if (read(fds[1], reply, 1) != 0) return 99; ' : '') : '') + 'return 0; }' : ""}
	${lifetime || exporting ? 'char ready; close(gate[1]); if (read(gate[0], &ready, 1) != 1) return 111; close(gate[0]);' : ""}
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
			await commitBenchmarkFixture(fixture.workspace, "Bound process invocation");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { skip });
			const scope = { sessionID: "binding", turnID: "recorded" }, later = { ...scope, turnID: "prepared" };
			const command = (descriptors ? "exec 3<fd.txt; exec 4<&3; exec 8<fd.txt; IFS= read -r -N 1 discarded <&3; " : "") +
				(directory ? "exec 10<.; exec 11<&10; " : "") +
				(nullDevice ? (mode === "native-null-stdin" ? "exec 0<>/dev/null; exec 6<&0; " : "exec 6<>/dev/null; ") + "exec 7<&6; " : "") +
				"export BOUND_SECRET='private value'; " + (launcher ? "exec fd-launch parent" : "printf 'parent\\n'; exec -a bound-name worker 'private argument'") +
				(mode === "native-merged" ? " 2>&1" : mode === "native-closed-input" ? " 0<&-" : "");
			const route = await fixture.prepareActorReplay(!native);
			const invocation = resolvePiToolInvocation("bash", { command: "exit 92" }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
			let binding: ProcessExecutionBinding | undefined;
			if (!native) {
				for (const value of ["before", ...Array.from({ length: 8 }, (_, version) => `history-${version}`)]) {
					await writeFile(path.join(fixture.workspace, "input.txt"), `${value}\n`);
					const branch = await forkReusableBash(fixture, { command, label: value, actionNamespace: "binding", executionFingerprint, executionScope: scope });
					try {
						expect(branch.output).toMatchObject({ isError: false, result: { content: [{ text: `parent\n${value}\n` }] } });
						const invocation = resolvePiToolInvocation("bash", { command }, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!;
						const permission = PI_ACTION_SEMANTICS.buildKey("bash", { command }, fixture.workspace, "binding", { fingerprint: executionFingerprint, context: invocation })!;
						const binding = branch.operations![0]!;
						for (const operation of [{ binding: Object.freeze({ ...binding }), permission }, { binding, permission: { ...permission, key: "different action with colliding hash" } }]) {
							await expect(fixture.world.speculation.fingerprint!({ effect: "unbounded", requirements: PI_ACTION_SEMANTICS.definition("bash")!.requirements, tool: "bash",
								action: { ...permission, executionContext: { ...invocation, operation } } })).rejects.toThrow("binding is unavailable");
						}
					} finally { await branch.dispose(); }
				}
				[binding] = fixture.backend.executionBindings(later);
				expect(binding, JSON.stringify(fixture.backend.metrics())).toBeDefined();
				const history = publishing.mock.calls.map(([certificate]) => certificate).filter(certificate => certificate.weakKey === binding!.key), [certificate] = history;
				expect(certificate!.producer.execution.authority).toBe("speculative");
				expect(certificate!.dependencyCertificate.taints).toEqual(["clock", "random"]);
				expect(await fixture.backend.store.findByWeakKey(binding!.key, path.join(fixture.workspace, "worker"))).toEqual([]);
				expect(JSON.stringify(binding)).not.toContain("private argument");
				expect(JSON.stringify(binding)).not.toContain("private value");
				expect(new Set(history.map(certificate => certificate.id)).size).toBe(9);
				for (const certificate of history) await fixture.backend.store.put(certificate);
				await writeFile(path.join(fixture.workspace, "input.txt"), "after\n");
				await expect(validateTransferredProcessEvidence(certificate!.dependencyCertificate)).resolves.toMatchObject({ status: "stale" });
				const validation = vi.spyOn(await import("../src/provenance-validation.ts"), "validateDynamicDependencyCertificate");
				try {
					for (const value of ["after", "before", "history-3", "after"]) {
						await writeFile(path.join(fixture.workspace, "input.txt"), `${value}\n`); validation.mockClear();
						expect(await fixture.backend.bindingStale(binding!)).toBe(value === "after");
						expect(validation).toHaveBeenCalledOnce(); // All historical results share current evidence; every new call captures it again.
					}
				} finally { validation.mockRestore(); }

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
				await writeFile(path.join(fixture.workspace, "input.txt"), "after\n");
			}

			const prepared = patternProcessHost(fixture, scope.sessionID, { candidateLimit: 4, maxConcurrentActions: 6,
				patternAware: patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 }) }, {
				// This boundary fixture budgets two native preparations plus the Actor; single-CPU live transfers are tested above.
				resources: { initial: { cpuCount: 6, idleCpuCount: 6 }, sample: async () => ({ cpuCount: 6, idleCpuCount: 6 }) },
				preflight: ({ args, action }) => { expect(args).toHaveProperty("command"); expect(action.input.command).toBe((args as { command: string }).command); return true; },
			}), { events, patternStore, start, call } = prepared;
			host = prepared.host;
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
			// The previewed whole Bash command is adopted outright once its evidence validates; a native run still binds its child.
			expect(seedFallback).toHaveBeenCalledTimes(native ? 1 : 0);
			expect(fixture.backend.actorMetrics().hits, JSON.stringify({ actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics() })).toBe(0);
			binding ??= fixture.backend.executionBindings(later).at(-1);
			expect(binding, "a real native miss must retain its launch without publishing a result").toBeDefined();
			if (["completed", "native", "native-merged", "native-descriptors", "native-pipe-live"].includes(mode))
				expect(fixture.backend.operationHints(binding!).preparation).toBe(mode === "native" || mode === "native-merged" ? "current_workspace" : undefined);
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
			await writeFile(path.join(fixture.workspace, "input.txt"), "newest\n");
			const before = fixture.backend.metrics();
			const publicationStart = publishing.mock.calls.length;
			if (running) publication = holdProcessPublication(fixture.backend);
			if (mode.includes("prepared-")) {
				const fork = fixture.workspaceSandbox.fork.bind(fixture.workspaceSandbox);
				const borrowing = vi.spyOn(fixture.workspaceSandbox, "fork").mockImplementation(async options => {
					if (options.preparation) await writeFile(path.join(fixture.workspace, "input.txt"), "changed during preparation\n");
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
				await expect.poll(() => events.filter(event => event.type === "candidate" && event.turnID === "prepared" &&
					event.candidate.kind === "operation" && event.state.status === "succeeded").length).toBe(launcher ? 2 : 1);
				expect(await readFile(path.join(fixture.workspace, "input.txt"), "utf8")).toBe("changed during preparation\n");
				restorePreparation?.(); restorePreparation = undefined;
				// Every preparation is sealed before this change; no later producer can capture the replacement value.
				await writeFile(path.join(fixture.workspace, "input.txt"), mode === "native-prepared-restored" ? "newest\n" : "changed after preparation\n");
			}
			expect(fixture.backend.metrics().misses).toBeGreaterThan(before.misses);
			const changedParent = command.replace("parent", "automatic-parent");
			if (running) {
				expect((await executeProcess(fixture, route.executor, changedParent, { scope: { ...scope, turnID: "foreign" } })).output).toBe("automatic-parent\nnewest\n");
				expect(publication!.joined(), "another turn cannot wait for this one-shot producer").toBe(false);
			}
			const actor = vi.fn(() => fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { ...scope, turnID: "prepared" } }) }, () => fixture.tool.execute("prepared", { command: changedParent })));
			const nativeExecution = host.execute(call("prepared", changedParent), undefined, actor);
			void nativeExecution.catch(() => undefined);
			if (running) await expect.poll(() => publication!.joined() || { actor: fixture.backend.actorMetrics(), producer: fixture.backend.metrics() }).toBe(true);
			const stalePreparation = mode.endsWith("prepared-stale");
			expect((await nativeExecution).content).toEqual([{ type: "text", text: `automatic-parent\n${stalePreparation ? "changed after preparation\n" : "newest\n"}${suffix}` }]);
			expect(actor).toHaveBeenCalledOnce();
			if (segmented || orphan) expect(fixture.backend.actorMetrics().hits).toBe(1);
			if (orphan) expect(publishing.mock.calls.some(([certificate]) => certificate.prototype.inheritedFDs.some(fd => fd.installed === false))).toBe(true);
			expect(fixture.backend.actorMetrics().joinedHits, JSON.stringify({ joined: publication?.joined(), metrics: fixture.backend.actorMetrics() })).toBe(Number(running));
			await host.finishTurn("prepared");
			const timing = consumedTiming(events, "prepared");
			if (stalePreparation) expect(timing.hiddenComputeMs, JSON.stringify({ metrics: fixture.backend.actorMetrics(), operations: events.filter(event => event.type === "operation_prediction") })).toBe(0);
			else expect(timing.hiddenComputeMs,
				JSON.stringify({ metrics: fixture.backend.actorMetrics(), producer: fixture.backend.metrics(), bindings: fixture.backend.executionBindings(later), operations: events.filter(event => event.type === "operation_prediction") })).toBeGreaterThan(0);
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

nativeTest.for(["temporary", "persistent", "descendant", "merged", "stale"])("seals output pipe status before reusing a child (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
		const fixture = await benchmark("pi-output-status-", "git", {});
		await writeFile(path.join(fixture.workspace, "input.txt"), "before\n");
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: `${cHeaders("fcntl.h unistd.h")}
int main(void) {
	for (volatile unsigned long i = 0; i < 20000000ul; ++i) {}
	int fd = dup(${mode === "merged" ? 2 : 1}), flags = fcntl(fd, F_GETFL);
	if (fd < 0 || flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK)) return 71;
	${mode === "descendant" ? 'int child = fork(); if (child < 0) return 72; if (child) return 0;' : ""}
	${mode !== "persistent" ? 'if (fcntl(fd, F_SETFL, flags)) return 73;' : ""}
	char bytes[32]; int input = open("input.txt", O_RDONLY); if (input < 0) return 74;
	int length = read(input, bytes, sizeof(bytes)); return length < 0 || write(fd, bytes, length) != length;
}` });
		await commitBenchmarkFixture(fixture.workspace, "output status");
		const scope = { sessionID: "output-status", turnID: "learned" }, later = { ...scope, turnID: "prepared" };
		const { execute, binding, invocation, seed } = await learnWorker(fixture, scope, `worker${mode === "merged" ? " 2>&1" : ""} | cat`);
		expect(seed).toBe("before\n");
		await fixture.workspaceSandbox.withWorkspace(fixture.workspace, workspace =>
			withPreparedWorker(fixture, { sourceRoot: fixture.workspace, workspace, invocation, scope: later }, binding!, "before\n", async session => {
				await session.seal([]);
				if (mode === "stale") await writeFile(path.join(fixture.workspace, "input.txt"), "after\n");
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
			await compileBenchmarkHelper(fixture.workspace, { source: "locker.c", output: "locker", code: lockHelpers + `int main(int argc,char **argv) {
	if(argc!=2)return 70;
	if(argv[1][0]=='i')return lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB);
	return query(8,2,4,F_UNLCK,2,4) || query(8,10,3,F_RDLCK,10,3) || flock(8,LOCK_EX|LOCK_NB)!=-1 || errno!=EAGAIN;
}` });
		}
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: (locks ? lockHelpers : "") + `${cHeaders("unistd.h sys/wait.h fcntl.h")}
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
` });
		const body = `IFS= read -r -N 1 discard <&3; IFS= read -r -N 1 discard <&8; locker init; printf '%s\\n' "$1"; worker; IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&8; printf 'tail:%s:%s\\n' "$a" "$b"; locker check`;
		if (memory) {
			await compileBenchmarkHelper(fixture.workspace, { source: "fd-memory.c", output: "fd-memory", code: lockHelpers + `
${cHeaders("sys/mman.h sys/wait.h stdio.h")}
${rightsHelpers}
int main(int argc,char **argv) {
	char bytes[6];int input=open("input.txt",O_RDONLY);if(argc!=2 || input<0 || read(input,bytes,6)!=6)return 80;close(input);
	${mode === "locks-close-queues" ? 'int queues[2]; struct iovec vector = {bytes, 1}; struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1}; if (socketpair(AF_UNIX, SOCK_STREAM, 0, queues) || sendmsg(queues[0], &message, 0) != 1 || close(queues[0]) || close(queues[1])) return 88;' : ""}
	int fd=memfd_create("lock-input",0);if(fd<0 || write(fd,bytes,6)!=6 || lseek(fd,0,SEEK_SET)!=0 || dup2(fd,3)<0)return 81;
	if(fd!=3)close(fd);if(dup2(3,4)<0)return 82;fd=open("/proc/self/fd/3",O_RDWR);if(fd<0 || dup2(fd,8)<0)return 83;if(fd!=8)close(fd);
	${mode === "locks-close-nested" ? 'if (wrap_queue(wrap_queue(memfd_create("unrelated", MFD_CLOEXEC), 0), 0) < 0) return 88;' : ""}
	${closing ? `int gate[2];char byte;if(pipe2(gate,O_CLOEXEC) || lseek(3,1,SEEK_SET)!=1 || lseek(8,1,SEEK_SET)!=1 || lock(3,F_WRLCK,2,4) || flock(3,LOCK_SH|LOCK_NB))return 84;
	puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 85;
	if(!child){close(gate[1]);if(read(gate[0],&byte,1)!=1)_exit(86);close(gate[0]);execl("./worker","worker",(char *)0);_exit(86);}
	close(gate[0]);close(3);close(4);if(write(gate[1],"x",1)!=1)return 86;close(gate[1]);int status;
	if(waitpid(child,&status,0)!=child || status || read(8,&byte,1)!=1 || query(8,10,3,F_UNLCK,10,3) || flock(8,LOCK_EX|LOCK_NB) || flock(8,LOCK_UN))return 87;
	printf("tail::%c\\n",byte);return 0;` : `execl("/bin/bash","bash","-c",${JSON.stringify(body)},"fd-memory",argv[1],(char *)0);return 84;`}
}` });
		}
		await commitBenchmarkFixture(fixture.workspace, "Inherited FD binding");
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
						expect(await readFile(alias, "utf8")).toBe(writable ? `${changed ? "uvwx" : "abcd"}XY` : pathWrite ? `${changed ? "uv" : "ab"}Z${changed ? "xyz" : "def"}` : changed ? "uvwxyz" : "abcdef");
						if (mode === "hardlink-rename" || mode === "hardlink-unlink") expect(existsSync(input)).toBe(false);
						if (mode === "hardlink-rename") expect((await stat(path.join(fixture.workspace, "moved.txt"))).ino).toBe((await stat(alias)).ino);
						if (mode === "hardlink-split") expect(await readFile(input, "utf8")).toBe("new");
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
		await writeFile(path.join(fixture.workspace, "input.txt"), "abcdef");
		const helpers = `#define _GNU_SOURCE
${cHeaders("sys/socket.h sys/wait.h poll.h errno.h fcntl.h stdio.h string.h unistd.h")}
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
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker", code: helpers + `int main(void) {
	char bytes[8];struct pollfd ready={.fd=22,.events=POLLIN};
	if(passPacket("",0) || read(22,bytes,0)!=0 || poll(&ready,1,0)!=1 || takePacket(0,1,0,0) || takePacket(0,0,0,'a'))return 70;
	if(write(21,"abcdef",6)!=6 || read(22,bytes,2)!=2 || memcmp(bytes,"ab",2))return 71;
	if(send(20,"XY",2,0)!=2 || recv(22,bytes,8,0)!=2 || memcmp(bytes,"XY",2))return 72;
	struct iovec vector={bytes,0};struct msghdr message={.msg_iov=&vector,.msg_iovlen=1};
	if(send(20,"truncate",8,0)!=8 || recv(22,bytes,2,MSG_PEEK|MSG_TRUNC)!=8 || memcmp(bytes,"tr",2) || recvmsg(22,&message,MSG_TRUNC)!=8 || !(message.msg_flags&MSG_TRUNC))return 74;
	if(passPacket("pqrs",4) || takePacket(2,0,1,'b') || recv(22,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN || passPacket("tail",4))return 73;
	if(shutdown(20,SHUT_WR) || send(20,"x",1,MSG_NOSIGNAL)!=-1 || errno!=EPIPE || send(22,"back",4,0)!=4 || recv(20,bytes,8,0)!=4 || memcmp(bytes,"back",4) || shutdown(20,SHUT_RD))return 75;
	if(${type === "SOCK_DGRAM" ? "recv(20,bytes,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(20,bytes,1,0)!=0"})return 75;
	${closing ? "if(close(20) || close(21))return 76;" : ""}
	ready.revents=0;if(poll(&ready,1,0)!=1 || ready.revents!=${type === "SOCK_DGRAM" ? "POLLIN" : "(POLLIN|POLLHUP)"})return 77;
	for(volatile unsigned long i=0;i<200000000ul;i++){}
	return write(1,"packets\\n",8)!=8;
}
` });
		await compileBenchmarkHelper(fixture.workspace, { source: "fd-launch.c", output: "fd-launch", code: helpers + `int main(int argc,char **argv) {
	if(argc!=2)return 80;int pair[2],gate[2];char byte;
	if(socketpair(AF_UNIX,${type}|SOCK_CLOEXEC|SOCK_NONBLOCK,0,pair) || dup2(pair[0],20)<0 || dup2(pair[0],21)<0 || dup2(pair[1],22)<0)return 81;
	close(pair[0]);close(pair[1]);int fd=open("input.txt",O_RDONLY);if(fd<0 || dup2(fd,9)<0)return 82;if(fd!=9)close(fd);
	if(pipe2(gate,O_CLOEXEC))return 83;puts(argv[1]);fflush(stdout);pid_t child=fork();if(child<0)return 83;
	if(!child){close(gate[1]);if(read(gate[0],&byte,1)!=1)_exit(84);close(gate[0]);execl("./worker","worker",(char *)0);_exit(84);}
	close(gate[0]);${closing ? "close(20);close(21);" : ""}if(write(gate[1],"x",1)!=1)return 84;close(gate[1]);
	int status;if(waitpid(child,&status,0)!=child || status || lseek(9,0,SEEK_CUR)!=2 || takePacket(4,0,0,'c') || lseek(9,0,SEEK_CUR)!=3)return 85;
	if(${type === "SOCK_DGRAM" ? "recv(22,&byte,1,MSG_DONTWAIT)!=-1 || errno!=EAGAIN" : "recv(22,&byte,1,0)!=0"})return 86;
	return 0;
}
` });
		await commitBenchmarkFixture(fixture.workspace, "Packet resource graph");
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
		await compileBenchmarkHelper(root, { source: fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)), output: "helper", arguments: ["-pthread", "-Werror"] });
		const boundary = await LinuxHeldExecBoundary.open({ storeRoot: root, binary });
		try {
			for (const stop of ["waiter", "preparing", "owner"]) {
				const gate = path.join(root, `probe-${stop}`), marker = gate + "-pid", helper = gate + "-helper", storeRoot = gate + "-store";
				await writeFile(helper, `#!/bin/sh\nif [ "$1" = "--skip-code" ]; then echo $$ > '${marker}'; while [ ! -f '${gate}' ]; do sleep .01; done; fi\nexec '${binary}' "$@"\n`, { mode: 0o700 });
				const backend = new LinuxProcessReuseBackend({ storeRoot, heldExecBinary: helper }), host = adaptProcessToolOperations(createLocalBashOperations({ shellPath: "/bin/bash" }));
				const create = () => new ProcessExecutionCoordinator(host, { enabled: () => true,
					prepare: (refresh, signal) => backend.prepareActorReplay(host, { sourceRoot: root, invocation: () => undefined,
						held: { realShell: "/bin/bash", executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) } }, refresh || stop === "preparing", signal),
					reset: () => backend.resetActorReplay() });
				let coordinator = create(); const controller = new AbortController();
				const execute = async (signal?: AbortSignal) => { let output = "";
					try { await coordinator.operations.exec("printf ready", root, { signal, env: {}, onData: data => { output += data.toString(); } }); return output; }
					catch (error) { if (signal?.aborted) return "aborted"; throw error; }
				};
				try {
					await backend.store.put(PROCESS_CERTIFICATE); const first = execute(controller.signal);
					await expect.poll(() => existsSync(marker)).toBe(true); const pid = Number(await readFile(marker, "utf8")), second = execute();
					if (stop !== "owner") {
						controller.abort(); expect(await scheduling.waitForCompletion(first, undefined, 500)).toEqual({ status: "completed", value: "aborted" });
						expect((await scheduling.waitForCompletion(second, undefined, 30)).status).toBe("deadline"); expect(() => process.kill(pid, 0)).not.toThrow();
						await writeFile(gate, ""); expect(await second).toBe("ready");
					} else {
						expect((await scheduling.waitForCompletion(coordinator.dispose(), undefined, 500)).status).toBe("completed");
						expect(await Promise.all([first, second])).toEqual(["ready", "ready"]); coordinator = create(); await writeFile(gate, "");
					}
					expect(() => process.kill(pid, 0)).toThrow(); expect((await coordinator.refreshActorRoute()).state).toBe("ready"); expect(await execute()).toBe("ready");
				} finally { await coordinator.dispose(); await backend.dispose(); }
				expect((await filesystem.readdir(storeRoot)).filter(name => name.endsWith(".sock"))).toEqual([]);
			}
			const run = (...args: string[]) => childProcess.spawnSync(binary, args, { encoding: "utf8", timeout: 1_000 });
			const passThrough = "printf out; printf err >&2; exit 7";
			const direct = childProcess.spawnSync("/bin/bash", ["-c", passThrough], { encoding: "utf8" });
			expect(run("/bin/bash", "-c", passThrough)).toMatchObject({ status: direct.status, signal: direct.signal, stdout: direct.stdout, stderr: direct.stderr });
			expect(run("--skip-code", "42", "/bin/bash", "-c", "exec /bin/sleep 5").status).toBe(42);
			expect(childProcess.spawnSync("/bin/bash", ["-c", "grep '^TracerPid:' /proc/self/status"], { encoding: "utf8" }).stdout).toMatch(/\t0\n$/);
			expect(run("/bin/bash", "-c", "grep '^TracerPid:' /proc/self/status").stdout).not.toMatch(/\t0\n$/);
			expect(run("/bin/bash", "-c", "(sleep 0.05; kill -CONT $$) & kill -STOP $$; printf resumed")).toMatchObject({ status: 0, stdout: "resumed" });
			const text = `bound-name\nliteral ' $ value\nprivate value\n${root}\nstdin\n`;
			for (const [route, stdout, stderr] of [["12", text, "stderr"], ["11", text + "stderr", ""], ["21", "stderr", text], ["22", "", text + "stderr"]]) {
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
						expect(await readFile(queueReport, "utf8")).toMatch(/^OFD 1\n0 (?:0|2) 0 \d+ \d+\nRUNNING \d+\n$/);
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
			const command = `/usr/bin/setsid /bin/sh -c 'echo $$ > ${pidFile}; sleep 1; echo done > ${completed}' </dev/null >/dev/null 2>&1 & exit 7`;
			expect(childProcess.spawnSync(binary, ["/bin/bash", "-c", command], { timeout: 900 }).status, "the shell returns without its detached descendants").toBe(7);
			await expect.poll(() => readFile(completed, "utf8").catch(() => ""), { timeout: 5000 }).toBe("done\n"); // Released, not killed with the tracer.
			await rm(pidFile);
			const tracer = childProcess.spawn(binary, ["/bin/bash", "-c", command.replace("sleep 1", "sleep 10").replace("exit 7", "wait")]);
			const stopped = new Promise<void>((resolve) => tracer.once("close", () => resolve()));
			let pid = 0;
			try {
				await expect.poll(async () => pid = Number(await readFile(pidFile, "utf8").catch(() => "")), { timeout: 1000 }).toBeGreaterThan(0);
				tracer.kill("SIGKILL");
				await stopped;
				await expect.poll(async () => (await readFile(`/proc/${pid}/stat`, "utf8").catch(() => "")).match(/\) (\w) /)?.[1],
					{ timeout: 1000, message: "killed tracer must not release its tracees to run" }).toSatisfy(state => state === undefined || state === "Z");
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
			type Replay = Pick<Extract<HeldExecDecision, { kind: "replay" }>, "descriptorOffsets" | "resourceEvents" | "output">;
			const replay = (mode: string, decide: (process: HeldExecProcess) => Promise<Partial<Replay> | undefined>,
				options: { descriptors?: Parameters<typeof held>[0]["descriptors"]; executor?: typeof native; beforeCommit?: (process: HeldExecProcess) => Promise<void> } = {}) => {
				const failed = mode.endsWith("commit-failure"), controller = new AbortController(), adopted = vi.fn(), settle = vi.fn();
				const commit = vi.fn(async (process: HeldExecProcess) => { await options.beforeCommit?.(process); if (failed) throw new Error("injected commit failure"); });
				const executor = held({ descriptors: options.descriptors ?? true, decide: async process => {
					if (await filesystem.readlink(`/proc/${process.pid}/exe`) !== "/usr/bin/true") return { kind: "continue" };
					const decision = await decide(process); if (!decision) return { kind: "continue" };
					if (mode === "cancel") controller.abort();
					return { kind: "replay", exitCode: 0, output: [{ fd: 1, data: Buffer.from("replayed:") }],
						commit: () => commit(process), adopted, settle, ...decision };
				} }, options.executor);
				return { adopted, settle, verify: async (command: string, output: string, commits: number) => {
					const running = executor.execute(command, { signal: controller.signal });
					if (failed) await expect(running, mode).rejects.toMatchObject({ disposition: "poisoned" });
					else if (mode === "cancel") await expect(running, mode).rejects.toThrow();
					else expect(await running, mode).toEqual({ exitCode: 0 });
					expect(executor.output, mode).toBe(failed || mode === "cancel" ? "" : output);
					expect(commit, mode).toHaveBeenCalledTimes(commits);
				} };
			};
			const descriptorProbe = path.join(root, "descriptor-probe");
			await compileBenchmarkHelper(root, { source: "descriptor-probe.c", output: "descriptor-probe", arguments: ["-pthread", "-Werror"], code: `#define _GNU_SOURCE
${cHeaders("pthread.h sched.h stdlib.h poll.h stdio.h stdint.h sys/file.h sys/stat.h sys/syscall.h sys/wait.h sys/eventfd.h sys/mman.h")}
${rightsHelpers}
static int packet_capture(char **argv) {
	int pair[2],offset=0;char bytes[4];
	if(socketpair(AF_UNIX,atoi(argv[1])|SOCK_NONBLOCK,0,pair) || send(pair[0],"",0,0)!=0 || send(pair[0],"tail",4,0)!=4)return 2;
	pid_t child=fork();if(child<0)return 3;if(!child){execl("/bin/true","true",(char *)0);_exit(4);}
	int status;if(waitpid(child,&status,0)!=child || status)return 5;
	/* If capture peeked the zero-length skb, the first offset peek skips it. */
	if(setsockopt(pair[1],SOL_SOCKET,SO_PEEK_OFF,&offset,sizeof(offset)) || recv(pair[1],bytes,1,MSG_PEEK)!=0 || recv(pair[1],bytes,1,MSG_PEEK)!=1 || bytes[0]!='t')return 6;
	offset=-1;if(setsockopt(pair[1],SOL_SOCKET,SO_PEEK_OFF,&offset,sizeof(offset)) || recv(pair[1],bytes,4,0)!=0 || recv(pair[1],bytes,4,0)!=4 || memcmp(bytes,"tail",4))return 7;
	return 0;
}
static int pipe_probe(char **argv) {
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
static int message_probe(char **argv) {
	int fds[2]; if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) || fds[0] != 3 || fds[1] != 4) return 71;
	int input = open(argv[1], O_RDONLY); if (input < 0 || dup2(input, 9) < 0) return 72; close(input);
	int passed[2] = {9, 3};
	if (write(4, "L", 1) != 1 || pass(4, "qI", 2, passed, 2, 0)) return 73;
	if (pass(4, "t", 1, passed, 2, 0) || write(4, "bcaXYZ", 6) != 6) return 74;
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
static int queue_owners(char **argv) {
	int state = atoi(argv[2]), local = (state >= 3 && state <= 5) || state >= 11, other = state == 2 || state == 9 || state == 10, data[2], channel[2], gate[2], fd; char type = *argv[1], byte; uint64_t value = 5;
	if (type == 's') { if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, data)) return 71; fd = data[0]; }
	else if (type == 'c') fd = eventfd(5, EFD_NONBLOCK | EFD_CLOEXEC);
	else { fd = memfd_create("queued-file", MFD_CLOEXEC); if (write(fd, &value, 8) != 8 || lseek(fd, 0, SEEK_SET)) return 71; }
	if (fd < 0 || dup2(fd, 20) < 0 || dup2(fd, 21) < 0) return 72;
	close(fd);
	if (type == 'f' && local && flock(20, LOCK_EX | LOCK_NB)) return 72;
	if (state) {
		int reference = other ? (type == 'c' ? eventfd(5, EFD_CLOEXEC) : open("/proc/self/fd/20", O_RDWR | O_CLOEXEC)) : 20;
		if (state >= 7) reference = wrap_queue(other ? reference : dup(reference), state == 10 || state == 12);
		if (reference < 0 || socketpair(AF_UNIX, SOCK_STREAM | (local || state == 7 ? 0 : SOCK_CLOEXEC), 0, channel) || pass(channel[0], "x", 1, &reference, 1, 0)) return 73;
		if (state == 2 || state >= 7) close(reference);
		if (state == 6) { if (take(channel[1], 'x', &reference, 1, 0, 0)) return 74; close(reference); }
	}
	if (pipe2(gate, O_CLOEXEC)) return 75;
	pid_t child = fork(); if (child < 0) return 75;
	if (!child) { close(gate[1]); if (read(gate[0], &byte, 1) != 1) return 76; close(gate[0]); execl("/bin/true", "true", (char *)0); return 76; }
	if (state) { close(20); close(21); } if (local) { close(channel[0]); close(channel[1]); }
	close(gate[0]); if (write(gate[1], "x", 1) != 1) return 77; close(gate[1]);
	int status; if (waitpid(child, &status, 0) != child || status) return 78;
	if (type == 's') { struct pollfd ready = {.fd = data[1], .events = POLLIN | POLLRDHUP}; if (poll(&ready, 1, 0) < 0 || ((state == 1 || state >= 7) && ready.revents)) return 79; }
	if (state && !local && state != 6) { if (take(channel[1], 'x', &fd, 1, 0, 0)) return 80; } else fd = 20;
	if (state >= 7 && !local) { int nested = fd, rights[2]; if (take(nested, 'x', rights, state == 10 ? 2 : 1, 0, 0)) return 80; close(nested); fd = rights[0]; if (state == 10) close(rights[1]); }
	if (type == 's') { if (state != 6) close(fd); struct pollfd ready = {.fd = data[1], .events = POLLIN | POLLRDHUP}; return poll(&ready, 1, 0) != 1 || read(data[1], &byte, 1) != 0; }
	if (local) value = 0; else if (type == 'f') value = lseek(fd, 0, SEEK_CUR); else if (read(fd, &value, 8) != 8) return 81;
	printf("%llu", (unsigned long long)value); return 0;
}
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
	const struct { const char *name; int argc; int (*run)(char **); } probes[] = {{"packet", 3, packet_capture}, {"pipe", 3, pipe_probe}, {"message", 4, message_probe}, {"queue", 4, queue_owners}};
	for (unsigned index = 0; argc > 1 && index < sizeof(probes) / sizeof(*probes); index++) if (!strcmp(argv[1], probes[index].name)) return argc == probes[index].argc ? probes[index].run(argv + 1) : 70;
	if (argc >= 3 && !strncmp(argv[1], "external-", 9)) {
		int pair[2];
		if ((!strcmp(argv[1], "external-socket") ? socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair) : pipe2(pair, O_CLOEXEC)) ||
			write(pair[1], "abc", 3) != 3 || close(pair[1]) || dup2(pair[0], 0) < 0 || close(pair[0])) return 99;
		execv(argv[2], argv + 2); return 100;
	}
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
			int passed[] = {3, 3, 8};
			if (pass(sockets[0], &byte, 1, passed, 3, strstr(argv[1], "batch") ? -1 : 0)) return 97;
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
			if (take(sockets[1], 'x', received, 3, 0, strstr(argv[1], "batch") ? 1 : strstr(argv[1], "failed") ? -1 : 0)) return 103;
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
		int sockets[2], fd = 3;
		if (socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) < 0 || pass(sockets[0], "x", 1, &fd, 1, 0)) return 74;
		close(sockets[0]); close(sockets[1]);
	} else if (!strncmp(argv[1], "thread", 6)) {
		pthread_t worker;
		if (pthread_create(&worker, 0, thread, strcmp(argv[1], "thread") ? argv : 0)) return 77;
		if (!strcmp(argv[1], "thread-exit")) pthread_exit(0);
		if (pthread_join(worker, 0)) return 77;
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
` });
			for (const type of [2, 5]) {
				const executor = held({ descriptors: true, decide: async () => ({ kind: "continue" }) });
				expect(await executor.execute(`exec '${descriptorProbe}' packet ${type}`)).toEqual({ exitCode: 0 });
			}
			// A vfork parent cannot stop until its child execs: a sibling's exec decision must not wait for it (cargo build scripts hung).
			await compileBenchmarkHelper(root, { source: "spawner.c", output: "spawner", code: `#include <sys/syscall.h>\n#include <sys/wait.h>\n#include <time.h>\n#include <unistd.h>
int main(void) { int status; struct timespec pause = {0, 300000000}; pid_t sibling = fork(), child;
	if (!sibling) { usleep(50000); execl("/bin/true", "true", (char *)0); _exit(1); }
	if (!(child = vfork())) { syscall(SYS_nanosleep, &pause, 0); execl("/bin/true", "true", (char *)0); _exit(1); }
	return waitpid(child, &status, 0) != child || status || waitpid(sibling, &status, 0) != sibling || status; }\n` });
			expect(await held({ descriptors: true, decide: async () => ({ kind: "continue" }) }).execute("exec ./spawner")).toEqual({ exitCode: 0 });
			const compat = path.join(root, "compat32");
			await writeFile(`${compat}.s`, ".global _start\n_start: movl $1, %eax; movl $7, %ebx; int $0x80\n");
			execFileSync("cc", ["-nostdlib", "-m32", "-static", `${compat}.s`, "-o", compat]);
			if (childProcess.spawnSync(compat).status === 7) for (const descriptors of [false, true]) {
				const decide = vi.fn(async () => ({ kind: "continue" as const }));
				const executor = held({ descriptors, decide });
				expect(await executor.execute(`exec '${compat}'`)).toEqual({ exitCode: 7 });
				expect(decide).not.toHaveBeenCalled();
			}
			for (const [redirection, route] of [["", [1, 2]], ["2>&1", [1, 1]], ["3>&1", undefined], ["0<&-", [1, 2]], ["1>/dev/null", [0, 2]], ["2>&1 1>/dev/null", [0, 2]],
				["&>/dev/null", [0, 0]], ["2>>/dev/null", [1, 0]], ["1>&-", undefined]] as const) {
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
				// A sibling stopped inside a call (the read builtin's ioctl) leaves ownership unproven by design: let it finish first.
				expect(await inspecting.execute(`exec 3<'${input}'; exec 4<&3; exec 5<'${input}'; ` +
					`(while IFS= read -r -N 1 value <&4; do :; done) & wait; /bin/true; /bin/true`)).toEqual({ exitCode: 0 });
				expect(failures).toEqual([]); expect(snapshots).toHaveLength(2);
			}
			for (const type of ["pipe", "socket"] as const) for (const descriptors of [true, "inspect"] as const) {
				await writeFile(external, `#!/bin/sh\nexec '${descriptorProbe}' external-${type} '${binary}' "$@"\n`, { mode: 0o700 });
				const snapshots: NonNullable<HeldExecProcess["descriptors"]>[] = [], failures: unknown[] = [], commit = vi.fn(async () => {});
				const executor = held({ descriptors: () => descriptors, decide: async process => {
					try {
						const captured = process.descriptors!; snapshots.push(captured);
						await inspectHeldExecProcess(process.pid, await filesystem.readlink(`/proc/${process.pid}/exe`), captured);
						const input = captured.find(({ fd }) => fd === 0)!;
						return { kind: "replay", exitCode: 0, output: [], commit, descriptorOffsets: [{ ...input, before: 0, after: 1, content: Buffer.from("abc") }] };
					} catch (error) { failures.push(error); return { kind: "continue" }; }
				} }, adaptProcessToolOperations(createLocalBashOperations({ shellPath: external })));
				expect(await executor.execute('/bin/true; IFS= read -r -N 3 value; printf "%s" "$value"')).toEqual({ exitCode: 0 });
				expect(failures, `${type}:${descriptors}`).toEqual([]);
				expect(snapshots).toHaveLength(1);
				expect(snapshots[0]).toMatchObject([{ fd: 0, type, owned: false, queueHex: "616263" }]);
				expect(commit).not.toHaveBeenCalled(); expect(executor.output).toBe("abc");
			}
			await writeFile(external, `#!/bin/sh\nexec 9<'${input}'\nexec '${binary}' "$@"\n`, { mode: 0o700 });
			for (const mode of ["lock", "export", "rights", "rights-batch", "rights-failed", "rights-unowned", "rights-orphan", "rights-pipe-orphan", "pidfd", "table", "thread", "thread-exec", "shared-table", "shared-exec", "overlap",
				"unshare", "unshare-noop", "range-close", "range-cloexec", "range-invalid"]) {
				let snapshot: HeldExecProcess["descriptors"];
				const split = mode.startsWith("unshare") || mode.startsWith("range-");
				const imported = mode.startsWith("rights") || mode === "pidfd";
				const unknown = mode === "rights-unowned";
				const shared = mode === "unshare-noop" || mode === "range-invalid", slots: number[][] = [];
				const ownership: boolean[] = [];
				const executor = replay(mode, async process => {
					snapshot = process.descriptors;
					slots.push(snapshot!.map(({ fd }) => fd));
					ownership.push(snapshot!.find(({ fd }) => fd === 3)!.owned);
					return mode === "export" || split || imported ? { descriptorOffsets: snapshot!.map(({ offset, ...descriptor }) =>
						({ ...descriptor, before: offset, after: Number(offset) + 1, ...(descriptor.type === "pipe" ? { content: Buffer.from(descriptor.queueHex!, "hex") } : {}) })), output: [] } : undefined;
				}, { executor: mode === "rights-unowned" ? adaptProcessToolOperations(createLocalBashOperations({ shellPath: external })) : native });
				await executor.verify(`exec 3<'${input}'; '${descriptorProbe}' ${mode} '${input}'; result=$?; IFS= read -r -N 1 byte <&3; printf '%s' "$byte"; exit "$result"`,
					split ? "c" : imported && !unknown ? "b" : "a", split ? 2 : imported && !unknown ? 1 : 0);
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
			await replay("output pipe", async process => ({ output: [{ fd: 1, data: Buffer.alloc(1024 * 1024, "x") }],
				descriptorOffsets: process.descriptors!.map(({ offset, ...descriptor }) => ({ ...descriptor, before: offset, after: Number(offset) + 1 })) }))
				.verify(`exec 3<'${input}'; /bin/true | /usr/bin/wc -c; IFS= read -r -N 1 byte <&3; printf '%s' "$byte"`, "1048576\nb", 1);
			for (const mode of ["partial", "empty", "flags", "queue-conflict", "contents", "overrun", "stale", "live", "packet", "inspect", "commit-failure", "journal", "journal-conflict", "journal-write-readonly", "journal-shutdown-pipe", "journal-commit-failure"]) {
				const accepted = ["partial", "empty", "flags", "journal"].includes(mode), journal = mode.startsWith("journal"), failedCommit = mode.endsWith("commit-failure");
				let descriptors: HeldExecProcess["descriptors"];
				const executor = replay(mode, async process => {
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
					return { descriptorOffsets,
						...(journal ? { resourceEvents: [{ fd: 3, kind: "peek" as const, data: Buffer.from("abc") }, { fd: 0, kind: mode === "journal-write-readonly" ? "produce" as const : mode === "journal-shutdown-pipe" ? "shutdown" as const : "consume" as const, data: Buffer.from("ab") },
							{ fd: 8, kind: "consume" as const, data: Buffer.from(mode === "journal-conflict" ? "x" : "c") }] } : {}) };
				}, { descriptors: mode === "inspect" ? () => "inspect" : true });
				await executor.verify(`'${descriptorProbe}' pipe ${mode}`, accepted ? `replayed:${mode === "empty" ? "" : "def"}` : mode === "stale" ? "bcdef" : "abcdef", Number(accepted || failedCommit));
				if (accepted) expect(descriptors, mode).toMatchObject([{ fd: 0, alias: 0, type: "pipe", owned: true }, { fd: 3, alias: 0, type: "pipe", owned: true }, { fd: 8, alias: 8, type: "pipe", owned: true }]);
			}
			for (const orphan of [false, true]) for (const mode of ["messages", "start", "end", "rights", "commit-failure", "cancel"]) {
				const pinCounts: number[] = [];
				const executor = replay(mode, async process => {
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
					return { descriptorOffsets };
				});
				await executor.verify(`'${descriptorProbe}' message '${input}' ${Number(orphan)}`, `${mode === "messages" ? "replayed:replayed:" : ""}LqItbcaXYZ`, mode === "messages" ? 2 : Number(mode === "commit-failure"));
				if (pinCounts.length === 2) expect(pinCounts[1]).toBe(pinCounts[0]);
			}
			for (const scenario of ["counter", "value", "identity", "alias", "overflow", "release", "commit-failure",
				"counter:queued", "counter:other", "counter:local", "counter:partial", "counter:drained", "counter:nested", "counter:external-nested", "counter:external-other", "counter:external-cycle", "counter:discarded", "counter:discard-nested", "counter:discard-cycle", "counter:peer-closed", "file:queued", "file:other", "file:local", "file:partial", "file:drained", "file:nested", "file:external-nested", "file:external-other", "file:external-cycle", "file:discarded", "file:discard-nested", "file:discard-cycle", "file:peer-closed", "socket:queued", "socket:consumed", "socket:external-nested"]) {
				const [mode, queue] = scenario.split(":"), type = mode === "file" ? "file" : mode === "socket" ? "socket" : "counter";
				const state = [undefined, "queued", "other", "local", "partial", "drained", "consumed", "nested", "external-nested", "external-other", "external-cycle", "discard-nested", "discard-cycle"].indexOf(queue === "discarded" || queue === "peer-closed" ? "drained" : queue), local = state >= 3 && state <= 5 || state >= 11;
				const accepted = ["counter", "counter:other", "counter:drained", "counter:external-other", "counter:external-cycle", "counter:discarded", "counter:discard-nested", "file:queued", "file:other", "file:drained", "file:nested", "file:external-nested", "file:external-other", "file:external-cycle", "file:discarded", "file:discard-nested"].includes(scenario), outside: number[] = [];
				const external = !queue || ["queued", "nested", "external-nested"].includes(queue);
				const executor = replay(scenario, async process => {
					const descriptorOffsets = process.descriptors!.filter(fd => fd.fd === 20 || fd.fd === 21 || (local && queue !== "partial" || queue === "nested") && fd.type === "socket").map(fd => {
						if (fd.fd === 20 || fd.fd === 21) outside.push(fd.outside!);
						let content = fd.queueHex === undefined ? undefined : Buffer.from(fd.queueHex, "hex");
						if (fd.counter) { content = Buffer.alloc(9); content.writeBigUInt64LE(BigInt(mode === "value" || mode === "alias" && fd.fd === 21 ? 6 : fd.counter.value)); }
						return { ...fd, before: fd.offset, after: type === "file" && !fd.type ? 1 : fd.offset, content, ...(fd.counter ? { event: fd.counter.id + (mode === "identity" ? 2 : 1) } : {}) };
					});
					if (type === "socket") return;
					const data = Buffer.alloc(8); data.writeBigUInt64LE(mode === "overflow" ? 0xffffffffffffffffn : 2n);
					return { output: [], descriptorOffsets, resourceEvents: [
						...(queue === "drained" ? [{ fd: descriptorOffsets.find(fd => fd.messages)?.fd!, kind: "consume" as const, data: Buffer.from("x") }] : []),
						...descriptorOffsets.filter(fd => queue?.startsWith("discard") ? fd.messages?.length : queue === "peer-closed" && fd.type === "socket" && !fd.messages).map(fd => ({ fd: fd.fd, kind: "release" as const, data: Buffer.from([3]) })),
						...(type === "counter" ? [{ fd: 21, kind: "produce" as const, data }] : []),
						...(local || type === "counter" && (mode === "release" || queue) ? [{ fd: 21, kind: "release" as const, data: Buffer.from([3]) }] : [])] };
				});
				await executor.verify(`exec '${descriptorProbe}' queue ${type} ${state}`, type === "socket" ? "" : local ? "0" : type === "file" ? external ? "1" : "0" : !queue && accepted ? "7" : "5", Number(accepted || mode === "commit-failure"));
				expect(outside, scenario).toEqual([external ? 3 : 0, external ? 3 : 0]);
			}
			for (const mode of ["shared", "unlinked", "offset", "identity", "flags", "closed", "alias-conflict", "invalid", "commit-failure",
				"null", "null-offset", "null-content", "zero", "status-set", "status-clear", "status-conflict", "status-unsupported", "directory", "directory-offset", "directory-content", "directory-stale", "directory-replaced", "directory-no-path", "opath", "opath-offset", "opath-content", "opath-flags", "directory-opath", "directory-symlink"]) {
				await writeFile(input, "abcdef");
				const directory = mode.startsWith("directory"), opath = mode.includes("opath"), device = directory || opath || mode.startsWith("null") || mode === "zero";
				const target = directory ? directoryInput : opath ? input : device ? mode === "zero" ? "/dev/zero" : "/dev/null" : input;
				const accepted = mode === "shared" || mode === "unlinked" || mode === "null" || mode === "status-set" || mode === "status-clear" || mode === "directory" || mode === "opath" || mode === "opath-content" || mode === "directory-opath";
				const executor = replay(mode, async ({ pid, descriptors }) => {
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
					return { descriptorOffsets };
				}, { descriptors: directory, beforeCommit: async ({ pid }) => { expect(await readFile(`/proc/${pid}/fdinfo/3`, "utf8")).toMatch(/^pos:\s*0$/m); } });
				await executor.verify(`exec 3<'${target}'; exec 4<&3; exec 5<'${target}'; ${opath ? `'${descriptorProbe}' opath '${target}'` : mode === "status-clear" ? `'${descriptorProbe}' status '${input}'` : "/bin/true"}; ` +
					(mode.startsWith("status-") ? `for fd in 3 4 5; do while read -r key value; do if [[ $key == flags: ]]; then (( (8#$value & 3072) == (${mode === "status-set" ? 3072 : 0} * (fd != 5)) )) || exit 90; fi; done </proc/self/fdinfo/$fd; done; ` : "") +
					(device ? "printf native" : `IFS= read -r -N 1 a <&4; IFS= read -r -N 1 b <&5; IFS= read -r -N 1 c <&3; printf '%s:%s:%s' "$a" "$b" "$c"`),
					device ? (accepted ? "replayed:native" : "native") : accepted ? "replayed:d:b:e" : "a:a:b", Number(accepted || mode === "commit-failure"));
				expect(executor.adopted).toHaveBeenCalledTimes(Number(accepted));
				expect(executor.settle.mock.calls).toEqual([[accepted || mode === "commit-failure"]]);
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
				try {
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
				} finally { waiting.resolve(); await Promise.allSettled([siblings]); }
			}
			for (const command of ["exec /bin/sh -c 'exec /bin/true'", `exec 3<'${input}'; exec '${descriptorProbe}' thread-exit '${input}'`]) {
				const visited: string[] = [], completed: string[] = [];
				const chained = held({ decide: async ({ pid }) => {
					const image = `${pid}:${await filesystem.readlink(`/proc/${pid}/exe`)}`;
					visited.push(image);
					return { kind: "continue", observeCompletion: () => { completed.push(image); } };
				} });
				expect(await chained.execute(command)).toEqual({ exitCode: 0 });
				expect(visited).toHaveLength(2);
				expect(completed.sort(), "every exec image must retain its completion owner through replacement").toEqual(visited.sort());
			}
			await replay("delivery", async () => ({ output: [{ fd: 1, data: Buffer.alloc(2 * 1024 * 1024, 97) }] }),
				{ descriptors: false, beforeCommit: () => writeFile(path.join(root, "producer-armed"), "ready") })
				.verify("/bin/true | (while [[ ! -e producer-armed ]]; do :; done; /usr/bin/wc -c)", "2097152\n", 1);
			const actorContext = new AsyncLocalStorage<string>(), execIDs = new Set<string>();
			for (const disposition of [undefined, "recoverable", "poisoned", "killed"] as const) {
				const after = path.join(root, `after-${disposition}`);
				const scope = { sessionID: "session", turnID: "original" };
				const nativeDone = deferred();
				const adopted = vi.fn(() => { throw new Error("advisory feedback failed"); });
				const observeTiming = vi.fn<(timing: HeldExecTiming) => void>();
				let heldPid = 0;
				const commit = vi.fn(async () => {
					if (disposition === "killed") { process.kill(heldPid, "SIGKILL"); await nativeDone.promise; }
					else if (disposition) throw effectCommitFailure(new Error("injected commit failure"), disposition);
				});
				const decide = vi.fn(async (process: HeldExecProcess) => {
					expect(actorContext.getStore()).toBe("original");
					expect(process.id).toMatch(/^[a-f0-9]{48}:1$/); expect(process.sequence).toBe(1);
					execIDs.add(process.id);
					heldPid = process.pid; return { kind: "replay" as const, output: [], exitCode: 0, commit, adopted, observeTiming };
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
				expect(observeTiming).toHaveBeenCalledOnce();
				const timing = observeTiming.mock.calls[0]![0];
				expect(timing).toMatchObject({ barrier: "none", outcome: disposition ? "failed" : "adopted" });
				expect(timing.native).toBeUndefined();
				if (!disposition) {
					expect(timing.committedAt).toBeGreaterThanOrEqual(timing.requestedAt);
					expect(timing.completedAt).toBeGreaterThanOrEqual(timing.committedAt!);
				}
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
		} finally { await boundary.close(); await rm(root, { recursive: true, force: true }); }
	});

	test("commits what it wrote outside the workspace on adoption, over the host state it replaced", async ({ benchmark }) => {
		const fixture = await benchmark("pi-private-writes-"), outside = await mkdtemp(path.join(os.tmpdir(), "pi-private-outside-"));
		try {
			for (const name of ["kept", "stay"]) await writeFile(path.join(outside, name), "old\n");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
			const scratch = "t=$(mktemp -d) && echo t > $t/x && cat $t/x && rm -r $t", host = async (name: string) => readFile(path.join(outside, name), "utf8").catch(() => "absent");
			// Written, replaced and scratch paths in one command: only the net changes commit, and only on adoption, with the times
			// they were left. The directory holding them lists the host's entries with the branch's, and is the host's own.
			const branch = await forkReusableBash(fixture, { label: "commit", command: `mkdir ${outside}/d && echo private > ${outside}/d/f && echo new > ${outside}/kept && cat ${outside}/d/f && ` +
				`touch -d @981173106.123456789 ${outside}/d/f && echo $(ls -a ${outside}) $(stat -c %i ${outside}) && ${scratch}`, actionNamespace: "private-writes", executionFingerprint });
			try {
				expect(textOutput(branch.output.result)).toBe(`private\n. .. d kept stay ${(await stat(outside)).ino}\nt\n`);
				expect([await host("d/f"), await host("kept")], "a private write must not reach the host before adoption").toEqual(["absent", "old\n"]);
				await branch.commit();
				expect([await host("d/f"), await host("kept"), (await stat(path.join(outside, "d/f"), { bigint: true })).mtimeNs]).toEqual(["private\n", "new\n", 981173106123456789n]);
			} finally { await branch.dispose?.(); }
			// A child that read another's scratch file saw the command's own doing: the command stands on the workspace as it found it.
			const scratched = await forkReusableBash(fixture, { label: "scratch", command: "mkdir w && echo t > w/x && cat w/x && rm -r w", actionNamespace: "private-writes", executionFingerprint });
			try { const validation = await scratched.validate?.(); expect([textOutput(scratched.output.result), validation], JSON.stringify({ validation, metrics: fixture.backend.metrics() })).toMatchObject(["t\n", { status: "valid" }]); } finally { await scratched.dispose?.(); }
			// A host object someone changed meanwhile is no baseline: the Actor must run the command instead.
			const raced = await forkReusableBash(fixture, { label: "raced", command: `echo again > ${outside}/kept`, actionNamespace: "private-writes", executionFingerprint });
			try {
				await writeFile(path.join(outside, "kept"), "raced\n");
				await expect(raced.commit()).rejects.toThrow();
				expect(await host("kept")).toBe("raced\n");
			} finally { await raced.dispose?.(); }
			// Successors read prior children's private writes; the whole transaction proves them without publishing native history.
			for (const label of ["prepared", "fresh"]) {
				const before = fixture.backend.metrics(), child = await forkReusableBash(fixture, { label, command: `/bin/sh -c 'printf "g\\n" > "$1"' sh ${outside}/g && cat ${outside}/g`, actionNamespace: "private-writes", executionFingerprint });
				try {
					expect([textOutput(child.output.result), await host("g"), fixture.backend.metrics().hits > before.hits], JSON.stringify(fixture.backend.metrics())).toEqual(["g\n", "absent", false]);
					expect(await child.validate?.()).toMatchObject({ status: "valid" });
					if (label === "fresh") { await child.commit(); expect(await host("g")).toBe("g\n"); }
				} finally { await child.dispose?.(); }
			}
			// An independently prepared native operation has the same external-write baseline as a whole tool.
			const args = { command: `/bin/sh -c 'printf "bound\\n" > "$1"' sh ${outside}/kept` };
			const invocation = resolvePiToolInvocation("bash", args, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath })!;
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
			const open = fixture.backend.open.bind(fixture.backend);
			const racing = vi.spyOn(fixture.backend, "open").mockImplementationOnce(async input => {
				const session = await open(input);
				return { ...session, executeBinding: async binding => {
					const result = await session.executeBinding(binding);
					await writeFile(path.join(outside, "kept"), "raced\n");
					return result;
				} };
			});
			try {
				await expect(prepareBound()).rejects.toThrow(/unrepresentable writes outside the workspace.*external_write/);
				expect(await host("kept")).toBe("raced\n");
			} finally { racing.mockRestore(); }
		} finally { await fixture.dispose(); await rm(outside, { recursive: true, force: true }); }
	});

	test("settles a brokered child's inherited pipe as the child left its queue, executed and replayed", async ({ benchmark }) => {
		const fixture = await benchmark("pi-pipe-settle-");
		await compileBenchmarkHelper(fixture.workspace, { source: "pipe-child.c", output: "pipe-child", code: "#include <unistd.h>\nint main(void) { char byte; return read(3, &byte, 1) != 1 || byte != 'a' || write(4, \"cX\", 2) != 2; }\n" });
		// Both ends reach the child; the parent reads what the queue holds once it exits.
		await compileBenchmarkHelper(fixture.workspace, { source: "piper.c", output: "piper", code: `#include <stdio.h>\n#include <sys/wait.h>\n#include <unistd.h>
int main(void) { int fds[2], status; char queue[8]; if (pipe(fds) || fds[0] != 3 || write(4, "ab", 2) != 2) return 90; pid_t child = fork();
	if (!child) { execlp("pipe-child", "pipe-child", (char *)0); _exit(91); }
	if (waitpid(child, &status, 0) != child || status) return 92; close(4); ssize_t length = read(3, queue, sizeof(queue));
	return length < 0 || printf("%.*s\\n", (int)length, queue) < 0; }\n` });
		const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
		for (const label of ["executed", "replayed"]) {
			const before = fixture.backend.metrics(), branch = await forkReusableBash(fixture, { label, command: "./piper", actionNamespace: "pipe-settle", executionFingerprint });
			try {
				const after = fixture.backend.metrics();
				expect({ text: textOutput(branch.output.result), bypasses: after.bypasses - before.bypasses, hits: after.hits > before.hits }, JSON.stringify(after))
					.toEqual({ text: "bcX\n", bypasses: 0, hits: label === "replayed" });
			} finally { await branch.dispose?.(); }
		}
	});

	test("owns output and capture lifetimes, preserves concurrency and brokers an internal pipe", async ({ skip, benchmark }) => {
		const fixture = await benchmark("pi-process-concurrency-");
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
			await writeFile(path.join(fixture.workspace, "barrier-worker"), [
				"#!/bin/sh", "set -C",
				"if ( : > \"$1/slot\" ) 2>/dev/null; then self=one other=two; else self=two other=one; fi",
				": > \"$1/$self\"", "while [ ! -e \"$1/$other\" ]; do :; done",
			].join("\n"));
			await chmod(path.join(fixture.workspace, "barrier-worker"), 0o755);
			// Scripts keep their own path as $0 and resolve relative modules, directly or through a bypass's shadow.
			for (const [name, script] of [["redirect-worker", "#!/bin/sh\nprintf 'redirected %s\\n' \"$0\""], ["route-worker", "#!/bin/sh\nprintf 'out\\n'; printf 'err\\n' >&2"],
				["bin.js", "#!/usr/bin/env node\nconsole.log(require('./lib.js'));"], ["lib.js", "module.exports = 'lib';"]]) {
				await writeFile(path.join(fixture.workspace, name), `${script}\n`);
				await chmod(path.join(fixture.workspace, name), 0o755);
			}
			await compileBenchmarkHelper(fixture.workspace, { source: "fd-check.c", output: "fd-check", code: "#include <fcntl.h>\nint main(void) {\n\tint mask = 0;\n\tfor (int fd = 0; fd < 3; fd++) if (fcntl(fd, F_GETFD) >= 0) mask |= 1 << fd;\n\treturn mask;\n}\n" });
			const streamProbes = ["", "0<&-", "1>&-", "2>&-", "0<&- 1>&- 2>&-", "3>&1"].map((redirection) =>
				"status=0; fd-check " + redirection + " || status=$?; printf 'fds:%s\\n' \"$status\"").join("; ");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { skip });
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
				command: "set -e; /bin/bash -c '(/bin/sleep 0.05; echo x > escaped.txt) 2>/dev/null & exit 0'; until [ -e escaped.txt ]; do :; done; /usr/bin/printf 'trace-root-fallback\\n'; mkdir barrier; barrier-worker barrier & first=$!; barrier-worker barrier & second=$!; wait \"$first\"; wait \"$second\"; redirect-worker | { read line; printf '%s\\n' \"$line\" > redirected.txt; printf '%s\\n' \"$line\"; }; " +
					"route-worker 2>/dev/null; route-worker 2>&1 1>/dev/null; ./bin.js; /usr/bin/printf 'file-fallback\\n' > redirected-file.txt; /usr/bin/cat < redirected-file.txt; printf 'pipe-fallback\\n' | /usr/bin/cat; " + streamProbes + "; printf '%32768s:end' ''",
				actionNamespace: "process-concurrency-test",
				executionFingerprint,
			});
			expect(branch.output.isError, JSON.stringify({ output: branch.output, metrics: fixture.backend.metrics() })).toBe(false);
			const text = branch.output.result.content[0];
			// Discarded streams route into /dev/null; the stderr kept by `2>&1 1>/dev/null` still reaches stdout.
			expect(text?.type === "text" && text.text).toBe(`trace-root-fallback\nredirected ${path.join(fixture.workspace, "redirect-worker")}\nout\nerr\nlib\nfile-fallback\npipe-fallback\nfds:7\nfds:6\nfds:5\nfds:3\nfds:0\nfds:7\n` + " ".repeat(32768) + ":end");
			const nextCapture = await vi.mocked(captures[1]!.finish).mock.results[0]!.value;
			expect({ allocationFailed, aborts: vi.mocked(captures[0]!.abort).mock.calls.length, nextComplete: nextCapture.complete },
				nextCapture.complete ? undefined : nextCapture.reason).toEqual({ allocationFailed: true, aborts: 1, nextComplete: true });
			expect(captures[0]!.finish).not.toHaveBeenCalled();
			expect(branch.executionMetrics.reuse?.misses).toBeGreaterThanOrEqual(3);
			expect(branch.executionMetrics.reuse?.bypasses).toBe(4); // Capture failure and three unsupported stdio contexts; a pipeline writer is brokered.
			// With process.execve a bypass runs its native image in place, inside the top-level trace; the injected capture failure still escapes it.
			const validation = JSON.stringify(await branch.validate?.());
			expect([validation.includes("broker_bypass:"), validation.includes("injected trace allocation failure")], validation)
				.toEqual([typeof process.execve !== "function", true]);
			expect(existsSync(path.join(fixture.workspace, "escaped.txt")), "an orphan's write, adopted by the supervisor, stays in the sandbox").toBe(false);
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
					// The seal names why nothing completed: the unfinished execution's own error.
					await expect(running).rejects.toThrow(failure === "spawn" ? "missing-executable ENOENT" : "aborted");
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
		}
	}, 15_000);

	test.for(["trace", "transaction", "dependency", "input_image", "publication"] as const)("preserves output and capture ownership when %s fails", { timeout: 15_000 }, async (failure, { skip, benchmark }) => {
		const fixture = await benchmark("pi-process-capture-failure-", failure === "dependency" ? "git" : undefined); // Invalidate the copied prestate.
		const { readFile: readTrace, rm: removeFile } = await vi.importActual<typeof filesystem>("node:fs/promises");
		const { spawn } = await vi.importActual<typeof childProcess>("node:child_process");
		const entered = deferred(), failed = deferred(), gate = deferred();
		const error = new Error(failure === "dependency" ? "transaction baseline changed: input.txt" : `injected ${failure} capture failure`);
		let traceRoot: string | undefined, released = false, cleanupBeforeRelease = false, returned = false, executions = 0;
		let processContext = {};
		let restoreTransactions: (() => void) | undefined;
		const open = fixture.backend.open.bind(fixture.backend);
		const opening = vi.spyOn(fixture.backend, "open").mockImplementation(async (input) => {
			processContext = { workspace: input.workspace.sandboxRoot, scope: input.scope };
			const begin = input.workspace.transactions.begin;
			const recording = vi.spyOn(input.workspace.transactions, "begin").mockImplementation(async () => {
				const capture = await begin();
				if (failure === "publication" || failure === "input_image") return capture;
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
			if (failure === "input_image" && String(args[0]).endsWith("/proof.json")) { failed.resolve(); await gate.promise; throw error; }
			if (String(args[0]).includes("/trace-")) {
				traceRoot = path.dirname(String(args[0]));
				if (failure === "publication" || failure === "dependency" || failure === "input_image") return readTrace(...args);
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
			const inputName = failure === "input_image" ? path.join(fixture.root, "input.txt") : "input.txt";
			await writeFile(path.resolve(fixture.workspace, inputName), "capture-once");
			await compileBenchmarkHelper(fixture.workspace, { source: "emit.c", output: "emit", code: `#include <fcntl.h>\n#include <unistd.h>\nint main(void) { char text[12]; return read(open(${JSON.stringify(inputName)}, O_RDONLY), text, 12) != 12 || write(1, text, 12) != 12; }\n` });
			await commitBenchmarkFixture(fixture.workspace, "Process capture failure");
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { skip });
			running = forkReusableBash(fixture, { command: "emit", label: failure, actionNamespace: "capture-owners", executionFingerprint });
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
			const detail = failure === "input_image" ? undefined : JSON.parse(lastError.split("; process=")[1]!);
			if (failure !== "input_image") {
				expect(detail).toMatchObject({ ...processContext, requestID: 1,
					stage: failure === "publication" ? "history_publication" : failure === "dependency" ? "dependencies" : `${failure}_capture` });
				expect(detail.weakKey).toMatch(/^sha256:[a-f0-9]{64}$/);
			}
			const validation = await branch.validate?.();
			if (failure === "publication") {
				await expect(validateTransferredProcessEvidence(publishing.mock.calls[0]![0].dependencyCertificate)).resolves.toMatchObject({ status: "valid" });
				expect(lastError).toContain(`nested_publish:${error.message}`);
				expect(detail).toMatchObject({ certificateID: publishing.mock.calls[0]![0].id, complete: true, taints: ["clock", "random"] });
				// Only publication failed; the captured root still represents the shell's original directory metadata.
				expect(validation).toMatchObject({ status: "valid" });
				expect((await fixture.backend.store.stats()).certificates).toBe(0);
			} else {
				expect(validation?.status).toBe("indeterminate");
				expect(JSON.stringify(validation)).toContain(`nested_capture:${error.message}`);
			}
		} finally {
			released = true; gate.resolve();
			await running?.then((branch) => branch.dispose(), () => undefined);
			restoreTransactions?.(); opening.mockRestore(); reading.mockRestore(); removing.mockRestore(); spawning.mockRestore(); publishing.mockRestore();
		}
	});

	test.for(["rename", "posix-lock", "ofd-lock", "flock", "rdtsc", "auxv-random"] as const)("rejects reuse when isolated resource semantics differ from native (%s)", { timeout: 20_000 }, async (mode, { skip, benchmark }) => {
		const instanceInput = mode === "rdtsc" || mode === "auxv-random";
		if (instanceInput && process.arch !== "x64") return skip("x86-64 ELF input probe");
		if (mode === "rename") {
			const overlay = await linuxOverlayfsCapability();
			if (!overlay.available) return skip(overlay.detail);
		}
		const fixture = await benchmark("pi-process-driver-semantics-", mode === "rename" ? "overlayfs" : undefined);
		const { workspace, backend } = fixture;
		let branch: Awaited<ReturnType<typeof forkReusableBash>> | undefined;
		let locker: childProcess.ChildProcessWithoutNullStreams | undefined, lockerClosed: Promise<unknown> | undefined;
		try {
			await mkdir(path.join(workspace, "source"));
			await writeFile(path.join(workspace, "source", "value.txt"), "value\n", "utf8");
			if (mode !== "rename") {
				await compileBenchmarkHelper(workspace, { source: "probe.c", output: "probe", code: instanceInput ? `${cHeaders("elf.h stdint.h stdio.h unistd.h")}
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
` });
				await commitBenchmarkFixture(workspace, "Resource control observation");
				if (!instanceInput) {
					locker = childProcess.spawn(path.join(workspace, "probe"), ["hold"], { cwd: workspace, env: fixture.environment, stdio: ["pipe", "pipe", "pipe"] });
					lockerClosed = once(locker, "close");
					expect((await Promise.race([once(locker.stdout, "data"), lockerClosed.then(() => { throw new Error("lock holder exited"); })]))[0].toString()).toBe("R");
					expect(execFileSync(path.join(workspace, "probe"), [], { cwd: workspace, env: fixture.environment, encoding: "utf8" })).toBe("L");
				}
			}
			const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { includeWorkspaceFingerprint: true });
			branch = await forkReusableBash(fixture, { command: mode === "rename" ? "mv source moved && mkdir shut && chmod 600 shut" : "probe", label: "driver-semantics-test",
				actionNamespace: "driver-semantics-test", executionFingerprint });
			expect(branch.output.isError, JSON.stringify(branch.output)).toBe(false);
			if (mode !== "rename") {
				expect(branch.output.result.content).toEqual([{ type: "text", text: instanceInput ? expect.stringMatching(/^\d+\n$/) : "U" }]);
				const route = await fixture.prepareActorReplay(true);
				const { output } = await executeProcess(fixture, route.executor, ": changed-parent; probe", { scope: { sessionID: "benchmark", turnID: "later" } });
				expect(output, JSON.stringify(backend.actorMetrics())).toEqual(instanceInput ? expect.stringMatching(/^\d+\n$/) : "L");
				expect(backend.actorMetrics().hits).toBe(0);
			}
			// A lock the sandbox granted, or found free, is a dependency the host's holder breaks.
			if (!instanceInput) {
				const validation = await branch.validate?.();
				expect(validation?.status).toBe(mode === "rename" ? "indeterminate" : "stale");
				expect(JSON.stringify(validation)).toContain(mode === "rename" ? "filesystem_semantics" : "value.txt");
			}
			expect(branch.executionMetrics.reuse?.requests).toBeGreaterThan(0);
			expect(branch.executionMetrics.reuse?.executionMs).toBeGreaterThan(0);
			if (!mode.endsWith("lock")) {
				expect(backend.metrics().tainted).toBeGreaterThan(0);
				expect(backend.metrics().published).toBe(0);
				expect((await backend.store.stats()).certificates).toBe(0);
			}
			expect((await stat(path.join(workspace, "source"))).isDirectory()).toBe(true);
			await expect(stat(path.join(workspace, "moved"))).rejects.toThrow();
		} finally {
			locker?.stdin.end(); await lockerClosed;
			await branch?.dispose();
		}
	});
});
nativeTest.for(["git", "auto"] as const)("preserves native metadata across stat families and rejects volatile or changed results (%s)", { timeout: 120_000 }, async (driver, { benchmark }) => {
	const fixture = await benchmark("pi-native-metadata-", driver, {}, os.homedir());
	const actual = await vi.importActual<typeof filesystem>("node:fs/promises"), pins: filesystem.FileHandle[] = [];
	const opening = vi.mocked(filesystem.open).mockImplementation(async (...args) => {
		const handle = await actual.open(...args);
		if (typeof args[1] === "number" && args[1] & 0x200000) pins.push(handle);
		return handle;
	});
	try {
		const bin = path.join(fixture.root, "host-bin"); await mkdir(bin);
		await compileBenchmarkHelper(bin, { source: "pid.c", output: "cat", code: '#include <stdio.h>\n#include <unistd.h>\nint main(void) { printf("%ld\\n", (long)getpid()); return 0; }\n' });
		await writeFile(path.join(fixture.workspace, "README"), "metadata fixture\n");
		await filesystem.chmod(path.join(fixture.workspace, "README"), 0o640);
		await filesystem.link(path.join(fixture.workspace, "README"), path.join(fixture.workspace, "README-alias"));
		await writeFile(path.join(fixture.workspace, ".gitignore"), "dist/\n");
		await mkdir(path.join(fixture.workspace, "dist"));
		await writeFile(path.join(fixture.workspace, "dist", "module.cjs"), "module.exports = 42;\n");
		await mkdir(path.join(fixture.workspace, "many"));
		for (let index = 0; index < 300; index++) await mkdir(path.join(fixture.workspace, "many", `d${index}`));
		await writeFile(path.join(fixture.workspace, "many", "file"), "regular\n");
		await filesystem.symlink("d0", path.join(fixture.workspace, "many", "alias"));
		await compileBenchmarkHelper(fixture.workspace, { source: "metadata.c", output: "metadata", code: String.raw`#define _GNU_SOURCE
${cHeaders("dirent.h fcntl.h stdio.h string.h sys/mman.h sys/stat.h sys/syscall.h sys/sysmacros.h unistd.h")}
static void emit(struct stat *s) { printf("%llu %llu %lld %o %llu\n", (unsigned long long)s->st_dev, (unsigned long long)s->st_ino, (long long)s->st_size, s->st_mode, (unsigned long long)s->st_nlink); }
int main(int argc, char **argv) {
	const char *name = argc > 1 ? argv[1] : "README";
	struct stat s; struct statx x; int fd = open(name, O_RDONLY);
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
		printf("%llu %llu %lld %o %u\n", (unsigned long long)makedev(x.stx_dev_major, x.stx_dev_minor), (unsigned long long)x.stx_ino, (long long)x.stx_size, x.stx_mode, x.stx_nlink);
	}
	return close(fd);
}
` });
		await filesystem.symlink("README", path.join(fixture.workspace, "metadata-link"));
		await commitBenchmarkFixture(fixture.workspace, "metadata");
		const stamp = `${Math.floor(Date.now() / 1000) - 30} +0000`;
		execFileSync("git", ["commit", "--amend", "--no-edit", "--date", stamp], { cwd: fixture.workspace, env: { ...process.env, GIT_COMMITTER_DATE: stamp } });
		const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
		let turn = 0;
		const produce = (command: string) => forkReusableBash(fixture, { command, label: "metadata", executionFingerprint, actionNamespace: "metadata",
			executionScope: { sessionID: "metadata", turnID: String(turn++) } });
		const native = (command: string) => execFileSync("/bin/bash", ["-c", command], { cwd: fixture.workspace, env: { ...fixture.environment }, encoding: "utf8" });
		const publication = vi.mocked(filesystem.writeFile).mockImplementation(async (...args) => {
			if (String(args[0]).endsWith("/metadata.json")) throw new Error("injected metadata publication failure");
			return actual.writeFile(...args);
		});
		try {
			await expect(produce("true")).rejects.toThrow("injected metadata publication failure");
			expect(pins.length).toBeGreaterThan(0); expect(pins.every(handle => handle.fd === -1)).toBe(true);
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
			if (mutation.endsWith("legacy")) { expect(await readFile(path.join(fixture.workspace, "legacy-result"), "utf8")).toBe("legacy"); await rm(path.join(fixture.workspace, "legacy-result")); }
		}
		for (const file of ["README", "many"]) {
			const command = `./metadata ${file} proc`, candidate = await produce(command);
			try {
				// The proc link itself names the caller's descriptor; compare followed metadata only.
				expect(candidate.output.isError, textOutput(candidate.output.result)).toBe(false);
				const followed = (text: string) => text.trimEnd().split("\n").filter((_, index) => index !== 1 && index !== 7);
				expect(followed(textOutput(candidate.output.result))).toEqual(followed(native(command)));
			} finally { await candidate.dispose(); }
		}
		for (const name of [".", "metadata-link", "dist/module.cjs"]) {
			const command = name.startsWith("dist/") ? `node -e "console.log(require('./${name}'))"` : `./metadata ${name}`, candidate = await produce(command);
			try {
				expect(candidate.output.isError, textOutput(candidate.output.result)).toBe(false);
				expect(textOutput(candidate.output.result)).toBe(native(command));
				expect((await candidate.validate?.())?.status).toBe("valid");
				if (name.startsWith("dist/")) {
					await writeFile(path.join(fixture.workspace, name), "module.exports = 43;\n");
					expect((await candidate.validate?.())?.status).toBe("stale");
					continue;
				}
				if (name !== ".") continue;
				const info = await filesystem.stat(fixture.workspace);
				await filesystem.utimes(fixture.workspace, info.atime, new Date(info.mtimeMs - 2000));
				expect((await candidate.validate?.())?.status).toBe("stale");
			} finally { await candidate.dispose(); }
		}
		const branch = await produce("./metadata");
		try {
			expect(branch.output.isError).toBe(false);
			expect(textOutput(branch.output.result)).toBe(native("./metadata"));
			expect((await branch.validate?.())?.status).toBe("valid");
			execFileSync("python3", ["-c", 'import os; p="README"; s=os.stat(p); b=open(p,"rb").read(); q=p+".swap"; open(q,"wb").write(b); os.chmod(q,s.st_mode & 0o7777); os.utime(q, ns=(s.st_atime_ns,s.st_mtime_ns)); os.replace(q,p)'], { cwd: fixture.workspace });
			expect(textOutput(branch.output.result)).not.toBe(native("./metadata"));
			expect((await branch.validate?.())?.status).toBe("stale");
		} finally { await branch.dispose?.(); }
		for (const command of [`${bin}/cat`, "git log -1 --format=%cr"]) {
			const outputs: string[] = [];
			const before = fixture.backend.metrics().wholeCommandHits;
			for (let index = 0; index < 2; index++) {
				const candidate = await produce(command);
				try { expect(candidate.output.isError).toBe(false); outputs.push(textOutput(candidate.output.result)); }
				finally { await candidate.dispose?.(); }
				if (command.startsWith("git") && index === 0) await new Promise(resolve => setTimeout(resolve, 1500));
			}
			expect(fixture.backend.metrics().wholeCommandHits - before, command).toBe(0);
			expect(outputs[0], command).not.toBe(outputs[1]);
		}
	} finally {
		try { await fixture.dispose(); expect(pins.every(handle => handle.fd === -1)).toBe(true); }
		finally { opening.mockRestore(); }
	}
});
