import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, open, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { ResourceInputSession } from "../src/resource-version.ts";
import { readContent } from "../src/content-chunks.ts";
import { fileURLToPath } from "node:url";
import { nativeBuildDefinitions } from "../src/config-schema.ts";
import { executionDomainCollector, resolveLinuxExecHelper } from "../src/linux-held-exec.ts";
import { executionRecords, observeExecution, traceTail } from "../src/execution-observer.ts";
import { createBashTool, createReadTool } from "@earendil-works/pi-coding-agent";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { expect, test } from "vitest";
import {
	createControlledBashOperations,
	resolveSpeculativeConfig,
	withSpeculativeConfig,
} from "../src/index.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import {
	adaptProcessToolOperations,
	createLinuxProcessExecutionWorld,
	definedProcessEnvironment,
	ProcessExecutionCoordinator,
} from "../src/linux-process-world.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { createLinuxProcessBenchmark, textOutput } from "./linux-process-fixture.ts";

test
	.skipIf(process.platform !== "linux")
	.for(
		(["none", "provenance"] as const).flatMap((actorObservationMode) =>
			(["bytes", "metadata", "alias"] as const).map((mode) => ({ actorObservationMode, mode })),
		),
	)(
	"public process replay projects stdout only when the program observes bytes ($actorObservationMode, $mode)",
	{ timeout: 60000 },
	async ({ mode, actorObservationMode }) => {
		const fixture = await createLinuxProcessBenchmark("pi-stream-contract-", "in_memory", {}, os.homedir());
		await fixture.write("input.txt", "first\nsecond\n");
		await fixture.compile("emit", {
			code: `#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
int main(int argc, char **argv) {
  if (argc != 3) return 2;
  struct stat state;
  if (!strcmp(argv[2], "metadata") && fstat(1, &state)) return 3;
  if (!strcmp(argv[2], "alias")) {
    int alias = dup(1);
    if (alias < 0 || fstat(alias, &state)) return 4;
    close(alias);
  }
  int input = open(argv[1], O_RDONLY);
  if (input < 0) return 5;
  char bytes[256];
  ssize_t length = read(input, bytes, sizeof(bytes));
  close(input);
  if (length < 0 || write(1, bytes, length) != length) return 6;
  return 0;
}
`,
		});
		const host = systemHost(
			fixture.workspace,
			[fixture.tool],
			{
				executionWorlds: [fixture.world],
				resolveInvocation: (_tool, input) => fixture.invocation(input as Record<string, unknown>),
				config: {
					processReuse: {
						actorObservationMode,
						environmentMode: "allowlist",
						environmentKeys: ["PATH", "HOME", "LANG", "PWD", "SHELL"],
						replayAcceptedTaints: ["clock", "random", "pid_observation", "descriptor_observation"],
					},
				},
			},
			{ prepareObservedOperations: false },
		);
		try {
			await fixture.prepare();
			await host.start();
			const command = `./emit input.txt ${mode}`;
			await host.preview("bash", { command });
			await host.ready();
			expect(textOutput(await host.execute("bash", { command }))).toBe("first\nsecond\n");
			const before = fixture.backend.actorMetrics().hits;
			expect(textOutput(await host.execute("bash", { command: `${command} | head -n 1` }))).toBe("first\n");
			expect(fixture.backend.actorMetrics().hits - before, fixture.backend.actorMetrics().lastError).toBe(
				mode === "bytes" ? 1 : 0,
			);
			if (mode === "bytes") expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
		} finally {
			await host.dispose();
			await fixture.dispose();
		}
	},
);

test.skipIf(process.platform !== "linux")(
	"isolates domain quotas, attributes unsupported calls across fork and detects inherited fd writes",
	async () => {
		const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC);
		const collector = await executionDomainCollector(binary);
		const root = await mkdtemp(path.join(os.homedir(), "pi-domain-observer-"));
		const probe = path.join(root, "probe");
		try {
			execFileSync("cc", [
				...nativeBuildDefinitions(resolveSpeculativeConfig({ nativeBuild: { domainTraceBytes: 104 } })),
				'-DPI_DOMAIN_BUILD="' + "0".repeat(64) + '"',
				"-O2",
				"-ffunction-sections",
				"-fdata-sections",
				fileURLToPath(new URL("./execution-observer-fixture.c", import.meta.url)),
				"-Wl,--gc-sections",
				"-o",
				probe,
			]);
			execFileSync(probe, ["quota"]);
			await using inherited = await open(path.join(root, "inherited"), "w+");
			await inherited.truncate(4096);
			for (const mode of ["private", "shared", "shared-pkey", "shared-futex", "remove", "unknown", "unknown", "fd"]) {
				const domain = collector.open();
				try {
					const read = domain.trace();
					const [launch, ...args] = domain.launch([probe, mode]);
					execFileSync(launch, args, { stdio: ["ignore", "pipe", "pipe", inherited.fd] });
					const definitions: number[] = [];
					const numbers = [254, 301, 241, 244, 129, 297, 444, 70000];
					const records = executionRecords(() => {
						const bytes = read();
						for (let offset = 0; offset < bytes.length; ) {
							const size = bytes.readUInt32LE(offset + 40);
							if (bytes.readBigUInt64LE(offset + 32) === 26n)
								definitions.push(Number(bytes.readBigUInt64LE(offset + 48)));
							offset += (48 + size + 7) & ~7;
						}
						return bytes;
					});
					await using tail = traceTail(records, root);
					const observation = await observeExecution(tail, probe, root);
					expect(new Set(definitions).size).toBe(definitions.length);
					if (mode === "fd") {
						expect(observation.complete).toBe(false);
						expect(observation.taints).toContain("untracked_fd");
						expect(observation.incompleteReasons).toContain("unresolved_fd_write");
						continue;
					}
					expect(observation.complete, observation.incompleteReasons.join(",")).toBe(true);
					if (mode === "private") {
						expect(observation.taints).not.toContain("unsupported_syscall");
					} else if (mode !== "unknown") {
						expect(observation.taints).toContain("unsupported_syscall");
						const diagnostic = mode === "shared-futex" ? "futex" : mode === "remove" ? "madvise" : "mprotect";
						expect(observation.unsupportedSyscalls).toEqual([diagnostic]);
					} else {
						expect(definitions).toEqual(expect.arrayContaining(numbers));
						expect(observation.taints).toContain("unsupported_syscall");
						expect(observation.unsupportedSyscalls).toContain("syscall_70000");
						for (const pid of observation.pids!) {
							const uses = records.journal.filter((r) => r.pid === pid && r.unsupported?.name === "syscall_70000");
							expect(uses).toHaveLength(1);
						}
					}
				} finally {
					domain.close();
				}
			}
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	},
);

test.skipIf(process.platform !== "linux")(
	"records ordered writes through a redirected stdout alias and bounds repeated regular reads",
	async () => {
		const binary = await resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC);
		const domain = (await executionDomainCollector(binary)).open();
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-binary-observation-"));
		const input = path.join(root, "input");
		try {
			await writeFile(input, "data");
			await using directory = await open(root);
			const inode = (await stat(input, { bigint: true })).ino;
			const records = executionRecords(domain.trace());
			await using tail = traceTail(records, root);
			const program = [
				"import os",
				"assert 'input' in os.listdir(4)",
				"fd = os.open('input', os.O_RDONLY)",
				"for _ in range(1500):",
				"    os.read(fd, 1)",
				"    os.pread(fd, 1, 0)",
				"os.close(fd)",
				"out = os.dup(1)",
				"os.dup2(os.open('/dev/null', os.O_WRONLY), 1)",
				"os.write(out, b'one')",
				"child = os.fork()",
				"if not child:",
				"    os.write(2, b'two')",
				"    os._exit(0)",
				"os.waitpid(child, 0)",
				"os.write(out, b'three')",
			].join("\n");
			const [launch, ...args] = domain.launch(["/usr/bin/python3", "-I", "-S", "-c", program]);
			const child = spawn(binary, ["--output-channel=3", launch, ...args], {
				cwd: root,
				stdio: ["ignore", "pipe", "pipe", "pipe", directory.fd],
			});
			const completed = once(child, "close");
			child.stdout!.resume();
			child.stderr!.resume();
			const channel = child.stdio[3] as import("node:stream").Duplex;
			let header = Buffer.alloc(0);
			try {
				for await (const chunk of channel) {
					header = Buffer.concat([header, chunk]);
					if (header.length === 16) channel.end("R");
					if (header.length > 16) throw new Error("invalid output channel header");
				}
				expect((await completed)[0]).toBe(0);
			} finally {
				if (child.exitCode === null && child.signalCode === null) child.kill();
			}
			const outputEndpoints = [0, 8].map((offset) => "socket:[" + header.readBigUInt64LE(offset) + "]");
			const result = await observeExecution(tail, "/usr/bin/python3", root, {
				captureOutput: true,
				outputEndpoints: [outputEndpoints[0]!, outputEndpoints[1]!],
			});
			expect(result.complete, result.incompleteReasons.join(",")).toBe(true);
			expect(result.tracedProcesses).toBe(2);
			expect(result.paths).toContainEqual({ path: root, role: "input", listed: true });
			expect(result.output?.map(({ fd, data }) => [fd, data.toString()])).toEqual([
				[1, "one"],
				[2, "two"],
				[1, "three"],
			]);
			expect(
				records.journal.filter(
					(event) => ["descriptor_read", "pread64"].includes(event.name) && event.anchors[0]?.inode === inode,
				),
			).toHaveLength(1);
		} finally {
			domain.close();
			await rm(root, { recursive: true, force: true });
		}
	},
);

test.skipIf(process.platform !== "linux")("native Actor children feed later Bash calls across command keys", { timeout: 60000 }, async () => {
	const fixture = await createLinuxProcessBenchmark("pi-native-child-", "in_memory", {}, os.homedir());
	const host = systemHost(fixture.workspace, [fixture.tool], {
		executionWorlds: [fixture.world],
		resolveInvocation: (_tool, input) => fixture.invocation(input as Record<string, unknown>),
		config: { processReuse: { environmentMode: "allowlist", environmentKeys: ["PATH", "LANG", "HOME"],
			replayAcceptedTaints: ["clock", "random", "pid_observation", "descriptor_observation"] } },
	}, { enabled: false, prepareObservedOperations: false });
	try {
		await fixture.write("source.txt", "one\n");
		await fixture.read("source.txt");
		await fixture.prepare();
		await host.start();
		const run = (command: string) => host.execute("bash", { command }).then(textOutput);
		expect(await run("cat source.txt; printf A")).toBe("one\nA");
		const first = fixture.backend.actorMetrics();
		expect(first.published, first.lastError).toBeGreaterThan(0);
		expect(await run("cat source.txt; printf B")).toBe("one\nB");
		const second = fixture.backend.actorMetrics();
		expect(second.hits, second.lastError).toBeGreaterThan(first.hits);
		expect(second.wholeCommandHits).toBe(first.wholeCommandHits);
		expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
		await fixture.write("source.txt", "two\n");
		expect(await run("cat source.txt; printf C")).toBe("two\nC");
		expect(fixture.backend.actorMetrics().hits).toBe(second.hits);
	} finally {
		await host.dispose();
		await fixture.dispose();
	}
});

test.skipIf(process.platform !== "linux")("a Bash write preserves independent file inputs for other tools", { timeout: 60000 }, async () => {
	const fixture = await createLinuxProcessBenchmark("pi-actor-input-", "in_memory", {}, os.homedir());
	const reader = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, {
		maxBytes: () => 1024 * 1024,
		persistentInputs: { store: fixture.backend.store, internalCache: true },
	});
	const host = systemHost(fixture.workspace, [fixture.tool, createReadTool(fixture.workspace)], {
		executionWorlds: [fixture.world, reader],
		resolveInvocation: (tool, input) => tool === "bash"
			? fixture.invocation(input as Record<string, unknown>)
			: resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: {} }),
		config: { processReuse: { replayAcceptedTaints: [] } },
	});
	const session = new ResourceInputSession({ store: fixture.backend.store, internalCache: true }, 1024 * 1024);
	try {
		await fixture.write("header.h", "original\n");
		await fixture.write("from-tool.h", "previewed\n");
		await fixture.prepare();
		await host.start();
		expect(textOutput(await host.execute("bash", { command: "cat header.h; printf created > generated" }))).toBe("original\n");
		expect(fixture.backend.actorMetrics().wholeCommandPublished).toBe(0);
		const target = path.join(fixture.workspace, "header.h");
		const inputs: string[] = [];
		for await (const token of session.recover(path.parse(target).root, new AbortController().signal, target)) {
			const file = token.view?.snapshotInput(target);
			if (file && "content" in file) inputs.push((await readContent(file.content)).toString());
		}
		expect(inputs).toEqual(["original\n"]);
		const first = fixture.backend.actorMetrics();
		expect(first.fileInputsPublished).toBeGreaterThan(0);
		expect(textOutput(await host.execute("bash", { command: "printf next-; cat header.h" }))).toBe("next-original\n");
		expect(fixture.backend.actorMetrics().fileInputHits).toBeGreaterThan(first.fileInputHits);
		expect(fixture.backend.actorMetrics().hits).toBe(0);

		await host.preview("read", { path: "from-tool.h" });
		await host.ready();
		const before = fixture.backend.actorMetrics().fileInputHits;
		expect(textOutput(await host.execute("bash", { command: "cat from-tool.h" }))).toBe("previewed\n");
		expect(fixture.backend.actorMetrics().fileInputHits).toBeGreaterThan(before);
		const previous = fixture.backend.metrics().fileInputHits;
		await host.preview("bash", { command: "cat from-tool.h; printf speculative" });
		await host.ready(2);
		expect(fixture.backend.metrics().fileInputHits).toBeGreaterThan(previous);
	} finally {
		await session.dispose();
		await host.dispose();
		await fixture.dispose();
	}
});

test.skipIf(process.platform !== "linux").for([false, true])(
	"public Actor persists and replays provenance (exact environment with subcompute=%s)",
	{ timeout: 60000 },
	async (exposeToControlledProcesses) => {
		const root = await mkdtemp(path.join(os.homedir(), "pi-provenance-public-"));
		const cwd = path.join(root, "workspace");
		const storeRoot = path.join(root, "cache");
		await mkdir(cwd);
		await writeFile(path.join(cwd, "input.txt"), "first\n");
		if (exposeToControlledProcesses)
			execFileSync("cc", [
				fileURLToPath(new URL("./subcompute-process.c", import.meta.url)),
				"-o",
				path.join(cwd, "subcompute-process"),
			]);
		let environment = {
			PATH: "/usr/bin:/bin",
			HOME: os.homedir(),
			LANG: "C",
			DECLARED: "one",
			UNOBSERVED: "initial",
		};
		const open = () => {
			const backend = new LinuxProcessReuseBackend({ storeRoot });
			const raw = adaptProcessToolOperations(createControlledBashOperations({ shellPath: "/bin/bash" }));
			const coordinator = new ProcessExecutionCoordinator(
				backend.completedReplayExecutor(raw, {
					sourceRoot: cwd,
					invocation: (request) =>
						resolvePiToolInvocation(
							"bash",
							{ command: request.command },
							{
								cwd,
								shellPath: "/bin/bash",
								environment: definedProcessEnvironment(request.environment),
							},
						)?.process,
				}),
			);
			const world = createLinuxProcessExecutionWorld({
				backend,
				coordinator,
				storeRoot,
				tools: ["bash"],
			});
			const tool = createBashTool(cwd, {
				shellPath: "/bin/bash",
				operations: coordinator.operations,
				exposeSessionEnvironment: false,
				spawnHook: (context) => ({ ...context, env: { ...environment } }),
			});
			const host = systemHost(
				cwd,
				[tool],
				{
					executionWorlds: [world],
					resolveInvocation: (_tool, input) =>
						resolvePiToolInvocation("bash", input, {
							cwd,
							shellPath: "/bin/bash",
							environment,
						}),
					config: {
						storage: { root: storeRoot },
						processReuse: {
							actorObservationMode: "provenance",
							environmentMode: exposeToControlledProcesses ? "exact" : "allowlist",
							environmentKeys: ["PATH", "HOME", "LANG", "DECLARED"],
							replayAcceptedTaints: ["clock", "random", "pid_observation", "descriptor_observation"],
						},
						runtime: { resourceCacheMaxEntries: 0 },
						subcomputation: { exposeToControlledProcesses },
					},
				},
				{
					enabled: false,
					prepareObservedOperations: false,
					resourceCacheMaxEntries: 0,
				},
			);
			return Object.assign(host, { backend });
		};
		let host = open();
		try {
			const ready = await host.backend.check(true, cwd);
			expect(ready.state, ready.detail).toBe("ready");
			await host.start();
			const command = exposeToControlledProcesses ? "./subcompute-process" : "cat input.txt";
			expect(textOutput(await host.execute("bash", { command }))).toBe("first\n");
			// The first read changes the freshly created file's observed atime. The next actual run sees a stable version.
			expect(textOutput(await host.execute("bash", { command }))).toBe("first\n");
			expect(
				host.backend.actorMetrics().wholeCommandPublished,
				host.backend.actorMetrics().lastError,
			).toBeGreaterThan(0);
			const database = new DatabaseSync(path.join(storeRoot, "metadata.sqlite"), { readOnly: true });
			try {
				expect(
					Number(database.prepare("SELECT COUNT(*) AS count FROM certificates").get()!.count),
				).toBeGreaterThan(0);
			} finally {
				database.close();
			}
			await host.dispose();
			if (!exposeToControlledProcesses) environment = { ...environment, UNOBSERVED: "changed" };
			host = open();
			await host.start();
			for (let index = 0; index < 2; index++)
				expect(textOutput(await host.execute("bash", { command }))).toBe("first\n");
			expect(host.backend.actorMetrics().wholeCommandHits, host.backend.actorMetrics().lastError).toBe(2);
			expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
			await writeFile(path.join(cwd, "input.txt"), "changed\n");
			expect(textOutput(await host.execute("bash", { command }))).toBe("changed\n");
			expect(host.backend.actorMetrics().wholeCommandHits).toBe(2);
			environment = { ...environment, DECLARED: "two" };
			expect(textOutput(await host.execute("bash", { command }))).toBe("changed\n");
			expect(host.backend.actorMetrics().wholeCommandHits).toBe(2);
		} finally {
			await host.dispose();
			await rm(root, { recursive: true, force: true });
		}
	},
);

test.skipIf(process.platform !== "linux").for([false, true])(
	"public speculative execution survives memory eviction and replays SQLite L2 (subcompute=%s)",
	{ timeout: 60000 },
	async (exposeToControlledProcesses) => {
		const root = await mkdtemp(path.join(os.homedir(), "pi-speculative-l2-"));
		const cwd = path.join(root, "workspace");
		const storeRoot = path.join(root, "cache");
		await mkdir(cwd);
		await writeFile(path.join(cwd, "input.txt"), "published by speculation\n");
		if (exposeToControlledProcesses)
			execFileSync("cc", [
				fileURLToPath(new URL("./subcompute-process.c", import.meta.url)),
				"-o",
				path.join(cwd, "subcompute-process"),
			]);
		const environment = {
			PATH: "/usr/bin:/bin",
			HOME: os.homedir(),
			LANG: "C",
			PWD: cwd,
		};
		const config = resolveSpeculativeConfig({
			storage: { root: storeRoot },
			runtime: { completedProcesses: 1, resourceCacheMaxEntries: 1 },
			subcomputation: { exposeToControlledProcesses },
			processReuse: {
				replayAcceptedTaints: ["clock", "random", "pid_observation", "descriptor_observation"],
			},
		});
		const backend = withSpeculativeConfig(config, () => new LinuxProcessReuseBackend({ storeRoot }));
		const invocation = (command: string) =>
			resolvePiToolInvocation("bash", { command }, { cwd, environment, shellPath: "/bin/bash" })!;
		const raw = adaptProcessToolOperations(createControlledBashOperations({ shellPath: "/bin/bash" }));
		const coordinator = new ProcessExecutionCoordinator(
			backend.completedReplayExecutor(raw, {
				sourceRoot: cwd,
				invocation: (request) => invocation(request.command).process,
			}),
		);
		const world = createLinuxProcessExecutionWorld({
			backend,
			coordinator,
			storeRoot,
			tools: ["bash"],
		});
		const tool = createBashTool(cwd, {
			shellPath: "/bin/bash",
			operations: coordinator.operations,
			exposeSessionEnvironment: false,
			spawnHook: (context) => ({ ...context, env: environment }),
		});
		const host = systemHost(
			cwd,
			[tool],
			{
				config,
				executionWorlds: [world],
				resolveInvocation: (_tool, input) => invocation((input as { command: string }).command),
			},
			{ resourceCacheMaxEntries: 1, prepareObservedOperations: false },
		);
		try {
			await host.start();
			const command = exposeToControlledProcesses ? "./subcompute-process" : "cat input.txt";
			expect(textOutput(await host.execute("bash", { command }))).toBe("published by speculation\n");
			const nativeCalls = host.fallbacks.length;
			await host.preview("bash", { command });
			await host.ready();
			expect(textOutput(await host.execute("bash", { command }))).toBe("published by speculation\n");
			expect(host.fallbacks.length, JSON.stringify(host.events)).toBe(nativeCalls);
			await host.preview("bash", { command: "printf second" });
			await host.ready(2);
			expect(textOutput(await host.execute("bash", { command: "printf second" }))).toBe("second");
			// Public storage maintenance retires completed memory owners while retaining valid SQLite certificates.
			await world.storage!.maintain("gc");
			const database = new DatabaseSync(path.join(storeRoot, "metadata.sqlite"), { readOnly: true });
			try {
				const rows = database.prepare("SELECT value FROM certificates").all();
				expect(rows.length).toBeGreaterThan(0);
				expect(
					rows.some((row) => JSON.parse(String(row.value)).producer.execution.authority === "speculative"),
				).toBe(true);
			} finally {
				database.close();
			}
			const before = backend.actorMetrics().wholeCommandHits;
			expect(textOutput(await host.execute("bash", { command }))).toBe("published by speculation\n");
			expect(backend.actorMetrics().wholeCommandHits, backend.actorMetrics().lastError).toBe(before + 1);
			expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
		} finally {
			await host.dispose();
			await rm(root, { recursive: true, force: true });
		}
	},
);
