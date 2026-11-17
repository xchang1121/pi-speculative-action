import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { createBashTool } from "@earendil-works/pi-coding-agent";
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
		const fixture = await createLinuxProcessBenchmark("pi-stream-contract-", "git", {}, os.homedir());
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
