import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LinuxProcessReuseBackend, type LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { workspaceSandboxFingerprint } from "../src/workspace-sandbox.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";

const INPUT_BYTES = 32 * 1024 * 1024;
const MEASURED_RUNS = 3;
const HELPER_SOURCE = String.raw`
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int write_all(int fd, const unsigned char *data, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);
    if (written < 0) { if (errno == EINTR) continue; return -1; }
    data += written; length -= (size_t)written;
  }
  return 0;
}

static int make_parents(const char *target) {
  char buffer[4096];
  size_t length = strlen(target);
  if (length == 0 || length >= sizeof(buffer)) return -1;
  memcpy(buffer, target, length + 1);
  for (char *cursor = buffer + 1; *cursor; cursor++) {
    if (*cursor != '/') continue;
    *cursor = 0;
    if (mkdir(buffer, 0750) != 0 && errno != EEXIST) return -1;
    *cursor = '/';
  }
  return 0;
}

int main(int argc, char **argv) {
  if (argc != 3 || make_parents(argv[2]) != 0) return 64;
  int input = open(argv[1], O_RDONLY);
  int output = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0640);
  if (input < 0 || output < 0) return 65;
  unsigned char buffer[65536];
  unsigned long long offset = 0;
  for (;;) {
    ssize_t length = read(input, buffer, sizeof(buffer));
    if (length == 0) break;
    if (length < 0) { if (errno == EINTR) continue; return 66; }
    for (ssize_t index = 0; index < length; index++, offset++) {
      unsigned int value = (unsigned int)buffer[index] ^ (unsigned int)offset;
      for (unsigned int round = 0; round < TRANSFORM_ROUNDS; round++) {
        value = (value * 1664525U + 1013904223U) ^ (value >> 13U);
      }
      buffer[index] = (unsigned char)(value ^ (value >> 8U) ^ (value >> 16U) ^ (value >> 24U));
    }
    if (write_all(output, buffer, (size_t)length) != 0) return 67;
  }
  close(input); close(output);
  return write_all(1, (const unsigned char *)"topology-generated\n", 19) == 0 ? 0 : 68;
}
`;

type NumericMetrics = Readonly<Record<string, number>>;

interface MeasuredRun {
	readonly label: string;
	readonly totalMs: number;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly resources: readonly string[];
	readonly metricDelta: NumericMetrics;
}

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const mode = argument("--mode") ?? "reuse";
if (mode !== "reuse" && mode !== "direct") throw new Error("--mode must be reuse or direct");
const transformRounds = integerArgument("--rounds", 96, 0, 4_096);
const sourceFiles = integerArgument("--source-files", 0, 0, 20_000);
const outputPath = argument("--output");
const workspaceDriver = workspaceDriverArgument(argument("--workspace-driver"));
const root = await mkdtemp(path.join(os.tmpdir(), "pi-topology-reuse-bench-"));
const workspace = path.join(root, "workspace");
const storeRoot = path.join(root, "process-reuse");
const outputDirectory = path.join(workspace, "generated");
const artifact = path.join(outputDirectory, "nested", "artifact.bin");
await mkdir(workspace);
const shellPath = "/bin/bash";
const environment = Object.freeze({
	PATH: `${workspace}:/home/${os.userInfo().username}/.local/bin:/usr/local/bin:/usr/bin:/bin`,
	HOME: os.homedir(),
	LANG: "C.UTF-8",
});
const localOperations = createLocalBashOperations({ shellPath });
const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(localOperations));
const backend = mode === "reuse" ? new LinuxProcessReuseBackend({ storeRoot }) : undefined;
const world = backend
	? createLinuxProcessExecutionWorld({ coordinator, backend, storeRoot, driver: workspaceDriver })
	: undefined;
const tool = createBashTool(workspace, {
	operations: coordinator.operations,
	shellPath,
	exposeSessionEnvironment: false,
	spawnHook: (context) => ({ ...context, env: { ...environment } }),
});

try {
	await prepareWorkspace(workspace);
	let status: Awaited<ReturnType<LinuxProcessReuseBackend["check"]>> | undefined;
	let executionFingerprint: string | undefined;
	let workspaceFingerprint: string | undefined;
	let routePreparationMs: number | undefined;
	if (backend && world) {
		status = await backend.check(true);
		if (status.state !== "ready") throw new Error(status.detail);
		const routePreparationStarted = performance.now();
		workspaceFingerprint = await workspaceSandboxFingerprint({ driver: workspaceDriver }, workspace);
		await world.prepare?.({ cwd: workspace });
		executionFingerprint = `${await backend.fingerprint()}:${workspaceFingerprint}`;
		routePreparationMs = performance.now() - routePreparationStarted;
	}

	const warmup = await runTask("warmup", "pi-topology-helper input.bin generated/nested/artifact.bin");
	if (mode === "reuse") {
		assert(warmup.metricDelta.misses === 1 && warmup.metricDelta.hits === 0, "warm-up did not execute once");
		assert(warmup.metricDelta.published === 1 && warmup.metricDelta.tainted === 0, "warm-up was not safely published");
		assert(
			JSON.stringify(warmup.resources) ===
				JSON.stringify(["generated", "generated/nested", "generated/nested/artifact.bin"]),
			"typed topology resources were not sealed",
		);
	}
	const expectedDigest = await fileDigest(artifact);

	const runs: MeasuredRun[] = [];
	for (let index = 0; index < MEASURED_RUNS; index++) {
		await rm(outputDirectory, { recursive: true, force: true });
		const run = await runTask(
			`${mode}-${index + 1}`,
			`printf 'parent-${index + 1}\\n'; command pi-topology-helper input.bin generated/nested/artifact.bin`,
		);
		if (mode === "reuse") {
			assert(run.metricDelta.hits === 1 && run.metricDelta.misses === 0, `reuse run ${index + 1} missed`);
		}
		assert((await fileDigest(artifact)) === expectedDigest, `run ${index + 1} artifact differs`);
		runs.push(run);
	}

	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		mode,
		workspaceDriver,
		...(workspaceFingerprint ? { workspaceFingerprint } : {}),
		host: {
			platform: process.platform,
			arch: process.arch,
			node: process.version,
			kernel: (await commandOutput("uname", ["-srm"])).trim(),
			...(status?.state === "ready"
				? {
						sandlock: (await commandOutput(status.sandlockBinary!, ["--version"])).trim(),
						strace: (await commandOutput(status.straceBinary!, ["-V"])).split(/\r?\n/)[0]?.trim(),
					}
				: {}),
		},
		subject: "stock Pi createBashTool running one process that creates two directories and a transformed artifact",
		fixture: {
			inputBytes: INPUT_BYTES,
			transformRoundsPerByte: transformRounds,
			sourceFiles,
			measuredRuns: MEASURED_RUNS,
		},
		assertions: {
			allArtifactsEqual: true,
			...(mode === "reuse" ? { warmupPublished: true, typedTopologyCaptured: true, allCrossParentHits: true } : {}),
		},
		warmup,
		runs,
		summary: {
			...(routePreparationMs !== undefined ? { routePreparationMs } : {}),
			medianMs: median(runs.map((run) => run.totalMs)),
			...(backend ? { metrics: numericMetrics(backend.metrics()) } : {}),
		},
	};
	const rendered = `${JSON.stringify(result, null, 2)}\n`;
	if (outputPath) {
		await mkdir(path.dirname(path.resolve(outputPath)), { recursive: true });
		await writeFile(path.resolve(outputPath), rendered, "utf8");
	}
	process.stdout.write(rendered);

	async function runTask(label: string, command: string): Promise<MeasuredRun> {
		const args = { command };
		if (!world || !backend || !executionFingerprint) {
			const started = performance.now();
			await tool.execute(`bench-${label}`, args, new AbortController().signal);
			return {
				label,
				totalMs: performance.now() - started,
				forkMs: 0,
				validationMs: 0,
				commitMs: 0,
				resources: [],
				metricDelta: {},
			};
		}
		const invocation = resolvePiToolInvocation("bash", args, { cwd: workspace, environment, shellPath });
		if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
		const action = PI_ACTION_SEMANTICS.buildKey("bash", args, workspace, "pi-topology-reuse-benchmark.v1", {
			fingerprint: executionFingerprint,
			context: invocation,
		});
		if (!action) throw new Error("Pi Bash action could not be keyed");
		const metricsBefore = numericMetrics(backend.metrics());
		const started = performance.now();
		const forkStarted = performance.now();
		const branch = await world.fork({
			cwd: workspace,
			tool,
			toolName: "bash",
			args,
			action,
			callID: `bench-${label}`,
			signal: new AbortController().signal,
		});
		const forkMs = performance.now() - forkStarted;
		if (branch.output.isError) throw new Error(JSON.stringify(branch.output.result));
		const validationStarted = performance.now();
		const validation = await branch.validate?.();
		const validationMs = performance.now() - validationStarted;
		if (validation?.status !== "valid") throw new Error(`branch validation failed: ${JSON.stringify(validation)}`);
		const commitStarted = performance.now();
		await branch.commit();
		const commitMs = performance.now() - commitStarted;
		const resources = [...branch.resources];
		branch.dispose();
		return {
			label,
			totalMs: performance.now() - started,
			forkMs,
			validationMs,
			commitMs,
			resources,
			metricDelta: subtractMetrics(metricsBefore, numericMetrics(backend.metrics())),
		};
	}
} finally {
	await world?.dispose?.();
	await rm(root, { recursive: true, force: true });
}

async function prepareWorkspace(workspace: string): Promise<void> {
	await writeFile(path.join(workspace, "pi-topology-helper.c"), HELPER_SOURCE, "utf8");
	await writeFile(path.join(workspace, "input.bin"), Buffer.alloc(INPUT_BYTES, 0x31));
	if (sourceFiles > 0) {
		const fixtureRoot = path.join(workspace, "fixture-files");
		await mkdir(fixtureRoot);
		for (let offset = 0; offset < sourceFiles; offset += 256) {
			await Promise.all(
				Array.from({ length: Math.min(256, sourceFiles - offset) }, (_value, index) => {
					const ordinal = offset + index;
					return writeFile(path.join(fixtureRoot, `${ordinal.toString().padStart(5, "0")}.txt`), `${ordinal}\n`, "utf8");
				}),
			);
		}
	}
	await commandOutput(
		"cc",
		[
			"-O2",
			"-Wall",
			"-Wextra",
			`-DTRANSFORM_ROUNDS=${transformRounds}U`,
			"-o",
			"pi-topology-helper",
			"pi-topology-helper.c",
		],
		workspace,
	);
	await commandOutput("git", ["init", "--quiet"], workspace);
	await commandOutput("git", ["config", "user.name", "Pi Topology Benchmark"], workspace);
	await commandOutput("git", ["config", "user.email", "benchmark@localhost"], workspace);
	await commandOutput("git", ["add", "."], workspace);
	await commandOutput("git", ["commit", "--quiet", "-m", "benchmark fixture"], workspace);
	await access(path.join(workspace, "pi-topology-helper"));
}

function fileDigest(target: string): Promise<string> {
	return new Promise((resolve, reject) => {
		const hash = createHash("sha256");
		const stream = createReadStream(target);
		stream.on("data", (chunk) => hash.update(chunk));
		stream.once("error", reject);
		stream.once("end", () => resolve(hash.digest("hex")));
	});
}

function commandOutput(executable: string, args: readonly string[], cwd?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { cwd, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
		});
	});
}

function numericMetrics(metrics: LinuxProcessReuseMetrics): NumericMetrics {
	return Object.fromEntries(
		Object.entries(metrics).filter((entry): entry is [string, number] => typeof entry[1] === "number"),
	);
}

function subtractMetrics(before: NumericMetrics, after: NumericMetrics): NumericMetrics {
	return Object.fromEntries(Object.entries(after).map(([name, value]) => [name, value - (before[name] ?? 0)]));
}

function median(values: readonly number[]): number {
	const sorted = [...values].sort((left, right) => left - right);
	return sorted[Math.floor(sorted.length / 2)]!;
}

function argument(name: string): string | undefined {
	const index = process.argv.indexOf(name);
	if (index < 0) return undefined;
	const value = process.argv[index + 1];
	if (!value || value.startsWith("--")) throw new Error(`${name} requires a value`);
	return value;
}

function workspaceDriverArgument(value: string | undefined): "auto" | "git" | "overlayfs" {
	if (value === undefined || value === "auto" || value === "git" || value === "overlayfs") return value ?? "auto";
	throw new Error(`--workspace-driver must be auto, git, or overlayfs: ${value}`);
}

function integerArgument(name: string, fallback: number, minimum: number, maximum: number): number {
	const raw = argument(name);
	if (raw === undefined) return fallback;
	const value = Number(raw);
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
	}
	return value;
}

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}
