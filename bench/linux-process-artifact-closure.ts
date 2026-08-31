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
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";

const INPUT_BYTES = 128 * 1024 * 1024;
const HIT_RUNS = 3;
const HELPER_SOURCE = String.raw`
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

static int write_all(int fd, const char *data, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);
    if (written < 0) { if (errno == EINTR) continue; return -1; }
    data += written; length -= (size_t)written;
  }
  return 0;
}

int main(int argc, char **argv) {
  if (argc != 3) return 64;
  struct timespec delay = {0, 300000000};
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
  int input = open(argv[1], O_RDONLY);
  int output = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (input < 0 || output < 0) return 65;
  char buffer[65536];
  for (;;) {
    ssize_t length = read(input, buffer, sizeof(buffer));
    if (length == 0) break;
    if (length < 0) { if (errno == EINTR) continue; return 66; }
    if (write_all(output, buffer, (size_t)length) != 0) return 67;
  }
  close(input); close(output);
  return write_all(1, "copied\n", 7) == 0 ? 0 : 68;
}
`;

type NumericMetrics = Readonly<Record<string, number>>;

interface MeasuredRun {
	readonly label: string;
	readonly totalMs: number;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly metricDelta: NumericMetrics;
}

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const outputPath = argument("--output");
const root = await mkdtemp(path.join(os.tmpdir(), "pi-artifact-closure-bench-"));
const workspace = path.join(root, "workspace");
const storeRoot = path.join(root, "process-reuse");
await mkdir(workspace);
const shellPath = "/bin/bash";
const environment = Object.freeze({
	PATH: `${workspace}:/home/${os.userInfo().username}/.local/bin:/usr/local/bin:/usr/bin:/bin`,
	HOME: os.homedir(),
	LANG: "C.UTF-8",
});
const localOperations = createLocalBashOperations({ shellPath });
const coordinator = new ProcessExecutionCoordinator(adaptProcessToolOperations(localOperations));
const backend = new LinuxProcessReuseBackend({ storeRoot });
const world = createLinuxProcessExecutionWorld({ coordinator, backend, storeRoot });
const tool = createBashTool(workspace, {
	operations: coordinator.operations,
	shellPath,
	exposeSessionEnvironment: false,
	spawnHook: (context) => ({ ...context, env: { ...environment } }),
});

try {
	await prepareWorkspace(workspace);
	const status = await backend.check(true);
	if (status.state !== "ready") throw new Error(status.detail);
	await world.prepare?.({ cwd: workspace });
	const executionFingerprint = await backend.fingerprint();
	const cold = await runTask("cold", "pi-artifact-helper input.bin artifact.bin", executionFingerprint);
	assert(cold.metricDelta.misses === 1 && cold.metricDelta.hits === 0, "cold execution was not published");
	const expectedDigest = await fileDigest(path.join(workspace, "input.bin"));
	assert((await fileDigest(path.join(workspace, "artifact.bin"))) === expectedDigest, "cold artifact differs");

	const hits: MeasuredRun[] = [];
	for (let index = 0; index < HIT_RUNS; index++) {
		await rm(path.join(workspace, "artifact.bin"), { force: true });
		const hit = await runTask(
			`hit-${index + 1}`,
			`printf 'parent-${index + 1}\\n'; command pi-artifact-helper input.bin artifact.bin`,
			executionFingerprint,
		);
		assert(hit.metricDelta.hits === 1 && hit.metricDelta.misses === 0, `hit ${index + 1} missed`);
		assert((await fileDigest(path.join(workspace, "artifact.bin"))) === expectedDigest, `hit ${index + 1} artifact differs`);
		hits.push(hit);
	}

	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: {
			platform: process.platform,
			arch: process.arch,
			node: process.version,
			kernel: (await commandOutput("uname", ["-srm"])).trim(),
			sandlock: (await commandOutput(status.sandlockBinary!, ["--version"])).trim(),
			strace: (await commandOutput(status.straceBinary!, ["-V"])).split(/\r?\n/)[0]?.trim(),
		},
		subject: "stock Pi createBashTool replaying one verified 128 MiB regular-file effect",
		fixture: { inputBytes: INPUT_BYTES, helperDelayMs: 300, hitRuns: HIT_RUNS },
		assertions: {
			coldPublished: true,
			allDifferentParentHits: true,
			allArtifactsEqual: true,
			...("validationArtifactsLoaded" in hits[0]!.metricDelta ? { verifiedClosureLoaded: true } : {}),
		},
		cold,
		hits,
		summary: {
			hitMedianMs: median(hits.map((run) => run.totalMs)),
			validationMedianMs: median(hits.map((run) => run.metricDelta.validationMs ?? 0)),
			coldToHitMedianSpeedup: cold.totalMs / median(hits.map((run) => run.totalMs)),
			metrics: numericMetrics(backend.metrics()),
		},
	};
	const rendered = `${JSON.stringify(result, null, 2)}\n`;
	if (outputPath) {
		await mkdir(path.dirname(path.resolve(outputPath)), { recursive: true });
		await writeFile(path.resolve(outputPath), rendered, "utf8");
	}
	process.stdout.write(rendered);

	async function runTask(label: string, command: string, executionFingerprint: string): Promise<MeasuredRun> {
		const args = { command };
		const invocation = resolvePiToolInvocation("bash", args, { cwd: workspace, environment, shellPath });
		if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
		const action = PI_ACTION_SEMANTICS.buildKey("bash", args, workspace, "pi-artifact-closure-benchmark.v1", {
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
		branch.dispose();
		return {
			label,
			totalMs: performance.now() - started,
			forkMs,
			validationMs,
			commitMs,
			metricDelta: subtractMetrics(metricsBefore, numericMetrics(backend.metrics())),
		};
	}
} finally {
	await world.dispose?.();
	await rm(root, { recursive: true, force: true });
}

async function prepareWorkspace(workspace: string): Promise<void> {
	await writeFile(path.join(workspace, "pi-artifact-helper.c"), HELPER_SOURCE, "utf8");
	await writeFile(path.join(workspace, "input.bin"), Buffer.alloc(INPUT_BYTES, 0x5a));
	await commandOutput("cc", ["-O2", "-Wall", "-Wextra", "-o", "pi-artifact-helper", "pi-artifact-helper.c"], workspace);
	await commandOutput("git", ["init", "--quiet"], workspace);
	await commandOutput("git", ["config", "user.name", "Pi Artifact Benchmark"], workspace);
	await commandOutput("git", ["config", "user.email", "benchmark@localhost"], workspace);
	await commandOutput("git", ["add", "pi-artifact-helper.c", "pi-artifact-helper", "input.bin"], workspace);
	await commandOutput("git", ["commit", "--quiet", "-m", "benchmark fixture"], workspace);
	await access(path.join(workspace, "pi-artifact-helper"));
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
	if (!value || value.startsWith("--")) throw new Error(`${name} requires a path`);
	return value;
}

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}
