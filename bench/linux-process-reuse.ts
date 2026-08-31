import { execFile } from "node:child_process";
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LinuxProcessReuseBackend, type LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";

const HELPER_SOURCE = String.raw`
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
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
  struct timespec delay = {1, 0};
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
  int input = open(argv[1], O_RDONLY);
  if (input < 0) return 65;
  char value[4096];
  ssize_t length = read(input, value, sizeof(value));
  close(input);
  if (length < 0) return 66;
  int output = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (output < 0) return 67;
  if (write_all(output, value, (size_t)length) != 0) return 68;
  close(output);
  write_all(1, "child-out:", 10); write_all(1, value, (size_t)length);
  write_all(2, "child-err\n", 10);
  return 0;
}
`;

interface MeasuredRun {
	readonly label: "cold" | "cross_parent_hit" | "dependency_invalidation";
	readonly command: string;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly totalMs: number;
	readonly validation: "valid";
	readonly output: string;
	readonly artifact: string;
	readonly metricDelta: LinuxProcessReuseMetrics;
}

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const outputPath = argument("--output");
const root = await mkdtemp(path.join(os.tmpdir(), "pi-bash-reuse-bench-"));
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
	const runs: MeasuredRun[] = [];

	runs.push(
		await runTask(
			"cold",
			"printf 'parent-a\\n'; pi-reuse-helper input.txt artifact.txt; printf 'parent-a-done\\n'",
			executionFingerprint,
		),
	);
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	runs.push(
		await runTask(
			"cross_parent_hit",
			"printf 'parent-b\\n'; command pi-reuse-helper input.txt artifact.txt; printf 'parent-b-done\\n'",
			executionFingerprint,
		),
	);
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	await writeFile(path.join(workspace, "input.txt"), "beta\n", "utf8");
	runs.push(
		await runTask(
			"dependency_invalidation",
			"printf 'parent-c\\n'; pi-reuse-helper input.txt artifact.txt; printf 'parent-c-done\\n'",
			executionFingerprint,
		),
	);

	const [cold, hit, invalidated] = runs;
	assert(cold?.artifact === "alpha\n", "cold artifact differs");
	assert(hit?.artifact === cold.artifact, "replayed artifact differs");
	const orderedChildOutput = "child-out:alpha\nchild-err\n";
	assert(cold?.output.includes(orderedChildOutput), "cold child output differs");
	assert(hit?.output.includes(orderedChildOutput), "replayed child output differs");
	assert(invalidated?.artifact === "beta\n", "changed input was not observed");
	assert(hit.metricDelta.hits === 1 && hit.metricDelta.misses === 0, "second parent did not hit");
	assert(invalidated.metricDelta.hits === 0 && invalidated.metricDelta.misses === 1, "changed input did not miss");
	assert(hit.totalMs < cold.totalMs, "cache hit was not faster than cold execution");

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
		subject: "stock Pi createBashTool through linux_process_reuse WorldBranch",
		assertions: {
			differentParentCommands: true,
			orderedChildOutputEqual: true,
			regularFileEffectEqual: true,
			adoptionFreshnessValid: true,
			changedInputForcedMiss: true,
		},
		runs,
		summary: {
			coldToCrossParentHitSpeedup: cold.totalMs / hit.totalMs,
			coldForkToCrossParentHitForkSpeedup: cold.forkMs / hit.forkMs,
			latencySavedMs: cold.totalMs - hit.totalMs,
			metrics: backend.metrics(),
		},
	};
	const rendered = `${JSON.stringify(result, null, 2)}\n`;
	if (outputPath) {
		await mkdir(path.dirname(path.resolve(outputPath)), { recursive: true });
		await writeFile(path.resolve(outputPath), rendered, "utf8");
	}
	process.stdout.write(rendered);

	async function runTask(
		label: MeasuredRun["label"],
		command: string,
		executionFingerprint: string,
	): Promise<MeasuredRun> {
		const args = { command };
		const invocation = resolvePiToolInvocation("bash", args, {
			cwd: workspace,
			environment,
			shellPath,
		});
		if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
		const action = PI_ACTION_SEMANTICS.buildKey("bash", args, workspace, "pi-bash-benchmark.v1", {
			fingerprint: executionFingerprint,
			context: invocation,
		});
		if (!action) throw new Error("Pi Bash action could not be keyed");
		const metricsBefore = backend.metrics();
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
		if (branch.output.isError) throw new Error(textOutput(branch.output.result));
		const validationStarted = performance.now();
		const validation = await branch.validate?.();
		const validationMs = performance.now() - validationStarted;
		if (validation?.status !== "valid") throw new Error(`branch validation failed: ${JSON.stringify(validation)}`);
		const commitStarted = performance.now();
		const committed = await branch.commit();
		const commitMs = performance.now() - commitStarted;
		branch.dispose();
		const metricsAfter = backend.metrics();
		return {
			label,
			command,
			forkMs,
			validationMs,
			commitMs,
			totalMs: performance.now() - started,
			validation: "valid",
			output: textOutput(committed.result),
			artifact: await readFile(path.join(workspace, "artifact.txt"), "utf8"),
			metricDelta: metricDelta(metricsBefore, metricsAfter),
		};
	}
} finally {
	await world.dispose?.();
	await rm(root, { recursive: true, force: true });
}

async function prepareWorkspace(workspace: string): Promise<void> {
	await writeFile(path.join(workspace, "pi-reuse-helper.c"), HELPER_SOURCE, "utf8");
	await writeFile(path.join(workspace, "input.txt"), "alpha\n", "utf8");
	await commandOutput("cc", ["-O2", "-Wall", "-Wextra", "-o", "pi-reuse-helper", "pi-reuse-helper.c"], workspace);
	await commandOutput("git", ["init", "--quiet"], workspace);
	await commandOutput("git", ["config", "user.name", "Pi Bash Reuse Benchmark"], workspace);
	await commandOutput("git", ["config", "user.email", "benchmark@localhost"], workspace);
	await commandOutput("git", ["add", "pi-reuse-helper.c", "pi-reuse-helper", "input.txt"], workspace);
	await commandOutput("git", ["commit", "--quiet", "-m", "benchmark fixture"], workspace);
	await access(path.join(workspace, "pi-reuse-helper"));
}

function commandOutput(executable: string, args: readonly string[], cwd?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { cwd, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
		});
	});
}

function textOutput(result: { readonly content: readonly { readonly type: string; readonly text?: string }[] }): string {
	return result.content
		.filter((item): item is { readonly type: "text"; readonly text: string } => item.type === "text" && typeof item.text === "string")
		.map((item) => item.text)
		.join("\n");
}

function metricDelta(before: LinuxProcessReuseMetrics, after: LinuxProcessReuseMetrics): LinuxProcessReuseMetrics {
	return {
		requests: after.requests - before.requests,
		hits: after.hits - before.hits,
		misses: after.misses - before.misses,
		bypasses: after.bypasses - before.bypasses,
		published: after.published - before.published,
		tainted: after.tainted - before.tainted,
		validationMs: after.validationMs - before.validationMs,
		validationCandidates: after.validationCandidates - before.validationCandidates,
		validationPathsets: after.validationPathsets - before.validationPathsets,
		validationFilesRead: after.validationFilesRead - before.validationFilesRead,
		validationBytesRead: after.validationBytesRead - before.validationBytesRead,
		validationArtifactsLoaded: after.validationArtifactsLoaded - before.validationArtifactsLoaded,
		validationArtifactBytesRead: after.validationArtifactBytesRead - before.validationArtifactBytesRead,
		executionMs: after.executionMs - before.executionMs,
		...(after.lastError !== before.lastError && after.lastError ? { lastError: after.lastError } : {}),
	};
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
