import { readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import type { LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeReusableBash,
	linuxBenchmarkHost,
	metricDelta,
	prepareLinuxProcessReuse,
	textOutput,
	workspaceDriverArgument,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

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

const outputPath = argument("--output");
const workspaceDriver = workspaceDriverArgument(argument("--workspace-driver"));
const fixture = await createLinuxProcessBenchmark("pi-bash-reuse-bench-", workspaceDriver);
const { backend, workspace } = fixture;

try {
	await prepareWorkspace(workspace);
	const { status, executionFingerprint, workspaceFingerprint, routePreparationMs } = await prepareLinuxProcessReuse(
		fixture,
		{ workspaceDriver, includeWorkspaceFingerprint: true },
	);
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
	assert(
		hit.metricDelta.hits === 1 && hit.metricDelta.crossTurnHits === 1 && hit.metricDelta.misses === 0,
		`second parent did not hit: ${JSON.stringify(hit.metricDelta)}`,
	);
	assert(
		invalidated.metricDelta.hits === 0 && invalidated.metricDelta.misses === 1,
		`changed input did not miss: ${JSON.stringify(invalidated.metricDelta)}`,
	);
	assert(hit.totalMs < cold.totalMs, "cache hit was not faster than cold execution");

	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
		subject: "stock Pi createBashTool through linux_process_reuse WorldBranch",
		workspaceDriver,
		workspaceFingerprint,
		assertions: {
			differentParentCommands: true,
			orderedChildOutputEqual: true,
			regularFileEffectEqual: true,
			adoptionFreshnessValid: true,
			changedInputForcedMiss: true,
		},
		runs,
		summary: {
			routePreparationMs,
			coldToCrossParentHitSpeedup: cold.totalMs / hit.totalMs,
			coldForkToCrossParentHitForkSpeedup: cold.forkMs / hit.forkMs,
			latencySavedMs: cold.totalMs - hit.totalMs,
			metrics: backend.metrics(),
		},
	};
	await writeBenchmarkReport(result, outputPath);

	async function runTask(
		label: MeasuredRun["label"],
		command: string,
		executionFingerprint: string,
	): Promise<MeasuredRun> {
		const execution = await executeReusableBash(fixture, {
			label,
			command,
			actionNamespace: "pi-bash-benchmark.v1",
			executionFingerprint,
			executionScope: { sessionID: "benchmark", turnID: label },
		});
		return {
			label,
			command,
			forkMs: execution.forkMs,
			validationMs: execution.validationMs,
			commitMs: execution.commitMs,
			totalMs: execution.totalMs,
			validation: "valid",
			output: textOutput(execution.output.result),
			artifact: await readFile(path.join(workspace, "artifact.txt"), "utf8"),
			metricDelta: metricDelta(execution.metricsBefore, execution.metricsAfter),
		};
	}
} finally {
	await fixture.dispose();
}

async function prepareWorkspace(workspace: string): Promise<void> {
	await writeFile(path.join(workspace, "pi-reuse-helper.c"), HELPER_SOURCE, "utf8");
	await writeFile(path.join(workspace, "input.txt"), "alpha\n", "utf8");
	await compileBenchmarkHelper(workspace, { source: "pi-reuse-helper.c", output: "pi-reuse-helper" });
	await commitBenchmarkFixture(workspace, "Pi Bash Reuse Benchmark", [
		"pi-reuse-helper.c",
		"pi-reuse-helper",
		"input.txt",
	]);
}
