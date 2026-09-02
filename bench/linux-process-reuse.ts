import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import type { LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import { observeStrace, straceCommand } from "../src/strace-observer.ts";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeDirectBash,
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
	readonly label: "cold" | "whole_command_hit" | "cross_parent_hit" | "dependency_invalidation";
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
	const directCommand = `exec /bin/bash ${shellQuote(path.join(workspace, "trace-workload.sh"))}`;
	const directExecution = await executeDirectBash(fixture, { label: "direct", command: directCommand });
	const direct = {
		totalMs: directExecution.totalMs,
		output: textOutput(directExecution.output),
		artifact: await readFile(path.join(workspace, "artifact.txt"), "utf8"),
	};
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	const traceRoot = path.join(fixture.root, "actor-trace");
	const tracePrefix = path.join(traceRoot, "process");
	await mkdir(traceRoot);
	const tracedCommand = straceCommand(status.straceBinary, tracePrefix, [
		"/bin/bash",
		path.join(workspace, "trace-workload.sh"),
	]);
	const traceExecution = await executeDirectBash(fixture, {
		label: "trace",
		command: `exec ${tracedCommand.map(shellQuote).join(" ")}`,
	});
	const traceObservation = await observeStrace(tracePrefix, "/bin/bash", workspace);
	const trace = {
		totalMs: traceExecution.totalMs,
		output: textOutput(traceExecution.output),
		artifact: await readFile(path.join(workspace, "artifact.txt"), "utf8"),
		observation: {
			complete: traceObservation.complete,
			paths: traceObservation.paths.length,
			taints: traceObservation.taints,
			tracedProcesses: traceObservation.tracedProcesses,
			strictCertificateEligible: traceObservation.complete && traceObservation.taints.length === 0,
		},
	};
	assert(
		trace.output === direct.output && trace.artifact === direct.artifact,
		`strace changed Actor-visible results: ${JSON.stringify({ direct, trace })}`,
	);
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	const runs: MeasuredRun[] = [];
	const coldCommand = "printf 'parent-a\\n'; pi-reuse-helper input.txt artifact.txt; printf 'parent-a-done\\n'";
	runs.push(await runTask("cold", coldCommand, executionFingerprint));
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	const actorMetricsBefore = backend.metrics();
	const actorReplay = await executeDirectBash(fixture, { label: "actor_replay", command: coldCommand });
	const actorReplayResult = {
		totalMs: actorReplay.totalMs,
		output: textOutput(actorReplay.output),
		artifact: await readFile(path.join(workspace, "artifact.txt"), "utf8"),
		metricDelta: metricDelta(actorMetricsBefore, backend.metrics()),
	};
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	runs.push(await runTask("whole_command_hit", coldCommand, executionFingerprint));
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

	const [cold, wholeHit, childHit, invalidated] = runs;
	assert(cold?.artifact === "alpha\n", "cold artifact differs");
	assert(actorReplayResult.artifact === cold.artifact && actorReplayResult.output === cold.output, "Actor replay differs");
	assert(wholeHit?.artifact === cold.artifact, "whole-command artifact differs");
	assert(childHit?.artifact === cold.artifact, "child-replayed artifact differs");
	const orderedChildOutput = "child-out:alpha\nchild-err\n";
	assert(cold?.output.includes(orderedChildOutput), "cold child output differs");
	assert(wholeHit?.output === cold.output, "whole-command output differs");
	assert(childHit?.output.includes(orderedChildOutput), "replayed child output differs");
	assert(invalidated?.artifact === "beta\n", "changed input was not observed");
	assert(
		wholeHit.metricDelta.wholeCommandHits === 1 && wholeHit.metricDelta.hits === 0,
		`complete Bash did not hit: ${JSON.stringify(wholeHit.metricDelta)}`,
	);
	assert(
		actorReplayResult.metricDelta.wholeCommandHits === 1 && actorReplayResult.metricDelta.wholeCommandMisses === 0,
		`Actor process outlet did not hit: ${JSON.stringify(actorReplayResult.metricDelta)}`,
	);
	assert(
		childHit.metricDelta.hits === 1 && childHit.metricDelta.crossTurnHits === 1 && childHit.metricDelta.misses === 0,
		`second parent did not hit: ${JSON.stringify(childHit.metricDelta)}`,
	);
	assert(
		invalidated.metricDelta.hits === 0 && invalidated.metricDelta.misses === 1,
		`changed input did not miss: ${JSON.stringify(invalidated.metricDelta)}`,
	);
	assert(wholeHit.totalMs < cold.totalMs, "whole-command hit was not faster than cold execution");
	assert(childHit.totalMs < cold.totalMs, "child cache hit was not faster than cold execution");

	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
		subject: "stock Pi createBashTool through linux_process_reuse WorldBranch",
		workspaceDriver,
		workspaceFingerprint,
		assertions: {
			directAndTraceEquivalent: true,
			actorReplayWithoutFork: true,
			wholeCommandReplay: true,
			differentParentCommands: true,
			orderedChildOutputEqual: true,
			regularFileEffectEqual: true,
			adoptionFreshnessValid: true,
			changedInputForcedMiss: true,
		},
		runs,
		direct,
		trace,
		actorReplay: actorReplayResult,
		summary: {
			routePreparationMs,
			traceOverheadMs: trace.totalMs - direct.totalMs,
			traceSlowdown: trace.totalMs / direct.totalMs,
			coldToWholeCommandHitSpeedup: cold.totalMs / wholeHit.totalMs,
			coldToActorReplaySpeedup: cold.totalMs / actorReplayResult.totalMs,
			coldToCrossParentHitSpeedup: cold.totalMs / childHit.totalMs,
			coldForkToCrossParentHitForkSpeedup: cold.forkMs / childHit.forkMs,
			latencySavedMs: cold.totalMs - wholeHit.totalMs,
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
	await writeFile(
		path.join(workspace, "trace-workload.sh"),
		"printf 'trace-parent\\n'; pi-reuse-helper input.txt artifact.txt 2>&1; printf 'trace-parent-done\\n'\n",
		"utf8",
	);
	await compileBenchmarkHelper(workspace, { source: "pi-reuse-helper.c", output: "pi-reuse-helper" });
	await commitBenchmarkFixture(workspace, "Pi Bash Reuse Benchmark", [
		"pi-reuse-helper.c",
		"pi-reuse-helper",
		"input.txt",
		"trace-workload.sh",
	]);
}

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'\\''`)}'`;
}
