import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { observeStrace, straceCommand } from "../src/strace-observer.ts";
import {
	argument,
	assert,
	benchmarkWriteAllC,
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

${benchmarkWriteAllC()}

int main(int argc, char **argv) {
  if (argc != 3) return 64;
  int cwd = open(".", O_RDONLY | O_DIRECTORY);
  if (cwd < 0 || chdir("/") != 0 || fchdir(cwd) != 0) return 69;
  close(cwd);
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

const outputPath = argument("--output");
const workspaceDriver = workspaceDriverArgument(argument("--workspace-driver"));
const fixture = await createLinuxProcessBenchmark("pi-bash-reuse-bench-", workspaceDriver);
const { backend, workspace } = fixture;
const artifactPath = path.join(workspace, "artifact.txt");

try {
	await prepareWorkspace(workspace);
	const { status, executionFingerprint, workspaceFingerprint, routePreparationMs } = await prepareLinuxProcessReuse(
		fixture,
		{ workspaceDriver, includeWorkspaceFingerprint: true },
	);
	const coldCommand = "printf 'parent-a\\n'; ./pi-reuse-helper input.txt artifact.txt 2>&1; printf 'parent-a-done\\n'";
	const direct = await runDirect("direct", coldCommand);
	const traceRoot = path.join(fixture.root, "actor-trace");
	const tracePrefix = path.join(traceRoot, "process");
	await mkdir(traceRoot);
	const tracedCommand = straceCommand(status.straceBinary, tracePrefix, [
		"/bin/bash",
		"-c",
		coldCommand,
	]);
	const traceResult = await runDirect("trace", `exec ${tracedCommand.map(shellQuote).join(" ")}`);
	const traceObservation = await observeStrace(tracePrefix, "/bin/bash", workspace);
	const trace = {
		...traceResult,
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
	const cold = await runTask("cold", coldCommand);
	const actorMetricsBefore = backend.metrics();
	const actorAttemptResult = {
		...await runDirect("actor_attempt", coldCommand),
		metricDelta: metricDelta(actorMetricsBefore, backend.metrics()),
	};
	const sameParentHit = await runTask("same_parent_child_hit", coldCommand);
	const childHit = await runTask("cross_parent_hit",
		"printf 'parent-b\\n'; command ./pi-reuse-helper input.txt artifact.txt 2>&1; printf 'parent-b-done\\n'",
	);
	await writeFile(path.join(workspace, "input.txt"), "beta\n", "utf8");
	const invalidated = await runTask("dependency_invalidation",
		"printf 'parent-c\\n'; ./pi-reuse-helper input.txt artifact.txt 2>&1; printf 'parent-c-done\\n'",
	);

	assert(cold.artifact === "alpha\n", "cold artifact differs");
	assert(direct.artifact === cold.artifact && direct.output === cold.output, "direct Actor execution differs");
	assert(actorAttemptResult.artifact === cold.artifact && actorAttemptResult.output === cold.output, "Actor fallback differs");
	assert(sameParentHit.artifact === cold.artifact, "same-parent artifact differs");
	assert(childHit.artifact === cold.artifact, "child-replayed artifact differs");
	assert(cold.output === "parent-a\nchild-out:alpha\nchild-err\nparent-a-done\n", "cold child output differs");
	assert(sameParentHit.output === cold.output, "same-parent output differs");
	assert(childHit.output === cold.output.replaceAll("parent-a", "parent-b"), "replayed child output differs");
	assert(invalidated.artifact === "beta\n" && invalidated.output === cold.output.replaceAll("parent-a", "parent-c").replace("alpha", "beta"),
		"changed input was not observed");
	assert(
		sameParentHit.metricDelta.wholeCommandHits === 0 &&
			sameParentHit.metricDelta.wholeCommandMisses === 1 &&
			sameParentHit.metricDelta.hits === 1,
		`same-parent child did not hit safely: ${JSON.stringify(sameParentHit.metricDelta)}`,
	);
	assert(
		actorAttemptResult.metricDelta.wholeCommandHits === 0 && actorAttemptResult.metricDelta.wholeCommandMisses === 1,
		`Actor process outlet crossed execution semantics: ${JSON.stringify(actorAttemptResult.metricDelta)}`,
	);
	assert(
		childHit.metricDelta.hits === 1 && childHit.metricDelta.crossTurnHits === 1 && childHit.metricDelta.misses === 0,
		`second parent did not hit: ${JSON.stringify(childHit.metricDelta)}`,
	);
	assert(
		invalidated.metricDelta.hits === 0 && invalidated.metricDelta.misses === 1,
		`changed input did not miss: ${JSON.stringify(invalidated.metricDelta)}`,
	);
	assert(sameParentHit.totalMs < cold.totalMs, "same-parent child hit was not faster than cold execution");
	assert(childHit.totalMs < cold.totalMs, "child cache hit was not faster than cold execution");

	const result = {
		schemaVersion: 3,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
		subject: "stock Pi createBashTool through linux_process_reuse WorldBranch",
		workspaceDriver,
		workspaceFingerprint,
		assertions: {
			directAndTraceEquivalent: true,
			directoryFdCwdTransition: true,
			actorCrossEnvironmentReplayRejected: true,
			sameParentChildReplay: true,
			differentParentCommands: true,
			orderedChildOutputEqual: true,
			regularFileEffectEqual: true,
			adoptionFreshnessValid: true,
			changedInputForcedMiss: true,
		},
		runs: [cold, sameParentHit, childHit, invalidated],
		direct,
		trace,
		actorAttempt: actorAttemptResult,
		summary: {
			routePreparationMs,
			traceOverheadMs: trace.totalMs - direct.totalMs,
			traceSlowdown: trace.totalMs / direct.totalMs,
			coldToSameParentChildHitSpeedup: cold.totalMs / sameParentHit.totalMs,
			actorFallbackOverheadMs: actorAttemptResult.totalMs - direct.totalMs,
			coldToCrossParentHitSpeedup: cold.totalMs / childHit.totalMs,
			coldForkToCrossParentHitForkSpeedup: cold.forkMs / childHit.forkMs,
			sameParentChildLatencySavedMs: cold.totalMs - sameParentHit.totalMs,
			metrics: backend.metrics(),
		},
	};
	await writeBenchmarkReport(result, outputPath);

	async function runTask(label: string, command: string) {
		await rm(artifactPath, { force: true });
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
			...execution.measurement,
			validation: "valid",
			output: textOutput(execution.output.result),
			artifact: await readFile(artifactPath, "utf8"),
		};
	}
	async function runDirect(label: string, command: string) {
		await rm(artifactPath, { force: true });
		const execution = await executeDirectBash(fixture, { label, command });
		return { totalMs: execution.totalMs, output: textOutput(execution.output), artifact: await readFile(artifactPath, "utf8") };
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

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'\\''`)}'`;
}
