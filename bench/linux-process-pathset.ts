import { readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeReusableBash,
	linuxBenchmarkHost,
	median,
	numericMetrics,
	prepareLinuxProcessReuse,
	subtractMetrics,
	textOutput,
	type NumericMetrics,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

const HISTORY = 8;
const INPUT_BYTES = 32 * 1024 * 1024;
const HELPER_SOURCE = String.raw`
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
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
  struct timespec delay = {0, 500000000};
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
  int input = open(argv[1], O_RDONLY);
  if (input < 0) return 65;
  uint64_t hash = UINT64_C(1469598103934665603);
  unsigned char buffer[65536];
  for (;;) {
    ssize_t length = read(input, buffer, sizeof(buffer));
    if (length == 0) break;
    if (length < 0) { if (errno == EINTR) continue; return 66; }
    for (ssize_t index = 0; index < length; index++) {
      hash ^= buffer[index]; hash *= UINT64_C(1099511628211);
    }
  }
  close(input);
  char value[64];
  int length = snprintf(value, sizeof(value), "%016llx\n", (unsigned long long)hash);
  int output = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (output < 0 || write_all(output, value, (size_t)length) != 0) return 67;
  close(output);
  return write_all(1, value, (size_t)length) == 0 ? 0 : 68;
}
`;

interface MeasuredRun {
	readonly label: string;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly totalMs: number;
	readonly output: string;
	readonly artifact: string;
	readonly metricDelta: NumericMetrics;
}

const outputPath = argument("--output");
const fixture = await createLinuxProcessBenchmark("pi-bash-pathset-bench-");
const { backend, workspace } = fixture;

try {
	await prepareWorkspace(workspace);
	const { status, executionFingerprint } = await prepareLinuxProcessReuse(fixture);
	const histories: MeasuredRun[] = [];
	for (let index = 0; index < HISTORY; index++) {
		await writeInput(workspace, index);
		await rm(path.join(workspace, "artifact.txt"), { force: true });
		const run = await runTask(`history-${index}`, "pi-pathset-helper input.bin artifact.txt", executionFingerprint);
		assert(run.metricDelta.misses === 1 && run.metricDelta.hits === 0, `history ${index} was not a miss`);
		histories.push(run);
	}

	await writeInput(workspace, 0);
	await rm(path.join(workspace, "artifact.txt"), { force: true });
	const hit = await runTask(
		"oldest-state-hit",
		"printf 'different-parent\\n'; command pi-pathset-helper input.bin artifact.txt",
		executionFingerprint,
	);
	assert(hit.metricDelta.hits === 1 && hit.metricDelta.misses === 0, "oldest historical state did not hit");
	if ("validationCandidates" in hit.metricDelta) {
		assert(hit.metricDelta.validationCandidates === HISTORY, "not all historical certificates were considered");
		assert(hit.metricDelta.validationPathsets === 1, "one dependency pathset was captured more than once");
		assert(
			hit.metricDelta.validationBytesRead < INPUT_BYTES * 2,
			"pathset validation read the large input more than once",
		);
	}

	const coldMedian = median(histories.map((run) => run.totalMs));
	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
		subject: "stock Pi createBashTool with eight historical strong keys sharing one dynamic pathset",
		fixture: { histories: HISTORY, inputBytes: INPUT_BYTES, helperDelayMs: 500 },
		assertions: {
			allHistoricalStatesPublished: true,
			oldestStateReusedAcrossDifferentParent: true,
			artifactAndOutputEqual: hit.output.trimEnd().endsWith(hit.artifact),
			...("validationPathsets" in hit.metricDelta ? { onePathsetCapture: true } : {}),
		},
		histories,
		hit,
		summary: {
			coldMedianMs: coldMedian,
			hitMs: hit.totalMs,
			coldMedianToHitSpeedup: coldMedian / hit.totalMs,
			latencySavedMs: coldMedian - hit.totalMs,
			metrics: numericMetrics(backend.metrics()),
		},
	};
	await writeBenchmarkReport(result, outputPath);

	async function runTask(label: string, command: string, executionFingerprint: string): Promise<MeasuredRun> {
		const execution = await executeReusableBash(fixture, {
			label,
			command,
			actionNamespace: "pi-bash-pathset-benchmark.v1",
			executionFingerprint,
		});
		return {
			label,
			forkMs: execution.forkMs,
			validationMs: execution.validationMs,
			commitMs: execution.commitMs,
			totalMs: execution.totalMs,
			output: textOutput(execution.output.result),
			artifact: (await readFile(path.join(workspace, "artifact.txt"), "utf8")).trim(),
			metricDelta: subtractMetrics(
				numericMetrics(execution.metricsBefore),
				numericMetrics(execution.metricsAfter),
			),
		};
	}
} finally {
	await fixture.dispose();
}

async function prepareWorkspace(workspace: string): Promise<void> {
	await writeFile(path.join(workspace, "pi-pathset-helper.c"), HELPER_SOURCE, "utf8");
	await writeInput(workspace, 0);
	await compileBenchmarkHelper(workspace, { source: "pi-pathset-helper.c", output: "pi-pathset-helper" });
	await commitBenchmarkFixture(workspace, "Pi Bash Pathset Benchmark", [
		"pi-pathset-helper.c",
		"pi-pathset-helper",
		"input.bin",
	]);
}

function writeInput(workspace: string, version: number): Promise<void> {
	return writeFile(path.join(workspace, "input.bin"), Buffer.alloc(INPUT_BYTES, version + 1));
}
