import { rm, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeReusableBash,
	fileDigest,
	linuxBenchmarkHost,
	median,
	numericMetrics,
	prepareLinuxProcessReuse,
	subtractMetrics,
	type NumericMetrics,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

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

interface MeasuredRun {
	readonly label: string;
	readonly totalMs: number;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly metricDelta: NumericMetrics;
}

const outputPath = argument("--output");
const fixture = await createLinuxProcessBenchmark("pi-artifact-closure-bench-");
const { backend, workspace } = fixture;

try {
	await prepareWorkspace(workspace);
	const { status, executionFingerprint } = await prepareLinuxProcessReuse(fixture);
	const cold = await runTask("cold", "./pi-artifact-helper input.bin artifact.bin", executionFingerprint);
	assert(cold.metricDelta.misses === 1 && cold.metricDelta.hits === 0, "cold execution was not published");
	const expectedDigest = await fileDigest(path.join(workspace, "input.bin"));
	assert((await fileDigest(path.join(workspace, "artifact.bin"))) === expectedDigest, "cold artifact differs");

	const hits: MeasuredRun[] = [];
	for (let index = 0; index < HIT_RUNS; index++) {
		await rm(path.join(workspace, "artifact.bin"), { force: true });
		const hit = await runTask(
			`hit-${index + 1}`,
			`printf 'parent-${index + 1}\\n'; command ./pi-artifact-helper input.bin artifact.bin`,
			executionFingerprint,
		);
		assert(hit.metricDelta.hits === 1 && hit.metricDelta.misses === 0, `hit ${index + 1} missed`);
		assert((await fileDigest(path.join(workspace, "artifact.bin"))) === expectedDigest, `hit ${index + 1} artifact differs`);
		hits.push(hit);
	}

	const result = {
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
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
	await writeBenchmarkReport(result, outputPath);

	async function runTask(label: string, command: string, executionFingerprint: string): Promise<MeasuredRun> {
		const execution = await executeReusableBash(fixture, {
			label,
			command,
			actionNamespace: "pi-artifact-closure-benchmark.v1",
			executionFingerprint,
		});
		return {
			label,
			totalMs: execution.totalMs,
			forkMs: execution.forkMs,
			validationMs: execution.validationMs,
			commitMs: execution.commitMs,
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
	await writeFile(path.join(workspace, "pi-artifact-helper.c"), HELPER_SOURCE, "utf8");
	await writeFile(path.join(workspace, "input.bin"), Buffer.alloc(INPUT_BYTES, 0x5a));
	await compileBenchmarkHelper(workspace, { source: "pi-artifact-helper.c", output: "pi-artifact-helper" });
	await commitBenchmarkFixture(workspace, "Pi Artifact Benchmark", [
		"pi-artifact-helper.c",
		"pi-artifact-helper",
		"input.bin",
	]);
}
