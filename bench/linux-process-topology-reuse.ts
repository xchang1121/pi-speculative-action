import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeDirectBash,
	executeReusableBash,
	fileDigest,
	integerArgument,
	linuxBenchmarkHost,
	median,
	numericMetrics,
	prepareLinuxProcessReuse,
	subtractMetrics,
	type NumericMetrics,
	workspaceDriverArgument,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

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

interface MeasuredRun {
	readonly label: string;
	readonly totalMs: number;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
	readonly resources: readonly string[];
	readonly metricDelta: NumericMetrics;
}

const mode = argument("--mode") ?? "reuse";
if (mode !== "reuse" && mode !== "direct") throw new Error("--mode must be reuse or direct");
const transformRounds = integerArgument("--rounds", 96, 0, 4_096);
const sourceFiles = integerArgument("--source-files", 0, 0, 20_000);
const outputPath = argument("--output");
const workspaceDriver = workspaceDriverArgument(argument("--workspace-driver"));
const fixture = await createLinuxProcessBenchmark("pi-topology-reuse-bench-", workspaceDriver);
const { backend, workspace } = fixture;
const outputDirectory = path.join(workspace, "generated");
const artifact = path.join(outputDirectory, "nested", "artifact.bin");

try {
	await prepareWorkspace(workspace);
	const reusePreparation = mode === "reuse"
		? await prepareLinuxProcessReuse(fixture, { workspaceDriver, includeWorkspaceFingerprint: true })
		: undefined;

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
		...(reusePreparation?.workspaceFingerprint
			? { workspaceFingerprint: reusePreparation.workspaceFingerprint }
			: {}),
		host: await linuxBenchmarkHost(reusePreparation?.status),
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
			...(reusePreparation ? { routePreparationMs: reusePreparation.routePreparationMs } : {}),
			medianMs: median(runs.map((run) => run.totalMs)),
			...(mode === "reuse" ? { metrics: numericMetrics(backend.metrics()) } : {}),
		},
	};
	await writeBenchmarkReport(result, outputPath);

	async function runTask(label: string, command: string): Promise<MeasuredRun> {
		if (!reusePreparation) {
			const execution = await executeDirectBash(fixture, { label, command });
			return {
				label,
				totalMs: execution.totalMs,
				forkMs: 0,
				validationMs: 0,
				commitMs: 0,
				resources: [],
				metricDelta: {},
			};
		}
		const execution = await executeReusableBash(fixture, {
			label,
			command,
			actionNamespace: "pi-topology-reuse-benchmark.v1",
			executionFingerprint: reusePreparation.executionFingerprint,
		});
		return {
			label,
			totalMs: execution.totalMs,
			forkMs: execution.forkMs,
			validationMs: execution.validationMs,
			commitMs: execution.commitMs,
			resources: execution.resources,
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
	await compileBenchmarkHelper(workspace, {
		source: "pi-topology-helper.c",
		output: "pi-topology-helper",
		arguments: [`-DTRANSFORM_ROUNDS=${transformRounds}U`],
	});
	await commitBenchmarkFixture(workspace, "Pi Topology Benchmark");
}
