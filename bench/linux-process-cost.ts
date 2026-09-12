import { readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import {
	argument, assert, benchmarkWriteAllC, commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark,
	executeReusableBash, fileDigest, linuxBenchmarkHost, median, numericMetrics, prepareLinuxProcessReuse,
	textOutput, writeBenchmarkReport,
} from "./linux-process-harness.ts";

const scenario = argument("--case");
assert(scenario === "pathset" || scenario === "artifacts", "--case must be pathset or artifacts");
const pathset = scenario === "pathset", histories = pathset ? 8 : 1, hitRuns = pathset ? 1 : 3;
const inputBytes = (pathset ? 32 : 128) * 1024 * 1024, helperDelayMs = pathset ? 500 : 300;
const helper = pathset ? "pi-pathset-helper" : "pi-artifact-helper", artifact = pathset ? "artifact.txt" : "artifact.bin";
const source = String.raw`
#include <errno.h>
#include <fcntl.h>
${pathset ? "#include <stdint.h>\n" : ""}#include <stdio.h>
#include <time.h>
#include <unistd.h>

${benchmarkWriteAllC()}

int main(int argc, char **argv) {
  if (argc != 3) return 64;
  struct timespec delay = {0, ${helperDelayMs * 1_000_000}};
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
  int input = open(argv[1], O_RDONLY);
${pathset ? String.raw`  if (input < 0) return 65;
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
  return write_all(1, value, (size_t)length) == 0 ? 0 : 68;` : String.raw`  int output = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (input < 0 || output < 0) return 65;
  char buffer[65536];
  for (;;) {
    ssize_t length = read(input, buffer, sizeof(buffer));
    if (length == 0) break;
    if (length < 0) { if (errno == EINTR) continue; return 66; }
    if (write_all(output, buffer, (size_t)length) != 0) return 67;
  }
  close(input); close(output);
  return write_all(1, "copied\n", 7) == 0 ? 0 : 68;`}
}
`;

const outputPath = argument("--output");
const fixture = await createLinuxProcessBenchmark(pathset ? "pi-bash-pathset-bench-" : "pi-artifact-closure-bench-");
const { backend, workspace } = fixture;
const command = `./${helper} input.bin ${artifact}`, artifactPath = path.join(workspace, artifact);
const writeInput = (version: number) => writeFile(path.join(workspace, "input.bin"), Buffer.alloc(inputBytes, pathset ? version + 1 : 0x5a));

try {
	await writeFile(path.join(workspace, `${helper}.c`), source, "utf8");
	await writeInput(0);
	await compileBenchmarkHelper(workspace, { source: `${helper}.c`, output: helper });
	await commitBenchmarkFixture(workspace, pathset ? "Pi Bash Pathset Benchmark" : "Pi Artifact Benchmark", [`${helper}.c`, helper, "input.bin"]);
	const { status, executionFingerprint } = await prepareLinuxProcessReuse(fixture);
	const cold: Awaited<ReturnType<typeof runTask>>[] = [], hits: typeof cold = [];
	for (let index = 0; index < histories; index++) {
		if (pathset) await writeInput(index);
		await rm(artifactPath, { force: true });
		const run = await runTask(pathset ? `history-${index}` : "cold", command);
		assert(run.metricDelta.misses === 1 && run.metricDelta.hits === 0 && run.metricDelta.published === 1,
			`${run.label} was not executed and published once`);
		cold.push(run);
	}
	const expected = pathset ? cold[0]!.artifact : await fileDigest(path.join(workspace, "input.bin"));
	assert(cold[0]!.artifact === expected, "cold artifact differs");
	if (pathset) await writeInput(0);
	for (let index = 0; index < hitRuns; index++) {
		await rm(artifactPath, { force: true });
		const parent = pathset ? "different-parent" : `parent-${index + 1}`;
		const hit = await runTask(pathset ? "oldest-state-hit" : `hit-${index + 1}`, `printf '${parent}\\n'; command ${command}`, `${parent}\n`);
		assert(hit.metricDelta.hits === 1 && hit.metricDelta.misses === 0, `${hit.label} missed`);
		assert(hit.artifact === expected, `${hit.label} artifact differs`);
		assert(hit.metricDelta.validationArtifactsLoaded > 0, `${hit.label} did not load its verified artifact closure`);
		if (pathset) {
			assert(hit.metricDelta.validationCandidates === histories, "not all historical certificates were considered");
			assert(hit.metricDelta.validationPathsets === 1, `expected one pathset capture; observed ${hit.metricDelta.validationPathsets}`);
			assert(hit.metricDelta.validationBytesRead < inputBytes * 2, "pathset validation read the large input more than once");
		}
		hits.push(hit);
	}
	const coldMedian = median(cold.map(run => run.totalMs)), hitMedian = median(hits.map(run => run.totalMs));
	await writeBenchmarkReport({
		schemaVersion: 1, measuredAt: new Date().toISOString(), host: await linuxBenchmarkHost(status),
		subject: pathset ? "stock Pi createBashTool with eight historical strong keys sharing one dynamic pathset"
			: "stock Pi createBashTool replaying one verified 128 MiB regular-file effect",
		fixture: { inputBytes, helperDelayMs, ...(pathset ? { histories } : { hitRuns }) },
		assertions: pathset ? { allHistoricalStatesPublished: true, oldestStateReusedAcrossDifferentParent: true,
			artifactAndOutputEqual: true, onePathsetCapture: true }
			: { coldPublished: true, allDifferentParentHits: true, allArtifactsEqual: true, verifiedClosureLoaded: true },
		...(pathset ? { histories: cold, hit: hits[0] } : { cold: cold[0], hits }),
		summary: { ...(pathset ? { coldMedianMs: coldMedian, hitMs: hitMedian, coldMedianToHitSpeedup: coldMedian / hitMedian,
			latencySavedMs: coldMedian - hitMedian } : { hitMedianMs: hitMedian, coldToHitMedianSpeedup: coldMedian / hitMedian,
			validationMedianMs: median(hits.map(run => run.metricDelta.validationMs)) }), metrics: numericMetrics(backend.metrics()) },
	}, outputPath);

	async function runTask(label: string, command: string, prefix = "") {
		const execution = await executeReusableBash(fixture, { label, command, executionFingerprint,
			actionNamespace: pathset ? "pi-bash-pathset-benchmark.v1" : "pi-artifact-closure-benchmark.v1" });
		const output = textOutput(execution.output.result);
		const artifact = pathset ? (await readFile(artifactPath, "utf8")).trim() : await fileDigest(artifactPath);
		assert(output === `${prefix}${pathset ? artifact : "copied"}\n`, `${label} output differs`);
		return { label, ...execution.measurement, output, artifact };
	}
} finally {
	await fixture.dispose();
}
