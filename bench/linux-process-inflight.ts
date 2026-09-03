import { writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	createLinuxProcessBenchmark,
	executeDirectBash,
	forkReusableBash,
	linuxBenchmarkHost,
	metricDelta,
	prepareLinuxProcessReuse,
	textOutput,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const outputPath = argument("--output");
const fixture = await createLinuxProcessBenchmark("pi-process-inflight-");

try {
	await writeFile(`${fixture.workspace}/fixture.txt`, "fixture\n");
	await commitBenchmarkFixture(fixture.workspace, "Pi In-Flight Process Benchmark");
	const { status, executionFingerprint } = await prepareLinuxProcessReuse(fixture);
	const command = "printf 'pid:%s\\n' \"$$\"; sleep 2; printf 'complete\\n'";
	const direct = await executeDirectBash(fixture, { label: "direct", command });
	const beforeProducer = fixture.backend.metrics();
	const producerStarted = performance.now();
	const producer = forkReusableBash(fixture, {
		label: "producer",
		command,
		actionNamespace: "pi-process-inflight.v1",
		executionFingerprint,
		executionScope: { sessionID: "benchmark", turnID: "speculative" },
	}).then((branch) => ({ branch, durationMs: performance.now() - producerStarted }));
	await waitUntil(() => fixture.backend.metrics().wholeCommandMisses > beforeProducer.wholeCommandMisses);
	const speculationLeadMs = 1_500;
	await delay(speculationLeadMs);
	const beforeActor = fixture.backend.metrics();
	const [production, ...actors] = await Promise.all([
		producer,
		executeDirectBash(fixture, { label: "actor-a", command }),
		executeDirectBash(fixture, { label: "actor-b", command }),
	]);
	const { branch } = production;
	try {
		const producerOutput = textOutput(branch.output.result);
		const adopted = actors.filter((actor) => textOutput(actor.output) === producerOutput);
		const fallback = actors.filter((actor) => textOutput(actor.output) !== producerOutput);
		const actorMetrics = metricDelta(beforeActor, fixture.backend.metrics());
		const validation = await branch.validate?.();
		assert(!branch.output.isError, `speculative producer failed: ${producerOutput}`);
		assert(validation?.status === "valid", `speculative result did not validate: ${JSON.stringify(validation)}`);
		assert(adopted.length === 1 && fallback.length === 1, "running result was not claimed exactly once");
		assert(actorMetrics.wholeCommandHits === 1 && actorMetrics.wholeCommandMisses === 1, "Actor join/fallback accounting differs");
		assert(fixture.backend.metrics().wholeCommandPublished === 0, "one-shot PID result leaked into persistent history");
		assert(adopted[0]!.totalMs < direct.totalMs, "running-result transfer did not shorten Actor latency");
		await writeBenchmarkReport({
			schemaVersion: 1,
			measuredAt: new Date().toISOString(),
			host: await linuxBenchmarkHost(status),
			subject: "one-shot transfer of a still-running speculative Bash result",
			assertions: {
				validatedBeforeTransfer: true,
				singleConsumerClaim: true,
				actorDidNotReexecute: true,
				outputEqual: true,
				taintedResultNotPersisted: true,
			},
			directMs: direct.totalMs,
			producerMs: production.durationMs,
			adoptingActorMs: adopted[0]!.totalMs,
			fallbackActorMs: fallback[0]!.totalMs,
			speculationLeadMs,
			actorLatencySavedMs: direct.totalMs - adopted[0]!.totalMs,
			speedup: direct.totalMs / adopted[0]!.totalMs,
			actorWholeCommandMetrics: {
				requests: actorMetrics.wholeCommandRequests,
				hits: actorMetrics.wholeCommandHits,
				misses: actorMetrics.wholeCommandMisses,
				replayMs: actorMetrics.wholeCommandReplayMs,
				avoidedProcessMs: actorMetrics.wholeCommandAvoidedProcessMs,
				hitOverheadMs: actorMetrics.wholeCommandHitOverheadMs,
			},
		}, outputPath);
	} finally {
		await branch.dispose();
	}
} finally {
	await fixture.dispose();
}

async function waitUntil(condition: () => boolean, timeoutMs = 10_000): Promise<void> {
	const deadline = performance.now() + timeoutMs;
	while (!condition()) {
		if (performance.now() >= deadline) throw new Error("timed out waiting for speculative process start");
		await delay(10);
	}
}
