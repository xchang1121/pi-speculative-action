import { writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { createFauxCore, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	createLinuxProcessBenchmark,
	executeDirectBash,
	linuxBenchmarkHost,
	metricDelta,
	prepareLinuxProcessReuse,
	textOutput,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const outputPath = argument("--output");
const fixture = await createLinuxProcessBenchmark("pi-process-inflight-");
const command = "printf 'pid:%s\\n' \"$$\"; /usr/bin/sleep 4; printf 'complete\\n'";
const turnID = "inflight";
const faux = createFauxCore({ provider: "benchmark", models: [{ id: "benchmark", reasoning: false }] });
const model = faux.getModel();
const events: SpeculativeActionEvent<string>[] = [];
const host = createSpeculativeActionHost("benchmark", {
	cwd: fixture.workspace,
	getSettings: () => ({
		enabled: true,
		drafterEnabled: true,
		drafterMaxDepth: 0,
		candidateLimit: 1,
		maxConcurrentActions: 1,
		tools: ["bash"], patternAware: { enabled: false }, selfSpeculation: { enabled: false },
	}),
	draftModel: model,
	complete: async () => fauxAssistantMessage(fauxToolCall("bash", { command }), { stopReason: "toolUse" }),
	resolveInvocation: (tool, args) => resolvePiToolInvocation(tool, args, {
		cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath,
	}),
	preflight: () => true,
	executionWorlds: [{ ...fixture.world, dispose: undefined }],
	onEvent: (event) => {
		events.push(event);
	},
});

try {
	await writeFile(`${fixture.workspace}/fixture.txt`, "fixture\n");
	await commitBenchmarkFixture(fixture.workspace, "Pi In-Flight Process Benchmark");
	const { status } = await prepareLinuxProcessReuse(fixture);
	const direct = await executeDirectBash(fixture, { label: "direct", command });
	const beforeProducer = fixture.backend.metrics();
	await host.startTurn({
		turnID, actorModel: model,
		context: { systemPrompt: "benchmark", messages: [], tools: [fixture.tool] },
		actorOptions: undefined, tools: [fixture.tool],
	});
	await waitUntil(() => fixture.backend.metrics().wholeCommandMisses > beforeProducer.wholeCommandMisses);
	const speculationLeadMs = 3_000;
	await delay(speculationLeadMs);
	assert(!events.some((event) => event.type === "candidate" && event.state.status === "succeeded"),
		"candidate completed before the Actor arrived");
	const beforeActor = fixture.backend.metrics();
	const adopted = await actor("actor-a");
	const fallback = await actor("actor-b");
	await waitUntil(() => events.filter((event) => event.type === "actor_action").length === 2);
	const actorEvents = events.filter((event) => event.type === "actor_action");
	const adoptedEvents = actorEvents.filter((event) => event.settlement.provider.kind === "speculative");
	const fallbackEvents = actorEvents.filter((event) => event.settlement.provider.kind === "actor");
	const producerMs = events.flatMap((event) =>
		event.type === "candidate" && event.state.status === "succeeded" ? [event.state.executionMs] : [])[0];
	const actorMetrics = metricDelta(beforeActor, fixture.backend.metrics());
	assert(adoptedEvents.length === 1 && fallbackEvents.length === 1, "candidate was not adopted exactly once");
	assert(producerMs !== undefined, "candidate did not complete");
	assert(textOutput(adopted.output) !== textOutput(fallback.output), "second Actor unexpectedly reused the PID-tainted result");
	assert(actorMetrics.wholeCommandHits === 0 && actorMetrics.wholeCommandMisses === 1,
		"top-level Actor routing bypassed the Runtime scheduler");
	assert(fixture.backend.metrics().wholeCommandPublished === 0, "one-shot PID result leaked into persistent history");
	assert(adopted.totalMs < direct.totalMs,
		`running-result adoption did not shorten Actor latency: ${JSON.stringify({ directMs: direct.totalMs, adoptedMs: adopted.totalMs })}`);
	await writeBenchmarkReport({
		schemaVersion: 2,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(status),
		subject: "Runtime-owned adoption of a still-running speculative Bash result",
		assertions: {
			validatedBeforeCommit: true, singleConsumerClaim: true, adoptingActorDidNotReexecute: true,
			fallbackExecutedOnce: true, taintedResultNotPersisted: true, runtimeOwnsTopLevelScheduling: true,
		},
		directMs: direct.totalMs,
		producerMs,
		adoptingActorMs: adopted.totalMs,
		fallbackActorMs: fallback.totalMs,
		speculationLeadMs,
		actorLatencySavedMs: direct.totalMs - adopted.totalMs,
		speedup: direct.totalMs / adopted.totalMs,
		actorWholeCommandMetrics: { requests: actorMetrics.wholeCommandRequests,
			hits: actorMetrics.wholeCommandHits, misses: actorMetrics.wholeCommandMisses },
	}, outputPath);
	await host.finishTurn(turnID, true);

	async function actor(id: string) {
		const started = performance.now();
		const output = await host.execute(
			{ turnID, id, tool: "bash", args: { command }, tools: [fixture.tool] },
			undefined,
			(operation) => fixture.tool.execute(operation.callID ?? id, operation.input as never, operation.signal),
		);
		return { totalMs: performance.now() - started, output };
	}
} finally {
	await host.dispose();
	await fixture.dispose();
}

async function waitUntil(condition: () => boolean, timeoutMs = 10_000): Promise<void> {
	const deadline = performance.now() + timeoutMs;
	while (!condition()) {
		if (performance.now() >= deadline) throw new Error("timed out waiting for speculative process state");
		await delay(10);
	}
}
