import { readFile } from "node:fs/promises";
import path from "node:path";
import { SpeculationScheduler } from "../src/scheduler.ts";
import {
	argument,
	median,
	numberArgument,
	requiredArgument,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

interface MeasuredRun {
	readonly totalMs: number;
	readonly forkMs: number;
	readonly validationMs: number;
	readonly commitMs: number;
}

interface TopologyReport {
	readonly mode: "direct" | "reuse";
	readonly host: Readonly<Record<string, unknown>>;
	readonly fixture: {
		readonly inputBytes: number;
		readonly transformRoundsPerByte: number;
		readonly measuredRuns: number;
	};
	readonly runs: readonly MeasuredRun[];
}

const directPath = requiredArgument("--direct");
const reusePath = requiredArgument("--reuse");
const outputPath = argument("--output");
const expected = argument("--expect");
if (expected !== undefined && expected !== "join" && expected !== "fallback") {
	throw new Error("--expect must be join or fallback");
}
const expectedReady = argument("--expect-ready");
if (expectedReady !== undefined && expectedReady !== "join" && expectedReady !== "fallback") {
	throw new Error("--expect-ready must be join or fallback");
}
const elapsedMs = numberArgument("--elapsed-ms", 0);
const direct = await readReport(directPath, "direct");
const reuse = await readReport(reusePath, "reuse");
if (JSON.stringify(direct.fixture) !== JSON.stringify(reuse.fixture)) {
	throw new Error("direct and reuse reports describe different fixtures");
}
if (!direct.runs.length || !reuse.runs.length) throw new Error("reports must contain measured runs");

const scheduler = new SpeculationScheduler<object>();
const identity = {
	tool: "bash",
	executionFingerprint: "linux_process_reuse:measured-machine-class",
};
for (const run of direct.runs) scheduler.observeActorService(identity, run.totalMs);
for (const run of reuse.runs) {
	scheduler.observeSpeculativeService(identity, run.forkMs);
	scheduler.observeAdoption(identity, run.validationMs + run.commitMs);
}
const inFlight = scheduler.assessCandidateJoin({
	identity,
	state: "running",
	expectedSpeculativeDurationMs: median(reuse.runs.map((run) => run.forkMs)),
	elapsedMs,
});
const ready = scheduler.assessCandidateJoin({
	identity,
	state: "succeeded",
	expectedSpeculativeDurationMs: median(reuse.runs.map((run) => run.forkMs)),
});
const actual = inFlight.allowed ? "join" : "fallback";
if (expected !== undefined && actual !== expected) {
	throw new Error(`expected ${expected}, measured policy selected ${actual}`);
}
const actualReady = ready.allowed ? "join" : "fallback";
if (expectedReady !== undefined && actualReady !== expectedReady) {
	throw new Error(`expected ready ${expectedReady}, measured policy selected ${actualReady}`);
}

const directMedianMs = median(direct.runs.map((run) => run.totalMs));
const replayForkMedianMs = median(reuse.runs.map((run) => run.forkMs));
const adoptionMedianMs = median(reuse.runs.map((run) => run.validationMs + run.commitMs));
const result = {
	schemaVersion: 1,
	measuredAt: new Date().toISOString(),
	host: reuse.host,
	fixture: direct.fixture,
	samples: {
		direct: direct.runs.length,
		replay: reuse.runs.length,
	},
	medians: {
		directMs: directMedianMs,
		replayForkMs: replayForkMedianMs,
		adoptionMs: adoptionMedianMs,
		synchronousReplayMs: median(reuse.runs.map((run) => run.totalMs)),
		readyHitSavingMs: directMedianMs - adoptionMedianMs,
		inFlightSavingAtMeasuredEstimateMs: directMedianMs - replayForkMedianMs - adoptionMedianMs + elapsedMs,
	},
	decision: { inFlight, ready },
	assertions: {
		...(expected === undefined ? {} : { expected: true, policy: expected }),
		...(expectedReady === undefined ? {} : { expectedReady: true, readyPolicy: expectedReady }),
	},
};
await writeBenchmarkReport(result, outputPath);

async function readReport(filePath: string, mode: TopologyReport["mode"]): Promise<TopologyReport> {
	const value = JSON.parse(await readFile(path.resolve(filePath), "utf8")) as Partial<TopologyReport>;
	if (value.mode !== mode || !value.fixture || !Array.isArray(value.runs)) {
		throw new Error(`${filePath} is not a ${mode} topology report`);
	}
	for (const run of value.runs) {
		for (const metric of [run.totalMs, run.forkMs, run.validationMs, run.commitMs]) {
			if (typeof metric !== "number" || !Number.isFinite(metric) || metric < 0) {
				throw new Error(`${filePath} contains an invalid timing`);
			}
		}
	}
	return value as TopologyReport;
}
