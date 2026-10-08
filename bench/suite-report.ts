import { toolSpeedup } from "../src/task-timing.ts";

export interface SuiteBenchmarkSummary {
	readonly actualEndToEndMs: number;
	readonly toolWaitMs: number;
	/** Absent in legacy or incomplete reports; raw tool wait is not a substitute. */
	readonly actorComputeMs?: number;
	readonly reusedExecutionMs?: number;
	readonly reusedExecutionIncomplete?: true;
	readonly actorActions: number;
	readonly speculativeHits: number;
	readonly actorCost: number;
	readonly drafterCost: number;
	readonly patchCandidate: boolean;
	readonly timedOut: boolean;
	readonly turnLimitReached: boolean;
	readonly agentError?: string;
	readonly benchmarkErrors?: Readonly<Record<string, string>>;
	readonly patchClean: boolean;
	readonly changedFiles: readonly string[];
	readonly coveredGoldFiles: readonly string[];
}

export interface SuiteBenchmarkRun {
	readonly instance: string;
	readonly repeat: number;
	readonly arm?: "on" | "off";
	readonly output: string;
	readonly implementationCommit?: string;
	readonly summary?: SuiteBenchmarkSummary;
	readonly error?: string;
}

type MeasuredRun = SuiteBenchmarkRun & {
	readonly summary: SuiteBenchmarkSummary & { readonly actorComputeMs: number; readonly reusedExecutionMs: number };
};
type ToolWaitRun = SuiteBenchmarkRun & { readonly summary: SuiteBenchmarkSummary };

export function summarizeSuite(runs: readonly SuiteBenchmarkRun[]) {
	const measured = runs.filter(hasTiming);
	const toolWaits = runs.filter(hasToolWait).map(run => run.summary.toolWaitMs);
	const reused = runs.flatMap(run => measuredValue(run.summary?.reusedExecutionMs));
	const invalidRuns = runs.flatMap((run) => {
		const reasons = [...(run.error ? ["runner_error"] : []), ...screeningFailures(run.summary)];
		if (run.summary && !hasTiming(run)) reasons.push("unavailable_timing");
		return reasons.length ? [{ instance: run.instance, repeat: run.repeat, output: run.output,
			...(run.error ? { error: run.error } : {}), reasons }] : [];
	});
	return {
		runs: runs.length,
		patchCandidates: runs.length - invalidRuns.length,
		allRunsScreenedIn: runs.length > 0 && invalidRuns.length === 0,
		statistics: {
			primaryEstimator: "ratio_of_means",
			baseline: "same_run_actor_compute_plus_gross_reused_execution",
			samplePolicy: "all_measured_runs",
		},
		unmeasuredRuns: runs.length - measured.length,
		// Incomplete denominators must not hide known gross savings or actual waits.
		diagnostics: {
			toolWaitMeasuredRuns: toolWaits.length, toolWaitMs: total(toolWaits),
			toolWaitMeanMs: toolWaits.length ? total(toolWaits)! / toolWaits.length : undefined,
			toolWaitP95Ms: nearestRank(toolWaits, 0.95),
			reuseMeasuredRuns: reused.length, reusedExecutionMs: total(reused),
			reusedExecutionIncomplete: runs.some(run => run.summary?.reusedExecutionIncomplete) || undefined,
		},
		implementationCommits: [...new Set(runs.flatMap((run) => run.implementationCommit ? [run.implementationCommit] : []))],
		invalidRuns,
		pooled: measured.length ? pooled(measured) : undefined,
		byInstance: Object.fromEntries(
			[...new Set(runs.map((run) => run.instance))].map((instance) => {
				const values = measured.filter((run) => run.instance === instance);
				return [instance, values.length ? pooled(values) : null];
			}),
		),
	};
}

/** Paired tool wait comparison; independent runs may be slower with speculation. */
export function summarizePairs(runs: readonly SuiteBenchmarkRun[]) {
	const on = runs.filter((run) => run.arm === "on"), off = runs.filter((run) => run.arm === "off");
	const pairs = on.filter(hasToolWait).flatMap((run) => {
		const other = off.find((candidate) => candidate.instance === run.instance && candidate.repeat === run.repeat);
		return other && hasToolWait(other) ? [{ instance: run.instance, repeat: run.repeat, onToolWaitMs: run.summary.toolWaitMs, offToolWaitMs: other.summary.toolWaitMs }] : [];
	});
	const onMs = pairs.reduce((total, pair) => total + pair.onToolWaitMs, 0), offMs = pairs.reduce((total, pair) => total + pair.offToolWaitMs, 0);
	return {
		pairs,
		pairedToolWaitRatio: onMs > 0 ? offMs / onMs : null,
		on: summarizeSuite(on),
		off: summarizeSuite(off),
	};
}

export function safeName(value: string): string {
	return value.replaceAll(/[^A-Za-z0-9._-]/g, "_");
}

function hasTiming(run: SuiteBenchmarkRun): run is MeasuredRun {
	return !!run.summary && !run.summary.reusedExecutionIncomplete && measured(run.summary.actorComputeMs) && measured(run.summary.reusedExecutionMs);
}

function hasToolWait(run: SuiteBenchmarkRun): run is ToolWaitRun {
	return !!run.summary && measured(run.summary.toolWaitMs);
}

function measured(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function measuredValue(value: unknown): number[] { return measured(value) ? [value] : []; }
function total(values: readonly number[]): number | undefined { return values.length ? values.reduce((sum, value) => sum + value, 0) : undefined; }

export function nearestRank(values: readonly number[], percentile: number): number | undefined {
	if (!values.length) return undefined;
	if (!(percentile > 0 && percentile <= 1)) throw new Error("percentile must be in (0, 1]");
	const ordered = [...values].sort((left, right) => left - right);
	return ordered[Math.max(1, Math.ceil(percentile * ordered.length)) - 1];
}

function pooled(runs: readonly MeasuredRun[]) {
	const actualEndToEndMs = sum(runs, "actualEndToEndMs");
	const actorComputeMs = sum(runs, "actorComputeMs"), reusedExecutionMs = sum(runs, "reusedExecutionMs");
	const toolWaitRuns = runs.filter(hasToolWait), toolWaitMs = toolWaitRuns.length ? sum(toolWaitRuns, "toolWaitMs") : undefined;
	const actorActions = sum(runs, "actorActions");
	const speculativeHits = sum(runs, "speculativeHits");
	return {
		runs: runs.length,
		instanceClusters: new Set(runs.map(run => run.instance)).size,
		actualEndToEndMs,
		actualEndToEndMeanMs: actualEndToEndMs / runs.length,
		actualEndToEndP95Ms: nearestRank(runs.map((run) => run.summary.actualEndToEndMs), 0.95),
		toolWaitMs,
		toolWaitMeasuredRuns: toolWaitRuns.length,
		toolWaitMeanMs: toolWaitMs === undefined ? undefined : toolWaitMs / toolWaitRuns.length,
		toolWaitP95Ms: nearestRank(toolWaitRuns.map((run) => run.summary.toolWaitMs), 0.95),
		actorComputeMs,
		reusedExecutionMs,
		baselineComputeMs: actorComputeMs + reusedExecutionMs,
		toolSpeedup: toolSpeedup({ actorComputeMs, reusedExecutionMs }),
		fullyReused: actorComputeMs === 0 && reusedExecutionMs > 0,
		actorActions,
		speculativeHits,
		hitRate: actorActions > 0 ? speculativeHits / actorActions : 0,
		actorCost: sum(runs, "actorCost"),
		drafterCost: sum(runs, "drafterCost"),
	};
}

function sum(runs: readonly MeasuredRun[], key: NumericSummaryKey): number {
	return runs.reduce((total, run) => total + run.summary[key], 0);
}

type NumericSummaryKey = {
	[Key in keyof MeasuredRun["summary"]]-?: MeasuredRun["summary"][Key] extends number ? Key : never;
}[keyof MeasuredRun["summary"]];

function screeningFailures(summary: SuiteBenchmarkSummary | undefined): string[] {
	if (!summary) return ["unavailable_summary"];
	const reasons = [
		summary.timedOut ? "timed_out" : undefined,
		summary.turnLimitReached ? "turn_limit_reached" : undefined,
		summary.agentError ? "agent_error" : undefined,
		Object.keys(summary.benchmarkErrors ?? {}).length ? "benchmark_error" : undefined,
		!summary.patchClean ? "patch_not_clean" : undefined,
		!summary.changedFiles.length ? "no_changed_files" : undefined,
		!summary.coveredGoldFiles.length ? "no_gold_file_overlap" : undefined,
	].filter((reason): reason is string => reason !== undefined);
	return reasons.length || summary.patchCandidate ? reasons : ["patch_candidate_false"];
}
