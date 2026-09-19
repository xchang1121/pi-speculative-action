export interface SuiteBenchmarkSummary {
	readonly actualEndToEndMs: number;
	readonly serializedCounterfactualMs: number;
	readonly hiddenLatencyMs: number;
	readonly estimatedSavingsMs?: number;
	readonly executionAheadMs: number;
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
	readonly output: string;
	readonly implementationCommit?: string;
	readonly summary?: SuiteBenchmarkSummary;
	readonly error?: string;
}

type MeasuredRun = SuiteBenchmarkRun & { readonly summary: SuiteBenchmarkSummary };

export function summarizeSuite(runs: readonly SuiteBenchmarkRun[]) {
	const measured = runs.filter(hasTiming);
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
			baseline: "same_run_serialized_counterfactual",
			samplePolicy: "all_measured_runs",
		},
		unmeasuredRuns: runs.length - measured.length,
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

function hasTiming(run: SuiteBenchmarkRun): run is MeasuredRun {
	return !!run.summary && Number.isFinite(run.summary.actualEndToEndMs) && run.summary.actualEndToEndMs > 0 &&
		Number.isFinite(run.summary.serializedCounterfactualMs) && run.summary.serializedCounterfactualMs >= 0;
}

export function nearestRank(values: readonly number[], percentile: number): number | undefined {
	if (!values.length) return undefined;
	if (!(percentile > 0 && percentile <= 1)) throw new Error("percentile must be in (0, 1]");
	const ordered = [...values].sort((left, right) => left - right);
	return ordered[Math.max(1, Math.ceil(percentile * ordered.length)) - 1];
}

function pooled(runs: readonly MeasuredRun[]) {
	const actualEndToEndMs = sum(runs, "actualEndToEndMs");
	const serializedCounterfactualMs = sum(runs, "serializedCounterfactualMs");
	const actorActions = sum(runs, "actorActions");
	const speculativeHits = sum(runs, "speculativeHits");
	const estimatedSavingsMs = runs.every(run => Number.isFinite(run.summary.estimatedSavingsMs))
		? runs.reduce((total, run) => total + run.summary.estimatedSavingsMs!, 0) : undefined;
	return {
		runs: runs.length,
		instanceClusters: new Set(runs.map(run => run.instance)).size,
		actualEndToEndMs,
		serializedCounterfactualMs,
		actualEndToEndMeanMs: actualEndToEndMs / runs.length,
		serializedCounterfactualMeanMs: serializedCounterfactualMs / runs.length,
		accelerationRatio: serializedCounterfactualMs / actualEndToEndMs,
		estimatedSavingsMs,
		optimisticAccelerationRatio: estimatedSavingsMs === undefined ? undefined : 1 + estimatedSavingsMs / actualEndToEndMs,
		meanLatencyDifferenceMs: (actualEndToEndMs - serializedCounterfactualMs) / runs.length,
		actualEndToEndP95Ms: nearestRank(runs.map((run) => run.summary.actualEndToEndMs), 0.95),
		serializedCounterfactualP95Ms: nearestRank(
			runs.map((run) => run.summary.serializedCounterfactualMs),
			0.95,
		),
		hiddenLatencyMs: sum(runs, "hiddenLatencyMs"),
		executionAheadMs: sum(runs, "executionAheadMs"),
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
	[Key in keyof SuiteBenchmarkSummary]-?: SuiteBenchmarkSummary[Key] extends number ? Key : never;
}[keyof SuiteBenchmarkSummary];

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
