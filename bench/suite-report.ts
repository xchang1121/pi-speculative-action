export interface SuiteBenchmarkSummary {
	readonly actualEndToEndMs: number;
	readonly serializedCounterfactualMs: number;
	readonly hiddenLatencyMs: number;
	readonly executionAheadMs: number;
	readonly actorActions: number;
	readonly speculativeHits: number;
	readonly actorCost: number;
	readonly drafterCost: number;
	readonly patchCandidate: boolean;
	readonly timedOut: boolean;
	readonly turnLimitReached: boolean;
	readonly agentError?: string;
	readonly patchClean: boolean;
	readonly changedFiles: readonly string[];
	readonly coveredGoldFiles: readonly string[];
}

export interface SuiteBenchmarkRun {
	readonly instance: string;
	readonly repeat: number;
	readonly output: string;
	readonly implementationCommit: string;
	readonly summary: SuiteBenchmarkSummary;
}

export interface SuiteStatisticsOptions {
	readonly bootstrapSamples?: number;
	readonly seed?: number;
}

export interface PairedLatencyObservation {
	readonly cluster: string;
	readonly baselineMs: number;
	readonly treatmentMs: number;
}

export interface PairedLatencyStatistics {
	readonly pairs: number;
	readonly clusters: number;
	readonly ratioOfMeans: number;
	readonly ratioOfMeansCI95?: readonly [number, number];
	readonly baselineMeanMs: number;
	readonly treatmentMeanMs: number;
	readonly meanDifferenceMs: number;
	readonly meanDifferenceCI95?: readonly [number, number];
}

export function summarizeSuite(
	runs: readonly SuiteBenchmarkRun[],
	options: SuiteStatisticsOptions = {},
) {
	const statistics = normalizeStatisticsOptions(options);
	const accepted = runs.filter((run) => run.summary.patchCandidate);
	return {
		runs: runs.length,
		patchCandidates: accepted.length,
		allRunsScreenedIn: runs.length > 0 && accepted.length === runs.length,
		statistics: {
			primaryEstimator: "ratio_of_means",
			cluster: "instance",
			bootstrapSamples: statistics.bootstrapSamples,
			seed: statistics.seed,
		},
		implementationCommits: [...new Set(runs.map((run) => run.implementationCommit))],
		invalidRuns: runs
			.filter((run) => !run.summary.patchCandidate)
			.map((run) => ({
				instance: run.instance,
				repeat: run.repeat,
				output: run.output,
				reasons: screeningFailures(run.summary),
			})),
		pooled: accepted.length ? pooled(accepted, statistics) : undefined,
		byInstance: Object.fromEntries(
			[...new Set(runs.map((run) => run.instance))].map((instance) => {
				const values = accepted.filter((run) => run.instance === instance);
				return [instance, values.length ? pooled(values, statistics) : null];
			}),
		),
	};
}

export function nearestRank(values: readonly number[], percentile: number): number | undefined {
	if (!values.length) return undefined;
	if (!(percentile > 0 && percentile <= 1)) throw new Error("percentile must be in (0, 1]");
	const ordered = [...values].sort((left, right) => left - right);
	return ordered[Math.max(1, Math.ceil(percentile * ordered.length)) - 1];
}

export function pairedLatencyStatistics(
	observations: readonly PairedLatencyObservation[],
	options: SuiteStatisticsOptions = {},
): PairedLatencyStatistics {
	if (!observations.length) throw new Error("paired latency statistics require at least one observation");
	for (const observation of observations) {
		if (
			!Number.isFinite(observation.baselineMs) ||
			observation.baselineMs < 0 ||
			!Number.isFinite(observation.treatmentMs) ||
			observation.treatmentMs <= 0
		) {
			throw new Error("paired latency observations require non-negative baselines and positive treatments");
		}
	}
	const statistics = normalizeStatisticsOptions(options);
	const clusters = new Map<string, PairedLatencyObservation[]>();
	for (const observation of observations) {
		const values = clusters.get(observation.cluster) ?? [];
		values.push(observation);
		clusters.set(observation.cluster, values);
	}
	const clusterValues = [...clusters.values()];
	const ratioOfMeans = latencyRatio(observations);
	const meanDifferenceMs = mean(observations.map((observation) => observation.treatmentMs - observation.baselineMs));
	const ratios: number[] = [];
	const differences: number[] = [];
	const random = seededRandom(statistics.seed);
	for (let sample = 0; sample < statistics.bootstrapSamples; sample++) {
		const selected: PairedLatencyObservation[] = [];
		for (let index = 0; index < clusterValues.length; index++) {
			selected.push(...clusterValues[Math.floor(random() * clusterValues.length)]!);
		}
		ratios.push(latencyRatio(selected));
		differences.push(mean(selected.map((observation) => observation.treatmentMs - observation.baselineMs)));
	}
	return {
		pairs: observations.length,
		clusters: clusters.size,
		ratioOfMeans,
		...(ratios.length ? { ratioOfMeansCI95: [quantile(ratios, 0.025), quantile(ratios, 0.975)] as const } : {}),
		baselineMeanMs: mean(observations.map((observation) => observation.baselineMs)),
		treatmentMeanMs: mean(observations.map((observation) => observation.treatmentMs)),
		meanDifferenceMs,
		...(differences.length
			? { meanDifferenceCI95: [quantile(differences, 0.025), quantile(differences, 0.975)] as const }
			: {}),
	};
}

function pooled(runs: readonly SuiteBenchmarkRun[], options: Required<SuiteStatisticsOptions>) {
	const actualEndToEndMs = sum(runs, "actualEndToEndMs");
	const serializedCounterfactualMs = sum(runs, "serializedCounterfactualMs");
	const actorActions = sum(runs, "actorActions");
	const speculativeHits = sum(runs, "speculativeHits");
	const latency = pairedLatencyStatistics(
		runs.map((run) => ({
			cluster: run.instance,
			baselineMs: run.summary.serializedCounterfactualMs,
			treatmentMs: run.summary.actualEndToEndMs,
		})),
		options,
	);
	return {
		runs: runs.length,
		instanceClusters: latency.clusters,
		actualEndToEndMs,
		serializedCounterfactualMs,
		accelerationRatio: latency.ratioOfMeans,
		accelerationRatioCI95: latency.ratioOfMeansCI95,
		meanLatencyDifferenceMs: latency.meanDifferenceMs,
		meanLatencyDifferenceCI95: latency.meanDifferenceCI95,
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

function normalizeStatisticsOptions(options: SuiteStatisticsOptions): Required<SuiteStatisticsOptions> {
	const bootstrapSamples = options.bootstrapSamples ?? 10_000;
	const seed = options.seed ?? 42;
	if (!Number.isSafeInteger(bootstrapSamples) || bootstrapSamples < 0)
		throw new Error("bootstrapSamples must be a non-negative integer");
	if (!Number.isSafeInteger(seed)) throw new Error("seed must be an integer");
	return { bootstrapSamples, seed };
}

function latencyRatio(observations: readonly PairedLatencyObservation[]): number {
	const treatment = mean(observations.map((observation) => observation.treatmentMs));
	return mean(observations.map((observation) => observation.baselineMs)) / treatment;
}

function mean(values: readonly number[]): number {
	return values.reduce((total, value) => total + value, 0) / values.length;
}

function quantile(values: readonly number[], fraction: number): number {
	const ordered = [...values].sort((left, right) => left - right);
	if (ordered.length === 1) return ordered[0]!;
	const position = fraction * (ordered.length - 1);
	const lower = Math.floor(position);
	const upper = Math.ceil(position);
	if (lower === upper) return ordered[lower]!;
	const weight = position - lower;
	return ordered[lower]! * (1 - weight) + ordered[upper]! * weight;
}

function seededRandom(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let value = state;
		value = Math.imul(value ^ (value >>> 15), value | 1);
		value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
		return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
	};
}

function sum(runs: readonly SuiteBenchmarkRun[], key: NumericSummaryKey): number {
	return runs.reduce((total, run) => total + run.summary[key], 0);
}

type NumericSummaryKey = {
	[Key in keyof SuiteBenchmarkSummary]-?: SuiteBenchmarkSummary[Key] extends number ? Key : never;
}[keyof SuiteBenchmarkSummary];

function screeningFailures(summary: SuiteBenchmarkSummary): string[] {
	const reasons = [
		summary.timedOut ? "timed_out" : undefined,
		summary.turnLimitReached ? "turn_limit_reached" : undefined,
		summary.agentError ? "agent_error" : undefined,
		!summary.patchClean ? "patch_not_clean" : undefined,
		!summary.changedFiles.length ? "no_changed_files" : undefined,
		!summary.coveredGoldFiles.length ? "no_gold_file_overlap" : undefined,
	].filter((reason): reason is string => reason !== undefined);
	return reasons.length ? reasons : ["patch_candidate_false"];
}
