import { bashTailLinesView, buildPiActionKey } from "../src/action-semantics.ts";

export interface BashCorpusCall {
	readonly task: string;
	readonly sequence: number;
	readonly command: string;
	readonly durationMs: number;
	readonly category?: string;
	readonly workspaceGeneration?: number;
	readonly previousTool?: string;
}

export interface BashCorpusCoverage {
	readonly coreRepeatOpportunities: number;
	readonly coreRepeatActorMs: number;
	readonly directlyCovered: number;
	readonly directlyCoveredActorMs: number;
	readonly bucketCovered: number;
	readonly bucketCoveredActorMs: number;
	readonly incrementalBucketCovered: number;
	readonly incrementalBucketCoveredActorMs: number;
}

export interface BashCorpusAnalysis {
	readonly tasks: number;
	readonly calls: number;
	readonly durationMs: number;
	readonly categories: Readonly<Record<string, { readonly calls: number; readonly durationMs: number }>>;
	/** Historical repetition is an upper bound only; workspace mutations may invalidate prior results. */
	readonly exactRepeatUpperBound: {
		readonly opportunities: number;
		readonly actorMs: number;
		readonly afterExplicitMutation: number;
		readonly afterExplicitMutationActorMs: number;
		readonly sameGeneration: number;
		readonly sameGenerationActorMs: number;
		readonly immediatelyAfterExplicitMutation: number;
		readonly immediatelyAfterExplicitMutationActorMs: number;
	};
	readonly tailViews: {
		readonly syntacticTerminalTailCalls: number;
		readonly provableCalls: number;
		readonly provableDurationMs: number;
		readonly rejectedLookalikeCalls: number;
		readonly widths: Readonly<Record<string, { readonly calls: number; readonly durationMs: number }>>;
		readonly variantCoreGroups: number;
		readonly variantCalls: number;
		readonly coveringBuckets: readonly number[];
		readonly coverage: BashCorpusCoverage;
	};
}

interface ProvableTailCall extends BashCorpusCall {
	readonly core: string;
	readonly lines: number;
}

/** Extract the stable tool-call surface emitted by the SWE-bench history summarizer. */
export function bashCallsFromSWEbenchSummary(value: unknown): readonly BashCorpusCall[] {
	if (!Array.isArray(value)) return [];
	const calls: BashCorpusCall[] = [];
	for (const rawRecord of value) {
		const record = asRecord(rawRecord);
		const task = string(record?.instance_id);
		const details = record?.tool_call_details;
		if (!task || !Array.isArray(details)) continue;
		let sequence = 0;
		let workspaceGeneration = 0;
		let previousTool: string | undefined;
		for (const rawDetail of details) {
			const detail = asRecord(rawDetail);
			const tool = string(detail?.tool);
			if (tool === "bash") {
				const args = asRecord(detail?.args);
				const command = string(args?.command);
				const durationMs = finiteNonNegative(detail?.duration_ms);
				if (command !== undefined && durationMs !== undefined) {
					const category = string(detail?.bash_category);
					calls.push({
						task,
						sequence,
						command,
						durationMs,
						workspaceGeneration,
						...(previousTool ? { previousTool } : {}),
						...(category ? { category } : {}),
					});
				}
			}
			if ((tool === "edit" || tool === "write") && detail?.is_error !== true) workspaceGeneration++;
			if (tool) previousTool = tool;
			sequence++;
		}
	}
	return calls;
}

/**
 * Analyze only relations already proved by the production Bash tail projector.
 *
 * Bucket coverage is a counterfactual for widening each prior tail view to the
 * smallest configured bucket. It is not reported as a measured cache hit.
 */
export function analyzeBashCorpus(
	calls: readonly BashCorpusCall[],
	options: { readonly cwd?: string; readonly coveringBuckets?: readonly number[] } = {},
): BashCorpusAnalysis {
	const cwd = options.cwd ?? "/testbed";
	const coveringBuckets = normalizedBuckets(options.coveringBuckets ?? []);
	const categories = new Map<string, { calls: number; durationMs: number }>();
	const exactSeen = new Map<string, BashCorpusCall>();
	let exactRepeatOpportunities = 0;
	let exactRepeatActorMs = 0;
	let exactRepeatAfterMutation = 0;
	let exactRepeatAfterMutationActorMs = 0;
	let exactRepeatSameGeneration = 0;
	let exactRepeatSameGenerationActorMs = 0;
	let exactRepeatImmediatelyAfterMutation = 0;
	let exactRepeatImmediatelyAfterMutationActorMs = 0;
	let syntacticTerminalTailCalls = 0;
	let rejectedLookalikeCalls = 0;
	const provable: ProvableTailCall[] = [];

	for (const call of calls) {
		const category = call.category ?? "uncategorized";
		const categoryMetrics = categories.get(category) ?? { calls: 0, durationMs: 0 };
		categoryMetrics.calls++;
		categoryMetrics.durationMs += call.durationMs;
		categories.set(category, categoryMetrics);

		const exactIdentity = JSON.stringify([call.task, call.command]);
		const previous = exactSeen.get(exactIdentity);
		if (previous) {
			exactRepeatOpportunities++;
			exactRepeatActorMs += call.durationMs;
			if (
				call.workspaceGeneration !== undefined &&
				previous.workspaceGeneration !== undefined &&
				call.workspaceGeneration > previous.workspaceGeneration
			) {
				exactRepeatAfterMutation++;
				exactRepeatAfterMutationActorMs += call.durationMs;
			} else if (
				call.workspaceGeneration !== undefined &&
				call.workspaceGeneration === previous.workspaceGeneration
			) {
				exactRepeatSameGeneration++;
				exactRepeatSameGenerationActorMs += call.durationMs;
			}
			if (call.previousTool === "edit" || call.previousTool === "write") {
				exactRepeatImmediatelyAfterMutation++;
				exactRepeatImmediatelyAfterMutationActorMs += call.durationMs;
			}
		}
		exactSeen.set(exactIdentity, call);

		const looksLikeTail = terminalTailLines(call.command) !== undefined;
		if (looksLikeTail) syntacticTerminalTailCalls++;
		const action = buildPiActionKey("bash", { command: call.command }, cwd);
		const view = action ? bashTailLinesView(action) : undefined;
		if (!view) {
			if (looksLikeTail) rejectedLookalikeCalls++;
			continue;
		}
		provable.push({ ...call, core: view.core, lines: view.lines });
	}

	const widths = aggregate(
		provable,
		(call) => String(call.lines),
		(call) => call.durationMs,
	);
	const groups = groupBy(provable, (call) => JSON.stringify([call.task, call.core]));
	let variantCoreGroups = 0;
	let variantCalls = 0;
	let coreRepeatOpportunities = 0;
	let coreRepeatActorMs = 0;
	let directlyCovered = 0;
	let directlyCoveredActorMs = 0;
	let bucketCovered = 0;
	let bucketCoveredActorMs = 0;
	let incrementalBucketCovered = 0;
	let incrementalBucketCoveredActorMs = 0;

	for (const group of groups.values()) {
		const ordered = [...group].sort((left, right) => left.sequence - right.sequence);
		if (new Set(ordered.map((call) => call.lines)).size > 1) {
			variantCoreGroups++;
			variantCalls += ordered.length;
		}
		const prior: ProvableTailCall[] = [];
		for (const call of ordered) {
			if (prior.length) {
				coreRepeatOpportunities++;
				coreRepeatActorMs += call.durationMs;
				const direct = prior.some((candidate) => candidate.lines >= call.lines);
				const bucket = prior.some(
					(candidate) => coveringLines(candidate.lines, coveringBuckets) >= call.lines,
				);
				if (direct) {
					directlyCovered++;
					directlyCoveredActorMs += call.durationMs;
				}
				if (bucket) {
					bucketCovered++;
					bucketCoveredActorMs += call.durationMs;
					if (!direct) {
						incrementalBucketCovered++;
						incrementalBucketCoveredActorMs += call.durationMs;
					}
				}
			}
			prior.push(call);
		}
	}

	return {
		tasks: new Set(calls.map((call) => call.task)).size,
		calls: calls.length,
		durationMs: sum(calls, (call) => call.durationMs),
		categories: orderedRecord(categories),
		exactRepeatUpperBound: {
			opportunities: exactRepeatOpportunities,
			actorMs: exactRepeatActorMs,
			afterExplicitMutation: exactRepeatAfterMutation,
			afterExplicitMutationActorMs: exactRepeatAfterMutationActorMs,
			sameGeneration: exactRepeatSameGeneration,
			sameGenerationActorMs: exactRepeatSameGenerationActorMs,
			immediatelyAfterExplicitMutation: exactRepeatImmediatelyAfterMutation,
			immediatelyAfterExplicitMutationActorMs: exactRepeatImmediatelyAfterMutationActorMs,
		},
		tailViews: {
			syntacticTerminalTailCalls,
			provableCalls: provable.length,
			provableDurationMs: sum(provable, (call) => call.durationMs),
			rejectedLookalikeCalls,
			widths: orderedRecord(widths, (left, right) => Number(left) - Number(right)),
			variantCoreGroups,
			variantCalls,
			coveringBuckets,
			coverage: {
				coreRepeatOpportunities,
				coreRepeatActorMs,
				directlyCovered,
				directlyCoveredActorMs,
				bucketCovered,
				bucketCoveredActorMs,
				incrementalBucketCovered,
				incrementalBucketCoveredActorMs,
			},
		},
	};
}

function terminalTailLines(command: string): number | undefined {
	const match = /\|\s*tail\s+(?:-(\d+)|-n\s+(\d+))\s*$/su.exec(command);
	const lines = Number(match?.[1] ?? match?.[2]);
	return match && Number.isSafeInteger(lines) && lines > 0 ? lines : undefined;
}

function normalizedBuckets(values: readonly number[]): readonly number[] {
	return [...new Set(values.filter((value) => Number.isSafeInteger(value) && value > 0))].sort(
		(left, right) => left - right,
	);
}

function coveringLines(lines: number, buckets: readonly number[]): number {
	return buckets.find((bucket) => bucket >= lines) ?? lines;
}

function aggregate<Value>(
	values: readonly Value[],
	key: (value: Value) => string,
	duration: (value: Value) => number,
): Map<string, { calls: number; durationMs: number }> {
	const result = new Map<string, { calls: number; durationMs: number }>();
	for (const value of values) {
		const selected = key(value);
		const metrics = result.get(selected) ?? { calls: 0, durationMs: 0 };
		metrics.calls++;
		metrics.durationMs += duration(value);
		result.set(selected, metrics);
	}
	return result;
}

function orderedRecord<Value>(
	values: ReadonlyMap<string, Value>,
	compare: (left: string, right: string) => number = (left, right) => left.localeCompare(right),
): Readonly<Record<string, Value>> {
	return Object.fromEntries([...values].sort(([left], [right]) => compare(left, right)));
}

function groupBy<Value>(values: readonly Value[], key: (value: Value) => string): Map<string, Value[]> {
	const result = new Map<string, Value[]>();
	for (const value of values) {
		const selected = key(value);
		const group = result.get(selected) ?? [];
		group.push(value);
		result.set(selected, group);
	}
	return result;
}

function sum<Value>(values: readonly Value[], metric: (value: Value) => number): number {
	return values.reduce((total, value) => total + metric(value), 0);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function string(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

function finiteNonNegative(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}
