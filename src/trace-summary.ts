import { nonNegativeFinite as metric } from "./number-utils.ts";
import type { SpeculativeActionEvent, SpeculativeCacheSnapshot } from "./events.ts";
import { emptyWorldReuseMetrics, type WorldReuseMetrics } from "./execution-world.ts";
import type { ResolutionCause } from "./settlement.ts";

export type SpeculativeTraceSummary = Readonly<ReturnType<typeof emptySpeculativeTraceSummary>>;

export interface ModeTraceSummary {
	readonly observed: number;
	readonly matched: number;
	readonly adopted: number;
	readonly started: number;
	/** Measured speculative production wall time, including failed and cancelled work. */
	readonly productionMs: number;
	/** Gross computation actually consumed by Actor calls, attributed to the execution owner. */
	readonly reusedExecutionMs: number;
}
type ModesBySource = Readonly<Record<string, Readonly<Record<string, ModeTraceSummary>>>>;

const EMPTY_CACHE: SpeculativeCacheSnapshot = {
	cacheCapacity: 0, cacheByteCapacity: 0, cacheCold: 0, cacheHot: 0, inFlightJobs: 0, resultEntries: 0, resultBytes: 0,
	branchEntries: 0, branchBytes: 0, exclusiveCandidates: 0, sharedCandidates: 0,
};

export function emptySpeculativeTraceSummary(cache: SpeculativeCacheSnapshot | Pick<SpeculativeCacheSnapshot, "cacheCapacity" | "cacheByteCapacity"> = EMPTY_CACHE) {
	return {
		sourceRequests: 0,
		sourceOutcomes: {} as Readonly<Record<string, number>>,
		lastSourceFailure: undefined as string | undefined,
		predictionsSettled: 0, predictionsObserved: 0, predictionsMatched: 0, predictionsAdopted: 0, predictionPrecision: 0, adoptionYield: 0,
		predictionsBySource: {} as Readonly<Record<string, readonly number[]>>, // [observed, matched, adopted]
		modesBySource: {} as ModesBySource,
		predictionUnobserved: {} as Readonly<Record<string, number>>,
		predictionRejectedAfterMatch: {} as Readonly<Record<string, number>>,
		operationPredictionsSettled: 0,
		operationPredictionsAdopted: 0,
		candidateStarted: 0, candidateSucceeded: 0, candidateFailed: 0, candidateCancelled: 0,
		candidateTerminalCauses: {} as Readonly<Record<string, number>>,
		actorActions: 0,
		actorActionsByTool: {} as Readonly<Record<string, readonly number[]>>, // [actions, reused]
		speculativeHits: 0,
		exactReuseHits: 0, // Adopted identical K(a) results.
		inputReuseHits: 0, // Current tool semantics evaluated over another execution's sealed inputs.
		partialResultReuseHits: 0, // Adopted lossless views from a different K(a); its execution ran in full.
		partialResultReuseByProjector: {} as Readonly<Record<string, number>>,
		actorPreviews: 0,
		actorFallbacks: 0,
		hitRate: 0,
		actorCandidateRejections: {} as Readonly<Record<string, number>>,
		tasks: 0, actorComputeMs: 0 as number | undefined, toolWaitMs: 0, reusedExecutionMs: 0,
		reusedExecutionIncomplete: undefined as true | undefined,
		totalDraftTokens: 0,
		processReuse: emptyWorldReuseMetrics(), // Inside speculative worlds, never the Actor route.
		cache: { ...EMPTY_CACHE, ...cache },
	};
}

/** The sole reducer used by both live UI state and persisted trace replay. */
export function reduceSpeculativeTrace<SessionID>(
	current: SpeculativeTraceSummary,
	event: SpeculativeActionEvent<SessionID>,
): SpeculativeTraceSummary {
	const next = {
		...current,
		cache: { ...event.cache },
	};
	// Prediction support and execution ownership are different: shared work is charged to its producer only.
	if ((event.type === "prediction" || event.type === "operation_prediction") && event.mode && event.settlement.observation === "observed") {
		const match = event.settlement.match;
		next.modesBySource = addMode(current.modesBySource, event.settlement.prediction.source, event.mode,
			{ observed: 1, matched: Number(match.matched), adopted: Number(match.matched && match.adoption.status === "adopted") });
	} else if (event.type === "candidate" && event.candidate.mode && event.candidate.origin === "prediction") {
		next.modesBySource = addMode(current.modesBySource, event.candidate.source, event.candidate.mode,
			event.state.status === "running" ? { started: 1 } : { productionMs: metric(event.state.executionMs) });
	} else if (event.type === "actor_action") {
		for (const timing of event.computation?.reusedByMode ?? []) next.modesBySource = addMode(next.modesBySource, timing.source, timing.mode,
			{ reusedExecutionMs: metric(timing.reusedExecutionMs) });
	}
	switch (event.type) {
		case "operation_prediction":
			next.operationPredictionsSettled++;
			if (event.settlement.observation === "observed" && event.settlement.match.matched &&
				event.settlement.match.adoption.status === "adopted") next.operationPredictionsAdopted++;
			break;
		case "task":
			next.tasks++;
			next.actorComputeMs = current.actorComputeMs !== undefined && event.timing.actorComputeMs !== undefined
				? current.actorComputeMs + metric(event.timing.actorComputeMs) : undefined;
			next.toolWaitMs += metric(event.timing.toolWaitMs);
			next.reusedExecutionMs += metric(event.timing.reusedExecutionMs);
			next.reusedExecutionIncomplete = current.reusedExecutionIncomplete || event.timing.reusedExecutionIncomplete;
			break;
		case "source_request":
			next.sourceRequests++;
			next.totalDraftTokens = Math.max(next.totalDraftTokens, metric(event.totalDraftTokens));
			next.sourceOutcomes = increment(current.sourceOutcomes, event.request.settlement.status);
			if (event.request.settlement.status === "error" || event.request.settlement.status === "timeout") {
				const { cause } = event.request.settlement;
				next.lastSourceFailure = `${event.request.request.source} ${causeKey(cause)}${cause.detail ? ` ${cause.detail.slice(0, 200)}` : ""}`;
			}
			break;
		case "prediction": {
			next.predictionsSettled++;
			const settlement = event.settlement;
			if (settlement.observation === "unobserved") {
				next.predictionUnobserved = increment(current.predictionUnobserved, causeKey(settlement.cause));
				break;
			}
			next.predictionsObserved++;
			next.predictionsBySource = tally(current.predictionsBySource, settlement.prediction.source, true, settlement.match.matched,
				settlement.match.matched && settlement.match.adoption.status === "adopted");
			if (!settlement.match.matched) break;
			next.predictionsMatched++;
			if (settlement.match.adoption.status === "adopted") next.predictionsAdopted++;
			else next.predictionRejectedAfterMatch = increment(current.predictionRejectedAfterMatch, causeKey(settlement.match.adoption.cause));
			break;
		}
		case "candidate":
			if (event.state.status === "running") next.candidateStarted++;
			else {
				if (event.state.status === "succeeded") {
					next.candidateSucceeded++;
					const reuse = event.candidate.world?.executionMetrics.reuse;
					if (reuse) next.processReuse = addReuseMetrics(next.processReuse, reuse);
				}
				else {
					if (event.state.status === "failed") next.candidateFailed++;
					else next.candidateCancelled++;
					next.candidateTerminalCauses = increment(current.candidateTerminalCauses, causeKey(event.state.cause));
				}
			}
			break;
		case "actor_action":
			next.actorActions++;
			next.actorActionsByTool = tally(current.actorActionsByTool, event.settlement.tool, true, event.settlement.provider.kind === "speculative");
			if (event.settlement.rejections.length) {
				const counts = { ...current.actorCandidateRejections };
				for (const rejection of event.settlement.rejections) {
					const key = causeKey(rejection.cause);
					counts[key] = (counts[key] ?? 0) + 1;
				}
				next.actorCandidateRejections = counts;
			}
			if (event.settlement.provider.kind === "speculative") {
				next.speculativeHits++;
				const match = event.settlement.provider.match;
				if (match.kind === "projected") {
					next.partialResultReuseHits++;
					next.partialResultReuseByProjector = increment(current.partialResultReuseByProjector, match.projector);
				} else if (match.kind === "inputs") next.inputReuseHits++;
				else next.exactReuseHits++;
			} else {
				if (event.settlement.provider.origin === "preview") {
					next.actorPreviews++;
				} else {
					next.actorFallbacks++;
				}
			}
			break;
	}
	next.hitRate = ratio(next.speculativeHits, next.actorActions);
	next.predictionPrecision = ratio(next.predictionsMatched, next.predictionsObserved);
	next.adoptionYield = ratio(next.predictionsAdopted, next.predictionsMatched);
	return next;
}

export function summarizeSpeculativeTrace<SessionID>(
	events: ReadonlyArray<SpeculativeActionEvent<SessionID>>,
): SpeculativeTraceSummary {
	return events.reduce<SpeculativeTraceSummary>(reduceSpeculativeTrace, emptySpeculativeTraceSummary());
}

function addReuseMetrics(left: WorldReuseMetrics, right: WorldReuseMetrics): WorldReuseMetrics {
	const merged = { ...left } as unknown as Record<string, number | string | undefined>;
	for (const [key, value] of Object.entries(right)) {
		if (key === "lastError") merged[key] = value;
		else merged[key] = metric(Number(merged[key])) + metric(Number(value));
	}
	return merged as unknown as WorldReuseMetrics;
}

function causeKey(cause: ResolutionCause): string {
	return `${cause.stage}:${cause.code}`;
}

function ratio(numerator: number, denominator: number): number {
	return denominator > 0 ? numerator / denominator : 0;
}

/** Parallel counters per key: each flag increments its own position. */
function tally(target: Readonly<Record<string, readonly number[]>>, key: string, ...flags: boolean[]): Record<string, readonly number[]> {
	return { ...target, [key]: flags.map((flag, index) => (target[key]?.[index] ?? 0) + Number(flag)) };
}

function increment(target: Readonly<Record<string, number>>, key: string): Record<string, number> {
	return { ...target, [key]: (target[key] ?? 0) + 1 };
}

function addMode(target: ModesBySource, source: string, mode: string, delta: Partial<ModeTraceSummary>): ModesBySource {
	const modes = Object.hasOwn(target, source) ? target[source]! : {};
	const current = Object.hasOwn(modes, mode) ? modes[mode]! : {
		observed: 0, matched: 0, adopted: 0, started: 0, productionMs: 0, reusedExecutionMs: 0,
	};
	const next = { ...current };
	for (const key of Object.keys(delta) as (keyof ModeTraceSummary)[]) next[key] += metric(delta[key]);
	return { ...target, [source]: { ...modes, [mode]: next } };
}
