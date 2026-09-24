import { finiteNumber, nonNegativeFinite as metric } from "./number-utils.ts";
import type { SpeculativeActionEvent, SpeculativeCacheSnapshot } from "./events.ts";
import { emptyWorldReuseMetrics, type WorldReuseMetrics } from "./execution-world.ts";
import type { ResolutionCause } from "./settlement.ts";

export type SpeculativeTraceSummary = Readonly<ReturnType<typeof emptySpeculativeTraceSummary>>;

const EMPTY_CACHE: SpeculativeCacheSnapshot = {
	cacheCapacity: 0, cacheByteCapacity: 0, cacheCold: 0, cacheHot: 0, inFlightJobs: 0, resultEntries: 0, resultBytes: 0,
	branchEntries: 0, branchBytes: 0, exclusiveCandidates: 0, sharedCandidates: 0, cacheTools: [], cacheExecutions: [],
};

export function emptySpeculativeTraceSummary(cache: SpeculativeCacheSnapshot | Pick<SpeculativeCacheSnapshot, "cacheCapacity" | "cacheByteCapacity"> = EMPTY_CACHE) {
	return {
		sourceRequests: 0,
		sourceOutcomes: {} as Readonly<Record<string, number>>,
		lastSourceFailure: undefined as string | undefined,
		predictionsSettled: 0, predictionsObserved: 0, predictionsMatched: 0, predictionsAdopted: 0, predictionPrecision: 0, adoptionYield: 0,
		predictionsBySource: {} as Readonly<Record<string, readonly number[]>>, // [observed, matched, adopted]
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
		tasks: 0, endToEndMs: 0, nonToolMs: 0, actorPhaseMs: 0, orchestrationMs: 0, toolExecutionMs: 0, serializedMs: 0, hiddenLatencyMs: 0, estimatedSavingsMs: 0,
		speculativeExecutionMs: 0, actorExecutionMs: 0, executionAheadMs: 0, attemptLeadMs: 0, hitLatencyMs: 0, totalDraftTokens: 0,
		processReuse: emptyWorldReuseMetrics(), // Inside speculative worlds, never the Actor route.
		cache: cloneCache({ ...EMPTY_CACHE, ...cache }),
	};
}

/** The sole reducer used by both live UI state and persisted trace replay. */
export function reduceSpeculativeTrace<SessionID>(
	current: SpeculativeTraceSummary,
	event: SpeculativeActionEvent<SessionID>,
): SpeculativeTraceSummary {
	const next = {
		...current,
		cache: cloneCache(event.cache),
	};
	switch (event.type) {
		case "operation_prediction":
			next.operationPredictionsSettled++;
			if (event.settlement.observation === "observed" && event.settlement.match.matched &&
				event.settlement.match.adoption.status === "adopted") next.operationPredictionsAdopted++;
			break;
		case "task":
			next.tasks++;
			next.endToEndMs += metric(event.timing.endToEndMs);
			next.nonToolMs += metric(event.timing.nonToolMs);
			next.actorPhaseMs += metric(event.timing.actorPhaseMs);
			next.orchestrationMs += metric(event.timing.orchestrationMs);
			next.toolExecutionMs += metric(event.timing.toolExecutionMs);
			next.serializedMs += metric(event.timing.serializedMs);
			next.hiddenLatencyMs += metric(event.timing.hiddenLatencyMs);
			next.estimatedSavingsMs += finiteNumber(event.timing.estimatedSavingsMs) ?? 0; // Signed: speculation can cost time.
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
			next.totalDraftTokens = Math.max(next.totalDraftTokens, metric(event.candidate.totalDraftTokens));
			if (event.state.status === "running") next.candidateStarted++;
			else {
				if (event.candidate.origin === "prediction") {
					next.speculativeExecutionMs += metric(event.state.executionMs);
				}
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
				next.executionAheadMs += metric(event.settlement.provider.timing.executionAheadMs);
				next.attemptLeadMs += metric(event.settlement.provider.timing.attemptLeadMs);
				next.hitLatencyMs += metric(event.settlement.provider.timing.hitLatencyMs);
			} else {
				next.actorExecutionMs += metric(event.settlement.provider.durationMs);
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

function cloneCache(cache: SpeculativeCacheSnapshot): SpeculativeCacheSnapshot {
	return { ...cache, cacheTools: [...cache.cacheTools], cacheExecutions: [...cache.cacheExecutions] };
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
