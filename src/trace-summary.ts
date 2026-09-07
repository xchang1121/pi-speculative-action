import type { SpeculativeActionEvent, SpeculativeCacheSnapshot } from "./events.ts";
import { emptyWorldReuseMetrics, type WorldReuseMetrics } from "./execution-world.ts";
import type { ResolutionCause } from "./settlement.ts";

export type SpeculativeTraceSummary = Readonly<ReturnType<typeof emptySpeculativeTraceSummary>>;

const EMPTY_CACHE: SpeculativeCacheSnapshot = {
	cacheCapacity: 0,
	cacheByteCapacity: 0,
	cacheCold: 0,
	cacheHot: 0,
	inFlightJobs: 0,
	resultEntries: 0,
	resultBytes: 0,
	branchEntries: 0,
	branchBytes: 0,
	exclusiveCandidates: 0,
	sharedCandidates: 0,
	cacheTools: [],
	cacheExecutions: [],
};

export function emptySpeculativeTraceSummary(cache: SpeculativeCacheSnapshot | Pick<SpeculativeCacheSnapshot, "cacheCapacity" | "cacheByteCapacity"> = EMPTY_CACHE) {
	return {
		sourceRequests: 0,
		sourceOutcomes: {} as Readonly<Record<string, number>>,
		predictionsSettled: 0,
		predictionsObserved: 0,
		predictionsMatched: 0,
		predictionsAdopted: 0,
		predictionPrecision: 0,
		adoptionYield: 0,
		predictionUnobserved: {} as Readonly<Record<string, number>>,
		predictionRejectedAfterMatch: {} as Readonly<Record<string, number>>,
		candidateStarted: 0,
		candidateSucceeded: 0,
		candidateFailed: 0,
		candidateCancelled: 0,
		candidateTerminalCauses: {} as Readonly<Record<string, number>>,
		actorActions: 0,
		speculativeHits: 0,
		exactReuseHits: 0, // Adopted identical K(a) results.
		partialResultReuseHits: 0, // Adopted lossless views from a different K(a); its execution ran in full.
		partialResultReuseByProjector: {} as Readonly<Record<string, number>>,
		actorPreviews: 0,
		actorFallbacks: 0,
		hitRate: 0,
		actorCandidateRejections: {} as Readonly<Record<string, number>>,
		tasks: 0,
		endToEndMs: 0,
		nonToolMs: 0,
		actorPhaseMs: 0,
		orchestrationMs: 0,
		toolExecutionMs: 0,
		serializedMs: 0,
		hiddenLatencyMs: 0,
		speculativeExecutionMs: 0,
		actorExecutionMs: 0,
		executionAheadMs: 0,
		attemptLeadMs: 0,
		hitLatencyMs: 0,
		executionBlockedActorActions: 0, // Matched predictions deliberately not executed.
		executionBlockedAttemptLeadMs: 0, // Earliest matching intent to Actor interception.
		executionBlockedPotentialHiddenLatencyMs: 0, // Counterfactual safe overlap, not measured speedup.
		executionBlockedPotentialHitLatencyMs: 0, // Actor work remaining after that overlap.
		totalDraftTokens: 0,
		processReuse: emptyWorldReuseMetrics(), // Inside speculative worlds, never the Actor route.
		cache: cloneCache({ ...EMPTY_CACHE, ...cache }),
	};
}

/** The sole reducer used by both live UI state and persisted trace replay. */
export function reduceSpeculativeTrace<SessionID>(
	current: SpeculativeTraceSummary,
	event: SpeculativeActionEvent<SessionID>,
): SpeculativeTraceSummary {
	const next = mutableSummary(current);
	next.cache = cloneCache(event.cache);
	switch (event.type) {
		case "task":
			next.tasks++;
			next.endToEndMs += metric(event.timing.endToEndMs);
			next.nonToolMs += metric(event.timing.nonToolMs);
			next.actorPhaseMs += metric(event.timing.actorPhaseMs);
			next.orchestrationMs += metric(event.timing.orchestrationMs);
			next.toolExecutionMs += metric(event.timing.toolExecutionMs);
			next.serializedMs += metric(event.timing.serializedMs);
			next.hiddenLatencyMs += metric(event.timing.hiddenLatencyMs);
			break;
		case "source_request":
			next.sourceRequests++;
			increment(next.sourceOutcomes, event.request.settlement.status);
			break;
		case "prediction": {
			next.predictionsSettled++;
			const settlement = event.settlement;
			if (settlement.observation === "unobserved") {
				increment(next.predictionUnobserved, causeKey(settlement.cause));
				break;
			}
			next.predictionsObserved++;
			if (!settlement.match.matched) break;
			next.predictionsMatched++;
			if (settlement.match.adoption.status === "adopted") next.predictionsAdopted++;
			else increment(next.predictionRejectedAfterMatch, causeKey(settlement.match.adoption.cause));
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
					increment(next.candidateTerminalCauses, causeKey(event.state.cause));
				}
			}
			break;
		case "actor_action":
			next.actorActions++;
			for (const rejection of event.settlement.rejections) {
				increment(next.actorCandidateRejections, causeKey(rejection.cause));
			}
			if (event.settlement.provider.kind === "speculative") {
				next.speculativeHits++;
				const match = event.settlement.provider.match;
				if (match.kind === "projected") {
					next.partialResultReuseHits++;
					increment(next.partialResultReuseByProjector, match.projector);
				} else next.exactReuseHits++;
				next.executionAheadMs += metric(event.settlement.provider.timing.executionAheadMs);
				next.attemptLeadMs += metric(event.settlement.provider.timing.attemptLeadMs);
				next.hitLatencyMs += metric(event.settlement.provider.timing.hitLatencyMs);
			} else {
				next.actorExecutionMs += metric(event.settlement.provider.durationMs);
				if (event.settlement.provider.origin === "preview") {
					next.actorPreviews++;
				} else {
					next.actorFallbacks++;
					const timing = event.settlement.provider.executionBlockedTiming;
					if (timing) {
						next.executionBlockedActorActions++;
						next.executionBlockedAttemptLeadMs += metric(timing.attemptLeadMs);
						next.executionBlockedPotentialHiddenLatencyMs += metric(timing.executionAheadMs);
						next.executionBlockedPotentialHitLatencyMs += metric(timing.hitLatencyMs);
					}
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

type MutableSummary = {
	-readonly [Key in keyof SpeculativeTraceSummary]: SpeculativeTraceSummary[Key] extends Readonly<
		Record<string, number>
	>
		? Record<string, number>
		: SpeculativeTraceSummary[Key];
};

function mutableSummary(current: SpeculativeTraceSummary): MutableSummary {
	return {
		...current,
		sourceOutcomes: { ...current.sourceOutcomes },
		predictionUnobserved: { ...current.predictionUnobserved },
		predictionRejectedAfterMatch: { ...current.predictionRejectedAfterMatch },
		candidateTerminalCauses: { ...current.candidateTerminalCauses },
		actorCandidateRejections: { ...current.actorCandidateRejections },
		partialResultReuseByProjector: { ...current.partialResultReuseByProjector },
		processReuse: { ...current.processReuse },
		cache: cloneCache(current.cache),
	};
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

function metric(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function ratio(numerator: number, denominator: number): number {
	return denominator > 0 ? numerator / denominator : 0;
}

function increment(target: Record<string, number>, key: string): void {
	target[key] = (target[key] ?? 0) + 1;
}
