import type { SpeculativeActionEvent } from "../src/events.ts";

/** Keep benchmark dimensions and their chronological traces on the same event inventory. */
export function benchmarkTraceReport<SessionID>(
	events: readonly SpeculativeActionEvent<SessionID>[],
	actorActionsByTool: Record<string, number>,
	speculationEnabled: boolean,
) {
	const requests = events.filter((event) => event.type === "source_request").map((event) => event.request.request);
	const predictions = events.filter((event) => event.type === "prediction").map((event) => event.settlement);
	const candidates = events.filter((event) => event.type === "candidate").filter((event) => event.state.status === "running");
	const actors = events.filter((event) => event.type === "actor_action");
	const hits = actors.flatMap((event) => event.settlement.provider.kind === "speculative"
		? [{ event, provider: event.settlement.provider }] : []);
	const native = actors.flatMap((event) => event.settlement.provider.kind === "actor"
		? [{ tool: event.settlement.tool, origin: event.settlement.provider.origin }] : []);
	const actorActionTrace = actors.map((event) => ({
		turnID: event.turnID,
		sequence: event.settlement.actorAction.sequence,
		tool: event.settlement.tool,
		action: event.actualAction,
		provider: event.settlement.provider.kind,
		matchedPredictionSources: [...new Set(event.settlement.matchedPredictions.map((prediction) => prediction.source))],
		...(event.candidate ? { candidateSource: event.candidate.source, predictedAction: event.candidate.predictedAction } : {}),
	}));
	const predictionsBySource: Record<string, { settled: number; observed: number; matched: number; adopted: number }> = {};
	for (const settlement of predictions) {
		const counts = (predictionsBySource[settlement.prediction.source] ??= { settled: 0, observed: 0, matched: 0, adopted: 0 });
		counts.settled++;
		if (settlement.observation === "unobserved") continue;
		counts.observed++;
		if (!settlement.match.matched) continue;
		counts.matched++;
		if (settlement.match.adoption.status === "adopted") counts.adopted++;
	}
	return {
		predictionsBySource,
		sourceRequestKinds: countBy(requests, (request) => request.kind),
		sourceRequestsBySource: countBy(requests, (request) => request.source),
		candidateStartsBySource: countBy(candidates, (event) => event.candidate.source),
		candidateStartsByTool: countBy(candidates, (event) => event.candidate.tool),
		candidateStartsByDepth: countBy(candidates, (event) => String(event.candidate.depth)),
		speculativeHitsByDepth: countBy(hits, ({ event }) => String(event.candidate?.depth ?? 0)),
		speculativeHitsByTool: countBy(hits, ({ event }) => event.settlement.tool),
		speculativeHitsByRelation: countBy(hits, ({ provider }) => provider.match.kind === "projected" ? `projected:${provider.match.projector}` : provider.match.kind),
		speculativeHitProvidersBySource: countBy(hits, ({ event }) => event.candidate?.source ?? "cache"),
		actorActionMatchesByPredictionSource: countBy(actorActionTrace.flatMap((event) => event.matchedPredictionSources), (source) => source),
		actorFallbacksByTool: countBy(native.filter((event) => event.origin !== "preview"), (event) => event.tool,
			speculationEnabled ? {} : { ...actorActionsByTool }),
		actorPreviewsByTool: countBy(native.filter((event) => event.origin === "preview"), (event) => event.tool),
		candidateStartTrace: candidates.map((event) => ({ turnID: event.turnID, source: event.candidate.source,
			tool: event.candidate.tool, depth: event.candidate.depth, action: event.candidate.predictedAction })),
		actorActionTrace,
	};
}

function countBy<Value>(values: readonly Value[], key: (value: Value) => string, counts: Record<string, number> = {}) {
	for (const value of values) {
		const name = key(value);
		counts[name] = (counts[name] ?? 0) + 1;
	}
	return counts;
}
