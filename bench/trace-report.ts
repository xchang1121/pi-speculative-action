import type { SpeculativeActionEvent } from "../src/events.ts";

type TraceEvent<SessionID, Kind extends SpeculativeActionEvent<SessionID>["type"]> =
	Omit<Extract<SpeculativeActionEvent<SessionID>, { readonly type: Kind }>, "cache" | "sessionID">;

/** Keep benchmark dimensions and their chronological traces on the same event inventory. */
export function benchmarkTraceReport<SessionID>(
	events: readonly SpeculativeActionEvent<SessionID>[],
	actorActionsByTool: Record<string, number>,
	speculationEnabled: boolean,
) {
	const requests = events.filter((event) => event.type === "source_request");
	const predictions = events.filter((event) => event.type === "prediction" || event.type === "operation_prediction");
	const candidateEvents = events.filter((event) => event.type === "candidate");
	const candidates = candidateEvents.filter((event) => event.state.status === "running");
	const actors = events.filter((event) => event.type === "actor_action");
	const hits = actors.flatMap((event) => event.settlement.provider.kind === "speculative"
		? [{ event, provider: event.settlement.provider }] : []);
	const native = actors.flatMap((event) => event.settlement.provider.kind === "actor"
		? [{ tool: event.settlement.tool, origin: event.settlement.provider.origin }] : []);
	const predictionsBySource: Record<string, { settled: number; observed: number; matched: number; adopted: number }> = {};
	for (const { type, settlement } of predictions) {
		if (type !== "prediction") continue;
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
		sourceRequestKinds: countBy(requests, (event) => event.request.request.kind),
		sourceRequestsBySource: countBy(requests, (event) => event.request.request.source),
		candidateStartsBySource: countBy(candidates, (event) => event.candidate.source),
		candidateStartsByTool: countBy(candidates, (event) => event.candidate.tool),
		candidateStartsByDepth: countBy(candidates, (event) => String(event.candidate.depth)),
		speculativeHitsByDepth: countBy(hits, ({ event }) => String(event.candidate?.depth ?? 0)),
		speculativeHitsByTool: countBy(hits, ({ event }) => event.settlement.tool),
		speculativeHitsByRelation: countBy(hits, ({ provider }) => provider.match.kind === "projected" ? `projected:${provider.match.projector}` : provider.match.kind),
		speculativeHitProvidersBySource: countBy(hits, ({ event }) => event.candidate?.source ?? "cache"),
		actorActionMatchesByPredictionSource: countBy(actors.flatMap(({ settlement }) => [...new Set(settlement.matchedPredictions.map(prediction => prediction.source))]), source => source),
		actorFallbacksByTool: countBy(native.filter((event) => event.origin !== "preview"), (event) => event.tool,
			speculationEnabled ? {} : { ...actorActionsByTool }),
		actorPreviewsByTool: countBy(native.filter((event) => event.origin === "preview"), (event) => event.tool),
		sourceRequestTrace: traceEvents<SessionID, "source_request">(requests),
		predictionTrace: traceEvents<SessionID, "prediction" | "operation_prediction">(predictions),
		candidateTrace: traceEvents<SessionID, "candidate">(candidateEvents),
		actorActionTrace: traceEvents<SessionID, "actor_action">(actors),
	};
}

function traceEvents<SessionID, Kind extends SpeculativeActionEvent<SessionID>["type"]>(
	events: readonly Extract<SpeculativeActionEvent<SessionID>, { readonly type: Kind }>[],
): TraceEvent<SessionID, Kind>[] {
	return events.map(({ cache: _cache, sessionID: _sessionID, ...event }) => event);
}

function countBy<Value>(values: readonly Value[], key: (value: Value) => string, counts: Record<string, number> = {}) {
	for (const value of values) {
		const name = key(value);
		counts[name] = (counts[name] ?? 0) + 1;
	}
	return counts;
}
