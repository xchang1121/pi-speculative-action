import { describe, expect, it } from "vitest";
import { slowCallReport } from "../bench/trace-report.ts";
import { ActorAction } from "../src/actor-action.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { cause } from "../src/settlement.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { emptySpeculativeTraceSummary } from "../src/trace-summary.ts";

describe("slow tool call diagnosis", () => {
	it.each([0, 80])("joins recorded stages and recognizes partial reuse with %s ms hidden", hiddenComputeMs => {
		const envelope = { sessionID: "session", turnID: "turn", timestamp: 1000, cache: emptySpeculativeTraceSummary().cache };
		const rejected = new ActorAction({ issuedAt: 100, identity: { id: "stale", sequence: 1, turnID: "turn" }, tool: "bash", fallback: cause("matching", "no_candidate") });
		rejected.rejectCandidate("candidate", { kind: "exact", distance: 0 }, cause("freshness", "resource_changed"));
		rejected.deferToFallback(); rejected.settleActor(new TimelineInterval(200, 900), false);
		const partial = new ActorAction({ issuedAt: 100, identity: { id: "partial", sequence: 2, turnID: "turn" }, tool: "bash", fallback: cause("matching", "no_candidate") });
		partial.deferToFallback(); partial.settleActor(new TimelineInterval(500, 900), false);
		const events: SpeculativeActionEvent<string>[] = [
			{ ...envelope, type: "source_request", request: { request: { source: "drafter", turnID: "turn", index: 0, kind: "proposal", targetDecisionSequence: 1 },
				durationMs: 1, settlement: { status: "empty", cause: cause("source", "drafter_token_limit") } }, totalDraftTokens: 100 },
			{ ...envelope, type: "candidate", candidate: { id: "candidate", origin: "prediction", tool: "bash", source: "pattern_aware", depth: 0, predictedAction: "bash build",
				route: { backend: "resource_version", isolation: "resource_snapshot", reuse: "shared_result", scope: "fallback", fingerprint: "test" } }, state: { status: "succeeded", executionMs: 500 } },
			{ ...envelope, type: "prediction", settlement: { prediction: { id: "prediction", source: "pattern_aware", proposalID: "proposal", actionID: "action" },
				observation: "observed", actorAction: rejected.identity, match: { matched: true, relation: { kind: "exact", distance: 0 },
					adoption: { status: "rejected", candidateID: "candidate", cause: cause("freshness", "resource_changed") } } } },
			{ ...envelope, type: "actor_action", settlement: rejected.settlement!, actualAction: "bash build", computation: { toolComputeMs: 700, hiddenComputeMs: 0 } },
			{ ...envelope, type: "actor_action", settlement: partial.settlement!, actualAction: "bash wrapper", computation: { toolComputeMs: 480, hiddenComputeMs, reused: true } },
		];
		// Another session's matching display string and decision must not become this call's evidence.
		events.push({ ...events[0]!, sessionID: "other" } as SpeculativeActionEvent<string>);
		const report = slowCallReport(events, [
			{ id: "stale", startedAt: 0, completedAt: 1000 }, { id: "partial", startedAt: 200, completedAt: 1000 },
			{ id: "missing", startedAt: 300, completedAt: 900 }, { id: "short", startedAt: 0, completedAt: 100 },
			{ id: "unfinished", startedAt: 0 }, { id: "invalid", startedAt: NaN, completedAt: 1000 },
		]);
		expect(report).toMatchObject({ thresholdMs: 500, measuredCalls: 4, slowCalls: 3, summedCallWaitMs: 2500, summedSlowCallWaitMs: 2400, slowCallWaitFraction: 0.96 });
		expect(report.calls.map(call => call.id)).toEqual(["stale", "partial", "missing"]);
		expect(report.calls[0]).toMatchObject({ diagnosis: "recorded_settlement", partialReuse: false,
			actor: { settlement: { provider: { cause: { stage: "freshness", code: "resource_changed" } } } },
			sourceRequestsForDecision: [{ request: { settlement: { cause: { code: "drafter_token_limit" } } } }],
			predictions: [{ settlement: { match: { matched: true, adoption: { status: "rejected" } } } }],
			candidates: [{ candidate: { id: "candidate" }, state: { status: "succeeded" } }],
		});
		const staleCall = report.calls[0]!;
		if (staleCall.diagnosis !== "recorded_settlement") throw new Error("Expected the stale call's recorded settlement");
		expect(staleCall.sourceRequestsForDecision).toHaveLength(1);
		expect(report.calls[1]).toMatchObject({ partialReuse: true, sourceRequestsForDecision: [], candidates: [],
			actor: { computation: { hiddenComputeMs }, settlement: { provider: { cause: { code: "no_candidate" } } } } });
		expect(report.calls[2]).toMatchObject({ diagnosis: "settlement_unavailable" });
	});

	it.each([
		{ name: "sessions", sessionID: "other", turnID: "turn", sequence: 1 },
		{ name: "turns", sessionID: "session", turnID: "other", sequence: 1 },
		{ name: "sequences", sessionID: "session", turnID: "turn", sequence: 2 },
	])("keeps an unscoped wait ambiguous when its ID occurs in multiple $name", ({ sessionID, turnID, sequence }) => {
		const action = new ActorAction({ issuedAt: 100, identity: { id: "shared", sequence: 1, turnID: "turn" }, tool: "bash", fallback: cause("matching", "no_candidate") });
		action.deferToFallback(); action.settleActor(new TimelineInterval(0, 900), false);
		const first: SpeculativeActionEvent<string> = { sessionID: "session", turnID: "turn", timestamp: 1000, cache: emptySpeculativeTraceSummary().cache,
			type: "actor_action", settlement: action.settlement!, actualAction: "bash build" };
		const second: SpeculativeActionEvent<string> = { ...first, sessionID, turnID,
			settlement: { ...action.settlement!, actorAction: { ...action.identity, sequence, turnID } } };
		for (const events of [[first, second], [second, first]]) {
			expect(slowCallReport(events, [{ id: "shared", startedAt: 0, completedAt: 1000 }]).calls).toEqual([
				{ id: "shared", startedAt: 0, completedAt: 1000, wallMs: 1000, diagnosis: "settlement_ambiguous" },
			]);
		}
	});

	it("joins predictions only to the complete actor identity in the same session", () => {
		const action = new ActorAction({ issuedAt: 100, identity: { id: "shared", sequence: 1, turnID: "turn" }, tool: "bash", fallback: cause("matching", "no_candidate") });
		action.deferToFallback(); action.settleActor(new TimelineInterval(0, 900), false);
		const envelope = { sessionID: "session", turnID: "turn", timestamp: 1000, cache: emptySpeculativeTraceSummary().cache };
		const predictions: SpeculativeActionEvent<string>[] = [
			{ name: "correct", sessionID: "session", identity: action.identity },
			{ name: "other-session", sessionID: "other", identity: action.identity },
			{ name: "other-turn", sessionID: "session", identity: { ...action.identity, turnID: "other" } },
			{ name: "other-sequence", sessionID: "session", identity: { ...action.identity, sequence: 2 } },
			{ name: "other-kind", sessionID: "session", identity: { ...action.identity, kind: "operation" as const } },
		].map<SpeculativeActionEvent<string>>(({ name, sessionID, identity }) => ({ ...envelope, sessionID, type: identity.kind === "operation" ? "operation_prediction" : "prediction",
			settlement: { prediction: { id: name, source: "pattern_aware", proposalID: "proposal", actionID: name },
				observation: "observed", actorAction: identity, match: { matched: false } } }));
		const report = slowCallReport([...predictions, { ...envelope, type: "actor_action", settlement: action.settlement!, actualAction: "bash build" }],
			[{ id: "shared", startedAt: 0, completedAt: 1000 }]);
		const call = report.calls[0]!;
		if (call.diagnosis !== "recorded_settlement") throw new Error("Expected a uniquely identified actor settlement");
		expect(call.predictions).toHaveLength(1);
		expect(call.predictions[0]!.settlement.prediction.id).toBe("correct");
	});

	it("does not fabricate elapsed waits or a coverage percentage for unfinished calls", () => {
		expect(slowCallReport([], [{ id: "running", startedAt: 100 }])).toMatchObject({ measuredCalls: 0, slowCalls: 0, slowCallWaitFraction: null, calls: [] });
	});

	it("keeps a whole Actor preview distinct from computation reused inside fallback execution", () => {
		const identity = { id: "preview", sequence: 1, turnID: "turn" };
		const event: SpeculativeActionEvent<string> = {
			sessionID: "session", turnID: "turn", timestamp: 1000, cache: emptySpeculativeTraceSummary().cache, type: "actor_action",
			settlement: { actorAction: identity, tool: "read", matchedPredictions: [], rejections: [],
				provider: { kind: "actor", origin: "preview", candidateID: "candidate", durationMs: 600, isError: false,
					toolExecution: new TimelineInterval(0, 600) } },
			actualAction: "read source", computation: { toolComputeMs: 600, hiddenComputeMs: 600 },
		};
		expect(slowCallReport([event], [{ id: "preview", startedAt: 0, completedAt: 700 }]).calls[0]).toMatchObject({
			partialReuse: false, actor: { computation: { toolComputeMs: 600, hiddenComputeMs: 600 } },
		});
	});
});
