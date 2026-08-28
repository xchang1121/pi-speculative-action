import { describe, expect, test } from "vitest";
import type { CandidateEventDescriptor, SpeculativeActionEvent, SpeculativeCacheSnapshot } from "../src/events.ts";
import {
	emptySpeculativeTraceSummary,
	reduceSpeculativeTrace,
	summarizeSpeculativeTrace,
} from "../src/trace-summary.ts";

describe("speculative trace reduction", () => {
	test("keeps source, prediction, execution, and Actor outcomes orthogonal in live and replayed metrics", () => {
		const events = authoritativeEvents();
		const live = events.reduce(reduceSpeculativeTrace, emptySpeculativeTraceSummary());
		const replayed = summarizeSpeculativeTrace(events);

		expect(live).toEqual(replayed);
		expect(replayed).toMatchObject({
			sourceRequests: 1,
			sourceOutcomes: { timeout: 1 },
			predictionsSettled: 4,
			predictionsObserved: 3,
			predictionsMatched: 2,
			predictionsAdopted: 1,
			predictionPrecision: 2 / 3,
			adoptionYield: 1 / 2,
			predictionUnobserved: { "source:timeout": 1 },
			predictionRejectedAfterMatch: { "freshness:resource_changed": 1 },
			candidateStarted: 2,
			candidateSucceeded: 1,
			candidateFailed: 0,
			candidateCancelled: 1,
			candidateTerminalCauses: { "control:preempted": 1 },
			actorActions: 2,
			speculativeHits: 1,
			exactReuseHits: 1,
			partialResultReuseHits: 0,
			partialResultReuseByProjector: {},
			actorFallbacks: 1,
			hitRate: 1 / 2,
			actorCandidateRejections: { "compatibility:backend_indeterminate": 1 },
			tasks: 1,
			endToEndMs: 100,
			nonToolMs: 70,
			toolExecutionMs: 42,
			serializedMs: 112,
			hiddenLatencyMs: 12,
			speculativeExecutionMs: 40,
			actorExecutionMs: 12,
			executionAheadMs: 30,
			attemptLeadMs: 80,
			hitLatencyMs: 10,
			executionBlockedActorActions: 1,
			executionBlockedAttemptLeadMs: 20,
			executionBlockedPotentialHiddenLatencyMs: 12,
			executionBlockedPotentialHitLatencyMs: 0,
			totalDraftTokens: 7,
			cache: { resultEntries: 2, cacheCold: 1, cacheHot: 1 },
		});
	});

	test("classifies exact and projected adoptions as whole-action and partial-result reuse", () => {
		const events = [
			actorHit("exact", { kind: "exact", distance: 0 }),
			actorHit("projected", { kind: "projected", projector: "read.range", distance: 40 }),
		];

		expect(summarizeSpeculativeTrace(events)).toMatchObject({
			actorActions: 2,
			speculativeHits: 2,
			exactReuseHits: 1,
			partialResultReuseHits: 1,
			partialResultReuseByProjector: { "read.range": 1 },
		});
	});
});

function actorHit(
	id: string,
	match:
		| { readonly kind: "exact"; readonly distance: 0 }
		| { readonly kind: "projected"; readonly projector: string; readonly distance: number },
): SpeculativeActionEvent<string> {
	return {
		sessionID: "session",
		turnID: "turn",
		timestamp: 1,
		cache: cache(),
		type: "actor_action",
		actualAction: `read ${id}`,
		settlement: {
			actorAction: { id, sequence: 1, turnID: "turn" },
			tool: "read",
			matchedPredictions: [],
			rejections: [],
			provider: {
				kind: "speculative",
				candidateID: `candidate-${id}`,
				match,
				timing: { executionAheadMs: 1, attemptLeadMs: 2, hitLatencyMs: 0 },
				toolExecution: { startedAt: 0, completedAt: 1 },
			},
		},
	};
}

function authoritativeEvents(): SpeculativeActionEvent<string>[] {
	const base = { sessionID: "session", turnID: "turn", timestamp: 1, cache: cache() };
	const actorAction = { id: "actor", sequence: 1, turnID: "turn" };
	const prediction = { id: "prediction", source: "pattern_aware", proposalID: "proposal", actionID: "action" };
	const exact = { kind: "exact" as const, distance: 0 as const };
	return [
		{
			...base,
			type: "task",
			timing: {
				startedAt: 0,
				completedAt: 100,
				endToEndMs: 100,
				nonToolMs: 70,
				actorPhaseMs: 65,
				orchestrationMs: 5,
				toolExecutionMs: 42,
				serializedMs: 112,
				hiddenLatencyMs: 12,
				authoritativeToolCount: 2,
			},
		},
		{
			...base,
			type: "source_request",
			request: {
				request: {
					source: "drafter",
					turnID: "turn",
					index: 0,
					kind: "proposal",
					targetDecisionSequence: 1,
				},
				startedAt: 0,
				durationMs: 5,
				settlement: { status: "timeout", cause: { stage: "source", code: "timeout" } },
			},
		},
		{
			...base,
			type: "prediction",
			settlement: { prediction, observation: "unobserved", cause: { stage: "source", code: "timeout" } },
		},
		{
			...base,
			type: "prediction",
			settlement: { prediction, observation: "observed", actorAction, match: { matched: false } },
		},
		{
			...base,
			type: "prediction",
			settlement: {
				prediction,
				observation: "observed",
				actorAction,
				match: {
					matched: true,
					relation: exact,
					adoption: {
						status: "rejected",
						candidateID: "candidate",
						cause: { stage: "freshness", code: "resource_changed" },
					},
				},
			},
		},
		{
			...base,
			type: "prediction",
			settlement: {
				prediction,
				observation: "observed",
				actorAction,
				match: {
					matched: true,
					relation: exact,
					adoption: { status: "adopted", candidateID: "candidate" },
				},
			},
		},
		{ ...base, type: "candidate", candidate: candidate("one"), state: { status: "running", startedAt: 0 } },
		{
			...base,
			type: "candidate",
			candidate: candidate("one"),
			state: { status: "succeeded", startedAt: 0, completedAt: 40, executionMs: 40 },
		},
		{ ...base, type: "candidate", candidate: candidate("two"), state: { status: "running", startedAt: 0 } },
		{
			...base,
			type: "candidate",
			candidate: candidate("two"),
			state: {
				status: "cancelled",
				cause: { stage: "control", code: "preempted" },
				startedAt: 0,
				completedAt: 1,
				executionMs: Number.NaN,
			},
		},
		{
			...base,
			type: "actor_action",
			actualAction: "read README.md",
			settlement: {
				actorAction,
				tool: "read",
				actionKeyHash: "hash",
				matchedPredictions: [{ id: "prediction", source: "pattern_aware", proposalID: "plan", actionID: "next" }],
				rejections: [
					{
						candidateID: "rejected",
						match: exact,
						cause: { stage: "compatibility", code: "backend_indeterminate" },
					},
				],
				provider: {
					kind: "speculative",
					candidateID: "candidate",
					match: exact,
					timing: { executionAheadMs: 30, attemptLeadMs: 80, hitLatencyMs: 10 },
					toolExecution: { startedAt: 0, completedAt: 30 },
				},
			},
		},
		{
			...base,
			cache: cache({ resultEntries: 2, cacheCold: 1, cacheHot: 1 }),
			type: "actor_action",
			actualAction: "read other.ts",
			settlement: {
				actorAction: { ...actorAction, id: "actor-2", sequence: 2 },
				tool: "read",
				matchedPredictions: [],
				rejections: [],
				provider: {
					kind: "actor",
					origin: "fallback",
					durationMs: 12,
					isError: false,
					toolExecution: { startedAt: 40, completedAt: 52 },
					executionBlockedTiming: { attemptLeadMs: 20, executionAheadMs: 12, hitLatencyMs: 0 },
				},
			},
		},
	];
}

function candidate(id: string): CandidateEventDescriptor {
	return {
		id,
		origin: "prediction",
		tool: "read",
		actionKeyHash: `hash-${id}`,
		execution: "resource_snapshot",
		source: "pattern_aware",
		depth: 0,
		predictedAction: "read README.md",
		predictionLatencyMs: 2,
		draftTokens: 7,
		totalDraftTokens: 7,
		expectedDurationMs: 40,
		estimatedBytes: 128,
		validation: { durationMs: 0, bytesRead: 0, filesRead: 0 },
	};
}

function cache(overrides: Partial<SpeculativeCacheSnapshot> = {}): SpeculativeCacheSnapshot {
	return {
		cacheCapacity: 8,
		cacheByteCapacity: 1024,
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
		...overrides,
	};
}
