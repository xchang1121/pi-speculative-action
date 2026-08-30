import { describe, expect, it } from "vitest";
import type {
	CandidateEventDescriptor,
	CandidateExecutionProjection,
	SpeculativeActionEvent,
	SpeculativeCacheSnapshot,
} from "../src/events.ts";
import type { SpeculativeExecutionRoute } from "../src/execution-world.ts";
import { DEFAULT_BENEFIT_GATE_POLICY } from "../src/fork-benefit-gate.ts";
import {
	UnboundedExecutionUtilityGate,
	unboundedExecutionUtilityDescriptor,
} from "../src/unbounded-execution-utility-gate.ts";

const ROUTE: SpeculativeExecutionRoute = {
	isolation: "runtime_sandbox",
	reuse: "exclusive_branch",
	scope: "runtime",
	backend: "runtime",
	fingerprint: "runtime:v1",
};

const CACHE: SpeculativeCacheSnapshot = {
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
};

function descriptor(id: string): CandidateEventDescriptor {
	return {
		id,
		origin: "prediction",
		tool: "bash",
		actionKeyHash: `hash:${id}`,
		execution: ROUTE.isolation,
		executionBackend: ROUTE.backend,
		executionFingerprint: ROUTE.fingerprint,
		source: "drafter",
		depth: 0,
		predictedAction: `bash echo ${id}`,
		predictionLatencyMs: 1,
		draftTokens: 1,
		totalDraftTokens: 1,
		expectedDurationMs: 10,
		estimatedBytes: 1,
		validation: { durationMs: 0, bytesRead: 0, filesRead: 0 },
	};
}

function candidateEvent(
	turnID: string,
	candidate: CandidateEventDescriptor,
	state: CandidateExecutionProjection,
): SpeculativeActionEvent<string> {
	return {
		type: "candidate",
		sessionID: "session",
		turnID,
		timestamp: 0,
		cache: CACHE,
		candidate,
		state,
	};
}

function admission(turnID: string) {
	return {
		sessionID: "session",
		turnID,
		prediction: { source: "drafter", proposalID: `proposal:${turnID}`, actionID: "next" },
		descriptor: unboundedExecutionUtilityDescriptor({ source: "drafter", tool: "bash", route: ROUTE }),
		enabled: true,
	};
}

describe("unbounded execution utility gate", () => {
	it("pauses a negative partition and reopens it from an exact shadow match", () => {
		let clock = 0;
		const gate = new UnboundedExecutionUtilityGate(DEFAULT_BENEFIT_GATE_POLICY, () => clock);
		for (let index = 1; index <= 4; index++) {
			const turnID = `turn-${index}`;
			expect(gate.decide(admission(turnID)).allowed).toBe(true);
			const candidate = descriptor(`candidate-${index}`);
			gate.recordEvent(candidateEvent(turnID, candidate, { status: "running", startedAt: clock }));
			clock += 10;
			gate.recordEvent(
				candidateEvent(turnID, candidate, {
					status: "succeeded",
					startedAt: clock - 10,
					completedAt: clock,
					executionMs: 10,
				}),
			);
			gate.finishTurn("session", turnID);
		}

		expect(gate.snapshot().entries[0]).toMatchObject({ samples: 4, expectedNetBenefitMs: -10 });
		const blockedTurn = "turn-5";
		expect(gate.decide(admission(blockedTurn))).toMatchObject({
			allowed: false,
			reason: "negative_utility",
		});
		clock += 500;
		gate.recordEvent(actorFallbackEvent(blockedTurn, "another-proposal", clock, 400));
		expect(gate.snapshot().shadowMatches).toBe(0);
		gate.recordEvent(actorFallbackEvent(blockedTurn, `proposal:${blockedTurn}`, clock, 400));
		gate.finishTurn("session", blockedTurn);

		expect(gate.snapshot()).toMatchObject({ suppressedCandidates: 1, shadowMatches: 1, samples: 4 });
		expect(gate.snapshot().entries[0]?.expectedNetBenefitMs).toBeGreaterThan(25);
		expect(gate.decide(admission("turn-6"))).toMatchObject({ allowed: true, reason: "profitable" });
	});

	it("cancels useful work cost once and credits every realized reuse lead", () => {
		let clock = 0;
		const gate = new UnboundedExecutionUtilityGate(DEFAULT_BENEFIT_GATE_POLICY, () => clock);
		const firstTurn = "hit-1";
		expect(gate.decide(admission(firstTurn)).allowed).toBe(true);
		const candidate = descriptor("useful");
		gate.recordEvent(candidateEvent(firstTurn, candidate, { status: "running", startedAt: 0 }));
		clock = 100;
		gate.recordEvent(
			candidateEvent(firstTurn, candidate, {
				status: "succeeded",
				startedAt: 0,
				completedAt: 100,
				executionMs: 100,
			}),
		);
		gate.recordEvent(speculativeHit(firstTurn, candidate, 40));
		gate.finishTurn("session", firstTurn);
		expect(gate.snapshot().entries[0]?.expectedNetBenefitMs).toBe(40);

		gate.recordEvent(speculativeHit("hit-2", candidate, 20));
		gate.finishTurn("session", "hit-2");
		expect(gate.snapshot().entries[0]).toMatchObject({ samples: 2, expectedNetBenefitMs: 30 });
	});

	it("does not close a finished turn until its running candidate settles", () => {
		const gate = new UnboundedExecutionUtilityGate();
		const turnID = "late-terminal";
		expect(gate.decide(admission(turnID)).allowed).toBe(true);
		const candidate = descriptor("late");
		gate.recordEvent(candidateEvent(turnID, candidate, { status: "running", startedAt: 0 }));
		gate.finishTurn("session", turnID);
		expect(gate.snapshot().samples).toBe(0);

		gate.recordEvent(
			candidateEvent(turnID, candidate, {
				status: "succeeded",
				startedAt: 0,
				completedAt: 10,
				executionMs: 10,
			}),
		);
		expect(gate.snapshot().entries[0]).toMatchObject({ samples: 1, expectedNetBenefitMs: -10 });
	});

	it("bounds independently calibrated runtime fingerprints", () => {
		const gate = new UnboundedExecutionUtilityGate();
		for (let index = 0; index <= 256; index++) {
			gate.decide({
				...admission(`fingerprint-${index}`),
				descriptor: {
					...admission(`fingerprint-${index}`).descriptor,
					routeFingerprint: `runtime:${index}`,
				},
				enabled: false,
			});
		}
		const snapshot = gate.snapshot();
		expect(snapshot.entries).toHaveLength(256);
		expect(snapshot.entries.some((entry) => entry.routeFingerprint === "runtime:0")).toBe(false);
		expect(snapshot.entries.some((entry) => entry.routeFingerprint === "runtime:256")).toBe(true);
	});
});

function speculativeHit(
	turnID: string,
	candidate: CandidateEventDescriptor,
	executionAheadMs: number,
): SpeculativeActionEvent<string> {
	return {
		type: "actor_action",
		sessionID: "session",
		turnID,
		timestamp: 0,
		cache: CACHE,
		actualAction: candidate.predictedAction,
		candidate,
		execution: ROUTE.isolation,
		settlement: {
			actorAction: { id: `actor:${turnID}`, sequence: 1, turnID },
			tool: "bash",
			matchedPredictions: [],
			rejections: [],
			provider: {
				kind: "speculative",
				candidateID: candidate.id,
				match: { kind: "exact", distance: 0 },
				timing: { attemptLeadMs: executionAheadMs, executionAheadMs, hitLatencyMs: 0 },
				toolExecution: { startedAt: 0, completedAt: 100 },
			},
		},
	};
}

function actorFallbackEvent(
	turnID: string,
	proposalID: string,
	startedAt: number,
	durationMs: number,
): SpeculativeActionEvent<string> {
	return {
		type: "actor_action",
		sessionID: "session",
		turnID,
		timestamp: 0,
		cache: CACHE,
		actualAction: "bash echo exact",
		settlement: {
			actorAction: { id: `actor:${turnID}`, sequence: 5, turnID },
			tool: "bash",
			matchedPredictions: [
				{
					id: `prediction:${proposalID}`,
					source: "drafter",
					proposalID,
					actionID: "next",
				},
			],
			rejections: [],
			provider: {
				kind: "actor",
				origin: "fallback",
				durationMs,
				isError: false,
				toolExecution: { startedAt, completedAt: startedAt + durationMs },
				executionBlockedTiming: {
					attemptLeadMs: startedAt,
					executionAheadMs: Math.min(startedAt, durationMs),
					hitLatencyMs: Math.max(0, durationMs - startedAt),
				},
			},
		},
	};
}
