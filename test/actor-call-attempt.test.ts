import { describe, expect, it, vi } from "vitest";
import { ActorCallAttempt } from "../src/actor-call-attempt.ts";
import { ActorAction } from "../src/actor-action.ts";
import { cause } from "../src/settlement.ts";

const identity = { id: "call", sequence: 1, turnID: "turn" } as const;
const exact = { kind: "exact", distance: 0 } as const;

describe("ActorCallAttempt", () => {
	it("owns rejection context and releases Actor admission exactly once", () => {
		const release = vi.fn();
		const action = new ActorAction({ identity, tool: "read" });
		const attempt = new ActorCallAttempt<{ readonly id: string }, string>({
			action,
			fallback: cause("matching", "no_candidate"),
			releaseActorAdmission: release,
		});
		const stale = cause("freshness", "resource_changed");

		expect(attempt.rejectCandidate("candidate", exact, stale)).toBe(true);
		expect(attempt.fallback).toEqual({ cause: stale, candidateID: "candidate" });
		expect(attempt.deferToFallback()).toEqual({ status: "rejected", cause: stale, candidateID: "candidate" });
		attempt.close();
		attempt.close();
		expect(release).toHaveBeenCalledOnce();
	});

	it("settles one selected provider and returns its prediction adoption", () => {
		const action = new ActorAction({ identity, tool: "read" });
		const attempt = new ActorCallAttempt<{ readonly id: string }, string>({
			action,
			fallback: cause("matching", "no_candidate"),
			releaseActorAdmission: () => {},
		});
		expect(
			attempt.select({
				candidate: { id: "candidate" },
				match: exact,
				output: "value",
				timing: { executionAheadMs: 1, attemptLeadMs: 2, hitLatencyMs: 3 },
				toolExecution: { startedAt: 1, completedAt: 2 },
			}),
		).toBe(true);
		expect(attempt.settleSelection([], "speculative")).toEqual({
			status: "adopted",
			candidateID: "candidate",
		});
		expect(attempt.settleSelection([], "speculative")).toBeUndefined();
		expect(action.settlement?.provider).toMatchObject({ kind: "speculative", candidateID: "candidate" });
	});

	it("leaves unexpected exits ready for the authoritative fallback", () => {
		const action = new ActorAction({ identity, tool: "read" });
		const attempt = new ActorCallAttempt<{ readonly id: string }, string>({
			action,
			fallback: cause("matching", "no_candidate"),
			releaseActorAdmission: () => {},
		});

		attempt.close();
		expect(action.state.status).toBe("awaiting_fallback");
	});
});
