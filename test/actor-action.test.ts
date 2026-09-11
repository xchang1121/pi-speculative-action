import { deferred } from "./async.ts";
import { describe, expect, it, vi } from "vitest";
import { ActorAction } from "../src/actor-action.ts";
import { BoundedEventQueue, PostSettlementQueue } from "../src/post-settlement.ts";
import { cause } from "../src/settlement.ts";

const identity = { id: "call-1", sequence: 7, turnID: "turn-1" } as const;
const exact = { kind: "exact", distance: 0 } as const;
const actionKey = {
	key: "key",
	hash: "hash",
	tool: "read",
	input: { path: "file.ts" },
	resources: ["file.ts"],
	semanticsEpoch: "1",
	schemaHash: "schema",
	executionFingerprint: "executor",
};

describe("ActorAction", () => {
	it("owns candidate rejections and one authoritative provider", () => {
		for (const provider of ["speculative", "preview"] as const) {
			const release = vi.fn(), action = new ActorAction<{ readonly id: string }, string>({
				identity, tool: "read", actionKey, fallback: cause("matching", "no_candidate"), releaseActorAdmission: release,
			});
			const selection = { candidate: { id: "fresh" }, match: exact, output: "value",
				timing: { executionAheadMs: 40, attemptLeadMs: 55, hitLatencyMs: 3 }, toolExecution: { startedAt: 10, completedAt: 50 } };
			expect(action.rejectCandidate("stale", exact, cause("freshness", "resource_changed"))).toBe(true);
			expect(action.select({ ...selection, candidate: { id: "stale" } })).toBe(false);
			expect(action.select(selection)).toBe(true);
			action.close(); action.close();
			expect(action.state.status).toBe("selected");
			expect(action.rejectCandidate("fresh", exact, cause("execution", "late"))).toBe(false);
			expect(action.setFallback(cause("control", "late"))).toBe(false);
			expect(action.deferToFallback()).toBeUndefined();
			expect(action.settleActor(100, false)).toBeUndefined();
			expect(action.settleSelection([{ id: "prediction", source: "pattern", proposalID: "plan", actionID: "next" }], provider))
				.toEqual(provider === "preview" ? { status: "rejected", candidateID: "fresh", cause: cause("control", "actor_preview_provider") }
					: { status: "adopted", candidateID: "fresh" });
			const settled = action.settlement;
			expect(settled).toMatchObject({ actorAction: identity, matchedPredictions: [{ id: "prediction", source: "pattern" }],
				rejections: [{ candidateID: "stale", cause: { stage: "freshness" } }], provider: { candidateID: "fresh",
					...(provider === "preview" ? { kind: "actor", origin: "preview", durationMs: 40 } : { kind: "speculative", match: exact }) } });
			for (const value of [settled?.provider, settled?.matchedPredictions, settled?.rejections[0]?.cause]) {
				expect(Object.isFrozen(value)).toBe(true);
			}
			expect(action.select(selection)).toBe(false);
			expect(action.settleSelection([], provider)).toBeUndefined();
			expect(action.settleActor(100, false)).toBeUndefined();
			action.close();
			expect(release).toHaveBeenCalledOnce();
		}
	});

	it("spans interception and exactly one Actor fallback completion", () => {
		for (const mode of ["empty", "rejected", "interrupted"]) {
			const release = vi.fn(), action = new ActorAction({ identity, tool: "bash",
				fallback: cause("matching", "no_candidate"), releaseActorAdmission: release });
			if (mode !== "empty") {
				const failure = cause("execution", "tool_failed");
				expect(mode === "rejected" ? action.rejectCandidate("failed", exact, failure) : action.setFallback(failure, "failed")).toBe(true);
				expect(action.fallback).toEqual({ cause: failure, candidateID: "failed" });
				expect(action.deferToFallback()).toEqual({ status: "rejected", cause: failure, candidateID: "failed" });
			}
			action.close(); action.close();
			expect(release).toHaveBeenCalledOnce();
			expect(action.state.status).toBe("awaiting_fallback");
			expect(action.rejectCandidate("late", exact, cause("execution", "late"))).toBe(false);
			expect(action.settleActor(Number.NaN, true)).toMatchObject({
				rejections: mode === "rejected" ? [{ candidateID: "failed" }] : [],
				provider: { kind: "actor", durationMs: 0, isError: true },
			});
			expect(action.settleActor(1, false)).toBeUndefined();
		}
	});

	it("settles isolation-blocked benefit with the same capped timing decomposition", () => {
		for (const [attemptLeadMs, executionAheadMs, hitLatencyMs, completedAt] of [[80, 80, 40, 1000], [200, 120, 0, undefined]]) {
			const action = new ActorAction({ identity, tool: "bash", actionKey,
				fallback: cause("execution", "isolation_unavailable"), releaseActorAdmission: () => {} });
			expect(action.deferToFallback([], attemptLeadMs)?.status).toBe("rejected");
			expect(action.settleActor(120, false, completedAt)).toMatchObject({ provider: { kind: "actor", durationMs: 120,
				executionBlockedTiming: { attemptLeadMs, executionAheadMs, hitLatencyMs } } });
		}
	});
});

describe("PostSettlementQueue", () => {
	it("preserves order, contains failures, and drains recursively enqueued work", async () => {
		const order: number[] = [];
		const failures = vi.fn(() => {
			throw new Error("diagnostic failed");
		});
		const queue = new PostSettlementQueue(failures);
		queue.enqueue(async () => {
			order.push(1);
			queue.enqueue(() => {
				order.push(3);
			});
			throw new Error("observer failed");
		});
		queue.enqueue(() => {
			order.push(2);
		});

		await queue.flush();
		expect(order).toEqual([1, 2, 3]);
		expect(failures).toHaveBeenCalledOnce();
		await queue.close();
		expect(queue.enqueue(() => {})).toBe(false);
	});
});

describe("BoundedEventQueue", () => {
	it("bounds stalled observers, preserves order, and resumes after failures", async () => {
		const { promise: blocked, resolve: release } = deferred();
		const delivered: number[] = [];
		const failures = vi.fn();
		const queue = new BoundedEventQueue<number>(4, async (event) => {
			delivered.push(event);
			if (event === 1) await blocked;
			if (event === 2) throw new Error("observer failed");
		}, failures);

		expect(queue.enqueue(1)).toBe(true);
		expect(queue.enqueue(2)).toBe(true);
		expect(queue.enqueue(3)).toBe(true);
		expect(queue.enqueue(4)).toBe(true);
		expect(queue.enqueue(5)).toBe(false);
		expect(queue.snapshot()).toMatchObject({ capacity: 4, pending: 4, dropped: 1 });

		release();
		await queue.flush();
		expect(delivered).toEqual([1, 2, 3, 4]);
		expect(failures).toHaveBeenCalledOnce();
		expect(queue.snapshot()).toMatchObject({ pending: 0, dropped: 1, oldestPendingMs: 0 });
		await queue.close();
		expect(queue.enqueue(6)).toBe(false);
	});
});
