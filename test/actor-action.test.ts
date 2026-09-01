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
		const action = new ActorAction({ identity, tool: "read", actionKey });
		expect(action.reject("stale", exact, cause("freshness", "resource_changed"))).toBe(true);
		const settled = action.adopt(
			"fresh",
			exact,
			{
				executionAheadMs: 40,
				attemptLeadMs: 55,
				hitLatencyMs: 3,
			},
			{ startedAt: 10, completedAt: 50 },
			[{ id: "prediction", source: "pattern", proposalID: "plan", actionID: "next" }],
		);

		expect(settled).toMatchObject({
			actorAction: identity,
			matchedPredictions: [{ id: "prediction", source: "pattern" }],
			rejections: [{ candidateID: "stale", cause: { stage: "freshness" } }],
			provider: { kind: "speculative", candidateID: "fresh", match: exact },
		});
		expect(Object.isFrozen(settled?.provider)).toBe(true);
		expect(Object.isFrozen(settled?.matchedPredictions)).toBe(true);
		expect(Object.isFrozen(settled?.rejections[0]?.cause)).toBe(true);
		expect(
			action.adopt(
				"another",
				exact,
				{ executionAheadMs: 1, attemptLeadMs: 1, hitLatencyMs: 1 },
				{ startedAt: 1, completedAt: 2 },
			),
		).toBeUndefined();
		expect(action.settleActor(100, false)).toBeUndefined();
	});

	it("spans interception and exactly one Actor fallback completion", () => {
		const action = new ActorAction({ identity, tool: "bash" });
		action.reject("failed", exact, cause("execution", "tool_failed"));
		expect(action.deferToFallback()).toBe(true);
		expect(action.reject("late", exact, cause("execution", "late"))).toBe(false);
		expect(action.settleActor(Number.NaN, true)).toMatchObject({
			rejections: [{ candidateID: "failed" }],
			provider: { kind: "actor", durationMs: 0, isError: true },
		});
		expect(action.settleActor(1, false)).toBeUndefined();
	});

	it("settles isolation-blocked benefit with the same capped timing decomposition", () => {
		const partiallyAhead = new ActorAction({ identity, tool: "bash", actionKey });
		expect(partiallyAhead.deferToFallback([], 80)).toBe(true);
		expect(partiallyAhead.settleActor(120, false, 1_000)).toMatchObject({
			provider: {
				kind: "actor",
				durationMs: 120,
				executionBlockedTiming: { attemptLeadMs: 80, executionAheadMs: 80, hitLatencyMs: 40 },
			},
		});

		const fullyAhead = new ActorAction({ identity: { ...identity, id: "call-2" }, tool: "bash", actionKey });
		fullyAhead.deferToFallback([], 200);
		expect(fullyAhead.settleActor(120, false)).toMatchObject({
			provider: {
				executionBlockedTiming: { attemptLeadMs: 200, executionAheadMs: 120, hitLatencyMs: 0 },
			},
		});
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
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => {
			release = resolve;
		});
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
