import { deferred } from "./async.ts";
import { describe, expect, it, vi } from "vitest";
import { ActorAction } from "../src/actor-action.ts";
import { BoundedEventQueue, PostSettlementQueue } from "../src/post-settlement.ts";
import { cause } from "../src/settlement.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { emptySpeculativeTraceSummary, summarizeSpeculativeTrace } from "../src/trace-summary.ts";

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
			const action = new ActorAction<{ readonly id: string }, string>({
				identity, tool: "read", actionKey, fallback: cause("matching", "no_candidate"),
			});
			const selection = { candidate: { id: "fresh" }, match: exact, output: "value",
				timing: { executionAheadMs: 40, attemptLeadMs: 55, hitLatencyMs: 3 }, toolExecution: { startedAt: 10, completedAt: 50 } };
			expect(action.rejectCandidate("stale", exact, cause("freshness", "resource_changed"))).toBe(true);
			expect(action.select({ ...selection, candidate: { id: "stale" } })).toBe(false);
			expect(action.select(selection)).toBe(true);
			action.deferToFallback();
			expect(action.state.status).toBe("selected");
			expect(action.rejectCandidate("fresh", exact, cause("execution", "late"))).toBe(false);
			expect(action.setFallback(cause("control", "late"))).toBe(false);
			expect(action.deferToFallback()).toBeUndefined();
			expect(action.settleActor(selection.toolExecution, false)).toBeUndefined();
			expect(action.settleSelection([{ id: "prediction", source: "pattern", proposalID: "plan", actionID: "next" }], provider))
				.toEqual(provider === "preview" ? { status: "rejected", candidateID: "fresh", cause: cause("control", "actor_preview_provider") }
					: { status: "adopted", candidateID: "fresh" });
			const settled = action.settlement;
			expect(summarizeSpeculativeTrace([{ type: "actor_action", settlement: settled!, actualAction: "read README.md",
				sessionID: "session", turnID: identity.turnID, timestamp: 0, cache: emptySpeculativeTraceSummary().cache,
			}])).toMatchObject(provider === "speculative"
				? { executionAheadMs: 40, attemptLeadMs: 55, hitLatencyMs: 3, actorExecutionMs: 0 }
				: { executionAheadMs: 0, attemptLeadMs: 0, hitLatencyMs: 0, actorExecutionMs: 40, actorPreviews: 1 });
			expect(settled).toMatchObject({ actorAction: identity, matchedPredictions: [{ id: "prediction", source: "pattern" }],
				rejections: [{ candidateID: "stale", cause: { stage: "freshness" } }], provider: { candidateID: "fresh",
					...(provider === "preview" ? { kind: "actor", origin: "preview", durationMs: 40 } : { kind: "speculative", match: exact }) } });
			for (const value of [settled?.provider, settled?.matchedPredictions, settled?.rejections[0]?.cause]) {
				expect(Object.isFrozen(value)).toBe(true);
			}
			expect(action.select(selection)).toBe(false);
			expect(action.settleSelection([], provider)).toBeUndefined();
			expect(action.settleActor(selection.toolExecution, false)).toBeUndefined();
		}
	});

	it("spans interception and exactly one Actor fallback completion", () => {
		for (const mode of ["empty", "rejected", "interrupted"]) {
			const execution = new TimelineInterval(0, 0);
			const action = new ActorAction({ identity, tool: "bash", fallback: cause("matching", "no_candidate") });
			if (mode !== "empty") {
				const failure = cause("execution", "tool_failed");
				expect(mode === "rejected" ? action.rejectCandidate("failed", exact, failure) : action.setFallback(failure, "failed")).toBe(true);
				expect(action.fallback).toEqual({ cause: failure, candidateID: "failed" });
				expect(action.deferToFallback()).toEqual({ status: "rejected", cause: failure, candidateID: "failed" });
			}
			action.deferToFallback();
			expect(action.state.status).toBe("awaiting_fallback");
			expect(action.rejectCandidate("late", exact, cause("execution", "late"))).toBe(false);
			expect(action.settleActor(execution, true)).toMatchObject({
				rejections: mode === "rejected" ? [{ candidateID: "failed" }] : [],
				provider: { kind: "actor", durationMs: 0, isError: true },
			});
			expect(action.settleActor(execution, false)).toBeUndefined();
		}
	});

	it("reports an isolation-blocked fallback without inventing completed computation", () => {
		const execution = new TimelineInterval(100, 220), timeline = new TaskTimeline(0);
		const action = new ActorAction({ identity, tool: "bash", actionKey,
			fallback: cause("execution", "isolation_unavailable") });
		expect(action.deferToFallback()?.status).toBe("rejected");
		expect(action.settleActor(execution, false)).toMatchObject({ provider: { kind: "actor", durationMs: 120 } });
		expect(action.settlement?.provider.toolExecution).toBe(execution);
		timeline.recordActor(0, 100); timeline.recordActor(220, 500);
		timeline.recordTool(action.settlement!.provider.toolExecution);
		expect(timeline.measure(500)).toMatchObject({ serializedMs: 500, toolExecutionMs: 120, hiddenLatencyMs: 0 });
	});
});

describe("ordered delivery", () => {
	it.each(["settlement", "observer"])("drains %s delivery through failures, reentrancy and concurrent flushes", async kind => {
		const { promise: blocked, resolve: release } = deferred();
		const delivered: number[] = [];
		const failures = vi.fn(() => { enqueue(6); throw new Error("diagnostic failed"); });
		const deliver = async (event: number) => {
			delivered.push(event);
			if (event === 1) await blocked;
			if (event === 2) throw new Error("observer failed");
			if (event === 3) enqueue(7);
		};
		const queue = kind === "settlement" ? new PostSettlementQueue(failures) : new BoundedEventQueue(4, deliver, failures);
		const enqueue = (event: number) => queue instanceof PostSettlementQueue
			? queue.enqueue(() => deliver(event)) : queue.enqueue(event);

		for (const event of [1, 2, 3, 4]) expect(enqueue(event)).toBe(true);
		expect(delivered).toEqual(kind === "settlement" ? [] : [1]);
		expect(enqueue(5)).toBe(kind === "settlement");
		if (queue instanceof BoundedEventQueue) expect(queue.snapshot()).toMatchObject({ capacity: 4, pending: 4, dropped: 1 });

		const flushing = [queue.flush(), queue.flush()];
		release(); await Promise.all(flushing);
		const expected = [1, 2, 3, 4, ...(kind === "settlement" ? [5] : []), 6, 7];
		expect(delivered).toEqual(expected);
		expect(failures).toHaveBeenCalledOnce();
		if (queue instanceof BoundedEventQueue) expect(queue.snapshot()).toMatchObject({ pending: 0, dropped: 1, oldestPendingMs: 0 });
		expect(enqueue(8)).toBe(true); await queue.flush();
		expect(delivered).toEqual([...expected, 8]);
		await queue.close();
		expect(enqueue(9)).toBe(false);
	});
});
