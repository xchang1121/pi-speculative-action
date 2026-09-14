import { describe, expect, it } from "vitest";
import { CandidateExecution } from "../src/candidate-execution.ts";
import { cause } from "../src/settlement.ts";
import { TimelineInterval } from "../src/task-timing.ts";

describe("CandidateExecution", () => {
	it.each(["shared", "exclusive"] as const)("owns %s leases independently of immutable execution", async (kind) => {
		const candidate = new CandidateExecution<string>(kind), released = candidate.acquire("actor")!;
		expect(released).toMatchObject({ owner: "actor", kind, state: "active", active: true });
		const peer = candidate.acquire("peer");
		expect(Boolean(peer)).toBe(kind === "shared");
		const reservation = candidate.reservation;
		expect(reservation).toEqual(kind === "shared"
			? { kind, owners: ["actor", "peer"] } : { kind, status: "reserved", turnID: "actor" });
		expect(Object.isFrozen(reservation)).toBe(true);
		if (reservation.kind === "shared") expect(Object.isFrozen(reservation.owners)).toBe(true);
		expect(released.release()).toBe(true);
		const adopted = candidate.acquire("actor")!;
		expect(released.release()).toBe(false);
		expect(released.adopt()).toBe(false);
		expect(candidate.acquire("actor")).toBeUndefined();
		expect(adopted.adopt()).toBe(kind === "shared");
		expect(candidate.start(10)).toBe(true);
		if (kind === "shared") {
			expect(candidate.reservation).toEqual({ kind, owners: ["peer"] });
			expect(peer!.release()).toBe(true);
			expect(peer!.release()).toBe(false);
		} else {
			expect(candidate.acquire("peer")).toBeUndefined();
			expect(adopted.adopt()).toBe(false);
		}
		expect(candidate.execution).toEqual({ status: "running", startedAt: 10 });
		const toolExecution = new TimelineInterval(10, 25);
		expect(candidate.succeed("result", toolExecution, 15)).toBe(true);
		if (kind === "exclusive") expect(adopted.adopt()).toBe(true);
		expect(adopted).toMatchObject({ state: kind === "exclusive" ? "consumed" : "released", active: false });
		expect(adopted.release()).toBe(false);
		expect(adopted.adopt()).toBe(false);
		expect(candidate.reservation).toEqual(kind === "exclusive" ? { kind, status: "consumed" } : { kind, owners: [] });
		expect(candidate.execution).toEqual({
			status: "succeeded",
			output: "result",
			toolExecution,
			executionMs: 15,
		});
		expect(Object.isFrozen(candidate.execution)).toBe(true);
		expect(candidate.execution.status === "succeeded" && candidate.execution.toolExecution).toBe(toolExecution);
		expect(Reflect.set(toolExecution, "completedAt", 100)).toBe(false);
		expect(Object.isFrozen(candidate.reservation)).toBe(true);
		await expect(candidate.completion).resolves.toEqual(candidate.execution);
		const late = candidate.acquire("later");
		expect(Boolean(late)).toBe(kind === "shared");
		expect(late?.release()).toBe(kind === "shared" ? true : undefined);
	});

	it("settles failure and cancellation exactly once", async () => {
		const failed = new CandidateExecution<string>("shared");
		const failure = cause("execution", "tool_failed");
		expect(failed.fail(failure, 8, 0)).toBe(true);
		expect(failed.acquire("actor")).toBeUndefined();
		expect(failed.cancel(cause("control", "late_cancel"), 9, 0)).toBe(false);
		await expect(failed.completion).resolves.toMatchObject({ status: "failed", cause: failure });

		const cancelled = new CandidateExecution<string>("shared");
		const cancellation = cause("control", "turn_aborted");
		expect(cancelled.start(3)).toBe(true);
		expect(cancelled.cancel(cancellation, 7, 4)).toBe(true);
		expect(cancelled.controller.signal.aborted).toBe(true);
		expect(cancelled.acquire("actor")).toBeUndefined();
		expect(cancelled.succeed("late", new TimelineInterval(3, 9), 6)).toBe(false);
		await expect(cancelled.completion).resolves.toMatchObject({ status: "cancelled", cause: cancellation });
	});

});
