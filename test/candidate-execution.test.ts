import { describe, expect, it } from "vitest";
import { CandidateExecution } from "../src/candidate-execution.ts";
import { cause } from "../src/settlement.ts";

describe("CandidateExecution", () => {
	it("owns reservation cleanup through an idempotent lease", () => {
		const candidate = new CandidateExecution<string>("exclusive");
		candidate.start(1);
		candidate.succeed("ok", 2, 1);
		const lease = candidate.acquire("actor");
		expect(lease).toMatchObject({ owner: "actor", kind: "exclusive", state: "active", active: true });
		expect(lease?.adopt()).toBe(true);
		expect(lease).toMatchObject({ state: "consumed", active: false });
		expect(lease?.release()).toBe(false);
		expect(candidate.reservation).toEqual({ kind: "exclusive", status: "consumed" });
	});

	it("releases shared leases without consuming the reusable result", () => {
		const candidate = new CandidateExecution<string>("shared");
		const lease = candidate.acquire("actor");
		expect(lease?.adopt()).toBe(true);
		expect(lease?.state).toBe("released");
		expect(candidate.reservation).toEqual({ kind: "shared", owners: [] });
	});

	it("keeps execution success immutable when an exclusive result is consumed", async () => {
		const candidate = new CandidateExecution<string>("exclusive");

		expect(candidate.start(10)).toBe(true);
		expect(candidate.reserve("turn-a")).toBe(true);
		expect(candidate.succeed("result", 25, 15)).toBe(true);
		expect(candidate.consume("turn-a")).toBe(true);

		expect(candidate.execution).toEqual({
			status: "succeeded",
			output: "result",
			startedAt: 10,
			completedAt: 25,
			executionMs: 15,
		});
		expect(candidate.reservation).toEqual({ kind: "exclusive", status: "consumed" });
		expect(Object.isFrozen(candidate.execution)).toBe(true);
		expect(Object.isFrozen(candidate.reservation)).toBe(true);
		await expect(candidate.completion).resolves.toEqual(candidate.execution);
	});

	it("serializes reservations without changing execution state", () => {
		const candidate = new CandidateExecution<string>("exclusive");
		candidate.start(1);

		expect(candidate.reserve("turn-a")).toBe(true);
		expect(candidate.reserve("turn-b")).toBe(false);
		expect(candidate.release("turn-b")).toBe(false);
		expect(candidate.release("turn-a")).toBe(true);
		expect(candidate.reserve("turn-b")).toBe(true);
		expect(candidate.execution).toEqual({ status: "running", startedAt: 1 });
	});

	it("settles failure and cancellation exactly once", async () => {
		const failed = new CandidateExecution<string>("shared");
		const failure = cause("execution", "tool_failed");
		expect(failed.fail(failure, 8, 0)).toBe(true);
		expect(failed.cancel(cause("control", "late_cancel"), 9, 0)).toBe(false);
		await expect(failed.completion).resolves.toMatchObject({ status: "failed", cause: failure });

		const cancelled = new CandidateExecution<string>("shared");
		const cancellation = cause("control", "turn_aborted");
		expect(cancelled.start(3)).toBe(true);
		expect(cancelled.cancel(cancellation, 7, 4)).toBe(true);
		expect(cancelled.controller.signal.aborted).toBe(true);
		expect(cancelled.succeed("late", 9, 6)).toBe(false);
		await expect(cancelled.completion).resolves.toMatchObject({ status: "cancelled", cause: cancellation });
	});

	it("tracks every shared Actor join so scheduled work cannot be preempted underneath it", () => {
		const candidate = new CandidateExecution<string>("shared");
		candidate.start(0);
		candidate.succeed("shared", 1, 1);

		expect(candidate.reserve("turn-a")).toBe(true);
		expect(candidate.reserve("turn-b")).toBe(true);
		expect(candidate.reserve("turn-a")).toBe(false);
		expect(candidate.reservation).toEqual({ kind: "shared", owners: ["turn-a", "turn-b"] });
		const reservation = candidate.reservation;
		expect(reservation.kind === "shared" && Object.isFrozen(reservation.owners)).toBe(true);
		expect(candidate.release("turn-a")).toBe(true);
		expect(candidate.reservation).toEqual({ kind: "shared", owners: ["turn-b"] });
		expect(candidate.release("turn-a")).toBe(false);
		expect(candidate.release("turn-b")).toBe(true);
		expect(candidate.consume("turn-a")).toBe(false);
		expect(candidate.execution.status).toBe("succeeded");
	});
});
