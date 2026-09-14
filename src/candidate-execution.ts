import { nonNegativeFinite as metric } from "./number-utils.ts";
import type { ResolutionCause } from "./settlement.ts";
import { TimelineInterval } from "./task-timing.ts";

export type CandidateExecutionState<Output> =
	| { readonly status: "queued" }
	| { readonly status: "running"; readonly startedAt: number }
	| {
			readonly status: "succeeded";
			readonly output: Output;
			readonly toolExecution: TimelineInterval;
			readonly executionMs: number;
	  }
	| {
			readonly status: "failed" | "cancelled";
			readonly cause: ResolutionCause;
			readonly startedAt?: number;
			readonly completedAt: number;
			readonly executionMs: number;
	  };

export type CandidateReservation =
	| { readonly kind: "shared"; readonly owners: readonly string[] }
	| {
			readonly kind: "exclusive";
			readonly status: "available" | "reserved" | "consumed";
			readonly turnID?: string;
	  };

export type CandidateExecutionSettlement<Output> = Extract<
	CandidateExecutionState<Output>,
	{ readonly status: "succeeded" | "failed" | "cancelled" }
>;

type CandidateReservationLeaseState = "active" | "released" | "consumed";

/** One acquired reservation. Every exit can safely call `release`; adoption makes it a no-op. */
export interface CandidateReservationLease {
	readonly owner: string;
	readonly kind: CandidateReservation["kind"];
	readonly state: CandidateReservationLeaseState;
	readonly active: boolean;
	release(): boolean;
	adopt(): boolean;
}

/** Owns execution and reservation as independent, monotonic facts. */
export class CandidateExecution<Output> {
	readonly controller: AbortController;
	readonly completion: Promise<CandidateExecutionSettlement<Output>>;
	private executionValue: CandidateExecutionState<Output> = Object.freeze({ status: "queued" });
	private reservationValue: CandidateReservation;
	private settleCompletion!: (settlement: CandidateExecutionSettlement<Output>) => void;

	constructor(reuse: "shared" | "exclusive", controller = new AbortController()) {
		this.controller = controller;
		this.reservationValue =
			reuse === "shared"
				? Object.freeze({ kind: "shared", owners: Object.freeze([]) })
				: Object.freeze({ kind: "exclusive", status: "available" });
		this.completion = new Promise((resolve) => {
			this.settleCompletion = resolve;
		});
	}

	get execution(): CandidateExecutionState<Output> {
		return this.executionValue;
	}

	get reservation(): CandidateReservation {
		return this.reservationValue;
	}

	start(startedAt: number): boolean {
		if (this.executionValue.status !== "queued") return false;
		this.executionValue = Object.freeze({ status: "running", startedAt: metric(startedAt) });
		return true;
	}

	succeed(output: Output, toolExecution: TimelineInterval, executionMs: number): boolean {
		if (this.executionValue.status !== "running") return false;
		const settlement: CandidateExecutionSettlement<Output> = Object.freeze({
			status: "succeeded",
			output,
			toolExecution: TimelineInterval.from(toolExecution),
			executionMs: metric(executionMs),
		});
		this.executionValue = settlement;
		this.settleCompletion(settlement);
		return true;
	}

	fail(cause: ResolutionCause, completedAt: number, executionMs: number): boolean {
		return this.finish("failed", cause, completedAt, executionMs);
	}

	cancel(cause: ResolutionCause, completedAt: number, executionMs: number): boolean {
		const changed = this.finish("cancelled", cause, completedAt, executionMs);
		if (changed) this.controller.abort(cause);
		return changed;
	}

	acquire(owner: string): CandidateReservationLease | undefined {
		if (this.executionValue.status === "failed" || this.executionValue.status === "cancelled") return undefined;
		const reservation = this.reservationValue;
		if (reservation.kind === "shared") {
			if (reservation.owners.includes(owner)) return undefined;
			this.reservationValue = Object.freeze({
				kind: "shared",
				owners: Object.freeze([...reservation.owners, owner]),
			});
		} else {
			if (reservation.status !== "available") return undefined;
			this.reservationValue = Object.freeze({ kind: "exclusive", status: "reserved", turnID: owner });
		}
		const kind = reservation.kind;
		let state: CandidateReservationLeaseState = "active";
		const settle = (adopt: boolean) => {
			if (state !== "active") return false;
			const consumed = adopt && kind === "exclusive";
			if (consumed && this.executionValue.status !== "succeeded") return false;
			const current = this.reservationValue;
			this.reservationValue = Object.freeze(current.kind === "shared"
				? { kind: "shared", owners: Object.freeze(current.owners.filter((value) => value !== owner)) }
				: { kind: "exclusive", status: consumed ? "consumed" : "available" });
			state = consumed ? "consumed" : "released";
			return true;
		};
		return {
			owner,
			kind,
			get state() { return state; },
			get active() { return state === "active"; },
			release: () => settle(false),
			adopt: () => settle(true),
		};
	}

	private finish(
		status: "failed" | "cancelled",
		cause: ResolutionCause,
		completedAt: number,
		executionMs: number,
	): boolean {
		if (this.executionValue.status !== "queued" && this.executionValue.status !== "running") return false;
		const startedAt = this.executionValue.status === "running" ? this.executionValue.startedAt : undefined;
		const settlement: CandidateExecutionSettlement<Output> = Object.freeze({
			status,
			cause: Object.freeze({ ...cause }),
			...(startedAt !== undefined ? { startedAt } : {}),
			completedAt: metric(completedAt),
			executionMs: metric(executionMs),
		});
		this.executionValue = settlement;
		this.settleCompletion(settlement);
		return true;
	}
}
