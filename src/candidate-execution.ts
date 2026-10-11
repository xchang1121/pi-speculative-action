import { nonNegativeFinite as metric } from "./number-utils.ts";
import type { ResolutionCause } from "./settlement.ts";
import { TimelineInterval } from "./task-timing.ts";

export type CandidateExecutionState<Output> =
	| { readonly status: "queued" }
	| { readonly status: "running"; readonly startedAt: number }
	| { readonly status: "succeeded"; readonly output: Output; readonly toolExecution: TimelineInterval; readonly executionMs: number; }
	| {
			readonly status: "failed" | "cancelled";
			readonly cause: ResolutionCause;
			readonly executionMs: number;
	  };

export type CandidateReservation =
	| { readonly kind: "shared"; readonly owners: readonly string[] }
	| { readonly kind: "exclusive"; readonly status: "available" | "reserved" | "consumed"; readonly turnID?: string; };

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
	private readonly released?: () => void;
	private executionValue: CandidateExecutionState<Output> = Object.freeze({ status: "queued" });
	private reservationValue: CandidateReservation;
	private settleCompletion!: (settlement: CandidateExecutionSettlement<Output>) => void;

	constructor(reuse: "shared" | "exclusive", controller = new AbortController(), released?: () => void) {
		this.controller = controller;
		this.released = released;
		this.reservationValue =
			reuse === "shared"
				? Object.freeze({ kind: "shared", owners: Object.freeze([]) })
				: Object.freeze({ kind: "exclusive", status: "available" });
		this.completion = new Promise(resolve => { this.settleCompletion = resolve; });
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
		return this.finish({ status: "succeeded", output, toolExecution, executionMs });
	}

	fail(cause: ResolutionCause, executionMs: number): boolean {
		return this.finish({ status: "failed", cause, executionMs });
	}

	cancel(cause: ResolutionCause, executionMs: number): boolean {
		const changed = this.finish({ status: "cancelled", cause, executionMs });
		if (changed) this.controller.abort(cause);
		return changed;
	}

	acquire(owner: string): CandidateReservationLease | undefined {
		if (this.executionValue.status === "failed" || this.executionValue.status === "cancelled") return undefined;
		const reservation = this.reservationValue;
		if (reservation.kind === "shared") {
			if (reservation.owners.includes(owner)) return undefined;
			this.reservationValue = Object.freeze({ kind: "shared", owners: Object.freeze([...reservation.owners, owner]) });
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
			if (kind === "exclusive") this.released?.();
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

	private finish(settlement: CandidateExecutionSettlement<Output>): boolean {
		if (this.executionValue.status !== "running" && (this.executionValue.status !== "queued" || settlement.status === "succeeded")) return false;
		const owned = settlement.status === "succeeded" ? { ...settlement, toolExecution: TimelineInterval.from(settlement.toolExecution) }
			: { ...settlement, cause: Object.freeze({ ...settlement.cause }) };
		this.executionValue = Object.freeze({ ...owned, executionMs: metric(owned.executionMs) });
		this.settleCompletion(this.executionValue);
		return true;
	}
}
