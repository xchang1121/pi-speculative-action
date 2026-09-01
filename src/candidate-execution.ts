import type { ResolutionCause } from "./settlement.ts";

export type CandidateExecutionState<Output> =
	| { readonly status: "queued" }
	| { readonly status: "running"; readonly startedAt: number }
	| {
			readonly status: "succeeded";
			readonly output: Output;
			readonly startedAt: number;
			readonly completedAt: number;
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

class CandidateReservationLeaseHandle implements CandidateReservationLease {
	readonly owner: string;
	readonly kind: CandidateReservation["kind"];
	private stateValue: CandidateReservationLeaseState = "active";
	private readonly releaseReservation: () => boolean;
	private readonly adoptReservation: () => boolean;

	constructor(
		owner: string,
		kind: CandidateReservation["kind"],
		releaseReservation: () => boolean,
		adoptReservation: () => boolean,
	) {
		this.owner = owner;
		this.kind = kind;
		this.releaseReservation = releaseReservation;
		this.adoptReservation = adoptReservation;
	}

	get state(): CandidateReservationLeaseState {
		return this.stateValue;
	}

	get active(): boolean {
		return this.stateValue === "active";
	}

	release(): boolean {
		if (!this.active || !this.releaseReservation()) return false;
		this.stateValue = "released";
		return true;
	}

	adopt(): boolean {
		if (!this.active || !this.adoptReservation()) return false;
		this.stateValue = this.kind === "exclusive" ? "consumed" : "released";
		return true;
	}
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

	succeed(output: Output, completedAt: number, executionMs: number): boolean {
		if (this.executionValue.status !== "running") return false;
		const settlement: CandidateExecutionSettlement<Output> = Object.freeze({
			status: "succeeded",
			output,
			startedAt: this.executionValue.startedAt,
			completedAt: metric(completedAt),
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

	reserve(turnID: string): boolean {
		if (this.executionValue.status === "failed" || this.executionValue.status === "cancelled") return false;
		if (this.reservationValue.kind === "shared") {
			if (this.reservationValue.owners.includes(turnID)) return false;
			this.reservationValue = Object.freeze({
				kind: "shared",
				owners: Object.freeze([...this.reservationValue.owners, turnID]),
			});
			return true;
		}
		if (this.reservationValue.status !== "available") return false;
		this.reservationValue = Object.freeze({ kind: "exclusive", status: "reserved", turnID });
		return true;
	}

	acquire(owner: string): CandidateReservationLease | undefined {
		if (!this.reserve(owner)) return undefined;
		const kind = this.reservationValue.kind;
		return new CandidateReservationLeaseHandle(
			owner,
			kind,
			() => this.release(owner),
			() => (kind === "exclusive" ? this.consume(owner) : this.release(owner)),
		);
	}

	release(turnID: string): boolean {
		if (this.reservationValue.kind === "shared") {
			if (!this.reservationValue.owners.includes(turnID)) return false;
			this.reservationValue = Object.freeze({
				kind: "shared",
				owners: Object.freeze(this.reservationValue.owners.filter((owner) => owner !== turnID)),
			});
			return true;
		}
		if (
			this.reservationValue.kind !== "exclusive" ||
			this.reservationValue.status !== "reserved" ||
			this.reservationValue.turnID !== turnID
		) {
			return false;
		}
		this.reservationValue = Object.freeze({ kind: "exclusive", status: "available" });
		return true;
	}

	consume(turnID: string): boolean {
		if (
			this.executionValue.status !== "succeeded" ||
			this.reservationValue.kind !== "exclusive" ||
			this.reservationValue.status !== "reserved" ||
			this.reservationValue.turnID !== turnID
		) {
			return false;
		}
		this.reservationValue = Object.freeze({ kind: "exclusive", status: "consumed" });
		return true;
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

function metric(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}
