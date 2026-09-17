import { nonNegativeCount as finiteCount } from "./number-utils.ts";
import { nonNegativeNumber } from "./setting-input.ts";
import { errorDetail } from "./error-utils.ts";
import type {
	ResolutionCause,
	SettledSourceRequest,
	SourceRequestIdentity,
	SourceRequestSettlement,
} from "./settlement.ts";
import { cause } from "./settlement.ts";
import { waitForCandidate } from "./scheduler.ts";

export interface SourceRequestResult<Value> extends SettledSourceRequest {
	readonly value?: Value;
}

/** Turn-scoped authority token. Expiration prevents late producer results from entering admission. */
export class SourceGeneration {
	readonly signal: AbortSignal;
	private readonly controller = new AbortController();
	private expiredCause?: ResolutionCause;
	private detachParent?: () => void;

	constructor(parent?: AbortSignal) {
		this.signal = this.controller.signal;
		if (!parent) return;
		const abort = () => this.expire(cause("control", "turn_aborted"));
		if (parent.aborted) abort();
		else {
			parent.addEventListener("abort", abort, { once: true });
			this.detachParent = () => parent.removeEventListener("abort", abort);
		}
	}

	get active(): boolean {
		return !this.expiredCause;
	}

	get expiration(): ResolutionCause | undefined {
		return this.expiredCause;
	}

	expire(expiration: ResolutionCause): boolean {
		if (this.expiredCause) return false;
		this.expiredCause = Object.freeze({ ...expiration });
		this.detachParent?.();
		this.detachParent = undefined;
		this.controller.abort(this.expiredCause);
		return true;
	}
}

/** Producer timing/cancellation; the caller retains physical production until close, separately from admission. */
export async function runSourceRequest<Value>(input: {
	readonly request: SourceRequestIdentity;
	readonly generation: SourceGeneration;
	readonly timeoutMs?: number;
	readonly produce: (signal: AbortSignal) => Value | Promise<Value>;
	readonly count: (value: Value) => number;
}): Promise<SourceRequestResult<Value>> {
	const startedAt = performance.now();
	const finish = (settlement: SourceRequestSettlement) => result(input.request, startedAt, settlement);
	const aborted = () => finish({ status: "aborted", cause: cause("source",
		input.generation.expiration?.code ?? "generation_expired", input.generation.expiration?.detail) });
	const failed = (code: string, error: unknown) => finish({ status: "error", cause: cause("source", code, errorDetail(error)) });
	if (!input.generation.active) return aborted();

	const controller = new AbortController();
	// Produced proposals can leave preparation in flight until their generation closes.
	const signal = AbortSignal.any([input.generation.signal, controller.signal]);
	const producer = Promise.resolve()
		.then(() => {
			signal.throwIfAborted();
			return input.produce(signal);
		})
		.then(
			(value) => ({ kind: "produced" as const, value }),
			(error) => ({ kind: "error" as const, error }),
		);

	const waited = await waitForCandidate(producer, input.generation.signal, nonNegativeNumber(input.timeoutMs, undefined));
	if (waited.status === "deadline") {
		const expiration = cause("source", "timeout");
		controller.abort(expiration);
		return finish({ status: "timeout", cause: expiration });
	}
	if (waited.status === "aborted" || !input.generation.active) return aborted();
	const outcome = waited.value;
	if (outcome.kind === "error") return failed("producer_error", outcome.error);
	let proposalCount: number;
	try {
		proposalCount = finiteCount(input.count(outcome.value));
	} catch (error) {
		return failed("result_error", error);
	}
	return {
		...finish(proposalCount > 0 ? { status: "produced", proposalCount } : { status: "empty" }),
		value: outcome.value,
	};
}

function result(
	request: SourceRequestIdentity,
	startedAt: number,
	settlement: SourceRequestSettlement,
): SettledSourceRequest {
	return Object.freeze({
		request: Object.freeze({ ...request }),
		startedAt,
		durationMs: Math.max(0, performance.now() - startedAt),
		settlement: Object.freeze(settlement),
	});
}
