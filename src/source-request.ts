import { nonNegativeCount as finiteCount } from "./number-utils.ts";
import { nonNegativeNumber } from "./setting-input.ts";
import { errorDetail } from "./error-utils.ts";
import type { ResolutionCause, SettledSourceRequest, SourceRequestIdentity, SourceRequestSettlement } from "./settlement.ts";
import { cause } from "./settlement.ts";
import { waitForCandidate } from "./scheduler.ts";

/** A deliberate source admission decision; it produced no proposal and is not a provider failure. */
export class SourceRequestSuppressed extends Error {
	readonly cause: ResolutionCause & { readonly stage: "source" };
	constructor(cause: SourceRequestSuppressed["cause"]) { super(cause.detail ?? cause.code); this.cause = Object.freeze({ ...cause }); }
}

/** Turn-scoped authority token. Expiration prevents late producer results from entering admission. */
export class SourceGeneration {
	private readonly controller = new AbortController();
	readonly signal: AbortSignal = this.controller.signal;

	constructor(parent?: AbortSignal) {
		if (!parent) return;
		const abort = () => this.expire(cause("control", "turn_aborted"));
		if (parent.aborted) abort();
		else parent.addEventListener("abort", abort, { once: true, signal: this.signal });
	}

	get active(): boolean {
		return !this.signal.aborted;
	}

	get expiration(): ResolutionCause | undefined {
		return this.signal.reason as ResolutionCause | undefined;
	}

	expire(expiration: ResolutionCause): void {
		if (this.signal.aborted) return;
		this.controller.abort(Object.freeze({ ...expiration }));
	}
}

/** Producer timing/cancellation; the caller retains physical production until close, separately from admission. */
export async function runSourceRequest<Value>(input: {
	readonly request: SourceRequestIdentity;
	readonly generation: SourceGeneration;
	readonly timeoutMs?: number;
	readonly produce: (signal: AbortSignal) => Value | Promise<Value>;
	readonly count: (value: Value) => number;
}): Promise<SettledSourceRequest & { readonly value?: Value }> {
	const startedAt = performance.now();
	const finish = (settlement: SourceRequestSettlement): SettledSourceRequest => Object.freeze({
		request: Object.freeze({ ...input.request }), durationMs: Math.max(0, performance.now() - startedAt), settlement: Object.freeze(settlement),
	});
	const aborted = () => finish({ status: "aborted", cause: cause("source",
		input.generation.expiration?.code ?? "generation_expired", input.generation.expiration?.detail) });
	const failed = (code: string, error: unknown) => finish({ status: "error", cause: cause("source", code, errorDetail(error)) });
	if (!input.generation.active) return aborted();

	const controller = new AbortController();
	// Produced proposals can leave preparation in flight until their generation closes.
	const signal = AbortSignal.any([input.generation.signal, controller.signal]);
	const producer = Promise.resolve()
		.then(() => { signal.throwIfAborted(); return input.produce(signal); })
		.then((value) => ({ kind: "produced" as const, value }), (error) => ({ kind: "error" as const, error }));

	const waited = await waitForCandidate(producer, input.generation.signal, nonNegativeNumber(input.timeoutMs, undefined));
	if (waited.status === "deadline") {
		const expiration = cause("source", "timeout");
		controller.abort(expiration);
		return finish({ status: "timeout", cause: expiration });
	}
	if (waited.status === "aborted" || !input.generation.active) return aborted();
	const outcome = waited.value;
	if (outcome.kind === "error") return outcome.error instanceof SourceRequestSuppressed
		? finish({ status: "empty", cause: outcome.error.cause }) : failed("producer_error", outcome.error);
	let proposalCount: number;
	try {
		proposalCount = finiteCount(input.count(outcome.value));
	} catch (error) {
		return failed("result_error", error);
	}
	return { ...finish(proposalCount > 0 ? { status: "produced", proposalCount } : { status: "empty" }), value: outcome.value };
}
