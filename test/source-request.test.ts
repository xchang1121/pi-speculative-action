import { deferred } from "./async.ts";
import { describe, expect, it, vi } from "vitest";
import { cause } from "../src/settlement.ts";
import { runSourceRequest, SourceGeneration, SourceRequestSuppressed } from "../src/source-request.ts";
import { emptySpeculativeTraceSummary, summarizeSpeculativeTrace } from "../src/trace-summary.ts";

const request = { source: "source", turnID: "turn", index: 0, kind: "proposal", targetDecisionSequence: 1 } as const;

describe("source request ownership", () => {
	it("classifies independent outcomes while keeping producer work within its generation", async () => {
		for (const [value, count, status, code] of [
			[["a"], 1, "produced", undefined], [[], 0, "empty", undefined], [["a"], NaN, "empty", undefined],
			[new SourceRequestSuppressed(cause("source", "task_budget")), 0, "empty", "task_budget"],
			[0, 0, "error", "producer_error"], [["a"], new Error("malformed"), "error", "result_error"],
		] as const) {
			const generation = new SourceGeneration(); let producerSignal: AbortSignal | undefined;
			const settled = await runSourceRequest({ request, generation, timeoutMs: 0,
				produce: (signal) => { producerSignal = signal; if (value === 0 || value instanceof SourceRequestSuppressed) throw value; return value; },
				count: () => { if (count instanceof Error) throw count; return count; },
			});
			expect(settled.settlement).toMatchObject({ status, ...(code ? { cause: { stage: "source", code } } : {}) });
			expect(Object.isFrozen(settled.request) && Object.isFrozen(settled.settlement)).toBe(true);
			expect(summarizeSpeculativeTrace([{ type: "source_request", sessionID: "session", turnID: request.turnID, timestamp: 0,
				cache: emptySpeculativeTraceSummary().cache, request: settled, totalDraftTokens: 0 }]).sourceOutcomes).toEqual({ [status]: 1 });
			expect(producerSignal?.aborted).toBe(false);
			generation.expire(cause("control", "turn_finished"));
			expect(producerSignal?.aborted).toBe(true);
			expect(producerSignal?.reason).toEqual(generation.expiration);
		}
	});

	it("owns expiration and deadlines without admitting late success or leaking late rejection", async () => {
		vi.useFakeTimers();
		try {
			for (const mode of ["expired", "queued", "abort", "timeout"] as const) for (const late of
				(mode === "expired" || mode === "queued" ? ["resolve"] : ["resolve", "reject"])) {
				const parent = new AbortController(), generation = new SourceGeneration(parent.signal);
				const { promise: producer, resolve: release, reject } = deferred<string[]>();
				const { promise: entered, resolve: enter } = deferred();
				const count = vi.fn((value: string[]) => value.length); let signal: AbortSignal | undefined;
				if (mode === "expired") generation.expire(cause("control", "turn_finished"));
				const pending = runSourceRequest({ request, generation, timeoutMs: 10, count,
					produce: (input) => { signal = input; enter(); return producer; },
				});
				if (mode === "queued") parent.abort();
				else if (mode !== "expired") {
					await entered;
					if (mode === "abort") parent.abort(); else await vi.advanceTimersByTimeAsync(10);
				}
				const settled = await pending;
				if (late === "reject" && signal) reject(new Error("late failure")); else release(["late"]);
				await vi.runAllTimersAsync();
				expect(settled.settlement).toMatchObject({ status: mode === "timeout" ? "timeout" : "aborted",
					cause: { stage: "source", code: { expired: "turn_finished", queued: "turn_aborted", abort: "turn_aborted", timeout: "timeout" }[mode] },
				});
				expect(signal?.aborted).toBe(mode === "expired" || mode === "queued" ? undefined : true);
				expect(settled.value).toBeUndefined();
				expect(count).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
			}
		} finally { vi.useRealTimers(); }
	});
});
