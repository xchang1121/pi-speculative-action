import { expect, test } from "vitest";
import { ModelRequestNotSent, ModelRequestScheduler, type ModelRequestEvent } from "../src/model-request.ts";
import { DrafterTaskBudget } from "../src/drafter-budget.ts";
import { normalizeDrafterRequestSettings } from "../src/common.ts";
import { assistantCompletion, recoverAssistantCompletion, scheduledModelStream, sdkRejectedToolAttempt } from "../src/model-request-adapter.ts";
import { createAssistantMessageEventStream, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { deferred } from "./async.ts";
import { testModel } from "./model.ts";

test("cancelled transports keep capacity, queued cancellations never send, and the Actor retains capacity", async () => {
	const scheduler = new ModelRequestScheduler(), events: ModelRequestEvent[] = [], gates = [deferred(), deferred(), deferred()];
	const controllers = gates.map(() => new AbortController()); let sent = 0;
	const options = { domain: "service", context: { taskID: "task" }, policy: { maxConcurrent: 3, actorReserve: 1 }, onEvent: (event: ModelRequestEvent) => events.push(event) };
	const drafts = gates.map((gate, index) => scheduler.run({ ...options, role: "drafter", signal: controllers[index]!.signal,
		execute: async request => { sent++; await gate.promise; expect(request.signal.aborted).toBe(false); return index; },
		settled: value => ({ remoteFinished: true, usage: { input: value + 1 } }) }));
	await expect.poll(() => sent).toBe(2);
	const queued = expect(drafts[2]).rejects.toBeInstanceOf(ModelRequestNotSent);
	controllers.forEach(controller => controller.abort()); await queued;
	expect(scheduler.snapshot()).toHaveLength(2);
	expect(events.filter(event => event.phase === "cancel-requested")).toHaveLength(2);
	await expect(scheduler.run({ ...options, role: "actor", execute: async () => "actor" })).resolves.toBe("actor");
	expect(sent).toBe(2);
	gates[0]!.resolve(); gates[1]!.resolve(); await Promise.all(drafts.slice(0, 2));
	expect(await scheduler.drain("task", 100)).toEqual([]);
	expect(events.filter(event => event.phase === "usage-settled")).toHaveLength(3);
});

test("unknown upstream completion stays quarantined until independent settlement", async () => {
	const scheduler = new ModelRequestScheduler(), events: ModelRequestEvent[] = [];
	const options = { domain: "same-service", context: { taskID: "task" }, policy: { maxConcurrent: 2, actorReserve: 1 }, onEvent: (event: ModelRequestEvent) => events.push(event) };
	await expect(scheduler.run({ ...options, role: "drafter", execute: async () => { throw new Error("connection lost after send"); } })).rejects.toThrow("connection lost");
	const unknown = scheduler.snapshot()[0]!; expect(unknown.phase).toBe("remote-unknown");
	let started = false;
	const next = scheduler.run({ ...options, role: "drafter", execute: async () => { started = true; } });
	await Promise.resolve(); expect(started).toBe(false);
	await scheduler.run({ ...options, role: "actor", execute: async () => "done" });
	expect(scheduler.settleRemote(unknown.id, { usage: { output: 12 } })).toBe(true);
	await next; expect(scheduler.snapshot()).toEqual([]);
});

test("stream cancellation returns locally while remote usage still settles", async () => {
	const scheduler = new ModelRequestScheduler(), remote = createAssistantMessageEventStream(), events: ModelRequestEvent[] = [];
	const controller = new AbortController(); let transportSignal: AbortSignal | undefined;
	const stream = scheduledModelStream((_model, _context, options) => { transportSignal = options?.signal; return remote; }, scheduler,
		{ role: "actor", context: () => ({ taskID: "stream" }), policy: () => ({ maxConcurrent: 2, actorReserve: 1 }), onEvent: event => events.push(event) });
	const local = await stream(testModel("actor"), { messages: [] }, { signal: controller.signal });
	await expect.poll(() => transportSignal !== undefined).toBe(true); controller.abort();
	expect((await local.result()).stopReason).toBe("aborted"); expect(transportSignal!.aborted).toBe(false);
	const message = fauxAssistantMessage("done"); remote.push({ type: "done", reason: "stop", message }); remote.end(message);
	await scheduler.drain("stream", 1000);
	expect(events.at(-1)?.phase).toBe("usage-settled"); expect(scheduler.snapshot()).toEqual([]);
});

test("a completed 429 response releases its slot without a hidden retry", async () => {
	const scheduler = new ModelRequestScheduler(); let calls = 0;
	const wrapped = scheduledModelStream(async (model, _context, options) => {
		calls++; expect(options?.maxRetries).toBe(0);
		await options?.onResponse?.({ status: 429, headers: { "x-request-id": "upstream-429" } }, model);
		const result = createAssistantMessageEventStream(), message = fauxAssistantMessage([], { stopReason: "error" });
		result.push({ type: "error", reason: "error", error: message }); result.end(message); return result;
	}, scheduler, { role: "drafter", context: () => ({ taskID: "limited" }), policy: () => ({ maxConcurrent: 2, actorReserve: 1 }) });
	await (await wrapped(testModel("same-model"), { messages: [] })).result();
	await scheduler.drain("limited", 1000); expect(scheduler.snapshot()).toEqual([]); expect(calls).toBe(1);
});

test("progress reservations protect later turns without exceeding task caps", async () => {
	const budget = new DrafterTaskBudget(), policy = normalizeDrafterRequestSettings({ drafterTaskMaxRequests: 4, drafterTaskMaxTokens: 4000,
		drafterBudgetInitialFraction: 0.25, drafterBudgetHorizonTurns: 4 });
	const run = () => budget.run({ model: testModel("draft"), context: { messages: [] }, options: { maxTokens: 10 }, policy,
		complete: async () => fauxAssistantMessage("ok") });
	budget.beginTurn("one"); await run(); expect(await run()).toBeUndefined();
	budget.beginTurn("four"); budget.beginTurn("three"); budget.beginTurn("two");
	await run(); await run(); await run(); expect(await run()).toBeUndefined(); expect(budget.snapshot().requests).toBe(4);
	budget.finishTask(); budget.beginTurn("new"); await run(); expect(budget.snapshot().requests).toBe(1);
});

test("a queued cancellation refunds budget, whereas uncertain sends retain their reservation", async () => {
	const budget = new DrafterTaskBudget(), policy = normalizeDrafterRequestSettings({ drafterBudgetInitialFraction: 1 });
	const run = (error: Error) => budget.run({ model: testModel("draft"), context: { messages: [] }, options: { maxTokens: 10 }, policy, complete: async () => { throw error; } });
	await expect(run(new ModelRequestNotSent())).rejects.toThrow(); expect(budget.snapshot()).toMatchObject({ requests: 0, unreportedTokens: 0, reservedTokens: 0 });
	await expect(run(new Error("unknown send"))).rejects.toThrow(); expect(budget.snapshot().unreportedTokens).toBeGreaterThan(0);
});

test("internal completion recovery shares request limits and progress until the host task ends", async () => {
	const budget = new DrafterTaskBudget(), policy = normalizeDrafterRequestSettings({ drafterTaskMaxRequests: 2,
		drafterTaskMaxTokens: 4000, drafterBudgetInitialFraction: 0.5, drafterBudgetHorizonTurns: 2 });
	let sent = 0;
	const run = () => budget.run({ model: testModel("draft"), context: { messages: [] }, options: { maxTokens: 10 }, policy,
		complete: async () => { sent++; return fauxAssistantMessage("ok"); } });
	await budget.withTask(async () => {
		budget.beginTurn("inspect"); await run(); expect(await run()).toBeUndefined();
		budget.finishTask(); budget.beginTurn("recover-one"); await run();
		await budget.withTask(async () => {
			budget.finishTask(); budget.beginTurn("recover-two"); expect(await run()).toBeUndefined();
		});
		budget.finishTask(); expect(await run()).toBeUndefined();
	});
	expect(sent).toBe(2); expect(budget.snapshot().requests).toBe(2);
	await budget.withTask(async () => { budget.beginTurn("next-user-task"); await run(); });
	expect(sent).toBe(3); expect(budget.snapshot().requests).toBe(1);
});

test("host task recovery retains uncertain token charges and releases its scope after failure", async () => {
	const budget = new DrafterTaskBudget(), policy = normalizeDrafterRequestSettings({ drafterTaskMaxRequests: 10,
		drafterTaskMaxTokens: 100, drafterBudgetInitialFraction: 1 });
	let sent = 0;
	const run = () => budget.run({ model: testModel("draft"), context: { messages: [] }, inputTokens: 40,
		options: { maxTokens: 40 }, policy, complete: async () => { sent++; throw new Error("uncertain upstream"); } });
	await expect(budget.withTask(async () => {
		budget.beginTurn("inspect"); await expect(run()).rejects.toThrow("uncertain upstream");
		budget.finishTask(); budget.beginTurn("recover"); expect(await run()).toBeUndefined();
		expect(budget.snapshot().unreportedTokens).toBe(80);
		throw new Error("host failed");
	})).rejects.toThrow("host failed");
	expect(sent).toBe(1);
	await budget.withTask(async () => { budget.beginTurn("next-user-task"); await expect(run()).rejects.toThrow("uncertain upstream"); });
	expect(sent).toBe(2); expect(budget.snapshot().unreportedTokens).toBe(80);
});

test("reasoning-only stop is incomplete even when the provider calls it successful", () => {
	expect(assistantCompletion(fauxAssistantMessage([{ type: "thinking", thinking: "private reasoning" }]))).toBe("reasoning-only");
	expect(assistantCompletion(fauxAssistantMessage("   "))).toBe("empty");
	expect(assistantCompletion(fauxAssistantMessage("No supported finding."))).toBe("visible");
});

test("completion recovery is bounded, respects the task budget, and never retries failed requests", async () => {
	const reasoning = fauxAssistantMessage([{ type: "thinking", thinking: "unfinished" }]);
	let latest = reasoning, allowed = true;
	const attempts: number[] = [];
	const options = { latest: () => latest, canContinue: () => allowed,
		recover: async (attempt: number) => { attempts.push(attempt); } };
	expect(await recoverAssistantCompletion(options)).toBe(2);
	expect(attempts).toEqual([1, 2]);
	allowed = false; expect(await recoverAssistantCompletion(options)).toBe(0);
	allowed = true; latest = fauxAssistantMessage([], { stopReason: "error" });
	expect(await recoverAssistantCompletion(options)).toBe(0);
	latest = reasoning;
	expect(await recoverAssistantCompletion({ ...options, recover: async () => { latest = fauxAssistantMessage("No supported finding."); } })).toBe(1);
	latest = reasoning;
	expect(await recoverAssistantCompletion({ ...options, recover: async () => { allowed = false; } })).toBe(1);
});

test("SDK pre-rejection is separate from a missing actual tool settlement", () => {
	const error = [{ type: "text", text: "Tool read not found" }];
	expect(sdkRejectedToolAttempt("read", ["write"], true, error)).toBe(true);
	expect(sdkRejectedToolAttempt("read", ["read", "write"], true, error)).toBe(false);
	expect(sdkRejectedToolAttempt("read", ["write"], false, error)).toBe(false);
	expect(sdkRejectedToolAttempt("read", ["write"], true, [{ type: "text", text: "permission denied" }])).toBe(false);
});
