import { expect, test, vi } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { DrafterTaskBudget } from "../src/drafter-budget.ts";
import { normalizeSpeculativeAgentSettings } from "../src/agent-integration.ts";
import { recoverAssistantCompletion, assistantCompletion } from "../src/model-request-adapter.ts";
import { ModelRequestScheduler } from "../src/model-request.ts";
import { runSourceRequest, SourceGeneration } from "../src/source-request.ts";
import { deferred } from "./async.ts";
import { testModel } from "./model.ts";

const policy = () => normalizeSpeculativeAgentSettings({ drafterMaxTokens: null, drafterTaskMaxRequests: null,
  drafterTaskMaxTokens: null, predictionTimeoutMs: null, modelRequestDrainTimeoutMs: null, selfSpeculation: { forkMaxTokens: null } });

test("explicit unlimited limits survive normalization and outlive the old task allowance", async () => {
  const settings = policy(), budget = new DrafterTaskBudget();
  expect(settings).toMatchObject({ drafterMaxTokens: null, drafterTaskMaxRequests: null,
    drafterTaskMaxTokens: null, predictionTimeoutMs: null, modelRequestDrainTimeoutMs: null, selfSpeculation: { forkMaxTokens: null } });
  await budget.withTask(async () => {
    budget.beginTurn("one");
    for (let index = 0; index < 70; index++) {
      if (index === 35) { budget.finishTask(); budget.beginTurn("recovery"); }
      await budget.run({ model: testModel("unknown", { maxTokens: 0, contextWindow: 0 }),
        context: { messages: [] }, inputTokens: 500000, policy: settings,
        complete: async (_model, _context, options) => { expect(options?.maxTokens).toBeUndefined(); return fauxAssistantMessage("done"); } });
    }
  });
  expect(budget.snapshot()).toMatchObject({ requests: 70, skippedRequests: 0, reservedTokens: 0, unreportedRequests: 0 });
});

test("uncapped failed output records unknown usage without Infinity or NaN accounting", async () => {
  const budget = new DrafterTaskBudget();
  await expect(budget.run({ model: testModel("unknown", { maxTokens: 0, contextWindow: 0 }), context: { messages: [] },
    inputTokens: 500000, policy: policy(), complete: async () => { throw new Error("external failure"); } })).rejects.toThrow("external failure");
  expect(budget.snapshot()).toMatchObject({ unreportedRequests: 1, unreportedTokens: 500000, reservedTokens: 0 });
  expect(Object.values(budget.snapshot()).every(Number.isFinite)).toBe(true);
  expect(budget.available(policy())).toBe(true);
});

test("length-limited text is incomplete and unlimited recovery stops at completion or caller cancellation", async () => {
  let latest = fauxAssistantMessage("unfinished visible text", { stopReason: "length" }), allowed = true;
  expect(assistantCompletion(latest)).toBe("length");
  expect(await recoverAssistantCompletion({ latest: () => latest, canContinue: () => allowed, maxAttempts: null,
    recover: async attempt => { if (attempt === 7) latest = fauxAssistantMessage("Finished."); } })).toBe(7);
  latest = fauxAssistantMessage([], { stopReason: "length" });
  expect(await recoverAssistantCompletion({ latest: () => latest, canContinue: () => allowed, maxAttempts: null,
    recover: async () => { allowed = false; } })).toBe(1);
});

test("unlimited prediction and teardown wait for completion beyond former deadlines", async () => {
  vi.useFakeTimers();
  const gate = deferred(), generation = new SourceGeneration(), scheduler = new ModelRequestScheduler();
  try {
    const request = runSourceRequest({ request: { turnID: "turn", source: "test", kind: "proposal", index: 0, targetDecisionSequence: 1 },
      generation, timeoutMs: policy().predictionTimeoutMs ?? undefined, produce: () => gate.promise, count: () => 1 });
    const remote = scheduler.run({ domain: "local", role: "drafter", context: { taskID: "task" },
      policy: { maxConcurrent: 2, actorReserve: 1 }, execute: () => gate.promise });
    await Promise.resolve();
    let drained = false;
    const drain = scheduler.drain("task").then(value => { drained = true; return value; });
    await vi.advanceTimersByTimeAsync(3_600_000);
    expect(drained).toBe(false); expect(generation.active).toBe(true);
    gate.resolve(); await remote;
    expect((await request).settlement.status).toBe("produced"); expect(await drain).toEqual([]);
  } finally { gate.resolve(); vi.useRealTimers(); }
});
