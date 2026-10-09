import { createReadTool } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import { DrafterTaskBudget, drafterInputTokens, drafterOpportunityKey } from "../src/drafter-budget.ts";
import { SourceRequestSuppressed } from "../src/source-request.ts";
import type { PlanProposal, PlanUpdate } from "../src/plan-proposal.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { deferred } from "./async.ts";
import { testModel } from "./model.ts";

function first(value: PlanProposal | PlanUpdate | readonly (PlanProposal | PlanUpdate)[] | undefined) {
	if (!value || Array.isArray(value)) return undefined;
	return ("actions" in value ? value.actions : "upsert" in value ? value.upsert : undefined)?.[0];
}

const readInput = { path: "notes.txt" };
const readReply = () => fauxAssistantMessage([{ type: "toolCall", id: "read", name: "read", arguments: readInput }], { stopReason: "toolUse" });
function sourceRequest(turnID: string, sourceConfig: Record<string, unknown> = {}, prepareExecution?: () => void, schemaHashes = {}) {
	const tool = createReadTool("/workspace");
	return { startInput: { sessionID: "session", turnID, actorModel: testModel(), actorOptions: undefined, context: { messages: [], tools: [tool] }, tools: [tool] },
		data: { tools: new Map([["read", tool]]), schemaHashes, prepareExecution }, definitions: [], candidateNames: ["read"],
		settings: { enabled: true, resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000, tools: ["read"], sourceConfig },
		proposalIndex: 0, proposalCount: 2, signal: new AbortController().signal };
}
// The runtime maps deliberate suppression to an empty proposal; unrelated failures remain visible here.
async function draftValue<T>(value: T | Promise<T>): Promise<T | undefined> {
	try { return await value; } catch (error) { if (!(error instanceof SourceRequestSuppressed)) throw error; }
}
function continuation(request: ReturnType<typeof sourceRequest>, root: NonNullable<ReturnType<typeof first>>) {
	return { ...request, proposalID: `drafter:${request.startInput.turnID}:0`, actionID: root.id, feedback: root.feedback, revision: 1,
		trigger: "execution_succeeded" as const, output: { result: { content: [], details: {} }, isError: false },
		candidate: { id: "root", key: PI_ACTION_SEMANTICS.buildKey("read", readInput, "/workspace")!, tool: "read", input: readInput } };
}

describe("Drafter request and token budgets", () => {
	it.each(["throw", "aborted", "error"] as const)("reserves concurrent tokens, caps the last output and retains missing usage after %s", async failure => {
		const budget = new DrafterTaskBudget(), context = { systemPrompt: "prompt", messages: [] }, model = testModel();
		const prompt = drafterInputTokens(context), policy = { drafterTaskMaxRequests: 8, drafterTaskMaxTokens: 100 + prompt * 2 };
		const pending = deferred<ReturnType<typeof fauxAssistantMessage>>(), limits: number[] = [];
		const request = { model, context, policy, options: { maxTokens: 80 }, complete: async (_model: unknown, _context: unknown, options: { maxTokens?: number } = {}) => {
			limits.push(options.maxTokens!); return pending.promise;
		} };
		const first = budget.run(request), second = budget.run(request);
		expect(limits).toEqual([80, 20]);
		expect(await budget.run(request)).toBeUndefined();
		if (failure === "throw") pending.reject(new Error("provider lost its response"));
		else pending.resolve(fauxAssistantMessage([], { stopReason: failure }));
		await Promise.allSettled([first, second]);
		expect(budget.snapshot()).toEqual({ requests: 2, reportedTokens: 0, unreportedTokens: policy.drafterTaskMaxTokens,
			reservedTokens: 0, skippedRequests: 1 });
		expect(await budget.run(request)).toBeUndefined();
	});

	it("accounts actual input, output and cache usage without charging a late completion to the next task", async () => {
		const budget = new DrafterTaskBudget(), pending = deferred<ReturnType<typeof fauxAssistantMessage>>();
		const message = fauxAssistantMessage([]);
		message.usage = { ...message.usage, input: 10, output: 5, cacheRead: 20, cacheWrite: 15, totalTokens: 50 };
		const request = { model: testModel(), context: { messages: [] }, policy: { drafterTaskMaxRequests: 1, drafterTaskMaxTokens: 100 },
			options: { maxTokens: 80 }, complete: async () => pending.promise };
		const previous = budget.run(request);
		expect(await budget.run(request)).toBeUndefined();
		budget.finishTask();
		await budget.run({ ...request, complete: async () => message });
		pending.resolve(message); await previous;
		expect(budget.snapshot()).toMatchObject({ requests: 1, reportedTokens: 50, unreportedTokens: 0, reservedTokens: 0 });
	});

	it.each(["width", "depth", "probe", "prompt"] as const)("shares task request and prompt budgets with %s and resets only after task completion", async dimension => {
		const budget = new DrafterTaskBudget(); let calls = 0, preparations = 0;
		const complete = async () => { calls++; return readReply(); };
		const policy = { drafterTaskMaxRequests: 2, drafterTaskMaxTokens: dimension === "prompt" ? 1 : 10000 };
		const controller = createDrafterPlanSource({ sessionID: "session", complete, drafterBudget: budget });
		const request = sourceRequest("one", policy, () => { preparations++; });
		try {
			if (dimension === "prompt") {
				await expect(controller.source.propose({ ...request, proposalCount: 1 }))
					.rejects.toMatchObject({ cause: { stage: "source", code: "drafter_token_limit", detail: expect.stringContaining('"remainingTokens":1') } });
				expect([preparations, calls]).toEqual([0, 0]);
				return;
			}
			const root = first(await controller.source.propose(request))!;
			const nextStep = continuation(request, root);
			if (dimension === "depth") {
				if (typeof controller.source.continueOn === "function") expect(controller.source.continueOn(nextStep)).toBe(true);
				expect(first(await controller.source.continue!(nextStep))).toBeDefined();
			} else if (dimension === "width") expect(first(await controller.source.propose({ ...request, proposalIndex: 1 }))).toBeDefined();
			else expect(await budget.run({ model: testModel(), context: { messages: [] }, policy, complete })).toBeDefined();
			expect(await draftValue(controller.source.propose({ ...request, proposalIndex: 1 }))).toBeUndefined();
			controller.finishTurn("session", "one");
			const next = { ...request, startInput: { ...request.startInput, turnID: "two" } };
			expect(await draftValue(controller.source.propose(next))).toBeUndefined(); expect(calls).toBe(2); expect(preparations).toBe(1);
			controller.finishSession(); expect(first(await controller.source.propose(next))).toBeDefined(); expect(calls).toBe(3);
		} finally { controller.finishSession(); }
	});

	it("keys probability calibration by tool context while ignoring argument churn", () => {
		const model = testModel(), context = { messages: [readReply()] }, key = drafterOpportunityKey(model, context);
		const changed = fauxAssistantMessage(["different.txt", "another.txt"].map(path => ({ type: "toolCall", id: path, name: "read", arguments: { path } })));
		expect(drafterOpportunityKey(model, { messages: [changed] })).toBe(key);
		expect(drafterOpportunityKey(model, context, { read: "changed-schema" })).not.toBe(key);
		expect(drafterOpportunityKey(model, context, undefined, [{ tool: "read", horizon: 2 }])).not.toBe(key);
	});

	it.each(["width", "depth"] as const)("keeps unused %s available within explicit budgets without latency history", async dimension => {
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async () => readReply() });
		try {
			for (let round = 0; round < 8; round++) {
				const request = sourceRequest(`turn-${round}`, { drafterMaxDepth: 1, drafterTaskMaxRequests: 16, drafterTaskMaxTokens: 100000 });
				const root = first(await controller.source.propose(request))!;
				expect(root).toMatchObject({ empiricalProbability: 0.5, adoptionProbability: 0.5 });
				const next = continuation(request, root);
				if (dimension === "depth" && typeof controller.source.continueOn === "function") expect(controller.source.continueOn(next)).toBe(true);
				expect(first(await (dimension === "width" ? controller.source.propose({ ...request, proposalIndex: 1 }) : controller.source.continue!(next)))).toBeDefined();
				controller.finishTurn("session", request.startInput.turnID);
			}
			expect(controller.snapshot().budget.requests).toBe(16);
		} finally { controller.finishSession(); }
	});
});
