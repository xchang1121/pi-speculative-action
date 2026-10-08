import { createReadTool } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import { DrafterTaskBudget, drafterInputTokens, drafterOpportunityKey, type DrafterUtilityBatch } from "../src/drafter-budget.ts";
import { SourceRequestSuppressed } from "../src/source-request.ts";
import type { PlanProposal, PlanUpdate } from "../src/plan-proposal.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { deferred, nextTurn } from "./async.ts";
import { testModel } from "./model.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import type { ActorActionProvider, SourceRequestKind, SourceRequestSettlement } from "../src/settlement.ts";

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

describe("Drafter marginal request utility", () => {
	it.each(["throw", "aborted", "error"] as const)("reserves concurrent tokens, caps the last output and retains missing usage after %s", async failure => {
		const budget = new DrafterTaskBudget(), context = { systemPrompt: "prompt", messages: [] }, model = testModel();
		const prompt = drafterInputTokens(context), policy = { drafterTaskMaxRequests: 8, drafterTaskMaxTokens: 100 + prompt * 2 };
		const pending = deferred<ReturnType<typeof fauxAssistantMessage>>(), limits: number[] = [];
		const utility = budget.start("request", false);
		const request = { model, context, policy, utility, ancestors: [utility], options: { maxTokens: 80 }, complete: async (_model: unknown, _context: unknown, options: { maxTokens?: number } = {}) => {
			limits.push(options.maxTokens!); return pending.promise;
		} };
		const first = budget.run(request), second = budget.run(request);
		expect(limits).toEqual([80, 20]);
		expect(await budget.run(request)).toBeUndefined();
		if (failure === "throw") pending.reject(new Error("provider lost its response"));
		else pending.resolve(fauxAssistantMessage([], { stopReason: failure }));
		await Promise.allSettled([first, second]);
		expect(budget.snapshot()).toEqual({ requests: 2, reportedTokens: 0, unreportedTokens: policy.drafterTaskMaxTokens,
			reservedTokens: 0, explorationTokens: 0, futureOpportunityUsed: false, skippedRequests: 1 });
		expect(utility.chargedTokens).toBe(policy.drafterTaskMaxTokens);
		expect(await budget.run(request)).toBeUndefined();
	});

	it.each([5, 5000, undefined])("reserves a future request unless observed marginal saved-ms/token justifies it (%s)", async benefitMs => {
		const budget = new DrafterTaskBudget(), context = { messages: [] }, charge = 100 + drafterInputTokens(context);
		const policy = { drafterTaskMaxRequests: 32, drafterTaskMaxTokens: 12 * charge };
		const message = fauxAssistantMessage([]); message.usage = { ...message.usage, input: charge, totalTokens: charge };
		const request = { model: testModel(), context, policy, options: { maxTokens: 100 }, complete: async () => message };
		const utility = (key: string, benefitMs: number | undefined) => Object.assign(budget.start(key, true, key === "depth"), { benefitMs, finished: true });
		const base = utility("base", 500);
		await budget.run({ ...request, utility: base });
		const marginal = utility("depth", benefitMs);
		await budget.run({ ...request, utility: marginal, ancestors: [base], marginal: true });
		expect([base.chargedTokens, marginal.chargedTokens]).toEqual([2 * charge, charge]);
		for (let index = 0; index < 9; index++) await budget.run(request);
		const offered = { ...request, utility: utility("depth", 0), ancestors: [base], marginal: true };
		expect(Boolean(await budget.run(offered))).toBe(benefitMs === 5000);
		if (benefitMs !== 5000) {
			expect(budget.snapshot().reportedTokens).toBe(11 * charge);
			marginal.benefitMs = 5000; // Late adoption amends the old charged request, rather than adding a sample.
			budget.finish(marginal);
			expect(await budget.run(offered)).toBeDefined();
		}
		expect(budget.snapshot()).toMatchObject({ requests: 12, reportedTokens: 12 * charge, reservedTokens: 0 });
		expect(await budget.run(request)).toBeUndefined();
	});

	it("shortens exploration before large prompts exhaust the task, retaining bounded recovery probes", async () => {
		const budget = new DrafterTaskBudget(), first = budget.start("model", true), context = { messages: [] };
		const request = { model: testModel(), context, policy: { drafterTaskMaxRequests: 3, drafterTaskMaxTokens: 10000 },
			options: { maxTokens: 80 }, complete: async () => fauxAssistantMessage([]) };
		await budget.run({ ...request, utility: first }); budget.finish(first);
		const probes = [];
		for (let index = 0; index < 4; index++) probes.push(Boolean(await budget.run({ ...request, utility: budget.start("model", true) })));
		expect(probes).toEqual([false, false, false, true]);
		budget.credit([first], { reusedExecutionMs: 5000, costMs: 1 });
		expect(await budget.run({ ...request, utility: budget.start("model", true) })).toBeDefined();
	});

	it("applies saved-ms per full reservation to roots and retains a final fresh opportunity after exploration", async () => {
		const budget = new DrafterTaskBudget(), context = { messages: [] }, charge = 100 + drafterInputTokens(context), skipped: string[] = [];
		const message = fauxAssistantMessage([]); message.usage = { ...message.usage, input: charge, totalTokens: charge };
		const request = { model: testModel(), context, options: { maxTokens: 100 }, policy: { drafterTaskMaxRequests: 32, drafterTaskMaxTokens: 12 * charge },
			complete: async () => message, onSkipped: (reason: string) => { skipped.push(reason); } };
		for (const [key, benefitMs] of [["valuable", 5000], ["cheap", 50]] as const) {
			const utility = budget.start(key, true); expect(await budget.run({ ...request, utility })).toBeDefined();
			utility.benefitMs = benefitMs; budget.finish(utility);
		}
		expect(await budget.run({ ...request, utility: budget.start("cheap", true) })).toBeUndefined();
		expect(skipped).toEqual(["drafter_negative_utility"]);
		const larger = budget.start("cheap", true); larger.expectedBenefitMs = 5000;
		expect(await budget.run({ ...request, inputTokens: 200, utility: larger })).toBeUndefined();
		expect(await budget.run({ ...request, utility: budget.start("cheap", true) })).toBeUndefined();
		const recovery = budget.start("cheap", true); recovery.benefitMs = 50;
		expect(await budget.run({ ...request, utility: recovery })).toBeDefined(); budget.finish(recovery);
		for (let index = 0; index < 8; index++) await budget.run(request);
		expect(await budget.run({ ...request, utility: budget.start("new-workflow-phase", true, false, true) })).toBeDefined();
		expect(budget.snapshot()).toMatchObject({ requests: 12, reportedTokens: charge * 12, futureOpportunityUsed: true });
	});

	it("keeps endpoint failures across phase changes, while opportunity keys ignore argument churn", async () => {
		const budget = new DrafterTaskBudget(), model = testModel(), context = { messages: [readReply()] }, key = drafterOpportunityKey(model, context);
		const changed = fauxAssistantMessage(["different.txt", "another.txt"].map(path => ({ type: "toolCall", id: path, name: "read", arguments: { path } })));
		expect(drafterOpportunityKey(model, { messages: [changed] })).toBe(key);
		expect(drafterOpportunityKey(model, context, { read: "changed-schema" })).not.toBe(key);
		expect(drafterOpportunityKey(model, context, undefined, [{ tool: "read", horizon: 2 }])).not.toBe(key);
		const request = { model, context, options: { maxTokens: 100 }, policy: { drafterTaskMaxRequests: 16, drafterTaskMaxTokens: 100000 },
			complete: async () => fauxAssistantMessage([], { stopReason: "error" }) };
		const cancellation = new AbortController(), pending = deferred<ReturnType<typeof fauxAssistantMessage>>();
		const cancelled = budget.run({ ...request, utility: budget.start("cancelled", true), options: { ...request.options, signal: cancellation.signal }, complete: () => pending.promise });
		const failing = budget.start("failing", true); failing.expectedBenefitMs = 5000;
		for (let index = 0; index < 2; index++) await budget.run({ ...request, utility: failing });
		budget.finish(failing);
		cancellation.abort(); pending.resolve(fauxAssistantMessage([], { stopReason: "aborted" })); await cancelled;
		const utility = budget.start("third", true); utility.expectedBenefitMs = 5000;
		let reason: string | undefined;
		expect(await budget.run({ ...request, utility, onSkipped: value => { reason = value; } })).toBeUndefined();
		expect(reason).toBe("drafter_failure_circuit");
		for (let index = 0; index < 2; index++) expect(await budget.run({ ...request, utility: budget.start("failing", true) })).toBeUndefined();
		const recovery = budget.start("failing", true), success = { ...request, complete: async () => fauxAssistantMessage([]) };
		expect(await budget.run({ ...success, utility: recovery })).toBeDefined(); // The fourth endpoint opportunity performs a real retry.
		budget.credit([recovery], { reusedExecutionMs: 500 }); budget.finish(recovery);
		expect(await budget.run({ ...success, utility: budget.start("failing", true) })).toBeDefined();
	});

	it("keeps cancelled usage charged and reserves one costly phase despite changing tools and hints", async () => {
		const budget = new DrafterTaskBudget(), pending = deferred<ReturnType<typeof fauxAssistantMessage>>(), cancel = new AbortController();
		const request = { model: testModel("large", { contextWindow: 131072 }), context: { messages: [] }, options: { maxTokens: 1000 },
			policy: { drafterTaskMaxRequests: 24, drafterTaskMaxTokens: 131072 }, complete: async () => fauxAssistantMessage([]) };
		const root = budget.start("initial", true), causes: string[] = [], diagnostics: Record<string, unknown>[] = [];
		const actorContext = (tools: readonly string[]) => ({ messages: [fauxAssistantMessage(tools.map(tool => ({ type: "toolCall" as const, id: tool, name: tool, arguments: {} })))] });
		const opportunity = (tools: readonly string[], horizon?: number) => drafterOpportunityKey(request.model, actorContext(tools), undefined,
			horizon === undefined ? [] : [{ tool: "read", horizon }]);
		const first = budget.run({ ...request, inputTokens: 2000, utility: root, options: { ...request.options, signal: cancel.signal }, complete: () => pending.promise });
		expect(await budget.run({ ...request, inputTokens: 2000, utility: budget.start("width", true, true), ancestors: [root], onSkipped: reason => causes.push(reason) })).toBeUndefined();
		const actualOutput = fauxAssistantMessage([]); actualOutput.usage = { ...actualOutput.usage, input: 3000, totalTokens: 3000 };
		expect(await budget.run({ ...request, inputTokens: 2000, utility: budget.start("peer-depth", true, true), ancestors: [root],
			afterExecution: true, complete: async () => actualOutput })).toBeDefined();
		budget.finish(root); cancel.abort(); pending.resolve(fauxAssistantMessage([], { stopReason: "aborted" })); await first;
		for (const [tools, horizon, prompt, abort, cold, allowed] of [[["bash", "find"], 0, 4000, true, false, true], [["read"], 1, 8000, false, false, true],
			[["read"], 1, 27000, false, false, false], [["grep"], 0, 27000, false, false, false], [["read"], 2, 27000, false, false, false],
			[["read"], undefined, 27000, false, false, false], [["unknown"], 0, 27000, false, false, false], [["edit"], 0, 55000, false, false, false],
			[["bash"], 0, 55000, false, true, true], [["write"], 0, 55000, false, true, false]] as const) {
			const utility = budget.start(opportunity(tools, horizon), true, false, cold), cancellation = new AbortController();
			const run = () => budget.run({ ...request, context: actorContext(tools), inputTokens: prompt, utility, options: { ...request.options, signal: cancellation.signal },
				onSkipped: (reason, detail) => { causes.push(reason); if (detail) diagnostics.push(JSON.parse(detail)); }, complete: async () => {
					if (abort) { cancellation.abort(); return fauxAssistantMessage([], { stopReason: "aborted" }); }
					const message = fauxAssistantMessage([]); message.usage = { ...message.usage, input: prompt, output: 1000, totalTokens: prompt + 1000 }; return message;
				} });
			if (prompt === 27000 && horizon === 1) for (let probe = 0; probe < 4; probe++) expect(await run()).toBeUndefined();
			else expect(Boolean(await run())).toBe(allowed);
			budget.finish(utility);
		}
		expect(causes).toContain("drafter_parent_unmeasured"); expect(causes).toContain("drafter_exploration_limit");
		expect(diagnostics).toEqual(expect.arrayContaining([expect.objectContaining({ promptTokens: 27000, reservationTokens: 28000,
			explorationCap: 32768, opportunityFresh: true, coldOpportunity: false, futureOpportunityUsed: false }),
			expect.objectContaining({ opportunity: opportunity(["read"]), opportunityFresh: true, coldOpportunity: false }),
			expect.objectContaining({ opportunity: opportunity(["edit"], 0), opportunityFresh: true, coldOpportunity: false, futureOpportunityUsed: false }),
			expect.objectContaining({ opportunity: opportunity(["write"], 0), coldOpportunity: true, futureOpportunityUsed: true })]));
		const profitable = budget.start("profitable", true); profitable.expectedBenefitMs = 5000;
		expect(await budget.run({ ...request, inputTokens: 2000, utility: profitable, complete: async () => actualOutput })).toBeDefined();
		expect(budget.snapshot()).toMatchObject({ requests: 6, reportedTokens: 71000, unreportedTokens: 8000,
			reservedTokens: 0, explorationTokens: 76000, futureOpportunityUsed: true });
	});

	it("keeps the sole costly opportunity when an overlapping continuation pushes cheap roots over exploration", async () => {
		const budget = new DrafterTaskBudget(), pending = deferred<ReturnType<typeof fauxAssistantMessage>>(), model = testModel("large", { contextWindow: 131072, maxTokens: 4096 });
		const reply = (tokens: number) => { const message = fauxAssistantMessage([]); message.usage = { ...message.usage, input: tokens, totalTokens: tokens }; return message; };
		const request = { model, context: { messages: [] }, options: { maxTokens: 4096 }, policy: { drafterTaskMaxRequests: 24, drafterTaskMaxTokens: 131072 }, complete: async () => reply(14049) };
		const root = budget.start("early", true); await budget.run({ ...request, inputTokens: 9953, utility: root });
		const continuation = budget.run({ ...request, inputTokens: 5300, utility: budget.start("continuation", true, true), ancestors: [root], afterExecution: true, complete: () => pending.promise });
		try {
			for (const tool of ["read", "grep", "edit", "unknown"]) {
				const context = { messages: [fauxAssistantMessage([{ type: "toolCall", id: tool, name: tool, arguments: {} }])] };
				expect(await budget.run({ ...request, context, inputTokens: 5677, utility: budget.start(drafterOpportunityKey(model, context), true),
					onSkipped: (_reason, detail) => expect(JSON.parse(detail!)).toMatchObject({ explorationTokens: 23445, reservationTokens: 9773, coldOpportunity: false, futureOpportunityUsed: false }) })).toBeUndefined();
			}
			expect(budget.snapshot()).toMatchObject({ requests: 2, reservedTokens: 9396, explorationTokens: 23445, futureOpportunityUsed: false });
			pending.resolve(reply(6068)); await continuation;
			expect(await budget.run({ ...request, inputTokens: 35548, utility: budget.start("costly", true, false, true), complete: async () => reply(39644) })).toBeDefined();
			expect(budget.snapshot()).toMatchObject({ requests: 3, reportedTokens: 59761, reservedTokens: 0, explorationTokens: 59761, futureOpportunityUsed: true });
		} finally { pending.resolve(reply(6068)); await continuation; }
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

	it("spends one cold phase after native service exceeds completed root latency, without forecasting savings", async () => {
		let horizon = 0, turnID = "seed"; const limits: number[] = [];
		const controller = createDrafterPlanSource({ sessionID: "session", patternHints: async () => [{ tool: "read", input: readInput, horizon }],
			complete: async (_model, _context, options) => { limits.push(options!.maxTokens!); const message = readReply(); message.usage = { ...message.usage, input: 600, totalTokens: 600 }; return message; } });
		const offer = (next: string, prompt = 4000, tool = "bash", maxRequests = 24) => {
			turnID = next; const base = sourceRequest(next, { drafterMaxDepth: 0, drafterMaxTokens: 100, drafterTaskMaxTokens: 10000, drafterTaskMaxRequests: maxRequests });
			const previous = fauxAssistantMessage([{ type: "toolCall", id: next, name: tool, arguments: {} }]);
			previous.usage = { ...previous.usage, input: prompt, totalTokens: prompt };
			return controller.source.propose({ ...base, startInput: { ...base.startInput, context: { ...base.startInput.context, messages: [previous] } } });
		};
		const requestDone = (durationMs: number, kind: SourceRequestKind = "proposal", settlement: SourceRequestSettlement = { status: "produced", proposalCount: 1 }, owner = turnID) =>
			controller.source.onRequestSettled!({ request: { source: "drafter", turnID: owner, index: 0, kind, targetDecisionSequence: 1 }, durationMs, settlement });
		const fallback = (durationMs: number, isError = false): ActorActionProvider => ({ kind: "actor", origin: "fallback", durationMs, isError,
			cause: { stage: "admission", code: "native_fallback" }, toolExecution: new TimelineInterval(0, Number.isFinite(durationMs) ? durationMs : 1) });
		const observe = (provider: ActorActionProvider, owner = turnID) => controller.actorActionSettled({ sessionID: "session", turnID: owner, settlement: {
			actorAction: { id: owner, sequence: 1, turnID: owner }, tool: "bash", matchedPredictions: [], rejections: [], provider } });
		const utility = (value: Awaited<ReturnType<typeof offer>>) => (first(value)!.feedback as { utility: { key: string; coldOpportunity: boolean; expectedBenefitMs?: number } }).utility;
		try {
			const seed = await offer("seed", 500); expect(utility(seed).coldOpportunity).toBe(false);
			requestDone(800); requestDone(500); requestDone(10000, "continuation"); requestDone(Infinity);
			for (const status of ["empty", "error", "timeout", "aborted"] as const) requestDone(10000, "proposal", { status, cause: { stage: "source", code: "excluded" } });
			controller.finishTurn("session", "seed");
			const preview: ActorActionProvider = { kind: "actor", origin: "preview", candidateID: "preview", durationMs: 10000, isError: false, toolExecution: new TimelineInterval(0, 10000) };
			const reused: ActorActionProvider = { kind: "speculative", candidateID: "hit", match: { kind: "exact", distance: 0 }, timing: { hitLatencyMs: 0, expectedActorMs: 10000 }, toolExecution: new TimelineInterval(0, 10000) };
			for (const [index, provider] of [fallback(100), fallback(825), preview, reused, fallback(10000, true), fallback(Infinity)].entries()) {
				await expect(offer(`cheap-${index}`, 4000, ["read", "grep", "edit", "write", "unknown", "bash"][index])).rejects.toMatchObject({ cause: { detail: expect.stringContaining('"coldOpportunity":false') } });
				observe(provider); controller.finishTurn("session", turnID);
			}
			await expect(offer("expensive")).rejects.toMatchObject({ cause: { code: "drafter_negative_utility" } });
			observe(fallback(826)); controller.finishTurn("session", turnID);
			const admitted = await offer("after-expensive"); expect(utility(admitted).coldOpportunity).toBe(true); expect(utility(admitted).expectedBenefitMs).toBeUndefined();
			expect(JSON.parse(utility(admitted).key)).toEqual((JSON.parse(utility(seed).key) as unknown[]).map((value, index) => index === 4 ? "costly_root" : value));
			expect(controller.snapshot().budget).toMatchObject({ requests: 2, reportedTokens: 1200, explorationTokens: 1200, futureOpportunityUsed: true });
			expect(limits).toEqual([100, 100]); controller.finishTurn("session", turnID); horizon = 2;
			await expect(offer("later-edit", 5000, "edit")).rejects.toMatchObject({ cause: { code: "drafter_exploration_limit", detail: expect.stringContaining('"futureOpportunityUsed":true') } });
			await expect(offer("hard-limit", 5000, "edit", 2)).rejects.toMatchObject({ cause: { code: "drafter_request_limit" } });
			controller.finishSession(); requestDone(1, "proposal", undefined, "seed"); observe(fallback(10000), "expensive");
			expect(utility(await offer("next-task", 500)).coldOpportunity).toBe(false); observe(fallback(10000)); controller.finishTurn("session", turnID); horizon = 3;
			await expect(offer("no-current-root-measurement")).rejects.toMatchObject({ cause: { code: "drafter_exploration_limit", detail: expect.stringContaining('"coldOpportunity":false') } });
		} finally { controller.finishSession(); }
	});

	it.each(["width", "depth", "probe", "prompt"] as const)("shares task request and prompt budgets with %s and resets only after task completion", async dimension => {
		const budget = new DrafterTaskBudget(); let calls = 0, preparations = 0;
		const complete = async () => { calls++; return readReply(); };
		const policy = { drafterTaskMaxRequests: 2, drafterTaskMaxTokens: dimension === "prompt" ? 1 : 10000 };
		const controller = createDrafterPlanSource({ sessionID: "session", complete, drafterBudget: budget });
		const request = sourceRequest("one", { ...policy, drafterGateEnabled: dimension === "prompt" }, () => { preparations++; });
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

	it.each(["width", "depth"] as const)("contracts unproductive %s, probes and recovers from late adoption", async dimension => {
		const calls: string[] = [];
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async (_model, context) => {
			calls.push(String(context.messages.length));
			return readReply();
		} });
		const extras: boolean[] = []; let recoveredAt = 0;
		try {
			for (let round = 1; round <= 10; round++) {
				const base = sourceRequest(`turn-${round}`, { drafterMaxDepth: 1 }, undefined, { read: "schema" });
				const request = { ...base, settings: { ...base.settings, candidateLimit: 2 } };
				const root = first(await controller.source.propose(request)); expect(root).toBeDefined();
				const nextStep = continuation(request, root!);
				if (dimension === "depth" && typeof controller.source.continueOn === "function") expect(controller.source.continueOn(nextStep)).toBe(true);
				const extra = first(await draftValue(dimension === "width" ? controller.source.propose({ ...request, proposalIndex: 1 }) : controller.source.continue!(nextStep)));
				extras.push(Boolean(extra));
				const credit = async (action: NonNullable<typeof root>) => controller.actorActionSettled({ sessionID: "session", turnID: request.startInput.turnID,
					candidate: { source: "drafter" } as never, candidateFeedback: action.feedback,
					reusedComputations: [{ source: "drafter", feedback: action.reuseFeedback, reusedExecutionMs: 500 }], settlement: {
						actorAction: { id: `hit-${round}`, sequence: round * 2 + Number(action !== root), turnID: request.startInput.turnID },
						provider: { kind: "speculative", timing: { hitLatencyMs: 0 } }, matchedPredictions: [{ source: "drafter" }] } as never });
				await credit(root!); // The first request remains useful even while its expansion is wasted.
				controller.finishTurn("session", request.startInput.turnID); await nextTurn();
				if (recoveredAt) { expect(extra).toBeDefined(); await credit(extra!); }
				else if (extra && extras.includes(false)) { await credit(extra); recoveredAt = round; } // Amend the closed recovery probe.
			}
			expect(extras[0]).toBe(true); expect(extras).toContain(false); expect(recoveredAt).toBeGreaterThan(1);
			expect(calls).toHaveLength(10 + extras.filter(Boolean).length);
		} finally { controller.finishSession(); }
	});

	it("credits measured physical work to each shared ancestor once, including later partial fallback reuse", async () => {
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async () => readReply() });
		const request = sourceRequest("producer", { drafterMaxDepth: 2, drafterGateEnabled: false });
		const root = first(await controller.source.propose(request))!;
		const expand = async (action: typeof root, revision: number) => {
			const next = { ...continuation(request, action), revision };
			if (typeof controller.source.continueOn !== "function") throw new Error("Missing Drafter continuation");
			expect(controller.source.continueOn(next)).toBe(true);
			return first(await controller.source.continue!(next))!;
		};
		try {
			const child = await expand(root, 1), descendant = await expand(child, 2);
			const lineage = (action: typeof root) => action.feedback as { utility: DrafterUtilityBatch; marginalUtilities?: readonly DrafterUtilityBatch[] };
			const ancestors = [lineage(root).utility, ...lineage(descendant).marginalUtilities!];
			expect(ancestors).toHaveLength(3);
			for (const action of [root, child, descendant]) {
				const token = action.reuseFeedback as { kind: string; utilities: readonly DrafterUtilityBatch[] };
				expect(Object.keys(token)).toEqual(["kind", "utilities"]);
				expect(token.kind).toBe("drafter_utility"); expect(Object.isFrozen(token)).toBe(true); expect(Object.isFrozen(token.utilities)).toBe(true);
				expect(token.utilities).toEqual([...new Set([lineage(action).utility, ...lineage(action).marginalUtilities ?? []])]);
			}
			controller.finishTurn("session", "producer"); await nextTurn();
			controller.actorActionSettled({ sessionID: "session", turnID: "consumer", candidate: { source: "drafter" } as never,
				candidateFeedback: descendant.feedback, computation: { actorComputeMs: 0, reusedExecutionMs: 450 }, reusedComputations: [
					{ source: "drafter", feedback: child.reuseFeedback, reusedExecutionMs: 100 },
					{ source: "drafter", feedback: descendant.reuseFeedback, reusedExecutionMs: 60 },
					{ source: "pattern_aware", feedback: descendant.reuseFeedback, reusedExecutionMs: 200 },
					{ source: "drafter", feedback: descendant.feedback, reusedExecutionMs: 90 },
				], settlement: { actorAction: { id: "whole", sequence: 1, turnID: "consumer" },
					provider: { kind: "speculative", timing: { expectedActorMs: 90000, hitLatencyMs: 7 } },
					matchedPredictions: [{ source: "drafter" }, { source: "drafter" }, { source: "pattern_aware" }] } as never });
			expect(ancestors.map(({ benefitMs, costMs }) => ({ benefitMs, costMs }))).toEqual([
				{ benefitMs: 160, costMs: 7 }, { benefitMs: 160, costMs: 7 }, { benefitMs: 60, costMs: 7 },
			]);
			controller.actorActionSettled({ sessionID: "session", turnID: "consumer", computation: { actorComputeMs: 40, reusedExecutionMs: 100 },
				reusedComputations: [{ source: "drafter", feedback: child.reuseFeedback, reusedExecutionMs: 100 }],
				settlement: { actorAction: { id: "partial", sequence: 2, turnID: "consumer" },
					provider: { kind: "actor", origin: "fallback", durationMs: 40, isError: false }, matchedPredictions: [] } as never });
			expect(ancestors.map(({ benefitMs, costMs }) => ({ benefitMs, costMs }))).toEqual([
				{ benefitMs: 260, costMs: 7 }, { benefitMs: 260, costMs: 7 }, { benefitMs: 60, costMs: 7 },
			]);
		} finally { controller.finishSession(); }
	});

	it("counts cancelled losing requests only after they settle, even when turns finish out of order", async () => {
		const pending = new Map<number, ReturnType<typeof deferred<void>>>();
		let calls = 0;
		const controller = createDrafterPlanSource({ sessionID: "session", patternHints: async () => [{ tool: "read", input: readInput, horizon: 0, expectedLatencyBenefitMs: 5000 }], complete: async (_model, _context, options) => {
			const index = calls++;
			if (index % 2) { const gate = deferred(); pending.set(index, gate); await gate.promise; options?.signal?.throwIfAborted(); }
			return readReply();
		} });
		const requests = Array.from({ length: 5 }, (_, round) => sourceRequest(`turn-${round}`, { drafterPatternHints: true }));
		const losing: Promise<unknown>[] = [];
		try {
			for (const request of requests.slice(0, 4)) {
				const root = first(await controller.source.propose(request))!;
				losing.push(Promise.resolve(controller.source.propose({ ...request, proposalIndex: 1 })).catch(() => undefined)); await nextTurn();
				await controller.actorActionSettled({ sessionID: "session", turnID: request.startInput.turnID, candidate: { source: "drafter" } as never,
					candidateFeedback: root.feedback, reusedComputations: [{ source: "drafter", feedback: root.reuseFeedback, reusedExecutionMs: 500 }],
					settlement: { actorAction: { id: request.startInput.turnID, sequence: calls, turnID: request.startInput.turnID },
						provider: { kind: "speculative", timing: { hitLatencyMs: 0 } },
						matchedPredictions: [{ source: "drafter" }] } as never });
			}
			for (const index of [3, 1, 2, 0]) { controller.finishTurn("session", requests[index]!.startInput.turnID); pending.get(index * 2 + 1)!.resolve(); }
			await Promise.all(losing); await nextTurn();
			expect(first(await controller.source.propose(requests[4]!))).toBeDefined();
			expect(await draftValue(controller.source.propose({ ...requests[4]!, proposalIndex: 1 }))).toBeUndefined();
			expect(calls).toBe(9);
		} finally { for (const gate of pending.values()) gate.resolve(); controller.finishSession(); await Promise.all(losing); }
	});
});
