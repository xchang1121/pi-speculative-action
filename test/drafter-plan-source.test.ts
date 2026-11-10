import { expect, test, vi } from "vitest";
import { fauxAssistantMessage, fauxToolCall, Type } from "@earendil-works/pi-ai";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import { normalizeDrafterRequestSettings } from "../src/common.ts";
import { deferred } from "./async.ts";
import { testModel } from "./model.ts";
import { buildActionKey } from "../src/action-semantics.ts";
import type { PlanProposal } from "../src/plan-proposal.ts";

function request(signal: AbortSignal, settings = {}) {
	return { startInput: { sessionID: "task", turnID: "one", actorOptions: {}, actorModel: testModel("actor", { contextWindow: 100000 }),
		context: { messages: [], tools: [{ name: "read", description: "Read", parameters: Type.Object({ path: Type.String() }) }] }, tools: [] },
		data: { tools: new Map(), schemaHashes: {} }, settings: { enabled: true, resourceCacheMaxEntries: 8, predictionTimeoutMs: 1000,
			sourceConfig: normalizeDrafterRequestSettings({ drafterBudgetInitialFraction: 1, ...settings }) },
		definitions: [], proposalIndex: 0, proposalCount: 1, signal };
}
const root = () => fauxAssistantMessage(fauxToolCall("read", { path: "a.txt" }, { id: "a" }));

test.each(["disabled", "failed", "timeout"])("optional pattern hints cannot block basic drafting (%s)", async mode => {
	const controller = new AbortController(), diagnostic = vi.fn();
	const hints = vi.fn(async () => { if (mode === "timeout") return new Promise<never>(() => {}); throw new Error("capture unavailable"); });
	const complete = vi.fn(async () => root());
	const plans = createDrafterPlanSource({ sessionID: "task", complete, patternHints: hints, onDiagnostic: diagnostic });
	const proposal = await plans.source.propose(request(controller.signal, { drafterPatternHints: mode !== "disabled", drafterPatternHintsTimeoutMs: 10 }));
	expect(proposal).toMatchObject({ actions: [{ tool: "read", input: { path: "a.txt" } }] });
	expect(complete).toHaveBeenCalledTimes(1); expect(hints).toHaveBeenCalledTimes(mode === "disabled" ? 0 : 1);
	if (mode !== "disabled") expect(diagnostic).toHaveBeenCalledWith(expect.objectContaining({ code: mode === "failed" ? "pattern_hints_failed" : "pattern_hints_timeout" }));
	controller.abort();
});

test("publishes a multi-tool root while independent scope selection is still pending", async () => {
	const controller = new AbortController(), scope = deferred(), entered = deferred();
	const plans = createDrafterPlanSource({ sessionID: "task", complete: async (_model, context) => {
		if (context.tools?.[0]?.name === "speculative_continuations") {
			expect(context.messages.some(message => message.role === "toolResult")).toBe(false);
			entered.resolve(); await scope.promise;
			return fauxAssistantMessage(fauxToolCall("speculative_continuations", { scopes: [["a"], ["b"]] }));
		}
		return fauxAssistantMessage([fauxToolCall("read", { path: "a.txt" }, { id: "a" }), fauxToolCall("read", { path: "b.txt" }, { id: "b" })]);
	} });
	try {
		const proposal = await plans.source.propose(request(controller.signal));
		await entered.promise;
		expect(proposal).toMatchObject({ actions: [{ id: "0:0", tool: "read" }, { id: "0:1", tool: "read" }] });
	} finally { controller.abort(); scope.resolve(); }
});

test("a root prepared before scope selection still starts its independent successor without the other root", async () => {
	const controller = new AbortController(), scope = deferred();
	const seen: string[][] = [];
	const plans = createDrafterPlanSource({ sessionID: "task", complete: async (_model, context) => {
		if (context.tools?.[0]?.name === "speculative_continuations") {
			await scope.promise;
			return fauxAssistantMessage(fauxToolCall("speculative_continuations", { scopes: [["a"], ["b"]] }));
		}
		const observations = context.messages.filter(message => message.role === "toolResult");
		if (observations.length) {
			seen.push(observations.flatMap(message => message.content.flatMap(part => part.type === "text" ? [part.text] : [])));
			return fauxAssistantMessage(fauxToolCall("read", { path: "child.txt" }));
		}
		return fauxAssistantMessage([fauxToolCall("read", { path: "a.txt" }, { id: "a" }), fauxToolCall("read", { path: "b.txt" }, { id: "b" })]);
	} });
	try {
		const input = request(controller.signal, { drafterMaxDepth: 1 }), proposal = await plans.source.propose(input) as PlanProposal;
		const start = (index: number) => {
			const action = proposal.actions[index]!, key = buildActionKey({ tool: "read", input: action.input as Record<string, unknown>, resources: [] });
			const output = { result: { content: [{ type: "text" as const, text: `result-${index}` }], details: {} }, isError: false };
			const choose = plans.source.continueOn; if (typeof choose !== "function") throw new Error("missing continuation selector");
			const branches = choose({ actionID: action.id, action: key, feedback: action.feedback, output, trigger: "execution_succeeded" });
			if (typeof branches === "boolean" || !branches[0]) throw new Error("missing continuation scope");
			return plans.source.continue!({ ...input, proposalID: proposal.id, actionID: action.id, feedback: branches[0].feedback,
				candidate: { id: action.id, tool: "read", input: key.input }, revision: index + 1, output, trigger: "execution_succeeded" });
		};
		const first = start(0); scope.resolve(); const update = await first;
		expect(update).toBeDefined(); expect(seen).toEqual([["result-0", "Observation withheld from this continuation branch."]]);
		await start(1);
		expect(seen[1]).toEqual(["Observation withheld from this continuation branch.", "result-1"]);
		const changes = Array.isArray(update) ? update : [update];
		expect(changes[0]).toMatchObject({ upsert: [{ dependsOn: [{ actionID: "0:0", condition: "prediction" }, { actionID: "0:1", condition: "resource" }] }] });
	} finally { scope.resolve(); controller.abort(); }
});
