import { createReadTool } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import type { PlanProposal, PlanUpdate } from "../src/plan-proposal.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { deferred, nextTurn } from "./async.ts";
import { testModel } from "./model.ts";

function first(value: PlanProposal | PlanUpdate | readonly (PlanProposal | PlanUpdate)[] | undefined) {
	if (!value || Array.isArray(value)) return undefined;
	return ("actions" in value ? value.actions : "upsert" in value ? value.upsert : undefined)?.[0];
}

describe("Drafter marginal request utility", () => {
	it.each(["width", "depth"] as const)("contracts unproductive %s, probes and recovers from late adoption", async dimension => {
		const tool = createReadTool("/workspace"), calls: string[] = [];
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async (_model, context) => {
			calls.push(String(context.messages.length));
			return fauxAssistantMessage([{ type: "toolCall", id: "read", name: "read", arguments: { path: "notes.txt" } }], { stopReason: "toolUse" });
		} });
		const extras: boolean[] = [];
		try {
			for (let round = 1; round <= 9; round++) {
				const request = { startInput: { sessionID: "session", turnID: `turn-${round}`, actorModel: testModel("actor"),
					context: { messages: [], tools: [tool] }, tools: [tool], actorOptions: undefined },
					data: { tools: new Map([["read", tool]]), schemaHashes: { read: "schema" } }, definitions: [], candidateNames: ["read"],
					settings: { enabled: true, candidateLimit: 2, resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000, tools: ["read"], sourceConfig: { drafterMaxDepth: 1 } },
					proposalIndex: 0, proposalCount: 2, signal: new AbortController().signal };
				const root = first(await controller.source.propose(request)); expect(root).toBeDefined();
				const continuation = { ...request, proposalID: `drafter:turn-${round}:0`, actionID: root!.id, feedback: root!.feedback,
					revision: 1, trigger: "execution_succeeded" as const, output: { result: { content: [], details: {} }, isError: false },
					candidate: { id: "root", key: PI_ACTION_SEMANTICS.buildKey("read", { path: "notes.txt" }, "/workspace")!, tool: "read", input: { path: "notes.txt" } } };
				if (dimension === "depth" && typeof controller.source.continueOn === "function") expect(controller.source.continueOn(continuation)).toBe(true);
				const extra = first(dimension === "width" ? await controller.source.propose({ ...request, proposalIndex: 1 }) : await controller.source.continue!(continuation));
				extras.push(Boolean(extra));
				const credit = async (feedback: unknown) => controller.actorActionSettled({ sessionID: "session", turnID: request.startInput.turnID,
					candidate: { source: "drafter" } as never, candidateFeedback: feedback, settlement: {
						provider: { kind: "speculative", timing: { expectedActorMs: 500, hitLatencyMs: 0 } }, matchedPredictions: [{ source: "drafter" }] } as never });
				await credit(root!.feedback); // The first request remains useful even while its expansion is wasted.
				controller.finishTurn("session", request.startInput.turnID); await nextTurn();
				if (round >= 8 && extra) await credit(extra.feedback); // Late feedback amends the closed turn's sample.
			}
			expect(extras).toEqual([true, true, true, true, false, false, false, true, true]);
			expect(calls).toHaveLength(15);
		} finally { controller.finishSession(); }
	});

	it("counts cancelled losing requests only after they settle, even when turns finish out of order", async () => {
		const tool = createReadTool("/workspace"), pending = new Map<number, ReturnType<typeof deferred<void>>>();
		let calls = 0;
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async (_model, _context, options) => {
			const index = calls++;
			if (index % 2) { const gate = deferred(); pending.set(index, gate); await gate.promise; options?.signal?.throwIfAborted(); }
			return fauxAssistantMessage([{ type: "toolCall", id: "read", name: "read", arguments: { path: "notes.txt" } }], { stopReason: "toolUse" });
		} });
		const requests = Array.from({ length: 5 }, (_, round) => ({ startInput: { sessionID: "session", turnID: `turn-${round}`, actorModel: testModel("actor"),
			context: { messages: [], tools: [tool] }, tools: [tool], actorOptions: undefined }, data: { tools: new Map([["read", tool]]), schemaHashes: {} },
			definitions: [], candidateNames: ["read"], settings: { enabled: true, resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000, tools: ["read"] },
			proposalIndex: 0, proposalCount: 2, signal: new AbortController().signal }));
		const losing: Promise<unknown>[] = [];
		try {
			for (const request of requests.slice(0, 4)) {
				const root = first(await controller.source.propose(request))!;
				losing.push(Promise.resolve(controller.source.propose({ ...request, proposalIndex: 1 })).catch(() => undefined)); await nextTurn();
				await controller.actorActionSettled({ sessionID: "session", turnID: request.startInput.turnID, candidate: { source: "drafter" } as never,
					candidateFeedback: root.feedback, settlement: { provider: { kind: "speculative", timing: { expectedActorMs: 500, hitLatencyMs: 0 } },
						matchedPredictions: [{ source: "drafter" }] } as never });
			}
			for (const index of [3, 1, 2, 0]) { controller.finishTurn("session", requests[index]!.startInput.turnID); pending.get(index * 2 + 1)!.resolve(); }
			await Promise.all(losing); await nextTurn();
			expect(first(await controller.source.propose(requests[4]!))).toBeDefined();
			expect(await controller.source.propose({ ...requests[4]!, proposalIndex: 1 })).toBeUndefined();
			expect(calls).toBe(9);
		} finally { for (const gate of pending.values()) gate.resolve(); controller.finishSession(); await Promise.all(losing); }
	});
});
