import { describe, expect, it } from "vitest";
import { analyzeTape, analyzeTapeForkGate, type LlmTape } from "../bench/tape-analysis.ts";

describe("LLM tape action analysis", () => {
	it("pairs exact contexts, deduplicates full K(a), and measures only early exact candidates", () => {
		const messages = [{ role: "user", content: "fix" }];
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, [call("read", '{"path":"a"}')]),
				exchange(1, "draft", messages, 30, [call("read", '{"path":"a"}')]),
				exchange(2, "draft", messages, 40, [call("read", '{"path":"a"}')]),
				exchange(3, "draft", messages, 20, [call("read", '{"path":"b"}')]),
				exchange(4, "draft", [{ role: "user", content: "other" }], 10, [call("read", '{"path":"a"}')]),
			],
		};

		const result = analyzeTape(tape, "actor", "draft");

		expect(result.summary).toEqual({
			opportunities: 1,
			exactHits: 1,
			hitRate: 1,
			exactReadyBeforeActor: 1,
			earlyHitRate: 1,
			candidateCount: 3,
			uniqueCandidateCount: 2,
			duplicateCandidateCount: 1,
			uniqueYield: 2 / 3,
			actorDecodeMs: 100,
			drafterServiceMs: 90,
			exactLeadMs: 70,
		});
		expect(result.opportunities[0]).toMatchObject({
			drafterSequences: [1, 2, 3],
			exactReadyBeforeActor: true,
			earliestExactReadyMs: 30,
		});
	});

	it("ignores malformed, incomplete, and non-tool responses", () => {
		const messages = [{ role: "user", content: "fix" }];
		const incomplete = exchange(1, "draft", messages, 10, [call("read", "{")]);
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, []),
				{ ...incomplete, response: { ...incomplete.response, completed: false } },
			],
		};

		expect(analyzeTape(tape, "actor", "draft")).toMatchObject({
			completedExchanges: 1,
			incompleteExchanges: 1,
			opportunities: [],
		});
	});

	it("replays the rolling gate against the fastest-completing same-context Drafter", () => {
		const exchanges: LlmTape["exchanges"][number][] = [];
		for (let index = 0; index < 7; index++) {
			const messages = [{ role: "user", content: `decision-${index}` }];
			exchanges.push(exchange(index * 2, "actor", messages, 100, [call("read", `{"path":"actor-${index}"}`)]));
			exchanges.push(exchange(index * 2 + 1, "draft", messages, 60, [call("read", `{"path":"miss-${index}"}`)]));
			if (index === 0)
				exchanges.push(exchange(1_000, "draft", messages, 10, [call("read", '{"path":"actor-0"}')]));
		}
		const result = analyzeTapeForkGate({ exchanges }, "actor", "draft", {
			enabled: true,
			minSamples: 4,
			windowSize: 4,
			minNetBenefitMs: 25,
			probeInterval: 4,
			failureThreshold: 2,
		});

		expect(result).toMatchObject({
			decisions: 7,
			allowed: 4,
			skipped: 3,
			exactHitsAvailable: 1,
			exactHitsRetained: 1,
			forkCostMs: 370,
			gatedForkCostMs: 190,
		});
	});
});

function exchange(
	sequence: number,
	model: string,
	messages: unknown,
	endedAtMs: number,
	chunks: readonly string[],
): LlmTape["exchanges"][number] {
	return {
		sequence,
		request: { descriptor: { body: { model, messages } } },
		response: {
			completed: true,
			endedAtMs,
			chunks: chunks.map((data) => ({ dataBase64: Buffer.from(data).toString("base64") })),
		},
	};
}

function call(name: string, argumentsDelta: string): string {
	return `data: ${JSON.stringify({
		choices: [{ delta: { tool_calls: [{ index: 0, function: { name, arguments: argumentsDelta } }] } }],
	})}\n\ndata: [DONE]\n\n`;
}
