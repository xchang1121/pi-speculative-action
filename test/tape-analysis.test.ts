import { describe, expect, it } from "vitest";
import {
	analyzeTape,
	type LlmTape,
} from "../bench/tape-analysis.ts";

describe("LLM tape action analysis", () => {
	it("charges each recorded request once across a multi-tool Actor batch", () => {
		const messages = [{ role: "user", content: "two actions" }];
		const batch = calls(["read", '{"path":"a"}'], ["write", '{"path":"b"}']);
		const result = analyzeTape({ exchanges: [
			exchange(0, "actor", messages, 100, [batch]),
			exchange(1, "draft", messages, 30, [batch]),
			exchange(2, "draft", messages, 60, [call("read", '{"path":"a"}')]),
		] }, "actor", "draft");
		expect(result.summary).toMatchObject({ opportunities: 2, candidateCount: 3,
			uniqueCandidateCount: 2, actorServiceMs: 100, drafterServiceMs: 90 });
	});

	it.each([Infinity, 1, 2, 3, 5])("pairs exact contexts and full K(a) across %s-byte stream chunks", (chunkBytes) => {
		const messages = [{ role: "user", content: "fix" }];
		const args = JSON.stringify({ path: chunkBytes === Infinity ? "a" : "文档/😀.txt" });
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, [call("read", args)]),
				exchange(1, "draft", messages, 30, [call("read", args)], chunkBytes),
				exchange(2, "draft", messages, 40, [call("read", args)], chunkBytes),
				exchange(3, "draft", messages, 20, [call("read", '{"path":"b"}')]),
				exchange(4, "draft", [{ role: "user", content: "other" }], 10, [call("read", '{"path":"a"}')]),
			],
		};

		const result = analyzeTape(tape, "actor", "draft");

		expect(result.summary).toEqual({
			actorRequests: 1,
			drafterRequests: 4,
			actorServiceMs: 100,
			drafterServiceMs: 100,
			requestsWithoutDuration: 0,
			opportunities: 1,
			exactMatches: 1,
			matchRate: 1,
			candidateCount: 4,
			uniqueCandidateCount: 3,
			duplicateCandidateCount: 1,
			uniqueYield: 3 / 4,
		});
		expect(result.opportunities).toEqual([{
			actorSequence: 0, actorAction: { name: "read", arguments: JSON.parse(args) }, drafterSequences: [1, 2],
		}]);
		expect(result.requests[1]).toMatchObject({
			body: tape.exchanges[1]!.request.descriptor.body, serviceMs: 30,
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

	it.each(["messages", "tools", "functions"])("requires equal %s while retaining decoding options in the request evidence", (field) => {
		const messages = [{ role: "user", content: "same request" }];
		const event = call("read", '{"path":"a"}');
		const base = { messages, tools: [{ type: "function", function: { name: "read", parameters: { type: "object" } } }], functions: [] };
		const bodies = [
			{ ...base, model: "actor", reasoning_effort: "high", max_tokens: 1000, tool_choice: "auto" },
			{ ...base, model: "draft", reasoning_effort: "low", max_tokens: 128, tool_choice: "required" },
			{ ...base, model: "draft", [field]: [{ changed: true }] },
		];
		const tape = { exchanges: bodies.map((body, index) => ({ ...exchange(index, body.model, messages, 20, [event]),
			request: { descriptor: { body } } })) };
		const result = analyzeTape(tape, "actor", "draft");
		expect(result.opportunities[0]?.drafterSequences).toEqual([1]);
		expect(result.requests.map((request) => request.body)).toEqual(bodies);
	});

	it("retains partial, failed and malformed requests with usage while excluding their batches from comparison", () => {
		const messages = [{ role: "user", content: "decision" }], event = call("read", '{"path":"a"}');
		const usage = { prompt_tokens: 80, completion_tokens: 12, total_tokens: 92,
			prompt_tokens_details: { cached_tokens: 50 }, completion_tokens_details: { reasoning_tokens: 8 } };
		const receipt = `data: ${JSON.stringify({ choices: [], usage })}\n\n`;
		const exchanges = [exchange(0, "actor", messages, 100, [event]), exchange(1, "actor", messages, 90, [event]),
			...Array.from({ length: 4 }, (_, index) => exchange(index + 2, "draft", messages, (index + 1) * 10, [event, receipt]))];
		exchanges[2] = { ...exchanges[2]!, response: { ...exchanges[2]!.response, completed: false } };
		exchanges[3] = { ...exchanges[3]!, response: { ...exchanges[3]!.response, status: 503, error: { code: "upstream" } } };
		exchanges[4] = exchange(4, "draft", messages, 30, [event, "data: {malformed}\n\n", receipt]);
		exchanges.push(exchange(6, "draft", messages, NaN, [call("read", "{")]));
		const tape = { exchanges }, before = structuredClone(tape), result = analyzeTape(tape, "actor", "draft");
		expect(tape).toEqual(before);
		expect(result.summary).toMatchObject({ actorRequests: 2, drafterRequests: 5, actorServiceMs: 190,
			drafterServiceMs: 100, requestsWithoutDuration: 1, opportunities: 2, exactMatches: 2, candidateCount: 1 });
		expect(result.opportunities.map((opportunity) => opportunity.drafterSequences)).toEqual([[5], [5]]);
		expect(result.requests.slice(2, 6).map((request) => request.usage)).toEqual([usage, usage, usage, usage]);
		expect(result.requests[4]).toMatchObject({ malformedEvents: 1 });
		expect(result.requests[6]).toMatchObject({ invalidCalls: 1, serviceMs: undefined });
	});

	it("assembles interleaved tool arguments in provider batch order", () => {
		const delta = (index: number, fn: object) => `data: ${JSON.stringify({ choices: [{ delta: { tool_calls: [{ index, function: fn }] } }] })}\n\n`;
		const chunks = [delta(1, { name: "write", arguments: '{"path":' }), delta(0, { name: "read", arguments: '{"path":"文档/😀.txt"}' }), delta(1, { arguments: '"b"}' })];
		const result = analyzeTape({ exchanges: [exchange(0, "actor", [], 100, chunks, 1), exchange(1, "draft", [], 10, chunks, 2)] }, "actor", "draft");
		expect(result.requests.map((request) => request.calls)).toEqual([0, 1].map(() => [
			{ name: "read", arguments: { path: "文档/😀.txt" } }, { name: "write", arguments: { path: "b" } },
		]));
		expect(result.summary).toMatchObject({ opportunities: 2, exactMatches: 2, candidateCount: 2, drafterServiceMs: 10 });
	});

	it("rejects ambiguous model roles and duplicate request identities", () => {
		expect(() => analyzeTape({ exchanges: [] }, "same", "same")).toThrow("Distinct model IDs");
		const request = exchange(0, "actor", [], 1, []);
		expect(() => analyzeTape({ exchanges: [request, request] }, "actor", "draft")).toThrow("unique non-negative integers");
	});
});

type TapeFixtureChunk = string | { readonly atMs: number; readonly data: string };

function exchange(
	sequence: number,
	model: string,
	messages: unknown,
	endedAtMs: number,
	chunks: readonly TapeFixtureChunk[],
	chunkBytes = Infinity,
): LlmTape["exchanges"][number] {
	return {
		sequence,
		request: { descriptor: { body: { model, messages } } },
		response: {
			completed: true,
			endedAtMs,
			chunks: chunks.flatMap((chunk) => {
				const data = typeof chunk === "string" ? chunk : chunk.data;
				const bytes = Buffer.from(data), parts = [];
				for (let offset = 0; offset < bytes.length; offset += chunkBytes) parts.push({
					...(typeof chunk === "string" ? {} : { atMs: chunk.atMs }),
					dataBase64: bytes.subarray(offset, offset + chunkBytes).toString("base64"),
				});
				return parts;
			}),
		},
	};
}

function call(name: string, argumentsDelta: string): string {
	return calls([name, argumentsDelta]);
}

function calls(...entries: readonly (readonly [name: string, argumentsDelta: string])[]): string {
	return `data: ${JSON.stringify({
		choices: [
			{
				delta: {
					tool_calls: entries.map(([name, argumentsDelta], index) => ({
						index,
						function: { name, arguments: argumentsDelta },
					})),
				},
			},
		],
	})}\n\ndata: [DONE]\n\n`;
}
