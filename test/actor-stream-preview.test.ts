import { createFauxCore, fauxAssistantMessage, fauxToolCall, type UserMessage } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import { ActorStreamPreviewTracker } from "../src/actor-stream-preview.ts";

const prompt: UserMessage = { role: "user", content: "inspect", timestamp: 0 };

describe("Actor stream previews", () => {
	it.each(["read", "grep", "custom"])("waits for complete %s input without rewriting its arguments", async (tool) => {
		const input = { path: String.raw`src/a\"b.ts`, offset: 200, limit: 20 };
		const result = await streamedCalls(tool, input);

		expect(result.calls).toEqual([input]);
		expect(result.callCharacter).toBe(result.endCharacter);
	});

	it("scans each fragment once and waits for strict complete input or the official end", () => {
		const call = fauxToolCall("custom", {}, { id: "chunked-call" });
		const partial = fauxAssistantMessage(call, { stopReason: "toolUse" });
		const tracker = new ActorStreamPreviewTracker();
		const delta = (value: string, contentIndex = 0) =>
			tracker.observe({ type: "toolcall_delta", contentIndex, delta: value, partial });
		const end = () => tracker.observe({ type: "toolcall_end", contentIndex: 0, toolCall: call, partial });
		const preview = (args: Record<string, unknown>, id = call.id) =>
			({ type: "call", call: { id, name: call.name, arguments: args } });
		for (const input of [{ path: "src/a,]b.ts", offset: 200 },
			{ nested: [{ braces: '{}[]', escapes: '\\"\\\\\n\u263a', number: -1.2e30 }, null, true] },
			{ content: 'code = { text: "quoted", path: "C:\\nested" };\n'.repeat(8192) }]) {
			for (const chunk of [1, 7, 256]) {
				tracker.clear();
				expect(tracker.observe({ type: "toolcall_start", contentIndex: 0, partial }))
					.toEqual([{ type: "tool", tool: call.name }]);
				const text = ' \n' + JSON.stringify(input), parse = vi.spyOn(JSON, "parse");
				try {
					const previews = [];
					for (let offset = 0; offset < text.length; offset += chunk) {
						const hints = delta(text.slice(offset, offset + chunk));
						if (hints.length) expect(offset + chunk).toBeGreaterThanOrEqual(text.length);
						previews.push(...hints);
					}
					expect(previews).toEqual([preview(input)]);
					expect(delta(' \n' + ' '.repeat(1024))).toEqual([]);
					expect(end()).toEqual([]);
					expect(parse).toHaveBeenCalledTimes(1);
					expect(parse).toHaveBeenCalledWith(text);
				} finally { parse.mockRestore(); }
			}
		}
		for (const invalid of ['{"x":1,}', '{"x":[1}', '{"x":"unterminated', '[{}]', 'null', '"{}"', '{}junk']) {
			tracker.clear();
			tracker.observe({ type: "toolcall_start", contentIndex: 0, partial });
			const parse = vi.spyOn(JSON, "parse");
			try {
				expect(delta(invalid)).toEqual([]);
				const attempts = parse.mock.calls.length;
				for (let index = 0; index < 10; index++) expect(delta(' ')).toEqual([]);
				expect(parse).toHaveBeenCalledTimes(attempts);
			} finally { parse.mockRestore(); }
			expect(end()).toEqual([preview({})]);
		}
		tracker.clear();
		call.id = "";
		call.name = "";
		expect(delta('{"late":true}')).toEqual([]);
		call.id = "late-id";
		call.name = "custom";
		expect(delta(' \n')).toEqual([{ type: "tool", tool: call.name }, preview({ late: true })]);
		expect(end()).toEqual([]);
		tracker.clear();
		partial.content.push(fauxToolCall("custom", {}, { id: "second-call" }));
		delta('{"first":');
		delta('{"second":', 1);
		expect(delta('2}', 1)).toEqual([preview({ second: 2 }, "second-call")]);
		expect(delta('1}')).toEqual([preview({ first: 1 })]);
		tracker.clear();
		delta('{"abandoned":');
		tracker.clear();
		expect(delta('true}')).toEqual([{ type: "tool", tool: call.name }]);
		expect(end()).toEqual([preview({})]);
	});
});

async function streamedCalls(tool: string, input: Record<string, unknown>) {
	const actor = createFauxCore({
		provider: `actor-preview-${tool}`,
		models: [{ id: "actor", reasoning: false }],
		tokensPerSecond: 100_000,
		tokenSize: { min: 1, max: 1 },
	});
	actor.setResponses([
		fauxAssistantMessage(fauxToolCall(tool, input, { id: `${tool}-call` }), { stopReason: "toolUse" }),
	]);
	const tracker = new ActorStreamPreviewTracker();
	const calls: Record<string, unknown>[] = [];
	let characters = 0;
	let callCharacter = -1;
	const stream = actor.streamSimple(actor.getModel(), { messages: [prompt] });
	for await (const update of stream) {
		if (update.type === "toolcall_delta") characters += update.delta.length;
		for (const preview of tracker.observe(update)) {
			if (preview.type !== "call") continue;
			calls.push(preview.call.arguments);
			callCharacter = characters;
		}
	}
	return { calls, callCharacter, endCharacter: JSON.stringify(input).length };
}
