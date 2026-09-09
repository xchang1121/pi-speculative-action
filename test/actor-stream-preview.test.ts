import { createFauxCore, fauxAssistantMessage, fauxToolCall, type UserMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { ActorStreamPreviewTracker } from "../src/actor-stream-preview.ts";

const prompt: UserMessage = { role: "user", content: "inspect", timestamp: 0 };

describe("Actor stream previews", () => {
	it.each(["read", "grep", "custom"])("waits for complete %s input without rewriting its arguments", async (tool) => {
		const input = { path: String.raw`src/a\"b.ts`, offset: 200, limit: 20 };
		const result = await streamedCalls(tool, input);

		expect(result.calls).toEqual([input]);
		expect(result.callCharacter).toBe(result.endCharacter);
	});

	it("ignores delimiters and completed fields until the full object arrives", () => {
		const partial = fauxAssistantMessage(fauxToolCall("read", {}, { id: "chunked-read" }), {
			stopReason: "toolUse",
		});
		const tracker = new ActorStreamPreviewTracker();
		tracker.observe({ type: "toolcall_start", contentIndex: 0, partial });

		expect(
			tracker.observe({
				type: "toolcall_delta",
				contentIndex: 0,
				delta: '{"path":"src/a,]',
				partial,
			}),
		).toEqual([]);
		expect(
			tracker.observe({
				type: "toolcall_delta",
				contentIndex: 0,
				delta: 'b.ts","offset":',
				partial,
			}),
		).toEqual([]);
		expect(tracker.observe({ type: "toolcall_delta", contentIndex: 0, delta: "200}", partial })).toEqual([
			{ type: "call", call: { id: "chunked-read", name: "read", arguments: { path: "src/a,]b.ts", offset: 200 } } },
		]);
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
