import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { expect, test } from "vitest";
import { ActorStreamPreviewTracker } from "../src/settlement.ts";

test("streamed previews finish once across escaped quotes and nested delimiters split between chunks", () => {
	const tracker = new ActorStreamPreviewTracker();
	const args = { path: 'a\\b"}', nested: [{ text: "[{" }] };
	const toolCall = fauxToolCall("read", args, { id: "stream" });
	const partial = fauxAssistantMessage(toolCall);
	const events = [...JSON.stringify(args)].flatMap((delta) =>
		tracker.observe({ type: "toolcall_delta", contentIndex: 0, delta, partial }),
	);
	const call = { type: "call", call: { id: toolCall.id, name: toolCall.name, arguments: args } };
	expect(events).toEqual([call]);
	const end = { type: "toolcall_end", contentIndex: 0, toolCall, partial } as const;
	expect(tracker.observe(end)).toEqual([]);
	tracker.clear();
	expect(tracker.observe(end)).toEqual([call]);
});
