import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { expect, test } from "vitest";
import { PlanRuntime } from "../src/runtime-engine.ts";
import { ActorStreamPreviewTracker, type PlanAction } from "../src/settlement.ts";

test.each([false, true])(
	"accepts deep plans in either submission order and rejects invalid revisions (reverse %s)",
	(reverse) => {
		const runtime = new PlanRuntime();
		const count = 12_000;
		const actions: PlanAction[] = Array.from({ length: count }, (_, index) => ({
			id: String(index),
			type: "tool_call",
			tool: "custom",
			input: {},
			...(index ? { dependsOn: [{ actionID: String(index - 1) }] } : {}),
		}));
		if (reverse) actions.reverse();
		expect(runtime.apply({ id: "deep", source: "custom", revision: 1, actions }, 0).accepted).toBe(true);
		expect(runtime.get("deep", String(count - 1))).toMatchObject({
			expectedDecisionSeq: count,
			readiness: "waiting",
		});
		expect(runtime.promote("deep", "0").status).toBe("scheduled");
		for (const actionID of [String(count - 1), "missing"]) {
			expect(
				runtime.apply(
					{
						proposalID: "deep",
						source: "custom",
						revision: 2,
						upsert: [
							{
								id: "0",
								type: "tool_call",
								tool: "custom",
								input: {},
								dependsOn: [{ actionID }],
							},
						],
					},
					0,
				),
			).toEqual({ accepted: false, reason: "invalid_dependency" });
			expect(runtime.plan("deep")?.revision).toBe(1);
		}
	},
);

test("streamed previews finish once across escaped quotes and nested delimiters split between chunks", () => {
	const tracker = new ActorStreamPreviewTracker();
	const args = { path: 'a\\b"}', nested: [{ text: "[{" }] };
	const toolCall = fauxToolCall("read", args, { id: "stream" });
	const partial = fauxAssistantMessage(toolCall);
	const events = [...JSON.stringify(args)].flatMap((delta) =>
		tracker.observe({ type: "toolcall_delta", contentIndex: 0, delta, partial }),
	);
	const call = { type: "call", call: { id: toolCall.id, name: toolCall.name, arguments: args } };
	expect(events).toEqual([{ type: "tool", tool: "read" }, call]);
	const end = { type: "toolcall_end", contentIndex: 0, toolCall, partial } as const;
	expect(tracker.observe(end)).toEqual([]);
	tracker.clear();
	expect(tracker.observe(end)).toEqual([call]);
});
