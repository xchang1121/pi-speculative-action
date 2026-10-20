import { expect, test } from "vitest";
import { PlanRuntime } from "../src/plan-runtime.ts";
import type { PlanAction } from "../src/plan-proposal.ts";

test.each([false, true])("accepts deep plans in either submission order and rejects invalid revisions (reverse %s)", reverse => {
	const runtime = new PlanRuntime(), count = 12_000;
	const actions: PlanAction[] = Array.from({ length: count }, (_, index) => ({ id: String(index), type: "tool_call", tool: "custom", input: {},
		...(index ? { dependsOn: [{ actionID: String(index - 1) }] } : {}) }));
	if (reverse) actions.reverse();
	expect(runtime.apply({ id: "deep", source: "custom", revision: 1, actions }, 0).accepted).toBe(true);
	expect(runtime.get("deep", String(count - 1))).toMatchObject({ expectedDecisionSeq: count, readiness: "waiting" });
	expect(runtime.promote("deep", "0").status).toBe("scheduled");
	for (const actionID of [String(count - 1), "missing"]) {
		expect(runtime.apply({ proposalID: "deep", source: "custom", revision: 2,
			upsert: [{ id: "0", type: "tool_call", tool: "custom", input: {}, dependsOn: [{ actionID }] }] }, 0))
			.toEqual({ accepted: false, reason: "invalid_dependency" });
		expect(runtime.plan("deep")?.revision).toBe(1);
	}
});
