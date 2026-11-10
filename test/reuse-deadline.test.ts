import { expect, test } from "vitest";
import { ProjectionFailureMemory } from "../src/reuse-deadline.ts";
import { attachSdkTools } from "../src/sdk-tool-binding.ts";

test("projection failure suppression is scoped to its immutable owner and expires", () => {
	let now = 10; const memory = new ProjectionFailureMemory(() => now), original = {}, replacement = {};
	memory.remember(original, "grep:engine1", 100);
	expect(memory.blocked(original, "grep:engine1")).toBe(true);
	expect(memory.blocked(replacement, "grep:engine1")).toBe(false);
	expect(memory.blocked(original, "grep:engine2")).toBe(false);
	now = 110; expect(memory.blocked(original, "grep:engine1")).toBe(false);
});

test("an SDK instance mismatch is diagnosed instead of silently claiming interception", () => {
	const diagnostics: unknown[] = [];
	const binding = attachSdkTools({} as Parameters<typeof attachSdkTools>[0], { onDiagnostic: diagnostic => diagnostics.push(diagnostic) });
	expect(binding.attached).toBe(false); expect(binding.tools()).toEqual([]);
	expect(diagnostics).toEqual([expect.objectContaining({ code: "sdk_binding_unavailable" })]); binding.dispose();
});
