import { describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import type { ThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";

describe("ThinkThread profile lifecycle", () => {
	it.each(["read", "write", "edit", "bash", "hit", "failed"])("invalidates BASE only on an Actor mutation before settlement: %s", async (mode) => {
		const host = createSpeculativeActionHost("session", {
			cwd: "/workspace", complete: async () => { throw new Error("No model requests expected"); },
		});
		const actorFallbackSettled = vi.fn(async () => undefined);
		const finishTurn = vi.fn(async () => undefined);
		const wrapped = withThinkThreadProfileLifecycle(host, { actorFallbackSettled, finishTurn } as unknown as ThinkThreadExecutionWorld);
		const actual = vi.spyOn(host.runtime, "actual");
		const result = { content: [], details: {} };
		const failure = new Error("Actor failed after writing");
		const executor = vi.fn(async () => { if (mode === "failed") throw failure; return result; });
		const tool = mode === "hit" || mode === "failed" ? "write" : mode;
		if (mode === "hit") vi.spyOn(host.runtime, "consume").mockResolvedValueOnce({ result, isError: false });
		if (mode === "failed") actorFallbackSettled.mockRejectedValueOnce(new Error("BASE cleanup failed"));
		try {
			const pending = wrapped.execute({ turnID: "turn", tool, args: {}, tools: [] }, undefined, executor);
			if (mode === "failed") await expect(pending).rejects.toBe(failure);
			else await expect(pending).resolves.toBe(result);
			expect(executor).toHaveBeenCalledTimes(mode === "hit" ? 0 : 1);
			const mutated = mode !== "read" && mode !== "hit";
			expect(actorFallbackSettled).toHaveBeenCalledTimes(mutated ? 1 : 0);
			if (mutated) expect(actorFallbackSettled.mock.invocationCallOrder[0]).toBeLessThan(actual.mock.invocationCallOrder[0]!);
			await wrapped.finishTurn("turn");
			expect(finishTurn).toHaveBeenCalledWith("turn");
		} finally { await wrapped.dispose(); }
	});

	it("cleans the pool on failed host settlement and preserves the legacy actual outlet", async () => {
		const host = createSpeculativeActionHost("session", { cwd: "/workspace", complete: vi.fn() });
		const actorFallbackSettled = vi.fn(async () => undefined);
		const finishTurn = vi.fn(async () => undefined);
		const wrapped = withThinkThreadProfileLifecycle(host, { actorFallbackSettled, finishTurn } as unknown as ThinkThreadExecutionWorld);
		const failure = new Error("settlement failed");
		try {
			for (const tool of ["read", "write"]) await wrapped.actual({ turnID: "turn", tool, args: {}, tools: [], durationMs: 1, output: { result: { content: [], details: {} }, isError: false } });
			expect(actorFallbackSettled).toHaveBeenCalledOnce();
			vi.spyOn(host, "finishTurn").mockRejectedValueOnce(failure);
			await expect(wrapped.finishTurn("turn")).rejects.toBe(failure);
			expect(finishTurn).toHaveBeenCalledWith("turn");
		} finally { await wrapped.dispose(); }
	});
});
