import { describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import type { AgentExecutionWorld } from "../src/agent-execution-world.ts";

describe("shared execution world lifecycle", () => {
	it.each(["read", "write", "edit", "bash", "hit", "failed"])("invalidates preparation only on an Actor mutation before settlement: %s", async (mode) => {
		const actorFallbackSettled = vi.fn(async () => undefined);
		const finishTurn = vi.fn(async () => undefined);
		const world = lifecycleWorld(actorFallbackSettled, finishTurn);
		const host = createSpeculativeActionHost("session", {
			cwd: "/workspace", complete: async () => { throw new Error("No model requests expected"); },
			executionWorlds: [world, world],
		});
		const settle = vi.fn(async () => undefined);
		const result = { content: [], details: {} };
		const failure = new Error("Actor failed after writing");
		const executor = vi.fn(async () => { if (mode === "failed") throw failure; return result; });
		const tool = mode === "hit" || mode === "failed" ? "write" : mode;
		vi.spyOn(host.runtime, "prepareActorCall").mockResolvedValueOnce({ settle,
			...(mode === "hit" ? { output: { result, isError: false } } : {}) });
		if (mode === "failed") actorFallbackSettled.mockRejectedValueOnce(new Error("BASE cleanup failed"));
		try {
			const pending = host.execute({ turnID: "turn", tool, args: {}, tools: [] }, undefined, executor);
			if (mode === "failed") await expect(pending).rejects.toBe(failure);
			else await expect(pending).resolves.toBe(result);
			expect(executor).toHaveBeenCalledTimes(mode === "hit" ? 0 : 1);
			const mutated = mode !== "read" && mode !== "hit";
			expect(actorFallbackSettled).toHaveBeenCalledTimes(mutated ? 1 : 0);
			expect(settle).toHaveBeenCalledTimes(mode === "hit" ? 0 : 1);
			if (mutated) expect(actorFallbackSettled.mock.invocationCallOrder[0]).toBeLessThan(settle.mock.invocationCallOrder[0]!);
			await host.finishTurn("turn");
			expect(finishTurn).toHaveBeenCalledOnce();
			expect(finishTurn).toHaveBeenCalledWith("turn");
		} finally { await host.dispose(); }
	});

	it("cleans world preparation when finishing the host turn fails", async () => {
		const actorFallbackSettled = vi.fn(async () => undefined);
		const finishTurn = vi.fn(async () => undefined);
		const host = createSpeculativeActionHost("session", { cwd: "/workspace", complete: vi.fn(),
			executionWorlds: [lifecycleWorld(actorFallbackSettled, finishTurn)] });
		const failure = new Error("settlement failed");
		try {
			vi.spyOn(host.runtime, "finishTurn").mockRejectedValueOnce(failure);
			await expect(host.finishTurn("turn")).rejects.toBe(failure);
			expect(finishTurn).toHaveBeenCalledWith("turn");
		} finally { await host.dispose(); }
	});

	it("drains every world when a runtime world's turn cleanup fails", async () => {
		const failure = new Error("runtime cleanup failed");
		const runtime = lifecycleWorld(vi.fn(), vi.fn(async () => { throw failure; }));
		const fallback = { ...lifecycleWorld(vi.fn(), vi.fn()), id: "fallback", scope: "fallback" as const, isolation: "resource_snapshot" as const };
		const host = createSpeculativeActionHost("session", { cwd: "/workspace", complete: vi.fn(), executionWorlds: [runtime, fallback] });
		try {
			await expect(host.finishTurn("turn")).rejects.toMatchObject({ errors: [failure] });
			expect(fallback.finishTurn).toHaveBeenCalledExactlyOnceWith("turn");
		} finally { await host.dispose(); }
	});
});

function lifecycleWorld(actorFallbackSettled: () => Promise<void>, finishTurn: (turnID: string) => Promise<void>): AgentExecutionWorld {
	return { id: "test-world", scope: "runtime", isolation: "runtime_sandbox", actorFallbackSettled, finishTurn,
		speculation: { capabilities: [], execute: vi.fn() } };
}
