import { describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeActionHost } from "../src/agent-integration.ts";
import type { ThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";

describe("ThinkThread profile extension lifecycle", () => {
	it("prepares each turn and invalidates BASE only after mutating Actor fallbacks", async () => {
		const host = hostFixture();
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host.value, world.value, hostOptions());

		await wrapped.startTurn(startInput("turn-1"));
		expect(world.prepare).toHaveBeenCalledWith({ cwd: "/workspace" });
		expect(world.beginTurn).toHaveBeenCalledWith("turn-1");
		expect(world.beginTurn.mock.invocationCallOrder[0]).toBeLessThan(host.startTurn.mock.invocationCallOrder[0]!);

		await wrapped.actual(actualInput("read"));
		expect(world.actorFallbackSettled).not.toHaveBeenCalled();
		await wrapped.actual(actualInput("write"));
		await wrapped.actual(actualInput("bash"));
		expect(world.actorFallbackSettled).toHaveBeenCalledTimes(2);

		await wrapped.finishTurn("turn-1");
		expect(host.finishTurn).toHaveBeenCalledWith("turn-1");
		expect(world.finishTurn).toHaveBeenCalledWith("turn-1");
	});

	it("finishes the world when host turn startup fails", async () => {
		const startupError = new Error("host startup failed");
		const host = hostFixture({ startTurnError: startupError });
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host.value, world.value, hostOptions());

		await expect(wrapped.startTurn(startInput("turn-failed"))).rejects.toBe(startupError);
		expect(world.finishTurn).toHaveBeenCalledWith("turn-failed");
	});

	it("finishes the world when host turn settlement fails", async () => {
		const settlementError = new Error("host settlement failed");
		const host = hostFixture({ finishTurnError: settlementError });
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host.value, world.value, hostOptions());

		await wrapped.startTurn(startInput("turn-failed"));
		await expect(wrapped.finishTurn("turn-failed")).rejects.toBe(settlementError);
		expect(world.finishTurn).toHaveBeenCalledWith("turn-failed");
	});

	it("does not initialize ThinkThread when its pre-execution layer is disabled", async () => {
		const host = hostFixture();
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host.value, world.value, {
			...hostOptions(),
			speculativeExecutionWorldEnabled: () => false,
		});

		await wrapped.startTurn(startInput("turn-actor"));
		await wrapped.actual(actualInput("write"));
		await wrapped.finishTurn("turn-actor");
		expect(host.startTurn).toHaveBeenCalledOnce();
		expect(world.prepare).not.toHaveBeenCalled();
		expect(world.beginTurn).not.toHaveBeenCalled();
		expect(world.actorFallbackSettled).not.toHaveBeenCalled();
		expect(world.finishTurn).not.toHaveBeenCalled();
	});

	it.each(["write", "edit", "bash"])("invalidates %s before the new host.execute reports Actor settlement", async (tool) => {
		const options = hostOptions();
		const host = createSpeculativeActionHost("session", options);
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host, world.value, options);
		const actual = vi.spyOn(host.runtime, "actual");
		const output = { content: [{ type: "text" as const, text: "Actor result" }], details: {} };
		const executor = vi.fn(async () => output);
		try {
			await expect(wrapped.execute({ turnID: "turn", tool, args: {}, tools: [] }, undefined, executor)).resolves.toBe(output);
			expect(executor).toHaveBeenCalledOnce();
			expect(actual).toHaveBeenCalledOnce();
			expect(world.actorFallbackSettled).toHaveBeenCalledOnce();
			expect(world.actorFallbackSettled.mock.invocationCallOrder[0]).toBeLessThan(actual.mock.invocationCallOrder[0]!);
		} finally {
			await wrapped.dispose();
		}
	});

	it("does not invalidate a read or a speculative hit", async () => {
		const options = hostOptions();
		const host = createSpeculativeActionHost("session", options);
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host, world.value, options);
		const result = { content: [], details: {} };
		const executor = vi.fn(async () => result);
		try {
			await wrapped.execute({ turnID: "turn", tool: "read", args: {}, tools: [] }, undefined, executor);
			vi.spyOn(host.runtime, "consume").mockResolvedValueOnce({ result, isError: false });
			await expect(wrapped.execute({ turnID: "turn", tool: "write", args: {}, tools: [] }, undefined, executor)).resolves.toBe(result);
			expect(executor).toHaveBeenCalledOnce();
			expect(world.actorFallbackSettled).not.toHaveBeenCalled();
		} finally {
			await wrapped.dispose();
		}
	});

	it("invalidates partially failed Actor writes without replacing their error", async () => {
		const options = hostOptions();
		const host = createSpeculativeActionHost("session", options);
		const world = worldFixture();
		const wrapped = withThinkThreadProfileLifecycle(host, world.value, options);
		const failure = new Error("Actor failed after writing");
		world.actorFallbackSettled.mockRejectedValueOnce(new Error("cleanup failed"));
		try {
			await expect(wrapped.execute({ turnID: "turn", tool: "bash", args: {}, tools: [] }, undefined, async () => { throw failure; }))
				.rejects.toBe(failure);
			expect(world.actorFallbackSettled).toHaveBeenCalledOnce();
		} finally {
			await wrapped.dispose();
		}
	});
});

function hostFixture(options: { readonly startTurnError?: Error; readonly finishTurnError?: Error } = {}) {
	const startTurn = vi.fn(async () => {
		if (options.startTurnError) throw options.startTurnError;
	});
	const finishTurn = vi.fn(async () => {
		if (options.finishTurnError) throw options.finishTurnError;
	});
	const value = {
		startTurn,
		actual: vi.fn(async () => undefined),
		finishTurn,
	} as unknown as SpeculativeActionHost;
	return { value, startTurn, finishTurn };
}

function worldFixture() {
	const prepare = vi.fn(async () => undefined);
	const beginTurn = vi.fn(async () => undefined);
	const actorFallbackSettled = vi.fn(async () => undefined);
	const finishTurn = vi.fn(async () => undefined);
	const value = {
		id: "thinkthread",
		speculation: { prepare },
		beginTurn,
		actorFallbackSettled,
		finishTurn,
	} as unknown as ThinkThreadExecutionWorld;
	return { value, prepare, beginTurn, actorFallbackSettled, finishTurn };
}

function hostOptions(): CreateSpeculativeActionHostOptions {
	return { cwd: "/workspace", complete: async () => { throw new Error("No model calls expected"); } };
}

function startInput(turnID: string): Parameters<SpeculativeActionHost["startTurn"]>[0] {
	return { turnID } as Parameters<SpeculativeActionHost["startTurn"]>[0];
}

function actualInput(tool: string): Parameters<SpeculativeActionHost["actual"]>[0] {
	return { tool } as Parameters<SpeculativeActionHost["actual"]>[0];
}
