import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Model } from "@earendil-works/pi-ai";
import { type AgentPosixClient, parseFsSnapshotId, parseThinkThreadId } from "@thinkthread/agent-posix";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";

describe("ThinkThread through the production speculative host", () => {
	it.each([false, true])("reuses an Actor read across turns only while fresh (stale=%s)", async (stale) => {
		let snapshotSequence = 0;
		let changed = false;
		const snapshotCreate = vi.fn(async () => ({
			snapshotId: parseFsSnapshotId(`fsnap-00000000-0000-4000-8000-${String(++snapshotSequence).padStart(12, "0")}`),
			ownerThinkthreadId: parseThinkThreadId("tt-00000000-0000-4000-8000-000000000001"),
			createdAtUnixMs: snapshotSequence,
			logicalBytes: 1,
		}));
		const snapshotRemove = vi.fn(async () => ({}));
		const verify = vi.fn(async () => ({
			status: changed ? "stale" : "matched",
			durationMs: 0, comparedEntries: 1, comparedBytes: 1,
		}));
		const run = vi.fn(async () => { throw new Error("Authoritative capture must not execute another tool"); });
		const client = {
			selfView: async () => ({ capabilities: [{ id: "thinkthread.fs.self", version: 1 }] }),
			fs: { stat: async () => ({}), snapshotCreate, snapshotRemove, verify, run, requestClose: async () => ({}) },
		} as unknown as AgentPosixClient;
		const world = createThinkThreadExecutionWorld({ clientFactory: () => client, runnerFingerprint: "test" });
		const events: SpeculativeActionEvent<string>[] = [];
		const options = {
			cwd: process.env.THINKTHREAD_FS ?? "/workspace",
			getSettings: () => ({ enabled: true, drafterEnabled: false, tools: ["read"], patternAware: { enabled: false } }),
			complete: async () => { throw new Error("No model requests expected"); },
			preflight: () => true,
			executionWorlds: [world],
			onEvent: (event: SpeculativeActionEvent<string>) => { events.push(event); },
		};
		const host = withThinkThreadProfileLifecycle(createSpeculativeActionHost("session", options), world, options);
		const schema = Type.Object({ path: Type.String() });
		const executor = vi.fn(async () => ({ content: [{ type: "text" as const, text: changed ? "new" : "original" }], details: {} }));
		const tool: AgentTool<typeof schema> = { name: "read", label: "read", description: "read", parameters: schema, execute: executor };
		const actorModel: Model<"openai-responses"> = {
			id: "test", name: "test", api: "openai-responses", provider: "openai", baseUrl: "https://example.invalid",
			reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024,
		};
		const input = (turnID: string) => ({ turnID, actorModel, context: { messages: [], tools: [tool] }, tools: [tool], actorOptions: undefined });
		try {
			await host.startTurn(input("first"));
			const first = await host.execute({ turnID: "first", id: "first-read", tool: "read", args: { path: "notes.txt" }, tools: [tool] }, undefined, executor);
			await host.finishTurn("first");
			changed = stale;
			await host.startTurn(input("second"));
			const second = await host.execute({ turnID: "second", id: "second-read", tool: "read", args: { path: "notes.txt" }, tools: [tool] }, undefined, executor);
			await host.finishTurn("second", true);
			await vi.waitFor(() => expect(events.filter((event) => event.type === "actor_action")).toHaveLength(2));
			expect(executor).toHaveBeenCalledTimes(stale ? 2 : 1);
			expect(snapshotCreate).toHaveBeenCalledTimes(stale ? 2 : 1);
			expect(verify).toHaveBeenCalled();
			expect(run).not.toHaveBeenCalled();
			if (!stale) expect(second).toBe(first);
			else expect(second.content).toEqual([{ type: "text", text: "new" }]);
			const last = events.filter((event) => event.type === "actor_action").at(-1);
			expect(last?.settlement.provider.kind).toBe(stale ? "actor" : "speculative");
		} finally {
			await host.dispose();
		}
		expect(snapshotRemove).toHaveBeenCalledTimes(snapshotSequence);
	});
});
