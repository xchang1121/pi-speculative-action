import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Model } from "@earendil-works/pi-ai";
import { type AgentPosixClient } from "@thinkthread/agent-posix";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";

describe("ThinkThread profile with shared Actor observation", () => {
	it.each(["stable", "changed", "ABA"])("proves the live Actor window before cross-turn reuse: %s", async (change) => {
		const cwd = await mkdtemp(path.join(os.tmpdir(), "thinkthread-observation-"));
		const file = path.join(cwd, "notes.txt");
		await writeFile(file, "A");
		const snapshotCreate = vi.fn(async () => { throw new Error("Actor observation must use the shared resource proof"); });
		const client = {
			selfView: async () => ({ capabilities: [{ id: "thinkthread.fs.self", version: 1 }] }),
			fs: { stat: async () => ({}), snapshotCreate },
		} as unknown as AgentPosixClient;
		const world = createThinkThreadExecutionWorld({ clientFactory: () => client, runnerFingerprint: "test" });
		const options = {
			cwd,
			getSettings: () => ({ enabled: true, drafterEnabled: false, tools: ["read"], patternAware: { enabled: false } }),
			complete: async () => { throw new Error("No model requests expected"); },
			preflight: () => true,
			executionWorlds: [world],
		};
		const host = withThinkThreadProfileLifecycle(createSpeculativeActionHost("session", options), world, options);
		const executor = vi.fn(async () => {
			const restore = change === "ABA" && executor.mock.calls.length === 1;
			if (restore) await writeFile(file, "B");
			const text = await readFile(file, "utf8");
			if (restore) await writeFile(file, "A");
			return { content: [{ type: "text" as const, text }], details: {} };
		});
		const schema = Type.Object({ path: Type.String() });
		const tool: AgentTool<typeof schema> = { name: "read", label: "read", description: "read", parameters: schema, execute: executor };
		const actorModel: Model<"openai-responses"> = {
			id: "test", name: "test", api: "openai-responses", provider: "openai", baseUrl: "https://example.invalid",
			reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024,
		};
		const execute = async (turnID: string) => {
			await host.startTurn({ turnID, actorModel, context: { messages: [], tools: [tool] }, tools: [tool], actorOptions: undefined });
			const result = await host.execute({ turnID, id: turnID, tool: "read", args: { path: "notes.txt" }, tools: [tool] }, undefined, executor);
			await host.finishTurn(turnID);
			return result;
		};
		try {
			const first = await execute("first");
			expect(first.content).toEqual([{ type: "text", text: change === "ABA" ? "B" : "A" }]);
			if (change === "changed") await writeFile(file, "B");
			const second = await execute("second");
			expect(second.content).toEqual([{ type: "text", text: change === "changed" ? "B" : "A" }]);
			expect(executor).toHaveBeenCalledTimes(change === "stable" ? 1 : 2);
			expect(snapshotCreate).not.toHaveBeenCalled();
		} finally {
			await host.dispose();
			await rm(cwd, { recursive: true, force: true });
		}
	});
});
