import { describe, expect, it, vi } from "vitest";
import { buildPiActionKey } from "../src/action-semantics.ts";
import { EffectTransactionCoordinator } from "../src/effect-transaction.ts";
import { type SpeculativeExecutionRoute, WorldCommitRejectedError } from "../src/execution-world.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";
import { cause, zeroValidationMetrics } from "../src/settlement.ts";

interface Call {
	readonly sessionID: string;
	readonly turnID: string;
	readonly id: string;
	readonly tool: string;
	readonly args: Record<string, unknown>;
}

describe("execution-world commit rejection classification", () => {
	it.each([true, false])("preserves classified rejection=%s through the shared transaction", async (classified) => {
		const events: SpeculativeActionEvent<string>[] = [];
		const route: SpeculativeExecutionRoute = {
			isolation: "runtime_sandbox", scope: "runtime", reuse: "exclusive_branch", backend: "test", fingerprint: "test:v1",
		};
		const failure = cause("freshness", "backend_conflict");
		const commit = vi.fn(async (): Promise<string> => {
			throw classified ? new WorldCommitRejectedError(failure, "changed") : new Error("commit failed");
		});
		const dispose = vi.fn(async () => {});
		const transactions = new EffectTransactionCoordinator<string>();
		const runtime = makeStructuralSpeculativeActionRuntime<string, string, Call, Call, Call, undefined>({
			settings: () => ({ enabled: true, tools: ["write"], resourceCacheMaxEntries: 8, predictionTimeoutMs: 100 }),
			definitions: () => [{ name: "write" }],
			stateData: () => undefined,
			actionKey: (tool, args) => buildPiActionKey(tool, args, "/workspace"),
			resolveExecution: () => route,
			preflightCandidate: () => ({ ok: true }),
			actual: (call) => ({ id: call.id, tool: call.tool, input: call.args }),
			executeCandidate: ({ action, callID }) => transactions.execute(
				transactions.begin({ tool: action.tool, callID, route }),
				async () => ({
					backend: "test", output: "speculative", resources: ["a.txt"], capturedBytes: 1, executionMetrics: {},
					compatibility: { status: "compatible", backend: "test", executionFingerprint: action.executionFingerprint },
					validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
					commit, dispose,
				}),
			),
			onEvent: (event) => { events.push(event); },
		});
		const call: Call = { sessionID: "session", turnID: "turn", id: "write-1", tool: "write", args: { path: "a.txt", content: "a" } };
		try {
			await runtime.startTurn(call);
			await runtime.previewActorCall(call);
			await vi.waitFor(() => expect(events.some((event) => event.type === "candidate" && event.state.status === "succeeded")).toBe(true));
			await expect(runtime.consume(call)).resolves.toBeUndefined();
			await runtime.actual({ ...call, durationMs: 1, output: "Actor" });
			await runtime.finishTurn(call);
			await vi.waitFor(() => expect(events.some((event) => event.type === "actor_action")).toBe(true));
			const settlement = events.find((event) => event.type === "actor_action")?.settlement;
			expect(settlement?.rejections[0]?.cause).toMatchObject(classified ? failure : { stage: "commit", code: "world_commit_failed" });
			expect(settlement?.provider).toMatchObject({ kind: "actor", origin: "fallback" });
			expect(commit).toHaveBeenCalledOnce();
			expect(dispose).toHaveBeenCalledOnce();
		} finally {
			await runtime.dispose();
		}
	});
});
