import { describe, expect, it, vi } from "vitest";
import { buildPiActionKey } from "../src/action-semantics.ts";
import { effectCommitFailure, EffectTransactionCoordinator } from "../src/effect-transaction.ts";
import type { SpeculativeExecutionRoute } from "../src/execution-world.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";
import { cause, zeroValidationMetrics } from "../src/settlement.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";

interface Call {
	readonly sessionID: string;
	readonly turnID: string;
	readonly id: string;
	readonly tool: string;
	readonly args: Record<string, unknown>;
}

describe("execution-world commit rejection classification", () => {
	it.each(["classified", "unclassified", "compatibility_drift"])("preserves %s rejection through the transaction and Actor fallback", async (scenario) => {
		const events: SpeculativeActionEvent<string>[] = [];
		const route: SpeculativeExecutionRoute = {
			isolation: "runtime_sandbox", scope: "runtime", reuse: "exclusive_branch", backend: "test", fingerprint: "test:v1",
		};
		const failure = cause("freshness", "backend_conflict");
		const commit = vi.fn(async (): Promise<string> => {
			if (scenario === "compatibility_drift") return "speculative";
			throw scenario === "classified"
				? effectCommitFailure(new Error("changed"), "recoverable", "changed", failure)
				: effectCommitFailure(new Error("commit failed"), "recoverable");
		});
		const dispose = vi.fn(async () => {});
		const transactions = new EffectTransactionCoordinator<string>();
		const gateway = new ToolExecutionGateway<undefined, string>([]), actor = vi.fn(async () => "Actor");
		const runtime = makeStructuralSpeculativeActionRuntime<string, string, Call, Call, Call, undefined>({
			settings: () => ({ enabled: true, tools: ["write"], resourceCacheMaxEntries: 8, predictionTimeoutMs: 100 }),
			definitions: () => [{ name: "write" }],
			stateData: () => undefined,
			actionKey: (tool, args) => buildPiActionKey(tool, args, "/workspace"),
			resolveExecution: () => route,
			preflightCandidate: () => ({ ok: true }),
			actual: (call) => ({ id: call.id, tool: call.tool, input: call.args }),
			executeCandidate: async ({ action, callID }) => {
				const source = {
					backend: "test", output: "speculative", resources: ["a.txt"], capturedBytes: 1, executionMetrics: {},
					compatibility: scenario === "compatibility_drift" ? { status: "incompatible" as const, backend: "test", code: "sealed_incompatible" }
						: { status: "compatible" as const, backend: "test", executionFingerprint: action.executionFingerprint },
					validate: async () => ({ status: "valid" as const, metrics: zeroValidationMetrics() }),
					commit, dispose,
				};
				const transaction = await transactions.execute(transactions.begin({ tool: action.tool, callID, route }), async () => source);
				Object.assign(source.compatibility, { status: "compatible", executionFingerprint: action.executionFingerprint });
				return transaction;
			},
			onEvent: (event) => { events.push(event); },
		});
		const call: Call = { sessionID: "session", turnID: "turn", id: "write-1", tool: "write", args: { path: "a.txt", content: "a" } };
		try {
			await runtime.startTurn(call);
			await runtime.previewActorCall(call);
			await vi.waitFor(() => expect(events.some((event) => event.type === "candidate" && event.state.status === "succeeded")).toBe(true));
			await expect(gateway.executeAuthoritative({ tool: call.tool, input: call.args }, actor, {
				reuse: () => runtime.consume(call), settled: async (settlement) => {
					if (settlement.status === "succeeded") await runtime.actual({ ...call, ...settlement });
				},
			})).resolves.toBe("Actor");
			expect(actor).toHaveBeenCalledOnce();
			await runtime.finishTurn(call);
			await vi.waitFor(() => expect(events.some((event) => event.type === "actor_action")).toBe(true));
			const settlement = events.find((event) => event.type === "actor_action")?.settlement;
			expect(settlement?.rejections[0]?.cause).toMatchObject(scenario === "classified" ? failure : scenario === "compatibility_drift"
				? { stage: "compatibility", code: "backend_incompatible", detail: "sealed_incompatible" } : { stage: "commit", code: "world_commit_failed" });
			expect(settlement?.provider).toMatchObject({ kind: "actor", origin: "fallback" });
			expect(commit).toHaveBeenCalledTimes(scenario === "compatibility_drift" ? 0 : 1);
			expect(dispose).toHaveBeenCalledOnce();
		} finally {
			await runtime.dispose();
			await gateway.dispose();
		}
	});
});
