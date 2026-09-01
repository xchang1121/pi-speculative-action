import type { AgentPlanSource } from "./agent-runtime-types.ts";
import type { SelfSpeculationActionBridge } from "./self-speculation-action-bridge.ts";

export function createSelfSpeculationPlanSource(
	bridge: SelfSpeculationActionBridge | undefined,
): AgentPlanSource {
	return {
		id: "self-speculation",
		enabled: (settings) => bridge !== undefined && settings.sourceConfig?.selfSpeculationActionEnabled === true,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "actor_decision",
		propose: async ({ startInput, candidateNames, signal }) => {
			const batches = (await bridge?.waitForBatches(startInput.turnID, signal)) ?? [];
			const allowed = new Set(candidateNames);
			return batches
				.filter((batch) => batch.calls.length > 0 && batch.calls.every((call) => allowed.has(call.tool)))
				.map((batch) => ({
					id: `self-speculation:${startInput.turnID}:${batch.id}`,
					source: "self-speculation",
					revision: 0,
					actions: batch.calls.map((call) => ({
						id: call.id,
						type: "tool_call" as const,
						tool: call.tool,
						input: call.input,
						feedback: {
							batchID: batch.id,
							callID: call.id,
							callIndex: call.index,
							evidence: batch.evidence,
						},
					})),
				}));
		},
	};
}
