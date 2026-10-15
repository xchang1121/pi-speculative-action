import type { AgentTool } from "@earendil-works/pi-agent-core";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { expect } from "vitest";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { testModel } from "./model.ts";

/** Exercise the same tool outlet as the extension; only the external model response is scripted. */
export function systemHost(cwd: string, tools: AgentTool[], options: Partial<CreateSpeculativeActionHostOptions> = {},
	settings: SpeculativeAgentSettingsInput = {}, sessionID = "system") {
	const events: SpeculativeActionEvent<string>[] = [], fallbacks: string[] = [];
	let turnID = "first", calls = 0;
	const host = createSpeculativeActionHost(sessionID, {
		cwd, complete: async () => fauxAssistantMessage("done"),
		preflight: () => true,
		resources: { initial: { cpuCount: 2, idleCpuCount: 2 }, sample: () => new Promise(() => {}) },
		getSettings: () => ({ enabled: true, drafterEnabled: false, candidateLimit: 1, maxConcurrentActions: 2,
			predictionTimeoutMs: 10_000, patternAware: { enabled: false }, prepareObservedOperations: false, ...settings }),
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
		executionWorlds: [createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { maxBytes: () => 8 * 1024 * 1024 })],
		...options,
		onEvent: event => { events.push(event); return options.onEvent?.(event); },
	});
	const call = (tool: string, args: Record<string, unknown>, id = `call-${++calls}`) => ({ turnID, id, tool, args, tools });
	const authoritative = async (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id = "oracle", invocation = options.resolveInvocation?.(tool, args) ?? null) => {
		const bound = await invocation;
		if (bound?.authoritative) return (await bound.authoritative({ callID: id, args, signal: signal ?? new AbortController().signal })).result;
		return tools.find(candidate => candidate.name === tool)!.execute(id, args as never, signal);
	};
	return {
		host, events, fallbacks, oracle: authoritative,
		start: async (id = "first") => {
			turnID = id;
			await host.startTurn({ turnID, tools, actorOptions: {}, actorModel: testModel("actor", { contextWindow: 262144 }),
				context: { systemPrompt: "Use tools to inspect and edit these files.", messages: [], tools } });
		},
		preview: (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id?: string) => host.previewActorCall(call(tool, args, id), signal),
		execute: (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id?: string) =>
			host.execute(call(tool, args, id), signal, operation => {
				fallbacks.push(tool);
				return authoritative(tool, operation.input as Record<string, unknown>, operation.signal, operation.callID, operation.invocation ?? null);
			}),
		ready: async (count = 1) => {
			try { await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded").length,
				{ timeout: 10_000 }).toBeGreaterThanOrEqual(count); }
			catch (cause) { throw new Error(JSON.stringify(events), { cause }); }
		},
		finish: (terminal = false) => host.finishTurn(turnID, terminal),
		dispose: () => host.dispose(),
	};
}
