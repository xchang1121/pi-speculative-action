import type { AgentTool } from "@earendil-works/pi-agent-core";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { expect } from "vitest";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { MaterializedSpeculativeCandidate, SpeculativeActionEvent } from "../src/runtime.ts";
import type { ToolComputationTiming } from "../src/task-timing.ts";
import { testModel } from "./model.ts";

type SystemEvent<Type extends SpeculativeActionEvent<string>["type"]> = Extract<SpeculativeActionEvent<string>, { type: Type }>;

export const idleResources = (cpuCount: number) => ({ initial: { cpuCount, idleCpuCount: cpuCount }, sample: () => new Promise<never>(() => {}) });

export const resourceWorld = (semantics = PI_ACTION_SEMANTICS, maxBytes = () => 1024 * 1024) => createResourceSnapshotExecutionWorld(semantics, { maxBytes });

/** Exercise the same tool outlet as the extension; only the external model response is scripted. */
export function systemHost(cwd: string, tools: AgentTool[], options: Partial<CreateSpeculativeActionHostOptions> = {},
	settings: SpeculativeAgentSettingsInput = {}, sessionID = "system") {
	const events: SpeculativeActionEvent<string>[] = [], fallbacks: string[] = [];
	const materialized: MaterializedSpeculativeCandidate<string>[] = [];
	const ofType = <Type extends SpeculativeActionEvent<string>["type"]>(type: Type) => events.filter((event): event is SystemEvent<Type> => event.type === type);
	const completed = (file?: string, source?: string): SystemEvent<"candidate">[] => ofType("candidate").filter(event => event.state.status === "succeeded" &&
		(!source || event.candidate.source === source) && (!file || JSON.parse(event.candidate.predictedAction).input.path === file));
	let turnID = "first", calls = 0;
	const host = createSpeculativeActionHost(sessionID, {
		cwd, complete: async () => fauxAssistantMessage("done"),
		preflight: () => true,
		resources: idleResources(2),
		getSettings: () => ({ enabled: true, drafterEnabled: false, candidateLimit: 1, maxConcurrentActions: 2,
			predictionTimeoutMs: 10_000, patternAware: { enabled: false }, prepareObservedOperations: false, ...settings }),
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
		executionWorlds: [resourceWorld(PI_ACTION_SEMANTICS, () => 8 * 1024 * 1024)],
		...options,
		onEvent: event => { events.push(event); return options.onEvent?.(event); },
	});
	host.runtime.subscribe({ candidate_materialized: candidate => { materialized.push(candidate); } });
	const call = (tool: string, args: Record<string, unknown>, id = `call-${++calls}`) => ({ turnID, id, tool, args, tools });
	const authoritative = async (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id = "oracle", invocation = options.resolveInvocation?.(tool, args) ?? null) => {
		const bound = await invocation;
		if (bound?.authoritative) return (await bound.authoritative({ callID: id, args, signal: signal ?? new AbortController().signal })).result;
		return tools.find(candidate => candidate.name === tool)!.execute(id, args as never, signal);
	};
	return {
		host, events, fallbacks, materialized, completed, oracle: authoritative,
		get drafter() { return host.drafterBudgetSnapshot(); },
		get actors(): SystemEvent<"actor_action">[] { return ofType("actor_action"); },
		get predictions(): SystemEvent<"prediction">[] { return ofType("prediction"); },
		start: (id = "first") => host.startTurn({ turnID: turnID = id, tools, actorOptions: {}, actorModel: testModel("actor", { contextWindow: 262144 }),
			context: { systemPrompt: "Use tools to inspect and edit these files.", messages: [], tools } }),
		preview: (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id?: string) => host.previewActorCall(call(tool, args, id), signal),
		execute: (tool: string, args: Record<string, unknown>, signal?: AbortSignal, id?: string) =>
			host.execute(call(tool, args, id), signal, operation => {
				fallbacks.push(tool);
				return authoritative(tool, operation.input as Record<string, unknown>, operation.signal, operation.callID, operation.invocation ?? null);
			}),
		ready: (count: number | readonly string[] = 1, source?: string) => expect.poll(() => typeof count === "number" ? completed(undefined, source).length : count.filter(file => completed(file, source).length).length,
			{ timeout: 10_000 }).toBeGreaterThanOrEqual(typeof count === "number" ? count : count.length).catch(cause => { throw new Error(JSON.stringify(events), { cause }); }),
		finish: (terminal = false) => host.finishTurn(turnID, terminal),
		dispose: () => host.dispose(),
	};
}

export function expectHiddenCompute(timing: ToolComputationTiming, consumed: boolean) {
	if (consumed) expect(timing.hiddenComputeMs).toBeGreaterThan(0); else expect(timing.hiddenComputeMs).toBe(0);
	expect(timing.hiddenComputeMs).toBeLessThanOrEqual(timing.toolComputeMs!);
}
