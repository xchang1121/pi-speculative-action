import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { DrafterTaskBudget } from "./drafter-budget.ts";
import type { ActualToolCall, SpeculativePlanSource, SpeculativeActionRuntime, RuntimeEventMap, TurnInput } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

export interface DraftOptionsContext {
	readonly actorModel: Model<Api>;
	readonly draftModel: Model<Api>;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly signal: AbortSignal;
}

export type DraftModelSelection =
	| Model<Api>
	| ((actorModel: Model<Api>) => Model<Api> | undefined | Promise<Model<Api> | undefined>);

/** Model services supplied by the host; prediction hints remain private source wiring. */
export interface DrafterModelOptions {
	/** Defaults to the Actor model when omitted or unresolved. */
	readonly draftModel?: DraftModelSelection;
	/** Resolve request options, including credentials for a different provider. */
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	/** Share reservations with other model requests in the same user task. */
	readonly drafterBudget?: DrafterTaskBudget;
}

export interface AgentStartInput extends Pick<DraftOptionsContext, "actorModel" | "actorOptions">, Omit<TurnInput<string>, "terminal"> {
	readonly context: Context;
	readonly tools: readonly AgentTool[];
}

export interface AgentConsumeInput extends Omit<ActualToolCall, "input">, TurnInput<string> {
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
}

export interface AgentStateData {
	readonly tools: ReadonlyMap<string, AgentTool>;
	readonly schemaHashes: Readonly<Record<string, string>>;
}

export type AgentPlanSource = SpeculativePlanSource<
	string,
	ToolSettlement,
	AgentStartInput,
	AgentStateData
>;

export type AgentRuntime = SpeculativeActionRuntime<string, ToolSettlement, AgentStartInput, AgentConsumeInput, AgentConsumeInput, AgentStateData>;
export type AgentRuntimeEvents = RuntimeEventMap<string, ToolSettlement, AgentStartInput, AgentConsumeInput, AgentStateData>;

export function agentBatchKey(sessionID: string, turnID: string): string {
	return JSON.stringify([sessionID, turnID]);
}

export function definitionSchemaHashes(
	definitions: readonly { readonly name: string; readonly inputSchema?: unknown }[],
): Readonly<Record<string, string>> {
	return Object.fromEntries(
		definitions.map((definition) => [definition.name, stableValueHash(definition.inputSchema ?? null)]),
	);
}
