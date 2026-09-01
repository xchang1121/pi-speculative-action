import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { Api, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { SpeculativePlanSource } from "./runtime.ts";
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

export interface AgentStartInput {
	readonly sessionID: string;
	readonly turnID: string;
	readonly actorModel: Model<Api>;
	readonly context: Context;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly tools: readonly AgentTool[];
}

export interface AgentConsumeInput {
	readonly sessionID: string;
	readonly turnID: string;
	readonly id?: string;
	readonly tool: string;
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
	readonly terminal?: boolean;
}

export interface AgentStateData {
	readonly tools: ReadonlyMap<string, AgentTool>;
	readonly schemaHashes: Readonly<Record<string, string>>;
}

export type AgentPlanSource = SpeculativePlanSource<
	string,
	ToolSettlement,
	AgentStartInput,
	AgentConsumeInput,
	AgentStateData
>;

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
