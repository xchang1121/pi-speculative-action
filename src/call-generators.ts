import type { ActionEffect, ActionKey } from "./action-semantics.ts";
import type { AgentStartInput, AgentStateData } from "./agent-integration.ts";
import type { ExecutionSession, SpeculativeRun } from "./execution-session.ts";
import type { ToolSettlement } from "./execution-world.ts";
import type {
	MaterializedSpeculativeCandidate,
	PredictionSettlement,
	RuntimeTurnContext,
	SourceRequestKind,
} from "./settlement.ts";

export interface GeneratedCall {
	readonly key: ActionKey;
	readonly effect: ActionEffect;
	readonly run: SpeculativeRun<ToolSettlement>;
}
export interface CompletedCall extends GeneratedCall {
	readonly output: ToolSettlement;
}
export interface Prediction
	extends Pick<
		MaterializedSpeculativeCandidate<string>,
		| "tool"
		| "input"
		| "depth"
		| "horizon"
		| "conditionalProbability"
		| "empiricalProbability"
		| "adoptionProbability"
	> {
	readonly source: string;
	readonly group: string;
	readonly id: string;
	readonly latestHorizon?: number;
	readonly mode?: string;
	readonly feedback?: unknown;
	onIssued?(): void;
	onFeedback?(settlement: PredictionSettlement, served: boolean): void;
}
/** Private to the generators and their learning/telemetry; only concrete calls cross the session. */
export interface GeneratorTurn extends RuntimeTurnContext<AgentStartInput, AgentStateData> {
	readonly signal: AbortSignal;
	readonly session: ExecutionSession<ToolSettlement>;
	issue(
		prediction: Prediction,
		options?: { after?: string; previousWrite?: string; signal?: AbortSignal },
	): Promise<GeneratedCall | undefined>;
	peers(source: string, calls: readonly CompletedCall[]): void;
	request(
		source: string,
		kind: SourceRequestKind,
		signal: AbortSignal,
		produce: (signal: AbortSignal, report: (tokens: number) => void) => Promise<number>,
	): Promise<void>;
}
export interface CallGenerator {
	start(turn: GeneratorTurn): void | Promise<void>;
	peers?(turn: GeneratorTurn, source: string, calls: readonly CompletedCall[]): void | Promise<void>;
}
