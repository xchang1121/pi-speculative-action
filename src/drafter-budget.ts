import { calculateContextTokens } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { stableValueHash } from "./stable-value-hash.ts";

export interface DrafterBudgetSnapshot {
	readonly requests: number;
	readonly reportedTokens: number;
	readonly unreportedRequests: number;
}

/** Shared usage accounting; request limits belong to the host and provider. */
export class DrafterTaskBudget {
	private state = this.empty();
	private finished = false;

	async run(input: {
		readonly model: Model<Api>;
		readonly context: Context;
		readonly options?: SimpleStreamOptions;
		readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	}): Promise<AssistantMessage> {
		input.options?.signal?.throwIfAborted();
		if (this.finished) { this.state = this.empty(); this.finished = false; }
		const state = this.state;
		state.requests++; state.unreportedRequests++;
		const message = await input.complete(input.model, input.context, input.options);
		const tokens = calculateContextTokens(message.usage);
		if (Number.isFinite(tokens) && tokens >= 0 && (tokens > 0 || !["error", "aborted"].includes(message.stopReason))) {
			state.reportedTokens += tokens; state.unreportedRequests--;
		}
		return message;
	}

	finishTask(): void { this.finished = true; }
	snapshot(): DrafterBudgetSnapshot { return { ...this.state }; }
	private empty() { return { requests: 0, reportedTokens: 0, unreportedRequests: 0 }; }
}

/** Probability calibration follows actual tool context and workflow shape, never argument churn. */
export function drafterOpportunityKey(model: Model<Api>, context: Context, schemas?: Readonly<Record<string, string>>,
	hints: readonly { readonly tool: string; readonly horizon?: number }[] = [], kind = "root"): string {
	const previous = [...context.messages].reverse().find(message => message.role === "assistant" && message.content.some(part => part.type === "toolCall"));
	const calls = previous?.role === "assistant" ? [...new Set(previous.content.flatMap(part => part.type === "toolCall"
		? [part.name] : []))].sort() : [];
	return JSON.stringify([model.provider, model.api, model.baseUrl, model.id, kind, stableValueHash(schemas ?? context.tools ?? []), calls,
		[...new Set(hints.map(hint => JSON.stringify([hint.tool, hint.horizon ?? 0])))].sort()]);
}
