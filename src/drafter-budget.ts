import { calculateContextTokens, estimateContextTokens } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { DrafterRequestSettings } from "./common.ts";

type BudgetPolicy = Pick<DrafterRequestSettings, "drafterTaskMaxRequests" | "drafterTaskMaxTokens">;
export interface DrafterBudgetSnapshot {
	readonly requests: number;
	readonly reportedTokens: number;
	readonly unreportedTokens: number;
	readonly reservedTokens: number;
	readonly skippedRequests: number;
}

/** One user task shares reservations across concurrent roots, continuations and Drafter probes. */
export class DrafterTaskBudget {
	private state = this.empty();
	private finished = false;

	available(policy: BudgetPolicy): boolean {
		if (this.finished) { this.state = this.empty(); this.finished = false; }
		const state = this.state;
		if (state.requests < policy.drafterTaskMaxRequests &&
			state.reportedTokens + state.unreportedTokens + state.reservedTokens < policy.drafterTaskMaxTokens) return true;
		state.skippedRequests++;
		return false;
	}

	async run(input: {
		readonly model: Model<Api>;
		readonly context: Context;
		readonly options?: SimpleStreamOptions;
		readonly policy: BudgetPolicy;
		readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
		readonly started?: () => void;
	}): Promise<AssistantMessage | undefined> {
		input.options?.signal?.throwIfAborted();
		if (!this.available(input.policy)) return undefined;
		const state = this.state, prompt = drafterInputTokens(input.context);
		const remaining = input.policy.drafterTaskMaxTokens - state.reportedTokens - state.unreportedTokens - state.reservedTokens;
		const maxTokens = Math.floor(Math.min(input.options?.maxTokens ?? input.model.maxTokens, input.model.maxTokens,
			input.model.contextWindow - prompt, remaining - prompt));
		if (!(maxTokens > 0)) { state.skippedRequests++; return undefined; }
		const reservation = prompt + maxTokens;
		state.requests++;
		state.reservedTokens += reservation;
		let reported = false;
		try {
			input.started?.();
			const message = await input.complete(input.model, input.context, { ...input.options, maxTokens });
			const tokens = calculateContextTokens(message.usage);
			if (Number.isFinite(tokens) && tokens >= 0 && (tokens > 0 || !["error", "aborted"].includes(message.stopReason))) {
				state.reportedTokens += tokens; reported = true;
			}
			return message;
		} finally {
			state.reservedTokens -= reservation;
			// Missing usage can still be billable; keep the reservation charged, including aborts and failures.
			if (!reported) state.unreportedTokens += reservation;
		}
	}

	finishTask(): void { this.finished = true; }
	snapshot(): DrafterBudgetSnapshot { return { ...this.state }; }
	private empty() { return { requests: 0, reportedTokens: 0, unreportedTokens: 0, reservedTokens: 0, skippedRequests: 0 }; }
}

export function drafterInputTokens(context: Context): number {
	const estimate = estimateContextTokens(context.messages);
	return estimate.tokens + (estimate.lastUsageIndex === null
		? Math.ceil(((context.systemPrompt?.length ?? 0) + JSON.stringify(context.tools ?? []).length) / 4) : 0);
}
