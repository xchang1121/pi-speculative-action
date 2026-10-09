import { calculateContextTokens, estimateContextTokens } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { DrafterRequestSettings } from "./common.ts";
import { stableValueHash } from "./stable-value-hash.ts";

type BudgetPolicy = Pick<DrafterRequestSettings, "drafterTaskMaxRequests" | "drafterTaskMaxTokens">;
export type DrafterBudgetSnapshot = Readonly<ReturnType<DrafterTaskBudget["snapshot"]>>;
type Suppression = "drafter_request_limit" | "drafter_token_limit" | "drafter_context_limit";
type OnSkipped = (reason: Suppression, detail?: string) => void;

/** One user task shares reservations across concurrent roots, continuations and Drafter probes. */
export class DrafterTaskBudget {
	private state = this.empty();
	private finished = false;

	available(policy: BudgetPolicy, onSkipped?: OnSkipped): boolean {
		if (this.finished) { this.state = this.empty(); this.finished = false; }
		const state = this.state;
		if (state.requests < policy.drafterTaskMaxRequests && this.spent() < policy.drafterTaskMaxTokens) return true;
		this.skip(state.requests >= policy.drafterTaskMaxRequests ? "drafter_request_limit" : "drafter_token_limit", onSkipped);
		return false;
	}

	async run(input: {
		readonly model: Model<Api>;
		readonly context: Context;
		/** A larger estimate when request-only tools changed since the last reported Actor usage. */
		readonly inputTokens?: number;
		readonly options?: SimpleStreamOptions;
		readonly policy: BudgetPolicy;
		readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
		readonly started?: () => void;
		readonly onSkipped?: OnSkipped;
	}): Promise<AssistantMessage | undefined> {
		input.options?.signal?.throwIfAborted();
		if (!this.available(input.policy, input.onSkipped)) return undefined;
		const state = this.state, prompt = Math.max(drafterInputTokens(input.context), input.inputTokens ?? 0);
		const remaining = input.policy.drafterTaskMaxTokens - this.spent();
		const output = Math.min(input.options?.maxTokens ?? input.model.maxTokens, input.model.maxTokens, input.model.contextWindow - prompt);
		const maxTokens = Math.floor(Math.min(output, remaining - prompt));
		if (!(maxTokens > 0)) return this.skip(prompt >= input.model.contextWindow ? "drafter_context_limit" : "drafter_token_limit", input.onSkipped,
			JSON.stringify({ promptTokens: prompt, remainingTokens: remaining, contextWindow: input.model.contextWindow }));
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
	snapshot() { return { ...this.state }; }
	private skip(reason: Suppression, onSkipped?: OnSkipped, detail?: string): undefined { this.state.skippedRequests++; onSkipped?.(reason, detail); return undefined; }
	private spent() { return this.state.reportedTokens + this.state.unreportedTokens + this.state.reservedTokens; }
	private empty() { return { requests: 0, reportedTokens: 0, unreportedTokens: 0, reservedTokens: 0, skippedRequests: 0 }; }
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

export function drafterInputTokens(context: Context): number {
	const estimate = estimateContextTokens(context.messages);
	return estimate.tokens + (estimate.lastUsageIndex === null
		? Math.ceil(((context.systemPrompt?.length ?? 0) + JSON.stringify(context.tools ?? []).length) / 4) : 0);
}
