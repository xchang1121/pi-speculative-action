import { calculateContextTokens, estimateContextTokens } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { DrafterRequestSettings } from "./common.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import { ModelRequestNotSent } from "./model-request.ts";

type BudgetPolicy = Pick<DrafterRequestSettings, "drafterTaskMaxRequests" | "drafterTaskMaxTokens"> & Partial<Pick<DrafterRequestSettings, "drafterBudgetHorizonTurns" | "drafterBudgetInitialFraction">>;
export interface DrafterBudgetSnapshot {
	readonly requests: number;
	readonly reportedTokens: number;
	readonly unreportedTokens: number;
	/** Usage is unknown for these requests; an uncapped output has no finite reservation estimate. */
	readonly unreportedRequests: number;
	readonly reservedTokens: number;
	readonly skippedRequests: number;
}
type Suppression = "drafter_request_limit" | "drafter_token_limit" | "drafter_context_limit" | "drafter_progress_reserve";
type OnSkipped = (reason: Suppression, detail?: string) => void;

/** One user task shares reservations across concurrent roots, continuations and Drafter probes. */
export class DrafterTaskBudget {
	private state = this.empty();
	private finished = false;
	private taskScopes = 0;
	private turns = new Set<string>();
	/** Keep one host task's allowance across internal SDK completion/recovery boundaries. */
	async withTask<Value>(run: () => Promise<Value>): Promise<Value> {
		this.resetFinished(); this.taskScopes++;
		try { return await run(); }
		finally { if (--this.taskScopes === 0) this.finished = true; }
	}
	beginTurn(turnID: string): void { this.resetFinished(); this.turns.add(turnID); }
	private resetFinished() { if (this.finished) { this.state = this.empty(); this.turns.clear(); this.finished = false; } }
	private allowance(policy: BudgetPolicy) {
		const initial = policy.drafterBudgetInitialFraction ?? 1, horizon = policy.drafterBudgetHorizonTurns ?? 1;
		return Math.min(1, initial + (1 - initial) * Math.max(0, this.turns.size - 1) / Math.max(1, horizon - 1));
	}

	available(policy: BudgetPolicy, onSkipped?: OnSkipped): boolean {
		this.resetFinished();
		const state = this.state;
		const requests = policy.drafterTaskMaxRequests ?? Infinity, tokens = policy.drafterTaskMaxTokens ?? Infinity;
		if (state.requests < requests && this.spent() < tokens) {
			if ((policy.drafterTaskMaxRequests === null || state.requests < Math.ceil(requests * this.allowance(policy))) &&
				(policy.drafterTaskMaxTokens === null || this.spent() < tokens * this.allowance(policy))) return true;
			this.skip("drafter_progress_reserve", onSkipped); return false;
		}
		this.skip(state.requests >= requests ? "drafter_request_limit" : "drafter_token_limit", onSkipped);
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
		readonly onSkipped?: OnSkipped;
	}): Promise<AssistantMessage | undefined> {
		input.options?.signal?.throwIfAborted();
		if (!this.available(input.policy, input.onSkipped)) return undefined;
		const state = this.state, prompt = Math.max(drafterInputTokens(input.context), input.inputTokens ?? 0);
		const remaining = input.policy.drafterTaskMaxTokens === null ? Infinity : input.policy.drafterTaskMaxTokens * this.allowance(input.policy) - this.spent();
		const contextRemaining = input.model.contextWindow > 0 ? input.model.contextWindow - prompt : Infinity;
		const requested = input.options?.maxTokens;
		const output = requested === undefined ? Infinity : Math.min(requested, input.model.maxTokens > 0 ? input.model.maxTokens : Infinity);
		// Without an explicit output or task cap, leave the request length to the provider.
		const cap = Math.floor(Math.min(output, remaining - prompt, requested === undefined ? Infinity : contextRemaining));
		if (!(cap > 0) || !(contextRemaining > 0)) return this.skip(contextRemaining <= 0 ? "drafter_context_limit" : "drafter_token_limit", input.onSkipped,
			JSON.stringify({ promptTokens: prompt, remainingTokens: remaining, contextWindow: input.model.contextWindow }));
		const maxTokens = Number.isFinite(cap) ? cap : undefined;
		const reservation = prompt + (maxTokens ?? 0);
		state.requests++;
		state.reservedTokens += reservation;
		let reported = false, notSent = false;
		try {
			const message = await input.complete(input.model, input.context, { ...input.options, maxTokens });
			const tokens = calculateContextTokens(message.usage);
			if (Number.isFinite(tokens) && tokens >= 0 && (tokens > 0 || !["error", "aborted"].includes(message.stopReason))) {
				state.reportedTokens += tokens; reported = true;
			}
			return message;
		} catch (error) {
			if (error instanceof ModelRequestNotSent) { notSent = true; state.requests--; }
			throw error;
		} finally {
			state.reservedTokens -= reservation;
			// Missing usage can still be billable; keep the reservation charged, including aborts and failures.
			if (!reported && !notSent) { state.unreportedTokens += reservation; state.unreportedRequests++; }
		}
	}

	finishTask(): void { if (!this.taskScopes) this.finished = true; }
	snapshot(): DrafterBudgetSnapshot { return { ...this.state }; }
	private skip(reason: Suppression, onSkipped?: OnSkipped, detail?: string): undefined { this.state.skippedRequests++; onSkipped?.(reason, detail); return undefined; }
	private spent() { return this.state.reportedTokens + this.state.unreportedTokens + this.state.reservedTokens; }
	private empty() { return { requests: 0, reportedTokens: 0, unreportedTokens: 0, unreportedRequests: 0, reservedTokens: 0, skippedRequests: 0 }; }
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
