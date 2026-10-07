import { calculateContextTokens, estimateContextTokens } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { DrafterRequestSettings } from "./common.ts";
import { BenefitGate, creditAdoption, DEFAULT_BENEFIT_GATE_POLICY, type BenefitGatePolicy, type BenefitDecisionReason } from "./fork-benefit-gate.ts";
import type { ActorHitTiming } from "./settlement.ts";
import { stableValueHash } from "./stable-value-hash.ts";

type BudgetPolicy = Pick<DrafterRequestSettings, "drafterTaskMaxRequests" | "drafterTaskMaxTokens">;
export interface DrafterUtilityBatch {
	readonly key: string;
	readonly coldOpportunity: boolean;
	chargedTokens: number;
	costMs: number;
	benefitMs?: number;
	finished: boolean;
	pendingRequests: number;
	readonly policy: BenefitGatePolicy;
	readonly marginal: boolean;
	expectedBenefitMs?: number;
	startedRequests: number;
	update?: ReturnType<BenefitGate["observe"]>;
}
export type DrafterUtilitySnapshot = Readonly<ReturnType<DrafterTaskBudget["utilitySnapshot"]>>;
export type DrafterBudgetSnapshot = Readonly<ReturnType<DrafterTaskBudget["snapshot"]>>;
type Suppression = `drafter_${BenefitDecisionReason}` | "drafter_request_limit" | "drafter_token_limit" | "drafter_context_limit" | "drafter_future_reserve" | "drafter_exploration_limit" | "drafter_parent_unmeasured";
type OnSkipped = (reason: Suppression, detail?: string) => void;

/** One user task shares reservations across concurrent roots, continuations and Drafter probes. */
export class DrafterTaskBudget {
	private state = this.empty();
	private finished = false;
	private readonly samples = new Set<DrafterUtilityBatch>();
	private readonly gate = new BenefitGate();
	private readonly attempted = new Set<string>();
	private latestKey?: string;
	private skippedBatches = 0;

	available(policy: BudgetPolicy, onSkipped?: OnSkipped): boolean {
		if (this.finished) { this.state = this.empty(); this.attempted.clear(); this.finished = false; }
		const state = this.state;
		if (state.requests < policy.drafterTaskMaxRequests && this.spent() < policy.drafterTaskMaxTokens) return true;
		this.skip(state.requests >= policy.drafterTaskMaxRequests ? "drafter_request_limit" : "drafter_token_limit", onSkipped);
		return false;
	}

	start(key: string, enabled: boolean, marginal = false, coldOpportunity = false): DrafterUtilityBatch {
		if (!marginal) this.latestKey = key;
		return { key, coldOpportunity, policy: { ...DEFAULT_BENEFIT_GATE_POLICY, enabled }, marginal, startedRequests: 0, pendingRequests: 0,
			chargedTokens: 0, costMs: 0, benefitMs: 0, finished: false };
	}

	finish(...batches: DrafterUtilityBatch[]): void {
		for (const batch of batches) { batch.finished = true; this.observe(batch); }
	}

	credit(batches: readonly DrafterUtilityBatch[], timing: ActorHitTiming, shares = 1): void {
		for (const batch of new Set(batches)) { creditAdoption(batch, timing, shares); this.observe(batch); }
	}

	utilitySnapshot() {
		const state = this.latestKey ? this.gate.snapshot(this.latestKey) : undefined;
		return { skippedBatches: this.skippedBatches, samples: state?.samples ?? 0, expectedNetBenefitMs: state?.expectedNetBenefitMs };
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
		readonly utility?: DrafterUtilityBatch;
		readonly ancestors?: readonly DrafterUtilityBatch[];
		/** Real tool outputs, unlike another racing guess, can justify a cold continuation. */
		readonly afterExecution?: boolean;
		readonly marginal?: boolean;
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
		const implicit = !input.utility && input.marginal ? this.start(drafterOpportunityKey(input.model, input.context, undefined, [], "probe"), true, true) : undefined;
		if (implicit) implicit.benefitMs = undefined; // Its actual fork adoption belongs to SelfSpeculation, not this ledger.
		const batch = input.utility ?? implicit;
		const ancestors = new Set([batch, ...input.ancestors ?? []].filter((value): value is DrafterUtilityBatch => !!value));
		const healthKey = JSON.stringify([input.model.provider, input.model.api, input.model.baseUrl, input.model.id, "endpoint"]);
		const reservation = prompt + maxTokens;
		let exploring = false;
		if (batch?.policy.enabled) {
			const measured = new Map<string, { saved: number; tokens: number }>();
			for (const sample of this.samples) if (sample.finished && !sample.pendingRequests && sample.benefitMs !== undefined && sample.chargedTokens > 0) {
				const previous = measured.get(sample.key) ?? { saved: 0, tokens: 0 };
				measured.set(sample.key, { saved: previous.saved + sample.benefitMs - sample.costMs, tokens: previous.tokens + sample.chargedTokens });
			}
			const density = (key?: string) => Math.max(0, ...[...measured].filter(([name]) => name !== key).map(([, sample]) => sample.saved / sample.tokens));
			const threshold = Math.max(batch.policy.minNetBenefitMs, (prompt + output) * density(batch.key));
			const forecast = this.forecast(batch), history = this.gate.snapshot(batch.key), fresh = !this.attempted.has(batch.key) && !history.samples;
			const reserve = Math.max(input.policy.drafterTaskMaxTokens / 2, 2 * (prompt + output));
			const reject = (reason: Suppression, expected = forecast) => this.skip(reason, input.onSkipped, JSON.stringify({
				promptTokens: prompt, outputTokens: maxTokens, reservationTokens: reservation, remainingTokens: remaining,
				explorationTokens: state.explorationTokens, explorationCap: input.policy.drafterTaskMaxTokens / 4, reserveTokens: reserve,
				forecastMs: forecast ?? null, expectedMs: expected ?? null, thresholdMs: threshold, opportunityFresh: fresh, coldOpportunity: batch.coldOpportunity,
				futureOpportunityUsed: state.futureOpportunityUsed, opportunity: batch.key }));
			const parentUseful = [...ancestors].some(parent => parent !== batch &&
				(parent.pendingRequests === 0 && parent.chargedTokens > 0 && parent.benefitMs !== undefined
					? parent.benefitMs - parent.costMs : this.gate.snapshot(parent.key).expectedNetBenefitMs ?? 0) >= batch.policy.minNetBenefitMs);
			// An unresolved root is not evidence for paying for a second racing guess.
			if (batch.marginal && !input.afterExecution && [...ancestors].some(parent => parent !== batch && parent.pendingRequests > 0) && !parentUseful &&
				(forecast === undefined || forecast < threshold))
				return reject("drafter_parent_unmeasured");
			const policy = { ...batch.policy, minNetBenefitMs: threshold, minSamples: fresh ? 1 : 0 };
			const health = this.gate.decide(healthKey, { ...batch.policy, minNetBenefitMs: 0 });
			if (!health.allowed) return reject(`drafter_${health.reason}`);
			// One endpoint circuit owns transport recovery; a real retry still competes for task tokens.
			const decision = health.reason === "failure_probe" ? { ...health, expectedNetBenefitMs: forecast ?? history.expectedNetBenefitMs }
				: this.gate.decide(batch.key, policy, forecast), expected = decision.expectedNetBenefitMs;
			if (!decision.allowed) {
				if (!batch.marginal) this.skippedBatches++;
				return reject(`drafter_${decision.reason}`, expected);
			}
			exploring = expected === undefined || expected < threshold;
			const superior = expected !== undefined && expected >= threshold && expected / (prompt + output) > density(batch.marginal ? batch.key : undefined);
			const needsExploration = exploring && state.requests > 0 && state.explorationTokens + reservation > input.policy.drafterTaskMaxTokens / 4;
			const needsReserve = state.requests > 0 && remaining - reservation < reserve && !superior;
			const future = needsExploration && !batch.marginal && fresh && batch.coldOpportunity && !state.futureOpportunityUsed;
			if ((needsExploration || needsReserve) && !future)
				return reject(needsExploration ? "drafter_exploration_limit" : "drafter_future_reserve", expected);
			if (needsExploration || needsReserve) state.futureOpportunityUsed = true;
			this.attempted.add(batch.key);
		}
		state.requests++;
		state.reservedTokens += reservation;
		if (exploring) state.explorationTokens += reservation;
		for (const utility of ancestors) {
			utility.pendingRequests++; utility.startedRequests++;
			if (utility.policy.enabled) this.samples.add(utility);
		}
		while (this.samples.size > 32) this.samples.delete(this.samples.values().next().value!);
		let charged = reservation, reported = false, failed = false;
		try {
			input.started?.();
			const message = await input.complete(input.model, input.context, { ...input.options, maxTokens });
			failed = ["error", "aborted"].includes(message.stopReason) && !input.options?.signal?.aborted;
			const tokens = calculateContextTokens(message.usage);
			if (Number.isFinite(tokens) && tokens >= 0 && (tokens > 0 || !["error", "aborted"].includes(message.stopReason))) {
				state.reportedTokens += tokens; charged = tokens; reported = true;
			}
			return message;
		} catch (error) {
			failed = !input.options?.signal?.aborted;
			throw error;
		} finally {
			state.reservedTokens -= reservation;
			if (exploring) state.explorationTokens += charged - reservation;
			if (batch?.policy.enabled && !input.options?.signal?.aborted) this.gate.observe(healthKey, { costMs: 0, benefitMs: 0, failed }, batch.policy);
			// Missing usage can still be billable; keep the reservation charged, including aborts and failures.
			if (!reported) state.unreportedTokens += reservation;
			if (implicit) implicit.finished = true;
			for (const utility of ancestors) {
				utility.chargedTokens += charged; utility.pendingRequests--; this.observe(utility);
			}
		}
	}

	finishTask(): void { this.finished = true; }
	snapshot() { return { ...this.state }; }
	private skip(reason: Suppression, onSkipped?: OnSkipped, detail?: string): undefined { this.state.skippedRequests++; onSkipped?.(reason, detail); return undefined; }
	private observe(batch: DrafterUtilityBatch): void {
		if (!batch.policy.enabled || !batch.finished || !batch.startedRequests || batch.pendingRequests) return;
		if (batch.update) batch.update(batch); else batch.update = this.gate.observe(batch.key, batch, batch.policy);
	}
	private forecast(batch: DrafterUtilityBatch): number | undefined {
		if (batch.expectedBenefitMs === undefined) return undefined;
		const samples = [...this.samples].filter(sample => sample.key === batch.key && sample.finished && !sample.pendingRequests);
		const calibrated = samples.filter(sample => sample.expectedBenefitMs !== undefined && sample.benefitMs !== undefined);
		const predicted = calibrated.reduce((sum, sample) => sum + sample.expectedBenefitMs!, 0);
		const realized = calibrated.reduce((sum, sample) => sum + sample.benefitMs!, 0);
		return Math.max(0, batch.expectedBenefitMs * (predicted > 0 ? Math.min(1, realized / predicted) : 1) -
			samples.reduce((sum, sample) => sum + sample.costMs, 0) / Math.max(1, samples.length));
	}
	private spent() { return this.state.reportedTokens + this.state.unreportedTokens + this.state.reservedTokens; }
	private empty() { return { requests: 0, reportedTokens: 0, unreportedTokens: 0, reservedTokens: 0, explorationTokens: 0, futureOpportunityUsed: false, skippedRequests: 0 }; }
}

/** Utility identity includes actual tool context and predicted workflow shape, never argument churn. */
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
