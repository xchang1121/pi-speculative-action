import {
	calculateContextTokens,
	estimateContextTokens,
	type AgentToolCall,
} from "@earendil-works/pi-agent-core";
import {
	clampThinkingLevel,
	type Api,
	type AssistantMessage,
	type Context,
	type Model,
	type SimpleStreamOptions,
	type ToolResultMessage,
} from "@earendil-works/pi-ai";
import {
	clampCandidateLimit,
	DEFAULTS,
	drafterRequestTemperature,
	normalizeDrafterRequestSettings,
} from "./common.ts";
import {
	DrafterUtilityGate,
	type DrafterUtilityBatch,
	type DrafterUtilityGateSnapshot,
} from "./drafter-utility-gate.ts";
import {
	agentBatchKey,
	type AgentPlanSource,
	type DraftModelSelection,
	type DraftOptionsContext,
} from "./agent-runtime-types.ts";
import type { PlanAction, PlanProposal } from "./plan-proposal.ts";
import type { ActorActionFeedback } from "./runtime.ts";

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions & { readonly toolChoice?: "auto" | "required" };
	readonly utility: DrafterUtilityBatch;
	readonly tools: ReadonlySet<string>;
}

interface DrafterPlanFeedback extends DrafterBatch {
	readonly kind: "drafter_plan";
	readonly message: AssistantMessage;
	readonly depth: number;
	readonly calls: ReadonlyMap<string, AgentToolCall>;
	readonly results: Map<string, ToolResultMessage>;
	claimed: boolean;
}

/** Shared preparation belongs to all live proposals, until the batch retires. */
class DrafterPreparation {
	private readonly controller = new AbortController();
	private readonly owners = new Set<() => void>();
	readonly signal = this.controller.signal;
	readonly ready: Promise<DrafterBatch | undefined>;

	constructor(prepare: (signal: AbortSignal) => Promise<DrafterBatch | undefined>) {
		this.ready = Promise.resolve().then(() => this.signal.aborted ? undefined : prepare(this.signal));
	}

	async propose(signal: AbortSignal, produce: (batch: DrafterBatch, signal: AbortSignal) => Promise<PlanProposal | undefined>): Promise<PlanProposal | undefined> {
		if (signal.aborted || this.signal.aborted) return undefined;
		const release = () => {
			signal.removeEventListener("abort", release);
			if (this.owners.delete(release) && !this.owners.size) this.dispose();
		};
		this.owners.add(release);
		signal.addEventListener("abort", release, { once: true });
		let proposal: PlanProposal | undefined;
		try {
			const batch = await this.ready, requestSignal = AbortSignal.any([signal, this.signal]);
			if (batch && !requestSignal.aborted) proposal = await produce(batch, requestSignal);
			return proposal;
		} finally { if (!proposal) release(); }
	}

	dispose(): void {
		if (this.signal.aborted) return;
		this.controller.abort();
		for (const release of this.owners) release();
	}
}

export interface DrafterPlanSourceController {
	readonly source: AgentPlanSource;
	readonly snapshot: () => DrafterUtilityGateSnapshot;
	readonly finishTurn: (sessionID: string, turnID: string) => void;
	readonly actorActionSettled: (feedback: ActorActionFeedback<string>) => Promise<void>;
	readonly finishSession: () => void;
}

export function createDrafterPlanSource(input: {
	readonly sessionID: string;
	/** Drafter model. Defaults to the actor model when omitted or unresolved. */
	readonly draftModel?: DraftModelSelection;
	/** Resolve drafter request options, including credentials when using a different provider. */
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	/** Provider completion used by the drafter. */
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
}): DrafterPlanSourceController {
	const batches = new Map<string, DrafterPreparation>();
	const gate = new DrafterUtilityGate();
	const finishBatch = (key: string) => {
		const batch = batches.get(key);
		batches.delete(key);
		batch?.dispose();
		// Model/auth failures are already represented by source request events.
		void batch?.ready.then((value) => { if (value) gate.finish(value.utility); }).catch(() => {});
	};
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, report: ((tokens: number) => void) | undefined,
		prefix: string, depth = 0, dependsOn?: PlanAction["dependsOn"]) => {
		signal.throwIfAborted();
		gate.requestStarted(batch.utility);
		let failed = false;
		try {
			const { options } = batch, forced = options.toolChoice === "required" ? { onPayload: forceToolChoice(options.onPayload) } : {};
			const message = await input.complete(batch.model, batch.context, { ...options, ...forced, signal });
			report?.(calculateContextTokens(message.usage));
			if (message.stopReason === "error" || message.stopReason === "aborted")
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
			// Calls to tools outside the prediction list drop out; the partial batch can no longer be continued as a whole.
			const kept = calls.filter((call) => batch.tools.has(call.name));
			if (!kept.length || new Set(calls.map((call) => call.id)).size !== calls.length) return undefined;
			const feedback: DrafterPlanFeedback = { ...batch, kind: "drafter_plan", message, depth,
				calls: new Map(kept.map((call, index) => [`${prefix}:${index}`, call])), results: new Map(), claimed: kept.length < calls.length };
			return { actions: [...feedback.calls].map(([id, call]): PlanAction => ({
				id, type: "tool_call", tool: call.name, input: call.arguments, depth, feedback, dependsOn,
				diagnostic: JSON.stringify({ toolCallID: call.id, tool: call.name, input: call.arguments }, null, 2),
			})) };
		} catch (error) {
			failed = !signal.aborted;
			throw error;
		} finally {
			gate.requestSettled(batch.utility, failed);
		}
	};
	const source: AgentPlanSource = {
		id: "drafter",
		enabled: (settings) => settings.drafterEnabled ?? DEFAULTS.drafterEnabled,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "actor_decision",
		continuationBatch: ({ feedback }) => {
			const batch = asDrafterPlanFeedback(feedback);
			return batch && [...batch.calls.keys()];
		},
		multiStepEnabled: (settings, feedback) => {
			const maxDepth = normalizeDrafterRequestSettings(settings.sourceConfig).drafterMaxDepth;
			if (maxDepth === 0) return false;
			if (feedback === undefined) return true;
			const previous = asDrafterPlanFeedback(feedback);
			return previous !== undefined && previous.depth < maxDepth;
		},
		continueOn: ({ trigger, actionID, feedback, output }) => {
			const previous = asDrafterPlanFeedback(feedback), call = previous?.calls.get(actionID);
			if (trigger !== "execution_succeeded" || !previous || !call || previous.claimed || previous.results.has(actionID)) return false;
			previous.results.set(actionID, { ...output.result, role: "toolResult", toolCallId: call.id,
				toolName: call.name, isError: output.isError, timestamp: Date.now() });
			return previous.results.size === previous.calls.size;
		},
		proposalCount: (settings) => clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
		concurrentProposalPolicy: (settings) =>
			clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit) === 2 ? "first_produced" : "all",
		propose: async ({ startInput, data, candidateNames, proposalIndex, proposalCount, signal, settings, reportDraftTokens }): Promise<PlanProposal | undefined> => {
			if (signal.aborted) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				const tools = new Set(candidateNames.filter((name) => data.tools.has(name)));
				if (!tools.size) return undefined;
				batch = new DrafterPreparation(async (signal) => {
					const { draftModel, getDraftOptions } = input, { actorModel, actorOptions, context } = startInput;
					const model = (typeof draftModel === "function" ? await draftModel(actorModel) : draftModel) ?? actorModel;
					if (signal.aborted) return undefined;
					const utility = gate.start(JSON.stringify([model.provider, model.api, model.baseUrl, model.id]), settings.sourceConfig?.drafterGateEnabled !== false);
					if (!utility.allowed || !drafterContextFits(model, context, drafter.drafterMaxTokens)) return undefined;
					const configuredDraftOptions = getDraftOptions ? await getDraftOptions({ actorModel, draftModel: model, actorOptions, signal }) : actorOptions;
					if (signal.aborted) return undefined;
					// Inherit transport options, while the Drafter owns its reasoning and output budget.
					const { maxTokens: _actorMaxTokens, reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
					const reasoning = clampThinkingLevel(model, getDraftOptions ? requestedReasoning ?? "off" : "off");
					return { model, context, options: { ...requestOptions, reasoning: reasoning === "off" ? undefined : reasoning }, utility, tools };
				});
				batches.set(batchKey, batch);
			}
			return batch.propose(signal, async (prepared, signal) => {
				const draftOptions: DrafterBatch["options"] = {
					...prepared.options,
					temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter),
					maxTokens: drafter.drafterMaxTokens,
					// Thinking providers can reject forced tool calls; preserve their normal tool decision.
					toolChoice: prepared.options.reasoning ? "auto" : "required",
					deferred: false,
					sessionId: prepared.options.sessionId ?? input.sessionID,
					cacheRetention: prepared.options.cacheRetention ?? "short",
				};
				if (!prepared.utility.startedRequests) data.prepareExecution?.(candidateNames, batch.signal);
				const draft = await completeDraft({ ...prepared, options: draftOptions }, signal, reportDraftTokens, String(proposalIndex));
				return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
			});
		},
		continue: async ({ proposalID, revision, feedback, signal, reportDraftTokens }) => {
			const previous = asDrafterPlanFeedback(feedback);
			if (!previous || signal.aborted || previous.claimed || previous.results.size !== previous.calls.size) return undefined;
			previous.claimed = true;
			const context: Context = {
				...previous.context,
				messages: [...previous.context.messages, previous.message,
					...[...previous.calls.keys()].map((id) => previous.results.get(id)!)],
			};
			const options = { ...previous.options, toolChoice: "auto" as const };
			if (!drafterContextFits(previous.model, context, options.maxTokens)) return undefined;
			const draft = await completeDraft({ ...previous, context, options }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map((actionID) => ({ actionID, condition: "execution_succeeded" })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions };
		},
	};

	return {
		source,
		snapshot: () => gate.snapshot(),
		finishTurn: (sessionID, turnID) => finishBatch(agentBatchKey(sessionID, turnID)),
		actorActionSettled: async ({ settlement, candidate, candidateFeedback, sessionID, turnID }) => {
			const sources = new Set(settlement.matchedPredictions.map((prediction) => prediction.source));
			if (settlement.provider.kind !== "speculative" || !sources.has("drafter")) return;
			// Every source that predicted the adopted call shares its credit, whichever one executed it.
			const owner = candidate?.source === "drafter" ? asDrafterPlanFeedback(candidateFeedback) : undefined;
			const utility = owner?.utility ?? (await batches.get(agentBatchKey(sessionID, turnID))?.ready.catch(() => undefined))?.utility;
			if (utility) gate.creditAdoption(utility, settlement.provider.timing, sources.size);
		},
		finishSession: () => { for (const key of batches.keys()) finishBatch(key); },
	};
}

/** Simple streams drop toolChoice for most APIs; spell a forced call on the final payload where the API has one. */
function forceToolChoice(inherited: SimpleStreamOptions["onPayload"]): SimpleStreamOptions["onPayload"] {
	return async (payload, model) => {
		const next = (await inherited?.(payload, model)) ?? payload, forced = FORCED_TOOL_CHOICE[model.api];
		return forced && next && typeof next === "object" && "tools" in next ? { ...next, tool_choice: forced } : next;
	};
}
const FORCED_TOOL_CHOICE: Readonly<Record<string, unknown>> = { "anthropic-messages": { type: "any" }, "openai-completions": "required",
	"openai-responses": "required", "azure-openai-responses": "required", "openai-codex-responses": "required" };

/** Preserve the Actor-visible history whole; a shorter Drafter skips instead of compacting it. */
function drafterContextFits(model: Model<Api>, context: Context, maxTokens: number | undefined): boolean {
	const estimate = estimateContextTokens(context.messages);
	const staticPrompt =
		estimate.lastUsageIndex === null
			? Math.ceil(((context.systemPrompt?.length ?? 0) + JSON.stringify(context.tools ?? []).length) / 4)
			: 0;
	const output = Math.min(maxTokens ?? model.maxTokens, model.maxTokens);
	return estimate.tokens + staticPrompt + output <= model.contextWindow;
}

function asDrafterPlanFeedback(value: unknown): DrafterPlanFeedback | undefined {
	return value && typeof value === "object" && (value as { kind?: unknown }).kind === "drafter_plan"
		? (value as DrafterPlanFeedback)
		: undefined;
}
