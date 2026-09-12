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
	readonly options: SimpleStreamOptions;
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
	private readonly owners = new Map<AbortSignal, () => void>();
	readonly signal = this.controller.signal;
	readonly ready: Promise<DrafterBatch | undefined>;

	constructor(prepare: (signal: AbortSignal) => Promise<DrafterBatch | undefined>) {
		this.ready = Promise.resolve().then(() => this.signal.aborted ? undefined : prepare(this.signal));
	}

	retain(signal: AbortSignal): void {
		if (signal.aborted || this.signal.aborted || this.owners.has(signal)) return;
		const release = () => { this.owners.delete(signal); if (!this.owners.size) this.dispose(); };
		this.owners.set(signal, release);
		signal.addEventListener("abort", release, { once: true });
	}

	dispose(): void {
		this.controller.abort();
		for (const [signal, release] of this.owners) signal.removeEventListener("abort", release);
		this.owners.clear();
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
	readonly draftModel?: DraftModelSelection;
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
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
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, prefix: string,
		depth = 0, dependsOn?: PlanAction["dependsOn"]) => {
		signal.throwIfAborted();
		gate.requestStarted(batch.utility);
		const startedAt = performance.now();
		let failed = false;
		try {
			const message = await input.complete(batch.model, batch.context, { ...batch.options, signal });
			if (message.stopReason === "error" || message.stopReason === "aborted")
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
			if (!calls.length || calls.some((call) => !batch.tools.has(call.name)) ||
				new Set(calls.map((call) => call.id)).size !== calls.length) return undefined;
			const feedback: DrafterPlanFeedback = { ...batch, kind: "drafter_plan", message, depth,
				calls: new Map(calls.map((call, index) => [`${prefix}:${index}`, call])), results: new Map(), claimed: false };
			return { actions: [...feedback.calls].map(([id, call]): PlanAction => ({
				id, type: "tool_call", tool: call.name, input: call.arguments, depth, feedback, dependsOn,
				diagnostic: JSON.stringify({ toolCallID: call.id, tool: call.name, input: call.arguments }, null, 2),
			})), draftTokens: calculateContextTokens(message.usage) };
		} catch (error) {
			failed = !signal.aborted;
			throw error;
		} finally {
			gate.requestSettled(batch.utility, performance.now() - startedAt, failed);
		}
	};
	const source: AgentPlanSource = {
		id: "drafter",
		enabled: (settings) => settings.drafterEnabled ?? DEFAULTS.drafterEnabled,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "actor_decision",
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
		propose: async ({
			startInput,
			data,
			candidateNames,
			proposalIndex,
			proposalCount,
			signal,
			settings,
		}): Promise<PlanProposal | undefined> => {
			if (signal.aborted) return undefined;
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				batch = new DrafterPreparation(async (signal) => {
					const model = (
						typeof input.draftModel === "function"
							? await input.draftModel(startInput.actorModel)
							: input.draftModel) ?? startInput.actorModel;
					if (signal.aborted) return undefined;
					const utility = gate.start(
						JSON.stringify([model.provider, model.api, model.baseUrl, model.id]),
						settings.sourceConfig?.drafterGateEnabled !== false,
					);
					const configuredDraftOptions = utility.allowed
						? input.getDraftOptions
							? await input.getDraftOptions({
									actorModel: startInput.actorModel,
									draftModel: model,
									actorOptions: startInput.actorOptions,
									signal,
								})
							: startInput.actorOptions
						: undefined;
					if (signal.aborted) return undefined;
					// Inherit transport options, while the Drafter owns its reasoning and output budget.
					const { maxTokens: _actorMaxTokens, reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
					const reasoning = clampThinkingLevel(model, input.getDraftOptions ? requestedReasoning ?? "off" : "off");
					return {
						model,
						context: startInput.context,
						options: { ...requestOptions, reasoning: reasoning === "off" ? undefined : reasoning },
						utility,
						tools: new Set(candidateNames.filter((name) => data.tools.has(name))),
					};
				});
				batches.set(batchKey, batch);
			}
			batch.retain(signal);
			signal = AbortSignal.any([signal, batch.signal]);
			const prepared = await batch.ready;
			if (!prepared?.utility.allowed || signal.aborted) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			const draftOptions: SimpleStreamOptions & { readonly toolChoice: "auto" | "required" } = {
				...prepared.options,
				temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter),
				...(drafter.drafterMaxTokens ? { maxTokens: drafter.drafterMaxTokens } : {}),
				// Thinking providers can reject forced tool calls; preserve their normal tool decision.
				toolChoice: prepared.options.reasoning ? "auto" : "required",
				deferred: false,
				sessionId: prepared.options.sessionId ?? input.sessionID,
				cacheRetention: prepared.options.cacheRetention ?? "short",
			};
			if (!drafterContextFits(prepared.model, prepared.context, draftOptions.maxTokens)) return undefined;
			if (!prepared.utility.startedRequests) data.prepareExecution?.(candidateNames, batch.signal);
			const draft = await completeDraft({ ...prepared, options: draftOptions }, signal, String(proposalIndex));
			return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
		},
		continue: async ({ proposalID, revision, feedback, signal }) => {
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
			const draft = await completeDraft({ ...previous, context, options }, signal, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map((actionID) => ({ actionID, condition: "execution_succeeded" })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions, draftTokens: draft.draftTokens };
		},
	};

	return {
		source,
		snapshot: () => gate.snapshot(),
		finishTurn: (sessionID, turnID) => finishBatch(agentBatchKey(sessionID, turnID)),
		actorActionSettled: async (feedback) => {
			const { settlement } = feedback;
			const owner = asDrafterPlanFeedback(feedback.candidateFeedback);
			if (
				!owner ||
				feedback.candidate?.source !== "drafter" ||
				settlement.provider.kind !== "speculative" ||
				!settlement.matchedPredictions.some((prediction) => prediction.source === "drafter")
			)
				return;
			gate.creditAdoption(owner.utility, settlement.provider.timing);
		},
		finishSession: () => {
			for (const key of batches.keys()) finishBatch(key);
			gate.reset();
		},
	};
}

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
