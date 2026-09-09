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
import type { ToolSettlement } from "./tool-settlement.ts";

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions;
	readonly utility: DrafterUtilityBatch;
}

interface DrafterPlanFeedback extends DrafterBatch {
	readonly kind: "drafter_plan";
	readonly message: AssistantMessage;
	readonly depth: number;
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
	const batches = new Map<string, Promise<DrafterBatch>>();
	const gate = new DrafterUtilityGate();
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal): Promise<AssistantMessage> => {
		signal.throwIfAborted();
		gate.requestStarted(batch.utility);
		const startedAt = performance.now();
		let failed = false;
		try {
			const message = await input.complete(batch.model, batch.context, { ...batch.options, signal });
			if (message.stopReason === "error" || message.stopReason === "aborted")
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			return message;
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
		continueOn: ["execution_succeeded"],
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
			const proposalID = `drafter:${startInput.turnID}:${proposalIndex}`;
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				batch = (async () => {
					const configuredDraftModel =
						typeof input.draftModel === "function"
							? await input.draftModel(startInput.actorModel)
							: input.draftModel;
					const model = configuredDraftModel ?? startInput.actorModel;
					const utility = gate.start(
						drafterModelKey(model),
						settings.sourceConfig?.drafterGateEnabled !== false,
					);
					let configuredDraftOptions: SimpleStreamOptions | undefined;
					if (utility.allowed) {
						configuredDraftOptions = input.getDraftOptions
							? await input.getDraftOptions({
									actorModel: startInput.actorModel,
									draftModel: model,
									actorOptions: startInput.actorOptions,
									signal,
								})
							: startInput.actorOptions;
					}
					return {
						model,
						context: startInput.context,
						options: configuredDraftOptions ?? {},
						utility,
					};
				})();
				batches.set(batchKey, batch);
			}
			const prepared = await batch;
			if (!prepared.utility.allowed || signal.aborted) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			const reasoning = clampThinkingLevel(prepared.model, "off");
			const { maxTokens: _actorMaxTokens, ...requestOptions } = prepared.options;
			const draftOptions: SimpleStreamOptions & { readonly toolChoice: "required" } = {
				...requestOptions,
				temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter),
				...(drafter.drafterMaxTokens ? { maxTokens: drafter.drafterMaxTokens } : {}),
				toolChoice: "required",
				reasoning: reasoning === "off" ? undefined : reasoning,
				deferred: false,
				sessionId: prepared.options.sessionId ?? input.sessionID,
				cacheRetention: prepared.options.cacheRetention ?? "short",
			};
			if (!drafterContextFits(prepared.model, prepared.context, draftOptions.maxTokens)) return undefined;
			const request = { ...prepared, options: draftOptions };
			const message = await completeDraft(request, signal);
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			if (!data.tools.has(call.name) || !candidateNames.includes(call.name)) return undefined;
			const feedback = drafterFeedback(request, message, call, 0);
			return {
				id: proposalID,
				source: "drafter",
				revision: 0,
				actions: [drafterPlanAction(`${proposalIndex}:${call.id}`, call, feedback)],
				draftTokens: calculateContextTokens(message.usage),
			};
		},
		continue: async ({ proposalID, actionID, revision, feedback, output, signal }) => {
			const previous = asDrafterPlanFeedback(feedback);
			if (!previous || signal.aborted) return undefined;
			const previousCall = previous.message.content.find(
				(item): item is AgentToolCall => item.type === "toolCall",
			);
			if (!previousCall) return undefined;
			const context: Context = {
				...previous.context,
				messages: [...previous.context.messages, previous.message, drafterToolResult(previousCall, output)],
			};
			const continuationOptions = { ...previous.options, toolChoice: "auto" as const };
			if (!drafterContextFits(previous.model, context, continuationOptions.maxTokens)) return undefined;
			const request = { ...previous, context, options: continuationOptions };
			const message = await completeDraft(request, signal);
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			const next = drafterFeedback(request, message, call, previous.depth + 1);
			return {
				proposalID,
				source: "drafter",
				revision,
				upsert: [
					drafterPlanAction(`${actionID}/rollout:${next.depth}:${call.id}`, call, next, [
						{ actionID, condition: "execution_succeeded" },
					]),
				],
				draftTokens: calculateContextTokens(message.usage),
			};
		},
	};

	return {
		source,
		snapshot: () => gate.snapshot(),
		finishTurn: (sessionID, turnID) => {
			const batch = batches.get(agentBatchKey(sessionID, turnID));
			batches.delete(agentBatchKey(sessionID, turnID));
			if (!batch) return;
			void batch
				.then((value) => gate.finish(value.utility))
				.catch(() => {
					// Model/auth resolution failures are already represented by source request events.
				});
		},
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
			batches.clear();
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

function drafterModelKey(model: Model<Api>): string {
	return JSON.stringify([model.provider, model.api, model.baseUrl, model.id]);
}

function asDrafterPlanFeedback(value: unknown): DrafterPlanFeedback | undefined {
	return value && typeof value === "object" && (value as { kind?: unknown }).kind === "drafter_plan"
		? (value as DrafterPlanFeedback)
		: undefined;
}

function drafterPlanAction(
	id: string,
	call: AgentToolCall,
	feedback: DrafterPlanFeedback,
	dependsOn?: PlanAction["dependsOn"],
): PlanAction {
	return {
		id,
		type: "tool_call",
		tool: call.name,
		input: call.arguments,
		diagnostic: JSON.stringify({ toolCallID: call.id, tool: call.name, input: call.arguments }, null, 2),
		depth: feedback.depth,
		feedback,
		...(dependsOn?.length ? { dependsOn } : {}),
	};
}

function drafterFeedback(
	batch: DrafterBatch,
	message: AssistantMessage,
	call: AgentToolCall,
	depth: number,
): DrafterPlanFeedback {
	return {
		...batch,
		kind: "drafter_plan",
		message: { ...message, content: message.content.filter((item) => item.type !== "toolCall" || item === call) },
		depth,
	};
}

function drafterToolResult(call: AgentToolCall, output: ToolSettlement): ToolResultMessage {
	return {
		...output.result,
		role: "toolResult",
		toolCallId: call.id,
		toolName: call.name,
		isError: output.isError,
		timestamp: Date.now(),
	};
}
