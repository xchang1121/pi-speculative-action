import type { AgentTool, AgentToolCall } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions, ToolResultMessage } from "@earendil-works/pi-ai";
import {
	clampCandidateLimit,
	DEFAULTS,
	drafterRequestTemperature,
	normalizeDrafterRequestSettings,
	usageTokenCount,
} from "./common.ts";
import {
	DrafterUtilityGate,
	type DrafterUtilityBatch,
	type DrafterUtilityGateSnapshot,
} from "./drafter-utility-gate.ts";
import {
	agentBatchKey,
	type AgentPlanSource,
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

interface DrafterPlanFeedback {
	readonly kind: "drafter_plan";
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions;
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
	readonly draftModel?:
		| Model<Api>
		| ((actorModel: Model<Api>) => Model<Api> | undefined | Promise<Model<Api> | undefined>);
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	readonly validateArguments: (
		tool: AgentTool,
		toolName: string,
		input: unknown,
		callID: string,
	) => unknown | undefined;
}): DrafterPlanSourceController {
	const batches = new Map<string, Promise<DrafterBatch>>();
	const gate = new DrafterUtilityGate();
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
						proposalCount,
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
			if (!prepared.utility.allowed) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			const { maxTokens: _actorMaxTokens, ...requestOptions } = prepared.options;
			const draftOptions: SimpleStreamOptions & { readonly toolChoice: "required" } = {
				...requestOptions,
				temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter),
				...(drafter.drafterMaxTokens ? { maxTokens: drafter.drafterMaxTokens } : {}),
				toolChoice: "required",
				reasoning: undefined,
				deferred: false,
				sessionId: prepared.options.sessionId ?? input.sessionID,
				cacheRetention: prepared.options.cacheRetention ?? "short",
			};
			const requestStartedAt = performance.now();
			let requestFailed = false;
			let message: AssistantMessage;
			try {
				message = await input.complete(prepared.model, prepared.context, { ...draftOptions, signal });
				if (message.stopReason === "error" || message.stopReason === "aborted") {
					requestFailed = message.stopReason === "error" || !signal.aborted;
					throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
				}
			} catch (error) {
				if (!signal.aborted) requestFailed = true;
				throw error;
			} finally {
				gate.requestSettled(prepared.utility, performance.now() - requestStartedAt, requestFailed);
			}
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			const tool = data.tools.get(call.name);
			if (
				!tool ||
				!candidateNames.includes(call.name) ||
				input.validateArguments(tool, call.name, call.arguments, call.id) === undefined
			)
				return undefined;
			const feedback = drafterFeedback(prepared.model, prepared.context, draftOptions, message, call, 0);
			return {
				id: proposalID,
				source: "drafter",
				revision: 0,
				actions: [drafterPlanAction(`${proposalIndex}:${call.id}`, call, feedback)],
				draftTokens: usageTokenCount(message.usage),
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
			const message = await input.complete(previous.model, context, { ...continuationOptions, signal });
			if (message.stopReason === "error" || message.stopReason === "aborted") {
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			}
			const call = message.content.find((item): item is AgentToolCall => item.type === "toolCall");
			if (!call) return undefined;
			const next = drafterFeedback(previous.model, context, continuationOptions, message, call, previous.depth + 1);
			return {
				proposalID,
				source: "drafter",
				revision,
				upsert: [
					drafterPlanAction(`${actionID}/rollout:${next.depth}:${call.id}`, call, next, [
						{ actionID, condition: "execution_succeeded" },
					]),
				],
				draftTokens: usageTokenCount(message.usage),
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
			if (
				feedback.candidate?.source !== "drafter" ||
				settlement.provider.kind !== "speculative" ||
				!settlement.matchedPredictions.some(
					(prediction) =>
						prediction.source === "drafter" && prediction.proposalID.startsWith(`drafter:${feedback.turnID}:`),
				)
			)
				return;
			const batch = batches.get(agentBatchKey(feedback.sessionID, feedback.turnID));
			if (!batch) return;
			try {
				gate.creditExecutionAhead((await batch).utility, settlement.provider.timing.executionAheadMs);
			} catch {
				// Source resolution failures cannot own an adopted speculative candidate.
			}
		},
		finishSession: () => {
			batches.clear();
			gate.reset();
		},
	};
}

function drafterModelKey(model: Model<Api>): string {
	return JSON.stringify([model.provider, model.api, model.id]);
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
	model: Model<Api>,
	context: Context,
	requestOptions: SimpleStreamOptions,
	message: AssistantMessage,
	call: AgentToolCall,
	depth: number,
): DrafterPlanFeedback {
	return {
		kind: "drafter_plan",
		model,
		context,
		options: requestOptions,
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
