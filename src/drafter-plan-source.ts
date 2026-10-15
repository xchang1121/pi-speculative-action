import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { calculateContextTokens, type AgentToolCall } from "@earendil-works/pi-agent-core";
import { clampThinkingLevel, fauxAssistantMessage, Type, validateToolArguments, type Api, type AssistantMessage, type Context, type Model, type SimpleStreamOptions, type ToolResultMessage } from "@earendil-works/pi-ai";
import { clampCandidateLimit, DEFAULTS, drafterRequestTemperature, normalizeDrafterRequestSettings, type DrafterRequestSettings } from "./common.ts";
import { DrafterTaskBudget, drafterInputTokens, drafterOpportunityKey } from "./drafter-budget.ts";
import { agentBatchKey, type AgentPlanSource, type DraftModelSelection, type DraftOptionsContext } from "./agent-runtime-types.ts";
import type { PlanAction, PlanProposal } from "./plan-proposal.ts";
import type { PatternAwareCandidate } from "./pattern-aware.ts";
import type { SpeculativeActionSettings } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import { immutableSnapshot } from "./stable-json.ts";
import { SourceRequestSuppressed } from "./source-request.ts";
import { cause } from "./settlement.ts";
import { canonicalToolObservation, toolObservationContract, toolObservationKey, type ToolSettlement } from "./tool-settlement.ts";
import type { ActionKey } from "./action-semantics.ts";
import { SharedAbortScope } from "./runtime-lifecycle.ts";
import { waitForCompletion } from "./scheduler.ts";

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions;
	readonly key: string;
	readonly budgetPolicy: DrafterRequestSettings;
}

interface DrafterPlanFeedback extends DrafterBatch {
	readonly message: AssistantMessage;
	readonly depth: number;
	readonly calls: ReadonlyMap<string, DrafterCall>;
	continuation?: { readonly controller: AbortController; observations?: readonly string[] };
}
interface DrafterObservation { readonly message: ToolResultMessage; readonly key: string; }
interface DrafterCall {
	readonly call: AgentToolCall;
	action?: ActionKey;
	result?: DrafterObservation;
	forecast?: string;
}

export function createDrafterPlanSource(input: {
	readonly sessionID: string;
	/** Drafter model. Defaults to the actor model when omitted or unresolved. */
	readonly draftModel?: DraftModelSelection;
	/** Resolve drafter request options, including credentials when using a different provider. */
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	/** Provider completion used by the drafter. */
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	/** Share this ledger with other model Drafter requests in the same user task. */
	readonly drafterBudget?: DrafterTaskBudget;
	readonly patternHints?: (input: { readonly sessionID: string; readonly schemaHashes: Readonly<Record<string, string>>; readonly settings: SpeculativeActionSettings })
		=> Promise<readonly Pick<PatternAwareCandidate, "tool" | "input" | "horizon">[]>;
}) {
	const batches = new Map<string, { lifetime: SharedAbortScope; ready: Promise<DrafterBatch | undefined> }>();
	const budget = input.drafterBudget ?? new DrafterTaskBudget();
	const observations = { forecasted: 0, compared: 0, matched: 0 };
	const feedbacks = new WeakMap<object, DrafterPlanFeedback>();
	// Separate Beta(1, 1) estimates over the latest 32 eligible outcomes per model and tool contract.
	const calibration = new BoundedRecencyMap<string, { matches: number[]; adoptions: number[] }>(128);
	const calibrationKey = (batch: DrafterBatch, tool: string) => JSON.stringify([batch.key, tool]);
	const probability = (samples: readonly number[] = []) => (samples.reduce((sum, value) => sum + value, 0) + 1) / (samples.length + 2);
	const observe = (samples: number[], outcome: boolean) => { samples.push(Number(outcome)); if (samples.length > 32) samples.shift(); };
	const probabilities = (batch: DrafterBatch, tool: string) => {
		const samples = calibration.get(calibrationKey(batch, tool));
		return { empiricalProbability: probability(samples?.matches), adoptionProbability: probability(samples?.adoptions) };
	};
	const finishBatch = (key: string) => {
		const batch = batches.get(key);
		batches.delete(key);
		batch?.lifetime.abort();
	};
	const completeRequest = async (batch: DrafterBatch, context: Context, signal: AbortSignal,
		report: ((tokens: number) => void) | undefined, force?: true | string) => {
		signal.throwIfAborted();
		const options = { ...batch.options, maxTokens: batch.budgetPolicy.drafterMaxTokens, deferred: false,
			sessionId: batch.options.sessionId ?? input.sessionID, cacheRetention: batch.options.cacheRetention ?? "short" as const };
		const inputTokens = drafterInputTokens({ ...context, tools: batch.context.tools }) + Math.max(0,
			Math.ceil((JSON.stringify(context.tools ?? []).length - JSON.stringify(batch.context.tools ?? []).length) / 4));
		if (!drafterContextFits(batch.model, context, options.maxTokens, inputTokens)) suppress("drafter_context_limit");
		const forced = !options.reasoning && force ? { onPayload: forceToolChoice(options.onPayload, force === true ? undefined : force) } : {};
		const response = await waitForCompletion(budget.run({ model: batch.model, context, inputTokens, options: { ...options, ...forced, signal },
			policy: batch.budgetPolicy, complete: input.complete, onSkipped: suppress }).then(message => {
			if (!message) return undefined;
			report?.(calculateContextTokens(message.usage));
			if (message.stopReason === "error" || message.stopReason === "aborted")
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			return message;
		}), signal);
		return response.status === "completed" ? response.value : undefined;
	};
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, report: ((tokens: number) => void) | undefined,
		prefix: string, depth = 0, dependsOn?: PlanAction["dependsOn"]) => {
		const message = await completeRequest(batch, batch.context, signal, report, depth === 0 ? true : undefined);
		if (!message) return undefined;
		const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
		if (!calls.length || new Set(calls.map(call => call.id)).size !== calls.length) return undefined;
		const feedback: DrafterPlanFeedback = { ...batch, depth, message,
			calls: new Map(calls.map((call, member) => [`${prefix}:${member}`, { call }])), continuation: undefined };
		feedbacks.set(feedback, feedback);
		return { actions: [...feedback.calls].map(([id, { call }]): PlanAction => ({ id, type: "tool_call", tool: call.name,
			input: call.arguments, depth, feedback, dependsOn, ...probabilities(batch, call.name) })) };
	};
	const source: AgentPlanSource = {
		id: "drafter",
		onSettled: ({ actionID, feedback, settlement }) => {
			const batch = feedbacks.get(feedback as object), tool = batch?.calls.get(actionID)?.call.name;
			if (!batch || !tool || settlement.observation !== "observed") return;
			const key = calibrationKey(batch, tool);
			let samples = calibration.get(key);
			if (!samples) { samples = { matches: [], adoptions: [] }; calibration.set(key, samples); }
			observe(samples.matches, settlement.match.matched);
			if (settlement.match.matched) observe(samples.adoptions, settlement.match.adoption.status === "adopted");
		},
		enabled: (settings) => (settings.drafterEnabled ?? DEFAULTS.drafterEnabled) && settings.tools?.length !== 0,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "turn",
		continuationBatch: ({ feedback }) => {
			const batch = feedbacks.get(feedback as object);
			return batch ? [...batch.calls.keys()] : undefined;
		},
		multiStepEnabled: (settings, feedback) => {
			const maxDepth = normalizeDrafterRequestSettings(settings.sourceConfig).drafterMaxDepth;
			if (maxDepth === 0) return false;
			if (feedback === undefined) return true;
			const previous = feedbacks.get(feedback as object);
			return previous !== undefined && previous.depth < maxDepth;
		},
		continueOn: ({ actionID, action, feedback, output }) => {
			const previous = feedbacks.get(feedback as object), member = previous?.calls.get(actionID);
			if (!previous || !member) return false;
			const result = output && privateObservation(member.call, output, action);
			if (result && member.forecast) {
				observations.compared++; observations.matched += Number(member.forecast === result.key);
				member.forecast = undefined;
			}
			if (result) member.result = result;
			member.action ??= action;
			const members = [...previous.calls.values()], continuation = previous.continuation;
			if (!members.every(member => member.action)) return false;
			if (!continuation) return true;
			if (!output || !members.every(member => member.result && !member.forecast) ||
				members.every((member, index) => member.result!.key === continuation.observations?.[index])) return false;
			continuation.controller.abort();
			return true;
		},
		proposalCount: (settings) => clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
		concurrentProposalPolicy: (settings) =>
			clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit) === 2 ? "first_produced" : "all",
		propose: async ({ startInput, data, proposalIndex, proposalCount, signal, settings, reportDraftTokens }): Promise<PlanProposal | undefined> => {
			if (signal.aborted) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			if (!budget.available(drafter, suppress)) return undefined;
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				const lifetime = new SharedAbortScope(), signal = lifetime.signal;
				batch = { lifetime, ready: Promise.resolve().then(async () => {
					if (signal.aborted) return undefined;
					const { draftModel, getDraftOptions } = input, { actorModel, actorOptions } = startInput;
					// Optional pattern hints follow the Actor history to preserve its prefix.
					const hints = await input.patternHints?.({ sessionID: startInput.sessionID, schemaHashes: data.schemaHashes, settings });
					const selected: Context = settings.tools ? { ...startInput.context, tools: startInput.context.tools?.filter(tool => settings.tools!.includes(tool.name)) } : startInput.context;
					const context: Context = drafter.drafterPatternHints && hints?.length ? { ...selected, messages: [...selected.messages, { role: "user", timestamp: Date.now(),
						content: `(Speculation hint, not from the user.) Calls that followed similar steps in this workspace:\n${hints.map((hint) => `- ${hint.tool} ${JSON.stringify(hint.input)}${hint.horizon === undefined ? "" : `; expected after ${hint.horizon} batches`}`).join("\n")}` }] }
						: selected;
					const model = (typeof draftModel === "function" ? await draftModel(actorModel) : draftModel) ?? actorModel;
					if (signal.aborted) return undefined;
					const key = drafterOpportunityKey(model, startInput.context, data.schemaHashes, hints);
					if (!drafterContextFits(model, context, drafter.drafterMaxTokens)) suppress("drafter_context_limit");
					const configuredDraftOptions = getDraftOptions ? await getDraftOptions({ actorModel, draftModel: model, actorOptions, signal }) : actorOptions;
					if (signal.aborted) return undefined;
					// Inherit transport options, while the Drafter owns its reasoning and output budget.
					const { maxTokens: _actorMaxTokens, reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
					const reasoning = clampThinkingLevel(model, getDraftOptions ? requestedReasoning ?? "off" : "off");
					return { model, context, options: { ...requestOptions, reasoning: reasoning === "off" ? undefined : reasoning }, key, budgetPolicy: drafter };
				}) };
				batches.set(batchKey, batch);
			}
			const release = batch.lifetime.acquire(signal);
			signal = AbortSignal.any([signal, batch.lifetime.signal]);
			let draft: Awaited<ReturnType<typeof completeDraft>>;
			try {
				const prepared = await batch.ready;
				if (!prepared || signal.aborted) return undefined;
				draft = await completeDraft({ ...prepared, options: { ...prepared.options, temperature: drafterRequestTemperature(proposalIndex, proposalCount, drafter) } },
					signal, reportDraftTokens, String(proposalIndex));
				return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
			} finally { if (!draft) release(); }
		},
		continue: async ({ proposalID, revision, feedback, signal, reportDraftTokens, settings }) => {
			const previous = feedbacks.get(feedback as object);
			if (!previous || signal.aborted || previous.continuation && !previous.continuation.controller.signal.aborted) return undefined;
			const continuation: NonNullable<DrafterPlanFeedback["continuation"]> = { controller: new AbortController() };
			previous.continuation = continuation;
			signal = AbortSignal.any([signal, continuation.controller.signal]);
			const policy = normalizeDrafterRequestSettings(settings.sourceConfig);
			if (previous.depth >= policy.drafterMaxDepth) return undefined;
			const missing = [...previous.calls.values()].filter(member => !member.result);
			if (missing.length) {
				const context = observationContext(previous, missing);
				const message = await completeRequest({ ...previous, budgetPolicy: policy }, context, signal, reportDraftTokens, OBSERVATION_TOOL);
				if (signal.aborted) return undefined;
				const pending = missing.filter(member => !member.result);
				if (pending.length) {
					const calls = message?.content.filter((item): item is AgentToolCall => item.type === "toolCall");
					if (calls?.length !== 1 || calls[0]!.name !== OBSERVATION_TOOL) return undefined;
					const values = validateToolArguments(context.tools![0]!, calls[0]!).results as (Pick<ToolResultMessage, "content" | "isError"> & { callID: string; exitCode?: number })[];
					if (new Set(values.map(value => value.callID)).size !== missing.length || stableValueHash(values) !== stableValueHash(calls[0]!.arguments.results)) return undefined;
					for (const member of pending) {
						const { content, isError, exitCode } = values.find(value => value.callID === member.call.id)!;
						member.result = privateObservation(member.call, { result: { content, details: {} }, isError, exitCode }, member.action);
						member.forecast = member.result.key; observations.forecasted++;
					}
				}
			}
			if (signal.aborted) return undefined;
			// Freeze the exact private observations used for this prediction, even if real results arrive during inference.
			const context = continuationContext(previous.context, previous.message, [...previous.calls.values()].map(member => member.result!.message));
			continuation.observations = [...previous.calls.values()].map(member => member.result!.key);
			const draft = await completeDraft({ ...previous, context, budgetPolicy: policy }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls].map(([actionID, member]) => ({ actionID, condition: "prediction", observation: member.result!.key })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions };
		},
		// A peer's executed batch, such as a fork's early calls, rolls out as the Drafter's own does: its calls and results extend the Actor context.
		continueFrom: async ({ startInput, settings, batch: peers, signal, reportDraftTokens }) => {
			const prepared = await batches.get(agentBatchKey(startInput.sessionID, startInput.turnID))?.ready.catch(() => undefined);
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig), ids = peers.map((_, index) => `peer:${index}`);
			if (!prepared || signal.aborted) return undefined;
			const calls = peers.map(({ candidate }, index) => ({ type: "toolCall" as const, id: ids[index]!, name: candidate.tool, arguments: { ...candidate.input } }));
			const context = continuationContext(prepared.context, { ...fauxAssistantMessage(calls, { stopReason: "toolUse" }),
				api: prepared.model.api, provider: prepared.model.provider, model: prepared.model.id },
				peers.map(({ candidate, output }, index) => privateObservation(calls[index]!, output, candidate.key).message));
			const options = { ...prepared.options, temperature: drafterRequestTemperature(0, 1, drafter) };
			const id = `drafter:peer:${stableValueHash(peers.map(({ identity }) => identity.id))}`;
			const draft = await completeDraft({ ...prepared, context, options, budgetPolicy: drafter }, signal, reportDraftTokens, id, 1, peers.map(({ identity, candidate, output }) =>
				({ proposalID: identity.proposalID, actionID: identity.actionID, identity: identity.id, condition: "prediction" as const, observation: toolObservationKey(output, candidate.key) ?? "" })));
			return draft && { id, source: "drafter", revision: 0, actions: draft.actions };
		},
	};

	return {
		source,
		snapshot: () => ({ budget: budget.snapshot(), observations: { ...observations } }),
		finishTurn: (sessionID: string, turnID: string) => { finishBatch(agentBatchKey(sessionID, turnID)); },
		finishSession: () => { for (const key of batches.keys()) finishBatch(key); budget.finishTask(); },
	};
}

/** Simple streams drop toolChoice for most APIs; spell a forced call on the final payload where the API has one. */
export function forceToolChoice(inherited: SimpleStreamOptions["onPayload"], tool?: string): SimpleStreamOptions["onPayload"] {
	return async (payload, model) => {
		const next = (await inherited?.(payload, model)) ?? payload;
		const forced = !tool ? FORCED_TOOL_CHOICE[model.api] : model.api === "anthropic-messages" ? { type: "tool", name: tool }
			: model.api === "openai-completions" ? { type: "function", function: { name: tool } }
			: FORCED_TOOL_CHOICE[model.api] ? { type: "function", name: tool } : undefined;
		return forced && next && typeof next === "object" && "tools" in next ? { ...next, tool_choice: forced } : next;
	};
}
const FORCED_TOOL_CHOICE: Readonly<Record<string, unknown>> = { "anthropic-messages": { type: "any" }, "openai-completions": "required",
	"openai-responses": "required", "azure-openai-responses": "required", "openai-codex-responses": "required" };

const OBSERVATION_TOOL = "speculative_observations";
const OBSERVATION_CONTENT = Type.Array(Type.Union([
	Type.Object({ type: Type.Literal("text"), text: Type.String() }),
	Type.Object({ type: Type.Literal("image"), data: Type.String(), mimeType: Type.String() }),
]));
function observationContext(batch: DrafterPlanFeedback, missing: readonly DrafterCall[]): Context {
	const candidates = missing.map(({ call, action }) => {
		const { version, content = OBSERVATION_CONTENT } = toolObservationContract(action);
		return { call, tool: batch.context.tools?.find(tool => tool.name === call.name), observation: { version, content } };
	});
	return { ...batch.context, messages: [...batch.context.messages, { role: "user", timestamp: Date.now(),
		content: `Predict the pending observations under their bound schemas.\n${JSON.stringify({ draft: batch.message.content,
			observations: [...batch.calls.values()].flatMap(member => member.result ? [member.result.message] : []), candidates })}` }],
		tools: [{ name: OBSERVATION_TOOL, description: "Private predicted tool observations; never executable results.", parameters: Type.Object({
			results: Type.Array(Type.Union(candidates.map(({ call, observation }) => Type.Object({ callID: Type.Literal(call.id),
				isError: Type.Boolean(), exitCode: Type.Optional(Type.Integer()), content: observation.content,
			}, { additionalProperties: false }))), { minItems: missing.length, maxItems: missing.length }),
		}, { additionalProperties: false }) }] };
}

function privateObservation(call: AgentToolCall, output: ToolSettlement, action?: ActionKey): DrafterObservation {
	const { key, content } = canonicalToolObservation(output, action);
	return { key, message: { role: "toolResult", toolCallId: call.id, toolName: call.name, content: immutableSnapshot(content), isError: output.isError, timestamp: Date.now() } };
}

function suppress(reason: string, detail?: string): never { throw new SourceRequestSuppressed(cause("source", reason, detail)); }

function continuationContext(context: Context, message: AssistantMessage, results: ToolResultMessage[]): Context {
	return { ...context, messages: [...context.messages, message, ...results] };
}

/** Preserve the Actor-visible history whole; a shorter Drafter skips instead of compacting it. */
function drafterContextFits(model: Model<Api>, context: Context, maxTokens: number | undefined, inputTokens = drafterInputTokens(context)): boolean {
	const output = Math.min(maxTokens ?? model.maxTokens, model.maxTokens);
	return inputTokens + output <= model.contextWindow;
}
