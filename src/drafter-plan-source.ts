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
import { SourceRequestSuppressed } from "./source-request.ts";
import { cause } from "./settlement.ts";
import { toolObservationKey } from "./tool-settlement.ts";
import { SharedAbortScope } from "./runtime-lifecycle.ts";

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions & { readonly toolChoice?: "auto" | "required" };
	readonly key: string;
	readonly budgetPolicy: DrafterRequestSettings;
}

interface DrafterPlanFeedback extends DrafterBatch {
	readonly message: AssistantMessage;
	readonly depth: number;
	readonly calls: ReadonlyMap<string, AgentToolCall>;
	readonly results: Map<string, ToolResultMessage>;
	readonly forecasts: Map<string, string>;
	readonly prepared: Set<string>;
	claimed: boolean;
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
		report: ((tokens: number) => void) | undefined, tool?: string) => {
		signal.throwIfAborted();
		const { options } = batch;
		const inputTokens = drafterInputTokens({ ...context, tools: batch.context.tools }) + Math.max(0,
			Math.ceil((JSON.stringify(context.tools ?? []).length - JSON.stringify(batch.context.tools ?? []).length) / 4));
		if (!drafterContextFits(batch.model, context, options.maxTokens, inputTokens)) suppress("drafter_context_limit");
		const forced = !options.reasoning && (tool || options.toolChoice === "required") ? { onPayload: forceToolChoice(options.onPayload, tool) } : {};
		const message = await budget.run({ model: batch.model, context, inputTokens, options: { ...options, ...forced, signal },
			policy: batch.budgetPolicy, complete: input.complete, onSkipped: suppress });
		if (!message) return undefined;
		report?.(calculateContextTokens(message.usage));
		if (message.stopReason === "error" || message.stopReason === "aborted")
			throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
		return message;
	};
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, report: ((tokens: number) => void) | undefined,
		prefix: string, depth = 0, dependsOn?: PlanAction["dependsOn"]) => {
		const message = await completeRequest(batch, batch.context, signal, report);
		if (!message) return undefined;
		const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
		if (!calls.length || new Set(calls.map(call => call.id)).size !== calls.length) return undefined;
		const feedback: DrafterPlanFeedback = { ...batch, depth, message,
			calls: new Map(calls.map((call, member) => [`${prefix}:${member}`, call])), results: new Map(), forecasts: new Map(), prepared: new Set(), claimed: false };
		feedbacks.set(feedback, feedback);
		return { actions: [...feedback.calls].map(([id, call]): PlanAction => ({ id, type: "tool_call", tool: call.name,
			input: call.arguments, depth, feedback, dependsOn, ...probabilities(batch, call.name) })) };
	};
	const source: AgentPlanSource = {
		id: "drafter",
		onSettled: ({ actionID, feedback, settlement }) => {
			const batch = feedbacks.get(feedback as object), tool = batch?.calls.get(actionID)?.name;
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
		continueOn: ({ trigger, actionID, feedback, output }) => {
			const previous = feedbacks.get(feedback as object), call = previous?.calls.get(actionID);
			if (!previous || !call) return false;
			if (output && previous.forecasts.has(actionID)) {
				observations.compared++; observations.matched += Number(previous.forecasts.get(actionID) === toolObservationKey(output));
				previous.forecasts.delete(actionID);
			}
			if (output) previous.results.set(actionID, { role: "toolResult", toolCallId: call.id, toolName: call.name,
				content: output.result.content, isError: output.isError, timestamp: Date.now() });
			if (trigger === "prepared") previous.prepared.add(actionID);
			return !previous.claimed && previous.prepared.size === previous.calls.size;
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
				draft = await completeDraft({ ...prepared, options: draftOptions }, signal, reportDraftTokens, String(proposalIndex));
				return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
			} finally { if (!draft) release(); }
		},
		continue: async ({ proposalID, revision, feedback, signal, reportDraftTokens, settings }) => {
			const previous = feedbacks.get(feedback as object);
			if (!previous || signal.aborted || previous.claimed) return undefined;
			previous.claimed = true;
			const policy = normalizeDrafterRequestSettings(settings.sourceConfig);
			if (previous.depth >= policy.drafterMaxDepth) return undefined;
			const missing = [...previous.calls].filter(([id]) => !previous.results.has(id));
			if (missing.length) {
				const context = observationContext(previous, missing);
				const message = await completeRequest({ ...previous, budgetPolicy: policy }, context, signal, reportDraftTokens, OBSERVATION_TOOL);
				const calls = message?.content.filter((item): item is AgentToolCall => item.type === "toolCall");
				try {
					if (calls?.length === 1 && calls[0]!.name === OBSERVATION_TOOL) {
						const values = validateToolArguments(context.tools![0]!, calls[0]!).results as (Pick<ToolResultMessage, "content" | "isError"> & { callID: string })[];
						const results = new Map(values.map(result => [result.callID, result]));
						if (results.size === missing.length && stableValueHash(values) === stableValueHash(calls[0]!.arguments.results)) for (const [id, call] of missing) if (!previous.results.has(id)) {
							const { content, isError } = results.get(call.id)!;
							const result: ToolResultMessage = { role: "toolResult", toolCallId: call.id, toolName: call.name, content, isError, timestamp: Date.now() };
							previous.results.set(id, result); previous.forecasts.set(id, observationKey(result)); observations.forecasted++;
						}
					}
				} catch { /* Malformed forecasts cannot replace real results that arrived during inference. */ }
			}
			if (previous.results.size !== previous.calls.size || signal.aborted) return undefined;
			// Freeze the exact private observations used for this prediction, even if real results arrive during inference.
			const results = new Map(previous.results);
			const context = continuationContext(previous.context, { ...previous, results });
			const options = { ...previous.options, toolChoice: "auto" as const };
			const draft = await completeDraft({ ...previous, context, options, budgetPolicy: policy }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map(actionID => ({ actionID, condition: "prediction", observation: observationKey(results.get(actionID)!) })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions };
		},
		// A peer's executed batch, such as a fork's early calls, rolls out as the Drafter's own does: its calls and results extend the Actor context.
		continueFrom: async ({ startInput, settings, batch: peers, signal, reportDraftTokens }) => {
			const prepared = await batches.get(agentBatchKey(startInput.sessionID, startInput.turnID))?.ready.catch(() => undefined);
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig), ids = peers.map((_, index) => `peer:${index}`);
			if (!prepared || signal.aborted) return undefined;
			const calls = new Map(peers.map(({ candidate }, index) => [ids[index]!, { type: "toolCall" as const, id: ids[index]!, name: candidate.tool, arguments: { ...candidate.input } }]));
			const context = continuationContext(prepared.context, { calls, message: { ...fauxAssistantMessage([...calls.values()], { stopReason: "toolUse" }),
				api: prepared.model.api, provider: prepared.model.provider, model: prepared.model.id },
				results: new Map(peers.map(({ candidate, output }, index) => [ids[index]!, { ...output.result, role: "toolResult" as const,
					toolCallId: ids[index]!, toolName: candidate.tool, isError: output.isError, timestamp: Date.now() }])) });
			const options = { ...prepared.options, temperature: drafterRequestTemperature(0, 1, drafter), maxTokens: drafter.drafterMaxTokens, toolChoice: "auto" as const };
			const id = `drafter:peer:${stableValueHash(peers.map(({ identity }) => identity.id))}`;
			const draft = await completeDraft({ ...prepared, context, options, budgetPolicy: drafter }, signal, reportDraftTokens, id, 1, peers.map(({ identity, output }) =>
				({ proposalID: identity.proposalID, actionID: identity.actionID, identity: identity.id, condition: "prediction" as const, observation: toolObservationKey(output) })));
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
function observationContext(batch: DrafterPlanFeedback, missing: readonly (readonly [string, AgentToolCall])[]): Context {
	return { ...batch.context, messages: [...batch.context.messages, { role: "user", timestamp: Date.now(),
		content: `Predict the exact model-visible results of the following pending tool calls, using the conversation above. These are private forecasts, not real execution. Return one ${OBSERVATION_TOOL} call with each callID, content blocks and isError. Preserve errors and exact text, whitespace and image data.\n${JSON.stringify(missing.map(([, call]) => call))}` }],
		tools: [{ name: OBSERVATION_TOOL, description: "Private predicted tool observations; never executable results.", parameters: Type.Object({
			results: Type.Array(Type.Object({ callID: Type.Union(missing.map(([, call]) => Type.Literal(call.id))), isError: Type.Boolean(),
				content: Type.Array(Type.Union([
					Type.Object({ type: Type.Literal("text"), text: Type.String() }),
					Type.Object({ type: Type.Literal("image"), data: Type.String(), mimeType: Type.String() }),
				])),
			}), { minItems: missing.length, maxItems: missing.length }),
		}) }] };
}

const observationKey = (result: ToolResultMessage) => toolObservationKey({ result: { content: result.content, details: {} }, isError: result.isError });

function suppress(reason: string, detail?: string): never { throw new SourceRequestSuppressed(cause("source", reason, detail)); }

function continuationContext(context: Context, batch: Pick<DrafterPlanFeedback, "message" | "calls" | "results">): Context {
	return { ...context, messages: [...context.messages, batch.message, ...[...batch.calls.keys()].map(id => batch.results.get(id)!)] };
}

/** Preserve the Actor-visible history whole; a shorter Drafter skips instead of compacting it. */
function drafterContextFits(model: Model<Api>, context: Context, maxTokens: number | undefined, inputTokens = drafterInputTokens(context)): boolean {
	const output = Math.min(maxTokens ?? model.maxTokens, model.maxTokens);
	return inputTokens + output <= model.contextWindow;
}
