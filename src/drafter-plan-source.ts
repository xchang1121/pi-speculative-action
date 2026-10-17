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
import { cause, predictionLearning } from "./settlement.ts";
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
	readonly continuations: readonly { readonly actionIDs: readonly string[]; readonly feedback: DrafterContinuation }[];
}
interface DrafterContinuation {
	readonly forecasts: Map<string, DrafterObservation>;
	request?: { readonly controller: AbortController; observations?: readonly string[] };
}
interface DrafterObservation { readonly message: ToolResultMessage; readonly key: string; }
interface DrafterCall {
	readonly call: AgentToolCall;
	action?: ActionKey;
	result?: DrafterObservation;
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
		let scopes = [calls.map(call => call.id)];
		if (calls.length > 1 && depth < batch.budgetPolicy.drafterMaxDepth) {
			// Decide visibility before any observations exist; post-hoc dependency labels could hide inputs already read.
			const context = scopeContext(batch, message, calls);
			try {
				const args = privateToolArguments(await completeRequest(batch, context, signal, report, CONTINUATION_TOOL), context);
				if (args) scopes = args.scopes as string[][];
			} catch { /* An unavailable or invalid scope selection conservatively reads the full batch. */ }
		}
		if (signal.aborted) return undefined;
		const unique = new Map(scopes.map(scope => { const ids = calls.flatMap((call, index) => scope.includes(call.id) ? [`${prefix}:${index}`] : []); return [JSON.stringify(ids), ids]; }));
		const feedback: DrafterPlanFeedback = { ...batch, depth, message,
			calls: new Map(calls.map((call, member) => [`${prefix}:${member}`, { call }])),
			continuations: [...unique.values()].map(actionIDs => ({ actionIDs, feedback: { forecasts: new Map() } })) };
		feedbacks.set(feedback, feedback);
		for (const continuation of feedback.continuations) feedbacks.set(continuation.feedback, feedback);
		return { actions: [...feedback.calls].map(([id, { call }]): PlanAction => {
			const samples = calibration.get(calibrationKey(batch, call.name));
			return { id, type: "tool_call", tool: call.name, input: call.arguments, depth, feedback, dependsOn,
				empiricalProbability: probability(samples?.matches), adoptionProbability: probability(samples?.adoptions) };
		}) };
	};
	const source: AgentPlanSource = {
		id: "drafter",
		onSettled: ({ actionID, feedback, settlement, served }) => {
			const batch = feedbacks.get(feedback as object), tool = batch?.calls.get(actionID)?.call.name;
			const { matched, adopted } = predictionLearning(settlement, served);
			if (!batch || !tool || matched === undefined && adopted === undefined) return;
			const key = calibrationKey(batch, tool);
			const samples = calibration.getOrCreate(key, () => ({ matches: [], adoptions: [] }));
			if (matched !== undefined) observe(samples.matches, matched);
			if (adopted !== undefined) observe(samples.adoptions, adopted);
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
			if (result) member.result = result;
			member.action ??= action;
			return previous.continuations.filter(({ actionIDs, feedback: continuation }) => {
				if (!actionIDs.includes(actionID)) return false;
				const forecast = continuation.forecasts.get(actionID);
				if (result && forecast) {
					observations.compared++; observations.matched += Number(forecast.key === result.key);
					continuation.forecasts.delete(actionID);
				}
				if (!actionIDs.every(id => previous.calls.get(id)!.action)) return false;
				const request = continuation.request;
				if (!request) return true;
				if (!result || result.key === request.observations?.[actionIDs.indexOf(actionID)]) return false;
				request.controller.abort();
				return true;
			});
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
		continue: async ({ proposalID, revision, reserveRevision, feedback, signal, reportDraftTokens, settings }) => {
			const previous = feedbacks.get(feedback as object);
			const branch = previous?.continuations.find(branch => branch.feedback === feedback);
			if (!previous || !branch || signal.aborted) return undefined;
			const continuation = branch.feedback;
			if (continuation.request && !continuation.request.controller.signal.aborted) return undefined;
			const request: NonNullable<DrafterContinuation["request"]> = { controller: new AbortController() };
			continuation.request = request;
			signal = AbortSignal.any([signal, request.controller.signal]);
			const policy = normalizeDrafterRequestSettings(settings.sourceConfig);
			if (previous.depth >= policy.drafterMaxDepth) return undefined;
			const current = (id: string) => previous.calls.get(id)!.result ?? continuation.forecasts.get(id);
			const missing = branch.actionIDs.filter(id => !current(id)).map(id => [id, previous.calls.get(id)!] as const);
			if (missing.length) {
				const context = observationContext(previous, missing.map(([, member]) => member), branch.actionIDs.flatMap(id => current(id) ? [current(id)!.message] : []));
				const message = await completeRequest({ ...previous, budgetPolicy: policy }, context, signal, reportDraftTokens, OBSERVATION_TOOL);
				if (signal.aborted) return undefined;
				const values = privateToolArguments(message, context)?.results as (Pick<ToolResultMessage, "content" | "isError"> & { callID: string; exitCode?: number })[] | undefined;
				if (!values || new Set(values.map(value => value.callID)).size !== missing.length) return undefined;
				for (const [id, member] of missing) {
					const { content, isError, exitCode } = values.find(value => value.callID === member.call.id)!;
					continuation.forecasts.set(id, privateObservation(member.call, { result: { content, details: {} }, isError, exitCode }, member.action));
					observations.forecasted++;
				}
			}
			if (signal.aborted) return undefined;
			// Freeze the exact private observations used for this prediction, even if real results arrive during inference.
			const context = continuationContext(previous.context, previous.message, [...previous.calls].map(([id, { call }]) =>
				branch.actionIDs.includes(id) ? current(id)!.message : privateObservation(call, { result: { content: [{ type: "text",
					text: "Observation withheld from this continuation branch." }], details: {} }, isError: false }).message));
			request.observations = branch.actionIDs.map(id => current(id)!.key);
			const draft = await completeDraft({ ...previous, context, budgetPolicy: policy }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map(actionID => { const index = branch.actionIDs.indexOf(actionID); return { actionID,
					...index < 0 ? { condition: "resource" as const } : { condition: "prediction" as const, observation: request.observations![index]! } }; }));
			return draft && { proposalID, source: "drafter", revision: reserveRevision?.() ?? revision, upsert: draft.actions };
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
const CONTINUATION_TOOL = "speculative_continuations";
function scopeContext(batch: DrafterBatch, message: AssistantMessage, calls: readonly AgentToolCall[]): Context {
	return { ...batch.context, messages: [...batch.context.messages, { role: "user", timestamp: Date.now(),
		content: `Before seeing this batch's results, choose the observation scopes needed to predict subsequent tool calls. Each scope starts an independent continuation that can read only those call results. Use separate scopes for independent work and a joint scope when results must be combined. Do not duplicate work across scopes.\n${JSON.stringify({ draft: message.content })}` }],
		tools: [{ name: CONTINUATION_TOOL, description: "Select the minimal sets of tool observations needed by subsequent branches.", parameters: Type.Object({
			scopes: Type.Array(Type.Array(Type.Union(calls.map(call => Type.Literal(call.id))), { minItems: 1, uniqueItems: true }), { minItems: 1, uniqueItems: true }),
		}, { additionalProperties: false }) }] };
}
const OBSERVATION_CONTENT = Type.Array(Type.Union([
	Type.Object({ type: Type.Literal("text"), text: Type.String() }),
	Type.Object({ type: Type.Literal("image"), data: Type.String(), mimeType: Type.String() }),
]));
function observationContext(batch: DrafterPlanFeedback, missing: readonly DrafterCall[], observations: readonly ToolResultMessage[]): Context {
	const candidates = missing.map(({ call, action }) => {
		const { version, content = OBSERVATION_CONTENT } = toolObservationContract(action);
		return { call, tool: batch.context.tools?.find(tool => tool.name === call.name), observation: { version, content } };
	});
	return { ...batch.context, messages: [...batch.context.messages, { role: "user", timestamp: Date.now(),
		content: `Predict the pending observations under their bound schemas.\n${JSON.stringify({ draft: batch.message.content,
			observations, candidates })}` }],
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

function privateToolArguments(message: AssistantMessage | undefined, context: Context): Record<string, unknown> | undefined {
	const calls = message?.content.filter((item): item is AgentToolCall => item.type === "toolCall"), tool = context.tools![0]!;
	if (calls?.length !== 1 || calls[0]!.name !== tool.name) return undefined;
	const args = validateToolArguments(tool, calls[0]!);
	return stableValueHash(args) === stableValueHash(calls[0]!.arguments) ? args : undefined;
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
