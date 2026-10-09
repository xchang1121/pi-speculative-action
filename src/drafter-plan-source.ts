import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { widenReadGuess } from "./action-semantics.ts";
import { calculateContextTokens, type AgentToolCall } from "@earendil-works/pi-agent-core";
import { clampThinkingLevel, fauxAssistantMessage, type Api, type AssistantMessage, type Context, type Model, type SimpleStreamOptions, type ToolResultMessage } from "@earendil-works/pi-ai";
import { clampCandidateLimit, DEFAULTS, drafterRequestTemperature, normalizeDrafterRequestSettings, type DrafterRequestSettings } from "./common.ts";
import { DrafterTaskBudget, drafterInputTokens, drafterOpportunityKey, type DrafterBudgetSnapshot } from "./drafter-budget.ts";
import { agentBatchKey, type AgentPlanSource, type DraftModelSelection, type DraftOptionsContext } from "./agent-runtime-types.ts";
import type { PlanAction, PlanProposal } from "./plan-proposal.ts";
import type { PatternAwareCandidate } from "./pattern-aware.ts";
import type { SpeculativeActionSettings } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import { asRecord } from "./stable-json.ts";
import { SourceRequestSuppressed } from "./source-request.ts";
import { cause } from "./settlement.ts";

type WorkflowHint = Pick<PatternAwareCandidate, "tool" | "input"> &
	Partial<Pick<PatternAwareCandidate, "horizon">>;

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly hints?: readonly WorkflowHint[];
	readonly options: SimpleStreamOptions & { readonly toolChoice?: "auto" | "required" };
	readonly key: string;
	readonly requests: { started: number };
	readonly budgetPolicy: DrafterRequestSettings;
	readonly tools: ReadonlySet<string>;
	readonly prepareExecution?: () => void;
}

interface DrafterPlanFeedback extends DrafterBatch {
	readonly kind: "drafter_plan";
	readonly message: AssistantMessage;
	readonly depth: number;
	readonly calls: ReadonlyMap<string, AgentToolCall>;
	readonly results: Map<string, ToolResultMessage>;
	readonly predecessors: readonly DrafterPlanFeedback[];
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
		=> Promise<readonly WorkflowHint[]>;
}) {
	const batches = new Map<string, DrafterPreparation>();
	const budget = input.drafterBudget ?? new DrafterTaskBudget();
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
		batch?.dispose();
	};
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, report: ((tokens: number) => void) | undefined,
		prefix: string, depth = 0, dependsOn?: PlanAction["dependsOn"]) => {
		signal.throwIfAborted();
		const { options } = batch, steps = batch.context.tools?.some(tool => tool.name === WORKFLOW_TOOL) ? 1 : batch.budgetPolicy.drafterMaxDepth - depth + 1;
		const context = workflowContext(batch.context, batch.tools, steps);
		const inputTokens = drafterInputTokens({ ...context, tools: batch.context.tools }) + (context === batch.context ? 0 :
			Math.ceil((JSON.stringify(context.tools).length - JSON.stringify(batch.context.tools ?? []).length) / 4));
		if (!drafterContextFits(batch.model, context, options.maxTokens, inputTokens)) suppress("drafter_context_limit");
		const forced = options.toolChoice === "required" ? { onPayload: forceToolChoice(options.onPayload, steps > 1 ? WORKFLOW_TOOL : undefined) } : {};
		const message = await budget.run({ model: batch.model, context, inputTokens, options: { ...options, ...forced, signal },
			policy: batch.budgetPolicy, complete: input.complete, onSkipped: suppress,
			started: () => { if (++batch.requests.started === 1) batch.prepareExecution?.(); } });
		if (!message) return undefined;
		report?.(calculateContextTokens(message.usage));
		if (message.stopReason === "error" || message.stopReason === "aborted")
			throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
		const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
		const workflow = steps > 1 && calls.length === 1 && calls[0]!.name === WORKFLOW_TOOL;
		const planned = workflow ? asRecord(calls[0]!.arguments)?.steps : undefined;
		if (workflow && (!Array.isArray(planned) || !planned.length || planned.length > steps)) return undefined;
		const groups = workflow ? (planned as unknown[]).map((value, index) => {
			const step = asRecord(value), input = asRecord(step?.input);
			return step && typeof step.tool === "string" && input ? [{ type: "toolCall" as const,
				id: `${calls[0]!.id}:${index}`, name: step.tool, arguments: input }] : [];
		}) : [calls];
		if (groups.some(group => !group.length) || new Set(calls.map(call => call.id)).size !== calls.length) return undefined;
		const actions: PlanAction[] = [], predecessors: DrafterPlanFeedback[] = [];
		for (const [index, group] of groups.entries()) {
			const kept = group.filter(call => batch.tools.has(call.name));
			// A removed workflow step may mutate inputs: never jump across it to later steps.
			if (workflow && kept.length !== group.length) break;
			const feedback: DrafterPlanFeedback = { ...batch, kind: "drafter_plan", depth: depth + index,
				message: workflow ? draftMessage(batch, kept) : message, predecessors: [...predecessors],
				calls: new Map(kept.map((call, member) => [`${prefix}:${index}:${member}`, call])), results: new Map(),
				claimed: kept.length < group.length || index < groups.length - 1 };
			for (const [id, call] of feedback.calls) actions.push({ id, type: "tool_call", tool: call.name,
				input: widenReadGuess(call.name, call.arguments), depth: feedback.depth, feedback, dependsOn, ...probabilities(batch, call.name) });
			dependsOn = [...feedback.calls.keys()].map(actionID => ({ actionID, condition: "execution_succeeded" }));
			predecessors.push(feedback);
		}
		return actions.length ? { actions } : undefined;
	};
	const source: AgentPlanSource = {
		id: "drafter",
		onSettled: ({ actionID, feedback, settlement }) => {
			const batch = asDrafterPlanFeedback(feedback), tool = batch?.calls.get(actionID)?.name;
			if (!batch || !tool || settlement.observation !== "observed") return;
			const key = calibrationKey(batch, tool);
			let samples = calibration.get(key);
			if (!samples) { samples = { matches: [], adoptions: [] }; calibration.set(key, samples); }
			observe(samples.matches, settlement.match.matched);
			if (settlement.match.matched) observe(samples.adoptions, settlement.match.adoption.status === "adopted");
		},
		enabled: (settings) => settings.drafterEnabled ?? DEFAULTS.drafterEnabled,
		timeoutMs: (settings) => settings.predictionTimeoutMs,
		requestLifetime: "turn",
		continuationBatch: ({ feedback }) => {
			const batch = asDrafterPlanFeedback(feedback);
			return batch && !batch.claimed ? [...batch.calls.keys()] : undefined;
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
			if (trigger !== "execution_succeeded" || !previous || !call || previous.results.has(actionID)) return false;
			previous.results.set(actionID, { ...output.result, role: "toolResult", toolCallId: call.id,
				toolName: call.name, isError: output.isError, timestamp: Date.now() });
			return !previous.claimed && previous.results.size === previous.calls.size;
		},
		proposalCount: (settings) => clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
		concurrentProposalPolicy: (settings) =>
			clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit) === 2 ? "first_produced" : "all",
		propose: async ({ startInput, data, candidateNames, proposalIndex, proposalCount, signal, settings, reportDraftTokens }): Promise<PlanProposal | undefined> => {
			if (signal.aborted) return undefined;
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
			if (!budget.available(drafter, suppress)) return undefined;
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				const tools = new Set(candidateNames.filter((name) => data.tools.has(name)));
				if (!tools.size) return undefined;
				batch = new DrafterPreparation(async (signal) => {
					const { draftModel, getDraftOptions } = input, { actorModel, actorOptions } = startInput;
					// Optional pattern hints follow the Actor history to preserve its prefix.
					const hints = await input.patternHints?.({ sessionID: startInput.sessionID, schemaHashes: data.schemaHashes, settings });
					const context: Context = drafter.drafterPatternHints && hints?.length ? { ...startInput.context, messages: [...startInput.context.messages, { role: "user", timestamp: Date.now(),
						content: `(Speculation hint, not from the user.) Calls that followed similar steps in this workspace:\n${hints.map((hint) => `- ${hint.tool} ${JSON.stringify(hint.input)}${hint.horizon === undefined ? "" : `; expected after ${hint.horizon} batches`}`).join("\n")}` }] }
						: startInput.context;
					const model = (typeof draftModel === "function" ? await draftModel(actorModel) : draftModel) ?? actorModel;
					if (signal.aborted) return undefined;
					const key = drafterOpportunityKey(model, startInput.context, data.schemaHashes, hints);
					if (!drafterContextFits(model, context, drafter.drafterMaxTokens)) suppress("drafter_context_limit");
					const configuredDraftOptions = getDraftOptions ? await getDraftOptions({ actorModel, draftModel: model, actorOptions, signal }) : actorOptions;
					if (signal.aborted) return undefined;
					// Inherit transport options, while the Drafter owns its reasoning and output budget.
					const { maxTokens: _actorMaxTokens, reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
					const reasoning = clampThinkingLevel(model, getDraftOptions ? requestedReasoning ?? "off" : "off");
					return { model, context, hints, options: { ...requestOptions, reasoning: reasoning === "off" ? undefined : reasoning }, key, requests: { started: 0 }, budgetPolicy: drafter, tools,
						prepareExecution: () => data.prepareExecution?.(candidateNames, signal) };
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
				const draft = await completeDraft({ ...prepared, options: draftOptions }, signal, reportDraftTokens, String(proposalIndex));
				return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
			});
		},
		continue: async ({ proposalID, revision, feedback, signal, reportDraftTokens, settings }) => {
			const previous = asDrafterPlanFeedback(feedback);
			if (!previous || signal.aborted || previous.claimed || [...previous.predecessors, previous].some(batch => batch.results.size !== batch.calls.size)) return undefined;
			previous.claimed = true;
			const context = continuationContext(previous.context, [...previous.predecessors, previous]);
			const options = { ...previous.options, toolChoice: "auto" as const };
			const draft = await completeDraft({ ...previous, context, options, budgetPolicy: normalizeDrafterRequestSettings(settings.sourceConfig) }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map((actionID) => ({ actionID, condition: "execution_succeeded" })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions };
		},
		// A peer's executed batch, such as a fork's early calls, rolls out as the Drafter's own does: its calls and results extend the Actor context.
		continueFrom: async ({ startInput, settings, batch: peers, signal, reportDraftTokens }) => {
			const prepared = await batches.get(agentBatchKey(startInput.sessionID, startInput.turnID))?.ready.catch(() => undefined);
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig), ids = peers.map((_, index) => `peer:${index}`);
			if (!prepared || signal.aborted) return undefined;
			const calls = new Map(peers.map(({ candidate }, index) => [ids[index]!, { type: "toolCall" as const, id: ids[index]!, name: candidate.tool, arguments: { ...candidate.input } }]));
			const context = continuationContext(prepared.context, [{ calls, message: draftMessage(prepared, [...calls.values()]),
				results: new Map(peers.map(({ candidate, output }, index) => [ids[index]!, { ...output.result, role: "toolResult" as const,
					toolCallId: ids[index]!, toolName: candidate.tool, isError: output.isError, timestamp: Date.now() }])) }]);
			const options = { ...prepared.options, temperature: drafterRequestTemperature(0, 1, drafter), maxTokens: drafter.drafterMaxTokens, toolChoice: "auto" as const };
			const id = `drafter:peer:${stableValueHash(peers.map(({ identity }) => identity.id))}`;
			const draft = await completeDraft({ ...prepared, context, options, budgetPolicy: drafter }, signal, reportDraftTokens, id, 1, peers.map(({ identity }) =>
				({ proposalID: identity.proposalID, actionID: identity.actionID, identity: identity.id, condition: "execution_succeeded" as const })));
			return draft && { id, source: "drafter", revision: 0, actions: draft.actions };
		},
	};

	return {
		source,
		snapshot: (): { readonly budget: DrafterBudgetSnapshot } => ({ budget: budget.snapshot() }),
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

const WORKFLOW_TOOL = "speculative_workflow";
function workflowContext(context: Context, tools: ReadonlySet<string>, maxSteps: number): Context {
	if (maxSteps <= 1) return context;
	return { ...context, messages: [...context.messages, { role: "user", timestamp: Date.now(),
		content: "You are predicting the Actor's next tool workflow for the conversation above. Respond with exactly one speculative_workflow call. Put every predicted native tool invocation in steps using its tool name and original input object; native tool names are not callable at the top level in this request. Include later steps only when their complete arguments are already supported by the conversation; stop before any step that needs an unseen result. Do not invent tool output." }], tools: [{ name: WORKFLOW_TOOL,
		description: "Ordered native tool predictions, including known costly validation; executed serially in one speculative workspace.",
		parameters: { type: "object", required: ["steps"], properties: { steps: { type: "array", minItems: 1, maxItems: maxSteps,
			items: { anyOf: (context.tools ?? []).filter(tool => tools.has(tool.name)).map(tool => ({ type: "object", description: tool.description,
				required: ["tool", "input"], properties: { tool: { type: "string", enum: [tool.name] }, input: tool.parameters } })) } } } } }] };
}

function suppress(reason: string, detail?: string): never { throw new SourceRequestSuppressed(cause("source", reason, detail)); }

function draftMessage(batch: DrafterBatch, calls: readonly AgentToolCall[]): AssistantMessage {
	return { ...fauxAssistantMessage([...calls], { stopReason: "toolUse" }), api: batch.model.api, provider: batch.model.provider, model: batch.model.id };
}

function continuationContext(context: Context, batches: readonly Pick<DrafterPlanFeedback, "message" | "calls" | "results">[]): Context {
	return { ...context, messages: [...context.messages, ...batches.flatMap(batch =>
		[batch.message, ...[...batch.calls.keys()].map(id => batch.results.get(id)!)])] };
}

/** Preserve the Actor-visible history whole; a shorter Drafter skips instead of compacting it. */
function drafterContextFits(model: Model<Api>, context: Context, maxTokens: number | undefined, inputTokens = drafterInputTokens(context)): boolean {
	const output = Math.min(maxTokens ?? model.maxTokens, model.maxTokens);
	return inputTokens + output <= model.contextWindow;
}

function asDrafterPlanFeedback(value: unknown): DrafterPlanFeedback | undefined {
	return value && typeof value === "object" && (value as { kind?: unknown }).kind === "drafter_plan"
		? (value as DrafterPlanFeedback)
		: undefined;
}
