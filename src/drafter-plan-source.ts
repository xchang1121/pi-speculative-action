import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { widenReadGuess } from "./action-semantics.ts";
import { calculateContextTokens, type AgentToolCall } from "@earendil-works/pi-agent-core";
import { clampThinkingLevel, type Api, type AssistantMessage, type Context, type Model, type SimpleStreamOptions, type ToolResultMessage } from "@earendil-works/pi-ai";
import { clampCandidateLimit, DEFAULTS, drafterRequestTemperature, normalizeDrafterRequestSettings, type DrafterRequestSettings } from "./common.ts";
import { DrafterTaskBudget, drafterInputTokens, type DrafterBudgetSnapshot } from "./drafter-budget.ts";
import { DrafterUtilityGate, type DrafterUtilityBatch, type DrafterUtilityGateSnapshot } from "./drafter-utility-gate.ts";
import { agentBatchKey, type AgentPlanSource, type DraftModelSelection, type DraftOptionsContext } from "./agent-runtime-types.ts";
import type { PlanAction, PlanProposal } from "./plan-proposal.ts";
import type { ActorActionFeedback, SpeculativeActionSettings } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";

interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions & { readonly toolChoice?: "auto" | "required" };
	readonly utility: DrafterUtilityBatch;
	readonly budgetPolicy: DrafterRequestSettings;
	readonly tools: ReadonlySet<string>;
	readonly schemaHashes: Readonly<Record<string, string>>;
	readonly prepareExecution?: () => void;
	readonly expansion: { readonly key: string; readonly stages: Map<string, DrafterUtilityBatch>; finished: boolean };
	readonly marginalUtilities?: readonly DrafterUtilityBatch[];
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
	readonly snapshot: () => DrafterUtilityGateSnapshot & { readonly budget: DrafterBudgetSnapshot };
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
	/** Share this ledger with other model Drafter requests in the same user task. */
	readonly drafterBudget?: DrafterTaskBudget;
	readonly patternHints?: (input: { readonly sessionID: string; readonly schemaHashes: Readonly<Record<string, string>>; readonly settings: SpeculativeActionSettings })
		=> Promise<readonly { readonly tool: string; readonly input: Readonly<Record<string, unknown>> }[]>;
}): DrafterPlanSourceController {
	const batches = new Map<string, DrafterPreparation>();
	const budget = input.drafterBudget ?? new DrafterTaskBudget();
	const gate = new DrafterUtilityGate();
	const expansionGate = new DrafterUtilityGate();
	// Separate Beta(1, 1) estimates over the latest 32 eligible outcomes per model and tool contract.
	const calibration = new BoundedRecencyMap<string, { matches: number[]; adoptions: number[] }>(128);
	const calibrationKey = (batch: DrafterBatch, tool: string) => JSON.stringify([batch.utility.key, tool, batch.schemaHashes[tool]]);
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
		// Model/auth failures are already represented by source request events.
		void batch?.ready.then((value) => {
			if (!value) return;
			gate.finish(value.utility);
			value.expansion.finished = true;
			for (const utility of value.expansion.stages.values()) expansionGate.finish(utility);
		}).catch(() => {});
	};
	const completeDraft = async (batch: DrafterBatch, signal: AbortSignal, report: ((tokens: number) => void) | undefined,
		prefix: string, depth = 0, dependsOn?: PlanAction["dependsOn"], width = 0) => {
		signal.throwIfAborted();
		// Each additional width/depth spends its own exploration budget and must repay itself through adoption.
		const stage = depth > 0 ? `depth:${depth}` : width > 0 ? `width:${width}` : undefined;
		let marginal: DrafterUtilityBatch | undefined;
		if (stage) {
			marginal = batch.expansion.stages.get(stage);
			if (!marginal) {
				marginal = expansionGate.start(JSON.stringify([batch.expansion.key, stage]), batch.utility.policy.enabled);
				marginal.finished = batch.expansion.finished;
				batch.expansion.stages.set(stage, marginal);
			}
			if (!marginal.allowed) return undefined;
		}
		let failed = false, started = false;
		try {
			const { options } = batch, forced = options.toolChoice === "required" ? { onPayload: forceToolChoice(options.onPayload) } : {};
			const message = await budget.run({ model: batch.model, context: batch.context, options: { ...options, ...forced, signal },
				policy: batch.budgetPolicy, complete: input.complete, started: () => {
					if (!batch.utility.startedRequests) batch.prepareExecution?.();
					started = true; gate.requestStarted(batch.utility);
					if (marginal) expansionGate.requestStarted(marginal);
				} });
			if (!message) return undefined;
			report?.(calculateContextTokens(message.usage));
			if (message.stopReason === "error" || message.stopReason === "aborted")
				throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
			const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
			// Calls to tools outside the prediction list drop out; the partial batch can no longer be continued as a whole.
			const kept = calls.filter((call) => batch.tools.has(call.name));
			if (!kept.length || new Set(calls.map((call) => call.id)).size !== calls.length) return undefined;
			const marginalUtilities = [...new Set([...(batch.marginalUtilities ?? []), ...(marginal ? [marginal] : [])])];
			const feedback: DrafterPlanFeedback = { ...batch, marginalUtilities, kind: "drafter_plan", message, depth,
				calls: new Map(kept.map((call, index) => [`${prefix}:${index}`, call])), results: new Map(), claimed: kept.length < calls.length };
			return { actions: [...feedback.calls].map(([id, call]): PlanAction => ({
				id, type: "tool_call", tool: call.name, input: widenReadGuess(call.name, call.arguments), depth, feedback, dependsOn, ...probabilities(batch, call.name),
				diagnostic: JSON.stringify({ toolCallID: call.id, tool: call.name, input: call.arguments }, null, 2),
			})) };
		} catch (error) {
			failed = !signal.aborted;
			throw error;
		} finally {
			if (started) {
				gate.requestSettled(batch.utility, failed);
				if (marginal) expansionGate.requestSettled(marginal, failed);
			}
		}
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
			if (settlement.match.matched) {
				const adoption = settlement.match.adoption;
				// Deliberate Actor calibration supplies timing evidence, not evidence that this result was unusable.
				if (adoption.status === "rejected" && adoption.cause.code === "candidate_calibration_sample") return;
				observe(samples.adoptions, adoption.status === "adopted");
			}
		},
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
			if (!budget.available(drafter)) return undefined;
			const batchKey = agentBatchKey(startInput.sessionID, startInput.turnID);
			let batch = batches.get(batchKey);
			if (!batch) {
				const tools = new Set(candidateNames.filter((name) => data.tools.has(name)));
				if (!tools.size) return undefined;
				batch = new DrafterPreparation(async (signal) => {
					const { draftModel, getDraftOptions } = input, { actorModel, actorOptions } = startInput;
					// Hints trail the Actor's history, so the cached prefix the Drafter shares with the Actor stays intact.
					const hints = drafter.drafterPatternHints ? await input.patternHints?.({ sessionID: startInput.sessionID, schemaHashes: data.schemaHashes, settings }) : undefined;
					const context: Context = hints?.length ? { ...startInput.context, messages: [...startInput.context.messages, { role: "user", timestamp: Date.now(),
						content: `(Speculation hint, not from the user.) Calls that followed similar steps in this workspace:\n${hints.map((hint) => `- ${hint.tool} ${JSON.stringify(hint.input)}`).join("\n")}` }] }
						: startInput.context;
					const model = (typeof draftModel === "function" ? await draftModel(actorModel) : draftModel) ?? actorModel;
					if (signal.aborted) return undefined;
					const utility = gate.start(JSON.stringify([model.provider, model.api, model.baseUrl, model.id]), settings.sourceConfig?.drafterGateEnabled !== false);
					if (!utility.allowed || !drafterContextFits(model, context, drafter.drafterMaxTokens)) return undefined;
					const configuredDraftOptions = getDraftOptions ? await getDraftOptions({ actorModel, draftModel: model, actorOptions, signal }) : actorOptions;
					if (signal.aborted) return undefined;
					// Inherit transport options, while the Drafter owns its reasoning and output budget.
					const { maxTokens: _actorMaxTokens, reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
					const reasoning = clampThinkingLevel(model, getDraftOptions ? requestedReasoning ?? "off" : "off");
					const schemaHashes = { ...data.schemaHashes };
					return { model, context, options: { ...requestOptions, reasoning: reasoning === "off" ? undefined : reasoning }, utility, budgetPolicy: drafter, tools, schemaHashes,
						prepareExecution: () => data.prepareExecution?.(candidateNames, signal),
						expansion: { key: JSON.stringify([utility.key, Object.entries(schemaHashes).sort(([a], [b]) => a.localeCompare(b))]), stages: new Map(), finished: false } };
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
				const draft = await completeDraft({ ...prepared, options: draftOptions }, signal, reportDraftTokens, String(proposalIndex), 0, undefined, proposalIndex);
				return draft && { id: `drafter:${startInput.turnID}:${proposalIndex}`, source: "drafter", revision: 0, ...draft };
			});
		},
		continue: async ({ proposalID, revision, feedback, signal, reportDraftTokens, settings }) => {
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
			const draft = await completeDraft({ ...previous, context, options, budgetPolicy: normalizeDrafterRequestSettings(settings.sourceConfig) }, signal, reportDraftTokens, `rollout:${revision}`, previous.depth + 1,
				[...previous.calls.keys()].map((actionID) => ({ actionID, condition: "execution_succeeded" })));
			return draft && { proposalID, source: "drafter", revision, upsert: draft.actions };
		},
		// A peer's executed batch, such as a fork's early calls, rolls out as the Drafter's own does: its calls and results extend the Actor context.
		continueFrom: async ({ startInput, settings, batch: peers, signal, reportDraftTokens }) => {
			const prepared = await batches.get(agentBatchKey(startInput.sessionID, startInput.turnID))?.ready.catch(() => undefined);
			const drafter = normalizeDrafterRequestSettings(settings.sourceConfig), ids = peers.map((_, index) => `peer:${index}`);
			if (!prepared || signal.aborted) return undefined;
			const zero = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, message: AssistantMessage = { role: "assistant", api: prepared.model.api,
				provider: prepared.model.provider, model: prepared.model.id, stopReason: "toolUse", timestamp: Date.now(), usage: { ...zero, totalTokens: 0, cost: { ...zero, total: 0 } },
				content: peers.map(({ candidate }, index) => ({ type: "toolCall", id: ids[index]!, name: candidate.tool, arguments: { ...candidate.input } })) };
			const context: Context = { ...prepared.context, messages: [...prepared.context.messages, message, ...peers.map(({ candidate, output }, index): ToolResultMessage => ({
				role: "toolResult", toolCallId: ids[index]!, toolName: candidate.tool, content: output.result.content, details: output.result.details, isError: output.isError, timestamp: Date.now() }))] };
			const options = { ...prepared.options, temperature: drafterRequestTemperature(0, 1, drafter), maxTokens: drafter.drafterMaxTokens, toolChoice: "auto" as const };
			if (!drafterContextFits(prepared.model, context, options.maxTokens)) return undefined;
			const id = `drafter:peer:${stableValueHash(peers.map(({ identity }) => identity.id))}`;
			const draft = await completeDraft({ ...prepared, context, options, budgetPolicy: drafter }, signal, reportDraftTokens, id, 1, peers.map(({ identity }) =>
				({ proposalID: identity.proposalID, actionID: identity.actionID, identity: identity.id, condition: "execution_succeeded" as const })));
			return draft && { id, source: "drafter", revision: 0, actions: draft.actions };
		},
	};

	return {
		source,
		snapshot: () => ({ ...gate.snapshot(), budget: budget.snapshot() }),
		finishTurn: (sessionID, turnID) => finishBatch(agentBatchKey(sessionID, turnID)),
		actorActionSettled: async ({ settlement, candidate, candidateFeedback, sessionID, turnID }) => {
			const sources = new Set(settlement.matchedPredictions.map((prediction) => prediction.source));
			if (settlement.provider.kind !== "speculative" || !sources.has("drafter")) return;
			// Every source that predicted the adopted call shares its credit, whichever one executed it.
			const owner = candidate?.source === "drafter" ? asDrafterPlanFeedback(candidateFeedback) : undefined;
			const utility = owner?.utility ?? (await batches.get(agentBatchKey(sessionID, turnID))?.ready.catch(() => undefined))?.utility;
			if (utility) gate.creditAdoption(utility, settlement.provider.timing, sources.size);
			// Descendants credit the extra root/depth that made them possible, once per stage and Actor settlement.
			for (const marginal of owner?.marginalUtilities ?? []) expansionGate.creditAdoption(marginal, settlement.provider.timing, sources.size);
		},
		finishSession: () => { for (const key of batches.keys()) finishBatch(key); budget.finishTask(); },
	};
}

/** Simple streams drop toolChoice for most APIs; spell a forced call on the final payload where the API has one. */
export function forceToolChoice(inherited: SimpleStreamOptions["onPayload"]): SimpleStreamOptions["onPayload"] {
	return async (payload, model) => {
		const next = (await inherited?.(payload, model)) ?? payload, forced = FORCED_TOOL_CHOICE[model.api];
		return forced && next && typeof next === "object" && "tools" in next ? { ...next, tool_choice: forced } : next;
	};
}
const FORCED_TOOL_CHOICE: Readonly<Record<string, unknown>> = { "anthropic-messages": { type: "any" }, "openai-completions": "required",
	"openai-responses": "required", "azure-openai-responses": "required", "openai-codex-responses": "required" };

/** Preserve the Actor-visible history whole; a shorter Drafter skips instead of compacting it. */
function drafterContextFits(model: Model<Api>, context: Context, maxTokens: number | undefined): boolean {
	const output = Math.min(maxTokens ?? model.maxTokens, model.maxTokens);
	return drafterInputTokens(context) + output <= model.contextWindow;
}

function asDrafterPlanFeedback(value: unknown): DrafterPlanFeedback | undefined {
	return value && typeof value === "object" && (value as { kind?: unknown }).kind === "drafter_plan"
		? (value as DrafterPlanFeedback)
		: undefined;
}
