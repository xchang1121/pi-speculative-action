import type { AgentToolCall } from "@earendil-works/pi-agent-core";
import { calculateContextTokens } from "@earendil-works/pi-agent-core";
import type {
	Api,
	AssistantMessage,
	Context,
	Model,
	SimpleStreamOptions,
	ToolResultMessage,
} from "@earendil-works/pi-ai";
import { clampThinkingLevel, fauxAssistantMessage, Type, validateToolArguments } from "@earendil-works/pi-ai";
import type { ActionKey } from "./action-semantics.ts";
import type { DrafterModelOptions } from "./agent-integration.ts";
import type { CallGenerator, CompletedCall, GeneratedCall, GeneratorTurn } from "./call-generators.ts";
import type { DrafterRequestSettings } from "./config-schema.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	speculativeConfig,
	drafterRequestTemperature,
	normalizeDrafterRequestSettings,
} from "./config-schema.ts";
import type { ToolSettlement } from "./execution-world.ts";
import { canonicalToolObservation, toolObservationContract } from "./execution-world.ts";
import { BoundedRecencyMap, immutableSnapshot, stableValueHash } from "./primitives.ts";
import { coversDrafterRoot, type PatternPlanSourceController } from "./pattern-plan-source.ts";
import { waitForCompletion } from "./runtime-lifecycle.ts";
import { predictionLearning, SourceRequestSuppressed } from "./settlement.ts";
export interface DrafterBudgetSnapshot {
	readonly requests: number;
	readonly reportedTokens: number;
	readonly unreportedRequests: number;
}
/** Shared usage accounting; request limits belong to the host and provider. */
export class DrafterTaskBudget {
	private latest = this.empty();
	private active: typeof this.latest | undefined = this.latest;
	async run(input: {
		readonly model: Model<Api>;
		readonly context: Context;
		readonly options?: SimpleStreamOptions;
		readonly complete: (
			model: Model<Api>,
			context: Context,
			options?: SimpleStreamOptions,
		) => Promise<AssistantMessage>;
	}): Promise<AssistantMessage> {
		input.options?.signal?.throwIfAborted();
		const state = (this.active ??= this.latest = this.empty());
		state.requests++;
		state.unreportedRequests++;
		const message = await input.complete(input.model, input.context, input.options);
		const tokens = calculateContextTokens(message.usage);
		if (
			Number.isFinite(tokens) &&
			tokens >= 0 &&
			(tokens > 0 || !["error", "aborted"].includes(message.stopReason))
		) {
			state.reportedTokens += tokens;
			state.unreportedRequests--;
		}
		return message;
	}
	finishTask(): void {
		this.active = undefined;
	}
	snapshot(): DrafterBudgetSnapshot {
		return { ...this.latest };
	}
	private empty() {
		return { requests: 0, reportedTokens: 0, unreportedRequests: 0 };
	}
}
/** Probability calibration follows actual tool context and workflow shape, never argument churn. */
export function drafterOpportunityKey(
	model: Model<Api>,
	context: Context,
	schemas?: Readonly<Record<string, string>>,
	hints: readonly {
		readonly tool: string;
		readonly horizon?: number;
	}[] = [],
	kind = "root",
): string {
	const previous = [...context.messages]
		.reverse()
		.find(
			(message) => message.role === "assistant" && message.content.some((part) => part.type === "toolCall"),
		);
	const calls =
		previous?.role === "assistant"
			? [...new Set(previous.content.flatMap((part) => (part.type === "toolCall" ? [part.name] : [])))].sort()
			: [];
	return JSON.stringify([
		model.provider,
		model.api,
		model.baseUrl,
		model.id,
		kind,
		stableValueHash(schemas ?? context.tools ?? []),
		calls,
		[...new Set(hints.map((hint) => JSON.stringify([hint.tool, hint.horizon ?? 0])))].sort(),
	]);
}
interface DrafterBatch {
	readonly model: Model<Api>;
	readonly context: Context;
	readonly options: SimpleStreamOptions;
	readonly key: string;
	readonly budgetPolicy: DrafterRequestSettings;
}
interface DrafterPlanFeedback extends DrafterBatch {
	readonly message: AssistantMessage;
}
interface DrafterObservation {
	readonly message: ToolResultMessage;
	readonly key: string;
}
interface DrafterCall {
	readonly call: AgentToolCall;
	action?: ActionKey;
	result?: DrafterObservation;
}
export interface DrafterPlanSnapshot {
	readonly budget: DrafterBudgetSnapshot;
	readonly observations: {
		readonly forecasted: number;
		readonly compared: number;
		readonly matched: number;
	};
}
export function createDrafterPlanSource(
	input: DrafterModelOptions & {
		readonly sessionID: string;
		readonly patternHints?: PatternPlanSourceController["hints"];
	},
) {
	const batches = new WeakMap<GeneratorTurn, Promise<(DrafterBatch & { skipRoot: boolean }) | undefined>>();
	const budget = input.drafterBudget ?? new DrafterTaskBudget();
	const observations = { forecasted: 0, compared: 0, matched: 0 };
	const latencies = new BoundedRecencyMap<string, number>(speculativeConfig().prediction.calibrationBuckets);
	const latencyKey = (batch: DrafterBatch, tool?: string) =>
		JSON.stringify([batch.model.provider, batch.model.baseUrl, batch.model.id, tool ?? null]);
	// Separate Beta(1, 1) estimates over the latest 32 eligible outcomes per model and tool contract.
	const calibration = new BoundedRecencyMap<
		string,
		{
			matches: number[];
			adoptions: number[];
		}
	>(speculativeConfig().prediction.calibrationBuckets);
	const calibrationKey = (batch: DrafterBatch, tool: string) => JSON.stringify([batch.key, tool]);
	const probability = (samples: readonly number[] = []) =>
		(samples.reduce((sum, value) => sum + value, 0) +
			speculativeConfig().prediction.calibrationSuccessPrior) /
		(samples.length +
			speculativeConfig().prediction.calibrationSuccessPrior +
			speculativeConfig().prediction.calibrationFailurePrior);
	const observe = (samples: number[], value: number) => {
		samples.push(value);
		if (samples.length > speculativeConfig().prediction.calibrationWindow) samples.shift();
	};
	const completeRequest = async (
		batch: DrafterBatch,
		context: Context,
		signal: AbortSignal,
		report: ((tokens: number) => void) | undefined,
		force?: true | string,
	) => {
		signal.throwIfAborted();
		const options = {
			...batch.options,
			deferred: false,
			sessionId: batch.options.sessionId ?? input.sessionID,
			cacheRetention: batch.options.cacheRetention ?? ("short" as const),
		};
		const forced =
			!options.reasoning && force
				? {
						onPayload: forceToolChoice(options.onPayload, force === true ? undefined : force),
					}
				: {};
		const started = performance.now();
		const response = await waitForCompletion(
			budget
				.run({
					model: batch.model,
					context,
					options: { ...options, ...forced, signal },
					complete: input.complete,
				})
				.then((message) => {
					if (!message) return undefined;
					report?.(calculateContextTokens(message.usage));
					if (message.stopReason === "error" || message.stopReason === "aborted")
						throw new Error(message.errorMessage ?? `Drafter stopped with ${message.stopReason}`);
					return message;
				}),
			signal,
		);
		if (response.status !== "completed") return;
		if (response.value) latencies.set(latencyKey(batch), performance.now() - started);
		return response.value;
	};
	const prepare = (turn: GeneratorTurn) => {
		const existing = batches.get(turn);
		if (existing) return existing;
		const { startInput, data, settings, signal } = turn;
		const drafter = normalizeDrafterRequestSettings(settings.sourceConfig);
		const ready = (async () => {
			const { draftModel, getDraftOptions } = input;
			const { actorModel, actorOptions } = startInput;
			// Optional pattern hints follow the Actor history to preserve its prefix.
			const hints = await input
				.patternHints?.({
					sessionID: startInput.sessionID,
					startInput,
					schemaHashes: data.schemaHashes,
					patternIdentities: data.patternIdentities,
					settings,
				})
				.catch(() => []);
			if (signal.aborted) return undefined;
			const selected: Context = settings.tools
				? {
						...startInput.context,
						tools: startInput.context.tools?.filter((tool) => settings.tools!.includes(tool.name)),
					}
				: startInput.context;
			const context: Context =
				drafter.drafterPatternHints && hints?.length
					? {
							...selected,
							messages: [
								...selected.messages,
								{
									role: "user",
									timestamp: Date.now(),
									content: `(Speculation hint, not from the user.) Calls that followed similar steps in this workspace:\n${hints.map((hint) => `- ${hint.tool} ${JSON.stringify(hint.input)}${hint.horizon === undefined ? "" : `; expected after ${hint.horizon} batches`}`).join("\n")}`,
								},
							],
						}
					: selected;
			const model =
				(typeof draftModel === "function" ? await draftModel(actorModel) : draftModel) ?? actorModel;
			if (signal.aborted) return undefined;
			const key = drafterOpportunityKey(model, startInput.context, data.schemaHashes, hints);
			const configuredDraftOptions = getDraftOptions
				? await getDraftOptions({
						actorModel,
						draftModel: model,
						actorOptions,
						signal,
					})
				: actorOptions;
			if (signal.aborted) return undefined;
			const { reasoning: requestedReasoning, ...requestOptions } = configuredDraftOptions ?? {};
			const reasoning = clampThinkingLevel(model, getDraftOptions ? (requestedReasoning ?? "off") : "off");
			return {
				model,
				context,
				options: {
					...requestOptions,
					reasoning: reasoning === "off" ? undefined : reasoning,
				},
				key,
				budgetPolicy: drafter,
				skipRoot: hints?.some(coversDrafterRoot) ?? false,
			};
		})();
		batches.set(turn, ready);
		return ready;
	};
	const draft = async (
		turn: GeneratorTurn,
		batch: DrafterBatch,
		signal: AbortSignal,
		group: string,
		depth = 0,
		after?: string,
		admitted?: () => void,
	): Promise<void> => {
		await turn.request("drafter", depth ? "continuation" : "proposal", signal, async (signal, report) => {
			const message = await completeRequest(
				batch,
				batch.context,
				signal,
				report,
				depth === 0 ? true : undefined,
			);
			if (!message) return 0;
			const calls = message.content.filter((item): item is AgentToolCall => item.type === "toolCall");
			if (!calls.length || new Set(calls.map((call) => call.id)).size !== calls.length) return 0;
			const members: (DrafterCall & { issued?: GeneratedCall; started: number })[] = [];
			let previousWrite: string | undefined;
			for (const [index, call] of calls.entries()) {
				const bucket = calibrationKey(batch, call.name);
				const samples = calibration.get(bucket);
				const issued = await turn.issue(
					{
						group,
						id: String(index),
						source: "drafter",
						tool: call.name,
						input: call.arguments,
						depth,
						empiricalProbability: probability(samples?.matches),
						adoptionProbability: probability(samples?.adoptions),
						onFeedback: (settlement, served) => {
							const result = predictionLearning(settlement, served);
							const row = calibration.getOrCreate(bucket, () => ({ matches: [], adoptions: [] }));
							if (result.matched !== undefined) observe(row.matches, Number(result.matched));
							if (result.adopted !== undefined) observe(row.adoptions, Number(result.adopted));
						},
					},
					{ after, previousWrite, signal },
				);
				members.push({ call, action: issued?.key, issued, started: performance.now() });
				if (issued) {
					// The Drafter owns every run whose arguments depend on this private forecast.
					const withdraw = () => {
						if (!turn.signal.aborted) void turn.session.release(issued.run.runID);
					};
					if (signal.aborted) withdraw();
					else signal.addEventListener("abort", withdraw, { once: true });
					if (!signal.aborted) admitted?.();
				}
				if (issued && issued.effect !== "observation") previousWrite = issued.run.runID;
			}
			if (signal.aborted) return 0;
			const results = members.map(async (member) => {
				if (!member.issued) return;
				const result = await member.issued.run.completion;
				if (result.status !== "completed") return;
				latencies.set(latencyKey(batch, member.call.name), performance.now() - member.started);
				member.result = privateObservation(member.call, result.output, member.action);
				return { ...member.issued, output: result.output };
			});
			void Promise.all(results).then((peers) => {
				if (!signal.aborted && peers.every((p): p is CompletedCall => !!p)) turn.peers("drafter", peers);
			});
			if (depth < batch.budgetPolicy.drafterMaxDepth) {
				let scopes = [calls.map((call) => call.id)];
				if (calls.length > 1 && members.some((m) => m.issued)) {
					const context = scopeContext(batch, message, calls);
					const reply = await completeRequest(batch, context, signal, report, CONTINUATION_TOOL).catch(() => undefined);
					const args = privateToolArguments(reply, context);
					if (args) scopes = args.scopes as string[][];
				}
				if (signal.aborted) return members.filter((m) => m.issued).length;
				for (const ids of new Map(scopes.map((ids) => [JSON.stringify([...ids].sort()), ids])).values()) {
					const visible = members.filter((m) => ids.includes(m.call.id));
					if (!visible.length || visible.some((m) => !m.issued)) continue;
					const reconcile = async () => {
						const actual = Promise.all(visible.map((m) => results[members.indexOf(m)]!));
						const forecasts = new Map<string, DrafterObservation>();
						// When every pending tool previously beat this model, give the actual results
						// their measured head start. A slower invocation can still open a forecast.
						const missing = visible.filter((m) => !m.result);
						const modelMs = latencies.get(latencyKey(batch)) ?? 0;
						const durations = missing.map((m) => latencies.get(latencyKey(batch, m.call.name)));
						if (durations.length && durations.every((ms): ms is number => ms !== undefined && ms < modelMs))
							await waitForCompletion(actual, signal, Math.max(...durations));
						if (signal.aborted) return;
						const pending = visible.filter((m) => !m.result);
						const guessed = new AbortController();
						const suffixSignal = AbortSignal.any([signal, guessed.signal]);
						const suffix = async (
							observed: ReadonlyMap<string, DrafterObservation>,
							suffixSignal: AbortSignal,
						) => {
							// A private write is a state dependency even when its model observation was withheld.

							const context = continuationContext(
								batch.context,
								message,
								members.map(
									(m) =>
										(ids.includes(m.call.id) && observed.get(m.call.id)?.message) ||
										privateObservation(m.call, {
											result: {
												content: [
													{ type: "text", text: "Observation withheld from this continuation branch." },
												],
												details: {},
											},
											isError: false,
										}).message,
								),
							);
							await draft(
								turn,
								{ ...batch, context },
								suffixSignal,
								group + "/" + ids.join(",") + "/" + depth,
								depth + 1,
								previousWrite ?? after,
							);
						};
						let forecasted = false;
						const compared = new Set<string>();
						let mismatch = false;
						const compare = (member: DrafterCall) => {
							const predicted = forecasts.get(member.call.id);
							if (!predicted || !member.result || compared.has(member.call.id)) return;
							compared.add(member.call.id);
							observations.compared++;
							if (member.result.key === predicted.key) observations.matched++;
							else {
								mismatch = true;
								guessed.abort();
							}
						};
						if (pending.length) {
							const stopForecast = new AbortController();
							void actual.then(() => stopForecast.abort(new SourceRequestSuppressed({ stage: "source", code: "actual_result_ready" })));
							await turn.request(
								"drafter",
								"observation",
								AbortSignal.any([signal, stopForecast.signal]),
								async (observationSignal, report) => {
									const context = observationContext(
										{ ...batch, message },
										pending,
										visible.flatMap((m) => (m.result ? [m.result.message] : [])),
									);
									const reply = await completeRequest(
										batch,
										context,
										observationSignal,
										report,
										OBSERVATION_TOOL,
									);
									const values = privateToolArguments(reply, context)?.results as
										| (Pick<ToolResultMessage, "content" | "isError"> & {
												callID: string;
												exitCode?: number;
										  })[]
										| undefined;
									if (!values || new Set(values.map((v) => v.callID)).size !== pending.length) return 0;
									for (const m of visible) {
										const value = values.find((v) => v.callID === m.call.id);
										if (value) {
											forecasts.set(
												m.call.id,
												privateObservation(
													m.call,
													{
														result: { content: value.content, details: {} },
														isError: value.isError,
														exitCode: value.exitCode,
													},
													m.action,
												),
											);
											observations.forecasted++;
										} else if (m.result) forecasts.set(m.call.id, m.result);
									}
									if (forecasts.size !== visible.length || signal.aborted) return 0;
									forecasted = true;
									for (const m of pending) {
										compare(m);
										void results[members.indexOf(m)]!.then(() => compare(m));
									}
									if (mismatch) return 0;
									void suffix(forecasts, suffixSignal).catch(() => {});
									return values.length;
								},
							);
						}
						const settled = await actual;
						if (signal.aborted || settled.some((v) => !v)) {
							guessed.abort();
							return;
						}
						if (!forecasted || mismatch)
							await suffix(new Map(visible.map((m) => [m.call.id, m.result!])), signal);
					};
					void reconcile().catch(() => {});
				}
			}
			return members.filter((m) => m.issued).length;
		});
	};
	const enabled = (turn: GeneratorTurn) =>
		turn.settings.enabled &&
		(turn.settings.drafterEnabled ?? DEFAULTS.drafterEnabled) &&
		turn.settings.tools?.length !== 0;
	const source: CallGenerator = {
		async start(turn) {
			if (!enabled(turn)) return;
			const batch = await prepare(turn);
			if (!batch || turn.signal.aborted) return;
			if (batch.skipRoot) {
				await turn.request("drafter", "proposal", turn.signal, async () => {
					throw new SourceRequestSuppressed({ stage: "source", code: "pattern_covers_root" });
				});
				return;
			}
			const count = clampCandidateLimit(turn.settings.candidateLimit ?? DEFAULTS.candidateLimit);
			const controllers = Array.from({ length: count }, () => new AbortController());
			await Promise.all(
				controllers.map((controller, index) =>
					draft(
						turn,
						{
							...batch,
							options: {
								...batch.options,
								temperature: drafterRequestTemperature(index, count, batch.budgetPolicy),
							},
						},
						AbortSignal.any([turn.signal, controller.signal]),
						"drafter:" + turn.startInput.turnID + ":" + index,
						0,
						undefined,
						() => {
							if (
								!controller.signal.aborted &&
								speculativeConfig().prediction.concurrentProposalPolicy === "first_produced"
							)
								for (const peer of controllers) if (peer !== controller) peer.abort();
						},
					),
				),
			);
		},
		async peers(turn, source, peers) {
			if (source === "drafter" || !enabled(turn) || !peers.length) return;
			const batch = await prepare(turn);
			if (!batch || turn.signal.aborted || batch.budgetPolicy.drafterMaxDepth < 1) return;
			const calls = peers.map(({ key }, index) => ({
				type: "toolCall" as const,
				id: "peer:" + index,
				name: key.tool,
				arguments: { ...key.input },
			}));
			const context = continuationContext(
				batch.context,
				{
					...fauxAssistantMessage(calls, { stopReason: "toolUse" }),
					api: batch.model.api,
					provider: batch.model.provider,
					model: batch.model.id,
				},
				peers.map((p, i) => privateObservation(calls[i]!, p.output, p.key).message),
			);
			const writers = peers.filter((p) => p.effect !== "observation");
			if (writers.length <= 1)
				await draft(
					turn,
					{ ...batch, context },
					turn.signal,
					"drafter:peer:" + peers.map((p) => p.run.runID).join(","),
					1,
					writers[0]?.run.runID,
				);
		},
	};
	return {
		source,
		finish: () => budget.finishTask(),
		snapshot: (): DrafterPlanSnapshot => ({ budget: budget.snapshot(), observations: { ...observations } }),
	};
}
/** Simple streams drop toolChoice for most APIs; spell a forced call on the final payload where the API has one. */
export function forceToolChoice(
	inherited: SimpleStreamOptions["onPayload"],
	tool?: string,
): SimpleStreamOptions["onPayload"] {
	return async (payload, model) => {
		const next = (await inherited?.(payload, model)) ?? payload;
		const forced = !tool
			? FORCED_TOOL_CHOICE[model.api]
			: model.api === "anthropic-messages"
				? { type: "tool", name: tool }
				: model.api === "openai-completions"
					? { type: "function", function: { name: tool } }
					: FORCED_TOOL_CHOICE[model.api]
						? { type: "function", name: tool }
						: undefined;
		return forced && next && typeof next === "object" && "tools" in next
			? { ...next, tool_choice: forced }
			: next;
	};
}
const FORCED_TOOL_CHOICE: Readonly<Record<string, unknown>> = {
	"anthropic-messages": { type: "any" },
	"openai-completions": "required",
	"openai-responses": "required",
	"azure-openai-responses": "required",
	"openai-codex-responses": "required",
};
const OBSERVATION_TOOL = "speculative_observations";
const CONTINUATION_TOOL = "speculative_continuations";
function scopeContext(
	batch: DrafterBatch,
	message: AssistantMessage,
	calls: readonly AgentToolCall[],
): Context {
	return {
		...batch.context,
		messages: [
			...batch.context.messages,
			{
				role: "user",
				timestamp: Date.now(),
				content: `Before seeing this batch's results, choose the observation scopes needed to predict subsequent tool calls. Each scope starts an independent continuation that can read only those call results. Use separate scopes for independent work and a joint scope when results must be combined. Do not duplicate work across scopes.\n${JSON.stringify({ draft: message.content })}`,
			},
		],
		tools: [
			{
				name: CONTINUATION_TOOL,
				description: "Select the minimal sets of tool observations needed by subsequent branches.",
				parameters: Type.Object(
					{
						scopes: Type.Array(
							Type.Array(Type.Union(calls.map((call) => Type.Literal(call.id))), {
								minItems: 1,
								uniqueItems: true,
							}),
							{ minItems: 1, uniqueItems: true },
						),
					},
					{ additionalProperties: false },
				),
			},
		],
	};
}
const OBSERVATION_CONTENT = Type.Array(
	Type.Union([
		Type.Object({ type: Type.Literal("text"), text: Type.String() }),
		Type.Object({
			type: Type.Literal("image"),
			data: Type.String(),
			mimeType: Type.String(),
		}),
	]),
);
function observationContext(
	batch: DrafterPlanFeedback,
	missing: readonly DrafterCall[],
	observations: readonly ToolResultMessage[],
): Context {
	const candidates = missing.map(({ call, action }) => {
		const { content = OBSERVATION_CONTENT } = toolObservationContract(action);
		return {
			call,
			tool: batch.context.tools?.find((tool) => tool.name === call.name),
			observation: { content },
		};
	});
	return {
		...batch.context,
		messages: [
			...batch.context.messages,
			{
				role: "user",
				timestamp: Date.now(),
				content: `Predict the pending observations under their bound schemas.\n${JSON.stringify({
					draft: batch.message.content,
					observations,
					candidates,
				})}`,
			},
		],
		tools: [
			{
				name: OBSERVATION_TOOL,
				description: "Private predicted tool observations; never executable results.",
				parameters: Type.Object(
					{
						results: Type.Array(
							Type.Union(
								candidates.map(({ call, observation }) =>
									Type.Object(
										{
											callID: Type.Literal(call.id),
											isError: Type.Boolean(),
											exitCode: Type.Optional(Type.Integer()),
											content: observation.content,
										},
										{ additionalProperties: false },
									),
								),
							),
							{ minItems: missing.length, maxItems: missing.length },
						),
					},
					{ additionalProperties: false },
				),
			},
		],
	};
}
function privateObservation(
	call: AgentToolCall,
	output: ToolSettlement,
	action?: ActionKey,
): DrafterObservation {
	const { key, content } = canonicalToolObservation(output, action);
	return {
		key,
		message: {
			role: "toolResult",
			toolCallId: call.id,
			toolName: call.name,
			content: immutableSnapshot(content),
			isError: output.isError,
			timestamp: Date.now(),
		},
	};
}
function privateToolArguments(
	message: AssistantMessage | undefined,
	context: Context,
): Record<string, unknown> | undefined {
	const calls = message?.content.filter((item): item is AgentToolCall => item.type === "toolCall");
	const tool = context.tools![0]!;
	if (calls?.length !== 1 || calls[0]!.name !== tool.name) return undefined;
	const args = validateToolArguments(tool, calls[0]!);
	return stableValueHash(args) === stableValueHash(calls[0]!.arguments) ? args : undefined;
}
function continuationContext(
	context: Context,
	message: AssistantMessage,
	results: ToolResultMessage[],
): Context {
	return { ...context, messages: [...context.messages, message, ...results] };
}
