import type { AgentTool, AgentToolCall, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { validateToolArguments } from "@earendil-works/pi-ai";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionKey, type ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection,
	type SpeculativeToolSelectionInput,
} from "./common.ts";
import type {
	AgentConsumeInput,
	AgentStartInput,
	AgentStateData,
	DraftModelSelection,
	DraftOptionsContext,
} from "./agent-runtime-types.ts";
import { definitionSchemaHashes } from "./agent-runtime-types.ts";
import type { ActorForkPlanSource } from "./actor-fork-plan-source.ts";
import type {
	ExecutionWorldDiagnosticSnapshot,
	SpeculativeExecutionRoute,
} from "./execution-world.ts";
import type { DrafterUtilityGateSnapshot } from "./drafter-utility-gate.ts";
import { createDrafterPlanSource } from "./drafter-plan-source.ts";
import {
	PATTERN_AWARE_DEFAULTS,
	type PatternAwareSettings,
	type PatternAwareStore,
	patternAwareSettings,
} from "./pattern-aware.ts";
import { createPatternPlanSource } from "./pattern-plan-source.ts";
import type {
	CandidatePreflight,
	ActorActionFeedback,
	MaterializedActorAction,
	MaterializedSpeculativeCandidate,
	PredictionFeedback,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionSettings,
} from "./runtime.ts";
import { normalizeSelfSpeculationSettings, type SelfSpeculationSettingsInput } from "./self-speculation.ts";
import { makeSpeculativeActionRuntime } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import { toolErrorSettlement, type ToolInvocation, type ToolSettlement } from "./tool-settlement.ts";
import { ToolExecutionGateway, type ToolOperation } from "./tool-execution-gateway.ts";

export interface SpeculativeAgentSettingsInput {
	readonly enabled?: boolean;
	readonly drafterEnabled?: boolean;
	/** Adaptively skip a root Drafter batch when its measured action-side utility is negative. */
	readonly drafterGateEnabled?: boolean;
	/** Output-informed successor actions retained after the first Drafter action. */
	readonly drafterMaxDepth?: number;
	/** Optional hard output cap for each one-action Drafter request; omitted uses the provider default. */
	readonly drafterMaxTokens?: number;
	/** Number of leading Drafter requests sent at temperature zero. */
	readonly drafterDeterministicCandidates?: number;
	/** Inclusive temperature range stratified across the remaining requests. */
	readonly drafterTemperatureMin?: number;
	readonly drafterTemperatureMax?: number;
	readonly candidateLimit?: number;
	readonly maxConcurrentActions?: number;
	readonly resourceCacheMaxEntries?: number;
	readonly resourceCacheMaxBytes?: number;
	readonly predictionTimeoutMs?: number;
	readonly patternAware?: Partial<PatternAwareSettings>;
	readonly selfSpeculation?: SelfSpeculationSettingsInput;
	/** Legacy grouped input is accepted only for configuration migration. */
	readonly tools?: SpeculativeToolSelectionInput;
}

export interface SpeculativeAgentPreflightContext {
	readonly tool: AgentTool;
	readonly toolName: string;
	readonly args: unknown;
	readonly action: ActionKey;
	readonly route: SpeculativeExecutionRoute;
	readonly signal: AbortSignal;
}

export type { DraftOptionsContext } from "./agent-runtime-types.ts";

export interface CreateSpeculativeActionHostOptions {
	/** Workspace root used for action canonicalization and resource validation. */
	readonly cwd: string;
	/** Runtime settings. The feature remains disabled when omitted. */
	readonly getSettings?: () => SpeculativeAgentSettingsInput | Promise<SpeculativeAgentSettingsInput>;
	/** Drafter model. Defaults to the actor model when omitted or unresolved. */
	readonly draftModel?: DraftModelSelection;
	/** Resolve drafter request options, including credentials when using a different provider. */
	readonly getDraftOptions?: (context: DraftOptionsContext) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	/** Provider completion used by the drafter. */
	readonly complete: (model: Model<Api>, context: Context, options?: SimpleStreamOptions) => Promise<AssistantMessage>;
	/** Resolve the concrete executor identity used by both speculative and actor calls. */
	readonly resolveInvocation?: (
		tool: string,
		input: unknown,
	) => ToolInvocation | undefined | Promise<ToolInvocation | undefined>;
	/**
	 * Non-interactive permission and policy check for speculative execution.
	 * Candidates are rejected when this callback is absent.
	 */
	readonly preflight?: (
		context: SpeculativeAgentPreflightContext,
	) => boolean | CandidatePreflight | Promise<boolean | CandidatePreflight>;
	/** Canonical K(a), projection, and resource-version semantics for this host. */
	readonly actionSemantics?: ActionSemanticsRegistry;
	/** Lossless Π rules; each rule owns key relation, realized coverage, and output reconstruction. */
	readonly projectionRules?: readonly ActionProjectionRule<ToolSettlement>[];
	/** Actor probe source shared with the decoder-feedback coordinator. */
	readonly actorForkPlanSource?: ActorForkPlanSource;
	/** Ordered execution capabilities. A runtime-wide sandbox takes precedence over local fallbacks. */
	readonly executionWorlds?: readonly SpeculativeAgentExecutionWorld[];
	/** Optional persistence root for workspace-hashed PatternAware state. */
	readonly patternStateDirectory?: string;
	/** Stable logical workspace identity when physical checkout paths are ephemeral. */
	readonly patternWorkspaceIdentity?: string;
	/** Optional injected store, primarily for embedding and deterministic tests. */
	readonly patternStore?: PatternAwareStore | Promise<PatternAwareStore>;
	/** Starts request-scoped inference integration before prediction sources launch. */
	readonly onTurnStarted?: (input: {
		readonly turnID: string;
		readonly actorModel: Model<Api>;
		readonly context: Context;
		readonly decisionSequence: number;
	}) => void | Promise<void>;
	/** Receives every validated K(a) as a concrete tool call, independent of execution isolation. */
	readonly onCandidateMaterialized?: (candidate: MaterializedSpeculativeCandidate<string>) => void | Promise<void>;
	/** Receives the exact Runtime-owned K(a) of each authoritative Actor call. */
	readonly onActorActionMaterialized?: (action: MaterializedActorAction<string>) => void | Promise<void>;
	/** Receives authoritative adoption and realized execution-ahead feedback. */
	readonly onActorActionSettled?: (feedback: ActorActionFeedback<string>) => void | Promise<void>;
	/** Receives observed prediction matches and adoption independently of decoder verification. */
	readonly onPredictionSettled?: (feedback: PredictionFeedback<string>) => void | Promise<void>;
	readonly onEvent?: (event: SpeculativeActionEvent<string>) => void | Promise<void>;
}

export interface SpeculativeActionHost {
	readonly sessionID: string;
	readonly runtime: SpeculativeActionRuntime<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentConsumeInput
	>;
	readonly executionWorldDiagnostics: (
		refresh?: boolean,
	) => Promise<readonly ExecutionWorldDiagnosticSnapshot[]>;
	readonly startTurn: (input: Omit<AgentStartInput, "sessionID">, signal?: AbortSignal) => Promise<void>;
	readonly previewActorTool: (
		input: { readonly turnID: string; readonly tool: string },
		signal?: AbortSignal,
	) => Promise<void>;
	readonly previewActorCall: (input: Omit<AgentConsumeInput, "sessionID">, signal?: AbortSignal) => Promise<void>;
	readonly consume: (
		input: Omit<AgentConsumeInput, "sessionID">,
		signal?: AbortSignal,
	) => Promise<ToolSettlement | undefined>;
	/** One tool outlet: reuse lookup, Actor fallback, timing, and settlement reporting. */
	readonly execute: (
		input: SpeculativeToolExecutionInput,
		signal: AbortSignal | undefined,
		executor: (operation: ToolOperation) => Promise<AgentToolResult<unknown>>,
	) => Promise<AgentToolResult<unknown>>;
	readonly actual: (
		input: Omit<AgentConsumeInput, "sessionID"> & { readonly durationMs: number; readonly output?: ToolSettlement },
	) => Promise<void>;
	readonly finishTurn: (turnID: string, terminal?: boolean) => Promise<void>;
	readonly drafterGateSnapshot: () => ActionDrafterGateSnapshot;
	readonly dispose: () => Promise<void>;
}

export interface SpeculativeToolExecutionInput {
	readonly turnID?: string;
	readonly id?: string;
	readonly tool: string;
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
}

export type ActionDrafterGateSnapshot = DrafterUtilityGateSnapshot;

export { patternPlanActionID } from "./pattern-plan-source.ts";

/**
 * Build source-neutral speculative plan execution for a host. The host owns lifecycle and tool interception.
 */
export function createSpeculativeActionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
): SpeculativeActionHost {
	const actionSemantics = options.actionSemantics ?? PI_ACTION_SEMANTICS;
	const projectionRules = (options.projectionRules ?? []).filter((rule) => actionSemantics.supportsProjector(rule.id));
	const executionWorlds = [...new Set(options.executionWorlds ?? [])];
	if (
		!executionWorlds.some(
			(world) =>
				world.id === "resource_version" && world.scope === "fallback" && world.isolation === "resource_snapshot",
		)
	) {
		executionWorlds.push(createResourceSnapshotExecutionWorld(actionSemantics));
	}
	const executionGateway = new ToolExecutionGateway(executionWorlds);
	const resolveExecutionRoute = (tool: string, signal?: AbortSignal, action?: ActionKey) => {
		const definition = actionSemantics.definition(tool);
		return definition
			? executionGateway.resolve(
					{
						operation: {
							tool,
							input: undefined,
							...(signal ? { signal } : {}),
							...(action ? { action } : {}),
						},
						effect: definition.effect,
						requirements: definition.requirements,
					},
					{ cwd: options.cwd, ...(signal ? { signal } : {}) },
				)
			: undefined;
	};
	const resolveSettings = async (): Promise<SpeculativeActionSettings> => {
		const settings = (await options.getSettings?.()) ?? {};
		const drafter = normalizeDrafterRequestSettings(settings);
		const selfSpeculation = normalizeSelfSpeculationSettings(settings.selfSpeculation);
		return {
			enabled: typeof settings.enabled === "boolean" ? settings.enabled : DEFAULTS.enabled,
			drafterEnabled:
				typeof settings.drafterEnabled === "boolean" ? settings.drafterEnabled : DEFAULTS.drafterEnabled,
			candidateLimit: clampCandidateLimit(settings.candidateLimit ?? DEFAULTS.candidateLimit),
			maxConcurrentActions: clampCandidateLimit(settings.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions),
			resourceCacheMaxEntries: normalizePositiveInteger(
				settings.resourceCacheMaxEntries,
				DEFAULTS.resourceCacheMaxEntries,
			),
			resourceCacheMaxBytes: normalizePositiveInteger(
				settings.resourceCacheMaxBytes,
				DEFAULTS.resourceCacheMaxBytes,
			),
			predictionTimeoutMs: normalizeTimeout(settings.predictionTimeoutMs),
			sourceConfig: {
				...drafter,
				drafterGateEnabled:
					typeof settings.drafterGateEnabled === "boolean"
						? settings.drafterGateEnabled
						: DEFAULTS.drafterGateEnabled,
				patternAware: patternAwareSettings(settings.patternAware ?? PATTERN_AWARE_DEFAULTS),
				actorForkActionEnabled:
					options.actorForkPlanSource !== undefined &&
					selfSpeculation.enabled &&
					selfSpeculation.forkEnabled &&
					selfSpeculation.forkActionEnabled &&
					selfSpeculation.forkTransport === "sidecar",
			},
			tools: normalizeSpeculativeToolSelection(settings.tools, actionSemantics.toolNames()),
		};
	};
	const prepareExecutionWorlds = async (tools: readonly string[], signal?: AbortSignal): Promise<void> => {
		await Promise.all([...new Set(tools)].map((tool) => resolveExecutionRoute(tool, signal)));
	};
	const drafterPlans = createDrafterPlanSource({
		sessionID,
		draftModel: options.draftModel,
		getDraftOptions: options.getDraftOptions,
		complete: options.complete,
		validateArguments: validateCandidateArguments,
	});
	const patternPlans = createPatternPlanSource({
		sessionID,
		cwd: options.cwd,
		actionSemantics,
		projectionRules,
		stateDirectory: options.patternStateDirectory,
		workspaceIdentity: options.patternWorkspaceIdentity,
		store: options.patternStore,
	});
	const runtime = makeSpeculativeActionRuntime<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentConsumeInput,
		AgentStateData
	>({
		actionSemantics,
		sources: [
			patternPlans.source,
			drafterPlans.source,
			...(options.actorForkPlanSource ? [options.actorForkPlanSource.source] : []),
		],
		settings: resolveSettings,
		definitions: (input) =>
			input.tools.map((tool) => ({ name: tool.name, description: tool.description, inputSchema: tool.parameters })),
		stateData: (input) => ({
			tools: new Map(input.tools.map((tool) => [tool.name, tool])),
			schemaHashes: definitionSchemaHashes(
				input.tools.map((tool) => ({ name: tool.name, inputSchema: tool.parameters })),
			),
		}),
		actionKey: async (toolName, input, context) => {
			let tool: AgentTool | undefined;
			let validated: unknown;
			if (context.type === "consume") {
				tool = context.consumeInput.tools.find((candidate) => candidate.name === toolName);
				validated = input;
			} else {
				tool = context.data.tools.get(toolName);
				if (!tool) return undefined;
				validated = validateCandidateArguments(tool, toolName, input, "spec_key");
				if (validated === undefined) return undefined;
			}
			if (!tool) return undefined;
			let invocation: ToolInvocation | undefined;
			try {
				invocation = await options.resolveInvocation?.(toolName, validated);
			} catch {
				return undefined;
			}
			const schemaHash =
				context.type === "consume" ? stableValueHash(tool.parameters ?? null) : context.data.schemaHashes[toolName];
			return actionSemantics.buildKey(
				toolName,
				validated,
				options.cwd,
				schemaHash,
				invocation
					? {
							fingerprint: stableValueHash(invocation.identity ?? invocation),
							context: invocation,
						}
					: undefined,
			);
		},
		resolveExecution: ({ tool, action, signal }) => resolveExecutionRoute(tool, signal, action),
		captureAuthoritativeResult: async ({ startInput, data, tool: toolName, concrete, action, callID, signal }) => {
			const tool = data.tools.get(toolName);
			if (!tool) return undefined;
			const definition = actionSemantics.definition(toolName);
			if (!definition) return undefined;
			const args = validateCandidateArguments(tool, toolName, concrete, callID);
			if (args === undefined) return undefined;
			const operation: ToolOperation = { tool: toolName, callID, input: args, signal, action };
			const captured = await executionGateway.captureAuthoritativeResult(
				{ operation, effect: definition.effect, requirements: definition.requirements },
				{ cwd: options.cwd, signal },
				(operation) => ({
					cwd: options.cwd,
					tool,
					toolName: operation.tool,
					args: operation.input,
					action: operation.action ?? action,
					callID: operation.callID ?? callID,
					signal: operation.signal ?? signal,
					executionScope: { sessionID: startInput.sessionID, turnID: startInput.turnID },
				}),
			);
			if (!captured) return undefined;
			return {
				route: captured.route,
				seal: (output) => captured.capture.seal(output),
				dispose: () => captured.capture.dispose(),
			};
		},
		actual: (input) => ({ id: input.id, tool: input.tool, input: input.args }),
		preflightCandidate: async ({ data, tool: toolName, concrete, action, route, callID, signal }) => {
			const tool = data.tools.get(toolName);
			if (!tool || !options.preflight) return { ok: false, reason: "permission_or_policy" };
			const args = validateCandidateArguments(tool, toolName, concrete, callID);
			if (args === undefined) return { ok: false, reason: "invalid_tool_call_input" };
			const result = await options.preflight({ tool, toolName, args, action, route, signal });
			return typeof result === "boolean"
				? result
					? { ok: true }
					: { ok: false, reason: "permission_or_policy" }
				: result;
		},
		authorizeCandidate: async ({ stateData, tool: toolName, concrete, action, route, signal }) => {
			const tool = stateData.tools.get(toolName);
			if (!tool || !options.preflight) return { ok: false, reason: "permission_or_policy_changed" };
			const args = validateCandidateArguments(tool, toolName, concrete, "spec_authorize");
			if (args === undefined) return { ok: false, reason: "invalid_tool_call_input" };
			const result = await options.preflight({
				tool,
				toolName,
				args,
				action,
				route,
				signal: signal ?? new AbortController().signal,
			});
			if (typeof result === "boolean") {
				return result ? { ok: true } : { ok: false, reason: "permission_or_policy_changed" };
			}
			return result.ok ? result : { ...result, reason: "permission_or_policy_changed" };
		},
		executeCandidate: async ({ startInput, data, tool: toolName, concrete, action, route, callID, signal, parentWorld }) => {
			const tool = data.tools.get(toolName);
			if (!tool) throw new Error(`Tool ${toolName} not found`);
			const args = validateCandidateArguments(tool, toolName, concrete, callID);
			if (args === undefined) throw new Error(`Invalid arguments for tool ${toolName}`);
			return executionGateway.executeSpeculative(
				{ tool: toolName, callID, input: args, signal, action },
				route,
				(operation) => ({
					cwd: options.cwd,
					tool,
					toolName: operation.tool,
					args: operation.input,
					action: operation.action ?? action,
					callID: operation.callID ?? callID,
					signal: operation.signal ?? signal,
					executionScope: { sessionID: startInput.sessionID, turnID: startInput.turnID },
					...(parentWorld?.checkpoint ? { parentCheckpoint: parentWorld.checkpoint } : {}),
				}),
			);
		},
		rejectCandidateOutput: ({ output }) => (output.isError ? "tool_error_result" : undefined),
		projectionRules,
		onTurnStarted: async ({ startInput, decisionSequence, settings, signal }) => {
			try {
				await options.onTurnStarted?.({
					turnID: startInput.turnID,
					actorModel: startInput.actorModel,
					context: startInput.context,
					decisionSequence,
				});
			} catch {
				// Optional inference integration cannot prevent source launch or Actor execution.
			}
			void prepareExecutionWorlds(settings.tools, signal).catch(() => {
				// Turn warm-up is best-effort; route resolution remains authoritative.
			});
			patternPlans.turnStarted(startInput, settings);
		},
		onTurnFinished: ({ startInput, settings, terminal }) => {
			drafterPlans.finishTurn(startInput.sessionID, startInput.turnID);
			patternPlans.turnFinished(startInput, settings, terminal);
		},
		onCandidateMaterialized: options.onCandidateMaterialized,
		onActorActionMaterialized: options.onActorActionMaterialized,
		onActorActionSettled: async (feedback) => {
			await drafterPlans.actorActionSettled(feedback);
			await options.onActorActionSettled?.(feedback);
		},
		onPredictionSettled: options.onPredictionSettled,
		onEvent: options.onEvent,
	});

	return {
		sessionID,
		runtime,
		executionWorldDiagnostics: (refresh = false) =>
			executionGateway.diagnostics({ cwd: options.cwd, ...(refresh ? { refresh: true } : {}) }),
		startTurn: (input, signal) => runtime.startTurn({ ...input, sessionID }, signal),
		previewActorTool: (input, signal) => runtime.previewActorTool({ ...input, sessionID }, signal),
		previewActorCall: (input, signal) => runtime.previewActorCall({ ...input, sessionID }, signal),
		consume: (input, signal) => runtime.consume({ ...input, sessionID }, signal),
		execute: (input, signal, executor) => {
			const operation: ToolOperation = {
				tool: input.tool,
				input: input.args,
				...(input.id ? { callID: input.id } : {}),
				...(signal ? { signal } : {}),
			};
			const actorCall = input.turnID
				? {
						turnID: input.turnID,
						id: input.id,
						tool: input.tool,
						args: input.args,
						tools: input.tools,
					}
				: undefined;
			return executionGateway.executeAuthoritative(operation, executor, {
				...(actorCall
					? {
							reuse: async () => (await runtime.consume({ ...actorCall, sessionID }, signal))?.result,
							settled: async (settlement) => {
								await runtime.actual({
									...actorCall,
									sessionID,
									durationMs: settlement.durationMs,
									output:
										settlement.status === "succeeded"
											? { result: settlement.output, isError: false }
											: toolErrorSettlement(settlement.error),
								});
							},
						}
					: {}),
			});
		},
		actual: (input) => runtime.actual({ ...input, sessionID }),
		drafterGateSnapshot: drafterPlans.snapshot,
		finishTurn: async (turnID, terminal = false) => {
			await runtime.finishTurn({ sessionID, turnID, tool: "", args: {}, tools: [], terminal });
			if (terminal) {
				drafterPlans.finishSession();
				await patternPlans.finishSession();
			}
		},
		dispose: async () => {
			try {
				await runtime.releaseSession(sessionID);
				drafterPlans.finishSession();
				await patternPlans.finishSession();
			} finally {
				try {
					await patternPlans.dispose();
				} finally {
					await executionGateway.dispose();
				}
			}
		},
	};
}

function validateCandidateArguments(
	tool: AgentTool,
	toolName: string,
	input: unknown,
	callID: string,
): unknown | undefined {
	try {
		const prepared = tool.prepareArguments ? tool.prepareArguments(input) : input;
		const toolCall: AgentToolCall = {
			type: "toolCall",
			id: callID,
			name: toolName,
			arguments: prepared as Record<string, unknown>,
		};
		return validateToolArguments(tool, toolCall);
	} catch {
		return undefined;
	}
}

function normalizeTimeout(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0
		? Math.floor(value)
		: DEFAULTS.predictionTimeoutMs;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}
