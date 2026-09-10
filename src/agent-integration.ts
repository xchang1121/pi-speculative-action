import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { validateToolArguments } from "@earendil-works/pi-ai";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionKey, type ActionSemanticsRegistry, ownActionKeyProjector, PI_ACTION_SEMANTICS, RESOURCE_INPUT_ACTION_KEY_PROJECTOR } from "./action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type AgentExecutionWorld } from "./agent-execution-world.ts";
import {
	clampCandidateLimit,
	DEFAULTS,
	normalizeDrafterRequestSettings,
	normalizeSpeculativeToolSelection,
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
import { nonNegativeInteger, positiveInteger } from "./setting-input.ts";
import { immutableSnapshot, isImmutableSnapshot } from "./stable-json.ts";
import { toolErrorSettlement, type ToolInvocation, type ToolSettlement } from "./tool-settlement.ts";
import { ToolExecutionGateway, type ToolOperation } from "./tool-execution-gateway.ts";

const ACTOR_OPERATION = Symbol("actor-operation");
const RAW_ACTOR_CALL = Symbol("raw-actor-call");
type BoundActorCall = AgentConsumeInput & { readonly [ACTOR_OPERATION]?: () => Promise<ToolOperation>; readonly [RAW_ACTOR_CALL]?: true };

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
	/** Prediction selection, independent of execution permissions; omitted uses the registered tools. */
	readonly tools?: readonly string[];
}

/** Shared host/package boundary: the TUI and runtime must interpret saved policy identically. */
export function normalizeSpeculativeAgentSettings(input: SpeculativeAgentSettingsInput = {}, allowed = DEFAULTS.tools) {
	return {
		...normalizeDrafterRequestSettings(input),
		enabled: typeof input.enabled === "boolean" ? input.enabled : DEFAULTS.enabled,
		drafterEnabled: typeof input.drafterEnabled === "boolean" ? input.drafterEnabled : DEFAULTS.drafterEnabled,
		drafterGateEnabled: typeof input.drafterGateEnabled === "boolean" ? input.drafterGateEnabled : DEFAULTS.drafterGateEnabled,
		candidateLimit: clampCandidateLimit(input.candidateLimit ?? DEFAULTS.candidateLimit),
		maxConcurrentActions: clampCandidateLimit(input.maxConcurrentActions ?? DEFAULTS.maxConcurrentActions),
		resourceCacheMaxEntries: positiveInteger(input.resourceCacheMaxEntries, DEFAULTS.resourceCacheMaxEntries),
		resourceCacheMaxBytes: positiveInteger(input.resourceCacheMaxBytes, DEFAULTS.resourceCacheMaxBytes),
		predictionTimeoutMs: nonNegativeInteger(input.predictionTimeoutMs, DEFAULTS.predictionTimeoutMs),
		patternAware: patternAwareSettings(input.patternAware ?? PATTERN_AWARE_DEFAULTS),
		selfSpeculation: normalizeSelfSpeculationSettings(input.selfSpeculation),
		tools: normalizeSpeculativeToolSelection(input.tools, allowed),
	} as const;
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
	/** Bind the concrete executor used by both speculative and Actor calls; rejection fails this invocation. */
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
	readonly executionWorlds?: readonly AgentExecutionWorld[];
	/** Dynamic policy for pre-Actor execution only; observation and Actor result reuse stay independent. */
	readonly speculativeExecutionWorldEnabled?: (backend: string) => boolean;
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
	/** Raw streamed arguments; preparation is provisional and never binds the final Actor call. */
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
	/** Final host-prepared arguments; this outlet must not prepare them a second time. */
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
	const projectionRules = [RESOURCE_INPUT_ACTION_KEY_PROJECTOR, ...(options.projectionRules ?? [])]
		.filter((rule) => actionSemantics.supportsProjector(rule.id)).map(ownActionKeyProjector);
	const executionWorlds = [...new Set(options.executionWorlds ?? [])];
	if (
		!executionWorlds.some(
			(world) =>
				world.id === "resource_version" && world.scope === "fallback" && world.isolation === "resource_snapshot",
		)
	) {
		executionWorlds.push(createResourceSnapshotExecutionWorld(actionSemantics));
	}
	const executionGateway = new ToolExecutionGateway(executionWorlds, options.speculativeExecutionWorldEnabled);
	const resolveExecutionRoute = (tool: string, signal?: AbortSignal, action?: ActionKey) => {
		const definition = actionSemantics.definition(action ?? tool);
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
		const { patternAware, selfSpeculation, drafterGateEnabled, drafterMaxDepth, drafterMaxTokens,
			drafterDeterministicCandidates, drafterTemperatureMin, drafterTemperatureMax, ...policy } =
			normalizeSpeculativeAgentSettings(await options.getSettings?.(), actionSemantics.toolNames());
		return {
			...policy,
			sourceConfig: {
				drafterMaxDepth, drafterMaxTokens, drafterDeterministicCandidates, drafterTemperatureMin, drafterTemperatureMax,
				drafterGateEnabled, patternAware,
				actorForkActionEnabled:
					options.actorForkPlanSource !== undefined &&
					selfSpeculation.enabled &&
					selfSpeculation.forkEnabled &&
					selfSpeculation.forkActionEnabled &&
					selfSpeculation.forkTransport === "sidecar",
			},
		};
	};
	const drafterPlans = createDrafterPlanSource({
		sessionID,
		draftModel: options.draftModel,
		getDraftOptions: options.getDraftOptions,
		complete: options.complete,
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
	const sources = [patternPlans.source, drafterPlans.source,
		...(options.actorForkPlanSource ? [options.actorForkPlanSource.source] : [])];
	const resolveBinding = async (tool: string, input: unknown, schemaHash?: string) => {
		const resolved = await options.resolveInvocation?.(tool, input);
		const invocation = resolved && Object.freeze({ ...resolved,
			...(resolved.identity !== undefined ? { identity: immutableSnapshot(resolved.identity) } : {}),
			...(resolved.process ? { process: immutableSnapshot(resolved.process) } : {}),
		});
		const action = schemaHash === undefined || ![input, invocation?.identity, invocation?.process].every(isImmutableSnapshot)
			? undefined : actionSemantics.buildKey(tool, input, options.cwd, schemaHash, invocation
			? { fingerprint: stableValueHash(invocation.identity ?? invocation), context: invocation, semantics: invocation.semantics } : undefined);
		return { ...(invocation ? { invocation } : {}), ...(action ? { action } : {}) };
	};
	const checkPermission = async (tool: AgentTool | undefined, context: Omit<SpeculativeAgentPreflightContext, "tool">, recheck = false): Promise<CandidatePreflight> => {
		const reason = recheck ? "permission_or_policy_changed" : "permission_or_policy";
		const result = tool && options.preflight ? await options.preflight({ ...context, tool }) : false;
		if (typeof result === "boolean") return result ? { ok: true } : { ok: false, reason };
		return recheck && !result.ok ? { ...result, reason } : result;
	};
	const runtime = makeSpeculativeActionRuntime<
		string,
		ToolSettlement,
		AgentStartInput,
		AgentConsumeInput,
		AgentConsumeInput,
		AgentStateData
	>({
		actionSemantics,
		sources,
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
				const call = context.consumeInput as BoundActorCall, bind = call[ACTOR_OPERATION];
				if (bind) return (await bind()).action;
				tool = call.tools.find((candidate) => candidate.name === toolName);
				validated = call[RAW_ACTOR_CALL] ? tool && prepareToolArguments(tool, input) : immutableSnapshot(input);
			} else {
				tool = context.data.tools.get(toolName);
				validated = tool && prepareToolArguments(tool, input);
			}
			if (!tool || validated === undefined) return undefined;
			const schemaHash =
				context.type === "consume" ? stableValueHash(tool.parameters ?? null) : context.data.schemaHashes[toolName];
			return (await resolveBinding(toolName, validated, schemaHash)).action;
		},
		resolveExecution: ({ tool, action, signal }) => resolveExecutionRoute(tool, signal, action),
		captureAuthoritativeResult: async ({ startInput, data, tool: toolName, concrete, action, callID, signal }) => {
			const tool = data.tools.get(toolName);
			if (!tool) return undefined;
			const definition = actionSemantics.definition(action);
			if (!definition) return undefined;
			const operation: ToolOperation = { tool: toolName, callID, input: concrete, signal, action };
			const captured = await executionGateway.captureAuthoritativeResult(
				{ operation, effect: definition.effect, requirements: definition.requirements },
				{ cwd: options.cwd, signal },
				{
					cwd: options.cwd, tool, toolName, args: concrete, action, callID, signal,
					executionScope: { sessionID: startInput.sessionID, turnID: startInput.turnID },
				},
			);
			return captured && { route: captured.route, ...captured.capture };
		},
		actual: (input) => ({ id: input.id, tool: input.tool, input: input.args }),
		preflightCandidate: ({ data, tool: toolName, concrete, action, route, signal }) =>
			checkPermission(data.tools.get(toolName), { toolName, args: concrete, action, route, signal }),
		authorizeCandidate: ({ stateData, tool: toolName, concrete, action, route, signal }) =>
			checkPermission(stateData.tools.get(toolName), { toolName, args: concrete, action, route, signal: signal ?? new AbortController().signal }, true),
		executeCandidate: async ({ startInput, data, tool: toolName, concrete, action, route, callID, signal, parentWorld }) => {
			const tool = data.tools.get(toolName);
			if (!tool) throw new Error(`Tool ${toolName} not found`);
			const args = structuredClone(concrete);
			return executionGateway.executeSpeculative(
				{ tool: toolName, callID, input: args, signal, action },
				route,
				{
					cwd: options.cwd, tool, toolName, args, action, callID, signal,
					executionScope: { sessionID: startInput.sessionID, turnID: startInput.turnID },
					...(parentWorld?.checkpoint ? { parentCheckpoint: parentWorld.checkpoint } : {}),
				},
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
			if (sources.some((source) => source.enabled(settings))) {
				void Promise.all(settings.tools.filter((name) => startInput.tools.some((tool) => tool.name === name))
					.map((tool) => resolveExecutionRoute(tool, signal))).catch(() => {});
			}
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
		previewActorCall: (input, signal) => runtime.previewActorCall({ ...input, sessionID, [RAW_ACTOR_CALL]: true } as BoundActorCall, signal),
		consume: (input, signal) => runtime.consume({ ...input, sessionID }, signal),
		execute: (input, signal, executor) => {
			const operation: ToolOperation = {
				tool: input.tool,
				input: immutableSnapshot(input.args),
				...(input.id ? { callID: input.id } : {}),
				...(signal ? { signal } : {}),
			};
			let binding: Promise<ToolOperation> | undefined;
			// One invocation owns its binding; resolve inside consume so Actor arrival includes binding cost.
			const bind = () => binding ??= (async () => {
				const tool = input.tools.find((tool) => tool.name === input.tool);
				return Object.freeze({ ...operation, ...await resolveBinding(operation.tool, operation.input,
					tool ? stableValueHash(tool.parameters ?? null) : undefined) });
			})();
			const actorCall = input.turnID
				? {
						[ACTOR_OPERATION]: bind,
						turnID: input.turnID,
						id: input.id,
						tool: input.tool,
						args: operation.input,
						tools: input.tools,
					}
				: undefined;
			return executionGateway.executeAuthoritative(operation, () => bind().then(executor), {
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

/** Raw predictions and previews cross Pi's preparation boundary once, before identity is sealed. */
function prepareToolArguments(tool: AgentTool, input: unknown): unknown | undefined {
	try {
		const owned = structuredClone(input);
		const prepared = tool.prepareArguments ? tool.prepareArguments(owned) : owned;
		return immutableSnapshot(validateToolArguments(tool, {
			type: "toolCall",
			id: "spec_key",
			name: tool.name,
			arguments: prepared as Record<string, unknown>,
		}));
	} catch {
		return undefined;
	}
}
