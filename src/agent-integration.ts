import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { validateToolArguments } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import {
	resolveSpeculativeConfig,
	speculativeConfig,
	withSpeculativeConfig,
	type SpeculativeConfigInput,
} from "./config-schema.ts";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { ActionKey, ActionProjectionRule, ActionSemanticsRegistry } from "./action-semantics.ts";
import { buildActionKey, PI_ACTION_SEMANTICS, resolveActionProjectionRules } from "./action-semantics.ts";
import type { AgentExecutionWorld, SpeculativeToolExecutionContext } from "./agent-execution-world.ts";
import { createResourceSnapshotExecutionWorld } from "./agent-execution-world.ts";
import type { DrafterRequestSettings } from "./config-schema.ts";
import {
	booleanOr,
	normalizeDrafterRequestSettings,
	normalizeSchedulingSettings,
	positiveInteger,
	positiveMilliseconds,
} from "./config-schema.ts";
import type { DrafterPlanSnapshot, DrafterTaskBudget } from "./drafter-plan-source.ts";
import { createDrafterPlanSource } from "./drafter-plan-source.ts";
import type {
	ExecutionOperationBinding,
	ExecutionWorldDiagnosticSnapshot,
	SpeculativeExecutionRoute,
	ToolInvocation,
	ToolSettlement,
	WorldCheckpoint,
} from "./execution-world.ts";
import {
	isRegisteredExecutionWorld,
	ToolExecutionGateway,
	toolObservationKey,
	UNRESTRICTED_PROCESS_EFFECTS,
} from "./execution-world.ts";
import type { PatternAwareSettings, PatternAwareStore } from "./pattern-aware.ts";
import { patternAwareSettings } from "./pattern-aware.ts";
import { createPatternPlanSource } from "./pattern-plan-source.ts";
import {
	PI_BASH_TIMEOUT_PROJECTION_RULE,
	PI_GREP_LITERAL_PROJECTION_RULE,
	piToolErrorSettlement,
} from "./pi-tool-invocation.ts";
import { asRecord, immutableSnapshot, isImmutableSnapshot, stableValueHash } from "./primitives.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import type {
	ActualToolCall,
	CandidatePreflight,
	PreparedActorCall,
	RuntimeEventMap,
	SpeculativeActionEvent,
	SpeculativeActionRuntime,
	SpeculativeActionSettings,
	SpeculativePlanSource,
	TurnInput,
} from "./runtime-engine.ts";
import { createExecutionPreparation, makeSpeculativeActionRuntime } from "./runtime-engine.ts";
import type { ActorForkPlanSource, SelfSpeculationSettingsInput } from "./self-speculation.ts";
import { normalizeSelfSpeculationSettings } from "./self-speculation.ts";
import type { ToolOperation } from "./settlement.ts";
import { speculativeToolInvocation } from "./speculative-tool.ts";
import { SubcomputeService } from "./subcompute.ts";
import type { ExecutionResourceMonitor } from "./system-resources.ts";
import { createSystemResourceMonitor } from "./system-resources.ts";
import type { TimelineDependency } from "./task-timing.ts";
import { TimelineInterval } from "./task-timing.ts";
export interface DraftOptionsContext {
	readonly actorModel: Model<Api>;
	readonly draftModel: Model<Api>;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly signal: AbortSignal;
}
export type DraftModelSelection =
	Model<Api> | ((actorModel: Model<Api>) => Model<Api> | undefined | Promise<Model<Api> | undefined>);
/** Model services supplied by the host; prediction hints remain private source wiring. */
export interface DrafterModelOptions {
	/** Defaults to the Actor model when omitted or unresolved. */
	readonly draftModel?: DraftModelSelection;
	/** Resolve request options, including credentials for a different provider. */
	readonly getDraftOptions?: (
		context: DraftOptionsContext,
	) => SimpleStreamOptions | Promise<SimpleStreamOptions>;
	readonly complete: (
		model: Model<Api>,
		context: Context,
		options?: SimpleStreamOptions,
	) => Promise<AssistantMessage>;
	/** Share usage accounting with other model requests in the same user task. */
	readonly drafterBudget?: DrafterTaskBudget;
}
export interface AgentStartInput
	extends Pick<DraftOptionsContext, "actorModel" | "actorOptions">, Omit<TurnInput<string>, "terminal"> {
	readonly context: Context;
	readonly tools: readonly AgentTool[];
}
export interface AgentConsumeInput extends Omit<ActualToolCall, "input">, TurnInput<string> {
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
}
export interface AgentStateData {
	readonly tools: ReadonlyMap<string, AgentTool>;
	readonly schemaHashes: Readonly<Record<string, string>>;
}
export type AgentPlanSource = SpeculativePlanSource<string, ToolSettlement, AgentStartInput, AgentStateData>;
export type AgentRuntime = SpeculativeActionRuntime<
	string,
	ToolSettlement,
	AgentStartInput,
	AgentConsumeInput,
	AgentConsumeInput,
	AgentStateData
>;
export type AgentRuntimeEvents = RuntimeEventMap<
	string,
	ToolSettlement,
	AgentStartInput,
	AgentConsumeInput,
	AgentStateData
>;
export function agentBatchKey(sessionID: string, turnID: string): string {
	return JSON.stringify([sessionID, turnID]);
}
export function definitionSchemaHashes(
	definitions: readonly {
		readonly name: string;
		readonly inputSchema?: unknown;
	}[],
): Readonly<Record<string, string>> {
	return Object.fromEntries(
		definitions.map((definition) => [definition.name, stableValueHash(definition.inputSchema ?? null)]),
	);
}
type Execute = (tool: AgentTool, args: Parameters<AgentTool["execute"]>) => ReturnType<AgentTool["execute"]>;
type Turn = {
	readonly tools: readonly AgentTool[];
	execute?: Execute;
	closed?: boolean;
};
type Registered = {
	readonly execute: Function;
	readonly stock: boolean;
};
const registeredTools = new WeakMap<AgentTool, Registered>();
const sdkSessions = new WeakMap<
	object,
	{
		turn: Turn;
		tools: Map<string, AgentTool>;
		active: () => readonly string[];
	}
>();
const sdkExecution = new AsyncLocalStorage<AgentTool>();
const declaredSessionTools = new WeakMap<object, Map<string, AgentTool>>();
/** Register an explicit tool through Pi's public extension API. */
export function registerSpeculativeTool(pi: ExtensionAPI, definition: ToolDefinition<any, any>): void {
	const contexts = new WeakMap<object, ExtensionContext>();
	const instances = new WeakMap<object, AgentTool>();
	const select = (context: ExtensionContext) => {
		const key = context.sessionManager;
		contexts.set(key, context);
		let tool = instances.get(key);
		if (!tool) {
			tool = {
				...definition,
				execute: (callID, args, signal, update) =>
					definition.execute(callID, args, signal, update, contexts.get(key)!),
			};
			instances.set(key, tool);
			registeredTools.set(tool, { execute: definition.execute, stock: false });
			let tools = declaredSessionTools.get(key);
			if (!tools) declaredSessionTools.set(key, (tools = new Map()));
			tools.set(tool.name, tool);
		}
		return tool;
	};
	pi.on("session_start", (_event, context) => {
		select(context);
	});
	pi.registerTool({
		...definition,
		execute: (callID, args, signal, update, context) =>
			executeSdkTool(context, select(context), [callID, args, signal, update], definition),
	});
}
/** An explicit public tool outlet; registration and host execution own this binding. */
export async function executeSdkTool(
	context: ExtensionContext,
	tool: AgentTool,
	args: Parameters<AgentTool["execute"]>,
	definition?: ToolDefinition,
): ReturnType<AgentTool["execute"]> {
	if (sdkExecution.getStore() === tool) return tool.execute(...args);
	if (definition) registeredTools.set(tool, { execute: definition.execute, stock: false });
	const scope = sdkSessions.get(context.sessionManager);
	if (!scope) return tool.execute(...args);
	const selected = definition ? tool : (scope.tools.get(tool.name) ?? tool);
	scope.tools.set(tool.name, selected);
	return sdkExecution.run(selected, () =>
		scope.turn.execute && !scope.turn.closed ? scope.turn.execute(selected, args) : selected.execute(...args),
	);
}
/** Keep opaque callbacks available for child-operation learning, never for executing a host function ahead. */
const callbackTools = new WeakMap<
	AgentTool,
	{
		readonly source: AgentTool;
		readonly execute: AgentTool["execute"];
		readonly tool: AgentTool;
		readonly invocation: ToolInvocation;
	}
>();
export function sdkCallbackTool(tool: AgentTool): AgentTool {
	let entry = callbackTools.get(tool);
	if (!entry || entry.execute !== tool.execute) {
		// The SDK already prepared Actor arguments. An arbitrary preparation callback cannot run ahead on the host.
		entry = {
			source: tool,
			execute: tool.execute,
			tool: {
				...tool,
				prepareArguments: undefined,
				execute: tool.execute.bind(tool),
			},
			invocation: {
				executor: "pi.sdk.callback",
				callback: currentDefinition(tool)?.execute ?? tool.execute,
				identity: { callback: randomUUID() },
				semantics: {
					tool: tool.name,
					epoch: "pi.sdk.callback:1",
					effect: "unbounded",
					requirements: {
						capabilities: ["invocation.host_function", ...UNRESTRICTED_PROCESS_EFFECTS.capabilities],
					},
					canonicalize: (input) => {
						const record = asRecord(input);
						return record && { input: record, resources: [] };
					},
				},
			},
		};
		callbackTools.set(tool, entry);
		callbackTools.set(entry.tool, entry);
	}
	return entry.tool;
}
export function sdkToolInvocation(tool: AgentTool, cwd?: string): ToolInvocation {
	const entry = callbackTools.get(tool);
	if (!entry) return sdkToolInvocation(sdkCallbackTool(tool), cwd);
	const callback = currentDefinition(entry.source)?.execute;
	return (cwd && callback && speculativeToolInvocation(callback, cwd)) || entry.invocation;
}
function currentDefinition(tool: AgentTool): Registered | undefined {
	return registeredTools.get(tool);
}
export const isStockSdkTool = (tool: AgentTool): boolean => registeredTools.get(tool)?.stock === true;
export function currentSdkTurn(context: ExtensionContext): Turn | undefined {
	const scope = sdkSessions.get(context.sessionManager);
	if (!scope) return;
	scope.turn.closed = false;
	return scope.turn;
}
export function attachSdkTools(
	context: ExtensionContext,
	tools: readonly AgentTool[] = [],
	active = () => tools.map((tool) => tool.name),
) {
	const available = new Map(tools.map((tool) => [tool.name, tool]));
	for (const tool of tools) registeredTools.set(tool, { execute: tool.execute, stock: true });
	for (const [name, tool] of declaredSessionTools.get(context.sessionManager) ?? [])
		available.set(name, tool);
	const turn: Turn = {
		get tools() {
			return active().flatMap((name) => (available.has(name) ? [available.get(name)!] : []));
		},
	};
	const scope = { turn, tools: available, active };
	sdkSessions.set(context.sessionManager, scope);
	return {
		tools: () => turn.tools,
		dispose: () => {
			turn.closed = true;
			if (sdkSessions.get(context.sessionManager) === scope) sdkSessions.delete(context.sessionManager);
			declaredSessionTools.delete(context.sessionManager);
		},
	};
}
const ACTOR_OPERATION = Symbol("actor-operation");
const RAW_ACTOR_CALL = Symbol("raw-actor-call");
type AgentToolOperation = ToolOperation & {
	readonly invocation?: ToolInvocation;
};
type BoundActorCall = AgentConsumeInput & {
	readonly [ACTOR_OPERATION]?: () => Promise<AgentToolOperation>;
	readonly [RAW_ACTOR_CALL]?: true;
};
export interface SpeculativeAgentSettingsInput
	extends Partial<DrafterRequestSettings>, Partial<Omit<SpeculativeActionSettings, "sourceConfig">> {
	readonly patternAware?: Partial<PatternAwareSettings>;
	readonly selfSpeculation?: SelfSpeculationSettingsInput;
}
/** Shared host/package boundary: the TUI and runtime must interpret saved policy identically. */
export function normalizeSpeculativeAgentSettings(input: SpeculativeAgentSettingsInput = {}) {
	const defaults = speculativeConfig().runtime;
	return {
		...normalizeDrafterRequestSettings(input),
		enabled: booleanOr(input.enabled, defaults.enabled),
		reuseEnabled: booleanOr(input.reuseEnabled, defaults.reuseEnabled),
		drafterEnabled: booleanOr(input.drafterEnabled, defaults.drafterEnabled),
		prepareObservedOperations: booleanOr(input.prepareObservedOperations, defaults.prepareObservedOperations),
		...(input.tools === undefined
			? {}
			: {
					tools:
						Array.isArray(input.tools) && input.tools.every((tool) => typeof tool === "string")
							? [...new Set(input.tools)]
							: [],
				}),
		candidateLimit: positiveInteger(input.candidateLimit, defaults.candidateLimit),
		maxConcurrentActions: positiveInteger(input.maxConcurrentActions, defaults.maxConcurrentActions),
		resourceCacheMaxEntries: positiveInteger(input.resourceCacheMaxEntries, defaults.resourceCacheMaxEntries),
		resourceCacheMaxBytes: positiveInteger(input.resourceCacheMaxBytes, defaults.resourceCacheMaxBytes),
		predictionTimeoutMs: positiveMilliseconds(input.predictionTimeoutMs, undefined),
		scheduling: normalizeSchedulingSettings(input.scheduling),
		patternAware: patternAwareSettings(input.patternAware),
		selfSpeculation: normalizeSelfSpeculationSettings(input.selfSpeculation),
	} as const;
}
export interface SpeculativeAgentPreflightContext extends Pick<
	SpeculativeToolExecutionContext,
	"tool" | "toolName" | "args" | "action" | "signal"
> {
	readonly route: SpeculativeExecutionRoute;
}
export interface CreateSpeculativeActionHostOptions extends DrafterModelOptions {
	readonly config?: SpeculativeConfigInput;
	/** Override host observations for embedding or deterministic tests. */
	readonly resources?: ExecutionResourceMonitor;
	/** Canonical K(a), projection, and resource-version semantics for this host. */
	readonly actionSemantics?: ActionSemanticsRegistry;
	/** Workspace root used for action canonicalization and resource validation. */
	readonly cwd: string;
	/** Runtime settings. Speculative production defaults off; caching, reuse and observation default on. */
	readonly getSettings?: () => SpeculativeAgentSettingsInput | Promise<SpeculativeAgentSettingsInput>;
	/** Bind the concrete executor used by both speculative and Actor calls; rejection fails this invocation. */
	readonly resolveInvocation?: (
		tool: string,
		input: unknown,
		definition?: AgentTool,
	) => ToolInvocation | undefined | Promise<ToolInvocation | undefined>;
	/**
	 * Non-interactive permission and policy check for speculative execution.
	 * Candidates are rejected when this callback is absent.
	 */
	readonly preflight?: (
		context: SpeculativeAgentPreflightContext,
	) => boolean | CandidatePreflight | Promise<boolean | CandidatePreflight>;
	/** Lossless Π rules; each rule owns key relation, realized coverage, and output reconstruction. */
	readonly projectionRules?: readonly ActionProjectionRule<ToolSettlement>[];
	/** Actor probe source shared with the decoder-feedback coordinator. */
	readonly actorForkPlanSource?: ActorForkPlanSource;
	/** Ordered execution capabilities. A runtime-wide sandbox takes precedence over local fallbacks. */
	readonly executionWorlds?: readonly AgentExecutionWorld[];
	/** Dynamic policy for speculative execution and the public Host's native route; resource observation stays independent. */
	readonly speculativeExecutionWorldEnabled?: (backend: string) => boolean;
	/** Optional persistence root for workspace-hashed PatternAware state. */
	readonly patternStateDirectory?: string;
	/** The configured owner already reserved the store directory; publication may not recreate it. */
	readonly patternStoreReservedRoot?: boolean;
	/** Stable logical workspace identity when physical checkout paths are ephemeral. */
	readonly patternWorkspaceIdentity?: string;
	/** Optional injected store, primarily for embedding and deterministic tests. */
	readonly patternStore?: PatternAwareStore | Promise<PatternAwareStore>;
	readonly onEvent?: (event: SpeculativeActionEvent<string>) => void | Promise<void>;
}
export interface SpeculativeActionHost {
	readonly sessionID: string;
	readonly runtime: AgentRuntime;
	readonly executionWorldDiagnostics: (
		refresh?: boolean,
	) => Promise<readonly ExecutionWorldDiagnosticSnapshot[]>;
	readonly startTurn: (input: Omit<AgentStartInput, "sessionID">, signal?: AbortSignal) => Promise<void>;
	readonly previewActorTool: (
		input: {
			readonly turnID: string;
			readonly tool: string;
		},
		signal?: AbortSignal,
	) => Promise<void>;
	/** Raw streamed arguments; preparation is provisional and never binds the final Actor call. */
	readonly previewActorCall: (
		input: Omit<AgentConsumeInput, "sessionID">,
		signal?: AbortSignal,
	) => Promise<void>;
	/** One tool outlet: reuse lookup, Actor fallback, timing, and settlement reporting. */
	readonly execute: (
		input: SpeculativeToolExecutionInput,
		signal: AbortSignal | undefined,
		executor: (operation: AgentToolOperation) => Promise<AgentToolResult<unknown>>,
	) => Promise<AgentToolResult<unknown>>;
	readonly finishTurn: (turnID: string, terminal?: boolean) => Promise<void>;
	readonly drafterBudgetSnapshot: () => ActionDrafterBudgetSnapshot;
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
export type ActionDrafterBudgetSnapshot = DrafterPlanSnapshot;
export { patternPlanActionID } from "./pattern-plan-source.ts";
/**
 * Build source-neutral speculative plan execution for a host. The host owns lifecycle and tool interception.
 */
export function createSpeculativeActionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
): SpeculativeActionHost {
	return createAgentExecutionHost(sessionID, options, (world, operation) =>
		world.isolation === "resource_snapshot" ||
		(isRegisteredExecutionWorld(world, operation) &&
			(operation === "speculation" || options.speculativeExecutionWorldEnabled?.(world.id) !== false)) ||
		(operation === "speculation" && world.isolation === "workspace_branch")
			? undefined
			: "Outside the supported resource closure; the Actor executes this operation",
	);
}
/** Internal engine assembly for backend qualification. The public Pi Host always applies its resource closure. */
export function createAgentExecutionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
	admission?: (
		world: AgentExecutionWorld,
		operation: "speculation" | "observation" | "observeOperations",
	) => string | undefined,
): SpeculativeActionHost {
	const config = resolveSpeculativeConfig(options.config);
	const storeRoot = config.storage.root ?? path.join(os.tmpdir(), "pi-host-reuse-" + randomUUID());
	const store = withSpeculativeConfig(config, () => new ProvenanceCertificateStore(storeRoot));
	const service = withSpeculativeConfig(
		config,
		() => new SubcomputeService(options.cwd, store, () => config.runtime.resourceCacheMaxBytes),
	);
	const bind = <T extends object>(target: T): T =>
		new Proxy(target, {
			get(object, key) {
				const value = Reflect.get(object, key, object);
				if (typeof value !== "function") return value;
				return (...args: unknown[]) =>
					withSpeculativeConfig(config, () => service.run(() => Reflect.apply(value, object, args)));
			},
		});
	const host = withSpeculativeConfig(config, () =>
		assembleAgentExecutionHost(sessionID, options, store, admission),
	);
	return bind({
		...host,
		runtime: bind(host.runtime),
		dispose: async () => {
			try {
				await host.dispose();
			} finally {
				try {
					await service.dispose();
				} finally {
					await store.dispose();
					if (!config.storage.root) await rm(storeRoot, { recursive: true, force: true });
				}
			}
		},
	});
}
function assembleAgentExecutionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
	store: ProvenanceCertificateStore,
	admission?: (
		world: AgentExecutionWorld,
		operation: "speculation" | "observation" | "observeOperations",
	) => string | undefined,
): SpeculativeActionHost {
	const actionSemantics = options.actionSemantics ?? PI_ACTION_SEMANTICS;
	const projectionRules = resolveActionProjectionRules(
		[
			...(options.projectionRules ?? []),
			...(!speculativeConfig().explicitOnly
				? [PI_BASH_TIMEOUT_PROJECTION_RULE, PI_GREP_LITERAL_PROJECTION_RULE]
				: []),
		],
		actionSemantics,
	);
	let inputMaxBytes = speculativeConfig().runtime.resourceCacheMaxBytes;
	const executionWorlds = [...new Set(options.executionWorlds ?? [])];
	if (
		!executionWorlds.some(
			(world) =>
				world.id === "resource_version" &&
				world.scope === "fallback" &&
				world.isolation === "resource_snapshot",
		)
	) {
		executionWorlds.push(
			createResourceSnapshotExecutionWorld(actionSemantics, {
				maxBytes: () => inputMaxBytes,
				persistentInputs: { store, internalCache: true },
			}),
		);
	}
	const inClosure = (world: AgentExecutionWorld) => !admission?.(world, "speculation");
	const executionGateway = new ToolExecutionGateway(
		executionWorlds,
		options.speculativeExecutionWorldEnabled,
		undefined,
		admission,
	);
	const resolveExecutionRoute = (
		tool: string,
		signal?: AbortSignal,
		action?: ActionKey,
		parentCheckpoint?: WorldCheckpoint,
	) => {
		const invocation = action?.executionContext as ToolInvocation | undefined;
		if (invocation?.authoritativeOnly && !invocation.operation) return undefined;
		const definition = actionSemantics.definition(action ?? tool);
		return (
			definition &&
			executionGateway.resolve(
				{
					tool,
					action,
					parentCheckpoint,
					backend: (action?.executionContext as ToolInvocation | undefined)?.operation?.binding.backend,
					effect: definition.effect,
					requirements: definition.requirements,
				},
				{ cwd: options.cwd, signal },
			)
		);
	};
	const resolveSettings = async (): Promise<SpeculativeActionSettings> => {
		const {
			patternAware,
			selfSpeculation,
			drafterMaxDepth,
			drafterDeterministicCandidates,
			drafterTemperatureMin,
			drafterTemperatureMax,
			drafterPatternHints,
			...policy
		} = normalizeSpeculativeAgentSettings(await options.getSettings?.());
		inputMaxBytes = options.config?.runtime?.resourceCacheMaxBytes ?? policy.resourceCacheMaxBytes;
		return {
			...policy,
			resourceCacheMaxEntries:
				options.config?.runtime?.resourceCacheMaxEntries ?? policy.resourceCacheMaxEntries,
			resourceCacheMaxBytes: inputMaxBytes,
			sourceConfig: {
				drafterMaxDepth,
				drafterDeterministicCandidates,
				drafterTemperatureMin,
				drafterTemperatureMax,
				drafterPatternHints,
				patternAware,
				actorForkActionEnabled:
					options.actorForkPlanSource !== undefined &&
					selfSpeculation.enabled &&
					selfSpeculation.forkEnabled &&
					selfSpeculation.forkActionEnabled &&
					selfSpeculation.forkTransport !== "provider",
			},
		};
	};
	const drafterPlans = createDrafterPlanSource({
		sessionID,
		draftModel: options.draftModel,
		getDraftOptions: options.getDraftOptions,
		complete: options.complete,
		drafterBudget: options.drafterBudget,
		patternHints: (hint) => patternPlans.hints(hint),
	});
	const patternPlans = createPatternPlanSource({
		sessionID,
		cwd: options.cwd,
		actionSemantics,
		projectionRules,
		stateDirectory: options.patternStateDirectory,
		reservedRoot: options.patternStoreReservedRoot,
		workspaceIdentity: options.patternWorkspaceIdentity,
		store: options.patternStore,
	});
	const executionPreparation = createExecutionPreparation(options.cwd, actionSemantics);
	const resolveBinding = async (
		tool: string,
		input: unknown,
		schemaHash?: string,
		definition?: AgentTool,
	) => {
		// A candidate runs its key's canonical input (a redundant `cd <cwd> &&` dropped): its invocation is resolved from that input.
		const resolved =
			(await options.resolveInvocation?.(
				tool,
				(() => {
					try {
						return actionSemantics.definition(tool)?.canonicalize(input, options.cwd)?.input ?? input;
					} catch {
						return input;
					}
				})(),
				definition,
			)) ??
			(definition && speculativeToolInvocation(definition.execute, options.cwd));
		const invocation =
			resolved &&
			Object.freeze({
				...resolved,
				...(resolved.observation
					? {
							observation: Object.freeze({
								...resolved.observation,
								content: immutableSnapshot(resolved.observation.content),
							}),
						}
					: {}),
				...(resolved.identity !== undefined ? { identity: immutableSnapshot(resolved.identity) } : {}),
				...(resolved.process ? { process: immutableSnapshot(resolved.process) } : {}),
			});
		let action;
		if (
			schemaHash === undefined ||
			![input, invocation?.identity, invocation?.process].every(isImmutableSnapshot)
		) {
			action = undefined;
		} else {
			action = actionSemantics.buildKey(
				tool,
				input,
				options.cwd,
				schemaHash,
				invocation
					? {
							fingerprint: stableValueHash(
								invocation.filesystem
									? [
											invocation.identity ?? invocation,
											{
												filesystemRoot: invocation.filesystemRoot ?? options.cwd,
											},
										]
									: (invocation.identity ?? invocation),
							),
							context: invocation,
							semantics: invocation.semantics,
						}
					: undefined,
			);
		}
		return {
			...(invocation ? { invocation } : {}),
			...(action ? { action } : {}),
		};
	};
	const checkPermission = async (
		tool: AgentTool | undefined,
		context: Omit<SpeculativeAgentPreflightContext, "tool">,
		recheck = false,
	): Promise<CandidatePreflight> => {
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
		resources:
			options.resources ??
			createSystemResourceMonitor(async () => (await options.getSettings?.())?.scheduling),
		sources: [
			patternPlans.source,
			drafterPlans.source,
			executionPreparation.source,
			...(options.actorForkPlanSource ? [options.actorForkPlanSource.source] : []),
		],
		settings: resolveSettings,
		definitions: (input) =>
			input.tools.map((tool) => ({
				name: tool.name,
				description: tool.description,
				inputSchema: tool.parameters,
			})),
		stateData: (input) => ({
			tools: new Map(input.tools.map((tool) => [tool.name, tool])),
			// Concrete candidates prepare their routes inside the runtime's resource budget.
			// Warming every possible tool here would bypass that budget before a proposal exists.
			schemaHashes: definitionSchemaHashes(
				input.tools.map((tool) => ({
					name: tool.name,
					inputSchema: tool.parameters,
				})),
			),
		}),
		actionKey: async (toolName, input, context) => {
			let tool: AgentTool | undefined;
			let validated: unknown;
			if (context.type === "consume") {
				const call = context.consumeInput as BoundActorCall;
				const bind = call[ACTOR_OPERATION];
				if (bind) return (await bind()).action;
				tool = call.tools.find((candidate) => candidate.name === toolName);
				validated = call[RAW_ACTOR_CALL]
					? tool && prepareToolArguments(tool, input)
					: immutableSnapshot(input);
			} else {
				tool = context.data.tools.get(toolName);
				validated = tool && prepareToolArguments(tool, input);
			}
			if (!tool || validated === undefined) return undefined;
			const schemaHash =
				context.type === "consume"
					? stableValueHash(tool.parameters ?? null)
					: context.data.schemaHashes[toolName];
			const { action, invocation } = await resolveBinding(toolName, validated, schemaHash, tool);
			const operation = context.type === "start" ? context.operation : undefined;
			if (!operation) return action;
			const definition = action && actionSemantics.definition(action);
			if (!action || !invocation || !definition || operation.permissionHash !== action.hash) return undefined;
			const epoch = `${definition.epoch}:operation:${operation.backend}`;
			return buildActionKey({
				...action,
				input: { operation: operation.identity },
				semanticsEpoch: epoch,
				semantics: {
					...definition,
					resourceScope: undefined,
					...operation.semantics,
					epoch,
					projectors: [],
				},
				executionContext: Object.freeze({
					...invocation,
					operation: Object.freeze({ binding: operation, permission: action }),
				}),
			});
		},
		resolveExecution: ({ tool, action, signal, parentWorld }) =>
			resolveExecutionRoute(tool, signal, action, parentWorld?.checkpoint),
		captureAuthoritativeResult: async ({
			startInput,
			consumeInput,
			tool: toolName,
			concrete,
			action,
			callID,
			signal,
		}) => {
			const tool = consumeInput.tools.find((tool) => tool.name === toolName);
			if (!tool) return undefined;
			const invocation =
				(await (consumeInput as BoundActorCall)[ACTOR_OPERATION]?.())?.invocation ??
				(action?.executionContext as ToolInvocation | undefined);
			const definition = invocation?.semantics ?? actionSemantics.definition(action ?? toolName);
			const captured = await executionGateway.captureAuthoritativeResult(
				{
					tool: toolName,
					callID,
					action,
					executionContext: invocation,
					effect: definition?.effect ?? "unbounded",
					requirements: definition?.requirements ?? UNRESTRICTED_PROCESS_EFFECTS,
				},
				{ cwd: options.cwd, signal },
				{
					cwd: options.cwd,
					tool,
					toolName,
					args: concrete,
					action,
					executionContext: invocation,
					callID,
					signal,
					executionScope: {
						sessionID: startInput.sessionID,
						turnID: startInput.turnID,
					},
				},
			);
			return (
				captured && {
					route: captured.route,
					computationDependencies: captured.computationDependencies,
					...captured.capture,
				}
			);
		},
		actual: (input) => ({ id: input.id, tool: input.tool, input: input.args }),
		preflightCandidate: ({ data, tool: toolName, concrete, action, route, signal }) => {
			const permission =
				(action.executionContext as ToolInvocation | undefined)?.operation?.permission ?? action;
			return checkPermission(data.tools.get(toolName), {
				toolName,
				args: permission === action ? concrete : permission.input,
				action: permission,
				route,
				signal,
			});
		},
		authorizeCandidate: ({ stateData, tool: toolName, concrete, action, route, signal }) =>
			checkPermission(
				stateData.tools.get(toolName),
				{
					toolName,
					args: concrete,
					action,
					route,
					signal: signal ?? new AbortController().signal,
				},
				true,
			),
		executeCandidate: async ({
			startInput,
			data,
			tool: toolName,
			concrete,
			route,
			parentWorld,
			candidate: _candidate,
			index: _index,
			...context
		}) => {
			const { action, callID, signal } = context;
			const tool = data.tools.get(toolName);
			if (!tool) throw new Error(`Tool ${toolName} not found`);
			const args = structuredClone(concrete);
			const executionScope = {
				sessionID: startInput.sessionID,
				turnID: startInput.turnID,
				callID,
			};
			return executionGateway.executeSpeculative(
				{
					tool: toolName,
					callID,
					input: args,
					signal,
					action,
					scope: executionScope,
				},
				route,
				{
					...context,
					cwd: options.cwd,
					tool,
					toolName,
					args,
					executionScope,
					...(parentWorld?.checkpoint ? { parentCheckpoint: parentWorld.checkpoint } : {}),
				},
			);
		},
		observationKey: toolObservationKey,
		rejectCandidateOutput: ({ output }) =>
			output.isError && output.exitCode === undefined ? "tool_error_result" : undefined,
		projectionRules,
		onEvent: options.onEvent,
	});
	const disconnect = [
		patternPlans.connect(runtime),
		drafterPlans.connect(runtime),
		executionPreparation.connect(runtime),
	];
	return {
		sessionID,
		runtime,
		executionWorldDiagnostics: (refresh = false) =>
			executionGateway.diagnostics({
				cwd: options.cwd,
				...(refresh ? { refresh: true } : {}),
			}),
		startTurn: (input, signal) => runtime.startTurn({ ...input, sessionID }, signal),
		previewActorTool: (input, signal) => runtime.previewActorTool({ ...input, sessionID }, signal),
		previewActorCall: (input, signal) =>
			runtime.previewActorCall({ ...input, sessionID, [RAW_ACTOR_CALL]: true } as BoundActorCall, signal),
		execute: (input, signal, executor) =>
			runtime.trackActorTool(sessionID, async (issuedAt) => {
				const scope = input.turnID
					? {
							sessionID,
							turnID: input.turnID,
							...(input.id !== undefined ? { callID: input.id } : {}),
						}
					: undefined;
				const operation: AgentToolOperation = {
					tool: input.tool,
					scope,
					input: immutableSnapshot(input.args),
					...(input.id ? { callID: input.id } : {}),
					...(signal ? { signal } : {}),
				};
				let binding: Promise<AgentToolOperation> | undefined;
				// One invocation owns its binding; prepare after Actor arrival so timing includes binding cost.
				const bind = () =>
					(binding ??= (async () => {
						const tool = input.tools.find((tool) => tool.name === input.tool);
						return Object.freeze({
							...operation,
							...(await resolveBinding(
								operation.tool,
								operation.input,
								tool ? stableValueHash(tool.parameters ?? null) : undefined,
								tool,
							)),
						});
					})());
				const actorCall = input.turnID
					? {
							...input,
							sessionID,
							turnID: input.turnID,
							args: operation.input,
							[ACTOR_OPERATION]: bind,
						}
					: undefined;
				let prepared: PreparedActorCall<ToolSettlement> | undefined;
				let operations: ExecutionOperationBinding[] | undefined;
				const computations: TimelineDependency[] = [];
				return executionGateway.executeAuthoritative(
					operation,
					async () => {
						const bound = await bind();
						const execute = (inputs?: (path?: string) => Iterable<object>) =>
							scope
								? executionGateway.observeOperations(
										bound.action,
										scope,
										() => executor(bound),
										(bindings, dependencies) => {
											if (bindings.length) (operations ??= []).push(...bindings);
											if (dependencies) computations.push(...dependencies);
										},
										prepared?.observeOperations,
										inputs,
										{
											cwd: options.cwd,
											signal,
											reuseEnabled: prepared?.reuseEnabled,
										},
									)
								: executor(bound);
						try {
							return await (prepared?.withInputs ? prepared.withInputs(execute) : execute());
						} finally {
							if (
								(bound.invocation?.semantics ?? actionSemantics.definition(bound.action ?? bound.tool))
									?.effect !== "observation"
							) {
								await TimelineInterval.outside(() =>
									Promise.allSettled(
										executionWorlds.filter(inClosure).map(async (world) => world.actorFallbackSettled?.()),
									),
								);
							}
						}
					},
					{
						computationDependencies: () => computations,
						...(actorCall
							? {
									reuse: async () => {
										prepared = await runtime.prepareActorCall(actorCall, signal, issuedAt);
										const output = prepared?.output;
										if (output?.isError)
											throw ToolExecutionGateway.reusedFailure(
												output.result.content
													.flatMap((item) => (item.type === "text" ? [item.text] : []))
													.join("\n"),
											);
										return output?.result;
									},
									settled: async (settlement) => {
										if (!prepared) return;
										// The Actor's own failed command carries its exit code, as a speculative one does.
										const output =
											settlement.status === "succeeded"
												? { result: settlement.output, isError: false }
												: piToolErrorSettlement(input.tool, settlement.error);
										await prepared.settle(settlement.toolExecution, output, operations);
									},
								}
							: {}),
					},
				);
			}),
		drafterBudgetSnapshot: drafterPlans.snapshot,
		finishTurn: async (turnID, terminal = false) => {
			try {
				await runtime.finishTurn({
					sessionID,
					turnID,
					tool: "",
					args: {},
					tools: [],
					terminal,
				});
			} finally {
				const closed = await Promise.allSettled(
					executionWorlds.filter(inClosure).map(async (world) => world.finishTurn?.(turnID)),
				);
				const failures = closed.flatMap((result) => (result.status === "rejected" ? [result.reason] : []));
				if (failures.length) throw new AggregateError(failures, "Execution world turn cleanup failed");
			}
		},
		dispose: async () => {
			try {
				await runtime.disposeSession(sessionID);
			} finally {
				for (const unsubscribe of disconnect) unsubscribe();
				try {
					await patternPlans.dispose();
				} finally {
					try {
						await executionPreparation.dispose();
					} finally {
						await executionGateway.dispose();
					}
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
		return immutableSnapshot(
			validateToolArguments(tool, {
				type: "toolCall",
				id: "spec_key",
				name: tool.name,
				arguments: prepared as Record<string, unknown>,
			}),
		);
	} catch {
		return undefined;
	}
}
