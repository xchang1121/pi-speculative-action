import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { validateToolArguments } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import {
	SpeculativeConfiguration,
	speculativeConfig,
	withSpeculativeConfig,
	type SpeculativeConfigInput,
} from "./config-schema.ts";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { ActionKey, ActionSemanticsRegistry } from "./action-semantics.ts";
import { actionKeyMatch, buildActionKey, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type { AgentExecutionWorld, SpeculativeToolExecutionContext } from "./agent-execution-world.ts";
import { createResourceSnapshotExecutionWorld } from "./agent-execution-world.ts";
import type { DrafterRequestSettings } from "./config-schema.ts";
import {
	booleanOr,
	normalizeDrafterRequestSettings,
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
	UNRESTRICTED_PROCESS_EFFECTS,
} from "./execution-world.ts";
import type { PatternAwareSettings, PatternAwareStore } from "./pattern-aware.ts";
import { patternAwareSettings } from "./pattern-aware.ts";
import { createPatternPlanSource } from "./pattern-plan-source.ts";
import { piToolErrorSettlement } from "./pi-tool-invocation.ts";
import { asRecord, immutableSnapshot, isImmutableSnapshot, stableValueHash } from "./primitives.ts";
import { observeResourceChanges } from "./resource-version.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import type {
	ActualToolCall,
	CandidatePreflight,
	CandidateEventDescriptor,
	CandidateRejection,
	PredictionIdentity,
	PredictionSettlement,
	SpeculativeCacheSnapshot,
	RuntimeEventMap,
	SpeculativeActionEvent,
	SpeculativeActionSettings,
	TurnInput,
} from "./settlement.ts";
import { LocalExecutionSession } from "./runtime-engine.ts";
import type { ExecutionCall, ExecutionSession, SpeculativeRun } from "./execution-session.ts";
import type { CallGenerator, GeneratorTurn, Prediction } from "./call-generators.ts";
import { cause, diagnosticAction, RuntimeSubscriptions, runSourceRequest } from "./settlement.ts";
import { PendingTasks } from "./runtime-lifecycle.ts";
import type { ActorForkPlanSource, SelfSpeculationSettingsInput } from "./self-speculation.ts";
import { normalizeSelfSpeculationSettings } from "./self-speculation.ts";
import type { ToolOperation } from "./settlement.ts";
import { speculativeToolInvocation, speculativeToolPatternIdentity } from "./speculative-tool.ts";
import { SubcomputeService, type TrustedSubcomputeStep } from "./subcompute.ts";
import { PythonImportStep } from "./python-compilation.ts";
import { TaskTimeline, TimelineInterval, type SubcomputeCounts } from "./task-timing.ts";
export interface DraftOptionsContext {
	readonly actorModel: Model<Api>;
	readonly draftModel: Model<Api>;
	readonly actorOptions: SimpleStreamOptions | undefined;
	readonly signal: AbortSignal;
}
export type DraftModelSelection =
	| Model<Api>
	| ((actorModel: Model<Api>) => Model<Api> | undefined | Promise<Model<Api> | undefined>);
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
	extends Pick<DraftOptionsContext, "actorModel" | "actorOptions">,
		Omit<TurnInput<string>, "terminal"> {
	readonly context: Context;
	readonly tools: readonly AgentTool[];
}
export interface AgentConsumeInput extends Omit<ActualToolCall, "input">, TurnInput<string> {
	readonly args: unknown;
	readonly tools: readonly AgentTool[];
}
export interface AgentStateData {
	readonly patternIdentities?: Readonly<Record<string, string>>;
	readonly tools: ReadonlyMap<string, AgentTool>;
	readonly schemaHashes: Readonly<Record<string, string>>;
}
export type AgentRuntimeEvents = RuntimeEventMap<
	string,
	ToolSettlement,
	AgentStartInput,
	AgentConsumeInput,
	AgentStateData
>;
export interface AgentRuntime {
	subscribe: RuntimeSubscriptions<AgentRuntimeEvents>["subscribe"];
	settingsChanged(): Promise<void>;
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
		scope.turn.execute ? scope.turn.execute(selected, args) : selected.execute(...args),
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
					epoch: "pi.sdk.callback",
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
	return sdkSessions.get(context.sessionManager)?.turn;
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
			if (sdkSessions.get(context.sessionManager) !== scope) return;
			sdkSessions.delete(context.sessionManager);
			declaredSessionTools.delete(context.sessionManager);
		},
	};
}
type AgentToolOperation = ToolOperation & {
	readonly invocation?: ToolInvocation;
};
export interface SpeculativeAgentSettingsInput
	extends Partial<DrafterRequestSettings>,
		Partial<Omit<SpeculativeActionSettings, "sourceConfig">> {
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
		patternAware: patternAwareSettings(input.patternAware),
		selfSpeculation: normalizeSelfSpeculationSettings(input.selfSpeculation),
	} as const;
}
export interface SpeculativeAgentPreflightContext
	extends Pick<SpeculativeToolExecutionContext, "tool" | "toolName" | "args" | "action" | "signal"> {
	readonly route: SpeculativeExecutionRoute;
}
export interface CreateSpeculativeActionHostOptions extends DrafterModelOptions {
	readonly trustedSteps?: readonly TrustedSubcomputeStep[];
	readonly provenanceStore?: ProvenanceCertificateStore;
	readonly executionSession?: ExecutionSession<ToolSettlement>;
	readonly config?: SpeculativeConfigInput;
	readonly configuration?: SpeculativeConfiguration;
	/** Canonical K(a) and resource-version semantics for this host. */
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
	/** Raw streamed arguments; preparation is provisional and never binds the final Actor call. */
	readonly previewActorCall: (
		input: Omit<AgentConsumeInput, "sessionID">,
		signal?: AbortSignal,
	) => Promise<SpeculativeRun<ToolSettlement> | undefined>;
	/** One tool outlet: reuse lookup, Actor fallback, timing, and settlement reporting. */
	readonly execute: (
		input: SpeculativeToolExecutionInput,
		signal?: AbortSignal,
		executor?: (operation: AgentToolOperation) => Promise<AgentToolResult<unknown>>,
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
/**
 * Connect the additional-call generators and the Actor outlet to one execution session.
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
	const config = options.configuration ?? new SpeculativeConfiguration(options.config);
	const ownsStore = !options.provenanceStore;
	const ownsRoot = ownsStore && !config.current.storage.root;
	const storeRoot = options.provenanceStore?.root ?? config.current.storage.root ?? path.join(os.tmpdir(), "pi-host-reuse-" + randomUUID());
	const store = options.executionSession
		? undefined
		: options.provenanceStore ?? withSpeculativeConfig(config, () => new ProvenanceCertificateStore(storeRoot));
	const service: SubcomputeService | undefined = !store
		? undefined
		: withSpeculativeConfig(
				config,
				() =>
					new SubcomputeService(
						options.cwd,
						store,
						() => service!.runtime().resourceCacheMaxBytes ?? config.current.runtime.resourceCacheMaxBytes,
						[
							...(process.platform === "linux" ? [new PythonImportStep(options.cwd)] : []),
							...(options.trustedSteps ?? []),
						],
					),
			);
	const bind = <T extends object>(target: T): T =>
		new Proxy(target, {
			get(object, key) {
				const value = Reflect.get(object, key, object);
				if (typeof value !== "function") return value;
				return (...args: unknown[]) =>
					withSpeculativeConfig(config, () =>
						service
							? service.run(() => Reflect.apply(value, object, args))
							: Reflect.apply(value, object, args),
					);
			},
		});
	const host = withSpeculativeConfig(config, () =>
		assembleAgentExecutionHost(sessionID, options, store, service, admission),
	);
	return bind({
		...host,
		runtime: bind(host.runtime),
		dispose: async () => {
			try {
				await host.dispose();
			} finally {
				try {
					await service?.dispose();
				} finally {
					if (ownsStore) {
						await store?.dispose();
						if (store && ownsRoot) await rm(storeRoot, { recursive: true, force: true });
					}
				}
			}
		},
	});
}
function assembleAgentExecutionHost(
	sessionID: string,
	options: CreateSpeculativeActionHostOptions,
	store: ProvenanceCertificateStore | undefined,
	service: SubcomputeService | undefined,
	admission?: (
		world: AgentExecutionWorld,
		operation: "speculation" | "observation" | "observeOperations",
	) => string | undefined,
): SpeculativeActionHost {
	const actionSemantics = options.actionSemantics ?? PI_ACTION_SEMANTICS;
	let inputMaxBytes = speculativeConfig().runtime.resourceCacheMaxBytes;
	const executionWorlds = options.executionSession ? [] : [...new Set(options.executionWorlds ?? [])];
	if (
		store &&
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
	const patternPlans = createPatternPlanSource({
		sessionID,
		cwd: options.cwd,
		actionSemantics,
		stateDirectory: options.patternStateDirectory,
		reservedRoot: options.patternStoreReservedRoot,
		workspaceIdentity: options.patternWorkspaceIdentity,
		store: options.patternStore,
	});
	const drafterPlans = createDrafterPlanSource({
		sessionID,
		draftModel: options.draftModel,
		getDraftOptions: options.getDraftOptions,
		complete: options.complete,
		drafterBudget: options.drafterBudget,
		patternHints: patternPlans.hints,
	});
	const resolveBinding = async (
		tool: string,
		input: unknown,
		schemaHash?: string,
		definition?: AgentTool,
	) => {
		if (options.executionSession) {
			const value = asRecord(input);
			return {
				action:
					value &&
					(actionSemantics.buildKey(tool, value, options.cwd, schemaHash) ??
						buildActionKey({ tool, input: value, resources: [], schemaHash })),
			};
		}
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
		const action =
			schemaHash === undefined ||
			![input, invocation?.identity, invocation?.process].every(isImmutableSnapshot)
				? undefined
				: actionSemantics.buildKey(
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
	type Binding = Awaited<ReturnType<typeof resolveBinding>>;
	type CallRecord = {
		binding: Binding;
		tool?: AgentTool;
		turnID: string;
		tools?: readonly AgentTool[];
		executor?: Parameters<SpeculativeActionHost["execute"]>[2];
		prediction?: Prediction;
		error?: { value: unknown };
		rejections: CandidateRejection[];
		steps?: Map<string, SubcomputeCounts>;
	};
	type PredictionState =
		| { readonly kind: "running"; readonly settled: Promise<void> }
		| { readonly kind: "completed"; readonly producerRunID: string };
	const calls = new WeakMap<ExecutionCall, CallRecord>();
	const measured = <T>(call: ExecutionCall, execute: () => T) =>
		service ? service.withMetrics(calls.get(call)!.steps ??= new Map(), execute) : execute();
	const subscriptions = new RuntimeSubscriptions<AgentRuntimeEvents>();
	const pending = new PendingTasks();
	const turns = new Map<string, GeneratorTurn>();
	const previews = new WeakMap<
		GeneratorTurn,
		Map<string, { input: string; run: Promise<SpeculativeRun<ToolSettlement> | undefined> }>
	>();
	const controllers = new WeakMap<GeneratorTurn, AbortController>();
	let currentSettings: SpeculativeActionSettings = {
		enabled: false,
		resourceCacheMaxEntries: speculativeConfig().runtime.resourceCacheMaxEntries,
	};
	if (service) service.runtime = () => currentSettings;
	let timeline = new TaskTimeline(performance.now());
	let sequence = 0;
	let decision = 0;
	let requestIndex = 0;
	let tokenTotal = 0;
	let closed = false;
	const owners = new Map<string, CandidateEventDescriptor>();
	const predictions = new Map<
		string,
		{
			prediction: Prediction;
			identity: PredictionIdentity;
			key: ActionKey;
			turnID: string;
			lastDecision: number;
			run: SpeculativeRun<ToolSettlement>;
			after?: string;
			descriptor?: CandidateEventDescriptor;
			state: PredictionState;
		}
	>();
	const lastActor = new Map<string, AgentRuntimeEvents["actor_materialized"]["identity"]>();
	const emptyCache = (): SpeculativeCacheSnapshot => ({
		cacheCapacity: currentSettings.resourceCacheMaxEntries,
		cacheCold: 0,
		cacheHot: 0,
		inFlightJobs: 0,
		resultEntries: 0,
		resultBytes: 0,
		branchEntries: 0,
		branchBytes: 0,
		exclusiveCandidates: 0,
		sharedCandidates: 0,
	});
	type Envelope = "sessionID" | "turnID" | "timestamp" | "cache";
	type Payload =
		SpeculativeActionEvent<string> extends infer E ? (E extends {} ? Omit<E, Envelope> : never) : never;
	const event = (turnID: string, payload: Payload, steps?: Map<string, SubcomputeCounts>) => {
		if (!options.onEvent) return;
		const subcomputations = service?.drainMetrics(steps);
		const full = {
			...payload,
			sessionID,
			turnID,
			timestamp: Date.now(),
			cache: local?.snapshot() ?? emptyCache(),
			...(subcomputations && Object.keys(subcomputations).length ? { subcomputations } : {}),
		} as SpeculativeActionEvent<string>;
		try {
			void Promise.resolve(options.onEvent(full)).catch(() => {});
		} catch {
			/* Optional diagnostics. */
		}
	};
	const catalog = (tools: readonly AgentTool[]): AgentStateData => ({
		tools: new Map(tools.map((tool) => [tool.name, tool])),
		schemaHashes: definitionSchemaHashes(tools.map((t) => ({ name: t.name, inputSchema: t.parameters }))),
		patternIdentities: Object.fromEntries(
			tools.flatMap((tool) => {
				const source = callbackTools.get(tool)?.source ?? tool;
				const id =
					speculativeToolPatternIdentity(currentDefinition(source)?.execute ?? source.execute) ??
					actionSemantics.definition(tool.name)?.patternIdentity;
				return id ? [[tool.name, id]] : [];
			}),
		),
	});
	const generators: CallGenerator[] = [
		patternPlans.source,
		drafterPlans.source,
		...(options.actorForkPlanSource ? [options.actorForkPlanSource.source] : []),
	];
	const remembered = new Map<string, { key: ActionKey; binding: ExecutionOperationBinding }>();
	const remember = (
		key: ActionKey | undefined,
		tool: AgentTool | undefined,
		bindings: readonly ExecutionOperationBinding[] = [],
	) => {
		if (!key || !tool) return;
		for (const binding of bindings) {
			if (binding.permissionHash !== key.hash || binding.available === false || binding.fed) continue;
			const id = JSON.stringify([key.hash, binding.identity]);
			remembered.delete(id);
			remembered.set(id, { key, binding });
			if (
				remembered.size >
				Math.min(currentSettings.resourceCacheMaxEntries, speculativeConfig().runtime.preparedOperations)
			)
				remembered.delete(remembered.keys().next().value!);
		}
		const commands = new Set([...remembered.values()].map((row) => row.key.key));
		if (commands.size > speculativeConfig().runtime.preparedCommands) {
			const oldest = commands.values().next().value;
			for (const [id, row] of remembered) if (row.key.key === oldest) remembered.delete(id);
		}
	};
	const local: LocalExecutionSession<ToolSettlement> | undefined = options.executionSession
		? undefined
		: new LocalExecutionSession({
				settings: () => ({
					reuseEnabled: currentSettings.reuseEnabled,
					maxConcurrentActions: currentSettings.maxConcurrentActions,
					resourceCacheMaxEntries: currentSettings.resourceCacheMaxEntries,
					resourceCacheMaxBytes: currentSettings.resourceCacheMaxBytes,
				}),
				effect: (key) => actionSemantics.effect(key),
				key: async (call) => calls.get(call)?.binding.action,
				route: async (_call, key, signal, parent) =>
					resolveExecutionRoute(key.tool, signal, key, parent?.checkpoint),
				authorize: async (call, key, route, signal, recheck) => {
					const action = (key.executionContext as ToolInvocation | undefined)?.operation?.permission ?? key;
					return checkPermission(
						calls.get(call)?.tool,
						{ toolName: call.tool, args: action.input, action, route, signal },
						recheck,
					);
				},
				fork: async (call, action, route, callID, signal, resources, parent) => {
					const { tool, turnID } = calls.get(call)!;
					if (!tool) throw new Error("tool_unavailable");
					const scope = { sessionID, turnID, callID };
					const runFork = () =>
						executionGateway.executeSpeculative(
							{ tool: call.tool, callID, input: action.input, signal, action, scope },
							route,
							{
								...resources,
								cwd: options.cwd,
								tool,
								toolName: call.tool,
								args: structuredClone(action.input),
								action,
								callID,
								signal,
								executionScope: scope,
								acceptOperationScope: (s) => s.sessionID === sessionID,
								parentCheckpoint: parent?.checkpoint,
							},
						);
					return service ? service.run(() => measured(call, runFork)) : runFork();
				},
				capture: async (call, action, signal) => {
					const { tool, turnID, binding } = calls.get(call)!;
					if (!tool) return;
					const definition = binding.invocation?.semantics ?? actionSemantics.definition(action ?? call.tool);
					const captured = await executionGateway.captureAuthoritativeResult(
						{
							tool: call.tool,
							callID: call.id,
							action,
							executionContext: binding.invocation,
							effect: definition?.effect ?? "unbounded",
							requirements: definition?.requirements ?? UNRESTRICTED_PROCESS_EFFECTS,
						},
						{ cwd: options.cwd, signal },
						{
							cwd: options.cwd,
							tool,
							toolName: call.tool,
							args: call.input,
							action,
							executionContext: binding.invocation,
							callID: call.id ?? call.tool,
							signal,
							executionScope: { sessionID, turnID },
						},
					);
					return (
						captured && {
							...captured.capture,
							route: captured.route,
							computationDependencies: captured.computationDependencies,
						}
					);
				},
				native: async (call, action, signal, resources, dependencies) => {
					const state = calls.get(call)!;
					const { tool, turnID, binding, executor } = state;
					const scope = { sessionID, turnID, callID: call.id };
					const operation: AgentToolOperation = {
						tool: call.tool,
						input: call.input,
						scope,
						callID: call.id,
						signal,
						...binding,
					};
					const invoke = async () => {
						if (executor) return executor(operation);
						if (!tool) throw new Error("tool_unavailable");
						const callID = call.id ?? call.tool;
						if (binding.invocation?.authoritative)
							return (await binding.invocation.authoritative({ callID, args: call.input, signal })).result;
						return tool.execute(callID, call.input, signal);
					};
					try {
						const result = await executionGateway.executeAuthoritative(
							operation,
							() =>
								executionGateway.observeOperations(
									action,
									scope,
									invoke,
									(bindings, steps) => {
										remember(action, tool, bindings);
										if (steps) dependencies.push(...steps);
									},
									currentSettings.prepareObservedOperations,
									resources.inputs,
									{ cwd: options.cwd, signal, reuseEnabled: currentSettings.reuseEnabled },
								),
							{
								settled: (receipt) => {
									dependencies.push({ computation: receipt.toolExecution, shared: [receipt.toolExecution] });
								},
							},
						);
						return { result, isError: false };
					} catch (error) {
						state.error = { value: error };
						return piToolErrorSettlement(call.tool, error);
					} finally {
						if (
							(binding.invocation?.semantics ?? actionSemantics.definition(action ?? call.tool))?.effect !==
							"observation"
						)
							await TimelineInterval.outside(() =>
								Promise.allSettled(
									executionWorlds.filter(inClosure).map(async (w) => w.actorFallbackSettled?.()),
								),
							);
					}
				},
				reject: (output) => output.isError && output.exitCode === undefined,
				writes: (paths) => service?.invalidate(paths),
				observed: async (call, key, branch) => {
					const state = calls.get(call)!;
					remember(key, state.tool, branch?.operations);
					if (state.tools)
						await prepareOperations(
							state.turnID,
							state.tools,
							actionSemantics.effect(key) === "workspace_mutation",
						);
				},
				rejection: (call, rejection) => calls.get(call)?.rejections.push(rejection),
				event: (call, id, route, status, ms, branch, failure) => {
					const state = calls.get(call)!;
					const pred = state.prediction;
					const operation = (state.binding.action?.executionContext as ToolInvocation | undefined)?.operation;
					const source = operation ? "backend" : (pred?.source ?? "actor_result");
					const descriptor: CandidateEventDescriptor = {
						id,
						route,
						tool: call.tool,
						depth: pred?.depth ?? 0,
						source,
						mode: pred?.mode,
						...(operation ? { kind: "operation" as const } : {}),
						origin: operation
							? "backend"
							: source === "actor_result"
								? "actor_result"
								: source === "actor_preview"
									? "actor_preview"
									: "prediction",
						predictedAction: diagnosticAction(call.tool, call.input, state.binding.action),
						...(branch?.executionMetrics
							? { world: { backend: branch.backend, executionMetrics: branch.executionMetrics } }
							: {}),
					};
					owners.delete(id);
					owners.set(id, descriptor);
					const activePrediction = predictions.get(id);
					if (activePrediction) activePrediction.descriptor = descriptor;
					if (
						owners.size >
						currentSettings.resourceCacheMaxEntries + (currentSettings.maxConcurrentActions ?? 1)
					)
						owners.delete(owners.keys().next().value!);

					event(state.turnID, {
						type: "candidate",
						candidate: descriptor,
						state:
							status === "running"
								? { status }
								: status === "succeeded"
									? { status, executionMs: ms }
									: { status, cause: failure ?? cause("execution", status), executionMs: ms },
					}, status !== "running" && source !== "actor_result" ? state.steps : undefined);
					if (status !== "running")
						void subscriptions.publish("execution_settled", {
							sessionID,
							turnID: state.turnID,
							candidate: descriptor,
							reuseFeedback: pred?.feedback,
							status,
							executionMs: ms,
						});
				},
			});
	const session = options.executionSession ?? local!;
	const disconnect = patternPlans.connect({
		subscribe: (h) => subscriptions.subscribe(h),
		settingsChanged: async () => {},
	});
	const refresh = async () => {
		currentSettings = await resolveSettings();
	};
	const settlePrediction = (
		id: string,
		p: NonNullable<ReturnType<typeof predictions.get>>,
		settlement: PredictionSettlement,
		served: boolean,
	) => {
		predictions.delete(id);
		p.prediction.onFeedback?.(settlement, served);
		const info = { sessionID, turnID: p.turnID, tool: p.prediction.tool, action: p.key, settlement };
		void subscriptions.publish("prediction_settled", info);
		void subscriptions.publish("prediction_feedback", {
			actionID: p.prediction.id,
			feedback: p.prediction.feedback,
			settlement,
			served,
		});
		event(p.turnID, {
			type: "prediction",
			settlement,
			tool: p.key.tool,
			predictedAction: diagnosticAction(p.key.tool, p.key.input, p.key),
			mode: p.prediction.mode,
		});
	};
	const startTurn = async (startInput: AgentStartInput, external?: AbortSignal) => {
		if (closed) return;
		await refresh();
		const { turnID } = startInput;
		const controller = new AbortController();
		const signal = external ? AbortSignal.any([controller.signal, external]) : controller.signal;
		const data = catalog(startInput.tools);
		const sequenceAtStart = ++decision;
		const settings = currentSettings;
		const turn: GeneratorTurn = {
			startInput,
			data,
			signal,
			session,
			settings,
			async issue(prediction, opts = {}) {
				const preview = prediction.source === "actor_preview";
				if (
					signal.aborted ||
					opts.signal?.aborted ||
					(!preview && (!settings.enabled || (settings.tools && !settings.tools.includes(prediction.tool))))
				)
					return;
				const tool = data.tools.get(prediction.tool);
				if (!tool) return;
				const args = prepareToolArguments(tool, prediction.input);
				if (args === undefined) return;
				const binding = await resolveBinding(tool.name, args, data.schemaHashes[tool.name], tool);
				const key = binding.action;
				if (!key || signal.aborted || opts.signal?.aborted) return;
				const call: ExecutionCall = { tool: tool.name, input: asRecord(args) ?? {}, id: prediction.id };
				calls.set(call, { tool, turnID, binding, prediction, rejections: [] });
				const effect = actionSemantics.effect(key) ?? "unbounded";
				const after = effect !== "observation" ? (opts.previousWrite ?? opts.after) : opts.after;
				const run = measured(call, () => session.speculate(call, {
					after,
					signal: preview && opts.signal ? opts.signal : external,
				}));
				if (!preview) {
					const identity = {
						id: run.runID,
						source: prediction.source,
						proposalID: prediction.group,
						actionID: prediction.id,
					};
					const p: NonNullable<ReturnType<typeof predictions.get>> = {
						identity,
						prediction,
						key,
						turnID,
						run,
						after,
						lastDecision: sequenceAtStart + (prediction.latestHorizon ?? prediction.horizon ?? 0),
						descriptor: owners.get(run.runID),
						state: {
							kind: "running",
							settled: run.completion.then((completion) => {
								if (completion.status === "completed")
									p.state = { kind: "completed", producerRunID: completion.producerRunID ?? run.runID };
								else if (predictions.has(run.runID))
									settlePrediction(
										run.runID,
										p,
										{ prediction: identity, observation: "unobserved", cause: cause("admission", completion.reason) },
										false,
									);
							}),
						},
					};
					predictions.set(run.runID, p);
					prediction.onIssued?.();
					void subscriptions.publish("prediction_admitted", {
						feedback: prediction.feedback,
						actionID: prediction.id,
						source: prediction.source,
						issued: true,
					});
					void subscriptions.publish("candidate_materialized", {
						sessionID,
						turnID,
						tool: key.tool,
						predictedAction: key,
						input: prediction.input,
						source: prediction.source,
						proposalID: prediction.group,
						actionID: prediction.id,
						depth: prediction.depth,
						horizon: prediction.horizon,
						targetDecision: sequenceAtStart + (prediction.horizon ?? 0),
						lastDecision: sequenceAtStart + (prediction.latestHorizon ?? prediction.horizon ?? 0),
						adoptionProbability: prediction.adoptionProbability,
						empiricalProbability: prediction.empiricalProbability,
						conditionalProbability: prediction.conditionalProbability,
					});
				}
				return { key, run, effect };
			},
			peers: (source, calls) => {
				if (!signal.aborted)
					for (const generator of generators)
						if (generator.peers)
							void pending.track(Promise.resolve(generator.peers(turn, source, calls))).catch(() => {});
			},
			request(source, kind, signal, produce) {
				let tokens: number | undefined;
				const report = runSourceRequest({
					request: { source, kind, turnID, index: requestIndex++, targetDecisionSequence: sequenceAtStart },
					signal,
					timeoutMs: settings.predictionTimeoutMs,
					produce: (s) =>
						produce(s, (n) => {
							tokens = (tokens ?? 0) + n;
						}),
					count: (n) => n,
				});
				return pending.track(
					report.then(({ value: _, ...record }) => {
						const request = { ...record, ...(tokens !== undefined && { draftTokens: tokens }) };
						tokenTotal += tokens ?? 0;
						event(turnID, { type: "source_request", request, totalDraftTokens: tokenTotal });
						return subscriptions.publish("source_request", { sessionID, turnID, request });
					}),
				);
			},
		};
		turns.set(turnID, turn);
		controllers.set(turn, controller);
		await subscriptions.publish("turn_started", {
			startInput,
			data,
			settings,
			decisionSequence: sequenceAtStart,
			definitions: startInput.tools.map((tool) => ({
				name: tool.name,
				description: tool.description,
				inputSchema: tool.parameters,
			})),
			signal,
		});
		for (const generator of generators)
			void pending.track(Promise.resolve(generator.start(turn))).catch(() => {});
	};
	let workspaceCursor: import("./resource-version.ts").ResourceVersionToken | undefined;
	let maintenance = Promise.resolve();
	const prepareOperations = (turnID: string, tools: readonly AgentTool[], write: boolean) => {
		maintenance = maintenance
			.then(async () => {
				const settings = currentSettings;
				if (
					!local ||
					closed ||
					!settings.enabled ||
					settings.reuseEnabled === false ||
					!settings.prepareObservedOperations ||
					!remembered.size
				)
					return;
				const before = workspaceCursor;
				workspaceCursor = await observeResourceChanges(options.cwd).catch(() => undefined);
				const changed = before?.manager.changesSince(before);
				await before?.release();
				if (
					!write &&
					(!changed ||
						(!changed.uncertain &&
							!changed.paths.some((p) => !path.relative(options.cwd, p).split(path.sep).includes(".git"))))
				)
					return;
				for (const { key, binding } of remembered.values()) {
					const tool = tools.find((t) => t.name === key.tool);
					if (
						!tool ||
						stableValueHash(tool.parameters ?? null) !== key.schemaHash ||
						binding.available === false ||
						binding.fed
					)
						continue;
					void pending
						.track(
							(async () => {
								if ((await binding.stale?.()) !== true || closed) return;
								const permission = (await resolveBinding(tool.name, key.input, key.schemaHash, tool)).action;
								const definition = permission && actionSemantics.definition(permission);
								if (!permission || permission.hash !== binding.permissionHash || !definition) return;
								const epoch = definition.epoch + ":operation:" + binding.backend;
								const op = buildActionKey({
									...permission,
									input: { operation: binding.identity },
									semanticsEpoch: epoch,
									semantics: { ...definition, resourceScope: undefined, ...binding.semantics, epoch },
									executionContext: {
										...(permission.executionContext as ToolInvocation),
										operation: { binding, permission },
									},
								});
								const call: ExecutionCall = { tool: tool.name, input: key.input };
								calls.set(call, { tool, turnID, binding: { action: op }, rejections: [] });
								const run = measured(call, () => local.speculate(call));
								void run.completion.then(() => local.release(run.runID));
							})(),
						)
						.catch(() => {});
				}
			})
			.catch(() => {});
		return maintenance;
	};
	const finishTurn = async (turnID: string, terminal = false) => {
		const turn = turns.get(turnID);
		if (turn) {
			controllers.get(turn)?.abort();
			turns.delete(turnID);
			const actor = lastActor.get(turnID);
			for (const [id, p] of predictions)
				if (terminal || p.lastDecision <= decision) {
					settlePrediction(
						id,
						p,
						actor
							? {
									prediction: p.identity,
									observation: "observed",
									actorAction: actor,
									match: { matched: false },
								}
							: {
									prediction: p.identity,
									observation: "unobserved",
									cause: cause("control", "turn_finished"),
								},
						false,
					);
				}
			lastActor.delete(turnID);
			await subscriptions.publish("turn_finished", { ...turn, terminal });
		}
		await Promise.all(executionWorlds.filter(inClosure).map((w) => w.finishTurn?.(turnID)));
		if (terminal) {
			event(turnID, { type: "task", timing: timeline.measure(performance.now()) });
			timeline = new TaskTimeline(performance.now());
		}
	};
	return {
		sessionID,
		runtime: { subscribe: (h) => subscriptions.subscribe(h), settingsChanged: refresh },
		executionWorldDiagnostics: (refresh = false) =>
			executionGateway.diagnostics({ cwd: options.cwd, refresh }),
		startTurn: (input, signal) => startTurn({ ...input, sessionID }, signal),
		previewActorCall: async (input, signal) => {
			const turn = turns.get(input.turnID);
			if (!turn || currentSettings.reuseEnabled === false || signal?.aborted) return;
			const submit = () =>
				turn
					.issue(
						{
							source: "actor_preview",
							group: "preview",
							id: input.id ?? "preview",
							tool: input.tool,
							input: asRecord(input.args) ?? {},
						},
						{ signal },
					)
					.then((p) => p?.run);
			if (!input.id) return submit();
			let issued = previews.get(turn);
			if (!issued) previews.set(turn, (issued = new Map()));
			const signature = stableValueHash([input.tool, input.args]);
			const prior = issued.get(input.id);
			if (prior?.input === signature) return prior.run;
			if (prior) void prior.run.then((run) => run && session.release(run.runID));
			const run = submit();
			issued.set(input.id, { input: signature, run });
			return run;
		},
		execute: async (input, signal, executor) => {
			const issued = performance.now();
			const finish = timeline.startToolWait(issued);
			try {
				const turnID = input.turnID ?? "";
				const turn = turns.get(turnID);
				const tool = input.tools.find((t) => t.name === input.tool);
				const args = immutableSnapshot(input.args);
				const identity = {
					id: input.id ?? "actor-" + (sequence + 1),
					sequence: ++sequence,
					decisionSequence: decision,
					turnID,
				};
				const call: ExecutionCall = { tool: input.tool, input: asRecord(args) ?? {}, id: identity.id };
				const binding = await resolveBinding(
					input.tool,
					args,
					tool && stableValueHash(tool.parameters ?? null),
					tool,
				);
				const state: CallRecord = { binding, tool, turnID, tools: input.tools, executor, rejections: [] };
				calls.set(call, state);
				const action = binding.action;
				if (action)
					void subscriptions.publish("actor_materialized", {
						sessionID,
						turnID,
						identity,
						action,
						tool: call.tool,
						input: action.input,
					});
				const result = await measured(call, () => session.execute(call, { signal }));
				const durationMs = result.toolExecution.completedAt - result.toolExecution.startedAt;
				const roots = [{ computation: result.toolExecution, reused: result.reusedInterval ?? result.served }];
				const computation = timeline.recordCall(roots, issued, performance.now());
				const candidate = result.producerRunID
					? (owners.get(result.producerRunID) ?? predictions.get(result.producerRunID)?.descriptor)
					: undefined;
				const adopted = TimelineInterval.reusedProducers(roots);
				const matched: PredictionIdentity[] = [];
				const match = {
					kind: result.reuseKind ?? "exact",
					distance: result.reuseKind === "inputs" ? 1 : 0,
				} as const;
				if (result.served)
					await Promise.all(
						[...predictions.values()].map((p) =>
							p.state.kind === "running"
								? Promise.race([p.state.settled, Promise.resolve().then(() => undefined)])
								: undefined,
						),
					);
				for (const [id, p] of predictions) {
					const relation = action && actionKeyMatch(p.key, action);
					if (relation) matched.push(p.identity);
					const credit = () => {
						const producer = p.state.kind === "completed" ? p.state.producerRunID : id;
						const served = (result.served && producer === result.producerRunID) || adopted.has(producer);
						if (!relation && !served) return;
						const fact = { prediction: p.identity, observation: "observed" as const, actorAction: identity };
						const settlement: PredictionSettlement = relation
							? {
									...fact,
									match: {
										matched: true,
										relation,
										adoption: served
											? { status: "adopted", candidateID: result.producerRunID ?? id }
											: {
													status: "rejected",
													cause: state.rejections[0]?.cause ?? cause("matching", "actor_fallback"),
												},
									},
								}
							: { ...fact, match: { matched: false } };
						settlePrediction(id, p, settlement, served);
					};
					if (
						result.served && result.producerRunID && match.kind === "exact" && relation &&
						!p.after && p.state.kind === "running" && id !== result.producerRunID
					) {
						// Reserve this observation while the equivalent root handle confirms its physical producer.
						predictions.delete(id);
						void pending.track(p.state.settled.then(credit));
					} else credit();
				}
				lastActor.set(turnID, identity);

				const settlement: AgentRuntimeEvents["actor_settled"]["settlement"] = {
					actorAction: identity,
					tool: call.tool,
					actionKeyHash: action?.hash,
					matchedPredictions: matched,
					rejections: state.rejections,
					provider: result.served
						? candidate?.origin === "actor_preview" && match.kind === "exact"
							? {
									kind: "actor",
									origin: "preview",
									candidateID: candidate.id,
									durationMs,
									isError: false,
									toolExecution: result.toolExecution,
								}
							: {
									kind: "speculative",
									candidateID: result.producerRunID ?? identity.id,
									match: match as import("./action-semantics.ts").ActionKeyMatch,
									toolExecution: result.toolExecution,
								}
						: {
								kind: "actor",
								origin: "fallback",
								cause:
									state.rejections[0]?.cause ??
									cause("matching", action ? "no_reusable_candidate" : "action_not_keyable"),
								durationMs,
								isError: result.output.isError,
								toolExecution: result.toolExecution,
							},
				};
				const feedback = { sessionID, turnID, action, settlement, computation, candidate };
				event(turnID, {
					type: "actor_action",
					settlement,
					computation,
					candidate,
					actualAction: diagnosticAction(call.tool, call.input, action),
				}, state.steps);
				void subscriptions.publish("actor_settled", feedback);
				if (turn)
					await subscriptions.publish("actor_observed", {
						...feedback,
						...turn,
						consumeInput: { ...input, turnID, sessionID },
						tool: call.tool,
						concrete: call.input,
						output: result.output,
						durationMs,
						order: identity.sequence,
						signal: turn.signal,
					});
				if (state.error) throw state.error.value;
				if (result.output.isError)
					throw ToolExecutionGateway.reusedFailure(
						result.output.result.content.flatMap((i) => (i.type === "text" ? [i.text] : [])).join("\n"),
					);
				return result.output.result;
			} finally {
				finish(performance.now());
			}
		},
		finishTurn,
		drafterBudgetSnapshot: drafterPlans.snapshot,
		dispose: async () => {
			if (closed) return;
			closed = true;
			try {
				for (const id of turns.keys()) await finishTurn(id);
				await session.close();
				await maintenance;
				await workspaceCursor?.release();
				await pending.drain();
				await subscriptions.publish("session_closed", { sessionID, terminal: true });
			} finally {
				disconnect();
				drafterPlans.finish();
				await patternPlans.dispose();
				await executionGateway.dispose();
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
