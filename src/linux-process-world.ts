import { getShellConfig, type BashOperations } from "@earendil-works/pi-coding-agent";
import { AsyncLocalStorage } from "node:async_hooks";
import path from "node:path";
import type { ActionKey } from "./action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import { projectProcessEnvironment, speculativeConfig } from "./config-schema.ts";
import type { ExecutionOperationBinding, ExecutionScope, ToolInvocation } from "./execution-world.ts";
import {
	currentExecutionScope,
	registerExecutionWorld,
	snapshotExecutionScope,
	toolErrorSettlement,
	UNRESTRICTED_PROCESS_EFFECTS,
} from "./execution-world.ts";
import type { LinuxProcessBackendOptions, LinuxProcessSession } from "./linux-process-backend.ts";
import { LinuxProcessReuseBackend } from "./linux-process-backend.ts";
import { isRecord } from "./primitives.ts";
import type { ProcessExecutionBinding } from "./process-reuse.ts";
import { spawnControlled } from "./process-startup.mjs";
import { controlledSubcomputeChannel, subcomputeEnvironment } from "./subcompute.ts";
import type { WorkspaceSandboxOptions } from "./workspace-sandbox.ts";
import { WorkspaceSandboxService } from "./workspace-sandbox.ts";
/** Public BashOperations outlet with explicit process interception. */
export function createControlledBashOperations(
	options: {
		readonly shellPath?: string;
	} = {},
): BashOperations {
	return {
		exec: async (command, cwd, { onData, signal, timeout, env }) => {
			signal?.throwIfAborted();
			if (
				timeout !== undefined &&
				(!Number.isFinite(timeout) || timeout <= 0 || timeout * 1000 > 0x7fffffff)
			) {
				throw new Error("Invalid timeout");
			}
			const shell = getShellConfig(options.shellPath);
			const sdk = import.meta.resolve("@earendil-works/pi-coding-agent");
			const [shellHelpers, childHelpers] = await Promise.all([
				import(new URL("./utils/shell.js", sdk).href),
				import(new URL("./utils/child-process.js", sdk).href),
			]);
			const fromStdin = shell.commandTransport === "stdin";
			const environment = env ?? shellHelpers.getShellEnv();
			const channel = controlledSubcomputeChannel(environment);
			const transport = channel
				? { PI_SUBCOMPUTE_SOCKET: channel.socket, PI_SUBCOMPUTE_CONTEXT: environment.PI_SUBCOMPUTE_CONTEXT }
				: speculativeConfig().subcomputation.exposeToControlledProcesses
					? await subcomputeEnvironment()
					: {};
			const child = spawnControlled(shell.shell, fromStdin ? shell.args : [...shell.args, command], {
				cwd,
				detached: process.platform !== "win32",
				env: {
					...(speculativeConfig().processReuse.environmentMode === "isolated"
						? projectProcessEnvironment(environment)
						: environment),
					...transport,
				},
				stdio: [fromStdin ? "pipe" : "ignore", "pipe", "pipe"],
				windowsHide: true,
			});
			const completion: Promise<number | null> = childHelpers.waitForChildProcess(child);
			if (fromStdin) {
				child.stdin?.on("error", () => {});
				child.stdin?.end(command);
			}
			child.stdout?.on("data", onData);
			child.stderr?.on("data", onData);
			if (child.pid) shellHelpers.trackDetachedChildPid(child.pid);
			const abort = () => {
				if (child.pid) shellHelpers.killProcessTree(child.pid);
			};
			let timedOut = false;
			const timer =
				timeout === undefined
					? undefined
					: setTimeout(() => {
							timedOut = true;
							abort();
						}, timeout * 1000);
			signal?.addEventListener("abort", abort, { once: true });
			if (signal?.aborted) abort();
			try {
				const exitCode = await completion;
				signal?.throwIfAborted();
				if (timedOut) throw new Error(`timeout:${timeout}`);
				return { exitCode };
			} finally {
				clearTimeout(timer);
				signal?.removeEventListener("abort", abort);
				if (child.pid) shellHelpers.untrackDetachedChildPid(child.pid);
			}
		},
	};
}
export interface ProcessExecutionRequest {
	readonly command: string;
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string | undefined>>;
	readonly timeout?: number;
	readonly signal?: AbortSignal;
	readonly scope?: ExecutionScope;
	readonly onData: (data: Buffer) => void;
}
export type ProcessExecutionResult = Awaited<ReturnType<BashOperations["exec"]>>;
export interface ProcessExecutor {
	readonly execute: (request: ProcessExecutionRequest) => Promise<ProcessExecutionResult>;
}
export type ProcessRouteState = "disabled" | "idle" | "probing" | "ready" | "unavailable";
export interface ProcessRouteSnapshot {
	readonly state: ProcessRouteState;
	readonly detail: string;
}
export type ProcessToolOperations = BashOperations;
/** One process outlet; execution worlds replace only its dynamic async scope. */
export class ProcessExecutionCoordinator implements ProcessExecutor {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor | undefined>();
	private readonly host: ProcessExecutor;
	readonly operations: ProcessToolOperations;
	constructor(host: ProcessExecutor) {
		this.host = host;
		this.operations = Object.freeze({
			exec: (command: string, cwd: string, options: Parameters<ProcessToolOperations["exec"]>[2]) =>
				this.execute({
					command,
					cwd,
					environment: options.env ?? process.env,
					onData: options.onData,
					...(options.signal ? { signal: options.signal } : {}),
					...(options.timeout !== undefined ? { timeout: options.timeout } : {}),
				}),
		});
	}
	async execute(request: ProcessExecutionRequest): Promise<ProcessExecutionResult> {
		if (speculativeConfig().processReuse.environmentMode === "isolated")
			request = {
				...request,
				environment: projectProcessEnvironment(definedProcessEnvironment(request.environment)),
			};
		if (speculativeConfig().subcomputation.exposeToControlledProcesses)
			request = {
				...request,
				environment: {
					...request.environment,
					...(await subcomputeEnvironment()),
				},
			};
		const scope = snapshotExecutionScope(currentExecutionScope() ?? request.scope);
		if (scope) request = { ...request, scope };
		return (this.scope.getStore() ?? this.host).execute(request);
	}
	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		return this.scope.run(executor, operation);
	}
}
export function adaptProcessToolOperations(operations: ProcessToolOperations): ProcessExecutor {
	return {
		execute: ({ command, cwd, onData, signal, timeout, environment }) =>
			operations.exec(command, cwd, {
				onData,
				signal,
				timeout,
				env: definedProcessEnvironment(environment),
			}),
	};
}
export function definedProcessEnvironment(
	environment: Readonly<Record<string, string | undefined>>,
): Record<string, string> {
	return Object.fromEntries(
		Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined),
	);
}
type HostProcessWorldOptions = Pick<
	LinuxProcessExecutionWorldOptions,
	| "coordinator"
	| "backend"
	| "tools"
	| "storeRoot"
	| "store"
	| "sandlockBinary"
	| "heldExecBinary"
	| "deniedPaths"
>;
/** Public Host route for finished, validated branches; it cannot observe or restore Actor processes. */
export function createCompletedProcessExecutionWorld(
	options: HostProcessWorldOptions,
): SpeculativeAgentExecutionWorld {
	return createLinuxProcessExecutionWorld({
		...options,
		driver: "in_memory",
		completedOnly: true,
	});
}
/** Native operations are admitted only through owned bindings and revalidated effects, inputs and handles. */
export function createRestorableProcessExecutionWorld(options: HostProcessWorldOptions) {
	return createLinuxProcessExecutionWorld({
		...options,
		driver: "in_memory",
		completedOnly: false,
	});
}
export interface LinuxProcessExecutionWorldOptions
	extends LinuxProcessBackendOptions, WorkspaceSandboxOptions {
	readonly coordinator: ProcessExecutionCoordinator;
	readonly completedOnly?: boolean;
	/** Tools whose complete process outlet is bound by this host, not all process-shaped effect contracts. */
	readonly tools: readonly string[];
	readonly backend?: LinuxProcessReuseBackend;
	/** Shared explicitly with sibling execution worlds when they belong to one host lifecycle. */
	readonly workspaceSandbox?: WorkspaceSandboxService;
}
/** Generic process world; eligibility follows action semantics and a structured process invocation. */
export function createLinuxProcessExecutionWorld(options: LinuxProcessExecutionWorldOptions) {
	options = Object.freeze({
		...options,
		tools: Object.freeze([...options.tools]),
		deniedPaths: options.deniedPaths && Object.freeze([...options.deniedPaths]),
	});
	const {
		coordinator,
		backend: suppliedBackend,
		workspaceSandbox: suppliedWorkspace,
		...configuration
	} = options;
	const backend = suppliedBackend ?? new LinuxProcessReuseBackend(configuration);
	const workspaceSandbox = suppliedWorkspace ?? new WorkspaceSandboxService();
	const ownsWorkspaceSandbox = suppliedWorkspace === undefined;
	workspaceSandbox.useContentStore(backend.store);
	const { driver, overlayfsBinary, fusermountBinary } = options;
	// Processes observe their inputs' times and inodes: an overlay over the workspace itself shows the Actor's own.
	const workspaceOptions = {
		driver,
		overlayfsBinary,
		fusermountBinary,
		liveLower: true,
	};
	const operations = new WeakMap<
		ExecutionOperationBinding,
		{
			readonly binding: WeakRef<ProcessExecutionBinding>;
			readonly permissionKey: string;
		}
	>();
	const describeOperation = (binding: ProcessExecutionBinding, permission: ActionKey) => {
		const reference = new WeakRef(binding);
		const descriptor = Object.freeze({
			backend: "linux_process_reuse",
			identity: binding.key,
			permissionHash: permission.hash,
			semantics: Object.freeze({
				effect: "unbounded" as const,
				requirements: UNRESTRICTED_PROCESS_EFFECTS,
			}),
			get available() {
				return reference.deref()?.available ?? false;
			},
			stale: () => {
				const current = reference.deref();
				return current ? backend.bindingStale(current) : Promise.resolve(true);
			},
		});
		operations.set(descriptor, {
			binding: reference,
			permissionKey: permission.key,
		});
		return descriptor;
	};
	const operationFor = (action: ActionKey) => {
		const operation = (action.executionContext as ToolInvocation | undefined)?.operation;
		if (!operation) return undefined;
		const issued = operations.get(operation.binding);
		const binding = issued?.binding.deref();
		if (!binding?.available || operation.permission.key !== issued?.permissionKey)
			throw new Error("internal process binding is unavailable");
		return binding;
	};
	const qualifiedDrivers = new Map<string, Awaited<ReturnType<WorkspaceSandboxService["qualify"]>>>();
	let backendChecked = false;
	const qualify = async (sourceRoot: string) => {
		const root = path.resolve(sourceRoot);
		const selected = await workspaceSandbox.qualify(workspaceOptions);
		qualifiedDrivers.set(root, selected);
		return selected;
	};
	return registerExecutionWorld(
		{
			id: options.completedOnly ? "linux_completed_process" : "linux_process_reuse",
			scope: "runtime",
			isolation: "runtime_sandbox",
			storage: backend.storage,
			nativeDiagnostics: (enabled?: boolean) => backend.nativeDiagnostics(enabled),
			actorMetrics: () => backend.actorMetrics(),
			observeOperations: options.completedOnly
				? undefined
				: ({ action, scope, learn, inputs, cwd, signal, enabled, reuseEnabled }, execute, observe) =>
						backend.observeBindings(
							scope,
							() => (enabled && cwd ? backend.observeNative(cwd, scope, signal, execute) : execute()),
							(bindings, computations) => {
								observe(
									action ? bindings.map((binding) => describeOperation(binding, action)) : [],
									computations,
								);
							},
							learn,
							inputs,
							reuseEnabled,
						),
			speculation: {
				tools: options.completedOnly ? options.tools : undefined,
				capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
				acceptsCheckpoint: (checkpoint, cwd) => workspaceSandbox.acceptsCheckpoint(checkpoint, cwd),
				fingerprint: async (request) => {
					backendChecked = true;
					const { action } = request;
					const tool = action?.tool ?? request.tool;
					const invocation = action && processInvocation(action.executionContext);
					const operation = action && operationFor(action);
					const cwd = operation ? backend.bindingWorkspace(operation) : invocation?.cwd;
					if (action && !operation && !invocation)
						throw new Error("execution action has no process invocation");
					if (tool && !operation && !options.tools.includes(tool))
						throw new Error("Complete host process outlet is not bound");
					const [processFingerprint, workspaceFingerprint] = await Promise.all([
						backend.fingerprint(),
						cwd
							? qualify(cwd).then((selected) => selected.fingerprint)
							: workspaceSandbox.fingerprint(workspaceOptions),
					]);
					return `${processFingerprint}:${workspaceFingerprint}`;
				},
				diagnostics: async ({ cwd, refresh }) => {
					if (!refresh && !backendChecked)
						return {
							state: "registered" as const,
							detail: "Checked on first process fork",
						};
					backendChecked = true;
					const [status, store] = await Promise.all([
						backend.check(refresh, cwd),
						backend.store.stats(refresh),
					]);
					const storage = {
						entries: store.certificates,
						maxEntries: backend.store.limits.maxCertificates,
						bytes: store.totalBytes,
						maxBytes: backend.store.limits.maxBytes,
						orphanArtifacts: store.orphanArtifacts,
						overBudget: store.overBudget,
					};
					if (status.state !== "ready")
						return {
							state: "unavailable" as const,
							detail: status.detail,
							storage,
						};
					const selected = qualifiedDrivers.get(path.resolve(cwd));
					return {
						state: "ready" as const,
						detail: selected
							? `${status.detail}; ${selected.driver} workspace driver selected`
							: `${status.detail}; workspace route not prepared yet`,
						storage,
					};
				},
				prepare: async ({ cwd, signal }) => {
					backendChecked = true;
					const status = await backend.check(false, cwd);
					if (status.state !== "ready") throw new Error(status.detail);
					const selected = await qualify(cwd);
					const prepared = await workspaceSandbox.prepare(cwd, {
						...workspaceOptions,
						driver: selected.driver,
						signal,
					});
					qualifiedDrivers.set(path.resolve(cwd), prepared);
				},
				execute: async (context) => {
					const operation = operationFor(context.action);
					const invocation = operation ?? processInvocation(context.action.executionContext);
					if (!invocation) throw new Error("execution action has no process invocation");
					const sourceRoot = path.resolve(context.cwd);
					const selected = (operation && qualifiedDrivers.get(sourceRoot)) || (await qualify(sourceRoot));
					const parent = workspaceSandbox.checkpoint(context.parentCheckpoint, sourceRoot);
					let session: LinuxProcessSession | undefined;
					let launches = 0;
					let exitCode: number | null | undefined;
					try {
						const snapshot = await backend.withProducer(() =>
							workspaceSandbox.capture(
								{
									...workspaceOptions,
									cwd: sourceRoot,
									driver: selected.driver,
									executionMetrics: () => ({ reuse: session?.metrics() }),
									validate: () => session!.validate(),
									afterCapture: () => session!.seal([]),
									execute: async (workspace) => {
										session = await backend.open({
											sourceRoot,
											workspace,
											invocation,
											scope: context.executionScope,
											signal: context.signal,
											onOperationAdopted: context.onOperationAdopted,
											acceptOperationScope: context.acceptOperationScope,
										});
										try {
											if (operation) {
												const result = await session.executeBinding(operation);
												return {
													result: {
														content: [],
														details: { exit: result.exit },
													},
													isError: false,
												};
											}
											const result = await coordinator.runWith(
												{
													execute: async (request) => {
														launches++;
														return ({ exitCode } = await session!.executor.execute(request));
													},
												},
												() => context.tool.execute(context.callID, context.args as never, context.signal),
											);
											if (!launches)
												throw new Error("process-backed tool bypassed the process execution outlet");
											return { result, isError: false };
										} catch (error) {
											if (launches !== 1 || typeof exitCode !== "number" || exitCode === 0) throw error;
											return { ...toolErrorSettlement(error), exitCode };
										} finally {
											await session.close();
										}
									},
								},
								parent,
							),
						);
						const branch = workspaceSandbox.branch(
							{
								cwd: sourceRoot,
								action: context.action,
								parentCheckpoint: context.parentCheckpoint,
								validate: () => session!.validate(),
							},
							snapshot,
						);
						const commit = branch.commit.bind(branch);
						const dispose = branch.dispose?.bind(branch);
						return Object.assign(branch, {
							computationDependencies: session!.computationDependencies(),
							operations: Object.freeze(
								options.completedOnly
									? []
									: session!.executionBindings().map((binding) => describeOperation(binding, context.action)),
							),
							commit: () => {
								if (operation) throw new Error("internal process output cannot commit an enclosing tool");
								return session!.ownership.commit(commit);
							},
							dispose: async () => {
								try {
									await dispose?.();
								} finally {
									await session!.dispose?.();
								}
							},
						});
					} catch (error) {
						await session?.dispose?.();
						throw error;
					}
				},
			},
			dispose: async () => {
				qualifiedDrivers.clear();
				try {
					await backend.dispose();
				} finally {
					if (ownsWorkspaceSandbox) await workspaceSandbox.dispose();
				}
			},
		} satisfies SpeculativeAgentExecutionWorld & {
			nativeDiagnostics: (enabled?: boolean) => ReturnType<LinuxProcessReuseBackend["nativeDiagnostics"]>;
			actorMetrics: () => ReturnType<LinuxProcessReuseBackend["actorMetrics"]>;
		},
		{
			operations: options.completedOnly ? ["speculation"] : ["speculation", "observeOperations"],
			verify: async ({ cwd, refresh }) => {
				backendChecked = true;
				const status = await backend.check(refresh, cwd);
				if (status.state !== "ready") throw new Error(status.detail);
				return {
					fingerprint: status.fingerprint!,
					externalEffects: "excluded",
				};
			},
		},
	);
}
function processInvocation(value: unknown): ToolInvocation["process"] | undefined {
	if (!isRecord(value)) return undefined;
	const processValue = value.process as ToolInvocation["process"];
	if (
		!processValue ||
		typeof processValue.command !== "string" ||
		typeof processValue.cwd !== "string" ||
		typeof processValue.shell !== "string" ||
		!Array.isArray(processValue.shellArgs) ||
		(processValue.commandTransport !== "argv" && processValue.commandTransport !== "stdin")
	) {
		return undefined;
	}
	return processValue;
}
