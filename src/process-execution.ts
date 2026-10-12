import { AsyncLocalStorage } from "node:async_hooks";
import { errorMessage } from "./error-utils.ts";
import { currentExecutionScope, snapshotExecutionScope, withExecutionScope, type ExecutionScope } from "./execution-world.ts";
import { waitForCompletion } from "./scheduler.ts";
import type { BashOperations } from "@earendil-works/pi-coding-agent";

/** One process launch as observed at the generic tool-execution outlet. */
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

export interface ProcessExecutor { readonly execute: (request: ProcessExecutionRequest) => Promise<ProcessExecutionResult>; }

export type ProcessRouteState = "disabled" | "idle" | "probing" | "ready" | "degraded" | "unavailable";
export interface ProcessRouteSnapshot { readonly state: ProcessRouteState; readonly detail: string; }
export type PreparedProcessExecutionRoute =
	| (ProcessRouteSnapshot & { readonly state: "ready" | "degraded"; readonly executor: ProcessExecutor })
	| (ProcessRouteSnapshot & { readonly state: "unavailable" });
export interface ProcessExecutionRoute {
	readonly enabled: () => boolean;
	readonly prepare: (refresh: boolean, signal: AbortSignal) => Promise<PreparedProcessExecutionRoute>;
	readonly reset?: () => Promise<void>;
}

export type ProcessToolOperations = BashOperations;

interface ActorProcessGeneration {
	readonly controller: AbortController;
	readonly preparation: Promise<PreparedProcessExecutionRoute>;
	readonly executions: Set<Promise<ProcessExecutionResult>>;
	prepared?: PreparedProcessExecutionRoute;
}

/** One process outlet; execution worlds replace only its dynamic async scope. */
export class ProcessExecutionCoordinator implements ProcessExecutor {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor | undefined>();
	private readonly host: ProcessExecutor;
	private readonly actorRoute?: ProcessExecutionRoute;
	private actor?: ActorProcessGeneration | { readonly retirement: Promise<void> };
	private disposed = false;
	readonly operations: ProcessToolOperations;

	constructor(host: ProcessExecutor, actorRoute?: ProcessExecutionRoute) {
		this.host = host;
		this.actorRoute = actorRoute;
		this.operations = Object.freeze({
			exec: (command: string, cwd: string, options: Parameters<ProcessToolOperations["exec"]>[2]) => this.execute({
				command, cwd, environment: options.env ?? process.env, onData: options.onData,
				...(options.signal ? { signal: options.signal } : {}), ...(options.timeout !== undefined ? { timeout: options.timeout } : {}),
			}),
		});
	}

	async execute(request: ProcessExecutionRequest): Promise<ProcessExecutionResult> {
		const scope = snapshotExecutionScope(currentExecutionScope() ?? request.scope);
		if (scope) request = { ...request, scope };
		return this.scope.getStore()?.execute(request) ?? this.executeActor(request);
	}

	actorDiagnostics(): ProcessRouteSnapshot {
		if (this.disposed) return { state: "unavailable", detail: "Process route disposed" };
		if (!this.actorRoute) return { state: "unavailable", detail: "Actor process reuse is not configured" };
		if (!this.actorRoute.enabled()) return { state: "disabled", detail: "Actor process reuse is disabled" };
		if (this.actor && "retirement" in this.actor) return { state: "probing", detail: "Retiring Actor process reuse; new calls use the original executor" };
		return this.actor?.prepared ?? (this.actor
			? { state: "probing", detail: "Checking Actor process reuse" }
			: { state: "idle", detail: "Checked on first process execution" });
	}

	async refreshActorRoute(): Promise<ProcessRouteSnapshot> {
		await this.resetActorRoute();
		if (!this.disposed && this.actorRoute?.enabled()) await this.prepareActorRoute(true)?.preparation;
		return this.actorDiagnostics();
	}

	async dispose(): Promise<void> { this.disposed = true; await this.resetActorRoute(); }

	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		// Overlap Actor readiness with actual production; its generation owns preparation through retirement.
		if (!this.disposed && this.actorRoute?.enabled()) this.prepareActorRoute(true);
		return this.scope.run(executor, operation);
	}

	/** Own the Actor scope before tool binding, reuse, and process preparation can yield. */
	runActor<Value>(scope: ExecutionScope | undefined, operation: () => Promise<Value>): Promise<Value> {
		return withExecutionScope(scope, () => this.scope.run(undefined, operation));
	}

	private async executeActor(request: ProcessExecutionRequest): Promise<ProcessExecutionResult> {
		const generation = this.actorRoute?.enabled() && !this.disposed ? this.prepareActorRoute() : undefined;
		if (!generation) return this.host.execute(request);
		const prepared = await waitForCompletion(generation.preparation, request.signal);
		if (prepared.status !== "completed" || this.disposed || this.actor !== generation || !this.actorRoute?.enabled() || !("executor" in prepared.value)) return this.host.execute(request);
		const executor = prepared.value.executor, execution = Promise.resolve().then(() => executor.execute(request));
		generation.executions.add(execution);
		try { return await execution; } finally { generation.executions.delete(execution); }
	}

	private prepareActorRoute(refresh = false): ActorProcessGeneration | undefined {
		if (!this.actorRoute) throw new Error("Actor process reuse is not configured");
		if (this.actor) return "retirement" in this.actor ? undefined : this.actor;
		const generation: ActorProcessGeneration = {
			controller: new AbortController(),
			executions: new Set(),
			preparation: Promise.resolve().then(() => withExecutionScope(undefined, () => this.scope.exit(() => this.actorRoute!.prepare(refresh, generation.controller.signal))))
			.catch((error): PreparedProcessExecutionRoute => ({
				state: "unavailable",
				detail: errorMessage(error),
			}))
			.then((prepared) => (generation.prepared = prepared)),
		};
		return this.actor = generation;
	}

	private resetActorRoute(): Promise<void> {
		const generation = this.actor;
		if (!generation) return Promise.resolve();
		if ("retirement" in generation) return generation.retirement;
		// Detach admission before yielding; only this generation's already admitted calls may drain.
		const retiring = { retirement: Promise.resolve().then(async () => {
			generation.controller.abort(new Error("Actor process preparation retired"));
			await generation.preparation;
			await Promise.allSettled(generation.executions);
			await this.actorRoute?.reset?.();
		}).finally(() => { if (this.actor === retiring) this.actor = undefined; }) };
		this.actor = retiring;
		return retiring.retirement;
	}
}

export function adaptProcessToolOperations(operations: ProcessToolOperations): ProcessExecutor {
	return { execute: ({ command, cwd, onData, signal, timeout, environment }) =>
		operations.exec(command, cwd, { onData, signal, timeout, env: definedProcessEnvironment(environment) }) };
}

export function definedProcessEnvironment(environment: Readonly<Record<string, string | undefined>>): Record<string, string> {
	return Object.fromEntries(Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined));
}
