import { AsyncLocalStorage } from "node:async_hooks";
import type { BashOperations } from "@earendil-works/pi-coding-agent";

/** One process launch as observed at the generic tool-execution outlet. */
export interface ProcessExecutionRequest {
	readonly command: string;
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string | undefined>>;
	readonly timeout?: number;
	readonly signal?: AbortSignal;
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
	readonly prepare: (refresh: boolean) => Promise<PreparedProcessExecutionRoute>;
	readonly reset?: () => Promise<void>;
}

export type ProcessToolOperations = BashOperations;

interface ActorProcessGeneration {
	readonly preparation: Promise<PreparedProcessExecutionRoute>;
	readonly executions: Set<Promise<ProcessExecutionResult>>;
	prepared?: PreparedProcessExecutionRoute;
}

/** One process outlet; execution worlds replace only its dynamic async scope. */
export class ProcessExecutionCoordinator {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor>();
	private readonly host: ProcessExecutor;
	private readonly actorRoute?: ProcessExecutionRoute;
	private actor?: ActorProcessGeneration | { readonly retirement: Promise<void> };
	private disposed = false;
	readonly operations: ProcessToolOperations;

	constructor(host: ProcessExecutor, actorRoute?: ProcessExecutionRoute) {
		this.host = host;
		this.actorRoute = actorRoute;
		this.operations = Object.freeze({
			exec: async (command: string, cwd: string, options: Parameters<ProcessToolOperations["exec"]>[2]) => {
				const request = {
					command,
					cwd,
					environment: options.env ?? process.env,
					onData: options.onData,
					...(options.signal ? { signal: options.signal } : {}),
					...(options.timeout !== undefined ? { timeout: options.timeout } : {}),
				};
				return this.scope.getStore()?.execute(request) ?? this.executeActor(request);
			},
		});
	}

	actorDiagnostics(): ProcessRouteSnapshot {
		if (this.disposed) return { state: "unavailable", detail: "Process route disposed" };
		if (!this.actorRoute) return { state: "unavailable", detail: "Actor process reuse is not configured" };
		if (!this.actorRoute.enabled()) return { state: "disabled", detail: "Actor process reuse is disabled" };
		if (this.actor && "retirement" in this.actor) return { state: "probing", detail: "Retiring Actor process reuse; new calls use the original executor" };
		return this.actor?.prepared ?? (this.actor
			? { state: "probing", detail: "Checking Actor process reuse" }
			: { state: "idle", detail: "Checked on first Bash execution" });
	}

	async refreshActorRoute(): Promise<ProcessRouteSnapshot> {
		await this.resetActorRoute();
		if (!this.disposed && this.actorRoute?.enabled()) await this.prepareActorRoute(true)?.preparation;
		return this.actorDiagnostics();
	}

	async dispose(): Promise<void> {
		this.disposed = true;
		await this.resetActorRoute();
	}

	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		return this.scope.run(executor, operation);
	}

	private async executeActor(request: ProcessExecutionRequest): Promise<ProcessExecutionResult> {
		const generation = this.actorRoute?.enabled() && !this.disposed ? this.prepareActorRoute() : undefined;
		if (!generation) return this.host.execute(request);
		const prepared = await generation.preparation;
		if (this.disposed || this.actor !== generation || !this.actorRoute?.enabled() || !("executor" in prepared)) return this.host.execute(request);
		const execution = Promise.resolve().then(() => prepared.executor.execute(request));
		generation.executions.add(execution);
		try { return await execution; } finally { generation.executions.delete(execution); }
	}

	private prepareActorRoute(refresh = false): ActorProcessGeneration | undefined {
		if (!this.actorRoute) throw new Error("Actor process reuse is not configured");
		if (this.actor) return "retirement" in this.actor ? undefined : this.actor;
		const generation: ActorProcessGeneration = {
			executions: new Set(),
			preparation: Promise.resolve().then(() => this.actorRoute!.prepare(refresh))
			.catch((error): PreparedProcessExecutionRoute => ({
				state: "unavailable",
				detail: error instanceof Error ? error.message : String(error),
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
		const retiring = { retirement: (async () => {
			await generation.preparation;
			await Promise.allSettled(generation.executions);
			await this.actorRoute?.reset?.();
		})().finally(() => { if (this.actor === retiring) this.actor = undefined; }) };
		this.actor = retiring;
		return retiring.retirement;
	}
}

export function adaptProcessToolOperations(operations: ProcessToolOperations): ProcessExecutor {
	return {
		execute: (request) =>
			operations.exec(request.command, request.cwd, {
				onData: request.onData,
				...(request.signal ? { signal: request.signal } : {}),
				...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
				env: definedProcessEnvironment(request.environment),
			}),
	};
}

export function definedProcessEnvironment(environment: Readonly<Record<string, string | undefined>>): Record<string, string> {
	return Object.fromEntries(Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined));
}
