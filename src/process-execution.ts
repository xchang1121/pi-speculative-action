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
	readonly prepare: () => Promise<PreparedProcessExecutionRoute>;
	readonly reset?: () => Promise<void>;
}

export type ProcessToolOperations = BashOperations;

/** One process outlet; execution worlds replace only its dynamic async scope. */
export class ProcessExecutionCoordinator {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor>();
	private readonly host: ProcessExecutor;
	private readonly actorRoute?: ProcessExecutionRoute;
	private actorPreparation?: Promise<PreparedProcessExecutionRoute>;
	private preparedActor?: PreparedProcessExecutionRoute;
	private disposed = false;
	readonly operations: ProcessToolOperations;

	constructor(host: ProcessExecutor, actorRoute?: ProcessExecutionRoute) {
		this.host = host;
		this.actorRoute = actorRoute;
		this.operations = Object.freeze({
			exec: async (command: string, cwd: string, options: Parameters<ProcessToolOperations["exec"]>[2]) =>
				(this.scope.getStore() ?? (await this.actorExecutor())).execute({
					command,
					cwd,
					environment: options.env ?? process.env,
					onData: options.onData,
					...(options.signal ? { signal: options.signal } : {}),
					...(options.timeout !== undefined ? { timeout: options.timeout } : {}),
				}),
		});
	}

	actorDiagnostics(): ProcessRouteSnapshot {
		if (this.disposed) return { state: "unavailable", detail: "Process route disposed" };
		if (!this.actorRoute) return { state: "unavailable", detail: "Actor process reuse is not configured" };
		if (!this.actorRoute.enabled()) return { state: "disabled", detail: "Speculative Bash is disabled" };
		return this.preparedActor ?? (this.actorPreparation
			? { state: "probing", detail: "Checking Actor process reuse" }
			: { state: "idle", detail: "Checked on first Bash execution" });
	}

	async refreshActorRoute(): Promise<ProcessRouteSnapshot> {
		await this.resetActorRoute();
		if (this.actorRoute?.enabled()) await this.prepareActorRoute();
		return this.actorDiagnostics();
	}

	async dispose(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		await this.resetActorRoute();
	}

	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		return this.scope.run(executor, operation);
	}

	private async actorExecutor(): Promise<ProcessExecutor> {
		if (!this.actorRoute?.enabled() || this.disposed) return this.host;
		const prepared = await this.prepareActorRoute();
		return this.actorRoute.enabled() && "executor" in prepared ? prepared.executor : this.host;
	}

	private prepareActorRoute(): Promise<PreparedProcessExecutionRoute> {
		if (!this.actorRoute) throw new Error("Actor process reuse is not configured");
		this.actorPreparation ??= this.actorRoute.prepare()
			.catch((error): PreparedProcessExecutionRoute => ({
				state: "unavailable",
				detail: error instanceof Error ? error.message : String(error),
			}))
			.then((prepared) => (this.preparedActor = prepared));
		return this.actorPreparation;
	}

	private async resetActorRoute(): Promise<void> {
		await this.actorPreparation;
		this.actorPreparation = this.preparedActor = undefined;
		await this.actorRoute?.reset?.();
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
