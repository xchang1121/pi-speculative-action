import { AsyncLocalStorage } from "node:async_hooks";
import { currentExecutionScope, snapshotExecutionScope, type ExecutionScope } from "./execution-world.ts";
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

export type ProcessRouteState = "disabled" | "idle" | "probing" | "ready" | "unavailable";
export interface ProcessRouteSnapshot { readonly state: ProcessRouteState; readonly detail: string; }

export type ProcessToolOperations = BashOperations;

/** One process outlet; execution worlds replace only its dynamic async scope. */
export class ProcessExecutionCoordinator implements ProcessExecutor {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor | undefined>();
	private readonly host: ProcessExecutor;
	readonly operations: ProcessToolOperations;

	constructor(host: ProcessExecutor) {
		this.host = host;
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
		return (this.scope.getStore() ?? this.host).execute(request);
	}

	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		return this.scope.run(executor, operation);
	}
}

export function adaptProcessToolOperations(operations: ProcessToolOperations): ProcessExecutor {
	return { execute: ({ command, cwd, onData, signal, timeout, environment }) =>
		operations.exec(command, cwd, { onData, signal, timeout, env: definedProcessEnvironment(environment) }) };
}

export function definedProcessEnvironment(environment: Readonly<Record<string, string | undefined>>): Record<string, string> {
	return Object.fromEntries(Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined));
}
