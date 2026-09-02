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

export interface ProcessExecutor {
	readonly execute: (request: ProcessExecutionRequest) => Promise<ProcessExecutionResult>;
}

/** Compatibility name for Pi's public process-backed tool outlet. */
export type ProcessToolOperations = BashOperations;

/**
 * One outlet for every process-backed tool.
 *
 * Tool definitions keep ownership of argument validation, streaming, truncation, and result
 * formatting. Execution worlds only replace the process launch for the dynamic async scope.
 */
export class ProcessExecutionCoordinator {
	private readonly scope = new AsyncLocalStorage<ProcessExecutor>();
	private readonly host: ProcessExecutor;
	readonly operations: ProcessToolOperations;

	constructor(host: ProcessExecutor) {
		this.host = host;
		this.operations = Object.freeze({
			exec: (command: string, cwd: string, options: Parameters<ProcessToolOperations["exec"]>[2]) =>
				(this.scope.getStore() ?? this.host).execute({
					command,
					cwd,
					environment: options.env ?? process.env,
					onData: options.onData,
					...(options.signal ? { signal: options.signal } : {}),
					...(options.timeout !== undefined ? { timeout: options.timeout } : {}),
				}),
		});
	}

	/** Bind an executor to exactly one tool execution and every async child it creates. */
	runWith<Value>(executor: ProcessExecutor, operation: () => Promise<Value>): Promise<Value> {
		return this.scope.run(executor, operation);
	}
}

export function adaptProcessToolOperations(operations: ProcessToolOperations): ProcessExecutor {
	return {
		execute: (request) =>
			operations.exec(request.command, request.cwd, {
				onData: request.onData,
				...(request.signal ? { signal: request.signal } : {}),
				...(request.timeout !== undefined ? { timeout: request.timeout } : {}),
				env: Object.fromEntries(
					Object.entries(request.environment).filter((entry): entry is [string, string] => entry[1] !== undefined),
				),
			}),
	};
}
