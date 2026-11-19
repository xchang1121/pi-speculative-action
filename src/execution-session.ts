import type { TimelineInterval } from "./task-timing.ts";

export interface ExecutionCall {
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly id?: string;
}

export interface ExecuteResult<Output> {
	readonly output: Output;
	readonly served: boolean;
	readonly reuseKind?: "exact" | "inputs";
	/** Physical producer of the consumed work. Independent submissions have their own release handles. */
	readonly producerRunID?: string;
	readonly toolExecution: TimelineInterval;
	/** A newly reconstructed view can itself contain reused steps. */
	readonly reusedInterval?: boolean;
}

export type RunCompletion<Output> =
	| ({ readonly status: "completed" } & ExecuteResult<Output>)
	| { readonly status: "cancelled" | "rejected"; readonly reason: string };

export interface SpeculativeRun<Output> {
	readonly runID: string;
	readonly completion: Promise<RunCompletion<Output>>;
}

export interface ExecutionSession<Output> {
	speculate(call: ExecutionCall, options?: { after?: string; signal?: AbortSignal }): SpeculativeRun<Output>;
	execute(call: ExecutionCall, options?: { signal?: AbortSignal }): Promise<ExecuteResult<Output>>;
	release(runID: string): void | Promise<void>;
	flush(): Promise<void>;
	close(): Promise<void>;
}
