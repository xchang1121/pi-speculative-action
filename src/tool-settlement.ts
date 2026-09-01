import type { AgentToolResult } from "@earendil-works/pi-agent-core";

/** Host-neutral result consumed by the speculative scheduler. */
export interface ToolSettlement<TDetails = unknown> {
	readonly result: AgentToolResult<TDetails>;
	readonly isError: boolean;
}

export function toolErrorSettlement(error: unknown): ToolSettlement {
	const message = error instanceof Error ? error.message : String(error);
	return {
		result: { content: [{ type: "text", text: message }], details: {} },
		isError: true,
	};
}

/** Exact process invocation accepted by an optional isolated-process backend. */
export interface ToolProcessInvocation {
	readonly command: string;
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly shell: string;
	readonly shellArgs: readonly string[];
	readonly commandTransport: "argv" | "stdin";
	readonly timeout?: number;
}

/** Versioned identity of the concrete tool executor. */
export interface ToolInvocation {
	readonly executor: string;
	/** Input-invariant executor identity used by K(a); the exact invocation remains in `process`. */
	readonly identity?: unknown;
	readonly process?: ToolProcessInvocation;
}
