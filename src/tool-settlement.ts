import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { formatThrownValue } from "@earendil-works/pi-ai";

/** Host-neutral result consumed by the speculative scheduler. */
export interface ToolSettlement<TDetails = unknown> {
	readonly result: AgentToolResult<TDetails>;
	readonly isError: boolean;
}

export function toolErrorSettlement(error: unknown): ToolSettlement {
	return {
		result: { content: [{ type: "text", text: formatThrownValue(error) }], details: {} },
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

/** Filesystem capabilities supplied by an execution world, never ambient host defaults. */
export interface ToolFilesystemOperations {
	readonly readFile: (target: string, maxBytes?: number) => Promise<Buffer>;
	readonly access: (target: string, writable?: boolean) => Promise<void>;
	readonly exists?: (target: string) => boolean | Promise<boolean>;
	readonly stat?: (target: string) => { isDirectory: () => boolean; size?: number } | Promise<{ isDirectory: () => boolean; size?: number }>;
	readonly readdir?: (target: string) => string[] | Promise<string[]>;
	readonly writeFile?: (target: string, content: string) => Promise<void>;
	readonly mkdir?: (target: string) => Promise<void>;
}

/** Versioned identity of the concrete tool executor. */
export interface ToolInvocation {
	readonly executor: string;
	/** Input-invariant executor identity used by K(a); the exact invocation remains in `process`. */
	readonly identity?: unknown;
	readonly process?: ToolProcessInvocation;
	/** Explicit trusted operation binding; never permission to call the supplied host tool. */
	readonly filesystem?: (
		view: ToolFilesystemOperations,
		request: { readonly args: unknown; readonly callID: string; readonly signal: AbortSignal },
	) => Promise<ToolSettlement>;
}
