import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ActionSemanticsDefinition } from "./action-semantics.ts";

/** Host-neutral result consumed by the speculative scheduler. */
export interface ToolSettlement<TDetails = unknown> {
	readonly result: AgentToolResult<TDetails>;
	readonly isError: boolean;
}

export function toolErrorSettlement(error: unknown): ToolSettlement {
	return {
		result: { content: [{ type: "text", text: error instanceof Error ? error.message || error.name : String(error) }], details: {} },
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

export type ToolFilesystemStat = { isDirectory: () => boolean; size?: number; type?: "file" | "directory" | "symlink" | "special"; link?: string; realPath?: string };

/** Filesystem capabilities supplied by an execution world, never ambient host defaults. */
export interface ToolFilesystemOperations {
	readonly readFile: (target: string, maxBytes?: number) => Promise<Buffer>;
	readonly access: (target: string, writable?: boolean) => Promise<void>;
	readonly exists?: (target: string) => boolean | Promise<boolean>;
	/** File size needs its own evidence. `entry` exposes the final entry/link without following it. */
	readonly stat?: (target: string, fields?: "type" | "entry") => ToolFilesystemStat | Promise<ToolFilesystemStat>;
	readonly readdir?: (target: string) => string[] | Promise<string[]>;
	readonly writeFile?: (target: string, content: string) => Promise<void>;
	readonly mkdir?: (target: string) => Promise<void>;
}

/** Versioned identity of the concrete tool executor. */
export interface ToolInvocation {
	readonly executor: string;
	/** Explicit common Actor/speculation profile; never inferred from the tool's name. */
	readonly semantics?: ActionSemanticsDefinition;
	/** Input-invariant executor identity used by K(a); the exact invocation remains in `process`. */
	readonly identity?: unknown;
	readonly process?: ToolProcessInvocation;
	/** Explicit read-only input boundary; omitted operations remain confined to the host workspace. */
	readonly filesystemRoot?: string;
	/** Explicit selected Actor semantics; the host invokes this inside its original execution callback. */
	readonly authoritative?: (request: Parameters<NonNullable<ToolInvocation["filesystem"]>>[1]) => Promise<ToolSettlement>;
	/** Explicit trusted operation binding; never permission to call the supplied host tool. */
	readonly filesystem?: (
		view: ToolFilesystemOperations,
		request: { readonly args: unknown; readonly callID: string; readonly signal: AbortSignal },
	) => Promise<ToolSettlement>;
}
