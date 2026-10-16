import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { TSchema } from "@earendil-works/pi-ai";
import type { ActionKey, ActionSemanticsDefinition } from "./action-semantics.ts";
import type { ExecutionOperationBinding, WorldResultCapture } from "./execution-world.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import { immutableSnapshot } from "./stable-json.ts";

/** Host-neutral result consumed by the speculative scheduler. */
export interface ToolSettlement<TDetails = unknown> {
	readonly result: AgentToolResult<TDetails>;
	readonly isError: boolean;
	/** A process that ran to completion and exited non-zero failed on its own terms: once validated, the Actor may adopt it. */
	readonly exitCode?: number;
}

export interface ToolObservationContract {
	readonly version: string;
	/** Model-visible content schema; defaults to text/image blocks. */
	readonly content?: TSchema;
	/** Pure, binding-owned normalization; changing these rules requires a new version. */
	readonly canonicalize?: (content: ToolSettlement["result"]["content"]) => ToolSettlement["result"]["content"];
}
export const EXACT_TOOL_OBSERVATION: ToolObservationContract = Object.freeze({ version: "model-visible/1" });
export const toolObservationContract = (action?: ActionKey): ToolObservationContract =>
	(action?.executionContext as ToolInvocation | undefined)?.observation ?? EXACT_TOOL_OBSERVATION;
/** Normalization affects prediction ancestry only; it never changes the Actor's output or execution proofs. */
export function toolObservationKey(output: ToolSettlement, action?: ActionKey): string | undefined {
	try { return canonicalToolObservation(output, action).key; } catch { return undefined; }
}
export function canonicalToolObservation(output: ToolSettlement, action?: ActionKey) {
	const contract = toolObservationContract(action), original = output.result.content;
	const content = contract.canonicalize ? immutableSnapshot(contract.canonicalize(immutableSnapshot(original))) : original;
	return { content, key: stableValueHash([contract.version, contract.content, content, output.isError, output.exitCode]) };
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
	/** `entry` does not follow the final link; includeSize=false returns only type/path fields and their evidence. */
	readonly stat?: (target: string, fields?: "type" | "entry", includeSize?: boolean) => ToolFilesystemStat | Promise<ToolFilesystemStat>;
	readonly readdir?: (target: string) => string[] | Promise<string[]>;
	readonly writeFile?: (target: string, content: string) => Promise<void>;
	readonly mkdir?: (target: string) => Promise<void>;
	/** Build is pure and reads only view inputs; binding/key cover other inputs and values stay immutable. Resource is a hint; signal owns this consumer. */
	readonly prepare?: <Value, Result>(binding: object, key: string,
		build: (view: ToolFilesystemOperations, signal: AbortSignal) => Promise<{ readonly value: Value; readonly bytes: number; readonly dispose: () => void | Promise<void> }>,
		consume: (value: Value) => Promise<Result>, resource?: string, signal?: AbortSignal) => Promise<Result>;
}

/** Identity of the concrete tool executor. */
export interface ToolInvocation {
	readonly executor: string;
	readonly observation?: ToolObservationContract;
	/** Internal execution uses the current enclosing action for permission and an opaque backend binding. */
	readonly operation?: { readonly binding: ExecutionOperationBinding; readonly permission: ActionKey };
	/** Explicit common Actor/speculation profile; never inferred from the tool's name. */
	readonly semantics?: ActionSemanticsDefinition;
	/** Input-invariant executor identity used by K(a); the exact invocation remains in `process`. */
	readonly identity?: unknown;
	readonly process?: ToolProcessInvocation;
	/** Explicit read-only input boundary; omitted operations remain confined to the host workspace. */
	readonly filesystemRoot?: string;
	/** Explicit selected Actor semantics; the host invokes this inside its original execution callback. */
	readonly authoritative?: (request: Parameters<NonNullable<ToolInvocation["filesystem"]>>[1]) => Promise<ToolSettlement>;
	/** Arm bounded input and pure-computation retention in the selected Actor executor, without replaying effects. */
	readonly captureInputs?: (action: ActionKey | undefined, maxBytes: number, callID: string) => WorldResultCapture<ToolSettlement>;
	/** Explicit trusted operation binding; never permission to call the supplied host tool. */
	readonly filesystem?: (
		view: ToolFilesystemOperations,
		request: { readonly args: unknown; readonly callID: string; readonly signal: AbortSignal },
	) => Promise<ToolSettlement>;
}
