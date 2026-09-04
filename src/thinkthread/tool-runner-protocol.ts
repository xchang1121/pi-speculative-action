import { createHash } from "node:crypto";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ToolSettlement } from "../tool-settlement.ts";

export const THINKTHREAD_TOOL_RUNNER_VERSION = 1 as const;
const THINKTHREAD_TOOL_RUNNER_PREFIX = "PI_SPECULATIVE_ACTION_RESULT_V1:";
export const THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES = 1024 * 1024;

export const THINKTHREAD_TOOL_NAMES = ["read", "grep", "find", "ls", "write", "edit"] as const;
export type ThinkThreadToolName = typeof THINKTHREAD_TOOL_NAMES[number];

export interface ThinkThreadToolRunnerRequestV1 {
	readonly version: typeof THINKTHREAD_TOOL_RUNNER_VERSION;
	readonly tool: ThinkThreadToolName;
	readonly callID: string;
	readonly args: unknown;
	readonly autoResizeImages: boolean;
}

interface ThinkThreadToolRunnerResponseV1 {
	readonly version: typeof THINKTHREAD_TOOL_RUNNER_VERSION;
	readonly settlement: {
		readonly result: {
			readonly content: AgentToolResult<unknown>["content"];
			readonly details: unknown;
		};
		readonly isError: boolean;
	};
}

const TOOL_NAMES = new Set<string>(THINKTHREAD_TOOL_NAMES);

export function encodeThinkThreadToolRunnerRequest(request: ThinkThreadToolRunnerRequestV1): Uint8Array {
	const bytes = new TextEncoder().encode(JSON.stringify(request));
	if (bytes.byteLength > THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES) {
		throw new Error("ThinkThread tool runner request exceeds 1 MiB");
	}
	return bytes;
}

export function decodeThinkThreadToolRunnerRequest(bytes: Uint8Array): ThinkThreadToolRunnerRequestV1 {
	if (bytes.byteLength > THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES) {
		throw new Error("ThinkThread tool runner request exceeds 1 MiB");
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
	} catch {
		throw new Error("ThinkThread tool runner request is not valid UTF-8 JSON");
	}
	const record = asRecord(parsed);
	if (!record || record.version !== THINKTHREAD_TOOL_RUNNER_VERSION) {
		throw new Error("ThinkThread tool runner request version is unsupported");
	}
	if (typeof record.tool !== "string" || !TOOL_NAMES.has(record.tool)) {
		throw new Error("ThinkThread tool runner request tool is unsupported");
	}
	if (typeof record.callID !== "string" || record.callID.length === 0) {
		throw new Error("ThinkThread tool runner request callID is invalid");
	}
	if (typeof record.autoResizeImages !== "boolean") {
		throw new Error("ThinkThread tool runner request autoResizeImages is invalid");
	}
	return {
		version: THINKTHREAD_TOOL_RUNNER_VERSION,
		tool: record.tool as ThinkThreadToolName,
		callID: record.callID,
		args: record.args,
		autoResizeImages: record.autoResizeImages,
	};
}

export function encodeThinkThreadToolRunnerResponse(settlement: ToolSettlement): string {
	const response: ThinkThreadToolRunnerResponseV1 = {
		version: THINKTHREAD_TOOL_RUNNER_VERSION,
		settlement: {
			result: {
				content: settlement.result.content,
				details: settlement.result.details ?? null,
			},
			isError: settlement.isError,
		},
	};
	const payload = Buffer.from(JSON.stringify(response), "utf8");
	return `${THINKTHREAD_TOOL_RUNNER_PREFIX}${payload.byteLength}:${digest(payload)}:${payload.toString("base64")}`;
}

export function decodeThinkThreadToolRunnerResponse(stdout: Uint8Array): ToolSettlement {
	const text = new TextDecoder("utf-8", { fatal: true }).decode(stdout);
	if (!text.startsWith(THINKTHREAD_TOOL_RUNNER_PREFIX)) {
		throw new Error("ThinkThread tool runner response prefix is missing");
	}
	const frame = text.slice(THINKTHREAD_TOOL_RUNNER_PREFIX.length);
	if (!frame || frame.includes("\n") || frame.includes("\r")) {
		throw new Error("ThinkThread tool runner response frame is invalid");
	}
	const match = /^(\d+):([0-9a-f]{64}):([A-Za-z0-9+/]*={0,2})$/.exec(frame);
	if (!match) throw new Error("ThinkThread tool runner response frame is invalid");
	const declaredBytes = Number(match[1]);
	const payload = Buffer.from(match[3]!, "base64");
	if (!Number.isSafeInteger(declaredBytes) || declaredBytes !== payload.byteLength || digest(payload) !== match[2]) {
		throw new Error("ThinkThread tool runner response integrity check failed");
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payload));
	} catch {
		throw new Error("ThinkThread tool runner response payload is invalid");
	}
	const response = asRecord(parsed);
	const settlement = asRecord(response?.settlement);
	const result = asRecord(settlement?.result);
	if (
		response?.version !== THINKTHREAD_TOOL_RUNNER_VERSION ||
		!settlement ||
		typeof settlement.isError !== "boolean" ||
		!result ||
		!Array.isArray(result.content) ||
		!result.content.every(validContent)
	) {
		throw new Error("ThinkThread tool runner response schema is invalid");
	}
	return {
		result: {
			content: result.content as AgentToolResult<unknown>["content"],
			details: result.details === null ? undefined : result.details,
		},
		isError: settlement.isError,
	};
}

function validContent(value: unknown): boolean {
	const content = asRecord(value);
	return !!content && typeof content.type === "string";
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function digest(bytes: Uint8Array): string {
	return createHash("sha256").update(bytes).digest("hex");
}
