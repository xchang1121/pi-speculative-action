import { pathToFileURL } from "node:url";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import {
	createBashToolDefinition,
	createEditToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
	createWriteToolDefinition,
	type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { withPiProjectionCoverage } from "../pi-read-projection.ts";
import { toolErrorSettlement, type ToolSettlement } from "../tool-settlement.ts";
import {
	decodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES,
	type ThinkThreadToolRunnerRequestV1,
} from "./tool-runner-protocol.ts";

interface RunnableTool {
	readonly execute: (
		callID: string,
		args: never,
		signal?: AbortSignal,
		onUpdate?: undefined,
		context?: ExtensionContext,
	) => Promise<AgentToolResult<unknown>>;
}

export async function runThinkThreadTool(
	request: ThinkThreadToolRunnerRequestV1,
	cwd = process.cwd(),
): Promise<ToolSettlement> {
	const tool = createTool(request, cwd);
	try {
		const result = withPiProjectionCoverage(
			request.tool,
			request.args,
			await tool.execute(request.callID, request.args as never),
		);
		return { result, isError: false };
	} catch (error) {
		return toolErrorSettlement(error);
	}
}

async function main(): Promise<void> {
	try {
		const request = decodeThinkThreadToolRunnerRequest(await readStdin());
		process.stdout.write(encodeThinkThreadToolRunnerResponse(await runThinkThreadTool(request)));
	} catch (error) {
		process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
		process.exitCode = 2;
	}
}

function createTool(request: ThinkThreadToolRunnerRequestV1, cwd: string): RunnableTool {
	switch (request.tool) {
		case "read":
			return createReadToolDefinition(cwd, {
				autoResizeImages: request.autoResizeImages,
			}) as unknown as RunnableTool;
		case "grep":
			return createGrepToolDefinition(cwd) as unknown as RunnableTool;
		case "find":
			return createFindToolDefinition(cwd) as unknown as RunnableTool;
		case "ls":
			return createLsToolDefinition(cwd) as unknown as RunnableTool;
		case "write":
			return createWriteToolDefinition(cwd) as unknown as RunnableTool;
		case "edit":
			return createEditToolDefinition(cwd) as unknown as RunnableTool;
		case "bash":
			return createBashToolDefinition(cwd, {
				...(request.shellPath ? { shellPath: request.shellPath } : {}),
				...(request.shellCommandPrefix ? { commandPrefix: request.shellCommandPrefix } : {}),
				exposeSessionEnvironment: false,
				spawnHook: (context) => ({ ...context, env: { ...process.env } }),
			}) as unknown as RunnableTool;
	}
}

async function readStdin(): Promise<Uint8Array> {
	const chunks: Buffer[] = [];
	let bytes = 0;
	for await (const chunk of process.stdin) {
		const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
		bytes += buffer.byteLength;
		if (bytes > THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES) {
			throw new Error("ThinkThread tool runner stdin exceeds 1 MiB");
		}
		chunks.push(buffer);
	}
	return Buffer.concat(chunks, bytes);
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
	void main();
}
