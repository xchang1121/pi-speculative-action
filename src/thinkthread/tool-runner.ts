import { pathToFileURL } from "node:url";
import path from "node:path";
import { createPiToolDefinitions } from "../pi-tool-invocation.ts";
import { withPiProjectionCoverage } from "../pi-read-projection.ts";
import { buildPiActionKey } from "../action-semantics.ts";
import { assertNoSymlinkPath } from "../filesystem-evidence.ts";
import { toolErrorSettlement, type ToolSettlement } from "../tool-settlement.ts";
import {
	decodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_RUNNER_MAX_REQUEST_BYTES,
	type ThinkThreadToolRunnerRequestV1,
} from "./tool-runner-protocol.ts";

export async function runThinkThreadTool(
	request: ThinkThreadToolRunnerRequestV1,
	cwd = process.cwd(),
): Promise<ToolSettlement> {
	const action = buildPiActionKey(request.tool, request.args, cwd);
	if (!action) throw new Error("Runner cannot prove the stock tool path identity");
	for (const resource of action.resources) await assertNoSymlinkPath(cwd, path.resolve(cwd, resource));
	const tool = createPiToolDefinitions(cwd, { read: { autoResizeImages: request.autoResizeImages } }).get(request.tool);
	if (!tool) throw new Error(`ThinkThread tool runner does not support ${request.tool}`);
	try {
		const result = withPiProjectionCoverage(
			request.tool,
			request.args,
			await tool.execute(request.callID, request.args as never, undefined, undefined, undefined as never),
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
