import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
	createEditToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
	createWriteToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { withPiProjectionCoverage } from "../src/pi-read-projection.ts";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import {
	decodeThinkThreadToolRunnerRequest,
	decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_RUNNER_VERSION,
	type ThinkThreadToolRunnerRequestV1,
	type ThinkThreadToolName,
} from "../src/thinkthread/tool-runner-protocol.ts";
import { toolErrorSettlement } from "../src/tool-settlement.ts";

const roots: string[] = [];

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("ThinkThread stock Pi tool runner", () => {
	it("round-trips an integrity-checked request and response frame", () => {
		const request = runnerRequest("read", { path: "notes.txt" });
		expect(decodeThinkThreadToolRunnerRequest(encodeThinkThreadToolRunnerRequest(request))).toEqual(request);

		const settlement = {
			result: { content: [{ type: "text" as const, text: "hello" }], details: { source: "test" } },
			isError: false,
		};
		const frame = encodeThinkThreadToolRunnerResponse(settlement);
		expect(decodeThinkThreadToolRunnerResponse(Buffer.from(frame))).toEqual(settlement);
		const corrupted = `${frame.slice(0, -1)}${frame.endsWith("A") ? "B" : "A"}`;
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(corrupted))).toThrow("integrity");
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(`${frame}\nnoise`))).toThrow("frame");
	});

	it.each([
		["read", { path: "notes.txt" }],
		["grep", { pattern: "beta", path: "." }],
		["find", { pattern: "*.txt", path: "." }],
		["ls", { path: "." }],
		["write", { path: "generated.txt", content: "generated\n" }],
		["edit", { path: "notes.txt", edits: [{ oldText: "beta", newText: "gamma" }] }],
	] as const)("matches the native Pi %s result and workspace effects", async (tool, args) => {
		const [nativeRoot, thinkThreadRoot] = await Promise.all([
			mkdtemp(path.join(os.tmpdir(), "pi-native-tool-")),
			mkdtemp(path.join(os.tmpdir(), "pi-thinkthread-tool-")),
		]);
		roots.push(nativeRoot, thinkThreadRoot);
		await Promise.all([seed(nativeRoot), seed(thinkThreadRoot)]);
		const request = runnerRequest(tool, args);

		const [native, isolated] = await Promise.all([
			runNativeTool(request, nativeRoot),
			runThinkThreadTool(request, thinkThreadRoot),
		]);
		expect(normalizeWorkspace(isolated, thinkThreadRoot)).toEqual(normalizeWorkspace(native, nativeRoot));
		expect(await workspaceState(thinkThreadRoot)).toEqual(await workspaceState(nativeRoot));
	});
});

function runnerRequest(tool: ThinkThreadToolName, args: unknown) {
	return {
		version: THINKTHREAD_TOOL_RUNNER_VERSION,
		tool,
		callID: `call-${tool}`,
		args,
		autoResizeImages: true,
	};
}

async function runNativeTool(request: ThinkThreadToolRunnerRequestV1, cwd: string) {
	const tools = [
		createReadToolDefinition(cwd, { autoResizeImages: request.autoResizeImages }),
		createGrepToolDefinition(cwd), createFindToolDefinition(cwd), createLsToolDefinition(cwd),
		createWriteToolDefinition(cwd), createEditToolDefinition(cwd),
	];
	const tool = tools.find((candidate) => candidate.name === request.tool)!;
	try {
		const result = withPiProjectionCoverage(request.tool, request.args,
			await tool.execute(request.callID, request.args as never, undefined, undefined, undefined as never));
		return { result, isError: false };
	} catch (error) {
		return toolErrorSettlement(error);
	}
}

async function seed(root: string): Promise<void> {
	await mkdir(path.join(root, "nested"));
	await Promise.all([
		writeFile(path.join(root, "notes.txt"), "alpha\nbeta\n", "utf8"),
		writeFile(path.join(root, "nested", "todo.txt"), "beta\n", "utf8"),
	]);
}

async function workspaceState(root: string): Promise<readonly (string | null)[]> {
	return Promise.all(["notes.txt", "nested/todo.txt", "generated.txt"].map(async (file) => {
		try { return await readFile(path.join(root, file), "utf8"); }
		catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
	}));
}

function normalizeWorkspace(value: unknown, root: string): unknown {
	if (typeof value === "string") return value.split(root).join("<workspace>").split(root.replaceAll("\\", "/")).join("<workspace>");
	if (Array.isArray(value)) return value.map((item) => normalizeWorkspace(item, root));
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, normalizeWorkspace(child, root)]));
}
