import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import {
	decodeThinkThreadToolRunnerRequest,
	decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest,
	encodeThinkThreadToolRunnerResponse,
	THINKTHREAD_TOOL_RUNNER_VERSION,
	type ThinkThreadToolName,
} from "../src/thinkthread/tool-runner-protocol.ts";

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

	it("executes all seven stock tools with their normal result semantics", async () => {
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-thinkthread-runner-"));
		roots.push(root);
		await writeFile(path.join(root, "notes.txt"), "alpha\nbeta\n", "utf8");

		const read = await runThinkThreadTool(runnerRequest("read", { path: "notes.txt" }), root);
		expect(text(read)).toContain("alpha");

		const grep = await runThinkThreadTool(runnerRequest("grep", { pattern: "beta", path: "." }), root);
		expect(text(grep)).toContain("notes.txt");

		const find = await runThinkThreadTool(runnerRequest("find", { pattern: "*.txt", path: "." }), root);
		expect(text(find)).toContain("notes.txt");

		const ls = await runThinkThreadTool(runnerRequest("ls", { path: "." }), root);
		expect(text(ls)).toContain("notes.txt");

		const write = await runThinkThreadTool(
			runnerRequest("write", { path: "generated.txt", content: "generated\n" }),
			root,
		);
		expect(write.isError).toBe(false);
		expect(await readFile(path.join(root, "generated.txt"), "utf8")).toBe("generated\n");

		const edit = await runThinkThreadTool(
			runnerRequest("edit", {
				path: "notes.txt",
				edits: [{ oldText: "beta", newText: "gamma" }],
			}),
			root,
		);
		expect(edit.isError).toBe(false);
		expect(await readFile(path.join(root, "notes.txt"), "utf8")).toContain("gamma");

		const bash = await runThinkThreadTool(
			runnerRequest("bash", { command: "printf bash-result; printf side-effect > bash.txt" }),
			root,
		);
		expect(text(bash)).toContain("bash-result");
		expect(await readFile(path.join(root, "bash.txt"), "utf8")).toBe("side-effect");
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

function text(settlement: Awaited<ReturnType<typeof runThinkThreadTool>>): string {
	return settlement.result.content
		.filter(
			(item): item is Extract<(typeof settlement.result.content)[number], { type: "text" }> => item.type === "text",
		)
		.map((item) => item.text)
		.join("\n");
}
