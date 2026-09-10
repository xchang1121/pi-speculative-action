import { describe, expect, it, vi } from "vitest";
import * as filesystem from "node:fs/promises";
import { formatThrownValue } from "@earendil-works/pi-ai";
import { toolErrorSettlement } from "../src/tool-settlement.ts";
import { qualifyStockTool, STOCK_TOOL_CASES } from "../bench/stock-tool-qualification.ts";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import {
	decodeThinkThreadToolRunnerRequest, decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest, encodeThinkThreadToolRunnerResponse, THINKTHREAD_TOOL_RUNNER_VERSION,
} from "../src/thinkthread/tool-runner-protocol.ts";

vi.mock("node:fs/promises", { spy: true });

describe("ThinkThread stock Pi tool runner", () => {
	it("round-trips integrity-checked frames and rejects unqualified Pi modules", async () => {
		const request = {
			version: THINKTHREAD_TOOL_RUNNER_VERSION, tool: "read" as const,
			callID: "call-read", args: { path: "notes.txt" }, autoResizeImages: true, modelSupportsImages: false,
		};
		expect(decodeThinkThreadToolRunnerRequest(encodeThinkThreadToolRunnerRequest(request))).toEqual(request);
		for (const invalid of [{ version: 1 }, { autoResizeImages: undefined }, { modelSupportsImages: undefined }, { modelSupportsImages: "false" }]) {
			expect(() => decodeThinkThreadToolRunnerRequest(Buffer.from(JSON.stringify({ ...request, ...invalid })))).toThrow();
		}
		const settlement = {
			result: { content: [{ type: "text" as const, text: "hello" }], details: { source: "test" } }, isError: false,
		};
		const frame = encodeThinkThreadToolRunnerResponse(settlement);
		expect(decodeThinkThreadToolRunnerResponse(Buffer.from(frame))).toEqual(settlement);
		const corrupted = `${frame.slice(0, -1)}${frame.endsWith("A") ? "B" : "A"}`;
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(corrupted))).toThrow("integrity");
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(`${frame}\nnoise`))).toThrow("frame");
		for (const error of [new Error("failed"), new Error(""), Object.assign(new Error(""), { name: "CustomError" }),
			"failed", null, undefined, NaN, 17n, Symbol("error"), { toString: () => "custom" }]) {
			const result = decodeThinkThreadToolRunnerResponse(Buffer.from(encodeThinkThreadToolRunnerResponse(toolErrorSettlement(error))));
			expect(result).toEqual({ result: { content: [{ type: "text", text: formatThrownValue(error) }], details: {} }, isError: true });
		}
		const version = vi.spyOn(filesystem, "readFile").mockResolvedValueOnce('{"version":"0.84.2"}');
		try { await expect(runThinkThreadTool(request)).rejects.toThrow("Requalify the installed Pi tool modules"); }
		finally { version.mockRestore(); }
	});

	it.each(STOCK_TOOL_CASES)("matches stock Pi %s and its applicable fallback (local runner, not Runtime)", async (name, args) => {
		const result = await qualifyStockTool(name, args);
		expect(result.evidence).toBe("Local wire runner only");
	});
});
