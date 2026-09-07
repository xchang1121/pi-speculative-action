import { describe, expect, it } from "vitest";
import { qualifyStockTool, STOCK_TOOL_CASES } from "../bench/stock-tool-qualification.ts";
import {
	decodeThinkThreadToolRunnerRequest, decodeThinkThreadToolRunnerResponse,
	encodeThinkThreadToolRunnerRequest, encodeThinkThreadToolRunnerResponse, THINKTHREAD_TOOL_RUNNER_VERSION,
} from "../src/thinkthread/tool-runner-protocol.ts";

describe("ThinkThread stock Pi tool runner", () => {
	it("round-trips an integrity-checked request and response frame", () => {
		const request = {
			version: THINKTHREAD_TOOL_RUNNER_VERSION, tool: "read" as const,
			callID: "call-read", args: { path: "notes.txt" }, autoResizeImages: true,
		};
		expect(decodeThinkThreadToolRunnerRequest(encodeThinkThreadToolRunnerRequest(request))).toEqual(request);
		const settlement = {
			result: { content: [{ type: "text" as const, text: "hello" }], details: { source: "test" } }, isError: false,
		};
		const frame = encodeThinkThreadToolRunnerResponse(settlement);
		expect(decodeThinkThreadToolRunnerResponse(Buffer.from(frame))).toEqual(settlement);
		const corrupted = `${frame.slice(0, -1)}${frame.endsWith("A") ? "B" : "A"}`;
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(corrupted))).toThrow("integrity");
		expect(() => decodeThinkThreadToolRunnerResponse(Buffer.from(`${frame}\nnoise`))).toThrow("frame");
	});

	it.each(STOCK_TOOL_CASES)("matches stock Pi %s and its applicable fallback (local runner, not Runtime)", async (name, args) => {
		const result = await qualifyStockTool(name, args);
		expect(result.evidence).toBe("Local wire runner only");
	});
});
