import { describe, expect, it } from "vitest";
import { formatThrownValue } from "@earendil-works/pi-ai";
import { toolErrorSettlement } from "../src/tool-settlement.ts";
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
		for (const error of [new Error("failed"), new Error(""), Object.assign(new Error(""), { name: "CustomError" }),
			"failed", null, undefined, NaN, 17n, Symbol("error"), { toString: () => "custom" }]) {
			const result = decodeThinkThreadToolRunnerResponse(Buffer.from(encodeThinkThreadToolRunnerResponse(toolErrorSettlement(error))));
			expect(result).toEqual({ result: { content: [{ type: "text", text: formatThrownValue(error) }], details: {} }, isError: true });
		}
	});

	it.each(STOCK_TOOL_CASES)("matches stock Pi %s and its applicable fallback (local runner, not Runtime)", async (name, args) => {
		const result = await qualifyStockTool(name, args);
		expect(result.evidence).toBe("Local wire runner only");
	});
});
