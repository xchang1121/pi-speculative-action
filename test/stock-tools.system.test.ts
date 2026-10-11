import { describe, expect, it } from "vitest";
import { qualifyStockTool, STOCK_TOOL_CASES } from "../bench/stock-tool-qualification.ts";

describe("ThinkThread stock Pi tool runner", () => {
	it.each(STOCK_TOOL_CASES)("matches stock Pi %s and its applicable fallback (local runner, not Runtime)", async (name, args) => {
		const result = await qualifyStockTool(name, args);
		expect(result.evidence).toBe("Local wire runner only");
	});
});
