import { describe, it } from "vitest";
import { qualifyStockTool, STOCK_TOOL_CASES } from "../bench/stock-tool-qualification.ts";

describe("stock Pi filesystem fallback routes", () => {
	it.each(STOCK_TOOL_CASES)("matches stock Pi %s output and effects", async (name, args) => {
		await qualifyStockTool(name, args);
	});
});
