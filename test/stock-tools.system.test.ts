import { describe, it } from "vitest";
import { qualifyStockTool, STOCK_TOOL_CASES } from "../bench/stock-tool-qualification.ts";

describe("stock Pi default extension routes", () => {
	it.each(STOCK_TOOL_CASES)("matches stock Pi %s output and effects", async (name, args) => {
		await qualifyStockTool(name, args);
	});
	it.each(STOCK_TOOL_CASES.filter(([name]) => name === "grep" || name === "find"))("honors an explicit native %s route", async (name, args) => {
		await qualifyStockTool(name, args, "native");
	});
});
