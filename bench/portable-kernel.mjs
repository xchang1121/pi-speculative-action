import { STOCK_TOOL_CASES, qualifyStockTool } from "./stock-tool-qualification.ts";

// The same public Pi session and Host outlet as the shipped extension.
const report = [];
for (const [name, input] of STOCK_TOOL_CASES) {
	report.push({ name, result: await qualifyStockTool(name, input) });
}
console.log(JSON.stringify({ platform: process.platform, node: process.version, report }, null, 2));
