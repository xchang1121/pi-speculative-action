import { Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { defineSpeculativeStep, defineSpeculativeTool } from "@earendil-works/pi-speculative-action";

// Share these immutable definitions between tools. Keep configuration in step inputs.
export const rows = defineSpeculativeStep({
	id: "example.dataset.rows", version: "1", dependencies: {},
	run: async ({ fs }, input: { path: string }) => {
		const bytes = await fs.readFile(input.path, 1024 * 1024 + 1);
		if (bytes.length > 1024 * 1024) throw new Error("Example dataset exceeds 1 MiB");
		const values: unknown = JSON.parse(bytes.toString("utf8"));
		if (!Array.isArray(values) || values.some(value => typeof value !== "number" || !Number.isFinite(value)))
			throw new Error("Expected an array of finite numbers");
		return values as number[];
	},
});
export const summary = defineSpeculativeStep({
	id: "example.dataset.summary", version: "1", dependencies: { rows },
	run: async ({ step }, input: { path: string; multiplier: number }) => {
		const values = await step("rows", { path: input.path });
		return { count: values.length, total: values.reduce((sum, value) => sum + value * input.multiplier, 0) };
	},
});
export const report = defineSpeculativeStep({
	id: "example.dataset.report", version: "1", dependencies: { summary },
	run: async ({ step }, input: { path: string; multiplier: number; title: string }) => {
		const value = await step("summary", { path: input.path, multiplier: input.multiplier });
		return { ...value, text: `${input.title}: ${value.count} rows, total ${value.total}` };
	},
});
const fields = { path: Type.String(), multiplier: Type.Number(), title: Type.String() };

export const inspectDataset = defineSpeculativeTool({
	name: "inspect_dataset", label: "Inspect dataset", description: "Summarize a JSON array of numbers in the workspace.",
	parameters: Type.Object(fields), version: "1", effect: "observation", steps: { report },
	resources: input => [input.path],
	execute: async ({ step }, input) => {
		const result = await step("report", input);
		return { content: [{ type: "text", text: result.text }], details: result };
	},
});
export const writeDatasetReport = defineSpeculativeTool({
	name: "write_dataset_report", label: "Write dataset report", description: "Write a dataset summary to a workspace file.",
	parameters: Type.Object({ ...fields, output: Type.String() }), version: "1", effect: "workspace_mutation", steps: { report },
	resources: input => [input.path, input.output],
	execute: async ({ step, fs }, input) => {
		// Output path is not an input to the calculation, so changing it can retain the complete report.
		const result = await step("report", { path: input.path, multiplier: input.multiplier, title: input.title });
		// Effects stay outside reusable steps and use the execution world's filesystem.
		await fs.writeFile(input.output, `${result.text}\n`);
		return { content: [{ type: "text", text: `Wrote ${input.output}: ${result.text}` }], details: result };
	},
});

export default function register(pi: ExtensionAPI) {
	pi.registerTool(inspectDataset);
	pi.registerTool(writeDatasetReport);
}
