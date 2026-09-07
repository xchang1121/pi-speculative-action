import type { ActionProjectionRule } from "./action-key-projection.ts";
import { BASH_TAIL_LINES_ACTION_KEY_PROJECTOR, bashTailLinesView } from "./action-semantics.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

type BashTailLinesCoverage = {
	readonly kind: "tail_lines";
	readonly lines: number;
};

/** Losslessly narrow one completed, untruncated Bash suffix view to a shorter suffix. */
export const PI_BASH_TAIL_LINES_PROJECTION_RULE = {
	...BASH_TAIL_LINES_ACTION_KEY_PROJECTOR,
	captureCoverage: (action, output) => {
		const view = bashTailLinesView(action);
		if (!view || output.isError || output.result.content.length !== 1) return undefined;
		const content = output.result.content[0];
		if (content?.type !== "text" || outputTruncated(output.result.details)) return undefined;
		return { kind: "tail_lines", lines: view.lines } satisfies BashTailLinesCoverage;
	},
	projectOutput: ({ speculative, actor, output, coverage }): ToolSettlement | undefined => {
		const speculativeView = bashTailLinesView(speculative);
		const actorView = bashTailLinesView(actor);
		const realized = tailCoverage(coverage);
		const content = output.result.content[0];
		if (
			output.isError ||
			!speculativeView ||
			!actorView ||
			!realized ||
			realized.lines !== speculativeView.lines ||
			actorView.lines > realized.lines ||
			output.result.content.length !== 1 ||
			content?.type !== "text" ||
			outputTruncated(output.result.details)
		) {
			return undefined;
		}
		return {
			result: {
				...output.result,
				content: [{ type: "text", text: textTail(content.text, actorView.lines) }],
			},
			isError: false,
		};
	},
} satisfies ActionProjectionRule<ToolSettlement>;

function tailCoverage(value: unknown): BashTailLinesCoverage | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const record = value as Record<string, unknown>;
	return record.kind === "tail_lines" &&
		typeof record.lines === "number" &&
		Number.isSafeInteger(record.lines) &&
		record.lines > 0
		? { kind: "tail_lines", lines: record.lines }
		: undefined;
}

function outputTruncated(details: unknown): boolean {
	return !!details && typeof details === "object" && !Array.isArray(details) && "truncation" in details;
}

function textTail(text: string, limit: number): string {
	const terminated = text.endsWith("\n");
	const body = terminated ? text.slice(0, -1) : text;
	const lines = body === "" ? [] : body.split("\n");
	if (lines.length <= limit) return text;
	const selected = lines.slice(-limit).join("\n");
	return terminated ? `${selected}\n` : selected;
}
