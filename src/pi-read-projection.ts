import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import {
	DEFAULT_MAX_BYTES,
	DEFAULT_MAX_LINES,
	formatSize,
	type ReadToolDetails,
	type ReadToolInput,
	truncateHead,
} from "@earendil-works/pi-coding-agent";
import {
	type ActionProjectionRule,
	READ_RANGE_ACTION_KEY_PROJECTOR,
	READ_RANGE_COVERAGE_DETAILS_KEY,
	type ReadRangeCoverage,
} from "./action-key-projection.ts";
import { asRecord, readActionRange } from "./action-semantics.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

/** An incomplete Actor stream may start only the existing lossless read-range projection. */
export function canPreviewIncompletePiCall(tool: string, input: Readonly<Record<string, unknown>>): boolean {
	return tool === "read" && typeof input.path === "string";
}

/** Optional Pi text-output fast path. Default hosts reuse sealed inputs without this rule. */
export const PI_READ_RANGE_PROJECTION_RULE = {
	...READ_RANGE_ACTION_KEY_PROJECTOR,
	captureCoverage: (action, output) => {
		if (action.tool !== "read" || output.isError) return undefined;
		const details = output.result.details as { [READ_RANGE_COVERAGE_DETAILS_KEY]?: unknown } | undefined;
		return parseReadCoverage(details?.[READ_RANGE_COVERAGE_DETAILS_KEY]);
	},
	projectOutput: ({ actor, output, coverage }): ToolSettlement | undefined => {
		if (output.isError) return undefined;
		const actorRange = readActionRange(actor);
		const actorUsesDefaultLimit = actor.input.limit === undefined;
		const snapshot = parseReadCoverage(coverage);
		const sourceLines = snapshot ? readCoverageLines(output, snapshot) : undefined;
		if (!actorRange || !snapshot || !sourceLines || actorRange.offset > snapshot.totalLines) return undefined;

		const selectionEndExclusive = actorUsesDefaultLimit
			? snapshot.totalLines + 1
			: Math.min(actorRange.offset + actorRange.limit, snapshot.totalLines + 1);
		if (actorRange.offset < snapshot.startLine || selectionEndExclusive > snapshot.endLineExclusive) {
			return undefined;
		}

		const selectedLines = sourceLines.slice(
			actorRange.offset - snapshot.startLine,
			selectionEndExclusive - snapshot.startLine,
		);
		const selectedContent = selectedLines.join("\n");
		const truncation = truncateHead(selectedContent, {
			maxLines: snapshot.maxLines,
			maxBytes: snapshot.maxBytes,
		});
		if (truncation.firstLineExceedsLimit) return undefined;

		const startLine = actorRange.offset;
		const startLineIndex = startLine - 1;
		let outputText: string;
		if (truncation.truncated) {
			const endLine = startLine + truncation.outputLines - 1;
			const nextOffset = endLine + 1;
			outputText = truncation.content;
			if (truncation.truncatedBy === "lines") {
				outputText += `\n\n[Showing lines ${startLine}-${endLine} of ${snapshot.totalLines}. Use offset=${nextOffset} to continue.]`;
			} else {
				outputText += `\n\n[Showing lines ${startLine}-${endLine} of ${snapshot.totalLines} (${formatSize(snapshot.maxBytes)} limit). Use offset=${nextOffset} to continue.]`;
			}
		} else if (!actorUsesDefaultLimit && startLineIndex + selectedLines.length < snapshot.totalLines) {
			const remaining = snapshot.totalLines - (startLineIndex + selectedLines.length);
			const nextOffset = startLine + selectedLines.length;
			outputText = `${truncation.content}\n\n[${remaining} more lines in file. Use offset=${nextOffset} to continue.]`;
		} else {
			outputText = truncation.content;
		}

		const realizedLineCount = truncation.truncated ? truncation.outputLines : selectedLines.length;
		const projectedCoverage: ReadRangeCoverage = {
			...snapshot,
			startLine,
			endLineExclusive: startLine + realizedLineCount,
			payloadTextLength: truncation.content.length,
		};
		return {
			result: {
				...output.result,
				content: [{ type: "text", text: outputText }],
				details: {
					...(truncation.truncated ? { truncation } : {}),
					[READ_RANGE_COVERAGE_DETAILS_KEY]: projectedCoverage,
				},
			},
			isError: false,
		};
	},
} satisfies ActionProjectionRule<ToolSettlement>;

function parseReadCoverage(value: unknown): ReadRangeCoverage | undefined {
	const record = asRecord(value);
	if (record?.kind !== "text") return undefined;
	const fields = ["startLine", "endLineExclusive", "totalLines", "payloadTextLength", "maxLines", "maxBytes"] as const;
	const snapshot = Object.fromEntries(fields.map((field) => [field, record[field]])) as Record<typeof fields[number], number>;
	if (!Object.values(snapshot).every(Number.isSafeInteger)) return undefined;
	const { startLine, endLineExclusive, totalLines, payloadTextLength, maxLines, maxBytes } = snapshot;
	if (startLine < 1 || totalLines < 1 || payloadTextLength < 0 || maxLines < 1 || maxBytes < 1 ||
		endLineExclusive < startLine || endLineExclusive > totalLines + 1) return undefined;
	return { kind: "text", ...snapshot };
}

function readCoverageLines(output: ToolSettlement, coverage: ReadRangeCoverage): readonly string[] | undefined {
	const content = output.result.content[0];
	if (!content || content.type !== "text" || coverage.payloadTextLength > content.text.length) return undefined;
	const payload = content.text.slice(0, coverage.payloadTextLength);
	const lineCount = coverage.endLineExclusive - coverage.startLine;
	const lines = lineCount === 0 ? [] : payload.split("\n");
	return lines.length === lineCount ? lines : undefined;
}

/** Attach lossless in-memory range evidence to an unmodified Pi read result. */
export function withPiReadCoverage(
	input: ReadToolInput,
	result: AgentToolResult<ReadToolDetails | undefined>,
): AgentToolResult<ReadToolDetails | undefined> {
	const coverage = inferPiReadCoverage(input, result);
	if (!coverage) return result;
	const details = {
		...(result.details ?? {}),
		[READ_RANGE_COVERAGE_DETAILS_KEY]: coverage,
	};
	return { ...result, details };
}

/** Attach Pi-specific realized coverage without changing the underlying tool result. */
export function withPiProjectionCoverage(
	tool: string,
	input: unknown,
	result: AgentToolResult<unknown>,
): AgentToolResult<unknown> {
	if (tool !== "read" || typeof asRecord(input)?.path !== "string") return result;
	return withPiReadCoverage(input as ReadToolInput, result as AgentToolResult<ReadToolDetails | undefined>);
}

function inferPiReadCoverage(
	input: ReadToolInput,
	result: AgentToolResult<ReadToolDetails | undefined>,
): ReadRangeCoverage | undefined {
	if (result.content.length !== 1 || result.content[0]?.type !== "text") return undefined;
	const text = result.content[0].text, startLine = input.offset ?? 1, limit = input.limit;
	if (!Number.isSafeInteger(startLine) || startLine < 1 ||
		(limit !== undefined && (!Number.isSafeInteger(limit) || limit < 0))) return undefined;
	const truncation = result.details?.truncation;
	if (truncation?.firstLineExceedsLimit) return undefined;
	let payload = text, remaining = 0;
	if (truncation?.truncated) {
		payload = truncation.content;
		// Only the suffix outside Pi's structured payload can supply the file's total line count.
		const suffix = text.slice(payload.length);
		const total = Number(/^\n\n\[Showing lines \d+-\d+ of (\d+)/.exec(suffix)?.[1]);
		const end = startLine + truncation.outputLines - 1;
		const byteLimit = truncation.truncatedBy === "bytes" ?  ` (${formatSize(truncation.maxBytes)} limit)` : "";
		if (!text.startsWith(payload) ||
			suffix !== `\n\n[Showing lines ${startLine}-${end} of ${total}${byteLimit}. Use offset=${end + 1} to continue.]`) return undefined;
		remaining = total - end;
	} else if (limit !== undefined) {
		const notice = /\n\n\[(\d+) more lines in file\. Use offset=(\d+) to continue\.\]$/.exec(text);
		const prefix = notice ? text.slice(0, notice.index) : text;
		const prefixLines = prefix === "" && limit === 0 ? 0 : prefix.split("\n").length;
		// A real bounded-read notice follows exactly limit lines. Otherwise it is document text.
		if (notice && prefixLines === limit && Number(notice[1]) > 0 && Number(notice[2]) === startLine + limit) {
			payload = prefix; remaining = Number(notice[1]);
		}
	}
	const lineCount = payload === "" && limit === 0 ? 0 : payload.split("\n").length;
	if (remaining < 0 || (limit !== undefined && lineCount > limit)) return undefined;
	return parseReadCoverage({
		kind: "text", startLine, endLineExclusive: startLine + lineCount,
		totalLines: startLine - 1 + lineCount + remaining, payloadTextLength: payload.length,
		maxLines: truncation?.maxLines ?? DEFAULT_MAX_LINES, maxBytes: truncation?.maxBytes ?? DEFAULT_MAX_BYTES,
	});
}
