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
import { buildActionKey, READ_DEFAULT_LIMIT, readActionRange } from "./action-semantics.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

/** An incomplete Actor stream may start only the existing lossless read-range projection. */
export function canPreviewIncompletePiCall(tool: string, input: Readonly<Record<string, unknown>>): boolean {
	return tool === "read" && typeof input.path === "string";
}

/** Production Pi read projection. Other tools remain exact-only. */
export const PI_READ_RANGE_PROJECTION_RULE = {
	...READ_RANGE_ACTION_KEY_PROJECTOR,
	coveringAction: (action) => {
		const range = readActionRange(action);
		if (!range || range.limit === 0 || range.limit >= READ_DEFAULT_LIMIT) return undefined;
		return buildActionKey({
			tool: action.tool,
			resources: action.resources,
			input: {
				...action.input,
				offset: Math.max(1, range.end - READ_DEFAULT_LIMIT + 1),
				limit: READ_DEFAULT_LIMIT,
			},
			schemaHash: action.schemaHash,
			semanticsEpoch: action.semanticsEpoch,
			executionFingerprint: action.executionFingerprint,
			...(action.executionContext !== undefined ? { executionContext: action.executionContext } : {}),
		});
	},
	captureCoverage: (action, output) => {
		if (action.tool !== "read" || output.isError) return undefined;
		return parseReadCoverage(readCoverageValue(output.result.details));
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
			kind: "text",
			startLine,
			endLineExclusive: startLine + realizedLineCount,
			totalLines: snapshot.totalLines,
			payloadTextLength: truncation.content.length,
			maxLines: snapshot.maxLines,
			maxBytes: snapshot.maxBytes,
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
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const record = value as Record<string, unknown>;
	if (record.kind !== "text") return undefined;
	const startLine = finiteInteger(record.startLine);
	const endLineExclusive = finiteInteger(record.endLineExclusive);
	const totalLines = finiteInteger(record.totalLines);
	const payloadTextLength = finiteInteger(record.payloadTextLength);
	const maxLines = finiteInteger(record.maxLines);
	const maxBytes = finiteInteger(record.maxBytes);
	if (
		startLine === undefined ||
		endLineExclusive === undefined ||
		totalLines === undefined ||
		payloadTextLength === undefined ||
		maxLines === undefined ||
		maxBytes === undefined ||
		startLine < 1 ||
		totalLines < 1 ||
		payloadTextLength < 0 ||
		maxLines < 1 ||
		maxBytes < 1 ||
		endLineExclusive < startLine ||
		endLineExclusive > totalLines + 1
	) {
		return undefined;
	}
	return {
		kind: "text",
		startLine,
		endLineExclusive,
		totalLines,
		payloadTextLength,
		maxLines,
		maxBytes,
	};
}

function readCoverageValue(details: unknown): unknown {
	if (!details || typeof details !== "object") return undefined;
	return (details as { readonly [READ_RANGE_COVERAGE_DETAILS_KEY]?: unknown })[READ_RANGE_COVERAGE_DETAILS_KEY];
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
	const details: ReadToolDetails & { [READ_RANGE_COVERAGE_DETAILS_KEY]: ReadRangeCoverage } = {
		...(result.details ?? {}),
		[READ_RANGE_COVERAGE_DETAILS_KEY]: coverage,
	};
	return {
		...result,
		details,
	};
}

/** Attach Pi-specific realized coverage without changing the underlying tool result. */
export function withPiProjectionCoverage(
	tool: string,
	input: unknown,
	result: AgentToolResult<unknown>,
): AgentToolResult<unknown> {
	if (tool !== "read" || !isReadToolInput(input)) return result;
	return withPiReadCoverage(input, result as AgentToolResult<ReadToolDetails | undefined>);
}

function inferPiReadCoverage(
	input: ReadToolInput,
	result: AgentToolResult<ReadToolDetails | undefined>,
): ReadRangeCoverage | undefined {
	if (result.content.length !== 1 || result.content[0]?.type !== "text") return undefined;
	const text = result.content[0].text;
	const startLine = Math.max(1, Math.floor(input.offset ?? 1));
	const truncation = result.details?.truncation;
	if (truncation?.firstLineExceedsLimit) return undefined;

	if (truncation?.truncated) {
		const totalLines = totalLinesFromTruncationNotice(text);
		if (totalLines === undefined || !text.startsWith(truncation.content)) return undefined;
		return {
			kind: "text",
			startLine,
			endLineExclusive: startLine + truncation.outputLines,
			totalLines,
			payloadTextLength: truncation.content.length,
			maxLines: truncation.maxLines,
			maxBytes: truncation.maxBytes,
		};
	}

	const limited = /\n\n\[(\d+) more lines in file\. Use offset=\d+ to continue\.\]$/.exec(text);
	const remaining = limited ? Number(limited[1]) : 0;
	const payload = limited ? text.slice(0, limited.index) : text;
	const requestedLimit = input.limit === undefined ? undefined : Math.max(0, Math.floor(input.limit));
	const lineCount = payload === "" && requestedLimit === 0 ? 0 : payload.split("\n").length;
	const totalLines = startLine - 1 + lineCount + remaining;
	if (totalLines < 1) return undefined;
	return {
		kind: "text",
		startLine,
		endLineExclusive: startLine + lineCount,
		totalLines,
		payloadTextLength: payload.length,
		maxLines: DEFAULT_MAX_LINES,
		maxBytes: DEFAULT_MAX_BYTES,
	};
}

function totalLinesFromTruncationNotice(text: string): number | undefined {
	const match = /\[Showing lines \d+-\d+ of (\d+)(?:\.| \()/.exec(text);
	if (!match) return undefined;
	const total = Number(match[1]);
	return Number.isSafeInteger(total) && total > 0 ? total : undefined;
}

function finiteInteger(value: unknown): number | undefined {
	return typeof value === "number" && Number.isInteger(value) ? value : undefined;
}

function isReadToolInput(value: unknown): value is ReadToolInput {
	return (
		!!value &&
		typeof value === "object" &&
		!Array.isArray(value) &&
		typeof (value as { path?: unknown }).path === "string"
	);
}
