import { createReadToolDefinition, type ReadToolInput } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { READ_RANGE_COVERAGE_DETAILS_KEY, type ReadRangeCoverage } from "../src/action-key-projection.ts";
import { actionKeyMatch, buildPiActionKey } from "../src/action-semantics.ts";
import { PI_READ_RANGE_PROJECTION_RULE, withPiReadCoverage } from "../src/pi-read-projection.ts";

const long = Array.from({ length: 2002 }, (_, index) => `line-${index + 1}`).join("\n");
const literalNotice = "one\n\n[999 more lines in file. Use offset=2 to continue.]";
describe("read coverage on unmodified Pi output", () => {
	it.each([
		{ name: "limited", source: "one\ntwo\nthree\nfour", input: { offset: 2, limit: 2 } },
		{ name: "zero", source: "one\ntwo", input: { limit: 0 } },
		{ name: "line truncation", source: long, input: {} },
		{ name: "explicit truncation", source: long, input: { limit: 2000 } },
		{ name: "byte truncation", source: Array(600).fill("x".repeat(100)).join("\n"), input: {} },
		{ name: "oversized first line", source: "x".repeat(50 * 1024 + 1) + "\ntail", input: {}, unproven: true },
		{ name: "literal suffix", source: literalNotice, input: {} },
		{ name: "literal bounded suffix", source: literalNotice, input: { limit: 100 } },
		{ name: "literal truncation inside payload", source: "[Showing lines 1-2000 of 999999. Use offset=2001 to continue.]\n" + long, input: {} },
	])("proves source coverage independently of document text: $name", async ({ source, input, unproven }) => {
		const args: ReadToolInput = { path: "notes.txt", ...input };
		const tool = createReadToolDefinition("/workspace", { operations: {
			access: async () => undefined, readFile: async () => Buffer.from(source), detectImageMimeType: async () => undefined,
		} });
		const execute = (query: ReadToolInput) => tool.execute("read", query, undefined, undefined, undefined as never);
		const result = withPiReadCoverage(args, await execute(args));
		const coverage = (result.details as { [READ_RANGE_COVERAGE_DETAILS_KEY]?: ReadRangeCoverage } | undefined)?.[READ_RANGE_COVERAGE_DETAILS_KEY];
		if (unproven) { expect(coverage).toBeUndefined(); return; }
		const startLine = args.offset ?? 1, totalLines = source.split("\n").length;
		const count = Math.min(result.details?.truncation?.outputLines ?? Infinity, args.limit ?? 2000, totalLines - startLine + 1);
		expect(coverage).toMatchObject({ startLine, totalLines, endLineExclusive: startLine + count,
			payloadTextLength: source.split("\n").slice(startLine - 1, startLine - 1 + count).join("\n").length });
		if (args.limit === 0) return;
		const query = { path: args.path, offset: startLine, limit: 1 };
		const speculative = buildPiActionKey("read", args, "/workspace")!, actor = buildPiActionKey("read", query, "/workspace")!;
		const keyMatch = actionKeyMatch(speculative, actor, [PI_READ_RANGE_PROJECTION_RULE]);
		expect(keyMatch?.kind).toBe("projected");
		if (keyMatch?.kind !== "projected") throw new Error("expected range relation");
		const projected = await PI_READ_RANGE_PROJECTION_RULE.projectOutput({ speculative, actor,
			output: { result, isError: false }, coverage, keyMatch });
		const expected = await execute(query);
		expect(projected?.result.content).toEqual(expected.content);
		expect(Object.entries(projected?.result.details ?? {})).toEqual(Object.entries(expected.details ?? {}));
	});
});
