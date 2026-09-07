import { describe, expect, it } from "vitest";
import { READ_RANGE_COVERAGE_DETAILS_KEY, type ReadRangeCoverage } from "../src/action-key-projection.ts";
import { actionKeyCovers, actionKeyMatch, buildPiActionKey, readActionRange } from "../src/action-semantics.ts";
import { PI_READ_RANGE_PROJECTION_RULE } from "../src/pi-read-projection.ts";
import type { ToolSettlement } from "../src/tool-settlement.ts";

const cwd = "/workspace";

function readKey(path: string, offset?: number, limit?: number) {
	const action = buildPiActionKey("read", { path, offset, limit }, cwd);
	if (!action) throw new Error("Expected a read action key");
	return action;
}

function coverage(
	lines: readonly string[],
	options: {
		readonly startLine?: number;
		readonly totalLines?: number;
		readonly maxLines?: number;
		readonly maxBytes?: number;
	} = {},
): ReadRangeCoverage {
	const startLine = options.startLine ?? 1;
	const totalLines = options.totalLines ?? startLine + lines.length - 1;
	return {
		kind: "text",
		startLine,
		endLineExclusive: startLine + lines.length,
		totalLines,
		payloadTextLength: lines.join("\n").length,
		maxLines: options.maxLines ?? 2000,
		maxBytes: options.maxBytes ?? 50 * 1024,
	};
}

function settlement(snapshot?: unknown, text = "speculative output", isError = false): ToolSettlement {
	return {
		result: {
			content: [{ type: "text", text }],
			details: snapshot === undefined ? undefined : { [READ_RANGE_COVERAGE_DETAILS_KEY]: snapshot },
		},
		isError,
	};
}

function coveredSettlement(
	lines: readonly string[],
	options: Parameters<typeof coverage>[1] = {},
	isError = false,
): ToolSettlement {
	return settlement(coverage(lines, options), lines.join("\n"), isError);
}

async function project(
	speculative: ReturnType<typeof readKey>,
	actor: ReturnType<typeof readKey>,
	output: ToolSettlement,
) {
	const keyMatch = actionKeyMatch(speculative, actor, [PI_READ_RANGE_PROJECTION_RULE]);
	if (keyMatch?.kind !== "projected") throw new Error("Expected a projected read-key match");
	const realizedCoverage = PI_READ_RANGE_PROJECTION_RULE.captureCoverage(speculative, output);
	if (realizedCoverage === undefined) return undefined;
	return PI_READ_RANGE_PROJECTION_RULE.projectOutput({
		speculative,
		actor,
		output,
		coverage: realizedCoverage,
		keyMatch,
	});
}

function outputText(output: ToolSettlement | undefined): string | undefined {
	const content = output?.result.content[0];
	return content?.type === "text" ? content.text : undefined;
}

describe("Pi read range projection", () => {
	it("executes a narrow prediction through its earliest default covering view", () => {
		const predicted = readKey("notes.txt", 20, 5);
		const covering = PI_READ_RANGE_PROJECTION_RULE.coveringAction?.(predicted);
		const deepCovering = PI_READ_RANGE_PROJECTION_RULE.coveringAction?.(readKey("notes.txt", 2500, 5));

		expect(covering && readActionRange(covering)).toMatchObject({ offset: 1, limit: 2000 });
		expect(covering && actionKeyCovers(covering, predicted, [PI_READ_RANGE_PROJECTION_RULE])).toBe(true);
		expect(PI_READ_RANGE_PROJECTION_RULE.coveringAction?.(readKey("notes.txt", 100, 5))?.key).toBe(covering?.key);
		expect(deepCovering && readActionRange(deepCovering)).toMatchObject({ offset: 505, limit: 2000 });
		expect(PI_READ_RANGE_PROJECTION_RULE.coveringAction?.(readKey("notes.txt", 20))).toBeUndefined();
		expect(PI_READ_RANGE_PROJECTION_RULE.coveringAction?.(readKey("notes.txt", 20, 0))).toBeUndefined();
	});

	it.each([
		{ name: "middle interval", spec: [1, 10], actor: [3, 2], lines: ["one", "two", "three", "four", "five"],
			options: { totalLines: 20 }, text: "three\nfour\n\n[16 more lines in file. Use offset=5 to continue.]" },
		{ name: "default through EOF", spec: [1, 2], actor: [3], lines: ["one", "two", "three", "four", "five"], text: "three\nfour\nfive" },
		{ name: "zero limit", spec: [1, 5], actor: [2, 0], lines: ["one", "two", "three", "four", "five"],
			text: "\n\n[4 more lines in file. Use offset=2 to continue.]" },
		{ name: "realized EOF", spec: [1, 10], actor: [2, 20], lines: ["one", "two", "three", "four", "five"], text: "two\nthree\nfour\nfive" },
		{ name: "uncovered interval", spec: [1, 4], actor: [3, 3], lines: ["one", "two", "three", "four"], options: { totalLines: 10 } },
		{ name: "CRLF", spec: [1, 3], actor: [2, 2], lines: ["one\r", "two\r", "three"], text: "two\r\nthree" },
		{ name: "line truncation", spec: [1, 5], actor: [1, 4], lines: ["one", "two", "three", "four", "five"],
			options: { maxLines: 2 }, text: "one\ntwo\n\n[Showing lines 1-2 of 5. Use offset=3 to continue.]", truncatedBy: "lines" },
		{ name: "byte truncation", spec: [1, 4], actor: [1, 3], lines: ["aa", "bb", "cc"], options: { maxBytes: 5 },
			text: "aa\nbb\n\n[Showing lines 1-2 of 3 (5B limit). Use offset=3 to continue.]", truncatedBy: "bytes" },
		{ name: "oversized first line", spec: [1, 3], actor: [1, 2], lines: ["abcdef", "x", "y"], options: { maxBytes: 5 } },
	])("preserves output-only fallback: $name", async ({ spec, actor, lines, options, text, truncatedBy }) => {
		const output = await project(readKey("notes.txt", spec[0], spec[1]), readKey("notes.txt", actor[0], actor[1]),
			coveredSettlement(lines, options));
		expect(outputText(output)).toBe(text);
		if (text === undefined) expect(output).toBeUndefined();
		else {
			const evidence = PI_READ_RANGE_PROJECTION_RULE.captureCoverage(readKey("notes.txt", actor[0], actor[1]), output!) as ReadRangeCoverage;
			expect(evidence.startLine).toBe(actor[0]);
			expect(evidence.payloadTextLength).toBe(text.split("\n\n[")[0]!.length);
			if (truncatedBy) expect(output?.result.details).toMatchObject({ truncation: { truncatedBy, outputLines: 2 } });
		}
	});

	it("does not treat an explicit bounded read as an in-flight default view", async () => {
		const implicit = readKey("notes.txt", 1), explicit = readKey("notes.txt", 1, 2);
		const firstTwo = coveredSettlement(["one", "two"], { totalLines: 5, maxLines: 2 });
		expect(actionKeyCovers(implicit, explicit, [PI_READ_RANGE_PROJECTION_RULE])).toBe(true);
		expect(actionKeyCovers(explicit, implicit, [PI_READ_RANGE_PROJECTION_RULE])).toBe(false);
		expect(outputText(await project(implicit, explicit, firstTwo))).toBe("one\ntwo\n\n[3 more lines in file. Use offset=3 to continue.]");
		expect(await project(explicit, implicit, firstTwo)).toBeUndefined();
	});

	it.each([
		["mismatched end", { ...coverage(["one", "two"]), endLineExclusive: 9 }],
		["negative payload length", { ...coverage(["one", "two"]), payloadTextLength: -1 }],
		["end before start", { ...coverage(["one", "two"]), endLineExclusive: 0 }],
		["non-positive byte limit", { ...coverage(["one", "two"]), maxBytes: 0 }],
		["fractional start", { ...coverage(["one", "two"]), startLine: 1.5 }],
	])("rejects malformed coverage: %s", (_label, malformed) => {
		const output = settlement(malformed);
		expect(PI_READ_RANGE_PROJECTION_RULE.captureCoverage(readKey("notes.txt", 1, 2), output)).toBeUndefined();
	});

	it("rejects coverage whose descriptor exceeds the existing output payload", async () => {
		const snapshot = coverage(["one", "two"]);
		expect(
			await project(readKey("notes.txt", 1, 2), readKey("notes.txt", 2, 1), settlement(snapshot, "one")),
		).toBeUndefined();
	});

	it("does not capture missing coverage, tool errors, or non-read outputs", () => {
		const read = readKey("notes.txt", 1, 2);
		const grep = buildPiActionKey("grep", { pattern: "TODO", path: "." }, cwd);
		expect(PI_READ_RANGE_PROJECTION_RULE.captureCoverage(read, settlement())).toBeUndefined();
		expect(
			PI_READ_RANGE_PROJECTION_RULE.captureCoverage(read, coveredSettlement(["one", "two"], {}, true)),
		).toBeUndefined();
		expect(grep).toBeDefined();
		if (grep) {
			expect(PI_READ_RANGE_PROJECTION_RULE.captureCoverage(grep, coveredSettlement(["one", "two"]))).toBeUndefined();
		}
	});

	it("keeps different resources and grep/find actions outside the read relation", () => {
		const read = readKey("notes.txt", 1, 10);
		expect(actionKeyMatch(read, readKey("other.txt", 2, 2), [PI_READ_RANGE_PROJECTION_RULE])).toBeUndefined();
		for (const tool of ["grep", "find"] as const) {
			const speculative = buildPiActionKey(tool, { path: ".", pattern: "*.ts", limit: 10 }, cwd);
			const actor = buildPiActionKey(tool, { path: ".", pattern: "*.ts", limit: 2 }, cwd);
			expect(speculative).toBeDefined();
			expect(actor).toBeDefined();
			if (speculative && actor) {
				expect(actionKeyMatch(speculative, actor, [PI_READ_RANGE_PROJECTION_RULE])).toBeUndefined();
			}
		}
	});
});
