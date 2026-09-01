import { describe, expect, it } from "vitest";
import {
	nonEmptyTextInput,
	nonNegativeIntegerInput,
	nonNegativeNumberInput,
	optionalPositiveIntegerInput,
	optionalTextInput,
	positiveIntegerInput,
	probabilityInput,
} from "../src/setting-input.ts";

describe("typed setting input", () => {
	it("enforces integer and numeric domains at their boundaries", () => {
		expect(positiveIntegerInput("Count").parse(" 3 ")).toEqual({ ok: true, value: 3 });
		expect(positiveIntegerInput("Count").parse("0")).toEqual({
			ok: false,
			error: "Count must be a positive integer.",
		});
		expect(nonNegativeIntegerInput("Depth").parse("")).toEqual({ ok: true, value: 0 });
		expect(nonNegativeIntegerInput("Depth").parse("0.5").ok).toBe(false);
		expect(nonNegativeNumberInput("Delay").parse("0.25")).toEqual({ ok: true, value: 0.25 });
		expect(nonNegativeNumberInput("Delay").parse("-1").ok).toBe(false);
	});

	it("accepts probability endpoints and rejects non-finite or out-of-range values", () => {
		const probability = probabilityInput("Confidence");
		expect(probability.parse("0")).toEqual({ ok: true, value: 0 });
		expect(probability.parse("1")).toEqual({ ok: true, value: 1 });
		expect(probability.parse("1.01").ok).toBe(false);
		expect(probability.parse("NaN").ok).toBe(false);
	});

	it("models optional values without weakening the underlying validation", () => {
		const tokens = optionalPositiveIntegerInput("Tokens");
		expect(tokens.format(undefined)).toBe("");
		expect(tokens.parse("  ")).toEqual({ ok: true, value: undefined });
		expect(tokens.parse("16")).toEqual({ ok: true, value: 16 });
		expect(tokens.parse("0").ok).toBe(false);

		expect(nonEmptyTextInput("Decoder").parse(" auto ")).toEqual({ ok: true, value: "auto" });
		expect(nonEmptyTextInput("Decoder").parse(" ").ok).toBe(false);
		expect(optionalTextInput("Token environment").parse("  ")).toEqual({ ok: true, value: undefined });
	});

	it("keeps storage transforms and display formatting in the descriptor", () => {
		const mebibytes = positiveIntegerInput("Memory", {
			format: (bytes) => String(bytes / (1024 * 1024)),
			transform: (value) => value * 1024 * 1024,
		});
		expect(mebibytes.format(64 * 1024 * 1024)).toBe("64");
		expect(mebibytes.parse("96")).toEqual({ ok: true, value: 96 * 1024 * 1024 });
	});
});
