import { describe, expect, test } from "vitest";
import { stableEqual, stableStringify } from "../src/stable-json.ts";

describe("stable JSON values", () => {
	test("preserves canonical object and integer-index key order", () => {
		const first = { zebra: 1, "10": "ten", alpha: { right: 2, left: 1 }, "2": "two", "01": "named" };
		const second = { "01": "named", "2": "two", alpha: { left: 1, right: 2 }, "10": "ten", zebra: 1 };

		expect(stableStringify(first)).toBe(stableStringify(second));
		expect(stableEqual(first, second)).toBe(true);
		expect(stableEqual(first, { ...second, zebra: 2 })).toBe(false);
		expect(stableStringify(first)).toBe('{"2":"two","10":"ten","01":"named","alpha":{"left":1,"right":2},"zebra":1}');
		const named = { "\u00e9": 2, "e\u0301": 1, Z: 3, z: 4, _: 5 };
		const reversed = Object.fromEntries(Object.entries(named).reverse());
		expect(stableEqual(named, reversed)).toBe(true);
		expect(stableStringify(named)).toBe(stableStringify(reversed));
		expect(stableStringify(named)).toBe('{"Z":3,"_":5,"e\u0301":1,"z":4,"\u00e9":2}');
	});

	test("matches JSON omission and array placeholder semantics", () => {
		const sparse = [undefined, Number.NaN, Number.POSITIVE_INFINITY, () => undefined, Symbol("ignored")];
		sparse.length = 6;

		expect(stableStringify({ omitted: undefined, kept: sparse, negativeZero: -0 })).toBe(
			'{"kept":[null,null,null,null,null,null],"negativeZero":0}',
		);
		expect(stableEqual({ omitted: undefined, kept: sparse }, { kept: Array(6).fill(null) })).toBe(true);
		expect(stableEqual(sparse, [null, null, null, null, null, 1])).toBe(false);
	});

	test("ignores custom toJSON while retaining enumerable data", () => {
		expect(stableStringify({ value: 1, toJSON: () => ({ replaced: true }) })).toBe('{"value":1}');
		expect(stableEqual({ value: 1, toJSON: () => ({ replaced: true }) }, { value: 1 })).toBe(true);
		expect(() => stableStringify({ value: 1n })).toThrow(TypeError);
		expect(() => stableEqual({ value: 1n }, { value: 1n })).toThrow(TypeError);
	});
});
