import { describe, expect, it } from "vitest";
import { BoundedRecencyMap } from "../src/bounded-recency-map.ts";

describe("BoundedRecencyMap", () => {
	it("evicts the least recently used derived entry", () => {
		const cache = new BoundedRecencyMap<string, number | null>(2);
		cache.set("first", null);
		cache.set("second", 2);

		expect(cache.get("first")).toBeNull();
		expect(cache.set("third", 3)).toEqual({ key: "second", value: 2 });
		expect(cache.get("second")).toBeUndefined();
		expect([...cache.values()]).toEqual([null, 3]);
	});
});
