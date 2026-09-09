import { describe, expect, it } from "vitest";
import { PpmCountTrie } from "../src/ppm-count-trie.ts";

describe("PpmCountTrie", () => {
	it.each([0, 2])("counts the root and every bounded suffix exactly once at order %i", (order) => {
		const model = new PpmCountTrie(order);
		model.observe(["old", "recent", "latest"], "read", 7);

		expect(model.snapshot()).toEqual([
			{ context: [], counts: { read: 1 }, lastSeen: 7 },
			...(order ? [
				{ context: ["recent", "latest"], counts: { read: 1 }, lastSeen: 7 },
				{ context: ["latest"], counts: { read: 1 }, lastSeen: 7 },
			] : []),
		]);
		expect(model.size).toBe(order + 1);
		expect(model.estimate(["old", "recent", "latest"], "read")?.order).toBe(Math.min(order, 1));
	});

	it("uses the longest matching suffix to disambiguate a shared unigram", () => {
		const model = new PpmCountTrie(2);
		for (let index = 0; index < 8; index++) model.observe(["grep", "success"], "read", index);
		for (let index = 0; index < 8; index++) model.observe(["edit", "success"], "bash", index + 8);

		expect(model.probability(["grep", "success"], "read")).toBeGreaterThan(0.8);
		expect(model.probability(["grep", "success"], "read")).toBeGreaterThan(
			model.probability(["grep", "success"], "bash") ?? 1,
		);
		expect(model.estimate(["grep", "success"], "read")?.order).toBe(2);
	});

	it("starts at the shortest deterministic suffix instead of a sparse extension", () => {
		const model = new PpmCountTrie(2);
		for (let index = 0; index < 8; index++) model.observe(["stable"], "read", index);
		model.observe(["rare", "stable"], "read", 8);

		expect(model.estimate(["rare", "stable"], "read")?.order).toBe(1);
		expect(model.probability(["rare", "stable"], "read")).toBe(model.probability(["new", "stable"], "read"));
	});

	it("escapes from an unseen long context to a shorter suffix", () => {
		const model = new PpmCountTrie(3);
		for (let index = 0; index < 6; index++) model.observe(["grep"], "read", index);
		for (let index = 0; index < 4; index++) model.observe(["other", "grep"], "bash", index + 6);

		const estimate = model.estimate(["new", "grep"], "read");
		expect(estimate).toMatchObject({ order: 1, evidence: 6 });
		expect(estimate?.probability).toBeGreaterThan(0);
		expect(estimate?.escapeMass).toBeGreaterThanOrEqual(0);
		expect(estimate?.escapeMass).toBeLessThanOrEqual(1);
	});

	it("forgets stale target majorities while preserving disabled and stationary ordering", () => {
		const model = new PpmCountTrie(1);
		for (const context of [[], ["shift"]] as const) {
			model.setCount(context, "read", 10, 10);
			model.setCount(context, "find", 4, 28);
		}
		const rawRead = model.probability(["shift"], "read")!;
		const rawFind = model.probability(["shift"], "find")!;
		expect(rawRead).toBeGreaterThan(rawFind);
		expect(model.probability(["shift"], "read", 30, 0)).toBe(rawRead);
		expect(model.probability(["shift"], "find", 30, 8)).toBeGreaterThan(
			model.probability(["shift"], "read", 30, 8) ?? 1,
		);
		model.setCount(["stationary"], "read", 8, 30);
		model.setCount(["stationary"], "find", 4, 30);
		expect(model.probability(["stationary"], "read", 34, 8)).toBeGreaterThan(
			model.probability(["stationary"], "find", 34, 8) ?? 1,
		);
		expect(model.estimate(["shift"], "bash", 30, 8)).toBeUndefined();
	});

	it("does not refresh a stale count bucket when one old target recurs", () => {
		const model = new PpmCountTrie(1);
		for (let sequence = 0; sequence < 32; sequence++) model.observe(["context"], "old", sequence, 64);
		for (let sequence = 32; sequence < 288; sequence++) model.observe(["noise"], "noise", sequence, 64);
		model.observe(["context"], "old", 288, 64);

		const oldCount = model.snapshot().find((row) => row.context[0] === "context")?.counts.old;
		expect(oldCount).toBeLessThan(3);
		for (let sequence = 289; sequence < 292; sequence++) model.observe(["context"], "new", sequence, 64);
		expect(model.probability(["context"], "new", 292, 64)).toBeGreaterThan(
			model.probability(["context"], "old", 292, 64) ?? 1,
		);
	});

	it("keeps probabilities finite under large and fractional restored counts", () => {
		const model = new PpmCountTrie(2);
		model.setCount([], "read", Number.MAX_SAFE_INTEGER, 1);
		model.setCount([], "bash", Number.MAX_SAFE_INTEGER, 1);
		model.setCount(["grep"], "read", 0.5, 2);

		const probability = model.probability(["grep"], "read");
		expect(probability).toBeTypeOf("number");
		expect(Number.isFinite(probability)).toBe(true);
		expect(probability).toBeGreaterThanOrEqual(0);
		expect(probability).toBeLessThanOrEqual(1);
		expect(model.probability([], "read")).toBeCloseTo(0.5);
		expect(model.snapshot()[0]).toEqual({
			context: [],
			counts: { bash: Number.MAX_SAFE_INTEGER, read: Number.MAX_SAFE_INTEGER },
			lastSeen: 1,
		});
	});

	it("restores deterministic snapshots and ignores malformed evidence", () => {
		const model = new PpmCountTrie(2);
		model.restore([
			null,
			{ context: ["grep"], counts: { read: 3, bash: -1 }, lastSeen: 8 },
			{ context: ["too", "deep", "context"], counts: { bash: 5 }, lastSeen: 9 },
			{ context: [], counts: { read: Number.NaN, grep: 2 }, lastSeen: 7 },
		]);

		expect(model.snapshot()).toEqual([
			{ context: [], counts: { grep: 2 }, lastSeen: 7 },
			{ context: ["grep"], counts: { read: 3 }, lastSeen: 8 },
		]);
	});

	it("trims in place with the same decay state as snapshot restoration", () => {
		const model = new PpmCountTrie(2);
		model.observe(["ancestor", "leaf"], "read", 1, 2);
		model.observe(["ancestor", "leaf"], "bash", 5, 2);
		model.observe(["rare"], "grep", 6, 2);
		const restored = new PpmCountTrie(2);
		restored.restore(model.snapshot(2));

		model.trim(2);

		expect(model.snapshot()).toEqual(restored.snapshot());
		expect(model.snapshot().map((row) => row.context)).toEqual([[], ["ancestor", "leaf"]]);
		for (const target of ["read", "bash", "grep"]) {
			expect(model.distribution(["ancestor", "leaf"], 8, 2).get(target)).toEqual(
				restored.estimate(["ancestor", "leaf"], target, 8, 2),
			);
		}
		model.observe(["ancestor", "leaf"], "read", 10, 2);
		restored.observe(["ancestor", "leaf"], "read", 10, 2);
		model.trim(2);
		restored.trim(2);
		expect(model.snapshot()).toEqual(restored.snapshot());
	});

	it("reconfigures to a shorter order without losing retained suffix evidence", () => {
		const model = new PpmCountTrie(3);
		model.observe(["a", "b", "c"], "read", 1);

		model.reconfigure(1, 8);

		expect(model.maxOrder).toBe(1);
		expect(model.snapshot().map((row) => row.context)).toEqual([[], ["c"]]);
		expect(model.probability(["a", "b", "c"], "read")).toBeGreaterThan(0);
	});

	it("orders equal-count snapshot rows deterministically", () => {
		const model = new PpmCountTrie(1);
		model.setCount(["z"], "read", 1, 2);
		model.setCount(["a"], "read", 1, 2);
		model.setCount([], "read", 2, 2);

		expect(model.snapshot().map((row) => row.context)).toEqual([[], ["a"], ["z"]]);
	});
});
