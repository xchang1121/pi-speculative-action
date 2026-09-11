import { describe, expect, it } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { type ActionKey, actionKeyCovers, buildPiActionKey } from "../src/action-semantics.ts";
import { CandidateStore, speculativeCacheValue } from "../src/candidate-stores.ts";

interface Entry {
	readonly id: string;
	readonly key: ReturnType<typeof key>;
	readonly estimatedBytes: number;
}

describe("CandidateStore", () => {
	it.each(["id", "partition", "project"] as const)("owns %s while keeping projection reuse and retirement coherent", (field) => {
		let partitionAvailable = true;
		let partitionCalls = 0;
		let replaceDuringPartition: (() => void) | undefined;
		let retireDuringMatch: ((action: ActionKey) => void) | undefined;
		const projector = {
			...READ_RANGE_ACTION_KEY_PROJECTOR,
			partition: (action: ActionKey) => {
				partitionCalls++;
				replaceDuringPartition?.();
				if (partitionAvailable) return READ_RANGE_ACTION_KEY_PROJECTOR.partition(action);
				if (field === "partition") throw new Error("projection temporarily unavailable");
				return field === "id" ? undefined : "changed partition";
			},
			project: (speculative: ActionKey, actor: ActionKey) => {
				retireDuringMatch?.(speculative);
				return READ_RANGE_ACTION_KEY_PROJECTOR.project(speculative, actor);
			},
		};
		const store = new CandidateStore<string, Entry>([projector]);
		const broad = entry("broad", "a.ts", 1, 200);
		const tight = entry("tight", "a.ts", 80, 60);
		const requested = entry("requested", "a.ts", 100, 10);
		store.getOrCreate("one", broad.key, () => broad);
		store.getOrCreate("one", tight.key, () => tight);
		const registeredPartitions = partitionCalls;
		store.settle("one", broad);
		expect(partitionCalls).toBe(registeredPartitions);
		expect(store.pending("one")).toEqual([tight]);
		expect(store.get("one", broad.id)).toBe(broad);
		Object.assign(projector, { [field]: field === "id" ? "changed" : () => undefined });

		const compatible = store.getOrCreate("one", requested.key, () => requested, (existing) =>
			actionKeyCovers(existing.key, requested.key, [READ_RANGE_ACTION_KEY_PROJECTOR]),
		);
		expect(compatible).toMatchObject({
			entry: tight,
			inserted: false,
			match: { kind: "projected", projector: "read.range" },
		});
		expect(store.getOrCreate("one", tight.key, () => entry("duplicate", "a.ts", 80, 60))).toMatchObject({
			entry: tight,
			inserted: false,
			match: { kind: "exact" },
		});
		expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["tight", "broad"]);
		expect(store.lookup("two", requested.key)).toEqual([]);
		store.insert("two", tight);
		expect(store.touch("one", tight)).toBe(true);
		const callsBeforeRelease = partitionCalls;
		partitionAvailable = false;
		expect(store.delete("one", tight)).toBe(true);
		const releaseCalls = partitionCalls - callsBeforeRelease;
		partitionAvailable = true;
		expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["broad"]);
		expect(store.lookup("two", requested.key).map((item) => item.entry.id)).toEqual(["tight"]);
		expect(releaseCalls).toBe(0);
		store.insert("one", tight);
		let deletedDuringMatch = false;
		retireDuringMatch = (action) => {
			if (field === "project" && action !== tight.key) return;
			retireDuringMatch = undefined;
			deletedDuringMatch = store.delete("one", broad);
			if (field === "id") store.insert("one", broad);
		};
		expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["tight"]);
		expect(deletedDuringMatch).toBe(true);
		if (field === "id") {
			expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["tight", "broad"]);
			expect(store.delete("one", broad)).toBe(true);
		}
		const replacement = entry("replacement", "a.ts", 100, 10);
		const inserted = store.getOrCreate("one", replacement.key, () => replacement, (existing) => {
			store.delete("one", existing);
			return true;
		});
		expect(inserted).toMatchObject({ entry: replacement, inserted: true });
		expect(store.values("one")).toEqual([replacement]);
		const rebound = entry("rebound", "a.ts", 100, 10);
		replaceDuringPartition = () => {
			replaceDuringPartition = undefined;
			store.delete("one", replacement);
			store.insert("one", rebound);
		};
		expect(store.lookup("one", requested.key).map((item) => item.entry)).toEqual([rebound]);
		expect(store.delete("one", rebound)).toBe(true);
		expect(store.delete("two", tight)).toBe(true);
		expect(store.allValues()).toEqual([]);
	});

	it("keeps distinct exact owners when their execution contexts cannot be reused", () => {
		const store = new CandidateStore<string, Entry>([]);
		const root = entry("root", "same.ts");
		const derived = entry("derived", "same.ts");
		expect(store.getOrCreate("session", root.key, () => root).inserted).toBe(true);
		store.settle("session", root, false);
		expect(store.pending("session")).toEqual([]);
		expect(store.cached("session")).toEqual([]);
		const separate = store.getOrCreate("session", derived.key, () => derived, () => false);
		expect(separate.inserted).toBe(true);
		expect(store.lookup("session", root.key).map((item) => item.entry.id)).toEqual(["derived", "root"]);
		expect(store.touch("session", root)).toBe(true);
		expect(store.lookup("session", root.key).map((item) => item.entry.id)).toEqual(["root", "derived"]);
		expect(store.delete("session", root)).toBe(true);
		expect(store.lookup("session", derived.key)[0]?.entry).toBe(derived);
		const retireExact = (existing: Entry) => store.delete("session", existing);
		const next = entry("next", "same.ts");
		expect(store.getOrCreate("session", next.key, () => next, retireExact))
			.toMatchObject({ entry: next, inserted: true });
		expect(store.get("session", next.id)).toBe(next);
		const nested = entry("nested", "same.ts");
		expect(store.getOrCreate("session", nested.key, () => nested, (existing) => {
			retireExact(existing);
			store.insert("session", nested);
			return false;
		})).toMatchObject({ entry: nested, inserted: false });
		expect(store.values("session")).toEqual([nested]);
	});
});

describe("candidate retention", () => {
	it("retains scoped freshness and reuse evidence through bounded cache pressure", () => {
		for (const copies of [1, 170]) {
			let scores = 0;
			const cache = new CandidateStore<string, Entry>([], (item) => {
				scores++;
				return item.id.startsWith("valuable") ? 100 : item.id.startsWith("shared") ? 1 : Number.NaN;
			});
			const group = (name: string) => Array.from({ length: copies }, (_, index) =>
				entry(`${name}:${index}`, `${name}-${index}.ts`, 1, 20, 8));
			const shared = group("shared"), valuable = group("valuable"), worthless = group("worthless");
			for (const item of shared) {
				cache.settle("one", item);
				cache.settle("two", item);
				cache.recordActorHit("one", item);
				const evidence = cache.evidenceOf("one", item);
				cache.settle("one", item);
				expect(cache.evidenceOf("one", item)).toEqual(evidence);
			}
			for (const item of valuable) cache.settle("one", item);
			const limits = { maxEntries: 2 * copies, maxBytes: 16 * copies, hotFraction: 0.5 };
			for (const item of valuable) {
				const last = item === valuable.at(-1);
				scores = 0;
				expect(cache.recordActorHit("one", item, last ? limits : undefined)).toEqual(last ? shared : []);
				expect(scores).toBeLessThanOrEqual(2 * copies);
			}
			for (const [scope, entries, segment, actorHits] of [
				["one", shared, "cold", 1], ["one", valuable, "hot", 1], ["two", shared, "cold", 0],
			] as const) {
				for (const item of entries) expect(cache.evidenceOf(scope, item)).toMatchObject({ segment, actorHits });
			}
			for (const item of worthless) cache.settle("one", item);
			const trim = (expected: Entry[], budget = limits, canEvict?: (item: Entry) => boolean) => {
				scores = 0;
				const count = cache.values("one").length;
				expect(cache.trim("one", budget, canEvict)).toEqual(expected);
				expect(scores).toBeLessThanOrEqual(count);
			};
			// Borrowed results count toward the budget even when pressure cannot yet evict them.
			trim(worthless.slice(1), limits, (item) => worthless.includes(item) && item !== worthless[0]);
			expect(cache.values("one")).toHaveLength(2 * copies + 1);
			trim([worthless[0]!]);
			trim(shared, { ...limits, maxEntries: copies, maxBytes: 8 * copies });
			const older = valuable[0]!, fresh = { ...older, id: "fresh" };
			cache.settle("one", fresh);
			expect(cache.lookup("one", fresh.key).map((item) => item.entry)).toEqual([fresh, older]);
			for (const item of [older, fresh]) expect(cache.evidenceOf("one", item)).toBeDefined();
			expect(cache.delete("one", fresh)).toBe(true);
			expect(cache.values("one")).toEqual(valuable);
			expect(cache.snapshot("one")).toEqual({ coldEntries: 0, hotEntries: copies, coldBytes: 0, hotBytes: 8 * copies });
		}
	});

	it("decays proven reuse value while keeping validation and projection costs honest", () => {
		const base = {
			executionMs: 100,
			expectedValidationMs: 10,
			expectedProjectionMs: 5,
			bytes: 4_096,
			insertedAt: 0,
		};
		const freshHot = speculativeCacheValue({ ...base, actorHits: 1, lastActorHitAt: 0 }, 0, 1_000);
		const agedHot = speculativeCacheValue({ ...base, actorHits: 1, lastActorHitAt: 0 }, 1_000, 1_000);
		const freshCold = speculativeCacheValue({ ...base, actorHits: 0 }, 0, 1_000);

		expect(freshHot).toBeGreaterThan(agedHot);
		expect(agedHot).toBeGreaterThan(freshCold);
		expect(speculativeCacheValue({ ...base, actorHits: 3, expectedValidationMs: 100 }, 0, 1_000)).toBe(0);
	});
});

function entry(id: string, path: string, offset = 1, limit = 20, estimatedBytes = 1): Entry {
	return { id, key: key(path, offset, limit), estimatedBytes };
}

function key(path: string, offset = 1, limit = 20) {
	const action = buildPiActionKey("read", { path, offset, limit }, "", "schema");
	if (!action) throw new Error("read action key should be supported");
	return action satisfies ActionKey;
}
