import { describe, expect, it } from "vitest";
import { READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { type ActionKey, actionKeyCovers, buildPiActionKey } from "../src/action-semantics.ts";
import { ActionStore, ResultCache, speculativeCacheValue } from "../src/candidate-stores.ts";

interface Entry {
	readonly id: string;
	readonly key: ReturnType<typeof key>;
	readonly estimatedBytes: number;
}

describe("ActionStore", () => {
	it.each(["id", "partition", "project"] as const)("owns %s while directionally reusing the tightest compatible projection", (field) => {
		const projector = { ...READ_RANGE_ACTION_KEY_PROJECTOR };
		const store = new ActionStore<string, Entry>([projector]);
		const broad = entry("broad", "a.ts", 1, 200);
		const tight = entry("tight", "a.ts", 80, 60);
		const requested = entry("requested", "a.ts", 100, 10);
		store.insertOrGetCompatible("one", broad);
		store.insertOrGetCompatible("one", tight);
		Object.assign(projector, { [field]: field === "id" ? "changed" : () => undefined });

		const compatible = store.insertOrGetCompatible("one", requested, (existing) =>
			actionKeyCovers(existing.key, requested.key, [READ_RANGE_ACTION_KEY_PROJECTOR]),
		);
		expect(compatible).toMatchObject({
			entry: tight,
			inserted: false,
			match: { kind: "projected", projector: "read.range" },
		});
		expect(store.insertOrGetCompatible("one", entry("duplicate", "a.ts", 80, 60))).toMatchObject({
			entry: tight,
			inserted: false,
			match: { kind: "exact" },
		});
		expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["tight", "broad"]);
		expect(store.lookup("two", requested.key)).toEqual([]);
		expect(store.delete("one", tight)).toBe(true);
		expect(store.lookup("one", requested.key).map((item) => item.entry.id)).toEqual(["broad"]);
	});

	it("keeps distinct exact owners when their execution contexts cannot be reused", () => {
		const store = new ActionStore<string, Entry>([], true);
		const root = entry("root", "same.ts");
		const derived = entry("derived", "same.ts");
		expect(store.insertOrGetCompatible("session", root).inserted).toBe(true);
		expect(
			store.insertOrGetCompatible(
				"session",
				derived,
				() => false,
				() => false,
			).inserted,
		).toBe(true);
		expect(store.lookup("session", root.key).map((item) => item.entry.id)).toEqual(["derived", "root"]);
		expect(store.touch("session", root)).toBe(true);
		expect(store.lookup("session", root.key).map((item) => item.entry.id)).toEqual(["root", "derived"]);
		expect(store.delete("session", root)).toBe(true);
		expect(store.getExact("session", derived.key)).toBe(derived);
	});
});

describe("ResultCache", () => {
	it("owns reuse evidence independently from cold/hot retention", () => {
		const cache = new ResultCache<string, Entry>([], (item) =>
			item.id === "valuable" ? 100 : item.id === "shared" ? 1 : Number.NaN,
		);
		const shared = entry("shared", "a.ts", 1, 20, 8);
		const valuable = entry("valuable", "b.ts", 1, 20, 8);
		const worthless = entry("worthless", "c.ts", 1, 20, 8);
		cache.insert("one", shared);
		cache.insert("two", shared);
		cache.insert("one", valuable);
		cache.recordActorHit("one", shared);
		expect(cache.recordActorHit("one", valuable, { maxEntries: 2, maxBytes: 16, hotFraction: 0.5 })).toEqual([
			shared,
		]);
		cache.insert("one", worthless);

		expect(cache.evidenceOf("one", shared)).toMatchObject({
			segment: "cold",
			actorHits: 1,
		});
		expect(cache.evidenceOf("one", valuable)).toMatchObject({
			segment: "hot",
			actorHits: 1,
		});
		expect(cache.evidenceOf("two", shared)).toMatchObject({
			segment: "cold",
			actorHits: 0,
		});
		expect(cache.trim("one", { maxEntries: 2, maxBytes: 16 })).toEqual([worthless]);
		expect(cache.trim("one", { maxEntries: 1, maxBytes: 8 })).toEqual([shared]);
		expect(cache.values("one")).toEqual([valuable]);
		expect(cache.snapshot("one")).toEqual({
			coldEntries: 0,
			hotEntries: 1,
			coldBytes: 0,
			hotBytes: 8,
		});
	});

	it("keeps in-flight, reusable, and exclusive entries independent at the same action key", () => {
		const jobs = new ActionStore<string, Entry>();
		const results = new ResultCache<string, Entry>();
		const branches = new ActionStore<string, Entry>();
		const job = entry("job", "a.ts");
		const result = entry("result", "a.ts");
		const branch = entry("branch", "a.ts");
		jobs.insert("session", job);
		results.insert("session", result);
		branches.insert("session", branch);

		expect(jobs.delete("session", job)).toBe(true);
		expect(results.values("session")).toEqual([result]);
		expect(branches.getExact("session", branch.key)).toBe(branch);
		expect(results.trim("session", { maxEntries: 0, maxBytes: 0 })).toEqual([result]);
		expect(branches.values("session")).toEqual([branch]);
	});

	it("retains exact freshness generations independently", () => {
		const cache = new ResultCache<string, Entry>();
		const older = entry("older", "same.ts");
		const fresh = entry("fresh", "same.ts");
		cache.insert("session", older);
		cache.insert("session", fresh);

		expect(cache.lookup("session", fresh.key).map((item) => item.entry.id)).toEqual(["fresh", "older"]);
		expect(cache.evidenceOf("session", older)).toBeDefined();
		expect(cache.evidenceOf("session", fresh)).toBeDefined();
		expect(cache.delete("session", fresh)).toBe(true);
		expect(cache.values("session")).toEqual([older]);
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
