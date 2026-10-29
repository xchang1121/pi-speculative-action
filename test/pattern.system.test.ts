import { afterEach, expect, test, vi } from "vitest";
import path from "node:path";
import { temporaryDirectories } from "./filesystem.ts";
import { PpmCountTrie } from "../src/ppm-count-trie.ts";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { systemHost } from "./agent-system-fixture.ts";
import { PatternAwareStore, PATTERN_AWARE_DEFAULTS, type PatternAwareEvent } from "../src/pattern-aware.ts";

const directories = temporaryDirectories("pi-pattern-system-");
afterEach(directories.dispose);

test.each([1, 2])("preserves constant prediction support when another session chose %s", async mode => {
	const file = path.join(await directories.create(), "support.json");
	const settings = { ...PATTERN_AWARE_DEFAULTS, presets: [], maxContextLength: 1, maxFutureGap: 0 };
	let store = new PatternAwareStore(settings, file);
	await store.load();
	const event = (sessionID: string, turnID: string, tool = "inspect", input = {}) =>
		({ sessionID, turnID, tool, input, outcome: "success" as const, durationMs: 1 });
	for (const [session, choice, count] of [["first", 1, 4], ["second", mode, 1]] as const) for (let index = 0; index < count; index++) {
		store.observe(event(session, String(index)));
		store.observe(event(session, String(index), "choose", { mode: choice }));
		store.finishSession(session);
	}
	const assertSupport = () => {
		for (const session of ["first", "unseen"]) expect(store.predictAfterBatch(session, [event(session, "new")]).map(candidate => candidate.input))
			.toEqual(session === "first" || mode === 1 ? [{ mode: 1 }] : []);
	};
	assertSupport(); await store.flush();
	store = new PatternAwareStore(settings, file);
	await store.load(); assertSupport();
});

test("scopes reserved observation revisions to each PatternAware turn", async () => {
	const f = systemHost(process.cwd(), [createReadTool(process.cwd())], { patternStore: new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, presets: [] }) },
		{ patternAware: { enabled: true, presets: [] } });
	let extra = 0;
	let prior: Parameters<typeof f.host.runtime.submitPlan>[0] | undefined;
	const admitted: string[] = [];
	f.host.runtime.subscribe({ prediction_admitted: event => { admitted.push(event.proposalID); } });
	const reserve = vi.spyOn(f.host.runtime, "reservePlanRevision").mockImplementation((_session, _id, minimum) => minimum + extra);
	const submit = vi.spyOn(f.host.runtime, "submitPlan");
	try {
		for (let round = 0; round < 2; round++) {
			await f.start("same");
			if (prior) await f.host.runtime.submitPlan(prior, { id: "stale-turn", source: "pattern_aware", revision: 0,
				actions: [{ id: "read", type: "tool_call", tool: "read", input: { path: "package.json" } }] });
			for (const [offset, revision] of [[0, 0], [5, 6], [0, 7]]) {
				extra = offset!; submit.mockClear();
				await f.host.execute({ turnID: "same", tool: "inspect", args: {}, tools: [] }, undefined, async () => ({ content: [], details: {} }));
				await expect.poll(() => submit.mock.calls.at(-1)?.[1]).toMatchObject({ revision });
				prior = submit.mock.calls.at(-1)?.[0];
			}
			await f.finish();
		}
		expect(admitted).toEqual([]);
	} finally { await f.dispose(); reserve.mockRestore(); submit.mockRestore(); }
});

const locations: PatternAwareEvent = { sessionID: "bindings", turnID: "one", tool: "custom-inspect", input: {},
	outcome: "success", durationMs: 1, sequence: 1, outputLocations: [{ path: "src/a.ts", line: 3 }, { path: "src/b.ts", line: 17 }] };

test.each([false, true])("learns complete argument relations with a single prediction (cross-row %s)", crossRow => {
	const store = new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, presets: [], maxContextLength: 1, maxFutureGap: 0, beamWidth: 1 });
	for (let index = 0; index < 4; index++) {
		const context = { ...locations, sessionID: String(index), outputLocations: [
			{ path: `src/a${index}.ts`, line: 3 + 100 * index }, { path: `src/b${index}.ts`, line: 17 + 100 * index },
		] };
		store.observe(context);
		store.observe({ ...context, tool: "custom-read", input: { path: `test/a${index}.ts`, offset: (crossRow ? 17 : 3) + 100 * index }, outputLocations: undefined });
		store.finishSession(context.sessionID);
	}
	const predicted = store.predictAfterBatch("unseen", [{ ...locations, sessionID: "unseen",
		outputLocations: [{ path: "src/c.ts", line: 5 }, { path: "src/d.ts", line: 21 }] }]);
	expect(predicted.map(({ input }) => input)).toEqual([{ path: "test/c.ts", offset: crossRow ? 21 : 5 }]);
});

test.each([1, 8, 64])("keeps bounded learned suffixes through persistence and further learning (capacity %s)", capacity => {
	const model = new PpmCountTrie(4), restored = new PpmCountTrie(4), history: string[] = [];
	for (let sequence = 1; sequence <= 200; sequence++) {
		const target = sequence % 3 ? "inspect" : "render";
		for (const store of [model, restored]) { store.observe(history, target, sequence, 32); store.trim(capacity); }
		expect(model.size).toBeLessThanOrEqual(capacity);
		expect(restored.distribution(history, sequence, 32)).toEqual(model.distribution(history, sequence, 32));
		if (sequence % 17 === 0) restored.restore(JSON.parse(JSON.stringify(model.snapshot())));
		history.push(`context:${sequence % 7}`);
		if (history.length > 4) history.shift();
	}
	const probabilities = [...model.distribution(history, 200, 32).values()];
	expect(probabilities.every(value => Number.isFinite(value) && value > 0 && value < 1)).toBe(true);
	expect(probabilities.reduce((sum, value) => sum + value, 0)).toBeLessThanOrEqual(1);
});
