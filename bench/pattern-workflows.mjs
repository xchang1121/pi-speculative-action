// Run with node --import tsx bench/pattern-workflows.mjs.
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import {
	PatternAwareStore,
	PATTERN_AWARE_DEFAULTS,
	patternAwareActionSemantics,
} from "../src/pattern-aware.ts";
import { createPatternPlanSource } from "../src/pattern-plan-source.ts";

// Action keys include the workspace. Keep their tie breaks deterministic across repeated runs.
const workspace = path.resolve(os.tmpdir(), "pi-pattern-workflows");
const root = await mkdtemp(path.join(os.tmpdir(), "pi-pattern-workflows-"));
const pattern = { ...PATTERN_AWARE_DEFAULTS, maxPatterns: 128 };
const file = path.join(root, "workspace.json");
const semantics = patternAwareActionSemantics(PI_ACTION_SEMANTICS, workspace);
const store = new PatternAwareStore(pattern, file, semantics);
const frozen = new PatternAwareStore(pattern, file, semantics);
const settings = { enabled: true, sourceConfig: { patternAware: pattern } };
const controller = createPatternPlanSource({
	sessionID: "benchmark",
	cwd: workspace,
	actionSemantics: PI_ACTION_SEMANTICS,
	projectionRules: [],
	store,
});
let events;
controller.connect({
	subscribe: (listeners) => {
		events = listeners;
		return () => {};
	},
	reservePlanRevision: (_session, _id, minimum) => minimum,
	submitPlan: () => {},
});

function workflows(index, training) {
	const unit = "unit" + index;
	const src = "src/" + unit + ".ts";
	const spec = "test/" + unit + ".test.ts";
	const line = 20 + index;
	const read = (path, offset) => ["read", offset ? { path, offset } : { path }, "source"];
	const edit = ["edit", { path: src, edits: [{ oldText: "false", newText: "true" }] }, "edited"];
	const matches = ["first", "second", "third"].map((name) => name + "/" + unit + ".ts");
	const chosen = training && index % 11 === 0 ? "unrelated.ts" : matches[index % 4 === 0 ? 1 : 0];
	return [
		[
			["grep", { pattern: unit }, src + ":" + line + ": export"],
			read(src, line),
			edit,
			["bash", { command: "npm test" }, "    at verify (" + spec + ":" + line + ":4)", true],
			read(spec, line),
		],
		[["find", { pattern: "**/*" + index + "*.ts" }, matches.join("\n")], read(chosen)],
		[["grep", { pattern: unit }, "No matches found"], ["ls", { path: "src" }, unit + ".ts"], read(src)],
		[
			[
				"grep",
				{ pattern: unit },
				src + ":3: ...\n\n[100 matches limit reached]",
				false,
				{ matchLimitReached: 100 },
			],
			["find", { pattern: "**/*.test.ts" }, spec],
			read(spec),
		],
	];
}
const score = {
	opportunities: 0,
	issued: 0,
	correct: 0,
	candidates: 0,
	falseCandidates: 0,
	terminals: 0,
	falseTerminalPredictions: 0,
};
async function episode(id, trace, training) {
	let previous = [];
	for (const [index, [tool, input, text, failed = false, details]] of trace.entries()) {
		if (!training && index) {
			const hit = (action) => action.tool === tool && isDeepStrictEqual(action.input, input);
			score.opportunities++;
			score.issued += Number(previous.length > 0);
			score.correct += Number(!!previous[0] && hit(previous[0]));
			score.candidates += previous.length;
			score.falseCandidates += previous.filter((action) => !hit(action)).length;
		}
		const startInput = { sessionID: id, turnID: String(index) };
		await events.actor_observed({
			startInput,
			consumeInput: startInput,
			tool,
			concrete: input,
			settings,
			durationMs: 1,
			order: 0,
			data: { schemaHashes: {}, tools: new Set(training ? [tool] : []) },
			output: {
				result: { content: [{ type: "text", text }], details },
				isError: failed,
				exitCode: failed ? 1 : 0,
			},
		});
		await events.turn_finished({ startInput, settings, terminal: training && index === trace.length - 1 });
		if (!training) {
			// Drain the confirmed Actor turn before predicting the next call from frozen evidence.
			await controller.hints({ sessionID: id, schemaHashes: {}, settings });
			const history = [...store.recent(id)];
			const last = history.pop();
			previous = frozen
				.continue({ history, visitedPatternIDs: [], pathProbability: 1 }, last, {}, true, pattern)
				.filter((action) => action.horizon === 0 && !action.background);
		}
	}
	if (!training) {
		score.terminals++;
		score.falseTerminalPredictions += Number(previous.length > 0);
		await events.turn_finished({ startInput: { sessionID: id, turnID: "end" }, settings, terminal: true });
	}
}

// Predict before observing the Actor's choice. Portable evidence comes only from the source workspace.
async function rowPreferences() {
	const portable = path.join(root, "rows-shared.json");
	const source = new PatternAwareStore(pattern, path.join(root, "rows-source.json"), undefined, portable);
	await source.load();
	const select = (store, id, chosen, workspace) => {
		const rows = Array.from({ length: 4 }, (_, row) => ({ symbol: workspace + "/" + id + "#" + row }));
		const event = { sessionID: id, turnID: id, outcome: "success", durationMs: 1 };
		store.observe({ ...event, tool: "symbols", input: {}, output: { rows } });
		const list = store.predict(id).filter((p) => p.horizon === 0 && !p.background);
		const best = list[0];
		const actual = { symbol: rows[chosen].symbol };
		const matched = !!best && isDeepStrictEqual(best.input, actual);
		if (best) {
			store.issued(best.patternID);
			store.settled(best.patternID, {
				prediction: { id, proposalID: id, actionID: id, source: "pattern_aware" },
				observation: "observed",
				actorAction: { id, turnID: id, sequence: 1 },
				match: matched
					? {
							matched: true,
							relation: { kind: "exact", distance: 0 },
							adoption: { status: "adopted", candidateID: id },
						}
					: { matched: false },
			});
		}
		store.observe({ ...event, tool: "show", input: actual });
		store.finishSession(id, true);
		return { issued: Number(!!best), exact: Number(matched) };
	};
	for (let i = 0; i < 40; i++) select(source, "source-" + i, i % 4 === 3 ? 1 : 0, "go");
	await source.flush();
	const scores = [
		{ calls: 0, issued: 0, top1: 0 },
		{ calls: 0, issued: 0, top1: 0 },
	];
	for (let trial = 0; trial < 8; trial++) {
		const dest = new PatternAwareStore(pattern, undefined, undefined, portable);
		await dest.load();
		let seed = trial + 1;
		for (const [phase, [major, minor]] of [
			[0, 1],
			[3, 0],
		].entries()) {
			const order = Array.from({ length: 100 }, (_, i) => (i < 75 ? major : minor));
			for (let i = order.length - 1; i > 0; i--) {
				seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
				const j = Math.floor((seed / 4294967296) * (i + 1));
				[order[i], order[j]] = [order[j], order[i]];
			}
			for (const [i, row] of order.entries()) {
				const result = select(dest, phase + "-" + i, row, "kotlin");
				scores[phase].calls++;
				scores[phase].issued += result.issued;
				scores[phase].top1 += result.exact;
			}
		}
	}
	return { stable: scores[0], shifted: scores[1] };
}

try {
	for (let index = 0; index < 20; index++) {
		for (const [kind, trace] of workflows(index, true).entries())
			await episode("train-" + index + "-" + kind, trace, true);
		const noise = Array.from({ length: 8 }, (_, member) => [
			"inspect",
			{ operation: "explore-" + index + "-" + member },
			"unrelated output",
		]);
		await episode("noise-" + index, noise, true);
	}
	// Include the final turn before freezing the training snapshot.
	await controller.hints({ sessionID: "benchmark", schemaHashes: {}, settings });
	await store.load();
	store.finishSession("benchmark");
	await store.flush();
	await frozen.load();
	const bytes = await readFile(file);
	const { patterns, pools } = JSON.parse(bytes);
	for (let index = 100; index < 112; index++)
		for (const [kind, trace] of workflows(index, false).entries())
			await episode("eval-" + index + "-" + kind, trace, false);
	console.log(
		JSON.stringify(
			{
				...score,
				pools: pools.length,
				patterns: patterns.length,
				bytes: bytes.length,
				rowPreferences: await rowPreferences(),
			},
			null,
			2,
		),
	);
} finally {
	await controller.dispose();
	await rm(root, { recursive: true, force: true });
}
