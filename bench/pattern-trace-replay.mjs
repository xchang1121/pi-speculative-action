// node --import tsx bench/pattern-trace-replay.mjs /path/to/task-*/record.session.jsonl
import { readFile, mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { createCodingTools, createReadOnlyTools } from "@earendil-works/pi-coding-agent";
import { definitionSchemaHashes } from "../src/agent-integration.ts";
import { actionKeyMatch, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import {
	acquirePatternAwareStore,
	PATTERN_AWARE_DEFAULTS,
	patternAwareActionSemantics,
	projectPatternAwareObservation,
} from "../src/pattern-aware.ts";

async function transcript(file) {
	if (!file.endsWith(".session.jsonl"))
		throw new Error("Expected a benchmark .session.jsonl and its companion .json");
	const [log, summary] = await Promise.all([
		readFile(file, "utf8"),
		readFile(file.replace(/\.session\.jsonl$/, ".json"), "utf8"),
	]);
	const entries = log
		.trim()
		.split(/\r?\n/)
		.map((line) => JSON.parse(line));
	const header = entries.shift();
	const { metadata, traces } = JSON.parse(summary);
	if (!metadata.repo || !header.cwd) throw new Error("Missing benchmark repository or session workspace");
	const byID = new Map(entries.map((entry) => [entry.id, entry]));
	const branch = [];
	let cursor = entries.at(-1);
	while (cursor) {
		branch.push(cursor);
		byID.delete(cursor.id);
		if (!cursor.parentId) break;
		cursor = byID.get(cursor.parentId);
		if (!cursor) throw new Error("Incomplete or cyclic session ancestry");
	}
	const batches = [];
	const historical = new Map(traces.actorActions.map((action) => [action.id, action]));
	const results = new Map();
	for (const { message } of branch.reverse()) {
		if (message?.role === "toolResult") results.set(message.toolCallId, message);
		if (message?.role !== "assistant") continue;
		const calls = message.content.filter((block) => block.type === "toolCall");
		if (calls.length) batches.push(calls);
	}
	return { repo: metadata.repo, session: header.id, cwd: header.cwd, batches, results, historical };
}

async function replay(tape, stateDirectory) {
	const workspace = path.join(stateDirectory, encodeURIComponent(tape.repo), encodeURIComponent(tape.cwd));
	const definitions = [...createCodingTools(tape.cwd), ...createReadOnlyTools(tape.cwd)];
	const schemas = definitionSchemaHashes(
		definitions.map((tool) => ({ name: tool.name, inputSchema: tool.parameters })),
	);
	const rules = PI_ACTION_SEMANTICS.projectors();
	const semantics = patternAwareActionSemantics(PI_ACTION_SEMANTICS, tape.cwd, rules);
	const key = (tool, input) => semantics.actionKey(tool, input, schemas[tool]);
	const lease = await acquirePatternAwareStore(workspace, PATTERN_AWARE_DEFAULTS, stateDirectory, semantics);
	const { store } = lease;
	const startingPatterns = store.snapshot().length;
	const score = {
		batches: 0,
		calls: 0,
		top1: 0,
		proposals: 0,
		falseProposals: 0,
		rawExact: 0,
		exact: 0,
		projected: 0,
		coveredCalls: 0,
		matchedRecordedWallMs: 0,
	};
	const predict = () => store.predict(tape.session, schemas).filter((p) => p.horizon === 0 && !p.background);
	try {
		for (const [index, batch] of tape.batches.entries()) {
			const predictions = predict();
			if (index) score.batches++;
			score.calls += batch.length;
			score.proposals += predictions.length;
			const targets = batch.map((call) => ({ call, key: key(call.name, call.arguments) }));
			const covered = new Set();
			const settlements = new Map();
			for (const [rank, p] of predictions.entries()) {
				const predicted = key(p.tool, p.input);
				const matches = targets
					.filter(({ call }) => call.name === p.tool)
					.map(({ call, key: actor }) => ({
						call,
						raw: isDeepStrictEqual(call.arguments, p.input),
						relation: predicted && actor && actionKeyMatch(predicted, actor, rules, true),
					}));
				score.rawExact += Number(matches.some(({ raw }) => raw));
				const compatible = matches.filter(({ relation }) => relation)
					.sort(
						(a, b) =>
							Number(b.raw) - Number(a.raw) ||
							Number(b.relation.kind === "exact") - Number(a.relation.kind === "exact") ||
							a.relation.distance - b.relation.distance,
					);
				if (compatible.length) {
					score[compatible[0].relation.kind]++;
					score.top1 += Number(rank === 0);
					for (const { call } of compatible) covered.add(call.id);
				} else score.falseProposals++;
				const adopted = compatible.find(({ call }) => {
					const old = tape.historical.get(call.id);
					if (
						old?.provider !== "speculative" ||
						old.candidateSource !== "pattern_aware" ||
						!old.predictedAction
					)
						return false;
					const issued = JSON.parse(old.predictedAction);
					return issued.tool === p.tool && key(issued.tool, issued.input)?.key === predicted.key;
				});
				for (const id of p.supportingPatternIDs) {
					// A shadow relation can be scored; export credit requires the archived host's actual adoption.
					const old = settlements.get(id);
					if (!old || adopted || (compatible.length && !old.matched)) {
						settlements.set(id, { matched: compatible.length > 0, adopted });
					}
				}
			}
			score.coveredCalls += covered.size;
			for (const id of covered) score.matchedRecordedWallMs += tape.historical.get(id)?.wallMs ?? 0;
			for (const [id, { matched, adopted }] of settlements) {
				if (matched && !adopted) continue;
				const call = adopted?.call ?? batch[0];
				store.settled(id, {
					prediction: { id, proposalID: id, actionID: id, source: "pattern_aware" },
					observation: "observed",
					actorAction: { id: call.id, turnID: String(index), sequence: index },
					match: adopted
						? {
								matched: true,
								relation: adopted.relation,
								adoption: { status: "adopted", candidateID: call.id },
							}
						: { matched: false },
				});
			}
			store.observeBatch(
				batch.map((call) => {
					const result = tape.results.get(call.id);
					if (result?.toolName !== call.name) throw new Error("Missing or mismatched recorded tool result");
					return {
						sessionID: tape.session,
						turnID: String(index),
						tool: call.name,
						input: call.arguments,
						schemaHash: schemas[call.name],
						outcome: result.isError ? "failure" : "success",
						durationMs: tape.historical.get(call.id)?.wallMs ?? 0,
						...projectPatternAwareObservation(
							result,
							{ tool: call.name, input: call.arguments, failed: result.isError },
							tape.cwd,
						),
					};
				}),
			);
			store.observeTurn();
		}
		const tail = predict().length;
		score.falseProposals += tail;
		score.proposals += tail;
		store.finishSession(tape.session, true);
		return { score, startingPatterns };
	} finally {
		await lease.release();
	}
}

const tapes = await Promise.all(process.argv.slice(2).map(transcript));
const repositories = [...new Set(tapes.map((tape) => tape.repo))].sort();
if (repositories.length < 2 || new Set(tapes.map((t) => t.session)).size !== tapes.length)
	throw new Error("Provide distinct recorded sessions from at least two repositories");
const folds = {};
for (const held of repositories) {
	const state = await mkdtemp(path.join(os.tmpdir(), "pattern-fold-"));
	try {
		for (const tape of tapes.filter((t) => t.repo !== held)) await replay(tape, state);
		for (const tape of tapes.filter((t) => t.repo === held)) {
			const { score, startingPatterns } = await replay(tape, state);
			const total = (folds[held] ??= { startingPatterns });
			for (const [key, value] of Object.entries(score)) total[key] = (total[key] ?? 0) + value;
		}
	} finally {
		await rm(state, { recursive: true, force: true });
	}
}
console.log(JSON.stringify({ folds }, null, 2));
