// Offline PatternAware replay over Actor transcripts kept by `run.ts --keep-session` (P1.4): every Actor batch is a
// decision; PatternAware proposes, the batch is scored (exact or covering action key), then observed with its results.
// Usage: tsx bench/pattern-replay.ts <session.jsonl...>   (PERSIST_FILE=<file> carries learning across invocations)
import { readFileSync } from "node:fs";
import { actionKeyCovers, PI_ACTION_SEMANTICS, READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-semantics.ts";
import { PatternAwareStore, patternAwareActionSemantics, patternAwareSettings } from "../src/pattern-aware.ts";
import { createPatternPlanSource } from "../src/pattern-plan-source.ts";

type Call = { readonly id: string; readonly tool: string; readonly input: Record<string, unknown> };
type Result = { readonly content: unknown[]; readonly details: unknown; readonly isError: boolean };
type Prediction = { readonly type: string; readonly tool: string; readonly input: Record<string, unknown>; readonly feedback?: unknown };
type Counts = { actual: number; covered: number; predicted: number; hits: number };

const sessions = process.argv.slice(2).map((file) => {
	const records = readFileSync(file, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line));
	const batches: Call[][] = [], results = new Map<string, Result>();
	for (const { message } of records) {
		if (message?.role === "assistant") {
			const calls = (message.content as { type: string; id: string; name: string; arguments: Record<string, unknown> }[])
				.filter((block) => block.type === "toolCall").map((block) => ({ id: block.id, tool: block.name, input: block.arguments }));
			if (calls.length) batches.push(calls);
		} else if (message?.role === "toolResult") {
			results.set(message.toolCallId, { content: message.content, details: message.details, isError: message.isError === true });
		}
	}
	return { cwd: String(records.find((record) => record.type === "session")?.cwd ?? process.cwd()), batches, results };
});
const tools = ["read", "grep", "find", "ls", "bash", "edit", "write"];
const settings = patternAwareSettings({ enabled: true, multiStepEnabled: true });
const runtimeSettings = { enabled: true, tools, sourceConfig: { patternAware: settings } } as never;
const data = { tools: new Map(tools.map((name) => [name, { name, parameters: {} }])), schemaHashes: Object.fromEntries(tools.map((name) => [name, "schema"])) };
const totals: Counts & { decisions: number } = { decisions: 0, actual: 0, covered: 0, predicted: 0, hits: 0 }, byTool: Record<string, Counts> = {};
const count = (tool: string) => byTool[tool] ??= { actual: 0, covered: 0, predicted: 0, hits: 0 };
let turn = 0;
for (const [index, { cwd, batches, results }] of sessions.entries()) {
	const rules = [READ_RANGE_ACTION_KEY_PROJECTOR], sessionID = `session-${index}`;
	const store = new PatternAwareStore(settings, process.env.PERSIST_FILE, patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd, rules));
	if (process.env.PERSIST_FILE) await store.load();
	const controller = createPatternPlanSource({ sessionID, cwd, store, actionSemantics: PI_ACTION_SEMANTICS, projectionRules: rules });
	const key = (tool: string, input: Record<string, unknown>) => { try { return PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, "schema"); } catch { return undefined; } };
	const request = (startInput: unknown) => ({ startInput, data, settings: runtimeSettings, definitions: [], candidateNames: tools, proposalIndex: 0, proposalCount: 1, signal: new AbortController().signal });
	let carried: Prediction[] = [], startInput: unknown;
	for (const batch of batches) {
		startInput = { sessionID, turnID: `turn-${++turn}`, tools: [], actorModel: {}, context: { systemPrompt: "", messages: [], tools: [] } };
		controller.turnStarted(startInput as never, runtimeSettings);
		const proposal = await controller.source.propose(request(startInput) as never);
		const predictions = [...carried, ...(proposal && "actions" in proposal ? proposal.actions as readonly Prediction[] : [])].filter((action) => action.type === "tool_call");
		const actual = batch.map((call) => key(call.tool, call.input)), covered = new Set<number>();
		totals.decisions++;
		for (const prediction of predictions) {
			const predicted = key(prediction.tool, prediction.input);
			const hit = actual.findIndex((action) => predicted && action && (predicted.key === action.key || actionKeyCovers(predicted, action, rules)));
			totals.predicted++; count(prediction.tool).predicted++;
			if (hit >= 0) { totals.hits++; count(prediction.tool).hits++; covered.add(hit); }
			if (!prediction.feedback) continue;
			// The runtime's feedback: each issued prediction settles as matched and adopted, or observed but unmatched.
			const identity = { proposalID: "p", actionID: "a", feedback: prediction.feedback };
			await controller.source.onIssued?.(identity as never);
			await controller.source.onSettled?.({ ...identity, settlement: { prediction: { id: "p", source: "pattern_aware", proposalID: "p", actionID: "a" }, observation: "observed",
				actorAction: { id: "actor", sequence: 0, turnID: `turn-${turn}` }, match: hit >= 0 ? { matched: true, relation: { kind: "exact", distance: 0 }, adoption: { status: "adopted", candidateID: "c" } } : { matched: false } } } as never);
		}
		for (const [order, call] of batch.entries()) {
			totals.actual++; count(call.tool).actual++;
			if (covered.has(order)) { totals.covered++; count(call.tool).covered++; }
			const result = results.get(call.id) ?? { content: [], details: undefined, isError: false };
			const update = await controller.source.observe!({ ...request(startInput), consumeInput: { sessionID, turnID: `turn-${turn}`, tool: call.tool, args: call.input, tools: [] },
				action: key(call.tool, call.input), tool: call.tool, concrete: call.input, output: { result: { content: result.content, details: result.details }, isError: result.isError },
				durationMs: 100, order } as never);
			if (update && "actions" in update) carried = (update.actions as readonly Prediction[]).filter((action) => action.type === "tool_call");
		}
		controller.turnFinished(startInput as never, runtimeSettings, false);
	}
	if (startInput) controller.turnFinished(startInput as never, runtimeSettings, true);
	if (process.env.PERSIST_FILE) await store.flush();
	await controller.finishSession();
	await controller.dispose();
}
const percent = (part: number, whole: number) => whole ? `${(100 * part / whole).toFixed(1)}%` : "-";
console.log(JSON.stringify({ ...totals, precision: percent(totals.hits, totals.predicted), recall: percent(totals.covered, totals.actual),
	byTool: Object.fromEntries(Object.entries(byTool).map(([tool, counts]) => [tool, { ...counts, precision: percent(counts.hits, counts.predicted), recall: percent(counts.covered, counts.actual) }])) }, null, 1));
