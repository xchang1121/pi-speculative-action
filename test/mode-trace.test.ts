import { describe, expect, it } from "vitest";
import type { CandidateEventDescriptor, SpeculativeActionEvent } from "../src/events.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import { formatSpeculativeActionStatus, normalizeSpeculativeActionSettings } from "../src/extension.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { emptySpeculativeTraceSummary, reduceSpeculativeTrace, summarizeSpeculativeTrace, type SpeculativeTraceSummary } from "../src/trace-summary.ts";

const envelope = { sessionID: "session", turnID: "turn", timestamp: 0, cache: emptySpeculativeTraceSummary().cache };
const candidate = (mode: string, id = "candidate"): CandidateEventDescriptor => ({
	id, mode, origin: "prediction", tool: "read", source: "pattern_aware", depth: 1, predictedAction: "read file.ts",
	route: { isolation: "resource_snapshot", reuse: "shared_result", scope: "fallback", backend: "resource_version", fingerprint: "test" },
});

function prediction(mode: string, matched: boolean, adopted: boolean, operation = false): SpeculativeActionEvent<string> {
	const observed = {
		prediction: { id: mode, source: "pattern_aware", proposalID: "plan", actionID: mode },
		observation: "observed" as const, actorAction: { id: "actor", sequence: 1, turnID: "turn" },
	};
	return { ...envelope, type: operation ? "operation_prediction" : "prediction", mode, settlement: matched ? {
		...observed, match: { matched: true, relation: { kind: "exact", distance: 0 }, adoption: adopted
			? { status: "adopted", candidateID: "candidate" }
			: { status: "rejected", candidateID: "candidate", cause: { stage: "freshness", code: "changed" } } },
	} : { ...observed, match: { matched: false } } };
}

function actor(mode: string, sequence: number, hiddenComputeMs?: number, inputs = false): Extract<SpeculativeActionEvent<string>, { type: "actor_action" }> {
	return { ...envelope, type: "actor_action", candidate: candidate(mode), actualAction: "read file.ts",
		...(hiddenComputeMs === undefined ? {} : { computation: { toolComputeMs: 120, hiddenComputeMs,
			hiddenByMode: [{ source: "pattern_aware", mode, hiddenComputeMs }] } }),
		settlement: {
			actorAction: { id: `actor-${sequence}`, sequence, turnID: "turn" }, tool: "read", matchedPredictions: [], rejections: [],
			provider: { kind: "speculative", candidateID: "candidate", match: inputs ? { kind: "inputs", distance: 0 } : { kind: "exact", distance: 0 },
				toolExecution: new TimelineInterval(10, 1010) },
		},
	};
}

describe("per-mode trace results", () => {
	it("preserves incomplete computation evidence across later complete tasks in both live and replay summaries", () => {
		const events: SpeculativeActionEvent<string>[] = [true, false].map(incomplete => ({ ...envelope, type: "task",
			timing: { startedAt: 0, completedAt: 20, toolComputeMs: 30, hiddenComputeMs: 20, toolWaitMs: 20,
				...(incomplete ? { hiddenComputeIncomplete: true as const } : {}) } }));
		const summary = summarizeSpeculativeTrace(events);
		expect(summary).toMatchObject({ toolComputeMs: 60, hiddenComputeMs: 40, hiddenComputeIncomplete: true });
		expect(events.reduce(reduceSpeculativeTrace, emptySpeculativeTraceSummary())).toEqual(summary);
	});

	it("keeps prediction support separate from one execution owner and credits repeated real consumption", () => {
		const events: SpeculativeActionEvent<string>[] = [
			{ ...envelope, type: "candidate", candidate: candidate("recheck-search"), state: { status: "running" } },
			{ ...envelope, type: "candidate", candidate: candidate("recheck-search"), state: { status: "succeeded", executionMs: 12 } },
			prediction("recheck-search", true, true), prediction("result-neighbors", true, true),
			actor("recheck-search", 1, 40), actor("recheck-search", 2, 40),
		];
		const summary = summarizeSpeculativeTrace(events);
		expect(summary.modesBySource.pattern_aware).toEqual({
			"recheck-search": { observed: 1, matched: 1, adopted: 1, started: 1, productionMs: 12, hiddenComputeMs: 80 },
			"result-neighbors": { observed: 1, matched: 1, adopted: 1, started: 0, productionMs: 0, hiddenComputeMs: 0 },
		});
		const live = events.reduce<SpeculativeTraceSummary>((current, event) => {
			for (const modes of Object.values(current.modesBySource)) {
				for (const result of Object.values(modes)) Object.freeze(result);
				Object.freeze(modes);
			}
			Object.freeze(current.modesBySource); Object.freeze(current);
			return reduceSpeculativeTrace(current, event);
		}, emptySpeculativeTraceSummary());
		expect(live).toEqual(summary);
		const status = formatSpeculativeActionStatus({ settings: normalizeSpeculativeActionSettings(undefined),
			metrics: { ...summary, actorProcessReuse: emptyWorldReuseMetrics() } });
		expect(status).toContain("Mode results: Recheck search: 1/1 matched, 1 adopted; 80ms hidden, 12ms production wall (1 started)");
		expect(status).toContain("Result neighbors: 1/1 matched, 1 adopted; 0ms hidden, 0ms production wall (0 started)");
	});

	it("uses measured reconstruction benefit and never treats the original result or adoption time as saved computation", () => {
		const summary = summarizeSpeculativeTrace([actor("reported-files", 1, 9, true), actor("reported-files", 2, undefined, true)]);
		expect(summary.modesBySource.pattern_aware?.["reported-files"]).toEqual({
			observed: 0, matched: 0, adopted: 0, started: 0, productionMs: 0, hiddenComputeMs: 9,
		});
	});

	it("attributes fallback consumption from its receipt breakdown and leaves missing provenance unassigned", () => {
		const selected = actor("selected-result", 1, 45);
		const summary = summarizeSpeculativeTrace([
			{ ...selected, candidate: undefined, computation: { toolComputeMs: 57, hiddenComputeMs: 45, hiddenByMode: [
				{ source: "pattern_aware", mode: "child", hiddenComputeMs: 30 },
				{ source: "other", mode: "preparation", hiddenComputeMs: 10 },
			] } },
			{ ...selected, computation: { toolComputeMs: 45, hiddenComputeMs: 45 } },
		]);
		expect(summary.modesBySource.pattern_aware?.child?.hiddenComputeMs).toBe(30);
		expect(summary.modesBySource.other?.preparation?.hiddenComputeMs).toBe(10);
		expect(summary.modesBySource.pattern_aware?.["selected-result"]).toBeUndefined();
	});

	it("includes failed and cancelled production costs and operation outcomes without crediting rejected results", () => {
		const events: SpeculativeActionEvent<string>[] = [
			...(["failed", "cancelled"] as const).flatMap((status, index): SpeculativeActionEvent<string>[] => [
				{ ...envelope, type: "candidate", candidate: candidate("recent-command", status), state: { status: "running" } },
				{ ...envelope, type: "candidate", candidate: candidate("recent-command", status),
					state: { status, executionMs: 15 + index * 5, cause: { stage: "execution", code: status } } },
			]),
			prediction("recent-command", true, false, true), prediction("recent-command", false, false, true),
		];
		expect(summarizeSpeculativeTrace(events).modesBySource.pattern_aware?.["recent-command"]).toEqual({
			observed: 2, matched: 1, adopted: 0, started: 2, productionMs: 35, hiddenComputeMs: 0,
		});
	});

	it("isolates equal mode labels by source and clamps invalid measured costs", () => {
		const events: SpeculativeActionEvent<string>[] = [
			{ ...envelope, type: "candidate", candidate: candidate("shared-label"), state: { status: "succeeded", executionMs: 7 } },
			{ ...envelope, type: "candidate", candidate: { ...candidate("shared-label"), source: "other" }, state: { status: "succeeded", executionMs: Infinity } },
			{ ...envelope, type: "candidate", candidate: { ...candidate("preview"), origin: "actor_preview" }, state: { status: "succeeded", executionMs: 99 } },
		];
		const modes = summarizeSpeculativeTrace(events).modesBySource;
		expect(modes.pattern_aware?.["shared-label"]?.productionMs).toBe(7);
		expect(modes.other?.["shared-label"]?.productionMs).toBe(0);
		expect(modes.pattern_aware?.preview).toBeUndefined();
	});
});
