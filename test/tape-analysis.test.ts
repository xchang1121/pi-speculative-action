import { describe, expect, it } from "vitest";
import {
	analyzeTape,
	analyzeTapeDrafterRace,
	analyzeTapeDrafterWidth,
	analyzeTapeForkGate,
	analyzeTapeReprobe,
	type LlmTape,
} from "../bench/tape-analysis.ts";

describe("LLM tape action analysis", () => {
	it("pairs exact contexts, deduplicates full K(a), and measures only early exact candidates", () => {
		const messages = [{ role: "user", content: "fix" }];
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, [call("read", '{"path":"a"}')]),
				exchange(1, "draft", messages, 30, [call("read", '{"path":"a"}')]),
				exchange(2, "draft", messages, 40, [call("read", '{"path":"a"}')]),
				exchange(3, "draft", messages, 20, [call("read", '{"path":"b"}')]),
				exchange(4, "draft", [{ role: "user", content: "other" }], 10, [call("read", '{"path":"a"}')]),
			],
		};

		const result = analyzeTape(tape, "actor", "draft");

		expect(result.summary).toEqual({
			opportunities: 1,
			exactHits: 1,
			hitRate: 1,
			exactReadyBeforeActor: 1,
			earlyHitRate: 1,
			candidateCount: 3,
			uniqueCandidateCount: 2,
			duplicateCandidateCount: 1,
			uniqueYield: 2 / 3,
			actorDecodeMs: 100,
			drafterServiceMs: 90,
			exactLeadMs: 70,
		});
		expect(result.opportunities[0]).toMatchObject({
			drafterSequences: [1, 2, 3],
			exactReadyBeforeActor: true,
			earliestExactReadyMs: 30,
		});
	});

	it("ignores malformed, incomplete, and non-tool responses", () => {
		const messages = [{ role: "user", content: "fix" }];
		const incomplete = exchange(1, "draft", messages, 10, [call("read", "{")]);
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, []),
				{ ...incomplete, response: { ...incomplete.response, completed: false } },
			],
		};

		expect(analyzeTape(tape, "actor", "draft")).toMatchObject({
			completedExchanges: 1,
			incompleteExchanges: 1,
			opportunities: [],
		});
	});

	it("replays the rolling gate against the fastest-completing same-context Drafter", () => {
		const exchanges: LlmTape["exchanges"][number][] = [];
		for (let index = 0; index < 7; index++) {
			const messages = [{ role: "user", content: `decision-${index}` }];
			exchanges.push(exchange(index * 2, "actor", messages, 100, [call("read", `{"path":"actor-${index}"}`)]));
			exchanges.push(exchange(index * 2 + 1, "draft", messages, 60, [call("read", `{"path":"miss-${index}"}`)]));
			if (index === 0)
				exchanges.push(exchange(1_000, "draft", messages, 10, [call("read", '{"path":"actor-0"}')]));
		}
		const result = analyzeTapeForkGate({ exchanges }, "actor", "draft", {
			enabled: true,
			minSamples: 4,
			windowSize: 4,
			minNetBenefitMs: 25,
			probeInterval: 4,
			failureThreshold: 2,
		});

		expect(result).toMatchObject({
			decisions: 7,
			allowed: 4,
			skipped: 3,
			exactHitsAvailable: 1,
			exactHitsRetained: 1,
			forkCostMs: 370,
			gatedForkCostMs: 190,
		});
	});

	it("measures bounded D2 recovery separately from later Actor snapshot runway", () => {
		const messages = [{ role: "user", content: "decision" }];
		const result = analyzeTapeReprobe(
			{
				exchanges: [
					exchange(0, "actor", messages, 120, [
						{ atMs: 10, data: content("thinking ") },
						{ atMs: 40, data: content("more") },
						{ atMs: 90, data: call("read", '{"path":"actor"}') },
					]),
					exchange(1, "draft", messages, 30, [call("read", '{"path":"miss"}')]),
					exchange(2, "draft", messages, 50, [call("read", '{"path":"actor"}')]),
					exchange(3, "draft", messages, 70, [call("read", '{"path":"later"}')]),
				],
			},
			"actor",
			"draft",
		);

		expect(result).toEqual({
			decisions: 1,
			actorActionTurns: 1,
			d1ExactHits: 0,
			d1Misses: 1,
			boundedReprobes: 1,
			secondProbeRecoveredHits: 1,
			anyLaterRecoveredHits: 1,
			additionalForkCostMs: 50,
			snapshotReprobeTurns: 1,
			snapshotReprobeActionTurns: 1,
			snapshotReprobeRunwayMs: 50,
		});
	});

	it("replays Drafter width in dispatch order and charges request costs once per Actor turn", () => {
		const messages = [{ role: "user", content: "decision" }];
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, [calls(["read", '{"path":"first"}'], ["write", '{"path":"second"}'])]),
				exchange(1, "draft", messages, 80, [call("read", '{"path":"first"}')]),
				exchange(2, "draft", messages, 10, [call("write", '{"path":"second"}')]),
				exchange(3, "draft", messages, 20, [call("read", '{"path":"first"}')]),
			],
		};

		const result = analyzeTapeDrafterWidth(tape, "actor", "draft", [3, 1, 2, 2, 0]);

		expect(result).toMatchObject({
			actorTurns: 1,
			opportunities: 2,
			availableDrafterRequests: 3,
			availableDrafterServiceMs: 110,
		});
		expect(result.points).toEqual([
			{
				width: 1,
				actorTurns: 1,
				opportunities: 2,
				exactHits: 1,
				marginalExactHits: 1,
				hitRate: 0.5,
				exactReadyBeforeActor: 1,
				earlyHitRate: 0.5,
				drafterRequests: 1,
				requestReductionFromAvailable: 2 / 3,
				drafterServiceMs: 80,
				serviceReductionFromAvailable: 30 / 110,
				drafterCompletionSpanMs: 80,
				candidateCount: 1,
				uniqueCandidateCount: 1,
				duplicateCandidateCount: 0,
				uniqueYield: 1,
				exactLeadMs: 20,
			},
			{
				width: 2,
				actorTurns: 1,
				opportunities: 2,
				exactHits: 2,
				marginalExactHits: 1,
				hitRate: 1,
				exactReadyBeforeActor: 2,
				earlyHitRate: 1,
				drafterRequests: 2,
				requestReductionFromAvailable: 1 / 3,
				drafterServiceMs: 90,
				serviceReductionFromAvailable: 20 / 110,
				drafterCompletionSpanMs: 80,
				candidateCount: 2,
				uniqueCandidateCount: 2,
				duplicateCandidateCount: 0,
				uniqueYield: 1,
				exactLeadMs: 110,
			},
			{
				width: 3,
				actorTurns: 1,
				opportunities: 2,
				exactHits: 2,
				marginalExactHits: 0,
				hitRate: 1,
				exactReadyBeforeActor: 2,
				earlyHitRate: 1,
				drafterRequests: 3,
				requestReductionFromAvailable: 0,
				drafterServiceMs: 110,
				serviceReductionFromAvailable: 0,
				drafterCompletionSpanMs: 80,
				candidateCount: 3,
				uniqueCandidateCount: 2,
				duplicateCandidateCount: 1,
				uniqueYield: 2 / 3,
				exactLeadMs: 170,
			},
		]);
	});

	it("rejects an empty static Drafter width grid", () => {
		expect(() => analyzeTapeDrafterWidth({ exchanges: [] }, "actor", "draft", [0, -1, 1.5])).toThrow(
			"At least one positive integer Drafter width is required",
		);
	});

	it("races the dispatch-selected Drafters and charges only residual service after a valid winner", () => {
		const messages = [{ role: "user", content: "decision" }];
		const tape: LlmTape = {
			exchanges: [
				exchange(0, "actor", messages, 100, [calls(["read", '{"path":"slow"}'], ["write", '{"path":"fast"}'])]),
				exchange(1, "draft", messages, 80, [call("read", '{"path":"slow"}')]),
				exchange(2, "draft", messages, 10, [call("write", '{"path":"fast"}')]),
				// Faster completion is outside width=2 and must not enter the race.
				exchange(3, "draft", messages, 1, [call("read", '{"path":"slow"}')]),
			],
		};

		expect(analyzeTapeDrafterRace(tape, "actor", "draft", 2)).toEqual({
			width: 2,
			actorTurns: 1,
			opportunities: 2,
			winnerTurns: 1,
			noWinnerTurns: 0,
			selectedDrafterRequests: 2,
			abortableDrafterRequests: 1,
			abortableRequestRate: 0.5,
			fullDrafterServiceMs: 90,
			racedDrafterServiceMs: 20,
			residualServiceSavedMs: 70,
			serviceReduction: 70 / 90,
			fullCandidateCount: 2,
			fullUniqueCandidateCount: 2,
			racedCandidateCount: 1,
			racedUniqueCandidateCount: 1,
			fullExactHits: 2,
			racedExactHits: 1,
			laterRecoveredExactHits: 1,
			fullExactReadyBeforeActor: 2,
			racedExactReadyBeforeActor: 1,
			fullExactLeadMs: 110,
			racedExactLeadMs: 90,
		});
	});

	it("does not let an empty response win or claim already-completed peers as abortable", () => {
		const messages = [{ role: "user", content: "decision" }];
		const result = analyzeTapeDrafterRace(
			{
				exchanges: [
					exchange(0, "actor", messages, 100, [call("read", '{"path":"winner"}')]),
					exchange(1, "draft", messages, 5, [content("no tool")]),
					exchange(2, "draft", messages, 20, [call("read", '{"path":"winner"}')]),
					exchange(3, "draft", messages, 50, [call("read", '{"path":"late"}')]),
				],
			},
			"actor",
			"draft",
			3,
		);

		expect(result).toMatchObject({
			winnerTurns: 1,
			abortableDrafterRequests: 1,
			fullDrafterServiceMs: 75,
			racedDrafterServiceMs: 45,
			residualServiceSavedMs: 30,
			fullCandidateCount: 2,
			racedCandidateCount: 1,
			fullExactHits: 1,
			racedExactHits: 1,
			laterRecoveredExactHits: 0,
		});
	});

	it("rejects an invalid Drafter race width", () => {
		expect(() => analyzeTapeDrafterRace({ exchanges: [] }, "actor", "draft", 0)).toThrow(
			"A positive integer Drafter race width is required",
		);
	});
});

type TapeFixtureChunk = string | { readonly atMs: number; readonly data: string };

function exchange(
	sequence: number,
	model: string,
	messages: unknown,
	endedAtMs: number,
	chunks: readonly TapeFixtureChunk[],
): LlmTape["exchanges"][number] {
	return {
		sequence,
		request: { descriptor: { body: { model, messages } } },
		response: {
			completed: true,
			endedAtMs,
			chunks: chunks.map((chunk) => {
				const data = typeof chunk === "string" ? chunk : chunk.data;
				return {
					...(typeof chunk === "string" ? {} : { atMs: chunk.atMs }),
					dataBase64: Buffer.from(data).toString("base64"),
				};
			}),
		},
	};
}

function content(value: string): string {
	return `data: ${JSON.stringify({ choices: [{ delta: { content: value } }] })}\n\n`;
}

function call(name: string, argumentsDelta: string): string {
	return `data: ${JSON.stringify({
		choices: [{ delta: { tool_calls: [{ index: 0, function: { name, arguments: argumentsDelta } }] } }],
	})}\n\ndata: [DONE]\n\n`;
}

function calls(...entries: readonly (readonly [name: string, argumentsDelta: string])[]): string {
	return `data: ${JSON.stringify({
		choices: [
			{
				delta: {
					tool_calls: entries.map(([name, argumentsDelta], index) => ({
						index,
						function: { name, arguments: argumentsDelta },
					})),
				},
			},
		],
	})}\n\ndata: [DONE]\n\n`;
}
