import { stableStringify } from "../src/stable-json.ts";
import { BenefitGate, type BenefitGatePolicy } from "../src/fork-benefit-gate.ts";

interface TapeChunk {
	readonly atMs?: number;
	readonly dataBase64: string;
}

interface TapeExchange {
	readonly sequence: number;
	readonly request: {
		readonly descriptor: {
			readonly body?: unknown;
		};
	};
	readonly response?: {
		readonly endedAtMs?: number;
		readonly completed?: boolean;
		readonly chunks?: readonly TapeChunk[];
	};
}

export interface LlmTape {
	readonly format?: string;
	readonly exchanges: readonly TapeExchange[];
}

interface TapeToolCall {
	readonly name: string;
	readonly arguments: unknown;
}

interface TapeOpportunity {
	readonly actorSequence: number;
	readonly actorAction: TapeToolCall;
	readonly actorDecodeMs: number;
	readonly drafterSequences: readonly number[];
	readonly drafterRequestCount: number;
	readonly candidateCount: number;
	readonly uniqueCandidateCount: number;
	readonly duplicateCandidateCount: number;
	readonly exactHit: boolean;
	readonly exactReadyBeforeActor: boolean;
	readonly earliestExactReadyMs?: number;
	readonly exactLeadMs: number;
	readonly drafterServiceMs: number;
}

export interface TapeAnalysis {
	readonly actorModel: string;
	readonly drafterModel: string;
	readonly completedExchanges: number;
	readonly incompleteExchanges: number;
	readonly opportunities: readonly TapeOpportunity[];
	readonly summary: {
		readonly opportunities: number;
		readonly exactHits: number;
		readonly hitRate: number;
		readonly exactReadyBeforeActor: number;
		readonly earlyHitRate: number;
		readonly candidateCount: number;
		readonly uniqueCandidateCount: number;
		readonly duplicateCandidateCount: number;
		readonly uniqueYield: number;
		readonly actorDecodeMs: number;
		readonly drafterServiceMs: number;
		readonly exactLeadMs: number;
	};
}

export interface TapeForkGateAnalysis {
	readonly decisions: number;
	readonly allowed: number;
	readonly skipped: number;
	readonly requestReduction: number;
	readonly exactHitsAvailable: number;
	readonly exactHitsRetained: number;
	readonly forkCostMs: number;
	readonly gatedForkCostMs: number;
	readonly forkCostReduction: number;
	readonly netBenefitMs: number;
	readonly gatedNetBenefitMs: number;
}

export interface TapeReprobeAnalysis {
	readonly decisions: number;
	readonly actorActionTurns: number;
	readonly d1ExactHits: number;
	readonly d1Misses: number;
	readonly boundedReprobes: number;
	readonly secondProbeRecoveredHits: number;
	readonly anyLaterRecoveredHits: number;
	readonly additionalForkCostMs: number;
	readonly snapshotReprobeTurns: number;
	readonly snapshotReprobeActionTurns: number;
	readonly snapshotReprobeRunwayMs: number;
}

interface TapeDrafterWidthPoint {
	readonly width: number;
	readonly actorTurns: number;
	readonly opportunities: number;
	readonly exactHits: number;
	readonly marginalExactHits: number;
	readonly hitRate: number;
	readonly exactReadyBeforeActor: number;
	readonly earlyHitRate: number;
	readonly drafterRequests: number;
	readonly requestReductionFromAvailable: number;
	readonly drafterServiceMs: number;
	readonly serviceReductionFromAvailable: number;
	readonly drafterCompletionSpanMs: number;
	readonly candidateCount: number;
	readonly uniqueCandidateCount: number;
	readonly duplicateCandidateCount: number;
	readonly uniqueYield: number;
	readonly exactLeadMs: number;
}

export interface TapeDrafterWidthAnalysis {
	readonly actorTurns: number;
	readonly opportunities: number;
	readonly availableDrafterRequests: number;
	readonly availableDrafterServiceMs: number;
	readonly points: readonly TapeDrafterWidthPoint[];
}

export interface TapeDrafterRaceAnalysis {
	readonly width: number;
	readonly actorTurns: number;
	readonly opportunities: number;
	readonly winnerTurns: number;
	readonly noWinnerTurns: number;
	readonly selectedDrafterRequests: number;
	readonly abortableDrafterRequests: number;
	readonly abortableRequestRate: number;
	readonly fullDrafterServiceMs: number;
	readonly racedDrafterServiceMs: number;
	readonly residualServiceSavedMs: number;
	readonly serviceReduction: number;
	readonly fullCandidateCount: number;
	readonly fullUniqueCandidateCount: number;
	readonly racedCandidateCount: number;
	readonly racedUniqueCandidateCount: number;
	readonly fullExactHits: number;
	readonly racedExactHits: number;
	readonly laterRecoveredExactHits: number;
	readonly fullExactReadyBeforeActor: number;
	readonly racedExactReadyBeforeActor: number;
	readonly fullExactLeadMs: number;
	readonly racedExactLeadMs: number;
}

interface ParsedExchange {
	readonly sequence: number;
	readonly model: string;
	readonly contextKey: string;
	readonly endedAtMs: number;
	readonly calls: readonly TapeToolCall[];
	readonly snapshotDeltaMs: readonly number[];
	readonly toolDeltaMs: readonly number[];
}

export function analyzeTape(tape: LlmTape, actorModel: string, drafterModel: string): TapeAnalysis {
	const { completed, actors, draftersByContext } = pairTape(tape, actorModel, drafterModel);
	const opportunities = actors
		.flatMap((actor) =>
			actor.calls.map((actorAction) =>
				opportunity(actor, actorAction, draftersByContext.get(actor.contextKey) ?? []),
			),
		);
	const exactHits = opportunities.filter((value) => value.exactHit).length;
	const earlyHits = opportunities.filter((value) => value.exactReadyBeforeActor).length;
	const candidateCount = sum(opportunities, (value) => value.candidateCount);
	const uniqueCandidateCount = sum(opportunities, (value) => value.uniqueCandidateCount);
	return {
		actorModel,
		drafterModel,
		completedExchanges: completed.length,
		incompleteExchanges: tape.exchanges.length - completed.length,
		opportunities,
		summary: {
			opportunities: opportunities.length,
			exactHits,
			hitRate: ratio(exactHits, opportunities.length),
			exactReadyBeforeActor: earlyHits,
			earlyHitRate: ratio(earlyHits, opportunities.length),
			candidateCount,
			uniqueCandidateCount,
			duplicateCandidateCount: candidateCount - uniqueCandidateCount,
			uniqueYield: ratio(uniqueCandidateCount, candidateCount),
			actorDecodeMs: sum(opportunities, (value) => value.actorDecodeMs),
			drafterServiceMs: sum(opportunities, (value) => value.drafterServiceMs),
			exactLeadMs: sum(opportunities, (value) => value.exactLeadMs),
		},
	};
}

/** Simulate the rolling policy using decode lead as a proxy, not measured Actor savings. */
export function analyzeTapeForkGate(
	tape: LlmTape,
	actorModel: string,
	drafterModel: string,
	policy: BenefitGatePolicy,
): TapeForkGateAnalysis {
	const { actors, draftersByContext } = pairTape(tape, actorModel, drafterModel);
	const gate = new BenefitGate();
	let decisions = 0;
	let allowed = 0;
	let exactHitsAvailable = 0;
	let exactHitsRetained = 0;
	let forkCostMs = 0;
	let gatedForkCostMs = 0;
	let netBenefitMs = 0;
	let gatedNetBenefitMs = 0;
	for (const actor of actors) {
		const proxy = [...(draftersByContext.get(actor.contextKey) ?? [])].sort(
			(left, right) => left.endedAtMs - right.endedAtMs || left.sequence - right.sequence,
		)[0];
		if (!proxy) continue;
		decisions++;
		const exact = proxy.calls.some((candidate) =>
			actor.calls.some((actual) => actionIdentity(candidate) === actionIdentity(actual)),
		);
		const exactLeadMs = exact ? Math.max(0, actor.endedAtMs - proxy.endedAtMs) : 0;
		const net = exactLeadMs - proxy.endedAtMs;
		forkCostMs += proxy.endedAtMs;
		netBenefitMs += net;
		if (exact) exactHitsAvailable++;
		const decision = gate.decide(actorModel, policy);
		if (!decision.allowed) continue;
		allowed++;
		gatedForkCostMs += proxy.endedAtMs;
		gatedNetBenefitMs += net;
		if (exact) exactHitsRetained++;
		gate.observe(actorModel, { costMs: proxy.endedAtMs, benefitMs: exactLeadMs }, policy);
	}
	return {
		decisions,
		allowed,
		skipped: decisions - allowed,
		requestReduction: ratio(decisions - allowed, decisions),
		exactHitsAvailable,
		exactHitsRetained,
		forkCostMs,
		gatedForkCostMs,
		forkCostReduction: ratio(forkCostMs - gatedForkCostMs, forkCostMs),
		netBenefitMs,
		gatedNetBenefitMs,
	};
}

/** Measure whether one D2 retry could recover a D1 miss and whether Actor stream runway exists. */
export function analyzeTapeReprobe(
	tape: LlmTape,
	actorModel: string,
	drafterModel: string,
): TapeReprobeAnalysis {
	const { actors, draftersByContext } = pairTape(tape, actorModel, drafterModel);
	let decisions = 0;
	let actorActionTurns = 0;
	let d1ExactHits = 0;
	let boundedReprobes = 0;
	let secondProbeRecoveredHits = 0;
	let anyLaterRecoveredHits = 0;
	let additionalForkCostMs = 0;
	let snapshotReprobeTurns = 0;
	let snapshotReprobeActionTurns = 0;
	let snapshotReprobeRunwayMs = 0;
	for (const actor of actors) {
		const drafters = [...(draftersByContext.get(actor.contextKey) ?? [])].sort(
			(left, right) => left.endedAtMs - right.endedAtMs || left.sequence - right.sequence,
		);
		if (!drafters.length) continue;
		decisions++;
		if (actor.calls.length) actorActionTurns++;
		const actual = new Set(actor.calls.map(actionIdentity));
		const exact = (candidate: ParsedExchange): boolean =>
			candidate.calls.some((call) => actual.has(actionIdentity(call)));
		if (exact(drafters[0])) {
			d1ExactHits++;
		} else if (drafters.length > 1) {
			boundedReprobes++;
			additionalForkCostMs += drafters[1].endedAtMs;
			if (exact(drafters[1])) secondProbeRecoveredHits++;
			if (drafters.slice(1).some(exact)) anyLaterRecoveredHits++;
		}

		const actionBoundaryMs = actor.toolDeltaMs.length ? Math.min(...actor.toolDeltaMs) : actor.endedAtMs;
		const snapshots = actor.snapshotDeltaMs.filter((atMs) => atMs < actionBoundaryMs);
		if (snapshots.length < 2) continue;
		snapshotReprobeTurns++;
		if (actor.calls.length) snapshotReprobeActionTurns++;
		snapshotReprobeRunwayMs += Math.max(0, actionBoundaryMs - snapshots[1]);
	}
	return {
		decisions,
		actorActionTurns,
		d1ExactHits,
		d1Misses: decisions - d1ExactHits,
		boundedReprobes,
		secondProbeRecoveredHits,
		anyLaterRecoveredHits,
		additionalForkCostMs,
		snapshotReprobeTurns,
		snapshotReprobeActionTurns,
		snapshotReprobeRunwayMs,
	};
}

/**
 * Replay a static Drafter request cap in dispatch order.
 *
 * Request/service costs are charged once per Actor turn, even when the Actor
 * emits multiple tool calls. Exact coverage remains action-scoped.
 */
export function analyzeTapeDrafterWidth(
	tape: LlmTape,
	actorModel: string,
	drafterModel: string,
	widths: readonly number[],
): TapeDrafterWidthAnalysis {
	const selectedWidths = [...new Set(widths.filter((width) => Number.isSafeInteger(width) && width > 0))].sort(
		(left, right) => left - right,
	);
	if (!selectedWidths.length) throw new Error("At least one positive integer Drafter width is required");

	const turns = drafterTurns(tape, actorModel, drafterModel);
	const available = summarizeCandidates(turns);
	const actionOpportunities = sum(turns, ({ actor }) => actor.calls.length);
	let previousExactHits = 0;
	const points = selectedWidths.map((width): TapeDrafterWidthPoint => {
		const metrics = summarizeCandidates(turns.map(({ actor, drafters }) => ({ actor, drafters: drafters.slice(0, width) })));
		const point = {
			width,
			actorTurns: turns.length,
			opportunities: actionOpportunities,
			...metrics,
			marginalExactHits: metrics.exactHits - previousExactHits,
			hitRate: ratio(metrics.exactHits, actionOpportunities),
			earlyHitRate: ratio(metrics.exactReadyBeforeActor, actionOpportunities),
			requestReductionFromAvailable: ratio(available.drafterRequests - metrics.drafterRequests, available.drafterRequests),
			serviceReductionFromAvailable: ratio(available.drafterServiceMs - metrics.drafterServiceMs, available.drafterServiceMs),
			duplicateCandidateCount: metrics.candidateCount - metrics.uniqueCandidateCount,
			uniqueYield: ratio(metrics.uniqueCandidateCount, metrics.candidateCount),
		};
		previousExactHits = metrics.exactHits;
		return point;
	});
	return {
		actorTurns: turns.length,
		opportunities: actionOpportunities,
		availableDrafterRequests: available.drafterRequests,
		availableDrafterServiceMs: available.drafterServiceMs,
		points,
	};
}

/**
 * Replay a hedged Drafter race at a fixed dispatch width. The first completed
 * response containing tools wins as a whole batch; empty responses do not cancel
 * peers. Requests launch together, so only service after the winner completes is
 * counterfactually removable. Costs are per request and coverage is per action.
 */
export function analyzeTapeDrafterRace(
	tape: LlmTape,
	actorModel: string,
	drafterModel: string,
	width: number,
): TapeDrafterRaceAnalysis {
	if (!Number.isSafeInteger(width) || width <= 0) throw new Error("A positive integer Drafter race width is required");
	const turns = drafterTurns(tape, actorModel, drafterModel)
		.map(({ actor, drafters }) => ({ actor, drafters: drafters.slice(0, width) }));
	let winnerTurns = 0;
	let abortableDrafterRequests = 0;
	let racedDrafterServiceMs = 0;
	const winners = turns.map(({ actor, drafters }) => {
		const winner = drafters.filter(exchange => exchange.calls.length > 0)
			.sort((left, right) => left.endedAtMs - right.endedAtMs || left.sequence - right.sequence)[0];
		if (winner) winnerTurns++;
		const boundary = winner?.endedAtMs ?? Infinity;
		racedDrafterServiceMs += sum(drafters, exchange => Math.min(exchange.endedAtMs, boundary));
		abortableDrafterRequests += drafters.filter(exchange => exchange.endedAtMs > boundary).length;
		return { actor, drafters: winner ? [winner] : [] };
	});
	const full = summarizeCandidates(turns), raced = summarizeCandidates(winners);
	const residualServiceSavedMs = full.drafterServiceMs - racedDrafterServiceMs;
	return {
		width,
		actorTurns: turns.length,
		opportunities: sum(turns, ({ actor }) => actor.calls.length),
		winnerTurns,
		noWinnerTurns: turns.length - winnerTurns,
		selectedDrafterRequests: full.drafterRequests,
		abortableDrafterRequests,
		abortableRequestRate: ratio(abortableDrafterRequests, full.drafterRequests),
		fullDrafterServiceMs: full.drafterServiceMs,
		racedDrafterServiceMs,
		residualServiceSavedMs,
		serviceReduction: ratio(residualServiceSavedMs, full.drafterServiceMs),
		fullCandidateCount: full.candidateCount,
		fullUniqueCandidateCount: full.uniqueCandidateCount,
		racedCandidateCount: raced.candidateCount,
		racedUniqueCandidateCount: raced.uniqueCandidateCount,
		fullExactHits: full.exactHits,
		racedExactHits: raced.exactHits,
		laterRecoveredExactHits: full.exactHits - raced.exactHits,
		fullExactReadyBeforeActor: full.exactReadyBeforeActor,
		racedExactReadyBeforeActor: raced.exactReadyBeforeActor,
		fullExactLeadMs: full.exactLeadMs,
		racedExactLeadMs: raced.exactLeadMs,
	};
}

interface DrafterTurn {
	readonly actor: ParsedExchange;
	readonly drafters: readonly ParsedExchange[];
}

function drafterTurns(tape: LlmTape, actorModel: string, drafterModel: string): DrafterTurn[] {
	const { actors, draftersByContext } = pairTape(tape, actorModel, drafterModel);
	for (const drafters of draftersByContext.values()) drafters.sort((left, right) => left.sequence - right.sequence);
	return actors.filter(exchange => exchange.calls.length > 0).flatMap(actor => {
		const drafters = draftersByContext.get(actor.contextKey) ?? [];
		return drafters.length ? [{ actor, drafters }] : [];
	});
}

/** Index the earliest whole response per action; duplicates consume candidates but not extra coverage. */
function summarizeCandidates(turns: readonly DrafterTurn[]) {
	const metrics = {
		exactHits: 0, exactReadyBeforeActor: 0, exactLeadMs: 0,
		drafterRequests: 0, drafterServiceMs: 0, drafterCompletionSpanMs: 0,
		candidateCount: 0, uniqueCandidateCount: 0,
	};
	for (const { actor, drafters } of turns) {
		const ready = new Map<string, number>();
		for (const drafter of drafters) for (const call of drafter.calls) {
			const identity = actionIdentity(call);
			ready.set(identity, Math.min(ready.get(identity) ?? Infinity, drafter.endedAtMs));
			metrics.candidateCount++;
		}
		metrics.drafterRequests += drafters.length;
		metrics.drafterServiceMs += sum(drafters, exchange => exchange.endedAtMs);
		metrics.drafterCompletionSpanMs += Math.max(0, ...drafters.map(exchange => exchange.endedAtMs));
		metrics.uniqueCandidateCount += ready.size;
		for (const actual of actor.calls) {
			const atMs = ready.get(actionIdentity(actual));
			if (atMs === undefined) continue;
			metrics.exactHits++;
			const lead = Math.max(0, actor.endedAtMs - atMs);
			if (lead > 0) metrics.exactReadyBeforeActor++;
			metrics.exactLeadMs += lead;
		}
	}
	return metrics;
}

function pairTape(tape: LlmTape, actorModel: string, drafterModel: string) {
	const completed = tape.exchanges.filter((exchange) => exchange.response?.completed === true);
	const actors: ParsedExchange[] = [], draftersByContext = new Map<string, ParsedExchange[]>();
	for (const exchange of completed) {
		const body = record(exchange.request.descriptor.body);
		const model = string(body?.model), endedAtMs = finiteMetric(exchange.response?.endedAtMs);
		if (!model || endedAtMs === undefined) continue;
		const events = decodeSseEvents(exchange.response?.chunks ?? []);
		const parsed = {
			sequence: exchange.sequence, model, endedAtMs,
			contextKey: stableStringify(body?.messages ?? []),
			calls: decodeToolCalls(events), ...decodeStreamShape(events),
		};
		if (model === actorModel) actors.push(parsed);
		if (model === drafterModel) {
			const drafters = draftersByContext.get(parsed.contextKey) ?? [];
			drafters.push(parsed);
			draftersByContext.set(parsed.contextKey, drafters);
		}
	}
	return { completed, actors, draftersByContext };
}

function opportunity(
	actor: ParsedExchange,
	actorAction: TapeToolCall,
	drafters: readonly ParsedExchange[],
): TapeOpportunity {
	const candidates = drafters.flatMap((exchange) => exchange.calls);
	const unique = new Map(candidates.map((candidate) => [actionIdentity(candidate), candidate]));
	const actorIdentity = actionIdentity(actorAction);
	const exact = drafters.filter((exchange) =>
		exchange.calls.some((candidate) => actionIdentity(candidate) === actorIdentity),
	);
	const earliestExactReadyMs = exact.length ? Math.min(...exact.map((exchange) => exchange.endedAtMs)) : undefined;
	const exactLeadMs = earliestExactReadyMs === undefined ? 0 : Math.max(0, actor.endedAtMs - earliestExactReadyMs);
	return {
		actorSequence: actor.sequence,
		actorAction,
		actorDecodeMs: actor.endedAtMs,
		drafterSequences: drafters.map((exchange) => exchange.sequence),
		drafterRequestCount: drafters.length,
		candidateCount: candidates.length,
		uniqueCandidateCount: unique.size,
		duplicateCandidateCount: candidates.length - unique.size,
		exactHit: exact.length > 0,
		exactReadyBeforeActor: exactLeadMs > 0,
		...(earliestExactReadyMs === undefined ? {} : { earliestExactReadyMs }),
		exactLeadMs,
		drafterServiceMs: sum(drafters, (exchange) => exchange.endedAtMs),
	};
}

function decodeToolCalls(events: readonly DecodedSseEvent[]): readonly TapeToolCall[] {
	const calls = new Map<number, { name: string; arguments: string }>();
	for (const event of events) {
		const root = event.value;
		const choice = record(array(root?.choices)[0]);
		const delta = record(choice?.delta) ?? record(choice?.message);
		for (const rawCall of array(delta?.tool_calls)) {
			const call = record(rawCall);
			const index = integer(call?.index) ?? 0;
			const fn = record(call?.function);
			const current = calls.get(index) ?? { name: "", arguments: "" };
			current.name += string(fn?.name) ?? "";
			current.arguments += string(fn?.arguments) ?? "";
			calls.set(index, current);
		}
	}
	return [...calls]
		.sort(([left], [right]) => left - right)
		.flatMap(([, call]) => {
			if (!call.name.trim()) return [];
			try {
				return [{ name: call.name, arguments: JSON.parse(call.arguments || "{}") }];
			} catch {
				return [];
			}
		});
}

function decodeStreamShape(events: readonly DecodedSseEvent[]): {
	readonly snapshotDeltaMs: readonly number[];
	readonly toolDeltaMs: readonly number[];
} {
	const snapshotDeltaMs: number[] = [];
	const toolDeltaMs: number[] = [];
	for (const event of events) {
		if (event.atMs === undefined) continue;
		const choice = record(array(event.value.choices)[0]);
		const delta = record(choice?.delta) ?? record(choice?.message);
		if (!delta) continue;
		if (
			(nonEmptyString(delta.content) ?? nonEmptyString(delta.reasoning_content) ?? nonEmptyString(delta.reasoning)) !==
			undefined
		)
			snapshotDeltaMs.push(event.atMs);
		if (array(delta.tool_calls).length) toolDeltaMs.push(event.atMs);
	}
	return { snapshotDeltaMs, toolDeltaMs };
}

interface DecodedSseEvent {
	readonly atMs?: number;
	readonly value: Readonly<Record<string, unknown>>;
}

function decodeSseEvents(chunks: readonly TapeChunk[]): readonly DecodedSseEvent[] {
	const events: DecodedSseEvent[] = [];
	let buffered = "";
	let latestAtMs: number | undefined;
	for (const chunk of chunks) {
		buffered += Buffer.from(chunk.dataBase64, "base64").toString("utf8");
		latestAtMs = finiteMetric(chunk.atMs) ?? latestAtMs;
		const blocks = buffered.split(/\r?\n\r?\n/u);
		buffered = blocks.pop() ?? "";
		for (const block of blocks) appendDecodedSseEvent(events, block, latestAtMs);
	}
	if (buffered.trim()) appendDecodedSseEvent(events, buffered, latestAtMs);
	return events;
}

function appendDecodedSseEvent(target: DecodedSseEvent[], block: string, atMs: number | undefined): void {
	const data = block
		.split(/\r?\n/u)
		.filter((line) => line.startsWith("data:"))
		.map((line) => line.slice(5).trim())
		.join("\n");
	if (!data || data === "[DONE]") return;
	try {
		const value = record(JSON.parse(data));
		if (value) target.push({ ...(atMs === undefined ? {} : { atMs }), value });
	} catch {
		// Malformed or truncated events are intentionally ignored by the strict analyzer.
	}
}

function actionIdentity(call: TapeToolCall): string {
	return stableStringify({ tool: call.name, input: call.arguments });
}

function sum<Value>(values: readonly Value[], value: (item: Value) => number): number {
	return values.reduce((total, item) => total + value(item), 0);
}

function ratio(numerator: number, denominator: number): number {
	return denominator > 0 ? numerator / denominator : 0;
}

function record(value: unknown): Record<string, unknown> | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function array(value: unknown): readonly unknown[] {
	return Array.isArray(value) ? value : [];
}

function string(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
	const selected = string(value);
	return selected?.length ? selected : undefined;
}

function integer(value: unknown): number | undefined {
	return typeof value === "number" && Number.isSafeInteger(value) ? value : undefined;
}

function finiteMetric(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}
