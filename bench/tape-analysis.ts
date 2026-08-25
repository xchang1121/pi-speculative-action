import { stableStringify } from "../src/stable-json.ts";
import { ForkBenefitGate, type ForkBenefitGatePolicy } from "../src/fork-benefit-gate.ts";

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

export interface TapeToolCall {
	readonly name: string;
	readonly arguments: unknown;
}

export interface TapeOpportunity {
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
	const { completed, parsed } = parseTape(tape);
	const draftersByContext = groupBy(
		parsed.filter((exchange) => exchange.model === drafterModel),
		(exchange) => exchange.contextKey,
	);
	const opportunities = parsed
		.filter((exchange) => exchange.model === actorModel)
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

/** Replay the production rolling gate with the fastest-completing same-context Drafter as a D1 proxy. */
export function analyzeTapeForkGate(
	tape: LlmTape,
	actorModel: string,
	drafterModel: string,
	policy: ForkBenefitGatePolicy,
): TapeForkGateAnalysis {
	const { parsed } = parseTape(tape);
	const draftersByContext = groupBy(
		parsed.filter((exchange) => exchange.model === drafterModel),
		(exchange) => exchange.contextKey,
	);
	const gate = new ForkBenefitGate();
	let decisions = 0;
	let allowed = 0;
	let exactHitsAvailable = 0;
	let exactHitsRetained = 0;
	let forkCostMs = 0;
	let gatedForkCostMs = 0;
	let netBenefitMs = 0;
	let gatedNetBenefitMs = 0;
	for (const actor of parsed.filter((exchange) => exchange.model === actorModel)) {
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
		gate.observe(actorModel, { forkLatencyMs: proxy.endedAtMs, exactLeadMs }, policy);
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
	const { parsed } = parseTape(tape);
	const draftersByContext = groupBy(
		parsed.filter((exchange) => exchange.model === drafterModel),
		(exchange) => exchange.contextKey,
	);
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
	for (const actor of parsed.filter((exchange) => exchange.model === actorModel)) {
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

function parseTape(tape: LlmTape): { readonly completed: readonly TapeExchange[]; readonly parsed: readonly ParsedExchange[] } {
	const completed = tape.exchanges.filter((exchange) => exchange.response?.completed === true);
	const parsed = completed.flatMap((exchange) => {
		const body = record(exchange.request.descriptor.body);
		const model = string(body?.model);
		const endedAtMs = finiteMetric(exchange.response?.endedAtMs);
		if (!model || endedAtMs === undefined) return [];
		const chunks = exchange.response?.chunks ?? [];
		const stream = decodeStreamShape(chunks);
		return [
			{
				sequence: exchange.sequence,
				model,
				contextKey: stableStringify(body?.messages ?? []),
				endedAtMs,
				calls: decodeToolCalls(chunks),
				snapshotDeltaMs: stream.snapshotDeltaMs,
				toolDeltaMs: stream.toolDeltaMs,
			},
		];
	});
	return { completed, parsed };
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

function decodeToolCalls(chunks: readonly TapeChunk[]): readonly TapeToolCall[] {
	const calls = new Map<number, { name: string; arguments: string }>();
	for (const event of decodeSseEvents(chunks)) {
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

function decodeStreamShape(chunks: readonly TapeChunk[]): {
	readonly snapshotDeltaMs: readonly number[];
	readonly toolDeltaMs: readonly number[];
} {
	const snapshotDeltaMs: number[] = [];
	const toolDeltaMs: number[] = [];
	for (const event of decodeSseEvents(chunks)) {
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

function groupBy<Value>(values: readonly Value[], key: (value: Value) => string): Map<string, Value[]> {
	const grouped = new Map<string, Value[]>();
	for (const value of values) {
		const selected = key(value);
		const bucket = grouped.get(selected) ?? [];
		bucket.push(value);
		grouped.set(selected, bucket);
	}
	return grouped;
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
