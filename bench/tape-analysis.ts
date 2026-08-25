import { stableStringify } from "../src/stable-json.ts";

interface TapeChunk {
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

interface ParsedExchange {
	readonly sequence: number;
	readonly model: string;
	readonly contextKey: string;
	readonly endedAtMs: number;
	readonly calls: readonly TapeToolCall[];
}

export function analyzeTape(tape: LlmTape, actorModel: string, drafterModel: string): TapeAnalysis {
	const completed = tape.exchanges.filter((exchange) => exchange.response?.completed === true);
	const parsed = completed.flatMap((exchange) => {
		const body = record(exchange.request.descriptor.body);
		const model = string(body?.model);
		const endedAtMs = finiteMetric(exchange.response?.endedAtMs);
		if (!model || endedAtMs === undefined) return [];
		return [
			{
				sequence: exchange.sequence,
				model,
				contextKey: stableStringify(body?.messages ?? []),
				endedAtMs,
				calls: decodeToolCalls(exchange.response?.chunks ?? []),
			},
		];
	});
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
	const text = chunks.map((chunk) => Buffer.from(chunk.dataBase64, "base64").toString("utf8")).join("");
	for (const block of text.split(/\r?\n\r?\n/u)) {
		const data = block
			.split(/\r?\n/u)
			.filter((line) => line.startsWith("data:"))
			.map((line) => line.slice(5).trim())
			.join("\n");
		if (!data || data === "[DONE]") continue;
		let event: unknown;
		try {
			event = JSON.parse(data);
		} catch {
			continue;
		}
		const root = record(event);
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

function integer(value: unknown): number | undefined {
	return typeof value === "number" && Number.isSafeInteger(value) ? value : undefined;
}

function finiteMetric(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}
