import { StringDecoder } from "node:string_decoder";
import { asRecord as record, stableStringify } from "../src/stable-json.ts";

export interface LlmTape {
	readonly format?: string;
	readonly exchanges: readonly {
		readonly sequence: number;
		readonly request: { readonly descriptor: { readonly body?: unknown } };
		readonly response?: {
			readonly status?: number | null;
			readonly error?: unknown;
			readonly endedAtMs?: number;
			readonly completed?: boolean;
			readonly chunks?: readonly { readonly dataBase64: string; readonly atMs?: number }[];
		};
	}[];
}

interface TapeToolCall {
	readonly name: string;
	readonly arguments: unknown;
}

/** Recorded Chat Completions evidence. The recorded format has request durations, not a shared arrival clock. */
export function analyzeTape(tape: LlmTape, actorModel: string, drafterModel: string) {
	if (actorModel === drafterModel) throw new Error("Distinct model IDs are required to identify Actor and Drafter requests");
	const sequences = new Set<number>();
	const requests = tape.exchanges.map((exchange) => {
		if (!Number.isSafeInteger(exchange.sequence) || exchange.sequence < 0 || sequences.has(exchange.sequence))
			throw new Error("Tape request sequences must be unique non-negative integers");
		sequences.add(exchange.sequence);
		const body = record(exchange.request.descriptor.body), response = exchange.response;
		const decoded = decodeResponse(response?.chunks ?? []);
		return {
			sequence: exchange.sequence,
			body: exchange.request.descriptor.body,
			model: typeof body?.model === "string" ? body.model : undefined,
			completed: response?.completed === true,
			status: response?.status,
			error: response?.error,
			serviceMs: finiteMetric(response?.endedAtMs),
			...decoded,
		};
	});
	const actors = requests.filter((request) => request.model === actorModel);
	const drafters = requests.filter((request) => request.model === drafterModel);
	type Request = typeof requests[number];
	const contextKey = (request: Request): string | undefined => {
		const body = record(request.body);
		if (!request.completed || request.error || request.malformedEvents || request.invalidCalls ||
			(request.status !== undefined && (request.status === null || request.status < 200 || request.status >= 300)) ||
			!Array.isArray(body?.messages)) return undefined;
		return stableStringify({ messages: body.messages, tools: body.tools, functions: body.functions });
	};
	// Index whole recorded batches once. Context equality permits comparison, not request ownership or reuse.
	const candidates = new Map<string, Map<string, Set<number>>>();
	let candidateCount = 0;
	for (const request of drafters) {
		const context = contextKey(request);
		if (context === undefined) continue;
		const actions = candidates.get(context) ?? new Map<string, Set<number>>();
		candidates.set(context, actions);
		for (const call of request.calls) {
			const key = stableStringify(call), producers = actions.get(key) ?? new Set<number>();
			producers.add(request.sequence);
			actions.set(key, producers);
			candidateCount++;
		}
	}
	const opportunities = actors.flatMap((actor) => {
		const context = contextKey(actor);
		if (context === undefined) return [];
		return actor.calls.map((actorAction) => ({
			actorSequence: actor.sequence,
			actorAction,
			drafterSequences: [...(candidates.get(context)?.get(stableStringify(actorAction)) ?? [])],
		}));
	});
	const exactMatches = opportunities.filter((value) => value.drafterSequences.length > 0).length;
	const uniqueCandidateCount = [...candidates.values()].reduce((total, actions) => total + actions.size, 0);
	const serviceMs = (values: readonly Request[]) => values.reduce((total, request) => total + (request.serviceMs ?? 0), 0);
	return {
		actorModel,
		drafterModel,
		completedExchanges: requests.filter((request) => request.completed).length,
		incompleteExchanges: requests.filter((request) => !request.completed).length,
		requests,
		opportunities,
		summary: {
			actorRequests: actors.length,
			drafterRequests: drafters.length,
			actorServiceMs: serviceMs(actors),
			drafterServiceMs: serviceMs(drafters),
			requestsWithoutDuration: [...actors, ...drafters].filter((request) => request.serviceMs === undefined).length,
			opportunities: opportunities.length,
			exactMatches,
			matchRate: opportunities.length ? exactMatches / opportunities.length : 0,
			candidateCount,
			uniqueCandidateCount,
			duplicateCandidateCount: candidateCount - uniqueCandidateCount,
			uniqueYield: candidateCount ? uniqueCandidateCount / candidateCount : 0,
		},
	};
}

function decodeResponse(chunks: readonly { readonly dataBase64: string }[]) {
	const calls = new Map<number, { name: string; arguments: string }>();
	const decoder = new StringDecoder("utf8");
	let buffered = "", malformedEvents = 0, invalidCalls = 0, usage: unknown;
	const append = (block: string) => {
		const data = block.split(/\r?\n/u).filter((line) => line.startsWith("data:"))
			.map((line) => line.slice(5).trim()).join("\n");
		if (!data || data === "[DONE]") return;
		let root;
		try { root = record(JSON.parse(data)); }
		catch { malformedEvents++; return; }
		if (!root) { malformedEvents++; return; }
		if (root.usage !== undefined && root.usage !== null) usage = root.usage;
		const choice = record(array(root.choices)[0]);
		const delta = record(choice?.delta) ?? record(choice?.message);
		for (const rawCall of array(delta?.tool_calls)) {
			const call = record(rawCall), fn = record(call?.function), index = call?.index ?? 0;
			if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 0 || !fn) { invalidCalls++; continue; }
			const current = calls.get(index) ?? { name: "", arguments: "" };
			for (const field of ["name", "arguments"] as const) {
				if (fn[field] !== undefined && typeof fn[field] !== "string") invalidCalls++;
				else current[field] += fn[field] ?? "";
			}
			calls.set(index, current);
		}
	};
	for (const chunk of chunks) {
		buffered += decoder.write(Buffer.from(chunk.dataBase64, "base64"));
		const blocks = buffered.split(/\r?\n\r?\n/u);
		buffered = blocks.pop() ?? "";
		for (const block of blocks) append(block);
	}
	buffered += decoder.end();
	if (buffered.trim()) append(buffered);
	const parsed: TapeToolCall[] = [];
	for (const [, call] of [...calls].sort(([left], [right]) => left - right)) {
		if (!call.name.trim()) { invalidCalls++; continue; }
		try { parsed.push({ name: call.name, arguments: JSON.parse(call.arguments || "{}") }); }
		catch { invalidCalls++; }
	}
	return { calls: parsed, usage, malformedEvents, invalidCalls };
}

function array(value: unknown): readonly unknown[] {
	return Array.isArray(value) ? value : [];
}

function finiteMetric(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}
