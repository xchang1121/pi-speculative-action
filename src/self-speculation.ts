import { randomUUID } from "node:crypto";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import type { MaterializedSpeculativeCandidate } from "./runtime.ts";

export type SelfSpeculationForkTransport = "provider" | "sidecar";

export interface SelfSpeculationSettingsInput {
	readonly enabled?: boolean;
	/** Trusted control-plane endpoint exposed by the inference runtime. */
	readonly endpoint?: string;
	/** Top-level field carrying the stable request ID in provider payloads. */
	readonly requestIDField?: string;
	readonly candidatePath?: string;
	readonly forkPath?: string;
	readonly clearPath?: string;
	readonly timeoutMs?: number;
	readonly maxCandidates?: number;
	readonly maxDraftTokens?: number;
	/** Tool-call body format used when concrete K(a) candidates are tokenized. */
	readonly draftFormat?: string;
	/** Exact target-model boundary preceding a boundary-relative action draft. */
	readonly draftBoundary?: string;
	/** Optional environment variable containing a bearer token for the control plane. */
	readonly apiKeyEnv?: string;
	readonly forkEnabled?: boolean;
	readonly forkTransport?: SelfSpeculationForkTransport;
	readonly forkMaxTokens?: number;
	readonly forkTemperature?: number;
	readonly forkDecoder?: string;
	readonly forkForcedPrefix?: string;
	/** Require a capable engine to expose token logprobs to its SPORK fork. */
	readonly requireLogprobs?: boolean;
	/** Apply the same provider-side self-fork control to Drafter requests. */
	readonly drafterEnabled?: boolean;
}

export interface SelfSpeculationSettings {
	readonly enabled: boolean;
	readonly endpoint: string;
	readonly requestIDField: string;
	readonly candidatePath: string;
	readonly forkPath: string;
	readonly clearPath: string;
	readonly timeoutMs: number;
	readonly maxCandidates: number;
	readonly maxDraftTokens: number;
	readonly draftFormat: string;
	readonly draftBoundary: string;
	readonly apiKeyEnv?: string;
	readonly forkEnabled: boolean;
	readonly forkTransport: SelfSpeculationForkTransport;
	readonly forkMaxTokens: number;
	readonly forkTemperature: number;
	readonly forkDecoder: string;
	readonly forkForcedPrefix: string;
	readonly requireLogprobs: boolean;
	readonly drafterEnabled: boolean;
}

export const SELF_SPECULATION_DEFAULTS: SelfSpeculationSettings = Object.freeze({
	enabled: false,
	endpoint: "http://127.0.0.1:8000",
	requestIDField: "request_id",
	candidatePath: "/self-speculation/candidates",
	forkPath: "/self-speculation/fork",
	clearPath: "/self-speculation/clear",
	timeoutMs: 2_000,
	maxCandidates: 8,
	maxDraftTokens: 20,
	draftFormat: "tagged_json",
	draftBoundary: "<tool_call>",
	forkEnabled: true,
	forkTransport: "provider",
	forkMaxTokens: 128,
	forkTemperature: 0,
	forkDecoder: "auto",
	forkForcedPrefix: "<tool_call>",
	requireLogprobs: false,
	drafterEnabled: true,
});

export function normalizeSelfSpeculationSettings(value: unknown): SelfSpeculationSettings {
	const input = isRecord(value) ? value : {};
	const endpoint = nonEmptyString(input.endpoint) ?? SELF_SPECULATION_DEFAULTS.endpoint;
	const apiKeyEnv = nonEmptyString(input.apiKeyEnv);
	return {
		enabled: booleanOr(input.enabled, SELF_SPECULATION_DEFAULTS.enabled),
		endpoint: endpoint.replace(/\/+$/u, ""),
		requestIDField: nonEmptyString(input.requestIDField) ?? SELF_SPECULATION_DEFAULTS.requestIDField,
		candidatePath: httpPath(input.candidatePath, SELF_SPECULATION_DEFAULTS.candidatePath),
		forkPath: httpPath(input.forkPath, SELF_SPECULATION_DEFAULTS.forkPath),
		clearPath: httpPath(input.clearPath, SELF_SPECULATION_DEFAULTS.clearPath),
		timeoutMs: positiveInteger(input.timeoutMs, SELF_SPECULATION_DEFAULTS.timeoutMs),
		maxCandidates: positiveInteger(input.maxCandidates, SELF_SPECULATION_DEFAULTS.maxCandidates),
		maxDraftTokens: positiveInteger(input.maxDraftTokens, SELF_SPECULATION_DEFAULTS.maxDraftTokens),
		draftFormat: nonEmptyString(input.draftFormat) ?? SELF_SPECULATION_DEFAULTS.draftFormat,
		draftBoundary: nonEmptyString(input.draftBoundary) ?? SELF_SPECULATION_DEFAULTS.draftBoundary,
		...(apiKeyEnv ? { apiKeyEnv } : {}),
		forkEnabled: booleanOr(input.forkEnabled, SELF_SPECULATION_DEFAULTS.forkEnabled),
		forkTransport: input.forkTransport === "sidecar" ? "sidecar" : "provider",
		forkMaxTokens: positiveInteger(input.forkMaxTokens, SELF_SPECULATION_DEFAULTS.forkMaxTokens),
		forkTemperature: nonNegativeNumber(input.forkTemperature, SELF_SPECULATION_DEFAULTS.forkTemperature),
		forkDecoder: nonEmptyString(input.forkDecoder) ?? SELF_SPECULATION_DEFAULTS.forkDecoder,
		forkForcedPrefix: nonEmptyString(input.forkForcedPrefix) ?? SELF_SPECULATION_DEFAULTS.forkForcedPrefix,
		requireLogprobs: booleanOr(input.requireLogprobs, SELF_SPECULATION_DEFAULTS.requireLogprobs),
		drafterEnabled: booleanOr(input.drafterEnabled, SELF_SPECULATION_DEFAULTS.drafterEnabled),
	};
}

export interface SelfSpeculationCoordinatorSnapshot {
	readonly actorRequestID?: string;
	readonly bufferedCandidates: number;
	readonly candidateSubmissions: number;
	readonly forkRequests: number;
	readonly failures: number;
	readonly lastError?: string;
}

export interface SelfSpeculationCoordinatorOptions {
	readonly settings: () => SelfSpeculationSettings;
	readonly fetch?: typeof globalThis.fetch;
	readonly requestID?: () => string;
}

interface TurnState {
	readonly turnID: string;
	readonly decisionSequence: number;
	readonly model: Model<Api>;
	readonly context: Context;
	readonly settings: SelfSpeculationSettings;
	readonly candidates: Map<string, CandidateRecord>;
	requestID?: string;
	requestBound: boolean;
	forkRequested: boolean;
	content: string;
	reasoning: string;
	outputChunks: number;
	dirty: boolean;
	flushTask?: Promise<void>;
	forkTask?: Promise<void>;
	providerPayload?: unknown;
}

interface CandidateRecord {
	readonly key: string;
	readonly hash: string;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
	readonly sources: Set<string>;
	readonly provenance: Array<{ readonly proposalID: string; readonly actionID: string }>;
	readonly sequence: number;
	readonly expectedDecisionSequence: number;
	depth: number;
	horizon: number;
	latestDecisionSequence: number;
	conditionalProbability: number;
	empiricalProbability: number;
	expectedLatencyBenefitMs: number;
	expectedDurationMs: number;
}

type ProviderRole = "actor" | "drafter";

/**
 * Request-scoped bridge between speculative-action predictions and a SPORK-capable engine.
 * Network work is serialized and best-effort; it never owns Actor correctness or lifecycle.
 */
export class SelfSpeculationCoordinator {
	private readonly settings: () => SelfSpeculationSettings;
	private readonly fetch: typeof globalThis.fetch;
	private readonly requestID: () => string;
	private readonly background = new Set<Promise<void>>();
	private readonly pendingCandidates = new Map<number, Map<string, CandidateRecord>>();
	private active?: TurnState;
	private latestStartedDecisionSequence = 0;
	private acceptingCandidates = false;
	private candidateSequence = 0;
	private submissions = 0;
	private forks = 0;
	private failureCount = 0;
	private lastFailure?: string;

	constructor(options: SelfSpeculationCoordinatorOptions) {
		this.settings = options.settings;
		this.fetch = options.fetch ?? globalThis.fetch;
		this.requestID = options.requestID ?? randomUUID;
	}

	startTurn(turnID: string, model: Model<Api>, context: Context, decisionSequence: number): void {
		this.closeActive(true);
		const settings = this.settings();
		if (!settings.enabled || !Number.isSafeInteger(decisionSequence) || decisionSequence < 1) {
			this.pendingCandidates.clear();
			this.acceptingCandidates = false;
			return;
		}
		this.acceptingCandidates = true;
		this.latestStartedDecisionSequence = decisionSequence;
		for (const target of this.pendingCandidates.keys()) {
			if (target < decisionSequence) this.pendingCandidates.delete(target);
		}
		const candidates = this.pendingCandidates.get(decisionSequence) ?? new Map();
		this.pendingCandidates.delete(decisionSequence);
		this.active = {
			turnID,
			decisionSequence,
			model,
			context: serializableContext(context),
			settings,
			candidates,
			requestBound: false,
			forkRequested: false,
			content: "",
			reasoning: "",
			outputChunks: 0,
			dirty: candidates.size > 0,
		};
	}

	/** Bind exactly one non-Drafter provider request to the current speculative turn. */
	decorateActorPayload(payload: unknown): unknown {
		const state = this.active;
		if (!state || state.requestBound) return payload;
		const settings = state.settings;
		const existing = isRecord(payload) ? nonEmptyString(payload[settings.requestIDField]) : undefined;
		state.requestID = existing ?? this.requestID();
		state.requestBound = true;
		state.providerPayload = cloneSerializable(payload);
		this.scheduleFlush(state);
		return providerPayload(payload, settings, state.requestID, "actor");
	}

	decorateDrafterPayload(payload: unknown): unknown {
		const settings = this.settings();
		if (
			!settings.enabled ||
			!settings.forkEnabled ||
			!settings.drafterEnabled ||
			settings.forkTransport !== "provider"
		)
			return payload;
		return providerPayload(payload, settings, this.requestID(), "drafter");
	}

	actorRequestID(): string | undefined {
		return this.active?.requestID;
	}

	addCandidate(candidate: MaterializedSpeculativeCandidate<string>): void {
		const state = this.active;
		if (!this.settings().enabled || !this.acceptingCandidates) return;
		const targetDecisionSequence = candidate.expectedDecisionSequence;
		if (!Number.isSafeInteger(targetDecisionSequence) || targetDecisionSequence < 1) return;
		if (state && targetDecisionSequence < state.decisionSequence) return;
		if (!state && targetDecisionSequence < this.latestStartedDecisionSequence) return;
		const candidates =
			state && targetDecisionSequence === state.decisionSequence
				? state.candidates
				: this.pendingCandidates.get(targetDecisionSequence) ?? new Map<string, CandidateRecord>();
		if (candidates !== state?.candidates) this.pendingCandidates.set(targetDecisionSequence, candidates);
		this.mergeCandidate(candidates, candidate);
		if (candidates !== state?.candidates || !state) return;
		state.dirty = true;
		this.scheduleFlush(state);
	}

	private mergeCandidate(
		candidates: Map<string, CandidateRecord>,
		candidate: MaterializedSpeculativeCandidate<string>,
	): void {
		const existing = candidates.get(candidate.action.key);
		const source = candidate.source || "unknown";
		if (existing) {
			existing.sources.add(source);
			if (!existing.provenance.some((item) => item.proposalID === candidate.proposalID && item.actionID === candidate.actionID)) {
				existing.provenance.push({ proposalID: candidate.proposalID, actionID: candidate.actionID });
			}
			existing.depth = Math.min(existing.depth, metric(candidate.depth, 0));
			existing.horizon = Math.min(existing.horizon, metric(candidate.horizon, 0));
			existing.latestDecisionSequence = Math.max(
				existing.latestDecisionSequence,
				candidate.latestDecisionSequence,
			);
			existing.conditionalProbability = Math.max(
				existing.conditionalProbability,
				metric(candidate.conditionalProbability, 0),
			);
			existing.empiricalProbability = Math.max(
				existing.empiricalProbability,
				metric(candidate.empiricalProbability, 0),
			);
			existing.expectedLatencyBenefitMs = Math.max(
				existing.expectedLatencyBenefitMs,
				metric(candidate.expectedLatencyBenefitMs, 0),
			);
			existing.expectedDurationMs = Math.max(
				existing.expectedDurationMs,
				metric(candidate.expectedDurationMs, 0),
			);
		} else {
			candidates.set(candidate.action.key, {
				key: candidate.action.key,
				hash: candidate.action.hash,
				tool: candidate.tool,
				input: structuredClone(candidate.input),
				sources: new Set([source]),
				provenance: [{ proposalID: candidate.proposalID, actionID: candidate.actionID }],
				sequence: this.candidateSequence++,
				expectedDecisionSequence: candidate.expectedDecisionSequence,
				depth: metric(candidate.depth, 0),
				horizon: metric(candidate.horizon, 0),
				latestDecisionSequence: candidate.latestDecisionSequence,
				conditionalProbability: metric(candidate.conditionalProbability, 0),
				empiricalProbability: metric(candidate.empiricalProbability, 0),
				expectedLatencyBenefitMs: metric(candidate.expectedLatencyBenefitMs, 0),
				expectedDurationMs: metric(candidate.expectedDurationMs, 0),
			});
		}
	}

	observeActorOutput(event: AssistantMessageEvent): void {
		const state = this.active;
		if (!state || !state.settings.forkEnabled || state.forkRequested) return;
		const settings = state.settings;
		if (event.type === "text_delta") state.content += event.delta;
		else if (event.type === "thinking_delta") state.reasoning += event.delta;
		else return;
		state.outputChunks++;
		if (!event.delta || !state.requestID) return;
		state.forkRequested = true;
		if (settings.forkTransport !== "sidecar") return;
		this.forks++;
		state.forkTask = this.post(
			settings.forkPath,
			{
				version: 1,
				request_id: state.requestID,
				model: modelPayload(state.model),
				context: contextPayload(state.context, state.providerPayload),
				snapshot: {
					generated_text: state.reasoning + state.content,
					content: state.content,
					reasoning: state.reasoning,
					chunk_count: state.outputChunks,
					output_chunk_count: state.outputChunks,
				},
				options: forkPayload(settings),
			},
			settings,
		)
			.then(() => undefined)
			.finally(() => {
				state.forkTask = undefined;
			});
		this.track(state.forkTask);
	}

	endTurn(): void {
		this.closeActive(true);
	}

	/** Clear both the active request and every future-decision candidate. */
	reset(): void {
		this.closeActive(false);
		this.pendingCandidates.clear();
		this.latestStartedDecisionSequence = 0;
		this.acceptingCandidates = false;
	}

	private closeActive(preserveForRetry: boolean): void {
		const state = this.active;
		this.active = undefined;
		if (state && preserveForRetry && state.candidates.size) {
			const retained = this.pendingCandidates.get(state.decisionSequence) ?? new Map<string, CandidateRecord>();
			mergeCandidateRecords(retained, state.candidates.values());
			this.pendingCandidates.set(state.decisionSequence, retained);
		}
		if (!state?.requestID) return;
		const pending = [state.flushTask, state.forkTask].filter(
			(task): task is Promise<void> => task !== undefined,
		);
		const cleanup = Promise.allSettled(pending)
			.then(() =>
				this.post(
					state.settings.clearPath,
					{ version: 1, request_id: state.requestID },
					state.settings,
				),
			)
			.then(() => undefined);
		this.track(cleanup);
	}

	snapshot(): SelfSpeculationCoordinatorSnapshot {
		return {
			...(this.active?.requestID ? { actorRequestID: this.active.requestID } : {}),
			bufferedCandidates:
				(this.active?.candidates.size ?? 0) +
				[...this.pendingCandidates.values()].reduce((total, candidates) => total + candidates.size, 0),
			candidateSubmissions: this.submissions,
			forkRequests: this.forks,
			failures: this.failureCount,
			...(this.lastFailure ? { lastError: this.lastFailure } : {}),
		};
	}

	async dispose(): Promise<void> {
		this.reset();
		while (this.background.size) await Promise.allSettled([...this.background]);
	}

	private scheduleFlush(state: TurnState): void {
		if (!state.requestID || state.flushTask) return;
		state.flushTask = this.flush(state).finally(() => {
			state.flushTask = undefined;
			if (state.dirty && state.requestID && this.active === state) this.scheduleFlush(state);
		});
		this.track(state.flushTask);
	}

	private async flush(state: TurnState): Promise<void> {
		while (state.dirty && state.requestID) {
			state.dirty = false;
			const settings = state.settings;
			const candidates = rankedCandidates(state.candidates.values()).slice(0, settings.maxCandidates);
			if (!candidates.length) continue;
			await this.post(
				settings.candidatePath,
				{
					version: 1,
					request_id: state.requestID,
					model: modelPayload(state.model),
					max_draft_tokens: settings.maxDraftTokens,
					format: settings.draftFormat,
					boundary: settings.draftBoundary,
					candidates: candidates.map(candidatePayload),
				},
				settings,
			);
			this.submissions++;
		}
	}

	private async post(
		path: string,
		payload: Readonly<Record<string, unknown>>,
		settings: SelfSpeculationSettings = this.settings(),
	): Promise<unknown> {
		const controller = new AbortController();
		const timeout = setTimeout(() => controller.abort(), settings.timeoutMs);
		try {
			const apiKey = settings.apiKeyEnv ? process.env[settings.apiKeyEnv] : undefined;
			const response = await this.fetch(`${settings.endpoint}${path}`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
				},
				body: JSON.stringify(payload),
				signal: controller.signal,
			});
			if (!response.ok) throw new Error(`self-speculation control plane returned HTTP ${response.status}`);
			return response.status === 204 ? undefined : await response.json().catch(() => undefined);
		} catch (error) {
			this.failureCount++;
			this.lastFailure = error instanceof Error ? error.message : String(error);
			throw error;
		} finally {
			clearTimeout(timeout);
		}
	}

	private track(task: Promise<void>): void {
		this.background.add(task);
		void task
			.catch(() => undefined)
			.finally(() => this.background.delete(task));
	}
}

function providerPayload(
	payload: unknown,
	settings: SelfSpeculationSettings,
	requestID: string,
	role: ProviderRole,
): unknown {
	if (!isRecord(payload)) return payload;
	const identified = {
		...payload,
		[settings.requestIDField]: requestID,
	};
	if (settings.forkTransport === "sidecar") return identified;
	return {
		...identified,
		self_speculation: {
			version: 1,
			role,
			fork: role === "actor" ? settings.forkEnabled : settings.forkEnabled && settings.drafterEnabled,
			fork_transport: settings.forkTransport,
			max_draft_tokens: settings.maxDraftTokens,
			draft_format: settings.draftFormat,
			draft_boundary: settings.draftBoundary,
			fork_max_tokens: settings.forkMaxTokens,
			fork_temperature: settings.forkTemperature,
			fork_decoder: settings.forkDecoder,
			fork_forced_prefix: settings.forkForcedPrefix,
			require_logprobs: settings.requireLogprobs,
		},
	};
}

function forkPayload(settings: SelfSpeculationSettings): Readonly<Record<string, unknown>> {
	return {
		max_tokens: settings.forkMaxTokens,
		temperature: settings.forkTemperature,
		decoder: settings.forkDecoder,
		forced_prefix: settings.forkForcedPrefix,
		require_logprobs: settings.requireLogprobs,
		max_draft_tokens: settings.maxDraftTokens,
		draft_format: settings.draftFormat,
		draft_boundary: settings.draftBoundary,
	};
}

function candidatePayload(candidate: CandidateRecord): Readonly<Record<string, unknown>> {
	return {
		id: candidate.hash,
		sources: [...candidate.sources].sort(),
		provenance: candidate.provenance,
		tool_call: { name: candidate.tool, arguments: candidate.input },
		score: {
			depth: candidate.depth,
			horizon: candidate.horizon,
			expected_decision_sequence: candidate.expectedDecisionSequence,
			latest_decision_sequence: candidate.latestDecisionSequence,
			conditional_probability: candidate.conditionalProbability,
			empirical_probability: candidate.empiricalProbability,
			expected_latency_benefit_ms: candidate.expectedLatencyBenefitMs,
			expected_duration_ms: candidate.expectedDurationMs,
		},
	};
}

function mergeCandidateRecords(target: Map<string, CandidateRecord>, records: Iterable<CandidateRecord>): void {
	for (const record of records) {
		const existing = target.get(record.key);
		if (!existing) {
			target.set(record.key, {
				...record,
				input: structuredClone(record.input),
				sources: new Set(record.sources),
				provenance: record.provenance.map((item) => ({ ...item })),
			});
			continue;
		}
		for (const source of record.sources) existing.sources.add(source);
		for (const item of record.provenance) {
			if (!existing.provenance.some((value) => value.proposalID === item.proposalID && value.actionID === item.actionID))
				existing.provenance.push({ ...item });
		}
		existing.depth = Math.min(existing.depth, record.depth);
		existing.horizon = Math.min(existing.horizon, record.horizon);
		existing.latestDecisionSequence = Math.max(
			existing.latestDecisionSequence,
			record.latestDecisionSequence,
		);
		existing.conditionalProbability = Math.max(existing.conditionalProbability, record.conditionalProbability);
		existing.empiricalProbability = Math.max(existing.empiricalProbability, record.empiricalProbability);
		existing.expectedLatencyBenefitMs = Math.max(
			existing.expectedLatencyBenefitMs,
			record.expectedLatencyBenefitMs,
		);
		existing.expectedDurationMs = Math.max(existing.expectedDurationMs, record.expectedDurationMs);
	}
}

function rankedCandidates(candidates: Iterable<CandidateRecord>): CandidateRecord[] {
	return [...candidates].sort(
		(left, right) =>
			left.horizon - right.horizon ||
			right.conditionalProbability - left.conditionalProbability ||
			right.empiricalProbability - left.empiricalProbability ||
			right.expectedLatencyBenefitMs - left.expectedLatencyBenefitMs ||
			right.expectedDurationMs - left.expectedDurationMs ||
			left.depth - right.depth ||
			left.sequence - right.sequence,
	);
}

function serializableContext(context: Context): Context {
	return {
		...context,
		messages: structuredClone(context.messages),
		tools: context.tools?.map((tool) => ({
			name: tool.name,
			description: tool.description,
			parameters: structuredClone(tool.parameters),
		})),
	};
}

function contextPayload(context: Context, providerPayload?: unknown): Readonly<Record<string, unknown>> {
	return {
		system_prompt: context.systemPrompt,
		messages: context.messages,
		tools: context.tools?.map((tool) => ({
			name: tool.name,
			description: tool.description,
			parameters: tool.parameters,
		})),
		...(providerPayload !== undefined ? { provider_payload: providerPayload } : {}),
	};
}

function cloneSerializable(value: unknown): unknown {
	try {
		return structuredClone(value);
	} catch {
		return undefined;
	}
}

function modelPayload(model: Model<Api>): Readonly<Record<string, unknown>> {
	return { provider: model.provider, api: model.api, id: model.id };
}

function metric(value: number | undefined, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function httpPath(value: unknown, fallback: string): string {
	const selected = nonEmptyString(value);
	return selected?.startsWith("/") ? selected : fallback;
}

function positiveInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function nonNegativeNumber(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : fallback;
}

function booleanOr(value: unknown, fallback: boolean): boolean {
	return typeof value === "boolean" ? value : fallback;
}

function nonEmptyString(value: unknown): string | undefined {
	return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
