import { randomUUID } from "node:crypto";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import {
	DEFAULT_BENEFIT_GATE_POLICY,
	ForkBenefitGate,
	type ForkBenefitGatePolicy,
} from "./fork-benefit-gate.ts";
import type { MaterializedSpeculativeCandidate } from "./runtime.ts";
import type { SelfSpeculationActionBridge, SelfSpeculationActionCandidate } from "./self-speculation-action-bridge.ts";

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
	/** Admit complete sidecar fork tool calls to the ordinary speculative-action runtime. */
	readonly forkActionEnabled?: boolean;
	readonly forkTransport?: SelfSpeculationForkTransport;
	readonly forkMaxTokens?: number;
	readonly forkTemperature?: number;
	readonly forkDecoder?: string;
	readonly forkForcedPrefix?: string;
	/** Require a capable engine to expose token logprobs to its SPORK fork. */
	readonly requireLogprobs?: boolean;
	/** Apply the same provider-side self-fork control to Drafter requests. */
	readonly drafterEnabled?: boolean;
	readonly forkGateEnabled?: boolean;
	readonly forkGateMinSamples?: number;
	readonly forkGateWindowSize?: number;
	readonly forkGateMinNetBenefitMs?: number;
	readonly forkGateProbeInterval?: number;
	readonly forkGateFailureThreshold?: number;
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
	readonly forkActionEnabled: boolean;
	readonly forkTransport: SelfSpeculationForkTransport;
	readonly forkMaxTokens: number;
	readonly forkTemperature: number;
	readonly forkDecoder: string;
	readonly forkForcedPrefix: string;
	readonly requireLogprobs: boolean;
	readonly drafterEnabled: boolean;
	readonly forkGateEnabled: boolean;
	readonly forkGateMinSamples: number;
	readonly forkGateWindowSize: number;
	readonly forkGateMinNetBenefitMs: number;
	readonly forkGateProbeInterval: number;
	readonly forkGateFailureThreshold: number;
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
	maxDraftTokens: 28,
	draftFormat: "tagged_json",
	draftBoundary: "<tool_call>",
	forkEnabled: true,
	forkActionEnabled: true,
	forkTransport: "provider",
	forkMaxTokens: 128,
	forkTemperature: 0,
	forkDecoder: "auto",
	forkForcedPrefix: "<tool_call>",
	requireLogprobs: false,
	drafterEnabled: true,
	forkGateEnabled: DEFAULT_BENEFIT_GATE_POLICY.enabled,
	forkGateMinSamples: DEFAULT_BENEFIT_GATE_POLICY.minSamples,
	forkGateWindowSize: DEFAULT_BENEFIT_GATE_POLICY.windowSize,
	forkGateMinNetBenefitMs: DEFAULT_BENEFIT_GATE_POLICY.minNetBenefitMs,
	forkGateProbeInterval: DEFAULT_BENEFIT_GATE_POLICY.probeInterval,
	forkGateFailureThreshold: DEFAULT_BENEFIT_GATE_POLICY.failureThreshold,
});

export function normalizeSelfSpeculationSettings(value: unknown): SelfSpeculationSettings {
	const input = isRecord(value) ? value : {};
	const endpoint = nonEmptyString(input.endpoint) ?? SELF_SPECULATION_DEFAULTS.endpoint;
	const apiKeyEnv = nonEmptyString(input.apiKeyEnv);
	const forkGateMinSamples = positiveInteger(
		input.forkGateMinSamples,
		SELF_SPECULATION_DEFAULTS.forkGateMinSamples,
	);
	const forkGateWindowSize = Math.max(
		forkGateMinSamples,
		positiveInteger(input.forkGateWindowSize, SELF_SPECULATION_DEFAULTS.forkGateWindowSize),
	);
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
		forkActionEnabled: booleanOr(input.forkActionEnabled, SELF_SPECULATION_DEFAULTS.forkActionEnabled),
		forkTransport: input.forkTransport === "sidecar" ? "sidecar" : "provider",
		forkMaxTokens: positiveInteger(input.forkMaxTokens, SELF_SPECULATION_DEFAULTS.forkMaxTokens),
		forkTemperature: nonNegativeNumber(input.forkTemperature, SELF_SPECULATION_DEFAULTS.forkTemperature),
		forkDecoder: nonEmptyString(input.forkDecoder) ?? SELF_SPECULATION_DEFAULTS.forkDecoder,
		forkForcedPrefix: nonEmptyString(input.forkForcedPrefix) ?? SELF_SPECULATION_DEFAULTS.forkForcedPrefix,
		requireLogprobs: booleanOr(input.requireLogprobs, SELF_SPECULATION_DEFAULTS.requireLogprobs),
		drafterEnabled: booleanOr(input.drafterEnabled, SELF_SPECULATION_DEFAULTS.drafterEnabled),
		forkGateEnabled: booleanOr(input.forkGateEnabled, SELF_SPECULATION_DEFAULTS.forkGateEnabled),
		forkGateMinSamples,
		forkGateWindowSize,
		forkGateMinNetBenefitMs: nonNegativeNumber(
			input.forkGateMinNetBenefitMs,
			SELF_SPECULATION_DEFAULTS.forkGateMinNetBenefitMs,
		),
		forkGateProbeInterval: positiveInteger(
			input.forkGateProbeInterval,
			SELF_SPECULATION_DEFAULTS.forkGateProbeInterval,
		),
		forkGateFailureThreshold: positiveInteger(
			input.forkGateFailureThreshold,
			SELF_SPECULATION_DEFAULTS.forkGateFailureThreshold,
		),
	};
}

export interface SelfSpeculationCoordinatorSnapshot {
	readonly actorRequestID?: string;
	readonly bufferedCandidates: number;
	readonly candidateSubmissions: number;
	readonly forkRequests: number;
	readonly candidateReceipts: number;
	readonly forkCompletions: number;
	readonly forkCandidates: number;
	readonly forkAgreements: number;
	readonly forkExactMatches: number;
	readonly submittedDraftTokens: number;
	/** Registration acknowledgements; not necessarily target-model acceptance. */
	readonly acceptedDraftTokens: number;
	readonly verificationRequests: number;
	readonly verifiedDraftProposals: number;
	readonly verifiedDraftTokens: number;
	readonly verifiedAcceptedDraftTokens: number;
	readonly verifiedRejectedDraftTokens: number;
	readonly unresolvedDraftProposals: number;
	readonly unresolvedDraftTokens: number;
	readonly verifiedDraftAcceptanceRate?: number;
	readonly lastVerification?: SelfSpeculationVerificationOutcome;
	readonly forkLatencyMs: number;
	readonly forkLogprobTokens: number;
	readonly forkMeanLogprob?: number;
	readonly forkGateSkips: number;
	readonly forkGateSamples: number;
	readonly forkGateExpectedNetBenefitMs?: number;
	readonly failures: number;
	readonly lastError?: string;
}

export interface SelfSpeculationVerificationStep {
	readonly candidateIndex: number;
	readonly candidateID?: string;
	readonly sources: readonly string[];
	readonly draftedTokens: number;
	readonly acceptedTokens: number;
	readonly rejectedTokens: number;
}

export interface SelfSpeculationVerificationOutcome {
	readonly requestID: string;
	readonly speculativeSteps: number;
	readonly draftedTokens: number;
	readonly acceptedTokens: number;
	readonly rejectedTokens: number;
	readonly acceptanceRate: number;
	readonly meanAcceptanceLength: number;
	readonly unresolvedProposals: number;
	readonly unresolvedDraftTokens: number;
	readonly steps: readonly SelfSpeculationVerificationStep[];
}

export interface SelfSpeculationCoordinatorOptions {
	readonly settings: () => SelfSpeculationSettings;
	readonly fetch?: typeof globalThis.fetch;
	readonly requestID?: () => string;
	readonly actionBridge?: SelfSpeculationActionBridge;
	/** Resolve the same exact K(a) identity used by Actor execution. */
	readonly actionKey?: (tool: string, input: Readonly<Record<string, unknown>>) => string | undefined;
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
	readonly actorActionKeys: Set<string>;
	readonly forkCandidateKeys: Set<string>;
	readonly matchedForkKeys: Set<string>;
	readonly candidateSourcesByID: Map<string, Set<string>>;
	readonly actorActionTimes: Map<string, number>;
	readonly gateKey: string;
	forkStartedAt?: number;
	forkCompletedAt?: number;
	forkFailed: boolean;
	ended: boolean;
	gateSampleRecorded: boolean;
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
	private readonly actionKey: (tool: string, input: Readonly<Record<string, unknown>>) => string | undefined;
	private readonly actionBridge?: SelfSpeculationActionBridge;
	private readonly forkGate = new ForkBenefitGate();
	private readonly background = new Set<Promise<void>>();
	private readonly pendingCandidates = new Map<number, Map<string, CandidateRecord>>();
	private active?: TurnState;
	private latestStartedDecisionSequence = 0;
	private acceptingCandidates = false;
	private candidateSequence = 0;
	private submissions = 0;
	private forks = 0;
	private receipts = 0;
	private completedForks = 0;
	private observedForkCandidates = 0;
	private agreedForkCandidates = 0;
	private exactForkMatches = 0;
	private draftTokensSubmitted = 0;
	private draftTokensAccepted = 0;
	private verificationRequests = 0;
	private verifiedDraftProposals = 0;
	private verifiedDraftTokens = 0;
	private verifiedAcceptedDraftTokens = 0;
	private verifiedRejectedDraftTokens = 0;
	private unresolvedDraftProposals = 0;
	private unresolvedDraftTokens = 0;
	private lastVerification?: SelfSpeculationVerificationOutcome;
	private totalForkLatencyMs = 0;
	private totalForkLogprob = 0;
	private totalForkLogprobTokens = 0;
	private forkGateSkips = 0;
	private latestGateKey?: string;
	private failureCount = 0;
	private lastFailure?: string;

	constructor(options: SelfSpeculationCoordinatorOptions) {
		this.settings = options.settings;
		this.fetch = options.fetch ?? globalThis.fetch;
		this.requestID = options.requestID ?? randomUUID;
		this.actionKey = options.actionKey ?? fallbackActionKey;
		this.actionBridge = options.actionBridge;
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
			actorActionKeys: new Set(),
			forkCandidateKeys: new Set(),
			matchedForkKeys: new Set(),
			candidateSourcesByID: new Map(),
			actorActionTimes: new Map(),
			gateKey: modelKey(model),
			forkFailed: false,
			ended: false,
			gateSampleRecorded: false,
		};
		this.actionBridge?.startTurn(turnID);
		this.latestGateKey = modelKey(model);
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
		const gateDecision = this.forkGate.decide(state.gateKey, forkGatePolicy(settings));
		if (!gateDecision.allowed) {
			this.forkGateSkips++;
			this.actionBridge?.publish(state.turnID, []);
			return;
		}
		this.forks++;
		state.forkStartedAt = performance.now();
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
			.then((receipt) => this.recordReceipt(receipt, state, true))
			.catch((error: unknown) => {
				state.forkFailed = true;
				state.forkCompletedAt = performance.now();
				this.actionBridge?.publish(state.turnID, []);
				this.finalizeGateSample(state);
				throw error;
			})
			.finally(() => {
				state.forkTask = undefined;
			});
		this.track(state.forkTask);
	}

	/** Observe the authoritative Actor action regardless of fork completion order. */
	observeActorAction(tool: string, input: unknown): void {
		const state = this.active;
		if (!state || !isRecord(input)) return;
		const key = this.actionKey(tool, input);
		if (!key) return;
		state.actorActionKeys.add(key);
		if (!state.actorActionTimes.has(key)) state.actorActionTimes.set(key, performance.now());
		this.reconcileForkMatches(state);
	}

	endTurn(): void {
		this.closeActive(true);
	}

	/** Clear both the active request and every future-decision candidate. */
	reset(): void {
		this.closeActive(false);
		this.actionBridge?.reset();
		this.pendingCandidates.clear();
		this.latestStartedDecisionSequence = 0;
		this.acceptingCandidates = false;
	}

	private closeActive(preserveForRetry: boolean): void {
		const state = this.active;
		if (state) {
			state.ended = true;
			this.finalizeGateSample(state);
			this.actionBridge?.closeTurn(state.turnID);
		}
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
			.then((receipt) => this.recordVerification(receipt, state));
		this.track(cleanup);
	}

	snapshot(): SelfSpeculationCoordinatorSnapshot {
		const gate = this.latestGateKey ? this.forkGate.snapshot(this.latestGateKey) : undefined;
		return {
			...(this.active?.requestID ? { actorRequestID: this.active.requestID } : {}),
			bufferedCandidates:
				(this.active?.candidates.size ?? 0) +
				[...this.pendingCandidates.values()].reduce((total, candidates) => total + candidates.size, 0),
			candidateSubmissions: this.submissions,
			forkRequests: this.forks,
			candidateReceipts: this.receipts,
			forkCompletions: this.completedForks,
			forkCandidates: this.observedForkCandidates,
			forkAgreements: this.agreedForkCandidates,
			forkExactMatches: this.exactForkMatches,
			submittedDraftTokens: this.draftTokensSubmitted,
			acceptedDraftTokens: this.draftTokensAccepted,
			verificationRequests: this.verificationRequests,
			verifiedDraftProposals: this.verifiedDraftProposals,
			verifiedDraftTokens: this.verifiedDraftTokens,
			verifiedAcceptedDraftTokens: this.verifiedAcceptedDraftTokens,
			verifiedRejectedDraftTokens: this.verifiedRejectedDraftTokens,
			unresolvedDraftProposals: this.unresolvedDraftProposals,
			unresolvedDraftTokens: this.unresolvedDraftTokens,
			...(this.verifiedDraftTokens > 0
				? { verifiedDraftAcceptanceRate: this.verifiedAcceptedDraftTokens / this.verifiedDraftTokens }
				: {}),
			...(this.lastVerification ? { lastVerification: this.lastVerification } : {}),
			forkLatencyMs: this.totalForkLatencyMs,
			forkLogprobTokens: this.totalForkLogprobTokens,
			...(this.totalForkLogprobTokens > 0
				? { forkMeanLogprob: this.totalForkLogprob / this.totalForkLogprobTokens }
				: {}),
			forkGateSkips: this.forkGateSkips,
			forkGateSamples: gate?.samples ?? 0,
			...(gate?.expectedNetBenefitMs === undefined
				? {}
				: { forkGateExpectedNetBenefitMs: gate.expectedNetBenefitMs }),
			failures: this.failureCount,
			...(this.lastFailure ? { lastError: this.lastFailure } : {}),
		};
	}

	private recordVerification(receipt: unknown, state: TurnState): void {
		const verification = record(record(receipt)?.verification);
		if (!verification || !state.requestID) return;
		try {
			const sourcesByCandidateID = new Map(
				[...state.candidateSourcesByID].map(([candidateID, sources]) => [
					candidateID,
					[...sources].sort(),
				] as const),
			);
			const outcome = parseVerificationOutcome(
				verification,
				state.requestID,
				sourcesByCandidateID,
			);
			this.verificationRequests++;
			this.verifiedDraftProposals += outcome.speculativeSteps;
			this.verifiedDraftTokens += outcome.draftedTokens;
			this.verifiedAcceptedDraftTokens += outcome.acceptedTokens;
			this.verifiedRejectedDraftTokens += outcome.rejectedTokens;
			this.unresolvedDraftProposals += outcome.unresolvedProposals;
			this.unresolvedDraftTokens += outcome.unresolvedDraftTokens;
			this.lastVerification = outcome;
		} catch (error) {
			this.failureCount++;
			this.lastFailure = error instanceof Error ? error.message : String(error);
		}
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
			const receipt = await this.post(
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
			this.recordReceipt(receipt, state, false);
			this.submissions++;
		}
	}

	private recordReceipt(receipt: unknown, state: TurnState, fork: boolean): void {
		if (!isRecord(receipt)) {
			if (fork) this.actionBridge?.publish(state.turnID, []);
			return;
		}
		this.receipts++;
		this.draftTokensSubmitted += nonNegativeInteger(receipt.draft_token_count);
		this.draftTokensAccepted += nonNegativeInteger(receipt.accepted_token_count);
		if (fork) {
			this.completedForks++;
			state.forkCompletedAt = performance.now();
		}
		const details = record(receipt.details);
		const bundle = record(details?.bundle);
		const actionCandidates = new Map<string, SelfSpeculationActionCandidate>();
		for (const rawCandidate of array(bundle?.candidates)) {
			const candidate = record(rawCandidate);
			if (!candidate) continue;
			const sources = array(candidate.sources).filter((value): value is string => typeof value === "string");
			for (const rawCandidateID of array(candidate.candidate_ids)) {
				const candidateID = nonEmptyString(rawCandidateID);
				if (!candidateID) continue;
				const knownSources = state.candidateSourcesByID.get(candidateID) ?? new Set<string>();
				for (const source of sources) knownSources.add(source);
				state.candidateSourcesByID.set(candidateID, knownSources);
			}
			if (!fork) continue;
			if (!sources.includes("self-speculation")) continue;
			const call = record(array(candidate.tool_calls)[0]);
			const tool = nonEmptyString(call?.name);
			const input = record(call?.arguments);
			const key = tool && input ? this.actionKey(tool, input) : undefined;
			if (key && !state.forkCandidateKeys.has(key)) {
				state.forkCandidateKeys.add(key);
				this.observedForkCandidates++;
				if (sources.some((source) => source !== "self-speculation")) this.agreedForkCandidates++;
			}
			if (key && tool && input && !actionCandidates.has(key))
				actionCandidates.set(key, { tool, input: structuredClone(input) });
			const forkObservation = record(candidate.fork);
			this.totalForkLatencyMs += observedNonNegativeNumber(forkObservation?.total_ms);
			const logprobs = record(forkObservation?.logprobs);
			const logprobTokens = nonNegativeInteger(logprobs?.token_count);
			const meanLogprob = finiteNumber(logprobs?.mean);
			if (logprobTokens > 0 && meanLogprob !== undefined) {
				this.totalForkLogprob += meanLogprob * logprobTokens;
				this.totalForkLogprobTokens += logprobTokens;
			}
		}
		if (!fork) return;
		this.actionBridge?.publish(
			state.turnID,
			state.settings.forkActionEnabled ? [...actionCandidates.values()].slice(0, state.settings.maxCandidates) : [],
		);
		this.reconcileForkMatches(state);
		this.finalizeGateSample(state);
	}

	private reconcileForkMatches(state: TurnState): void {
		for (const key of state.forkCandidateKeys) {
			if (!state.actorActionKeys.has(key) || state.matchedForkKeys.has(key)) continue;
			state.matchedForkKeys.add(key);
			this.exactForkMatches++;
		}
	}

	private finalizeGateSample(state: TurnState): void {
		if (
			state.gateSampleRecorded ||
			!state.ended ||
			state.forkStartedAt === undefined ||
			state.forkCompletedAt === undefined
		)
			return;
		state.gateSampleRecorded = true;
		const actorMatchAt = [...state.matchedForkKeys]
			.map((key) => state.actorActionTimes.get(key))
			.filter((value): value is number => value !== undefined)
			.reduce<number | undefined>((earliest, value) => (earliest === undefined ? value : Math.min(earliest, value)), undefined);
		this.forkGate.observe(
			state.gateKey,
			{
				forkLatencyMs: state.forkCompletedAt - state.forkStartedAt,
				exactLeadMs:
					actorMatchAt === undefined ? 0 : Math.max(0, actorMatchAt - state.forkCompletedAt),
				...(state.forkFailed ? { failed: true } : {}),
			},
			forkGatePolicy(state.settings),
		);
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
			fork_gate: forkGatePayload(settings),
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
		fork_gate: forkGatePayload(settings),
	};
}

function forkGatePayload(settings: SelfSpeculationSettings): Readonly<Record<string, unknown>> {
	return {
		enabled: settings.forkGateEnabled,
		min_samples: settings.forkGateMinSamples,
		window_size: settings.forkGateWindowSize,
		min_net_benefit_ms: settings.forkGateMinNetBenefitMs,
		probe_interval: settings.forkGateProbeInterval,
		failure_threshold: settings.forkGateFailureThreshold,
	};
}

function forkGatePolicy(settings: SelfSpeculationSettings): ForkBenefitGatePolicy {
	return {
		enabled: settings.forkGateEnabled,
		minSamples: settings.forkGateMinSamples,
		windowSize: settings.forkGateWindowSize,
		minNetBenefitMs: settings.forkGateMinNetBenefitMs,
		probeInterval: settings.forkGateProbeInterval,
		failureThreshold: settings.forkGateFailureThreshold,
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

function modelKey(model: Model<Api>): string {
	return JSON.stringify([model.provider, model.api, model.id]);
}

function parseVerificationOutcome(
	verification: Readonly<Record<string, unknown>>,
	requestID: string,
	sourcesByCandidateID: ReadonlyMap<string, readonly string[]>,
): SelfSpeculationVerificationOutcome {
	const rawSteps = verification.steps;
	if (rawSteps !== undefined && !Array.isArray(rawSteps))
		throw new Error("self-speculation verification steps must be an array");
	const steps = (rawSteps ?? []).map((value, index) => {
		const step = record(value);
		if (!step) throw new Error("self-speculation verification step must be an object");
		const draftedTokens = requiredVerificationInteger(step.drafted_tokens, "drafted_tokens", true);
		const acceptedTokens = requiredVerificationInteger(step.accepted_tokens, "accepted_tokens");
		const rejectedTokens = optionalVerificationInteger(step.rejected_tokens, "rejected_tokens") ??
			draftedTokens - acceptedTokens;
		if (acceptedTokens > draftedTokens || acceptedTokens + rejectedTokens !== draftedTokens)
			throw new Error("self-speculation verification step token counts are inconsistent");
		const candidateIndex = optionalVerificationInteger(step.candidate_index, "candidate_index") ?? index;
		const candidateID = step.candidate_id === undefined || step.candidate_id === null
			? undefined
			: nonEmptyString(step.candidate_id);
		if (step.candidate_id !== undefined && step.candidate_id !== null && !candidateID)
			throw new Error("self-speculation verification candidate_id must be a non-empty string");
		return Object.freeze({
			candidateIndex,
			...(candidateID ? { candidateID } : {}),
			sources: Object.freeze([...(candidateID ? sourcesByCandidateID.get(candidateID) ?? [] : [])]),
			draftedTokens,
			acceptedTokens,
			rejectedTokens,
		});
	});
	const stepDraftedTokens = steps.reduce((total, step) => total + step.draftedTokens, 0);
	const stepAcceptedTokens = steps.reduce((total, step) => total + step.acceptedTokens, 0);
	const stepRejectedTokens = steps.reduce((total, step) => total + step.rejectedTokens, 0);
	const speculativeSteps = optionalVerificationInteger(verification.num_spec_steps, "num_spec_steps") ?? steps.length;
	const draftedTokens = optionalVerificationInteger(verification.num_draft_tokens, "num_draft_tokens") ??
		stepDraftedTokens;
	const acceptedTokens = optionalVerificationInteger(
		verification.num_accepted_draft_tokens,
		"num_accepted_draft_tokens",
	) ?? stepAcceptedTokens;
	const rejectedTokens = optionalVerificationInteger(
		verification.num_rejected_draft_tokens,
		"num_rejected_draft_tokens",
	) ?? draftedTokens - acceptedTokens;
	if (acceptedTokens > draftedTokens || acceptedTokens + rejectedTokens !== draftedTokens)
		throw new Error("self-speculation verification token counts are inconsistent");
	if (
		steps.length > 0 &&
		(speculativeSteps !== steps.length ||
			draftedTokens !== stepDraftedTokens ||
			acceptedTokens !== stepAcceptedTokens ||
			rejectedTokens !== stepRejectedTokens)
	)
		throw new Error("self-speculation verification totals do not match its steps");
	const unresolvedProposals = optionalVerificationInteger(
		verification.unresolved_proposals,
		"unresolved_proposals",
	) ?? 0;
	const unresolvedDraftTokens = optionalVerificationInteger(
		verification.unresolved_draft_tokens,
		"unresolved_draft_tokens",
	) ?? 0;
	const meanAcceptanceLength = optionalVerificationNumber(
		verification.mean_acceptance_length,
		"mean_acceptance_length",
	) ?? (speculativeSteps > 0 ? 1 + acceptedTokens / speculativeSteps : 1);
	return Object.freeze({
		requestID,
		speculativeSteps,
		draftedTokens,
		acceptedTokens,
		rejectedTokens,
		acceptanceRate: draftedTokens > 0 ? acceptedTokens / draftedTokens : 0,
		meanAcceptanceLength,
		unresolvedProposals,
		unresolvedDraftTokens,
		steps: Object.freeze(steps),
	});
}

function optionalVerificationNumber(value: unknown, field: string): number | undefined {
	if (value === undefined || value === null) return undefined;
	if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
		throw new Error(`self-speculation verification ${field} must be a non-negative number`);
	return value;
}

function optionalVerificationInteger(value: unknown, field: string): number | undefined {
	const number = optionalVerificationNumber(value, field);
	if (number === undefined) return undefined;
	if (!Number.isSafeInteger(number))
		throw new Error(`self-speculation verification ${field} must be an integer`);
	return number;
}

function requiredVerificationInteger(value: unknown, field: string, positive = false): number {
	const number = optionalVerificationInteger(value, field);
	if (number === undefined || (positive && number === 0))
		throw new Error(
			`self-speculation verification ${field} must be ${positive ? "positive" : "present"}`,
		);
	return number;
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

function record(value: unknown): Record<string, unknown> | undefined {
	return isRecord(value) ? value : undefined;
}

function array(value: unknown): readonly unknown[] {
	return Array.isArray(value) ? value : [];
}

function finiteNumber(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function observedNonNegativeNumber(value: unknown): number {
	return Math.max(0, finiteNumber(value) ?? 0);
}

function nonNegativeInteger(value: unknown): number {
	return Math.floor(observedNonNegativeNumber(value));
}

function fallbackActionKey(tool: string, input: Readonly<Record<string, unknown>>): string {
	return JSON.stringify([tool, input]);
}
