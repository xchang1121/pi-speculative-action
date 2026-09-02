import { createHash, randomUUID } from "node:crypto";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import {
	DEFAULT_BENEFIT_GATE_POLICY,
	ForkBenefitGate,
	type ForkBenefitGatePolicy,
} from "./fork-benefit-gate.ts";
import {
	createActorForkPlanSource,
	type ActorProbeSchedule,
	type ActorProbeSnapshot,
	type ActorForkActionBatch,
	type ActorForkActionEvidence,
	type ActorForkPlanSource,
} from "./actor-fork-plan-source.ts";
import type { MaterializedSpeculativeCandidate, PredictionFeedback } from "./runtime.ts";
import type { ActionKey } from "./action-semantics.ts";
import type { ActorActionSettlement } from "./settlement.ts";
import { EvidenceLedger } from "./self-speculation-evidence.ts";
import { stableStringify } from "./stable-json.ts";

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
	/** Minimum SPORK selected-token top-1 probability required for action execution. */
	readonly forkActionMinConfidence?: number;
	readonly forkTransport?: SelfSpeculationForkTransport;
	readonly forkMaxTokens?: number;
	readonly forkTemperature?: number;
	readonly forkDecoder?: string;
	readonly forkForcedPrefix?: string;
	/** Require a capable engine to expose token logprobs to its SPORK fork. */
	readonly requireLogprobs?: boolean;
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
	readonly forkActionMinConfidence: number;
	readonly forkTransport: SelfSpeculationForkTransport;
	readonly forkMaxTokens: number;
	readonly forkTemperature: number;
	readonly forkDecoder: string;
	readonly forkForcedPrefix: string;
	readonly requireLogprobs: boolean;
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
	draftBoundary: "auto",
	forkEnabled: true,
	forkActionEnabled: true,
	forkActionMinConfidence: 0.9,
	forkTransport: "provider",
	forkMaxTokens: 128,
	forkTemperature: 0,
	forkDecoder: "auto",
	forkForcedPrefix: "auto",
	requireLogprobs: false,
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
		forkActionMinConfidence: probability(
			input.forkActionMinConfidence,
			SELF_SPECULATION_DEFAULTS.forkActionMinConfidence,
		),
		forkTransport: input.forkTransport === "sidecar" ? "sidecar" : "provider",
		forkMaxTokens: positiveInteger(input.forkMaxTokens, SELF_SPECULATION_DEFAULTS.forkMaxTokens),
		forkTemperature: nonNegativeNumber(input.forkTemperature, SELF_SPECULATION_DEFAULTS.forkTemperature),
		forkDecoder: nonEmptyString(input.forkDecoder) ?? SELF_SPECULATION_DEFAULTS.forkDecoder,
		forkForcedPrefix: nonEmptyString(input.forkForcedPrefix) ?? SELF_SPECULATION_DEFAULTS.forkForcedPrefix,
		requireLogprobs: booleanOr(input.requireLogprobs, SELF_SPECULATION_DEFAULTS.requireLogprobs),
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
	readonly forkRetries: number;
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
	readonly forkActionAdoptions: number;
	readonly forkExecutionAheadMs: number;
	readonly decoderEvidenceContexts: number;
	readonly decoderVerificationSteps: number;
	readonly actionEvidenceContexts: number;
	readonly actionEvidenceObservations: number;
	readonly actionEvidenceAdoptions: number;
	readonly failures: number;
	readonly lastError?: string;
}

export interface SelfSpeculationVerificationStep {
	readonly candidateIndex: number;
	readonly candidateID?: string;
	readonly candidateIDs: readonly string[];
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
	readonly actorForkPlanSource?: ActorForkPlanSource;
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
	dirty: boolean;
	flushTask?: Promise<void>;
	forkTask?: Promise<void>;
	providerPayload?: unknown;
	readonly actorActionKeys: Set<string>;
	readonly forkCandidateKeys: Set<string>;
	readonly agreedForkKeys: Set<string>;
	readonly matchedForkKeys: Set<string>;
	readonly candidateSourcesByID: Map<string, Set<string>>;
	readonly candidateToolsByID: Map<string, Set<string>>;
	readonly gateKey: string;
	forkExecutionAheadMs: number;
	forkStartedAt?: number;
	forkCompletedAt?: number;
	forkFailed: boolean;
	ended: boolean;
	gateSampleRecorded: boolean;
}

interface CandidateRecord {
	readonly id: string;
	readonly key: string;
	readonly executionKey: string;
	readonly executionID: string;
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

interface CandidateCalibration {
	readonly decoderProbability: number;
	readonly actionProbability: number;
	readonly jointProbability: number;
}

interface ParsedSidecarActionCall {
	readonly index: number;
	readonly callID?: string;
	readonly format?: string;
	readonly tool: string;
	readonly input: Readonly<Record<string, unknown>>;
}

interface ForkReceiptOutcome {
	readonly committed: boolean;
	readonly batches: readonly ActorForkActionBatch[];
}

/**
 * Request-scoped decoder-feedback coordinator for a SPORK-capable engine.
 * Network work is serialized and best-effort; it never owns Actor correctness or lifecycle.
 */
export class SelfSpeculationCoordinator {
	private readonly settings: () => SelfSpeculationSettings;
	private readonly fetch: typeof globalThis.fetch;
	private readonly requestID: () => string;
	readonly actorForkPlanSource: ActorForkPlanSource;
	private readonly forkGate = new ForkBenefitGate();
	private readonly decoderEvidence = new EvidenceLedger(4, 2);
	private readonly actionEvidence = new EvidenceLedger(2, 1);
	private readonly background = new Set<Promise<void>>();
	private readonly pendingCandidates = new Map<number, Map<string, CandidateRecord>>();
	private active?: TurnState;
	private latestStartedDecisionSequence = 0;
	private acceptingCandidates = false;
	private candidateSequence = 0;
	private submissions = 0;
	private forks = 0;
	private forkRetries = 0;
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
	private forkActionAdoptions = 0;
	private totalForkExecutionAheadMs = 0;
	private latestGateKey?: string;
	private failureCount = 0;
	private lastFailure?: string;

	constructor(options: SelfSpeculationCoordinatorOptions) {
		this.settings = options.settings;
		this.fetch = options.fetch ?? globalThis.fetch;
		this.requestID = options.requestID ?? randomUUID;
		this.actorForkPlanSource = options.actorForkPlanSource ?? createActorForkPlanSource();
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
			dirty: candidates.size > 0,
			actorActionKeys: new Set(),
			forkCandidateKeys: new Set(),
			agreedForkKeys: new Set(),
			matchedForkKeys: new Set(),
			candidateSourcesByID: new Map(),
			candidateToolsByID: new Map(),
			gateKey: modelKey(model),
			forkExecutionAheadMs: 0,
			forkFailed: false,
			ended: false,
			gateSampleRecorded: false,
		};
		this.actorForkPlanSource.startTurn(turnID);
		this.latestGateKey = modelKey(model);
	}

	/** Bind exactly one authoritative Actor provider request to the current speculative turn. */
	decorateActorPayload(payload: unknown): unknown {
		const state = this.active;
		if (!state || state.requestBound) return payload;
		const settings = state.settings;
		const existing = isRecord(payload) ? nonEmptyString(payload[settings.requestIDField]) : undefined;
		state.requestID = existing ?? this.requestID();
		state.requestBound = true;
		state.providerPayload = cloneSerializable(payload);
		this.actorForkPlanSource.bindActorRequest(state.turnID);
		this.scheduleFlush(state);
		return providerPayload(payload, settings, state.requestID, this.actorForkPlanSource.schedule);
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
		const record = this.mergeCandidate(candidates, candidate);
		if (state && candidates === state.candidates && record.sources.has("self-speculation")) {
			if (!state.forkCandidateKeys.has(record.key)) {
				state.forkCandidateKeys.add(record.key);
				this.observedForkCandidates++;
			}
			if (
				[...record.sources].some((source) => source !== "self-speculation") &&
				!state.agreedForkKeys.has(record.key)
			) {
				state.agreedForkKeys.add(record.key);
				this.agreedForkCandidates++;
			}
			this.reconcileForkMatches(state);
		}
		if (candidates !== state?.candidates || !state) return;
		state.dirty = true;
		this.scheduleFlush(state);
	}

	private mergeCandidate(
		candidates: Map<string, CandidateRecord>,
		candidate: MaterializedSpeculativeCandidate<string>,
	): CandidateRecord {
		const predictedAction = candidate.predictedAction;
		const executionAction = candidate.executionAction;
		const existing = candidates.get(predictedAction.key);
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
			return existing;
		} else {
			const record: CandidateRecord = {
				id: actionIdentity(predictedAction.key),
				key: predictedAction.key,
				executionKey: executionAction.key,
				executionID: actionIdentity(executionAction.key),
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
			};
			candidates.set(predictedAction.key, record);
			return record;
		}
	}

	observeActorOutput(event: AssistantMessageEvent): void {
		const state = this.active;
		if (!state || !state.settings.forkEnabled || state.settings.forkTransport !== "sidecar") return;
		if (event.type === "toolcall_start" || event.type === "done" || event.type === "error") {
			this.actorForkPlanSource.finishActorStream(state.turnID);
			return;
		}
		const snapshot = this.actorForkPlanSource.observeActorDelta(state.turnID, event);
		if (snapshot) this.scheduleActorProbe(state, snapshot);
	}

	private scheduleActorProbe(state: TurnState, snapshot?: ActorProbeSnapshot): void {
		if (state.ended || state.forkTask || !state.requestID) return;
		const probe = snapshot ?? this.actorForkPlanSource.claimPendingProbe(state.turnID);
		if (!probe) return;
		const settings = state.settings;
		if (probe.attempt === 1) {
			const gateDecision = this.forkGate.decide(state.gateKey, forkGatePolicy(settings));
			if (!gateDecision.allowed) {
				this.forkGateSkips++;
				this.actorForkPlanSource.publish(state.turnID, []);
				return;
			}
		} else {
			this.forkRetries++;
		}
		this.forks++;
		state.forkStartedAt ??= performance.now();
		const signal = this.actorForkPlanSource.probeSignal(state.turnID);
		const task = this.post(
			settings.forkPath,
			{
				version: 1,
				request_id: state.requestID,
				model: modelPayload(state.model),
				context: contextPayload(state.context, state.providerPayload),
				snapshot: actorProbeSnapshotPayload(probe),
				options: forkPayload(settings),
			},
			settings,
			signal,
		)
			.then((receipt) => {
				const outcome = this.recordReceipt(receipt, state, true);
				const exhausted = this.actorForkPlanSource.finishProbe(state.turnID);
				if (settings.forkActionEnabled && !outcome?.committed && !exhausted) return;
				this.actorForkPlanSource.publish(
					state.turnID,
					state.settings.forkActionEnabled ? outcome?.batches ?? [] : [],
				);
				this.reconcileForkMatches(state);
				this.finalizeGateSample(state);
			})
			.catch((error: unknown) => {
				state.forkCompletedAt = performance.now();
				this.actorForkPlanSource.finishProbe(state.turnID);
				this.actorForkPlanSource.publish(state.turnID, []);
				if (signal?.aborted) {
					this.finalizeGateSample(state);
					return;
				}
				state.forkFailed = true;
				this.finalizeGateSample(state);
				throw error;
			})
			.finally(() => {
				if (state.forkTask === task) state.forkTask = undefined;
				if (this.active === state && !state.ended) this.scheduleActorProbe(state);
			});
		state.forkTask = task;
		this.track(task);
	}

	/** Observe the authoritative Actor action regardless of fork completion order. */
	observeActorAction(action: ActionKey): void {
		const state = this.active;
		if (!state) return;
		const key = action.key;
		state.actorActionKeys.add(key);
		this.reconcileForkMatches(state);
	}

	/** Feed authoritative adoption into action utility without conflating it with token verification. */
	observeActorSettlement(settlement: ActorActionSettlement): void {
		const state = this.active;
		if (!state) return;
		const matchedSources = new Set(settlement.matchedPredictions.map((prediction) => prediction.source));
		if (!matchedSources.has("self-speculation") || settlement.provider.kind !== "speculative") return;
		const share = settlement.provider.timing.executionAheadMs / Math.max(1, matchedSources.size);
		state.forkExecutionAheadMs += share;
		this.totalForkExecutionAheadMs += share;
		this.forkActionAdoptions++;
	}

	/** Feed semantic prediction adoption into decoder ranking without touching token evidence. */
	observePredictionSettlement(feedback: PredictionFeedback<string>): void {
		const state = this.active;
		const settlement = feedback.settlement;
		if (!state || settlement.observation !== "observed") return;
		const adopted = settlement.match.matched && settlement.match.adoption.status === "adopted";
		this.actionEvidence.observe(
			actionEvidenceContext(state, feedback.tool, settlement.prediction.source),
			1,
			adopted ? 1 : 0,
		);
	}

	endTurn(): void {
		this.closeActive(true);
	}

	/** Clear both the active request and every future-decision candidate. */
	reset(): void {
		this.closeActive(false);
		this.actorForkPlanSource.reset();
		this.pendingCandidates.clear();
		this.latestStartedDecisionSequence = 0;
		this.acceptingCandidates = false;
	}

	private closeActive(preserveForRetry: boolean): void {
		const state = this.active;
		if (state) {
			state.ended = true;
			this.finalizeGateSample(state);
			this.actorForkPlanSource.closeTurn(state.turnID);
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
		const decoderEvidence = this.decoderEvidence.snapshot();
		const actionEvidence = this.actionEvidence.snapshot();
		return {
			...(this.active?.requestID ? { actorRequestID: this.active.requestID } : {}),
			bufferedCandidates:
				(this.active?.candidates.size ?? 0) +
				[...this.pendingCandidates.values()].reduce((total, candidates) => total + candidates.size, 0),
			candidateSubmissions: this.submissions,
			forkRequests: this.forks,
			forkRetries: this.forkRetries,
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
			forkActionAdoptions: this.forkActionAdoptions,
			forkExecutionAheadMs: this.totalForkExecutionAheadMs,
			decoderEvidenceContexts: decoderEvidence.contexts,
			decoderVerificationSteps: decoderEvidence.observations,
			actionEvidenceContexts: actionEvidence.contexts,
			actionEvidenceObservations: actionEvidence.trials,
			actionEvidenceAdoptions: actionEvidence.successes,
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
			this.observeVerificationEvidence(state, outcome);
		} catch (error) {
			this.failureCount++;
			this.lastFailure = error instanceof Error ? error.message : String(error);
		}
	}

	private observeVerificationEvidence(state: TurnState, outcome: SelfSpeculationVerificationOutcome): void {
		for (const step of outcome.steps) {
			const records = [...state.candidates.values()].filter((candidate) => step.candidateIDs.includes(candidate.id));
			const tools = new Set(records.map((candidate) => candidate.tool));
			for (const candidateID of step.candidateIDs) {
				for (const tool of state.candidateToolsByID.get(candidateID) ?? []) tools.add(tool);
			}
			if (!tools.size) continue;
			const sources = step.sources.length
				? step.sources
				: [...new Set(records.flatMap((candidate) => [...candidate.sources]))];
			for (const tool of tools) {
				for (const source of sources) {
					this.decoderEvidence.observe(
						decoderEvidenceContext(state, tool, source),
						step.draftedTokens,
						step.acceptedTokens,
					);
				}
			}
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
			const candidates = rankedCandidates(
				state.candidates.values(),
				(candidate) => this.candidateCalibration(state, candidate),
			).slice(0, settings.maxCandidates);
			if (!candidates.length) continue;
			const receipt = await this.post(
				settings.candidatePath,
				{
					version: 2,
					request_id: state.requestID,
					model: modelPayload(state.model),
					max_draft_tokens: settings.maxDraftTokens,
					format: settings.draftFormat,
					...(settings.draftBoundary === "auto" ? {} : { boundary: settings.draftBoundary }),
					candidates: candidates.map((candidate) =>
						candidatePayload(candidate, this.candidateCalibration(state, candidate)),
					),
				},
				settings,
			);
			this.recordReceipt(receipt, state, false);
			this.submissions++;
		}
	}

	private candidateCalibration(state: TurnState, candidate: CandidateRecord): CandidateCalibration {
		const decoderProbability = this.decoderEvidence.probability(
			[...candidate.sources].map((source) => decoderEvidenceContext(state, candidate.tool, source)),
		);
		const actionProbability = this.actionEvidence.probability(
			[...candidate.sources].map((source) => actionEvidenceContext(state, candidate.tool, source)),
		);
		return {
			decoderProbability,
			actionProbability,
			jointProbability: decoderProbability * actionProbability,
		};
	}

	private recordReceipt(receipt: unknown, state: TurnState, fork: boolean): ForkReceiptOutcome | undefined {
		if (!isRecord(receipt)) {
			return fork ? { committed: false, batches: [] } : undefined;
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
		const actionBatches = new Map<string, ActorForkActionBatch>();
		for (const rawCandidate of array(bundle?.candidates)) {
			const candidate = record(rawCandidate);
			if (!candidate) continue;
			const sources = uniqueStrings(candidate.sources);
			const candidateIDs = uniqueStrings(candidate.candidate_ids);
			const rawCalls = array(candidate.tool_calls);
			const calls = rawCalls
				.map((value, index) => parsedSidecarActionCall(value, index))
				.filter((value): value is ParsedSidecarActionCall => value !== undefined);
			for (const candidateID of candidateIDs) {
				const knownSources = state.candidateSourcesByID.get(candidateID) ?? new Set<string>();
				for (const source of sources) knownSources.add(source);
				state.candidateSourcesByID.set(candidateID, knownSources);
				const knownTools = state.candidateToolsByID.get(candidateID) ?? new Set<string>();
				for (const call of calls) knownTools.add(call.tool);
				state.candidateToolsByID.set(candidateID, knownTools);
			}
			if (!fork) continue;
			if (!sources.includes("self-speculation")) continue;
			const forkObservation = record(candidate.fork);
			this.totalForkLatencyMs += observedNonNegativeNumber(forkObservation?.total_ms);
			const logprobs = record(forkObservation?.logprobs);
			const logprobTokens = nonNegativeInteger(logprobs?.token_count);
			const meanLogprob = finiteNumber(logprobs?.mean);
			const confidence = probabilityOrUndefined(record(logprobs?.tool_name)?.minimum_probability);
			if (logprobTokens > 0 && meanLogprob !== undefined) {
				this.totalForkLogprob += meanLogprob * logprobTokens;
				this.totalForkLogprobTokens += logprobTokens;
			}
			if (
				!rawCalls.length ||
				calls.length !== rawCalls.length ||
				(state.settings.forkActionMinConfidence > 0 &&
					(confidence === undefined || confidence < state.settings.forkActionMinConfidence))
			)
				continue;
			const fingerprint = sidecarActionBatchFingerprint(calls);
			const score = record(candidate.score);
			const evidence: ActorForkActionEvidence = {
				candidateIDs,
				sources,
				provenance: structuredClone(array(candidate.provenance)),
				actionIdentities: structuredClone(array(candidate.action_identities)),
				draftTokenCount: nonNegativeInteger(candidate.draft_token_count),
				...(confidence !== undefined ? { confidence } : {}),
				...(score ? { score: structuredClone(score) } : {}),
				...(forkObservation ? { fork: structuredClone(forkObservation) } : {}),
			};
			const existing = actionBatches.get(fingerprint);
			if (existing) {
				actionBatches.set(fingerprint, { ...existing, evidence: [...existing.evidence, evidence] });
				continue;
			}
			const batchID = sidecarActionBatchID(fingerprint);
			actionBatches.set(fingerprint, {
				id: batchID,
				calls: calls.map((call, index) => ({ id: `${index}:fork`, ...call })),
				evidence: [evidence],
			});
		}
		if (!fork) return undefined;
		const batches = [...actionBatches.values()].slice(0, state.settings.maxCandidates);
		return { committed: batches.length > 0, batches };
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
		this.forkGate.observe(
			state.gateKey,
			{
				costMs: state.forkCompletedAt - state.forkStartedAt,
				benefitMs: state.forkExecutionAheadMs,
				...(state.forkFailed ? { failed: true } : {}),
			},
			forkGatePolicy(state.settings),
		);
	}

	private async post(
		path: string,
		payload: Readonly<Record<string, unknown>>,
		settings: SelfSpeculationSettings = this.settings(),
		externalSignal?: AbortSignal,
	): Promise<unknown> {
		const controller = new AbortController();
		const timeout = setTimeout(() => controller.abort(), settings.timeoutMs);
		const signal = externalSignal ? AbortSignal.any([controller.signal, externalSignal]) : controller.signal;
		try {
			const apiKey = settings.apiKeyEnv ? process.env[settings.apiKeyEnv] : undefined;
			const response = await this.fetch(`${settings.endpoint}${path}`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
				},
				body: JSON.stringify(payload),
				signal,
			});
			if (!response.ok) throw new Error(`self-speculation control plane returned HTTP ${response.status}`);
			return response.status === 204 ? undefined : await response.json().catch(() => undefined);
		} catch (error) {
			if (!externalSignal?.aborted) {
				this.failureCount++;
				this.lastFailure = error instanceof Error ? error.message : String(error);
			}
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
	probeSchedule: ActorProbeSchedule,
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
			fork: settings.forkEnabled,
			fork_transport: settings.forkTransport,
			max_draft_tokens: settings.maxDraftTokens,
			draft_format: settings.draftFormat,
			...(settings.draftBoundary === "auto" ? {} : { draft_boundary: settings.draftBoundary }),
			fork_max_tokens: settings.forkMaxTokens,
			fork_temperature: settings.forkTemperature,
			fork_decoder: settings.forkDecoder,
			...(settings.forkForcedPrefix === "auto"
				? {}
				: { fork_forced_prefix: settings.forkForcedPrefix }),
			require_logprobs: requiresForkLogprobs(settings),
			d2: {
				confidence_metric: "minimum_tool_name_probability",
				confidence_threshold: settings.forkActionMinConfidence,
				max_attempts: probeSchedule.maxAttempts,
				retry_token_step: probeSchedule.retryStreamUpdates,
			},
			fork_gate: forkGatePayload(settings),
		},
	};
}

function forkPayload(settings: SelfSpeculationSettings): Readonly<Record<string, unknown>> {
	return {
		max_tokens: settings.forkMaxTokens,
		temperature: settings.forkTemperature,
		decoder: settings.forkDecoder,
		...(settings.forkForcedPrefix === "auto" ? {} : { forced_prefix: settings.forkForcedPrefix }),
		require_logprobs: requiresForkLogprobs(settings),
		max_draft_tokens: settings.maxDraftTokens,
		draft_format: settings.draftFormat,
		...(settings.draftBoundary === "auto" ? {} : { draft_boundary: settings.draftBoundary }),
		fork_gate: forkGatePayload(settings),
	};
}

function actorProbeSnapshotPayload(snapshot: ActorProbeSnapshot): Readonly<Record<string, unknown>> {
	return {
		attempt: snapshot.attempt,
		generated_text: snapshot.generatedText,
		content: snapshot.content,
		reasoning: snapshot.reasoning,
		chunk_count: snapshot.outputChunks,
		output_chunk_count: snapshot.outputChunks,
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

function candidatePayload(candidate: CandidateRecord, calibration: CandidateCalibration): Readonly<Record<string, unknown>> {
	return {
		id: candidate.id,
		action_identity: {
			version: 1,
			predicted_action_id: candidate.id,
			execution_action_id: candidate.executionID,
			projected: candidate.key !== candidate.executionKey,
		},
		sources: [...candidate.sources].sort(),
		provenance: candidate.provenance,
		tool_call: { name: candidate.tool, arguments: candidate.input },
		score: {
			decoder_acceptance_probability: calibration.decoderProbability,
			action_adoption_probability: calibration.actionProbability,
			joint_speculation_probability: calibration.jointProbability,
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

function rankedCandidates(
	candidates: Iterable<CandidateRecord>,
	calibration: (candidate: CandidateRecord) => CandidateCalibration,
): CandidateRecord[] {
	return [...candidates]
		.map((candidate) => ({ candidate, calibration: calibration(candidate) }))
		.sort(
			(left, right) =>
				left.candidate.horizon - right.candidate.horizon ||
				right.calibration.jointProbability - left.calibration.jointProbability ||
				right.calibration.decoderProbability - left.calibration.decoderProbability ||
				right.candidate.conditionalProbability - left.candidate.conditionalProbability ||
				right.candidate.empiricalProbability - left.candidate.empiricalProbability ||
				right.candidate.expectedLatencyBenefitMs - left.candidate.expectedLatencyBenefitMs ||
				right.candidate.expectedDurationMs - left.candidate.expectedDurationMs ||
				left.candidate.depth - right.candidate.depth ||
				left.candidate.sequence - right.candidate.sequence,
		)
		.map(({ candidate }) => candidate);
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

function decoderEvidenceContext(state: TurnState, tool: string, source: string) {
	return {
		model: state.gateKey,
		endpoint: state.settings.endpoint,
		format: state.settings.draftFormat,
		boundary: state.settings.draftBoundary,
		tool,
		source,
	};
}

function actionEvidenceContext(state: TurnState, tool: string, source: string) {
	return { model: state.gateKey, tool, source };
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
		const candidateIDs = [...new Set([
			...(candidateID ? [candidateID] : []),
			...array(step.candidate_ids).map(nonEmptyString).filter((value): value is string => value !== undefined),
		])];
		const reportedSources = array(step.sources)
			.map(nonEmptyString)
			.filter((value): value is string => value !== undefined);
		const sources = reportedSources.length
			? [...new Set(reportedSources)]
			: [...new Set(candidateIDs.flatMap((id) => sourcesByCandidateID.get(id) ?? []))];
		return Object.freeze({
			candidateIndex,
			...(candidateID ? { candidateID } : {}),
			candidateIDs: Object.freeze(candidateIDs),
			sources: Object.freeze(sources),
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

function probability(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1 ? value : fallback;
}

function probabilityOrUndefined(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1 ? value : undefined;
}

function requiresForkLogprobs(settings: SelfSpeculationSettings): boolean {
	return (
		settings.requireLogprobs ||
		((settings.forkTransport === "provider" || settings.forkActionEnabled) &&
			settings.forkActionMinConfidence > 0)
	);
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

function actionIdentity(key: string): string {
	return `action:v1:${createHash("sha256").update(key).digest("hex")}`;
}

function parsedSidecarActionCall(value: unknown, fallbackIndex: number): ParsedSidecarActionCall | undefined {
	const call = record(value);
	const tool = nonEmptyString(call?.name);
	const input = record(call?.arguments);
	if (!tool || !input) return undefined;
	const observedIndex = finiteNumber(call?.index);
	const index =
		observedIndex !== undefined && Number.isSafeInteger(observedIndex) && observedIndex >= 0
			? observedIndex
			: fallbackIndex;
	const callID = nonEmptyString(call?.call_id);
	const format = nonEmptyString(call?.format);
	return {
		index,
		...(callID ? { callID } : {}),
		...(format ? { format } : {}),
		tool,
		input: structuredClone(input),
	};
}

function sidecarActionBatchFingerprint(calls: readonly ParsedSidecarActionCall[]): string {
	return stableStringify(
		calls.map((call) => ({
			index: call.index,
			...(call.callID ? { callID: call.callID } : {}),
			...(call.format ? { format: call.format } : {}),
			tool: call.tool,
			input: call.input,
		})),
	);
}

function sidecarActionBatchID(fingerprint: string): string {
	return `fork:v1:${createHash("sha256").update(fingerprint).digest("hex").slice(0, 32)}`;
}

function uniqueStrings(value: unknown): string[] {
	return [...new Set(array(value).map(nonEmptyString).filter((item): item is string => item !== undefined))];
}
