import { hash, randomUUID } from "node:crypto";
import { errorMessage } from "./error-utils.ts";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import {
	DEFAULT_BENEFIT_GATE_POLICY,
	creditAdoption, BenefitGate,
	type BenefitGatePolicy,
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
import { asRecord as record, isRecord, stableStringify } from "./stable-json.ts";
import { finiteNumber, nonNegativeFinite, nonNegativeCount } from "./number-utils.ts";
import { booleanOr, nonNegativeNumber, positiveInteger, probability, settingsParser } from "./setting-input.ts";

export type SelfSpeculationForkTransport = "provider" | "sidecar";

export interface SelfSpeculationSettingsInput extends Partial<SelfSpeculationSettings> {}

export interface SelfSpeculationSettings extends Readonly<typeof selfSpeculationDefaults> {
	/** Optional environment variable containing a bearer token for the control plane. */
	readonly apiKeyEnv?: string;
}

const selfSpeculationDefaults = {
	enabled: false,
	/** Trusted control-plane endpoint exposed by the inference runtime. */
	endpoint: "http://127.0.0.1:8000",
	/** Top-level field carrying the stable request ID in provider payloads. */
	requestIDField: "request_id",
	candidatePath: "/self-speculation/candidates",
	forkPath: "/self-speculation/fork",
	clearPath: "/self-speculation/clear",
	timeoutMs: 2_000,
	maxCandidates: 8,
	maxDraftTokens: 28,
	/** Actor tool-call protocol Profile; D3 serialization always follows this Profile. */
	actorProfile: "tagged_json",
	/** Tool-call body format used when concrete K(a) candidates are tokenized. */
	draftFormat: "auto",
	/** Exact target-model boundary preceding a boundary-relative action draft. */
	draftBoundary: "auto",
	forkEnabled: true,
	/** Admit complete sidecar fork tool calls to the ordinary speculative-action runtime. */
	forkActionEnabled: true,
	/** Minimum SPORK selected-token top-1 probability required for action execution. */
	forkActionMinConfidence: 0.9,
	forkTransport: "provider" as SelfSpeculationForkTransport,
	forkMaxTokens: 128,
	forkTemperature: 0,
	forkDecoder: "auto",
	forkForcedPrefix: "auto",
	/** Require a capable engine to expose token logprobs to its SPORK fork. */
	requireLogprobs: false,
	forkGateEnabled: DEFAULT_BENEFIT_GATE_POLICY.enabled,
	forkGateMinSamples: DEFAULT_BENEFIT_GATE_POLICY.minSamples,
	forkGateWindowSize: DEFAULT_BENEFIT_GATE_POLICY.windowSize,
	forkGateMinNetBenefitMs: DEFAULT_BENEFIT_GATE_POLICY.minNetBenefitMs,
	forkGateProbeInterval: DEFAULT_BENEFIT_GATE_POLICY.probeInterval,
	forkGateFailureThreshold: DEFAULT_BENEFIT_GATE_POLICY.failureThreshold,
};

export const SELF_SPECULATION_DEFAULTS: SelfSpeculationSettings = Object.freeze(selfSpeculationDefaults);

const parseSettings = settingsParser(selfSpeculationDefaults, {
	enabled: booleanOr,
	endpoint: (value, fallback) => (nonEmptyString(value) ?? fallback).replace(/\/+$/u, ""),
	requestIDField: textOr,
	candidatePath: httpPath,
	forkPath: httpPath,
	clearPath: httpPath,
	timeoutMs: positiveInteger,
	maxCandidates: positiveInteger,
	maxDraftTokens: positiveInteger,
	actorProfile: textOr,
	draftFormat: textOr,
	draftBoundary: textOr,
	forkEnabled: booleanOr,
	forkActionEnabled: booleanOr,
	forkActionMinConfidence: probability,
	forkTransport: (value) => value === "sidecar" ? "sidecar" : "provider",
	forkMaxTokens: positiveInteger,
	forkTemperature: nonNegativeNumber,
	forkDecoder: textOr,
	forkForcedPrefix: textOr,
	requireLogprobs: booleanOr,
	forkGateEnabled: booleanOr,
	forkGateMinSamples: positiveInteger,
	forkGateWindowSize: positiveInteger,
	forkGateMinNetBenefitMs: nonNegativeNumber,
	forkGateProbeInterval: positiveInteger,
	forkGateFailureThreshold: positiveInteger,
});

export function normalizeSelfSpeculationSettings(value: unknown): SelfSpeculationSettings {
	const input = isRecord(value) ? value : {};
	const result = parseSettings(input);
	result.forkGateWindowSize = Math.max(result.forkGateMinSamples, result.forkGateWindowSize);
	const apiKeyEnv = nonEmptyString(input.apiKeyEnv);
	return apiKeyEnv ? { ...result, apiKeyEnv } : result;
}

export interface SelfSpeculationCoordinatorSnapshot extends ReturnType<SelfSpeculationCoordinator["snapshot"]> {}

export interface SelfSpeculationVerificationOutcome extends ReturnType<typeof parseVerificationOutcome> {}

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
	readonly context: ReturnType<typeof contextPayload>;
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
	readonly reportedCandidates: Map<string, ReportedCandidate>;
	readonly gateKey: string;
	readonly forkUtility: { costMs: number; benefitMs: number | undefined };
	/** Summed probe compute; the Actor's streaming between retries costs the fork nothing. */
	forkBusyMs?: number;
	forkFailed: boolean;
	ended: boolean;
	gateSampleRecorded: boolean;
}

interface ReportedCandidate {
	readonly sources: Set<string>;
	readonly tools: Set<string>;
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
	private readonly forkGate = new BenefitGate();
	private readonly decoderEvidence = new EvidenceLedger(4, 2);
	private readonly actionEvidence = new EvidenceLedger(2, 1);
	private readonly background = new Set<Promise<void>>();
	private readonly pendingCandidates = new Map<number, Map<string, CandidateRecord>>();
	private active?: TurnState;
	private latestStartedDecisionSequence = 0;
	private acceptingCandidates = false;
	private candidateSequence = 0;
	private readonly counters = {
		candidateSubmissions: 0,
		forkRequests: 0,
		forkRetries: 0,
		candidateReceipts: 0,
		forkCompletions: 0,
		forkCandidates: 0,
		forkAgreements: 0,
		forkExactMatches: 0,
		submittedDraftTokens: 0,
		/** Registration acknowledgements; not necessarily target-model acceptance. */
		acceptedDraftTokens: 0,
		verificationRequests: 0,
		verifiedDraftProposals: 0,
		verifiedDraftTokens: 0,
		verifiedAcceptedDraftTokens: 0,
		verifiedRejectedDraftTokens: 0,
		unresolvedDraftProposals: 0,
		unresolvedDraftTokens: 0,
		forkLatencyMs: 0,
		forkLogprobTokens: 0,
		forkGateSkips: 0,
		forkActionAdoptions: 0,
		forkExecutionAheadMs: 0,
		failures: 0,
	};
	private lastVerification?: SelfSpeculationVerificationOutcome;
	private totalForkLogprob = 0;
	private latestGateKey?: string;
	private lastFailure?: string;
	private lastResolvedActorProfile?: string;
	private lastProfileResolutionSource?: string;

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
			context: contextPayload(context),
			settings,
			candidates,
			requestBound: false,
			dirty: candidates.size > 0,
			actorActionKeys: new Set(),
			forkCandidateKeys: new Set(),
			agreedForkKeys: new Set(),
			matchedForkKeys: new Set(),
			reportedCandidates: new Map(),
			gateKey: modelKey(model),
			forkUtility: { costMs: 0, benefitMs: 0 },
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
		// Only the runtime exposing the control plane accepts these fields; hosted APIs reject unknown ones.
		return originOf(state.model.baseUrl) === originOf(settings.endpoint)
			? providerPayload(payload, settings, state.requestID, this.actorForkPlanSource.schedule) : payload;
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
				this.counters.forkCandidates++;
			}
			if (
				[...record.sources].some((source) => source !== "self-speculation") &&
				!state.agreedForkKeys.has(record.key)
			) {
				state.agreedForkKeys.add(record.key);
				this.counters.forkAgreements++;
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
			if (!existing.provenance.some((item) => item.proposalID === candidate.proposalID && item.actionID === candidate.actionID))
				existing.provenance.push({ proposalID: candidate.proposalID, actionID: candidate.actionID });
			for (const field of ["depth", "horizon"] as const)
				existing[field] = Math.min(existing[field], finiteNumber(candidate[field]) ?? 0);
			for (const field of ["conditionalProbability", "empiricalProbability", "expectedLatencyBenefitMs", "expectedDurationMs"] as const)
				existing[field] = Math.max(existing[field], finiteNumber(candidate[field]) ?? 0);
			existing.latestDecisionSequence = Math.max(existing.latestDecisionSequence, candidate.latestDecisionSequence);
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
				depth: finiteNumber(candidate.depth) ?? 0,
				horizon: finiteNumber(candidate.horizon) ?? 0,
				latestDecisionSequence: candidate.latestDecisionSequence,
				conditionalProbability: finiteNumber(candidate.conditionalProbability) ?? 0,
				empiricalProbability: finiteNumber(candidate.empiricalProbability) ?? 0,
				expectedLatencyBenefitMs: finiteNumber(candidate.expectedLatencyBenefitMs) ?? 0,
				expectedDurationMs: finiteNumber(candidate.expectedDurationMs) ?? 0,
			};
			candidates.set(predictedAction.key, record);
			return record;
		}
	}

	observeActorOutput(event: AssistantMessageEvent): void {
		const state = this.active;
		if (!state || !state.settings.forkEnabled || state.settings.forkTransport !== "sidecar") return;
		if (event.type === "toolcall_start" || event.type === "done" || event.type === "error") return this.finishActorOutput();
		const snapshot = this.actorForkPlanSource.observeActorDelta(state.turnID, event);
		if (snapshot) this.scheduleActorProbe(state, snapshot);
	}

	/** Pi ends an Actor message with message_end, never a done update: a text-only answer leaves nothing to fork. */
	finishActorOutput(): void {
		if (this.active) this.actorForkPlanSource.finishActorStream(this.active.turnID);
	}

	private scheduleActorProbe(state: TurnState, snapshot?: ActorProbeSnapshot): void {
		if (state.ended || state.forkTask || !state.requestID) return;
		const probe = snapshot ?? this.actorForkPlanSource.claimPendingProbe(state.turnID);
		if (!probe) return;
		const settings = state.settings;
		if (probe.attempt === 1) {
			const gateDecision = this.forkGate.decide(state.gateKey, forkGatePolicy(settings));
			if (!gateDecision.allowed) {
				this.counters.forkGateSkips++;
				this.actorForkPlanSource.publish(state.turnID, []);
				return;
			}
		} else {
			this.counters.forkRetries++;
		}
		this.counters.forkRequests++;
		const probeStartedAt = performance.now(), signal = this.actorForkPlanSource.startProbe(state.turnID);
		const task = this.post(
			settings.forkPath,
			{
				version: 1,
				request_id: state.requestID,
				model: modelPayload(state.model),
				context: { ...state.context, ...(state.providerPayload !== undefined ? { provider_payload: state.providerPayload } : {}) },
				snapshot: {
					attempt: probe.attempt,
					generated_text: probe.generatedText,
					content: probe.content,
					reasoning: probe.reasoning,
					chunk_count: probe.outputChunks,
					output_chunk_count: probe.outputChunks,
				},
				options: forkPayload(settings),
			},
			settings,
			signal,
		)
			.finally(() => { state.forkBusyMs = (state.forkBusyMs ?? 0) + performance.now() - probeStartedAt; })
			.then((receipt) => {
				const outcome = this.recordReceipt(receipt, state, true);
				const exhausted = this.actorForkPlanSource.finishProbe(state.turnID);
				if (settings.forkActionEnabled && !outcome?.committed && !exhausted) return;
				this.actorForkPlanSource.publish(state.turnID, state.settings.forkActionEnabled ? outcome?.batches ?? [] : []);
				this.reconcileForkMatches(state);
				this.finalizeGateSample(state);
			})
			.catch((error: unknown) => {
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
		const shares = matchedSources.size;
		creditAdoption(state.forkUtility, settlement.provider.timing, shares);
		this.counters.forkExecutionAheadMs += settlement.provider.timing.executionAheadMs / shares;
		this.counters.forkActionAdoptions++;
	}

	/** Feed semantic prediction adoption into decoder ranking without touching token evidence. */
	observePredictionSettlement(feedback: PredictionFeedback<string>): void {
		const state = this.active;
		const settlement = feedback.settlement;
		if (!state || settlement.observation !== "observed") return;
		const adopted = settlement.match.matched && settlement.match.adoption.status === "adopted";
		this.actionEvidence.observe(actionEvidenceContext(state, feedback.tool, settlement.prediction.source), 1, adopted ? 1 : 0);
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
			// The active decision owns its bundle exclusively; outstanding submissions keep the old snapshot.
			this.pendingCandidates.set(state.decisionSequence, new Map([...state.candidates].map(([key, candidate]) => [key, {
				...candidate,
				input: structuredClone(candidate.input),
				sources: new Set(candidate.sources),
				provenance: candidate.provenance.map((item) => ({ ...item })),
			}])));
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

	snapshot() {
		const gate = this.latestGateKey ? this.forkGate.snapshot(this.latestGateKey) : undefined;
		const decoderEvidence = this.decoderEvidence.snapshot();
		const actionEvidence = this.actionEvidence.snapshot();
		const snapshot = {
			...this.counters,
			...(this.active?.requestID ? { actorRequestID: this.active.requestID } : {}),
			...(this.lastResolvedActorProfile
				? { resolvedActorProfile: this.lastResolvedActorProfile }
				: {}),
			...(this.lastProfileResolutionSource
				? { profileResolutionSource: this.lastProfileResolutionSource }
				: {}),
			bufferedCandidates:
				(this.active?.candidates.size ?? 0) +
				[...this.pendingCandidates.values()].reduce((total, candidates) => total + candidates.size, 0),
			...(this.counters.verifiedDraftTokens > 0
				? { verifiedDraftAcceptanceRate: this.counters.verifiedAcceptedDraftTokens / this.counters.verifiedDraftTokens }
				: {}),
			...(this.lastVerification ? { lastVerification: this.lastVerification } : {}),
			...(this.counters.forkLogprobTokens > 0
				? { forkMeanLogprob: this.totalForkLogprob / this.counters.forkLogprobTokens }
				: {}),
			forkGateSamples: gate?.samples ?? 0,
			...(gate?.expectedNetBenefitMs === undefined
				? {}
				: { forkGateExpectedNetBenefitMs: gate.expectedNetBenefitMs }),
			decoderEvidenceContexts: decoderEvidence.contexts,
			decoderVerificationSteps: decoderEvidence.observations,
			actionEvidenceContexts: actionEvidence.contexts,
			actionEvidenceObservations: actionEvidence.trials,
			actionEvidenceAdoptions: actionEvidence.successes,
			...(this.lastFailure ? { lastError: this.lastFailure } : {}),
		};
		return snapshot as Readonly<typeof snapshot>;
	}

	private recordVerification(receipt: unknown, state: TurnState): void {
		const verification = record(record(receipt)?.verification);
		if (!verification || !state.requestID) return;
		try {
			const outcome = parseVerificationOutcome(verification, state.requestID, state.reportedCandidates);
			this.counters.verificationRequests++;
			this.counters.verifiedDraftProposals += outcome.speculativeSteps;
			this.counters.verifiedDraftTokens += outcome.draftedTokens;
			this.counters.verifiedAcceptedDraftTokens += outcome.acceptedTokens;
			this.counters.verifiedRejectedDraftTokens += outcome.rejectedTokens;
			this.counters.unresolvedDraftProposals += outcome.unresolvedProposals;
			this.counters.unresolvedDraftTokens += outcome.unresolvedDraftTokens;
			this.lastVerification = outcome;
			this.observeVerificationEvidence(state, outcome);
		} catch (error) {
			this.counters.failures++;
			this.lastFailure = errorMessage(error);
		}
	}

	private observeVerificationEvidence(state: TurnState, outcome: SelfSpeculationVerificationOutcome): void {
		for (const step of outcome.steps) {
			const records = [...state.candidates.values()].filter((candidate) => step.candidateIDs.includes(candidate.id));
			const tools = new Set(records.map((candidate) => candidate.tool));
			for (const candidateID of step.candidateIDs) {
				for (const tool of state.reportedCandidates.get(candidateID)?.tools ?? []) tools.add(tool);
			}
			if (!tools.size) continue;
			const sources = step.sources.length
				? step.sources
				: [...new Set(records.flatMap((candidate) => [...candidate.sources]))];
			for (const tool of tools) {
				for (const source of sources) {
					this.decoderEvidence.observe(decoderEvidenceContext(state, tool, source), step.draftedTokens, step.acceptedTokens);
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
					actor_profile: settings.actorProfile,
					...(settings.draftFormat === "auto" ? {} : { format: settings.draftFormat }),
					...(settings.draftBoundary === "auto" ? {} : { boundary: settings.draftBoundary }),
					candidates: candidates.map(({ candidate, calibration }) => candidatePayload(candidate, calibration)),
				},
				settings,
			);
			this.recordReceipt(receipt, state, false);
			this.counters.candidateSubmissions++;
		}
	}

	private candidateCalibration(state: TurnState, candidate: CandidateRecord): CandidateCalibration {
		const decoderProbability = this.decoderEvidence.probability(
			[...candidate.sources].map((source) => decoderEvidenceContext(state, candidate.tool, source)),
		);
		const actionProbability = this.actionEvidence.probability(
			[...candidate.sources].map((source) => actionEvidenceContext(state, candidate.tool, source)),
		);
		return { decoderProbability, actionProbability, jointProbability: decoderProbability * actionProbability };
	}

	private recordReceipt(receipt: unknown, state: TurnState, fork: boolean): ForkReceiptOutcome | undefined {
		if (!isRecord(receipt)) return fork ? { committed: false, batches: [] } : undefined;
		this.counters.candidateReceipts++;
		this.counters.submittedDraftTokens += nonNegativeCount(receipt.draft_token_count);
		this.counters.acceptedDraftTokens += nonNegativeCount(receipt.accepted_token_count);
		if (fork) this.counters.forkCompletions++;
		const details = record(receipt.details);
		const bundle = record(details?.bundle);
		const actionBatches = new Map<string, ActorForkActionBatch>();
		for (const rawCandidate of array(bundle?.candidates)) {
			const candidate = record(rawCandidate);
			if (!candidate) continue;
			const profileResolution = record(candidate.profile);
			const resolvedProfile = record(profileResolution?.profile);
			const resolvedProfileID = nonEmptyString(resolvedProfile?.id);
			const resolutionSource = nonEmptyString(profileResolution?.source);
			if (resolvedProfileID) this.lastResolvedActorProfile = resolvedProfileID;
			if (resolutionSource) this.lastProfileResolutionSource = resolutionSource;
			const sources = uniqueStrings(candidate.sources);
			const candidateIDs = uniqueStrings(candidate.candidate_ids);
			const rawCalls = array(candidate.tool_calls);
			const calls = rawCalls
				.map((value, index) => parsedSidecarActionCall(value, index))
				.filter((value) => value !== undefined);
			for (const candidateID of candidateIDs) {
				const known = state.reportedCandidates.get(candidateID) ?? { sources: new Set<string>(), tools: new Set<string>() };
				for (const source of sources) known.sources.add(source);
				for (const call of calls) known.tools.add(call.tool);
				state.reportedCandidates.set(candidateID, known);
			}
			if (!fork) continue;
			if (!sources.includes("self-speculation")) continue;
			const forkObservation = record(candidate.fork);
			this.counters.forkLatencyMs += nonNegativeFinite(forkObservation?.total_ms);
			const logprobs = record(forkObservation?.logprobs);
			const logprobTokens = nonNegativeCount(logprobs?.token_count);
			const meanLogprob = finiteNumber(logprobs?.mean);
			const confidence = probability(record(logprobs?.tool_name)?.minimum_probability, undefined);
			if (logprobTokens > 0 && meanLogprob !== undefined) {
				this.totalForkLogprob += meanLogprob * logprobTokens;
				this.counters.forkLogprobTokens += logprobTokens;
			}
			if (
				!rawCalls.length ||
				calls.length !== rawCalls.length ||
				(state.settings.forkActionMinConfidence > 0 &&
					(confidence === undefined || confidence < state.settings.forkActionMinConfidence))
			)
				continue;
			const fingerprint = stableStringify(calls);
			const score = record(candidate.score);
			const evidence: ActorForkActionEvidence = {
				candidateIDs,
				sources,
				provenance: structuredClone(array(candidate.provenance)),
				actionIdentities: structuredClone(array(candidate.action_identities)),
				draftTokenCount: nonNegativeCount(candidate.draft_token_count),
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
			this.counters.forkExactMatches++;
		}
	}

	private finalizeGateSample(state: TurnState): void {
		if (state.gateSampleRecorded || !state.ended || state.forkBusyMs === undefined) return;
		state.gateSampleRecorded = true;
		this.forkGate.observe(
			state.gateKey,
			{ ...state.forkUtility, costMs: state.forkBusyMs + state.forkUtility.costMs, ...(state.forkFailed ? { failed: true } : {}) },
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
				this.counters.failures++;
				this.lastFailure = errorMessage(error);
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
	const identified = { ...payload, [settings.requestIDField]: requestID };
	if (settings.forkTransport === "sidecar") return identified;
	const { max_tokens, temperature, decoder, forced_prefix, ...fork } = forkPayload(settings);
	return {
		...identified,
		self_speculation: {
			version: 2,
			fork: settings.forkEnabled,
			fork_transport: settings.forkTransport,
			...fork,
			fork_max_tokens: max_tokens,
			fork_temperature: temperature,
			fork_decoder: decoder,
			...(forced_prefix === undefined ? {} : { fork_forced_prefix: forced_prefix }),
			d2: {
				confidence_metric: "minimum_tool_name_probability",
				confidence_threshold: settings.forkActionMinConfidence,
				max_attempts: probeSchedule.maxAttempts,
				retry_token_step: probeSchedule.retryStreamUpdates,
			},
		},
	};
}

function forkPayload(settings: SelfSpeculationSettings) {
	return {
		actor_profile: settings.actorProfile,
		...(settings.draftFormat === "auto" ? {} : { draft_format: settings.draftFormat }),
		max_tokens: settings.forkMaxTokens,
		temperature: settings.forkTemperature,
		decoder: settings.forkDecoder,
		...(settings.forkForcedPrefix === "auto" ? {} : { forced_prefix: settings.forkForcedPrefix }),
		require_logprobs: requiresForkLogprobs(settings),
		max_draft_tokens: settings.maxDraftTokens,
		...(settings.draftBoundary === "auto" ? {} : { draft_boundary: settings.draftBoundary }),
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

function forkGatePolicy(settings: SelfSpeculationSettings): BenefitGatePolicy {
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

function rankedCandidates(
	candidates: Iterable<CandidateRecord>,
	calibration: (candidate: CandidateRecord) => CandidateCalibration,
) {
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
		);
}

function contextPayload(context: Context) {
	return {
		system_prompt: context.systemPrompt,
		messages: structuredClone(context.messages),
		tools: context.tools?.map((tool) => ({
			name: tool.name,
			description: tool.description,
			parameters: structuredClone(tool.parameters),
		})),
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

const originOf = (value: string | undefined) => URL.canParse(value ?? "") ? new URL(value!).origin : undefined;

function decoderEvidenceContext(state: TurnState, tool: string, source: string) {
	return {
		model: state.gateKey,
		endpoint: state.settings.endpoint,
		actorProfile: state.settings.actorProfile,
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
	reportedCandidates: ReadonlyMap<string, ReportedCandidate>,
) {
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
			: [...new Set(candidateIDs.flatMap((id) => [...(reportedCandidates.get(id)?.sources ?? [])].sort()))];
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

function httpPath(value: unknown, fallback: string): string {
	const selected = nonEmptyString(value);
	return selected?.startsWith("/") ? selected : fallback;
}

function requiresForkLogprobs(settings: SelfSpeculationSettings): boolean {
	return (
		settings.requireLogprobs ||
		((settings.forkTransport === "provider" || settings.forkActionEnabled) &&
			settings.forkActionMinConfidence > 0)
	);
}

function textOr(value: unknown, fallback: string): string {
	return nonEmptyString(value) ?? fallback;
}

function nonEmptyString(value: unknown): string | undefined {
	return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function array(value: unknown): readonly unknown[] {
	return Array.isArray(value) ? value : [];
}

function actionIdentity(key: string): string {
	return `action:${hash("sha256", key)}`;
}

function parsedSidecarActionCall(value: unknown, fallbackIndex: number) {
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

function sidecarActionBatchID(fingerprint: string): string {
	return `fork:${hash("sha256", fingerprint).slice(0, 32)}`;
}

function uniqueStrings(value: unknown): string[] {
	return [...new Set(array(value).map(nonEmptyString).filter((item): item is string => item !== undefined))];
}
