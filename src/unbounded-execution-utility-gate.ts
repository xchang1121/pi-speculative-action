import type { CandidateEventDescriptor, CandidateExecutionProjection, SpeculativeActionEvent } from "./events.ts";
import type { SpeculativeExecution, SpeculativeExecutionRoute } from "./execution-world.ts";
import {
	BenefitGate,
	DEFAULT_BENEFIT_GATE_POLICY,
	type BenefitDecision,
	type BenefitGatePolicy,
} from "./fork-benefit-gate.ts";
import type { PredictionIdentity } from "./settlement.ts";

const CANDIDATE_LEDGER_LIMIT = 4_096;
const CLOSED_TURN_LIMIT = 4_096;
const DESCRIPTOR_LIMIT = 256;
const OPEN_TURN_LIMIT = 4_096;
const SUPPRESSED_PREDICTION_LIMIT = 4_096;

export interface UnboundedExecutionUtilityDescriptor {
	readonly source: string;
	readonly tool: string;
	readonly execution: SpeculativeExecution;
	readonly backend: string;
	readonly routeFingerprint: string;
}

export interface UnboundedExecutionUtilityAdmission {
	readonly sessionID: string;
	readonly turnID: string;
	readonly prediction: Pick<PredictionIdentity, "source" | "proposalID" | "actionID">;
	readonly descriptor: UnboundedExecutionUtilityDescriptor;
	readonly enabled: boolean;
}

export interface UnboundedExecutionUtilityGateEntrySnapshot extends UnboundedExecutionUtilityDescriptor {
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
	readonly consecutiveFailures: number;
	readonly suppressedDecisions: number;
}

export interface UnboundedExecutionUtilityGateSnapshot {
	readonly candidateExecutions: number;
	readonly suppressedCandidates: number;
	readonly shadowMatches: number;
	readonly samples: number;
	readonly entries: readonly UnboundedExecutionUtilityGateEntrySnapshot[];
}

interface TurnObservation {
	costMs: number;
	benefitMs: number;
	failed: boolean;
}

interface SuppressedPrediction {
	readonly gateKey: string;
	readonly suppressedAt: number;
}

interface TurnLedger {
	readonly observations: Map<string, TurnObservation>;
	readonly activeCandidates: Set<string>;
	readonly suppressedPredictions: Map<string, SuppressedPrediction>;
	finished: boolean;
}

interface CandidateLedger {
	readonly gateKey: string;
	readonly costMs: number;
	credited: boolean;
}

/**
 * Source/tool/backend-scoped utility accounting for speculative unbounded actions.
 *
 * Misses retain their real execution cost. The first Actor adoption of completed
 * work cancels that cost and credits only realized execution-ahead. A prediction
 * suppressed by this gate remains structurally matchable in the Runtime; a later
 * Actor fallback can therefore contribute shadow benefit without fabricating a hit.
 */
export class UnboundedExecutionUtilityGate {
	private readonly gate = new BenefitGate();
	private readonly policy: BenefitGatePolicy;
	private readonly descriptors = new Map<string, UnboundedExecutionUtilityDescriptor>();
	private readonly turns = new Map<string, TurnLedger>();
	private readonly closedTurns = new Set<string>();
	private readonly candidateLedgers = new Map<string, CandidateLedger>();
	private readonly terminalCandidates = new Set<string>();
	private candidateExecutions = 0;
	private suppressedCandidates = 0;
	private shadowMatches = 0;
	private readonly now: () => number;

	constructor(
		policy: BenefitGatePolicy = DEFAULT_BENEFIT_GATE_POLICY,
		now: () => number = () => performance.now(),
	) {
		this.policy = { ...policy };
		this.now = now;
	}

	decide(input: UnboundedExecutionUtilityAdmission): BenefitDecision {
		const gateKey = descriptorKey(input.descriptor);
		this.rememberDescriptor(gateKey, input.descriptor);
		const decision = this.gate.decide(gateKey, { ...this.policy, enabled: input.enabled });
		if (decision.allowed) return decision;
		this.suppressedCandidates++;
		const ledger = this.turn(input.sessionID, input.turnID);
		if (ledger) {
			const predictionKey = predictionIdentityKey(input.prediction);
			const existing = ledger.suppressedPredictions.get(predictionKey);
			const suppressedAt = this.now();
			if (!existing || suppressedAt < existing.suppressedAt) {
				ledger.suppressedPredictions.set(predictionKey, { gateKey, suppressedAt });
				trimMap(ledger.suppressedPredictions, SUPPRESSED_PREDICTION_LIMIT);
			}
		}
		return decision;
	}

	recordEvent<SessionID>(event: SpeculativeActionEvent<SessionID>): void {
		if (event.type === "candidate") {
			this.recordCandidate(event.sessionID, event.turnID, event.candidate, event.state);
			return;
		}
		if (event.type === "actor_action") this.recordActorAction(event);
	}

	finishTurn(sessionID: unknown, turnID: string): void {
		const key = turnKey(sessionID, turnID);
		if (this.closedTurns.has(key)) return;
		const ledger = this.turns.get(key);
		if (!ledger) {
			this.closeTurn(key);
			return;
		}
		ledger.finished = true;
		this.finalizeTurn(key, ledger);
	}

	snapshot(): UnboundedExecutionUtilityGateSnapshot {
		const entries = [...this.descriptors.entries()]
			.map(([key, descriptor]): UnboundedExecutionUtilityGateEntrySnapshot => ({
				...descriptor,
				...this.gate.snapshot(key),
			}))
			.sort(
				(left, right) =>
					left.source.localeCompare(right.source) ||
					left.tool.localeCompare(right.tool) ||
					left.backend.localeCompare(right.backend) ||
					left.routeFingerprint.localeCompare(right.routeFingerprint),
			);
		return {
			candidateExecutions: this.candidateExecutions,
			suppressedCandidates: this.suppressedCandidates,
			shadowMatches: this.shadowMatches,
			samples: entries.reduce((total, entry) => total + entry.samples, 0),
			entries,
		};
	}

	reset(): void {
		this.gate.reset();
		this.descriptors.clear();
		this.turns.clear();
		this.closedTurns.clear();
		this.candidateLedgers.clear();
		this.terminalCandidates.clear();
		this.candidateExecutions = 0;
		this.suppressedCandidates = 0;
		this.shadowMatches = 0;
	}

	private recordCandidate(
		sessionID: unknown,
		turnID: string,
		candidate: CandidateEventDescriptor,
		state: CandidateExecutionProjection,
	): void {
		if (candidate.origin !== "prediction") return;
		const descriptor = candidateDescriptor(candidate);
		const gateKey = descriptorKey(descriptor);
		const key = turnKey(sessionID, turnID);
		if (!this.descriptors.has(gateKey)) {
			// A dynamic runtime fingerprint may have aged out while its process was
			// still running. Ignore its obsolete utility, but do not strand the turn.
			const existing = this.turns.get(key);
			if (state.status !== "running" && existing?.activeCandidates.delete(candidate.id)) {
				this.finalizeTurn(key, existing);
			}
			return;
		}
		const ledger = this.turn(sessionID, turnID);
		if (!ledger) return;
		if (state.status === "running") {
			ledger.activeCandidates.add(candidate.id);
			return;
		}
		if (this.terminalCandidates.has(candidate.id)) return;
		const started = state.status === "succeeded" || state.startedAt !== undefined || ledger.activeCandidates.has(candidate.id);
		ledger.activeCandidates.delete(candidate.id);
		if (!started) {
			this.finalizeTurn(key, ledger);
			return;
		}
		this.terminalCandidates.add(candidate.id);
		trimSet(this.terminalCandidates, CANDIDATE_LEDGER_LIMIT);
		const costMs = metric(state.executionMs);
		const observation = this.observation(ledger, gateKey);
		observation.costMs += costMs;
		observation.failed ||= state.status === "failed";
		this.candidateLedgers.set(candidate.id, { gateKey, costMs, credited: false });
		trimMap(this.candidateLedgers, CANDIDATE_LEDGER_LIMIT);
		this.candidateExecutions++;
		this.finalizeTurn(key, ledger);
	}

	private recordActorAction<SessionID>(
		event: Extract<SpeculativeActionEvent<SessionID>, { readonly type: "actor_action" }>,
	): void {
		const provider = event.settlement.provider;
		if (provider.kind === "speculative" && event.candidate?.origin === "prediction") {
			const descriptor = candidateDescriptor(event.candidate);
			const gateKey = descriptorKey(descriptor);
			if (!this.descriptors.has(gateKey)) return;
			const ledger = this.turn(event.sessionID, event.turnID);
			if (!ledger) return;
			const observation = this.observation(ledger, gateKey);
			observation.benefitMs += metric(provider.timing.executionAheadMs);
			const candidate = this.candidateLedgers.get(provider.candidateID);
			if (candidate && candidate.gateKey === gateKey && !candidate.credited) {
				candidate.credited = true;
				observation.benefitMs += candidate.costMs;
			}
			return;
		}
		if (provider.kind !== "actor" || provider.origin !== "fallback" || !provider.executionBlockedTiming) return;
		const ledger = this.turns.get(turnKey(event.sessionID, event.turnID));
		if (!ledger) return;
		const matched = new Map<string, number>();
		for (const prediction of event.settlement.matchedPredictions) {
			const suppressed = ledger.suppressedPredictions.get(predictionIdentityKey(prediction));
			if (!suppressed) continue;
			const descriptor = this.descriptors.get(suppressed.gateKey);
			if (!descriptor || descriptor.tool !== event.settlement.tool) continue;
			const previous = matched.get(suppressed.gateKey);
			if (previous === undefined || suppressed.suppressedAt < previous) {
				matched.set(suppressed.gateKey, suppressed.suppressedAt);
			}
		}
		for (const [gateKey, suppressedAt] of matched) {
			const gateSpecificLeadMs = Math.max(0, provider.toolExecution.startedAt - suppressedAt);
			const benefitMs = Math.min(
				metric(provider.executionBlockedTiming.executionAheadMs),
				gateSpecificLeadMs,
			);
			if (benefitMs <= 0) continue;
			this.observation(ledger, gateKey).benefitMs += benefitMs;
			this.shadowMatches++;
		}
	}

	private observation(ledger: TurnLedger, gateKey: string): TurnObservation {
		const existing = ledger.observations.get(gateKey);
		if (existing) return existing;
		const created: TurnObservation = { costMs: 0, benefitMs: 0, failed: false };
		ledger.observations.set(gateKey, created);
		return created;
	}

	private turn(sessionID: unknown, turnID: string): TurnLedger | undefined {
		const key = turnKey(sessionID, turnID);
		if (this.closedTurns.has(key)) return undefined;
		const existing = this.turns.get(key);
		if (existing) return existing;
		const created: TurnLedger = {
			observations: new Map(),
			activeCandidates: new Set(),
			suppressedPredictions: new Map(),
			finished: false,
		};
		this.turns.set(key, created);
		while (this.turns.size > OPEN_TURN_LIMIT) {
			const oldest = this.turns.keys().next().value as string | undefined;
			if (oldest === undefined) break;
			this.turns.delete(oldest);
			this.closeTurn(oldest);
		}
		return created;
	}

	private finalizeTurn(key: string, ledger: TurnLedger): void {
		if (!ledger.finished || ledger.activeCandidates.size > 0) return;
		for (const [gateKey, observation] of ledger.observations) {
			if (this.descriptors.has(gateKey)) this.gate.observe(gateKey, observation, this.policy);
		}
		this.turns.delete(key);
		this.closeTurn(key);
	}

	private closeTurn(key: string): void {
		this.closedTurns.add(key);
		while (this.closedTurns.size > CLOSED_TURN_LIMIT) {
			const oldest = this.closedTurns.values().next().value as string | undefined;
			if (oldest === undefined) break;
			this.closedTurns.delete(oldest);
		}
	}

	private rememberDescriptor(gateKey: string, descriptor: UnboundedExecutionUtilityDescriptor): void {
		// Treat this as an LRU: externally supplied runtime fingerprints may change
		// over a long-lived session, but obsolete calibration must not grow forever.
		this.descriptors.delete(gateKey);
		this.descriptors.set(gateKey, Object.freeze({ ...descriptor }));
		while (this.descriptors.size > DESCRIPTOR_LIMIT) {
			const oldest = this.descriptors.keys().next().value as string | undefined;
			if (oldest === undefined) break;
			this.descriptors.delete(oldest);
			this.gate.delete(oldest);
		}
	}
}

export function unboundedExecutionUtilityDescriptor(input: {
	readonly source: string;
	readonly tool: string;
	readonly route: SpeculativeExecutionRoute;
}): UnboundedExecutionUtilityDescriptor {
	return {
		source: input.source,
		tool: input.tool,
		execution: input.route.isolation,
		backend: input.route.backend,
		routeFingerprint: input.route.fingerprint,
	};
}

function candidateDescriptor(candidate: CandidateEventDescriptor): UnboundedExecutionUtilityDescriptor {
	return {
		source: candidate.source,
		tool: candidate.tool,
		execution: candidate.execution,
		backend: candidate.executionBackend,
		routeFingerprint: candidate.executionFingerprint,
	};
}

function descriptorKey(descriptor: UnboundedExecutionUtilityDescriptor): string {
	return JSON.stringify([
		descriptor.source,
		descriptor.tool,
		descriptor.execution,
		descriptor.backend,
		descriptor.routeFingerprint,
	]);
}

function predictionIdentityKey(
	prediction: Pick<PredictionIdentity, "source" | "proposalID" | "actionID">,
): string {
	return JSON.stringify([prediction.source, prediction.proposalID, prediction.actionID]);
}

function turnKey(sessionID: unknown, turnID: string): string {
	return JSON.stringify([String(sessionID), turnID]);
}

function metric(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function trimMap<Key, Value>(map: Map<Key, Value>, limit: number): void {
	while (map.size > limit) {
		const oldest = map.keys().next().value as Key | undefined;
		if (oldest === undefined) break;
		map.delete(oldest);
	}
}

function trimSet<Value>(set: Set<Value>, limit: number): void {
	while (set.size > limit) {
		const oldest = set.values().next().value as Value | undefined;
		if (oldest === undefined) break;
		set.delete(oldest);
	}
}
