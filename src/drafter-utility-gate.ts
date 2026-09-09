import { adoptionUtility, BenefitGate, DEFAULT_BENEFIT_GATE_POLICY, type BenefitGatePolicy } from "./fork-benefit-gate.ts";
import type { ActorHitTiming } from "./settlement.ts";

export interface DrafterUtilityBatch {
	readonly key: string;
	readonly generation: number;
	readonly policy: BenefitGatePolicy;
	readonly allowed: boolean;
	startedRequests: number;
	pendingRequests: number;
	costMs: number;
	benefitMs: number;
	failed: boolean;
	finished: boolean;
	update?: ReturnType<BenefitGate["observe"]>;
}

export interface DrafterUtilityGateSnapshot {
	readonly skippedBatches: number;
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
}

/** Batch-atomic action utility accounting over the shared rolling benefit policy. */
export class DrafterUtilityGate {
	private readonly gate = new BenefitGate();
	private generation = 0;
	private skippedBatches = 0;
	private latestKey?: string;

	start(key: string, enabled: boolean): DrafterUtilityBatch {
		const policy = { ...DEFAULT_BENEFIT_GATE_POLICY, enabled };
		const decision = this.gate.decide(key, policy);
		this.latestKey = key;
		if (!decision.allowed) this.skippedBatches++;
		return {
			key,
			generation: this.generation,
			policy,
			allowed: decision.allowed,
			startedRequests: 0,
			pendingRequests: 0,
			costMs: 0,
			benefitMs: 0,
			failed: false,
			finished: false,
		};
	}

	requestStarted(batch: DrafterUtilityBatch): void {
		batch.startedRequests++;
		batch.pendingRequests++;
	}

	requestSettled(batch: DrafterUtilityBatch, costMs: number, failed = false): void {
		batch.pendingRequests--;
		batch.costMs += Number.isFinite(costMs) ? Math.max(0, costMs) : 0;
		batch.failed ||= failed;
		this.observe(batch);
	}

	creditAdoption(batch: DrafterUtilityBatch, timing: ActorHitTiming): void {
		const utility = adoptionUtility(timing);
		batch.costMs += utility.costMs;
		batch.benefitMs += utility.benefitMs;
		this.observe(batch);
	}

	finish(batch: DrafterUtilityBatch): void {
		batch.finished = true;
		this.observe(batch);
	}

	snapshot(): DrafterUtilityGateSnapshot {
		const state = this.latestKey ? this.gate.snapshot(this.latestKey) : undefined;
		return {
			skippedBatches: this.skippedBatches,
			samples: state?.samples ?? 0,
			...(state?.expectedNetBenefitMs === undefined
				? {}
				: { expectedNetBenefitMs: state.expectedNetBenefitMs }),
		};
	}

	reset(): void {
		this.generation++;
		this.gate.reset();
		this.skippedBatches = 0;
		this.latestKey = undefined;
	}

	private observe(batch: DrafterUtilityBatch): void {
		if (
			batch.generation !== this.generation ||
			!batch.allowed ||
			!batch.policy.enabled ||
			!batch.finished ||
			batch.startedRequests === 0 || batch.pendingRequests > 0
		)
			return;
		const observation = { costMs: batch.costMs, benefitMs: batch.benefitMs, failed: batch.failed };
		if (batch.update) batch.update(observation);
		else batch.update = this.gate.observe(batch.key, observation, batch.policy);
	}
}
