import { BenefitGate, DEFAULT_BENEFIT_GATE_POLICY, type BenefitGatePolicy } from "./fork-benefit-gate.ts";

export interface DrafterUtilityBatch {
	readonly key: string;
	readonly generation: number;
	readonly policy: BenefitGatePolicy;
	readonly allowed: boolean;
	readonly expectedRequests: number;
	settledRequests: number;
	costMs: number;
	benefitMs: number;
	failed: boolean;
	finished: boolean;
	observed: boolean;
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

	start(key: string, expectedRequests: number, enabled: boolean): DrafterUtilityBatch {
		const policy = { ...DEFAULT_BENEFIT_GATE_POLICY, enabled };
		const decision = this.gate.decide(key, policy);
		this.latestKey = key;
		if (!decision.allowed) this.skippedBatches++;
		return {
			key,
			generation: this.generation,
			policy,
			allowed: decision.allowed,
			expectedRequests: Number.isFinite(expectedRequests) ? Math.max(1, Math.floor(expectedRequests)) : 1,
			settledRequests: 0,
			costMs: 0,
			benefitMs: 0,
			failed: false,
			finished: false,
			observed: false,
		};
	}

	requestSettled(batch: DrafterUtilityBatch, costMs: number, failed = false): void {
		batch.settledRequests++;
		batch.costMs += Number.isFinite(costMs) ? Math.max(0, costMs) : 0;
		batch.failed ||= failed;
		this.observe(batch);
	}

	creditExecutionAhead(batch: DrafterUtilityBatch, executionAheadMs: number): void {
		batch.benefitMs += Number.isFinite(executionAheadMs) ? Math.max(0, executionAheadMs) : 0;
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
			batch.observed ||
			!batch.allowed ||
			!batch.policy.enabled ||
			!batch.finished ||
			batch.settledRequests < batch.expectedRequests
		)
			return;
		batch.observed = true;
		this.gate.observe(
			batch.key,
			{
				costMs: batch.costMs,
				benefitMs: batch.benefitMs,
				...(batch.failed ? { failed: true } : {}),
			},
			batch.policy,
		);
	}
}
