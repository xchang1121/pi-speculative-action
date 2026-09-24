import { creditAdoption, BenefitGate, DEFAULT_BENEFIT_GATE_POLICY, type BenefitGatePolicy } from "./fork-benefit-gate.ts";
import type { ActorHitTiming } from "./settlement.ts";

export interface DrafterUtilityBatch {
	readonly key: string;
	readonly policy: BenefitGatePolicy;
	readonly allowed: boolean;
	startedRequests: number;
	pendingRequests: number;
	costMs: number;
	benefitMs: number | undefined;
	failed: boolean;
	finished: boolean;
	update?: ReturnType<BenefitGate["observe"]>;
}

export interface DrafterUtilityGateSnapshot {
	readonly skippedBatches: number;
	readonly samples: number;
	readonly expectedNetBenefitMs?: number;
}

/**
 * Batch-atomic action utility over the shared rolling benefit policy. Drafter requests run beside the Actor, so only
 * adoption latency is Actor-visible cost; request tokens are a separate budget. State spans prompts of one session.
 */
export class DrafterUtilityGate {
	private readonly gate = new BenefitGate();
	private skippedBatches = 0;
	private latestKey?: string;

	start(key: string, enabled: boolean): DrafterUtilityBatch {
		const policy = { ...DEFAULT_BENEFIT_GATE_POLICY, enabled };
		const decision = this.gate.decide(key, policy);
		this.latestKey = key;
		if (!decision.allowed) this.skippedBatches++;
		return { key, policy, allowed: decision.allowed, startedRequests: 0, pendingRequests: 0, costMs: 0, benefitMs: 0, failed: false, finished: false };
	}

	requestStarted(batch: DrafterUtilityBatch): void {
		batch.startedRequests++;
		batch.pendingRequests++;
	}

	requestSettled(batch: DrafterUtilityBatch, failed = false): void {
		batch.pendingRequests--;
		batch.failed ||= failed;
		this.observe(batch);
	}

	creditAdoption(batch: DrafterUtilityBatch, timing: ActorHitTiming, shares?: number): void {
		creditAdoption(batch, timing, shares);
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

	private observe(batch: DrafterUtilityBatch): void {
		if (!batch.allowed || !batch.policy.enabled || !batch.finished || batch.startedRequests === 0 || batch.pendingRequests > 0) return;
		if (batch.update) batch.update(batch);
		else batch.update = this.gate.observe(batch.key, batch, batch.policy);
	}
}
