import { nonNegativeCount as sequence, nonNegativeFinite as finite, positiveCount as units } from "./number-utils.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import type { WorldCompatibilityEvidence } from "./execution-world.ts";
import { DEFAULT_BENEFIT_GATE_POLICY } from "./fork-benefit-gate.ts";
import { fitsResourceBudget } from "./resource-budget.ts";

export interface PredictionForecast extends ServiceTimingIdentity {
	readonly expectedDurationMs?: number;
	readonly resourceDemand?: number;
	readonly decisionBatchesUntilCall?: number;
	readonly actorPhase?: {
		readonly kind: "decision" | "cycle";
		readonly elapsedMs: number;
	};
	readonly criticalPathMs?: number;
	readonly expectedLatencyBenefitMs?: number;
	readonly background?: boolean;
	/** Dependencies have settled and this action is their immediate zero-horizon successor. */
	readonly dependenciesResolved?: boolean;
}

export interface ScheduledWork {
	readonly expectedDurationMs: number;
	readonly resourceUnits: number;
	readonly decisionBatchesUntilCall: number;
	readonly criticalPathMs: number;
	readonly priorityMs: number;
	readonly background: boolean;
}

export interface ServiceTimingIdentity {
	readonly tool: string;
	/** Stable execution environment shared by comparable service samples. */
	readonly executionFingerprint?: string;
	/** Exact K(a) or producer/consumer pair, before falling back to the wider timing class. */
	readonly actionKeyHash?: string;
	/** Distinct work within one executor, such as exact adoption versus input re-evaluation. */
	readonly operation?: string;
}

export interface CandidateJoinPolicy {
	/** Required estimated Actor critical-path saving before waiting for unfinished work. */
	readonly minNetBenefitMs: number;
	/** Initial Actor wait cap; omission preserves eager adoption while a route learns. */
	readonly uncalibratedWaitMs?: number;
	/** Uncertainty allowance added to the estimated remaining-time deadline during warm-up. */
	readonly warmupWaitMs: number;
	/** Slack applied to a high-quantile remaining-time estimate. */
	readonly durationSlack: number;
}

export interface CandidateJoinRequest {
	/** Producer service; omitted consumer/adoption identities preserve exact-replay callers. */
	readonly identity: ServiceTimingIdentity;
	readonly actorIdentity?: ServiceTimingIdentity;
	readonly adoptionIdentity?: ServiceTimingIdentity;
	readonly state: "queued" | "running" | "succeeded";
	readonly expectedSpeculativeDurationMs: number;
	readonly elapsedMs?: number;
}

type CandidateJoinReason = "ready" | "warmup_probe" | "profitable" | "fallback_faster";

export interface CandidateJoinDecision {
	readonly allowed: boolean;
	readonly reason: CandidateJoinReason;
	/** Zero for a completed candidate. A finite positive value is an Actor-side deadline. */
	readonly waitBudgetMs: number;
	readonly speculativeSamples: number;
	readonly actorSamples: number;
	readonly adoptionSamples: number;
	readonly expectedRemainingMs: number;
	readonly expectedAdoptionMs: number;
	readonly expectedActorMs?: number;
	readonly expectedNetBenefitMs?: number;
}

export type CandidateWaitResult<T> =
	| { readonly status: "completed"; readonly value: T }
	| { readonly status: "aborted" }
	| { readonly status: "deadline" };

/** Owns cancellation/deadline settlement for producer requests and in-flight adoption. */
export async function waitForCandidate<T>(
	promise: Promise<T>,
	signal?: AbortSignal,
	waitBudgetMs?: number,
): Promise<CandidateWaitResult<T>> {
	if (signal?.aborted) {
		void promise.catch(() => undefined);
		return { status: "aborted" };
	}
	const bounded = waitBudgetMs !== undefined && Number.isFinite(waitBudgetMs);
	if (!signal && !bounded) return { status: "completed", value: await promise };
	return new Promise((resolve, reject) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const finish = (complete: () => void) => {
			if (settled) return;
			settled = true;
			if (timer) clearTimeout(timer);
			signal?.removeEventListener("abort", aborted);
			complete();
		};
		const aborted = () => finish(() => resolve({ status: "aborted" }));
		signal?.addEventListener("abort", aborted, { once: true });
		if (bounded) timer = setTimeout(() => finish(() => resolve({ status: "deadline" })), Math.max(0, waitBudgetMs));
		void promise.then(
			(value) => finish(() => resolve({ status: "completed", value })),
			(error) => finish(() => reject(error)),
		);
	});
}

export type SchedulerAdmission =
	| { readonly admitted: true; readonly work: ScheduledWork }
	| {
			readonly admitted: false;
			readonly work: ScheduledWork;
			readonly reason: "budget_exhausted" | "not_profitable" | "failure_circuit";
	  };

export type WorldCompatibilityDecision =
	| { readonly compatible: true }
	| {
			readonly compatible: false;
			readonly code: "backend_incompatible" | "backend_indeterminate" | "execution_fingerprint_changed";
			readonly detail?: string;
	  };

interface SchedulerEntry<Job> {
	readonly job: Job;
	work: ScheduledWork;
	readonly sequence: number;
}

/** Owns forecast aggregation, timing observations, capacity, and preemption. */
export class SpeculationScheduler<Job extends object> {
	private readonly entries = new Map<Job, SchedulerEntry<Job>>();
	private readonly speculativeServiceTimes = new BoundedRecencyMap<string, SampleWindow>(1024);
	private readonly actorServiceTimes = new BoundedRecencyMap<string, SampleWindow>(1024);
	private readonly adoptionTimes = new BoundedRecencyMap<string, SampleWindow>(1024);
	private readonly actorDecisionDurations = new SampleWindow();
	private readonly actorCycles = new SampleWindow();
	private readonly candidateJoinPolicy: CandidateJoinPolicy;
	private sequence = 0;
	private decisionSequence = 0;

	constructor(options: { readonly candidateJoinPolicy?: Partial<CandidateJoinPolicy> } = {}) {
		const policy = options.candidateJoinPolicy;
		this.candidateJoinPolicy = Object.freeze({
			minNetBenefitMs: finite(policy?.minNetBenefitMs ?? DEFAULT_BENEFIT_GATE_POLICY.minNetBenefitMs),
			...(policy?.uncalibratedWaitMs === undefined ? {} : { uncalibratedWaitMs: finite(policy.uncalibratedWaitMs) }),
			warmupWaitMs: finite(policy?.warmupWaitMs ?? 25),
			durationSlack: Math.max(1, finite(policy?.durationSlack ?? 1.25)),
		});
	}

	admit(
		job: Job,
		forecasts: readonly PredictionForecast[],
		capacity: number,
		role: "producer" | "actor" = "producer",
		/** Ranking may supply its estimate from the same synchronous admission pass. */
		work: ScheduledWork = this.evaluate(forecasts),
		/** Physical producer identity; consumer forecasts can describe projected actions. Omit for confirmed Actor previews. */
		executionIdentity?: ServiceTimingIdentity,
	): SchedulerAdmission {
		if (role === "producer") {
			if (forecasts.length && !forecasts.some((forecast) => this.canLaunch(forecast, work.expectedDurationMs)))
				return { admitted: false, work, reason: "not_profitable" };
			if (!fitsResourceBudget(this.entries.values(), work.resourceUnits, capacity))
				return { admitted: false, work, reason: "budget_exhausted" };
			if (executionIdentity?.actionKeyHash &&
				this.speculativeServiceTimes.get(timingKeys(executionIdentity)[0]!)?.allowExecution(job, this.decisionSequence) === false)
				return { admitted: false, work, reason: "failure_circuit" };
		}
		this.entries.set(job, { job, work, sequence: this.sequence++ });
		return { admitted: true, work };
	}

	refresh(job: Job, forecasts: readonly PredictionForecast[]): ScheduledWork | undefined {
		const entry = this.entries.get(job);
		if (!entry) return undefined;
		entry.work = this.evaluate(forecasts);
		return entry.work;
	}

	complete(job: Job): boolean {
		return this.entries.delete(job);
	}

	/** Choose cancellation victims; only their executor completion returns physical capacity. */
	preemptFor(
		resourceUnits: number,
		capacity: number,
		canPreempt: (job: Job) => boolean = () => true,
	): readonly Job[] {
		const remaining = [...this.entries.values()];
		const victims: Job[] = [];
		while (!fitsResourceBudget(remaining, resourceUnits, capacity)) {
			const victim = remaining
				.filter((entry) => canPreempt(entry.job))
				.sort(compareVictim)[0];
			if (!victim) break;
			remaining.splice(remaining.indexOf(victim), 1);
			victims.push(victim.job);
		}
		return victims;
	}

	evaluate(forecasts: readonly PredictionForecast[]): ScheduledWork {
		let remaining = forecasts.length;
		const work = forecasts.reduce((work, forecast) => {
			remaining--;
			const expectedDurationMs = this.duration(forecast) ?? 1;
			const criticalPathMs = Math.max(expectedDurationMs, finite(forecast.criticalPathMs));
			const runwayMs = this.actorRunway(forecast);
			const benefitDurationMs = positive(forecast.expectedDurationMs, expectedDurationMs);
			const runwayScale = runwayMs === undefined ? 1 : Math.min(1, runwayMs / benefitDurationMs);
			work.expectedDurationMs = Math.max(work.expectedDurationMs, expectedDurationMs);
			work.resourceUnits = Math.max(work.resourceUnits, units(forecast.resourceDemand));
			work.decisionBatchesUntilCall = Math.min(work.decisionBatchesUntilCall, sequence(forecast.decisionBatchesUntilCall));
			work.criticalPathMs = Math.max(work.criticalPathMs, criticalPathMs);
			work.priorityMs = Math.max(work.priorityMs, forecast.expectedLatencyBenefitMs === undefined
				? criticalPathMs : finite(forecast.expectedLatencyBenefitMs) * runwayScale);
			work.background = forecast.background === true && work.background;
			return work;
		}, {
			expectedDurationMs: 0,
			resourceUnits: 1,
			decisionBatchesUntilCall: remaining ? Infinity : 0,
			criticalPathMs: 0,
			priorityMs: 0,
			background: remaining > 0,
		});
		// Missing slots leave the numerical forecast indeterminate.
		if (remaining) {
			work.expectedDurationMs = work.resourceUnits = work.decisionBatchesUntilCall =
				work.criticalPathMs = work.priorityMs = NaN;
		}
		return work;
	}

	launchDelay(forecast: PredictionForecast, safetyMarginMs = 10): number {
		if (forecast.dependenciesResolved || sequence(forecast.decisionBatchesUntilCall) <= 1) return 0;
		const duration = this.duration(forecast, 0.9);
		if (duration === undefined) return 0;
		const availableMs = this.actorRunway(forecast, forecast.actorPhase ?? { kind: "cycle", elapsedMs: 0 }) ?? 0;
		return Math.max(0, availableMs - duration - finite(safetyMarginMs));
	}

	assessCompatibility(
		evidence: WorldCompatibilityEvidence,
		actorExecutionFingerprint: string,
	): WorldCompatibilityDecision {
		if (evidence.status !== "compatible") {
			return {
				compatible: false,
				code: evidence.status === "incompatible" ? "backend_incompatible" : "backend_indeterminate",
				detail: evidence.detail ?? evidence.code,
			};
		}
		return evidence.executionFingerprint === actorExecutionFingerprint
			? { compatible: true }
			: { compatible: false, code: "execution_fingerprint_changed" };
	}

	observeActorTiming(decisionDurationMs: number, cycleDurationMs?: number): void {
		this.decisionSequence++;
		this.actorDecisionDurations.observe(decisionDurationMs);
		if (cycleDurationMs !== undefined) this.actorCycles.observe(cycleDurationMs);
	}

	observeSpeculativeService(identity: ServiceTimingIdentity, durationMs: number, failed = false): void {
		if (!failed) this.observeTiming(this.speculativeServiceTimes, identity, durationMs);
		else if (identity.actionKeyHash) {
			// Failed attempts cannot stand in for successful service or affect unrelated actions in the timing class.
			const key = timingKeys(identity)[0]!, samples = this.speculativeServiceTimes.get(key) ?? new SampleWindow();
			samples.observeFailure();
			this.speculativeServiceTimes.set(key, samples);
		}
	}

	observeActorService(identity: ServiceTimingIdentity, durationMs: number): void {
		this.observeTiming(this.actorServiceTimes, identity, durationMs);
	}

	observeAdoption(identity: ServiceTimingIdentity, durationMs: number): void {
		this.observeTiming(this.adoptionTimes, identity, durationMs);
	}

	/**
	 * Decide whether the Actor should adopt speculative work. A rejected candidate keeps
	 * running until ordinary invalidation, so both sides of the comparison can continue learning.
	 */
	assessCandidateJoin(request: CandidateJoinRequest): CandidateJoinDecision {
		const policy = this.candidateJoinPolicy;
		const speculative = this.timingEstimate(this.speculativeServiceTimes, request.identity, 0.9, "upper");
		const actor = this.timingEstimate(this.actorServiceTimes, request.actorIdentity ?? request.identity, 0.25);
		const adoption = this.timingEstimate(this.adoptionTimes, request.adoptionIdentity ?? request.identity, 0.75, "upper");
		const expectedActorMs = actor?.value;
		const expectedSpeculativeMs =
			speculative?.value ?? positive(request.expectedSpeculativeDurationMs, 1);
		const elapsedMs = request.state === "running" ? finite(request.elapsedMs) : 0;
		const expectedRemainingMs =
			request.state === "succeeded" ? 0 : Math.max(0, expectedSpeculativeMs - elapsedMs);
		const expectedAdoptionMs = adoption?.value ?? 0;
		const expectedNetBenefitMs =
			expectedActorMs === undefined
				? undefined
				: expectedActorMs - expectedRemainingMs - expectedAdoptionMs;
		const base = {
			speculativeSamples: speculative?.samples ?? 0,
			actorSamples: actor?.samples ?? 0,
			adoptionSamples: adoption?.samples ?? 0,
			expectedRemainingMs,
			expectedAdoptionMs,
			...(expectedActorMs === undefined ? {} : { expectedActorMs }),
			...(expectedNetBenefitMs === undefined ? {} : { expectedNetBenefitMs }),
		};

		if (request.state === "succeeded") {
			// Repeated loss must sample the alternative; cached hits cannot grow Actor evidence.
			if (
				actor?.exact && adoption?.exact &&
				adoption.samples >= DEFAULT_BENEFIT_GATE_POLICY.minSamples &&
				expectedNetBenefitMs !== undefined &&
				expectedNetBenefitMs < 0
			) {
				const allowed = adoption.window.allowProbe();
				return { allowed, reason: allowed ? "ready" : "fallback_faster", waitBudgetMs: 0, ...base };
			}
			return { allowed: true, reason: "ready", waitBudgetMs: 0, ...base };
		}

		if (expectedActorMs === undefined) {
			const waitBudgetMs = policy.uncalibratedWaitMs ?? Number.POSITIVE_INFINITY;
			return {
				allowed: waitBudgetMs > 0,
				reason: "warmup_probe",
				waitBudgetMs,
				...base,
			};
		}
		if (expectedNetBenefitMs === undefined || expectedNetBenefitMs < policy.minNetBenefitMs) {
			return { allowed: false, reason: "fallback_faster", waitBudgetMs: 0, ...base };
		}
		const actorDeadlineMs = Math.max(
			0,
			expectedActorMs - expectedAdoptionMs - policy.minNetBenefitMs,
		);
		const estimatedDeadlineMs = expectedRemainingMs * policy.durationSlack + policy.warmupWaitMs;
		const waitBudgetMs = Math.min(actorDeadlineMs, estimatedDeadlineMs);
		if (waitBudgetMs <= 0) {
			return { allowed: false, reason: "fallback_faster", waitBudgetMs: 0, ...base };
		}
		return {
			allowed: true,
			reason: speculative ? "profitable" : "warmup_probe",
			waitBudgetMs,
			...base,
		};
	}

	snapshot(): readonly { readonly job: Job; readonly work: ScheduledWork }[] {
		return [...this.entries.values()]
			.sort((left, right) => left.sequence - right.sequence)
			.map(({ job, work }) => ({ job, work }));
	}

	/** With exact Actor evidence and an explicit forecast, avoid launching work its consumer would reject. */
	private canLaunch(forecast: PredictionForecast, expectedDurationMs: number): boolean {
		const runway = this.actorRunway(forecast);
		if (runway === undefined || forecast.expectedDurationMs === undefined || !forecast.actionKeyHash ||
			!this.actorServiceTimes.get(timingKeys(forecast)[0]!)?.count) return true;
		return this.assessCandidateJoin({
			identity: forecast,
			state: "running",
			expectedSpeculativeDurationMs: expectedDurationMs,
			elapsedMs: runway,
		}).allowed;
	}

	private actorRunway(forecast: PredictionForecast, phase = forecast.actorPhase): number | undefined {
		if (!phase) return undefined;
		const cycleMs = this.actorCycles.estimate(0.25);
		const decisionMs = this.actorDecisionDurations.estimate(0.25);
		const decisions = sequence(forecast.decisionBatchesUntilCall);
		if (phase.kind === "decision") {
			if (decisionMs === undefined) return undefined;
			const futureCycles = Math.max(0, decisions - 1);
			if (futureCycles > 0 && cycleMs === undefined) return undefined;
			return Math.max(0, decisionMs - phase.elapsedMs) + futureCycles * (cycleMs ?? 0);
		}
		if (cycleMs !== undefined) return Math.max(0, decisions * cycleMs - phase.elapsedMs);
		return decisions === 1 ? decisionMs : undefined;
	}

	private duration(forecast: PredictionForecast, quantile = 0.5): number | undefined {
		const observed = this.timingEstimate(
			this.speculativeServiceTimes,
			forecast,
			quantile,
		)?.value;
		// A source's action-specific estimate remains a lower bound. Wider timing classes can
		// conservatively raise scheduling cost, but must not make an explicitly long action look short.
		return Math.max(finite(forecast.expectedDurationMs), observed ?? 0) || undefined;
	}

	private observeTiming(
		windows: BoundedRecencyMap<string, SampleWindow>,
		identity: ServiceTimingIdentity,
		durationMs: number,
	): void {
		for (const key of timingKeys(identity)) {
			const samples = windows.get(key) ?? new SampleWindow();
			samples.observe(durationMs);
			windows.set(key, samples);
		}
	}

	private timingEstimate(
		windows: BoundedRecencyMap<string, SampleWindow>,
		identity: ServiceTimingIdentity,
		quantile: number,
		selection: QuantileSelection = "lower",
	): TimingEstimate | undefined {
		for (const [index, key] of timingKeys(identity).entries()) {
			const window = windows.get(key), value = window?.estimate(quantile, selection);
			if (value !== undefined) return { value, samples: window!.count, window: window!, exact: Boolean(identity.actionKeyHash) && index === 0 };
		}
		return undefined;
	}
}

interface TimingEstimate {
	readonly value: number;
	readonly samples: number;
	readonly window: SampleWindow;
	readonly exact: boolean;
}

class SampleWindow {
	private readonly values: number[] = [];
	private sortedValues?: number[];
	private suppressedSinceProbe = 0;
	private failures?: {
		readonly count: number;
		readonly decisions: WeakMap<object, { readonly sequence: number; readonly allowed: boolean }>;
	};

	get count(): number {
		return this.values.length;
	}

	observe(value: number): void {
		this.failures = undefined;
		const normalized = finite(value);
		if (normalized <= 0) return;
		this.suppressedSinceProbe = 0;
		this.sortedValues = undefined;
		this.values.push(normalized);
		if (this.values.length > 64) this.values.shift();
	}

	observeFailure(): void {
		this.failures = { count: (this.failures?.count ?? 0) + 1, decisions: new WeakMap() };
		this.suppressedSinceProbe = 0;
	}

	/** Dispatch repeats do not consume probes; retained work can probe again after the next Actor decision. */
	allowExecution(job: object, sequence: number): boolean {
		if (!this.failures || this.failures.count < DEFAULT_BENEFIT_GATE_POLICY.failureThreshold) return true;
		let decision = this.failures.decisions.get(job);
		if (!decision || decision.sequence !== sequence) {
			decision = { sequence, allowed: this.allowProbe() };
			this.failures.decisions.set(job, decision);
		}
		return decision.allowed;
	}

	/** The same bounded evidence owns recovery, including decisions made before a probe settles. */
	allowProbe(): boolean {
		if (++this.suppressedSinceProbe < DEFAULT_BENEFIT_GATE_POLICY.probeInterval) return false;
		this.suppressedSinceProbe = 0;
		return true;
	}

	estimate(value: number, selection: QuantileSelection = "lower"): number | undefined {
		if (!this.values.length) return undefined;
		const sorted = this.sortedValues ??= [...this.values].sort((left, right) => left - right);
		const index = (sorted.length - 1) * Math.max(0, Math.min(1, value));
		return sorted[selection === "upper" ? Math.ceil(index) : Math.floor(index)]!;
	}
}

type QuantileSelection = "lower" | "upper";

function timingKeys(identity: ServiceTimingIdentity): readonly string[] {
	const group = [identity.tool, identity.executionFingerprint ?? "", identity.operation ?? ""];
	return [...(identity.actionKeyHash ? [JSON.stringify(["action", ...group, identity.actionKeyHash])] : []),
		JSON.stringify(["class", ...group])];
}

function compareVictim<Job>(left: SchedulerEntry<Job>, right: SchedulerEntry<Job>): number {
	return (
		Number(right.work.background) - Number(left.work.background) ||
		right.work.decisionBatchesUntilCall - left.work.decisionBatchesUntilCall ||
		left.work.priorityMs - right.work.priorityMs ||
		left.work.criticalPathMs - right.work.criticalPathMs ||
		right.sequence - left.sequence
	);
}

function positive(value: number | undefined, fallback: number): number {
	const normalized = finite(value);
	return normalized > 0 ? normalized : Math.max(1, finite(fallback));
}
