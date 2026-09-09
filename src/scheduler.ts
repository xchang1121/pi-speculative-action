import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import type { SpeculativeExecution, WorldCompatibilityEvidence } from "./execution-world.ts";
import { DEFAULT_BENEFIT_GATE_POLICY } from "./fork-benefit-gate.ts";
import type {
	SpeculativeResourceBudget,
	SpeculativeResourceClass,
	SpeculativeResourceProfile,
} from "./resource-budget.ts";
import { resourceProfile, speculativeResourceBudget } from "./resource-budget.ts";

export interface PredictionForecast extends ServiceTimingIdentity {
	readonly execution: SpeculativeExecution;
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
	readonly resource: SpeculativeResourceProfile;
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

const DEFAULT_CANDIDATE_JOIN_POLICY: CandidateJoinPolicy = Object.freeze({
	minNetBenefitMs: DEFAULT_BENEFIT_GATE_POLICY.minNetBenefitMs,
	warmupWaitMs: 25,
	durationSlack: 1.25,
});

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
			readonly reason: "budget_exhausted";
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

	constructor(options: { readonly candidateJoinPolicy?: Partial<CandidateJoinPolicy> } = {}) {
		this.candidateJoinPolicy = normalizeCandidateJoinPolicy(options.candidateJoinPolicy);
	}

	admit(
		job: Job,
		forecasts: readonly PredictionForecast[],
		capacity: number | SpeculativeResourceBudget,
		role: "producer" | "actor" = "producer",
	): SchedulerAdmission {
		const work = this.evaluate(forecasts);
		const budget = normalizeBudget(capacity);
		if (role === "producer" && !fits([...this.entries.values()], work.resource, budget)) {
			return { admitted: false, work, reason: "budget_exhausted" };
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
		resource: SpeculativeResourceProfile,
		capacity: number | SpeculativeResourceBudget,
		canPreempt: (job: Job) => boolean = () => true,
	): readonly Job[] {
		const budget = normalizeBudget(capacity);
		const remaining = [...this.entries.values()];
		const victims: SchedulerEntry<Job>[] = [];
		while (
			!fits(
				remaining.filter((entry) => !victims.includes(entry)),
				resource,
				budget,
			)
		) {
			const victim = remaining
				.filter((entry) => !victims.includes(entry) && canPreempt(entry.job))
				.sort(compareVictim)[0];
			if (!victim) break;
			victims.push(victim);
		}
		return victims.map((entry) => entry.job);
	}

	evaluate(forecasts: readonly PredictionForecast[]): ScheduledWork {
		if (forecasts.length === 0) return emptyWork();
		const evaluated = forecasts.map((forecast) => this.evaluateOne(forecast));
		const resource = evaluated.reduce(
			(current, item) => mergeResource(current, item.resource),
			evaluated[0]!.resource,
		);
		return {
			expectedDurationMs: Math.max(...evaluated.map((item) => item.expectedDurationMs)),
			resource,
			decisionBatchesUntilCall: Math.min(...evaluated.map((item) => item.decisionBatchesUntilCall)),
			criticalPathMs: Math.max(...evaluated.map((item) => item.criticalPathMs)),
			priorityMs: Math.max(...evaluated.map((item) => item.priorityMs)),
			background: evaluated.every((item) => item.background),
		};
	}

	launchDelay(forecast: PredictionForecast, safetyMarginMs = 10): number {
		if (forecast.dependenciesResolved) return 0;
		const decisionBatchesUntilCall = sequence(forecast.decisionBatchesUntilCall);
		if (decisionBatchesUntilCall <= 1) return 0;
		const duration = this.duration(forecast, 0.9);
		const availableMs = this.actorRunway(forecast, duration) ?? 0;
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
		this.actorDecisionDurations.observe(decisionDurationMs);
		if (cycleDurationMs !== undefined) this.actorCycles.observe(cycleDurationMs);
	}

	observeSpeculativeService(identity: ServiceTimingIdentity, durationMs: number): void {
		this.observeTiming(this.speculativeServiceTimes, identity, durationMs);
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
			// Sparse or wider-class fallback samples must not veto ready work. Sunk lookup costs cancel.
			if (
				actor?.exact && adoption?.exact &&
				Math.min(actor.samples, adoption.samples) >= DEFAULT_BENEFIT_GATE_POLICY.minSamples &&
				expectedNetBenefitMs !== undefined &&
				expectedNetBenefitMs < 0
			) {
				return { allowed: false, reason: "fallback_faster", waitBudgetMs: 0, ...base };
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

	private evaluateOne(forecast: PredictionForecast): ScheduledWork {
		const expectedDurationMs = this.duration(forecast);
		const baseResource = resourceProfile(forecast.execution);
		const resource = {
			class: baseResource.class,
			units: Math.max(baseResource.units, units(forecast.resourceDemand)),
		};
		const criticalPathMs = Math.max(expectedDurationMs, finite(forecast.criticalPathMs));
		const runwayMs = this.actorRunway(forecast);
		const benefitDurationMs = positive(forecast.expectedDurationMs, expectedDurationMs);
		const runwayScale = runwayMs === undefined ? 1 : Math.min(1, runwayMs / benefitDurationMs);
		return {
			expectedDurationMs,
			resource,
			decisionBatchesUntilCall: sequence(forecast.decisionBatchesUntilCall),
			criticalPathMs,
			priorityMs:
				forecast.expectedLatencyBenefitMs === undefined
					? criticalPathMs
					: finite(forecast.expectedLatencyBenefitMs) * runwayScale,
			background: forecast.background === true,
		};
	}

	private actorRunway(forecast: PredictionForecast, fallbackDurationMs?: number): number | undefined {
		const phase =
			forecast.actorPhase ??
			(fallbackDurationMs === undefined ? undefined : { kind: "cycle" as const, elapsedMs: 0 });
		if (!phase) return undefined;
		const fallbackCycleMs = fallbackDurationMs === undefined ? undefined : Math.max(50, fallbackDurationMs * 2);
		const cycleMs =
			fallbackCycleMs === undefined
				? this.actorCycles.estimate(0.25)
				: this.actorCycles.quantile(0.25, fallbackCycleMs);
		const decisionMs =
			fallbackCycleMs === undefined
				? this.actorDecisionDurations.estimate(0.25)
				: this.actorDecisionDurations.quantile(0.25, cycleMs ?? fallbackCycleMs);
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

	private duration(forecast: PredictionForecast, quantile = 0.5): number {
		const actionDuration = positive(forecast.expectedDurationMs, 1);
		const observed = this.timingEstimate(
			this.speculativeServiceTimes,
			forecast,
			quantile,
		)?.value;
		// A source's action-specific estimate remains a lower bound. Wider timing classes can
		// conservatively raise scheduling cost, but must not make an explicitly long action look short.
		return Math.max(actionDuration, observed ?? 0);
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
			if (value !== undefined) return { value, samples: window!.count, exact: Boolean(identity.actionKeyHash) && index === 0 };
		}
		return undefined;
	}
}

interface TimingEstimate {
	readonly value: number;
	readonly samples: number;
	readonly exact: boolean;
}

class SampleWindow {
	private readonly values: number[] = [];

	get count(): number {
		return this.values.length;
	}

	observe(value: number): void {
		const normalized = finite(value);
		if (normalized <= 0) return;
		this.values.push(normalized);
		if (this.values.length > 64) this.values.shift();
	}

	quantile(value: number, fallback: number): number {
		return this.estimate(value) ?? positive(fallback, 1);
	}

	estimate(value: number, selection: QuantileSelection = "lower"): number | undefined {
		if (!this.values.length) return undefined;
		const sorted = [...this.values].sort((left, right) => left - right);
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

function normalizeCandidateJoinPolicy(policy: Partial<CandidateJoinPolicy> | undefined): CandidateJoinPolicy {
	return Object.freeze({
		minNetBenefitMs: finite(policy?.minNetBenefitMs ?? DEFAULT_CANDIDATE_JOIN_POLICY.minNetBenefitMs),
		...(policy?.uncalibratedWaitMs === undefined ? {} : { uncalibratedWaitMs: finite(policy.uncalibratedWaitMs) }),
		warmupWaitMs: finite(policy?.warmupWaitMs ?? DEFAULT_CANDIDATE_JOIN_POLICY.warmupWaitMs),
		durationSlack: Math.max(1, finite(policy?.durationSlack ?? DEFAULT_CANDIDATE_JOIN_POLICY.durationSlack)),
	});
}

function emptyWork(): ScheduledWork {
	return {
		expectedDurationMs: 0,
		resource: { class: "filesystem", units: 1 },
		decisionBatchesUntilCall: 0,
		criticalPathMs: 0,
		priorityMs: 0,
		background: false,
	};
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

function normalizeBudget(capacity: number | SpeculativeResourceBudget): SpeculativeResourceBudget {
	return typeof capacity === "number" ? speculativeResourceBudget(capacity) : capacity;
}

function mergeResource(
	left: SpeculativeResourceProfile,
	right: SpeculativeResourceProfile,
): SpeculativeResourceProfile {
	return {
		class: left.class === right.class ? left.class : "global",
		units: Math.max(left.units, right.units),
	};
}

function fits<Job>(
	entries: readonly SchedulerEntry<Job>[],
	incoming: SpeculativeResourceProfile,
	budget: SpeculativeResourceBudget,
): boolean {
	if (totalUnits(entries) + incoming.units > budget.total) return false;
	for (const resourceClass of resourceClasses()) {
		const incomingUnits = incoming.class === resourceClass || incoming.class === "global" ? incoming.units : 0;
		if (classUnits(entries, resourceClass) + incomingUnits > budget.classes[resourceClass]) return false;
	}
	return true;
}

function totalUnits<Job>(entries: readonly SchedulerEntry<Job>[]): number {
	return entries.reduce((total, entry) => total + entry.work.resource.units, 0);
}

function classUnits<Job>(entries: readonly SchedulerEntry<Job>[], resourceClass: SpeculativeResourceClass): number {
	return entries.reduce(
		(total, entry) =>
			total +
			(entry.work.resource.class === resourceClass || entry.work.resource.class === "global"
				? entry.work.resource.units
				: 0),
		0,
	);
}

function resourceClasses(): readonly SpeculativeResourceClass[] {
	return ["filesystem", "workspace", "process", "global"];
}

function units(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(1, Math.floor(value)) : 1;
}

function finite(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

function positive(value: number | undefined, fallback: number): number {
	const normalized = finite(value);
	return normalized > 0 ? normalized : Math.max(1, finite(fallback));
}

function sequence(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}
