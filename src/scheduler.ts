import { nonNegativeCount as sequence, nonNegativeFinite as finite, positiveCount as units } from "./number-utils.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import type { WorldCompatibilityEvidence } from "./execution-world.ts";
import { RESOURCE_DIMENSIONS as dimensions, type ExecutionResourceMonitor, type ExecutionResourceSnapshot, type HardwareResources } from "./system-resources.ts";

export interface ExecutionIdentity {
	readonly tool: string;
	readonly semanticsEpoch?: string;
	readonly executionFingerprint?: string;
	readonly actionKeyHash?: string;
}

/** Current work and dependency facts. Duration is an explicit bound for this execution, never a learned service time. */
export interface PredictionForecast extends ExecutionIdentity {
	readonly expectedDurationMs?: number;
	readonly elapsedMs?: number;
	readonly resourceDemand?: number | HardwareResources;
	readonly decisionBatchesUntilCall?: number;
	readonly criticalPathSteps?: number;
	readonly hitProbability?: number;
	readonly confidence?: number;
	readonly adoptionProbability?: number;
	readonly background?: boolean;
	readonly actorDemand?: boolean;
	readonly actorHint?: boolean;
	readonly dependenciesResolved?: boolean;
}

export interface ScheduledWork {
	readonly expectedDurationMs?: number;
	readonly elapsedMs: number;
	readonly resources: HardwareResources;
	readonly resourceUnits: number;
	readonly decisionBatchesUntilCall: number;
	readonly criticalPathSteps: number;
	readonly confidence: number;
	readonly background: boolean;
	readonly actorDemand: boolean;
	readonly actorHint: boolean;
	readonly dependenciesResolved: boolean;
}

export const CANDIDATE_JOIN_TIMEOUT_MS = 1_000;
export function candidateJoinBudget(state: "queued" | "running" | "succeeded"): number {
	return state === "succeeded" ? 0 : CANDIDATE_JOIN_TIMEOUT_MS;
}
export type CandidateWaitResult<T> = { readonly status: "completed"; readonly value: T } | { readonly status: "aborted" } | { readonly status: "deadline" };

/** Protocol deadline and cancellation, independent of scheduling value or past executions. */
export async function waitForCandidate<T>(promise: Promise<T>, signal?: AbortSignal, waitBudgetMs?: number): Promise<CandidateWaitResult<T>> {
	if (signal?.aborted) { void promise.catch(() => undefined); return { status: "aborted" }; }
	const bounded = waitBudgetMs !== undefined && Number.isFinite(waitBudgetMs);
	if (!signal && !bounded) return { status: "completed", value: await promise };
	return new Promise((resolve, reject) => {
		let settled = false, timer: ReturnType<typeof setTimeout> | undefined;
		const finish = (complete: () => void) => {
			if (settled) return;
			settled = true; clearTimeout(timer); signal?.removeEventListener("abort", aborted); complete();
		};
		const aborted = () => finish(() => resolve({ status: "aborted" }));
		signal?.addEventListener("abort", aborted, { once: true });
		if (bounded) timer = setTimeout(() => finish(() => resolve({ status: "deadline" })), Math.max(0, waitBudgetMs));
		void promise.then(value => finish(() => resolve({ status: "completed", value })), error => finish(() => reject(error)));
	});
}

type Role = "execution" | "preparation" | "actor";
export interface SchedulingScope { readonly owner: object; readonly limit: number; }
interface SchedulerEntry<Job> {
	readonly job: Job; readonly scope: SchedulingScope; readonly role: Role; readonly sequence: number;
	work: ScheduledWork;
}
export type SchedulerAdmission = { readonly admitted: true; readonly work: ScheduledWork } |
	{ readonly admitted: false; readonly work: ScheduledWork; readonly reason: "budget_exhausted" | "failure_circuit" | "outside_launch_window" };
export type WorldCompatibilityDecision = { readonly compatible: true } | {
	readonly compatible: false;
	readonly code: "backend_incompatible" | "backend_indeterminate" | "execution_fingerprint_changed";
	readonly detail?: string;
};
interface SchedulerOptions {
	readonly resources?: ExecutionResourceMonitor;
	readonly active?: () => boolean;
	readonly changed?: () => void;
}

const add = (target: HardwareResources, source: HardwareResources, scale = 1) => {
	for (const key of dimensions) if (source[key] !== undefined) target[key] = (target[key] ?? 0) + finite(source[key]) * scale;
	return target;
};

/** One physical ledger and ordering for all sessions, preparations, producers and real Actor work. */
export class SpeculationScheduler<Job extends object> {
	private readonly entries = new Map<Job, SchedulerEntry<Job>>();
	private readonly failures = new BoundedRecencyMap<string, { count: number; probes: number; decisions: WeakMap<object, { sequence: number; allowed: boolean }> }>(1024);
	private sequence = 0;
	private decisionSequence = 0;
	private hardware?: ExecutionResourceSnapshot;
	private capacity: HardwareResources = {};
	private admission: HardwareResources = {};
	private timer?: ReturnType<typeof setTimeout>;
	private sampling = false;
	private readonly options: SchedulerOptions;
	constructor(options: SchedulerOptions = {}) { this.options = options; if (options.resources) this.updateResources(options.resources.initial); }

	evaluate(forecasts: readonly PredictionForecast[]): ScheduledWork {
		const resources: HardwareResources = {}, durations = forecasts.flatMap(f => f.expectedDurationMs === undefined ? [] : [finite(f.expectedDurationMs)]);
		for (const forecast of forecasts) {
			const demand = typeof forecast.resourceDemand === "number" ? { cpu: units(forecast.resourceDemand) } : forecast.resourceDemand ?? { cpu: 1 };
			for (const key of dimensions) if (demand[key] !== undefined) resources[key] = Math.max(resources[key] ?? 0, finite(demand[key]));
		}
		return {
			...(durations.length ? { expectedDurationMs: Math.max(...durations) } : {}),
			elapsedMs: Math.max(0, ...forecasts.map(f => finite(f.elapsedMs))), resources, resourceUnits: Math.max(1, resources.cpu ?? 0),
			decisionBatchesUntilCall: Math.min(...forecasts.map(f => sequence(f.decisionBatchesUntilCall))),
			criticalPathSteps: Math.max(1, ...forecasts.map(f => units(f.criticalPathSteps))),
			confidence: Math.max(0, ...forecasts.map(f => Math.min(1, finite(f.confidence ?? ((f.hitProbability ?? 1) * (f.adoptionProbability ?? 1)))))),
			background: forecasts.length > 0 && forecasts.every(f => f.background),
			actorDemand: forecasts.some(f => f.actorDemand), actorHint: forecasts.some(f => f.actorHint),
			dependenciesResolved: forecasts.some(f => f.dependenciesResolved),
		};
	}

	/** Advance with the live Actor/dependency frontier, keeping distant mutable inputs uncaptured. */
	ready(work: ScheduledWork): boolean { return work.actorDemand || work.dependenciesResolved || work.decisionBatchesUntilCall <= work.criticalPathSteps; }

	compare(left: ScheduledWork, right: ScheduledWork): number {
		const remaining = (work: ScheduledWork) => work.expectedDurationMs === undefined ? undefined : Math.max(0, work.expectedDurationMs - work.elapsedMs);
		const a = remaining(left), b = remaining(right);
		return Number(right.actorDemand) - Number(left.actorDemand) ||
			Math.max(0, left.decisionBatchesUntilCall - left.criticalPathSteps) - Math.max(0, right.decisionBatchesUntilCall - right.criticalPathSteps) ||
			Number(right.actorHint) - Number(left.actorHint) || Number(left.background) - Number(right.background) ||
			left.decisionBatchesUntilCall - right.decisionBatchesUntilCall || right.confidence - left.confidence ||
			this.pressure(left) - this.pressure(right) || (a === undefined || b === undefined ? 0 : a - b);
	}

	admit(job: Job, forecasts: readonly PredictionForecast[], scope: SchedulingScope, role: Role = "execution", work = this.evaluate(forecasts), identity?: ExecutionIdentity): SchedulerAdmission {
		if (role !== "actor" && !work.actorDemand) {
			if (!this.ready(work)) return { admitted: false, work, reason: "outside_launch_window" };
			const failed = identity && this.failures.get(executionKey(identity));
			if (failed && failed.count >= 2) {
				let decision = failed.decisions.get(job);
				if (!decision || decision.sequence !== this.decisionSequence) {
					decision = { sequence: this.decisionSequence, allowed: ++failed.probes % 4 === 0 }; failed.decisions.set(job, decision);
				}
				if (!decision.allowed) return { admitted: false, work, reason: "failure_circuit" };
			}
			if (this.shortages(work, scope).length) return { admitted: false, work, reason: "budget_exhausted" };
		}
		this.entries.set(job, { job, work, scope, role, sequence: this.sequence++ });
		this.watch();
		return { admitted: true, work };
	}

	refresh(job: Job, forecasts: readonly PredictionForecast[]): ScheduledWork | undefined {
		const entry = this.entries.get(job);
		// Updating demand/priority cannot return the hardware still owned by this physical execution.
		if (entry) entry.work = { ...this.evaluate(forecasts), resources: entry.work.resources, resourceUnits: entry.work.resourceUnits };
		return entry?.work;
	}
	complete(job: Job): boolean { return this.entries.delete(job); }
	has(job: Job): boolean { return this.entries.has(job); }
	advance(): void { this.decisionSequence++; }
	observe(identity: ExecutionIdentity, failed: boolean): void {
		const key = executionKey(identity);
		if (!failed) this.failures.delete(key);
		else this.failures.set(key, { count: (this.failures.get(key)?.count ?? 0) + 1, probes: 0, decisions: new WeakMap() });
	}

	/** Cancellation never returns capacity. Draining victims are subtracted only while selecting more victims. */
	preemptFor(scope: SchedulingScope, incoming: ScheduledWork | undefined, canPreempt: (job: Job) => boolean, draining: (job: Job) => boolean): readonly Job[] {
		const remaining = this.snapshot().filter(entry => !draining(entry.job)), victims: Job[] = [];
		const physical = !incoming || incoming.actorDemand;
		if (incoming && !incoming.actorDemand && this.shortages(incoming, scope, [], physical).length) return victims;
		for (let missing; (missing = this.shortages(incoming, scope, remaining, physical)).length;) {
			const victim = remaining.filter(entry => entry.role !== "actor" && !entry.work.actorDemand && canPreempt(entry.job) &&
				missing.some(key => key === "scope" ? entry.scope.owner === scope.owner : (entry.work.resources[key] ?? 0) > 0) &&
				(!incoming || incoming.actorDemand || this.compare(incoming, entry.work) < 0))
				.sort((a, b) => this.compare(b.work, a.work) || a.work.elapsedMs - b.work.elapsedMs || b.sequence - a.sequence)[0];
			if (!victim) break;
			remaining.splice(remaining.indexOf(victim), 1); victims.push(victim.job);
		}
		return victims;
	}

	snapshot(owner?: object): readonly SchedulerEntry<Job>[] { return [...this.entries.values()].filter(entry => !owner || entry.scope.owner === owner); }
	inspect() {
		const usage = { actorUnits: 0, preparationUnits: 0, executionUnits: 0 }, reserved: HardwareResources = {};
		for (const entry of this.entries.values()) { usage[`${entry.role}Units`] += entry.work.resourceUnits; add(reserved, entry.work.resources); }
		return this.hardware && { ...this.hardware, admissionCapacity: this.admission.cpu ?? 0, ...usage, reserved, capacity: { ...this.capacity }, available: { ...this.admission } };
	}

	watch(delay = 250): void {
		if (!this.options.resources || this.timer || this.sampling || !this.options.active?.()) return;
		this.timer = setTimeout(() => {
			this.timer = undefined;
			if (!this.options.active?.()) return;
			this.sampling = true;
			void this.options.resources!.sample().then(snapshot => {
				if (this.options.active?.()) { this.updateResources(snapshot); this.options.changed?.(); }
			}).catch(() => {}).finally(() => { this.sampling = false; this.watch(); });
		}, delay);
		this.timer.unref?.();
	}
	close(): void { clearTimeout(this.timer); this.timer = undefined; }

	assessCompatibility(evidence: WorldCompatibilityEvidence, fingerprint: string): WorldCompatibilityDecision {
		if (evidence.status !== "compatible") return { compatible: false, code: evidence.status === "incompatible" ? "backend_incompatible" : "backend_indeterminate", detail: evidence.detail ?? evidence.code };
		return evidence.executionFingerprint === fingerprint ? { compatible: true } : { compatible: false, code: "execution_fingerprint_changed" };
	}

	private pressure(work: ScheduledWork): number {
		return Math.max(0, ...dimensions.map(key => (work.resources[key] ?? 0) / Math.max(1e-9, this.capacity[key] ?? Infinity)));
	}
	private shortages(incoming: ScheduledWork | undefined, scope: SchedulingScope, entries = this.snapshot(), physical = false): (typeof dimensions[number] | "scope")[] {
		const total = add({}, incoming?.resources ?? {}); let scoped = incoming?.resourceUnits ?? 0;
		for (const entry of entries) { add(total, entry.work.resources); if (entry.scope.owner === scope.owner) scoped += entry.work.resourceUnits; }
		const capacity = physical ? this.capacity : this.admission;
		return [...(scoped > scope.limit ? ["scope" as const] : []),
			...dimensions.filter(key => (total[key] ?? 0) > (capacity[key] ?? (key === "gpu" || key === "gpuMemory" ? 0 : Infinity)))];
	}
	private updateResources(snapshot: ExecutionResourceSnapshot): void {
		if (!Number.isFinite(snapshot.cpuCount) || snapshot.cpuCount < 1) return;
		this.hardware = snapshot;
		this.capacity = { ...snapshot.capacity, cpu: units(snapshot.cpuCount) };
		const allocated: HardwareResources = {};
		for (const entry of this.entries.values()) add(allocated, entry.work.resources);
		this.admission = { ...this.capacity };
		for (const key of dimensions) {
			const idle = key === "cpu" ? snapshot.idleCpuCount ?? Number((allocated.cpu ?? 0) === 0) : snapshot.available?.[key];
			if (idle !== undefined && Number.isFinite(idle) && idle >= 0)
				this.admission[key] = Math.min(this.capacity[key] ?? Infinity, (allocated[key] ?? 0) + (key === "cpu" ? Math.round(idle) : idle));
		}
	}
}

function executionKey(identity: ExecutionIdentity): string { return JSON.stringify([identity.tool, identity.semanticsEpoch, identity.executionFingerprint, identity.actionKeyHash]); }
