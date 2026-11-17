import { speculativeConfig } from "./config-schema.ts";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import os from "node:os";
import { promisify } from "node:util";
import type { SchedulingSettings } from "./config-schema.ts";
import { normalizeSchedulingSettings, SCHEDULING_DEFAULTS } from "./config-schema.ts";
import type { WorldCompatibilityEvidence } from "./execution-world.ts";
import {
	BoundedRecencyMap,
	nonNegativeFinite as finite,
	nonNegativeCount as sequence,
	positiveCount as units,
} from "./primitives.ts";
export const RESOURCE_DIMENSIONS = ["cpu", "memory", "io", "gpu", "gpuMemory", "network"] as const;
export type HardwareResources = Partial<Record<(typeof RESOURCE_DIMENSIONS)[number], number>>;
/** Scheduling hints only. They grant no execution or reuse authority. */
export interface ExecutionResourceSnapshot {
	/** CPUs this process can use, including its affinity/cpuset restriction. */
	readonly cpuCount: number;
	/** Idle CPU equivalents over the preceding sample; missing until a comparable sample exists. */
	readonly idleCpuCount?: number;
	readonly capacity?: HardwareResources;
	readonly available?: HardwareResources;
}
export interface ExecutionResourceMonitor {
	readonly initial: ExecutionResourceSnapshot;
	/** Called off the Actor path. A slow or failed sample must never hold up a tool call. */
	readonly sample: () => Promise<ExecutionResourceSnapshot>;
}
type CpuTimes = Pick<os.CpuInfo, "times">;
/** Linux lists CPU IDs, not a count: affinity to CPU 7 must not sample CPU 0. */
export function cpuAffinity(status: string): readonly number[] | undefined {
	const list = /^Cpus_allowed_list:\s*([\d,-]+)\s*$/m.exec(status)?.[1];
	if (!list) return;
	const cpus = new Set<number>();
	for (const part of list.split(",")) {
		if (!/^\d+(?:-\d+)?$/.test(part)) return;
		const [first, last = first] = part.split("-").map(Number);
		if (
			first === undefined ||
			last === undefined ||
			!Number.isSafeInteger(first) ||
			!Number.isSafeInteger(last) ||
			first < 0 ||
			last < first ||
			last > speculativeConfig().scheduling.maxCpuID ||
			cpus.size + last - first + 1 > speculativeConfig().scheduling.maxAffinityCPUs
		)
			return;
		for (let cpu = first; cpu <= last; cpu++) cpus.add(cpu);
	}
	return [...cpus].sort((a, b) => a - b);
}
export function idleCpuCount(
	previous: readonly CpuTimes[],
	current: readonly CpuTimes[],
	cpuCount: number,
	allowed?: readonly number[],
): number | undefined {
	const fractions: number[] = [];
	for (const index of allowed ?? current.map((_, index) => index)) {
		const before = previous[index]?.times;
		const after = current[index]?.times;
		if (!before || !after) return;
		const deltas = (Object.keys(after) as (keyof typeof after)[]).map((key) => after[key] - before[key]);
		const elapsed = deltas.reduce((sum, value) => sum + value, 0);
		const idle = after.idle - before.idle;
		if (!Number.isFinite(elapsed) || elapsed <= 0 || deltas.some((value) => value < 0)) return;
		fractions.push(Math.max(0, Math.min(1, idle / elapsed)));
	}
	if (fractions.length < cpuCount) return;
	// Where affinity IDs are unavailable, use the busiest possible subset instead of assuming idle host CPUs are usable.
	return fractions
		.sort((a, b) => a - b)
		.slice(0, cpuCount)
		.reduce((sum, value) => sum + value, 0);
}
export function createSystemResourceMonitor(
	settings: () =>
		Partial<SchedulingSettings> | undefined | Promise<Partial<SchedulingSettings> | undefined> = () =>
		undefined,
): ExecutionResourceMonitor {
	let previous: readonly CpuTimes[] | undefined;
	let previousAffinity: string | undefined;
	let lastGpuSample = -Infinity;
	let gpu: Pick<ExecutionResourceSnapshot, "capacity" | "available"> = {};
	let gpuUnavailable = false;
	const memory = () => ({
		capacity: {
			memory: Math.min(os.totalmem(), process.constrainedMemory() || Infinity),
			io: 1,
		},
		available: { memory: Math.min(os.freemem(), process.availableMemory()) },
	});
	return {
		initial: { cpuCount: Math.max(1, os.availableParallelism()), ...memory() },
		sample: async () => {
			const policy = normalizeSchedulingSettings(await settings());
			const allowed =
				process.platform === "linux"
					? cpuAffinity(await readFile("/proc/self/status", "utf8").catch(() => ""))
					: undefined;
			const cpuCount = Math.max(1, Math.min(os.availableParallelism(), allowed?.length ?? Infinity));
			const current = os.cpus();
			const affinity = JSON.stringify([cpuCount, allowed]);
			const idle =
				previous && affinity === previousAffinity
					? idleCpuCount(previous, current, cpuCount, allowed)
					: undefined;
			previous = current;
			previousAffinity = affinity;
			const io =
				process.platform === "linux"
					? ioAvailability(await readFile("/proc/pressure/io", "utf8").catch(() => ""))
					: undefined;
			// GPU telemetry is optional; never put a driver subprocess on the Actor path.
			if (!gpuUnavailable && performance.now() - lastGpuSample >= policy.gpuPollIntervalMs) {
				lastGpuSample = performance.now();
				try {
					gpu =
						gpuAvailability(
							(
								await promisify(execFile)(
									"nvidia-smi",
									["--query-gpu=memory.total,memory.free,utilization.gpu", "--format=csv,noheader,nounits"],
									{
										timeout: policy.gpuProbeTimeoutMs,
										windowsHide: true,
										maxBuffer: speculativeConfig().scheduling.gpuProbeBytes,
									},
								)
							).stdout,
						) ?? {};
				} catch (error) {
					gpu = {};
					gpuUnavailable = (error as NodeJS.ErrnoException).code === "ENOENT";
				}
			}
			const ram = memory();
			return {
				cpuCount,
				...(idle === undefined ? {} : { idleCpuCount: idle }),
				capacity: { ...ram.capacity, ...gpu.capacity },
				available: {
					...ram.available,
					...gpu.available,
					...(io === undefined ? {} : { io }),
				},
			};
		},
	};
}
export function ioAvailability(pressure: string): number | undefined {
	const value = /^some\s+avg10=([\d.]+)/m.exec(pressure)?.[1];
	return value === undefined || !Number.isFinite(Number(value))
		? undefined
		: Math.max(0, 1 - Number(value) / 100);
}
export function gpuAvailability(
	csv: string,
): Pick<ExecutionResourceSnapshot, "capacity" | "available"> | undefined {
	const rows = csv
		.trim()
		.split(/\r?\n/)
		.map((line) => line.split(",").map((value) => Number(value.trim())));
	if (
		!rows.length ||
		rows.some((row) => row.length !== 3 || row.some((value) => !Number.isFinite(value) || value < 0))
	)
		return;
	return {
		capacity: {
			gpu: rows.length,
			gpuMemory: rows.reduce((sum, row) => sum + row[0]!, 0) * 1024 * 1024,
		},
		available: {
			gpu: rows.reduce((sum, row) => sum + Math.max(0, 1 - row[2]! / 100), 0),
			gpuMemory: rows.reduce((sum, row) => sum + Math.min(row[0]!, row[1]!), 0) * 1024 * 1024,
		},
	};
}
export interface ExecutionIdentity {
	readonly tool: string;
	readonly semanticsEpoch?: string;
	readonly executionFingerprint?: string;
	readonly actionKeyHash?: string;
}
/** Current work and dependency facts. Duration is an explicit bound for this execution, never a learned service time. */
export interface PredictionForecast
	extends ExecutionIdentity, Partial<Omit<ScheduledWork, "resources" | "resourceUnits">> {
	readonly resourceDemand?: number | HardwareResources;
	readonly hitProbability?: number;
	readonly adoptionProbability?: number;
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
type Role = "execution" | "preparation" | "actor";
export interface SchedulingScope {
	readonly owner: object;
	readonly limit: number;
	readonly scheduling?: Partial<SchedulingSettings>;
}
interface SchedulerEntry<Job> {
	readonly job: Job;
	readonly scope: SchedulingScope;
	readonly role: Role;
	readonly sequence: number;
	work: ScheduledWork;
}
export type SchedulerAdmission =
	| {
			readonly admitted: true;
			readonly work: ScheduledWork;
	  }
	| {
			readonly admitted: false;
			readonly work: ScheduledWork;
			readonly reason: "budget_exhausted" | "failure_circuit" | "outside_launch_window";
	  };
export type WorldCompatibilityDecision =
	| {
			readonly compatible: true;
	  }
	| {
			readonly compatible: false;
			readonly code: "backend_incompatible" | "backend_indeterminate" | "execution_fingerprint_changed";
			readonly detail?: string;
	  };
interface SchedulerOptions {
	readonly resources?: ExecutionResourceMonitor;
	readonly pollIntervalMs?: () => number;
	readonly active?: () => boolean;
	readonly changed?: () => void;
}
const add = (target: HardwareResources, source: HardwareResources, scale = 1) => {
	for (const key of RESOURCE_DIMENSIONS)
		if (source[key] !== undefined) target[key] = (target[key] ?? 0) + finite(source[key]) * scale;
	return target;
};
/** One physical ledger and ordering for all sessions, preparations, producers and real Actor work. */
export class SpeculationScheduler<Job extends object> {
	private readonly entries = new Map<Job, SchedulerEntry<Job>>();
	private readonly failures = new BoundedRecencyMap<
		string,
		{
			count: number;
			decisions: WeakMap<
				object,
				{
					sequence: number;
					probes: number;
					allowed: boolean;
				}
			>;
		}
	>(speculativeConfig().scheduling.failureEntries);
	private sequence = 0;
	private decisionSequence = 0;
	private capacity: HardwareResources = {};
	private admission: HardwareResources = {};
	private timer?: ReturnType<typeof setTimeout>;
	private sampling = false;
	private readonly options: SchedulerOptions;
	constructor(options: SchedulerOptions = {}) {
		this.options = options;
		if (options.resources) this.updateResources(options.resources.initial);
	}
	evaluate(forecasts: readonly PredictionForecast[]): ScheduledWork {
		const resources: HardwareResources = {};
		const durations = forecasts.flatMap((f) =>
			f.expectedDurationMs === undefined ? [] : [finite(f.expectedDurationMs)],
		);
		for (const forecast of forecasts) {
			const demand =
				typeof forecast.resourceDemand === "number"
					? { cpu: units(forecast.resourceDemand) }
					: (forecast.resourceDemand ?? { cpu: 1 });
			for (const key of RESOURCE_DIMENSIONS)
				if (demand[key] !== undefined) resources[key] = Math.max(resources[key] ?? 0, finite(demand[key]));
		}
		return {
			...(durations.length ? { expectedDurationMs: Math.max(...durations) } : {}),
			elapsedMs: Math.max(0, ...forecasts.map((f) => finite(f.elapsedMs))),
			resources,
			resourceUnits: Math.max(1, resources.cpu ?? 0),
			decisionBatchesUntilCall: Math.min(...forecasts.map((f) => sequence(f.decisionBatchesUntilCall))),
			criticalPathSteps: Math.max(1, ...forecasts.map((f) => units(f.criticalPathSteps))),
			confidence: Math.max(
				0,
				...forecasts.map((f) =>
					Math.min(1, finite(f.confidence ?? (f.hitProbability ?? 1) * (f.adoptionProbability ?? 1))),
				),
			),
			background: forecasts.length > 0 && forecasts.every((f) => f.background),
			actorDemand: forecasts.some((f) => f.actorDemand),
			actorHint: forecasts.some((f) => f.actorHint),
			dependenciesResolved: forecasts.some((f) => f.dependenciesResolved),
		};
	}
	/** Advance with the live Actor/dependency frontier, keeping distant mutable inputs uncaptured. */
	ready(work: ScheduledWork): boolean {
		return (
			work.actorDemand || work.dependenciesResolved || work.decisionBatchesUntilCall <= work.criticalPathSteps
		);
	}
	compare(left: ScheduledWork, right: ScheduledWork): number {
		const remaining = (work: ScheduledWork) =>
			work.expectedDurationMs === undefined
				? undefined
				: Math.max(0, work.expectedDurationMs - work.elapsedMs);
		const a = remaining(left);
		const b = remaining(right);
		return (
			Number(right.actorDemand) - Number(left.actorDemand) ||
			Math.max(0, left.decisionBatchesUntilCall - left.criticalPathSteps) -
				Math.max(0, right.decisionBatchesUntilCall - right.criticalPathSteps) ||
			Number(right.actorHint) - Number(left.actorHint) ||
			Number(left.background) - Number(right.background) ||
			left.decisionBatchesUntilCall - right.decisionBatchesUntilCall ||
			right.confidence - left.confidence ||
			this.pressure(left) - this.pressure(right) ||
			(a === undefined || b === undefined ? 0 : a - b)
		);
	}
	admit(
		job: Job,
		forecasts: readonly PredictionForecast[],
		scope: SchedulingScope,
		role: Role = "execution",
		work = this.evaluate(forecasts),
		identity?: ExecutionIdentity,
	): SchedulerAdmission {
		if (role !== "actor" && !work.actorDemand) {
			const policy = normalizeSchedulingSettings(scope.scheduling);
			if (!this.ready(work)) return { admitted: false, work, reason: "outside_launch_window" };
			const failed = identity && this.failures.get(executionKey(identity));
			if (failed && failed.count >= policy.failureThreshold) {
				let decision = failed.decisions.get(scope.owner);
				if (!decision || decision.sequence !== this.decisionSequence) {
					const probes = (decision?.probes ?? 0) + 1;
					decision = {
						sequence: this.decisionSequence,
						probes,
						allowed: probes % policy.failureRetryDecisions === 0,
					};
					failed.decisions.set(scope.owner, decision);
				}
				if (!decision.allowed) return { admitted: false, work, reason: "failure_circuit" };
			}
			if (this.shortages(work, scope).length) return { admitted: false, work, reason: "budget_exhausted" };
		}
		this.entries.set(job, {
			job,
			work,
			scope,
			role,
			sequence: this.sequence++,
		});
		this.watch();
		return { admitted: true, work };
	}
	refresh(job: Job, forecasts: readonly PredictionForecast[]): ScheduledWork | undefined {
		const entry = this.entries.get(job);
		// Updating demand/priority cannot return the hardware still owned by this physical execution.
		if (entry)
			entry.work = {
				...this.evaluate(forecasts),
				resources: entry.work.resources,
				resourceUnits: entry.work.resourceUnits,
			};
		return entry?.work;
	}
	complete(job: Job): boolean {
		return this.entries.delete(job);
	}
	has(job: Job): boolean {
		return this.entries.has(job);
	}
	advance(): void {
		this.decisionSequence++;
	}
	observe(identity: ExecutionIdentity, failed: boolean): void {
		const key = executionKey(identity);
		if (!failed) this.failures.delete(key);
		else
			this.failures.set(key, {
				count: (this.failures.get(key)?.count ?? 0) + 1,
				decisions: new WeakMap(),
			});
	}
	/** Cancellation never returns capacity. Draining victims are subtracted only while selecting more victims. */
	preemptFor(
		scope: SchedulingScope,
		incoming: ScheduledWork | undefined,
		canPreempt: (job: Job) => boolean,
		draining: (job: Job) => boolean,
	): readonly Job[] {
		const remaining = this.snapshot().filter((entry) => !draining(entry.job));
		const victims: Job[] = [];
		const physical = !incoming || incoming.actorDemand;
		if (incoming && !incoming.actorDemand && this.shortages(incoming, scope, [], physical).length)
			return victims;
		for (let missing; (missing = this.shortages(incoming, scope, remaining, physical)).length;) {
			const victim = remaining
				.filter(
					(entry) =>
						entry.role !== "actor" &&
						!entry.work.actorDemand &&
						canPreempt(entry.job) &&
						missing.some((key) =>
							key === "scope" ? entry.scope.owner === scope.owner : (entry.work.resources[key] ?? 0) > 0,
						) &&
						(!incoming || incoming.actorDemand || this.compare(incoming, entry.work) < 0),
				)
				.sort(
					(a, b) =>
						this.compare(b.work, a.work) || a.work.elapsedMs - b.work.elapsedMs || b.sequence - a.sequence,
				)[0];
			if (!victim) break;
			remaining.splice(remaining.indexOf(victim), 1);
			victims.push(victim.job);
		}
		return victims;
	}
	snapshot(owner?: object): readonly SchedulerEntry<Job>[] {
		return [...this.entries.values()].filter((entry) => !owner || entry.scope.owner === owner);
	}
	watch(delay = this.options.pollIntervalMs?.() ?? SCHEDULING_DEFAULTS.resourcePollIntervalMs): void {
		if (!this.options.resources || this.timer || this.sampling || !this.options.active?.()) return;
		this.timer = setTimeout(() => {
			this.timer = undefined;
			if (!this.options.active?.()) return;
			this.sampling = true;
			void this.options
				.resources!.sample()
				.then((snapshot) => {
					if (this.options.active?.()) {
						this.updateResources(snapshot);
						this.options.changed?.();
					}
				})
				.catch(() => {})
				.finally(() => {
					this.sampling = false;
					this.watch();
				});
		}, delay);
		this.timer.unref?.();
	}
	close(): void {
		clearTimeout(this.timer);
		this.timer = undefined;
	}
	assessCompatibility(evidence: WorldCompatibilityEvidence, fingerprint: string): WorldCompatibilityDecision {
		if (evidence.status !== "compatible")
			return {
				compatible: false,
				code: evidence.status === "incompatible" ? "backend_incompatible" : "backend_indeterminate",
				detail: evidence.detail ?? evidence.code,
			};
		return evidence.executionFingerprint === fingerprint
			? { compatible: true }
			: { compatible: false, code: "execution_fingerprint_changed" };
	}
	private pressure(work: ScheduledWork): number {
		return Math.max(
			0,
			...RESOURCE_DIMENSIONS.map(
				(key) => (work.resources[key] ?? 0) / Math.max(1e-9, this.capacity[key] ?? Infinity),
			),
		);
	}
	private shortages(
		incoming: ScheduledWork | undefined,
		scope: SchedulingScope,
		entries = this.snapshot(),
		physical = false,
	): ((typeof RESOURCE_DIMENSIONS)[number] | "scope")[] {
		const total = add({}, incoming?.resources ?? {});
		let scoped = incoming?.resourceUnits ?? 0;
		for (const entry of entries) {
			add(total, entry.work.resources);
			if (entry.scope.owner === scope.owner) scoped += entry.work.resourceUnits;
		}
		const capacity = physical ? this.capacity : this.admission;
		return [
			...(scoped > scope.limit ? ["scope" as const] : []),
			...RESOURCE_DIMENSIONS.filter(
				(key) => (total[key] ?? 0) > (capacity[key] ?? (key === "gpu" || key === "gpuMemory" ? 0 : Infinity)),
			),
		];
	}
	private updateResources(snapshot: ExecutionResourceSnapshot): void {
		if (!Number.isFinite(snapshot.cpuCount) || snapshot.cpuCount < 1) return;
		this.capacity = { ...snapshot.capacity, cpu: units(snapshot.cpuCount) };
		const allocated: HardwareResources = {};
		for (const entry of this.entries.values()) add(allocated, entry.work.resources);
		this.admission = { ...this.capacity };
		for (const key of RESOURCE_DIMENSIONS) {
			const idle =
				key === "cpu"
					? (snapshot.idleCpuCount ?? Number((allocated.cpu ?? 0) === 0))
					: snapshot.available?.[key];
			if (idle !== undefined && Number.isFinite(idle) && idle >= 0)
				this.admission[key] = Math.min(
					this.capacity[key] ?? Infinity,
					(allocated[key] ?? 0) + (key === "cpu" ? Math.round(idle) : idle),
				);
		}
	}
}
function executionKey(identity: ExecutionIdentity): string {
	return JSON.stringify([
		identity.tool,
		identity.semanticsEpoch,
		identity.executionFingerprint,
		identity.actionKeyHash,
	]);
}
