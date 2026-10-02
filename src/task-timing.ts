import { nonNegativeFinite as metric } from "./number-utils.ts";

export interface TimelineDependency {
	readonly computation: TimelineInterval;
	/** Parts already included in the enclosing execution, or spent waiting for this computation. */
	readonly shared?: readonly TimelineInterval[];
}

const dependencies = new WeakMap<TimelineInterval, readonly TimelineDependency[]>();

/** One immutable computation interval, shared by every adoption of that computation. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;

	constructor(startedAt: number, completedAt: number, inputs: readonly TimelineDependency[] = []) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		if (inputs.length) dependencies.set(this, Object.freeze(inputs.map(input => Object.freeze({
			computation: TimelineInterval.from(input.computation),
			shared: Object.freeze((input.shared ?? []).map(TimelineInterval.from)),
		}))));
		Object.freeze(this);
	}

	static from(interval: TimelineInterval): TimelineInterval {
		const { startedAt, completedAt } = interval;
		return interval instanceof TimelineInterval ? interval : new TimelineInterval(startedAt, completedAt);
	}
}

export interface SpeculativeTaskTiming extends ReturnType<TaskTimeline["measure"]> {}

export function toolSpeedup(timing: { readonly toolWaitMs: number; readonly hiddenLatencyMs: number }): number | null {
	const { toolWaitMs, hiddenLatencyMs } = timing;
	return toolWaitMs > 0 && Number.isFinite(toolWaitMs + hiddenLatencyMs) && hiddenLatencyMs >= 0
		? (toolWaitMs + hiddenLatencyMs) / toolWaitMs : null;
}

/** Retains scalar endpoints; counting never owns Actor identities, results or retired computations. */
export class TaskTimeline {
	private readonly actorPhases: number[] = [];
	private readonly toolWaits: number[] = [];
	private readonly authoritativeTools: { readonly startedAt: number; readonly endpoints: readonly number[]; native: boolean }[] = [];
	private readonly computations = new WeakMap<TimelineInterval, { native: boolean }>();
	readonly startedAt: number;

	constructor(startedAt: number) { this.startedAt = metric(startedAt); }

	recordActor(startedAt: number, completedAt: number): void {
		this.actorPhases.push(startedAt, completedAt);
	}

	/** Full Actor wait, including preparation, adoption, fallback and settlement; unfinished calls clip at task end. */
	startToolWait(startedAt: number): (completedAt: number) => void {
		const end = this.toolWaits.push(startedAt, Number.MAX_VALUE) - 1;
		return completedAt => { this.toolWaits[end] = Math.min(this.toolWaits[end]!, metric(completedAt)); };
	}

	/** Register accepted computations once; native parallel calls retain their overlap. */
	recordTool(interval: TimelineInterval, adopted = false): void {
		const visited = new Set<TimelineInterval>();
		// Only a native Actor execution stays native; adopted and reused computations ran ahead of their callers.
		const visit = (computation: TimelineInterval, native: boolean): void => {
			if (visited.has(computation)) return;
			visited.add(computation);
			const inputs = dependencies.get(computation) ?? [];
			const shared = inputs.map(({ computation, shared }) => (shared ?? []).map(part => ({
				startedAt: Math.max(computation.startedAt, part.startedAt),
				completedAt: Math.min(computation.completedAt, part.completedAt),
			})).filter(part => part.completedAt > part.startedAt));
			const known = this.computations.get(computation);
			if (known) known.native &&= native;
			else {
				const tool = { startedAt: computation.startedAt, endpoints: exclusiveEndpoints(computation, shared.flat()), native };
				this.computations.set(computation, tool);
				this.authoritativeTools.push(tool);
			}
			for (const input of inputs) visit(input.computation, false);
		};
		visit(interval, !adopted);
	}

	measure(endedAt: number) {
		const startedAt = this.startedAt, completedAt = Math.max(startedAt, metric(endedAt));
		const actorPhases = clipped(this.actorPhases, startedAt, completedAt);
		const computations = this.authoritativeTools.filter(tool => tool.startedAt >= startedAt)
			.map(tool => ({ native: tool.native, parts: clipped(tool.endpoints, startedAt, completedAt) })).filter(tool => tool.parts.length);
		const authoritativeTools = computations.flatMap(tool => tool.parts);
		const nativeTools = computations.flatMap(tool => tool.native ? tool.parts : []);
		const endToEndMs = completedAt - startedAt, actorPhaseMs = unionDuration(actorPhases), toolExecutionMs = duration(authoritativeTools);
		const orchestrationMs = Math.max(0, endToEndMs - unionDuration([...actorPhases, ...authoritativeTools])), nonToolMs = actorPhaseMs + orchestrationMs;
		// Native calls overlapping each other (a parallel batch) would overlap without speculation: count their union.
		const hiddenLatencyMs = nonNegativeDifference(nonToolMs + toolExecutionMs - duration(nativeTools) + unionDuration(nativeTools), endToEndMs);
		const toolWaitMs = unionDuration(clipped(this.toolWaits, startedAt, completedAt));
		return Object.freeze({ startedAt, completedAt, endToEndMs, nonToolMs, actorPhaseMs, orchestrationMs, toolExecutionMs, toolWaitMs, hiddenLatencyMs,
			/** Distinct accepted computations with exclusive time in this task, not Actor call count. */
			authoritativeToolCount: computations.length,
		});
	}
}

function nonNegativeDifference(left: number, right: number): number {
	const difference = left - right;
	const tolerance = Number.EPSILON * Math.max(1, left, right) * 16;
	return difference > tolerance ? difference : 0;
}

function exclusiveEndpoints(interval: TimelineInterval, shared: readonly TimelineInterval[]): number[] {
	const endpoints: number[] = [];
	let start = interval.startedAt;
	for (const part of [...shared].sort((left, right) => left.startedAt - right.startedAt)) {
		if (part.completedAt <= start || part.startedAt >= interval.completedAt) continue;
		if (part.startedAt > start) endpoints.push(start, part.startedAt);
		start = Math.min(interval.completedAt, Math.max(start, part.completedAt));
	}
	if (start < interval.completedAt) endpoints.push(start, interval.completedAt);
	return endpoints;
}

function clipped(endpoints: readonly number[], startedAt: number, completedAt: number): TimelineInterval[] {
	const intervals: TimelineInterval[] = [];
	for (let index = 0; index < endpoints.length; index += 2) {
		const start = metric(endpoints[index]!);
		const end = Math.min(completedAt, Math.max(start, metric(endpoints[index + 1]!)));
		const clippedStart = Math.max(startedAt, start);
		if (end > clippedStart) intervals.push({ startedAt: clippedStart, completedAt: end });
	}
	return intervals;
}

function duration(intervals: readonly TimelineInterval[]): number {
	return intervals.reduce((total, interval) => total + interval.completedAt - interval.startedAt, 0);
}

function unionDuration(intervals: readonly TimelineInterval[]): number {
	const sorted = [...intervals].sort((left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt);
	let total = 0;
	let start = 0, end = 0;
	for (const interval of sorted) {
		if (interval.startedAt > end) {
			total += end - start;
			start = interval.startedAt;
		}
		end = Math.max(end, interval.completedAt);
	}
	return total + end - start;
}
