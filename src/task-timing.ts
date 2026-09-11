/** One immutable computation interval, shared by every adoption of that computation. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;

	constructor(startedAt: number, completedAt: number) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		Object.freeze(this);
	}

	static from(interval: TimelineInterval): TimelineInterval {
		const { startedAt, completedAt } = interval;
		return interval instanceof TimelineInterval ? interval : new TimelineInterval(startedAt, completedAt);
	}
}

export interface SpeculativeTaskTiming {
	readonly startedAt: number;
	readonly completedAt: number;
	readonly endToEndMs: number;
	readonly nonToolMs: number;
	readonly actorPhaseMs: number;
	readonly orchestrationMs: number;
	readonly toolExecutionMs: number;
	readonly serializedMs: number;
	readonly hiddenLatencyMs: number;
	/** Distinct accepted producer/query computations in this task, not Actor call count. */
	readonly authoritativeToolCount: number;
}

/** Retains scalar endpoints; counting never owns Actor identities, results or retired computations. */
export class TaskTimeline {
	private readonly actorPhases: number[] = [];
	private readonly authoritativeTools: number[] = [];
	private readonly computations = new WeakSet<TimelineInterval>();
	readonly startedAt: number;

	constructor(startedAt: number) { this.startedAt = metric(startedAt); }

	recordActor(startedAt: number, completedAt: number): void {
		this.actorPhases.push(startedAt, completedAt);
	}

	recordTool(interval: TimelineInterval): void {
		if (this.computations.has(interval)) return;
		this.computations.add(interval);
		this.authoritativeTools.push(interval.startedAt, interval.completedAt);
	}

	measure(endedAt: number): SpeculativeTaskTiming {
		const startedAt = this.startedAt, completedAt = Math.max(startedAt, metric(endedAt));
		const actorPhases = clipped(this.actorPhases, startedAt, completedAt, false);
		const authoritativeTools = clipped(this.authoritativeTools, startedAt, completedAt, true);
		const endToEndMs = completedAt - startedAt;
		const actorPhaseMs = unionDuration(actorPhases);
		const toolExecutionMs = authoritativeTools.reduce((total, interval) => total + interval.completedAt - interval.startedAt, 0);
		const coveredMs = unionDuration([...actorPhases, ...authoritativeTools]);
		const orchestrationMs = Math.max(0, endToEndMs - coveredMs);
		const measuredNonToolMs = actorPhaseMs + orchestrationMs;
		const hiddenLatencyMs = nonNegativeDifference(measuredNonToolMs + toolExecutionMs, endToEndMs);
		const serializedMs = endToEndMs + hiddenLatencyMs;
		const nonToolMs = Math.max(0, serializedMs - toolExecutionMs);
		return Object.freeze({
			startedAt,
			completedAt,
			endToEndMs,
			nonToolMs,
			actorPhaseMs,
			orchestrationMs,
			toolExecutionMs,
			serializedMs,
			hiddenLatencyMs,
			authoritativeToolCount: authoritativeTools.length,
		});
	}
}

/** Reconstructs a no-overlap baseline from distinct computations in one authoritative timeline. */
export function measureSpeculativeTask(input: {
	readonly startedAt: number;
	readonly completedAt: number;
	readonly actorPhases: readonly TimelineInterval[];
	readonly authoritativeTools: readonly TimelineInterval[];
}): SpeculativeTaskTiming {
	const timeline = new TaskTimeline(input.startedAt);
	for (const interval of input.actorPhases) timeline.recordActor(interval.startedAt, interval.completedAt);
	for (const interval of input.authoritativeTools) timeline.recordTool(interval);
	return timeline.measure(input.completedAt);
}

function nonNegativeDifference(left: number, right: number): number {
	const difference = left - right;
	const tolerance = Number.EPSILON * Math.max(1, left, right) * 16;
	return difference > tolerance ? difference : 0;
}

function clipped(endpoints: readonly number[], startedAt: number, completedAt: number, tool: boolean): TimelineInterval[] {
	const intervals: TimelineInterval[] = [];
	for (let index = 0; index < endpoints.length; index += 2) {
		const start = metric(endpoints[index]!);
		if (tool && start < startedAt) continue;
		const end = Math.min(completedAt, Math.max(start, metric(endpoints[index + 1]!)));
		const clippedStart = Math.max(startedAt, start);
		if (end > clippedStart) intervals.push({ startedAt: clippedStart, completedAt: end });
	}
	return intervals;
}

function unionDuration(intervals: readonly TimelineInterval[]): number {
	const sorted = [...intervals].sort(
		(left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt,
	);
	let total = 0;
	let current: TimelineInterval | undefined;
	for (const interval of sorted) {
		if (!current) {
			current = interval;
			continue;
		}
		if (interval.startedAt <= current.completedAt) {
			current = { startedAt: current.startedAt, completedAt: Math.max(current.completedAt, interval.completedAt) };
			continue;
		}
		total += current.completedAt - current.startedAt;
		current = interval;
	}
	return current ? total + current.completedAt - current.startedAt : total;
}

function metric(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}
