import { nonNegativeFinite as metric } from "./number-utils.ts";

export interface TimelineDependency {
	readonly computation: TimelineInterval;
	/** Existing native-service evidence when the retained result has no original interval. */
	readonly expectedActorMs?: number;
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
			...(input.expectedActorMs === undefined ? {} : { expectedActorMs: metric(input.expectedActorMs) }),
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

/** Retains scalar endpoints; counting never owns Actor identities, results or retired computations. */
export class TaskTimeline {
	private readonly actorPhases: number[] = [];
	private readonly authoritativeTools: { readonly startedAt: number; readonly endpoints: readonly number[]; native: boolean }[] = [];
	private readonly computations = new WeakMap<TimelineInterval, { native: boolean }>();
	private estimatedSavingsMs = 0;
	readonly startedAt: number;

	constructor(startedAt: number) { this.startedAt = metric(startedAt); }

	recordActor(startedAt: number, completedAt: number): void {
		this.actorPhases.push(startedAt, completedAt);
	}

	/** Call once per settled Actor operation; adoption includes its actual waiting and validation time. */
	recordTool(interval: TimelineInterval, adoption?: { readonly hitLatencyMs: number; readonly expectedNativeMs?: number }): void {
		const costs = new Map<TimelineInterval, number>();
		// Only a native Actor execution stays native; adopted and reused computations ran ahead of their callers.
		const visit = (computation: TimelineInterval, native: boolean): number => {
			const cached = costs.get(computation);
			if (cached !== undefined) return cached;
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
			const cost = computation.completedAt - computation.startedAt
				+ inputs.reduce((total, input) => total + nonNegativeDifference(Math.max(visit(input.computation, false), metric(input.expectedActorMs)), unionDuration(input.shared ?? [])), 0);
			costs.set(computation, cost);
			return cost;
		};
		const serialMs = visit(interval, !adoption);
		// Per-call credit includes retained work from earlier tasks. Native parents already include child waits.
		// Adoption is measured against native execution history when there is any; a slower hit is a loss.
		const referenceMs = adoption?.expectedNativeMs ?? serialMs;
		const actualMs = adoption ? metric(adoption.hitLatencyMs) : interval.completedAt - interval.startedAt;
		this.estimatedSavingsMs += nonNegativeDifference(referenceMs, actualMs) - nonNegativeDifference(actualMs, referenceMs);
	}

	/** Speculation's own time on a native Actor call's path. */
	recordOverhead(durationMs: number): void {
		this.estimatedSavingsMs -= metric(durationMs);
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
		const serializedMs = endToEndMs + hiddenLatencyMs;
		return Object.freeze({ startedAt, completedAt, endToEndMs, nonToolMs, actorPhaseMs, orchestrationMs, toolExecutionMs, serializedMs, hiddenLatencyMs,
			/** Signed avoided service time against native history, net of speculation's own cost on native calls. */
			estimatedSavingsMs: this.estimatedSavingsMs,
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
	const sorted = [...intervals].sort(
		(left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt,
	);
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
