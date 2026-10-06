import { AsyncLocalStorage } from "node:async_hooks";
import { nonNegativeFinite as metric } from "./number-utils.ts";

export interface TimelineDependency {
	readonly computation: TimelineInterval;
	/** Work produced inside this evaluation retains its enclosing native parallelism. */
	readonly owned?: boolean;
	/** Parts already included in the enclosing execution, or spent waiting for this computation. */
	readonly shared?: readonly TimelineInterval[];
}

const dependencies = new WeakMap<TimelineInterval, readonly TimelineDependency[]>();
const collecting = new AsyncLocalStorage<{ inputs?: Map<TimelineInterval, boolean> }>();
const provenance = new WeakMap<TimelineInterval, { readonly id: string; readonly priorMs: number }>();

/** One immutable computation interval, shared by every adoption of that computation. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;

	constructor(startedAt: number, completedAt: number, inputs: readonly TimelineDependency[] = []) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		if (inputs.length) dependencies.set(this, Object.freeze(inputs.map(input => Object.freeze({
			computation: TimelineInterval.from(input.computation),
			owned: input.owned,
			shared: Object.freeze((input.shared ?? []).map(TimelineInterval.from)),
		}))));
		Object.freeze(this);
	}

	static from(interval: TimelineInterval): TimelineInterval {
		const { startedAt, completedAt } = interval;
		return interval instanceof TimelineInterval ? interval : new TimelineInterval(startedAt, completedAt);
	}

	/** A failed or rejected evaluation never contributes its borrowed work to its caller. */
	static async collect<T>(execute: () => T | Promise<T>) {
		const scope = { inputs: new Map<TimelineInterval, boolean>() as Map<TimelineInterval, boolean> | undefined };
		try {
			const output = await collecting.run(scope, execute);
			return { output, dependencies: Object.freeze([...scope.inputs!].map(([computation, owned]) => ({ computation, owned, shared: [computation] }))) };
		} finally { scope.inputs = undefined; }
	}

	/** Register only work actually consumed by the current evaluation, including shared preparations. */
	static use(computation: TimelineInterval | undefined): void {
		const inputs = collecting.getStore()?.inputs;
		if (computation && inputs && !inputs.has(computation)) inputs.set(computation, false);
	}

	/** Split produced resources out of their enclosing execution before they can be borrowed elsewhere. */
	static own(computation: TimelineInterval): TimelineInterval {
		collecting.getStore()?.inputs?.set(computation, true);
		return computation;
	}

	/** A validated retained artifact keeps its measured work even when its original clock is unavailable. */
	static retained(id: string, durationMs: number, original?: TimelineInterval): TimelineInterval {
		const computation = original ?? new TimelineInterval(0, 0);
		provenance.set(computation, { id, priorMs: original ? 0 : metric(durationMs) });
		return computation;
	}
}

export interface SpeculativeTaskTiming extends ReturnType<TaskTimeline["measure"]> {}

export function toolSpeedup(timing: { readonly toolWaitMs: number; readonly hiddenLatencyMs: number }): number | null {
	const { toolWaitMs, hiddenLatencyMs } = timing;
	return toolWaitMs > 0 && Number.isFinite(toolWaitMs + hiddenLatencyMs) && hiddenLatencyMs >= 0
		? (toolWaitMs + hiddenLatencyMs) / toolWaitMs : null;
}

type RecordedComputation = {
	startedAt: number; endpoints: readonly number[]; native: boolean; priorMs: number; owner?: RecordedComputation;
};

/** Retains scalar endpoints; counting never owns Actor identities, results or retired computations. */
export class TaskTimeline {
	private readonly actorPhases: number[] = [];
	private readonly toolWaits: number[] = [];
	private readonly authoritativeTools: RecordedComputation[] = [];
	private readonly computations = new WeakMap<TimelineInterval, RecordedComputation>();
	private readonly retained = new Map<string, RecordedComputation>();
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
		const visited = new Map<TimelineInterval, boolean>();
		// Only a native Actor execution stays native; adopted and reused computations ran ahead of their callers.
		const visit = (computation: TimelineInterval, native: boolean, owner?: RecordedComputation): void => {
			const inputs = dependencies.get(computation) ?? [];
			const shared = inputs.map(({ computation, shared }) => (shared ?? []).map(part => ({
				startedAt: Math.max(computation.startedAt, part.startedAt),
				completedAt: Math.min(computation.completedAt, part.completedAt),
			})).filter(part => part.completedAt > part.startedAt));
			const source = provenance.get(computation), endpoints = exclusiveEndpoints(computation, shared.flat());
			let tool = this.computations.get(computation) ?? (source && this.retained.get(source.id));
			if (tool) {
				// Prefer same-process endpoints when both retained history and its live producer were consumed.
				if (!tool.endpoints.length && endpoints.length) Object.assign(tool, { startedAt: computation.startedAt, endpoints, priorMs: 0 });
				// Reusing work already executed natively in this task cannot erase its original parallelism.
				tool.native = (tool.native && native) || ((tool.native || native) && tool.startedAt >= this.startedAt);
			} else {
				tool = { startedAt: computation.startedAt, endpoints, native, priorMs: source?.priorMs ?? 0 };
				this.authoritativeTools.push(tool);
			}
			this.computations.set(computation, tool);
			if (source) this.retained.set(source.id, tool);
			if (owner) tool.owner = owner;
			if (visited.has(computation) && (native || !visited.get(computation))) return;
			visited.set(computation, native);
			for (const input of inputs) visit(input.computation, native && !!input.owned, input.owned ? tool : undefined);
		};
		visit(interval, !adopted);
	}

	measure(endedAt: number) {
		const startedAt = this.startedAt, completedAt = Math.max(startedAt, metric(endedAt));
		const actorPhases = clipped(this.actorPhases, startedAt, completedAt);
		const computations = new Map<RecordedComputation, { priorMs: number; all: TimelineInterval[] }>();
		for (const tool of this.authoritativeTools) {
			let owner = tool;
			while (owner.owner && (!owner.owner.native || owner.owner.startedAt >= startedAt)) owner = owner.owner;
			if (owner.native && owner.startedAt < startedAt) continue;
			const all = clipped(tool.endpoints, owner.native ? startedAt : 0, completedAt);
			if (!all.length && !tool.priorMs) continue;
			const group = computations.get(owner) ?? { priorMs: 0, all: [] };
			group.priorMs += tool.priorMs; group.all.push(...all); computations.set(owner, group);
		}
		const authoritativeTools = [...computations.values()].flatMap(tool => tool.all.map(part => ({ ...part, startedAt: Math.max(startedAt, part.startedAt) })).filter(part => part.completedAt > part.startedAt));
		const nativeGroups = [...computations].filter(([owner]) => owner.native).map(([, tool]) => tool.all);
		const nativeTools = nativeGroups.flat();
		// A resource and its producing tool share one execution; extracting it must not invent internal parallel savings.
		const toolExecutionMs = [...computations.values()].reduce((sum, tool) => sum + unionDuration(tool.all) + tool.priorMs, 0);
		// Native calls overlapping each other (a parallel batch) would overlap without speculation: count their union.
		const hiddenLatencyMs = nonNegativeDifference(unionDuration(actorPhases) + toolExecutionMs - nativeGroups.reduce((sum, group) => sum + unionDuration(group), 0) + unionDuration(nativeTools),
			unionDuration([...actorPhases, ...authoritativeTools]));
		const toolWaitMs = unionDuration(clipped(this.toolWaits, startedAt, completedAt));
		return Object.freeze({ startedAt, completedAt, toolExecutionMs, toolWaitMs, hiddenLatencyMs,
			/** Distinct accepted computations, including consumed retained work; not Actor call count. */
			authoritativeToolCount: computations.size,
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
