import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { nonNegativeFinite as metric } from "./number-utils.ts";
import { normalizeTimelineComputation, TIMELINE_COMPUTATION_LIMITS, type SerializedTimelineComputation, type SerializedTimelineNode } from "./timeline-computation.ts";
export { normalizeTimelineComputation, TIMELINE_COMPUTATION_LIMITS, type SerializedTimelineComputation } from "./timeline-computation.ts";

export interface TimelineDependency {
	readonly computation: TimelineInterval;
	/** Work produced here retains its enclosing tool's parallelism. */
	readonly owned?: boolean;
	/** Only a successful consumption of existing work issues this receipt. */
	readonly reused?: boolean;
	/** Validation, adoption and delivery are excluded from computation accounting. */
	readonly overhead?: boolean;
	/** The current boundary cannot separate remaining calculation from adoption overhead. */
	readonly computeUncertain?: boolean;
	/** Parts already included in the enclosing computation, or spent joining this input. */
	readonly shared?: readonly TimelineInterval[];
}

type Receipt = Pick<TimelineDependency, "owned" | "reused" | "overhead" | "computeUncertain">;
const dependencies = new WeakMap<TimelineInterval, readonly TimelineDependency[]>();
const collecting = new AsyncLocalStorage<{ inputs?: Map<TimelineInterval, Receipt> }>();
const provenance = new WeakMap<TimelineInterval, { readonly id: string; readonly priorMs: number }>();
const owners = new WeakMap<TimelineInterval, TimelineInterval>();
const producers = new WeakMap<TimelineInterval, ComputationProducer>();
const identities = new WeakMap<object, string>(), publishedIdentities = new WeakSet<TimelineInterval>();
const concurrencyGroups = new WeakMap<TimelineInterval, ReadonlySet<string>>();
const incompleteReuse = new WeakSet<TimelineInterval>();
const clocks = new WeakMap<TimelineInterval, string>();
const identityNamespace = randomUUID();
let nextIdentity = 0;

export interface ComputationProducer {
	readonly source: string;
	readonly mode?: string;
	/** Opaque live producer lineage; never written to persisted evidence or diagnostic metrics. */
	readonly feedback?: unknown;
}
export interface ModeComputationTiming { readonly source: string; readonly mode: string; readonly reusedExecutionMs: number; }
export interface ComputationReuseShare { readonly source: string; readonly feedback: unknown; readonly reusedExecutionMs: number; }
type ComputationLineage = Pick<ComputationReuseShare, "source" | "feedback">;

/** Immutable calculation evidence; carries no Actor, candidate or output ownership. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;

	constructor(startedAt: number, completedAt: number, inputs: readonly TimelineDependency[] = []) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		if (inputs.length) dependencies.set(this, Object.freeze(inputs.map(input => Object.freeze({
			computation: TimelineInterval.from(input.computation),
			owned: input.owned, reused: input.reused, overhead: input.overhead, computeUncertain: input.computeUncertain,
			shared: Object.freeze((input.shared ?? (input.overhead ? [input.computation] : [])).map(TimelineInterval.from)),
		}))));
		for (const input of dependencies.get(this) ?? []) if (input.owned) owners.set(input.computation, this);
		Object.freeze(this);
	}

	static from(interval: TimelineInterval): TimelineInterval {
		const { startedAt, completedAt } = interval;
		return interval instanceof TimelineInterval ? interval : new TimelineInterval(startedAt, completedAt);
	}

	/** Failed evaluations never transfer their borrowed work to the caller. */
	static async collect<T>(execute: () => T | Promise<T>) {
		const scope = { inputs: new Map<TimelineInterval, Receipt>() as Map<TimelineInterval, Receipt> | undefined };
		try {
			const output = await collecting.run(scope, execute);
			return { output, dependencies: Object.freeze([...scope.inputs!].map(([computation, receipt]) => ({ computation, ...receipt, shared: [computation] }))) };
		} finally { scope.inputs = undefined; }
	}

	static use(computation: TimelineInterval | undefined): void {
		const inputs = collecting.getStore()?.inputs;
		if (computation && inputs && !inputs.has(computation)) inputs.set(computation, { reused: true });
	}

	static own(computation: TimelineInterval): TimelineInterval {
		collecting.getStore()?.inputs?.set(computation, { owned: true });
		return computation;
	}

	static exclude(computation: TimelineInterval, computeUncertain = false): void {
		collecting.getStore()?.inputs?.set(computation, { overhead: true, ...(computeUncertain ? { computeUncertain: true } : {}) });
	}

	/** The same accounting boundary for validation, adoption and cleanup in every backend. */
	static async overhead<T>(execute: () => T | Promise<T>): Promise<T> {
		const startedAt = performance.now();
		try { return await execute(); }
		finally { TimelineInterval.exclude(new TimelineInterval(startedAt, performance.now())); }
	}

	/** Diagnostic provenance belongs to the physical producer; later consumers cannot replace it. */
	static producedBy(computation: TimelineInterval, producer: ComputationProducer): void {
		if (producers.has(computation)) return;
		const owner = computationProducer(computation) ?? Object.freeze({ source: producer.source,
			...(producer.mode !== undefined ? { mode: producer.mode } : {}), ...(producer.feedback !== undefined ? { feedback: producer.feedback } : {}) });
		producers.set(computation, owner);
		for (const input of dependencies.get(computation) ?? []) if (input.owned) TimelineInterval.producedBy(input.computation, owner);
	}

	/** A production can establish parallelism before its enclosing interval exists. */
	static group(computation: TimelineInterval, owner: object): void {
		const groups = new Set(concurrencyGroups.get(computation));
		groups.add(objectIdentity(owner)); concurrencyGroups.set(computation, groups);
	}

	/** Missing historical evidence contributes no invented duration and remains visibly incomplete. */
	static unknownReuse(id: string): TimelineInterval {
		const computation = TimelineInterval.retained(id, 0);
		incompleteReuse.add(computation);
		return computation;
	}

	/** Persist the same immutable evidence used by recordCall; saturation omits the entire graph. */
	static serialize(computation: TimelineInterval): SerializedTimelineComputation | undefined {
		const selected = new Map<string, { computation: TimelineInterval; groups: Set<string>; producer?: ComputationProducer | null }>();
		const visited = new Set<TimelineInterval>();
		let valid = true, edgeCount = 0, spanCount = 0, groupCount = 0;
		const visit = (current: TimelineInterval): void => {
			if (!valid || visited.has(current)) return;
			visited.add(current);
			if (visited.size > TIMELINE_COMPUTATION_LIMITS.nodes * 4) { valid = false; return; }
			const id = computationIdentity(current), prior = selected.get(id);
			const chosen = preferredComputation(prior?.computation, current);
			const groups = prior?.groups ?? new Set<string>();
			const aliases = computationGroups(current, true, TIMELINE_COMPUTATION_LIMITS.groups);
			if (aliases.size > TIMELINE_COMPUTATION_LIMITS.groups) { valid = false; return; }
			for (const group of aliases) if (!groups.has(group)) { groups.add(group); groupCount++; }
			if (groupCount > TIMELINE_COMPUTATION_LIMITS.groups) { valid = false; return; }
			const producer = computationProducer(current);
			selected.set(id, { computation: chosen, groups, producer: mergeProducers(prior?.producer, producer) });
			publishedIdentities.add(current);
			if (selected.size > TIMELINE_COMPUTATION_LIMITS.nodes) { valid = false; return; }
			for (const input of dependencies.get(current) ?? []) {
				spanCount += input.shared?.length ?? 0;
				if (++edgeCount > TIMELINE_COMPUTATION_LIMITS.edges * 4 || spanCount > TIMELINE_COMPUTATION_LIMITS.spans * 4) {
					valid = false; return;
				}
				visit(input.computation);
			}
		};
		visit(computation);
		if (!valid) return undefined;
		const reachable = new Set<string>();
		const include = (id: string): void => {
			if (reachable.has(id)) return;
			reachable.add(id);
			for (const input of dependencies.get(selected.get(id)!.computation) ?? []) include(computationIdentity(input.computation));
		};
		include(computationIdentity(computation));
		const nodes: SerializedTimelineNode[] = [];
		for (const [id, { computation: current, groups, producer }] of selected) {
			if (!reachable.has(id)) continue;
			const priorMs = provenance.get(current)?.priorMs;
			nodes.push({ id, clock: computationClock(current), startedAt: current.startedAt, completedAt: current.completedAt,
				...(priorMs ? { priorMs } : {}), ...(producer ? { producer: { source: producer.source,
					...(producer.mode !== undefined ? { mode: producer.mode } : {}) } } : {}), ...(groups.size ? { groups: [...groups] } : {}),
				...(incompleteReuse.has(current) ? { incomplete: true as const } : {}),
				inputs: (dependencies.get(current) ?? []).map(input => ({ id: computationIdentity(input.computation),
					...(input.owned && computationIdentity(owners.get(input.computation) ?? current) === id ? { owned: true as const } : {}),
					...(input.reused ? { reused: true as const } : {}),
					...(input.overhead ? { overhead: true as const } : {}), ...(input.computeUncertain ? { computeUncertain: true as const } : {}),
					shared: (input.shared ?? []).map(part => ({ clock: computationClock(part), startedAt: part.startedAt, completedAt: part.completedAt })),
				})),
			});
		}
		return normalizeTimelineComputation({ version: 1, root: computationIdentity(computation), nodes });
	}

	/** Each restore is independent; stable identities deduplicate only within the receiving call. */
	static restore(value: unknown): TimelineInterval | undefined {
		const graph = normalizeTimelineComputation(value);
		if (!graph) return undefined;
		const nodes = new Map(graph.nodes.map(node => [node.id, node])), restored = new Map<string, TimelineInterval>();
		const restore = (id: string): TimelineInterval => {
			const existing = restored.get(id);
			if (existing) return existing;
			const node = nodes.get(id)!;
			const current = new TimelineInterval(node.startedAt, node.completedAt, (node.inputs ?? []).map(input => ({
				computation: restore(input.id), owned: input.owned, reused: input.reused, overhead: input.overhead,
				computeUncertain: input.computeUncertain, shared: input.shared?.map(part => {
					const span = new TimelineInterval(part.startedAt, part.completedAt);
					clocks.set(span, part.clock);
					return span;
				}),
			})));
			identities.set(current, id); publishedIdentities.add(current);
			clocks.set(current, node.clock);
			provenance.set(current, { id, priorMs: node.priorMs ?? 0 });
			if (node.groups) concurrencyGroups.set(current, new Set(node.groups));
			if (node.producer) producers.set(current, node.producer);
			if (node.incomplete) incompleteReuse.add(current);
			restored.set(id, current);
			return current;
		};
		return restore(graph.root);
	}

	/** Retained history contributes its measured duration even without a same-process clock. */
	static retained(id: string, durationMs: number, original?: TimelineInterval): TimelineInterval {
		const computation = original ?? new TimelineInterval(0, 0);
		const identity = publishedIdentities.has(computation) ? computationIdentity(computation) : id;
		identities.set(computation, identity);
		provenance.set(computation, { id: identity, priorMs: original ? provenance.get(original)?.priorMs ?? 0 : metric(durationMs) });
		return computation;
	}
}

export interface ToolComputationTiming {
	readonly actorComputeMs?: number;
	readonly reusedExecutionMs: number;
	/** The recorded reuse is a lower bound because some original evidence is unavailable. */
	readonly reusedExecutionIncomplete?: true;
	/** Only unambiguous producer shares of the same deduplicated reuse total. */
	readonly reusedByMode?: readonly ModeComputationTiming[];
}
export interface SpeculativeTaskTiming extends ReturnType<TaskTimeline["measure"]> {}

/** Gross Actor computation ratio; zero remaining calculation is represented separately as fully reused. */
export function toolSpeedup({ actorComputeMs, reusedExecutionMs, reusedExecutionIncomplete }: ToolComputationTiming): number | null {
	return !reusedExecutionIncomplete && actorComputeMs !== undefined && actorComputeMs > 0 && Number.isFinite(actorComputeMs + reusedExecutionMs) && reusedExecutionMs >= 0
		? (actorComputeMs + reusedExecutionMs) / actorComputeMs : null;
}

export class TaskTimeline {
	private readonly toolWaits: number[] = [];
	private actorComputeMs: number | undefined = 0;
	private reusedExecutionMs = 0;
	private reusedExecutionIncomplete = false;
	readonly startedAt: number;

	constructor(startedAt: number) { this.startedAt = metric(startedAt); }

	/** Raw latency for diagnosis only; it is never the computation-ratio denominator. */
	startToolWait(startedAt: number): (completedAt: number) => void {
		const end = this.toolWaits.push(startedAt, Number.MAX_VALUE) - 1;
		return completedAt => { this.toolWaits[end] = Math.min(this.toolWaits[end]!, metric(completedAt)); };
	}

	recordTool(computation: TimelineInterval, reused = false): ToolComputationTiming {
		return this.recordCall([{ computation, reused }]);
	}

	/** Exactly one settled Actor call. Shared work counts once here; an independent later reuse counts again. */
	recordCall(roots: readonly TimelineDependency[], onReused?: (shares: readonly ComputationReuseShare[]) => void): ToolComputationTiming {
		const selected = new Map<string, { computation: TimelineInterval; flags: number; producer?: ComputationProducer | null; lineage?: ComputationLineage | null }>();
		const visited = new Map<TimelineInterval, number>();
		const groups = new ComputationGroups();
		let complete = true, reusedExecutionIncomplete = false;
		const visit = (input: TimelineDependency, inheritedReuse = false): void => {
			if (input.computeUncertain && !inheritedReuse && !input.reused) complete = false;
			if (input.computeUncertain && (inheritedReuse || input.reused)) reusedExecutionIncomplete = true;
			if (input.overhead) return;
			const computation = input.computation, reused = inheritedReuse || !!input.reused, flag = reused ? 2 : 1;
			if ((visited.get(computation) ?? 0) & flag) return;
			visited.set(computation, (visited.get(computation) ?? 0) | flag);
			const key = computationIdentity(computation), previous = selected.get(key);
			for (const group of computationGroups(computation)) groups.join(key, group);
			const producer = computationProducer(computation), combined = mergeProducers(previous?.producer, producer);
			selected.set(key, { computation: preferredComputation(previous?.computation, computation),
				flags: (previous?.flags ?? 0) | flag,
				producer: combined, lineage: combined === null ? null : mergeLineages(previous?.lineage,
					producer?.feedback !== undefined ? { source: producer.source, feedback: producer.feedback } : undefined) });
			for (const dependency of dependencies.get(computation) ?? []) visit(dependency, reused);
		};
		for (const root of roots) visit(root);
		const grouped = [new Map<string, TimelineInterval[]>(), new Map<string, TimelineInterval[]>()];
		const attributed = new Map<string, { interval: TimelineInterval; producer?: ComputationProducer }[]>();
		const policy = new Map<string, { interval: TimelineInterval; producer?: ComputationLineage }[]>();
		const lineageTokens = new Map<unknown, Map<string, ComputationLineage>>(), receipts = new Map<ComputationLineage, number>();
		const lineageToken = (lineage: ComputationLineage): ComputationLineage => {
			let sources = lineageTokens.get(lineage.feedback);
			if (!sources) { sources = new Map(); lineageTokens.set(lineage.feedback, sources); }
			let token = sources.get(lineage.source);
			if (!token) { token = lineage; sources.set(lineage.source, token); }
			return token;
		};
		const creditLineage = (lineage: ComputationLineage | null | undefined, duration: number): void => {
			if (!lineage || duration <= 0) return;
			const token = lineageToken(lineage);
			receipts.set(token, (receipts.get(token) ?? 0) + duration);
		};
		const modes = new Map<string, ModeComputationTiming>();
		const credit = (producer: ComputationProducer | null | undefined, duration: number): void => {
			if (!producer?.mode || duration <= 0) return;
			const key = producerKey(producer);
			modes.set(key, { source: producer.source, mode: producer.mode, reusedExecutionMs: (modes.get(key)?.reusedExecutionMs ?? 0) + duration });
		};
		const totals = [0, 0];
		for (const { computation, flags, producer, lineage } of selected.values()) {
			if (flags & 2 && incompleteReuse.has(computation)) reusedExecutionIncomplete = true;
			const source = provenance.get(computation);
			const shared = (dependencies.get(computation) ?? []).flatMap(input => (input.shared ?? []).flatMap(part => {
				if (computationClock(part) !== computationClock(computation)) return [];
				// A current-clock join has no coordinate relationship to an older producer's interval.
				const sameClock = computationClock(part) === computationClock(input.computation);
				return [{ startedAt: sameClock ? Math.max(input.computation.startedAt, part.startedAt) : part.startedAt,
					completedAt: sameClock ? Math.min(input.computation.completedAt, part.completedAt) : part.completedAt }];
			})).filter(part => part.completedAt > part.startedAt);
			const all = exclusiveIntervals(computation, shared);
			const owner = JSON.stringify([groups.owner(computationIdentity(computation)), computationClock(computation)]);
			for (const [index, groupsByOwner] of grouped.entries()) if (flags & (1 << index)) {
				totals[index]! += source?.priorMs ?? 0;
				const group = groupsByOwner.get(owner) ?? [];
				group.push(...all); groupsByOwner.set(owner, group);
			}
			if (flags & 2) {
				credit(producer, source?.priorMs ?? 0);
				const group = attributed.get(owner) ?? [];
				group.push(...all.map(interval => ({ interval, producer: producer?.mode ? producer : undefined }))); attributed.set(owner, group);
				if (onReused) {
					creditLineage(lineage, source?.priorMs ?? 0);
					const shares = policy.get(owner) ?? [];
					shares.push(...all.map(interval => ({ interval, producer: lineage ?? undefined }))); policy.set(owner, shares);
				}
			}
		}
		for (const [index, groupsByOwner] of grouped.entries()) for (const all of groupsByOwner.values()) totals[index]! += unionDuration(all);
		for (const spans of attributed.values()) attributeUnion(spans, credit, producerKey);
		if (onReused) for (const spans of policy.values()) attributeUnion(spans, creditLineage, lineageToken);
		const reusedByMode = Object.freeze([...modes.values()].map(value => Object.freeze(value)));
		const timing = Object.freeze({ actorComputeMs: complete ? totals[0]! : undefined, reusedExecutionMs: totals[1]!,
			...(reusedExecutionIncomplete ? { reusedExecutionIncomplete: true as const } : {}),
			...(reusedByMode.length ? { reusedByMode } : {}) });
		this.actorComputeMs = this.actorComputeMs !== undefined && timing.actorComputeMs !== undefined ? this.actorComputeMs + timing.actorComputeMs : undefined;
		this.reusedExecutionMs += timing.reusedExecutionMs;
		this.reusedExecutionIncomplete ||= reusedExecutionIncomplete;
		onReused?.(Object.freeze([...receipts].map(([lineage, reusedExecutionMs]) => Object.freeze({ ...lineage, reusedExecutionMs }))));
		return timing;
	}

	measure(endedAt: number) {
		const startedAt = this.startedAt, completedAt = Math.max(startedAt, metric(endedAt));
		const waits: TimelineInterval[] = [];
		for (let index = 0; index < this.toolWaits.length; index += 2) {
			const start = Math.max(startedAt, metric(this.toolWaits[index]!)), end = Math.min(completedAt, metric(this.toolWaits[index + 1]!));
			if (end > start) waits.push({ startedAt: start, completedAt: end });
		}
		return Object.freeze({ startedAt, completedAt, actorComputeMs: this.actorComputeMs,
			reusedExecutionMs: this.reusedExecutionMs, ...(this.reusedExecutionIncomplete ? { reusedExecutionIncomplete: true as const } : {}),
			toolWaitMs: unionDuration(waits) });
	}
}

function objectIdentity(object: object): string {
	let id = identities.get(object);
	if (!id) { id = `timeline:${identityNamespace}:${++nextIdentity}`; identities.set(object, id); }
	return id;
}

function computationClock(computation: TimelineInterval): string { return clocks.get(computation) ?? identityNamespace; }

function computationIdentity(computation: TimelineInterval): string {
	const existing = identities.get(computation);
	if (existing) return existing;
	const id = provenance.get(computation)?.id;
	if (id) { identities.set(computation, id); return id; }
	return objectIdentity(computation);
}

function preferredComputation(previous: TimelineInterval | undefined, current: TimelineInterval): TimelineInterval {
	if (!previous || incompleteReuse.has(previous) && !incompleteReuse.has(current)) return current;
	if (incompleteReuse.has(current) && !incompleteReuse.has(previous)) return previous;
	return previous.completedAt <= previous.startedAt && current.completedAt > current.startedAt ? current : previous;
}

/** Follow ownership only to obtain aliases; ancestors are never selected as consumed work. */
function computationGroups(computation: TimelineInterval, publish = false, limit = Infinity): ReadonlySet<string> {
	const groups = new Set<string>(), visited = new Set<TimelineInterval>();
	let current: TimelineInterval | undefined = computation;
	while (current && !visited.has(current)) {
		visited.add(current);
		if (publish) publishedIdentities.add(current);
		for (const group of concurrencyGroups.get(current) ?? []) {
			groups.add(group);
			if (groups.size > limit) return groups;
		}
		const owner = owners.get(current);
		if (owner) groups.add(computationIdentity(owner));
		if (groups.size > limit) return groups;
		current = owner;
	}
	return groups;
}

/** Per-call union of original concurrency identities, including independently restored aliases. */
class ComputationGroups {
	private readonly parents = new Map<string, string>();
	owner(id: string): string {
		const parent = this.parents.get(id);
		if (!parent) return id;
		const owner = this.owner(parent);
		if (owner !== parent) this.parents.set(id, owner);
		return owner;
	}
	join(left: string, right: string): void {
		const a = this.owner(left), b = this.owner(right);
		if (a !== b) this.parents.set(a, b);
	}
}

function producerKey(producer: ComputationProducer): string { return JSON.stringify([producer.source, producer.mode]); }

function mergeProducers(previous: ComputationProducer | null | undefined, current: ComputationProducer | undefined): ComputationProducer | null | undefined {
	if (previous === null || previous && current && (previous.source !== current.source ||
		previous.mode !== undefined && current.mode !== undefined && previous.mode !== current.mode)) return null;
	return previous?.mode !== undefined ? previous : current?.mode !== undefined ? current : previous ?? current;
}

function mergeLineages(previous: ComputationLineage | null | undefined, current: ComputationLineage | undefined): ComputationLineage | null | undefined {
	if (previous === null || previous && current && (previous.source !== current.source ||
		previous.feedback !== current.feedback && !Object.is(previous.feedback, current.feedback))) return null;
	return previous ?? current;
}

/** Ownership passes production provenance to children; borrowed dependencies have their own owner chain. */
function computationProducer(computation: TimelineInterval): ComputationProducer | undefined {
	let current: TimelineInterval | undefined = computation;
	while (current) {
		const producer = producers.get(current);
		if (producer) return producer;
		current = owners.get(current);
	}
	return undefined;
}

/** The same owner union as R; an overlapping segment with conflicting or missing provenance stays unassigned. */
function attributeUnion<T>(spans: readonly { interval: TimelineInterval; producer?: T }[],
	credit: (producer: T, duration: number) => void, keyOf: (producer: T) => unknown): void {
	const points = spans.flatMap(({ interval, producer }) => [
		{ at: interval.startedAt, delta: 1, producer }, { at: interval.completedAt, delta: -1, producer },
	]).sort((left, right) => left.at - right.at);
	const active = new Map<unknown, { producer: T; count: number }>();
	let previous = 0, unknown = 0;
	for (const point of points) {
		if (point.at > previous && unknown === 0 && active.size === 1) credit(active.values().next().value!.producer, point.at - previous);
		previous = point.at;
		if (!point.producer) { unknown += point.delta; continue; }
		const key = keyOf(point.producer), count = (active.get(key)?.count ?? 0) + point.delta;
		if (count) active.set(key, { producer: point.producer, count }); else active.delete(key);
	}
}

function exclusiveIntervals(interval: TimelineInterval, shared: readonly TimelineInterval[]): TimelineInterval[] {
	const intervals: TimelineInterval[] = [];
	let start = interval.startedAt;
	for (const part of [...shared].sort((left, right) => left.startedAt - right.startedAt)) {
		if (part.completedAt <= start || part.startedAt >= interval.completedAt) continue;
		if (part.startedAt > start) intervals.push({ startedAt: start, completedAt: part.startedAt });
		start = Math.min(interval.completedAt, Math.max(start, part.completedAt));
	}
	if (start < interval.completedAt) intervals.push({ startedAt: start, completedAt: interval.completedAt });
	return intervals;
}

function unionDuration(intervals: readonly TimelineInterval[]): number {
	const sorted = [...intervals].sort((left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt);
	let total = 0, start = 0, end = 0;
	for (const interval of sorted) {
		if (interval.startedAt > end) { total += end - start; start = interval.startedAt; }
		end = Math.max(end, interval.completedAt);
	}
	return total + end - start;
}
