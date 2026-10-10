import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { nonNegativeFinite as metric } from "./number-utils.ts";
import { normalizeTimelineComputation, type SerializedTimelineComputation, type SerializedTimelineNode } from "./timeline-computation.ts";
export { normalizeTimelineComputation, type SerializedTimelineComputation } from "./timeline-computation.ts";

export interface TimelineDependency {
	readonly computation: TimelineInterval;
	/** Work produced here retains its enclosing tool's parallelism. */
	readonly owned?: boolean;
	/** Only a successful consumption of existing work issues this receipt. */
	readonly reused?: boolean;
	/** Adoption-time validation and delivery are excluded; original proof and sealing are computation. */
	readonly overhead?: boolean;
	/** The current boundary cannot separate remaining calculation from adoption overhead. */
	readonly computeUncertain?: boolean;
	/** Parts already included in the enclosing computation, or spent joining this input. */
	readonly shared?: readonly TimelineInterval[];
}

type Receipt = Pick<TimelineDependency, "owned" | "reused" | "overhead" | "computeUncertain">;
const dependencies = new WeakMap<TimelineInterval, readonly TimelineDependency[]>();
const collecting = new AsyncLocalStorage<{ inputs?: Map<TimelineInterval, Receipt> }>();
const calculating = new AsyncLocalStorage<CalculationClock | undefined>();
const calculationSpans = new WeakMap<TimelineInterval, readonly TimelineInterval[]>();
const provenance = new WeakMap<TimelineInterval, { readonly id: string; readonly priorMs: number }>();
const owners = new WeakMap<TimelineInterval, TimelineInterval>();
const producers = new WeakMap<TimelineInterval, ComputationProducer>();
const identities = new WeakMap<object, string>(), publishedIdentities = new WeakSet<TimelineInterval>();
const concurrencyGroups = new WeakMap<TimelineInterval, ReadonlySet<string>>();
const incompleteReuse = new WeakSet<TimelineInterval>();
const clocks = new WeakMap<TimelineInterval, string>();
const timeOrigins = new WeakMap<TimelineInterval, number>();
const identityNamespace = randomUUID();
let nextIdentity = 0;

export interface ComputationProducer {
	readonly source: string;
	readonly mode?: string;
}
export interface ModeComputationTiming { readonly source: string; readonly mode: string; readonly hiddenComputeMs: number; }

/** Immutable calculation evidence; carries no Actor, candidate or output ownership. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;

	constructor(startedAt: number, completedAt: number, inputs: readonly TimelineDependency[] = [], spans?: readonly TimelineInterval[]) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		if (spans) calculationSpans.set(this, Object.freeze(spans.map(TimelineInterval.from)));
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

	/** An abandoned evaluation spent its own calculation, but did not successfully consume borrowed work. */
	static attempted(computation: TimelineInterval): TimelineInterval {
		const copies = new Map<TimelineInterval, TimelineInterval>(), pending = [{ current: computation, finished: false }];
		while (pending.length) {
			const { current, finished } = pending.pop()!;
			if (copies.has(current)) continue;
			if (!finished) {
				pending.push({ current, finished: true });
				for (const input of dependencies.get(current) ?? []) if (!input.reused) pending.push({ current: input.computation, finished: false });
				continue;
			}
			const result = new TimelineInterval(current.startedAt, current.completedAt, (dependencies.get(current) ?? []).map(input =>
				input.reused ? { computation: input.computation, overhead: true, shared: input.shared }
					: { ...input, computation: copies.get(input.computation)! }), calculationSpans.get(current));
			identities.set(result, computationIdentity(current)); clocks.set(result, computationClock(current));
			const timeOrigin = computationTimeOrigin(current);
			if (timeOrigin !== undefined) timeOrigins.set(result, timeOrigin);
			const source = provenance.get(current), producer = producers.get(current), groups = concurrencyGroups.get(current);
			if (source) provenance.set(result, source);
			if (producer) producers.set(result, producer);
			if (groups) concurrencyGroups.set(result, groups);
			copies.set(current, result);
		}
		return copies.get(computation)!;
	}

	static exclude(computation: TimelineInterval, computeUncertain = false): void {
		collecting.getStore()?.inputs?.set(computation, { overhead: true, ...(computeUncertain ? { computeUncertain: true } : {}) });
	}

	/** Record only active calculation segments; control work never produces a duration or a cost sample. */
	static async measure<T>(execute: () => T | Promise<T>, inputs: (output: T) => readonly TimelineDependency[] = () => []) {
		const startedAt = performance.now(), clock = new CalculationClock(startedAt, calculating.getStore());
		try {
			const evaluation = await calculating.run(clock, () => TimelineInterval.collect(execute));
			const completedAt = clock.finish();
			let computation: TimelineInterval;
			try { computation = new TimelineInterval(startedAt, completedAt, [...evaluation.dependencies, ...inputs(evaluation.output)], clock.spans); }
			catch { computation = new TimelineInterval(startedAt, completedAt, [], clock.spans); } // Evidence cannot replace the execution outcome.
			return { ...evaluation, computation };
		} finally { clock.finish(); }
	}

	/** Cross a calculation boundary without timing adoption-time validation, delivery or cleanup. */
	static async outside<T>(execute: () => T | Promise<T>): Promise<T> {
		const clock = calculating.getStore();
		if (!clock) return execute();
		for (let current: CalculationClock | undefined = clock; current; current = current.parent) current.pause();
		try { return await calculating.run(undefined, execute); }
		finally { for (let current: CalculationClock | undefined = clock; current; current = current.parent) current.resume(); }
	}

	/** Freeze the active calculation up to an observed native execution boundary. */
	static current(startedAt: number, completedAt: number, inputs: readonly TimelineDependency[] = []): TimelineInterval {
		return new TimelineInterval(startedAt, completedAt, inputs, calculating.getStore()?.read(startedAt, completedAt));
	}

	/** Diagnostic provenance belongs to the physical producer; later consumers cannot replace it. */
	static producedBy(computation: TimelineInterval, producer: ComputationProducer): void {
		const pending = [{ computation, producer }];
		while (pending.length) {
			const { computation: current, producer } = pending.pop()!;
			if (producers.has(current)) continue;
			const owner = computationProducer(current) ?? Object.freeze({ source: producer.source,
				...(producer.mode !== undefined ? { mode: producer.mode } : {}) });
			producers.set(current, owner);
			for (const input of dependencies.get(current) ?? []) if (input.owned) pending.push({ computation: input.computation, producer: owner });
		}
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

	/** Persist the complete immutable evidence used by recordCall, regardless of graph size. */
	static serialize(computation: TimelineInterval): SerializedTimelineComputation | undefined {
		const selected = new Map<string, { computation: TimelineInterval; groups: Set<string>; producer?: ComputationProducer | null }>();
		const visited = new Set<TimelineInterval>(), pending = [computation];
		while (pending.length) {
			const current = pending.pop()!;
			if (visited.has(current)) continue;
			visited.add(current);
			const id = computationIdentity(current), prior = selected.get(id);
			const chosen = preferredComputation(prior?.computation, current);
			const groups = prior?.groups ?? new Set<string>();
			for (const group of computationGroups(current, true)) groups.add(group);
			const producer = computationProducer(current);
			selected.set(id, { computation: chosen, groups, producer: mergeProducers(prior?.producer, producer) });
			publishedIdentities.add(current);
			const inputs = dependencies.get(current) ?? [];
			for (let index = inputs.length - 1; index >= 0; index--) pending.push(inputs[index]!.computation);
		}
		const reachable = new Set<string>(), remaining = [computationIdentity(computation)];
		while (remaining.length) {
			const id = remaining.pop()!;
			if (reachable.has(id)) continue;
			reachable.add(id);
			for (const input of dependencies.get(selected.get(id)!.computation) ?? []) remaining.push(computationIdentity(input.computation));
		}
		const nodes: SerializedTimelineNode[] = [];
		for (const [id, { computation: current, groups, producer }] of selected) {
			if (!reachable.has(id)) continue;
			const priorMs = provenance.get(current)?.priorMs;
			const spans = calculationSpans.get(current);
			nodes.push({ id, clock: computationClock(current), startedAt: current.startedAt, completedAt: current.completedAt,
				...(computationTimeOrigin(current) !== undefined ? { timeOrigin: computationTimeOrigin(current) } : {}),
				...(spans ? { spans: spans.map(({ startedAt, completedAt }) => ({ startedAt, completedAt })) } : {}),
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
		return normalizeTimelineComputation({ version: 3, root: computationIdentity(computation), nodes });
	}

	/** Each restore is independent; stable identities deduplicate only within the receiving call. */
	static restore(value: unknown): TimelineInterval | undefined {
		const graph = normalizeTimelineComputation(value);
		if (!graph) return undefined;
		const restored = new Map<string, TimelineInterval>();
		for (const node of graph.nodes) {
			const current = new TimelineInterval(node.startedAt, node.completedAt, (node.inputs ?? []).map(input => ({
				computation: restored.get(input.id)!, owned: input.owned, reused: input.reused, overhead: input.overhead,
				computeUncertain: input.computeUncertain, shared: input.shared?.map(part => {
					const span = new TimelineInterval(part.startedAt, part.completedAt);
					clocks.set(span, part.clock);
					return span;
				}),
			})), node.spans?.map(span => new TimelineInterval(span.startedAt, span.completedAt)));
			identities.set(current, node.id); publishedIdentities.add(current);
			clocks.set(current, node.clock);
			if (node.timeOrigin !== undefined) timeOrigins.set(current, node.timeOrigin);
			provenance.set(current, { id: node.id, priorMs: node.priorMs ?? 0 });
			if (node.groups) concurrencyGroups.set(current, new Set(node.groups));
			if (node.producer) producers.set(current, node.producer);
			if (node.incomplete) incompleteReuse.add(current);
			restored.set(node.id, current);
		}
		return restored.get(graph.root);
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
	/** All calculation actually used by this call, including the producer's remaining work. */
	readonly toolComputeMs?: number;
	/** Successfully reused calculation completed before this Actor call was issued. */
	readonly hiddenComputeMs: number;
	/** Actor wait outside consumed execution boundaries, including validation and delivery; never part of T/(T-H). */
	readonly adoptionWaitMs?: number;
	/** A successful receipt, even when no calculation was hidden before the call. */
	readonly reused?: true;
	/** The recorded hidden time is a lower bound because original timing evidence is unavailable. */
	readonly hiddenComputeIncomplete?: true;
	/** Only unambiguous producer shares of the same deduplicated hidden total. */
	readonly hiddenByMode?: readonly ModeComputationTiming[];
}

/** Pausing closes the current calculation; no interval is created for time outside it. */
class CalculationClock {
	readonly spans: TimelineInterval[] = [];
	private startedAt: number;
	private paused = 0;
	private completedAt?: number;
	readonly parent?: CalculationClock;
	constructor(startedAt: number, parent?: CalculationClock) { this.startedAt = startedAt; this.parent = parent; }
	read(startedAt: number, completedAt: number): readonly TimelineInterval[] {
		return [...this.spans, ...(!this.paused && this.completedAt === undefined ? [new TimelineInterval(this.startedAt, completedAt)] : [])]
			.filter(span => span.completedAt > startedAt && span.startedAt < completedAt)
			.map(span => new TimelineInterval(Math.max(startedAt, span.startedAt), Math.min(completedAt, span.completedAt)));
	}
	pause(): void {
		if (this.completedAt !== undefined || this.paused++ > 0) return;
		this.close(performance.now());
	}
	resume(): void {
		if (this.completedAt !== undefined || --this.paused > 0) return;
		this.startedAt = performance.now();
	}
	finish(): number {
		if (this.completedAt === undefined) {
			this.completedAt = performance.now();
			if (!this.paused) this.close(this.completedAt);
		}
		return this.completedAt;
	}
	private close(completedAt: number): void {
		if (completedAt > this.startedAt) this.spans.push(new TimelineInterval(this.startedAt, completedAt));
	}
}
export interface SpeculativeTaskTiming extends ReturnType<TaskTimeline["measure"]> {}

/** T / (T - H); adoption costs are excluded from both quantities. */
export function toolSpeedup({ toolComputeMs, hiddenComputeMs, hiddenComputeIncomplete }: ToolComputationTiming): number | null {
	return !hiddenComputeIncomplete && toolComputeMs !== undefined && Number.isFinite(toolComputeMs) && hiddenComputeMs >= 0 && toolComputeMs > hiddenComputeMs
		? toolComputeMs / (toolComputeMs - hiddenComputeMs) : null;
}

export class TaskTimeline {
	private readonly toolWaits: { startedAt: number; completedAt: number }[] = [];
	private adoptionWaits: TimelineInterval[] | undefined = [];
	private toolComputeMs: number | undefined = 0;
	private hiddenComputeMs = 0;
	private hiddenComputeIncomplete = false;
	readonly startedAt: number;

	constructor(startedAt: number) { this.startedAt = metric(startedAt); }

	/** Raw latency for diagnosis only; it is never the computation-ratio denominator. */
	startToolWait(startedAt: number): (completedAt: number) => void {
		const wait = { startedAt, completedAt: Number.MAX_VALUE }; this.toolWaits.push(wait);
		return completedAt => { wait.completedAt = Math.min(wait.completedAt, metric(completedAt)); };
	}

	recordTool(computation: TimelineInterval, issuedAt: number, reused = false): ToolComputationTiming { return this.recordCall([{ computation, reused }], issuedAt); }

	/** Exactly one settled Actor call. Shared work counts once here; an independent later reuse counts again. */
	recordCall(roots: readonly TimelineDependency[], issuedAt: number, completedAt?: number): ToolComputationTiming {
		const selected = new Map<string, { computation: TimelineInterval; flags: number; producer?: ComputationProducer | null }>();
		const visited = new Map<TimelineInterval, number>(), groups = new ComputationGroups();
		let complete = true, hiddenComputeIncomplete = !Number.isFinite(issuedAt);
		let adoptionComplete = Number.isFinite(issuedAt) && completedAt !== undefined && Number.isFinite(completedAt) && completedAt >= issuedAt;
		const observed: TimelineInterval[] = [];
		const pending = roots.map(input => ({ input, inheritedReuse: false })).reverse();
		while (pending.length) {
			const { input, inheritedReuse } = pending.pop()!;
			if (input.computeUncertain) { complete = false; if (inheritedReuse || input.reused) hiddenComputeIncomplete = true; }
			if (input.overhead) continue;
			const computation = input.computation, reused = inheritedReuse || !!input.reused, flag = reused ? 2 : 1;
			if ((visited.get(computation) ?? 0) & flag) continue;
			visited.set(computation, (visited.get(computation) ?? 0) | flag);
			const key = computationIdentity(computation), previous = selected.get(key);
			for (const group of computationGroups(computation)) groups.join(key, group);
			selected.set(key, { computation: preferredComputation(previous?.computation, computation),
				flags: (previous?.flags ?? 0) | flag, producer: mergeProducers(previous?.producer, computationProducer(computation)) });
			const inputs = dependencies.get(computation) ?? [];
			for (let index = inputs.length - 1; index >= 0; index--) pending.push({ input: inputs[index]!, inheritedReuse: reused });
		}
		const grouped = [new Map<string, TimelineInterval[]>(), new Map<string, TimelineInterval[]>()];
		const attributed = new Map<string, { interval: TimelineInterval; producer?: ComputationProducer }[]>();
		const modes = new Map<string, ModeComputationTiming>();
		const credit = (producer: ComputationProducer | null | undefined, duration: number): void => {
			if (!producer?.mode || duration <= 0) return;
			const key = producerKey(producer);
			modes.set(key, { source: producer.source, mode: producer.mode, hiddenComputeMs: (modes.get(key)?.hiddenComputeMs ?? 0) + duration });
		};
		const totals = [0, 0];
		let reused = false;
		for (const { computation, flags, producer } of selected.values()) {
			if (incompleteReuse.has(computation)) { complete = false; if (flags & 2) hiddenComputeIncomplete = true; }
			const priorMs = provenance.get(computation)?.priorMs ?? 0;
			totals[0]! += priorMs;
			const shared = (dependencies.get(computation) ?? []).flatMap(input => (input.shared ?? []).flatMap(part => {
				if (computationClock(part) !== computationClock(computation)) return [];
				const sameClock = computationClock(part) === computationClock(input.computation);
				return [{ startedAt: sameClock ? Math.max(input.computation.startedAt, part.startedAt) : part.startedAt,
					completedAt: sameClock ? Math.min(input.computation.completedAt, part.completedAt) : part.completedAt }];
			})).filter(part => part.completedAt > part.startedAt);
			const all = (calculationSpans.get(computation) ?? [computation]).flatMap(span => exclusiveIntervals(span, shared));
			const owner = JSON.stringify([groups.owner(computationIdentity(computation)), computationClock(computation)]);
			const origin = computationTimeOrigin(computation);
			const cutoff = computationClock(computation) === identityNamespace ? issuedAt : origin === undefined ? NaN : issuedAt + (performance.timeOrigin - origin);
			if (priorMs > 0 || all.length > 0 && !Number.isFinite(cutoff)) adoptionComplete = false;
			if (adoptionComplete) for (const span of all) observed.push({ startedAt: span.startedAt + issuedAt - cutoff, completedAt: span.completedAt + issuedAt - cutoff });
			const hidden = flags & 2 && Number.isFinite(cutoff) ? all.filter(span => span.startedAt < cutoff)
				.map(span => ({ startedAt: span.startedAt, completedAt: Math.min(span.completedAt, cutoff) })) : [];
			for (const [index, intervals] of [all, hidden].entries()) {
				const group = grouped[index]!.get(owner) ?? [];
				for (const interval of intervals) group.push(interval);
				grouped[index]!.set(owner, group);
			}
			if (flags & 2) {
				reused = true;
				if (priorMs > 0 || all.length > 0 && !Number.isFinite(cutoff)) hiddenComputeIncomplete = true;
				const group = attributed.get(owner) ?? [];
				for (const interval of hidden) group.push({ interval, producer: producer?.mode ? producer : undefined });
				attributed.set(owner, group);
			}
		}
		for (const [index, groupsByOwner] of grouped.entries()) for (const all of groupsByOwner.values()) totals[index]! += unionDuration(all);
		for (const spans of attributed.values()) attributeUnion(spans, credit, producerKey);
		const hiddenByMode = Object.freeze([...modes.values()].map(value => Object.freeze(value)));
		const adoption = complete && adoptionComplete ? exclusiveIntervals(new TimelineInterval(issuedAt, completedAt!), observed) : undefined;
		if (adoption) { for (const interval of adoption) this.adoptionWaits?.push(interval); } else this.adoptionWaits = undefined;
		const timing = Object.freeze({ toolComputeMs: complete ? totals[0]! : undefined, hiddenComputeMs: totals[1]!,
			...(adoption ? { adoptionWaitMs: unionDuration(adoption) } : {}),
			...(reused ? { reused: true as const } : {}), ...(hiddenComputeIncomplete ? { hiddenComputeIncomplete: true as const } : {}),
			...(hiddenByMode.length ? { hiddenByMode } : {}) });
		this.toolComputeMs = this.toolComputeMs !== undefined && timing.toolComputeMs !== undefined ? this.toolComputeMs + timing.toolComputeMs : undefined;
		this.hiddenComputeMs += timing.hiddenComputeMs;
		this.hiddenComputeIncomplete ||= hiddenComputeIncomplete;
		return timing;
	}

	measure(endedAt: number) {
		const startedAt = this.startedAt, completedAt = Math.max(startedAt, metric(endedAt));
		const duration = (spans: readonly TimelineInterval[]) => unionDuration(spans.map(span => ({
			startedAt: Math.max(startedAt, metric(span.startedAt)), completedAt: Math.min(completedAt, metric(span.completedAt)),
		})).filter(span => span.completedAt > span.startedAt));
		return Object.freeze({ startedAt, completedAt, toolComputeMs: this.toolComputeMs,
			hiddenComputeMs: this.hiddenComputeMs, ...(this.hiddenComputeIncomplete ? { hiddenComputeIncomplete: true as const } : {}),
			...(this.adoptionWaits ? { adoptionWaitMs: duration(this.adoptionWaits) } : {}), toolWaitMs: duration(this.toolWaits) });
	}
}

function objectIdentity(object: object): string {
	let id = identities.get(object);
	if (!id) { id = `timeline:${identityNamespace}:${++nextIdentity}`; identities.set(object, id); }
	return id;
}

function computationTimeOrigin(computation: TimelineInterval): number | undefined { return computationClock(computation) === identityNamespace ? performance.timeOrigin : timeOrigins.get(computation); }

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
function computationGroups(computation: TimelineInterval, publish = false): ReadonlySet<string> {
	const groups = new Set<string>(), visited = new Set<TimelineInterval>();
	let current: TimelineInterval | undefined = computation;
	while (current && !visited.has(current)) {
		visited.add(current);
		if (publish) publishedIdentities.add(current);
		for (const group of concurrencyGroups.get(current) ?? []) groups.add(group);
		const owner = owners.get(current);
		if (owner) groups.add(computationIdentity(owner));
		current = owner;
	}
	return groups;
}

/** Per-call union of original concurrency identities, including independently restored aliases. */
class ComputationGroups {
	private readonly parents = new Map<string, string>();
	owner(id: string): string {
		let owner = id;
		while (this.parents.has(owner)) owner = this.parents.get(owner)!;
		while (this.parents.has(id)) { const parent = this.parents.get(id)!; this.parents.set(id, owner); id = parent; }
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

/** The same owner union as H; an overlapping segment with conflicting or missing provenance stays unassigned. */
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
	let total = 0, end = 0;
	for (const interval of [...intervals].sort((left, right) => left.startedAt - right.startedAt)) {
		total += Math.max(0, interval.completedAt - Math.max(end, interval.startedAt));
		end = Math.max(end, interval.completedAt);
	}
	return total;
}
