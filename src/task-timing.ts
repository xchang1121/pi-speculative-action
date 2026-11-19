export interface SerializedTimelineComputation {
	readonly root: string;
	readonly nodes: readonly SerializedTimelineNode[];
}
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { speculativeConfig } from "./config-schema.ts";
import type { WorldReuseMetrics } from "./execution-world.ts";
import { emptyWorldReuseMetrics } from "./execution-world.ts";
import {
	dependencyOrder,
	incrementCounts as increment,
	nonNegativeFinite as metric,
	isRecord as record,
} from "./primitives.ts";
import type { ResolutionCause, SpeculativeActionEvent, SpeculativeCacheSnapshot } from "./settlement.ts";
export interface SerializedTimelineNode {
	readonly id: string;
	/** Monotonic coordinate namespace; endpoints from different clocks are never compared. */
	readonly clock: string;
	/** Unix epoch origin of this monotonic clock, used only to compare Actor issue time. */
	readonly timeOrigin?: number;
	readonly startedAt: number;
	readonly completedAt: number;
	/** Explicit calculation segments; absence denotes continuous computation. */
	readonly spans?: readonly {
		readonly startedAt: number;
		readonly completedAt: number;
	}[];
	readonly priorMs?: number;
	readonly producer?: {
		readonly source: string;
		readonly mode?: string;
		readonly id?: string;
	};
	/** Concurrency aliases only: these never select an ancestor or its other work. */
	readonly groups?: readonly string[];
	readonly incomplete?: true;
	readonly inputs?: readonly {
		readonly id: string;
		readonly owned?: true;
		readonly reused?: true;
		readonly overhead?: true;
		readonly computeUncertain?: true;
		readonly shared?: readonly {
			readonly clock: string;
			readonly startedAt: number;
			readonly completedAt: number;
		}[];
	}[];
}
const INPUT_FLAGS = ["owned", "reused", "overhead", "computeUncertain"] as const;
/** Validate complete telemetry and order dependencies before consumers; invalid evidence does not invalidate replay. */
export function normalizeTimelineComputation(value: unknown): SerializedTimelineComputation | undefined {
	try {
		if (
			!record(value) ||
			Object.keys(value).some((key) => key !== "root" && key !== "nodes") ||
			!identity(value.root) ||
			!Array.isArray(value.nodes) ||
			!value.nodes.length
		)
			return undefined;
		const byID = new Map<string, SerializedTimelineNode>();
		const origins = new Map<string, number>();
		for (const raw of value.nodes) {
			if (
				!record(raw) ||
				!identity(raw.id) ||
				!identity(raw.clock) ||
				byID.has(raw.id) ||
				!endpoints(raw) ||
				(raw.priorMs !== undefined && !duration(raw.priorMs)) ||
				(raw.timeOrigin !== undefined && !duration(raw.timeOrigin)) ||
				(raw.incomplete !== undefined && typeof raw.incomplete !== "boolean")
			)
				return undefined;
			if (raw.timeOrigin !== undefined) {
				if (origins.has(raw.clock) && origins.get(raw.clock) !== raw.timeOrigin) return undefined;
				origins.set(raw.clock, raw.timeOrigin as number);
			}
			let producer: SerializedTimelineNode["producer"];
			const calculation = records(raw.spans, (part) => {
				if (!endpoints(part) || part.startedAt < raw.startedAt || part.completedAt > raw.completedAt)
					throw new Error();
				return { startedAt: part.startedAt, completedAt: part.completedAt };
			});
			if (raw.producer !== undefined) {
				if (
					!record(raw.producer) ||
					!identity(raw.producer.source) ||
					(raw.producer.mode !== undefined && !identity(raw.producer.mode)) ||
					(raw.producer.id !== undefined && !identity(raw.producer.id))
				)
					return undefined;
				producer = Object.freeze({
					source: raw.producer.source,
					...(raw.producer.mode !== undefined ? { mode: raw.producer.mode as string } : {}),
					...(raw.producer.id !== undefined ? { id: raw.producer.id as string } : {}),
				});
			}
			let aliases: readonly string[] | undefined;
			if (raw.groups !== undefined) {
				if (!Array.isArray(raw.groups)) return undefined;
				aliases = Object.freeze([...new Set<string>(raw.groups)]);
				if (!aliases.every(identity)) return undefined;
			}
			const inputs = records(raw.inputs, (input) => {
				if (
					!identity(input.id) ||
					INPUT_FLAGS.some((key) => input[key] !== undefined && typeof input[key] !== "boolean")
				)
					throw new Error();
				const shared = records(input.shared, (span) => {
					if (!identity(span.clock) || !endpoints(span)) throw new Error();
					return {
						clock: span.clock,
						startedAt: span.startedAt,
						completedAt: span.completedAt,
					};
				});
				return {
					id: input.id,
					...Object.fromEntries(INPUT_FLAGS.filter((key) => input[key]).map((key) => [key, true])),
					...(shared ? { shared } : {}),
				};
			});
			const node = Object.freeze({
				id: raw.id,
				clock: raw.clock,
				startedAt: raw.startedAt,
				completedAt: raw.completedAt,
				...(raw.timeOrigin !== undefined ? { timeOrigin: raw.timeOrigin as number } : {}),
				...(calculation ? { spans: calculation } : {}),
				...(raw.priorMs !== undefined ? { priorMs: raw.priorMs as number } : {}),
				...(producer ? { producer } : {}),
				...(aliases?.length ? { groups: aliases } : {}),
				...(raw.incomplete ? { incomplete: true as const } : {}),
				...(inputs?.length ? { inputs } : {}),
			});
			byID.set(node.id, node);
		}
		if (!byID.has(value.root)) return undefined;
		const owned = new Map<string, string>();
		for (const node of byID.values())
			for (const input of node.inputs ?? []) {
				if (!byID.has(input.id)) return undefined;
				if (input.owned) {
					if (owned.has(input.id) && owned.get(input.id) !== node.id) return undefined;
					owned.set(input.id, node.id);
				}
			}
		const ordered = dependencyOrder([byID.get(value.root)!], (node) =>
			[...(node.inputs ?? [])].reverse().map((input) => byID.get(input.id)),
		);
		if (!ordered || ordered.length !== byID.size) return undefined;
		return Object.freeze({
			root: value.root,
			nodes: Object.freeze(ordered),
		});
	} catch {
		return undefined;
	}
}
function records<Value extends object>(
	value: unknown,
	normalize: (item: Record<string, unknown>) => Value,
): readonly Readonly<Value>[] | undefined {
	if (value === undefined) return undefined;
	if (!Array.isArray(value)) throw new Error();
	return Object.freeze(
		Array.from(value, (item) => {
			if (!record(item)) throw new Error();
			return Object.freeze(normalize(item));
		}),
	);
}
function identity(value: unknown): value is string {
	return typeof value === "string" && value.length > 0 && !value.includes("\0");
}
function duration(value: unknown): value is number {
	return (
		typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER
	);
}
function endpoints(
	value: Record<string, unknown>,
): value is Record<string, unknown> & Pick<SerializedTimelineNode, "startedAt" | "completedAt"> {
	return duration(value.startedAt) && duration(value.completedAt) && value.completedAt >= value.startedAt;
}
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
interface TimelineEvidence {
	inputs?: readonly TimelineDependency[];
	spans?: readonly TimelineInterval[];
	owner?: TimelineInterval;
	producer?: ComputationProducer;
	id?: string;
	clock?: string;
	timeOrigin?: number;
	priorMs?: number;
	groups?: ReadonlySet<string>;
	published?: true;
	incomplete?: true;
}
const timelineEvidence = new WeakMap<object, TimelineEvidence>();
function evidence(object: object): TimelineEvidence {
	let value = timelineEvidence.get(object);
	if (!value) timelineEvidence.set(object, (value = {}));
	return value;
}
type Receipt = Pick<TimelineDependency, "owned" | "reused" | "overhead" | "computeUncertain">;
const collecting = new AsyncLocalStorage<{
	inputs?: Map<TimelineInterval, Receipt>;
	producer?: ComputationProducer;
}>();
const calculating = new AsyncLocalStorage<CalculationClock | undefined>();
const identityNamespace = randomUUID();
let nextIdentity = 0;
function computationCollection(producer = collecting.getStore()?.producer) {
	const scope = {
		inputs: new Map<TimelineInterval, Receipt>() as Map<TimelineInterval, Receipt> | undefined,
		producer: producer && Object.freeze({ ...producer }),
	};
	return {
		scope,
		complete: <Value>(output: Value) => ({
			output,
			dependencies: Object.freeze(
				[...scope.inputs!].map(([computation, receipt]) => ({
					computation,
					...receipt,
					shared: [computation],
				})),
			),
		}),
		close: () => {
			scope.inputs = undefined;
		},
	};
}
export type ComputationProducer = NonNullable<SerializedTimelineNode["producer"]>;
export interface ModeComputationTiming {
	readonly source: string;
	readonly mode: string;
	readonly hiddenComputeMs: number;
}
/** Immutable calculation evidence; carries no Actor, candidate or output ownership. */
export class TimelineInterval {
	readonly startedAt: number;
	readonly completedAt: number;
	constructor(
		startedAt: number,
		completedAt: number,
		inputs: readonly TimelineDependency[] = [],
		spans?: readonly TimelineInterval[],
		producer: ComputationProducer | null = collecting.getStore()?.producer ?? null,
	) {
		this.startedAt = metric(startedAt);
		this.completedAt = Math.max(this.startedAt, metric(completedAt));
		const state = evidence(this);
		if (producer) state.producer = Object.freeze({ ...producer });
		if (spans) state.spans = Object.freeze(spans.map(TimelineInterval.from));
		if (inputs.length)
			state.inputs = Object.freeze(
				inputs.map((input) =>
					Object.freeze({
						computation: TimelineInterval.from(input.computation),
						owned: input.owned,
						reused: input.reused,
						overhead: input.overhead,
						computeUncertain: input.computeUncertain,
						shared: Object.freeze(
							(input.shared ?? (input.overhead ? [input.computation] : [])).map(TimelineInterval.from),
						),
					}),
				),
			);
		for (const input of state.inputs ?? []) if (input.owned) evidence(input.computation).owner = this;
		Object.freeze(this);
	}
	static from(interval: TimelineInterval): TimelineInterval {
		const { startedAt, completedAt } = interval;
		return interval instanceof TimelineInterval
			? interval
			: new TimelineInterval(startedAt, completedAt, [], undefined, null);
	}
	static get producer(): ComputationProducer | undefined {
		return collecting.getStore()?.producer;
	}
	static producerID(owner: object, key: string): string {
		return `${computationIdentity(owner)}:${key}`;
	}
	/** Successful consumption identifies physical producers before or after their enclosing tools finish. */
	static reusedProducers(roots: readonly TimelineDependency[]): ReadonlySet<string> {
		return new Set(
			[...consumedComputations(roots).selected.values()].flatMap(({ flags, producer }) =>
				flags & 2 && producer?.id ? [producer.id] : [],
			),
		);
	}
	/** Record provenance before publication; failed evaluations never transfer borrowed work to the caller. */
	static async collect<T>(execute: () => T | Promise<T>, producer = collecting.getStore()?.producer) {
		const collection = computationCollection(producer);
		try {
			return collection.complete(await collecting.run(collection.scope, execute));
		} finally {
			collection.close();
		}
	}
	/** Synchronous language operations use the same producer graph and active calculation clock. */
	static measureSync<T>(execute: () => T) {
		const collection = computationCollection();
		const startedAt = performance.now();
		const clock = new CalculationClock(startedAt, calculating.getStore());
		try {
			const evaluation = collection.complete(
				calculating.run(clock, () => collecting.run(collection.scope, execute)),
			);
			return {
				...evaluation,
				computation: new TimelineInterval(
					startedAt,
					clock.finish(),
					evaluation.dependencies,
					clock.spans,
					collection.scope.producer,
				),
			};
		} finally {
			clock.finish();
			collection.close();
		}
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
		const copies = new Map<TimelineInterval, TimelineInterval>();
		for (const current of dependencyOrder([computation], (node) =>
			(evidence(node).inputs ?? [])
				.filter((input) => !input.reused)
				.map((input) => input.computation)
				.reverse(),
		)!) {
			const { inputs, spans, priorMs, groups } = evidence(current);
			const result = new TimelineInterval(
				current.startedAt,
				current.completedAt,
				(inputs ?? []).map((input) =>
					input.reused
						? {
								computation: input.computation,
								overhead: true,
								shared: input.shared,
							}
						: { ...input, computation: copies.get(input.computation)! },
				),
				spans,
				computationProducer(current) ?? null,
			);
			Object.assign(evidence(result), {
				id: computationIdentity(current),
				clock: computationClock(current),
				timeOrigin: computationTimeOrigin(current),
				priorMs,
				groups,
			});
			copies.set(current, result);
		}
		return copies.get(computation)!;
	}
	static exclude(computation: TimelineInterval, computeUncertain = false): void {
		collecting.getStore()?.inputs?.set(computation, {
			overhead: true,
			...(computeUncertain ? { computeUncertain: true } : {}),
		});
	}
	/** Record only active calculation segments; control work never produces a duration or a cost sample. */
	static async measure<T>(
		execute: () => T | Promise<T>,
		inputs: (output: T) => readonly TimelineDependency[] = () => [],
		producer = collecting.getStore()?.producer,
	) {
		producer = producer && Object.freeze({ ...producer });
		const startedAt = performance.now();
		const clock = new CalculationClock(startedAt, calculating.getStore());
		try {
			const evaluation = await calculating.run(clock, () => TimelineInterval.collect(execute, producer));
			const completedAt = clock.finish();
			let computation: TimelineInterval;
			try {
				computation = new TimelineInterval(
					startedAt,
					completedAt,
					[...evaluation.dependencies, ...inputs(evaluation.output)],
					clock.spans,
					producer,
				);
			} catch {
				computation = new TimelineInterval(startedAt, completedAt, [], clock.spans, producer);
			} // Evidence cannot replace the execution outcome.
			return { ...evaluation, computation };
		} finally {
			clock.finish();
		}
	}
	/** Cross a calculation boundary without timing adoption-time validation, delivery or cleanup. */
	static async outside<T>(execute: () => T | Promise<T>): Promise<T> {
		const clock = calculating.getStore();
		if (!clock) return execute();
		for (let current: CalculationClock | undefined = clock; current; current = current.parent)
			current.pause();
		try {
			return await calculating.run(undefined, execute);
		} finally {
			for (let current: CalculationClock | undefined = clock; current; current = current.parent)
				current.resume();
		}
	}
	static outsideSync<T>(execute: () => T): T {
		const clock = calculating.getStore();
		for (let current = clock; current; current = current.parent) current.pause();
		try {
			return calculating.run(undefined, execute);
		} finally {
			for (let current = clock; current; current = current.parent) current.resume();
		}
	}
	/** Freeze the active calculation up to an observed native execution boundary. */
	static current(
		startedAt: number,
		completedAt: number,
		inputs: readonly TimelineDependency[] = [],
	): TimelineInterval {
		return new TimelineInterval(
			startedAt,
			completedAt,
			inputs,
			calculating.getStore()?.read(startedAt, completedAt),
		);
	}
	/** Diagnostic provenance belongs to the physical producer; later consumers cannot replace it. */
	static producedBy(computation: TimelineInterval, producer: ComputationProducer): void {
		const pending = [{ computation, producer }];
		while (pending.length) {
			const { computation: current, producer } = pending.pop()!;
			const state = evidence(current);
			if (state.producer) continue;
			state.producer = computationProducer(current) ?? Object.freeze({ ...producer });
			for (const input of state.inputs ?? [])
				if (input.owned)
					pending.push({
						computation: input.computation,
						producer: state.producer,
					});
		}
	}
	/** A production can establish parallelism before its enclosing interval exists. */
	static group(computation: TimelineInterval, owner: object): void {
		const groups = new Set(evidence(computation).groups);
		groups.add(computationIdentity(owner));
		evidence(computation).groups = groups;
	}
	/** Missing historical evidence contributes no invented duration and remains visibly incomplete. */
	static unknownReuse(id: string): TimelineInterval {
		const computation = TimelineInterval.retained(id, 0);
		evidence(computation).incomplete = true;
		return computation;
	}
	/** A shared overlap needs only its time range, not another copy of its dependency graph. */
	static span(computation: TimelineInterval) {
		return {
			startedAt: computation.startedAt,
			completedAt: computation.completedAt,
			timeOrigin: computationTimeOrigin(computation),
		};
	}
	/** Persist the complete immutable evidence used by recordCall, regardless of graph size. */
	static serialize(computation: TimelineInterval): SerializedTimelineComputation | undefined {
		const selected = new Map<
			string,
			{
				computation: TimelineInterval;
				groups: Set<string>;
				producer?: ComputationProducer | null;
			}
		>();
		const visited = new Set<TimelineInterval>();
		const pending = [computation];
		while (pending.length) {
			const current = pending.pop()!;
			if (visited.has(current)) continue;
			visited.add(current);
			const id = computationIdentity(current);
			const prior = selected.get(id);
			const chosen = preferredComputation(prior?.computation, current);
			const groups = prior?.groups ?? new Set<string>();
			for (const group of computationGroups(current, true)) groups.add(group);
			const producer = computationProducer(current);
			selected.set(id, {
				computation: chosen,
				groups,
				producer: mergeProducers(prior?.producer, producer),
			});
			evidence(current).published = true;
			const inputs = evidence(current).inputs ?? [];
			for (let index = inputs.length - 1; index >= 0; index--) pending.push(inputs[index]!.computation);
		}
		const reachable = new Set<string>();
		const remaining = [computationIdentity(computation)];
		const nodes: unknown[] = [];
		while (remaining.length) {
			const id = remaining.pop()!;
			if (reachable.has(id)) continue;
			reachable.add(id);
			const { computation: current, groups, producer } = selected.get(id)!;
			const { priorMs, spans, incomplete, inputs } = evidence(current);
			for (const input of inputs ?? []) remaining.push(computationIdentity(input.computation));
			nodes.push({
				id,
				clock: computationClock(current),
				startedAt: current.startedAt,
				completedAt: current.completedAt,
				timeOrigin: computationTimeOrigin(current),
				spans,
				priorMs: priorMs || undefined,
				producer: producer ?? undefined,
				groups: [...groups],
				incomplete,
				inputs: (inputs ?? []).map((input) => ({
					id: computationIdentity(input.computation),
					owned: !!input.owned && computationIdentity(evidence(input.computation).owner ?? current) === id,
					reused: !!input.reused,
					overhead: !!input.overhead,
					computeUncertain: !!input.computeUncertain,
					shared: (input.shared ?? []).map((part) => ({
						clock: computationClock(part),
						startedAt: part.startedAt,
						completedAt: part.completedAt,
					})),
				})),
			});
		}
		return normalizeTimelineComputation({
			root: computationIdentity(computation),
			nodes,
		});
	}
	/** Each restore is independent; stable identities deduplicate only within the receiving call. */
	static restore(value: unknown): TimelineInterval | undefined {
		const graph = normalizeTimelineComputation(value);
		if (!graph) return undefined;
		const restored = new Map<string, TimelineInterval>();
		for (const node of graph.nodes) {
			const current = new TimelineInterval(
				node.startedAt,
				node.completedAt,
				(node.inputs ?? []).map((input) => ({
					computation: restored.get(input.id)!,
					owned: input.owned,
					reused: input.reused,
					overhead: input.overhead,
					computeUncertain: input.computeUncertain,
					shared: input.shared?.map((part) => {
						const span = new TimelineInterval(part.startedAt, part.completedAt);
						evidence(span).clock = part.clock;
						return span;
					}),
				})),
				node.spans,
				node.producer ?? null,
			);
			Object.assign(evidence(current), {
				id: node.id,
				published: true,
				clock: node.clock,
				timeOrigin: node.timeOrigin,
				priorMs: node.priorMs ?? 0,
				groups: node.groups && new Set(node.groups),
				incomplete: node.incomplete,
			});
			restored.set(node.id, current);
		}
		return restored.get(graph.root);
	}
	/** Retained history contributes its measured duration even without a same-process clock. */
	static retained(id: string, durationMs: number, original?: TimelineInterval): TimelineInterval {
		const computation = original ?? new TimelineInterval(0, 0, [], undefined, null);
		const state = evidence(computation);
		state.id = state.published ? computationIdentity(computation) : id;
		state.priorMs = original ? (state.priorMs ?? 0) : metric(durationMs);
		return computation;
	}
}
export interface ToolComputationTiming {
	readonly subcomputationHits?: number;
	readonly reusedComputeMs?: number;
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
	constructor(startedAt: number, parent?: CalculationClock) {
		this.startedAt = startedAt;
		this.parent = parent;
	}
	read(startedAt: number, completedAt: number): readonly TimelineInterval[] {
		return [
			...this.spans,
			...(!this.paused && this.completedAt === undefined
				? [new TimelineInterval(this.startedAt, completedAt)]
				: []),
		]
			.filter((span) => span.completedAt > startedAt && span.startedAt < completedAt)
			.map(
				(span) =>
					new TimelineInterval(Math.max(startedAt, span.startedAt), Math.min(completedAt, span.completedAt)),
			);
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
export function toolSpeedup({ toolComputeMs, hiddenComputeMs }: ToolComputationTiming): number | null {
	if (toolComputeMs === undefined || !Number.isFinite(toolComputeMs) || hiddenComputeMs < 0) return null;
	if (toolComputeMs === 0) return 1;
	return toolComputeMs / Math.max(0, toolComputeMs - hiddenComputeMs);
}
export class TaskTimeline {
	private readonly toolWaits: {
		startedAt: number;
		completedAt: number;
	}[] = [];
	private adoptionWaits: TimelineInterval[] | undefined = [];
	private toolComputeMs: number | undefined = 0;
	private hiddenComputeMs = 0;
	private hiddenComputeIncomplete = false;
	readonly startedAt: number;
	constructor(startedAt: number) {
		this.startedAt = metric(startedAt);
	}
	/** Raw latency for diagnosis only; it is never the computation-ratio denominator. */
	startToolWait(startedAt: number): (completedAt: number) => void {
		const wait = { startedAt, completedAt: Number.MAX_VALUE };
		this.toolWaits.push(wait);
		return (completedAt) => {
			wait.completedAt = Math.min(wait.completedAt, metric(completedAt));
		};
	}
	recordTool(computation: TimelineInterval, issuedAt: number, reused = false): ToolComputationTiming {
		return this.recordCall([{ computation, reused }], issuedAt);
	}
	/** Exactly one settled Actor call. Shared work counts once here; an independent later reuse counts again. */
	recordCall(
		roots: readonly TimelineDependency[],
		issuedAt: number,
		completedAt?: number,
	): ToolComputationTiming {
		let { selected, groups, complete, incomplete } = consumedComputations(roots);
		let hiddenComputeIncomplete = incomplete || !Number.isFinite(issuedAt);
		let adoptionComplete =
			Number.isFinite(issuedAt) &&
			completedAt !== undefined &&
			Number.isFinite(completedAt) &&
			completedAt >= issuedAt;
		const observed: TimelineInterval[] = [];
		const grouped = new Map<string, TimelineInterval[]>();
		const attributed = new Map<
			string,
			{
				interval: TimelineInterval;
				producer?: ComputationProducer;
			}[]
		>();
		const modes = new Map<string, ModeComputationTiming>();
		const credit = (producer: ComputationProducer | null | undefined, duration: number): void => {
			if (!producer?.mode || duration <= 0) return;
			const key = producerKey(producer);
			modes.set(key, {
				source: producer.source,
				mode: producer.mode,
				hiddenComputeMs: (modes.get(key)?.hiddenComputeMs ?? 0) + duration,
			});
		};
		const totals = [0, 0];
		let reused = false;
		let subcomputationHits = 0;
		for (const { computation, flags, producer } of selected.values()) {
			const { incomplete, priorMs = 0, inputs, spans } = evidence(computation);
			if (incomplete) {
				complete = false;
				if (flags & 2) hiddenComputeIncomplete = true;
			}
			totals[0]! += priorMs;
			const shared = (inputs ?? [])
				.flatMap((input) =>
					(input.shared ?? []).flatMap((part) => {
						if (computationClock(part) !== computationClock(computation)) return [];
						const sameClock = computationClock(part) === computationClock(input.computation);
						return [
							{
								startedAt: sameClock ? Math.max(input.computation.startedAt, part.startedAt) : part.startedAt,
								completedAt: sameClock
									? Math.min(input.computation.completedAt, part.completedAt)
									: part.completedAt,
							},
						];
					}),
				)
				.filter((part) => part.completedAt > part.startedAt);
			const all = (spans ?? [computation]).flatMap((span) => exclusiveIntervals(span, shared));
			const owner = JSON.stringify([
				groups.owner(computationIdentity(computation)),
				computationClock(computation),
			]);
			const origin = computationTimeOrigin(computation);
			const cutoff =
				computationClock(computation) === identityNamespace
					? issuedAt
					: origin === undefined
						? NaN
						: issuedAt + (performance.timeOrigin - origin);
			if (priorMs > 0 || (all.length > 0 && !Number.isFinite(cutoff))) adoptionComplete = false;
			if (adoptionComplete)
				for (const span of all)
					observed.push({
						startedAt: span.startedAt + issuedAt - cutoff,
						completedAt: span.completedAt + issuedAt - cutoff,
					});
			const hidden =
				flags & 2
					? Number.isFinite(cutoff)
						? all
								.filter((span) => span.startedAt < cutoff)
								.map((span) => ({
									startedAt: span.startedAt,
									completedAt: Math.min(span.completedAt, cutoff),
								}))
						: all
					: [];
			const group = grouped.get(owner) ?? [];
			for (const interval of all) group.push(interval);
			grouped.set(owner, group);
			if (flags & 2) {
				reused = true;
				subcomputationHits++;
				totals[1]! += priorMs;
				credit(producer, priorMs);
				const group = attributed.get(owner) ?? [];
				for (const interval of hidden)
					group.push({
						interval,
						producer: producer?.mode ? producer : undefined,
					});
				attributed.set(owner, group);
			}
		}
		for (const all of grouped.values()) totals[0]! += unionDuration(all);
		for (const spans of attributed.values()) totals[1]! += attributeUnion(spans, credit, producerKey);
		const hiddenByMode = Object.freeze([...modes.values()].map((value) => Object.freeze(value)));
		const adoption =
			complete && adoptionComplete
				? exclusiveIntervals(new TimelineInterval(issuedAt, completedAt!), observed)
				: undefined;
		if (adoption) {
			for (const interval of adoption) this.adoptionWaits?.push(interval);
		} else this.adoptionWaits = undefined;
		const timing = Object.freeze({
			toolComputeMs: totals[0]!,
			hiddenComputeMs: totals[1]!,
			subcomputationHits,
			reusedComputeMs: totals[1]!,
			...(adoption ? { adoptionWaitMs: unionDuration(adoption) } : {}),
			...(reused ? { reused: true as const } : {}),
			...(hiddenComputeIncomplete ? { hiddenComputeIncomplete: true as const } : {}),
			...(hiddenByMode.length ? { hiddenByMode } : {}),
		});
		this.toolComputeMs =
			this.toolComputeMs !== undefined && timing.toolComputeMs !== undefined
				? this.toolComputeMs + timing.toolComputeMs
				: undefined;
		this.hiddenComputeMs += timing.hiddenComputeMs;
		this.hiddenComputeIncomplete ||= hiddenComputeIncomplete;
		return timing;
	}
	measure(endedAt: number) {
		const startedAt = this.startedAt;
		const completedAt = Math.max(startedAt, metric(endedAt));
		const duration = (spans: readonly TimelineInterval[]) =>
			unionDuration(
				spans
					.map((span) => ({
						startedAt: Math.max(startedAt, metric(span.startedAt)),
						completedAt: Math.min(completedAt, metric(span.completedAt)),
					}))
					.filter((span) => span.completedAt > span.startedAt),
			);
		return Object.freeze({
			startedAt,
			completedAt,
			toolComputeMs: this.toolComputeMs,
			hiddenComputeMs: this.hiddenComputeMs,
			...(this.hiddenComputeIncomplete ? { hiddenComputeIncomplete: true as const } : {}),
			...(this.adoptionWaits ? { adoptionWaitMs: duration(this.adoptionWaits) } : {}),
			toolWaitMs: duration(this.toolWaits),
		});
	}
}
function consumedComputations(roots: readonly TimelineDependency[]) {
	const selected = new Map<
		string,
		{
			computation: TimelineInterval;
			flags: number;
			producer?: ComputationProducer | null;
		}
	>();
	const visited = new Map<TimelineInterval, number>();
	const groups = new ComputationGroups();
	let uncertainty: "none" | "local" | "reused" = "none";
	const pending = roots.map((input) => ({ input, inheritedReuse: false })).reverse();
	while (pending.length) {
		const { input, inheritedReuse } = pending.pop()!;
		if (input.computeUncertain) {
			if (inheritedReuse || input.reused) uncertainty = "reused";
			else if (uncertainty === "none") uncertainty = "local";
		}
		if (input.overhead) continue;
		const computation = input.computation;
		const reused = inheritedReuse || !!input.reused;
		const flag = reused ? 2 : 1;
		if ((visited.get(computation) ?? 0) & flag) continue;
		visited.set(computation, (visited.get(computation) ?? 0) | flag);
		const key = computationIdentity(computation);
		const previous = selected.get(key);
		for (const group of computationGroups(computation)) groups.join(key, group);
		selected.set(key, {
			computation: preferredComputation(previous?.computation, computation),
			flags: (previous?.flags ?? 0) | flag,
			producer: mergeProducers(previous?.producer, computationProducer(computation)),
		});
		const inputs = evidence(computation).inputs ?? [];
		for (let index = inputs.length - 1; index >= 0; index--)
			pending.push({ input: inputs[index]!, inheritedReuse: reused });
	}
	return { selected, groups, complete: uncertainty === "none", incomplete: uncertainty === "reused" };
}
function computationIdentity(object: object): string {
	return (evidence(object).id ||= `timeline:${identityNamespace}:${++nextIdentity}`);
}
function computationTimeOrigin(computation: TimelineInterval): number | undefined {
	return computationClock(computation) === identityNamespace
		? performance.timeOrigin
		: evidence(computation).timeOrigin;
}
function computationClock(computation: TimelineInterval): string {
	return evidence(computation).clock ?? identityNamespace;
}
function preferredComputation(
	previous: TimelineInterval | undefined,
	current: TimelineInterval,
): TimelineInterval {
	if (!previous || (evidence(previous).incomplete && !evidence(current).incomplete)) return current;
	if (evidence(current).incomplete && !evidence(previous).incomplete) return previous;
	return previous.completedAt <= previous.startedAt && current.completedAt > current.startedAt
		? current
		: previous;
}
/** Follow ownership only to obtain aliases; ancestors are never selected as consumed work. */
function computationGroups(computation: TimelineInterval, publish = false): ReadonlySet<string> {
	const groups = new Set<string>();
	const visited = new Set<TimelineInterval>();
	let current: TimelineInterval | undefined = computation;
	while (current && !visited.has(current)) {
		visited.add(current);
		const state = evidence(current);
		if (publish) state.published = true;
		for (const group of state.groups ?? []) groups.add(group);
		const owner: TimelineInterval | undefined = state.owner;
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
		while (this.parents.has(id)) {
			const parent = this.parents.get(id)!;
			this.parents.set(id, owner);
			id = parent;
		}
		return owner;
	}
	join(left: string, right: string): void {
		const a = this.owner(left);
		const b = this.owner(right);
		if (a !== b) this.parents.set(a, b);
	}
}
function producerKey(producer: ComputationProducer): string {
	return JSON.stringify([producer.source, producer.mode]);
}
function mergeProducers(
	previous: ComputationProducer | null | undefined,
	current: ComputationProducer | undefined,
): ComputationProducer | null | undefined {
	if (
		previous === null ||
		(previous &&
			current &&
			(["source", "mode", "id"] as const).some(
				(key) => previous[key] !== undefined && current[key] !== undefined && previous[key] !== current[key],
			))
	)
		return null;
	return previous && current
		? {
				...previous,
				...current,
				mode: previous.mode ?? current.mode,
				id: previous.id ?? current.id,
			}
		: (previous ?? current);
}
/** Ownership passes production provenance to children; borrowed dependencies have their own owner chain. */
function computationProducer(computation: TimelineInterval): ComputationProducer | undefined {
	let current: TimelineInterval | undefined = computation;
	while (current) {
		const producer = evidence(current).producer;
		if (producer) return producer;
		current = evidence(current).owner;
	}
	return undefined;
}
/** The same owner union as H; an overlapping segment with conflicting or missing provenance stays unassigned. */
function attributeUnion<T>(
	spans: readonly {
		interval: TimelineInterval;
		producer?: T;
	}[],
	credit: (producer: T, duration: number) => void,
	keyOf: (producer: T) => unknown,
): number {
	const points = spans
		.flatMap(({ interval, producer }) => [
			{ at: interval.startedAt, delta: 1, producer },
			{ at: interval.completedAt, delta: -1, producer },
		])
		.sort((left, right) => left.at - right.at);
	const active = new Map<
		unknown,
		{
			producer: T;
			count: number;
		}
	>();
	let previous = 0;
	let unknown = 0;
	let total = 0;
	for (const point of points) {
		if (point.at > previous && (unknown > 0 || active.size)) total += point.at - previous;
		if (point.at > previous && unknown === 0 && active.size === 1)
			credit(active.values().next().value!.producer, point.at - previous);
		previous = point.at;
		if (!point.producer) {
			unknown += point.delta;
			continue;
		}
		const key = keyOf(point.producer);
		const count = (active.get(key)?.count ?? 0) + point.delta;
		if (count) active.set(key, { producer: point.producer, count });
		else active.delete(key);
	}
	return total;
}
function exclusiveIntervals(
	interval: TimelineInterval,
	shared: readonly TimelineInterval[],
): TimelineInterval[] {
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
	let total = 0;
	let end = 0;
	for (const interval of [...intervals].sort((left, right) => left.startedAt - right.startedAt)) {
		total += Math.max(0, interval.completedAt - Math.max(end, interval.startedAt));
		end = Math.max(end, interval.completedAt);
	}
	return total;
}
export type SubcomputeCounts = { hits: number; misses: number; reusedMs: number };
export type SubcomputeMetrics = Readonly<Record<string, Readonly<SubcomputeCounts>>>;
export function summarizeSubcomputations(groups: Iterable<SubcomputeMetrics | undefined>) {
	const totals = new Map<string, SubcomputeCounts>();
	for (const group of groups)
		for (const [name, delta] of Object.entries(group ?? {})) {
			const counts = totals.get(name) ?? { hits: 0, misses: 0, reusedMs: 0 };
			counts.hits += delta.hits;
			counts.misses += delta.misses;
			counts.reusedMs += delta.reusedMs;
			totals.set(name, counts);
		}
	return Object.fromEntries(Array.from(totals, ([name, counts]) => [
		name, { ...counts, hitRate: counts.hits / (counts.hits + counts.misses || 1) },
	] as const));
}
export const subcomputeTiming = new AsyncLocalStorage<(namespace: string, hit: boolean, receipt?: TimelineInterval) => void>();
type SubcomputationSummary = "actionHitRate" | "subcomputationHits" | "reusedComputeMs" | "subcomputations";
export type SpeculativeTraceSummary = Readonly<
	Omit<ReturnType<typeof emptySpeculativeTraceSummary>, SubcomputationSummary> &
		Partial<Pick<ReturnType<typeof emptySpeculativeTraceSummary>, SubcomputationSummary>>
>;
export interface ModeTraceSummary {
	readonly observed: number;
	readonly matched: number;
	readonly adopted: number;
	readonly started: number;
	/** Measured speculative production wall time, including failed and cancelled work. */
	readonly productionMs: number;
	/** Calculation hidden before Actor calls, attributed to the execution owner. */
	readonly hiddenComputeMs: number;
}
type ModesBySource = Readonly<Record<string, Readonly<Record<string, ModeTraceSummary>>>>;
const EMPTY_CACHE: SpeculativeCacheSnapshot = {
	cacheCapacity: 0,
	cacheByteCapacity: 0,
	cacheCold: 0,
	cacheHot: 0,
	inFlightJobs: 0,
	resultEntries: 0,
	resultBytes: 0,
	branchEntries: 0,
	branchBytes: 0,
	exclusiveCandidates: 0,
	sharedCandidates: 0,
};
export function emptySpeculativeTraceSummary(
	cache:
		| SpeculativeCacheSnapshot
		| Pick<SpeculativeCacheSnapshot, "cacheCapacity" | "cacheByteCapacity"> = EMPTY_CACHE,
) {
	return {
		sourceRequests: 0,
		sourceOutcomes: {} as Readonly<Record<string, number>>,
		lastSourceFailure: undefined as string | undefined,
		predictionsSettled: 0,
		predictionsObserved: 0,
		predictionsMatched: 0,
		predictionsAdopted: 0,
		predictionPrecision: 0,
		adoptionYield: 0,
		predictionsBySource: {} as Readonly<Record<string, readonly number[]>>, // [observed, matched, adopted]
		modesBySource: {} as ModesBySource,
		predictionUnobserved: {} as Readonly<Record<string, number>>,
		predictionRejectedAfterMatch: {} as Readonly<Record<string, number>>,
		operationPredictionsSettled: 0,
		operationPredictionsAdopted: 0,
		candidateStarted: 0,
		candidateSucceeded: 0,
		candidateFailed: 0,
		candidateCancelled: 0,
		candidateTerminalCauses: {} as Readonly<Record<string, number>>,
		actorActions: 0,
		actorActionsByTool: {} as Readonly<Record<string, readonly number[]>>, // [actions, reused]
		speculativeHits: 0,
		exactReuseHits: 0, // Adopted identical K(a) results.
		inputReuseHits: 0, // Current tool semantics evaluated over another execution's sealed inputs.
		actorPreviews: 0,
		actorFallbacks: 0,
		hitRate: 0,
		actionHitRate: 0,
		subcomputationHits: 0,
		subcomputations: {} as ReturnType<typeof summarizeSubcomputations>,
		reusedComputeMs: 0,
		actorCandidateRejections: {} as Readonly<Record<string, number>>,
		tasks: 0,
		toolComputeMs: 0 as number | undefined,
		toolWaitMs: 0,
		hiddenComputeMs: 0,
		adoptionWaitMs: 0 as number | undefined,
		hiddenComputeIncomplete: undefined as true | undefined,
		totalDraftTokens: 0,
		processReuse: emptyWorldReuseMetrics(), // Inside speculative worlds, never the Actor route.
		cache: { ...EMPTY_CACHE, ...cache },
	};
}
/** The sole reducer used by both live UI state and persisted trace replay. */
export function reduceSpeculativeTrace<SessionID>(
	current: SpeculativeTraceSummary,
	event: SpeculativeActionEvent<SessionID>,
): SpeculativeTraceSummary {
	const next = {
		...current,
		actionHitRate: current.actionHitRate ?? current.hitRate,
		subcomputationHits: current.subcomputationHits ?? 0,
		reusedComputeMs: current.reusedComputeMs ?? 0,
		cache: { ...event.cache },
	};
	if (event.subcomputations)
		next.subcomputations = summarizeSubcomputations([current.subcomputations, event.subcomputations]);
	// Prediction support and execution ownership are different: shared work is charged to its producer only.
	if (
		(event.type === "prediction" || event.type === "operation_prediction") &&
		event.mode &&
		event.settlement.observation === "observed"
	) {
		const match = event.settlement.match;
		next.modesBySource = addMode(current.modesBySource, event.settlement.prediction.source, event.mode, {
			observed: 1,
			matched: Number(match.matched),
			adopted: Number(match.matched && match.adoption.status === "adopted"),
		});
	} else if (event.type === "candidate" && event.candidate.mode && event.candidate.origin === "prediction") {
		next.modesBySource = addMode(
			current.modesBySource,
			event.candidate.source,
			event.candidate.mode,
			event.state.status === "running" ? { started: 1 } : { productionMs: metric(event.state.executionMs) },
		);
	} else if (event.type === "actor_action") {
		for (const timing of event.computation?.hiddenByMode ?? [])
			next.modesBySource = addMode(next.modesBySource, timing.source, timing.mode, {
				hiddenComputeMs: metric(timing.hiddenComputeMs),
			});
	}
	switch (event.type) {
		case "operation_prediction":
			next.operationPredictionsSettled++;
			if (
				event.settlement.observation === "observed" &&
				event.settlement.match.matched &&
				event.settlement.match.adoption.status === "adopted"
			)
				next.operationPredictionsAdopted++;
			break;
		case "task":
			next.tasks++;
			for (const key of ["toolComputeMs", "adoptionWaitMs"] as const)
				next[key] =
					current[key] !== undefined && event.timing[key] !== undefined
						? current[key] + metric(event.timing[key])
						: undefined;
			next.toolWaitMs += metric(event.timing.toolWaitMs);
			next.hiddenComputeMs += metric(event.timing.hiddenComputeMs);
			next.hiddenComputeIncomplete = current.hiddenComputeIncomplete || event.timing.hiddenComputeIncomplete;
			break;
		case "source_request":
			next.sourceRequests++;
			next.totalDraftTokens = Math.max(next.totalDraftTokens, metric(event.totalDraftTokens));
			next.sourceOutcomes = increment(current.sourceOutcomes, [event.request.settlement.status]);
			if (event.request.settlement.status === "error" || event.request.settlement.status === "timeout") {
				const { cause } = event.request.settlement;
				next.lastSourceFailure = `${event.request.request.source} ${causeKey(cause)}${cause.detail ? ` ${cause.detail.slice(0, speculativeConfig().runtime.sourceFailureCharacters)}` : ""}`;
			}
			break;
		case "prediction": {
			next.predictionsSettled++;
			const settlement = event.settlement;
			if (settlement.observation === "unobserved") {
				next.predictionUnobserved = increment(current.predictionUnobserved, [causeKey(settlement.cause)]);
				break;
			}
			next.predictionsObserved++;
			next.predictionsBySource = tally(
				current.predictionsBySource,
				settlement.prediction.source,
				true,
				settlement.match.matched,
				settlement.match.matched && settlement.match.adoption.status === "adopted",
			);
			if (!settlement.match.matched) break;
			next.predictionsMatched++;
			if (settlement.match.adoption.status === "adopted") next.predictionsAdopted++;
			else
				next.predictionRejectedAfterMatch = increment(current.predictionRejectedAfterMatch, [
					causeKey(settlement.match.adoption.cause),
				]);
			break;
		}
		case "candidate":
			next[candidateCounters[event.state.status]]++;
			if (event.state.status === "succeeded") {
				const reuse = event.candidate.world?.executionMetrics.reuse;
				if (reuse) next.processReuse = addReuseMetrics(next.processReuse, reuse);
			} else if (event.state.status !== "running")
				next.candidateTerminalCauses = increment(current.candidateTerminalCauses, [
					causeKey(event.state.cause),
				]);
			break;
		case "actor_action":
			next.actorActions++;
			next.subcomputationHits += metric(event.computation?.subcomputationHits ?? 0);
			next.reusedComputeMs += metric(
				event.computation?.reusedComputeMs ?? event.computation?.hiddenComputeMs ?? 0,
			);
			next.actorActionsByTool = tally(
				current.actorActionsByTool,
				event.settlement.tool,
				true,
				event.settlement.provider.kind === "speculative",
			);
			if (event.settlement.rejections.length)
				next.actorCandidateRejections = increment(
					current.actorCandidateRejections,
					event.settlement.rejections.map((rejection) => causeKey(rejection.cause)),
				);
			if (event.settlement.provider.kind === "speculative") {
				next.speculativeHits++;
				const match = event.settlement.provider.match;
				if (match.kind === "inputs") next.inputReuseHits++;
				else next.exactReuseHits++;
			} else next[event.settlement.provider.origin === "preview" ? "actorPreviews" : "actorFallbacks"]++;
			break;
	}
	next.hitRate = ratio(next.speculativeHits, next.actorActions);
	next.actionHitRate = next.hitRate;
	next.predictionPrecision = ratio(next.predictionsMatched, next.predictionsObserved);
	next.adoptionYield = ratio(next.predictionsAdopted, next.predictionsMatched);
	return next;
}
const candidateCounters = {
	running: "candidateStarted",
	succeeded: "candidateSucceeded",
	failed: "candidateFailed",
	cancelled: "candidateCancelled",
} as const;
export function summarizeSpeculativeTrace<SessionID>(
	events: ReadonlyArray<SpeculativeActionEvent<SessionID>>,
): SpeculativeTraceSummary {
	return events.reduce<SpeculativeTraceSummary>(reduceSpeculativeTrace, emptySpeculativeTraceSummary());
}
function addReuseMetrics(left: WorldReuseMetrics, right: WorldReuseMetrics): WorldReuseMetrics {
	const merged = { ...left } as unknown as Record<string, number | string | undefined>;
	for (const [key, value] of Object.entries(right)) {
		if (key === "lastError") merged[key] = value;
		else merged[key] = metric(Number(merged[key])) + metric(Number(value));
	}
	return merged as unknown as WorldReuseMetrics;
}
function causeKey(cause: ResolutionCause): string {
	return `${cause.stage}:${cause.code}`;
}
function ratio(numerator: number, denominator: number): number {
	return denominator > 0 ? numerator / denominator : 0;
}
/** Parallel counters per key: each flag increments its own position. */
function tally(
	target: Readonly<Record<string, readonly number[]>>,
	key: string,
	...flags: boolean[]
): Record<string, readonly number[]> {
	return {
		...target,
		[key]: flags.map((flag, index) => (target[key]?.[index] ?? 0) + Number(flag)),
	};
}
function addMode(
	target: ModesBySource,
	source: string,
	mode: string,
	delta: Partial<ModeTraceSummary>,
): ModesBySource {
	const modes = Object.hasOwn(target, source) ? target[source]! : {};
	const current = Object.hasOwn(modes, mode)
		? modes[mode]!
		: {
				observed: 0,
				matched: 0,
				adopted: 0,
				started: 0,
				productionMs: 0,
				hiddenComputeMs: 0,
			};
	const next = { ...current };
	for (const key of Object.keys(delta) as (keyof ModeTraceSummary)[]) next[key] += metric(delta[key]);
	return { ...target, [source]: { ...modes, [mode]: next } };
}
