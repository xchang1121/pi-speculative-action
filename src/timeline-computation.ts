import { dependencyOrder } from "./dependency-order.ts";
import { isRecord as record } from "./stable-json.ts";

/** Complete observational evidence, independent of replay authority and output identity. */
export interface SerializedTimelineComputation {
	readonly version: 1 | 2 | 3;
	readonly root: string;
	readonly nodes: readonly SerializedTimelineNode[];
}

export interface SerializedTimelineNode {
	readonly id: string;
	/** Monotonic coordinate namespace; endpoints from different clocks are never compared. */
	readonly clock: string;
	/** Unix epoch origin of this monotonic clock, used only to compare Actor issue time. */
	readonly timeOrigin?: number;
	readonly startedAt: number;
	readonly completedAt: number;
	/** Explicit calculation segments; absence preserves historical continuous computation. */
	readonly spans?: readonly { readonly startedAt: number; readonly completedAt: number }[];
	readonly priorMs?: number;
	readonly producer?: { readonly source: string; readonly mode?: string; readonly id?: string };
	/** Concurrency aliases only: these never select an ancestor or its other work. */
	readonly groups?: readonly string[];
	readonly incomplete?: true;
	readonly inputs?: readonly {
		readonly id: string;
		readonly owned?: true;
		readonly reused?: true;
		readonly overhead?: true;
		readonly computeUncertain?: true;
		readonly shared?: readonly { readonly clock: string; readonly startedAt: number; readonly completedAt: number }[];
	}[];
}

const INPUT_FLAGS = ["owned", "reused", "overhead", "computeUncertain"] as const;

/** Validate complete telemetry and order dependencies before consumers; invalid evidence does not invalidate replay. */
export function normalizeTimelineComputation(value: unknown): SerializedTimelineComputation | undefined {
	try {
		if (!record(value) || ![1, 2, 3].includes(value.version as number) || !identity(value.root) || !Array.isArray(value.nodes) ||
			!value.nodes.length) return undefined;
		const byID = new Map<string, SerializedTimelineNode>();
		const origins = new Map<string, number>();
		for (const raw of value.nodes) {
			if (!record(raw) || !identity(raw.id) || !identity(raw.clock) || byID.has(raw.id) || !endpoints(raw) ||
				raw.priorMs !== undefined && !duration(raw.priorMs) || raw.timeOrigin !== undefined && !duration(raw.timeOrigin) ||
				raw.incomplete !== undefined && typeof raw.incomplete !== "boolean") return undefined;
			if (raw.timeOrigin !== undefined) {
				if (origins.has(raw.clock) && origins.get(raw.clock) !== raw.timeOrigin) return undefined;
				origins.set(raw.clock, raw.timeOrigin as number);
			}
			let producer: SerializedTimelineNode["producer"];
			const calculation = records(raw.spans, part => {
				if (!endpoints(part) || part.startedAt < raw.startedAt || part.completedAt > raw.completedAt) throw new Error();
				return { startedAt: part.startedAt, completedAt: part.completedAt };
			});
			if (raw.producer !== undefined) {
				if (!record(raw.producer) || !identity(raw.producer.source) ||
					raw.producer.mode !== undefined && !identity(raw.producer.mode) || raw.producer.id !== undefined && !identity(raw.producer.id)) return undefined;
				producer = Object.freeze({ source: raw.producer.source, ...(raw.producer.mode !== undefined ? { mode: raw.producer.mode as string } : {}),
					...(raw.producer.id !== undefined ? { id: raw.producer.id as string } : {}) });
			}
			let aliases: readonly string[] | undefined;
			if (raw.groups !== undefined) {
				if (!Array.isArray(raw.groups)) return undefined;
				aliases = Object.freeze([...new Set<string>(raw.groups)]);
				if (!aliases.every(identity)) return undefined;
			}
			const inputs = records(raw.inputs, input => {
				if (!identity(input.id) || INPUT_FLAGS.some(key => input[key] !== undefined && typeof input[key] !== "boolean")) throw new Error();
				const shared = records(input.shared, span => {
					if (!identity(span.clock) || !endpoints(span)) throw new Error();
					return { clock: span.clock, startedAt: span.startedAt, completedAt: span.completedAt };
				});
				return { id: input.id, ...Object.fromEntries(INPUT_FLAGS.filter(key => input[key]).map(key => [key, true])),
					...(shared ? { shared } : {}) };
			});
			const node = Object.freeze({ id: raw.id, clock: raw.clock, startedAt: raw.startedAt, completedAt: raw.completedAt,
				...(raw.timeOrigin !== undefined ? { timeOrigin: raw.timeOrigin as number } : {}),
				...(calculation ? { spans: calculation } : {}),
				...(raw.priorMs !== undefined ? { priorMs: raw.priorMs as number } : {}), ...(producer ? { producer } : {}),
				...(aliases?.length ? { groups: aliases } : {}), ...(raw.incomplete ? { incomplete: true as const } : {}),
				...(inputs?.length ? { inputs } : {}) });
			byID.set(node.id, node);
		}
		if (!byID.has(value.root)) return undefined;
		const owned = new Map<string, string>();
		for (const node of byID.values()) for (const input of node.inputs ?? []) {
			if (!byID.has(input.id)) return undefined;
			if (input.owned) {
				if (owned.has(input.id) && owned.get(input.id) !== node.id) return undefined;
				owned.set(input.id, node.id);
			}
		}
		const ordered = dependencyOrder([byID.get(value.root)!], node => [...node.inputs ?? []].reverse().map(input => byID.get(input.id)));
		if (!ordered || ordered.length !== byID.size) return undefined;
		return Object.freeze({ version: value.version as 1 | 2 | 3, root: value.root, nodes: Object.freeze(ordered) });
	} catch { return undefined; }
}

function records<Value extends object>(value: unknown, normalize: (item: Record<string, unknown>) => Value): readonly Readonly<Value>[] | undefined {
	if (value === undefined) return undefined;
	if (!Array.isArray(value)) throw new Error();
	return Object.freeze(Array.from(value, item => {
		if (!record(item)) throw new Error();
		return Object.freeze(normalize(item));
	}));
}

function identity(value: unknown): value is string { return typeof value === "string" && value.length > 0 && !value.includes("\0"); }
function duration(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER; }
function endpoints(value: Record<string, unknown>): value is Record<string, unknown> & Pick<SerializedTimelineNode, "startedAt" | "completedAt"> {
	return duration(value.startedAt) && duration(value.completedAt) && value.completedAt >= value.startedAt;
}
