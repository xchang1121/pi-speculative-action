/** Bounded observational evidence, independent of replay authority and output identity. */
export interface SerializedTimelineComputation {
	readonly version: 1 | 2;
	readonly root: string;
	readonly nodes: readonly SerializedTimelineNode[];
}

export interface SerializedTimelineNode {
	readonly id: string;
	/** Monotonic coordinate namespace; endpoints from different clocks are never compared. */
	readonly clock: string;
	readonly startedAt: number;
	readonly completedAt: number;
	/** Explicit calculation segments; absence preserves historical continuous computation. */
	readonly spans?: readonly { readonly startedAt: number; readonly completedAt: number }[];
	readonly priorMs?: number;
	readonly producer?: { readonly source: string; readonly mode?: string };
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

/** Saturation drops the whole optional graph; it must never drop only an overhead cut. */
export const TIMELINE_COMPUTATION_LIMITS = Object.freeze({ nodes: 256, edges: 1024, spans: 2048, groups: 1024, bytes: 128 * 1024 });

/** Invalid telemetry is unavailable evidence, not an invalid execution certificate. */
export function normalizeTimelineComputation(value: unknown): SerializedTimelineComputation | undefined {
	try {
		if (!record(value) || value.version !== 1 && value.version !== 2 || !identity(value.root) || !Array.isArray(value.nodes) ||
			!value.nodes.length || value.nodes.length > TIMELINE_COMPUTATION_LIMITS.nodes) return undefined;
		const nodes: SerializedTimelineNode[] = [], byID = new Map<string, SerializedTimelineNode>();
		let edges = 0, spans = 0, groups = 0;
		for (const raw of value.nodes) {
			if (!record(raw) || !identity(raw.id) || !identity(raw.clock) || byID.has(raw.id) || !endpoints(raw) ||
				raw.priorMs !== undefined && !duration(raw.priorMs) || raw.incomplete !== undefined && typeof raw.incomplete !== "boolean") return undefined;
			let producer: SerializedTimelineNode["producer"];
			let calculation: SerializedTimelineNode["spans"];
			if (raw.spans !== undefined) {
				if (!Array.isArray(raw.spans) || (spans += raw.spans.length) > TIMELINE_COMPUTATION_LIMITS.spans) return undefined;
				const parts: NonNullable<SerializedTimelineNode["spans"]>[number][] = [];
				for (const part of raw.spans) {
					if (!record(part) || !endpoints(part) || (part.startedAt as number) < (raw.startedAt as number) ||
						(part.completedAt as number) > (raw.completedAt as number)) return undefined;
					parts.push(Object.freeze({ startedAt: part.startedAt as number, completedAt: part.completedAt as number }));
				}
				calculation = Object.freeze(parts);
			}
			if (raw.producer !== undefined) {
				if (!record(raw.producer) || !identity(raw.producer.source) || raw.producer.mode !== undefined && !identity(raw.producer.mode)) return undefined;
				producer = Object.freeze({ source: raw.producer.source, ...(raw.producer.mode !== undefined ? { mode: raw.producer.mode as string } : {}) });
			}
			let aliases: readonly string[] | undefined;
			if (raw.groups !== undefined) {
				if (!Array.isArray(raw.groups) || raw.groups.length > TIMELINE_COMPUTATION_LIMITS.groups ||
					!raw.groups.every(identity) || (groups += raw.groups.length) > TIMELINE_COMPUTATION_LIMITS.groups) return undefined;
				aliases = Object.freeze([...new Set<string>(raw.groups)]);
			}
			const inputs: NonNullable<SerializedTimelineNode["inputs"]>[number][] = [];
			if (raw.inputs !== undefined) {
				if (!Array.isArray(raw.inputs) || (edges += raw.inputs.length) > TIMELINE_COMPUTATION_LIMITS.edges) return undefined;
				for (const input of raw.inputs) {
					if (!record(input) || !identity(input.id) || ["owned", "reused", "overhead", "computeUncertain"].some(
						key => input[key] !== undefined && typeof input[key] !== "boolean")) return undefined;
					const shared: { readonly clock: string; readonly startedAt: number; readonly completedAt: number }[] = [];
					if (input.shared !== undefined) {
						if (!Array.isArray(input.shared) || (spans += input.shared.length) > TIMELINE_COMPUTATION_LIMITS.spans) return undefined;
						for (const span of input.shared) {
							if (!record(span) || !identity(span.clock) || !endpoints(span)) return undefined;
							shared.push(Object.freeze({ clock: span.clock, startedAt: span.startedAt as number, completedAt: span.completedAt as number }));
						}
					}
					inputs.push(Object.freeze({ id: input.id, ...(input.owned ? { owned: true as const } : {}),
						...(input.reused ? { reused: true as const } : {}), ...(input.overhead ? { overhead: true as const } : {}),
						...(input.computeUncertain ? { computeUncertain: true as const } : {}),
						...(input.shared !== undefined ? { shared: Object.freeze(shared) } : {}) }));
				}
			}
			const node = Object.freeze({ id: raw.id, clock: raw.clock, startedAt: raw.startedAt as number, completedAt: raw.completedAt as number,
				...(calculation ? { spans: calculation } : {}),
				...(raw.priorMs !== undefined ? { priorMs: raw.priorMs as number } : {}), ...(producer ? { producer } : {}),
				...(aliases?.length ? { groups: aliases } : {}), ...(raw.incomplete ? { incomplete: true as const } : {}),
				...(inputs.length ? { inputs: Object.freeze(inputs) } : {}) });
			nodes.push(node); byID.set(node.id, node);
		}
		if (!byID.has(value.root)) return undefined;
		const owned = new Map<string, string>();
		for (const node of nodes) for (const input of node.inputs ?? []) {
			if (!byID.has(input.id)) return undefined;
			if (input.owned) {
				if (owned.has(input.id) && owned.get(input.id) !== node.id) return undefined;
				owned.set(input.id, node.id);
			}
		}
		const visiting = new Set<string>(), visited = new Set<string>();
		const visit = (id: string): boolean => {
			if (visiting.has(id)) return false;
			if (visited.has(id)) return true;
			visiting.add(id);
			for (const input of byID.get(id)!.inputs ?? []) if (!visit(input.id)) return false;
			visiting.delete(id); visited.add(id);
			return true;
		};
		if (!visit(value.root) || visited.size !== nodes.length) return undefined;
		const graph = Object.freeze({ version: value.version as 1 | 2, root: value.root, nodes: Object.freeze(nodes) });
		return Buffer.byteLength(JSON.stringify(graph), "utf8") <= TIMELINE_COMPUTATION_LIMITS.bytes ? graph : undefined;
	} catch { return undefined; }
}

function record(value: unknown): value is Record<string, unknown> { return !!value && typeof value === "object" && !Array.isArray(value); }
function identity(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 256 && !value.includes("\0"); }
function duration(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER; }
function endpoints(value: Record<string, unknown>): boolean { return duration(value.startedAt) && duration(value.completedAt) && value.completedAt >= value.startedAt; }
