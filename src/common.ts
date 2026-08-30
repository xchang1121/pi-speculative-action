import {
	KEYABLE_TOOLS,
	OBSERVATION_ACTION_TOOLS,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "./action-semantics.ts";

export interface LegacySpeculativeToolGroups {
	readonly resourceCached?: readonly string[];
	readonly sandbox?: readonly string[];
	readonly predictionOnly?: readonly string[];
}

export type SpeculativeToolSelectionInput = readonly string[] | LegacySpeculativeToolGroups;

export interface DrafterToolDefinition {
	readonly name: string;
	readonly description?: string;
	readonly inputSchema?: unknown;
}

export interface DrafterRequestSettings {
	readonly drafterMaxDepth: number;
	readonly drafterMaxTokens?: number;
	readonly drafterDeterministicCandidates: number;
	readonly drafterTemperatureMin: number;
	readonly drafterTemperatureMax: number;
}

const DRAFTER_DEFAULTS: DrafterRequestSettings = {
	drafterMaxDepth: 1,
	drafterDeterministicCandidates: 1,
	drafterTemperatureMin: 0.7,
	drafterTemperatureMax: 0.7,
};

export const DEFAULTS = {
	enabled: false,
	drafterEnabled: true,
	drafterGateEnabled: true,
	...DRAFTER_DEFAULTS,
	candidateLimit: 2,
	maxConcurrentActions: 8,
	resourceCacheMaxEntries: 512,
	resourceCacheMaxBytes: 256 * 1024 * 1024,
	predictionTimeoutMs: 300_000,
	tools: KEYABLE_TOOLS,
};

export function clampCandidateLimit(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(1, Math.floor(value)) : 1;
}

/** Normalize the current tool list and migrate the former three-group configuration at one boundary. */
export function normalizeSpeculativeToolSelection(
	value: unknown,
	allowed: readonly string[] = KEYABLE_TOOLS,
): readonly string[] {
	const allowedSet = new Set(allowed);
	const select = (items: readonly string[]) => [...new Set(items.filter((item) => allowedSet.has(item)))];
	if (Array.isArray(value)) {
		return value.every((item): item is string => typeof item === "string") ? select(value) : select(allowed);
	}
	if (!value || typeof value !== "object") return select(allowed);
	const legacy = value as Record<string, unknown>;
	return select([
		...stringArrayOr(legacy.resourceCached, OBSERVATION_ACTION_TOOLS),
		...stringArrayOr(legacy.sandbox, WORKSPACE_MUTATION_ACTION_TOOLS),
		...stringArrayOr(legacy.predictionOnly, UNBOUNDED_ACTION_TOOLS),
	]);
}

export function normalizeDrafterRequestSettings(value: unknown): DrafterRequestSettings {
	const input = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
	const lower = nonNegativeNumber(input.drafterTemperatureMin, DEFAULTS.drafterTemperatureMin);
	const upper = nonNegativeNumber(input.drafterTemperatureMax, DEFAULTS.drafterTemperatureMax);
	const maxTokens = optionalPositiveInteger(input.drafterMaxTokens);
	return {
		drafterMaxDepth: nonNegativeInteger(input.drafterMaxDepth, DEFAULTS.drafterMaxDepth),
		...(maxTokens ? { drafterMaxTokens: maxTokens } : {}),
		drafterDeterministicCandidates: nonNegativeInteger(
			input.drafterDeterministicCandidates,
			DEFAULTS.drafterDeterministicCandidates,
		),
		drafterTemperatureMin: Math.min(lower, upper),
		drafterTemperatureMax: Math.max(lower, upper),
	};
}

/** Stratify non-deterministic requests across the configured range for any proposal count. */
export function drafterRequestTemperature(
	proposalIndex: number,
	proposalCount: number,
	settings: DrafterRequestSettings,
): number {
	const count = clampCandidateLimit(proposalCount);
	const index = Math.max(0, Math.min(count - 1, Math.floor(proposalIndex)));
	const deterministic = Math.min(count, settings.drafterDeterministicCandidates);
	if (index < deterministic) return 0;
	const stochasticCount = count - deterministic;
	if (stochasticCount === 1) return (settings.drafterTemperatureMin + settings.drafterTemperatureMax) / 2;
	return (
		settings.drafterTemperatureMin +
		((settings.drafterTemperatureMax - settings.drafterTemperatureMin) * (index - deterministic)) /
			(stochasticCount - 1)
	);
}

export function usageTokenCount(
	usage:
		| {
				readonly input?: number;
				readonly output?: number;
				readonly cacheRead?: number;
				readonly cacheWrite?: number;
				readonly totalTokens?: number;
		  }
		| undefined,
): number {
	if (!usage) return 0;
	if (typeof usage.totalTokens === "number" && Number.isFinite(usage.totalTokens)) return usage.totalTokens;
	return [usage.input, usage.output, usage.cacheRead, usage.cacheWrite]
		.filter((value): value is number => typeof value === "number" && Number.isFinite(value))
		.reduce((sum, value) => sum + value, 0);
}

function optionalPositiveInteger(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : undefined;
}

function nonNegativeInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.floor(value) : fallback;
}

function nonNegativeNumber(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : fallback;
}

function stringArrayOr(value: unknown, fallback: readonly string[]): readonly string[] {
	return Array.isArray(value) && value.every((item): item is string => typeof item === "string") ? value : fallback;
}
