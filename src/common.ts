import { KEYABLE_TOOLS } from "./action-semantics.ts";
import { nonNegativeInteger, nonNegativeNumber, positiveInteger } from "./setting-input.ts";

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

/** Only omitted selection defaults to allowed tools; malformed input disables prediction. */
export function normalizeSpeculativeToolSelection(
	value: unknown,
	allowed: readonly string[] = KEYABLE_TOOLS,
): readonly string[] {
	const items = value === undefined ? allowed : value;
	if (!Array.isArray(items) || !items.every((item): item is string => typeof item === "string")) return [];
	const allowedSet = new Set(allowed);
	return [...new Set(items.filter((item) => allowedSet.has(item)))];
}

export function normalizeDrafterRequestSettings(value: unknown): DrafterRequestSettings {
	const input = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
	const lower = nonNegativeNumber(input.drafterTemperatureMin, DEFAULTS.drafterTemperatureMin);
	const upper = nonNegativeNumber(input.drafterTemperatureMax, DEFAULTS.drafterTemperatureMax);
	const maxTokens = positiveInteger(input.drafterMaxTokens, undefined);
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
