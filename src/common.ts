import { KEYABLE_TOOLS, type ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import type { SpeculativeActionSettings } from "./runtime-contracts.ts";
import { positiveCount } from "./number-utils.ts";
import { booleanOr, nonNegativeInteger, nonNegativeNumber, positiveInteger, settingsParser } from "./setting-input.ts";

export interface DrafterToolDefinition { readonly name: string; readonly description?: string; readonly inputSchema?: unknown; }

export type DrafterRequestSettings = Readonly<typeof DRAFTER_DEFAULTS>;

const { defaults: DRAFTER_DEFAULTS, parse: parseDrafterSettings } = settingsParser({
	/** Output-informed successor actions retained after the first Drafter action. */
	drafterMaxDepth: [3, nonNegativeInteger],
	/** Output cap for each Drafter request: one proposed tool batch never needs the Actor's answer budget. */
	drafterMaxTokens: [4096, positiveInteger],
	/** Cumulative request and input/output token budgets for one user task. */
	drafterTaskMaxRequests: [32, positiveInteger],
	drafterTaskMaxTokens: [262_144, positiveInteger],
	/** Number of leading Drafter requests sent at temperature zero. */
	drafterDeterministicCandidates: [1, nonNegativeInteger],
	/** Inclusive temperature range stratified across the remaining requests. */
	drafterTemperatureMin: [0.7, nonNegativeNumber],
	drafterTemperatureMax: [0.7, nonNegativeNumber],
	/** Show the Drafter the calls PatternAware expects next (for A/B; off by default). */
	drafterPatternHints: [false, booleanOr],
});

export const DEFAULTS = {
	enabled: false,
	drafterEnabled: true,
	...DRAFTER_DEFAULTS,
	candidateLimit: 2,
	maxConcurrentActions: 8,
	resourceCacheMaxEntries: 512,
	resourceCacheMaxBytes: 256 * 1024 * 1024,
	predictionTimeoutMs: 300_000,
	thinkThreadTimeoutMs: 120_000,
	tools: KEYABLE_TOOLS,
};

export const clampCandidateLimit = positiveCount;

export function candidateToolNames(
	settings: SpeculativeActionSettings,
	semantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): readonly string[] {
	const known = new Set(semantics.toolNames());
	return [...new Set(settings.tools)].filter((tool) => known.has(tool));
}

/** Only omitted selection defaults to allowed tools; malformed input disables prediction. */
export function normalizeSpeculativeToolSelection(value: unknown, allowed: readonly string[] = KEYABLE_TOOLS): readonly string[] {
	const items = value === undefined ? allowed : value;
	if (!Array.isArray(items) || !items.every((item): item is string => typeof item === "string")) return [];
	const allowedSet = new Set(allowed);
	return [...new Set(items.filter((item) => allowedSet.has(item)))];
}

export function normalizeDrafterRequestSettings(value: unknown): DrafterRequestSettings {
	const result = parseDrafterSettings(value && typeof value === "object" ? value as Record<string, unknown> : undefined);
	const lower = result.drafterTemperatureMin, upper = result.drafterTemperatureMax;
	return { ...result, drafterTemperatureMin: Math.min(lower, upper), drafterTemperatureMax: Math.max(lower, upper) };
}

/** Stratify non-deterministic requests across the configured range for any proposal count. */
export function drafterRequestTemperature(proposalIndex: number, proposalCount: number, settings: DrafterRequestSettings): number {
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
