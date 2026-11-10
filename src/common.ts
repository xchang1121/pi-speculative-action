import { positiveCount } from "./number-utils.ts";
import { booleanOr, nonNegativeInteger, nonNegativeNumber, optionalPositiveInteger, optionalPositiveMilliseconds, positiveInteger, probability, settingsParser } from "./setting-input.ts";

export interface DrafterToolDefinition { readonly name: string; readonly description?: string; readonly inputSchema?: unknown; }

export type DrafterRequestSettings = Readonly<typeof DRAFTER_DEFAULTS>;

const { defaults: DRAFTER_DEFAULTS, parse: parseDrafterSettings } = settingsParser({
	/** Successor batches predicted from private observations while earlier actions execute. */
	drafterMaxDepth: [3, nonNegativeInteger],
	/** Output cap for each Drafter request; null leaves generation length to the provider. */
	drafterMaxTokens: [4096 as number | null, optionalPositiveInteger],
	/** Cumulative budgets for one host task; explicit null disables each cap. */
	drafterTaskMaxRequests: [32 as number | null, optionalPositiveInteger],
	drafterTaskMaxTokens: [262_144 as number | null, optionalPositiveInteger],
	/** Number of leading Drafter requests sent at temperature zero. */
	drafterDeterministicCandidates: [1, nonNegativeInteger],
	/** Inclusive temperature range stratified across the remaining requests. */
	drafterTemperatureMin: [0.7, nonNegativeNumber],
	drafterTemperatureMax: [0.7, nonNegativeNumber],
	/** Show the Drafter the calls PatternAware expects next (for A/B; off by default). */
	drafterPatternHints: [false, booleanOr],
	/** Optional hints cannot consume the root's entire lead time. */
	drafterPatternHintsTimeoutMs: [250, positiveInteger],
	/** Release task budget gradually; the cumulative hard caps remain unchanged. */
	drafterBudgetHorizonTurns: [16, positiveInteger],
	drafterBudgetInitialFraction: [0.25, probability],
	/** Shared upstream capacity, independent from speculative tool execution capacity. */
	modelRequestMaxConcurrent: [4, positiveInteger],
	modelRequestActorReserve: [1, positiveInteger],
	/** null waits for dispatched inference to settle without a client teardown deadline. */
	modelRequestDrainTimeoutMs: [30_000 as number | null, optionalPositiveMilliseconds],
});

export const DEFAULTS = {
	enabled: false,
	reuseEnabled: true,
	drafterEnabled: true,
	prepareObservedOperations: true,
	...DRAFTER_DEFAULTS,
	candidateLimit: 2,
	maxConcurrentActions: 8,
	resourceCacheMaxEntries: 512,
	resourceCacheMaxBytes: 256 * 1024 * 1024,
	predictionTimeoutMs: 300_000,
	reuseLookupTimeoutMs: 5_000,
};

export const clampCandidateLimit = positiveCount;

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
