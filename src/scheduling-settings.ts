import { positiveMilliseconds, positiveInteger, probability, settingsParser } from "./setting-input.ts";

/** Admission and resource sampling hints; none of these fields grant execution or reuse authority. */
export type SchedulingSettings = Readonly<typeof SCHEDULING_DEFAULTS>;
export const { defaults: SCHEDULING_DEFAULTS, parse: normalizeSchedulingSettings } = settingsParser({
	resourcePollIntervalMs: [250, positiveMilliseconds],
	gpuPollIntervalMs: [1_000, positiveMilliseconds],
	gpuProbeTimeoutMs: [1_000, positiveMilliseconds],
	failureThreshold: [2, positiveInteger],
	failureRetryDecisions: [4, positiveInteger],
	heavyCpu: [2, positiveInteger],
	lightCpu: [1, positiveInteger],
	heavyMemoryBytes: [64 * 1024 * 1024, positiveInteger],
	lightMemoryBytes: [8 * 1024 * 1024, positiveInteger],
	heavyIo: [0.25, probability],
	lightIo: [0.125, probability],
});
