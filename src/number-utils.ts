/** Validate discrete quantities without coercing, rounding or clamping malformed evidence. */
export function isSafeCount(value: unknown, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): value is number {
	return Number.isSafeInteger(value) && (value as number) >= minimum && (value as number) <= maximum;
}

export function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

/** Clamp measured values; non-finite and missing evidence contributes zero. */
export function nonNegativeFinite(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(0, value) : 0;
}

export function nonNegativeCount(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(0, Math.floor(value)) : 0;
}

export function positiveCount(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(1, Math.floor(value)) : 1;
}

export function finiteNumber(value: unknown): number | undefined {
	return isFiniteNumber(value) ? value : undefined;
}

export function clampProbability(value: number): number {
	return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}

/** Copy and increment own data keys; arbitrary names never address inherited properties. */
export function incrementCounts(target: Readonly<Record<string, number>>, keys: readonly string[]): Record<string, number> {
	const counts = { ...Object.fromEntries(keys.map(key => [key, 0])), ...target };
	for (const key of keys) counts[key]!++;
	return counts;
}
