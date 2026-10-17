/** Validate discrete quantities without coercing, rounding or clamping malformed evidence. */
export function isSafeCount(value: unknown, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): value is number {
	return Number.isSafeInteger(value) && (value as number) >= minimum && (value as number) <= maximum;
}

/** Clamp measured values; non-finite and missing evidence contributes zero. */
export function nonNegativeFinite(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

export function nonNegativeCount(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

export function positiveCount(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(1, Math.floor(value)) : 1;
}

export function finiteNumber(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function clampProbability(value: number): number {
	return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}
