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
