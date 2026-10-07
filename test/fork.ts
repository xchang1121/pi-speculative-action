/** The wire envelope shared by sidecar coordinator and host integration fixtures. */
export function forkReceipt(candidates: readonly Record<string, unknown>[], metadata: Record<string, unknown> = {}): Record<string, unknown> {
	return { ...metadata, details: { bundle: { candidates } } };
}
