export function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

export function hasErrorCode(error: unknown, code: string): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === code);
}

export function isMissing(error: unknown): boolean {
	return hasErrorCode(error, "ENOENT");
}
