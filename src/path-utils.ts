import path from "node:path";

export function slash(value: string): string {
	return value.replaceAll("\\", "/");
}

/** Normalize a logical resource without folding case. */
export function normalizeLogicalPath(value: string): string {
	const normalized = path.posix.normalize(slash(value));
	return normalized === "/" || /^[A-Za-z]:\/$/.test(normalized) ? normalized : normalized.replace(/\/$/, "");
}

/** Preserve the spelling of a resolved physical path; the backing volume may distinguish case. */
export function filesystemPathKey(value: string): string {
	return slash(path.resolve(value));
}

export function relativeFilesystemPath(root: string, target: string): string | undefined {
	const resolvedRoot = path.resolve(root);
	const resolvedTarget = path.resolve(target);
	const relative = path.relative(resolvedRoot, resolvedTarget);
	if (relative === "") return resolvedRoot === resolvedTarget ? "" : undefined;
	if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return undefined;
	return path.resolve(resolvedRoot, relative) === resolvedTarget ? relative : undefined;
}

export function containsFilesystemPath(root: string, target: string): boolean {
	return relativeFilesystemPath(root, target) !== undefined;
}

export function containsLogicalPath(root: string, target: string): boolean {
	const relative = path.posix.relative(normalizeLogicalPath(root), normalizeLogicalPath(target));
	return relative === "" || (relative !== ".." && !relative.startsWith("../") && !path.posix.isAbsolute(relative));
}
