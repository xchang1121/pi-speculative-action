import path from "node:path";

export const slash = (value: string): string => path.sep === "/" ? value : value.replaceAll(path.sep, "/");

/** Normalize a logical resource without folding case. */
export function normalizeLogicalPath(value: string): string {
	const normalized = path.posix.normalize(slash(value));
	return normalized === "/" || /^[A-Za-z]:\/$/.test(normalized) ? normalized : normalized.replace(/\/$/, "");
}

/** Preserve the spelling of a resolved physical path; the backing volume may distinguish case. */
export function filesystemPathKey(value: string): string {
	return slash(path.resolve(value));
}

/** Windows drive letters never distinguish case; directory names may, so they keep their exact spelling. */
export function sameFilesystemPath(left: string, right: string): boolean {
	const drive = (value: string) => process.platform === "win32" ? value.replace(/^[a-z](?=:)/u, (letter) => letter.toUpperCase()) : value;
	return drive(left) === drive(right);
}

/** An absolute POSIX path without an empty, `.` or `..` segment or a trailing slash: between two, containment is a prefix. */
const NORMALIZED_ABSOLUTE = /^(?:\/(?!\.{1,2}(?:\/|$))[^/]+)+$/;

export function relativeFilesystemPath(root: string, target: string): string | undefined {
	if (path.sep === "/" && NORMALIZED_ABSOLUTE.test(root) && NORMALIZED_ABSOLUTE.test(target)) return target === root ? "" : target.startsWith(`${root}/`) ? target.slice(root.length + 1) : undefined;
	const resolvedRoot = path.resolve(root), resolvedTarget = path.resolve(target), relative = path.relative(resolvedRoot, resolvedTarget);
	if (relative === "") return sameFilesystemPath(resolvedRoot, resolvedTarget) ? "" : undefined;
	if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return undefined;
	return sameFilesystemPath(path.resolve(resolvedRoot, relative), resolvedTarget) ? relative : undefined;
}

export const containsFilesystemPath = (root: string, target: string): boolean => relativeFilesystemPath(root, target) !== undefined;

export function containsLogicalPath(root: string, target: string): boolean {
	const relative = path.posix.relative(normalizeLogicalPath(root), normalizeLogicalPath(target));
	return relative === "" || (relative !== ".." && !relative.startsWith("../") && !path.posix.isAbsolute(relative));
}
