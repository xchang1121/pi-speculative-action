import path from "node:path";

export function slash(value: string): string {
	return value.replaceAll("\\", "/");
}

export function contains(root: string, target: string): boolean {
	const relative = path.relative(root, target);
	return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}
