import { constants as fsConstants } from "node:fs";
import { access, realpath } from "node:fs/promises";
import path from "node:path";

/** Resolve a trusted host executable from explicit paths before consulting PATH. */
export async function resolveHostExecutable(
	explicit: string | undefined,
	name: string,
	fallbacks: readonly string[] = [],
	alternateNames: readonly string[] = [],
): Promise<string> {
	for (const candidate of [explicit, ...fallbacks]) {
		if (candidate && await executable(candidate)) return realpath(candidate);
	}
	for (const executableName of [name, ...alternateNames]) {
		for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
			if (!directory) continue;
			const candidate = path.join(directory, executableName);
			if (await executable(candidate)) return realpath(candidate);
		}
	}
	throw new Error(`Executable not found: ${[name, ...alternateNames].join(" or ")}`);
}

async function executable(candidate: string): Promise<boolean> {
	return access(candidate, fsConstants.X_OK).then(() => true, () => false);
}
