import { execFile, type ExecFileOptions } from "node:child_process";
import { constants as fsConstants } from "node:fs";
import { access, realpath } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

export const execHostFile = promisify(execFile);

/** Resolve a trusted host executable from explicit paths before consulting PATH. */
export async function resolveHostExecutable(
	explicit: string | undefined,
	name: string,
	fallbacks: readonly string[] = [],
	alternateNames: readonly string[] = [],
	preserveSymlink = false,
): Promise<string> {
	const directories = (process.env.PATH ?? "").split(path.delimiter).filter(Boolean);
	for (const candidate of [explicit, ...fallbacks, ...[name, ...alternateNames].flatMap(name => directories.map(directory => path.join(directory, name)))])
		if (candidate && await access(candidate, fsConstants.X_OK).then(() => true, () => false))
			return preserveSymlink ? path.resolve(candidate) : realpath(candidate);
	throw new Error(`Executable not found: ${[name, ...alternateNames].join(" or ")}`);
}

/** Run a host utility with bounded output; callers retain their timeout and environment policy. */
export function execHostText(executable: string, args: readonly string[], options: ExecFileOptions = {}): Promise<string> {
	return execHostFile(executable, args, { maxBuffer: 16 * 1024 * 1024, ...options, encoding: "utf8" })
		.then(({ stdout, stderr }) => `${stdout}${stderr}`, error => {
			if (error.stderr === undefined) throw error;
			throw new Error(`${executable} failed: ${error.stderr.trim() || error.message}`, { cause: error });
		});
}
