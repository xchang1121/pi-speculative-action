import { createHash } from "node:crypto";
import path from "node:path";
import { execHostFile } from "./executable-path.ts";

async function gitOutput(cwd: string, args: readonly string[]): Promise<string> {
	const { stdout } = await execHostFile("git", ["--no-optional-locks", "-C", cwd, ...args], {
		encoding: "utf8",
		windowsHide: true,
	});
	return stdout.trim();
}

/**
 * Keep learned patterns attached to the repository, even when runners reuse the
 * same checkout path for unrelated projects. Raw remotes never leave this scope.
 */
export async function resolvePatternWorkspaceIdentity(cwd: string): Promise<string> {
	const absoluteCwd = path.resolve(cwd);
	try {
		const layout = await gitOutput(absoluteCwd, [
			"rev-parse",
			"--path-format=absolute",
			"--show-toplevel",
			"--git-common-dir",
		]);
		const [root, commonDirectory] = layout.split(/\r?\n/u);
		if (!root || !commonDirectory) return absoluteCwd;

		let repository = commonDirectory;
		try {
			repository = (await gitOutput(absoluteCwd, ["config", "--get", "remote.origin.url"])) || commonDirectory;
		} catch {
			// Local repositories still have a stable common Git directory.
		}

		const relativeCwd = path.relative(root, absoluteCwd);
		const digest = createHash("sha256").update(repository).update("\0").update(relativeCwd).digest("hex");
		return path.join(path.parse(absoluteCwd).root, ".pi-pattern-repositories", digest);
	} catch {
		return absoluteCwd;
	}
}
