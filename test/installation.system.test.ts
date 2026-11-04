import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, test } from "vitest";
import { readText, temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-install-system-");
afterEach(directories.dispose);

test.skipIf(process.platform !== "linux").each(["mirror", "source", "descendant", "foreign"])("verifies the working snapshot without resetting user checkouts (%s)", async mode => {
	const root = await directories.create(), repo = path.join(root, "source"), bin = path.join(root, "bin"), work = path.join(root, "verification");
	await Promise.all([mkdir(path.join(repo, "scripts"), { recursive: true }), mkdir(bin)]);
	await writeFile(path.join(bin, "npm"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
	await writeFile(path.join(bin, "node"), '#!/bin/sh\nset -eu\n[ "$1" != ./src/setup-linux-process-backend.mjs ] || exit 0\n[ "$2" = --run ]\n[ "$(command -v fusermount3)" = "$(dirname "$PI_SPEC_HELD_EXEC")/fusermount3" ]\n[ "$PI_SPEC_HELD_EXEC_OBSERVER" = "$PI_SPEC_HELD_EXEC.observer" ]\necho verified-helpers\n', { mode: 0o755 });
	await writeFile(path.join(bin, "sudo"), '#!/bin/sh\nset -eu\n[ "$1" = -- ]\nshift\nexec env -i "$@"\n', { mode: 0o755 });
	const helpers = path.join(work, "home/.local/bin"); await mkdir(helpers, { recursive: true });
	await writeFile(path.join(helpers, "fusermount3"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
	await writeFile(path.join(helpers, "pi-speculative-domain"), '#!/bin/sh\nset -eu\n[ "$1" = --user ]\n[ "$2" = "$(id -u)" ]\nshift 2\necho verified-user\nexec "$@"\n', { mode: 0o755 });
	const git = (...args: string[]) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
	await writeFile(path.join(repo, "scripts", "wsl-verify.sh"), await readFile(fileURLToPath(new URL("../scripts/wsl-verify.sh", import.meta.url))));
	await writeFile(path.join(repo, "tracked"), "base");
	git("init", "--quiet"); git("add", "."); git("-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "--quiet", "-m", "fixture");
	await writeFile(path.join(repo, "tracked"), "modified");
	const added = "new file\nwith spaces";
	await writeFile(path.join(repo, added), "new source");
	const checkout = mode === "source" ? repo : mode === "descendant" ? path.join(repo, "child") : path.join(work, "checkout");
	if (mode === "foreign") {
		await mkdir(checkout, { recursive: true }); execFileSync("git", ["init", "--quiet", checkout]);
		await writeFile(path.join(checkout, "user-file"), "keep");
	}
	const before = git("status", "--porcelain");
	const run = () => execFileSync("bash", [path.join(repo, "scripts", "wsl-verify.sh")], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
		env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, PI_SPEC_WSL_WORK: work, PI_SPEC_WSL_CHECKOUT: checkout,
			PI_SPEC_HELD_EXEC_OBSERVER: path.join(helpers, "pi-speculative-held-exec.observer") }, timeout: 30_000 });
	if (mode === "mirror") {
		expect(run()).toContain("verified-user\nverified-helpers");
		expect(await readText(checkout, "tracked")).toBe("modified");
		expect(await readText(checkout, added)).toBe("new source");
	} else {
		expect(run).toThrow();
		if (mode === "foreign") expect(await readText(checkout, "user-file")).toBe("keep");
	}
	expect(git("status", "--porcelain")).toBe(before);
	expect(await readText(repo, "tracked")).toBe("modified");
	expect(await readText(repo, added)).toBe("new source");
});
