import { execFileSync } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import path from "node:path";
import { afterEach, describe, expect, test } from "vitest";
import { resolvePatternWorkspaceIdentity } from "../src/workspace-identity.ts";

const { create: temporaryDirectory, dispose } = temporaryDirectories("pi-pattern-identity-");

afterEach(dispose);

describe("PatternAware workspace identity", () => {
	test("retains path scoping outside a Git repository", async () => {
		const cwd = await temporaryDirectory();
		expect(await resolvePatternWorkspaceIdentity(cwd)).toBe(path.resolve(cwd));
	});

	test("shares a logical repository across clones and partitions remotes and subdirectories", async () => {
		const cwd = await temporaryDirectory();
		execFileSync("git", ["-C", cwd, "init", "--quiet"]);
		execFileSync("git", ["-C", cwd, "remote", "add", "origin", "https://example.test/one.git"]);
		const secondClone = await temporaryDirectory();
		execFileSync("git", ["-C", secondClone, "init", "--quiet"]);
		execFileSync("git", ["-C", secondClone, "remote", "add", "origin", "https://example.test/one.git"]);
		const subdirectory = path.join(cwd, "packages", "app");
		await mkdir(subdirectory, { recursive: true });

		const first = await resolvePatternWorkspaceIdentity(cwd);
		expect(await resolvePatternWorkspaceIdentity(cwd)).toBe(first);
		expect(await resolvePatternWorkspaceIdentity(secondClone)).toBe(first);
		expect(await resolvePatternWorkspaceIdentity(subdirectory)).not.toBe(first);

		execFileSync("git", ["-C", cwd, "remote", "set-url", "origin", "https://example.test/two.git"]);
		expect(await resolvePatternWorkspaceIdentity(cwd)).not.toBe(first);
		expect(first).not.toContain("example.test");
	});
});
