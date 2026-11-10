import fs, { mkdir, rm, symlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { onExit } from "./async.ts";

const directories = temporaryDirectories("pi-search-index-");
afterEach(async () => { vi.restoreAllMocks(); await directories.dispose(); });

test.each(["ordinary", "glob", "context", "limit", "literal", "case", "unicode", "added", "deleted", "ignore-changed", "symlink"])(
	"shared captured file selection preserves search semantics (%s)", async mode => {
	const cwd = await directories.create(); await mkdir(path.join(cwd, "nested"));
	await writeFile(path.join(cwd, "a.txt"), "alpha\nbeta\nALPHA\n");
	await writeFile(path.join(cwd, "nested/b.c"), "before\nalpha\nafter\n");
	await writeFile(path.join(cwd, "名字.txt"), "alpha\n");
	await writeFile(path.join(cwd, ".ignore"), "ignored.txt\n");
	await writeFile(path.join(cwd, "ignored.txt"), "alpha\n");
	if (mode === "symlink") await symlink(path.join(cwd, "nested"), path.join(cwd, "alias"), "junction");
	const profile = await createClosedSearchProfile(cwd), invocation = profile.invocations.get("grep")!;
	const scratch = vi.spyOn(fs, "mkdtemp");
	expect(invocation, "qualified pinned grep engine").toBeDefined();
	const f = systemHost(cwd, [createGrepTool(cwd)], { resolveInvocation: () => invocation });
	await using _cleanup = onExit(async () => { await f.dispose(); await profile.pool.dispose(); });
	const args = { pattern: mode === "literal" ? "alpha" : "alpha|beta", path: mode === "unicode" ? "名字.txt" : ".",
		...(mode === "glob" ? { glob: "*.c" } : {}), ...(mode === "context" ? { context: 1 } : {}),
		...(mode === "limit" ? { limit: 1 } : {}), ...(mode === "literal" ? { literal: true } : {}), ...(mode === "case" ? { ignoreCase: true } : {}) };
	await f.start(); await f.preview("grep", args); await f.ready();
	if (mode === "added") await writeFile(path.join(cwd, "new.txt"), "alpha\n");
	if (mode === "deleted") await rm(path.join(cwd, "a.txt"));
	if (mode === "ignore-changed") await writeFile(path.join(cwd, ".ignore"), "a.txt\n");
	const expected = await invocation.authoritative!({ callID: "oracle", args, signal: new AbortController().signal });
	expect(await f.execute("grep", args)).toMatchObject(expected.result);
	// A different regex can borrow the namespace/index but must evaluate its own content and formatting.
	const next = { ...args, pattern: "after", context: 1 };
	const nextExpected = await invocation.authoritative!({ callID: "oracle-next", args: next, signal: new AbortController().signal });
	expect(await f.execute("grep", next)).toMatchObject(nextExpected.result);
	if (mode === "ordinary") expect(scratch.mock.calls.filter(([prefix]) => String(prefix).includes("inputs-")).length).toBe(1);
});
