import { afterAll, expect, test } from "vitest";
import { link, lstat, mkdir, readdir, rename, unlink, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { pinFilesystemObject } from "../src/filesystem-evidence.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { temporaryDirectories, readText } from "./filesystem.ts";

const directories = temporaryDirectories("pi-object-lease-");
afterAll(() => directories.dispose());
const output = { result: { content: [], details: {} }, isError: false };

test("retains an object while its names move and disappear", async () => {
	const root = await directories.create(), directory = path.join(root, "directory"), moved = path.join(root, "moved");
	await mkdir(directory); await writeFile(path.join(directory, "input"), "base");
	const lease = await pinFilesystemObject(path.join(directory, "input"));
	try {
		await rename(directory, moved);
		expect((await lstat(path.join(moved, "input"), { bigint: true })).ino).toBe(lease.stat.ino);
		await unlink(path.join(moved, "input")); await writeFile(path.join(moved, "input"), "next");
		expect((await lstat(path.join(moved, "input"), { bigint: true })).ino).not.toBe(lease.stat.ino);
	} finally { await lease.close(); }
});

test.for([false, true])("adopts a renamed private directory and its checkpoint successor (checkpoint=%s)", async checkpoint => {
	const bare = await directories.create(), root = await directories.create(), service = new WorkspaceSandboxService();
	const setup = async (cwd: string) => { await mkdir(path.join(cwd, "directory")); await writeFile(path.join(cwd, "directory/input"), "base"); await link(path.join(cwd, "directory/input"), path.join(cwd, "alias")); };
	const operate = async (cwd: string) => { await rename(path.join(cwd, "directory"), path.join(cwd, "moved")); await writeFile(path.join(cwd, "moved/input"), "changed"); };
	await setup(bare); await setup(root); await operate(bare);
	try {
		const captured = await service.capture({ cwd: root, driver: "git", execute: async workspace => { await operate(workspace.sandboxRoot); return output; } });
		expect(await readText(root, "directory/input")).toBe("base"); expect(await readdir(root)).not.toContain("moved");
		const action = PI_ACTION_SEMANTICS.buildKey("write", { path: "moved/input", content: "changed" }, root, "lease")!;
		const branch = service.branch({ cwd: root, action }, captured);
		try {
			const successor = checkpoint ? await service.capture({ cwd: root, driver: "git", execute: async workspace => {
				expect(await readText(workspace.sandboxRoot, "moved/input")).toBe("changed");
				await writeFile(path.join(workspace.sandboxRoot, "moved/input"), "successor"); return output;
			} }, service.checkpoint(branch.checkpoint, root)) : undefined;
			await branch.commit(); await branch.commit();
			for (const name of ["moved/input", "alias"]) expect(await readText(root, name)).toBe(await readText(bare, name));
			expect((await lstat(path.join(root, "moved/input"), { bigint: true })).ino).toBe((await lstat(path.join(root, "alias"), { bigint: true })).ino);
			if (successor) { await service.commitDelta(successor); expect(await readText(root, "alias")).toBe("successor"); }
		} finally { await branch.dispose(); }
	} finally { await service.dispose(); }
});

test.skipIf(process.platform !== "win32" || process.arch !== "x64")("qualifies packaged ID leases, hardlink capacity and owner cleanup", { timeout: 90_000 }, async () => {
	const { stdout } = await promisify(execFile)(process.execPath, ["--expose-gc", fileURLToPath(new URL("./windows-filesystem.fixture.mjs", import.meta.url))]);
	const result = JSON.parse(stdout);
	expect(result.results).toHaveLength(6); expect(result.results.every((entry: { status: string }) => entry.status === "passed"), stdout).toBe(true);
	expect(result.cleaned).toBe(true); expect(result.liveLeases).toBe(0);
});
