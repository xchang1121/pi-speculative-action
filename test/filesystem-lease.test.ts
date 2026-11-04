import { afterAll, expect, test, vi } from "vitest";
import { link, lstat, mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { deferred, nextTurn } from "./async.ts";
import { temporaryDirectories, readText } from "./filesystem.ts";

const directories = temporaryDirectories("pi-object-lease-");
afterAll(() => directories.dispose());
const output = { result: { content: [], details: {} }, isError: false };

test.for(["fresh", "stale", "closing", "alias", "alias-stale", "binary"])("transfers workspace inputs through their owner (%s)", async mode => {
	const root = await directories.create(), service = new WorkspaceSandboxService(), target = path.join(root, "input");
	const entered = deferred<void>(), release = deferred<void>(); let disposed = 0, closed = false;
	const before = Buffer.from(mode === "binary" ? [0, 1, 255] : "before"), after = Buffer.from(mode === "binary" ? [0, 2, 254] : "after");
	await writeFile(target, before);
	if (mode.startsWith("alias")) await link(target, path.join(root, "alias"));
	const snapshot = await service.capture({ cwd: root, driver: "git", execute: async workspace => {
		await writeFile(path.join(workspace.sandboxRoot, "input"), after); return output;
	} });
	const action = PI_ACTION_SEMANTICS.buildKey("write", { path: "input", content: "after" }, root, "input-transfer")!;
	const branch = service.branch({ cwd: root, action }, { ...snapshot, dispose: async () => { disposed++; await snapshot.dispose?.(); } });
	const validate = ResourceVersionManager.prototype.validate;
	const spy = mode === "closing" ? vi.spyOn(ResourceVersionManager.prototype, "validate").mockImplementation(async function(this: ResourceVersionManager, token) {
		entered.resolve(); await release.promise; return validate.call(this, token);
	}) : undefined;
	let inputs: Awaited<ReturnType<NonNullable<typeof branch.takeReadInputs>>>;
	try {
		if (mode.endsWith("stale")) await writeFile(mode === "alias-stale" ? path.join(root, "alias") : target, "changed");
		const reading = branch.takeReadInputs!(1024 * 1024);
		if (mode === "closing") {
			await Promise.race([entered.promise, reading.then(() => { throw new Error("input capture ended before validation"); })]);
			const closing = Promise.resolve(branch.dispose()).then(() => { closed = true; });
			await nextTurn(); expect(closed).toBe(false); expect(disposed).toBe(0);
			release.resolve(); await closing;
		}
		inputs = await reading;
		expect(!!inputs).toBe(!mode.endsWith("stale") && mode !== "closing");
		if (inputs) {
			expect((await inputs.validate!()).status).toBe("valid");
			await branch.commit(); expect(await readFile(target)).toEqual(after);
			const committed = await branch.takeCommittedInputs!(1024 * 1024);
			try { expect(committed).toBeDefined(); expect((await committed!.validate!()).status).toBe("valid"); }
			finally { await committed?.dispose(); }
		}
		expect(await branch.takeReadInputs!(1024 * 1024)).toBeUndefined();
	} finally { release.resolve(); spy?.mockRestore(); await inputs?.dispose(); await branch.dispose(); await branch.dispose(); await service.dispose(); }
	expect(disposed).toBe(1);
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
