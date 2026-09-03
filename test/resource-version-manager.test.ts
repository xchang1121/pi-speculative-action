import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, test } from "vitest";
import { ActionSemanticsRegistry, buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { filesystemPathKey } from "../src/filesystem-evidence.ts";
import {
	captureResourceVersion,
	closeResourceVersionManagers,
	ResourceVersionManager,
	releaseResourceVersion,
	resourceDependencies,
	validateResourceVersion,
	watchResourceVersion,
} from "../src/resource-version.ts";

const roots: string[] = [];
const execFileAsync = promisify(execFile);

afterEach(async () => {
	closeResourceVersionManagers();
	await Promise.all(roots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })));
});

describe("speculative action resource versions", () => {
	test("requires exact evidence even when a watcher reports no change", async () => {
		const root = await workspace();
		await fs.mkdir(path.join(root, "src"), { recursive: true });
		await Promise.all(
			Array.from({ length: 32 }, (_, index) =>
				fs.writeFile(path.join(root, "src", `value-${index}.ts`), `export const value${index} = ${index}\n`),
			),
		);
		const token = await captureResourceVersion(action("grep", ["src"]), root);
		const result = await validateResourceVersion(token);

		expect(token.captureBytes).toBeGreaterThan(0);
		expect(token.captureFiles).toBe(32);
		expect(result).toMatchObject({ expired: false, mode: "exact", filesRead: 32 });
		expect(result.bytesRead).toBeGreaterThan(0);
	});

	test("expires file content dependencies after a write", async () => {
		const root = await workspace();
		const file = path.join(root, "src", "value.ts");
		await fs.mkdir(path.dirname(file), { recursive: true });
		await fs.writeFile(file, "export const value = 1\n");
		const token = await captureResourceVersion(action("read", ["src/value.ts"]), root);

		await fs.writeFile(file, "export const value = 2\n");
		await settleWatcher();

		const result = await validateResourceVersion(token);
		expect(result.expired).toBe(true);
		expect(result.mode).toBe("watcher");
		expect(result.bytesRead).toBe(0);
	});

	test("expires file content dependencies after atomic replacement", async () => {
		const root = await workspace();
		const file = path.join(root, "value.ts");
		const replacement = path.join(root, "replacement.ts");
		await fs.writeFile(file, "export const value = 1\n");
		const token = await captureResourceVersion(action("read", ["value.ts"]), root);

		await fs.writeFile(replacement, "export const value = 2\n");
		await fs.rename(replacement, file);
		await settleWatcher();

		expect((await validateResourceVersion(token)).expired).toBe(true);
	});

	test("keeps a file dependency valid after an unrelated sibling changes", async () => {
		const root = await workspace();
		const tracked = path.join(root, "tracked.ts");
		const sibling = path.join(root, "sibling.ts");
		await fs.writeFile(tracked, "tracked\n");
		await fs.writeFile(sibling, "one\n");
		const token = await captureResourceVersion(action("read", ["tracked.ts"]), root);

		await fs.writeFile(sibling, "two\n");
		await settleWatcher();

		expect(await validateResourceVersion(token)).toMatchObject({
			expired: false,
			mode: "exact",
			bytesRead: 8,
			filesRead: 1,
		});
	});

	test.runIf(process.platform !== "win32")("fingerprints a symlink target rather than only its link text", async () => {
		const root = await workspace();
		const target = path.join(root, "target.txt");
		const link = path.join(root, "input.txt");
		await fs.writeFile(target, "before");
		await fs.symlink("target.txt", link);
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture(resourceDependencies(action("read", ["input.txt"]), root));

		await fs.writeFile(target, "after!");
		const result = await manager.validate(token);
		manager.close();

		expect(result).toMatchObject({ expired: true, mode: "exact" });
		expect(result.bytesRead).toBe(6);
	});

	test.runIf(process.platform !== "win32")("rejects resource symlinks that escape the workspace", async () => {
		const root = await workspace();
		const outside = await workspace();
		await fs.writeFile(path.join(outside, "secret.txt"), "secret");
		await fs.symlink(path.join(outside, "secret.txt"), path.join(root, "input.txt"));
		const manager = new ResourceVersionManager(root, { watch: false });

		await expect(manager.capture(resourceDependencies(action("read", ["input.txt"]), root))).rejects.toThrow(
			"resource_symlink_escapes_workspace",
		);
		manager.close();
	});

	test.runIf(process.platform === "linux")("rejects special files without opening them", async () => {
		const root = await workspace();
		const fifo = path.join(root, "input.pipe");
		await execFileAsync("mkfifo", [fifo]);
		const manager = new ResourceVersionManager(root, { watch: false });

		await expect(manager.capture(resourceDependencies(action("read", ["input.pipe"]), root))).rejects.toThrow(
			"unsupported_resource_type:fifo",
		);
		manager.close();
	});

	test("preserves path case in cache and watcher identities", () => {
		expect(filesystemPathKey(path.join("root", "Case"))).not.toBe(filesystemPathKey(path.join("root", "case")));
	});

	test("keeps find results for content-only writes and expires them for entry changes", async () => {
		const root = await workspace();
		const file = path.join(root, "src", "value.ts");
		await fs.mkdir(path.dirname(file), { recursive: true });
		await fs.writeFile(file, "one\n");
		const token = await captureResourceVersion(action("find", ["src"]), root);

		await fs.writeFile(file, "two\n");
		await settleWatcher();
		expect((await validateResourceVersion(token)).expired).toBe(false);

		await fs.writeFile(path.join(root, "src", "added.ts"), "added\n");
		await settleWatcher();
		expect((await validateResourceVersion(token)).expired).toBe(true);
	});

	test("keeps ls results for content writes and expires them for entry changes", async () => {
		const root = await workspace();
		const file = path.join(root, "value.ts");
		await fs.writeFile(file, "one\n");
		const token = await captureResourceVersion(action("ls", ["."]), root);

		await fs.writeFile(file, "two\n");
		await settleWatcher();
		expect((await validateResourceVersion(token)).expired).toBe(false);

		await fs.writeFile(path.join(root, "added.ts"), "added\n");
		await settleWatcher();
		expect((await validateResourceVersion(token)).expired).toBe(true);
	});

	test("expires find results when ignore rules change", async () => {
		const root = await workspace();
		await fs.mkdir(path.join(root, "src"), { recursive: true });
		await fs.writeFile(path.join(root, "src", "value.ts"), "one\n");
		await fs.writeFile(path.join(root, ".gitignore"), "generated/\n");
		const token = await captureResourceVersion(action("find", ["src"]), root);

		await fs.writeFile(path.join(root, ".gitignore"), "generated/\nignored/\n");
		await settleWatcher();

		expect((await validateResourceVersion(token)).expired).toBe(true);
	});

	test("tracks grep as a subtree-content dependency", async () => {
		const root = await workspace();
		const file = path.join(root, "src", "value.ts");
		await fs.mkdir(path.dirname(file), { recursive: true });
		await fs.writeFile(file, "needle\n");
		const token = await captureResourceVersion(action("grep", ["src"]), root);

		await fs.writeFile(file, "changed\n");
		await settleWatcher();

		expect((await validateResourceVersion(token)).expired).toBe(true);
	});

	test("derives custom-tool resource evidence from action semantics rather than tool names", async () => {
		const root = await workspace();
		const grep = PI_ACTION_SEMANTICS.definition("grep");
		const write = PI_ACTION_SEMANTICS.definition("write");
		if (!grep || !write) throw new Error("Pi resource semantics unavailable");
		const semantics = new ActionSemanticsRegistry([
			{ ...grep, tool: "custom_query", epoch: "test.custom-query.v1" },
			{ ...write, tool: "custom_write", epoch: "test.custom-write.v1" },
		]);
		const processAction = semantics.buildKey("custom_query", { pattern: "ok", path: "." }, root);
		const writeAction = semantics.buildKey("custom_write", { path: "out.txt", content: "ok" }, root);
		if (!processAction || !writeAction) throw new Error("custom action key unavailable");

		expect(resourceDependencies(processAction, root, semantics)).toEqual([
			{ path: path.resolve(root), scope: "tree_content" },
		]);
		expect(resourceDependencies(writeAction, root, semantics)).toEqual([]);
	});

	test("ignores adoption staging files in tree-scoped dependencies", async () => {
		const root = await workspace();
		await fs.writeFile(path.join(root, "value.ts"), "one\n");
		const token = await captureResourceVersion(action("grep", ["."]), root);
		const temporary = path.join(root, ".pi-speculative-test.tmp");

		await fs.writeFile(temporary, "staged\n");
		await fs.rm(temporary);
		await settleWatcher();

		expect((await validateResourceVersion(token)).expired).toBe(false);
	});

	test("fails closed when a token is absent or exact validation detects a change", async () => {
		expect((await validateResourceVersion(undefined)).expired).toBe(true);

		const root = await workspace();
		const file = path.join(root, "value.ts");
		await fs.writeFile(file, "one\n");
		const key = action("read", ["value.ts"]);
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture(resourceDependencies(key, root));

		await fs.writeFile(file, "two\n");
		const result = await manager.validate(token);
		manager.close();

		expect(result.expired).toBe(true);
		expect(result.mode).toBe("exact");
		expect(result.bytesRead).toBeGreaterThan(0);
	});

	test("notifies active cache owners when a dependency becomes stale", async () => {
		const root = await workspace();
		const file = path.join(root, "value.ts");
		await fs.writeFile(file, "one\n");
		const token = await captureResourceVersion(action("read", ["value.ts"]), root);
		const invalidated = new Promise<string>((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error("resource invalidation timed out")), 3000);
			const release = watchResourceVersion(token, (changedPath) => {
				clearTimeout(timeout);
				release();
				resolve(changedPath);
			});
		});

		await fs.writeFile(file, "two\n");

		expect(path.resolve(await invalidated)).toBe(path.resolve(file));
	});

	test("releases an unwatched token and retires its workspace manager", async () => {
		const root = await workspace();
		await fs.writeFile(path.join(root, "value.txt"), "one\n");
		const key = buildActionKey({
			tool: "bash",
			resources: ["."],
			input: { command: "cat value.txt" },
		});
		const first = await captureResourceVersion(key, root);

		releaseResourceVersion(first);
		const second = await captureResourceVersion(key, root);

		expect(second.manager).not.toBe(first.manager);
		releaseResourceVersion(second);
	});

	test("keeps a shared manager alive until every unwatched token is released", async () => {
		const root = await workspace();
		const key = buildActionKey({
			tool: "bash",
			resources: ["."],
			input: { command: "echo test" },
		});
		const first = await captureResourceVersion(key, root);
		const second = await captureResourceVersion(key, root);

		releaseResourceVersion(first);
		const third = await captureResourceVersion(key, root);

		expect(third.manager).toBe(second.manager);
		releaseResourceVersion(second);
		releaseResourceVersion(third);
	});
});

function action(tool: "read" | "grep" | "find" | "ls", resources: ReadonlyArray<string>) {
	return buildActionKey({
		tool,
		resources,
		input: tool === "read" || tool === "ls" ? { path: resources[0] } : { path: resources[0], pattern: "*" },
	});
}

async function workspace() {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-resource-version-"));
	roots.push(root);
	return root;
}

async function settleWatcher() {
	await new Promise((resolve) => setTimeout(resolve, 80));
}
