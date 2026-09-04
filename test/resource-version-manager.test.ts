import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, test } from "vitest";
import { ActionSemanticsRegistry, buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { filesystemPathKey } from "../src/filesystem-evidence.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
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
	test.each([
		{ name: "watcher available", watch: true },
		{ name: "watcher unavailable", watch: false },
	])("seals only stable Actor observation windows with $name", async ({ watch }) => {
		const scenarios = [
			{ name: "unchanged", mutate: async (_root: string, _file: string) => {}, sealed: true },
			{ name: "A to B", mutate: async (_root: string, file: string) => fs.writeFile(file, "B") },
			{
				name: "A to B to A",
				mutate: async (_root: string, file: string) => {
					await fs.writeFile(file, "B");
					await fs.writeFile(file, "A");
					await fs.utimes(file, new Date(), new Date(Date.now() + 5_000));
				},
			},
			{
				name: "same-content replacement",
				mutate: async (root: string, file: string) => {
					const replacement = path.join(root, "replacement.txt");
					await fs.writeFile(replacement, "A");
					await fs.rename(replacement, file);
				},
			},
			{
				name: "restored directory entries",
				mutate: async (root: string) => {
					const temporary = path.join(root, "temporary.txt");
					await fs.writeFile(temporary, "temporary");
					await fs.rm(temporary);
					await fs.utimes(root, new Date(), new Date(Date.now() + 5_000));
				},
			},
		];
		for (const scenario of scenarios) {
			const root = await workspace();
			const file = path.join(root, "value.txt");
			await fs.writeFile(file, "A");
			const manager = new ResourceVersionManager(root, { watch });
			const target = scenario.name === "restored directory entries" ? ["."] : ["value.txt"];
			const token = await manager.capture(resourceDependencies(action("read", target), root));
			await scenario.mutate(root, file);
			if (watch) await settleWatcher();

			const result = await manager.seal(token);
			expect(result.expired, scenario.name).toBe(!scenario.sealed);
			releaseResourceVersion(token);
			manager.close();
		}
	});

	test("a failed seal discards only the cache capture and releases its manager", async () => {
		const root = await workspace();
		const file = path.join(root, "value.txt");
		await fs.writeFile(file, "A");
		const key = action("read", ["value.txt"]);
		const probe = await captureResourceVersion(key, root);
		const manager = probe.manager;
		const world = createResourceSnapshotExecutionWorld();
		const capture = await world.observation!.capture({
			cwd: root,
			tool: {} as never,
			toolName: "read",
			args: { path: "value.txt" },
			action: key,
			callID: "actor-read",
			signal: new AbortController().signal,
		});
		releaseResourceVersion(probe);
		await fs.writeFile(file, "B");
		await settleWatcher();

		const actorOutput = { result: { content: [{ type: "text" as const, text: "A" }], details: {} }, isError: false };
		await expect(capture.seal(actorOutput)).rejects.toThrow("resource_observation_window_changed");
		expect(actorOutput.result.content[0]?.text).toBe("A");
		const next = await captureResourceVersion(key, root);
		expect(next.manager).not.toBe(manager);
		releaseResourceVersion(next);
	});

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

		expect(result).toMatchObject({ expired: false, mode: "exact", filesRead: 32 });
		expect(result.bytesRead).toBeGreaterThan(0);
	});

	test.each([
		{ change: "write", expired: true },
		{ change: "replace", expired: true },
		{ change: "sibling", expired: false },
	])("validates file content after a $change", async ({ change, expired }) => {
		const root = await workspace();
		const file = path.join(root, "value.ts");
		await fs.writeFile(file, "tracked\n");
		const token = await captureResourceVersion(action("read", ["value.ts"]), root);
		if (change === "replace") {
			const replacement = path.join(root, "replacement.ts");
			await fs.writeFile(replacement, "changed\n");
			await fs.rename(replacement, file);
		} else {
			await fs.writeFile(change === "write" ? file : path.join(root, "sibling.ts"), "changed\n");
		}
		await settleWatcher();

		const result = await validateResourceVersion(token);
		expect(result.expired).toBe(expired);
		expect(result.mode).toBe(expired ? "watcher" : "exact");
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

	test.each([
		{ tool: "find" as const, change: "content", expired: false },
		{ tool: "find" as const, change: "entry", expired: true },
		{ tool: "find" as const, change: "ignore", expired: true },
		{ tool: "ls" as const, change: "content", expired: false },
		{ tool: "ls" as const, change: "entry", expired: true },
		{ tool: "grep" as const, change: "content", expired: true },
		{ tool: "grep" as const, change: "staging", expired: false },
	])("applies $tool tree semantics to $change changes", async ({ tool, change, expired }) => {
		const root = await workspace();
		const file = path.join(root, "src", "value.ts");
		await fs.mkdir(path.dirname(file), { recursive: true });
		await fs.writeFile(file, "one\n");
		await fs.writeFile(path.join(root, ".gitignore"), "generated/\n");
		const token = await captureResourceVersion(action(tool, [change === "staging" ? "." : "src"]), root);
		const changed = change === "content"
			? file
			: change === "ignore"
				? path.join(root, ".gitignore")
				: path.join(root, change === "staging" ? ".pi-speculative-test.tmp" : path.join("src", "added.ts"));
		await fs.writeFile(changed, "changed\n");
		if (change === "staging") await fs.rm(changed);
		await settleWatcher();

		expect((await validateResourceVersion(token)).expired).toBe(expired);
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

	test("keeps a manager until its final token is released, then retires it", async () => {
		const root = await workspace();
		await fs.writeFile(path.join(root, "value.txt"), "one\n");
		const key = buildActionKey({ tool: "bash", resources: ["."], input: { command: "cat value.txt" } });
		const first = await captureResourceVersion(key, root);
		const second = await captureResourceVersion(key, root);
		releaseResourceVersion(first);
		const third = await captureResourceVersion(key, root);
		expect(third.manager).toBe(second.manager);
		releaseResourceVersion(second);
		releaseResourceVersion(third);
		const retired = await captureResourceVersion(key, root);
		expect(retired.manager).not.toBe(first.manager);
		releaseResourceVersion(retired);
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
