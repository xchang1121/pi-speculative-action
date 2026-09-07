import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createFindTool, createGrepTool, createReadTool } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, test, vi } from "vitest";
import { ActionSemanticsRegistry, buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { captureStableFile } from "../src/filesystem-evidence.ts";
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

	test.runIf(process.platform === "linux").each(["grep", "find"] as const)("does not certify %s from workspace-only evidence", async (name) => {
		const root = await workspace(), config = await workspace();
		await fs.mkdir(path.join(config, "fd"));
		const configuration = path.join(config, name === "grep" ? "rg" : "fd/ignore");
		await fs.writeFile(configuration, "");
		await fs.writeFile(path.join(root, "value.ts"), "alpha\n");
		vi.stubEnv("XDG_CONFIG_HOME", config);
		vi.stubEnv("RIPGREP_CONFIG_PATH", path.join(config, "rg"));
		try {
			const args = { path: ".", pattern: name === "grep" ? "alpha" : "*.ts" };
			const tool = name === "grep" ? createGrepTool(root) : createFindTool(root);
			const before = await tool.execute("before", args);
			await expect(captureResourceVersion(PI_ACTION_SEMANTICS.buildKey(name, args, root)!, root))
				.rejects.toThrow("resource_dependencies_unproven");
			await fs.writeFile(configuration, name === "grep" ? "--glob\n!value.ts\n" : "value.ts\n");
			expect((await tool.execute("after", args)).content).not.toEqual(before.content);
		} finally { vi.unstubAllEnvs(); }
	});

	test.each([
		{ change: "write", expired: true },
		{ change: "replace", expired: true },
		{ change: "sibling", expired: false },
	].flatMap((scenario) => [true, false].map((watch) => ({ ...scenario, watch }))))(
	"validates file content after a $change (watch=$watch)", async ({ change, expired, watch }) => {
		expect((await validateResourceVersion(undefined)).expired).toBe(true);
		const root = await workspace();
		const file = path.join(root, "value.ts");
		await fs.writeFile(file, "tracked\n");
		const manager = new ResourceVersionManager(root, { watch });
		const args = { path: "@value.ts" };
		const key = PI_ACTION_SEMANTICS.buildKey("read", args, root)!;
		const token = await manager.capture(resourceDependencies(key, root));
		expect((await createReadTool(root).execute("actor", args)).content).toMatchObject([{ text: "tracked\n" }]);
		if (change === "replace") {
			const replacement = path.join(root, "replacement.ts");
			await fs.writeFile(replacement, "changed\n");
			await fs.rename(replacement, file);
		} else {
			await fs.writeFile(change === "write" ? file : path.join(root, "sibling.ts"), "changed\n");
		}
		if (watch) await settleWatcher();

		const result = await validateResourceVersion(token);
		expect(result.expired).toBe(expired);
		expect(result.mode).toBe(expired && watch ? "watcher" : "exact");
		releaseResourceVersion(token);
		manager.close();
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
		await expect(captureStableFile(fifo)).rejects.toThrow("not_regular_file");
		const manager = new ResourceVersionManager(root, { watch: false });

		await expect(manager.capture(resourceDependencies(action("read", ["input.pipe"]), root))).rejects.toThrow(
			"unsupported_resource_type:fifo",
		);
		manager.close();
	});

	test.each([
		{ scope: "tree_query" as const, change: "content", expired: false },
		{ scope: "tree_query" as const, change: "entry", expired: true },
		{ scope: "tree_query" as const, change: "ignore", expired: true },
		{ scope: "tree_query" as const, change: "fdignore", expired: true },
		{ scope: "tree_entries" as const, change: "content", expired: false },
		{ scope: "tree_entries" as const, change: "entry", expired: true },
		{ scope: "tree_entries" as const, change: "git", expired: true },
		{ scope: "tree_content" as const, change: "content", expired: true },
		{ scope: "tree_content" as const, change: "ignore", expired: true },
		{ scope: "tree_content" as const, change: "staging", expired: false },
	])("applies $scope evidence to $change changes", async ({ scope, change, expired }) => {
		const root = await workspace();
		const file = path.join(root, "src", "value.ts");
		await fs.mkdir(path.dirname(file), { recursive: true });
		await fs.writeFile(file, "one\n");
		await fs.writeFile(path.join(root, ".gitignore"), "generated/\n");
		await fs.writeFile(path.join(root, "src", ".fdignore"), "hidden.ts\n");
		const manager = new ResourceVersionManager(root, { watch: false });
		const semantics = new ActionSemanticsRegistry([{ ...PI_ACTION_SEMANTICS.definition("ls")!, tool: "query", resourceScope: scope }]);
		const token = await manager.capture(resourceDependencies(action("query", [change === "staging" ? "." : "src"]), root, semantics));
		const changed = change === "git" ? path.join(root, "src", ".git") : change === "content"
			? file
			: change === "ignore"
				? path.join(root, ".gitignore")
				: change === "fdignore" ? path.join(root, "src", ".fdignore")
				: path.join(root, change === "staging" ? ".pi-speculative-test.tmp" : path.join("src", "added.ts"));
		await fs.writeFile(changed, "changed\n");
		if (change === "staging") await fs.rm(changed);
		try {
			expect((await manager.validate(token)).expired).toBe(expired);
		} finally {
			releaseResourceVersion(token);
			manager.close();
		}
	});

	test("derives custom-tool resource evidence from action semantics rather than tool names", async () => {
		const root = await workspace();
		const grep = { ...PI_ACTION_SEMANTICS.definition("ls")!, resourceScope: "tree_content" as const };
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
			{ path: path.join(root, ".git", "info", "exclude"), scope: "content" },
		]);
		expect(resourceDependencies(writeAction, root, semantics)).toEqual([]);
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
		const key = action("read", ["value.txt"]);
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

function action(tool: string, resources: ReadonlyArray<string>) {
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
