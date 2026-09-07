import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createFindTool, createGrepTool, createReadTool, createReadToolDefinition, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, test, vi } from "vitest";
import { ActionSemanticsRegistry, buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { captureStableFile } from "../src/filesystem-evidence.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
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
	test.each([true, false])("seals only stable Actor observation windows (watch=%s)", async (watch) => {
		for (const change of ["unchanged", "write", "restore", "replace", "entries"] as const) {
			const root = await workspace();
			const file = path.join(root, "value.txt");
			await fs.writeFile(file, "A");
			const manager = new ResourceVersionManager(root, { watch });
			const target = change === "entries" ? ["."] : ["value.txt"];
			const token = await manager.capture(resourceDependencies(action(change === "entries" ? "ls" : "read", target), root));
			if (change === "write" || change === "restore") await fs.writeFile(file, "B");
			if (change === "restore") {
				await fs.writeFile(file, "A");
				await fs.utimes(file, new Date(), new Date(Date.now() + 5_000));
			}
			if (change === "replace" || change === "entries") {
				const temporary = path.join(root, "temporary.txt");
				await fs.writeFile(temporary, "A");
				if (change === "replace") await fs.rename(temporary, file);
				else {
					await fs.rm(temporary);
					await fs.utimes(root, new Date(), new Date(Date.now() + 5_000));
				}
			}
			if (watch) await settleWatcher();
			expect((await manager.seal(token)).expired, change).toBe(change !== "unchanged");
			releaseResourceVersion(token);
			manager.close();
		}
	});

	test("owns bounded immutable inputs and never converts unproven access into absence", async () => {
		const root = await workspace(), file = path.join(root, "value.txt");
		await fs.writeFile(file, "A");
		const manager = new ResourceVersionManager(root, { watch: false });
		const dependencies = resourceDependencies(action("read", ["value.txt", "missing"]), root);
		await expect(manager.capture(dependencies, 0)).rejects.toThrow("resource_snapshot_budget_exceeded");
		const token = await manager.capture(dependencies, 4096), view = token.view!;
		(await view.readFile(file)).fill(66);
		await fs.writeFile(file, "B");
		expect((await view.readFile(file)).toString()).toBe("A");
		expect(view.exists(path.join(root, "missing"))).toBe(false);
		expect(() => view.capture(file, { type: "missing" })).toThrow("not_capturing");
		expect(() => view.exists(path.join(root, "unknown"))).toThrow("resource_access_unproven");
		expect(() => view.assertComplete()).toThrow("resource_access_unproven");
		expect((await manager.seal(token)).expired).toBe(true);
		releaseResourceVersion(token);
		await expect(view.readFile(file)).rejects.toThrow("disposed");
		manager.close();
	});

	test.each([false, true])("capture and branch share one resource lifetime (changed=%s)", async (changed) => {
		const root = await workspace();
		const file = path.join(root, "value.txt");
		await fs.writeFile(file, "A");
		const key = action("read", ["value.txt"]);
		const probe = await captureResourceVersion(key, root);
		const manager = probe.manager;
		const world = createResourceSnapshotExecutionWorld();
		const capture = await world.observation!.capture({
			cwd: root, tool: {} as never, toolName: "read", args: { path: "value.txt" },
			action: key, callID: "actor-read", signal: new AbortController().signal,
		});
		const actorOutput = { result: { content: [{ type: "text" as const, text: "A" }], details: {} }, isError: false };
		if (changed) {
			await fs.writeFile(file, "B");
			await settleWatcher();
			await expect(capture.seal(actorOutput)).rejects.toThrow("resource_observation_window_changed");
		} else {
			const branch = await capture.seal(actorOutput);
			await capture.dispose(); // A sealed capture no longer owns the token.
			expect(await branch.commit()).toBe(actorOutput);
			branch.watch?.(() => {});
			await branch.dispose();
			await branch.dispose();
			expect((await branch.validate?.())?.status).toBe("stale");
			await expect(branch.commit()).rejects.toThrow("disposed");
		}
		await expect(capture.seal(actorOutput)).rejects.toThrow("already consumed");
		expect(actorOutput.result.content[0]?.text).toBe("A");
		const next = await captureResourceVersion(key, root);
		expect(next.manager).toBe(manager); // The unrelated probe still owns a reference.
		releaseResourceVersion(probe);
		releaseResourceVersion(next);
		const retired = await captureResourceVersion(key, root);
		expect(retired.manager).not.toBe(manager);
		releaseResourceVersion(retired);
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
		{ scope: "entries" as const, stale: ["entry"] },
		{ scope: "tree_entries" as const, stale: ["entry", "deep"] },
		{ scope: "tree_content" as const, stale: ["content", "entry", "deep"] },
	])("validates exactly the declared $scope, without guessing configuration paths", async ({ scope, stale }) => {
		for (const [change, relative] of Object.entries({ content: "src/value.ts", entry: "src/added.ts",
			deep: "src/nested/added.ts", outside: ".gitignore", restore: "src/transient" })) {
			const root = await workspace();
			await fs.mkdir(path.join(root, "src/nested"), { recursive: true });
			await fs.writeFile(path.join(root, "src/value.ts"), "one\n");
			const manager = new ResourceVersionManager(root, { watch: false });
			const token = await manager.capture([{ path: "src", scope }]);
			await fs.writeFile(path.join(root, relative), "changed\n");
			if (change === "restore") await fs.rm(path.join(root, relative));
			expect((await manager.validate(token)).expired, change).toBe(stale.includes(change));
			releaseResourceVersion(token);
			manager.close();
		}
	});

	test("executes the stock image renderer with the captured vision and resizing identity", async () => {
		const root = await workspace(), args = { path: "pixel.gif" };
		await fs.writeFile(path.join(root, args.path), Buffer.from("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7", "base64"));
		const token = await captureResourceVersion(PI_ACTION_SEMANTICS.buildKey("read", args, root)!, root, PI_ACTION_SEMANTICS, 4096);
		try {
			for (const modelSupportsImages of [true, false]) for (const autoResizeImages of [true, false]) {
				const invocation = resolvePiToolInvocation("read", args, { cwd: root, environment: {}, modelSupportsImages, autoResizeImages })!;
				const context = { model: { input: modelSupportsImages ? ["image"] : [] } } as ExtensionContext;
				const expected = await createReadToolDefinition(root, { autoResizeImages }).execute("actor", args, undefined, undefined, context);
				const output = await invocation.filesystem!(token.view!, { args, callID: "speculate", signal: new AbortController().signal });
				expect(expected.content.some((item) => item.type === "image")).toBe(true);
				expect(output.result).toEqual(expected);
			}
		} finally { releaseResourceVersion(token); }
	});

	test("derives custom-tool resource evidence from action semantics rather than tool names", async () => {
		const root = await workspace();
		const grep = { ...PI_ACTION_SEMANTICS.definition("ls")!, resourceScope: "tree_content" as const };
		const write = PI_ACTION_SEMANTICS.definition("write")!;
		const semantics = new ActionSemanticsRegistry([
			{ ...grep, tool: "custom_query", epoch: "test.custom-query.v1" },
			{ ...write, tool: "custom_write", epoch: "test.custom-write.v1" },
		]);
		const processAction = semantics.buildKey("custom_query", { pattern: "ok", path: "." }, root)!;
		const writeAction = semantics.buildKey("custom_write", { path: "out.txt", content: "ok" }, root)!;

		expect(resourceDependencies(processAction, root, semantics)).toEqual([
			{ path: path.resolve(root), scope: "tree_content" },
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
