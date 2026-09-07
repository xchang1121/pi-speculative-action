import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createFindTool, createGrepTool, createLsTool, createReadTool, createReadToolDefinition, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, test, vi } from "vitest";
import { buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { captureStableFile } from "../src/filesystem-evidence.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import {
	captureResourceVersion,
	closeResourceVersionManagers,
	ResourceReadView,
	ResourceVersionManager,
	releaseResourceVersion,
	resourceDependencies,
	validateResourceVersion,
} from "../src/resource-version.ts";

const roots: string[] = [];
const execFileAsync = promisify(execFile);

afterEach(async () => {
	closeResourceVersionManagers();
	await Promise.all(roots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })));
});

describe("speculative action resource versions", () => {
	test.each([true, false])("seals only stable Actor observation windows (watch=%s)", async (watch) => {
		for (const change of ["unchanged", "sibling", "write", "restore", "replace", "entries"] as const) {
			const root = await workspace({ "value.txt": "A" });
			const file = path.join(root, "value.txt");
			const manager = new ResourceVersionManager(root, { watch });
			const target = change === "entries" ? ["."] : ["value.txt"];
			const token = await manager.capture(resourceDependencies(action(change === "entries" ? "ls" : "read", target), root));
			if (change === "write" || change === "restore") await fs.writeFile(file, "B");
			if (change === "sibling") await fs.writeFile(path.join(root, "sibling"), "B");
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
			const changed = change !== "unchanged" && change !== "sibling";
			expect((await manager.validate(token)).expired, change).toBe(watch ? changed : change === "write");
			expect((await manager.seal(token)).expired, change).toBe(changed);
			releaseResourceVersion(token);
			manager.close();
		}
	});

	test("owns bounded immutable inputs and never converts unproven access into absence", async () => {
		expect((await validateResourceVersion(undefined)).expired).toBe(true);
		const root = await workspace(), file = path.join(root, "value.txt");
		const payload = Buffer.concat([Buffer.alloc(1024 * 1024, 65), Buffer.alloc(1024 * 1024, 66), Buffer.from("end")]);
		await fs.writeFile(file, payload);
		const manager = new ResourceVersionManager(root, { watch: false });
		const dependencies = resourceDependencies(action("read", ["value.txt", "missing"]), root);
		for (const [budget, paths] of [[0, dependencies], [payload.length, dependencies.slice(0, 1)]] as const) {
			const opened = vi.spyOn(fs, "open");
			const observed = await manager.capture(paths, budget);
			expect(opened).toHaveBeenCalledOnce(); opened.mockRestore();
			expect(observed.view).toBeUndefined(); // No partial input authority after either payload or metadata exhaustion.
			expect((await manager.seal(observed)).expired).toBe(false);
			observed.release();
		}
		const token = await manager.capture(dependencies, 3 * 1024 * 1024), view = token.view!;
		await expect(view.evaluate(async (scope) => {
			try { scope.exists(path.join(root, "unknown")); } catch { /* Tool may swallow a failed stat. */ }
		})).rejects.toThrow("resource_access_unproven");
		expect(await view.evaluate((scope) => scope.readFile(file))).toEqual(payload);
		expect((await captureStableFile(file)).hash).toBe(createHash("sha256").update(payload).digest("hex"));
		(await view.readFile(file)).fill(66);
		await fs.writeFile(file, "B");
		expect([await view.readFile(file), view.stat(file).size]).toEqual([payload, payload.length]);
		expect(view.exists(path.join(root, "missing"))).toBe(false);
		expect(() => view.capture(file, { type: "missing" })).toThrow("not_capturing");
		expect(() => view.exists(path.join(root, "unknown"))).toThrow("resource_access_unproven");
		expect(() => view.assertComplete()).toThrow("resource_access_unproven");
		expect((await manager.seal(token)).expired).toBe(true);
		releaseResourceVersion(token);
		await expect(view.readFile(file)).rejects.toThrow("disposed");
		await expect(view.evaluate(async () => "late")).rejects.toThrow("disposed");
		manager.close();
	});

	test("retains payloads across both orders of overlapping metadata captures", async () => {
		for (const reverse of [false, true]) {
			const view = new ResourceReadView(4096), root = path.resolve("sealed"), file = path.join(root, "value");
			const captures = [() => { view.capture(root, { type: "directory", entries: ["value"] }); view.capture(file, { type: "file", content: Buffer.from("A") }); },
				() => { view.capture(root, { type: "directory" }); view.capture(file, { type: "file", content: undefined }); }];
			for (const capture of reverse ? captures.reverse() : captures) capture();
			view.seal();
			expect(view.readdir(root)).toEqual(["value"]);
			expect([(await view.readFile(file)).toString(), view.stat(file).size]).toEqual(["A", 1]);
			expect(() => view.capture(file, { type: "file" })).toThrow("not_capturing");
			view.dispose();
		}
	});

	test("re-evaluates original read arguments over sealed bytes, not parsed output notices", async () => {
		const root = await workspace(), file = path.join(root, "value.txt");
		const text = "first\r\n\n[999 more lines in file. Use offset=3 to continue.]\n" + "x".repeat(60_000) + "\nlast";
		await fs.writeFile(file, text);
		const args = { path: "value.txt", offset: 1, limit: 1 }, native = createReadTool(root);
		const invocation = resolvePiToolInvocation("read", args, { cwd: root, environment: {} })!;
		const key = PI_ACTION_SEMANTICS.buildKey("read", args, root, "", { fingerprint: "original", context: invocation })!;
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 100_000 });
		const signal = new AbortController().signal;
		const branch = await world.speculation!.execute({ cwd: root, tool: native, toolName: "read", args, action: key, callID: "spec", signal });
		try {
			for (const query of [{ path: "@value.txt", offset: 2, limit: 0 }, { path: "value.txt", offset: 3 },
				{ path: "@value.txt", offset: 4, limit: 1 }, { path: file, offset: 5 }]) {
				const action = PI_ACTION_SEMANTICS.buildKey("read", query, root, "", { fingerprint: "original", context: invocation })!;
				expect((await branch.reconstruct!({ action, args: query, callID: "actor", signal }))?.result)
					.toEqual(await native.execute("native", query));
			}
			await expect(branch.reconstruct!({ action: key, args: { path: "unproven" }, callID: "bad", signal })).rejects.toThrow("unproven");
			expect((await branch.validate!()).status).toBe("valid");
			expect((await branch.reconstruct!({ action: key, args, callID: "retry", signal }))?.result).toEqual(await native.execute("native", args));
		} finally { await branch.dispose(); }
	});

	test.each([false, true])("capture and branch share one resource lifetime (changed=%s)", async (changed) => {
		const root = await workspace({ "value.txt": "A" });
		const file = path.join(root, "value.txt");
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
			const invalidated = new Promise<string | undefined>((resolve) => branch.watch?.(resolve));
			await fs.writeFile(file, "B");
			expect(await invalidated).toBe(file);
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

	test.for(["empty", "short", "grow", "shrink", "replace"])("owns descriptor reads through %s", async (change, { skip }) => {
		if (process.platform === "win32" && change === "replace") return skip("Windows denies replacement of the open destination");
		for (const retain of [false, true]) {
			const payload = Buffer.from(change === "empty" ? "" : "initial contents");
			const root = await workspace({ value: payload }), file = path.join(root, "value");
			const handle = await fs.open(file, "r"), read = handle.read.bind(handle);
			const open = vi.spyOn(fs, "open").mockResolvedValueOnce(handle);
			vi.spyOn(handle, "read").mockImplementationOnce((async (buffer: Buffer) => {
				if (change === "grow") await fs.appendFile(file, "more");
				if (change === "shrink") await fs.truncate(file, 1);
				if (change === "replace") { const replacement = path.join(root, "new"); await fs.writeFile(replacement, payload); await fs.rename(replacement, file); }
				return read(buffer, 0, Math.min(3, buffer.byteLength), null);
			}) as typeof handle.read);
			try {
				const capture = captureStableFile(file, Infinity, retain);
				if (["empty", "short"].includes(change)) {
					expect(await capture).toMatchObject({ hash: createHash("sha256").update(payload).digest("hex"), bytesRead: payload.length,
						...(retain ? { content: payload } : {}) });
				} else await expect(capture).rejects.toThrow("file_changed_during_capture");
				expect(handle.fd).toBe(-1);
			} finally { open.mockRestore(); }
		}
	});

	test.for(["file", "directory"] as const)("resolves sealed %s link chains without granting unproven paths", async (kind, { skip }) => {
		if (kind === "file" && process.platform === "win32") return skip("file symlinks require Windows privileges");
		const root = await workspace(), outside = await workspace(), directory = kind === "directory";
		const target = path.join(root, "value.txt"), alias = path.join(root, "alias"), link = path.join(root, "input");
		const tree = path.join(root, "tree"), content = directory ? path.join(tree, "value.txt") : target;
		if (directory) await fs.mkdir(tree);
		await fs.writeFile(content, "before");
		const type = directory ? process.platform === "win32" ? "junction" : "dir" : "file";
		await fs.symlink(directory ? tree : target, link, type); await fs.symlink(link, alias, type);
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture([{ path: alias, scope: directory ? "tree_content" : "content" }], 8192);
		try {
			const name = directory ? "ls" : "read", args = { path: alias };
			const native = directory ? createLsTool(root) : createReadTool(root);
			const actual = await resolvePiToolInvocation(name, args, { cwd: root, environment: {} })!.filesystem!(token.view!,
				{ args, callID: "spec", signal: new AbortController().signal });
			token.view!.assertComplete();
			const expected = await native.execute("actor", args);
			expect(actual.result.content).toEqual(expected.content);
			expect(Object.entries(actual.result.details ?? {})).toEqual(Object.entries(expected.details ?? {}));
			await fs.writeFile(content, "after!");
			expect((await token.view!.readFile(directory ? path.join(alias, "value.txt") : alias)).toString()).toBe("before");
			expect(await manager.validate(token)).toMatchObject({ expired: true, mode: "exact", bytesRead: 6 });
			await fs.writeFile(path.join(outside, "value.txt"), "external");
			const escape = path.join(root, "escape");
			await fs.symlink(directory ? outside : path.join(outside, "value.txt"), escape, type);
			await expect(manager.capture([{ path: escape, scope: "tree_content" }], 8192)).rejects.toThrow("resource_symlink_escapes_workspace");
			expect(() => token.view!.exists(path.join(alias, "unproven"))).toThrow("resource_access_unproven");
		} finally { token.release(); manager.close(); }
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

});

function action(tool: string, resources: ReadonlyArray<string>) {
	return buildActionKey({
		tool,
		resources,
		input: tool === "read" || tool === "ls" ? { path: resources[0] } : { path: resources[0], pattern: "*" },
	});
}

async function workspace(files: Readonly<Record<string, string | Buffer>> = {}) {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-resource-version-"));
	roots.push(root);
	await Promise.all(Object.entries(files).map(([name, content]) => fs.writeFile(path.join(root, name), content)));
	return root;
}

async function settleWatcher() {
	await new Promise((resolve) => setTimeout(resolve, 80));
}
