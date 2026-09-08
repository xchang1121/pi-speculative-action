import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
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
	ResourceVersionManager,
	releaseResourceVersion,
	resourceDependencies,
} from "../src/resource-version.ts";

const roots: string[] = [];
const execFileAsync = promisify(execFile);

afterEach(async () => {
	closeResourceVersionManagers();
	await Promise.all(roots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })));
});

describe("speculative action resource versions", () => {
	test.each([true, false])("seals eager observations and on-demand inputs (watch=%s)", async (watch) => {
		for (const onDemand of [false, true]) for (const change of ["unchanged", "sibling", "write", "restore", "replace", "entries"] as const) {
			const root = await workspace({ "value.txt": "A" });
			const file = path.join(root, "value.txt");
			const manager = new ResourceVersionManager(root, { watch });
			const target = change === "entries" ? ["."] : ["value.txt"];
			const token = await manager.capture(onDemand ? undefined : resourceDependencies(action(change === "entries" ? "ls" : "read", target), root), 8192);
			if (onDemand) {
				expect((await manager.validate(token)).expired).toBe(true); // Open capture is never an adoptable certificate.
				const value = change === "entries" ? root : file, view = token.view!;
				const inputs = [() => view.stat(value), () => change === "entries" ? view.readdir(value) : view.readFile(value)];
				for (const capture of watch ? inputs : inputs.reverse()) await capture();
				expect(change === "entries" ? await view.readdir(root) : (await view.readFile(file)).toString()).toEqual(change === "entries" ? ["value.txt"] : "A");
			}
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
			token.view!.seal();
			expect((await manager.validate(token)).expired, change).toBe(change === "write");
			expect((await manager.seal(token)).expired, change).toBe(onDemand || process.platform === "win32" || change !== "unchanged");
			const released = manager.validate(token);
			releaseResourceVersion(token);
			expect((await released).expired).toBe(true); // Release during validation retires the evidence, not just its payload.
			manager.close();
		}
	});

	test("owns bounded immutable inputs and never converts unproven access into absence", async () => {
		const payload = Buffer.concat([Buffer.alloc(1024 * 1024, 65), Buffer.alloc(1024 * 1024, 66), Buffer.from("end")]);
		const root = await workspace({ "value.txt": payload }), file = path.join(root, "value.txt");
		const manager = new ResourceVersionManager(root, { watch: false });
		const dependencies = resourceDependencies(action("read", ["value.txt", "missing"]), root);
		for (const [budget, paths] of [[0, dependencies], [payload.length, dependencies.slice(0, 1)]] as const) {
			const opened = vi.spyOn(fs, "open");
			const observed = await manager.capture(paths, budget);
			expect(opened).toHaveBeenCalledOnce(); opened.mockRestore();
			expect(observed.view).toBeUndefined(); // No partial input authority after either payload or metadata exhaustion.
			expect((await manager.validate(observed)).expired).toBe(false);
			observed.release();
		}
		const token = await manager.capture(dependencies, 3 * 1024 * 1024), view = token.view!;
		await expect(view.evaluate(async (scope) => {
			try { await scope.exists(path.join(root, "unknown")); } catch { /* Tool may swallow a failed stat. */ }
		})).rejects.toThrow("resource_access_unproven");
		expect(await view.evaluate((scope) => scope.readFile(file))).toEqual(payload);
		(await view.readFile(file)).fill(66);
		await fs.writeFile(file, "B");
		expect([await view.readFile(file), (await view.stat(file)).size, (await view.stat(file, "type")).size]).toEqual([payload, payload.length, undefined]);
		expect(await view.exists(path.join(root, "missing"))).toBe(false);
		expect(() => view.capture(file, { type: "missing" })).toThrow("not_capturing");
		await expect(view.exists(path.join(root, "unknown"))).rejects.toThrow("resource_access_unproven");
		expect(() => view.assertComplete()).toThrow("resource_access_unproven");
		expect((await manager.seal(token)).expired).toBe(true);
		releaseResourceVersion(token);
		await expect(view.readFile(file)).rejects.toThrow("disposed");
		await expect(view.evaluate(async () => "late")).rejects.toThrow("disposed");
		manager.close();
	});

	test("coalesces capture of one input but never seals or leaks a pending read", async () => {
		const root = await workspace({ value: "A" }), manager = new ResourceVersionManager(root, { watch: false });
		let notify!: () => void, resume!: () => void;
		const entered = new Promise<void>((resolve) => { notify = resolve; }), gate = new Promise<void>((resolve) => { resume = resolve; });
		const handle = await fs.open(path.join(root, "value"), "r");
		const open = vi.spyOn(fs, "open").mockImplementationOnce(async () => { notify(); await gate; return handle; });
		const token = await manager.capture(undefined, 8192), read = () => token.view!.readFile(path.join(root, "value"));
		const pending = Promise.allSettled([read(), read()]);
		try {
			await entered;
			expect((await manager.seal(token)).expired).toBe(true);
			token.release(); resume();
			expect((await pending).map((entry) => entry.status)).toEqual(["rejected", "rejected"]);
			expect(handle.fd).toBe(-1); expect(open).toHaveBeenCalledOnce();
		} finally { resume(); await pending; token.release(); open.mockRestore(); manager.close(); }
	});

	test("re-evaluates original read arguments over sealed bytes, not parsed output notices", async () => {
		const text = "first\r\n\n[999 more lines in file. Use offset=3 to continue.]\n" + "x".repeat(60_000) + "\nlast";
		const root = await workspace({ "value.txt": text }), file = path.join(root, "value.txt");
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

	test.runIf(process.platform !== "win32")("capture and branch share one resource lifetime", async () => {
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
		const branch = await capture.seal(actorOutput);
		await capture.dispose(); // A sealed capture no longer owns the token.
		expect(await branch.commit()).toBe(actorOutput);
		await fs.writeFile(file, "B");
		expect((await branch.validate!()).status).toBe("stale");
		await fs.writeFile(file, "A"); expect((await branch.validate!()).status).toBe("valid");
		await branch.dispose(); await branch.dispose();
		expect((await branch.validate?.())?.status).toBe("stale");
		await expect(branch.commit()).rejects.toThrow("disposed");
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
		const root = await workspace({ "value.ts": "alpha\n" }), config = await workspace({ [name === "grep" ? "rg" : "fd/ignore"]: "" });
		const configuration = path.join(config, name === "grep" ? "rg" : "fd/ignore");
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

	test.for([["empty", "short"], ["admission"], ["grow", "shrink"], ["replace"], ["seal"]])("owns the file identity through %j", async (changes, { skip }) => {
		const lstat = fs.lstat.bind(fs);
		if (process.platform === "win32" && changes.includes("replace")) return skip("Windows denies replacement of the open destination");
		for (const change of changes) for (const retain of [false, true]) {
			const payload = Buffer.from(change === "empty" ? "" : "initial contents");
			const root = await workspace({ value: payload }), file = path.join(root, "value");
			const handle = await fs.open(file, "r"), read = handle.read.bind(handle);
			const open = vi.spyOn(fs, "open").mockImplementationOnce(async () => {
				if (change === "admission") await fs.appendFile(file, "more");
				return handle;
			});
			const inspect = vi.spyOn(fs, "lstat").mockImplementationOnce(lstat).mockImplementationOnce((async (...args: Parameters<typeof fs.lstat>) => {
				if (change === "seal") await fs.appendFile(file, "more"); // After the final fstat, before the path proof.
				return lstat(...args);
			}) as typeof fs.lstat);
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
				if (change === "admission") expect(handle.read).not.toHaveBeenCalled();
				expect(handle.fd).toBe(-1);
			} finally { open.mockRestore(); inspect.mockRestore(); }
		}
	});

	test.for(["file", "directory"] as const)("resolves sealed %s link chains without granting unproven paths", async (kind, { skip }) => {
		if (kind === "file" && process.platform === "win32") return skip("file symlinks require Windows privileges");
		const directory = kind === "directory", root = await workspace({ [directory ? "tree/value.txt" : "value.txt"]: "before" }), outside = await workspace({ "value.txt": "external" });
		const target = path.join(root, "value.txt"), alias = path.join(root, "alias"), link = path.join(root, "input");
		const tree = path.join(root, "tree"), content = directory ? path.join(tree, "value.txt") : target;
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
			expect(await token.view!.stat(alias, "entry")).toMatchObject({ type: "symlink", link: await fs.readlink(alias) });
			expect((await token.view!.stat(directory ? path.join(alias, "value.txt") : target, "entry")).type).toBe("file");
			const expected = await native.execute("actor", args);
			expect(actual.result.content).toEqual(expected.content);
			expect(Object.entries(actual.result.details ?? {})).toEqual(Object.entries(expected.details ?? {}));
			const leaf = directory ? path.join(alias, "value.txt") : alias, parked = path.join(root, "parked");
			const observed = await manager.capture([{ path: leaf, scope: "content" }]);
			await fs.rename(link, parked);
			try {
				await fs.symlink(directory ? outside : path.join(outside, "value.txt"), link, type);
				expect(await fs.readFile(leaf, "utf8")).toBe("external"); // Actual Actor sees B, not the captured A.
			} finally { await fs.rm(link, { force: true }); await fs.rename(parked, link); }
			try {
				expect(await fs.readFile(leaf, "utf8")).toBe("before");
				expect((await manager.seal(observed)).expired).toBe(true); // Restoring the SAME junction can preserve Windows inode/ctime.
			} finally { observed.release(); }
			await fs.writeFile(content, "after!");
			expect((await token.view!.readFile(directory ? path.join(alias, "value.txt") : alias)).toString()).toBe("before");
			expect(await manager.validate(token)).toMatchObject({ expired: true, mode: "exact", bytesRead: 6 });
			const escape = path.join(root, "escape");
			await fs.symlink(directory ? outside : path.join(outside, "value.txt"), escape, type);
			await expect(manager.capture([{ path: escape, scope: "tree_content" }], 8192)).rejects.toThrow("resource_symlink_escapes_workspace");
			for (const destination of [directory ? outside : path.join(outside, "value.txt"), path.join(root, "missing"), escape]) {
				await fs.rm(escape); await fs.symlink(destination, escape, type);
				const metadata = await manager.capture(undefined, 8192), opened = vi.spyOn(fs, "open");
				try {
					expect(await metadata.view!.stat(escape, "entry")).toMatchObject({ type: "symlink", link: await fs.readlink(escape), size: undefined });
					metadata.view!.seal();
					expect(await manager.validate(metadata)).toMatchObject({ expired: false, filesRead: 0, bytesRead: 0 });
					await expect(metadata.view!.evaluate((view) => view.readFile(escape))).rejects.toThrow("resource_access_unproven");
					await fs.rm(escape); await fs.symlink(path.join(root, "changed"), escape, type);
					expect((await manager.validate(metadata)).expired).toBe(true);
					expect(opened).not.toHaveBeenCalled();
				} finally { opened.mockRestore(); metadata.release(); }
			}
			await expect(token.view!.exists(path.join(alias, "unproven"))).rejects.toThrow("resource_access_unproven");
		} finally { token.release(); manager.close(); }
	});

	test("rejects known non-regular paths before opening a data descriptor", async () => {
		const root = await workspace(), manager = new ResourceVersionManager(root, { watch: false }), paths = [root];
		if (process.platform === "linux") {
			const fifo = path.join(root, "input.pipe"); await execFileAsync("mkfifo", [fifo]); paths.push(fifo);
		}
		const open = vi.spyOn(fs, "open");
		try {
			for (const target of paths) {
				const metadata = await manager.capture([{ path: target, scope: "entry" }], 4096);
				try { expect((await metadata.view!.stat(target, "entry")).type).toBe(target === root ? "directory" : "special"); }
				finally { metadata.release(); }
				await expect(captureStableFile(target)).rejects.toThrow("not_regular_file");
				await expect(manager.capture([{ path: target, scope: "content" }])).rejects.toThrow("unsupported_resource_type:");
			}
			expect(open).not.toHaveBeenCalled();
		} finally { open.mockRestore(); manager.close(); }
	});

	test.each([
		{ scope: "entry" as const, stale: ["kind"] },
		{ scope: "type" as const, stale: ["kind"] },
		{ scope: "stat" as const, stale: ["content", "kind"] },
		{ scope: "entries" as const, stale: ["entry", "kind"] },
		{ scope: "tree_entries" as const, stale: ["entry", "deep", "kind"] },
		{ scope: "tree_content" as const, stale: ["content", "entry", "deep", "kind"] },
	])("validates exactly the declared $scope, without guessing configuration paths", async ({ scope, stale }) => {
		for (const [change, relative] of Object.entries({ content: "src/value.ts", entry: "src/added.ts",
			deep: "src/nested/added.ts", outside: ".gitignore", kind: "src/value.ts" })) {
			const root = await workspace({ "src/value.ts": "one\n", "src/nested/existing.ts": "" });
			const manager = new ResourceVersionManager(root, { watch: false });
			const token = await manager.capture([{ path: ["entry", "type", "stat"].includes(scope) ? "src/value.ts" : "src", scope }], 4096);
			if (["entry", "type"].includes(scope)) await expect(token.view!.evaluate((view) => view.stat(path.join(root, "src/value.ts")))).rejects.toThrow("unproven");
			if (change === "kind") { await fs.rm(path.join(root, relative)); await fs.mkdir(path.join(root, relative)); }
			else await fs.writeFile(path.join(root, relative), "changed\n");
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

function action(tool: "read" | "ls", resources: ReadonlyArray<string>) {
	return buildActionKey({ tool, resources, input: { path: resources[0] } });
}

async function workspace(files: Readonly<Record<string, string | Buffer>> = {}) {
	const root = await fs.mkdtemp(path.join(process.cwd(), "test", "pi-resource-version-"));
	roots.push(root);
	await Promise.all(Object.entries(files).map(async ([name, content]) => {
		await fs.mkdir(path.dirname(path.join(root, name)), { recursive: true });
		await fs.writeFile(path.join(root, name), content);
	}));
	return root;
}
