import { gated, deferred, nextTurn } from "./async.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { constants } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import { createFindTool, createGrepTool, createLsTool, createReadTool, createReadToolDefinition, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, test, vi } from "vitest";
import { buildActionKey, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { borrowResourceObject, createCommittedResourceInputs, createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { captureHeldDescriptorInputs } from "../src/linux-held-exec.ts";
import { captureStableFile, hashExecutableFile } from "../src/filesystem-evidence.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import {
	captureResourceVersion,
	invalidateResourceInputs,
	closeResourceVersionManagers,
	fingerprintIO,
	ResourceVersionManager,
	type ResourceVersionToken,
	type ResourceInput,
	releaseResourceVersion,
	resourceDependencies,
} from "../src/resource-version.ts";

const directories = temporaryDirectories("pi-resource-version-", path.join(process.cwd(), "test"));
const execFileAsync = promisify(execFile);
const isDataOpen = (flags: unknown) => typeof flags !== "number" || !(flags & 0x200000); // Linux O_PATH has no I/O authority.

function directoryImage(names: readonly string[]): Buffer {
	return Buffer.concat([".", "..", ...names].map((name, index) => {
		const record = Buffer.alloc(Math.ceil((20 + Buffer.byteLength(name)) / 8) * 8);
		record.writeBigUInt64LE(BigInt(index + 1)); record.writeBigInt64LE(BigInt(index + 1), 8);
		record.writeUInt16LE(record.length, 16); record[18] = index < 2 ? 4 : 8; record.write(name, 19); return record;
	}));
}

afterEach(async () => {
	closeResourceVersionManagers();
	await directories.dispose();
});

describe("speculative action resource versions", () => {
	test("serves adoption validation I/O ahead of queued whole-tree work", async () => {
		const root = await directories.create(), manager = new ResourceVersionManager(root), file = path.join(root, "file.txt"), order: string[] = [];
		await fs.writeFile(file, "same");
		const token = await manager.capture([{ path: file, scope: "content" }]), gate = deferred<void>();
		const held = Array.from({ length: 12 }, () => fingerprintIO(() => gate.promise));
		const queued = Array.from({ length: 80 }, (_, index) => fingerprintIO(async () => { order.push(`scan:${index}`); await new Promise(resolve => setTimeout(resolve, 5)); }));
		const validation = manager.validate(token).then((result) => { order.push("validated"); return result; });
		gate.resolve();
		expect((await validation).expired).toBe(false);
		await Promise.all([...held, ...queued]);
		expect(order.indexOf("validated")).toBeLessThan(60); // A few slot turnovers, not the whole queue.
		await token.release();
	});

	test("binds hardlink topology even when every name, byte and link count stays equal", async () => {
		const root = await directories.create(), manager = new ResourceVersionManager(root);
		const named = (name: string) => path.join(root, name);
		await fs.writeFile(named("a"), "same"); await fs.writeFile(named("c"), "same");
		await fs.link(named("a"), named("b")); await fs.link(named("c"), named("d"));
		const first = await manager.capture([{ path: root, scope: "tree_content" }]);
		let second: ResourceVersionToken | undefined;
		try {
			const observation = [...first.observations.values()].find(value => value.scope === "tree_content")!;
			expect(observation.aliases?.map(group => group.paths.map(name => path.basename(name)))).toEqual([["a", "b"], ["c", "d"]]);
			await fs.unlink(named("b")); await fs.unlink(named("d"));
			await fs.link(named("c"), named("b")); await fs.link(named("a"), named("d"));
			second = await manager.capture([{ path: root, scope: "tree_content" }]);
			expect([...second.observations.values()].find(value => value.scope === "tree_content")!.fingerprint).not.toBe(observation.fingerprint);
			expect((await manager.validate(first)).expired).toBe(true);
		} finally { await first.release(); await second?.release(); manager.close(); }
	});

	test.for((["retain", "cancel", "budget", "outside"] as const).flatMap(mode => (["file", "directory"] as const).map(type => ({ mode, type }))))(
		"owns passive process input pins through $mode ($type)", async ({ mode, type }, { skip }) => {
		if (process.platform !== "linux") return skip("Linux object pins");
		const root = await workspace({ data: "shared" }), outside = await workspace({ data: "outside" });
		const file = path.join(mode === "outside" ? outside : root, type === "file" ? "data" : ""), args = { command: "worker" };
		const content = type === "file" ? Buffer.from(mode === "outside" ? "outside" : "shared") : directoryImage(["data"]);
		const invocation = resolvePiToolInvocation("bash", args, { cwd: root, environment: {} })!;
		const action = PI_ACTION_SEMANTICS.buildKey("bash", args, root, "", { fingerprint: "process", context: invocation })!;
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => mode === "budget" ? 0 : 65536 });
		const capture = await world.observation!.capture({ cwd: root, tool: createReadTool(root), toolName: "bash", args, action,
			callID: "capture", signal: new AbortController().signal });
		const handle = await fs.open(file, "r"), metadata = await handle.stat({ bigint: true });
		const sources = [capture.inputSource!], opened = vi.spyOn(fs, "open"), handles: import("node:fs/promises").FileHandle[] = [];
		let branch: Awaited<ReturnType<typeof capture.seal>> | undefined;
		try {
			expect(capture.inputsOnly).toBe(true);
			for (const repeated of [false, true]) {
				if (mode === "cancel" && repeated) await capture.dispose();
				opened.mockClear();
				await captureHeldDescriptorInputs(process.pid, [{ fd: handle.fd, alias: handle.fd, flags: 0, offset: 0,
					device: String(metadata.dev), inode: String(metadata.ino), owned: true,
					...(type === "directory" ? { type, directoryHex: content.toString("hex") } : {}) }], 1024, [], () => sources);
				expect(opened).toHaveBeenCalledTimes(mode === "retain" && repeated ? 0 : type === "file" ? 2 : 1);
				handles.push(...await Promise.all(opened.mock.results.map(result => result.value)));
			}
			const output = { result: { content: [], details: {} }, isError: false };
			if (mode !== "retain") { await expect(capture.seal(output)).rejects.toThrow(); return; }
			branch = await capture.seal(output);
			expect(branch.inputsOnly).toBe(true); await expect(branch.commit()).rejects.toThrow("input_only_branch");
			expect(await borrowResourceObject(sources, file, metadata, 1024)).toBeUndefined();
			expect((await borrowResourceObject([branch.inputSource!], file, metadata, 1024))?.content).toEqual(content);
			expect(await branch.validate!()).toMatchObject({ status: "valid" });
			await fs.writeFile(type === "directory" ? path.join(file, "new") : file, "mutate"); expect(await branch.validate!()).toMatchObject({ status: "stale" });
			await branch.dispose(); expect(await borrowResourceObject([branch.inputSource!], file, metadata, 1024)).toBeUndefined();
		} finally {
			opened.mockRestore(); await handle.close(); await branch?.dispose(); await capture.dispose();
			expect(handles.every(handle => handle.fd === -1), "every retained or declined kernel pin is closed").toBe(true);
		}
	});

	test.skipIf(process.platform !== "linux")("promotes a directory pin only into an equal pre-budgeted names view", async () => {
		const root = await workspace({ "a": "one", "\ue000": "two", "\u{10000}": "three" });
		const names = await fs.readdir(root), content = directoryImage(names);
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture([{ path: root, scope: "names" }], 8192), bytes = token.view!.bytes;
		const handle = await fs.open(root, "r"), metadata = await handle.stat({ bigint: true });
		const { captureHeldDirectory } = await import("../src/filesystem-evidence.ts");
		let pin: import("node:fs/promises").FileHandle | undefined;
		try {
			const forged = await captureHeldDirectory(process.pid, handle.fd, directoryImage(["forged"]), metadata, root);
			expect(token.view!.retainObject(root, forged)).toBe(false); await forged.object!.dispose();
			const actual = await captureHeldDirectory(process.pid, handle.fd, content, metadata, root);
			expect(actual.entries).toEqual(names); expect(token.view!.retainObject(root, actual)).toBe(true);
			expect(token.view!.bytes).toBe(bytes);
			await token.view!.borrowObject(root, async (capture, handle) => { pin = handle; expect(capture.content).toEqual(content); });
			expect(await token.view!.readdir(root)).toEqual(names);
		} finally { await token.release(); await handle.close(); manager.close(); expect(pin!.fd).toBe(-1); }
	});

	test.skipIf(process.platform !== "linux")("revokes pinned input objects while draining an admitted borrower", async () => {
		const root = await workspace({ data: "shared" }), file = path.join(root, "data");
		const token = await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 8192);
		await token.view!.readFile(file); token.view!.seal();
		const entered = deferred<void>(), finish = deferred<void>();
		let descriptor: import("node:fs/promises").FileHandle | undefined;
		const borrowed = token.view!.borrowObject(file, async (capture, handle) => {
			descriptor = handle; expect(capture.content?.toString()).toBe("shared"); entered.resolve();
			await finish.promise; expect((await handle.stat()).isFile()).toBe(true);
		});
		await entered.promise;
		const disposed = token.release();
		await expect(token.view!.borrowObject(file, async () => {})).rejects.toThrow("disposed");
		expect(descriptor!.fd).toBeGreaterThanOrEqual(0); finish.resolve();
		await borrowed; await disposed; expect(descriptor!.fd).toBe(-1);
	});

	test.for(["read", "write", "stale"] as const)("shares %s inputs with FD capture and rejects replacement or revoked owners", async (source, { skip }) => {
		if (process.platform !== "linux") return skip("Linux object pins");
		const root = await workspace({ data: "shared" }), file = path.join(root, "data"), args = { path: file };
		const invocation = resolvePiToolInvocation("read", args, { cwd: root, environment: {} })!;
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 65536 });
		const action = PI_ACTION_SEMANTICS.buildKey("read", args, root, "", { fingerprint: "read", context: invocation })!;
		const branch = source === "read" ? await world.speculation!.execute({ cwd: root, tool: createReadTool(root), toolName: "read", args,
			action, callID: "read", signal: new AbortController().signal }) : await createCommittedResourceInputs(
			{ result: { content: [], details: {} }, isError: false }, action, root,
			new Map([[file, Buffer.from(source === "stale" ? "forged" : "shared")]]), 65536);
		const capturedBytes = branch.capturedBytes;
		const handle = await fs.open(file, "r"), expected = await handle.stat({ bigint: true }), sources = [branch.inputSource!];
		const opened = vi.spyOn(fs, "open");
		try {
			expect((await borrowResourceObject(sources, file, expected, 16))?.content?.toString()).toBe(source === "read" ? "shared" : undefined);
			for (const repeated of [false, true]) {
				opened.mockClear();
				const graph = await captureHeldDescriptorInputs(process.pid, [{ fd: handle.fd, alias: handle.fd, flags: 0,
					offset: 0, device: String(expected.dev), inode: String(expected.ino), owned: true }], 16, [], () => sources);
				expect(Buffer.from(graph.objects[handle.fd]!.content!, "base64").toString()).toBe("shared");
				expect(opened).toHaveBeenCalledTimes(source === "read" || repeated && source === "write" ? 0 : 2);
				expect((await borrowResourceObject(sources, file, expected, 16))?.content?.toString()).toBe(source === "stale" ? undefined : "shared");
				expect(branch.capturedBytes, "pin admission uses its existing byte budget").toBe(capturedBytes);
			}
			await fs.writeFile(path.join(root, "replacement"), "shared"); await fs.rename(path.join(root, "replacement"), file);
			expect(await borrowResourceObject(sources, file, await fs.stat(file, { bigint: true }), 16)).toBeUndefined();
			await branch.dispose(); expect(await borrowResourceObject(sources, file, expected, 16)).toBeUndefined();
		} finally { opened.mockRestore(); await handle.close(); await branch.dispose(); }
	});

	test("owns supplied poststates without reading payloads, scanning directories or claiming a host observation window", async () => {
		const root = await workspace({ "value.txt": "A" }), file = path.join(root, "value.txt"), bytes = Buffer.from("A");
		const absent = path.join(root, "deleted"), names = ["value.txt"], opened = vi.spyOn(fs, "open"), scanned = vi.spyOn(fs, "readdir");
		const token = await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 8192,
			new Map<string, ResourceInput>([[file, bytes], [root, { names }], [absent, null]]));
		try {
			expect(opened.mock.calls.filter(([, flags]) => isDataOpen(flags))).toHaveLength(0);
			names.push("unowned"); expect(await token.view!.readdir(root)).toEqual(["value.txt"]);
			expect(await token.view!.exists(absent)).toBe(false); expect(scanned).not.toHaveBeenCalled();
			expect(token.watching).toBe(false); expect(token.preciseContent).toEqual([]);
			bytes[0] = 66; expect(await token.view!.readFile(file)).toEqual(Buffer.from("A"));
			expect((await token.manager.validate(token)).expired).toBe(false);
			await fs.writeFile(absent, "new"); expect((await token.manager.validate(token)).expired).toBe(true);
			await expect(captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 8192, new Map([[absent, null]]))).rejects.toThrow("resource_input_type_changed");
			await fs.unlink(absent); expect((await token.manager.validate(token)).expired).toBe(false);
			await fs.writeFile(file, "B"); expect((await token.manager.validate(token)).expired).toBe(true);
			await expect(captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 0, new Map([[file, bytes]]))).rejects.toThrow("budget");
			await fs.unlink(file); await fs.mkdir(file);
			await expect(captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 8192, new Map([[file, bytes]]))).rejects.toThrow("resource_input_not_regular");
		} finally { opened.mockRestore(); scanned.mockRestore(); await token.release(); }
	});

	test.each([true, false])("seals eager observations and on-demand inputs (watch=%s)", async (watch) => {
		for (const onDemand of [false, true]) for (const change of ["unchanged", "ancestor", "sibling", "write", "restore", "replace", "kind", "entries"] as const) {
			const parent = await workspace({ "workspace/value.txt": "A" }), root = path.join(parent, "workspace");
			const file = path.join(root, "value.txt");
			const manager = new ResourceVersionManager(root, { watch });
			const target = change === "entries" ? ["."] : ["value.txt"];
			// Only ancestors outside this fixture are fixed; its parent, workspace and files stay physical.
			const ancestors = new Map<string, import("node:fs").BigIntStats>(), nativeStat = fs.lstat;
			if (!onDemand) for (let directory = path.dirname(parent); !ancestors.has(directory); directory = path.dirname(directory))
				ancestors.set(directory, await nativeStat(directory, { bigint: true }));
			const stat = onDemand ? undefined : vi.spyOn(fs, "lstat").mockImplementation((async (file, options) =>
				(options?.bigint && ancestors.get(String(file))) || nativeStat(file, options)) as typeof fs.lstat);
			let token: ResourceVersionToken | undefined;
			try {
				token = await manager.capture(onDemand ? undefined : resourceDependencies(action(change === "entries" ? "ls" : "read", target), root), 8192);
				expect(manager.changesSince(token).uncertain).toBe(!watch || onDemand);
				expect(Reflect.set(token.preciseContent, 0, "unproven")).toBe(false);
				if (onDemand) {
					expect((await manager.validate(token)).expired).toBe(true); // Open capture is never an adoptable certificate.
					const value = change === "entries" ? root : file, view = token.view!;
					const inputs = [() => view.stat(value, "entry"), () => change === "entries" ? view.readdir(value) : view.readFile(value)];
					for (const capture of watch ? inputs : inputs.reverse()) await capture();
					const evidence = [view.bytes, [...token.observations]];
					view.capture(value, { type: change === "entries" ? "directory" : "file" });
					const opened = vi.spyOn(fs, "open"), stat = vi.spyOn(fs, "lstat"), resolved = vi.spyOn(fs, "realpath");
					try {
						expect(await view.exists(value)).toBe(true);
						for (const fields of [undefined, "type", "entry"] as const) {
							const info = await view.stat(value, fields);
							expect(info.isDirectory()).toBe(change === "entries");
							expect(info.size).toBe(change !== "entries" && !fields ? 1 : undefined);
						}
						if (change !== "entries") await view.access(value);
						expect([opened.mock.calls.length, stat.mock.calls.length, resolved.mock.calls.length, view.bytes, [...token.observations]])
							.toEqual([0, 0, 0, ...evidence]);
					} finally { opened.mockRestore(); stat.mockRestore(); resolved.mockRestore(); }
					expect(change === "entries" ? await view.readdir(root) : (await view.readFile(file)).toString()).toEqual(change === "entries" ? ["value.txt"] : "A");
				}
				if (change === "write" || change === "restore") await fs.writeFile(file, change === "write" ? "longer B" : "B");
				if (change === "kind") { await fs.rm(file); await fs.mkdir(file); }
				if (change === "unchanged") await directories.create(); // Shared ancestor noise must not change this fixture's expected outcome.
				if (change === "ancestor") await fs.mkdir(path.join(parent, "unrelated"));
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
				expect((await manager.validate(token)).expired, change).toBe(change === "write" || change === "kind");
				if (onDemand) {
					let dependencies: ReadonlySet<string> | undefined;
					await token.view!.evaluate((view) => view.stat(change === "entries" ? root : file, "entry"), (observed) => { dependencies = observed; });
					expect(dependencies?.size).toBe(2);
					const scoped = { ...token, observations: new Map([...token.observations].filter(([key]) => dependencies!.has(key))) };
					const checked = await manager.validate(scoped);
					expect(checked.expired, change).toBe(change === "kind" || (change === "write" && !watch));
					if (watch) expect(checked).toMatchObject({ filesRead: 0, bytesRead: 0 });
				}
				const sealed = await manager.seal(token);
				expect(sealed.expired, JSON.stringify({ watch, onDemand, change, sealed })).toBe(onDemand || process.platform === "win32" || change !== "unchanged");
				if (change === "ancestor" && !onDemand && process.platform !== "win32")
					expect(sealed).toMatchObject({ mode: "exact", reason: "resource_observation_window_changed" });
				const released = manager.validate(token);
				releaseResourceVersion(token);
				expect((await released).expired).toBe(true); // Release during validation retires the evidence, not just its payload.
			} finally { await token?.release(); stat?.mockRestore(); manager.close(); }
		}
	});

	test("keeps preparation notifications without reading or authorizing resource content", async () => {
		for (const watch of [true, false]) for (const snapshotExcludes of [[], [".git"]]) {
			const root = await workspace({ "value.txt": "before" }), idle = vi.fn();
			const manager = new ResourceVersionManager(root, { watch, snapshotExcludes, onIdle: idle });
			const reads = vi.spyOn(fs, "open");
			const token = await manager.observeChanges();
			try {
				expect([token.observations.size, token.view]).toEqual([0, undefined]);
				await expect(manager.capture([])).rejects.toThrow("resource_dependencies_unproven");
				for (const content of ["before", "after!"]) {
					await fs.writeFile(path.join(root, "value.txt"), content);
					if (watch) await vi.waitFor(() => expect(manager.changesSince(token).paths).toContain(path.join(root, "value.txt")));
					else expect(manager.changesSince(token).uncertain).toBe(true);
					expect(await manager.validate(token)).toMatchObject({ expired: true, bytesRead: 0, filesRead: 0 });
					expect((await manager.seal(token)).expired).toBe(true);
				}
				expect(reads).not.toHaveBeenCalled();
				await token.release(); await token.release(); expect(idle).toHaveBeenCalledTimes(1);
			} finally { reads.mockRestore(); await token.release(); manager.close(); }
		}
	});

	test.each([true, false])("rechecks shared missing ancestors on every adoption (watch=%s)", async (watch) => {
		const root = await workspace({ "value.txt": "A" }), parent = path.join(root, "missing");
		const manager = new ResourceVersionManager(root, { watch });
		const paths = Array.from({ length: 24 }, (_, index) => path.join(parent, `input-${index}.txt`));
		const token = await manager.capture(paths.map((path) => ({ path, scope: "content" })), 65536);
		try {
			expect((await manager.validate(token)).expired).toBe(false);
			await fs.mkdir(parent);
			expect((await manager.validate(token)).expired).toBe(false); // Every requested file is still absent.
			await fs.writeFile(paths[17]!, "new input");
			expect((await manager.validate(token)).expired).toBe(true);
			await fs.rm(paths[17]!);
			expect((await manager.validate(token)).expired).toBe(false);
			await fs.rmdir(parent);
			await fs.writeFile(parent, "not a directory");
			const nativeError = await fs.lstat(paths[0]!).then(() => undefined, (error: NodeJS.ErrnoException) => error.code);
			expect((await manager.validate(token)).expired).toBe(nativeError !== "ENOENT"); // Preserve the platform's missing-path error.
		} finally { await token.release(); manager.close(); }
	});

	test("owns bounded immutable inputs and never converts unproven access into absence", async () => {
		const payload = Buffer.concat([Buffer.alloc(1024 * 1024, 65), Buffer.alloc(1024 * 1024, 66), Buffer.from("end")]);
		const root = await workspace({ "value.txt": payload }), file = path.join(root, "value.txt");
		const manager = new ResourceVersionManager(root, { watch: false });
		const dependencies = resourceDependencies(action("read", ["value.txt", "missing"]), root);
		for (const [budget, paths] of [[0, dependencies], [payload.length, dependencies.slice(0, 1)],
			[3 * 1024 * 1024, [...dependencies, { path: root, scope: "tree_content" }]]] as const) {
			const opened = vi.spyOn(fs, "open");
			const token = await manager.capture(paths, budget), view = token.view;
			const reads = opened.mock.calls.filter(([, flags]) => isDataOpen(flags)).length; opened.mockRestore();
			expect(reads).toBe(1); // Concurrent scopes share the same content capture and retained allocation.
			expect(await manager.validate(token)).toMatchObject({ expired: false, filesRead: 1 });
			expect(Boolean(view)).toBe(budget > payload.length); // Exhaustion revokes all retained input authority.
			if (!view) { token.release(); continue; }
			await expect(view.evaluate(async (scope) => {
				try { await scope.exists(path.join(root, "unknown")); } catch { /* Tool may swallow a failed stat. */ }
			})).rejects.toThrow("resource_access_unproven");
			expect(await view.evaluate((scope) => scope.readFile(file))).toEqual(payload);
			(await view.readFile(file)).fill(66);
			await fs.writeFile(file, "B");
			expect([await view.readFile(file), (await view.stat(file)).size, (await view.stat(file, "type")).size]).toEqual([payload, payload.length, undefined]);
			expect(await view.exists(path.join(root, "missing"))).toBe(false);
			for (const type of ["missing", "file"] as const) expect(() => view.capture(file, { type })).toThrow("not_capturing");
			await expect(view.exists(path.join(root, "unknown"))).rejects.toThrow("resource_access_unproven");
			expect(() => view.assertComplete()).toThrow("resource_access_unproven");
			expect((await manager.seal(token)).expired).toBe(true);
			releaseResourceVersion(token);
			await expect(view.readFile(file)).rejects.toThrow("disposed");
			await expect(view.evaluate(async () => "late")).rejects.toThrow("disposed");
		}
		manager.close();
	});

	test("shares pending entry reads across scopes and rechecks the next adoption", async () => {
		const root = await workspace({ value: "A" }), file = path.join(root, "value"), gate = gated();
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture((["entry", "type", "stat"] as const).map(scope => ({ path: file, scope })));
		const nativeStat = fs.lstat;
		const stat = vi.spyOn(fs, "lstat").mockImplementation((async (target, options) => {
			if (String(target) === file) await gate.wait();
			return nativeStat(target, options);
		}) as typeof fs.lstat);
		const pending = manager.validate(token);
		try {
			await gate.entered;
			expect(stat.mock.calls.filter(([target]) => String(target) === file)).toHaveLength(1);
			gate.release();
			expect((await pending).expired).toBe(false);
			expect(stat.mock.calls.filter(([target]) => String(target) === file)).toHaveLength(4); // Shared admission plus each scope's final identity check.
			await fs.appendFile(file, "changed");
			expect((await manager.validate(token)).expired).toBe(true);
		} finally { gate.release(); await pending; stat.mockRestore(); await token.release(); manager.close(); }
	});

	test("coalesces pending input reads and drains failed parallel captures before releasing ownership", async () => {
		for (const phase of ["pending", "dependencies", "tree"]) {
			const root = await workspace({ value: "A", broken: "B" }), idle = vi.fn();
			const manager = new ResourceVersionManager(root, { watch: false, onIdle: idle });
			const handle = await fs.open(path.join(root, "value"), "r"), broken = phase === "pending" ? undefined : await fs.open(path.join(root, "broken"), "r");
			const read = handle.read.bind(handle), failure = new Error("injected read failure");
			let settled = false;
			const gate = gated();
			const { promise: failed, resolve: fail } = deferred();
			const nativeOpen = fs.open.bind(fs);
			const open = vi.spyOn(fs, "open").mockImplementation(async (file, flags, mode) => !isDataOpen(flags) ? nativeOpen(file, flags, mode)
				: await fs.realpath(file) === path.join(root, "value") ? handle : broken!);
			vi.spyOn(handle, "read").mockImplementationOnce((async (...args: Parameters<typeof handle.read>) => {
				await gate.wait(); return read(...args);
			}) as typeof handle.read);
			if (broken) {
				const close = broken.close.bind(broken);
				vi.spyOn(broken, "read").mockImplementationOnce(async () => { await gate.entered; throw failure; });
				vi.spyOn(broken, "close").mockImplementationOnce(async () => { await close(); fail(); });
			}
			const token = phase === "pending" ? await manager.capture(undefined, 8192) : undefined;
			let release: void | Promise<void> = undefined;
			const pending = (token ? Promise.allSettled([token.view!.readFile(path.join(root, "value")), token.view!.readFile(path.join(root, "value"))])
				.then((entries) => entries.map((entry) => entry.status)) : manager.capture(phase === "tree" ? [{ path: root, scope: "tree_content" }] :
					[{ path: "value", scope: "content" }, { path: "broken", scope: "content" }], 8192).then((token) => token.release(), (error: unknown) => error))
				.finally(() => { settled = true; });
			try {
				await gate.entered;
				if (token) {
					expect((await manager.seal(token)).expired).toBe(true);
					release = token.release(); expect(token.release()).toBe(release);
					await expect(token.view!.readFile(path.join(root, "value"))).rejects.toThrow("disposed");
				}
				else await failed;
				await nextTurn();
				expect({ settled, released: idle.mock.calls.length, reading: handle.fd >= 0 }, phase).toEqual({ settled: false, released: 0, reading: true });
				gate.release();
				if (token) expect(await pending).toEqual(["rejected", "rejected"]); else expect(await pending).toBe(failure);
				await release;
				expect([handle.fd, broken?.fd ?? -1, idle.mock.calls.length, open.mock.calls.filter(([, flags]) => isDataOpen(flags)).length]).toEqual([-1, -1, 1, token ? 1 : 2]);
			} finally { gate.release(); await pending; await token?.release(); await Promise.all([handle.close(), broken?.close()]); open.mockRestore(); manager.close(); }
		}
	});

	test.each([false, true])("owns prepared inputs with their original evidence and sealed byte budget (exhausted=%s)", async (exhausted) => {
		const root = await workspace({ "value.txt": "A", unused: "B", "inside/other": "C" }), file = path.join(root, "value.txt");
		const manager = new ResourceVersionManager(root, { watch: false }), token = await manager.capture(undefined, 65536), view = token.view!;
		const binding = {}, dispose = vi.fn(), build = vi.fn(async (inputs: import("../src/tool-settlement.ts").ToolFilesystemOperations) => {
			const value = (await inputs.readFile(file)).toString();
			expect(await inputs.exists!(path.join(root, "missing"))).toBe(false);
			return { value, bytes: exhausted ? 65536 : 1, dispose };
		});
		try {
			await view.readFile(path.join(root, "unused")); await view.stat(path.join(root, "inside"), "type");
			expect(await view.prepare(binding, "selection", build, async value => value)).toBe("A");
			view.seal(); const bytes = view.bytes;
			let dependencies: ReadonlySet<string> | undefined;
			const query = () => view.evaluate(v => v.prepare(binding, "selection", build, async value => value), observed => { dependencies = observed; });
			expect(await query()).toBe("A"); expect(build).toHaveBeenCalledTimes(exhausted ? 2 : 1);
			expect(dispose).toHaveBeenCalledTimes(exhausted ? 2 : 0); expect(view.bytes).toBe(bytes);
			expect(dependencies?.size).toBeGreaterThan(0);
			const scoped = { ...token, observations: new Map([...token.observations].filter(([key]) => dependencies!.has(key))) };
			await fs.writeFile(path.join(root, "unused"), "irrelevant");
			expect((await manager.validate(scoped)).expired).toBe(false);
			await fs.writeFile(path.join(root, "missing"), "now present");
			expect((await manager.validate(scoped)).expired).toBe(true);
			await fs.unlink(path.join(root, "missing")); await fs.writeFile(file, "changed");
			expect((await manager.validate(scoped)).expired).toBe(true);
			await expect(view.evaluate(v => v.prepare(binding, "selection", build, async value => value), undefined, path.join(root, "inside")))
				.rejects.toThrow("resource_access_unproven");
			expect(await query()).toBe("A"); // Query failure cannot revoke a sibling's sealed input.
			await view.evaluate(v => v.prepare({}, "selection", build, async value => value)); // A different binding must rebuild and release.
			expect(build).toHaveBeenCalledTimes(exhausted ? 5 : 3);
			expect(dispose).toHaveBeenCalledTimes(exhausted ? 4 : 1); expect(view.bytes).toBe(bytes);
		} finally { await token.release(); manager.close(); }
		expect(dispose).toHaveBeenCalledTimes(exhausted ? 4 : 2);
	});

	test.each(["content", "names"] as const)("retains only independently proven metadata after %s revocation", async scope => {
		const root = await workspace({ value: "A" }), manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture(undefined, 8192), view = token.view!, target = scope === "names" ? root : path.join(root, "value");
		const consume = async (inputs: typeof view) => scope === "names" ? inputs.readdir(target) : inputs.readFile(target);
		try {
			await view.stat(target, "type"); await consume(view); view.seal(); const bytes = view.bytes;
			expect(view.invalidate(new Set([`${scope}:${target.replaceAll(path.sep, "/")}`]))).not.toContain(target.replaceAll(path.sep, "/"));
			expect(view.bytes).toBeLessThan(bytes);
			expect((await view.evaluate(view => view.stat(target, "type"))).isDirectory()).toBe(scope === "names");
			await expect(view.evaluate(consume)).rejects.toThrow("resource_access_unproven");
			view.invalidate(new Set([`type:${target.replaceAll(path.sep, "/")}`]));
			await expect(view.evaluate(view => view.stat(target, "type"))).rejects.toThrow("resource_access_unproven");
		} finally { await token.release(); manager.close(); }
	});

	test.each([false, true])("revokes changed inputs and reclaims preparations after borrowers finish (%s)", async (borrowed) => {
		const root = await workspace({ a: "A", b: "B" }), a = path.join(root, "a"), b = path.join(root, "b");
		const manager = new ResourceVersionManager(root, { watch: false }), token = await manager.capture(undefined, 65536), view = token.view!;
		const binding = {}, dispose = vi.fn(), build = vi.fn(async (inputs: import("../src/tool-settlement.ts").ToolFilesystemOperations, file: string) =>
			({ value: (await inputs.readFile(file)).toString(), bytes: 1, dispose }));
		const prepare = (file: string) => view.evaluate(v => v.prepare(binding, file, v => build(v, file), async value => value));
		const gate = gated(), consuming = gated(); let active: Promise<unknown> | undefined, consumer: Promise<unknown> | undefined;
		const capturing = await manager.capture(undefined, 8192);
		try {
			for (const file of [a, b]) await view.prepare(binding, file, v => build(v, file), async value => value);
			view.seal(); const bytes = view.bytes;
			if (borrowed) {
				consumer = view.evaluate(v => v.prepare(binding, a, v => build(v, a), async value => { await consuming.wait(); return value; }));
				await consuming.entered;
			}
			active = capturing.view!.prepare(binding, "in-flight", async inputs => {
				const resource = await build(inputs, a); await gate.wait(); return resource;
			}, async value => value);
			await gate.entered;
			await fs.writeFile(a, "changed");
			const opened = vi.spyOn(fs, "open"), stat = vi.spyOn(fs, "lstat");
			try {
				const removed = invalidateResourceInputs([token, capturing], [a]).map(file => file.replaceAll("\\", "/"));
				expect(removed).toContain(a.replaceAll("\\", "/")); expect(removed).not.toContain(b.replaceAll("\\", "/"));
				expect(invalidateResourceInputs([token, capturing], [a])).toEqual([]);
				expect(await prepare(b)).toBe("B"); expect(build).toHaveBeenCalledTimes(3);
				expect(await view.evaluate(v => v.readFile(b))).toEqual(Buffer.from("B"));
				expect(opened).not.toHaveBeenCalled(); expect(stat).not.toHaveBeenCalled();
			} finally { opened.mockRestore(); stat.mockRestore(); }
			await expect(prepare(a)).rejects.toThrow("resource_access_unproven");
			expect(view.bytes).toBeLessThan(bytes);
			expect(dispose).toHaveBeenCalledTimes(borrowed ? 0 : 1);
			if (borrowed) {
				const heldBytes = view.bytes; consuming.release(); expect(await consumer).toBe("A");
				await nextTurn(); expect(dispose).toHaveBeenCalledOnce(); expect(view.bytes).toBeLessThan(heldBytes);
			}
			gate.release(); await active; expect(dispose).toHaveBeenCalledTimes(2);
			capturing.view!.seal();
			await expect(capturing.view!.evaluate(v => v.prepare(binding, "in-flight", v => build(v, a), async value => value))).rejects.toThrow("resource_access_unproven");
			expect((await manager.validate(token)).expired).toBe(true);
			await fs.writeFile(a, "A"); expect((await manager.validate(token)).expired).toBe(false);
		} finally { consuming.release(); gate.release(); await consumer?.catch(() => {}); await active?.catch(() => {}); await token.release(); await capturing.release(); manager.close(); }
		expect(dispose).toHaveBeenCalledTimes(3);
	});

	test.each(["capturing", "transient", "transferred", "composed", "composed-unowned", "composed-reader", "composed-cancelled", "composed-budget", "missing", "revoked", "cancelled"])("shares concurrent preparation with independent consumers and owned proof (%s)", async mode => {
		const root = await workspace({ value: "A" }), file = path.join(root, "value"), idle = vi.fn(), manager = new ResourceVersionManager(root, { watch: false, onIdle: idle });
		const token = await manager.capture(undefined, 8192), extra = await manager.capture(undefined, 8192), destination = await manager.capture(undefined, mode === "composed-budget" ? 1 : 8192);
		const view = token.view!, binding = {}, gate = gated(), consuming = gated(), dispose = vi.fn(), observed = [vi.fn(), vi.fn()];
		const composed = mode.startsWith("composed"), independent = composed && mode !== "composed-unowned" || mode === "missing";
		const builds = mode === "composed-unowned" || mode === "composed-reader" ? 2 : 1;
		const cancelled = mode.endsWith("cancelled"), retained: ResourceVersionToken[] = [];
		if (mode !== "missing") await (composed ? extra.view! : view).readFile(file); extra.view!.seal();
		if (mode === "transient") await view.readdir(root);
		const capturing = mode === "capturing" || mode === "revoked";
		if (!capturing) view.seal();
		const build = vi.fn(async (inputs: import("../src/tool-settlement.ts").ToolFilesystemOperations) => {
			const value = (await inputs.readFile(file)).toString(); await gate.wait(); return { value, bytes: 1, dispose };
		});
		const query = (index: number) => {
			const operation = (inputs: import("../src/tool-settlement.ts").ToolFilesystemOperations) => inputs.prepare!(binding, "shared", build, async value => {
				if (index === 1) await consuming.wait();
				if (index === 0 && cancelled) throw new Error("consumer cancelled");
				return value;
			}, root);
			const source = (token: ResourceVersionToken) => ({ view: token.view!, observed: (keys: ReadonlySet<string> | undefined) => {
				observed[index]!(keys); return independent ? [{ ...token, view: undefined }] : undefined;
			} });
			return capturing ? operation(view) : view.evaluate(operation, observed[index], root,
				composed ? () => [source(extra)] : undefined,
				mode === "transferred" || composed || mode === "missing" ? async () => source(destination) : undefined,
				independent && (mode !== "composed-reader" || index === 0) ? proofs => proofs.map(token => {
					const proof = manager.retain(token); retained.push(proof); observed[index]!(new Set(proof.observations.keys())); return proof;
				}) : undefined);
		};
		const first = query(0); await gate.entered;
		const second = query(1), settled = Promise.allSettled([first, second]);
		try {
			await nextTurn(); expect(build).toHaveBeenCalledTimes(builds);
			if (mode === "revoked") { await fs.writeFile(file, "changed"); invalidateResourceInputs([token], [file]); }
			gate.release(); await consuming.entered; await first.catch(() => {});
			if (mode === "transient") expect(view.invalidate(new Set([`names:${root.replaceAll(path.sep, "/")}`])))
				.toContain(root.replaceAll(path.sep, "/")); // A temporary preparation must not keep a revoked name indexed.
			expect(build).toHaveBeenCalledTimes(builds); expect(dispose).not.toHaveBeenCalled();
			if (independent && mode !== "composed-reader") { await extra.release(); expect(retained).toHaveLength(2); }
			consuming.release(); expect(await settled).toMatchObject([
				cancelled ? { status: "rejected", reason: new Error("consumer cancelled") } : { status: "fulfilled", value: "A" },
				{ status: "fulfilled", value: "A" },
			]);
			if (!capturing) for (const observer of observed.slice(cancelled ? 1 : 0)) expect(observer).toHaveBeenCalled();
			if (capturing) view.seal();
			expect((await manager.validate(token)).expired).toBe(mode === "revoked");
			if (composed || mode === "missing") expect(observed.every(observer => observer.mock.calls.some(([keys]) => keys?.has(`content:${file.replaceAll(path.sep, "/")}`)))).toBe(true);
			for (const proof of retained) expect((await manager.validate(proof)).expired).toBe(false);
			if (retained.length) { await fs.writeFile(file, "changed"); for (const proof of retained) expect((await manager.validate(proof)).expired).toBe(true); }
		} finally { gate.release(); consuming.release(); await settled; await token.release(); await extra.release(); await destination.release(); await Promise.all(retained.map(proof => proof.release())); manager.close(); }
		expect(dispose).toHaveBeenCalledTimes(builds);
		expect(idle).toHaveBeenCalledOnce();
	});

	test.each(["build", "consume"])("drains prepared input %s before releasing its owner", async (phase) => {
		const root = await workspace({ value: "A" }), manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture(undefined, 8192), view = token.view!, binding = {}, gate = gated(), dispose = vi.fn();
		const build = async (inputs: import("../src/tool-settlement.ts").ToolFilesystemOperations) => {
			const value = (await inputs.readFile(path.join(root, "value"))).toString();
			if (phase === "build") await gate.wait();
			return { value, bytes: 1, dispose };
		};
		let pending: Promise<unknown> | undefined, release: void | Promise<void>;
		try {
			if (phase === "consume") {
				await view.prepare(binding, "input", build, async value => value); view.seal();
				await expect(view.evaluate(v => v.prepare(binding, "input", build, async () => { throw new Error("consumer cancelled"); }))).rejects.toThrow("consumer cancelled");
				expect(dispose).not.toHaveBeenCalled();
			}
			pending = Promise.all([0, 1].map(() => view.prepare(binding, "input", build, async value => { if (phase === "consume") await gate.wait(); return value; })));
			const settled = Promise.allSettled([pending]); await gate.entered;
			let released = false; release = token.release(); void Promise.resolve(release).then(() => { released = true; });
			await nextTurn(); expect(released).toBe(false); expect(dispose).not.toHaveBeenCalled();
			gate.release(); expect(await settled).toMatchObject([{ status: "rejected", reason: new Error("resource_snapshot_disposed") }]);
			await release; expect(dispose).toHaveBeenCalledOnce();
		} finally { gate.release(); await pending?.catch(() => {}); await token.release(); manager.close(); }
	});

	test.each(["retained", "budget", "build", "consume"])("transfers composed preparations into their capture lifetime (%s)", async (phase) => {
		const root = await workspace({ value: "A", "inside/other": "B" }), manager = new ResourceVersionManager(root, { watch: false });
		const source = await manager.capture(undefined, 8192), destination = await manager.capture(undefined, 8192), reader = await manager.capture(undefined, 8192);
		const binding = {}, dispose = vi.fn(), gate = gated();
		await source.view!.readFile(path.join(root, "value")); source.view!.seal();
		const proof = manager.retain(source);
		const build = vi.fn(async (view: import("../src/tool-settlement.ts").ToolFilesystemOperations) => {
			const value = (await view.readFile(path.join(root, "value"))).toString();
			if (phase === "build") await gate.wait();
			return { value, bytes: phase === "budget" ? 8192 : 1, dispose };
		});
		let pending: Promise<unknown> | undefined, release: void | Promise<void>;
		try {
			pending = source.view!.evaluate(view => view.prepare(binding, "selection", build, async value => {
				if (phase === "consume") await gate.wait(); return value;
			}, root), undefined, root, undefined, async () => ({ view: destination.view!, observed: () => {} }));
			if (phase === "build" || phase === "consume") {
				const settled = Promise.allSettled([pending]); await gate.entered;
				let released = false; release = destination.release(); void Promise.resolve(release).then(() => { released = true; });
				await nextTurn(); expect(released).toBe(phase === "build"); expect(dispose).not.toHaveBeenCalled();
				gate.release(); expect(await settled).toMatchObject([{ status: "rejected", reason: new Error("resource_snapshot_disposed") }]);
				await release;
			} else {
				expect(await pending).toBe("A"); destination.view!.seal();
				await source.release();
				const query = (identity = binding, key = "selection", rootOverride = root) => destination.view!.evaluate(
					view => view.prepare(identity, key, build, async value => value), observed => { expect(observed).toBeUndefined(); }, rootOverride);
				if (phase === "retained") {
					expect(await query()).toBe("A"); expect(build).toHaveBeenCalledOnce(); expect(dispose).not.toHaveBeenCalled();
					reader.view!.seal(); const observed = vi.fn();
					const borrowed = () => reader.view!.evaluate(view => view.prepare(binding, "selection", build, async value => value, root),
						undefined, root, () => [{ view: destination.view!, observed }]);
					expect(destination.view!.resources).toContainEqual({ path: root.replaceAll("\\", "/"), descendants: false });
					expect(await borrowed()).toBe("A"); expect(build).toHaveBeenCalledOnce(); expect(observed).toHaveBeenCalledWith(undefined);
					for (const [identity, key, boundary] of [[{}, "selection", root], [binding, "different", root], [binding, "selection", path.join(root, "inside")]] as const)
						await expect(query(identity, key, boundary)).rejects.toThrow("resource_access_unproven");
					expect(await query()).toBe("A");
					expect(invalidateResourceInputs([destination, proof], [path.join(root, "value")])).toContain(root.replaceAll("\\", "/"));
					expect(destination.view!.resources).toEqual([]);
					await expect(borrowed()).rejects.toThrow("resource_access_unproven");
					await expect(query()).rejects.toThrow("resource_access_unproven");
				} else await expect(query()).rejects.toThrow("resource_access_unproven");
			}
		} finally { gate.release(); await pending?.catch(() => {}); await source.release(); await destination.release(); await proof.release(); await reader.release(); manager.close(); }
		expect(dispose).toHaveBeenCalledOnce();
	});

	test.each([undefined, 1, 8192])("retains query evidence and bounded directory metadata independently (%s)", async metadataBytes => {
		const root = await workspace({ value: "A", unused: "B" }), idle = vi.fn();
		const alias = path.join(root, "alias"), data = path.join(root, "data");
		await fs.mkdir(data); await fs.writeFile(path.join(data, "value"), "C");
		await fs.symlink(data, alias, process.platform === "win32" ? "junction" : "dir");
		const manager = new ResourceVersionManager(root, { watch: false, onIdle: idle });
		const token = await manager.capture(undefined, 8192), view = token.view!;
		const payload = await manager.capture([{ path: path.join(data, "value"), scope: "content" }], 8192);
		let retained: ResourceVersionToken | undefined;
		try {
			await view.stat(root, "type");
			await view.stat(alias, "type");
			await view.readFile(path.join(root, "value"));
			await view.exists(path.join(root, "missing"));
			expect(() => manager.retain(token)).toThrow("resource_snapshot_not_sealed");
			view.seal(); retained = manager.retain(token, metadataBytes);
			expect(Boolean(retained.view)).toBe(metadataBytes === 8192); expect(retained.observations).not.toBe(token.observations);
			await token.release(); expect(idle).not.toHaveBeenCalled();
			if (retained.view) {
				const opened = vi.spyOn(fs, "open"), stat = vi.spyOn(fs, "lstat");
				try {
					expect((await retained.view.evaluate(v => v.stat(root, "type"))).isDirectory()).toBe(true);
					const observed = new Set<string>(), lookup = vi.fn((_target: string) => [{ view: payload.view!, observed: (keys: ReadonlySet<string> | undefined) => {
						for (const key of keys ?? []) observed.add(key);
					} }]);
					for (const boundary of [root, alias]) expect((await retained.view.evaluate(v => v.readFile(path.join(alias, "value")), keys => {
						for (const key of keys ?? []) observed.add(key);
					}, boundary, lookup)).toString()).toBe("C");
					expect(path.resolve(lookup.mock.calls[0]![0])).toBe(path.join(data, "value"));
					expect([...observed].some(key => retained!.observations.get(key)?.path === alias)).toBe(true);
					expect([...observed].some(key => payload.observations.get(key)?.path === path.join(data, "value"))).toBe(true);
					await expect(retained.view.evaluate(v => v.readFile(path.join(root, "value")))).rejects.toThrow("resource_access_unproven");
					expect(opened).not.toHaveBeenCalled(); expect(stat).not.toHaveBeenCalled();
				} finally { opened.mockRestore(); stat.mockRestore(); }
			}
			await fs.writeFile(path.join(root, "unused"), "irrelevant");
			expect((await manager.validate(retained)).expired).toBe(false);
			await fs.unlink(alias); await fs.symlink(root, alias, process.platform === "win32" ? "junction" : "dir");
			expect((await manager.validate(retained)).expired).toBe(true);
			await fs.unlink(alias); await fs.symlink(data, alias, process.platform === "win32" ? "junction" : "dir");
			await fs.writeFile(path.join(root, "missing"), "present");
			expect((await manager.validate(retained)).expired).toBe(true);
			await payload.release(); await retained.release(); await retained.release(); expect(idle).toHaveBeenCalledOnce();
			expect(() => manager.retain(retained!)).toThrow("resource_version_owner_changed");
		} finally { await payload.release(); await retained?.release(); await token.release(); manager.close(); }
	});

	test.each(["physical", "logical"])("composes alias metadata and %s bytes after the namespace source retires", async location => {
		const root = await workspace({ unused: "A" }), data = path.join(root, "data"), alias = path.join(root, "alias");
		await fs.mkdir(data); await fs.writeFile(path.join(data, "value.txt"), "first\nsecond\nthird\n");
		await fs.symlink(data, alias, process.platform === "win32" ? "junction" : "dir");
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["ls", "read"], maxBytes: () => 65536 });
		const request = (toolName: "ls" | "read", args: { path: string; offset?: number }) => {
			const invocation = resolvePiToolInvocation(toolName, args, { cwd: root, environment: {} })!;
			return { cwd: root, tool: toolName === "ls" ? createLsTool(root) : createReadTool(root), toolName, args,
				action: PI_ACTION_SEMANTICS.buildKey(toolName, args, root, "", { fingerprint: toolName, context: invocation })!,
				callID: toolName, signal: new AbortController().signal };
		};
		const execute = world.speculation!.execute, namespace = await execute(request("ls", { path: alias }));
		const payload = await execute(request("read", { path: path.join(location === "physical" ? data : alias, "value.txt") }));
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture");
		let composed: Awaited<ReturnType<typeof execute>> | undefined;
		try {
			const query = request("read", { path: path.join(alias, "value.txt") });
			composed = await execute({ ...query, inputs: () => [namespace.inputSource!, payload.inputSource!] });
			expect(captures).not.toHaveBeenCalled();
			await namespace.dispose();
			expect((await composed.validate!()).status).toBe("valid");
			expect(composed.inputResources?.some(input => path.resolve(input.path) === query.args.path)).toBe(true);
			const next = request("read", { path: path.join(alias, "value.txt"), offset: 2 });
			const result = await composed.reconstruct!({ ...next, inputs: () => [payload.inputSource!] });
			expect(result?.output.result).toEqual(await createReadTool(root).execute("reference", next.args));
			expect((await result?.validate?.())?.status).toBe("valid"); expect(captures).not.toHaveBeenCalled();
			await fs.unlink(alias); await fs.symlink(root, alias, process.platform === "win32" ? "junction" : "dir");
			expect((await composed.validate!()).status).toBe("stale");
			expect((await result?.validate?.())?.status).toBe("stale");
			await composed.dispose(); expect((await result?.validate?.())?.status).toBe("stale");
		} finally { captures.mockRestore(); await composed?.dispose(); await payload.dispose(); await namespace.dispose(); }
	});

	test("fills only missing inputs within one evaluation without expanding sealed read authority", async () => {
		const root = await workspace({ known: "A", missing: "B" }), manager = new ResourceVersionManager(root, { watch: false });
		const source = await manager.capture(undefined, 8192), extra = await manager.capture(undefined, 8192);
		const known = path.join(root, "known"), missing = path.join(root, "missing");
		await source.view!.readFile(known); source.view!.seal();
		const opened = vi.spyOn(fs, "open"), dependencies = new Set<string>();
		const fill = vi.fn(async () => ({ view: extra.view!, observed: (keys: ReadonlySet<string> | undefined) => {
			for (const key of keys ?? []) dependencies.add(key);
		} }));
		try {
			await expect(source.view!.evaluate(view => view.readFile(missing))).rejects.toThrow("resource_access_unproven");
			expect(await source.view!.evaluate(async view => (await Promise.all([known, missing, missing].map(file => view.readFile(file)))).map(bytes => bytes.toString()),
				undefined, root, undefined, fill)).toEqual(["A", "B", "B"]);
			extra.view!.seal();
			expect(opened.mock.calls.filter(([file]) => String(file) === known)).toHaveLength(0);
			expect(opened.mock.calls.filter(([file]) => String(file) === missing)).toHaveLength(1);
			expect([...dependencies].map(key => extra.observations.get(key)?.scope)).toContain("content");
			const before = fill.mock.calls.length;
			await expect(source.view!.evaluate(view => view.readFile(path.join(root, "..", "outside")), undefined, root, undefined, fill))
				.rejects.toThrow("resource_access_unproven");
			expect(fill).toHaveBeenCalledTimes(before);
			await expect(source.view!.evaluate(async view => {
				try { await view.readFile(missing); } catch { /* A tool cannot turn an unproven read into an adoptable result. */ }
				return "masked";
			}, undefined, root, undefined, async () => { throw new Error("capture denied"); })).rejects.toThrow("capture denied");
			await fs.writeFile(missing, "changed");
			expect((await manager.validate(extra)).expired).toBe(true);
		} finally { opened.mockRestore(); await source.release(); await extra.release(); manager.close(); }
	});

	test.each([false, true])("confines names, aliases and negative observations to the current root (composed=%s)", async composed => {
		const root = await workspace({ "inside/data.txt": "A", "outside/data.txt": "B" });
		const inside = path.join(root, "inside"), outside = path.join(root, "outside"), link = path.join(inside, "link");
		const directoryLink = process.platform === "win32" ? "junction" : "dir";
		await fs.symlink(outside, link, directoryLink);
		const manager = new ResourceVersionManager(root, { watch: false }), token = await manager.capture(undefined, 8192), view = token.view!;
		const boundaryManager = new ResourceVersionManager(inside, { watch: false }), boundary = await boundaryManager.capture(undefined, 8192);
		boundary.view!.seal();
		const evaluate = <T>(operation: (v: typeof view) => Promise<T>) => composed
			? boundary.view!.evaluate(operation, keys => { expect([...keys!].map(key => boundary.observations.get(key)?.scope)).toContain("resolution"); }, inside,
				() => [{ view, observed: keys => { expect([...keys!].every(key => token.observations.has(key))).toBe(true); } }])
			: view.evaluate(operation, undefined, inside);
		try {
			if (!composed) await view.stat(inside, "type");
			for (const target of [path.join(inside, "data.txt"), path.join(outside, "data.txt"), path.join(link, "data.txt")]) await view.readFile(target);
			for (const target of [path.join(inside, "missing"), path.join(link, "missing")]) expect(await view.exists(target)).toBe(false);
			view.seal();
			expect((await evaluate(v => v.readFile(path.join(inside, "data.txt")))).toString()).toBe("A");
			expect(await evaluate(v => v.exists(path.join(inside, "missing")))).toBe(false);
			for (const [target, missing] of [[path.join(outside, "data.txt"), false], [path.join(link, "data.txt"), false], [path.join(link, "missing"), true]] as const)
				await expect(evaluate(async v => missing ? await v.exists(target) : await v.readFile(target))).rejects.toThrow("resource_access_unproven");
			expect((await view.evaluate(v => v.readFile(path.join(outside, "data.txt")))).toString()).toBe("B");
			expect((await manager.validate(token)).expired).toBe(false);
		} finally { await token.release(); await boundary.release(); manager.close(); boundaryManager.close(); }

		// The same leaf can remain reachable after the root redirects elsewhere and a child points back.
		const alias = path.join(root, "scope"), bounce = path.join(outside, "bounce");
		await fs.symlink(inside, alias, directoryLink);
		await fs.mkdir(path.join(inside, "bounce")); await fs.writeFile(path.join(inside, "bounce/data.txt"), "same");
		await fs.symlink(path.join(inside, "bounce"), bounce, directoryLink);
		const scopedManager = new ResourceVersionManager(alias, { watch: false });
		const scoped = await scopedManager.capture([{ path: "bounce/data.txt", scope: "content" }], 8192);
		try {
			const leaf = path.join(alias, "bounce/data.txt"), before = await fs.realpath(leaf);
			await fs.unlink(alias); await fs.symlink(outside, alias, directoryLink);
			expect(await fs.realpath(leaf)).toBe(before);
			expect((await scopedManager.validate({ ...scoped, observations: new Map([...scoped.observations].filter(([, entry]) => entry.scope !== "resolution")) })).expired).toBe(false);
			expect((await scopedManager.validate(scoped)).expired).toBe(true);
		} finally { await scoped.release(); scopedManager.close(); }
	});

	test.each([false, true])("re-evaluates sealed bytes without expanding Actor observation authority (captured-only=%s)", async (capturedOnly) => {
		const text = "first\r\n\n[999 more lines in file. Use offset=3 to continue.]\n" + "x".repeat(60_000) + "\nlast";
		const root = await workspace({ "value.txt": text }), file = path.join(root, "value.txt");
		const args = { path: "value.txt", offset: 1, limit: 1 }, native = createReadTool(root);
		const stock = resolvePiToolInvocation("read", args, { cwd: root, environment: {} })!;
		const configuration = path.join(await workspace({ rule: "A" }), "rule");
		let afterRead: (() => Promise<void>) | undefined;
		const invocation = { ...stock, ...(capturedOnly ? { filesystemRoot: path.parse(root).root } : {}),
			filesystem: async (...request: Parameters<NonNullable<typeof stock.filesystem>>) => {
				if (capturedOnly) await request[0].readFile(configuration);
				const output = await stock.filesystem!(...request);
				await afterRead?.(); return output;
			},
		};
		const binding = { fingerprint: "original", context: invocation, ...(capturedOnly ? {
			semantics: { ...PI_ACTION_SEMANTICS.definition("read")!, resourceScope: "captured_inputs" as const },
		} : {}) };
		const key = PI_ACTION_SEMANTICS.buildKey("read", args, root, "", binding)!;
		if (capturedOnly) await expect(captureResourceVersion(key, root, PI_ACTION_SEMANTICS, 100_000)).rejects.toThrow("resource_dependencies_unproven");
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 100_000 });
		const signal = new AbortController().signal;
		if (capturedOnly) {
			const unbound = PI_ACTION_SEMANTICS.buildKey("read", args, root, "", { ...binding, context: { ...invocation, filesystemRoot: undefined } })!;
			await expect(world.speculation!.execute({ cwd: root, tool: native, toolName: "read", args, action: unbound, callID: "denied", signal })).rejects.toThrow("escapes workspace");
		}
		const execute = () => world.speculation!.execute({ cwd: root, tool: native, toolName: "read", args, action: key, callID: "spec", signal });
		const branch = await execute();
		const reconstructed = vi.spyOn(branch, "reconstruct");
		try {
			const expanded = PI_ACTION_SEMANTICS.buildKey("read", args, root, "", { ...binding,
				semantics: { ...PI_ACTION_SEMANTICS.definition("read")!, requirements: { capabilities: ["filesystem.read", "validation.resource_snapshot", "network.mediate"] } } })!;
			expect(await branch.reconstruct!({ action: expanded, args, callID: "unsupported-effects", signal })).toBeUndefined();
			for (const query of [{ path: "@value.txt", offset: 2, limit: 0 }, { path: "value.txt", offset: 3 },
				{ path: "@value.txt", offset: 4, limit: 1 }, { path: file, offset: 5 }]) {
				const action = PI_ACTION_SEMANTICS.buildKey("read", query, root, "", binding)!;
				expect((await branch.reconstruct!({ action, args: query, callID: "actor", signal }))?.output.result)
					.toEqual(await native.execute("native", query));
			}
			if (capturedOnly) {
				const query = { path: configuration }, action = PI_ACTION_SEMANTICS.buildKey("read", query, path.dirname(configuration), "", binding)!;
				const narrow = await branch.reconstruct!({ action, args: query, callID: "configuration", signal });
				expect(narrow?.output.result).toEqual(await native.execute("native", query));
				await fs.writeFile(file, "unrelated input changed");
				expect((await branch.validate!()).status).toBe("stale");
				expect(await narrow?.validate?.()).toMatchObject({ status: "valid", metrics: { filesRead: 1 } });
				await fs.writeFile(configuration, "changed query input");
				expect((await narrow?.validate?.())?.status).toBe("stale");
				await fs.writeFile(file, text); await fs.writeFile(configuration, "A");
			}
			await expect(branch.reconstruct!({ action: key, args: { path: "unproven" }, callID: "bad", signal })).rejects.toThrow("unproven");
			expect((await branch.validate!()).status).toBe("valid");
			expect((await branch.reconstruct!({ action: key, args, callID: "retry", signal }))?.output.result).toEqual(await native.execute("native", args));
			await fs.writeFile(configuration, "B");
			expect((await branch.validate!()).status).toBe(capturedOnly ? "stale" : "valid");
		} finally {
			await Promise.allSettled(reconstructed.mock.results.map(async ({ value }) => (await value)?.dispose?.()));
			await branch.dispose();
		}
		for (const target of capturedOnly ? [file, configuration] : [file]) {
			const before = await fs.readFile(target), expected = await native.execute("control", args);
			afterRead = () => fs.writeFile(target, "changed before sealing");
			const retained = await execute(); afterRead = undefined;
			try {
				expect(retained.output.result).toEqual(expected);
				expect((await retained.validate!()).status).toBe("stale");
				await fs.writeFile(target, before);
				expect((await retained.validate!()).status).toBe("valid");
				expect((await retained.commit()).result).toEqual(expected);
			} finally { await fs.writeFile(target, before); await retained.dispose(); }
		}
	});

	test.runIf(process.platform !== "win32")("capture and branch share one resource lifetime", async () => {
		const root = await workspace({ "value.txt": "A" });
		const file = path.join(root, "value.txt");
		const key = action("read", ["value.txt"]);
		const probe = await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, 8192);
		const manager = probe.manager;
		const world = createResourceSnapshotExecutionWorld();
		const capture = await world.observation!.capture({
			cwd: root, tool: {} as never, toolName: "read", args: { path: "value.txt" },
			action: key, callID: "actor-read", signal: new AbortController().signal,
		});
		const actorOutput = { result: { content: [{ type: "text" as const, text: "A" }], details: {} }, isError: false };
		const branch = await capture.seal(actorOutput);
		expect(manager.changesSince(probe).uncertain).toBe(true); // Later watcher startup cannot recover the earlier capture window.
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

	test.for([["empty", "short", "chunks"], ["admission", "settled"], ["grow", "shrink", "read-error", "cancel-pinned", "cancel-reading"], ["replace"], ["seal"]])("owns the file identity through %j", async (changes, { skip }) => {
		if (process.platform === "win32" && changes.includes("replace")) return skip("Windows denies replacement of the open destination");
		for (const change of changes) for (const mode of ["hash", "content", "executable"]) {
			const executable = mode === "executable", retain = mode === "content";
			if (executable && ["admission", "seal", "settled"].includes(change)) continue; // Only path captures certify pathname stability.
			if (change.startsWith("cancel-") && !executable) continue;
			const controller = new AbortController(), cancelled = new Error("cancelled observation");
			const payload = change === "chunks" ? Buffer.alloc(2 * 1024 * 1024 + 7, 43) : Buffer.from(change === "empty" ? "" : "initial contents");
			const root = await workspace({ value: payload }), file = path.join(root, "value");
			const nativeOpen = fs.open.bind(fs), handle = await nativeOpen(file, "r"), read = handle.read.bind(handle), stat = handle.stat.bind(handle);
			let inspections = 0;
			const open = vi.spyOn(fs, "open").mockImplementation(async (target, flags, mode) => {
				if (!isDataOpen(flags)) return nativeOpen(target, flags, mode);
				if (change === "admission") await fs.appendFile(file, "more");
				return handle.fd < 0 ? nativeOpen(target, flags, mode) : handle;
			});
			vi.spyOn(handle, "stat").mockImplementation((async (...args: Parameters<typeof handle.stat>) => {
				const result = await stat(...args);
				if (++inspections === 2 && change === "seal") await fs.appendFile(file, "more"); // After final fstat, before path proof.
				return result;
			}) as typeof handle.stat);
			vi.spyOn(handle, "read").mockImplementationOnce((async (buffer: Buffer) => {
				if (change === "cancel-reading") controller.abort(cancelled);
				if (change === "read-error") throw new Error("injected read failure");
				if (change === "grow") await fs.appendFile(file, "more");
				if (change === "shrink") await fs.truncate(file, 1);
				if (change === "replace") { const replacement = path.join(root, "new"); await fs.writeFile(replacement, payload); await fs.rename(replacement, file); }
				return read(buffer, 0, Math.min(3, buffer.byteLength), null);
			}) as typeof handle.read);
			const manager = new ResourceVersionManager(root, { watch: false }), nativeReaddir = fs.readdir;
			const { promise: closed, resolve: finish } = deferred(), close = handle.close.bind(handle);
			vi.spyOn(handle, "close").mockImplementationOnce(async () => { await close(); finish(); });
			const readdir = change === "settled" ? vi.spyOn(fs, "readdir").mockImplementation((async (...args: Parameters<typeof fs.readdir>) => {
				await closed; await fs.appendFile(file, "more"); return nativeReaddir(...args);
			}) as typeof fs.readdir) : undefined;
			try {
				const capture = change === "settled" ? manager.capture([{ path: file, scope: "content" }, { path: root, scope: "tree_content" }], retain ? 8192 : undefined)
					: executable ? hashExecutableFile(file, { signal: controller.signal, pinned: () => {
						expect(inspections).toBe(1); // The opened image identity was checked before native execution can resume.
						if (change === "cancel-pinned") controller.abort(cancelled);
					} }) : captureStableFile(file, Infinity, retain);
				if (["empty", "short", "chunks"].includes(change)) {
					const hash = createHash("sha256").update(payload).digest("hex");
					if (executable) expect(await capture).toBe(`sha256:${hash}`);
					else expect(await capture).toMatchObject({ hash, bytesRead: payload.length, ...(retain ? { content: payload } : {}) });
					if (!retain) for (const [buffer] of vi.mocked(handle.read).mock.calls) {
						expect(Buffer.isBuffer(buffer) ? buffer.byteLength : Infinity).toBeLessThanOrEqual(1024 * 1024);
					}
				} else await expect(capture).rejects.toThrow(change.startsWith("cancel-") ? cancelled.message : change === "read-error" ? "injected read failure" : "file_changed_during_capture");
				if (change === "admission" || change === "cancel-pinned") expect(handle.read).not.toHaveBeenCalled();
				if (change === "cancel-reading") expect(handle.read).toHaveBeenCalledOnce();
				expect(handle.fd).toBe(-1);
			} finally { open.mockRestore(); readdir?.mockRestore(); manager.close(); }
		}
	});

	test("reads a hard-link alias independently of another capture's ongoing read", async () => {
		const payload = Buffer.alloc(2 * 1024 * 1024 + 7, 43), root = await workspace({ image: payload }), gate = gated();
		const file = path.join(root, "image"), alias = path.join(root, "alias"), nativeOpen = fs.open.bind(fs);
		await fs.link(file, alias);
		let opened = 0;
		const open = vi.spyOn(fs, "open").mockImplementation(async (target, flags, permissions) => {
			const handle = await nativeOpen(target, flags, permissions), read = handle.read.bind(handle);
			if (isDataOpen(flags) && opened++ === 0) vi.spyOn(handle, "read").mockImplementationOnce((async (...args: Parameters<typeof handle.read>) => {
				await gate.wait(); return read(...args);
			}) as typeof handle.read);
			return handle;
		});
		try {
			const owner = captureStableFile(file), hash = createHash("sha256").update(payload).digest("hex"); await gate.entered;
			// Bytes the owner already read could predate a same-size rewrite that coarse timestamps hide from this stat.
			expect(await captureStableFile(alias)).toMatchObject({ hash, bytesRead: payload.length });
			gate.release();
			expect(await owner).toMatchObject({ hash, bytesRead: payload.length });
		} finally { gate.release(); open.mockRestore(); }
	});

	test.runIf(process.platform === "linux")("distinguishes pinned images from pathname snapshots after alias targets change", async () => {
		for (const change of ["replace", "unlink", "rewrite"]) {
			const root = await workspace({ image: "old" }), file = path.join(root, "image"), handle = await fs.open(file, "r");
			const alias = `/proc/self/fd/${handle.fd}`, digest = (value: string) => `sha256:${createHash("sha256").update(value).digest("hex")}`;
			const manager = new ResourceVersionManager(path.parse(root).root, { watch: false });
			try {
				expect(await hashExecutableFile(alias)).toBe(digest("old"));
				if (change === "replace") { await fs.rename(file, path.join(root, "old")); await fs.writeFile(file, "new"); }
				else if (change === "unlink") await fs.unlink(file);
				else await fs.writeFile(file, "new");
				expect(await hashExecutableFile(alias)).toBe(digest(change === "rewrite" ? "new" : "old"));
				await expect(captureStableFile(alias)).rejects.toThrow("not_regular_file");
				for (const decoy of change === "unlink" ? [false, true] : [false]) {
					if (decoy) {
						const displayed = await fs.readlink(alias); expect(path.dirname(displayed)).toBe(root);
						await fs.writeFile(displayed, "decoy");
					}
					const metadata = await manager.capture([{ path: alias, scope: "entry" }], 8192), lazy = await manager.capture(undefined, 8192);
					const open = vi.spyOn(fs, "open");
					try {
						expect((await manager.validate(metadata)).expired).toBe(false);
						expect((await metadata.view!.stat(alias, "entry")).type).toBe("symlink");
						if (change === "unlink") {
							await expect(manager.capture([{ path: alias, scope: "content" }], 8192)).rejects.toThrow("filesystem_link_resolution_changed");
							await expect(lazy.view!.readFile(alias)).rejects.toThrow("filesystem_link_resolution_changed");
							expect(open).not.toHaveBeenCalled();
						} else expect((await lazy.view!.readFile(alias)).toString()).toBe(change === "rewrite" ? "new" : "old");
					} finally { open.mockRestore(); await metadata.release(); await lazy.release(); }
				}
			} finally { manager.close(); await handle.close(); }
		}
	});

	test.for(["file", "directory", "relative"] as const)("resolves sealed %s link chains without granting unproven paths", async (kind, { skip }) => {
		if (kind !== "directory" && process.platform === "win32") return skip("file symlinks require Windows privileges");
		const directory = kind === "directory", relative = kind === "relative", resource = relative ? "nested/deep/.git" : ".git";
		const root = await workspace({ [directory ? resource + "/value.txt" : resource]: "before", ...(relative ? { "deep/.git": "lexical decoy" } : {}) });
		const outside = await workspace({ "value.txt": "external" });
		const target = path.join(root, resource), alias = path.join(root, "alias"), link = path.join(root, "input");
		const content = directory ? path.join(target, "value.txt") : target;
		const type = directory ? process.platform === "win32" ? "junction" : "dir" : "file";
		if (relative) await fs.symlink("nested/deep", path.join(root, "parts"), "dir");
		await fs.symlink(relative ? "parts/../deep/.git" : target, link, type); await fs.symlink(link, alias, type);
		const query = path.join(root, "query");
		if (relative) { await fs.mkdir(query); await fs.symlink(alias, path.join(query, "alias")); }
		const manager = new ResourceVersionManager(root, { watch: false });
		const token = await manager.capture([{ path: alias, scope: directory ? "tree_content" : "content" }], 8192);
		const snapshot = new ResourceVersionManager(root, { watch: false, snapshotExcludes: [".git"] });
		const baseline = await snapshot.capture([{ path: ".", scope: "tree_content" }]);
		try {
			const name = directory ? "ls" : "read", args = { path: alias };
			const native = directory ? createLsTool(root) : createReadTool(root);
			const actual = await resolvePiToolInvocation(name, args, { cwd: root, environment: {} })!.filesystem!(token.view!,
				{ args, callID: "spec", signal: new AbortController().signal });
			token.view!.assertComplete();
			expect(await token.view!.stat(alias, "entry")).toMatchObject({ type: "symlink", link: await fs.readlink(alias),
				realPath: path.join(await fs.realpath(root), "alias") });
			expect(await token.view!.stat(directory ? path.join(alias, "value.txt") : target, "entry")).toMatchObject({ type: "file", realPath: await fs.realpath(content) });
			const expected = await native.execute("actor", args);
			expect(actual.result.content).toEqual(expected.content);
			expect(Object.entries(actual.result.details ?? {})).toEqual(Object.entries(expected.details ?? {}));
			for (const entryFirst of [false, true]) {
				const lazy = await manager.capture(undefined, 8192), view = lazy.view!;
				try {
					if (entryFirst) await view.stat(alias, "entry");
					if (directory) await view.readdir(alias); else await view.readFile(alias);
					const count = lazy.observations.size;
					expect((await view.stat(alias, "entry")).realPath).toBe(path.join(await fs.realpath(root), "alias"));
					expect((await view.stat(alias, "type")).realPath).toBe(await fs.realpath(target));
					expect(lazy.observations.size).toBe(count);
					view.seal(); expect((await manager.validate(lazy)).expired).toBe(false);
				} finally { await lazy.release(); }
			}
			const leaf = directory ? path.join(alias, "value.txt") : alias;
			let dependencies: ReadonlySet<string> | undefined;
			await token.view!.evaluate((view) => view.readFile(leaf), (observed) => { dependencies = observed; });
			expect(token.view!.resources.filter(input => input.descendants).map(input => path.basename(input.path)).sort())
				.toEqual(["alias"]); // Only the requested alias owns a query entry; intermediate links remain binding evidence.
			expect(dependencies?.size).toBe(2); // Child reads retain the complete alias chain and current root resolution.
			const scoped = { ...token, observations: new Map([...token.observations].filter(([key]) => dependencies!.has(key))) };
			for (const requested of [leaf, ...(relative ? [query] : [])]) for (const replaced of [link, ...(relative ? [path.dirname(target)] : [])]) {
				const observed = await manager.capture([{ path: requested, scope: requested === query ? "tree_content" : "content" }]);
				const parked = path.join(path.dirname(replaced), "parked");
				await fs.rename(replaced, parked);
				try {
					if (replaced === link) await fs.symlink(directory ? outside : path.join(outside, "value.txt"), link, type);
					else { await fs.mkdir(replaced); await fs.writeFile(path.join(replaced, ".git"), "external"); }
					expect(await fs.readFile(leaf, "utf8")).toBe("external"); // Actor sees B while the original leaf and links remain intact.
					expect((await manager.validate(scoped)).expired).toBe(true);
				} finally { await fs.rm(replaced, { force: true, recursive: replaced !== link }); await fs.rename(parked, replaced); }
				try {
					expect(await fs.readFile(leaf, "utf8")).toBe("before");
					expect((await manager.seal(observed)).expired).toBe(true); // Restoring the same directory or junction cannot certify the window.
				} finally { await observed.release(); }
			}
			expect((await snapshot.validate(baseline)).expired).toBe(false);
			await fs.writeFile(content, "after!");
			expect((await snapshot.validate(baseline)).expired).toBe(true); // Visible aliases retain their excluded targets' evidence.
			expect((await token.view!.readFile(directory ? path.join(alias, "value.txt") : alias)).toString()).toBe("before");
			expect(await manager.validate(token)).toMatchObject({ expired: true, mode: "exact", bytesRead: 6 });
			const escape = path.join(root, "escape");
			await fs.symlink(directory ? outside : path.join(outside, "value.txt"), escape, type);
			await expect(manager.capture([{ path: escape, scope: "tree_content" }], 8192)).rejects.toThrow("resource_symlink_escapes_workspace");
			for (const destination of [directory ? outside : path.join(outside, "value.txt"), path.join(root, "missing"), escape]) {
				await fs.rm(escape); await fs.symlink(destination, escape, type);
				const metadata = await manager.capture(undefined, 8192), opened = vi.spyOn(fs, "open");
				try {
					expect(await metadata.view!.stat(escape, "entry")).toMatchObject({ type: "symlink", link: await fs.readlink(escape), size: undefined,
						realPath: path.join(await fs.realpath(root), path.basename(escape)) });
					metadata.view!.seal();
					expect(await manager.validate(metadata)).toMatchObject({ expired: false, filesRead: 0, bytesRead: 0 });
					await expect(metadata.view!.evaluate((view) => view.readFile(escape))).rejects.toThrow("resource_access_unproven");
					if (destination === escape) await expect(manager.capture([{ path: escape, scope: "content" }])).rejects.toThrow("resource_symlink_cycle");
					await fs.rm(escape); await fs.symlink(path.join(root, "changed"), escape, type);
					expect((await manager.validate(metadata)).expired).toBe(true);
					expect(opened).not.toHaveBeenCalled();
				} finally { opened.mockRestore(); metadata.release(); }
			}
			await expect(token.view!.exists(path.join(alias, "unproven"))).rejects.toThrow("resource_access_unproven");
		} finally { token.release(); manager.close(); baseline.release(); snapshot.close(); }
	});

	test("rejects non-regular paths before data access and inconsistent virtual file sizes", async () => {
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
				await expect(hashExecutableFile(target)).rejects.toThrow("not_regular_file");
				await expect(manager.capture([{ path: target, scope: "content" }])).rejects.toThrow("unsupported_resource_type:");
			}
			expect(open.mock.calls.filter(([, flags]) => isDataOpen(flags))).toHaveLength(0);
			if (process.platform === "linux") for (const capture of [captureStableFile, hashExecutableFile]) {
				await expect(capture("/proc/version")).rejects.toThrow("file_changed_during_capture");
			}
		} finally { open.mockRestore(); manager.close(); }
	});

	test.runIf(process.platform === "linux")("rejects a replaced FIFO without admitting a writer", async () => {
		for (const phase of ["binding", "data", "opened"]) {
			const root = await workspace({ value: "A" }), file = path.join(root, "value"), nativeOpen = fs.open.bind(fs);
			let replaced = false, admitted = false;
			const handles: Awaited<ReturnType<typeof fs.open>>[] = [];
			const open = vi.spyOn(fs, "open").mockImplementation(async (target, flags, mode) => {
				const replace = !replaced && (phase === "binding" ? target === file : isDataOpen(flags));
				if (replace && phase !== "opened") { await fs.unlink(file); await execFileAsync("mkfifo", [file]); }
				const descriptor = await nativeOpen(target, flags, mode); handles.push(descriptor);
				if (replace) {
					replaced = true;
					if (phase === "opened") { await fs.unlink(file); await execFileAsync("mkfifo", [file]); }
					try {
						const writer = await nativeOpen(file, constants.O_WRONLY | constants.O_NONBLOCK);
						admitted = true; await writer.close();
					} catch (error) { expect((error as NodeJS.ErrnoException).code).toBe("ENXIO"); }
				}
				return descriptor;
			});
			try {
				await expect(captureStableFile(file)).rejects.toThrow();
				expect({ replaced, admitted, closed: handles.every((handle) => handle.fd === -1) }, phase)
					.toEqual({ replaced: true, admitted: false, closed: true });
			} finally { open.mockRestore(); }
		}
	});

	test.each([
		{ scope: "entry" as const, stale: ["kind"] },
		{ scope: "type" as const, stale: ["kind"] },
		{ scope: "stat" as const, stale: ["content", "kind"] },
		{ scope: "entries" as const, stale: ["entry", "kind"] },
		{ scope: "tree_entries" as const, stale: ["entry", "deep", "kind"] },
		{ scope: "tree_content" as const, stale: ["content", "entry", "deep", "kind", "config", "metadata", "nested_metadata"] },
		{ scope: "tree_content" as const, stale: ["content", "entry", "deep", "kind", "config"], snapshotExcludes: [".git"] },
	])("validates exactly $scope with snapshot exclusions $snapshotExcludes", async ({ scope, stale, snapshotExcludes }) => {
		for (const [change, relative] of Object.entries({ content: "src/value.ts", entry: "src/added.ts",
			deep: "src/nested/added.ts", outside: ".gitignore", kind: "src/value.ts", config: "src/.gitignore",
			metadata: "src/.git/config", nested_metadata: "src/nested/.git/config" })) {
			const root = await workspace({ "src/value.ts": "one\n", "src/nested/existing.ts": "", "src/.gitignore": "",
				"src/.git/config": "metadata", "src/nested/.git/config": "nested metadata" });
			const excludes = [...snapshotExcludes ?? []];
			const manager = new ResourceVersionManager(root, { watch: false, snapshotExcludes: excludes });
			excludes.length = 0; // Caller mutation cannot change a manager's proof boundary.
			const dependencies = [{ path: ["entry", "type", "stat"].includes(scope) ? "src/value.ts" : "src", scope }];
			const token = await manager.capture(dependencies, snapshotExcludes ? undefined : 4096);
			if (snapshotExcludes && change === "content") {
				await expect(manager.capture(dependencies, 4096)).rejects.toThrow("resource_filtered_snapshot_not_readable");
				expect((await manager.seal(token)).expired).toBe(true);
			}
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
				expect(await runThinkThreadTool({ tool: "read", args,
					callID: "runner", autoResizeImages, modelSupportsImages }, root)).toEqual(output);
			}
		} finally { releaseResourceVersion(token); }
	});

});

function action(tool: "read" | "ls", resources: ReadonlyArray<string>) {
	return buildActionKey({ tool, resources, input: { path: resources[0] } });
}

async function workspace(files: Readonly<Record<string, string | Buffer>> = {}) {
	const root = await directories.create();
	await Promise.all(Object.entries(files).map(async ([name, content]) => {
		await fs.mkdir(path.dirname(path.join(root, name)), { recursive: true });
		await fs.writeFile(path.join(root, name), content);
	}));
	return root;
}
