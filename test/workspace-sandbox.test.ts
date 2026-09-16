import { temporaryDirectories } from "./filesystem.ts";
import { gated, nextTurn } from "./async.ts";
import { runProgram, shellQuote } from "./command.ts";
import { constants as fsConstants } from "node:fs";
import { access, chmod, type FileHandle, link, mkdir, mkdtemp, open, readFile, readdir, rm, stat, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createEditTool, createWriteTool, withFileMutationQueue } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import { ActionSemanticsRegistry, buildPiActionKey } from "../src/action-semantics.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import {
	effectCapabilitiesCover,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ToolInvocation, ToolSettlement } from "../src/tool-settlement.ts";
import { LinuxOverlayfsCapabilityRegistry, linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
import { advanceFilesystemClock } from "../src/filesystem-evidence.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { isPoisonedEffectCommit } from "../src/effect-transaction.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";
import {
	readSandboxDirectoryState,
	WorkspaceSandboxService,
	type SandboxFileChange,
} from "../src/workspace-sandbox.ts";

const writeTool = createWriteTool(process.cwd());
const editTool = createEditTool(process.cwd());

let sandbox: WorkspaceSandboxService;
const { create: temporaryRoot, dispose: disposeRoots } = temporaryDirectories("pi-spec-");
beforeEach(() => { sandbox = new WorkspaceSandboxService(); });
vi.mock("node:fs/promises", async (original) => {
	const fs = await original<typeof import("node:fs/promises")>();
	return { ...fs, mkdir: vi.fn(fs.mkdir), mkdtemp: vi.fn(fs.mkdtemp), rm: vi.fn(fs.rm), writeFile: vi.fn(fs.writeFile) };
});

afterEach(async () => {
	try { await sandbox.dispose(); }
	finally { await disposeRoots(); }
});

describe("workspace-branch ExecutionWorld", () => {
	it("retains shared baseline work when a preparation owner cancels", async () => {
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		for (const phase of ["repository", "baseline"]) for (const owner of ["none", "active", "cancelled"]) {
			const root = await temporaryRoot(), controller = new AbortController();
			let workspaces = 0, captures = 0;
			const gate = gated();
			const capture = ResourceVersionManager.prototype.capture;
			const observer = vi.spyOn(ResourceVersionManager.prototype, "capture").mockImplementation(async function (this: ResourceVersionManager, ...args) {
				captures++;
				const token = await capture.apply(this, args);
				if (phase === "baseline") await gate.wait();
				return token;
			});
			vi.mocked(mkdtemp).mockImplementation(async (prefix, options) => {
				const directory = await fs.mkdtemp(prefix, options);
				if (String(prefix).endsWith(`${path.sep}action-`)) workspaces++;
				if (phase === "repository" && String(prefix).includes("pi-speculative-action-pool-")) await gate.wait();
				return directory;
			});
			const pending = sandbox.prepare(root, { driver: "git", signal: controller.signal });
			try {
				await Promise.race([gate.entered, pending.then(() => { throw new Error("Preparation did not reach the held stage"); })]);
				const other = owner === "none" ? Promise.resolve() : sandbox.prepare(root,
					{ driver: "git", ...(owner === "cancelled" ? { signal: controller.signal } : {}) });
				controller.abort(new Error("owner closed")); gate.release();
				const [cancelled, live] = await Promise.allSettled([pending, other]);
				expect(cancelled).toMatchObject({ status: "rejected", reason: { message: "owner closed" } });
				expect(live.status).toBe(owner === "cancelled" ? "rejected" : "fulfilled");
				expect(workspaces).toBe(Number(owner === "active"));
				expect(captures).toBe(Number(phase === "baseline" || owner === "active"));
				const branch = await sandbox.createExecutionWorld({ driver: "git" }).speculation.execute(
					context(root, "write", writeTool, { path: "value.txt", content: "live owner\n" }));
				await branch.commit(); await branch.dispose();
				expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("live owner\n");
			} finally {
				gate.release(); await pending.catch(() => undefined); observer.mockRestore(); vi.mocked(mkdtemp).mockImplementation(fs.mkdtemp);
				await sandbox.closePools([root]);
			}
		}
	});

	it.each(["explicit", "idle", "idle-replaced"])("owns %s pool retirement through service disposal", async (retirement) => {
		const root = await temporaryRoot();
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const first = new WorkspaceSandboxService();
		const second = new WorkspaceSandboxService();
		const firstWorld = first.createExecutionWorld({ driver: "git" });
		const firstSibling = first.createExecutionWorld({ driver: "git" });
		const secondWorld = second.createExecutionWorld({ driver: "git" });
		const signal = new AbortController().signal, validations = vi.spyOn(ResourceVersionManager.prototype, "validate");
		const changes = vi.spyOn(ResourceVersionManager.prototype, "changesSince");
		const gate = gated(), schedule = globalThis.setTimeout;
		let expire: (() => void) | undefined, pool: string | undefined, closed = false;
		const removals = new Map<string, number>();
		const timers = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback, ms, ...args) => {
			const timer = schedule(callback, ms, ...args);
			if (ms === 5 * 60 * 1000) expire = () => { clearTimeout(timer); callback(...args); };
			return timer;
		});
		try {
			await writeFile(path.join(root, "value.txt"), "before\n", "utf8");
			await Promise.all([
				firstWorld.speculation.prepare?.({ cwd: root, signal }),
				firstSibling.speculation.prepare?.({ cwd: root, signal }),
				secondWorld.speculation.prepare?.({ cwd: root, signal }),
			]);
			await firstSibling.speculation.prepare?.({ cwd: root, signal: new AbortController().signal });
			expect(validations).toHaveBeenCalledTimes(2); // One initial validation per service, shared across warm generations.
			changes.mockReturnValue({ uncertain: true, paths: [] });
			const failure = new Error("baseline validation failed");
			validations.mockRejectedValueOnce(failure);
			await expect(firstWorld.speculation.prepare?.({ cwd: root, signal })).rejects.toBe(failure);
			await firstSibling.speculation.prepare?.({ cwd: root, signal });
			expect(validations).toHaveBeenCalledTimes(4); // Failed warm-up does not poison retry.
			const abandoned = await firstWorld.speculation.execute(
				context(root, "write", writeTool, { path: "value.txt", content: "abandoned\n" }),
			);

			vi.mocked(rm).mockImplementation(async (target, options) => {
				if (path.dirname(String(target)) === path.resolve(os.tmpdir()) && path.basename(String(target)).startsWith("pi-speculative-action-pool-")) {
					pool = String(target); removals.set(pool, (removals.get(pool) ?? 0) + 1);
					await gate.wait();
				}
				return fs.rm(target, options);
			});
			if (retirement !== "explicit") {
				expect(expire).toBeDefined(); expire!(); await gate.entered;
			}
			if (retirement === "idle-replaced") await first.prepare(root, { driver: "git" });
			const closing = Promise.all([firstWorld.dispose?.(), firstSibling.dispose?.(), first.dispose()]).then(() => { closed = true; });
			await gate.entered;
			await nextTurn();
			expect(closed).toBe(false);
			expect((await stat(pool!)).isDirectory()).toBe(true);
			gate.release(); await closing;
			expect([...removals.values()]).toEqual(retirement === "idle-replaced" ? [1, 1] : [1]);
			for (const directory of removals.keys()) await expect(stat(directory)).rejects.toThrow();
			await expect(first.prepare(root, { driver: "git" })).rejects.toThrow("service is disposed");
			await expect(abandoned.commit()).rejects.toThrow("service is disposed");

			const args = { path: "value.txt", content: "after\n" };
			const branch = await secondWorld.speculation.execute(context(root, "write", writeTool, args));
			await branch.commit();
			expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("after\n");
		} finally {
			gate.release(); timers.mockRestore(); vi.mocked(rm).mockImplementation(fs.rm);
			validations.mockRestore(); changes.mockRestore();
			await Promise.allSettled([first.dispose(), secondWorld.dispose?.(), second.dispose()]);
		}
	});

	it("retires every selected pool when another pool fails to close", async () => {
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const owner = new WorkspaceSandboxService(), pools: string[] = [], removed: string[] = [];
		const roots = await Promise.all(["failed", "healthy", "retained"].map(() => temporaryRoot()));
		vi.mocked(mkdtemp).mockImplementation(async (prefix, options) => {
			const directory = await fs.mkdtemp(prefix, options);
			if (String(prefix).endsWith("pi-speculative-action-pool-")) pools.push(directory);
			return directory;
		});
		try {
			for (const root of roots) await owner.prepare(root, { driver: "git" });
			expect(pools).toHaveLength(3);
			vi.mocked(rm).mockImplementation(async (target, options) => {
				if (pools.includes(String(target))) {
					removed.push(String(target));
					if (String(target) === pools[0]) throw new Error("pool removal failed");
				}
				return fs.rm(target, options);
			});
			await expect(owner.closePools(roots.slice(0, 2))).rejects.toThrow("pool removal failed");
			expect(removed.sort()).toEqual(pools.slice(0, 2).sort());
			await expect(stat(pools[1]!)).rejects.toThrow();
			expect((await stat(pools[2]!)).isDirectory()).toBe(true);
			await owner.dispose();
			expect(removed.sort()).toEqual([...pools].sort());
		} finally {
			vi.mocked(mkdtemp).mockImplementation(fs.mkdtemp); vi.mocked(rm).mockImplementation(fs.rm);
			await owner.dispose();
			for (const pool of pools) {
				expect(path.dirname(pool)).toBe(path.resolve(os.tmpdir()));
				expect(path.basename(pool)).toMatch(/^pi-speculative-action-pool-/);
				await fs.rm(pool, { recursive: true, force: true });
			}
		}
	});

	it("qualifies auto OverlayFS by the exact immutable baseline size", async ({ skip }) => {
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const root = await temporaryRoot();
		await writeFile(path.join(root, "small.txt"), "small\n", "utf8");
		expect(await sandbox.fingerprint({ driver: "auto" }, root)).toBe("git-worktree");
		await Promise.all(
			Array.from({ length: 100 }, (_value, index) =>
				writeFile(path.join(root, `${index.toString().padStart(4, "0")}.txt`), `${index}\n`, "utf8"),
			),
		);
		expect(await sandbox.fingerprint({ driver: "auto" }, root)).toBe("git-worktree");
		await Promise.all(
			Array.from({ length: 160 }, (_value, index) => {
				const ordinal = index + 100;
				return writeFile(path.join(root, `${ordinal.toString().padStart(4, "0")}.txt`), `${ordinal}\n`, "utf8");
			}),
		);
		expect(await sandbox.fingerprint({ driver: "auto" }, root)).toMatch(/^linux-overlayfs:/);
	});

	it("binds stock file operations without invoking host functions or rewriting outputs", async () => {
		const root = await temporaryRoot();
		await writeFile(path.join(root, ".gitattributes"), "* text eol=lf ident\n");
		const world = sandbox.createExecutionWorld({ driver: "git" });
		expect(world.scope).toBe("fallback");
		expect(effectCapabilitiesCover(world.speculation.capabilities, WORKSPACE_PATH_MUTATION_EFFECTS)).toBe(true);
		expect(effectCapabilitiesCover(world.speculation.capabilities, UNRESTRICTED_PROCESS_EFFECTS)).toBe(false);
		const target = path.join(root, "nested/created.txt"), before = `\uFEFFbefore ${root}\r\n`;
		const forbidden = { ...writeTool, execute: vi.fn(async () => { throw new Error("host function invoked"); }) };
		for (const [name, args, initial] of [
			["write", { path: "@nested/created.txt", content: before }, undefined],
			["write", { path: "@nested/created.txt", content: before }, before],
			["edit", { path: target, edits: [{ oldText: "before", newText: "after" }] }, before],
		] as const) {
			const native = name === "write" ? createWriteTool(root) : createEditTool(root);
			const expected = await native.execute("actor", args as never);
			const expectedBytes = await readFile(target);
			if (initial === undefined) await rm(path.dirname(target), { recursive: true, force: true });
			else await writeFile(target, initial);
			const request = context(root, name, forbidden, args);
			await expect(world.speculation.execute({ ...request, action: { ...request.action, executionContext: undefined } }))
				.rejects.toThrow("explicitly bound");
			const branch = await world.speculation.execute(request);
			expect(branch.output).toEqual({ result: expected, isError: false });
			if (initial === undefined) await expect(stat(target)).rejects.toThrow();
			else expect(await readFile(target, "utf8")).toBe(initial);
			const first = branch.commit();
			expect(branch.commit()).toBe(first);
			await first;
			expect(branch.commitMetrics).toMatchObject({ resourcesCommitted: initial === undefined ? 2 : 1 });
			expect(await readFile(target)).toEqual(expectedBytes);
		}
		expect(forbidden.execute).not.toHaveBeenCalled();
	});

	it.each(["capture", "refine", "execute", "checkpoint"])("owns %s deltas across branch commit and descendant materialization", async (boundary) => {
		const root = await temporaryRoot();
		await writeFile(path.join(root, "value.txt"), "before\n", "utf8");
		const action = buildPiActionKey("write", { path: "value.txt", content: "after\n" }, root);
		if (!action) throw new Error("action key missing");
		let observed: { readonly content: string; readonly before: string; readonly after: string } | undefined;
		let retained: SandboxFileChange | undefined;
		const branch = await sandbox.fork({
			cwd: root,
			action,
			execute: async (workspace) => {
				await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "after\n", "utf8");
				const metadata = path.join(workspace.sandboxRoot, ".git");
				await rm(metadata, { force: true }); await writeFile(metadata, "gitdir: missing\n");
				return boundary === "execute" ? { output: settlement("done"), changes: [
					retained = fileTransition(root, "value.txt", "before\n", "after\n"),
				] } : settlement("done");
			},
			afterCapture: async (workspace, capture) => {
				const change = capture.changes[0];
				if (!change || change.kind === "directory" || !change.before || !change.after) {
					throw new Error("captured change missing");
				}
				observed = {
					content: await readFile(path.join(workspace.sandboxRoot, "value.txt"), "utf8"),
					before: Buffer.from(change.before).toString("utf8"),
					after: Buffer.from(change.after).toString("utf8"),
				};
				if (boundary === "capture") retained = change;
				if (boundary === "refine") return [retained = { ...change }];
			},
		});

		expect(observed).toEqual({ content: "after\n", before: "before\n", after: "after\n" });
		expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("before\n");
		if (boundary === "checkpoint") retained = Reflect.get(branch.checkpoint!, "changes")?.[0];
		retained?.before?.fill(120); retained?.after?.fill(120);
		const child = await sandbox.fork({ cwd: root, action, parentCheckpoint: branch.checkpoint,
			execute: async (workspace) => settlement(await readFile(path.join(workspace.sandboxRoot, "value.txt"), "utf8")),
		});
		expect(child.output).toEqual(settlement("after\n"));
		await expect(branch.commit()).resolves.toEqual(settlement("done"));
		expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("after\n");
		await Promise.all([branch.dispose(), child.dispose()]);
	});

	it.each(["storage", "mount"])("owns partial OverlayFS %s preparation until every admitted allocation settles", async (phase) => {
		const root = await temporaryRoot(), gate = gated(), fault = new Error("private allocation failed"), owned: string[] = [];
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const capability = vi.spyOn(LinuxOverlayfsCapabilityRegistry.prototype, "capability").mockResolvedValue({
			available: true, binary: "unreachable-overlay-driver", fusermountBinary: "unreachable-unmount", fingerprint: "allocation-only", detail: "fixture" });
		let action: Promise<unknown> | undefined, directory: ReturnType<typeof mkdir> | undefined, settled = false;
		vi.mocked(mkdtemp).mockImplementation(async (prefix, options) => {
			if (phase === "storage" && String(prefix).endsWith("overlay-storage-")) { await action; throw fault; }
			const pending = fs.mkdtemp(prefix, options).then(value => {
				if (/\b(?:action|overlay-storage)-$/.test(String(prefix))) owned.push(String(value));
				return value;
			});
			if (String(prefix).endsWith(`${path.sep}action-`)) action = pending;
			return pending;
		});
		vi.mocked(mkdir).mockImplementation(async (target, options) => {
			if (path.basename(String(target)) === "upper") { await gate.entered; throw fault; }
			if (path.basename(String(target)) === "work") return directory = gate.wait().then(() => fs.mkdir(target, options));
			return fs.mkdir(target, options);
		});
		const execute = vi.fn(async () => settlement("unexpected execution"));
		const pending = sandbox.fork({ cwd: root, driver: "overlayfs", action: requiredAction("write", { path: "value", content: "next" }, root), execute })
			.then(value => value, error => error).finally(() => { settled = true; });
		try {
			if (phase === "mount") {
				await gate.entered; await nextTurn();
				expect({ settled, removing: vi.mocked(rm).mock.calls.some(([target]) => owned.includes(String(target))) }).toEqual({ settled: false, removing: false });
				gate.release();
			}
			expect(await pending).toBe(fault);
			expect(execute).not.toHaveBeenCalled();
			for (const target of owned) await expect(stat(target)).rejects.toThrow();
		} finally {
			gate.release(); await Promise.allSettled([pending, action, directory]); capability.mockRestore();
			vi.mocked(mkdir).mockImplementation(fs.mkdir); vi.mocked(mkdtemp).mockImplementation(fs.mkdtemp);
			await sandbox.closePools([root]);
		}
	});

	it("seals copy-ups, creations, and whiteouts from the typed OverlayFS frontier", async ({ skip }) => {
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const root = await temporaryRoot();
		await mkdir(path.join(root, "replaced"));
		await Promise.all([
			writeFile(path.join(root, "changed.txt"), "before\n", "utf8"),
			writeFile(path.join(root, "deleted.txt"), "deleted\n", "utf8"),
			writeFile(path.join(root, "replaced", "lower.txt"), "lower\n", "utf8"),
		]);
		const branch = await sandbox.fork({
			cwd: root,
			driver: "overlayfs",
			action: requiredAction("write", { path: "changed.txt", content: "after\n" }, root),
			execute: async (workspace) => {
				const capture = await workspace.transactions.begin();
				expect(await readdir(workspace.sandboxRoot)).not.toContain(
					".pi-speculative-runtime.tmp",
				);
				await Promise.all([
					writeFile(path.join(workspace.sandboxRoot, "changed.txt"), "after\n", "utf8"),
					writeFile(path.join(workspace.sandboxRoot, "created.txt"), "created\n", "utf8"),
					rm(path.join(workspace.sandboxRoot, "deleted.txt")),
				]);
				await rm(path.join(workspace.sandboxRoot, "replaced"), { recursive: true });
				await mkdir(path.join(workspace.sandboxRoot, "replaced"));
				await writeFile(path.join(workspace.sandboxRoot, "replaced", "created.txt"), "opaque\n", "utf8");
				const delta = await capture.finish();
				expect(delta.complete).toBe(true);
				expect((await readdir(workspace.sandboxRoot)).some((entry) => entry.startsWith(".pi-speculative-"))).toBe(
					false,
				);
				return settlement("overlay");
			},
		});
		expect(branch.resources).toEqual([
			"changed.txt",
			"created.txt",
			"deleted.txt",
			"replaced/created.txt",
			"replaced/lower.txt",
		]);
		await branch.commit();
		expect(await readFile(path.join(root, "changed.txt"), "utf8")).toBe("after\n");
		expect(await readFile(path.join(root, "created.txt"), "utf8")).toBe("created\n");
		expect(await readFile(path.join(root, "replaced", "created.txt"), "utf8")).toBe("opaque\n");
		await expect(stat(path.join(root, "deleted.txt"))).rejects.toThrow();
		await expect(stat(path.join(root, "replaced", "lower.txt"))).rejects.toThrow();
	});

	it("quarantines an unverified live mount without blocking pool shutdown", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const host = await linuxOverlayfsCapability();
		if (!host.available) return skip(host.detail);
		const root = await temporaryRoot();
		const marker = path.join(root, "probe-unmounted");
		const retainedTarget = path.join(root, "retained-mount");
		const wrapper = path.join(root, "fusermount-unresolved");
		let retainedMounts: string[] = [];
		try {
			await writeFile(
				wrapper,
				`#!/bin/sh\nif [ ! -e ${shellQuote(marker)} ]; then\n  : > ${shellQuote(marker)}\n  exec ${shellQuote(host.fusermountBinary)} "$@"\nfi\nfor target do :; done\nprintf '%s\\n' "$target" > ${shellQuote(retainedTarget)}\nexit 42\n`,
				"utf8",
			);
			await chmod(wrapper, 0o755);
			const options = { fusermountBinary: wrapper };
			expect((await linuxOverlayfsCapability(options)).available).toBe(true);
			await expect(
				sandbox.fork({
					cwd: root,
					driver: "overlayfs",
					...options,
					action: requiredAction("write", { path: "value.txt", content: "value\n" }, root),
					execute: async () => settlement("unsafe-unmount"),
				}),
			).rejects.toThrow(/cleanup|mounted|unmount/i);
			retainedMounts = [(await readFile(retainedTarget, "utf8")).trim()];
			expect(await fuseOverlayMountTargets()).toContain(retainedMounts[0]);
			await completesWithin(sandbox.closePools([root]), 500);
		} finally {
			for (const target of retainedMounts) {
				await runProgram(host.fusermountBinary, ["-u", "-z", target]);
				expect(await fuseOverlayMountTargets()).not.toContain(target);
				await rm(path.dirname(path.dirname(target)), { recursive: true, force: true });
			}
		}
	});

	it("validates consumed inputs and written outputs together without rewriting reads", async () => {
		const root = await temporaryRoot();
		const input = path.join(root, "input.txt"), target = path.join(root, "output.txt");
		const directory = path.join(root, "readable"); await mkdir(directory);
		await writeFile(path.join(directory, "keep"), "");
		const world = sandbox.createExecutionWorld();
		try {
			for (const changed of [undefined, input, target, "permission", "directory-permission"]) {
				await writeFile(input, "base\n"); await writeFile(target, "before\n");
				const before = await stat(input, { bigint: true });
				const branch = await world.speculation.execute(boundContext(root, async (view) => {
					await view.access(input, true);
					await view.access(directory);
					const bytes = await view.readFile(input);
					await view.writeFile!(target, bytes.toString());
					return settlement((await view.readFile(target)).toString());
				}));
				expect(branch.resources).toEqual(["output.txt"]);
				expect(await readFile(target, "utf8")).toBe("before\n");
				if (changed === "permission") await chmod(input, 0o444);
				else if (changed === "directory-permission") await chmod(directory, 0);
				else if (changed) await writeFile(changed, "actor\n");
				const permissionChange = changed === "permission" || changed === "directory-permission";
				const permissionDenied = permissionChange && !(await access(changed === "permission" ? input : directory,
					changed === "permission" ? fsConstants.R_OK | fsConstants.W_OK : fsConstants.R_OK).then(() => true, () => false));
				const commit = branch.commit();
				expect(branch.commit()).toBe(commit);
				if (changed && (!permissionChange || permissionDenied)) {
					if (permissionDenied) await expect(commit).rejects.toThrow();
					else await expect(commit).rejects.toThrow(`resource changed before commit: ${path.basename(changed)}`);
					expect(branch.commitMetrics).toBeUndefined();
					expect(await readFile(target, "utf8")).toBe(changed === target ? "actor\n" : "before\n");
				} else {
					await expect(commit).resolves.toEqual(settlement("base\n"));
					expect(branch.commitMetrics).toMatchObject({ resourcesCommitted: 1, resourcesValidated: 3 });
					const after = await stat(input, { bigint: true });
					expect([after.ino, after.mtimeNs]).toEqual([before.ino, before.mtimeNs]);
					if (!changed) expect(after.ctimeNs).toBe(before.ctimeNs);
					expect(await readFile(target, "utf8")).toBe("base\n");
				}
				await branch.dispose();
				await chmod(input, 0o666); await chmod(directory, 0o755);
			}
		} finally {
			await chmod(input, 0o666).catch(() => undefined);
			await chmod(directory, 0o755).catch(() => undefined);
		}
	});

	it("materializes a parent checkpoint privately and commits ordered deltas", async () => {
		const root = await temporaryRoot();
		const target = path.join(root, "lineage.txt");
		await writeFile(target, "base\n", "utf8");
		const world = sandbox.createExecutionWorld();
		const parentArgs = { path: "lineage.txt", content: "parent\n" };
		const parent = await world.speculation.execute(context(root, "write", writeTool, parentArgs));
		const lineage = parent.checkpoint!.lineage;
		for (const field of ["id", "lineage", "depth"]) Reflect.set(parent.checkpoint!, field, "changed");
		await expect(sandbox.fork({ cwd: root, action: requiredAction("write", parentArgs, root),
			parentCheckpoint: { ...parent.checkpoint! }, execute: async () => settlement("unused"),
		})).rejects.toThrow("another backend");
		await expect(sandbox.fork({ cwd: await temporaryRoot(), action: requiredAction("write", parentArgs, root),
			parentCheckpoint: parent.checkpoint, execute: async () => settlement("unused"),
		})).rejects.toThrow("another workspace");
		const childArgs = { path: "lineage.txt", edits: [{ oldText: "parent", newText: "child" }] };
		const child = await world.speculation.execute({
			...context(root, "edit", editTool, childArgs),
			parentCheckpoint: parent.checkpoint,
		});

		expect(child.checkpoint?.lineage).toBe(lineage);
		expect(child.checkpoint?.depth).toBe(1);
		expect(await readFile(target, "utf8")).toBe("base\n");
		await parent.commit();
		expect(await readFile(target, "utf8")).toBe("parent\n");
		await child.commit();
		expect(await readFile(target, "utf8")).toBe("child\n");
	});

	it("preserves native directory creation and rejects unproven self-observation", async () => {
		const root = await temporaryRoot();
		const directory = path.join(root, "generated", "nested");
		const mask = process.umask();
		try {
			process.umask(0o022);
			const world = sandbox.createExecutionWorld();
			const parent = await world.speculation.execute(boundContext(root, async (view) => {
				await view.mkdir!(directory); return settlement("directory");
			}));
			const child = await world.speculation.execute({
				...context(root, "write", writeTool, { path: "generated/nested/value.txt", content: "child\n" }),
				parentCheckpoint: parent.checkpoint,
			});

			expect(parent.resources).toEqual(["generated", "generated/nested"]);
			expect(child.checkpoint?.lineage).toBe(parent.checkpoint?.lineage);
			await expect(stat(directory)).rejects.toThrow();
			process.umask(0o077);
			const native = path.join(root, "native", "nested"); await mkdir(native, { recursive: true });
			await parent.commit();
			expect((await stat(directory)).isDirectory()).toBe(true);
			expect((await stat(directory)).mode).toBe((await stat(native)).mode);
			await child.commit();
			expect(await readFile(path.join(directory, "value.txt"), "utf8")).toBe("child\n");
			for (const operation of ["read", "file-access", "directory-access"]) {
				const created = path.join(root, operation);
				await expect(world.speculation.execute(boundContext(root, async (view) => {
					if (operation === "directory-access") await view.mkdir!(created);
					else await view.writeFile!(created, "private\n");
					await (operation === "read" ? view.readFile(created) : view.access(created, true)).catch(() => undefined);
					return settlement("unproven permissions");
				}))).rejects.toThrow("Created input permissions require authoritative execution");
				await expect(stat(created)).rejects.toThrow();
			}
		} finally {
			process.umask(mask);
		}
	});

	it("owns partial directory creation and permits fallback only after complete rollback", async () => {
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		for (const fault of ["topology", "mkdir", "foreign"]) {
			const root = await temporaryRoot(), template = await temporaryRoot();
			const directory = path.join(root, "generated"), foreign = path.join(directory, "foreign.txt");
			const file = fileTransition(root, fault === "topology" ? "generated/value.txt" : "generated/nested/value.txt", undefined, "child");
			const gateway = new ToolExecutionGateway<unknown, ToolSettlement>([]);
			let nativeCalls = 0;
			try {
				const expectedEmpty = await readSandboxDirectoryState(template);
				if (!expectedEmpty) throw new Error("template directory state missing");
				vi.mocked(mkdir).mockImplementation((async (target, options) => {
					if (target === path.join(directory, "nested")) {
						if (fault === "foreign") await writeFile(foreign, "external");
						throw new Error("injected mkdir failure");
					}
					return fs.mkdir(target, options);
				}) as typeof mkdir);
				const error = await gateway.executeAuthoritative({ tool: "operation", input: {} }, async () => {
					nativeCalls++;
					await expect(stat(directory)).rejects.toThrow();
					return settlement("actor");
				}, { reuse: () => sandbox.commitDelta({ output: settlement("reused"), changes: fault === "topology"
					? [{ kind: "directory", root, target: directory, resource: "generated", after: expectedEmpty }, file] : [file] }) })
					.then(() => undefined, (failure: unknown) => failure);
				expect(nativeCalls, fault).toBe(fault === "foreign" ? 0 : 1);
				expect(fault === "foreign" ? isPoisonedEffectCommit(error) : error === undefined, fault).toBe(true);
				await expect(stat(file.target)).rejects.toThrow();
				if (fault === "foreign") expect(await readFile(foreign, "utf8")).toBe("external");
				else await expect(stat(directory)).rejects.toThrow();
			} finally {
				vi.mocked(mkdir).mockReset(); await gateway.dispose();
			}
		}
	});

	it("deletes a typed directory tree in child-before-parent order", async () => {
		const root = await temporaryRoot();
		const outer = path.join(root, "generated");
		const inner = path.join(outer, "nested");
		const target = path.join(inner, "value.txt");
		await mkdir(inner, { recursive: true });
		await writeFile(target, "remove me\n");
		const [outerBefore, innerBefore, fileBefore] = await Promise.all([
			readSandboxDirectoryState(outer),
			readSandboxDirectoryState(inner),
			stat(target),
		]);
		if (!outerBefore || !innerBefore) throw new Error("directory baseline missing");
		await sandbox.commitDelta({
			output: settlement("deleted"),
			changes: [
				{ kind: "directory", root, target: outer, resource: "generated", before: outerBefore },
				{ ...fileTransition(root, "generated/nested/value.txt", "remove me\n", undefined), beforeMode: fileBefore.mode & 0o777 },
				{ kind: "directory", root, target: inner, resource: "generated/nested", before: innerBefore },
			],
		});
		await expect(stat(target)).rejects.toThrow();
		await expect(stat(inner)).rejects.toThrow();
		await expect(stat(outer)).rejects.toThrow();
	});

	it("preserves native file identity and never retries a possibly applied content write", async () => {
		for (const fault of ["none", "hardlink", "readonly", "write", "close"]) {
			const root = await temporaryRoot(), target = path.join(root, "value.txt");
			await writeFile(target, "before");
			const observer = await open(target, "r"), prototype = Object.getPrototypeOf(observer);
			const originalWrite = observer.writeFile;
			const gateway = new ToolExecutionGateway<unknown, ToolSettlement>([]);
			let nativeCalls = 0;
			try {
				if (fault === "hardlink") await link(target, path.join(root, "alias.txt"));
				if (fault === "readonly") await chmod(target, 0o444);
				if (fault === "write" || fault === "close") vi.spyOn(prototype, "writeFile").mockImplementationOnce(async function (this: FileHandle) {
					await originalWrite.call(this, fault === "write" ? "partial" : "after");
					if (fault === "write") throw new Error("injected partial write");
					const close = this.close.bind(this);
					this.close = async () => { await close(); throw new Error("injected close failure"); };
				});
				const poisoned = fault === "write" || fault === "close";
				const native = async () => { nativeCalls++; await writeFile(target, "after"); return settlement("done"); };
				const error = await gateway.executeAuthoritative({ tool: "write", input: {} }, native, {
					reuse: () => sandbox.commitDelta({ output: settlement("done"), changes: [
						{ ...fileTransition(root, "value.txt", "before", "after"), operation: "write_contents" },
					] }),
				}).then(() => undefined, (failure: unknown) => failure);
				expect(isPoisonedEffectCommit(error), fault).toBe(poisoned);
				if (fault !== "readonly") expect(nativeCalls, fault).toBe(fault === "hardlink" ? 1 : 0);
				else if (error) expect(nativeCalls).toBe(1); // Root may legitimately have permission despite mode 0444.
				const expected = fault === "write" ? "partial" : error && !poisoned ? "before" : "after";
				expect((await observer.readFile()).toString(), fault).toBe(expected);
				expect(await readFile(target, "utf8"), fault).toBe(expected);
				if (fault === "hardlink") expect(await readFile(path.join(root, "alias.txt"), "utf8")).toBe("after");
			} finally {
				vi.restoreAllMocks(); await observer.close(); await gateway.dispose();
				await chmod(target, 0o644);
			}
		}
	});

	it.each([false, true])("drains parallel private staging before committing, failing or closing (failure=%s)", async (failure) => {
		const root = await temporaryRoot(), target = path.join(root, "control");
		await writeFile(target, "control");
		const observer = await open(target, "r"), sync = observer.sync;
		const gate = gated(), handles: FileHandle[] = [];
		let returned = false, closed = false, active = 0, peak = 0;
		const syncing = vi.spyOn(Object.getPrototypeOf(observer), "sync").mockImplementation(async function (this: FileHandle) {
			const index = handles.push(this) - 1;
			peak = Math.max(peak, ++active);
			try {
				if (index === 0) { await gate.wait(); }
				else if (index === 1 && failure) throw new Error("injected staging sync failure");
				await sync.call(this);
			} finally { active--; }
		});
		const changes = Array.from({ length: 16 }, (_, index) => fileTransition(root, `file-${index}`, undefined, `value-${index}`));
		const pending = sandbox.commitDelta({ output: settlement("done"), changes }).then(
			output => { returned = true; return { output }; }, error => { returned = true; return { error }; });
		let retirement: Promise<void> | undefined;
		try {
			await gate.entered;
			await vi.waitFor(() => expect(handles.length).toBeGreaterThan(1));
			retirement = sandbox.dispose().then(() => { closed = true; });
			await nextTurn();
			expect({ returned, closed }).toEqual({ returned: false, closed: false });
			for (const change of changes) await expect(stat(change.target)).rejects.toMatchObject({ code: "ENOENT" });
		} finally {
			gate.release(); await Promise.allSettled([pending, retirement ?? sandbox.dispose()]);
			syncing.mockRestore(); await observer.close();
		}
		expect(peak).toBeGreaterThan(1); expect(peak).toBeLessThanOrEqual(12);
		expect(handles.every(handle => handle.fd === -1)).toBe(true);
		const result = await pending;
		if (failure) expect(result).toMatchObject({ error: { disposition: "recoverable", message: "injected staging sync failure" } });
		else {
			expect(result).toEqual({ output: settlement("done") });
			for (const change of changes) expect(await readFile(change.target)).toEqual(change.after);
		}
		expect((await readdir(root)).sort()).toEqual(["control", ...(failure ? [] : changes.map(change => change.resource))].sort());
	});

	it.each(["unchanged", "paths", "after", "before", "directory-before", "directory-after"])("owns queued %s data and validates every baseline before one commit wins", async (mutation) => {
		const root = await temporaryRoot();
		const target = path.join(root, "value.txt");
		await writeFile(target, "base\n", "utf8");
		const alternate = path.join(root, "alternate.txt"), directory = path.join(root, "directory");
		await writeFile(alternate, "base\n"); await mkdir(directory);
		const directoryState = await readSandboxDirectoryState(directory);
		const stale = path.join(root, "z-stale.txt");
		await writeFile(stale, "actor");
		await expect(sandbox.commitDelta({ output: settlement("unused"), changes: [
			fileTransition(root, "value.txt", "base\n", "invalid"), fileTransition(root, "z-stale.txt", "base", "invalid"),
		] })).rejects.toThrow("resource changed before commit: z-stale.txt");
		expect(await readFile(target, "utf8")).toBe("base\n");
		expect(await readFile(stale, "utf8")).toBe("actor");
		const deltas = ["first\n", "second\n"].map((after) => ({
			output: settlement(after.trim()),
			changes: [fileTransition(root, "value.txt", "base\n", after), {
				kind: "directory" as const, root, target: directory, resource: "directory",
				before: { ...directoryState! }, after: { ...directoryState! },
			}],
		}));
		const gate = gated();
		const blocker = withFileMutationQueue(target, gate.wait);
		await gate.entered;
		const pending = Promise.allSettled(deltas.map((delta) => sandbox.commitDelta(delta)));
		for (const delta of deltas) {
			const file = delta.changes[0] as SandboxFileChange, directoryChange = delta.changes[1]!;
			if (mutation === "paths") Object.assign(file, { target: alternate, resource: "alternate.txt" });
			if (mutation === "after" || mutation === "before") file[mutation]!.fill(120);
			if (mutation.startsWith("directory-")) Object.assign(directoryChange[mutation === "directory-before" ? "before" : "after"]!, { entriesDigest: "mutated" });
		}
		const retirement = sandbox.dispose();
		expect(sandbox.dispose()).toBe(retirement);
		await expect(sandbox.commitDelta(deltas[0]!)).rejects.toThrow("service is disposed");
		expect(
			await Promise.race([retirement.then(() => true), new Promise<false>((resolve) => setImmediate(() => resolve(false)))]),
		).toBe(false);
		gate.release();
		const results = await pending;
		await Promise.all([blocker, retirement]);
		expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
		expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
		expect(["first\n", "second\n"]).toContain(await readFile(target, "utf8"));
		expect(await readFile(alternate, "utf8")).toBe("base\n");
		expect(await readSandboxDirectoryState(directory)).toEqual(directoryState);
	});

	it.each(["native", "thinkthread"])("rejects path escape and source symlink traversal before invoking %s tools", async (route) => {
		const root = await temporaryRoot();
		const outside = await temporaryRoot();
		let executions = 0;
		const countingTool = {
			...writeTool,
			execute: async (...args: Parameters<typeof writeTool.execute>) => {
				executions++;
				return writeTool.execute(...args);
			},
		};
		const execute = (args: { path: string; content: string }) => route === "native"
			? sandbox.createExecutionWorld().speculation.execute(context(root, "write", countingTool, args))
			: runThinkThreadTool({ version: 2, tool: "write", callID: "guard", args, autoResizeImages: true, modelSupportsImages: true }, root);
		const escapingInput = { path: "../outside.txt", content: "no" };
		await expect(execute(escapingInput)).rejects.toThrow();
		await symlink(outside, path.join(root, "linked"), process.platform === "win32" ? "junction" : "dir");
		const linked = { path: "linked/out.txt", content: "no" };
		await expect(execute(linked)).rejects.toThrow(/symlink/);
		expect(executions).toBe(0);
		await expect(stat(path.join(outside, "out.txt"))).rejects.toThrow();
	});

	it.each(["index", "template", "global-config", "system-config"])("isolates inherited Git %s settings and excluded repository metadata", async (setting) => {
		const root = await temporaryRoot();
		await runProgram("git", ["init"], root);
		await runProgram("git", ["config", "user.email", "test@example.com"], root);
		await runProgram("git", ["config", "user.name", "Test"], root);
		await writeFile(path.join(root, "tracked.txt"), "base\n", "utf8");
		await runProgram("git", ["add", "tracked.txt"], root);
		await runProgram("git", ["commit", "-m", "base"], root);
		await writeFile(path.join(root, "staged.txt"), "user\n", "utf8");
		await runProgram("git", ["add", "staged.txt"], root);
		const beforeStatus = await runProgram("git", ["status", "--short"], root);
		const beforeBranch = await runProgram("git", ["branch", "--show-current"], root);
		const index = path.join(root, ".git/index"), beforeIndex = await readFile(index);
		const external = await temporaryRoot(), hook = path.join(external, "hooks/post-checkout");
		await mkdir(path.dirname(hook));
		await writeFile(hook, `#!/bin/sh\nprintf hooked > ${shellQuote(path.join(external, "hooked"))}\n`);
		await chmod(hook, 0o755);
		const config = path.join(external, "config");
		await runProgram("git", ["config", "--file", config, "core.hooksPath", path.dirname(hook)]);
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture");
		try {
			if (setting === "index") vi.stubEnv("GIT_INDEX_FILE", index);
			else if (setting === "template") vi.stubEnv("GIT_TEMPLATE_DIR", external);
			else vi.stubEnv(setting === "global-config" ? "GIT_CONFIG_GLOBAL" : "GIT_CONFIG_SYSTEM", config);
			await sandbox.prepare(root, { driver: "git" });
			expect(await readFile(index)).toEqual(beforeIndex);
			captures.mockClear();
			await writeFile(path.join(root, ".git", "audit-cache"), "metadata changed\n");
			await sandbox.prepare(root, { driver: "git" });
			expect(captures).not.toHaveBeenCalled();
			const args = { path: "created.txt", content: "speculative\n" };
			await sandbox.createExecutionWorld().speculation.execute(context(root, "write", writeTool, args));
			expect(await readFile(index)).toEqual(beforeIndex);
			await expect(stat(path.join(external, "hooked"))).rejects.toThrow();
		} finally { captures.mockRestore(); vi.unstubAllEnvs(); }

		expect(await runProgram("git", ["status", "--short"], root)).toBe(beforeStatus);
		expect(await runProgram("git", ["branch", "--show-current"], root)).toBe(beforeBranch);
		await expect(stat(path.join(root, "created.txt"))).rejects.toThrow();
	});

	it.each(["stat-cache", "attributes", "encoding", "delayed-events", "partial-events", "uncertain-events"])("preserves exact bytes across %s changes and parallel workspaces", async (setting) => {
		const root = await temporaryRoot();
		const encoding = setting === "encoding" ? "utf16le" : "utf8";
		const stable = Buffer.from("$Id$\r\n", encoding), timestamp = new Date("2020-01-01T00:00:00Z");
		await writeFile(path.join(root, "value1.txt"), stable);
		if (setting === "attributes" || setting === "encoding") await writeFile(path.join(root, ".gitattributes"),
			`*.txt text eol=lf ident${setting === "encoding" ? " working-tree-encoding=UTF-16LE" : ""}\n`);
		const signal = new AbortController().signal, captures = vi.spyOn(ResourceVersionManager.prototype, "capture");
		const events = vi.spyOn(ResourceVersionManager.prototype, "changesSince");
		try {
			events.mockReturnValue({ uncertain: setting !== "delayed-events" && setting !== "partial-events",
				paths: setting === "partial-events" ? [path.join(root, "value1.txt")] : [] });
			vi.stubEnv("GIT_CONFIG_COUNT", "1");
			vi.stubEnv("GIT_CONFIG_KEY_0", "core.trustctime");
			vi.stubEnv("GIT_CONFIG_VALUE_0", "false");
			for (const [iteration, text] of ["before\r\n", "after!\r\n", "after!\r\n"].entries()) {
				const baseline = Buffer.from(text, encoding);
				await writeFile(path.join(root, "value[1].txt"), baseline);
				await utimes(path.join(root, "value[1].txt"), timestamp, timestamp);
				await sandbox.prepare(root, { driver: "git", signal });
				const count = captures.mock.calls.length;
				const roots = await Promise.all(["first\n", "second\n"].map((content) =>
					sandbox.withWorkspace(root, async ({ sandboxRoot }) => {
						expect(await readFile(path.join(sandboxRoot, "value[1].txt"))).toEqual(baseline);
						expect(await readFile(path.join(sandboxRoot, "value1.txt"))).toEqual(stable);
						await writeFile(path.join(sandboxRoot, "value[1].txt"), content);
						expect(await readFile(path.join(sandboxRoot, "value[1].txt"), "utf8")).toBe(content);
						return sandboxRoot;
					})));
				expect(captures.mock.calls.length).toBe(count + Number(setting === "delayed-events" && iteration === 1));
				expect(new Set(roots).size).toBe(2);
				expect(await readFile(path.join(root, "value[1].txt"))).toEqual(baseline);
				for (const workspace of roots) await expect(stat(workspace)).rejects.toThrow();
			}
		} finally { captures.mockRestore(); events.mockRestore(); vi.unstubAllEnvs(); }
	});

	it("repairs staged bytes that changed between source capture and validation", async () => {
		const root = await temporaryRoot(), target = path.join(root, "value.txt"), content = "before\n";
		await writeFile(target, content);
		const capture = ResourceVersionManager.prototype.capture, validate = ResourceVersionManager.prototype.validate;
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture").mockImplementationOnce(async function (this: ResourceVersionManager, ...args) {
			const token = await capture.apply(this, args); await writeFile(target, "temporarily copied bytes\n"); return token;
		});
		const validations = vi.spyOn(ResourceVersionManager.prototype, "validate").mockImplementationOnce(async function (this: ResourceVersionManager, token) {
			await writeFile(target, content); return validate.call(this, token);
		});
		try {
			await sandbox.prepare(root, { driver: "git" });
			expect(await sandbox.withWorkspace(root, ({ sandboxRoot }) => readFile(path.join(sandboxRoot, "value.txt"), "utf8"))).toBe(content);
			expect(await readFile(target, "utf8")).toBe(content);
		} finally { captures.mockRestore(); validations.mockRestore(); }
	});

	it("retires a stale prepared workspace once across competing warm-ups", async () => {
		const root = await temporaryRoot(), gate = gated();
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const validations = vi.spyOn(ResourceVersionManager.prototype, "validate"), pending: Promise<void>[] = [];
		const changes = vi.spyOn(ResourceVersionManager.prototype, "changesSince").mockReturnValue({ uncertain: true, paths: [] });
		let heldRoot: string | undefined, removals = 0;
		vi.mocked(mkdtemp).mockImplementation(async (prefix, options) => {
			const directory = await fs.mkdtemp(prefix, options);
			if (!heldRoot && String(prefix).endsWith(`${path.sep}action-`)) {
				heldRoot = directory; await gate.wait();
			}
			return directory;
		});
		vi.mocked(rm).mockImplementation(async (target, options) => {
			if (String(target) === heldRoot) removals++;
			return fs.rm(target, options);
		});
		try {
			await writeFile(path.join(root, "value.txt"), "before\n");
			pending.push(sandbox.prepare(root, { driver: "git" }));
			await gate.entered;
			const repository = await Reflect.get(sandbox, "state").repositories.values().next().value;
			const previous = repository.baseline.commit;
			await writeFile(path.join(root, "value.txt"), "after\n");
			pending.push(sandbox.prepare(root, { driver: "git" }));
			await vi.waitFor(() => expect(repository.baseline.commit).not.toBe(previous));
			await repository.lock; await nextTurn();
			const count = validations.mock.calls.length;
			pending.push(sandbox.prepare(root, { driver: "git" }));
			await vi.waitFor(() => expect(validations.mock.calls.length).toBeGreaterThan(count));
			await repository.lock; await nextTurn();
			gate.release(); await Promise.all(pending);
			expect(removals).toBe(1);
			expect(await sandbox.withWorkspace(root, ({ sandboxRoot }) => readFile(path.join(sandboxRoot, "value.txt"), "utf8")))
				.toBe("after\n");
		} finally {
			gate.release(); await Promise.allSettled(pending); validations.mockRestore(); changes.mockRestore();
			vi.mocked(mkdtemp).mockImplementation(fs.mkdtemp); vi.mocked(rm).mockImplementation(fs.rm);
		}
	});

	it.each(["none", "workspace", "registration"])("owns workspace removal and %s cleanup failure before returning", async (failure) => {
		const root = await temporaryRoot(), gate = gated();
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		await writeFile(path.join(root, "value.txt"), "before\n");
		let owned: { processRoot: string; gitDirectory: string; dispose: () => Promise<void> } | undefined, settled = false;
		const removed: string[] = [], fault = new Error("private removal failed");
		vi.mocked(rm).mockImplementation(async (target, options) => {
			if (owned && [owned.processRoot, owned.gitDirectory].includes(String(target))) {
				removed.push(String(target));
				if (String(target) === (failure === "registration" ? owned.gitDirectory : owned.processRoot)) {
					await gate.wait();
					if (failure !== "none") throw fault;
				}
			}
			return fs.rm(target, options);
		});
		const execution = sandbox.withWorkspace(root, async (workspace) => {
			owned = { processRoot: workspace.processRoot, dispose: Reflect.get(workspace, "dispose"),
				gitDirectory: (await readFile(path.join(workspace.sandboxRoot, ".git"), "utf8")).trim().slice(8) };
			await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "private\n");
		}).then(() => { settled = true; }, error => { settled = true; throw error; });
		try {
			await gate.entered; await nextTurn();
			expect(settled).toBe(false);
			expect((await stat(owned!.gitDirectory)).isDirectory()).toBe(true);
			expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("before\n");
			gate.release();
			if (failure === "none") await execution;
			else await expect(execution).rejects.toMatchObject({ errors: [fault] });
			const disposal = owned!.dispose();
			expect(owned!.dispose()).toBe(disposal);
			await disposal.catch(() => undefined);
			expect(removed).toEqual(failure === "workspace" ? [owned!.processRoot] : [owned!.processRoot, owned!.gitDirectory]);
			vi.mocked(rm).mockImplementation(fs.rm);
			if (failure === "none") await sandbox.withWorkspace(root, async (workspace) => {
				const next = (await readFile(path.join(workspace.sandboxRoot, ".git"), "utf8")).trim().slice(8);
				expect(next).toBe(owned!.gitDirectory);
				await owned!.dispose();
				expect((await stat(next)).isDirectory()).toBe(true);
				expect(await readFile(path.join(workspace.sandboxRoot, "value.txt"), "utf8")).toBe("before\n");
			});
			await sandbox.closePools([root]);
			await expect(stat(owned!.processRoot)).rejects.toThrow();
			await expect(stat(owned!.gitDirectory)).rejects.toThrow();
		} finally {
			gate.release(); await execution.catch(() => undefined); vi.mocked(rm).mockImplementation(fs.rm);
		}
	});

	it("defers observation and retains its baseline across warm-up and aborted intervals", async () => {
		const root = await temporaryRoot();
		await writeFile(path.join(root, "changed.txt"), "before\n", "utf8");
		await writeFile(path.join(root, "deleted.txt"), "deleted\n", "utf8");
		await writeFile(path.join(root, "untouched.txt"), "stable\n", "utf8");
		await sandbox.withWorkspace(root, async (workspace) => {
			const clock = path.join(workspace.processRoot, "workspace-transaction.clock");
			await expect(stat(clock)).rejects.toThrow();
			await writeFile(path.join(root, "changed.txt"), "next baseline\n", "utf8");
			await sandbox.prepare(root, { driver: "git" });
			const initial = await workspace.transactions.begin();
			Reflect.set(workspace, "commit", "0".repeat(40));
			expect((await stat(clock)).isFile()).toBe(true);
			await initial.abort();
			const capture = await workspace.transactions.begin();
			await writeFile(path.join(workspace.sandboxRoot, "changed.txt"), "after!\n", "utf8");
			await writeFile(path.join(workspace.sandboxRoot, "created.txt"), "created\n", "utf8");
			await rm(path.join(workspace.sandboxRoot, "deleted.txt"));
			const delta = await capture.finish();

			if (!delta.complete) throw new Error(`workspace transaction was incomplete: ${delta.reason}`);
			const beforeEntry = delta.before.entries.get("changed.txt");
			const afterEntry = delta.after.entries.get("changed.txt");
			if (beforeEntry?.kind !== "file" || afterEntry?.kind !== "file") throw new Error("change clock missing");
			expect(afterEntry.changeTimeMs).toBeGreaterThan(beforeEntry.changeTimeMs);
			expect(
				delta.changes.map((change) => ({
					path: change.relativePath,
					before: change.before ? Buffer.from(change.before).toString("utf8") : undefined,
					after: change.after ? Buffer.from(change.after).toString("utf8") : undefined,
				})),
			).toEqual([
				{ path: "changed.txt", before: "before\n", after: "after!\n" },
				{ path: "created.txt", before: undefined, after: "created\n" },
				{ path: "deleted.txt", before: "deleted\n", after: undefined },
			]);
		});
	});

	it("fences clock advance, stalled timestamps and replaced identities independently of wall-clock jumps", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
		let wall = 0;
		const wallClock = vi.spyOn(Date, "now").mockImplementation(() => wall += 1_000);
		try {
			for (const state of ["advance", "stalled", "aliased", "replaced"]) {
				const identity = { dev: 1, ino: 2, nlink: 1 };
				let queries = 0;
				const clock = { truncate: async () => {}, write: async () => {}, stat: async () => ({
					...identity, isFile: () => true, ctimeMs: state === "advance" && ++queries > 3 ? 11 : 10,
					...(state === "aliased" ? { nlink: 2 } : state === "replaced" ? { ino: 3 } : {}),
				}) } as unknown as FileHandle;
				const pending = advanceFilesystemClock(clock, 10, identity);
				const assertion = state === "advance" ? expect(pending).resolves.toBeUndefined()
					: expect(pending).rejects.toThrow(state === "stalled" ? "did not advance" : "identity changed");
				await vi.runAllTimersAsync();
				await assertion;
			}
		} finally {
			wallClock.mockRestore();
			vi.useRealTimers();
		}
	});

	it("marks unsupported inode transitions incomplete without undoing the operation", async ({ skip }) => {
		if (process.platform === "win32") return skip("symlink creation requires Windows privileges");
		const root = await temporaryRoot();
		await writeFile(path.join(root, "target.txt"), "target\n", "utf8");
		await sandbox.withWorkspace(root, async (workspace) => {
			const capture = await workspace.transactions.begin();
			const linkPath = path.join(workspace.sandboxRoot, "link.txt");
			await symlink("target.txt", linkPath);
			const delta = await capture.finish();
			if (delta.complete) throw new Error("symlink transition was unexpectedly reusable");
			expect(delta.reason).toContain("unsupported_workspace_transition:link.txt");
			expect((await stat(linkPath)).isFile()).toBe(true);
		});
	});

	it("fails closed for overlapping workspace mutation intervals and recovers afterward", async () => {
		const root = await temporaryRoot();
		await writeFile(path.join(root, "value.txt"), "base\n", "utf8");
		await sandbox.withWorkspace(root, async (workspace) => {
			const first = await workspace.transactions.begin();
			const second = await workspace.transactions.begin();
			await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "overlap\n", "utf8");
			for (const delta of await Promise.all([first.finish(), second.finish()])) {
				expect(delta).toMatchObject({ complete: false, changes: [], reason: "overlapping_workspace_transaction" });
			}

			const recovered = await workspace.transactions.begin();
			await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "recovered\n", "utf8");
			const recoveredDelta = await recovered.finish();
			if (!recoveredDelta.complete) {
				throw new Error(`recovered workspace transaction was incomplete: ${recoveredDelta.reason}`);
			}
			expect(Buffer.from(recoveredDelta.changes[0]?.before ?? []).toString("utf8")).toBe("overlap\n");
			expect(Buffer.from(recoveredDelta.changes[0]?.after ?? []).toString("utf8")).toBe("recovered\n");
		});
	});

	it("drains admitted file requests and refuses cancelled or swallowed failures", async () => {
		const root = await temporaryRoot();
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const target = path.join(root, "held.txt"), world = sandbox.createExecutionWorld();
		try {
			const controller = new AbortController(); controller.abort(new Error("cancelled"));
			await expect(sandbox.prepare(root, { signal: controller.signal })).rejects.toThrow("cancelled");
			for (const cancelled of [false, true]) {
				await fs.writeFile(target, "before");
				const gate = gated();
				const abort = new AbortController();
				let outlet: Parameters<NonNullable<ToolInvocation["filesystem"]>>[0];
				vi.mocked(writeFile).mockImplementation(async (file, data, options) => {
					if (String(file).endsWith("held.txt") && String(file) !== target) await gate.wait();
					return fs.writeFile(file, data, options);
				});
				let settled = false;
				const pending = world.speculation.execute({ ...boundContext(root, async (view) => {
					outlet = view; void view.writeFile!(target, "after"); return settlement("done");
				}), signal: abort.signal }).finally(() => { settled = true; });
				await Promise.race([gate.entered, pending.then(() => { throw new Error("File request did not reach the held write"); })]);
				try { expect(settled).toBe(false); if (cancelled) abort.abort(new Error("cancelled")); }
				finally { gate.release(); }
				if (cancelled) await expect(pending).rejects.toThrow("cancelled");
				else { const branch = await pending; await branch.commit(); await branch.dispose(); }
				await expect(outlet!.writeFile!(target, "late")).rejects.toThrow("execution lifetime is closed");
				expect(await readFile(target, "utf8")).toBe(cancelled ? "before" : "after");
				vi.mocked(writeFile).mockImplementation(fs.writeFile);
			}
			await expect(world.speculation.execute(boundContext(root, async (view) => {
				await view.readFile(path.join(root, "absent")).catch(() => undefined); return settlement("ignored failure");
			}))).rejects.toThrow("Workspace input does not exist");
		} finally {
			vi.mocked(writeFile).mockImplementation(fs.writeFile);
		}
	});
});

const boundSemantics = new ActionSemanticsRegistry([{ tool: "transform", epoch: "test-operation", effect: "workspace_mutation",
	requirements: WORKSPACE_PATH_MUTATION_EFFECTS, canonicalize: () => ({ input: {}, resources: [] }) }]);
function boundContext(root: string, filesystem: NonNullable<ToolInvocation["filesystem"]>) {
	const action = boundSemantics.buildKey("transform", {}, root, "test-schema", {
		fingerprint: "test-operation", context: { executor: "test-operation", filesystem },
	});
	if (!action) throw new Error("bound action is missing");
	return { cwd: root, tool: writeTool, toolName: "transform", args: {}, action, callID: "bound", signal: new AbortController().signal };
}

function context<Schema extends (typeof writeTool)["parameters"] | (typeof editTool)["parameters"]>(
	root: string,
	toolName: "write" | "edit",
	tool: AgentTool<Schema>,
	args: unknown,
) {
	return {
		cwd: root,
		tool,
		toolName,
		args,
		action: {
			...(buildPiActionKey(toolName, args, root) ?? requiredAction("write", { path: "safe.txt", content: "boundary probe" }, root)),
			executionContext: resolvePiToolInvocation(toolName, args, { cwd: root, environment: {} }),
		},
		callID: `spec-${toolName}`,
		signal: new AbortController().signal,
	};
}

function requiredAction(tool: string, args: unknown, cwd: string) {
	const action = buildPiActionKey(tool, args, cwd);
	if (!action) throw new Error(`Expected action key for ${tool}`);
	return action;
}

function settlement(text: string): ToolSettlement {
	return { result: { content: [{ type: "text", text }], details: undefined }, isError: false };
}

function fileTransition(root: string, resource: string, before: string | undefined, after: string | undefined): SandboxFileChange {
	return { root, resource, target: path.resolve(root, resource),
		before: before === undefined ? undefined : Buffer.from(before), after: after === undefined ? undefined : Buffer.from(after) };
}

async function fuseOverlayMountTargets(): Promise<string[]> {
	if (process.platform !== "linux") return [];
	const mountInfo = await readFile("/proc/self/mountinfo", "utf8");
	const targets: string[] = [];
	for (const line of mountInfo.split("\n")) {
		const separator = line.indexOf(" - ");
		if (separator === -1) continue;
		const left = line.slice(0, separator).split(" ");
		const filesystem = line.slice(separator + 3).split(" ")[0];
		if (filesystem !== "fuse.fuse-overlayfs" && filesystem !== "fuse-overlayfs") continue;
		targets.push((left[4] ?? "").replace(/\\([0-7]{3})/g, (_match, value) => String.fromCharCode(Number.parseInt(value, 8))));
	}
	return targets;
}

async function completesWithin(operation: Promise<void>, timeoutMs: number): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			operation,
			new Promise<never>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error("workspace pool shutdown timed out")), timeoutMs);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}
