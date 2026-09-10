import { execFile } from "node:child_process";
import { constants as fsConstants } from "node:fs";
import { access, chmod, type FileHandle, link, mkdir, mkdtemp, open, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
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
import { linuxOverlayfsCapability } from "../src/linux-overlayfs.ts";
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
beforeEach(() => { sandbox = new WorkspaceSandboxService(); });
vi.mock("node:fs/promises", async (original) => {
	const fs = await original<typeof import("node:fs/promises")>();
	return { ...fs, mkdir: vi.fn(fs.mkdir), mkdtemp: vi.fn(fs.mkdtemp), writeFile: vi.fn(fs.writeFile) };
});

afterEach(async () => {
	await sandbox.dispose();
});

describe("workspace-branch ExecutionWorld", () => {
	it("stops cancelled preparation between stages without cancelling a concurrent owner", async () => {
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		for (const phase of ["repository", "baseline"]) for (const owner of ["none", "active", "cancelled"]) {
			const root = await temporaryRoot("cancel-prepare"), controller = new AbortController();
			let entered!: () => void, release!: () => void, workspaces = 0, captures = 0;
			const started = new Promise<void>((resolve) => { entered = resolve; }), gate = new Promise<void>((resolve) => { release = resolve; });
			const capture = ResourceVersionManager.prototype.capture;
			const observer = vi.spyOn(ResourceVersionManager.prototype, "capture").mockImplementation(async function (this: ResourceVersionManager, ...args) {
				captures++;
				const token = await capture.apply(this, args);
				if (phase === "baseline") { entered(); await gate; }
				return token;
			});
			vi.mocked(mkdtemp).mockImplementation(async (prefix, options) => {
				const directory = await fs.mkdtemp(prefix, options);
				if (String(prefix).endsWith(`${path.sep}action-`)) workspaces++;
				if (phase === "repository" && String(prefix).includes("pi-speculative-action-pool-")) { entered(); await gate; }
				return directory;
			});
			const pending = sandbox.prepare(root, { driver: "git", signal: controller.signal });
			try {
				await Promise.race([started, pending.then(() => { throw new Error("Preparation did not reach the held stage"); })]);
				const other = owner === "none" ? Promise.resolve() : sandbox.prepare(root,
					{ driver: "git", ...(owner === "cancelled" ? { signal: controller.signal } : {}) });
				controller.abort(new Error("owner closed")); release();
				const [cancelled, live] = await Promise.allSettled([pending, other]);
				expect(cancelled).toMatchObject({ status: "rejected", reason: { message: "owner closed" } });
				expect(live.status).toBe(owner === "cancelled" ? "rejected" : "fulfilled");
				expect(workspaces).toBe(Number(owner === "active"));
				expect(captures).toBe(Number(phase === "baseline") + Number(owner === "active"));
				const branch = await sandbox.createExecutionWorld({ driver: "git" }).speculation.execute(
					context(root, "write", writeTool, { path: "value.txt", content: "live owner\n" }));
				await branch.commit(); await branch.dispose();
				expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("live owner\n");
			} finally {
				release(); await pending.catch(() => undefined); observer.mockRestore(); vi.mocked(mkdtemp).mockImplementation(fs.mkdtemp);
				await sandbox.closePools([root]); await rm(root, { recursive: true, force: true });
			}
		}
	});

	it("keeps pool and lock ownership inside an explicit service lifecycle", async () => {
		const root = await temporaryRoot("service-lifecycle");
		const first = new WorkspaceSandboxService();
		const second = new WorkspaceSandboxService();
		const firstWorld = first.createExecutionWorld({ driver: "git" });
		const firstSibling = first.createExecutionWorld({ driver: "git" });
		const secondWorld = second.createExecutionWorld({ driver: "git" });
		try {
			await writeFile(path.join(root, "value.txt"), "before\n", "utf8");
			await Promise.all([
				firstWorld.speculation.prepare?.({ cwd: root }),
				firstSibling.speculation.prepare?.({ cwd: root }),
				secondWorld.speculation.prepare?.({ cwd: root }),
			]);
			const abandoned = await firstWorld.speculation.execute(
				context(root, "write", writeTool, { path: "value.txt", content: "abandoned\n" }),
			);

			await Promise.all([firstWorld.dispose?.(), firstSibling.dispose?.()]);
			await first.dispose();
			await expect(first.prepare(root, { driver: "git" })).rejects.toThrow("service is disposed");
			await expect(abandoned.commit()).rejects.toThrow("service is disposed");

			const args = { path: "value.txt", content: "after\n" };
			const branch = await secondWorld.speculation.execute(context(root, "write", writeTool, args));
			await branch.commit();
			expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("after\n");
		} finally {
			await Promise.allSettled([first.dispose(), secondWorld.dispose?.(), second.dispose()]);
			await rm(root, { recursive: true, force: true });
		}
	});

	it("qualifies auto OverlayFS by the exact immutable baseline size", async ({ skip }) => {
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const root = await temporaryRoot("overlay-auto-cost");
		try {
			await writeFile(path.join(root, "small.txt"), "small\n", "utf8");
			expect(await sandbox.fingerprint({ driver: "auto" }, root)).toBe("git-worktree:v1");
			await Promise.all(
				Array.from({ length: 100 }, (_value, index) =>
					writeFile(path.join(root, `${index.toString().padStart(4, "0")}.txt`), `${index}\n`, "utf8"),
				),
			);
			expect(await sandbox.fingerprint({ driver: "auto" }, root)).toBe("git-worktree:v1");
			await Promise.all(
				Array.from({ length: 160 }, (_value, index) => {
					const ordinal = index + 100;
					return writeFile(path.join(root, `${ordinal.toString().padStart(4, "0")}.txt`), `${ordinal}\n`, "utf8");
				}),
			);
			expect(await sandbox.fingerprint({ driver: "auto" }, root)).toMatch(/^linux-overlayfs:v1:/);
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("binds stock file operations without invoking host functions or rewriting outputs", async () => {
		const root = await temporaryRoot("write");
		try {
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
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("makes the exact transaction delta available while the private workspace is still sealed", async () => {
		const root = await temporaryRoot("after-capture");
		try {
			await writeFile(path.join(root, "value.txt"), "before\n", "utf8");
			const action = buildPiActionKey("write", { path: "value.txt", content: "after\n" }, root);
			if (!action) throw new Error("action key missing");
			let observed: { readonly content: string; readonly before: string; readonly after: string } | undefined;
			const branch = await sandbox.fork({
				cwd: root,
				action,
				execute: async (workspace) => {
					await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "after\n", "utf8");
					return settlement("done");
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
				},
			});

			expect(observed).toEqual({ content: "after\n", before: "before\n", after: "after\n" });
			expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("before\n");
			branch.dispose();
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("seals copy-ups, creations, and whiteouts from the typed OverlayFS frontier", async ({ skip }) => {
		const overlay = await linuxOverlayfsCapability();
		if (!overlay.available) return skip(overlay.detail);
		const root = await temporaryRoot("overlay-frontier");
		try {
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
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("quarantines an unverified live mount without blocking pool shutdown", async ({ skip }) => {
		if (process.platform !== "linux") return skip("Linux only");
		const host = await linuxOverlayfsCapability();
		if (!host.available) return skip(host.detail);
		const root = await temporaryRoot("overlay-quarantine");
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
			await rm(root, { recursive: true, force: true });
		}
	});

	it("validates consumed inputs and written outputs together without rewriting reads", async () => {
		const root = await temporaryRoot("conflict");
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
			await rm(root, { recursive: true, force: true });
		}
	});

	it("materializes a parent checkpoint privately and commits ordered deltas", async () => {
		const root = await temporaryRoot("lineage");
		const target = path.join(root, "lineage.txt");
		try {
			await writeFile(target, "base\n", "utf8");
			const world = sandbox.createExecutionWorld();
			const parentArgs = { path: "lineage.txt", content: "parent\n" };
			const parent = await world.speculation.execute(context(root, "write", writeTool, parentArgs));
			const childArgs = { path: "lineage.txt", edits: [{ oldText: "parent", newText: "child" }] };
			const child = await world.speculation.execute({
				...context(root, "edit", editTool, childArgs),
				parentCheckpoint: parent.checkpoint,
			});

			expect(child.checkpoint?.lineage).toBe(parent.checkpoint?.lineage);
			expect(child.checkpoint?.depth).toBe(1);
			expect(await readFile(target, "utf8")).toBe("base\n");
			await parent.commit();
			expect(await readFile(target, "utf8")).toBe("parent\n");
			await child.commit();
			expect(await readFile(target, "utf8")).toBe("child\n");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("preserves native directory creation and rejects unproven self-observation", async () => {
		const root = await temporaryRoot("directory-lineage");
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
			await rm(root, { recursive: true, force: true });
		}
	});

	it("owns partial directory creation and permits fallback only after complete rollback", async () => {
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		for (const fault of ["topology", "mkdir", "foreign"]) {
			const root = await temporaryRoot(`directory-${fault}`), template = await temporaryRoot("directory-template");
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
				await Promise.all([rm(root, { recursive: true, force: true }), rm(template, { recursive: true, force: true })]);
			}
		}
	});

	it("deletes a typed directory tree in child-before-parent order", async () => {
		const root = await temporaryRoot("directory-delete");
		const outer = path.join(root, "generated");
		const inner = path.join(outer, "nested");
		const target = path.join(inner, "value.txt");
		try {
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
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("preserves native file identity and never retries a possibly applied content write", async () => {
		for (const fault of ["none", "hardlink", "readonly", "write", "close"]) {
			const root = await temporaryRoot(`content-${fault}`), target = path.join(root, "value.txt");
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
				await chmod(target, 0o644); await rm(root, { recursive: true, force: true });
			}
		}
	});

	it("validates every path under the mutation lock before allowing one competing baseline to win", async () => {
		const root = await temporaryRoot("lock");
		const target = path.join(root, "value.txt");
		try {
			await writeFile(target, "base\n", "utf8");
			const stale = path.join(root, "z-stale.txt");
			await writeFile(stale, "actor");
			await expect(sandbox.commitDelta({ output: settlement("unused"), changes: [
				fileTransition(root, "value.txt", "base\n", "invalid"), fileTransition(root, "z-stale.txt", "base", "invalid"),
			] })).rejects.toThrow("resource changed before commit: z-stale.txt");
			expect(await readFile(target, "utf8")).toBe("base\n");
			expect(await readFile(stale, "utf8")).toBe("actor");
			const deltas = ["first\n", "second\n"].map((after) => ({
				output: settlement(after.trim()),
				changes: [fileTransition(root, "value.txt", "base\n", after)],
			}));

			let enterBlock!: () => void;
			let releaseBlock!: () => void;
			const entered = new Promise<void>((resolve) => (enterBlock = resolve));
			const blocker = withFileMutationQueue(target, () => {
				enterBlock();
				return new Promise<void>((resolve) => (releaseBlock = resolve));
			});
			await entered;
			const pending = Promise.allSettled(deltas.map((delta) => sandbox.commitDelta(delta)));
			const retirement = sandbox.dispose();
			expect(sandbox.dispose()).toBe(retirement);
			await expect(sandbox.commitDelta(deltas[0]!)).rejects.toThrow("service is disposed");
			expect(
				await Promise.race([retirement.then(() => true), new Promise<false>((resolve) => setImmediate(() => resolve(false)))]),
			).toBe(false);
			releaseBlock();
			const results = await pending;
			await Promise.all([blocker, retirement]);
			expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
			expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
			expect(["first\n", "second\n"]).toContain(await readFile(target, "utf8"));
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it.each(["native", "thinkthread"])("rejects path escape and source symlink traversal before invoking %s tools", async (route) => {
		const root = await temporaryRoot("paths");
		const outside = await temporaryRoot("outside");
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
		try {
			const escapingInput = { path: "../outside.txt", content: "no" };
			await expect(execute(escapingInput)).rejects.toThrow();
			await symlink(outside, path.join(root, "linked"), process.platform === "win32" ? "junction" : "dir");
			const linked = { path: "linked/out.txt", content: "no" };
			await expect(execute(linked)).rejects.toThrow(/symlink/);
			expect(executions).toBe(0);
			await expect(stat(path.join(outside, "out.txt"))).rejects.toThrow();
		} finally {
			await Promise.all([rm(root, { recursive: true, force: true }), rm(outside, { recursive: true, force: true })]);
		}
	});

	it("does not modify an existing repository's index or branch", async () => {
		const root = await temporaryRoot("git");
		try {
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
			const args = { path: "created.txt", content: "speculative\n" };
			await sandbox.createExecutionWorld().speculation.execute(context(root, "write", writeTool, args));

			expect(await runProgram("git", ["status", "--short"], root)).toBe(beforeStatus);
			expect(await runProgram("git", ["branch", "--show-current"], root)).toBe(beforeBranch);
			await expect(stat(path.join(root, "created.txt"))).rejects.toThrow();
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("keeps parallel private workspaces isolated and cleans them after use", async () => {
		const root = await temporaryRoot("parallel");
		try {
			await writeFile(path.join(root, "value.txt"), "base\n", "utf8");
			const values = await Promise.all(
				["first\n", "second\n"].map((content) =>
					sandbox.withWorkspace(root, async (workspace) => {
						await writeFile(path.join(workspace.sandboxRoot, "value.txt"), content, "utf8");
						return {
							root: workspace.sandboxRoot,
							content: await readFile(path.join(workspace.sandboxRoot, "value.txt"), "utf8"),
						};
					}),
				),
			);
			expect(new Set(values.map((value) => value.root)).size).toBe(2);
			expect(values.map((value) => value.content).sort()).toEqual(["first\n", "second\n"]);
			expect(await readFile(path.join(root, "value.txt"), "utf8")).toBe("base\n");
			for (const value of values) await expect(stat(value.root)).rejects.toThrow();
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("defers observation until a transaction and captures exact deltas after an aborted interval", async () => {
		const root = await temporaryRoot("transaction");
		try {
			await writeFile(path.join(root, "changed.txt"), "before\n", "utf8");
			await writeFile(path.join(root, "deleted.txt"), "deleted\n", "utf8");
			await writeFile(path.join(root, "untouched.txt"), "stable\n", "utf8");
			await sandbox.withWorkspace(root, async (workspace) => {
				const clock = path.join(workspace.processRoot, "workspace-transaction.clock");
				await expect(stat(clock)).rejects.toThrow();
				const initial = await workspace.transactions.begin();
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
		} finally {
			await rm(root, { recursive: true, force: true });
		}
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
		const root = await temporaryRoot("transaction-inode");
		try {
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
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("fails closed for overlapping workspace mutation intervals and recovers afterward", async () => {
		const root = await temporaryRoot("transaction-overlap");
		try {
			await writeFile(path.join(root, "value.txt"), "base\n", "utf8");
			await sandbox.withWorkspace(root, async (workspace) => {
				const first = await workspace.transactions.begin();
				const second = await workspace.transactions.begin();
				await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "overlap\n", "utf8");
				const [firstDelta, secondDelta] = await Promise.all([first.finish(), second.finish()]);
				expect(firstDelta).toMatchObject({
					complete: false,
					changes: [],
					reason: "overlapping_workspace_transaction",
				});
				expect(secondDelta).toMatchObject({
					complete: false,
					changes: [],
					reason: "overlapping_workspace_transaction",
				});

				const recovered = await workspace.transactions.begin();
				await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "recovered\n", "utf8");
				const recoveredDelta = await recovered.finish();
				if (!recoveredDelta.complete) {
					throw new Error(`recovered workspace transaction was incomplete: ${recoveredDelta.reason}`);
				}
				expect(Buffer.from(recoveredDelta.changes[0]?.before ?? []).toString("utf8")).toBe("overlap\n");
				expect(Buffer.from(recoveredDelta.changes[0]?.after ?? []).toString("utf8")).toBe("recovered\n");
			});
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("drains admitted file requests and refuses cancelled or swallowed failures", async () => {
		const root = await temporaryRoot("cancelled");
		const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
		const target = path.join(root, "held.txt"), world = sandbox.createExecutionWorld();
		try {
			const controller = new AbortController(); controller.abort(new Error("cancelled"));
			await expect(sandbox.prepare(root, { signal: controller.signal })).rejects.toThrow("cancelled");
			for (const cancelled of [false, true]) {
				await fs.writeFile(target, "before");
				let enter!: () => void, unblock!: () => void;
				const entered = new Promise<void>((resolve) => { enter = resolve; }), release = new Promise<void>((resolve) => { unblock = resolve; });
				const abort = new AbortController();
				let outlet: Parameters<NonNullable<ToolInvocation["filesystem"]>>[0];
				vi.mocked(writeFile).mockImplementation(async (file, data, options) => {
					if (String(file).endsWith("held.txt") && String(file) !== target) { enter(); await release; }
					return fs.writeFile(file, data, options);
				});
				let settled = false;
				const pending = world.speculation.execute({ ...boundContext(root, async (view) => {
					outlet = view; void view.writeFile!(target, "after"); return settlement("done");
				}), signal: abort.signal }).finally(() => { settled = true; });
				await Promise.race([entered, pending.then(() => { throw new Error("File request did not reach the held write"); })]);
				try { expect(settled).toBe(false); if (cancelled) abort.abort(new Error("cancelled")); }
				finally { unblock(); }
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
			await rm(root, { recursive: true, force: true });
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

async function temporaryRoot(label: string): Promise<string> {
	return mkdtemp(path.join(os.tmpdir(), `pi-spec-${label}-`));
}

function runProgram(executable: string, args: readonly string[], cwd?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", cwd }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable} failed: ${stderr || error.message}`, { cause: error }));
			else resolve(stdout);
		});
	});
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

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'\\''`)}'`;
}
