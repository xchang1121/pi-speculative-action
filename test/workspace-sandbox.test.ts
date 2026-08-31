import { execFile } from "node:child_process";
import { link, mkdir, mkdtemp, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { buildPiActionKey } from "../src/action-semantics.ts";
import {
	effectCapabilitiesCover,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ToolSettlement } from "../src/tool-settlement.ts";
import {
	closeWorkspaceSandboxPools,
	commitSandboxDelta,
	createWorkspaceSandbox,
	forkSandboxWorkspace,
	prepareSandboxWorkspace,
	withSandboxWorkspace,
} from "../src/workspace-sandbox.ts";

const writeParameters = Type.Object({ path: Type.String(), content: Type.String() });
const editParameters = Type.Object({
	path: Type.String(),
	edits: Type.Array(Type.Object({ oldText: Type.String(), newText: Type.String() })),
});

const writeTool: AgentTool<typeof writeParameters> = {
	name: "write",
	label: "write",
	description: "write",
	parameters: writeParameters,
	async execute(_callID, args) {
		await mkdir(path.dirname(args.path), { recursive: true });
		await writeFile(args.path, args.content, "utf8");
		return { content: [{ type: "text", text: `Successfully wrote ${args.path}` }], details: undefined };
	},
};

const editTool: AgentTool<typeof editParameters> = {
	name: "edit",
	label: "edit",
	description: "edit",
	parameters: editParameters,
	async execute(_callID, args) {
		let content = await readFile(args.path, "utf8");
		for (const edit of args.edits) {
			if (!content.includes(edit.oldText)) throw new Error("oldText missing");
			content = content.replace(edit.oldText, edit.newText);
		}
		await writeFile(args.path, content, "utf8");
		return { content: [{ type: "text", text: `Successfully edited ${args.path}` }], details: undefined };
	},
};

afterEach(async () => {
	await closeWorkspaceSandboxPools();
});

describe("workspace-branch ExecutionWorld", () => {
	it("accepts workspace mutations and commits a sealed write exactly once", async () => {
		const root = await temporaryRoot("write");
		try {
			const args = { path: "nested/created.txt", content: "isolated\n" };
			const world = createWorkspaceSandbox();
			expect(world.scope).toBe("fallback");
			if (world.scope !== "fallback") throw new Error("Expected a fallback world");
			expect(effectCapabilitiesCover(world.capabilities, WORKSPACE_PATH_MUTATION_EFFECTS)).toBe(true);
			expect(effectCapabilitiesCover(world.capabilities, UNRESTRICTED_PROCESS_EFFECTS)).toBe(false);
			expect(
				await world.fingerprint?.({
					effect: "workspace_mutation",
					requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
				}),
			).toBe("git-worktree:v1");
			const branch = await world.fork(context(root, "write", writeTool, args));

			expect(branch.state).toBe("sealed");
			expect(branch.resources).toEqual(["nested/created.txt"]);
			await expect(stat(path.join(root, args.path))).rejects.toThrow();
			const first = branch.commit();
			expect(branch.commit()).toBe(first);
			await first;
			expect(branch.state).toBe("committed");
			expect(await readFile(path.join(root, args.path), "utf8")).toBe("isolated\n");
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
			const branch = await forkSandboxWorkspace({
				cwd: root,
				action,
				execute: async (workspace) => {
					await writeFile(path.join(workspace.sandboxRoot, "value.txt"), "after\n", "utf8");
					return settlement("done");
				},
				afterCapture: async (workspace, capture) => {
					const change = capture.changes[0];
					if (!change?.before || !change.after) throw new Error("captured change missing");
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

	it("rejects adoption after the Actor changes the same file", async () => {
		const root = await temporaryRoot("conflict");
		const target = path.join(root, "value.txt");
		try {
			await writeFile(target, "base\n", "utf8");
			const args = { path: "value.txt", edits: [{ oldText: "base", newText: "speculative" }] };
			const branch = await createWorkspaceSandbox().fork(context(root, "edit", editTool, args));
			await writeFile(target, "actor\n", "utf8");

			await expect(branch.commit()).rejects.toThrow("resource changed before commit: value.txt");
			expect(branch.state).toBe("failed");
			expect(await readFile(target, "utf8")).toBe("actor\n");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("materializes a parent checkpoint privately and commits ordered deltas", async () => {
		const root = await temporaryRoot("lineage");
		const target = path.join(root, "lineage.txt");
		try {
			await writeFile(target, "base\n", "utf8");
			const world = createWorkspaceSandbox();
			const parentArgs = { path: "lineage.txt", content: "parent\n" };
			const parent = await world.fork(context(root, "write", writeTool, parentArgs));
			const childArgs = { path: "lineage.txt", edits: [{ oldText: "parent", newText: "child" }] };
			const child = await world.fork({
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

	it("validates every path before an atomic multi-file commit", async () => {
		const root = await temporaryRoot("atomic");
		const first = path.join(root, "a.txt");
		const stale = path.join(root, "b.txt");
		try {
			await writeFile(first, "a0", "utf8");
			await writeFile(stale, "actor", "utf8");
			await expect(
				commitSandboxDelta({
					output: settlement("unused"),
					changes: [
						{ root, target: first, resource: "a.txt", before: Buffer.from("a0"), after: Buffer.from("a1") },
						{
							root,
							target: stale,
							resource: "b.txt",
							before: Buffer.from("b0"),
							after: Buffer.from("b1"),
						},
					],
				}),
			).rejects.toThrow("resource changed before commit: b.txt");
			expect(await readFile(first, "utf8")).toBe("a0");
			expect(await readFile(stale, "utf8")).toBe("actor");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("serializes competing commits so only one shared baseline wins", async () => {
		const root = await temporaryRoot("lock");
		const target = path.join(root, "value.txt");
		try {
			await writeFile(target, "base\n", "utf8");
			const deltas = ["first\n", "second\n"].map((after) => ({
				output: settlement(after.trim()),
				changes: [
					{ root, target, resource: "value.txt", before: Buffer.from("base\n"), after: Buffer.from(after) },
				],
			}));

			const results = await Promise.allSettled(deltas.map(commitSandboxDelta));
			expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
			expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
			expect(["first\n", "second\n"]).toContain(await readFile(target, "utf8"));
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("rejects path escape and source symlink traversal before invoking the tool", async () => {
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
		try {
			const escapingInput = { path: "../outside.txt", content: "no" };
			await expect(
				createWorkspaceSandbox().fork(context(root, "write", countingTool, escapingInput)),
			).rejects.toThrow("escapes workspace");
			await symlink(outside, path.join(root, "linked"), process.platform === "win32" ? "junction" : "dir");
			const linked = { path: "linked/out.txt", content: "no" };
			await expect(createWorkspaceSandbox().fork(context(root, "write", countingTool, linked))).rejects.toThrow(
				"contains symlink",
			);
			expect(executions).toBe(0);
			await expect(stat(path.join(outside, "out.txt"))).rejects.toThrow();
		} finally {
			await Promise.all([rm(root, { recursive: true, force: true }), rm(outside, { recursive: true, force: true })]);
		}
	});

	it("does not modify an existing repository's index or branch", async () => {
		const root = await temporaryRoot("git");
		try {
			await runGit(["init"], root);
			await runGit(["config", "user.email", "test@example.com"], root);
			await runGit(["config", "user.name", "Test"], root);
			await writeFile(path.join(root, "tracked.txt"), "base\n", "utf8");
			await runGit(["add", "tracked.txt"], root);
			await runGit(["commit", "-m", "base"], root);
			await writeFile(path.join(root, "staged.txt"), "user\n", "utf8");
			await runGit(["add", "staged.txt"], root);
			const beforeStatus = await runGit(["status", "--short"], root);
			const beforeBranch = await runGit(["branch", "--show-current"], root);
			const args = { path: "created.txt", content: "speculative\n" };
			await createWorkspaceSandbox().fork(context(root, "write", writeTool, args));

			expect(await runGit(["status", "--short"], root)).toBe(beforeStatus);
			expect(await runGit(["branch", "--show-current"], root)).toBe(beforeBranch);
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
					withSandboxWorkspace(root, async (workspace) => {
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

	it("captures exact regular-file deltas through the generic workspace transaction driver", async () => {
		const root = await temporaryRoot("transaction");
		try {
			await writeFile(path.join(root, "changed.txt"), "before\n", "utf8");
			await writeFile(path.join(root, "deleted.txt"), "deleted\n", "utf8");
			await writeFile(path.join(root, "untouched.txt"), "stable\n", "utf8");
			await withSandboxWorkspace(root, async (workspace) => {
				const capture = await workspace.transactions.begin();
				await writeFile(path.join(workspace.sandboxRoot, "changed.txt"), "after!\n", "utf8");
				await writeFile(path.join(workspace.sandboxRoot, "created.txt"), "created\n", "utf8");
				await rm(path.join(workspace.sandboxRoot, "deleted.txt"));
				const delta = await capture.finish();

				expect(delta.complete).toBe(true);
				if (!delta.complete) throw new Error(`workspace transaction was incomplete: ${delta.reason}`);
				const beforeEntry = delta.before.entries.get("changed.txt");
				const afterEntry = delta.after.entries.get("changed.txt");
				if (beforeEntry?.kind !== "file" || afterEntry?.kind !== "file") throw new Error("change clock missing");
				expect(afterEntry.changeTimeMs).toBeGreaterThan(beforeEntry.changeTimeMs);
				expect(delta.changes.map((change) => change.relativePath)).toEqual([
					"changed.txt",
					"created.txt",
					"deleted.txt",
				]);
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

	it("defers transaction observation until the first mutation interval", async () => {
		const root = await temporaryRoot("transaction-deferred");
		try {
			await writeFile(path.join(root, "value.txt"), "base\n", "utf8");
			await withSandboxWorkspace(root, async (workspace) => {
				const clock = path.join(workspace.processRoot, "workspace-transaction.clock");
				await expect(stat(clock)).rejects.toThrow();
				const capture = await workspace.transactions.begin();
				expect((await stat(clock)).isFile()).toBe(true);
				await capture.abort();
			});
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("poisons reuse if the private change-clock identity is replaced", async () => {
		if (process.platform === "win32") return;
		const root = await temporaryRoot("transaction-clock");
		try {
			await writeFile(path.join(root, "value.txt"), "base\n", "utf8");
			await withSandboxWorkspace(root, async (workspace) => {
				const first = await workspace.transactions.begin();
				await first.abort();
				const clock = path.join(workspace.processRoot, "workspace-transaction.clock");
				await link(clock, path.join(workspace.processRoot, "workspace-transaction.alias"));
				const second = await workspace.transactions.begin();
				const delta = await second.finish();
				expect(delta.complete).toBe(false);
				if (delta.complete) throw new Error("replaced workspace clock was unexpectedly accepted");
				expect(delta.reason).toContain("workspace transaction clock identity changed");
			});
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("marks unsupported inode transitions incomplete without undoing the operation", async () => {
		if (process.platform === "win32") return;
		const root = await temporaryRoot("transaction-inode");
		try {
			await writeFile(path.join(root, "target.txt"), "target\n", "utf8");
			await withSandboxWorkspace(root, async (workspace) => {
				const capture = await workspace.transactions.begin();
				const linkPath = path.join(workspace.sandboxRoot, "link.txt");
				await symlink("target.txt", linkPath);
				const delta = await capture.finish();
				expect(delta.complete).toBe(false);
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
			await withSandboxWorkspace(root, async (workspace) => {
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
				expect(recoveredDelta.complete).toBe(true);
				expect(Buffer.from(recoveredDelta.changes[0]?.before ?? []).toString("utf8")).toBe("overlap\n");
				expect(Buffer.from(recoveredDelta.changes[0]?.after ?? []).toString("utf8")).toBe("recovered\n");
			});
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	it("fails a cancelled warm-up before allocating a private workspace", async () => {
		const root = await temporaryRoot("cancelled");
		const controller = new AbortController();
		controller.abort(new Error("cancelled"));
		try {
			await expect(prepareSandboxWorkspace(root, { signal: controller.signal })).rejects.toThrow("cancelled");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});
});

function context<Schema extends typeof writeParameters | typeof editParameters>(
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
		action:
			buildPiActionKey(toolName, args, root) ??
			requiredAction("write", { path: "safe.txt", content: "boundary probe" }, root),
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

function temporaryRoot(label: string): Promise<string> {
	return mkdtemp(path.join(os.tmpdir(), `pi-spec-${label}-`));
}

function runGit(args: string[], cwd: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile("git", args, { cwd }, (error, stdout, stderr) => {
			if (error) reject(new Error(stderr));
			else resolve(stdout);
		});
	});
}
