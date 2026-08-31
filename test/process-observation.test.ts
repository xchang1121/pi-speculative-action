import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import {
	captureWorkspaceStructure,
	captureWorkspaceTree,
	diffWorkspaceStructures,
	diffWorkspaceTrees,
	ExecutionPathProjection,
	parentSnapshotEntry,
	relativeSnapshotEntry,
	snapshotDependency,
} from "../src/process-observation.ts";

describe("process observation", () => {
	test("projects disposable paths and seals regular final-state effects", async () => {
		const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-process-observation-"));
		const source = path.join(parent, "source");
		const workspace = path.join(parent, "private", "workspace");
		await fs.mkdir(workspace, { recursive: true });
		await fs.writeFile(path.join(workspace, "input.txt"), "before");
		const projection = new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace });
		try {
			const before = await captureWorkspaceTree(workspace);
			await Promise.all([
				fs.writeFile(path.join(workspace, "input.txt"), "after"),
				fs.writeFile(path.join(workspace, "created.txt"), "created"),
			]);
			const after = await captureWorkspaceTree(workspace, { includeFileContent: true });
			const diff = diffWorkspaceTrees(before, after, projection);

			expect(diff.complete).toBe(true);
			expect(diff.effects.map((effect) => [effect.kind, effect.logicalPath])).toEqual([
				["write", path.join(source, "created.txt").replaceAll("\\", "/")],
				["write", path.join(source, "input.txt").replaceAll("\\", "/")],
			]);
			expect(projection.toPhysical(path.join(source, "input.txt"))).toBe(path.join(workspace, "input.txt"));
			const dependency = snapshotDependency(
				projection.toLogical(path.join(workspace, "input.txt")),
				relativeSnapshotEntry(before, path.join(workspace, "input.txt")),
				parentSnapshotEntry(before, path.join(workspace, "input.txt")),
			);
			expect(dependency).toMatchObject({ kind: "file", role: "input" });
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});

	test("joins content-free structure snapshots with an authoritative regular-file delta", async () => {
		const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-process-structure-"));
		const source = path.join(parent, "source");
		const workspace = path.join(parent, "private", "workspace");
		await fs.mkdir(workspace, { recursive: true });
		const target = path.join(workspace, "value.bin");
		const beforeBytes = Buffer.alloc(2 * 1024 * 1024, 0x41);
		const afterBytes = Buffer.alloc(beforeBytes.byteLength, 0x42);
		await fs.writeFile(target, beforeBytes);
		const projection = new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace });
		try {
			const before = await captureWorkspaceStructure(workspace);
			await fs.writeFile(target, afterBytes);
			const after = await captureWorkspaceStructure(workspace);
			const beforeEntry = before.entries.get("value.bin");
			const afterEntry = after.entries.get("value.bin");
			if (beforeEntry?.kind !== "file" || afterEntry?.kind !== "file") throw new Error("file structure missing");

			expect(before.bytesRead).toBe(0);
			expect(after.bytesRead).toBe(0);
			expect("digest" in beforeEntry).toBe(false);
			const diff = diffWorkspaceStructures(
				before,
				after,
				[
					{
						relativePath: "value.bin",
						before: beforeBytes,
						after: afterBytes,
						beforeMode: beforeEntry.mode,
						afterMode: afterEntry.mode,
					},
				],
				projection,
			);
			expect(diff.complete).toBe(true);
			expect(diff.effects).toHaveLength(1);
			expect(diff.effects[0]).toMatchObject({ kind: "write", relativePath: "value.bin" });
			expect(diff.effects[0]?.after?.content).toBe(afterBytes);
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});

	test("fails closed when the regular-file delta cannot represent a directory transition", async () => {
		const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-process-directory-"));
		const source = path.join(parent, "source");
		const workspace = path.join(parent, "private", "workspace");
		await fs.mkdir(workspace, { recursive: true });
		const projection = new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace });
		try {
			const before = await captureWorkspaceStructure(workspace);
			const beforeTree = await captureWorkspaceTree(workspace);
			await fs.mkdir(path.join(workspace, "empty"));
			const after = await captureWorkspaceStructure(workspace);
			const afterTree = await captureWorkspaceTree(workspace, { includeFileContent: true });
			const diff = diffWorkspaceStructures(before, after, [], projection);

			expect(diff).toMatchObject({
				complete: false,
				reason: expect.stringContaining("unsupported_directory_transition"),
			});
			expect(diffWorkspaceTrees(beforeTree, afterTree, projection)).toMatchObject({
				complete: false,
				reason: expect.stringContaining("unsupported_directory"),
			});
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});

	test("fails closed when replaying bytes would lose hard-link identity", async () => {
		const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-process-hardlink-"));
		const source = path.join(parent, "source");
		const workspace = path.join(parent, "private", "workspace");
		await fs.mkdir(workspace, { recursive: true });
		const original = path.join(workspace, "original.txt");
		const linked = path.join(workspace, "linked.txt");
		const content = Buffer.from("shared inode\n");
		await fs.writeFile(original, content);
		const projection = new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace });
		try {
			const before = await captureWorkspaceStructure(workspace);
			const beforeTree = await captureWorkspaceTree(workspace);
			await fs.link(original, linked);
			const after = await captureWorkspaceStructure(workspace);
			const afterTree = await captureWorkspaceTree(workspace, { includeFileContent: true });
			const linkedEntry = after.entries.get("linked.txt");
			if (linkedEntry?.kind !== "file") throw new Error("hard link structure missing");
			const diff = diffWorkspaceStructures(
				before,
				after,
				[
					{
						relativePath: "linked.txt",
						after: content,
						afterMode: linkedEntry.mode,
					},
				],
				projection,
			);

			expect(diff).toMatchObject({ complete: false, reason: expect.stringContaining("unsupported_hardlink") });
			expect(diffWorkspaceTrees(beforeTree, afterTree, projection)).toMatchObject({
				complete: false,
				reason: expect.stringContaining("unsupported_hardlink"),
			});
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});
});
