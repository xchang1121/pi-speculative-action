import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import {
	captureWorkspaceStructure,
	diffWorkspaceStructures,
	ExecutionPathProjection,
	snapshotDependency,
} from "../src/process-observation.ts";

describe("process observation", () => {
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
			expect(beforeEntry.metadataDigest).toBe(afterEntry.metadataDigest);
			// Same-size rapid rewrites can share observable timestamps on coarse-clock filesystems.
			// The regular-file delta, not incidental metadata movement, is the authoritative evidence.
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
			expect(diff.effects[0]).not.toHaveProperty("after");
			const effect = diff.effects[0];
			if (effect?.kind !== "write") throw new Error("write effect missing");
			expect(effect.before).toMatchObject({ kind: "file", size: beforeBytes.byteLength });
			const parentEntry = before.entries.get("");
			if (parentEntry?.kind !== "directory") throw new Error("workspace root structure missing");
			expect(projection.toPhysical(path.join(source, "value.bin"))).toBe(target);
			expect(snapshotDependency(projection.toLogical(target), effect.before, parentEntry)).toMatchObject({
				kind: "file",
				role: "input",
			});
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});

	test("models empty-directory creation and deletion as typed topology effects", async () => {
		const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-process-directory-"));
		const source = path.join(parent, "source");
		const workspace = path.join(parent, "private", "workspace");
		await fs.mkdir(workspace, { recursive: true });
		const projection = new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace });
		try {
			const before = await captureWorkspaceStructure(workspace);
			await fs.mkdir(path.join(workspace, "empty"));
			const after = await captureWorkspaceStructure(workspace);
			const diff = diffWorkspaceStructures(before, after, [], projection);

			expect(diff.complete).toBe(true);
			expect(diff.effects).toHaveLength(1);
			expect(diff.effects[0]).toMatchObject({ kind: "mkdir", relativePath: "empty" });
			const created = diff.effects[0];
			if (created?.kind !== "mkdir") throw new Error("mkdir effect missing");
			expect(created.after).toMatchObject({ kind: "directory", mode: expect.any(Number) });

			await fs.rmdir(path.join(workspace, "empty"));
			const removed = diffWorkspaceStructures(after, await captureWorkspaceStructure(workspace), [], projection);
			expect(removed.complete).toBe(true);
			expect(removed.effects).toHaveLength(1);
			expect(removed.effects[0]).toMatchObject({ kind: "rmdir", relativePath: "empty" });
			if (removed.effects[0]?.kind !== "rmdir") throw new Error("rmdir effect missing");
			expect(removed.effects[0].before.entriesDigest).toBe(created.after.entriesDigest);
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
			await fs.link(original, linked);
			const after = await captureWorkspaceStructure(workspace);
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
		} finally {
			await fs.rm(parent, { recursive: true, force: true });
		}
	});
});
