import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { captureStableFile } from "../src/filesystem-evidence.ts";
import {
	captureWorkspaceStructure,
	diffWorkspaceStructures,
	ExecutionPathProjection,
	hydrateWorkspaceFileEntry,
	snapshotDependency,
} from "../src/process-observation.ts";

describe("process observation", () => {
	test("joins content-free structure snapshots with an authoritative regular-file delta", async ({ onTestFinished }) => {
		const { source, workspace, projection } = await observationWorkspace("structure", onTestFinished);
		const target = path.join(workspace, "value.bin");
		const beforeBytes = Buffer.alloc(2 * 1024 * 1024, 0x41);
		const afterBytes = Buffer.alloc(beforeBytes.byteLength, 0x42);
		await fs.writeFile(target, beforeBytes);
		await fs.mkdir(path.join(workspace, "nested"));
		const limited = await captureWorkspaceStructure(workspace, { maxFiles: 1 });
		expect([limited.complete, limited.files, limited.entries.size]).toEqual([false, 1, 2]);
		const before = await captureWorkspaceStructure(workspace);
		const captured = await captureStableFile(target);
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
		const delta = {
			relativePath: "./value.bin",
			before: beforeBytes,
			after: afterBytes,
			beforeMode: beforeEntry.mode,
			afterMode: afterEntry.mode,
		};
		const diff = diffWorkspaceStructures(before, after, [delta], projection);
		expect(diff.complete).toBe(true);
		expect(diff.effects).toHaveLength(1);
		expect(diff.effects[0]).toMatchObject({ logicalPath: projection.toLogical(target), relativePath: "value.bin" });
		expect(diff.effects[0]?.change).toMatchObject({ ...delta, operation: "write_contents", object: { path: projection.toLogical(target), before: true } });
		const input = hydrateWorkspaceFileEntry(beforeEntry, beforeBytes);
		expect(hydrateWorkspaceFileEntry(beforeEntry, captured)).toEqual(input);
		expect(hydrateWorkspaceFileEntry(beforeEntry, await captureStableFile(target))).toBeUndefined();
		expect(input).toMatchObject({ kind: "file", size: beforeBytes.byteLength });
		const parentEntry = before.entries.get("");
		if (parentEntry?.kind !== "directory") throw new Error("workspace root structure missing");
		expect(projection.toPhysical(path.join(source, "value.bin"))).toBe(target);
		expect(snapshotDependency(projection.toLogical(target), input, parentEntry)).toMatchObject({
			kind: "file",
			role: "input",
		});
	});

	test("models empty-directory creation and deletion as typed topology effects", async ({ onTestFinished }) => {
		const { workspace, projection } = await observationWorkspace("directory", onTestFinished);
		const before = await captureWorkspaceStructure(workspace);
		await fs.mkdir(path.join(workspace, "empty"));
		const after = await captureWorkspaceStructure(workspace);
		const diff = diffWorkspaceStructures(before, after, [], projection);

		expect(diff.complete).toBe(true);
		expect(diff.effects).toHaveLength(1);
		expect(diff.effects[0]).toMatchObject({ relativePath: "empty", change: { kind: "directory" } });
		const created = diff.effects[0]?.change;
		if (created?.kind !== "directory") throw new Error("mkdir effect missing");
		expect(created.before).toBeUndefined();
		expect(created.after).toMatchObject({ kind: "directory", mode: expect.any(Number) });

		await fs.rmdir(path.join(workspace, "empty"));
		const removed = diffWorkspaceStructures(after, await captureWorkspaceStructure(workspace), [], projection);
		expect(removed.complete).toBe(true);
		expect(removed.effects).toHaveLength(1);
		expect(removed.effects[0]).toMatchObject({ relativePath: "empty", change: { kind: "directory" } });
		expect(removed.effects[0]?.change.after).toBeUndefined();
		expect(removed.effects[0]?.change.before).toBe(created.after);
	});

	test("fails closed when replaying bytes would lose hard-link identity", async ({ onTestFinished }) => {
		const { workspace, projection } = await observationWorkspace("hardlink", onTestFinished);
		const original = path.join(workspace, "original.txt");
		const linked = path.join(workspace, "linked.txt");
		const content = Buffer.from("shared inode\n");
		await fs.writeFile(original, content);
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

		expect(diff).toMatchObject({ complete: false, reason: expect.stringContaining("object_anchor_missing") });
	});
});

async function observationWorkspace(prefix: string, onFinished: (cleanup: () => Promise<void>) => void) {
	const parent = await fs.mkdtemp(path.join(os.tmpdir(), `pi-process-${prefix}-`));
	onFinished(() => fs.rm(parent, { recursive: true, force: true }));
	const source = path.join(parent, "source");
	const workspace = path.join(parent, "private", "workspace");
	await fs.mkdir(workspace, { recursive: true });
	return { source, workspace, projection: new ExecutionPathProjection({ sourceRoot: source, workspaceRoot: workspace }) };
}
