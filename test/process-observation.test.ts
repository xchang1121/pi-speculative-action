import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import {
	captureWorkspaceTree,
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
});
