import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

/** Each suite owns its directories and chooses when to dispose them after its other resources. */
export function temporaryDirectories(prefix: string, base?: string) {
	const roots: string[] = [];
	return {
		async create(directory = base ?? os.tmpdir()): Promise<string> {
			const root = await mkdtemp(path.join(directory, prefix));
			assert.equal(path.dirname(root), path.resolve(directory));
			assert.ok(path.basename(root).startsWith(prefix));
			roots.push(root);
			return root;
		},
		async dispose(): Promise<void> {
			const removed = await Promise.allSettled(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
			const failed = removed.find(result => result.status === "rejected");
			if (failed) throw failed.reason;
		},
	};
}
