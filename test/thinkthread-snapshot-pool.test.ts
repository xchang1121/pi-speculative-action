import { parseFsSnapshotId, parseThinkThreadId } from "@thinkthread/agent-posix";
import { describe, expect, it, vi } from "vitest";
import type { DurableFsExecutor } from "../src/thinkthread/durable-fs.ts";
import { ThinkThreadSnapshotPool } from "../src/thinkthread/snapshot-pool.ts";

const ownerID = parseThinkThreadId("tt-00000000-0000-4000-8000-000000000001");

describe("ThinkThread snapshot pool", () => {
	it("does not clear a replacement BASE when an invalidated creation fails", async () => {
		let rejectFirst!: (error: Error) => void;
		const firstSnapshot = new Promise<never>((_resolve, reject) => {
			rejectFirst = reject;
		});
		const snapshotCreate = vi
			.fn()
			.mockImplementationOnce(() => firstSnapshot)
			.mockResolvedValue(snapshotView(2));
		const snapshotRemove = vi.fn(async () => undefined);
		const durable = {
			snapshotCreate,
			snapshotRemove,
			drainCleanup: vi.fn(async () => 0),
		} as unknown as DurableFsExecutor;
		const pool = new ThinkThreadSnapshotPool(durable);
		await pool.beginTurn("turn-1");

		const first = pool.acquireRoot().then(
			() => undefined,
			(error: unknown) => error,
		);
		const invalidation = pool.invalidate().then(
			() => undefined,
			(error: unknown) => error,
		);
		const replacement = await pool.acquireRoot();
		rejectFirst(new Error("first BASE failed"));

		expect(await first).toBeInstanceOf(Error);
		expect(await invalidation).toBeInstanceOf(Error);
		const reused = await pool.acquireRoot();
		expect(reused.lease.id).toBe(replacement.lease.id);
		expect(snapshotCreate).toHaveBeenCalledTimes(2);

		await Promise.all([replacement.lease.release(), reused.lease.release()]);
		await pool.finishTurn("turn-1");
		expect(snapshotRemove).toHaveBeenCalledOnce();
		await pool.dispose();
	});
});

function snapshotView(sequence: number) {
	return {
		snapshotId: parseFsSnapshotId(`fsnap-00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`),
		ownerThinkthreadId: ownerID,
		createdAtUnixMs: sequence,
		logicalBytes: 100,
	};
}
