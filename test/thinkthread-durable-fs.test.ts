import {
	type AgentPosixClient,
	parseFsSnapshotId,
	parseRequestId,
	parseThinkThreadId,
	TransportError,
} from "@thinkthread/agent-posix";
import { describe, expect, it, vi } from "vitest";
import { DurableFsExecutor } from "../src/thinkthread/durable-fs.ts";
import { ThinkThreadRecoveryRequiredError } from "../src/thinkthread/errors.ts";

const snapshotID = parseFsSnapshotId("fsnap-00000000-0000-4000-8000-000000000001");
const ownerID = parseThinkThreadId("tt-00000000-0000-4000-8000-000000000002");
const snapshot = {
	snapshotId: snapshotID,
	ownerThinkthreadId: ownerID,
	createdAtUnixMs: 1,
	logicalBytes: 42,
};

describe("ThinkThread durable fs executor", () => {
	it("retries not-sent delivery with the same request ID", async () => {
		const requestIDs: string[] = [];
		const snapshotCreate = vi.fn(async ({ requestId }: { readonly requestId: string }) => {
			requestIDs.push(requestId);
			if (requestIDs.length === 1) throw new TransportError("not sent", "not_sent");
			return snapshot;
		});
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestClose }));

		await expect(durable.snapshotCreate()).resolves.toEqual(snapshot);
		expect(requestIDs).toHaveLength(2);
		expect(new Set(requestIDs).size).toBe(1);
		expect(requestClose).toHaveBeenCalledOnce();
	});

	it("settles completion-unknown from durable status and closes the record", async () => {
		let requestID = parseRequestId("req-00000000-0000-4000-8000-000000000003");
		const snapshotCreate = vi.fn(async ({ requestId }: { readonly requestId: typeof requestID }) => {
			requestID = requestId;
			throw new TransportError("unknown", "completion_unknown");
		});
		const requestStatus = vi.fn(async () => ({
			requestId: requestID,
			method: "fs.snapshot.create" as const,
			state: "succeeded" as const,
			acceptedAtUnixMs: 1,
			finishedAtUnixMs: 2,
			result: snapshot,
			error: null,
		}));
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestStatus, requestClose }));

		await expect(durable.snapshotCreate()).resolves.toEqual(snapshot);
		expect(requestStatus).toHaveBeenCalledWith({ requestId: requestID });
		expect(requestClose).toHaveBeenCalledWith({ requestId: requestID });
	});

	it("fails closed and retains a needs-recovery record", async () => {
		let requestID = parseRequestId("req-00000000-0000-4000-8000-000000000004");
		const snapshotCreate = vi.fn(async ({ requestId }: { readonly requestId: typeof requestID }) => {
			requestID = requestId;
			throw new TransportError("unknown", "completion_unknown");
		});
		const requestStatus = vi.fn(async () => ({
			requestId: requestID,
			method: "fs.snapshot.create" as const,
			state: "needs_recovery" as const,
			acceptedAtUnixMs: 1,
			finishedAtUnixMs: 2,
			result: null,
			error: null,
		}));
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestStatus, requestClose }));

		await expect(durable.snapshotCreate()).rejects.toBeInstanceOf(ThinkThreadRecoveryRequiredError);
		expect(requestClose).not.toHaveBeenCalled();
	});

	it("queues a failed record close and drains it without repeating the operation", async () => {
		const snapshotCreate = vi.fn(async () => snapshot);
		const requestClose = vi
			.fn()
			.mockRejectedValueOnce(new TransportError("not sent", "not_sent"))
			.mockResolvedValue({});
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestClose }));

		await expect(durable.snapshotCreate()).resolves.toEqual(snapshot);
		expect(snapshotCreate).toHaveBeenCalledOnce();
		expect(durable.cleanupBacklog()).toBe(1);
		await expect(durable.drainCleanup()).resolves.toBe(0);
		expect(requestClose).toHaveBeenCalledTimes(2);
		expect(snapshotCreate).toHaveBeenCalledOnce();
	});
});

function fakeClient(fs: Record<string, unknown>): AgentPosixClient {
	return { fs } as unknown as AgentPosixClient;
}
