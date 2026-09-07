import {
	type AgentPosixClient,
	parseFsPayloadId,
	parseFsSnapshotId,
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
	it.each(["not_sent", "completion_unknown", "needs_recovery"] as const)("settles %s without changing request identity", async (delivery) => {
		const requestIDs: string[] = [];
		const snapshotCreate = vi.fn(async ({ requestId }: { readonly requestId: string }) => {
			requestIDs.push(requestId);
			if (delivery === "not_sent" && requestIDs.length > 1) return snapshot;
			throw new TransportError("transport failed", delivery === "not_sent" ? "not_sent" : "completion_unknown");
		});
		const requestStatus = vi.fn(async () => ({
			requestId: requestIDs[0], method: "fs.snapshot.create",
			state: delivery === "needs_recovery" ? "needs_recovery" : "succeeded",
			acceptedAtUnixMs: 1, finishedAtUnixMs: 2, result: snapshot, error: null,
		}));
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestStatus, requestClose }));
		if (delivery === "needs_recovery") await expect(durable.snapshotCreate()).rejects.toBeInstanceOf(ThinkThreadRecoveryRequiredError);
		else await expect(durable.snapshotCreate()).resolves.toEqual(snapshot);
		expect(requestIDs).toHaveLength(delivery === "not_sent" ? 2 : 1);
		expect(new Set(requestIDs).size).toBe(1);
		expect(requestStatus).toHaveBeenCalledTimes(delivery === "not_sent" ? 0 : 1);
		expect(requestClose).toHaveBeenCalledTimes(delivery === "needs_recovery" ? 0 : 1);
	});

	it.each(["staging_abort", "retry", "cancelled"])("owns upload, invocation and terminal artifacts: %s", async (outcome) => {
		const controller = new AbortController();
		const payloadId = parseFsPayloadId("fspayload-00000000-0000-4000-8000-000000000005");
		const payloadCreate = vi.fn(async () => ({ payloadId }));
		const payloadSeal = vi.fn(async () => {
			if (outcome === "staging_abort") controller.abort();
			return { payloadId };
		});
		const result = { targetSnapshotId: snapshotID, exit: { kind: "cancelled" } };
		const run = vi.fn(async (_request: { requestId: string }) => {
			if (outcome === "cancelled") throw new TransportError("unknown", "completion_unknown");
			if (run.mock.calls.length === 1) throw new TransportError("not sent", "not_sent");
			return result;
		});
		const requestStatus = vi.fn(async ({ requestId }: { requestId: string }) => ({
			requestId, method: "fs.run", state: "cancelled", result, error: null,
		}));
		const payloadRemove = vi.fn(async () => ({}));
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({
			payloadCreate, payloadWrite: async () => ({ payloadId }), payloadSeal, payloadRemove,
			run, requestStatus, requestClose, requestCancel: async () => ({}),
		}));
		const pending = durable.runWithInput({ snapshotId: snapshotID, writes: "snapshot", invocation: { argv: ["node"] } }, Buffer.from("input"), controller.signal);
		if (outcome === "staging_abort") await expect(pending).rejects.toMatchObject({ name: "AbortError" });
		else await expect(pending).resolves.toBe(result);
		expect(payloadCreate).toHaveBeenCalledOnce();
		expect(run).toHaveBeenCalledTimes(outcome === "staging_abort" ? 0 : outcome === "retry" ? 2 : 1);
		expect(payloadRemove).toHaveBeenCalledTimes(outcome === "staging_abort" ? 1 : 0);
		expect(requestClose).toHaveBeenCalledTimes(outcome === "staging_abort" ? 0 : 1);
		if (outcome === "retry") expect(run.mock.calls[0]).toEqual(run.mock.calls[1]);
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
