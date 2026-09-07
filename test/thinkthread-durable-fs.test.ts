import { mkdtemp, rm } from "node:fs/promises";
import { getEventListeners } from "node:events";
import { createServer, type Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import {
	type AgentPosixClient,
	ClientConfig, MAX_CONTROL_FRAME_BYTES, SensitiveRequestFrame,
	parseFsPayloadId,
	parseFsSnapshotId,
	parseThinkThreadId,
	TransportError,
} from "@thinkthread/agent-posix";
import { describe, expect, it, vi } from "vitest";
import { DurableFsExecutor } from "../src/thinkthread/durable-fs.ts";
import { ThinkThreadRecoveryRequiredError } from "../src/thinkthread/errors.ts";
import { createThinkThreadClient } from "../src/thinkthread/control-transport.ts";

const snapshotID = parseFsSnapshotId("fsnap-00000000-0000-4000-8000-000000000001");
const ownerID = parseThinkThreadId("tt-00000000-0000-4000-8000-000000000002");
const snapshot = {
	snapshotId: snapshotID,
	ownerThinkthreadId: ownerID,
	createdAtUnixMs: 1,
	logicalBytes: 42,
};

describe("ThinkThread durable fs executor", () => {
	it.each(["not_sent", "completion_unknown", "needs_recovery", "running"] as const)("settles %s without changing request identity", async (delivery) => {
		vi.useFakeTimers();
		const requestIDs: string[] = [];
		const snapshotCreate = vi.fn(async ({ requestId }: { readonly requestId: string }) => {
			requestIDs.push(requestId);
			if (delivery === "not_sent" && requestIDs.length > 1) return snapshot;
			throw new TransportError("transport failed", delivery === "not_sent" ? "not_sent" : "completion_unknown");
		});
		const requestStatus = vi.fn(async () => ({
			requestId: requestIDs[0], method: "fs.snapshot.create",
			state: delivery === "needs_recovery" || delivery === "running" ? delivery : "succeeded",
			acceptedAtUnixMs: 1, finishedAtUnixMs: 2, result: snapshot, error: null,
		}));
		const requestClose = vi.fn(async () => ({}));
		const durable = new DurableFsExecutor(fakeClient({ snapshotCreate, requestStatus, requestClose }));
		const recovery = delivery === "needs_recovery" || delivery === "running";
		try {
			const pending = recovery ? expect(durable.snapshotCreate()).rejects.toBeInstanceOf(ThinkThreadRecoveryRequiredError)
				: expect(durable.snapshotCreate()).resolves.toEqual(snapshot);
			await vi.advanceTimersByTimeAsync(5_001);
			await pending;
		} finally { vi.useRealTimers(); }
		expect(requestIDs).toHaveLength(delivery === "not_sent" ? 2 : 1);
		expect(new Set(requestIDs).size).toBe(1);
		expect(requestStatus.mock.calls.length).toBe(delivery === "not_sent" ? 0 : delivery === "running" ? 101 : 1);
		expect(requestClose).toHaveBeenCalledTimes(recovery ? 0 : 1);
	});

	it.each(["staging_abort", "delivery_abort", "retry", "cancelled"])("owns upload, invocation and terminal artifacts: %s", async (outcome) => {
		const controller = new AbortController();
		const payloadId = parseFsPayloadId("fspayload-00000000-0000-4000-8000-000000000005");
		const payloadCreate = vi.fn(async () => ({ payloadId }));
		const payloadSeal = vi.fn(async () => {
			if (outcome === "staging_abort") controller.abort();
			return { payloadId };
		});
		const result = { targetSnapshotId: snapshotID, exit: { kind: "cancelled" } };
		const run = vi.fn(async (_request: { requestId: string }) => {
			if (outcome === "delivery_abort") controller.abort();
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
		const aborted = outcome.endsWith("abort");
		if (aborted) await expect(pending).rejects.toMatchObject({ name: "AbortError" });
		else await expect(pending).resolves.toBe(result);
		expect(payloadCreate).toHaveBeenCalledOnce();
		expect(run).toHaveBeenCalledTimes(outcome === "staging_abort" ? 0 : outcome === "retry" ? 2 : 1);
		expect(payloadRemove).toHaveBeenCalledTimes(aborted ? 1 : 0);
		expect(requestClose).toHaveBeenCalledTimes(aborted ? 0 : 1);
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

	it.each(["frame", "EOF", "oversize", "deadline", "abort"])("releases the control connection on %s", async (outcome) => {
		const directory = await mkdtemp(path.join(os.tmpdir(), "tt-transport-"));
		const endpoint = process.platform === "win32" ? `\\\\.\\pipe\\tt-${path.basename(directory)}` : path.join(directory, "control");
		const controller = new AbortController();
		const peers = new Set<Socket>();
		const server = createServer({ allowHalfOpen: true }, (socket) => {
			peers.add(socket);
			socket.on("error", () => undefined);
			socket.once("data", () => {
				if (outcome === "frame") socket.end("reply\n");
				if (outcome === "EOF") socket.end("partial");
				if (outcome === "oversize") socket.end(Buffer.alloc(MAX_CONTROL_FRAME_BYTES + 1, 65));
				if (outcome === "abort") controller.abort();
			});
			socket.once("end", () => { if (outcome !== "deadline") socket.end(); });
		});
		await new Promise<void>((resolve) => server.listen(endpoint, resolve));
		try {
			const client = createThinkThreadClient(new ClientConfig(endpoint, "test"), { timeoutMs: 100, signal: controller.signal });
			const pending = client.transport.roundTrip(new SensitiveRequestFrame(Buffer.from("request\n")));
			if (outcome === "frame") await expect(pending).resolves.toEqual(Buffer.from("reply"));
			else await expect(pending).rejects.toMatchObject({ delivery: "completion_unknown" });
			expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
		} finally {
			for (const peer of peers) peer.destroy();
			await new Promise<void>((resolve) => server.close(() => resolve()));
			await rm(directory, { recursive: true, force: true });
		}
	});
});

function fakeClient(fs: Record<string, unknown>): AgentPosixClient {
	return { fs } as unknown as AgentPosixClient;
}
