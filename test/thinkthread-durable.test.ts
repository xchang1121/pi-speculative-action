import { AgentPosixClient, ClientConfig, parseFsSnapshotId, TransportError } from "@thinkthread/agent-posix";
import { expect, test } from "vitest";
import { DurableFsExecutor } from "../src/thinkthread/durable-fs.ts";
import { RuntimeLifecycleLane } from "../src/runtime-lifecycle.ts";
import { deferred, nextTurn } from "./async.ts";

const snapshot = parseFsSnapshotId("fsnap-00000000-0000-0000-0000-000000000001");
type Request = { id: string; method: string; params: { requestId: string } };
const rejected = (code: string) => ({ ok: false, error: { code, message: code, retryable: false } });
const uncertain = () => { throw new TransportError("reply lost", "completion_unknown"); };
function peer(reply: (request: Request, index: number) => unknown) {
	const calls: Request[] = [];
	const client = new AgentPosixClient(new ClientConfig("test", "test"), { roundTrip: async frame => {
		const request = JSON.parse(Buffer.from(frame.bytesForTransport()).toString()) as Request;
		calls.push(request);
		return Buffer.from(JSON.stringify({ protocolVersion: 2, id: request.id, ...reply(request, calls.length - 1) as object }));
	} });
	return { calls, durable: new DurableFsExecutor(client) };
}
function status(request: Request, state: string, method = "fs.snapshot.remove") {
	return { ok: true, result: { requestId: request.params.requestId, method, state,
		acceptedAtUnixMs: 1, finishedAtUnixMs: state === "accepted" || state === "running" ? 0 : 2,
		result: state === "succeeded" ? {} : null, error: state === "failed" || state === "needs_recovery" ? rejected("FsFailure").error : null } };
}

test.each(["not_sent", "absent", "running", "rejected"])("reconciles %s delivery with the original durable identity", async outcome => {
	let invocations = 0, polls = 0, closes = 0;
	const { durable, calls } = peer(request => {
		if (request.method === "fs.request.close") { closes++; return { ok: true, result: {} }; }
		if (request.method === "fs.request.status") {
			polls++;
			return outcome === "absent" ? rejected("RequestNotFound") : status(request, polls === 1 ? "running" : "succeeded");
		}
		if (++invocations > 1) return { ok: true, result: {} };
		if (outcome === "not_sent") throw new TransportError("not connected", "not_sent");
		return outcome === "rejected" ? rejected("AlreadyAccepted") : uncertain();
	});
	await durable.snapshotRemove(snapshot);
	expect(invocations).toBe(outcome === "not_sent" || outcome === "absent" ? 2 : 1);
	expect(closes).toBe(1);
	expect(new Set(calls.map(call => call.params.requestId)).size).toBe(1);
});

test.each(["failed", "needs_recovery", "closing", "missing", "poll_failure", "method_mismatch"])("preserves request ownership after %s", async outcome => {
	let polls = 0;
	const { durable, calls } = peer(request => {
		if (request.method === "fs.snapshot.remove") return uncertain();
		if (request.method === "fs.request.close") return { ok: true, result: {} };
		if (++polls === 1 && outcome === "missing") return status(request, "running");
		if (outcome === "missing") return rejected("RequestNotFound");
		if (outcome === "poll_failure") return uncertain();
		return status(request, outcome === "method_mismatch" ? "running" : outcome, outcome === "method_mismatch" ? "fs.run" : undefined);
	});
	const codes: Record<string, string> = { failed: "FsFailure", missing: "request_disappeared", poll_failure: "needs_recovery", closing: "response_closing" };
	const code = codes[outcome] ?? outcome;
	await expect(durable.snapshotRemove(snapshot)).rejects.toMatchObject({ code });
	expect(calls.filter(call => call.method === "fs.snapshot.remove")).toHaveLength(1);
	expect(calls.filter(call => call.method === "fs.request.close")).toHaveLength(outcome === "failed" ? 1 : 0);
});

test("retries response cleanup without replaying a completed effect", async () => {
	let closes = 0;
	const { durable, calls } = peer(request => request.method === "fs.request.close" && ++closes === 1 ? uncertain() : { ok: true, result: {} });
	await durable.snapshotRemove(snapshot);
	expect(await durable.drainCleanup()).toBe(0);
	expect(closes).toBe(2);
	expect(calls.filter(call => call.method === "fs.snapshot.remove")).toHaveLength(1);
});

test.each(["before_run", "in_flight", "not_sent"])("keeps payload and TARGET ownership through %s cancellation", async outcome => {
	const controller = new AbortController();
	const run = { exit: { kind: "cancelled" }, outputChunks: [], outputTruncated: false, retainedOutputBytes: 0,
		observedOutputBytes: 0, runKey: "run", targetSnapshotId: snapshot, changedPaths: 1, changedBytes: 3,
		metrics: { setupMs: 1, executeMs: 2, sealMs: 3, cleanupMs: 4 } };
	const payload = { payloadId: "fspayload-00000000-0000-0000-0000-000000000001", expectedBytes: 0, currentBytes: 0,
		state: "sealed", lifecycle: { kind: "caller_managed" } };
	const { durable, calls } = peer(request => {
		if (request.method === "fs.payload.create") return { ok: true, result: { ...payload, state: "open" } };
		if (request.method === "fs.payload.seal") {
			if (outcome === "before_run") controller.abort();
			return { ok: true, result: payload };
		}
		if (request.method === "fs.run") {
			if (outcome === "not_sent") throw new TransportError("not connected", "not_sent");
			controller.abort(); return uncertain();
		}
		if (request.method === "fs.request.cancel") return { ok: true, result: { accepted: true, state: "running" } };
		if (request.method === "fs.request.status") return { ok: true, result: {
			...status(request, "cancelled", "fs.run").result, result: run } };
		return { ok: true, result: {} };
	});
	const result = durable.runWithInput({ snapshotId: snapshot, writes: "snapshot", invocation: { argv: ["tool"] } }, new Uint8Array(), controller.signal);
	if (outcome === "in_flight") await expect(result).resolves.toEqual(run);
	else await expect(result).rejects.toThrow();
	expect(calls.filter(call => call.method === "fs.run")).toHaveLength(outcome === "before_run" ? 0 : outcome === "not_sent" ? 3 : 1);
	expect(calls.filter(call => call.method === "fs.payload.remove")).toHaveLength(outcome === "in_flight" ? 0 : 1);
	expect(calls.filter(call => call.method === "fs.request.close")).toHaveLength(outcome === "in_flight" ? 1 : 0);
	if (outcome === "in_flight") expect(calls.find(call => call.method === "fs.request.cancel")?.params.requestId)
		.toBe(calls.find(call => call.method === "fs.run")!.params.requestId);
});

test("closing drains admitted resource acquisition and nested releases exactly once", async () => {
	const lifetime = new RuntimeLifecycleLane(), acquired = deferred(), cleanup = deferred();
	const events: string[] = [];
	const resource = { dispose: async () => {
		events.push("release");
		lifetime.own({ dispose: async () => { await cleanup.promise; events.push("nested"); } });
		throw new Error("cleanup failure");
	} };
	const operation = lifetime.admit(async () => { await acquired.promise; lifetime.own(resource); lifetime.own(resource); events.push("acquired"); });
	const close = lifetime.dispose(() => { events.push("closed"); });
	expect(lifetime.dispose()).toBe(close);
	await expect(lifetime.admit(async () => {})).rejects.toThrow("closed");
	acquired.resolve(); await operation; await nextTurn();
	expect(events).toEqual(["acquired", "release"]);
	cleanup.resolve(); await close;
	await lifetime.release(resource);
	expect(events).toEqual(["acquired", "release", "nested", "closed"]);
});
