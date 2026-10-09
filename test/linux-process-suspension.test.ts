import { EventEmitter } from "node:events";
import * as filesystem from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { Duplex } from "node:stream";
import type { TimerOptions } from "node:timers";
import * as timers from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { deferred } from "./async.ts";

vi.mock("node:fs/promises", { spy: true });
vi.mock("node:timers/promises", { spy: true });

class Control extends EventEmitter {
	readonly requests: number[] = [];
	write(bytes: Buffer, callback: (error?: Error) => void) { this.requests.push(bytes.readInt32LE(0)); callback(); }
	reply(pid: number) { const reply = Buffer.alloc(40); reply.writeInt32LE(pid); this.emit("data", reply); return reply; }
}

/** Drive the real native request framing with sampled process states and explicit cancellation, without processes or wall-clock waits. */
function fixture(input: { states?: string[]; reports?: string[]; replies?: number[] } = {}) {
	const backend = new LinuxProcessReuseBackend({ storeRoot: path.join(os.tmpdir(), "suspension-fixture") });
	const request = Reflect.get(backend, "requestProcessImageAtFrontier") as (report: string, channel: Duplex, wake: () => boolean,
		stop: AbortSignal, execution: AbortSignal) => Promise<{ pid: number; reply: Buffer } | undefined>;
	const join = new AbortController(), execution = new AbortController(), channel = new Control(), requested = deferred();
	const states = [...input.states ?? ["0 0x3 0x0\n"]], reports = [...input.reports ?? ["RUNNING 123\n"]], replies = [...input.replies ?? [123]];
	const next = <Value>(values: Value[]): Value | undefined => values.length > 1 ? values.shift() : values[0];
	const reads = vi.spyOn(filesystem, "readFile").mockImplementation(async target => {
		if (target === "/fixture/fd-offsets") return next(reports) ?? "";
		if (target === "/proc/123/syscall") return next(states) ?? "";
		throw new Error(`unexpected read: ${String(target)}`);
	});
	const pauses: number[] = [];
	let afterPause: (() => void) | undefined;
	const delay = vi.spyOn(timers, "setTimeout").mockImplementation(<Value = void>(ms?: number, value?: Value, options?: TimerOptions) => {
		pauses.push(ms ?? 0); afterPause?.();
		return options?.signal?.aborted ? Promise.reject(options.signal.reason) : Promise.resolve(value as Value);
	});
	const wake = vi.fn(() => {
		requested.resolve(); const reply = next(replies); if (reply !== undefined) channel.reply(reply); return true;
	});
	return { channel, join, execution, requested, wake, pauses, reads,
		onPause: (callback: () => void) => { afterPause = callback; },
		run: () => request.call(backend, "/fixture/fd-offsets", channel as unknown as Duplex, wake,
			AbortSignal.any([join.signal, execution.signal]), execution.signal),
		close: () => { join.abort(); execution.abort(); reads.mockRestore(); delay.mockRestore(); } };
}

describe("native suspension frontier polling", () => {
	it("waits through CPU and non-I/O states, then retries a completed ACK write before capturing the read frontier", async () => {
		const test = fixture({ states: ["running\n", "39 0x0\n", "1 0x4\n", "0 0x3\n"], replies: [0, 123] });
		try {
			const captured = await test.run();
			expect(captured?.pid).toBe(123);
			expect(captured?.reply.readInt32LE(0)).toBe(123);
			expect(test.channel.requests).toEqual([123, 123]);
			expect(test.pauses).toEqual([10, 10, 10]);
		} finally { test.close(); }
	});

	it("stops permanent safe declines when the Actor cancels its join", async () => {
		const test = fixture({ replies: [0] });
		test.onPause(() => { if (test.pauses.length === 3) test.join.abort(new Error("Actor cancelled")); });
		try {
			await expect(test.run()).resolves.toBeUndefined();
			expect(test.channel.requests).toEqual([123, 123, 123]);
			expect(test.pauses).toEqual([10, 10, 10]);
		} finally { test.close(); }
	});

	it("stops transient non-I/O polling at cancellation without sending a native request", async () => {
		const test = fixture({ states: ["39 0x0\n"] });
		test.onPause(() => { if (test.pauses.length === 2) test.join.abort(); });
		try {
			await expect(test.run()).resolves.toBeUndefined();
			expect(test.channel.requests).toEqual([]);
			expect(test.pauses).toEqual([10, 10]);
		} finally { test.close(); }
	});

	it.each([0, 123])("drains an in-flight reply after the Actor cancels (reply=%s)", async reply => {
		const test = fixture({ replies: [] });
		try {
			let settled = false;
			const pending = test.run().finally(() => { settled = true; });
			await test.requested.promise;
			test.join.abort(new Error("Actor cancelled"));
			await Promise.resolve();
			expect(settled).toBe(false);
			expect(test.channel.listenerCount("data")).toBe(1);
			const bytes = test.channel.reply(reply), captured = await pending;
			expect(captured).toEqual(reply ? { pid: 123, reply: bytes } : undefined);
			expect(test.channel.requests).toEqual([123]);
			expect(test.pauses).toEqual([]);
			expect(test.channel.listenerCount("data")).toBe(0);
		} finally { test.close(); }
	});

	it("returns a native capture failure once without retrying the retired process", async () => {
		const test = fixture({ replies: [-123] });
		try {
			const captured = await test.run();
			expect(captured?.reply.readInt32LE(0)).toBe(-123);
			expect(test.channel.requests).toEqual([123]);
			expect(test.pauses).toEqual([]);
		} finally { test.close(); }
	});

	it("releases an in-flight request when its owning execution is aborted", async () => {
		const test = fixture({ replies: [] });
		try {
			const pending = test.run(), rejected = expect(pending).rejects.toThrow("continuation producer closed");
			await test.requested.promise;
			test.execution.abort();
			await rejected;
			expect(test.channel.requests).toEqual([123]);
			expect(test.channel.listenerCount("data")).toBe(0);
			expect(test.pauses).toEqual([]);
		} finally { test.close(); }
	});

	it.each(["completed", "missing"] as const)("preserves the exit condition for a %s process", async condition => {
		const test = fixture(condition === "completed" ? { reports: ["OFD 0\n"] } : { states: [""] });
		try {
			await expect(test.run()).resolves.toBeUndefined();
			expect(test.channel.requests).toEqual([]);
			expect(test.pauses).toEqual([]);
		} finally { test.close(); }
	});
});
