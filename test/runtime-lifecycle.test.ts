import { deferred, nextTurn } from "./async.ts";
import { describe, expect, it, vi } from "vitest";
import { RuntimeLifecycleLane } from "../src/runtime-lifecycle.ts";

describe("RuntimeLifecycleLane", () => {
	it("closes after admitted serial proofs and concurrent borrowers, including a failed proof", async () => {
		const lane = new RuntimeLifecycleLane(), order: string[] = [];
		const proof = deferred(), borrower = deferred();
		const first = lane.serialize(async () => { order.push("first"); await proof.promise; throw new Error("stale proof"); });
		const failed = expect(first).rejects.toThrow("stale proof");
		const second = lane.serialize(() => { order.push("second"); return 42; });
		const parallel = lane.admit(() => borrower.promise);
		let closed = false;
		const closing = lane.close(async () => { await lane.drain(); order.push("closed"); }).then(() => { closed = true; });
		try {
			await expect(lane.serialize(() => order.push("late"))).rejects.toThrow("closed");
			proof.resolve();
			await failed;
			expect(await second).toBe(42);
			await nextTurn();
			expect(closed).toBe(false);
			expect(order).toEqual(["first", "second"]);
		} finally { proof.resolve(); borrower.resolve(); await Promise.all([failed, second, parallel, closing]); }
		expect(order).toEqual(["first", "second", "closed"]);
	});

	it("serializes reusable operations and contains a failed predecessor", async () => {
		const order: number[] = [];
		const lane = new RuntimeLifecycleLane();
		const first = lane.run(async () => {
			order.push(1);
			throw new Error("failed lifecycle callback");
		});
		const second = lane.run(() => {
			order.push(2);
		});

		await expect(first).rejects.toThrow("failed lifecycle callback");
		await second;
		expect(order).toEqual([1, 2]);
	});

	it.each([false, true])("seals synchronously and coalesces close and release callers (dispose fails=%s)", async (fails) => {
		const { promise: gate, resolve: release } = deferred();
		const close = vi.fn(async () => {
			await gate;
		});
		const late = vi.fn();
		const lane = new RuntimeLifecycleLane();
		let nested: Promise<void> | undefined, reenter = true, closed = false;
		const { promise: resourceGate, resolve: finishResource } = deferred();
		const resource = { dispose: vi.fn(async () => {
			if (reenter) { reenter = false; nested = lane.release(resource); }
			await resourceGate;
			if (fails) throw new Error("resource cleanup failed");
		}) };
		const firstRelease = lane.release(resource), secondRelease = lane.release(resource);

		const first = lane.close(close);
		const second = lane.close(close);
		const afterSeal = lane.run(late);
		const completion = first.then(() => { closed = true; });
		try {
			expect(lane.sealed).toBe(true);
			expect(first).toBe(second);
			expect(afterSeal).toBe(first);
			expect(late).not.toHaveBeenCalled();
			release();
			await nextTurn();
			expect(closed).toBe(false);
			expect(resource.dispose).toHaveBeenCalledOnce();
			expect(firstRelease).toBe(secondRelease);
			expect(nested).toBe(firstRelease);
		} finally {
			release(); finishResource();
			await Promise.all([firstRelease, secondRelease, nested, first, second, afterSeal, completion]);
		}
		expect(close).toHaveBeenCalledOnce();
		expect(lane.release(resource)).toBe(firstRelease);
	});
});
