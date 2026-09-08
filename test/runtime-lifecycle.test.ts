import { describe, expect, it, vi } from "vitest";
import { RuntimeLifecycleLane } from "../src/runtime-lifecycle.ts";

describe("RuntimeLifecycleLane", () => {
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
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const close = vi.fn(async () => {
			await gate;
		});
		const late = vi.fn();
		const lane = new RuntimeLifecycleLane();
		let finishResource!: () => void, nested: Promise<void> | undefined, reenter = true, closed = false;
		const resourceGate = new Promise<void>((resolve) => { finishResource = resolve; });
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
			await new Promise<void>((resolve) => setImmediate(resolve));
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
