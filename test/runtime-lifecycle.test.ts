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

	it("seals synchronously and coalesces every close caller", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const close = vi.fn(async () => {
			await gate;
		});
		const late = vi.fn();
		const lane = new RuntimeLifecycleLane();

		const first = lane.close(close);
		const second = lane.close(close);
		const afterSeal = lane.run(late);
		expect(lane.sealed).toBe(true);
		expect(first).toBe(second);
		expect(afterSeal).toBe(first);
		expect(late).not.toHaveBeenCalled();

		release();
		await first;
		expect(close).toHaveBeenCalledOnce();
	});
});
