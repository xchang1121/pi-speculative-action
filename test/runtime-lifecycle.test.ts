import { expect, test } from "vitest";
import { RuntimeLifecycleLane } from "../src/runtime-lifecycle.ts";
import { SpeculationScheduler, type ExecutionResourceSnapshot } from "../src/system-resources.ts";
import { deferred, nextTurn } from "./async.ts";

test("closing drains admitted resource acquisition and nested releases exactly once", async () => {
	const lifetime = new RuntimeLifecycleLane();
	const acquired = deferred();
	const cleanup = deferred();
	const events: string[] = [];
	const resource = {
		dispose: async () => {
			events.push("release");
			lifetime.own({
				dispose: async () => {
					await cleanup.promise;
					events.push("nested");
				},
			});
			throw new Error("cleanup failure");
		},
	};
	const operation = lifetime.admit(async () => {
		await acquired.promise;
		lifetime.own(resource);
		lifetime.own(resource);
		events.push("acquired");
	});
	const close = lifetime.dispose(() => {
		events.push("closed");
	});
	expect(lifetime.dispose()).toBe(close);
	await expect(lifetime.admit(async () => {})).rejects.toThrow("closed");
	acquired.resolve();
	await operation;
	await nextTurn();
	expect(events).toEqual(["acquired", "release"]);
	cleanup.resolve();
	await close;
	await lifetime.release(resource);
	expect(events).toEqual(["acquired", "release", "nested", "closed"]);
});

test("closing a scheduler while its resource sample is in flight discards the sample", async () => {
	const started = deferred();
	const result = deferred<ExecutionResourceSnapshot>();
	let changes = 0;
	const scheduler = new SpeculationScheduler({
		resources: {
			initial: { cpuCount: 1 },
			sample: () => {
				started.resolve();
				return result.promise;
			},
		},
		active: () => true,
		changed: () => changes++,
	});
	try {
		scheduler.watch(0);
		await started.promise;
		scheduler.close();
		result.resolve({ cpuCount: 2 });
		await nextTurn();
		expect(changes).toBe(0);
	} finally {
		scheduler.close();
	}
});
