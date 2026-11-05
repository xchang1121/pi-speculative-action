import { expect, test } from "vitest";
import { RuntimeLifecycleLane } from "../src/runtime-lifecycle.ts";
import { deferred, nextTurn } from "./async.ts";

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
