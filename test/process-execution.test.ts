import { describe, expect, test, vi } from "vitest";
import {
	ProcessExecutionCoordinator,
	type PreparedProcessExecutionRoute,
	type ProcessExecutor,
} from "../src/process-execution.ts";

describe("ProcessExecutionCoordinator", () => {
	const executor = (label: string, calls: string[]): ProcessExecutor => ({
		execute: async (request) => {
			calls.push(`${label}:${request.command}`);
			return { exitCode: 0 };
		},
	});
	const invoke = (coordinator: ProcessExecutionCoordinator, command = "true") =>
		coordinator.operations.exec(command, "/work", { onData: () => undefined, env: { PATH: "/bin" } });

	test("routes only the dynamic execution scope and restores the host outlet", async () => {
		const calls: string[] = [];
		const coordinator = new ProcessExecutionCoordinator(executor("host", calls));

		await invoke(coordinator, "outside-before");
		await coordinator.runWith(executor("isolated", calls), async () => {
			await Promise.resolve();
			await invoke(coordinator, "inside");
		});
		await invoke(coordinator, "outside-after");

		expect(calls).toEqual(["host:outside-before", "isolated:inside", "host:outside-after"]);
	});

	test("owns disabled, lazy, shared preparation, refresh, and disposal states", async () => {
		const calls: string[] = [];
		const pending = deferred<PreparedProcessExecutionRoute>();
		let enabled = false;
		const prepare = vi.fn(() => pending.promise);
		const reset = vi.fn(async () => undefined);
		const coordinator = new ProcessExecutionCoordinator(executor("raw", calls), {
			enabled: () => enabled,
			prepare,
			reset,
		});

		expect(coordinator.actorDiagnostics().state).toBe("disabled");
		await invoke(coordinator, "disabled");
		expect(prepare).not.toHaveBeenCalled();
		enabled = true;
		expect(coordinator.actorDiagnostics().state).toBe("idle");
		const first = invoke(coordinator, "first");
		const second = invoke(coordinator, "second");
		await vi.waitFor(() => expect(coordinator.actorDiagnostics().state).toBe("probing"));
		expect(prepare).toHaveBeenCalledTimes(1);
		pending.resolve({ state: "ready", detail: "ready", executor: executor("reuse", calls) });
		await Promise.all([first, second]);
		expect(coordinator.actorDiagnostics().state).toBe("ready");
		await coordinator.runWith(executor("world", calls), () => invoke(coordinator, "scoped"));

		enabled = false;
		await coordinator.refreshActorRoute();
		expect(coordinator.actorDiagnostics().state).toBe("disabled");
		prepare.mockResolvedValueOnce({ state: "degraded", detail: "whole calls only", executor: executor("limited", calls) });
		enabled = true;
		await coordinator.refreshActorRoute();
		expect(coordinator.actorDiagnostics().state).toBe("degraded");
		await invoke(coordinator, "refreshed");
		await coordinator.dispose();
		expect(coordinator.actorDiagnostics().state).toBe("unavailable");
		expect(reset).toHaveBeenCalledTimes(3);
		expect(calls).toEqual(["raw:disabled", "reuse:first", "reuse:second", "world:scoped", "limited:refreshed"]);
	});

	test("falls back exactly once when lazy preparation fails", async () => {
		const calls: string[] = [];
		const prepare = vi.fn(async () => { throw new Error("helper unavailable"); });
		const coordinator = new ProcessExecutionCoordinator(executor("raw", calls), {
			enabled: () => true,
			prepare,
		});

		await invoke(coordinator, "actor");
		await invoke(coordinator, "later");

		expect(prepare).toHaveBeenCalledTimes(1);
		expect(coordinator.actorDiagnostics()).toEqual({ state: "unavailable", detail: "helper unavailable" });
		expect(calls).toEqual(["raw:actor", "raw:later"]);
	});
});

function deferred<Value>() {
	let resolve!: (value: Value) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<Value>((accept, fail) => {
		resolve = accept;
		reject = fail;
	});
	return { promise, resolve, reject };
}
