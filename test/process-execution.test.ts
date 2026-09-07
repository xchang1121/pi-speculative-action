import { describe, expect, test, vi } from "vitest";
import { ProcessExecutionCoordinator, type PreparedProcessExecutionRoute, type ProcessExecutor } from "../src/process-execution.ts";

function barrier<Value = void>() {
	let resolve!: (value: Value) => void;
	return { promise: new Promise<Value>((done) => { resolve = done; }), resolve: (value: Value) => resolve(value) };
}

describe("ProcessExecutionCoordinator", () => {
	test.each(["preparing", "executing", "rejected", "thrown"] as const)("owns route retirement while %s", async (phase) => {
		for (const dispose of [false, true]) {
			const calls: string[] = [], prepared = barrier<PreparedProcessExecutionRoute>(), probing = barrier();
			const executing = barrier(), finish = barrier(), resetting = barrier(), close = barrier();
			let enabled = false, retired = false, active = 0;
			const executor = (label: string): ProcessExecutor => ({ execute: async (request) => {
				calls.push(`${label}:${request.command}`);
				if (label === "reuse") {
					if (++active === 2) executing.resolve();
					await finish.promise;
					expect(retired, "executor was closed beneath an admitted Actor call").toBe(false);
				}
				return { exitCode: 0 };
			} });
			const prepare = vi.fn(() => {
				probing.resolve();
				if (phase === "thrown") throw new Error("helper unavailable");
				return phase === "rejected" ? Promise.reject(new Error("helper unavailable")) : prepared.promise;
			});
			const reset = vi.fn(async () => { retired = true; resetting.resolve(); await close.promise; });
			const coordinator = new ProcessExecutionCoordinator(executor("raw"), { enabled: () => enabled, prepare, reset });
			const invoke = (command: string) => coordinator.operations.exec(command, "/work", { onData: () => {}, env: { PATH: "/bin" } });
			expect(coordinator.actorDiagnostics().state).toBe("disabled");
			await invoke("disabled");
			await coordinator.runWith(executor("world"), async () => { await Promise.resolve(); await invoke("scoped"); });
			expect(prepare).not.toHaveBeenCalled();
			enabled = true;
			expect(coordinator.actorDiagnostics().state).toBe("idle");
			const first = invoke("first"), second = invoke("second");
			const started = Promise.allSettled([first, second]); // Capture boundary exceptions without unhandled rejections.
			await probing.promise;
			if (phase === "executing") {
				prepared.resolve({ state: "ready", detail: "ready", executor: executor("reuse") });
				await executing.promise;
				expect(coordinator.actorDiagnostics().state).toBe("ready");
			} else if (phase !== "preparing") {
				await started;
				expect(coordinator.actorDiagnostics()).toEqual({ state: "unavailable", detail: "helper unavailable" });
			}
			expect(prepare).toHaveBeenCalledOnce();
			const retire = () => dispose ? coordinator.dispose() : coordinator.refreshActorRoute();
			const retiredCalls = Promise.allSettled([retire(), retire()]);
			const during = Promise.allSettled([invoke("during")]);
			prepared.resolve({ state: "ready", detail: "ready", executor: executor("reuse") });
			finish.resolve();
			await resetting.promise;
			prepare.mockResolvedValue({ state: "degraded", detail: "fresh", executor: executor("fresh") });
			close.resolve();
			expect(await started).toEqual([{ status: "fulfilled", value: { exitCode: 0 } }, { status: "fulfilled", value: { exitCode: 0 } }]);
			expect((await during)[0]?.status).toBe("fulfilled");
			expect((await retiredCalls).every((result) => result.status === "fulfilled")).toBe(true);
			expect(reset).toHaveBeenCalledOnce();
			expect(coordinator.actorDiagnostics().state).toBe(dispose ? "unavailable" : "degraded");
			await invoke("after");
			expect(calls).toContain("raw:during");
			expect(calls.slice(0, 2)).toEqual(["raw:disabled", "world:scoped"]);
			for (const command of ["first", "second"]) expect(calls.filter((call) => call.endsWith(`:${command}`)))
				.toEqual([`${phase === "executing" ? "reuse" : "raw"}:${command}`]);
			expect(calls.at(-1)).toBe(`${dispose ? "raw" : "fresh"}:after`);
			await coordinator.dispose();
		}
	});
});
