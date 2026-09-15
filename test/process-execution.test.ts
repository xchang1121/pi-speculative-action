import { gated, deferred as barrier } from "./async.ts";
import { describe, expect, test, vi } from "vitest";
import { ProcessExecutionCoordinator, type PreparedProcessExecutionRoute, type ProcessExecutor } from "../src/process-execution.ts";

describe("ProcessExecutionCoordinator", () => {
	test.each(["preparing", "executing", "rejected", "thrown"] as const)("owns route retirement while %s", async (phase) => {
		for (const dispose of [false, true]) for (const warm of [false, true]) {
			const calls: string[] = [], prepared = barrier<PreparedProcessExecutionRoute>(), probing = barrier();
			const executing = barrier(), finish = barrier(), resetGate = gated();
			let enabled = false, retired = false, active = 0;
			const executor = (label: string): ProcessExecutor => ({ execute: async (request) => {
				calls.push(`${label}:${request.command}`);
				if (["first", "second"].includes(request.command)) {
					expect(request.scope).toEqual({ sessionID: "session", turnID: request.command });
					expect(Object.isFrozen(request.scope)).toBe(true);
				}
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
				return phase === "rejected" ? Promise.reject(new Error("helper unavailable")) : (async () => {
					enabled = false;
					try { await invoke("probe"); } finally { enabled = true; }
					return prepared.promise;
				})();
			});
			const reset = vi.fn(async () => { retired = true; await resetGate.wait(); });
			const coordinator = new ProcessExecutionCoordinator(executor("raw"), { enabled: () => enabled, prepare, reset });
			const invoke = (command: string) => coordinator.operations.exec(command, "/work", { onData: () => {}, env: { PATH: "/bin" } });
			expect(coordinator.actorDiagnostics().state).toBe("disabled");
			await invoke("disabled");
			await coordinator.runWith(executor("world"), async () => {
				await Promise.resolve(); await invoke("scoped");
				expect(prepare).not.toHaveBeenCalled();
				enabled = true;
				expect(coordinator.actorDiagnostics().state).toBe("idle");
				if (warm) expect(await coordinator.runWith(executor("nested"), async () => {
					await Promise.resolve(); return invoke("prediction");
				})).toEqual({ exitCode: 0 }); // Prediction completes while Actor preparation is still held.
			});
			if (warm) expect(prepare).toHaveBeenCalledOnce();
			const delayed = barrier(), logicalScope = { sessionID: "session", turnID: "first" };
			const first = coordinator.runActor(logicalScope, async () => { await delayed.promise; return invoke("first"); });
			logicalScope.turnID = "second";
			const second = coordinator.runActor(logicalScope, async () => { await delayed.promise; return invoke("second"); });
			logicalScope.turnID = "later"; delayed.resolve();
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
			expect(prepare).toHaveBeenCalledWith(warm);
			const retire = () => dispose ? coordinator.dispose() : coordinator.refreshActorRoute();
			const retiredCalls = Promise.allSettled([retire(), retire()]);
			const during = Promise.allSettled([invoke("during")]);
			await coordinator.runWith(executor("world"), async () => invoke("retiring"));
			expect(prepare).toHaveBeenCalledOnce();
			prepared.resolve({ state: "ready", detail: "ready", executor: executor("reuse") });
			finish.resolve();
			await resetGate.entered;
			prepare.mockResolvedValue({ state: "degraded", detail: "fresh", executor: executor("fresh") });
			resetGate.release();
			expect(await started).toEqual([{ status: "fulfilled", value: { exitCode: 0 } }, { status: "fulfilled", value: { exitCode: 0 } }]);
			expect((await during)[0]?.status).toBe("fulfilled");
			expect((await retiredCalls).every((result) => result.status === "fulfilled")).toBe(true);
			expect(reset).toHaveBeenCalledOnce();
			expect(coordinator.actorDiagnostics().state).toBe(dispose ? "unavailable" : "degraded");
			if (!dispose) expect(prepare).toHaveBeenLastCalledWith(true);
			await invoke("after");
			const preparations = prepare.mock.calls.length;
			await coordinator.runWith(executor("world"), async () => invoke("after-prediction"));
			expect(prepare).toHaveBeenCalledTimes(preparations);
			expect(calls).toContain("raw:during");
			expect(calls.slice(0, 2)).toEqual(["raw:disabled", "world:scoped"]);
			if (warm) expect(calls).toContain("nested:prediction");
			if (phase === "preparing" || phase === "executing") expect(calls.filter(call => call.endsWith(":probe"))).toEqual(["raw:probe"]);
			for (const command of ["first", "second"]) expect(calls.filter((call) => call.endsWith(`:${command}`)))
				.toEqual([`${phase === "executing" ? "reuse" : "raw"}:${command}`]);
			expect(calls.slice(-2)).toEqual([`${dispose ? "raw" : "fresh"}:after`, "world:after-prediction"]);
			await coordinator.dispose();
		}
	});
});
