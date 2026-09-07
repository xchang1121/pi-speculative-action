import { describe, expect, it, vi } from "vitest";
import {
	effectCommitFailure,
	type EffectTransaction,
	EffectTransactionCoordinator,
} from "../src/effect-transaction.ts";
import type { SpeculativeExecutionRoute, WorldBranch } from "../src/execution-world.ts";

const route: SpeculativeExecutionRoute = {
	isolation: "runtime_sandbox",
	reuse: "exclusive_branch",
	scope: "runtime",
	backend: "test",
	fingerprint: "test:v1",
};

describe("EffectTransactionCoordinator", () => {
	it.each([false, true])("owns concurrent commit across pending validation=%s", async (pending) => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => { release = resolve; });
		const commit = vi.fn(async () => "committed");
		const dispose = vi.fn(async () => {});
		const coordinator = new EffectTransactionCoordinator<string>();
		const attempt = coordinator.begin({ tool: "arbitrary", callID: "call-1", route });
		expect(attempt.state).toBe("begun");
		for (const unowned of [{ ...attempt }, new EffectTransactionCoordinator<string>().begin(attempt.descriptor)])
			await expect(coordinator.execute(unowned, async () => branch())).rejects.toThrow("another coordinator");

		const transaction = await coordinator.execute(attempt, async () =>
			branch({
				validate: async () => { await gate; return { status: "valid", metrics: metrics() }; },
				commit,
				dispose,
			}),
		);
		expect([transaction.state, attempt.state]).toEqual(["sealed", "sealed"]);
		await expect(transaction.commit()).rejects.toThrow("requires successful validation");
		const validation = transaction.validate();
		if (!pending) { release(); await validation; }
		const commits = [transaction.commit(), transaction.commit()];
		release();
		const [first, second] = await Promise.all(commits);
		expect([first, second]).toEqual(["committed", "committed"]);
		expect(commit).toHaveBeenCalledOnce();
		expect([transaction.state, attempt.state]).toEqual(["committed", "committed"]);
		await transaction.abort();
		expect(dispose).toHaveBeenCalledOnce();
		expect(transaction.state).toBe("committed");
	});

	it("coordinates validation and abort through one lifecycle", async () => {
		let releaseValidation: (() => void) | undefined;
		const validationGate = new Promise<void>((resolve) => {
			releaseValidation = resolve;
		});
		const dispose = vi.fn();
		const coordinator = new EffectTransactionCoordinator<string>();
		const attempt = coordinator.begin({ tool: "write", route });
		const transaction = await coordinator.execute(attempt, async () =>
			branch({
				validate: async () => {
					await validationGate;
					return { status: "valid", metrics: metrics() };
				},
				dispose,
			}),
		);

		const validation = transaction.validate();
		expect([transaction.state, attempt.state]).toEqual(["validating", "validating"]);
		const aborted = transaction.abort();
		expect(transaction.state).toBe("aborting");
		releaseValidation?.();
		await validation;
		await aborted;

		expect([transaction.state, attempt.state]).toEqual(["aborted", "aborted"]);
		expect(dispose).toHaveBeenCalledOnce();
	});

	it("fails closed on stale validation and aborts captured authoritative state", async () => {
		const disposeBranch = vi.fn();
		const disposeCapture = vi.fn();
		const coordinator = new EffectTransactionCoordinator<string>();
		const capturedAttempt = coordinator.begin({ tool: "custom", route });
		const capture = coordinator.capture(capturedAttempt, {
			seal: async (output) =>
				branch({
					output,
					validate: async () => ({
						status: "stale",
						cause: { stage: "freshness", code: "changed" },
						metrics: metrics(),
					}),
					dispose: disposeBranch,
				}),
			dispose: disposeCapture,
		});
		const transaction = (await capture.seal("actor-output")) as EffectTransaction<string>;

		expect(await transaction.validate()).toMatchObject({ status: "stale" });
		await expect(transaction.commit()).rejects.toThrow("requires successful validation");
		await transaction.abort();
		expect(disposeBranch).toHaveBeenCalledOnce();
		expect(disposeCapture).not.toHaveBeenCalled();
		expect(transaction.state).toBe("aborted");

		const abandonedAttempt = coordinator.begin({ tool: "custom", route });
		const abandoned = coordinator.capture(abandonedAttempt, {
			seal: async (output) => branch({ output }),
			dispose: disposeCapture,
		});
		await abandoned.dispose();
		expect(disposeCapture).toHaveBeenCalledOnce();
		expect(abandonedAttempt.state).toBe("aborted");
	});

	it("classifies failures once, including unclassified partial commits", async () => {
		for (const disposition of ["recoverable", "poisoned", undefined] as const) {
			const dispose = vi.fn();
			const failure = disposition ? effectCommitFailure(new Error("commit failed"), disposition) : new Error("unknown state");
			const coordinator = new EffectTransactionCoordinator<string>();
			const transaction = await coordinator.execute(
				coordinator.begin({ tool: "write", route }),
				async () =>
					branch({
						validate: async () => ({ status: "valid", metrics: metrics() }),
						commit: async () => Promise.reject(failure),
						dispose,
					}),
			);
			await transaction.validate();

			const commits = await Promise.allSettled([transaction.commit(), transaction.commit()]);
			expect(commits[0]).toEqual(commits[1]);
			expect(commits[0]).toMatchObject({ status: "rejected", reason: { disposition: disposition ?? "poisoned" } });
			expect(transaction.state).toBe(disposition === "recoverable" ? "failed" : "poisoned");
			await transaction.abort();
			expect(transaction.state).toBe(disposition === "recoverable" ? "aborted" : "poisoned");
			expect(dispose).toHaveBeenCalledOnce();
		}
	});

});

function branch(overrides: Partial<WorldBranch<string>> = {}): WorldBranch<string> {
	const output = overrides.output ?? "sealed";
	return {
		output,
		backend: "test",
		resources: [],
		capturedBytes: 0,
		executionMetrics: {},
		compatibility: { status: "compatible", backend: "test", executionFingerprint: "executor" },
		commit: async () => output,
		dispose: () => {},
		...overrides,
	};
}

function metrics() {
	return { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" as const };
}
