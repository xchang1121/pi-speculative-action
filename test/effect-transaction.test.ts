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
	it("enforces validate-before-commit and joins an at-most-once commit", async () => {
		const commit = vi.fn(async () => "committed");
		const dispose = vi.fn(async () => {});
		const coordinator = new EffectTransactionCoordinator<string>();
		const attempt = coordinator.begin({ tool: "arbitrary", callID: "call-1", route });
		expect(attempt.state).toBe("begun");

		const transaction = await coordinator.execute(attempt, async () =>
			branch({
				validate: async () => ({ status: "valid", metrics: metrics() }),
				commit,
				dispose,
			}),
		);
		expect(transaction.state).toBe("sealed");
		expect(attempt.state).toBe(transaction.state);
		await expect(transaction.commit()).rejects.toThrow("requires successful validation");
		expect(await transaction.validate()).toMatchObject({ status: "valid" });
		expect(transaction.state).toBe("validated");
		expect(attempt.state).toBe(transaction.state);

		const [first, second] = await Promise.all([transaction.commit(), transaction.commit()]);
		expect([first, second]).toEqual(["committed", "committed"]);
		expect(commit).toHaveBeenCalledOnce();
		expect(transaction.state).toBe("committed");
		expect(attempt.state).toBe(transaction.state);
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
		expect(transaction.state).toBe("validating");
		expect(attempt.state).toBe(transaction.state);
		const aborted = transaction.abort();
		expect(transaction.state).toBe("aborting");
		releaseValidation?.();
		await validation;
		await aborted;

		expect(transaction.state).toBe("aborted");
		expect(attempt.state).toBe(transaction.state);
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

	it("distinguishes restored failures from indeterminate partial commits", async () => {
		for (const disposition of ["recoverable", "poisoned"] as const) {
			const dispose = vi.fn();
			const failure = effectCommitFailure(new Error("commit failed"), disposition);
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

			await expect(transaction.commit()).rejects.toBe(failure);
			expect(transaction.state).toBe(disposition === "poisoned" ? "poisoned" : "failed");
			await transaction.abort();
			expect(transaction.state).toBe(disposition === "poisoned" ? "poisoned" : "aborted");
			expect(dispose).toHaveBeenCalledOnce();
		}
	});

	it("treats an unclassified backend commit failure as poisoned", async () => {
		const coordinator = new EffectTransactionCoordinator<string>();
		const transaction = await coordinator.execute(
			coordinator.begin({ tool: "custom", route }),
			async () =>
				branch({
					validate: async () => ({ status: "valid", metrics: metrics() }),
					commit: async () => Promise.reject(new Error("unknown state")),
				}),
		);
		await transaction.validate();

		await expect(transaction.commit()).rejects.toMatchObject({ disposition: "poisoned" });
		expect(transaction.state).toBe("poisoned");
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
