import { describe, expect, it, vi } from "vitest";
import {
	effectCommitFailure,
	type EffectTransaction,
	EffectTransactionCoordinator,
} from "../src/effect-transaction.ts";
import type { SpeculativeExecutionRoute, WorldBranch } from "../src/execution-world.ts";
import { buildPiActionKey } from "../src/action-semantics.ts";

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

	it("retires resources only after admitted validation, reconstruction and commit finish", async () => {
		for (const phase of ["reconstruction", "validation", "committing", "committed"] as const) for (const fails of [false, true]) {
			let release!: () => void, enter!: () => void;
			const gate = new Promise<void>((resolve) => { release = resolve; });
			const entered = new Promise<void>((resolve) => { enter = resolve; });
			const failure = new Error("borrow failed"), dispose = vi.fn();
			const borrow = async () => { enter(); await gate; expect(dispose).not.toHaveBeenCalled(); if (fails) throw failure; };
			const coordinator = new EffectTransactionCoordinator<string>();
			const transaction = await coordinator.execute(coordinator.begin({ tool: "read", route: { ...route, reuse: "shared_result" } }), async () => branch({
				validate: async () => { if (phase === "validation") await borrow(); return { status: "valid", metrics: metrics() }; },
				reconstruct: async () => { await borrow(); return "rebuilt"; },
				commit: async () => { if (phase === "committing") await borrow(); return "committed"; }, dispose,
			}));
			const request = { action: buildPiActionKey("read", { path: "notes" }, "/workspace")!, args: {}, callID: "actor", signal: new AbortController().signal };
			if (phase !== "validation") await transaction.validate();
			if (phase === "committed") await transaction.commit();
			const operations = Promise.allSettled(phase === "validation" ? [transaction.validate()] : phase === "committing" ? [transaction.commit()]
				: [transaction.reconstruct!(request), transaction.reconstruct!(request)]);
			await entered;
			const aborts = Promise.all([transaction.abort(), transaction.abort()]);
			const late = Promise.allSettled([transaction.validate(), transaction.reconstruct!(request)]);
			try {
				await new Promise<void>((resolve) => setImmediate(resolve));
				expect(dispose).not.toHaveBeenCalled();
			} finally { release(); await operations; await aborts; }
			for (const result of await operations) expect(result.status).toBe(fails && phase !== "validation" ? "rejected" : "fulfilled");
			expect(await late).toMatchObject([{ status: "fulfilled", value: { status: "indeterminate" } }, { status: "fulfilled", value: undefined }]);
			expect(dispose).toHaveBeenCalledOnce();
			expect(transaction.state).toBe(phase === "committed" || (phase === "committing" && !fails) ? "committed" : phase === "committing" ? "poisoned" : "aborted");
			expect(await transaction.validate()).toMatchObject({ status: "indeterminate" });
			expect(await transaction.reconstruct!(request)).toBeUndefined();
			if (phase === "committed" || (phase === "committing" && !fails)) await expect(transaction.commit()).resolves.toBe("committed");
			else await expect(transaction.commit()).rejects.toMatchObject({ disposition: phase === "committing" ? "poisoned" : "recoverable" });
		}
	});

	it.each(["stale", "missing", "throws"])("requires a backend proof for shared results (%s)", async (proof) => {
		const disposeBranch = vi.fn();
		const disposeCapture = vi.fn();
		const coordinator = new EffectTransactionCoordinator<string>();
		const capturedAttempt = coordinator.begin({ tool: "custom", route: { ...route, reuse: "shared_result" } });
		const capture = coordinator.capture(capturedAttempt, {
			seal: async (output) =>
				branch({
					output,
					validate: proof === "missing" ? undefined : async () => {
						if (proof === "throws") throw new Error("no evidence");
						return { status: "stale", cause: { stage: "freshness", code: "changed" }, metrics: metrics() };
					},
					dispose: disposeBranch,
				}),
			dispose: disposeCapture,
		});
		const transaction = (await capture.seal("actor-output")) as EffectTransaction<string>;

		expect(await transaction.validate()).toMatchObject({ status: proof === "stale" ? "stale" : "indeterminate",
			cause: { code: proof === "stale" ? "changed" : proof === "missing" ? "validation_unavailable" : "validation_failed" } });
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
