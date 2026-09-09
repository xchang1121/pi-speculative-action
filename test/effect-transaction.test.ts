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
	it.each(["settled", "pending", "revalidate"])("owns concurrent commit across validation=%s", async (phase) => {
		for (const disposition of ["success", "recoverable", "poisoned", undefined] as const) {
			let release!: () => void;
			const gate = new Promise<void>((resolve) => { release = resolve; });
			const failure = disposition === "success" ? undefined : disposition ? effectCommitFailure(new Error("commit failed"), disposition) : new Error("unknown state");
			const commit = vi.fn(async () => { if (failure) throw failure; return "committed"; }), dispose = vi.fn();
			const coordinator = new EffectTransactionCoordinator<string>();
			const attempt = coordinator.begin({ tool: "arbitrary", callID: "call-1", route });
			expect(attempt.state).toBe("begun");
			expect("stateValue" in attempt).toBe(false); expect(Reflect.set(attempt, "state", "validated")).toBe(false);
			for (const unowned of [{ ...attempt }, new EffectTransactionCoordinator<string>().begin(attempt.descriptor)])
				await expect(coordinator.execute(unowned, async () => branch())).rejects.toThrow("another coordinator");
			const source = branch({
				validate: async () => { await gate; return { status: "valid", metrics: metrics() }; },
				commit, dispose,
			});
			const transaction = await coordinator.execute(attempt, async () => source);
			Object.assign(source, { output: "replaced after sealing", commit: vi.fn(), dispose: vi.fn() });
			expect(transaction.output).toBe("sealed");
			expect([transaction.state, attempt.state]).toEqual(["sealed", "sealed"]);
			await expect(transaction.commit()).rejects.toThrow("requires successful validation");
			const validation = transaction.validate();
			if (phase !== "pending") { release(); await validation; }
			const commits = Promise.allSettled([transaction.commit(), transaction.commit()]);
			if (phase === "revalidate") expect((await transaction.validate()).status).toBe(failure ? "indeterminate" : "valid");
			release();
			const [first, second] = await commits;
			expect(first).toEqual(second);
			expect(first).toMatchObject(failure ? { status: "rejected", reason: { disposition: disposition ?? "poisoned" } } : { status: "fulfilled", value: "committed" });
			const state = !failure ? "committed" : disposition === "recoverable" ? "failed" : "poisoned";
			expect([transaction.state, attempt.state]).toEqual([state, state]); expect(commit).toHaveBeenCalledOnce();
			await transaction.abort(); expect(dispose).toHaveBeenCalledOnce();
			expect(transaction.state).toBe(state === "failed" ? "aborted" : state);
		}
	});

	it.each(["external", "callback"])("retires resources after admitted operations finish (close=%s)", async (closing) => {
		for (const phase of ["reconstruction", "validation", "committing", "committed"] as const) for (const fails of [false, true]) {
			let release!: () => void, enter!: () => void;
			const gate = new Promise<void>((resolve) => { release = resolve; });
			const entered = new Promise<void>((resolve) => { enter = resolve; });
			const failure = new Error("borrow failed"), dispose = vi.fn();
			const borrow = async () => {
				if (closing === "callback") void transaction.abort();
				enter(); await gate; expect(dispose).not.toHaveBeenCalled(); if (fails) throw failure;
			};
			const coordinator = new EffectTransactionCoordinator<string>();
			const transaction = await coordinator.execute(coordinator.begin({ tool: "read", route: { ...route, reuse: "shared_result" } }), async () => branch({
				validate: async () => { if (phase === "validation") await borrow(); return { status: "valid", metrics: metrics() }; },
				reconstruct: async () => { await borrow(); return "rebuilt"; },
				commit: async () => { if (phase === "committing") await borrow(); return "committed"; }, dispose,
			}));
			const request = { action: buildPiActionKey("read", { path: "notes" }, "/workspace")!, args: {}, callID: "actor", signal: new AbortController().signal };
			if (phase === "committing" || phase === "committed") await transaction.validate();
			if (phase === "committed") await transaction.commit();
			const invoke = () => phase === "validation" ? transaction.validate() : phase === "committing" ? transaction.commit() : transaction.reconstruct!(request);
			const operations = Promise.allSettled(Array.from({ length: closing === "external" && ["reconstruction", "committed"].includes(phase) ? 2 : 1 }, invoke));
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
			if (phase === "committed" || (phase === "committing" && !fails)) await expect(transaction.commit()).resolves.toBe("sealed");
			else await expect(transaction.commit()).rejects.toMatchObject({ disposition: phase === "committing" ? "poisoned" : "recoverable" });
		}
	});

	it.each(["stale", "missing", "throws"])("requires a backend proof for shared results (%s)", async (proof) => {
		const disposeBranch = vi.fn();
		const disposeCapture = vi.fn();
		const coordinator = new EffectTransactionCoordinator<string>();
		const offeredRoute = { ...route, reuse: "shared_result" as SpeculativeExecutionRoute["reuse"] };
		const capturedAttempt = coordinator.begin({ tool: "custom", route: offeredRoute });
		offeredRoute.reuse = "exclusive_branch"; offeredRoute.fingerprint = "changed after begin";
		const offeredProof = { status: "stale" as const, cause: { stage: "freshness" as const, code: "changed" }, metrics: metrics() };
		const offeredBranch = branch({
			validate: proof === "missing" ? undefined : async () => {
				if (proof === "throws") throw new Error("no evidence");
				return offeredProof;
			}, dispose: disposeBranch,
		});
		const capture = coordinator.capture(capturedAttempt, {
			seal: async (output) => Object.assign(offeredBranch, { output }),
			dispose: disposeCapture,
		});
		const transaction = (await capture.seal("actor-output")) as EffectTransaction<string>;
		Object.assign(offeredBranch, { validate: async () => ({ status: "valid", metrics: metrics() }) });

		const validation = await transaction.validate();
		Object.assign(offeredProof, { status: "valid" }); offeredProof.metrics.durationMs = 99;
		expect(Reflect.set(validation, "status", "valid")).toBe(false); expect(Reflect.set(validation.metrics, "durationMs", 99)).toBe(false);
		expect(validation).toMatchObject({ status: proof === "stale" ? "stale" : "indeterminate",
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

	it("owns sealed data and operation slots without changing opaque backend or Actor owners", async () => {
		const getter = vi.fn(() => "not data"), opaque = Object.create({ method() {} });
		const metadataKey = Symbol("evidence");
		const data = () => {
			const value = ["sealed"], sparse = new Array<unknown>(3);
			const details = { value, [metadataKey]: value, sparse, ["__proto__"]: { literal: true } };
			sparse[1] = details;
			return details;
		};
		for (const captured of [false, true]) for (const details of [data(), opaque, new Date(), Buffer.from("raw"), new Proxy({ payload: 1 }, {}),
			{ method() {} }, { value: Symbol("opaque") }, { [metadataKey]: opaque },
			Object.defineProperty({}, metadataKey, { value: 1 }), Object.defineProperty({}, metadataKey, { get: getter, enumerable: true }),
			Object.defineProperty({}, "hidden", { value: 1 }), { get value() { return getter(); } }]) {
			const output = { content: ["sealed"], details }, expected = { content: ["sealed"], details: data() };
			const shareable = "value" in details && Array.isArray(Object.getOwnPropertyDescriptor(details, "value")?.value);
			const coordinator = new EffectTransactionCoordinator<typeof output>();
			const attempt = coordinator.begin({ tool: "custom", route: { ...route, reuse: "shared_result" } });
			const checkpoint = { backend: "test", id: "sealed", lineage: "root", depth: 1, handle() {} };
			const metadata = { backend: "test", resources: ["sealed.txt"], capturedBytes: 1, executionMetrics: { setupMs: 1 },
				compatibility: { status: "incompatible" as const, backend: "test", code: "sealed_incompatible" } };
			const commit = vi.fn(async function (this: WorldBranch<typeof output>) { expect(this).toBe(source); return output; });
			const dispose = vi.fn(function (this: WorldBranch<typeof output>) { expect(this).toBe(source); });
			const source: WorldBranch<typeof output> = { ...metadata, checkpoint, output, commit, dispose,
				reconstruct: async function () { expect(this).toBe(source); return expected; },
				validate: async function () { expect(this).toBe(source); return { status: "valid", metrics: metrics() }; } };
			const pending = captured ? coordinator.capture(attempt, { seal: () => source, dispose: () => {} }).seal(output)
				: coordinator.execute(attempt, async () => source);
			if (!shareable) {
				await expect(pending).rejects.toThrow("shared_output_not_data");
				expect(attempt.state).toBe("failed"); expect(dispose).toHaveBeenCalledOnce(); expect(commit).not.toHaveBeenCalled();
				expect(source.output).toBe(output); expect(Object.isFrozen(details)).toBe(false);
				continue;
			}
			const transaction = await pending;
			const sealedMetadata = structuredClone(metadata), replaced = vi.fn();
			metadata.resources.push("late.txt"); metadata.executionMetrics.setupMs = 99;
			Object.assign(metadata.compatibility, { status: "compatible", executionFingerprint: "late" });
			Object.assign(source, { backend: "late", checkpoint: undefined, capturedBytes: 99, resources: [], executionMetrics: {},
				compatibility: { status: "compatible", backend: "late", executionFingerprint: "late" },
				validate: replaced, reconstruct: replaced, commit: replaced, dispose: replaced });
			expect(transaction).toMatchObject(sealedMetadata); expect(transaction.checkpoint).toBe(checkpoint);
			expect(Object.isFrozen(checkpoint)).toBe(false);
			for (const [owner, key] of [[transaction, "commit"], [transaction.resources, "0"], [transaction.executionMetrics, "setupMs"],
				[transaction.compatibility, "status"]] as const) expect(Reflect.set(owner, key, "changed")).toBe(false);
			output.content.push("provider edit"); (details as { value: string[] }).value.push("provider edit");
			const borrowed = transaction.output;
			expect(borrowed.details[metadataKey]).toBe(borrowed.details.value);
			expect(borrowed.details.sparse[1]).toBe(borrowed.details);
			expect(0 in borrowed.details.sparse).toBe(false);
			expect(Object.getPrototypeOf(borrowed.details)).toBe(Object.prototype);
			borrowed.content.push("reader edit"); borrowed.details[metadataKey].push("reader edit");
			expect(transaction.output).toEqual(expected);
			expect(await transaction.reconstruct!({ action: buildPiActionKey("read", { path: "sealed.txt" }, "/workspace")!,
				args: {}, callID: "actor", signal: new AbortController().signal })).toEqual(expected);
			await expect(transaction.commit()).rejects.toThrow("requires successful validation");
			await transaction.validate!();
			const [first, second] = await Promise.all([transaction.commit(), transaction.commit()]);
			Object.assign(source, { commitMetrics: { durationMs: 2, validationMs: 1, bytesValidated: 1, resourcesValidated: 1, resourcesCommitted: 1 } });
			expect(transaction.commitMetrics).toMatchObject({ resourcesCommitted: 1 });
			first.content.push("Actor edit"); (first.details as { value: string[] }).value.push("Actor edit");
			expect(second).toEqual(expected); expect(commit).toHaveBeenCalledOnce();
			await transaction.dispose(); expect(dispose).toHaveBeenCalledOnce(); expect(replaced).not.toHaveBeenCalled();
		}
		expect(getter).not.toHaveBeenCalled();
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
