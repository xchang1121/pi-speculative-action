import { deferred, nextTurn } from "./async.ts";
import { testBranch } from "./branch.ts";
import { describe, expect, it, vi } from "vitest";
import {
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ExecutionWorld } from "../src/execution-world.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { ToolExecutionGateway, type ToolOperation, type AuthoritativeExecutionSettlement } from "../src/tool-execution-gateway.ts";

type TestContext = { readonly value: string };
type TestWorld = ExecutionWorld<TestContext, string>;

describe("ToolExecutionGateway", () => {
	it("seals one admission lifetime and drains Actor, preparation and world work before disposal", async () => {
		for (const phase of ["actor", "actor_failed", "prepare", "fork", "fork_failed", "seal_failed", "capture", "diagnostics"]) {
			let probing = false;
			const { promise: entered, resolve: enter } = deferred(), { promise: gate, resolve: release } = deferred();
			const dispose = vi.fn(), failure = new Error("admitted execution failed");
			const borrow = async () => { enter(); await gate; expect(dispose).not.toHaveBeenCalled(); if (phase.endsWith("failed")) throw failure; };
			const world: TestWorld = { id: "workspace", scope: "fallback", isolation: "workspace_branch", dispose,
				speculation: { tools: ["custom_process"], capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
					prepare: async () => { if (probing && ["prepare", "diagnostics"].includes(phase)) await borrow(); },
					execute: async ({ value }) => {
						if (phase !== "seal_failed") await borrow();
						return { ...branch("workspace", value), ...(phase === "seal_failed" ? { output: Object.create({ opaque: true }) as string, dispose: borrow } : {}) };
					} },
				observation: { capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
					capture: async () => { await borrow(); return { seal: async (output) => branch("workspace", output), dispose: () => {} }; } },
			};
			const gateway = new ToolExecutionGateway([world]), preparation = { cwd: "/workspace" };
			const operation = { tool: "custom_process", callID: "call", input: { any: "shape" } }, context = { value: "sealed" };
			const effect = phase === "seal_failed" ? "observation" as const : "workspace_mutation" as const;
			const requirement = { operation, effect, requirements: WORKSPACE_PATH_MUTATION_EFFECTS };
			const route = (await gateway.resolve(requirement, preparation))!;
			expect(route).toMatchObject({ backend: "workspace", reuse: phase === "seal_failed" ? "shared_result" : "exclusive_branch" });
			expect(await gateway.resolve({ operation, effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS }, preparation)).toBeUndefined();
			probing = true;
			const pending = phase.startsWith("actor") ? gateway.executeAuthoritative(operation, async () => { await borrow(); return "actor"; })
				: phase.startsWith("fork") || phase === "seal_failed" ? gateway.executeSpeculative(operation, route, context)
				: phase === "capture" ? gateway.captureAuthoritativeResult(requirement, preparation, context)
				: phase === "diagnostics" ? gateway.diagnostics({ ...preparation, refresh: true }) : gateway.resolve(requirement, preparation);
			const outcome = Promise.allSettled([pending]); await entered;
			const retirement = Promise.all([gateway.dispose(), gateway.dispose()]);
			try {
				await nextTurn();
				expect(dispose, phase).not.toHaveBeenCalled();
				await expect(gateway.executeAuthoritative(operation, async () => "late Actor")).rejects.toThrow("closed");
				await expect(gateway.resolve(requirement, preparation)).rejects.toThrow("closed");
				await expect(gateway.executeSpeculative(operation, route, context)).rejects.toThrow("closed");
				await expect(gateway.captureAuthoritativeResult(requirement, preparation, context)).rejects.toThrow("closed");
				await expect(gateway.diagnostics({ ...preparation, refresh: true })).rejects.toThrow("closed");
			} finally { release(); await outcome; await retirement; }
			expect((await outcome)[0]?.status).toBe(phase.endsWith("failed") ? "rejected" : "fulfilled");
			expect(dispose).toHaveBeenCalledOnce();
		}
	});

	it("settles each authoritative attempt once without replacing its executor or failure", async () => {
		const gateway = new ToolExecutionGateway<TestContext, string>([]);
		const operation = { tool: "third_party_tool", callID: "actor", input: { value: 42 } };
		const failure = new Error("Actor failure"), poisoned = effectCommitFailure(new Error("rollback failed"), "poisoned");
		const succeed = async () => 42, fail = () => { throw failure; };
		const observerFailure = new Error("Observer failure"), failObservation = () => { throw observerFailure; };
		const observers = [undefined, failObservation, async () => failObservation(), (value: AuthoritativeExecutionSettlement<number>) => {
			Object.assign(value, { status: "succeeded", output: -1, error: observerFailure });
		}];
		const cases = [
			{ name: "Actor", actor: succeed, reuse: undefined, output: 42, executions: 1 },
			{ name: "recoverable reuse", actor: succeed, reuse: async () => fail(), output: 42, executions: 1 },
			{ name: "falsy reuse hit", actor: succeed, reuse: async () => 0, output: 0, executions: 0 },
			{ name: "Actor throws", actor: fail, reuse: undefined, error: failure, executions: 1 },
			{ name: "Actor rejects", actor: async () => fail(), reuse: undefined, error: failure, executions: 1 },
			{ name: "poisoned commit", actor: succeed, reuse: async () => { throw poisoned; }, error: poisoned, executions: 0 },
		];
		for (const row of cases) for (const observe of observers) {
			const executor = vi.fn((received: ToolOperation) => {
				expect(received).toBe(operation);
				return row.actor();
			});
			const settled = observe && vi.fn(observe);
			const execution = gateway.executeAuthoritative(operation, executor, { reuse: row.reuse, settled });
			if ("error" in row) await expect(execution, row.name).rejects.toBe(row.error);
			else await expect(execution, row.name).resolves.toBe(row.output);
			expect(executor, row.name).toHaveBeenCalledTimes(row.executions);
			if (settled) expect(settled, row.name).toHaveBeenCalledTimes(row.executions);
			if (settled && row.executions) expect(settled).toHaveBeenCalledWith(expect.objectContaining({
				status: "error" in row ? "failed" : "succeeded", durationMs: expect.any(Number),
				...("error" in row ? { error: row.error } : { output: row.output }),
			}));
		}
	});
});

function branch(backend: string, output: string) {
	return testBranch(output, { backend, executionFingerprint: `${backend}:v1` });
}
