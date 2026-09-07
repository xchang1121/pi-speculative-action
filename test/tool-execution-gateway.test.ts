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
	it("owns speculative routing, capture, execution, and world lifecycle", async () => {
		const dispose = vi.fn(async () => {});
		const world: TestWorld = {
			id: "workspace",
			scope: "fallback",
			isolation: "workspace_branch",
			speculation: {
				capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
				execute: async ({ value }) => branch("workspace", value),
			},
			observation: {
				capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
				capture: async ({ value }) => ({
					seal: async (output) => branch("workspace", `${value}:${output}`),
					dispose: () => {},
				}),
			},
			dispose,
		};
		const gateway = new ToolExecutionGateway([world]);
		const operation = { tool: "custom_process", callID: "spec-1", input: { any: "shape" } };
		const requirement = {
			operation,
			effect: "workspace_mutation" as const,
			requirements: WORKSPACE_PATH_MUTATION_EFFECTS,
		};
		const route = await gateway.resolve(requirement, { cwd: "/workspace" });

		expect(route).toMatchObject({ backend: "workspace", reuse: "exclusive_branch" });
		const transaction = route
			? await gateway.executeSpeculative(operation, route, () => ({ value: "sealed" }))
			: undefined;
		expect(transaction?.output).toBe("sealed");
		expect(transaction?.state).toBe("sealed");
		const capture = await gateway.captureAuthoritativeResult(
			requirement,
			{ cwd: "/workspace" },
			() => ({ value: "baseline" }),
		);
		expect((await capture?.capture.seal("actor"))?.output).toBe("baseline:actor");
		expect(
			await gateway.resolve(
				{ operation, effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS },
				{ cwd: "/workspace" },
			),
		).toBeUndefined();
		await gateway.dispose();
		expect(dispose).toHaveBeenCalledOnce();
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
	return {
		output,
		backend,
		resources: [],
		capturedBytes: 0,
		executionMetrics: {},
		compatibility: { status: "compatible" as const, backend, executionFingerprint: `${backend}:v1` },
		commit: async () => output,
		dispose: () => {},
	};
}
