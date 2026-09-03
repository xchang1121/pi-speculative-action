import { describe, expect, it, vi } from "vitest";
import {
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ExecutionWorld } from "../src/execution-world.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { ToolExecutionGateway, type ToolOperation } from "../src/tool-execution-gateway.ts";

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

	it("runs authoritative operations through the same source-neutral boundary", async () => {
		const gateway = new ToolExecutionGateway<TestContext, string>([]);
		const operation = { tool: "third_party_tool", callID: "actor-1", input: { value: 42 } };
		const executor = vi.fn(async (received: ToolOperation) =>
			(received.input as { readonly value: number }).value,
		);

		expect(await gateway.executeAuthoritative(operation, executor)).toBe(42);
		expect(executor).toHaveBeenCalledOnce();
		expect(executor).toHaveBeenCalledWith(operation);
	});

	it("owns reuse, Actor fallback timing, and failure-isolated observation", async () => {
		const gateway = new ToolExecutionGateway<TestContext, string>([]);
		const operation = { tool: "read", callID: "actor", input: { path: "file" } };
		const executor = vi.fn(async () => 42);
		const settled = vi.fn(async () => {
			throw new Error("observer failed");
		});

		expect(
			await gateway.executeAuthoritative(operation, executor, {
				reuse: async () => {
					throw new Error("cache failed");
				},
				settled,
			}),
		).toBe(42);
		expect(executor).toHaveBeenCalledOnce();
		expect(settled).toHaveBeenCalledWith(
			expect.objectContaining({ status: "succeeded", output: 42, durationMs: expect.any(Number) }),
		);

		executor.mockClear();
		settled.mockClear();
		expect(
			await gateway.executeAuthoritative(operation, executor, { reuse: async () => 7, settled }),
		).toBe(7);
		expect(executor).not.toHaveBeenCalled();
		expect(settled).not.toHaveBeenCalled();
	});

	it("reports the original Actor error without letting observation replace it", async () => {
		const gateway = new ToolExecutionGateway<TestContext, string>([]);
		const failure = new Error("actor failed");
		const observer = vi.fn(async () => {
			throw new Error("observer failed");
		});

		await expect(
			gateway.executeAuthoritative(
				{ tool: "bash", input: "exit 1" },
				async () => {
					throw failure;
				},
				{ settled: observer },
			),
		).rejects.toBe(failure);
		expect(observer).toHaveBeenCalledWith(
			expect.objectContaining({ status: "failed", error: failure, durationMs: expect.any(Number) }),
		);
	});

	it("never starts the Actor after an indeterminate reuse commit", async () => {
		const gateway = new ToolExecutionGateway<TestContext, string>([]);
		const executor = vi.fn(async () => "actor");
		const poisoned = effectCommitFailure(new Error("rollback failed"), "poisoned");

		await expect(
			gateway.executeAuthoritative({ tool: "write", input: {} }, executor, {
				reuse: async () => Promise.reject(poisoned),
			}),
		).rejects.toBe(poisoned);
		expect(executor).not.toHaveBeenCalled();
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
