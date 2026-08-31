import { describe, expect, it, vi } from "vitest";
import type { ExecutionWorld } from "../src/execution-world.ts";
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
			supports: ({ effect }) => effect === "workspace_mutation",
			fork: async ({ value }) => branch("workspace", value),
			captureAuthoritativeResult: async ({ value }) => ({
				seal: async (output) => branch("workspace", `${value}:${output}`),
				dispose: () => {},
			}),
			dispose,
		};
		const gateway = new ToolExecutionGateway([world]);
		const operation = { tool: "custom_process", callID: "spec-1", input: { any: "shape" } };
		const requirement = { operation, effect: "workspace_mutation" as const };
		const route = await gateway.resolve(requirement, { cwd: "/workspace" });

		expect(route).toMatchObject({ backend: "workspace", reuse: "exclusive_branch" });
		expect(route && (await gateway.executeSpeculative(operation, route, () => ({ value: "sealed" }))).output).toBe(
			"sealed",
		);
		const capture = await gateway.captureAuthoritativeResult(
			requirement,
			{ cwd: "/workspace" },
			() => ({ value: "baseline" }),
		);
		expect((await capture?.capture.seal("actor"))?.output).toBe("baseline:actor");
		expect(
			await gateway.resolve({ operation, effect: "unbounded" }, { cwd: "/workspace" }),
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
});

function branch(backend: string, output: string) {
	return {
		output,
		backend,
		resources: [],
		capturedBytes: 0,
		executionMetrics: {},
		compatibility: { status: "compatible" as const, backend, executionFingerprint: `${backend}:v1` },
		state: "sealed" as const,
		commit: async () => output,
		dispose: () => {},
	};
}
