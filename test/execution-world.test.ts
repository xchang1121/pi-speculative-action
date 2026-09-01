import { describe, expect, it, vi } from "vitest";
import {
	type EffectCapabilities,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ExecutionWorld, SpeculativeExecution } from "../src/execution-world.ts";
import { ExecutionWorldRouter, sameSpeculativeExecutionRoute } from "../src/execution-world.ts";

type TestWorld = ExecutionWorld<{ readonly value: string }, string>;
const preparation = { cwd: "/workspace" };

describe("ExecutionWorldRouter", () => {
	it("uses one runtime sandbox for every effect, then exact local fallbacks, then blocks", async () => {
		const resource = fallback("resource", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const workspace = fallback("workspace", "workspace_branch", WORKSPACE_PATH_MUTATION_EFFECTS.capabilities);
		const runtimeRouter = new ExecutionWorldRouter([resource, workspace, runtime("runtime")]);
		const requests = [
			{ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS },
			{ effect: "workspace_mutation", requirements: WORKSPACE_PATH_MUTATION_EFFECTS },
			{ effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS },
		] as const;

		for (const request of requests) {
			expect(await runtimeRouter.resolve(request, preparation)).toMatchObject({
				backend: "runtime",
				isolation: "runtime_sandbox",
				reuse: request.effect === "observation" ? "shared_result" : "exclusive_branch",
			});
		}

		const localRouter = new ExecutionWorldRouter([workspace, resource]);
		expect(await localRouter.resolve(requests[0], preparation)).toMatchObject({ backend: "resource" });
		expect(await localRouter.resolve(requests[1], preparation)).toMatchObject({ backend: "workspace" });
		expect(await localRouter.resolve(requests[2], preparation)).toBeUndefined();
	});

	it("falls through unavailable worlds and exclusively owns backend lifecycle", async () => {
		const unavailable = {
			...runtime("unavailable"),
			prepare: vi.fn(async () => {
				throw new Error("unavailable");
			}),
		};
		const configureStorage = vi.fn();
		const maintainStorage = vi.fn(async () => ({ removedEntries: 2, removedArtifacts: 3, removedBytes: 4 }));
		const resource = {
			...fallback("resource", "resource_snapshot", "all"),
			storage: { configure: configureStorage, maintain: maintainStorage },
		};
		const router = new ExecutionWorldRouter([unavailable, resource, resource]);
		const route = await router.resolve(
			{ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS },
			preparation,
		);

		expect(route).toMatchObject({ backend: "resource", scope: "fallback" });
		expect(unavailable.prepare).toHaveBeenCalledOnce();
		expect(route && (await router.fork(route, { value: "captured" })).output).toBe("captured");
		expect(route && sameSpeculativeExecutionRoute(route, { ...route })).toBe(true);
		expect(route && sameSpeculativeExecutionRoute(route, { ...route, fingerprint: "changed" })).toBe(false);
		expect(await router.diagnostics(preparation)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					id: "unavailable",
					state: "unavailable",
					detail: "unavailable",
				}),
				expect.objectContaining({
					id: "resource",
					state: "ready",
					detail: "Route prepared successfully",
				}),
			]),
		);
		router.configureStorage({ maxEntries: 5, maxBytes: 6 });
		expect(configureStorage).toHaveBeenCalledWith({ maxEntries: 5, maxBytes: 6 });
		expect(await router.maintainStorage("gc")).toEqual([
			{
				id: "resource",
				operation: "gc",
				status: "completed",
				removedEntries: 2,
				removedArtifacts: 3,
				removedBytes: 4,
			},
		]);
		await router.dispose();
		expect(unavailable.dispose).toHaveBeenCalledOnce();
		expect(resource.dispose).toHaveBeenCalledOnce();

		expect(
			() => new ExecutionWorldRouter([runtime("same"), fallback("same", "resource_snapshot", "all")]),
		).toThrow("duplicate execution world same");
	});

	it("captures an authoritative result with the first explicitly capable world", async () => {
		const disposeCapture = vi.fn();
		const resourceBase = fallback("resource", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const resource: TestWorld = {
			...resourceBase,
			captureAuthoritativeResult: vi.fn(async () => ({
				seal: (output: string) => resourceBase.fork({ value: output }),
				dispose: disposeCapture,
			})),
		};
		const runtimeWithoutCapture = { ...runtime("runtime"), prepare: vi.fn(async () => {}) };
		const router = new ExecutionWorldRouter([runtimeWithoutCapture, resource]);

		const captured = await router.captureAuthoritativeResult(
			{ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS },
			preparation,
			{ value: "unused" },
		);

		expect(captured?.route).toMatchObject({ backend: "resource", reuse: "shared_result" });
		expect(runtimeWithoutCapture.prepare).not.toHaveBeenCalled();
		expect(resource.captureAuthoritativeResult).toHaveBeenCalledOnce();
		const branch = await captured?.capture.seal("actor output");
		expect(await branch?.commit()).toBe("actor output");
		expect(disposeCapture).not.toHaveBeenCalled();
	});
});

function runtime(id: string): TestWorld {
	return { ...lifecycle(id), scope: "runtime", isolation: "runtime_sandbox", capabilities: "all" };
}

function fallback(
	id: string,
	isolation: Exclude<SpeculativeExecution, "runtime_sandbox">,
	capabilities: EffectCapabilities,
): TestWorld {
	return { ...lifecycle(id), scope: "fallback", isolation, capabilities };
}

function lifecycle(id: string) {
	const dispose = vi.fn(async () => {});
	return {
		id,
		fingerprint: () => `${id}:v1`,
		fork: async ({ value }: { readonly value: string }) => ({
			output: value,
			backend: id,
			resources: [],
			capturedBytes: 0,
			executionMetrics: {},
			compatibility: { status: "compatible" as const, backend: id, executionFingerprint: "executor" },
			commit: async () => value,
			dispose: () => {},
		}),
		dispose,
	};
}
