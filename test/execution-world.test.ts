import { describe, expect, it, vi } from "vitest";
import {
	KEYABLE_TOOLS,
	PI_ACTION_SEMANTICS,
} from "../src/action-semantics.ts";
import {
	type EffectCapabilities,
	RESOURCE_OBSERVATION_EFFECTS,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";
import type { ExecutionWorld, ExecutionWorldDiagnosticSnapshot, SpeculativeExecution } from "../src/execution-world.ts";
import {
	executionCapabilityStatus,
	ExecutionWorldRouter,
	sameSpeculativeExecutionRoute,
} from "../src/execution-world.ts";

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
		const resource = fallback("resource", "resource_snapshot", "all");
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

	it.each([
		["Windows", "Linux host required"],
		["macOS", "Linux host required"],
		["WSL 1", "required namespace unavailable"],
	] as const)("routes portable tools but blocks Bash on %s", (_platform, processDetail) => {
		const status = toolStatuses(platformWorlds("unavailable", processDetail));
		expect(status).toEqual({
			read: "ready",
			grep: "ready",
			find: "ready",
			ls: "ready",
			write: "registered",
			edit: "registered",
			bash: "unavailable",
		});
	});

	it.each(["Linux", "WSL 2"])("routes Bash when the Linux process world is ready on %s", () => {
		expect(toolStatuses(platformWorlds("ready", "process isolation ready"))).toEqual({
			read: "ready",
			grep: "ready",
			find: "ready",
			ls: "ready",
			write: "registered",
			edit: "registered",
			bash: "ready",
		});
	});

	it("lets an injected all-effect runtime make every tool routable on any host", () => {
		const worlds = [
			...platformWorlds("unavailable", "Linux host required"),
			diagnostic("host_runtime", "runtime", "runtime_sandbox", "all", "ready", "host runtime ready"),
		];
		expect(new Set(Object.values(toolStatuses(worlds)))).toEqual(new Set(["ready"]));
	});
});

function toolStatuses(worlds: readonly ExecutionWorldDiagnosticSnapshot[]): Record<string, string> {
	return Object.fromEntries(
		KEYABLE_TOOLS.map((tool) => [
			tool,
			executionCapabilityStatus(PI_ACTION_SEMANTICS.requirements(tool)!, worlds).state,
		]),
	);
}

function platformWorlds(
	processState: "ready" | "unavailable",
	processDetail: string,
): readonly ExecutionWorldDiagnosticSnapshot[] {
	return [
		diagnostic(
			"linux_process_reuse",
			"runtime",
			"runtime_sandbox",
			UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			processState,
			processDetail,
		),
		diagnostic(
			"git_worktree",
			"fallback",
			"workspace_branch",
			WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
			"registered",
			"checked on first use",
		),
		diagnostic(
			"resource_version",
			"fallback",
			"resource_snapshot",
			RESOURCE_OBSERVATION_EFFECTS.capabilities,
			"ready",
			"resource validation ready",
		),
	];
}

function diagnostic(
	id: string,
	scope: ExecutionWorldDiagnosticSnapshot["scope"],
	isolation: ExecutionWorldDiagnosticSnapshot["isolation"],
	capabilities: EffectCapabilities,
	state: ExecutionWorldDiagnosticSnapshot["state"],
	detail: string,
): ExecutionWorldDiagnosticSnapshot {
	return { id, scope, isolation, capabilities, state, detail };
}

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
