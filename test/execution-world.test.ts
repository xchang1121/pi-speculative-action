import { describe, expect, it, vi } from "vitest";
import { testBranch } from "./branch.ts";
import { buildPiActionKey, KEYABLE_TOOLS, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { type EffectCapabilities, RESOURCE_OBSERVATION_EFFECTS, UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS } from "../src/effect-model.ts";
import type { ExecutionWorld, ExecutionWorldDiagnosticSnapshot, SpeculativeExecution } from "../src/execution-world.ts";
import { executionCapabilityStatus, ExecutionWorldRouter, sameSpeculativeExecutionRoute } from "../src/execution-world.ts";

type BaseTestWorld = ExecutionWorld<{ readonly value: string }, string>;
type TestWorld = BaseTestWorld & { readonly speculation: NonNullable<BaseTestWorld["speculation"]> };
const preparation = { cwd: "/workspace" };

describe("ExecutionWorldRouter", () => {
	it("admits process input observation without granting process execution or result replay", async () => {
		const request = { effect: "unbounded" as const, requirements: UNRESTRICTED_PROCESS_EFFECTS };
		let inputsOnly = false;
		const dispose = vi.fn(), base = fallback("inputs", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const router = new ExecutionWorldRouter([{ ...base, observation: { capabilities: [], inputsOnly: () => true,
			capture: async () => ({ ...(inputsOnly ? { inputsOnly: true as const } : {}), dispose,
				seal: async () => { throw new Error("unused"); } }) } }]);
		expect(await router.resolve(request, preparation)).toBeUndefined();
		expect(await router.captureAuthoritativeResult(request, preparation, { value: "actor" })).toBeUndefined();
		expect(dispose).toHaveBeenCalledOnce(); inputsOnly = true;
		expect(await router.captureAuthoritativeResult(request, preparation, { value: "actor" }))
			.toMatchObject({ route: { reuse: "shared_result" }, capture: { inputsOnly: true } });
		await router.dispose();
	});

	it("uses one runtime sandbox for every effect, then exact local fallbacks, then blocks", async () => {
		const resource = fallback("resource", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const workspace = fallback("workspace", "workspace_branch", WORKSPACE_PATH_MUTATION_EFFECTS.capabilities);
		const checkpoint = { backend: "parent", id: "parent", lineage: "parent", depth: 0 }, runtimeWorld = runtime("runtime");
		const acceptsCheckpoint = vi.fn((value: unknown, cwd: string) => value === checkpoint && cwd === preparation.cwd);
		const runtimeRouter = new ExecutionWorldRouter([resource, workspace,
			{ ...runtimeWorld, speculation: { ...runtimeWorld.speculation, acceptsCheckpoint } }]);
		const requests = [
			{ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS },
			{ effect: "workspace_mutation", requirements: WORKSPACE_PATH_MUTATION_EFFECTS },
			{ effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS },
		] as const;

		for (const request of requests) {
			const route = await runtimeRouter.resolve(request, preparation);
			expect(route).toMatchObject({
				backend: "runtime",
				isolation: "runtime_sandbox",
				reuse: request.effect === "observation" ? "shared_result" : "exclusive_branch",
			});
			expect(route?.acceptsCheckpoint?.(checkpoint)).toBe(request.effect === "observation" ? undefined : true);
			expect(structuredClone(route)).not.toHaveProperty("acceptsCheckpoint");
			expect({ ...route }).not.toHaveProperty("acceptsCheckpoint");
		}
		expect(acceptsCheckpoint).toHaveBeenCalledWith(checkpoint, preparation.cwd);
		const retained = await runtimeRouter.resolve(requests[1], preparation);
		await runtimeRouter.dispose();
		expect(retained!.acceptsCheckpoint!(checkpoint)).toBe(false);

		const localRouter = new ExecutionWorldRouter([workspace, resource]);
		expect(await localRouter.resolve(requests[0], preparation)).toMatchObject({ backend: "resource" });
		expect(await localRouter.resolve(requests[1], preparation)).toMatchObject({ backend: "workspace" });
		expect(await localRouter.resolve(requests[2], preparation)).toBeUndefined();
	});

	it("falls through unavailable worlds and exclusively owns backend lifecycle", async () => {
		const unavailableBase = runtime("unavailable");
		const unavailable = {
			...unavailableBase,
			speculation: { ...unavailableBase.speculation, prepare: vi.fn(async () => { throw new Error("unavailable"); }) },
		};
		const resource = fallback("resource", "resource_snapshot", "all");
		const router = new ExecutionWorldRouter([unavailable, resource, resource]);
		const route = await router.resolve({ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS }, preparation);

		expect(route).toMatchObject({ backend: "resource", scope: "fallback" });
		expect(unavailable.speculation.prepare).toHaveBeenCalledOnce();
		for (const backend of ["unavailable", "missing"]) expect(await router.resolve({ backend,
			effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS }, preparation)).toBeUndefined();
		expect(route && (await router.fork(route, { value: "captured" })).output).toBe("captured");
		expect(route && sameSpeculativeExecutionRoute(route, { ...route })).toBe(true);
		expect(route && sameSpeculativeExecutionRoute(route, { ...route, fingerprint: "changed" })).toBe(false);
		expect(await router.diagnostics(preparation)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ id: "unavailable", state: "unavailable", detail: "unavailable" }),
				expect.objectContaining({ id: "resource", state: "ready", detail: "Route prepared successfully" }),
			]),
		);
		const failures = [new Error("runtime cleanup failed"), new Error("resource cleanup failed")];
		vi.mocked(unavailable.dispose!).mockImplementation(() => { throw failures[0]; });
		vi.mocked(resource.dispose!).mockRejectedValue(failures[1]);
		const closing = router.dispose();
		expect(router.dispose()).toBe(closing);
		await expect(closing).rejects.toMatchObject({ message: "Execution world cleanup failed", errors: failures });
		expect(unavailable.dispose).toHaveBeenCalledOnce();
		expect(resource.dispose).toHaveBeenCalledOnce();

		expect(
			() => new ExecutionWorldRouter([runtime("same"), fallback("same", "resource_snapshot", "all")]),
		).toThrow("duplicate execution world same");
	});

	it("captures an authoritative result with the first explicitly capable world", async () => {
		const disposeCapture = vi.fn();
		const resourceBase = fallback("resource", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const capture = vi.fn(async () => ({
			seal: (output: string) => resourceBase.speculation.execute({ value: output }),
			dispose: disposeCapture,
		}));
		const resource: TestWorld = {
			...resourceBase,
			observation: { capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities, capture },
		};
		const runtimeBase = runtime("runtime");
		const prepare = vi.fn(async () => {});
		const runtimeWithoutCapture = { ...runtimeBase, speculation: { ...runtimeBase.speculation, prepare } };
		const router = new ExecutionWorldRouter([runtimeWithoutCapture, resource]);

		const captured = await router.captureAuthoritativeResult(
			{ effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS },
			preparation,
			{ value: "unused" },
		);

		expect(captured?.route).toMatchObject({ backend: "resource", reuse: "shared_result" });
		expect(prepare).not.toHaveBeenCalled();
		expect(capture).toHaveBeenCalledOnce();
		const branch = await captured?.capture.seal("actor output");
		expect(await branch?.commit()).toBe("actor output");
		expect(disposeCapture).not.toHaveBeenCalled();
	});

	it("routes Actor observation independently from speculative containment", async () => {
		const world = fallback("split", "workspace_branch", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const capture = vi.fn(async () => ({
			seal: (output: string) => world.speculation.execute({ value: output }),
			dispose: () => {},
		}));
		const split: TestWorld = {
			...world,
			observation: { capabilities: "all", capture },
			observeOperations: async ({ learn }, execute) => { observations.push(learn); return execute(); },
		};
		const observations: Array<boolean | undefined> = [];
		let speculationEnabled: boolean | undefined = true;
		const router = new ExecutionWorldRouter([split], () => {
			if (speculationEnabled === undefined) throw new Error("policy unavailable");
			return speculationEnabled;
		});
		const observationRequest = { effect: "observation" as const, requirements: RESOURCE_OBSERVATION_EFFECTS };
		const action = buildPiActionKey("read", { path: "file" }, preparation.cwd)!;
		const observe = (learn: boolean) => router.observeOperations(action, { sessionID: "session", turnID: "turn" },
			async () => "native once", () => {}, learn);
		await expect(observe(true)).resolves.toBe("native once");
		await expect(observe(false)).resolves.toBe("native once");

		const route = await router.resolve(observationRequest, preparation);
		expect(route).toBeDefined();
		speculationEnabled = false;
		expect(() => router.fork(route!, { value: "must not execute" })).toThrow("disabled by routing policy");
		expect(await router.resolve(observationRequest, preparation)).toBeUndefined();
		expect(
			await router.resolve({ effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS }, preparation),
		).toBeUndefined();
		expect(
			await router.captureAuthoritativeResult(
				{ effect: "unbounded", requirements: UNRESTRICTED_PROCESS_EFFECTS },
				preparation,
				{ value: "actor" },
			),
		).toMatchObject({ route: { backend: "split", reuse: "exclusive_branch" } });
		expect(capture).toHaveBeenCalledOnce();
		expect((await router.diagnostics(preparation))[0]).toMatchObject({
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			observation: { capabilities: "all" },
		});
		await expect(observe(true)).resolves.toBe("native once");
		speculationEnabled = undefined;
		await expect(observe(true)).resolves.toBe("native once");
		expect(observations).toEqual([true, false, false, false]);
	});

	it.each([false, true])("keeps observation authority independent from speculative tool scopes (%s)", async scoped => {
		const { speculation, ...lifecycle } = fallback("observe", "resource_snapshot", RESOURCE_OBSERVATION_EFFECTS.capabilities);
		const capture = vi.fn(async () => ({
			seal: async (output: string) => testBranch(output, { backend: "observe", executionFingerprint: "executor" }),
			dispose: () => {},
		}));
		const world: BaseTestWorld = {
			...lifecycle,
			...(scoped ? { speculation: { ...speculation, tools: ["read"] } } : {}),
			observation: { capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities, ...(scoped ? { tools: ["grep"] } : {}), capture },
		};
		const router = new ExecutionWorldRouter([world]);
		const request = (tool?: "read" | "grep") => ({
			effect: "observation" as const,
			requirements: RESOURCE_OBSERVATION_EFFECTS,
			...(tool ? { action: buildPiActionKey(tool, { path: ".", ...(tool === "grep" ? { pattern: "x" } : {}) }, "/workspace")! } : {}),
		});
		if (scoped) {
			expect(await router.resolve(request("read"), preparation)).toBeDefined();
			expect(await router.resolve(request("grep"), preparation)).toBeUndefined();
			expect(await router.captureAuthoritativeResult(request("read"), preparation, { value: "actor" })).toBeUndefined();
			expect(await router.captureAuthoritativeResult(request("grep"), preparation, { value: "actor" })).toBeDefined();
		} else {
			expect(await router.resolve(request(), preparation)).toBeUndefined();
			expect(await router.captureAuthoritativeResult(request(), preparation, { value: "actor" })).toBeDefined();
		}
		const diagnostics = await router.diagnostics(preparation);
		expect(diagnostics).toEqual([
			expect.objectContaining({ state: scoped ? "ready" : "unavailable", observation: expect.objectContaining({ state: "ready" }) }),
		]);
		expect(executionCapabilityStatus(RESOURCE_OBSERVATION_EFFECTS, diagnostics, "speculation", scoped ? "grep" : undefined).state)
			.toBe("unavailable");
		expect(executionCapabilityStatus(RESOURCE_OBSERVATION_EFFECTS, diagnostics, "observation", scoped ? "grep" : undefined).state)
			.toBe("ready");
	});

	it.each(["unavailable", "ready"] as const)("keeps portable routes independent of process capability=%s", (state) => {
		expect(toolStatuses(platformWorlds(state, "process capability"))).toEqual({
			read: "ready",
			grep: "unavailable",
			find: "unavailable",
			ls: "ready",
			write: "registered",
			edit: "registered",
			bash: state,
		});
	});

	it("lets an injected all-effect runtime make every tool routable on any host", () => {
		const worlds: ExecutionWorldDiagnosticSnapshot[] = [
			...platformWorlds("unavailable", "Linux host required"),
			{ id: "host_runtime", scope: "runtime", isolation: "runtime_sandbox", capabilities: "all", state: "ready", detail: "host runtime ready" },
		];
		expect(new Set(Object.values(toolStatuses(worlds)))).toEqual(new Set(["ready"]));
	});
});

function toolStatuses(worlds: readonly ExecutionWorldDiagnosticSnapshot[]): Record<string, string> {
	return Object.fromEntries(
		KEYABLE_TOOLS.map((tool) => [tool, executionCapabilityStatus(PI_ACTION_SEMANTICS.requirements(tool)!, worlds).state]),
	);
}

function platformWorlds(processState: "ready" | "unavailable", processDetail: string): readonly ExecutionWorldDiagnosticSnapshot[] {
	return [
		{ id: "linux_process_reuse", scope: "runtime", isolation: "runtime_sandbox", capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			state: processState, detail: processDetail },
		{ id: "git_worktree", scope: "fallback", isolation: "workspace_branch", capabilities: WORKSPACE_PATH_MUTATION_EFFECTS.capabilities,
			state: "registered", detail: "checked on first use" },
		{ id: "resource_version", scope: "fallback", isolation: "resource_snapshot", capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			state: "ready", detail: "resource validation ready" },
	];
}

function runtime(id: string): TestWorld {
	return world(id, "runtime", "runtime_sandbox", "all");
}

function fallback(id: string, isolation: Exclude<SpeculativeExecution, "runtime_sandbox">, capabilities: EffectCapabilities): TestWorld {
	return world(id, "fallback", isolation, capabilities);
}

function world(id: string, scope: TestWorld["scope"], isolation: TestWorld["isolation"], capabilities: EffectCapabilities): TestWorld {
	const dispose = vi.fn(async () => {});
	return {
		id,
		scope,
		isolation,
		speculation: {
			capabilities,
			fingerprint: () => `${id}`,
			execute: async ({ value }: { readonly value: string }) => testBranch(value, { backend: id, executionFingerprint: "executor" }),
		},
		dispose,
	} as TestWorld;
}
