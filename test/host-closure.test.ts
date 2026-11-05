import { writeFile } from "node:fs/promises";
import path from "node:path";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test } from "vitest";
import type { AgentExecutionWorld, SpeculativeToolExecutionContext } from "../src/agent-execution-world.ts";
import { resourceWorld, systemHost, expectHiddenCompute } from "./agent-system-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { onExit } from "./async.ts";
import { textOutput } from "./linux-process-fixture.ts";
import { createCompletedProcessExecutionWorld, createRestorableProcessExecutionWorld, createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { ExecutionWorldRouter, registerExecutionWorld, type ExecutionWorldEntry } from "../src/execution-world.ts";
import { ProcessExecutionCoordinator } from "../src/process-execution.ts";

const directories = temporaryDirectories("pi-host-closure-");
afterEach(directories.dispose);

test.for(["native", "external", "copied-completed", "copied-restorable"] as const)("rejects every unsupported execution entry before provider work (%s)", async kind => {
	const cwd = await directories.create(), probes: string[] = [];
	await writeFile(path.join(cwd, "input.txt"), "bound input");
	const entered = async (entry: string): Promise<never> => { probes.push(entry); throw new Error("outside resource closure"); };
	const source = kind.startsWith("copied-") ? (kind === "copied-completed" ? createCompletedProcessExecutionWorld : createRestorableProcessExecutionWorld)({ storeRoot: path.join(cwd, "store"), tools: ["read"],
		coordinator: new ProcessExecutionCoordinator({ execute: () => entered("process") }) }) : resourceWorld();
	let disposed = 0;
	const outside: AgentExecutionWorld = {
		...source, id: kind.startsWith("copied-") ? source.id : `outside-${kind}`,
		scope: "runtime", isolation: "runtime_sandbox",
		speculation: { ...resourceWorld().speculation!, fingerprint: () => entered("fingerprint"), prepare: () => entered("prepare"),
			diagnostics: () => entered("diagnostics"), execute: () => entered("execute") },
		observation: { ...resourceWorld().observation!, fingerprint: () => entered("capture-fingerprint"), capture: () => entered("capture"),
			diagnostics: () => entered("observation-diagnostics") },
		observeOperations: () => entered("observeOperations"),
		actorFallbackSettled: () => entered("actorFallbackSettled"), finishTurn: () => entered("finishTurn"),
		dispose: async () => { disposed++; await source.dispose?.(); },
	};
	const f = systemHost(cwd, [createReadTool(cwd)], { executionWorlds: [outside] });
	await using _cleanup = onExit(f.dispose);
	await f.host.executionWorldDiagnostics(true);
	await f.start();
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("bound input");
	await f.finish(); await f.start("second");
	await f.preview("read", { path: "input.txt" });
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("bound input");
	expectHiddenCompute(f.actors.at(-1)!.computation!, true);
	await f.finish(); await f.dispose();
	expect(probes).toEqual([]); expect(disposed).toBe(1);
});

test("admits registered providers through qualification, freshness and retirement", async () => {
	const cwd = await directories.create(), checks: ExecutionWorldEntry[] = [];
	await writeFile(path.join(cwd, "input.txt"), "one");
	let available = true, forks = 0, observers = 0, disposed = 0;
	const source = resourceWorld(), speculation = source.speculation!;
	const world = registerExecutionWorld({ ...source, id: "independent_provider", scope: "runtime", isolation: "runtime_sandbox",
		speculation: { ...speculation, execute: (context: Parameters<typeof speculation.execute>[0]) => { forks++; return speculation.execute(context); } },
		observeOperations: async <Value>(_request: unknown, execute: () => Promise<Value>) => { observers++; return execute(); },
		dispose: async () => { disposed++; },
	} satisfies AgentExecutionWorld, { operations: ["speculation", "observeOperations"], verify: async ({ operation }) => {
		checks.push(operation); if (!available) throw new Error("provider retired");
		return { fingerprint: "bound-resource-provider:1", externalEffects: "excluded" };
	} });
	expect(Object.isFrozen(world)).toBe(true); expect(Object.isFrozen(world.speculation)).toBe(true);
	const f = systemHost(cwd, [createReadTool(cwd), { ...createReadTool(cwd), name: "other" }], { executionWorlds: [world] });
	await using _cleanup = onExit(f.dispose);
	await f.start(); await f.preview("read", { path: "input.txt" }); await f.ready();
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("one");
	expect(f.fallbacks).toEqual([]); expect(forks).toBe(1); expectHiddenCompute(f.actors.at(-1)!.computation!, true);
	available = false; await writeFile(path.join(cwd, "input.txt"), "two");
	expect(textOutput(await f.execute("read", { path: "input.txt" }))).toBe("two");
	expect(textOutput(await f.execute("other", { path: "input.txt" }))).toBe("two");
	expect(f.fallbacks).toEqual(["other"]); expect(forks).toBe(1); expect(observers).toBe(0);
	expect(checks).toContain("observeOperations"); expect(checks).not.toContain("observation");
	expect(await f.host.executionWorldDiagnostics(true)).toContainEqual(expect.objectContaining({ id: world.id, state: "unavailable", detail: "provider retired" }));
	await f.finish(); await f.dispose(); await world.dispose(); expect(disposed).toBe(1);
});

test("the public process factory reaches provider qualification without a private factory brand", async () => {
	const cwd = await directories.create();
	const world = createLinuxProcessExecutionWorld({ storeRoot: path.join(cwd, "store"), tools: [],
		coordinator: new ProcessExecutionCoordinator({ execute: async () => { throw new Error("no process expected"); } }) });
	const f = systemHost(cwd, [], { executionWorlds: [world] });
	await using _cleanup = onExit(f.dispose);
	expect(await f.host.executionWorldDiagnostics()).toContainEqual(expect.objectContaining({ id: world.id, state: "registered" }));
	const status = (await f.host.executionWorldDiagnostics(true)).find(status => status.id === world.id)!;
	expect(status.detail).not.toContain("Outside the supported resource closure");
	if (process.platform !== "linux") expect(status).toMatchObject({ state: "unavailable", detail: "Linux host required" });
});

test("qualification changes invalidate resolved routes and registration freezes entry methods", async () => {
	const cwd = await directories.create(), source = resourceWorld();
	let version = 1, executions = 0, disposed = 0;
	const implementation = { ...source, id: "changing-provider", scope: "runtime" as const, isolation: "runtime_sandbox" as const,
		speculation: { ...source.speculation!, execute: async () => { executions++; throw new Error("original-executor"); } },
		dispose: async () => { disposed++; await source.dispose?.(); } };
	const qualification = { operations: ["speculation"] as ExecutionWorldEntry[],
		verify: async () => ({ fingerprint: String(version), externalEffects: "excluded" as const }) };
	const world = registerExecutionWorld(implementation, qualification), router = new ExecutionWorldRouter([world]);
	await using _cleanup = onExit(() => router.dispose());
	implementation.speculation.execute = async () => { throw new Error("replaced-executor"); };
	implementation.dispose = async () => { throw new Error("replaced-dispose"); };
	qualification.operations.length = 0; qualification.verify = async () => { throw new Error("replaced-probe"); };
	const request = { effect: "observation" as const, requirements: { capabilities: [] } };
	const route = await router.resolve(request, { cwd }); expect(route).toBeDefined();
	version++;
	await expect(router.fork(route!, {} as SpeculativeToolExecutionContext)).rejects.toThrow("qualification changed");
	expect(executions).toBe(0);
	const refreshed = await router.resolve(request, { cwd });
	await expect(router.fork(refreshed!, {} as SpeculativeToolExecutionContext)).rejects.toThrow("original-executor");
	expect(executions).toBe(1);
	await world.dispose();
	await expect(router.fork(refreshed!, {} as SpeculativeToolExecutionContext)).rejects.toThrow("retired");
	await router.dispose(); expect(disposed).toBe(1);
});

test("an independently qualified observation entry is visible and wraps the actual Actor once", async () => {
	const cwd = await directories.create(); await writeFile(path.join(cwd, "input.txt"), "observed");
	let observations = 0;
	const world = registerExecutionWorld({ id: "observer-only", scope: "runtime", isolation: "runtime_sandbox",
		observeOperations: async <Value>(_request: unknown, execute: () => Promise<Value>) => { observations++; return execute(); },
	} satisfies AgentExecutionWorld, { operations: ["observeOperations"], verify: async ({ operation }) => {
		expect(operation).toBe("observeOperations"); return { fingerprint: "observer:1", externalEffects: "excluded" };
	} });
	const f = systemHost(cwd, [Object.assign(createReadTool(cwd), { name: "other" })], { executionWorlds: [world] });
	await using _cleanup = onExit(f.dispose);
	expect(await f.host.executionWorldDiagnostics(true)).toContainEqual(expect.objectContaining({ id: world.id, state: "ready" }));
	await f.start(); expect(textOutput(await f.execute("other", { path: "input.txt" }))).toBe("observed");
	expect(observations).toBe(1); expect(f.fallbacks).toEqual(["other"]); await f.finish();
});
