import { execHostText } from "../src/executable-path.ts";
import { access, copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, vi, type TestAPI } from "vitest";
import { deferred, onExit } from "./async.ts";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LinuxProcessReuseBackend, type LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import { resolveLinuxExecHelper } from "../src/linux-held-exec.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, definedProcessEnvironment, ProcessExecutionCoordinator, type ProcessExecutionRequest, type ProcessExecutor } from "../src/process-execution.ts";
import { WorkspaceSandboxService, type WorkspaceSandboxDriver } from "../src/workspace-sandbox.ts";
import type { ProcessHandoffRegistry } from "../src/process-handoff.ts";
import * as scheduling from "../src/scheduler.ts";

export type LinuxProcessBenchmark = Readonly<Awaited<ReturnType<typeof createLinuxProcessBenchmark>>>;

/** Tests release their backends after assertions, failures and early skips. */
export const processTest: ReturnType<TestAPI<{ benchmark: typeof createLinuxProcessBenchmark }>["skipIf"]> = test.extend<{ benchmark: typeof createLinuxProcessBenchmark }>({
	benchmark: async ({}, use) => {
		const fixtures: LinuxProcessBenchmark[] = [];
		await using _cleanup = onExit(async () => { for (const fixture of fixtures) await fixture.dispose(); });
		await use(async (...args) => { const fixture = await createLinuxProcessBenchmark(...args); fixtures.push(fixture); return fixture; });
	},
}).skipIf(process.platform !== "linux");

/** Hold finished production before its shared writer admission until an Actor joins owned running work. */
export function holdProcessPublication(backend: LinuxProcessReuseBackend, delayMs = 0, beforeRelease = async () => {}) {
	const release = deferred();
	let joined = false, reached = false, timer: ReturnType<typeof setTimeout> | undefined;
	const handoffs = Reflect.get(backend, "handoffs") as ProcessHandoffRegistry;
	const observing = vi.spyOn(handoffs, "observeRunning"), publish = backend.store.withPublication.bind(backend.store);
	const publication = vi.spyOn(backend.store, "withPublication").mockImplementation(async (...args) => {
		reached = true; await release.promise; return publish(...args);
	});
	const wait = scheduling.waitForCompletion;
	const assessment = vi.spyOn(scheduling, "waitForCompletion").mockImplementation((promise, signal, timeoutMs) => {
		if (observing.mock.calls.some(([, owner]) => owner.completion === promise) && !joined && !signal?.aborted) {
			joined = true; timer = setTimeout(() => { void beforeRelease().then(release.resolve, release.reject); }, delayMs);
		}
		return wait(promise, signal, timeoutMs);
	});
	return {
		reached: () => reached,
		joined: () => joined,
		close: () => { clearTimeout(timer); release.resolve(); publication.mockRestore(); assessment.mockRestore(); observing.mockRestore(); },
	};
}

export async function createLinuxProcessBenchmark(
	rootPrefix: string,
	workspaceDriver?: WorkspaceSandboxDriver,
	{ processImages, ...backendOptions }: Omit<ConstructorParameters<typeof LinuxProcessReuseBackend>[0], "storeRoot"> & { readonly processImages?: false } = {},
	parent = os.tmpdir(),
) {
	if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
	const root = await mkdtemp(path.join(await mkdir(parent, { recursive: true }).then(() => parent), rootPrefix));
	const workspace = path.join(root, "workspace");
	const storeRoot = path.join(root, "process-reuse");
	await mkdir(workspace);
	const shellPath = "/bin/bash";
	const environment: Readonly<Record<string, string>> = Object.freeze({
		PATH: `${workspace}:${path.dirname(process.execPath)}:/home/${os.userInfo().username}/.local/bin:/usr/local/bin:/usr/bin:/bin`,
		HOME: os.homedir(),
		SHELL: shellPath,
		LANG: "C.UTF-8",
	});
	const executor = (shellPath: string) => adaptProcessToolOperations(createLocalBashOperations({ shellPath })), host = executor(shellPath);
	if (processImages === false) {
		const binary = await resolveLinuxExecHelper(backendOptions.heldExecBinary ?? process.env.PI_SPEC_HELD_EXEC);
		backendOptions.heldExecBinary = path.join(root, "held-exec");
		for (const suffix of ["", ".node"]) await copyFile(binary + suffix, backendOptions.heldExecBinary + suffix);
	}
	const backend = new LinuxProcessReuseBackend({ storeRoot, ...backendOptions });
	const coordinator = new ProcessExecutionCoordinator(backend.completedReplayExecutor(host, {
		sourceRoot: workspace, invocation: request => resolvePiToolInvocation("bash", { command: request.command }, {
			cwd: request.cwd, environment: definedProcessEnvironment(request.environment), shellPath,
		})?.process,
	}));
	const workspaceSandbox = new WorkspaceSandboxService();
	const world = createLinuxProcessExecutionWorld({
		workspaceSandbox,
		coordinator,
		tools: PI_OPERATION_TOOLS.process,
		backend,
		storeRoot,
		...(workspaceDriver ? { driver: workspaceDriver } : {}),
	});
	const tool: AgentTool = createBashTool(workspace, {
		operations: coordinator.operations,
		shellPath,
		exposeSessionEnvironment: false,
		spawnHook: (context) => ({ ...context, env: { ...environment } }),
	});
	let disposal: Promise<void> | undefined;
	return {
		root,
		workspace,
		storeRoot,
		shellPath,
		environment,
		coordinator,
		backend,
		world,
		workspaceSandbox,
		tool: tool as AgentTool, // Preserve a portable type across nested schema packages.
		prepareActorReplay: (refresh?: boolean) => backend.prepareActorReplay(host, {
			sourceRoot: workspace, invocation: () => undefined, held: true,
		}, refresh).then(route => "executor" in route ? route : Promise.reject(new Error(route.detail))),
		dispose: () => disposal ??= Promise.resolve().then(() => world.dispose?.())
			.finally(() => workspaceSandbox.dispose()).finally(() => rm(root, { recursive: true, force: true })),
	};
}

export async function prepareLinuxProcessReuse(
	fixture: LinuxProcessBenchmark,
	options: { readonly workspaceDriver?: WorkspaceSandboxDriver; readonly includeWorkspaceFingerprint?: boolean; readonly skip?: (reason: string) => void } = {},
) {
	const status = await fixture.backend.check(true);
	if (status.state !== "ready") { options.skip?.(status.detail); throw new Error(status.detail); }
	if (!status.sandlockBinary || !status.straceBinary) throw new Error("Linux process backend omitted ready binaries");
	const workspaceFingerprint = options.includeWorkspaceFingerprint
		? await fixture.workspaceSandbox.fingerprint({ driver: options.workspaceDriver ?? "auto" }, fixture.workspace)
		: undefined;
	await fixture.world.speculation.prepare?.({ cwd: fixture.workspace });
	const backendFingerprint = await fixture.backend.fingerprint();
	return { executionFingerprint: workspaceFingerprint ? `${backendFingerprint}:${workspaceFingerprint}` : backendFingerprint };
}

export interface ReusableBashInput {
	readonly label: string;
	readonly command: string;
	readonly actionNamespace: string;
	readonly executionFingerprint: string;
	readonly executionScope?: { readonly sessionID: string; readonly turnID: string };
	readonly signal?: AbortSignal;
}

export async function forkReusableBash(fixture: Pick<LinuxProcessBenchmark, "world" | "tool" | "workspace" | "environment" | "shellPath">, input: ReusableBashInput) {
	const args = { command: input.command };
	const invocation = resolvePiToolInvocation("bash", args, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath });
	if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
	const action = PI_ACTION_SEMANTICS.buildKey("bash", args, fixture.workspace, input.actionNamespace, { fingerprint: input.executionFingerprint, context: invocation });
	if (!action) throw new Error("Pi Bash action could not be keyed");
	const branch = await fixture.world.speculation.execute({
		cwd: fixture.workspace,
		tool: fixture.tool,
		toolName: "bash",
		args,
		action,
		callID: `bench-${input.label}`,
		signal: input.signal ?? new AbortController().signal,
		executionScope: input.executionScope ?? { sessionID: "benchmark", turnID: "benchmark" },
	});
	if (branch.inputsOnly) { await branch.dispose(); throw new Error("Bash fork must produce its tool result"); }
	return branch;
}

export async function compileBenchmarkHelper(
	workspace: string,
	input: { readonly source: string; readonly output: string; readonly arguments?: readonly string[]; readonly code?: string },
): Promise<void> {
	if (input.code !== undefined) await writeFile(path.join(workspace, input.source), input.code);
	await execHostText("cc", ["-O2", "-Wall", "-Wextra", ...(input.arguments ?? []), "-o", input.output, input.source], { cwd: workspace });
	await access(path.join(workspace, input.output));
}

/** Render shared C boilerplate without changing include order or the helper's source bytes. */
export function cHeaders(headers: string): string {
	return headers.split(" ").map(header => `#include <${header}>`).join("\n");
}

export async function executeProcess(fixture: Pick<LinuxProcessBenchmark, "workspace" | "environment">, executor: ProcessExecutor,
	command: string, request: Pick<ProcessExecutionRequest, "scope" | "signal" | "timeout"> = {}) {
	let output = "";
	return { ...await executor.execute({ command, cwd: fixture.workspace, environment: fixture.environment, ...request, onData: data => { output += data.toString(); } }), output };
}

export async function commitBenchmarkFixture(workspace: string, name: string, paths: readonly string[] = ["."]): Promise<void> {
	await execHostText("git", ["init", "--quiet"], { cwd: workspace });
	await execHostText("git", ["config", "user.name", name], { cwd: workspace });
	await execHostText("git", ["config", "user.email", "benchmark@localhost"], { cwd: workspace });
	await execHostText("git", ["add", ...paths], { cwd: workspace });
	await execHostText("git", ["commit", "--quiet", "-m", "benchmark fixture"], { cwd: workspace });
}

export function textOutput(result: { readonly content: readonly { readonly type: string; readonly text?: string }[] }): string {
	return result.content.flatMap(item => item.type === "text" && typeof item.text === "string" ? [item.text] : []).join("\n");
}

export function metricDelta(before: LinuxProcessReuseMetrics, after: LinuxProcessReuseMetrics): LinuxProcessReuseMetrics {
	return {
		...Object.fromEntries(Object.entries(after).flatMap(([name, value]) => typeof value === "number" ? [[name, value - (Reflect.get(before, name) ?? 0)]] : [])),
		...(after.lastError !== before.lastError && after.lastError ? { lastError: after.lastError } : {}),
	} as LinuxProcessReuseMetrics;
}
