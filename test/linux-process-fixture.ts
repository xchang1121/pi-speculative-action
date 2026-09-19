import { execFile } from "node:child_process";
import { access, mkdir, mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { vi } from "vitest";
import { deferred } from "./async.ts";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import {
	LinuxProcessReuseBackend,
	type LinuxProcessReuseMetrics,
} from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { WorkspaceSandboxService, type WorkspaceSandboxDriver } from "../src/workspace-sandbox.ts";
import type { ProcessHandoffRegistry } from "../src/process-handoff.ts";
import type { SpeculationScheduler } from "../src/scheduler.ts";

const BENCHMARK_SCOPE = { sessionID: "benchmark", turnID: "benchmark" } as const;

export type LinuxProcessBenchmark = Readonly<Awaited<ReturnType<typeof createLinuxProcessBenchmark>>>;

/** Retain a real producer at publication until an Actor is admitted to its running work. */
export function holdProcessPublication(backend: LinuxProcessReuseBackend) {
	const release = deferred();
	let reached = false;
	const handoffs = Reflect.get(backend, "handoffs") as ProcessHandoffRegistry;
	const publish = handoffs.publish.bind(handoffs);
	const publication = vi.spyOn(handoffs, "publish").mockImplementation(async (...args) => {
		reached = true; await release.promise; return publish(...args);
	});
	const scheduler = Reflect.get(backend, "processScheduler") as SpeculationScheduler<object>;
	const assess = scheduler.assessCandidateJoin.bind(scheduler);
	let evidence: { request: Parameters<typeof assess>[0]; decision: ReturnType<typeof assess> } | undefined;
	const assessment = vi.spyOn(scheduler, "assessCandidateJoin").mockImplementation(request => {
		const decision = assess(request);
		if (request.state === "running") {
			evidence = { request, decision };
			if (decision.allowed) queueMicrotask(() => release.resolve());
		}
		return decision;
	});
	return {
		reached: () => reached,
		evidence: () => evidence,
		close: () => { release.resolve(); publication.mockRestore(); assessment.mockRestore(); },
	};
}

export async function createLinuxProcessBenchmark(
	rootPrefix: string,
	workspaceDriver?: WorkspaceSandboxDriver,
) {
	if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
	const root = await mkdtemp(path.join(os.tmpdir(), rootPrefix));
	const workspace = path.join(root, "workspace");
	const storeRoot = path.join(root, "process-reuse");
	await mkdir(workspace);
	const shellPath = "/bin/bash";
	const environment: Readonly<Record<string, string>> = Object.freeze({
		PATH: `${workspace}:/home/${os.userInfo().username}/.local/bin:/usr/local/bin:/usr/bin:/bin`,
		HOME: os.homedir(),
		SHELL: shellPath,
		LANG: "C.UTF-8",
	});
	const localOperations = createLocalBashOperations({ shellPath });
	const backend = new LinuxProcessReuseBackend({
		storeRoot,
		...(process.env.PI_SPEC_SANDLOCK ? { sandlockBinary: process.env.PI_SPEC_SANDLOCK } : {}),
		...(process.env.PI_SPEC_HELD_EXEC ? { heldExecBinary: process.env.PI_SPEC_HELD_EXEC } : {}),
		...(process.env.PI_SPEC_STRACE ? { straceBinary: process.env.PI_SPEC_STRACE } : {}),
	});
	const coordinator = new ProcessExecutionCoordinator(
		backend.completedReplayExecutor(adaptProcessToolOperations(localOperations), {
			sourceRoot: workspace,
			invocation: (request) =>
				resolvePiToolInvocation("bash", { command: request.command }, {
					cwd: request.cwd,
					environment: Object.fromEntries(
						Object.entries(request.environment).filter((entry): entry is [string, string] => entry[1] !== undefined),
					),
					shellPath,
				})?.process,
		}),
	);
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
	let disposed = false;
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
		prepareActorReplay: (refresh?: boolean) => backend.prepareActorReplay(adaptProcessToolOperations(localOperations), {
			sourceRoot: workspace, invocation: () => undefined, held: { realShell: shellPath,
				executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) },
		}, refresh),
		dispose: async () => {
			if (disposed) return;
			disposed = true;
			try {
				try { await world.dispose?.(); } finally { await workspaceSandbox.dispose(); }
			} finally {
				await rm(root, { recursive: true, force: true });
			}
		},
	};
}

export async function prepareLinuxProcessReuse(
	fixture: LinuxProcessBenchmark,
	options: { readonly workspaceDriver?: WorkspaceSandboxDriver; readonly includeWorkspaceFingerprint?: boolean } = {},
) {
	const status = await fixture.backend.check(true);
	if (status.state !== "ready") throw new Error(status.detail);
	if (!status.sandlockBinary || !status.straceBinary) throw new Error("Linux process backend omitted ready binaries");
	const workspaceFingerprint = options.includeWorkspaceFingerprint
		? await fixture.workspaceSandbox.fingerprint({ driver: options.workspaceDriver ?? "auto" }, fixture.workspace)
		: undefined;
	await fixture.world.speculation.prepare?.({ cwd: fixture.workspace });
	const backendFingerprint = await fixture.backend.fingerprint();
	return {
		executionFingerprint: workspaceFingerprint
			? `${backendFingerprint}:${workspaceFingerprint}`
			: backendFingerprint,
	};
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
	const invocation = resolvePiToolInvocation("bash", args, {
		cwd: fixture.workspace,
		environment: fixture.environment,
		shellPath: fixture.shellPath,
	});
	if (!invocation) throw new Error("Pi Bash invocation could not be materialized");
	const action = PI_ACTION_SEMANTICS.buildKey("bash", args, fixture.workspace, input.actionNamespace, {
		fingerprint: input.executionFingerprint,
		context: invocation,
	});
	if (!action) throw new Error("Pi Bash action could not be keyed");
	return fixture.world.speculation.execute({
		cwd: fixture.workspace,
		tool: fixture.tool,
		toolName: "bash",
		args,
		action,
		callID: `bench-${input.label}`,
		signal: input.signal ?? new AbortController().signal,
		executionScope: input.executionScope ?? BENCHMARK_SCOPE,
	});
}

export async function compileBenchmarkHelper(
	workspace: string,
	input: { readonly source: string; readonly output: string; readonly arguments?: readonly string[] },
): Promise<void> {
	await commandOutput(
		"cc",
		["-O2", "-Wall", "-Wextra", ...(input.arguments ?? []), "-o", input.output, input.source],
		workspace,
	);
	await access(path.join(workspace, input.output));
}

export async function commitBenchmarkFixture(
	workspace: string,
	name: string,
	paths: readonly string[] = ["."],
): Promise<void> {
	await commandOutput("git", ["init", "--quiet"], workspace);
	await commandOutput("git", ["config", "user.name", name], workspace);
	await commandOutput("git", ["config", "user.email", "benchmark@localhost"], workspace);
	await commandOutput("git", ["add", ...paths], workspace);
	await commandOutput("git", ["commit", "--quiet", "-m", "benchmark fixture"], workspace);
}

function commandOutput(executable: string, args: readonly string[], cwd?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { cwd, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
		});
	});
}

export function textOutput(result: { readonly content: readonly { readonly type: string; readonly text?: string }[] }): string {
	return result.content
		.filter((item): item is { readonly type: "text"; readonly text: string } =>
			item.type === "text" && typeof item.text === "string")
		.map((item) => item.text)
		.join("\n");
}

export function metricDelta(before: LinuxProcessReuseMetrics, after: LinuxProcessReuseMetrics): LinuxProcessReuseMetrics {
	return {
		...Object.fromEntries(Object.entries(numericMetrics(after)).map(([name, value]) => [name, value - (Reflect.get(before, name) ?? 0)])),
		...(after.lastError !== before.lastError && after.lastError ? { lastError: after.lastError } : {}),
	} as LinuxProcessReuseMetrics;
}

function numericMetrics(metrics: LinuxProcessReuseMetrics): Readonly<Record<string, number>> {
	return Object.fromEntries(
		Object.entries(metrics).filter((entry): entry is [string, number] => typeof entry[1] === "number"),
	);
}
