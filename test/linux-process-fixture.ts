import { execHostText } from "../src/executable-path.ts";
import { access, copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { MessageChannel, type Worker } from "node:worker_threads";
import { expect, test, type TestAPI } from "vitest";
import { onExit } from "./async.ts";
import { readText } from "./filesystem.ts";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LinuxProcessReuseBackend, type LinuxProcessReuseMetrics } from "../src/linux-process-backend.ts";
import { resolveLinuxExecHelper } from "../src/linux-held-exec.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, definedProcessEnvironment, ProcessExecutionCoordinator, type ProcessExecutionRequest, type ProcessExecutor } from "../src/process-execution.ts";
import { WorkspaceSandboxService, type WorkspaceSandboxDriver } from "../src/workspace-sandbox.ts";
import { createAgentExecutionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { PatternAwareStore, patternAwareSettings, patternAwareActionSemantics } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/events.ts";
import { testModel } from "./model.ts";
import type { ProcessProvenanceCertificate } from "../src/provenance-certificate.ts";
import type { ExecutionScope } from "../src/execution-world.ts";

export type LinuxProcessBenchmark = Readonly<Awaited<ReturnType<typeof createLinuxProcessBenchmark>>>;
process.execArgv.push("--import", new URL("./process-owner-fixture.mjs", import.meta.url).href);

export function patternProcessHost(fixture: LinuxProcessBenchmark, sessionID: string, settings: SpeculativeAgentSettingsInput,
	options: Partial<CreateSpeculativeActionHostOptions> = {}, tools = [fixture.tool]) {
	const events: SpeculativeActionEvent<string>[] = [], patternStore = new PatternAwareStore(patternAwareSettings(settings.patternAware), undefined,
		patternAwareActionSemantics(PI_ACTION_SEMANTICS, fixture.workspace));
	const host = createAgentExecutionHost(sessionID, { cwd: fixture.workspace, patternStore,
		complete: async () => { throw new Error("unexpected inference"); }, preflight: () => true, executionWorlds: [fixture.world],
		getSettings: () => ({ enabled: true, drafterEnabled: false, ...settings }),
		resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd: fixture.workspace, environment: fixture.environment, shellPath: fixture.shellPath }),
		...options, onEvent: event => { events.push(event); options.onEvent?.(event); },
	});
	return { host, events, patternStore,
		waitFor: async (ready: () => boolean) => { await expect.poll(() => ready() || events.slice(-12), { timeout: 10_000 }).toBe(true); },
		start: (turnID: string, signal?: AbortSignal, systemPrompt = "unchanged") => host.startTurn({ turnID, tools, actorModel: testModel("actor"), actorOptions: undefined, context: { systemPrompt, messages: [], tools } }, signal),
		call: (turnID: string, command: string) => ({ turnID, id: turnID, tool: "bash", args: { command }, tools }),
	};
}

export async function recordProcessPublications(fixture: LinuxProcessBenchmark) {
	await fixture.inOwner(async state => {
		state.publishing = state.method((await state.load("../src/reuse-planner.ts")).ProcessReusePlanner.prototype, "publishCompleted");
	});
	return () => fixture.inOwner(state => state.publishing.mock.calls.map(({ arguments: [certificate] }: any) => certificate) as ProcessProvenanceCertificate[]);
}

/** Tests release their backends after assertions, failures and early skips. */
export const processTest: ReturnType<TestAPI<{ benchmark: typeof createLinuxProcessBenchmark }>["skipIf"]> = test.extend<{ benchmark: typeof createLinuxProcessBenchmark }>({
	benchmark: async ({}, use) => {
		const fixtures: LinuxProcessBenchmark[] = [];
		await using _cleanup = onExit(async () => { for (const fixture of fixtures) await fixture.dispose(); });
		await use(async (...args) => { const fixture = await createLinuxProcessBenchmark(...args); fixtures.push(fixture); return fixture; });
	},
}).skipIf(process.platform !== "linux");

export async function holdOwnedProcessPublication(fixture: Pick<LinuxProcessBenchmark, "inOwner">, beforeRelease = async () => {}, executablePath?: string) {
	await fixture.inOwner(async (state, executablePath) => {
		const { ProvenanceCertificateStore } = await state.load("../src/reuse-store.ts"), { ProcessHandoffRegistry } = await state.load("../src/process-handoff.ts"),
			{ LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), { AsyncLocalStorage } = await state.load("node:async_hooks");
		const publication = ProvenanceCertificateStore.prototype.withPublication, acquire = ProcessHandoffRegistry.prototype.acquire;
		const execute = LinuxProcessReuseBackend.prototype.executeRequest, frames = new AsyncLocalStorage(); let held = false;
		const pending = state.deferred(); state.release = pending.resolve; state.reached = state.deferred(); state.joined = false; state.joinedGate = state.deferred();
		LinuxProcessReuseBackend.prototype.executeRequest = function (...args: unknown[]) { return frames.run({}, () => execute.apply(this, args)); };
		ProcessHandoffRegistry.prototype.acquire = async function (...args: unknown[]) {
			const result = await acquire.apply(this, args), frame = frames.getStore();
			if (frame && result.kind === "work") frame.work = result.work;
			return result;
		};
		ProvenanceCertificateStore.prototype.withPublication = async function (...args: unknown[]) {
			const work = frames.getStore()?.work;
			if (work && !held && (!executablePath || work.executablePath === executablePath)) {
				held = true; const completion = work.completion;
				Object.defineProperty(work, "completion", { get: () => { state.joined = true; state.joinedGate.resolve(true); return completion; } });
				state.reached.resolve(); await pending.promise;
			}
			return publication.apply(this, args);
		};
		state.closePublication = () => { state.joinedGate.resolve(false); state.release(); ProvenanceCertificateStore.prototype.withPublication = publication;
			ProcessHandoffRegistry.prototype.acquire = acquire; LinuxProcessReuseBackend.prototype.executeRequest = execute; };
	}, executablePath);
	const joined = fixture.inOwner(state => state.joinedGate.promise as Promise<boolean>).then(async observed => {
		if (!observed) return;
		try { await beforeRelease(); }
		finally { await fixture.inOwner(state => state.release()); }
	});
	return { ready: () => fixture.inOwner(state => state.reached.promise as Promise<void>), joined: () => fixture.inOwner(state => state.joined as boolean),
		close: async () => { await fixture.inOwner(state => state.closePublication()); await joined; } };
}

export function inspectProcessOwner(create: () => object) {
	const { port1: testPort, port2 } = new MessageChannel();
	(Reflect.get(create(), "connection").worker as Worker).postMessage({ processTestPort: port2 }, [port2]);
	testPort.unref();
	const inOwner = <Value, Args extends unknown[]>(run: (state: Record<string, any>, ...args: Args) => Value, ...args: Args): Promise<Awaited<Value>> => {
		const { port1, port2 } = new MessageChannel();
		return new Promise<Awaited<Value>>((resolve, reject) => {
			port1.once("message", ({ value, error }) => { port1.close(); if (error) reject(new Error(error)); else resolve(value); });
			port1.once("close", () => reject(new Error("process test owner closed")));
			testPort.postMessage({ source: run.toString(), args, reply: port2 }, [port2]);
		});
	};
	return { inOwner, dispose: () => testPort.close() };
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
		for (const suffix of ["", ".node", ".sqlite"]) await copyFile(binary + suffix, backendOptions.heldExecBinary + suffix);
	}
	const backend = new LinuxProcessReuseBackend({ storeRoot, ...backendOptions });
	const { inOwner, dispose: closeInspection } = inspectProcessOwner(() => Reflect.get(backend, "processOwner").call(backend));
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
	let executionFingerprint: string | undefined, calls = 0;
	const invocation = (args: Record<string, unknown>) => resolvePiToolInvocation("bash", args, { cwd: workspace, environment, shellPath })!;
	return {
		root,
		workspace,
		storeRoot,
		shellPath,
		environment,
		coordinator,
		backend,
		inOwner,
		world,
		workspaceSandbox,
		invocation,
		tool: tool as AgentTool, // Preserve a portable type across nested schema packages.
		compile: compileBenchmarkHelper.bind(undefined, workspace),
		commit: commitBenchmarkFixture.bind(undefined, workspace),
		write: (file: string, data: Parameters<typeof writeFile>[1], options?: Parameters<typeof writeFile>[2]) => writeFile(path.join(workspace, file), data, options),
		read: readText.bind(undefined, workspace),
		prepare: async ({ workspaceDriver: driver, includeWorkspaceFingerprint, skip }: {
			readonly workspaceDriver?: WorkspaceSandboxDriver; readonly includeWorkspaceFingerprint?: boolean; readonly skip?: (reason: string) => void;
		} = {}) => {
			const status = await backend.check(true);
			if (status.state !== "ready") { skip?.(status.detail); throw new Error(status.detail); }
			if (!status.sandlockBinary || !status.straceBinary) throw new Error("Linux process backend omitted ready binaries");
			const workspaceFingerprint = includeWorkspaceFingerprint ? await workspaceSandbox.fingerprint({ driver: driver ?? "auto" }, workspace) : undefined;
			await world.speculation.prepare?.({ cwd: workspace });
			const backendFingerprint = await backend.fingerprint();
			return { executionFingerprint: executionFingerprint = workspaceFingerprint ? `${backendFingerprint}:${workspaceFingerprint}` : backendFingerprint };
		},
		fork: async (command: string, options: { readonly namespace?: string; readonly executionScope?: ExecutionScope; readonly signal?: AbortSignal } = {}) => {
			if (!executionFingerprint) throw new Error("Prepare the process fixture before forking");
			const args = { command }, action = PI_ACTION_SEMANTICS.buildKey("bash", args, workspace, options.namespace ?? "benchmark", {
				fingerprint: executionFingerprint, context: invocation(args),
			});
			if (!action) throw new Error("Pi Bash action could not be keyed");
			const branch = await world.speculation.execute({ cwd: workspace, tool, toolName: "bash", args, action, callID: `bench-${++calls}`,
				signal: options.signal ?? new AbortController().signal, executionScope: options.executionScope ?? { sessionID: "benchmark", turnID: "benchmark" } });
			if (branch.inputsOnly) { await branch.dispose(); throw new Error("Bash fork must produce its tool result"); }
			return branch;
		},
		prepareActorReplay: async (refresh?: boolean) => {
			if (refresh) await backend.refreshNativeObservation();
			const replay = backend.completedReplayExecutor(host, { sourceRoot: workspace, invocation: () => undefined });
			return { executor: { execute: (request: ProcessExecutionRequest) => backend.observeNative(workspace, request.scope, request.signal, () => replay.execute(request)) } };
		},
		dispose: () => disposal ??= Promise.resolve().then(() => world.dispose?.()).finally(closeInspection)
			.finally(() => workspaceSandbox.dispose()).finally(() => rm(root, { recursive: true, force: true })),
	};
}

export async function compileBenchmarkHelper(
	workspace: string, output: string,
	{ source = `${output}.c`, arguments: compilerArguments = [], code }: { readonly source?: string; readonly arguments?: readonly string[]; readonly code?: string } = {},
): Promise<void> {
	if (code !== undefined) await writeFile(path.join(workspace, source), code);
	await execHostText("cc", ["-O2", "-Wall", "-Wextra", ...compilerArguments, "-o", output, source], { cwd: workspace });
	await access(path.join(workspace, output));
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
