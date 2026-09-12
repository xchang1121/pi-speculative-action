import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendStatus,
	type LinuxProcessReuseMetrics,
} from "../src/linux-process-backend.ts";
import { createLinuxProcessExecutionWorld } from "../src/linux-process-world.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import { WorkspaceSandboxService, type WorkspaceSandboxDriver } from "../src/workspace-sandbox.ts";

export type NumericMetrics = Readonly<Record<string, number>>;
export function benchmarkWriteAllC(type: "char" | "unsigned char" = "char"): string {
	return String.raw`static int write_all(int fd, const ${type} *data, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);
    if (written < 0) { if (errno == EINTR) continue; return -1; }
    data += written; length -= (size_t)written;
  }
  return 0;
}`;
}
const BENCHMARK_SCOPE = { sessionID: "benchmark", turnID: "benchmark" } as const;
type ReadyLinuxProcessBackendStatus = LinuxProcessBackendStatus & {
	readonly state: "ready";
	readonly sandlockBinary: string;
	readonly straceBinary: string;
};

export type LinuxProcessBenchmark = Readonly<Awaited<ReturnType<typeof createLinuxProcessBenchmark>>>;

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
	const readyStatus: ReadyLinuxProcessBackendStatus = {
		...status,
		state: "ready",
		sandlockBinary: status.sandlockBinary,
		straceBinary: status.straceBinary,
	};
	const started = performance.now();
	const workspaceFingerprint = options.includeWorkspaceFingerprint
		? await fixture.workspaceSandbox.fingerprint({ driver: options.workspaceDriver ?? "auto" }, fixture.workspace)
		: undefined;
	await fixture.world.speculation.prepare?.({ cwd: fixture.workspace });
	const backendFingerprint = await fixture.backend.fingerprint();
	return {
		status: readyStatus,
		executionFingerprint: workspaceFingerprint
			? `${backendFingerprint}:${workspaceFingerprint}`
			: backendFingerprint,
		...(workspaceFingerprint ? { workspaceFingerprint } : {}),
		routePreparationMs: performance.now() - started,
	};
}

export async function executeReusableBash(
	fixture: Pick<LinuxProcessBenchmark, "backend" | "world" | "tool" | "workspace" | "environment" | "shellPath">,
	input: ReusableBashInput,
) {
	const metricsBefore = fixture.backend.metrics();
	const started = performance.now();
	const branch = await forkReusableBash(fixture, input);
	const forkMs = performance.now() - started;
	const result = await (async () => {
		if (branch.output.isError) throw new Error(textOutput(branch.output.result));
		const validationStarted = performance.now();
		const validation = await branch.validate?.();
		const validationMs = performance.now() - validationStarted;
		if (validation?.status !== "valid") throw new Error(`branch validation failed: ${JSON.stringify(validation)}`);
		const commitStarted = performance.now();
		const committed = await branch.commit();
		const commitMs = performance.now() - commitStarted;
		return {
			measurement: { forkMs, validationMs, commitMs },
			output: committed,
			resources: Object.freeze([...branch.resources]),
		};
	})().finally(() => branch.dispose());
	return { ...result, measurement: { ...result.measurement, totalMs: performance.now() - started,
		metricDelta: metricDelta(metricsBefore, fixture.backend.metrics()) } };
}

export interface ReusableBashInput {
	readonly label: string;
	readonly command: string;
	readonly actionNamespace: string;
	readonly executionFingerprint: string;
	readonly executionScope?: { readonly sessionID: string; readonly turnID: string };
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
		signal: new AbortController().signal,
		executionScope: input.executionScope ?? BENCHMARK_SCOPE,
	});
}

export async function executeDirectBash(
	fixture: LinuxProcessBenchmark,
	input: { readonly label: string; readonly command: string },
) {
	const started = performance.now();
	const output = await fixture.tool.execute(
		`bench-${input.label}`,
		{ command: input.command },
		new AbortController().signal,
	);
	return { totalMs: performance.now() - started, output };
}

export async function linuxBenchmarkHost(
	status?: ReadyLinuxProcessBackendStatus,
) {
	return {
		platform: process.platform,
		arch: process.arch,
		node: process.version,
		kernel: (await commandOutput("uname", ["-srm"])).trim(),
		...(status
			? {
				sandlock: (await commandOutput(status.sandlockBinary, ["--version"])).trim(),
				strace: (await commandOutput(status.straceBinary, ["-V"])).split(/\r?\n/)[0]?.trim(),
			}
			: {}),
	};
}

export async function writeBenchmarkReport(value: unknown, outputPath?: string): Promise<void> {
	const rendered = `${JSON.stringify(value, null, 2)}\n`;
	if (outputPath) {
		await mkdir(path.dirname(path.resolve(outputPath)), { recursive: true });
		await writeFile(path.resolve(outputPath), rendered, "utf8");
	}
	process.stdout.write(rendered);
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

export async function fileDigest(target: string): Promise<string> {
	const hash = createHash("sha256");
	for await (const chunk of createReadStream(target)) hash.update(chunk);
	return hash.digest("hex");
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

export function numericMetrics(metrics: LinuxProcessReuseMetrics): NumericMetrics {
	return Object.fromEntries(
		Object.entries(metrics).filter((entry): entry is [string, number] => typeof entry[1] === "number"),
	);
}

export function median(values: readonly number[]): number {
	const sorted = [...values].sort((left, right) => left - right);
	return sorted[Math.floor(sorted.length / 2)]!;
}

export function argument(name: string): string | undefined {
	const index = process.argv.indexOf(name);
	if (index < 0) return undefined;
	const value = process.argv[index + 1];
	if (!value || value.startsWith("--")) throw new Error(`${name} requires a value`);
	return value;
}

export function integerArgument(name: string, fallback: number, minimum: number, maximum: number): number {
	const raw = argument(name);
	if (raw === undefined) return fallback;
	const value = Number(raw);
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
	}
	return value;
}

export function workspaceDriverArgument(value: string | undefined): WorkspaceSandboxDriver {
	if (value === undefined || value === "auto" || value === "git" || value === "overlayfs") return value ?? "auto";
	throw new Error(`--workspace-driver must be auto, git, or overlayfs: ${value}`);
}

export function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

export async function waitUntil(condition: () => boolean, timeoutMs = 10_000, intervalMs = 10): Promise<void> {
	const deadline = performance.now() + timeoutMs;
	while (!condition()) {
		if (performance.now() >= deadline) throw new Error("timed out waiting for speculative process state");
		await delay(intervalMs);
	}
}
