import path from "node:path";
import type { SpeculativeAgentExecutionWorld, SpeculativeToolExecutionContext } from "./agent-execution-world.ts";
import { UNRESTRICTED_PROCESS_EFFECTS } from "./effect-model.ts";
import {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
import { ProcessExecutionCoordinator } from "./process-execution.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import { forkSandboxWorkspace, prepareSandboxWorkspace } from "./workspace-sandbox.ts";

export interface LinuxProcessExecutionWorldOptions extends LinuxProcessBackendOptions {
	readonly coordinator: ProcessExecutionCoordinator;
	readonly backend?: LinuxProcessReuseBackend;
	readonly gitBinary?: string;
}

/** Generic process world; eligibility follows invocation/effect capabilities, never a tool name. */
export function createLinuxProcessExecutionWorld(
	options: LinuxProcessExecutionWorldOptions,
): SpeculativeAgentExecutionWorld {
	const backend = options.backend ?? new LinuxProcessReuseBackend(options);
	const roots = new Set<string>();
	return {
		id: "linux_process_reuse",
		scope: "runtime",
		isolation: "runtime_sandbox",
		capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
		fingerprint: () => backend.fingerprint(),
		prepare: async ({ cwd, signal }) => {
			const status = await backend.check();
			if (status.state !== "ready") throw new Error(status.detail);
			roots.add(path.resolve(cwd));
			await prepareSandboxWorkspace(cwd, {
				...(options.gitBinary ? { gitBinary: options.gitBinary } : {}),
				...(signal ? { signal } : {}),
			});
		},
		fork: async (context) => {
			const invocation = processInvocation(context.action.executionContext);
			if (!invocation) throw new Error("execution action has no process invocation");
			const sourceRoot = path.resolve(context.cwd);
			roots.add(sourceRoot);
			let validate: LinuxProcessSession["validate"] | undefined;
			let seal: LinuxProcessSession["seal"] | undefined;
			return forkSandboxWorkspace({
				cwd: sourceRoot,
				action: context.action,
				...(context.parentCheckpoint ? { parentCheckpoint: context.parentCheckpoint } : {}),
				...(options.gitBinary ? { gitBinary: options.gitBinary } : {}),
				validate: async () =>
					validate
						? validate()
						: {
								status: "indeterminate",
								cause: { stage: "freshness", code: "process_evidence_missing" },
								metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" },
							},
				afterCapture: async (_workspace, capture) => {
					if (!seal) throw new Error("process evidence sealer is missing");
					return seal(capture.changes);
				},
				execute: async (workspace) => {
					const session = await backend.open({
						sourceRoot,
						workspace,
						invocation,
						signal: context.signal,
					});
					validate = session.validate;
					seal = session.seal;
					let launches = 0;
					try {
						const result = await options.coordinator.runWith(
							{
								execute: (request) => {
									launches++;
									return session.executor.execute(request);
								},
							},
							() => context.tool.execute(context.callID, context.args as never, context.signal),
						);
						if (launches === 0) throw new Error("process-backed tool bypassed the process execution outlet");
						return {
							result: replacePhysicalPaths(result, workspace.sandboxRoot, sourceRoot),
							isError: false,
						};
					} catch (error) {
						return errorSettlement(replaceMessagePath(error, workspace.sandboxRoot, sourceRoot));
					} finally {
						await session.close();
					}
				},
			});
		},
		dispose: async () => {
			roots.clear();
			await backend.dispose();
		},
	};
}

function processInvocation(value: unknown): ToolInvocation["process"] | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const processValue = (value as ToolInvocation).process;
	if (
		!processValue ||
		typeof processValue.command !== "string" ||
		typeof processValue.cwd !== "string" ||
		typeof processValue.shell !== "string" ||
		!Array.isArray(processValue.shellArgs) ||
		(processValue.commandTransport !== "argv" && processValue.commandTransport !== "stdin")
	) {
		return undefined;
	}
	return processValue;
}

function errorSettlement(error: unknown): ToolSettlement {
	return {
		result: {
			content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }],
			details: {},
		},
		isError: true,
	};
}

function replaceMessagePath(error: unknown, from: string, to: string): unknown {
	if (!(error instanceof Error)) return error;
	const replacement = new Error(error.message.split(from).join(to), { cause: error.cause });
	replacement.name = error.name;
	return replacement;
}

function replacePhysicalPaths<Value>(value: Value, from: string, to: string): Value {
	if (typeof value === "string") return value.split(from).join(to) as Value;
	if (Array.isArray(value)) return value.map((item) => replacePhysicalPaths(item, from, to)) as Value;
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>).map(([key, child]) => [key, replacePhysicalPaths(child, from, to)]),
	) as Value;
}
