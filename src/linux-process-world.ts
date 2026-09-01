import path from "node:path";
import type { SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import { UNRESTRICTED_PROCESS_EFFECTS } from "./effect-model.ts";
import {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessReuseMetrics,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
import { ProcessExecutionCoordinator } from "./process-execution.ts";
import type { ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import {
	WorkspaceSandboxService,
	type WorkspaceSandboxOptions,
} from "./workspace-sandbox.ts";

export interface LinuxProcessExecutionWorldOptions extends LinuxProcessBackendOptions, WorkspaceSandboxOptions {
	readonly coordinator: ProcessExecutionCoordinator;
	readonly backend?: LinuxProcessReuseBackend;
	/** Shared explicitly with sibling execution worlds when they belong to one host lifecycle. */
	readonly workspaceSandbox?: WorkspaceSandboxService;
}

/** Generic process world; eligibility follows invocation/effect capabilities, never a tool name. */
export function createLinuxProcessExecutionWorld(
	options: LinuxProcessExecutionWorldOptions,
): SpeculativeAgentExecutionWorld {
	const backend = options.backend ?? new LinuxProcessReuseBackend(options);
	const workspaceSandbox = options.workspaceSandbox ?? new WorkspaceSandboxService();
	const ownsWorkspaceSandbox = options.workspaceSandbox === undefined;
	const workspaceOptions = pickWorkspaceOptions(options);
	const roots = new Set<string>();
	const qualifiedDrivers = new Map<string, Awaited<ReturnType<WorkspaceSandboxService["qualify"]>>>();
	const qualify = async (sourceRoot: string) => {
		const root = path.resolve(sourceRoot);
		const selected = await workspaceSandbox.qualify(workspaceOptions, root);
		qualifiedDrivers.set(root, selected);
		return selected;
	};
	return {
		id: "linux_process_reuse",
		scope: "runtime",
		isolation: "runtime_sandbox",
		capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
		storage: {
			configure: (limits) => backend.configureStorage(limits),
			maintain: (operation) => backend.maintainStorage(operation),
		},
		fingerprint: async (request) => {
			const invocation = request.action ? processInvocation(request.action.executionContext) : undefined;
			const [processFingerprint, workspaceFingerprint] = await Promise.all([
				backend.fingerprint(),
				invocation?.cwd
					? qualify(invocation.cwd).then((selected) => selected.fingerprint)
					: workspaceSandbox.fingerprint(workspaceOptions),
			]);
			return `${processFingerprint}:${workspaceFingerprint}`;
		},
		diagnostics: async ({ cwd, refresh }) => {
			const [status, store] = await Promise.all([backend.check(refresh), backend.store.stats()]);
			const storage = {
				entries: store.certificates,
				maxEntries: store.limits.maxCertificates,
				bytes: store.totalBytes,
				maxBytes: store.limits.maxBytes,
				orphanArtifacts: store.orphanArtifacts,
				overBudget: store.overBudget,
			};
			if (status.state !== "ready") return { state: "unavailable", detail: status.detail, storage };
			const selected = qualifiedDrivers.get(path.resolve(cwd));
			return {
				state: "ready",
				detail: selected
					? `${status.detail}; ${selected.driver} workspace driver selected`
					: `${status.detail}; workspace route not prepared yet`,
				storage,
			};
		},
		prepare: async ({ cwd, signal }) => {
			const status = await backend.check();
			if (status.state !== "ready") throw new Error(status.detail);
			roots.add(path.resolve(cwd));
			const selected = await qualify(cwd);
			await workspaceSandbox.prepare(cwd, {
				...workspaceOptions,
				driver: selected.driver,
				...(signal ? { signal } : {}),
			});
		},
		fork: async (context) => {
			const invocation = processInvocation(context.action.executionContext);
			if (!invocation) throw new Error("execution action has no process invocation");
			const sourceRoot = path.resolve(context.cwd);
			roots.add(sourceRoot);
			const selected = await qualify(sourceRoot);
			let validate: LinuxProcessSession["validate"] | undefined;
			let seal: LinuxProcessSession["seal"] | undefined;
			let reuseMetrics: LinuxProcessReuseMetrics | undefined;
			return workspaceSandbox.fork({
				cwd: sourceRoot,
				action: context.action,
				...(context.parentCheckpoint ? { parentCheckpoint: context.parentCheckpoint } : {}),
				...workspaceOptions,
				driver: selected.driver,
				executionMetrics: () => (reuseMetrics ? { reuse: reuseMetrics } : {}),
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
						...(context.executionScope ? { scope: context.executionScope } : {}),
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
						reuseMetrics = session.metrics();
					}
				},
			});
		},
		dispose: async () => {
			const ownedRoots = [...roots];
			roots.clear();
			qualifiedDrivers.clear();
			try {
				await backend.dispose();
			} finally {
				if (ownsWorkspaceSandbox) await workspaceSandbox.dispose();
				else await workspaceSandbox.closePools(ownedRoots);
			}
		},
	};
}

function pickWorkspaceOptions(options: LinuxProcessExecutionWorldOptions): WorkspaceSandboxOptions {
	return {
		...(options.gitBinary ? { gitBinary: options.gitBinary } : {}),
		...(options.driver ? { driver: options.driver } : {}),
		...(options.overlayfsBinary ? { overlayfsBinary: options.overlayfsBinary } : {}),
		...(options.fusermountBinary ? { fusermountBinary: options.fusermountBinary } : {}),
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
