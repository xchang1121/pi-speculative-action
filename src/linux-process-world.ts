import path from "node:path";
import { UNBOUNDED_ACTION_TOOLS } from "./action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import { UNRESTRICTED_PROCESS_EFFECTS } from "./effect-model.ts";
import {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
import { ProcessExecutionCoordinator } from "./process-execution.ts";
import { toolErrorSettlement, type ToolInvocation } from "./tool-settlement.ts";
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

/** Generic process world; eligibility follows action semantics and a structured process invocation. */
export function createLinuxProcessExecutionWorld(
	options: LinuxProcessExecutionWorldOptions,
): SpeculativeAgentExecutionWorld {
	const backend = options.backend ?? new LinuxProcessReuseBackend(options);
	const workspaceSandbox = options.workspaceSandbox ?? new WorkspaceSandboxService();
	const ownsWorkspaceSandbox = options.workspaceSandbox === undefined;
	const { gitBinary, driver, overlayfsBinary, fusermountBinary } = options;
	const workspaceOptions = { gitBinary, driver, overlayfsBinary, fusermountBinary };
	const roots = new Set<string>();
	const qualifiedDrivers = new Map<string, Awaited<ReturnType<WorkspaceSandboxService["qualify"]>>>();
	let backendChecked = false;
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
		storage: backend.storage,
		speculation: {
			capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			tools: UNBOUNDED_ACTION_TOOLS,
			fingerprint: async (request) => {
				backendChecked = true;
				const invocation = request.action ? processInvocation(request.action.executionContext) : undefined;
				if (request.action && !invocation) throw new Error("execution action has no process invocation");
				const [processFingerprint, workspaceFingerprint] = await Promise.all([
					backend.fingerprint(),
					invocation?.cwd
						? qualify(invocation.cwd).then((selected) => selected.fingerprint)
						: workspaceSandbox.fingerprint(workspaceOptions),
				]);
				return `${processFingerprint}:${workspaceFingerprint}`;
			},
			diagnostics: async ({ cwd, refresh }) => {
				if (!refresh && !backendChecked)
					return { state: "registered" as const, detail: "Checked on first process fork" };
				backendChecked = true;
				const [status, store] = await Promise.all([backend.check(refresh), backend.store.stats(refresh)]);
				const storage = {
					entries: store.certificates,
					maxEntries: backend.store.limits.maxCertificates,
					bytes: store.totalBytes,
					maxBytes: backend.store.limits.maxBytes,
					orphanArtifacts: store.orphanArtifacts,
					overBudget: store.overBudget,
				};
				if (status.state !== "ready") return { state: "unavailable" as const, detail: status.detail, storage };
				const selected = qualifiedDrivers.get(path.resolve(cwd));
				return {
					state: "ready" as const,
					detail: selected
						? `${status.detail}; ${selected.driver} workspace driver selected`
						: `${status.detail}; workspace route not prepared yet`,
					storage,
				};
			},
			prepare: async ({ cwd, signal }) => {
				backendChecked = true;
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
			execute: async (context) => {
			const invocation = processInvocation(context.action.executionContext);
			if (!invocation) throw new Error("execution action has no process invocation");
			const sourceRoot = path.resolve(context.cwd);
			roots.add(sourceRoot);
			const selected = await qualify(sourceRoot);
			let session: LinuxProcessSession | undefined;
			try {
				return await workspaceSandbox.fork({
				cwd: sourceRoot,
				action: context.action,
				...(context.parentCheckpoint ? { parentCheckpoint: context.parentCheckpoint } : {}),
				...workspaceOptions,
				driver: selected.driver,
				executionMetrics: () => (session ? { reuse: session.metrics() } : {}),
				validate: async () =>
					session
						? session.validate()
						: {
								status: "indeterminate",
								cause: { stage: "freshness", code: "process_evidence_missing" },
								metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" },
							},
				afterCapture: async (_workspace, capture) => {
					if (!session) throw new Error("process evidence sealer is missing");
					return session.seal(capture.changes);
				},
				execute: async (workspace) => {
					session = await backend.open({
						sourceRoot,
						workspace,
						invocation,
						...(context.executionScope ? { scope: context.executionScope } : {}),
						signal: context.signal,
					});
					const executor = session.executor;
					let launches = 0;
					try {
						const result = await options.coordinator.runWith(
							{
								execute: (request) => {
									launches++;
									return executor.execute(request);
								},
							},
							() => context.tool.execute(context.callID, context.args as never, context.signal),
						);
						if (launches === 0) throw new Error("process-backed tool bypassed the process execution outlet");
						// The virtual root already preserves logical paths. Output bytes are data, not paths to rewrite.
						return { result, isError: false };
					} catch (error) {
						return toolErrorSettlement(error);
					}
				},
				});
			} finally {
				await session?.close();
			}
			},
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
