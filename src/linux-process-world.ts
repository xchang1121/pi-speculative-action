import path from "node:path";
import type { ActionKey } from "./action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "./agent-execution-world.ts";
import { UNRESTRICTED_PROCESS_EFFECTS } from "./effect-model.ts";
import { LinuxProcessReuseBackend, type LinuxProcessBackendOptions } from "./linux-process-backend.ts";
import { restoreNativeDependency } from "./linux-process-sync.ts";
import { ProcessExecutionCoordinator } from "./process-execution.ts";
import type { ProcessExecutionBinding } from "./process-handoff.ts";
import type { ExecutionOperationBinding } from "./execution-world.ts";
import type { ToolInvocation } from "./tool-settlement.ts";
import { WorkspaceSandboxService, type WorkspaceSandboxOptions } from "./workspace-sandbox.ts";

export interface LinuxProcessExecutionWorldOptions extends LinuxProcessBackendOptions, WorkspaceSandboxOptions {
	readonly coordinator: ProcessExecutionCoordinator;
	/** Tools whose complete process outlet is bound by this host, not all process-shaped effect contracts. */
	readonly tools: readonly string[];
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
	// Processes observe their inputs' times and inodes: an overlay over the workspace itself shows the Actor's own.
	const workspaceOptions = { gitBinary, driver, overlayfsBinary, fusermountBinary, liveLower: true };
	const operations = new WeakMap<ExecutionOperationBinding, { readonly binding: WeakRef<ProcessExecutionBinding>; readonly permissionKey: string }>();
	const describeOperation = (binding: ProcessExecutionBinding, permission: ActionKey) => {
		const reference = new WeakRef(binding);
		const descriptor = Object.freeze({ backend: "linux_process_reuse", identity: binding.key, permissionHash: permission.hash,
			semantics: Object.freeze({ effect: "unbounded" as const, requirements: UNRESTRICTED_PROCESS_EFFECTS }), ...backend.operationHints(binding),
			get executionMs() { return reference.deref()?.executionMs ?? 0; },
			get available() { return reference.deref()?.available ?? false; },
			stale: () => { const current = reference.deref(); return current ? backend.bindingStale(current) : Promise.resolve(true); } });
		operations.set(descriptor, { binding: reference, permissionKey: permission.key });
		return descriptor;
	};
	const operationFor = (action: ActionKey) => {
		const operation = (action.executionContext as ToolInvocation | undefined)?.operation;
		if (!operation) return undefined;
		const issued = operations.get(operation.binding), binding = issued?.binding.deref();
		if (!binding?.available || operation.permission.key !== issued?.permissionKey) throw new Error("internal process binding is unavailable");
		return binding;
	};
	const qualifiedDrivers = new Map<string, Awaited<ReturnType<WorkspaceSandboxService["qualify"]>>>();
	let backendChecked = false;
	const qualify = async (sourceRoot: string) => {
		const root = path.resolve(sourceRoot);
		const selected = await backend.processOwner().call<Awaited<ReturnType<WorkspaceSandboxService["qualify"]>>>("qualify", { options: workspaceOptions, cwd: root });
		qualifiedDrivers.set(root, selected);
		return selected;
	};
	return {
		id: "linux_process_reuse",
		scope: "runtime",
		isolation: "runtime_sandbox",
		storage: backend.storage,
		observeOperations: ({ action, scope, learn, inputs, cwd, signal, enabled }, execute, observe) => backend.observeBindings(scope,
			() => enabled && cwd ? backend.observeNative(cwd, scope, signal, execute) : execute(), (bindings, computations) => {
			observe(action ? bindings.map(binding => describeOperation(binding, action)) : [], computations);
		}, learn, inputs),
		speculation: {
			capabilities: UNRESTRICTED_PROCESS_EFFECTS.capabilities,
			acceptsCheckpoint: (checkpoint, cwd) => workspaceSandbox.acceptsCheckpoint(checkpoint, cwd),
			fingerprint: async (request) => {
				backendChecked = true;
				const { action } = request, tool = action?.tool ?? request.tool, invocation = action && processInvocation(action.executionContext);
				const operation = action && operationFor(action), cwd = operation ? backend.bindingWorkspace(operation) : invocation?.cwd;
				if (action && !operation && !invocation) throw new Error("execution action has no process invocation");
				if (tool && !operation && !options.tools.includes(tool))
					throw new Error("Complete host process outlet is not bound");
				const [processFingerprint, workspaceFingerprint] = await Promise.all([
					backend.fingerprint(),
					cwd ? qualify(cwd).then((selected) => selected.fingerprint) : backend.processOwner().call<string>("fingerprint", workspaceOptions),
				]);
				return `${processFingerprint}:${workspaceFingerprint}`;
			},
			diagnostics: async ({ cwd, refresh }) => {
				if (!refresh && !backendChecked) return { state: "registered" as const, detail: "Checked on first process fork" };
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
				const selected = await qualify(cwd);
				const prepared = await backend.processOwner().call<Awaited<ReturnType<WorkspaceSandboxService["prepare"]>>>("prepare", { cwd, options: { ...workspaceOptions, driver: selected.driver } }, signal);
				qualifiedDrivers.set(path.resolve(cwd), prepared);
			},
			execute: async (context) => {
			const operation = operationFor(context.action), invocation = operation ?? processInvocation(context.action.executionContext);
			if (!invocation) throw new Error("execution action has no process invocation");
			const sourceRoot = path.resolve(context.cwd);
			const selected = operation && qualifiedDrivers.get(sourceRoot) || await qualify(sourceRoot);
			const parent = workspaceSandbox.checkpoint(context.parentCheckpoint, sourceRoot);
			const captured = await backend.processOwner().run({ cwd: sourceRoot, parent, scope: context.executionScope,
				invocation: operation ? backend.serializeBinding(operation)! : invocation as NonNullable<ToolInvocation["process"]>, options: { ...workspaceOptions, driver: selected.driver } },
				context.signal, async executor => ({ result: await options.coordinator.runWith(executor,
					() => context.tool.execute(context.callID, context.args as never, context.signal)), isError: false }), context);
			const branch = workspaceSandbox.branch({ cwd: sourceRoot, action: context.action, parentCheckpoint: context.parentCheckpoint, validate: captured.validate }, captured.snapshot);
			const commit = branch.commit.bind(branch), dispose = branch.dispose?.bind(branch);
			return Object.assign(branch, {
				computationDependencies: captured.computations.flatMap(dependency => restoreNativeDependency(dependency) ?? []),
				operations: Object.freeze(captured.bindings.flatMap(entry => { const binding = backend.restoreBinding(entry); return binding ? [describeOperation(binding, context.action)] : []; })),
				commit: () => { if (operation) throw new Error("internal process output cannot commit an enclosing tool"); return captured.commit(commit); },
				dispose: async () => { try { await dispose?.(); } finally { await captured.dispose(); } },
			});
			},
		},
		dispose: async () => {
			qualifiedDrivers.clear();
			try {
				await backend.dispose();
			} finally {
				if (ownsWorkspaceSandbox) await workspaceSandbox.dispose();
			}
		},
	};
}

function processInvocation(value: unknown): ToolInvocation["process"] | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const processValue = (value as ToolInvocation).process;
	if (
		!processValue || typeof processValue.command !== "string" || typeof processValue.cwd !== "string" || typeof processValue.shell !== "string" ||
		!Array.isArray(processValue.shellArgs) ||
		(processValue.commandTransport !== "argv" && processValue.commandTransport !== "stdin")
	) {
		return undefined;
	}
	return processValue;
}
