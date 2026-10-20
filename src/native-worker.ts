import { AsyncResource } from "node:async_hooks";
import { parentPort, type MessagePort } from "node:worker_threads";
import { createNativeRpc, ownsNativeProcesses, type NativeRpcContext } from "./native-owner.ts";
import { TimelineInterval, type ComputationProducer } from "./task-timing.ts";
import type { ExecutionOperationAdoption, ExecutionScope } from "./execution-world.ts";
import { ProcessHandoffOwnership, type ProcessHandoffLender } from "./process-handoff.ts";
import { LinuxProcessReuseBackend, observeWorkspaceWrites, type LinuxProcessBackendOptions, type LinuxProcessSession } from "./linux-process-backend.ts";
import { restoreWorkspaceObservation, type SandboxWorkspaceContext, type SandboxWorkspaceObservation } from "./workspace-sandbox.ts";
import type { WorkspaceTransactionCapture, WorkspaceTransactionOwnership } from "./workspace-transaction.ts";
import type { ToolProcessInvocation, ToolSettlement } from "./tool-settlement.ts";
import type { ProcessExecutionRequest } from "./process-execution.ts";
import { serializeNativeDependency, type SynchronousProcessRequest, type SynchronousProcessResult, type SerializedProcessBinding } from "./linux-process-sync.ts";

// Composition stays in the worker entry: clients never import the services they delegate to.
type NativeSessionInput = Parameters<LinuxProcessReuseBackend["open"]>[0];
type SessionSnapshot = Pick<SynchronousProcessResult, "bindings" | "computations" | "metrics">;

async function serveWorkspaceObservation(port: MessagePort, clock: Int32Array, options: SandboxWorkspaceObservation, workspaces: Map<string, SandboxWorkspaceContext>) {
	const observation = Promise.resolve().then(() => restoreWorkspaceObservation(options, () => rpc.call("hints"))), captures = new Map<number, WorkspaceTransactionCapture>(); let sequence = 0;
	const rpc = createNativeRpc(port, clock, async (method, args: { id: number; resource: string; maxBytes: number; owned: boolean }): Promise<unknown> => {
		const { workspace, dispose } = await observation;
		if (method === "ready") { workspaces.set(options.id, workspace); return options.id; }
		if (method === "begin") { const id = ++sequence; captures.set(id, await workspace.transactions.begin()); return id; }
		if (method === "writes") { return observeWorkspaceWrites(workspace, args as unknown as Parameters<typeof observeWorkspaceWrites>[1]); }
		if (method === "dispose") return dispose();
		if (method === "close") { workspaces.delete(options.id); rpc.fail(new Error("workspace observation closed")); port.close(); return; }
		const capture = captures.get(args.id);
		if (method === "readBefore") { if (!capture?.readBefore) throw new Error("workspace interval input unavailable"); return capture.readBefore(args.resource, args.maxBytes); }
		captures.delete(args.id);
		if (method === "abort") return capture?.abort();
		if (method === "finish") return capture ? capture.finish(args.owned ? async () => {
			const proof = await rpc.call<Omit<WorkspaceTransactionOwnership, "interfered"> | undefined>("ownership", { id: args.id });
			return proof && { ...proof, interfered: (paths, until) => rpc.call("interfered", { id: args.id, paths, until }) };
		} : undefined) : { complete: false, changes: [], reason: "transaction_already_settled" };
		throw new Error(`unknown workspace observation operation: ${method}`);
	});
	void observation.catch(error => { rpc.fail(error instanceof Error ? error : new Error(String(error))); port.close(); });
}

async function serveNativeBackend(port: MessagePort, clock: Int32Array, options: LinuxProcessBackendOptions,
	workspaces: ReadonlyMap<string, SandboxWorkspaceContext>) {
	let backend: LinuxProcessReuseBackend | undefined;
	const observations = new Map<number, { readonly finish: () => void; release?: () => void | Promise<void> }>();
	const grants = new Map<number, { readonly scope?: ExecutionScope; readonly owners?: readonly number[] }>();
	const works = new Map<number, { readonly controller: AbortController; session?: LinuxProcessSession; available?: () => boolean;
		dispatch?: (operation: string, value: never, context: NativeRpcContext) => Promise<unknown>; readonly apply: () => Promise<void> }>();
	const retained = new Map<number, { readonly ownership: WeakRef<ProcessHandoffOwnership>; readonly adopted: (adoption: ExecutionOperationAdoption) => void;
		readonly accept: (scope: ExecutionScope) => boolean }>();
	try {
		backend = new LinuxProcessReuseBackend(options);
		const rpc = createNativeRpc(port, clock, async (method, args: unknown, context) => {
			if (method === "observe") return observe(args as { id: number; request: SynchronousProcessRequest }, context.attached);
			if (method === "open") {
				const { id, workspace, producer, ...input } = args as { id: number; workspace: string; producer?: ComputationProducer } & Omit<NativeSessionInput, "workspace" | "invocation"> & { invocation: ToolProcessInvocation | SerializedProcessBinding };
				const sandbox = workspaces.get(workspace); if (!sandbox) throw new Error("native workspace owner retired");
				const controller = new AbortController(), cancel = () => controller.abort(context.signal.reason);
				const entry = { controller, apply: () => rpc.call<void>("apply", { id }) };
				works.set(id, entry); context.signal.addEventListener("abort", cancel, { once: true });
				try {
					const invocation = "invocation" in input.invocation ? backend!.restoreBinding(input.invocation) : input.invocation;
					if (!invocation) throw new Error("native work binding expired");
					const { output: session } = await TimelineInterval.collect(() => openSession(id, { ...input, workspace: sandbox, invocation }), producer);
					return session.ownership.authority.buffer;
				} finally { context.signal.removeEventListener("abort", cancel); }
			}
			if (method === "session") {
				const { id, operation, value } = args as { id: number; operation: string; value: never }, dispatch = works.get(id)?.dispatch;
				if (!dispatch) throw new Error("native work retired"); return dispatch(operation, value, context);
			}
			if (method === "snapshot") return sessionSnapshot(args as number);
			if (method === "cancel") { works.get(args as number)?.controller?.abort(new Error("native work cancelled")); return; }
			if (method === "release") { const entry = works.get(args as number); works.delete(args as number); entry?.controller?.abort(new Error("native work retired")); return entry?.session?.close(); }
			if (method === "commit") {
				const entry = works.get(args as number); if (!entry?.session) throw new Error("native work retired");
				return entry.session.ownership.commit(() => entry.apply());
			}
			if (method === "stale") { const binding = backend!.restoreBinding(args as SerializedProcessBinding); return !binding || backend!.bindingStale(binding); }
			if (method === "configure") { backend!.storage.configure(args as Parameters<NonNullable<typeof backend>["storage"]["configure"]>[0]); return; }
			if (method === "maintain") return backend!.storage.maintain(args as Parameters<NonNullable<typeof backend>["storage"]["maintain"]>[0]);
			if (method === "reset") return backend!.resetNativeObservation();
			if (method === "joinable") return works.get(args as number)?.available?.() ?? false;
			if (method === "finished") { observations.get(args as number)?.finish(); return; }
			if (method === "settled") { void observations.get(args as number)?.release?.(); observations.delete(args as number); return; }
			if (method === "close") {
				for (const observation of observations.values()) { observation.finish(); void observation.release?.(); }
				rpc.fail(new Error("native process owner closing"));
				for (const entry of works.values()) entry.controller.abort(new Error("native process owner closing"));
				return Promise.allSettled([...works.values()].map(entry => entry.session?.close())).then(() => backend!.dispose()).finally(() => port.close());
			}
			throw new Error(`unknown native work operation: ${method}`);
		}, () => {
			for (const [id, owner] of retained) if (!owner.ownership.deref()) retained.delete(id);
			return { live: backend!.hasReusableWork, metrics: backend!.producerMetrics() };
		});
		const sessionSnapshot = (id: number): SessionSnapshot => {
			const session = works.get(id)?.session; if (!session) throw new Error("native work retired");
			return { metrics: session.metrics(), bindings: session.executionBindings().flatMap(binding => backend!.serializeBinding(binding) ?? []),
				computations: session.computationDependencies().flatMap(dependency => serializeNativeDependency(dependency) ?? []) };
		};
		const openSession = async (id: number, input: NativeSessionInput) => {
			const entry = works.get(id)!;
			const adopted = (adoption: ExecutionOperationAdoption) => backend!.recordWorkerAdoption(id, adoption);
			const accept = (scope: ExecutionScope) => [...grants.values()].some(grant => grant.scope?.sessionID === scope.sessionID &&
				grant.scope.turnID === scope.turnID && grant.scope.callID === scope.callID && grant.owners?.includes(id));
			const session = entry.session = await backend!.open({ ...input, signal: entry.controller.signal, onOperationAdopted: adopted, acceptOperationScope: accept,
				onOperationJoinable: available => { entry.available = available; } });
			if (entry.controller.signal.aborted) { await session.close(); entry.controller.signal.throwIfAborted(); }
			retained.set(id, { ownership: new WeakRef(session.ownership), adopted, accept });
			entry.dispatch = AsyncResource.bind(async (operation: string, value: unknown, context: NativeRpcContext) => {
				if (operation === "execute") return session.executor.execute({ ...value as ProcessExecutionRequest, ...context });
				if (operation === "binding") { const binding = backend!.restoreBinding(value as SerializedProcessBinding); if (!binding) throw new Error("native work binding expired"); return session.executeBinding(binding); }
				if (operation === "seal") return session.seal(value as Parameters<LinuxProcessSession["seal"]>[0]);
				if (operation === "validate" || operation === "close") return session[operation]();
				throw new Error(`unknown native session operation: ${operation}`);
			});
			return session;
		};
		const observe = async ({ id, request }: { id: number; request: SynchronousProcessRequest }, attached: NativeRpcContext["attached"]) => {
			const finished = new Promise<void>(finish => { observations.set(id, { finish }); }); grants.set(id, request);
			try {
				const completed = await backend!.observeSynchronous(request, attached, finished,
					request.asynchronous ? delta => rpc.call<ToolSettlement>("effects", { id, delta }) : undefined,
					request.asynchronous ? (key, excluded, signal) => rpc.call<Awaited<ReturnType<ProcessHandoffLender>>>("borrow", { id, key, excluded }, signal) : undefined);
				observations.get(id)!.release = completed.release; return completed.result;
			} finally { grants.delete(id); }
		};
	} catch {
		await backend?.dispose(); port.close();
	}
}

if (ownsNativeProcesses) {
	const workspaces = new Map<string, SandboxWorkspaceContext>();
	parentPort!.on("message", ({ kind, options, port, shutdown, shared }: { kind?: "backend" | "workspace"; options: never; port?: MessagePort; shutdown?: boolean; shared: SharedArrayBuffer }) => {
		if (shutdown) { parentPort!.close(); return; }
		if (port && kind === "backend") void serveNativeBackend(port, new Int32Array(shared), options, workspaces);
		if (port && kind === "workspace") void serveWorkspaceObservation(port, new Int32Array(shared), options, workspaces);
	});
}
