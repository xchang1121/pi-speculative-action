import { AsyncResource } from "node:async_hooks";
import { MessageChannel, type MessagePort, type Transferable, Worker, parentPort, receiveMessageOnPort, workerData } from "node:worker_threads";
import { TimelineInterval, type ComputationProducer, type SerializedTimelineComputation, type TimelineDependency } from "./task-timing.ts";
import { emptyWorldReuseMetrics, type ExecutionOperationAdoption, type ExecutionScope, type WorldReuseMetrics } from "./execution-world.ts";
import { ProcessHandoffOwnership, type ProcessHandoffTransfer, type ProcessHandoffLender } from "./process-handoff.ts";
import type { LinuxProcessBackendOptions, LinuxProcessReuseBackend, LinuxProcessSession, NativeProcessInvocation } from "./linux-process-backend.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import type { CapturedFilesystemTransfer } from "./filesystem-evidence.ts";
import type { WorkspaceSandboxService, SandboxExecutionDelta,
	SandboxWorkspaceContext, SandboxWorkspaceObservation } from "./workspace-sandbox.ts";
import type { WorkspaceTransactionCapture, WorkspaceTransactionDriver, WorkspaceTransactionOwnership } from "./workspace-transaction.ts";
import { type ToolProcessInvocation, type ToolSettlement } from "./tool-settlement.ts";
import type { ProcessExecutionRequest } from "./process-execution.ts";

type SerializedDependency = Omit<TimelineDependency, "computation" | "shared"> & {
	readonly computation: SerializedTimelineComputation;
	readonly shared?: readonly SerializedTimelineComputation[];
};
export interface SynchronousProcessRequest {
	readonly gate: number; readonly sourceRoot: string; readonly scope?: ExecutionScope;
	readonly learn: boolean; readonly descriptors?: boolean | "inspect"; readonly skipRoot?: boolean;
	readonly handoffs: ProcessHandoffTransfer;
	readonly inputs?: readonly CapturedFilesystemTransfer[];
	readonly owners?: readonly number[];
	readonly claims: SharedArrayBuffer;
	readonly ordering?: SharedArrayBuffer;
	readonly asynchronous?: true;
	readonly lender?: SharedArrayBuffer;
}
export interface SerializedProcessBinding {
	readonly sequence: number; readonly key: string; readonly executionMs: number; readonly invocation: NativeProcessInvocation; readonly scope: ExecutionScope;
}
export interface SynchronousProcessResult {
	readonly bindings: readonly SerializedProcessBinding[];
	readonly computations: readonly SerializedDependency[];
	readonly adoptions: readonly { readonly owner: { readonly loan: number } | { readonly work: number };
		readonly adoption: Omit<ExecutionOperationAdoption, "computation"> & { readonly computation?: SerializedDependency } }[];
	readonly metrics: WorldReuseMetrics;
	readonly inputs?: readonly CapturedFilesystemTransfer[];
}
type NativeSessionInput = Parameters<LinuxProcessReuseBackend["open"]>[0];
type SessionSnapshot = { readonly bindings: readonly SerializedProcessBinding[]; readonly computations: readonly SerializedDependency[]; readonly metrics: WorldReuseMetrics };
const retiredSessions = new FinalizationRegistry<{ readonly owner: WeakRef<SynchronousProcessObserver>; readonly id: number; readonly signal?: AbortSignal; readonly abort: () => void }>(({ owner, id, signal, abort }) => {
	signal?.removeEventListener("abort", abort);
	void owner.deref()?.call("release", id).catch(() => undefined);
});
type WorkerState = { readonly live: boolean; readonly metrics: WorldReuseMetrics };
type RemoteError = { readonly message: string; readonly disposition?: "recoverable" | "poisoned" };
const remoteError = (error: unknown): RemoteError => ({ message: error instanceof Error ? error.message : String(error),
	...(error instanceof EffectCommitFailure ? { disposition: error.disposition } : {}) });
const restoreError = (error: RemoteError) => error.disposition ? effectCommitFailure(new Error(error.message), error.disposition) : new Error(error.message);

type RpcResponse = { readonly data: Uint8Array } | { readonly attached: number | undefined } |
	{ readonly value?: unknown; readonly error?: RemoteError; readonly state?: WorkerState };
type RpcContext = { readonly signal: AbortSignal; readonly onData: (data: Buffer) => void; readonly attached: (pid?: number) => void };

/** A request owns its reply channel; synchronous reads never dispatch the caller's callbacks. */
function createProcessRpc(port: MessagePort, clock: Int32Array,
	invoke: (method: string, args: never, context: RpcContext) => unknown,
	state?: () => WorkerState, observed?: (state: WorkerState) => void) {
	const channels = new Set<MessagePort>();
	let failure: Error | undefined;
	port.on("message", ({ method, args, reply }: { method: string; args: never; reply?: MessagePort }) => {
		const controller = new AbortController(), cancel = () => controller.abort(new Error("native work cancelled"));
		if (reply) { track(reply); reply.on("message", cancel); reply.once("close", cancel); }
		const send = (response: RpcResponse) => { reply?.postMessage(response); Atomics.add(clock, 0, 1); Atomics.notify(clock, 0); };
		void Promise.resolve().then(() => { if (failure) throw failure;
			return invoke(method, args, { signal: controller.signal, onData: data => send({ data }), attached: pid => send({ attached: pid }) }); })
			.then(value => send({ value, state: state?.() })).catch(error => send({ error: remoteError(error), state: state?.() })).finally(() => reply?.close());
	});
	function notify(method: string, args?: unknown): void { port.postMessage({ method, args }); }
	function call<Value>(method: string, args?: unknown, signal?: AbortSignal, onData?: (data: Buffer) => void): Promise<Value> {
		try { return request<Value>(method, args, signal, onData).completion; } catch (error) { return Promise.reject(error); }
	}
	function request<Value>(method: string, args?: unknown, signal?: AbortSignal, onData?: (data: Buffer) => void, transfers: readonly Transferable[] = []) {
		if (failure) throw failure; signal?.throwIfAborted();
		const { port1, port2 } = new MessageChannel(), cancel = () => port1.postMessage(undefined); track(port1);
		let failed: { error: unknown } | undefined, settle!: (response: RpcResponse) => void;
		const result = new Promise<Value>((resolve, reject) => {
			settle = response => {
				if ("attached" in response) return;
				if ("data" in response) { try { if (!failed) onData?.(Buffer.from(response.data)); } catch (error) { failed = { error }; cancel(); } return; }
				if (response.state) observed?.(response.state);
				if (failed || response.error) reject(failed ? failed.error : restoreError(response.error!)); else resolve(response.value as Value);
			};
			port1.on("message", settle); port1.once("close", () => reject(failure ?? new Error("native work channel closed")));
			signal?.addEventListener("abort", cancel, { once: true });
			try { port.postMessage({ method, args, reply: port2 }, [port2, ...transfers]); } catch (error) { port2.close(); reject(error); }
		});
		const completion = result.finally(() => { signal?.removeEventListener("abort", cancel); port1.close(); });
		return { completion, receive: (): RpcResponse => {
			const deadline = performance.now() + 15_000;
			while (!failure && performance.now() < deadline) {
				const version = Atomics.load(clock, 0), response = receiveMessageOnPort(port1)?.message as RpcResponse | undefined;
				if (response) {
					if ("data" in response) throw new Error("native synchronous channel yielded output");
					settle(response); if ("error" in response && response.error) throw restoreError(response.error); return response;
				}
				if (Atomics.load(clock, 1)) break;
				Atomics.wait(clock, 0, version, Math.max(1, deadline - performance.now()));
			}
			throw failure ?? new Error("synchronous native observer unavailable");
		} };
	}
	function track(port: MessagePort): void { channels.add(port); port.once("close", () => channels.delete(port)); }
	function fail(error: Error): void { failure = error; Atomics.store(clock, 1, 1); Atomics.notify(clock, 0); for (const channel of channels) channel.close(); }
	return { notify, call, request, fail };
}

export const ownsNativeProcesses = workerData?.nativeProcessObserver === true;
let nativeOwner: { readonly worker: Worker; readonly clients: Set<(error: Error) => void>; readonly exited: Promise<void> } | undefined;

/** Backends keep separate registries; workspaces share one journal wherever those backends execute. */
function connectNativeOwner(kind: "backend" | "workspace", options: LinuxProcessBackendOptions | SandboxWorkspaceObservation,
	invoke: Parameters<typeof createProcessRpc>[2], observed?: Parameters<typeof createProcessRpc>[4]) {
	if (!nativeOwner) {
		const clients = new Set<(error: Error) => void>();
		const worker = new Worker(new URL(import.meta.url), { workerData: { nativeProcessObserver: true },
			execArgv: process.execArgv.filter(argument => !argument.startsWith("--input-type")) });
		const owner = nativeOwner = { worker, clients, exited: new Promise<void>(resolve => worker.once("exit", () => {
			for (const fail of clients) fail(new Error("native process owner exited"));
			if (nativeOwner === owner) nativeOwner = undefined; resolve();
		})) };
		worker.on("error", error => { for (const fail of clients) fail(error); }); worker.unref();
	}
	const owner = nativeOwner, clock = new Int32Array(new SharedArrayBuffer(8)), { port1: port, port2 } = new MessageChannel();
	const rpc = createProcessRpc(port, clock, invoke, undefined, observed); owner.clients.add(rpc.fail);
	const closed = new Promise<void>(resolve => port.once("close", () => {
		rpc.fail(new Error("native owner connection closed")); owner.clients.delete(rpc.fail);
		if (owner.clients.size) { resolve(); return; }
		if (nativeOwner === owner) nativeOwner = undefined;
		owner.worker.ref(); owner.worker.postMessage({ shutdown: true }); void owner.exited.then(resolve);
	}));
	try { owner.worker.postMessage({ kind, options, shared: clock.buffer, port: port2 }, [port2]); port.unref(); }
	catch (error) { port2.close(); port.close(); throw error; }
	return { worker: owner.worker, rpc, closed, close: () => { port.ref(); rpc.notify("close"); return closed; } };
}

export function ownWorkspaceObservation(description: () => SandboxWorkspaceObservation): { readonly ready: () => Promise<string>;
	readonly call: <Value>(method: string, args?: unknown) => Promise<Value>; readonly transactions: WorkspaceTransactionDriver } {
	let connection: ReturnType<typeof connectNativeOwner> | undefined, initialized: Promise<string> | undefined, disposal: Promise<void> | undefined;
	const owners = new Map<number, { readonly get: () => Promise<WorkspaceTransactionOwnership | undefined>; proof?: WorkspaceTransactionOwnership }>();
	const ready = (): Promise<string> => {
		if (disposal) return Promise.reject(new Error("workspace transaction driver is disposed"));
		if (!connection) connection = connectNativeOwner("workspace", description(), async (method, args: { id: number; paths: ReadonlySet<string>; until: number }) => {
			if (method === "hints") return description().hints;
			const owner = owners.get(args.id); if (!owner) throw new Error("workspace interval owner retired");
			if (method === "ownership") { const proof = owner.proof = await owner.get(); return proof && { ...proof, interfered: undefined }; }
			if (method === "interfered" && owner.proof) return owner.proof.interfered(args.paths, args.until);
			throw new Error("workspace interval ownership unavailable");
		});
		return initialized ??= connection.rpc.call<string>("ready");
	};
	const call = async <Value>(method: string, args?: unknown): Promise<Value> => { await ready(); return connection!.rpc.call<Value>(method, args); };
	return { ready, call, transactions: {
		begin: async () => {
			const id = await call<number>("begin"); let settled = false;
			return { readBefore: (resource, maxBytes) => call("readBefore", { id, resource, maxBytes }), abort: async () => { if (!settled) { settled = true; await call("abort", { id }); } },
				finish: async get => { if (settled) return { complete: false, changes: [], reason: "transaction_already_settled" }; settled = true; if (get) owners.set(id, { get });
					try { return await call("finish", { id, owned: !!get }); } finally { owners.delete(id); } } };
		},
		dispose: () => disposal ??= connection ? connection.rpc.call<void>("dispose").finally(() => connection!.close()) : Promise.resolve(),
	} };
}

async function serveWorkspaceObservation(port: MessagePort, clock: Int32Array, options: SandboxWorkspaceObservation, workspaces: Map<string, SandboxWorkspaceContext>) {
	const { restoreWorkspaceObservation } = await import("./workspace-sandbox.ts");
	const observation = Promise.resolve().then(() => restoreWorkspaceObservation(options, () => rpc.call("hints"))), captures = new Map<number, WorkspaceTransactionCapture>(); let sequence = 0;
	const rpc = createProcessRpc(port, clock, async (method, args: { id: number; resource: string; maxBytes: number; owned: boolean }) => {
		const { workspace, dispose } = await observation;
		if (method === "ready") { workspaces.set(options.id, workspace); return options.id; }
		if (method === "begin") { const id = ++sequence; captures.set(id, await workspace.transactions.begin()); return id; }
		if (method === "writes") { const { observeWorkspaceWrites } = await import("./linux-process-backend.ts"); return observeWorkspaceWrites(workspace, args as unknown as Parameters<typeof observeWorkspaceWrites>[1]); }
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

/** Preserve producer identities; shared intervals will be translated into the receiving caller's clock. */
export function serializeNativeDependency(dependency: TimelineDependency): SerializedDependency | undefined {
	const computation = TimelineInterval.serialize(dependency.computation), shared = dependency.shared?.map(TimelineInterval.serialize);
	return computation && (!shared || shared.every(span => span !== undefined)) ? { ...dependency, computation, shared } : undefined;
}
export function restoreNativeDependency(dependency: SerializedDependency): TimelineDependency | undefined {
	const computation = TimelineInterval.restore(dependency.computation), shared: TimelineInterval[] = [];
	let uncertain = dependency.computeUncertain;
	for (const graph of dependency.shared ?? []) {
		const restored = TimelineInterval.restore(graph), origin = graph.nodes.find(node => node.id === graph.root)?.timeOrigin;
		if (!restored) return;
		if (origin === undefined) { uncertain = true; continue; }
		// Shared time cuts the enclosing caller's clock; the original computation keeps its producer clock.
		const offset = origin - performance.timeOrigin;
		shared.push(new TimelineInterval(restored.startedAt + offset, restored.completedAt + offset));
	}
	return computation ? { ...dependency, computation, shared, ...(uncertain ? { computeUncertain: true } : {}) } : undefined;
}

/** Only this private channel is pumped; synchronous execution never dispatches caller callbacks. */
export class SynchronousProcessObserver {
	private readonly connection: ReturnType<typeof connectNativeOwner>;
	private closing?: Promise<void>;
	private failure?: Error;
	private sequence = 0;
	private get rpc(): ReturnType<typeof createProcessRpc> { return this.connection.rpc; }
	private readonly owners = new Map<number, { readonly adopted?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
		readonly accepts?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean> }>();
	private readonly callbacks = new Map<number, (method: string, args: never, context: RpcContext) => Promise<unknown>>();
	private state: WorkerState = { live: false, metrics: emptyWorldReuseMetrics() };

	constructor(options: LinuxProcessBackendOptions) {
		this.connection = connectNativeOwner("backend", options, (method, args: { id: number }, context) => {
			const callback = this.callbacks.get(args.id); if (!callback) throw new Error("native work callback retired"); return callback(method, args as never, context);
		}, state => {
			this.state = state;
			// A reply can carry adoption after consuming the last remote record; its local callback owns delivery.
			for (const [id, owner] of this.owners) if (!owner.adopted?.deref() && !owner.accepts?.deref()) this.owners.delete(id);
		});
		void this.connection.closed.then(() => { this.failure = new Error("native process owner closed"); });
	}

	get hasWork(): boolean { return this.state.live || this.callbacks.size > 0; }
	metrics(): WorldReuseMetrics { return this.state.metrics; }
	call<Value>(method: string, args?: unknown, signal?: AbortSignal): Promise<Value> { return this.rpc.call(method, args, signal); }
	grants(scope?: ExecutionScope): readonly number[] {
		return scope ? [...this.owners].filter(([, owner]) => owner.accepts?.deref()?.(scope, true)).map(([id]) => id) : [];
	}
	adopted(id: number, adoption: ExecutionOperationAdoption): void { this.owners.get(id)?.adopted?.deref()?.(adoption); }
	private retain(callbacks: Pick<NativeSessionInput, "onOperationAdopted" | "acceptOperationScope" | "onOperationJoinable" | "signal">): number {
		const id = ++this.sequence;
		this.owners.set(id, { adopted: callbacks.onOperationAdopted && new WeakRef(callbacks.onOperationAdopted),
			accepts: callbacks.acceptOperationScope && new WeakRef(callbacks.acceptOperationScope) });
		callbacks.onOperationJoinable?.(() => !callbacks.signal?.aborted && !this.failure && !this.closing && this.read<boolean>("joinable", id));
		return id;
	}
	private read<Value>(method: string, args?: unknown): Value {
		const request = this.rpc.request<Value>(method, args); void request.completion.catch(() => undefined);
		const response = request.receive(); if (!("value" in response)) throw new Error("native owner did not return a value"); return response.value as Value;
	}
	async open(input: NativeSessionInput, workspace: string, backend: Pick<LinuxProcessReuseBackend, "serializeBinding" | "restoreBinding">): Promise<LinuxProcessSession> {
		const { signal } = input, invocation = "environment" in input.invocation ? input.invocation : backend.serializeBinding(input.invocation);
		if (!invocation) throw new Error("process execution binding is unavailable in this scope");
		const id = this.retain(input), lease = { id }, abort = () => this.rpc.notify("cancel", id);
		signal?.addEventListener("abort", abort, { once: true });
		try {
			const authority = await this.rpc.call<SharedArrayBuffer>("open", { id: lease.id, workspace, sourceRoot: input.sourceRoot, invocation,
				scope: input.scope, producer: TimelineInterval.producer }, signal);
			const call = <Value>(operation: string, value?: unknown, request?: ProcessExecutionRequest) => this.rpc.call<Value>("session", { id: lease.id, operation, value }, request?.signal, request?.onData);
			const snapshot = () => this.read<SessionSnapshot>("snapshot", lease.id);
			let closing: Promise<void> | undefined;
			const ownership = new ProcessHandoffOwnership(input.onOperationAdopted, input.acceptOperationScope, undefined, authority, async <Value>(apply: () => Promise<Value>) => {
				let value: Value; this.callbacks.set(lease.id, async () => { value = await apply(); });
				try { await this.rpc.call("commit", lease.id); return value!; } finally { this.callbacks.delete(lease.id); }
			});
			retiredSessions.register(lease, { owner: new WeakRef(this), id, signal, abort }, lease);
			return { ownership,
				executor: { execute: request => call("execute", { ...request, signal: undefined, onData: undefined }, request) },
				executeBinding: async binding => {
					const value = backend.serializeBinding(binding); if (!value) throw new Error("process execution binding is unavailable in this scope");
					const result = await call<Awaited<ReturnType<LinuxProcessSession["executeBinding"]>>>("binding", value);
					return { ...result, output: result.output.map(event => ({ ...event, data: Buffer.from(event.data) })) };
				},
				metrics: () => Object.freeze(snapshot().metrics),
				executionBindings: () => Object.freeze(snapshot().bindings.flatMap(binding => backend.restoreBinding(binding) ?? [])),
				computationDependencies: () => Object.freeze(snapshot().computations.flatMap(dependency => restoreNativeDependency(dependency) ?? [])),
				seal: changes => call("seal", changes), validate: () => call("validate"),
				close: () => closing ??= call<void>("close").finally(() => signal?.removeEventListener("abort", abort)),
				dispose: () => { retiredSessions.unregister(lease); signal?.removeEventListener("abort", abort);
					return this.closing || this.failure ? Promise.resolve() : this.rpc.call<void>("release", lease.id); },
			};
		} catch (error) { signal?.removeEventListener("abort", abort); void this.rpc.call("release", lease.id).catch(() => undefined); throw error; }
	}

	attach(request: SynchronousProcessRequest, commit?: WorkspaceSandboxService["commitDelta"], borrow?: ProcessHandoffLender) {
		const id = ++this.sequence;
		this.callbacks.set(id, AsyncResource.bind(async (method, args: { delta: SandboxExecutionDelta; key: Parameters<ProcessHandoffLender>[0]; excluded: Parameters<ProcessHandoffLender>[1] }, context) => {
			if (method === "effects" && commit) return commit(args.delta);
			if (method === "borrow" && borrow) return borrow(args.key, args.excluded, context.signal);
			throw new Error("native observation callback unavailable");
		}));
		const buffers = request.handoffs.results.flatMap(result => [...result.artifacts.map(artifact => artifact.bytes.buffer as ArrayBuffer),
			...(result.continuation ? [result.continuation.image.buffer as ArrayBuffer] : [])]);
		const pending = this.rpc.request<SynchronousProcessResult>("observe", { id, request }, undefined, undefined, request.asynchronous ? [] : buffers);
		void pending.completion.catch(() => undefined);
		const attached = pending.receive();
		if (!("attached" in attached)) throw new Error("synchronous observer did not attach");
		return { pid: attached.attached, completion: request.asynchronous ? pending.completion : undefined,
			retire: () => this.rpc.notify("finished", id), release: () => { this.callbacks.delete(id); this.rpc.notify("settled", id); }, finish: () => {
				this.rpc.notify("finished", id); const completed = pending.receive();
				if (!("value" in completed)) throw new Error("synchronous observation did not complete");
				return completed.value as SynchronousProcessResult;
			} };
	}

	close(): Promise<void> {
		return this.closing ??= this.connection.close();
	}
}

async function serveNativeBackend(port: MessagePort, clock: Int32Array, options: LinuxProcessBackendOptions,
	workspaces: ReadonlyMap<string, SandboxWorkspaceContext>) {
	let backend: import("./linux-process-backend.ts").LinuxProcessReuseBackend | undefined;
	const observations = new Map<number, { readonly finish: () => void; release?: () => void | Promise<void> }>();
	const grants = new Map<number, { readonly scope?: ExecutionScope; readonly owners?: readonly number[] }>();
	const works = new Map<number, { readonly controller: AbortController; session?: LinuxProcessSession; available?: () => boolean;
		dispatch?: (operation: string, value: never, context: RpcContext) => Promise<unknown>; readonly apply: () => Promise<void> }>();
	const retained = new Map<number, { readonly ownership: WeakRef<ProcessHandoffOwnership>; readonly adopted: (adoption: ExecutionOperationAdoption) => void;
		readonly accept: (scope: ExecutionScope) => boolean }>();
	try {
		const { LinuxProcessReuseBackend } = await import("./linux-process-backend.ts"); backend = new LinuxProcessReuseBackend(options);
		const rpc = createProcessRpc(port, clock, async (method, args: unknown, context) => {
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
			entry.dispatch = AsyncResource.bind(async (operation: string, value: unknown, context: RpcContext) => {
				if (operation === "execute") return session.executor.execute({ ...value as ProcessExecutionRequest, ...context });
				if (operation === "binding") { const binding = backend!.restoreBinding(value as SerializedProcessBinding); if (!binding) throw new Error("native work binding expired"); return session.executeBinding(binding); }
				if (operation === "seal") return session.seal(value as Parameters<LinuxProcessSession["seal"]>[0]);
				if (operation === "validate" || operation === "close") return session[operation]();
				throw new Error(`unknown native session operation: ${operation}`);
			});
			return session;
		};
		const observe = async ({ id, request }: { id: number; request: SynchronousProcessRequest }, attached: RpcContext["attached"]) => {
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
