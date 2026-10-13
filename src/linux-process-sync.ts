import { AsyncResource } from "node:async_hooks";
import { MessageChannel, type MessagePort, type Transferable, Worker, isMainThread, parentPort, receiveMessageOnPort, workerData } from "node:worker_threads";
import { TimelineInterval, type ComputationProducer, type SerializedTimelineComputation, type TimelineDependency } from "./task-timing.ts";
import { emptyWorldReuseMetrics, type ExecutionOperationAdoption, type ExecutionScope, type WorldReuseMetrics } from "./execution-world.ts";
import type { ProcessHandoffOwnership, ProcessHandoffTransfer } from "./process-handoff.ts";
import type { LinuxProcessBackendOptions, LinuxProcessSession, NativeProcessInvocation } from "./linux-process-backend.ts";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";
import type { CapturedFilesystemTransfer } from "./filesystem-evidence.ts";
import { WorkspaceSandboxService, type WorkspaceCheckpoint, type WorkspaceExecutionSnapshot, type WorkspaceSandboxOptions, type SandboxExecutionDelta } from "./workspace-sandbox.ts";
import { toolErrorSettlement, type ToolProcessInvocation, type ToolSettlement } from "./tool-settlement.ts";
import type { ProcessExecutionRequest, ProcessExecutionResult, ProcessExecutor } from "./process-execution.ts";
import type { ResourceValidation } from "./settlement.ts";

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
export interface NativeProcessWork {
	readonly cwd: string; readonly invocation: ToolProcessInvocation | SerializedProcessBinding;
	readonly scope?: ExecutionScope; readonly parent?: WorkspaceCheckpoint; readonly options: WorkspaceSandboxOptions;
}
interface CapturedProcessWork {
	readonly snapshot: WorkspaceExecutionSnapshot; readonly bindings: readonly SerializedProcessBinding[]; readonly computations: readonly SerializedDependency[];
}
type WorkerState = { readonly live: boolean; readonly metrics: WorldReuseMetrics; readonly owners: readonly number[] };
type RemoteError = { readonly message: string; readonly disposition?: "recoverable" | "poisoned" };
const remoteError = (error: unknown): RemoteError => ({ message: error instanceof Error ? error.message : String(error),
	...(error instanceof EffectCommitFailure ? { disposition: error.disposition } : {}) });
const restoreError = (error: RemoteError) => error.disposition ? effectCommitFailure(new Error(error.message), error.disposition) : new Error(error.message);

type RpcResponse = { readonly data: Uint8Array } | { readonly attached: number | undefined } |
	{ readonly value?: unknown; readonly error?: RemoteError; readonly state?: WorkerState };
type RpcContext = { readonly signal: AbortSignal; readonly onData: (data: Buffer) => void; readonly attached: (pid?: number) => void };

/** A request owns its reply channel; synchronous reads never dispatch the caller's callbacks. */
function createProcessRpc(port: MessagePort | Worker, clock: Int32Array,
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
	function fail(error: Error): void { failure = error; for (const channel of channels) channel.close(); }
	return { notify, call, request, fail };
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
	private readonly clock = new Int32Array(new SharedArrayBuffer(8));
	private readonly worker: Worker;
	private readonly exited: Promise<void>;
	private closing?: Promise<void>;
	private failure?: Error;
	private sequence = 0;
	private readonly rpc: ReturnType<typeof createProcessRpc>;
	private readonly owners = new Map<number, { readonly adopted?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
		readonly accepts?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean> }>();
	private readonly callbacks = new Map<number, () => Promise<unknown>>();
	private readonly commits = new Map<number, WorkspaceSandboxService["commitDelta"]>();
	private state: WorkerState = { live: false, metrics: emptyWorldReuseMetrics(), owners: [] };

	constructor(options: LinuxProcessBackendOptions) {
		this.worker = new Worker(new URL(import.meta.url), { workerData: { nativeProcessObserver: true, shared: this.clock.buffer, options },
			execArgv: process.execArgv.filter(argument => !argument.startsWith("--input-type")) });
		this.rpc = createProcessRpc(this.worker, this.clock, (method, args: { id: number; delta: SandboxExecutionDelta }) => {
			if (method === "effects") { const commit = this.commits.get(args.id); if (!commit) throw new Error("native effect owner retired"); return commit(args.delta); }
			const callback = this.callbacks.get(args.id); if (!callback) throw new Error("native work callback retired"); return callback();
		}, undefined, state => {
			this.state = state;
			for (const id of this.owners.keys()) if (!this.callbacks.has(id) && !state.owners.includes(id)) this.owners.delete(id);
		});
		const failed = (error: Error) => { this.failure = error; this.rpc.fail(error); };
		this.worker.on("error", failed);
		this.exited = new Promise(resolve => this.worker.once("exit", () => {
			Atomics.store(this.clock, 1, 1); Atomics.notify(this.clock, 0); failed(new Error("native process owner exited")); resolve();
		}));
		this.worker.unref();
	}

	get hasWork(): boolean { return this.state.live || this.callbacks.size > 0; }
	metrics(): WorldReuseMetrics { return this.state.metrics; }
	call<Value>(method: string, args?: unknown, signal?: AbortSignal): Promise<Value> { return this.rpc.call(method, args, signal); }
	grants(scope?: ExecutionScope): readonly number[] {
		return scope ? [...this.owners].filter(([, owner]) => owner.accepts?.deref()?.(scope, true)).map(([id]) => id) : [];
	}
	adopted(id: number, adoption: ExecutionOperationAdoption): void { this.owners.get(id)?.adopted?.deref()?.(adoption); }

	async run(work: NativeProcessWork, signal: AbortSignal, execute: (executor: ProcessExecutor) => Promise<ToolSettlement>,
		callbacks: { readonly onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
			readonly acceptOperationScope?: (scope: ExecutionScope, salvage?: boolean) => boolean;
			readonly onOperationJoinable?: (available: () => boolean) => void }) {
		const id = ++this.sequence;
		this.owners.set(id, { adopted: callbacks.onOperationAdopted && new WeakRef(callbacks.onOperationAdopted),
			accepts: callbacks.acceptOperationScope && new WeakRef(callbacks.acceptOperationScope) });
		let output: ToolSettlement | undefined;
		this.callbacks.set(id, AsyncResource.bind(async () => {
			try { output = await execute({ execute: request => this.rpc.call<ProcessExecutionResult>("exec",
				{ id, request: { ...request, signal: undefined, onData: undefined } }, request.signal, request.onData) }); }
			catch (error) { output = toolErrorSettlement(error); throw error; }
		}));
		callbacks.onOperationJoinable?.(() => {
			if (signal.aborted || this.failure || this.closing) return false;
			const request = this.rpc.request<boolean>("joinable", id); void request.completion.catch(() => undefined);
			const response = request.receive(); return "value" in response && response.value === true;
		});
		try {
			const captured = await this.rpc.call<CapturedProcessWork>("capture", { id, work, producer: TimelineInterval.producer }, signal);
			const snapshot = output ? { ...captured.snapshot, output: { ...output,
				...(captured.snapshot.output.exitCode === undefined ? {} : { exitCode: captured.snapshot.output.exitCode }) } } : captured.snapshot;
			return { ...captured, snapshot, validate: () => this.rpc.call<ResourceValidation>("validate", id),
				commit: async (apply: () => Promise<ToolSettlement>) => {
					this.callbacks.set(id, async () => { await apply(); });
					try { await this.rpc.call<void>("commit", id); return await apply(); }
					catch (error) { throw effectCommitFailure(error, "poisoned"); } finally { this.callbacks.delete(id); }
				}, dispose: () => this.closing || this.failure ? Promise.resolve() : this.rpc.call<void>("release", id) };
		} catch (error) { void this.rpc.call("release", id).catch(() => undefined); throw error; }
		finally { this.callbacks.delete(id); }
	}

	attach(request: SynchronousProcessRequest, commit?: WorkspaceSandboxService["commitDelta"]) {
		const id = ++this.sequence;
		if (commit) this.commits.set(id, AsyncResource.bind(commit));
		const buffers = request.handoffs.results.flatMap(result => [...result.artifacts.map(artifact => artifact.bytes.buffer as ArrayBuffer),
			...(result.continuation ? [result.continuation.image.buffer as ArrayBuffer] : [])]);
		const pending = this.rpc.request<SynchronousProcessResult>("observe", { id, request }, undefined, undefined, request.asynchronous ? [] : buffers);
		void pending.completion.catch(() => undefined);
		const attached = pending.receive();
		if (!("attached" in attached)) throw new Error("synchronous observer did not attach");
		return { pid: attached.attached, completion: request.asynchronous ? pending.completion : undefined,
			retire: () => this.rpc.notify("finished", id), release: () => { this.commits.delete(id); this.rpc.notify("settled", id); }, finish: () => {
				this.rpc.notify("finished", id); const completed = pending.receive();
				if (!("value" in completed)) throw new Error("synchronous observation did not complete");
				return completed.value as SynchronousProcessResult;
			} };
	}

	close(): Promise<void> {
		if (this.closing) return this.closing;
		this.worker.ref();
		this.rpc.notify("close");
		return this.closing = this.exited;
	}
}

if (!isMainThread && workerData?.nativeProcessObserver === true) void (async () => {
	const { shared, options } = workerData as { shared: SharedArrayBuffer; options: LinuxProcessBackendOptions };
	const clock = new Int32Array(shared);
	let backend: import("./linux-process-backend.ts").LinuxProcessReuseBackend | undefined;
	const workspace = new WorkspaceSandboxService(), observations = new Map<number, { readonly finish: () => void; release?: () => void | Promise<void> }>();
	const grants = new Map<number, { readonly scope?: ExecutionScope; readonly owners?: readonly number[] }>();
	const works = new Map<number, { readonly signal: AbortSignal; session?: LinuxProcessSession; available?: () => boolean;
		launches: number; exitCode?: number | null; readonly operation: boolean; readonly apply: () => Promise<void> }>();
	const prepared = new Map<string, Awaited<ReturnType<WorkspaceSandboxService["prepare"]>>>();
	const retained = new Map<number, { readonly ownership: WeakRef<ProcessHandoffOwnership>; readonly adopted: (adoption: ExecutionOperationAdoption) => void;
		readonly accept: (scope: ExecutionScope) => boolean }>();
	try {
		const { LinuxProcessReuseBackend } = await import("./linux-process-backend.ts"); backend = new LinuxProcessReuseBackend(options);
		const rpc = createProcessRpc(parentPort!, clock, async (method, args: unknown, context) => {
			if (method === "observe") return observe(args as { id: number; request: SynchronousProcessRequest }, context.attached);
			if (method === "capture") {
				const { id, work, producer } = args as { id: number; work: NativeProcessWork; producer?: ComputationProducer };
				const entry: NonNullable<ReturnType<typeof works.get>> = { signal: context.signal, launches: 0,
					operation: "invocation" in work.invocation, apply: () => rpc.call("apply", { id }) };
				works.set(id, entry);
				const adopted = (adoption: ExecutionOperationAdoption) => backend!.recordWorkerAdoption(id, adoption);
				const accept = (scope: ExecutionScope) => [...grants.values()].some(grant => grant.scope?.sessionID === scope.sessionID &&
					grant.scope.turnID === scope.turnID && grant.scope.callID === scope.callID && grant.owners?.includes(id));
				const binding = "invocation" in work.invocation ? backend!.restoreBinding(work.invocation) : undefined;
				if (entry.operation && !binding) throw new Error("native work binding expired");
				let unfinished: unknown;
				const { output: snapshot } = await TimelineInterval.collect(() => backend!.withProducer(() => workspace.capture({ ...work.options, cwd: work.cwd,
					preparation: binding ? prepared.get(work.cwd) : undefined,
					executionMetrics: () => ({ reuse: entry.session?.metrics() }), validate: () => entry.session!.validate(),
					afterCapture: (_workspace, capture) => { if (unfinished !== undefined) throw unfinished; return entry.session!.seal(capture.changes); },
					execute: async sandbox => {
						const session = entry.session = await backend!.open({ sourceRoot: work.cwd, workspace: sandbox, invocation: binding ?? work.invocation as ToolProcessInvocation,
							scope: work.scope, signal: context.signal, onOperationAdopted: adopted, acceptOperationScope: accept,
							onOperationJoinable: available => { entry.available = available; } });
						retained.set(id, { ownership: new WeakRef(session.ownership), adopted, accept });
						try {
							if (binding) { const result = await session.executeBinding(binding);
								return { result: { content: [], details: result.suspended ? { suspended: true } : { exit: result.exit } }, isError: false }; }
							await rpc.call("tool", { id });
							if (!entry.launches) throw new Error("process-backed tool bypassed the process execution outlet");
							return { result: { content: [], details: {} }, isError: false };
						} catch (error) {
							if (entry.launches === 1 && typeof entry.exitCode === "number" && entry.exitCode !== 0) return { ...toolErrorSettlement(error), exitCode: entry.exitCode };
							unfinished = error; return toolErrorSettlement(error);
						} finally { await session.close(); }
					},
				}, work.parent)), producer);
				return { snapshot, bindings: entry.session!.executionBindings().flatMap(binding => backend!.serializeBinding(binding) ?? []),
					computations: entry.session!.computationDependencies().flatMap(dependency => serializeNativeDependency(dependency) ?? []) } satisfies CapturedProcessWork;
			}
			if (method === "exec") {
				const { id, request } = args as { id: number; request: ProcessExecutionRequest }, entry = works.get(id);
				if (!entry?.session) throw new Error("native work retired"); entry.launches++;
				return { exitCode: entry.exitCode } = await entry.session.executor.execute({ ...request, ...context });
			}
			if (method === "release") { works.delete(args as number); return; }
			if (method === "validate" || method === "commit") {
				const entry = works.get(args as number); if (!entry?.session) throw new Error("native work retired");
				if (method === "validate") return entry.session.validate();
				if (entry.operation) throw new Error("internal process output cannot commit an enclosing tool");
				return entry.session.ownership.commit(entry.apply);
			}
			if (method === "prepare" || method === "qualify") {
				const { cwd, options } = args as { cwd: string; options: WorkspaceSandboxOptions };
				if (method === "qualify") return workspace.qualify(options, cwd);
				const selected = await workspace.prepare(cwd, { ...options, signal: context.signal }); prepared.set(cwd, selected); return selected;
			}
			if (method === "fingerprint") return workspace.fingerprint(args as WorkspaceSandboxOptions);
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
				return backend!.dispose().finally(() => workspace.dispose()).finally(() => parentPort!.close());
			}
			throw new Error(`unknown native work operation: ${method}`);
		}, () => {
			for (const [id, owner] of retained) if (!owner.ownership.deref()) retained.delete(id);
			return { live: backend!.hasReusableWork, metrics: backend!.producerMetrics(), owners: [...retained.keys()] };
		});
		const observe = async ({ id, request }: { id: number; request: SynchronousProcessRequest }, attached: RpcContext["attached"]) => {
			const finished = new Promise<void>(finish => { observations.set(id, { finish }); }); grants.set(id, request);
			try {
				const completed = await backend!.observeSynchronous(request, attached, finished,
					request.asynchronous ? delta => rpc.call<ToolSettlement>("effects", { id, delta }) : undefined);
				observations.get(id)!.release = completed.release; return completed.result;
			} finally { grants.delete(id); }
		};
	} catch {
		Atomics.store(clock, 1, 1); Atomics.notify(clock, 0);
		await backend?.dispose(); parentPort!.close();
	}
})();
