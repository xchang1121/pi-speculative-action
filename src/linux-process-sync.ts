import { AsyncResource } from "node:async_hooks";
import { connectNativeOwner, type NativeRpcContext } from "./native-owner.ts";
import { TimelineInterval, type SerializedTimelineComputation, type TimelineDependency } from "./task-timing.ts";
import { emptyWorldReuseMetrics, type ExecutionOperationAdoption, type ExecutionScope, type WorldReuseMetrics } from "./execution-world.ts";
import { ProcessHandoffOwnership, type ProcessHandoffTransfer, type ProcessHandoffLender } from "./process-handoff.ts";
import type { LinuxProcessBackendOptions, LinuxProcessReuseBackend, LinuxProcessSession, NativeProcessInvocation } from "./linux-process-backend.ts";
import type { CapturedFilesystemTransfer } from "./filesystem-evidence.ts";
import type { WorkspaceSandboxService, SandboxExecutionDelta } from "./workspace-sandbox.ts";
import type { ProcessExecutionRequest } from "./process-execution.ts";

type SerializedDependency = Omit<TimelineDependency, "computation" | "shared"> & {
	readonly computation: SerializedTimelineComputation;
	readonly shared?: readonly SerializedTimelineComputation[];
};
export interface SynchronousProcessRequest {
	readonly gate: number; readonly sourceRoot: string; readonly scope?: ExecutionScope;
	readonly learn: boolean; readonly reuseEnabled?: boolean; readonly descriptors?: boolean | "inspect"; readonly skipRoot?: boolean;
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
type SessionSnapshot = Pick<SynchronousProcessResult, "bindings" | "computations" | "metrics">;
const retiredSessions = new FinalizationRegistry<{ readonly owner: WeakRef<SynchronousProcessObserver>; readonly id: number; readonly signal?: AbortSignal; readonly abort: () => void }>(({ owner, id, signal, abort }) => {
	signal?.removeEventListener("abort", abort);
	void owner.deref()?.call("release", id).catch(() => undefined);
});
type WorkerState = { readonly live: boolean; readonly metrics: WorldReuseMetrics };
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
	private readonly connection: ReturnType<typeof connectNativeOwner<WorkerState>>;
	private closing?: Promise<void>;
	private failure?: Error;
	private sequence = 0;
	private get rpc() { return this.connection.rpc; }
	private readonly owners = new Map<number, { readonly adopted?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
		readonly accepts?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean> }>();
	private readonly callbacks = new Map<number, (method: string, args: never, context: NativeRpcContext) => Promise<unknown>>();
	private state: WorkerState = { live: false, metrics: emptyWorldReuseMetrics() };

	constructor(options: LinuxProcessBackendOptions) {
		this.connection = connectNativeOwner<WorkerState>("backend", options, (method, args: { id: number }, context) => {
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
