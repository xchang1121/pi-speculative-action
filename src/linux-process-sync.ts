import { MessageChannel, type MessagePort, Worker, isMainThread, receiveMessageOnPort, workerData } from "node:worker_threads";
import { TimelineInterval, type SerializedTimelineComputation, type TimelineDependency } from "./task-timing.ts";
import type { ExecutionOperationAdoption, ExecutionScope, WorldReuseMetrics } from "./execution-world.ts";
import type { ProcessHandoffTransfer } from "./process-handoff.ts";
import type { LinuxProcessBackendOptions, NativeProcessInvocation } from "./linux-process-backend.ts";
import { isPoisonedEffectCommit } from "./effect-transaction.ts";

type SerializedDependency = Omit<TimelineDependency, "computation" | "shared"> & {
	readonly computation: SerializedTimelineComputation;
	readonly shared?: readonly SerializedTimelineComputation[];
};
export interface SynchronousProcessRequest {
	readonly gate: number; readonly sourceRoot: string; readonly scope?: ExecutionScope;
	readonly learn: boolean; readonly descriptors?: boolean | "inspect"; readonly skipRoot?: boolean;
	readonly handoffs: ProcessHandoffTransfer;
}
export interface SynchronousProcessResult {
	readonly bindings: readonly { readonly sequence: number; readonly key: string; readonly executionMs: number; readonly invocation: NativeProcessInvocation }[];
	readonly computations: readonly SerializedDependency[];
	readonly adoptions: readonly { readonly index: number; readonly adoption: Omit<ExecutionOperationAdoption, "computation"> & { readonly computation?: SerializedDependency } }[];
	readonly metrics: WorldReuseMetrics;
}
type Response = { readonly kind: "ready" } | { readonly kind: "attached"; readonly pid?: number } |
	{ readonly kind: "result"; readonly result: SynchronousProcessResult } | { readonly kind: "error"; readonly message: string; readonly poisoned: boolean };

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
	private readonly port: MessagePort;
	private readonly clock = new Int32Array(new SharedArrayBuffer(8));
	private readonly worker: Worker;
	private readonly exited: Promise<void>;
	private initialized = false;
	private closing?: Promise<void>;
	private failure?: Error;

	constructor(options: LinuxProcessBackendOptions) {
		const { port1, port2 } = new MessageChannel(); this.port = port1;
		this.worker = new Worker(new URL(import.meta.url), { workerData: { nativeProcessObserver: true, port: port2, shared: this.clock.buffer, options },
			execArgv: process.execArgv.filter(argument => !argument.startsWith("--input-type")), transferList: [port2] });
		this.worker.on("error", error => { this.failure = error; });
		this.exited = new Promise(resolve => this.worker.once("exit", () => { Atomics.store(this.clock, 1, 1); resolve(); }));
		this.worker.unref(); this.port.unref();
	}

	attach(request: SynchronousProcessRequest): { readonly pid?: number; readonly finish: () => SynchronousProcessResult } {
		if (!this.initialized) { if (this.receive().kind !== "ready") throw new Error("synchronous observer did not initialize"); this.initialized = true; }
		const buffers = request.handoffs.results.flatMap(result => [...result.artifacts.map(artifact => artifact.bytes.buffer as ArrayBuffer),
			...(result.continuation ? [result.continuation.image.buffer as ArrayBuffer] : [])]);
		this.port.postMessage({ kind: "attach", request }, buffers);
		const attached = this.receive();
		if (attached.kind !== "attached") throw new Error("synchronous observer did not attach");
		return { pid: attached.pid, finish: () => {
			this.port.postMessage({ kind: "finished" });
			const completed = this.receive();
			if (completed.kind !== "result") throw new Error("synchronous observation did not complete");
			return completed.result;
		} };
	}

	private receive(): Response {
		const deadline = performance.now() + 15_000;
		while (!this.closing && !this.failure && performance.now() < deadline) {
			const version = Atomics.load(this.clock, 0), response = receiveMessageOnPort(this.port)?.message as Response | undefined;
			if (response?.kind === "error") throw Object.assign(new Error(response.message), { poisoned: response.poisoned });
			if (response) return response;
			if (Atomics.load(this.clock, 1)) break;
			Atomics.wait(this.clock, 0, version, Math.max(1, deadline - performance.now()));
		}
		throw this.failure ?? new Error("synchronous native observer unavailable");
	}

	close(): Promise<void> {
		if (this.closing) return this.closing;
		this.port.postMessage({ kind: "close" });
		const force = setTimeout(() => void this.worker.terminate(), 2_000); force.unref();
		return this.closing = this.exited.finally(() => { clearTimeout(force); this.port.close(); });
	}
}

if (!isMainThread && workerData?.nativeProcessObserver === true) void (async () => {
	const { port, shared, options } = workerData as { port: MessagePort; shared: SharedArrayBuffer; options: LinuxProcessBackendOptions };
	const clock = new Int32Array(shared), send = (response: Response) => { port.postMessage(response); Atomics.add(clock, 0, 1); Atomics.notify(clock, 0); };
	let backend: import("./linux-process-backend.ts").LinuxProcessReuseBackend | undefined, finish: (() => void) | undefined;
	try {
		const { LinuxProcessReuseBackend } = await import("./linux-process-backend.ts"); backend = new LinuxProcessReuseBackend(options);
		port.on("message", (message: { kind: "close" | "finished" } | { kind: "attach"; request: SynchronousProcessRequest }) => {
			if (message.kind === "close") { void backend!.dispose().finally(() => port.close()); return; }
			if (message.kind === "finished") { finish?.(); return; }
			if (message.kind !== "attach") return;
			const finished = new Promise<void>(resolve => { finish = resolve; });
			void backend!.observeSynchronous(message.request, pid => send({ kind: "attached", pid }), finished)
				.then(result => send({ kind: "result", result }), error => send({ kind: "error", message: String(error), poisoned: isPoisonedEffectCommit(error) }))
				.finally(() => { finish = undefined; });
		});
		send({ kind: "ready" });
	} catch (error) {
		send({ kind: "error", message: String(error), poisoned: false }); Atomics.store(clock, 1, 1); Atomics.notify(clock, 0);
		await backend?.dispose(); port.close();
	}
})();
