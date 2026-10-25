import { MessageChannel, type MessagePort, type Transferable, Worker, receiveMessageOnPort, workerData } from "node:worker_threads";
import { EffectCommitFailure, effectCommitFailure } from "./effect-transaction.ts";

type RemoteError = { readonly message: string; readonly disposition?: "recoverable" | "poisoned" };
const remoteError = (error: unknown): RemoteError => ({ message: error instanceof Error ? error.message : String(error),
	...(error instanceof EffectCommitFailure ? { disposition: error.disposition } : {}) });
const restoreError = (error: RemoteError) => error.disposition ? effectCommitFailure(new Error(error.message), error.disposition) : new Error(error.message);

type RpcResponse<State> = { readonly data: Uint8Array } | { readonly attached: number | undefined } |
	{ readonly value?: unknown; readonly error?: RemoteError; readonly state?: State };
export type NativeRpcContext = { readonly signal: AbortSignal; readonly onData: (data: Buffer) => void; readonly attached: (pid?: number) => void;
	/** Reply-owned loans remain valid through the receiver's asynchronous verification. */
	readonly retain: (release: () => void) => void };

/** A request owns its reply channel; synchronous reads never dispatch the caller's callbacks. */
export function createNativeRpc<State = never>(port: MessagePort, clock: Int32Array,
	invoke: (method: string, args: never, context: NativeRpcContext) => unknown,
	state?: () => State, observed?: (state: State) => void) {
	const channels = new Set<MessagePort>();
	let failure: Error | undefined;
	port.on("message", ({ method, args, reply }: { method: string; args: never; reply?: MessagePort }) => {
		const controller = new AbortController(), cancel = () => controller.abort(new Error("native work cancelled"));
		const retained: (() => void)[] = [], release = () => { for (const close of retained.splice(0)) close(); };
		if (reply) { track(reply); reply.on("message", cancel); reply.once("close", () => { cancel(); release(); }); }
		const send = (response: RpcResponse<State>) => { reply?.postMessage(response); Atomics.add(clock, 0, 1); Atomics.notify(clock, 0); };
		void Promise.resolve().then(() => { if (failure) throw failure;
			return invoke(method, args, { signal: controller.signal, onData: data => send({ data }), attached: pid => send({ attached: pid }),
				retain: close => { if (!reply || controller.signal.aborted) close(); else retained.push(close); } }); })
			.then(value => send({ value, state: state?.() })).catch(error => { release(); send({ error: remoteError(error), state: state?.() }); })
			.finally(() => { if (!retained.length) reply?.close(); });
	});
	function notify(method: string, args?: unknown): void { port.postMessage({ method, args }); }
	function call<Value>(method: string, args?: unknown, signal?: AbortSignal, onData?: (data: Buffer) => void, decode?: (value: unknown) => Value | Promise<Value>): Promise<Value> {
		try { return request<Value>(method, args, signal, onData, [], decode).completion; } catch (error) { return Promise.reject(error); }
	}
	function request<Value>(method: string, args?: unknown, signal?: AbortSignal, onData?: (data: Buffer) => void, transfers: readonly Transferable[] = [], decode?: (value: unknown) => Value | Promise<Value>) {
		if (failure) throw failure; signal?.throwIfAborted();
		const { port1, port2 } = new MessageChannel(), cancel = () => port1.postMessage(undefined); track(port1);
		let failed: { error: unknown } | undefined, settle!: (response: RpcResponse<State>) => void;
		const result = new Promise<Value>((resolve, reject) => {
			settle = response => {
				if ("attached" in response) return;
				if ("data" in response) { try { if (!failed) onData?.(Buffer.from(response.data)); } catch (error) { failed = { error }; cancel(); } return; }
				if (response.state) observed?.(response.state);
				if (failed || response.error) reject(failed ? failed.error : restoreError(response.error!));
				else { try { resolve(decode ? decode(response.value) : response.value as Value); } catch (error) { reject(error); } }
			};
			port1.on("message", settle); port1.once("close", () => reject(failure ?? new Error("native work channel closed")));
			signal?.addEventListener("abort", cancel, { once: true });
			try { port.postMessage({ method, args, reply: port2 }, [port2, ...transfers]); } catch (error) { port2.close(); reject(error); }
		});
		const completion = result.finally(() => { signal?.removeEventListener("abort", cancel); port1.close(); });
		return { completion, receive: (): RpcResponse<State> => {
			const deadline = performance.now() + 15_000;
			while (!failure && performance.now() < deadline) {
				const version = Atomics.load(clock, 0), response = receiveMessageOnPort(port1)?.message as RpcResponse<State> | undefined;
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
export function connectNativeOwner<State = never>(kind: "backend" | "workspace", options: unknown,
	invoke: Parameters<typeof createNativeRpc<State>>[2], observed?: (state: State) => void) {
	if (!nativeOwner) {
		const clients = new Set<(error: Error) => void>();
		const worker = new Worker(new URL(import.meta.url.endsWith(".ts") ? "./native-worker.ts" : "./native-worker.js", import.meta.url), { workerData: { nativeProcessObserver: true },
			execArgv: process.execArgv.filter(argument => !argument.startsWith("--input-type")) });
		const owner = nativeOwner = { worker, clients, exited: new Promise<void>(resolve => worker.once("exit", () => {
			for (const fail of clients) fail(new Error("native process owner exited"));
			if (nativeOwner === owner) nativeOwner = undefined; resolve();
		})) };
		worker.on("error", error => { for (const fail of clients) fail(error); }); worker.unref();
	}
	const owner = nativeOwner, clock = new Int32Array(new SharedArrayBuffer(8)), { port1: port, port2 } = new MessageChannel();
	const rpc = createNativeRpc(port, clock, invoke, undefined, observed); owner.clients.add(rpc.fail);
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
