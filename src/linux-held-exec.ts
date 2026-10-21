import { isSafeCount } from "./number-utils.ts";
import { isUtf8 } from "node:buffer";
import { spawn, type ChildProcess } from "node:child_process";
import { on, once } from "node:events";
import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { createRequire } from "node:module";
import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, mkdir, open, readFile, readdir, readlink, realpath, rm, stat } from "node:fs/promises";
import { captureProcessContext, processLaunchOptions, validProcessContext, type ProcessExecutionContext, type ProcessLaunchOptions } from "./process-context.mjs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import { captureHeldDirectory, captureStableFile, sameFilesystemIdentity } from "./filesystem-evidence.ts";
import type { ResourceObjectInputs } from "./agent-execution-world.ts";
import { sha256Digest, type Sha256Digest } from "./stable-value-hash.ts";
import {
	isOFDPosition, type OFDPosition, RESOURCE_TRANSITIONS, type ResourceTransitionKind, type ProcessResourceEffects, type ArtifactReference,
} from "./provenance-certificate.ts";
import { containsFilesystemPath } from "./path-utils.ts";
import { snapshotExecutionScope, type ExecutionScope } from "./execution-world.ts";
import { captureWorkspaceFileAliases } from "./process-observation.ts";
import { qualifyNativeStartup } from "./process-startup.mjs";

const MAX_REQUEST_BYTES = 4 * 1024 * 1024 + 32768;
const MAX_OUTPUT_EVENTS = 65_536;
const MAX_OUTPUT_BYTES = 512 * 1024 * 1024;
type NativeSpawn = (this: ChildProcess, options: object) => number;
type NativeSyncSpawn = (this: unknown, options: object) => unknown;
const startupScope = new AsyncLocalStorage<{
	readonly skipRoot?: boolean;
	readonly launch: (child: ChildProcess, start: () => number) => number;
	readonly synchronous?: (start: () => unknown) => unknown;
}>();
const nativeRequire = createRequire(import.meta.url);
const startupPrototype = nativeRequire("node:child_process").ChildProcess.prototype as { spawn: NativeSpawn };
let startupUsers = 0, starting = false, restoreStartup: (() => void) | undefined;

function enterStartup(): void {
	if (startupUsers++) return;
	const original = startupPrototype.spawn;
	const intercept: NativeSpawn = function(options) {
		const start = () => { const outer = starting; starting = true;
			try { return Reflect.apply(original, this, [options]); } finally { starting = outer; } };
		const scope = !starting && startupScope.getStore();
		return scope ? scope.launch(this, start) : start();
	};
	startupPrototype.spawn = intercept;
	// All three public synchronous APIs call this binding, including references imported before observation.
	let synchronous: { spawn: NativeSyncSpawn } | undefined, originalSync: NativeSyncSpawn | undefined;
	const interceptSync: NativeSyncSpawn = function(options) {
		const start = () => Reflect.apply(originalSync!, this, [options]);
		const scope = startupScope.getStore();
		return scope?.synchronous ? scope.synchronous(start) : start();
	};
	try {
		synchronous = (process as unknown as { binding(name: string): { spawn: NativeSyncSpawn } }).binding("spawn_sync");
		originalSync = synchronous.spawn; synchronous.spawn = interceptSync;
	} catch { /* An unavailable internal binding leaves its native route intact. */ }
	restoreStartup = () => {
		if (--startupUsers) return;
		if (startupPrototype.spawn === intercept) startupPrototype.spawn = original;
		if (synchronous?.spawn === interceptSync) synchronous.spawn = originalSync!;
	};
}

export interface HeldExecProcess {
	/** Host-owned launch identity and ordered exec event; never a reusable bare PID. */
	readonly id: string;
	readonly sequence: number;
	readonly pid: number;
	readonly tracerPid: number;
	/** Unprivileged owner of queued-only snapshot pins, retired before native validation. */
	readonly holderPid?: number;
	readonly sourceRoot: string;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	/** Native KCMP_FILE groups. Owned entries are captured with the launch tree stopped.
	 * `owned` proves creation in the traced tree with no observed export; it does not
	 * authorize content reuse or cover an external debugger duplicating its handles. */
	readonly descriptors?: readonly HeldFileDescriptor[];
	/** A cold queue needs an ownership-tracked launch before its shared peek cursor can be borrowed. */
	readonly trackQueues?: true;
}

export type QueueMessage = { readonly start: number; readonly end: number; readonly rights: readonly number[] };
export type OFDLock = { readonly kind: 0 | 1; readonly type: 0 | 1; readonly start: string; readonly length: string };

function validOFDLocks(locks: readonly OFDLock[] | undefined): boolean {
	return locks === undefined || Array.isArray(locks) && locks.length > 0 && locks.length <= 64 && locks.every(lock =>
		lock && [0, 1].includes(lock.kind) && [0, 1].includes(lock.type) && [lock.start, lock.length].every(value =>
			typeof value === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value) && BigInt(value) <= 0x7fffffffffffffffn) &&
		BigInt(lock.start) + BigInt(lock.length) <= 0x7fffffffffffffffn && (lock.kind || lock.start === "0" && lock.length === "0"));
}

function validQueueMessages(messages: readonly QueueMessage[] | undefined, bytes: number): boolean {
	if (messages === undefined) return true;
	if (!Array.isArray(messages) || !messages.length || messages.length > 64) return false;
	let end = 0, rights = 0;
	for (const message of messages) {
		if (!message || !Number.isSafeInteger(message.start) || !Number.isSafeInteger(message.end) ||
			message.start < end || message.end <= message.start || message.end > bytes || !Array.isArray(message.rights) || !message.rights.length ||
			message.rights.some((fd: number) => !isSafeCount(fd, 0, 0x7fffffff))) return false;
		end = message.end; rights += message.rights.length;
	}
	return rights <= 64;
}

export interface HeldFileDescriptor {
	/** A queued-only OFD is pinned by the authenticated tracer, not installed in the tracee. */
	readonly pin?: number;
	readonly type?: "null" | "directory" | "pipe" | "socket" | "eventfd";
	readonly counter?: { readonly id: number; readonly value: string; readonly semaphore: number };
	readonly directoryHex?: string;
	/** Native non-consuming queue snapshot; `owned` separately authorizes adoption. */
	readonly queueHex?: string;
	readonly messages?: readonly QueueMessage[];
	readonly locks?: readonly OFDLock[];
	readonly eof?: boolean;
	readonly capacity?: number;
	/** Read/write references outside this exec image; snapshot pins never count. */
	readonly outside?: number;
	readonly socket?: { readonly type?: 1 | 2 | 5; readonly shutdown: number; readonly peerShutdown: number; readonly peerInode: number; readonly peerQueued: number; readonly allocated: number };
	readonly fd: number;
	readonly alias: number;
	readonly device: string;
	readonly inode: string;
	readonly flags: number;
	readonly offset: OFDPosition;
	readonly owned: boolean;
}

/** IDs are canonical graph representatives; queued-only descriptions have no handle edge. */
export interface ProcessResourceGraph {
	readonly handles: readonly { readonly fd: number; readonly description: number }[];
	readonly descriptions: Readonly<Record<number, {
		readonly object: number; readonly flags: number; readonly position?: OFDPosition; readonly locks?: readonly OFDLock[];
		readonly outside?: number;
	}>>;
	readonly objects: Readonly<Record<number, {
		readonly type: "regular" | "null" | "directory" | "pipe" | "socket" | "eventfd";
		readonly counter?: { readonly value: string; readonly semaphore: number };
		readonly contentDigest: Sha256Digest; readonly sourcePath?: string; readonly sourceAliases?: readonly string[]; readonly content?: string;
		/** A queue belongs to the kernel object, independently of its read OFDs. */
		readonly queue?: { readonly eof: boolean; readonly bytes: number; readonly capacity: number; readonly producer: "closed" | "live"; readonly outside?: number; readonly messages?: readonly QueueMessage[] };
		readonly socket?: { readonly type?: 1 | 2 | 5; readonly shutdown: number; readonly allocated: number;
			readonly peer: { readonly connected: boolean; readonly shutdown: number; readonly bytes: number; readonly object?: number } };
	}>>;
}

/** Flatten only at existing native/context protocol boundaries; bindings retain the graph. */
interface FileDescriptorInput extends Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type" | "locks" | "outside">,
	Pick<ProcessResourceGraph["objects"][number], "contentDigest" | "sourcePath" | "sourceAliases" | "content"> {
	readonly installed?: false;
	/** One image per inode; distinct OFDs open it independently. */
	readonly image: number;
}

export function descriptorInputs(graph?: ProcessResourceGraph): readonly FileDescriptorInput[] {
	if (!graph) return [];
	const installed = new Set(graph.handles.map(handle => handle.description));
	return [...graph.handles, ...Object.keys(graph.descriptions).map(Number).filter(id => !installed.has(id))
		.map(id => ({ fd: id, description: id, installed: false as const }))].sort((a, b) => a.fd - b.fd).map(handle => {
		const { fd, description } = handle;
		const ofd = graph.descriptions[description]!, object = graph.objects[ofd.object]!;
		return { fd, alias: description, flags: ofd.flags, offset: ofd.position ?? 0, image: ofd.object,
			...(ofd.locks ? { locks: ofd.locks } : {}),
			...(ofd.outside !== undefined || object.queue ? { outside: ofd.outside ?? object.queue?.outside ?? 3 } : {}),
			...("installed" in handle ? { installed: false as const } : {}),
			...(object.type === "regular" ? {} : { type: object.type }), contentDigest: object.contentDigest,
			...(object.sourcePath ? { sourcePath: object.sourcePath } : {}),
			...(object.sourceAliases ? { sourceAliases: object.sourceAliases } : {}),
			...(fd === ofd.object && object.content !== undefined ? { content: object.content } : {}) };
	});
}

/** Reduce the native per-handle report to one transition per OFD and kernel object. */
export function descriptorEffects(graph: ProcessResourceGraph, report: readonly {
	readonly fd: number; readonly before: OFDPosition; readonly after: OFDPosition; readonly afterFlags?: number; readonly content?: ArtifactReference;
}[]): ProcessResourceEffects {
	const inputs = descriptorInputs(graph);
	if (report.length !== inputs.length) throw new Error("incomplete resource transition");
	const descriptions = new Map<number, ProcessResourceEffects["descriptions"][number]>();
	const objects = new Map<number, ProcessResourceEffects["objects"][number]>();
	for (const [index, handle] of inputs.entries()) {
		const position = report[index]!, ofd = graph.descriptions[handle.alias]!, object = graph.objects[ofd.object]!;
		if (position.fd !== handle.fd || position.before !== (ofd.position ?? 0) || !isOFDPosition(position.after) ||
			position.afterFlags !== undefined && (!isSafeCount(position.afterFlags, 0, 0x7fffffff) ||
				((position.afterFlags ^ ofd.flags) & ~0xc00))) throw new Error("invalid resource transition");
		const previous = descriptions.get(handle.alias), shared = objects.get(ofd.object);
		if (previous && (previous.position?.after !== (ofd.position === undefined ? undefined : position.after) || previous.flags !== position.afterFlags) ||
			object.queue && (typeof position.after !== "number" || position.after > object.queue.bytes || shared && shared.consumed !== position.after) ||
			!object.queue && ofd.position === undefined && position.after !== 0 ||
			position.content && (object.type !== "regular" || handle.fd !== ofd.object)) throw new Error("inconsistent shared resource transition");
		if (!previous) descriptions.set(handle.alias, { id: handle.alias,
			...(ofd.position !== undefined ? { position: { before: ofd.position, after: position.after } } : {}),
			...(position.afterFlags !== undefined ? { flags: position.afterFlags } : {}) });
		if (!shared) objects.set(ofd.object, { id: ofd.object, ...(object.queue ? { consumed: position.after as number } : {}),
			...(position.content ? { content: position.content } : {}) });
	}
	return { descriptions: [...descriptions.values()], objects: [...objects.values()] };
}

export interface HeldExecSnapshot extends Partial<ProcessLaunchOptions> {
	readonly executable: string;
	readonly invocation?: readonly [path: string, comm: string];
	readonly argv: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: Pick<ProcessExecutionContext, "key" | "umask" | "descriptorTypes" | "regularDescriptors">;
	readonly resources?: ProcessResourceGraph;
}

export interface HeldExecClock {
	readonly startedAt: number;
	readonly completedAt: number;
}

/** Same-process transport clocks; native completion exists only when the existing decision requested it. */
export interface HeldExecTiming {
	readonly requestedAt: number;
	readonly completedAt: number;
	readonly committedAt?: number;
	readonly barrier: "none" | "inspection" | "descriptors";
	readonly outcome: "continued" | "adopted" | "declined" | "failed";
	readonly native?: HeldExecClock;
}

export type HeldExecDecision = { readonly observeTiming?: (timing: HeldExecTiming) => void } & (
	/** Continue only this held launch; a later launch must observe candidates available then. */
	| { readonly kind: "continue"; readonly observeCompletion?: (durationMs: number | undefined, clock?: HeldExecClock) => void | Promise<void> }
	| ({
			readonly kind: "replay";
			/** Release a reservation only when the native peer has definitely performed no adoption effects. */
			readonly settle?: (consumed: boolean) => void;
			readonly output: readonly { readonly fd: 1 | 2; readonly data: Buffer }[];
			readonly resourceEvents?: readonly { readonly fd: number; readonly kind: ResourceTransitionKind; readonly data: Buffer; readonly requested?: number }[];
			/** Applied after commit, before output. The caller owns predecessor proof and serialization of every OFD sharer. */
			readonly descriptorOffsets?: readonly (Pick<HeldFileDescriptor, "fd" | "device" | "inode" | "flags" | "eof" | "capacity" | "socket" | "messages"> & {
				readonly before: OFDPosition; readonly after: OFDPosition; readonly afterFlags?: number; readonly path?: string;
				/** File replacement bytes, or the expected full pipe queue before consuming `after` bytes. */
				readonly content?: Buffer;
				readonly event?: number;
			})[];
			/** Called only after the native tracer has made original execution impossible. */
			readonly commit: () => Promise<void>;
			readonly adopted?: () => void;
	  } & ({ readonly exitCode: number; readonly continuation?: never } |
		{ readonly exitCode?: never; readonly continuation: { readonly image: Buffer; readonly physicalRoot: string } })));

export interface LinuxHeldExecOptions { readonly storeRoot: string; readonly binary?: string; readonly observerBinary?: string; readonly signal?: AbortSignal; }

type ActiveExecution = ReturnType<LinuxHeldExecBoundary["createAttachment"]>;

export interface HeldObservation {
	readonly sourceRoot: string;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	readonly decide: (process: HeldExecProcess) => Promise<HeldExecDecision>;
	readonly descriptors?: boolean | "inspect";
	readonly skipRoot?: boolean;
}

export interface HeldExecAttachment {
	readonly pid?: number;
	readonly completion: Promise<void>;
	readonly retire: () => void;
}

interface WireRequest {
	readonly token: string;
	readonly execution: string;
	readonly pid: number;
	readonly tracer: number;
	readonly holder?: number;
	readonly descriptors?: readonly HeldFileDescriptor[];
	readonly trackQueues?: true;
}

/** A two-phase held-exec transport: arm the exit stub before committing reusable effects. */
export class LinuxHeldExecBoundary {
	private readonly token = randomBytes(32).toString("hex");
	private readonly active = new Map<string, ActiveExecution>();
	private readonly calls = new Set<PromiseWithResolvers<void> & { readonly controller: AbortController }>();
	private readonly server: net.Server;
	private readonly sockets = new Set<net.Socket>();
	private readonly socketPath: string;
	private closing?: Promise<void>;

	private readonly startup: (operation: number) => number;
	private readonly observerBinary: string;
	private constructor(socketPath: string, native: Awaited<ReturnType<typeof qualifyNativeStartup>>) {
		this.startup = native.startup; this.observerBinary = native.observer;
		this.socketPath = socketPath;
		this.server = net.createServer({ allowHalfOpen: true }, (socket) => {
			this.sockets.add(socket);
			socket.on("error", () => socket.destroy());
			socket.once("close", () => this.sockets.delete(socket));
			void this.serve(socket);
		});
	}

	static async open(options: LinuxHeldExecOptions): Promise<LinuxHeldExecBoundary> {
		if (process.platform !== "linux" || process.arch !== "x64") throw new Error("x86-64 Linux required");
		options.signal?.throwIfAborted();
		const binary = await resolveLinuxExecHelper(options.binary);
		let native: Awaited<ReturnType<typeof qualifyNativeStartup>>;
		try { native = await startupScope.exit(() => qualifyNativeStartup(binary, { observer: options.observerBinary, signal: options.signal })); }
		catch (error) { options.signal?.throwIfAborted(); throw new Error(`held-exec functional probe failed${await inheritedTracer()}: ${error instanceof Error ? error.message : error}`, { cause: error }); }
		await mkdir(options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(options.storeRoot, 0o700);
		const candidate = path.join(options.storeRoot, `held-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const socketPath = Buffer.byteLength(candidate) < 104
			? candidate
			: path.join("/tmp", `pi-held-${process.getuid?.() ?? 0}-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const boundary = new LinuxHeldExecBoundary(socketPath, native);
		await listenUnixSocket(boundary.server, socketPath);
		try { await chmod(socketPath, 0o600); options.signal?.throwIfAborted(); return boundary; } catch (error) { await boundary.close(); throw error; }
	}

	/** An existing whole-command adapter already owns its root; descendants still expose their operations. */
	static command<Value>(execute: () => Promise<Value>): Promise<Value> {
		const scope = startupScope.getStore();
		return scope ? startupScope.run({ ...scope, skipRoot: true }, execute) : execute();
	}

	/** The callback owns admission; each actual native child owns a distinct ticket and observer. */
	async observe<Value>(options: HeldObservation, execute: () => Promise<Value>,
		remote?: { readonly synchronous: (gate: number, skipRoot?: boolean) => { readonly pid?: number; readonly finish: () => void };
			readonly asynchronous: (gate: number, skipRoot?: boolean) => HeldExecAttachment }): Promise<Value> {
		if (this.closing || options.signal?.aborted) return execute();
		const controller = new AbortController(), launches = new Set<{ readonly child: ChildProcess; readonly attachment: HeldExecAttachment }>();
		const signal = AbortSignal.any([controller.signal, ...(options.signal ? [options.signal] : [])]);
		const snapshot = { ...options, scope: snapshotExecutionScope(options.scope), signal };
		let closed = false;
		const call = { controller, ...Promise.withResolvers<void>() };
		this.calls.add(call); enterStartup();
		try {
			const available = () => !closed && !signal.aborted;
			return await startupScope.run({ skipRoot: options.skipRoot,
				...(remote ? { synchronous: (start: () => unknown) => available() ? this.start(gate => remote.synchronous(gate, startupScope.getStore()?.skipRoot), start) : start() } : {}),
				launch: (child, start) => available() ? this.start(gate => {
					const attachment = remote ? remote.asynchronous(gate, startupScope.getStore()?.skipRoot) : this.attach({ ...snapshot, skipRoot: startupScope.getStore()?.skipRoot }, gate);
					if (remote) { signal.addEventListener("abort", attachment.retire, { once: true });
						const settled = () => signal.removeEventListener("abort", attachment.retire); void attachment.completion.then(settled, settled); }
					launches.add({ child, attachment }); return { pid: attachment.pid, finish: () => { if (!child.pid) attachment.retire(); } };
				}, start) : start() }, execute);
		} finally {
			closed = true;
			for (const { child, attachment } of launches) if (!child.pid || child.exitCode === null && child.signalCode === null) attachment.retire();
			const settled = await Promise.allSettled([...launches].map(launch => launch.attachment.completion));
			this.calls.delete(call); call.resolve(); restoreStartup!();
			for (const result of settled) if (result.status === "rejected") throw result.reason;
		}
	}

	private start<Value>(attach: (gate: number) => { readonly pid?: number; readonly finish: () => void }, start: () => Value): Value {
		let attachment: ReturnType<typeof attach> | undefined;
		let gate = -1;
		try {
			gate = this.startup(0);
			if (!Number.isInteger(gate) || gate < 0) throw new Error("native startup gate unavailable");
			attachment = attach(gate);
			if (!attachment.pid || this.startup(attachment.pid) !== 0) throw new Error("native observer unavailable");
		} catch {
			if (gate >= 0) this.startup(-1);
			try { attachment?.finish(); } catch { /* No authoritative child was started. */ }
			attachment = undefined;
		}
		// Never catch the authoritative spawn: its synchronous failure must not rerun it.
		try { return start(); } finally { if (gate >= 0) this.startup(-1); attachment?.finish(); }
	}

	/** The gate belongs to the launching thread; an observer may serve it from another worker. */
	attach(options: HeldObservation, gate: number): HeldExecAttachment {
		const { observer, completion, retire } = this.createAttachment(options, gate);
		return { pid: observer.pid, completion, retire };
	}

	private createAttachment(options: HeldObservation, gate: number) {
		if (this.closing) throw new Error("native observation is closing");
		const execution = randomBytes(24).toString("hex"), controller = new AbortController();
		const observer = startupScope.exit(() => spawn(this.observerBinary, ["--attach"], { stdio: ["pipe", "pipe", "ignore", gate], env: {
			PI_SPEC_HELD_EXEC_SOCKET: this.socketPath, PI_SPEC_HELD_EXEC_TOKEN: this.token, PI_SPEC_HELD_EXEC_ID: execution,
			...(options.descriptors ? { PI_SPEC_HELD_EXEC_DESCRIPTORS: options.descriptors === "inspect" ? "2" : "1" } : {}),
			...(options.skipRoot ? { PI_SPEC_HELD_EXEC_ROOT: "skip" } : {}),
		} }));
		observer.on("error", () => {}); observer.stdin?.on("error", () => {}); observer.unref();
		const done = new Promise<void>(resolve => {
			observer.once("close", () => resolve());
			observer.stdout!.once("data", data => { if (data.toString() === "R") { observer.stdout!.destroy(); resolve(); } });
		});
		const signal: AbortSignal = AbortSignal.any([controller.signal, ...(options.signal ? [options.signal] : [])]);
		const retire = (): void => {
			if (!controller.signal.aborted) controller.abort(new Error("native observation retired"));
			observer.stdin?.end(); for (const socket of active.sockets) socket.destroy();
		};
		const active = { ...options, signal, observer, retire, sequence: 0, failure: undefined as Error | undefined, decide: startupScope.exit(() => AsyncResource.bind(options.decide)),
			barrier: (options.descriptors === "inspect" ? "inspection" : options.descriptors ? "descriptors" : "none") as HeldExecTiming["barrier"],
			pending: new Set<Promise<void>>(), sockets: new Set<net.Socket>(), completion: done.then(async (): Promise<void> => {
				await Promise.allSettled(active.pending); signal.removeEventListener("abort", retire); this.active.delete(execution);
				if (active.failure) throw active.failure;
			}),
		};
		active.completion.catch(() => undefined); this.active.set(execution, active); signal.addEventListener("abort", retire, { once: true });
		return active;
	}

	close(): Promise<void> {
		return this.closing ??= Promise.resolve().then(async () => {
			const stopped = new Promise<void>((resolve) => this.server.close(() => resolve()));
			for (const call of this.calls) call.controller.abort(new Error("held-exec boundary disposed"));
			const active = [...this.active.values()]; for (const execution of active) execution.retire();
			for (const socket of this.sockets) socket.destroy();
			await Promise.allSettled([stopped, ...[...this.calls].map(call => call.promise), ...active.map(execution => execution.completion)]);
			await rm(this.socketPath, { force: true }).catch(() => undefined);
		});
	}

	private async serve(socket: net.Socket): Promise<void> {
		const requestedAt = performance.now();
		let prepared = false;
		let active: ActiveExecution | undefined;
		let decision: HeldExecDecision | undefined, native: HeldExecClock | undefined;
		let completedAt: number | undefined, committedAt: number | undefined;
		let outcome: HeldExecTiming["outcome"] = "declined";
		const { promise: pending, resolve: release } = Promise.withResolvers<void>();
		try {
			const request = parseRequest(await readLine(socket));
			active = request?.token === this.token ? this.active.get(request.execution) : undefined;
			active?.pending.add(pending);
			active?.sockets.add(socket);
			if (!request || !active || !(await heldBy(request, active.observer))) { return void socket.end("C\n"); }
			active.signal?.throwIfAborted();
			decision = await active.decide({
				id: `${request.execution}:${++active.sequence}`,
				sequence: active.sequence,
				pid: request.pid,
				tracerPid: request.tracer,
				...(request.holder ? { holderPid: request.holder } : {}),
				sourceRoot: active.sourceRoot,
				scope: active.scope,
				...(active.signal ? { signal: active.signal } : {}),
				...(request.descriptors ? { descriptors: request.descriptors } : {}),
				...(request.trackQueues ? { trackQueues: true as const } : {}),
			});
			active.signal?.throwIfAborted();
			if (decision.kind === "continue") {
				completedAt = performance.now(); outcome = "continued";
				if (!decision.observeCompletion) return void socket.end("C\n");
				native = await observeCompletion(socket, decision.observeCompletion);
				if (native) completedAt = native.startedAt;
				return;
			}
			const positions = decision.descriptorOffsets?.map(position => ({ ...position, afterFlags: position.afterFlags ?? position.flags })) ?? [];
			const resourceEvents = decision.resourceEvents ?? [];
			const continuation = decision.continuation;
			const code = continuation ? 256 : decision.exitCode!;
			const physicalRoot = Buffer.from(continuation?.physicalRoot ?? ""), sourceRoot = Buffer.from(continuation ? active.sourceRoot : "");
			const total = [...decision.output, ...resourceEvents].reduce((sum, event) => sum + event.data.length, 0) +
				positions.reduce((sum, position) => sum + (position.content?.length ?? 0) + Buffer.byteLength(position.path ?? ""), 0) +
				(continuation?.image.length ?? 0) + physicalRoot.length + sourceRoot.length;
			const descriptors = new Set<number>();
			if (!isSafeCount(code, 0, continuation ? 256 : 255) ||
				continuation && (!Buffer.isBuffer(continuation.image) || !continuation.image.length || continuation.image.length > 65 * 1024 * 1024 ||
					!path.isAbsolute(continuation.physicalRoot) || continuation.physicalRoot.includes("\0") || physicalRoot.length >= 4096 || sourceRoot.length >= 4096) ||
				decision.output.length > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || positions.length > 64 || positions.some(position => {
					const duplicate = descriptors.has(position.fd); descriptors.add(position.fd);
					return duplicate || position.path !== undefined && (typeof position.path !== "string" || !path.isAbsolute(position.path) || position.path.includes("\0") || Buffer.byteLength(position.path) >= 4096) ||
						!validQueueMessages(position.messages, position.content?.length ?? 0) ||
						position.content !== undefined && !Buffer.isBuffer(position.content) ||
						![position.capacity ?? 0, position.event ?? 0, ...Object.values(position.socket ?? {})].every(value => isSafeCount(value)) || (position.event ?? 0) > 0x80000000 ||
						![position.before, position.after].every(isOFDPosition) ||
						![position.fd, position.flags, position.afterFlags].every(value => isSafeCount(value)) ||
						((position.flags ^ position.afterFlags) & ~0xc00) !== 0 ||
						position.fd > 0x7fffffff || position.flags > 0x7fffffff || position.afterFlags > 0x7fffffff || ![position.device, position.inode].every(value =>
							typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 0xffffffffffffffffn);
				}) || resourceEvents.length > 1024 || resourceEvents.some(event => !descriptors.has(event.fd) ||
					!RESOURCE_TRANSITIONS.includes(event.kind) || !Buffer.isBuffer(event.data) || event.data.length > 2 * 1024 * 1024 ||
					event.requested !== undefined && (!isSafeCount(event.requested, 0, 2 * 1024 * 1024)))) return void socket.end("C\n");
			// Once a proposal is delivered the peer may arm its exit stub, even if its ACK is lost.
			prepared = true;
			await writeProcessChannel(socket, Buffer.from(`P ${code} ${decision.output.length} ${total} ${positions.length} ${resourceEvents.length} ${continuation?.image.length ?? 0} ${physicalRoot.length} ${sourceRoot.length}\n`));
			for (const position of positions) {
				await writeProcessChannel(socket, Buffer.from(`S ${position.fd} ${position.device} ${position.inode} ${position.flags} ${position.before} ${position.after} ${position.content?.length ?? -1} ${position.afterFlags} ${Buffer.byteLength(position.path ?? "")} ${position.eof === undefined ? -1 : Number(position.eof)} ${position.capacity ?? 0} ${position.socket?.shutdown ?? 0} ${position.socket?.peerShutdown ?? 0} ${position.socket?.peerInode ?? 0} ${position.socket?.peerQueued ?? 0} ${position.socket?.allocated ?? 0} ${position.event ?? 0} ${position.messages?.length ?? 0} ${position.socket ? position.socket.type ?? 1 : 0}\n${position.messages?.map(message => `M ${position.fd} ${message.start} ${message.end} ${message.rights.length} ${message.rights.join(" ")}\n`).join("") ?? ""}`));
				if (position.path) await writeProcessChannel(socket, Buffer.from(position.path));
				if (position.content) await writeProcessChannel(socket, position.content);
			}
			for (const event of resourceEvents) {
				await writeProcessChannel(socket, Buffer.from(`Q ${RESOURCE_TRANSITIONS.indexOf(event.kind)} ${event.fd} ${event.data.length} ${event.requested ?? event.data.length}\n`));
				await writeProcessChannel(socket, event.data);
			}
			for (const event of decision.output) {
				await writeProcessChannel(socket, Buffer.from(`O ${event.fd} ${event.data.length}\n`));
				await writeProcessChannel(socket, event.data);
			}
			if (continuation) { await writeProcessChannel(socket, physicalRoot); await writeProcessChannel(socket, sourceRoot); await writeProcessChannel(socket, continuation.image); }
			const acknowledgement = await readLine(socket);
			if (acknowledgement === "N") { prepared = false; return void socket.end(); }
			if (acknowledgement !== "A") throw new Error("held-exec adoption was not acknowledged");
			active.signal?.throwIfAborted();
			await decision.commit();
			committedAt = performance.now();
			await writeProcessChannel(socket, Buffer.from("R\n"));
			if ((await readLine(socket)) !== "D") throw new Error("held-exec adoption completion is unknown");
			completedAt = performance.now(); outcome = "adopted";
			try { decision.adopted?.(); } catch { /* Feedback cannot poison a completed handoff. */ }
			socket.end();
		} catch (error) {
			completedAt ??= performance.now(); outcome = "failed";
			if (active && (prepared || isPoisonedEffectCommit(error))) {
				// The logical call, not just the held child, owns an irreversible handoff.
				active.failure ??= effectCommitFailure(new Error("held-exec handoff failed", { cause: error }), "poisoned");
			}
			if (!socket.destroyed) socket.end(active?.failure ? "F\n" : "C\n");
		} finally {
			if (decision?.kind === "replay") decision.settle?.(prepared);
			if (active && decision?.observeTiming) try {
				decision.observeTiming({ requestedAt, completedAt: completedAt ?? performance.now(), barrier: active.barrier, outcome,
					...(committedAt !== undefined ? { committedAt } : {}), ...(native ? { native } : {}) });
			} catch { /* Timing evidence cannot replace the process outcome. */ }
			active?.pending.delete(pending);
			active?.sockets.delete(socket);
			release();
		}
	}
}

async function observeCompletion(socket: net.Socket,
	observe: (durationMs: number | undefined, clock?: HeldExecClock) => void | Promise<void>): Promise<HeldExecClock | undefined> {
	const startedAt = performance.now();
	let clock: HeldExecClock | undefined;
	try {
		await writeProcessChannel(socket, Buffer.from("O\n"));
		if (await readLine(socket) === "D") clock = Object.freeze({ startedAt, completedAt: performance.now() });
	} finally {
		try {
			await observe(clock && Math.max(0, clock.completedAt - clock.startedAt), clock);
		} catch {
			// Drain optional observation without replacing the already-authorized process outcome.
		}
		socket.end();
	}
	return clock;
}

/** Resolve the native helper shared by transparent dispatch and x86-64 Actor handoff. */
export async function resolveLinuxExecHelper(binary?: string): Promise<string> {
	if (process.platform !== "linux") throw new Error("Linux required");
	const resolved = await realpath(binary ?? path.join(os.homedir(), ".local", "bin", "pi-speculative-held-exec"));
	await access(resolved, fsConstants.X_OK);
	return resolved;
}

/** Inspect the installed, stopped image; stdio mode leaves extra descriptors to the broker capture. */
export async function inspectHeldExecProcess(pid: number, executable: string, descriptors?: readonly HeldFileDescriptor[] | "stdio"): Promise<HeldExecSnapshot & { readonly context: ProcessExecutionContext }> {
	const root = `/proc/${pid}`;
	const [cwd, command, environmentBytes, descriptorNames] = await Promise.all([
		readlink(`${root}/cwd`),
		readFile(`${root}/cmdline`),
		readFile(`${root}/environ`),
		readdir(`${root}/fd`),
	]);
	const evidence = descriptors === "stdio" ? undefined : descriptors ?? [];
	if (!validDescriptors(evidence)) throw new Error("invalid native descriptor context");
	const context = await captureProcessContext(pid, descriptors === "stdio" ? descriptorNames.filter(name => /^[012]$/.test(name)) : descriptorNames,
		evidence?.filter(descriptor => descriptor.pin === undefined));
	if (descriptors !== "stdio" && !validProcessContext(context)) throw new Error("held process descriptors are not replayable");
	const argv = decodeNullFields(command);
	if (!argv.length) throw new Error("held process argv is empty");
	const environment: Record<string, string> = Object.create(null);
	for (const entry of decodeNullFields(environmentBytes)) {
		const separator = entry.indexOf("=");
		if (separator < 1 || Object.hasOwn(environment, entry.slice(0, separator))) throw new Error("held process environment is not canonical");
		environment[entry.slice(0, separator)] = entry.slice(separator + 1);
	}
	let invokedPath: string | undefined;
	{
		const auxiliary = await readFile(`${root}/auxv`), memory = await open(`${root}/mem`, "r");
		try {
			for (let at = 0; at + 16 <= auxiliary.length; at += 16) if (auxiliary.readBigUInt64LE(at) === 31n) {
				const bytes = Buffer.alloc(8192), { bytesRead } = await memory.read(bytes, 0, bytes.length, Number(auxiliary.readBigUInt64LE(at + 8)));
				const end = bytes.subarray(0, bytesRead).indexOf(0);
				if (end >= 0) invokedPath = bytes.subarray(0, end).toString();
				break;
			}
		} finally { await memory.close(); }
		if (!invokedPath) throw new Error("held process invocation name is unavailable");
	}
	return {
		executable, ...processLaunchOptions(context),
		argv, cwd, environment, context, invocation: [invokedPath, (await readFile(`${root}/comm`, "utf8")).replace(/\n$/, "")],
	};
}

/** Capture one image per inode; directory anchors and null devices need no payload. Only a native lease authorizes adoption. */
export async function captureHeldDescriptorInputs(pid: number, descriptors: readonly HeldFileDescriptor[], maxBytes: number, deniedPaths: readonly string[] = [],
	inputs?: ResourceObjectInputs, tracerPid?: number, namespaceRoot?: string): Promise<ProcessResourceGraph> {
	const handles: Array<ProcessResourceGraph["handles"][number]> = [];
	const descriptions: Record<number, ProcessResourceGraph["descriptions"][number]> = {};
	const objects: Record<number, ProcessResourceGraph["objects"][number]> = {};
	const images = new Map<string, number>();
	let remaining = maxBytes;
	const storeObject = (file: string, fd: number, object: Omit<ProcessResourceGraph["objects"][number], "contentDigest" | "content">, bytes?: Buffer, digest?: Sha256Digest) => {
		remaining -= bytes?.length ?? 0;
		if (remaining < 0) throw new Error(`inherited ${object.type} exceeds input budget`);
		objects[fd] = { ...object, contentDigest: digest ?? sha256Digest(bytes ?? ""), ...(bytes ? { content: bytes.toString("base64") } : {}) };
		images.set(file, fd);
	};
	const aliases = captureWorkspaceFileAliases(namespaceRoot);
	for (const descriptor of descriptors) {
		const { fd, alias: representative, flags, offset } = descriptor;
		if (descriptor.pin !== undefined && (!isSafeCount(tracerPid, 1))) throw new Error("queued OFD has no authenticated holder");
		const holder = descriptor.pin === undefined ? pid : tracerPid!, handle = descriptor.pin ?? fd;
		const heldPath = `/proc/${holder}/fd/${handle}`;
		if ((descriptor.flags & 3) === 3 || descriptor.fd === 1 || descriptor.fd === 2) throw new Error("unsupported inherited descriptor effects");
		const file = `${descriptor.device}:${descriptor.inode}${descriptor.counter ? `:${descriptor.counter.id}` : ""}`, image = images.get(file);
		const object = image ?? fd;
		if (descriptor.pin === undefined) handles.push({ fd, description: representative });
		if (fd === representative) descriptions[representative] = { object, flags, ...(!descriptor.type || descriptor.type === "directory" ? { position: offset } : {}),
			...((!descriptor.type || descriptor.type === "eventfd") && descriptor.outside !== undefined ? { outside: descriptor.outside } : {}),
			...(descriptor.locks ? { locks: descriptor.locks } : {}) };
		if (image !== undefined && descriptor.type === "directory" && fd === representative && await readlink(heldPath) !== objects[image]!.sourcePath)
			throw new Error("inherited directory namespace aliases are unproven");
		if (image !== undefined) continue;
		const endpoint = await readlink(heldPath);
		if (deniedPaths.some(denied => containsFilesystemPath(denied, endpoint) || containsFilesystemPath(denied, endpoint.replace(/ \(deleted\)$/, ""))))
			throw new Error("inherited descriptor refers to a denied resource");
		if (descriptor.type) {
			if (descriptor.type === "eventfd") {
				if (endpoint !== "anon_inode:[eventfd]" || !descriptor.counter) throw new Error("unproven inherited counter");
				const bytes = Buffer.alloc(9); bytes.writeBigUInt64LE(BigInt(descriptor.counter.value)); bytes[8] = descriptor.counter.semaphore;
				storeObject(file, fd, { type: "eventfd", counter: { value: descriptor.counter.value, semaphore: descriptor.counter.semaphore } }, bytes);
				continue;
			}
			if (descriptor.type === "pipe" || descriptor.type === "socket") {
				if (!/^(pipe|socket):\[\d+\]$/.test(endpoint) || descriptor.queueHex === undefined) throw new Error("unproven inherited stream");
				const bytes = Buffer.from(descriptor.queueHex, "hex");
				storeObject(file, fd, { type: descriptor.type,
					queue: { eof: descriptor.eof!, bytes: bytes.length, capacity: descriptor.capacity!, producer: descriptor.eof ? "closed" : "live", outside: descriptor.outside ?? 3,
						...(descriptor.messages ? { messages: descriptor.messages } : {}) },
					...(descriptor.socket ? { socket: { ...(descriptor.socket.type ? { type: descriptor.socket.type } : {}), shutdown: descriptor.socket.shutdown, allocated: descriptor.socket.allocated,
						peer: { connected: !!descriptor.socket.peerInode, shutdown: descriptor.socket.peerShutdown, bytes: descriptor.socket.peerQueued } } } : {}) }, bytes);
				continue;
			}
			if (descriptor.type === "directory") {
				const metadata = await stat(endpoint, { bigint: true });
				if (!metadata.isDirectory() || String(metadata.dev) !== descriptor.device || String(metadata.ino) !== descriptor.inode) throw new Error("held directory pathname changed");
				const directoryHex = descriptors.find(other => other.device === descriptor.device && other.inode === descriptor.inode && other.directoryHex !== undefined)?.directoryHex;
				if (directoryHex !== undefined) {
					const bytes = Buffer.from(directoryHex, "hex");
					storeObject(file, fd, { type: "directory", sourcePath: endpoint }, bytes);
					if (inputs) {
						const borrowed = await inputs.borrow(endpoint, metadata, bytes.length, capture => !!capture.content?.equals(bytes));
						if (!borrowed) {
							const captured = await captureHeldDirectory(holder, handle, bytes, metadata, endpoint).catch(() => undefined);
							if (captured && !inputs.retain(endpoint, captured)) await captured.object?.dispose();
						}
					}
					continue;
				}
			}
			storeObject(file, fd, { type: descriptor.type, ...(descriptor.type === "directory" ? { sourcePath: endpoint } : {}) });
			continue;
		}
		let captured = inputs && await inputs.borrow(endpoint, await stat(heldPath, { bigint: true }), remaining,
			async capture => sameFilesystemIdentity(capture.stat, await stat(heldPath, { bigint: true })));
		const borrowed = !!captured;
		captured ??= await captureStableFile(heldPath, remaining, true, { retainObject: !!inputs, verifyPath: false });
		let retained = borrowed;
		try {
			if (String(captured.stat.dev) !== descriptor.device || String(captured.stat.ino) !== descriptor.inode || !captured.content) {
				throw new Error("held descriptor identity changed");
			}
			const sourcePath = captured.stat.nlink ? endpoint : undefined;
			const sourceAliases = await aliases(endpoint, captured.stat);
			if (sourcePath) {
				const metadata = await stat(sourcePath, { bigint: true });
				if (metadata.dev !== captured.stat.dev || metadata.ino !== captured.stat.ino) throw new Error("held descriptor pathname changed");
			}
			storeObject(file, fd, { type: "regular", ...(sourcePath ? { sourcePath } : {}), ...(sourceAliases ? { sourceAliases } : {}) },
				captured.content, `sha256:${captured.hash}`);
			if (!borrowed && sourcePath && inputs) retained = inputs.retain(sourcePath, { ...captured, realPath: sourcePath });
		} finally { if (!retained) await captured.object?.dispose(); }
	}
	for (const descriptor of descriptors) {
		const object = descriptions[descriptor.alias]!.object, socket = objects[object]!.socket;
		if (!socket) continue;
		const peer = images.get(`${descriptor.device}:${descriptor.socket!.peerInode}`);
		if (peer !== undefined) objects[object] = { ...objects[object]!, socket: { ...socket, peer: { ...socket.peer, object: peer } } };
	}
	return { handles, descriptions, objects };
}

function decodeNullFields(bytes: Buffer): string[] {
	if (!isUtf8(bytes)) throw new Error("held process metadata is not valid UTF-8");
	const fields = bytes.toString("utf8").split("\0");
	if (fields.at(-1) === "") fields.pop();
	return fields;
}

function parseRequest(line: string): WireRequest | undefined {
	try {
		const value = JSON.parse(line) as Partial<WireRequest>;
		return /^[0-9a-f]{64}$/.test(value.token ?? "") &&
			/^[0-9a-f]{48}$/.test(value.execution ?? "") && isSafeCount(value.pid, 1) &&
			isSafeCount(value.tracer, 1) && (value.holder === undefined || isSafeCount(value.holder)) &&
			(value.trackQueues === undefined || value.trackQueues === true && value.descriptors === undefined) &&
			validDescriptors(value.descriptors) ? value as WireRequest : undefined;
	} catch {
		return undefined;
	}
}

function validDescriptors(descriptors: WireRequest["descriptors"]): boolean {
	if (descriptors === undefined) return true;
	if (!Array.isArray(descriptors) || descriptors.length > 64) return false;
	let previous = -1;
	const aliases = new Map<number, HeldFileDescriptor>();
	for (const descriptor of descriptors) {
		if (descriptor?.pin !== undefined && (!isSafeCount(descriptor.pin, 0, 0x7fffffff) || descriptor.alias !== descriptor.fd)) return false;
		if (!descriptor || descriptor.type !== undefined && !["null", "directory", "pipe", "socket", "eventfd"].includes(descriptor.type) || descriptor.type !== undefined && descriptor.type !== "directory" && descriptor.offset !== 0 ||
			!validOFDLocks(descriptor.locks) || descriptor.locks !== undefined && (descriptor.type !== undefined || descriptor.fd !== descriptor.alias) ||
			(descriptor.type === "directory" && !(descriptor.flags & 0x200000) ? typeof descriptor.directoryHex !== "string" || descriptor.directoryHex.length > 4 * 1024 * 1024 || !/^(?:[0-9a-f]{2})*$/.test(descriptor.directoryHex) : descriptor.directoryHex !== undefined) ||
			(descriptor.type === "eventfd" ? !descriptor.counter || !isSafeCount(descriptor.counter.id, 0, 0x7fffffff) ||
				!/^(0|[1-9][0-9]{0,19})$/.test(descriptor.counter.value) || BigInt(descriptor.counter.value) >= 0xffffffffffffffffn || ![0, 1].includes(descriptor.counter.semaphore) : descriptor.counter !== undefined) ||
			(descriptor.outside !== undefined && (!isSafeCount(descriptor.outside, 0, 3) ||
				(descriptor.type !== undefined && descriptor.type !== "eventfd" ? !["pipe", "socket"].includes(descriptor.type) : descriptor.outside !== 0 && descriptor.outside !== 3))) ||
			(descriptor.messages !== undefined && (descriptor.type !== "socket" || !validQueueMessages(descriptor.messages, (descriptor.queueHex?.length ?? 0) / 2))) ||
			(descriptor.type === "pipe" || descriptor.type === "socket" ? typeof descriptor.eof !== "boolean" || typeof descriptor.queueHex !== "string" || descriptor.queueHex.length > 4 * 1024 * 1024 || descriptor.queueHex.length % 2 !== 0 || !/^[0-9a-f]*$/.test(descriptor.queueHex) || !isSafeCount(descriptor.capacity, 4096) : descriptor.queueHex !== undefined || descriptor.eof !== undefined || descriptor.capacity !== undefined) ||
			(descriptor.type === "socket" ? !descriptor.socket || ![1, 2, 5].includes(descriptor.socket.type ?? 1) || ![descriptor.socket.shutdown, descriptor.socket.peerShutdown].every(value => isSafeCount(value, 0, 3)) || ![descriptor.socket.peerInode, descriptor.socket.peerQueued, descriptor.socket.allocated].every(value => isSafeCount(value)) : descriptor.socket !== undefined) ||
			!isOFDPosition(descriptor.offset) || ![descriptor.fd, descriptor.alias, descriptor.flags].every(value => isSafeCount(value)) ||
			descriptor.fd <= previous || descriptor.fd > 0x7fffffff || descriptor.alias > descriptor.fd || descriptor.flags > 0x7fffffff ||
			typeof descriptor.owned !== "boolean" || ![descriptor.device, descriptor.inode].every(value =>
				typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 0xffffffffffffffffn)) return false;
		const alias = aliases.get(descriptor.alias);
		if (descriptor.alias !== descriptor.fd && (!alias || alias.device !== descriptor.device || alias.inode !== descriptor.inode ||
			alias.type !== descriptor.type || alias.flags !== descriptor.flags || alias.offset !== descriptor.offset || alias.owned !== descriptor.owned || alias.outside !== descriptor.outside ||
			JSON.stringify(alias.counter) !== JSON.stringify(descriptor.counter) || JSON.stringify(alias.messages) !== JSON.stringify(descriptor.messages))) return false;
		if (descriptor.alias === descriptor.fd) aliases.set(descriptor.fd, descriptor);
		previous = descriptor.fd;
	}
	const reachable = new Set(descriptors.filter(descriptor => descriptor.pin === undefined).map(descriptor => descriptor.alias));
	for (const id of reachable) for (const message of aliases.get(id)?.messages ?? []) for (const right of message.rights) {
		if (!aliases.has(right)) return false;
		reachable.add(right);
	}
	return descriptors.every(descriptor => reachable.has(descriptor.alias));
}

/** A tracer already on this process (a ThinkThread Runtime, a debugger) follows its children and holds their only ptrace slot. */
export async function inheritedTracer(): Promise<string> {
	const tracer = /^TracerPid:\s*([1-9]\d*)$/m.exec(await readFile("/proc/self/status", "utf8").catch(() => ""))?.[1];
	return tracer ? `: processes here are already traced by pid ${tracer}` : "";
}

async function heldBy({ pid, tracer, holder }: WireRequest, observer: ChildProcess): Promise<boolean> {
	const alive = () => observer.pid === tracer && observer.exitCode === null && observer.signalCode === null;
	if (!alive()) return false;
	try {
		const status = await readFile(`/proc/${pid}/status`, "utf8");
		if (holder) {
			const owner = await readFile(`/proc/${holder}/status`, "utf8");
			if (!new RegExp(`^PPid:\\s*${tracer}$`, "m").test(owner) || !/^NoNewPrivs:\s+1$/m.test(owner) ||
				!["Prm", "Eff", "Inh", "Amb"].every(kind => new RegExp(`^Cap${kind}:\\s+0+$`, "m").test(owner)) ||
				![["Uid", process.getuid!()], ["Gid", process.getgid!()]].every(([kind, id]) => new RegExp(`^${kind}:(?:\\s+${id}){4}$`, "m").test(owner))) return false;
		}
		return alive() && new RegExp(`^TracerPid:\\s*${tracer}$`, "m").test(status) && /^State:\s+t\b/m.test(status);
	} catch { return false; }
}

async function readLine(socket: net.Socket): Promise<string> {
	const chunks = on(socket, "data", { close: ["end", "close"] });
	let body = "";
	try {
		socket.resume();
		for await (const [chunk] of chunks) {
			body += chunk.toString("utf8");
			const newline = body.indexOf("\n");
			if (newline >= 0) return body.slice(0, newline);
			if (Buffer.byteLength(body) > MAX_REQUEST_BYTES) throw new Error("held-exec message too large");
		}
		throw new Error("held-exec peer closed");
	} finally { socket.pause(); }
}

export function writeProcessChannel(channel: NodeJS.WritableStream, data: Buffer): Promise<void> {
	return new Promise((resolve, reject) => channel.write(data, error => error ? reject(error) : resolve()));
}

export async function listenUnixSocket(server: net.Server, socketPath: string): Promise<void> {
	await once(server.listen(socketPath), "listening");
}
