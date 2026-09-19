import { execFile } from "node:child_process";
import { AsyncResource } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, mkdir, readFile, readdir, readlink, realpath, rm, stat } from "node:fs/promises";
import { captureProcessContext, validProcessContext, type ProcessExecutionContext } from "./process-context.mjs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import type { ProcessExecutor } from "./process-execution.ts";
import { captureHeldDirectory, captureHeldFile, sameFilesystemIdentity } from "./filesystem-evidence.ts";
import { borrowResourceObject, retainResourceObject } from "./agent-execution-world.ts";
import { sha256Digest, isOFDPosition, type OFDPosition, RESOURCE_TRANSITIONS, type ResourceTransitionKind, type Sha256Digest, type ProcessResourceEffects, type ArtifactReference } from "./provenance-certificate.ts";
import { containsFilesystemPath } from "./path-utils.ts";
import { snapshotExecutionScope, type ExecutionScope } from "./execution-world.ts";
import { captureWorkspaceStructure } from "./process-observation.ts";

const HELPER_PROTOCOL_VERSION = 34;
const WIRE_PROTOCOL_VERSION = 1;
const MAX_REQUEST_BYTES = 4 * 1024 * 1024 + 32768;
const MAX_OUTPUT_EVENTS = 65_536;
const MAX_OUTPUT_BYTES = 512 * 1024 * 1024;
const PRIVATE_ENV = {
	shell: "PI_SPEC_HELD_EXEC_SHELL",
	socket: "PI_SPEC_HELD_EXEC_SOCKET",
	token: "PI_SPEC_HELD_EXEC_TOKEN",
	execution: "PI_SPEC_HELD_EXEC_ID",
	descriptors: "PI_SPEC_HELD_EXEC_DESCRIPTORS",
} as const;
const PRIVATE_ENV_NAMES: readonly string[] = Object.values(PRIVATE_ENV);

export interface HeldExecProcess {
	/** Host-owned launch identity and ordered exec event; never a reusable bare PID. */
	readonly id: string;
	readonly sequence: number;
	readonly pid: number;
	readonly tracerPid: number;
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
			message.rights.some((fd: number) => !Number.isInteger(fd) || fd < 0 || fd > 0x7fffffff)) return false;
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
interface FileDescriptorInput extends Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type"> {
	readonly locks?: readonly OFDLock[];
	readonly outside?: number;
	readonly installed?: false;
	readonly contentDigest: Sha256Digest;
	/** One image per inode; distinct OFDs open it independently. */
	readonly image: number;
	readonly sourcePath?: string;
	readonly sourceAliases?: readonly string[];
	readonly content?: string;
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
			...(ofd.outside !== undefined ? { outside: ofd.outside } : {}),
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
			position.afterFlags !== undefined && (!Number.isSafeInteger(position.afterFlags) || position.afterFlags < 0 || position.afterFlags > 0x7fffffff ||
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

export interface HeldExecSnapshot {
	readonly executable: string;
	/** Actual output aliasing, usable for another isolated launch after context revalidation. */
	readonly outputRoute?: readonly [1 | 2, 1 | 2];
	readonly outputPipes?: readonly [boolean, boolean];
	readonly argv: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: Pick<ProcessExecutionContext, "key" | "umask" | "descriptorTypes" | "regularDescriptors">;
	readonly resources?: ProcessResourceGraph;
}

export type HeldExecDecision =
	| { readonly kind: "continue"; readonly observeCompletion?: (durationMs: number | undefined) => void | Promise<void> }
	| ({
			readonly kind: "replay";
			readonly output: readonly { readonly fd: 1 | 2; readonly data: Buffer }[];
			readonly resourceEvents?: readonly { readonly fd: number; readonly kind: ResourceTransitionKind; readonly data: Buffer; readonly requested?: number }[];
			/** Applied after commit, before output. The caller owns predecessor proof and serialization of every OFD sharer. */
			readonly descriptorOffsets?: readonly {
				readonly fd: number; readonly device: string; readonly inode: string;
				readonly flags: number; readonly before: OFDPosition; readonly after: OFDPosition; readonly afterFlags?: number; readonly path?: string;
				/** File replacement bytes, or the expected full pipe queue before consuming `after` bytes. */
				readonly content?: Buffer;
				readonly eof?: boolean;
				readonly capacity?: number;
				readonly socket?: HeldFileDescriptor["socket"];
				readonly event?: number;
				readonly messages?: readonly QueueMessage[];
			}[];
			/** Called only after the native tracer has made original execution impossible. */
			readonly commit: () => Promise<void>;
			readonly adopted?: () => void;
	  } & ({ readonly exitCode: number; readonly continuation?: never } |
		{ readonly exitCode?: never; readonly continuation: { readonly image: Buffer; readonly physicalRoot: string } }));

export interface LinuxHeldExecOptions {
	readonly storeRoot: string;
	readonly binary?: string;
}

interface ActiveExecution {
	sequence: number;
	readonly sourceRoot: string;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	readonly decide: (process: HeldExecProcess) => Promise<HeldExecDecision>;
	readonly pending: Set<Promise<void>>;
	readonly controller: AbortController;
	readonly completion: Promise<void>;
	failure?: Error;
}

interface WireRequest {
	readonly version: 1;
	readonly token: string;
	readonly execution: string;
	readonly pid: number;
	readonly tracer: number;
	readonly descriptors?: readonly HeldFileDescriptor[];
	readonly trackQueues?: true;
}

/** A two-phase held-exec transport: arm the exit stub before committing reusable effects. */
export class LinuxHeldExecBoundary {
	readonly shellPath: string;
	private readonly token = randomBytes(32).toString("hex");
	private readonly active = new Map<string, ActiveExecution>();
	private readonly server: net.Server;
	private readonly sockets = new Set<net.Socket>();
	private readonly socketPath: string;
	private closing?: Promise<void>;

	private constructor(binary: string, socketPath: string) {
		this.shellPath = binary;
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
		const binary = await resolveLinuxExecHelper(options.binary);
		const probe = await execute(binary, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"]);
		if (probe.code !== 42 || probe.signal) throw new Error("held-exec functional probe failed");
		await mkdir(options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(options.storeRoot, 0o700);
		const candidate = path.join(options.storeRoot, `held-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const socketPath = Buffer.byteLength(candidate) < 104
			? candidate
			: path.join(os.tmpdir(), `pi-held-${process.getuid?.() ?? 0}-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const boundary = new LinuxHeldExecBoundary(binary, socketPath);
		await listenUnixSocket(boundary.server, socketPath);
		try {
			await chmod(socketPath, 0o600);
			return boundary;
		} catch (error) { await boundary.close(); throw error; }
	}

	executor(
		host: ProcessExecutor,
		options: Pick<ActiveExecution, "sourceRoot" | "decide"> & { readonly realShell: string;
			readonly descriptors?: boolean | ((request: Parameters<ProcessExecutor["execute"]>[0]) => boolean | "inspect") },
		fallback: ProcessExecutor = host,
	): ProcessExecutor {
		return {
			execute: async (request) => {
				// The transport never owns caller-provided environment entries. A collision
				// therefore disables handoff for this launch and preserves stock Bash semantics.
				if (this.closing || request.signal?.aborted || PRIVATE_ENV_NAMES.some((name) => Object.hasOwn(request.environment, name))) {
					return fallback.execute(request);
				}
				const execution = randomBytes(24).toString("hex");
				const descriptors = typeof options.descriptors === "function" ? options.descriptors(request) : options.descriptors;
				const controller = new AbortController();
				let finished!: () => void;
				const active: ActiveExecution = {
					sequence: 0,
					sourceRoot: options.sourceRoot,
					scope: snapshotExecutionScope(request.scope),
					decide: AsyncResource.bind(options.decide),
					pending: new Set<Promise<void>>(), controller,
					completion: new Promise<void>((resolve) => { finished = resolve; }),
					signal: AbortSignal.any([controller.signal, ...(request.signal ? [request.signal] : [])]),
				};
				this.active.set(execution, active);
				try {
					return await host.execute({
						...request,
						signal: active.signal,
						environment: {
							...request.environment,
							[PRIVATE_ENV.shell]: options.realShell,
							[PRIVATE_ENV.socket]: this.socketPath,
							[PRIVATE_ENV.token]: this.token,
							[PRIVATE_ENV.execution]: execution,
							...(descriptors ? { [PRIVATE_ENV.descriptors]: descriptors === "inspect" ? "2" : "1" } : {}),
						},
					});
				} finally {
					await Promise.allSettled(active.pending);
					this.active.delete(execution);
					finished();
					if (active.failure) throw active.failure;
				}
			},
		};
	}

	close(): Promise<void> {
		return this.closing ??= Promise.resolve().then(async () => {
			const stopped = new Promise<void>((resolve) => this.server.close(() => resolve()));
			for (const active of this.active.values()) active.controller.abort(new Error("held-exec boundary disposed"));
			for (const socket of this.sockets) socket.destroy();
			await Promise.allSettled([stopped, ...[...this.active.values()].map((active) => active.completion)]);
			await rm(this.socketPath, { force: true }).catch(() => undefined);
		});
	}

	private async serve(socket: net.Socket): Promise<void> {
		let prepared = false;
		let active: ActiveExecution | undefined;
		let release!: () => void;
		const pending = new Promise<void>((resolve) => { release = resolve; });
		try {
			const request = parseRequest(await readLine(socket));
			active = request?.token === this.token ? this.active.get(request.execution) : undefined;
			active?.pending.add(pending);
			if (!request || !active || !(await heldBy(request.pid, request.tracer, this.shellPath))) {
				return void socket.end("C\n");
			}
			throwIfAborted(active.signal);
			const decision = await active.decide({
				id: `${request.execution}:${++active.sequence}`,
				sequence: active.sequence,
				pid: request.pid,
				tracerPid: request.tracer,
				sourceRoot: active.sourceRoot,
				scope: active.scope,
				...(active.signal ? { signal: active.signal } : {}),
				...(request.descriptors ? { descriptors: request.descriptors } : {}),
				...(request.trackQueues ? { trackQueues: true as const } : {}),
			});
			if (decision.kind === "continue") {
				if (!decision.observeCompletion) return void socket.end("C\n");
				await observeCompletion(socket, decision.observeCompletion);
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
			if (!Number.isSafeInteger(code) || code < 0 || code > (continuation ? 256 : 255) ||
				continuation && (!Buffer.isBuffer(continuation.image) || !continuation.image.length || continuation.image.length > 65 * 1024 * 1024 ||
					!path.isAbsolute(continuation.physicalRoot) || continuation.physicalRoot.includes("\0") || physicalRoot.length >= 4096 || sourceRoot.length >= 4096) ||
				decision.output.length > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || positions.length > 64 || positions.some(position => {
					const duplicate = descriptors.has(position.fd); descriptors.add(position.fd);
					return duplicate || position.path !== undefined && (typeof position.path !== "string" || !path.isAbsolute(position.path) || position.path.includes("\0") || Buffer.byteLength(position.path) >= 4096) ||
						!validQueueMessages(position.messages, position.content?.length ?? 0) ||
						position.content !== undefined && !Buffer.isBuffer(position.content) ||
						![position.capacity ?? 0, position.event ?? 0, ...Object.values(position.socket ?? {})].every(value => Number.isSafeInteger(value) && value >= 0) || (position.event ?? 0) > 0x80000000 ||
						![position.before, position.after].every(isOFDPosition) ||
						![position.fd, position.flags, position.afterFlags].every(value => Number.isSafeInteger(value) && value >= 0) ||
						position.fd > 0x7fffffff || position.flags > 0x7fffffff || position.afterFlags > 0x7fffffff || ![position.device, position.inode].every(value =>
							typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 0xffffffffffffffffn);
				}) || resourceEvents.length > 1024 || resourceEvents.some(event => !descriptors.has(event.fd) ||
					!RESOURCE_TRANSITIONS.includes(event.kind) || !Buffer.isBuffer(event.data) || event.data.length > 2 * 1024 * 1024 ||
					event.requested !== undefined && (!Number.isSafeInteger(event.requested) || event.requested < 0 || event.requested > 2 * 1024 * 1024))) return void socket.end("C\n");
			// Once a proposal is delivered the peer may arm its exit stub, even if its ACK is lost.
			prepared = true;
			await write(socket, Buffer.from(`P ${code} ${decision.output.length} ${total} ${positions.length} ${resourceEvents.length} ${continuation?.image.length ?? 0} ${physicalRoot.length} ${sourceRoot.length}\n`));
			for (const position of positions) {
				await write(socket, Buffer.from(`S ${position.fd} ${position.device} ${position.inode} ${position.flags} ${position.before} ${position.after} ${position.content?.length ?? -1} ${position.afterFlags} ${Buffer.byteLength(position.path ?? "")} ${position.eof === undefined ? -1 : Number(position.eof)} ${position.capacity ?? 0} ${position.socket?.shutdown ?? 0} ${position.socket?.peerShutdown ?? 0} ${position.socket?.peerInode ?? 0} ${position.socket?.peerQueued ?? 0} ${position.socket?.allocated ?? 0} ${position.event ?? 0} ${position.messages?.length ?? 0} ${position.socket ? position.socket.type ?? 1 : 0}\n${position.messages?.map(message => `M ${position.fd} ${message.start} ${message.end} ${message.rights.length} ${message.rights.join(" ")}\n`).join("") ?? ""}`));
				if (position.path) await write(socket, Buffer.from(position.path));
				if (position.content) await write(socket, position.content);
			}
			for (const event of resourceEvents) {
				await write(socket, Buffer.from(`Q ${RESOURCE_TRANSITIONS.indexOf(event.kind)} ${event.fd} ${event.data.length} ${event.requested ?? event.data.length}\n`));
				await write(socket, event.data);
			}
			for (const event of decision.output) {
				await write(socket, Buffer.from(`O ${event.fd} ${event.data.length}\n`));
				await write(socket, event.data);
			}
			if (continuation) {
				await write(socket, physicalRoot); await write(socket, sourceRoot); await write(socket, continuation.image);
			}
			const acknowledgement = await readLine(socket);
			if (acknowledgement === "N") { prepared = false; return void socket.end(); }
			if (acknowledgement !== "A") throw new Error("held-exec adoption was not acknowledged");
			throwIfAborted(active.signal);
			await decision.commit();
			await write(socket, Buffer.from("R\n"));
			if ((await readLine(socket)) !== "D") throw new Error("held-exec adoption completion is unknown");
			try { decision.adopted?.(); } catch { /* Feedback cannot poison a completed handoff. */ }
			socket.end();
		} catch (error) {
			if (active && (prepared || isPoisonedEffectCommit(error))) {
				// The logical call, not just the held child, owns an irreversible handoff.
				active.failure ??= effectCommitFailure(new Error("held-exec handoff failed", { cause: error }), "poisoned");
			}
			if (!socket.destroyed) socket.end(active?.failure ? "F\n" : "C\n");
		} finally {
			active?.pending.delete(pending);
			release();
		}
	}
}

async function observeCompletion(socket: net.Socket, observe: (durationMs: number | undefined) => void | Promise<void>): Promise<void> {
	const startedAt = performance.now();
	let durationMs: number | undefined;
	try {
		await write(socket, Buffer.from("O\n"));
		if (await readLine(socket) === "D") durationMs = Math.max(0, performance.now() - startedAt);
	} finally {
		try {
			await observe(durationMs);
		} catch {
			// Drain optional observation without replacing the already-authorized process outcome.
		}
		socket.end();
	}
}

/** Resolve the native helper shared by transparent dispatch and x86-64 Actor handoff. */
export async function resolveLinuxExecHelper(binary?: string): Promise<string> {
	if (process.platform !== "linux") throw new Error("Linux required");
	const resolved = await realpath(binary ?? path.join(os.homedir(), ".local", "bin", "pi-speculative-held-exec"));
	await access(resolved, fsConstants.X_OK);
	if ((await execute(resolved, ["--protocol-version"])).stdout.trim() !== String(HELPER_PROTOCOL_VERSION)) {
		throw new Error("held-exec protocol mismatch; rerun npm run setup:linux");
	}
	return resolved;
}

/** Inspect an image while PTRACE_EVENT_EXEC guarantees it has not run a user instruction. */
export async function inspectHeldExecProcess(pid: number, executable: string, descriptors?: readonly HeldFileDescriptor[]): Promise<HeldExecSnapshot> {
	const root = `/proc/${pid}`;
	const [cwd, command, environmentBytes, descriptorNames] = await Promise.all([
		readlink(`${root}/cwd`),
		readFile(`${root}/cmdline`),
		readFile(`${root}/environ`),
		readdir(`${root}/fd`),
	]);
	if (!validDescriptors(descriptors)) throw new Error("invalid native descriptor context");
	const context = await captureProcessContext(pid, descriptorNames, descriptors?.filter(descriptor => descriptor.pin === undefined));
	if (!validProcessContext(context)) throw new Error("held process descriptors are not replayable");
	const argv = decodeNullFields(command);
	if (!argv.length) throw new Error("held process argv is empty");
	const environment: Record<string, string> = {};
	for (const entry of decodeNullFields(environmentBytes)) {
		const separator = entry.indexOf("=");
		if (separator < 1 || Object.hasOwn(environment, entry.slice(0, separator))) throw new Error("held process environment is not canonical");
		environment[entry.slice(0, separator)] = entry.slice(separator + 1);
	}
	return {
		executable,
		outputRoute: context.outputEndpoints[0] === context.outputEndpoints[1] ? [1, 1] : [1, 2],
		outputPipes: [context.descriptorTypes[1] === "pipe", context.descriptorTypes[2] === "pipe"],
		argv, cwd, environment, context,
	};
}

/** Capture one image per inode; directory anchors and null devices need no payload. Only a native lease authorizes adoption. */
export async function captureHeldDescriptorInputs(pid: number, descriptors: readonly HeldFileDescriptor[], maxBytes: number, deniedPaths: readonly string[] = [],
	inputs?: (path: string) => Iterable<object>, tracerPid?: number, namespaceRoot?: string): Promise<ProcessResourceGraph> {
	const handles: Array<ProcessResourceGraph["handles"][number]> = [];
	const descriptions: Record<number, ProcessResourceGraph["descriptions"][number]> = {};
	const objects: Record<number, ProcessResourceGraph["objects"][number]> = {};
	const images = new Map<string, number>();
	let remaining = maxBytes;
	let namespace: ReturnType<typeof captureWorkspaceStructure> | undefined;
	for (const descriptor of descriptors) {
		const { fd, alias: representative, flags, offset } = descriptor;
		if (descriptor.pin !== undefined && (!Number.isSafeInteger(tracerPid) || tracerPid! <= 0)) throw new Error("queued OFD has no authenticated holder");
		const holder = descriptor.pin === undefined ? pid : tracerPid!, handle = descriptor.pin ?? fd;
		const heldPath = `/proc/${holder}/fd/${handle}`;
		if ((descriptor.flags & 3) === 3 || descriptor.fd === 1 || descriptor.fd === 2) throw new Error("unsupported inherited descriptor effects");
		const file = `${descriptor.device}:${descriptor.inode}${descriptor.counter ? `:${descriptor.counter.id}` : ""}`, image = images.get(file);
		const object = image ?? fd;
		if (descriptor.pin === undefined) handles.push({ fd, description: representative });
		if (fd === representative) descriptions[representative] = { object, flags, ...(!descriptor.type || descriptor.type === "directory" ? { position: offset } : {}),
			...(!descriptor.type && descriptor.outside !== undefined ? { outside: descriptor.outside } : {}),
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
				remaining -= bytes.length; if (remaining < 0) throw new Error("inherited counter exceeds input budget");
				objects[fd] = { type: "eventfd", counter: { value: descriptor.counter.value, semaphore: descriptor.counter.semaphore },
					contentDigest: sha256Digest(bytes), content: bytes.toString("base64") };
				images.set(file, fd); continue;
			}
			if (descriptor.type === "pipe" || descriptor.type === "socket") {
				if (!/^(pipe|socket):\[\d+\]$/.test(endpoint) || descriptor.queueHex === undefined) throw new Error("unproven inherited stream");
				const bytes = Buffer.from(descriptor.queueHex, "hex"); remaining -= bytes.length;
				if (remaining < 0) throw new Error("inherited stream exceeds input budget");
				objects[fd] = { type: descriptor.type, contentDigest: sha256Digest(bytes), content: bytes.toString("base64"),
					queue: { eof: descriptor.eof!, bytes: bytes.length, capacity: descriptor.capacity!, producer: descriptor.eof ? "closed" : "live", outside: descriptor.outside ?? 3,
						...(descriptor.messages ? { messages: descriptor.messages } : {}) },
					...(descriptor.socket ? { socket: { ...(descriptor.socket.type ? { type: descriptor.socket.type } : {}), shutdown: descriptor.socket.shutdown, allocated: descriptor.socket.allocated,
						peer: { connected: !!descriptor.socket.peerInode, shutdown: descriptor.socket.peerShutdown, bytes: descriptor.socket.peerQueued } } } : {}) };
				images.set(file, fd); continue;
			}
			if (descriptor.type === "directory") {
				const metadata = await stat(endpoint, { bigint: true });
				if (!metadata.isDirectory() || String(metadata.dev) !== descriptor.device || String(metadata.ino) !== descriptor.inode) throw new Error("held directory pathname changed");
				const directoryHex = descriptors.find(other => other.device === descriptor.device && other.inode === descriptor.inode && other.directoryHex !== undefined)?.directoryHex;
				if (directoryHex !== undefined) {
					const bytes = Buffer.from(directoryHex, "hex"); remaining -= bytes.length;
					if (remaining < 0) throw new Error("inherited directory exceeds input budget");
					objects[fd] = { type: "directory", sourcePath: endpoint, contentDigest: sha256Digest(bytes), content: bytes.toString("base64") };
					if (inputs) {
						const borrowed = await borrowResourceObject(inputs(endpoint), endpoint, metadata, bytes.length);
						if (!borrowed?.content?.equals(bytes)) {
							const captured = await captureHeldDirectory(holder, handle, bytes, metadata, endpoint).catch(() => undefined);
							if (captured && !retainResourceObject(inputs(endpoint), endpoint, captured)) await captured.object?.dispose();
						}
					}
					images.set(file, fd); continue;
				}
			}
			objects[fd] = { type: descriptor.type, contentDigest: sha256Digest(""), ...(descriptor.type === "directory" ? { sourcePath: endpoint } : {}) };
			images.set(file, fd);
			continue;
		}
		let captured = inputs && await borrowResourceObject(inputs(endpoint), endpoint, await stat(heldPath, { bigint: true }), remaining);
		if (captured && !sameFilesystemIdentity(captured.stat, await stat(heldPath, { bigint: true }))) captured = undefined;
		const borrowed = !!captured;
		captured ??= await captureHeldFile(holder, handle, remaining, !!inputs);
		let retained = borrowed;
		try {
			if (String(captured.stat.dev) !== descriptor.device || String(captured.stat.ino) !== descriptor.inode || !captured.content) {
				throw new Error("held descriptor identity changed");
			}
			const sourcePath = captured.stat.nlink ? endpoint : undefined;
			let sourceAliases: readonly string[] | undefined;
			if (captured.stat.nlink > 1n) {
				if (!namespaceRoot || !containsFilesystemPath(namespaceRoot, endpoint)) throw new Error("inherited descriptor alias namespace is unavailable");
				const snapshot = await (namespace ??= captureWorkspaceStructure(namespaceRoot));
				const entry = snapshot.entries.get(path.relative(namespaceRoot, endpoint));
				if (!snapshot.complete || entry?.kind !== "file" || entry.object !== `${captured.stat.dev}:${captured.stat.ino}` ||
					entry.aliases?.length !== Number(captured.stat.nlink)) throw new Error("inherited descriptor alias namespace is not closed");
				sourceAliases = entry.aliases;
			}
			if (sourcePath) {
				const metadata = await stat(sourcePath, { bigint: true });
				if (metadata.dev !== captured.stat.dev || metadata.ino !== captured.stat.ino) throw new Error("held descriptor pathname changed");
			}
			remaining -= captured.content.byteLength;
			objects[fd] = { type: "regular", ...(sourcePath ? { sourcePath } : {}), ...(sourceAliases ? { sourceAliases } : {}),
				contentDigest: `sha256:${captured.hash}`, content: captured.content.toString("base64") };
			images.set(file, fd);
			if (!borrowed && sourcePath && inputs) retained = retainResourceObject(inputs(sourcePath), sourcePath, { ...captured, realPath: sourcePath });
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
	const fields = bytes.toString("utf8").split("\0");
	if (fields.at(-1) === "") fields.pop();
	if (!Buffer.from(`${fields.join("\0")}${bytes.at(-1) === 0 ? "\0" : ""}`, "utf8").equals(bytes)) {
		throw new Error("held process metadata is not valid UTF-8");
	}
	return fields;
}

function parseRequest(line: string): WireRequest | undefined {
	try {
		const value = JSON.parse(line) as Partial<WireRequest>;
		return value.version === WIRE_PROTOCOL_VERSION && /^[0-9a-f]{64}$/.test(value.token ?? "") &&
			/^[0-9a-f]{48}$/.test(value.execution ?? "") && Number.isSafeInteger(value.pid) && value.pid! > 0 &&
			Number.isSafeInteger(value.tracer) && value.tracer! > 0 && (value.trackQueues === undefined || value.trackQueues === true && value.descriptors === undefined) &&
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
		if (descriptor?.pin !== undefined && (!Number.isSafeInteger(descriptor.pin) || descriptor.pin < 0 || descriptor.pin > 0x7fffffff || descriptor.alias !== descriptor.fd)) return false;
		if (!descriptor || descriptor.type !== undefined && !["null", "directory", "pipe", "socket", "eventfd"].includes(descriptor.type) || descriptor.type !== undefined && descriptor.type !== "directory" && descriptor.offset !== 0 ||
			!validOFDLocks(descriptor.locks) || descriptor.locks !== undefined && (descriptor.type !== undefined || descriptor.fd !== descriptor.alias) ||
			(descriptor.type === "directory" && !(descriptor.flags & 0x200000) ? typeof descriptor.directoryHex !== "string" || descriptor.directoryHex.length > 4 * 1024 * 1024 || !/^(?:[0-9a-f]{2})*$/.test(descriptor.directoryHex) : descriptor.directoryHex !== undefined) ||
			(descriptor.type === "eventfd" ? !descriptor.counter || !Number.isInteger(descriptor.counter.id) || descriptor.counter.id < 0 || descriptor.counter.id > 0x7fffffff ||
				!/^(0|[1-9][0-9]{0,19})$/.test(descriptor.counter.value) || BigInt(descriptor.counter.value) >= 0xffffffffffffffffn || ![0, 1].includes(descriptor.counter.semaphore) : descriptor.counter !== undefined) ||
			(descriptor.outside !== undefined && (!Number.isInteger(descriptor.outside) || descriptor.outside < 0 || descriptor.outside > 3 ||
				(descriptor.type !== undefined ? !["pipe", "socket"].includes(descriptor.type) : descriptor.outside !== 0 && descriptor.outside !== 3))) ||
			(descriptor.messages !== undefined && (descriptor.type !== "socket" || !validQueueMessages(descriptor.messages, (descriptor.queueHex?.length ?? 0) / 2))) ||
			(descriptor.type === "pipe" || descriptor.type === "socket" ? typeof descriptor.eof !== "boolean" || typeof descriptor.queueHex !== "string" || descriptor.queueHex.length > 4 * 1024 * 1024 || descriptor.queueHex.length % 2 !== 0 || !/^[0-9a-f]*$/.test(descriptor.queueHex) || !Number.isSafeInteger(descriptor.capacity) || descriptor.capacity! < 4096 : descriptor.queueHex !== undefined || descriptor.eof !== undefined || descriptor.capacity !== undefined) ||
			(descriptor.type === "socket" ? !descriptor.socket || ![1, 2, 5].includes(descriptor.socket.type ?? 1) || ![descriptor.socket.shutdown, descriptor.socket.peerShutdown].every(value => Number.isInteger(value) && value >= 0 && value <= 3) || ![descriptor.socket.peerInode, descriptor.socket.peerQueued, descriptor.socket.allocated].every(value => Number.isSafeInteger(value) && value >= 0) : descriptor.socket !== undefined) ||
			!isOFDPosition(descriptor.offset) || ![descriptor.fd, descriptor.alias, descriptor.flags].every(value => Number.isSafeInteger(value) && value >= 0) ||
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

async function heldBy(pid: number, tracer: number, binary: string): Promise<boolean> {
	try {
		const [status, executable] = await Promise.all([
			readFile(`/proc/${pid}/status`, "utf8"),
			realpath(`/proc/${tracer}/exe`),
		]);
		return executable === binary && new RegExp(`^TracerPid:\\s*${tracer}$`, "m").test(status) && /^State:\s+t\b/m.test(status);
	} catch {
		return false;
	}
}

async function readLine(socket: net.Socket): Promise<string> {
	socket.pause();
	return new Promise((resolve, reject) => {
		let body = "";
		const cleanup = () => {
			socket.pause();
			socket.off("data", onData);
			socket.off("end", onEnd);
			socket.off("close", onEnd);
			socket.off("error", onError);
		};
		const finish = (error?: unknown, value?: string) => {
			cleanup();
			error ? reject(error) : resolve(value ?? "");
		};
		const onData = (chunk: Buffer) => {
			body += chunk.toString("utf8");
			const newline = body.indexOf("\n");
			if (newline >= 0) finish(undefined, body.slice(0, newline));
			else if (Buffer.byteLength(body) > MAX_REQUEST_BYTES) finish(new Error("held-exec message too large"));
		};
		const onEnd = () => finish(new Error("held-exec peer closed"));
		const onError = (error: Error) => finish(error);
		socket.on("data", onData);
		socket.once("end", onEnd);
		socket.once("close", onEnd);
		socket.once("error", onError);
		socket.resume();
	});
}

async function write(socket: net.Socket, data: Buffer): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		socket.write(data, (error) => error ? reject(error) : resolve());
	});
}

export function listenUnixSocket(server: net.Server, socketPath: string): Promise<void> {
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(socketPath, () => {
			server.off("error", reject);
			resolve();
		});
	});
}

function execute(command: string, args: readonly string[]): Promise<{ stdout: string; code: number | null; signal: NodeJS.Signals | null }> {
	return new Promise((resolve, reject) => {
		execFile(command, args, { encoding: "utf8" }, (error, stdout) => {
			if (error && typeof error.code !== "number") return void reject(error);
			resolve({ stdout, code: error && typeof error.code === "number" ? error.code : 0, signal: error?.signal ?? null });
		});
	});
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (signal?.aborted) throw signal.reason ?? new Error("aborted");
}
