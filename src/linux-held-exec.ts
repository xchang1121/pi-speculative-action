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
import { captureHeldFile } from "./filesystem-evidence.ts";
import { sha256Digest, type Sha256Digest } from "./provenance-certificate.ts";
import { containsFilesystemPath } from "./path-utils.ts";
import { snapshotExecutionScope, type ExecutionScope } from "./execution-world.ts";

const HELPER_PROTOCOL_VERSION = 16;
const WIRE_PROTOCOL_VERSION = 1;
const MAX_REQUEST_BYTES = 32768;
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
}

export interface HeldFileDescriptor {
	readonly type?: "null" | "directory";
	readonly fd: number;
	readonly alias: number;
	readonly device: string;
	readonly inode: string;
	readonly flags: number;
	readonly offset: number;
	readonly owned: boolean;
}

export interface FileDescriptorInput extends Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type"> {
	readonly contentDigest: Sha256Digest;
	/** One image per inode; distinct OFDs open it independently. */
	readonly image: number;
	readonly sourcePath?: string;
	readonly content?: string;
}

export interface HeldExecSnapshot {
	readonly executable: string;
	/** Actual output aliasing, usable for another isolated launch after context revalidation. */
	readonly outputRoute?: readonly [1 | 2, 1 | 2];
	readonly argv: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: Pick<ProcessExecutionContext, "key" | "umask" | "descriptorTypes" | "regularDescriptors">;
	readonly descriptorInputs?: readonly FileDescriptorInput[];
}

export type HeldExecDecision =
	| { readonly kind: "continue"; readonly observeCompletion?: (durationMs: number | undefined) => void | Promise<void> }
	| {
			readonly kind: "replay";
			readonly exitCode: number;
			readonly output: readonly { readonly fd: 1 | 2; readonly data: Buffer }[];
			/** Applied after commit, before output. The caller owns predecessor proof and serialization of every OFD sharer. */
			readonly descriptorOffsets?: readonly {
				readonly fd: number; readonly device: string; readonly inode: string;
				readonly flags: number; readonly before: number; readonly after: number; readonly afterFlags?: number; readonly path?: string;
				/** Replace inode contents through a separate writer; never disturb OFD flags or position. */
				readonly content?: Buffer;
			}[];
			/** Called only after the native tracer has made original execution impossible. */
			readonly commit: () => Promise<void>;
			readonly adopted?: () => void;
	  };

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
			});
			if (decision.kind === "continue") {
				if (!decision.observeCompletion) return void socket.end("C\n");
				await observeCompletion(socket, decision.observeCompletion);
				return;
			}
			const positions = decision.descriptorOffsets?.map(position => ({ ...position, afterFlags: position.afterFlags ?? position.flags })) ?? [];
			const total = decision.output.reduce((sum, event) => sum + event.data.length, 0) + positions.reduce((sum, position) => sum + (position.content?.length ?? 0) + Buffer.byteLength(position.path ?? ""), 0);
			const descriptors = new Set<number>();
			if (!Number.isSafeInteger(decision.exitCode) || decision.exitCode < 0 || decision.exitCode > 255 ||
				decision.output.length > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || positions.length > 64 || positions.some(position => {
					const duplicate = descriptors.has(position.fd); descriptors.add(position.fd);
					return duplicate || position.path !== undefined && (typeof position.path !== "string" || !path.isAbsolute(position.path) || position.path.includes("\0") || Buffer.byteLength(position.path) >= 4096) ||
						position.content !== undefined && !Buffer.isBuffer(position.content) ||
						![position.fd, position.flags, position.before, position.after, position.afterFlags].every(value => Number.isSafeInteger(value) && value >= 0) ||
						position.fd > 0x7fffffff || position.flags > 0x7fffffff || position.afterFlags > 0x7fffffff || ![position.device, position.inode].every(value =>
							typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 0xffffffffffffffffn);
				})) return void socket.end("C\n");
			// Once a proposal is delivered the peer may arm its exit stub, even if its ACK is lost.
			prepared = true;
			await write(socket, Buffer.from(`P ${decision.exitCode} ${decision.output.length} ${total} ${positions.length}\n`));
			for (const position of positions) {
				await write(socket, Buffer.from(`S ${position.fd} ${position.device} ${position.inode} ${position.flags} ${position.before} ${position.after} ${position.content?.length ?? -1} ${position.afterFlags} ${Buffer.byteLength(position.path ?? "")}\n`));
				if (position.path) await write(socket, Buffer.from(position.path));
				if (position.content) await write(socket, position.content);
			}
			for (const event of decision.output) {
				await write(socket, Buffer.from(`O ${event.fd} ${event.data.length}\n`));
				await write(socket, event.data);
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
	const context = await captureProcessContext(pid, descriptorNames, descriptors);
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
		argv, cwd, environment, context,
	};
}

/** Capture one image per inode; directory anchors and null devices need no payload. Only a native lease authorizes adoption. */
export async function captureHeldDescriptorInputs(pid: number, descriptors: readonly HeldFileDescriptor[], maxBytes: number, deniedPaths: readonly string[] = []): Promise<readonly FileDescriptorInput[]> {
	const inputs = new Map<number, FileDescriptorInput>();
	const images = new Map<string, FileDescriptorInput>();
	let remaining = maxBytes;
	for (const descriptor of descriptors) {
		const { fd, alias: representative, flags, offset } = descriptor;
		const identity = { fd, alias: representative, flags, offset, ...(descriptor.type ? { type: descriptor.type } : {}) };
		if ((descriptor.flags & 3) === 3 || descriptor.fd === 1 || descriptor.fd === 2) throw new Error("unsupported inherited descriptor effects");
		const file = `${descriptor.device}:${descriptor.inode}`, image = images.get(file);
		if (image && descriptor.type === "directory" && fd === representative && await readlink(`/proc/${pid}/fd/${fd}`) !== image.sourcePath)
			throw new Error("inherited directory namespace aliases are unproven");
		if (image) { inputs.set(descriptor.fd, { ...identity, image: image.image, contentDigest: image.contentDigest,
			...(image.sourcePath ? { sourcePath: image.sourcePath } : {}) }); continue; }
		const endpoint = await readlink(`/proc/${pid}/fd/${fd}`);
		if (deniedPaths.some(denied => containsFilesystemPath(denied, endpoint) || containsFilesystemPath(denied, endpoint.replace(/ \(deleted\)$/, ""))))
			throw new Error("inherited descriptor refers to a denied resource");
		if (descriptor.type) {
			if (descriptor.type === "directory") {
				const metadata = await stat(endpoint, { bigint: true });
				if (!metadata.isDirectory() || String(metadata.dev) !== descriptor.device || String(metadata.ino) !== descriptor.inode) throw new Error("held directory pathname changed");
			}
			const input = { ...identity, image: fd, contentDigest: sha256Digest(""), ...(descriptor.type === "directory" ? { sourcePath: endpoint } : {}) };
			inputs.set(fd, input);
			if (descriptor.type === "directory") images.set(file, input);
			continue;
		}
		const captured = await captureHeldFile(pid, descriptor.fd, remaining);
		if (String(captured.stat.dev) !== descriptor.device || String(captured.stat.ino) !== descriptor.inode || !captured.content) {
			throw new Error("held descriptor identity changed");
		}
		if (captured.stat.nlink > 1n) throw new Error("inherited descriptor namespace aliases are unproven");
		const sourcePath = captured.stat.nlink ? endpoint : undefined;
		if (sourcePath) {
			const metadata = await stat(sourcePath, { bigint: true });
			if (metadata.dev !== captured.stat.dev || metadata.ino !== captured.stat.ino) throw new Error("held descriptor pathname changed");
		}
		remaining -= captured.content.byteLength;
		const capturedInput: FileDescriptorInput = { ...identity, image: fd, ...(sourcePath ? { sourcePath } : {}),
			contentDigest: `sha256:${captured.hash}`, content: captured.content.toString("base64") };
		inputs.set(descriptor.fd, capturedInput); images.set(file, capturedInput);
	}
	return [...inputs.values()];
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
			Number.isSafeInteger(value.tracer) && value.tracer! > 0 && validDescriptors(value.descriptors) ? value as WireRequest : undefined;
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
		if (!descriptor || descriptor.type !== undefined && !["null", "directory"].includes(descriptor.type) || descriptor.type !== undefined && descriptor.offset !== 0 ||
			![descriptor.fd, descriptor.alias, descriptor.flags, descriptor.offset].every(value => Number.isSafeInteger(value) && value >= 0) ||
			descriptor.fd <= previous || descriptor.fd > 0x7fffffff || descriptor.alias > descriptor.fd || descriptor.flags > 0x7fffffff ||
			typeof descriptor.owned !== "boolean" || ![descriptor.device, descriptor.inode].every(value =>
				typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= 0xffffffffffffffffn)) return false;
		const alias = aliases.get(descriptor.alias);
		if (descriptor.alias !== descriptor.fd && (!alias || alias.device !== descriptor.device || alias.inode !== descriptor.inode ||
			alias.type !== descriptor.type || alias.flags !== descriptor.flags || alias.offset !== descriptor.offset || alias.owned !== descriptor.owned)) return false;
		if (descriptor.alias === descriptor.fd) aliases.set(descriptor.fd, descriptor);
		previous = descriptor.fd;
	}
	return true;
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
