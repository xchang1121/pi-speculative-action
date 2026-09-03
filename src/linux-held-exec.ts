import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, mkdir, readFile, readdir, readlink, realpath, rm, stat } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type { ProcessExecutor } from "./process-execution.ts";

const PROTOCOL_VERSION = 1;
const MAX_REQUEST_BYTES = 2048;
const MAX_OUTPUT_EVENTS = 65_536;
const MAX_OUTPUT_BYTES = 512 * 1024 * 1024;
const PRIVATE_ENV = {
	shell: "PI_SPEC_HELD_EXEC_SHELL",
	socket: "PI_SPEC_HELD_EXEC_SOCKET",
	token: "PI_SPEC_HELD_EXEC_TOKEN",
	execution: "PI_SPEC_HELD_EXEC_ID",
} as const;

export interface HeldExecProcess {
	readonly pid: number;
	readonly tracerPid: number;
	readonly sourceRoot: string;
	readonly signal?: AbortSignal;
}

export interface HeldExecSnapshot {
	readonly executable: string;
	readonly argv: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: {
		readonly key: string;
		readonly umask: number;
		readonly descriptorTypes: readonly ["device" | "other", "pipe" | "socket", "pipe" | "socket"];
	};
}

export type HeldExecDecision =
	| { readonly kind: "continue" }
	| {
			readonly kind: "replay";
			readonly exitCode: number;
			readonly output: readonly { readonly fd: 1 | 2; readonly data: Buffer }[];
			/** Called only after the native tracer has made original execution impossible. */
			readonly commit: () => Promise<void>;
	  };

export interface LinuxHeldExecOptions {
	readonly storeRoot: string;
	readonly binary?: string;
	readonly decide: (process: HeldExecProcess) => Promise<HeldExecDecision>;
}

interface ActiveExecution {
	readonly sourceRoot: string;
	readonly signal?: AbortSignal;
}

interface WireRequest {
	readonly version: 1;
	readonly token: string;
	readonly execution: string;
	readonly pid: number;
	readonly tracer: number;
}

/** A two-phase held-exec transport: arm the exit stub before committing reusable effects. */
export class LinuxHeldExecBoundary {
	readonly shellPath: string;
	private readonly token = randomBytes(32).toString("hex");
	private readonly active = new Map<string, ActiveExecution>();
	private readonly server: net.Server;
	private readonly sockets = new Set<net.Socket>();
	private readonly socketPath: string;
	private readonly decide: LinuxHeldExecOptions["decide"];
	private closed = false;

	private constructor(binary: string, socketPath: string, decide: LinuxHeldExecOptions["decide"]) {
		this.shellPath = binary;
		this.socketPath = socketPath;
		this.decide = decide;
		this.server = net.createServer({ allowHalfOpen: true }, (socket) => {
			this.sockets.add(socket);
			socket.once("close", () => this.sockets.delete(socket));
			void this.serve(socket);
		});
	}

	static async open(options: LinuxHeldExecOptions): Promise<LinuxHeldExecBoundary> {
		if (process.platform !== "linux" || process.arch !== "x64") throw new Error("x86-64 Linux required");
		const binary = await realpath(options.binary ?? path.join(os.homedir(), ".local", "bin", "pi-speculative-held-exec"));
		await access(binary, fsConstants.X_OK);
		if ((await execute(binary, ["--protocol-version"])).stdout.trim() !== String(PROTOCOL_VERSION)) {
			throw new Error("held-exec protocol mismatch; rerun npm run setup:linux");
		}
		const probe = await execute(binary, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"]);
		if (probe.code !== 42 || probe.signal) throw new Error("held-exec functional probe failed");
		await mkdir(options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(options.storeRoot, 0o700);
		const candidate = path.join(options.storeRoot, `held-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const socketPath = Buffer.byteLength(candidate) < 104
			? candidate
			: path.join(os.tmpdir(), `pi-held-${process.getuid?.() ?? 0}-${process.pid}-${randomBytes(6).toString("hex")}.sock`);
		const boundary = new LinuxHeldExecBoundary(binary, socketPath, options.decide);
		await listen(boundary.server, socketPath);
		await chmod(socketPath, 0o600);
		return boundary;
	}

	executor(host: ProcessExecutor, options: { readonly realShell: string; readonly sourceRoot: string; readonly enabled: () => boolean }): ProcessExecutor {
		return {
			execute: async (request) => {
				const execution = randomBytes(24).toString("hex");
				const enabled = options.enabled() && !request.signal?.aborted;
				if (enabled) this.active.set(execution, { sourceRoot: options.sourceRoot, ...(request.signal ? { signal: request.signal } : {}) });
				try {
					return await host.execute({
						...request,
						environment: {
							...request.environment,
							[PRIVATE_ENV.shell]: options.realShell,
							...(enabled ? {
								[PRIVATE_ENV.socket]: this.socketPath,
								[PRIVATE_ENV.token]: this.token,
								[PRIVATE_ENV.execution]: execution,
							} : {}),
						},
					});
				} finally {
					this.active.delete(execution);
				}
			},
		};
	}

	async close(): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		this.active.clear();
		for (const socket of this.sockets) socket.destroy();
		await new Promise<void>((resolve) => this.server.close(() => resolve()));
		await rm(this.socketPath, { force: true }).catch(() => undefined);
	}

	private async serve(socket: net.Socket): Promise<void> {
		let prepared = false;
		try {
			const request = parseRequest(await readLine(socket));
			const active = request?.token === this.token ? this.active.get(request.execution) : undefined;
			if (!request || !active || !(await heldBy(request.pid, request.tracer, this.shellPath))) {
				return void socket.end("C\n");
			}
			throwIfAborted(active.signal);
			const decision = await this.decide({
				pid: request.pid,
				tracerPid: request.tracer,
				sourceRoot: active.sourceRoot,
				...(active.signal ? { signal: active.signal } : {}),
			});
			if (decision.kind === "continue") return void socket.end("C\n");
			const total = decision.output.reduce((sum, event) => sum + event.data.length, 0);
			if (!Number.isSafeInteger(decision.exitCode) || decision.exitCode < 0 || decision.exitCode > 255 ||
				decision.output.length > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES) return void socket.end("C\n");
			await write(socket, Buffer.from(`P ${decision.exitCode} ${decision.output.length} ${total}\n`));
			for (const event of decision.output) {
				await write(socket, Buffer.from(`O ${event.fd} ${event.data.length}\n`));
				await write(socket, event.data);
			}
			if ((await readLine(socket)) !== "A") return void socket.end();
			prepared = true;
			throwIfAborted(active.signal);
			await decision.commit();
			socket.end("R\n");
		} catch {
			if (!socket.destroyed) socket.end(prepared ? "F\n" : "C\n");
		}
	}
}

/** Inspect an image while PTRACE_EVENT_EXEC guarantees it has not run a user instruction. */
export async function inspectHeldExecProcess(pid: number): Promise<HeldExecSnapshot> {
	const root = `/proc/${pid}`;
	const [executable, cwd, command, environmentBytes, status, limits, processStat, descriptorNames] = await Promise.all([
		realpath(`${root}/exe`),
		readlink(`${root}/cwd`),
		readFile(`${root}/cmdline`),
		readFile(`${root}/environ`),
		readFile(`${root}/status`, "utf8"),
		readFile(`${root}/limits`, "utf8"),
		readFile(`${root}/stat`, "utf8"),
		readdir(`${root}/fd`),
	]);
	if (descriptorNames.some((name) => !/^\d+$/.test(name)) || descriptorNames.map(Number).sort((a, b) => a - b).join(",") !== "0,1,2") {
		throw new Error("held process has unmodeled inherited descriptors");
	}
	const argv = decodeNullFields(command);
	if (!argv.length) throw new Error("held process argv is empty");
	const environment: Record<string, string> = {};
	for (const entry of decodeNullFields(environmentBytes)) {
		const separator = entry.indexOf("=");
		if (separator < 1 || Object.hasOwn(environment, entry.slice(0, separator))) throw new Error("held process environment is not canonical");
		environment[entry.slice(0, separator)] = entry.slice(separator + 1);
	}
	const aliases = new Map<string, number>();
	const descriptors: Array<{ fd: number; type: "regular" | "pipe" | "socket" | "tty" | "device" | "other"; flags: number; alias: number; endpoint: string }> = [];
	for (const fd of [0, 1, 2]) {
		const [metadata, endpoint, info] = await Promise.all([
			stat(`${root}/fd/${fd}`),
			readlink(`${root}/fd/${fd}`),
			readFile(`${root}/fdinfo/${fd}`, "utf8"),
		]);
		const identity = `${metadata.dev}:${metadata.ino}`;
		if (!aliases.has(identity)) aliases.set(identity, aliases.size);
		const encodedFlags = /^flags:\s*([0-7]+)/m.exec(info)?.[1];
		if (!encodedFlags) throw new Error(`held descriptor ${fd} flags unavailable`);
		descriptors.push({
			fd,
			type: metadata.isFile() ? "regular" : metadata.isFIFO() ? "pipe" : metadata.isSocket() ? "socket" :
				metadata.isCharacterDevice() ? (endpoint.startsWith("/dev/pts/") ? "tty" : "device") : "other",
			flags: Number.parseInt(encodedFlags, 8),
			alias: aliases.get(identity)!,
			endpoint,
		});
	}
	if (descriptors[0]!.type !== "device" || !["pipe", "socket"].includes(descriptors[1]!.type) ||
		!["pipe", "socket"].includes(descriptors[2]!.type)) throw new Error("held process descriptors are not replayable");
	const uid = numbers(statusField(status, "Uid"));
	const gid = numbers(statusField(status, "Gid"));
	if (uid.length !== 4 || gid.length !== 4) throw new Error("held process credentials are incomplete");
	const groups = numbers(statusField(status, "Groups"));
	if (!groups.includes(gid[1]!)) groups.push(gid[1]!);
	groups.sort((left, right) => left - right);
	const semantic = {
		executionDomain: "ptrace-v1",
		rlimits: limits.split("\n").slice(1).map((line) => line.trim().split(/\s{2,}/).slice(0, 2)),
		credentials: { uid: uid[0], euid: uid[1], gid: gid[0], egid: gid[1], groups },
		signals: { blocked: statusField(status, "SigBlk"), ignored: statusField(status, "SigIgn") },
		scheduling: {
			nice: Number(processStat.slice(processStat.lastIndexOf(") ") + 2).trim().split(/\s+/)[16]),
			cpus: statusField(status, "Cpus_allowed_list"),
			memoryNodes: statusField(status, "Mems_allowed_list"),
		},
		descriptors: descriptors.map(({ endpoint, ...value }) => ({
			...value,
			flags: value.flags & ~0o2000000,
			...(value.type === "device" ? { endpoint } : {}),
		})),
	};
	const descriptorTypes: HeldExecSnapshot["context"]["descriptorTypes"] = [
		"device",
		descriptors[1]!.type as "pipe" | "socket",
		descriptors[2]!.type as "pipe" | "socket",
	];
	return {
		executable,
		argv,
		cwd,
		environment,
		context: { key: JSON.stringify(semantic), umask: Number.parseInt(statusField(status, "Umask"), 8), descriptorTypes },
	};
}

function decodeNullFields(bytes: Buffer): string[] {
	const fields = bytes.toString("utf8").split("\0");
	if (fields.at(-1) === "") fields.pop();
	if (!Buffer.from(`${fields.join("\0")}${bytes.at(-1) === 0 ? "\0" : ""}`, "utf8").equals(bytes)) {
		throw new Error("held process metadata is not valid UTF-8");
	}
	return fields;
}

function statusField(status: string, name: string): string {
	const match = new RegExp(`^${name}:\\s*(.*)$`, "m").exec(status);
	if (!match) throw new Error(`held process status lacks ${name}`);
	const value = match[1]!.trim();
	return value;
}

function numbers(value: string): number[] {
	const result = value ? value.split(/\s+/).map(Number) : [];
	if (result.some((item) => !Number.isSafeInteger(item) || item < 0)) throw new Error("invalid held process status numbers");
	return result;
}

function parseRequest(line: string): WireRequest | undefined {
	try {
		const value = JSON.parse(line) as Partial<WireRequest>;
		return value.version === PROTOCOL_VERSION && /^[0-9a-f]{64}$/.test(value.token ?? "") &&
			/^[0-9a-f]{48}$/.test(value.execution ?? "") && Number.isSafeInteger(value.pid) && value.pid! > 0 &&
			Number.isSafeInteger(value.tracer) && value.tracer! > 0 ? value as WireRequest : undefined;
	} catch {
		return undefined;
	}
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
		socket.once("error", onError);
		socket.resume();
	});
}

async function write(socket: net.Socket, data: Buffer): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		socket.write(data, (error) => error ? reject(error) : resolve());
	});
}

function listen(server: net.Server, socketPath: string): Promise<void> {
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
