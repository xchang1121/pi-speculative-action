import { AsyncResource } from "node:async_hooks";
import { isUtf8 } from "node:buffer";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { on, once } from "node:events";
import { closeSync, constants, lstatSync, openSync, readSync, realpathSync, statSync } from "node:fs";
import { chmod, mkdir, open, readFile, readdir, readlink, rm } from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { getSystemErrorName } from "node:util";
import { nativeBuildDefinitions, speculativeConfig } from "./config-schema.ts";
import {
	effectCommitFailure,
	isPoisonedEffectCommit,
	snapshotExecutionScope,
	type ExecutionScope,
} from "./execution-world.ts";
import { immutableSnapshot, isSafeCount, sha256Digest } from "./primitives.ts";
import {
	captureProcessContext,
	processLaunchOptions,
	validProcessContext,
	type ProcessExecutionContext,
	type ProcessLaunchOptions,
} from "./process-context.mjs";
import {
	nativeFilesystem,
	nativeProcessLibrary,
	qualifyNativeStartup,
	resolveLinuxExecHelper,
	startupScope,
} from "./process-startup.mjs";
import { type DynamicDependency, type OFDPosition } from "./provenance-certificate.ts";
import { PendingTasks } from "./runtime-lifecycle.ts";
export { resolveLinuxExecHelper } from "./process-startup.mjs";
/** Kernel collection shares the execution owner's lifetime, not its privileges. */
export async function executionDomainCollector(binary: string) {
	const capacities = speculativeConfig().nativeBuild;
	const changeBatch = Math.floor((capacities.domainValues - 1) / 3);
	const socket = process.env.PI_SPEC_DOMAIN_SOCKET;
	if (!socket)
		throw new Error(
			"execution_domain_missing: start Pi through pi-speculative-domain --user <uid>; cgroup v2, kernel BTF and eBPF hooks are required",
		);
	if (!nativeFilesystem) nativeProcessLibrary(binary);
	const request = (operation: number, domain = 0n, values: readonly (number | bigint)[] | Buffer = []) => {
		const count = Buffer.isBuffer(values) ? Math.ceil(values.length / 8) : values.length;
		if (count > capacities.domainValues) throw new Error("execution domain query exceeds its batch limit");
		const packet = Buffer.concat([
			Buffer.alloc(16 + capacities.domainValues * 8),
			Buffer.from(`${socket}\0`),
		]);
		packet.writeUInt32LE(operation);
		packet.writeUInt32LE(count, 4);
		packet.writeBigUInt64LE(domain, 8);
		if (Buffer.isBuffer(values)) values.copy(packet, 16);
		else
			for (const [index, value] of values.entries()) packet.writeBigUInt64LE(BigInt(value), 16 + index * 8);
		const status = nativeFilesystem?.(-15, packet);
		if (status !== 0) {
			const code = typeof status === "number" && status < 0 ? getSystemErrorName(status) : "EPROTO";
			const reason =
				operation === 5 &&
				(
					{
						EACCES: "root_permissions: root must be caller-owned and not group/other writable",
						E2BIG: `root_depth_limit: at most ${capacities.domainAncestors} levels and distinct parent directories`,
						ENOSPC: `root_count_limit: at most ${capacities.domainRoots} roots`,
						ENAMETOOLONG: "root_path_limit: path must fit PATH_MAX",
						EINVAL: "root_not_canonical: use the resolved absolute directory",
					} as Record<string, string>
				)[code];
			throw new Error(reason || `execution_domain_unavailable:${code}`);
		}
		return packet;
	};
	const fingerprint = sha256Digest(
		Buffer.concat([
			...(await Promise.all(
				["c", "h"].map((extension) =>
					readFile(new URL(`../src/linux-execution-domain.${extension}`, import.meta.url)),
				),
			)),
			Buffer.from(JSON.stringify(nativeBuildDefinitions())),
		]),
	);
	const check = (root?: string): void => {
		const hello = request(0);
		if (
			hello.readBigUInt64LE(8) !== 18n ||
			hello.readUInt32LE(4) !== 8 ||
			hello.toString("ascii", 16, 80) !== fingerprint.slice(7)
		)
			throw new Error("execution domain collector changed; rebuild the Linux backend");
		if (root !== undefined) {
			const domain = collector.open();
			try {
				const reason = domain.watch([root]);
				if (reason || domain.version() === undefined)
					throw new Error(reason || domain.failure || "observer_unavailable");
			} finally {
				domain.close();
			}
		}
	};
	const collector = {
		fingerprint,
		check,
		open: () => {
			const id = request(1).readBigUInt64LE(8);
			let phase: "open" | "watching" | "closed" = "open";
			let descriptor: number | undefined;
			let failure: string | undefined;
			return {
				get closed() {
					return phase === "closed";
				},
				get failure() {
					return failure;
				},
				trace: (broker = "") => {
					if (phase === "closed" || descriptor !== undefined)
						throw new Error("execution record owner is unavailable");
					descriptor = Number(request(11, id, Buffer.from(`${broker}\0`)).readBigUInt64LE(16));
					let offset = 0;
					return () => {
						if (phase === "closed" || descriptor === undefined)
							throw new Error("execution record owner is closed");
						const fence = request(12, id);
						const length = Number(fence.readBigUInt64LE(16));
						if (
							fence.readUInt32LE(4) !== 4 ||
							fence.readBigUInt64LE(32) ||
							fence.readBigUInt64LE(24) > fence.readBigUInt64LE(40) ||
							!isSafeCount(length, offset, capacities.domainTraceBytes)
						)
							throw new Error("execution records are incomplete");
						const bytes = Buffer.allocUnsafe(length - offset);
						if (readSync(descriptor, bytes, 0, bytes.length, offset) !== bytes.length)
							throw new Error("execution records changed while reading");
						offset = length;
						return bytes;
					};
				},
				launch: (command: readonly [string, ...string[]]): readonly [string, ...string[]] => [
					binary,
					"--execution-domain",
					String(id),
					socket,
					...command,
				],
				watch: (roots: readonly string[]) => {
					if (phase === "closed") return "execution record owner is closed";
					try {
						request(5, id, Buffer.from(`${roots.join("\0")}\0`));
						phase = "watching";
					} catch (error) {
						phase = "open";
						return (error as Error).message;
					}
				},
				admit: (
					objects: readonly {
						readonly dev: bigint;
						readonly ino: bigint;
						readonly path: string;
					}[],
				) => {
					if (phase !== "watching") return false;
					try {
						for (let index = 0; index < objects.length; index += changeBatch) {
							const descriptors: number[] = [];
							const values: (number | bigint)[] = [];
							try {
								for (const info of objects.slice(index, index + changeBatch)) {
									const descriptor = openSync(info.path, 0x200000 | constants.O_NOFOLLOW);
									descriptors.push(descriptor);
									values.push(descriptor, info.dev, info.ino);
								}
								request(13, id, values);
							} finally {
								for (const descriptor of descriptors) closeSync(descriptor);
							}
						}
						return true;
					} catch {
						return false;
					}
				},
				version: () => {
					if (phase !== "watching") return;
					const answer = request(6, id);
					const incomplete = answer.readBigUInt64LE(24) | answer.readBigUInt64LE(32);
					failure = incomplete ? `observer_history_incomplete:0x${incomplete.toString(16)}` : undefined;
					return incomplete ? undefined : answer.readBigUInt64LE(16);
				},
				changes: (
					since: bigint,
					inputs: readonly {
						readonly dev: bigint;
						readonly ino: bigint;
						readonly contents?: boolean;
					}[],
				) => {
					if (phase !== "watching") return;
					const objects = new Set<string>();
					const unstable = new Set<string>();
					let version: bigint | undefined;
					try {
						for (let start = 0; start < Math.max(1, inputs.length); start += changeBatch) {
							const batch = inputs.slice(start, start + changeBatch);
							const values: (number | bigint)[] = [since];
							for (const input of batch) values.push(input.dev, input.ino, input.contents === false ? 2 : 3);
							const answer = request(7, id, values);
							if (answer.readUInt32LE(4) !== values.length) return;
							version ??= answer.readBigUInt64LE(16);
							for (const [index, input] of batch.entries()) {
								const flags = answer.readBigUInt64LE(40 + index * 24);
								const key = `${input.dev}:${input.ino}`;
								if (flags & 1n) objects.add(key);
								if (flags & 2n) unstable.add(key);
							}
						}
						return { version: version!, objects, unstable };
					} catch {
						return;
					}
				},
				close: () => {
					if (phase === "closed") return;
					phase = "closed";
					try {
						request(4, id);
					} finally {
						if (descriptor !== undefined) closeSync(descriptor);
					}
				},
			};
		},
	};
	check();
	return collector;
}
/** Bind a lookup to the collector's continuous history. The final check runs only
 * after the native consumer has staged its immutable bytes and before it is armed. */
export function executionDomainProof(
	collector: Awaited<ReturnType<typeof executionDomainCollector>>,
	root: string,
) {
	const namespace = statSync("/proc/self/ns/mnt", { bigint: true });
	const domain = collector.open();
	let reason = domain.watch([root]) ?? "";
	const since = domain.version();
	if (since === undefined) reason ||= domain.failure || "observer_unavailable";
	return {
		get reason() {
			return reason;
		},
		seal(
			dependencies: readonly DynamicDependency[],
			resolve: (target: string) => string | undefined,
		): () => boolean {
			const objects = new Map<
				string,
				{
					dev: bigint;
					ino: bigint;
					path: string;
				}
			>();
			const bindings = new Map<
				string,
				{
					dev: bigint;
					ino: bigint;
					contents: boolean;
				}
			>();
			const lookedUp = new Map<
				string,
				{
					dev: bigint;
					ino: bigint;
				}
			>();
			let supported = since !== undefined;
			const observe = (target: string, contents: boolean) => {
				let first = true;
				for (;;) {
					try {
						const previous = lookedUp.get(target);
						const info = previous ?? lstatSync(target, { bigint: true });
						const key = `${info.dev}:${info.ino}`;
						bindings.set(key, {
							dev: info.dev,
							ino: info.ino,
							contents: bindings.get(key)?.contents || (first && contents),
						});
						if (first && contents) objects.set(key, { dev: info.dev, ino: info.ino, path: target });
						if (previous) break;
						lookedUp.set(target, { dev: info.dev, ino: info.ino });
					} catch (error) {
						if (!["ENOENT", "ENOTDIR"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
					}
					const parent = path.dirname(target);
					if (target === parent) break;
					target = parent;
					first = false;
				}
			};
			try {
				for (const input of dependencies) {
					if (input.kind === "fd") continue;
					if (input.kind === "lock") {
						reason = "unobserved_lock";
						supported = false;
						continue;
					}
					for (const logical of [input.path, ...(input.kind === "file" ? (input.aliases ?? []) : [])]) {
						let target = resolve(logical);
						if (!target) {
							reason = "unmapped_path";
							supported = false;
							continue;
						}
						if (input.kind === "absence") {
							target = path.dirname(target);
							for (;;) {
								try {
									lstatSync(target);
									break;
								} catch (error) {
									if (
										!["ENOENT", "ENOTDIR"].includes((error as NodeJS.ErrnoException).code ?? "") ||
										target === path.dirname(target)
									)
										throw error;
									target = path.dirname(target);
								}
							}
						}
						observe(target, true);
						// Preserve symlink lookup ancestors as well as the resolved object's ancestors.
						try {
							observe(realpathSync.native(target), true);
						} catch (error) {
							if (input.kind !== "symlink" || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
						}
					}
				}
			} catch (error) {
				reason = `input_lookup:${(error as Error).message}`;
				supported = false;
			}
			const inputs = [...bindings.values()];
			const unchanged = () => {
				const changes = domain.changes(since!, inputs);
				if (changes && !changes.objects.size) return true;
				reason = "input_changed_or_observer_discontinuity";
				return false;
			};
			return () => {
				try {
					if (!supported || domain.closed || !unchanged()) return false;
					if (!domain.admit([...objects.values()])) {
						reason = "input_census_unstable_or_unsupported";
						return false;
					}
					if (!unchanged()) return false;
					if (statSync("/proc/self/ns/mnt", { bigint: true }).ino !== namespace.ino) {
						reason = "mount_namespace_changed";
						return false;
					}
					return true;
				} catch (error) {
					reason = `observer_error:${(error as Error).message}`;
					return false;
				}
			};
		},
		close() {
			try {
				domain.close();
			} catch {
				/* A lost owner already revoked its evidence. */
			}
		},
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
}
export interface HeldFileDescriptor {
	readonly fd: number;
	readonly alias: number;
	readonly device: string;
	readonly inode: string;
	readonly flags: number;
	readonly offset: OFDPosition;
	readonly capacity?: number;
	readonly eof?: boolean;
	readonly queueHex?: string;
}
export interface HeldExecSnapshot extends Partial<ProcessLaunchOptions> {
	readonly executable: string;
	readonly invocation?: readonly [path: string, comm: string];
	readonly argv: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: Pick<ProcessExecutionContext, "key" | "umask" | "descriptorTypes">;
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
	readonly barrier: "none" | "descriptors";
	readonly outcome: "continued" | "adopted" | "declined" | "failed";
	readonly native?: HeldExecClock;
}
export type HeldExecDecision = {
	readonly observeTiming?: (timing: HeldExecTiming) => void;
} &
	/** Continue only this held launch; a later launch must observe candidates available then. */
	(| {
				readonly kind: "continue";
				readonly observeCompletion?: (
					durationMs: number | undefined,
					clock?: HeldExecClock,
				) => void | Promise<void>;
		  }
		| {
				readonly kind: "replay";
				/** Final proof after staging, while declining still leaves native execution intact. */
				readonly validate: () => boolean | Promise<boolean>;
				/** Release a reservation only when the native peer has definitely performed no adoption effects. */
				readonly settle?: (consumed: boolean) => void | Promise<void>;
				readonly output: readonly {
					readonly fd: 1 | 2;
					readonly data: Buffer;
				}[];
				/** Called only after the native tracer has made original execution impossible. */
				readonly commit: () => Promise<void>;
				readonly adopted?: () => void;
				readonly exitCode: number;
		  }
	);
export interface LinuxHeldExecOptions {
	readonly storeRoot: string;
	readonly reservedRoot?: boolean;
	readonly binary?: string;
	readonly observerBinary?: string;
	readonly signal?: AbortSignal;
}
type ActiveExecution = ReturnType<LinuxHeldExecBoundary["registerExecution"]>;
export interface HeldObservation {
	readonly sourceRoot: string;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	readonly decide: (process: HeldExecProcess) => Promise<HeldExecDecision>;
	readonly descriptors?: boolean;
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
}
/** A two-phase held-exec transport: arm the exit stub before committing reusable effects. */
export class LinuxHeldExecBoundary {
	private readonly token = randomBytes(32).toString("hex");
	private readonly active = new Map<string, ActiveExecution>();
	private readonly calls = new Set<
		PromiseWithResolvers<void> & {
			readonly controller: AbortController;
		}
	>();
	private readonly server: net.Server;
	private readonly sockets = new Set<net.Socket>();
	private readonly socketPath: string;
	get endpoint() {
		return this.socketPath;
	}
	private closing?: Promise<void>;
	private readonly startup: (operation: number) => number;
	private readonly observerBinary: string;
	private constructor(socketPath: string, native: Awaited<ReturnType<typeof qualifyNativeStartup>>) {
		this.startup = native.startup;
		this.observerBinary = native.observer;
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
		try {
			native = await startupScope.exit(() =>
				qualifyNativeStartup(binary, {
					observer: options.observerBinary,
					signal: options.signal,
				}),
			);
		} catch (error) {
			options.signal?.throwIfAborted();
			throw new Error(
				`held-exec functional probe failed${await inheritedTracer()}: ${error instanceof Error ? error.message : error}`,
				{ cause: error },
			);
		}
		if (!options.reservedRoot) await mkdir(options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(options.storeRoot, 0o700);
		const candidate = path.join(
			options.storeRoot,
			`held-${process.pid}-${randomBytes(6).toString("hex")}.sock`,
		);
		const socketPath =
			Buffer.byteLength(candidate) < 104
				? candidate
				: path.join(
						"/tmp",
						`pi-held-${process.getuid?.() ?? 0}-${process.pid}-${randomBytes(6).toString("hex")}.sock`,
					);
		const boundary = new LinuxHeldExecBoundary(socketPath, native);
		await listenUnixSocket(boundary.server, socketPath);
		try {
			await chmod(socketPath, 0o600);
			options.signal?.throwIfAborted();
			return boundary;
		} catch (error) {
			await boundary.close();
			throw error;
		}
	}
	/** An existing whole-command adapter already owns its root; descendants still expose their operations. */
	static command<Value>(execute: () => Promise<Value>): Promise<Value> {
		const scope = startupScope.getStore();
		return scope ? startupScope.run({ ...scope, skipRoot: true }, execute) : execute();
	}
	/** The callback owns admission; each actual native child owns a distinct ticket and observer. */
	async observe<Value>(options: HeldObservation, execute: () => Promise<Value>): Promise<Value> {
		if (this.closing || options.signal?.aborted) return execute();
		const controller = new AbortController();
		const launches = new Set<{
			readonly child: ChildProcess;
			readonly attachment: HeldExecAttachment;
		}>();
		const signal = AbortSignal.any([controller.signal, ...(options.signal ? [options.signal] : [])]);
		const snapshot = {
			...options,
			scope: snapshotExecutionScope(options.scope),
			signal,
		};
		let closed = false;
		const call = { controller, ...Promise.withResolvers<void>() };
		this.calls.add(call);
		try {
			const available = () => !closed && !signal.aborted;
			return await startupScope.run(
				{
					skipRoot: options.skipRoot,
					launch: (start) => {
						if (!available()) return start();
						let child: ChildProcess | undefined;
						return this.start(
							(gate) => {
								const attachment = this.attach(
									{ ...snapshot, skipRoot: startupScope.getStore()?.skipRoot },
									gate,
								);
								return {
									pid: attachment.pid,
									finish: () => {
										if (child?.pid) launches.add({ child, attachment });
										else attachment.retire();
									},
								};
							},
							() => {
								child = start();
								return child;
							},
						);
					},
				},
				execute,
			);
		} finally {
			closed = true;
			for (const { child, attachment } of launches)
				if (!child.pid || (child.exitCode === null && child.signalCode === null)) attachment.retire();
			const settled = await Promise.allSettled([...launches].map((launch) => launch.attachment.completion));
			this.calls.delete(call);
			call.resolve();
			for (const result of settled) if (result.status === "rejected") throw result.reason;
		}
	}
	private start<Value>(
		attach: (gate: number) => {
			readonly pid?: number;
			readonly finish: () => void;
		},
		start: () => Value,
	): Value {
		let attachment: ReturnType<typeof attach> | undefined;
		let gate = -1;
		try {
			gate = this.startup(0);
			if (!Number.isInteger(gate) || gate < 0) throw new Error("native startup gate unavailable");
			attachment = attach(gate);
			if (!attachment.pid || this.startup(attachment.pid) !== 0)
				throw new Error("native observer unavailable");
		} catch {
			if (gate >= 0) this.startup(-1);
			try {
				attachment?.finish();
			} catch {
				/* No authoritative child was started. */
			}
			attachment = undefined;
		}
		// Never catch the authoritative spawn: its synchronous failure must not rerun it.
		try {
			return start();
		} finally {
			if (gate >= 0) this.startup(-1);
			attachment?.finish();
		}
	}
	/** The gate belongs to the launching thread; an observer may serve it from another worker. */
	attach(options: HeldObservation, gate: number): HeldExecAttachment {
		const { observer, completion, retire } = this.createAttachment(options, gate);
		return { pid: observer.pid, completion, retire };
	}
	private createAttachment(options: HeldObservation, gate: number) {
		if (this.closing) throw new Error("native observation is closing");
		const execution = randomBytes(24).toString("hex");
		const controller = new AbortController();
		const observer = startupScope.exit(() =>
			spawn(this.observerBinary, ["--attach"], {
				stdio: ["pipe", "pipe", "ignore", gate],
				env: {
					PI_SPEC_DOMAIN_SOCKET: process.env.PI_SPEC_DOMAIN_SOCKET,
					PI_SPEC_HELD_EXEC_SOCKET: this.socketPath,
					PI_SPEC_HELD_EXEC_TOKEN: this.token,
					PI_SPEC_HELD_EXEC_ID: execution,
					...(options.descriptors ? { PI_SPEC_HELD_EXEC_DESCRIPTORS: "1" } : {}),
					...(options.skipRoot ? { PI_SPEC_HELD_EXEC_ROOT: "skip" } : {}),
				},
			}),
		);
		observer.on("error", () => {});
		observer.stdin?.on("error", () => {});
		observer.unref();
		const done = new Promise<void>((resolve) => {
			observer.once("close", () => resolve());
			observer.stdout!.once("data", (data) => {
				if (data.toString() === "R") {
					observer.stdout!.destroy();
					resolve();
				}
			});
		});
		return this.registerExecution(options, observer, execution, controller, done, () =>
			observer.stdin?.end(),
		);
	}
	/** Attach at the controlled launch gate before the kernel installs its first image. */
	trace<Value>(options: HeldObservation, execute: () => Promise<Value>): Promise<Value> {
		return this.observe({ ...options, skipRoot: true }, execute);
	}
	private registerExecution(
		options: HeldObservation,
		observer: ChildProcess,
		execution: string,
		controller: AbortController,
		done: Promise<void>,
		stop: () => void,
	) {
		const signal: AbortSignal = AbortSignal.any([
			controller.signal,
			...(options.signal ? [options.signal] : []),
		]);
		const retire = (): void => {
			if (!controller.signal.aborted) controller.abort(new Error("native observation retired"));
			stop();
			for (const socket of active.sockets) socket.destroy();
		};
		const active = {
			...options,
			signal,
			observer,
			retire,
			sequence: 0,
			failure: undefined as Error | undefined,
			decide: startupScope.exit(() => AsyncResource.bind(options.decide)),
			barrier: (options.descriptors ? "descriptors" : "none") as HeldExecTiming["barrier"],
			pending: new PendingTasks(),
			sockets: new Set<net.Socket>(),
			completion: done.then(async (): Promise<void> => {
				this.active.delete(execution);
				await active.pending.drain();
				signal.removeEventListener("abort", retire);
				if (active.failure) throw active.failure;
			}),
		};
		active.completion.catch(() => undefined);
		this.active.set(execution, active);
		signal.addEventListener("abort", retire, { once: true });
		return active;
	}
	close(): Promise<void> {
		return (this.closing ??= Promise.resolve().then(async () => {
			const stopped = new Promise<void>((resolve) => this.server.close(() => resolve()));
			for (const call of this.calls) call.controller.abort(new Error("held-exec boundary disposed"));
			const active = [...this.active.values()];
			for (const execution of active) execution.retire();
			for (const socket of this.sockets) socket.destroy();
			await Promise.allSettled([
				stopped,
				...[...this.calls].map((call) => call.promise),
				...active.map((execution) => execution.completion),
			]);
			await rm(this.socketPath, { force: true }).catch(() => undefined);
		}));
	}
	private async serve(socket: net.Socket): Promise<void> {
		const requestedAt = performance.now();
		let prepared = false;
		let active: ActiveExecution | undefined;
		let decision: HeldExecDecision | undefined;
		let native: HeldExecClock | undefined;
		let completedAt: number | undefined;
		let committedAt: number | undefined;
		let outcome: HeldExecTiming["outcome"] = "declined";
		const { promise: pending, resolve: release } = Promise.withResolvers<void>();
		try {
			const request = parseRequest(await readLine(socket));
			active = request?.token === this.token ? this.active.get(request.execution) : undefined;
			active?.pending.track(pending);
			active?.sockets.add(socket);
			if (!request || !active || !(await heldBy(request, active.observer))) {
				return void socket.end("C\n");
			}
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
			});
			active.signal?.throwIfAborted();
			if (decision.kind === "continue") {
				completedAt = performance.now();
				outcome = "continued";
				if (!decision.observeCompletion) return void socket.end("C\n");
				native = await observeCompletion(socket, decision.observeCompletion);
				if (native) completedAt = native.startedAt;
				return;
			}
			const code = decision.exitCode;
			const total = decision.output.reduce((sum, event) => sum + event.data.length, 0);
			if (
				!isSafeCount(code, 0, 255) ||
				decision.output.length > speculativeConfig().processReuse.maxOutputEvents ||
				total > speculativeConfig().processReuse.maxCaptureBytes ||
				decision.output.some((event) => ![1, 2].includes(event.fd) || !Buffer.isBuffer(event.data))
			)
				return void socket.end("C\n");
			await writeProcessChannel(
				socket,
				Buffer.from(`P2 ${code} ${decision.output.length} ${total} 0 0 0 0 0\n`),
			);
			for (const event of decision.output) {
				await writeProcessChannel(socket, Buffer.from(`O ${event.fd} ${event.data.length}\n`));
				await writeProcessChannel(socket, event.data);
			}
			const acknowledgement = await readLine(socket);
			if (acknowledgement === "N") {
				prepared = false;
				return void socket.end();
			}
			if (acknowledgement !== "S") throw new Error("held-exec staging was not acknowledged");
			if (!(await decision.validate())) {
				outcome = "declined";
				return void socket.end("C\n");
			}
			active.signal?.throwIfAborted();
			// This grant owns the observed snapshot. Later source changes cannot alter the staged bytes.
			// From this point a lost ACK may mean the peer has already armed its exit stub.
			prepared = true;
			await writeProcessChannel(socket, Buffer.from("G\n"));
			if ((await readLine(socket)) !== "A") throw new Error("held-exec adoption was not acknowledged");
			active.signal?.throwIfAborted();
			await decision.commit();
			committedAt = performance.now();
			await writeProcessChannel(socket, Buffer.from("R\n"));
			if ((await readLine(socket)) !== "D") throw new Error("held-exec adoption completion is unknown");
			completedAt = performance.now();
			outcome = "adopted";
			try {
				decision.adopted?.();
			} catch {
				/* Feedback cannot poison a completed handoff. */
			}
			socket.end();
		} catch (error) {
			completedAt ??= performance.now();
			outcome = "failed";
			if (active && (prepared || isPoisonedEffectCommit(error))) {
				// The logical call, not just the held child, owns an irreversible handoff.
				active.failure ??= effectCommitFailure(
					new Error("held-exec handoff failed", { cause: error }),
					"poisoned",
				);
			}
			if (!socket.destroyed) socket.end(active?.failure ? "F\n" : "C\n");
		} finally {
			if (decision?.kind === "replay") {
				try {
					await decision.settle?.(prepared);
				} catch {
					/* A failed durable settlement leaves the claim unavailable. */
				}
				completedAt = performance.now();
			}
			if (active && decision?.observeTiming)
				try {
					decision.observeTiming({
						requestedAt,
						completedAt: completedAt ?? performance.now(),
						barrier: active.barrier,
						outcome,
						...(committedAt !== undefined ? { committedAt } : {}),
						...(native ? { native } : {}),
					});
				} catch {
					/* Timing evidence cannot replace the process outcome. */
				}
			active?.sockets.delete(socket);
			release();
		}
	}
}
async function observeCompletion(
	socket: net.Socket,
	observe: (durationMs: number | undefined, clock?: HeldExecClock) => void | Promise<void>,
): Promise<HeldExecClock | undefined> {
	const startedAt = performance.now();
	let clock: HeldExecClock | undefined;
	try {
		await writeProcessChannel(socket, Buffer.from("O\n"));
		if ((await readLine(socket)) === "D")
			clock = Object.freeze({ startedAt, completedAt: performance.now() });
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
/** Inspect the installed, stopped image; stdio mode leaves extra descriptors to the broker capture. */
export async function inspectHeldExecProcess(
	pid: number,
	executable: string,
	descriptors?: readonly HeldFileDescriptor[] | "stdio",
): Promise<
	HeldExecSnapshot & {
		readonly context: ProcessExecutionContext;
	}
> {
	const root = `/proc/${pid}`;
	const [cwd, command, environmentBytes, descriptorNames] = await Promise.all([
		readlink(`${root}/cwd`),
		readFile(`${root}/cmdline`),
		readFile(`${root}/environ`),
		readdir(`${root}/fd`),
	]);
	const evidence = descriptors === "stdio" ? undefined : (descriptors ?? []);
	if (!validDescriptors(evidence)) throw new Error("invalid native descriptor context");
	const context = await captureProcessContext(
		pid,
		descriptors === "stdio" ? descriptorNames.filter((name) => /^[012]$/.test(name)) : descriptorNames,
	);
	if (descriptors !== "stdio" && !validProcessContext(context))
		throw new Error("held process descriptors are not replayable");
	const argv = decodeNullFields(command);
	if (!argv.length) throw new Error("held process argv is empty");
	const environment: Record<string, string> = Object.create(null);
	for (const entry of decodeNullFields(environmentBytes)) {
		const separator = entry.indexOf("=");
		if (separator < 1 || Object.hasOwn(environment, entry.slice(0, separator)))
			throw new Error("held process environment is not canonical");
		environment[entry.slice(0, separator)] = entry.slice(separator + 1);
	}
	let invokedPath: string | undefined;
	{
		const auxiliary = await readFile(`${root}/auxv`);
		const memory = await open(`${root}/mem`, "r");
		try {
			for (let at = 0; at + 16 <= auxiliary.length; at += 16)
				if (auxiliary.readBigUInt64LE(at) === 31n) {
					const bytes = Buffer.alloc(speculativeConfig().processReuse.invocationPathBytes);
					const { bytesRead } = await memory.read(
						bytes,
						0,
						bytes.length,
						Number(auxiliary.readBigUInt64LE(at + 8)),
					);
					const end = bytes.subarray(0, bytesRead).indexOf(0);
					if (end >= 0) invokedPath = bytes.subarray(0, end).toString();
					break;
				}
		} finally {
			await memory.close();
		}
		if (!invokedPath) throw new Error("held process invocation name is unavailable");
	}
	return {
		executable,
		...processLaunchOptions(context),
		argv,
		cwd,
		environment,
		context,
		invocation: [invokedPath, (await readFile(`${root}/comm`, "utf8")).replace(/\n$/, "")],
	};
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
			/^[0-9a-f]{48}$/.test(value.execution ?? "") &&
			isSafeCount(value.pid, 1) &&
			isSafeCount(value.tracer, 1) &&
			(value.holder === undefined || isSafeCount(value.holder)) &&
			validDescriptors(value.descriptors)
			? immutableSnapshot(value as WireRequest)
			: undefined;
	} catch {
		return undefined;
	}
}
function validDescriptors(descriptors: WireRequest["descriptors"]): boolean {
	return descriptors === undefined || (Array.isArray(descriptors) && descriptors.length === 0);
}
export async function inheritedTracer(): Promise<string> {
	const tracer = /^TracerPid:\s*([1-9]\d*)$/m.exec(
		await readFile("/proc/self/status", "utf8").catch(() => ""),
	)?.[1];
	return tracer ? `: processes here are already traced by pid ${tracer}` : "";
}
async function heldBy({ pid, tracer, holder }: WireRequest, observer: ChildProcess): Promise<boolean> {
	const alive = () => observer.pid === tracer && observer.exitCode === null && observer.signalCode === null;
	if (!alive()) return false;
	try {
		const status = await readFile(`/proc/${pid}/status`, "utf8");
		if (holder) {
			const owner = await readFile(`/proc/${holder}/status`, "utf8");
			if (
				!new RegExp(`^PPid:\\s*${tracer}$`, "m").test(owner) ||
				!/^NoNewPrivs:\s+1$/m.test(owner) ||
				!["Prm", "Eff", "Inh", "Amb"].every((kind) => new RegExp(`^Cap${kind}:\\s+0+$`, "m").test(owner)) ||
				![
					["Uid", process.getuid!()],
					["Gid", process.getgid!()],
				].every(([kind, id]) => new RegExp(`^${kind}:(?:\\s+${id}){4}$`, "m").test(owner))
			)
				return false;
		}
		return (
			alive() && new RegExp(`^TracerPid:\\s*${tracer}$`, "m").test(status) && /^State:\s+t\b/m.test(status)
		);
	} catch {
		return false;
	}
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
			if (Buffer.byteLength(body) > speculativeConfig().processReuse.maxRequestBytes)
				throw new Error("held-exec message too large");
		}
		throw new Error("held-exec peer closed");
	} finally {
		socket.pause();
	}
}
export function writeProcessChannel(channel: NodeJS.WritableStream, data: Buffer): Promise<void> {
	return new Promise((resolve, reject) =>
		channel.write(data, (error) => (error ? reject(error) : resolve())),
	);
}
export async function listenUnixSocket(server: net.Server, socketPath: string): Promise<void> {
	await once(server.listen(socketPath), "listening");
}
