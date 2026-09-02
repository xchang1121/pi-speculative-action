import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
	access,
	chmod,
	link,
	lstat,
	mkdir,
	mkdtemp,
	readFile,
	readdir,
	realpath,
	rm,
	symlink,
	writeFile,
} from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import {
	createExecPrototype,
	digestObject,
	type DynamicDependency,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type ExitOutcome,
	type OrderedEffectEvent,
	type ProcessProducerProof,
	processWeakKey,
	type ProvenanceTaint,
	sealProcessCertificate,
	sha256Digest,
	type Sha256Digest,
	type WorkspaceEffectState,
} from "./provenance-certificate.ts";
import {
	captureAbsenceDependency,
	captureDirectoryDependency,
	captureFileDependency,
	captureSymlinkDependency,
	validateDynamicDependencyCertificate,
} from "./provenance-validation.ts";
import {
	diffWorkspaceStructures,
	ExecutionPathProjection,
	hydrateWorkspaceFileEntry,
	snapshotDependency,
	type WorkspaceStructureSnapshot,
	type WorkspaceTreeEntry,
} from "./process-observation.ts";
import type { ProcessExecutionRequest, ProcessExecutionResult, ProcessExecutor } from "./process-execution.ts";
import {
	emptyWorldReuseMetrics,
	type ExecutionScope,
	type ExecutionWorldStorageControl,
	type WorldReuseMetrics,
} from "./execution-world.ts";
import { type ProcessReusePlan, ProcessReusePlanner } from "./reuse-planner.ts";
import {
	ProvenanceCertificateStore,
	type ProvenanceStoreOptions,
	type VerifiedArtifactClosure,
} from "./reuse-store.ts";
import { observeStrace, straceCommand, type StraceObservation } from "./strace-observer.ts";
import type { ToolProcessInvocation } from "./tool-settlement.ts";
import type { ResourceValidation } from "./settlement.ts";
import {
	commitSandboxDelta,
	readSandboxDirectoryState,
	type SandboxDirectoryChange,
	type SandboxFileChange,
	type SandboxWorkspaceChange,
	type SandboxWorkspaceContext,
} from "./workspace-sandbox.ts";
import type { WorkspaceRegularDelta } from "./workspace-transaction.ts";

const BACKEND_EPOCH = "pi-linux-process-v10";
const POLICY_ID = "sandlock-namespaced-transparent-exec-v9";
const MAX_REQUEST_BYTES = 4 * 1024 * 1024;
const MAX_CAPTURE_BYTES = 512 * 1024 * 1024;
/** Native inputs consumed by this exact one-shot execution; they still prohibit any later replay. */
const TRANSFERRED_INPUT_TAINTS = new Set<ProvenanceTaint>(["clock", "random", "pid_observation"]);

export interface LinuxProcessBackendOptions {
	readonly storeRoot: string;
	readonly store?: ProvenanceStoreOptions;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
	readonly unshareBinary?: string;
	/** Additional host paths that speculative processes must never read. */
	readonly deniedPaths?: readonly string[];
}

export interface CompletedProcessReplayOptions {
	readonly sourceRoot: string;
	readonly invocation: (request: ProcessExecutionRequest) => ToolProcessInvocation | undefined;
	readonly enabled?: () => boolean;
}

export interface LinuxProcessBackendStatus {
	readonly state: "ready" | "unavailable";
	readonly detail: string;
	readonly fingerprint?: string;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
	readonly unshareBinary?: string;
}

export type LinuxProcessReuseMetrics = WorldReuseMetrics;
type CountedReuseMetric = Exclude<keyof WorldReuseMetrics, "lastError">;
type MutableLinuxProcessReuseMetrics = { -readonly [Key in CountedReuseMetric]: number } & {
	lastError?: string;
};

export interface LinuxProcessSession {
	readonly executor: ProcessExecutor;
	readonly metrics: () => LinuxProcessReuseMetrics;
	/** Join the outer workspace transaction delta to the process observation before validation. */
	readonly seal: (changes: readonly SandboxWorkspaceChange[]) => Promise<readonly SandboxDirectoryChange[]>;
	/** Revalidate every observed input immediately before Actor adoption. */
	readonly validate: () => Promise<ResourceValidation>;
	readonly close: () => Promise<void>;
}

interface ReadyBackend {
	readonly sandlock: string;
	readonly strace: string;
	readonly unshare: string;
	readonly mount: string;
	readonly fingerprint: string;
	readonly platformFingerprint: Sha256Digest;
	readonly observerFingerprint: Sha256Digest;
}

interface InterposedDirectory {
	readonly source: string;
	readonly target: string;
	readonly shadow: string;
	readonly view: string;
}

interface ProcessInterposition {
	readonly launcher: string;
	readonly namespaceLauncher: string;
	readonly planPath: string;
	readonly directories: readonly InterposedDirectory[];
	readonly executablePaths: readonly string[];
	readonly dependencies: readonly DynamicDependency[];
}

interface DispatcherRequest {
	readonly version: 1;
	readonly token: string;
	readonly name: string;
	readonly invokedPath: string;
	readonly args: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly umask: number;
	readonly stdin: "closed";
}

interface BufferedOutput {
	readonly fd: 1 | 2;
	readonly data: Buffer;
}

interface DispatcherResponse {
	readonly version: 1;
	readonly kind: "hit" | "executed" | "bypass";
	readonly executable?: string;
	readonly output?: readonly { readonly fd: 1 | 2; readonly data: string }[];
	readonly exit?: ExitOutcome;
	readonly weakKey?: Sha256Digest;
}

interface ActiveSession {
	readonly token: string;
	readonly sourceRoot: string;
	readonly workspace: SandboxWorkspaceContext;
	readonly invocation: ToolProcessInvocation;
	readonly scope?: ExecutionScope;
	readonly projection: ExecutionPathProjection;
	readonly interposition: ProcessInterposition;
	readonly originalPath: string;
	readonly deniedPaths: readonly string[];
	readonly producer: ProcessProducerProof;
	readonly socketPath: string;
	readonly server: net.Server;
	readonly nestedEvidence: DynamicDependencyCertificate[];
	readonly incompleteReasons: Set<string>;
	readonly metrics: MutableLinuxProcessReuseMetrics;
	topLevelCapture?: TopLevelCapture;
	topLevelExecution?: {
		readonly prototype: ExecPrototype;
		readonly outcome: SpawnOutcome;
		readonly observedProcessMs: number;
	};
	topLevelEvidence?: DynamicDependencyCertificate;
	sealPromise?: Promise<readonly SandboxDirectoryChange[]>;
	closed: boolean;
}

interface TopLevelCapture {
	readonly before: WorkspaceStructureSnapshot;
	readonly after: WorkspaceStructureSnapshot;
	readonly observation: StraceObservation;
}

interface WorkspaceDependencySource {
	readonly entry: (physicalPath: string) => Promise<WorkspaceTreeEntry | undefined>;
	readonly parentEntry: (physicalPath: string) => Promise<WorkspaceTreeEntry | undefined>;
}

interface SpawnOutcome {
	readonly code: number | null;
	readonly signal: NodeJS.Signals | null;
	readonly output: readonly BufferedOutput[];
}

/** Linux-only process substrate. Unavailable dependencies remove the route instead of weakening it. */
export class LinuxProcessReuseBackend {
	readonly store: ProvenanceCertificateStore;
	readonly planner: ProcessReusePlanner;
	readonly storage: ExecutionWorldStorageControl;
	private readonly options: LinuxProcessBackendOptions;
	private ready?: Promise<ReadyBackend>;
	private platformFingerprint?: Promise<Sha256Digest>;
	private disposed = false;
	private readonly inFlight = new Map<Sha256Digest, Promise<void>>();
	private readonly certificateScopes = new Map<Sha256Digest, ExecutionScope>();
	private readonly counters: MutableLinuxProcessReuseMetrics = { ...emptyWorldReuseMetrics() };

	constructor(options: LinuxProcessBackendOptions) {
		this.options = options;
		this.store = new ProvenanceCertificateStore(options.storeRoot, options.store);
		this.planner = new ProcessReusePlanner({ store: this.store });
		this.storage = {
			configure: ({ maxEntries, maxBytes }) =>
				this.store.configure({ maxCertificates: maxEntries, maxBytes }),
			maintain: async (operation) => {
				const result = await (operation === "gc" ? this.store.gc() : this.store.clear());
				if (operation === "clear") this.certificateScopes.clear();
				return {
					removedEntries: result.removedCertificates,
					removedArtifacts: result.removedArtifacts,
					removedBytes: result.removedBytes,
				};
			},
		};
	}

	async check(refresh = false): Promise<LinuxProcessBackendStatus> {
		if (process.platform !== "linux") return { state: "unavailable", detail: "Linux host required" };
		if (refresh) this.ready = undefined;
		try {
			const ready = await this.resolveReady();
			return {
				state: "ready",
				detail: "Landlock/seccomp + user/PID/network/IPC namespaces + strace provenance ready",
				fingerprint: ready.fingerprint,
				sandlockBinary: ready.sandlock,
				straceBinary: ready.strace,
				unshareBinary: ready.unshare,
			};
		} catch (error) {
			return { state: "unavailable", detail: errorMessage(error) };
		}
	}

	async fingerprint(): Promise<string> {
		return (await this.resolveReady()).fingerprint;
	}

	metrics(): LinuxProcessReuseMetrics {
		return Object.freeze({ ...this.counters });
	}

	/** Hit-only Actor path. Certificate lookup and replay deliberately require no tracing or confinement tools. */
	completedReplayExecutor(host: ProcessExecutor, options: CompletedProcessReplayOptions): ProcessExecutor {
		const sourceRoot = path.resolve(options.sourceRoot);
		const acceptProducer = (producer: ProcessProducerProof) =>
			actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
		return {
			execute: async (request) => {
				if (process.platform !== "linux" || options.enabled?.() === false || !pathContains(sourceRoot, request.cwd)) {
					return host.execute(request);
				}
				const requestStarted = performance.now();
				this.counters.wholeCommandRequests++;
				let committed = false;
				try {
					throwIfAborted(request.signal);
					const invocation = options.invocation(request);
					if (!invocation) return this.actorReplayMiss(host, request);
					assertInvocationMatches(invocation, request);
					const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
					const prototype = await actorTopLevelPrototype(
						invocation,
						request,
						projection,
						await this.resolvePlatformFingerprint(),
					);
					const plan = await this.planner.plan({
						prototype,
						acceptProducer,
						contract: {
							sink: "buffered",
							orderedJournal: true,
							transactionalEffects: true,
							mode: "completed_replay",
						},
						validation: { resolvePath: (logicalPath) => projection.toPhysical(logicalPath) },
					});
					this.recordLookup(plan.lookup);
					if (plan.kind !== "completed_replay") {
						if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
							this.counters.lastError = `actor_reuse_miss:${plan.reasons.join(",")}`;
						}
						return this.actorReplayMiss(host, request);
					}
					throwIfAborted(request.signal);
					const replayStarted = performance.now();
					await replayFilesystemEffects(plan.artifacts, plan.certificate.result.journal, projection, sourceRoot);
					committed = true;
					for (const event of loadOutputEvents(plan.artifacts, plan.certificate.result.journal)) request.onData(event.data);
					this.counters.wholeCommandReplayMs += Math.max(0, performance.now() - replayStarted);
					this.counters.wholeCommandAvoidedProcessMs += plan.certificate.result.observedProcessMs ?? 0;
					this.counters.wholeCommandHitOverheadMs += Math.max(0, performance.now() - requestStarted);
					this.counters.wholeCommandHits++;
					return { exitCode: plan.certificate.result.exit.kind === "code" ? plan.certificate.result.exit.code : null };
				} catch (error) {
					this.counters.lastError = `actor_replay:${errorMessage(error)}`;
					if (committed || error instanceof AggregateError) throw error;
					return this.actorReplayMiss(host, request);
				}
			},
		};
	}

	async open(input: {
		readonly sourceRoot: string;
		readonly workspace: SandboxWorkspaceContext;
		readonly invocation: ToolProcessInvocation;
		readonly scope?: ExecutionScope;
		readonly signal?: AbortSignal;
	}): Promise<LinuxProcessSession> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		const ready = await this.resolveReady();
		throwIfAborted(input.signal);
		const sourceRoot = path.resolve(input.sourceRoot);
		const projection = new ExecutionPathProjection({
			sourceRoot,
			workspaceRoot: input.workspace.sandboxRoot,
			privateRoot: input.workspace.processRoot,
		});
		const originalPath = input.invocation.environment.PATH ?? input.invocation.environment.Path ?? "";
		const token = randomToken();
		const socketPath = path.join(input.workspace.processRoot, `broker-${token.slice(0, 12)}.sock`);
		const interposition = await createProcessInterposition({
			privateRoot: input.workspace.processRoot,
			pathValue: originalPath,
			projection,
			sourceRoot,
			workspaceRoot: input.workspace.sandboxRoot,
			workspaceExcludes: input.workspace.observationExcludes,
			token,
			socketPath,
			mountBinary: ready.mount,
			excludedExecutables: [
				input.invocation.shell,
				process.execPath,
				ready.sandlock,
				ready.strace,
				ready.unshare,
				ready.mount,
			],
		});
		const deniedPaths = sensitivePaths(this.options.storeRoot, this.options.deniedPaths).filter(
			(target) =>
				!pathContains(input.workspace.sandboxRoot, target) && !pathContains(input.workspace.processRoot, target),
		);
		const producer = speculativeProducerProof(ready, deniedPaths);
		const session = {} as ActiveSession;
		const server = net.createServer({ allowHalfOpen: true }, (socket) => this.serve(session, socket));
		Object.assign(session, {
			token,
			sourceRoot,
			workspace: input.workspace,
			invocation: input.invocation,
			scope: input.scope,
			projection,
			interposition,
			originalPath,
			deniedPaths,
			producer,
			socketPath,
			server,
			nestedEvidence: [],
			incompleteReasons: new Set<string>(),
			metrics: { ...emptyWorldReuseMetrics() },
			closed: false,
		});
		await listen(server, socketPath);
		const close = async () => {
			if (session.closed) return;
			session.closed = true;
			await closeServer(server);
			await rm(socketPath, { force: true }).catch(() => undefined);
		};
		return {
			executor: { execute: (request) => this.executeTopLevel(session, request) },
			metrics: () => Object.freeze({ ...session.metrics }),
			seal: (changes) => {
				session.sealPromise ??= this.seal(session, changes);
				return session.sealPromise;
			},
			validate: () => validateTransferredProcessEvidence(session.topLevelEvidence, session.incompleteReasons),
			close,
		};
	}

	async dispose(): Promise<void> {
		this.disposed = true;
		await Promise.allSettled([...this.inFlight.values()]);
		this.certificateScopes.clear();
	}

	private async resolveReady(): Promise<ReadyBackend> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		this.ready ??= this.probe();
		return this.ready;
	}

	private resolvePlatformFingerprint(): Promise<Sha256Digest> {
		this.platformFingerprint ??= execText("uname", ["-srm"]).then((kernel) =>
			digestObject({ kernel: kernel.trim(), arch: process.arch }),
		);
		return this.platformFingerprint;
	}

	private async probe(): Promise<ReadyBackend> {
		if (process.platform !== "linux") throw new Error("Linux host required");
		await mkdir(this.options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(this.options.storeRoot, 0o700);
		const [sandlock, strace, unshare, mount] = await Promise.all([
			resolveBinary(this.options.sandlockBinary, "sandlock", [path.join(os.homedir(), ".local", "bin", "sandlock")]),
			resolveBinary(this.options.straceBinary, "strace"),
			resolveBinary(this.options.unshareBinary, "unshare"),
			resolveBinary(undefined, "mount"),
		]);
		const [sandlockCheck, sandlockVersion, straceVersion, mountVersion, platformFingerprint] = await Promise.all([
			execText(sandlock, ["check"]),
			execText(sandlock, ["--version"]),
			execText(strace, ["-V"]),
			execText(mount, ["--version"]),
			this.resolvePlatformFingerprint(),
		]);
		if (!sandlockCheck.includes("Status:         OK")) throw new Error("Sandlock kernel protections are unavailable");
		await execText(unshare, namespaceArguments(["true"]));
		const mountProbe = await mkdtemp(path.join(os.tmpdir(), "pi-process-mount-probe-"));
		try {
			const source = path.join(mountProbe, "source");
			const target = path.join(mountProbe, "target");
			await Promise.all([mkdir(source), mkdir(target)]);
			await execText(unshare, namespaceArguments([mount, "--bind", source, target]));
		} finally {
			await rm(mountProbe, { recursive: true, force: true });
		}
		const observerFingerprint = digestObject({ epoch: BACKEND_EPOCH });
		const fingerprint = digestObject({
			epoch: BACKEND_EPOCH,
			policy: POLICY_ID,
			sandlock: sandlockVersion.trim(),
			strace: straceVersion.split(/\r?\n/)[0]?.trim(),
			mount: mountVersion.split(/\r?\n/)[0]?.trim(),
			platformFingerprint,
			arch: process.arch,
			deniedPaths: sensitivePaths(this.options.storeRoot, this.options.deniedPaths),
		});
		return { sandlock, strace, unshare, mount, fingerprint, platformFingerprint, observerFingerprint };
	}

	private async executeTopLevel(session: ActiveSession, request: ProcessExecutionRequest): Promise<{ exitCode: number | null }> {
		const requestStarted = performance.now();
		if (session.closed) throw new Error("Linux process session is closed");
		const ready = await this.resolveReady();
		assertInvocationMatches(session.invocation, request);
		const invocationCwd = path.resolve(request.cwd);
		if (!pathContains(session.sourceRoot, invocationCwd)) throw new Error("process cwd escapes source workspace");
		const physicalCwd = session.projection.toPhysical(invocationCwd);
		if (!physicalCwd || !pathContains(session.workspace.sandboxRoot, physicalCwd)) throw new Error("process cwd is unmapped");
		const environment = normalizeEnvironment(request.environment);
		const command = request.command;
		const prototype = await this.topLevelPrototype(session, request, ready, environment);
		this.add(session, "wholeCommandRequests");
		const initial = await this.plan(session, prototype);
		if (initial) return this.replayTopLevel(session, initial, request, requestStarted);
		this.add(session, "wholeCommandMisses");
		const sandbox = sandboxArguments({
			ready,
			workspaceRoot: session.workspace.sandboxRoot,
			privateRoot: session.workspace.processRoot,
			cwd: physicalCwd,
			deniedPaths: session.deniedPaths,
			command: [session.invocation.shell, ...shellArguments(session.invocation, command)],
			...(request.timeout !== undefined ? { timeoutSeconds: request.timeout } : {}),
		});
		const before = await session.workspace.structure.capture();
		const traceRoot = await mkdtemp(path.join(session.workspace.processRoot, "top-trace-"));
		const tracePrefix = path.join(traceRoot, "process");
		const traced = straceCommand(ready.strace, tracePrefix, sandbox);
		let outcome: SpawnOutcome;
		const processStarted = performance.now();
		try {
			outcome = await runSpawn(
				ready.unshare,
				namespaceArguments([
					process.execPath,
					session.interposition.namespaceLauncher,
					session.interposition.planPath,
					...traced,
				]),
				{
					cwd: physicalCwd,
					environment,
					...(session.invocation.commandTransport === "stdin" ? { stdin: Buffer.from(command, "utf8") } : {}),
					...(request.signal ? { signal: request.signal } : {}),
					...(request.timeout !== undefined ? { timeoutSeconds: request.timeout } : {}),
					onOutput: (event) => request.onData(event.data),
				},
			);
			session.topLevelExecution = {
				prototype,
				outcome,
				observedProcessMs: Math.max(0, performance.now() - processStarted),
			};
			try {
				const after = await session.workspace.structure.capture();
				const observation = await observeStrace(tracePrefix, session.invocation.shell, physicalCwd, {
					ignoredExecutablePaths: session.interposition.executablePaths,
					guardFilesystemSemanticsWithin: [session.workspace.sandboxRoot, session.sourceRoot],
				});
				session.topLevelCapture = { before, after, observation };
				for (const reason of observation.incompleteReasons) session.incompleteReasons.add(`top_trace:${reason}`);
			} catch (error) {
				session.incompleteReasons.add(`top_capture:${errorMessage(error)}`);
				session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
			}
		} finally {
			await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
		}
		if (request.signal?.aborted) throw new Error("aborted");
		return { exitCode: outcome.signal ? null : outcome.code };
	}

	private async seal(
		session: ActiveSession,
		changes: readonly SandboxWorkspaceChange[],
	): Promise<readonly SandboxDirectoryChange[]> {
		const directories = await sealSessionEvidence(session, changes);
		try {
			await this.publishTopLevel(session, changes);
		} catch (error) {
			this.setError(session, `top_publish:${errorMessage(error)}`);
		}
		return directories;
	}

	private async topLevelPrototype(
		session: ActiveSession,
		request: ProcessExecutionRequest,
		ready: ReadyBackend,
		environment: Readonly<Record<string, string>>,
	): Promise<ExecPrototype> {
		return topLevelProcessPrototype(session.invocation, request, environment, session.projection, ready.platformFingerprint);
	}

	private async replayTopLevel(
		session: ActiveSession,
		plan: Extract<ProcessReusePlan, { kind: "completed_replay" }>,
		request: ProcessExecutionRequest,
		requestStarted: number,
	): Promise<{ exitCode: number | null }> {
		const replayStarted = performance.now();
		const before = await session.workspace.structure.capture();
		await replayFilesystemEffects(plan.artifacts, plan.certificate.result.journal, session.projection, session.workspace.sandboxRoot);
		const after = await session.workspace.structure.capture();
		session.nestedEvidence.push(plan.certificate.dependencyCertificate);
		session.topLevelCapture = {
			before,
			after,
			observation: { complete: true, paths: [], taints: [], tracedProcesses: 0, incompleteReasons: [] },
		};
		for (const event of loadOutputEvents(plan.artifacts, plan.certificate.result.journal)) request.onData(event.data);
		this.add(session, "wholeCommandReplayMs", Math.max(0, performance.now() - replayStarted));
		this.add(session, "wholeCommandAvoidedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
		this.add(session, "wholeCommandHitOverheadMs", Math.max(0, performance.now() - requestStarted));
		this.add(session, "wholeCommandHits");
		return { exitCode: plan.certificate.result.exit.kind === "code" ? plan.certificate.result.exit.code : null };
	}

	private async publishTopLevel(session: ActiveSession, changes: readonly SandboxWorkspaceChange[]): Promise<void> {
		const execution = session.topLevelExecution;
		const evidence = session.topLevelEvidence;
		if (!execution || !evidence) return;
		const journal: OrderedEffectEvent[] = [];
		let sequence = 0;
		for (const change of changes) {
			const logicalPath = slash(path.resolve(session.sourceRoot, change.resource));
			journal.push(await workspaceTransition(this.store, sequence++, logicalPath, change));
		}
		for (const event of execution.outcome.output) {
			journal.push({ sequence: sequence++, kind: "output", fd: event.fd, data: await this.store.artifacts.put(event.data) });
		}
		const certificate = sealProcessCertificate({
			prototype: execution.prototype,
			producer: session.producer,
			dependencyCertificate: evidence,
			result: {
				replayProfile: "buffered_noninteractive",
				observedProcessMs: execution.observedProcessMs,
				journal,
				exit: exitOutcome(execution.outcome),
			},
		});
		if (await this.planner.publishCompleted(certificate)) {
			this.add(session, "wholeCommandPublished");
			this.rememberScope(certificate.id, session.scope);
		}
	}

	private serve(session: ActiveSession, socket: net.Socket): void {
		let body = "";
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => {
			body += chunk;
			if (Buffer.byteLength(body) > MAX_REQUEST_BYTES) socket.destroy(new Error("request too large"));
		});
		socket.once("error", () => undefined);
		socket.once("end", () => {
			void this.handleWireRequest(session, body)
				.then((response) => socket.end(JSON.stringify(response)))
				.catch(async (error) => {
					this.setError(session, errorMessage(error));
					session.incompleteReasons.add(`broker:${errorMessage(error)}`);
					const request = parseDispatcherRequest(body);
					const executable = request ? await this.resolveRequestedExecutable(session, request).catch(() => undefined) : undefined;
					socket.end(JSON.stringify({ version: 1, kind: "bypass", ...(executable ? { executable } : {}) }));
				});
		});
	}

	private async handleWireRequest(session: ActiveSession, body: string): Promise<DispatcherResponse> {
		const requestStarted = performance.now();
		const request = parseDispatcherRequest(body);
		if (!request || request.token !== session.token || session.closed) throw new Error("invalid dispatcher request");
		this.add(session, "requests");
		const executable = await this.resolveRequestedExecutable(session, request);
		if (!eligibleRequest(session, request, executable)) {
			this.add(session, "bypasses");
			session.incompleteReasons.add(`broker_bypass:${request.name}`);
			return { version: 1, kind: "bypass", executable };
		}
		const prototype = await this.prototype(session, request, executable);
		const weakKey = processWeakKey(prototype);
		const initial = await this.plan(session, prototype);
		if (initial) return this.replay(session, initial, weakKey, false, requestStarted);

		const preceding = this.inFlight.get(weakKey);
		if (preceding) {
			await preceding;
			const joined = await this.plan(session, prototype);
			if (joined) return this.replay(session, joined, weakKey, true, requestStarted);
		}

		this.add(session, "misses");
		let release!: () => void;
		const pending = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.inFlight.set(weakKey, pending);
		try {
			return await this.executeAndPublish(session, request, executable, prototype, weakKey);
		} finally {
			release();
			if (this.inFlight.get(weakKey) === pending) this.inFlight.delete(weakKey);
		}
	}

	private async plan(session: ActiveSession, prototype: ExecPrototype) {
		const plan = await this.planner.plan({
			prototype,
			acceptProducer: (producer) => compatibleProducer(session.producer, producer),
			contract: {
				sink: "buffered",
				orderedJournal: true,
				transactionalEffects: true,
				mode: "completed_replay",
			},
			validation: { resolvePath: (logicalPath) => session.projection.toPhysical(logicalPath) },
		});
		this.recordLookup(plan.lookup, session);
		if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
			this.setError(session, `reuse_miss:${plan.reasons.join(",")}${
				plan.changedDependencies?.length ? `:${plan.changedDependencies.join(",")}` : ""
			}`);
		}
		return plan.kind === "completed_replay" ? plan : undefined;
	}

	private recordLookup(lookup: ProcessReusePlan["lookup"], session?: ActiveSession): void {
		const record = (metric: CountedReuseMetric, value: number) => {
			if (session) this.add(session, metric, value);
			else this.counters[metric] += value;
		};
		record("validationMs", lookup.durationMs);
		record("validationCandidates", lookup.candidateCertificates);
		record("validationPathsets", lookup.pathsetsValidated);
		record("validationFilesRead", lookup.filesRead);
		record("validationBytesRead", lookup.bytesRead);
		record("validationArtifactsLoaded", lookup.artifactsLoaded);
		record("validationArtifactBytesRead", lookup.artifactBytesRead);
	}

	private actorReplayMiss(host: ProcessExecutor, request: ProcessExecutionRequest): Promise<ProcessExecutionResult> {
		this.counters.wholeCommandMisses++;
		return host.execute(request);
	}

	private async replay(
		session: ActiveSession,
		plan: Extract<ProcessReusePlan, { kind: "completed_replay" }>,
		weakKey: Sha256Digest,
		joined: boolean,
		requestStarted: number,
	): Promise<DispatcherResponse> {
		const started = performance.now();
		let replayed = false;
		try {
			const { artifacts, certificate } = plan;
			const output = wireOutput(loadOutputEvents(artifacts, certificate.result.journal));
			await replayFilesystemEffects(artifacts, certificate.result.journal, session.projection, session.workspace.sandboxRoot);
			session.nestedEvidence.push(certificate.dependencyCertificate);
			this.add(session, "hits");
			if (joined) this.add(session, "joinedHits");
			const producer = this.certificateScopes.get(certificate.id);
			this.add(
				session,
				producer && session.scope
					? producer.sessionID === session.scope.sessionID && producer.turnID === session.scope.turnID
						? "sameTurnHits"
						: "crossTurnHits"
					: "unattributedHits",
			);
			replayed = true;
			return { version: 1, kind: "hit", weakKey, output, exit: certificate.result.exit };
		} finally {
			this.add(session, "replayMs", Math.max(0, performance.now() - started));
			const observed = plan.certificate.result.observedProcessMs;
			if (replayed && observed !== undefined) {
				this.add(session, "timedHits");
				this.add(session, "avoidedProcessMs", observed);
				this.add(session, "timedHitOverheadMs", Math.max(0, performance.now() - requestStarted));
			}
		}
	}

	private async executeAndPublish(
		session: ActiveSession,
		request: DispatcherRequest,
		executable: string,
		prototype: ExecPrototype,
		weakKey: Sha256Digest,
	): Promise<DispatcherResponse> {
		const ready = await this.resolveReady();
		const started = performance.now();
		const transaction = await session.workspace.transactions.begin();
		const traceRoot = await mkdtemp(path.join(session.workspace.processRoot, "trace-"));
		const tracePrefix = path.join(traceRoot, "process");
		let outcome: SpawnOutcome | undefined;
		let observedProcessMs: number | undefined;
		let transactionFinishing = false;
		try {
			const command = straceCommand(ready.strace, tracePrefix, [
				ready.sandlock,
				...sandboxPolicyArguments(
					session.workspace.sandboxRoot,
					session.workspace.processRoot,
					request.cwd,
					session.deniedPaths,
				),
				"--",
				executable,
				...request.args,
			]);
			const processStarted = performance.now();
			outcome = await runSpawn(ready.unshare, namespaceArguments(command), {
				cwd: request.cwd,
				environment: executionEnvironment(request.environment, executable),
			});
			observedProcessMs = Math.max(0, performance.now() - processStarted);
			try {
				transactionFinishing = true;
				const [delta, observation] = await Promise.all([
					transaction.finish(),
					observeStrace(tracePrefix, executable, request.cwd, {
						guardFilesystemSemanticsWithin: [session.workspace.sandboxRoot, session.sourceRoot],
					}),
				]);
				if (observation.incompleteReasons.length) {
					this.setError(session, `trace:${observation.incompleteReasons.join(",")}`);
					for (const reason of observation.incompleteReasons) session.incompleteReasons.add(`nested_trace:${reason}`);
				}
				if (!delta.complete) {
					this.setError(session, `transaction:${delta.reason}`);
					throw new Error(`workspace transaction is incomplete: ${delta.reason}`);
				}
				const { before, after } = delta;
				const effects = diffWorkspaceStructures(before, after, delta.changes, session.projection);
				const evidence = await captureDependencies(
					session,
					transactionDependencySource(before, delta.changes),
					observation.paths,
					effects.effects,
				);
				if (evidence.incompleteReasons.length) {
					this.setError(session, `evidence:${evidence.incompleteReasons.join(",")}`);
				}
				const taints = new Set<ProvenanceTaint>(observation.taints);
				for (const taint of evidence.taints) taints.add(taint);
				if (!effects.complete) taints.add("unsupported_syscall");
				if (!before.complete || !after.complete || !observation.complete) {
					taints.add("trace_incomplete");
				}
				const journal: OrderedEffectEvent[] = [];
				let sequence = 0;
				const changes = new Map(delta.changes.map((change) => [path.normalize(change.relativePath), change]));
				for (const effect of effects.effects) {
					switch (effect.kind) {
						case "delete":
						case "write": {
							const change = changes.get(path.normalize(effect.relativePath));
							if (!change) throw new Error(`transaction change is unavailable: ${effect.relativePath}`);
							journal.push(await workspaceTransition(this.store, sequence++, effect.logicalPath, change));
							break;
						}
						case "rmdir":
							journal.push(await workspaceTransition(this.store, sequence++, effect.logicalPath, { kind: "directory", before: effect.before }));
							break;
						case "mkdir":
							journal.push(await workspaceTransition(this.store, sequence++, effect.logicalPath, { kind: "directory", after: effect.after }));
							break;
					}
				}
				for (const event of outcome.output) {
					const data = await this.store.artifacts.put(event.data);
					journal.push({ sequence: sequence++, kind: "output", fd: event.fd, data });
				}
				const dependencyCertificate: DynamicDependencyCertificate = {
					complete:
						before.complete &&
						after.complete &&
						observation.complete &&
						effects.complete &&
						evidence.complete,
					dependencies: evidence.dependencies,
					taints: [...taints],
				};
				const exit = exitOutcome(outcome);
				const certificate = sealProcessCertificate({
					prototype,
					producer: session.producer,
					dependencyCertificate,
					result: { replayProfile: "buffered_noninteractive", observedProcessMs, journal, exit },
				});
				session.nestedEvidence.push(certificate.dependencyCertificate);
				if (await this.planner.publishCompleted(certificate)) {
					this.add(session, "published");
					this.rememberScope(certificate.id, session.scope);
				}
				if (taints.size) this.add(session, "tainted");
			} catch (error) {
				// The process already ran. Certificate failure must never cause dispatcher fallback/re-execution.
				this.setError(session, `post_execution_capture:${errorMessage(error)}`);
				session.incompleteReasons.add(`nested_capture:${errorMessage(error)}`);
			}
			const exit = exitOutcome(outcome);
			return { version: 1, kind: "executed", weakKey, output: wireOutput(outcome.output), exit };
		} finally {
			this.add(session, "executionMs", Math.max(0, performance.now() - started));
			if (!transactionFinishing) await transaction.abort().catch(() => undefined);
			await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
		}
	}

	private add(session: ActiveSession, metric: CountedReuseMetric, value = 1): void {
		this.counters[metric] += value;
		session.metrics[metric] += value;
	}

	private setError(session: ActiveSession, detail: string): void {
		this.counters.lastError = detail;
		session.metrics.lastError = detail;
	}

	private rememberScope(id: Sha256Digest, scope: ExecutionScope | undefined): void {
		if (!scope) return;
		this.certificateScopes.delete(id);
		this.certificateScopes.set(id, scope);
		while (this.certificateScopes.size > this.store.limits.maxCertificates) {
			this.certificateScopes.delete(this.certificateScopes.keys().next().value!);
		}
	}

	private async resolveRequestedExecutable(session: ActiveSession, request: DispatcherRequest): Promise<string> {
		if (!request.name || request.name.includes("/") || request.name.includes("\0")) throw new Error("invalid executable name");
		if (path.isAbsolute(request.invokedPath) && path.basename(request.invokedPath) === request.name) {
			const invoked = path.resolve(request.invokedPath);
			const covered = session.interposition.directories.find(
				(directory) => path.resolve(path.dirname(invoked)) === path.resolve(directory.target),
			);
			if (covered) {
				const resolved = await realpath(path.join(covered.source, request.name));
				const stat = await lstat(resolved);
				if (!stat.isFile()) throw new Error("invoked executable is not a regular file");
				await access(resolved, fsConstants.X_OK);
				return resolved;
			}
		}
		const pathValue = request.environment.PATH ?? session.originalPath;
		const directories = pathValue.split(path.delimiter).filter(Boolean);
		for (const directory of directories) {
			const candidate = path.resolve(request.cwd, directory, request.name);
			try {
				const stat = await lstat(candidate);
				if (!stat.isFile()) continue;
				await access(candidate, fsConstants.X_OK);
				return await realpath(candidate);
			} catch (error) {
				if (!missing(error) && !permissionDenied(error)) throw error;
			}
		}
		throw new Error(`executable not found: ${request.name}`);
	}

	private async prototype(session: ActiveSession, request: DispatcherRequest, executable: string): Promise<ExecPrototype> {
		const [content, ready] = await Promise.all([readFile(executable), this.resolveReady()]);
		const environment = Object.fromEntries(
			Object.entries(executionEnvironment(request.environment, executable)).map(([name, value]) => [
				name,
				session.projection.normalizeValue(value),
			]),
		);
		const argv = [request.name, ...request.args].map((value) => session.projection.normalizeValue(value));
		return createExecPrototype({
			executablePath: session.projection.toLogical(executable),
			executableDigest: sha256Digest(content),
			argv,
			logicalCwd: session.projection.toLogical(request.cwd),
			environment,
			umask: request.umask,
			rlimitsDigest: digestObject({ nofile: "broker-controlled", processes: 64 }),
			signalDispositionsDigest: digestObject({ spawn: "node-default-v1" }),
			credentialsDigest: digestObject({ userNamespace: "preserve-current-user", uid: process.getuid?.(), gid: process.getgid?.() }),
			schedulingDigest: digestObject({ scheduler: "inherited", cpuCount: os.availableParallelism() }),
			stdin: { type: "closed", eof: true },
			fileDescriptorTableComplete: true,
			inheritedFDs: [
				{ fd: 0, type: "device", flagsDigest: digestObject({ mode: "read", endpoint: "/dev/null" }), eof: true },
				{ fd: 1, type: "pipe", flagsDigest: digestObject({ mode: "write", sink: "buffered" }) },
				{ fd: 2, type: "pipe", flagsDigest: digestObject({ mode: "write", sink: "buffered" }) },
			],
			platformFingerprint: ready.platformFingerprint,
		});
	}
}

async function sealSessionEvidence(
	session: ActiveSession,
	changes: readonly SandboxWorkspaceChange[],
): Promise<readonly SandboxDirectoryChange[]> {
	const capture = session.topLevelCapture;
	if (!capture) {
		session.incompleteReasons.add("top_capture_missing");
		session.topLevelEvidence ??= { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error("top-level workspace capture is missing");
	}
	const fileChanges = changes.filter((change): change is SandboxFileChange => change.kind !== "directory");
	const regularDeltas: WorkspaceRegularDelta[] = fileChanges.map((change) => ({
		relativePath: change.resource,
		...(change.before ? { before: change.before } : {}),
		...(change.after ? { after: change.after } : {}),
		...(change.beforeMode !== undefined ? { beforeMode: change.beforeMode } : {}),
		...(change.afterMode !== undefined ? { afterMode: change.afterMode } : {}),
	}));
	const effects = diffWorkspaceStructures(
		capture.before,
		capture.after,
		regularDeltas,
		session.projection,
	);
	if (!effects.complete) {
		session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error(`top-level workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	}
	const directoryChanges = await sourceDirectoryChanges(session, effects.effects);
	try {
		const evidence = await captureDependencies(
			session,
			transactionDependencySource(capture.before, regularDeltas),
			capture.observation.paths,
			effects.effects,
		);
		for (const reason of evidence.incompleteReasons) session.incompleteReasons.add(`top_evidence:${reason}`);
		if (!effects.complete) session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		session.topLevelEvidence = mergeDependencyEvidence(
			[
				{
					complete:
						capture.before.complete &&
						capture.after.complete &&
						capture.observation.complete &&
						effects.complete &&
						evidence.complete,
					dependencies: evidence.dependencies,
					taints: [...new Set([...capture.observation.taints, ...evidence.taints])],
				},
				{ complete: true, dependencies: session.interposition.dependencies, taints: [] },
				...session.nestedEvidence,
			],
			session.incompleteReasons,
			new Set(effects.effects.map((effect) => path.posix.dirname(effect.logicalPath.replaceAll("\\", "/")))),
		);
	} catch (error) {
		session.incompleteReasons.add(`top_seal:${errorMessage(error)}`);
		session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
	}
	return directoryChanges;
}

async function sourceDirectoryChanges(
	session: ActiveSession,
	effects: ReturnType<typeof diffWorkspaceStructures>["effects"],
): Promise<readonly SandboxDirectoryChange[]> {
	const changes: SandboxDirectoryChange[] = [];
	for (const effect of effects) {
		if (effect.kind !== "mkdir" && effect.kind !== "rmdir") continue;
		const resource = slash(path.normalize(effect.relativePath));
		const target = path.resolve(session.sourceRoot, resource);
		if (!pathContains(session.sourceRoot, target) || target === path.resolve(session.sourceRoot)) {
			throw new Error(`directory effect escapes source workspace: ${effect.relativePath}`);
		}
		const sourceBefore = await readSandboxDirectoryState(target);
		if (effect.kind === "mkdir") {
			if (sourceBefore !== undefined) throw new Error(`directory creation baseline changed: ${resource}`);
			changes.push({
				kind: "directory",
				root: session.sourceRoot,
				target,
				resource,
				after: {
					entriesDigest: effect.after.entriesDigest,
					mode: effect.after.mode,
					uid: effect.after.uid,
					gid: effect.after.gid,
				},
			});
			continue;
		}
		const sandboxBefore = {
			entriesDigest: effect.before.entriesDigest,
			mode: effect.before.mode,
			uid: effect.before.uid,
			gid: effect.before.gid,
		};
		if (!sameDirectoryStateValue(sourceBefore, sandboxBefore)) {
			throw new Error(`source directory differs from execution baseline: ${resource}`);
		}
		changes.push({
			kind: "directory",
			root: session.sourceRoot,
			target,
			resource,
			before: sandboxBefore,
		});
	}
	return Object.freeze(changes);
}

function sameDirectoryStateValue(
	left: Awaited<ReturnType<typeof readSandboxDirectoryState>>,
	right: NonNullable<Awaited<ReturnType<typeof readSandboxDirectoryState>>>,
): boolean {
	return (
		left !== undefined &&
		left.entriesDigest === right.entriesDigest &&
		left.mode === right.mode &&
		left.uid === right.uid &&
		left.gid === right.gid
	);
}

function transactionDependencySource(
	snapshot: WorkspaceStructureSnapshot,
	changes: readonly WorkspaceRegularDelta[],
): WorkspaceDependencySource {
	const deltas = new Map<string, WorkspaceRegularDelta>();
	for (const change of changes) {
		const relative = path.normalize(change.relativePath);
		if (deltas.has(relative)) throw new Error(`duplicate transaction delta: ${change.relativePath}`);
		deltas.set(relative, change);
	}
	const cached = new Map<string, Promise<WorkspaceTreeEntry | undefined>>();
	const entry = (physicalPath: string): Promise<WorkspaceTreeEntry | undefined> => {
		const relative = path.relative(snapshot.root, path.resolve(physicalPath));
		if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
			return Promise.reject(new Error(`workspace dependency escapes snapshot: ${physicalPath}`));
		}
		const existing = cached.get(relative);
		if (existing) return existing;
		const pending = (async () => {
			const structure = snapshot.entries.get(relative);
			if (!structure || structure.kind !== "file") return structure;
			const delta = deltas.get(relative);
			if (delta && delta.before === undefined) {
				throw new Error(`transaction baseline is missing file bytes: ${delta.relativePath}`);
			}
			const bytes = delta?.before ?? (await readFile(path.resolve(snapshot.root, relative)));
			const hydrated = hydrateWorkspaceFileEntry(structure, bytes);
			if (!hydrated) throw new Error(`transaction baseline size changed: ${relative}`);
			return hydrated;
		})();
		cached.set(relative, pending);
		return pending;
	};
	return {
		entry,
		parentEntry: (physicalPath) =>
			path.resolve(physicalPath) === path.resolve(snapshot.root)
				? Promise.resolve(undefined)
				: entry(path.dirname(physicalPath)),
	};
}

async function captureDependencies(
	session: ActiveSession,
	before: WorkspaceDependencySource,
	observed: readonly { readonly path: string; readonly role: "input" | "executable" | "shared_object" | "metadata" }[],
	effects: readonly { readonly logicalPath: string; readonly relativePath: string; readonly before?: unknown }[],
): Promise<{
	readonly complete: boolean;
	readonly dependencies: readonly DynamicDependency[];
	readonly taints: readonly ProvenanceTaint[];
	readonly incompleteReasons: readonly string[];
}> {
	const dependencies = new Map<string, DynamicDependency>();
	const taints = new Set<ProvenanceTaint>();
	const incompleteReasons = new Set<string>();
	let complete = true;
	const add = (dependency: DynamicDependency | undefined, reason = "dependency_unavailable") => {
		if (!dependency) {
			complete = false;
			incompleteReasons.add(reason);
			return;
		}
		const identity = dependency.kind === "fd" ? `fd:${dependency.fd}` : `${dependency.kind}:${dependency.path}`;
		const existing = dependencies.get(identity);
		if (
			existing?.kind === "file" &&
			dependency.kind === "file" &&
			(existing.role === "executable" || dependency.role !== "executable")
		) {
			return;
		}
		dependencies.set(identity, dependency);
	};

	for (const item of observed) {
		const observedPath = path.resolve(item.path);
		if (STABLE_SANDBOX_DEVICES.has(observedPath) || interposedDirectoryFor(session, observedPath)) continue;
		const physical = pathContains(session.sourceRoot, observedPath)
			? (session.projection.toPhysical(observedPath) ?? observedPath)
			: observedPath;
		if (session.projection.isWorkspacePhysical(physical)) {
			const logical = session.projection.toLogical(physical);
			const [entry, parentEntry] = await Promise.all([before.entry(physical), before.parentEntry(physical)]);
			add(
				snapshotDependency(
					logical,
					entry,
					parentEntry,
					item.role,
					{
						excludedEntries: workspaceMetadataExclusions(session, physical),
						parentExcludedEntries: workspaceMetadataExclusions(session, path.dirname(physical)),
					},
				),
			);
			continue;
		}
		if (!(await immutableHostPath(physical))) {
			taints.add("mutable_input");
			incompleteReasons.add(`mutable:${physical}`);
		}
		try {
			for (const dependency of await captureHostPath(physical, item.role)) add(dependency);
		} catch (error) {
			complete = false;
			taints.add("trace_incomplete");
			incompleteReasons.add(`capture:${physical}:${errorMessage(error)}`);
		}
	}
	for (const effect of effects) {
		const physical = session.projection.toPhysical(effect.logicalPath);
		if (!physical) {
			complete = false;
			incompleteReasons.add(`effect_unmapped:${effect.logicalPath}`);
			continue;
		}
		add(
			snapshotDependency(
				effect.logicalPath,
				await before.entry(physical),
				await before.parentEntry(physical),
				"input",
				{
					excludedEntries: workspaceMetadataExclusions(session, physical),
					parentExcludedEntries: workspaceMetadataExclusions(session, path.dirname(physical)),
				},
			),
		);
	}
	return {
		complete,
		dependencies: [...dependencies.values()],
		taints: [...taints],
		incompleteReasons: [...incompleteReasons],
	};
}

const STABLE_SANDBOX_DEVICES = new Set(["/dev/null", "/dev/tty", "/dev/zero", "/dev/full"]);

function interposedDirectoryFor(session: ActiveSession, target: string): InterposedDirectory | undefined {
	return session.interposition.directories.find(
		(directory) => path.resolve(path.dirname(target)) === path.resolve(directory.target),
	);
}

function workspaceMetadataExclusions(session: ActiveSession, target: string): readonly string[] | undefined {
	return path.resolve(target) === path.resolve(session.workspace.sandboxRoot)
		? session.workspace.observationExcludes
		: undefined;
}

async function captureHostPath(
	physicalPath: string,
	role: "input" | "executable" | "shared_object" | "metadata",
): Promise<readonly DynamicDependency[]> {
	const dependencies: DynamicDependency[] = [];
	const normalized = path.resolve(physicalPath);
	let current = path.parse(normalized).root;
	for (const component of normalized.slice(current.length).split(path.sep).filter(Boolean)) {
		current = path.join(current, component);
		try {
			const stat = await lstat(current);
			if (stat.isSymbolicLink()) dependencies.push(await captureSymlinkDependency(current, slash(current)));
		} catch (error) {
			if (!missing(error)) throw error;
			break;
		}
	}
	let target = normalized;
	try {
		const stat = await lstat(target);
		if (stat.isSymbolicLink()) target = await realpath(target);
		const targetStat = await lstat(target);
		if (targetStat.isFile()) {
			dependencies.push((await captureFileDependency(target, slash(target), role, { includeMetadata: true })).dependency);
		} else if (targetStat.isDirectory()) {
			dependencies.push(await captureDirectoryDependency(target, slash(target), true));
		} else {
			throw new Error("unsupported host dependency");
		}
	} catch (error) {
		if (!missing(error)) throw error;
		const missingPath = await nearestMissingPath(target);
		const absence = await captureAbsenceDependency(missingPath, slash(missingPath), true);
		if (absence) dependencies.push(absence);
	}
	return dependencies;
}

async function immutableHostPath(target: string): Promise<boolean> {
	const normalized = path.resolve(target);
	if (["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp", "/home"].some((root) => pathContains(root, normalized))) {
		return false;
	}
	try {
		const stat = await lstat(normalized);
		if (stat.isSymbolicLink()) {
			const parent = await lstat(path.dirname(normalized));
			if (stat.uid !== 0 || parent.uid !== 0 || (parent.mode & 0o022) !== 0) return false;
			return immutableHostPath(await realpath(normalized));
		}
		return stat.uid === 0 && (stat.mode & 0o022) === 0;
	} catch (error) {
		if (!missing(error)) return false;
		try {
			const missingPath = await nearestMissingPath(normalized);
			const parent = await lstat(path.dirname(missingPath));
			return parent.uid === 0 && (parent.mode & 0o022) === 0;
		} catch {
			return false;
		}
	}
}

/** The first absent component plus its existing parent form a stable negative dependency. */
async function nearestMissingPath(target: string): Promise<string> {
	const missingComponents: string[] = [];
	let current = path.resolve(target);
	while (true) {
		try {
			await lstat(current);
			return missingComponents.length ? path.join(current, missingComponents.at(-1)!) : target;
		} catch (error) {
			if (!missing(error)) throw error;
			const parent = path.dirname(current);
			if (parent === current) throw error;
			missingComponents.push(path.basename(current));
			current = parent;
		}
	}
}

async function replayFilesystemEffects(
	artifacts: VerifiedArtifactClosure,
	journal: readonly OrderedEffectEvent[],
	projection: ExecutionPathProjection,
	workspaceRoot: string,
): Promise<void> {
	const changes: SandboxWorkspaceChange[] = [];
	for (const event of journal) {
		if (event.kind === "output") continue;
		const target = projection.toPhysical(event.path);
		if (!target || !pathContains(workspaceRoot, target) || target === path.resolve(workspaceRoot)) {
			throw new Error(`replay effect escapes workspace: ${event.path}`);
		}
		const resource = slash(path.relative(workspaceRoot, target));
		if (event.before.kind === "directory" || event.after.kind === "directory") {
			if (event.before.kind !== "absent" && event.before.kind !== "directory") throw new Error(`unsupported replay type change: ${event.path}`);
			if (event.after.kind !== "absent" && event.after.kind !== "directory") throw new Error(`unsupported replay type change: ${event.path}`);
			changes.push({
				kind: "directory",
				root: workspaceRoot,
				target,
				resource,
				...(event.before.kind === "directory" ? { before: directoryState(event.before) } : {}),
				...(event.after.kind === "directory" ? { after: directoryState(event.after) } : {}),
			});
			continue;
		}
		changes.push({
			root: workspaceRoot,
			target,
			resource,
			...(event.before.kind === "file" ? { before: artifacts.read(event.before.data), beforeMode: event.before.mode } : {}),
			...(event.after.kind === "file" ? { after: artifacts.read(event.after.data), afterMode: event.after.mode } : {}),
		});
	}
	if (!changes.length) return;
	await commitSandboxDelta({
		output: { result: { content: [], details: {} }, isError: false },
		changes,
	});
}

async function workspaceTransition(
	store: ProvenanceCertificateStore,
	sequence: number,
	logicalPath: string,
	change:
		| Pick<SandboxFileChange, "kind" | "before" | "after" | "beforeMode" | "afterMode">
		| Pick<SandboxDirectoryChange, "kind" | "before" | "after">,
): Promise<Extract<OrderedEffectEvent, { kind: "workspace" }>> {
	let before: WorkspaceEffectState;
	let after: WorkspaceEffectState;
	if (change.kind === "directory") {
		before = change.before ? { kind: "directory", ...change.before } : { kind: "absent" };
		after = change.after ? { kind: "directory", ...change.after } : { kind: "absent" };
	} else {
		before = await regularEffectState(store, change.before, change.beforeMode);
		after = await regularEffectState(store, change.after, change.afterMode);
	}
	return { sequence, kind: "workspace", path: logicalPath, before, after };
}

async function regularEffectState(
	store: ProvenanceCertificateStore,
	content: Uint8Array | undefined,
	mode: number | undefined,
): Promise<WorkspaceEffectState> {
	if (content === undefined) return { kind: "absent" };
	if (mode === undefined) throw new Error("transaction file mode is unavailable");
	return { kind: "file", data: await store.artifacts.put(content), mode };
}

function directoryState(state: Extract<WorkspaceEffectState, { kind: "directory" }>): SandboxDirectoryChange["before"] {
	return { entriesDigest: state.entriesDigest, mode: state.mode, uid: state.uid, gid: state.gid };
}

function loadOutputEvents(
	artifacts: VerifiedArtifactClosure,
	journal: readonly OrderedEffectEvent[],
): readonly BufferedOutput[] {
	const output: BufferedOutput[] = [];
	for (const event of journal) {
		if (event.kind !== "output") continue;
		const data = artifacts.read(event.data);
		output.push({ fd: event.fd, data });
	}
	return output;
}

function wireOutput(output: readonly BufferedOutput[]): readonly { readonly fd: 1 | 2; readonly data: string }[] {
	return output.map((event) => ({ fd: event.fd, data: event.data.toString("base64") }));
}

async function createProcessInterposition(input: {
	readonly privateRoot: string;
	readonly pathValue: string;
	readonly projection: ExecutionPathProjection;
	readonly sourceRoot: string;
	readonly workspaceRoot: string;
	readonly workspaceExcludes: readonly string[];
	readonly token: string;
	readonly socketPath: string;
	readonly mountBinary: string;
	readonly excludedExecutables: readonly string[];
}): Promise<ProcessInterposition> {
	if (process.execPath.includes("\n") || process.execPath.includes("\r") || process.execPath.includes(" ")) {
		throw new Error("Node executable path cannot be represented by dispatcher shebang");
	}
	const root = path.join(input.privateRoot, "process-interposition");
	const viewRoot = path.join(root, "views");
	const shadowRoot = path.join(root, "originals");
	await Promise.all([mkdir(viewRoot, { recursive: true }), mkdir(shadowRoot, { recursive: true })]);
	const launcher = path.join(root, "process-dispatcher.mjs");
	const dispatcher = new URL("./process-dispatcher.mjs", import.meta.url).href;
	const namespaceLauncher = path.join(root, "process-namespace-launcher.mjs");
	const namespaceModule = new URL("./process-namespace-launcher.mjs", import.meta.url).href;
	const directories: InterposedDirectory[] = [];
	const seenSources = new Set<string>();
	for (const rawDirectory of input.pathValue.split(path.delimiter)) {
		if (!rawDirectory || !path.isAbsolute(rawDirectory)) continue;
		const logicalDirectory = path.resolve(rawDirectory);
		const projected = input.projection.toPhysical(logicalDirectory) ?? logicalDirectory;
		let source: string;
		try {
			source = await realpath(projected);
			if (!(await lstat(source)).isDirectory()) continue;
		} catch {
			continue;
		}
		if (seenSources.has(source)) continue;
		seenSources.add(source);
		let target = logicalDirectory;
		if (!input.projection.isWorkspacePhysical(source)) {
			target = await realpath(logicalDirectory).catch(() => logicalDirectory);
		}
		const index = directories.length.toString().padStart(3, "0");
		directories.push({ source, target, shadow: path.join(shadowRoot, index), view: path.join(viewRoot, index) });
	}
	const configuration = {
		version: 1,
		socketPath: input.socketPath,
		token: input.token,
		directories: directories.map(({ target, shadow }) => ({ target, shadow })),
	};
	await writeFile(
		launcher,
		`#!${process.execPath}\nglobalThis.__PI_SPEC_PROCESS_DISPATCHER__=${JSON.stringify(configuration)};\nawait import(${JSON.stringify(dispatcher)});\n`,
		{ mode: 0o755 },
	);
	await chmod(launcher, 0o755);
	await writeFile(namespaceLauncher, `#!${process.execPath}\nawait import(${JSON.stringify(namespaceModule)});\n`, { mode: 0o755 });
	await chmod(namespaceLauncher, 0o755);

	const excluded = new Set<string>();
	for (const candidate of input.excludedExecutables) {
		try {
			excluded.add(await realpath(candidate));
		} catch {
			// A missing exclusion cannot be executed.
		}
	}
	const executablePaths: string[] = [];
	const dependencies: DynamicDependency[] = [];
	for (const directory of directories) {
		await Promise.all([mkdir(directory.shadow, { recursive: true }), mkdir(directory.view, { recursive: true })]);
		let entries: string[];
		try {
			entries = await readdir(directory.source);
		} catch {
			continue;
		}
		for (const name of entries) {
			if (!name || name.includes("/") || name.includes("\0")) continue;
			const sourceEntry = path.join(directory.source, name);
			const viewEntry = path.join(directory.view, name);
			try {
				const linkStat = await lstat(sourceEntry);
				const resolved = await realpath(sourceEntry);
				const resolvedStat = await lstat(resolved);
				let executable = resolvedStat.isFile() && !excluded.has(resolved);
				if (executable) {
					try {
						await access(sourceEntry, fsConstants.X_OK);
					} catch {
						executable = false;
					}
				}
				if (executable) {
					if (linkStat.isSymbolicLink()) await symlink(launcher, viewEntry);
					else await link(launcher, viewEntry);
					executablePaths.push(path.join(directory.target, name));
				} else {
					await symlink(path.join(directory.shadow, name), viewEntry);
				}
			} catch {
				// Preserve an unreadable entry through the shadow mount instead of guessing its type.
				await symlink(path.join(directory.shadow, name), viewEntry).catch(() => undefined);
			}
		}
		dependencies.push(
			await captureDirectoryDependency(
				directory.source,
				slash(directory.target),
				true,
				path.resolve(directory.source) === path.resolve(input.workspaceRoot) ? input.workspaceExcludes : [],
			),
		);
	}
	const planPath = path.join(root, "mount-plan.json");
	await writeFile(
		planPath,
		JSON.stringify({
			version: 1,
			mountBinary: input.mountBinary,
			workspace: { source: input.workspaceRoot, target: input.sourceRoot },
			directories,
		}),
		{ mode: 0o600 },
	);
	return {
		launcher,
		namespaceLauncher,
		planPath,
		directories: Object.freeze(directories),
		executablePaths: Object.freeze(executablePaths),
		dependencies: Object.freeze(dependencies),
	};
}

function sandboxArguments(input: {
	readonly ready: ReadyBackend;
	readonly workspaceRoot: string;
	readonly privateRoot: string;
	readonly cwd: string;
	readonly deniedPaths: readonly string[];
	readonly command: readonly string[];
	readonly timeoutSeconds?: number;
}): readonly string[] {
	return [
		input.ready.sandlock,
		...sandboxPolicyArguments(input.workspaceRoot, input.privateRoot, input.cwd, input.deniedPaths),
		...(input.timeoutSeconds !== undefined ? ["--timeout", String(Math.max(1, Math.ceil(input.timeoutSeconds)))] : []),
		"--",
		...input.command,
	];
}

function sandboxPolicyArguments(
	workspaceRoot: string,
	privateRoot: string,
	cwd: string,
	deniedPaths: readonly string[],
): readonly string[] {
	return [
		"run",
		"--fs-read",
		"/",
		"--fs-write",
		workspaceRoot,
		"--fs-write",
		privateRoot,
		...deniedPaths.flatMap((target) => ["--fs-deny", target]),
		"--time-start",
		new Date().toISOString(),
		"--no-huge-pages",
		"--no-coredump",
		"--max-processes",
		"64",
		"--cwd",
		cwd,
	];
}

function speculativeProducerProof(
	ready: ReadyBackend,
	deniedPaths: readonly string[],
): ProcessProducerProof {
	return Object.freeze({
		observer: { provider: "strace", fingerprint: ready.observerFingerprint },
		execution: {
			authority: "speculative",
			confinement: {
				provider: "sandlock+namespaces",
				fingerprint: digestObject({ policy: POLICY_ID, deniedPaths }),
			},
		},
	} satisfies ProcessProducerProof);
}

function compatibleProducer(expected: ProcessProducerProof, candidate: ProcessProducerProof): boolean {
	return (
		expected.observer.provider === candidate.observer.provider &&
		expected.observer.fingerprint === candidate.observer.fingerprint &&
		expected.execution.authority === "speculative" &&
		candidate.execution.authority === "speculative" &&
		expected.execution.confinement.provider === candidate.execution.confinement.provider &&
		expected.execution.confinement.fingerprint === candidate.execution.confinement.fingerprint
	);
}

function namespaceArguments(command: readonly string[]): readonly string[] {
	return [
		"--user",
		"--map-current-user",
		"--keep-caps",
		"--mount",
		"--pid",
		"--fork",
		"--mount-proc",
		"--net",
		"--ipc",
		"--uts",
		"--",
		...command,
	];
}

async function runSpawn(
	executable: string,
	args: readonly string[],
	options: {
		readonly cwd: string;
		readonly environment: Readonly<Record<string, string>>;
		readonly stdin?: Buffer;
		readonly signal?: AbortSignal;
		readonly timeoutSeconds?: number;
		readonly onOutput?: (event: BufferedOutput) => void;
	},
): Promise<SpawnOutcome> {
	throwIfAborted(options.signal);
	const child = spawn(executable, args, {
		cwd: options.cwd,
		env: options.environment,
		detached: true,
		stdio: [options.stdin ? "pipe" : "ignore", "pipe", "pipe"],
	});
	const output: BufferedOutput[] = [];
	const append = (fd: 1 | 2, value: Buffer) => {
		const event = { fd, data: Buffer.from(value) } as const;
		const previous = output.at(-1);
		if (previous?.fd === fd && previous.data.byteLength + event.data.byteLength <= 1024 * 1024) {
			(output as BufferedOutput[])[output.length - 1] = { fd, data: Buffer.concat([previous.data, event.data]) };
		} else output.push(event);
		options.onOutput?.(event);
	};
	child.stdout?.on("data", (value: Buffer) => append(1, value));
	child.stderr?.on("data", (value: Buffer) => append(2, value));
	if (options.stdin) child.stdin?.end(options.stdin);
	const terminate = () => {
		if (!child.pid) return;
		try {
			process.kill(-child.pid, "SIGKILL");
		} catch {
			child.kill("SIGKILL");
		}
	};
	const onAbort = () => terminate();
	options.signal?.addEventListener("abort", onAbort, { once: true });
	let timeout: ReturnType<typeof setTimeout> | undefined;
	let timedOut = false;
	if (options.timeoutSeconds !== undefined) {
		timeout = setTimeout(() => {
			timedOut = true;
			terminate();
		}, Math.max(1, options.timeoutSeconds * 1000));
	}
	try {
		const result = await new Promise<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>((resolve, reject) => {
			child.once("error", reject);
			child.once("exit", (code, signal) => resolve({ code, signal }));
		});
		if (options.signal?.aborted) throw new Error("aborted");
		if (timedOut) throw new Error(`timeout:${options.timeoutSeconds}`);
		return { ...result, output };
	} finally {
		if (timeout) clearTimeout(timeout);
		options.signal?.removeEventListener("abort", onAbort);
	}
}

function parseDispatcherRequest(body: string): DispatcherRequest | undefined {
	try {
		const value: unknown = JSON.parse(body.trim());
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const request = value as Partial<DispatcherRequest>;
		if (
			request.version !== 1 ||
			typeof request.token !== "string" ||
			typeof request.name !== "string" ||
			typeof request.invokedPath !== "string" ||
			request.invokedPath.includes("\0") ||
			!Array.isArray(request.args) ||
			!request.args.every((argument) => typeof argument === "string" && !argument.includes("\0")) ||
			typeof request.cwd !== "string" ||
			!request.environment ||
			typeof request.environment !== "object" ||
			!Number.isSafeInteger(request.umask) ||
			request.stdin !== "closed"
		) {
			return undefined;
		}
		if (
			!Object.entries(request.environment).every(
				([name, value]) => name.length > 0 && !name.includes("=") && !name.includes("\0") && typeof value === "string" && !value.includes("\0"),
			)
		) {
			return undefined;
		}
		return request as DispatcherRequest;
	} catch {
		return undefined;
	}
}

function eligibleRequest(session: ActiveSession, request: DispatcherRequest, executable: string): boolean {
	return (
		pathContains(session.workspace.sandboxRoot, request.cwd) &&
		request.args.length <= 4096 &&
		request.args.reduce((total, value) => total + Buffer.byteLength(value), 0) <= 1024 * 1024 &&
		Boolean(executable)
	);
}

function executionEnvironment(environment: Readonly<Record<string, string>>, executable: string): Record<string, string> {
	const result = { ...environment };
	for (const name of Object.keys(result)) if (name.startsWith("PI_SPEC_")) delete result[name];
	result._ = executable;
	return result;
}

function normalizeEnvironment(environment: Readonly<Record<string, string | undefined>>): Record<string, string> {
	return Object.fromEntries(Object.entries(environment).filter((entry): entry is [string, string] => entry[1] !== undefined));
}

function shellArguments(invocation: ToolProcessInvocation, command: string): string[] {
	return invocation.commandTransport === "argv" ? [...invocation.shellArgs, command] : [...invocation.shellArgs];
}

async function actorTopLevelPrototype(
	invocation: ToolProcessInvocation,
	request: ProcessExecutionRequest,
	projection: ExecutionPathProjection,
	platformFingerprint: Sha256Digest,
): Promise<ExecPrototype> {
	return topLevelProcessPrototype(invocation, request, normalizeEnvironment(request.environment), projection, platformFingerprint);
}

async function topLevelProcessPrototype(
	invocation: ToolProcessInvocation,
	request: ProcessExecutionRequest,
	environment: Readonly<Record<string, string>>,
	projection: ExecutionPathProjection,
	platformFingerprint: Sha256Digest,
): Promise<ExecPrototype> {
	const argv = [invocation.shell, ...invocation.shellArgs];
	if (invocation.commandTransport === "argv") argv.push(request.command);
	return createExecPrototype({
		executablePath: invocation.shell,
		executableDigest: sha256Digest(await readFile(invocation.shell)),
		argv: argv.map((value) => projection.normalizeValue(value)),
		logicalCwd: projection.toLogical(projection.toPhysical(request.cwd) ?? request.cwd),
		environment,
		umask: process.umask(),
		rlimitsDigest: digestObject({ inherited: sha256Digest(await readFile("/proc/self/limits")) }),
		signalDispositionsDigest: digestObject({ spawn: "node-default-v1" }),
		credentialsDigest: digestObject({
			uid: process.getuid?.(), euid: process.geteuid?.(), gid: process.getgid?.(), egid: process.getegid?.(), groups: process.getgroups?.(),
		}),
		schedulingDigest: digestObject({ scheduler: "inherited", cpuCount: os.availableParallelism(), timeout: request.timeout ?? null }),
		stdin:
			invocation.commandTransport === "stdin"
				? { type: "bytes", digest: sha256Digest(request.command), eof: true }
				: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [
			{ fd: 0, type: invocation.commandTransport === "stdin" ? "pipe" : "device", flagsDigest: digestObject({ mode: "read" }), eof: true },
			{ fd: 1, type: "pipe", flagsDigest: digestObject({ mode: "write", sink: "buffered" }) },
			{ fd: 2, type: "pipe", flagsDigest: digestObject({ mode: "write", sink: "buffered" }) },
		],
		platformFingerprint,
	});
}

function actorReplayProducer(producer: ProcessProducerProof, deniedPaths: readonly string[]): boolean {
	if (producer.observer.provider !== "strace" || producer.observer.fingerprint !== digestObject({ epoch: BACKEND_EPOCH })) {
		return false;
	}
	return producer.execution.authority === "actor" || (
		producer.execution.confinement.provider === "sandlock+namespaces" &&
		producer.execution.confinement.fingerprint === digestObject({ policy: POLICY_ID, deniedPaths })
	);
}

async function resolveBinary(explicit: string | undefined, name: string, fallbacks: readonly string[] = []): Promise<string> {
	for (const candidate of [explicit, ...fallbacks].filter((value): value is string => Boolean(value))) {
		try {
			await access(candidate, fsConstants.X_OK);
			return await realpath(candidate);
		} catch {
			// Try PATH.
		}
	}
	const pathValue = process.env.PATH ?? "";
	for (const directory of pathValue.split(path.delimiter)) {
		if (!directory) continue;
		const candidate = path.join(directory, name);
		try {
			await access(candidate, fsConstants.X_OK);
			return await realpath(candidate);
		} catch {
			// Continue.
		}
	}
	throw new Error(`${name} executable not found`);
}

function execText(executable: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
		});
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

function closeServer(server: net.Server): Promise<void> {
	return new Promise((resolve) => server.close(() => resolve()));
}

function exitOutcome(outcome: SpawnOutcome): ExitOutcome {
	if (!outcome.signal) return { kind: "code", code: outcome.code ?? 125 };
	return { kind: "signal", signal: os.constants.signals[outcome.signal] ?? 9, coreDumped: false };
}

function randomToken(): string {
	return randomBytes(32).toString("hex");
}

function assertInvocationMatches(invocation: ToolProcessInvocation, request: ProcessExecutionRequest): void {
	if (request.command !== invocation.command) throw new Error("process command differs from the action execution context");
	if (path.resolve(request.cwd) !== path.resolve(invocation.cwd)) {
		throw new Error("process cwd differs from the action execution context");
	}
	if (request.timeout !== invocation.timeout) throw new Error("process timeout differs from the action execution context");
	if (digestObject(normalizeEnvironment(request.environment)) !== digestObject(invocation.environment)) {
		throw new Error("process environment differs from the action execution context");
	}
}

export async function validateTransferredProcessEvidence(
	evidence: DynamicDependencyCertificate | undefined,
	incompleteReasons: Iterable<string> = [],
): Promise<ResourceValidation> {
	if (!evidence) {
		return {
			status: "indeterminate",
			cause: { stage: "freshness", code: "process_evidence_missing" },
			metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" },
		};
	}
	const blockingTaints = evidence.taints.filter((taint) => !TRANSFERRED_INPUT_TAINTS.has(taint));
	const validation = await validateDynamicDependencyCertificate(
		{ ...evidence, taints: blockingTaints },
		{ maxFileBytes: MAX_CAPTURE_BYTES },
	);
	const metrics = {
		durationMs: validation.durationMs,
		bytesRead: validation.bytesRead,
		filesRead: validation.filesRead,
		mode: "exact" as const,
	};
	if (validation.status === "valid") return { status: "valid", metrics };
	if (validation.status === "stale") {
		return {
			status: "stale",
			cause: { stage: "freshness", code: "process_dependency_changed", detail: validation.changed.join(",") },
			metrics,
		};
	}
	return {
		status: "indeterminate",
		cause: {
			stage: "freshness",
			code: "process_provenance_indeterminate",
			detail: [validation.reason, ...incompleteReasons].join(","),
		},
		metrics,
	};
}

function mergeDependencyEvidence(
	certificates: readonly DynamicDependencyCertificate[],
	incompleteReasons: Set<string>,
	mutatedDirectories: ReadonlySet<string> = new Set(),
): DynamicDependencyCertificate {
	const dependencies = new Map<string, DynamicDependency>();
	const encodings = new Map<string, Sha256Digest>();
	const taints = new Set<ProvenanceTaint>();
	let complete = certificates.length > 0;
	for (const certificate of certificates) {
		complete &&= certificate.complete;
		for (const taint of certificate.taints) taints.add(taint);
		for (const dependency of certificate.dependencies) {
			const identity = dependency.kind === "fd" ? `fd:${dependency.fd}` : `${dependency.kind}:${dependency.path}`;
			const existing = dependencies.get(identity);
			if (
				existing?.kind === "file" &&
				dependency.kind === "file" &&
				existing.contentDigest === dependency.contentDigest &&
				existing.metadataDigest === dependency.metadataDigest
			) {
				if (existing.role !== "executable" && dependency.role === "executable") {
					dependencies.set(identity, dependency);
					encodings.set(identity, digestObject(dependency));
				}
				continue;
			}
			const encoding = digestObject(dependency);
			const previous = encodings.get(identity);
			if (previous && previous !== encoding) {
				if (dependency.kind === "directory" && mutatedDirectories.has(dependency.path.replaceAll("\\", "/"))) {
					continue;
				}
				complete = false;
				incompleteReasons.add(`dependency_changed_during_execution:${identity}`);
				continue;
			}
			dependencies.set(identity, dependency);
			encodings.set(identity, encoding);
		}
	}
	return {
		complete: complete && incompleteReasons.size === 0,
		dependencies: Object.freeze([...dependencies.values()]),
		taints: Object.freeze([...taints]),
	};
}

function sensitivePaths(storeRoot: string, additional: readonly string[] | undefined): readonly string[] {
	const home = os.homedir();
	return Object.freeze(
		[
			storeRoot,
			path.join(home, ".ssh"),
			path.join(home, ".gnupg"),
			path.join(home, ".aws"),
			path.join(home, ".azure"),
			path.join(home, ".kube"),
			path.join(home, ".docker", "config.json"),
			path.join(home, ".config", "gcloud"),
			path.join(home, ".config", "gh", "hosts.yml"),
			path.join(home, ".git-credentials"),
			path.join(home, ".netrc"),
			path.join(home, ".npmrc"),
			path.join(home, ".pypirc"),
			path.join(home, ".pi", "agent", "auth.json"),
			path.join(home, ".codex", "auth.json"),
			...(additional ?? []),
		]
			.filter((value) => path.isAbsolute(value))
			.map((value) => path.resolve(value))
			.filter((value, index, values) => values.indexOf(value) === index),
	);
}

function pathContains(root: string, target: string): boolean {
	const relative = path.relative(path.resolve(root), path.resolve(target));
	return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function slash(value: string): string {
	return value.replaceAll("\\", "/");
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (signal?.aborted) throw signal.reason ?? new Error("aborted");
}

function missing(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
}

function permissionDenied(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && (error.code === "EACCES" || error.code === "EPERM"));
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
