import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
	access,
	chmod,
	copyFile,
	link,
	lstat,
	mkdir,
	mkdtemp,
	readFile,
	readdir,
	readlink,
	realpath,
	rm,
	writeFile,
} from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
	createExecPrototype,
	digestObject,
	dynamicDependencyIdentity,
	type DynamicDependency,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type ExitOutcome,
	type OrderedEffectEvent,
	type ProcessProducerProof,
	type ProcessProvenanceCertificate,
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
import { isPoisonedEffectCommit } from "./effect-transaction.ts";
import {
	inspectHeldExecProcess,
	LinuxHeldExecBoundary,
	resolveLinuxExecHelper,
	type HeldExecDecision,
	type HeldExecProcess,
} from "./linux-held-exec.ts";
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
import { SpeculationScheduler, type ServiceTimingIdentity, waitForCandidate } from "./scheduler.ts";
import { observeStrace, straceCommand, type ObservedProcessPath, type StraceObservation } from "./strace-observer.ts";
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

const BACKEND_EPOCH = "pi-linux-process-v15";
const POLICY_ID = "sandlock-virtual-root-transparent-exec-v13";
const LEAF_POLICY_ID = "sandlock-virtual-workspace-leaf-v2";
const MAX_REQUEST_BYTES = 4 * 1024 * 1024;
const MAX_CAPTURE_BYTES = 512 * 1024 * 1024;
/** Native inputs consumed by this exact one-shot execution; they still prohibit any later replay. */
const TRANSFERRED_INPUT_TAINTS = new Set<ProvenanceTaint>([
	"clock", "random", "pid_observation", "descriptor_observation",
]);

export interface LinuxProcessBackendOptions {
	readonly storeRoot: string;
	readonly store?: ProvenanceStoreOptions;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
	readonly heldExecBinary?: string;
	/** Additional host paths that speculative processes must never read. */
	readonly deniedPaths?: readonly string[];
}

export interface CompletedProcessReplayOptions {
	readonly sourceRoot: string;
	readonly invocation: (request: ProcessExecutionRequest) => ToolProcessInvocation | undefined;
	readonly enabled?: () => boolean;
}

export interface HeldExecActorReplayOptions {
	readonly sourceRoot: string;
	readonly realShell: string;
	readonly enabled: () => boolean;
	readonly scope?: () => ExecutionScope | undefined;
}

export interface HeldExecActorReplayRoute {
	readonly shellPath: string;
	readonly executor: (host: ProcessExecutor) => ProcessExecutor;
}

export interface LinuxProcessBackendStatus {
	readonly state: "ready" | "unavailable";
	readonly detail: string;
	readonly fingerprint?: string;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
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
	readonly fingerprint: string;
	readonly platformFingerprint: Sha256Digest;
	readonly observerFingerprint: Sha256Digest;
	readonly executionContext: DispatcherExecutionContext;
	readonly dispatcher: string;
}

interface InterposedDirectory {
	readonly source: string;
	readonly target: string;
	readonly shadow: string;
	readonly view: string;
}

interface SandboxMount {
	readonly virtualPath: string;
	readonly hostPath: string;
	readonly readOnly: boolean;
}

interface ExecMount {
	readonly virtualPath: string;
	readonly hostPath: string;
}

interface ProcessInterposition {
	readonly mounts: readonly SandboxMount[];
	readonly execMounts: readonly ExecMount[];
	readonly directories: readonly InterposedDirectory[];
	readonly executables: readonly (readonly [intercepted: string, original: string])[];
	readonly dependencies: readonly DynamicDependency[];
}

interface DispatcherRequest {
	readonly version: 2;
	readonly token: string;
	readonly name: string;
	readonly invokedPath: string;
	readonly argv0: string;
	readonly args: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: DispatcherExecutionContext;
}

interface DispatcherExecutionContext {
	readonly key: string;
	/** Parent contract, excluding state that the host-side leaf broker deliberately supplies. */
	readonly launchKey: string;
	readonly umask: number;
	readonly descriptorTypes: readonly ["device" | "other", "pipe" | "socket", "pipe" | "socket"];
	readonly outputEndpoints: readonly [string, string];
}

type OutputRoute = readonly [1 | 2, 1 | 2];

interface BufferedOutput {
	readonly fd: 1 | 2;
	readonly data: Buffer;
}

interface DispatcherResponse {
	readonly version: 2;
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
	readonly nestedProducer: ProcessProducerProof;
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
	topLevelOutputEndpoints?: Promise<readonly [string, string]>;
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

interface ProcessTransfer {
	readonly completion: Promise<void>;
	readonly finish: () => void;
	readonly startedAt: number;
	readonly scope: ExecutionScope | undefined;
	candidate?: ProcessProvenanceCertificate;
	state: "running" | "completed" | "claimed";
}

type CompletedProcessPlan = Extract<ProcessReusePlan, { kind: "completed_replay" }>;

/** Linux-only process substrate. Unavailable dependencies remove the route instead of weakening it. */
export class LinuxProcessReuseBackend {
	readonly store: ProvenanceCertificateStore;
	readonly planner: ProcessReusePlanner;
	readonly storage: ExecutionWorldStorageControl;
	private readonly options: LinuxProcessBackendOptions;
	private ready?: Promise<ReadyBackend>;
	private platformFingerprint?: Promise<Sha256Digest>;
	private heldExecBoundary?: LinuxHeldExecBoundary;
	private heldExecOpening?: Promise<LinuxHeldExecBoundary>;
	private disposed = false;
	private readonly transfers = new Map<Sha256Digest, ProcessTransfer[]>();
	private readonly certificateScopes = new Map<Sha256Digest, ExecutionScope>();
	private readonly processScheduler = new SpeculationScheduler<object>({ candidateJoinPolicy: { uncalibratedWaitMs: 0 } });
	private readonly counters: MutableLinuxProcessReuseMetrics = { ...emptyWorldReuseMetrics() };

	constructor(options: LinuxProcessBackendOptions) {
		this.options = options;
		this.store = new ProvenanceCertificateStore(options.storeRoot, options.store);
		this.planner = new ProcessReusePlanner({ store: this.store });
		this.storage = {
			configure: ({ maxEntries, maxBytes }) => {
				this.store.configure({ maxCertificates: maxEntries, maxBytes });
				this.trimTransfers();
			},
			maintain: async (operation) => {
				this.trimTransfers(true);
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
				detail: "Landlock/seccomp virtual filesystem + strace provenance ready",
				fingerprint: ready.fingerprint,
				sandlockBinary: ready.sandlock,
				straceBinary: ready.strace,
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

	/** Hold real Actor child execs; a miss continues exactly once and requires no Fork dependencies. */
	async heldExecActorReplay(options: HeldExecActorReplayOptions): Promise<HeldExecActorReplayRoute> {
		const boundary = await (this.heldExecOpening ??= LinuxHeldExecBoundary.open({
			storeRoot: this.options.storeRoot,
			...(this.options.heldExecBinary ? { binary: this.options.heldExecBinary } : {}),
		}));
		this.heldExecBoundary = boundary;
		return {
			shellPath: boundary.shellPath,
			executor: (host) => boundary.executor(host, {
				realShell: options.realShell,
				sourceRoot: path.resolve(options.sourceRoot),
				enabled: options.enabled,
				decide: (process) => this.decideHeldExec(process, options.scope?.()),
			}),
		};
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
					const plan = await this.plan(prototype, projection, acceptProducer);
					if (!plan) return this.actorReplayMiss(host, request);
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
					if (committed || isPoisonedEffectCommit(error)) throw error;
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
			dispatcherBinary: ready.dispatcher,
			excludedExecutables: [
				input.invocation.shell,
				process.execPath,
				ready.dispatcher,
				ready.sandlock,
				ready.strace,
			],
		});
		const deniedPaths = sensitivePaths(this.options.storeRoot, this.options.deniedPaths).filter(
			(target) =>
				!pathContains(input.workspace.sandboxRoot, target) && !pathContains(input.workspace.processRoot, target),
		);
		const producer = speculativeProducerProof(ready, deniedPaths, POLICY_ID);
		const nestedProducer = speculativeProducerProof(ready, deniedPaths, LEAF_POLICY_ID);
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
			nestedProducer,
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
		const running = [...this.transfers.values()].flat().filter((transfer) => transfer.state === "running");
		for (const work of running) work.finish();
		await Promise.allSettled(running.map((work) => work.completion));
		await this.heldExecBoundary?.close();
		this.transfers.clear();
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
		const [sandlock, strace, dispatcher] = await Promise.all([
			resolveBinary(this.options.sandlockBinary, "pi-speculative-sandlock", [
				path.join(os.homedir(), ".local", "bin", "pi-speculative-sandlock"),
			]),
			resolveBinary(this.options.straceBinary, "strace"),
			resolveLinuxExecHelper(this.options.heldExecBinary),
		]);
		const [sandlockCheck, sandlockVersion, straceVersion, platformFingerprint] = await Promise.all([
			execText(sandlock, ["check"]),
			execText(sandlock, ["--version"]),
			execText(strace, ["-V"]),
			this.resolvePlatformFingerprint(),
		]);
		if (!sandlockCheck.includes("Status:         OK")) throw new Error("Sandlock kernel protections are unavailable");
		const mountProbe = await mkdtemp(path.join(os.tmpdir(), "pi-process-view-probe-"));
		let executionContext: DispatcherExecutionContext | undefined;
		try {
			const logicalRoot = path.join(mountProbe, "logical");
			const physicalRoot = path.join(mountProbe, "physical");
			await Promise.all([mkdir(logicalRoot), mkdir(physicalRoot)]);
			executionContext = await probeExecutionContext({ sandlock, strace, logicalRoot, physicalRoot });
		} finally {
			await rm(mountProbe, { recursive: true, force: true });
		}
		if (!executionContext) throw new Error("process execution context probe failed");
		const observerFingerprint = digestObject({ epoch: BACKEND_EPOCH });
		const fingerprint = digestObject({
			epoch: BACKEND_EPOCH,
			policy: POLICY_ID,
			sandlock: sandlockVersion.trim(),
			strace: straceVersion.split(/\r?\n/)[0]?.trim(),
			platformFingerprint,
			executionContext: sha256Digest(executionContext.key),
			arch: process.arch,
			deniedPaths: sensitivePaths(this.options.storeRoot, this.options.deniedPaths),
		});
		return {
			sandlock, strace, fingerprint, platformFingerprint, observerFingerprint,
			executionContext, dispatcher,
		};
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
		const logicalCwd = session.projection.toLogical(physicalCwd);
		const prototype = await topLevelProcessPrototype(
			session.invocation, request, environment, session.projection, ready.platformFingerprint,
		);
		this.add(session, "wholeCommandRequests");
		const plan = await this.plan(
			prototype,
			session.projection,
			(candidate) => compatibleProducer(session.producer, candidate),
			session,
		);
		if (plan) return this.replayTopLevel(session, plan, request, requestStarted);
		this.add(session, "wholeCommandMisses");
		const sandbox = sandboxArguments({
			ready,
			cwd: logicalCwd,
			deniedPaths: session.deniedPaths,
			writablePaths: [session.workspace.sandboxRoot, session.socketPath],
			mounts: session.interposition.mounts,
			execMounts: session.interposition.execMounts,
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
				ready.strace,
				traced.slice(1),
				{
					cwd: physicalCwd,
					environment,
					onSpawn: (pid) => {
						session.topLevelOutputEndpoints = Promise.all([
							readlink(`/proc/${pid}/fd/1`),
							readlink(`/proc/${pid}/fd/2`),
						]).then(([stdout, stderr]) => [stdout, stderr] as const);
					},
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
				const observation = await observeStrace(tracePrefix, session.invocation.shell, logicalCwd, {
					interposedExecutables: session.interposition.executables,
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
		if (await this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS)) {
			this.rememberScope(certificate.id, session.scope);
			this.add(session, "wholeCommandPublished");
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
					const received = parseDispatcherRequest(body);
					const request = received ? materializeDispatcherRequest(session, received) : undefined;
					const executable = request ? await this.resolveRequestedExecutable(session, request).catch(() => undefined) : undefined;
					socket.end(JSON.stringify({ version: 2, kind: "bypass", ...(executable ? { executable } : {}) }));
				});
		});
	}

	private async handleWireRequest(session: ActiveSession, body: string): Promise<DispatcherResponse> {
		const requestStarted = performance.now();
		const received = parseDispatcherRequest(body);
		if (!received || received.token !== session.token || session.closed) throw new Error("invalid dispatcher request");
		const request = materializeDispatcherRequest(session, received);
		if (!request) throw new Error("dispatcher cwd is unmapped");
		this.add(session, "requests");
		const ready = await this.resolveReady();
		const executable = await this.resolveRequestedExecutable(session, request);
		const outputRoute = await eligibleRequest(session, request, executable, ready.executionContext);
		if (!outputRoute) {
			this.add(session, "bypasses");
			session.incompleteReasons.add(`broker_bypass:${request.name}`);
			return { version: 2, kind: "bypass", executable };
		}
		const prototype = await this.prototype(session, request, executable, outputRoute);
		const weakKey = processWeakKey(prototype);
		const acquired = await this.acquireProcessResult(
			weakKey,
			(live) => this.plan(prototype, session.projection, (candidate) => compatibleProducer(session.nestedProducer, candidate), session, live),
			undefined,
			session.scope,
		);
		if (acquired.plan) return this.replay(session, acquired.plan, weakKey, acquired.joined, requestStarted);
		if (!acquired.work) throw new Error("process work reservation failed");
		this.add(session, "misses");
		try {
			return await this.executeAndPublish(session, request, executable, prototype, weakKey, outputRoute, acquired.work);
		} finally {
			acquired.work.finish();
		}
	}

	private async acquireProcessResult(
		weakKey: Sha256Digest,
		lookup: (live?: ProcessProvenanceCertificate) => Promise<CompletedProcessPlan | undefined>,
		signal?: AbortSignal,
		scope?: ExecutionScope,
		actor?: { readonly timing: ServiceTimingIdentity; readonly arrivedAt: number },
	): Promise<{ readonly plan?: CompletedProcessPlan; readonly work?: ProcessTransfer; readonly joined: boolean; readonly waitedMs: number; readonly actorMs?: number }> {
		let joined = false;
		let waitedMs = 0;
		let actorMs: number | undefined;
		const miss = () => actor ? { joined, waitedMs } : { work: this.claimProcessWork(weakKey, scope), joined, waitedMs };
		const hit = (plan: CompletedProcessPlan) => ({
			plan, joined, waitedMs, ...(actorMs === undefined ? {} : { actorMs }),
		});
		const admitCompleted = () => {
			if (!actor || joined) return true;
			const decision = this.processScheduler.assessCandidateJoin({
				identity: actor.timing,
				state: "succeeded",
				expectedSpeculativeDurationMs: 1,
			});
			actorMs = decision.expectedActorMs;
			return decision.allowed;
		};
		while (true) {
			const completed = await lookup();
			if (completed) return admitCompleted() ? hit(completed) : miss();
			const transfers = this.transfers.get(weakKey) ?? [];
			for (const transfer of [...transfers].reverse()) {
				if (transfer.state !== "completed" || !transfer.candidate || !sameScope(transfer.scope, scope)) continue;
				const plan = await lookup(transfer.candidate);
				if (plan && transfer.state === "completed") {
					if (!admitCompleted()) return miss();
					transfer.state = "claimed";
					this.removeTransfer(weakKey, transfer);
					return hit(plan);
				}
			}
			const running = transfers.find((transfer) => transfer.state === "running" && sameScope(transfer.scope, scope));
			if (!running) return miss();
			if (!actor) {
				joined = true;
				await running.completion;
				continue;
			}
			const decision = this.processScheduler.assessCandidateJoin({
				identity: actor.timing,
				state: "running",
				expectedSpeculativeDurationMs: 1,
				elapsedMs: Math.max(0, performance.now() - running.startedAt),
				actorElapsedMs: Math.max(0, performance.now() - actor.arrivedAt),
			});
			actorMs = decision.expectedActorMs;
			if (!decision.allowed) return miss();
			const waitStarted = performance.now();
			const finished = await waitForCandidate(running.completion, signal, decision.waitBudgetMs);
			waitedMs += Math.max(0, performance.now() - waitStarted);
			if (finished.status !== "completed") {
				throwIfAborted(signal);
				return miss();
			}
			joined = true;
		}
	}

	private claimProcessWork(weakKey: Sha256Digest, scope?: ExecutionScope): ProcessTransfer {
		let settle!: () => void;
		const completion = new Promise<void>((resolve) => {
			settle = resolve;
		});
		const work: ProcessTransfer = {
			completion,
			startedAt: performance.now(),
			scope,
			state: "running",
			finish: () => {
				if (work.state !== "running") return;
				work.state = "completed";
				if (!work.candidate || !work.scope) this.removeTransfer(weakKey, work);
				else this.trimTransfers();
				settle();
			},
		};
		const transfers = this.transfers.get(weakKey) ?? [];
		transfers.push(work);
		this.transfers.set(weakKey, transfers);
		return work;
	}

	private removeTransfer(weakKey: Sha256Digest, transfer: ProcessTransfer): void {
		const retained = this.transfers.get(weakKey)?.filter((candidate) => candidate !== transfer) ?? [];
		if (retained.length) this.transfers.set(weakKey, retained);
		else this.transfers.delete(weakKey);
	}

	private trimTransfers(clear = false): void {
		let excess = clear ? Number.POSITIVE_INFINITY :
			[...this.transfers.values()].flat().filter((transfer) => transfer.state === "completed").length - this.store.limits.maxCertificates;
		if (excess <= 0) return;
		for (const [weakKey, transfers] of this.transfers) {
			const retained = transfers.filter((transfer) => transfer.state === "running" || excess-- <= 0);
			if (retained.length) this.transfers.set(weakKey, retained);
			else this.transfers.delete(weakKey);
		}
	}

	private async plan(
		prototype: ExecPrototype,
		projection: ExecutionPathProjection,
		acceptProducer: (producer: ProcessProducerProof) => boolean,
		session?: ActiveSession,
		live?: ProcessProvenanceCertificate,
	): Promise<CompletedProcessPlan | undefined> {
		const plan = await this.planner.plan({
			prototype,
			acceptProducer,
			contract: {
				sink: "buffered",
				orderedJournal: true,
				transactionalEffects: true,
				mode: "completed_replay",
			},
			validation: {
				resolvePath: (logicalPath) => projection.toPhysical(logicalPath),
				...(session ? { acceptedTaints: SAME_CONFINEMENT_TAINTS } : {}),
			},
			...(live ? { live: { certificate: live, acceptedTaints: [...TRANSFERRED_INPUT_TAINTS] } } : {}),
		});
		this.recordLookup(plan.lookup, session);
		if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
			const detail = `reuse_miss:${plan.reasons.join(",")}${
				plan.changedDependencies?.length ? `:${plan.changedDependencies.join(",")}` : ""
			}`;
			if (session) this.setError(session, detail);
			else this.counters.lastError = `actor_${detail}`;
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

	private async decideHeldExec(process: HeldExecProcess, scope?: ExecutionScope): Promise<HeldExecDecision> {
		const requestStarted = performance.now();
		this.counters.requests++;
		try {
			throwIfAborted(process.signal);
			const sourceRoot = path.resolve(process.sourceRoot);
			const snapshot = await inspectHeldExecProcess(process.pid);
			if (!pathContains(sourceRoot, snapshot.cwd)) {
				this.counters.bypasses++;
				return { kind: "continue" };
			}
			const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
			const prototype = createExecPrototype({
				executablePath: projection.toLogical(snapshot.executable),
				executableDigest: sha256Digest(await readFile(`/proc/${process.pid}/exe`)),
				argv: snapshot.argv.map((value) => projection.normalizeValue(value)),
				logicalCwd: projection.toLogical(snapshot.cwd),
				environment: Object.fromEntries(
					Object.entries(snapshot.environment).map(([name, value]) => [name, projection.normalizeValue(value)]),
				),
				umask: snapshot.context.umask,
				processContextDigest: sha256Digest(snapshot.context.key),
				stdin: { type: "closed", eof: true },
				fileDescriptorTableComplete: true,
				inheritedFDs: snapshot.context.descriptorTypes.map((type, fd) => ({
					fd,
					type,
					flagsDigest: sha256Digest(`${snapshot.context.key}\0${fd}`),
					...(fd === 0 ? { eof: true } : {}),
				})),
				platformFingerprint: await this.resolvePlatformFingerprint(),
			});
			const weakKey = processWeakKey(prototype);
			const timing = processTimingIdentity(prototype, weakKey);
			const accepted = (producer: ProcessProducerProof) =>
				actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
			const acquired = await this.acquireProcessResult(
				weakKey,
				(live) => this.plan(prototype, projection, accepted, undefined, live),
				process.signal,
				scope,
				{ timing, arrivedAt: requestStarted },
			);
			const plan = acquired.plan;
			if (!plan || plan.certificate.result.exit.kind !== "code") {
				this.counters.misses++;
				return {
					kind: "continue",
					observeCompletion: (durationMs) => this.processScheduler.observeActorService(timing, durationMs),
				};
			}
			const output = loadOutputEvents(plan.artifacts, plan.certificate.result.journal);
			return {
				kind: "replay",
				exitCode: plan.certificate.result.exit.code,
				output,
				commit: async () => {
					const started = performance.now();
					try {
						throwIfAborted(process.signal);
						await replayFilesystemEffects(plan.artifacts, plan.certificate.result.journal, projection, sourceRoot);
						this.recordHit(plan.certificate, acquired.joined, undefined, scope);
						this.processScheduler.observeAdoption(
							timing,
							Math.max(0, performance.now() - requestStarted - acquired.waitedMs),
						);
						if (acquired.actorMs !== undefined) {
							this.counters.timedHits++;
							this.counters.avoidedProcessMs += acquired.actorMs;
							this.counters.timedHitOverheadMs += Math.max(0, performance.now() - requestStarted);
						}
					} catch (error) {
						this.counters.lastError = `actor_child_commit:${errorMessage(error)}`;
						throw error;
					} finally {
						this.counters.replayMs += Math.max(0, performance.now() - started);
					}
				},
			};
		} catch (error) {
			this.counters.bypasses++;
			this.counters.lastError = `actor_child:${errorMessage(error)}`;
			return { kind: "continue" };
		}
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
			this.recordHit(certificate, joined, session);
			replayed = true;
			return { version: 2, kind: "hit", weakKey, output, exit: certificate.result.exit };
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
		outputRoute: OutputRoute,
		work: ProcessTransfer,
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
			const logicalExecutable = session.projection.toLogical(executable);
			const logicalCwd = session.projection.toLogical(request.cwd);
			const command = straceCommand(ready.strace, tracePrefix, [
				ready.sandlock,
				...sandboxPolicyArguments(
					logicalCwd,
					session.deniedPaths,
					[session.workspace.sandboxRoot],
					[{ virtualPath: session.sourceRoot, hostPath: session.workspace.sandboxRoot, readOnly: false }],
					[],
				),
				"--",
				process.execPath,
				fileURLToPath(new URL("./process-dispatcher.mjs", import.meta.url)),
				"--exec",
				outputRoute.join(""),
				request.name,
				logicalExecutable,
				...request.args,
			]);
			const processStarted = performance.now();
			outcome = await runSpawn(ready.strace, command.slice(1), {
				cwd: request.cwd,
				environment: executionEnvironment(request.environment),
			});
			observedProcessMs = Math.max(0, performance.now() - processStarted);
			try {
				transactionFinishing = true;
				const [delta, observation] = await Promise.all([
					transaction.finish(),
					observeStrace(tracePrefix, logicalExecutable, session.projection.toLogical(request.cwd), {
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
					producer: session.nestedProducer,
					dependencyCertificate,
					result: { replayProfile: "buffered_noninteractive", observedProcessMs, journal, exit },
				});
				session.nestedEvidence.push(certificate.dependencyCertificate);
				this.rememberScope(certificate.id, session.scope);
				if (await this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS)) {
					this.add(session, "published");
				} else work.candidate = certificate;
				if (taints.size) {
					this.add(session, "tainted");
					this.setError(session, `tainted:${[...taints].join(",")}`);
				}
			} catch (error) {
				// The process already ran. Certificate failure must never cause dispatcher fallback/re-execution.
				this.setError(session, `post_execution_capture:${errorMessage(error)}`);
				session.incompleteReasons.add(`nested_capture:${errorMessage(error)}`);
			}
			const exit = exitOutcome(outcome);
			return { version: 2, kind: "executed", weakKey, output: wireOutput(outcome.output), exit };
		} finally {
			const durationMs = Math.max(0, performance.now() - started);
			this.add(session, "executionMs", durationMs);
			if (outcome) this.processScheduler.observeSpeculativeService(processTimingIdentity(prototype, weakKey), durationMs);
			if (!transactionFinishing) await transaction.abort().catch(() => undefined);
			await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
		}
	}

	private add(session: ActiveSession, metric: CountedReuseMetric, value = 1): void {
		this.counters[metric] += value;
		session.metrics[metric] += value;
	}

	private recordHit(
		certificate: ProcessProvenanceCertificate,
		joined: boolean,
		session?: ActiveSession,
		scope: ExecutionScope | undefined = session?.scope,
	): void {
		const add = (metric: CountedReuseMetric) => session ? this.add(session, metric) : this.counters[metric]++;
		add("hits");
		if (joined) add("joinedHits");
		const producer = this.certificateScopes.get(certificate.id);
		add(
			producer && scope
				? sameScope(producer, scope)
					? "sameTurnHits"
					: "crossTurnHits"
				: "unattributedHits",
		);
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
				(directory) => [directory.target, directory.view].some(
					(candidate) => path.resolve(path.dirname(invoked)) === path.resolve(candidate),
				),
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

	private async prototype(
		session: ActiveSession,
		request: DispatcherRequest,
		executable: string,
		outputRoute: OutputRoute,
	): Promise<ExecPrototype> {
		const [content, ready] = await Promise.all([readFile(executable), this.resolveReady()]);
		const context = routedExecutionContext(ready.executionContext, outputRoute);
		const contextDigest = sha256Digest(context.key);
		const environment = Object.fromEntries(
			Object.entries(executionEnvironment(request.environment)).map(([name, value]) => [
				name,
				session.projection.normalizeValue(value),
			]),
		);
		const argv = [request.argv0, ...request.args].map((value) => session.projection.normalizeValue(value));
		return createExecPrototype({
			executablePath: session.projection.toLogical(executable),
			executableDigest: sha256Digest(content),
			argv,
			logicalCwd: session.projection.toLogical(request.cwd),
			environment,
			umask: context.umask,
			processContextDigest: contextDigest,
			stdin: { type: "closed", eof: true },
			fileDescriptorTableComplete: true,
			inheritedFDs: context.descriptorTypes.map((type, fd) => ({
				fd, type, flagsDigest: sha256Digest(`${context.key}\0${fd}`), ...(fd === 0 ? { eof: true } : {}),
			})),
			platformFingerprint: ready.platformFingerprint,
		});
	}
}

function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}

function processTimingIdentity(prototype: ExecPrototype, weakKey: Sha256Digest): ServiceTimingIdentity {
	return {
		tool: "process",
		executionFingerprint: digestObject({
			executable: prototype.executableDigest,
			context: prototype.processContextDigest,
			platform: prototype.platformFingerprint,
		}),
		actionKeyHash: weakKey,
	};
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
	observed: readonly ObservedProcessPath[],
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
		const identity = dynamicDependencyIdentity(dependency);
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
		if (isInterposedLauncher(session, observedPath)) continue;
		if (session.deniedPaths.some((denied) => pathContains(denied, observedPath))) {
			taints.add("escaped_sandbox");
			incompleteReasons.add(`denied:${observedPath}`);
			continue;
		}
		const physical = pathContains(session.sourceRoot, observedPath)
			? (session.projection.toPhysical(observedPath) ?? observedPath)
			: observedPath;
		if (item.role === "metadata") {
			add({
				kind: "metadata",
				path: session.projection.isWorkspacePhysical(physical) ? session.projection.toLogical(physical) : slash(physical),
				followSymlinks: item.followSymlinks,
				digest: item.digest,
			});
			continue;
		}
		if (STABLE_SANDBOX_DEVICES.has(observedPath)) continue;
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
const SAME_CONFINEMENT_TAINTS = ["confinement_observation"] as const;

function isInterposedLauncher(session: ActiveSession, target: string): boolean {
	const normalized = path.resolve(target);
	return session.interposition.executables.some(([intercepted]) => path.resolve(intercepted) === normalized);
}

function workspaceMetadataExclusions(session: ActiveSession, target: string): readonly string[] | undefined {
	return path.resolve(target) === path.resolve(session.workspace.sandboxRoot)
		? session.workspace.observationExcludes
		: undefined;
}

async function captureHostPath(
	physicalPath: string,
	role: Exclude<ObservedProcessPath["role"], "metadata">,
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
	readonly dispatcherBinary: string;
	readonly excludedExecutables: readonly string[];
}): Promise<ProcessInterposition> {
	const root = path.join(input.privateRoot, "process-interposition");
	const viewRoot = path.join(root, "views");
	const shadowRoot = path.join(root, "originals");
	await Promise.all([mkdir(viewRoot, { recursive: true }), mkdir(shadowRoot, { recursive: true })]);
	const launcher = path.join(root, "dispatcher");
	const dispatcher = fileURLToPath(new URL("./process-dispatcher.mjs", import.meta.url));
	await copyFile(input.dispatcherBinary, launcher);
	await chmod(launcher, 0o755);
	const directories: InterposedDirectory[] = [];
	const seenTargets = new Set<string>();
	for (const rawDirectory of input.pathValue.split(path.delimiter)) {
		if (!rawDirectory || !path.isAbsolute(rawDirectory)) continue;
		const logicalDirectory = path.resolve(rawDirectory);
		if (seenTargets.has(logicalDirectory)) continue;
		seenTargets.add(logicalDirectory);
		const projected = input.projection.toPhysical(logicalDirectory) ?? logicalDirectory;
		let source: string;
		try {
			source = await realpath(projected);
			if (!(await lstat(source)).isDirectory()) continue;
		} catch {
			continue;
		}
		const index = directories.length.toString().padStart(3, "0");
		const shadow = path.join(shadowRoot, index);
		if ([logicalDirectory, source, shadow, process.execPath, dispatcher].some((value) => /[\r\n]/.test(value))) continue;
		directories.push({
			source,
			target: logicalDirectory,
			shadow,
			view: path.join(viewRoot, index),
		});
	}
	const configurationPath = path.join(root, "configuration.json");
	const configuration = {
		version: 2,
		socketPath: input.socketPath,
		token: input.token,
		directories: directories.map(({ target, view, shadow }) => ({ target, view, shadow })),
	};
	await writeFile(configurationPath, JSON.stringify(configuration), { mode: 0o600 });

	const excluded = new Set<string>();
	for (const candidate of input.excludedExecutables) {
		try {
			excluded.add(await realpath(candidate));
		} catch {
			// A missing exclusion cannot be executed.
		}
	}
	const executables: Array<readonly [string, string]> = [];
	const execMounts: ExecMount[] = [];
	const dependencies: DynamicDependency[] = [];
	const dependencySources = new Set<string>();
	for (const directory of directories) {
		await Promise.all([mkdir(directory.shadow, { recursive: true }), mkdir(directory.view, { recursive: true })]);
		await writeFile(
			path.join(directory.view, ".pi-spec-dispatch-v1"),
			["PI_SPEC_DISPATCH_V1", process.execPath, dispatcher, configurationPath, directory.target, directory.shadow, ""].join("\n"),
			{ mode: 0o600 },
		);
		let entries: string[];
		try {
			entries = await readdir(directory.source);
		} catch {
			continue;
		}
		for (const name of entries) {
			if (!name || name === ".pi-spec-dispatch-v1" || name.includes("/") || name.includes("\0")) continue;
			const sourceEntry = path.join(directory.source, name);
			const viewEntry = path.join(directory.view, name);
			try {
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
					await link(launcher, viewEntry);
					const intercepted = path.join(directory.target, name);
					executables.push([intercepted, path.join(directory.shadow, name)]);
					executables.push([viewEntry, path.join(directory.shadow, name)]);
					execMounts.push({ virtualPath: intercepted, hostPath: viewEntry });
				}
			} catch {
				// An entry that cannot be proved executable remains visible through its original directory.
			}
		}
		if (!dependencySources.has(directory.source)) {
			dependencySources.add(directory.source);
			dependencies.push(
				await captureDirectoryDependency(
					directory.source,
					input.projection.isWorkspacePhysical(directory.source)
						? input.projection.toLogical(directory.source)
						: slash(directory.source),
					true,
					path.resolve(directory.source) === path.resolve(input.workspaceRoot) ? input.workspaceExcludes : [],
				),
			);
		}
	}
	const mounts = uniqueSandboxMounts([
		...directories.map(({ shadow, source }) => ({ virtualPath: shadow, hostPath: source, readOnly: true })),
		{ virtualPath: input.sourceRoot, hostPath: input.workspaceRoot, readOnly: false },
	]);
	return {
		mounts,
		execMounts: Object.freeze(execMounts),
		directories: Object.freeze(directories),
		executables: Object.freeze(executables),
		dependencies: Object.freeze(dependencies),
	};
}

function sandboxArguments(input: {
	readonly ready: ReadyBackend;
	readonly cwd: string;
	readonly deniedPaths: readonly string[];
	readonly writablePaths: readonly string[];
	readonly mounts: readonly SandboxMount[];
	readonly execMounts: readonly ExecMount[];
	readonly command: readonly string[];
	readonly timeoutSeconds?: number;
}): readonly string[] {
	return [
		input.ready.sandlock,
		...sandboxPolicyArguments(
			input.cwd, input.deniedPaths, input.writablePaths, input.mounts, input.execMounts,
		),
		...(input.timeoutSeconds !== undefined ? ["--timeout", String(Math.max(1, Math.ceil(input.timeoutSeconds)))] : []),
		"--",
		...input.command,
	];
}

function sandboxPolicyArguments(
	cwd: string,
	deniedPaths: readonly string[],
	writablePaths: readonly string[],
	mounts: readonly SandboxMount[],
	execMounts: readonly ExecMount[],
): readonly string[] {
	return [
		"run",
		"--chroot",
		"/",
		...mounts.flatMap((mount) => ["--fs-mount", sandboxMountArgument(mount)]),
		...execMounts.flatMap((mount) => ["--exec-mount", execMountArgument(mount)]),
		"--fs-read",
		"/",
		...writablePaths.flatMap((target) => ["--fs-write", target]),
		...mounts.filter((mount) => !mount.readOnly).flatMap((mount) => ["--fs-write", mount.virtualPath]),
		...[...STABLE_SANDBOX_DEVICES].flatMap((target) => ["--fs-write", target]),
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

function uniqueSandboxMounts(mounts: readonly SandboxMount[]): readonly SandboxMount[] {
	const seen = new Set<string>();
	return Object.freeze(
		[...mounts]
			.sort((left, right) => right.virtualPath.length - left.virtualPath.length)
			.filter(({ virtualPath }) => {
				const normalized = path.resolve(virtualPath);
				if (seen.has(normalized)) return false;
				seen.add(normalized);
				return true;
			}),
	);
}

function sandboxMountArgument(mount: SandboxMount): string {
	if (!path.isAbsolute(mount.virtualPath) || !path.isAbsolute(mount.hostPath) || [mount.virtualPath, mount.hostPath].some((value) => value.includes(":"))) {
		throw new Error(`Sandlock mount cannot represent ${mount.virtualPath}:${mount.hostPath}`);
	}
	return `${mount.virtualPath}:${mount.hostPath}:${mount.readOnly ? "ro" : "rw"}`;
}

function execMountArgument(mount: ExecMount): string {
	if (!path.isAbsolute(mount.virtualPath) || !path.isAbsolute(mount.hostPath) || [mount.virtualPath, mount.hostPath].some((value) => value.includes(":"))) {
		throw new Error(`Sandlock exec mount cannot represent ${mount.virtualPath}:${mount.hostPath}`);
	}
	return `${mount.virtualPath}:${mount.hostPath}`;
}

async function probeExecutionContext(input: {
	readonly sandlock: string;
	readonly strace: string;
	readonly logicalRoot: string;
	readonly physicalRoot: string;
}): Promise<DispatcherExecutionContext> {
	const command = straceCommand(input.strace, path.join(input.physicalRoot, "context"), [
		input.sandlock,
		...sandboxPolicyArguments(input.logicalRoot, [], [input.physicalRoot], [
			{ virtualPath: input.logicalRoot, hostPath: input.physicalRoot, readOnly: false },
		], []),
		"--",
		process.execPath,
		fileURLToPath(new URL("./process-dispatcher.mjs", import.meta.url)),
		"--exec",
		"12",
		"pi-context-probe",
		process.execPath,
		fileURLToPath(new URL("./process-dispatcher.mjs", import.meta.url)),
		"--probe-context",
		input.logicalRoot,
	]);
	const outcome = await runSpawn(
		input.strace,
		command.slice(1),
		{ cwd: input.physicalRoot, environment: normalizeEnvironment(process.env) },
	);
	if (outcome.signal || outcome.code !== 0) throw new Error("process execution context probe failed");
	const stdout = Buffer.concat(outcome.output.filter(({ fd }) => fd === 1).map(({ data }) => data)).toString();
	const parsed: unknown = JSON.parse(stdout);
	if (!validDispatcherContext(parsed)) throw new Error("process execution context probe returned invalid data");
	return parsed;
}

function speculativeProducerProof(
	ready: ReadyBackend,
	deniedPaths: readonly string[],
	policy = POLICY_ID,
): ProcessProducerProof {
	return Object.freeze({
		observer: { provider: "strace", fingerprint: ready.observerFingerprint },
		execution: {
			authority: "speculative",
			confinement: {
				provider: "sandlock",
				fingerprint: digestObject({ policy, deniedPaths }),
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
		readonly onSpawn?: (pid: number) => void;
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
	if (child.pid) options.onSpawn?.(child.pid);
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
		const result = await new Promise<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>(
			(resolve, reject) => {
				child.once("error", reject);
				// `close` follows process exit and complete drainage of every stdio stream.
				child.once("close", (code, signal) => resolve({ code, signal }));
			},
		);
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
			request.version !== 2 ||
			typeof request.token !== "string" ||
			typeof request.name !== "string" ||
			typeof request.invokedPath !== "string" ||
			request.invokedPath.includes("\0") ||
			typeof request.argv0 !== "string" ||
			request.argv0.length > 1024 * 1024 ||
			request.argv0.includes("\0") ||
			!Array.isArray(request.args) ||
			!request.args.every((argument) => typeof argument === "string" && !argument.includes("\0")) ||
			typeof request.cwd !== "string" ||
			!request.environment ||
			typeof request.environment !== "object" ||
			!validDispatcherContext(request.context)
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

function materializeDispatcherRequest(
	session: ActiveSession,
	request: DispatcherRequest,
): DispatcherRequest | undefined {
	const cwd = session.projection.toPhysical(request.cwd);
	return cwd ? { ...request, cwd } : undefined;
}

async function eligibleRequest(
	session: ActiveSession,
	request: DispatcherRequest,
	executable: string,
	expectedContext: DispatcherExecutionContext,
): Promise<OutputRoute | undefined> {
	const endpoints = await session.topLevelOutputEndpoints?.catch(() => undefined);
	if (
		!executable || !endpoints || !pathContains(session.workspace.sandboxRoot, request.cwd) ||
		request.args.length > 4096 || request.args.reduce((sum, value) => sum + Buffer.byteLength(value), 0) > 1024 * 1024
	) return undefined;
	const routeOf = (endpoint: string): 0 | 1 | 2 => endpoint === endpoints[0] ? 1 : endpoint === endpoints[1] ? 2 : 0;
	const stdout = routeOf(request.context.outputEndpoints[0]);
	const stderr = routeOf(request.context.outputEndpoints[1]);
	if (!stdout || !stderr) return undefined;
	const route: OutputRoute = [stdout, stderr];
	const context = routedExecutionContext(expectedContext, route);
	return request.context.launchKey === context.launchKey && request.context.umask === context.umask ? route : undefined;
}

function routedExecutionContext(context: DispatcherExecutionContext, route: OutputRoute): DispatcherExecutionContext {
	const semantic = JSON.parse(context.key) as {
		credentials: Record<string, unknown>;
		signals: { blocked: string; ignored: string };
		descriptors: Record<string, unknown>[];
		[key: string]: unknown;
	};
	if (!semantic.credentials || !validSignalState(semantic.signals) ||
		!Array.isArray(semantic.descriptors) || semantic.descriptors.length !== 3) {
		throw new Error("invalid probed execution context");
	}
	const descriptors = [
		semantic.descriptors[0]!,
		{ ...semantic.descriptors[route[0]]!, fd: 1, alias: 1 },
		{ ...semantic.descriptors[route[1]]!, fd: 2, alias: route[0] === route[1] ? 1 : 2 },
	];
	// libuv resets the signal mask and dispositions for every spawned target.
	const signals = {
		blocked: semantic.signals.blocked.replace(/[0-9a-f]/gi, "0"),
		ignored: semantic.signals.ignored.replace(/[0-9a-f]/gi, "0"),
	};
	const routed = { ...semantic, executionDomain: "ptrace-v1", signals, descriptors };
	return {
		...context,
		key: JSON.stringify(routed),
		launchKey: JSON.stringify({
			...routed,
			credentials: { ...semantic.credentials, groups: "broker-preserved" },
			signals: "broker-normalized",
		}),
		descriptorTypes: [
			context.descriptorTypes[0],
			context.descriptorTypes[route[0]],
			context.descriptorTypes[route[1]],
		],
	};
}

function validSignalState(value: unknown): value is { blocked: string; ignored: string } {
	if (!value || typeof value !== "object") return false;
	const signals = value as { blocked?: unknown; ignored?: unknown };
	return typeof signals.blocked === "string" && /^[0-9a-f]+$/i.test(signals.blocked) &&
		typeof signals.ignored === "string" && /^[0-9a-f]+$/i.test(signals.ignored);
}

function validDispatcherContext(value: unknown): value is DispatcherExecutionContext {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const context = value as Partial<DispatcherExecutionContext>;
	return (
		typeof context.key === "string" && context.key.length > 0 && context.key.length <= 64 * 1024 &&
		typeof context.launchKey === "string" && context.launchKey.length > 0 && context.launchKey.length <= 64 * 1024 &&
		Number.isSafeInteger(context.umask) && context.umask! >= 0 && context.umask! <= 0o777 &&
		Array.isArray(context.descriptorTypes) && context.descriptorTypes.length === 3 &&
		context.descriptorTypes[0] === "device" && ["pipe", "socket"].includes(context.descriptorTypes[1] ?? "") &&
		["pipe", "socket"].includes(context.descriptorTypes[2] ?? "") &&
		Array.isArray(context.outputEndpoints) && context.outputEndpoints.length === 2 &&
		context.outputEndpoints.every((endpoint) => typeof endpoint === "string" && endpoint.length <= 4096)
	);
}

function executionEnvironment(environment: Readonly<Record<string, string>>): Record<string, string> {
	const result = { ...environment };
	for (const name of Object.keys(result)) if (name.startsWith("PI_SPEC_")) delete result[name];
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
		processContextDigest: digestObject({
			limits: sha256Digest(await readFile("/proc/self/limits")),
			credentials: {
				uid: process.getuid?.(), euid: process.geteuid?.(), gid: process.getgid?.(), egid: process.getegid?.(), groups: process.getgroups?.(),
			},
			scheduler: { cpuCount: os.availableParallelism(), timeout: request.timeout ?? null },
			signals: "node-default-v1",
		}),
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
	if (producer.execution.authority === "actor") return true;
	const confinement = producer.execution.confinement;
	return (
		confinement.provider === "sandlock" &&
		[POLICY_ID, LEAF_POLICY_ID].some(
			(policy) => confinement.fingerprint === digestObject({ policy, deniedPaths }),
		)
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
			const identity = dynamicDependencyIdentity(dependency);
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
