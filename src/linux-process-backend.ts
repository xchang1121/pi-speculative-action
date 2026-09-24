import { routedProcessContext, validProcessContext, type ProcessExecutionContext } from "./process-context.mjs";
import { execFile, spawn } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { constants as fsConstants } from "node:fs";
import {
	access,
	chmod,
	copyFile,
	link,
	lstat,
	mkdir,
	mkdtemp,
	open,
	readFile,
	readdir,
	realpath,
	rm,
	writeFile,
} from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { finished } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import { errorMessage, isMissing as missing } from "./error-utils.ts";
import { stableEqual } from "./stable-json.ts";
import { TimelineInterval, type TimelineDependency } from "./task-timing.ts";
import {
	createExecPrototype,
	digestObject,
	dynamicDependencyIdentity,
	type DynamicDependency,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type ExitOutcome,
	type OrderedEffectEvent,
	type OFDPosition,
	type ProcessProducerProof,
	type ProcessProvenanceCertificate,
	type ProcessResultRecord,
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
	validateDynamicDependencyCertificate,
} from "./provenance-validation.ts";
import {
	diffWorkspaceStructures,
	ExecutionPathProjection,
	hydrateWorkspaceFileEntry,
	snapshotDependency,
	type WorkspaceStructureSnapshot,
	type WorkspaceTransactionDiff,
	type WorkspaceTreeEntry,
} from "./process-observation.ts";
import {
	definedProcessEnvironment,
	type PreparedProcessExecutionRoute,
	type ProcessExecutionRequest,
	type ProcessExecutionResult,
	type ProcessExecutor,
} from "./process-execution.ts";
import { isPoisonedEffectCommit } from "./effect-transaction.ts";
import { resolveHostExecutable } from "./executable-path.ts";
import { assertNoSymlinkPath, captureStableFile, hashExecutableFile, mapFilesystem, sameFilesystemIdentity, walkFilesystemPath } from "./filesystem-evidence.ts";
import {
	captureHeldDescriptorInputs,
	inspectHeldExecProcess,
	LinuxHeldExecBoundary,
	listenUnixSocket,
	resolveLinuxExecHelper,
	type HeldExecDecision,
	type HeldExecProcess,
	type HeldExecSnapshot,
	descriptorInputs, descriptorEffects,
	type ProcessResourceGraph,
} from "./linux-held-exec.ts";
import {
	emptyWorldReuseMetrics,
	snapshotExecutionScope,
	type ExecutionScope,
	type ExecutionOperationAdoption,
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
import { ProcessHandoffOwnership, ProcessHandoffRegistry, sameScope, type ProcessContinuation, type ProcessExecutionBinding, type ProcessHandoff, type ProcessHandoffLookup } from "./process-handoff.ts";
import {
	WorkspaceSandboxService,
	readSandboxDirectoryState,
	sameSandboxState,
	type SandboxDirectoryChange,
	type SandboxFileChange,
	type SandboxWorkspaceChange,
	type SandboxWorkspaceContext,
} from "./workspace-sandbox.ts";
import { containsFilesystemPath as pathContains, relativeFilesystemPath, slash } from "./path-utils.ts";

const BACKEND_EPOCH = "pi-linux-process-instance-inputs";
const POLICY_ID = "sandlock-virtual-root-transparent-exec";
const LEAF_POLICY_ID = "sandlock-virtual-workspace-leaf";
const MAX_REQUEST_BYTES = 4 * 1024 * 1024, LEARNED_LAUNCHES = 64;
const MAX_CONTINUATION_BYTES = 65 * 1024 * 1024;
const IO_FRONTIERS = new Map([[0, "read"], [1, "write"], [19, "readv"], [20, "writev"], [44, "sendto"], [45, "recvfrom"], [46, "sendmsg"], [47, "recvmsg"]]);
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
}

export interface ActorProcessReplayOptions extends CompletedProcessReplayOptions {
	readonly held?: {
		readonly realShell: string;
		readonly executor: (shellPath: string) => ProcessExecutor;
		readonly scope?: () => ExecutionScope | undefined;
	};
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
	readonly computationDependencies: () => readonly TimelineDependency[];
	/** Captured exec units in dispatcher arrival order, still speculative until the enclosing branch is adopted. */
	readonly executionBindings: () => readonly ProcessExecutionBinding[];
	/** Execute one retained exec unit in this fresh sandbox; its output is not the enclosing tool's result. */
	readonly executeBinding: (binding: ProcessExecutionBinding) => Promise<{
		readonly output: readonly BufferedOutput[];
		readonly exit?: ExitOutcome;
		readonly suspended?: true;
	}>;
	readonly ownership: ProcessHandoffOwnership;
	readonly metrics: () => LinuxProcessReuseMetrics;
	/** Join the outer workspace transaction delta to the process observation before validation. */
	readonly seal: (changes: readonly SandboxWorkspaceChange[]) => Promise<readonly SandboxWorkspaceChange[]>;
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
	readonly executionContext: ProcessExecutionContext;
	readonly dispatcher: string;
	readonly imageLibrary?: string;
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

interface DispatcherRequest {
	readonly token: string;
	readonly name: string;
	readonly invokedPath: string;
	readonly argv0: string;
	readonly args: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly context: ProcessExecutionContext;
	/** Present when a bypass will exec the native image in place of this process. */
	readonly pid?: number;
}

/** The outlet each target output uses; 0 discards into /dev/null. */
type OutputRoute = readonly [0 | 1 | 2, 0 | 1 | 2];
type RequestEligibility = { readonly route: OutputRoute } | { readonly reason: string };
type ProcessArguments = Pick<DispatcherRequest, "argv0" | "args" | "cwd" | "environment"> & {
	readonly closeStdin?: boolean; readonly resources?: ProcessResourceGraph; readonly outputPipes?: readonly [boolean, boolean];
};
type BoundProcessInvocation = ProcessArguments & {
	readonly sourceRoot: string;
	readonly executable: string;
	readonly outputRoute: OutputRoute;
	readonly producer?: ProcessProducerProof;
};

interface BufferedOutput {
	readonly fd: 1 | 2;
	readonly data: Buffer;
}

interface DispatcherResponse {
	readonly kind: "hit" | "executed" | "bypass" | "suspended";
	readonly executable?: string;
	readonly output?: readonly { readonly fd: 1 | 2; readonly data: string }[];
	readonly exit?: ExitOutcome;
	readonly weakKey?: Sha256Digest;
}

interface ActiveSession {
	readonly token: string;
	readonly ownership: ProcessHandoffOwnership;
	readonly sourceRoot: string;
	readonly workspace: SandboxWorkspaceContext;
	readonly invocation: ToolProcessInvocation;
	readonly scope?: ExecutionScope;
	readonly projection: ExecutionPathProjection;
	interposition: Awaited<ReturnType<typeof createProcessInterposition>>;
	readonly originalPath: string;
	readonly deniedPaths: readonly string[];
	readonly producer: ProcessProducerProof;
	readonly nestedProducer: ProcessProducerProof;
	readonly socketPath: string;
	readonly signal: AbortSignal;
	readonly pending: Set<Promise<unknown>>;
	readonly nestedEvidence: DynamicDependencyCertificate[];
	readonly executionBindings: Map<number, ProcessExecutionBinding>;
	readonly computations: TimelineDependency[];
	readonly incompleteReasons: Set<string>;
	/** Bypasses that exec their native image in place; the top-level trace must show each one resume. */
	readonly bypasses: [pid: number, reason: string][];
	readonly metrics: MutableLinuxProcessReuseMetrics;
	topLevelCapture?: TopLevelCapture;
	topLevelExecution?: {
		readonly prototype: ExecPrototype;
		readonly outcome: SpawnOutcome;
		readonly observedProcessMs: number;
	};
	topLevelEvidence?: DynamicDependencyCertificate;
	topLevelOutputEndpoints?: readonly [string, string];
	sealPromise?: Promise<readonly SandboxWorkspaceChange[]>;
	closing?: Promise<void>;
}

interface TopLevelCapture {
	readonly before: WorkspaceStructureSnapshot;
	readonly after: WorkspaceStructureSnapshot;
	readonly observation: StraceObservation;
}

interface SpawnOutcome {
	readonly code: number | null;
	readonly signal: NodeJS.Signals | null;
	readonly output: readonly BufferedOutput[];
}

type ReadyProcessPlan = Exclude<ProcessReusePlan, { kind: "miss" }>;

/** Linux-only process substrate. Unavailable dependencies remove the route instead of weakening it. */
export class LinuxProcessReuseBackend {
	private readonly observations = new AsyncLocalStorage<{ scope: ExecutionScope; sequence: number; closed: boolean; learn: boolean; learned: Set<string>;
		inputs?: (path: string) => Iterable<object>; bindings: Map<number, ProcessExecutionBinding>; computations: TimelineDependency[] }>();

	/** Keep actual completed launches and acknowledged adoptions in their enclosing native call's order. */
	async observeBindings<Value>(scope: ExecutionScope, execute: () => Promise<Value>,
		observe: (bindings: readonly ProcessExecutionBinding[], computations: readonly TimelineDependency[]) => void, learn = false,
		inputs?: (path: string) => Iterable<object>): Promise<Value> {
		const observation = { scope: snapshotExecutionScope(scope)!, sequence: 0, closed: false,
			learn, learned: new Set<string>(), inputs, bindings: new Map<number, ProcessExecutionBinding>(), computations: [] as TimelineDependency[] };
		try { return await this.observations.run(observation, execute); }
		finally {
			observation.closed = true;
			try { observe([...observation.bindings].sort(([left], [right]) => left - right).map(([, binding]) => binding),
				Object.freeze([...observation.computations])); }
			catch { /* Learning cannot replace the native result or error. */ }
		}
	}
	readonly store: ProvenanceCertificateStore;
	readonly planner: ProcessReusePlanner;
	readonly storage: ExecutionWorldStorageControl;
	private readonly options: LinuxProcessBackendOptions;
	private ready?: Promise<ReadyBackend>;
	private platformFingerprint?: Promise<Sha256Digest>;
	private heldExec?: Promise<LinuxHeldExecBoundary>;
	private disposed = false;
	private readonly handoffs: ProcessHandoffRegistry<BoundProcessInvocation | { readonly trackingOnly: true; readonly sourceRoot: string }>;
	private readonly processScheduler = new SpeculationScheduler<object>();
	private readonly counters: MutableLinuxProcessReuseMetrics = { ...emptyWorldReuseMetrics() };
	private readonly actorCounters: MutableLinuxProcessReuseMetrics = { ...emptyWorldReuseMetrics() };
	private readonly replayWorkspace = new WorkspaceSandboxService();
	private disposal?: Promise<void>;
	private producers = 0;

	constructor(options: LinuxProcessBackendOptions) {
		this.options = options;
		this.store = new ProvenanceCertificateStore(options.storeRoot, { ...options.store, acceptedTaints: SAME_CONFINEMENT_TAINTS });
		this.planner = new ProcessReusePlanner({ store: this.store });
		this.handoffs = new ProcessHandoffRegistry(this.store.limits.maxCertificates, Math.min(MAX_CONTINUATION_BYTES, this.store.limits.maxBytes));
		this.storage = {
			configure: ({ maxEntries, maxBytes }) => {
				this.store.configure({ maxCertificates: maxEntries, maxBytes });
				this.handoffs.configure(this.store.limits.maxCertificates, Math.min(MAX_CONTINUATION_BYTES, this.store.limits.maxBytes));
			},
			maintain: async (operation) => {
				this.handoffs.clearCompleted();
				const result = await (operation === "gc" ? this.store.gc() : this.store.clear());
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
				detail: `Landlock/seccomp virtual filesystem + strace provenance ready; ${ready.imageLibrary
					? "single-thread live I/O continuation available" : "live I/O continuation unavailable; setup:linux enables the capture tier"}`,
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

	/** Aggregate backend counters retained for qualification and low-level diagnostics. */
	metrics(): LinuxProcessReuseMetrics {
		return Object.freeze({ ...this.counters });
	}

	/** Actor-path counters, excluding child reuse performed inside speculative worlds. */
	actorMetrics(): LinuxProcessReuseMetrics {
		return Object.freeze({ ...this.actorCounters });
	}

	/** Scoped launches; sandbox bindings are still speculative until adopted. Raw parameters are never persisted. */
	executionBindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return this.handoffs.bindings(scope).filter(binding => {
			const invocation = this.handoffs.resolveBinding(binding, scope);
			return invocation && !("trackingOnly" in invocation);
		});
	}

	/** Keep possible publication visible through preparation, execution, and final evidence capture. */
	async withProducer<Value>(run: () => Promise<Value>): Promise<Value> {
		this.producers++;
		try { return await run(); } finally { this.producers--; }
	}

	private get hasLiveResults(): boolean {
		return this.producers > 0 || this.handoffs.hasResults;
	}

	async prepareActorReplay(host: ProcessExecutor, options: ActorProcessReplayOptions, refresh = false): Promise<PreparedProcessExecutionRoute> {
		if (process.platform !== "linux") return { state: "unavailable", detail: "Linux or WSL 2 required" };
		let state: "degraded" | "ready" = "degraded";
		let detail = options.held ? "Bash history; child handoff checked when evidence exists or on refresh"
			: "matching whole Bash calls; this shell cannot hold child processes";
		let prepared: Promise<ProcessExecutor> | undefined;
		const prepare = async () => {
			let executor = host;
			if (options.held) try {
				const boundary = await (this.heldExec ??= LinuxHeldExecBoundary.open({
					storeRoot: this.options.storeRoot,
					...(this.options.heldExecBinary ? { binary: this.options.heldExecBinary } : {}),
				}));
				executor = boundary.executor(options.held.executor(boundary.shellPath), {
					realShell: options.held.realShell,
					sourceRoot: path.resolve(options.sourceRoot),
					descriptors: request => {
						if (request.scope) for (const binding of this.handoffs.bindings(request.scope)) {
							const invocation = this.handoffs.resolveBinding(binding, request.scope);
							if (invocation && ("trackingOnly" in invocation || invocation.resources?.handles.length) && invocation.sourceRoot === path.resolve(options.sourceRoot)) return true;
						}
						return this.observations.getStore()?.learn ? "inspect" : false;
					},
					decide: (process) => this.decideHeldExec(process, process.scope),
				}, host);
				state = "ready";
				detail = "matching whole Bash calls plus completed or running child processes";
			} catch (error) {
				detail = `matching whole Bash calls; child handoff unavailable (${errorMessage(error)})`;
			}
			return this.completedReplayExecutor(executor, options);
		};
		if (refresh) await (prepared = prepare());
		return { get state() { return state; }, get detail() { return detail; }, executor: {
			execute: async (request) => {
				try {
					request = { ...request, scope: snapshotExecutionScope("scope" in request ? request.scope : options.held?.scope?.()) };
				} catch { return host.execute(request); }
				const observation = this.observations.getStore();
				const learning = observation?.learn && !observation.closed && sameScope(observation.scope, request.scope);
				// Learning explicitly requests held execs; other calls retain the empty-history fast path.
				if ((!learning && !this.hasLiveResults && !(await this.store.mayHaveCertificates()) && !this.hasLiveResults) || this.disposed) return host.execute(request);
				return (await (prepared ??= prepare())).execute(request);
			},
		} };
	}

	async resetActorReplay(): Promise<void> {
		const heldExec = this.heldExec;
		this.heldExec = undefined;
		await heldExec?.then((boundary) => boundary.close(), () => undefined);
	}

	/** Hit-only Actor path. Certificate lookup and replay deliberately require no tracing or confinement tools. */
	completedReplayExecutor(host: ProcessExecutor, options: CompletedProcessReplayOptions): ProcessExecutor {
		const sourceRoot = path.resolve(options.sourceRoot);
		const acceptProducer = (producer: ProcessProducerProof) =>
			actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
		return {
			execute: async (request) => {
				if (process.platform !== "linux" || !pathContains(sourceRoot, request.cwd)) return host.execute(request);
				const requestStarted = performance.now();
				this.addActor("wholeCommandRequests");
				let committed = false;
				let timing: ServiceTimingIdentity | undefined;
				try {
					request.signal?.throwIfAborted();
					const invocation = options.invocation(request);
					if (!invocation) return this.actorReplayMiss(host, request);
					assertInvocationMatches(invocation, request);
					const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
					const platformFingerprint = await this.resolvePlatformFingerprint();
					const prototype = await topLevelProcessPrototype(
						invocation,
						request,
						definedProcessEnvironment(request.environment),
						projection,
						platformFingerprint,
					);
					const weakKey = processWeakKey(prototype);
					timing = processTimingIdentity(prototype, weakKey);
					const admission = this.processScheduler.assessCandidateJoin({ identity: timing, state: "succeeded" });
					if (!admission.allowed) return this.actorReplayMiss(host, request, timing);
					const plan = await this.plan(weakKey, prototype.executablePath, projection, acceptProducer);
					if (!plan?.certificate.result.exit) return this.actorReplayMiss(host, request, timing);
					request.signal?.throwIfAborted();
					const replayStarted = performance.now();
					await replayFilesystemEffects(this.replayWorkspace, plan.artifacts, plan.certificate.result.journal, projection, sourceRoot);
					committed = true;
					for (const event of loadOutputEvents(plan.artifacts, plan.certificate.result.journal)) request.onData(event.data);
					const hitLatencyMs = Math.max(0, performance.now() - requestStarted);
					const observation = this.observations.getStore();
					if (observation && !observation.closed && sameScope(observation.scope, request.scope))
						observation.computations.push(reusedComputation(plan.certificate.result, requestStarted));
					this.addActor("wholeCommandReplayMs", Math.max(0, performance.now() - replayStarted));
					this.addActor("wholeCommandReusedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
					this.addActor("wholeCommandHits");
					this.processScheduler.observeAdoption(timing, hitLatencyMs);
					return { exitCode: plan.certificate.result.exit.kind === "code" ? plan.certificate.result.exit.code : null };
				} catch (error) {
					this.setActorError(`actor_replay:${errorMessage(error)}`);
					if (committed || isPoisonedEffectCommit(error)) throw error;
					return this.actorReplayMiss(host, request, timing);
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
		readonly onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
		readonly acceptOperationScope?: (scope: ExecutionScope) => boolean;
	}): Promise<LinuxProcessSession> {
		return this.withProducer(() => this.createSession(input));
	}

	private async createSession(input: Parameters<LinuxProcessReuseBackend["open"]>[0]): Promise<LinuxProcessSession> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		const ready = await this.resolveReady();
		input.signal?.throwIfAborted();
		const sourceRoot = path.resolve(input.sourceRoot);
		const projection = new ExecutionPathProjection({
			sourceRoot,
			workspaceRoot: input.workspace.sandboxRoot,
			privateRoot: input.workspace.processRoot,
		});
		const originalPath = input.invocation.environment.PATH ?? input.invocation.environment.Path ?? "";
		const token = randomToken();
		const socketPath = path.join(input.workspace.processRoot, `broker-${token.slice(0, 12)}.sock`);
		const deniedPaths = sensitivePaths(this.options.storeRoot, this.options.deniedPaths).filter(
			(target) =>
				!pathContains(input.workspace.sandboxRoot, target) && !pathContains(input.workspace.processRoot, target),
		);
		const producer = speculativeProducerProof(ready, deniedPaths, POLICY_ID);
		const nestedProducer = speculativeProducerProof(ready, deniedPaths, LEAF_POLICY_ID);
		const controller = new AbortController();
		const server = net.createServer({ allowHalfOpen: true }, (socket) => this.serve(session, socket));
		const session: ActiveSession = {
			token,
			sourceRoot,
			workspace: input.workspace,
			invocation: input.invocation,
			scope: snapshotExecutionScope(input.scope),
			projection,
			interposition: { mounts: [], execMounts: [], directories: [], executables: [], dependencies: [] },
			originalPath,
			deniedPaths,
			producer,
			nestedProducer,
			socketPath,
			signal: AbortSignal.any([controller.signal, ...(input.signal ? [input.signal] : [])]),
			pending: new Set<Promise<unknown>>(),
			ownership: new ProcessHandoffOwnership(input.onOperationAdopted, input.acceptOperationScope),
			nestedEvidence: [],
			executionBindings: new Map(),
			computations: [],
			incompleteReasons: new Set<string>(), bypasses: [],
			metrics: { ...emptyWorldReuseMetrics() },
		};
		this.producers++;
		let executionKind: "tool" | "operation" | undefined;
		let dispatch: Promise<void> | undefined;
		const execute = <Value>(kind: NonNullable<typeof executionKind>, operation: () => Promise<Value>): Promise<Value> => {
			if (session.closing || executionKind === "operation" || executionKind && executionKind !== kind)
				return Promise.reject(new Error("process session execution boundary is already consumed"));
			executionKind = kind;
			const pending = Promise.resolve().then(async () => {
				session.signal?.throwIfAborted();
				// A bound operation already names its executable; only enclosing tools need PATH interception.
				if (kind === "tool") await (dispatch ??= createProcessInterposition({
					privateRoot: input.workspace.processRoot,
					pathValue: originalPath,
					projection,
					sourceRoot,
					workspaceRoot: input.workspace.sandboxRoot,
					workspaceExcludes: input.workspace.observationExcludes,
					signal: session.signal,
					token,
					socketPath,
					dispatcherBinary: ready.dispatcher,
					excludedExecutables: [input.invocation.shell, process.execPath, ready.dispatcher, ready.sandlock, ready.strace],
				}).then(interposition => {
					session.signal?.throwIfAborted();
					session.interposition = interposition;
					return listenUnixSocket(server, socketPath);
				}));
				return operation();
			}).finally(() => { session.pending.delete(pending); });
			session.pending.add(pending);
			return pending;
		};
		return {
			ownership: session.ownership,
			computationDependencies: () => Object.freeze([...session.computations]),
			executionBindings: () => Object.freeze([...session.executionBindings].sort(([left], [right]) => left - right).map(([, binding]) => binding)),
			executor: { execute: (request) => execute("tool", () => this.executeTopLevel(session, request)) },
			executeBinding: (binding) => execute("operation", () => this.executeBinding(session, binding)),
			metrics: () => Object.freeze({ ...session.metrics }),
			seal: (changes) => {
				session.sealPromise ??= this.withProducer(() => this.seal(session, changes));
				return session.sealPromise;
			},
			validate: () => validateTransferredProcessEvidence(session.topLevelEvidence, session.incompleteReasons),
			close: () => session.closing ??= Promise.resolve().then(async () => {
				controller.abort(new Error("Linux process session closed"));
				await dispatch?.catch(() => undefined);
				try { await closeServer(server); } finally {
					await Promise.allSettled(session.pending);
					await rm(socketPath, { force: true }).catch(() => undefined);
				}
			}).finally(() => { this.producers--; }),
		};
	}

	dispose(): Promise<void> {
		if (this.disposal) return this.disposal;
		this.disposed = true;
		this.handoffs.dispose();
		return this.disposal = this.resetActorReplay().finally(() => this.replayWorkspace.dispose());
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
			resolveHostExecutable(this.options.sandlockBinary, "pi-speculative-sandlock", [
				path.join(os.homedir(), ".local", "bin", "pi-speculative-sandlock"),
			]),
			resolveHostExecutable(this.options.straceBinary, "strace", [path.join(os.homedir(), ".local", "bin", "pi-speculative-strace")]),
			resolveLinuxExecHelper(this.options.heldExecBinary),
		]);
		const [sandlockCheck, sandlockVersion, straceVersion, platformFingerprint] = await Promise.all([
			execText(sandlock, ["check"]),
			execText(sandlock, ["--version"]),
			execText(strace, ["-V"]),
			this.resolvePlatformFingerprint(),
		]);
		if (!sandlockCheck.includes("Status:         OK")) throw new Error("Sandlock kernel protections are unavailable");
		const imageLibrary = await execText(strace, ["--kill-on-exit", "-f", "-q", "-e", "trace=none", "-o", "/dev/null",
			`--handoff-library=${dispatcher}.so`, "--handoff-image=/dev/null", "--", "/bin/true"])
			.then(() => `${dispatcher}.so`, () => undefined);
		const mountProbe = await mkdtemp(path.join(os.tmpdir(), "pi-process-view-probe-"));
		let executionContext: ProcessExecutionContext | undefined;
		try {
			const logicalRoot = path.join(mountProbe, "logical");
			const physicalRoot = path.join(mountProbe, "physical");
			await Promise.all([mkdir(logicalRoot), mkdir(physicalRoot)]);
			executionContext = await probeExecutionContext({ sandlock, strace, dispatcher, logicalRoot, physicalRoot });
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
			executionContext, dispatcher, ...(imageLibrary ? { imageLibrary } : {}),
		};
	}

	private async executeTopLevel(session: ActiveSession, request: ProcessExecutionRequest): Promise<{ exitCode: number | null }> {
		if (session.closing) throw new Error("Linux process session is closed");
		const signal = AbortSignal.any([session.signal, ...(request.signal ? [request.signal] : [])]);
		signal?.throwIfAborted();
		const ready = await this.resolveReady();
		assertInvocationMatches(session.invocation, request);
		const invocationCwd = path.resolve(request.cwd);
		if (!pathContains(session.sourceRoot, invocationCwd)) throw new Error("process cwd escapes source workspace");
		const physicalCwd = session.projection.toPhysical(invocationCwd);
		if (!physicalCwd || !pathContains(session.workspace.sandboxRoot, physicalCwd)) throw new Error("process cwd is unmapped");
		const environment = definedProcessEnvironment(request.environment);
		const command = request.command;
		const logicalCwd = session.projection.toLogical(physicalCwd);
		const prototype = await topLevelProcessPrototype(
			session.invocation, request, environment, session.projection, ready.platformFingerprint,
		);
		this.add(session, "wholeCommandRequests");
		const plan = await this.plan(
			processWeakKey(prototype),
			prototype.executablePath,
			session.projection,
			(candidate) => compatibleProducer(session.producer, candidate),
			session,
		);
		signal?.throwIfAborted();
		if (plan?.kind === "completed_replay") return this.replayTopLevel(session, plan, request);
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
					onOutputEndpoints: (endpoints) => { session.topLevelOutputEndpoints = endpoints; },
					...(session.invocation.commandTransport === "stdin" ? { stdin: Buffer.from(command, "utf8") } : {}),
					signal,
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
					interposedExecutables: session.interposition.executables, interpositionInterpreter: process.execPath,
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
		signal?.throwIfAborted();
		return { exitCode: outcome.signal ? null : outcome.code };
	}

	private async seal(
		session: ActiveSession,
		changes: readonly SandboxWorkspaceChange[],
	): Promise<readonly SandboxWorkspaceChange[]> {
		const refined = await sealSessionEvidence(session, changes);
		try {
			await this.publishTopLevel(session, refined);
		} catch (error) {
			this.setError(session, `top_publish:${errorMessage(error)}`);
		}
		return refined;
	}

	private async replayTopLevel(
		session: ActiveSession,
		plan: Extract<ProcessReusePlan, { kind: "completed_replay" }>,
		request: ProcessExecutionRequest,
	): Promise<{ exitCode: number | null }> {
		const replayStarted = performance.now();
		const before = await session.workspace.structure.capture();
		await replayFilesystemEffects(this.replayWorkspace, plan.artifacts, plan.certificate.result.journal, session.projection, session.workspace.sandboxRoot);
		const after = await session.workspace.structure.capture();
		session.nestedEvidence.push(plan.certificate.dependencyCertificate);
		session.topLevelCapture = {
			before,
			after,
			observation: { complete: true, paths: [], taints: [], tracedProcesses: 0, incompleteReasons: [] },
		};
		for (const event of loadOutputEvents(plan.artifacts, plan.certificate.result.journal)) request.onData(event.data);
		this.add(session, "wholeCommandReplayMs", Math.max(0, performance.now() - replayStarted));
		this.add(session, "wholeCommandReusedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
		this.add(session, "wholeCommandHits");
		session.computations.push(reusedComputation(plan.certificate.result, replayStarted));
		return { exitCode: plan.certificate.result.exit?.kind === "code" ? plan.certificate.result.exit.code : null };
	}

	private async publishTopLevel(session: ActiveSession, changes: readonly SandboxWorkspaceChange[]): Promise<void> {
		const execution = session.topLevelExecution;
		const evidence = session.topLevelEvidence;
		if (!execution || !evidence) return;
		const certificate = sealProcessCertificate({
			prototype: execution.prototype,
			producer: session.producer,
			dependencyCertificate: evidence,
			result: await captureProcessResult(this.store, execution.outcome, execution.observedProcessMs,
				changes.map(change => ({ logicalPath: slash(path.resolve(session.sourceRoot, change.resource)), change }))),
		});
		if (await this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS)) this.add(session, "wholeCommandPublished");
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
			const pending = Promise.resolve().then(() => this.handleWireRequest(session, body))
				.then((response) => { socket.end(JSON.stringify(response)); })
				.catch((error) => {
					this.setError(session, errorMessage(error));
					session.incompleteReasons.add(`broker:${errorMessage(error)}`);
					socket.end(JSON.stringify({ kind: "failed" }));
				}).finally(() => { session.pending.delete(pending); });
			session.pending.add(pending);
		});
	}

	private async handleWireRequest(session: ActiveSession, body: string): Promise<DispatcherResponse> {
		const received = parseDispatcherRequest(body);
		if (!received || received.token !== session.token || session.closing) throw new Error("invalid dispatcher request");
		session.signal?.throwIfAborted();
		const request = materializeDispatcherRequest(session, received);
		if (!request) throw new Error("dispatcher cwd is unmapped");
		this.add(session, "requests");
		const requestID = session.metrics.requests;
		const ready = await this.resolveReady();
		const executable = await this.resolveRequestedExecutable(session, request);
		const eligibility = await eligibleRequest(session, request, ready.executionContext);
		if ("reason" in eligibility) {
			this.add(session, "bypasses");
			const reason = `broker_bypass:${request.name}:${eligibility.reason}`;
			if (request.pid === undefined) session.incompleteReasons.add(reason);
			else session.bypasses.push([request.pid, reason]);
			return { kind: "bypass", executable };
		}
		const { argv0, args, cwd, environment } = request;
		return this.executeRequest(session, { argv0, args, cwd, environment }, executable, eligibility.route, requestID);
	}

	private async executeBinding(session: ActiveSession, binding: ProcessExecutionBinding) {
		const invocation = this.handoffs.resolveBinding(binding, session.scope);
		if (!invocation || "trackingOnly" in invocation || invocation.sourceRoot !== session.sourceRoot ||
			invocation.producer && !compatibleProducer(session.nestedProducer, invocation.producer))
			throw new Error("process execution binding is unavailable in this scope");
		const cwd = session.projection.toPhysical(invocation.cwd), executable = session.projection.toPhysical(invocation.executable);
		if (!cwd || !executable) throw new Error("bound process paths are unmapped");
		const request = { ...invocation, cwd };
		const prototype = await this.prototype(session, request, executable, invocation.outputRoute);
		if (processWeakKey(prototype) !== binding.key) throw new Error("bound process execution context changed");
		this.add(session, "requests");
		const result = await this.executeRequest(session, request, executable, invocation.outputRoute, session.metrics.requests, prototype,
			capture => { session.topLevelCapture = { ...capture,
				observation: { complete: true, paths: [], taints: [], tracedProcesses: 0, incompleteReasons: [] } }; });
		return { output: (result.output ?? []).map(({ fd, data }) => ({ fd, data: Buffer.from(data, "base64") })),
			...(result.kind === "suspended" ? { suspended: true as const } : { exit: result.exit! }) };
	}

	private async executeRequest(session: ActiveSession, request: ProcessArguments, executable: string, outputRoute: OutputRoute,
		requestID: number, prototype?: ExecPrototype,
		captureWorkspace?: (capture: Omit<TopLevelCapture, "observation">) => void): Promise<DispatcherResponse> {
		prototype ??= await this.prototype(session, request, executable, outputRoute);
		const weakKey = processWeakKey(prototype);
		const acquired = await this.acquireProcessResult(
			weakKey,
			(live, excluded) => this.plan(weakKey, prototype.executablePath, session.projection, (candidate) => compatibleProducer(session.nestedProducer, candidate), session, live, excluded),
			session.signal,
			session.scope,
			{ ownership: session.ownership, executablePath: prototype.executablePath },
		);
		if (acquired.plan?.kind === "completed_replay") {
			const before = captureWorkspace ? await session.workspace.structure.capture() : undefined;
			const result = await this.replay(session, acquired.plan, weakKey, acquired);
			if (before) captureWorkspace!({ before, after: await session.workspace.structure.capture() });
			const binding = acquired.producer?.binding;
			if (binding && this.handoffs.resolveBinding(binding, session.scope)) session.executionBindings.set(requestID, binding);
			return result;
		}
		if (!acquired.work) throw new Error("process work reservation failed");
		this.add(session, "misses");
		try {
			return await this.executeAndPublish(session, request, executable, prototype, weakKey, outputRoute, acquired.work, requestID, captureWorkspace);
		} finally {
			this.handoffs.complete(weakKey, acquired.work);
			const computation = acquired.work.computation;
			if (computation) session.computations.push({ computation, shared: [computation] });
		}
	}

	private async acquireProcessResult(
		weakKey: Sha256Digest,
		lookup: ProcessHandoffLookup<ReadyProcessPlan>,
		signal: AbortSignal | undefined,
		scope: ExecutionScope | undefined,
		participant: { readonly timing: ServiceTimingIdentity } | { readonly ownership: ProcessHandoffOwnership; readonly executablePath: string },
	): Promise<{ readonly plan?: ReadyProcessPlan; readonly work?: ProcessHandoff; readonly producer?: ProcessHandoff; readonly continuation?: ProcessContinuation;
		readonly waiting?: readonly TimelineInterval[]; readonly joined: boolean; readonly waitedMs: number }> {
		let waitedMs = 0;
		const waits: { readonly handoff: ProcessHandoff; readonly interval: TimelineInterval }[] = [];
		let admission = "timing" in participant ? this.processScheduler.assessCandidateJoin({ identity: participant.timing, state: "succeeded" }) : undefined;
		if (admission && !admission.allowed) return { joined: false, waitedMs };
		const acquired = await this.handoffs.acquire({
			key: weakKey,
			scope,
			lookup,
			...("timing" in participant ? {
				role: "actor" as const,
				waitForRunning: async (running: ProcessHandoff) => {
					if (!running.ownership.acceptsScope(running.scope, scope)) return "miss";
					admission = this.processScheduler.assessCandidateJoin({
						identity: participant.timing, state: "running",
						elapsedMs: Math.max(0, performance.now() - running.startedAt),
					});
					if (!admission.allowed) return "miss";
					const check = running.inputsChanged;
					if (check) {
						const started = performance.now();
						try { if (await check()) return "rejected"; }
						finally { this.addActor("validationMs", Math.max(0, performance.now() - started)); }
					}
					const waitStarted = performance.now();
					const waiting = new AbortController(), stop = signal ? AbortSignal.any([signal, waiting.signal]) : waiting.signal;
					const completion = running.suspend ? running.suspend(stop).then(() => running.completion) : running.completion;
					const finished = await waitForCandidate(completion, signal, admission.waitBudgetMs).finally(() => waiting.abort());
					const interval = new TimelineInterval(waitStarted, performance.now());
					waits.push({ handoff: running, interval });
					waitedMs += interval.completedAt - interval.startedAt;
					if (finished.status === "completed") return "completed";
					signal?.throwIfAborted();
					return "miss";
				},
			} : { role: "producer" as const, ownership: participant.ownership, executablePath: participant.executablePath }),
		});
		return {
			...(acquired.kind === "hit" ? { plan: acquired.plan, producer: acquired.producer, continuation: acquired.continuation,
				waiting: waits.filter(wait => wait.handoff === acquired.producer).map(wait => wait.interval) } : {}),
			...(acquired.kind === "work" ? { work: acquired.work } : {}),
			joined: acquired.joined,
			waitedMs,
		};
	}

	private async plan(
		weakKey: Sha256Digest,
		executablePath: string,
		projection: ExecutionPathProjection,
		acceptProducer: (producer: ProcessProducerProof) => boolean,
		session?: ActiveSession,
		live?: readonly ProcessProvenanceCertificate[],
		excludedCertificates?: ReadonlySet<Sha256Digest>,
		continuation = false,
	): Promise<ReadyProcessPlan | undefined> {
		const plan = await this.planner.plan({
			weakKey,
			executablePath,
			acceptProducer,
			excludedCertificates,
			contract: {
				sink: "buffered",
				orderedJournal: true,
				transactionalEffects: true,
				...(continuation ? { continuation: true as const } : {}),
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
			else this.setActorError(`actor_${detail}`);
		}
		return plan.kind !== "miss" ? plan : undefined;
	}

	private recordLookup(lookup: ProcessReusePlan["lookup"], session?: ActiveSession): void {
		const record = (metric: CountedReuseMetric, value: number) => {
			if (session) this.add(session, metric, value);
			else this.addActor(metric, value);
		};
		record("validationMs", lookup.durationMs);
		record("validationCandidates", lookup.candidateCertificates);
		record("validationPathsets", lookup.pathsetsValidated);
		record("validationFilesRead", lookup.filesRead);
		record("validationBytesRead", lookup.bytesRead);
		record("validationArtifactsLoaded", lookup.artifactsLoaded);
		record("validationArtifactBytesRead", lookup.artifactBytesRead);
	}

	private async actorReplayMiss(host: ProcessExecutor, request: ProcessExecutionRequest, timing?: ServiceTimingIdentity): Promise<ProcessExecutionResult> {
		this.addActor("wholeCommandMisses");
		const started = performance.now();
		try {
			return await host.execute(request);
		} finally { if (timing && !request.signal?.aborted) this.processScheduler.observeActorService(timing, Math.max(0, performance.now() - started)); }
	}

	private async decideHeldExec(process: HeldExecProcess, scope?: ExecutionScope): Promise<HeldExecDecision> {
		const observation = this.observations.getStore();
		let learning = observation?.learn && !observation.closed && sameScope(observation.scope, scope);
		const order = observation ? ++observation.sequence : 0;
		const requestStarted = performance.now();
		this.addActor("requests");
		try {
			process.signal?.throwIfAborted();
			const sourceRoot = path.resolve(process.sourceRoot);
			const executable = await realpath(`/proc/${process.pid}/exe`);
			const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
			const executablePath = projection.toLogical(executable);
			if (learning && process.trackQueues) {
				// An acquisition hint lives in the existing bounded binding owner, never in a prediction or result cache.
				this.handoffs.observe(sha256Digest(`queue-tracking:${sourceRoot}`), executablePath, scope!, { trackingOnly: true, sourceRoot }, 0);
				this.addActor("misses"); return { kind: "continue" };
			}
			// A call learns each distinct launch once, and at most LEARNED_LAUNCHES of them: an exec-dense loop or build
			// would otherwise pay a held inspection and a whole-executable digest on every exec.
			if (learning) {
				const launch = `${executablePath}\0${await readFile(`/proc/${process.pid}/cmdline`, "latin1")}`, learned = observation!.learned;
				if (learning = learned.size < LEARNED_LAUNCHES && !learned.has(launch)) learned.add(launch);
			}
			const available = this.handoffs.mayHaveExecutable(executablePath) || await this.store.mayHaveCertificates(executablePath) ||
				this.handoffs.mayHaveExecutable(executablePath);
			if (!learning && !available) {
				this.addActor("misses");
				return { kind: "continue" };
			}
			const inspected = await inspectHeldExecProcess(process.pid, executable, process.descriptors);
			const resources = process.descriptors?.length
				? await captureHeldDescriptorInputs(process.pid, process.descriptors, Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes),
					sensitivePaths(this.options.storeRoot, this.options.deniedPaths), observation?.closed ? undefined : observation?.inputs, process.tracerPid, sourceRoot) : undefined;
			const snapshot = { ...inspected, ...(resources ? { resources } : {}) };
			if (!pathContains(sourceRoot, snapshot.cwd)) {
				this.addActor("bypasses");
				return { kind: "continue" };
			}
			const observe = (prototype: ExecPrototype, durationMs: number) => {
				const weakKey = processWeakKey(prototype);
				this.processScheduler.observeActorService(processTimingIdentity(prototype, weakKey), durationMs);
				if (!learning || observation!.closed || process.signal?.aborted || !snapshot.outputRoute ||
					observation!.bindings.size >= this.store.limits.maxCertificates) return;
				const binding = this.handoffs.observe(weakKey, executablePath, scope!, {
					argv0: snapshot.argv[0]!, args: snapshot.argv.slice(1), environment: snapshot.environment,
					cwd: projection.toLogical(snapshot.cwd), executable: executablePath, sourceRoot, outputRoute: snapshot.outputRoute,
					...(snapshot.outputPipes?.some(Boolean) ? { outputPipes: snapshot.outputPipes } : {}),
					...(snapshot.context.descriptorTypes[0] === "closed" ? { closeStdin: true } : {}),
					...(resources ? { resources } : {}),
				}, durationMs);
				if (binding) observation!.bindings.set(order, binding);
			};
			if (!available || resources && process.descriptors!.some(({ owned }) => !owned)) {
				// Pin the actual image before resuming it. Learning must not wait for a large digest after a short native call.
				const platform = await this.resolvePlatformFingerprint(), controller = new AbortController();
				let pinned!: () => void, digest: Sha256Digest | undefined;
				const ready = new Promise<void>(resolve => { pinned = resolve; });
				const capturing = hashExecutableFile(`/proc/${process.pid}/exe`, { pinned,
					signal: process.signal ? AbortSignal.any([process.signal, controller.signal]) : controller.signal,
				}).then(value => { digest = value; }, () => {}).finally(pinned);
				await ready;
				this.addActor("misses");
				return { kind: "continue", observeCompletion: async durationMs => {
					if (!digest || durationMs === undefined) controller.abort();
					await capturing;
					if (durationMs !== undefined && digest) observe(bufferedProcessPrototype(snapshot, projection, digest, platform), durationMs);
				} };
			}
			const prototype = bufferedProcessPrototype(
				snapshot, projection, await hashExecutableFile(`/proc/${process.pid}/exe`),
				await this.resolvePlatformFingerprint(),
			);
			const weakKey = processWeakKey(prototype);
			const timing = processTimingIdentity(prototype, weakKey);
			const accepted = (producer: ProcessProducerProof) =>
				actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
			const acquired = await this.acquireProcessResult(
				weakKey,
				(live, excluded) => this.plan(weakKey, executablePath, projection, accepted, undefined, live, excluded, true),
				process.signal,
				scope,
				{ timing },
			);
			const plan = acquired.plan;
			const continuation = acquired.continuation;
			if (!plan || (plan.certificate.result.continuation ? !continuation || sha256Digest(continuation.image) !== plan.certificate.result.continuation.imageDigest :
				plan.certificate.result.exit.kind !== "code")) {
				this.addActor("misses");
				return {
					kind: "continue",
					observeCompletion: durationMs => { if (durationMs !== undefined) observe(prototype, durationMs); },
				};
			}
			const output = loadOutputEvents(plan.artifacts, plan.certificate.result.journal);
			return {
				kind: "replay",
				...(continuation ? { continuation } : { exitCode: (plan.certificate.result.exit as Extract<ExitOutcome, { kind: "code" }>).code }),
				output,
				...(plan.certificate.result.resources?.transitions ? { resourceEvents: plan.certificate.result.resources.transitions.map(event =>
					({ fd: event.id, kind: event.kind, data: plan.artifacts.read(event.data), ...(event.requested !== undefined ? { requested: event.requested } : {}) })) } : {}),
				...(resources ? { descriptorOffsets: descriptorInputs(resources).map(input => {
					const descriptor = process.descriptors!.find(({ fd }) => fd === input.fd)!;
					const effects = plan.certificate.result.resources!;
					const ofd = effects.descriptions.find(effect => effect.id === input.alias)!;
					const object = effects.objects.find(effect => effect.id === input.image)!;
					return { fd: input.fd, before: input.offset, after: object.consumed ?? ofd.position?.after ?? 0,
						device: descriptor.device, inode: descriptor.inode, flags: descriptor.flags, afterFlags: ofd.flags,
						...(input.type === "directory" ? { path: input.sourcePath!, ...(!(input.flags & 0x200000) && resources.objects[input.image]!.content !== undefined ?
							{ content: Buffer.from(resources.objects[input.image]!.content!, "base64") } : {}) } : {}),
						...(input.type === "eventfd" ? { event: descriptor.counter!.id + 1, content: Buffer.from(resources.objects[input.image]!.content!, "base64") } : {}),
						...(input.type === "pipe" || input.type === "socket" ? { content: Buffer.from(resources.objects[input.image]!.content!, "base64"), eof: resources.objects[input.image]!.queue!.eof,
							capacity: descriptor.capacity, socket: descriptor.socket, messages: descriptor.messages }
							: input.fd === input.image && object.content ? { content: plan.artifacts.read(object.content) } : {}) };
				}) } : {}),
				commit: async () => {
					const started = performance.now();
					try {
						process.signal?.throwIfAborted();
						await replayFilesystemEffects(this.replayWorkspace, plan.artifacts, plan.certificate.result.journal, projection, sourceRoot);
					} catch (error) {
						this.setActorError(`actor_child_commit:${errorMessage(error)}`);
						throw error;
					} finally {
						this.addActor("replayMs", Math.max(0, performance.now() - started));
					}
				},
				adopted: () => {
					const binding = acquired.producer?.binding;
					if (observation && !observation.closed && sameScope(observation.scope, scope)) {
						if (binding && this.handoffs.resolveBinding(binding, scope) && observation.bindings.size < this.store.limits.maxCertificates)
							observation.bindings.set(order, binding);
						observation.computations.push(reusedComputation(plan.certificate.result, requestStarted, acquired));
					}
					this.recordHit(acquired.producer?.scope, acquired.joined, undefined, scope);
					this.processScheduler.observeAdoption(timing, Math.max(0, performance.now() - requestStarted - acquired.waitedMs));
					this.addActor("reusedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
					if (scope) acquired.producer?.ownership.adopted({ scope, id: process.id,
						sequence: process.sequence, operationIdentity: weakKey });
				},
			};
		} catch (error) {
			this.addActor("bypasses");
			this.setActorError(`actor_child:${errorMessage(error)}`);
			return { kind: "continue" };
		}
	}

	private async replay(
		session: ActiveSession,
		plan: Extract<ProcessReusePlan, { kind: "completed_replay" }>,
		weakKey: Sha256Digest,
		acquired: { readonly joined: boolean; readonly producer?: ProcessHandoff; readonly waiting?: readonly TimelineInterval[] },
	): Promise<DispatcherResponse> {
		const started = performance.now();
		let replayed = false;
		try {
			const { artifacts, certificate } = plan;
			const output = wireOutput(loadOutputEvents(artifacts, certificate.result.journal));
			await replayFilesystemEffects(this.replayWorkspace, artifacts, certificate.result.journal, session.projection, session.workspace.sandboxRoot);
			session.nestedEvidence.push(certificate.dependencyCertificate);
			this.recordHit(acquired.producer?.scope, acquired.joined, session);
			session.computations.push(reusedComputation(certificate.result, started, acquired));
			replayed = true;
			return { kind: "hit", weakKey, output, exit: certificate.result.exit };
		} finally {
			this.add(session, "replayMs", Math.max(0, performance.now() - started));
			const observed = plan.certificate.result.observedProcessMs;
			if (replayed && observed !== undefined) this.add(session, "reusedProcessMs", observed);
		}
	}

	private async executeAndPublish(
		session: ActiveSession,
		request: ProcessArguments,
		executable: string,
		prototype: ExecPrototype,
		weakKey: Sha256Digest,
		outputRoute: OutputRoute,
		work: ProcessHandoff,
		requestID: number,
		captureWorkspace?: (capture: Omit<TopLevelCapture, "observation">) => void,
	): Promise<DispatcherResponse> {
		const ready = await this.resolveReady();
		const started = performance.now();
		const transaction = await session.workspace.transactions.begin();
		let traceRoot: string | undefined;
		let descriptorReport: Awaited<ReturnType<typeof open>> | undefined;
		const inheritedFiles: Awaited<ReturnType<typeof open>>[] = [];
		let outcome: SpawnOutcome | undefined;
		let transactionFinishing = false;
		let releaseInputs: (() => void) | undefined, inputCheck: Promise<boolean> | undefined;
		let stage = "capture", dependencyCertificate: DynamicDependencyCertificate | undefined, certificateID: Sha256Digest | undefined;
		let continuation: ProcessContinuation | undefined, frozen: { pid: number; fd: number; syscall: string; bytes: number } | undefined;
		let suspensionAttempted = false;
		const failureDetail = (error: unknown) => `${errorMessage(error)}; process=${JSON.stringify({
			stage, requestID, weakKey, scope: session.scope, workspace: session.workspace.sandboxRoot,
			executable: prototype.executablePath, certificateID, complete: dependencyCertificate?.complete, taints: dependencyCertificate?.taints,
		})}`;
		try {
			traceRoot = await mkdtemp(path.join(session.workspace.processRoot, "trace-"));
			const tracePrefix = path.join(traceRoot, "process");
			const logicalExecutable = session.projection.toLogical(executable);
			const logicalCwd = session.projection.toLogical(request.cwd);
			let descriptorManifest: string | undefined, descriptorReportPath: string | undefined;
			const directoryImages: Array<readonly [string, string]> = [];
			const inputs = descriptorInputs(request.resources);
			const outputPipes = request.outputPipes?.some(Boolean);
			const live = !!captureWorkspace && !!ready.imageLibrary && inputs.every(input => input.installed !== false) &&
				inputs.some(input => input.type === "eventfd" || (input.type === "pipe" || input.type === "socket") && !request.resources!.objects[input.image]!.queue!.eof);
			const resourceJournal = live || inputs.some(input => !input.type || input.type === "eventfd" || input.type === "socket" || input.type === "pipe" && (input.flags & 3) !== 0);
			const streamIdentity = (position: { fd: number; inode: string }) => {
				const input = inputs.find(input => input.fd === position.fd)!;
				return !input.type ? `file:${position.inode}` : input.type === "eventfd" ? `eventfd:${input.image}` : position.inode;
			};
			const descriptorImages = new Map<number, { physical: string; logical: string; workspace: boolean; state: import("node:fs").BigIntStats }>();
			if (inputs.length || outputPipes) {
				descriptorManifest = path.join(traceRoot, "fd-inputs");
				descriptorReportPath = path.join(traceRoot, "fd-offsets");
				descriptorReport = await open(descriptorReportPath, "wx+", 0o600);
				let manifest = `INPUTS ${inputs.length} ${Number(!!request.closeStdin)} ${Number(resourceJournal)}\n`;
				for (const descriptor of inputs) {
					if (descriptor.fd === descriptor.image && descriptor.type !== "null") {
						const workspace = !!descriptor.sourcePath && pathContains(session.sourceRoot, descriptor.sourcePath);
						const physical = workspace ? session.projection.toPhysical(descriptor.sourcePath!)! : path.join(traceRoot, `fd-${descriptor.image}`);
						let state: import("node:fs").BigIntStats;
						if (descriptor.type === "directory") {
							if (!workspace) throw new Error("inherited directory is outside the workspace");
							await assertNoSymlinkPath(session.workspace.sandboxRoot, physical);
							state = await lstat(physical, { bigint: true });
							if (!state.isDirectory()) throw new Error("inherited directory predecessor changed");
							if (descriptor.content !== undefined) {
								const raw = path.join(traceRoot, `directory-${descriptor.image}`);
								await writeFile(raw, Buffer.from(descriptor.content, "base64"), { flag: "wx", mode: 0o600 });
								directoryImages.push([physical, raw]);
							}
						} else if (workspace) {
							await assertNoSymlinkPath(session.workspace.sandboxRoot, physical);
							const captured = await captureStableFile(physical, MAX_REQUEST_BYTES);
							if (`sha256:${captured.hash}` !== descriptor.contentDigest || captured.stat.nlink !== BigInt(descriptor.sourceAliases?.length ?? 1)) throw new Error("inherited FD predecessor changed");
							for (const alias of descriptor.sourceAliases ?? []) {
								const target = session.projection.toPhysical(alias)!;
								await assertNoSymlinkPath(session.workspace.sandboxRoot, target);
								if (!sameFilesystemIdentity(captured.stat, await lstat(target, { bigint: true }))) throw new Error("inherited FD alias changed");
							}
							state = captured.stat;
						} else {
							await writeFile(physical, Buffer.from(descriptor.content!, "base64"), { flag: "wx", mode: 0o600 });
							state = await lstat(physical, { bigint: true });
						}
						descriptorImages.set(descriptor.image, { physical, logical: workspace ? descriptor.sourcePath! : physical, workspace, state });
					}
					const image = descriptor.fd === descriptor.alias ? descriptor.type === "null" ? "/dev/null" : descriptorImages.get(descriptor.image)!.logical : "";
					if (descriptor.fd === descriptor.alias && (descriptor.flags & 0x200000 /* O_PATH */))
						inheritedFiles.push(await open(descriptor.type === "null" ? "/dev/null" : descriptorImages.get(descriptor.image)!.physical, descriptor.flags));
					const object = request.resources!.objects[descriptor.image]!;
					const stream = object.counter ? 6 : object.socket ? object.socket.peer.connected ? 4 : 5 : object.queue ? (descriptor.flags & 3) === 1 ? 3 : object.queue.eof ? 1 : 2 : 0;
					manifest += `${descriptor.fd} ${descriptor.alias} ${descriptor.flags} ${descriptor.offset} ${Buffer.byteLength(image)} ${stream} ${object.queue?.capacity ?? 0} ${object.socket?.shutdown ?? 0} ${object.socket?.peer.shutdown ?? 0} ${object.socket?.peer.object ?? -1} ${descriptor.outside ?? object.queue?.outside ?? 3} ${Number(descriptor.installed !== false)} ${object.socket ? object.socket.type ?? 1 : 0}\n${image}\n`;
				}
				for (const [image, object] of Object.entries(request.resources!.objects)) for (const message of object.queue?.messages ?? [])
					manifest += `M ${image} ${message.start} ${message.end} ${message.rights.length} ${message.rights.join(" ")}\n`;
				for (const descriptor of inputs) if (descriptor.fd === descriptor.alias) for (const lock of descriptor.locks ?? [])
					manifest += `L ${descriptor.fd} ${lock.kind} ${lock.type} ${lock.start} ${lock.length}\n`;
				await writeFile(descriptorManifest, manifest, { flag: "wx", mode: 0o600 });
			}
			const changedInput = async () => {
				const changes = session.workspace.sourceChanges?.();
				if (!changes?.paths.length || !transaction.readBefore) return false;
				let inputs: Set<string> | undefined;
				for (const changed of changes.paths) {
					const relative = relativeFilesystemPath(session.sourceRoot, changed);
					if (relative === undefined || session.deniedPaths.some(denied => pathContains(denied, changed))) continue;
					inputs ??= new Set((await observeStrace(tracePrefix, logicalExecutable, logicalCwd, { previewBytes: 1024 * 1024 }))
						.paths.filter(observed => observed.role === "input").map(observed => path.resolve(observed.path)));
					if (!inputs.has(changed)) continue;
					const before = await transaction.readBefore(slash(relative), MAX_REQUEST_BYTES);
					if (!before) continue;
					// Only observed inputs need comparison; the transaction prestate includes predecessor effects.
					await assertNoSymlinkPath(session.sourceRoot, changed);
					const current = await captureStableFile(changed, MAX_REQUEST_BYTES);
					this.addActor("validationFilesRead", current.shared ? 0 : 1);
					this.addActor("validationBytesRead", current.shared ? 0 : current.bytesRead);
					if (sha256Digest(before) === `sha256:${current.hash}`) continue;
					this.setActorError(`actor_running_input_changed:${changed}`);
					return true;
				}
				return false;
			};
			releaseInputs = this.handoffs.observeInputs(weakKey, work, () => inputCheck ??=
				changedInput().catch(() => true /* an unproven check rejects the join */).finally(() => { inputCheck = undefined; }));
			const imagePath = path.join(traceRoot, "continuation");
			const command = straceCommand(ready.strace, tracePrefix, [
				ready.sandlock,
				...sandboxPolicyArguments(
					logicalCwd,
					session.deniedPaths,
					[session.workspace.sandboxRoot, ...(descriptorReportPath ? [descriptorReportPath] : []),
						...[...descriptorImages.values()].filter(image => !image.workspace).map(image => image.physical)],
					[{ virtualPath: session.sourceRoot, hostPath: session.workspace.sandboxRoot, readOnly: false }],
					[],
				),
				...inheritedFiles.flatMap((_, index) => ["--preserve-fd", String(index + 3)]),
				...directoryImages.flatMap(([directory, image]) => ["--directory-image", sandboxMountArgument({ virtualPath: directory, hostPath: image, readOnly: false })]),
				"--",
				ready.dispatcher,
				descriptorManifest ? "--exec-fds" : request.closeStdin ? "--exec-closed-input" : "--exec",
				outputRoute.join("") + (outputPipes ? request.outputPipes!.map(pipe => pipe ? "p" : "s").join("") : ""),
				...(descriptorManifest ? [descriptorManifest, descriptorReportPath!] : []),
				request.argv0,
				logicalExecutable,
				...request.args,
			], resourceJournal, live);
			const processStarted = performance.now();
			const clockOffset = Number(process.hrtime.bigint()) / 1e6 - performance.now();
			stage = "execution";
			outcome = await runSpawn(ready.strace, [...(live ? [`--handoff-fd=${inheritedFiles.length + 3}`, `--handoff-library=${ready.imageLibrary}`, `--handoff-image=${imagePath}`] : []), ...command.slice(1)], {
				cwd: request.cwd,
				environment: request.environment,
				signal: AbortSignal.any([session.signal, work.signal]),
				inheritedFiles: inheritedFiles.map(file => file.fd),
				...(live ? { onControl: (channel: import("node:stream").Duplex, wake: () => boolean) => {
					let suspended: Promise<void> | undefined;
					const release = this.handoffs.observeSuspension(weakKey, work, joinSignal => suspended ??= (async () => {
						const stop = AbortSignal.any([session.signal, work.signal, ...(joinSignal ? [joinSignal] : [])]);
						let pid = 0;
						// Only probe while an Actor already waits within its join budget. A CPU
						// prefix must be allowed to reach I/O instead of waiting forever for EOF.
						while (!stop.aborted) {
							const report = await readFile(descriptorReportPath!, "utf8").catch(() => ""), ready = /^RUNNING (\d+)\n$/.exec(report);
							if (ready) {
								pid = Number(ready[1]);
								const state = await readFile(`/proc/${pid}/syscall`, "utf8").catch(() => ""), syscall = Number(state.split(" ", 1)[0]);
								if (!state) return;
								if (IO_FRONTIERS.has(syscall)) break;
								if (syscall >= 0) return;
							} else if (report.startsWith("OFD ")) return;
							await delay(10, undefined, { signal: stop }).catch(() => undefined);
						}
						if (stop.aborted) return;
						suspensionAttempted = true;
						const reply = await requestProcessImage(pid, channel, wake, AbortSignal.any([session.signal, work.signal]));
						if (reply.readInt32LE(0) !== pid) return;
						const bytes = Number(reply.readBigUInt64LE(16)), begin = Number(reply.readBigUInt64LE(24)) / 1e6 - clockOffset,
							end = Number(reply.readBigUInt64LE(32)) / 1e6 - clockOffset, fd = reply.readInt32LE(4), syscall = IO_FRONTIERS.get(reply.readInt32LE(8));
						if (!syscall || !Number.isSafeInteger(bytes) || bytes <= 0 || fd < 0 || begin < processStarted || end < begin || end > performance.now()) throw new Error("invalid native continuation frontier");
						const file = await open(imagePath, "r");
						let image: Buffer;
						try {
							if ((await file.stat()).size > Math.min(MAX_CONTINUATION_BYTES, this.store.limits.maxBytes)) throw new Error("continuation exceeds retained resource budget");
							image = await file.readFile();
						} finally { await file.close(); }
						continuation = { image, physicalRoot: session.workspace.sandboxRoot, computation: new TimelineInterval(begin, end) };
						frozen = { pid, fd, syscall, bytes };
					})().catch(error => { this.setActorError(`actor_suspend:${errorMessage(error)}`); }).finally(() => {
						if (!suspensionAttempted) suspended = undefined;
					}));
					return () => { release(); return suspended; };
				} } : {}),
			});
			// Replays write each event to the target's own descriptor, whichever outlet this route gave it.
			outcome = { ...outcome, output: outcome.output.map(event => ({ ...event, fd: outputRoute[0] === event.fd ? 1 : 2 })) };
			const observedProcessMs = continuation ? continuation.computation.completedAt - continuation.computation.startedAt : Math.max(0, performance.now() - processStarted);
			releaseInputs();
			try {
				if (suspensionAttempted && !continuation) throw new Error("private process suspension was not sealed");
				const descriptorOffsets = descriptorReport && inputs.length ? parseDescriptorOffsets(await descriptorReport.readFile(), inputs) : undefined;
				transactionFinishing = true;
				const captures = [
					transaction.finish(),
					observeStrace(tracePrefix, logicalExecutable, session.projection.toLogical(request.cwd), {
						...(frozen ? { frozen } : {}),
						guardFilesystemSemanticsWithin: [session.workspace.sandboxRoot, session.sourceRoot],
						inheritedDirectoryImages: directoryImages.flatMap(([physical]) => [physical, session.projection.toLogical(physical)]),
						inheritedFileImages: [...descriptorImages.values()].flatMap(image => [image.logical, image.physical])
							.concat(inputs.some(input => input.type === "null") ? ["/dev/null"] : [])
							.concat((descriptorOffsets ?? []).filter(position => inputs.find(input => input.fd === position.fd)!.type === "pipe").map(position => `pipe:[${position.inode}]`)),
						inheritedStreams: resourceJournal ? descriptorOffsets?.filter(position => {
							const input = inputs.find(input => input.fd === position.fd)!;
							return input.type === "socket" || input.type === "pipe" || input.type === "eventfd";
						}).map(streamIdentity) : undefined,
						inheritedHandles: resourceJournal ? descriptorOffsets?.flatMap(position => {
							const input = inputs.find(input => input.fd === position.fd)!, object = request.resources!.objects[input.image]!;
							return [{ fd: input.fd, installed: input.installed, description: input.alias, inode: streamIdentity(position), flags: input.flags, outside: input.outside ?? object.queue?.outside ?? 3, packet: !!object.socket && (object.socket.type ?? 1) !== 1,
								queuedBytes: object.queue?.bytes, ...(object.queue && input.fd === input.image ? { queueData: Buffer.from(object.content!, "base64"), messages: object.queue.messages } : {}) }];
						}) : undefined,
					}),
				] as const;
				const [delta, observation] = await Promise.all(captures).catch(async (error: unknown) => {
					// Both captures own live workspace/trace resources until they settle.
					stage = (await Promise.allSettled(captures)).flatMap((result, index) =>
						result.status === "rejected" ? [index === 0 ? "transaction_capture" : "trace_capture"] : []).join("+");
					throw error;
				});
				if (continuation) continuation = { ...continuation, image: bindContinuationDescriptors(continuation.image, frozen!, inputs,
					!!request.closeStdin, observation.finalHandles ?? [], descriptorOffsets!) };
				// A destroyed OFD has no observable final position. If it escaped into a
				// surviving message, its position instead needs a kernel observation.
				if (!continuation && inputs.some(input => input.outside === 0 && observation.retainedDescriptions?.includes(input.alias)))
					throw new Error("queued file description has no final position observation");
				if (observation.incompleteReasons.length) {
					this.setError(session, `trace:${observation.incompleteReasons.join(",")}`);
					for (const reason of observation.incompleteReasons) session.incompleteReasons.add(`nested_trace:${reason}`);
				}
				if (!delta.complete) {
					stage = "transaction_capture";
					this.setError(session, `transaction:${delta.reason}`);
					throw new Error(`workspace transaction is incomplete: ${delta.reason}`);
				}
				const { before, after } = delta;
				// A bound child is the entire execution interval; its transaction already sealed both endpoints.
				captureWorkspace?.({ before, after });
				stage = "workspace_effects";
				const effects = diffWorkspaceStructures(before, after, delta.changes, session.projection);
				stage = "dependencies";
				const evidence = await captureDependencies(
					session,
					transactionDependencySource(before, effects),
					observation.paths,
					effects.effects,
				);
				if (evidence.incompleteReasons.length) {
					this.setError(session, `evidence:${evidence.incompleteReasons.join(",")}`);
				}
				const taints = new Set<ProvenanceTaint>(observation.taints);
				// Private images preserve FD/OFD relations, but cannot also represent an independently accessed pathname.
				if (inputs.some(descriptor => !descriptorImages.get(descriptor.image)?.workspace && descriptor.sourcePath && observation.paths.some(observed =>
					path.resolve(observed.path) === descriptor.sourcePath || observed.role !== "metadata" && pathContains(observed.path, descriptor.sourcePath!)))) {
					taints.add("untracked_fd");
				}
				for (const taint of evidence.taints) taints.add(taint);
				if (!observation.complete) taints.add("trace_incomplete");
				dependencyCertificate = {
					complete: observation.complete && evidence.complete,
					dependencies: evidence.dependencies,
					taints: [...taints],
				};
				stage = "artifacts";
				const baseResult = await captureProcessResult(this.store, outcome, observedProcessMs, effects.effects);
				const finalObjects = new Map([...after.entries].flatMap(([name, entry]) => entry.kind === "file" && entry.object ? [[entry.object, name] as const] : []));
				if (descriptorOffsets) for (const position of descriptorOffsets) {
					const input = inputs.find(({ fd }) => fd === position.fd)!;
					if (input.type === "null" || input.type === "pipe" || input.type === "socket" || input.type === "eventfd" || input.fd !== input.image) continue;
					const image = descriptorImages.get(input.image)!;
					const finalName = image.workspace && !input.type ? finalObjects.get(`${position.device}:${position.inode}`) : undefined;
					if (position.detached) {
						const final = position.detached;
						if (final.mode !== image.state.mode || final.uid !== image.state.uid || final.gid !== image.state.gid ||
							!effects.complete) throw new Error("detached file object transition is incomplete");
						if (!finalName) {
							if (sha256Digest(final.content) !== input.contentDigest || final.modified !== image.state.mtimeNs) position.content = await this.store.artifacts.put(final.content);
							continue;
						}
						const event = baseResult.journal.find(event => event.kind === "workspace" && event.path === session.projection.toLogical(path.join(after.root, finalName)));
						if (sha256Digest(final.content) !== (event?.kind === "workspace" && event.after.kind === "file" ? event.after.data.digest : input.contentDigest)) throw new Error("file object and remaining aliases diverged");
					}
					const physical = finalName ? path.join(after.root, finalName) : image.physical;
					const current = await lstat(physical, { bigint: true });
					if ((input.type === "directory" ? !current.isDirectory() : !current.isFile()) || String(current.dev) !== position.device || String(current.ino) !== position.inode ||
						current.mode !== image.state.mode || current.uid !== image.state.uid || current.gid !== image.state.gid)
						throw new Error("inherited FD namespace changed during execution");
					if (input.type === "directory" && !sameFilesystemIdentity(current, image.state)) throw new Error("inherited directory changed during enumeration");
					// The common workspace object transaction owns every named inode write and namespace edge.
					if (image.workspace && !input.type) {
						if (!finalName || !effects.complete) throw new Error("inherited file object transition is incomplete");
						continue;
					}
					if (input.type === "directory" || sameFilesystemIdentity(current, image.state)) continue;
					const captured = await captureStableFile(physical, MAX_REQUEST_BYTES, true);
					if (`sha256:${captured.hash}` !== input.contentDigest || current.mtimeNs !== image.state.mtimeNs) {
						position.content = await this.store.artifacts.put(captured.content!);
					} else throw new Error("unmodeled inherited FD metadata effect");
				}
				const transitions: NonNullable<import("./provenance-certificate.ts").ProcessResourceEffects["transitions"]>[number][] = [];
				for (const event of observation.resourceJournal ?? []) {
					const input = inputs.find(input => streamIdentity(descriptorOffsets!.find(position => position.fd === input.fd)!) === event.inode &&
						(event.description !== undefined ? input.alias === event.description : event.kind === "produce" ? (input.flags & 3) !== 0 :
							!["consume", "peek"].includes(event.kind) || (input.flags & 3) !== 1));
					if (!input) throw new Error("unbound stream transition");
					transitions.push({ id: input.alias, kind: event.kind, data: await this.store.artifacts.put(event.data), ...(event.requested !== undefined ? { requested: event.requested } : {}) });
				}
				const { exit, ...prefixResult } = baseResult;
				const result: ProcessResultRecord = { ...prefixResult, ...(continuation ? { continuation: { imageDigest: sha256Digest(continuation.image), imageBytes: continuation.image.length } } : { exit: exit! }),
					...(descriptorOffsets ? { resources: { ...descriptorEffects(request.resources!, descriptorOffsets), ...(resourceJournal ? { transitions } : {}) } } : {}) };
				stage = "certificate";
				const certificate = sealProcessCertificate({ prototype, producer: session.nestedProducer, dependencyCertificate, result });
				certificateID = certificate.id;
				session.nestedEvidence.push(certificate.dependencyCertificate);
				if (taints.size) {
					this.add(session, "tainted");
					this.setError(session, `tainted:${[...taints].join(",")}`);
				}
				stage = "handoff_registration";
				if (await this.handoffs.publish(
					weakKey,
					work,
					certificate,
					() => {
						if (continuation) return Promise.resolve(false);
						const binding = this.handoffs.bind(weakKey, work, {
							argv0: request.argv0, args: request.args, environment: request.environment,
							cwd: logicalCwd, executable: logicalExecutable, sourceRoot: session.sourceRoot, outputRoute, producer: session.nestedProducer,
							...(request.outputPipes ? { outputPipes: request.outputPipes } : {}),
							...(request.closeStdin ? { closeStdin: true } : {}),
								...(request.resources ? { resources: request.resources } : {}),
						});
						if (binding) session.executionBindings.set(requestID, binding);
						stage = "history_publication";
						return this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS).catch((error: unknown) => {
							// Optional history storage cannot invalidate already sealed execution evidence.
							this.setError(session, `nested_publish:${failureDetail(error)}`);
							return false;
						});
					},
					continuation,
				)) {
					this.add(session, "published");
				}
			} catch (error) {
				// The process already ran. Certificate failure must never cause dispatcher fallback/re-execution.
				const detail = failureDetail(error);
				this.setError(session, `post_execution_capture:${detail}`);
				session.incompleteReasons.add(`nested_capture:${detail}`);
			}
			if (continuation) return { kind: "suspended", weakKey };
			const exit = exitOutcome(outcome);
			return { kind: "executed", weakKey, output: wireOutput(outcome.output), exit };
		} catch (error) {
			if (stage !== "capture" || captureWorkspace || session.signal.aborted || work.signal.aborted) throw error;
			const detail = failureDetail(error);
			this.add(session, "bypasses");
			this.setError(session, detail);
			session.incompleteReasons.add(`broker:${detail}`);
			return { kind: "bypass", executable };
		} finally {
			try { await Promise.all([descriptorReport?.close(), ...inheritedFiles.map(file => file.close())]); } catch (error) { this.setError(session, `descriptor_report_close:${errorMessage(error)}`); }
			releaseInputs?.();
			await inputCheck;
			const durationMs = Math.max(0, performance.now() - started);
			this.add(session, "executionMs", durationMs);
			if (outcome) this.processScheduler.observeSpeculativeService(processTimingIdentity(prototype, weakKey), durationMs);
			if (!transactionFinishing) await transaction.abort().catch(() => undefined);
			if (traceRoot) await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
		}
	}

	private add(session: ActiveSession, metric: CountedReuseMetric, value = 1): void {
		this.counters[metric] += value;
		session.metrics[metric] += value;
	}

	private addActor(metric: CountedReuseMetric, value = 1): void {
		this.counters[metric] += value;
		this.actorCounters[metric] += value;
	}

	private recordHit(
		producer: ExecutionScope | undefined,
		joined: boolean,
		session?: ActiveSession,
		scope: ExecutionScope | undefined = session?.scope,
	): void {
		const add = (metric: CountedReuseMetric) => session ? this.add(session, metric) : this.addActor(metric);
		add("hits");
		if (joined) add("joinedHits");
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

	private setActorError(detail: string): void {
		this.counters.lastError = detail;
		this.actorCounters.lastError = detail;
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
		request: ProcessArguments,
		executable: string,
		outputRoute: OutputRoute,
	): Promise<ExecPrototype> {
		const [executableDigest, ready] = await Promise.all([hashExecutableFile(executable), this.resolveReady()]);
		return bufferedProcessPrototype({
			executable,
			argv: [request.argv0, ...request.args],
			cwd: request.cwd,
			environment: request.environment,
			context: routedProcessContext(ready.executionContext, outputRoute, request.closeStdin, descriptorInputs(request.resources).filter(input => input.installed !== false), request.outputPipes),
			...(request.resources ? { resources: request.resources } : {}),
		}, session.projection, executableDigest, ready.platformFingerprint);
	}
}

function bufferedProcessPrototype(
	snapshot: HeldExecSnapshot,
	projection: ExecutionPathProjection,
	executableDigest: Sha256Digest,
	platformFingerprint: string,
): ExecPrototype {
	const { context } = snapshot;
	const inputs = descriptorInputs(snapshot.resources);
	const input = inputs.find(({ fd }) => fd === 0);
	return createExecPrototype({
		executablePath: projection.toLogical(snapshot.executable),
		executableDigest,
		argv: snapshot.argv.map((value) => projection.normalizeValue(value)),
		logicalCwd: projection.toLogical(snapshot.cwd),
		environment: Object.fromEntries(
			Object.entries(snapshot.environment).map(([name, value]) => [name, projection.normalizeValue(value)]),
		),
		umask: context.umask,
		processContextDigest: sha256Digest(context.key),
		stdin: input ? { type: "bytes", digest: input.contentDigest, eof: snapshot.resources!.objects[input.image]!.queue?.eof ?? true } : { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [...context.descriptorTypes.map((type, fd) => ({
			fd,
			type,
			flagsDigest: sha256Digest(`${context.key}\0${fd}`),
			...(fd === 0 ? { eof: true } : {}),
		})), ...inputs.filter(({ fd }) => fd > 2).map(({ fd }) => ({ fd, type: "regular" as const,
			flagsDigest: sha256Digest(`${context.key}\0${fd}`) }))].map(descriptor => {
			const input = inputs.find(({ fd }) => fd === descriptor.fd);
			return input ? { ...descriptor, flagsDigest: input.locks || input.outside !== undefined ? digestObject({ flags: input.flags, locks: input.locks, outside: input.outside }) : sha256Digest(String(input.flags)), ...(input.installed === false ? { installed: false as const } : {}), type: input.type ?? descriptor.type, alias: input.alias, object: input.image, contentDigest: input.contentDigest, offset: input.offset,
				eof: snapshot.resources!.objects[input.image]!.queue?.eof,
				...(snapshot.resources!.objects[input.image]!.queue ? { endpointDigest: digestObject({ queue: snapshot.resources!.objects[input.image]!.queue, socket: snapshot.resources!.objects[input.image]!.socket }) } : {}),
				...(input.sourcePath ? { resourcePath: projection.toLogical(input.sourcePath) } : {}),
				...(input.sourceAliases ? { resourceAliases: input.sourceAliases.map(name => projection.toLogical(name)).sort() } : {}) } : descriptor;
		}),
		platformFingerprint,
	});
}

function bindContinuationDescriptors(image: Buffer, frontier: { pid: number; fd: number }, inputs: ReturnType<typeof descriptorInputs>, closedInput: boolean,
	handles: NonNullable<import("./strace-observer.ts").StraceObservation["finalHandles"]>, positions: ReturnType<typeof parseDescriptorOffsets>) {
	let cursor = 0;
	const line = () => {
		const end = image.indexOf(10, cursor);
		if (end < 0 || end - cursor > 256) throw new Error("invalid continuation header");
		const text = image.toString("ascii", cursor, end); cursor = end + 1; return text;
	};
	const first = line(), header = /^PIIMAGE (\d+) (\d+) (\d+) (\d+)$/.exec(first);
	if (!header || Number(header[1]) !== frontier.pid || Number(header[2]) > 256) throw new Error("unbound continuation image");
	const records = new Map<number, string[]>(), bound: string[] = [], descriptions = new Map<number, string[]>();
	for (let index = 0; index < Number(header[2]); index++) {
		const row = line(), match = /^(\d+) \d+ \d+ \d+ \d+ \d+$/.exec(row), fd = Number(match?.[1]);
		if (!match || !Number.isSafeInteger(fd) || records.has(fd)) throw new Error("invalid continuation FD table");
		const fields = row.split(" "), handle = handles.find(handle => handle.fd === fd);
		if (Number(fields[5]) !== fd) throw new Error("continuation image was already bound");
		const input = handle?.description !== undefined ? inputs.find(input => input.alias === handle.description) : undefined;
		if (input) {
			const before = positions.find(position => position.fd === input.fd)!;
			if (fields[3] !== before.device || fields[4] !== before.inode || Boolean(Number(fields[1]) & 0x80000) !== handle!.cloexec)
				throw new Error("continuation descriptor identity changed");
			const state = [String(Number(fields[1]) & ~0x80000), ...fields.slice(2, 5)], previous = descriptions.get(input.alias);
			if (previous && previous.join(" ") !== state.join(" ")) throw new Error("continuation shared OFD diverged");
			descriptions.set(input.alias, state); fields[5] = String(input.fd);
		} else if (fd > 2 || fd === 0 && closedInput) throw new Error("unbound continuation handle");
		records.set(fd, fields); bound.push(fields.join(" "));
	}
	const pending = inputs.find(input => input.fd === Number(records.get(frontier.fd)?.[5]));
	if (!pending || !["pipe", "socket", "eventfd"].includes(pending.type ?? "") ||
		[...(closedInput ? [] : [0]), 1, 2, ...handles.map(handle => handle.fd)].some(fd => !records.has(fd)) ||
		cursor + Number(header[3]) + Number(header[4]) !== image.length) throw new Error("incomplete continuation FD table");
	for (const position of positions) {
		const input = inputs.find(input => input.fd === position.fd)!, state = descriptions.get(input.alias);
		if (!state) continue;
		const after = Number(state[1]); position.after = Number.isSafeInteger(after) ? after : state[1]!;
		position.afterFlags = Number(state[0]) !== input.flags ? Number(state[0]) : undefined;
	}
	return Buffer.concat([Buffer.from(`${first}\n${bound.join("\n")}\n`), image.subarray(cursor)]);
}

function requestProcessImage(pid: number, channel: import("node:stream").Duplex, wake: () => boolean, signal: AbortSignal): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		let reply = Buffer.alloc(0), settled = false;
		const finish = (error?: Error) => {
			if (settled) return; settled = true;
			channel.off("data", data); channel.off("error", failed); channel.off("close", closed); signal.removeEventListener("abort", closed);
			if (error) reject(error); else resolve(reply);
		};
		const data = (bytes: Buffer) => {
			reply = Buffer.concat([reply, bytes]);
			if (reply.length >= 40) finish(reply.length === 40 ? undefined : new Error("invalid continuation reply"));
		};
		const failed = (error: Error) => finish(error), closed = () => finish(new Error("continuation producer closed"));
		channel.on("data", data); channel.once("error", failed); channel.once("close", closed); signal.addEventListener("abort", closed, { once: true });
		if (signal.aborted) return closed();
		const request = Buffer.alloc(4); request.writeInt32LE(pid);
		channel.write(request, error => {
			if (error) finish(error);
			else { try { if (!wake()) closed(); } catch (error) { finish(error as Error); } }
		});
	});
}

function parseDescriptorOffsets(report: Buffer, inputs: ReturnType<typeof descriptorInputs>): Array<{
	fd: number; before: OFDPosition; after: OFDPosition; device: string; inode: string; afterFlags?: number;
	content?: import("./provenance-certificate.ts").ArtifactReference;
	detached?: { content: Buffer; mode: bigint; uid: bigint; gid: bigint; modified: bigint };
}> {
	let cursor = 0, bytes = 0;
	const line = () => {
		const end = report.indexOf(10, cursor);
		if (end < 0 || end - cursor > 256) throw new Error("incomplete inherited OFD result");
		const value = report.toString("ascii", cursor, end); cursor = end + 1; return value;
	};
	if (line() !== `OFD ${inputs.length}`) throw new Error("invalid inherited OFD header");
	const positions: ReturnType<typeof parseDescriptorOffsets> = inputs.map(input => {
		const value = line();
		if (!/^\d+ \d+ \d+ \d+ \d+$/.test(value)) throw new Error("invalid inherited OFD result");
		const fields = value.split(" "), [fd, flags, after] = fields.slice(0, 3).map(Number);
		if (fd !== input.fd) throw new Error("inherited descriptor report changed order");
		return { fd, before: input.offset, after: Number.isSafeInteger(after) ? after! : fields[2]!, ...(flags !== input.flags ? { afterFlags: flags! } : {}), device: fields[3]!, inode: fields[4]! };
	});
	while (cursor < report.length) {
		const value = line();
		if (!/^F \d+ \d+ \d+ \d+ -?\d+ \d+ \d+$/.test(value)) throw new Error("invalid detached file image");
		const [, fd, mode, uid, gid, seconds, nanos, length] = value.split(" "), size = Number(length);
		const input = inputs.find(input => input.fd === Number(fd)), position = positions.find(position => position.fd === Number(fd));
		if (!input || input.type || input.fd !== input.image || !position || position.detached || !Number.isSafeInteger(size) ||
			size < 0 || (bytes += size) > MAX_REQUEST_BYTES / 2 || cursor + size >= report.length || report[cursor + size] !== 10 || Number(nanos) >= 1e9) throw new Error("unbound detached file image");
		position.detached = { content: report.subarray(cursor, cursor + size), mode: BigInt(mode!), uid: BigInt(uid!), gid: BigInt(gid!), modified: BigInt(seconds!) * 1_000_000_000n + BigInt(nanos!) };
		cursor += size + 1;
	}
	return positions;
}

function reusedComputation(result: ProcessResultRecord, startedAt: number,
	acquired?: { readonly producer?: ProcessHandoff; readonly waiting?: readonly TimelineInterval[] }): TimelineDependency {
	if (acquired?.producer?.computation) return { computation: acquired.producer.computation, shared: acquired.waiting };
	const computation = new TimelineInterval(startedAt, performance.now());
	return { computation, shared: [computation], expectedActorMs: result.observedProcessMs };
}

function processTimingIdentity(prototype: ExecPrototype, weakKey: Sha256Digest): ServiceTimingIdentity {
	return {
		tool: "process",
		executionFingerprint: digestObject({
			executable: prototype.executableDigest,
			// Different arguments can select a cheap probe or expensive work in the same image.
			argv: prototype.argvDigest,
			context: prototype.processContextDigest,
			platform: prototype.platformFingerprint,
		}),
		actionKeyHash: weakKey,
	};
}

async function sealSessionEvidence(
	session: ActiveSession,
	changes: readonly SandboxWorkspaceChange[],
): Promise<readonly SandboxWorkspaceChange[]> {
	const capture = session.topLevelCapture;
	if (!capture) {
		session.incompleteReasons.add("top_capture_missing");
		session.topLevelEvidence ??= { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error("top-level workspace capture is missing");
	}
	// A bypass that did not resume in place ran outside the top-level trace.
	for (const [pid, reason] of session.bypasses) if (!capture.observation.resumedInterpositions?.includes(pid)) session.incompleteReasons.add(reason);
	const frontier = [...new Set([...capture.before.entries.keys(), ...capture.after.entries.keys()])].filter(name => {
		const before = capture.before.entries.get(name), after = capture.after.entries.get(name);
		return name && (before?.kind === "file" || after?.kind === "file") && before?.changeDigest !== after?.changeDigest && !changes.some(change => path.normalize(change.resource) === name);
	});
	if (frontier.length) {
		if (!session.workspace.captureChanges) throw new Error("workspace object frontier is unavailable");
		changes = [...changes, ...await session.workspace.captureChanges(frontier)];
	}
	const regularDeltas = changes.flatMap((change) => change.kind === "directory" ? [] : [{
		relativePath: change.resource,
		before: change.before,
		after: change.after,
		beforeMode: change.beforeMode,
		afterMode: change.afterMode,
	}]);
	const effects = diffWorkspaceStructures(capture.before, capture.after, regularDeltas, session.projection);
	if (!effects.complete) {
		session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error(`top-level workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	}
	const directoryChanges = await sourceDirectoryChanges(session, effects.effects);
	try {
		const evidence = await captureDependencies(
			session,
			transactionDependencySource(capture.before, effects),
			capture.observation.paths,
			effects.effects,
		);
		for (const reason of evidence.incompleteReasons) session.incompleteReasons.add(`top_evidence:${reason}`);
		session.topLevelEvidence = mergeDependencyEvidence(
			[
				{
					complete: capture.observation.complete && evidence.complete,
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
	return [...effects.effects.flatMap(({ relativePath, change }) => change.kind === "directory" ? [] : [{
		...change, root: session.sourceRoot, target: path.resolve(session.sourceRoot, relativePath), resource: relativePath,
	}]), ...directoryChanges];
}

async function sourceDirectoryChanges(
	session: ActiveSession,
	effects: ReturnType<typeof diffWorkspaceStructures>["effects"],
): Promise<readonly SandboxDirectoryChange[]> {
	const changes: SandboxDirectoryChange[] = [];
	for (const effect of effects) {
		if (effect.change.kind !== "directory") continue;
		const resource = slash(path.normalize(effect.relativePath));
		const target = path.resolve(session.sourceRoot, resource);
		if (!pathContains(session.sourceRoot, target) || target === path.resolve(session.sourceRoot)) {
			throw new Error(`directory effect escapes source workspace: ${effect.relativePath}`);
		}
		const sourceBefore = await readSandboxDirectoryState(target);
		const before = effect.change.before ? directoryState(effect.change.before) : undefined;
		if (before === undefined) {
			if (sourceBefore !== undefined) throw new Error(`directory creation baseline changed: ${resource}`);
		} else if (!sameSandboxState(sourceBefore, before)) {
			throw new Error(`source directory differs from execution baseline: ${resource}`);
		}
		changes.push({
			kind: "directory",
			root: session.sourceRoot,
			target,
			resource,
			...(before ? { before } : {}),
			...(effect.change.after ? { after: directoryState(effect.change.after) } : {}),
		});
	}
	return Object.freeze(changes);
}

function transactionDependencySource(
	snapshot: WorkspaceStructureSnapshot,
	effects: WorkspaceTransactionDiff,
) {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason}`);
	const deltas = new Map(effects.effects.flatMap(({ relativePath, change }) =>
		change.kind === "directory" ? [] : [[relativePath, change] as const]));
	const cached = new Map<string, Promise<WorkspaceTreeEntry | undefined>>();
	return (physicalPath: string) => {
		const relative = relativeFilesystemPath(snapshot.root, physicalPath);
		if (relative === undefined) {
			return Promise.reject(new Error(`workspace dependency escapes snapshot: ${physicalPath}`));
		}
		if (!cached.has(relative)) cached.set(relative, (async () => {
			const structure = snapshot.entries.get(relative);
			if (!structure || structure.kind !== "file") return structure;
			const content = deltas.get(relative)?.before ?? await captureStableFile(structure.contentPath ?? path.resolve(snapshot.root, relative), structure.size);
			const hydrated = hydrateWorkspaceFileEntry(structure, content);
			if (!hydrated) throw new Error(`transaction baseline changed: ${relative}`);
			return hydrated;
		})());
		return cached.get(relative)!;
	};
}

async function captureDependencies(
	session: ActiveSession,
	before: ReturnType<typeof transactionDependencySource>,
	observed: readonly ObservedProcessPath[],
	effects: readonly { readonly logicalPath: string }[],
) {
	const workspaceDependency = async (physical: string, logical: string, role: Exclude<ObservedProcessPath["role"], "metadata">) => {
		const [entry, parent] = await Promise.all([before(physical),
			path.resolve(physical) === path.resolve(session.workspace.sandboxRoot) ? undefined : before(path.dirname(physical))]);
		return snapshotDependency(logical, entry?.kind === "file" && entry.aliases ? { ...entry,
			aliases: entry.aliases.map(name => session.projection.toLogical(name)).sort() } : entry, parent, role, {
			excludedEntries: workspaceMetadataExclusions(session, physical),
			parentExcludedEntries: workspaceMetadataExclusions(session, path.dirname(physical)),
		});
	};
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

	// Kernel pathname walk over the baseline: links are recorded and expanded in place and `..` leaves the directory
	// actually reached. Sandlock collapses `..` first, so another result means the sandbox read another object.
	const walk = async (logical: string, follow: boolean) => {
		const pending = logical.split("/").filter(Boolean), links: string[] = [];
		let current = "/";
		for (let segment = pending.shift(); segment !== undefined; segment = pending.shift()) {
			if (segment === "..") { current = path.posix.dirname(current); continue; }
			const next = path.posix.join(current, segment), physical = pathContains(session.sourceRoot, next) ? session.projection.toPhysical(next) : undefined;
			const entry = physical ? await before(physical) : undefined;
			if (entry?.kind === "file" && pending.length) return undefined; // Native ENOTDIR; lexical collapse would continue.
			if (entry?.kind !== "symlink" || !pending.length && !follow) { current = next; continue; }
			if (links.push(next) > 40) return undefined;
			pending.unshift(...entry.target.split("/").filter(Boolean));
			if (entry.target.startsWith("/")) current = "/";
		}
		return { path: current, links };
	};
	const interposed = new Set(session.interposition.executables.map(([target]) => path.resolve(target)));
	for (const item of observed) {
		const follow = item.role !== "metadata" || item.followSymlinks, walked = await walk(item.path, follow);
		if (!walked || walked.path !== (item.path.split("/").includes("..") ? (await walk(path.posix.normalize(item.path), follow))?.path : walked.path)) {
			add(undefined, `pathname_walk:${item.path}`); continue;
		}
		for (const link of item.role === "metadata" ? [] : walked.links) add(await workspaceDependency(session.projection.toPhysical(link)!, link, "input"));
		const observedPath = item.role === "metadata" ? path.resolve(item.path) : walked.path;
		if (interposed.has(observedPath)) continue;
		if (session.deniedPaths.some((denied) => pathContains(denied, observedPath))) { taints.add("escaped_sandbox"); incompleteReasons.add(`denied:${observedPath}`); continue; }
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
			add(await workspaceDependency(physical, session.projection.toLogical(physical), item.role));
			continue;
		}
		try {
			const captured = await captureHostPath(physical, item.role);
			if (captured) for (const dependency of captured) add(dependency);
			else { taints.add("mutable_input"); add(undefined, `mutable:${physical}`); }
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
		add(await workspaceDependency(physical, effect.logicalPath, "input"));
	}
	return { complete, dependencies: [...dependencies.values()], taints: [...taints], incompleteReasons: [...incompleteReasons] };
}

const STABLE_SANDBOX_DEVICES = new Set(["/dev/null", "/dev/tty", "/dev/zero", "/dev/full"]);
const SAME_CONFINEMENT_TAINTS = ["confinement_observation"] as const;

function workspaceMetadataExclusions(session: ActiveSession, target: string): readonly string[] | undefined {
	return path.resolve(target) === path.resolve(session.workspace.sandboxRoot) ? session.workspace.observationExcludes : undefined;
}

async function captureHostPath(
	physicalPath: string,
	role: Exclude<ObservedProcessPath["role"], "metadata">,
): Promise<readonly DynamicDependency[] | undefined> {
	const dependencies: DynamicDependency[] = [];
	for await (const { path: current, info, link, terminal } of walkFilesystemPath(path.resolve(physicalPath))) {
		if (["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp", "/home"].some((root) => pathContains(root, current))) return undefined;
		if (!info) {
			const absence = await captureAbsenceDependency(current, slash(current), true);
			if (!absence) throw new Error("host dependency changed during capture");
			return [...dependencies, absence];
		}
		if (info.uid !== 0n || (link === undefined && (info.mode & 0o022n) !== 0n)) return undefined;
		if (link !== undefined) dependencies.push({ kind: "symlink", path: slash(current), target: link, targetDigest: sha256Digest(Buffer.from(link, "utf8")) });
		else if (terminal && info.isFile()) dependencies.push((await captureFileDependency(current, slash(current), role, { includeMetadata: true })).dependency);
		else if (terminal && info.isDirectory()) dependencies.push(await captureDirectoryDependency(current, slash(current), true));
		else if (!info.isFile() && !info.isDirectory()) throw new Error("unsupported host dependency");
	}
	return dependencies;
}


async function replayFilesystemEffects(
	owner: WorkspaceSandboxService,
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
			...(event.operation ? { operation: event.operation } : {}),
			...(event.object ? { object: { ...event.object, path: projection.toPhysical(event.object.path)! } } : {}),
			...(event.aliases ? { aliases: event.aliases.map(name => projection.toPhysical(name)!) } : {}),
			...(event.before.kind === "file" ? { before: artifacts.read(event.before.data), beforeMode: event.before.mode } : {}),
			...(event.after.kind === "file" ? { after: artifacts.read(event.after.data), afterMode: event.after.mode } : {}),
		});
	}
	if (!changes.length) return;
	await owner.commitDelta({
		output: { result: { content: [], details: {} }, isError: false },
		changes,
	});
}

/** Whole commands and held children publish the same ordered, content-addressed result format. */
async function captureProcessResult(
	store: ProvenanceCertificateStore,
	outcome: SpawnOutcome,
	observedProcessMs: number,
	effects: readonly { readonly logicalPath: string; readonly change:
		| Pick<SandboxFileChange, "kind" | "before" | "after" | "beforeMode" | "afterMode" | "operation" | "object" | "aliases">
		| Pick<SandboxDirectoryChange, "kind" | "before" | "after"> }[],
): Promise<Extract<ProcessResultRecord, { readonly exit: ExitOutcome }>> {
	const journal: OrderedEffectEvent[] = [];
	for (const { logicalPath, change } of effects) {
		const state = async (side: "before" | "after"): Promise<WorkspaceEffectState> => {
			if (change.kind === "directory") return change[side] ? { kind: "directory", ...change[side] } : { kind: "absent" };
			const content = change[side], mode = change[side === "before" ? "beforeMode" : "afterMode"];
			if (content === undefined) return { kind: "absent" };
			if (mode === undefined) throw new Error("transaction file mode is unavailable");
			return { kind: "file", data: await store.artifacts.put(content), mode };
		};
		journal.push({ sequence: journal.length, kind: "workspace", path: logicalPath, before: await state("before"), after: await state("after"),
			...(change.kind !== "directory" ? { ...(change.operation ? { operation: change.operation } : {}),
				...(change.object ? { object: change.object } : {}), ...(change.aliases ? { aliases: change.aliases } : {}) } : {}) });
	}
	for (const event of outcome.output) {
		journal.push({ sequence: journal.length, kind: "output", fd: event.fd, data: await store.artifacts.put(event.data) });
	}
	return { replayProfile: "buffered_noninteractive", observedProcessMs, journal, exit: exitOutcome(outcome) };
}

function directoryState(state: Extract<WorkspaceEffectState, { kind: "directory" }>): SandboxDirectoryChange["before"] {
	return { entriesDigest: state.entriesDigest, mode: state.mode, uid: state.uid, gid: state.gid };
}

/** Output events carry the target's own descriptor. */
function loadOutputEvents(artifacts: VerifiedArtifactClosure, journal: readonly OrderedEffectEvent[]): readonly BufferedOutput[] {
	return journal.flatMap(event => event.kind === "output" ? [{ fd: event.fd, data: artifacts.read(event.data) }] : []);
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
	readonly signal?: AbortSignal;
	readonly token: string;
	readonly socketPath: string;
	readonly dispatcherBinary: string;
	readonly excludedExecutables: readonly string[];
}) {
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
		input.signal?.throwIfAborted();
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
	input.signal?.throwIfAborted();
	const configuration = {
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
	const sources = new Map<string, InterposedDirectory[]>();
	for (const directory of directories) {
		const aliases = sources.get(directory.source) ?? [];
		aliases.push(directory);
		sources.set(directory.source, aliases);
	}
	for (const [source, aliases] of sources) {
		for (const directory of aliases) {
			input.signal?.throwIfAborted();
			await Promise.all([mkdir(directory.shadow, { recursive: true }), mkdir(directory.view, { recursive: true })]);
			await writeFile(
				path.join(directory.view, ".pi-spec-dispatch"),
				["PI_SPEC_DISPATCH", process.execPath, dispatcher, configurationPath, directory.target, directory.shadow, ""].join("\n"),
				{ mode: 0o600 },
			);
		}
		let entries: string[];
		try {
			entries = await readdir(source);
		} catch {
			continue;
		}
		// Each physical entry is probed once; aliases retain independent exec-only mappings.
		// Bound preparation and settle every alias link before capturing directory evidence.
		await mapFilesystem(entries, async (name) => {
			input.signal?.throwIfAborted();
			if (!name || name === ".pi-spec-dispatch" || name.includes("/") || name.includes("\0")) return;
			const sourceEntry = path.join(source, name);
			try {
				const resolved = await realpath(sourceEntry);
				const resolvedStat = await lstat(resolved);
				if (!resolvedStat.isFile() || excluded.has(resolved)) return;
				await access(sourceEntry, fsConstants.X_OK);
			} catch {
				return; // Unproved entries remain visible through the original directory.
			}
			for (const directory of aliases) {
				const viewEntry = path.join(directory.view, name);
				try {
					await link(launcher, viewEntry);
					const intercepted = path.join(directory.target, name);
					executables.push([intercepted, path.join(directory.shadow, name)]);
					executables.push([viewEntry, path.join(directory.shadow, name)]);
					execMounts.push({ virtualPath: intercepted, hostPath: viewEntry });
				} catch {
					// One unavailable view cannot suppress another alias's mapping.
				}
			}
		});
		input.signal?.throwIfAborted();
		dependencies.push(
			await captureDirectoryDependency(
				source,
				input.projection.isWorkspacePhysical(source) ? input.projection.toLogical(source) : slash(source),
				true,
				path.resolve(source) === path.resolve(input.workspaceRoot) ? input.workspaceExcludes : [],
			),
		);
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
	readonly dispatcher: string;
	readonly logicalRoot: string;
	readonly physicalRoot: string;
}): Promise<ProcessExecutionContext> {
	await writeFile(path.join(input.physicalRoot, "script-position"), "#!/bin/sh\nexit 42\n", { mode: 0o700 });
	const command = straceCommand(input.strace, path.join(input.physicalRoot, "context"), [
		input.sandlock,
		...sandboxPolicyArguments(input.logicalRoot, [], [input.physicalRoot], [
			{ virtualPath: input.logicalRoot, hostPath: input.physicalRoot, readOnly: false },
		], []),
		"--",
		input.dispatcher,
		"--exec",
		"12",
		"pi-context-probe",
		process.execPath,
		fileURLToPath(new URL("./process-dispatcher.mjs", import.meta.url)),
		"--probe-context",
		input.logicalRoot,
		path.join(input.logicalRoot, "script-position"),
	]);
	const outcome = await runSpawn(
		input.strace,
		command.slice(1),
		{ cwd: input.physicalRoot, environment: definedProcessEnvironment(process.env) },
	);
	if (outcome.signal || outcome.code !== 0) throw new Error("process execution context probe failed");
	const stdout = Buffer.concat(outcome.output.filter(({ fd }) => fd === 1).map(({ data }) => data)).toString();
	const parsed: unknown = JSON.parse(stdout);
	if (!validProcessContext(parsed)) throw new Error("process execution context probe returned invalid data");
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
		readonly onOutputEndpoints?: (endpoints: readonly [string, string]) => void;
		readonly inheritedFiles?: readonly number[];
		readonly onControl?: (channel: import("node:stream").Duplex, wake: () => boolean) => (() => void | Promise<void>);
	},
): Promise<SpawnOutcome> {
	options.signal?.throwIfAborted();
	const channels = options.onOutputEndpoints ? await acquireOutputChannels(options.signal) : undefined;
	try {
		options.signal?.throwIfAborted();
		if (channels) options.onOutputEndpoints!([channels.entries[0]!.endpoint, channels.entries[1]!.endpoint]);
		const child = spawn(executable, args, {
			cwd: options.cwd,
			env: options.environment,
			detached: true,
			stdio: [options.stdin ? "pipe" : "ignore", ...(channels ? channels.entries.map((entry) => entry.target!) : ["pipe", "pipe"] as const),
				...(options.inheritedFiles ?? []), ...(options.onControl ? ["pipe" as const] : [])],
		});
		const output: BufferedOutput[] = [];
		child.once("spawn", () => channels?.releaseWriters());
		const append = (fd: 1 | 2, value: Buffer) => {
			const event = { fd, data: Buffer.from(value) } as const;
			const previous = output.at(-1);
			if (previous?.fd === fd && previous.data.byteLength + event.data.byteLength <= 1024 * 1024) {
				(output as BufferedOutput[])[output.length - 1] = { fd, data: Buffer.concat([previous.data, event.data]) };
			} else output.push(event);
			options.onOutput?.(event);
		};
		const sources = channels?.entries.map((entry) => entry.source) ?? [child.stdout!, child.stderr!];
		const drained = Promise.all(sources.map((source, index) => {
			source.on("data", (value: Buffer) => append(index === 0 ? 1 : 2, value));
			return finished(source, { readable: true, writable: false, cleanup: true });
		}));
		void drained.catch(() => undefined);
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
		const completed = new Promise<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>(
			(resolve, reject) => {
				child.once("error", reject);
				// Owned channels drain separately from the child's Node-managed stdio.
				child.once("close", (code, signal) => resolve({ code, signal }));
			},
		);
		let releaseControl: (() => void | Promise<void>) | undefined;
		try {
			if (options.onControl) releaseControl = options.onControl(child.stdio.at(-1) as import("node:stream").Duplex, () => child.kill("SIGUSR2"));
			const [result] = await Promise.all([completed, drained]);
			if (options.signal?.aborted) throw new Error("aborted");
			if (timedOut) throw new Error(`timeout:${options.timeoutSeconds}`);
			return { ...result, output };
		} catch (error) {
			terminate();
			await completed.catch(() => undefined);
			throw error;
		} finally {
			if (timeout) clearTimeout(timeout);
			options.signal?.removeEventListener("abort", onAbort);
			await releaseControl?.();
		}
	} finally { await channels?.dispose(); }
}

/** Own the write endpoints before inheritance; a running tracer may replace its descriptors. */
async function acquireOutputChannels(signal?: AbortSignal) {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-process-output-"));
	const entries: { server: net.Server; source: net.Socket; target?: net.Socket; endpoint: string }[] = [];
	const releaseWriters = () => { for (const entry of entries) entry.target?.destroy(); };
	const dispose = async () => {
		releaseWriters();
		for (const entry of entries) entry.source.destroy();
		await Promise.all(entries.map(({ server }) => new Promise<void>((resolve) => server.close(() => resolve()))));
		await rm(root, { recursive: true, force: true });
	};
	try {
		for (const fd of [1, 2]) {
			signal?.throwIfAborted();
			const socketPath = path.join(root, String(fd));
			const entry = { server: net.createServer({ pauseOnConnect: true }), source: new net.Socket({ signal }), endpoint: "" } as typeof entries[number];
			entries.push(entry);
			entry.source.on("error", () => undefined); // Abort may precede the stream-drain observer.
			entry.server.maxConnections = 1;
			entry.server.once("connection", (socket) => { entry.target = socket; });
			await listenUnixSocket(entry.server, socketPath);
			const accepted = once(entry.server, "connection");
			await Promise.all([accepted, once(entry.source.connect(socketPath), "connect")]);
			// One connected server endpoint in our private namespace; no Node private fd API.
			const peers = (await readFile("/proc/net/unix", "utf8")).split("\n")
				.map((line) => line.trim().split(/\s+/))
				.filter((fields) => fields[7] === socketPath && fields[4] === "0001" && fields[5] === "03");
			if (peers.length !== 1 || !/^\d+$/.test(peers[0]![6]!)) throw new Error("output_endpoint_identity_unproven");
			entry.endpoint = `socket:[${peers[0]![6]}]`;
		}
		return { entries, releaseWriters, dispose };
	} catch (error) { await dispose(); throw error; }
}

function parseDispatcherRequest(body: string): DispatcherRequest | undefined {
	try {
		const value: unknown = JSON.parse(body.trim());
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const request = value as Partial<DispatcherRequest>;
		const text = (field: unknown) => typeof field === "string" && !field.includes("\0");
		return typeof request.token === "string" && typeof request.name === "string" && text(request.invokedPath) && text(request.argv0) &&
			request.argv0!.length <= 1024 * 1024 && Array.isArray(request.args) && request.args.every(text) && typeof request.cwd === "string" &&
			!!request.environment && typeof request.environment === "object" && (request.pid === undefined || Number.isSafeInteger(request.pid) && request.pid > 0) &&
			Object.entries(request.environment).every(([name, value]) => name.length > 0 && !name.includes("=") && text(name) && text(value))
			? request as DispatcherRequest : undefined;
	} catch {
		return undefined;
	}
}

function materializeDispatcherRequest(session: ActiveSession, request: DispatcherRequest): DispatcherRequest | undefined {
	const cwd = session.projection.toPhysical(request.cwd);
	return cwd ? { ...request, cwd } : undefined;
}

async function eligibleRequest(session: ActiveSession, request: DispatcherRequest, expectedContext: ProcessExecutionContext): Promise<RequestEligibility> {
	if (!validProcessContext(request.context)) return { reason: "process_context_unsupported" };
	if (!pathContains(session.workspace.sandboxRoot, request.cwd)) return { reason: "cwd_outside_workspace" };
	if (request.args.length > 4096) return { reason: "argument_count_limit" };
	if (request.args.reduce((sum, value) => sum + Buffer.byteLength(value), 0) > 1024 * 1024) return { reason: "argument_bytes_limit" };
	const endpoints = session.topLevelOutputEndpoints;
	if (!endpoints) return { reason: "output_endpoint_capture_missing" };
	if (request.context.outputEndpoints.some((endpoint) => !endpoint)) return { reason: "request_output_endpoint_missing" };
	// The launch key below still checks a discarded descriptor's type and flags.
	const routeOf = (endpoint: string) => endpoint === endpoints[0] ? 1 : endpoint === endpoints[1] ? 2 : endpoint === "/dev/null" ? 0 : undefined;
	const route = [routeOf(request.context.outputEndpoints[0]), routeOf(request.context.outputEndpoints[1])] as const;
	if (route[0] === undefined || route[1] === undefined) return { reason: `output_endpoint_mismatch:${JSON.stringify({ expected: endpoints, observed: request.context.outputEndpoints })}` };
	const outputRoute: OutputRoute = [route[0], route[1]];
	const context = routedProcessContext(expectedContext, outputRoute);
	if (request.context.launchKey !== context.launchKey) return { reason: "launch_key_mismatch" };
	if (request.context.umask !== context.umask) return { reason: "umask_mismatch" };
	return { route: outputRoute };
}

function shellArguments(invocation: ToolProcessInvocation, command: string): string[] {
	return invocation.commandTransport === "argv" ? [...invocation.shellArgs, command] : [...invocation.shellArgs];
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
		executableDigest: await hashExecutableFile(invocation.shell),
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
			signals: "node-default",
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

function execText(executable: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
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
	if (!stableEqual(definedProcessEnvironment(request.environment), invocation.environment)) {
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
				existing.metadataDigest === dependency.metadataDigest && stableEqual(existing.aliases, dependency.aliases)
			) {
				if (existing.role !== "executable" && dependency.role === "executable") dependencies.set(identity, dependency);
				continue;
			}
			if (existing && !stableEqual(existing, dependency)) {
				if (dependency.kind === "directory" && mutatedDirectories.has(dependency.path.replaceAll("\\", "/"))) continue;
				complete = false;
				incompleteReasons.add(`dependency_changed_during_execution:${identity}`);
				continue;
			}
			dependencies.set(identity, dependency);
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


function permissionDenied(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && (error.code === "EACCES" || error.code === "EPERM"));
}
