import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { outputStatusFlags, processContextFromRaw, routedProcessContext, validProcessContext, type ProcessExecutionContext, type RawProcessContext } from "./process-context.mjs";
import { execFile, spawn } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { constants as fsConstants } from "node:fs";
import { access, chmod, copyFile, link, lstat, mkdir, mkdtemp, open, readFile, readdir, realpath, rm, stat, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { finished } from "node:stream/promises";
import { errorMessage, isMissing as missing } from "./error-utils.ts";
import { stableEqual } from "./stable-json.ts";
import { TimelineInterval, type TimelineDependency } from "./task-timing.ts";
import { createExecPrototype, digestObject, dynamicDependencyIdentity, type DynamicDependency, type DynamicDependencyCertificate, type ExecPrototype,
	type ExitOutcome, filesystemObservationDigest, ONE_SHOT_TAINTS, type OrderedEffectEvent, type OFDPosition, type ProcessProducerProof,
	type ProcessProvenanceCertificate, type ProcessResultRecord, processWeakKey, type ProvenanceTaint, sealProcessCertificate, sha256Digest,
	type Sha256Digest, type WorkspaceEffectState } from "./provenance-certificate.ts";
import { captureAbsenceDependency, captureDirectoryDependency, captureFileDependency,
	validateDynamicDependencyCertificate } from "./provenance-validation.ts";
import { diffWorkspaceStructures, ExecutionPathProjection, hydrateWorkspaceFileEntry, snapshotDependency, type WorkspaceStructureSnapshot,
	type WorkspaceTransactionDiff, type WorkspaceTreeEntry } from "./process-observation.ts";
import { definedProcessEnvironment, type PreparedProcessExecutionRoute, type ProcessExecutionRequest, type ProcessExecutionResult,
	type ProcessExecutor } from "./process-execution.ts";
import { isPoisonedEffectCommit } from "./effect-transaction.ts";
import { resolveHostExecutable } from "./executable-path.ts";
import { assertNoSymlinkPath, captureStableFile, hashExecutableFile, mapFilesystem, sameFilesystemIdentity, walkFilesystemPath } from "./filesystem-evidence.ts";
import { captureHeldDescriptorInputs, inspectHeldExecProcess, LinuxHeldExecBoundary, listenUnixSocket, resolveLinuxExecHelper, type HeldExecDecision,
	type HeldExecProcess, type HeldExecSnapshot, descriptorInputs, descriptorEffects, inheritedTracer, type ProcessResourceGraph } from "./linux-held-exec.ts";
import { emptyWorldReuseMetrics, snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption, type ExecutionWorldStorageControl,
	type WorldReuseMetrics } from "./execution-world.ts";
import { type ProcessReusePlan, ProcessReusePlanner } from "./reuse-planner.ts";
import { ProvenanceCertificateStore, type ProvenanceStoreOptions, type VerifiedArtifactClosure } from "./reuse-store.ts";
import { SpeculationScheduler, type ServiceTimingIdentity, waitForCandidate } from "./scheduler.ts";
import { observeStrace, straceCommand, tracedObservations, tracedWrites, type ObservedProcessPath, type StraceObservation, type TracedWrite, writesWithin } from "./strace-observer.ts";
import type { WorkspaceTransactionOwnership } from "./workspace-transaction.ts";
import type { ToolProcessInvocation } from "./tool-settlement.ts";
import type { ResourceValidation } from "./settlement.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, sameScope, type ProcessContinuation, type ProcessExecutionBinding, type ProcessHandoff, type ProcessHandoffLookup } from "./process-handoff.ts";
import { WorkspaceSandboxService, readSandboxDirectoryState, restoreModifiedTimes, sameSandboxState, type SandboxDirectoryChange, type SandboxFileChange,
	type SandboxWorkspaceChange, type SandboxWorkspaceContext } from "./workspace-sandbox.ts";
import { containsFilesystemPath as pathContains, relativeFilesystemPath, slash } from "./path-utils.ts";

const BACKEND_EPOCH = "pi-linux-process-instance-inputs";
const POLICY_ID = "sandlock-virtual-root-transparent-exec";
const LEAF_POLICY_ID = "sandlock-virtual-workspace-leaf";
const MAX_REQUEST_BYTES = 4 * 1024 * 1024, LEARNED_LAUNCHES = 64, MAX_INTERPOSED_MOUNT_BYTES = 512 * 1024, CHEAP_CHILD_MS = 500;
const MAX_CONTINUATION_BYTES = 65 * 1024 * 1024;
const IO_FRONTIERS = new Map([[0, "read"], [1, "write"], [19, "readv"], [20, "writev"], [44, "sendto"], [45, "recvfrom"], [46, "sendmsg"], [47, "recvmsg"]]);
const MAX_CAPTURE_BYTES = 512 * 1024 * 1024;
/** Native inputs consumed by this exact one-shot execution; they still prohibit any later replay. */
const TRANSFERRED_INPUT_TAINTS = new Set<ProvenanceTaint>(ONE_SHOT_TAINTS);

export interface LinuxProcessBackendOptions {
	readonly storeRoot: string;
	readonly store?: ProvenanceStoreOptions;
	/** Whether two distinct runs agreeing on a result make it repeatable despite the one-shot inputs every process may read. */
	readonly witnessRepeats?: () => boolean;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
	readonly heldExecBinary?: string;
	/** Additional host paths that speculative processes must never read. */
	readonly deniedPaths?: readonly string[];
	/** A nested child whose recent traced runs all took less resumes in place instead of in its own sandbox (0 disables). */
	readonly cheapChildMs?: number;
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
type MutableLinuxProcessReuseMetrics = { -readonly [Key in CountedReuseMetric]: number } & { lastError?: string; };

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

interface InterposedDirectory { readonly source: string; readonly target: string; readonly shadow: string; readonly view: string; }
/** A PATH entry the dispatcher stands in for, and the file it reaches. */
interface InterceptedExecutable { readonly intercepted: string; readonly view: string; readonly native: string; readonly file: string; }

interface SandboxMount { readonly virtualPath: string; readonly hostPath: string; readonly readOnly: boolean; }

interface ExecMount { readonly virtualPath: string; readonly hostPath: string; readonly alias?: true; }

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
	/** Inherited pipes beyond stdio as the launcher sees them now: each end, and its queue unconsumed. */
	readonly descriptors?: readonly { readonly fd: number; readonly alias: number; readonly device: string; readonly inode: string; readonly flags: number;
		readonly capacity: number; readonly eof: boolean; readonly queueHex: string }[];
}

/** The outlet each target output uses; 0 discards into /dev/null. */
type OutputRoute = readonly [0 | 1 | 2, 0 | 1 | 2];
type RequestEligibility = { readonly route: OutputRoute; readonly outputPipes?: readonly [boolean, boolean]; readonly outputFlags?: readonly [number, number] } | { readonly reason: string };
/** A brokered run's net effect on an inherited pipe of one repeated byte, applied by the launcher to its own end. */
type StreamSettlement = { readonly fd: number; readonly kind: "i" | "o"; readonly data: Buffer };
type ProcessArguments = Pick<DispatcherRequest, "argv0" | "args" | "cwd" | "environment"> & {
	readonly closeStdin?: boolean; readonly resources?: ProcessResourceGraph; readonly outputPipes?: readonly [boolean, boolean]; readonly streams?: true; // inherited pipes the launcher holds and settles
	/** Status flags its parent set on its outputs, which its launcher sets again. */
	readonly outputFlags?: readonly [number, number];
};
type BoundProcessInvocation = ProcessArguments & {
	readonly sourceRoot: string;
	readonly executable: string;
	readonly outputRoute: OutputRoute;
	readonly producer?: ProcessProducerProof;
};

interface BufferedOutput { readonly fd: 1 | 2; readonly data: Buffer; }

interface DispatcherResponse {
	readonly kind: "hit" | "executed" | "bypass" | "suspended";
	readonly executable?: string;
	readonly output?: readonly { readonly fd: 1 | 2; readonly data: string }[];
	readonly exit?: ExitOutcome;
	readonly weakKey?: Sha256Digest;
	readonly streams?: readonly StreamSettlement[];
}

/** A brokered run writing into a session's workspace: its launcher's pid and, once known, what it and the runs brokered from its tree wrote. */
interface SessionWriter {
	readonly startedAt: number; endedAt?: number; readonly tracePrefix: string; writes?: readonly TracedWrite[]; settled?: true;
	readonly pid?: number; written?: readonly string[]; descendants?: readonly SessionWriter[];
}

interface ActiveSession {
	readonly token: string;
	readonly ownership: ProcessHandoffOwnership;
	readonly sourceRoot: string;
	/** The workspace's own repository, shown read-only in place of the snapshot's: git reads what the Actor's git reads. */
	readonly gitDirectory?: string;
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
	/** Brokered runs whose effects overlapping siblings left unattributable: what they observed joins the command's own evidence. */
	readonly foldedObservations: StraceObservation[];
	readonly executionBindings: Map<number, ProcessExecutionBinding>;
	readonly computations: TimelineDependency[];
	readonly incompleteReasons: Set<string>;
	/** Bypasses that exec their native image in place; the top-level trace must show each one resume. */
	readonly bypasses: [pid: number, reason: string][];
	/** Bypassed launches that resumed in place within a brokered run's trace, which that run's evidence covers. */
	readonly resumed: Set<number>;
	/** Nested executions whose workspace intervals may overlap: running ones by their live trace, settled ones by their writes. */
	readonly writers: Set<SessionWriter>;
	readonly metrics: MutableLinuxProcessReuseMetrics;
	topLevelCapture?: TopLevelCapture;
	topLevelExecution?: { readonly prototype: ExecPrototype; readonly outcome: SpawnOutcome; readonly observedProcessMs: number; };
	topLevelEvidence?: DynamicDependencyCertificate;
	topLevelOutputEndpoints?: readonly [string, string];
	/** Output sockets of running brokered children: their own children write there, and those bytes reach the child's capture. */
	readonly nestedOutputEndpoints: Set<readonly [string, string]>;
	sealPromise?: Promise<readonly SandboxWorkspaceChange[]>;
	/** When the top level started writing into the session's private branch. */
	privateSince?: number;
	closing?: Promise<void>;
}

interface TopLevelCapture {
	readonly before: WorkspaceStructureSnapshot;
	readonly after: WorkspaceStructureSnapshot;
	readonly observation: StraceObservation;
}

interface SpawnOutcome { readonly code: number | null; readonly signal: NodeJS.Signals | null; readonly output: readonly BufferedOutput[]; }

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
			try { observe([...observation.bindings].sort(([left], [right]) => left - right).map(([, binding]) => binding), Object.freeze([...observation.computations])); }
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
	private readonly actorExecutableDirectories = new Set<string>();
	private readonly processScheduler = new SpeculationScheduler<object>();
	/** Recent traced run times of nested children by executable, the longest kept: a cheap child never repays its own sandbox. */
	private readonly childRunMs = new BoundedRecencyMap<string, readonly number[]>(512);
	/** Executable entries of a PATH directory, by the directory's identity and the exclusions (see createProcessInterposition). */
	private readonly executableEntries = new BoundedRecencyMap<string, readonly (readonly [name: string, file: string])[]>(64);
	private sharedInterposition?: SharedInterposition;
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
				return { removedEntries: result.removedCertificates, removedArtifacts: result.removedArtifacts, removedBytes: result.removedBytes };
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

	async fingerprint(): Promise<string> { return (await this.resolveReady()).fingerprint; }

	/** Aggregate backend counters retained for qualification and low-level diagnostics. */
	metrics(): LinuxProcessReuseMetrics { return Object.freeze({ ...this.counters }); }

	/** Actor-path counters, excluding child reuse performed inside speculative worlds. */
	actorMetrics(): LinuxProcessReuseMetrics { return Object.freeze({ ...this.actorCounters }); }

	/** Scoped launches; sandbox bindings are still speculative until adopted. Raw parameters are never persisted. */
	/** Whether a bound launch reads a queue only another process writes while it runs: alone, it runs only up to that input. */
	fed(binding: ProcessExecutionBinding): boolean {
		const invocation = this.handoffs.resolveBinding(binding, binding.scope), resources = invocation && "resources" in invocation ? invocation.resources : undefined;
		return Object.entries(resources?.objects ?? {}).some(([image, object]) => object.queue?.producer === "live" &&
			!Object.values(resources!.descriptions).some(description => description.object === Number(image) && (description.flags & 3) !== 0));
	}

	executionBindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return this.handoffs.bindings(scope).filter(binding => {
			const invocation = this.handoffs.resolveBinding(binding, scope);
			return invocation && !("trackingOnly" in invocation);
		});
	}

	/** Whether a run of a learned launch would redo work: no result of it, this session's or stored, holds on the workspace now. */
	async bindingStale(binding: ProcessExecutionBinding): Promise<boolean> {
		const invocation = this.handoffs.resolveBinding(binding, binding.scope);
		if (!invocation || "trackingOnly" in invocation) return true;
		const projection = new ExecutionPathProjection({ sourceRoot: invocation.sourceRoot, workspaceRoot: invocation.sourceRoot });
		for (const certificate of [...this.handoffs.results(binding.key, binding.scope), ...await this.store.findByWeakKey(binding.key, invocation.executable).catch(() => [])]) {
			if ((await validateDynamicDependencyCertificate(certificate.dependencyCertificate, { resolvePath: (logical) => projection.toPhysical(logical),
				acceptedTaints: [...TRANSFERRED_INPUT_TAINTS] }).catch(() => undefined))?.status === "valid") return false;
		}
		return true;
	}

	/** Keep possible publication visible through preparation, execution, and final evidence capture. */
	async withProducer<Value>(run: () => Promise<Value>): Promise<Value> {
		this.producers++;
		try { return await run(); } finally { this.producers--; }
	}

	private get hasLiveResults(): boolean { return this.producers > 0 || this.handoffs.hasResults; }

	async prepareActorReplay(host: ProcessExecutor, options: ActorProcessReplayOptions, refresh = false): Promise<PreparedProcessExecutionRoute> {
		if (process.platform !== "linux") return { state: "unavailable", detail: "Linux or WSL 2 required" };
		let state: "degraded" | "ready" = "degraded";
		let detail = options.held ? "Bash history; child handoff checked when evidence exists or on refresh" : "matching whole Bash calls; this shell cannot hold child processes";
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
						// A speculative producer may hold an inherited-pipe result: its launch identity includes those descriptors.
						return this.observations.getStore()?.learn || this.hasLiveResults ? "inspect" : false;
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
		const acceptProducer = (producer: ProcessProducerProof) => actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
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
					const prototype = await topLevelProcessPrototype(invocation, request, definedProcessEnvironment(request.environment), projection, platformFingerprint);
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
		readonly acceptOperationScope?: (scope: ExecutionScope, salvage?: boolean) => boolean;
	}): Promise<LinuxProcessSession> {
		return this.withProducer(() => this.createSession(input));
	}

	private async createSession(input: Parameters<LinuxProcessReuseBackend["open"]>[0]): Promise<LinuxProcessSession> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		const ready = await this.resolveReady();
		input.signal?.throwIfAborted();
		const sourceRoot = path.resolve(input.sourceRoot);
		const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: input.workspace.sandboxRoot, privateRoot: input.workspace.processRoot });
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
		const gitDirectory = await lstat(path.join(sourceRoot, ".git")).then((info) => info.isDirectory() ? path.join(sourceRoot, ".git") : undefined, () => undefined);
		const session: ActiveSession = {
			token,
			sourceRoot,
			...(gitDirectory ? { gitDirectory } : {}),
			workspace: input.workspace,
			invocation: input.invocation,
			scope: snapshotExecutionScope(input.scope),
			projection,
			interposition: { mounts: [], directories: [], entries: [], dependencies: [] },
			originalPath,
			deniedPaths,
			producer,
			nestedProducer,
			socketPath,
			signal: AbortSignal.any([controller.signal, ...(input.signal ? [input.signal] : [])]),
			pending: new Set<Promise<unknown>>(),
			ownership: new ProcessHandoffOwnership(input.onOperationAdopted, input.acceptOperationScope),
			nestedEvidence: [], foldedObservations: [], resumed: new Set(),
			executionBindings: new Map(),
			computations: [],
			incompleteReasons: new Set<string>(), bypasses: [], writers: new Set(), nestedOutputEndpoints: new Set(),
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
					gitDirectory,
					privateRoot: input.workspace.processRoot,
					// Where the Actor's own children run from is intercepted too: a build tool execs its compiler by absolute path.
					pathValue: [originalPath, ...this.actorExecutableDirectories].join(path.delimiter),
					projection,
					sourceRoot,
					workspaceRoot: input.workspace.sandboxRoot,
					workspaceExcludes: input.workspace.observationExcludes,
					signal: session.signal,
					token,
					socketPath,
					dispatcherBinary: ready.dispatcher,
					excludedExecutables: [input.invocation.shell, process.execPath, ready.dispatcher, ready.sandlock, ready.strace],
					executableEntries: this.executableEntries,
					shared: this.sharedInterposition ??= { views: new BoundedRecencyMap(64), root: mkdtemp(path.join(os.tmpdir(), "pi-spec-interposition-")).then(async (shared) => {
						await mkdir(path.join(shared, "session"));
						await copyFile(ready.dispatcher, path.join(shared, "dispatcher"));
						await chmod(path.join(shared, "dispatcher"), 0o755);
						return shared;
					}) },
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
			seal: (changes) => { session.sealPromise ??= this.withProducer(() => this.seal(session, changes)); return session.sealPromise; },
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
		const shared = this.sharedInterposition?.root.then((root) => rm(root, { recursive: true, force: true }), () => undefined);
		return this.disposal = this.resetActorReplay().finally(() => Promise.all([this.replayWorkspace.dispose(), shared]));
	}

	private async resolveReady(): Promise<ReadyBackend> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		this.ready ??= this.probe();
		return this.ready;
	}

	private resolvePlatformFingerprint(): Promise<Sha256Digest> {
		this.platformFingerprint ??= execText("uname", ["-srm"]).then((kernel) => digestObject({ kernel: kernel.trim(), arch: process.arch }));
		return this.platformFingerprint;
	}

	private async probe(): Promise<ReadyBackend> {
		if (process.platform !== "linux") throw new Error("Linux host required");
		await mkdir(this.options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(this.options.storeRoot, 0o700);
		const [sandlock, strace, dispatcher] = await Promise.all([
			resolveHostExecutable(this.options.sandlockBinary, "pi-speculative-sandlock", [path.join(os.homedir(), ".local", "bin", "pi-speculative-sandlock")]),
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
		return { sandlock, strace, fingerprint, platformFingerprint, observerFingerprint, executionContext, dispatcher, ...(imageLibrary ? { imageLibrary } : {}) };
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
		const prototype = await topLevelProcessPrototype(session.invocation, request, environment, session.projection, ready.platformFingerprint);
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
			execMounts: interception(session.interposition).execMounts,
			privateWrites: path.join(session.workspace.processRoot, "private"),
			command: [session.invocation.shell, ...shellArguments(session.invocation, command)],
			...(request.timeout !== undefined ? { timeoutSeconds: request.timeout } : {}),
		});
		const before = await session.workspace.structure.capture();
		session.privateSince ??= Date.now();
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
			session.topLevelExecution = { prototype, outcome, observedProcessMs: Math.max(0, performance.now() - processStarted) };
			try {
				const after = await session.workspace.structure.capture();
				const observation = await observeStrace(tracePrefix, session.invocation.shell, logicalCwd, {
					interposedExecutables: interception(session.interposition).executables, brokeredWrites: pid => brokeredWrites(session, pid), privateUpper: privateUpper(session),
					...(session.topLevelOutputEndpoints ? { outputEndpoints: session.topLevelOutputEndpoints } : {}),
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

	private async seal(session: ActiveSession, changes: readonly SandboxWorkspaceChange[]): Promise<readonly SandboxWorkspaceChange[]> {
		const external = await privateCommits(path.join(session.workspace.processRoot, "private"), session.privateSince ?? 0);
		// Adopting without them would leave the host unlike the native run: the Actor runs it instead.
		if (external.unrepresentable.length) throw new Error(`unrepresentable writes outside the workspace: ${external.unrepresentable.join(",")}`);
		const refined = [...await sealSessionEvidence(session, changes), ...external.changes];
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
		await replayFilesystemEffects(this.replayWorkspace, plan.artifacts, plan.certificate.result.journal, session.projection, session.workspace.sandboxRoot,
			path.join(session.workspace.processRoot, "private"));
		const after = await session.workspace.structure.capture();
		session.nestedEvidence.push(plan.certificate.dependencyCertificate);
		session.topLevelCapture = { before, after, observation: { complete: true, paths: [], taints: [], tracedProcesses: 0, incompleteReasons: [] } };
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
			result: await captureProcessResult(this.store, execution.outcome, execution.observedProcessMs, changes.map(change => ({ logicalPath: slash(change.target), change }))),
		});
		if (await this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS, this.options.witnessRepeats?.())) this.add(session, "wholeCommandPublished");
	}

	private serve(session: ActiveSession, socket: net.Socket): void {
		let body = "";
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => { body += chunk; if (Buffer.byteLength(body) > MAX_REQUEST_BYTES) socket.destroy(new Error("request too large")); });
		socket.once("error", () => undefined);
		socket.once("end", () => {
			const pending = Promise.resolve().then(() => this.handleWireRequest(session, body)).then((response) => { socket.end(wireResponse(response)); })
				.catch((error) => {
					this.setError(session, errorMessage(error));
					session.incompleteReasons.add(`broker:${errorMessage(error)}`);
					socket.end("f\n");
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
		const resources = request.descriptors && await this.inheritedPipes(request).catch((error: unknown) => `inherited_pipes:${errorMessage(error)}`);
		const eligibility = typeof resources === "string" ? { reason: resources } : await eligibleRequest(session, request, ready.executionContext);
		if ("reason" in eligibility) {
			this.add(session, "bypasses");
			const reason = `broker_bypass:${request.name}:${eligibility.reason}`;
			if (request.pid === undefined) session.incompleteReasons.add(reason);
			else session.bypasses.push([request.pid, reason]);
			return { kind: "bypass", executable };
		}
		const { argv0, args, cwd, environment } = request;
		return this.executeRequest(session, { argv0, args, cwd, environment, ...(typeof resources === "object" ? { resources, streams: true as const } : {}),
			...(eligibility.outputPipes ? { outputPipes: eligibility.outputPipes } : {}), ...(eligibility.outputFlags ? { outputFlags: eligibility.outputFlags } : {}) },
			executable, eligibility.route, requestID, undefined, undefined, request.pid);
	}

	/** A pipe of one repeated byte (a jobserver's tokens) ends as it began whatever order its bytes move in: only those are brokered. */
	private async inheritedPipes(request: DispatcherRequest): Promise<ProcessResourceGraph> {
		// A read past the snapshot is answered only by the run's own writer: without one, a writer elsewhere would leave it waiting forever.
		const ends = (inode: string, access: number) => request.descriptors!.some(entry => entry.inode === inode && (entry.flags & 3) === access);
		if (request.pid === undefined || request.descriptors!.some(({ inode }) => !ends(inode, 0) || !ends(inode, 1))) throw new Error("writer_outside_run");
		return captureHeldDescriptorInputs(request.pid, request.descriptors!.map(({ fd, alias, device, inode, flags, capacity, eof, queueHex }) =>
			({ fd, alias, device, inode, flags, capacity, eof, queueHex, type: "pipe" as const, offset: 0, owned: false, outside: 3 })),
		Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes), sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
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
		const observation = { complete: true, paths: [], taints: [], tracedProcesses: 0, incompleteReasons: [] }, before = await session.workspace.structure.capture();
		const result = await this.executeRequest(session, request, executable, invocation.outputRoute, session.metrics.requests, prototype,
			capture => { session.topLevelCapture = { ...capture, observation }; });
		// Only it and what it launched ran here: when its own transaction could not seal (what it launched overlapped it), the session's
		// endpoints stand for its interval, and what it observed joins its launches' evidence.
		if (result.kind !== "suspended") session.topLevelCapture ??= { before, after: await session.workspace.structure.capture(), observation };
		return { output: (result.output ?? []).map(({ fd, data }) => ({ fd, data: Buffer.from(data, "base64") })),
			...(result.kind === "suspended" ? { suspended: true as const } : { exit: result.exit! }) };
	}

	private async executeRequest(session: ActiveSession, request: ProcessArguments, executable: string, outputRoute: OutputRoute,
		requestID: number, prototype?: ExecPrototype,
		captureWorkspace?: (capture: Omit<TopLevelCapture, "observation">) => void, inPlace?: number): Promise<DispatcherResponse> {
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
			const result = await this.replay(session, acquired.plan, weakKey, acquired, request.streams && descriptorInputs(request.resources!));
			if (inPlace !== undefined) session.writers.add({ startedAt: Date.now(), endedAt: Date.now(), tracePrefix: "", writes: [], settled: true, pid: inPlace,
				written: acquired.plan.certificate.result.journal.flatMap(event => event.kind === "workspace" && pathContains(session.sourceRoot, event.path) ? [slash(path.relative(session.sourceRoot, event.path))] : []) });
			if (before) captureWorkspace!({ before, after: await session.workspace.structure.capture() });
			const binding = acquired.producer?.binding;
			if (binding && this.handoffs.resolveBinding(binding, session.scope)) session.executionBindings.set(requestID, binding);
			return result;
		}
		if (!acquired.work) throw new Error("process work reservation failed");
		this.add(session, "misses");
		try {
			// Without a result to reuse, a child whose recent runs were all cheap resumes in place within its parent's trace.
			if (inPlace !== undefined && Math.max(...this.childRunMs.get(prototype.executablePath) ?? [Infinity]) < (this.options.cheapChildMs ?? CHEAP_CHILD_MS)) {
				this.add(session, "bypasses");
				session.bypasses.push([inPlace, `broker_bypass:${path.posix.basename(prototype.executablePath)}:cheap_child`]);
				return { kind: "bypass", executable };
			}
			return await this.executeAndPublish(session, request, executable, prototype, weakKey, outputRoute, acquired.work, requestID, captureWorkspace, inPlace);
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
		executablePath: string, projection: ExecutionPathProjection, acceptProducer: (producer: ProcessProducerProof) => boolean,
		session?: ActiveSession, live?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<Sha256Digest>, continuation = false,
	): Promise<ReadyProcessPlan | undefined> {
		const plan = await this.planner.plan({ weakKey, executablePath, acceptProducer, excludedCertificates,
			contract: { sink: "buffered", orderedJournal: true, transactionalEffects: true, ...(continuation ? { continuation: true as const } : {}) },
			validation: { resolvePath: (logicalPath) => projection.toPhysical(logicalPath), ...(session ? { acceptedTaints: SAME_CONFINEMENT_TAINTS } : {}) },
			...(live ? { live: { certificate: live, acceptedTaints: [...TRANSFERRED_INPUT_TAINTS] } } : {}),
		});
		this.recordLookup(plan.lookup, session);
		if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
			const detail = `reuse_miss:${plan.reasons.join(",")}${plan.changedDependencies?.length ? `:${plan.changedDependencies.join(",")}` : ""}`;
			if (session) this.setError(session, detail); else this.setActorError(`actor_${detail}`);
		}
		return plan.kind !== "miss" ? plan : undefined;
	}

	private recordLookup(lookup: ProcessReusePlan["lookup"], session?: ActiveSession): void {
		for (const [metric, value] of [["validationMs", lookup.durationMs], ["validationCandidates", lookup.candidateCertificates],
			["validationPathsets", lookup.pathsetsValidated], ["validationFilesRead", lookup.filesRead], ["validationBytesRead", lookup.bytesRead],
			["validationArtifactsLoaded", lookup.artifactsLoaded], ["validationArtifactBytesRead", lookup.artifactBytesRead]] as const) {
			if (session) this.add(session, metric, value); else this.addActor(metric, value); }
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
			if (this.actorExecutableDirectories.size < 64) this.actorExecutableDirectories.add(path.dirname(executable));
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
			// Already learned in this call with nothing to adopt: a producer appearing later in the call is not worth an exec's round trip.
			// Once this call learns no new launch, only the executable decides.
			if (!learning && !available) { this.addActor("misses"); return { kind: "continue",
				repeat: observation && !observation.closed && observation.learned.size < LEARNED_LAUNCHES ? "launch" : "executable" }; }
			const inspected = await inspectHeldExecProcess(process.pid, executable, process.descriptors);
			const resources = process.descriptors?.length
				? await captureHeldDescriptorInputs(process.pid, process.descriptors, Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes),
					sensitivePaths(this.options.storeRoot, this.options.deniedPaths), observation?.closed ? undefined : observation?.inputs, process.tracerPid, sourceRoot) : undefined;
			const snapshot = { ...inspected, ...(resources ? { resources } : {}) };
			if (!pathContains(sourceRoot, snapshot.cwd)) { this.addActor("bypasses"); return { kind: "continue" }; }
			const observe = (prototype: ExecPrototype, durationMs: number) => {
				const weakKey = processWeakKey(prototype);
				this.processScheduler.observeActorService(processTimingIdentity(prototype, weakKey), durationMs);
				if (!learning || observation!.closed || process.signal?.aborted || !snapshot.outputRoute || observation!.bindings.size >= this.store.limits.maxCertificates) return;
				const binding = this.handoffs.observe(weakKey, executablePath, scope!, {
					argv0: snapshot.argv[0]!, args: snapshot.argv.slice(1), environment: snapshot.environment,
					cwd: projection.toLogical(snapshot.cwd), executable: executablePath, sourceRoot, outputRoute: snapshot.outputRoute,
					...(snapshot.outputPipes?.some(Boolean) ? { outputPipes: snapshot.outputPipes } : {}), ...(outputStatusFlags(snapshot.context) ? { outputFlags: outputStatusFlags(snapshot.context) } : {}),
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
			const prototype = bufferedProcessPrototype(snapshot, projection, await hashExecutableFile(`/proc/${process.pid}/exe`), await this.resolvePlatformFingerprint());
			const weakKey = processWeakKey(prototype), timing = processTimingIdentity(prototype, weakKey);
			const accepted = (producer: ProcessProducerProof) => actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
			const acquired = await this.acquireProcessResult(weakKey,
				(live, excluded) => this.plan(weakKey, executablePath, projection, accepted, undefined, live, excluded, true), process.signal, scope, { timing });
			const { plan, continuation } = acquired;
			if (!plan || (plan.certificate.result.continuation ? !continuation || sha256Digest(continuation.image) !== plan.certificate.result.continuation.imageDigest :
				plan.certificate.result.exit.kind !== "code")) {
				this.addActor("misses");
				return { kind: "continue", observeCompletion: durationMs => { if (durationMs !== undefined) observe(prototype, durationMs); } };
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
					if (scope) acquired.producer?.ownership.adopted({ scope, id: process.id, sequence: process.sequence, operationIdentity: weakKey,
						executionMs: plan.certificate.result.observedProcessMs ?? 0 });
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
		inputs?: ReturnType<typeof descriptorInputs>,
	): Promise<DispatcherResponse> {
		const started = performance.now();
		let replayed = false;
		try {
			const { artifacts, certificate } = plan;
			const output = wireOutput(loadOutputEvents(artifacts, certificate.result.journal));
			await replayFilesystemEffects(this.replayWorkspace, artifacts, certificate.result.journal, session.projection, session.workspace.sandboxRoot,
				path.join(session.workspace.processRoot, "private"));
			session.nestedEvidence.push(certificate.dependencyCertificate);
			this.recordHit(acquired.producer?.scope, acquired.joined, session);
			session.computations.push(reusedComputation(certificate.result, started, acquired));
			replayed = true;
			const streams = inputs && streamSettlement(inputs, (certificate.result.resources?.transitions ?? []).map(event => ({ alias: event.id, kind: event.kind, data: artifacts.read(event.data) })));
			if (inputs && !streams) return { kind: "hit", weakKey, output: [], exit: { kind: "code", code: 125 } };
			return { kind: "hit", weakKey, output, exit: certificate.result.exit, ...(streams?.length ? { streams } : {}) };
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
		inPlace?: number,
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
		let stage = "capture", dependencyCertificate: DynamicDependencyCertificate | undefined, certificateID: Sha256Digest | undefined, unattributed: StraceObservation | undefined;
		let continuation: ProcessContinuation | undefined, frozen: { pid: number; fd: number; syscall: string; bytes: number } | undefined;
		const executedStreams: { alias: number; kind: string; data: Buffer }[] = [];
		let suspensionAttempted = false, writer: ActiveSession["writers"] extends Set<infer Writer> ? Writer | undefined : never;
		const failureDetail = (error: unknown) => `${errorMessage(error)}; process=${JSON.stringify({
			stage, requestID, weakKey, scope: session.scope, workspace: session.workspace.sandboxRoot,
			executable: prototype.executablePath, certificateID, complete: dependencyCertificate?.complete, taints: dependencyCertificate?.taints,
		})}`;
		try {
			traceRoot = await mkdtemp(path.join(session.workspace.processRoot, "trace-"));
			const tracePrefix = path.join(traceRoot, "process");
			const logicalExecutable = session.projection.toLogical(executable);
			const { execMounts, executables: interposedExecutables } = interception(session.interposition, executable), image = logicalExecutable;
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
				for (const [image, object] of Object.entries(request.resources?.objects ?? {})) for (const message of object.queue?.messages ?? [])
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
					inputs ??= new Set((await observeStrace(tracePrefix, image, logicalCwd, { previewBytes: 1024 * 1024 }))
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
				// A nested child's own children are brokered as the top level's are: a build tool's compilers run in its trace otherwise.
				...sandboxPolicyArguments(
					logicalCwd,
					session.deniedPaths,
					[session.workspace.sandboxRoot, session.socketPath, ...(descriptorReportPath ? [descriptorReportPath] : []),
						...[...descriptorImages.values()].filter(image => !image.workspace).map(image => image.physical)],
					[{ virtualPath: session.sourceRoot, hostPath: session.workspace.sandboxRoot, readOnly: false },
						...(session.gitDirectory ? [{ virtualPath: session.gitDirectory, hostPath: session.gitDirectory, readOnly: true }] : []), ...session.interposition.mounts],
					execMounts, path.join(session.workspace.processRoot, "private"),
				),
				...inheritedFiles.flatMap((_, index) => ["--preserve-fd", String(index + 3)]),
				...directoryImages.flatMap(([directory, image]) => ["--directory-image", sandboxMountArgument({ virtualPath: directory, hostPath: image, readOnly: false })]),
				"--",
				ready.dispatcher,
				descriptorManifest ? "--exec-fds" : request.closeStdin ? "--exec-closed-input" : "--exec",
				outputRoute.join("") + (outputPipes ? request.outputPipes!.map(pipe => pipe ? "p" : "s").join("") : "") + (request.outputFlags ? `,${request.outputFlags.join(",")}` : ""),
				...(descriptorManifest ? [descriptorManifest, descriptorReportPath!] : []),
				request.argv0,
				image,
				...request.args,
			], resourceJournal, live);
			const processStarted = performance.now();
			session.writers.add(writer = { startedAt: Date.now(), tracePrefix, ...(inPlace !== undefined ? { pid: inPlace } : {}) });
			const clockOffset = Number(process.hrtime.bigint()) / 1e6 - performance.now();
			stage = "execution";
			let outputEndpoints: readonly [string, string] | undefined;
			outcome = await runSpawn(ready.strace, [...(live ? [`--handoff-fd=${inheritedFiles.length + 3}`, `--handoff-library=${ready.imageLibrary}`, `--handoff-image=${imagePath}`] : []), ...command.slice(1)], {
				// The child writes to (and may query) these sockets; their identity lets its observation recognize them.
				onOutputEndpoints: (endpoints) => { session.nestedOutputEndpoints.add(outputEndpoints = endpoints); },
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
			writer.endedAt = Date.now();
			if (outputEndpoints) session.nestedOutputEndpoints.delete(outputEndpoints);
			// Replays write each event to the target's own descriptor, whichever outlet this route gave it.
			outcome = { ...outcome, output: outcome.output.map(event => ({ ...event, fd: outputRoute[0] === event.fd ? 1 : 2 })) };
			const observedProcessMs = continuation ? continuation.computation.completedAt - continuation.computation.startedAt : Math.max(0, performance.now() - processStarted);
			if (!continuation) this.childRunMs.set(prototype.executablePath, [observedProcessMs, ...this.childRunMs.get(prototype.executablePath) ?? []].slice(0, 8));
			releaseInputs();
			try {
				if (suspensionAttempted && !continuation) throw new Error("private process suspension was not sealed");
				const descriptorOffsets = descriptorReport && inputs.length ? parseDescriptorOffsets(await descriptorReport.readFile(), inputs) : undefined;
				transactionFinishing = true;
				const observing = observeStrace(tracePrefix, image, session.projection.toLogical(request.cwd), { interposedExecutables, brokeredWrites: pid => brokeredWrites(session, pid, writer), privateUpper: privateUpper(session),
						...(frozen ? { frozen } : {}), ...(outputEndpoints ? { outputEndpoints } : {}),
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
					});
				const roots = [session.workspace.sandboxRoot, session.sourceRoot], own = writer!;
				const named = (target: string) => roots.flatMap(root => pathContains(root, target) ? [slash(path.relative(root, target))] : [])[0];
				// An inherited writable workspace file is written outside any path the trace names.
				const ownership = async (): Promise<WorkspaceTransactionOwnership | undefined> => inputs.some(input => !input.type && (input.flags & 3) !== 0) ? undefined : observing.then(async observation => {
					// When it last looked at each workspace name; one the trace cannot place counts as seen at its end.
					const looked = new Map<string, number>();
					for (const [target, at] of await tracedObservations(tracePrefix)) { const name = named(target); if (name !== undefined) looked.set(name, Math.max(looked.get(name) ?? 0, at)); }
					// A run brokered from its own tree, which its trace holds as a launcher, wrote on its behalf.
					const traced = new Set(observation.pids);
					const children = [...session.writers].filter(other => other !== own && other.pid !== undefined && traced.has(other.pid));
					const descendants = own.descendants = [...new Set(children.flatMap(child => [child, ...child.descendants ?? []]))];
					own.written = observation.written ?? [];
					return { written: new Set(own.written), observed: new Map(observation.paths.flatMap(({ path: target }) => named(target) ?? []).map(name => [name, looked.get(name) ?? own.endedAt!] as const)), endedAt: own.endedAt!,
					interfered: async (paths, until) => {
						const targets = new Set([...paths].flatMap(name => roots.map(root => path.posix.join(root, name))));
						for (const other of session.writers) if (other !== own && !descendants.includes(other) && other.startedAt <= until && (other.endedAt ?? Infinity) >= own.startedAt &&
							writesWithin(other.writes ?? await tracedWrites(other.tracePrefix), targets, own.startedAt, until)) return true;
						return false;
					},
				}; });
				const captures = [transaction.finish(ownership), observing] as const;
				const [delta, observation] = await Promise.all(captures).catch(async (error: unknown) => {
					// Both captures own live workspace/trace resources until they settle.
					stage = (await Promise.allSettled(captures)).flatMap((result, index) =>
						result.status === "rejected" ? [index === 0 ? "transaction_capture" : "trace_capture"] : []).join("+");
					throw error;
				});
				own.written ??= observation.written ?? []; // What its launcher's trace answers for, whether or not its interval overlapped another.
				for (const pid of observation.resumedInterpositions ?? []) session.resumed.add(pid);
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
					unattributed = observation;
					throw new Error(`workspace transaction is incomplete: ${delta.reason}`);
				}
				const { before, after } = delta;
				// A bound child is the entire execution interval; its transaction already sealed both endpoints.
				captureWorkspace?.({ before, after });
				stage = "workspace_effects";
				const effects = diffWorkspaceStructures(before, after, delta.changes, session.projection);
				if (!effects.complete) unattributed = observation;
				stage = "dependencies";
				const evidence = await captureDependencies(session, before, observation.paths, effects, observation);
				if (evidence.incompleteReasons.length) this.setError(session, `evidence:${evidence.incompleteReasons.join(",")}`);
				const taints = new Set<ProvenanceTaint>(observation.taints);
				// Private images preserve FD/OFD relations, but cannot also represent an independently accessed pathname.
				if (inputs.some(descriptor => !descriptorImages.get(descriptor.image)?.workspace && descriptor.sourcePath && observation.paths.some(observed =>
					path.resolve(observed.path) === descriptor.sourcePath || observed.role !== "metadata" && pathContains(observed.path, descriptor.sourcePath!)))) {
					taints.add("untracked_fd");
				}
				for (const taint of evidence.taints) taints.add(taint);
				if (!observation.complete) taints.add("trace_incomplete");
				// A brokered descendant that did not resume in this trace ran outside it.
				const traced = new Set(observation.pids);
				const escaped = session.bypasses.some(([pid]) => traced.has(pid) && !observation.resumedInterpositions?.includes(pid));
				// What it left outside the workspace commits with its workspace effects; a change no baseline can stand for does not replay.
				const external = await privateCommits(path.join(session.workspace.processRoot, "private"), own.startedAt, observation.external ?? []);
				if (external.unrepresentable.length) this.setError(session, `evidence:${external.unrepresentable.join(",")}`);
				dependencyCertificate = {
					complete: observation.complete && evidence.complete && !escaped && !external.unrepresentable.length,
					dependencies: evidence.dependencies,
					taints: [...taints],
				};
				stage = "artifacts";
				const baseResult = await captureProcessResult(this.store, outcome, observedProcessMs,
					[...effects.effects, ...external.changes.map(change => ({ logicalPath: slash(change.target), change }))]);
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
					executedStreams.push({ alias: input.alias, kind: event.kind, data: event.data });
					transitions.push({ id: input.alias, kind: event.kind, data: await this.store.artifacts.put(event.data), ...(event.requested !== undefined ? { requested: event.requested } : {}) });
				}
				const { exit, ...prefixResult } = baseResult;
				const result: ProcessResultRecord = { ...prefixResult, ...(continuation ? { continuation: { imageDigest: sha256Digest(continuation.image), imageBytes: continuation.image.length } } : { exit: exit! }),
					...(descriptorOffsets ? { resources: { ...descriptorEffects(request.resources!, descriptorOffsets), ...(resourceJournal ? { transitions } : {}) } } : {}) };
				stage = "certificate";
				const certificate = sealProcessCertificate({ prototype, producer: session.nestedProducer, dependencyCertificate, result });
				certificateID = certificate.id;
				session.nestedEvidence.push(certificate.dependencyCertificate);
				if (taints.size) { this.add(session, "tainted"); this.setError(session, `tainted:${[...taints].join(",")}`); }
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
							...(request.outputPipes ? { outputPipes: request.outputPipes } : {}), ...(request.outputFlags ? { outputFlags: request.outputFlags } : {}),
							...(request.closeStdin ? { closeStdin: true } : {}),
								...(request.resources ? { resources: request.resources } : {}),
						});
						if (binding) session.executionBindings.set(requestID, binding);
						stage = "history_publication";
						return this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS, this.options.witnessRepeats?.()).catch((error: unknown) => {
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
				// Its command captures the effects whole: what it observed joins the command's evidence, measured from the command's start.
				if (unattributed?.complete && !unattributed.incompleteReasons.length && !continuation) session.foldedObservations.push(unattributed);
				else session.incompleteReasons.add(`nested_capture:${detail}`);
			}
			if (continuation) return { kind: "suspended", weakKey };
			const exit = exitOutcome(outcome);
			const streams = request.streams && streamSettlement(descriptorInputs(request.resources!), [...executedStreams]);
			if (streams === undefined && request.streams) {
				this.setError(session, "stream_settlement_unrepresentable");
				return { kind: "executed", weakKey, output: [], exit: { kind: "code", code: 125 } };
			}
			return { kind: "executed", weakKey, output: wireOutput(outcome.output), exit, ...(streams?.length ? { streams } : {}) };
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
			if (writer) {
				writer.writes ??= await tracedWrites(writer.tracePrefix).catch(() => [{ at: writer!.startedAt, opened: true }]);
				writer.settled = true;
				if ([...session.writers].every(other => other.settled)) session.writers.clear();
			}
			if (traceRoot) await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
		}
	}

	private add(session: ActiveSession, metric: CountedReuseMetric, value = 1): void {
		this.counters[metric] += value;
		session.metrics[metric] += value;
	}

	private addActor(metric: CountedReuseMetric, value = 1): void { this.counters[metric] += value; this.actorCounters[metric] += value; }

	private recordHit(
		producer: ExecutionScope | undefined,
		joined: boolean,
		session?: ActiveSession,
		scope: ExecutionScope | undefined = session?.scope,
	): void {
		const add = (metric: CountedReuseMetric) => session ? this.add(session, metric) : this.addActor(metric);
		add("hits");
		if (joined) add("joinedHits");
		add(producer && scope ? sameScope(producer, scope) ? "sameTurnHits" : "crossTurnHits" : "unattributedHits");
	}

	private setError(session: ActiveSession, detail: string): void { this.counters.lastError = detail; session.metrics.lastError = detail; }

	private setActorError(detail: string): void { this.counters.lastError = detail; this.actorCounters.lastError = detail; }

	private async resolveRequestedExecutable(session: ActiveSession, request: DispatcherRequest): Promise<string> {
		if (!request.name || request.name.includes("/") || request.name.includes("\0")) throw new Error("invalid executable name");
		if (path.isAbsolute(request.invokedPath) && path.basename(request.invokedPath) === request.name) {
			const invoked = path.resolve(request.invokedPath);
			const covered = session.interposition.directories.find(
				(directory) => [directory.target, directory.view].some((candidate) => path.resolve(path.dirname(invoked)) === path.resolve(candidate)),
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

	private async prototype(session: ActiveSession, request: ProcessArguments, executable: string, outputRoute: OutputRoute): Promise<ExecPrototype> {
		const [executableDigest, ready] = await Promise.all([hashExecutableFile(executable), this.resolveReady()]);
		return bufferedProcessPrototype({
			executable,
			argv: [request.argv0, ...request.args],
			cwd: request.cwd,
			environment: request.environment,
			context: routedProcessContext(ready.executionContext, outputRoute, request.closeStdin, descriptorInputs(request.resources).filter(input => input.installed !== false), request.outputPipes, request.outputFlags),
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
		environment: Object.fromEntries(Object.entries(snapshot.environment).map(([name, value]) => [name, projection.normalizeValue(value)])),
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
		channel.write(request, error => { if (error) finish(error); else { try { if (!wake()) closed(); } catch (error) { finish(error as Error); } } });
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

async function sealSessionEvidence(session: ActiveSession, changes: readonly SandboxWorkspaceChange[]): Promise<readonly SandboxWorkspaceChange[]> {
	const capture = session.topLevelCapture;
	if (!capture) {
		session.incompleteReasons.add("top_capture_missing");
		session.topLevelEvidence ??= { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error(`top-level workspace capture is missing: ${[...session.incompleteReasons].filter((reason) => reason.startsWith("top_capture:")).join("; ") || "not run"}`);
	}
	// A bypass that did not resume in place ran outside the top-level trace.
	for (const [pid, reason] of session.bypasses) if (!capture.observation.resumedInterpositions?.includes(pid) && !session.resumed.has(pid)) session.incompleteReasons.add(reason);
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
		...(change.beforeIdentity ? { beforeIdentity: change.beforeIdentity } : {}),
	}]);
	const effects = diffWorkspaceStructures(capture.before, capture.after, regularDeltas, session.projection);
	if (!effects.complete) {
		session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
		throw new Error(`top-level workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	}
	const directoryChanges = await sourceDirectoryChanges(session, effects.effects);
	try {
		const observed = [capture.observation, ...session.foldedObservations];
		const evidence = await captureDependencies(session, capture.before, observed.flatMap(observation => observation.paths), effects, { external: observed.flatMap(observation => observation.external ?? []),
			locks: observed.flatMap(observation => observation.locks ?? []), written: observed.flatMap(observation => observation.written ?? []) }, session.nestedEvidence);
		for (const reason of evidence.incompleteReasons) session.incompleteReasons.add(`top_evidence:${reason}`);
		session.topLevelEvidence = mergeDependencyEvidence(
			[
				{
					complete: observed.every(observation => observation.complete) && evidence.complete,
					dependencies: evidence.dependencies,
					taints: [...new Set([...observed.flatMap(observation => observation.taints), ...evidence.taints])],
				},
				{ complete: true, dependencies: session.interposition.dependencies, taints: [] },
				...evidence.runs,
			],
			session.incompleteReasons,
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

/** binfmt_script's interpreter (the first word after `#!`) or a little-endian ELF64's PT_INTERP. */
export async function imageInterpreter(file: string): Promise<string | undefined> {
	const handle = await open(file, "r").catch(() => undefined);
	try {
		const head = Buffer.alloc(256), read = handle ? (await handle.read(head, 0, 256, 0)).bytesRead : 0;
		if (head.subarray(0, 2).toString("latin1") === "#!") return /^[ \t]*([^ \t\n\0]+)/.exec(head.subarray(2, read).toString("latin1"))?.[1];
		if (read < 64 || head.readUInt32BE(0) !== 0x7f454c46 || head[4] !== 2 || head[5] !== 1) return undefined;
		const offset = Number(head.readBigUInt64LE(32)), size = head.readUInt16LE(54), count = Math.min(head.readUInt16LE(56), 64);
		const table = Buffer.alloc(size * count);
		await handle!.read(table, 0, table.length, offset);
		for (let entry = 0; size >= 56 && entry < count; entry++) {
			if (table.readUInt32LE(entry * size) !== 3) continue;
			const name = Buffer.alloc(Math.min(Number(table.readBigUInt64LE(entry * size + 32)), 4096));
			await handle!.read(name, 0, name.length, Number(table.readBigUInt64LE(entry * size + 8)));
			return name.toString("latin1").split("\0")[0] || undefined;
		}
	} finally {
		await handle?.close();
	}
}

async function captureDependencies(
	session: ActiveSession,
	snapshot: WorkspaceStructureSnapshot,
	observed: readonly ObservedProcessPath[],
	effects: WorkspaceTransactionDiff,
	{ external = [], locks = [], written = [] }: Pick<StraceObservation, "external" | "locks" | "written"> = {},
	runs: readonly DynamicDependencyCertificate[] = [],
) {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason}`);
	// The workspace as the run found it: the structure it began with, and the bytes its own writes replaced.
	const deltas = new Map(effects.effects.flatMap(({ relativePath, change }) => change.kind === "directory" ? [] : [[relativePath, change] as const]));
	const cached = new Map<string, Promise<WorkspaceTreeEntry | undefined>>();
	const before = (physicalPath: string) => {
		const relative = relativeFilesystemPath(snapshot.root, physicalPath);
		if (relative === undefined) return Promise.reject(new Error(`workspace dependency escapes snapshot: ${physicalPath}`));
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
	// A name it created itself depends only on having been free, not on everything else its directory held.
	const created = new Set(written.map(name => path.resolve(session.workspace.sandboxRoot, name)));
	const workspaceDependency = async (physical: string, logical: string, role: Exclude<ObservedProcessPath["role"], "metadata">, listed = true) => {
		const [entry, parent] = await Promise.all([before(physical),
			path.resolve(physical) === path.resolve(session.workspace.sandboxRoot) || created.has(path.resolve(physical)) ? undefined : before(path.dirname(physical))]);
		return snapshotDependency(logical, entry?.kind === "file" && entry.aliases ? { ...entry,
			aliases: entry.aliases.map(name => session.projection.toLogical(name)).sort() } : entry, parent, role, {
			excludedEntries: workspaceMetadataExclusions(session, physical),
			parentExcludedEntries: workspaceMetadataExclusions(session, path.dirname(physical)), listed,
		});
	};
	const dependencies = new Map<string, DynamicDependency>();
	const taints = new Set<ProvenanceTaint>();
	const incompleteReasons = new Set<string>();
	let complete = true;
	const add = (dependency: DynamicDependency | undefined, reason = "dependency_unavailable") => {
		if (!dependency) { complete = false; incompleteReasons.add(reason); return; }
		const identity = dynamicDependencyIdentity(dependency);
		const existing = dependencies.get(identity);
		if (existing?.kind === "file" && dependency.kind === "file" && (existing.role === "executable" || dependency.role !== "executable")) return;
		dependencies.set(identity, dependency);
	};
	// A run saw the workspace through the command's earlier writes: where what it saw is not how the command found a path,
	// the command depended on that path's state at its start.
	const changed = new Set(effects.effects.map(effect => effect.logicalPath));
	const atStart = async (dependency: DynamicDependency) => {
		const physical = dependency.kind === "fd" ? undefined : session.projection.toPhysical(dependency.path);
		const relative = physical && relativeFilesystemPath(snapshot.root, physical);
		if (dependency.kind === "fd" || !physical || relative === undefined || session.workspace.observationExcludes.includes(relative.split(path.sep)[0]!)) return dependency;
		const entry = snapshot.entries.get(relative), up = relativeFilesystemPath(snapshot.root, path.dirname(physical)), parent = up === undefined ? undefined : snapshot.entries.get(up);
		const same = dependency.kind === "absence" ? !entry && (dependency.parentEntriesDigest === undefined || parent?.kind === "directory" && parent.entriesDigest === dependency.parentEntriesDigest)
			: dependency.kind === "directory" ? entry?.kind === "directory" && entry.metadataDigest === dependency.metadataDigest && (dependency.entriesDigest ?? entry.entriesDigest) === entry.entriesDigest
			: dependency.kind === "symlink" ? entry?.kind === "symlink" && entry.target === dependency.target
			: dependency.kind === "lock" ? entry?.kind === "file"
			: !changed.has(dependency.path) && (dependency.kind === "metadata" ? entry !== undefined : entry?.kind === "file" && entry.metadataDigest === dependency.metadataDigest);
		return same ? dependency : workspaceDependency(physical, dependency.path, dependency.kind === "file" ? dependency.role : "input", dependency.kind === "directory" && !!dependency.entriesDigest);
	};

	// Kernel pathname walk over the baseline: links are recorded and expanded in place and `..` leaves the directory
	// actually reached. Sandlock collapses `..` first, so another result means the sandbox read another object.
	const walk = async (logical: string, follow: boolean) => {
		const pending = logical.split("/").filter(Boolean), links: string[] = [];
		let current = "/";
		for (let segment = pending.shift(); segment !== undefined; segment = pending.shift()) {
			if (segment === "..") { current = path.posix.dirname(current); continue; }
			const next = path.posix.join(current, segment);
			// The repository is the workspace's own, mounted read-only (also when reached through the overlay): the host walk captures it.
			const repository = session.projection.isWorkspacePhysical(next) ? session.projection.toLogical(next) : next;
			if (session.gitDirectory && pathContains(session.gitDirectory, repository)) return { path: path.posix.join(repository, ...pending), links };
			const physical = pathContains(session.sourceRoot, next) ? session.projection.toPhysical(next) : undefined;
			const entry = physical ? await before(physical) : undefined;
			if (entry?.kind === "file" && pending.length) return undefined; // Native ENOTDIR; lexical collapse would continue.
			if (entry?.kind !== "symlink" || !pending.length && !follow) { current = next; continue; }
			if (links.push(next) > 40) return undefined;
			pending.unshift(...entry.target.split("/").filter(Boolean));
			if (entry.target.startsWith("/")) current = "/";
		}
		return { path: current, links };
	};
	const interposed = new Set(session.interposition.entries.flatMap(entry => [path.resolve(entry.intercepted), path.resolve(entry.view)]));
	const own = new Set(external.flatMap(target => [path.resolve(target), path.dirname(path.resolve(target))]));
	// A lock on a file it made itself no one else could have held; on one that was there, another holder would have refused it.
	for (const lock of locks) if (await (session.projection.isWorkspacePhysical(lock.path) ? before(lock.path) : lstat(lock.path).catch(() => undefined))) {
		add({ kind: "lock", path: session.projection.isWorkspacePhysical(lock.path) ? session.projection.toLogical(lock.path) : slash(lock.path), exclusive: lock.exclusive });
	}
	const pending = [...observed], seenImages = new Set<string>(), hostPaths = new Map<string, { physical: string; role: Exclude<ObservedProcessPath["role"], "metadata">; listed: boolean }>();
	for (let item = pending.shift(); item; item = pending.shift()) {
		const follow = item.role !== "metadata" || item.followSymlinks, walked = await walk(item.path, follow);
		if (!walked || walked.path !== (item.path.split("/").includes("..") ? (await walk(path.posix.normalize(item.path), follow))?.path : walked.path)) {
			add(undefined, `pathname_walk:${item.path}`); continue;
		}
		for (const link of item.role === "metadata" ? [] : walked.links) add(await workspaceDependency(session.projection.toPhysical(link)!, link, "input"));
		const resolved = item.role === "metadata" ? path.resolve(item.path) : walked.path;
		if (interposed.has(resolved)) continue;
		// A native image resumed in place runs from its shadow, a read-only mount of the original directory.
		const shadow = session.interposition.directories.find((directory) => pathContains(directory.shadow, resolved));
		const observedPath = shadow ? path.join(shadow.source, path.relative(shadow.shadow, resolved)) : resolved;
		if (session.deniedPaths.some((denied) => pathContains(denied, observedPath))) { taints.add("escaped_sandbox"); incompleteReasons.add(`denied:${observedPath}`); continue; }
		const physical = pathContains(session.sourceRoot, observedPath) && !(session.gitDirectory && pathContains(session.gitDirectory, observedPath))
			? (session.projection.toPhysical(observedPath) ?? observedPath)
			: observedPath;
		// A loose object or pack is named by its content: its presence and size stand for its bytes.
		if (session.gitDirectory && item.role !== "metadata" && CONTENT_ADDRESSED_GIT.test(path.relative(session.gitDirectory, physical))) {
			const info = await lstat(physical, { bigint: true }).catch(() => undefined);
			if (info?.isFile()) { add({ kind: "metadata", path: slash(physical), followSymlinks: false, fields: ["mode", "size"], digest: filesystemObservationDigest(info, ["mode", "size"]) }); continue; }
		}
		if (KERNEL_CONFIGURATION.test(observedPath)) continue; // Changes only with the kernel's own configuration, like the clock.
		// What it wrote outside the workspace, and the directories holding those names, rest on its effects' baselines.
		if (own.has(physical)) continue;
		// A process reading its own state, or the host's CPU and cgroup limits, observes this one run like the clock or its pid.
		if (/^\/proc\/(?:self|thread-self|\d+)(?:\/|$)/.test(observedPath)) { taints.add("pid_observation"); continue; }
		// Reopening an inherited descriptor reaches what that descriptor carries, like a jobserver pipe, not a host file.
		if (/^\/dev\/fd\/\d+$/.test(observedPath)) { taints.add("descriptor_observation"); continue; }
		if (/^\/dev\/u?random$/.test(observedPath)) { taints.add("random"); continue; } // Entropy, as getrandom(2) reads it.
		if (/^\/sys\/(?:devices\/system\/cpu|fs\/cgroup)(?:\/|$)|^\/proc\/(?:meminfo|version|version_signature|cpuinfo|stat|loadavg|uptime)$/.test(observedPath)) { taints.add("clock"); continue; }
		if (item.role === "metadata") {
			if (created.has(path.resolve(physical))) continue; // Its own writes set what it saw of them.
			add(await atStart({
				kind: "metadata",
				path: session.projection.isWorkspacePhysical(physical) ? session.projection.toLogical(physical) : slash(physical),
				followSymlinks: item.followSymlinks,
				digest: item.digest,
				...(item.fields ? { fields: item.fields } : {}),
			}));
			continue;
		}
		if (STABLE_SANDBOX_DEVICES.has(observedPath)) continue;
		// The kernel itself opens a script's interpreter and an ELF's loader, which no traced syscall names.
		const interpreter = item.role === "executable" && seenImages.size < 16 && !seenImages.has(physical) && seenImages.add(physical)
			? await imageInterpreter(physical) : undefined;
		if (interpreter?.startsWith("/")) pending.push({ path: interpreter, role: "executable" });
		if (session.projection.isWorkspacePhysical(physical)) {
			add(await workspaceDependency(physical, session.projection.toLogical(physical), item.role, !!item.listed));
			continue;
		}
		hostPaths.set(`${item.role}\0${physical}`, { physical, role: item.role, listed: !!item.listed });
	}
	// Host files are independent of each other and of the workspace: capture them concurrently, add them in trace order.
	const hostCaptures = await mapFilesystem([...hostPaths.values()], ({ physical, role, listed }) =>
		captureHostPath(physical, role, listed).then(value => ({ value }), (error: unknown) => ({ error })));
	[...hostPaths.values()].forEach(({ physical }, index) => {
		const captured = hostCaptures[index]!;
		if ("error" in captured) { complete = false; taints.add("trace_incomplete"); incompleteReasons.add(`capture:${physical}:${errorMessage(captured.error)}`); }
		else if (captured.value) for (const dependency of captured.value) add(dependency);
		else { taints.add("mutable_input"); add(undefined, `mutable:${physical}`); }
	});
	for (const effect of effects.effects) {
		const physical = session.projection.toPhysical(effect.logicalPath);
		if (!physical) { complete = false; incompleteReasons.add(`effect_unmapped:${effect.logicalPath}`); continue; }
		add(await workspaceDependency(physical, effect.logicalPath, "input"));
	}
	const rebased = await mapFilesystem(runs, async run =>
		({ ...run, dependencies: (await mapFilesystem(run.dependencies, atStart)).flatMap(dependency => dependency ?? (add(undefined, "start_state_unavailable"), [])) }));
	return { complete, dependencies: [...dependencies.values()], taints: [...taints], incompleteReasons: [...incompleteReasons], runs: rebased };
}

const STABLE_SANDBOX_DEVICES = new Set(["/dev/null", "/dev/tty", "/dev/zero", "/dev/full"]);
const CONTENT_ADDRESSED_GIT = /^objects\/(?:[0-9a-f]{2}\/[0-9a-f]{38}(?:[0-9a-f]{24})?|pack\/pack-[0-9a-f]{40}(?:[0-9a-f]{24})?\.(?:pack|idx|rev|bitmap))$/;
/** Also what memory allocators (jemalloc, glibc) read at startup to size their mappings. */
const KERNEL_CONFIGURATION = /^\/proc\/(?:filesystems|mounts|sys\/vm\/overcommit_memory)$|^\/sys\/kernel\/mm\/transparent_hugepage\//;
const SAME_CONFINEMENT_TAINTS = ["confinement_observation"] as const;

function workspaceMetadataExclusions(session: ActiveSession, target: string): readonly string[] | undefined {
	return path.resolve(target) === path.resolve(session.workspace.sandboxRoot) ? session.workspace.observationExcludes : undefined;
}

async function captureHostPath(
	physicalPath: string,
	role: Exclude<ObservedProcessPath["role"], "metadata">,
	listed: boolean,
): Promise<readonly DynamicDependency[] | undefined> {
	const dependencies: DynamicDependency[] = [];
	for await (const { path: current, info, link, terminal } of walkFilesystemPath(path.resolve(physicalPath))) {
		// A runtime socket's absence validates exactly; what exists under these roots changes without a trace.
		if (["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp"].some((root) => pathContains(root, current)) && (!pathContains("/run", current) || info && terminal)) return undefined;
		if (!info) {
			const absence = await captureAbsenceDependency(current, slash(current), false); // A case-sensitive lookup proves itself; siblings may come and go.
			if (!absence) throw new Error("host dependency changed during capture");
			return [...dependencies, absence];
		}
		// Root's files, and this user's own outside the workspace (a PATH through ~/.local or nvm), validate exactly; no one else may write them.
		if (info.uid !== 0n && info.uid !== BigInt(process.getuid?.() ?? -1) || (link === undefined && (info.mode & 0o022n) !== 0n)) return undefined;
		if (link !== undefined) dependencies.push({ kind: "symlink", path: slash(current), target: link, targetDigest: sha256Digest(Buffer.from(link, "utf8")) });
		else if (terminal && info.isFile()) dependencies.push((await captureFileDependency(current, slash(current), role, { includeMetadata: true })).dependency);
		// A listing reveals names; a program that stats the directory records that metadata on its own.
		else if (terminal && info.isDirectory()) dependencies.push(await captureDirectoryDependency(current, slash(current), !listed, [], listed));
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
	privateStorage?: string,
): Promise<void> {
	const changes: SandboxWorkspaceChange[] = [];
	for (const event of journal) {
		if (event.kind === "output") continue;
		const target = projection.toPhysical(event.path), inside = !!target && pathContains(workspaceRoot, target);
		if (!target || target === path.resolve(workspaceRoot) || !inside && pathContains(projection.sourceRoot, target)) {
			throw new Error(`replay effect escapes workspace: ${event.path}`);
		}
		const root = inside ? workspaceRoot : await hostRoot(target), resource = slash(path.relative(root, target));
		if (event.before.kind === "directory" || event.after.kind === "directory") {
			if (event.before.kind !== "absent" && event.before.kind !== "directory") throw new Error(`unsupported replay type change: ${event.path}`);
			if (event.after.kind !== "absent" && event.after.kind !== "directory") throw new Error(`unsupported replay type change: ${event.path}`);
			changes.push({
				kind: "directory",
				root,
				target,
				resource,
				...(event.before.kind === "directory" ? { before: directoryState(event.before) } : {}),
				...(event.after.kind === "directory" ? { after: directoryState(event.after) } : {}),
			});
			continue;
		}
		changes.push({
			root,
			target,
			resource,
			...(event.operation ? { operation: event.operation } : {}),
			...(event.object ? { object: { ...event.object, path: projection.toPhysical(event.object.path)! } } : {}),
			...(event.aliases ? { aliases: event.aliases.map(name => projection.toPhysical(name)!) } : {}),
			...(event.before.kind === "file" ? { before: artifacts.read(event.before.data), beforeMode: event.before.mode } : {}),
			...(event.after.kind === "file" ? { after: artifacts.read(event.after.data), afterMode: event.after.mode, afterModified: event.after.modified } : {}),
		});
	}
	// Inside a speculative session, what lies outside the workspace belongs to its private branch until adoption.
	const external = privateStorage ? changes.filter(change => !pathContains(workspaceRoot, change.target)) : [];
	if (external.length) await privateReplay(privateStorage!, external);
	if (changes.length > external.length) await owner.commitDelta({ output: { result: { content: [], details: {} }, isError: false }, changes: changes.filter(change => !external.includes(change)) });
}

/** Apply changes in a session's private branch as its sandboxes would have: over the merged view's state, into its upper
 * directory, a removal of a host object as a whiteout its sandboxes read. */
async function privateReplay(storage: string, changes: readonly SandboxWorkspaceChange[]): Promise<void> {
	const { upper, log, deleted } = await privateBranch(storage);
	const merged = async (target: string) => (await lstat(path.join(upper, target)).then(() => path.join(upper, target), () => undefined)) ?? (deleted(target) ? undefined : target);
	for (const change of changes) {
		const current = await merged(change.target);
		const state = current === undefined ? undefined : change.kind === "directory" ? await readSandboxDirectoryState(current)
			: await readFile(current).then(async content => ({ content, mode: Number((await lstat(current)).mode) & 0o7777 }), () => undefined);
		if (!sameSandboxState(state, change.kind === "directory" ? change.before : change.before && { content: change.before, mode: change.beforeMode! })) {
			throw new Error(`resource changed before replay: ${change.target}`);
		}
	}
	for (const change of changes) {
		const copy = path.join(upper, change.target);
		if (change.kind === "directory" ? change.after : change.after !== undefined) {
			await mkdir(path.dirname(copy), { recursive: true });
			if (change.kind === "directory") await mkdir(copy, { recursive: true });
			else { await writeFile(copy, change.after!); await chmod(copy, change.afterMode ?? 0o644); await restoreModifiedTimes([[copy, change.afterModified]]); }
			continue;
		}
		await rm(copy, { recursive: true, force: true });
		if (await lstat(change.target).then(() => true, () => false)) {
			await mkdir(path.dirname(log), { recursive: true });
			await writeFile(log, `${change.target.slice(1).replace(/[\\\n\r]/g, char => char === "\n" ? "\\n" : char === "\r" ? "\\r" : "\\\\")}\n`, { flag: "a" });
		}
	}
}

/** Whole commands and held children publish the same ordered, content-addressed result format. */
async function captureProcessResult(
	store: ProvenanceCertificateStore,
	outcome: SpawnOutcome,
	observedProcessMs: number,
	effects: readonly { readonly logicalPath: string; readonly change:
		| Pick<SandboxFileChange, "kind" | "before" | "after" | "beforeMode" | "afterMode" | "afterModified" | "operation" | "object" | "aliases">
		| Pick<SandboxDirectoryChange, "kind" | "before" | "after"> }[],
): Promise<Extract<ProcessResultRecord, { readonly exit: ExitOutcome }>> {
	const journal: OrderedEffectEvent[] = [];
	for (const { logicalPath, change } of effects) {
		const state = async (side: "before" | "after"): Promise<WorkspaceEffectState> => {
			if (change.kind === "directory") return change[side] ? { kind: "directory", ...change[side] } : { kind: "absent" };
			const content = change[side], mode = change[side === "before" ? "beforeMode" : "afterMode"];
			if (content === undefined) return { kind: "absent" };
			if (mode === undefined) throw new Error("transaction file mode is unavailable");
			return { kind: "file", data: await store.artifacts.put(content), mode, ...(side === "after" && change.afterModified ? { modified: change.afterModified } : {}) };
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

/** Exec-only views of PATH directories outside the workspace, shared by every session: one per directory state. A session supplies
 * its broker through `session/`, the fixed path every view's sidecar names, mounted over from its private root. */
interface SharedInterposition { readonly root: Promise<string>; readonly views: BoundedRecencyMap<string, Promise<InterposedView | undefined>>; }
interface InterposedView { readonly view: string; readonly shadow: string; readonly names: readonly (readonly [name: string, file: string])[]; readonly dependency: DynamicDependency }

async function createProcessInterposition(input: {
	readonly gitDirectory?: string;
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
	readonly executableEntries: BoundedRecencyMap<string, readonly (readonly [name: string, file: string])[]>;
	readonly shared: SharedInterposition;
}) {
	const shared = await input.shared.root, root = path.join(input.privateRoot, "process-interposition");
	await mkdir(root, { recursive: true });
	const configurationPath = path.join(root, "configuration");
	input.signal?.throwIfAborted();
	if (/[\r\n]/.test(input.socketPath + input.token + shared)) throw new Error("dispatcher configuration cannot hold a line break");
	await writeFile(configurationPath, `${input.socketPath}\n${input.token}\n`, { mode: 0o600 });
	const excluded = [...new Set(await Promise.all(input.excludedExecutables.map(candidate => realpath(candidate).catch(() => undefined))))]
		.filter((candidate): candidate is string => candidate !== undefined).sort(); // A missing exclusion cannot be executed.
	const directories: InterposedDirectory[] = [], intercepts: InterceptedExecutable[] = [];
	const dependencies: DynamicDependency[] = [], seenTargets = new Set<string>();
	let mountBytes = 0, local = 0;
	// Each view is a directory of hard links to one dispatcher copy, a sidecar naming its routing, and the shadow its natives run from.
	const build = async (view: string, shadow: string, source: string, target: string, entries: readonly string[], identity: string, launcher: string) => {
		await Promise.all([mkdir(shadow, { recursive: true }), mkdir(view, { recursive: true })]);
		await writeFile(path.join(view, ".pi-spec-dispatch"), ["PI_SPEC_DISPATCH", path.join(shared, "session", "configuration"), target, shadow, ""].join("\n"), { mode: 0o600 });
		// Each physical entry is probed once per directory state.
		let names = input.executableEntries.get(identity);
		if (!names) {
			const probed: (readonly [string, string])[] = [];
			await mapFilesystem(entries, async (name) => {
				input.signal?.throwIfAborted();
				if (!name || name === ".pi-spec-dispatch" || name.includes("/") || name.includes("\0")) return;
				const sourceEntry = path.join(source, name);
				try {
					const resolved = await realpath(sourceEntry);
					if ((await lstat(resolved)).isFile() && !excluded.includes(resolved)) { await access(sourceEntry, fsConstants.X_OK); probed.push([name, resolved]); }
				} catch {
					// Unproved entries remain visible through the original directory.
				}
			});
			input.executableEntries.set(identity, names = probed);
		}
		const linked: (readonly [string, string])[] = [];
		await mapFilesystem(names, async (entry) => { await link(launcher, path.join(view, entry[0])).then(() => linked.push(entry), () => undefined); });
		const dependency = await captureDirectoryDependency(source, input.projection.isWorkspacePhysical(source) ? input.projection.toLogical(source) : slash(source), true,
			path.resolve(source) === path.resolve(input.workspaceRoot) ? input.workspaceExcludes : []);
		return { view, shadow, names: linked, dependency };
	};
	for (const rawDirectory of input.pathValue.split(path.delimiter)) {
		input.signal?.throwIfAborted();
		if (!rawDirectory || !path.isAbsolute(rawDirectory)) continue;
		const target = path.resolve(rawDirectory);
		if (seenTargets.has(target)) continue;
		seenTargets.add(target);
		const projected = input.projection.toPhysical(target) ?? target;
		let source: string, entries: string[], identity: string;
		try {
			source = await realpath(projected);
			// Adding, removing or renaming an entry changes the directory's times; a stale list only leaves a new entry native.
			const info = await stat(source, { bigint: true });
			if (!info.isDirectory()) continue;
			// Aliases of one directory share its probe; each keeps its own view.
			identity = [source, info.dev, info.ino, info.mtimeNs, info.ctimeNs, ...excluded].join("\0");
			entries = await readdir(source);
		} catch {
			continue;
		}
		if ([target, source].some((value) => /[\r\n]/.test(value))) continue;
		// Each mapping is a sandbox argument: a directory past the budget (a WSL PATH carries thousands of Windows
		// executables) stays native as a whole rather than overflow ARG_MAX.
		const bytes = entries.reduce((total, name) => total + 2 * name.length + target.length + shared.length + 96, 0);
		if ((mountBytes += bytes) > MAX_INTERPOSED_MOUNT_BYTES) { mountBytes -= bytes; continue; }
		let interposed: InterposedView | undefined;
		if (input.projection.isWorkspacePhysical(source)) {
			// A directory inside this workspace sandbox is this session's own.
			const index = String(local++).padStart(3, "0"), launcher = path.join(root, "dispatcher");
			if (local === 1) { await copyFile(input.dispatcherBinary, launcher); await chmod(launcher, 0o755); }
			interposed = await build(path.join(root, "views", index), path.join(root, "originals", index), source, target, entries, identity, launcher);
		} else {
			const key = sha256Digest(`${target}\0${identity}`).slice("sha256:".length, "sha256:".length + 32);
			let pending = input.shared.views.get(key);
			if (!pending) {
				pending = build(path.join(shared, "views", key), path.join(shared, "originals", key), source, target, entries, identity, path.join(shared, "dispatcher"))
					.catch(() => undefined);
				input.shared.views.set(key, pending);
			}
			interposed = await pending;
		}
		if (!interposed) continue;
		directories.push({ source, target, shadow: interposed.shadow, view: interposed.view });
		dependencies.push(interposed.dependency);
		for (const [name, file] of interposed.names) intercepts.push({ intercepted: path.join(target, name), view: path.join(interposed.view, name), native: path.join(interposed.shadow, name), file });
	}
	const mounts = uniqueSandboxMounts([
		...directories.map(({ shadow, source }) => ({ virtualPath: shadow, hostPath: source, readOnly: true })),
		{ virtualPath: path.join(shared, "session"), hostPath: root, readOnly: true },
		{ virtualPath: input.sourceRoot, hostPath: input.workspaceRoot, readOnly: false },
		...(input.gitDirectory ? [{ virtualPath: input.gitDirectory, hostPath: input.gitDirectory, readOnly: true }] : []),
	]);
	return { mounts, directories: Object.freeze(directories), entries: Object.freeze(intercepts), dependencies: Object.freeze(dependencies) };
}

/** What a process tree running `image` intercepts: every entry but the ones reaching `image`, which would broker it to itself.
 * The sandbox mounts and the observer's table are both this one view. */
function interception(interposition: ActiveSession["interposition"], image?: string) {
	const entries = interposition.entries.filter(entry => entry.file !== image);
	return {
		// A native image run from a shadow is its intercepted original: scripts are opened and named there.
		execMounts: [...entries.map(entry => ({ virtualPath: entry.intercepted, hostPath: entry.view })),
			...interposition.directories.map(({ shadow, target }) => ({ virtualPath: shadow, hostPath: target, alias: true as const }))],
		executables: entries.flatMap(entry => [[entry.intercepted, entry.native], [entry.view, entry.native]] as const) };
}

function sandboxArguments(input: {
	readonly ready: ReadyBackend;
	readonly cwd: string;
	readonly deniedPaths: readonly string[];
	readonly writablePaths: readonly string[];
	readonly mounts: readonly SandboxMount[];
	readonly execMounts: readonly ExecMount[];
	readonly privateWrites: string;
	readonly command: readonly string[];
	readonly timeoutSeconds?: number;
}): readonly string[] {
	return [
		input.ready.sandlock,
		...sandboxPolicyArguments(input.cwd, input.deniedPaths, input.writablePaths, input.mounts, input.execMounts, input.privateWrites),
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
	privateWrites: string,
): readonly string[] {
	return [
		"run",
		"--chroot",
		"/",
		// A write the host user could make but the policy does not grant lands in a private branch here, as it would succeed natively.
		"--workdir", "/", "--fs-storage", privateWrites, "--fs-branch", "writes", "--on-exit", "keep", "--on-error", "keep",
		...mounts.flatMap((mount) => ["--fs-mount", sandboxMountArgument(mount)]),
		...execMounts.flatMap((mount) => [mount.alias ? "--exec-alias" : "--exec-mount", sandboxMountArgument(mount)]),
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

/** What the brokered run a traced launcher `pid` started wrote, with its own descendants; a run never answers for itself. */
function brokeredWrites(session: ActiveSession, pid: number, self?: SessionWriter): readonly string[] | undefined {
	return [...session.writers].find(writer => writer !== self && writer.pid === pid)?.written;
}

const privateUpper = (session: ActiveSession) => path.join(session.workspace.processRoot, "private", "writes", "upper");

/** A session's private branch under `storage`: its upper directory, its whiteout log and the host paths that log hides. */
async function privateBranch(storage: string) {
	const upper = path.join(storage, "writes", "upper"), log = path.join(storage, "writes", "deleted.log");
	const deletions = (await readFile(log, "utf8").catch(() => "")).split("\n").filter(Boolean)
		.map(line => `/${line.replace(/\\(.)/g, (_, escaped: string) => escaped === "n" ? "\n" : escaped === "r" ? "\r" : escaped)}`);
	return { upper, log, deletions, deleted: (target: string) => deletions.some(entry => target === entry || target.startsWith(`${entry}/`)) };
}

/** The nearest directory above `target` the host holds: where a change outside the workspace stages and commits. */
async function hostRoot(target: string): Promise<string> {
	for (let directory = path.dirname(target); ; directory = path.dirname(directory)) {
		if ((await lstat(directory).catch(() => undefined))?.isDirectory() || directory === path.dirname(directory)) return directory;
	}
}

/** A session's private branch under `storage` as changes against the host, at `candidates` or wherever it differs: a copy that
 * differs from the host object, a directory the host lacks, a deletion of something the host holds. Each stands over the host
 * state it replaces, which only holds as a baseline for an object no one changed since `since`; anything else is unrepresentable. */
async function privateCommits(storage: string, since: number, candidates?: Iterable<string>) {
	const { upper, deletions, deleted } = await privateBranch(storage), stat = (target: string) => lstat(target, { bigint: true }).catch(() => undefined);
	const held: string[] = [], changes: SandboxWorkspaceChange[] = [], unrepresentable: string[] = [], walk = async (host: string): Promise<void> => {
		for (const name of await readdir(path.join(upper, host)).catch(() => [] as string[])) { held.push(path.posix.join(host, name)); await walk(path.posix.join(host, name)); }
	};
	if (!candidates) await walk("/");
	for (const target of new Set(candidates ? [...candidates].map(name => path.posix.resolve(name)) : [...held, ...deletions])) {
		const copy = path.join(upper, target), [own, native] = await Promise.all([stat(copy), stat(target)]);
		if (!own && !(native && deleted(target)) || own?.isDirectory() && native?.isDirectory()) continue;
		const file = async (at: string, info?: Awaited<ReturnType<typeof stat>>) => info?.isFile() ? { content: await readFile(at), mode: Number(info.mode) & 0o7777, modified: String(info.mtimeNs) } : undefined;
		const [after, before] = await Promise.all([file(copy, own), file(target, native)]);
		if (after && before && Buffer.from(after.content).equals(before.content) && after.mode === before.mode && after.modified === before.modified) continue;
		if (native && Number(native.ctimeMs) > since || own && !own.isFile() && !own.isDirectory() || native && !native.isFile() && !native.isDirectory()) {
			unrepresentable.push(`external_write:${target}`); continue;
		}
		const root = await hostRoot(target), resource = slash(path.relative(root, target));
		changes.push(own?.isDirectory() || native?.isDirectory()
			? { kind: "directory", root, target, resource, ...(native ? { before: (await readSandboxDirectoryState(target))! } : {}), ...(own ? { operation: "mkdir" as const, after: (await readSandboxDirectoryState(copy))! } : {}) }
			: { root, target, resource, ...(before ? { before: before.content, beforeMode: before.mode } : {}), ...(after ? { after: after.content, afterMode: after.mode, afterModified: after.modified } : {}) });
	}
	return { changes, unrepresentable };
}

function uniqueSandboxMounts(mounts: readonly SandboxMount[]): readonly SandboxMount[] {
	const seen = new Set<string>();
	return Object.freeze(
		[...mounts].sort((left, right) => right.virtualPath.length - left.virtualPath.length)
			.filter(({ virtualPath }) => {
				const normalized = path.resolve(virtualPath);
				if (seen.has(normalized)) return false;
				seen.add(normalized);
				return true;
			}),
	);
}

/** A filesystem mount names its access; an exec mount or alias only its image. */
function sandboxMountArgument(mount: SandboxMount | ExecMount): string {
	if (!path.isAbsolute(mount.virtualPath) || !path.isAbsolute(mount.hostPath) || [mount.virtualPath, mount.hostPath].some((value) => value.includes(":"))) {
		throw new Error(`Sandlock mount cannot represent ${mount.virtualPath}:${mount.hostPath}`);
	}
	return `${mount.virtualPath}:${mount.hostPath}${"readOnly" in mount ? (mount.readOnly ? ":ro" : ":rw") : ""}`;
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
		...sandboxPolicyArguments(input.logicalRoot, [], [input.physicalRoot], [{ virtualPath: input.logicalRoot, hostPath: input.physicalRoot, readOnly: false }], [], path.join(input.physicalRoot, "private")),
		"--",
		input.dispatcher,
		"--exec",
		"12",
		"pi-context-probe",
		input.dispatcher,
		"--probe-context",
		input.logicalRoot,
		path.join(input.logicalRoot, "script-position"),
	]);
	const outcome = await runSpawn(input.strace, command.slice(1), { cwd: input.physicalRoot, environment: definedProcessEnvironment(process.env) });
	if (outcome.signal || outcome.code !== 0) throw new Error(`process execution context probe failed${await inheritedTracer()}`);
	const stdout = Buffer.concat(outcome.output.filter(({ fd }) => fd === 1).map(({ data }) => data)).toString();
	const parsed = processContextFromRaw(JSON.parse(stdout) as RawProcessContext);
	if (!validProcessContext(parsed)) throw new Error("process execution context probe returned invalid data");
	return parsed;
}

function speculativeProducerProof(ready: ReadyBackend, deniedPaths: readonly string[], policy = POLICY_ID): ProcessProducerProof {
	return Object.freeze({
		observer: { provider: "strace", fingerprint: ready.observerFingerprint },
		execution: { authority: "speculative", confinement: { provider: "sandlock", fingerprint: digestObject({ policy, deniedPaths }) } },
	} satisfies ProcessProducerProof);
}

function compatibleProducer(expected: ProcessProducerProof, candidate: ProcessProducerProof): boolean {
	return expected.execution.authority === "speculative" && stableEqual(expected, candidate);
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
		const terminate = () => { if (!child.pid) return; try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); } };
		const onAbort = () => terminate();
		options.signal?.addEventListener("abort", onAbort, { once: true });
		let timeout: ReturnType<typeof setTimeout> | undefined;
		let timedOut = false;
		if (options.timeoutSeconds !== undefined) timeout = setTimeout(() => { timedOut = true; terminate(); }, Math.max(1, options.timeoutSeconds * 1000));
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
		} catch (error) { terminate(); await completed.catch(() => undefined); throw error; } finally {
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
			const peers = (await readFile("/proc/net/unix", "utf8")).split("\n").map((line) => line.trim().split(/\s+/))
				.filter((fields) => fields[7] === socketPath && fields[4] === "0001" && fields[5] === "03");
			if (peers.length !== 1 || !/^\d+$/.test(peers[0]![6]!)) throw new Error("output_endpoint_identity_unproven");
			entry.endpoint = `socket:[${peers[0]![6]}]`;
		}
		return { entries, releaseWriters, dispose };
	} catch (error) { await dispose(); throw error; }
}

/** The native launcher's framing: output as "o<fd> <length>\n<bytes>", then "x <code>", "s <signal number>" or "b" (bypass in place). */
function wireResponse(response: DispatcherResponse): Buffer {
	const frames = (response.output ?? []).flatMap(({ fd, data }) => [Buffer.from(`o${fd} ${Buffer.byteLength(data, "base64")}\n`), Buffer.from(data, "base64")]);
	const exit = response.exit, end = response.kind === "bypass" ? "b" : exit?.kind === "signal" ? `s ${exit.signal}` : `x ${exit?.kind === "code" ? exit.code : 125}`;
	return Buffer.concat([...frames, ...(response.streams ?? []).flatMap(({ fd, kind, data }) => [Buffer.from(`${kind}${fd} ${data.length}\n`), data]), Buffer.from(`${end}\n`)]);
}

/** The launcher's settlement of each inherited pipe: read back the shortest queue prefix, then write the suffix, that leave the queue as
 * the run left it. A queue of one repeated byte (a jobserver's tokens) usually needs neither. Undefined when the run did more than a queue can. */
function streamSettlement(inputs: ReturnType<typeof descriptorInputs>, events: readonly { readonly alias: number; readonly kind: string; readonly data: Buffer }[]): StreamSettlement[] | undefined {
	const queues = new Map<number, { readonly initial: Buffer; queue: Buffer }>();
	for (const input of inputs) if (input.type === "pipe" && !queues.has(input.image)) queues.set(input.image, { initial: Buffer.from(input.content ?? "", "base64"), queue: Buffer.from(input.content ?? "", "base64") });
	for (const { alias, kind, data } of events) {
		const image = inputs.find(input => input.alias === alias)?.image, state = image === undefined ? undefined : queues.get(image);
		// Looking, waiting or failing leaves a queue as it was.
		if (["peek", "release", "ready", "failure"].includes(kind)) continue; else if (!state || kind !== "produce" && kind !== "consume") return undefined;
		if (kind === "consume" && !state.queue.subarray(0, data.length).equals(data)) return undefined;
		state.queue = kind === "produce" ? Buffer.concat([state.queue, data]) : state.queue.subarray(data.length);
	}
	const settlement: StreamSettlement[] = [];
	for (const [image, { initial, queue }] of queues) {
		let read = 0;
		while (!queue.subarray(0, initial.length - read).equals(initial.subarray(read))) read++;
		const written = queue.subarray(initial.length - read), end = (access: number) => inputs.find(input => input.image === image && input.installed !== false && (input.flags & 3) === access);
		if (read && !end(0) || written.length && !end(1)) return undefined;
		if (read) settlement.push({ fd: end(0)!.fd, kind: "i", data: initial.subarray(0, read) });
		if (written.length) settlement.push({ fd: end(1)!.fd, kind: "o", data: written });
	}
	return settlement;
}

function parseDispatcherRequest(body: string): DispatcherRequest | undefined {
	try {
		const value: unknown = JSON.parse(body.trim());
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const reported = value as Partial<Omit<DispatcherRequest, "context"> & { context: RawProcessContext }>;
		// The launcher reports its standard streams as the kernel shows them; the broker derives the context.
		const request = { ...reported, context: processContextFromRaw(reported.context!) } as Partial<DispatcherRequest>;
		const text = (field: unknown) => typeof field === "string" && !field.includes("\0");
		return typeof request.token === "string" && typeof request.name === "string" && text(request.invokedPath) && text(request.argv0) &&
			request.argv0!.length <= 1024 * 1024 && Array.isArray(request.args) && request.args.every(text) && typeof request.cwd === "string" &&
			!!request.environment && typeof request.environment === "object" && (request.pid === undefined || Number.isSafeInteger(request.pid) && request.pid > 0) &&
			(request.descriptors === undefined || Array.isArray(request.descriptors) && request.descriptors.length <= 64 && request.descriptors.every(entry => entry &&
				[entry.fd, entry.alias, entry.flags, entry.capacity].every(Number.isSafeInteger) && entry.fd > 2 && typeof entry.eof === "boolean" &&
				[entry.device, entry.inode].every(value => typeof value === "string" && /^\d+$/.test(value)) && typeof entry.queueHex === "string" && /^(?:[0-9a-f]{2})*$/.test(entry.queueHex))) &&
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
	if (!session.topLevelOutputEndpoints) return { reason: "output_endpoint_capture_missing" };
	if (request.context.outputEndpoints.some((endpoint) => !endpoint)) return { reason: "request_output_endpoint_missing" };
	// The launch key below still checks a discarded descriptor's type and flags. Both outlets belong to one captured process.
	const endpoints = [session.topLevelOutputEndpoints, ...session.nestedOutputEndpoints].find(pair =>
		request.context.outputEndpoints.every(endpoint => pair.includes(endpoint) || endpoint === "/dev/null")) ?? session.topLevelOutputEndpoints;
	// A pipe another traced process reads (a build tool's compiler output) keeps its descriptor: the launcher writes there.
	const pipes = request.context.outputEndpoints.map(endpoint => /^pipe:\[\d+\]$/.test(endpoint) && !endpoints.includes(endpoint)) as [boolean, boolean];
	const routeOf = (endpoint: string, fd: 1 | 2) => pipes[fd - 1] ? fd === 2 && endpoint === request.context.outputEndpoints[0] ? 1 : fd
		: endpoint === endpoints[0] ? 1 : endpoint === endpoints[1] ? 2 : endpoint === "/dev/null" ? 0 : undefined;
	const route = [routeOf(request.context.outputEndpoints[0], 1), routeOf(request.context.outputEndpoints[1], 2)] as const;
	if (route[0] === undefined || route[1] === undefined) return { reason: `output_endpoint_mismatch:${JSON.stringify({ expected: endpoints, observed: request.context.outputEndpoints })}` };
	const outputRoute: OutputRoute = [route[0], route[1]], outputFlags = outputStatusFlags(request.context);
	const context = routedProcessContext(expectedContext, outputRoute, false, undefined, pipes.some(Boolean) ? pipes : undefined, outputFlags);
	if (request.context.launchKey !== context.launchKey) return { reason: "launch_key_mismatch" };
	if (request.context.umask !== context.umask) return { reason: "umask_mismatch" };
	return { route: outputRoute, ...(pipes.some(Boolean) ? { outputPipes: pipes } : {}), ...(outputFlags ? { outputFlags } : {}) };
}

function shellArguments(invocation: ToolProcessInvocation, command: string): string[] {
	return invocation.commandTransport === "argv" ? [...invocation.shellArgs, command] : [...invocation.shellArgs];
}

async function topLevelProcessPrototype(invocation: ToolProcessInvocation, request: ProcessExecutionRequest, environment: Readonly<Record<string, string>>,
	projection: ExecutionPathProjection, platformFingerprint: Sha256Digest): Promise<ExecPrototype> {
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
			credentials: { uid: process.getuid?.(), euid: process.geteuid?.(), gid: process.getgid?.(), egid: process.getegid?.(), groups: process.getgroups?.() },
			scheduler: { cpuCount: os.availableParallelism(), timeout: request.timeout ?? null },
			signals: "node-default",
		}),
		stdin: invocation.commandTransport === "stdin" ? { type: "bytes", digest: sha256Digest(request.command), eof: true } : { type: "closed", eof: true },
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
	if (producer.observer.provider !== "strace" || producer.observer.fingerprint !== digestObject({ epoch: BACKEND_EPOCH })) return false;
	const confinement = producer.execution.authority === "actor" ? undefined : producer.execution.confinement;
	return !confinement || confinement.provider === "sandlock" && [POLICY_ID, LEAF_POLICY_ID].some((policy) => confinement.fingerprint === digestObject({ policy, deniedPaths }));
}

function execText(executable: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable}: ${stderr || error.message}`));
			else resolve(`${stdout}${stderr}`);
		});
	});
}

function closeServer(server: net.Server): Promise<void> { return new Promise((resolve) => server.close(() => resolve())); }

function exitOutcome(outcome: SpawnOutcome): ExitOutcome {
	if (!outcome.signal) return { kind: "code", code: outcome.code ?? 125 };
	return { kind: "signal", signal: os.constants.signals[outcome.signal] ?? 9, coreDumped: false };
}

function randomToken(): string { return randomBytes(32).toString("hex"); }

function assertInvocationMatches(invocation: ToolProcessInvocation, request: ProcessExecutionRequest): void {
	if (request.command !== invocation.command) throw new Error("process command differs from the action execution context");
	if (path.resolve(request.cwd) !== path.resolve(invocation.cwd)) throw new Error("process cwd differs from the action execution context");
	if (request.timeout !== invocation.timeout) throw new Error("process timeout differs from the action execution context");
	if (!stableEqual(definedProcessEnvironment(request.environment), invocation.environment)) throw new Error("process environment differs from the action execution context");
}

export async function validateTransferredProcessEvidence(
	evidence: DynamicDependencyCertificate | undefined,
	incompleteReasons: Iterable<string> = [],
): Promise<ResourceValidation> {
	if (!evidence) {
		return { status: "indeterminate", cause: { stage: "freshness", code: "process_evidence_missing" }, metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } };
	}
	const blockingTaints = evidence.taints.filter((taint) => !TRANSFERRED_INPUT_TAINTS.has(taint));
	const validation = await validateDynamicDependencyCertificate({ ...evidence, taints: blockingTaints }, { maxFileBytes: MAX_CAPTURE_BYTES });
	const metrics = { durationMs: validation.durationMs, bytesRead: validation.bytesRead, filesRead: validation.filesRead, mode: "exact" as const };
	if (validation.status === "valid") return { status: "valid", metrics };
	if (validation.status === "stale") return { status: "stale", cause: { stage: "freshness", code: "process_dependency_changed", detail: validation.changed.join(",") }, metrics };
	return {
		status: "indeterminate",
		cause: { stage: "freshness", code: "process_provenance_indeterminate", detail: [validation.reason, ...incompleteReasons].join(",") },
		metrics,
	};
}

function mergeDependencyEvidence(certificates: readonly DynamicDependencyCertificate[], incompleteReasons: Set<string>): DynamicDependencyCertificate {
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
				existing?.kind === "file" && dependency.kind === "file" && existing.contentDigest === dependency.contentDigest &&
				existing.metadataDigest === dependency.metadataDigest && stableEqual(existing.aliases, dependency.aliases)
			) {
				if (existing.role !== "executable" && dependency.role === "executable") dependencies.set(identity, dependency);
				continue;
			}
			// One run listed a directory, or the one holding a missing name, that another only reached: both saw the same state.
			if (existing?.kind === dependency.kind && (dependency.kind === "directory" || dependency.kind === "absence") && Object.entries(dependency).every(([key, value]) =>
				!(key in existing) || stableEqual(Reflect.get(existing, key), value))) { dependencies.set(identity, { ...existing, ...dependency }); continue; }
			if (existing && !stableEqual(existing, dependency)) {
				complete = false;
				incompleteReasons.add(`dependency_changed_during_execution:${identity}`);
				continue;
			}
			dependencies.set(identity, dependency);
		}
	}
	return { complete: complete && incompleteReasons.size === 0, dependencies: Object.freeze([...dependencies.values()]), taints: Object.freeze([...taints]) };
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
