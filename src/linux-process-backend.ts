import { ContentChunkPool, DEFAULT_CONTENT_LIMITS, contentDigest, publishContent, restoreArtifactContent, writeContent } from "./content-chunks.ts";
import { isSafeCount } from "./number-utils.ts";
import { captureProcessContext, processLaunchOptions, routedProcessIdentity, validProcessContext, type ProcessExecutionContext, type ProcessLaunchOptions } from "./process-context.mjs";
import { spawn } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";
import { configureNativeFilesystem, nativeProcessLibrary } from "./process-startup.mjs";
import { on, once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { constants as fsConstants, readFileSync, type BigIntStats } from "node:fs";
import { chmod, copyFile, lstat, mkdir, mkdtemp, open, readFile, readdir, readlink, rm, stat, writeFile, type FileHandle } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { finished } from "node:stream/promises";
import { errorMessage, isMissing as missing } from "./error-utils.ts";
import { isRecord, stableEqual } from "./stable-json.ts";
import { TimelineInterval, type TimelineDependency } from "./task-timing.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import { ResourceObjectInputs } from "./agent-execution-world.ts";
import {
	certificateReplayable, createExecPrototype, dynamicDependencyIdentity, type DependencyRole, type DynamicDependency, type DynamicDependencyCertificate,
	type ExecPrototype, type ExitOutcome, ONE_SHOT_TAINTS, type OrderedEffectEvent, type OFDPosition, type ProcessProducerProof,
	type ProcessProvenanceCertificate, type ProcessResultRecord, processWeakKey, type ProvenanceTaint, sealProcessCertificate,
	type WorkspaceEffectState,
} from "./provenance-certificate.ts";
import { digestObject, sha256Digest, type Sha256Digest } from "./stable-value-hash.ts";
import {
	filesystemMetadataDigest, filesystemObservationDigest, FILESYSTEM_METADATA_FIELDS, assertNoSymlinkPath, cachedCapture, captureFileDigest, captureFilesystemEntry, captureStableFile,
	hashExecutableFile, mapFilesystem, readFilesystemDirectoryImage, withFilesystemReadView, FilesystemReadView, rememberCapture, sameFilesystemIdentity, sharedWalk, walkFilesystemPath, type StableFilesystemCapture,
} from "./filesystem-evidence.ts";
import {
	captureAbsenceDependency, captureDirectoryDependency, captureFileDependency, observeProcessInputs, validateDynamicDependencyCertificate,
} from "./provenance-validation.ts";
import {
	captureWorkspaceStructure, captureWorkspaceFileAliases, diffWorkspaceStructures, ExecutionPathProjection, hydrateWorkspaceFileEntry, snapshotDependency,
	type WorkspaceStructureSnapshot, type WorkspaceTransactionDiff,
} from "./process-observation.ts";
import { definedProcessEnvironment, type ProcessRouteSnapshot, type ProcessExecutionRequest,
	type ProcessExecutor } from "./process-execution.ts";
import { EffectCommitFailure, effectCommitFailure, isPoisonedEffectCommit } from "./effect-transaction.ts";
import { execHostText as execText, resolveHostExecutable } from "./executable-path.ts";
import {
	captureHeldDescriptorInputs, inspectHeldExecProcess, LinuxHeldExecBoundary, listenUnixSocket, writeProcessChannel, resolveLinuxExecHelper,
	type HeldExecDecision, type HeldExecProcess, type HeldExecSnapshot, type HeldExecTiming, type HeldExecClock, type HeldFileDescriptor,
	descriptorInputs, descriptorEffects, inheritedTracer, type ProcessResourceGraph,
} from "./linux-held-exec.ts";
import {
	emptyWorldReuseMetrics, snapshotExecutionScope, type ExecutionScope, type ExecutionOperationAdoption, type ExecutionResourceContext,
	type ExecutionOperationBinding, type ExecutionWorldStorageControl, type WorldReuseMetrics,
} from "./execution-world.ts";
import { type ProcessReusePlan, ProcessReusePlanner } from "./reuse-planner.ts";
import { ProvenanceCertificateStore, type ProvenanceStoreOptions, type VerifiedArtifactClosure } from "./reuse-store.ts";
import {
	observeStrace, straceCommand, traceTail, type StraceObservation, type TraceTail, type TracedWrite, writesWithin,
} from "./strace-observer.ts";
import type { WorkspaceTransactionCapture, WorkspaceTransactionDelta, WorkspaceTransactionOwnership } from "./workspace-transaction.ts";
import type { ToolProcessInvocation } from "./tool-settlement.ts";
import type { ResourceValidation } from "./settlement.ts";
import {
	ProcessHandoffOwnership, ProcessHandoffRegistry, sameScope, type ProcessContinuation, type ProcessExecutionBinding, type ProcessHandoff,
	type ProcessHandoffBorrow, type ProcessHandoffLender,
} from "./process-handoff.ts";
import {
	SynchronousProcessObserver, serializeNativeDependency, restoreNativeDependency, type SerializedProcessBinding, type SynchronousProcessRequest,
	type SynchronousProcessResult,
} from "./linux-process-sync.ts";
import {
	WorkspaceSandboxService, nativeWorkspaceOwner, nativeWorkspaceObservation, readSandboxDirectoryState, restoreModifiedTimes, sameSandboxState, sandboxWorkspaceChanges,
	type SandboxDirectoryState, type SandboxWorkspaceChange, type SandboxWorkspaceContext,
} from "./workspace-sandbox.ts";
import { containsFilesystemPath as pathContains, relativeFilesystemPath, slash } from "./path-utils.ts";
import { projectFileMutation } from "./workspace-state.ts";

// Certificates are meaningful only under the implementation that collected and authorized their proof.
const OBSERVER_FINGERPRINT = digestObject(["linux-process-backend", "strace-observer", "reuse-planner", "reuse-store",
	"provenance-certificate", "provenance-validation", "filesystem-evidence", "stable-value-hash", "workspace-state", "workspace-sandbox", "process-observation"]
	.map(name => sha256Digest(readFileSync(new URL(`./${name}.${import.meta.url.endsWith(".ts") ? "ts" : "js"}`, import.meta.url)))));
const POLICY_ID = "sandlock-virtual-root-transparent-exec-creation-mode";
const LEAF_POLICY_ID = "sandlock-virtual-workspace-leaf-creation-mode";
const MAX_REQUEST_BYTES = 4 * 1024 * 1024, LEARNED_LAUNCHES = 64;
const MAX_CONTINUATION_BYTES = 65 * 1024 * 1024;
const MAX_CAPTURE_BYTES = 512 * 1024 * 1024;
/** The consumer launcher places a dependency inside its existing traced parent; metadata owns neither one. */
const processDependencyPIDs = new WeakMap<TimelineDependency, number>();
/** Native inputs consumed by this exact one-shot execution; they still prohibit any later replay. */
const TRANSFERRED_INPUT_TAINTS = new Set<ProvenanceTaint>(ONE_SHOT_TAINTS);

export interface LinuxProcessBackendOptions {
	readonly storeRoot: string;
	readonly store?: ProvenanceStoreOptions;
	readonly sandlockBinary?: string;
	readonly straceBinary?: string;
	readonly heldExecBinary?: string;
	readonly heldExecObserverBinary?: string;
	/** Seal finished commands without capturing resumable process images. */
	readonly completedOnly?: boolean;
	/** Additional host paths that speculative processes must never read. */
	readonly deniedPaths?: readonly string[];
}

export interface CompletedProcessReplayOptions {
	readonly sourceRoot: string;
	readonly invocation: (request: ProcessExecutionRequest) => ToolProcessInvocation | undefined;
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
	/** Seal the session's owned workspace interval before validation. */
	readonly seal: (changes: readonly SandboxWorkspaceChange[]) => Promise<readonly SandboxWorkspaceChange[]>;
	/** Revalidate every observed input immediately before Actor adoption. */
	readonly validate: () => Promise<ResourceValidation>;
	readonly close: () => Promise<void>;
	/** Release a remote session after its evidence and ownership are no longer needed. */
	readonly dispose?: () => Promise<void>;
}

type ReadyBackend = Awaited<ReturnType<LinuxProcessReuseBackend["probe"]>>;

interface ExecMount { readonly virtualPath: string; readonly hostPath: string; }

interface SandboxMount extends ExecMount { readonly readOnly: boolean; }

interface DispatcherRequest extends Required<Pick<HeldExecSnapshot, "invocation" | "cwd" | "environment">> {
	readonly token: string;
	readonly argv0: string;
	readonly args: readonly string[];
	readonly context: ProcessExecutionContext;
	/** The successfully installed image, held before its first user instruction. */
	readonly pid: number;
	/** Inherited pipes beyond stdio as the launcher sees them now: each end, and its queue unconsumed. */
	readonly descriptors?: readonly Required<Pick<HeldFileDescriptor, "fd" | "alias" | "device" | "inode" | "flags" | "capacity" | "eof" | "queueHex">>[];
}

/** A brokered run's net effect on an inherited pipe of one repeated byte, applied by the launcher to its own end. */
type StreamSettlement = { readonly fd: number; readonly kind: "i" | "o"; readonly data: Buffer };
type ProcessArguments = Pick<DispatcherRequest, "argv0" | "args" | "cwd" | "environment"> & ProcessLaunchOptions & {
	readonly invocation?: readonly [path: string, comm: string];
	readonly imageSource?: string;
	readonly resources?: ProcessResourceGraph; readonly streams?: true; // inherited pipes the launcher holds and settles
};
type BoundProcessInvocation = ProcessArguments & {
	readonly sourceRoot: string; readonly executable: string;
	readonly producer?: ProcessProducerProof;
};
export type NativeProcessInvocation = BoundProcessInvocation | { readonly trackingOnly: true; readonly sourceRoot: string };

interface BufferedOutput { readonly fd: 1 | 2; readonly data: Buffer; }

interface DispatcherResponse {
	readonly kind: "hit" | "executed" | "bypass" | "suspended";
	readonly output?: readonly BufferedOutput[];
	readonly exit?: ExitOutcome;
	readonly streams?: readonly StreamSettlement[];
	/** Present only after a sealed result was registered; independent of optional disk publication. */
	readonly reusable?: boolean;
}

/** A brokered run writing into a session's workspace: its launcher's pid and, once known, what it and the runs brokered from its tree wrote. */
interface SessionWriter {
	readonly id: string; readonly owner: string; readonly startedAt: number; endedAt?: number; writes?: readonly TracedWrite[]; tail?: TraceTail; unsafe?: boolean;
	readonly pid?: number; written?: readonly string[]; external?: readonly string[]; descendants?: readonly SessionWriter[];
	observed?: ReadonlyMap<string, number>; evidence?: DynamicDependencyCertificate; observation?: StraceObservation;
}

const workspaceWriters = new WeakMap<SandboxWorkspaceContext["transactions"], Set<SessionWriter>>();

type ActiveSession = Awaited<ReturnType<LinuxProcessReuseBackend["allocateSession"]>>["session"];
type PrivatePrestate = Awaited<ReturnType<typeof privateBranch>> & { readonly startedAt: number; readonly entries: Map<string, BigIntStats>; readonly files: Map<string, StableFilesystemCapture>; structure?: WorkspaceStructureSnapshot };

interface SpawnOutcome { readonly code: number | null; readonly signal: NodeJS.Signals | null; readonly output: readonly BufferedOutput[]; }

type ReadyProcessPlan = Exclude<ProcessReusePlan, { kind: "miss" }>;
interface NativeObservation {
	readonly scope?: ExecutionScope; ordering: Int32Array; closed: boolean; readonly learn: boolean; pending: number;
	readonly reuseEnabled: boolean;
	readonly learned: Map<number, number>; readonly inputs?: ResourceObjectInputs;
	readonly bindings: Map<number, ProcessExecutionBinding>; readonly computations: TimelineDependency[]; readonly heldClock: ActorHeldClock;
	readonly metrics: MutableLinuxProcessReuseMetrics; readonly adoptions: SynchronousProcessResult["adoptions"][number][]; claims?: Int32Array;
	effects?: Pick<WorkspaceSandboxService, "commitDelta">;
	source?: { readonly available: Int32Array; readonly borrow: ProcessHandoffBorrow };
}

/** Linux-only process substrate. Unavailable dependencies remove the route instead of weakening it. */
export class LinuxProcessReuseBackend {
	private readonly observations = new AsyncLocalStorage<NativeObservation>();

	/** Keep actual completed launches and acknowledged adoptions in their enclosing native call's order. */
	async observeBindings<Value>(scope: ExecutionScope, execute: () => Promise<Value>,
		observe: (bindings: readonly ProcessExecutionBinding[], computations: readonly TimelineDependency[]) => void, learn = false,
		inputs?: ((path?: string) => Iterable<object>) | ResourceObjectInputs, reuseEnabled = true): Promise<Value> {
		const observation: NativeObservation = { scope: snapshotExecutionScope(scope), ordering: new Int32Array(new SharedArrayBuffer(4)), closed: false,
			learn, reuseEnabled, pending: 0, learned: new Map<number, number>(), inputs: !reuseEnabled ? undefined : typeof inputs === "function" ? new ResourceObjectInputs(inputs) : inputs,
			bindings: new Map<number, ProcessExecutionBinding>(), computations: [] as TimelineDependency[],
			heldClock: new ActorHeldClock(), metrics: emptyWorldReuseMetrics(), adoptions: [] };
		try { return await this.observations.run(observation, execute); }
		finally {
			observation.closed = true;
			try { observe([...observation.bindings].sort(([left], [right]) => left - right).map(([, binding]) => binding),
				Object.freeze([...observation.computations, ...observation.heldClock.dependencies()])); }
			catch { /* Learning cannot replace the native result or error. */ }
		}
	}
	readonly store: ProvenanceCertificateStore;
	readonly planner: ProcessReusePlanner;
	readonly storage: ExecutionWorldStorageControl;
	private readonly options: LinuxProcessBackendOptions;
	private ready?: Promise<ReadyBackend>;
	private platformFingerprint?: Promise<Sha256Digest>;
	private heldExec?: { readonly controller: AbortController; readonly ready: Promise<LinuxHeldExecBoundary>; status: ProcessRouteSnapshot };
	private nativeRetirement?: Promise<void>;
	private synchronous?: SynchronousProcessObserver;
	private disposed = false;
	private readonly handoffs: ProcessHandoffRegistry<NativeProcessInvocation>;
	private readonly counters: MutableLinuxProcessReuseMetrics = emptyWorldReuseMetrics();
	private readonly actorCounters: MutableLinuxProcessReuseMetrics = emptyWorldReuseMetrics();
	private readonly replayWorkspace: WorkspaceSandboxService;
	private disposal?: Promise<void>;
	private producers = 0;
	/** A conservative hint remains visible to observations started before local work existed. */
	private readonly localSource = new Int32Array(new SharedArrayBuffer(4));

	constructor(options: LinuxProcessBackendOptions) {
		this.options = { sandlockBinary: process.env.PI_SPEC_SANDLOCK, straceBinary: process.env.PI_SPEC_STRACE,
			heldExecBinary: process.env.PI_SPEC_HELD_EXEC, heldExecObserverBinary: process.env.PI_SPEC_HELD_EXEC_OBSERVER, ...options };
		configureNativeFilesystem(this.options.heldExecBinary);
		this.store = new ProvenanceCertificateStore(options.storeRoot, { ...options.store, acceptedTaints: SAME_CONFINEMENT_TAINTS });
		this.replayWorkspace = new WorkspaceSandboxService(this.store);
		this.planner = new ProcessReusePlanner({ store: this.store });
		this.handoffs = new ProcessHandoffRegistry(this.store, Math.min(MAX_CONTINUATION_BYTES, this.store.limits.maxBytes));
		this.storage = {
			configure: ({ maxEntries, maxBytes }) => {
				this.store.configure({ maxCertificates: maxEntries, maxBytes });
				this.handoffs.configure(this.store.limits.maxCertificates, Math.min(MAX_CONTINUATION_BYTES, this.store.limits.maxBytes));
				void this.synchronous?.call("configure", { maxEntries, maxBytes });
			},
			maintain: async (operation) => {
				this.handoffs.clearCompleted();
				await this.synchronous?.call("maintain", operation);
				const result = await (operation === "gc" ? this.store.gc() : this.store.clear());
				return { removedEntries: result.removedCertificates + result.removedInputVersions, removedArtifacts: result.removedArtifacts, removedBytes: result.removedBytes };
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
				detail: this.options.completedOnly ? "Finished command branches ready; live process capture disabled" : `Landlock/seccomp virtual filesystem + strace provenance ready; ${ready.imageLibrary
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
	metrics(): LinuxProcessReuseMetrics {
		const metrics = { ...this.counters }, remote = this.synchronous?.metrics();
		for (const key of Object.keys(metrics) as (keyof WorldReuseMetrics)[]) if (key !== "lastError") metrics[key] += this.actorCounters[key] + (remote?.[key] ?? 0);
		metrics.lastError ??= remote?.lastError;
		return Object.freeze(metrics);
	}
	producerMetrics(): LinuxProcessReuseMetrics { return { ...this.counters }; }
	private processOwner(): SynchronousProcessObserver {
		if (this.disposed) throw new Error("native process backend disposed");
		return this.synchronous ??= new SynchronousProcessObserver(this.options, this.store);
	}
	serializeBinding(binding: ProcessExecutionBinding, sequence = 0): SerializedProcessBinding | undefined {
		const invocation = this.handoffs.resolveBinding(binding, binding.scope);
		return invocation ? { sequence, key: binding.key, scope: binding.scope, executionMs: binding.executionMs, invocation } : undefined;
	}
	restoreBinding(binding: SerializedProcessBinding): ProcessExecutionBinding | undefined {
		return this.handoffs.observe(binding.key as Sha256Digest, "trackingOnly" in binding.invocation ? binding.invocation.sourceRoot : binding.invocation.executable,
			binding.scope, binding.invocation, binding.executionMs);
	}
	recordWorkerAdoption(work: number, adoption: ExecutionOperationAdoption): void {
		const observation = this.observations.getStore(), computation = adoption.computation && serializeNativeDependency(adoption.computation);
		if (observation && !observation.closed) observation.adoptions.push({ owner: { work }, adoption: { ...adoption, computation } });
	}

	/** Actor-path counters, excluding child reuse performed inside speculative worlds. */
	actorMetrics(): LinuxProcessReuseMetrics { return Object.freeze({ ...this.actorCounters }); }
	/** The issued operation owns its launch context independently of the enclosing tool's implementation. */
	bindingWorkspace(binding: ProcessExecutionBinding): string { return this.boundInvocation(binding, binding.scope).sourceRoot; }
	private boundInvocation(binding: ProcessExecutionBinding, scope: ExecutionScope | undefined): BoundProcessInvocation {
		const invocation = this.handoffs.resolveBinding(binding, scope);
		if (!invocation || "trackingOnly" in invocation) throw new Error("process execution binding is unavailable in this scope");
		return invocation;
	}

	/** A named input can propose a fresh launch; private predecessors and live queues keep their producer ordering. */
	operationHints(binding: ProcessExecutionBinding): Pick<ExecutionOperationBinding, "fed" | "preparation"> {
		const invocation = this.handoffs.resolveBinding(binding, binding.scope);
		if (!invocation || "trackingOnly" in invocation) return {};
		const resources = invocation.resources;
		if (reopensProcessInputs(invocation)) return { preparation: "current_workspace" };
		if (!resources) return {};
		return Object.entries(resources.objects).some(([image, object]) => object.queue?.producer === "live" &&
			!Object.values(resources.descriptions).some(description => description.object === Number(image) && (description.flags & 3) !== 0)) ? { fed: true } : {};
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
		const remote = () => this.synchronous?.call<boolean>("stale", this.serializeBinding(binding)).catch(() => true) ?? Promise.resolve(true);
		if (this.synchronous && !this.producers && !this.handoffs.hasResults) return remote();
		const stale = await this.store.read(async signal => {
			const projection = new ExecutionPathProjection({ sourceRoot: invocation.sourceRoot, workspaceRoot: invocation.sourceRoot });
			const { output } = await TimelineInterval.collect(() => this.boundRequest(binding, invocation, projection, signal));
			const key = processWeakKey(output.prototype);
			const checked = new Set<Sha256Digest>();
			const currentInputs = observeProcessInputs(key, evidence => TimelineInterval.collect(() => validateDynamicDependencyCertificate(evidence,
				// This scheduling observation grants no adoption authority; failed runs also suppress repeated preparation while their inputs hold.
				{ acceptedTaints: evidence.taints, signal })).then(result => result.output, () => undefined));
			for (const candidates of [this.handoffs.results(key, binding.scope), this.store.candidatesByWeakKey(key, invocation.executable, checked, signal)]) for await (const certificate of candidates) {
				if (checked.has(certificate.id)) continue;
				checked.add(certificate.id);
				if ((await currentInputs(certificate.dependencyCertificate))?.strongKey === certificate.strongKey) return false;
			}
			return true;
		}).catch(() => true);
		return stale ? remote() : false;
	}

	/** Keep possible publication visible through preparation, execution, and final evidence capture. */
	async withProducer<Value>(run: () => Promise<Value>): Promise<Value> {
		Atomics.store(this.localSource, 0, 1);
		this.producers++;
		try { return await run(); } finally { this.producers--; }
	}

	get hasReusableWork(): boolean { return this.producers > 0 || this.handoffs.hasResults || !!this.synchronous?.hasWork; }

	private heldBoundary(): Promise<LinuxHeldExecBoundary> {
		if (this.disposed || this.nativeRetirement) return Promise.reject(new Error("native process observation is retiring"));
		if (!this.heldExec) {
			const controller = new AbortController();
			const entry: NonNullable<LinuxProcessReuseBackend["heldExec"]> = { controller, status: { state: "probing", detail: "Checking native process startup" },
				ready: LinuxHeldExecBoundary.open({ storeRoot: this.options.storeRoot, reservedRoot: this.options.store?.reservedRoot, binary: this.options.heldExecBinary, observerBinary: this.options.heldExecObserverBinary,
					signal: AbortSignal.any([this.store.signal, controller.signal]) }).then(boundary => {
						entry.status = { state: "ready", detail: "Native child startup and validated result reuse" }; return boundary;
					}, error => { entry.status = { state: "unavailable", detail: errorMessage(error) }; throw error; }) };
			this.heldExec = entry;
		}
		return this.heldExec.ready;
	}
	nativeDiagnostics(enabled = true): ProcessRouteSnapshot {
		if (this.disposed || process.platform !== "linux") return { state: "unavailable", detail: this.disposed ? "Native route disposed" : "Linux or WSL 2 required" };
		if (!enabled) return { state: "disabled", detail: "Native process reuse is disabled" };
		return this.nativeRetirement ? { state: "probing", detail: "Draining native observations" }
			: this.heldExec?.status ?? { state: "idle", detail: "Checked when native work is learned or reusable evidence exists" };
	}
	async refreshNativeObservation(enabled = true): Promise<void> {
		await this.resetNativeObservation();
		if (enabled && !this.disposed && process.platform === "linux") await this.heldBoundary().catch(() => undefined);
	}

	private nativeObservation(sourceRoot: string, scope?: ExecutionScope, signal?: AbortSignal) {
		let descriptors: boolean | "inspect" = this.handoffs.needsContinuationTracking(scope);
		if (scope) for (const binding of this.handoffs.bindings(scope)) {
			const invocation = this.handoffs.resolveBinding(binding, scope);
			if (invocation && ("trackingOnly" in invocation || invocation.resources?.handles.length) && invocation.sourceRoot === sourceRoot) descriptors = true;
		}
		if (!descriptors && (this.observations.getStore()?.learn || this.hasReusableWork)) descriptors = "inspect";
		return { sourceRoot, scope: snapshotExecutionScope(scope), signal, descriptors,
			decide: (process: HeldExecProcess) => this.decideHeldExec(process, process.scope) };
	}

	/** Observe the native launches of the actual callback, independently of its tool name or process adapter. */
	async observeNative<Value>(sourceRoot: string, scope: ExecutionScope | undefined, signal: AbortSignal | undefined, execute: () => Promise<Value>): Promise<Value> {
		let boundary: LinuxHeldExecBoundary | undefined;
		const owner = snapshotExecutionScope(scope), observation = this.observations.getStore();
		try {
			if (!this.disposed && process.platform === "linux" && ((observation?.learn && !observation.closed && sameScope(observation.scope, owner)) ||
				this.hasReusableWork || await this.store.mayHaveCertificates(undefined, signal) || this.hasReusableWork)) {
				const ready = await waitForCompletion(this.heldBoundary(), signal);
				if (ready.status === "completed") boundary = ready.value;
			}
		} catch { /* Optional native observation leaves the actual callback authoritative. */ }
		const options = this.nativeObservation(path.resolve(sourceRoot), owner, signal);
		return boundary ? boundary.observe(options, execute, {
			synchronous: (gate, skipRoot) => this.attachSynchronous({ ...options, gate, skipRoot }),
			asynchronous: (gate, skipRoot) => this.attachSynchronous({ ...options, gate, skipRoot, asynchronous: true }),
		}) : execute();
	}

	private attachSynchronous(options: Pick<SynchronousProcessRequest, "gate" | "scope" | "sourceRoot" | "descriptors" | "skipRoot" | "asynchronous">) {
		const startedAt = performance.now(), observation = this.observations.getStore(), loan = this.handoffs.lend(options.scope);
		const claims = new Int32Array(new SharedArrayBuffer(4));
		let inputs: ReturnType<ResourceObjectInputs["lend"]> | undefined;
		let observer: SynchronousProcessObserver | undefined;
		const overhead = (start: number) => { const computation = new TimelineInterval(start, performance.now());
			observation?.computations.push({ computation, overhead: true, shared: [computation] }); };
		try {
			inputs = observation?.inputs?.lend(Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes));
			observer = this.processOwner();
			const attachment = observer.attach({ gate: options.gate, scope: options.scope, sourceRoot: options.sourceRoot, descriptors: options.descriptors,
				skipRoot: options.skipRoot, asynchronous: options.asynchronous, learn: !!observation?.learn, reuseEnabled: observation?.reuseEnabled, handoffs: loan.transfer, inputs: inputs?.transfer,
				owners: observer.grants(options.scope), claims: claims.buffer as SharedArrayBuffer, ordering: observation?.ordering.buffer as SharedArrayBuffer | undefined,
				lender: options.asynchronous ? this.localSource.buffer as SharedArrayBuffer : undefined },
				options.asynchronous ? delta => this.replayWorkspace.commitDelta(delta) : undefined, options.asynchronous ? loan.borrow : undefined);
			overhead(startedAt);
			const apply = (result: SynchronousProcessResult) => {
				for (const [key, value] of Object.entries(result.metrics)) {
					if (typeof value === "number") this.add(undefined, key as CountedReuseMetric, value); else if (value) this.setError(undefined, value);
				}
				for (const { owner, adoption } of result.adoptions) {
					const computation = adoption.computation && restoreNativeDependency(adoption.computation), binding = "loan" in owner && loan.binding(owner.loan);
					if (binding && observation && !observation.closed && observation.bindings.size < this.store.limits.maxCertificates)
						observation.bindings.set(adoption.sequence, binding);
					try { if ("loan" in owner) loan.adopted(owner.loan, { ...adoption, computation }); else observer!.adopted(owner.work, { ...adoption, computation }); }
					catch { /* Completed handoff feedback is advisory. */ }
				}
				if (observation && !observation.closed) {
					observation.inputs?.receive(result.inputs ?? []);
					for (const dependency of result.computations) { const restored = restoreNativeDependency(dependency); if (restored) observation.computations.push(restored); }
					for (const entry of result.bindings) nativeLearningSlot(observation, entry.executionMs, entry.key, this.store.limits.maxCertificates,
						() => ({ binding: this.restoreBinding(entry), order: entry.sequence }));
				}
			};
			const failed = (error: unknown) => {
				this.setError(undefined, `native_owner:${errorMessage(error)}`);
				void observer!.close(); if (this.synchronous === observer) this.synchronous = undefined;
				if (Atomics.load(claims, 0) || loan.claimed() || isPoisonedEffectCommit(error))
					throw effectCommitFailure(new Error("native handoff failed", { cause: error }), "poisoned");
			};
			const release = () => { attachment.release(); inputs?.release(); loan.settle(); };
			return { pid: attachment.pid, retire: attachment.retire,
				completion: attachment.completion?.then(apply).catch(failed).finally(release) ?? Promise.resolve(),
				finish: () => { const finishing = performance.now();
					try { apply(attachment.finish()); } catch (error) { failed(error); } finally { release(); overhead(finishing); }
				} };
		} catch (error) {
			this.setError(undefined, `native_owner:${errorMessage(error)}`);
			inputs?.release(); loan.settle(); overhead(startedAt); void observer?.close(); if (this.synchronous === observer) this.synchronous = undefined; throw error;
		}
	}

	/** Worker endpoint; the normal policy, native transport and observation admission remain authoritative. */
	async observeSynchronous(request: SynchronousProcessRequest, attached: (pid?: number) => void, finished: Promise<void>, commitDelta?: WorkspaceSandboxService["commitDelta"], borrow?: ProcessHandoffLender) {
		let observation: NativeObservation | undefined;
		const receive = (transfer: SynchronousProcessRequest["handoffs"]) => this.handoffs.receive(transfer, request.scope, (index, adoption) => {
			const computation = adoption.computation && serializeNativeDependency(adoption.computation);
			observation?.adoptions.push({ owner: { loan: index }, adoption: { ...adoption, computation } });
		});
		const releases = [await receive(request.handoffs)];
		let bindings: SynchronousProcessResult["bindings"] = [], computations: SynchronousProcessResult["computations"] = [];
		const maxBytes = Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes);
		const received = request.inputs && ResourceObjectInputs.receive(request.inputs, maxBytes);
		let loan: ReturnType<ResourceObjectInputs["lend"]> | undefined;
		try {
			const execute = async () => {
				observation = this.observations.getStore();
				observation!.claims = new Int32Array(request.claims);
				if (commitDelta) observation!.effects = { commitDelta };
				if (borrow && request.lender) {
					const available = new Int32Array(request.lender);
					observation!.source = { available, borrow: async (key, excluded, signal) => {
						if (!Atomics.load(available, 0)) return;
						const loan = await borrow(key, excluded, signal); if (!loan) return;
						const received = await receive(loan.transfer); releases.push(received);
						return { records: received.records, joined: loan.joined };
					} };
				}
				if (request.ordering) observation!.ordering = new Int32Array(request.ordering);
				const boundary = await this.heldBoundary(), options = this.nativeObservation(request.sourceRoot, request.scope);
				const descriptors = options.descriptors === true || request.descriptors === true ? true : options.descriptors || request.descriptors;
				const attachment = boundary.attach({ ...options, descriptors, skipRoot: request.skipRoot }, request.gate);
				attached(attachment.pid);
				if (request.asynchronous) void finished.then(attachment.retire); else await finished;
				await attachment.completion;
			};
			await this.observeBindings(request.scope!, execute, (_observed, dependencies) => {
				bindings = [...observation!.bindings].flatMap(([sequence, binding]) => this.serializeBinding(binding, sequence) ?? []);
				computations = dependencies.flatMap(dependency => { const serialized = serializeNativeDependency(dependency); return serialized ? [serialized] : []; });
			}, request.learn && !!request.scope, received?.inputs, request.reuseEnabled);
			loan = received?.inputs.lend(maxBytes);
			return { result: { bindings, computations, adoptions: observation!.adoptions, metrics: observation!.metrics, inputs: loan?.transfer },
				release: () => { loan?.release(); return received?.dispose(); } };
		} finally { for (const release of releases) release(); if (!loan) await received?.dispose(); }
	}

	resetNativeObservation(): Promise<void> {
		if (this.nativeRetirement) return this.nativeRetirement;
		const heldExec = this.heldExec;
		this.heldExec = undefined;
		const synchronous = this.synchronous; if (this.disposed) this.synchronous = undefined;
		heldExec?.controller.abort(new Error("held-exec initialization retired"));
		return this.nativeRetirement = Promise.resolve(heldExec?.ready).then(boundary => boundary?.close(), () => undefined)
			.then(() => this.disposed ? synchronous?.close() : synchronous?.call<void>("reset"))
			.finally(() => { this.nativeRetirement = undefined; });
	}

	/** Hit-only Actor path. Certificate lookup and replay deliberately require no tracing or confinement tools. */
	completedReplayExecutor(host: ProcessExecutor, options: CompletedProcessReplayOptions): ProcessExecutor {
		const sourceRoot = path.resolve(options.sourceRoot);
		const execute = (request: ProcessExecutionRequest) => LinuxHeldExecBoundary.command(() => host.execute(request));
		const acceptProducer = (producer: ProcessProducerProof) => actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
		return {
			execute: async (request) => {
				if (this.disposed || this.observations.getStore()?.reuseEnabled === false || process.platform !== "linux" || !pathContains(sourceRoot, request.cwd)) return execute(request);
				this.add(undefined, "wholeCommandRequests");
				const result = await TimelineInterval.outside(async () => {
				let committed = false;
				let plan: Awaited<ReturnType<LinuxProcessReuseBackend["plan"]>>;
				try {
					request.signal?.throwIfAborted();
					const invocation = options.invocation(request);
					if (!invocation || !await this.store.mayHaveCertificates(invocation.shell, request.signal)) return;
					assertInvocationMatches(invocation, request);
					const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
					const platformFingerprint = await this.resolvePlatformFingerprint();
					const prototype = (await TimelineInterval.collect(() => topLevelProcessPrototype(invocation, request, definedProcessEnvironment(request.environment), projection, platformFingerprint))).output;
					const weakKey = processWeakKey(prototype);
					plan = await this.plan(weakKey, prototype.executablePath, projection, acceptProducer, { signal: request.signal });
					if (this.disposed || !plan?.certificate.result.exit) return;
					request.signal?.throwIfAborted();
					await replayFilesystemEffects(this.replayWorkspace, this.store, plan.artifacts, plan.certificate.result.journal, projection);
					committed = true;
					for (const event of loadOutputEvents(plan.artifacts, plan.certificate.result.journal)) request.onData(event.data);
					const observation = this.observations.getStore();
					if (observation && !observation.closed && sameScope(observation.scope, request.scope))
						observation.computations.push(reusedComputation(plan.certificate));
					this.add(undefined, "wholeCommandReusedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
					this.add(undefined, "wholeCommandHits");
					return { exitCode: plan.certificate.result.exit.kind === "code" ? plan.certificate.result.exit.code : null };
				} catch (error) {
					this.setError(undefined, `actor_replay:${errorMessage(error)}`);
					if (committed || isPoisonedEffectCommit(error)) throw error;
				} finally { await plan?.artifacts.dispose(); }
				});
				if (result) return result;
				this.add(undefined, "wholeCommandMisses");
				return execute(request);
			},
		};
	}

	async open(input: Omit<ExecutionResourceContext, "inputs" | "onInputSource"> & {
		readonly sourceRoot: string;
		readonly workspace: SandboxWorkspaceContext;
		readonly invocation: ToolProcessInvocation | ProcessExecutionBinding;
		readonly scope?: ExecutionScope;
		readonly signal?: AbortSignal;
	}): Promise<LinuxProcessSession> {
		return this.withProducer(async (): Promise<LinuxProcessSession> => {
			const owner = nativeWorkspaceOwner(input.workspace);
			if (owner) return this.processOwner().open(input, await owner, this);
			const { session, ready, readyAt, controller, server } = await this.allocateSession(input), { socketPath } = session;
			let executionKind: "tool" | "operation" | undefined;
			input.onOperationJoinable?.(() => !session.signal.aborted && this.handoffs.hasJoinableWork(session.ownership));
			this.producers++;
			let dispatch: Promise<void> | undefined;
			const execute = <Value>(kind: NonNullable<typeof executionKind>, operation: () => Promise<Value>): Promise<Value> => {
				if (session.lifecycle.sealed || session.sealPromise || executionKind === "operation" || executionKind && executionKind !== kind)
					return Promise.reject(new Error("process session execution boundary is already consumed"));
				executionKind = kind;
				return session.lifecycle.admit(() => measureProcess(session, async () => {
					session.signal?.throwIfAborted();
					const dispatchStartedAt = performance.now();
					// Bound operations can launch reusable children too; interception excludes their own image below.
					await (dispatch ??= this.createProcessInterposition(session, ready).then(async interposition => {
						session.signal?.throwIfAborted();
						session.interposition = interposition;
						await listenUnixSocket(server, socketPath);
						await chmod(socketPath, 0o600);
						const dispatch = new TimelineInterval(dispatchStartedAt, performance.now());
						const computation = new TimelineInterval(creation.startedAt, dispatch.completedAt, [
							{ computation: creation, owned: true, shared: [creation] }, { computation: dispatch, owned: true, shared: [dispatch] },
						], []);
						TimelineInterval.group(computation, session);
						// Only recorded preparation cuts its caller's clock; work between creation and dispatch belongs to that caller.
						session.preparation = { computation, shared: [creation, dispatch] };
						session.computations.push({ ...session.preparation, owned: true });
					}));
					session.privateSince ??= Date.now();
					await (session.transaction ??= session.workspace.transactions.begin());
					return operation();
				}));
			};
			const creation = new TimelineInterval(readyAt, performance.now());
			return {
				ownership: session.ownership,
				computationDependencies: () => Object.freeze([...session.computations]),
				executionBindings: () => Object.freeze([...session.executionBindings].sort(([left], [right]) => left - right).map(([, binding]) => binding)),
				executor: { execute: request => execute("tool", async () => {
					const { invocation } = session;
					if (!invocation) throw new Error("process session has no whole-tool invocation");
					if (session.lifecycle.sealed) throw new Error("Linux process session is closed");
					assertInvocationMatches(invocation, request);
					const physicalCwd = session.projection.toPhysical(path.resolve(request.cwd));
					if (!pathContains(session.sourceRoot, path.resolve(request.cwd)) || !physicalCwd || !pathContains(session.workspace.sandboxRoot, physicalCwd))
						throw new Error("process cwd is unmapped");
					const ready = await this.resolveReady(), environment = definedProcessEnvironment(request.environment);
					const { output: prototype, computation: preparation } = await TimelineInterval.outside(() => TimelineInterval.measure(() =>
						topLevelProcessPrototype(invocation, request, environment, session.projection, ready.platformFingerprint)));
					this.add(session, "wholeCommandRequests");
					const result = await this.executeRequest(session, { argv0: invocation.shell, args: shellArguments(invocation, request.command), cwd: physicalCwd, environment, outputRoute: [1, 2] },
						invocation.shell, -session.metrics.wholeCommandRequests, prototype, true, undefined, preparation, request);
					if (result.kind === "hit") for (const event of result.output ?? []) request.onData(event.data);
					return { exitCode: result.exit?.kind === "code" ? result.exit.code : null };
				}) },
				executeBinding: binding => execute("operation", async () => {
					const invocation = this.boundInvocation(binding, session.scope);
					if (invocation.sourceRoot !== session.sourceRoot ||
						invocation.producer && !stableEqual(session.nestedProducer, invocation.producer))
						throw new Error("process execution binding is unavailable in this scope");
					const { output: { request, executable, prototype }, computation: preparation } = await TimelineInterval.outside(() =>
						TimelineInterval.measure(() => this.boundRequest(binding, invocation, session.projection, session.signal)));
					const resultsBefore = session.preparedResults;
					this.add(session, "requests");
					const result = await this.executeRequest(session, request, executable, session.metrics.requests, prototype, true, undefined, preparation);
					session.signal.throwIfAborted();
					if (!invocation.producer && !invocation.resources && result.kind === "executed" && result.reusable === false &&
						session.preparedResults === resultsBefore) {
						throw new Error(`bound process preparation produced no reusable result${session.metrics.lastError ? `: ${session.metrics.lastError}` : ""}`);
					}
					return { output: result.output ?? [],
						...(result.kind === "suspended" ? { suspended: true as const } : { exit: result.exit! }) };
				}),
				metrics: () => Object.freeze({ ...session.metrics }),
				seal: _changes => session.sealPromise ??= this.withProducer(async () => {
					await session.lifecycle.drain();
					const external = await privateCommits(session, session.privateSince ?? 0, sessionWriters(session));
					// Adopting without them would leave the host unlike the native run: the Actor runs it instead.
					if (!external.complete) throw new Error(`unrepresentable writes outside the workspace: external_write:${[...external.before].find(([, value]) => !value)?.[0]}`);
					return [...await sealSessionEvidence(session, external), ...external.changes];
				}),
				validate: async () => await readFile("/proc/self/mountinfo", "utf8") !== session.sourceMounts
					? { status: "stale", cause: { stage: "freshness", code: "source_mount_policy_changed" }, metrics: { bytesRead: 0, filesRead: 0, mode: "exact" } }
					: validateTransferredProcessEvidence(session.topLevelEvidence, session.incompleteReasons),
				close: () => session.lifecycle.close(async () => {
					controller.abort(new Error("Linux process session closed"));
					try {
						await dispatch?.catch(() => undefined);
						try { await closeServer(server); } finally {
							await session.lifecycle.drain();
							await captureSessionInterval(session);
							await rm(socketPath, { force: true }).catch(() => undefined);
						}
					} finally { session.privateFiles.clear(); this.producers--; }
				}),
			};
		});
	}

	private async allocateSession(input: Parameters<LinuxProcessReuseBackend["open"]>[0]) {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		const ready = await TimelineInterval.outside(async () => (await TimelineInterval.collect(() => this.resolveReady())).output);
		const readyAt = performance.now();
		input.signal?.throwIfAborted();
		const sourceRoot = path.resolve(input.sourceRoot);
		const sourceMounts = await readFile("/proc/self/mountinfo", "utf8");
		if (input.workspace.metadataMounts !== undefined && input.workspace.metadataMounts !== sourceMounts) throw new Error("source mount policy changed during workspace capture");
		const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: input.workspace.sandboxRoot });
		const invocation = "environment" in input.invocation ? input.invocation : undefined;
		const bound = "environment" in input.invocation ? undefined : this.boundInvocation(input.invocation, input.scope);
		if (bound && bound.sourceRoot !== sourceRoot) throw new Error("bound process workspace changed");
		const token = randomBytes(32).toString("hex");
		const processRoot = await mkdtemp(path.join(input.workspace.processRoot, "session-"));
		const candidateSocket = path.join(processRoot, `broker-${token.slice(0, 12)}.sock`);
		// sockaddr_un has a byte limit even when the private workspace supports longer paths.
		const socketPath = Buffer.byteLength(candidateSocket) < 104 ? candidateSocket : path.join("/tmp", `pi-broker-${randomBytes(16).toString("hex")}.sock`);
		const deniedPaths = sensitivePaths(this.options.storeRoot, this.options.deniedPaths).filter(
			(target) =>
				!pathContains(input.workspace.sandboxRoot, target) && !pathContains(input.workspace.processRoot, target),
		);
		const producer = speculativeProducerProof(ready, deniedPaths, POLICY_ID);
		const nestedProducer = speculativeProducerProof(ready, deniedPaths, LEAF_POLICY_ID);
		const controller = new AbortController();
		const server = net.createServer({ allowHalfOpen: true }, (socket) => this.serve(session, socket));
		const gitDirectory = await lstat(path.join(sourceRoot, ".git")).then((info) => info.isDirectory() ? path.join(sourceRoot, ".git") : undefined, () => undefined);
		const session = {
			token, processRoot, sourceRoot, sourceMounts, projection, deniedPaths, producer, nestedProducer, socketPath,
			// The workspace's own repository stays read-only instead of exposing the snapshot's repository.
			...(gitDirectory ? { gitDirectory } : {}), workspace: input.workspace, invocation,
			scope: snapshotExecutionScope(input.scope),
			interposition: "",
			signal: AbortSignal.any([controller.signal, ...(input.signal ? [input.signal] : [])]),
			lifecycle: new RuntimeLifecycleLane(),
			ownership: new ProcessHandoffOwnership(input.onOperationAdopted, input.acceptOperationScope),
			// Completed reusable children and retained live frontiers produced by this preparation.
			preparedResults: 0,
			executionBindings: new Map<number, ProcessExecutionBinding>(), computations: [] as TimelineDependency[],
			// One physical session setup, shared by every result that depended on it.
			preparation: undefined as TimelineDependency | undefined,
			incompleteReasons: new Set<string>(),
			// Only images stopped by these owned tracers may request execution.
			tracers: new Set<number>(),
			// Concurrent writers retain their live trace or settled writes for ownership attribution.
			writers: workspaceWriterRecords(input.workspace), observationOwner: nativeWorkspaceObservation(input.workspace),
			metrics: emptyWorldReuseMetrics() as MutableLinuxProcessReuseMetrics,
			transaction: undefined as Promise<WorkspaceTransactionCapture> | undefined,
			capture: undefined as Promise<WorkspaceTransactionDelta> | undefined,
			topLevelEvidence: undefined as DynamicDependencyCertificate | undefined,
			topLevelOutputEndpoints: undefined as readonly [string, string] | undefined,
			// Children write into their parent's captured output sockets.
			nestedOutputEndpoints: new Set<readonly [string, string]>(),
			sealPromise: undefined as Promise<readonly SandboxWorkspaceChange[]> | undefined,
			// When the top level began writing into the private branch.
			privateSince: undefined as number | undefined,
			privateFiles: new Map<string, StableFilesystemCapture>(),
		};
		return { session, ready, readyAt, controller, server };
	}

	dispose(): Promise<void> {
		if (this.disposal) return this.disposal;
		this.disposed = true;
		const handoffs = this.handoffs.dispose();
		const store = this.store.dispose();
		return this.disposal = this.resetNativeObservation().finally(() => Promise.all([this.replayWorkspace.dispose(), store, handoffs]));
	}

	private async resolveReady(): Promise<ReadyBackend> {
		if (this.disposed) throw new Error("Linux process backend is disposed");
		this.ready ??= TimelineInterval.collect(() => this.probe()).then(result => result.output);
		return this.ready;
	}

	private resolvePlatformFingerprint(): Promise<Sha256Digest> {
		this.platformFingerprint ??= execText("uname", ["-srm"]).then((kernel) => digestObject({ kernel: kernel.trim(), arch: process.arch }));
		return this.platformFingerprint;
	}

	private async probe() {
		if (process.platform !== "linux") throw new Error("Linux host required");
		if (!this.options.store?.reservedRoot) await mkdir(this.options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(this.options.storeRoot, 0o700);
		const [sandlock, strace, dispatcher] = await Promise.all([
			resolveHostExecutable(this.options.sandlockBinary, "pi-speculative-sandlock", [path.join(os.homedir(), ".local", "bin", "pi-speculative-sandlock")]),
			resolveHostExecutable(this.options.straceBinary, "strace", [path.join(os.homedir(), ".local", "bin", "pi-speculative-strace")]),
			resolveLinuxExecHelper(this.options.heldExecBinary),
		]);
		// The exact confinement and tracer builds, not their versions: a patched fix changes what their certificates mean.
		const [sandlockCheck, sandlockBuild, straceBuild, platformFingerprint] = await Promise.all([
			execText(sandlock, ["check"]), hashExecutableFile(sandlock), hashExecutableFile(strace), this.resolvePlatformFingerprint(),
		]);
		if (!sandlockCheck.includes("Status:         OK")) throw new Error("Sandlock kernel protections are unavailable");
		nativeProcessLibrary(dispatcher);
		const qualifyLibrary = (file: string) => execText(strace, ["--kill-on-exit", "-f", "-q", "-e", "trace=none", "-o", "/dev/null",
			`--process-library=${file}`, "--handoff-image=/dev/null", "--", "/bin/true"]).then(() => file);
		const imageLibrary = await qualifyLibrary(`${dispatcher}.so`).catch(() => undefined);
		const processLibrary = imageLibrary ?? await qualifyLibrary(`${dispatcher}.node`);
		const mountProbe = await mkdtemp(path.join(os.tmpdir(), "pi-process-view-probe-"));
		let executionContext: ProcessExecutionContext | undefined;
		try {
			const logicalRoot = path.join(mountProbe, "logical");
			const physicalRoot = path.join(mountProbe, "physical");
			await Promise.all([mkdir(logicalRoot), mkdir(physicalRoot)]);
			const routing = path.join(mountProbe, "routing"), launcher = path.join(routing, "dispatcher");
			await mkdir(routing); await copyFile(dispatcher, launcher); await chmod(launcher, 0o755);
			await writeFile(path.join(physicalRoot, "script-position"), "#!/bin/sh\nexit 42\n", { mode: 0o700 });
			const metadataImage = path.join(physicalRoot, "metadata.json"), metadataStat = await lstat(path.join(physicalRoot, "script-position"), { bigint: true });
			const metadataRow = FILESYSTEM_METADATA_FIELDS.map(field => String(metadataStat[field]));
			await writeFile(metadataImage, JSON.stringify([[metadataRow, metadataRow, 0]]), { mode: 0o600 });
			const command = straceCommand(strace, path.join(physicalRoot, "context"), [
				sandlock,
				...sandboxPolicyArguments(logicalRoot, [], [physicalRoot], [{ virtualPath: logicalRoot, hostPath: physicalRoot, readOnly: false },
					{ virtualPath: routing, hostPath: routing, readOnly: true }], [{ virtualPath: "/", hostPath: launcher }, { virtualPath: launcher, hostPath: launcher }], path.join(physicalRoot, "private")),
				"--metadata-image", metadataImage, "--preserve-fd", "3",
				"--",
				launcher,
				"--probe-context",
				logicalRoot,
				path.join(logicalRoot, "script-position"),
			]);
			const outcome = await runSpawn([strace, `--process-library=${processLibrary}`, `--exec-context-source=${launcher}`, ...command.slice(1)], { cwd: physicalRoot, environment: definedProcessEnvironment(process.env), onControl: channel => {
				const capture = (async () => {
					let bytes = Buffer.alloc(0);
					for await (const [chunk] of on(channel, "data", { close: ["end", "close"] })) {
						bytes = Buffer.concat([bytes, chunk]);
						if (bytes.length > 4) throw new Error("invalid context probe pid");
						if (bytes.length < 4) continue;
						const pid = bytes.readInt32LE();
						executionContext = await captureProcessContext(pid, (await readdir(`/proc/${pid}/fd`)).filter(name => /^[012]$/.test(name)));
						await writeProcessChannel(channel, Buffer.from("R")); channel.end(); return;
					}
					throw new Error("context probe closed before capture");
				})();
				void capture.catch(() => channel.destroy());
				return () => capture;
			} });
			if (outcome.signal || outcome.code !== 0) throw new Error("process execution context probe did not finish");
			if (!validProcessContext(executionContext)) throw new Error("process execution context probe returned invalid data");
		} catch (cause) {
			throw new Error(`process execution/metadata policy probe failed; rebuild with npm run setup:linux${await inheritedTracer()}`, { cause });
		} finally {
			await rm(mountProbe, { recursive: true, force: true });
		}
		const observerFingerprint = OBSERVER_FINGERPRINT;
		const fingerprint = digestObject({
			observerFingerprint,
			policy: POLICY_ID,
			sandlock: sandlockBuild,
			strace: straceBuild,
			platformFingerprint,
			executionContext: sha256Digest(executionContext.key),
			arch: process.arch,
			deniedPaths: sensitivePaths(this.options.storeRoot, this.options.deniedPaths),
		});
		return { sandlock, strace, fingerprint, platformFingerprint, observerFingerprint, executionContext, dispatcher, processLibrary, ...(imageLibrary ? { imageLibrary } : {}) };
	}

	private serve(session: ActiveSession, socket: net.Socket): void {
		let body = "";
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => { body += chunk; if (Buffer.byteLength(body) > MAX_REQUEST_BYTES) socket.destroy(new Error("request too large")); });
		socket.once("error", () => undefined);
		socket.once("end", AsyncLocalStorage.bind(() => {
			void session.lifecycle.track(Promise.resolve().then(() => this.handleWireRequest(session, body)).then((response) => { socket.end(wireResponse(response)); })
				.catch((error) => {
					this.setError(session, errorMessage(error));
					session.incompleteReasons.add(`broker:${errorMessage(error)}`);
					socket.end("f\n");
				}));
		}));
	}

	private async handleWireRequest(session: ActiveSession, body: string): Promise<DispatcherResponse> {
		const received = parseDispatcherRequest(body);
		if (!received || received.token !== session.token || session.lifecycle.sealed) throw new Error("invalid dispatcher request");
		session.signal?.throwIfAborted();
		const status = await readFile(`/proc/${received.pid}/status`, "utf8");
		if (!session.tracers.has(Number(/^TracerPid:\s+(\d+)$/m.exec(status)?.[1])) || !/^State:\s+t\b/m.test(status)) throw new Error("process is not held by this session");
		const route = (await readFile(`${session.interposition}.route.${received.pid}`, "utf8")).split("\0"), target = route[1];
		if (route.length < 5 || route[0] !== "B" || route.at(-1) !== "" || !target?.startsWith("/")) throw new Error("invalid exec route");
		const image = await open(`/proc/${received.pid}/exe`, "r");
		try {
			const imageSource = `/proc/${process.pid}/fd/${image.fd}`, actual = await readlink(imageSource), upper = privateUpper(session);
			if (session.projection.toLogical(actual.startsWith(`${upper}/`) ? actual.slice(upper.length) : actual) !== target) throw new Error("exec route image changed");
			const executable = session.projection.toPhysical((await image.stat()).nlink ? target : target.replace(/ \(deleted\)$/, ""))!;
			const snapshot = await inspectHeldExecProcess(received.pid, executable, "stdio"), cwd = session.projection.toPhysical(snapshot.cwd);
			if (!cwd || snapshot.invocation![0] !== route[2] || snapshot.invocation![1] !== Buffer.from(route[3]!).subarray(0, 15).toString()) throw new Error("exec context changed");
			const request: DispatcherRequest = { ...received, invocation: snapshot.invocation!, argv0: snapshot.argv[0]!,
				args: snapshot.argv.slice(1), cwd, environment: snapshot.environment, context: snapshot.context };
			this.add(session, "requests");
			const requestID = session.metrics.requests, ready = await this.resolveReady();
			const resources = request.descriptors?.length ? await this.inheritedPipes(request).catch((error: unknown) => `inherited_pipes:${errorMessage(error)}`) : undefined;
			const eligibility = typeof resources === "string" ? resources : eligibleRequest(session, request, ready.executionContext);
			if (typeof eligibility === "string") { this.setError(session, `process_bypass:${eligibility}`, "bypasses"); return { kind: "bypass" }; }
			const { argv0, args, environment, invocation } = request;
			return await this.executeRequest(session, { argv0, args, cwd, environment, invocation, imageSource, ...eligibility,
				...(typeof resources === "object" ? { resources, streams: true as const } : {}) }, executable, requestID, undefined, undefined, request.pid);
		} finally { await image.close(); }
	}

	/** A pipe of one repeated byte (a jobserver's tokens) ends as it began whatever order its bytes move in: only those are brokered. */
	private async inheritedPipes(request: DispatcherRequest): Promise<ProcessResourceGraph> {
		// A read past the snapshot is answered only by the run's own writer: without one, a writer elsewhere would leave it waiting forever.
		const ends = (inode: string, access: number) => request.descriptors!.some(entry => entry.inode === inode && (entry.flags & 3) === access);
		if (request.descriptors!.some(({ inode }) => !ends(inode, 0) || !ends(inode, 1))) throw new Error("writer_outside_run");
		return captureHeldDescriptorInputs(request.pid, request.descriptors!.map(({ fd, alias, device, inode, flags, capacity, eof, queueHex }) =>
			({ fd, alias, device, inode, flags, capacity, eof, queueHex, type: "pipe" as const, offset: 0, owned: false, outside: 3 })),
		Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes), sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
	}

	private async executeRequest(session: ActiveSession, request: ProcessArguments, executable: string,
		requestID: number, prototype?: ExecPrototype,
		captureWorkspace?: true, inPlace?: number, preparation?: TimelineInterval, outlet?: ProcessExecutionRequest): Promise<DispatcherResponse> {
		return measureProcess(session, async () => {
			const { outputRoute } = request;
			const signal = AbortSignal.any([session.signal, ...(outlet?.signal ? [outlet.signal] : [])]); signal.throwIfAborted();
			const captured = prototype ? { output: prototype, computation: preparation }
				: await TimelineInterval.outside(() => TimelineInterval.measure(() => this.prototype(session.projection, request, executable)));
			preparation = captured.computation;
			const observedPrototype = captured.output, weakKey = processWeakKey(observedPrototype), executablePath = observedPrototype.executablePath;
			const acquired = await TimelineInterval.outside(() => this.handoffs.acquire({
				key: weakKey, signal, scope: session.scope, producer: { ownership: session.ownership, executablePath },
				lookup: (live, excludedCertificates, artifacts) => this.plan(weakKey, executablePath, session.projection, candidate => stableEqual(outlet ? session.producer : session.nestedProducer, candidate), { session, live, excludedCertificates, artifacts, signal }),
			}));
			if (acquired.kind === "hit" && acquired.plan.kind === "completed_replay") {
				const plan = acquired.plan, binding = acquired.producer?.binding;
				return TimelineInterval.outside(async (): Promise<DispatcherResponse> => {
					const { artifacts, certificate } = plan;
					const output = loadOutputEvents(artifacts, certificate.result.journal), computation = reusedComputation(certificate, acquired);
					const paths = certificate.result.journal.flatMap(event => event.kind === "workspace" ? [event.path] : []), startedAt = Date.now();
					const writer: SessionWriter = { id: `${session.token}:${requestID}`, owner: session.token, startedAt, pid: inPlace, evidence: certificate.dependencyCertificate,
						external: paths.filter(target => !pathContains(session.sourceRoot, target)),
						written: paths.filter(target => pathContains(session.sourceRoot, target)).map(target => slash(path.relative(session.sourceRoot, target))),
						writes: [{ at: startedAt, opened: true, paths: paths.flatMap(target => [target, session.projection.toPhysical(target)!]) }],
						observed: new Map(certificate.dependencyCertificate.dependencies.flatMap(dependency => "path" in dependency && pathContains(session.sourceRoot, dependency.path)
							? [[slash(path.relative(session.sourceRoot, dependency.path)), startedAt] as const] : [])) };
					session.writers.add(writer);
					await shareSessionWriter(session, writer);
					await replayFilesystemEffects(this.replayWorkspace, this.store, artifacts, certificate.result.journal, session.projection,
						path.join(session.processRoot, "private")).then(() => acquired.settle?.(true, true), error => {
							writer.observed = undefined; acquired.settle?.(!(error instanceof EffectCommitFailure && error.disposition === "recoverable")); throw error;
						}).finally(async () => { writer.endedAt = Date.now(); await shareSessionWriter(session, writer); });
					if (outlet) this.add(session, "wholeCommandHits"); else this.recordHit(acquired.producer?.scope, acquired.joined, session);
					recordProcessDependency(session, computation, inPlace);
					const observed = certificate.result.observedProcessMs;
					if (observed !== undefined) this.add(session, outlet ? "wholeCommandReusedProcessMs" : "reusedProcessMs", observed);
					const inputs = request.streams && descriptorInputs(request.resources!), streams = inputs && streamSettlement(inputs, (certificate.result.resources?.transitions ?? []).map(event => ({ alias: event.id, kind: event.kind, data: artifacts.read(event.data) })));
					if (!outlet && binding && this.handoffs.resolveBinding(binding, session.scope)) session.executionBindings.set(requestID, binding);
					return inputs && !streams ? { kind: "hit", output: [], exit: { kind: "code", code: 125 } }
						: { kind: "hit", output, exit: certificate.result.exit, ...(streams?.length ? { streams } : {}) };
				}).finally(() => acquired.settle?.(false));
			}
			if (acquired.kind !== "work") throw new Error("process work reservation failed");
			this.add(session, outlet ? "wholeCommandMisses" : "misses");
			try {
				if (preparation) {
					TimelineInterval.group(preparation, session);
					recordProcessDependency(session, { computation: preparation, owned: true, shared: [preparation] }, inPlace);
				}
				const work = acquired.work;
				const ready = await this.resolveReady();
				const started = performance.now();
				const transaction = await session.workspace.transactions.begin();
				let traceRoot: string | undefined;
				let descriptors: ReturnType<typeof createProcessDescriptorCapture> | undefined;
				let outcome: SpawnOutcome | undefined, reusable: boolean | undefined;
				let transactionFinishing = false;
				let releaseRunning: (() => void) | undefined, inputCheck: Promise<boolean> | undefined;
				let stage = "capture", dependencyCertificate: DynamicDependencyCertificate | undefined, certificateID: Sha256Digest | undefined, unattributed: StraceObservation | undefined;
				let continuation: ProcessContinuation | undefined, frozen: { pid: number; fd: number; writing: boolean; bytes: number } | undefined;
				const executedStreams: { alias: number; kind: string; data: Buffer }[] = [];
				let suspensionAttempted = false, writer: SessionWriter | undefined;
				const failureDetail = (error: unknown) => `${errorMessage(error)}; process=${JSON.stringify({
					stage, requestID, weakKey, scope: session.scope, workspace: session.workspace.sandboxRoot,
					executable: observedPrototype.executablePath, certificateID, complete: dependencyCertificate?.complete, taints: dependencyCertificate?.taints,
				})}`;
				try {
					traceRoot = await mkdtemp(path.join(session.processRoot, "trace-"));
					const previous = session.privateFiles;
					const privateBefore: PrivatePrestate = { startedAt: Date.now(), ...await privateBranch(path.join(session.processRoot, "private")), entries: new Map(), files: session.privateFiles = new Map() };
					await mapFilesystem(sessionWriters(session), async writer => writer.tail?.read());
					let remaining = 128 * 1024 * 1024;
					privateBefore.structure = await captureWorkspaceStructure(privateBefore.upper, { exclude: [], observeStat: async (name, stat) => {
						if (privateBefore.entries.size >= 100_000) throw new Error("private snapshot exceeds its capture limit");
						const target = path.resolve("/", name); privateBefore.entries.set(target, stat);
						// A later write can overwrite the native image's working inode as well as a newly created private file.
						if (stat.isFile() && stat.size <= BigInt(Math.min(32 * 1024 * 1024, remaining))) {
							remaining -= Number(stat.size);
							const retained = previous.get(target), physical = path.join(privateBefore.upper, name);
							privateBefore.files.set(target, retained && sameFilesystemIdentity(retained.stat, stat) && cachedCapture(stat, physical)?.hash === retained.hash
								? retained : await captureStableFile(physical, Number(stat.size), true, { stat }));
						}
					} }).catch(error => { if (!missing(error) || privateBefore.entries.size) throw error; return undefined; });
					const tracePrefix = path.join(traceRoot, "process");
					const image = session.projection.toLogical(executable), [invokedPath, comm] = observedPrototype.invocation!, dispatcher = session.interposition;
					const launchPath = request.imageSource ?? image, logicalCwd = session.projection.toLogical(request.cwd), live = !this.options.completedOnly && !!captureWorkspace && !outlet && !!ready.imageLibrary;
					descriptors = createProcessDescriptorCapture(session, traceRoot, request, live);
					const { inputs, descriptorManifest, descriptorReportPath, descriptorImages, directoryImages, inheritedFiles, outputPipes: unchangedOutputPipes } = descriptors;
					const outputPipes = request.outputPipes?.some(Boolean);
					const streamInputs = inputs.filter(input => ["pipe", "socket", "eventfd"].includes(input.type ?? ""));
					const closedStreams = streamInputs.every(input => {
						const peer = request.resources!.objects[input.image]!.socket?.peer;
						return input.outside === 0 && (!peer?.connected || peer.object !== undefined);
					});
					const resourceJournal = live || inputs.some(input => !input.type || input.type === "eventfd" || input.type === "socket" || input.type === "pipe" && (input.flags & 3) !== 0);
					const streamIdentity = (position: { fd: number; inode: string }) => {
						const input = inputs.find(input => input.fd === position.fd)!;
						return !input.type ? `file:${position.inode}` : input.type === "eventfd" ? `eventfd:${input.image}` : position.inode;
					};
					await descriptors.prepare(resourceJournal);
					const changedInput = async () => {
						const changes = session.workspace.sourceChanges?.();
						if (!changes?.paths.length || !transaction.readBefore) return false;
						let inputs: Set<string> | undefined;
						for (const changed of changes.paths) {
							const relative = relativeFilesystemPath(session.sourceRoot, changed);
							if (relative === undefined || session.deniedPaths.some(denied => pathContains(denied, changed))) continue;
							inputs ??= new Set((await observeStrace(tracePrefix, image, logicalCwd, { launchPath, previewBytes: 1024 * 1024 }))
								.paths.filter(observed => observed.role === "input").map(observed => path.resolve(observed.path)));
							if (!inputs.has(changed)) continue;
							const before = await transaction.readBefore(slash(relative), MAX_REQUEST_BYTES);
							if (!before) continue;
							// Only observed inputs need comparison; the transaction prestate includes predecessor effects.
							await assertNoSymlinkPath(session.sourceRoot, changed);
							const current = await captureStableFile(changed, MAX_REQUEST_BYTES);
							this.add(undefined, "validationFilesRead", current.shared ? 0 : 1);
							this.add(undefined, "validationBytesRead", current.shared ? 0 : current.bytesRead);
							if (sha256Digest(before) === `sha256:${current.hash}`) continue;
							this.setError(undefined, `actor_running_input_changed:${changed}`);
							return true;
						}
						return false;
					};
					const advancing = (advance = async () => live || closedStreams) => this.handoffs.observeRunning(weakKey, work, async () =>
						!await (inputCheck ??= changedInput().catch(() => true).finally(() => { inputCheck = undefined; })) && advance());
					releaseRunning = advancing();
					const imagePath = path.join(traceRoot, "continuation");
					const command = straceCommand(ready.strace, tracePrefix, [
						ready.sandlock,
						// A nested child's own children are brokered as the top level's are: a build tool's compilers run in its trace otherwise.
						...sandboxPolicyArguments(
							logicalCwd,
							[...session.deniedPaths, `${dispatcher}.configuration`],
							[session.workspace.sandboxRoot, session.socketPath, ...(descriptorReportPath ? [descriptorReportPath] : []),
								...[...descriptorImages.values()].filter(image => !image.workspace).map(image => image.physical)],
							[{ virtualPath: session.sourceRoot, hostPath: session.workspace.sandboxRoot, readOnly: false },
								{ virtualPath: path.dirname(dispatcher), hostPath: path.dirname(dispatcher), readOnly: true },
								...(session.gitDirectory ? [{ virtualPath: session.gitDirectory, hostPath: session.gitDirectory, readOnly: true }] : [])],
							[{ virtualPath: "/", hostPath: dispatcher }, { virtualPath: dispatcher, hostPath: dispatcher },
								...(request.imageSource ? [{ virtualPath: launchPath, hostPath: launchPath }] : [])], path.join(session.processRoot, "private"),
						),
						...(session.workspace.metadataImage ? ["--metadata-image", session.workspace.metadataImage] : []),
						...(outlet?.timeout !== undefined ? ["--timeout", String(Math.max(1, Math.ceil(outlet.timeout)))] : []),
						...inheritedFiles.flatMap((_, index) => ["--preserve-fd", String(index + 3)]),
						...directoryImages.flatMap(([directory, image]) => ["--directory-image", sandboxMountArgument({ virtualPath: directory, hostPath: image, readOnly: false })]),
						"--",
						dispatcher,
						descriptorManifest ? "--exec-fds" : request.closeStdin ? "--exec-closed-input" : "--exec",
						outputRoute.join("") + (outputPipes ? request.outputPipes!.map(pipe => pipe ? "p" : "s").join("") : "") +
							(request.stackLimit !== undefined ? `:${request.stackLimit}` : "") + (request.outputFlags ? `,${request.outputFlags.join(",")}` : ""),
						...(descriptorManifest ? [descriptorManifest, descriptorReportPath!] : []),
						request.argv0,
						launchPath,
						...request.args,
					], resourceJournal, live);
					const processStarted = performance.now();
					session.writers.add(writer = { id: `${session.token}:${requestID}`, owner: session.token, startedAt: Date.now(), tail: traceTail(tracePrefix, logicalCwd), ...(inPlace !== undefined ? { pid: inPlace } : {}) });
					await shareSessionWriter(session, writer, [tracePrefix, logicalCwd]);
					const clockOffset = Number(process.hrtime.bigint()) / 1e6 - performance.now();
					stage = "execution";
					let outputEndpoints: readonly [string, string] | undefined;
					const observe = (descriptorOffsets: DescriptorOffset[] | undefined, frozen?: { pid: number; bytes: number }) => observeStrace(tracePrefix, image, logicalCwd, { launchPath, brokeredWrites: pid => brokeredWrites(session, pid, writer), privateUpper: privateUpper(session),
						...(frozen ? { frozen } : {}), ...(outputEndpoints ? { outputEndpoints } : {}),
						unchangedOutputPipes,
						guardFilesystemSemanticsWithin: [session.workspace.sandboxRoot, session.sourceRoot],
						inheritedDirectoryImages: directoryImages.flatMap(([physical]) => [physical, session.projection.toLogical(physical)]),
						inheritedFileImages: [...descriptorImages.values()].flatMap(image => [image.logical, image.physical])
							.concat(inputs.some(input => input.type === "null") ? ["/dev/null"] : [])
							.concat((descriptorOffsets ?? []).filter(position => inputs.find(input => input.fd === position.fd)!.type === "pipe").map(position => `pipe:[${position.inode}]`)),
						inheritedStreams: resourceJournal ? descriptorOffsets?.filter(position => streamInputs.some(input => input.fd === position.fd)).map(streamIdentity) : undefined,
						inheritedHandles: resourceJournal ? descriptorOffsets?.flatMap(position => {
							const input = inputs.find(input => input.fd === position.fd)!, object = request.resources!.objects[input.image]!;
							return [{ fd: input.fd, installed: input.installed, description: input.alias, inode: streamIdentity(position), flags: input.flags, outside: input.outside ?? 3, packet: !!object.socket && (object.socket.type ?? 1) !== 1,
								queuedBytes: object.queue?.bytes, ...(object.queue && input.fd === input.image ? { queueData: Buffer.from(object.content!, "base64"), messages: object.queue.messages } : {}) }];
						}) : undefined,
					});
					const readImage = async () => (await captureStableFile(imagePath, MAX_CONTINUATION_BYTES, true, { digest: false })).content!;
					const parseFrontier = (pid: number, reply: Buffer) => {
						const bytes = Number(reply.readBigUInt64LE(16)), begin = Number(reply.readBigUInt64LE(24)) / 1e6 - clockOffset,
							end = Number(reply.readBigUInt64LE(32)) / 1e6 - clockOffset, fd = reply.readInt32LE(4), syscall = reply.readInt32LE(8);
						if (reply.readInt32LE(0) !== pid || ![1, 2, 5, 6].includes(reply.readInt32LE(12)) || syscall < 0 || syscall >= 0x40000000 || !isSafeCount(bytes, 1) || fd < -1 ||
							begin < processStarted || end < begin || end > performance.now()) throw new Error("invalid native continuation frontier");
						return { frozen: { pid, fd, writing: [1, 20, 44, 46].includes(syscall), bytes }, computation: new TimelineInterval(begin, end) };
					};
					outcome = await runSpawn([ready.strace, `--process-library=${ready.processLibrary}`, `--exec-context-source=${dispatcher}`, `--exec-context-name=${invokedPath}`, `--exec-context-comm=${comm}`, ...(live ? [`--handoff-fd=${inheritedFiles.length + 3}`, `--handoff-image=${imagePath}`] : []), ...command.slice(1)], {
						// The child writes to (and may query) these sockets; their identity lets its observation recognize them.
						tracers: session.tracers, outputRoot: traceRoot, onOutputEndpoints: (endpoints) => {
							session.nestedOutputEndpoints.add(outputEndpoints = endpoints);
							if (captureWorkspace) session.topLevelOutputEndpoints = endpoints;
						},
						cwd: request.cwd,
						environment: request.environment,
						signal: AbortSignal.any([session.signal, work.signal, ...(outlet?.signal ? [outlet.signal] : [])]),
						onData: outlet?.onData, timeout: outlet?.timeout,
						...(outlet && session.invocation?.commandTransport === "stdin" ? { stdin: Buffer.from(outlet.command, "utf8") } : {}),
						inheritedFiles: inheritedFiles.map(file => file.fd),
						...(live ? { onControl: (channel: import("node:stream").Duplex, wake: () => boolean) => {
							let suspended: Promise<boolean> | undefined, demanded = false;
							const suspend = () => suspended ??= (async () => {
								const executionSignal = AbortSignal.any([session.signal, work.signal]);
								const frontier = await this.requestProcessImageAtFrontier(descriptorReportPath!, channel, wake, executionSignal, async (pid, reply, report) => {
									if (!demanded && !(reply.readInt32LE(12) & 4)) return undefined;
									const { frozen } = parseFrontier(pid, reply), positions = parseDescriptorOffsets(report, inputs);
									let observation: StraceObservation | undefined;
									try {
										observation = await observe(positions, frozen);
										if (observation.complete) {
											bindProcessDescriptors(inputs, positions, observation, { image: await readImage(), frontier: frozen, closedInput: !!request.closeStdin });
											return true;
										}
									} catch {}
									// Stop capture at a blocked stream; its ownership separately decides whether to await completion.
									const description = observation?.finalHandles?.find(handle => handle.fd === frozen.fd)?.description;
									return demanded && reply.readInt32LE(12) & 4 && streamInputs.some(input => input.alias === description) ? false : undefined;
								}, () => demanded);
								if (!frontier) return true; // It completed while the request was advancing.
								const { pid, reply } = frontier;
								if (reply.readInt32LE(0) === 0) return closedStreams;
								suspensionAttempted = true;
								if (reply.readInt32LE(0) !== pid) return false;
								const captured = parseFrontier(pid, reply);
								continuation = { image: await readImage(), physicalRoot: session.workspace.sandboxRoot, computation: captured.computation };
								frozen = captured.frozen;
								return true;
							})().catch(error => { this.setError(session, `process_suspend:${errorMessage(error)}`); return false; });
							releaseRunning = advancing(() => { demanded = true; if (suspended) wake(); return suspend(); });
							// Seal waiting prefixes while the owner can publish; a synchronous consumer cannot drive that publication.
							if (!closedStreams) void suspend();
							return () => { releaseRunning?.(); return suspended?.then(() => {}); };
						} } : {}),
					}).finally(() => { writer!.endedAt = Date.now(); });
					if (outputEndpoints) session.nestedOutputEndpoints.delete(outputEndpoints);
					// Replays write each event to the target's own descriptor, whichever outlet this route gave it.
					outcome = { ...outcome, output: outcome.output.map(event => ({ ...event, fd: outputRoute[0] === event.fd ? 1 : 2 })) };
					const observedProcessMs = continuation ? continuation.computation.completedAt - continuation.computation.startedAt : Math.max(0, performance.now() - processStarted);
					releaseRunning();
					try {
						if (suspensionAttempted && !continuation) throw new Error("private process suspension was not sealed");
						const descriptorOffsets = await descriptors.readPositions();
						transactionFinishing = true;
						const observing = observe(descriptorOffsets, frozen);
						const roots = [session.workspace.sandboxRoot, session.sourceRoot], own = writer!;
						const named = (target: string) => roots.flatMap(root => pathContains(root, target) ? [slash(path.relative(root, target))] : [])[0];
						const ownership = () => observing.then(async observation => {
							// When it last looked at each workspace name; one the trace cannot place counts as seen at its end.
							const looked = new Map<string, number>();
							for (const [target, at] of (await own.tail!.read()).seen) { const name = named(target); if (name !== undefined) looked.set(name, Math.max(looked.get(name) ?? 0, at)); }
							// A run brokered from its own tree, which its trace holds as a launcher, wrote on its behalf.
							const traced = new Set(observation.pids);
							const children = sessionWriters(session).filter(other => other !== own && other.pid !== undefined && traced.has(other.pid));
							const descendants = own.descendants = [...new Set(children.flatMap(child => [child, ...child.descendants ?? []]))];
							// Inherited writable workspace objects can mutate every alias without a pathname syscall.
							own.written = [...observation.written ?? [], ...inputs.flatMap(input => !input.type && (input.flags & 3) !== 0 && input.sourcePath
								? [input.sourcePath, ...input.sourceAliases ?? []].flatMap(target => named(target) ?? []) : [])];
							own.observed = new Map(observation.paths.flatMap(({ path: target }) => named(target) ?? []).map(name => [name, looked.get(name) ?? own.endedAt!] as const));
							return writerOwnership(session, [own, ...descendants]);
						});
						const captures = [transaction.finish(ownership), observing] as const;
						const [delta, observation] = await Promise.all(captures).catch(async (error: unknown) => {
							// Both captures own live workspace/trace resources until they settle.
							stage = (await Promise.allSettled(captures)).flatMap((result, index) =>
								result.status === "rejected" ? [index === 0 ? "transaction_capture" : "trace_capture"] : []).join("+");
							throw error;
						});
						own.written ??= observation.written ?? []; own.external = observation.external ?? [];
						const boundImage = bindProcessDescriptors(inputs, descriptorOffsets ?? [], observation, continuation && {
							image: continuation.image, frontier: frozen!, closedInput: !!request.closeStdin });
						if (continuation) continuation = { ...continuation, image: boundImage! };
						if (observation.incompleteReasons.length) {
							this.setError(session, `trace:${observation.incompleteReasons.join(",")}`);
							for (const reason of observation.incompleteReasons) session.incompleteReasons.add(`nested_trace:${reason}`);
						}
						if (!delta.complete) {
							stage = "transaction_capture";
							unattributed = observation;
							throw new Error(`workspace transaction is incomplete: ${delta.reason}`);
						}
						const { before, after } = delta;
						stage = "workspace_effects";
						const effects = diffWorkspaceStructures(before, after, delta.changes, session.projection);
						if (!effects.complete) unattributed = observation;
						stage = "dependencies";
						const observed = [observation, ...own.descendants?.flatMap(child => child.observation ?? []) ?? []];
						const external = await privateCommits(session, own.startedAt, [own, ...own.descendants ?? []], privateBefore);
						const evidence = await captureDependencies(session, before, observed, effects, external, own.descendants?.flatMap(child => child.evidence ?? []), privateBefore);
						if (evidence.incompleteReasons.length) this.setError(session, `evidence:${evidence.incompleteReasons.join(",")}`);
						const taints = new Set<ProvenanceTaint>(evidence.taints);
						// Private images preserve FD/OFD relations, but cannot also represent an independently accessed pathname.
						if (inputs.some(descriptor => !descriptorImages.get(descriptor.image)?.workspace && descriptor.sourcePath && observation.paths.some(observed =>
							path.resolve(observed.path) === descriptor.sourcePath || observed.role !== "metadata" && pathContains(observed.path, descriptor.sourcePath!)))) {
							taints.add("untracked_fd");
						}
						dependencyCertificate = {
							complete: evidence.complete,
							dependencies: evidence.dependencies,
							taints: [...taints],
						};
						stage = "artifacts";
						await this.store.withPublication(async () => {
							const journal: OrderedEffectEvent[] = [];
							for (const { logicalPath, change } of [...effects.effects, ...external.changes.map(change => ({ logicalPath: slash(change.target), change }))]) {
								const state = async (side: "before" | "after"): Promise<WorkspaceEffectState> => {
									if (change.kind === "directory") return change[side] ? { kind: "directory", ...directoryState(change[side]!) } : { kind: "absent" };
									const content = change[side], mode = change[side === "before" ? "beforeMode" : "afterMode"];
									if (content === undefined) return { kind: "absent" };
									if (mode === undefined) throw new Error("transaction file mode is unavailable");
									return { kind: "file", data: await publishContent(content, this.store), mode, ...(side === "after" && change.afterModified ? { modified: change.afterModified } : {}) };
								};
								journal.push({ sequence: journal.length, kind: "workspace", path: logicalPath, before: await state("before"), after: await state("after"),
									...(change.kind !== "directory" ? projectFileMutation(change) : {}) });
							}
							for (const event of outcome!.output) {
								journal.push({ sequence: journal.length, kind: "output", fd: event.fd, data: await this.store.artifacts.put(event.data) });
							}
							await descriptors!.captureFileEffects(this.store, descriptorOffsets, after, effects.complete, journal);
							const transitions: NonNullable<import("./provenance-certificate.ts").ProcessResourceEffects["transitions"]>[number][] = [];
							for (const event of observation.resourceJournal ?? []) {
								const input = inputs.find(input => streamIdentity(descriptorOffsets!.find(position => position.fd === input.fd)!) === event.inode &&
									(event.description !== undefined ? input.alias === event.description : event.kind === "produce" ? (input.flags & 3) !== 0 :
										!["consume", "peek"].includes(event.kind) || (input.flags & 3) !== 1));
								if (!input) throw new Error("unbound stream transition");
								executedStreams.push({ alias: input.alias, kind: event.kind, data: event.data });
								transitions.push({ id: input.alias, kind: event.kind, data: await this.store.artifacts.put(event.data), ...(event.requested !== undefined ? { requested: event.requested } : {}) });
							}
							// A transferred prefix includes producing its proof and sealed image; the separate native frontier is unchanged.
							const computation = TimelineInterval.current(work.startedAt, performance.now(), [
								...(preparation ? [{ computation: preparation, owned: true, shared: [preparation] }] : []),
								...(session.preparation ? [session.preparation] : []), ...session.computations.filter(input => {
									const pid = processDependencyPIDs.get(input);
									return pid !== undefined && observation.pids?.includes(pid);
								}),
							]);
							TimelineInterval.group(computation, session);
							recordProcessDependency(session, { computation, owned: true, shared: [computation] }, inPlace);
							const result: ProcessResultRecord = { replayProfile: "buffered_noninteractive", observedProcessMs, journal, ...(continuation ? { continuation: { imageDigest: sha256Digest(continuation.image), imageBytes: continuation.image.length } } : { exit: exitOutcome(outcome!) }),
								computation: TimelineInterval.serialize(computation),
								...(descriptorOffsets ? { resources: { ...descriptorEffects(request.resources!, descriptorOffsets), ...(resourceJournal ? { transitions } : {}) } } : {}) };
							stage = "certificate";
							const certificate = sealProcessCertificate({ prototype: observedPrototype, producer: outlet ? session.producer : session.nestedProducer, dependencyCertificate: dependencyCertificate!, result });
							const transferable = certificateReplayable(certificate, [...TRANSFERRED_INPUT_TAINTS, ...SAME_CONFINEMENT_TAINTS]);
							certificateID = certificate.id;
							// A child can read an earlier child's private output. Its parent can prove that input from the combined
							// transaction even when this child, considered alone against the host, could not.
							if (!evidence.sourceComplete && observation.complete && external.complete && !continuation) own.observation = observation;
							else own.evidence = certificate.dependencyCertificate;
							if (taints.size) {
								if (!outlet) this.add(session, "tainted");
								// Transferable input categories must not hide existing failures. Diagnostics never change sealed evidence.
								const reasons = (stage: string, values: readonly string[]) => values.slice(0, 4).map(value => `${stage}:${value}`)
									.concat(values.length > 4 ? [`${stage}:more_reasons`] : []);
								const detail = [`tainted:${[...taints].join(",")}`,
									...(observation.unsupportedSyscalls?.length ? [`syscalls:${observation.unsupportedSyscalls.join(",")}${observation.unsupportedSyscallsTruncated ? ",…" : ""}`] : []),
									...reasons("trace", observation.incompleteReasons), ...reasons("dependency", evidence.incompleteReasons)].join("; ");
								if (!session.metrics.lastError || !outlet && !transferable) this.setError(session, detail);
							}
							stage = "handoff_registration";
							if (await this.handoffs.publish(weakKey, work, certificate, () => {
								const { streams: _streams, imageSource: _imageSource, ...invocation } = request;
								const binding = !outlet && this.handoffs.bind(weakKey, work, { ...invocation, cwd: logicalCwd, executable: image,
									sourceRoot: session.sourceRoot, outputRoute, producer: session.nestedProducer });
								if (binding) session.executionBindings.set(requestID, binding);
								if (continuation) return Promise.resolve(false);
								stage = "history_publication";
								return this.planner.publishCompleted(certificate, SAME_CONFINEMENT_TAINTS).catch((error: unknown) => {
									// Optional history storage cannot invalidate already sealed execution evidence.
									this.setError(session, `nested_publish:${failureDetail(error)}`);
									return false;
								});
							}, continuation, computation)) this.add(session, outlet ? "wholeCommandPublished" : "published");
							if (!outlet) {
								// A sealed frontier or input seed remains useful even when its result cannot enter history.
								reusable = !!certificate.result.continuation || transferable;
								if (reusable || request.resources || inputs.length) session.preparedResults++;
							}
						});
					} catch (error) {
						// The process already ran. Certificate failure must never cause dispatcher fallback/re-execution.
						const detail = failureDetail(error);
						if (!outlet || !session.incompleteReasons.size) this.setError(session, `post_execution_capture:${detail}`);
						// Its command captures the effects whole: what it observed joins the command's evidence, measured from the command's start.
						if (unattributed?.complete && !unattributed.incompleteReasons.length && !continuation) writer!.observation = unattributed;
						else session.incompleteReasons.add(`nested_capture:${detail}`);
					}
					if (continuation) return { kind: "suspended" };
					const exit = exitOutcome(outcome);
					const streams = request.streams && streamSettlement(inputs, executedStreams);
					if (streams === undefined && request.streams) {
						this.setError(session, "stream_settlement_unrepresentable");
						return { kind: "executed", output: [], exit: { kind: "code", code: 125 } };
					}
					return { kind: "executed", output: outcome.output, exit, ...(streams?.length ? { streams } : {}),
						...(reusable !== undefined ? { reusable } : {}) };
				} catch (error) {
					if (stage !== "capture" || captureWorkspace || session.signal.aborted || work.signal.aborted) throw error;
					this.setError(session, failureDetail(error), "bypasses");
					return { kind: "bypass" };
				} finally {
					try { await descriptors?.close(); } catch (error) { this.setError(session, `descriptor_report_close:${errorMessage(error)}`); }
					releaseRunning?.();
					await inputCheck;
					const durationMs = Math.max(0, performance.now() - started);
					if (!outlet) this.add(session, "executionMs", durationMs);
					if (!transactionFinishing) await transaction.abort().catch(() => undefined);
					if (writer) {
						writer.endedAt ??= Date.now();
						writer.writes ??= await writer.tail!.read().then(tail => { writer!.unsafe = tail.unsafe; return tail.writes; },
							() => [{ at: writer!.startedAt, opened: true }]);
						await shareSessionWriter(session, writer).catch(error => { session.incompleteReasons.add(`writer_capture:${errorMessage(error)}`); });
						writer.tail = undefined;
					}
					if (traceRoot) await rm(traceRoot, { recursive: true, force: true }).catch(() => undefined);
				}
			} finally {
				this.handoffs.complete(weakKey, acquired.work);
			}
		}, inPlace);
	}

	private async plan(
		weakKey: Sha256Digest,
		executablePath: string, projection: ExecutionPathProjection, acceptProducer: (producer: ProcessProducerProof) => boolean,
		{ session, live, artifacts, excludedCertificates, continuation = false, signal = session?.signal }: {
			session?: ActiveSession; live?: readonly ProcessProvenanceCertificate[]; artifacts?: ReadonlyMap<Sha256Digest, VerifiedArtifactClosure>; excludedCertificates?: ReadonlySet<Sha256Digest>; continuation?: boolean; signal?: AbortSignal;
		} = {},
	): Promise<ReadyProcessPlan | undefined> {
		await using privateReads = session ? FilesystemReadView.ambient() : undefined;
		// Proof reads belong to the caller's excluded lookup interval, even when file capture emits owned receipts.
		const plan = (await TimelineInterval.collect(() => this.planner.plan({ weakKey, executablePath, acceptProducer, excludedCertificates, signal,
			contract: { sink: "buffered", orderedJournal: true, transactionalEffects: true, ...(continuation ? { continuation: true as const } : {}) },
			validation: { resolvePath: (logicalPath) => projection.toPhysical(logicalPath), ...(session ? { acceptedTaints: SAME_CONFINEMENT_TAINTS,
				readView: (physical: string) => projection.isWorkspacePhysical(physical) ? privateReads : undefined } : {}) },
			...(live ? { live: { certificate: live, artifacts, acceptedTaints: [...TRANSFERRED_INPUT_TAINTS] } } : {}),
		}))).output;
		for (const [metric, value] of [["validationCandidates", plan.lookup.candidateCertificates],
			["validationPathsets", plan.lookup.pathsetsValidated], ["validationFilesRead", plan.lookup.filesRead], ["validationBytesRead", plan.lookup.bytesRead],
			["validationArtifactsLoaded", plan.lookup.artifactsLoaded], ["validationArtifactBytesRead", plan.lookup.artifactBytesRead]] as const) {
			this.add(session, metric, value); }
		if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
			const detail = `reuse_miss:${plan.reasons.join(",")}${plan.changedDependencies?.length ? `:${plan.changedDependencies.join(",")}` : ""}`;
			if (session) this.setError(session, detail); else this.setError(undefined, `actor_${detail}`);
		}
		return plan.kind !== "miss" ? plan : undefined;
	}

	private async decideHeldExec(process: HeldExecProcess, scope?: ExecutionScope): Promise<HeldExecDecision> {
		const observation = this.observations.getStore();
		let reserved = false, completing = false;
		const releaseLearning = () => { if (reserved) { observation!.pending--; reserved = false; } };
		const measured = (decision: HeldExecDecision): HeldExecDecision => {
			if (decision.kind === "replay" && observation?.claims) {
				const settle = decision.settle, claims = observation.claims; let pending = true;
				Atomics.add(claims, 0, 1);
				decision = { ...decision, settle: consumed => {
					if (pending) { pending = false; if (!consumed) Atomics.sub(claims, 0, 1); settle?.(consumed); }
				} };
			}
			if (reserved && decision.kind === "continue" && decision.observeCompletion) {
				const complete = decision.observeCompletion; completing = true;
				decision = { ...decision, observeCompletion: async (...args) => { try { await complete(...args); } finally { releaseLearning(); } } };
			}
			decision = decision.kind === "continue" ? { ...decision, observeCompletion: decision.observeCompletion && AsyncLocalStorage.bind(decision.observeCompletion) }
				: { ...decision, commit: AsyncLocalStorage.bind(decision.commit), adopted: decision.adopted && AsyncLocalStorage.bind(decision.adopted),
					settle: decision.settle && AsyncLocalStorage.bind(decision.settle) };
			return !observation || observation.closed || !sameScope(observation.scope, scope)
			? decision : { ...decision, observeTiming: timing => { if (!observation.closed) observation.heldClock.record(timing); } };
		};
		const learning = observation?.learn && !observation.closed && sameScope(observation.scope, scope) && observation.pending < LEARNED_LAUNCHES;
		const order = observation ? Atomics.add(observation.ordering, 0, 1) + 1 : 0;
		let settle: ((consumed: boolean) => void) | undefined, transferred = false;
		this.add(undefined, "requests");
		try {
			process.signal?.throwIfAborted();
			const sourceRoot = path.resolve(process.sourceRoot);
			const installed = `/proc/${process.pid}/exe`, name = await readlink(installed);
			const executable = (await stat(installed)).nlink ? name : name.replace(/ \(deleted\)$/, "");
			const projection = new ExecutionPathProjection({ sourceRoot, workspaceRoot: sourceRoot });
			const executablePath = projection.toLogical(executable);
			if (learning && process.trackQueues) {
				// An acquisition hint lives in the existing bounded binding owner, never in a prediction or result cache.
				this.handoffs.observe(sha256Digest(`queue-tracking:${sourceRoot}`), executablePath, scope!, { trackingOnly: true, sourceRoot }, 0);
				this.add(undefined, "misses"); return measured({ kind: "continue" });
			}
			if (learning) { observation!.pending++; reserved = true; }
			const available = observation?.reuseEnabled !== false && (observation?.source && Atomics.load(observation.source.available, 0) ||
				this.handoffs.mayHaveExecutable(executablePath) || await this.store.mayHaveCertificates(executablePath, process.signal) || this.handoffs.mayHaveExecutable(executablePath));
			// Concurrent discovery is bounded, but a full retained set never shuts discovery off for the rest of the call.
			if (!learning && !available) { this.add(undefined, "misses"); return measured({ kind: "continue" }); }
			const inspected = await inspectHeldExecProcess(process.pid, executable, process.descriptors);
			const capturedInputs = await TimelineInterval.collect(() => observation?.reuseEnabled !== false && process.descriptors?.length
				? captureHeldDescriptorInputs(process.pid, process.descriptors, Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes),
					sensitivePaths(this.options.storeRoot, this.options.deniedPaths), observation?.closed ? undefined : observation?.inputs, process.holderPid ?? process.tracerPid, sourceRoot) : undefined);
			const resources = capturedInputs.output;
			const snapshot = { ...inspected, ...(resources ? { resources } : {}) };
			if (!pathContains(sourceRoot, snapshot.cwd)) { this.setError(undefined, "actor_bypass:cwd_outside_workspace", "bypasses"); return measured({ kind: "continue" }); }
			// Completion admits the most valuable launches; existing adoptions keep their bindings and computation receipts.
			const slot = (durationMs: number, key?: Sha256Digest, create?: Parameters<typeof nativeLearningSlot>[4]): boolean => {
				if (!learning || observation!.closed || process.signal?.aborted || !snapshot.outputRoute || !Number.isFinite(durationMs)) return false;
				return nativeLearningSlot(observation!, durationMs, key, this.store.limits.maxCertificates, create);
			};
			const observe = (prototype: ExecPrototype, durationMs: number) => {
				const weakKey = processWeakKey(prototype), { argv, context: _context, ...invocation } = snapshot;
				return slot(durationMs, weakKey, () => ({ order, binding: this.handoffs.observe(weakKey, executablePath, scope!, {
					...invocation, argv0: argv[0]!, args: argv.slice(1),
					cwd: projection.toLogical(snapshot.cwd), executable: executablePath, sourceRoot, outputRoute: snapshot.outputRoute!,
				}, durationMs) }));
			};
			if (!available || resources && process.descriptors!.some(({ owned }) => !owned)) {
				// Pin the executable's object now, digest it only after admission. Its original identity must still hold:
				// the pathname can be replaced after exec, and the pinned inode can be rewritten after the process exits.
				const image = await open(`/proc/${process.pid}/exe`, 0x200000);
				let before: Awaited<ReturnType<typeof image.stat>>;
				try { before = await image.stat({ bigint: true }); } catch (error) { await image.close(); throw error; }
				this.add(undefined, "misses");
				return measured({ kind: "continue", observeCompletion: async durationMs => {
					try {
						if (durationMs === undefined || slot(durationMs) === false || !sameFilesystemIdentity(before, await image.stat({ bigint: true }))) return;
						const digest = (await TimelineInterval.collect(() => hashExecutableFile(`/proc/self/fd/${image.fd}`, process.signal))).output;
						if (sameFilesystemIdentity(before, await image.stat({ bigint: true })))
							observe(bufferedProcessPrototype(snapshot, projection, digest, await this.resolvePlatformFingerprint()), durationMs);
					} finally { await image.close(); }
				} });
			}
			const digest = (await TimelineInterval.collect(() => hashExecutableFile(`/proc/${process.pid}/exe`, process.signal))).output;
			const prototype = bufferedProcessPrototype(snapshot, projection, digest, await this.resolvePlatformFingerprint());
			const weakKey = processWeakKey(prototype);
			const accepted = (producer: ProcessProducerProof) => actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
			const result = await this.handoffs.acquire({ key: weakKey, scope, signal: process.signal, borrow: observation?.source?.borrow,
				lookup: (live, excludedCertificates, artifacts) => this.plan(weakKey, executablePath, projection, accepted, { live, artifacts, excludedCertificates, continuation: true, signal: process.signal }) });
			const acquired = result.kind === "hit" ? result : undefined, { plan, continuation } = acquired ?? {};
			settle = acquired?.settle;
			if (!acquired || !plan || (plan.certificate.result.continuation ? !continuation || sha256Digest(continuation.image) !== plan.certificate.result.continuation.imageDigest :
				plan.certificate.result.exit.kind !== "code")) {
				this.add(undefined, "misses");
				return measured({ kind: "continue", ...(learning ? { observeCompletion: (durationMs: number | undefined) => {
					if (durationMs !== undefined) observe(prototype, durationMs);
				} } : {}) });
			}
			const output = loadOutputEvents(plan.artifacts, plan.certificate.result.journal), computation = reusedComputation(plan.certificate, acquired);
			const decision = measured({
				kind: "replay",
				settle,
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
					try {
						process.signal?.throwIfAborted();
						await replayFilesystemEffects(observation?.effects ?? this.replayWorkspace, this.store, plan.artifacts, plan.certificate.result.journal, projection);
					} catch (error) {
						this.setError(undefined, `actor_child_commit:${errorMessage(error)}`);
						throw error;
					}
				},
				adopted: () => {
					const binding = acquired.producer?.binding;
					if (observation && !observation.closed && sameScope(observation.scope, scope)) {
						if (binding && this.handoffs.resolveBinding(binding, scope) && observation.bindings.size < this.store.limits.maxCertificates)
							observation.bindings.set(order, binding);
						// Native acknowledgement consumes borrowed inputs; lookup alone is validation.
						observation.computations.push(...capturedInputs.dependencies.filter(input => !input.owned), computation);
					}
					this.recordHit(acquired.producer?.scope, acquired.joined, undefined, scope);
					this.add(undefined, "reusedProcessMs", plan.certificate.result.observedProcessMs ?? 0);
					if (scope) acquired.producer?.ownership.adopted({ scope, id: process.id, sequence: order, backend: "linux_process_reuse", operationIdentity: weakKey,
						executionMs: plan.certificate.result.observedProcessMs ?? 0, computation });
				},
			});
			transferred = true;
			return decision;
		} catch (error) {
			this.setError(undefined, `actor_child:${errorMessage(error)}`, "bypasses");
			return measured({ kind: "continue" });
		} finally { if (!transferred) settle?.(false); if (!completing) releaseLearning(); }
	}

	/** Admission observes a stopped process; safe declines keep advancing its existing request. */
	private async requestProcessImageAtFrontier(descriptorReportPath: string, channel: import("node:stream").Duplex,
		wake: () => boolean, signal: AbortSignal, admit: (pid: number, reply: Buffer, report: Buffer) => Promise<boolean | undefined>, demanded: () => boolean): Promise<{ pid: number; reply: Buffer } | undefined> {
		while (!signal.aborted && !channel.destroyed) {
			const report = await readFile(descriptorReportPath).catch(() => Buffer.alloc(0)), ready = /\nRUNNING (\d+)\n$/.exec(report.toString("ascii"));
			if (ready) {
				const pid = Number(ready[1]);
				const replies = on(channel, "data", { signal, close: ["close"] }), request = Buffer.alloc(4);
				const send = (choice: number) => { request.writeInt32LE(choice); return writeProcessChannel(channel, request); };
				let reply = Buffer.alloc(0);
				try {
					await send(demanded() ? pid : -pid); if (!wake()) throw new Error("continuation producer closed");
					for await (const [bytes] of replies) {
						reply = Buffer.concat([reply, bytes]);
						if (reply.length < 40) continue;
						if (reply.length !== 40) throw new Error("invalid continuation reply");
						if ((reply.readInt32LE(12) & 3) !== 2) return { pid, reply };
						const accepted = await admit(pid, reply, report.subarray(0, ready.index + 1)); signal.throwIfAborted();
						reply = Buffer.alloc(0); await send(accepted === undefined ? 0 : accepted ? pid : -pid);
					}
				} catch (error) { channel.destroy(); throw error; }
				finally { await replies.return?.(); }
				return;
			}
			if (!signal.aborted) await delay(10, undefined, { signal }).catch(() => undefined);
		}
	}

	private add(session: ActiveSession | undefined, metric: CountedReuseMetric, value = 1): void {
		if (session) this.counters[metric] += value; (session?.metrics ?? this.actorCounters)[metric] += value;
		const observation = !session && this.observations.getStore(); if (observation && !observation.closed) observation.metrics[metric] += value;
	}

	private recordHit(
		producer: ExecutionScope | undefined,
		joined: boolean,
		session?: ActiveSession,
		scope: ExecutionScope | undefined = session?.scope,
	): void {
		for (const metric of ["hits", ...(joined ? ["joinedHits"] as const : []),
			producer && scope ? sameScope(producer, scope) ? "sameTurnHits" : "crossTurnHits" : "unattributedHits"] as const) this.add(session, metric);
	}

	private setError(session: ActiveSession | undefined, detail: string, countedAs?: CountedReuseMetric): void {
		if (countedAs) this.add(session, countedAs);
		detail = detail.length > 4096 ? `${detail.slice(0, 4095)}…` : detail;
		this.counters.lastError = detail; (session?.metrics ?? this.actorCounters).lastError = detail;
		const observation = !session && this.observations.getStore(); if (observation && !observation.closed) observation.metrics.lastError = detail;
	}

	private async createProcessInterposition(session: ActiveSession, ready: ReadyBackend) {
		const dispatcher = path.join(session.processRoot, "routing", "dispatcher");
		session.signal?.throwIfAborted();
		if (/[\r\n]/.test(session.socketPath + session.token)) throw new Error("dispatcher configuration cannot hold a line break");
		await mkdir(path.dirname(dispatcher)); await copyFile(ready.dispatcher, dispatcher); await chmod(dispatcher, 0o755);
		await writeFile(`${dispatcher}.configuration`, `${session.socketPath}\n${session.token}\n`, { mode: 0o600 });
		return dispatcher;
	}

	/** Retain the learned launch authority, then propose a separate input version shared by scheduling and execution. */
	private async boundRequest(binding: ProcessExecutionBinding, invocation: BoundProcessInvocation, projection: ExecutionPathProjection, signal: AbortSignal) {
		const cwd = projection.toPhysical(invocation.cwd), executable = projection.toPhysical(invocation.executable);
		if (!cwd || !executable) throw new Error("bound process paths are unmapped");
		let request = { ...invocation, cwd }, prototype = await this.prototype(projection, request, executable);
		if (processWeakKey(prototype) !== binding.key) throw new Error("bound process execution context changed");
		if (invocation.resources && reopensProcessInputs(invocation)) {
			request = { ...request, resources: await reopenProcessResources(invocation.resources, projection,
				Math.min(MAX_REQUEST_BYTES / 2, this.store.limits.maxBytes), signal) };
			prototype = await this.prototype(projection, request, executable, prototype.executableDigest);
		}
		return { request, executable, prototype };
	}

	private async prototype(projection: ExecutionPathProjection, request: ProcessArguments, executable: string, executableDigest?: Sha256Digest): Promise<ExecPrototype> {
		const [digest, ready] = await Promise.all([executableDigest ?? hashExecutableFile(request.imageSource ?? executable), this.resolveReady()]);
		return bufferedProcessPrototype({
			...request, executable, argv: [request.argv0, ...request.args],
			context: routedProcessIdentity(ready.executionContext, request, descriptorInputs(request.resources).filter(input => input.installed !== false)),
		}, projection, digest, ready.platformFingerprint);
	}
}

function reopensProcessInputs(invocation: BoundProcessInvocation) {
	return !invocation.producer && Object.values(invocation.resources?.objects ?? {}).every(object => object.type === "null" ||
		(object.type === "regular" || object.type === "directory") && !!object.sourcePath && pathContains(invocation.sourceRoot, object.sourcePath));
}

/** Reopen paths into a new graph. Handles, shared OFDs, flags and cursors remain predictions checked at the held exec. */
async function reopenProcessResources(graph: ProcessResourceGraph, projection: ExecutionPathProjection, remaining: number,
	signal: AbortSignal): Promise<ProcessResourceGraph> {
	const objects: Record<number, ProcessResourceGraph["objects"][number]> = {}, remapped = new Map<number, number>(), images = new Map<string, number>();
	const fileAliases = captureWorkspaceFileAliases(projection.workspaceRoot);
	for (const [key, object] of Object.entries(graph.objects)) {
		signal.throwIfAborted();
		const id = Number(key);
		if (object.type === "null") { objects[id] = object; continue; }
		const logical = object.sourcePath!, physical = projection.toPhysical(logical)!;
		await assertNoSymlinkPath(projection.workspaceRoot, physical);
		const before = await lstat(physical, { bigint: true }), identity = `${before.dev}:${before.ino}`, previous = images.get(identity);
		if (previous !== undefined) { remapped.set(id, previous); continue; }
		let content: Buffer | undefined, aliases: readonly string[] | undefined;
		if (object.type === "regular") {
			const captured = await captureStableFile(physical, remaining, true, undefined, signal);
			if (!sameFilesystemIdentity(before, captured.stat)) throw new Error("reopened input changed during capture");
			content = captured.content!;
			aliases = (await fileAliases(physical, before))?.map(alias => projection.toLogical(alias)).sort();
		} else {
			if (!before.isDirectory()) throw new Error("reopened input type changed");
			if (object.content !== undefined) {
				// Enumeration cookies and inode numbers belong to the logical filesystem, not the private backing tree.
				content = await withFilesystemReadView(undefined, async source => {
					await assertNoSymlinkPath(projection.sourceRoot, logical, (target, info) => captureFilesystemEntry(target, info, source));
					return readFilesystemDirectoryImage(logical, remaining, signal, source);
				});
			}
		}
		if (!sameFilesystemIdentity(before, await lstat(physical, { bigint: true }))) throw new Error("reopened input changed during capture");
		remaining -= content?.length ?? 0;
		if (remaining < 0) throw new Error("reopened inputs exceed capture budget");
		objects[id] = { type: object.type, sourcePath: logical, contentDigest: sha256Digest(content ?? ""),
			...(content ? { content: content.toString("base64") } : {}), ...(aliases ? { sourceAliases: aliases } : {}) };
		images.set(identity, id);
	}
	return { ...graph, objects, descriptions: Object.fromEntries(Object.entries(graph.descriptions).map(([id, description]) =>
		[id, { ...description, object: remapped.get(description.object) ?? description.object }])) };
}

/** Owns inherited descriptor images, their final proofs, and every handle opened during preparation. */
function createProcessDescriptorCapture(session: ActiveSession, traceRoot: string, request: ProcessArguments, capture = false) {
	const inputs = descriptorInputs(request.resources);
	const needsReport = capture || inputs.length > 0 || request.outputPipes?.some(Boolean);
	const descriptorManifest = needsReport ? path.join(traceRoot, "fd-inputs") : undefined;
	const descriptorReportPath = needsReport ? path.join(traceRoot, "fd-offsets") : undefined;
	const descriptorImages = new Map<number, { physical: string; logical: string; workspace: boolean; state: import("node:fs").BigIntStats }>();
	const directoryImages: Array<readonly [string, string]> = [];
	const inheritedFiles: FileHandle[] = [];
	const outputPipes: string[] = [];
	let descriptorReport: FileHandle | undefined;
	return {
		inputs, descriptorManifest, descriptorReportPath, descriptorImages, directoryImages, inheritedFiles, outputPipes,
		async prepare(resourceJournal: boolean): Promise<void> {
			if (!descriptorManifest || !descriptorReportPath) return;
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
				manifest += `${descriptor.fd} ${descriptor.alias} ${descriptor.flags} ${descriptor.offset} ${Buffer.byteLength(image)} ${stream} ${object.queue?.capacity ?? 0} ${object.socket?.shutdown ?? 0} ${object.socket?.peer.shutdown ?? 0} ${object.socket?.peer.object ?? -1} ${descriptor.outside ?? 3} ${Number(descriptor.installed !== false)} ${object.socket ? object.socket.type ?? 1 : 0}\n${image}\n`;
			}
			for (const [image, object] of Object.entries(request.resources?.objects ?? {})) for (const message of object.queue?.messages ?? [])
				manifest += `M ${image} ${message.start} ${message.end} ${message.rights.length} ${message.rights.join(" ")}\n`;
			for (const descriptor of inputs) if (descriptor.fd === descriptor.alias) for (const lock of descriptor.locks ?? [])
				manifest += `L ${descriptor.fd} ${lock.kind} ${lock.type} ${lock.start} ${lock.length}\n`;
			await writeFile(descriptorManifest, manifest, { flag: "wx", mode: 0o600 });
		},
		async readPositions() {
			if (!descriptorReport) return undefined;
			const positions = parseDescriptorOffsets(await descriptorReport.readFile(), inputs, request.outputPipes?.some(Boolean)
				? { pipes: request.outputPipes, flags: request.outputFlags ?? [0, 0], endpoints: outputPipes } : undefined);
			return inputs.length ? positions : undefined;
		},
		async captureFileEffects(store: ProvenanceCertificateStore, descriptorOffsets: DescriptorOffset[] | undefined,
			after: WorkspaceStructureSnapshot, effectsComplete: boolean, journal: readonly OrderedEffectEvent[]): Promise<void> {
			const finalObjects = new Map([...after.entries].flatMap(([name, entry]) => entry.kind === "file" && entry.object ? [[entry.object, name] as const] : []));
			if (descriptorOffsets) for (const position of descriptorOffsets) {
				const input = inputs.find(({ fd }) => fd === position.fd)!;
				if (input.type === "null" || input.type === "pipe" || input.type === "socket" || input.type === "eventfd" || input.fd !== input.image) continue;
				const image = descriptorImages.get(input.image)!;
				const identity = `${image.state.dev}:${image.state.ino}`, reported = `${position.device}:${position.inode}`;
				const exposed = image.workspace ? session.workspace.metadataObjects?.get(identity) : undefined;
				if (exposed && exposed !== reported) throw new Error("inherited FD object changed during execution");
				const finalName = image.workspace && !input.type ? finalObjects.get(exposed ? identity : `${image.state.dev}:${position.inode}`) : undefined;
				if (position.detached) {
					const final = position.detached;
					if (final.mode !== image.state.mode || final.uid !== image.state.uid || final.gid !== image.state.gid ||
						!effectsComplete) throw new Error("detached file object transition is incomplete");
					if (!finalName) {
						if (sha256Digest(final.content) !== input.contentDigest || final.modified !== image.state.mtimeNs) position.content = await store.artifacts.put(final.content);
						continue;
					}
					const event = journal.find(event => event.kind === "workspace" && event.path === session.projection.toLogical(path.join(after.root, finalName)));
					if (sha256Digest(final.content) !== (event?.kind === "workspace" && event.after.kind === "file" ? event.after.data.digest : input.contentDigest)) throw new Error("file object and remaining aliases diverged");
				}
				const physical = finalName ? path.join(after.root, finalName) : image.physical;
				const current = await lstat(physical, { bigint: true });
				// Sandlock exposes the source filesystem's device; the journal names the private backing device.
				let device = current.dev;
				if (image.workspace) {
					let logical = finalName ? session.projection.toLogical(physical) : image.logical;
					for (;;) {
						try { device = (await stat(logical, { bigint: true })).dev; break; }
						catch (error) { if (!missing(error) || path.dirname(logical) === logical) throw error; logical = path.dirname(logical); }
					}
				}
				const currentObject = image.workspace && session.workspace.metadataObjects?.get(`${current.dev}:${current.ino}`) || `${device}:${current.ino}`;
				if ((input.type === "directory" ? !current.isDirectory() : !current.isFile()) || currentObject !== reported || current.dev !== image.state.dev ||
					current.mode !== image.state.mode || current.uid !== image.state.uid || current.gid !== image.state.gid)
					throw new Error("inherited FD namespace changed during execution");
				if (input.type === "directory" && !sameFilesystemIdentity(current, image.state)) throw new Error("inherited directory changed during enumeration");
				// The common workspace object transaction owns every named inode write and namespace edge.
				if (image.workspace && !input.type) {
					if (!finalName || !effectsComplete) throw new Error("inherited file object transition is incomplete");
					continue;
				}
				if (input.type === "directory" || sameFilesystemIdentity(current, image.state)) continue;
				const captured = await captureStableFile(physical, MAX_REQUEST_BYTES, true);
				if (`sha256:${captured.hash}` !== input.contentDigest || current.mtimeNs !== image.state.mtimeNs) {
					position.content = await store.artifacts.put(captured.content!);
				} else throw new Error("unmodeled inherited FD metadata effect");
			}
		},
		async close(): Promise<void> {
			const results = await Promise.allSettled([descriptorReport?.close(), ...inheritedFiles.map(file => file.close())]);
			const failed = results.find(result => result.status === "rejected");
			if (failed?.status === "rejected") throw failed.reason;
		},
	};
}

function bufferedProcessPrototype(
	snapshot: HeldExecSnapshot,
	projection: ExecutionPathProjection,
	executableDigest: Sha256Digest,
	platformFingerprint: string,
) {
	const { context } = snapshot;
	const inputs = descriptorInputs(snapshot.resources);
	const input = inputs.find(({ fd }) => fd === 0);
	return createExecPrototype({
		executablePath: projection.toLogical(snapshot.executable),
		invocation: snapshot.invocation,
		executableDigest,
		argv: snapshot.argv,
		logicalCwd: projection.toLogical(snapshot.cwd),
		environment: snapshot.environment,
		umask: context.umask,
		processContextDigest: sha256Digest(context.key),
		stdin: input ? { type: "bytes", digest: input.contentDigest, eof: snapshot.resources!.objects[input.image]!.queue?.eof ?? true } : { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [...context.descriptorTypes.keys(), ...inputs.filter(input => input.fd > 2).map(input => input.fd)].map(fd => {
			const input = inputs.find(input => input.fd === fd), type = input?.type ?? context.descriptorTypes[fd] ?? "regular";
			if (!input) return { fd, type, flagsDigest: sha256Digest(`${context.key}\0${fd}`), ...(fd === 0 ? { eof: true } : {}) };
			const { queue, socket } = snapshot.resources!.objects[input.image]!;
			return { fd, type, flagsDigest: input.locks || (!input.type || input.type === "eventfd") && input.outside !== undefined ? digestObject({ flags: input.flags, locks: input.locks, outside: input.outside }) : sha256Digest(String(input.flags)),
				...(input.installed === false ? { installed: false as const } : {}), alias: input.alias, object: input.image, contentDigest: input.contentDigest, offset: input.offset, eof: queue?.eof,
				...(queue ? { endpointDigest: digestObject({ queue, socket }) } : {}),
				...(input.sourcePath ? { resourcePath: projection.toLogical(input.sourcePath) } : {}),
				...(input.sourceAliases ? { resourceAliases: input.sourceAliases.map(name => projection.toLogical(name)).sort() } : {}) };
		}),
		platformFingerprint,
	});
}

function bindProcessDescriptors(inputs: ReturnType<typeof descriptorInputs>, positions: DescriptorOffset[], observation: StraceObservation,
	continuation?: { image: Buffer; frontier: { pid: number; fd: number; writing: boolean }; closedInput: boolean }) {
	const handles = observation.finalHandles ?? [];
	// Surviving queued OFDs need a current position; a continuation can prove it through an installed alias.
	if (inputs.some(input => !input.type && input.outside === 0 && observation.retainedDescriptions?.includes(input.alias) &&
		(!continuation || !handles.some(handle => handle.description === input.alias)))) throw new Error("queued file description has no final position observation");
	if (!continuation) return;
	const { image, frontier, closedInput } = continuation;
	let cursor = 0;
	const line = () => {
		const end = image.indexOf(10, cursor);
		if (end < 0 || end - cursor > 256) throw new Error("invalid continuation header");
		const text = image.toString("ascii", cursor, end); cursor = end + 1; return text;
	};
	const first = line(), header = /^PIIMAGE4 (\d+) (\d+) (\d+) (\d+)$/.exec(first);
	if (!header || Number(header[1]) !== frontier.pid || Number(header[2]) > 256) throw new Error("unbound continuation image");
	const records = new Map<number, string[]>(), descriptions = new Map<number, string[]>();
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
		records.set(fd, fields);
	}
	const pending = inputs.find(input => input.fd === Number(records.get(frontier.fd)?.[5]));
	if (frontier.fd >= 0 && !pending && !([1, 2].includes(frontier.fd) && frontier.writing) ||
		[...(closedInput ? [] : [0]), 1, 2, ...handles.map(handle => handle.fd)].some(fd => !records.has(fd)) ||
		cursor + Number(header[3]) + Number(header[4]) !== image.length) throw new Error("incomplete continuation FD table");
	for (const position of positions) {
		const input = inputs.find(input => input.fd === position.fd)!, state = descriptions.get(input.alias);
		if (!state) continue;
		const after = Number(state[1]); position.after = Number.isSafeInteger(after) ? after : state[1]!;
		position.afterFlags = Number(state[0]) !== input.flags ? Number(state[0]) : undefined;
	}
	return Buffer.concat([Buffer.from(`${first}\n${[...records.values()].map(row => row.join(" ")).join("\n")}\n`), image.subarray(cursor)]);
}

interface DescriptorOffset {
	fd: number; before: OFDPosition; after: OFDPosition; device: string; inode: string; afterFlags?: number;
	content?: import("./provenance-certificate.ts").ArtifactReference;
	detached?: { content: Buffer; mode: bigint; uid: bigint; gid: bigint; modified: bigint };
}

function parseDescriptorOffsets(report: Buffer, inputs: ReturnType<typeof descriptorInputs>, outputs?: {
	readonly pipes: readonly [boolean, boolean]; readonly flags: readonly [number, number]; readonly endpoints: string[];
}): DescriptorOffset[] {
	let cursor = 0, bytes = 0;
	const line = () => {
		const end = report.indexOf(10, cursor);
		if (end < 0 || end - cursor > 256) throw new Error("incomplete inherited OFD result");
		const value = report.toString("ascii", cursor, end); cursor = end + 1; return value;
	};
	if (line() !== `OFD ${inputs.length}`) throw new Error("invalid inherited OFD header");
	const positions: DescriptorOffset[] = inputs.map(input => {
		const value = line();
		if (!/^\d+ \d+ \d+ \d+ \d+$/.test(value)) throw new Error("invalid inherited OFD result");
		const fields = value.split(" "), [fd, flags, after] = fields.slice(0, 3).map(Number);
		if (fd !== input.fd) throw new Error("inherited descriptor report changed order");
		return { fd, before: input.offset, after: Number.isSafeInteger(after) ? after! : fields[2]!, ...(flags !== input.flags ? { afterFlags: flags! } : {}), device: fields[3]!, inode: fields[4]! };
	});
	while (cursor < report.length) {
		const value = line();
		const output = /^O ([12]) (\d+) ([1-9]\d{0,19})$/.exec(value);
		if (output) {
			const index = Number(output[1]) - 1;
			if (!outputs?.pipes[index] || outputs.endpoints[index] || BigInt(output[3]!) > 0xffffffffffffffffn ||
				Number(output[2]) !== (1 | outputs.flags[index]! & 0xc00)) throw new Error("output pipe status changed during execution");
			outputs.endpoints[index] = `pipe:[${output[3]}]`;
			continue;
		}
		if (!/^F \d+ \d+ \d+ \d+ -?\d+ \d+ \d+$/.test(value)) throw new Error("invalid detached file image");
		const [, fd, mode, uid, gid, seconds, nanos, length] = value.split(" "), size = Number(length);
		const input = inputs.find(input => input.fd === Number(fd)), position = positions.find(position => position.fd === Number(fd));
		if (!input || input.type || input.fd !== input.image || !position || position.detached || !isSafeCount(size) || (bytes += size) > MAX_REQUEST_BYTES / 2 || cursor + size >= report.length || report[cursor + size] !== 10 || Number(nanos) >= 1e9) throw new Error("unbound detached file image");
		position.detached = { content: report.subarray(cursor, cursor + size), mode: BigInt(mode!), uid: BigInt(uid!), gid: BigInt(gid!), modified: BigInt(seconds!) * 1_000_000_000n + BigInt(nanos!) };
		cursor += size + 1;
	}
	if (outputs?.pipes.some((pipe, index) => pipe && !outputs.endpoints[index])) throw new Error("output pipe status proof missing");
	return positions;
}

function recordProcessDependency(session: ActiveSession, dependency: TimelineDependency, pid?: number) {
	if (pid !== undefined) processDependencyPIDs.set(dependency, pid);
	session.computations.push(dependency);
}

async function measureProcess<T>(session: ActiveSession, execute: () => Promise<T>, pid?: number) {
	const first = session.computations.length;
	const result = await TimelineInterval.measure(execute, () => session.computations.slice(first).filter(input => processDependencyPIDs.get(input) === pid));
	recordProcessDependency(session, { computation: result.computation, owned: true, shared: [result.computation] }, pid);
	return result.output;
}

function nativeLearningSlot(observation: NativeObservation, duration: number, key: string | undefined, limit: number,
	create?: () => { readonly binding: ProcessExecutionBinding | undefined; readonly order: number }) {
	const entries = [...observation.learned], previous = key && entries.find(([order]) => observation.bindings.get(order)?.key === key);
	const victim = previous || (entries.length >= LEARNED_LAUNCHES || observation.bindings.size >= limit
		? entries.reduce<(typeof entries)[number] | undefined>((lowest, entry) => !lowest || entry[1] < lowest[1] ? entry : lowest, undefined) : undefined);
	if (victim ? duration <= victim[1] : observation.bindings.size >= limit) return false;
	const record = create?.();
	if (record?.binding) {
		if (victim) { observation.bindings.delete(victim[0]); observation.learned.delete(victim[0]); }
		observation.bindings.set(record.order, record.binding); observation.learned.set(record.order, duration);
	} return true;
}

/** Optimistic execution boundaries: control wins over sibling overlap; original producers keep their own clocks. */
class ActorHeldClock {
	private readonly spans: HeldExecClock[][] = [[], []];
	record(timing: HeldExecTiming): void {
		for (const [index, clock] of [{ startedAt: timing.requestedAt, completedAt: timing.completedAt }, timing.native].entries()) {
			if (!clock || clock.completedAt <= clock.startedAt) continue;
			const spans = this.spans[index]!;
			let start = clock.startedAt, end = clock.completedAt, left = 0, right = spans.length;
			while (left < right) { const middle = (left + right) >>> 1; if (spans[middle]!.completedAt < start) left = middle + 1; else right = middle; }
			while (right < spans.length && spans[right]!.startedAt <= end) {
				start = Math.min(start, spans[right]!.startedAt); end = Math.max(end, spans[right++]!.completedAt);
			}
			spans.splice(left, right - left, { startedAt: start, completedAt: end });
		}
	}
	dependencies(): TimelineDependency[] {
		const result: TimelineDependency[] = [];
		for (const [index, spans] of this.spans.entries()) if (spans.length) {
			const computation = new TimelineInterval(spans[0]!.startedAt, spans.at(-1)!.completedAt, index ? result : [], index ? spans : undefined);
			result.push({ computation, ...(index ? { owned: true } : { overhead: true }), shared: spans });
		}
		return result;
	}
}

function reusedComputation(certificate: ProcessProvenanceCertificate,
	acquired?: { readonly producer?: ProcessHandoff; readonly waiting?: readonly TimelineInterval[] }): TimelineDependency {
	const computation = acquired?.producer?.computation ?? TimelineInterval.restore(certificate.result.computation) ??
		TimelineInterval.unknownReuse(`${certificate.id}:${certificate.createdAt}`);
	return { computation, shared: acquired?.waiting, reused: true };
}

async function sealSessionEvidence(session: ActiveSession, external: Awaited<ReturnType<typeof privateCommits>>) {
	const capture = await captureSessionInterval(session);
	session.topLevelEvidence = { complete: false, dependencies: [], taints: ["trace_incomplete"] };
	if (!capture.complete) {
		session.incompleteReasons.add(`top_capture:${capture.reason}`);
		return [];
	}
	const effects = diffWorkspaceStructures(capture.before, capture.after, capture.changes, session.projection);
	if (!effects.complete) {
		session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		throw new Error(`top-level workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	}
	const committedChanges = sandboxWorkspaceChanges(session.sourceRoot, effects);
	try {
		const writers = sessionWriters(session), evidence = await captureDependencies(session, capture.before, writers.flatMap(writer => writer.observation ?? []), effects,
			external, writers.flatMap(writer => writer.evidence ?? []));
		for (const reason of evidence.incompleteReasons) session.incompleteReasons.add(`top_evidence:${reason}`);
		session.topLevelEvidence = { ...evidence, complete: evidence.complete && session.incompleteReasons.size === 0 };
	} catch (error) {
		session.incompleteReasons.add(`top_seal:${errorMessage(error)}`);
	}
	return committedChanges;
}

/** binfmt_script's interpreter (the first word after `#!`) or a little-endian ELF64's PT_INTERP. */
export async function imageInterpreter(file: string): Promise<string | undefined> {
	await using handle = await open(file, "r").catch(() => undefined);
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
}

async function captureDependencies(session: ActiveSession, snapshot: WorkspaceStructureSnapshot, observations: readonly StraceObservation[], effects: WorkspaceTransactionDiff,
	external: Awaited<ReturnType<typeof privateCommits>>, runs: readonly DynamicDependencyCertificate[] = [], privateBefore?: PrivatePrestate) {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason}`);
	await using privateReads = FilesystemReadView.ambient();
	const observed = observations.flatMap(part => part.paths),
		locks = observations.flatMap(part => part.locks ?? []), written = observations.flatMap(part => part.written ?? []);
	// Transaction structure and retained write preimages describe the run's starting state.
	const deltas = new Map(effects.effects.flatMap(({ relativePath, change }) => change.kind === "directory" ? [] : [[relativePath, change] as const]));
	const captureBefore = sharedWalk(async (relative: string) => {
		const structure = snapshot.entries.get(relative);
		if (!structure || structure.kind !== "file") return structure;
		const delta = deltas.get(relative), before = delta?.before, takenAtMs = Date.now();
		const captured = before ? undefined : await captureFileDigest(structure.contentPath ?? path.resolve(snapshot.root, relative), structure.size);
		const hydrated = await hydrateWorkspaceFileEntry(structure, before ?? captured!);
		if (!hydrated) throw new Error(`transaction baseline changed: ${relative}`);
		if (delta?.beforeIdentity) rememberCapture({ identity: delta.beforeIdentity, hash: hydrated.digest.slice("sha256:".length) }, takenAtMs);
		return hydrated;
	});
	const before = (physicalPath: string) => {
		const relative = relativeFilesystemPath(snapshot.root, physicalPath);
		return relative === undefined ? Promise.reject(new Error(`workspace dependency escapes snapshot: ${physicalPath}`)) : captureBefore(relative);
	};
	// A name it created itself depends only on having been free, not on everything else its directory held.
	const created = new Set(written.map(name => path.resolve(session.workspace.sandboxRoot, name)));
	const parentWrites = new Set([...created].map(name => path.dirname(name)));
	const exclusions = (target: string) => path.resolve(target) === path.resolve(session.workspace.sandboxRoot) ? session.workspace.observationExcludes : undefined;
	const workspaceDependency = async (physical: string, logical: string, role: DependencyRole, listed = true) => {
		const [entry, parent] = await Promise.all([before(physical),
			path.resolve(physical) === path.resolve(session.workspace.sandboxRoot) || created.has(path.resolve(physical)) ? undefined : before(path.dirname(physical))]);
		return snapshotDependency(logical, entry?.kind === "file" && entry.aliases ? { ...entry,
			aliases: entry.aliases.map(name => session.projection.toLogical(name)).sort() } : entry, parent, role, {
			excludedEntries: exclusions(physical),
			parentExcludedEntries: exclusions(path.dirname(physical)), listed,
		});
	};
	const dependencies = new Map<string, DynamicDependency | undefined>();
	const taints = new Set<ProvenanceTaint>([...observations, ...runs].flatMap(part => part.taints));
	const incompleteReasons = new Set(observations.flatMap(part => part.incompleteReasons));
	let complete = observations.every(part => part.complete);
	if (!complete) taints.add("trace_incomplete");
	const add = (dependency: DynamicDependency | undefined, reason = "dependency_unavailable") => {
		if (!dependency) { incompleteReasons.add(reason); return; }
		const identity = dynamicDependencyIdentity(dependency);
		const existing = dependencies.get(identity);
		if (existing?.kind === "file" && dependency.kind === "file" && stableEqual({ ...existing, role: dependency.role }, dependency)) {
			if (existing.role !== "executable" && dependency.role === "executable") dependencies.set(identity, dependency);
			return;
		}
		// A listing adds evidence to reaching the same directory; neither observation may replace conflicting evidence.
		if (existing?.kind === dependency.kind && (dependency.kind === "directory" || dependency.kind === "absence") && Object.entries(dependency).every(([key, value]) =>
			!(key in existing) || stableEqual(Reflect.get(existing, key), value))) { dependencies.set(identity, { ...existing, ...dependency }); return; }
		if (existing && !stableEqual(existing, dependency)) {
			incompleteReasons.add(`dependency_changed_during_execution:${identity}`);
			return;
		}
		dependencies.set(identity, dependency);
	};
	// Rebase owned inputs; metadata survives unless its private object vanished within this transaction.
	const changed = new Set(effects.effects.map(effect => effect.logicalPath));
	const retainedExternal = new Set(external.changes.filter(change => change.after).map(change => change.target));
	let unmovedPrivateObjects: Promise<boolean> | undefined;
	const privateObjectsUnmoved = () => unmovedPrivateObjects ??= (async () => {
		// A deleted name can still identify a retained output through rename/link. Without a full
		// object mapping, multi-path writes and unresolved writes cannot prove the object vanished.
		for (const writer of sessionWriters(session)) {
			const trace = writer.tail ? await writer.tail.read() : writer;
			if (trace.unsafe || !trace.writes || trace.writes.some(write => !write.paths || write.paths.length > 1)) return false;
		}
		return true;
	})();
	const vanishedPrivateObject = async (target: string) => external.before.get(target)?.kind === "absence" &&
		!retainedExternal.has(target) && await privateObjectsUnmoved();
	for (const target of observations.flatMap(part => part.metadataChanges ?? [])) if (!await vanishedPrivateObject(target)) {
		taints.add("mutable_input"); incompleteReasons.add(`metadata_changed:${target}`);
	}
	const atStart = async (dependency: DynamicDependency) => {
		if (dependency.kind !== "fd" && dependency.kind !== "lock" && external.before.has(dependency.path)) {
			const initial = external.before.get(dependency.path);
			// Only a private object created AND removed by this run can discard its metadata.
			// A retained output's identity must still agree with the object the Actor will see.
			if (dependency.kind !== "metadata" || await vanishedPrivateObject(dependency.path)) return initial;
		}
		const physical = dependency.kind === "fd" ? undefined : session.projection.toPhysical(dependency.path);
		const relative = physical && relativeFilesystemPath(snapshot.root, physical);
		if (dependency.kind === "fd" || !physical || relative === undefined || session.workspace.observationExcludes.includes(relative.split(path.sep)[0]!)) return dependency;
		const entry = snapshot.entries.get(relative), up = relativeFilesystemPath(snapshot.root, path.dirname(physical)), parent = up === undefined ? undefined : snapshot.entries.get(up);
		if (dependency.kind === "metadata" && parentWrites.has(physical) && entry?.kind === "directory" && entry.observed) return { ...dependency,
			digest: filesystemObservationDigest(session.workspace.projectMetadata?.(entry.observed) ?? entry.observed, dependency.fields) };
		const same = dependency.kind === "metadata" ? !!entry || changed.has(dependency.path) || !created.has(physical)
			: dependency.kind === "absence" ? !entry && (dependency.parentEntriesDigest === undefined || parent?.kind === "directory" && parent.entriesDigest === dependency.parentEntriesDigest)
			: dependency.kind === "directory" ? entry?.kind === "directory" && entry.metadataDigest === dependency.metadataDigest && (dependency.entriesDigest ?? entry.entriesDigest) === entry.entriesDigest
			: dependency.kind === "symlink" ? entry?.kind === "symlink" && entry.target === dependency.target
			: dependency.kind === "lock" ? entry?.kind === "file"
			: !changed.has(dependency.path) && entry?.kind === "file" && entry.metadataDigest === dependency.metadataDigest;
		return same ? dependency : workspaceDependency(physical, dependency.path, dependency.kind === "file" ? dependency.role : "input", dependency.kind === "directory" && !!dependency.entriesDigest);
	};

	// Kernel pathname walk over the baseline: links are recorded and expanded in place and `..` leaves the directory
	// actually reached. Sandlock collapses `..` first, so another result means the sandbox read another object.
	const walk = async (logical: string, follow: boolean, absent = false) => {
		const pending = logical.split("/").filter(Boolean), links: string[] = [];
		let current = "/";
		for (let segment = pending.shift(); segment !== undefined; segment = pending.shift()) {
			if (segment === "..") { current = path.posix.dirname(current); continue; }
			let next = path.posix.join(current, segment);
			// The repository is the workspace's own, mounted read-only (also when reached through the overlay): the host walk captures it.
			const repository = session.projection.isWorkspacePhysical(next) ? session.projection.toLogical(next) : next;
			const inRepository = session.gitDirectory && pathContains(session.gitDirectory, repository);
			if (inRepository) next = repository;
			const physical = !inRepository && pathContains(session.sourceRoot, next) ? session.projection.toPhysical(next) : undefined;
			const entry = physical ? await before(physical) : undefined;
			if (entry?.kind === "file" && pending.length) return { path: next, links, blocked: true as const };
			// A failed lookup needs both its original prefixes and the normalized target to retain their observed states.
			if (absent && pending.includes("..") && entry?.kind !== "symlink") {
				const prefix = physical ? await workspaceDependency(physical, next, "input", false)
					: await captureDirectoryDependency(next, next, false, [], false).catch(error => missing(error) ? captureAbsenceDependency(path.join(privateUpper(session), next), next, false, [], undefined, privateReads) : undefined);
				if (prefix?.kind !== "directory" && (prefix?.kind !== "absence" || created.has(physical ?? next) || own.has(next))) return;
				add(prefix);
			}
			if (entry?.kind !== "symlink" || !pending.length && !follow) { current = next; continue; }
			if (links.push(next) > 40) return undefined;
			pending.unshift(...entry.target.split("/").filter(Boolean));
			if (entry.target.startsWith("/")) current = "/";
		}
		return { path: current, links };
	};
	const own = new Set([...external.before.keys()].flatMap(target => [target, path.dirname(target)]));
	// A lock on a file it made itself no one else could have held; on one that was there, another holder would have refused it.
	for (const lock of locks) if (await (session.projection.isWorkspacePhysical(lock.path) ? before(lock.path) : lstat(lock.path).catch(() => undefined))) {
		add({ kind: "lock", path: session.projection.isWorkspacePhysical(lock.path) ? session.projection.toLogical(lock.path) : slash(lock.path), exclusive: lock.exclusive });
	}
	const pending = [...observed], seenImages = new Set<string>(), hostPaths = new Map<string, { physical: string; role: DependencyRole; listed: boolean; absent: boolean }>();
	for (let item = pending.shift(); item; item = pending.shift()) {
		const absent = item.role !== "metadata" && item.lookupFailure === "ENOENT";
		const follow = item.role !== "metadata" || item.followSymlinks, walked = await walk(item.path, follow, absent);
		// A blocking file must reproduce the observed ENOTDIR; every other walk must reach the same object after normalization.
		const normalized = !walked?.blocked && item.path.split("/").includes("..") ? await walk(path.posix.normalize(item.path), follow) : walked;
		if (!walked || walked.blocked && (item.role === "metadata" || item.lookupFailure !== "ENOTDIR") ||
			normalized?.blocked && !walked.blocked || walked.path !== normalized?.path) {
			add(undefined, `pathname_walk:${item.path}`); continue;
		}
		for (const link of item.role === "metadata" ? [] : walked.links) add(await workspaceDependency(session.projection.toPhysical(link)!, link, "input"));
		const observedPath = item.role === "metadata" ? path.resolve(item.path) : walked.path;
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
		if (own.has(physical) && (item.role !== "metadata" || !external.before.has(physical))) continue;
		// A process reading its own state, or the host's CPU and cgroup limits, observes this one run like the clock or its pid.
		if (/^\/proc\/(?:self|thread-self|\d+)(?:\/|$)/.test(observedPath)) { taints.add(observedPath.endsWith("/limits") ? "confinement_observation" : "pid_observation"); continue; }
		// Reopening an inherited descriptor reaches what that descriptor carries, like a jobserver pipe, not a host file.
		if (/^\/dev\/fd\/\d+$/.test(observedPath)) { taints.add("descriptor_observation"); continue; }
		if (/^\/dev\/u?random$/.test(observedPath)) { taints.add("random"); continue; } // Entropy, as getrandom(2) reads it.
		if (/^\/sys\/(?:devices\/system\/cpu|fs\/cgroup)(?:\/|$)|^\/proc\/(?:meminfo|version|version_signature|cpuinfo|stat|loadavg|uptime)$/.test(observedPath)) { taints.add("clock"); continue; }
		if (item.role === "metadata") {
			const { role, path: _path, afterWrite, ...metadata } = item;
			const dependency = { ...metadata, kind: role,
				path: session.projection.isWorkspacePhysical(physical) ? session.projection.toLogical(physical) : slash(physical) };
			const input = await atStart(dependency);
			const identity = dynamicDependencyIdentity(dependency);
			// A missing slot needs these fields bound before the write, possibly by a brokered child.
			if (afterWrite && input?.kind === "metadata") { if (!dependencies.has(identity)) dependencies.set(identity, undefined); }
			else add(input);
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
		const key = `${item.role}\0${physical}`, previous = hostPaths.get(key);
		hostPaths.set(key, { physical, role: item.role, listed: !!item.listed || !!previous?.listed,
			absent: absent && previous?.absent !== false });
	}
	// Capture independent external inputs concurrently, preserving trace order.
	await withFilesystemReadView(undefined, async view => {
		const capture = sharedWalk(target => captureFilesystemEntry(target, undefined, view)), images = await privateInputImages(session, new Set([...hostPaths.values()].map(item => item.physical)), privateBefore);
		for (const captured of await mapFilesystem([...hostPaths.values()], async ({ physical, role, listed, absent }) => {
			try { return { physical, value: images.has(physical) ? images.get(physical)?.map(dependency => dependency.kind === "file" ? { ...dependency, role } : dependency)
				: await captureHostPath(physical, role, listed, absent, capture, view) }; }
			catch (error) { return { physical, error }; }
		})) {
			if ("error" in captured) { taints.add("trace_incomplete"); add(undefined, `capture:${captured.physical}:${errorMessage(captured.error)}`); }
			else if (captured.value) for (const dependency of captured.value) add(dependency);
			else { taints.add("mutable_input"); add(undefined, `mutable:${captured.physical}`); }
		}
	});
	for (const effect of effects.effects) {
		const physical = session.projection.toPhysical(effect.logicalPath);
		if (!physical) { add(undefined, `effect_unmapped:${effect.logicalPath}`); continue; }
		add(await workspaceDependency(physical, effect.logicalPath, "input"));
	}
	for (const [target, dependency] of external.before) add(dependency, `external_write:${target}`);
	const rebased = (await mapFilesystem(runs.flatMap(run => run.dependencies), atStart))
		.flatMap(dependency => dependency ?? (add(undefined, "start_state_unavailable"), []));
	for (const [identity, dependency] of dependencies) if (!dependency && !rebased.some(dependency => dynamicDependencyIdentity(dependency) === identity)) add(undefined, `metadata_after_write:${identity}`);
	const sourceComplete = complete && incompleteReasons.size === 0;
	complete &&= runs.every(run => run.complete);
	for (const dependency of rebased) add(dependency);
	return { complete: complete && incompleteReasons.size === 0, sourceComplete, dependencies: Object.freeze([...dependencies.values()].flatMap(value => value ?? [])),
		taints: Object.freeze([...taints]), incompleteReasons: [...incompleteReasons] };
}

const STABLE_SANDBOX_DEVICES = new Set(["/dev/null", "/dev/tty", "/dev/zero", "/dev/full"]);
const CONTENT_ADDRESSED_GIT = /^objects\/(?:[0-9a-f]{2}\/[0-9a-f]{38}(?:[0-9a-f]{24})?|pack\/pack-[0-9a-f]{40}(?:[0-9a-f]{24})?\.(?:pack|idx|rev|bitmap))$/;
/** Also what memory allocators (jemalloc, glibc) read at startup to size their mappings. */
const KERNEL_CONFIGURATION = /^\/proc\/(?:filesystems|mounts|sys\/vm\/overcommit_memory)$|^\/sys\/kernel\/mm\/transparent_hugepage\//;
const SAME_CONFINEMENT_TAINTS = ["confinement_observation"] as const;

async function captureHostPath(physicalPath: string, role: DependencyRole, listed: boolean, absent: boolean,
	capture: typeof captureFilesystemEntry, view: FilesystemReadView) {
	// A uniformly failed lookup depends on this exact name remaining absent, including under mutable roots.
	if (absent) {
		const dependency = await captureAbsenceDependency(physicalPath, slash(physicalPath), false, [], undefined, view);
		return dependency ? [dependency] : undefined;
	}
	const dependencies: DynamicDependency[] = [];
	for await (const { path: current, info, link, terminal } of walkFilesystemPath(path.resolve(physicalPath), { capture, view })) {
		// What exists under these roots changes without a trace; absence was independently proved above.
		if (["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp"].some((root) => pathContains(root, current)) && (!pathContains("/run", current) || info && terminal)) return undefined;
		if (!info) return undefined;
		// Root's files, and this user's own outside the workspace (a PATH through ~/.local or nvm), validate exactly; no one else may write them.
		if (info.uid !== 0n && info.uid !== BigInt(process.getuid?.() ?? -1) || (link === undefined && (info.mode & 0o022n) !== 0n)) return undefined;
		if (link !== undefined) dependencies.push({ kind: "symlink", path: slash(current), target: link, targetDigest: sha256Digest(link) });
		else if (terminal && info.isFile()) dependencies.push((await captureFileDependency(current, slash(current), role, { includeMetadata: true, view })).dependency);
		// A listing reveals names; a program that stats the directory records that metadata on its own.
		else if (terminal && info.isDirectory()) dependencies.push(await captureDirectoryDependency(current, slash(current), !listed, [], listed, undefined, view));
		else if (!info.isFile() && !info.isDirectory()) throw new Error("unsupported host dependency");
	}
	return dependencies;
}


async function replayFilesystemEffects(
	owner: Pick<WorkspaceSandboxService, "commitDelta">,
	store: ProvenanceCertificateStore,
	artifacts: VerifiedArtifactClosure,
	journal: readonly OrderedEffectEvent[],
	projection: ExecutionPathProjection,
	privateStorage?: string,
) {
	await using privateReads = privateStorage ? FilesystemReadView.ambient() : undefined;
	const contents = new ContentChunkPool(store);
	try {
		const changes: SandboxWorkspaceChange[] = [], { workspaceRoot } = projection;
		for (const event of journal) {
			if (event.kind === "output") continue;
			const target = projection.toPhysical(event.path), inside = !!target && pathContains(workspaceRoot, target);
			if (!target || target === path.resolve(workspaceRoot) || !inside && pathContains(projection.sourceRoot, target)) {
				throw new Error(`replay effect escapes workspace: ${event.path}`);
			}
			const root = inside ? workspaceRoot : await hostRoot(target), address = { root, target, resource: slash(path.relative(root, target)) };
			if (event.before.kind === "directory" || event.after.kind === "directory") {
				if ([event.before, event.after].some(state => state.kind !== "absent" && state.kind !== "directory")) throw new Error(`unsupported replay type change: ${event.path}`);
				changes.push({
					...address, kind: "directory",
					...(event.before.kind === "directory" ? { before: directoryState(event.before) } : {}),
					...(event.after.kind === "directory" ? { after: directoryState(event.after) } : {}),
				});
				continue;
			}
			changes.push({
				...address,
				...projectFileMutation(event, name => projection.toPhysical(name)!),
				...(event.before.kind === "file" ? { before: await restoreArtifactContent(contents, event.before.data, artifacts), beforeMode: event.before.mode } : {}),
				...(event.after.kind === "file" ? { after: await restoreArtifactContent(contents, event.after.data, artifacts), afterMode: event.after.mode, afterModified: event.after.modified } : {}),
			});
		}
		// Inside a speculative session, what lies outside the workspace belongs to its private branch until adoption.
		const external = privateStorage ? changes.filter(change => !pathContains(workspaceRoot, change.target)) : [];
		if (external.length) await privateReplay(privateStorage!, external);
		if (changes.length > external.length) await owner.commitDelta({ output: { result: { content: [], details: {} }, isError: false }, changes: changes.filter(change => !external.includes(change)) }, privateReads);
	} finally { await contents.dispose(); }
}

/** Apply changes in a session's private branch as its sandboxes would have: over the merged view's state, into its upper
 * directory, a removal of a host object as a whiteout its sandboxes read. */
async function privateReplay(storage: string, changes: readonly SandboxWorkspaceChange[]) {
	const { upper, log, deleted } = await privateBranch(storage);
	const merged = async (target: string) => (await lstat(path.join(upper, target)).then(() => path.join(upper, target), () => undefined)) ?? (deleted(target) ? undefined : target);
	for (const change of changes) {
		const current = await merged(change.target);
		const captured = current && change.kind !== "directory" ? await captureStableFile(current, undefined, false) : undefined;
		if (captured && change.kind !== "directory" && (!change.before || captured.bytesRead !== change.before.byteLength || `sha256:${captured.hash}` !== contentDigest(change.before)))
			throw new Error(`resource changed before replay: ${change.target}`);
		const state = current === undefined ? undefined : change.kind === "directory" ? await readSandboxDirectoryState(current)
			: { content: change.before!, mode: Number(captured!.stat.mode) & 0o7777 };
		if (!sameSandboxState(state, change.kind === "directory" ? change.before : change.before && { content: change.before, mode: change.beforeMode! })) {
			throw new Error(`resource changed before replay: ${change.target}`);
		}
	}
	for (const change of changes) {
		const copy = path.join(upper, change.target);
		if (change.kind === "directory" ? change.after : change.after !== undefined) {
			await mkdir(path.dirname(copy), { recursive: true });
			if (change.kind === "directory") await mkdir(copy, { recursive: true });
			else {
				const handle = await open(copy, "w");
				try { await writeContent(handle, change.after!); } finally { await handle.close(); }
				await chmod(copy, change.afterMode ?? 0o644); await restoreModifiedTimes([[copy, change.afterModified]]);
			}
			continue;
		}
		await rm(copy, { recursive: true, force: true });
		if (await lstat(change.target).then(() => true, () => false)) {
			await mkdir(path.dirname(log), { recursive: true });
			await writeFile(log, `${change.target.slice(1).replace(/[\\\n\r]/g, char => char === "\n" ? "\\n" : char === "\r" ? "\\r" : "\\\\")}\n`, { flag: "a" });
		}
	}
}

function directoryState({ entriesDigest, mode, uid, gid }: SandboxDirectoryState) {
	return { entriesDigest, mode, uid, gid };
}

/** Output events carry the target's own descriptor. */
function loadOutputEvents(artifacts: VerifiedArtifactClosure, journal: readonly OrderedEffectEvent[]) {
	return journal.flatMap(event => event.kind === "output" ? [{ fd: event.fd, data: artifacts.read(event.data) }] : []);
}

function sandboxPolicyArguments(
	cwd: string,
	deniedPaths: readonly string[],
	writablePaths: readonly string[],
	mounts: readonly SandboxMount[],
	execMounts: readonly ExecMount[],
	privateWrites: string,
) {
	return [
		"run",
		"--chroot",
		"/",
		// A write the host user could make but the policy does not grant lands in a private branch here, as it would succeed natively.
		"--workdir", "/", "--fs-storage", privateWrites, "--fs-branch", "writes", "--on-exit", "keep", "--on-error", "keep",
		...mounts.flatMap((mount) => ["--fs-mount", sandboxMountArgument(mount)]),
		...execMounts.flatMap((mount) => ["--exec-mount", sandboxMountArgument(mount)]),
		"--fs-read",
		"/",
		...writablePaths.flatMap((target) => ["--fs-write", target]),
		...[...STABLE_SANDBOX_DEVICES].flatMap((target) => ["--fs-write", target]),
		...deniedPaths.flatMap((target) => ["--fs-deny", target]),
		"--no-huge-pages",
		"--no-coredump",
		"--max-processes",
		"64",
		"--cwd",
		cwd,
	];
}

/** What the brokered run a traced launcher `pid` started wrote, with its own descendants; a run never answers for itself. */
function brokeredWrites(session: ActiveSession, pid: number, self?: SessionWriter) {
	const writers = sessionWriters(session).filter(writer => writer !== self && writer.pid === pid);
	return writers.length && writers.every(writer => writer.written) ? [...new Set(writers.flatMap(writer =>
		[writer, ...writer.descendants ?? []].flatMap(child => [...child.written ?? [], ...child.external ?? []])))] : undefined;
}

const sessionWriters = (session: ActiveSession) => [...session.writers].filter(writer => writer.owner === session.token);

function workspaceWriterRecords(workspace: SandboxWorkspaceContext) {
	const writers = workspaceWriters.get(workspace.transactions) ?? new Set<SessionWriter>();
	workspaceWriters.set(workspace.transactions, writers); return writers;
}

type WorkspaceWriterTransfer = Pick<SessionWriter, "id" | "owner" | "startedAt" | "endedAt" | "writes"> & { readonly trace?: readonly [string, string] };
type WorkspaceWriteInquiry = { readonly exclude: readonly string[]; readonly targets: ReadonlySet<string>; readonly from: number; readonly until: number };

/** The workspace owner can inspect caller-owned native traces even while that caller is blocked. */
export async function observeWorkspaceWrites(workspace: SandboxWorkspaceContext, input: { readonly writer: WorkspaceWriterTransfer } | WorkspaceWriteInquiry): Promise<boolean> {
	const writers = workspaceWriterRecords(workspace);
	if ("writer" in input) {
		const { writer } = input, current = [...writers].find(record => record.id === writer.id) ?? { ...writer };
		Object.assign(current, writer, writer.trace ? { tail: traceTail(...writer.trace) } : writer.writes ? { tail: undefined } : {});
		writers.add(current); return false;
	}
	for (const other of writers) if (!input.exclude.includes(other.id) && other.startedAt <= input.until && (other.endedAt ?? Infinity) >= input.from) {
		const tail = other.writes ?? (await other.tail!.read()).writes;
		if ((other.endedAt ?? Infinity) >= input.from && writesWithin(other.writes ?? tail, input.targets, input.from, input.until)) return true;
	}
	return false;
}

async function shareSessionWriter(session: ActiveSession, { id, owner, startedAt, endedAt, writes }: SessionWriter, trace?: WorkspaceWriterTransfer["trace"]) {
	await session.observationOwner?.call("writes", { writer: { id, owner, startedAt, endedAt, writes, trace } });
}

function writerOwnership(session: ActiveSession, writers: readonly SessionWriter[]): WorkspaceTransactionOwnership | undefined {
	if (writers.some(writer => !writer.written || !writer.observed || writer.endedAt === undefined)) return;
	const observed = new Map<string, number>(), startedAt = Math.min(...writers.map(writer => writer.startedAt));
	for (const writer of writers) for (const [name, at] of writer.observed!) observed.set(name, Math.max(observed.get(name) ?? 0, at));
	return { written: new Set(writers.flatMap(writer => writer.written!)), observed, endedAt: Math.max(0, ...writers.map(writer => writer.endedAt!)),
		interfered: (paths, until) => {
			const inquiry = { exclude: writers.map(writer => writer.id), from: startedAt, until,
				targets: new Set([...paths].flatMap(name => [session.workspace.sandboxRoot, session.sourceRoot].map(root => path.posix.join(root, name)))) };
			return session.observationOwner?.call<boolean>("writes", inquiry) ?? observeWorkspaceWrites(session.workspace, inquiry);
		} };
}

function captureSessionInterval(session: ActiveSession): Promise<WorkspaceTransactionDelta> {
	return session.capture ??= Promise.resolve(session.transaction).then(capture => capture ? capture.finish(async () => writerOwnership(session, sessionWriters(session)))
		: { complete: false as const, changes: [], reason: "not_run" }).catch(error => ({ complete: false, changes: [], reason: errorMessage(error) }));
}

const privateUpper = (session: ActiveSession) => path.join(session.processRoot, "private", "writes", "upper");

/** A session's private branch under `storage`: its upper directory, its whiteout log and the host paths that log hides. */
async function privateBranch(storage: string) {
	const upper = path.join(storage, "writes", "upper"), log = path.join(storage, "writes", "deleted.log");
	const deletions = (await readFile(log, "utf8").catch(() => "")).split("\n").filter(Boolean)
		.map(line => `/${line.replace(/\\(.)/g, (_, escaped: string) => escaped === "n" ? "\n" : escaped === "r" ? "\r" : escaped)}`);
	return { upper, log, deleted: (target: string) => deletions.some(entry => target === entry || target.startsWith(`${entry}/`)) };
}

/** The nearest directory above `target` the host holds: where a change outside the workspace stages and commits. */
async function hostRoot(target: string) {
	for (let directory = path.dirname(target); ; directory = path.dirname(directory)) {
		if ((await lstat(directory).catch(() => undefined))?.isDirectory() || directory === path.dirname(directory)) return directory;
	}
}

/** Native images prove first reads; retained preimages prove earlier writes, with traced interference excluded. */
async function privateInputImages(session: ActiveSession, needed: ReadonlySet<string>, before?: PrivatePrestate, effects = false) {
	const root = path.join(session.processRoot, "private", "writes", "inputs"), indexes = await readdir(path.join(root, "paths")).catch(error => {
		if (missing(error)) return []; throw error;
	});
	if (indexes.length > 8192) throw new Error("input image index bound exceeded");
	const fields = FILESYSTEM_METADATA_FIELDS;
	const decode = (values: unknown) => {
		if (!Array.isArray(values) || values.length !== fields.length || values.some(value => typeof value !== "string" || !/^-?\d{1,40}$/.test(value)))
			throw new Error("invalid input image identity");
		return Object.fromEntries(fields.map((field, index) => [field, BigInt(values[index])])) as Record<typeof fields[number], bigint>;
	};
	const upper = privateUpper(session), predecessors = new Set<string>();
	const file = (target: string, hash: string, metadata: Pick<BigIntStats, "mode" | "uid" | "gid" | "size" | "nlink">): DynamicDependency => {
		if (before?.files.has(target)) predecessors.add(target);
		return { kind: "file", path: slash(target), role: "input", contentDigest: `sha256:${hash}`, metadataDigest: filesystemMetadataDigest(metadata) };
	};
	const capture = sharedWalk(async (key: string) => {
		const directory = path.join(root, key), row = JSON.parse(await readFile(path.join(directory, "proof.json"), "utf8")) as { path: string; physical: unknown; original: unknown };
		const original = decode(row.original), physical = decode(row.physical);
		if (!path.isAbsolute(row.path) || (original.mode & 0o170000n) !== 0o100000n || original.nlink !== 1n || original.size < 0n || original.size > 32n * 1024n * 1024n)
			throw new Error("invalid input image proof");
		const [baseline, working] = await Promise.all([captureFileDigest(path.join(directory, "baseline"), Number(original.size)), captureFileDigest(path.join(directory, "working"), 32 * 1024 * 1024)]);
		if (baseline.stat.size !== original.size || working.stat.dev !== physical.dev || working.stat.ino !== physical.ino) throw new Error("input image owner changed");
		const previous = before?.files.get(row.path);
		if (previous && (previous.stat.dev !== physical.dev || previous.stat.ino !== physical.ino)) return undefined;
		return previous ? file(row.path, previous.hash, { ...previous.stat, nlink: original.nlink + previous.stat.nlink - physical.nlink })
			: !before?.entries.has(row.path) && working.hash === baseline.hash && sameFilesystemIdentity(working.stat, physical) ? file(row.path, baseline.hash, original) : undefined;
	});
	const images = new Map<string, DynamicDependency[] | undefined>();
	for (const name of indexes) {
		if (!/^[0-9a-f]{16}\.json$/.test(name)) throw new Error("invalid input image index");
		const index = JSON.parse(await readFile(path.join(root, "paths", name), "utf8")) as { path: string; image: string; ambiguous?: boolean; sealed?: boolean; links: [string, string][] };
		if (!needed.has(index.path)) continue;
		if (!path.isAbsolute(index.path) || index.ambiguous || !index.sealed || !/^[0-9a-f]{16}$/.test(index.image) || !Array.isArray(index.links) || index.links.length > 40 ||
			index.links.some(link => !Array.isArray(link) || link.length !== 2 || !path.isAbsolute(link[0]) || typeof link[1] !== "string")) throw new Error("unsealed input image");
		const input = await capture(index.image);
		images.set(index.path, input ? [...index.links.map(([path, target]): DynamicDependency => ({ kind: "symlink", path, target, targetDigest: sha256Digest(target) })), input] : undefined);
	}
	for (const target of needed) if (!images.has(target)) {
		if (before?.entries.has(target) || await lstat(path.join(upper, target)).then(() => true, error => { if (!missing(error)) throw error; return false; })) {
			const previous = before?.entries.get(target)?.nlink === 1n && before.files.get(target);
			images.set(target, previous ? [file(target, previous.hash, previous.stat)] : undefined);
		}
	}
	if (!effects && predecessors.size && await privateInterference(session, predecessors, before!.startedAt))
		for (const [target, dependencies] of images) if (dependencies?.some(dependency => dependency.kind === "file" && predecessors.has(dependency.path))) images.set(target, undefined);
	return images;
}

/** Rebase every owned writer, including children retained as certificates, onto this run's private or host prestate. */
async function privateCommits(session: ActiveSession, since: number, owned: readonly SessionWriter[], prestate?: PrivatePrestate) {
	const { upper, deleted } = await privateBranch(path.join(session.processRoot, "private")), stat = (target: string) => lstat(target, { bigint: true }).catch(() => undefined);
	const changes: SandboxWorkspaceChange[] = [], before = new Map<string, DynamicDependency | undefined>();
	for (const target of new Set(owned.flatMap(writer => writer.external ?? []).map(name => path.posix.resolve(name)))) {
		if (STABLE_SANDBOX_DEVICES.has(target)) continue;
		const copy = path.join(upper, target), [own, native] = await Promise.all([stat(copy), stat(target)]);
		const prior = prestate?.entries.get(target), initial = prior ?? (prestate?.deleted(target) ? undefined : native);
		before.set(target, undefined);
		if (!prior && native && Number(native.ctimeMs) > since || [own, initial].some(info => info && !info.isFile() && !info.isDirectory()) ||
			prior?.isFile() && !prestate!.files.has(target)) continue;
		const file = (at: string, info?: BigIntStats) => info?.isFile() ? captureStableFile(at, MAX_CAPTURE_BYTES, true, { stat: info }) : undefined;
		const [afterFile, beforeFile] = await Promise.all([file(copy, own), prestate?.files.get(target) ?? file(target, initial)]);
		const entry = prestate?.structure?.entries.get(target.slice(1)), beforeDirectory = initial?.isDirectory()
			? prior ? entry?.kind === "directory" ? directoryState(entry) : undefined : await readSandboxDirectoryState(target) : undefined;
		before.set(target, beforeFile ? { kind: "file", path: target, role: "input", contentDigest: `sha256:${beforeFile.hash}`, metadataDigest: filesystemMetadataDigest(initial!) }
			: beforeDirectory ? { kind: "directory", path: target, entriesDigest: beforeDirectory.entriesDigest, metadataDigest: filesystemMetadataDigest(initial!) } : { kind: "absence", path: target });
		if (!own && !deleted(target) || own?.isDirectory() && initial?.isDirectory() || afterFile && beforeFile && afterFile.hash === beforeFile.hash && own!.mode === initial!.mode && own!.mtimeNs === initial!.mtimeNs) continue;
		const root = await hostRoot(target), resource = slash(path.relative(root, target));
		changes.push(own?.isDirectory() || initial?.isDirectory()
			? { kind: "directory", root, target, resource, before: beforeDirectory, ...(own ? { operation: "mkdir" as const, after: (await readSandboxDirectoryState(copy))! } : {}) }
			: { root, target, resource, ...(beforeFile ? { before: beforeFile.content!, beforeMode: Number(initial!.mode) & 0o7777 } : {}), ...(afterFile ? { after: afterFile.content!, afterMode: Number(own!.mode) & 0o7777, afterModified: String(own!.mtimeNs) } : {}) });
	}
	if (prestate) for (const [target, dependencies] of await privateInputImages(session,
		new Set([...prestate.files.keys()].filter(target => before.has(target))), prestate, true)) {
		before.set(target, dependencies?.find(dependency => dependency.kind === "file" && dependency.path === target));
	}
	if (prestate && before.size && await privateInterference(session, new Set(before.keys()), prestate.startedAt, owned)) for (const target of before.keys()) before.set(target, undefined);
	return { changes, before, complete: [...before.values()].every(Boolean) };
}

/** The entry is flushed before a syscall runs; a partial entry seen at capture start stays unsafe after completion. */
async function privateInterference(session: ActiveSession, paths: ReadonlySet<string>, since: number, owned: readonly SessionWriter[] = []) {
	const targets = new Set<string>(), until = Date.now();
	for (const target of paths) for (let name = target; ; name = path.dirname(name)) {
		targets.add(name); targets.add(path.join(privateUpper(session), name)); if (name === "/") break;
	}
	for (const writer of sessionWriters(session)) if (!owned.includes(writer) && (writer.endedAt ?? Infinity) >= since) {
		const tail = writer.tail ? await writer.tail.read() : writer;
		if (tail.unsafe || !tail.writes || writesWithin(tail.writes.filter(write => !write.paths || write.at >= since || write.mapped), targets, since, until)) return true;
	}
	return false;
}

/** A filesystem mount names its access; an exec mount only its image. */
function sandboxMountArgument(mount: SandboxMount | ExecMount) {
	if (!path.isAbsolute(mount.virtualPath) || !path.isAbsolute(mount.hostPath) || [mount.virtualPath, mount.hostPath].some((value) => value.includes(":"))) {
		throw new Error(`Sandlock mount cannot represent ${mount.virtualPath}:${mount.hostPath}`);
	}
	return `${mount.virtualPath}:${mount.hostPath}${"readOnly" in mount ? (mount.readOnly ? ":ro" : ":rw") : ""}`;
}

function speculativeProducerProof(ready: ReadyBackend, deniedPaths: readonly string[], policy = POLICY_ID) {
	return Object.freeze({
		observer: { provider: "strace", fingerprint: ready.observerFingerprint },
		execution: { authority: "speculative", confinement: { provider: "sandlock", fingerprint: digestObject({ policy, deniedPaths }) } },
	} satisfies ProcessProducerProof);
}

async function runSpawn(
	[executable, ...args]: readonly [string, ...string[]],
	options: Pick<ProcessExecutionRequest, "cwd" | "environment" | "signal" | "timeout"> & {
		readonly stdin?: Buffer;
		readonly onData?: (data: Buffer) => void;
		readonly outputRoot?: string;
		readonly tracers?: Set<number>;
		readonly onOutputEndpoints?: (endpoints: readonly [string, string]) => void;
		readonly inheritedFiles?: readonly number[];
		readonly onControl?: (channel: import("node:stream").Duplex, wake: () => boolean) => (() => void | Promise<void>);
	},
): Promise<SpawnOutcome> {
	options.signal?.throwIfAborted();
	await using channels = options.onOutputEndpoints ? await acquireOutputChannels(options.outputRoot!, options.signal) : undefined;
	options.signal?.throwIfAborted();
	if (channels) options.onOutputEndpoints!([channels.entries[0]!.endpoint, channels.entries[1]!.endpoint]);
	const child = spawn(executable, args, {
		cwd: options.cwd,
		env: options.environment,
		detached: true,
		stdio: [options.stdin ? "pipe" : "ignore", ...(channels ? channels.entries.map((entry) => entry.target!) : ["pipe", "pipe"] as const),
			...(options.inheritedFiles ?? []), ...(options.onControl ? ["pipe" as const] : [])],
	});
	if (child.pid) options.tracers?.add(child.pid);
	const output: BufferedOutput[] = [];
	child.once("spawn", () => channels?.releaseWriters());
	const append = (fd: 1 | 2, value: Buffer) => {
		const event = { fd, data: Buffer.from(value) } as const;
		const previous = output.at(-1);
		if (previous?.fd === fd && previous.data.byteLength + event.data.byteLength <= 1024 * 1024) {
			output[output.length - 1] = { fd, data: Buffer.concat([previous.data, event.data]) };
		} else output.push(event);
		options.onData?.(event.data);
	};
	const sources = channels?.entries.map((entry) => entry.source) ?? [child.stdout!, child.stderr!];
	const drained = Promise.all(sources.map((source, index) => {
		source.on("data", (value: Buffer) => append(index === 0 ? 1 : 2, value));
		return finished(source, { readable: true, writable: false, cleanup: true });
	}));
	void drained.catch(() => undefined);
	if (options.stdin) child.stdin?.end(options.stdin);
	const terminate = () => { if (!child.pid) return; try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); } };
	options.signal?.addEventListener("abort", terminate, { once: true });
	let timedOut = false;
	const timeout = options.timeout === undefined ? undefined : setTimeout(() => { timedOut = true; terminate(); }, Math.max(1, options.timeout * 1000));
	const completed: Promise<Pick<SpawnOutcome, "code" | "signal">> = once(child, "close").then(([code, signal]) => ({ code, signal }));
	let releaseControl: (() => void | Promise<void>) | undefined;
	try {
		if (options.onControl) releaseControl = options.onControl(child.stdio.at(-1) as import("node:stream").Duplex, () => child.kill("SIGUSR2"));
		const [result] = await Promise.all([completed, drained]);
		if (options.signal?.aborted) throw new Error("aborted");
		if (timedOut) throw new Error(`timeout:${options.timeout}`);
		return { ...result, output };
	} catch (error) { terminate(); await completed.catch(() => undefined); throw error; } finally {
		if (child.pid) options.tracers?.delete(child.pid);
		clearTimeout(timeout);
		options.signal?.removeEventListener("abort", terminate);
		await releaseControl?.();
	}
}

/** Own the write endpoints before inheritance; a running tracer may replace its descriptors. */
async function acquireOutputChannels(parent: string, signal?: AbortSignal) {
	const root = await mkdtemp(path.join(parent, "pi-process-output-")), directory = await open(root, fsConstants.O_RDONLY | fsConstants.O_DIRECTORY)
		.catch(async error => { await rm(root, { recursive: true, force: true }); throw error; });
	const entries: { server: net.Server; source: net.Socket; target?: net.Socket; endpoint: string }[] = [];
	const releaseWriters = () => { for (const entry of entries) entry.target?.destroy(); };
	const dispose = async () => {
		releaseWriters();
		for (const entry of entries) entry.source.destroy();
		await Promise.all(entries.map(({ server }) => closeServer(server)));
		await directory.close();
		await rm(root, { recursive: true, force: true });
	};
	try {
		for (const fd of [1, 2]) {
			signal?.throwIfAborted();
			// /proc/net/unix retains the literal name across processes, including /proc/self.
			const socketPath = `/proc/self/fd/${directory.fd}/${process.pid}-${path.basename(root)}-${fd}`;
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
		return { entries, releaseWriters, [Symbol.asyncDispose]: dispose };
	} catch (error) { await dispose(); throw error; }
}

/** The native launcher's framing: output as "o<fd> <length>\n<bytes>", then "x <code>", "s <signal number>" or "b" (bypass in place). */
function wireResponse(response: DispatcherResponse) {
	const frames = [...(response.output ?? []).map(event => ({ ...event, kind: "o" })), ...response.streams ?? []];
	const exit = response.exit, end = response.kind === "bypass" ? "b" : exit?.kind === "signal" ? `s ${exit.signal}` : `x ${exit?.kind === "code" ? exit.code : 125}`;
	return Buffer.concat([...frames.flatMap(({ fd, kind, data }) => [Buffer.from(`${kind}${fd} ${data.length}\n`), data]), Buffer.from(`${end}\n`)]);
}

/** The launcher's settlement of each inherited pipe: read back the shortest queue prefix, then write the suffix, that leave the queue as
 * the run left it. A queue of one repeated byte (a jobserver's tokens) usually needs neither. Undefined when the run did more than a queue can. */
function streamSettlement(inputs: ReturnType<typeof descriptorInputs>, events: readonly { readonly alias: number; readonly kind: string; readonly data: Buffer }[]) {
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

function parseDispatcherRequest(body: string) {
	try {
		const value: unknown = JSON.parse(body.trim());
		if (!isRecord(value)) return undefined;
		const request = value as Partial<DispatcherRequest>;
		return typeof request.token === "string" && isSafeCount(request.pid, 1) &&
			(request.descriptors === undefined || Array.isArray(request.descriptors) && request.descriptors.length <= 64 && request.descriptors.every(entry => entry &&
				[entry.fd, entry.alias, entry.flags, entry.capacity].every(Number.isSafeInteger) && entry.fd > 2 && typeof entry.eof === "boolean" &&
				[entry.device, entry.inode].every(value => typeof value === "string" && /^\d+$/.test(value)) && typeof entry.queueHex === "string" && /^(?:[0-9a-f]{2})*$/.test(entry.queueHex)))
			? request as Pick<DispatcherRequest, "token" | "pid" | "descriptors"> : undefined;
	} catch {
		return undefined;
	}
}

function eligibleRequest(session: ActiveSession, request: DispatcherRequest, expectedContext: ProcessExecutionContext) {
	if (!validProcessContext(request.context)) return "process_context_unsupported";
	if (!pathContains(session.workspace.sandboxRoot, request.cwd)) return "cwd_outside_workspace";
	if (request.args.length > 4096) return "argument_count_limit";
	if (request.args.reduce((sum, value) => sum + Buffer.byteLength(value), 0) > 1024 * 1024) return "argument_bytes_limit";
	if (!session.topLevelOutputEndpoints) return "output_endpoint_capture_missing";
	if (request.context.outputEndpoints.some((endpoint) => !endpoint)) return "request_output_endpoint_missing";
	// The launch key below still checks a discarded descriptor's type and flags. Both outlets belong to one captured process.
	const endpoints = [session.topLevelOutputEndpoints, ...session.nestedOutputEndpoints].find(pair =>
		request.context.outputEndpoints.every(endpoint => pair.includes(endpoint) || endpoint === "/dev/null")) ?? session.topLevelOutputEndpoints;
	const launch = processLaunchOptions(request.context, endpoints);
	if (!launch) return "output_endpoint_mismatch";
	const context = routedProcessIdentity(expectedContext, launch);
	if (request.context.launchKey !== context.launchKey) return "launch_key_mismatch";
	if (request.context.umask !== context.umask) return "umask_mismatch";
	return launch;
}

function shellArguments(invocation: ToolProcessInvocation, command: string) {
	return invocation.commandTransport === "argv" ? [...invocation.shellArgs, command] : [...invocation.shellArgs];
}

async function topLevelProcessPrototype(invocation: ToolProcessInvocation, request: ProcessExecutionRequest, environment: Readonly<Record<string, string>>,
	projection: ExecutionPathProjection, platformFingerprint: Sha256Digest) {
	return createExecPrototype({
		executablePath: invocation.shell,
		executableDigest: await hashExecutableFile(invocation.shell, request.signal),
		argv: [invocation.shell, ...shellArguments(invocation, request.command)],
		logicalCwd: projection.toLogical(projection.toPhysical(request.cwd) ?? request.cwd),
		environment,
		umask: process.umask(),
		processContextDigest: digestObject({
			limits: sha256Digest(await readFile("/proc/self/limits", { signal: request.signal })),
			credentials: { uid: process.getuid?.(), euid: process.geteuid?.(), gid: process.getgid?.(), egid: process.getegid?.(), groups: process.getgroups?.() },
			scheduler: { cpuCount: os.availableParallelism(), timeout: request.timeout ?? null },
			signals: "node-default",
		}),
		stdin: invocation.commandTransport === "stdin" ? { type: "bytes", digest: sha256Digest(request.command), eof: true } : { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [
			{ fd: 0, type: invocation.commandTransport === "stdin" ? "pipe" : "device", flagsDigest: digestObject({ mode: "read" }), eof: true },
			...[1, 2].map(fd => ({ fd, type: "pipe" as const, flagsDigest: digestObject({ mode: "write", sink: "buffered" }) })),
		],
		platformFingerprint,
	});
}

function actorReplayProducer(producer: ProcessProducerProof, deniedPaths: readonly string[]) {
	if (producer.observer.provider !== "strace" || producer.observer.fingerprint !== OBSERVER_FINGERPRINT) return false;
	const confinement = producer.execution.authority === "actor" ? undefined : producer.execution.confinement;
	return !confinement || confinement.provider === "sandlock" && [POLICY_ID, LEAF_POLICY_ID].some((policy) => confinement.fingerprint === digestObject({ policy, deniedPaths }));
}

function closeServer(server: net.Server): Promise<void> { return new Promise((resolve) => server.close(() => resolve())); }

function exitOutcome(outcome: SpawnOutcome): ExitOutcome {
	if (!outcome.signal) return { kind: "code", code: outcome.code ?? 125 };
	return { kind: "signal", signal: os.constants.signals[outcome.signal] ?? 9, coreDumped: false };
}

function assertInvocationMatches(invocation: ToolProcessInvocation, request: ProcessExecutionRequest) {
	const matches = {
		command: request.command === invocation.command,
		cwd: path.resolve(request.cwd) === path.resolve(invocation.cwd),
		timeout: request.timeout === invocation.timeout,
		environment: stableEqual(definedProcessEnvironment(request.environment), invocation.environment),
	};
	for (const [field, valid] of Object.entries(matches)) if (!valid) throw new Error(`process ${field} differs from the action execution context`);
}

export async function validateTransferredProcessEvidence(
	evidence: DynamicDependencyCertificate | undefined,
	incompleteReasons: Iterable<string> = [],
): Promise<ResourceValidation> {
	if (!evidence) {
		return { status: "indeterminate", cause: { stage: "freshness", code: "process_evidence_missing" }, metrics: { bytesRead: 0, filesRead: 0, mode: "exact" } };
	}
	const blockingTaints = evidence.taints.filter((taint) => !TRANSFERRED_INPUT_TAINTS.has(taint));
	const validation = await validateDynamicDependencyCertificate({ ...evidence, taints: blockingTaints }, { maxFileBytes: DEFAULT_CONTENT_LIMITS.maxLogicalBytes });
	const metrics = { bytesRead: validation.bytesRead, filesRead: validation.filesRead, mode: "exact" as const };
	if (validation.status === "valid") return { status: "valid", metrics };
	if (validation.status === "stale") return { status: "stale", cause: { stage: "freshness", code: "process_dependency_changed", detail: validation.changed.join(",") }, metrics };
	return {
		status: "indeterminate",
		cause: { stage: "freshness", code: "process_provenance_indeterminate", detail: [validation.reason, ...incompleteReasons].join(",") },
		metrics,
	};
}

function sensitivePaths(storeRoot: string, additional: readonly string[] | undefined) {
	const home = os.homedir(), credentials = [
		".ssh",
		".gnupg",
		".aws",
		".azure",
		".kube",
		".docker/config.json",
		".config/gcloud",
		".config/gh/hosts.yml",
		".git-credentials",
		".netrc",
		".npmrc",
		".pypirc",
		".pi/agent/auth.json",
		".codex/auth.json",
	].map(name => path.join(home, name));
	return Object.freeze([...new Set([storeRoot, ...credentials, ...additional ?? []]
		.filter(path.isAbsolute).map(value => path.resolve(value)))]);
}
