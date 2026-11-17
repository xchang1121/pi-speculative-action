import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { TSchema } from "@earendil-works/pi-ai";
import { AsyncLocalStorage } from "node:async_hooks";
import type { ActionEffect, ActionKey, ActionSemanticsDefinition } from "./action-semantics.ts";
import { cloneSharedData, errorMessage, immutableSnapshot, stableValueHash } from "./primitives.ts";
import type { ResourceInputSession, ToolFilesystemOperations } from "./resource-version.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { ResolutionCause, ResourceValidation, ToolOperation } from "./settlement.ts";
import { cause, zeroValidationMetrics } from "./settlement.ts";
import type { TimelineDependency } from "./task-timing.ts";
import { TimelineInterval } from "./task-timing.ts";
export type EffectCapability =
	| "invocation.host_function"
	| "invocation.process"
	| "invocation.workspace_path"
	| "filesystem.read"
	| "filesystem.write"
	| "process.spawn"
	| "network.mediate"
	| "ipc.mediate"
	| "time.virtualize"
	| "random.virtualize"
	| "output.gate"
	| "validation.resource_snapshot";
export interface EffectRequirements {
	readonly capabilities: readonly EffectCapability[];
}
/** `all` is reserved for a runtime-wide sandbox that contains every operation effect. */
export type EffectCapabilities = "all" | readonly EffectCapability[];
export const RESOURCE_OBSERVATION_EFFECTS = effectRequirements(
	"invocation.host_function",
	"filesystem.read",
	"validation.resource_snapshot",
);
export const WORKSPACE_PATH_MUTATION_EFFECTS = effectRequirements(
	"invocation.host_function",
	"invocation.workspace_path",
	"filesystem.read",
	"filesystem.write",
);
export const UNRESTRICTED_PROCESS_EFFECTS = effectRequirements(
	"invocation.process",
	"filesystem.read",
	"filesystem.write",
	"process.spawn",
	"network.mediate",
	"ipc.mediate",
	"time.virtualize",
	"random.virtualize",
	"output.gate",
);
export function effectRequirements(...capabilities: readonly EffectCapability[]): EffectRequirements {
	return Object.freeze({
		capabilities: Object.freeze([...new Set(capabilities)].sort()),
	});
}
export function effectCapabilitiesCover(provided: EffectCapabilities, required: EffectRequirements): boolean {
	if (provided === "all") return true;
	const available = new Set(provided);
	return required.capabilities.every((capability) => available.has(capability));
}
export function normalizeEffectRequirements(requirements: EffectRequirements): EffectRequirements {
	return effectRequirements(...requirements.capabilities);
}
export type EffectTransactionState =
	| "begun"
	| "executing"
	| "sealing"
	| "sealed"
	| "validating"
	| "validated"
	| "committing"
	| "committed"
	| "aborting"
	| "aborted"
	| "poisoned"
	| "failed";
export type EffectCommitDisposition = "recoverable" | "poisoned";
/** A commit failure whose disposition decides whether authoritative execution may still begin. */
export class EffectCommitFailure extends Error {
	readonly disposition: EffectCommitDisposition;
	readonly resolutionCause?: ResolutionCause;
	constructor(
		disposition: EffectCommitDisposition,
		message: string,
		cause: unknown,
		resolutionCause?: ResolutionCause,
	) {
		super(message, { cause });
		this.name = "EffectCommitFailure";
		this.disposition = disposition;
		this.resolutionCause = resolutionCause;
	}
}
export function effectCommitFailure(
	error: unknown,
	disposition: EffectCommitDisposition,
	message = errorMessage(error),
	resolutionCause?: ResolutionCause,
): EffectCommitFailure {
	return error instanceof EffectCommitFailure
		? error
		: new EffectCommitFailure(disposition, message, error, resolutionCause);
}
export function isPoisonedEffectCommit(error: unknown): error is EffectCommitFailure & {
	readonly disposition: "poisoned";
} {
	return error instanceof EffectCommitFailure && error.disposition === "poisoned";
}
/** Share pending/successful effects; only proven recoverable failures permit another attempt. */
export class EffectCommit<Output> {
	private outcome?: Promise<Output>;
	get result(): Promise<Output> | undefined {
		return this.outcome;
	}
	run(apply: () => Promise<Output>): Promise<Output> {
		return (this.outcome ??= Promise.resolve()
			.then(apply)
			.catch((error) => {
				const failure = effectCommitFailure(error, "poisoned");
				if (failure.disposition === "recoverable") this.outcome = undefined;
				throw failure;
			}));
	}
}
export interface EffectTransactionDescriptor {
	readonly tool: string;
	readonly callID?: string;
	readonly route: SpeculativeExecutionRoute;
}
/** Mutable only to the coordinator that issued it; callers receive a read-only lifecycle view. */
export interface EffectTransactionAttempt {
	readonly id: string;
	readonly descriptor: EffectTransactionDescriptor;
	readonly state: EffectTransactionState;
}
/** WorldBranch with mandatory validation and one abort lifecycle owned by the gateway. */
export type EffectTransaction<Output> = WorldBranch<Output> & {
	readonly transactionID: string;
	/** The sole public lifecycle for validation, adoption, and disposal. */
	readonly state: EffectTransactionState;
	readonly latestValidation?: ResourceValidation;
	readonly validate: () => Promise<ResourceValidation>;
	readonly abort: () => Promise<void>;
};
interface MutableEffectTransactionAttempt extends Omit<EffectTransactionAttempt, "state"> {
	stateValue: EffectTransactionState;
}
/** Coordinates begin → execute/seal → validate → commit/abort for every execution backend. */
export class EffectTransactionCoordinator<Output> {
	private readonly attempts = new WeakMap<EffectTransactionAttempt, MutableEffectTransactionAttempt>();
	private sequence = 0;
	begin(descriptor: EffectTransactionDescriptor): EffectTransactionAttempt {
		const owned: MutableEffectTransactionAttempt = {
			id: `tx_${++this.sequence}`,
			descriptor: immutableSnapshot(descriptor),
			stateValue: "begun",
		};
		const attempt = Object.freeze({
			id: owned.id,
			descriptor: owned.descriptor,
			get state() {
				return owned.stateValue;
			},
		});
		this.attempts.set(attempt, owned);
		return attempt;
	}
	async execute(
		attempt: EffectTransactionAttempt,
		executor: () => Promise<WorldBranch<Output>>,
	): Promise<EffectTransaction<Output>> {
		const owned = this.owned(attempt);
		this.transition(owned, "begun", "executing");
		try {
			const branch = await executor();
			this.transition(owned, "executing", "sealing");
			return await this.seal(owned, branch);
		} catch (error) {
			owned.stateValue = "failed";
			throw error;
		}
	}
	/** Wrap a pre-execution authoritative capture in the same transaction lifecycle. */
	capture(
		attempt: EffectTransactionAttempt,
		capture: WorldResultCapture<Output>,
	): WorldResultCapture<Output> {
		const owned = this.owned(attempt);
		const inputsOnly = capture.inputsOnly;
		return Object.freeze({
			...(inputsOnly ? { inputsOnly } : {}),
			get inputSource() {
				return owned.stateValue === "begun" ? capture.inputSource : undefined;
			},
			seal: async (output?: Output) => {
				this.transition(owned, "begun", "sealing");
				try {
					if (!inputsOnly && !capture.inputSource && output === undefined)
						throw new Error("authoritative_result_missing");
					const branch = await capture.seal(output);
					if ((inputsOnly || output === undefined) && !branch.inputsOnly) {
						await branch.dispose();
						throw new Error("input_only_capture_required");
					}
					return await this.seal(owned, branch);
				} catch (error) {
					owned.stateValue = "failed";
					await capture.dispose();
					throw error;
				}
			},
			dispose: async () => {
				if (owned.stateValue !== "begun") return;
				owned.stateValue = "aborting";
				try {
					await capture.dispose();
				} finally {
					owned.stateValue = "aborted";
				}
			},
		});
	}
	private async seal(
		attempt: MutableEffectTransactionAttempt,
		branch: WorldBranch<Output>,
	): Promise<EffectTransaction<Output>> {
		try {
			const transaction = sealEffectTransaction(attempt, branch);
			this.transition(attempt, "sealing", "sealed");
			return transaction;
		} catch (error) {
			try {
				await branch.dispose();
			} catch {
				/* Preserve the sealing failure. */
			}
			throw error;
		}
	}
	private owned(attempt: EffectTransactionAttempt): MutableEffectTransactionAttempt {
		const mutable = this.attempts.get(attempt);
		if (!mutable) throw new Error("effect transaction belongs to another coordinator");
		return mutable;
	}
	private transition(
		attempt: MutableEffectTransactionAttempt,
		expected: EffectTransactionState,
		next: EffectTransactionState,
	): void {
		if (attempt.stateValue !== expected)
			throw new Error(`effect transaction ${attempt.id} is ${attempt.stateValue}, expected ${expected}`);
		attempt.stateValue = next;
	}
}
function sealEffectTransaction<Output>(
	attempt: MutableEffectTransactionAttempt,
	branch: WorldBranch<Output>,
): EffectTransaction<Output> {
	if (branch.inputsOnly && "output" in branch) throw new Error("input_only_output_retained");
	const shared = attempt.descriptor.route.reuse === "shared_result";
	const retainedBytes = Object.getOwnPropertyDescriptor(branch, "capturedBytes")?.get?.bind(branch);
	const validateAndCommit = shared ? branch.validateAndCommit?.bind(branch) : undefined;
	const sealed: WorldBranch<Output> = Object.freeze({
		...immutableSnapshot({
			backend: branch.backend,
			resources: branch.resources,
			inputResources: shared ? branch.inputResources : undefined,
			capturedBytes: branch.capturedBytes,
			reconstructionScope:
				shared && branch.reconstruct && !validateAndCommit ? branch.reconstructionScope : undefined,
			executionMetrics: branch.executionMetrics,
			compatibility: branch.compatibility,
		}),
		// Checkpoints are opaque backend-issued handles; pin the reference without cloning their owner.
		checkpoint: branch.checkpoint,
		inputSource: shared ? branch.inputSource : undefined,
		invalidateInputs: shared ? branch.invalidateInputs?.bind(branch) : undefined,
		...(branch.inputsOnly
			? { inputsOnly: true as const }
			: { output: shared ? cloneSharedData(branch.output) : branch.output }),
		operations: branch.operations && Object.freeze([...branch.operations]),
		computationDependencies:
			branch.computationDependencies && Object.freeze([...branch.computationDependencies]),
		validate: validateAndCommit ?? branch.validate?.bind(branch),
		reconstruct: branch.reconstruct?.bind(branch),
		commit: branch.commit.bind(branch),
		dispose: branch.dispose.bind(branch),
		takeCommittedInputs: branch.takeCommittedInputs?.bind(branch),
		takeReadInputs: branch.takeReadInputs?.bind(branch),
	});
	let validation: ResourceValidation | undefined;
	let validationPromise: Promise<ResourceValidation> | undefined;
	const commit = new EffectCommit<Output>();
	let inputTransfer: ReturnType<NonNullable<WorldBranch<Output>["takeCommittedInputs"]>> | undefined;
	const lifecycle = new RuntimeLifecycleLane();
	const queryResources = new Set<() => Promise<void>>();
	const abort = (): Promise<void> => {
		if (!lifecycle.sealed && !["committed", "poisoned"].includes(attempt.stateValue) && !commit.result)
			attempt.stateValue = "aborting";
		return lifecycle.dispose(async () => {
			try {
				await Promise.allSettled([...queryResources].map((dispose) => dispose()));
				await sealed.dispose();
			} finally {
				if (!["committed", "poisoned"].includes(attempt.stateValue)) attempt.stateValue = "aborted";
			}
		});
	};
	const validate = async (queryProof?: WorldBranch<Output>["validate"]): Promise<ResourceValidation> => {
		// A reserved commit owns its proof window; a later validation must not reset that state.
		if (!lifecycle.sealed && commit.result && attempt.stateValue !== "committed")
			await Promise.allSettled([commit.result]);
		if (lifecycle.sealed || ["aborted", "aborting", "poisoned", "failed"].includes(attempt.stateValue)) {
			return {
				status: "indeterminate",
				cause: cause("freshness", "transaction_unavailable"),
				metrics: zeroValidationMetrics(),
			};
		}
		// Each request owns a fresh proof after its predecessors, never their earlier observation.
		const pending = lifecycle.serialize(async () => {
			if (!queryProof && ["sealed", "validated"].includes(attempt.stateValue))
				attempt.stateValue = "validating";
			const result = await validateWorldBranch(
				queryProof ? { validate: queryProof } : sealed,
				attempt.descriptor.route.reuse,
			);
			// A query borrows the same lifetime and validation lane, but cannot authorize the source output.
			if (!queryProof) {
				validation = result;
				if (attempt.stateValue === "validating")
					attempt.stateValue = result.status === "valid" ? "validated" : "sealed";
			}
			return result;
		});
		validationPromise = pending;
		try {
			return await pending;
		} finally {
			if (validationPromise === pending) validationPromise = undefined;
		}
	};
	const transaction: EffectTransaction<Output> = {
		...sealed,
		transactionID: attempt.id,
		get state() {
			return attempt.stateValue;
		},
		get latestValidation() {
			return validation;
		},
		get capturedBytes() {
			return retainedBytes ? retainedBytes() : sealed.capturedBytes;
		},
		takeCommittedInputs: sealed.takeCommittedInputs
			? async (maxBytes) => {
					if (lifecycle.sealed || attempt.stateValue !== "committed" || inputTransfer) return undefined;
					return (inputTransfer = lifecycle.track(
						Promise.resolve()
							.then(() => sealed.takeCommittedInputs!(maxBytes))
							.then(async (inputs) => {
								if (!lifecycle.sealed) return inputs;
								await inputs?.dispose();
								return undefined;
							}),
					));
				}
			: undefined,
		reconstruct:
			shared && sealed.reconstruct
				? async (request) => {
						// Borrowing sealed inputs grants no commit authority; freshness is checked after evaluation.
						if (
							lifecycle.sealed ||
							!["sealed", "validating", "validated", "committed"].includes(attempt.stateValue)
						)
							return undefined;
						const task = Promise.resolve()
							.then(() => sealed.reconstruct!(request))
							.then(async (result) => {
								if (!result) return undefined;
								// An atomic validation/commit callback retains its complete proof and effect ownership.
								const proof = !validateAndCommit && result.validate?.bind(result);
								const release = result.dispose?.bind(result);
								let released: Promise<void> | undefined;
								const dispose = () =>
									(released ??= Promise.resolve()
										.then(release)
										.finally(() => queryResources.delete(dispose)));
								if (release) queryResources.add(dispose);
								try {
									return Object.freeze({
										output: cloneSharedData(result.output),
										capturedBytes: result.capturedBytes,
										requiresQueryValidation: result.requiresQueryValidation,
										compatibility: proof ? immutableSnapshot(result.compatibility) : undefined,
										...(proof
											? {
													validate: () =>
														validate(() =>
															released
																? Promise.resolve({
																		status: "indeterminate",
																		cause: cause("freshness", "query_disposed"),
																		metrics: zeroValidationMetrics(),
																	})
																: proof(),
														),
												}
											: {}),
										...(release ? { dispose } : {}),
									});
								} catch (error) {
									await dispose().catch(() => {});
									throw error;
								}
							});
						return lifecycle.track(task);
					}
				: undefined,
		validate: () => validate(),
		commit: async () => {
			if (sealed.inputsOnly) throw effectCommitFailure(new Error("input_only_branch"), "recoverable");
			// An admitted effect keeps its original settlement, including during/after retirement.
			if (commit.result) return shared ? cloneSharedData(await commit.result) : commit.result;
			if (lifecycle.sealed)
				throw effectCommitFailure(new Error("effect transaction resources are retired"), "recoverable");
			if (!validationPromise && validation?.status !== "valid") {
				throw new Error(`effect transaction ${attempt.id} requires successful validation before commit`);
			}
			// Reserve the entire validation → commit operation before yielding, not just its effect.
			const pending = lifecycle.track(
				commit.run(async () => {
					try {
						await validationPromise;
						if (validation?.status !== "valid" || attempt.stateValue !== "validated") {
							throw effectCommitFailure(
								new Error(`effect transaction ${attempt.id} cannot commit from ${attempt.stateValue}`),
								"recoverable",
								undefined,
								validation?.status !== "valid" ? validation?.cause : undefined,
							);
						}
						attempt.stateValue = "committing";
						const output = await sealed.commit();
						attempt.stateValue = "committed";
						return shared ? sealed.output : output;
					} catch (error) {
						const failure = effectCommitFailure(
							error,
							"poisoned",
							"effect commit failed without proof that its side effects were restored",
						);
						attempt.stateValue = failure.disposition === "poisoned" ? "poisoned" : "sealed";
						validation = undefined;
						throw failure;
					}
				}),
			);
			return shared ? cloneSharedData(await pending) : pending;
		},
		abort,
		dispose: abort,
	};
	if (!sealed.inputsOnly)
		Object.defineProperty(transaction, "output", {
			get: () => (shared ? cloneSharedData(sealed.output) : sealed.output),
		});
	return Object.freeze(transaction);
}
/** Host-neutral result consumed by the speculative scheduler. */
export interface ToolSettlement<TDetails = unknown> {
	readonly result: AgentToolResult<TDetails>;
	readonly isError: boolean;
	/** A process that ran to completion and exited non-zero failed on its own terms: once validated, the Actor may adopt it. */
	readonly exitCode?: number;
}
export interface ToolObservationContract {
	readonly version: string;
	/** Model-visible content schema; defaults to text/image blocks. */
	readonly content?: TSchema;
	/** Pure, binding-owned normalization; changing these rules requires a new version. */
	readonly canonicalize?: (
		content: ToolSettlement["result"]["content"],
	) => ToolSettlement["result"]["content"];
}
export const EXACT_TOOL_OBSERVATION: ToolObservationContract = Object.freeze({
	version: "model-visible/1",
});
export const toolObservationContract = (action?: ActionKey): ToolObservationContract =>
	(action?.executionContext as ToolInvocation | undefined)?.observation ?? EXACT_TOOL_OBSERVATION;
/** Normalization affects prediction ancestry only; it never changes the Actor's output or execution proofs. */
export function toolObservationKey(output: ToolSettlement, action?: ActionKey): string | undefined {
	try {
		return canonicalToolObservation(output, action).key;
	} catch {
		return undefined;
	}
}
export function canonicalToolObservation(output: ToolSettlement, action?: ActionKey) {
	const contract = toolObservationContract(action);
	const original = output.result.content;
	const content = contract.canonicalize
		? immutableSnapshot(contract.canonicalize(immutableSnapshot(original)))
		: original;
	return {
		content,
		key: stableValueHash([contract.version, contract.content, content, output.isError, output.exitCode]),
	};
}
export function toolErrorSettlement(error: unknown): ToolSettlement {
	return {
		result: {
			content: [
				{
					type: "text",
					text: error instanceof Error ? error.message || error.name : String(error),
				},
			],
			details: {},
		},
		isError: true,
	};
}
/** Exact process invocation accepted by an optional isolated-process backend. */
export interface ToolProcessInvocation {
	readonly command: string;
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly shell: string;
	readonly shellArgs: readonly string[];
	readonly commandTransport: "argv" | "stdin";
	readonly timeout?: number;
}
/** Identity of the concrete tool executor. */
export interface ToolInvocation {
	readonly executor: string;
	/** Original Actor callback; automatic data calculations never authorize its outer effects. */
	readonly callback?: Function;
	/** The enclosing host callback may run only for an actual call; independently proven child operations remain eligible. */
	readonly authoritativeOnly?: true;
	readonly observation?: ToolObservationContract;
	/** Internal execution uses the current enclosing action for permission and an opaque backend binding. */
	readonly operation?: {
		readonly binding: ExecutionOperationBinding;
		readonly permission: ActionKey;
	};
	/** Explicit common Actor/speculation profile; never inferred from the tool's name. */
	readonly semantics?: ActionSemanticsDefinition;
	/** Input-invariant executor identity used by K(a); the exact invocation remains in `process`. */
	readonly identity?: unknown;
	readonly process?: ToolProcessInvocation;
	/** Explicit read-only input boundary; omitted operations remain confined to the host workspace. */
	readonly filesystemRoot?: string;
	/** Absolute potential access paths for ordering only; captured observations still decide validity. */
	readonly filesystemResources?: readonly string[];
	/** Explicit selected Actor semantics; the host invokes this inside its original execution callback. */
	readonly authoritative?: (
		request: Parameters<NonNullable<ToolInvocation["filesystem"]>>[1],
	) => Promise<ToolSettlement>;
	/** Arm bounded input and pure-computation retention in the selected Actor executor, without replaying effects. */
	readonly captureInputs?: (
		action: ActionKey | undefined,
		maxBytes: number,
		callID: string,
		inputs?: ResourceInputSession,
	) => WorldResultCapture<ToolSettlement>;
	/** Explicit trusted operation binding; never permission to call the supplied host tool. */
	readonly filesystem?: (
		view: ToolFilesystemOperations,
		request: {
			readonly args: unknown;
			readonly callID: string;
			readonly signal: AbortSignal;
		},
	) => Promise<ToolSettlement>;
}
/** Concrete isolation used for one speculative execution. */
export type SpeculativeExecution = "runtime_sandbox" | "resource_snapshot" | "workspace_branch";
export type WorldReuseStrategy = "shared_result" | "exclusive_branch";
export type ExecutionWorldScope = "runtime" | "fallback";
/** Execution ownership, separate from reusable action identity and cross-call sharing permission. */
export interface ExecutionScope {
	readonly sessionID: string;
	readonly turnID: string;
	readonly callID?: string;
}
export function snapshotExecutionScope(scope: ExecutionScope | undefined): ExecutionScope | undefined {
	return scope
		? Object.freeze({
				sessionID: scope.sessionID,
				turnID: scope.turnID,
				...(scope.callID !== undefined ? { callID: scope.callID } : {}),
			})
		: undefined;
}
const executionScope = new AsyncLocalStorage<ExecutionScope | undefined>();
export const currentExecutionScope = () => executionScope.getStore();
export const withExecutionScope = <Value>(scope: ExecutionScope | undefined, execute: () => Value): Value =>
	executionScope.run(snapshotExecutionScope(scope), execute);
/** Tool effects are resolved independently from K(a) and prediction source. */
export interface ExecutionWorldRequest {
	readonly backend?: string;
	readonly parentCheckpoint?: WorldCheckpoint;
	readonly effect: ActionEffect;
	readonly requirements: EffectRequirements;
	/** Tool scope is still known during warm-up, before a concrete action key exists. */
	readonly tool?: string;
	/** Present for concrete candidates; omitted for best-effort turn warm-up. */
	readonly action?: ActionKey;
	/** Selected Actor executor, independent of whole-call reuse identity. */
	readonly executionContext?: unknown;
}
/** One resolved execution capability. Absence of a route means speculation is blocked. */
export interface SpeculativeExecutionRoute {
	readonly isolation: SpeculativeExecution;
	readonly reuse: WorldReuseStrategy;
	readonly scope: ExecutionWorldScope;
	readonly backend: string;
	readonly fingerprint: string;
	/** Backend-authenticated parent state this route can materialize; never a result-freshness proof. */
	readonly acceptsCheckpoint?: (checkpoint: WorldCheckpoint) => boolean;
	/** Scheduling hint only; false permits preparing against the current world. */
	readonly dependsOn?: (parent: ActionKey) => Promise<boolean>;
}
export function sameSpeculativeExecutionRoute(
	left: SpeculativeExecutionRoute,
	right: SpeculativeExecutionRoute,
): boolean {
	return (
		left.isolation === right.isolation &&
		left.reuse === right.reuse &&
		left.scope === right.scope &&
		left.backend === right.backend &&
		left.fingerprint === right.fingerprint
	);
}
const WORLD_REUSE_COUNTERS = [
	"requests",
	"hits",
	"joinedHits",
	"sameTurnHits",
	"crossTurnHits",
	"unattributedHits",
	"misses",
	"bypasses",
	"published",
	"tainted",
	"validationCandidates",
	"validationPathsets",
	"validationFilesRead",
	"validationBytesRead",
	"validationArtifactsLoaded",
	"validationArtifactBytesRead",
	"executionMs",
	"reusedProcessMs",
	"wholeCommandRequests",
	"wholeCommandHits",
	"wholeCommandMisses",
	"wholeCommandPublished",
	"wholeCommandReusedProcessMs",
] as const;
type WorldReuseCounter = (typeof WORLD_REUSE_COUNTERS)[number];
/**
 * Backend-neutral process-reuse accounting. Reused-process time is producer-observed work;
 * complete-task latency and speedup come from TaskTimeline.
 */
export type WorldReuseMetrics = Readonly<Record<WorldReuseCounter, number>> & {
	readonly lastError?: string;
};
const EMPTY_WORLD_REUSE_METRICS = Object.fromEntries(
	WORLD_REUSE_COUNTERS.map((key) => [key, 0]),
) as unknown as WorldReuseMetrics;
export function emptyWorldReuseMetrics(): WorldReuseMetrics {
	return { ...EMPTY_WORLD_REUSE_METRICS };
}
export interface WorldExecutionMetrics {
	/** Validated result reuse performed by the world while executing the action. */
	readonly reuse?: WorldReuseMetrics;
}
/** Backend-issued evidence; policy decides whether it matches the Actor world. */
export type WorldCompatibilityEvidence =
	| {
			readonly status: "compatible";
			readonly backend: string;
			readonly executionFingerprint: string;
	  }
	| {
			readonly status: "incompatible" | "indeterminate";
			readonly backend: string;
			readonly code: string;
			readonly detail?: string;
	  };
/** Immutable execution state from which a later speculative action may derive. */
export interface WorldCheckpoint {
	readonly backend: string;
	readonly id: string;
	readonly lineage: string;
	readonly depth: number;
}
/** Opaque backend-issued capability for an internal unit; the enclosing action still owns permission. */
export interface ExecutionOperationBinding {
	readonly backend: string;
	readonly identity: string;
	/** The internal unit's effects; its enclosing tool still supplies the permission check. */
	readonly semantics: Pick<ActionSemanticsDefinition, "effect" | "requirements" | "resourceScope">;
	/** Live preparation hint only; true never replaces backend permission or dependency checks. */
	readonly available?: boolean;
	/** Retrieval hint only. The issuing backend must check the current enclosing action's complete K(a). */
	readonly permissionHash: string;
	readonly executionMs: number;
	/** Isolated service estimate, including observed preparation and capture. */
	/** Whether a run now would redo it: no result of it the backend holds is still valid for the workspace. */
	readonly stale?: () => Promise<boolean>;
	/** It reads what another process writes while it runs: alone, it runs only up to that input. */
	readonly fed?: true;
	/** Eligible to launch on current filesystem inputs without captured resources; not proof of parent independence or Actor freshness. */
	readonly preparation?: "current_workspace";
}
/** Delivered only after the authoritative OS boundary confirms the internal result was consumed. */
export interface ExecutionOperationAdoption {
	readonly scope: ExecutionScope;
	readonly id: string;
	readonly sequence: number;
	readonly backend: string;
	readonly operationIdentity: string;
	/** What producing the adopted result took, which the adopting call did not spend. */
	readonly executionMs?: number;
	/** The exact successful-consumption receipt also passed to authoritative computation accounting. */
	readonly computation?: TimelineDependency;
}
/** Backend resources remain scoped and revocable; discovery alone grants no adoption authority. */
export interface ExecutionResourceContext {
	readonly inputs?: (path?: string) => Iterable<object>;
	readonly onInputSource?: (source: object) => void;
	readonly onOperationAdopted?: (adoption: ExecutionOperationAdoption) => void;
	/** Scheduling hint only; the backend still proves every join. */
	readonly onOperationJoinable?: (available: () => boolean) => void;
	readonly acceptOperationScope?: (scope: ExecutionScope, salvage?: boolean) => boolean;
}
/** Input-only owners omit output and can serve proven queries, never the source action's result or effects. */
export type WorldOutput<Output> =
	| {
			readonly output: Output;
			readonly inputsOnly?: never;
	  }
	| {
			readonly inputsOnly: true;
			readonly output?: never;
	  };
/** Sealed backend artifact; ephemeral process, environment and network state do not cross this boundary.
 * Backends provide idempotent commits; EffectTransaction owns validation and adoption. */
export type WorldBranch<Output> = WorldOutput<Output> & {
	readonly backend: string;
	/** Observed internal work. Only successful Actor adoption makes these authoritative learning inputs. */
	readonly operations?: readonly ExecutionOperationBinding[];
	readonly computationDependencies?: readonly TimelineDependency[];
	readonly checkpoint?: WorldCheckpoint;
	readonly resources: readonly string[];
	/** Optional lookup priority hints; neither completeness nor adoption authority. */
	readonly inputResources?: readonly {
		readonly path: string;
		readonly descendants?: boolean;
	}[];
	/** Opaque backend input owner; preserved through transactions, never result adoption authority. */
	readonly inputSource?: object;
	/** Revoke borrowable inputs, optionally returning removed indexed names; sealed output proofs remain. */
	readonly invalidateInputs?: (paths: readonly string[]) => void | readonly string[];
	/** Transfer one input owner after a successful commit; the receiver owns its cleanup. */
	readonly takeCommittedInputs?: (maxBytes: number) => Promise<
		| (WorldBranch<Output> & {
				readonly inputsOnly: true;
		  })
		| undefined
	>;
	/** Before any commit, the unchanged bytes an exclusive branch read, as an independent input owner (at most once). */
	readonly takeReadInputs?: (maxBytes: number) => Promise<
		| (WorldBranch<Output> & {
				readonly inputsOnly: true;
		  })
		| undefined
	>;
	/** May evaluate the current action instead of only the source executor. Still requires query evidence. */
	readonly reconstructionScope?: "current_action";
	/** Captured persistent-effect bytes, excluding the serialized tool output. */
	readonly capturedBytes: number;
	readonly executionMetrics: WorldExecutionMetrics;
	readonly compatibility: WorldCompatibilityEvidence;
	/** Required for shared results; exclusive branches may instead prove conflicts atomically at commit. */
	readonly validate?: () => Promise<ResourceValidation>;
	/** Shared observations only: prove freshness and complete the effect-free backend commit together.
	 * Every call must validate afresh; the coordinator still owns Actor adoption and may call commit again. */
	readonly validateAndCommit?: () => Promise<ResourceValidation>;
	/** Re-evaluate sealed inputs; an optional query proof excludes inputs that evaluation never used. */
	readonly reconstruct?: (request: {
		readonly action: ActionKey;
		readonly args: unknown;
		readonly callID: string;
		readonly signal: AbortSignal;
		/** Leased sealed owners from the session's input index; the backend must prove every borrowed dependency. */
		readonly inputs?: (path?: string) => Iterable<object>;
	}) => Promise<
		| (Partial<Pick<WorldBranch<Output>, "compatibility" | "validate" | "capturedBytes" | "dispose">> & {
				readonly output: Output;
				/** Query-owned proof and storage; dispose after its last reader. Never commits the source output.
				 * A composed query cannot fall back to the source branch's narrower proof. */
				readonly requiresQueryValidation?: true;
		  })
		| undefined
	>;
	/** Shared adoption returns the sealed output; only exclusive effects may return an updated settlement.
	 * Unknown failures are indeterminate; backends may mark fully restored failures as recoverable. */
	readonly commit: () => Promise<Output>;
	/** Idempotently release every branch-local handle. Must be safe before or after commit. */
	readonly dispose: () => void | Promise<void>;
};
/** Only an actual backend proof can authorize a sealed result; path/event hints cannot replace it. */
export async function validateWorldBranch<Output>(
	branch: Pick<WorldBranch<Output>, "validate"> | undefined,
	reuse: WorldReuseStrategy,
): Promise<ResourceValidation> {
	try {
		const validation: ResourceValidation = branch?.validate
			? await branch.validate()
			: branch && reuse === "exclusive_branch"
				? { status: "valid", metrics: zeroValidationMetrics() }
				: {
						status: "indeterminate",
						cause: cause("freshness", "validation_unavailable"),
						metrics: zeroValidationMetrics(),
					};
		return immutableSnapshot(validation);
	} catch (error) {
		return immutableSnapshot({
			status: "indeterminate",
			cause: cause("freshness", "validation_failed", errorMessage(error)),
			metrics: zeroValidationMetrics(),
		});
	}
}
/** Pre-execution evidence that can seal one externally executed authoritative result. */
export interface WorldResultCapture<Output> {
	/** The capture retains inputs only; it cannot authorize reuse of the captured action's output. */
	readonly inputsOnly?: true;
	/** Revocable inputs available during execution and sealable after failure; does not grant output authority. */
	readonly inputSource?: object;
	/** Transfer completed work at most once. Only input captures may seal without a successful output. */
	readonly seal: (output?: Output) => WorldBranch<Output> | Promise<WorldBranch<Output>>;
	/** Release an unsealed baseline. Idempotent; a sealed branch owns its own cleanup. */
	readonly dispose: () => void | Promise<void>;
}
export interface CapturedExecutionWorldResult<Output> extends Pick<
	WorldBranch<Output>,
	"computationDependencies"
> {
	readonly route: SpeculativeExecutionRoute;
	readonly capture: WorldResultCapture<Output>;
}
export interface ExecutionWorldPreparation {
	readonly cwd: string;
	readonly signal?: AbortSignal;
}
export type ExecutionWorldHealthState = "registered" | "ready" | "unavailable";
export interface ExecutionWorldStorageSnapshot {
	readonly entries: number;
	readonly maxEntries: number;
	readonly bytes: number;
	readonly maxBytes: number;
	readonly orphanArtifacts?: number;
	readonly overBudget: boolean;
}
export interface ExecutionWorldStorageControl {
	/** Applies the retention policy synchronously; reclamation remains an explicit maintenance action. */
	readonly configure: (limits: Pick<ExecutionWorldStorageSnapshot, "maxEntries" | "maxBytes">) => void;
	readonly maintain: (operation: "gc" | "clear") => Promise<{
		readonly removedEntries: number;
		readonly removedArtifacts: number;
		readonly removedBytes: number;
	}>;
}
/** Backend-owned health independent from whether one concrete action has selected this world. */
export interface ExecutionWorldDiagnosticReport {
	readonly state: ExecutionWorldHealthState;
	readonly detail: string;
	readonly storage?: ExecutionWorldStorageSnapshot;
}
export interface ExecutionWorldDiagnosticsContext extends ExecutionWorldPreparation {
	/** Re-run backend capability probes instead of using their cached result. */
	readonly refresh?: boolean;
}
/** Source-neutral world status; capabilities describe speculative execution, with observation reported separately below. */
export interface ExecutionWorldDiagnosticSnapshot extends ExecutionWorldOperationDiagnostic {
	readonly id: string;
	readonly scope: ExecutionWorldScope;
	readonly isolation: SpeculativeExecution;
	/** Independently probed Actor-authorized observation, when the world provides it. */
	readonly observation?: ExecutionWorldOperationDiagnostic;
}
export interface ExecutionWorldOperationDiagnostic
	extends ExecutionWorldDiagnosticReport, Pick<ExecutionWorldOperation, "capabilities" | "tools"> {}
/** Fast, side-effect-free view of whether the registered worlds can route one effect contract. */
export interface ExecutionCapabilityStatus {
	readonly state: ExecutionWorldHealthState;
	readonly primary?: ExecutionWorldDiagnosticSnapshot;
	readonly candidates: readonly ExecutionWorldDiagnosticSnapshot[];
}
export function executionCapabilityStatus(
	requirements: EffectRequirements,
	worlds: readonly ExecutionWorldDiagnosticSnapshot[],
	operation: "speculation" | "observation" = "speculation",
	tool?: string,
): ExecutionCapabilityStatus {
	const candidates = (["runtime", "fallback"] as const).flatMap((scope) =>
		worlds.flatMap((world) => {
			if (world.scope !== scope) return [];
			const diagnostic = operation === "speculation" ? world : world.observation;
			return diagnostic &&
				supportsTool(diagnostic, tool) &&
				effectCapabilitiesCover(diagnostic.capabilities, requirements)
				? [{ ...world, ...diagnostic }]
				: [];
		}),
	);
	const primary =
		candidates.find((world) => world.state === "ready") ??
		candidates.find((world) => world.state === "registered") ??
		candidates[0];
	return Object.freeze({
		state: primary?.state ?? "unavailable",
		...(primary ? { primary } : {}),
		candidates: Object.freeze(candidates),
	});
}
export interface ExecutionWorldOperation {
	/** Atomic effects this operation can safely contain, observe, virtualize, or validate. */
	readonly capabilities: EffectCapabilities;
	/** Optional provider-native tool scope; omitted means every covered tool, and effect capabilities remain the safety boundary. */
	readonly tools?: readonly string[];
	/** Stable identity of the concrete provider used for route-local reuse. */
	readonly fingerprint?: (request: ExecutionWorldRequest) => string | Promise<string>;
	/** Idempotent and concurrency-safe; reject while unavailable so resolution can try the next world. */
	readonly prepare?: (input: ExecutionWorldPreparation) => Promise<void>;
	/** Read-only health and diagnostics. It must not weaken or bypass route preparation. */
	readonly diagnostics?: (
		input: ExecutionWorldDiagnosticsContext,
	) => ExecutionWorldDiagnosticReport | Promise<ExecutionWorldDiagnosticReport>;
}
export interface ExecutionWorldSpeculation<Context, Output> extends ExecutionWorldOperation {
	/** Explicit ancestry interoperability across routes; implementations must authenticate the checkpoint and workspace. */
	readonly acceptsCheckpoint?: (checkpoint: WorldCheckpoint, cwd: string) => boolean;
	readonly dependsOn?: (action: ActionKey, parent: ActionKey) => Promise<boolean>;
	readonly execute: (context: Context) => Promise<WorldBranch<Output>>;
}
export interface ExecutionWorldObservation<Context, Output> extends ExecutionWorldOperation {
	/** Observe inputs without claiming authority over the action's other effects or output. */
	readonly inputsOnly?: (request: ExecutionWorldRequest) => boolean;
	/** Capture freshness before a host-authoritative execution without executing the tool again. */
	readonly capture: (context: Context) => Promise<WorldResultCapture<Output>>;
}
interface ExecutionWorldLifecycle<Context, Output, ObservationContext> {
	readonly id: string;
	/** Optional persistent storage capability, independent from tool or action syntax. */
	readonly storage?: ExecutionWorldStorageControl;
	/** Pre-Actor execution and Actor-authorized observation deliberately have independent authority. */
	readonly speculation?: ExecutionWorldSpeculation<Context, Output>;
	readonly observation?: ExecutionWorldObservation<ObservationContext, Output>;
	/** Observe proven internal work inside exactly one native Actor call; never seals its whole result. */
	readonly observeOperations?: <Value>(
		request: {
			readonly action: ActionKey | undefined;
			readonly scope: ExecutionScope;
			readonly learn?: boolean;
			readonly inputs?: (path?: string) => Iterable<object>;
			readonly cwd?: string;
			readonly signal?: AbortSignal;
			readonly enabled?: boolean;
			readonly reuseEnabled?: boolean;
		},
		execute: () => Promise<Value>,
		observe: (
			bindings: readonly ExecutionOperationBinding[],
			computations?: readonly TimelineDependency[],
		) => void,
	) => Promise<Value>;
	/** Retire preparation invalidated by an Actor mutation before its successors may launch. */
	readonly actorFallbackSettled?: () => Promise<void>;
	/** Release turn-owned preparation; independently retained branches keep their own leases. */
	readonly finishTurn?: (turnID: string) => Promise<void>;
	/** Abort and drain backend-owned forks and branch cleanup before resolving. */
	readonly dispose?: () => Promise<void>;
}
/** Source-independent lifecycle for isolating, sealing, and committing speculative effects. */
export type ExecutionWorld<Context, Output, ObservationContext = Context> = ExecutionWorldLifecycle<
	Context,
	Output,
	ObservationContext
> &
	(
		| {
				/** A runtime world is preferred when its advertised guarantees cover the operation. */
				readonly scope: "runtime";
				readonly isolation: "runtime_sandbox";
		  }
		| {
				/** A host-local fallback advertises the same source-neutral effect guarantees. */
				readonly scope: "fallback";
				readonly isolation: Exclude<SpeculativeExecution, "runtime_sandbox">;
		  }
	);
export type ExecutionWorldEntry = "speculation" | "observation" | "observeOperations";
/** Trusted backend integration. Qualification tests the concrete isolation provider, not a tool name.
 * Every entry is rechecked; result freshness, effect transactions and ownership remain separate proofs. */
export interface ExecutionWorldQualification {
	readonly operations: readonly ExecutionWorldEntry[];
	readonly verify: (
		input: ExecutionWorldDiagnosticsContext & {
			readonly operation: ExecutionWorldEntry;
		},
	) => Promise<{
		readonly fingerprint: string;
		/** External service state and effects must be denied or excluded from speculative work. */
		readonly externalEffects: "excluded";
	}>;
}
const qualifiedWorlds = new WeakMap<
	object,
	ExecutionWorldQualification & {
		active: boolean;
	}
>();
/** Register an immutable implementation with an actual, repeatable provider qualification probe.
 * A copy is a different implementation and must be registered separately. Registration itself grants no result authority. */
export function registerExecutionWorld<World extends ExecutionWorld<any, any, any>>(
	world: World,
	qualification: ExecutionWorldQualification,
): World {
	const operations = Object.freeze([...new Set(qualification.operations)]);
	if (
		!operations.length ||
		operations.some(
			(operation) =>
				!["speculation", "observation", "observeOperations"].includes(operation) || !world[operation],
		) ||
		typeof qualification.verify !== "function"
	)
		throw new Error("execution world qualification requires concrete entries and a provider probe");
	const entry = {
		operations,
		verify: qualification.verify.bind(qualification),
		active: true,
	};
	const close = world.dispose?.bind(world);
	const snapshot = (operation: ExecutionWorldOperation | undefined) =>
		operation &&
		Object.freeze({
			...operation,
			capabilities:
				operation.capabilities === "all" ? ("all" as const) : Object.freeze([...operation.capabilities]),
			...(operation.tools ? { tools: Object.freeze([...operation.tools]) } : {}),
		});
	let disposed: Promise<void> | undefined;
	const registered = Object.freeze({
		...world,
		speculation: snapshot(world.speculation),
		observation: snapshot(world.observation),
		dispose: () => {
			entry.active = false;
			return (disposed ??= Promise.resolve().then(close));
		},
	}) as unknown as World;
	qualifiedWorlds.set(registered, entry);
	return registered;
}
export function isRegisteredExecutionWorld(world: object, operation: ExecutionWorldEntry): boolean {
	const entry = qualifiedWorlds.get(world);
	return !!entry?.active && entry.operations.includes(operation);
}
async function qualifyExecutionWorld(
	world: object,
	operation: ExecutionWorldEntry,
	input: ExecutionWorldDiagnosticsContext,
): Promise<string | undefined> {
	const entry = qualifiedWorlds.get(world);
	if (!entry) return;
	if (!isRegisteredExecutionWorld(world, operation))
		throw new Error("execution world entry is not registered or has been retired");
	input.signal?.throwIfAborted();
	const evidence = await entry.verify({ ...input, operation });
	input.signal?.throwIfAborted();
	if (
		!entry.active ||
		evidence.externalEffects !== "excluded" ||
		typeof evidence.fingerprint !== "string" ||
		!evidence.fingerprint.trim()
	)
		throw new Error("execution world qualification failed");
	return evidence.fingerprint;
}
type ExecutionWorldAdmission<Context, Output, ObservationContext> = (
	world: ExecutionWorld<Context, Output, ObservationContext>,
	operation: ExecutionWorldEntry,
) => string | undefined;
/** The only authority allowed to resolve, prepare, fork, and dispose speculative tool execution. */
export class ExecutionWorldRouter<Context, Output, ObservationContext = Context> {
	protected readonly lifecycle: RuntimeLifecycleLane;
	private readonly speculationEnabled: (backend: string) => boolean;
	private readonly admission?: ExecutionWorldAdmission<Context, Output, ObservationContext>;
	private readonly worldsByID = new Map<string, ExecutionWorld<Context, Output, ObservationContext>>();
	private readonly routeObservations = new Map<
		string,
		ExecutionWorldDiagnosticReport & {
			readonly cwd: string;
		}
	>();
	private readonly qualifications = new WeakMap<
		SpeculativeExecutionRoute,
		{
			readonly cwd: string;
			readonly fingerprint: string;
		}
	>();
	constructor(
		worlds: readonly ExecutionWorld<Context, Output, ObservationContext>[],
		speculationEnabled: (backend: string) => boolean = () => true,
		lifecycle = new RuntimeLifecycleLane(),
		admission?: ExecutionWorldAdmission<Context, Output, ObservationContext>,
	) {
		this.lifecycle = lifecycle;
		this.speculationEnabled = speculationEnabled;
		this.admission = admission;
		for (const world of new Set(worlds)) {
			if (!world.id.trim()) throw new Error("execution world id must not be empty");
			if (!world.speculation && !world.observation && !world.observeOperations)
				throw new Error(`execution world ${world.id} provides no operation`);
			if (this.worldsByID.has(world.id)) throw new Error(`duplicate execution world ${world.id}`);
			this.worldsByID.set(world.id, world);
		}
	}
	/** Runtime sandbox first, then local fallback; unavailable worlds are skipped. */
	async resolve(
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
	): Promise<SpeculativeExecutionRoute | undefined> {
		return this.lifecycle.admit(() =>
			this.select("speculation", request, preparation, (_world, route) => route),
		);
	}
	fork(route: SpeculativeExecutionRoute, context: Context): Promise<WorldBranch<Output>> {
		const world = this.worldsByID.get(route.backend);
		if (!world || world.scope !== route.scope || world.isolation !== route.isolation)
			throw new Error(`Execution world ${route.backend} is unavailable for ${route.isolation}`);
		const rejected = this.admission?.(world, "speculation");
		if (rejected) throw new Error(rejected);
		if (!this.speculationEnabled(world.id))
			throw new Error(`Execution world ${world.id} is disabled by routing policy`);
		if (!world.speculation)
			throw new Error(`Execution world ${world.id} does not provide speculative execution`);
		return this.lifecycle.admit(async () => {
			const qualified = this.qualifications.get(route);
			if (
				qualifiedWorlds.has(world) &&
				(!qualified ||
					(await qualifyExecutionWorld(world, "speculation", qualified)) !== qualified.fingerprint)
			)
				throw new Error("execution world qualification changed before execution");
			return world.speculation!.execute(context);
		});
	}
	observeOperations<Value>(
		action: ActionKey | undefined,
		scope: ExecutionScope,
		execute: () => Promise<Value>,
		observe: (
			bindings: readonly ExecutionOperationBinding[],
			computations?: readonly TimelineDependency[],
		) => void,
		learn = false,
		inputs?: (path?: string) => Iterable<object>,
		context?: {
			readonly cwd: string;
			readonly signal?: AbortSignal;
			readonly reuseEnabled?: boolean;
		},
	): Promise<Value> {
		for (const world of this.worldsByID.values()) {
			if (!world.observeOperations || this.admission?.(world, "observeOperations")) continue;
			const next = execute;
			execute = async () => {
				try {
					await qualifyExecutionWorld(world, "observeOperations", {
						cwd: context?.cwd ?? "",
						signal: context?.signal,
					});
				} catch {
					return next();
				}
				return world.observeOperations!(
					{ action, scope, learn, inputs, enabled: true, ...context },
					next,
					observe,
				);
			};
		}
		return execute();
	}
	/** Select a capture-capable world and snapshot its baseline before host execution. */
	async captureAuthoritativeResult(
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
		context: ObservationContext,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		return this.lifecycle.admit(() =>
			this.select("observation", request, preparation, async (world, route) => {
				const { output: capture, dependencies: computationDependencies } = await TimelineInterval.collect(
					() => world.observation!.capture(context),
				);
				if (
					(!request.action ||
						!effectCapabilitiesCover(world.observation!.capabilities, request.requirements)) &&
					!capture.inputsOnly
				) {
					await capture.dispose();
					throw new Error("input_only_capture_required");
				}
				return Object.freeze({
					route: capture.inputsOnly ? Object.freeze({ ...route, reuse: "shared_result" as const }) : route,
					capture,
					computationDependencies,
				});
			}),
		);
	}
	dispose(): Promise<void> {
		return this.lifecycle.dispose(async () => {
			const closed = await Promise.allSettled(
				[...this.worldsByID.values()].map(async (world) => world.dispose?.()),
			);
			const failures = closed.flatMap((result) => (result.status === "rejected" ? [result.reason] : []));
			if (failures.length) throw new AggregateError(failures, "Execution world cleanup failed");
		});
	}
	/** Inspect every registered world without attempting a speculative action. */
	async diagnostics(
		input: ExecutionWorldDiagnosticsContext,
	): Promise<readonly ExecutionWorldDiagnosticSnapshot[]> {
		return this.lifecycle.admit(() =>
			Promise.all(
				[...this.worldsByID.values()].map(async (world) => {
					const speculation = await this.diagnose(world, "speculation", input);
					const observation = await this.diagnose(world, "observation", input);
					const primary = (speculation ??
						observation ??
						(await this.diagnose(world, "observeOperations", input)))!;
					return Object.freeze({
						id: world.id,
						scope: world.scope,
						isolation: world.isolation,
						...primary,
						...(observation ? { observation } : {}),
					});
				}),
			),
		);
	}
	private async select<Selected>(
		kind: "speculation" | "observation",
		request: ExecutionWorldRequest,
		preparation: ExecutionWorldPreparation,
		select: (
			world: ExecutionWorld<Context, Output, ObservationContext>,
			route: SpeculativeExecutionRoute,
		) => Selected | undefined | Promise<Selected | undefined>,
	): Promise<Selected | undefined> {
		for (const scope of ["runtime", "fallback"] as const) {
			for (const world of this.worldsByID.values()) {
				if (
					this.admission?.(world, kind) ||
					world.scope !== scope ||
					(request.backend !== undefined && request.backend !== world.id) ||
					(kind === "speculation" && !this.speculationEnabled(world.id))
				)
					continue;
				const operation = world[kind];
				if (!operation) continue;
				try {
					if (!supportsTool(operation, request.action?.tool ?? request.tool)) continue;
					const acceptsCheckpoint = kind === "speculation" && world.speculation?.acceptsCheckpoint;
					if (
						request.parentCheckpoint &&
						(!acceptsCheckpoint || !acceptsCheckpoint(request.parentCheckpoint, preparation.cwd))
					)
						continue;
					if (
						!effectCapabilitiesCover(operation.capabilities, request.requirements) &&
						!(kind === "observation" && world.observation?.inputsOnly?.(request))
					)
						continue;
					const qualified = await qualifyExecutionWorld(world, kind, preparation);
					const identity = (await operation.fingerprint?.(request)) ?? `${world.id}:${world.isolation}`;
					const fingerprint = qualified === undefined ? identity : JSON.stringify([qualified, identity]);
					await operation.prepare?.(preparation);
					this.observeRoute(world.id, kind, preparation.cwd, "ready", "Route prepared successfully");
					const cwd = preparation.cwd;
					const route: SpeculativeExecutionRoute = {
						isolation: world.isolation,
						reuse:
							request.effect === "observation" && !request.parentCheckpoint
								? "shared_result"
								: "exclusive_branch",
						scope,
						backend: world.id,
						fingerprint,
					};
					// Capability stays out of transaction snapshots, diagnostic serialization and copied routes.
					if (acceptsCheckpoint) {
						const owner = new WeakRef(acceptsCheckpoint);
						const lifetime = new WeakRef(this.lifecycle);
						Object.defineProperty(route, "acceptsCheckpoint", {
							value: (checkpoint: WorldCheckpoint) =>
								lifetime.deref()?.sealed === false && owner.deref()?.(checkpoint, cwd) === true,
						});
					}
					if (kind === "speculation" && request.action && world.speculation?.dependsOn) {
						const action = request.action;
						const dependsOn = world.speculation.dependsOn;
						Object.defineProperty(route, "dependsOn", {
							value: (parent: ActionKey) => dependsOn(action, parent),
						});
					}
					Object.freeze(route);
					if (qualified !== undefined)
						this.qualifications.set(route, {
							cwd: preparation.cwd,
							fingerprint: qualified,
						});
					const selected = await select(world, route);
					if (selected !== undefined) return selected;
				} catch (error) {
					if (preparation.signal?.aborted) throw error;
					this.observeRoute(world.id, kind, preparation.cwd, "unavailable", errorMessage(error));
					// Unavailable worlds are skipped in explicit capability order.
				}
			}
		}
		return undefined;
	}
	private async diagnose(
		world: ExecutionWorld<Context, Output, ObservationContext>,
		kind: ExecutionWorldEntry,
		input: ExecutionWorldDiagnosticsContext,
	): Promise<ExecutionWorldOperationDiagnostic | undefined> {
		const operation: ExecutionWorldOperation | undefined =
			kind === "observeOperations" ? world.observeOperations && { capabilities: [] } : world[kind];
		if (!operation) return;
		const rejected =
			this.admission?.(world, kind) ??
			(kind === "speculation" && !this.speculationEnabled(world.id)
				? "Pre-execution disabled by routing policy"
				: undefined);
		if (rejected)
			return Object.freeze({
				capabilities: Object.freeze([]),
				state: "unavailable",
				detail: rejected,
			});
		const key = `${world.id}:${kind}`;
		if (input.refresh) this.routeObservations.delete(key);
		let report: ExecutionWorldDiagnosticReport | undefined;
		try {
			const qualified = input.refresh ? await qualifyExecutionWorld(world, kind, input) : undefined;
			report =
				(await operation.diagnostics?.(input)) ??
				(qualified !== undefined
					? { state: "ready", detail: "Provider qualification succeeded" }
					: undefined);
			if (!report && input.refresh && operation.prepare) {
				await operation.prepare(input);
				report = { state: "ready", detail: "Route prepared successfully" };
			}
		} catch (error) {
			report = { state: "unavailable", detail: errorMessage(error) };
		}
		if (input.refresh && report) this.routeObservations.set(key, { ...report, cwd: input.cwd });
		const route = this.routeObservations.get(key);
		if (route?.cwd === input.cwd && route.state === "unavailable") report = route;
		return Object.freeze({
			capabilities: operation.capabilities,
			...(operation.tools ? { tools: Object.freeze([...operation.tools]) } : {}),
			...(report ??
				(route?.cwd === input.cwd ? route : undefined) ?? {
					state: "registered",
					detail: "Registered; availability is checked during route preparation",
				}),
		});
	}
	private observeRoute(
		id: string,
		kind: "speculation" | "observation",
		cwd: string,
		state: ExecutionWorldHealthState,
		detail: string,
	): void {
		this.routeObservations.set(`${id}:${kind}`, { state, cwd, detail });
	}
}
function supportsTool(
	operation: {
		readonly tools?: readonly string[];
	},
	tool: string | undefined,
): boolean {
	return tool === undefined || operation.tools === undefined || operation.tools.includes(tool);
}
const REUSED_FAILURE = Symbol("reused tool failure");
/**
 * Source-neutral description of one concrete tool operation.
 *
 * Tool adapters resolve schema and host details before entering this boundary. Isolation,
 * validation, reuse, and authoritative execution therefore do not depend on Bash syntax or on
 * Pi's built-in tool names.
 */
export type AuthoritativeToolExecutor<Output> = (operation: ToolOperation) => Promise<Output>;
type AuthoritativeExecutionOutcome<Output> =
	| {
			readonly status: "succeeded";
			readonly output: Output;
	  }
	| {
			readonly status: "failed";
			readonly error: unknown;
	  };
export type AuthoritativeExecutionSettlement<Output> = AuthoritativeExecutionOutcome<Output> & {
	readonly durationMs: number;
	readonly toolExecution: TimelineInterval;
};
export interface AuthoritativeExecutionHooks<Output> {
	readonly computationDependencies?: () => readonly TimelineDependency[];
	/** Optional reuse provider. A poisoned commit propagates; non-commit provider failures fall through. */
	readonly reuse?: () => Promise<Output | undefined>;
	/** Best-effort authoritative observation. Failure never replaces the Actor result or error. */
	readonly settled?: (settlement: AuthoritativeExecutionSettlement<Output>) => void | Promise<void>;
}
/**
 * The sole lifecycle boundary for authoritative and speculative tool execution.
 *
 * Authoritative calls own optional reuse, Actor fallback, timing, and observation around the
 * supplied executor. Speculative calls fork an execution world. Keeping both paths here creates
 * one structural seam for effect transactions, provenance certificates, and persistent reuse.
 */
export class ToolExecutionGateway<Context, Output, ObservationContext = Context> extends ExecutionWorldRouter<
	Context,
	Output,
	ObservationContext
> {
	private readonly transactions = new EffectTransactionCoordinator<Output>();
	override async captureAuthoritativeResult(
		request: ExecutionWorldRequest & {
			readonly callID?: string;
		},
		preparation: ExecutionWorldPreparation,
		context: ObservationContext,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		const captured = await super.captureAuthoritativeResult(request, preparation, context);
		return (
			captured &&
			Object.freeze({
				...captured,
				capture: this.transactions.capture(
					this.transactions.begin({
						tool: request.action?.tool ?? request.tool ?? "",
						callID: request.callID,
						route: captured.route,
					}),
					captured.capture,
				),
			})
		);
	}
	/** Reuse supplied an adopted failure: the Actor receives the error its native run throws, not a native rerun. */
	static reusedFailure(message: string): Error {
		return Object.assign(new Error(message), { [REUSED_FAILURE]: true });
	}
	async executeAuthoritative<AuthoritativeOutput>(
		operation: ToolOperation,
		executor: AuthoritativeToolExecutor<AuthoritativeOutput>,
		hooks: AuthoritativeExecutionHooks<AuthoritativeOutput> = {},
	): Promise<AuthoritativeOutput> {
		return withExecutionScope("scope" in operation ? operation.scope : currentExecutionScope(), () =>
			this.lifecycle.admit(async () => {
				if (hooks.reuse) {
					try {
						const reused = await hooks.reuse();
						if (reused !== undefined) return reused;
					} catch (error) {
						if (
							isPoisonedEffectCommit(error) ||
							(
								error as
									| {
											[REUSED_FAILURE]?: true;
									  }
									| undefined
							)?.[REUSED_FAILURE]
						)
							throw error;
						// Reuse is optional; the supplied Actor executor remains authoritative.
					}
				}
				const { output: outcome, computation: toolExecution } = await TimelineInterval.measure(
					async (): Promise<AuthoritativeExecutionOutcome<AuthoritativeOutput>> => {
						try {
							return {
								status: "succeeded",
								output: await executor(operation),
							};
						} catch (error) {
							return { status: "failed", error };
						}
					},
					() => hooks.computationDependencies?.() ?? [],
				);
				const settlement = Object.freeze({
					...outcome,
					toolExecution,
					durationMs: toolExecution.completedAt - toolExecution.startedAt,
				});
				try {
					await hooks.settled?.(settlement);
				} catch {
					/* Observation cannot replace the original Actor settlement. */
				}
				if (settlement.status === "failed") throw settlement.error;
				return settlement.output;
			}),
		);
	}
	executeSpeculative(
		operation: ToolOperation,
		route: SpeculativeExecutionRoute,
		context: Context,
	): Promise<EffectTransaction<Output>> {
		const attempt = this.transactions.begin({
			tool: operation.tool,
			callID: operation.callID,
			route,
		});
		return this.lifecycle.track(
			this.transactions.execute(attempt, () =>
				withExecutionScope(operation.scope, () => this.fork(route, context)),
			),
		);
	}
}
