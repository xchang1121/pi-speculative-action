import { type SpeculativeExecutionRoute, validateWorldBranch, type WorldBranch, type WorldResultCapture } from "./execution-world.ts";
import { cause, type ResolutionCause, type ResourceValidation, zeroValidationMetrics } from "./settlement.ts";
import { cloneSharedData, immutableSnapshot } from "./stable-json.ts";
import { errorMessage } from "./error-utils.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";

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

	constructor(disposition: EffectCommitDisposition, message: string, cause: unknown, resolutionCause?: ResolutionCause) {
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

export function isPoisonedEffectCommit(error: unknown): error is EffectCommitFailure & { readonly disposition: "poisoned" } {
	return error instanceof EffectCommitFailure && error.disposition === "poisoned";
}

/** Share pending/successful effects; only proven recoverable failures permit another attempt. */
export class EffectCommit<Output> {
	private outcome?: Promise<Output>;
	get result(): Promise<Output> | undefined { return this.outcome; }
	run(apply: () => Promise<Output>): Promise<Output> {
		return this.outcome ??= Promise.resolve().then(apply).catch(error => {
			const failure = effectCommitFailure(error, "poisoned");
			if (failure.disposition === "recoverable") this.outcome = undefined;
			throw failure;
		});
	}
}

export interface EffectTransactionDescriptor { readonly tool: string; readonly callID?: string; readonly route: SpeculativeExecutionRoute; }

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
}

interface MutableEffectTransactionAttempt extends Omit<EffectTransactionAttempt, "state"> {
	stateValue: EffectTransactionState;
}

/** Coordinates begin → execute/seal → validate → commit/abort for every execution backend. */
export class EffectTransactionCoordinator<Output> {
	private readonly attempts = new WeakMap<EffectTransactionAttempt, MutableEffectTransactionAttempt>();
	private sequence = 0;

	begin(descriptor: EffectTransactionDescriptor): EffectTransactionAttempt {
		const owned: MutableEffectTransactionAttempt = { id: `tx_${++this.sequence}`, descriptor: immutableSnapshot(descriptor), stateValue: "begun" };
		const attempt = Object.freeze({ id: owned.id, descriptor: owned.descriptor, get state() { return owned.stateValue; } });
		this.attempts.set(attempt, owned);
		return attempt;
	}

	async execute(attempt: EffectTransactionAttempt, executor: () => Promise<WorldBranch<Output>>): Promise<EffectTransaction<Output>> {
		const owned = this.owned(attempt);
		this.transition(owned, "begun", "executing");
		try {
			const branch = await executor();
			this.transition(owned, "executing", "sealing");
			return await this.seal(owned, branch);
		} catch (error) { owned.stateValue = "failed"; throw error; }
	}

	/** Wrap a pre-execution authoritative capture in the same transaction lifecycle. */
	capture(attempt: EffectTransactionAttempt, capture: WorldResultCapture<Output>): WorldResultCapture<Output> {
		const owned = this.owned(attempt);
		const inputsOnly = capture.inputsOnly;
		return Object.freeze({
			...(inputsOnly ? { inputsOnly } : {}),
			get inputSource() { return owned.stateValue === "begun" && inputsOnly ? capture.inputSource : undefined; },
			seal: async (output?: Output) => {
				this.transition(owned, "begun", "sealing");
				try {
					if (!inputsOnly && output === undefined) throw new Error("authoritative_result_missing");
					const branch = await capture.seal(output);
					if (inputsOnly && !branch.inputsOnly) {
						await branch.dispose();
						throw new Error("input_only_capture_required");
					}
					return await this.seal(owned, branch);
				} catch (error) { owned.stateValue = "failed"; await capture.dispose(); throw error; }
			},
			dispose: async () => {
				if (owned.stateValue !== "begun") return;
				owned.stateValue = "aborting";
				try { await capture.dispose(); } finally { owned.stateValue = "aborted"; }
			},
		});
	}

	private async seal(attempt: MutableEffectTransactionAttempt, branch: WorldBranch<Output>): Promise<EffectTransaction<Output>> {
		try {
			const transaction = sealEffectTransaction(attempt, branch);
			this.transition(attempt, "sealing", "sealed");
			return transaction;
		} catch (error) { try { await branch.dispose(); } catch { /* Preserve the sealing failure. */ } throw error; }
	}

	private owned(attempt: EffectTransactionAttempt): MutableEffectTransactionAttempt {
		const mutable = this.attempts.get(attempt);
		if (!mutable) throw new Error("effect transaction belongs to another coordinator");
		return mutable;
	}

	private transition(attempt: MutableEffectTransactionAttempt, expected: EffectTransactionState, next: EffectTransactionState): void {
		if (attempt.stateValue !== expected) throw new Error(`effect transaction ${attempt.id} is ${attempt.stateValue}, expected ${expected}`);
		attempt.stateValue = next;
	}
}

function sealEffectTransaction<Output>(attempt: MutableEffectTransactionAttempt, branch: WorldBranch<Output>): EffectTransaction<Output> {
	if (branch.inputsOnly && "output" in branch) throw new Error("input_only_output_retained");
	const shared = attempt.descriptor.route.reuse === "shared_result";
	const retainedBytes = Object.getOwnPropertyDescriptor(branch, "capturedBytes")?.get?.bind(branch);
	const validateAndCommit = shared ? branch.validateAndCommit?.bind(branch) : undefined;
	const sealed: WorldBranch<Output> = Object.freeze({
		...immutableSnapshot({ backend: branch.backend, resources: branch.resources, inputResources: shared && branch.reconstruct ? branch.inputResources : undefined, capturedBytes: branch.capturedBytes,
			reconstructionScope: shared && branch.reconstruct && !validateAndCommit ? branch.reconstructionScope : undefined,
			executionMetrics: branch.executionMetrics, compatibility: branch.compatibility }),
		// Checkpoints are opaque backend-issued handles; pin the reference without cloning their owner.
		checkpoint: branch.checkpoint, inputSource: shared ? branch.inputSource : undefined,
		invalidateInputs: shared ? branch.invalidateInputs?.bind(branch) : undefined,
		...(branch.inputsOnly ? { inputsOnly: true as const } : { output: shared ? cloneSharedData(branch.output) : branch.output }),
		operations: branch.operations && Object.freeze([...branch.operations]),
		computationDependencies: branch.computationDependencies && Object.freeze([...branch.computationDependencies]),
		validate: validateAndCommit ?? branch.validate?.bind(branch), reconstruct: branch.reconstruct?.bind(branch),
		commit: branch.commit.bind(branch), dispose: branch.dispose.bind(branch),
		takeCommittedInputs: branch.takeCommittedInputs?.bind(branch), takeReadInputs: branch.takeReadInputs?.bind(branch),
	});
	let validation: ResourceValidation | undefined, validationPromise: Promise<ResourceValidation> | undefined;
	const commit = new EffectCommit<Output>();
	let inputTransfer: ReturnType<NonNullable<WorldBranch<Output>["takeCommittedInputs"]>> | undefined;
	const lifecycle = new RuntimeLifecycleLane();
	const queryResources = new Set<() => Promise<void>>();
	const abort = (): Promise<void> => {
		if (!lifecycle.sealed && !["committed", "poisoned"].includes(attempt.stateValue) && !commit.result) attempt.stateValue = "aborting";
		return lifecycle.close(async () => {
			try {
				await lifecycle.drain();
				await Promise.allSettled([...queryResources].map(dispose => dispose()));
				await sealed.dispose();
			} finally {
				if (!["committed", "poisoned"].includes(attempt.stateValue)) attempt.stateValue = "aborted";
			}
		});
	};
	const validate = async (queryProof?: WorldBranch<Output>["validate"]): Promise<ResourceValidation> => {
		// A reserved commit owns its proof window; a later validation must not reset that state.
		if (!lifecycle.sealed && commit.result && attempt.stateValue !== "committed") await Promise.allSettled([commit.result]);
		if (lifecycle.sealed || ["aborted", "aborting", "poisoned", "failed"].includes(attempt.stateValue)) {
			return { status: "indeterminate", cause: cause("freshness", "transaction_unavailable"), metrics: zeroValidationMetrics() };
		}
		// Each request owns a fresh proof after its predecessors, never their earlier observation.
		const pending = lifecycle.serialize(async () => {
			if (!queryProof && ["sealed", "validated"].includes(attempt.stateValue)) attempt.stateValue = "validating";
			const result = await validateWorldBranch(queryProof ? { validate: queryProof } : sealed, attempt.descriptor.route.reuse);
			// A query borrows the same lifetime and validation lane, but cannot authorize the source output.
			if (!queryProof) {
				validation = result;
				if (attempt.stateValue === "validating") attempt.stateValue = result.status === "valid" ? "validated" : "sealed";
			}
			return result;
		});
		validationPromise = pending;
		try { return await pending; } finally { if (validationPromise === pending) validationPromise = undefined; }
	};
	const transaction: EffectTransaction<Output> = {
		...sealed, transactionID: attempt.id,
		get state() { return attempt.stateValue; },
		get latestValidation() { return validation; },
		get capturedBytes() { return retainedBytes ? retainedBytes() : sealed.capturedBytes; },
		takeCommittedInputs: sealed.takeCommittedInputs ? async (maxBytes) => {
			if (lifecycle.sealed || attempt.stateValue !== "committed" || inputTransfer) return undefined;
			return inputTransfer = lifecycle.track(Promise.resolve().then(() => sealed.takeCommittedInputs!(maxBytes)).then(async inputs => {
				if (!lifecycle.sealed) return inputs;
				await inputs?.dispose(); return undefined;
			}));
		} : undefined,
		reconstruct: shared && sealed.reconstruct ? async (request) => {
			// Borrowing sealed inputs grants no commit authority; freshness is checked after evaluation.
			if (lifecycle.sealed || !["sealed", "validating", "validated", "committed"].includes(attempt.stateValue)) return undefined;
			const task = Promise.resolve().then(() => sealed.reconstruct!(request)).then(async (result) => {
				if (!result) return undefined;
				// An atomic validation/commit callback retains its complete proof and effect ownership.
				const proof = !validateAndCommit && result.validate?.bind(result);
				const release = result.dispose?.bind(result);
				let released: Promise<void> | undefined;
				const dispose = () => released ??= Promise.resolve().then(release).finally(() => queryResources.delete(dispose));
				if (release) queryResources.add(dispose);
				try {
					return Object.freeze({ output: cloneSharedData(result.output), capturedBytes: result.capturedBytes, requiresQueryValidation: result.requiresQueryValidation,
						compatibility: proof ? immutableSnapshot(result.compatibility) : undefined,
						...(proof ? { validate: () => validate(() => released ? Promise.resolve({ status: "indeterminate", cause: cause("freshness", "query_disposed"), metrics: zeroValidationMetrics() }) : proof()) } : {}),
						...(release ? { dispose } : {}) });
				} catch (error) { await dispose().catch(() => {}); throw error; }
			});
			return lifecycle.track(task);
		} : undefined,
		validate: () => validate(),
		commit: async () => {
			if (sealed.inputsOnly) throw effectCommitFailure(new Error("input_only_branch"), "recoverable");
			// An admitted effect keeps its original settlement, including during/after retirement.
			if (commit.result) return shared ? cloneSharedData(await commit.result) : commit.result;
			if (lifecycle.sealed) throw effectCommitFailure(new Error("effect transaction resources are retired"), "recoverable");
			if (!validationPromise && validation?.status !== "valid") {
				throw new Error(`effect transaction ${attempt.id} requires successful validation before commit`);
			}
			// Reserve the entire validation → commit operation before yielding, not just its effect.
			const pending = lifecycle.track(commit.run(async () => {
				try {
					await validationPromise;
					if (validation?.status !== "valid" || attempt.stateValue !== "validated") {
						throw effectCommitFailure(new Error(`effect transaction ${attempt.id} cannot commit from ${attempt.stateValue}`),
							"recoverable", undefined, validation?.status !== "valid" ? validation?.cause : undefined);
					}
					attempt.stateValue = "committing";
					const output = await sealed.commit();
					attempt.stateValue = "committed";
					return shared ? sealed.output : output;
				} catch (error) {
					const failure = effectCommitFailure(error, "poisoned", "effect commit failed without proof that its side effects were restored");
					attempt.stateValue = failure.disposition === "poisoned" ? "poisoned" : "sealed";
					validation = undefined;
					throw failure;
				}
			}));
			return shared ? cloneSharedData(await pending) : pending;
		},
		abort, dispose: abort,
	};
	if (!sealed.inputsOnly) Object.defineProperty(transaction, "output", { get: () => shared ? cloneSharedData(sealed.output) : sealed.output });
	return Object.freeze(transaction);
}
