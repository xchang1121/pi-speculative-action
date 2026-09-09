import { type SpeculativeExecutionRoute, validateWorldBranch, type WorldBranch, type WorldResultCapture } from "./execution-world.ts";
import { cause, type ResolutionCause, type ResourceValidation, zeroValidationMetrics } from "./settlement.ts";
import { cloneSharedData, immutableSnapshot } from "./stable-json.ts";

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
	message = error instanceof Error ? error.message : String(error),
	resolutionCause?: ResolutionCause,
): EffectCommitFailure {
	return error instanceof EffectCommitFailure
		? error
		: new EffectCommitFailure(disposition, message, error, resolutionCause);
}

export function isPoisonedEffectCommit(
	error: unknown,
): error is EffectCommitFailure & { readonly disposition: "poisoned" } {
	return error instanceof EffectCommitFailure && error.disposition === "poisoned";
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

/**
 * A sealed effect transaction presented to reuse policy.
 *
 * It remains structurally compatible with WorldBranch while making validation mandatory and
 * exposing one common abort operation. This lets existing isolation backends stay small while
 * the gateway owns the safety-critical lifecycle.
 */
export interface EffectTransaction<Output> extends WorldBranch<Output> {
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
		let consumed = false;
		return Object.freeze({
			seal: async (output: Output) => {
				if (consumed) throw new Error("effect transaction capture is already consumed");
				consumed = true;
				this.transition(owned, "begun", "sealing");
				try {
					return await this.seal(owned, await capture.seal(output));
				} catch (error) {
					owned.stateValue = "failed";
					await capture.dispose();
					throw error;
				}
			},
			dispose: async () => {
				if (consumed) return;
				consumed = true;
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
			try { await branch.dispose(); } catch { /* Preserve the sealing failure. */ }
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
		if (attempt.stateValue !== expected) {
			throw new Error(`effect transaction ${attempt.id} is ${attempt.stateValue}, expected ${expected}`);
		}
		attempt.stateValue = next;
	}
}

function sealEffectTransaction<Output>(attempt: MutableEffectTransactionAttempt, branch: WorldBranch<Output>): EffectTransaction<Output> {
	const shared = attempt.descriptor.route.reuse === "shared_result";
	const sealed: WorldBranch<Output> = Object.freeze({
		...immutableSnapshot({ backend: branch.backend, resources: branch.resources, capturedBytes: branch.capturedBytes,
			executionMetrics: branch.executionMetrics, compatibility: branch.compatibility }),
		// Checkpoints are opaque backend-issued handles; pin the reference without cloning their owner.
		checkpoint: branch.checkpoint, output: shared ? cloneSharedData(branch.output) : branch.output,
		validate: branch.validate?.bind(branch), reconstruct: branch.reconstruct?.bind(branch),
		commit: branch.commit.bind(branch), dispose: branch.dispose.bind(branch),
	});
	let validation: ResourceValidation | undefined, validationPromise: Promise<ResourceValidation> | undefined;
	let commitPromise: Promise<Output> | undefined, cleanupPromise: Promise<void> | undefined;
	const reconstructions = new Set<Promise<Output | undefined>>();
	const abort = (): Promise<void> => {
		if (cleanupPromise) return cleanupPromise;
		if (!["committed", "poisoned"].includes(attempt.stateValue) && !commitPromise) attempt.stateValue = "aborting";
		cleanupPromise = (async () => {
			try {
				await Promise.allSettled([validationPromise, commitPromise, ...reconstructions]);
				await sealed.dispose();
			} finally {
				if (!["committed", "poisoned"].includes(attempt.stateValue)) attempt.stateValue = "aborted";
			}
		})();
		return cleanupPromise;
	};
	return Object.freeze<EffectTransaction<Output>>({
		...sealed, transactionID: attempt.id,
		get state() { return attempt.stateValue; },
		get latestValidation() { return validation; },
		get output() { return shared ? cloneSharedData(sealed.output) : sealed.output; },
		// Commit telemetry is produced later, unlike sealed execution/compatibility evidence.
		get commitMetrics() { return immutableSnapshot(branch.commitMetrics); },
		reconstruct: shared && sealed.reconstruct ? async (request) => {
			// Borrowing sealed inputs grants no commit authority; freshness is checked after evaluation.
			if (cleanupPromise || !["sealed", "validating", "validated", "committed"].includes(attempt.stateValue)) return undefined;
			const task = Promise.resolve().then(() => sealed.reconstruct!(request)).then(cloneSharedData);
			reconstructions.add(task);
			try { return await task; } finally { reconstructions.delete(task); }
		} : undefined,
		validate: async () => {
			// A reserved commit owns its proof window; a later validation must not reset that state.
			if (!cleanupPromise && commitPromise && attempt.stateValue !== "committed") await Promise.allSettled([commitPromise]);
			if (cleanupPromise || ["aborted", "aborting", "poisoned", "failed"].includes(attempt.stateValue)) {
				return { status: "indeterminate", cause: cause("freshness", "transaction_unavailable"), metrics: zeroValidationMetrics() };
			}
			if (validationPromise) return validationPromise;
			const preserveCommitted = attempt.stateValue === "committed";
			attempt.stateValue = preserveCommitted ? "committed" : "validating";
			const pending = Promise.resolve().then(async () => {
				validation = await validateWorldBranch(sealed, attempt.descriptor.route.reuse);
				if (!preserveCommitted && attempt.stateValue === "validating") {
					attempt.stateValue = validation.status === "valid" ? "validated" : "sealed";
				}
				return validation;
			});
			validationPromise = pending;
			try { return await pending; } finally { if (validationPromise === pending) validationPromise = undefined; }
		},
		commit: async () => {
			// An admitted effect keeps its original settlement, including during/after retirement.
			if (commitPromise) return shared ? cloneSharedData(await commitPromise) : commitPromise;
			if (cleanupPromise) throw effectCommitFailure(new Error("effect transaction resources are retired"), "recoverable");
			if (!validationPromise && validation?.status !== "valid") {
				throw new Error(`effect transaction ${attempt.id} requires successful validation before commit`);
			}
			// Reserve the entire validation → commit operation before yielding, not just its effect.
			commitPromise = (async () => {
				await validationPromise;
				if (validation?.status !== "valid" || attempt.stateValue !== "validated") {
					throw new Error(`effect transaction ${attempt.id} cannot commit from ${attempt.stateValue}`);
				}
				attempt.stateValue = "committing";
				try {
					const output = await sealed.commit();
					attempt.stateValue = "committed";
					return shared ? sealed.output : output;
				} catch (error) {
					const failure = effectCommitFailure(error, "poisoned", "effect commit failed without proof that its side effects were restored");
					attempt.stateValue = failure.disposition === "poisoned" ? "poisoned" : "failed";
					throw failure;
				}
			})();
			return shared ? cloneSharedData(await commitPromise) : commitPromise;
		},
		abort, dispose: abort,
	});
}
