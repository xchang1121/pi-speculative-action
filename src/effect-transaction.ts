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
			const transaction = new SealedEffectTransaction(attempt, branch);
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

class SealedEffectTransaction<Output> implements EffectTransaction<Output> {
	private readonly attempt: MutableEffectTransactionAttempt;
	private readonly branch: WorldBranch<Output>;
	private readonly shared?: { readonly output: Output };
	private validation?: ResourceValidation;
	private validationPromise?: Promise<ResourceValidation>;
	private commitPromise?: Promise<Output>;
	private cleanupPromise?: Promise<void>;
	private readonly reconstructions = new Set<Promise<Output | undefined>>();

	constructor(attempt: MutableEffectTransactionAttempt, branch: WorldBranch<Output>) {
		this.attempt = attempt;
		this.branch = branch;
		if (attempt.descriptor.route.reuse === "shared_result") this.shared = { output: cloneSharedData(branch.output) };
	}

	get transactionID(): string {
		return this.attempt.id;
	}

	get state(): EffectTransactionState {
		return this.attempt.stateValue;
	}

	get latestValidation(): ResourceValidation | undefined {
		return this.validation;
	}

	get output(): Output {
		return this.shared ? structuredClone(this.shared.output) : this.branch.output;
	}

	get backend(): string {
		return this.branch.backend;
	}

	get checkpoint() {
		return this.branch.checkpoint;
	}

	get resources(): readonly string[] {
		return this.branch.resources;
	}

	get capturedBytes(): number {
		return this.branch.capturedBytes;
	}

	get executionMetrics() {
		return this.branch.executionMetrics;
	}

	get compatibility() {
		return this.branch.compatibility;
	}

	get commitMetrics() {
		return this.branch.commitMetrics;
	}

	get reconstruct(): WorldBranch<Output>["reconstruct"] {
		if (!this.branch.reconstruct || this.attempt.descriptor.route.reuse !== "shared_result") return undefined;
		return async (request) => {
			if (this.cleanupPromise || this.validation?.status !== "valid" || !["validated", "committed"].includes(this.state)) return undefined;
			const task = this.branch.reconstruct!(request).then(cloneSharedData);
			this.reconstructions.add(task);
			try { return await task; } finally { this.reconstructions.delete(task); }
		};
	}

	async validate(): Promise<ResourceValidation> {
		if (this.cleanupPromise || ["aborted", "aborting", "poisoned", "failed"].includes(this.attempt.stateValue)) {
			return {
				status: "indeterminate",
				cause: cause("freshness", "transaction_unavailable"),
				metrics: zeroValidationMetrics(),
			};
		}
		if (this.validationPromise) return this.validationPromise;
		const preserveCommitted = this.attempt.stateValue === "committed";
		this.attempt.stateValue = preserveCommitted ? "committed" : "validating";
		const pending = (async () => {
			const validation = await validateWorldBranch(this.branch, this.attempt.descriptor.route.reuse);
			this.validation = validation;
			if (!preserveCommitted && this.attempt.stateValue === "validating") {
				this.attempt.stateValue = validation.status === "valid" ? "validated" : "sealed";
			}
			return validation;
		})();
		this.validationPromise = pending;
		try {
			return await pending;
		} finally {
			if (this.validationPromise === pending) this.validationPromise = undefined;
		}
	}

	async commit(): Promise<Output> {
		// An admitted effect keeps its original settlement, including during/after retirement.
		if (this.commitPromise) return this.shared ? structuredClone(await this.commitPromise) : this.commitPromise;
		if (this.cleanupPromise) {
			throw effectCommitFailure(new Error("effect transaction resources are retired"), "recoverable");
		}
		if (!this.validationPromise && this.validation?.status !== "valid") {
			throw new Error(`effect transaction ${this.transactionID} requires successful validation before commit`);
		}
		// Reserve the entire validation → commit operation before yielding, not just its effect.
		this.commitPromise = (async () => {
			await this.validationPromise;
			if (this.validation?.status !== "valid" || this.attempt.stateValue !== "validated") {
				throw new Error(`effect transaction ${this.transactionID} cannot commit from ${this.attempt.stateValue}`);
			}
			this.attempt.stateValue = "committing";
			try {
				const output = await this.branch.commit();
				this.attempt.stateValue = "committed";
				return this.shared ? this.shared.output : output;
			} catch (error) {
				const failure = effectCommitFailure(
					error,
					"poisoned",
					"effect commit failed without proof that its side effects were restored",
				);
				this.attempt.stateValue = failure.disposition === "poisoned" ? "poisoned" : "failed";
				throw failure;
			}
		})();
		return this.shared ? structuredClone(await this.commitPromise) : this.commitPromise;
	}

	abort(): Promise<void> {
		if (this.cleanupPromise) return this.cleanupPromise;
		if (!["committed", "poisoned"].includes(this.state) && !this.commitPromise) this.attempt.stateValue = "aborting";
		const pending = (async () => {
			try {
				await Promise.allSettled([this.validationPromise, this.commitPromise, ...this.reconstructions]);
				await this.branch.dispose();
			} finally {
				if (!["committed", "poisoned"].includes(this.attempt.stateValue)) this.attempt.stateValue = "aborted";
			}
		})();
		this.cleanupPromise = pending;
		return pending;
	}

	dispose(): Promise<void> {
		return this.abort();
	}
}
