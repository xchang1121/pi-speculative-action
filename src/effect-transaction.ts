import type { SpeculativeExecutionRoute, WorldBranch, WorldResultCapture } from "./execution-world.ts";
import { cause, type ResourceValidation, zeroValidationMetrics } from "./settlement.ts";

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
	| "failed";

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

interface MutableEffectTransactionAttempt extends EffectTransactionAttempt {
	stateValue: EffectTransactionState;
}

/** Coordinates begin → execute/seal → validate → commit/abort for every execution backend. */
export class EffectTransactionCoordinator<Output> {
	private readonly attempts = new WeakSet<MutableEffectTransactionAttempt>();
	private sequence = 0;

	begin(descriptor: EffectTransactionDescriptor): EffectTransactionAttempt {
		const attempt = new TransactionAttempt(`tx_${++this.sequence}`, descriptor);
		this.attempts.add(attempt);
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
			return this.seal(owned, branch);
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
					return this.seal(owned, await capture.seal(output));
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

	private seal(
		attempt: MutableEffectTransactionAttempt,
		branch: WorldBranch<Output>,
	): EffectTransaction<Output> {
		this.transition(attempt, "sealing", "sealed");
		return new SealedEffectTransaction(attempt, branch);
	}

	private owned(attempt: EffectTransactionAttempt): MutableEffectTransactionAttempt {
		const mutable = attempt as MutableEffectTransactionAttempt;
		if (!this.attempts.has(mutable)) throw new Error("effect transaction belongs to another coordinator");
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

class TransactionAttempt implements MutableEffectTransactionAttempt {
	readonly id: string;
	readonly descriptor: EffectTransactionDescriptor;
	stateValue: EffectTransactionState = "begun";

	constructor(id: string, descriptor: EffectTransactionDescriptor) {
		this.id = id;
		this.descriptor = Object.freeze({ ...descriptor });
	}

	get state(): EffectTransactionState {
		return this.stateValue;
	}
}

class SealedEffectTransaction<Output> implements EffectTransaction<Output> {
	private readonly attempt: MutableEffectTransactionAttempt;
	private readonly branch: WorldBranch<Output>;
	private validation?: ResourceValidation;
	private validationPromise?: Promise<ResourceValidation>;
	private commitPromise?: Promise<Output>;
	private cleanupPromise?: Promise<void>;

	constructor(attempt: MutableEffectTransactionAttempt, branch: WorldBranch<Output>) {
		this.attempt = attempt;
		this.branch = branch;
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
		return this.branch.output;
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

	watch(onInvalidated: (changedPath?: string) => void): void {
		this.branch.watch?.(onInvalidated);
	}

	async validate(): Promise<ResourceValidation> {
		if (this.validationPromise) return this.validationPromise;
		if (["aborted", "aborting", "failed"].includes(this.attempt.stateValue)) {
			return {
				status: "indeterminate",
				cause: cause("freshness", "transaction_unavailable"),
				metrics: zeroValidationMetrics(),
			};
		}
		const preserveCommitted = this.attempt.stateValue === "committed";
		this.attempt.stateValue = preserveCommitted ? "committed" : "validating";
		const pending = (async () => {
			let validation: ResourceValidation;
			try {
				validation = this.branch.validate
					? await this.branch.validate()
					: { status: "valid", metrics: zeroValidationMetrics() };
			} catch (error) {
				validation = {
					status: "indeterminate",
					cause: cause(
						"freshness",
						"validation_failed",
						error instanceof Error ? error.message : String(error),
					),
					metrics: zeroValidationMetrics(),
				};
			}
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
		if (this.commitPromise) return this.commitPromise;
		if (this.attempt.stateValue === "committed") return this.branch.output;
		if (this.validationPromise) await this.validationPromise;
		if (this.validation?.status !== "valid") {
			throw new Error(`effect transaction ${this.transactionID} requires successful validation before commit`);
		}
		if (this.attempt.stateValue !== "validated") {
			throw new Error(`effect transaction ${this.transactionID} cannot commit from ${this.attempt.stateValue}`);
		}
		this.attempt.stateValue = "committing";
		const pending = (async () => {
			try {
				const output = await this.branch.commit();
				this.attempt.stateValue = "committed";
				return output;
			} catch (error) {
				this.attempt.stateValue = "failed";
				throw error;
			}
		})();
		this.commitPromise = pending;
		return pending;
	}

	abort(): Promise<void> {
		if (this.cleanupPromise) return this.cleanupPromise;
		const committed = this.attempt.stateValue === "committed";
		const committing = this.commitPromise;
		const validating = this.validationPromise;
		if (!committed && !committing) this.attempt.stateValue = "aborting";
		const pending = (async () => {
			try {
				await validating?.catch(() => undefined);
				await committing?.catch(() => undefined);
				await this.branch.dispose();
			} finally {
				if (this.attempt.stateValue !== "committed") this.attempt.stateValue = "aborted";
			}
		})();
		this.cleanupPromise = pending;
		return pending;
	}

	dispose(): Promise<void> {
		return this.abort();
	}
}
