import type { ActionEffect, ActionKey } from "./action-semantics.ts";
import type { EffectRequirements } from "./effect-model.ts";
import type { ToolInvocation } from "./tool-settlement.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import { TimelineInterval } from "./task-timing.ts";
import {
	EffectTransactionCoordinator,
	type EffectTransaction,
	isPoisonedEffectCommit,
} from "./effect-transaction.ts";
import {
	type CapturedExecutionWorldResult,
	type ExecutionWorldDiagnosticSnapshot,
	type ExecutionWorldDiagnosticsContext,
	ExecutionWorldRouter,
	type ExecutionWorld,
	type ExecutionWorldPreparation,
	type ExecutionOperationBinding,
	type ExecutionScope,
	type SpeculativeExecutionRoute,
} from "./execution-world.ts";

/**
 * Source-neutral description of one concrete tool operation.
 *
 * Tool adapters resolve schema and host details before entering this boundary. Isolation,
 * validation, reuse, and authoritative execution therefore do not depend on Bash syntax or on
 * Pi's built-in tool names.
 */
export interface ToolOperation {
	readonly tool: string;
	/** A backend-issued internal unit cannot fall through to another world's whole-tool executor. */
	readonly backend?: string;
	/** Absent for capability warm-up before a concrete call exists. */
	readonly callID?: string;
	readonly input: unknown;
	readonly signal?: AbortSignal;
	/** Present after the caller has resolved the canonical action identity. */
	readonly action?: ActionKey;
	/** The Actor owns its selected executor even when no reusable action key can be constructed. */
	readonly invocation?: ToolInvocation;
}

/** Dynamic effect requirement used to select an isolation backend. */
export interface ToolExecutionRequirement {
	readonly operation: ToolOperation;
	readonly effect: ActionEffect;
	readonly requirements: EffectRequirements;
}

export type AuthoritativeToolExecutor<Output> = (operation: ToolOperation) => Promise<Output>;

type AuthoritativeExecutionOutcome<Output> =
	| { readonly status: "succeeded"; readonly output: Output }
	| { readonly status: "failed"; readonly error: unknown };

export type AuthoritativeExecutionSettlement<Output> = AuthoritativeExecutionOutcome<Output> & {
	readonly durationMs: number;
	readonly toolExecution: TimelineInterval;
};

export interface AuthoritativeExecutionHooks<Output> {
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
export class ToolExecutionGateway<Context, Output> {
	private readonly router: ExecutionWorldRouter<Context, Output>;
	private readonly transactions = new EffectTransactionCoordinator<Output>();
	private readonly lifecycle = new RuntimeLifecycleLane();

	constructor(worlds: readonly ExecutionWorld<Context, Output>[], speculationEnabled?: (backend: string) => boolean) {
		this.router = new ExecutionWorldRouter(worlds, speculationEnabled, this.lifecycle);
	}

	resolve(
		requirement: ToolExecutionRequirement,
		preparation: ExecutionWorldPreparation,
	): Promise<SpeculativeExecutionRoute | undefined> {
		const { operation, effect, requirements } = requirement;
		return this.router.resolve({ tool: operation.tool, action: operation.action, backend: operation.backend, effect, requirements }, preparation);
	}

	diagnostics(input: ExecutionWorldDiagnosticsContext): Promise<readonly ExecutionWorldDiagnosticSnapshot[]> {
		return this.router.diagnostics(input);
	}

	observeOperations<Value>(action: ActionKey, scope: ExecutionScope, execute: () => Promise<Value>,
		observe: (bindings: readonly ExecutionOperationBinding[]) => void): Promise<Value> {
		return this.router.observeOperations(action, scope, execute, observe);
	}

	captureAuthoritativeResult(
		requirement: ToolExecutionRequirement,
		preparation: ExecutionWorldPreparation,
		context: Context,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		const { operation, effect, requirements } = requirement;
		return this.router.captureAuthoritativeResult(
			{ tool: operation.tool, action: operation.action, effect, requirements },
			preparation,
			context,
		).then((captured) =>
			captured
				? Object.freeze({
						route: captured.route,
						capture: this.transactions.capture(
							this.transactions.begin({ tool: operation.tool, callID: operation.callID, route: captured.route }),
							captured.capture,
						),
					})
				: undefined,
		);
	}

	async executeAuthoritative<AuthoritativeOutput>(
		operation: ToolOperation,
		executor: AuthoritativeToolExecutor<AuthoritativeOutput>,
		hooks: AuthoritativeExecutionHooks<AuthoritativeOutput> = {},
	): Promise<AuthoritativeOutput> {
		return this.lifecycle.admit(async () => {
			if (hooks.reuse) {
				try {
					const reused = await hooks.reuse();
					if (reused !== undefined) return reused;
				} catch (error) {
					if (isPoisonedEffectCommit(error)) throw error;
					// Reuse is optional; the supplied Actor executor remains authoritative.
				}
			}
			const startedAt = performance.now();
			let outcome: AuthoritativeExecutionOutcome<AuthoritativeOutput>;
			try {
				outcome = { status: "succeeded", output: await executor(operation) };
			} catch (error) {
				outcome = { status: "failed", error };
			}
			const toolExecution = new TimelineInterval(startedAt, performance.now());
			const settlement = Object.freeze({ ...outcome, toolExecution, durationMs: toolExecution.completedAt - toolExecution.startedAt });
			try { await hooks.settled?.(settlement); }
			catch { /* Observation cannot replace the original Actor settlement. */ }
			if (settlement.status === "failed") throw settlement.error;
			return settlement.output;
		});
	}

	executeSpeculative(
		operation: ToolOperation,
		route: SpeculativeExecutionRoute,
		context: Context,
	): Promise<EffectTransaction<Output>> {
		const attempt = this.transactions.begin({ tool: operation.tool, callID: operation.callID, route });
		return this.lifecycle.track(this.transactions.execute(attempt, () => this.router.fork(route, context)));
	}

	dispose(): Promise<void> {
		return this.router.dispose();
	}
}
