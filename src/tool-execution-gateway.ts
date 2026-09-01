import type { ActionEffect, ActionKey } from "./action-semantics.ts";
import type { EffectRequirements } from "./effect-model.ts";
import {
	EffectTransactionCoordinator,
	type EffectTransaction,
} from "./effect-transaction.ts";
import {
	type CapturedExecutionWorldResult,
	type ExecutionWorldDiagnosticSnapshot,
	type ExecutionWorldDiagnosticsContext,
	ExecutionWorldRouter,
	type ExecutionWorld,
	type ExecutionWorldPreparation,
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
	/** Absent for capability warm-up before a concrete call exists. */
	readonly callID?: string;
	readonly input: unknown;
	readonly signal?: AbortSignal;
	/** Present after the caller has resolved the canonical action identity. */
	readonly action?: ActionKey;
}

/** Dynamic effect requirement used to select an isolation backend. */
export interface ToolExecutionRequirement {
	readonly operation: ToolOperation;
	readonly effect: ActionEffect;
	readonly requirements: EffectRequirements;
}

export type AuthoritativeToolExecutor<Output> = (operation: ToolOperation) => Promise<Output>;
export type ToolExecutionContextFactory<Context> = (operation: ToolOperation) => Context;

export type AuthoritativeExecutionSettlement<Output> =
	| { readonly status: "succeeded"; readonly output: Output; readonly durationMs: number }
	| { readonly status: "failed"; readonly error: unknown; readonly durationMs: number };

export interface AuthoritativeExecutionHooks<Output> {
	/** Optional speculative/cache provider. Failure is isolated and falls through to the Actor executor. */
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

	constructor(worlds: readonly ExecutionWorld<Context, Output>[]) {
		this.router = new ExecutionWorldRouter(worlds);
	}

	resolve(
		requirement: ToolExecutionRequirement,
		preparation: ExecutionWorldPreparation,
	): Promise<SpeculativeExecutionRoute | undefined> {
		const { operation, effect, requirements } = requirement;
		return this.router.resolve(
			{
				effect,
				requirements,
				...(operation.action ? { action: operation.action } : {}),
			},
			preparation,
		);
	}

	diagnostics(input: ExecutionWorldDiagnosticsContext): Promise<readonly ExecutionWorldDiagnosticSnapshot[]> {
		return this.router.diagnostics(input);
	}

	captureAuthoritativeResult(
		requirement: ToolExecutionRequirement,
		preparation: ExecutionWorldPreparation,
		context: ToolExecutionContextFactory<Context>,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		const { operation, effect, requirements } = requirement;
		return this.router.captureAuthoritativeResult(
			{
				effect,
				requirements,
				...(operation.action ? { action: operation.action } : {}),
			},
			preparation,
			context(operation),
		).then((captured) =>
			captured
				? Object.freeze({
						route: captured.route,
						capture: this.transactions.capture(
							this.transactions.begin(descriptor(operation, captured.route)),
							captured.capture,
						),
					})
				: undefined,
		);
	}

	executeAuthoritative<AuthoritativeOutput>(
		operation: ToolOperation,
		executor: AuthoritativeToolExecutor<AuthoritativeOutput>,
		hooks: AuthoritativeExecutionHooks<AuthoritativeOutput> = {},
	): Promise<AuthoritativeOutput> {
		return executeAuthoritativeLifecycle(operation, executor, hooks);
	}

	executeSpeculative(
		operation: ToolOperation,
		route: SpeculativeExecutionRoute,
		context: ToolExecutionContextFactory<Context>,
	): Promise<EffectTransaction<Output>> {
		const attempt = this.transactions.begin(descriptor(operation, route));
		return this.transactions.execute(attempt, () => this.router.fork(route, context(operation)));
	}

	dispose(): Promise<void> {
		return this.router.dispose();
	}
}

async function executeAuthoritativeLifecycle<Output>(
	operation: ToolOperation,
	executor: AuthoritativeToolExecutor<Output>,
	hooks: AuthoritativeExecutionHooks<Output>,
): Promise<Output> {
	if (hooks.reuse) {
		try {
			const reused = await hooks.reuse();
			if (reused !== undefined) return reused;
		} catch {
			// Reuse is optional; the supplied Actor executor remains authoritative.
		}
	}
	const startedAt = performance.now();
	try {
		const output = await executor(operation);
		await observeAuthoritativeSettlement(hooks, {
			status: "succeeded",
			output,
			durationMs: Math.max(0, performance.now() - startedAt),
		});
		return output;
	} catch (error) {
		await observeAuthoritativeSettlement(hooks, {
			status: "failed",
			error,
			durationMs: Math.max(0, performance.now() - startedAt),
		});
		throw error;
	}
}

async function observeAuthoritativeSettlement<Output>(
	hooks: AuthoritativeExecutionHooks<Output>,
	settlement: AuthoritativeExecutionSettlement<Output>,
): Promise<void> {
	try {
		await hooks.settled?.(settlement);
	} catch {
		// Observation is optional and cannot alter the authoritative execution contract.
	}
}

function descriptor(operation: ToolOperation, route: SpeculativeExecutionRoute) {
	return {
		tool: operation.tool,
		...(operation.callID ? { callID: operation.callID } : {}),
		route,
	};
}
