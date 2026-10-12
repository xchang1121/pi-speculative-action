import type { ActionKey } from "./action-semantics.ts";
import type { ToolInvocation } from "./tool-settlement.ts";
const REUSED_FAILURE = Symbol("reused tool failure");
import { TimelineInterval, type TimelineDependency } from "./task-timing.ts";
import { EffectTransactionCoordinator, type EffectTransaction, isPoisonedEffectCommit } from "./effect-transaction.ts";
import { type CapturedExecutionWorldResult, currentExecutionScope, withExecutionScope, type ExecutionScope, ExecutionWorldRouter, type ExecutionWorldRequest, type ExecutionWorldPreparation,
	type SpeculativeExecutionRoute } from "./execution-world.ts";

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
	readonly scope?: ExecutionScope;
	readonly input: unknown;
	readonly signal?: AbortSignal;
	/** Present after the caller has resolved the canonical action identity. */
	readonly action?: ActionKey;
	/** The Actor owns its selected executor even when no reusable action key can be constructed. */
	readonly invocation?: ToolInvocation;
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
export class ToolExecutionGateway<Context, Output, ObservationContext = Context> extends ExecutionWorldRouter<Context, Output, ObservationContext> {
	private readonly transactions = new EffectTransactionCoordinator<Output>();

	override async captureAuthoritativeResult(
		request: ExecutionWorldRequest & { readonly callID?: string },
		preparation: ExecutionWorldPreparation,
		context: ObservationContext,
	): Promise<CapturedExecutionWorldResult<Output> | undefined> {
		const captured = await super.captureAuthoritativeResult(request, preparation, context);
		return captured && Object.freeze({ route: captured.route, capture: this.transactions.capture(
			this.transactions.begin({ tool: request.action?.tool ?? request.tool ?? "", callID: request.callID, route: captured.route }), captured.capture) });
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
		return withExecutionScope("scope" in operation ? operation.scope : currentExecutionScope(), () => this.lifecycle.admit(async () => {
			if (hooks.reuse) {
				try {
					const reused = await hooks.reuse();
					if (reused !== undefined) return reused;
				} catch (error) {
					if (isPoisonedEffectCommit(error) || (error as { [REUSED_FAILURE]?: true } | undefined)?.[REUSED_FAILURE]) throw error;
					// Reuse is optional; the supplied Actor executor remains authoritative.
				}
			}
			const { output: outcome, computation: toolExecution } = await TimelineInterval.measure(async (): Promise<AuthoritativeExecutionOutcome<AuthoritativeOutput>> => {
				try { return { status: "succeeded", output: await executor(operation) }; }
				catch (error) { return { status: "failed", error }; }
			}, () => hooks.computationDependencies?.() ?? []);
			const settlement = Object.freeze({ ...outcome, toolExecution, durationMs: toolExecution.completedAt - toolExecution.startedAt });
			try { await hooks.settled?.(settlement); }
			catch { /* Observation cannot replace the original Actor settlement. */ }
			if (settlement.status === "failed") throw settlement.error;
			return settlement.output;
		}));
	}

	executeSpeculative(
		operation: ToolOperation,
		route: SpeculativeExecutionRoute,
		context: Context,
	): Promise<EffectTransaction<Output>> {
		const attempt = this.transactions.begin({ tool: operation.tool, callID: operation.callID, route });
		return this.lifecycle.track(this.transactions.execute(attempt, () => withExecutionScope(operation.scope, () => this.fork(route, context))));
	}
}
