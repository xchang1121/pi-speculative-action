import type { ActionEffect, ActionKey } from "./action-semantics.ts";
import type { EffectRequirements } from "./effect-model.ts";
import {
	EffectTransactionCoordinator,
	type EffectTransaction,
} from "./effect-transaction.ts";
import {
	type CapturedExecutionWorldResult,
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

/**
 * The sole lifecycle boundary for authoritative and speculative tool execution.
 *
 * Authoritative calls currently execute through the supplied Pi executor, while speculative
 * calls fork an execution world. Keeping both paths here creates one structural seam for later
 * effect transactions, provenance certificates, and persistent reuse.
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
	): Promise<AuthoritativeOutput> {
		return executor(operation);
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

function descriptor(operation: ToolOperation, route: SpeculativeExecutionRoute) {
	return {
		tool: operation.tool,
		...(operation.callID ? { callID: operation.callID } : {}),
		route,
	};
}
