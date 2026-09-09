import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import type {
	ActorActionIdentity,
	ActorActionSettlement,
	ActorHitTiming,
	CandidateRejection,
	ExecutionBlockedTiming,
	PredictionIdentity,
	ResolutionCause,
} from "./settlement.ts";
import type { TimelineInterval } from "./task-timing.ts";

export type ActorActionState =
	| { readonly status: "matching" }
	| {
			readonly status: "awaiting_fallback";
			readonly matchedPredictions: readonly PredictionIdentity[];
			readonly executionBlockedAttemptLeadMs?: number;
	  }
	| { readonly status: "settled"; readonly value: ActorActionSettlement };

/** Exactly-once owner spanning Actor interception and the optional fallback execution. */
export class ActorAction {
	readonly identity: ActorActionIdentity;
	readonly tool: string;
	readonly actionKey?: ActionKey;
	private readonly rejections: CandidateRejection[] = [];
	private stateValue: ActorActionState = Object.freeze({ status: "matching" });

	constructor(input: {
		readonly identity: ActorActionIdentity;
		readonly tool: string;
		readonly actionKey?: ActionKey;
	}) {
		this.identity = Object.freeze({ ...input.identity });
		this.tool = input.tool;
		this.actionKey = input.actionKey;
	}

	get state(): ActorActionState {
		return this.stateValue;
	}

	get settlement(): ActorActionSettlement | undefined {
		return this.stateValue.status === "settled" ? this.stateValue.value : undefined;
	}

	reject(candidateID: string, match: ActionKeyMatch, cause: ResolutionCause): boolean {
		if (
			this.stateValue.status !== "matching" ||
			this.rejections.some((rejection) => rejection.candidateID === candidateID)
		) {
			return false;
		}
		this.rejections.push(
			Object.freeze({
				candidateID,
				match: Object.freeze({ ...match }),
				cause: Object.freeze({ ...cause }),
			}),
		);
		return true;
	}

	adopt(
		candidateID: string,
		match: ActionKeyMatch,
		timing: ActorHitTiming,
		toolExecution: TimelineInterval,
		matchedPredictions: readonly PredictionIdentity[] = [],
	): ActorActionSettlement | undefined {
		if (
			this.stateValue.status !== "matching" ||
			this.rejections.some((rejection) => rejection.candidateID === candidateID)
		) {
			return undefined;
		}
		return this.finish({
			actorAction: this.identity,
			tool: this.tool,
			...(this.actionKey ? { actionKeyHash: this.actionKey.hash } : {}),
			matchedPredictions: freezePredictions(matchedPredictions),
			rejections: Object.freeze([...this.rejections]),
			provider: Object.freeze({
				kind: "speculative",
				candidateID,
				match: Object.freeze({ ...match }),
				timing: normalizeTiming(timing),
				toolExecution: normalizeInterval(toolExecution),
			}),
		});
	}

	deferToFallback(
		matchedPredictions: readonly PredictionIdentity[] = [],
		executionBlockedAttemptLeadMs?: number,
	): boolean {
		if (this.stateValue.status !== "matching") return false;
		this.stateValue = Object.freeze({
			status: "awaiting_fallback",
			matchedPredictions: freezePredictions(matchedPredictions),
			...(executionBlockedAttemptLeadMs !== undefined
				? { executionBlockedAttemptLeadMs: finite(executionBlockedAttemptLeadMs) }
				: {}),
		});
		return true;
	}

	settlePreview(
		candidateID: string,
		toolExecution: TimelineInterval,
		matchedPredictions: readonly PredictionIdentity[] = [],
	): ActorActionSettlement | undefined {
		if (this.stateValue.status !== "matching") return undefined;
		const interval = normalizeInterval(toolExecution);
		return this.finish({
			actorAction: this.identity,
			tool: this.tool,
			...(this.actionKey ? { actionKeyHash: this.actionKey.hash } : {}),
			matchedPredictions: freezePredictions(matchedPredictions),
			rejections: Object.freeze([...this.rejections]),
			provider: Object.freeze({
				kind: "actor",
				origin: "preview",
				candidateID,
				durationMs: Math.max(0, interval.completedAt - interval.startedAt),
				isError: false,
				toolExecution: interval,
			}),
		});
	}

	settleActor(
		durationMs: number,
		isError: boolean,
		completedAt = performance.now(),
	): ActorActionSettlement | undefined {
		if (this.stateValue.status !== "awaiting_fallback") return undefined;
		const duration = finite(durationMs);
		const completed = finite(completedAt);
		const executionBlockedTiming =
			this.stateValue.executionBlockedAttemptLeadMs === undefined
				? undefined
				: normalizeExecutionBlockedTiming(this.stateValue.executionBlockedAttemptLeadMs, duration);
		return this.finish({
			actorAction: this.identity,
			tool: this.tool,
			...(this.actionKey ? { actionKeyHash: this.actionKey.hash } : {}),
			matchedPredictions: this.stateValue.matchedPredictions,
			rejections: Object.freeze([...this.rejections]),
			provider: Object.freeze({
				kind: "actor",
				origin: "fallback",
				durationMs: duration,
				isError,
				toolExecution: Object.freeze({ startedAt: Math.max(0, completed - duration), completedAt: completed }),
				...(executionBlockedTiming ? { executionBlockedTiming } : {}),
			}),
		});
	}

	private finish(value: ActorActionSettlement): ActorActionSettlement {
		const settlement = Object.freeze(value);
		this.stateValue = Object.freeze({ status: "settled", value: settlement });
		return settlement;
	}
}

function normalizeInterval(interval: TimelineInterval): TimelineInterval {
	const startedAt = finite(interval.startedAt);
	return Object.freeze({ startedAt, completedAt: Math.max(startedAt, finite(interval.completedAt)) });
}

function normalizeTiming(timing: ActorHitTiming): ActorHitTiming {
	return Object.freeze({
		executionAheadMs: finite(timing.executionAheadMs),
		attemptLeadMs: finite(timing.attemptLeadMs),
		hitLatencyMs: finite(timing.hitLatencyMs),
		...(timing.expectedActorMs !== undefined && Number.isFinite(timing.expectedActorMs)
			? { expectedActorMs: finite(timing.expectedActorMs) } : {}),
	});
}

function normalizeExecutionBlockedTiming(attemptLeadMs: number, durationMs: number): ExecutionBlockedTiming {
	const attemptLead = finite(attemptLeadMs);
	const duration = finite(durationMs);
	const executionAheadMs = Math.min(duration, attemptLead);
	return Object.freeze({
		attemptLeadMs: attemptLead,
		executionAheadMs,
		hitLatencyMs: Math.max(0, duration - executionAheadMs),
	});
}

function freezePredictions(predictions: readonly PredictionIdentity[]): readonly PredictionIdentity[] {
	const unique = new Map(predictions.map((prediction) => [prediction.id, Object.freeze({ ...prediction })]));
	return Object.freeze([...unique.values()]);
}

function finite(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}
