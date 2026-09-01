import { isDeepStrictEqual } from "node:util";
import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import type { CandidateExecutionState } from "./candidate-execution.ts";
import type {
	MaterializedPlan,
	PlanAction,
	PlanActionDependency,
	PlanActionDependencyCondition,
	PlanDelta,
	PlanProposal,
	PlanUpdate,
} from "./plan-proposal.ts";
import type {
	ActorActionIdentity,
	PlanActionIdentity,
	PredictionAdoption,
	PredictionIdentity,
	PredictionSettlement,
	ResolutionCause,
} from "./settlement.ts";

type PlanNodeExecution =
	| { readonly status: "deferred" }
	| { readonly status: "execution_blocked"; readonly cause: ResolutionCause }
	| { readonly status: "scheduled" }
	| { readonly status: "queued"; readonly candidateID: string }
	| { readonly status: "running"; readonly candidateID: string }
	| { readonly status: "succeeded"; readonly candidateID: string }
	| { readonly status: "failed" | "cancelled"; readonly cause: ResolutionCause; readonly candidateID?: string };

export type PredictionOpportunityState =
	| { readonly status: "pending" }
	| {
			readonly status: "matching";
			readonly actorAction: ActorActionIdentity;
			readonly relation: ActionKeyMatch;
	  }
	| { readonly status: "settled"; readonly settlement: PredictionSettlement };

export type PlanNodeReadiness = "ready" | "waiting" | "blocked" | "settled";

interface PlanRuntimeNodeBase {
	readonly identity: PlanActionIdentity;
	readonly proposalID: string;
	readonly source: string;
	readonly revision: number;
	readonly action: PlanAction;
	readonly actionKey?: ActionKey;
	readonly anchorDecisionSeq: number;
	readonly earliestDecisionSeq: number;
	readonly expectedDecisionSeq: number;
	readonly latestDecisionSeq: number;
	readonly criticalPathMs: number;
	readonly execution: PlanNodeExecution;
	readonly readiness: PlanNodeReadiness;
}

export type PlanRuntimeNode = PlanRuntimeNodeBase & {
	readonly prediction: PredictionIdentity;
	readonly predictionState: PredictionOpportunityState;
};

export type PredictionPlanRuntimeNode = PlanRuntimeNode;

export interface RetiredPlanNode {
	readonly node: PlanRuntimeNode;
	readonly opportunity: PredictionOpportunity;
}

export type PlanRuntimePromotion =
	| { readonly status: "scheduled"; readonly node: PlanRuntimeNode }
	| { readonly status: "waiting" | "blocked" | "settled" | "already_dispatched" | "missing" };

export type PlanRuntimeUpdateResult =
	| {
			readonly accepted: true;
			readonly plan: MaterializedPlan;
			readonly upserted: readonly PlanAction[];
			readonly removed: readonly string[];
			readonly retired: readonly RetiredPlanNode[];
	  }
	| {
			readonly accepted: false;
			readonly reason:
				| "invalid_identity"
				| "invalid_revision"
				| "source_mismatch"
				| "proposal_missing"
				| "stale_revision"
				| "duplicate_action"
				| "invalid_action"
				| "invalid_dependency";
	  };

/** Exactly-once owner of one source prediction's Actor-visible outcome. */
export class PredictionOpportunity {
	readonly identity: PredictionIdentity;
	private stateValue: PredictionOpportunityState = { status: "pending" };

	constructor(identity: PredictionIdentity) {
		this.identity = Object.freeze({ ...identity });
	}

	get state(): PredictionOpportunityState {
		return this.stateValue;
	}

	get settlement(): PredictionSettlement | undefined {
		return this.stateValue.status === "settled" ? this.stateValue.settlement : undefined;
	}

	claim(actorAction: ActorActionIdentity, relation: ActionKeyMatch): boolean {
		if (this.stateValue.status !== "pending") return false;
		this.stateValue = Object.freeze({
			status: "matching",
			actorAction: Object.freeze({ ...actorAction }),
			relation: Object.freeze({ ...relation }),
		});
		return true;
	}

	confirm(actorAction: ActorActionIdentity, adoption: PredictionAdoption): PredictionSettlement | undefined {
		if (this.stateValue.status !== "matching" || !sameActorAction(this.stateValue.actorAction, actorAction)) {
			return undefined;
		}
		return this.finish({
			prediction: this.identity,
			observation: "observed",
			actorAction: this.stateValue.actorAction,
			match: Object.freeze({
				matched: true,
				relation: this.stateValue.relation,
				adoption: freezeAdoption(adoption),
			}),
		});
	}

	miss(actorAction: ActorActionIdentity): PredictionSettlement | undefined {
		if (this.stateValue.status !== "pending") return undefined;
		return this.finish({
			prediction: this.identity,
			observation: "observed",
			actorAction: Object.freeze({ ...actorAction }),
			match: Object.freeze({ matched: false }),
		});
	}

	unobserve(cause: ResolutionCause): PredictionSettlement | undefined {
		if (this.stateValue.status !== "pending") return undefined;
		return this.finish({ prediction: this.identity, observation: "unobserved", cause: Object.freeze({ ...cause }) });
	}

	private finish(settlement: PredictionSettlement): PredictionSettlement {
		const value = Object.freeze(settlement);
		this.stateValue = Object.freeze({ status: "settled", settlement: value });
		return value;
	}
}

type MutablePlan = {
	id: string;
	source: string;
	revision: number;
	nextRevision: number;
	draftTokens: number;
	nodes: Map<string, MutableNode>;
};

interface PlanExecutionOwner {
	readonly execution: CandidateExecutionState<unknown>;
}

type MutableNodeExecution =
	| { readonly status: "deferred" }
	| { readonly status: "execution_blocked"; readonly cause: ResolutionCause }
	| { readonly status: "scheduled" }
	| {
			readonly status: "attached";
			readonly candidateID: string;
			readonly owner: PlanExecutionOwner;
	  };

type MutableNode = {
	identity: PlanActionIdentity;
	action: PlanAction;
	actionKey?: ActionKey;
	anchorDecisionSeq: number;
	earliestDecisionSeq: number;
	expectedDecisionSeq: number;
	latestDecisionSeq: number;
	criticalPathMs: number;
	execution: MutableNodeExecution;
	opportunity: PredictionOpportunity;
};

/** Owns plan materialization and prediction opportunities; execution is attached, never embedded. */
export class PlanRuntime {
	private readonly plans = new Map<string, MutablePlan>();

	apply(update: PlanUpdate, anchorDecisionSeq: number): PlanRuntimeUpdateResult {
		return "actions" in update
			? this.applyProposal(update, anchorDecisionSeq)
			: this.applyDelta(update, anchorDecisionSeq);
	}

	plan(proposalID: string): MaterializedPlan | undefined {
		const plan = this.plans.get(proposalID);
		return plan ? planSnapshot(plan) : undefined;
	}

	reserveRevision(proposalID: string): number | undefined {
		const plan = this.plans.get(proposalID);
		if (!plan) return undefined;
		const revision = Math.max(plan.nextRevision, plan.revision + 1);
		plan.nextRevision = revision + 1;
		return revision;
	}

	takeReady(
		settledDecisionSeq: number,
		shouldLaunch: (node: PlanRuntimeNode) => boolean = (node) => node.expectedDecisionSeq <= settledDecisionSeq + 1,
	): readonly PlanRuntimeNode[] {
		const ready = this.mutableValues()
			.filter(({ plan, node }) => {
				const snapshot = this.snapshot(plan, node);
				return node.execution.status === "deferred" && snapshot.readiness === "ready" && shouldLaunch(snapshot);
			})
			.sort(compareMutableNodes);
		for (const { node } of ready) node.execution = { status: "scheduled" };
		return ready.map(({ plan, node }) => this.snapshot(plan, node));
	}

	launchable(): readonly PlanRuntimeNode[] {
		return this.mutableValues()
			.map(({ plan, node }) => this.snapshot(plan, node))
			.filter((node) => node.execution.status === "deferred" && node.readiness === "ready");
	}

	promote(proposalID: string, actionID: string): PlanRuntimePromotion {
		const value = this.mutable(proposalID, actionID);
		if (!value) return { status: "missing" };
		const snapshot = this.snapshot(value.plan, value.node);
		if (snapshot.readiness === "settled") return { status: "settled" };
		if (snapshot.readiness === "blocked") return { status: "blocked" };
		if (value.node.execution.status !== "deferred") return { status: "already_dispatched" };
		if (snapshot.readiness === "waiting") return { status: "waiting" };
		value.node.execution = { status: "scheduled" };
		return { status: "scheduled", node: this.snapshot(value.plan, value.node) };
	}

	defer(proposalID: string, actionID: string): boolean {
		const node = this.mutable(proposalID, actionID)?.node;
		if (node?.execution.status !== "scheduled") return false;
		node.execution = { status: "deferred" };
		return true;
	}

	rearmExecution(candidateID: string): boolean {
		let rearmed = false;
		for (const { node } of this.mutableValues()) {
			if (
				node.execution.status !== "attached" ||
				node.execution.candidateID !== candidateID ||
				node.opportunity.state.status !== "pending" ||
				!executionSettled(executionProjection(node.execution))
			) {
				continue;
			}
			node.execution = { status: "deferred" };
			rearmed = true;
		}
		return rearmed;
	}

	bindActionKey(proposalID: string, actionID: string, actionKey: ActionKey): boolean {
		const node = this.mutable(proposalID, actionID)?.node;
		if (!node || node.actionKey) return false;
		node.actionKey = actionKey;
		return true;
	}

	markExecutionBlocked(proposalID: string, actionID: string, cause: ResolutionCause): boolean {
		const node = this.mutable(proposalID, actionID)?.node;
		if (node?.execution.status !== "deferred" || !node.actionKey) return false;
		node.execution = { status: "execution_blocked", cause: Object.freeze({ ...cause }) };
		return true;
	}

	attachExecution(proposalID: string, actionID: string, candidateID: string, owner: PlanExecutionOwner): boolean {
		const node = this.mutable(proposalID, actionID)?.node;
		if (!node || (node.execution.status !== "deferred" && node.execution.status !== "scheduled")) return false;
		node.execution = { status: "attached", candidateID, owner };
		return true;
	}

	claimMatch(
		proposalID: string,
		actionID: string,
		actorAction: ActorActionIdentity,
		relation: ActionKeyMatch,
	): PredictionOpportunity | undefined {
		const value = this.mutable(proposalID, actionID);
		if (!value || !this.isMatchable(value.plan, value.node, actorDecisionSequence(actorAction))) return undefined;
		const opportunity = value.node.opportunity;
		return opportunity.claim(actorAction, relation) ? opportunity : undefined;
	}

	confirm(
		opportunity: PredictionOpportunity,
		actorAction: ActorActionIdentity,
		adoption: PredictionAdoption,
	): PredictionSettlement | undefined {
		const finalized = opportunity.confirm(actorAction, adoption);
		if (!finalized) return undefined;
		const current = this.mutableValues().find(({ node }) => node.opportunity === opportunity);
		if (current) this.recompute(current.plan);
		return finalized;
	}

	miss(proposalID: string, actionID: string, actorAction: ActorActionIdentity): PredictionSettlement | undefined {
		const value = this.mutable(proposalID, actionID);
		return value?.node.opportunity.miss(actorAction);
	}

	unobserve(proposalID: string, actionID: string, cause: ResolutionCause): PredictionSettlement | undefined {
		const value = this.mutable(proposalID, actionID);
		return value?.node.opportunity.unobserve(cause);
	}

	get(proposalID: string, actionID: string): PlanRuntimeNode | undefined {
		const value = this.mutable(proposalID, actionID);
		return value ? this.snapshot(value.plan, value.node) : undefined;
	}

	opportunity(proposalID: string, actionID: string): PredictionOpportunity | undefined {
		return this.mutable(proposalID, actionID)?.node.opportunity;
	}

	values(): readonly PlanRuntimeNode[] {
		return this.mutableValues().map(({ plan, node }) => this.snapshot(plan, node));
	}

	pending(): readonly PredictionPlanRuntimeNode[] {
		return this.values().filter(isPendingPrediction);
	}

	matchable(decisionSequence: number): readonly PredictionPlanRuntimeNode[] {
		const sequence = Math.max(0, Math.floor(decisionSequence));
		return this.mutableValues().flatMap(({ plan, node }) => {
			if (!this.isMatchable(plan, node, sequence)) return [];
			const snapshot = this.snapshot(plan, node);
			return isPendingPrediction(snapshot) ? [snapshot] : [];
		});
	}

	unsettled(): readonly PredictionPlanRuntimeNode[] {
		return this.values().filter(isUnsettledPrediction);
	}

	due(settledDecisionSeq: number): readonly PredictionPlanRuntimeNode[] {
		return this.pending().filter((node) => node.latestDecisionSeq <= settledDecisionSeq);
	}

	drainBlocked(): readonly PlanRuntimeNode[] {
		return this.values().filter((node) => node.readiness === "blocked");
	}

	clear(): void {
		this.plans.clear();
	}

	private applyProposal(proposal: PlanProposal, anchorDecisionSeq: number): PlanRuntimeUpdateResult {
		if (!validIdentity(proposal.id, proposal.source)) return { accepted: false, reason: "invalid_identity" };
		if (!validRevision(proposal.revision)) return { accepted: false, reason: "invalid_revision" };
		const current = this.plans.get(proposal.id);
		if (current && current.source !== proposal.source) return { accepted: false, reason: "source_mismatch" };
		if (current && proposal.revision <= current.revision) return { accepted: false, reason: "stale_revision" };
		const validated = validateActions(proposal.actions);
		if (!validated.ok) return { accepted: false, reason: validated.reason };
		const actions = new Map(validated.actions.map((action) => [action.id, action]));
		if (!dependenciesAreValid(actions)) return { accepted: false, reason: "invalid_dependency" };
		return this.commit({
			id: proposal.id,
			source: proposal.source,
			revision: proposal.revision,
			draftTokens: finiteMetric(proposal.draftTokens),
			actions,
			upserted: validated.actions,
			anchorDecisionSeq,
		});
	}

	private applyDelta(delta: PlanDelta, anchorDecisionSeq: number): PlanRuntimeUpdateResult {
		if (!validIdentity(delta.proposalID, delta.source)) return { accepted: false, reason: "invalid_identity" };
		if (!validRevision(delta.revision)) return { accepted: false, reason: "invalid_revision" };
		const current = this.plans.get(delta.proposalID);
		if (!current) return { accepted: false, reason: "proposal_missing" };
		if (current.source !== delta.source) return { accepted: false, reason: "source_mismatch" };
		if (delta.revision <= current.revision) return { accepted: false, reason: "stale_revision" };
		const validated = validateActions(delta.upsert ?? []);
		if (!validated.ok) return { accepted: false, reason: validated.reason };
		const removals = new Set(delta.remove ?? []);
		if ([...removals].some((id) => !validToken(id))) return { accepted: false, reason: "invalid_action" };
		const actions = new Map([...current.nodes].map(([id, node]) => [id, node.action]));
		for (const id of removals) actions.delete(id);
		for (const action of validated.actions) actions.set(action.id, action);
		if (!dependenciesAreValid(actions)) return { accepted: false, reason: "invalid_dependency" };
		return this.commit({
			id: current.id,
			source: current.source,
			revision: delta.revision,
			draftTokens: current.draftTokens + finiteMetric(delta.draftTokens),
			actions,
			upserted: validated.actions,
			anchorDecisionSeq,
		});
	}

	private commit(input: {
		readonly id: string;
		readonly source: string;
		readonly revision: number;
		readonly draftTokens: number;
		readonly actions: ReadonlyMap<string, PlanAction>;
		readonly upserted: readonly PlanAction[];
		readonly anchorDecisionSeq: number;
	}): PlanRuntimeUpdateResult {
		const current = this.plans.get(input.id);
		const touched = new Set(input.upserted.map((action) => action.id));
		const replaced = new Set(
			input.upserted.flatMap((action) => {
				const previous = current?.nodes.get(action.id)?.action;
				return previous && !samePlanActionExecution(previous, action) ? [action.id] : [];
			}),
		);
		const removed = current ? [...current.nodes.keys()].filter((id) => !input.actions.has(id)) : [];
		const retiredIDs = new Set([...removed, ...replaced]);
		const retired: RetiredPlanNode[] = [];
		if (current) {
			for (const id of retiredIDs) {
				const node = current.nodes.get(id);
				if (node) {
					retired.push({
						node: this.snapshot(current, node),
						opportunity: node.opportunity,
					});
				}
			}
		}

		const anchor = sequence(input.anchorDecisionSeq);
		const nodes = new Map<string, MutableNode>();
		for (const [id, action] of input.actions) {
			const previous = current?.nodes.get(id);
			if (previous && !replaced.has(id)) {
				previous.action = action;
				if (touched.has(id) && previous.execution.status === "deferred") previous.anchorDecisionSeq = anchor;
				nodes.set(id, previous);
				continue;
			}
			nodes.set(id, newNode(input.id, input.source, input.revision, action, anchor));
		}

		const next: MutablePlan = {
			id: input.id,
			source: input.source,
			revision: input.revision,
			nextRevision: Math.max(current?.nextRevision ?? 0, input.revision + 1),
			draftTokens: input.draftTokens,
			nodes,
		};
		this.plans.set(next.id, next);
		this.recompute(next);
		return {
			accepted: true,
			plan: planSnapshot(next),
			upserted: input.upserted,
			removed,
			retired: Object.freeze(retired),
		};
	}

	private mutable(
		proposalID: string,
		actionID: string,
	): { readonly plan: MutablePlan; readonly node: MutableNode } | undefined {
		const plan = this.plans.get(proposalID);
		const node = plan?.nodes.get(actionID);
		return plan && node ? { plan, node } : undefined;
	}

	private mutableValues(): Array<{ readonly plan: MutablePlan; readonly node: MutableNode }> {
		return [...this.plans.values()].flatMap((plan) => [...plan.nodes.values()].map((node) => ({ plan, node })));
	}

	private snapshot(plan: MutablePlan, node: MutableNode): PlanRuntimeNode {
		const base = {
			identity: node.identity,
			proposalID: plan.id,
			source: plan.source,
			revision: plan.revision,
			action: node.action,
			...(node.actionKey ? { actionKey: node.actionKey } : {}),
			anchorDecisionSeq: node.anchorDecisionSeq,
			earliestDecisionSeq: node.earliestDecisionSeq,
			expectedDecisionSeq: node.expectedDecisionSeq,
			latestDecisionSeq: node.latestDecisionSeq,
			criticalPathMs: node.criticalPathMs,
			execution: executionProjection(node.execution),
			readiness: this.readiness(plan, node),
		};
		return Object.freeze({
			...base,
			prediction: node.opportunity.identity,
			predictionState: node.opportunity.state,
		});
	}

	private readiness(plan: MutablePlan, node: MutableNode): PlanNodeReadiness {
		if (node.opportunity.state.status === "settled") return "settled";
		if (this.dependenciesImpossible(plan, node)) return "blocked";
		return node.execution.status === "deferred" && this.dependenciesSatisfied(plan, node) ? "ready" : "waiting";
	}

	private dependenciesSatisfied(plan: MutablePlan, node: MutableNode): boolean {
		return (node.action.dependsOn ?? []).every((dependency) => {
			const parent = plan.nodes.get(dependency.actionID);
			return parent ? dependencySatisfied(parent, dependency.condition) : false;
		});
	}

	private dependenciesImpossible(plan: MutablePlan, node: MutableNode): boolean {
		return (node.action.dependsOn ?? []).some((dependency) => {
			const parent = plan.nodes.get(dependency.actionID);
			return !parent || dependencyImpossible(parent, dependency.condition);
		});
	}

	private isMatchable(plan: MutablePlan, node: MutableNode, decisionSequence: number): boolean {
		return (
			node.opportunity.state.status === "pending" &&
			node.earliestDecisionSeq <= decisionSequence &&
			this.dependenciesSatisfied(plan, node)
		);
	}

	private recompute(plan: MutablePlan): void {
		const actionSequence = (relativeHorizon: (action: PlanAction) => number) => {
			const memo = new Map<string, number>();
			const visiting = new Set<string>();
			const calculate = (node: MutableNode): number => {
				const cached = memo.get(node.action.id);
				if (cached !== undefined) return cached;
				if (visiting.has(node.action.id)) return node.anchorDecisionSeq + relativeHorizon(node.action) + 1;
				visiting.add(node.action.id);
				const settlement = node.opportunity.settlement;
				let value = predictionMatched(settlement)
					? actorDecisionSequence(settlement.actorAction)
					: node.anchorDecisionSeq + relativeHorizon(node.action) + 1;
				for (const dependency of node.action.dependsOn ?? []) {
					const parent = plan.nodes.get(dependency.actionID);
					if (parent) value = Math.max(value, calculate(parent) + 1);
				}
				visiting.delete(node.action.id);
				memo.set(node.action.id, value);
				return value;
			};
			return calculate;
		};
		const earliest = actionSequence(() => 0);
		const expected = actionSequence(horizon);
		const latest = actionSequence(latestHorizon);
		const dependents = new Map<string, MutableNode[]>();
		for (const node of plan.nodes.values()) {
			for (const dependency of node.action.dependsOn ?? []) {
				const values = dependents.get(dependency.actionID) ?? [];
				values.push(node);
				dependents.set(dependency.actionID, values);
			}
		}
		const criticalMemo = new Map<string, number>();
		const criticalPath = (node: MutableNode): number => {
			const cached = criticalMemo.get(node.action.id);
			if (cached !== undefined) return cached;
			const own = Math.max(1, finiteMetric(node.action.expectedDurationMs));
			const descendants = dependents.get(node.action.id) ?? [];
			const value = own + descendants.reduce((longest, child) => Math.max(longest, criticalPath(child)), 0);
			criticalMemo.set(node.action.id, value);
			return value;
		};
		for (const node of plan.nodes.values()) {
			node.earliestDecisionSeq = earliest(node);
			node.expectedDecisionSeq = expected(node);
			node.latestDecisionSeq = latest(node);
			node.criticalPathMs = criticalPath(node);
		}
	}
}

function newNode(
	proposalID: string,
	source: string,
	revision: number,
	action: PlanAction,
	anchorDecisionSeq: number,
): MutableNode {
	const identity: PlanActionIdentity = Object.freeze({
		id: planNodeID(source, proposalID, action.id, revision),
		source,
		proposalID,
		actionID: action.id,
	});
	return {
		identity,
		action,
		actionKey: undefined,
		anchorDecisionSeq,
		earliestDecisionSeq: anchorDecisionSeq + 1,
		expectedDecisionSeq: anchorDecisionSeq + horizon(action) + 1,
		latestDecisionSeq: anchorDecisionSeq + latestHorizon(action) + 1,
		criticalPathMs: Math.max(1, finiteMetric(action.expectedDurationMs)),
		execution: { status: "deferred" },
		opportunity: new PredictionOpportunity(identity),
	};
}

function dependencySatisfied(node: MutableNode, condition: PlanActionDependencyCondition | undefined): boolean {
	const execution = executionProjection(node.execution);
	switch (canonicalCondition(condition)) {
		case "actor_adopted":
			return predictionAdopted(node.opportunity.settlement);
		case "execution_succeeded":
			return execution.status === "succeeded";
		case "execution_settled":
			return executionSettled(execution);
	}
}

function dependencyImpossible(node: MutableNode, condition: PlanActionDependencyCondition | undefined): boolean {
	const execution = executionProjection(node.execution);
	switch (canonicalCondition(condition)) {
		case "actor_adopted":
			return node.opportunity.settlement !== undefined && !predictionAdopted(node.opportunity.settlement);
		case "execution_succeeded":
			return execution.status === "failed" || execution.status === "cancelled";
		case "execution_settled":
			return false;
	}
}

function predictionMatched(settlement: PredictionSettlement | undefined): settlement is Extract<
	PredictionSettlement,
	{ readonly observation: "observed" }
> & {
	readonly match: { readonly matched: true };
} {
	return settlement?.observation === "observed" && settlement.match.matched;
}

function predictionAdopted(settlement: PredictionSettlement | undefined): boolean {
	return predictionMatched(settlement) && settlement.match.adoption.status === "adopted";
}

function freezeAdoption(adoption: PredictionAdoption): PredictionAdoption {
	return adoption.status === "adopted"
		? Object.freeze({ ...adoption })
		: Object.freeze({ ...adoption, cause: Object.freeze({ ...adoption.cause }) });
}

function sameActorAction(left: ActorActionIdentity, right: ActorActionIdentity): boolean {
	return left.id === right.id && left.sequence === right.sequence && left.turnID === right.turnID;
}

function canonicalCondition(
	condition: PlanActionDependencyCondition | undefined,
): "execution_settled" | "execution_succeeded" | "actor_adopted" {
	if (condition === "actor_adopted") return "actor_adopted";
	if (condition === "execution_succeeded") return "execution_succeeded";
	return "execution_settled";
}

function executionSettled(execution: PlanNodeExecution): boolean {
	return execution.status === "succeeded" || execution.status === "failed" || execution.status === "cancelled";
}

function executionProjection(execution: MutableNodeExecution): PlanNodeExecution {
	if (execution.status !== "attached") return execution;
	const state = execution.owner.execution;
	if (state.status === "queued") return { status: "queued", candidateID: execution.candidateID };
	if (state.status === "running") return { status: "running", candidateID: execution.candidateID };
	if (state.status === "succeeded") return { status: "succeeded", candidateID: execution.candidateID };
	return {
		status: state.status,
		cause: state.cause,
		candidateID: execution.candidateID,
	};
}

function isPendingPrediction(node: PlanRuntimeNode): node is PredictionPlanRuntimeNode {
	return node.predictionState.status === "pending";
}

function isUnsettledPrediction(node: PlanRuntimeNode): node is PredictionPlanRuntimeNode {
	return node.predictionState.status !== "settled";
}

function compareMutableNodes(
	left: { readonly plan: MutablePlan; readonly node: MutableNode },
	right: { readonly plan: MutablePlan; readonly node: MutableNode },
): number {
	return (
		left.node.expectedDecisionSeq - right.node.expectedDecisionSeq ||
		right.node.criticalPathMs - left.node.criticalPathMs ||
		left.plan.id.localeCompare(right.plan.id) ||
		left.node.action.id.localeCompare(right.node.action.id)
	);
}

function planSnapshot(plan: MutablePlan): MaterializedPlan {
	return Object.freeze({
		id: plan.id,
		source: plan.source,
		revision: plan.revision,
		actions: Object.freeze([...plan.nodes.values()].map((node) => node.action)),
		draftTokens: plan.draftTokens,
	});
}

function validateActions(
	actions: readonly PlanAction[],
):
	| { readonly ok: true; readonly actions: readonly PlanAction[] }
	| { readonly ok: false; readonly reason: "duplicate_action" | "invalid_action" | "invalid_dependency" } {
	const result: PlanAction[] = [];
	const ids = new Set<string>();
	for (const source of actions) {
		if (!validToken(source.id) || !validToken(source.tool) || source.type !== "tool_call") {
			return { ok: false, reason: "invalid_action" };
		}
		if (ids.has(source.id)) return { ok: false, reason: "duplicate_action" };
		ids.add(source.id);
		const dependsOn = source.dependsOn
			? Object.freeze(
					source.dependsOn.map((dependency) => ({
						...dependency,
						condition: canonicalCondition(dependency.condition),
					})),
				)
			: undefined;
		if (dependsOn?.some((dependency) => !validToken(dependency.actionID))) {
			return { ok: false, reason: "invalid_dependency" };
		}
		let input: unknown;
		try {
			input = freezePlanValue(structuredClone(source.input));
		} catch {
			return { ok: false, reason: "invalid_action" };
		}
		result.push(
			Object.freeze({
				...source,
				input,
				...(dependsOn ? { dependsOn } : {}),
			}),
		);
	}
	return { ok: true, actions: Object.freeze(result) };
}

function dependenciesAreValid(actions: ReadonlyMap<string, PlanAction>): boolean {
	for (const action of actions.values()) {
		for (const dependency of action.dependsOn ?? []) {
			const parent = actions.get(dependency.actionID);
			if (dependency.actionID === action.id || !parent) {
				return false;
			}
		}
	}
	const visiting = new Set<string>();
	const visited = new Set<string>();
	const visit = (actionID: string): boolean => {
		if (visiting.has(actionID)) return false;
		if (visited.has(actionID)) return true;
		visiting.add(actionID);
		for (const dependency of actions.get(actionID)?.dependsOn ?? []) {
			if (!visit(dependency.actionID)) return false;
		}
		visiting.delete(actionID);
		visited.add(actionID);
		return true;
	};
	return [...actions.keys()].every(visit);
}

function samePlanActionExecution(left: PlanAction, right: PlanAction): boolean {
	return (
		left.tool === right.tool &&
		isDeepStrictEqual(left.input, right.input) &&
		isDeepStrictEqual(normalizedDependencies(left), normalizedDependencies(right))
	);
}

function normalizedDependencies(action: PlanAction): ReadonlyArray<Required<PlanActionDependency>> {
	return (action.dependsOn ?? [])
		.map(
			(dependency): Required<PlanActionDependency> => ({
				actionID: dependency.actionID,
				condition: canonicalCondition(dependency.condition),
			}),
		)
		.sort(
			(left, right) => left.actionID.localeCompare(right.actionID) || left.condition.localeCompare(right.condition),
		);
}

function planNodeID(source: string, proposalID: string, actionID: string, revision: number): string {
	return JSON.stringify([source, proposalID, actionID, revision]);
}

function horizon(action: PlanAction): number {
	return sequence(action.horizon ?? 0);
}

function latestHorizon(action: PlanAction): number {
	return Math.max(horizon(action), sequence(action.latestHorizon ?? action.horizon ?? 0));
}

function actorDecisionSequence(action: ActorActionIdentity): number {
	return sequence(action.decisionSequence ?? action.sequence);
}

function validIdentity(proposalID: string, source: string): boolean {
	return validToken(proposalID) && validToken(source);
}

function validToken(value: string): boolean {
	return typeof value === "string" && value.length > 0 && value.trim() === value;
}

function validRevision(value: number): boolean {
	return Number.isSafeInteger(value) && value >= 0;
}

function finiteMetric(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

function sequence(value: number): number {
	return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

function freezePlanValue<Value>(value: Value, seen = new WeakSet<object>()): Value {
	if (!value || typeof value !== "object" || Object.isFrozen(value) || seen.has(value)) return value;
	seen.add(value);
	for (const child of Object.values(value)) freezePlanValue(child, seen);
	return Object.freeze(value);
}
