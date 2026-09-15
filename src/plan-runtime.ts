import { nonNegativeCount as sequence, nonNegativeFinite as finiteMetric } from "./number-utils.ts";
import { isDeepStrictEqual } from "node:util";
import { immutableSnapshot, isImmutableSnapshot } from "./stable-json.ts";
import type { ActionKey, ActionKeyMatch } from "./action-semantics.ts";
import type { CandidateExecution } from "./candidate-execution.ts";
import type {
	MaterializedPlan,
	PlanAction,
	PlanActionDependency,
	PlanActionDependencyCondition,
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
	| { readonly status: "deferred" | "preparing" | "scheduled" }
	| { readonly status: "execution_blocked" | "failed"; readonly cause: ResolutionCause }
	| { readonly status: "queued" | "running" | "succeeded"; readonly candidateID: string }
	| { readonly status: "failed" | "cancelled"; readonly cause: ResolutionCause; readonly candidateID: string };

export type PredictionOpportunityState =
	| { readonly status: "pending" }
	| {
			readonly status: "matching";
			readonly actorAction: ActorActionIdentity;
			readonly relation: ActionKeyMatch;
	  }
	| { readonly status: "settled"; readonly settlement: PredictionSettlement };

export type PlanNodeReadiness = "ready" | "waiting" | "blocked" | "settled";

export interface PlanRuntimeNode {
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
	readonly prediction: PredictionIdentity;
	readonly predictionState: PredictionOpportunityState;
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
			readonly retired: readonly PlanRuntimeNode[];
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
	graph: PlanGraph;
};

type PlanGraph = { readonly plans: Set<MutablePlan>; ordered: readonly MutableNode[] };

type PlanExecutionOwner = Pick<CandidateExecution<unknown>, "execution" | "completion">;

type MutableNodeExecution =
	| PlanNodeExecution
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
	validDependencies: boolean;
	execution: MutableNodeExecution;
	opportunity: PredictionOpportunity;
};

const capturedUpdates = new WeakSet<PlanUpdate>();

/** Owns plan materialization and prediction opportunities; execution is attached, never embedded. */
export class PlanRuntime {
	private readonly plans = new Map<string, MutablePlan>();

	/** Capture once at handoff, before asynchronous binding can invoke producer callbacks. */
	static capture(update: PlanUpdate, multiStep = true): { readonly update: PlanUpdate } | Extract<PlanRuntimeUpdateResult, { accepted: false }> {
		const captured = capturedUpdates.has(update);
		if (captured && multiStep) return { update };
		update = { ...update };
		const id = "actions" in update ? update.id : update.proposalID;
		if (!validIdentity(id, update.source)) return { accepted: false, reason: "invalid_identity" };
		if (!validRevision(update.revision)) return { accepted: false, reason: "invalid_revision" };
		const remove = "actions" in update ? [] : [...(update.remove ?? [])];
		const offered = "actions" in update
			? multiStep ? update.actions : update.actions.filter((action) => finiteMetric(action.horizon) === 0 && (action.dependsOn?.length ?? 0) === 0)
			: multiStep ? update.upsert ?? [] : [];
		const validated = captured ? { ok: true as const, actions: Object.freeze(offered) } : validateActions(offered);
		if (!validated.ok) return { accepted: false, reason: validated.reason };
		if (remove.some((id) => !validToken(id))) return { accepted: false, reason: "invalid_action" };
		update = Object.freeze("actions" in update
			? { ...update, actions: validated.actions }
			: { ...update, upsert: validated.actions, remove: Object.freeze(remove) });
		capturedUpdates.add(update);
		return { update };
	}

	apply(update: PlanUpdate, anchorDecisionSeq: number): PlanRuntimeUpdateResult {
		const captured = PlanRuntime.capture(update);
		if (!("update" in captured)) return captured;
		const owned = captured.update, proposal = "actions" in owned;
		const id = proposal ? owned.id : owned.proposalID;
		const current = this.plans.get(id);
		if (!proposal && !current) return { accepted: false, reason: "proposal_missing" };
		if (current && current.source !== owned.source) return { accepted: false, reason: "source_mismatch" };
		if (current && owned.revision <= current.revision) return { accepted: false, reason: "stale_revision" };
		const upserted = proposal ? owned.actions : owned.upsert ?? [];
		const actions = new Map(proposal ? [] : [...current!.nodes].map(([id, node]) => [id, node.action] as const));
		if (!proposal) for (const id of owned.remove ?? []) actions.delete(id);
		for (const action of upserted) actions.set(action.id, action);
		const ordered = dependencyOrder(actions, (dependency) =>
			dependency.proposalID !== id && this.parent(id, dependency) !== undefined);
		if (!ordered) return { accepted: false, reason: "invalid_dependency" };
		return this.commit({
			id, source: owned.source, revision: owned.revision,
			draftTokens: (proposal ? 0 : current!.draftTokens) + finiteMetric(owned.draftTokens),
			actions, upserted, ordered, anchorDecisionSeq,
		});
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

	launchable(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.execution.status === "deferred" && node.opportunity.state.status !== "settled", "ready");
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
				!("candidateID" in node.execution) ||
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

	bindActionKey(identity: PlanActionIdentity, actionKey: ActionKey): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID)?.node;
		if (node?.identity.id !== identity.id || node.actionKey || node.execution.status !== "deferred" || node.opportunity.state.status === "settled") return false;
		node.actionKey = actionKey;
		node.execution = { status: "preparing" };
		return true;
	}

	finishPreparation(identity: PlanActionIdentity, blocked?: ResolutionCause): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID)?.node;
		if (node?.identity.id !== identity.id || node.execution.status !== "preparing" || node.opportunity.state.status === "settled") return false;
		node.execution = blocked ? { status: "execution_blocked", cause: Object.freeze({ ...blocked }) } : { status: "deferred" };
		return true;
	}

	attachExecution(proposalID: string, actionID: string, candidateID: string, owner: PlanExecutionOwner): boolean {
		const node = this.mutable(proposalID, actionID)?.node;
		if (!node || (node.execution.status !== "deferred" && node.execution.status !== "scheduled")) return false;
		const attachment = { status: "attached" as const, candidateID, owner };
		node.execution = attachment;
		void owner.completion.then(state => {
			if (node.execution === attachment) node.execution = projectExecution(state, candidateID);
		});
		return true;
	}

	/** Preparation and dependency rejection own no physical execution or candidate ID. */
	rejectExecution(identity: PlanActionIdentity, failure: ResolutionCause): boolean {
		const node = this.mutable(identity.proposalID, identity.actionID)?.node;
		if (node?.identity.id !== identity.id || !["deferred", "preparing", "scheduled"].includes(node.execution.status)) return false;
		node.execution = Object.freeze({ status: "failed", cause: Object.freeze({ ...failure }) });
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
		const current = this.mutable(opportunity.identity.proposalID, opportunity.identity.actionID);
		if (current?.node.opportunity === opportunity) this.recompute(current.plan.graph);
		return finalized;
	}

	miss(proposalID: string, actionID: string, actorAction: ActorActionIdentity): PredictionSettlement | undefined {
		const value = this.mutable(proposalID, actionID);
		return value?.node.opportunity.miss(actorAction);
	}

	get(proposalID: string, actionID: string): PlanRuntimeNode | undefined {
		const value = this.mutable(proposalID, actionID);
		return value ? this.snapshot(value.plan, value.node) : undefined;
	}

	dependency(proposalID: string, dependency: PlanActionDependency): PlanRuntimeNode | undefined {
		const node = this.parent(proposalID, dependency);
		return node ? this.get(node.identity.proposalID, node.action.id) : undefined;
	}

	opportunity(proposalID: string, actionID: string): PredictionOpportunity | undefined {
		return this.mutable(proposalID, actionID)?.node.opportunity;
	}

	values(): readonly PlanRuntimeNode[] {
		return this.select();
	}

	pending(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status === "pending");
	}

	matchable(decisionSequence: number): readonly PlanRuntimeNode[] {
		const sequence = Math.max(0, Math.floor(decisionSequence));
		return this.select((node, plan) => this.isMatchable(plan, node, sequence));
	}

	unsettled(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status !== "settled");
	}

	consumers(candidateID: string): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status !== "settled" &&
			"candidateID" in node.execution && node.execution.candidateID === candidateID);
	}

	due(settledDecisionSeq: number): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status === "pending" && node.latestDecisionSeq <= settledDecisionSeq);
	}

	drainBlocked(): readonly PlanRuntimeNode[] {
		return this.select((node) => node.opportunity.state.status !== "settled", "blocked");
	}

	clear(): void {
		this.plans.clear();
	}

	private commit(input: {
		readonly id: string;
		readonly source: string;
		readonly revision: number;
		readonly draftTokens: number;
		readonly actions: ReadonlyMap<string, PlanAction>;
		readonly upserted: readonly PlanAction[];
		readonly ordered: readonly PlanAction[];
		readonly anchorDecisionSeq: number;
	}): PlanRuntimeUpdateResult {
		const current = this.plans.get(input.id);
		const touched = new Set(input.upserted.map((action) => action.id));
		const replaced = new Set<string>();
		// A changed ancestor changes the child's execution context even when its own input is unchanged.
		for (const action of input.ordered) {
			const previous = current?.nodes.get(action.id)?.action;
			if (previous && ((touched.has(action.id) && !samePlanActionExecution(previous, action)) ||
				action.dependsOn?.some((dependency) => !dependency.proposalID && replaced.has(dependency.actionID)))) replaced.add(action.id);
		}
		const removed = current ? [...current.nodes.keys()].filter((id) => !input.actions.has(id)) : [];
		const retiredIDs = new Set([...removed, ...replaced]);
		const retired: PlanRuntimeNode[] = [];
		if (current) {
			for (const id of retiredIDs) {
				const node = current.nodes.get(id);
				if (node) retired.push(this.snapshot(current, node));
			}
		}

		const anchor = sequence(input.anchorDecisionSeq);
		const nodes = new Map<string, MutableNode>();
		for (const [id, action] of input.actions) {
			const previous = current?.nodes.get(id);
			if (previous && !replaced.has(id)) {
				previous.action = action;
				if (touched.has(id) && (previous.execution.status === "deferred" || previous.execution.status === "preparing")) previous.anchorDecisionSeq = anchor;
				nodes.set(id, previous);
				continue;
			}
			nodes.set(id, newNode(input.id, input.source, input.revision, action, anchor));
		}

		const connected = new Set(current ? [current.graph] : []);
		for (const action of input.actions.values()) for (const dependency of action.dependsOn ?? []) {
			if (dependency.proposalID) connected.add(this.plans.get(dependency.proposalID)!.graph);
		}
		const graph: PlanGraph = { plans: new Set(), ordered: [] };
		for (const previous of connected) for (const plan of previous.plans) {
			if (plan !== current) graph.plans.add(plan);
		}
		const next: MutablePlan = {
			id: input.id,
			source: input.source,
			revision: input.revision,
			nextRevision: Math.max(current?.nextRevision ?? 0, input.revision + 1),
			draftTokens: input.draftTokens,
			nodes,
			graph,
		};
		this.plans.set(next.id, next);
		graph.plans.add(next);
		for (const plan of graph.plans) plan.graph = graph;
		const visited = new Set<MutableNode>(), ordered: MutableNode[] = [];
		const visit = (node: MutableNode): void => {
			if (visited.has(node)) return;
			visited.add(node);
			for (const dependency of node.action.dependsOn ?? []) {
				const parent = this.parent(node.identity.proposalID, dependency);
				if (parent) visit(parent);
			}
			ordered.push(node);
		};
		// Only plans sharing dependency ancestry need a common order and settlement pass.
		for (const plan of graph.plans) for (const node of plan.nodes.values()) visit(node);
		graph.ordered = ordered;
		this.recompute(graph);
		return {
			accepted: true,
			plan: planSnapshot(next),
			upserted: Object.freeze([...input.upserted, ...input.ordered.filter((action) => replaced.has(action.id) && !touched.has(action.id))]),
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

	private parent(proposalID: string, dependency: PlanActionDependency): MutableNode | undefined {
		const node = this.mutable(dependency.proposalID ?? proposalID, dependency.actionID)?.node;
		return !dependency.proposalID || node?.identity.id === dependency.identity ? node : undefined;
	}

	private select(
		include: (node: MutableNode, plan: MutablePlan) => boolean = () => true,
		readiness?: PlanNodeReadiness,
	): readonly PlanRuntimeNode[] {
		const selected: PlanRuntimeNode[] = [];
		for (const plan of this.plans.values()) for (const node of plan.nodes.values()) {
			if (!include(node, plan)) continue;
			const current = this.readiness(plan, node);
			if (!readiness || current === readiness) selected.push(this.snapshot(plan, node, current));
		}
		return selected;
	}

	private snapshot(plan: MutablePlan, node: MutableNode, readiness = this.readiness(plan, node)): PlanRuntimeNode {
		return Object.freeze({
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
			readiness,
			prediction: node.opportunity.identity,
			predictionState: node.opportunity.state,
		});
	}

	private readiness(plan: MutablePlan, node: MutableNode): PlanNodeReadiness {
		if (node.opportunity.state.status === "settled") return "settled";
		const readiness = this.dependencyReadiness(plan, node);
		return readiness === "ready" && node.execution.status !== "deferred" ? "waiting" : readiness;
	}

	private dependencyReadiness(plan: MutablePlan, node: MutableNode): "ready" | "waiting" | "blocked" {
		if (!node.validDependencies) return "blocked";
		let readiness: "ready" | "waiting" = "ready";
		for (const dependency of node.action.dependsOn ?? []) {
			const parent = this.parent(plan.id, dependency);
			const state = parent ? dependencyReadiness(parent, dependency.condition) : "blocked";
			if (state === "blocked") return state;
			if (state === "waiting") readiness = state;
		}
		return readiness;
	}

	private isMatchable(plan: MutablePlan, node: MutableNode, decisionSequence: number): boolean {
		return (
			node.opportunity.state.status === "pending" &&
			node.earliestDecisionSeq <= decisionSequence &&
			this.dependencyReadiness(plan, node) === "ready"
		);
	}

	private recompute(graph: PlanGraph): void {
		// The accepted graph is immutable between revisions; reuse its validated topological order.
		for (const node of graph.ordered) {
			const settlement = node.opportunity.settlement;
			const matched = predictionMatched(settlement) ? actorDecisionSequence(settlement.actorAction) : undefined;
			node.earliestDecisionSeq = matched ?? node.anchorDecisionSeq + 1;
			node.expectedDecisionSeq = matched ?? node.anchorDecisionSeq + horizon(node.action) + 1;
			node.latestDecisionSeq = matched ?? node.anchorDecisionSeq + latestHorizon(node.action) + 1;
			node.criticalPathMs = 0;
			node.validDependencies = true;
			for (const dependency of node.action.dependsOn ?? []) {
				const parent = this.parent(node.identity.proposalID, dependency);
				if (!parent || !parent.validDependencies) node.validDependencies = false;
				if (!parent) continue;
				node.earliestDecisionSeq = Math.max(node.earliestDecisionSeq, parent.earliestDecisionSeq + 1);
				node.expectedDecisionSeq = Math.max(node.expectedDecisionSeq, parent.expectedDecisionSeq + 1);
				node.latestDecisionSeq = Math.max(node.latestDecisionSeq, parent.latestDecisionSeq + 1);
			}
		}
		for (let index = graph.ordered.length - 1; index >= 0; index--) {
			const node = graph.ordered[index]!;
			node.criticalPathMs += Math.max(1, finiteMetric(node.action.expectedDurationMs));
			for (const dependency of node.action.dependsOn ?? []) {
				const parent = this.parent(node.identity.proposalID, dependency);
				if (parent) parent.criticalPathMs = Math.max(parent.criticalPathMs, node.criticalPathMs);
			}
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
		validDependencies: true,
		execution: { status: "deferred" },
		opportunity: new PredictionOpportunity(identity),
	};
}

function dependencyReadiness(node: MutableNode, condition: PlanActionDependencyCondition | undefined): "ready" | "waiting" | "blocked" {
	if (condition === "actor_adopted") {
		const settlement = node.opportunity.settlement;
		return settlement === undefined ? "waiting" : predictionAdopted(settlement) ? "ready" : "blocked";
	}
	const execution = executionProjection(node.execution);
	if (!executionSettled(execution)) return "waiting";
	return condition === "execution_succeeded" && execution.status !== "succeeded" ? "blocked" : "ready";
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
	return execution.status === "attached" ? projectExecution(execution.owner.execution, execution.candidateID) : Object.freeze(execution);
}

function projectExecution(state: PlanExecutionOwner["execution"], candidateID: string): PlanNodeExecution {
	return Object.freeze(state.status === "failed" || state.status === "cancelled"
		? { status: state.status, cause: state.cause, candidateID } : { status: state.status, candidateID });
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
	// Capture scheduling records before cloning input graphs, which may invoke producer accessors.
	const result = actions.map((source) => ({ ...source }));
	const ids = new Set<string>();
	for (const source of result) {
		if (!validToken(source.id) || !validToken(source.tool) || source.type !== "tool_call") {
			return { ok: false, reason: "invalid_action" };
		}
		if (ids.has(source.id)) return { ok: false, reason: "duplicate_action" };
		ids.add(source.id);
		if (source.dependsOn) {
			source.dependsOn = Object.freeze(
				source.dependsOn.map(({ actionID, proposalID, identity, condition }) =>
					Object.freeze({ actionID, ...(proposalID !== undefined || identity !== undefined ? { proposalID, identity } : {}),
						condition: canonicalCondition(condition) }),
				),
			);
			if (source.dependsOn.some((dependency) => !validToken(dependency.actionID) ||
				("proposalID" in dependency && (!validToken(dependency.proposalID!) || !validToken(dependency.identity!))))) {
				return { ok: false, reason: "invalid_dependency" };
			}
		}
	}
	try {
		for (const source of result) {
			source.input = immutableSnapshot(source.input);
			if (!isImmutableSnapshot(source.input)) return { ok: false, reason: "invalid_action" };
			Object.freeze(source);
		}
	} catch {
		return { ok: false, reason: "invalid_action" };
	}
	return { ok: true, actions: Object.freeze(result) };
}

function dependencyOrder(actions: ReadonlyMap<string, PlanAction>, external: (dependency: PlanActionDependency) => boolean): readonly PlanAction[] | undefined {
	const visiting = new Set<string>();
	const ordered = new Map<string, PlanAction>();
	const visit = (actionID: string): boolean => {
		if (ordered.has(actionID)) return true;
		const action = actions.get(actionID);
		if (!action || visiting.has(actionID)) return false;
		visiting.add(actionID);
		for (const dependency of action.dependsOn ?? []) {
			if (!(dependency.proposalID ? external(dependency) : visit(dependency.actionID))) return false;
		}
		visiting.delete(actionID);
		ordered.set(actionID, action);
		return true;
	};
	return [...actions.keys()].every(visit) ? [...ordered.values()] : undefined;
}

function samePlanActionExecution(left: PlanAction, right: PlanAction): boolean {
	if (left.tool !== right.tool || !isDeepStrictEqual(left.input, right.input)) return false;
	// Owned dependency records have canonical fields; opaque IDs must not use locale ordering.
	const counts = new Map<string, number>();
	for (const [dependencies, delta] of [[left.dependsOn, 1], [right.dependsOn, -1]] as const) {
		for (const dependency of dependencies ?? []) {
			const key = JSON.stringify(dependency), count = (counts.get(key) ?? 0) + delta;
			if (count) counts.set(key, count);
			else counts.delete(key);
		}
	}
	return counts.size === 0;
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
