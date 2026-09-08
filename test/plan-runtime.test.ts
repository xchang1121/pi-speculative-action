import { describe, expect, it } from "vitest";
import { CandidateExecution } from "../src/candidate-execution.ts";
import type { PlanAction, PlanProposal } from "../src/plan-proposal.ts";
import { PlanRuntime } from "../src/plan-runtime.ts";
import { cause } from "../src/settlement.ts";

describe("PlanRuntime", () => {
	it("keeps execution failure and later Actor confirmation as independent facts", () => {
		const plan = new PlanRuntime();
		plan.apply(proposal([action("parent")]), 0);
		plan.takeReady(0);
		const execution = new CandidateExecution<string>("shared");
		plan.attachExecution("plan", "parent", "candidate", execution);
		expect(plan.get("plan", "parent")?.execution).toEqual({ status: "queued", candidateID: "candidate" });
		execution.start(1);
		execution.fail(cause("execution", "tool_failed"), 2, 1);
		const actor = { id: "actor", sequence: 1, turnID: "turn" } as const;
		const relation = { kind: "exact", distance: 0 } as const;
		const opportunity = plan.claimMatch("plan", "parent", actor, relation)!;

		const settlement = plan.confirm(opportunity, actor, {
			status: "rejected",
			candidateID: "candidate",
			cause: cause("execution", "tool_failed"),
		});

		expect(settlement).toMatchObject({
			observation: "observed",
			match: { matched: true, adoption: { status: "rejected" } },
		});
		expect(settlement?.observation === "observed" && Object.isFrozen(settlement.match)).toBe(true);
		expect(plan.get("plan", "parent")).toMatchObject({
			execution: { status: "failed" },
			predictionState: { status: "settled", settlement },
		});
		expect(
			plan.confirm(
				opportunity,
				{ id: "actor-2", sequence: 2, turnID: "turn" },
				{
					status: "adopted",
					candidateID: "candidate",
				},
			),
		).toBeUndefined();
	});

	it("schedules at the expected horizon and retains the prediction until its latest horizon", () => {
		const plan = new PlanRuntime();
		plan.apply(proposal([action("future", { horizon: 0, latestHorizon: 2 })]), 4);
		plan.takeReady(4);
		const execution = new CandidateExecution<string>("shared");
		plan.attachExecution("plan", "future", "candidate", execution);
		execution.cancel(cause("admission", "scheduler_preempted"), 1, 0);

		expect(plan.get("plan", "future")).toMatchObject({
			earliestDecisionSeq: 5,
			expectedDecisionSeq: 5,
			latestDecisionSeq: 7,
			execution: { status: "cancelled" },
			predictionState: { status: "pending" },
		});
		expect(plan.rearmExecution("candidate")).toBe(true);
		const retry = new CandidateExecution<string>("shared");
		plan.attachExecution("plan", "future", "retry", retry);
		retry.cancel(cause("freshness", "resource_changed"), 2, 0);
		expect(plan.due(6)).toEqual([]);
		expect(plan.due(7).map((node) => node.action.id)).toEqual(["future"]);
		const actor = { id: "actor", sequence: 7, turnID: "turn" } as const;
		const opportunity = plan.claimMatch("plan", "future", actor, { kind: "exact", distance: 0 })!;
		expect(plan.rearmExecution("retry")).toBe(false);

		const settlement = plan.confirm(opportunity, actor, {
			status: "rejected",
			candidateID: "candidate",
			cause: cause("admission", "candidate_unavailable"),
		});
		expect(settlement).toMatchObject({ observation: "observed", match: { matched: true } });

		const clamped = new PlanRuntime();
		clamped.apply(proposal([action("clamped", { horizon: 2, latestHorizon: 0 })]), 4);
		expect(clamped.get("plan", "clamped")).toMatchObject({ expectedDecisionSeq: 7, latestDecisionSeq: 7 });
	});

	it.each(["proposal", "delta"] as const)("owns %s identity before input capture and binds one canonical action key", (kind) => {
		const plan = new PlanRuntime();
		if (kind === "delta") plan.apply(proposal([action("retained")]), 0);
		const feedback = () => undefined;
		const offered = { ...action("keyed"), background: true, feedback };
		const peer = { ...action("peer"), dependsOn: [{ actionID: "keyed" }] };
		const update = kind === "proposal"
			? { ...proposal([offered, peer]), draftTokens: 3 }
			: { proposalID: "plan", source: "source", revision: 2, upsert: [offered, peer], remove: [] as string[], draftTokens: 3 };
		let path = "keyed.ts", reads = 0;
		offered.input = {
			get path() {
				reads++;
				Object.assign(offered, { id: "drifted", tool: "write", background: false });
				peer.id = "drifted-peer";
				peer.dependsOn[0]!.actionID = "missing";
				update.remove?.push("retained");
				Object.assign(update, {
					[kind === "proposal" ? "id" : "proposalID"]: "drifted", source: "drifted", revision: -1, draftTokens: 99,
				});
				return path;
			},
		};
		const captured = PlanRuntime.capture(update);
		if (!("update" in captured)) throw new Error(captured.reason);
		expect(PlanRuntime.capture(captured.update)).toEqual({ update: captured.update });
		expect(PlanRuntime.capture(update)).toEqual({ accepted: false, reason: "invalid_revision" });
		expect(Object.isFrozen(captured.update)).toBe(true);
		expect(plan.apply(captured.update, 0)).toMatchObject({
			accepted: true,
			plan: { id: "plan", source: "source", revision: kind === "proposal" ? 1 : 2, draftTokens: 3 },
		});
		const capturedAction = "actions" in captured.update ? captured.update.actions[0] : captured.update.upsert![0];
		expect(plan.get("plan", "keyed")?.action).toBe(capturedAction);
		const narrowed = PlanRuntime.capture(captured.update, false);
		if (!("update" in narrowed)) throw new Error(narrowed.reason);
		expect("actions" in narrowed.update ? narrowed.update.actions : narrowed.update.upsert).toEqual(kind === "proposal" ? [capturedAction] : []);
		path = "mutated.ts";
		expect(reads).toBe(1);
		expect(plan.get("plan", "keyed")?.action).toMatchObject({ id: "keyed", tool: "read", background: true });
		expect(plan.get("plan", "keyed")?.action.feedback).toBe(feedback);
		expect(plan.get("plan", "peer")?.action.dependsOn).toEqual([{ actionID: "keyed", condition: "execution_settled" }]);
		if (kind === "delta") expect(plan.get("plan", "retained")).toBeDefined();
		expect(Object.isFrozen(offered)).toBe(false);
		const key = {
			key: "key",
			hash: "hash",
			tool: "read",
			input: { path: "keyed.ts" },
			resources: ["keyed.ts"],
			semanticsEpoch: "1",
			schemaHash: "schema",
			executionFingerprint: "executor",
		};
		expect(plan.bindActionKey("plan", "keyed", key)).toBe(true);
		expect(plan.bindActionKey("plan", "keyed", { ...key, hash: "other" })).toBe(false);
		expect(plan.get("plan", "keyed")?.actionKey).toBe(key);
		expect(plan.get("plan", "keyed")?.action.input).toEqual({ path: "keyed.ts" });
		expect(Object.isFrozen(plan.get("plan", "keyed")?.action.input)).toBe(true);
		const invalid = new PlanRuntime();
		if (kind === "delta") invalid.apply(proposal([action("retained")]), 0);
		const uncloneable = action("uncloneable", { horizon: 1, input: { callback: () => undefined } });
		const future = kind === "proposal" ? proposal([action("now"), uncloneable])
			: { proposalID: "plan", source: "source", revision: 2, upsert: [uncloneable], remove: ["retained"] };
		expect(invalid.apply(future, 0)).toEqual({ accepted: false, reason: "invalid_action" });
		const immediate = PlanRuntime.capture(future, false);
		if (!("update" in immediate)) throw new Error(immediate.reason);
		expect(invalid.apply(immediate.update, 0)).toMatchObject({
			accepted: true, plan: { actions: kind === "proposal" ? [{ id: "now" }] : [] },
		});
	});

	it("keeps an execution-blocked node matchable without making it launchable", () => {
		const plan = new PlanRuntime();
		plan.apply(proposal([action("bash")]), 0);
		const key = {
			key: "key",
			hash: "hash",
			tool: "bash",
			input: { command: "npm test" },
			resources: ["."],
			semanticsEpoch: "pi.bash.v2",
			schemaHash: "schema",
			executionFingerprint: "pi.bash.local.v2",
		};

		expect(plan.bindActionKey("plan", "bash", key)).toBe(true);
		const blocked = cause("execution", "isolation_unavailable");
		expect(plan.markExecutionBlocked("plan", "bash", blocked)).toBe(true);
		expect(plan.markExecutionBlocked("plan", "bash", blocked)).toBe(false);
		expect(plan.launchable()).toEqual([]);
		expect(plan.matchable(1)).toMatchObject([
			{ actionKey: key, execution: { status: "execution_blocked", cause: blocked } },
		]);
	});

	it("keeps execution dependencies independent from Actor adoption", () => {
		const plan = new PlanRuntime();
		const dependency = { actionID: "parent", condition: "actor_adopted" as const };
		plan.apply(
			proposal([
				action("parent"),
				action("settled", { dependsOn: [{ actionID: "parent", condition: "execution_settled" }] }),
				action("succeeded", { dependsOn: [{ actionID: "parent", condition: "execution_succeeded" }] }),
				action("confirmed", { dependsOn: [dependency] }),
			]),
			0,
		);
		Reflect.set(dependency, "condition", "execution_succeeded");
		const exposed = plan.get("plan", "confirmed")!.action.dependsOn![0]!;
		const changed = Reflect.set(exposed, "condition", "execution_succeeded");

		expect(plan.takeReady(0).map((node) => node.action.id)).toEqual(["parent"]);
		const execution = new CandidateExecution<string>("shared");
		plan.attachExecution("plan", "parent", "candidate", execution);
		execution.start(0);
		execution.succeed("output", 1, 1);
		expect(plan.matchable(1).map((node) => node.action.id)).toEqual(["parent"]);
		expect(
			plan
				.matchable(2)
				.map((node) => node.action.id)
				.sort(),
		).toEqual(["parent", "settled", "succeeded"]);
		expect(
			plan
				.launchable()
				.map((node) => node.action.id)
				.sort(),
		).toEqual(["settled", "succeeded"]);
		expect(changed).toBe(false);
		expect(Object.isFrozen(exposed)).toBe(true);
		expect(Object.isFrozen(dependency)).toBe(false);
		const actor = { id: "actor", sequence: 1, turnID: "turn" } as const;
		const opportunity = plan.claimMatch("plan", "parent", actor, { kind: "exact", distance: 0 })!;

		plan.confirm(opportunity, actor, { status: "rejected", cause: cause("execution", "failed") });
		expect(
			plan
				.launchable()
				.map((node) => node.action.id)
				.sort(),
		).toEqual(["settled", "succeeded"]);
		expect(plan.drainBlocked().map((node) => node.action.id)).toEqual(["confirmed"]);
	});

	it.each(["forward", "reverse"] as const)("derives deadlines and critical paths independently of %s graph order", (order) => {
		const plan = new PlanRuntime();
		const short = action("short", { expectedDurationMs: 10, latestHorizon: 3 });
		const child = action("child", { expectedDurationMs: 80,
			dependsOn: [{ actionID: "short" }, { actionID: "critical", condition: "execution_succeeded" }] });
		const actions = [short, action("critical", { expectedDurationMs: 20, latestHorizon: 4 }), child,
			action("leaf", { expectedDurationMs: -1, horizon: 4, latestHorizon: 7, dependsOn: [{ actionID: "child" }] })];
		if (order === "reverse") actions.reverse();
		plan.apply(proposal(actions), 4);

		expect(plan.plan("plan")!.actions.map((action) => action.id)).toEqual(actions.map((action) => action.id));
		expect(plan.get("plan", "short")).toMatchObject({ expectedDecisionSeq: 5, latestDecisionSeq: 8, criticalPathMs: 91 });
		expect(plan.get("plan", "critical")).toMatchObject({ expectedDecisionSeq: 5, latestDecisionSeq: 9, criticalPathMs: 101 });
		expect(plan.get("plan", "child")).toMatchObject({ earliestDecisionSeq: 6, expectedDecisionSeq: 6, latestDecisionSeq: 10, criticalPathMs: 81 });
		expect(plan.get("plan", "leaf")).toMatchObject({ earliestDecisionSeq: 7, expectedDecisionSeq: 9, latestDecisionSeq: 12, criticalPathMs: 1 });
		const identity = plan.get("plan", "child")!.identity;
		expect(plan.apply({
			proposalID: "plan", source: "source", revision: 2,
			upsert: [action("child", {
				expectedDurationMs: 80,
				dependsOn: [{ actionID: "critical", condition: "execution_succeeded" }, { actionID: "short", condition: "execution_settled" }],
			})],
		}, 4)).toMatchObject({ accepted: true, retired: [] });
		expect(plan.get("plan", "child")!.identity).toBe(identity);
		expect(plan.get("plan", "child")!.action.dependsOn!.map((dependency) => dependency.actionID)).toEqual(["critical", "short"]);
		expect(plan.takeReady(4).map((node) => node.action.id)).toEqual(["critical", "short"]);

		const actor = { id: "actor", sequence: 99, decisionSequence: 7, turnID: "turn" } as const;
		const opportunity = plan.claimMatch("plan", "critical", actor, { kind: "exact", distance: 0 })!;
		plan.confirm(opportunity, actor, { status: "adopted", candidateID: "candidate" });
		expect(plan.get("plan", "child")).toMatchObject({
			earliestDecisionSeq: 8,
			expectedDecisionSeq: 8,
			latestDecisionSeq: 9,
		});
		expect(plan.get("plan", "leaf")).toMatchObject({ earliestDecisionSeq: 9, expectedDecisionSeq: 9, latestDecisionSeq: 12, criticalPathMs: 1 });
		const revisedChild = { ...child, horizon: 2, latestHorizon: 4, expectedDurationMs: 3.5 };
		expect(plan.apply({ proposalID: "plan", source: "source", revision: 3, upsert: [revisedChild] }, 4))
			.toMatchObject({ accepted: true, retired: [] });
		expect(plan.get("plan", "child")!.identity).toBe(identity);
		expect(plan.get("plan", "child")).toMatchObject({ earliestDecisionSeq: 8, expectedDecisionSeq: 8, latestDecisionSeq: 9, criticalPathMs: 4.5 });
		expect(plan.get("plan", "short")!.criticalPathMs).toBe(14.5);
		expect(plan.get("plan", "critical")!.criticalPathMs).toBe(24.5);
		expect(plan.apply({ proposalID: "plan", source: "source", revision: 4, remove: ["leaf"] }, 4).accepted).toBe(true);
		expect(plan.get("plan", "child")!.criticalPathMs).toBe(3.5);
		expect(plan.get("plan", "short")!.criticalPathMs).toBe(13.5);
		expect(plan.get("plan", "critical")!.criticalPathMs).toBe(23.5);
		expect(plan.apply({ proposalID: "plan", source: "source", revision: 5, upsert: [
			{ ...short, dependsOn: [{ actionID: "child" }] },
			{ ...revisedChild, dependsOn: [{ actionID: "critical", condition: "execution_succeeded" }] },
		] }, 8).accepted).toBe(true);
		expect(plan.get("plan", "short")).toMatchObject({ earliestDecisionSeq: 10, expectedDecisionSeq: 12, latestDecisionSeq: 14, criticalPathMs: 10 });
		expect(plan.get("plan", "child")).toMatchObject({ earliestDecisionSeq: 9, expectedDecisionSeq: 11, latestDecisionSeq: 13, criticalPathMs: 13.5 });
		expect(plan.get("plan", "critical")).toMatchObject({ earliestDecisionSeq: 7, expectedDecisionSeq: 7, latestDecisionSeq: 7, criticalPathMs: 33.5 });
		expect(plan.plan("plan")!.actions.map((action) => action.id)).toEqual(actions.filter((action) => action.id !== "leaf").map((action) => action.id));
		for (const invalid of [
			[action("a", { dependsOn: [{ actionID: "missing" }] })],
			[action("a", { dependsOn: [{ actionID: "a" }] })],
			[action("a", { dependsOn: [{ actionID: "b" }] }), action("b", { dependsOn: [{ actionID: "a" }] })],
		]) expect(new PlanRuntime().apply(proposal(invalid), 0)).toEqual({ accepted: false, reason: "invalid_dependency" });
	});

	it.each(["direct", "ancestor"] as const)("keeps a claimed opportunity authoritative through %s replacement", (mode) => {
		const plan = new PlanRuntime();
		const root = mode === "ancestor" ? "parent" : "target";
		const actions = [...(mode === "ancestor" ? [
			action("leaf", { dependsOn: [{ actionID: "target" }] }),
			action("target", { dependsOn: [{ actionID: "middle" }] }),
			action("middle", { dependsOn: [{ actionID: "parent" }] }),
		] : []), action(root, { input: { path: "old.ts" } }), action("independent")];
		plan.apply(proposal(actions), 0);
		const independent = plan.get("plan", "independent")!.identity, execution = new CandidateExecution<string>("shared");
		execution.start(0); execution.succeed("parent-output", 1, 1);
		for (const id of mode === "ancestor" ? ["parent", "middle", "independent"] : ["independent"])
			plan.attachExecution("plan", id, `${id}-candidate`, execution);
		const actor = { id: "actor", sequence: 4, turnID: "turn" } as const;
		const relation = { kind: "exact", distance: 0 } as const;
		const original = plan.claimMatch("plan", "target", actor, relation)!;
		const update = plan.apply(
			{
				proposalID: "plan",
				source: "source",
				revision: 2,
				upsert: [action(root, { input: { path: "new.ts" } })],
			},
			1,
		);

		const replaced = mode === "ancestor" ? ["parent", "middle", "target", "leaf"] : ["target"];
		expect(update).toMatchObject({ accepted: true, retired: replaced.map((id) => ({ node: { action: { id } } })) });
		if (!update.accepted) throw new Error(update.reason);
		expect(Object.isFrozen(update.upserted)).toBe(true);
		expect(update.upserted.map((action) => action.id)).toEqual(replaced);
		expect(update.plan.actions.map((action) => action.id)).toEqual(actions.map((action) => action.id));
		expect(plan.get("plan", "independent")?.identity).toBe(independent);
		expect(plan.get("plan", "independent")?.execution).toMatchObject({ status: "succeeded", candidateID: "independent-candidate" });
		for (const id of replaced) expect(plan.get("plan", id)?.execution).toEqual({ status: "deferred" });
		expect(original.settlement).toBeUndefined();
		expect(plan.opportunity("plan", "target")).not.toBe(original);
		expect(plan.confirm(original, actor, { status: "adopted", candidateID: "old-candidate" })).toMatchObject({
			observation: "observed",
			match: { matched: true, adoption: { status: "adopted" } },
		});
		expect(plan.opportunity("plan", "target")?.state).toEqual({ status: "pending" });
		const current = plan.get("plan", "target")!.identity;
		expect(plan.apply({ proposalID: "plan", source: "source", revision: 3,
			upsert: [action(root, { input: { path: "new.ts" }, horizon: 2, expectedDurationMs: 50 })] }, 1))
			.toMatchObject({ accepted: true, retired: [] });
		expect(plan.get("plan", "target")!.identity).toBe(current);
	});

	it("settles observed misses and unobserved control endings exactly once", () => {
		const plan = new PlanRuntime();
		plan.apply(proposal([action("miss"), action("aborted")]), 0);

		expect(plan.miss("plan", "miss", { id: "actor", sequence: 1, turnID: "turn" })).toMatchObject({
			observation: "observed",
			match: { matched: false },
		});
		expect(plan.unobserve("plan", "miss", cause("control", "late"))).toBeUndefined();
		expect(plan.unobserve("plan", "aborted", cause("control", "turn_aborted"))).toMatchObject({
			observation: "unobserved",
		});
	});

	it("lets only one concurrent Actor action claim an unsettled prediction", () => {
		const plan = new PlanRuntime();
		plan.apply(proposal([action("only")]), 0);
		const first = { id: "first", sequence: 1, turnID: "turn" };
		const second = { id: "first", sequence: 2, turnID: "other-turn" };
		const relation = { kind: "exact", distance: 0 } as const;

		expect(plan.claimMatch("plan", "only", first, relation)).toBeDefined();
		expect(plan.claimMatch("plan", "only", second, relation)).toBeUndefined();
		expect(plan.pending()).toHaveLength(0);
		expect(plan.unsettled()).toHaveLength(1);
		expect(plan.unobserve("plan", "only", cause("control", "shutdown"))).toBeUndefined();
		expect(plan.opportunity("plan", "only")?.state.status).toBe("matching");
		expect(
			plan.confirm(plan.opportunity("plan", "only")!, second, {
				status: "rejected",
				cause: cause("matching", "wrong_actor"),
			}),
		).toBeUndefined();
		expect(
			plan.confirm(plan.opportunity("plan", "only")!, first, {
				status: "rejected",
				cause: cause("freshness", "resource_changed"),
			}),
		).toMatchObject({ actorAction: first, match: { matched: true } });
	});
});

function proposal(actions: readonly PlanAction[]): PlanProposal {
	return { id: "plan", source: "source", revision: 1, actions };
}

function action(
	id: string,
	options: {
		readonly input?: unknown;
		readonly horizon?: number;
		readonly latestHorizon?: number;
		readonly dependsOn?: PlanAction["dependsOn"];
		readonly expectedDurationMs?: number;
	} = {},
): PlanAction {
	return {
		id,
		type: "tool_call",
		tool: "read",
		input: options.input ?? { path: `${id}.ts` },
		...(options.horizon !== undefined ? { horizon: options.horizon } : {}),
		...(options.latestHorizon !== undefined ? { latestHorizon: options.latestHorizon } : {}),
		...(options.dependsOn ? { dependsOn: options.dependsOn } : {}),
		...(options.expectedDurationMs !== undefined ? { expectedDurationMs: options.expectedDurationMs } : {}),
	};
}
