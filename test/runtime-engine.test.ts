import type { PlanAction } from "../src/plan-proposal.ts";
import { gated, deferred, barrier, nextTurn } from "./async.ts";
import { testBranch as world } from "./branch.ts";
import { describe, expect, it, vi } from "vitest";
import { type ActionProjectionRule, READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { buildPiActionKey, PI_ACTION_SEMANTICS, type ActionKey } from "../src/action-semantics.ts";
import { EffectTransactionCoordinator, effectCommitFailure } from "../src/effect-transaction.ts";
import {
	emptyWorldReuseMetrics,
	type SpeculativeExecutionRoute,
	type WorldBranch,
} from "../src/execution-world.ts";
import type {
	MaterializedSpeculativeCandidate,
	PreparedActorCall,
	SpeculativeActionRuntimeAdapter,
	SpeculativeActionEvent,
	SpeculativeActionSettings,
} from "../src/runtime.ts";
import { makeSpeculativeActionRuntime } from "../src/runtime.ts";
import { CandidateStore } from "../src/candidate-stores.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";
import { cause, type PredictionSettlement, type ResourceValidation, zeroValidationMetrics } from "../src/settlement.ts";
import { emptySpeculativeTraceSummary, reduceSpeculativeTrace, summarizeSpeculativeTrace } from "../src/trace-summary.ts";

interface Start<SessionID = string> {
	readonly sessionID: SessionID;
	readonly turnID: string;
}

interface Call<SessionID = string> extends Start<SessionID> {
	readonly id?: string;
	readonly tool: string;
	readonly input: Record<string, unknown>;
	readonly terminal?: boolean;
}

const settings: SpeculativeActionSettings = {
	enabled: true,
	resourceCacheMaxEntries: 32,
	resourceCacheMaxBytes: 1024 * 1024,
	predictionTimeoutMs: 100,
	maxConcurrentActions: 8,
	tools: ["read", "write", "bash"],
};

const RESOURCE_ROUTE: SpeculativeExecutionRoute = {
	isolation: "resource_snapshot",
	reuse: "shared_result",
	scope: "fallback",
	backend: "resource_version",
	fingerprint: "resource-version",
};

const MUTATION_ROUTE: SpeculativeExecutionRoute = {
	isolation: "workspace_branch",
	reuse: "exclusive_branch",
	scope: "fallback",
	backend: "test_world",
	fingerprint: "test-world",
};

type TestAdapter<SessionID = string> = SpeculativeActionRuntimeAdapter<SessionID, string, Start<SessionID>, Call<SessionID>, { readonly cwd: string }>;
type Source<SessionID = string> = NonNullable<TestAdapter<SessionID>["sources"]>[number];

function planSource(source: Omit<Source, "id" | "enabled"> & Partial<Pick<Source, "enabled">>): Source {
	return { id: "source", enabled: () => true, ...source };
}

function readAction<Input>(
	id: string,
	input: Input,
	options: Partial<Omit<PlanAction, "id" | "type" | "tool" | "input">> = {},
) {
	return { id, type: "tool_call" as const, tool: "read", ...options, input };
}

function plan(proposalID: string, input: Record<string, unknown> = { path: "README.md" }) {
	return { id: proposalID, source: "source", revision: 0, actions: [readAction("next", input, { feedback: proposalID })] };
}

function childPlanUpdate(
	context: { readonly proposalID: string; readonly actionID: string; readonly revision: number },
	id: string,
	path: string,
) {
	return {
		proposalID: context.proposalID,
		source: "source",
		revision: context.revision,
		upsert: [
			readAction(id, { path }, { dependsOn: [{ actionID: context.actionID, condition: "execution_succeeded" as const }] }),
		],
	};
}

function validResource() {
	return { status: "valid" as const, metrics: zeroValidationMetrics() };
}

function harness<SessionID = string>(input: Partial<Pick<TestAdapter<SessionID>,
	"settings" | "stateData" | "actionKey" | "resolveExecution" | "captureAuthoritativeResult" |
	"preflightCandidate" | "authorizeCandidate" | "onCandidateMaterialized" | "onTurnFinished" | "rejectCandidateOutput" | "executeCandidate"
>> & {
	readonly source: Source<SessionID>;
	readonly peers?: readonly Source<SessionID>[];
	readonly execute?: (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		signal: AbortSignal,
		parentWorld?: WorldBranch<string>,
	) => unknown | Promise<unknown>;
	readonly capture?: () => unknown | Promise<unknown>;
	readonly validate?: (version: unknown) => ResourceValidation;
	readonly projection?: ActionProjectionRule<string>;
	readonly onEvent?: false | TestAdapter<SessionID>["onEvent"];
}) {
	const events: SpeculativeActionEvent<SessionID>[] = [];
	let liveSummary = emptySpeculativeTraceSummary();
	const ready = candidateSucceeded<SessionID>();
	let executions = 0;
	const runtime = makeSpeculativeActionRuntime<SessionID, string, Start<SessionID>, Call<SessionID>, Call<SessionID>, { readonly cwd: string }>({
		sources: [input.source, ...(input.peers ?? [])],
		settings: input.settings ?? (() => settings),
		definitions: () => [{ name: "read" }, { name: "bash" }, { name: "write" }],
		stateData: input.stateData ?? (() => ({ cwd: "/workspace" })),
		actionKey: input.actionKey ?? ((tool, args) => buildPiActionKey(tool, args, "/workspace")),
		resolveExecution: input.resolveExecution ?? (({ tool }) => tool === "read" ? RESOURCE_ROUTE : tool === "write" ? MUTATION_ROUTE : undefined),
		captureAuthoritativeResult: input.captureAuthoritativeResult,
		rejectCandidateOutput: input.rejectCandidateOutput,
		actual: (call) => call,
		preflightCandidate: input.preflightCandidate ?? (() => ({ ok: true })),
		authorizeCandidate: input.authorizeCandidate,
		executeCandidate: input.executeCandidate ?? (async ({ tool, concrete, action, route, signal, parentWorld }) => {
			executions++;
			const version =
				route.isolation === "resource_snapshot" ? await (input.capture?.() ?? { version: 1 }) : undefined;
			const executed = await input.execute?.(tool, concrete, signal, parentWorld);
			if (isWorldBranch(executed)) return executed;
			return world((executed as string | undefined) ?? "speculative", {
				executionFingerprint: action.executionFingerprint,
				...(route.isolation === "resource_snapshot" ? { validate: async () => input.validate ? input.validate(version) : validResource() } : {}),
			});
		}),
		projectionRules: input.projection ? [input.projection] : [],
		onCandidateMaterialized: input.onCandidateMaterialized,
		onTurnFinished: input.onTurnFinished,
		onEvent: input.onEvent === false ? undefined : async (event) => {
			events.push(event);
			for (const value of Object.values(liveSummary)) if (value && typeof value === "object") Object.freeze(value);
			Object.freeze(liveSummary);
			liveSummary = reduceSpeculativeTrace(liveSummary, event);
			ready.observe(event);
			if (input.onEvent) await input.onEvent(event);
		},
	});
	return { runtime, events, executions: () => executions, ready, summary: () => {
		expect(summarizeSpeculativeTrace(events)).toEqual(liveSummary);
		return liveSummary;
	} };
}

function simulatedExecution(durationMs: number): TimelineInterval {
	// Protocol fixtures supply synthetic service spans; clock-sensitive cases record their own endpoints.
	return new TimelineInterval(0, durationMs);
}

async function runFallback(runtime: ReturnType<typeof harness<string>>["runtime"], actor: Call, durationMs = 1, output = "actor"): Promise<void> {
	const prepared = await runtime.prepareActorCall(actor);
	expect(prepared).toBeDefined();
	expect(prepared?.output).toBeUndefined();
	await prepared?.settle(simulatedExecution(durationMs), output);
}

function start(turnID: string): Start {
	return { sessionID: "session", turnID };
}

function call(turnID: string, input: Record<string, unknown> = { path: "README.md" }): Call {
	return { sessionID: "session", turnID, id: `call:${turnID}`, tool: "read", input };
}

describe("structural speculative runtime", () => {
	it.each(["same", "next", "unused"])("separates internal execution, matching and continuation in the %s turn even when an adapter collides keys", async mode => {
		const binding = Object.freeze({ backend: "process", identity: "child", permissionHash: "parent", executionMs: 2, expectedDurationMs: 3 });
		const complete = vi.fn(), materialized = vi.fn(), continuation = vi.fn(), settled = vi.fn();
		let adopted: Parameters<TestAdapter["executeCandidate"]>[0]["onOperationAdopted"];
		let acceptScope: Parameters<TestAdapter["executeCandidate"]>[0]["acceptOperationScope"];
		const source = planSource({ propose: () => ({ ...plan("internal"), actions: [
			{ ...readAction("child", { path: "README.md" }, { latestHorizon: 1 }), type: "operation", operation: binding },
			readAction("whole", { path: "README.md" }, { latestHorizon: 1 }),
		] }), continue: continuation, onSettled: settled });
		const { runtime, events, summary } = harness({ source, onCandidateMaterialized: materialized,
			executeCandidate: async ({ candidate, onOperationAdopted, acceptOperationScope }) => {
				complete(candidate.type);
				if (candidate.type === "operation") { adopted = onOperationAdopted; acceptScope = acceptOperationScope; }
				return world(candidate.type === "operation" ? "child only" : "whole tool", { validate: async () => validResource(),
					executionMetrics: candidate.type === "operation" ? { reuse: {
						...emptyWorldReuseMetrics(), requests: 2, hits: 1, crossTurnHits: 1, replayMs: 3,
					} } : {},
				});
			},
		});
		try {
			await runtime.startTurn(start("turn"));
			await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded").length).toBe(2);
			expect(acceptScope!(start("turn"))).toBe(true);
			expect(acceptScope!(start("next"))).toBe(false);
			const turn = mode === "next" ? "next" : "turn";
			if (mode === "next") {
				await runFallback(runtime, call("turn", { path: "unrelated" }));
				await runtime.finishTurn({ ...call("turn"), terminal: false });
				expect(acceptScope!(start("turn"))).toBe(false);
				await runtime.startTurn(start(turn));
				expect(acceptScope!(start(turn))).toBe(true);
			}
			expect(complete.mock.calls.map(([kind]) => kind).sort()).toEqual(["operation", "tool_call"]);
			expect(materialized).toHaveBeenCalledOnce();
			const receipt = { scope: { sessionID: "other", turnID: "turn" }, id: "launch:exec", sequence: 1, operationIdentity: "child" };
			expect(acceptScope!(receipt.scope)).toBe(false);
			adopted!(receipt);
			expect(settled).not.toHaveBeenCalled();
			const prepared = await runtime.prepareActorCall(call(turn));
			expect(prepared?.output).toBe("whole tool");
			if (mode !== "unused") adopted!({ ...receipt, scope: start(turn) });
			await runtime.finishTurn({ ...call(turn), terminal: mode === "unused" });
			expect(acceptScope!(start(turn))).toBe(false);
			expect(events.filter(event => event.type === "operation_prediction")).toMatchObject([{ settlement: mode === "unused" ? {
				observation: "unobserved", cause: { stage: "control", code: "session_terminal" },
			} : {
				prediction: { kind: "operation" }, actorAction: { kind: "operation" }, match: { matched: true, adoption: { status: "adopted" } },
			} }]);
			expect(summary()).toMatchObject({ operationPredictionsSettled: 1, operationPredictionsAdopted: mode === "unused" ? 0 : 1,
				candidateStarted: 2, candidateSucceeded: 2, candidateFailed: 0,
				processReuse: { requests: 2, hits: 1, crossTurnHits: 1, replayMs: 3 } });
			expect(continuation.mock.calls.every(([input]) => input.actionID === "whole")).toBe(true);
			expect(events.filter(event => event.type === "actor_action")).toHaveLength(mode === "next" ? 2 : 1);
		} finally { await runtime.dispose(); }
	});
	it.each(["call", "preview"] as const)("backs off shared failed work and immediately serves an Actor %s", async (mode) => {
		const materialized = [barrier(), barrier(), barrier()], failed = [barrier(), barrier()], recovered = barrier();
		const seen = [0, 0, 0], feedback: PredictionSettlement[] = [];
		let turn = 0, healthy = false;
		const attempted = deferred<ReturnType<SpeculationScheduler<object>["admit"]>>();
		const admit = SpeculationScheduler.prototype.admit;
		const admission = vi.spyOn(SpeculationScheduler.prototype, "admit").mockImplementation(function (this: SpeculationScheduler<object>, ...args) {
			const result = admit.apply(this, args);
			if (turn === 2) attempted.resolve(result);
			return result;
		});
		const source = (id: string): Source => ({ id, enabled: () => true,
			propose: ({ startInput }) => ({ ...plan(`${id}:${startInput.turnID}`, { path: "flaky.txt" }), source: id }),
			onSettled: ({ settlement }) => { feedback.push(settlement); },
		});
		const { runtime, executions: executionCount } = harness({ source: source("source"), peers: [source("peer")],
			onCandidateMaterialized: ({ turnID }) => {
				const index = Number(turnID);
				seen[index] = seen[index]! + 1;
				if (seen[index] === 2) materialized[index]!.arrive();
			},
			execute: async () => {
				await materialized[turn]!.promise;
				if (!healthy) throw new Error("temporary execution failure");
				return "recovered";
			},
			onEvent: (event) => {
				if (event.type !== "candidate") return;
				if (event.state.status === "failed") failed[Number(event.turnID)]?.arrive();
				if (event.state.status === "succeeded") recovered.arrive();
			},
		});
		try {
			for (; turn < 2; turn++) {
				await runtime.startTurn(start(String(turn))); await failed[turn]!.promise;
				const other = call(String(turn), { path: "other.txt" });
				await runFallback(runtime, other, 1000);
				await runtime.finishTurn(other);
			}
			const actor = call("2", { path: "flaky.txt" });
			await runtime.startTurn(actor); await materialized[2]!.promise;
			expect(await attempted.promise).toMatchObject({ admitted: false, reason: "failure_circuit" });
			expect(executionCount()).toBe(2);
			healthy = true;
			if (mode === "preview") { await runtime.previewActorCall(actor); await recovered.promise; }
			expect((await runtime.prepareActorCall(actor))?.output).toBe("recovered");
			await runtime.finishTurn({ ...actor, terminal: true });
			expect(executionCount()).toBe(3);
			expect(feedback).toHaveLength(6);
			expect(feedback.slice(-2)).toEqual(expect.arrayContaining(["source", "peer"].map(source =>
				expect.objectContaining({ prediction: expect.objectContaining({ source }), observation: "observed",
					match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }) }))));
		} finally { for (const ready of materialized) ready.arrive(); admission.mockRestore(); await runtime.dispose(); }
		expect(runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0, sharedCandidates: 0 });
	});

	it.each(["absent", "normal", "failed", "blocked"] as const)("owns settlement and task epochs independently of %s diagnostics", async (mode) => {
		const delivery = barrier(), completed = [barrier(), barrier()], feedback: PredictionSettlement[] = [], observed: string[] = [];
		const snapshots = vi.spyOn(CandidateStore.prototype, "snapshot"), timing = vi.spyOn(TaskTimeline.prototype, "recordTool");
		const { runtime, events, executions: executionCount } = harness({
			source: planSource({
				propose: ({ startInput }) => plan(startInput.turnID, { path: `${startInput.turnID}.txt` }),
				continue: ({ startInput }) => { completed[Number(startInput.turnID)]!.arrive(); return undefined; },
				observe: ({ action }) => { observed.push(String(action?.input.path)); return undefined; },
				onSettled: ({ settlement }) => { feedback.push(settlement); },
			}),
			onEvent: mode === "absent" ? false : () => {
				if (mode === "failed") throw new Error("injected diagnostic failure");
				if (mode === "blocked") return delivery.promise;
			},
		});
		try {
			for (let index = 0; index < 2; index++) {
				const actor = call(String(index), { path: `${index}.txt` });
				await runtime.startTurn(actor); await completed[index]!.promise;
				expect((await runtime.prepareActorCall(actor))?.output).toBe("speculative");
				await runtime.finishTurn({ ...actor, terminal: true });
			}
			expect(executionCount()).toBe(2);
			expect(observed).toEqual(["0.txt", "1.txt"]);
			expect(feedback).toHaveLength(2);
			for (const settlement of feedback) expect(settlement).toMatchObject({ observation: "observed", match: { matched: true, adoption: { status: "adopted" } } });
			expect(new Set(timing.mock.contexts).size).toBe(2);
			expect(runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0 });
			if (mode === "absent") { expect(snapshots).not.toHaveBeenCalled(); expect(events).toEqual([]); }
			else expect(snapshots).toHaveBeenCalled();
		} finally { delivery.arrive(); await runtime.dispose(); snapshots.mockRestore(); timing.mockRestore(); }
	});

	it.each(["number-string", "objects", "symbols", "strings"])("owns turns and cached results by the actual session identity: %s", async (kind) => {
		const ids: unknown[] = kind === "number-string" ? [1, "1"] : kind === "objects" ? [{}, {}] :
			kind === "symbols" ? [Symbol("session"), Symbol("session")] : ["A", "B"];
		const calls = ids.map((sessionID): Call<unknown> => ({ ...call("same-turn"), sessionID }));
		const closed: unknown[] = [], disposed: string[] = [];
		const { runtime } = harness<unknown>({ source: { id: "none", enabled: () => false, propose: () => undefined },
			onTurnFinished: ({ startInput }) => { closed.push(startInput.sessionID); },
			captureAuthoritativeResult: ({ action }) => ({ route: RESOURCE_ROUTE, dispose: () => {},
				seal: (output) => world(output, { executionFingerprint: action.executionFingerprint, validate: async () => validResource(),
					onDispose: () => { disposed.push(output); } }) }) });
		try {
			for (const [index, actor] of calls.entries()) {
				await runtime.startTurn(actor);
				const prepared = await runtime.prepareActorCall(actor);
				expect(prepared).toBeDefined(); expect(prepared?.output).toBeUndefined();
				await prepared?.settle(simulatedExecution(500), `session-${index}`);
			}
			expect(closed).toEqual([]);
			expect(calls.map(({ sessionID }) => runtime.inspect(sessionID).activeTurns)).toEqual([1, 1]);
			for (const [index, actor] of calls.entries())
				expect((await runtime.prepareActorCall(actor))?.output).toBe(`session-${index}`);
			await runtime.finishTurn(calls[0]!);
			expect(calls.map(({ sessionID }) => runtime.inspect(sessionID).activeTurns)).toEqual([0, 1]);
			await runtime.disposeSession(ids[0]);
			expect(disposed).toEqual(["session-0"]);
			expect((await runtime.prepareActorCall(calls[1]!))?.output).toBe("session-1");
			await runtime.finishTurn(calls[1]!);
			expect(closed.map((id) => ids.indexOf(id))).toEqual([0, 1]);
		} finally { await runtime.dispose(); }
		expect(disposed).toEqual(["session-0", "session-1"]);
	});

	it("serializes replacement with registration and closes the previous generation before launching another", async () => {
		const calls = [call("same-turn"), call("same-turn")], gate = gated();
		const closed: Start[] = [], predicted: Start[] = [], prediction = barrier();
		const { runtime } = harness({
			source: planSource({ propose: ({ startInput }) => {
				predicted.push(startInput); prediction.arrive(); return undefined;
			} }),
			stateData: async (input) => { if (input === calls[0]) { await gate.wait(); } return { cwd: "/workspace" }; },
			onTurnFinished: ({ startInput }) => { closed.push(startInput); },
		});
		try {
			const first = runtime.startTurn(calls[0]!); await gate.entered;
			const second = runtime.startTurn(calls[1]!); gate.release();
			await Promise.all([first, second]);
			expect(closed.map((input) => calls.findIndex((call) => call === input))).toEqual([0]);
			await prediction.promise;
			expect(predicted).toHaveLength(1); expect(predicted[0]).toBe(calls[1]);
			expect(runtime.inspect().activeTurns).toBe(1);
			await runtime.finishTurn(calls[1]!);
			expect(closed.map((input) => calls.findIndex((call) => call === input))).toEqual([0, 1]);
			expect(runtime.inspect().activeTurns).toBe(0);
		} finally { gate.release(); await runtime.dispose(); }
	});

	it.each(["unique", "duplicate", "absent", "same-input"] as const)("keeps result evidence with its execution handle through repeated and late settlement: %s", async (ids) => {
		for (const order of [[0, 1], [1, 0]]) {
			const seals: [unknown, string][] = [], disposals: unknown[] = [];
			const { runtime, events } = harness({ source: { id: "none", enabled: () => false, propose: () => undefined },
				captureAuthoritativeResult: ({ action }) => {
					const dispose = () => { disposals.push(action.input.path); };
					return { route: RESOURCE_ROUTE, dispose, seal: (output) => {
						seals.push([action.input.path, output]);
						return world(output, { executionFingerprint: action.executionFingerprint, onDispose: dispose });
					} };
				} });
			const calls = ["A", "B"].map((name) => ({ ...call("turn", { path: name }),
				id: ids === "unique" ? name : ids === "absent" ? undefined : "same" }));
			if (ids === "same-input") calls[1] = calls[0]!;
			try {
				await runtime.startTurn(calls[0]!);
				const prepared = await Promise.all(calls.map((actor) => runtime.prepareActorCall(actor)));
				expect(prepared[0]).not.toBe(prepared[1]);
				for (const handle of prepared) { expect(handle?.output).toBeUndefined(); expect(Object.isFrozen(handle)).toBe(true); }
				for (const index of order) {
					await prepared[index]?.settle(simulatedExecution(100), `content:${calls[index]!.input.path}:${index}`);
					await prepared[index]?.settle(simulatedExecution(100), "duplicate report");
				}
				expect(seals).toEqual(order.map((index) => [calls[index]!.input.path, `content:${calls[index]!.input.path}:${index}`]));
				const unfinished = await runtime.prepareActorCall({ ...calls[0]!, input: { path: "unfinished" } });
				await runtime.finishTurn(calls[0]!);
				expect(events.filter((event) => event.type === "actor_action").map((event) => event.settlement.actorAction.sequence))
					.toEqual(order.map((index) => index + 1));
				expect(disposals).toContain("unfinished");
				await runtime.startTurn(calls[0]!);
				const fresh = await runtime.prepareActorCall({ ...calls[0]!, input: { path: "C" } });
				expect(fresh?.output).toBeUndefined();
				await prepared[0]?.settle(simulatedExecution(100), "previous turn");
				await unfinished?.settle(simulatedExecution(100), "late previous turn");
				await fresh?.settle(simulatedExecution(100), "content:C");
				expect(seals.at(-1)).toEqual(["C", "content:C"]);
				expect(seals).toHaveLength(3);
			} finally { await runtime.dispose(); }
			expect(disposals.sort()).toEqual(["A", ids === "same-input" ? "A" : "B", "C", "unfinished"]);
		}
	});

	it.each(["requests", "single", "batch", "revisions", "observed", "observed-retained", "observed-aborted", "observed-terminal", "observed-disabled", "observed-disposed"] as const)("admits independent actions and proposals without head-of-line blocking: %s", async (mode) => {
		const slow = gated(), executed: string[] = [];
		const caller = new AbortController(), abandoned = deferred<void>(), retainedReady = candidateSucceeded(1, "slow.ts");
		const independentStarted = barrier(mode === "single" || mode === "revisions" || mode === "observed" ? 1 : 2);
		const replacementReady = candidateSucceeded(1, "replacement.ts");
		const keyed: string[] = [];
		const replacements: MaterializedSpeculativeCandidate<string>[] = [];
		const proposals = [
			{ id: "proposal:0", source: "source", revision: 0, actions: [
				readAction("slow", { path: "slow.ts" }),
				readAction("same-plan", { path: "same-plan.ts" }),
			] },
			plan("proposal:1", { path: "other-plan.ts" }),
		];
		const revisions = [proposals[0]!, { ...proposals[0]!, revision: 1 },
			{ ...plan("proposal:0", { path: "replacement.ts" }), revision: 2 }, proposals[1]!];
		const observed = [proposals[0]!, { proposalID: "proposal:0", source: "source", revision: 1, upsert: proposals[0]!.actions },
			{ proposalID: "proposal:0", source: "source", revision: 2, remove: ["slow"],
			upsert: [readAction("same-plan", { path: "replacement.ts" })] }, proposals[1]!];
		const revised = mode === "revisions" || mode === "observed";
		const observation = mode.startsWith("observed"), crossing = observation && !revised;
		const retained = mode === "observed-retained", aborted = mode === "observed-aborted";
		const source = planSource({
			proposalCount: () => mode === "requests" ? 2 : 1,
			propose: ({ proposalIndex }) => observation ? undefined : mode === "revisions" ? revisions : mode === "batch" ? proposals : proposals[proposalIndex],
			observe: ({ concrete }) => observation && concrete.path === "seed.ts" ? crossing ? proposals : observed : undefined,
		});
		const { runtime, events } = harness({
			source,
			actionKey: async (tool, args, context) => {
				if (context.type === "start") {
					keyed.push(String((args as { path?: unknown }).path));
					if (keyed.at(-1) === "slow.ts") {
						if (revised) {
							const revision = mode === "revisions" ? revisions[2]! : observed[2]!;
							Object.assign(revision, { [mode === "revisions" ? "id" : "proposalID"]: "proposal:1", revision: 3 });
							const replacement = "actions" in revision ? revision.actions![0]! : revision.upsert![0]!;
							replacement.id = "drifted"; replacement.input.path = "drifted-replacement.ts";
						}
						await slow.wait();
					}
				}
				return buildPiActionKey(tool, args, "/workspace");
			},
			execute: (_tool, concrete) => {
				executed.push(String(concrete.path));
				if (["same-plan.ts", "other-plan.ts"].includes(String(concrete.path))) independentStarted.arrive();
				return "speculative";
			},
			onCandidateMaterialized: (candidate) => { if (String(candidate.input.path).includes("replacement.ts")) replacements.push(candidate); },
			onEvent: event => {
				replacementReady.observe(event); retainedReady.observe(event);
				if (event.type === "prediction" && event.settlement.prediction.actionID === "slow" &&
					event.settlement.observation === "unobserved") abandoned.resolve();
			},
		});
		let turnID = "parallel-admission";
		try {
			await runtime.startTurn(start(turnID), caller.signal);
			if (observation) {
				const seed = call(turnID, { path: "seed.ts" });
				await runFallback(runtime, seed, 1, "Actor");
			}
			await slow.entered; await independentStarted.promise;
			if (revised) {
				expect(keyed).toContain("replacement.ts"); await replacementReady.promise;
				expect(keyed.filter(path => path === "slow.ts")).toHaveLength(1);
				expect(keyed).not.toContain("drifted-replacement.ts");
			} else expect(keyed).not.toContain("replacement.ts");
			expect(executed.sort()).toEqual([...(mode === "single" ? [] : ["other-plan.ts"]), revised ? "replacement.ts" : "same-plan.ts"]);
			if (crossing) {
				let closed = false;
				const closing = runtime.finishTurn(call(turnID)).then(() => { closed = true; });
				await nextTurn(); expect(closed).toBe(true); await closing;
				turnID = "next-decision"; await runtime.startTurn(start(turnID));
			}
			if (retained || aborted) {
				if (aborted) caller.abort();
				slow.release(); await (retained ? retainedReady.promise : abandoned.promise);
				expect(keyed.filter(path => path === "slow.ts")).toHaveLength(1);
				expect(executed.includes("slow.ts")).toBe(retained);
			}
			if (mode === "observed") {
				slow.release(); await runtime.finishTurn({ ...call(turnID), terminal: false });
				turnID = "next-decision"; await runtime.startTurn(start(turnID));
			}
			if (revised) expect(replacements).toMatchObject([{
				source: "source", proposalID: "proposal:0", actionID: mode === "revisions" ? "next" : "same-plan",
				input: { path: "replacement.ts" },
			}]);
			expect((await runtime.prepareActorCall(call(turnID, { path: retained ? "slow.ts" : revised ? "replacement.ts" : "same-plan.ts" })))?.output).toBe("speculative");
			if (crossing && !retained && !aborted) {
				let closed = false;
				const closing = (mode === "observed-disposed" ? runtime.dispose() : mode === "observed-disabled"
					? runtime.settingsChanged({ ...settings, enabled: false })
					: runtime.finishTurn({ ...call(turnID), terminal: true })).then(() => { closed = true; });
				await nextTurn(); expect(closed).toBe(false);
				expect(runtime.inspect().pendingPredictions).toBeGreaterThan(0);
				slow.release(); await closing;
				expect(executed).not.toContain("slow.ts");
				expect(runtime.inspect().pendingPredictions).toBe(0);
			}
		} finally {
			slow.release();
			await runtime.finishTurn({ ...call(turnID), terminal: true }); await runtime.dispose();
		}
		const settlements = events.filter(event => event.type === "prediction").map(event => event.settlement);
		expect(new Set(settlements.map(settlement => settlement.prediction.id)).size).toBe(settlements.length);
		if (revised) expect(settlements.filter(s => s.prediction.proposalID === "proposal:0" && s.observation === "observed"))
			.toMatchObject([{ prediction: { actionID: mode === "revisions" ? "next" : "same-plan" },
				match: { matched: true, adoption: { status: "adopted" } } }]);
	});

	it("settles matched and adopted as orthogonal facts exactly once", async () => {
		const settlements: PredictionSettlement[] = [];
		const issued = vi.fn(), admitted = vi.fn();
		const actionKey = vi.fn((tool: string, args: unknown) => buildPiActionKey(tool, args, "/workspace"));
		const offered = plan("stale", {});
		offered.actions[0]!.input = { path: "README.md" };
		const source = planSource({
			propose: ({ reportDraftTokens }) => { reportDraftTokens?.(3); return offered; },
			onIssued: issued, onAdmitted: admitted,
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		});
		const { runtime, events, summary, ready: candidateReady } = harness({
			source,
			validate: () => ({ status: "stale", cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() }),
			actionKey,
		});
		await runtime.startTurn(start("turn"));
		await candidateReady.promise;
		expect(summary().cache).toMatchObject({ resultEntries: 1, cacheCold: 1, cacheHot: 0 });

		const prepared = await runtime.prepareActorCall(call("turn"));
		expect(prepared?.output).toBeUndefined();
		await prepared?.settle(simulatedExecution(4), "actor");
		await runtime.finishTurn({ ...call("turn"), terminal: true });

		expect(settlements).toHaveLength(1);
		for (const notify of [issued, admitted]) {
			expect(notify.mock.contexts).toEqual([source]);
			expect(notify).toHaveBeenCalledWith({ proposalID: "stale", actionID: "next", feedback: "stale" });
		}
		expect(settlements[0]).toMatchObject({
			observation: "observed",
			match: { matched: true, adoption: { status: "rejected", cause: { stage: "freshness" } } },
		});
		const predictionEvents = events.filter((event) => event.type === "prediction");
		expect(predictionEvents).toHaveLength(1);
		expect(predictionEvents[0]!.type === "prediction" && predictionEvents[0]!.settlement).toBe(settlements[0]);
		expect(actionKey).toHaveBeenCalledTimes(2);
		expect(events.find((event) => event.type === "candidate")).toMatchObject({
			candidate: { draftTokens: 3, totalDraftTokens: 3 },
		});
		expect(summary()).toMatchObject({ predictionsSettled: 1, predictionsObserved: 1, predictionsMatched: 1, predictionsAdopted: 0,
			predictionPrecision: 1, adoptionYield: 0, predictionRejectedAfterMatch: { "freshness:resource_changed": 1 },
			actorActions: 1, speculativeHits: 0, actorFallbacks: 1, actorExecutionMs: 4, totalDraftTokens: 3 });
		const invalidTiming = events.map(event => event.type === "candidate" && event.state.status === "succeeded"
			? { ...event, state: { ...event.state, executionMs: Number.NaN } } : event);
		expect(summarizeSpeculativeTrace(invalidTiming).speculativeExecutionMs).toBe(0);
	});

	it("waits for an in-flight candidate to capture its resource baseline before validation", async () => {
		const captured = deferred<{ version: number }>();
		const captureStarted = barrier();
		const validate = vi.fn((version: unknown) =>
			version
				? validResource()
				: { status: "indeterminate" as const, cause: cause("freshness", "resource_version_missing"), metrics: zeroValidationMetrics() },
		);
		const source = planSource({
			propose: () => plan("in-flight"),
		});
		const { runtime } = harness({ source, capture: () => { captureStarted.arrive(); return captured.promise; }, validate });
		await runtime.startTurn(start("turn"));
		await captureStarted.promise;

		const consumed = runtime.prepareActorCall(call("turn")).then(prepared => prepared?.output);
		expect(validate).not.toHaveBeenCalled();
		captured.resolve({ version: 1 });
		await expect(consumed).resolves.toBe("speculative");
		expect(validate).toHaveBeenCalledOnce();
		expect(validate).toHaveBeenCalledWith({ version: 1 });
		await runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("waits for an unmeasured run up to the Actor's own cost rather than a placeholder duration", async () => {
		const started = deferred<void>(), { runtime } = harness({ source: planSource({ propose: ({ startInput }) => startInput.turnID === "cold" ? plan("cold") : undefined }),
			execute: async () => { started.resolve(); await new Promise((resolve) => setTimeout(resolve, 150)); return "cold"; } });
		try {
			await runtime.startTurn(start("calibration"));
			await runFallback(runtime, call("calibration"), 1000);
			await runtime.finishTurn({ ...call("calibration"), terminal: false });
			await runtime.startTurn(start("cold")); await started.promise;
			expect((await runtime.prepareActorCall(call("cold")))?.output).toBe("cold");
		} finally { await runtime.dispose(); }
	});

	it("bounds an uncalibrated in-flight join and falls back without cancelling the learning run", async () => {
		let enabled = false;
		const gate = gated();
		const source = planSource({
			enabled: () => enabled,
			propose: () => plan("bounded-join"),
			observesOperations: true,
			observe: () => undefined,
		});
		const { runtime, events, ready: candidateReady } = harness({ source, execute: async () => { await gate.wait(); return "learned"; } });

		await runtime.startTurn(start("calibration"));
		const calibration = call("calibration");
		await runFallback(runtime, calibration, 100);
		await runtime.finishTurn({ ...calibration, terminal: false });

		enabled = true;
		await runtime.startTurn(start("prediction"));
		await gate.entered;
		const prepared = await runtime.prepareActorCall(call("prediction"));
		expect(prepared?.output).toBeUndefined();
		expect(prepared?.observeOperations).toBe(true);

		gate.release();
		await candidateReady.promise;
		await prepared?.settle(simulatedExecution(100), "actor");
		await runtime.finishTurn({ ...call("prediction"), terminal: false });
		expect(
			events.find(
				(event) => event.type === "actor_action" && event.turnID === "prediction",
			),
		).toMatchObject({ settlement: { provider: { kind: "actor" }, rejections: [{ cause: { code: "candidate_join_deadline" } }] } });
		enabled = false;
		await runtime.startTurn(start("retained"));
		expect(await runtime.prepareActorCall(call("retained"))).toMatchObject({ output: "learned", observeOperations: false });
		await runtime.finishTurn({ ...call("retained"), terminal: true });
		const event = events.find((event) => event.type === "actor_action" && event.turnID === "retained");
		const retained = event?.type === "actor_action" ? event.settlement.provider : undefined;
		expect(retained?.kind).toBe("speculative");
		if (retained?.kind === "speculative") expect(retained.timing.expectedActorMs).toBeGreaterThanOrEqual(100);
	});

	it.each(["refresh", "disabled", "disposed", "unwrapped", "terminal", "replaced", "evicted", "concurrent-refresh"] as const)("keeps prediction launch ownership across validation: %s", async (mode) => {
		const ready = candidateSucceeded(), refreshed = candidateSucceeded(2);
		const late = mode === "concurrent-refresh", validating = barrier(), validationGate = barrier();
		const bindingGate = gated(), continued = barrier(), outputs: string[] = [];
		const coordinator = new EffectTransactionCoordinator<string>(), cleanup = vi.fn();
		const executed: string[] = [];
		let configured = settings, validations = 0;
		const refreshes = late || mode === "refresh" || mode === "replaced" || mode === "evicted";
		const source = planSource({
			propose: ({ startInput }) => startInput.turnID === "turn-3" ? undefined : (late && startInput.turnID === "turn-2"
				? ["first", "second"] : [startInput.turnID]).map((id) => plan(id)),
			continueOn: ["execution_succeeded"],
			continue: ({ output }) => { if (output !== "generation:1") { outputs.push(output); continued.arrive(); } return undefined; },
			observe: ({ concrete }) => mode === "replaced" && concrete.path === "replace.ts"
				? { proposalID: "turn-2", source: "source", revision: 1,
					upsert: [readAction("next", { path: "replacement.ts" })] } : undefined,
		});
		const { runtime } = harness({
			source,
			settings: () => configured,
			actionKey: async (tool, args, context) => {
				if (context.type === "start" && (args as { path: string }).path === "replacement.ts") { await bindingGate.wait(); }
				return buildPiActionKey(tool, args, "/workspace");
			},
			execute: (tool, concrete) => {
				const generation = executed.push(String(concrete.path));
				const branch = world(`generation:${generation}`, {
					executionFingerprint: buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint,
					validate: async () => {
						if (generation === 1) { validations++; validating.arrive(); await validationGate.promise; }
						return generation === 1 && mode !== "replaced" && mode !== "evicted"
							? { status: "indeterminate", cause: cause("freshness", "validation_failed"), metrics: zeroValidationMetrics() }
							: validResource();
					}, onDispose: cleanup,
				});
				return late || mode === "unwrapped" ? branch : coordinator.execute(coordinator.begin({ tool, route: RESOURCE_ROUTE }), async () => branch);
			},
			onEvent: (event) => { ready.observe(event); refreshed.observe(event); },
		});
		let closing: Promise<void> | undefined, closed = false;
		try {
			await runtime.startTurn(start("turn-1")); await ready.promise;
			const unrelated = call("turn-1", { path: "other.ts" });
			await runFallback(runtime, unrelated);
			await runtime.finishTurn({ ...unrelated, terminal: false });
			await runtime.startTurn(start("turn-2")); await validating.promise;
			if (late) {
				await nextTurn(); expect(validations).toBe(1);
				validationGate.arrive(); await refreshed.promise; await continued.promise;
				expect(outputs).toEqual(["generation:2"]); // Both consumers finish together; the source has one continuation slot.
			} else if (mode === "replaced") {
				const replacement = call("turn-2", { path: "replace.ts" });
				await runFallback(runtime, replacement); await bindingGate.entered;
			} else if (mode === "evicted") {
				await runtime.finishTurn({ ...call("turn-2"), terminal: false });
				configured = { ...settings, resourceCacheMaxBytes: 1 };
				await runtime.startTurn(start("turn-3"));
				expect(runtime.inspect().sharedCandidates).toBe(0);
			} else if (mode !== "refresh") {
				closing = (mode === "disposed" || mode === "unwrapped" ? runtime.dispose() : mode === "disabled"
					? runtime.settingsChanged({ ...settings, enabled: false })
					: runtime.finishTurn({ ...call("turn-2"), terminal: true })).then(() => { closed = true; });
				await nextTurn();
				if (mode !== "terminal") expect(closed).toBe(false);
			}
			validationGate.arrive(); await nextTurn();
			bindingGate.release(); await closing; await nextTurn();
			if (refreshes) await refreshed.promise;
			expect(executed).toEqual(["README.md", ...(refreshes ? [mode === "replaced" ? "replacement.ts" : "README.md"] : [])]);
			if (refreshes) {
				if (mode === "replaced") {
					await runtime.finishTurn({ ...call("turn-2"), terminal: false });
					await runtime.startTurn(start("turn-3"));
				}
				expect((await runtime.prepareActorCall(call(late || mode === "refresh" ? "turn-2" : "turn-3",
					{ path: mode === "replaced" ? "replacement.ts" : "README.md" })))?.output).toBe("generation:2");
			}
		} finally { validationGate.arrive(); bindingGate.release(); await closing; await runtime.dispose(); }
		expect(cleanup).toHaveBeenCalledTimes(executed.length);
	});

	it.each(["prediction", "continuation", "running", "sealed", "capture", "promotion", "sealing"] as const)("drains %s work before retiring its session", async (phase) => {
		const sourceWork = phase === "prediction" || phase === "continuation";
		for (const mode of sourceWork ? ["disabled", "disposed", "terminal"] as const : ["disabled", "disposed"] as const) {
			const started = barrier(), producerStarted = barrier(), expired = barrier(), finish = barrier(), cancelled = barrier(), releaseGate = gated();
			const ready = candidateSucceeded(); let released = false, observed = Promise.resolve();
			const cleanup = vi.fn(async () => { await releaseGate.wait(); released = true; });
			const observing = !sourceWork && phase !== "running" && phase !== "sealed";
			let production: Promise<ReturnType<typeof plan>> | undefined;
			const produce = (signal: AbortSignal) => production = (async () => {
				signal.addEventListener("abort", () => cancelled.arrive(), { once: true }); producerStarted.arrive();
				try {
					await finish.promise;
					if (mode === "disposed") throw new Error("late producer failure");
					return plan("late", { path: "late.ts" });
				}
				finally { await cleanup(); }
			})();
			const { runtime, summary, executions: executionCount } = harness({
				source: planSource({ enabled: () => !observing,
					timeoutMs: () => mode === "terminal" ? 0 : undefined,
					propose: ({ signal }) => phase === "prediction" ? produce(signal) : plan("late"),
					...(phase === "continuation" ? { continue: ({ signal }: { signal: AbortSignal }) => produce(signal) } : {}),
				}),
				execute: async (_tool, _input, signal) => {
					signal.addEventListener("abort", () => cancelled.arrive(), { once: true }); started.arrive();
					if (phase === "running") await finish.promise;
					return world("late", { onDispose: sourceWork ? undefined : cleanup });
				},
				onEvent: (event) => {
					ready.observe(event);
					if (event.type === "source_request" && event.request.settlement.status === "timeout") expired.arrive();
				},
				captureAuthoritativeResult: () => ({ route: RESOURCE_ROUTE, dispose: cleanup,
					seal: async (output) => { started.arrive(); if (phase === "sealing") await finish.promise; return world(output, { onDispose: cleanup }); } }),
				...(phase === "promotion" ? { rejectCandidateOutput: () => { throw new Error("optional cache policy failed"); } } : {}),
			});
			try {
				await runtime.startTurn(start("turn"));
				if (observing) {
					const prepared = await runtime.prepareActorCall(call("turn"));
					expect(prepared?.output).toBeUndefined();
					if (phase !== "capture") {
						observed = prepared!.settle(simulatedExecution(1), "actor");
						if (phase === "promotion") await observed; else await started.promise;
					}
				} else if (sourceWork) await producerStarted.promise;
				else await (phase === "running" ? started.promise : ready.promise);
				if (mode === "terminal") await expired.promise;
				const executions = executionCount();
				const closing = (mode === "disposed" ? runtime.dispose() : mode === "terminal"
					? runtime.finishTurn({ ...call("turn"), terminal: true })
					: runtime.settingsChanged({ ...settings, enabled: false }))
					.then(() => { expect(released, `${phase}: lifecycle returned before cleanup`).toBe(true); });
				const outcome = Promise.allSettled([closing]);
				if (phase === "running" || sourceWork) await cancelled.promise;
				if (phase === "sealing") await nextTurn();
				finish.arrive(); await releaseGate.entered;
				await nextTurn(); // Let the close continuation run; no elapsed-time race.
				releaseGate.release();
				expect(await outcome).toEqual([{ status: "fulfilled", value: undefined }]); await observed;
				expect(cleanup).toHaveBeenCalledOnce(); expect(executionCount()).toBe(executions);
				expect(runtime.inspect().sharedCandidates).toBe(mode === "terminal" && phase === "continuation" ? 1 : 0);
				if (mode === "terminal") expect(summary()).toMatchObject({ sourceOutcomes: { timeout: 1 },
					...(phase === "continuation" ? { predictionUnobserved: { "control:session_terminal": 1 } } : {}) });
			} finally { finish.arrive(); releaseGate.release(); await production?.catch(() => {}); await runtime.dispose(); }
		}
	});

	it("races proposals only after one valid binding and ignores cancelled materialization", async () => {
		for (const mode of ["empty", "invalid", "throw", "late"] as const) {
			const entered = barrier(3), first = barrier(), winner = barrier(), binding = barrier();
			const ready = candidateSucceeded(), aborted: number[] = [], materialized: string[] = [], abortReasons: string[] = [];
			const key = vi.fn(async (tool: string, args: unknown) => {
				if ((args as { path: string }).path === "first.ts") {
					if (mode === "late") await binding.promise;
					else if (mode === "throw") throw new Error("binding failed");
					else return undefined;
				}
				return buildPiActionKey(tool, args, "/workspace");
			});
			const { runtime, events, executions: executionCount } = harness({
				source: planSource({
					proposalCount: () => 3,
					concurrentProposalPolicy: () => "first_produced",
					propose: async ({ proposalIndex, signal }) => {
						signal.addEventListener("abort", () => { aborted.push(proposalIndex); abortReasons[proposalIndex] = signal.reason.code; }, { once: true });
						entered.arrive();
						await entered.promise;
						if (proposalIndex === 0) return mode === "empty" ? undefined : plan("first", { path: "first.ts" });
						if (proposalIndex === 1) {
							await winner.promise;
							return plan("winner");
						}
						return new Promise<undefined>((resolve) => signal.addEventListener("abort", () => resolve(undefined), { once: true }));
					},
				}),
				actionKey: key,
				onCandidateMaterialized: ({ input }) => { materialized.push(String(input.path)); },
				onEvent: (event) => {
					if (event.type === "source_request" && event.request.request.index === 0) first.arrive();
					ready.observe(event);
				},
			});
			try {
				await runtime.startTurn(start("turn"));
				await first.promise; await nextTurn();
				expect(aborted, mode).toEqual(mode === "late" ? [] : [0]);
				if (mode !== "late") expect(abortReasons[0]).toBe("source_slot_unused");
				winner.arrive();
				await ready.promise;
				expect(aborted, mode).toContain(2);
				expect(aborted, mode).not.toContain(1);
				if (mode === "late") expect(aborted).toContain(0);
				binding.arrive();
				expect((await runtime.prepareActorCall(call("turn")))?.output).toBe("speculative");
				await runtime.finishTurn({ ...call("turn"), terminal: true });
				expect([...aborted].sort()).toEqual([0, 1, 2]);
				expect(materialized, mode).toEqual(["README.md"]);
				expect(key).toHaveBeenCalledTimes(mode === "empty" ? 2 : 3);
				expect(executionCount()).toBe(1);
				expect(events).toContainEqual(expect.objectContaining({ type: "source_request",
					request: expect.objectContaining({ request: expect.objectContaining({ index: 2 }),
						settlement: expect.objectContaining({ status: "aborted", cause: expect.objectContaining({ code: "proposal_race_lost" }) }) }) }));
			} finally {
				winner.arrive();
				binding.arrive();
				await runtime.dispose();
			}
		}
	});

	it.each(["same", "alternate", "stale-before", "stale-after", "incompatible", "indeterminate", "exclusive", "denied"] as const)(
		"recalls sealed Actor observations with compatibility, freshness and authorization: %s", async (mode) => {
		let version = 1, captures = 0, seals = 0, now = 100;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const recalled = barrier(), outputs: string[] = [];
		const reusable = ["same", "alternate", "stale-after", "denied"].includes(mode);
		const fallback = mode === "stale-after" || mode === "denied";
		const validate = (captured: unknown): ResourceValidation => captured === version
			? validResource()
			: { status: "stale", cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() };
		const { runtime, events, summary, executions: executionCount } = harness({
			source: planSource({ continueOn: ["execution_succeeded"],
				propose: ({ startInput }) => startInput.turnID === "second" ? plan("recall") : undefined,
				continue: ({ output }) => { outputs.push(output); recalled.arrive(); return undefined; } }),
			resolveExecution: () => mode === "exclusive" ? MUTATION_ROUTE : mode === "same" ? RESOURCE_ROUTE
				: { ...RESOURCE_ROUTE, isolation: "runtime_sandbox", scope: "runtime", backend: "alternate", fingerprint: "alternate" },
			execute: () => { now += 6; return world(`fresh:${version}`, {
				executionFingerprint: buildPiActionKey("read", { path: "README.md" }, "/workspace")!.executionFingerprint,
				validate: async () => (validResource()) }); },
			authorizeCandidate: () => ({ ok: mode !== "denied", reason: "permission_changed" }),
			captureAuthoritativeResult: ({ action }) => {
				captures++; const captured = version;
				return { route: RESOURCE_ROUTE, dispose: () => {}, seal: async (output) => {
					seals++;
					const branch = world(output, { executionFingerprint: mode === "incompatible" ? "different-actor" : action.executionFingerprint,
						validate: async () => validate(captured) });
					return mode === "indeterminate" ? { ...branch, compatibility: { status: "indeterminate", backend: "test", code: "unknown" } } : branch;
				} };
			},
		});
		try {
			const first = call("first"), second = call("second");
			await runtime.startTurn(first);
			const original = await runtime.prepareActorCall(first);
			expect(original?.output).toBeUndefined(); now += 4;
			const execution = new TimelineInterval(now - 4, now);
			now += 50; // Observation may arrive after the executor has completed.
			await original?.settle(execution, "actor:1");
			await runtime.finishTurn({ ...first, terminal: false });
			now += 2;
			if (mode === "stale-before") version++;
			await runtime.startTurn(second); await recalled.promise;
			expect(executionCount()).toBe(reusable ? 0 : 1);
			expect(outputs).toEqual([reusable ? "actor:1" : `fresh:${version}`]);
			if (mode === "stale-after") version++;
			await runtime.previewActorCall(second);
			const prepared = await runtime.prepareActorCall(second);
			expect(prepared?.output).toBe(fallback ? undefined : outputs[0]);
			if (fallback) { now += 2; await prepared?.settle(new TimelineInterval(now - 2, now), "actor:2"); }
			else expect((await runtime.prepareActorCall(second))?.output).toBe(mode === "exclusive" ? "actor:1" : outputs[0]);
			expect(captures).toBe(fallback ? 2 : 1); expect(seals).toBe(captures);
			await runtime.finishTurn({ ...second, terminal: true });
			const providers = events.filter((event) => event.type === "actor_action").map((event) => event.settlement.provider);
			expect(providers[0]?.toolExecution).toBe(execution);
			if (reusable && !fallback) expect(providers[1]?.toolExecution).toBe(execution);
			expect(events.filter((event) => event.type === "prediction")).toHaveLength(1);
			expect(events.find((event) => event.type === "task")?.timing).toMatchObject({
				authoritativeToolCount: reusable && !fallback ? 1 : 2,
				toolExecutionMs: fallback ? 6 : reusable ? 4 : 10,
			});
			expect(summary()).toMatchObject({ tasks: 1, endToEndMs: now - 100, toolExecutionMs: fallback ? 6 : reusable ? 4 : 10,
				nonToolMs: reusable ? 52 : 58, serializedMs: now - 100 + (reusable ? 0 : 6), hiddenLatencyMs: reusable ? 0 : 6,
				speculativeExecutionMs: reusable ? 0 : 6, actorExecutionMs: fallback ? 6 : 4 });
		} finally { await runtime.dispose(); clock.mockRestore(); }
	});

	it("expires both pending and admitting next-action requests when the Actor intent arrives", async () => {
		let entered = 0;
		const proposalsEntered = barrier(2);
		const admission = gated();
		const requestsSettled = barrier(2);
		const source = planSource({
			requestLifetime: "actor_decision",
			proposalCount: () => 2,
			propose: ({ proposalIndex, signal }) => {
				entered++;
				proposalsEntered.arrive();
				if (proposalIndex === 0) return plan("empty", { path: "other.ts" });
				return new Promise((_, reject) => {
					signal.addEventListener("abort", () => reject(signal.reason), { once: true });
				});
			},
		});
		const { runtime, events, executions: executionCount } = harness({
			source,
			preflightCandidate: async () => {
				await admission.wait();
				return { ok: true };
			},
			onEvent: (event) => {
				if (event.type === "source_request") requestsSettled.arrive();
			},
		});
		await runtime.startTurn(start("turn"));
		await Promise.all([proposalsEntered.promise, admission.entered]);

		const prepared = await runtime.prepareActorCall(call("turn"));
		expect(prepared?.output).toBeUndefined();
		admission.release();
		await requestsSettled.promise;
		expect(executionCount()).toBe(0);
		expect(events.filter((event) => event.type === "source_request" && event.request.settlement.status === "aborted")).toHaveLength(1);
		await prepared?.settle(simulatedExecution(1), "actor");
		await runtime.finishTurn({ ...call("turn"), terminal: true });
		expect(runtime.inspect().pendingPredictions).toBe(0);
	});

	it.each(["matched", "terminal", "future", "next-terminal"] as const)("launches queued work only for current demand after Actor timings change: %s", async (mode) => {
		const queued = barrier(), materialized = barrier();
		const original = SpeculationScheduler.prototype.admit;
		const admission = vi.spyOn(SpeculationScheduler.prototype, "admit").mockImplementation(function (this: SpeculationScheduler<object>, job, forecasts, ...rest) {
			const result = original.call(this, job, forecasts, ...rest);
			if (!result.admitted && forecasts.length === (mode === "future" ? 2 : 1)) queued.arrive();
			return result;
		});
		const proposal = () => ({ ...plan("demand", {}), actions: [0, ...(mode === "future" ? [1] : [])].map((horizon) =>
			readAction(String(horizon), { path: "README.md" }, { horizon, expectedDurationMs: 500 }),
		) });
		const { runtime, executions: executionCount, ready: succeeded } = harness({
			source: planSource({
				propose: ({ startInput }) => mode !== "next-terminal" && startInput.turnID === "demand" ? proposal() : undefined,
				observe: ({ consumeInput }) => mode === "next-terminal" && consumeInput.turnID === "demand" ? proposal() : undefined }),
			onCandidateMaterialized: () => materialized.arrive(),
		});
		try {
			for (const [index, durationMs] of [1, 1000, 1000, 1000].entries()) {
				const actor = call(`seed-${index}`);
				await runtime.startTurn(actor);
				await runFallback(runtime, actor, durationMs, "native");
				await runtime.finishTurn(actor);
			}
			const actor = call("demand");
			await runtime.startTurn(actor); if (mode !== "next-terminal") await queued.promise;
			expect(executionCount()).toBe(0);
			await runFallback(runtime, actor, 1000, "native");
			if (mode === "next-terminal") await materialized.promise;
			await runtime.finishTurn({ ...actor, terminal: mode === "terminal" });
			if (mode === "next-terminal") {
				const done = call("done"); await runtime.startTurn(done);
				await runtime.finishTurn({ ...done, terminal: true });
			}
			if (mode === "future") await succeeded.promise;
			await nextTurn();
			expect(executionCount()).toBe(mode === "future" ? 1 : 0);
			expect(runtime.inspect().sharedCandidates).toBe(mode === "future" ? 1 : 0);
			if (mode === "future") {
				const next = call("next"); await runtime.startTurn(next);
				expect((await runtime.prepareActorCall(next))?.output).toBe("speculative");
			}
		} finally { await runtime.dispose(); admission.mockRestore(); }
	});

	it("orders queued predictions by the streamed tool name without holding the others back", async () => {
		const started: string[] = [], release = deferred<void>();
		const { runtime } = harness({ source: planSource({ propose: async () => { await release.promise; return plan("next", { path: "next.ts" }); } }),
			execute: async (_tool, input) => { started.push(String(input.path)); return "speculative"; } });
		try {
			await runtime.startTurn(start("turn"));
			await runtime.previewActorTool({ ...call("turn"), tool: "write" }); // The Actor is streaming a write call.
			release.resolve();
			await vi.waitFor(() => expect(started).toEqual(["next.ts"]));
		} finally { release.resolve(); await runtime.dispose(); }
	});

	it("re-arms a preempted future prediction so it relaunches once capacity returns", async () => {
		const executed: string[] = [], farStarted = barrier(), release = deferred<void>();
		const { runtime } = harness({ settings: () => ({ ...settings, maxConcurrentActions: 1 }),
			source: planSource({ propose: () => ({ id: "far", source: "source", revision: 0, actions: [readAction("far", { path: "far.ts" }, { horizon: 1 })] }) }),
			peers: [{ id: "peer", enabled: () => true, propose: async () => { await farStarted.promise; return { id: "near", source: "peer", revision: 0, actions: [readAction("near", { path: "near.ts" })] }; } }],
			execute: async (_tool, input, signal) => {
				const path = String(input.path); executed.push(path);
				if (executed.length === 1) { farStarted.arrive(); await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true })); }
				if (path === "near.ts") await release.promise;
				return path;
			} });
		try {
			await runtime.startTurn(start("turn"));
			await vi.waitFor(() => expect(executed).toEqual(["far.ts", "near.ts"]));
			release.resolve();
			await vi.waitFor(() => expect(executed).toEqual(["far.ts", "near.ts", "far.ts"]));
		} finally { release.resolve(); await runtime.dispose(); }
	});

	it("holds speculative capacity through cancellation and cleanup, but never queues the actual Actor behind it", async () => {
		for (const mode of ["producer", "preview", "queued", "running"] as const) {
			const executed: string[] = [], aborted: string[] = [];
			const busyStarted = barrier(), stop = barrier(), stopped = barrier(), cleanupGate = gated();
			const targetGate = gated(), targetQueued = barrier();
			const service = vi.spyOn(SpeculationScheduler.prototype, "observeSpeculativeService");
			const original = SpeculationScheduler.prototype.admit;
			const admission = vi.spyOn(SpeculationScheduler.prototype, "admit").mockImplementation(function (this: SpeculationScheduler<object>, job, forecasts, ...rest) {
				const result = original.call(this, job, forecasts, ...rest);
				if (forecasts[0]?.actionKeyHash === buildPiActionKey("read", { path: "target.ts" }, "/workspace")!.hash) targetQueued.arrive();
				return result;
			});
			const speculative = mode === "producer" || mode === "preview";
			const { runtime, summary } = harness({
				source: planSource({ propose: () => [
					{ id: "busy", source: "source", revision: 0, actions: [
						readAction("busy", { path: "busy.ts" }, { expectedLatencyBenefitMs: speculative ? 0 : 1 })] },
					...(mode === "preview" ? [] : [{ id: "target", source: "source", revision: 0, actions: [
						readAction("target", { path: "target.ts" }, { resourceDemand: mode === "queued" ? 2 : 1 })] }]),
				] }),
				settings: () => ({ ...settings, maxConcurrentActions: mode === "running" ? 2 : 1 }),
				actionKey: async (tool, args) => { if ((args as { path: string }).path === "target.ts") await busyStarted.promise; return buildPiActionKey(tool, args, "/workspace"); },
				execute: async (_tool, input, signal) => {
					const path = String(input.path); executed.push(path);
					if (path === "target.ts") { await targetGate.wait(); return "target"; }
					signal.addEventListener("abort", () => { aborted.push(path); stop.arrive(); }, { once: true }); busyStarted.arrive();
					await stop.promise; await stopped.promise;
					return world("busy", { onDispose: async () => { await cleanupGate.wait(); } });
				},
			});
			try {
				await runtime.startTurn(start("turn")); await busyStarted.promise;
				if (mode === "preview") await runtime.previewActorCall(call("turn", { path: "target.ts" }));
				if (mode === "running") await targetGate.entered;
				if (mode === "queued") await targetQueued.promise;
				if (speculative) {
					await stop.promise; await nextTurn();
					const cancelled = service.mock.calls.filter(([, , outcome]) => outcome === "cancelled");
					expect(cancelled).toHaveLength(1);
					expect(cancelled[0]![1]).toBeGreaterThan(0);
					expect(executed, "cancellation is not physical completion").toEqual(["busy.ts"]);
					stopped.arrive(); await cleanupGate.entered; await nextTurn();
					expect(service.mock.calls.filter(([, , outcome]) => outcome === "cancelled")).toEqual(cancelled);
					expect(summary()).toMatchObject({ candidateCancelled: 1, candidateTerminalCauses: { "admission:scheduler_preempted": 1 } });
					expect(executed, "cleanup still owns the resource slot").toEqual(["busy.ts"]);
					cleanupGate.release(); await targetGate.entered;
				}
				const consumed = runtime.prepareActorCall(call("turn", { path: "target.ts" })).then(prepared => prepared?.output);
				await targetGate.entered; targetGate.release();
				expect(await consumed).toBe("target");
				expect(executed).toEqual(["busy.ts", "target.ts"]);
				expect(aborted).toEqual(mode === "running" ? [] : ["busy.ts"]);
			} finally {
				stopped.arrive(); cleanupGate.release(); targetGate.release(); await runtime.dispose();
				const failures = service.mock.calls.filter(([, , failed]) => failed === true);
				service.mockRestore(); admission.mockRestore(); expect(failures).toEqual([]);
			}
		}
	});

	it.each((["no-reconstruction", "valid", "proof-missing", "uncovered", "rejected", "changed", "aborted", "running-unproven", "running-outside", "running-covered", "running-throws",
		"output-valid", "output-uncovered", "output-rejected", "output-opaque", "output-preferred", "input-lookup", "input-scope"] as const)
		.flatMap((scenario) => [false, ...(!scenario.startsWith("running") ? [true] : [])].map((preview) => [scenario, preview] as const)))(
	"adopts reconstructed input or owned output coverage only after stable evaluation: %s (preview=%s)", async (scenario, preview) => {
		const admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin");
		const adoption = vi.spyOn(SpeculationScheduler.prototype, "observeAdoption");
		const commit = vi.fn(async () => "committed"), queryDisposals: ReturnType<typeof vi.fn>[] = [];
		const gate = gated(), controller = new AbortController();
		const started = barrier(), completion = barrier(), authorized = barrier(), running = scenario.startsWith("running");
		const outputOnly = scenario.startsWith("output-");
		const succeeds = ["valid", "running-covered", "output-valid", "output-preferred", "input-lookup", "input-scope"].includes(scenario);
		const inputLookup = scenario.startsWith("input-"), scoped = scenario === "input-scope";
		let changed = false;
		const proof = async (): Promise<ResourceValidation> => changed
			? { status: "stale", cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() }
			: validResource();
		const validate = vi.fn(proof), queryValidate = vi.fn(proof);
		const actor = call("turn", { path: "README.md", offset: scenario === "running-outside" || inputLookup ? 200 : 10, limit: scenario === "running-unproven" ? 200 : 10 });
		const evidence = { complete: scenario !== "output-uncovered", view: { text: "narrow" } };
		const projection = { ...READ_RANGE_ACTION_KEY_PROJECTOR,
			canShareInFlight: scenario === "running-throws" ? () => { throw new Error("proof unavailable"); } : READ_RANGE_ACTION_KEY_PROJECTOR.canShareInFlight,
			captureCoverage: () => scenario === "output-opaque" ? Object.assign(Object.create({}), evidence) : evidence,
			projectOutput: ({ coverage }: { coverage: unknown }): string | undefined => {
				if (!outputOnly) return undefined;
				if (scenario === "output-rejected") throw new Error("projection failed");
				const borrowed = coverage as typeof evidence, output = borrowed.complete ? borrowed.view.text : undefined;
				borrowed.complete = false; borrowed.view.text = "changed by borrower";
				return output;
			} };
		const reconstruct = vi.fn<NonNullable<WorldBranch<string>["reconstruct"]>>(async (request) => {
			expect(request).toMatchObject({ args: actor.input, callID: actor.id, signal: controller.signal });
			await gate.wait();
			if (scenario === "rejected") throw new Error("evaluation failed");
			if (scenario === "uncovered") return undefined;
			const dispose = vi.fn(); queryDisposals.push(dispose);
			return { output: "narrow", dispose, ...(scoped ? { validate: queryValidate } : {}), ...(scenario === "proof-missing" ? { requiresQueryValidation: true as const } : {}) };
		});
		const { runtime, events, ready: candidateReady } = harness({
			source: planSource({
				propose: () => plan("projection", { path: "README.md", offset: 1, limit: 100 }) }),
			projection,
			authorizeCandidate: () => { authorized.arrive(); return { ok: true }; },
			execute: async () => { started.arrive(); if (running) await completion.promise;
				if (scenario === "output-valid") await new Promise<void>((resolve) => setTimeout(resolve, 5));
				return {
				...world("wide", { validate: scoped ? async () => { throw new Error("unrelated input is stale"); } : validate }),
				inputResources: [{ path: "/workspace/README.md" }],
				...(scenario === "no-reconstruction" || (outputOnly && scenario !== "output-preferred") ? {} : { reconstruct }),
				commit,
			}; },
		});
		await runtime.startTurn(start("turn"));
		await (running ? started.promise : candidateReady.promise);
		projection.canShareInFlight = () => true;
		if (!outputOnly) projection.projectOutput = () => "changed callback";
		else { evidence.complete = true; evidence.view.text = "changed by producer"; }
		const preparation = preview ? runtime.previewActorCall(actor, controller.signal) : Promise.resolve();
		if (preview) {
			if (!running && !outputOnly && scenario !== "no-reconstruction") await gate.entered;
			else await preparation;
			expect(validate).not.toHaveBeenCalled(); expect(commit).not.toHaveBeenCalled();
			if (scenario === "valid" || inputLookup) { gate.release(); await preparation; }
		}
		const consumed = runtime.prepareActorCall(actor, controller.signal).then(prepared => prepared?.output);
		try {
			if (running) {
				expect(await Promise.race([consumed, authorized.promise.then(() => "joined")])).toBe(succeeds ? "joined" : undefined);
				completion.arrive(); gate.release();
			} else if (scenario !== "no-reconstruction" && !outputOnly) {
				await gate.entered; changed = scenario === "changed";
				if (scenario === "aborted") controller.abort();
				gate.release();
			}
			expect(await consumed).toBe(succeeds ? "narrow" : undefined);
			expect(commit).toHaveBeenCalledTimes(succeeds && !scoped ? 1 : 0);
			expect(scoped ? queryValidate : validate).toHaveBeenCalledTimes(succeeds || scenario === "changed" ? 1 : 0);
			if (["rejected", "changed", "uncovered", "output-rejected"].includes(scenario)) expect(adoption).toHaveBeenCalledOnce();
			if (inputLookup) {
				expect((await runtime.prepareActorCall({ ...actor, id: "same-query" }))?.output).toBe("narrow");
				expect(reconstruct).toHaveBeenCalledOnce();
				changed = true;
				expect((await runtime.prepareActorCall({ ...actor, id: "stale-query" }))?.output).toBeUndefined();
				expect(reconstruct).toHaveBeenCalledOnce();
				expect(scoped ? queryValidate : validate).toHaveBeenCalledTimes(3);
			}
			if (scenario === "output-preferred") expect(reconstruct).not.toHaveBeenCalled();
			if (scenario === "output-valid") {
				expect((await runtime.prepareActorCall({ ...actor, id: "second-reader" }))?.output).toBe("narrow");
				expect(commit).toHaveBeenCalledTimes(2);
			}
			if (succeeds) {
				const request = admission.mock.lastCall![0], actorHash = buildPiActionKey(actor.tool, actor.input, "/workspace")!.hash;
				expect(request.actorIdentity?.actionKeyHash).toBe(actorHash);
				expect(adoption.mock.lastCall![0]).toEqual(request.adoptionIdentity);
				expect(request.adoptionIdentity).toMatchObject({ actionKeyHash: JSON.stringify([request.identity.actionKeyHash, actorHash]),
					operation: JSON.stringify([RESOURCE_ROUTE.backend, RESOURCE_ROUTE.fingerprint, RESOURCE_ROUTE.scope,
						RESOURCE_ROUTE.isolation, RESOURCE_ROUTE.reuse, inputLookup ? "inputs" : "read.range",
						...(preview || inputLookup || scenario === "output-valid" ? ["retained"] : [])]) });
			}
		} finally {
			completion.arrive(); gate.release(); await Promise.all([preparation, consumed]);
			await runtime.finishTurn({ ...actor, terminal: true }); await runtime.dispose();
			admission.mockRestore(); adoption.mockRestore();
		}
		for (const dispose of queryDisposals) expect(dispose).toHaveBeenCalledOnce();
		expect(events.find((event) => event.type === "task")?.timing?.authoritativeToolCount).toBe(succeeds ? outputOnly ? 2 : 1 : 0);
		if (scenario === "output-valid") expect(summarizeSpeculativeTrace(events)).toMatchObject({
			actorActions: 2, speculativeHits: 2, exactReuseHits: 0, partialResultReuseHits: 2,
			partialResultReuseByProjector: { "read.range": 2 }, hitRate: 1,
		});
		if (inputLookup) {
			expect(events.filter((event) => event.type === "actor_action").at(-1)?.settlement.matchedPredictions).toEqual([]);
			expect(events.filter((event) => event.type === "prediction").at(-1)?.settlement)
				.toMatchObject({ observation: "observed", match: { matched: false } });
		}
	});

	it.each((["joined", "failed", "denied", "aborted", "unbound", "retained"] as const).flatMap(mode => [false, true].map(preview => [mode, preview] as const)))(
	"joins an exact input consumer before rebuilding its source: %s (preview=%s)", async (mode, preview) => {
		const source = {}, gate = gated(), entered = barrier(), controller = new AbortController();
		const reconstruct = vi.fn(async () => ({ output: "reconstructed", capturedBytes: 16 }));
		const query = { path: "README.md", offset: 10, limit: 1 };
		let second = false;
		const { runtime, ready } = harness({
			source: planSource({ propose: () => plan(second ? "query" : "source", second ? query : { path: "README.md", offset: 1, limit: 1 }) }),
			authorizeCandidate: ({ candidate }) => ({ ok: mode !== "denied" || Number(candidate.input.offset) !== 10, reason: "denied" }),
			executeCandidate: async ({ concrete, action, inputs }) => {
				if (Number(concrete.offset) === 1) return { ...world("source", { executionFingerprint: action.executionFingerprint, validate: async () => validResource() }),
					inputSource: source, inputResources: [{ path: "/workspace/README.md" }], capturedBytes: 64, reconstruct };
				if (mode !== "unbound") expect([...inputs!("/workspace/README.md")]).toContain(source);
				entered.arrive(); await gate.wait();
				if (mode === "failed") throw new Error("consumer failed");
				return world("joined", { executionFingerprint: action.executionFingerprint, validate: async () => validResource() });
			},
		});
		let pending: ReturnType<typeof runtime.prepareActorCall> | undefined;
		try {
			await runtime.startTurn(start("seed")); await ready.promise;
			const seed = call("seed", { path: "README.md", offset: 1, limit: 1 });
			expect((await runtime.prepareActorCall(seed))?.output).toBe("source");
			if (mode === "retained") expect((await runtime.prepareActorCall({ ...seed, id: "cached", input: query }))?.output).toBe("reconstructed");
			await runtime.finishTurn(seed); second = true;
			await runtime.startTurn(start("query")); await entered.promise;
			if (preview) await runtime.previewActorCall(call("query", query), controller.signal);
			pending = runtime.prepareActorCall(call("query", query), controller.signal);
			await nextTurn(); await nextTurn();
			if (["joined", "failed", "aborted"].includes(mode)) expect(reconstruct).not.toHaveBeenCalled();
			else expect((await pending)?.output).toBe("reconstructed");
			if (mode === "aborted") controller.abort();
			gate.release();
			expect((await pending)?.output).toBe(mode === "joined" ? "joined" : mode === "aborted" ? undefined : "reconstructed");
			expect(reconstruct).toHaveBeenCalledTimes(["joined", "aborted"].includes(mode) ? 0 : 1);
		} finally { gate.release(); await pending; await runtime.dispose(); }
	});

	it.each(["actor", "prediction", "unscoped", "unproven"] as const)("retains independently provable inputs after the source output expires (%s)", async mode => {
		let changed = false, executions = 0;
		const source = {}, dispose = vi.fn(), commit = vi.fn();
		const stale = () => ({ status: "stale" as const, cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() });
		const validate = vi.fn(async () => changed ? stale() : validResource());
		const queryValidate = vi.fn(async () => validResource());
		const reconstruct = vi.fn(async ({ args }: { args: unknown }) => ({
			output: "sibling", ...(changed && mode !== "unproven" ? {
				validate: (args as { path: string }).path === "other.txt" ? queryValidate : async () => stale(),
			} : {}),
		}));
		const { runtime, ready, events } = harness({
			source: planSource({ propose: ({ startInput }) => startInput.turnID === "seed" || mode === "prediction" ? plan(startInput.turnID) : undefined }),
			executeCandidate: async ({ action }) => ++executions > 1
				? world("fresh", { executionFingerprint: action.executionFingerprint, validate: async () => validResource() })
				: { ...world("old", { validate, onDispose: dispose, onCommit: commit }),
					inputSource: source, inputResources: [{ path: "/workspace/README.md" }, { path: "/workspace/other.txt" }],
					...(mode !== "unscoped" ? { reconstructionScope: "current_action" as const } : {}), reconstruct },
		});
		try {
			await runtime.startTurn(start("seed")); await ready.promise;
			expect((await runtime.prepareActorCall(call("seed")))?.output).toBe("old");
			expect((await runtime.prepareActorCall(call("seed", { path: "other.txt" })))?.output).toBe("sibling");
			await runtime.finishTurn(call("seed")); changed = true;
			await runtime.startTurn(start("next"));
			if (mode === "prediction") await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded").length).toBe(2);
			expect((await runtime.prepareActorCall(call("next")))?.output).toBe(mode === "prediction" ? "fresh" : undefined);
			const checks = validate.mock.calls.length, commits = commit.mock.calls.length;
			for (const id of ["sibling", "retained-sibling"]) {
				expect((await runtime.prepareActorCall({ ...call("next", { path: "other.txt" }), id }))?.output)
					.toBe(mode === "unscoped" || mode === "unproven" ? undefined : "sibling");
			}
			expect(validate).toHaveBeenCalledTimes(checks); expect(commit).toHaveBeenCalledTimes(commits);
			if (mode === "actor" || mode === "prediction") {
				expect(reconstruct).toHaveBeenCalledTimes(2); expect(queryValidate).toHaveBeenCalledTimes(2);
				expect(dispose).not.toHaveBeenCalled();
			}
			if (mode !== "prediction") expect((await runtime.prepareActorCall({ ...call("next"), id: "still-stale" }))?.output).toBeUndefined();
			expect(executions).toBe(mode === "prediction" ? 2 : 1);
		} finally { await runtime.dispose(); }
		expect(dispose).toHaveBeenCalledOnce();
	});

	it.each(["repaired", "changed-again", "failed", "aborted", "unscoped"] as const)("bounds incremental reconstruction and validates its replacement (%s)", async mode => {
		const controller = new AbortController(), dispose = vi.fn();
		const stale = () => ({ status: "stale" as const, cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics(), reconstruct: true as const });
		const validate = vi.fn(async () => stale()), queryValidate = vi.fn(async () => mode === "changed-again" ? stale() : validResource());
		const reconstruct = vi.fn(async () => {
			if (mode === "failed") throw new Error("replacement unavailable");
			if (mode === "aborted") controller.abort();
			return { output: "repaired", validate: queryValidate, dispose };
		});
		const { runtime, ready } = harness({ source: planSource({ propose: () => plan("seed") }),
			execute: () => ({ ...world("expired", { validate }), inputSource: {}, inputResources: [{ path: "/workspace/README.md" }],
				...(mode === "unscoped" ? {} : { reconstructionScope: "current_action" as const }), reconstruct }),
		});
		try {
			await runtime.startTurn(start("seed")); await ready.promise;
			const result = await runtime.prepareActorCall(call("seed"), controller.signal);
			expect(result?.output).toBe(mode === "repaired" ? "repaired" : undefined);
			expect(validate).toHaveBeenCalledOnce();
			expect(reconstruct).toHaveBeenCalledTimes(mode === "unscoped" ? 0 : 1);
			expect(queryValidate).toHaveBeenCalledTimes(mode === "repaired" || mode === "changed-again" ? 1 : 0);
		} finally { await runtime.dispose(); }
		expect(dispose).toHaveBeenCalledTimes(mode === "failed" || mode === "unscoped" ? 0 : 1);
	});

	it.each([false, true])("keeps independent results and inputs after revocation (indexed=%s)", async indexed => {
		let bytes = 2048, budget = 4096;
		const dispose = vi.fn(), coordinator = new EffectTransactionCoordinator<string>();
		const reconstruct = vi.fn(async ({ args }: { args: unknown }) => (args as { path: string }).path === "sibling.txt"
			? { output: "sibling", validate: async () => validResource() } : undefined);
		const { runtime, ready } = harness({
			settings: () => ({ ...settings, resourceCacheMaxBytes: budget }),
			source: planSource({ propose: ({ startInput }) => startInput.turnID === "seed" ? plan("source") : undefined }),
			executeCandidate: async () => coordinator.execute(coordinator.begin({ tool: "read", route: RESOURCE_ROUTE }), async () => ({
				...world("retained", { validate: async () => validResource(), onDispose: dispose }),
				inputSource: {}, inputResources: [{ path: "/workspace/other.txt" }, { path: "/workspace/sibling.txt" }], reconstruct,
				get capturedBytes() { return bytes; },
				invalidateInputs: paths => { expect(paths).toHaveLength(1); expect(paths[0]!.replaceAll("\\", "/")).toMatch(/\/workspace\/other.txt$/);
					bytes = 64; return indexed ? paths : undefined; },
			})),
		});
		try {
			await runtime.startTurn(start("seed")); await ready.promise;
			const mutation = await runtime.prepareActorCall({ ...call("seed"), tool: "write", input: { path: "other.txt", content: "changed" } });
			expect(mutation?.output).toBeUndefined();
			await mutation!.settle(new TimelineInterval(1, 2), "written");
			expect(bytes).toBe(64); budget = 512;
			await runtime.finishTurn(call("seed")); await runtime.startTurn(start("reuse"));
			expect((await runtime.prepareActorCall(call("reuse", { path: "other.txt" })))?.output).toBeUndefined();
			expect(reconstruct).toHaveBeenCalledTimes(indexed ? 0 : 1);
			expect((await runtime.prepareActorCall({ ...call("reuse", { path: "sibling.txt" }), id: "sibling" }))?.output).toBe("sibling");
			expect(reconstruct).toHaveBeenCalledTimes(indexed ? 1 : 2);
			expect((await runtime.prepareActorCall(call("reuse")))?.output).toBe("retained");
			expect(dispose).not.toHaveBeenCalled();
		} finally { await runtime.dispose(); }
		expect(dispose).toHaveBeenCalledOnce();
	});

	it("aborts the observation signal of a closing turn so sources skip dropped predictions", async () => {
		const gate = deferred<void>(), aborted: boolean[] = [];
		const { runtime } = harness({ source: planSource({ propose: () => undefined,
			observe: async ({ signal }) => { if (!aborted.length) await gate.promise; aborted.push(Boolean(signal?.aborted)); return undefined; } }) });
		try {
			await runtime.startTurn(start("turn"));
			for (const id of ["first", "second"]) await runFallback(runtime, { ...call("turn"), id });
			const closing = runtime.finishTurn(call("turn"));
			gate.resolve(); await closing;
			expect(aborted).toEqual([true, true]);
		} finally { await runtime.dispose(); }
	});

	it("keeps running work through an unbounded native call and adopts it after validation", async () => {
		const started = deferred<void>(), release = deferred<void>();
		const { runtime, executions } = harness({ actionKey: (tool, args) => buildPiActionKey(tool, args, process.cwd()),
			source: planSource({ propose: () => plan("read") }), execute: async () => { started.resolve(); await release.promise; } });
		try {
			await runtime.startTurn(start("turn")); await started.promise;
			await runFallback(runtime, { ...call("turn"), id: "native", tool: "bash", input: { command: "git status" } });
			release.resolve();
			expect((await runtime.prepareActorCall(call("turn")))?.output).toBe("speculative");
			expect(executions()).toBe(1);
		} finally { await runtime.dispose(); }
	});

	it.each([false, true])("re-validates finished exclusive work off the Actor path after an unbounded native call (stale=%s)", async (stale) => {
		const validate = vi.fn(async () => stale ? { status: "stale" as const, cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() } : validResource());
		const { runtime, ready, executions } = harness({ actionKey: (tool, args) => buildPiActionKey(tool, args, process.cwd()), execute: () => world("written", { validate }),
			source: planSource({ propose: () => ({ ...plan("write"), actions: [{ id: "write", type: "tool_call", tool: "write", input: { path: "out.txt", content: "x" } }] }) }) });
		try {
			await runtime.startTurn(start("turn")); await ready.promise;
			await runFallback(runtime, { ...call("turn"), id: "native", tool: "bash", input: { command: "git status" } });
			await vi.waitFor(() => expect(executions()).toBe(stale ? 2 : 1));
			expect(validate).toHaveBeenCalled();
		} finally { await runtime.dispose(); }
	});

	it("charges a native call's matching, capture and settlement time as negative savings", async () => {
		let now = 100;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const { runtime, events } = harness({ source: { id: "none", enabled: () => false, propose: () => undefined },
			captureAuthoritativeResult: ({ action }) => { now += 3; return { route: RESOURCE_ROUTE, dispose: () => {},
				seal: (output) => { now += 4; return world(output, { executionFingerprint: action.executionFingerprint }); } }; } });
		try {
			const actor = call("turn"); await runtime.startTurn(actor);
			const prepared = await runtime.prepareActorCall(actor);
			now += 10; await prepared?.settle(new TimelineInterval(now - 10, now), "actor");
			await runtime.finishTurn({ ...actor, terminal: true });
			expect(events.find((event) => event.type === "task")?.timing).toMatchObject({ toolExecutionMs: 10, estimatedSavingsMs: -7 });
		} finally { await runtime.dispose(); clock.mockRestore(); }
	});

	it.each([0, 40])("calibrates loss and recovery per Actor call across competing cached results with %ims capture", async (captureMs) => {
		let now = 1, cost = 20;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const { runtime } = harness({ source: { id: "none", enabled: () => false, propose: () => undefined },
			captureAuthoritativeResult: ({ action }) => {
				now += captureMs / 2;
				return { route: RESOURCE_ROUTE, dispose: () => {}, seal: (output) => {
					now += captureMs / 2;
					return world(output, { executionFingerprint: action.executionFingerprint,
						validate: async () => { now += cost; return validResource(); } });
				} };
			} });
		const run = async (prefix: string, count: number) => {
			const reused: boolean[] = [];
			for (let index = 0; index < count; index++) {
				const actor = call(`${prefix}-${index}`); await runtime.startTurn(actor);
				const prepared = await runtime.prepareActorCall(actor); expect(prepared).toBeDefined();
				reused.push(prepared?.output !== undefined);
				if (prepared?.output === undefined) { now += 2; await prepared?.settle(new TimelineInterval(now - 2, now), "actor"); }
				else expect(prepared.output).toBe("actor");
				await runtime.finishTurn(actor);
			}
			return reused;
		};
		try {
			if (captureMs) expect((await run("capture-cost", 12)).slice(1).every(Boolean)).toBe(true);
			cost += captureMs;
			const loss = await run("loss", 32);
			if (!captureMs) expect(loss.slice(0, 5)).toEqual([false, true, true, true, true]);
			expect(loss.slice(-8).filter((reused) => !reused).length).toBeGreaterThanOrEqual(4);
			expect(loss.slice(-8).some(Boolean)).toBe(true);
			cost = 1;
			expect((await run("recovery", 256)).slice(-8).every(Boolean)).toBe(true);
		} finally { await runtime.dispose(); clock.mockRestore(); }
	});

	it.each(["workspace_mutation", "unbounded"] as const)("skips input retrieval for a bound %s action while retaining exact results", async effect => {
		const authorize = vi.fn(() => ({ ok: true as const })), reconstruct = vi.fn(async () => ({ output: "query" }));
		const { runtime, ready } = harness({ source: planSource({ propose: () => plan("inputs", { path: "input", offset: 1, limit: 1 }) }),
			actionKey: (tool, args) => PI_ACTION_SEMANTICS.buildKey(tool, args, "/workspace", "", (args as Record<string, unknown>).offset === 2 ? {
				fingerprint: "bound", semantics: { ...PI_ACTION_SEMANTICS.definition("read")!, epoch: `bound.${effect}`, effect },
			} : undefined), authorizeCandidate: authorize,
			execute: () => ({ ...world("source", { validate: async () => validResource() }), reconstruct,
				inputResources: [{ path: "/workspace/input" }], reconstructionScope: "current_action" as const }),
		});
		try {
			await runtime.startTurn(start("turn")); await ready.promise;
			expect((await runtime.prepareActorCall(call("turn", { path: "input", offset: 2, limit: 1 })))?.output).toBeUndefined();
			expect(authorize).not.toHaveBeenCalled(); expect(reconstruct).not.toHaveBeenCalled();
			expect((await runtime.prepareActorCall({ ...call("turn", { path: "input", offset: 1, limit: 1 }), id: "exact" }))?.output).toBe("source");
			expect(authorize).toHaveBeenCalledOnce(); expect(reconstruct).not.toHaveBeenCalled();
		} finally { await runtime.dispose(); }
	});

	it.each([[2, 4096, 0, 2, 10], [1, 4096, 0, 3, 10], [2, 128, 0, 3, 10], [2, 4096, 4096, 2, 10], [2, 4096, 0, 2, 10000]])("bounds sealed query results by %i entries and %i bytes with %i proof bytes (%i evaluations, %ims source)", async (entries, bytes, proofBytes, evaluations, sourceMs) => {
		const disposed = vi.fn(), queryDisposals: ReturnType<typeof vi.fn>[] = [];
		let now = 100;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now), admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin");
		const learned = entries === 2 && bytes === 4096 && !proofBytes, unretained = bytes === 128;
		const queryValidate = vi.fn(async () => { now += 3; return validResource(); });
		const reconstruct = vi.fn<NonNullable<WorldBranch<string>["reconstruct"]>>(async ({ args }) => { now += 20; const dispose = vi.fn(); queryDisposals.push(dispose); return { dispose, output: String((args as { offset: number }).offset), capturedBytes: proofBytes, ...(proofBytes ? { validate: queryValidate } : {}) }; });
		const { runtime, events, executions: executionCount, ready } = harness({
			source: planSource({ propose: ({ startInput }) => startInput.turnID === "first"
				? plan("inputs", { path: "input", offset: 1, limit: 1 }) : undefined }),
			settings: () => ({ ...settings, resourceCacheMaxEntries: entries, resourceCacheMaxBytes: bytes }),
			execute: () => { now += sourceMs; return { ...world("1", { onDispose: disposed,
				validate: async () => { now += 3; return validResource(); } }), reconstruct, inputResources: [{ path: "/workspace/input" }] }; },
		});
		try {
			await runtime.startTurn(start("first")); await ready.promise;
			for (const [index, offset] of [2, 3, 2].entries()) {
				const turnID = index === 2 ? "second" : "first";
				if (index === 2) {
					await runtime.finishTurn({ ...call("first"), terminal: false });
					await runtime.startTurn(start(turnID));
				}
				const actor = { ...call(turnID, { path: "input", offset, limit: 1 }), id: String(index) };
				if (!learned || index > 0) await runtime.previewActorCall(actor);
				expect((await runtime.prepareActorCall(actor))?.output).toBe(String(offset));
				if (learned && index === 0) {
					const scheduler = admission.mock.contexts[0] as SpeculationScheduler<object>, request = admission.mock.calls[0]![0];
					for (let sample = 0; sample < 4; sample++) {
						scheduler.observeActorService(request.actorIdentity!, 5);
						scheduler.observeAdoption(request.adoptionIdentity!, 100);
					}
				}
			}
			expect(reconstruct).toHaveBeenCalledTimes(evaluations); expect(executionCount()).toBe(1);
			await nextTurn();
			expect(queryDisposals.filter(dispose => dispose.mock.calls.length)).toHaveLength(proofBytes || unretained ? evaluations : entries === 1 ? evaluations - 1 : 0);
			expect(queryValidate).not.toHaveBeenCalled(); // Oversized proof falls back to the full proof without repeating the query.
			await runtime.finishTurn({ ...call("second"), terminal: true });
			expect(events.find((event) => event.type === "task")?.timing).toMatchObject({
				toolExecutionMs: evaluations * 20, authoritativeToolCount: evaluations,
				hiddenLatencyMs: learned || unretained ? 0 : proofBytes ? 20 : 40,
			});
			now += 10;
			await runtime.startTurn(start("next-task"));
			expect((await runtime.prepareActorCall(call("next-task", { path: "input", offset: 2, limit: 1 })))?.output).toBe("2");
			await runtime.finishTurn({ ...call("next-task"), terminal: true });
			expect(reconstruct).toHaveBeenCalledTimes(evaluations + (unretained ? 1 : 0));
			expect(events.filter((event) => event.type === "task").at(-1)?.timing).toMatchObject({
				toolExecutionMs: unretained ? 20 : 0, authoritativeToolCount: unretained ? 1 : 0, hiddenLatencyMs: 0,
				estimatedSavingsMs: unretained ? -3 : learned ? 2 : 17,
			});
		} finally { await runtime.dispose(); clock.mockRestore(); admission.mockRestore(); }
		expect(disposed).toHaveBeenCalledOnce(); expect(runtime.inspect().sharedCandidates).toBe(0);
		for (const dispose of queryDisposals) expect(dispose).toHaveBeenCalledOnce();
	});

	it("recomputes a proofless retained query instead of committing an input-only branch", async () => {
		const reconstruct = vi.fn<NonNullable<WorldBranch<string>["reconstruct"]>>(async ({ args }) => ({ dispose: vi.fn(), output: String((args as { offset: number }).offset), capturedBytes: 4096, validate: async () => validResource() }));
		const { runtime, ready } = harness({ source: planSource({ propose: () => plan("inputs", { path: "input", offset: 1, limit: 1 }) }),
			settings: () => ({ ...settings, resourceCacheMaxEntries: 2, resourceCacheMaxBytes: 4096 }),
			execute: () => ({ ...world("1"), inputsOnly: true as const, commit: () => Promise.reject(new Error("input_only_branch")), reconstruct, inputResources: [{ path: "/workspace/input" }] }) });
		try {
			await runtime.startTurn(start("first")); await ready.promise;
			for (const id of ["query", "repeat"]) expect((await runtime.prepareActorCall({ ...call("first", { path: "input", offset: 2, limit: 1 }), id }))?.output).toBe("2");
			expect(reconstruct).toHaveBeenCalledTimes(2);
		} finally { await runtime.dispose(); }
	});

	it("keeps an Actor query proof alive while its cached view is evicted", async () => {
		const gate = gated(), releases: ReturnType<typeof vi.fn>[] = [];
		let validating = false;
		const { runtime, ready, events } = harness({
			source: planSource({ propose: () => plan("inputs", { path: "input", offset: 1, limit: 1 }) }),
			settings: () => ({ ...settings, resourceCacheMaxEntries: 1, resourceCacheMaxBytes: 8192 }),
			execute: () => ({ ...world("1"), inputResources: [{ path: "/workspace/input" }],
				reconstruct: async ({ args }: Parameters<NonNullable<WorldBranch<string>["reconstruct"]>>[0]) => {
					const offset = (args as { offset: number }).offset, dispose = vi.fn(); releases.push(dispose);
					return { output: String(offset), dispose, capturedBytes: 4096, requiresQueryValidation: true,
						validate: async () => { if (validating && offset === 2) await gate.wait(); expect(dispose).not.toHaveBeenCalled(); return validResource(); } };
				} }),
		});
		let actor: ReturnType<typeof runtime.prepareActorCall> | undefined;
		try {
			await runtime.startTurn(start("turn")); await ready.promise;
			const query = call("turn", { path: "input", offset: 2, limit: 1 });
			await runtime.previewActorCall(query); validating = true;
			actor = runtime.prepareActorCall(query); await gate.entered;
			await runtime.previewActorCall({ ...call("turn", { path: "input", offset: 3, limit: 1 }), id: "evict" });
			await nextTurn(); expect(releases).toHaveLength(2); expect(releases[0]).not.toHaveBeenCalled();
			gate.release(); expect((await actor)?.output).toBe("2");
			await runtime.finishTurn({ ...query, terminal: false });
			expect(events.at(-1)!.cache.resultBytes).toBeGreaterThanOrEqual(4096);
		} finally { gate.release(); await actor; await runtime.dispose(); }
		for (const dispose of releases) expect(dispose).toHaveBeenCalledOnce();
	});

	it.each(["input", "executor", "denied", "closing"])("keeps prepared intent non-authoritative through %s", async (phase) => {
		const gate = gated();
		const disposed = vi.fn(), committed = vi.fn(), coordinator = new EffectTransactionCoordinator<string>();
		const gateway = new ToolExecutionGateway<unknown, string>([]), actor = vi.fn(async () => "Actor");
		let executor = "bound", allowed = true;
		const query = call("turn", { path: "README.md", offset: 10, limit: 1 });
		const { runtime, ready } = harness({
			source: planSource({ propose: () => plan("inputs", { path: "README.md", offset: 1, limit: 1 }) }),
			actionKey: (tool, input) => PI_ACTION_SEMANTICS.buildKey(tool, input, "/workspace", "", { fingerprint: executor }),
			authorizeCandidate: () => allowed ? { ok: true } : { ok: false, reason: "denied" },
			execute: () => coordinator.execute(coordinator.begin({ tool: "read", route: RESOURCE_ROUTE }), async () => ({
				inputResources: [{ path: "/workspace/README.md" }],
				...world("1", { executionFingerprint: "bound", onDispose: disposed, onCommit: committed,
					validate: async () => (validResource()) }),
				reconstruct: async ({ args }) => {
					const offset = (args as { offset: number }).offset;
					if (offset === 10) { await gate.wait(); }
					return { output: String(offset) };
				},
			})),
		});
		let preparation: Promise<void> | undefined, closing: Promise<void> | undefined;
		try {
			await runtime.startTurn(start("turn")); await ready.promise;
			preparation = runtime.previewActorCall(query); await gate.entered;
			expect(committed).not.toHaveBeenCalled();
			if (phase === "closing") {
				closing = runtime.dispose();
				expect(await Promise.race([closing.then(() => "closed"), new Promise<string>((resolve) => setImmediate(() => resolve("pending")))])).toBe("pending");
				expect(disposed).not.toHaveBeenCalled();
			} else {
				if (phase === "executor") executor = "rebound";
				if (phase === "denied") allowed = false;
				const formal = phase === "input" ? { ...query, input: { ...query.input, offset: 20 } } : query;
				let prepared: PreparedActorCall<string> | undefined;
				const delivered = gateway.executeAuthoritative({ tool: formal.tool, input: formal.input }, actor, {
					reuse: async () => { prepared = await runtime.prepareActorCall(formal); return prepared?.output; }, settled: async (result) => {
						if (result.status === "succeeded") await prepared?.settle(result.toolExecution, result.output);
					},
				});
				expect(await delivered).toBe(phase === "input" ? "20" : "Actor");
				expect(actor).toHaveBeenCalledTimes(phase === "input" ? 0 : 1);
				expect(committed).toHaveBeenCalledTimes(phase === "input" ? 1 : 0);
			}
		} finally {
			gate.release(); await Promise.all([preparation, closing]);
			await runtime.dispose(); await gateway.dispose();
		}
		expect(disposed).toHaveBeenCalledOnce(); expect(runtime.inspect().sharedCandidates).toBe(0);
	});

	it.each(["poisoned", "terminal", "disposed"] as const)("preserves claimed Actor commit ownership through %s", async (phase) => {
		const poisoned = effectCommitFailure(new Error("rollback failed"), "poisoned");
		const gate = gated();
		const coordinator = new EffectTransactionCoordinator<string>(), cleanup = vi.fn();
		const continuation = vi.fn(() => undefined), settlements: PredictionSettlement[] = [];
		const commit = vi.fn(async () => { await gate.wait(); if (phase === "poisoned") throw poisoned; return "speculative"; });
		const source = planSource({
			propose: () => plan("claimed"),
			continueOn: ["actor_adopted"], continue: continuation,
			onSettled: ({ settlement }) => { settlements.push(settlement); },
		});
		const { runtime, ready: candidateReady } = harness({
			source,
			execute: (tool, concrete) => coordinator.execute(coordinator.begin({ tool, callID: "claimed", route: RESOURCE_ROUTE }), async () => ({
				...world("speculative", { executionFingerprint: buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint }),
				validate: async () => (validResource()),
				commit, dispose: cleanup,
			})),
		});
		let consuming: Promise<string | undefined> | undefined, closing: Promise<void> | undefined;
		try {
			await runtime.startTurn(start("turn")); await candidateReady.promise;
			consuming = runtime.prepareActorCall(call("turn")).then(prepared => prepared?.output); await gate.entered;
			if (phase !== "poisoned") closing = phase === "disposed" ? runtime.dispose()
				: runtime.finishTurn({ ...call("turn"), terminal: true });
			await nextTurn();
			gate.release();
			if (phase === "poisoned") await expect(consuming).rejects.toBe(poisoned);
			else expect(await consuming).toBe("speculative");
			await closing; await runtime.dispose();
			expect(commit).toHaveBeenCalledTimes(1); expect(cleanup).toHaveBeenCalledTimes(1);
			expect(continuation).not.toHaveBeenCalled();
			if (phase !== "poisoned") expect(settlements).toEqual([expect.objectContaining({
				match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }),
			})]);
		} finally { gate.release(); await Promise.allSettled([consuming, closing]); await runtime.dispose(); }
	});

	it.each(["indeterminate", "compatibility_drift", "classified", "unclassified"] as const)("preserves %s rejection through the transaction and Actor fallback", async (scenario) => {
		const indeterminate = scenario === "indeterminate", incompatible = indeterminate || scenario === "compatibility_drift";
		const actor = indeterminate ? call("turn") : { ...call("turn"), tool: "write", input: { path: "a.txt", content: "a" } };
		const failure = cause("freshness", "backend_conflict"), dispose = vi.fn();
		const commit = vi.fn(async () => {
			if (incompatible) return "speculative";
			throw scenario === "classified" ? effectCommitFailure(new Error("changed"), "recoverable", "changed", failure)
				: effectCommitFailure(new Error("commit failed"), "recoverable");
		});
		const transactions = new EffectTransactionCoordinator<string>();
		const gateway = new ToolExecutionGateway<undefined, string>([]), executeActor = vi.fn(async () => "Actor");
		const settlements: PredictionSettlement[] = [];
		const { runtime, events, ready: candidateReady } = harness({
			source: planSource({
				propose: () => indeterminate ? plan("incompatible", actor.input) : undefined,
				onSettled: ({ settlement }) => { settlements.push(settlement); } }),
			execute: async (tool, concrete) => {
				const fingerprint = buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint;
				const source = { ...world("speculative", { resources: ["a.txt"], executionFingerprint: fingerprint }), capturedBytes: 1,
					compatibility: incompatible
						? { status: indeterminate ? "indeterminate" as const : "incompatible" as const, backend: "test", code: indeterminate ? "attestation_missing" : "sealed_incompatible" }
						: { status: "compatible" as const, backend: "test", executionFingerprint: fingerprint },
					validate: async () => (validResource()), commit, dispose };
				const transaction = await transactions.execute(transactions.begin({ tool, callID: actor.id,
					route: indeterminate ? RESOURCE_ROUTE : MUTATION_ROUTE }), async () => source);
				Object.assign(source.compatibility, { status: "compatible", executionFingerprint: fingerprint });
				return transaction;
			},
		});
		try {
			await runtime.startTurn(actor);
			if (!indeterminate) await runtime.previewActorCall(actor);
			await candidateReady.promise;
			let prepared: PreparedActorCall<string> | undefined;
			await expect(gateway.executeAuthoritative({ tool: actor.tool, input: actor.input }, executeActor, {
				reuse: async () => { prepared = await runtime.prepareActorCall(actor); return prepared?.output; }, settled: async (settlement) => {
					if (settlement.status === "succeeded") await prepared?.settle(settlement.toolExecution, settlement.output);
				},
			})).resolves.toBe("Actor");
			expect(executeActor).toHaveBeenCalledOnce();
			await runtime.finishTurn({ ...actor, terminal: indeterminate });
			await vi.waitFor(() => expect(events.some((event) => event.type === "actor_action")).toBe(true));
			const settlement = events.find((event) => event.type === "actor_action")?.settlement;
			expect(settlement?.rejections[0]?.cause).toMatchObject(scenario === "classified" ? failure : incompatible
				? { stage: "compatibility", code: indeterminate ? "backend_indeterminate" : "backend_incompatible",
					detail: indeterminate ? "attestation_missing" : "sealed_incompatible" } : { stage: "commit", code: "world_commit_failed" });
			expect(settlement?.provider).toMatchObject({ kind: "actor", origin: "fallback" });
			if (indeterminate) expect(summarizeSpeculativeTrace(events)).toMatchObject({
				actorCandidateRejections: { "compatibility:backend_indeterminate": 1 }, actorActions: 1, actorFallbacks: 1,
			});
			expect(commit).toHaveBeenCalledTimes(incompatible ? 0 : 1);
			expect(dispose).toHaveBeenCalledOnce();
			if (indeterminate) expect(settlements).toEqual([expect.objectContaining({ match: {
				matched: true, relation: { kind: "exact", distance: 0 },
				adoption: { status: "rejected", candidateID: expect.any(String), cause: settlement?.rejections[0]?.cause },
			} })]);
		} finally { await runtime.dispose(); await gateway.dispose(); }
	});

	it("keeps one turn on its settings snapshot while master disable remains immediate", async () => {
		let configured = settings;
		const source = planSource({
			propose: () => plan("epoch"),
		});
		const { runtime, ready: candidateReady } = harness({ source, settings: () => configured });
		await runtime.startTurn(start("turn-1"));
		await candidateReady.promise;

		configured = { ...settings, tools: settings.tools.filter((tool) => tool !== "read") };
		await runtime.settingsChanged(configured);
		expect((await runtime.prepareActorCall(call("turn-1")))?.output).toBe("speculative");
		await runtime.finishTurn({ ...call("turn-1"), terminal: false });

		await runtime.startTurn(start("turn-2"));
		expect((await runtime.prepareActorCall(call("turn-2")))?.output).toBe("speculative");

		configured = { ...settings, enabled: false };
		await runtime.settingsChanged(configured);
		expect(runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0 });
	});

	it.each(["running", "sealed valid", "sealed stale", "sealed unproven", "observation"])("reconciles Actor effects with $0 ownership", async (phase) => {
		let version = 0, executions = 0;
		const started = barrier(), gate = barrier(), commits = vi.fn();
		const settlements: PredictionSettlement[] = [];
		const { runtime, ready } = harness({
			source: { ...planSource({ propose: ({ startInput }) => startInput.turnID === "turn-1"
				? { ...plan("future", { path: "future.ts" }), actions: [readAction("next", { path: "future.ts" }, { horizon: 0, latestHorizon: 1, expectedDurationMs: 10 })] }
				: plan(`empty:${startInput.turnID}`, {}) }),
				onSettled: ({ settlement }) => { settlements.push(settlement); } },
			execute: async () => {
				const captured = version, output = `future:${++executions}`;
				started.arrive(); if (phase === "running" && executions === 1) await gate.promise;
				return world(output, { onCommit: commits, validate: phase === "sealed unproven" ? undefined : async () => captured === version
					? validResource()
					: { status: "stale", cause: cause("freshness", "changed"), metrics: zeroValidationMetrics() } });
			},
		});
		try {
			await runtime.startTurn(start("turn-1"));
			await (phase === "running" ? started.promise : ready.promise);
			const mutation: Call = { ...call("turn-1"), id: "mutation", tool: phase === "observation" ? "read" : "write",
				input: { path: "future.ts", ...(phase === "observation" ? { offset: 2001, limit: 1 } : { content: "new" }) } };
			const mutationCall = await runtime.prepareActorCall(mutation);
			expect(mutationCall?.output).toBeUndefined();
			if (phase === "sealed stale" || phase === "running") version++;
			await mutationCall?.settle(simulatedExecution(1), "Actor");
			gate.arrive(); if (phase === "running") await ready.promise;
			expect(executions).toBe(phase === "running" ? 2 : 1);
			expect(settlements).toHaveLength(0);
			await runtime.finishTurn({ ...call("turn-1"), terminal: false });
			await runtime.startTurn(start("turn-2"));
			const actor = call("turn-2", { path: "future.ts" }), hit = !["sealed stale", "sealed unproven"].includes(phase);
			const prepared = await runtime.prepareActorCall(actor);
			expect(prepared?.output).toBe(hit ? `future:${phase === "running" ? 2 : 1}` : undefined);
			if (!hit) await prepared?.settle(simulatedExecution(1), "Actor");
			expect(commits).toHaveBeenCalledTimes(hit ? 1 : 0);
			await runtime.finishTurn({ ...actor, terminal: true });
			expect(settlements).toHaveLength(1);
			expect(settlements[0]).toMatchObject({ observation: "observed", match: { matched: true, adoption: { status: hit ? "adopted" : "rejected" } } });
		} finally { gate.arrive(); await runtime.finishTurn({ ...call("turn-2"), terminal: true }); }
	});

	it("binds the actual executor independently from pending or completed preview identity", async () => {
		for (const [formalPath, settlePreview] of [
			["preview.ts", false], ["formal.ts", false], ["preview.ts", true], ["preview.ts", "next-event"],
		] as const) {
			const gate = gated();
			const resolveExecution = vi.fn(() => undefined);
			let executor = "preview", actionKeys = 0, captured: ActionKey | undefined;
			const { runtime } = harness({
				source: { id: "disabled", enabled: () => false, propose: () => undefined },
				actionKey: async (tool, input) => {
					const identity = executor;
					actionKeys++;
					if (actionKeys === 1) {
						await gate.wait();
					}
					return PI_ACTION_SEMANTICS.buildKey(tool, input, "/workspace", "", { fingerprint: identity });
				},
				resolveExecution,
				captureAuthoritativeResult: ({ action }) => { captured = action; return undefined; },
			});
			const turnID = `in-flight-key:${formalPath}:${settlePreview}`;
			await runtime.startTurn(start(turnID));
			const previewCall = call(turnID, { path: "preview.ts" });
			const preview = runtime.previewActorCall(previewCall);
			await gate.entered;
			if (settlePreview === true) {
				gate.release();
				await preview;
			}
			executor = "actor";
			const actorCall = { ...previewCall, input: { path: formalPath } };
			const consumed = settlePreview === "next-event"
				? nextTurn().then(() => runtime.prepareActorCall(actorCall)) : runtime.prepareActorCall(actorCall);
			gate.release(); await preview;
			expect((await consumed)?.output).toBeUndefined();
			expect(captured?.executionFingerprint).toBe("actor");
			expect(captured?.input.path).toBe(formalPath);
			expect(actionKeys).toBe(2);
			expect(resolveExecution, String(settlePreview)).toHaveBeenCalledTimes(settlePreview === true ? 1 : 0);
			await (await consumed)?.settle(simulatedExecution(1), "actor");
			await runtime.finishTurn({ ...actorCall, terminal: true });
		}
	});

	it.each(["parallel-predictions", "completed-predictions", "completed-changed", "different-routes", "late-prediction", "preview-first", "two-previews", "cancel-owner", "prediction-first", "future-prediction", "feedback-skip", "feedback-error"] as const)(
		"coalesces candidate admission across producer entrances: %s", async (mode) => {
		const dual = mode === "two-previews" || mode === "cancel-owner", distinct = mode === "different-routes";
		const completed = mode.startsWith("completed-"), validationGate = gated(), lateProposal = gated();
		const sourceCount = dual ? 0 : distinct ? 2 : mode === "parallel-predictions" || completed ? 8 : 1;
		const admitted = barrier(), admissionGate = barrier(), continued = barrier(sourceCount), firstContinued = barrier(sourceCount - 1);
		const proposalGate = gated(sourceCount), keyed = barrier(sourceCount - Number(completed)), executing = barrier(distinct ? 2 : 1), executionGate = barrier();
		const ready = candidateSucceeded(distinct ? 2 : 1), nextReady = candidateSucceeded(2), disposed = vi.fn();
		const settlements: PredictionSettlement[] = [];
		const filtered = mode.startsWith("feedback-");
		let admissions = 0, proposals = 0, routes = 0, validations = 0, changed = false;
		const { runtime, events, executions: executionCount } = harness({
			source: planSource({ enabled: () => !dual, proposalCount: () => sourceCount,
				continueOn: filtered ? () => { continued.arrive(); if (mode === "feedback-error") throw new Error("feedback failure"); return false; } : ["execution_succeeded"],
				propose: async ({ startInput, proposalIndex }) => {
					proposals++; await proposalGate.wait();
					if (completed && proposalIndex === sourceCount - 1) await lateProposal.wait();
					const proposal = plan(`${startInput.turnID}:${proposalIndex}`, { path: "README.md", ...(startInput.turnID === "range" ? { offset: 2 } : {}) });
					return mode === "future-prediction" ? { ...proposal, actions: proposal.actions.map((action) => ({ ...action, horizon: 3, expectedDurationMs: 10 })) } : proposal;
				}, continue: () => { firstContinued.arrive(); continued.arrive(); return undefined; }, onSettled: ({ settlement }) => { settlements.push(settlement); } }),
			preflightCandidate: async ({ candidate: draft }) => {
				if (draft.source === "actor_preview" && (mode === "late-prediction" || dual)) {
					if (++admissions === (dual ? 2 : 1)) admitted.arrive(); await admissionGate.promise;
				}
				return { ok: true };
			},
			resolveExecution: () => distinct ? { ...RESOURCE_ROUTE, backend: `route-${++routes}`, fingerprint: `route-${routes}` } : RESOURCE_ROUTE,
			execute: async (tool, concrete) => {
				executing.arrive(); if (distinct) await executionGate.promise;
				return world(concrete.offset === 2 ? "different query" : "shared observation", {
					executionFingerprint: buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint,
					validate: async () => {
						if (++validations === 1 && completed) await validationGate.wait();
						return changed ? { status: "stale", cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() } : validResource();
					}, onDispose: disposed });
			},
			onCandidateMaterialized: () => keyed.arrive(), onEvent: (event) => { ready.observe(event); nextReady.observe(event); },
		});
		const actor = call("turn"), second = { ...actor, id: "independent-observation" };
		try {
			await runtime.startTurn(actor);
			if (mode === "parallel-predictions" || distinct) {
				if (!distinct) expect(proposals).toBe(0);
				await proposalGate.entered; proposalGate.release();
				if (distinct) { await executing.promise; executionGate.arrive(); }
				await continued.promise;
			} else {
				if (mode === "prediction-first" || filtered) { proposalGate.release(); await continued.promise; }
				if (mode === "future-prediction") { proposalGate.release(); await keyed.promise; expect(runtime.inspect().deferredPlanActions).toBe(1); expect(executionCount()).toBe(0); }
				const previews = [runtime.previewActorCall(actor)];
				if (dual) previews.push(runtime.previewActorCall(second));
				if (mode === "late-prediction" || dual) {
					await admitted.promise;
					if (!dual) { proposalGate.release(); await continued.promise; }
					admissionGate.arrive();
				}
				await Promise.all(previews);
			}
			await ready.promise;
			if (completed) {
				proposalGate.release(); await validationGate.entered; await keyed.promise; await nextTurn();
				expect(validations).toBe(1); validationGate.release(); await firstContinued.promise;
				lateProposal.release(); await continued.promise; expect(validations).toBe(2);
				changed = mode === "completed-changed";
			}
			if (mode === "preview-first") { proposalGate.release(); await continued.promise; }
			expect(executionCount()).toBe(distinct ? 2 : 1);
			if (distinct) { expect(runtime.inspect().sharedCandidates).toBe(2); expect(routes).toBe(2); }
			expect(settlements).toEqual([]); expect(events.some((event) => event.type === "actor_action")).toBe(false);
			if (mode === "cancel-owner") {
				const changed = { ...actor, input: { path: "different.ts" } };
				await runFallback(runtime, changed, 1, "different observation");
			} else if (changed) await runFallback(runtime, actor, 1, "changed observation");
			else expect((await runtime.prepareActorCall(actor))?.output).toBe("shared observation");
			if (completed) expect(validations).toBe(3);
			if (dual || mode === "prediction-first") expect((await runtime.prepareActorCall(second))?.output).toBe("shared observation");
			await runtime.finishTurn({ ...actor, terminal: mode !== "prediction-first" });
			if (filtered) expect(events.filter((event) => event.type === "source_request" && event.request.request.kind === "continuation")).toEqual([]);
			if ((dual && mode !== "cancel-owner") || mode === "prediction-first") {
				const providers = events.filter((event) => event.type === "actor_action").map((event) => event.settlement.provider);
				expect(providers).toHaveLength(2);
				expect(providers[1]!.toolExecution).toBe(providers[0]!.toolExecution);
				if (dual) expect(events.find((event) => event.type === "task")?.timing.authoritativeToolCount).toBe(1);
			}
			if (mode === "parallel-predictions" || completed) {
				expect(settlements).toHaveLength(8);
				expect(new Set(settlements.map((item) => item.observation === "observed" && item.actorAction.id))).toEqual(new Set([actor.id]));
			}
			if (mode === "future-prediction") expect(settlements).toEqual([expect.objectContaining({ match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }) })]);
			if (mode === "prediction-first") {
				const providers = events.filter((event) => event.type === "actor_action").map((event) => event.settlement.provider);
				expect(providers).toHaveLength(2); expect(providers.every((provider) => provider.kind === "speculative")).toBe(true);
				expect(new Set(providers.map((provider) => "candidateID" in provider && provider.candidateID)).size).toBe(1);
				const range = call("range", { path: "README.md", offset: 2 });
				await runtime.startTurn(range); await nextReady.promise;
				expect((await runtime.prepareActorCall(range))?.output).toBe("different query");
				await runtime.finishTurn({ ...range, terminal: true });
				expect(events.find((event) => event.type === "task")).toMatchObject({ timing: { authoritativeToolCount: 2 } });
			}
			expect(executionCount()).toBe(distinct || mode === "prediction-first" ? 2 : 1);
		} finally { proposalGate.release(); admissionGate.arrive(); executionGate.arrive(); validationGate.release(); lateProposal.release(); await runtime.dispose(); }
		expect(disposed).toHaveBeenCalledTimes(executionCount());
	});

	it.each(["binding", "selection"] as const)("does not acquire a retired result after Actor %s waits", async (phase) => {
		const gate = gated(), ready = candidateSucceeded(), refreshed = candidateSucceeded(2), disposed = barrier(), commit = vi.fn();
		let configured = settings, executions = 0, authorizations = 0, allowOld = false;
		const { runtime } = harness({
			source: planSource({
				propose: ({ startInput }) => startInput.turnID.startsWith("producer") ? plan(startInput.turnID) : undefined }),
			settings: () => configured,
			actionKey: async (tool, args, context) => {
				if (phase === "binding" && context.type === "consume") { await gate.wait(); }
				return buildPiActionKey(tool, args, "/workspace");
			},
			authorizeCandidate: async () => {
				if (phase === "selection" && authorizations++ === 0) { await gate.wait(); return { ok: false, reason: "first_rejected" }; }
				return { ok: true };
			},
			execute: () => {
				const generation = ++executions;
				return world(`observation:${generation}`, {
					executionFingerprint: buildPiActionKey("read", { path: "README.md" }, "/workspace")!.executionFingerprint,
					validate: async () => phase === "selection" && generation === 1 && !allowOld
						? { status: "indeterminate", cause: cause("freshness", "unproven"), metrics: zeroValidationMetrics() }
						: validResource(), onCommit: commit, onDispose: disposed.arrive });
			},
			onEvent: (event) => { ready.observe(event); refreshed.observe(event); },
		});
		try {
			let actor = call("producer:1");
			await runtime.startTurn(actor); await ready.promise;
			if (phase === "selection") {
				const other = { ...actor, input: { path: "other.ts" } };
				await runFallback(runtime, other, 1, "other");
				await runtime.finishTurn({ ...other, terminal: false });
				actor = call("producer:2"); await runtime.startTurn(actor); await refreshed.promise; allowOld = true;
			}
			const consumed = runtime.prepareActorCall(actor).then(prepared => prepared?.output); await gate.entered;
			configured = { ...settings, resourceCacheMaxBytes: 1 };
			await runtime.startTurn(call("pressure")); await disposed.promise;
			gate.release(); expect(await consumed).toBeUndefined(); expect(commit).not.toHaveBeenCalled();
		} finally { gate.release(); await runtime.dispose(); }
	});

	it.each(["expiry", "inflight", "independent"] as const)("owns isolated preview execution through %s", async (mode) => {
		let effects = 0, native = 0;
		const gate = gated(), disposed = vi.fn(), independent = mode === "independent";
		const actor: Call = { ...call(mode), tool: independent ? "bash" : "write",
			input: independent ? { command: "increment-counter" } : { path: "preview.txt", content: mode } };
		const second = { ...actor, id: "second-effect" };
		const { runtime, executions: executionCount, ready } = harness({
			source: { id: "disabled", enabled: () => false, propose: () => undefined },
			resolveExecution: ({ tool }) => independent || tool === "write" ? MUTATION_ROUTE : undefined,
			execute: async () => {
				await gate.wait();
				return world("count:1", {
					executionFingerprint: buildPiActionKey(actor.tool, actor.input, "/workspace")!.executionFingerprint,
					checkpoint: { backend: "test", id: "preview", lineage: "preview", depth: 0 }, resources: ["."],
					onCommit: () => effects++, onDispose: disposed });
			},
		});
		try {
			await runtime.startTurn(actor);
			const previews = [runtime.previewActorCall(actor)];
			if (independent) previews.push(runtime.previewActorCall(second));
			await Promise.all(previews); await gate.entered;
			if (!independent) await runtime.previewActorCall({ ...actor, id: "unsupported", tool: "bash", input: { command: "echo preview" } });
			if (mode === "expiry") {
				gate.release(); await ready.promise; await runtime.finishTurn({ ...actor, terminal: false });
				expect(effects).toBe(0); expect(disposed).toHaveBeenCalledOnce();
				expect(runtime.inspect("session").exclusiveCandidates).toBe(0);
			} else {
				const consumed = runtime.prepareActorCall(actor).then(prepared => prepared?.output); expect(executionCount()).toBe(1); gate.release();
				expect(await consumed).toBe("count:1"); expect(effects).toBe(1);
				if (independent) {
					const prepared = await runtime.prepareActorCall(second);
					expect(prepared?.output).toBeUndefined();
					native++; await prepared?.settle(simulatedExecution(1), `count:${++effects}`);
					expect({ effects, native }).toEqual({ effects: 2, native: 1 });
				}
				await runtime.finishTurn({ ...actor, terminal: true });
			}
			expect(executionCount()).toBe(1);
		} finally { gate.release(); await runtime.dispose(); }
		expect(disposed).toHaveBeenCalledTimes(1);
	});

	it.each(["exact", "future", "due"] as const)("selects one captured prediction relation per plan without isolation: %s", async (mode) => {
		const settlements: PredictionSettlement[] = [];
		const projected = mode !== "exact", tool = projected ? "read" : "bash";
		const horizons = mode === "due" ? [4, 0, 2, 2, 1, 3] : projected ? [4, 3, 1, 1, 2, 5] : [0];
		const proposalIDs = projected ? ["second", "first"] : ["bash"];
		const predictedInput = projected ? { path: "README.md", offset: 1, limit: 100 } : { command: "build" };
		const actionCount = horizons.length * proposalIDs.length, routeChecked = barrier(actionCount);
		const project = vi.fn(READ_RANGE_ACTION_KEY_PROJECTOR.project);
		const source = planSource({
			propose: ({ startInput }) => startInput.turnID !== "turn-1" ? undefined : proposalIDs.map((id) => ({
				id, source: "source", revision: 0,
				actions: horizons.map((horizon, index) => ({
					id: String(index), type: "tool_call", tool, input: predictedInput, horizon, latestHorizon: 8,
				})),
			})),
			onSettled: ({ settlement }) => { settlements.push(settlement); },
		});
		const { runtime, events, executions: executionCount } = harness({
			source,
			projection: { ...READ_RANGE_ACTION_KEY_PROJECTOR, project },
			resolveExecution: () => { routeChecked.arrive(); return undefined; },
		});
		let turnID = "turn-1";
		try {
			await runtime.startTurn(start(turnID));
			await routeChecked.promise;
			await nextTurn();
			expect(runtime.inspect()).toMatchObject({
				exclusiveCandidates: 0, sharedCandidates: 0, executionBlockedPlanActions: actionCount,
			});
			for (let previous = 1; mode === "due" && previous <= 2; previous++) {
				const earlier = call(turnID, { path: "unrelated.ts" });
				await runFallback(runtime, earlier);
				await runtime.finishTurn({ ...earlier, terminal: false });
				turnID = `turn-${previous + 1}`;
				await runtime.startTurn(start(turnID));
			}
			const firstCall: Call = {
				sessionID: "session", turnID, id: "first", tool,
				input: projected ? { path: "README.md", offset: 10, limit: 10 } : predictedInput,
			};
			project.mockClear();
			await runtime.previewActorTool(firstCall);
			expect(project).not.toHaveBeenCalled();
			await runtime.previewActorCall(firstCall);
			expect.soft(project).toHaveBeenCalledTimes(projected ? actionCount : 0);
			expect(settlements).toEqual([]);
			project.mockClear();
			const prepared = await runtime.prepareActorCall(firstCall);
			expect(prepared?.output).toBeUndefined();
			expect.soft(project).toHaveBeenCalledTimes(projected ? actionCount : 0);
			await prepared?.settle(simulatedExecution(2), "actor-built");
			await runtime.finishTurn({ ...firstCall, terminal: true });
			const matched = settlements.filter((settlement) => settlement.observation === "observed" && settlement.match.matched);
			expect(matched).toMatchObject(proposalIDs.map((proposalID) => ({
				prediction: { proposalID, actionID: projected ? "2" : "0" }, actorAction: { id: "first" },
				match: {
					matched: true,
					relation: projected ? { kind: "projected", projector: "read.range", distance: 90 } : { kind: "exact", distance: 0 },
					adoption: { status: "rejected", cause: { stage: "execution", code: "isolation_unavailable" } },
				},
			})));
			expect(settlements).toHaveLength(actionCount);
			expect(new Set(settlements.map((settlement) => settlement.prediction.id)).size).toBe(actionCount);
			expect(events.filter((event) => event.type === "actor_action" && event.settlement.actorAction.id === "first"))
				.toMatchObject([{ settlement: { provider: { kind: "actor", origin: "fallback" } } }]);
			expect(executionCount()).toBe(0);
			expect(events.some((event) => event.type === "candidate")).toBe(false);
		} finally { await runtime.dispose(); }
	});

	it.each(["route", "preflight"] as const)("matches a continuation during pending %s without authorizing execution", async (phase) => {
		const preparation = gated(), settlements: PredictionSettlement[] = [];
		const child = { ...call("child"), tool: "write", input: { path: "child.ts", content: "next" } };
		const hold = async () => { await preparation.wait(); };
		const { runtime, executions: executionCount } = harness({
			source: planSource({
				propose: ({ startInput }) => startInput.turnID === "parent" ? plan("root") : undefined,
				continueOn: ["execution_succeeded"],
				continue: ({ proposalID, actionID, revision }) => ({ proposalID, source: "source", revision,
					upsert: [{ id: "child", type: "tool_call", tool: child.tool, input: child.input,
						dependsOn: [{ actionID, condition: "execution_succeeded" }] }] }),
				onSettled: ({ settlement }) => { settlements.push(settlement); },
			}),
			resolveExecution: async ({ tool }) => {
				if (tool === "read") return RESOURCE_ROUTE;
				if (phase === "route") await hold();
				return MUTATION_ROUTE;
			},
			preflightCandidate: async ({ candidate }) => {
				if (phase === "preflight" && candidate.tool === child.tool) await hold();
				return { ok: true };
			},
		});
		try {
			await runtime.startTurn(start("parent")); await preparation.entered;
			expect((await runtime.prepareActorCall(call("parent")))?.output).toBe("speculative");
			await runtime.finishTurn(call("parent"));
			await runtime.startTurn(child);
			const prepared = await runtime.prepareActorCall(child);
			expect(prepared?.output).toBeUndefined();
			expect(executionCount()).toBe(1);
			await prepared?.settle(simulatedExecution(1), "actor");
			preparation.release();
			await runtime.finishTurn({ ...child, terminal: true });
			expect(settlements.filter(({ prediction }) => prediction.actionID === "child")).toMatchObject([{
				observation: "observed", match: { matched: true, relation: { kind: "exact" },
					adoption: { status: "rejected", cause: { stage: "admission", code: "preparation_pending" } } },
			}]);
		} finally { preparation.release(); await runtime.dispose(); }
		expect(executionCount()).toBe(1);
		expect(runtime.inspect()).toMatchObject({ pendingPredictions: 0, deferredPlanActions: 0, sharedCandidates: 0, exclusiveCandidates: 0 });
	});

	it.each(["binding", "preflight"] as const)("retires a peer's pending %s when its parent disappears", async (phase) => {
		const preparation = gated(), retired = barrier();
		const materialized: string[] = [], preflighted: string[] = [];
		let preparationSignal: AbortSignal | undefined;
		const { runtime, executions: executionCount } = harness({
			source: planSource({
				propose: () => plan("root", { path: "parent.ts" }), continuationBatch: () => ["next"],
				observe: () => ({ proposalID: "root", source: "source", revision: 1, remove: ["next"] }),
			}),
			peers: [{ id: "peer", enabled: () => true, propose: () => undefined,
				continueFrom: ({ batch }) => ({ id: "child", source: "peer", revision: 0,
					actions: [readAction("next", { path: "child.ts" }, { dependsOn: batch.map(({ identity }) => ({
						proposalID: identity.proposalID, actionID: identity.actionID, identity: identity.id,
						condition: "execution_succeeded",
					})) })] }),
				onSettled: ({ settlement }) => {
					expect(settlement).toMatchObject({ observation: "unobserved", cause: { code: "dependency_impossible" } });
					retired.arrive();
				},
			}],
			actionKey: async (tool, args, context) => {
				if (phase === "binding" && context.type === "start" && (args as { path: string }).path === "child.ts") {
					await preparation.wait();
				}
				return buildPiActionKey(tool, args, "/workspace");
			},
			preflightCandidate: async ({ signal, candidate }) => {
				const candidatePath = (candidate.input as { path: string }).path;
				preflighted.push(candidatePath);
				if (phase === "preflight" && candidatePath === "child.ts") {
					preparationSignal = signal; await preparation.wait();
				}
				return { ok: true };
			},
			onCandidateMaterialized: (candidate) => { materialized.push(String(candidate.input.path)); },
		});
		try {
			await runtime.startTurn(start("parent")); await preparation.entered;
			expect((await runtime.prepareActorCall(call("parent", { path: "parent.ts" })))?.output).toBe("speculative");
			await retired.promise;
			if (phase === "preflight") expect(preparationSignal?.aborted).toBe(true);
			preparation.release(); await nextTurn();
			expect(materialized).toEqual(phase === "binding" ? ["parent.ts"] : ["parent.ts", "child.ts"]);
			expect(preflighted).toEqual(materialized);
		} finally { preparation.release(); await runtime.dispose(); }
		expect(executionCount()).toBe(1);
		expect(runtime.inspect()).toMatchObject({ pendingPredictions: 0, sharedCandidates: 0, exclusiveCandidates: 0 });
	});

	it.each(["complete", "arrived", "closed", "failed", "disabled"] as const)("shares only a complete root batch with a peer: %s", async (mode) => {
		const first = barrier(), secondReady = candidateSucceeded(1, "second.ts"), parentsReady = barrier(2);
		const peerGate = gated(), childReady = candidateSucceeded(1, "child.ts");
		const settlements: PredictionSettlement[] = [], materialized: string[] = [];
		let peerSignal: AbortSignal | undefined;
		const continueFrom = vi.fn<NonNullable<Source["continueFrom"]>>(async ({ batch, signal }) => {
			peerSignal = signal;
			expect(batch.map(({ candidate, output }) => [candidate.input.path, output])).toEqual([
				["first.ts", "first.ts:output"], ["second.ts", "second.ts:output"],
			]);
			await peerGate.wait();
			return { id: "peer", source: "peer", revision: 0, actions: [readAction("child", { path: "child.ts" }, {
				dependsOn: batch.map(({ identity }) => ({ actionID: identity.actionID, proposalID: identity.proposalID,
					identity: identity.id, condition: "execution_succeeded" })),
			})] };
		});
		const { runtime, executions: executionCount } = harness({
			source: planSource({ multiStepEnabled: () => false, requestLifetime: "actor_decision",
				propose: ({ startInput }) => startInput.turnID === "parent" ? { id: "roots", source: "source", revision: 0,
					actions: [readAction("first", { path: "first.ts" }), readAction("second", { path: "second.ts" })] } : undefined,
				continuationBatch: () => ["first", "second"], onSettled: ({ settlement }) => { settlements.push(settlement); },
			}),
			peers: [{ id: "peer", enabled: () => mode !== "disabled", propose: () => undefined, continueFrom,
				onSettled: ({ settlement }) => { settlements.push(settlement); } }],
			execute: async (_tool, input) => {
				if (input.path === "first.ts") { await first.promise; if (mode === "failed") throw new Error("parent failed"); }
				return `${input.path}:output`;
			},
			onCandidateMaterialized: (candidate) => { materialized.push(String(candidate.input.path)); },
			onEvent: (event) => {
				secondReady.observe(event); childReady.observe(event);
				if (event.type === "candidate" && event.state.status !== "running" &&
					(event.candidate.predictedAction.includes("first.ts") || event.candidate.predictedAction.includes("second.ts"))) parentsReady.arrive();
			},
		});
		let closing: Promise<void> | undefined;
		try {
			await runtime.startTurn(start("parent")); await secondReady.promise;
			expect(continueFrom).not.toHaveBeenCalled();
			first.arrive(); await parentsReady.promise;
			if (mode === "failed" || mode === "disabled") { expect(continueFrom).not.toHaveBeenCalled(); return; }
			await peerGate.entered;
			if (mode === "arrived") expect((await runtime.prepareActorCall(call("parent", { path: "first.ts" })))?.output).toBe("first.ts:output");
			if (mode === "closed") {
				let drained = false;
				closing = runtime.dispose().then(() => { drained = true; });
				await nextTurn(); expect(drained).toBe(false);
			}
			expect(peerSignal?.aborted).toBe(mode !== "complete");
			peerGate.release();
			if (mode === "complete") {
				await childReady.promise;
				for (const name of ["second", "first"]) expect((await runtime.prepareActorCall({
					...call("parent", { path: `${name}.ts` }), id: name }))?.output).toBe(`${name}.ts:output`);
				await runtime.finishTurn({ ...call("parent"), terminal: false });
				await runtime.startTurn(start("child"));
				expect((await runtime.prepareActorCall(call("child", { path: "child.ts" })))?.output).toBe("child.ts:output");
				await runtime.finishTurn({ ...call("child"), terminal: true });
				expect(settlements.map((settlement) => settlement.prediction.source)).toEqual(["source", "source", "peer"]);
				expect(settlements.every((settlement) => settlement.observation === "observed" && settlement.match.matched &&
					settlement.match.adoption.status === "adopted")).toBe(true);
			}
		} finally { first.arrive(); peerGate.release(); await closing; await runtime.dispose(); }
		expect(continueFrom).toHaveBeenCalledTimes(1);
		expect(materialized).toEqual(["first.ts", "second.ts", ...(mode === "complete" ? ["child.ts"] : [])]);
		expect(executionCount()).toBe(mode === "complete" ? 3 : 2);
		expect(runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0, sharedCandidates: 0, exclusiveCandidates: 0 });
	});

	it("keeps a next-decision continuation alive across parallel tools in one Actor decision", async () => {
		const parentReady = barrier();
		const gate = gated();
		const childReady = candidateSucceeded(1, "child.ts");
		const settlements: PredictionSettlement[] = [];
		const source = planSource({
			requestLifetime: "actor_decision",
			continueOn: ["actor_adopted"],
			propose: () => plan("parallel-continuation", { path: "parent.ts" }),
			continue: async ({ proposalID, revision, trigger }) => {
				if (trigger !== "actor_adopted") return undefined;
				await gate.wait();
				return { proposalID, source: "source", revision, upsert: [ readAction("child", { path: "child.ts" })] };
			},
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		});
		const { runtime } = harness({
			source,
			execute: (_tool, input) => {
				const path = String(input.path);
				if (path === "parent.ts") parentReady.arrive();
				return `${String(input.path)}:output`;
			},
			onEvent: childReady.observe,
		});
		await runtime.startTurn(start("parallel-continuation"));
		await parentReady.promise;

		const parent = { sessionID: "session", turnID: "parallel-continuation", id: "parent-call", tool: "read", input: { path: "parent.ts" } };
		expect((await runtime.prepareActorCall(parent))?.output).toBe("parent.ts:output");
		await gate.entered;

		const sibling = { ...parent, id: "sibling-call", input: { path: "sibling.ts" } };
		await runFallback(runtime, sibling, 1_000);

		gate.release();
		await childReady.promise;
		const sameBatchChild = { ...parent, id: "same-batch-child", input: { path: "child.ts" } };
		expect((await runtime.prepareActorCall(sameBatchChild))?.output).toBe("child.ts:output");
		await runtime.finishTurn({ ...parent, terminal: false });

		await runtime.startTurn(start("next-decision"));
		expect(
			(await runtime.prepareActorCall({ ...sameBatchChild, turnID: "next-decision", id: "next-decision-child" }))?.output,
		).toBe("child.ts:output");
		await runtime.finishTurn({ ...sameBatchChild, turnID: "next-decision", terminal: true });
		expect(
			settlements.map((settlement) =>
				settlement.observation === "observed" ? settlement.actorAction.decisionSequence : undefined,
			),
		).toEqual([1, 2]);
	});

	it.each(["retained", "retry", "expired", "replaced", "terminal"] as const)("keeps queued continuation authority %s across plan and turn boundaries", async (phase) => {
		const gate = gated();
		const retained = phase === "retained" || phase === "retry", nextChild = phase === "retry" ? "late-child" : "child";
		const childReady = candidateSucceeded(1, `${nextChild}.ts`);
		const replacementReady = candidateSucceeded(1, "replacement.ts");
		let proposals = 0;
		const continuations: string[] = [];
		const executed: string[] = [];
		const source = planSource({
			proposalCount: () => 1,
			propose: () => {
				proposals++;
				return plan("cross-turn", { path: "parent.ts" });
			},
			continue: async ({ proposalID, actionID, revision, candidate, trigger }) => {
				if (String(candidate.input.path) !== "parent.ts") return undefined;
				continuations.push(trigger);
				if (trigger === "execution_succeeded") {
					await gate.wait();
					if (phase === "retry") return undefined;
				}
				const child = trigger === "execution_succeeded" ? "child" : "late-child";
				return childPlanUpdate({ proposalID, actionID, revision }, child, `${child}.ts`);
			},
			observe: ({ concrete }) => phase === "replaced" && concrete.path === "replace.ts" ? {
				proposalID: "cross-turn", source: "source", revision: 2,
				upsert: [readAction("next", { path: "replacement.ts" })],
			} : undefined,
		});
		const { runtime } = harness({
			source,
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
			onEvent: (event) => { childReady.observe(event); replacementReady.observe(event); },
		});
		let closing: Promise<void> | undefined;
		try {
			await runtime.startTurn(start("parent-turn"));
			await gate.entered;
			expect((await runtime.prepareActorCall(call("parent-turn", { path: "parent.ts" })))?.output).toBe("parent.ts:output");
			if (phase === "terminal") closing = runtime.finishTurn({ ...call("parent-turn"), terminal: true });
			else if (phase === "replaced") {
				const replacement = { ...call("parent-turn", { path: "replace.ts" }), id: "replace-parent" };
				await runFallback(runtime, replacement);
				await replacementReady.promise;
				gate.release();
			} else {
				await runtime.finishTurn({ ...call("parent-turn"), terminal: false });
				expect(runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 1 });
				await runtime.startTurn(start("child-turn"));
				expect(proposals).toBe(1);
				if (retained) { gate.release(); await childReady.promise; }
				else {
					const unrelated = call("child-turn", { path: "other.ts" });
					await runFallback(runtime, unrelated);
				}
			}
			await nextTurn();
			expect(continuations).toEqual(["execution_succeeded", ...(phase === "retry" ? ["actor_adopted"] : [])]);
			expect(executed).toEqual(["parent.ts", ...(retained ? [`${nextChild}.ts`] : phase === "replaced" ? ["replacement.ts"] : [])]);
			if (retained) {
				expect((await runtime.prepareActorCall(call("child-turn", { path: `${nextChild}.ts` })))?.output).toBe(`${nextChild}.ts:output`);
				await runtime.finishTurn({ ...call("child-turn"), terminal: true });
			}
		} finally { gate.release(); await closing; await runtime.dispose(); }
	});

	it("adopts a target-state-valid child after its parent prediction misses", async () => {
		let enabled = true;
		let dependencyChange: boolean | undefined;
		const childPrepared = barrier();
		const executed: string[] = [];
		const childReady = candidateSucceeded(1, "late.ts");
		const source = planSource({
			enabled: () => enabled,
			propose: () => plan("conditional", { path: "parent.ts" }),
			continue: async ({ proposalID, actionID, revision, trigger }) => {
				if (trigger !== "execution_succeeded") return undefined;
				return childPlanUpdate({ proposalID, actionID, revision }, "late-child", "late.ts");
			},
		});
		const { runtime, events } = harness({
			source,
			preflightCandidate: ({ candidate }) => {
				if (candidate.dependsOn?.length) {
					dependencyChange = Reflect.set(candidate.dependsOn[0]!, "condition", "actor_adopted");
					childPrepared.arrive();
				}
				return { ok: true };
			},
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
			onEvent: childReady.observe,
		});

		try {
			await runtime.startTurn(start("miss"));
			await childPrepared.promise;
			expect(dependencyChange).toBe(false);
			await childReady.promise;
			const prepared = await runtime.prepareActorCall(call("miss", { path: "other.ts" }));
			expect(prepared?.output).toBeUndefined();
			await prepared?.settle(simulatedExecution(1), "actor");
			await runtime.finishTurn({ ...call("miss"), terminal: false });

			enabled = false;
			await runtime.startTurn(start("target"));
			expect((await runtime.prepareActorCall(call("target", { path: "late.ts" })))?.output).toBe("late.ts:output");
			await runtime.finishTurn({ ...call("target"), terminal: true });
			expect(executed).toEqual(["parent.ts", "late.ts"]);
			expect(
				events
					.filter((event) => event.type === "prediction")
					.map((event) => (event.settlement.observation === "observed" ? event.settlement.match.matched : undefined)),
			).toEqual([false, true]);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ predictionsSettled: 2, predictionsObserved: 2,
				predictionsMatched: 1, predictionsAdopted: 1, predictionPrecision: 1 / 2, adoptionYield: 1,
				actorActions: 2, speculativeHits: 1, actorFallbacks: 1, hitRate: 1 / 2 });
		} finally {
			await runtime.finishTurn({ ...call("target"), terminal: true });
		}
	});

	it.each(["baseline", "peer", "replaced", "adopted", "claimed", "cancelled"] as const)("keeps child reuse on its current parent lineage: %s", async (mode) => {
		const claimed = mode === "claimed" || mode === "cancelled", actorController = new AbortController();
		const replacementChild = barrier();
		let enabled = true, workspaceVersion = 0, holdReuse = false;
		const executed: string[] = [];
		const childParents: string[] = [];
		const aliasOutputs: string[] = [];
		const childrenReady = candidateSucceeded(2, '"content":"child"');
		const parentReady = candidateSucceeded(1, "parent-new"), validationStarted = barrier(), validationGate = barrier();
		const parentGate = gated(), aliasReady = barrier(), cleanup = vi.fn(), transactions = new EffectTransactionCoordinator<string>();
		const parentAction = (content: string) => ({ id: "parent", type: "tool_call" as const, tool: "write", input: { path: `${content}.txt`, content } });
		const childAction = { id: "child", type: "tool_call" as const, tool: "write", input: { path: "child.txt", content: "child" },
			expectedDurationMs: 1_000, dependsOn: [{ actionID: "parent", condition: "execution_succeeded" as const }] };
		const source = planSource({
			enabled: () => enabled,
			proposalCount: () => 2,
			continueOn: ["execution_succeeded"],
			continuationBatch: () => ["parent"],
			propose: ({ proposalIndex, startInput }) => startInput.turnID === "parent" ? ({
				id: `chain:${proposalIndex}`,
				source: "source",
				revision: 0,
				actions: [parentAction(`parent-${proposalIndex}`)],
			}) : undefined,
			observe: ({ concrete }) => concrete.path === "alias.ts"
				? { proposalID: "chain:0", source: "source", revision: 2, upsert: [{ ...childAction, id: "alias" }] }
				: concrete.path === "replace.ts" ? { proposalID: "chain:0", source: "source", revision: 3, upsert: [parentAction("parent-new")] } : undefined,
			continue: ({ proposalID, actionID, revision, candidate, output }) => {
				if (mode === "peer") return undefined;
				if (actionID === "alias") { aliasOutputs.push(output); aliasReady.arrive(); }
				if (String(candidate.input.content).startsWith("child")) return undefined;
				return { proposalID, source: "source", revision, upsert: [childAction] };
			},
		});
		const { runtime, events } = harness({
			source,
			peers: [{ id: "peer", enabled: () => mode === "peer", proposalCount: () => 2, propose: () => undefined,
				continueFrom: ({ batch }) => {
					expect(batch).toHaveLength(1);
					const { identity } = batch[0]!;
					return { id: `peer:${identity.id}`, source: "peer", revision: 0,
						actions: [{ ...childAction, dependsOn: [{ proposalID: identity.proposalID, actionID: identity.actionID,
							identity: identity.id, condition: "execution_succeeded" }] }] };
				},
			}],
			actionKey: async (tool, input, context) => {
				if (context.type === "start" && (input as { content?: string }).content === "parent-new") { await parentGate.wait(); }
				return buildPiActionKey(tool, input, "/workspace");
			},
			execute: (tool, input, _signal, parentWorld) => {
				const content = String(input.content);
				executed.push(content);
				if (content === "child") childParents.push(String(parentWorld?.output));
				if (content === "child" && parentWorld?.output === "parent-new") replacementChild.arrive();
				const output = content === "child" ? `child:${parentWorld?.output}` : content;
				const parentCheckpoint = parentWorld?.checkpoint;
				return transactions.execute(transactions.begin({ tool, route: MUTATION_ROUTE }), async () => world(output, {
					checkpoint: {
						backend: "test",
						id: output,
						lineage: parentCheckpoint?.lineage ?? output,
						depth: (parentCheckpoint?.depth ?? -1) + 1,
					},
					resources: ["."],
					onCommit: () => workspaceVersion++,
					onDispose: () => cleanup(output),
					validate: async () => {
						if (holdReuse && output === "child:parent-0") { validationStarted.arrive(); await validationGate.promise; }
						return validResource();
					},
				}));
			},
			onEvent: (event) => { childrenReady.observe(event); parentReady.observe(event); },
		});
		const expectedParent = mode === "replaced" ? "parent-new" : "parent-0";
		const parentCall: Call = { sessionID: "session", turnID: "parent", id: "actor-parent", tool: "write", input: parentAction(expectedParent).input };
		try {
			await runtime.startTurn(start("parent")); await childrenReady.promise;
			expect(executed.sort()).toEqual(["child", "child", "parent-0", "parent-1"]);
			expect(childParents.sort()).toEqual(["parent-0", "parent-1"]);
			if (mode !== "baseline" && mode !== "peer") {
				holdReuse = !claimed;
				const alias = call("parent", { path: "alias.ts" });
				await runFallback(runtime, alias);
				await (claimed ? aliasReady : validationStarted).promise;
				if (mode === "replaced") {
					const replacement = call("parent", { path: "replace.ts" });
					await runFallback(runtime, replacement); await parentGate.entered;
				} else if (mode === "adopted") expect((await runtime.prepareActorCall(parentCall))?.output).toBe(expectedParent);
				holdReuse = false; if (!claimed) validationGate.arrive(); await nextTurn();
				expect(aliasOutputs).toEqual(mode === "replaced" ? [] : ["child:parent-0"]);
				if (mode === "replaced") {
					parentGate.release(); await parentReady.promise; await replacementChild.promise;
					expect(childParents).toEqual(["parent-0", "parent-1", "parent-new"]);
				} else expect(childParents.filter((parent) => parent === "parent-0")).toEqual(["parent-0"]);
			}
			if (mode !== "adopted") expect((await runtime.prepareActorCall(parentCall))?.output).toBe(expectedParent);
			enabled = claimed; await runtime.finishTurn({ ...parentCall, terminal: false });
			await runtime.startTurn(start("child"));
			const childCall: Call = { ...parentCall, turnID: "child", id: "actor-child", input: childAction.input };
			holdReuse = claimed;
			const childConsumption = runtime.prepareActorCall(childCall, actorController.signal).then(prepared => prepared?.output);
			if (claimed) {
				await validationStarted.promise;
				const replacement = call("child", { path: "replace.ts" });
				await runFallback(runtime, replacement); await parentGate.entered;
				if (mode === "cancelled") actorController.abort();
				holdReuse = false; validationGate.arrive();
			}
			expect(await childConsumption).toBe(mode === "cancelled" ? undefined : `child:${expectedParent}`);
			expect(workspaceVersion).toBe(mode === "cancelled" ? 1 : 2);
			await nextTurn();
			expect(cleanup.mock.calls.filter(([output]) => output === `child:${expectedParent}`)).toHaveLength(1);
			parentGate.release();
			await runtime.finishTurn({ ...childCall, terminal: true });
			const predictions = events.filter((event) => event.type === "prediction").map((event) => event.settlement);
			expect(new Set(predictions.map((settlement) => settlement.prediction.id)).size).toBe(predictions.length);
			if (claimed) {
				const matched = predictions.filter((settlement) => settlement.observation === "observed" &&
					settlement.actorAction.id === childCall.id && settlement.match.matched);
				expect(matched).toHaveLength(1);
				expect(matched[0]).toMatchObject({ match: { adoption: mode === "cancelled"
					? { status: "rejected", cause: { code: "actor_aborted" } } : { status: "adopted" } } });
			}
			expect(events.filter((event) => event.type === "actor_action" && event.settlement.actorAction.id === childCall.id))
				.toHaveLength(mode === "cancelled" ? 0 : 1);
		} finally { validationGate.arrive(); parentGate.release(); await runtime.dispose(); }
		expect(cleanup).toHaveBeenCalledTimes(executed.length);
	});
});

function isWorldBranch(value: unknown): value is WorldBranch<string> {
	return Boolean(
		value && typeof value === "object" && typeof (value as Partial<WorldBranch<string>>).commit === "function",
	);
}

function candidateSucceeded<SessionID = string>(expected = 1, actionFragment?: string) {
	const reached = barrier(expected);
	return {
		promise: reached.promise,
		observe: (event: SpeculativeActionEvent<SessionID>) => {
			if (
				event.type === "candidate" &&
				event.state.status === "succeeded" &&
				(!actionFragment || event.candidate.predictedAction.includes(actionFragment))
			) reached.arrive();
		},
	};
}
