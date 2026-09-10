import { describe, expect, it, vi } from "vitest";
import { type ActionProjectionRule, READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { buildPiActionKey, PI_ACTION_SEMANTICS, RESOURCE_INPUT_ACTION_KEY_PROJECTOR, type ActionKey } from "../src/action-semantics.ts";
import { EffectTransactionCoordinator, effectCommitFailure } from "../src/effect-transaction.ts";
import {
	type SpeculativeExecutionRoute,
	type WorldBranch,
	type WorldCheckpoint,
	type WorldExecutionMetrics,
} from "../src/execution-world.ts";
import type {
	AuthoritativeResultCapture,
	CandidatePreflight,
	MaterializedSpeculativeCandidate,
	SpeculativeActionEvent,
	SpeculativeActionSettings,
	SpeculativeDraftCandidate,
	SpeculativePlanSource,
} from "../src/runtime.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";
import { SpeculationScheduler } from "../src/scheduler.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";
import { cause, type PredictionSettlement, type ResourceValidation, zeroValidationMetrics } from "../src/settlement.ts";

interface Start {
	readonly sessionID: string;
	readonly turnID: string;
}

interface Call extends Start {
	readonly id: string;
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
	fingerprint: "resource-version:v1",
};

const MUTATION_ROUTE: SpeculativeExecutionRoute = {
	isolation: "workspace_branch",
	reuse: "exclusive_branch",
	scope: "fallback",
	backend: "test_world",
	fingerprint: "test-world:v1",
};

type Source = SpeculativePlanSource<string, string, Start, Call, { readonly cwd: string }>;

function plan(source: string, proposalID: string, input: Record<string, unknown>) {
	return {
		id: proposalID,
		source,
		revision: 0,
		actions: [{ id: "next", type: "tool_call" as const, tool: "read", input, feedback: proposalID }],
	};
}

function futureReadSource(
	options: {
		readonly latestHorizon?: number;
		readonly expectedDurationMs?: number;
		readonly subsequent?: "empty" | "placeholder";
	} = {},
): Source {
	const { subsequent = "empty", ...action } = options;
	return {
		id: "source",
		enabled: () => true,
		propose: ({ startInput }) =>
			startInput.turnID === "turn-1"
				? {
						...plan("source", "future", { path: "future.ts" }),
						actions: [
							{ id: "next", type: "tool_call", tool: "read", input: { path: "future.ts" }, horizon: 0, ...action },
						],
					}
				: subsequent === "placeholder"
					? plan("source", `empty:${startInput.turnID}`, {})
					: { id: `empty:${startInput.turnID}`, source: "source", revision: 0, actions: [] },
	};
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
			{
				id,
				type: "tool_call" as const,
				tool: "read",
				input: { path },
				dependsOn: [{ actionID: context.actionID, condition: "execution_succeeded" as const }],
			},
		],
	};
}

function harness(input: {
	readonly source: Source;
	readonly settings?: () => SpeculativeActionSettings;
	readonly execute?: (
		tool: string,
		input: Readonly<Record<string, unknown>>,
		signal: AbortSignal,
		parentWorld?: WorldBranch<string>,
	) => unknown | Promise<unknown>;
	readonly expired?: () => boolean | Promise<boolean>;
	readonly capture?: () => unknown | Promise<unknown>;
	readonly validate?: (version: unknown) => ResourceValidation;
	readonly preflight?: (signal: AbortSignal, candidate: SpeculativeDraftCandidate) => CandidatePreflight | Promise<CandidatePreflight>;
	readonly authorize?: () => CandidatePreflight;
	readonly projection?: ActionProjectionRule<string>;
	readonly onCandidateMaterialized?: (candidate: MaterializedSpeculativeCandidate<string>) => void | Promise<void>;
	readonly onTurnFinished?: (input: { readonly terminal: boolean; readonly durationMs: number }) => void | Promise<void>;
	readonly onEvent?: (event: SpeculativeActionEvent<string>) => void | Promise<void>;
	readonly actionKey?: (
		tool: string,
		args: unknown,
		context: { readonly type: "start" | "consume" },
	) => ReturnType<typeof buildPiActionKey> | Promise<ReturnType<typeof buildPiActionKey>>;
	readonly resolveExecution?: (tool: string) => SpeculativeExecutionRoute | undefined;
	readonly captureAuthoritativeResult?: (
		action: NonNullable<ReturnType<typeof buildPiActionKey>>,
		signal: AbortSignal,
	) => AuthoritativeResultCapture<string> | undefined | Promise<AuthoritativeResultCapture<string> | undefined>;
	readonly rejectCandidateOutput?: (output: string) => string | undefined;
}) {
	const events: SpeculativeActionEvent<string>[] = [];
	let executions = 0;
	const runtime = makeStructuralSpeculativeActionRuntime<string, string, Start, Call, Call, { readonly cwd: string }>({
		sources: [input.source],
		settings: input.settings ?? (() => settings),
		definitions: () => [{ name: "read" }, { name: "bash" }, { name: "write" }],
		stateData: () => ({ cwd: "/workspace" }),
		actionKey: input.actionKey ?? ((tool, args) => buildPiActionKey(tool, args, "/workspace")),
		resolveExecution: ({ tool }) =>
			input.resolveExecution
				? input.resolveExecution(tool)
				: tool === "read"
					? RESOURCE_ROUTE
					: tool === "write"
						? MUTATION_ROUTE
						: undefined,
		...(input.captureAuthoritativeResult
			? {
					captureAuthoritativeResult: ({ action, signal }) =>
						input.captureAuthoritativeResult!(action, signal),
				}
			: {}),
		...(input.rejectCandidateOutput
			? { rejectCandidateOutput: ({ output }) => input.rejectCandidateOutput!(output) }
			: {}),
		actual: (call) => ({ id: call.id, tool: call.tool, input: call.input }),
		preflightCandidate: ({ signal, candidate }) => input.preflight?.(signal, candidate) ?? { ok: true },
		authorizeCandidate: input.authorize,
		executeCandidate: async ({ tool, concrete, action, route, signal, parentWorld }) => {
			executions++;
			const version =
				route.isolation === "resource_snapshot" ? await (input.capture?.() ?? { version: 1 }) : undefined;
			const executed = await input.execute?.(tool, concrete, signal, parentWorld);
			if (isWorldBranch(executed)) return executed;
			return world((executed as string | undefined) ?? "speculative", {
				executionFingerprint: action.executionFingerprint,
				...(route.isolation === "resource_snapshot"
					? {
							validate: async () =>
								input.validate
									? input.validate(version)
									: (await input.expired?.())
										? {
												status: "stale" as const,
												cause: cause("freshness", "resource_changed"),
												metrics: zeroValidationMetrics(),
											}
										: { status: "valid" as const, metrics: zeroValidationMetrics() },
						}
					: {}),
			});
		},
		projectionRules: [RESOURCE_INPUT_ACTION_KEY_PROJECTOR, ...(input.projection ? [input.projection] : [])],
		onCandidateMaterialized: input.onCandidateMaterialized,
		onTurnFinished: input.onTurnFinished,
		onEvent: async (event) => {
			events.push(event);
			await input.onEvent?.(event);
		},
	});
	return { runtime, events, executions: () => executions };
}

function call(turnID: string, input: Record<string, unknown> = { path: "README.md" }): Call {
	return { sessionID: "session", turnID, id: `call:${turnID}`, tool: "read", input };
}

describe("structural speculative runtime", () => {
	it.each(["requests", "single", "batch", "revisions", "observed"] as const)("admits independent actions and proposals without head-of-line blocking: %s", async (mode) => {
		const slow = barrier(), slowStarted = barrier(), executed: string[] = [];
		const replacementReady = candidateSucceeded(1, "replacement.ts");
		const keyed: string[] = [];
		const replacements: MaterializedSpeculativeCandidate<string>[] = [];
		const proposals = [
			{ id: "proposal:0", source: "source", revision: 0, actions: [
				{ id: "slow", type: "tool_call" as const, tool: "read", input: { path: "slow.ts" } },
				{ id: "same-plan", type: "tool_call" as const, tool: "read", input: { path: "same-plan.ts" } },
			] },
			plan("source", "proposal:1", { path: "other-plan.ts" }),
		];
		const revisions = [proposals[0]!, { ...plan("source", "proposal:0", { path: "replacement.ts" }), revision: 1 }, proposals[1]!];
		const observed = [proposals[0]!, { proposalID: "proposal:0", source: "source", revision: 1, remove: ["slow"],
			upsert: [{ id: "same-plan", type: "tool_call" as const, tool: "read", input: { path: "replacement.ts" } }] }, proposals[1]!];
		const revised = mode === "revisions" || mode === "observed";
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => mode === "requests" ? 2 : 1,
			propose: ({ proposalIndex }) => mode === "observed" ? undefined : mode === "revisions" ? revisions : mode === "batch" ? proposals : proposals[proposalIndex],
			observe: ({ concrete }) => mode === "observed" && concrete.path === "seed.ts" ? observed : undefined,
		};
		const fixture = harness({
			source,
			actionKey: async (tool, args, context) => {
				if (context.type === "start") {
					keyed.push(String((args as { path?: unknown }).path));
					if (keyed.at(-1) === "slow.ts") { slowStarted.arrive(); await slow.promise; }
				}
				return buildPiActionKey(tool, args, "/workspace");
			},
			execute: (_tool, concrete) => { executed.push(String(concrete.path)); return "speculative"; },
			onCandidateMaterialized: (candidate) => { if (String(candidate.input.path).includes("replacement.ts")) replacements.push(candidate); },
			onEvent: replacementReady.observe,
		});
		let turnID = "parallel-admission";
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			if (mode === "observed") {
				const seed = call(turnID, { path: "seed.ts" });
				expect(await fixture.runtime.consume(seed)).toBeUndefined();
				await fixture.runtime.actual({ ...seed, durationMs: 1, output: "Actor" });
			}
			await slowStarted.promise; await new Promise<void>((resolve) => setImmediate(resolve));
			expect(executed.sort()).toEqual([...(mode === "single" ? [] : ["other-plan.ts"]), "same-plan.ts"]);
			expect(keyed).not.toContain("replacement.ts");
			if (revised) {
				const revision = mode === "revisions" ? revisions[1]! : observed[1]!;
				Object.assign(revision, { [mode === "revisions" ? "id" : "proposalID"]: "proposal:1", revision: 2 });
				const replacement = "actions" in revision ? revision.actions![0]! : revision.upsert![0]!;
				replacement.id = "drifted";
				replacement.input.path = "drifted-replacement.ts";
				slow.arrive(); await replacementReady.promise;
				expect(keyed).toContain("replacement.ts");
				expect(keyed).not.toContain("drifted-replacement.ts");
			}
			if (mode === "observed") {
				slow.arrive(); await fixture.runtime.finishTurn({ ...call(turnID), terminal: false });
				turnID = "next-decision"; await fixture.runtime.startTurn({ sessionID: "session", turnID });
			}
			if (revised) expect(replacements).toMatchObject([{
				source: "source", proposalID: "proposal:0", actionID: mode === "revisions" ? "next" : "same-plan",
				input: { path: "replacement.ts" },
			}]);
			expect(await fixture.runtime.consume(call(turnID, { path: revised ? "replacement.ts" : "same-plan.ts" }))).toBe("speculative");
		} finally {
			slow.arrive();
			await fixture.runtime.finishTurn({ ...call(turnID), terminal: true }); await fixture.runtime.dispose();
		}
	});

	it("settles matched and adopted as orthogonal facts exactly once", async () => {
		const settlements: PredictionSettlement[] = [];
		const actionKey = vi.fn((tool: string, args: unknown) => buildPiActionKey(tool, args, "/workspace"));
		const offered = { ...plan("source", "stale", {}), draftTokens: 3 };
		offered.actions[0]!.input = {
			get path() {
				offered.draftTokens = 99;
				return "README.md";
			},
		};
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => offered,
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const candidateReady = candidateSucceeded();
		const fixture = harness({
			source,
			expired: () => true,
			actionKey,
			onEvent: candidateReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await candidateReady.promise;

		expect(await fixture.runtime.consume(call("turn"))).toBeUndefined();
		await fixture.runtime.actual({ ...call("turn"), durationMs: 4, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });

		expect(settlements).toHaveLength(1);
		expect(settlements[0]).toMatchObject({
			observation: "observed",
			match: {
				matched: true,
				adoption: { status: "rejected", cause: { stage: "freshness" } },
			},
		});
		const predictionEvents = fixture.events.filter((event) => event.type === "prediction");
		expect(predictionEvents).toHaveLength(1);
		expect(predictionEvents[0]!.type === "prediction" && predictionEvents[0]!.settlement).toBe(settlements[0]);
		expect(actionKey).toHaveBeenCalledTimes(2);
		expect(fixture.events.find((event) => event.type === "candidate")).toMatchObject({
			candidate: { draftTokens: 3, totalDraftTokens: 3 },
		});
	});

	it("waits for an in-flight candidate to capture its resource baseline before validation", async () => {
		const captured = deferred<{ version: number }>();
		const captureStarted = barrier();
		const validate = vi.fn((version: unknown) =>
			version
				? { status: "valid" as const, metrics: zeroValidationMetrics() }
				: {
						status: "indeterminate" as const,
						cause: cause("freshness", "resource_version_missing"),
						metrics: zeroValidationMetrics(),
					},
		);
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "in-flight", { path: "README.md" }),
		};
		const fixture = harness({
			source,
			capture: () => {
				captureStarted.arrive();
				return captured.promise;
			},
			validate,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await captureStarted.promise;

		const consumed = fixture.runtime.consume(call("turn"));
		expect(validate).not.toHaveBeenCalled();
		captured.resolve({ version: 1 });
		await expect(consumed).resolves.toBe("speculative");
		expect(validate).toHaveBeenCalledOnce();
		expect(validate).toHaveBeenCalledWith({ version: 1 });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("bounds an uncalibrated in-flight join and falls back without cancelling the learning run", async () => {
		let enabled = false;
		const gate = barrier();
		const executionStarted = barrier();
		const candidateReady = candidateSucceeded();
		const source: Source = {
			id: "source",
			enabled: () => enabled,
			propose: () => plan("source", "bounded-join", { path: "README.md" }),
		};
		const fixture = harness({
			source,
			execute: async () => {
				executionStarted.arrive();
				await gate.promise;
				return "learned";
			},
			onEvent: candidateReady.observe,
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "calibration" });
		const calibration = call("calibration");
		expect(await fixture.runtime.consume(calibration)).toBeUndefined();
		await fixture.runtime.actual({ ...calibration, durationMs: 100, output: "actor" });
		await fixture.runtime.finishTurn({ ...calibration, terminal: false });

		enabled = true;
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "prediction" });
		await executionStarted.promise;
		expect(await fixture.runtime.consume(call("prediction"))).toBeUndefined();

		gate.arrive();
		await candidateReady.promise;
		await fixture.runtime.actual({ ...call("prediction"), durationMs: 100, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("prediction"), terminal: false });
		expect(
			fixture.events.find(
				(event) => event.type === "actor_action" && event.turnID === "prediction",
			),
		).toMatchObject({
			settlement: {
				provider: { kind: "actor" },
				rejections: [{ cause: { code: "candidate_join_deadline" } }],
			},
		});
		enabled = false;
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "retained" });
		expect(await fixture.runtime.consume(call("retained"))).toBe("learned");
		await fixture.runtime.finishTurn({ ...call("retained"), terminal: true });
		expect(fixture.events.find((event) => event.type === "actor_action" && event.turnID === "retained"))
			.toMatchObject({ settlement: { provider: { kind: "speculative", timing: { expectedActorMs: 100 } } } });
	});

	it.each(["refresh", "disabled", "disposed", "unwrapped", "terminal", "replaced", "evicted"] as const)("keeps prediction launch ownership across validation: %s", async (mode) => {
		const ready = candidateSucceeded(), refreshed = candidateSucceeded(2);
		const validating = barrier(), validationGate = barrier(), binding = barrier(), bindingGate = barrier();
		const coordinator = new EffectTransactionCoordinator<string>(), cleanup = vi.fn();
		const executed: string[] = [];
		let configured = settings;
		const refreshes = mode === "refresh" || mode === "replaced" || mode === "evicted";
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) => startInput.turnID === "turn-3" ? undefined : plan("source", startInput.turnID, { path: "README.md" }),
			observe: ({ concrete }) => mode === "replaced" && concrete.path === "replace.ts"
				? { proposalID: "turn-2", source: "source", revision: 1,
					upsert: [{ id: "next", type: "tool_call", tool: "read", input: { path: "replacement.ts" } }] } : undefined,
		};
		const fixture = harness({
			source,
			settings: () => configured,
			actionKey: async (tool, args, context) => {
				if (context.type === "start" && (args as { path: string }).path === "replacement.ts") { binding.arrive(); await bindingGate.promise; }
				return buildPiActionKey(tool, args, "/workspace");
			},
			execute: (tool, concrete) => {
				const generation = executed.push(String(concrete.path));
				const branch = world(`generation:${generation}`, {
					executionFingerprint: buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint,
					validate: async () => {
						if (generation === 1) { validating.arrive(); await validationGate.promise; }
						return generation === 1 && mode !== "replaced" && mode !== "evicted"
							? { status: "indeterminate", cause: cause("freshness", "validation_failed"), metrics: zeroValidationMetrics() }
							: { status: "valid", metrics: zeroValidationMetrics() };
					}, onDispose: cleanup,
				});
				return mode === "unwrapped" ? branch : coordinator.execute(coordinator.begin({ tool, route: RESOURCE_ROUTE }), async () => branch);
			},
			onEvent: (event) => { ready.observe(event); refreshed.observe(event); },
		});
		let closing: Promise<void> | undefined, closed = false;
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" }); await ready.promise;
			const unrelated = call("turn-1", { path: "other.ts" });
			expect(await fixture.runtime.consume(unrelated)).toBeUndefined();
			await fixture.runtime.actual({ ...unrelated, durationMs: 1, output: "actor" });
			await fixture.runtime.finishTurn({ ...unrelated, terminal: false });
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" }); await validating.promise;
			if (mode === "replaced") {
				const replacement = call("turn-2", { path: "replace.ts" });
				expect(await fixture.runtime.consume(replacement)).toBeUndefined();
				await fixture.runtime.actual({ ...replacement, durationMs: 1, output: "actor" }); await binding.promise;
			} else if (mode === "evicted") {
				await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: false });
				configured = { ...settings, resourceCacheMaxBytes: 1 };
				await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-3" });
				expect(fixture.runtime.inspect().sharedCandidates).toBe(0);
			} else if (mode !== "refresh") {
				closing = (mode === "disposed" || mode === "unwrapped" ? fixture.runtime.dispose() : mode === "disabled"
					? fixture.runtime.settingsChanged({ ...settings, enabled: false })
					: fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true })).then(() => { closed = true; });
				await new Promise<void>((resolve) => setImmediate(resolve));
				if (mode !== "terminal") expect(closed).toBe(false);
			}
			validationGate.arrive(); await new Promise<void>((resolve) => setImmediate(resolve));
			bindingGate.arrive(); await closing; await new Promise<void>((resolve) => setImmediate(resolve));
			expect(executed).toEqual(["README.md", ...(refreshes ? [mode === "replaced" ? "replacement.ts" : "README.md"] : [])]);
			if (refreshes) {
				await refreshed.promise;
				if (mode === "replaced") {
					await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: false });
					await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-3" });
				}
				expect(await fixture.runtime.consume(call(mode === "refresh" ? "turn-2" : "turn-3",
					{ path: mode === "replaced" ? "replacement.ts" : "README.md" }))).toBe("generation:2");
			}
		} finally { validationGate.arrive(); bindingGate.arrive(); await closing; await fixture.runtime.dispose(); }
		expect(cleanup).toHaveBeenCalledTimes(executed.length);
	});

	it.each(["prediction", "continuation", "running", "sealed", "capture", "promotion", "sealing"] as const)("drains %s work before retiring its session", async (phase) => {
		const sourceWork = phase === "prediction" || phase === "continuation";
		for (const mode of sourceWork ? ["disabled", "disposed", "terminal"] as const : ["disabled", "disposed"] as const) {
			const started = barrier(), producerStarted = barrier(), expired = barrier(), finish = barrier(), cancelled = barrier(), releasing = barrier(), release = barrier();
			const ready = candidateSucceeded(); let released = false, observed = Promise.resolve();
			const cleanup = vi.fn(async () => { releasing.arrive(); await release.promise; released = true; });
			const observing = !sourceWork && phase !== "running" && phase !== "sealed";
			let production: Promise<ReturnType<typeof plan>> | undefined;
			const produce = (signal: AbortSignal) => production = (async () => {
				signal.addEventListener("abort", () => cancelled.arrive(), { once: true }); producerStarted.arrive();
				try {
					await finish.promise;
					if (mode === "disposed") throw new Error("late producer failure");
					return plan("source", "late", { path: "late.ts" });
				}
				finally { await cleanup(); }
			})();
			const fixture = harness({
				source: { id: "source", enabled: () => !observing,
					timeoutMs: () => mode === "terminal" ? 0 : undefined,
					propose: ({ signal }) => phase === "prediction" ? produce(signal) : plan("source", "late", { path: "README.md" }),
					...(phase === "continuation" ? { continue: ({ signal }: { signal: AbortSignal }) => produce(signal) } : {}),
				},
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
				await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
				if (observing) {
					expect(await fixture.runtime.consume(call("turn"))).toBeUndefined();
					if (phase !== "capture") {
						observed = fixture.runtime.actual({ ...call("turn"), durationMs: 1, output: "actor" });
						if (phase === "promotion") await observed; else await started.promise;
					}
				} else if (sourceWork) await producerStarted.promise;
				else await (phase === "running" ? started.promise : ready.promise);
				if (mode === "terminal") await expired.promise;
				const executions = fixture.executions();
				const closing = (mode === "disposed" ? fixture.runtime.dispose() : mode === "terminal"
					? fixture.runtime.finishTurn({ ...call("turn"), terminal: true })
					: fixture.runtime.settingsChanged({ ...settings, enabled: false }))
					.then(() => { expect(released, `${phase}: lifecycle returned before cleanup`).toBe(true); });
				const outcome = Promise.allSettled([closing]);
				if (phase === "running" || sourceWork) await cancelled.promise;
				if (phase === "sealing") await new Promise<void>((resolve) => setImmediate(resolve));
				finish.arrive(); await releasing.promise;
				await new Promise<void>((resolve) => setImmediate(resolve)); // Let the close continuation run; no elapsed-time race.
				release.arrive();
				expect(await outcome).toEqual([{ status: "fulfilled", value: undefined }]); await observed;
				expect(cleanup).toHaveBeenCalledOnce(); expect(fixture.executions()).toBe(executions);
				expect(fixture.runtime.inspect().sharedCandidates).toBe(mode === "terminal" && phase === "continuation" ? 1 : 0);
			} finally { finish.arrive(); release.arrive(); await production?.catch(() => {}); await fixture.runtime.dispose(); }
		}
	});

	it("runs eight independent producers concurrently and deduplicates only by K(a)", async () => {
		const gate = barrier();
		const proposalsEntered = barrier(8);
		const predictionsSettled = barrier(8);
		const candidateReady = candidateSucceeded();
		let entered = 0;
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => 8,
			propose: async ({ proposalIndex }) => {
				entered++;
				proposalsEntered.arrive();
				await gate.promise;
				return plan("source", `proposal:${proposalIndex}`, { path: "README.md" });
			},
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
				predictionsSettled.arrive();
			},
		};
		const fixture = harness({
			source,
			onEvent: candidateReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		expect(entered).toBe(0);
		await proposalsEntered.promise;
		gate.arrive();
		await candidateReady.promise;

		expect(await fixture.runtime.consume(call("turn"))).toBe("speculative");
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
		await predictionsSettled.promise;
		expect(fixture.executions()).toBe(1);
		expect(settlements).toHaveLength(8);
		expect(new Set(settlements.map((item) => item.observation === "observed" && item.actorAction.id))).toEqual(
			new Set(["call:turn"]),
		);
	});

	it("races proposals only after one valid binding and ignores cancelled materialization", async () => {
		for (const mode of ["empty", "invalid", "throw", "late"] as const) {
			const entered = barrier(3), first = barrier(), winner = barrier(), binding = barrier();
			const ready = candidateSucceeded(), aborted: number[] = [], materialized: string[] = [];
			const key = vi.fn(async (tool: string, args: unknown) => {
				if ((args as { path: string }).path === "first.ts") {
					if (mode === "late") await binding.promise;
					else if (mode === "throw") throw new Error("binding failed");
					else return undefined;
				}
				return buildPiActionKey(tool, args, "/workspace");
			});
			const fixture = harness({
				source: {
					id: "source", enabled: () => true, proposalCount: () => 3,
					concurrentProposalPolicy: () => "first_produced",
					propose: async ({ proposalIndex, signal }) => {
						signal.addEventListener("abort", () => aborted.push(proposalIndex), { once: true });
						entered.arrive();
						await entered.promise;
						if (proposalIndex === 0) return mode === "empty" ? undefined : plan("source", "first", { path: "first.ts" });
						if (proposalIndex === 1) {
							await winner.promise;
							return plan("source", "winner", { path: "README.md" });
						}
						return new Promise<undefined>((resolve) => signal.addEventListener("abort", () => resolve(undefined), { once: true }));
					},
				},
				actionKey: key,
				onCandidateMaterialized: ({ input }) => { materialized.push(String(input.path)); },
				onEvent: (event) => {
					if (event.type === "source_request" && event.request.request.index === 0) first.arrive();
					ready.observe(event);
				},
			});
			try {
				await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
				await first.promise;
				expect(aborted, mode).toEqual([]);
				winner.arrive();
				await ready.promise;
				expect(aborted, mode).toEqual([2]);
				binding.arrive();
				expect(await fixture.runtime.consume(call("turn"))).toBe("speculative");
				await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
				expect(materialized, mode).toEqual(["README.md"]);
				expect(key).toHaveBeenCalledTimes(mode === "empty" ? 2 : 3);
				expect(fixture.executions()).toBe(1);
				expect(fixture.events).toContainEqual(expect.objectContaining({ type: "source_request",
					request: expect.objectContaining({ request: expect.objectContaining({ index: 2 }),
						settlement: expect.objectContaining({ status: "aborted", cause: expect.objectContaining({ code: "proposal_race_lost" }) }) }) }));
			} finally {
				winner.arrive();
				binding.arrive();
				await fixture.runtime.dispose();
			}
		}
	});

	it("does not deduplicate equal K(a) work across different execution routes", async () => {
		const candidatesReady = candidateSucceeded(2);
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => [
				plan("source", "route-a", { path: "README.md" }),
				plan("source", "route-b", { path: "README.md" }),
			],
		};
		let routeSequence = 0;
		const fixture = harness({
			source,
			resolveExecution: () => {
				const id = `route-${++routeSequence}`;
				return { ...RESOURCE_ROUTE, backend: id, fingerprint: id };
			},
			onEvent: candidatesReady.observe,
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await candidatesReady.promise;
		expect(fixture.runtime.inspect().sharedCandidates).toBe(2);
		expect(fixture.executions()).toBe(2);
		expect(routeSequence).toBe(2);
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("counts shared Actor work once without substituting a different producer query", async () => {
		const candidateReady = candidateSucceeded();
		const secondReady = candidateSucceeded(2);
		let offset = 1;
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", `shared-timing:${offset}`, { path: "README.md", offset }),
		};
		const fixture = harness({
			source,
			execute: (_tool, args) => args.offset === 2 ? "different query" : "shared",
			onEvent: (event) => { candidateReady.observe(event); secondReady.observe(event); },
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await candidateReady.promise;

		expect(await fixture.runtime.consume(call("turn"))).toBe("shared");
		expect(await fixture.runtime.consume({ ...call("turn"), id: "call:repeat" })).toBe("shared");
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: false });

		const actorEvents = fixture.events.filter((event) => event.type === "actor_action");
		expect(actorEvents).toHaveLength(2);
		const candidateIDs = actorEvents.flatMap((event) =>
			event.settlement.provider.kind === "speculative" ? [event.settlement.provider.candidateID] : [],
		);
		expect(candidateIDs).toHaveLength(2);
		expect(new Set(candidateIDs).size).toBe(1);
		offset = 2;
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "second" });
		await secondReady.promise;
		expect(await fixture.runtime.consume(call("second", { path: "README.md", offset }))).toBe("different query");
		expect(fixture.executions()).toBe(2);
		await fixture.runtime.finishTurn({ ...call("second"), terminal: true });
		expect(fixture.events.find((event) => event.type === "task")).toMatchObject({ timing: { authoritativeToolCount: 2 } });
	});

	it("promotes an authoritative observation into the shared cache without a second execution", async () => {
		let resourceVersion = 1;
		let captures = 0;
		let seals = 0;
		const source: Source = { id: "disabled", enabled: () => false, propose: () => undefined };
		const fixture = harness({
			source,
			captureAuthoritativeResult: (action) => {
				captures++;
				const capturedVersion = resourceVersion;
				return {
					route: RESOURCE_ROUTE,
					seal: async (output) => {
						seals++;
						return world(output, {
							executionFingerprint: action.executionFingerprint,
							validate: async () =>
								capturedVersion === resourceVersion
									? { status: "valid", metrics: zeroValidationMetrics() }
									: {
											status: "stale",
											cause: cause("freshness", "resource_changed"),
											metrics: zeroValidationMetrics(),
										},
						});
					},
					dispose: () => {},
				};
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "actor-result-1" });
		const first = call("actor-result-1");
		expect(await fixture.runtime.consume(first)).toBeUndefined();
		await fixture.runtime.actual({ ...first, durationMs: 4, output: "actor:1" });
		await fixture.runtime.finishTurn({ ...first, terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "actor-result-2" });
		const second = call("actor-result-2");
		await fixture.runtime.previewActorCall(second);
		expect(fixture.executions()).toBe(0);
		expect(await fixture.runtime.consume(second)).toBe("actor:1");
		expect(captures).toBe(1);
		await fixture.runtime.finishTurn({ ...second, terminal: false });

		resourceVersion++;
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "actor-result-3" });
		const third = call("actor-result-3");
		expect(await fixture.runtime.consume(third)).toBeUndefined();
		expect(captures).toBe(2);
		await fixture.runtime.actual({ ...third, durationMs: 2, output: "actor:2" });
		expect(seals).toBe(2);
		await fixture.runtime.finishTurn({ ...third, terminal: true });
	});

	it("expires both pending and admitting next-action requests when the Actor intent arrives", async () => {
		let entered = 0;
		const proposalsEntered = barrier(2);
		const admissionEntered = barrier();
		const admission = barrier();
		const requestsSettled = barrier(2);
		const source: Source = {
			id: "source",
			enabled: () => true,
			requestLifetime: "actor_decision",
			proposalCount: () => 2,
			propose: ({ proposalIndex, signal }) => {
				entered++;
				proposalsEntered.arrive();
				if (proposalIndex === 0) return plan("source", "empty", { path: "other.ts" });
				return new Promise((_, reject) => {
					signal.addEventListener("abort", () => reject(signal.reason), { once: true });
				});
			},
		};
		const fixture = harness({
			source,
			preflight: async () => {
				admissionEntered.arrive();
				await admission.promise;
				return { ok: true };
			},
			onEvent: (event) => {
				if (event.type === "source_request") requestsSettled.arrive();
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await Promise.all([proposalsEntered.promise, admissionEntered.promise]);

		expect(await fixture.runtime.consume(call("turn"))).toBeUndefined();
		admission.arrive();
		await requestsSettled.promise;
		expect(fixture.executions()).toBe(0);
		expect(
			fixture.events.filter(
				(event) => event.type === "source_request" && event.request.settlement.status === "aborted",
			),
		).toHaveLength(1);
		await fixture.runtime.actual({ ...call("turn"), durationMs: 1, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
		expect(fixture.runtime.inspect().pendingPredictions).toBe(0);
	});

	it("holds speculative capacity through cancellation and cleanup, but never queues the actual Actor behind it", async () => {
		for (const mode of ["producer", "preview", "queued", "running"] as const) {
			const executed: string[] = [], aborted: string[] = [];
			const busyStarted = barrier(), stop = barrier(), stopped = barrier(), cleanup = barrier(), released = barrier();
			const targetStarted = barrier(), targetGate = barrier(), targetQueued = barrier();
			const original = SpeculationScheduler.prototype.admit;
			const admission = vi.spyOn(SpeculationScheduler.prototype, "admit").mockImplementation(function (this: SpeculationScheduler<object>, job, forecasts, ...rest) {
				const result = original.call(this, job, forecasts, ...rest);
				if (forecasts[0]?.actionKeyHash === buildPiActionKey("read", { path: "target.ts" }, "/workspace")!.hash) targetQueued.arrive();
				return result;
			});
			const speculative = mode === "producer" || mode === "preview";
			const fixture = harness({
				source: { id: "source", enabled: () => true, propose: () => [
					{ id: "busy", source: "source", revision: 0, actions: [
						{ id: "busy", type: "tool_call", tool: "read", input: { path: "busy.ts" }, expectedLatencyBenefitMs: speculative ? 0 : 1 }] },
					...(mode === "preview" ? [] : [{ id: "target", source: "source", revision: 0, actions: [
						{ id: "target", type: "tool_call" as const, tool: "read", input: { path: "target.ts" }, resourceDemand: mode === "queued" ? 2 : 1 }] }]),
				] },
				settings: () => ({ ...settings, maxConcurrentActions: mode === "running" ? 2 : 1 }),
				actionKey: async (tool, args) => { if ((args as { path: string }).path === "target.ts") await busyStarted.promise; return buildPiActionKey(tool, args, "/workspace"); },
				execute: async (_tool, input, signal) => {
					const path = String(input.path); executed.push(path);
					if (path === "target.ts") { targetStarted.arrive(); await targetGate.promise; return "target"; }
					signal.addEventListener("abort", () => { aborted.push(path); stop.arrive(); }, { once: true }); busyStarted.arrive();
					await stop.promise; await stopped.promise;
					return world("busy", { onDispose: async () => { cleanup.arrive(); await released.promise; } });
				},
			});
			try {
				await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" }); await busyStarted.promise;
				if (mode === "preview") await fixture.runtime.previewActorCall(call("turn", { path: "target.ts" }));
				if (mode === "running") await targetStarted.promise;
				if (mode === "queued") await targetQueued.promise;
				if (speculative) {
					await stop.promise; await new Promise<void>((resolve) => setImmediate(resolve));
					expect(executed, "cancellation is not physical completion").toEqual(["busy.ts"]);
					stopped.arrive(); await cleanup.promise; await new Promise<void>((resolve) => setImmediate(resolve));
					expect(executed, "cleanup still owns the resource slot").toEqual(["busy.ts"]);
					released.arrive(); await targetStarted.promise;
				}
				const consumed = fixture.runtime.consume(call("turn", { path: "target.ts" }));
				await targetStarted.promise; targetGate.arrive();
				expect(await consumed).toBe("target");
				expect(executed).toEqual(["busy.ts", "target.ts"]);
				expect(aborted).toEqual(mode === "running" ? [] : ["busy.ts"]);
			} finally { stopped.arrive(); released.arrive(); targetGate.arrive(); await fixture.runtime.dispose(); admission.mockRestore(); }
		}
	});

	it.each((["legacy-miss", "valid", "uncovered", "rejected", "changed", "aborted", "running-unproven", "running-outside", "running-covered", "running-throws",
		"output-valid", "output-uncovered", "output-rejected", "output-opaque", "output-preferred", "input-lookup"] as const)
		.flatMap((scenario) => [false, ...(!scenario.startsWith("running") ? [true] : [])].map((preview) => [scenario, preview] as const)))(
	"adopts reconstructed input or owned output coverage only after stable evaluation: %s (preview=%s)", async (scenario, preview) => {
		const admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin");
		const adoption = vi.spyOn(SpeculationScheduler.prototype, "observeAdoption");
		const commit = vi.fn(async () => "committed");
		const candidateReady = candidateSucceeded();
		const entered = barrier(), release = barrier(), controller = new AbortController();
		const started = barrier(), completion = barrier(), authorized = barrier(), running = scenario.startsWith("running");
		const outputOnly = scenario.startsWith("output-");
		const succeeds = ["valid", "running-covered", "output-valid", "output-preferred", "input-lookup"].includes(scenario);
		let changed = false;
		const validate = vi.fn(async (): Promise<ResourceValidation> => changed
			? { status: "stale", cause: cause("freshness", "resource_changed"), metrics: zeroValidationMetrics() }
			: { status: "valid", metrics: zeroValidationMetrics() });
		const actor = call("turn", { path: "README.md", offset: ["running-outside", "input-lookup"].includes(scenario) ? 200 : 10, limit: scenario === "running-unproven" ? 200 : 10 });
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
			entered.arrive(); await release.promise;
			if (scenario === "rejected") throw new Error("evaluation failed");
			return scenario === "uncovered" ? undefined : "narrow";
		});
		const fixture = harness({
			source: { id: "source", enabled: () => true,
				propose: () => plan("source", "projection", { path: "README.md", offset: 1, limit: 100 }) },
			projection,
			authorize: () => { authorized.arrive(); return { ok: true }; },
			execute: async () => { started.arrive(); if (running) await completion.promise;
				if (scenario === "output-valid") await new Promise<void>((resolve) => setTimeout(resolve, 5));
				return {
				...world("wide", { validate }),
				...(scenario === "legacy-miss" || (outputOnly && scenario !== "output-preferred") ? {} : { reconstruct }),
				commit,
			}; },
			onEvent: candidateReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await (running ? started.promise : candidateReady.promise);
		projection.canShareInFlight = () => true;
		if (!outputOnly) projection.projectOutput = () => "changed callback";
		else { evidence.complete = true; evidence.view.text = "changed by producer"; }
		const preparation = preview ? fixture.runtime.previewActorCall(actor, controller.signal) : Promise.resolve();
		if (preview) {
			if (!running && !outputOnly && scenario !== "legacy-miss") await entered.promise;
			else await preparation;
			expect(validate).not.toHaveBeenCalled(); expect(commit).not.toHaveBeenCalled();
			if (["valid", "input-lookup"].includes(scenario)) { release.arrive(); await preparation; }
		}
		const consumed = fixture.runtime.consume(actor, controller.signal);
		try {
			if (running) {
				expect(await Promise.race([consumed, authorized.promise.then(() => "joined")])).toBe(succeeds ? "joined" : undefined);
				completion.arrive(); release.arrive();
			} else if (scenario !== "legacy-miss" && !outputOnly) {
				await entered.promise; changed = scenario === "changed";
				if (scenario === "aborted") controller.abort();
				release.arrive();
			}
			expect(await consumed).toBe(succeeds ? "narrow" : undefined);
			expect(commit).toHaveBeenCalledTimes(succeeds ? 1 : 0);
			expect(validate).toHaveBeenCalledTimes(succeeds || scenario === "changed" ? 1 : 0);
			if (["rejected", "changed", "uncovered", "output-rejected"].includes(scenario)) expect(adoption).toHaveBeenCalledOnce();
			if (scenario === "input-lookup") {
				expect(await fixture.runtime.consume({ ...actor, id: "same-query" })).toBe("narrow");
				expect(reconstruct).toHaveBeenCalledOnce();
				changed = true;
				expect(await fixture.runtime.consume({ ...actor, id: "stale-query" })).toBeUndefined();
				expect(reconstruct).toHaveBeenCalledOnce();
				expect(validate).toHaveBeenCalledTimes(3);
			}
			if (scenario === "output-preferred") expect(reconstruct).not.toHaveBeenCalled();
			if (scenario === "output-valid") {
				expect(await fixture.runtime.consume({ ...actor, id: "second-reader" })).toBe("narrow");
				expect(commit).toHaveBeenCalledTimes(2);
			}
			if (succeeds) {
				const request = admission.mock.lastCall![0], actorHash = buildPiActionKey(actor.tool, actor.input, "/workspace")!.hash;
				expect(request.actorIdentity?.actionKeyHash).toBe(actorHash);
				expect(adoption.mock.lastCall![0]).toEqual(request.adoptionIdentity);
				expect(request.adoptionIdentity).toMatchObject({ actionKeyHash: JSON.stringify([request.identity.actionKeyHash, actorHash]),
					operation: JSON.stringify([RESOURCE_ROUTE.backend, RESOURCE_ROUTE.fingerprint, RESOURCE_ROUTE.scope,
						RESOURCE_ROUTE.isolation, RESOURCE_ROUTE.reuse, scenario === "input-lookup" ? "resource.inputs" : "read.range",
						...(preview || ["input-lookup", "output-valid"].includes(scenario) ? ["retained"] : [])]) });
			}
		} finally {
			completion.arrive(); release.arrive(); await Promise.all([preparation, consumed]);
			await fixture.runtime.finishTurn({ ...actor, terminal: true });
			admission.mockRestore(); adoption.mockRestore();
		}
		expect(fixture.events.find((event) => event.type === "task")?.timing?.authoritativeToolCount).toBe(succeeds ? 2 : 0);
		if (scenario === "input-lookup") {
			expect(fixture.events.filter((event) => event.type === "actor_action").at(-1)?.settlement.matchedPredictions).toEqual([]);
			expect(fixture.events.filter((event) => event.type === "prediction").at(-1)?.settlement)
				.toMatchObject({ observation: "observed", match: { matched: false } });
		}
	});

	it.each([[2, 4096, 2], [1, 4096, 3], [2, 128, 3]])("bounds sealed query results by %i entries and %i bytes", async (entries, bytes, evaluations) => {
		const ready = candidateSucceeded(), disposed = vi.fn();
		let now = 100;
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now), admission = vi.spyOn(SpeculationScheduler.prototype, "assessCandidateJoin");
		const learned = entries === 2 && bytes === 4096;
		const reconstruct = vi.fn<NonNullable<WorldBranch<string>["reconstruct"]>>(async ({ args }) => { now += 20; return String((args as { offset: number }).offset); });
		const fixture = harness({
			source: { id: "source", enabled: () => true, propose: ({ startInput }) => startInput.turnID === "first"
				? plan("source", "inputs", { path: "input", offset: 1, limit: 1 }) : undefined },
			settings: () => ({ ...settings, resourceCacheMaxEntries: entries, resourceCacheMaxBytes: bytes }),
			execute: () => { now += 10; return { ...world("1", { onDispose: disposed,
				validate: async () => { now += 3; return { status: "valid", metrics: zeroValidationMetrics() }; } }), reconstruct }; },
			onEvent: ready.observe,
		});
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "first" }); await ready.promise;
			for (const [index, offset] of [2, 3, 2].entries()) {
				const turnID = index === 2 ? "second" : "first";
				if (index === 2) {
					await fixture.runtime.finishTurn({ ...call("first"), terminal: false });
					await fixture.runtime.startTurn({ sessionID: "session", turnID });
				}
				const actor = { ...call(turnID, { path: "input", offset, limit: 1 }), id: String(index) };
				if (!learned || index > 0) await fixture.runtime.previewActorCall(actor);
				expect(await fixture.runtime.consume(actor)).toBe(String(offset));
				if (learned && index === 0) {
					const scheduler = admission.mock.contexts[0] as SpeculationScheduler<object>, request = admission.mock.calls[0]![0];
					for (let sample = 0; sample < 4; sample++) {
						scheduler.observeActorService(request.actorIdentity!, 5);
						scheduler.observeAdoption(request.adoptionIdentity!, 100);
					}
				}
			}
			expect(reconstruct).toHaveBeenCalledTimes(evaluations); expect(fixture.executions()).toBe(1);
			await fixture.runtime.finishTurn({ ...call("second"), terminal: true });
			expect(fixture.events.find((event) => event.type === "task")?.timing).toMatchObject({
				toolExecutionMs: 10 + evaluations * 20, authoritativeToolCount: 1 + evaluations,
				hiddenLatencyMs: learned || bytes === 128 ? 10 : 50,
			});
			now += 10;
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "next-task" });
			expect(await fixture.runtime.consume(call("next-task", { path: "input", offset: 2, limit: 1 }))).toBe("2");
			await fixture.runtime.finishTurn({ ...call("next-task"), terminal: true });
			expect(reconstruct).toHaveBeenCalledTimes(evaluations + (bytes === 128 ? 1 : 0));
			expect(fixture.events.filter((event) => event.type === "task").at(-1)?.timing).toMatchObject({
				toolExecutionMs: bytes === 128 ? 20 : 0, authoritativeToolCount: bytes === 128 ? 1 : 0, hiddenLatencyMs: 0,
			});
		} finally { await fixture.runtime.dispose(); clock.mockRestore(); admission.mockRestore(); }
		expect(disposed).toHaveBeenCalledOnce(); expect(fixture.runtime.inspect().sharedCandidates).toBe(0);
	});

	it.each(["input", "executor", "denied", "closing"])("keeps prepared intent non-authoritative through %s", async (phase) => {
		const ready = candidateSucceeded(), entered = barrier(), release = barrier();
		const disposed = vi.fn(), committed = vi.fn(), coordinator = new EffectTransactionCoordinator<string>();
		const gateway = new ToolExecutionGateway<unknown, string>([]), actor = vi.fn(async () => "Actor");
		let executor = "bound", allowed = true;
		const query = call("turn", { path: "README.md", offset: 10, limit: 1 });
		const fixture = harness({
			source: { id: "source", enabled: () => true, propose: () => plan("source", "inputs", { path: "README.md", offset: 1, limit: 1 }) },
			actionKey: (tool, input) => PI_ACTION_SEMANTICS.buildKey(tool, input, "/workspace", "", { fingerprint: executor }),
			authorize: () => allowed ? { ok: true } : { ok: false, reason: "denied" },
			execute: () => coordinator.execute(coordinator.begin({ tool: "read", route: RESOURCE_ROUTE }), async () => ({
				...world("1", { executionFingerprint: "bound", onDispose: disposed, onCommit: committed,
					validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }) }),
				reconstruct: async ({ args }) => {
					const offset = (args as { offset: number }).offset;
					if (offset === 10) { entered.arrive(); await release.promise; }
					return String(offset);
				},
			})),
			onEvent: ready.observe,
		});
		let preparation: Promise<void> | undefined, closing: Promise<void> | undefined;
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" }); await ready.promise;
			preparation = fixture.runtime.previewActorCall(query); await entered.promise;
			expect(committed).not.toHaveBeenCalled();
			if (phase === "closing") {
				closing = fixture.runtime.dispose();
				expect(await Promise.race([closing.then(() => "closed"), new Promise<string>((resolve) => setImmediate(() => resolve("pending")))])).toBe("pending");
				expect(disposed).not.toHaveBeenCalled();
			} else {
				if (phase === "executor") executor = "rebound";
				if (phase === "denied") allowed = false;
				const formal = phase === "input" ? { ...query, input: { ...query.input, offset: 20 } } : query;
				const delivered = gateway.executeAuthoritative({ tool: formal.tool, input: formal.input }, actor, {
					reuse: () => fixture.runtime.consume(formal), settled: async (result) => {
						if (result.status === "succeeded") await fixture.runtime.actual({ ...formal, ...result });
					},
				});
				expect(await delivered).toBe(phase === "input" ? "20" : "Actor");
				expect(actor).toHaveBeenCalledTimes(phase === "input" ? 0 : 1);
				expect(committed).toHaveBeenCalledTimes(phase === "input" ? 1 : 0);
			}
		} finally {
			release.arrive(); await Promise.all([preparation, closing]);
			await fixture.runtime.dispose(); await gateway.dispose();
		}
		expect(disposed).toHaveBeenCalledOnce(); expect(fixture.runtime.inspect().sharedCandidates).toBe(0);
	});

	it.each(["poisoned", "terminal", "disposed"] as const)("preserves claimed Actor commit ownership through %s", async (phase) => {
		const poisoned = effectCommitFailure(new Error("rollback failed"), "poisoned");
		const candidateReady = candidateSucceeded(), entered = barrier(), release = barrier();
		const coordinator = new EffectTransactionCoordinator<string>(), cleanup = vi.fn();
		const continuation = vi.fn(() => undefined), settlements: PredictionSettlement[] = [];
		const commit = vi.fn(async () => {
			entered.arrive(); await release.promise;
			if (phase === "poisoned") throw poisoned;
			return "speculative";
		});
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "claimed", { path: "README.md" }),
			continueOn: ["actor_adopted"], continue: continuation,
			onSettled: ({ settlement }) => { settlements.push(settlement); },
		};
		const fixture = harness({
			source,
			execute: (tool, concrete) => coordinator.execute(coordinator.begin({ tool, callID: "claimed", route: RESOURCE_ROUTE }), async () => ({
				...world("speculative", { executionFingerprint: buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint }),
				validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
				commit, dispose: cleanup,
			})),
			onEvent: candidateReady.observe,
		});
		let consuming: Promise<string | undefined> | undefined, closing: Promise<void> | undefined;
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" }); await candidateReady.promise;
			consuming = fixture.runtime.consume(call("turn")); await entered.promise;
			if (phase !== "poisoned") closing = phase === "disposed" ? fixture.runtime.dispose()
				: fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
			await new Promise<void>((resolve) => setImmediate(resolve));
			release.arrive();
			if (phase === "poisoned") await expect(consuming).rejects.toBe(poisoned);
			else expect(await consuming).toBe("speculative");
			await closing; await fixture.runtime.dispose();
			expect(commit).toHaveBeenCalledTimes(1); expect(cleanup).toHaveBeenCalledTimes(1);
			expect(continuation).not.toHaveBeenCalled();
			if (phase !== "poisoned") expect(settlements).toEqual([expect.objectContaining({
				match: expect.objectContaining({ matched: true, adoption: expect.objectContaining({ status: "adopted" }) }),
			})]);
		} finally { release.arrive(); await Promise.allSettled([consuming, closing]); await fixture.runtime.dispose(); }
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
		const transactions = new EffectTransactionCoordinator<string>(), candidateReady = candidateSucceeded();
		const gateway = new ToolExecutionGateway<undefined, string>([]), executeActor = vi.fn(async () => "Actor");
		const settlements: PredictionSettlement[] = [];
		const fixture = harness({
			source: { id: "source", enabled: () => true,
				propose: () => indeterminate ? plan("source", "incompatible", actor.input) : undefined,
				onSettled: ({ settlement }) => { settlements.push(settlement); } },
			execute: async (tool, concrete) => {
				const fingerprint = buildPiActionKey(tool, concrete, "/workspace")!.executionFingerprint;
				const source = { ...world("speculative", { resources: ["a.txt"], executionFingerprint: fingerprint }), capturedBytes: 1,
					compatibility: incompatible
						? { status: indeterminate ? "indeterminate" as const : "incompatible" as const, backend: "test", code: indeterminate ? "attestation_missing" : "sealed_incompatible" }
						: { status: "compatible" as const, backend: "test", executionFingerprint: fingerprint },
					validate: async () => ({ status: "valid" as const, metrics: zeroValidationMetrics() }), commit, dispose };
				const transaction = await transactions.execute(transactions.begin({ tool, callID: actor.id,
					route: indeterminate ? RESOURCE_ROUTE : MUTATION_ROUTE }), async () => source);
				Object.assign(source.compatibility, { status: "compatible", executionFingerprint: fingerprint });
				return transaction;
			},
			onEvent: candidateReady.observe,
		});
		try {
			await fixture.runtime.startTurn(actor);
			if (!indeterminate) await fixture.runtime.previewActorCall(actor);
			await candidateReady.promise;
			await expect(gateway.executeAuthoritative({ tool: actor.tool, input: actor.input }, executeActor, {
				reuse: () => fixture.runtime.consume(actor), settled: async (settlement) => {
					if (settlement.status === "succeeded") await fixture.runtime.actual({ ...actor, ...settlement });
				},
			})).resolves.toBe("Actor");
			expect(executeActor).toHaveBeenCalledOnce();
			await fixture.runtime.finishTurn({ ...actor, terminal: indeterminate });
			await vi.waitFor(() => expect(fixture.events.some((event) => event.type === "actor_action")).toBe(true));
			const settlement = fixture.events.find((event) => event.type === "actor_action")?.settlement;
			expect(settlement?.rejections[0]?.cause).toMatchObject(scenario === "classified" ? failure : incompatible
				? { stage: "compatibility", code: indeterminate ? "backend_indeterminate" : "backend_incompatible",
					detail: indeterminate ? "attestation_missing" : "sealed_incompatible" } : { stage: "commit", code: "world_commit_failed" });
			expect(settlement?.provider).toMatchObject({ kind: "actor", origin: "fallback" });
			expect(commit).toHaveBeenCalledTimes(incompatible ? 0 : 1);
			expect(dispose).toHaveBeenCalledOnce();
			if (indeterminate) expect(settlements).toEqual([expect.objectContaining({ match: {
				matched: true, relation: { kind: "exact", distance: 0 },
				adoption: { status: "rejected", candidateID: expect.any(String), cause: settlement?.rejections[0]?.cause },
			} })]);
		} finally { await fixture.runtime.dispose(); await gateway.dispose(); }
	});

	it("keeps one turn on its settings snapshot while master disable remains immediate", async () => {
		let configured = settings;
		const candidateReady = candidateSucceeded();
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "epoch", { path: "README.md" }),
		};
		const fixture = harness({
			source,
			settings: () => configured,
			onEvent: candidateReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await candidateReady.promise;

		configured = { ...settings, tools: settings.tools.filter((tool) => tool !== "read") };
		await fixture.runtime.settingsChanged(configured);
		expect(await fixture.runtime.consume(call("turn-1"))).toBe("speculative");
		await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
		expect(await fixture.runtime.consume(call("turn-2"))).toBe("speculative");

		configured = { ...settings, enabled: false };
		await fixture.runtime.settingsChanged(configured);
		expect(fixture.runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0 });
	});

	it.each(["running", "sealed valid", "sealed stale", "sealed unproven", "observation"])("reconciles Actor effects with $0 ownership", async (phase) => {
		let version = 0, executions = 0;
		const started = barrier(), gate = barrier(), ready = candidateSucceeded(), commits = vi.fn();
		const settlements: PredictionSettlement[] = [];
		const fixture = harness({
			source: { ...futureReadSource({ latestHorizon: 1, expectedDurationMs: 10, subsequent: "placeholder" }),
				onSettled: ({ settlement }) => { settlements.push(settlement); } },
			onEvent: ready.observe,
			execute: async () => {
				const captured = version, output = `future:${++executions}`;
				started.arrive(); if (phase === "running" && executions === 1) await gate.promise;
				return world(output, { onCommit: commits, validate: phase === "sealed unproven" ? undefined : async () => captured === version
					? { status: "valid", metrics: zeroValidationMetrics() }
					: { status: "stale", cause: cause("freshness", "changed"), metrics: zeroValidationMetrics() } });
			},
		});
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
			await (phase === "running" ? started.promise : ready.promise);
			const mutation: Call = { ...call("turn-1"), id: "mutation", tool: phase === "observation" ? "read" : "write",
				input: { path: "future.ts", ...(phase === "observation" ? { offset: 100, limit: 1 } : { content: "new" }) } };
			expect(await fixture.runtime.consume(mutation)).toBeUndefined();
			if (phase === "sealed stale" || phase === "running") version++;
			await fixture.runtime.actual({ ...mutation, durationMs: 1, output: "Actor" });
			gate.arrive(); if (phase === "running") await ready.promise;
			expect(executions).toBe(phase === "running" ? 2 : 1);
			expect(settlements).toHaveLength(0);
			await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
			const actor = call("turn-2", { path: "future.ts" }), hit = !["sealed stale", "sealed unproven"].includes(phase);
			expect(await fixture.runtime.consume(actor)).toBe(hit ? `future:${phase === "running" ? 2 : 1}` : undefined);
			if (!hit) await fixture.runtime.actual({ ...actor, durationMs: 1, output: "Actor" });
			expect(commits).toHaveBeenCalledTimes(hit ? 1 : 0);
			await fixture.runtime.finishTurn({ ...actor, terminal: true });
			expect(settlements).toHaveLength(1);
			expect(settlements[0]).toMatchObject({ observation: "observed", match: { matched: true, adoption: { status: hit ? "adopted" : "rejected" } } });
		} finally { gate.arrive(); await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true }); }
	});

	it("binds the actual executor independently from pending or completed preview identity", async () => {
		for (const [formalPath, settlePreview] of [
			["preview.ts", false], ["formal.ts", false], ["preview.ts", true],
		] as const) {
			const gate = barrier(), firstKeyStarted = barrier();
			let executor = "preview", actionKeys = 0, captured: ActionKey | undefined;
			const fixture = harness({
				source: { id: "disabled", enabled: () => false, propose: () => undefined },
				actionKey: async (tool, input) => {
					const identity = executor;
					actionKeys++;
					if (actionKeys === 1) {
						firstKeyStarted.arrive();
						await gate.promise;
					}
					return PI_ACTION_SEMANTICS.buildKey(tool, input, "/workspace", "", { fingerprint: identity });
				},
				resolveExecution: () => undefined,
				captureAuthoritativeResult: (action) => { captured = action; return undefined; },
			});
			const turnID = `in-flight-key:${formalPath}:${settlePreview}`;
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			const previewCall = call(turnID, { path: "preview.ts" });
			const preview = fixture.runtime.previewActorCall(previewCall);
			await firstKeyStarted.promise;
			if (settlePreview) {
				gate.arrive();
				await preview;
			}
			executor = "actor";
			const actorCall = { ...previewCall, input: { path: formalPath } };
			const consumed = fixture.runtime.consume(actorCall);
			gate.arrive(); await preview;
			expect(await consumed).toBeUndefined();
			expect(captured?.executionFingerprint).toBe("actor");
			expect(captured?.input.path).toBe(formalPath);
			expect(actionKeys).toBe(2);
			await fixture.runtime.actual({ ...actorCall, durationMs: 1, output: "actor" });
			await fixture.runtime.finishTurn({ ...actorCall, terminal: true });
		}
	});

	it("promotes a streamed Actor intent without claiming or committing its prediction", async () => {
		const settlements: PredictionSettlement[] = [];
		const planKeyed = barrier();
		const executionStarted = barrier();
		const candidateReady = candidateSucceeded();
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => ({
				id: "future",
				source: "source",
				revision: 0,
				actions: [
					{
						id: "next",
						type: "tool_call",
						tool: "read",
						input: { path: "future.ts" },
						horizon: 3,
						expectedDurationMs: 10,
					},
				],
			}),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
			onCandidateMaterialized: () => planKeyed.arrive(),
			execute: () => {
				executionStarted.arrive();
				return "future";
			},
			onEvent: candidateReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "streaming-intent" });
		await planKeyed.promise;
		expect(fixture.runtime.inspect().deferredPlanActions).toBe(1);
		expect(fixture.executions()).toBe(0);

		const actorCall = call("streaming-intent", { path: "future.ts" });
		await fixture.runtime.previewActorCall(actorCall);
		await Promise.all([executionStarted.promise, candidateReady.promise]);
		expect(settlements).toEqual([]);
		expect(fixture.events.some((event) => event.type === "actor_action")).toBe(false);
		expect(await fixture.runtime.consume(actorCall)).toBe("future");
		await fixture.runtime.finishTurn({ ...actorCall, terminal: true });
		expect(settlements).toHaveLength(1);
		expect(settlements[0]).toMatchObject({
			match: { matched: true, adoption: { status: "adopted" } },
		});
	});

	it("discards unconsumed previews, joins in-flight intent, and requires isolation", async () => {
		let committed = 0;
		let disposed = 0;
		const slow = barrier();
		const firstExecutionStarted = barrier();
		const slowExecutionStarted = barrier();
		const fixture = harness({
			source: { id: "disabled", enabled: () => false, propose: () => undefined },
			execute: (tool, input) => {
				if (input.content === "slow") {
					slowExecutionStarted.arrive();
					return slow.promise.then(() =>
							world(`${tool}:${String(input.path)}`, {
								checkpoint: { backend: "test", id: "slow", lineage: "slow", depth: 0 },
								resources: ["."],
								onCommit: () => committed++,
								onDispose: () => disposed++,
							}),
						);
				}
				firstExecutionStarted.arrive();
				return world(`${tool}:${String(input.path)}`, {
					checkpoint: { backend: "test", id: "preview", lineage: "preview", depth: 0 },
					resources: ["."],
					onCommit: () => committed++,
					onDispose: () => disposed++,
				});
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "aborted-preview" });
		const writeCall: Call = {
			sessionID: "session",
			turnID: "aborted-preview",
			id: "write-preview",
			tool: "write",
			input: { path: "preview.txt", content: "preview" },
		};
		await fixture.runtime.previewActorCall(writeCall);
		await firstExecutionStarted.promise;
		await fixture.runtime.previewActorCall({
			...writeCall,
			id: "bash-preview",
			tool: "bash",
			input: { command: "echo preview" },
		});
		expect(fixture.executions()).toBe(1);
		expect(committed).toBe(0);

		await fixture.runtime.finishTurn({ ...writeCall, terminal: false });
		expect(committed).toBe(0);
		expect(disposed).toBe(1);
		expect(fixture.runtime.inspect("session").exclusiveCandidates).toBe(0);

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "incomplete-preview" });
		const slowCall: Call = {
			...writeCall,
			turnID: "incomplete-preview",
			id: "slow-preview",
			input: { path: "slow.txt", content: "slow" },
		};
		await fixture.runtime.previewActorCall(slowCall);
		await slowExecutionStarted.promise;
		const consumed = fixture.runtime.consume(slowCall);
		expect(fixture.executions()).toBe(2);
		slow.arrive();
		expect(await consumed).toBe("write:slow.txt");
		expect(fixture.executions()).toBe(2);
		expect(committed).toBe(1);
		await fixture.runtime.finishTurn({ ...slowCall, terminal: true });
	});

	it.each(["exact", "future", "due"] as const)("selects one captured prediction relation per plan without isolation: %s", async (mode) => {
		const settlements: PredictionSettlement[] = [];
		const projected = mode !== "exact", tool = projected ? "read" : "bash";
		const horizons = mode === "due" ? [4, 0, 2, 2, 1, 3] : projected ? [4, 3, 1, 1, 2, 5] : [0];
		const proposalIDs = projected ? ["second", "first"] : ["bash"];
		const predictedInput = projected ? { path: "README.md", offset: 1, limit: 100 } : { command: "build" };
		const actionCount = horizons.length * proposalIDs.length, routeChecked = barrier(actionCount);
		const project = vi.fn(READ_RANGE_ACTION_KEY_PROJECTOR.project);
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) => startInput.turnID !== "turn-1" ? undefined : proposalIDs.map((id) => ({
				id, source: "source", revision: 0,
				actions: horizons.map((horizon, index) => ({
					id: String(index), type: "tool_call", tool, input: predictedInput, horizon, latestHorizon: 8,
				})),
			})),
			onSettled: ({ settlement }) => { settlements.push(settlement); },
		};
		const fixture = harness({
			source,
			projection: { ...READ_RANGE_ACTION_KEY_PROJECTOR, project },
			resolveExecution: () => { routeChecked.arrive(); return undefined; },
		});
		let turnID = "turn-1";
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			await routeChecked.promise;
			await new Promise<void>(setImmediate);
			expect(fixture.runtime.inspect()).toMatchObject({
				exclusiveCandidates: 0, sharedCandidates: 0, executionBlockedPlanActions: actionCount,
			});
			for (let previous = 1; mode === "due" && previous <= 2; previous++) {
				const earlier = call(turnID, { path: "unrelated.ts" });
				expect(await fixture.runtime.consume(earlier)).toBeUndefined();
				await fixture.runtime.actual({ ...earlier, durationMs: 1, output: "actor" });
				await fixture.runtime.finishTurn({ ...earlier, terminal: false });
				turnID = `turn-${previous + 1}`;
				await fixture.runtime.startTurn({ sessionID: "session", turnID });
			}
			const firstCall: Call = {
				sessionID: "session", turnID, id: "first", tool,
				input: projected ? { path: "README.md", offset: 10, limit: 10 } : predictedInput,
			};
			project.mockClear();
			await fixture.runtime.previewActorTool(firstCall);
			expect(project).not.toHaveBeenCalled();
			await fixture.runtime.previewActorCall(firstCall);
			expect.soft(project).toHaveBeenCalledTimes(projected ? actionCount : 0);
			expect(settlements).toEqual([]);
			project.mockClear();
			expect(await fixture.runtime.consume(firstCall)).toBeUndefined();
			expect.soft(project).toHaveBeenCalledTimes(projected ? actionCount : 0);
			await fixture.runtime.actual({ ...firstCall, durationMs: 2, output: "actor-built" });
			await fixture.runtime.finishTurn({ ...firstCall, terminal: true });
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
			expect(fixture.events.filter((event) => event.type === "actor_action" && event.settlement.actorAction.id === "first"))
				.toMatchObject([{ settlement: { provider: { kind: "actor", origin: "fallback" } } }]);
			expect(fixture.executions()).toBe(0);
			expect(fixture.events.some((event) => event.type === "candidate")).toBe(false);
		} finally { await fixture.runtime.dispose(); }
	});

	it("keeps a next-decision continuation alive across parallel tools in one Actor decision", async () => {
		const gate = barrier();
		const parentReady = barrier();
		const continuationStarted = barrier();
		const childReady = candidateSucceeded(1, "child.ts");
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			requestLifetime: "actor_decision",
			continueOn: ["actor_adopted"],
			propose: () => plan("source", "parallel-continuation", { path: "parent.ts" }),
			continue: async ({ proposalID, revision, trigger }) => {
				if (trigger !== "actor_adopted") return undefined;
				continuationStarted.arrive();
				await gate.promise;
				return {
					proposalID,
					source: "source",
					revision,
					upsert: [
						{
							id: "child",
							type: "tool_call",
							tool: "read",
							input: { path: "child.ts" },
						},
					],
				};
			},
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				const path = String(input.path);
				if (path === "parent.ts") parentReady.arrive();
				return `${String(input.path)}:output`;
			},
			onEvent: childReady.observe,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "parallel-continuation" });
		await parentReady.promise;

		const parent = {
			sessionID: "session",
			turnID: "parallel-continuation",
			id: "parent-call",
			tool: "read",
			input: { path: "parent.ts" },
		};
		expect(await fixture.runtime.consume(parent)).toBe("parent.ts:output");
		await continuationStarted.promise;

		const sibling = { ...parent, id: "sibling-call", input: { path: "sibling.ts" } };
		expect(await fixture.runtime.consume(sibling)).toBeUndefined();
		await fixture.runtime.actual({ ...sibling, durationMs: 1_000, output: "actor" });

		gate.arrive();
		await childReady.promise;
		const sameBatchChild = { ...parent, id: "same-batch-child", input: { path: "child.ts" } };
		expect(await fixture.runtime.consume(sameBatchChild)).toBe("child.ts:output");
		await fixture.runtime.finishTurn({ ...parent, terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "next-decision" });
		expect(
			await fixture.runtime.consume({ ...sameBatchChild, turnID: "next-decision", id: "next-decision-child" }),
		).toBe("child.ts:output");
		await fixture.runtime.finishTurn({ ...sameBatchChild, turnID: "next-decision", terminal: true });
		expect(
			settlements.map((settlement) =>
				settlement.observation === "observed" ? settlement.actorAction.decisionSequence : undefined,
			),
		).toEqual([1, 2]);
	});

	it.each(["retained", "retry", "expired", "replaced", "terminal"] as const)("keeps queued continuation authority %s across plan and turn boundaries", async (phase) => {
		const gate = barrier();
		const continuationStarted = barrier();
		const retained = phase === "retained" || phase === "retry", nextChild = phase === "retry" ? "late-child" : "child";
		const childReady = candidateSucceeded(1, `${nextChild}.ts`);
		const replacementReady = candidateSucceeded(1, "replacement.ts");
		let proposals = 0;
		const continuations: string[] = [];
		const executed: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => 1,
			propose: () => {
				proposals++;
				return plan("source", "cross-turn", { path: "parent.ts" });
			},
			continue: async ({ proposalID, actionID, revision, candidate, trigger }) => {
				if (String(candidate.input.path) !== "parent.ts") return undefined;
				continuations.push(trigger);
				if (trigger === "execution_succeeded") {
					continuationStarted.arrive(); await gate.promise;
					if (phase === "retry") return undefined;
				}
				const child = trigger === "execution_succeeded" ? "child" : "late-child";
				return childPlanUpdate({ proposalID, actionID, revision }, child, `${child}.ts`);
			},
			observe: ({ concrete }) => phase === "replaced" && concrete.path === "replace.ts" ? {
				proposalID: "cross-turn", source: "source", revision: 2,
				upsert: [{ id: "next", type: "tool_call", tool: "read", input: { path: "replacement.ts" } }],
			} : undefined,
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
			onEvent: (event) => { childReady.observe(event); replacementReady.observe(event); },
		});
		let closing: Promise<void> | undefined;
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "parent-turn" });
			await continuationStarted.promise;
			expect(await fixture.runtime.consume(call("parent-turn", { path: "parent.ts" }))).toBe("parent.ts:output");
			if (phase === "terminal") closing = fixture.runtime.finishTurn({ ...call("parent-turn"), terminal: true });
			else if (phase === "replaced") {
				const replacement = { ...call("parent-turn", { path: "replace.ts" }), id: "replace-parent" };
				expect(await fixture.runtime.consume(replacement)).toBeUndefined();
				await fixture.runtime.actual({ ...replacement, durationMs: 1, output: "actor" });
				await replacementReady.promise;
				gate.arrive();
			} else {
				await fixture.runtime.finishTurn({ ...call("parent-turn"), terminal: false });
				expect(fixture.runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 1 });
				await fixture.runtime.startTurn({ sessionID: "session", turnID: "child-turn" });
				expect(proposals).toBe(1);
				if (retained) { gate.arrive(); await childReady.promise; }
				else {
					const unrelated = call("child-turn", { path: "other.ts" });
					expect(await fixture.runtime.consume(unrelated)).toBeUndefined();
					await fixture.runtime.actual({ ...unrelated, durationMs: 1, output: "actor" });
				}
			}
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(continuations).toEqual(["execution_succeeded", ...(phase === "retry" ? ["actor_adopted"] : [])]);
			expect(executed).toEqual(["parent.ts", ...(retained ? [`${nextChild}.ts`] : phase === "replaced" ? ["replacement.ts"] : [])]);
			if (retained) {
				expect(await fixture.runtime.consume(call("child-turn", { path: `${nextChild}.ts` }))).toBe(`${nextChild}.ts:output`);
				await fixture.runtime.finishTurn({ ...call("child-turn"), terminal: true });
			}
		} finally { gate.arrive(); await closing; await fixture.runtime.dispose(); }
	});

	it("adopts a target-state-valid child after its parent prediction misses", async () => {
		let enabled = true;
		let dependencyChange: boolean | undefined;
		const childPrepared = barrier();
		const executed: string[] = [];
		const childReady = candidateSucceeded(1, "late.ts");
		const source: Source = {
			id: "source",
			enabled: () => enabled,
			propose: () => plan("source", "conditional", { path: "parent.ts" }),
			continue: async ({ proposalID, actionID, revision, trigger }) => {
				if (trigger !== "execution_succeeded") return undefined;
				return childPlanUpdate({ proposalID, actionID, revision }, "late-child", "late.ts");
			},
		};
		const fixture = harness({
			source,
			preflight: (_signal, candidate) => {
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
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "miss" });
			await childPrepared.promise;
			expect(dependencyChange).toBe(false);
			await childReady.promise;
			expect(await fixture.runtime.consume(call("miss", { path: "other.ts" }))).toBeUndefined();
			await fixture.runtime.actual({ ...call("miss", { path: "other.ts" }), durationMs: 1, output: "actor" });
			await fixture.runtime.finishTurn({ ...call("miss"), terminal: false });

			enabled = false;
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "target" });
			expect(await fixture.runtime.consume(call("target", { path: "late.ts" }))).toBe("late.ts:output");
			await fixture.runtime.finishTurn({ ...call("target"), terminal: true });
			expect(executed).toEqual(["parent.ts", "late.ts"]);
			expect(
				fixture.events
					.filter((event) => event.type === "prediction")
					.map((event) => (event.settlement.observation === "observed" ? event.settlement.match.matched : undefined)),
			).toEqual([false, true]);
		} finally {
			await fixture.runtime.finishTurn({ ...call("target"), terminal: true });
		}
	});

	it.each(["baseline", "replaced", "adopted", "claimed", "cancelled"] as const)("keeps child reuse on its current parent lineage: %s", async (mode) => {
		const claimed = mode === "claimed" || mode === "cancelled", actorController = new AbortController();
		let enabled = true, workspaceVersion = 0, holdReuse = false;
		const executed: string[] = [];
		const childParents: string[] = [];
		const aliasOutputs: string[] = [];
		const childrenReady = candidateSucceeded(2, '"content":"child"');
		const parentReady = candidateSucceeded(1, "parent-new"), validationStarted = barrier(), validationGate = barrier();
		const parentBinding = barrier(), parentGate = barrier(), aliasReady = barrier(), cleanup = vi.fn(), transactions = new EffectTransactionCoordinator<string>();
		const parentAction = (content: string) => ({ id: "parent", type: "tool_call" as const, tool: "write", input: { path: `${content}.txt`, content } });
		const childAction = { id: "child", type: "tool_call" as const, tool: "write", input: { path: "child.txt", content: "child" },
			expectedDurationMs: 1_000, dependsOn: [{ actionID: "parent", condition: "execution_succeeded" as const }] };
		const source: Source = {
			id: "source",
			enabled: () => enabled,
			proposalCount: () => 2,
			continueOn: ["execution_succeeded"],
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
				if (actionID === "alias") { aliasOutputs.push(output); aliasReady.arrive(); }
				if (String(candidate.input.content).startsWith("child")) return undefined;
				return { proposalID, source: "source", revision, upsert: [childAction] };
			},
		};
		const fixture = harness({
			source,
			actionKey: async (tool, input, context) => {
				if (context.type === "start" && (input as { content?: string }).content === "parent-new") { parentBinding.arrive(); await parentGate.promise; }
				return buildPiActionKey(tool, input, "/workspace");
			},
			execute: (tool, input, _signal, parentWorld) => {
				const content = String(input.content);
				executed.push(content);
				if (content === "child") childParents.push(String(parentWorld?.output));
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
						return { status: "valid", metrics: zeroValidationMetrics() };
					},
				}));
			},
			onEvent: (event) => { childrenReady.observe(event); parentReady.observe(event); },
		});
		const expectedParent = mode === "replaced" ? "parent-new" : "parent-0";
		const parentCall: Call = { sessionID: "session", turnID: "parent", id: "actor-parent", tool: "write", input: parentAction(expectedParent).input };
		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "parent" }); await childrenReady.promise;
			expect(executed.sort()).toEqual(["child", "child", "parent-0", "parent-1"]);
			expect(childParents.sort()).toEqual(["parent-0", "parent-1"]);
			if (mode !== "baseline") {
				holdReuse = !claimed;
				const alias = call("parent", { path: "alias.ts" });
				expect(await fixture.runtime.consume(alias)).toBeUndefined();
				await fixture.runtime.actual({ ...alias, durationMs: 1, output: "actor" });
				await (claimed ? aliasReady : validationStarted).promise;
				if (mode === "replaced") {
					const replacement = call("parent", { path: "replace.ts" });
					expect(await fixture.runtime.consume(replacement)).toBeUndefined();
					await fixture.runtime.actual({ ...replacement, durationMs: 1, output: "actor" }); await parentBinding.promise;
				} else if (mode === "adopted") expect(await fixture.runtime.consume(parentCall)).toBe(expectedParent);
				holdReuse = false; if (!claimed) validationGate.arrive(); await new Promise<void>(setImmediate);
				expect(aliasOutputs).toEqual(mode === "replaced" ? [] : ["child:parent-0"]);
				if (mode === "replaced") {
					parentGate.arrive(); await parentReady.promise; await new Promise<void>(setImmediate);
					expect(childParents).toEqual(["parent-0", "parent-1", "parent-new"]);
				} else expect(childParents.filter((parent) => parent === "parent-0")).toEqual(["parent-0"]);
			}
			if (mode !== "adopted") expect(await fixture.runtime.consume(parentCall)).toBe(expectedParent);
			enabled = claimed; await fixture.runtime.finishTurn({ ...parentCall, terminal: false });
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "child" });
			const childCall: Call = { ...parentCall, turnID: "child", id: "actor-child", input: childAction.input };
			holdReuse = claimed;
			const childConsumption = fixture.runtime.consume(childCall, actorController.signal);
			if (claimed) {
				await validationStarted.promise;
				const replacement = call("child", { path: "replace.ts" });
				expect(await fixture.runtime.consume(replacement)).toBeUndefined();
				await fixture.runtime.actual({ ...replacement, durationMs: 1, output: "actor" }); await parentBinding.promise;
				if (mode === "cancelled") actorController.abort();
				holdReuse = false; validationGate.arrive();
			}
			expect(await childConsumption).toBe(mode === "cancelled" ? undefined : `child:${expectedParent}`);
			expect(workspaceVersion).toBe(mode === "cancelled" ? 1 : 2);
			await new Promise<void>(setImmediate);
			expect(cleanup.mock.calls.filter(([output]) => output === `child:${expectedParent}`)).toHaveLength(1);
			parentGate.arrive();
			await fixture.runtime.finishTurn({ ...childCall, terminal: true });
			const predictions = fixture.events.filter((event) => event.type === "prediction").map((event) => event.settlement);
			expect(new Set(predictions.map((settlement) => settlement.prediction.id)).size).toBe(predictions.length);
			if (claimed) {
				const matched = predictions.filter((settlement) => settlement.observation === "observed" &&
					settlement.actorAction.id === childCall.id && settlement.match.matched);
				expect(matched).toHaveLength(1);
				expect(matched[0]).toMatchObject({ match: { adoption: mode === "cancelled"
					? { status: "rejected", cause: { code: "actor_aborted" } } : { status: "adopted" } } });
			}
			expect(fixture.events.filter((event) => event.type === "actor_action" && event.settlement.actorAction.id === childCall.id))
				.toHaveLength(mode === "cancelled" ? 0 : 1);
		} finally { validationGate.arrive(); parentGate.arrive(); await fixture.runtime.dispose(); }
		expect(cleanup).toHaveBeenCalledTimes(executed.length);
	});
});

function world(
	output: string,
	options: {
		readonly backend?: string;
		readonly checkpoint?: WorldCheckpoint;
		readonly executionMetrics?: WorldExecutionMetrics;
		readonly resources?: readonly string[];
		readonly onCommit?: () => void;
		readonly onDispose?: () => void;
		readonly executionFingerprint?: string;
		readonly validate?: () => Promise<ResourceValidation>;
	} = {},
): WorldBranch<string> {
	return {
		output,
		backend: options.backend ?? "test",
		...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
		resources: options.resources ?? [],
		capturedBytes: 0,
		executionMetrics: options.executionMetrics ?? {},
		compatibility: {
			status: "compatible" as const,
			backend: options.backend ?? "test",
			executionFingerprint: options.executionFingerprint ?? "",
		},
		...(options.validate ? { validate: options.validate } : {}),
		commit: async () => {
			options.onCommit?.();
			return output;
		},
		dispose: () => options.onDispose?.(),
	};
}

function isWorldBranch(value: unknown): value is WorldBranch<string> {
	return Boolean(
		value && typeof value === "object" && typeof (value as Partial<WorldBranch<string>>).commit === "function",
	);
}

function deferred<Value = void>() {
	let resolve!: (value: Value | PromiseLike<Value>) => void;
	const promise = new Promise<Value>((done) => (resolve = done));
	return { promise, resolve };
}

function barrier(expected = 1) {
	const done = deferred<void>();
	return {
		promise: done.promise,
		arrive: () => {
			if (expected > 0 && --expected === 0) done.resolve();
		},
	};
}

function candidateSucceeded(expected = 1, actionFragment?: string) {
	const reached = barrier(expected);
	return {
		promise: reached.promise,
		observe: (event: SpeculativeActionEvent<string>) => {
			if (
				event.type === "candidate" &&
				event.state.status === "succeeded" &&
				(!actionFragment || event.candidate.predictedAction.includes(actionFragment))
			) reached.arrive();
		},
	};
}
