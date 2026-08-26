import { describe, expect, it, vi } from "vitest";
import { type ActionProjectionRule, READ_RANGE_ACTION_KEY_PROJECTOR } from "../src/action-key-projection.ts";
import { buildPiActionKey } from "../src/action-semantics.ts";
import type { SpeculativeExecutionRoute, WorldBranch, WorldCheckpoint } from "../src/execution-world.ts";
import type {
	CandidatePreflight,
	SpeculativeActionEvent,
	SpeculativeActionSettings,
	SpeculativePlanSource,
} from "../src/runtime.ts";
import { makeStructuralSpeculativeActionRuntime } from "../src/runtime-engine.ts";
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
	readonly preflight?: (signal: AbortSignal) => CandidatePreflight | Promise<CandidatePreflight>;
	readonly projection?: boolean;
	readonly coveringAction?: ActionProjectionRule<string>["coveringAction"];
	readonly onEvent?: (event: SpeculativeActionEvent<string>) => void | Promise<void>;
	readonly actionKey?: (
		tool: string,
		args: unknown,
		context: { readonly type: "start" | "consume" },
	) => ReturnType<typeof buildPiActionKey> | Promise<ReturnType<typeof buildPiActionKey>>;
	readonly resolveExecution?: (tool: string) => SpeculativeExecutionRoute | undefined;
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
		actual: (call) => ({ id: call.id, tool: call.tool, input: call.input }),
		preflightCandidate: ({ signal }) => input.preflight?.(signal) ?? { ok: true },
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
		projectionRules: input.projection
			? [
					{
						...READ_RANGE_ACTION_KEY_PROJECTOR,
						...(input.coveringAction ? { coveringAction: input.coveringAction } : {}),
						captureCoverage: () => ({ complete: true }),
						projectOutput: () => undefined,
					},
				]
			: [],
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
	it("admits independent actions and proposals without head-of-line blocking", async () => {
		let releaseSlow!: () => void;
		const slow = new Promise<void>((resolve) => {
			releaseSlow = resolve;
		});
		let slowStarted = false;
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => 2,
			propose: ({ proposalIndex }) =>
				proposalIndex === 0
					? {
							id: "proposal:0",
							source: "source",
							revision: 0,
							actions: [
								{ id: "slow", type: "tool_call", tool: "read", input: { path: "slow.ts" } },
								{ id: "same-plan", type: "tool_call", tool: "read", input: { path: "same-plan.ts" } },
							],
						}
					: plan("source", "proposal:1", { path: "other-plan.ts" }),
		};
		const fixture = harness({
			source,
			actionKey: async (tool, args, context) => {
				if (context.type === "start" && (args as { path?: unknown }).path === "slow.ts") {
					slowStarted = true;
					await slow;
				}
				return buildPiActionKey(tool, args, "/workspace");
			},
		});

		try {
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "parallel-admission" });
			await waitFor(() => slowStarted);
			await waitFor(() => fixture.executions() === 2, 500);
			expect(await fixture.runtime.consume(call("parallel-admission", { path: "same-plan.ts" }))).toBe(
				"speculative",
			);
		} finally {
			releaseSlow();
			await fixture.runtime.finishTurn({
				...call("parallel-admission"),
				terminal: true,
			});
		}
	});

	it("settles matched and adopted as orthogonal facts exactly once", async () => {
		const settlements: PredictionSettlement[] = [];
		const actionKey = vi.fn((tool: string, args: unknown) => buildPiActionKey(tool, args, "/workspace"));
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "stale", { path: "README.md" }),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({ source, expired: () => true, actionKey });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

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
		expect(
			fixture.events
				.filter((event) => event.type === "candidate")
				.map((event) => (event.type === "candidate" ? event.state.status : undefined)),
		).toEqual(["running", "succeeded"]);
		expect(fixture.events.filter((event) => event.type === "source_request")).toHaveLength(1);
		expect(fixture.events.find((event) => event.type === "actor_action")).toMatchObject({
			settlement: {
				provider: { kind: "actor", durationMs: 4 },
				rejections: [{ cause: { stage: "freshness" } }],
			},
		});
		// The candidate and Actor intent are keyed once each. Fallback completion reuses
		// the ActorAction-owned identity instead of observing a later environment.
		expect(actionKey).toHaveBeenCalledTimes(2);
	});

	it("waits for an in-flight candidate to capture its resource baseline before validation", async () => {
		let releaseCapture!: () => void;
		const captured = new Promise<{ version: number }>((resolve) => {
			releaseCapture = () => resolve({ version: 1 });
		});
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
		const fixture = harness({ source, capture: () => captured, validate });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		const consumed = fixture.runtime.consume(call("turn"));
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(validate).not.toHaveBeenCalled();
		releaseCapture();
		await expect(consumed).resolves.toBe("speculative");
		expect(validate).toHaveBeenCalledOnce();
		expect(validate).toHaveBeenCalledWith({ version: 1 });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("keeps a fresh exact generation reachable when an older version is indeterminate", async () => {
		let captures = 0;
		let runs = 0;
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) => plan("source", startInput.turnID, { path: "README.md" }),
		};
		const fixture = harness({
			source,
			capture: () => ({ version: ++captures }),
			validate: (version) =>
				(version as { version: number }).version === 1
					? {
							status: "indeterminate",
							cause: cause("freshness", "validation_failed"),
							metrics: zeroValidationMetrics(),
						}
					: { status: "valid", metrics: zeroValidationMetrics() },
			execute: () => `generation:${++runs}`,
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await waitFor(() => runs === 1);
		const unrelated = call("turn-1", { path: "other.ts" });
		expect(await fixture.runtime.consume(unrelated)).toBeUndefined();
		await fixture.runtime.actual({ ...unrelated, durationMs: 1, output: "actor" });
		await fixture.runtime.finishTurn({ ...unrelated, terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
		await waitFor(() => runs === 2);
		expect(await fixture.runtime.consume(call("turn-2"))).toBe("generation:2");
		await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true });
	});

	it("keeps slow observers off the hit path and freezes event attribution before cleanup", async () => {
		let release!: () => void;
		const blocked = new Promise<void>((resolve) => {
			release = resolve;
		});
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "slow-observer", { path: "README.md" }),
		};
		const fixture = harness({ source, onEvent: () => blocked });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		const result = await Promise.race([
			fixture.runtime.consume(call("turn")),
			new Promise<"blocked">((resolve) => setTimeout(() => resolve("blocked"), 50)),
		]);
		expect(result).toBe("speculative");
		const disabled = fixture.runtime.settingsChanged({ ...settings, enabled: false });
		release();
		await disabled;
		expect(fixture.events.find((event) => event.type === "actor_action")).toMatchObject({
			settlement: { matchedPredictions: [{ source: "source" }] },
		});
	});

	it("disposes a sealed backend branch that arrives after its candidate was cancelled", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const dispose = vi.fn();
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "late-branch", { path: "README.md" }),
		};
		const fixture = harness({
			source,
			execute: async () => {
				await gate;
				return world("late", { onDispose: dispose });
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);
		const disabling = fixture.runtime.settingsChanged({ ...settings, enabled: false });
		release();
		await disabling;
		await waitFor(() => dispose.mock.calls.length === 1);
		expect(dispose).toHaveBeenCalledOnce();
	});

	it("preserves a claimed match when disable races in-flight validation", async () => {
		let validationEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			validationEntered = resolve;
		});
		let releaseValidation!: () => void;
		const validationGate = new Promise<void>((resolve) => {
			releaseValidation = resolve;
		});
		const settlements: PredictionSettlement[] = [];
		const dispose = vi.fn();
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "disable-race", { path: "README.md" }),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
			execute: () =>
				world("speculative", {
					onDispose: dispose,
					validate: async () => {
						validationEntered();
						await validationGate;
						return { status: "valid", metrics: zeroValidationMetrics() };
					},
				}),
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);
		const consumed = fixture.runtime.consume(call("turn"));
		await entered;

		const disabling = fixture.runtime.settingsChanged({ ...settings, enabled: false });
		releaseValidation();
		expect(await consumed).toBeUndefined();
		await disabling;
		await waitFor(() => settlements.length === 1);

		expect(settlements[0]).toMatchObject({
			observation: "observed",
			match: {
				matched: true,
				adoption: { status: "rejected", cause: { stage: "control", code: "disabled" } },
			},
		});
		expect(dispose).toHaveBeenCalledOnce();
	});

	it("runs eight independent producers concurrently and deduplicates only by K(a)", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let entered = 0;
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => 8,
			propose: async ({ proposalIndex }) => {
				entered++;
				await gate;
				return plan("source", `proposal:${proposalIndex}`, { path: "README.md" });
			},
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({ source });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		expect(entered).toBe(0);
		await waitFor(() => entered === 8);
		release();
		await waitFor(() => fixture.runtime.inspect().pendingPredictions === 0);
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		expect(await fixture.runtime.consume(call("turn"))).toBe("speculative");
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
		expect(fixture.executions()).toBe(1);
		expect(settlements).toHaveLength(8);
		expect(new Set(settlements.map((item) => item.observation === "observed" && item.actorAction.id))).toEqual(
			new Set(["call:turn"]),
		);
	});

	it("cancels outstanding initial proposal siblings only after the first produced result", async () => {
		let releaseWinner!: () => void;
		const winnerGate = new Promise<void>((resolve) => {
			releaseWinner = resolve;
		});
		const entered: number[] = [];
		const aborted: number[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			proposalCount: () => 3,
			concurrentProposalPolicy: () => "first_produced",
			propose: async ({ proposalIndex, signal }) => {
				entered.push(proposalIndex);
				if (proposalIndex === 0) return undefined;
				if (proposalIndex === 1) {
					await winnerGate;
					return plan("source", "winner", { path: "README.md" });
				}
				return new Promise<undefined>((resolve) => {
					signal.addEventListener(
						"abort",
						() => {
							aborted.push(proposalIndex);
							resolve(undefined);
						},
						{ once: true },
					);
				});
			},
		};
		const fixture = harness({ source });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => entered.length === 3);
		releaseWinner();
		await waitFor(() => fixture.events.filter((event) => event.type === "source_request").length === 3);
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		expect(aborted).toEqual([2]);
		expect(
			fixture.events
				.filter((event) => event.type === "source_request")
				.map((event) => [event.request.request.index, event.request.settlement]),
		).toEqual(
			expect.arrayContaining([
				[0, expect.objectContaining({ status: "empty" })],
				[1, expect.objectContaining({ status: "produced" })],
				[
					2,
					expect.objectContaining({
						status: "aborted",
						cause: expect.objectContaining({ code: "proposal_race_lost" }),
					}),
				],
			]),
		);
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("does not deduplicate equal K(a) work across different execution routes", async () => {
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
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 2);
		await waitFor(() => fixture.executions() === 2);
		expect(routeSequence).toBe(2);
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("counts one shared execution once when it serves multiple Actor actions", async () => {
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "shared-timing", { path: "README.md" }),
		};
		const fixture = harness({
			source,
			execute: async () => {
				await new Promise((resolve) => setTimeout(resolve, 10));
				return "shared";
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		expect(await fixture.runtime.consume(call("turn"))).toBe("shared");
		expect(await fixture.runtime.consume({ ...call("turn"), id: "call:repeat" })).toBe("shared");
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });

		const actorEvents = fixture.events.filter((event) => event.type === "actor_action");
		expect(actorEvents).toHaveLength(2);
		const candidateIDs = actorEvents.flatMap((event) =>
			event.settlement.provider.kind === "speculative" ? [event.settlement.provider.candidateID] : [],
		);
		expect(candidateIDs).toHaveLength(2);
		expect(new Set(candidateIDs).size).toBe(1);
		expect(fixture.events.find((event) => event.type === "task")).toMatchObject({
			timing: { authoritativeToolCount: 1 },
		});
	});

	it("preserves task timing when an already-aborted next turn is skipped", async () => {
		const source: Source = {
			id: "source",
			enabled: () => false,
			propose: () => plan("source", "unused", { path: "other.ts" }),
		};
		const fixture = harness({ source });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await new Promise((resolve) => setTimeout(resolve, 5));
		expect(await fixture.runtime.consume(call("turn-1"))).toBeUndefined();
		await fixture.runtime.actual({ ...call("turn-1"), durationMs: 1, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });

		const aborted = new AbortController();
		aborted.abort();
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" }, aborted.signal);
		await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true });

		expect(fixture.events.filter((event) => event.type === "task")).toHaveLength(1);
		expect(fixture.events.find((event) => event.type === "task")).toMatchObject({
			timing: { authoritativeToolCount: 1, toolExecutionMs: 1 },
		});
	});

	it("expires both pending and admitting next-action requests when the Actor intent arrives", async () => {
		let entered = 0;
		let admissionEntered = false;
		let releaseAdmission!: () => void;
		const admission = new Promise<void>((resolve) => {
			releaseAdmission = resolve;
		});
		const source: Source = {
			id: "source",
			enabled: () => true,
			requestLifetime: "actor_decision",
			proposalCount: () => 2,
			propose: ({ proposalIndex, signal }) => {
				entered++;
				if (proposalIndex === 0) return plan("source", "empty", { path: "other.ts" });
				return new Promise((_, reject) => {
					signal.addEventListener("abort", () => reject(signal.reason), { once: true });
				});
			},
		};
		const fixture = harness({
			source,
			preflight: async () => {
				admissionEntered = true;
				await admission;
				return { ok: true };
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => entered === 2 && admissionEntered);

		expect(await fixture.runtime.consume(call("turn"))).toBeUndefined();
		releaseAdmission();
		await waitFor(() => fixture.runtime.inspect().pendingPredictions === 0);
		await waitFor(() => fixture.events.filter((event) => event.type === "source_request").length === 2);
		expect(fixture.executions()).toBe(0);
		expect(
			fixture.events.filter(
				(event) => event.type === "source_request" && event.request.settlement.status === "aborted",
			),
		).toHaveLength(1);
		await fixture.runtime.actual({ ...call("turn"), durationMs: 1, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
	});

	it("queues unique candidates without letting background work take foreground capacity", async () => {
		let releaseFirst!: () => void;
		const firstGate = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const executed: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => ({
				id: "queue",
				source: "source",
				revision: 0,
				actions: [
					{
						id: "second",
						type: "tool_call",
						tool: "read",
						input: { path: "second.ts" },
						background: true,
						expectedLatencyBenefitMs: 10_000,
					},
					{ id: "first", type: "tool_call", tool: "read", input: { path: "first.ts" } },
				],
			}),
		};
		const fixture = harness({
			source,
			settings: () => ({ ...settings, maxConcurrentActions: 1 }),
			execute: async (_tool, input) => {
				const path = String(input.path);
				executed.push(path);
				if (path === "first.ts") await firstGate;
				return path;
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 2 && executed.length === 1);
		expect(executed).toEqual(["first.ts"]);

		releaseFirst();
		await waitFor(() => executed.length === 2);
		expect(executed).toEqual(["first.ts", "second.ts"]);
		expect(
			fixture.events.filter((event) => event.type === "candidate" && event.state.status === "cancelled"),
		).toEqual([]);
	});

	it("gives resolved causal work capacity without evicting an equal-deadline sibling", async () => {
		let releaseContinuation!: () => void;
		const continuationGate = new Promise<void>((resolve) => {
			releaseContinuation = resolve;
		});
		const executed: string[] = [];
		const aborted: string[] = [];
		let childCompleted = false;
		const read = (id: string, horizon = 0, background = false) => ({
			id,
			type: "tool_call" as const,
			tool: "read",
			input: { path: `${id}.ts` },
			horizon,
			expectedDurationMs: 1,
			background,
		});
		const source: Source = {
			id: "source",
			enabled: () => true,
			continueOn: ["execution_succeeded"],
			propose: () => ({
				id: "contention",
				source: "source",
				revision: 0,
				actions: [read("parent"), read("background", 0, true), read("same", 1), read("later", 2)],
			}),
			continue: async ({ proposalID, actionID, revision, candidate }) => {
				if (candidate.input.path !== "parent.ts") return undefined;
				await continuationGate;
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
							resourceDemand: 2,
							dependsOn: [{ actionID, condition: "execution_succeeded" }],
						},
					],
				};
			},
		};
		const fixture = harness({
			source,
			settings: () => ({ ...settings, maxConcurrentActions: 3 }),
			execute: async (_tool, input, signal) => {
				const path = String(input.path);
				executed.push(path);
				if (path === "parent.ts") return path;
				if (path === "child.ts") {
					await new Promise((resolve) => setTimeout(resolve, 80));
					childCompleted = true;
					return path;
				}
				return new Promise((_, reject) => {
					signal.addEventListener(
						"abort",
						() => {
							aborted.push(path);
							reject(signal.reason);
						},
						{ once: true },
					);
				});
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "contention" });
		await waitFor(() => executed.includes("parent.ts"));
		expect(await fixture.runtime.consume(call("contention", { path: "parent.ts" }))).toBe("parent.ts");
		await waitFor(() => executed.length === 4);
		releaseContinuation();
		await waitFor(() => childCompleted);
		expect(aborted.sort()).toEqual(["background.ts", "later.ts"]);
		expect(aborted).not.toContain("same.ts");

		const startedAt = performance.now();
		expect(await fixture.runtime.consume(call("contention", { path: "child.ts" }))).toBe("child.ts");
		expect(performance.now() - startedAt).toBeLessThan(25);
		expect(executed.filter((path) => path === "child.ts")).toHaveLength(1);
		expect(
			fixture.events.some(
				(event) =>
					event.type === "candidate" &&
					event.state.status === "cancelled" &&
					event.state.cause.code === "scheduler_preempted",
			),
		).toBe(true);
		await fixture.runtime.finishTurn({ ...call("contention"), terminal: true });
	});

	it("preempts only to start queued Actor work and joins running work at its existing capacity", async () => {
		for (const mode of ["queued", "running"] as const) {
			const executed: string[] = [];
			const aborted: string[] = [];
			const source: Source = {
				id: "source",
				enabled: () => true,
				propose: () => ({
					id: `promotion-${mode}`,
					source: "source",
					revision: 0,
					actions: [
						{ id: "busy", type: "tool_call", tool: "read", input: { path: "busy.ts" } },
						{
							id: "target",
							type: "tool_call",
							tool: "read",
							input: { path: "target.ts" },
							resourceDemand: mode === "queued" ? 2 : 1,
						},
					],
				}),
			};
			const fixture = harness({
				source,
				settings: () => ({ ...settings, maxConcurrentActions: mode === "queued" ? 1 : 2 }),
				execute: async (_tool, input, signal) => {
					const path = String(input.path);
					executed.push(path);
					if (path === "target.ts") {
						await new Promise((resolve) => setTimeout(resolve, 80));
						return "target";
					}
					return new Promise((_, reject) => {
						signal.addEventListener(
							"abort",
							() => {
								aborted.push(path);
								reject(signal.reason);
							},
							{ once: true },
						);
					});
				},
			});
			await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
			await waitFor(() => executed.length === (mode === "queued" ? 1 : 2));

			expect(await fixture.runtime.consume(call("turn", { path: "target.ts" }))).toBe("target");
			expect(executed).toEqual(["busy.ts", "target.ts"]);
			expect(aborted).toEqual(mode === "queued" ? ["busy.ts"] : []);
			const preemption = fixture.events.find(
				(event) =>
					event.type === "candidate" &&
					event.state.status === "cancelled" &&
					event.state.cause.code === "preempted_by_actor",
			);
			expect(preemption !== undefined).toBe(mode === "queued");
		}
	});

	it("cannot commit a speculative world when output projection fails", async () => {
		const commit = vi.fn(async () => "committed");
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "projection", { path: "README.md", offset: 1, limit: 100 }),
		};
		const fixture = harness({
			source,
			projection: true,
			execute: () => ({
				output: "wide",
				backend: "test",
				resources: [],
				capturedBytes: 0,
				executionMetrics: {},
				compatibility: { status: "compatible", backend: "test", executionFingerprint: "" },
				state: "sealed",
				commit,
				dispose: () => {},
			}),
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		expect(await fixture.runtime.consume(call("turn", { path: "README.md", offset: 10, limit: 10 }))).toBeUndefined();
		expect(commit).not.toHaveBeenCalled();
	});

	it("ignores a covering action that cannot prove K(a) containment", async () => {
		const executed: Record<string, unknown>[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "unsafe-cover", { path: "README.md", offset: 2, limit: 2 }),
		};
		const fixture = harness({
			source,
			projection: true,
			coveringAction: () => buildPiActionKey("read", { path: "other.ts" }, "/workspace"),
			execute: (_tool, input) => {
				executed.push({ ...input });
				return "output";
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "unsafe-cover" });
		await waitFor(() => executed.length === 1);

		expect(executed[0]).toMatchObject({ path: expect.stringMatching(/README\.md$/), offset: 2, limit: 2 });
	});

	it("settles a K(a) match as incompatible without committing backend effects", async () => {
		const commit = vi.fn(async () => "committed");
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "incompatible", { path: "README.md" }),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
			execute: () => ({
				output: "sealed",
				backend: "test",
				resources: [],
				capturedBytes: 0,
				executionMetrics: {},
				compatibility: { status: "indeterminate", backend: "test", code: "attestation_missing" },
				state: "sealed",
				commit,
				dispose: () => {},
			}),
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		expect(await fixture.runtime.consume(call("turn"))).toBeUndefined();
		await fixture.runtime.actual({ ...call("turn"), durationMs: 1, output: "actor" });
		await fixture.runtime.finishTurn({ ...call("turn"), terminal: true });
		expect(commit).not.toHaveBeenCalled();
		expect(settlements).toEqual([
			expect.objectContaining({
				match: expect.objectContaining({
					matched: true,
					adoption: expect.objectContaining({
						status: "rejected",
						cause: expect.objectContaining({ stage: "compatibility", code: "backend_indeterminate" }),
					}),
					relation: expect.any(Object),
				}),
			}),
		]);
	});

	it("drops a producer result that arrives after its timeout", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const source: Source = {
			id: "source",
			enabled: () => true,
			timeoutMs: () => 1,
			propose: async () => {
				await gate;
				return plan("source", "late", { path: "README.md" });
			},
		};
		const fixture = harness({ source });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn" });
		await waitFor(() =>
			fixture.events.some(
				(event) => event.type === "source_request" && event.request.settlement.status === "timeout",
			),
		);
		release();
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(fixture.runtime.inspect().sharedCandidates).toBe(0);
		expect(fixture.executions()).toBe(0);
	});

	it("keeps one turn on its settings snapshot while master disable remains immediate", async () => {
		let configured = settings;
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "epoch", { path: "README.md" }),
		};
		const fixture = harness({ source, settings: () => configured });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);

		configured = { ...settings, tools: settings.tools.filter((tool) => tool !== "read") };
		await fixture.runtime.settingsChanged(configured);
		expect(await fixture.runtime.consume(call("turn-1"))).toBe("speculative");
		await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
		expect(await fixture.runtime.consume(call("turn-2"))).toBeUndefined();

		configured = { ...settings, enabled: false };
		await fixture.runtime.settingsChanged(configured);
		expect(fixture.runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 0 });
	});

	it("rearms invalidated work while retaining a prediction until its latest horizon", async () => {
		let executions = 0;
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) =>
				startInput.turnID === "turn-1"
					? {
							...plan("source", "future", { path: "future.ts" }),
							actions: [
								{
									id: "next",
									type: "tool_call",
									tool: "read",
									input: { path: "future.ts" },
									horizon: 0,
									latestHorizon: 1,
									expectedDurationMs: 10,
								},
							],
						}
					: plan("source", `empty:${startInput.turnID}`, {}),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
			execute: () => `future:${++executions}`,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await waitFor(() => executions === 1);

		const mutation: Call = {
			sessionID: "session",
			turnID: "turn-1",
			id: "mutation",
			tool: "write",
			input: { path: "future.ts", content: "new" },
		};
		expect(await fixture.runtime.consume(mutation)).toBeUndefined();
		await fixture.runtime.actual({ ...mutation, durationMs: 1, output: "written" });
		await waitFor(() => executions === 2);
		expect(settlements).toEqual([]);
		await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
		expect(
			await fixture.runtime.consume({
				sessionID: "session",
				turnID: "turn-2",
				id: "future-call",
				tool: "read",
				input: { path: "future.ts" },
			}),
		).toBe("future:2");
		await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true });
		expect(settlements).toContainEqual(
			expect.objectContaining({
				match: {
					matched: true,
					adoption: { status: "adopted", candidateID: expect.any(String) },
					relation: expect.any(Object),
				},
			}),
		);
	});

	it("advances prediction horizons once per Actor tool batch", async () => {
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) =>
				startInput.turnID === "turn-1"
					? {
							id: "batch",
							source: "source",
							revision: 0,
							actions: [
								{ id: "same", type: "tool_call", tool: "read", input: { path: "same.ts" }, horizon: 0 },
								{ id: "next", type: "tool_call", tool: "read", input: { path: "next.ts" }, horizon: 1 },
							],
						}
					: { id: `empty:${startInput.turnID}`, source: "source", revision: 0, actions: [] },
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({ source, execute: (_tool, input) => String(input.path) });
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-1" });
		await waitFor(() => fixture.executions() >= 1);

		const unrelated = { ...call("turn-1"), id: "unrelated", tool: "find", input: { pattern: "*" } };
		expect(await fixture.runtime.consume(unrelated)).toBeUndefined();
		await fixture.runtime.actual({ ...unrelated, durationMs: 1, output: "files" });
		expect(await fixture.runtime.consume({ ...call("turn-1", { path: "same.ts" }), id: "same" })).toBe("same.ts");
		await fixture.runtime.finishTurn({ ...call("turn-1"), terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-2" });
		await waitFor(() => fixture.executions() === 2);
		expect(await fixture.runtime.consume({ ...call("turn-2", { path: "next.ts" }), id: "next" })).toBe("next.ts");
		await fixture.runtime.finishTurn({ ...call("turn-2"), terminal: true });

		expect(settlements).toHaveLength(2);
		expect(
			settlements.map((settlement) =>
				settlement.observation === "observed" ? settlement.actorAction.decisionSequence : undefined,
			),
		).toEqual([1, 2]);
	});

	it("retains a fresh completed result after its prediction is settled", async () => {
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: ({ startInput }) =>
				startInput.turnID === "turn-1"
					? {
							...plan("source", "future", { path: "future.ts" }),
							actions: [
								{
									id: "next",
									type: "tool_call",
									tool: "read",
									input: { path: "future.ts" },
									horizon: 0,
								},
							],
						}
					: { id: `empty:${startInput.turnID}`, source: "source", revision: 0, actions: [] },
		};
		const fixture = harness({ source, execute: () => "future" });
		for (let index = 1; index <= 12; index++) {
			const turnID = `turn-${index}`;
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			if (index === 1) await waitFor(() => fixture.runtime.inspect().sharedCandidates === 1);
			const unrelated = { ...call(turnID, { path: `other-${index}.ts` }), id: `other-${index}` };
			expect(await fixture.runtime.consume(unrelated)).toBeUndefined();
			await fixture.runtime.actual({ ...unrelated, durationMs: 1, output: "other" });
			await fixture.runtime.finishTurn({ ...unrelated, terminal: false });
		}
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-13" });
		expect(await fixture.runtime.consume(call("turn-13", { path: "future.ts" }))).toBe("future");
		await fixture.runtime.finishTurn({ ...call("turn-13"), terminal: true });
	});

	it("keeps future launch deadlines phase-correct after an Actor action arrives", async () => {
		let executions = 0;
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: async ({ startInput }) => {
				if (startInput.turnID !== "turn-3") return { id: "empty", source: "source", revision: 0, actions: [] };
				await new Promise((resolve) => setTimeout(resolve, 50));
				return {
					...plan("source", "distant", { path: "future.ts" }),
					actions: [
						{
							id: "next",
							type: "tool_call",
							tool: "read",
							input: { path: "future.ts" },
							horizon: 2,
							expectedDurationMs: 10,
						},
					],
				};
			},
		};
		const fixture = harness({
			source,
			execute: () => {
				executions++;
				return "future";
			},
		});
		const actorTurn = async (turnID: string) => {
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			await new Promise((resolve) => setTimeout(resolve, 20));
			const actorCall: Call = {
				sessionID: "session",
				turnID,
				id: turnID,
				tool: "find",
				input: { pattern: "*" },
			};
			expect(await fixture.runtime.consume(actorCall)).toBeUndefined();
			await new Promise((resolve) => setTimeout(resolve, 60));
			await fixture.runtime.actual({ ...actorCall, durationMs: 60, output: "files" });
			await fixture.runtime.finishTurn({ ...actorCall, terminal: false });
		};
		await actorTurn("turn-1");
		await actorTurn("turn-2");

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "turn-3" });
		await waitFor(() => fixture.runtime.inspect().deferredPlanActions === 1);
		await new Promise((resolve) => setTimeout(resolve, 10));
		const third = { ...call("turn-3"), id: "turn-3", tool: "find", input: { pattern: "*" } };
		expect(await fixture.runtime.consume(third)).toBeUndefined();
		await fixture.runtime.actual({ ...third, durationMs: 0, output: "files" });
		await fixture.runtime.finishTurn({ ...third, terminal: false });
		await new Promise((resolve) => setTimeout(resolve, 100));
		expect(executions).toBe(0);
		await waitFor(() => executions === 1);
		await fixture.runtime.finishTurn({ ...call("turn-3"), terminal: true });
	});

	it("shares only an unchanged K(a) computation still in flight from Actor preview", async () => {
		for (const [label, formalPath, settlePreview, callsBeforeRelease] of [
			["unchanged", "preview.ts", false, 1],
			["changed", "formal.ts", false, 2],
			["already-settled", "preview.ts", true, 2],
		] as const) {
			let release!: () => void;
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			let actionKeys = 0;
			const fixture = harness({
				source: { id: "disabled", enabled: () => false, propose: () => undefined },
				actionKey: async (tool, input) => {
					actionKeys++;
					if (actionKeys === 1) await gate;
					return buildPiActionKey(tool, input, "/workspace");
				},
			});
			const turnID = `in-flight-key:${label}`;
			await fixture.runtime.startTurn({ sessionID: "session", turnID });
			const previewCall = call(turnID, { path: "preview.ts" });
			const preview = fixture.runtime.previewActorCall(previewCall);
			await waitFor(() => actionKeys === 1);
			if (settlePreview) {
				release();
				await preview;
			}
			const actorCall = { ...previewCall, input: { path: formalPath } };
			const consumed = fixture.runtime.consume(actorCall);
			if (callsBeforeRelease === 1) await new Promise((resolve) => setTimeout(resolve, 10));
			else await waitFor(() => actionKeys === callsBeforeRelease);
			expect(actionKeys, label).toBe(callsBeforeRelease);
			if (!settlePreview) release();
			await preview;
			if ((await consumed) === undefined)
				await fixture.runtime.actual({ ...actorCall, durationMs: 1, output: "actor" });
			await fixture.runtime.finishTurn({ ...actorCall, terminal: true });
		}
	});

	it("promotes a streamed Actor intent without claiming or committing its prediction", async () => {
		const settlements: PredictionSettlement[] = [];
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
			execute: () => "future",
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "streaming-intent" });
		await waitFor(() => fixture.runtime.inspect().deferredPlanActions === 1);
		await new Promise((resolve) => setTimeout(resolve, 25));
		expect(fixture.executions()).toBe(0);

		const actorCall = call("streaming-intent", { path: "future.ts" });
		await fixture.runtime.previewActorCall(actorCall);
		await waitFor(() => fixture.executions() === 1, 80);
		await waitFor(() =>
			fixture.events.some((event) => event.type === "candidate" && event.state.status === "succeeded"),
		);
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
		let releaseSlow!: () => void;
		const slow = new Promise<void>((resolve) => {
			releaseSlow = resolve;
		});
		const fixture = harness({
			source: { id: "disabled", enabled: () => false, propose: () => undefined },
			execute: (tool, input) =>
				input.content === "slow"
					? slow.then(() =>
							world(`${tool}:${String(input.path)}`, {
								checkpoint: { backend: "test", id: "slow", lineage: "slow", depth: 0 },
								resources: ["."],
								onCommit: () => committed++,
								onDispose: () => disposed++,
							}),
						)
					: world(`${tool}:${String(input.path)}`, {
							checkpoint: { backend: "test", id: "preview", lineage: "preview", depth: 0 },
							resources: ["."],
							onCommit: () => committed++,
							onDispose: () => disposed++,
						}),
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
		await waitFor(() => fixture.executions() === 1);
		await fixture.runtime.previewActorCall({
			...writeCall,
			id: "bash-preview",
			tool: "bash",
			input: { command: "echo preview" },
		});
		await new Promise((resolve) => setTimeout(resolve, 10));
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
		await waitFor(() => fixture.executions() === 2);
		const consumed = fixture.runtime.consume(slowCall);
		await new Promise((resolve) => setTimeout(resolve, 10));
		releaseSlow();
		expect(await consumed).toBe("write:slow.txt");
		expect(fixture.executions()).toBe(2);
		expect(committed).toBe(1);
		await fixture.runtime.finishTurn({ ...slowCall, terminal: true });
	});

	it("uses streamed tool identity only to order complete queued predictions", async () => {
		let releaseBusy!: () => void;
		const busy = new Promise<void>((resolve) => {
			releaseBusy = resolve;
		});
		const started: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => ({
				id: "tool-hint",
				source: "source",
				revision: 0,
				actions: [
					{ id: "busy", type: "tool_call" as const, tool: "bash", input: { command: "busy" } },
					{ id: "wrong", type: "tool_call" as const, tool: "bash", input: { command: "wrong" } },
					{ id: "target", type: "tool_call" as const, tool: "read", input: { path: "target.ts" } },
					{ id: "wrong-read", type: "tool_call" as const, tool: "read", input: { path: "wrong.ts" } },
				],
			}),
		};
		const fixture = harness({
			source,
			settings: () => ({ ...settings, maxConcurrentActions: 1 }),
			resolveExecution: () => RESOURCE_ROUTE,
			execute: async (_tool, input) => {
				const action = String(input.path ?? input.command);
				started.push(action);
				if (input.command === "busy") await busy;
				return action;
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "tool-hint" });
		await waitFor(() => started.length === 1);
		await waitFor(() => fixture.runtime.inspect().sharedCandidates === 4);
		expect(started).toEqual(["busy"]);

		await fixture.runtime.previewActorTool({ sessionID: "session", turnID: "tool-hint", tool: "read" });
		releaseBusy();
		await waitFor(() => started.length >= 2);
		expect(started.slice(0, 2)).toEqual(["busy", "target.ts"]);
		expect(started).not.toContain("wrong");
		expect(fixture.events.some((event) => event.type === "actor_action")).toBe(false);

		const actorCall = call("tool-hint", { path: "target.ts" });
		expect(await fixture.runtime.consume(actorCall)).toBe("target.ts");
		await fixture.runtime.finishTurn({ ...actorCall, terminal: true });
	});

	it("records an exact match when isolation is unavailable without starting speculative execution", async () => {
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => ({
				id: "bash",
				source: "source",
				revision: 0,
				actions: [{ id: "next", type: "tool_call", tool: "bash", input: { command: "build" } }],
			}),
			onSettled: ({ settlement }) => {
				settlements.push(settlement);
			},
		};
		const fixture = harness({
			source,
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "parallel" });
		await waitFor(() => fixture.events.some((event) => event.type === "source_request"));
		expect(fixture.runtime.inspect()).toMatchObject({ exclusiveCandidates: 0, sharedCandidates: 0 });
		expect(fixture.executions()).toBe(0);
		const firstCall: Call = {
			sessionID: "session",
			turnID: "parallel",
			id: "first",
			tool: "bash",
			input: { command: "build" },
		};
		expect(await fixture.runtime.consume(firstCall)).toBeUndefined();
		expect(fixture.executions()).toBe(0);
		await fixture.runtime.actual({ ...firstCall, durationMs: 2, output: "actor-built" });
		await fixture.runtime.finishTurn({ ...firstCall, terminal: true });
		expect(settlements).toHaveLength(1);
		expect(settlements[0]).toMatchObject({
			actorAction: { id: "first" },
			match: {
				matched: true,
				adoption: { status: "rejected", cause: { stage: "execution", code: "isolation_unavailable" } },
			},
		});
		expect(fixture.events.some((event) => event.type === "candidate" && event.candidate.tool === "bash")).toBe(false);
		const actorEvent = fixture.events.find((event) => event.type === "actor_action");
		expect(actorEvent?.type === "actor_action" && actorEvent.settlement.provider.kind === "actor").toBe(true);
		if (
			actorEvent?.type === "actor_action" &&
			actorEvent.settlement.provider.kind === "actor" &&
			actorEvent.settlement.provider.origin === "fallback"
		) {
			const timing = actorEvent.settlement.provider.executionBlockedTiming;
			expect(timing).toBeDefined();
			expect(timing?.executionAheadMs).toBeLessThanOrEqual(2);
			expect((timing?.executionAheadMs ?? 0) + (timing?.hitLatencyMs ?? 0)).toBe(2);
		}
	});

	it("retains a queued confirmation continuation after an empty speculative continuation", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const triggers: string[] = [];
		const executed: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			propose: () => plan("source", "parent", { path: "parent.ts" }),
			continue: async ({ proposalID, revision, trigger }) => {
				triggers.push(trigger);
				if (trigger === "execution_succeeded") {
					await gate;
					return undefined;
				}
				return {
					proposalID,
					source: "source",
					revision,
					upsert: [{ id: "confirmed-child", type: "tool_call", tool: "read", input: { path: "child.ts" } }],
				};
			},
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "queued-confirmation" });
		await waitFor(() => triggers.includes("execution_succeeded"));
		expect(await fixture.runtime.consume(call("queued-confirmation", { path: "parent.ts" }))).toBe(
			"parent.ts:output",
		);
		release();
		await waitFor(() => triggers.includes("actor_adopted"));
		await waitFor(() => executed.includes("child.ts"));
		await fixture.runtime.finishTurn({ ...call("queued-confirmation"), terminal: true });
	});

	it("keeps a future continuation on the Actor runway after its dependency resolves", async () => {
		const started = new Map<string, number>();
		const source: Source = {
			id: "source",
			enabled: () => true,
			continueOn: ["execution_succeeded"],
			propose: () => plan("source", "timed-continuation", { path: "parent.ts" }),
			continue: ({ proposalID, actionID, revision }) => ({
				proposalID,
				source: "source",
				revision,
				upsert: [
					{
						id: "future-child",
						type: "tool_call",
						tool: "read",
						input: { path: "child.ts" },
						horizon: 2,
						expectedDurationMs: 5,
						dependsOn: [{ actionID, condition: "execution_succeeded" }],
					},
				],
			}),
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				started.set(String(input.path), performance.now());
				return "output";
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "timed-continuation" });
		await waitFor(() => started.has("child.ts"), 500);
		expect(started.get("child.ts")! - started.get("parent.ts")!).toBeGreaterThan(75);
		await fixture.runtime.finishTurn({ ...call("timed-continuation"), terminal: true });
	});

	it("keeps a next-decision continuation alive across parallel tools in one Actor decision", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let continuationStarted = false;
		const executed: string[] = [];
		const settlements: PredictionSettlement[] = [];
		const source: Source = {
			id: "source",
			enabled: () => true,
			requestLifetime: "actor_decision",
			continueOn: ["actor_adopted"],
			propose: () => plan("source", "parallel-continuation", { path: "parent.ts" }),
			continue: async ({ proposalID, revision, trigger }) => {
				if (trigger !== "actor_adopted") return undefined;
				continuationStarted = true;
				await gate;
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
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
		});
		await fixture.runtime.startTurn({ sessionID: "session", turnID: "parallel-continuation" });
		await waitFor(() => executed.includes("parent.ts"));

		const parent = {
			sessionID: "session",
			turnID: "parallel-continuation",
			id: "parent-call",
			tool: "read",
			input: { path: "parent.ts" },
		};
		expect(await fixture.runtime.consume(parent)).toBe("parent.ts:output");
		await waitFor(() => continuationStarted);

		const sibling = { ...parent, id: "sibling-call", input: { path: "sibling.ts" } };
		expect(await fixture.runtime.consume(sibling)).toBeUndefined();
		await fixture.runtime.actual({ ...sibling, durationMs: 1, output: "actor" });
		expect(
			fixture.events.some(
				(event) =>
					event.type === "source_request" &&
					event.request.request.kind === "continuation" &&
					event.request.settlement.status === "aborted",
			),
		).toBe(false);

		release();
		await waitFor(() => executed.includes("child.ts"));
		const sameBatchChild = { ...parent, id: "same-batch-child", input: { path: "child.ts" } };
		expect(await fixture.runtime.consume(sameBatchChild)).toBe("child.ts:output");
		await fixture.runtime.finishTurn({ ...parent, terminal: false });
		expect(
			settlements.map((settlement) =>
				settlement.observation === "observed" ? settlement.actorAction.decisionSequence : undefined,
			),
		).toEqual([1]);

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

	it("keeps a bounded continuation alive across turns without extending turn completion", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let continuationStarted = false;
		let proposals = 0;
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
				if (String(candidate.input.path) !== "parent.ts" || trigger !== "execution_succeeded") return undefined;
				continuationStarted = true;
				await gate;
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
							dependsOn: [{ actionID, condition: "execution_succeeded" }],
						},
					],
				};
			},
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "parent-turn" });
		await waitFor(() => continuationStarted);
		expect(await fixture.runtime.consume(call("parent-turn", { path: "parent.ts" }))).toBe("parent.ts:output");
		const finished = fixture.runtime.finishTurn({ ...call("parent-turn"), terminal: false });
		expect(
			await Promise.race([
				finished.then(() => true),
				new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 50)),
			]),
		).toBe(true);
		expect(fixture.runtime.inspect()).toMatchObject({ activeTurns: 0, pendingPredictions: 1 });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "child-turn" });
		expect(proposals).toBe(1);
		release();
		await waitFor(() => executed.includes("child.ts"));
		expect(await fixture.runtime.consume(call("child-turn", { path: "child.ts" }))).toBe("child.ts:output");
		await fixture.runtime.finishTurn({ ...call("child-turn"), terminal: true });
	});

	it("adopts a target-state-valid child after its parent prediction misses", async () => {
		let enabled = true;
		const executed: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => enabled,
			propose: () => plan("source", "conditional", { path: "parent.ts" }),
			continue: async ({ proposalID, actionID, revision, trigger }) => {
				if (trigger !== "execution_succeeded") return undefined;
				return {
					proposalID,
					source: "source",
					revision,
					upsert: [
						{
							id: "late-child",
							type: "tool_call",
							tool: "read",
							input: { path: "late.ts" },
							dependsOn: [{ actionID, condition: "execution_succeeded" }],
						},
					],
				};
			},
		};
		const fixture = harness({
			source,
			execute: (_tool, input) => {
				executed.push(String(input.path));
				return `${String(input.path)}:output`;
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "miss" });
		await waitFor(() => executed.includes("late.ts"));
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
	});

	it("keeps equal child actions isolated by parent world and rebases the adopted lineage", async () => {
		let enabled = true;
		let workspaceVersion = 0;
		const executed: string[] = [];
		const childParents: string[] = [];
		const source: Source = {
			id: "source",
			enabled: () => enabled,
			proposalCount: () => 2,
			continueOn: ["execution_succeeded"],
			propose: ({ proposalIndex }) => ({
				id: `chain:${proposalIndex}`,
				source: "source",
				revision: 0,
				actions: [
					{
						id: "parent",
						type: "tool_call",
						tool: "write",
						input: { path: `parent-${proposalIndex}.txt`, content: `parent-${proposalIndex}` },
					},
				],
			}),
			continue: ({ proposalID, actionID, revision, candidate }) => {
				if (String(candidate.input.content).startsWith("child")) return undefined;
				return {
					proposalID,
					source: "source",
					revision,
					upsert: [
						{
							id: "child",
							type: "tool_call",
							tool: "write",
							input: { path: "child.txt", content: "child" },
							dependsOn: [{ actionID, condition: "execution_succeeded" }],
						},
					],
				};
			},
		};
		const fixture = harness({
			source,
			execute: (_tool, input, _signal, parentWorld) => {
				const content = String(input.content);
				executed.push(content);
				if (content === "child") childParents.push(String(parentWorld?.output));
				const output = content === "child" ? `child:${parentWorld?.output}` : content;
				const parentCheckpoint = parentWorld?.checkpoint;
				return world(output, {
					checkpoint: {
						backend: "test",
						id: output,
						lineage: parentCheckpoint?.lineage ?? output,
						depth: (parentCheckpoint?.depth ?? -1) + 1,
					},
					resources: ["."],
					onCommit: () => workspaceVersion++,
				});
			},
		});

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "parent" });
		await waitFor(() => childParents.length === 2);
		expect(executed.sort()).toEqual(["child", "child", "parent-0", "parent-1"]);
		expect(childParents.sort()).toEqual(["parent-0", "parent-1"]);

		const parentCall: Call = {
			sessionID: "session",
			turnID: "parent",
			id: "actor-parent",
			tool: "write",
			input: { path: "parent-0.txt", content: "parent-0" },
		};
		expect(await fixture.runtime.consume(parentCall)).toBe("parent-0");
		enabled = false;
		await fixture.runtime.finishTurn({ ...parentCall, terminal: false });

		await fixture.runtime.startTurn({ sessionID: "session", turnID: "child" });
		const childCall: Call = {
			sessionID: "session",
			turnID: "child",
			id: "actor-child",
			tool: "write",
			input: { path: "child.txt", content: "child" },
		};
		expect(await fixture.runtime.consume(childCall)).toBe("child:parent-0");
		expect(workspaceVersion).toBe(2);
		await fixture.runtime.finishTurn({ ...childCall, terminal: true });
	});
});

function world(
	output: string,
	options: {
		readonly checkpoint?: WorldCheckpoint;
		readonly resources?: readonly string[];
		readonly onCommit?: () => void;
		readonly onDispose?: () => void;
		readonly executionFingerprint?: string;
		readonly validate?: () => Promise<ResourceValidation>;
	} = {},
): WorldBranch<string> {
	let state = "sealed" as "sealed" | "committing" | "committed" | "failed";
	return {
		output,
		backend: "test",
		...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
		resources: options.resources ?? [],
		capturedBytes: 0,
		executionMetrics: {},
		compatibility: {
			status: "compatible" as const,
			backend: "test",
			executionFingerprint: options.executionFingerprint ?? "",
		},
		...(options.validate ? { validate: options.validate } : {}),
		get state() {
			return state;
		},
		commit: async () => {
			state = "committing";
			options.onCommit?.();
			state = "committed";
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

async function waitFor(check: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!check()) {
		if (Date.now() >= deadline) throw new Error("condition was not met");
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
}
