import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, Model } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KEYABLE_TOOLS, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { PI_BASH_TAIL_LINES_PROJECTION_RULE } from "../src/pi-bash-projection.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { MaterializedSpeculativeCandidate, SpeculativeActionEvent } from "../src/runtime.ts";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
import type { ToolSettlement } from "../src/tool-settlement.ts";
import {
	normalizeSelfSpeculationSettings,
	SELF_SPECULATION_DEFAULTS,
	SelfSpeculationCoordinator,
} from "../src/self-speculation.ts";

const roots: string[] = [];
const readSchema = Type.Object({
	path: Type.String(),
	offset: Type.Optional(Type.Number()),
	limit: Type.Optional(Type.Number()),
});
const grepSchema = Type.Object({ pattern: Type.String(), path: Type.Optional(Type.String()) });
const bashSchema = Type.Object({ command: Type.String(), timeout: Type.Optional(Type.Number()) });
const mockToolSchema = Type.Any();
const mockToolCalls = [
	["read", { path: "notes.txt" }],
	["grep", { pattern: "one", path: "." }],
	["find", { pattern: "*.txt", path: "." }],
	["ls", { path: "." }],
	["bash", { command: "printf ready" }],
	["write", { path: "generated.txt", content: "ready" }],
	["edit", { path: "notes.txt", edits: [{ oldText: "one", newText: "ready" }] }],
] as const;

function model(id = "actor"): Model<"openai-responses"> {
	return {
		id,
		name: id,
		api: "openai-responses",
		provider: "openai",
		baseUrl: "https://example.invalid",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 8192,
		maxTokens: 2048,
	};
}

function assistant(content: AssistantMessage["content"], stopReason: AssistantMessage["stopReason"]): AssistantMessage {
	return {
		role: "assistant",
		content,
		api: "openai-responses",
		provider: "openai",
		model: "mock",
		usage: {
			input: 1,
			output: 1,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 2,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason,
		timestamp: Date.now(),
	};
}

function drafterCall(input: Record<string, unknown>): AssistantMessage {
	return assistant([{ type: "toolCall", id: "draft-1", name: "read", arguments: input }], "toolUse");
}

function settings(candidateLimit = 1) {
	return {
		enabled: true,
		drafterEnabled: true,
		candidateLimit,
		maxConcurrentActions: candidateLimit,
		tools: ["read"],
		patternAware: { enabled: false },
	};
}

function startInput(tool: AgentTool, turnID = "turn-1") {
	return {
		turnID,
		actorModel: model("actor"),
		context: { systemPrompt: "system", messages: [], tools: [tool] },
		actorOptions: undefined,
		tools: [tool],
	};
}

async function temporaryWorkspace(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-spec-host-"));
	roots.push(root);
	await writeFile(path.join(root, "notes.txt"), "one\ntwo\nthree\nfour", "utf8");
	return root;
}

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("speculative action host", () => {
	it("reuses running and completed results for every keyable Pi tool", async () => {
		expect(mockToolCalls.map(([tool]) => tool)).toEqual(KEYABLE_TOOLS);
		for (const phase of ["running", "completed"] as const) {
			for (const [toolName, args] of mockToolCalls) {
				const cwd = await temporaryWorkspace();
				const turnID = `${phase}-${toolName}`;
				const expected = `${phase}:${toolName}`;
				let release!: () => void;
				const gate = new Promise<void>((resolve) => {
					release = resolve;
				});
				const speculativeExecution = vi.fn(async () => {
					await gate;
					return { content: [{ type: "text" as const, text: expected }], details: {} };
				});
				const actorExecution = vi.fn(async () => speculativeExecution());
				const tool: AgentTool<typeof mockToolSchema> = {
					name: toolName,
					label: toolName,
					description: toolName,
					parameters: mockToolSchema,
					execute: speculativeExecution,
				};
				const events: SpeculativeActionEvent<string>[] = [];
				const sandbox = mockRuntimeWorld(async (context) => ({
					result: await context.tool.execute(context.callID, context.args as never, context.signal),
					isError: false,
				}));
				const host = createSpeculativeActionHost(`session-${turnID}`, {
					cwd,
					getSettings: () => ({ ...settings(), tools: [toolName] }),
					draftModel: model("draft"),
					complete: async () =>
						assistant([{ type: "toolCall", id: `draft-${toolName}`, name: toolName, arguments: args }], "toolUse"),
					preflight: () => true,
					executionWorlds: [sandbox],
					onEvent: (event) => {
						events.push(event);
					},
				});
				try {
					await host.startTurn(startInput(tool, turnID));
					await waitFor(() => speculativeExecution.mock.calls.length === 1);
					if (phase === "completed") {
						release();
						await waitFor(() => events.some((event) => event.type === "candidate" && event.state.status === "succeeded"));
					}
					let settled = false;
					const result = host.execute(
						{ turnID, id: `actor-${toolName}`, tool: toolName, args, tools: [tool] },
						undefined,
						actorExecution,
					).then((value) => {
						settled = true;
						return value;
					});
					if (phase === "running") {
						await new Promise<void>((resolve) => setImmediate(resolve));
						expect(settled, `${toolName} should join its running candidate`).toBe(false);
						expect(actorExecution, `${toolName} should not start Actor fallback`).not.toHaveBeenCalled();
						release();
					}
					expect((await result).content).toEqual([{ type: "text", text: expected }]);
					expect(speculativeExecution).toHaveBeenCalledOnce();
					expect(actorExecution).not.toHaveBeenCalled();
					await waitFor(() =>
						events.some((event) => event.type === "actor_action" && event.settlement.provider.kind === "speculative"),
					);
					expect(events.find((event) => event.type === "actor_action")).toMatchObject({
						settlement: { provider: { kind: "speculative", match: { kind: "exact" } } },
					});
					expect(events.find((event) => event.type === "candidate" && event.state.status === "succeeded")).toMatchObject({
						candidate: { route: { reuse: PI_ACTION_SEMANTICS.effect(toolName) === "observation" ? "shared_result" : "exclusive_branch" } },
					});
					await host.finishTurn(turnID, true);
				} finally {
					release();
					await host.dispose();
				}
			}
		}
	});

	it("reuses the Actor's own read across turns and invalidates it on resource change", async () => {
		const cwd = await temporaryWorkspace();
		let executions = 0;
		const events: SpeculativeActionEvent<string>[] = [];
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => {
				executions++;
				const text = await readFile(path.join(cwd, input.path), "utf8");
				return { content: [{ type: "text", text }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({
				enabled: true,
				drafterEnabled: false,
				candidateLimit: 1,
				maxConcurrentActions: 1,
				tools: ["read"],
				patternAware: { enabled: false },
				selfSpeculation: { enabled: false },
			}),
			complete: async () => assistant([], "stop"),
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		try {
			await host.startTurn(startInput(tool, "actor-cache-1"));
			const firstCall = {
				turnID: "actor-cache-1",
				id: "actor-cache-call-1",
				tool: "read",
				args: { path: "notes.txt" },
				tools: [tool],
			};
			expect(await host.consume(firstCall)).toBeUndefined();
			const firstResult = await tool.execute(firstCall.id, firstCall.args, undefined);
			await host.actual({ ...firstCall, durationMs: 2, output: { result: firstResult, isError: false } });
			expect(executions).toBe(1);
			expect(host.runtime.inspect("session").sharedCandidates).toBe(1);
			await host.finishTurn(firstCall.turnID);

			await host.startTurn(startInput(tool, "actor-cache-2"));
			const secondCall = { ...firstCall, turnID: "actor-cache-2", id: "actor-cache-call-2" };
			await host.previewActorCall(secondCall);
			expect(executions).toBe(1);
			const cached = await host.consume(secondCall);
			expect(cached, JSON.stringify(events, undefined, 2)).toBeDefined();
			expect(cached?.result.content).toEqual([{ type: "text", text: "one\ntwo\nthree\nfour" }]);
			expect(executions).toBe(1);
			await host.finishTurn(secondCall.turnID);

			await writeFile(path.join(cwd, "notes.txt"), "changed", "utf8");
			await host.startTurn(startInput(tool, "actor-cache-3"));
			const thirdCall = { ...firstCall, turnID: "actor-cache-3", id: "actor-cache-call-3" };
			expect(await host.consume(thirdCall)).toBeUndefined();
			const thirdResult = await tool.execute(thirdCall.id, thirdCall.args, undefined);
			await host.actual({ ...thirdCall, durationMs: 2, output: { result: thirdResult, isError: false } });
			expect(executions).toBe(2);
			await host.finishTurn(thirdCall.turnID, true);
		} finally {
			await host.dispose();
		}
	});

	it("matches Bash intent but leaves the only process execution to the Actor", async () => {
		const cwd = await temporaryWorkspace();
		let executions = 0;
		let preflights = 0;
		const events: SpeculativeActionEvent<string>[] = [];
		const materialized: MaterializedSpeculativeCandidate<string>[] = [];
		const turnStarts: Array<{ turnID: string; decisionSequence: number }> = [];
		const tool: AgentTool<typeof bashSchema> = {
			name: "bash",
			label: "bash",
			description: "bash",
			parameters: bashSchema,
			execute: async () => {
				executions++;
				return { content: [{ type: "text", text: "built" }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({
				...settings(),
				tools: ["bash"],
			}),
			draftModel: model("draft"),
			complete: async () =>
				assistant(
					[{ type: "toolCall", id: "draft-bash", name: "bash", arguments: { command: "npm test" } }],
					"toolUse",
				),
			preflight: () => {
				preflights++;
				return true;
			},
			onTurnStarted: ({ turnID, decisionSequence }) => {
				turnStarts.push({ turnID, decisionSequence });
			},
			onCandidateMaterialized: (candidate) => {
				materialized.push(candidate);
			},
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => host.runtime.inspect("session").executionBlockedPlanActions === 1);
		await waitFor(() => materialized.length === 1);
		expect(turnStarts).toEqual([{ turnID: "turn-1", decisionSequence: 1 }]);
		expect(host.runtime.inspect("session")).toMatchObject({
			exclusiveCandidates: 0,
			sharedCandidates: 0,
		});
		expect(executions).toBe(0);
		expect(preflights).toBe(0);
		expect(events.some((event) => event.type === "candidate")).toBe(false);
		expect(materialized).toMatchObject([
			{
				sessionID: "session",
				turnID: "turn-1",
				expectedDecisionSequence: 1,
				latestDecisionSequence: 1,
				source: "drafter",
				tool: "bash",
				input: { command: "npm test" },
				predictedAction: { tool: "bash", input: { command: "npm test" } },
				executionAction: { tool: "bash", input: { command: "npm test" } },
			},
		]);

		const call = { turnID: "turn-1", id: "actor-bash", tool: "bash", args: { command: "npm test" }, tools: [tool] };
		expect(await host.consume(call)).toBeUndefined();
		expect(executions).toBe(0);
		const result = await tool.execute("actor-bash", { command: "npm test" });
		await host.actual({ ...call, durationMs: 5, output: { result, isError: false } });
		await host.finishTurn("turn-1", true);

		await waitFor(() => events.some((event) => event.type === "prediction"));
		expect(executions).toBe(1);
		expect(events.find((event) => event.type === "prediction")).toMatchObject({
			settlement: {
				observation: "observed",
				match: {
					matched: true,
					adoption: { status: "rejected", cause: { stage: "execution", code: "isolation_unavailable" } },
				},
			},
		});
		await host.dispose();
	});

	it("adopts an in-flight projected Bash view from a runtime-wide sandbox", async () => {
		const cwd = await temporaryWorkspace();
		let actorExecutions = 0;
		let sandboxExecutions = 0;
		let releaseSandbox!: () => void;
		const sandboxGate = new Promise<void>((resolve) => {
			releaseSandbox = resolve;
		});
		const dispose = vi.fn();
		const tool: AgentTool<typeof bashSchema> = {
			name: "bash",
			label: "bash",
			description: "bash",
			parameters: bashSchema,
			execute: async () => {
				actorExecutions++;
				return { content: [{ type: "text", text: "actor" }], details: {} };
			},
		};
		const sandbox = mockRuntimeWorld(
			async () => {
				sandboxExecutions++;
				await sandboxGate;
				const text = `${Array.from({ length: 60 }, (_, index) => `line-${index + 1}`).join("\n")}\n`;
				const output = {
					result: { content: [{ type: "text" as const, text }], details: {} },
					isError: false,
				};
				return output;
			},
			dispose,
		);
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({ ...settings(), tools: ["bash"] }),
			draftModel: model("draft"),
			complete: async () =>
				assistant(
					[
						{
							type: "toolCall",
							id: "draft-bash",
							name: "bash",
							arguments: { command: "pytest -q 2>&1 | tail -60" },
						},
					],
					"toolUse",
				),
			preflight: () => true,
			resolveInvocation: (toolName, input) =>
				resolvePiToolInvocation(toolName, input, { cwd, environment: {}, shellPath: process.execPath }),
			projectionRules: [PI_BASH_TAIL_LINES_PROJECTION_RULE],
			executionWorlds: [sandbox, sandbox],
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => sandboxExecutions === 1);
		let settled = false;
		const pendingHit = host
			.consume({
				turnID: "turn-1",
				id: "actor-bash",
				tool: "bash",
				args: { command: "pytest -q 2>&1 | tail -20" },
				tools: [tool],
			})
			.then((hit) => {
				settled = true;
				return hit;
			});
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(settled).toBe(false);
		releaseSandbox();
		const hit = await pendingHit;

		const expected = `${Array.from({ length: 20 }, (_, index) => `line-${index + 41}`).join("\n")}\n`;
		expect(hit?.result.content).toEqual([{ type: "text", text: expected }]);
		expect(sandboxExecutions).toBe(1);
		expect(actorExecutions).toBe(0);
		expect(events.find((event) => event.type === "candidate")).toMatchObject({
			candidate: { execution: "runtime_sandbox" },
		});
		await host.dispose();
		expect(dispose).toHaveBeenCalledOnce();
	});

	it("rebases PatternAware from an authoritative Actor action within the same turn", async () => {
		const { cwd, patternSettings, patternStore, grepTool, readTool, materialized } =
			await patternRebaseFixture();
		const host = createSpeculativeActionHost("probe", {
			cwd,
			getSettings: () => ({
				...settings(4),
				drafterEnabled: false,
				tools: ["grep", "read"],
				patternAware: patternSettings,
			}),
			patternStore,
			complete: async () => assistant([{ type: "text", text: "unused" }], "stop"),
			preflight: () => true,
			onCandidateMaterialized: (candidate) => {
				materialized.push(candidate);
			},
		});
		const tools = [grepTool, readTool];
		await host.startTurn({
			turnID: "probe:turn",
			actorModel: model("actor"),
			context: { systemPrompt: "system", messages: [], tools },
			actorOptions: undefined,
			tools,
		});
		expect(materialized).toHaveLength(0);
		expect(
			await host.consume({
				turnID: "probe:turn",
				id: "actor-grep",
				tool: "grep",
				args: { pattern: "one", path: "." },
				tools,
			}),
		).toBeUndefined();
		const output = await grepTool.execute("actor-grep", { pattern: "one", path: "." });
		await host.actual({
			turnID: "probe:turn",
			id: "actor-grep",
			tool: "grep",
			args: { pattern: "one", path: "." },
			tools,
			durationMs: 12,
			output: { result: output, isError: false },
		});

		await waitFor(() => materialized.some((candidate) => candidate.source === "pattern_aware"));
		expect(materialized).toContainEqual(
			expect.objectContaining({
				sessionID: "probe",
				turnID: "probe:turn",
				expectedDecisionSequence: 2,
				latestDecisionSequence: 2,
				source: "pattern_aware",
				tool: "read",
				input: { path: "notes.txt" },
			}),
		);
		expect(patternStore.recent("probe")).toHaveLength(0);
		await host.finishTurn("probe:turn");
		await host.dispose();
	});

	it("rebases PatternAware after the Actor adopts a Drafter execution", async () => {
		const { cwd, patternSettings, patternStore, grepTool, readTool, materialized } =
			await patternRebaseFixture();
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("probe", {
			cwd,
			getSettings: () => ({
				...settings(1),
				drafterMaxDepth: 0,
				tools: ["grep", "read"],
				patternAware: patternSettings,
			}),
			patternStore,
			draftModel: model("draft"),
			complete: async () =>
				assistant(
					[{ type: "toolCall", id: "draft-grep", name: "grep", arguments: { pattern: "one", path: "." } }],
					"toolUse",
				),
			preflight: () => true,
			executionWorlds: [toolRuntimeWorld()],
			onCandidateMaterialized: (candidate) => {
				materialized.push(candidate);
			},
			onEvent: (event) => {
				events.push(event);
			},
		});
		const tools = [grepTool, readTool];
		await host.startTurn({
			turnID: "probe:turn",
			actorModel: model("actor"),
			context: { systemPrompt: "system", messages: [], tools },
			actorOptions: undefined,
			tools,
		});
		await waitFor(() =>
			events.some(
				(event) =>
					event.type === "candidate" &&
					event.candidate.source === "drafter" &&
					event.state.status === "succeeded",
			),
		);

		const adopted = await host.consume({
			turnID: "probe:turn",
			id: "actor-grep",
			tool: "grep",
			args: { pattern: "one", path: "." },
			tools,
		});

		expect(adopted).toBeDefined();
		await waitFor(() =>
			materialized.some((candidate) => candidate.source === "pattern_aware" && candidate.tool === "read"),
		);
		expect(materialized).toContainEqual(
			expect.objectContaining({
				expectedDecisionSequence: 2,
				latestDecisionSequence: 2,
				source: "pattern_aware",
				tool: "read",
				input: { path: "notes.txt" },
			}),
		);
		expect(patternStore.recent("probe")).toHaveLength(0);
		await host.finishTurn("probe:turn");
		await host.dispose();
	});

	it("turns one sidecar fork batch into safe parallel actions with real execution ahead", async () => {
		const cwd = await temporaryWorkspace();
		await writeFile(path.join(cwd, "wrong.txt"), "wrong", "utf8");
		await writeFile(path.join(cwd, "actor-miss.txt"), "actor", "utf8");
		const events: SpeculativeActionEvent<string>[] = [];
		const materialized: MaterializedSpeculativeCandidate<string>[] = [];
		const actorForkPlans = createActorForkPlanSource();
		let forkPath = "notes.txt";
		let forkMinimumLogprob = Math.log(0.95);
		let actionSourceEnabled = true;
		const selfSettings = () =>
			normalizeSelfSpeculationSettings({
				enabled: true,
				forkTransport: "sidecar",
				forkActionEnabled: actionSourceEnabled,
				forkGateEnabled: false,
				timeoutMs: 1_000,
			});
		const coordinator = new SelfSpeculationCoordinator({
			settings: selfSettings,
			requestID: () => "actor-request",
			actorForkPlanSource: actorForkPlans,
			fetch: vi.fn(async (input) =>
				Response.json(
					new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath
						? {
								details: {
									bundle: {
										candidates: [
											{
												candidate_ids: [`fork:${forkPath}`],
												sources: ["self-speculation"],
												tool_calls: [
													{ name: "read", arguments: { path: forkPath }, index: 0 },
													{ name: "read", arguments: { path: `${forkPath}.sibling` }, index: 1 },
												],
												fork: {
												logprobs: {
													token_count: 1,
													mean: forkMinimumLogprob,
													minimum: forkMinimumLogprob,
													tool_name: { minimum_probability: Math.exp(forkMinimumLogprob) },
												},
												},
											},
										],
									},
								},
							}
						: {},
				),
			),
		});
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => {
				await new Promise((resolve) => setTimeout(resolve, 80));
				return { content: [{ type: "text", text: input.path }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({
				...settings(1),
				drafterEnabled: false,
				maxConcurrentActions: 2,
				selfSpeculation: selfSettings(),
			}),
			actorForkPlanSource: actorForkPlans,
			complete: async () => assistant([], "stop"),
			preflight: () => true,
			executionWorlds: [toolRuntimeWorld()],
			onTurnStarted: ({ turnID, actorModel, context, decisionSequence }) =>
				coordinator.startTurn(turnID, actorModel, context, decisionSequence),
			onCandidateMaterialized: (candidate) => {
				materialized.push(candidate);
				coordinator.addCandidate(candidate);
			},
			onActorActionMaterialized: ({ action }) => coordinator.observeActorAction(action),
			onActorActionSettled: ({ settlement }) => coordinator.observeActorSettlement(settlement),
			onPredictionSettled: (feedback) => coordinator.observePredictionSettlement(feedback),
			onEvent: (event) => {
				events.push(event);
			},
		});
		const triggerFork = async (turnID: string) => {
			await host.startTurn(startInput(tool, turnID));
			coordinator.decorateActorPayload({ prompt: "P" });
			coordinator.observeActorOutput({ type: "text_delta", contentIndex: 0, delta: "x", partial: undefined as never });
		};
		const finishTurn = async (turnID: string) => {
			await host.finishTurn(turnID);
			coordinator.endTurn();
		};

		await triggerFork("fork-hit");
		await waitFor(() =>
			events.some(
				(event) => event.type === "candidate" && event.turnID === "fork-hit" && event.state.status === "succeeded",
			),
		);
		await waitFor(
			() => materialized.filter((candidate) => candidate.turnID === "fork-hit" && candidate.source === "self-speculation").length === 2,
		);
		const forkBatch = materialized.filter(
			(candidate) => candidate.turnID === "fork-hit" && candidate.source === "self-speculation",
		);
		expect(new Set(forkBatch.map((candidate) => candidate.proposalID)).size).toBe(1);
		expect(forkBatch.map((candidate) => candidate.actionID)).toEqual(["0:fork", "1:fork"]);
		const hit = await host.consume({
			turnID: "fork-hit",
			id: "actor-hit",
			tool: "read",
			args: { path: "notes.txt" },
			tools: [tool],
		});
		expect(hit?.result.content).toEqual([{ type: "text", text: "notes.txt" }]);
		await waitFor(() => events.some((event) => event.type === "actor_action" && event.turnID === "fork-hit"));
		const adopted = events.find((event) => event.type === "actor_action" && event.turnID === "fork-hit");
		expect(adopted).toMatchObject({ candidate: { source: "self-speculation" } });
		expect(
			adopted?.type === "actor_action" && adopted.settlement.provider.kind === "speculative"
				? adopted.settlement.provider.timing.executionAheadMs
				: 0,
		).toBeGreaterThan(50);
		expect(events.filter((event) => event.type === "source_request" && event.turnID === "fork-hit")).toHaveLength(1);
		await finishTurn("fork-hit");

		forkPath = "wrong.txt";
		await triggerFork("fork-miss");
		await waitFor(() =>
			events.some(
				(event) => event.type === "candidate" && event.turnID === "fork-miss" && event.state.status === "succeeded",
			),
		);
		expect(
			await host.consume({
				turnID: "fork-miss",
				id: "actor-miss",
				tool: "read",
				args: { path: "actor-miss.txt" },
				tools: [tool],
			}),
		).toBeUndefined();
		await host.actual({
			turnID: "fork-miss",
			id: "actor-miss",
			tool: "read",
			args: { path: "actor-miss.txt" },
			tools: [tool],
			durationMs: 80,
			output: { result: { content: [{ type: "text", text: "actor-miss.txt" }], details: {} }, isError: false },
		});
		await finishTurn("fork-miss");

		forkPath = "notes.txt";
		forkMinimumLogprob = Math.log(0.8);
		await triggerFork("fork-low-confidence");
		await waitFor(() => coordinator.snapshot().forkCompletions === 3);
		expect(events.some((event) => event.type === "candidate" && event.turnID === "fork-low-confidence")).toBe(false);
		coordinator.observeActorOutput({ type: "done", reason: "stop", message: assistant([], "stop") });
		await waitFor(() =>
			events.some((event) => event.type === "source_request" && event.turnID === "fork-low-confidence"),
		);
		await finishTurn("fork-low-confidence");
		const lowConfidenceRequests = events.filter(
			(event) => event.type === "source_request" && event.turnID === "fork-low-confidence",
		);
		expect(lowConfidenceRequests).toHaveLength(1);
		expect(lowConfidenceRequests[0]).toMatchObject({ request: { settlement: { status: "empty" } } });

		actionSourceEnabled = false;
		forkMinimumLogprob = Math.log(0.95);
		await triggerFork("fork-disabled");
		await waitFor(() => coordinator.snapshot().forkCompletions === 4);
		expect(events.some((event) => event.type === "source_request" && event.turnID === "fork-disabled")).toBe(false);
		expect(events.some((event) => event.type === "candidate" && event.turnID === "fork-disabled")).toBe(false);
		await finishTurn("fork-disabled");
		expect(coordinator.snapshot().forkActionAdoptions).toBe(1);
		expect(coordinator.snapshot().forkExecutionAheadMs).toBeGreaterThan(50);
		await host.dispose();
		await coordinator.dispose();
	}, 5_000);

	it("skips a shorter-context Drafter without truncating or compressing Actor history", async () => {
		const cwd = await temporaryWorkspace();
		const complete = vi.fn(async () => drafterCall({ path: "notes.txt" }));
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async () => ({ content: [{ type: "text" as const, text: "unused" }], details: {} }),
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: settings,
			draftModel: { ...model("short"), contextWindow: 32, maxTokens: 16 },
			complete,
			preflight: () => true,
		});

		await host.startTurn({
			...startInput(tool),
			context: { systemPrompt: "x".repeat(128), messages: [], tools: [tool] },
		});
		await waitFor(() => !host.runtime.inspect("session").pendingPredictions);

		expect(complete).not.toHaveBeenCalled();
		await host.dispose();
	});

});

function mockRuntimeWorld(
	execute: (context: Parameters<SpeculativeAgentExecutionWorld["speculation"]["execute"]>[0]) => ToolSettlement | Promise<ToolSettlement>,
	dispose?: SpeculativeAgentExecutionWorld["dispose"],
): SpeculativeAgentExecutionWorld {
	return {
		id: "runtime",
		scope: "runtime",
		isolation: "runtime_sandbox",
		speculation: {
			capabilities: "all",
			fingerprint: () => "runtime:v1",
			execute: async (context) => {
				const output = await execute(context);
				return {
					output,
					backend: "runtime",
					resources: [],
					capturedBytes: 0,
					executionMetrics: {},
					compatibility: {
						status: "compatible",
						backend: "runtime",
						executionFingerprint: context.action.executionFingerprint,
					},
					commit: async () => output,
					dispose: () => {},
				};
			},
		},
		...(dispose ? { dispose } : {}),
	};
}

function toolRuntimeWorld(): SpeculativeAgentExecutionWorld {
	return mockRuntimeWorld(async (context) => ({
		result: await context.tool.execute(context.callID, context.args as never, context.signal),
		isError: false,
	}));
}

async function patternRebaseFixture() {
	const cwd = await temporaryWorkspace();
	const patternSettings = { ...PATTERN_AWARE_DEFAULTS, minOccurrences: 2, multiStepEnabled: true };
	const patternStore = new PatternAwareStore(patternSettings);
	for (const [trainingSession, filePath] of [
		["training-a", "alpha.txt"],
		["training-b", "beta.txt"],
	] as const) {
		patternStore.observe({
			sessionID: trainingSession,
			turnID: `${trainingSession}:scan`,
			tool: "grep",
			input: { pattern: "one", path: "." },
			outcome: "success",
			outputPaths: [filePath],
			durationMs: 10,
		});
		patternStore.observe({
			sessionID: trainingSession,
			turnID: `${trainingSession}:read`,
			tool: "read",
			input: { path: filePath },
			outcome: "success",
			durationMs: 10,
		});
	}
	const grepTool: AgentTool<typeof grepSchema> = {
		name: "grep",
		label: "grep",
		description: "grep",
		parameters: grepSchema,
		execute: async () => ({ content: [{ type: "text", text: "notes.txt:1:one" }], details: {} }),
	};
	const readTool: AgentTool<typeof readSchema> = {
		name: "read",
		label: "read",
		description: "read",
		parameters: readSchema,
		execute: async () => ({ content: [{ type: "text", text: "one" }], details: {} }),
	};
	const materialized: MaterializedSpeculativeCandidate<string>[] = [];
	return { cwd, patternSettings, patternStore, grepTool, readTool, materialized };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for speculative runtime");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}
