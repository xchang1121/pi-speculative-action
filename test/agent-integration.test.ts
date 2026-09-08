import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, Model } from "@earendil-works/pi-ai";
import { createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KEYABLE_TOOLS, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { PI_READ_RANGE_PROJECTION_RULE } from "../src/pi-read-projection.ts";
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

async function temporaryWorkspace(base = os.tmpdir()): Promise<string> {
	const root = await mkdtemp(path.join(base, "pi-spec-host-"));
	roots.push(root);
	await writeFile(path.join(root, "notes.txt"), "one\ntwo\nthree\nfour", "utf8");
	return root;
}

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("speculative action host", () => {
	it("prepares raw predictions and previews once and adopts their keyed execution for every Pi tool", async () => {
		expect(mockToolCalls.map(([tool]) => tool)).toEqual(KEYABLE_TOOLS);
		for (const [origin, phase] of [["prediction", "running"], ["prediction", "completed"], ["preview", "running"], ["preview", "completed"]] as const) {
			for (const [toolName, proposal] of mockToolCalls) {
				const cwd = await temporaryWorkspace();
				const writer = createWriteTool(cwd);
				const args = toolName === "read" ? { ...proposal, offset: 2 } : proposal;
				const turnID = `${phase}-${toolName}`;
				const invocation = resolvePiToolInvocation(toolName, args, { cwd, environment: {} });
				const resourceExecution = PI_ACTION_SEMANTICS.effect(toolName) === "observation" ? invocation?.filesystem : undefined;
				const expected = resourceExecution ? toolName === "read" ? "two\nthree\nfour" : "notes.txt" : `${phase}:${toolName}`;
				const { promise: gate, resolve: release } = deferred<void>();
				const started = deferred<void>(), completed = deferred<void>(), adopted = deferred<void>();
				const speculativeExecution = vi.fn(async () => {
					started.resolve();
					await gate;
					return { content: [{ type: "text" as const, text: expected }], details: {} };
				});
				const actorExecution = vi.fn(async () => speculativeExecution());
				const permissions: Array<{ args: unknown; action: { input: unknown } }> = [];
				const prepareArguments = vi.fn((input: unknown) => {
					const value = structuredClone(input) as Record<string, unknown>;
					return toolName === "read" ? { ...value, offset: Number(value.offset ?? 1) + 1 } : value;
				});
				const tool: AgentTool<typeof mockToolSchema> = {
					name: toolName, label: toolName, description: toolName, parameters: mockToolSchema, prepareArguments,
					execute: resourceExecution ? async () => { throw new Error("Host tool must not execute speculatively"); } : speculativeExecution,
				};
				const events: SpeculativeActionEvent<string>[] = [];
				const sandbox = resourceExecution
					? createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: [toolName], maxBytes: () => 1024 * 1024 })
					: toolRuntimeWorld();
				const host = createSpeculativeActionHost(`session-${turnID}`, {
					cwd,
					getSettings: () => ({ ...settings(), drafterEnabled: origin === "prediction", drafterMaxDepth: 0, tools: [toolName] }),
					draftModel: model("draft"),
					complete: async () =>
						assistant([{ type: "toolCall", id: `draft-${toolName}`, name: toolName, arguments: proposal }], "toolUse"),
					preflight: (request) => { permissions.push(request); return true; },
					projectionRules: [PI_READ_RANGE_PROJECTION_RULE],
					resolveInvocation: () => resourceExecution ? { ...invocation!, filesystem: async (view, request) => {
						await speculativeExecution();
						return resourceExecution(view, request);
					} } : invocation,
					executionWorlds: [sandbox],
					onEvent: (event) => {
						events.push(event);
						if (event.type === "candidate" && event.state.status === "succeeded") completed.resolve();
						if (event.type === "actor_action") adopted.resolve();
					},
				});
				try {
					await host.startTurn({ ...startInput(tool, turnID), tools: resourceExecution ? [tool, writer] : [tool] });
					if (origin === "preview") for (let repeat = 0; repeat < 2; repeat++) {
						await host.previewActorCall({ turnID, id: `actor-${toolName}`, tool: toolName, args: proposal, tools: [tool] });
					}
					await started.promise;
					expect(prepareArguments).toHaveBeenCalledOnce();
					for (const { args, action } of permissions) expect(args).toEqual(action.input);
					if (phase === "completed") { release(); await completed.promise; }
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
					await adopted.promise;
					expect(prepareArguments).toHaveBeenCalledOnce();
					expect(events.find((event) => event.type === "actor_action"))
						.toMatchObject({ settlement: { provider: origin === "prediction"
							? { kind: "speculative", match: { kind: "exact" } } : { kind: "actor", origin: "preview" } } });
					expect(events.find((event) => event.type === "candidate" && event.state.status === "succeeded")).toMatchObject({
						candidate: { route: { reuse: PI_ACTION_SEMANTICS.effect(toolName) === "observation" ? "shared_result" : "exclusive_branch" } },
					});
					if (resourceExecution && origin === "prediction") {
						const query = toolName === "read" ? { path: "notes.txt", offset: 1, limit: 1 } : { path: ".", limit: 1 };
						const narrowed = await host.execute({ turnID, id: "another-view", tool: toolName, args: query, tools: [tool] }, undefined, actorExecution);
						const native = toolName === "read" ? createReadTool(cwd) : createLsTool(cwd);
						expect(narrowed).toEqual(await native.execute("native", query));
						expect(speculativeExecution).toHaveBeenCalledTimes(2); // Re-evaluation uses the sealed inputs, not the host tool.
						expect(actorExecution).not.toHaveBeenCalled();
						for (const changed of [false, true]) {
							const mutation = { path: changed && toolName === "ls" ? "added.txt" : "notes.txt", content: changed || toolName === "ls" ? "first\nchanged" : "one\ntwo\nthree\nfour" };
							await host.execute({ turnID, id: `write:${changed}`, tool: "write", args: mutation, tools: [tool, writer] }, undefined,
								() => writer.execute("native-write", mutation));
							const fallback = vi.fn(() => native.execute("native-read", args as never));
							const repeated = await host.execute({ turnID, id: `after-write:${changed}`, tool: toolName, args, tools: [tool, writer] }, undefined, fallback);
							expect(repeated).toEqual(await native.execute("control", args as never));
							expect(fallback).toHaveBeenCalledTimes(changed ? 1 : 0);
							if (!changed) expect(speculativeExecution).toHaveBeenCalledTimes(2);
						}
					}
					await host.finishTurn(turnID, true);
				} finally {
					release();
					await host.dispose();
				}
			}
		}
	});

	it.each([false, true])("only promotes proven host observations, independently of prediction (ThinkThread=%s)", async (thinkthread) => {
		const cwd = await temporaryWorkspace(path.join(process.cwd(), "bench")), file = path.join(cwd, "notes.txt");
		let tools: string[] = [];
		const tool = createReadTool(cwd);
		const clientFactory = vi.fn(() => { throw new Error("Actor observation must not initialize the SDK"); });
		const world = createThinkThreadExecutionWorld({ clientFactory, runnerFingerprint: "test" });
		const base = createSpeculativeActionHost("session", {
			cwd, getSettings: () => ({ enabled: true, drafterEnabled: false, tools, patternAware: { enabled: false } }),
			complete: async () => { throw new Error("No model calls expected"); }, preflight: () => true,
			resolveInvocation: (name, input) => resolvePiToolInvocation(name, input, { cwd, environment: {} }),
			speculativeExecutionWorldEnabled: () => false, executionWorlds: [
				...(thinkthread ? [world] : []),
				createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 4096 }),
			],
		});
		const host = thinkthread ? withThinkThreadProfileLifecycle(base, world) : base;
		let unstable = false;
		let args = { path: "@notes.txt", offset: 1 };
		const actor = vi.fn(async () => {
			if (unstable) await writeFile(file, "B\nsecond");
			const output = await tool.execute("read", args);
			if (unstable) await writeFile(file, "A\nsecond");
			return output;
		});
		try {
			for (const [turnID, input, changing, expected, calls, offset] of [
				["first", "A\nsecond", false, "A\nsecond", 1, 1],
				["input-hit", undefined, false, "second", 2, 2],
				["stale", "B\nsecond", false, "B\nsecond", 3, 1], ["ABA", "A\nsecond", true, "B\nsecond", 4, 1],
				["after-ABA", undefined, false, "A\nsecond", 5, 1],
			] as const) {
				if (input !== undefined) await writeFile(file, input);
				unstable = changing; args = { ...args, offset }; tools = [[], ["bash"], ["read"]][calls % 3]!;
				await host.startTurn(startInput(tool, turnID));
				const call = { turnID, id: turnID, tool: "read", args, tools: [tool] };
				await host.previewActorCall(call);
				expect((await host.execute(call, undefined, actor)).content).toEqual([{ type: "text", text: expected }]);
				expect(actor, turnID).toHaveBeenCalledTimes(calls - (process.platform !== "win32" && calls > 1 ? 1 : 0));
				await host.finishTurn(turnID);
			}
			expect(clientFactory).not.toHaveBeenCalled();
		} finally { await host.dispose(); }
	});

	it("binds one Actor operation through matching, fallback and settlement", async () => {
		const cwd = await temporaryWorkspace();
		for (const mode of ["keyed", "unkeyable", "outside-turn", "binding-error"]) {
			let profile = "initial", boundKey: unknown;
			const metadata = { command: "npm test", cwd, shell: process.execPath, commandTransport: "argv" as const,
				environment: { PROFILE: "initial" }, shellArgs: ["--initial"] };
			const descriptor = structuredClone(metadata);
			const problem = new Error("selected executor unavailable");
			const bindingStarted = deferred<void>(), releaseBinding = deferred<void>();
			const actor = vi.fn(async () => ({ content: [{ type: "text" as const, text: "built" }], details: {} }));
			const settled = vi.fn();
			const resolveInvocation = vi.fn(async () => {
				const invocation = { executor: profile, identity: metadata, process: metadata };
				bindingStarted.resolve(); await releaseBinding.promise;
				if (mode === "binding-error") throw problem;
				return invocation;
			});
			const tool: AgentTool<typeof bashSchema> = {
				name: "bash", label: "bash", description: "bash", parameters: bashSchema, execute: actor,
			};
			const host = createSpeculativeActionHost("session", {
				cwd, getSettings: () => ({ ...settings(), drafterEnabled: false, tools: ["bash"] }),
				complete: async () => { throw new Error("prediction disabled"); },
				resolveInvocation, onActorActionSettled: settled,
			});
			try {
				if (mode !== "outside-turn") await host.startTurn(startInput(tool));
				const call = { ...(mode !== "outside-turn" ? { turnID: "turn-1" } : {}), id: "actor-bash", tool: "bash",
					args: mode === "unkeyable" ? {} : { command: "npm test" }, tools: mode === "outside-turn" ? [] : [tool] };
				const admitted = structuredClone(call.args);
				const pending = host.execute(call, undefined, async (operation) => {
					metadata.environment.PROFILE = "reconfigured"; metadata.shellArgs.push("--later"); metadata.command = "later";
					expect(operation.input).toEqual(admitted);
					expect(operation.invocation).toEqual({ executor: "initial", identity: descriptor, process: descriptor });
					expect(operation.action?.executionContext).toEqual(mode === "keyed" ? operation.invocation : undefined);
					expect(operation.action?.input.command).toBe(mode === "keyed" ? "npm test" : undefined);
					boundKey = operation.action;
					return actor();
				});
				const outcome = mode === "binding-error" ? expect(pending).rejects.toBe(problem) : expect(pending).resolves.toHaveProperty("content.0.text", "built");
				await bindingStarted.promise; profile = "next"; call.args.command = "changed during binding"; releaseBinding.resolve();
				await outcome;
				await host.finishTurn("turn-1", true);
				expect(resolveInvocation).toHaveBeenCalledOnce(); expect(actor).toHaveBeenCalledTimes(mode === "binding-error" ? 0 : 1);
				if (mode === "keyed") { expect(settled).toHaveBeenCalledOnce(); expect(settled.mock.calls[0][0].action).toBe(boundKey); }
			} finally { releaseBinding.resolve(); await host.dispose(); }
		}
	});

	it.each(["running", "completed"])("does not adopt a %s Bash command through an unproven suffix relation", async (phase) => {
		const cwd = await temporaryWorkspace(), started = deferred<void>(), finish = deferred<void>(), completed = deferred<void>();
		const actor = vi.fn(async () => ({ content: [{ type: "text" as const, text: "tail arguments: -n 2" }], details: {} }));
		const tool: AgentTool<typeof bashSchema> = { name: "bash", label: "bash", description: "bash", parameters: bashSchema, execute: actor };
		const dispose = vi.fn();
		const sandbox = mockRuntimeWorld(async () => {
			started.resolve(); await finish.promise;
			return { result: { content: [{ type: "text", text: "tail arguments: -n 3" }], details: {} }, isError: false };
		}, dispose);
		const host = createSpeculativeActionHost("session", {
			cwd, getSettings: () => ({ ...settings(), tools: ["bash"] }), draftModel: model("draft"),
			complete: async () => assistant([{ type: "toolCall", id: "draft-bash", name: "bash",
				arguments: { command: "printf data 2>&1 | tail -n 3" } }], "toolUse"),
			preflight: () => true, executionWorlds: [sandbox, sandbox],
			resolveInvocation: (name, args) => resolvePiToolInvocation(name, args, { cwd, environment: {}, shellPath: process.execPath }),
			onEvent: (event) => { if (event.type === "candidate" && event.state.status === "succeeded") completed.resolve(); },
		});
		try {
			await host.startTurn(startInput(tool)); await started.promise;
			if (phase === "completed") { finish.resolve(); await completed.promise; }
			const output = await host.execute({ turnID: "turn-1", id: "actor-bash", tool: "bash",
				args: { command: "printf data 2>&1 | tail -n 2" }, tools: [tool] }, undefined, actor);
			expect(output.content).toEqual([{ type: "text", text: "tail arguments: -n 2" }]);
			expect(actor).toHaveBeenCalledOnce();
		} finally { finish.resolve(); await host.dispose(); }
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
					validate: async () => ({ status: "valid", metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } }), // Fixed fixture inputs.
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

function deferred<Value>() {
	let resolve!: (value: Value | PromiseLike<Value>) => void;
	const promise = new Promise<Value>((done) => { resolve = done; });
	return { promise, resolve };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for speculative runtime");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}
