import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, Model } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost, patternPlanActionID } from "../src/agent-integration.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { PI_BASH_TAIL_LINES_PROJECTION_RULE } from "../src/pi-bash-projection.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { MaterializedSpeculativeCandidate, SpeculativeActionEvent } from "../src/runtime.ts";
import { SelfSpeculationActionBridge } from "../src/self-speculation-action-bridge.ts";
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
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("speculative action host", () => {
	it("satisfies an actor call without modifying or wrapping an Agent instance", async () => {
		const cwd = await temporaryWorkspace();
		let executions = 0;
		const events: SpeculativeActionEvent<string>[] = [];
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async () => {
				executions++;
				return { content: [{ type: "text", text: "one\ntwo\nthree\nfour" }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: settings,
			draftModel: model("draft"),
			complete: async () => drafterCall({ path: "notes.txt" }),
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => events.some((event) => event.type === "candidate" && event.state.status === "succeeded"));
		const hit = await host.execute(
			{
				turnID: "turn-1",
				id: "actor-1",
				tool: "read",
				args: { path: "notes.txt" },
				tools: [tool],
			},
			undefined,
			async () => tool.execute("actor-1", { path: "notes.txt" }),
		);

		expect(hit.content).toEqual([{ type: "text", text: "one\ntwo\nthree\nfour" }]);
		expect(executions).toBe(1);
		expect(events.find((event) => event.type === "candidate")).toMatchObject({
			candidate: { execution: "resource_snapshot" },
		});
		await waitFor(() =>
			events.some((event) => event.type === "actor_action" && event.settlement.provider.kind === "speculative"),
		);
		await host.dispose();
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
		const sandbox: SpeculativeAgentExecutionWorld = {
			id: "runtime",
			scope: "runtime",
			isolation: "runtime_sandbox",
			capabilities: "all",
			fingerprint: () => "runtime:v1",
			fork: async (context) => {
				sandboxExecutions++;
				await sandboxGate;
				const text = `${Array.from({ length: 60 }, (_, index) => `line-${index + 1}`).join("\n")}\n`;
				const output = {
					result: { content: [{ type: "text" as const, text }], details: {} },
					isError: false,
				};
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
					state: "sealed",
					commit: async () => output,
					dispose: () => {},
				};
			},
			dispose,
		};
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

	it("prefers a runtime-wide sandbox over a read tool's resource-snapshot fallback", async () => {
		const cwd = await temporaryWorkspace();
		let hostExecutions = 0;
		const fork = vi.fn(async (context: Parameters<SpeculativeAgentExecutionWorld["fork"]>[0]) => {
			const output = {
				result: { content: [{ type: "text" as const, text: "runtime read" }], details: {} },
				isError: false,
			};
			return {
				output,
				backend: "runtime",
				resources: [],
				capturedBytes: 0,
				executionMetrics: {},
				compatibility: {
					status: "compatible" as const,
					backend: "runtime",
					executionFingerprint: context.action.executionFingerprint,
				},
				state: "sealed" as const,
				commit: async () => output,
				dispose: () => {},
			};
		});
		const runtimeWorld: SpeculativeAgentExecutionWorld = {
			id: "runtime",
			scope: "runtime",
			isolation: "runtime_sandbox",
			capabilities: "all",
			fork,
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async () => {
				hostExecutions++;
				return { content: [{ type: "text", text: "host read" }], details: {} };
			},
		};
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: settings,
			draftModel: model("draft"),
			complete: async () => drafterCall({ path: "notes.txt" }),
			preflight: () => true,
			executionWorlds: [runtimeWorld],
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => events.some((event) => event.type === "candidate" && event.state.status === "succeeded"));
		const hit = await host.consume({
			turnID: "turn-1",
			id: "actor-read",
			tool: "read",
			args: { path: "notes.txt" },
			tools: [tool],
		});

		expect(hit?.result.content).toEqual([{ type: "text", text: "runtime read" }]);
		expect(fork).toHaveBeenCalledOnce();
		expect(fork.mock.calls[0]?.[0]).toMatchObject({ toolName: "read" });
		expect(hostExecutions).toBe(0);
		await host.dispose();
	});

	it("falls through unavailable runtime worlds to the resource fallback", async () => {
		const cwd = await temporaryWorkspace();
		const brokenPrepare = vi.fn();
		const unavailablePrepare = vi.fn(async () => {
			throw new Error("runtime failed to start");
		});
		const broken: SpeculativeAgentExecutionWorld = {
			id: "broken",
			scope: "runtime",
			isolation: "runtime_sandbox",
			capabilities: "all",
			fingerprint: () => {
				throw new Error("backend unavailable");
			},
			prepare: brokenPrepare,
			fork: async () => {
				throw new Error("must not execute");
			},
		};
		const unavailable: SpeculativeAgentExecutionWorld = {
			id: "unavailable",
			scope: "runtime",
			isolation: "runtime_sandbox",
			capabilities: "all",
			prepare: unavailablePrepare,
			fork: async () => {
				throw new Error("must not execute");
			},
		};
		let hostExecutions = 0;
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async () => {
				hostExecutions++;
				return { content: [{ type: "text", text: "fallback" }], details: {} };
			},
		};
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: settings,
			draftModel: model("draft"),
			complete: async () => drafterCall({ path: "notes.txt" }),
			preflight: () => true,
			executionWorlds: [broken, unavailable],
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => events.some((event) => event.type === "candidate" && event.state.status === "succeeded"));
		expect(brokenPrepare).not.toHaveBeenCalled();
		expect(unavailablePrepare).toHaveBeenCalledWith(expect.objectContaining({ cwd }));
		expect(hostExecutions).toBe(1);
		expect(events.find((event) => event.type === "candidate")).toMatchObject({
			candidate: { execution: "resource_snapshot" },
		});
		await host.dispose();
	});

	it("orders pattern learning without generating discarded predictions", async () => {
		const cwd = await temporaryWorkspace();
		const patternStore = new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, minOccurrences: 1 });
		const originalObserve = patternStore.observeBatch.bind(patternStore);
		let analysisComplete = false;
		const observed = vi.spyOn(patternStore, "observeBatch").mockImplementation((...args) => {
			const deadline = performance.now() + 100;
			while (performance.now() < deadline) {
				// Model an expensive synchronous mining pass from a large real trace.
			}
			const result = originalObserve(...args);
			analysisComplete = true;
			return result;
		});
		const predictionStates: boolean[] = [];
		const originalPredict = patternStore.predict.bind(patternStore);
		vi.spyOn(patternStore, "predict").mockImplementation((...args) => {
			predictionStates.push(analysisComplete);
			return originalPredict(...args);
		});
		let resolveStore!: (store: PatternAwareStore) => void;
		const pendingStore = new Promise<PatternAwareStore>((resolve) => {
			resolveStore = resolve;
		});
		const actualEvents: SpeculativeActionEvent<string>[] = [];
		let disposed = 0;
		const sandbox: SpeculativeAgentExecutionWorld = {
			id: "unavailable",
			scope: "runtime",
			isolation: "runtime_sandbox",
			capabilities: "all",
			fingerprint: () => {
				throw new Error("unavailable");
			},
			fork: async () => {
				throw new Error("unused");
			},
			dispose: async () => {
				disposed++;
				throw new Error("cleanup failed");
			},
		};
		const tool: AgentTool<typeof grepSchema> = {
			name: "grep",
			label: "grep",
			description: "grep",
			parameters: grepSchema,
			execute: async () => ({ content: [{ type: "text", text: "nested/notes.txt:1:one" }], details: {} }),
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({
				...settings(),
				drafterEnabled: false,
				tools: ["grep"],
				patternAware: { ...PATTERN_AWARE_DEFAULTS, minOccurrences: 1 },
			}),
			patternStore: pendingStore,
			draftModel: model("draft"),
			complete: async () => assistant([{ type: "text", text: "no prediction" }], "stop"),
			preflight: () => true,
			executionWorlds: [sandbox],
			onEvent: (event) => {
				actualEvents.push(event);
			},
		});
		let started = false;
		const start = host.startTurn(startInput(tool)).then(() => {
			started = true;
		});
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(started).toBe(true);
		await start;
		await waitFor(() => host.runtime.inspect().pendingPredictions === 1);
		expect(
			await host.consume({
				turnID: "turn-1",
				id: "actor",
				tool: "grep",
				args: { pattern: "one", path: "src" },
				tools: [tool],
			}),
		).toBeUndefined();
		resolveStore(patternStore);
		await waitFor(() => actualEvents.filter((event) => event.type === "source_request").length === 1);
		expect(actualEvents.find((event) => event.type === "source_request")?.request.settlement.status).toBe("aborted");
		await host.actual({
			turnID: "turn-1",
			id: "actor",
			tool: "grep",
			args: { pattern: "one", path: "src" },
			tools: [tool],
			durationMs: 12,
			output: { result: await tool.execute("actor", { pattern: "one", path: "src" }), isError: false },
		});
		await host.finishTurn("turn-1");
		expect(observed).not.toHaveBeenCalled();
		await host.startTurn(startInput(tool, "turn-2"));

		await waitFor(() =>
			actualEvents.some(
				(event) =>
					event.type === "actor_action" &&
					event.settlement.provider.kind === "actor" &&
					event.settlement.provider.durationMs === 12,
			),
		);
		await waitFor(() => actualEvents.filter((event) => event.type === "source_request").length === 2);
		expect(observed.mock.calls[0]?.[0][0]).toMatchObject({
			input: { pattern: "one", path: "src" },
			outputPaths: ["src/nested/notes.txt"],
		});
		expect(predictionStates).toEqual([false, true]);
		await expect(host.dispose()).resolves.toBeUndefined();
		expect(disposed).toBe(1);
	});

	it("rebases PatternAware from an authoritative Actor action within the same turn", async () => {
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

	it("runs independent single-action drafts concurrently and deduplicates them by K(a)", async () => {
		const cwd = await temporaryWorkspace();
		await Promise.all([
			writeFile(path.join(cwd, "other.txt"), "other", "utf8"),
			writeFile(path.join(cwd, "ignored.txt"), "ignored", "utf8"),
		]);
		const executed: string[] = [];
		const events: SpeculativeActionEvent<string>[] = [];
		const requests: Array<{ context: Parameters<CreateComplete>[1]; options: Parameters<CreateComplete>[2] }> = [];
		let releaseSlowRequest: ((message: AssistantMessage) => void) | undefined;
		const slowRequest = new Promise<AssistantMessage>((resolve) => {
			releaseSlowRequest = resolve;
		});
		let activeRequests = 0;
		let maxActiveRequests = 0;
		const getDraftOptions = vi.fn(() => ({
			temperature: 1,
			maxTokens: 2_048,
			reasoning: "high" as const,
			deferred: true,
		}));
		const complete: CreateComplete = async (_model, context, requestOptions) => {
			const index = requests.length;
			requests.push({ context, options: requestOptions });
			activeRequests++;
			maxActiveRequests = Math.max(maxActiveRequests, activeRequests);
			await Promise.resolve();
			if (index === 6) {
				activeRequests--;
				throw new Error("one sample failed");
			}
			if (index === 7) {
				const result = await slowRequest;
				activeRequests--;
				return result;
			}
			if (index === 5) {
				activeRequests--;
				return assistant(
					[{ type: "toolCall", id: "actor-only", name: "actor_only", arguments: { path: "notes.txt" } }],
					"toolUse",
				);
			}
			activeRequests--;
			const selected = index % 2 === 0 ? "notes.txt" : "other.txt";
			return assistant(
				[
					{ type: "toolCall", id: `selected-${index}`, name: "read", arguments: { path: selected } },
					{ type: "toolCall", id: `ignored-${index}`, name: "read", arguments: { path: "ignored.txt" } },
				],
				"toolUse",
			);
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, args) => {
				executed.push(args.path);
				return { content: [{ type: "text", text: args.path }], details: {} };
			},
		};
		const actorOnly = vi.fn();
		const actorOnlyTool: AgentTool<typeof readSchema> = {
			...tool,
			name: "actor_only",
			label: "actor_only",
			execute: actorOnly,
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({
				...settings(11),
				drafterMaxDepth: 0,
				drafterMaxTokens: 96,
				drafterDeterministicCandidates: 2,
				drafterTemperatureMin: 0.4,
				drafterTemperatureMax: 1.6,
			}),
			draftModel: model("draft"),
			getDraftOptions,
			complete,
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		const actorInput = {
			...startInput(tool),
			context: { systemPrompt: "system", messages: [], tools: [tool, actorOnlyTool] },
			tools: [tool, actorOnlyTool],
		};
		await host.startTurn(actorInput);
		await waitFor(
			() => events.filter((event) => event.type === "candidate" && event.state.status === "succeeded").length === 2,
		);
		expect(host.runtime.inspect("session").pendingPredictions).toBe(1);
		releaseSlowRequest?.(assistant([{ type: "text", text: "no tool needed" }], "stop"));
		await waitFor(() => !host.runtime.inspect("session").pendingPredictions);

		expect(requests).toHaveLength(11);
		expect(maxActiveRequests).toBe(11);
		expect(getDraftOptions).toHaveBeenCalledTimes(1);
		expect(new Set(requests.map((request) => request.options?.sessionId))).toEqual(new Set(["session"]));
		expect(requests[0]?.context).toBe(actorInput.context);
		expect(requests.every((request) => request.context === requests[0]!.context)).toBe(true);
		const temperatures = requests.map((request) => request.options?.temperature);
		expect(temperatures.slice(0, 3)).toEqual([0, 0, 0.4]);
		expect(temperatures.at(-1)).toBeCloseTo(1.6);
		for (const request of requests) {
			expect(request.context.systemPrompt).toBe(actorInput.context.systemPrompt);
			expect(request.context.messages).toBe(actorInput.context.messages);
			expect(request.context.tools?.map((candidate) => candidate.name)).toEqual(["read", "actor_only"]);
			expect(request.options).toMatchObject({
				maxTokens: 96,
				toolChoice: "required",
				reasoning: undefined,
				deferred: false,
				cacheRetention: "short",
			});
		}
		expect(executed.sort()).toEqual(["notes.txt", "other.txt"]);
		expect(actorOnly).not.toHaveBeenCalled();
		await waitFor(
			() => events.filter((event) => event.type === "candidate" && event.state.status === "running").length === 2,
		);
		await host.dispose();
	});

	it("cancels the slower default-width Drafter after the first valid tool call", async () => {
		const cwd = await temporaryWorkspace();
		const events: SpeculativeActionEvent<string>[] = [];
		const requestSignals: AbortSignal[] = [];
		let loserAborted = false;
		const complete: CreateComplete = async (_model, _context, requestOptions) => {
			const signal = requestOptions?.signal;
			if (!signal) throw new Error("Drafter request must receive an AbortSignal");
			const index = requestSignals.push(signal) - 1;
			if (index === 0) {
				await Promise.resolve();
				return drafterCall({ path: "notes.txt" });
			}
			return new Promise<AssistantMessage>((resolve) => {
				signal.addEventListener(
					"abort",
					() => {
						loserAborted = true;
						resolve(assistant([], "aborted"));
					},
					{ once: true },
				);
			});
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => ({ content: [{ type: "text", text: input.path }], details: {} }),
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({ ...settings(2), drafterMaxDepth: 0 }),
			draftModel: model("draft"),
			complete,
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => requestSignals.length === 2);
		await waitFor(() => events.filter((event) => event.type === "source_request").length === 2);
		await waitFor(() => host.runtime.inspect("session").sharedCandidates === 1);

		expect(loserAborted).toBe(true);
		expect(requestSignals[1]?.aborted).toBe(true);
		expect(
			events
				.filter((event) => event.type === "source_request")
				.map((event) => event.request.settlement.status),
		).toEqual(expect.arrayContaining(["produced", "aborted"]));
		await host.dispose();
	});

	it("gates the whole Drafter batch by realized action execution ahead and recovers with a bounded probe", async () => {
		const cwd = await temporaryWorkspace();
		await writeFile(path.join(cwd, "wrong.txt"), "wrong", "utf8");
		const events: SpeculativeActionEvent<string>[] = [];
		let completeCalls = 0;
		let draftOptionsCalls = 0;
		let exactDraft = false;
		let actionOffset = 1;
		let gateEnabled = true;
		const complete: CreateComplete = async () => {
			completeCalls++;
			await new Promise((resolve) => setTimeout(resolve, 15));
			return drafterCall({ path: exactDraft ? "notes.txt" : "wrong.txt", offset: actionOffset });
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => {
				if (input.path === "notes.txt") await new Promise((resolve) => setTimeout(resolve, 300));
				return { content: [{ type: "text", text: input.path }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({ ...settings(2), drafterMaxDepth: 0, drafterGateEnabled: gateEnabled }),
			draftModel: model("draft"),
			getDraftOptions: () => {
				draftOptionsCalls++;
				return {};
			},
			complete,
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		const runTurn = async (index: number, providerExpected: boolean, exact: boolean) => {
			const turnID = `gate-${index}`;
			exactDraft = exact;
			// Each gate sample needs new work; otherwise authoritative-result reuse correctly bypasses the Drafter.
			actionOffset = index;
			await host.startTurn(startInput(tool, turnID));
			await waitFor(
				() => events.filter((event) => event.type === "source_request" && event.turnID === turnID).length === 2,
			);
			if (providerExpected && exact) {
				await waitFor(() =>
					events.some(
						(event) => event.type === "candidate" && event.turnID === turnID && event.state.status === "succeeded",
					),
				);
			}
			const hit = await host.consume({
				turnID,
				id: `actor-${index}`,
				tool: "read",
				args: { path: "notes.txt", offset: actionOffset },
				tools: [tool],
			});
			if (!hit) {
				await host.actual({
					turnID,
					id: `actor-${index}`,
					tool: "read",
					args: { path: "notes.txt", offset: actionOffset },
					tools: [tool],
					durationMs: 0,
					output: { result: { content: [], details: {} }, isError: false },
				});
			}
			await host.finishTurn(turnID);
		};

		for (let index = 1; index <= 4; index++) await runTurn(index, true, false);
		await waitFor(() => host.drafterGateSnapshot().samples === 4);
		expect(host.drafterGateSnapshot().expectedNetBenefitMs).toBeLessThan(0);
		expect(completeCalls).toBe(8);
		expect(draftOptionsCalls).toBe(4);

		for (let index = 5; index <= 7; index++) await runTurn(index, false, false);
		expect(completeCalls).toBe(8);
		expect(draftOptionsCalls).toBe(4);
		expect(host.drafterGateSnapshot().skippedBatches).toBe(3);

		await runTurn(8, true, true);
		await waitFor(() => (host.drafterGateSnapshot().expectedNetBenefitMs ?? -Infinity) >= 25);
		expect(host.drafterGateSnapshot().expectedNetBenefitMs).toBeGreaterThanOrEqual(25);
		for (let index = 9; index <= 12; index++) await runTurn(index, true, false);
		expect(completeCalls).toBe(18);
		await waitFor(() => (host.drafterGateSnapshot().expectedNetBenefitMs ?? Infinity) < 0);
		await runTurn(13, false, false);
		expect(completeCalls).toBe(18);

		gateEnabled = false;
		await runTurn(14, true, false);
		expect(completeCalls).toBe(20);
		expect(draftOptionsCalls).toBe(10);
		await host.dispose();
	}, 5_000);

	it("turns one sidecar fork batch into safe parallel actions with real execution ahead", async () => {
		const cwd = await temporaryWorkspace();
		await writeFile(path.join(cwd, "wrong.txt"), "wrong", "utf8");
		await writeFile(path.join(cwd, "actor-miss.txt"), "actor", "utf8");
		const events: SpeculativeActionEvent<string>[] = [];
		const materialized: MaterializedSpeculativeCandidate<string>[] = [];
		const actionBridge = new SelfSpeculationActionBridge();
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
			actionBridge,
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
			selfSpeculationActionBridge: actionBridge,
			complete: async () => assistant([], "stop"),
			preflight: () => true,
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
		const lowConfidenceRequests = events.filter(
			(event) => event.type === "source_request" && event.turnID === "fork-low-confidence",
		);
		expect(lowConfidenceRequests).toHaveLength(1);
		expect(lowConfidenceRequests[0]).toMatchObject({ request: { settlement: { status: "empty" } } });
		expect(events.some((event) => event.type === "candidate" && event.turnID === "fork-low-confidence")).toBe(false);
		await finishTurn("fork-low-confidence");

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

	it("does not let an invalid tool call cancel a valid default-width peer", async () => {
		const cwd = await temporaryWorkspace();
		const events: SpeculativeActionEvent<string>[] = [];
		const requestSignals: AbortSignal[] = [];
		let releaseValid!: () => void;
		const validGate = new Promise<void>((resolve) => {
			releaseValid = resolve;
		});
		const complete: CreateComplete = async (_model, _context, requestOptions) => {
			const signal = requestOptions?.signal;
			if (!signal) throw new Error("Drafter request must receive an AbortSignal");
			const index = requestSignals.push(signal) - 1;
			if (index === 0) return drafterCall({ offset: 1 });
			await validGate;
			return drafterCall({ path: "notes.txt" });
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => ({ content: [{ type: "text", text: input.path }], details: {} }),
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({ ...settings(2), drafterMaxDepth: 0 }),
			draftModel: model("draft"),
			complete,
			preflight: () => true,
			onEvent: (event) => {
				events.push(event);
			},
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => requestSignals.length === 2);
		await waitFor(() => events.filter((event) => event.type === "source_request").length === 1);
		expect(events.find((event) => event.type === "source_request")?.request.settlement.status).toBe("empty");
		expect(requestSignals[1]?.aborted).toBe(false);

		releaseValid();
		await waitFor(() => events.filter((event) => event.type === "source_request").length === 2);
		await waitFor(() => host.runtime.inspect("session").sharedCandidates === 1);
		expect(
			events
				.filter((event) => event.type === "source_request")
				.map((event) => event.request.settlement.status),
		).toEqual(expect.arrayContaining(["empty", "produced"]));
		await host.dispose();
	});

	it("lets output-informed Drafter branches terminate without forcing another tool", async () => {
		const cwd = await temporaryWorkspace();
		const requests: Array<Parameters<CreateComplete>[2]> = [];
		const executed: string[] = [];
		const complete: CreateComplete = async (_model, context, options) => {
			requests.push(options);
			const results = context.messages.filter((message) => message.role === "toolResult").length;
			if (results < 2) return drafterCall({ path: results === 0 ? "notes.txt" : "target.txt" });
			return (options as { toolChoice?: string }).toolChoice === "required"
				? drafterCall({ path: "forced-after-terminal.txt" })
				: assistant([{ type: "text", text: "done" }], "stop");
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async (_id, input) => {
				executed.push(input.path);
				return { content: [{ type: "text", text: input.path }], details: {} };
			},
		};
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: () => ({ ...settings(), drafterMaxDepth: 2 }),
			draftModel: model("draft"),
			complete,
			preflight: () => true,
		});

		await host.startTurn(startInput(tool));
		await waitFor(() => requests.length === 3);
		await waitFor(() => !host.runtime.inspect("session").pendingPredictions);

		expect(requests.map((options) => (options as { toolChoice?: string }).toolChoice)).toEqual([
			"required",
			"auto",
			"auto",
		]);
		expect(executed).toEqual(["notes.txt", "target.txt"]);
		await host.dispose();
	});

	it("uses the actor model by default and permits it as the explicit Drafter model", async () => {
		const cwd = await temporaryWorkspace();
		const usedModels: string[] = [];
		const hasMaxTokens: boolean[] = [];
		const complete: CreateComplete = async (usedModel, _context, options) => {
			usedModels.push(`${usedModel.provider}/${usedModel.id}`);
			hasMaxTokens.push(Object.hasOwn(options ?? {}, "maxTokens"));
			return assistant([{ type: "text", text: "no tool needed" }], "stop");
		};
		const tool: AgentTool<typeof readSchema> = {
			name: "read",
			label: "read",
			description: "read",
			parameters: readSchema,
			execute: async () => ({ content: [{ type: "text", text: "unused" }], details: {} }),
		};

		for (const draftModel of [undefined, model("actor")]) {
			const expectedRequests = usedModels.length + 1;
			const host = createSpeculativeActionHost("session", {
				cwd,
				getSettings: settings,
				...(draftModel ? { draftModel } : {}),
				complete,
				preflight: () => true,
			});
			await host.startTurn({ ...startInput(tool), actorOptions: { maxTokens: 2_048 } });
			await waitFor(() => usedModels.length === expectedRequests);
			await waitFor(() => !host.runtime.inspect("session").pendingPredictions);
			await host.dispose();
		}

		expect(usedModels).toEqual(["openai/actor", "openai/actor"]);
		expect(hasMaxTokens).toEqual([false, false]);
	});

	it("names equal actions under different parent paths independently", () => {
		const left = patternPlanActionID("shared", patternPlanActionID("left"));
		const right = patternPlanActionID("shared", patternPlanActionID("right"));
		expect(left).not.toBe(right);
	});
});

type CreateComplete = NonNullable<Parameters<typeof createSpeculativeActionHost>[1]["complete"]>;

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for speculative runtime");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}
