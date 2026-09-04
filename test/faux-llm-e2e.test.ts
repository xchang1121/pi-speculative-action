import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Agent, type AgentMessage, type AgentTool, type AgentToolResult } from "@earendil-works/pi-agent-core";
import {
	createFauxCore,
	type FauxContentBlock,
	type FauxResponseStep,
	fauxAssistantMessage,
	fauxThinking,
	fauxToolCall,
	type Message,
} from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "../src/effect-model.ts";
import { PATTERN_AWARE_DEFAULTS, type PatternAwareSettings, PatternAwareStore } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";

const roots: string[] = [];
const readSchema = Type.Object({ path: Type.String() });
const findSchema = Type.Object({ pattern: Type.String(), path: Type.Optional(Type.String()) });
const bashSchema = Type.Object({ command: Type.String() });

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("faux LLM speculative action end to end", () => {
	it("masks completed tool latency across fragmented Actor and Drafter streams", async () => {
		const cwd = await workspace();
		const tool = delayedRead(cwd, 120);
		const actorTurns = [
			turn([
				fauxThinking("inspect the requested file before answering ".repeat(4)),
				fauxToolCall("read", { path: "notes.txt" }),
			]),
			turn("done"),
		];
		const draftTurns = [turn(fauxToolCall("read", { path: "notes.txt" })), turn("no tool")];

		const speculative = await runAgent({
			cwd,
			sessionID: "completed-hit",
			tools: [tool],
			actorTurns,
			actorTokensPerSecond: 180,
			draftTurns,
			draftTokensPerSecond: 2_000,
			settings: drafterSettings(),
		});
		expect(speculative.streamEvents).toEqual(expect.arrayContaining(["thinking_delta", "toolcall_delta"]));
		expect(speculative.summary).toMatchObject({
			tasks: 1,
			actorActions: 1,
			speculativeHits: 1,
			actorFallbacks: 0,
		});
		expect(speculative.summary.executionAheadMs).toBeGreaterThanOrEqual(100);
		expect(speculative.summary.hitLatencyMs).toBeLessThan(40);
		expect(speculative.summary.hiddenLatencyMs).toBeGreaterThanOrEqual(100);
		expect(speculative.summary.serializedMs - speculative.summary.endToEndMs).toBeCloseTo(
			speculative.summary.hiddenLatencyMs,
		);
		expect(speculative.executions.read).toBe(1);
		expect(speculative.toolLatencyMs[0]).toBeLessThan(40);
	});

	it("continues after joining compatible work already in flight", async () => {
		const cwd = await workspace();
		await writeFile(path.join(cwd, "target.txt"), "target", "utf8");
		const run = (sessionID: string, drafterMaxDepth: number) => {
			const respond: FauxResponseStep = (context) => {
				const followsOwnDraft = context.messages.some(
					(message) => message.role === "assistant" && message.provider === `drafter-${sessionID}`,
				);
				const hasToolResult = context.messages.some((message) => message.role === "toolResult");
				return fauxAssistantMessage(
					!hasToolResult
						? fauxToolCall("read", { path: "notes.txt" })
						: followsOwnDraft
							? fauxToolCall("read", { path: "target.txt" })
							: "no tool",
					{ stopReason: !hasToolResult || followsOwnDraft ? "toolUse" : "stop" },
				);
			};
			return runAgent({
				cwd,
				sessionID,
				tools: [delayedRead(cwd, (file) => (file === "notes.txt" ? 160 : 180))],
				actorTurns: [
					turn(fauxToolCall("read", { path: "notes.txt" }), 45),
					turn(fauxToolCall("read", { path: "target.txt" }), 220),
					turn("done"),
				],
				actorTokensPerSecond: 2_000,
				draftTurns: [],
				draftResponses: Array.from({ length: 5 }, () => respond),
				draftTokensPerSecond: 2_000,
				settings: { ...drafterSettings(), drafterMaxDepth },
			});
		};
		const baseline = await run("in-flight-baseline", 0);
		const treatment = await run("in-flight-treatment", 1);

		expect(treatment.summary.sourceRequests).toBe(baseline.summary.sourceRequests);
		expect(baseline.summary).toMatchObject({ actorActions: 2, speculativeHits: 1, actorFallbacks: 1 });
		expect(treatment.summary).toMatchObject({ actorActions: 2, speculativeHits: 2, actorFallbacks: 0 });
		expect(treatment.executions.read).toBe(2);
		expect(treatment.summary.executionAheadMs).toBeGreaterThan(0);
		expect(treatment.summary.hitLatencyMs).toBeGreaterThan(0);
		expect(treatment.toolLatencyMs[1]).toBeLessThan(40);
		expect(baseline.summary.endToEndMs - treatment.summary.endToEndMs).toBeGreaterThan(120);
	});

	it("falls back once when Drafter delivery is late or terminates with an error", async () => {
		const cwd = await workspace();
		for (const [sessionID, draft] of [
			["late-draft", turn(fauxToolCall("read", { path: "notes.txt" }), 250)],
			["failed-draft", errorTurn("mock stream disconnected")],
		] as const) {
			const result = await runAgent({
				cwd,
				sessionID,
				tools: [delayedRead(cwd, 50)],
				actorTurns: [turn(fauxToolCall("read", { path: "notes.txt" })), turn("done")],
				actorTokensPerSecond: 2_000,
				draftTurns: [draft, turn("no tool")],
				draftTokensPerSecond: 2_000,
				settings: { ...drafterSettings(), predictionTimeoutMs: 100 },
			});

			expect(result.summary).toMatchObject({ actorActions: 1, speculativeHits: 0, actorFallbacks: 1 });
			expect(result.executions.read).toBe(1);
			expect(result.summary.actorExecutionMs).toBeGreaterThanOrEqual(40);
			expect(
				(result.summary.sourceOutcomes.error ?? 0) +
					(result.summary.sourceOutcomes.timeout ?? 0) +
					(result.summary.sourceOutcomes.aborted ?? 0),
			).toBeGreaterThanOrEqual(1);
		}
	});

	it("learns a dynamic action argument from atomic tool output", async () => {
		const cwd = await workspace();
		const files = ["./a.txt", "./b.txt", "./c.txt", "./d.txt", "./target.txt"];
		await Promise.all(files.map((file) => writeFile(path.join(cwd, file), file, "utf8")));
		const patternSettings = { ...PATTERN_AWARE_DEFAULTS, maxFutureGap: 0, minOccurrences: 2 };
		const store = patternStore(cwd, patternSettings);
		for (const [index, file] of files.slice(0, 4).entries()) {
			const sessionID = `atomic-training-${index}`;
			store.observe({
				sessionID,
				turnID: `${sessionID}:bash`,
				tool: "bash",
				input: { command: `discover ${file}` },
				output: { values: [file] },
				outcome: "success",
				durationMs: 1,
			});
			store.observe({
				sessionID,
				turnID: `${sessionID}:read`,
				tool: "read",
				input: { path: file },
				outcome: "success",
				durationMs: 120,
			});
			store.finishSession(sessionID);
		}
		const result = await runAgent({
			cwd,
			sessionID: "atomic-output-evaluation",
			tools: [atomicOutputBash(), delayedRead(cwd, 120)],
			actorTurns: [
				turn(fauxToolCall("bash", { command: "discover ./target.txt" })),
				turn(fauxToolCall("read", { path: "./target.txt" }), 220),
				turn("done"),
			],
			actorTokensPerSecond: 4_000,
			draftTurns: [],
			settings: { ...patternAwareSettings(patternSettings), tools: ["bash", "read"] },
			patternStore: store,
		});
		expect(result.summary).toMatchObject({ actorActions: 2, speculativeHits: 1, actorFallbacks: 1 });
		expect(result.executions).toEqual({ bash: 1, read: 1 });
		expect(result.summary.executionAheadMs).toBeGreaterThanOrEqual(100);
		expect(result.toolLatencyMs.at(-1)).toBeLessThan(40);
	});

	it("prioritizes fresh recurrence evidence under a one-slot scheduler", async () => {
		const cwd = await workspace();
		await writeFile(path.join(cwd, "old.txt"), "old", "utf8");
		const patternSettings: PatternAwareSettings = {
			...PATTERN_AWARE_DEFAULTS,
			beamWidth: 4,
			decayHalfLifeEvents: 64,
			maxContextLength: 1,
			maxFutureGap: 0,
		};
		const store = patternStore(cwd, patternSettings);
		const sessionID = "decayed-recurrence";
		let turnID = 0;
		const observe = (tool: "read" | "find", input: Record<string, unknown>) => {
			store.observe({
				sessionID,
				turnID: `training-${turnID++}`,
				tool,
				input,
				outcome: "success",
				durationMs: 80,
				schemaHash: schemaHash(tool === "read" ? readSchema : findSchema),
			});
		};
		for (let index = 0; index < 32; index++) observe("read", { path: "old.txt" });
		for (let index = 0; index < 256; index++) {
			store.observeTurn();
		}
		observe("read", { path: "old.txt" });
		for (let index = 0; index < 3; index++) observe("find", { pattern: "target" });
		store.observe({
			sessionID,
			turnID: "context-marker",
			tool: "write",
			input: { path: "marker.txt", content: "marker" },
			outcome: "success",
			durationMs: 1,
			learnTarget: false,
		});

		const result = await runAgent({
			cwd,
			sessionID,
			tools: [delayedRead(cwd, 80), delayedStep("find", findSchema, 80)],
			actorTurns: [turn(fauxToolCall("find", { pattern: "target" }), 40), turn("done")],
			actorTokensPerSecond: 4_000,
			draftTurns: [],
			settings: {
				...patternAwareSettings(patternSettings),
				maxConcurrentActions: 1,
				tools: ["read", "find"],
			},
			patternStore: store,
		});

		expect(result.summary).toMatchObject({ actorActions: 1, speculativeHits: 1, actorFallbacks: 0 });
		expect(result.summary.executionAheadMs).toBeGreaterThan(0);
	});

});

type ScriptedTurn = {
	readonly content: string | FauxContentBlock | readonly FauxContentBlock[];
	readonly delayMs: number;
	readonly stopReason: "stop" | "toolUse" | "error";
	readonly errorMessage?: string;
};

type RunAgentInput = {
	readonly cwd: string;
	readonly sessionID: string;
	readonly prompt?: string;
	readonly tools: readonly AgentTool[];
	readonly actorTurns: readonly ScriptedTurn[];
	readonly actorTokensPerSecond: number;
	readonly draftTurns: readonly ScriptedTurn[];
	readonly draftResponses?: readonly FauxResponseStep[];
	readonly draftTokensPerSecond?: number;
	readonly draftTokenSize?: number;
	readonly settings: SpeculativeAgentSettingsInput;
	readonly patternStore?: PatternAwareStore;
	readonly actorPreview?: "call" | "tool";
};

function turn(content: ScriptedTurn["content"], delayMs = 0): ScriptedTurn {
	return { content, delayMs, stopReason: contentHasTool(content) ? "toolUse" : "stop" };
}

function errorTurn(errorMessage: string): ScriptedTurn {
	return { content: "", delayMs: 0, stopReason: "error", errorMessage };
}

function contentHasTool(content: ScriptedTurn["content"]): boolean {
	const blocks = typeof content === "string" ? [] : Array.isArray(content) ? content : [content];
	return blocks.some((block) => block.type === "toolCall");
}

function scriptedResponses(turns: readonly ScriptedTurn[]): FauxResponseStep[] {
	return turns.map((script) => async () => {
		if (script.delayMs > 0) await delay(script.delayMs);
		return fauxAssistantMessage(script.content as string | FauxContentBlock | FauxContentBlock[], {
			stopReason: script.stopReason,
			...(script.errorMessage ? { errorMessage: script.errorMessage } : {}),
		});
	});
}

async function runAgent(input: RunAgentInput) {
	const actor = createFauxCore({
		provider: `actor-${input.sessionID}`,
		models: [{ id: "actor", reasoning: true }],
		tokensPerSecond: input.actorTokensPerSecond,
		tokenSize: { min: 1, max: 1 },
	});
	const drafter = createFauxCore({
		provider: `drafter-${input.sessionID}`,
		models: [{ id: "draft", reasoning: false }],
		tokensPerSecond: input.draftTokensPerSecond ?? 4_000,
		tokenSize: { min: input.draftTokenSize ?? 1, max: input.draftTokenSize ?? 1 },
	});
	actor.setResponses(scriptedResponses(input.actorTurns));
	drafter.setResponses(input.draftResponses ? [...input.draftResponses] : scriptedResponses(input.draftTurns));
	const events: SpeculativeActionEvent<string>[] = [];
	const executions: Record<string, number> = {};
	const toolLatencyMs: number[] = [];
	const measuredTools = input.tools.map(
		(base): AgentTool => ({
			...base,
			execute: async (callID, args, signal, onUpdate) => {
				executions[base.name] = (executions[base.name] ?? 0) + 1;
				return base.execute(callID, args as never, signal, onUpdate as never);
			},
		}),
	);
	const host = createSpeculativeActionHost(input.sessionID, {
		cwd: input.cwd,
		getSettings: () => input.settings,
		draftModel: drafter.getModel(),
		complete: (model, context, options) => drafter.streamSimple(model, context, options).result(),
		preflight: () => true,
		executionWorlds: [fauxRuntimeWorld()],
		...(input.patternStore ? { patternStore: input.patternStore } : {}),
		onEvent: (event) => {
			events.push(event);
		},
	});
	let currentTurnID: string | undefined;
	let lastTurnID: string | undefined;
	let turnSequence = 0;
	const actorTools = measuredTools.map(
		(base): AgentTool => ({
			...base,
			execute: async (callID, args, signal, onUpdate) => {
				const turnID = currentTurnID;
				if (!turnID) throw new Error("Actor tool executed outside a provider turn");
				const intentStartedAt = performance.now();
				const cached = await host.consume(
					{ turnID, id: callID, tool: base.name, args, tools: measuredTools },
					signal,
				);
				if (cached) {
					toolLatencyMs.push(performance.now() - intentStartedAt);
					return cached.result;
				}
				const toolStartedAt = performance.now();
				try {
					const result = await base.execute(callID, args as never, signal, onUpdate as never);
					await host.actual({
						turnID,
						id: callID,
						tool: base.name,
						args,
						tools: measuredTools,
						durationMs: performance.now() - toolStartedAt,
						output: { result, isError: false },
					});
					toolLatencyMs.push(performance.now() - intentStartedAt);
					return result;
				} catch (error) {
					await host.actual({
						turnID,
						id: callID,
						tool: base.name,
						args,
						tools: measuredTools,
						durationMs: performance.now() - toolStartedAt,
						output: {
							result: {
								content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }],
								details: {},
							},
							isError: true,
						},
					});
					throw error;
				}
			},
		}),
	);
	const streamEvents: string[] = [];
	const agent = new Agent({
		streamFn: actor.streamSimple,
		sessionId: input.sessionID,
		initialState: {
			model: actor.getModel(),
			systemPrompt: "Use tools to inspect the workspace, then answer briefly.",
			tools: actorTools,
		},
	});
	const prompt: AgentMessage = {
		role: "user",
		content: input.prompt ?? "Fix the reported issue by inspecting the relevant files.",
		timestamp: Date.now(),
	};
	agent.subscribe(async (event, signal) => {
		if (event.type === "message_update") {
			const update = event.assistantMessageEvent;
			streamEvents.push(update.type);
			if (input.actorPreview === "tool" && currentTurnID && update.type === "toolcall_start") {
				const call = update.partial.content[update.contentIndex];
				if (call?.type === "toolCall")
					await host.previewActorTool({ turnID: currentTurnID, tool: call.name }, signal);
			}
			if (input.actorPreview && currentTurnID && update.type === "toolcall_end") {
				await host.previewActorCall(
					{
						turnID: currentTurnID,
						id: update.toolCall.id,
						tool: update.toolCall.name,
						args: update.toolCall.arguments,
						tools: measuredTools,
					},
					signal,
				);
			}
		}
		if (event.type === "turn_start") {
			currentTurnID = `turn-${++turnSequence}`;
			lastTurnID = currentTurnID;
			await host.startTurn(
				{
					turnID: currentTurnID,
					actorModel: actor.getModel(),
					context: {
						systemPrompt: agent.state.systemPrompt,
						messages: standardMessages(
							turnSequence === 1 ? [...agent.state.messages, prompt] : agent.state.messages,
						),
						tools: measuredTools,
					},
					actorOptions: { signal },
					tools: measuredTools,
				},
				signal,
			);
		}
		if (event.type === "turn_end" && currentTurnID) {
			const turnID = currentTurnID;
			currentTurnID = undefined;
			await host.finishTurn(turnID, false);
		}
		if (event.type === "agent_end" && lastTurnID) await host.finishTurn(lastTurnID, true);
	});

	try {
		await agent.prompt(prompt);
		if (input.settings.enabled !== false) {
			await waitFor(
				() => events.filter((event) => event.type === "actor_action").length === toolLatencyMs.length,
				5_000,
			);
		}
		return {
			events,
			executions,
			streamEvents,
			toolLatencyMs,
			summary: summarizeSpeculativeTrace(events),
		};
	} finally {
		await host.dispose();
	}
}

function fauxRuntimeWorld(): SpeculativeAgentExecutionWorld {
	return {
		id: "faux_runtime",
		scope: "runtime",
		isolation: "runtime_sandbox",
		speculation: {
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			execute: async (context) => {
				const output = {
					result: await context.tool.execute(context.callID, context.args as never, context.signal),
					isError: false,
				};
				return {
					output,
					backend: "faux_runtime",
					resources: [],
					capturedBytes: 0,
					executionMetrics: {},
					compatibility: {
						status: "compatible",
						backend: "faux_runtime",
						executionFingerprint: context.action.executionFingerprint,
					},
					commit: async () => output,
					dispose: () => {},
				};
			},
		},
	};
}

function standardMessages(messages: readonly AgentMessage[]): Message[] {
	return messages.filter(
		(message): message is Extract<AgentMessage, { role: "user" | "assistant" | "toolResult" }> =>
			message.role === "user" || message.role === "assistant" || message.role === "toolResult",
	) as Message[];
}

function delayedRead(cwd: string, durationMs: number | ((file: string) => number)): AgentTool<typeof readSchema> {
	return {
		name: "read",
		label: "read",
		description: "Read a workspace file",
		parameters: readSchema,
		execute: async (_callID, args, signal) => {
			await delay(typeof durationMs === "number" ? durationMs : durationMs(args.path), signal);
			return textResult(await readFile(path.join(cwd, args.path), "utf8"));
		},
	};
}

function delayedStep(name: "find" | "ls", parameters: AgentTool["parameters"], durationMs: number): AgentTool {
	return {
		name,
		label: name,
		description: `Run ${name}`,
		parameters,
		execute: async (_callID, _args, signal) => {
			await delay(durationMs, signal);
			return textResult(name);
		},
	};
}

function atomicOutputBash(): AgentTool<typeof bashSchema> {
	return {
		name: "bash",
		label: "bash",
		description: "Discover a workspace path",
		parameters: bashSchema,
		execute: async (_callID, args) => ({
			content: [{ type: "text", text: args.command.split(/\s+/).at(-1) ?? "" }],
			details: undefined,
		}),
	};
}

function textResult(text: string): AgentToolResult<unknown> {
	return { content: [{ type: "text", text }], details: {} };
}

function drafterSettings(): SpeculativeAgentSettingsInput {
	return {
		enabled: true,
		drafterEnabled: true,
		candidateLimit: 1,
		maxConcurrentActions: 1,
		predictionTimeoutMs: 1_000,
		patternAware: { enabled: false },
		tools: ["read"],
	};
}

function patternStore(cwd: string, settings: PatternAwareSettings): PatternAwareStore {
	return new PatternAwareStore(settings, undefined, {
		namespace: "pi-action-semantics-v1",
		actionKey: (tool, input, schemaHash) => PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, schemaHash),
		projectors: [],
	});
}

function patternAwareSettings(patternAware: PatternAwareSettings): SpeculativeAgentSettingsInput {
	return {
		enabled: true,
		drafterEnabled: false,
		candidateLimit: 1,
		maxConcurrentActions: 4,
		predictionTimeoutMs: 1_000,
		patternAware,
		tools: ["grep", "read"],
	};
}

function schemaHash(value: unknown): string {
	return createHash("sha256")
		.update(JSON.stringify(stableValue(value)))
		.digest("hex")
		.slice(0, 32);
}

function stableValue(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(stableValue);
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>)
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([key, item]) => [key, stableValue(item)]),
	);
}

async function workspace(): Promise<string> {
	const cwd = await mkdtemp(path.join(os.tmpdir(), "pi-spec-faux-e2e-"));
	roots.push(cwd);
	await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\nthree\n", "utf8");
	return cwd;
}

function delay(durationMs: number, signal?: AbortSignal): Promise<void> {
	if (signal?.aborted) return Promise.reject(signal.reason ?? new Error("aborted"));
	return new Promise((resolve, reject) => {
		const onAbort = () => {
			clearTimeout(timer);
			reject(signal?.reason ?? new Error("aborted"));
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve();
		}, durationMs);
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for end-to-end settlement");
		await delay(5);
	}
}
