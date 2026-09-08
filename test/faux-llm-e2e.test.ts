import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Agent, type AgentMessage, type AgentTool, type AgentToolResult } from "@earendil-works/pi-agent-core";
import { createFauxCore, type FauxContentBlock, type FauxResponseStep, fauxAssistantMessage, fauxThinking, fauxToolCall, type Message } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "../src/effect-model.ts";
import { PATTERN_AWARE_DEFAULTS, type PatternAwareSettings, PatternAwareStore } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { stableValueHash } from "../src/stable-value-hash.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";

const roots: string[] = [];
const readSchema = Type.Object({ path: Type.String() });
const bashSchema = Type.Object({ command: Type.String() });

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("faux LLM speculative action end to end", () => {
	it("adopts a completed result across fragmented Actor and Drafter streams", async () => {
		const cwd = await workspace(), ready = barrier();
		const result = await runAgent({
			cwd, sessionID: "completed-hit", tools: [fileRead(cwd)], settings: drafterSettings(),
			actorTurns: [turn([fauxThinking("inspect the file before answering"), fauxToolCall("read", { path: "notes.txt" })], ready.promise), turn("done")],
			draftTurns: [turn(fauxToolCall("read", { path: "notes.txt" })), turn("no tool")],
			onEvent: (event) => { if (event.type === "candidate" && event.state.status === "succeeded") ready.resolve(); },
		});
		expect(result.streamEvents).toEqual(expect.arrayContaining(["thinking_delta", "toolcall_delta"]));
		expect(result.summary).toMatchObject({ tasks: 1, actorActions: 1, speculativeHits: 1, actorFallbacks: 0 });
		expect(result.executions).toEqual({ read: 1 });
		expect(result.actorFallbacks).toEqual([]);
		expect(result.outputs).toEqual([textResult("one\ntwo\nthree\n")]);
		const phases = result.events.filter((event) => event.type === "candidate").map((event) => event.state.status);
		expect(phases).toEqual(["running", "succeeded"]);
		expect(result.summary.serializedMs - result.summary.endToEndMs).toBeCloseTo(result.summary.hiddenLatencyMs);
	});

	it("joins a running parent and adopts its completed follow-up without re-execution", async () => {
		const cwd = await workspace();
		await writeFile(path.join(cwd, "target.txt"), "target", "utf8");
		for (const drafterMaxDepth of [0, 1]) {
			const started = barrier(), arrived = barrier(), childReady = barrier(), order: string[] = [];
			const sessionID = "in-flight-" + drafterMaxDepth;
			const respond: FauxResponseStep = (context) => {
				const ownDraft = context.messages.some((message) => message.role === "assistant" && message.provider === "drafter-" + sessionID);
				const hasResult = context.messages.some((message) => message.role === "toolResult");
				return fauxAssistantMessage(!hasResult ? fauxToolCall("read", { path: "notes.txt" })
					: ownDraft ? fauxToolCall("read", { path: "target.txt" }) : "no tool",
					{ stopReason: !hasResult || ownDraft ? "toolUse" : "stop" });
			};
			const result = await runAgent({
				cwd, sessionID, settings: { ...drafterSettings(), drafterMaxDepth },
				tools: [fileRead(cwd, async (file) => {
					if (file !== "notes.txt") return;
					order.push("producer started"); started.resolve();
					await arrived.promise;
					order.push("producer released");
				})],
				actorTurns: [turn(fauxToolCall("read", { path: "notes.txt" }), started.promise),
					turn(fauxToolCall("read", { path: "target.txt" }), drafterMaxDepth ? childReady.promise : undefined), turn("done")],
				draftTurns: Array.from({ length: 5 }, () => respond),
				onActorActionMaterialized: (action) => {
					if (action.input.path === "notes.txt") { order.push("Actor arrived"); arrived.resolve(); }
				},
				onEvent: (event) => {
					if (event.type === "candidate" && event.candidate.depth === 1 && event.state.status === "succeeded") childReady.resolve();
				},
			});
			expect(order).toEqual(["producer started", "Actor arrived", "producer released"]);
			expect(result.summary).toMatchObject({ actorActions: 2, speculativeHits: 1 + drafterMaxDepth, actorFallbacks: 1 - drafterMaxDepth });
			expect(result.executions).toEqual({ read: 2 });
			expect(result.actorFallbacks).toEqual(drafterMaxDepth ? [] : ["read"]);
			expect(result.outputs).toEqual([textResult("one\ntwo\nthree\n"), textResult("target")]);
		}
	});

	it("falls back once after a late draft, failed draft, or failed candidate", async () => {
		const cwd = await workspace();
		for (const mode of ["late", "draft error", "candidate error"]) {
			const ready = barrier();
			let attempts = 0;
			const result = await runAgent({
				cwd, sessionID: mode, settings: drafterSettings(),
				tools: [fileRead(cwd, async () => {
					if (++attempts === 1 && mode === "candidate error") throw new Error("speculation failed");
				})],
				actorTurns: [turn(fauxToolCall("read", { path: "notes.txt" }), mode === "late" ? undefined : ready.promise), turn("done")],
				draftTurns: [mode === "draft error"
					? () => fauxAssistantMessage("", { stopReason: "error", errorMessage: "mock stream disconnected" })
					: turn(fauxToolCall("read", { path: "notes.txt" }), mode === "late" ? ready.promise : undefined), turn("no tool")],
				onEvent: (event) => {
					if ((mode === "late" && event.type === "actor_action") || (mode === "draft error" && event.type === "source_request") ||
						(mode === "candidate error" && event.type === "candidate" && event.state.status === "failed")) ready.resolve();
				},
			});
			expect(result.summary).toMatchObject({ actorActions: 1, speculativeHits: 0, actorFallbacks: 1 });
			expect(result.actorFallbacks).toEqual(["read"]);
			expect(result.executions.read).toBe(mode === "candidate error" ? 2 : 1);
			expect(result.outputs).toEqual([textResult("one\ntwo\nthree\n")]);
			if (mode === "draft error") expect(result.summary.sourceOutcomes.error).toBeGreaterThanOrEqual(1);
			if (mode === "candidate error") expect(result.events).toEqual(expect.arrayContaining([
				expect.objectContaining({ type: "candidate", state: expect.objectContaining({ status: "failed" }) }),
			]));
		}
	});

	it("learns a dynamic action argument from authoritative tool output", async () => {
		const cwd = await workspace(), ready = barrier();
		const files = ["./a.txt", "./b.txt", "./c.txt", "./d.txt", "./target.txt"];
		await Promise.all(files.map((file) => writeFile(path.join(cwd, file), file, "utf8")));
		const settings = { ...PATTERN_AWARE_DEFAULTS, maxFutureGap: 0, minOccurrences: 2 };
		const store = patternStore(cwd, settings);
		for (const [index, file] of files.slice(0, 4).entries()) {
			const sessionID = "atomic-training-" + index;
			store.observe({ sessionID, turnID: sessionID + ":bash", tool: "bash", input: { command: "discover " + file },
				output: { values: [file] }, outcome: "success", durationMs: 1 });
			store.observe({ sessionID, turnID: sessionID + ":read", tool: "read", input: { path: file }, outcome: "success", durationMs: 120 });
			store.finishSession(sessionID);
		}
		const discover: AgentTool<typeof bashSchema> = {
			name: "bash", label: "discover", description: "Return the discovered workspace path", parameters: bashSchema,
			execute: async (_id, args) => ({ content: [{ type: "text", text: args.command.split(/\s+/).at(-1) ?? "" }], details: undefined }),
		};
		const result = await runAgent({
			cwd, sessionID: "atomic-output", patternStore: store,
			settings: { ...drafterSettings(), drafterEnabled: false, patternAware: settings, tools: ["bash", "read"] },
			tools: [discover, fileRead(cwd)],
			actorTurns: [turn(fauxToolCall("bash", { command: "discover ./target.txt" })),
				turn(fauxToolCall("read", { path: "./target.txt" }), ready.promise), turn("done")],
			onEvent: (event) => { if (event.type === "candidate" && event.candidate.tool === "read" && event.state.status === "succeeded") ready.resolve(); },
		});
		expect(result.summary).toMatchObject({ actorActions: 2, speculativeHits: 1, actorFallbacks: 1 });
		expect(result.executions).toEqual({ bash: 1, read: 1 });
		expect(result.actorFallbacks).toEqual(["bash"]);
		expect(result.outputs.at(-1)).toEqual(textResult("./target.txt"));
	});

	it("prioritizes fresh recurrence evidence under a one-slot scheduler", async () => {
		const cwd = await workspace(), ready = barrier();
		await writeFile(path.join(cwd, "old.txt"), "old", "utf8");
		const settings: PatternAwareSettings = { ...PATTERN_AWARE_DEFAULTS,
			beamWidth: 4, decayHalfLifeEvents: 64, maxContextLength: 1, maxFutureGap: 0 };
		const store = patternStore(cwd, settings), sessionID = "decayed-recurrence";
		let sequence = 0;
		const observe = (tool: "read" | "ls", input: Record<string, unknown>) => store.observe({
			sessionID, turnID: "training-" + sequence++, tool, input, outcome: "success", durationMs: 80, schemaHash: stableValueHash(readSchema),
		});
		for (let index = 0; index < 32; index++) observe("read", { path: "old.txt" });
		for (let index = 0; index < 256; index++) store.observeTurn();
		observe("read", { path: "old.txt" });
		for (let index = 0; index < 3; index++) observe("ls", { path: "." });
		store.observe({ sessionID, turnID: "context-marker", tool: "write", input: { path: "marker.txt", content: "marker" },
			outcome: "success", durationMs: 1, learnTarget: false });
		const result = await runAgent({
			cwd, sessionID, patternStore: store,
			settings: { ...drafterSettings(), drafterEnabled: false, patternAware: settings, tools: ["read", "ls"] },
			tools: [fileRead(cwd), { name: "ls", label: "ls", description: "List a fixture", parameters: readSchema, execute: async () => textResult("ls") }],
			actorTurns: [turn(fauxToolCall("ls", { path: "." }), ready.promise), turn("done")],
			onEvent: (event) => { if (event.type === "candidate" && event.candidate.tool === "ls" && event.state.status === "succeeded") ready.resolve(); },
		});
		expect(result.summary).toMatchObject({ actorActions: 1, speculativeHits: 1, actorFallbacks: 0 });
		expect(result.events.find((event) => event.type === "candidate" && event.state.status === "running")).toMatchObject({ candidate: { tool: "ls" } });
		expect(result.actorFallbacks).toEqual([]);
		expect(result.outputs).toEqual([textResult("ls")]);
	});
});

type RunAgentInput = Pick<CreateSpeculativeActionHostOptions, "onEvent" | "onActorActionMaterialized"> & {
	readonly cwd: string;
	readonly sessionID: string;
	readonly tools: readonly AgentTool[];
	readonly actorTurns: readonly FauxResponseStep[];
	readonly draftTurns?: readonly FauxResponseStep[];
	readonly settings: SpeculativeAgentSettingsInput;
	readonly patternStore?: PatternAwareStore;
};

function turn(content: string | FauxContentBlock | FauxContentBlock[], before?: Promise<void>): FauxResponseStep {
	return async () => {
		await before;
		const hasTool = typeof content !== "string" && (Array.isArray(content) ? content : [content]).some((block) => block.type === "toolCall");
		return fauxAssistantMessage(content, { stopReason: hasTool ? "toolUse" : "stop" });
	};
}

async function runAgent(input: RunAgentInput) {
	const actor = createFauxCore({ provider: "actor-" + input.sessionID,
		models: [{ id: "actor", reasoning: true }], tokensPerSecond: 4_000, tokenSize: { min: 1, max: 1 } });
	const drafter = createFauxCore({ provider: "drafter-" + input.sessionID,
		models: [{ id: "draft", reasoning: false }], tokensPerSecond: 4_000, tokenSize: { min: 1, max: 1 } });
	actor.setResponses([...input.actorTurns]);
	drafter.setResponses([...(input.draftTurns ?? [])]);
	const events: SpeculativeActionEvent<string>[] = [], streamEvents: string[] = [], actorFallbacks: string[] = [];
	const executions: Record<string, number> = {}, outputs: AgentToolResult<unknown>[] = [];
	const measuredTools = input.tools.map((base): AgentTool => ({
		...base,
		execute: async (callID, args, signal, onUpdate) => {
			executions[base.name] = (executions[base.name] ?? 0) + 1;
			return base.execute(callID, args as never, signal, onUpdate as never);
		},
	}));
	const host = createSpeculativeActionHost(input.sessionID, {
		cwd: input.cwd, getSettings: () => input.settings, draftModel: drafter.getModel(),
		complete: (model, context, options) => drafter.streamSimple(model, context, options).result(),
		preflight: () => true, executionWorlds: [fauxRuntimeWorld()], patternStore: input.patternStore,
		onActorActionMaterialized: input.onActorActionMaterialized,
		onEvent: (event) => { events.push(event); return input.onEvent?.(event); },
	});
	let currentTurnID: string | undefined, lastTurnID: string | undefined, sequence = 0;
	const actorTools = measuredTools.map((base): AgentTool => ({
		...base,
		execute: async (callID, args, signal, onUpdate) => {
			if (!currentTurnID) throw new Error("Actor tool executed outside a provider turn");
			const result = await host.execute({ turnID: currentTurnID, id: callID, tool: base.name, args, tools: measuredTools }, signal, () => {
				actorFallbacks.push(base.name);
				return base.execute(callID, args as never, signal, onUpdate as never);
			});
			outputs.push(result);
			return result;
		},
	}));
	const agent = new Agent({ streamFn: actor.streamSimple, sessionId: input.sessionID,
		initialState: { model: actor.getModel(), systemPrompt: "Use tools to inspect the workspace, then answer briefly.", tools: actorTools } });
	const prompt: AgentMessage = { role: "user", content: "Inspect the relevant files.", timestamp: Date.now() };
	agent.subscribe(async (event, signal) => {
		if (event.type === "message_update") streamEvents.push(event.assistantMessageEvent.type);
		if (event.type === "turn_start") {
			currentTurnID = lastTurnID = "turn-" + ++sequence;
			await host.startTurn({ turnID: currentTurnID, actorModel: actor.getModel(),
				context: { systemPrompt: agent.state.systemPrompt,
					messages: standardMessages(sequence === 1 ? [...agent.state.messages, prompt] : agent.state.messages), tools: measuredTools },
				actorOptions: { signal }, tools: measuredTools,
			}, signal);
		}
		if (event.type === "turn_end" && currentTurnID) {
			const turnID = currentTurnID;
			currentTurnID = undefined;
			await host.finishTurn(turnID, false);
		}
		if (event.type === "agent_end" && lastTurnID) await host.finishTurn(lastTurnID, true);
	});
	try { await agent.prompt(prompt); }
	finally { await host.dispose(); } // The real owner drains settlement; the fixture must not poll or reimplement it.
	return { events, executions, streamEvents, actorFallbacks, outputs, summary: summarizeSpeculativeTrace(events) };
}

function fauxRuntimeWorld(): SpeculativeAgentExecutionWorld {
	return {
		id: "faux_runtime", scope: "runtime", isolation: "runtime_sandbox",
		speculation: {
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			execute: async (context) => {
				const output = { result: await context.tool.execute(context.callID, context.args as never, context.signal), isError: false };
				return { output, backend: "faux_runtime", resources: [], capturedBytes: 0, executionMetrics: {},
					compatibility: { status: "compatible", backend: "faux_runtime", executionFingerprint: context.action.executionFingerprint },
					validate: async () => ({ status: "valid", metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } }), // Scripted fixture inputs stay immutable.
					commit: async () => output, dispose: () => {},
				};
			},
		},
	};
}

function standardMessages(messages: readonly AgentMessage[]): Message[] {
	return messages.filter((message) => message.role === "user" || message.role === "assistant" || message.role === "toolResult") as Message[];
}

function fileRead(cwd: string, before?: (file: string) => Promise<void>): AgentTool<typeof readSchema> {
	return {
		name: "read", label: "read", description: "Read a workspace file", parameters: readSchema,
		execute: async (_id, args) => {
			await before?.(args.path);
			return textResult(await readFile(path.join(cwd, args.path), "utf8"));
		},
	};
}

function textResult(text: string): AgentToolResult<unknown> {
	return { content: [{ type: "text", text }], details: {} };
}

function drafterSettings(): SpeculativeAgentSettingsInput {
	return { enabled: true, drafterEnabled: true, candidateLimit: 1, maxConcurrentActions: 1,
		predictionTimeoutMs: 1_000, patternAware: { enabled: false }, tools: ["read"] };
}

function patternStore(cwd: string, settings: PatternAwareSettings): PatternAwareStore {
	return new PatternAwareStore(settings, undefined, { namespace: "pi-action-semantics-v1",
		actionKey: (tool, input, schemaHash) => PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, schemaHash), projectors: [] });
}

async function workspace(): Promise<string> {
	const cwd = await mkdtemp(path.join(os.tmpdir(), "pi-spec-faux-e2e-"));
	roots.push(cwd);
	await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\nthree\n", "utf8");
	return cwd;
}

function barrier() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => { resolve = done; });
	return { promise, resolve };
}
