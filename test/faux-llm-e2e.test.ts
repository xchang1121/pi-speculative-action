import { textResult } from "./result.ts";
import { gated, deferred as barrier } from "./async.ts";
import { testBranch } from "./branch.ts";
import { readFile, writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import path from "node:path";
import { Agent, type AgentMessage, type AgentTool } from "@earendil-works/pi-agent-core";
import { createFauxCore, type FauxContentBlock, type FauxResponseStep, fauxAssistantMessage, fauxThinking, fauxToolCall, type Message } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import type { SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "../src/effect-model.ts";
import { PATTERN_AWARE_DEFAULTS, type PatternAwareSettings, PatternAwareStore, projectPatternAwareObservation } from "../src/pattern-aware.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { stableValueHash } from "../src/stable-value-hash.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";

const directories = temporaryDirectories("pi-spec-faux-e2e-");
const readSchema = Type.Object({ path: Type.String() });

afterEach(directories.dispose);

describe("faux LLM speculative action end to end", () => {
	it("keeps adopting fragmented Actor and Drafter streams without uncensored Actor samples", async () => {
		const cwd = await workspace(), ready = Array.from({ length: 5 }, barrier);
		const calls = ready.map((_, index) => fauxToolCall("read", { path: `${index}.txt` }));
		await Promise.all(ready.map((_, index) => writeFile(path.join(cwd, `${index}.txt`), "one\ntwo\nthree\n")));
		const result = await runAgent({
			cwd, sessionID: "completed-hit", tools: [fileRead(cwd)], settings: { ...drafterSettings(), drafterMaxDepth: 0 },
			actorTurns: [...calls.map((call, index) => turn([fauxThinking("inspect the file before answering"), call], ready[index]!.promise)), turn("done")],
			draftTurns: [...calls.map((call) => turn(call)), turn("no tool")],
			onEvent: (event) => {
				if ((event.type === "candidate" && event.state.status === "succeeded") ||
					(event.type === "source_request" && event.request.settlement.status === "empty")) ready[Number(event.turnID.slice(5)) - 1]?.resolve();
			},
		});
		expect(result.streamEvents).toEqual(expect.arrayContaining(["thinking_delta", "toolcall_delta"]));
		expect(result.summary).toMatchObject({ tasks: 1, actorActions: 5, speculativeHits: 5, actorFallbacks: 0 });
		expect(result.executions).toEqual({ read: 5 });
		expect(result.actorFallbacks).toEqual([]);
		expect(result.outputs).toEqual(calls.map(() => textResult("one\ntwo\nthree\n")));
		const phases = result.events.filter((event) => event.type === "candidate").map((event) => event.state.status);
		expect(phases).toEqual(calls.flatMap(() => ["running", "succeeded"]));
		expect(result.summary.serializedMs - result.summary.endToEndMs).toBeCloseTo(result.summary.hiddenLatencyMs);
	});

	it("joins a running parent and adopts its completed follow-up without re-execution", async () => {
		const cwd = await workspace();
		await writeFile(path.join(cwd, "target.txt"), "target", "utf8");
		for (const drafterMaxDepth of [0, 1]) {
			const producerGate = gated(), childReady = barrier(), order: string[] = [];
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
					order.push("producer started");
					await producerGate.wait();
					order.push("producer released");
				})],
				actorTurns: [turn(fauxToolCall("read", { path: "notes.txt" }), producerGate.entered),
					turn(fauxToolCall("read", { path: "target.txt" }), drafterMaxDepth ? childReady.promise : undefined), turn("done")],
				draftTurns: Array.from({ length: 5 }, () => respond),
				onActorActionMaterialized: (action) => {
					if (action.input.path === "notes.txt") { order.push("Actor arrived"); producerGate.release(); }
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
			expect(result.draftFeedback[0]).toMatchObject({ kind: "drafter_plan", utility: { benefitMs: undefined } });
			if (drafterMaxDepth) expect(result.draftFeedback[1]).toMatchObject({ kind: "drafter_plan", depth: 1, utility: { benefitMs: undefined } });
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

	it.each(["actor", "peer"] as const)("binds a dynamic next step from an %s batch without premature learning", async (origin) => {
		const cwd = await workspace(), ready = barrier();
		let historyBeforeChild: ReturnType<PatternAwareStore["recent"]> | undefined;
		const searchInputs = [{ path: "caf\u00e9" }, { path: "cafe\u0301" }], target = "./a-held.txt";
		const ranked = (paths: string[]) => ({ ...textResult(paths.join("\n")), details: undefined });
		const decoy = ranked(["./z-unused.txt", "./a-unused.txt"]);
		const files = ["./z-a.txt", "./z-b.txt", "./z-c.txt", "./z-d.txt", target];
		await Promise.all([...files, "./a-unused.txt", "./z-unused.txt"].map((file) => writeFile(path.join(cwd, file), file, "utf8")));
		const settings = { ...PATTERN_AWARE_DEFAULTS, maxFutureGap: 0, minOccurrences: 2 };
		const store = patternStore(cwd, settings);
		for (const [index, file] of files.slice(0, 4).entries()) {
			const sessionID = "atomic-training-" + index;
			const batch = searchInputs.map((input, sibling) => ({ sessionID, turnID: sessionID + ":ls", tool: "ls", input,
				...projectPatternAwareObservation(sibling ? decoy : ranked([file, "./a-unused.txt"])), outcome: "success" as const, durationMs: 1 }));
			store.observeBatch(index % 2 ? batch.reverse() : batch);
			store.observe({ sessionID, turnID: sessionID + ":read", tool: "read", input: { path: file }, outcome: "success", durationMs: 120 });
			store.finishSession(sessionID);
		}
		const discover: AgentTool<typeof readSchema> = {
			name: "ls", label: "discover", description: "Return ranked workspace paths", parameters: readSchema,
			execute: async (_id, args) => args.path === searchInputs[0]!.path ? ranked([target, "./z-unused.txt"]) : decoy,
		};
		const parent = [...searchInputs].reverse().map((input) => fauxToolCall("ls", input));
		const result = await runAgent({
			cwd, sessionID: "atomic-output", patternStore: store,
			settings: { ...drafterSettings(), drafterEnabled: origin === "peer", drafterMaxDepth: 0,
				patternAware: settings, tools: ["ls", "read"] },
			tools: [discover, fileRead(cwd)],
			actorTurns: [turn(parent, origin === "peer" ? ready.promise : undefined),
				turn(fauxToolCall("read", { path: target }), ready.promise), turn("done")],
			draftTurns: [turn(parent), turn("no tool"), turn("no tool")],
			onEvent: (event) => {
				if (event.type === "candidate" && event.candidate.tool === "read" && event.state.status === "succeeded") {
					historyBeforeChild = store.recent("atomic-output");
					ready.resolve();
				}
			},
		});
		expect(result.summary).toMatchObject({ actorActions: 3, speculativeHits: origin === "peer" ? 3 : 1, actorFallbacks: origin === "peer" ? 0 : 2 });
		expect(result.executions).toEqual({ ls: 2, read: 1 });
		expect(result.actorFallbacks).toEqual(origin === "peer" ? [] : ["ls", "ls"]);
		expect(result.outputs).toEqual([decoy, ranked([target, "./z-unused.txt"]), textResult(target)]);
		const adopted = result.events.flatMap((event) => event.type === "prediction" && event.settlement.observation === "observed" &&
			event.settlement.match.matched && event.settlement.match.adoption.status === "adopted" ? [event.settlement.prediction.source] : []);
		expect(adopted).toEqual(origin === "peer" ? ["drafter", "drafter", "pattern_aware"] : ["pattern_aware"]);
		if (origin === "peer") {
			expect(historyBeforeChild).toEqual([]);
			expect(result.events.find((event) => event.type === "candidate" && event.candidate.source === "pattern_aware"))
				.toMatchObject({ candidate: { depth: 2 } });
		}
	});

	it.each(["ls", "read"] as const)("prioritizes fresh %s recurrence evidence under a one-slot scheduler", async (tool) => {
		const cwd = await workspace(), ready = barrier();
		await writeFile(path.join(cwd, "old.txt"), "old", "utf8");
		const fresh = tool === "read" ? { path: "notes.txt" } : { path: "." };
		const settings: PatternAwareSettings = { ...PATTERN_AWARE_DEFAULTS,
			beamWidth: 1, decayHalfLifeEvents: 64, maxContextLength: 1, maxFutureGap: 0 };
		const store = patternStore(cwd, settings), sessionID = "decayed-recurrence";
		let sequence = 0;
		const observe = (tool: "read" | "ls", input: Record<string, unknown>) => store.observe({
			sessionID, turnID: "training-" + sequence++, tool, input, outcome: "success", durationMs: 80, schemaHash: stableValueHash(readSchema),
		});
		for (let index = 0; index < 32; index++) observe("read", { path: "old.txt" });
		for (let index = 0; index < 256; index++) store.observeTurn();
		observe("read", { path: "old.txt" });
		for (let index = 0; index < 3; index++) observe(tool, fresh);
		store.observe({ sessionID, turnID: "context-marker", tool: "write", input: { path: "marker.txt", content: "marker" },
			outcome: "success", durationMs: 1, learnTarget: false });
		const result = await runAgent({
			cwd, sessionID, patternStore: store,
			settings: { ...drafterSettings(), drafterEnabled: false, patternAware: settings, tools: ["read", "ls"] },
			tools: [fileRead(cwd), { name: "ls", label: "ls", description: "List a fixture", parameters: readSchema, execute: async () => textResult("ls") }],
			actorTurns: [turn(fauxToolCall(tool, fresh), ready.promise), turn("done")],
			onEvent: (event) => { if (event.type === "candidate" && event.candidate.tool === tool && event.state.status === "succeeded") ready.resolve(); },
		});
		expect(result.summary).toMatchObject({ actorActions: 1, speculativeHits: 1, actorFallbacks: 0 });
		expect(result.events.find((event) => event.type === "candidate" && event.state.status === "running")).toMatchObject({ candidate: { tool } });
		expect(result.executions[tool]).toBe(1);
		expect(result.actorFallbacks).toEqual([]);
		expect(result.outputs).toEqual([textResult(tool === "read" ? "one\ntwo\nthree\n" : "ls")]);
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
	const executions: Record<string, number> = {};
	const draftFeedback: unknown[] = [];
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
		onActorActionSettled: ({ candidateFeedback }) => { draftFeedback.push(candidateFeedback); },
		onEvent: (event) => { events.push(event); return input.onEvent?.(event); },
	});
	let currentTurnID: string | undefined, lastTurnID: string | undefined, sequence = 0;
	const actorTools = measuredTools.map((base): AgentTool => ({
		...base,
		execute: async (callID, args, signal, onUpdate) => {
			if (!currentTurnID) throw new Error("Actor tool executed outside a provider turn");
			return host.execute({ turnID: currentTurnID, id: callID, tool: base.name, args, tools: measuredTools }, signal, () => {
				actorFallbacks.push(base.name);
				return base.execute(callID, args as never, signal, onUpdate as never);
			});
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
	const outputs = agent.state.messages.flatMap(message => message.role === "toolResult"
		? [{ content: message.content, details: message.details }] : []);
	return { events, executions, streamEvents, actorFallbacks, outputs, draftFeedback, summary: summarizeSpeculativeTrace(events) };
}

function fauxRuntimeWorld(): SpeculativeAgentExecutionWorld {
	return {
		id: "faux_runtime", scope: "runtime", isolation: "runtime_sandbox",
		speculation: {
			capabilities: RESOURCE_OBSERVATION_EFFECTS.capabilities,
			execute: async (context) => {
				const output = { result: await context.tool.execute(context.callID, context.args as never, context.signal), isError: false };
				return testBranch(output, { backend: "faux_runtime", executionFingerprint: context.action.executionFingerprint,
					validate: async () => ({ status: "valid", metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } }), // Scripted fixture inputs stay immutable.
				});
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

function drafterSettings(): SpeculativeAgentSettingsInput {
	return { enabled: true, drafterEnabled: true, candidateLimit: 1, maxConcurrentActions: 1,
		predictionTimeoutMs: 1_000, patternAware: { enabled: false }, tools: ["read"] };
}

function patternStore(cwd: string, settings: PatternAwareSettings): PatternAwareStore {
	return new PatternAwareStore(settings, undefined, { namespace: "pi-action-semantics",
		actionKey: (tool, input, schemaHash) => PI_ACTION_SEMANTICS.buildKey(tool, input, cwd, schemaHash), projectors: [] });
}

async function workspace(): Promise<string> {
	const cwd = await directories.create();
	await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\nthree\n", "utf8");
	return cwd;
}
