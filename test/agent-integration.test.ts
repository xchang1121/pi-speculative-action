import { textResult } from "./result.ts";
import { gated, deferred, nextTurn } from "./async.ts";
import { testBranch } from "./branch.ts";
import fs, { writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import { testModel as model } from "./model.ts";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { fauxAssistantMessage, type AssistantMessage, type SimpleStreamOptions, type ThinkingLevel } from "@earendil-works/pi-ai";
import { createBashTool, createEditTool, createFindTool, createGrepTool, createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionSemanticsRegistry, buildPiActionKey, KEYABLE_TOOLS, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { borrowResourceObject, createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost, type CreateSpeculativeActionHostOptions } from "../src/agent-integration.ts";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import { patternAwareActionSemantics, acquirePatternAwareStore, PATTERN_AWARE_DEFAULTS, PatternAwareStore, patternAwareSettings } from "../src/pattern-aware.ts";
import { createPatternPlanSource } from "../src/pattern-plan-source.ts";
import { PI_READ_RANGE_PROJECTION_RULE, withPiProjectionCoverage } from "../src/pi-read-projection.ts";
import { createClosedSearchProfile, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { MaterializedSpeculativeCandidate, SpeculativeActionEvent } from "../src/runtime.ts";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
import type { ToolInvocation, ToolSettlement } from "../src/tool-settlement.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import {
	normalizeSelfSpeculationSettings,
	SELF_SPECULATION_DEFAULTS,
	SelfSpeculationCoordinator,
} from "../src/self-speculation.ts";

const directories = temporaryDirectories("pi-spec-host-");
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

function assistant(content: AssistantMessage["content"], stopReason: AssistantMessage["stopReason"]): AssistantMessage {
	const message = fauxAssistantMessage(content, { stopReason });
	return {
		...message,
		api: "openai-responses",
		provider: "openai",
		model: "mock",
		usage: { ...message.usage, cost: { ...message.usage.cost }, input: 1, output: 1, totalTokens: 2 },
	};
}

function drafterCall(input: Record<string, unknown>, name = "read", id = "draft-1"): AssistantMessage {
	return assistant([{ type: "toolCall", id, name, arguments: input }], "toolUse");
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

function drafterHost(sessionID: string, options: CreateSpeculativeActionHostOptions) {
	const events: SpeculativeActionEvent<string>[] = [];
	const host = createSpeculativeActionHost(sessionID, {
		draftModel: model("draft"), preflight: () => true, ...options,
		onEvent: event => { events.push(event); options.onEvent?.(event); },
	});
	return { host, events };
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

function patternRequest(
	tool: AgentTool,
	patternAware: ReturnType<typeof patternAwareSettings>,
	sessionID = "session",
	schemaHashes: Readonly<Record<string, string>> = {},
) {
	return {
		startInput: { ...startInput(tool), sessionID },
		data: { tools: new Map([["read", tool]]), schemaHashes },
		settings: { ...settings(), resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000, sourceConfig: { patternAware } },
		definitions: [], candidateNames: ["read"], proposalIndex: 0, proposalCount: 1, signal: new AbortController().signal,
	};
}

function patternStoreLease(cwd: string, configuration: ReturnType<typeof patternAwareSettings>) {
	return acquirePatternAwareStore(cwd, configuration, cwd, patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd));
}

async function temporaryWorkspace(base?: string): Promise<string> {
	const root = await directories.create(base);
	await writeFile(path.join(root, "notes.txt"), "one\ntwo\nthree\nfour", "utf8");
	return root;
}

afterEach(async () => {
	vi.restoreAllMocks();
	await directories.dispose();
});

describe("speculative action host", () => {
	it("owns concurrent binding and completion independently of caller IDs", async () => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		await writeFile(path.join(cwd, "other.txt"), "different content");
		for (const ids of ["unique", "duplicate", "absent"]) for (const order of [[0, 1], [1, 0]]) {
			const gates = [0, 1].map(() => ({ entered: deferred(), done: deferred() })), feedback: number[] = [];
			const binding = gated();
			const identities: object[] = [], sameIdentity: boolean[] = [];
			const complete = vi.fn(async () => { throw new Error("unexpected inference"); });
			const host = createSpeculativeActionHost("session", { cwd, complete, executionWorlds: [],
				resolveInvocation: async (_tool, input) => {
					if ((input as { path: string }).path === "notes.txt") { await binding.wait(); }
					return undefined;
				},
				getSettings: () => ({ ...settings(), drafterEnabled: false }),
				patternStore: new PatternAwareStore({ ...PATTERN_AWARE_DEFAULTS, enabled: false }),
				onActorActionMaterialized: ({ identity }) => { identities.push(identity); },
				onActorActionSettled: ({ settlement }) => {
					feedback.push(settlement.actorAction.sequence);
					sameIdentity.push(identities.includes(settlement.actorAction) && Object.isFrozen(settlement.actorAction));
				} });
			const inputs = ["notes.txt", "other.txt"].map((path) => ({ path })), native = vi.fn();
			const results: ReturnType<typeof host.execute>[] = [];
			try {
				await host.startTurn(startInput(tool));
				for (const [index, args] of inputs.entries()) {
					results.push(host.execute({ turnID: "turn-1", id: ids === "unique" ? String(index) : ids === "duplicate" ? "same" : undefined,
						tool: "read", args, tools: [tool] }, undefined, async (operation) => {
							native(index); const output = await tool.execute(String(index), operation.input as never, operation.signal);
							gates[index]!.entered.resolve(); await gates[index]!.done.promise; return output;
						}));
				}
				await binding.entered;
				const deadline = deferred<boolean>(), timer = setTimeout(() => deadline.resolve(false), 2000);
				try {
					expect(await Promise.race([gates[1]!.entered.promise.then(() => true), deadline.promise])).toBe(true);
				} finally { clearTimeout(timer); }
				expect(native.mock.calls).toEqual([[1]]);
				binding.release(); await Promise.all(gates.map(({ entered }) => entered.promise));
				for (const index of order) {
					gates[index]!.done.resolve();
					expect(await results[index]).toEqual(await tool.execute("oracle", inputs[index]!));
				}
				await host.finishTurn("turn-1", true);
				expect(feedback).toEqual(order.map((index) => index + 1));
				expect(sameIdentity).toEqual([true, true]);
				expect(native.mock.calls).toEqual([[1], [0]]); expect(complete).not.toHaveBeenCalled();
			} finally {
				binding.release();
				for (const gate of gates) gate.done.resolve();
				await Promise.allSettled(results); await host.dispose();
			}
		}
	});

	it("continues complete Drafter batches once within one request slot, preserving reasoning and ordered results", async () => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const message = assistant([{ type: "thinking", thinking: "fixture reasoning", thinkingSignature: "signature" },
			...[1, 2, 3].map((offset) => ({ type: "toolCall" as const, id: `call-${offset}`, name: "read", arguments: { path: "notes.txt", offset, limit: 1 } }))], "toolUse");
		for (const requested of [undefined, "off", "minimal", "low", "medium", "high", "xhigh", "max"] as const) {
			for (const supported of [true, false]) {
				const options: SimpleStreamOptions = Object.freeze({ reasoning: requested === "off" ? undefined : requested ?? "high", maxTokens: 1 });
				const finished = [deferred(), deferred(), deferred()], order: number[] = [];
				const complete = vi.fn<Parameters<typeof createDrafterPlanSource>[0]["complete"]>(async () => message);
				const { host, events } = drafterHost("session", { cwd, complete,
					draftModel: { ...model("draft"), reasoning: supported, thinkingLevelMap: { xhigh: "high", max: "max" } },
					...(requested === undefined ? {} : { getDraftOptions: () => options }),
					getSettings: () => ({ ...settings(), drafterMaxTokens: 128, drafterMaxDepth: 1, maxConcurrentActions: supported ? 1 : 3 }),
					executionWorlds: [mockRuntimeWorld(async (context) => {
						const offset = Number((context.args as { offset: number }).offset);
						if (!supported && offset < 3) await finished[offset]!.promise;
						const result = await tool.execute(context.callID, context.args as never);
						order.push(offset); finished[offset - 1]!.resolve(); return { result, isError: false };
					})],
				});
				try {
					await host.startTurn({ ...startInput(tool), actorOptions: options });
					await waitFor(() => complete.mock.calls.length === 2);
					expect(order).toEqual(supported ? [1, 2, 3] : [3, 2, 1]);
					const requests = complete.mock.calls;
					expect(requests[1]![1].messages[0]).toEqual(message);
					expect(requests[1]![1].messages.slice(1)).toMatchObject(await Promise.all([1, 2, 3].map(async (offset) => ({
						...await tool.execute("oracle", { path: "notes.txt", offset, limit: 1 }), role: "toolResult", toolCallId: `call-${offset}`, isError: false }))));
					const reasoning: ThinkingLevel | undefined = supported && requested !== "off" ? requested : undefined;
					expect(requests.map((request) => request[2])).toMatchObject([{ reasoning, maxTokens: 128, toolChoice: reasoning ? "auto" : "required" }, { reasoning, maxTokens: 128, toolChoice: "auto" }]);
					await host.finishTurn("turn-1", true);
					expect(complete).toHaveBeenCalledTimes(2);
					expect(events.filter((event) => event.type === "source_request" && event.request.request.kind === "continuation")).toHaveLength(1);
					expect(options.maxTokens).toBe(1);
				} finally { for (const gate of finished) gate.resolve(); await host.dispose(); }
			}
		}
	});

	it("prepares active requests and charges late Drafter continuations to their original observation", async () => {
		let now = 0;
		const prepareExecution = vi.fn();
		vi.spyOn(performance, "now").mockImplementation(() => now);
		const tool = createReadTool(await temporaryWorkspace());
		let reply = drafterCall({ path: "notes.txt" });
		const controller = createDrafterPlanSource({ sessionID: "session", complete: async () => {
			now += 100;
			return reply;
		} });
		const request = { startInput: { ...startInput(tool), sessionID: "session" },
			data: { tools: new Map([["read", tool]]), schemaHashes: {}, prepareExecution },
			settings: { ...settings(), resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000 },
			definitions: [], candidateNames: ["read"], proposalIndex: 0, proposalCount: 1, signal: new AbortController().signal };
		const proposal = await controller.source.propose(request);
		if (!proposal || Array.isArray(proposal) || !("actions" in proposal)) throw new Error("missing proposal");
		controller.finishTurn("session", "turn-1");
		await Promise.resolve();
		expect(controller.snapshot()).toMatchObject({ samples: 1, expectedNetBenefitMs: -100 });
		const continuation = { ...request, candidate: { id: "candidate", key: PI_ACTION_SEMANTICS.buildKey("read", { path: "notes.txt" }, "/")!,
			tool: "read", input: { path: "notes.txt" } }, proposalID: proposal.id, actionID: proposal.actions[0]!.id, revision: 1,
			feedback: proposal.actions[0]!.feedback, output: { result: { content: [], details: {} }, isError: false }, trigger: "execution_succeeded" as const };
		if (typeof controller.source.continueOn !== "function") throw new Error("missing batch admission");
		expect(controller.source.continueOn({ ...continuation, trigger: "actor_adopted" })).toBe(false);
		expect(controller.source.continueOn(continuation)).toBe(true);
		await Promise.all([controller.source.continue!(continuation), controller.source.continue!(continuation)]);
		expect(controller.source.continueOn(continuation)).toBe(false);
		expect(controller.snapshot()).toMatchObject({ samples: 1, expectedNetBenefitMs: -200 });
		expect(prepareExecution).toHaveBeenCalledOnce();
		for (let turn = 2; turn <= 5; turn++) {
			const turnID = `turn-${turn}`;
			const next = await controller.source.propose({ ...request, startInput: { ...request.startInput, turnID } });
			expect(Boolean(next)).toBe(turn < 5);
			expect(prepareExecution).toHaveBeenCalledTimes(Math.min(turn, 4));
			controller.finishTurn("session", turnID);
			await Promise.resolve();
		}
		expect(controller.snapshot().skippedBatches).toBe(1);
		controller.finishSession();
		expect(controller.snapshot().samples).toBe(0);
		const valid = reply.content[0]!;
		for (const [content, stopReason] of [
			[[], "stop"], [[valid, valid], "toolUse"],
			[[valid, { type: "toolCall", id: "disabled", name: "bash", arguments: {} }], "toolUse"],
			[[], "error"], [[], "aborted"],
		] as const) {
			reply = assistant([...content], stopReason);
			const proposal = controller.source.propose(request);
			if (stopReason === "error" || stopReason === "aborted") await expect(proposal).rejects.toThrow(`Drafter stopped with ${stopReason}`);
			else expect(await proposal).toBeUndefined();
			expect((prepareExecution.mock.calls.at(-1)![1] as AbortSignal).aborted, "a batch with no usable proposals must retire its warm-up").toBe(true);
			controller.finishSession();
		}
		const fork = createActorForkPlanSource();
		for (const phase of ["request-first", "runtime-first", "skipped", "aborted", "settled", "finished"]) {
			const prepareExecution = vi.fn(), signal = new AbortController();
			fork.startTurn(phase);
			if (phase === "settled" || phase === "finished") {
				fork.startProbe(phase);
				if (phase === "settled") fork.publish(phase, []);
				else fork.finishActorStream(phase);
			}
			if (phase === "request-first") fork.startProbe(phase);
			const proposal = fork.source.propose({ ...request, startInput: { ...request.startInput, turnID: phase },
				data: { ...request.data, prepareExecution }, signal: signal.signal });
			expect(prepareExecution).toHaveBeenCalledTimes(phase === "request-first" ? 1 : 0);
			if (phase === "aborted") signal.abort();
			if (phase !== "skipped") { fork.startProbe(phase); fork.startProbe(phase); }
			expect(prepareExecution).toHaveBeenCalledTimes(phase.endsWith("first") ? 1 : 0);
			fork.publish(phase, []);
			await proposal;
			fork.closeTurn(phase);
		}
	});

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
					return textResult(expected);
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
				const sandbox = resourceExecution
					? createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: [toolName], maxBytes: () => 1024 * 1024 })
					: toolRuntimeWorld();
				const prepareWorld = vi.fn(async (_input: { signal?: AbortSignal }) => {});
				let predictions = origin === "prediction";
				const { host, events } = drafterHost(`session-${turnID}`, {
					cwd,
					getSettings: () => ({ ...settings(), drafterEnabled: predictions, drafterMaxDepth: 0, tools: [toolName] }),
					complete: async () =>
						drafterCall(proposal, toolName, `draft-${toolName}`),
					preflight: (request) => { permissions.push(request); return true; },
					resolveInvocation: () => resourceExecution ? { ...invocation!, filesystem: async (view, request) => {
						await speculativeExecution();
						return resourceExecution(view, request);
					} } : invocation,
					executionWorlds: [{ ...sandbox, speculation: { ...sandbox.speculation!, prepare: prepareWorld } }],
					onEvent: (event) => {
						if (event.type === "candidate" && event.state.status === "succeeded") completed.resolve();
						if (event.type === "actor_action") adopted.resolve();
					},
				});
				try {
					await host.startTurn(startInput({ ...tool, name: "unregistered" }, `${turnID}:without-tool`));
					await host.finishTurn(`${turnID}:without-tool`, true);
					expect(prepareWorld).not.toHaveBeenCalled();
					await host.startTurn({ ...startInput(tool, turnID), tools: resourceExecution ? [tool, writer] : [tool] });
					expect(prepareWorld).not.toHaveBeenCalled();
					if (origin === "preview") for (let repeat = 0; repeat < 2; repeat++) {
						await host.previewActorCall({ turnID, id: `actor-${toolName}`, tool: toolName, args: proposal, tools: [tool] });
					}
					await started.promise;
					expect(prepareWorld).toHaveBeenCalled();
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
						await nextTurn();
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
						const delivered = await result;
						const pristine = structuredClone(delivered);
						delivered.content.push({ type: "text", text: "Actor-owned edit" });
						delivered.details = { actor: true };
						const retained = await host.execute({ turnID, id: "another-owner", tool: toolName, args, tools: [tool] }, undefined, actorExecution);
						expect(retained).toEqual(pristine);
						const query = toolName === "read" ? { path: "notes.txt", offset: 1, limit: 1 } : { path: ".", limit: 1 };
						const narrowed = await host.execute({ turnID, id: "another-view", tool: toolName, args: query, tools: [tool] }, undefined, actorExecution);
						const native = toolName === "read" ? createReadTool(cwd) : createLsTool(cwd);
						expect(narrowed).toEqual(await native.execute("native", query));
						narrowed.content.push({ type: "text", text: "Actor-owned query edit" });
						expect(await host.execute({ turnID, id: "same-view", tool: toolName, args: query, tools: [tool] }, undefined, actorExecution))
							.toEqual(await native.execute("native", query));
						expect(speculativeExecution).toHaveBeenCalledTimes(2); // Re-evaluation uses the sealed inputs, not the host tool.
						expect(actorExecution).not.toHaveBeenCalled();
						for (const changed of [false, true]) {
							const mutation = { path: changed && toolName === "ls" ? "added.txt" : "notes.txt", content: changed || toolName === "ls" ? "first\nchanged" : "one\ntwo\nthree\nfour" };
							await host.execute({ turnID, id: `write:${changed}`, tool: "write", args: mutation, tools: [tool, writer] }, undefined,
								() => writer.execute("native-write", mutation));
							const fallback = vi.fn(() => native.execute("native-read", query));
							const repeated = await host.execute({ turnID, id: `after-write:${changed}`, tool: toolName, args: query, tools: [tool, writer] }, undefined, fallback);
							expect(repeated).toEqual(await native.execute("control", query));
							expect(fallback).toHaveBeenCalledTimes(changed ? 1 : 0);
							if (!changed) expect(speculativeExecution).toHaveBeenCalledTimes(2);
						}
					}
					await host.finishTurn(turnID, true);
					if (origin === "prediction") expect(prepareWorld.mock.calls[0]![0].signal?.aborted).toBe(true);
					predictions = false; prepareWorld.mockClear();
					await host.startTurn(startInput(tool, `${turnID}:without-predictions`));
					await host.finishTurn(`${turnID}:without-predictions`, true);
					expect(prepareWorld).not.toHaveBeenCalled();
				} finally {
					release();
					await host.dispose();
				}
			}
		}
	});

	it.each(["validation", "reader", "opaque", "unproven", "closing"])("owns an output-only projection through %s and Actor settlement", async (phase) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const args = { path: "notes.txt", offset: 2, limit: 1 };
		const expected = await tool.execute("control", args);
		const ready = deferred<void>(), entered = deferred<void>(), release = deferred<void>();
		const worldDisposed = vi.fn(), committed = vi.fn();
		const actor = vi.fn(() => tool.execute("actor", args));
		let offered: ToolSettlement | undefined;
		const rule = { ...PI_READ_RANGE_PROJECTION_RULE, projectOutput: async (input: Parameters<typeof PI_READ_RANGE_PROJECTION_RULE.projectOutput>[0]) => {
			offered ??= PI_READ_RANGE_PROJECTION_RULE.projectOutput(input);
			if (phase === "opaque" && offered) Object.setPrototypeOf(offered, { opaque: true });
			entered.resolve();
			if (phase === "closing") await release.promise;
			return offered;
		} };
		const base = mockRuntimeWorld(async (context) => {
			await new Promise<void>((resolve) => setTimeout(resolve, 5)); // Measured reusable work, not forced admission.
			return { result: withPiProjectionCoverage("read", context.args,
				await tool.execute(context.callID, context.args as never, context.signal)), isError: false };
		}, worldDisposed);
		const world = { ...base, speculation: { ...base.speculation, execute: async (context: Parameters<typeof base.speculation.execute>[0]) => {
			const branch = await base.speculation.execute(context);
			return { ...branch, validate: async () => {
				if (phase === "validation" && offered) offered.result.content.push({ type: "text", text: "provider edit after projection" });
				return branch.validate!();
			}, commit: async () => { committed(); return branch.commit(); } };
		} } };
		const { host, events } = drafterHost("session", {
			cwd, getSettings: () => ({ ...settings(), drafterMaxDepth: 0 }),
			complete: async () => drafterCall({ path: "notes.txt" }),
			projectionRules: phase === "unproven" ? undefined : [rule], executionWorlds: [world],
			onEvent: (event) => { if (event.type === "candidate" && event.state.status === "succeeded") ready.resolve(); },
		});
		try {
			await host.startTurn(startInput(tool));
			await ready.promise;
			const call = { turnID: "turn-1", id: "projected", tool: "read", args, tools: [tool] };
			const delivered = host.execute(call, undefined, actor);
			if (phase === "closing") {
				await entered.promise;
				const closed = host.dispose();
				try {
					await nextTurn();
					expect(worldDisposed).not.toHaveBeenCalled();
				} finally { release.resolve(); await closed; }
			}
			const first = await delivered;
			expect(first.content).toEqual(expected.content);
			if (phase === "reader") {
				first.content.push({ type: "text", text: "Actor edit" });
				const second = await host.execute({ ...call, id: "another-reader" }, undefined, actor);
				expect(second.content).toEqual(expected.content);
			}
			const fallback = phase === "opaque" || phase === "unproven" || phase === "closing";
			expect(actor).toHaveBeenCalledTimes(fallback ? 1 : 0);
			expect(committed).toHaveBeenCalledTimes(fallback ? 0 : 1);
			if (phase === "unproven") {
				await host.finishTurn("turn-1");
				expect(events.find(event => event.type === "prediction")).toMatchObject({ settlement: {
					observation: "observed", match: { matched: true, adoption: { status: "rejected", cause: { code: "coverage_missing" } } },
				} });
			}
			if (!fallback) {
				expect(rule.captureCoverage(PI_ACTION_SEMANTICS.buildKey("read", args, cwd)!, { result: first, isError: false }))
					.toMatchObject({ startLine: 2, endLineExclusive: 3, totalLines: 4 });
				await waitFor(() => events.some((event) => event.type === "actor_action"));
				expect(events.find((event) => event.type === "actor_action")).toMatchObject({ settlement: {
					provider: { kind: "speculative", match: { kind: "projected", projector: "read.range" } },
				} });
			}
		} finally { release.resolve(); await host.dispose(); }
		expect(worldDisposed).toHaveBeenCalledOnce();
	});

	it.for([false, true])("owns composed query proofs across turns and source retirement (oversized=%s)", async (oversized, { skip }) => {
		const cwd = await temporaryWorkspace(), profile = await createClosedSearchProfile(cwd);
		if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg is unavailable"); }
		await writeFile(path.join(cwd, ".ignore"), "# shared selection rules\n");
		const tools: AgentTool[] = [createGrepTool(cwd), createReadTool(cwd), createFindTool(cwd)];
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: tools.map(tool => tool.name), maxBytes: () => 1024 * 1024 });
		const execute = world.speculation!.execute;
		const sources = new Map<string, Awaited<ReturnType<typeof execute>>>();
		vi.spyOn(world.speculation!, "execute").mockImplementation(async context => {
			const original = await execute(context);
			// The query belongs to the grep owner; read supplies only its foreign file input.
			const branch = context.toolName === "read" ? { ...original, inputResources: [{ path: path.join(cwd, "notes.txt") }] } : original;
			sources.set(context.toolName, branch);
			return !oversized ? branch : { ...branch, reconstruct: async request => {
				const query = await branch.reconstruct!(request);
				return query && { ...query, capturedBytes: 2 ** 30 };
			} };
		});
		const ready = deferred();
		let predict = true, completed = 0, evaluations = 0;
		const { host, events } = drafterHost("composed", {
			cwd,
			getSettings: () => ({ ...settings(), drafterEnabled: predict, drafterGateEnabled: false, drafterMaxDepth: 0,
				candidateLimit: 1, maxConcurrentActions: 2, tools: tools.map(tool => tool.name) }),
			complete: async () => assistant([
				{ type: "toolCall", id: "names", name: "grep", arguments: { pattern: "seed", path: ".", glob: "*.absent" } },
				{ type: "toolCall", id: "bytes", name: "read", arguments: { path: "notes.txt", limit: 1 } },
			], "toolUse"),
			resolveInvocation: (tool, input) => {
				const bound = profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} });
				return bound && { ...bound, filesystem: bound.filesystem && ((...args) => { evaluations++; return bound.filesystem!(...args); }) };
			},
			executionWorlds: [world],
			onEvent: event => { if (event.type === "candidate") {
				if (event.state.status === "succeeded" && ++completed === 2) ready.resolve();
				if (event.state.status === "failed" || event.state.status === "cancelled") ready.reject(new Error(JSON.stringify(event.state)));
			} },
		});
		try {
			await host.startTurn({ ...startInput(tools[0]!, "seed"), tools }); await ready.promise;
			predict = false; await host.finishTurn("seed");
			await host.startTurn({ ...startInput(tools[0]!, "query"), tools });
			const original = await fs.readFile(path.join(cwd, "notes.txt"));
			const args = { pattern: "two", path: ".", limit: 1 }, expected = await tools[0]!.execute("reference", args);
			const actor = vi.fn(() => tools[0]!.execute("native", args));
			let call = { turnID: "query", id: "query", tool: "grep", args, tools };
			const warm = { ...args, limit: 2 };
			expect(await host.execute({ ...call, id: "warm-matches", args: warm }, undefined, () => { throw new Error("warm query should reuse inputs"); }))
				.toEqual(await tools[0]!.execute("warm-oracle", warm));
			await host.previewActorCall(call);
			for (const id of ["query", "retained"]) expect(await host.execute({ ...call, id }, undefined, actor)).toEqual(expected);
			expect(actor.mock.calls.length, JSON.stringify(events.filter(event => event.type === "actor_action").map(event => event.settlement))).toBe(0);
			expect(evaluations).toBe(oversized ? 6 : 4);
			await writeFile(path.join(cwd, "notes.txt"), "one\ntwo\nchanged unused line\n");
			expect(await host.execute({ ...call, id: "stale-source" }, undefined, actor)).toEqual(expected);
			expect(actor).not.toHaveBeenCalled();
			await writeFile(path.join(cwd, "notes.txt"), original);
			expect(await host.execute({ ...call, id: "restored-source" }, undefined, actor)).toEqual(expected);
			expect(actor).not.toHaveBeenCalled();
			await host.finishTurn("query"); await host.startTurn({ ...startInput(tools[0]!, "again"), tools });
			call = { ...call, turnID: "again" }; const beforeRetirement = evaluations;
			await sources.get("read")!.dispose();
			expect(await host.execute({ ...call, id: "retired-source" }, undefined, actor)).toEqual(expected);
			expect(actor).not.toHaveBeenCalled();
			if (!oversized) expect(evaluations).toBe(beforeRetirement);
			await writeFile(path.join(cwd, "notes.txt"), "two changed after retirement\n");
			expect(await host.execute({ ...call, id: "changed-after-retirement" }, undefined, actor)).toEqual(await tools[0]!.execute("reference", args));
			expect(actor).not.toHaveBeenCalled();
			const find = { pattern: "notes.txt", path: "." }, findActor = vi.fn(() => tools[2]!.execute("native-find", find));
			expect(await host.execute({ ...call, id: "surviving-source", tool: "find", args: find }, undefined, findActor))
				.toEqual(await tools[2]!.execute("reference-find", find));
			expect(findActor).not.toHaveBeenCalled();
			await host.finishTurn(call.turnID, true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 8, exactReuseHits: 0, predictionsMatched: 0 });
		} finally { await host.dispose(); await profile.pool.dispose(); }
	});

	it.for(["local", "composed", "missing", "cancelled"] as const)("shares one running grep preparation across predictions and Actor queries with independent proofs (%s)", async (mode, { skip }) => {
		const cwd = await temporaryWorkspace(), profile = await createClosedSearchProfile(cwd), original = profile.invocations.get("grep");
		if (!original) { await profile.pool.dispose(); return skip("qualified rg is unavailable"); }
		const tool = createGrepTool(cwd), world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["grep", "read"], maxBytes: () => 1024 * 1024 });
		const joined = deferred<void>(), actorJoined = deferred<void>(), gate = gated(), consuming = gated(), cancelled = new AbortController(), nativeMkdtemp = fs.mkdtemp;
		const invocation: ToolInvocation = { ...original, filesystem: (view, request) => original.filesystem!({ ...view, prepare: (binding, key, build, consume, target) => {
			const pending = view.prepare!(binding, key, build, async value => {
				const result = await consume(value);
				if (target === cwd && mode === "cancelled" && request.callID === "second") await consuming.wait();
				if (target === cwd && mode === "cancelled" && request.callID === "first") {
					await consuming.entered; cancelled.abort(new Error("first consumer cancelled")); throw cancelled.signal.reason;
				}
				return result;
			}, target);
			if (request.callID === "second") joined.resolve(); if (request.callID === "actor") actorJoined.resolve(); return pending;
		} }, request) };
		const context = (args: { pattern: string; path: string; glob?: string }, callID: string) => ({
			cwd, tool, toolName: "grep", args, callID, signal: callID === "first" ? cancelled.signal : new AbortController().signal,
			action: { ...buildPiActionKey("grep", args, cwd)!, semantics: invocation.semantics, executionContext: invocation },
		});
		const seed = await world.speculation!.execute(context({ pattern: "seed", path: ".", ...(mode === "local" ? {} : { glob: "*.absent" }) }, "seed"));
		const readArgs = { path: "notes.txt" }, read = resolvePiToolInvocation("read", readArgs, { cwd, environment: {} })!;
		const payload = mode === "composed" || mode === "cancelled" ? await world.speculation!.execute({ cwd, tool: createReadTool(cwd), toolName: "read", args: readArgs,
			callID: "payload", signal: new AbortController().signal, action: { ...buildPiActionKey("read", readArgs, cwd)!, executionContext: read } }) : undefined;
		const inputs = () => [seed.inputSource!, ...(payload ? [payload.inputSource!] : [])];
		const directories = vi.spyOn(fs, "mkdtemp").mockImplementation(async (prefix, options) => {
			const directory = await nativeMkdtemp(prefix, options);
			if (path.basename(String(prefix)) === "inputs-") await gate.wait();
			return directory;
		});
		let branches: Awaited<ReturnType<NonNullable<typeof world.speculation>["execute"]>>[] = [];
		const args = [{ pattern: "two", path: ".", glob: "notes.txt" }, { pattern: "three", path: ".", glob: "notes.txt" }];
		const first = world.speculation!.execute({ ...context(args[0]!, "first"), inputs });
		await gate.entered;
		const second = world.speculation!.execute({ ...context(args[1]!, "second"), inputs });
		const settled = Promise.allSettled([first, second]);
		const actorArgs = { pattern: "one", path: ".", glob: "notes.txt" };
		const rebuilding = seed.reconstruct!({ ...context(actorArgs, "actor"), inputs });
		let query: Awaited<typeof rebuilding>;
		try {
			await Promise.all([joined.promise, actorJoined.promise]); await nextTurn(); gate.release();
			if (mode === "cancelled") { await expect.poll(() => cancelled.signal.aborted).toBe(true); await nextTurn(); consuming.release(); }
			const attempts = await settled; branches = attempts.flatMap(attempt => attempt.status === "fulfilled" ? [attempt.value] : []);
			query = await rebuilding; expect(query).toBeDefined();
			expect(attempts.map(attempt => attempt.status)).toEqual([mode === "cancelled" ? "rejected" : "fulfilled", "fulfilled"]);
			expect(directories.mock.calls.filter(([prefix]) => path.basename(String(prefix)) === "inputs-")).toHaveLength(1);
			for (const [index, attempt] of attempts.entries()) if (attempt.status === "fulfilled") expect(attempt.value.output.result).toEqual(
				(await original.authoritative!({ args: args[index]!, callID: "oracle", signal: new AbortController().signal })).result);
			expect(query!.output.result).toEqual((await original.authoritative!({ args: actorArgs, callID: "oracle", signal: new AbortController().signal })).result);
			await payload?.dispose(); if (branches.length === 2) await branches[0]!.dispose();
			expect((await branches.at(-1)!.validate!()).status).toBe("valid");
			await branches.at(-1)!.dispose(); expect((await query!.validate!()).status).toBe("valid");
			await writeFile(path.join(cwd, "notes.txt"), "changed"); expect((await query!.validate!()).status).toBe("stale");
		} finally { gate.release(); consuming.release(); await settled; await (await rebuilding)?.dispose?.(); await Promise.all(branches.map(branch => branch.dispose())); await seed.dispose(); await payload?.dispose(); directories.mockRestore(); await profile.pool.dispose(); }
	});

	it.for(["prepared", "composed", "partial"] as const)("borrows inputs during prediction and owns its proof after source retirement (%s)", async (coverage, { skip }) => {
		const partial = coverage === "partial";
		const cwd = await temporaryWorkspace(), profile = await createClosedSearchProfile(cwd);
		if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg is unavailable"); }
		await writeFile(path.join(cwd, "other.txt"), "two from another input\n");
		await writeFile(path.join(cwd, "unused.log"), "original\n");
		const tools: AgentTool[] = [createGrepTool(cwd), createReadTool(cwd), createLsTool(cwd)];
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: tools.map(tool => tool.name), maxBytes: () => 1024 * 1024 });
		const execute = world.speculation!.execute, sources: Awaited<ReturnType<typeof execute>>[] = [];
		let successor: Awaited<ReturnType<typeof execute>> | undefined;
		vi.spyOn(world.speculation!, "execute").mockImplementation(async context => {
			const branch = await execute(context);
			if (context.executionScope?.turnID === "seed") sources.push(branch);
			if (context.executionScope?.turnID === "again") successor = branch;
			return branch;
		});
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture"), opened = vi.spyOn(fs, "open");
		const directories = vi.spyOn(fs, "mkdtemp");
		const preparations = () => directories.mock.calls.filter(([target]) => path.basename(String(target)) === "inputs-").length;
		const ready = (turnID: string, count: number) => expect.poll(() => events.filter(event =>
			event.type === "candidate" && event.turnID === turnID && event.state.status === "succeeded"), { timeout: 5000 }).toHaveLength(count);
		const args = { pattern: "two", path: ".", glob: partial ? "*.txt" : "notes.txt" };
		let stage: "seed" | "query" | "names" = "seed", permitted = true;
		const { host, events } = drafterHost("prediction-inputs", {
			cwd, preflight: () => permitted,
			getSettings: () => ({ ...settings(), drafterGateEnabled: false, drafterMaxDepth: 0, maxConcurrentActions: 2,
				tools: tools.map(tool => tool.name), resourceCacheMaxEntries: 6, resourceCacheMaxBytes: 1024 * 1024 }),
			complete: async () => assistant(stage === "seed" ? [
				{ type: "toolCall", id: "names", name: "grep", arguments: { pattern: "seed", path: ".", glob: coverage === "prepared" ? args.glob : "*.absent" } },
				{ type: "toolCall", id: "bytes", name: "read", arguments: { path: "notes.txt", limit: 1 } },
			] : [{ type: "toolCall", id: "query", name: stage === "names" ? "ls" : "grep", arguments: stage === "names" ? { path: "." } : args }], "toolUse"),
			resolveInvocation: (tool, input) => profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
			executionWorlds: [world],
		});
		try {
			await host.startTurn({ ...startInput(tools[0]!, "seed"), tools }); await ready("seed", 2);
			await host.execute({ turnID: "seed", id: "seed-read", tool: "read", args: { path: "notes.txt", limit: 1 }, tools },
				undefined, () => { throw new Error("seed read should adopt its exact prediction"); });
			await host.finishTurn("seed"); stage = "query";
			const captured = captures.mock.calls.length, reads = opened.mock.calls.length, prepared = preparations();
			await host.startTurn({ ...startInput(tools[0]!, "query"), tools }); await ready("query", 1);
			expect(captures.mock.calls.length - captured).toBe(1); // New regex results need a budgeted owner even when inputs are already prepared.
			expect(preparations() - prepared).toBe(coverage === "prepared" ? 0 : 1);
			const sourceReads = opened.mock.calls.slice(reads).filter(([file]) => String(file) === path.join(cwd, "notes.txt"));
			expect(sourceReads).toHaveLength(0);
			expect(opened.mock.calls.slice(reads).filter(([file]) => String(file) === path.join(cwd, "other.txt"))).toHaveLength(Number(partial));
			for (const source of sources) await source.dispose();
			let call = { turnID: "query", id: "first", tool: "grep", args, tools };
			const current = async () => (await profile.invocations.get("grep")!.authoritative!({ args, callID: "reference", signal: new AbortController().signal })).result;
			const expected = await current(), actor = vi.fn(current);
			expect(await host.execute(call, undefined, actor)).toEqual(expected);
			await writeFile(path.join(cwd, "unused.log"), "unrelated content changed\n");
			expect(await host.execute({ ...call, id: "unrelated" }, undefined, actor)).toEqual(expected);
			expect(actor).not.toHaveBeenCalled();
			if (partial) {
				const input = { path: "other.txt" }, expectedRead = await tools[1]!.execute("reference-read", input);
				const fallback = vi.fn(() => tools[1]!.execute("fallback-read", input)), capturedBefore = captures.mock.calls.length;
				expect(await host.execute({ ...call, id: "cross-root-read", tool: "read", args: input }, undefined, fallback)).toEqual(expectedRead);
				expect(fallback).not.toHaveBeenCalled(); expect(captures.mock.calls.length).toBe(capturedBefore);
			}
			permitted = false;
			expect(await host.execute({ ...call, id: "denied" }, undefined, actor)).toEqual(expected);
			expect(actor).toHaveBeenCalledOnce(); permitted = true;
			if (coverage !== "prepared") {
				let capturesBefore = captures.mock.calls.length; const preparedBefore = preparations();
				args.pattern = "one|another";
				expect(await host.execute({ ...call, id: "reconstructed" }, undefined, actor)).toEqual(await current());
				await host.finishTurn("query"); stage = "names";
				await host.startTurn({ ...startInput(tools[2]!, "names"), tools }); await ready("names", 1);
				const names = { ...call, turnID: "names", id: "newer-names", tool: "ls", args: { path: "." } };
				await host.execute(names, undefined, () => { throw new Error("the names prediction must supply its result"); });
				await host.finishTurn("names"); stage = "query";
				capturesBefore = captures.mock.calls.length;
				args.pattern = "three|another";
				await host.startTurn({ ...startInput(tools[0]!, "again"), tools }); await ready("again", 1);
				call = { ...call, turnID: "again" };
				const validation = await successor!.validate!(); expect(validation.status, JSON.stringify(validation)).toBe("valid");
				expect(await host.execute(call, undefined, actor)).toEqual(await current());
				expect(actor).toHaveBeenCalledOnce();
				expect(captures.mock.calls.length).toBe(capturesBefore + 1); expect(preparations()).toBe(preparedBefore);
			}
			for (const name of partial ? ["notes.txt", "other.txt"] : ["notes.txt"]) {
				const file = path.join(cwd, name), original = await fs.readFile(file), calls = actor.mock.calls.length;
				await writeFile(file, "three changed\n");
				if (successor) expect((await successor.validate!()).status).toBe("stale");
				expect(await host.execute({ ...call, id: "changed-" + name }, undefined, actor)).toEqual(await current());
				expect(actor).toHaveBeenCalledTimes(calls + 1);
				await writeFile(file, original);
				if (coverage !== "prepared") {
					expect(await host.execute({ ...call, id: "restored-" + name }, undefined, actor)).toEqual(await current());
					expect(actor).toHaveBeenCalledTimes(calls + 1);
				}
			}
			await host.finishTurn(call.turnID, true);
			expect(events.filter(event => event.type === "prediction" && event.turnID === "query" && event.settlement.observation === "observed" &&
				event.settlement.match.matched && event.settlement.match.adoption.status === "adopted")).toHaveLength(1);
		} finally { await host.dispose(); await profile.pool.dispose(); captures.mockRestore(); opened.mockRestore(); directories.mockRestore(); }
	});

	it.for(["edit", "write", "add", "delete", "ignore"] as const)("rebuilds only affected grep computations across turns (%s)", async (change, { skip }) => {
		const cwd = await temporaryWorkspace(), profile = await createClosedSearchProfile(cwd);
		if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg is unavailable"); }
		await writeFile(path.join(cwd, "other.txt"), "two unchanged\nTWO upper\n");
		const tools: AgentTool[] = [createGrepTool(cwd), createReadTool(cwd), createWriteTool(cwd)], args = { pattern: "t.o", path: ".", glob: "*.txt" };
		const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["grep", "read"], maxBytes: () => 1024 * 1024 });
		const ready = deferred();
		let predict = true;
		const { host, events } = drafterHost("stale-output-inputs", {
			cwd,
			getSettings: () => ({ ...settings(), drafterEnabled: predict, drafterGateEnabled: false, drafterMaxDepth: 0, tools: ["grep", "read"] }),
			complete: async () => drafterCall(args, "grep", "seed"),
			resolveInvocation: (tool, input) => profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
			executionWorlds: [{ ...world, observation: undefined }], // No fallback capture can replace the original input owner.
			onEvent: event => { if (event.type === "candidate" && event.state.status === "succeeded") ready.resolve(); },
		});
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture"), directories = vi.spyOn(fs, "mkdtemp"), writes = vi.spyOn(fs, "writeFile");
		const matchedFiles = () => writes.mock.calls.filter(([file, bytes]) => path.basename(path.dirname(String(file))).startsWith("matches-") && Buffer.isBuffer(bytes)).length;
		try {
			await host.startTurn({ ...startInput(tools[0]!, "seed"), tools }); await ready.promise;
			expect(matchedFiles()).toBe(2);
			const current = async (query: unknown = args) => (await profile.invocations.get("grep")!.authoritative!({ args: query, callID: "reference", signal: new AbortController().signal })).result;
			const actor = vi.fn(() => current()), call = { turnID: "seed", id: "first", tool: "grep", args, tools };
			expect(await host.execute(call, undefined, actor)).toEqual(await current()); expect(actor).not.toHaveBeenCalled();
			predict = false; await host.finishTurn("seed");
			await host.startTurn({ ...startInput(tools[0]!, "changed"), tools });
			for (const query of [{ ...args, context: 1 }, { ...args, limit: 1 }, { ...args, glob: "other.*" }, { ...args, path: "other.txt" }]) {
				const fallback = vi.fn(() => current(query));
				expect(await host.execute({ ...call, turnID: "changed", id: JSON.stringify(query), args: query }, undefined, fallback)).toEqual(await current(query));
				expect(fallback).not.toHaveBeenCalled(); expect(matchedFiles()).toBe(2);
			}
			for (const query of [{ ...args, ignoreCase: true }, { ...args, literal: true }]) {
				const fallback = vi.fn(() => current(query)), before = matchedFiles();
				expect(await host.execute({ ...call, turnID: "changed", id: JSON.stringify(query), args: query }, undefined, fallback)).toEqual(await current(query));
				expect(fallback).not.toHaveBeenCalled(); expect(matchedFiles()).toBe(before + 2);
			}
			if (change === "write") {
				const input = { path: "notes.txt", content: "two changed\n" };
				await host.execute({ ...call, turnID: "changed", id: "write", tool: "write", args: input }, undefined, () => tools[2]!.execute("write", input));
			} else if (change === "delete") await fs.unlink(path.join(cwd, "notes.txt"));
			else await writeFile(path.join(cwd, change === "add" ? "new.txt" : change === "ignore" ? ".ignore" : "notes.txt"),
				change === "ignore" ? "notes.txt\n" : "two changed\n");
			for (const id of ["changed", "retained-change"])
				expect(await host.execute({ ...call, turnID: "changed", id }, undefined, actor)).toEqual(await current());
			const narrowed = { ...args, context: 2 }, narrow = vi.fn(() => current(narrowed));
			expect(await host.execute({ ...call, turnID: "changed", id: "changed-context", args: narrowed }, undefined, narrow)).toEqual(await current(narrowed));
			expect(narrow).not.toHaveBeenCalled();
			expect(actor.mock.calls.length, JSON.stringify(events.filter(event => event.type === "actor_action").map(event => event.type === "actor_action" && event.settlement.rejections))).toBe(0);
			expect(matchedFiles()).toBe(change === "edit" || change === "write" || change === "add" ? 7 : 6);
			await host.finishTurn("changed"); await host.startTurn({ ...startInput(tools[0]!, "sibling"), tools });
			const input = { path: "other.txt" }, expected = await tools[1]!.execute("reference-read", input);
			const fallback = vi.fn(() => tools[1]!.execute("fallback-read", input));
			const captured = captures.mock.calls.length, prepared = directories.mock.calls.length;
			for (const id of ["sibling", "retained"]) expect(await host.execute({ ...call, turnID: "sibling", tool: "read", args: input, id }, undefined, fallback)).toEqual(expected);
			expect(fallback).not.toHaveBeenCalled();
			expect(captures.mock.calls.length).toBe(captured); expect(directories.mock.calls.length).toBe(prepared);
			await host.finishTurn("sibling", true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ exactReuseHits: 1, inputReuseHits: 11 });
		} finally { await host.dispose(); await profile.pool.dispose(); captures.mockRestore(); directories.mockRestore(); writes.mockRestore(); }
	});

	it.skipIf(process.platform !== "linux")("leases captured file objects to internal Actor operations across turns", async () => {
		const cwd = await temporaryWorkspace(), file = path.join(cwd, "notes.txt"), tools = [createReadTool(cwd), createBashTool(cwd)];
		const base = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 65536 });
		let lookup: ((path: string) => Iterable<object>) | undefined, borrowed: Buffer | undefined, predict = true;
		const world: SpeculativeAgentExecutionWorld = { ...base, speculation: { ...base.speculation!, tools: ["read", "bash"] },
			observeOperations: async ({ action, inputs }, execute) => {
				if (action.tool === "bash") { lookup = inputs; borrowed = (await borrowResourceObject(inputs?.(file) ?? [], file, await fs.stat(file, { bigint: true }), 65536))?.content; }
				return execute();
			} };
		const { host, events } = drafterHost("actor-object-inputs", { cwd,
			getSettings: () => ({ ...settings(), tools: ["read", "bash"], drafterEnabled: predict, drafterGateEnabled: false, drafterMaxDepth: 0,
				resourceCacheMaxEntries: 4, resourceCacheMaxBytes: 65536 }),
			complete: async () => drafterCall({ path: "notes.txt" }, "read", "read"),
			resolveInvocation: (name, input) => resolvePiToolInvocation(name, input, { cwd, environment: {} }),
			executionWorlds: [world] });
		try {
			await host.startTurn({ ...startInput(tools[0]!, "read"), tools });
			await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded"), { timeout: 5000 }).toHaveLength(1);
			await host.execute({ turnID: "read", id: "read", tool: "read", args: { path: "notes.txt" }, tools }, undefined, () => { throw new Error("expected prediction"); });
			await host.finishTurn("read"); predict = false;
			await host.startTurn({ ...startInput(tools[1]!, "bash"), tools });
			await host.execute({ turnID: "bash", id: "bash", tool: "bash", args: { command: "cat notes.txt" }, tools }, undefined, async () => textResult("native"));
			expect(borrowed).toEqual(await fs.readFile(file)); expect(lookup).toBeDefined(); expect([...lookup!(file)]).toEqual([]);
			await host.finishTurn("bash", true);
		} finally { await host.dispose(); }
	});

	it.each(["write", "edit"])("hands Actor %s inputs across turns without rereading or replaying the mutation", async (tool) => {
		const cwd = await temporaryWorkspace(), tools = [createWriteTool(cwd), createEditTool(cwd), createReadTool(cwd)] as const;
		const args = tool === "write" ? { path: "notes.txt", content: "after\nsecond\n" }
			: { path: "notes.txt", edits: [{ oldText: "one", newText: "after" }] };
		const { host, events } = drafterHost("actor-write-inputs", {
			cwd,
			getSettings: () => ({ ...settings(), tools: ["write", "edit", "read"], drafterEnabled: false, resourceCacheMaxEntries: 4, resourceCacheMaxBytes: 65536 }),
			complete: async () => { throw new Error("drafter disabled"); },
			resolveInvocation: (name, input) => resolvePiToolInvocation(name, input, { cwd, environment: {} }),
			executionWorlds: [createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 65536 })],
		});
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture");
		try {
			await host.startTurn({ ...startInput(tools[0]!, "write"), tools });
			const call = { turnID: "write", id: "write", tool, args, tools };
			const execute = vi.fn(async (operation: Parameters<Parameters<typeof host.execute>[2]>[0]) =>
				(await operation.invocation!.authoritative!({ args: operation.input, callID: operation.callID!, signal: operation.signal! })).result);
			await host.execute(call, undefined, execute); expect(execute).toHaveBeenCalledOnce();
			await host.finishTurn("write");
			const captured = captures.mock.calls.length;
			expect(captures.mock.calls.filter(call => call[2] !== undefined)).toHaveLength(1);
			await host.startTurn({ ...startInput(tools[2]!, "read"), tools });
			const query = { path: args.path, offset: 2 }, read = vi.fn(() => tools[2]!.execute("native", query));
			const readCall = { turnID: "read", id: "read", tool: "read", args: query, tools };
			expect(await host.execute(readCall, undefined, read)).toEqual(await tools[2]!.execute("oracle", query));
			expect(read).not.toHaveBeenCalled(); expect(captures.mock.calls.length).toBe(captured);
			await writeFile(path.join(cwd, args.path), "external\nchanged\n");
			expect(await host.execute({ ...readCall, id: "changed" }, undefined, read)).toEqual(await tools[2]!.execute("oracle", query));
			expect(read).toHaveBeenCalledOnce();
			const repeated = host.execute({ ...call, turnID: "read", id: "repeat" }, undefined, execute);
			if (tool === "edit") await expect(repeated).rejects.toThrow(); else await repeated;
			expect(execute).toHaveBeenCalledTimes(2);
			await host.finishTurn("read", true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 1, predictionsMatched: 0 });
		} finally { await host.dispose(); captures.mockRestore(); }
	});

	it("reuses committed write inputs across turns while repeating mutations and rejecting stale reads", async () => {
		const cwd = await temporaryWorkspace(), sandbox = new WorkspaceSandboxService();
		const tools = [createWriteTool(cwd), createReadTool(cwd)], args = { path: "notes.txt", content: "committed\nsecond\n" };
		const world = sandbox.createExecutionWorld({ driver: "git" });
		let predict = true, readNext = false;
		const captures = vi.spyOn(ResourceVersionManager.prototype, "capture");
		const { host, events } = drafterHost("committed-inputs", {
			cwd, getSettings: () => ({ ...settings(), tools: ["write", "read"],
				drafterEnabled: predict, drafterGateEnabled: false, drafterMaxDepth: 0, resourceCacheMaxEntries: 4, resourceCacheMaxBytes: 1024 * 1024 }),
			complete: async () => drafterCall(readNext ? { path: args.path } : args, readNext ? "read" : "write", "next"),
			resolveInvocation: (tool, input) => resolvePiToolInvocation(tool, input, { cwd, environment: {} }),
			executionWorlds: [world, createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 1024 * 1024 })],
		});
		try {
			await host.startTurn({ ...startInput(tools[0]!, "write"), tools });
			await expect.poll(() => events.filter(event => event.type === "candidate" && event.state.status === "succeeded"), { timeout: 5000, message: "committed-input prediction" }).toHaveLength(1);
			await host.execute({ turnID: "write", id: "write", tool: "write", args, tools }, undefined, () => { throw new Error("write prediction must commit"); });
			await host.finishTurn("write"); predict = false;
			const captured = captures.mock.calls.length;
			await host.startTurn({ ...startInput(tools[1]!, "read"), tools });
			const query = { path: args.path, offset: 2 }, read = vi.fn(() => tools[1]!.execute("native", query as never));
			const call = { turnID: "read", id: "read", tool: "read", args: query, tools };
			expect(await host.execute(call, undefined, read)).toEqual(await tools[1]!.execute("oracle", query as never));
			expect(read).not.toHaveBeenCalled(); expect(captures.mock.calls.length).toBe(captured);
			await writeFile(path.join(cwd, args.path), "external\nchanged\n");
			expect(await host.execute({ ...call, id: "stale" }, undefined, read)).toEqual(await tools[1]!.execute("oracle", query as never));
			expect(read).toHaveBeenCalledOnce();
			const write = vi.fn(() => tools[0]!.execute("native", args as never));
			await host.execute({ turnID: "read", id: "repeat", tool: "write", args, tools }, undefined, write);
			expect(write).toHaveBeenCalledOnce(); expect(await fs.readFile(path.join(cwd, args.path), "utf8")).toBe(args.content);
			const nextArgs = { ...args, content: "latest\nstate\n" };
			await host.execute({ turnID: "read", id: "replace", tool: "write", args: nextArgs, tools }, undefined, () => tools[0]!.execute("native", nextArgs as never));
			await host.finishTurn("read");
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 1, exactReuseHits: 1 });
			predict = true; readNext = true;
			await host.startTurn({ ...startInput(tools[1]!, "predict-read"), tools });
			await expect.poll(() => events.filter(event => event.type === "candidate" && event.turnID === "predict-read" && event.state.status === "succeeded"), { timeout: 5000 }).toHaveLength(1);
			expect(await host.execute({ ...call, turnID: "predict-read", id: "new", args: { path: args.path } }, undefined, read))
				.toEqual(await tools[1]!.execute("oracle", { path: args.path } as never));
			expect(read).toHaveBeenCalledOnce(); await host.finishTurn("predict-read", true);
		} finally { await host.dispose(); await sandbox.dispose(); captures.mockRestore(); }
	});

	it("uses a completed search's inputs for current tools across turns without adopting its output or prediction", async ({ skip }) => {
		const cwd = await temporaryWorkspace(), profile = await createClosedSearchProfile(cwd);
		if (!profile.invocations.has("grep")) { await profile.pool.dispose(); return skip("qualified rg is unavailable"); }
		await writeFile(path.join(cwd, "unused.txt"), "original");
		await writeFile(path.join(cwd, ".ignore"), "# captured preparation rules\n");
		await writeFile(path.join(cwd, ".rgignore"), "# original transport rules\n");
		await fs.mkdir(path.join(cwd, "nested")); await writeFile(path.join(cwd, "nested/extra.txt"), "unmatched");
		const tools: AgentTool[] = [createReadTool(cwd), createLsTool(cwd), createFindTool(cwd), createGrepTool(cwd)];
		const ready = deferred<void>();
		const directories = vi.spyOn(fs, "mkdtemp"), mkdirs = vi.spyOn(fs, "mkdir"), copies = vi.spyOn(fs, "writeFile");
		const preparations = () => directories.mock.calls.filter(([name]) => path.basename(String(name)) === "inputs-").length;
		let predict = true, permitted = true, rootOverride: string | undefined, evaluations = 0;
		const { host, events } = drafterHost("resources", {
			cwd,
			getSettings: () => ({ ...settings(), drafterEnabled: predict, drafterGateEnabled: false, drafterMaxDepth: 0,
				tools: tools.map(tool => tool.name), resourceCacheMaxEntries: 16, resourceCacheMaxBytes: 1024 * 1024 }),
			complete: async () => drafterCall({ pattern: ".", path: "." }, "grep", "search"),
			resolveInvocation: (tool, input) => {
				const invocation = profile.invocations.get(tool) ?? resolvePiToolInvocation(tool, input, { cwd, environment: {} });
				return invocation && { ...invocation, ...(rootOverride ? { filesystemRoot: rootOverride } : {}),
					filesystem: invocation.filesystem && ((...args) => { evaluations++; return invocation.filesystem!(...args); }) };
			},
			preflight: context => { expect(context.action.tool).toBe(context.toolName); return permitted; },
			executionWorlds: [createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: tools.map(tool => tool.name), maxBytes: () => 1024 * 1024 })],
			onEvent: event => { if (event.type === "candidate") {
				if (event.state.status === "succeeded") ready.resolve();
				else if (event.state.status === "failed" || event.state.status === "cancelled") ready.reject(new Error(JSON.stringify(event.state.cause)));
			} },
		});
		try {
			await host.startTurn({ ...startInput(tools[3]!, "seed"), tools }); await ready.promise;
			const privatePath = (name: unknown) => String(name).includes(`${path.sep}inputs-`);
			const created = mkdirs.mock.calls.filter(([name]) => privatePath(name)).map(([name]) => String(name));
			expect(created.length).toBeGreaterThan(0); expect(new Set(created).size).toBe(created.length);
			expect(copies.mock.calls.filter(([name, bytes]) => privatePath(name) && Buffer.isBuffer(bytes) && bytes.toString() === "# captured preparation rules\n")).toHaveLength(1);
			predict = false; await host.finishTurn("seed");
			await host.startTurn({ ...startInput(tools[0]!, "consumer"), tools });
			expect(preparations()).toBe(1);
			for (const args of [{ pattern: "two", path: "." }, { pattern: "THREE", path: ".", ignoreCase: true, context: 1, limit: 1 },
				{ pattern: "one", path: ".", glob: "*.txt" }, { pattern: "^!", path: ".", glob: "*" }]) {
				const expected = await tools[3]!.execute("reference", args), actor = vi.fn(() => tools[3]!.execute("native", args));
				const call = { turnID: "consumer", id: args.pattern, tool: "grep", args, tools };
				expect(await host.execute(call, undefined, actor)).toEqual(expected);
				expect(await host.execute({ ...call, id: args.pattern + ":again" }, undefined, actor)).toEqual(expected);
				expect(actor).not.toHaveBeenCalled(); expect(preparations()).toBe(args.glob === "*" ? 3 : args.glob ? 2 : 1);
			}
			await writeFile(path.join(cwd, "unused.txt"), "changed but unused");
			for (const [name, args] of [["read", { path: "notes.txt", offset: 2, limit: 1 }], ["ls", { path: "." }],
				["find", { pattern: "*.txt", path: "." }], ["grep", { pattern: "two", path: "notes.txt" }]] as const) {
				const tool = tools.find(tool => tool.name === name)!, expected = await tool.execute("reference", args as never);
				const actor = vi.fn(() => tool.execute("native", args as never));
				const call = { turnID: "consumer", id: name, tool: name, args, tools };
				await host.previewActorCall(call);
				expect(await host.execute(call, undefined, actor), name).toEqual(expected);
				expect(await host.execute({ ...call, id: name + ":again" }, undefined, actor)).toEqual(expected);
				expect(actor).not.toHaveBeenCalled();
			}
			const args = { path: "notes.txt", offset: 2, limit: 1 }, reader = tools[0]!, actor = vi.fn(() => reader.execute("native", args));
			const call = { turnID: "consumer", id: "denied", tool: "read", args, tools };
			rootOverride = path.join(cwd, "unproven");
			await host.execute({ ...call, id: "root-rebound" }, undefined, actor); expect(actor).toHaveBeenCalledOnce();
			rootOverride = undefined; permitted = false;
			const beforeDeniedPreview = evaluations; await host.previewActorCall({ ...call, id: "denied-preview", args: { path: "notes.txt", offset: 3 } });
			expect(evaluations).toBe(beforeDeniedPreview);
			await host.execute(call, undefined, actor); expect(actor).toHaveBeenCalledTimes(2);
			permitted = true; await writeFile(path.join(cwd, "notes.txt"), "changed\ncurrent");
			expect((await host.execute({ ...call, id: "changed" }, undefined, actor)).content).toEqual([{ type: "text", text: "current" }]);
			expect(actor.mock.calls.length, JSON.stringify(events.filter(event => event.type === "actor_action" && event.settlement.actorAction.id === "changed"))).toBe(2);
			await host.finishTurn("consumer", true);
			expect(summarizeSpeculativeTrace(events)).toMatchObject({ inputReuseHits: 17, exactReuseHits: 0, partialResultReuseHits: 0, predictionsMatched: 0 });
			expect(events.filter(event => event.type === "actor_action" && event.settlement.provider.kind === "speculative")
				.every(event => event.type === "actor_action" && event.settlement.matchedPredictions.length === 0)).toBe(true);
		} finally { await host.dispose(); await profile.pool.dispose(); directories.mockRestore(); mkdirs.mockRestore(); copies.mockRestore(); }
	});

	it.each([false, true])("only promotes proven host observations, independently of prediction (ThinkThread=%s)", async (thinkthread) => {
		const cwd = await temporaryWorkspace(process.env.THINKTHREAD_FS ?? path.join(process.cwd(), "bench")), file = path.join(cwd, "notes.txt");
		let tools: string[] = [];
		const tool = createReadTool(cwd);
		const clientFactory = vi.fn(() => { throw new Error("Actor observation must not initialize the SDK"); });
		const world = createThinkThreadExecutionWorld({ clientFactory, runnerFingerprint: "test" });
		let unstable = false;
		const base = createSpeculativeActionHost("session", {
			cwd, getSettings: () => ({ enabled: true, drafterEnabled: false, tools, patternAware: { enabled: false } }),
			complete: async () => { throw new Error("No model calls expected"); }, preflight: () => !unstable,
			resolveInvocation: (name, input) => resolvePiToolInvocation(name, input, { cwd, environment: {} }),
			speculativeExecutionWorldEnabled: () => false, executionWorlds: [
				...(thinkthread ? [world] : []),
				createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 4096 }),
			],
		});
		const host = thinkthread ? withThinkThreadProfileLifecycle(base, world) : base;
		let args = { path: "@notes.txt", offset: 1 };
		const actor = vi.fn(async () => {
			if (unstable) await writeFile(file, "B\nsecond");
			const output = withPiProjectionCoverage("read", args, await tool.execute("read", args));
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
				const delivered = await host.execute(call, undefined, actor);
				expect(delivered.content).toEqual([{ type: "text", text: expected }]);
				delivered.content.push({ type: "text", text: "Actor-owned edit" });
				if (calls === 1) {
					const repeat = await host.execute({ ...call, id: `${turnID}:repeat` }, undefined, actor);
					expect(repeat.content).toEqual([{ type: "text", text: expected }]);
				}
				// Force the ABA host window above; once restored, independently proved old inputs remain reusable.
				expect(actor, turnID).toHaveBeenCalledTimes(calls + (process.platform === "win32" ? 1 : turnID === "after-ABA" ? -2 : calls > 1 ? -1 : 0));
				await host.finishTurn(turnID);
			}
			expect(clientFactory).not.toHaveBeenCalled();
		} finally { await host.dispose(); }
	});

	it("binds one Actor operation through matching, fallback and settlement", async () => {
		const cwd = await temporaryWorkspace();
		for (const mode of ["keyed", "presealed", "unkeyable", "outside-turn", "binding-error"]) {
			const keyed = mode === "keyed" || mode === "presealed";
			let profile = "initial", boundKey: unknown;
			const metadata = { command: "npm test", cwd, shell: process.execPath, commandTransport: "argv" as const,
				environment: { PROFILE: "initial" }, shellArgs: ["--initial"] };
			const descriptor = structuredClone(metadata);
			const problem = new Error("selected executor unavailable");
			const bindingGate = gated();
			const actor = vi.fn(async () => textResult("built"));
			const settled = vi.fn();
			const resolveInvocation = vi.fn(async () => {
				const invocation = { executor: profile, identity: metadata, process: metadata };
				await bindingGate.wait();
				if (mode === "binding-error") throw problem;
				return invocation;
			});
			const tool: AgentTool<typeof bashSchema> = { name: "bash", label: "bash", description: "bash", parameters: bashSchema, execute: actor };
			const host = createSpeculativeActionHost("session", {
				cwd, getSettings: () => ({ ...settings(), drafterEnabled: false, tools: ["bash"] }),
				complete: async () => { throw new Error("prediction disabled"); },
				resolveInvocation, onActorActionSettled: settled,
			});
			try {
				if (mode !== "outside-turn") await host.startTurn(startInput(tool));
				const mutableArgs = mode === "unkeyable" ? {} : { command: "npm test" };
				const call = { ...(mode !== "outside-turn" ? { turnID: "turn-1" } : {}), id: "actor-bash", tool: "bash",
					args: mode === "presealed" ? PI_ACTION_SEMANTICS.buildKey("bash", mutableArgs, cwd)!.input : mutableArgs,
					tools: mode === "outside-turn" ? [] : [tool] };
				const admitted = structuredClone(call.args);
				const pending = host.execute(call, undefined, async (operation) => {
					metadata.environment.PROFILE = "reconfigured"; metadata.shellArgs.push("--later"); metadata.command = "later";
					expect(operation.input).toEqual(admitted);
					expect(operation.input).not.toBe(call.args);
					expect(operation.invocation).toEqual({ executor: "initial", identity: descriptor, process: descriptor });
					expect(operation.action?.executionContext).toEqual(keyed ? operation.invocation : undefined);
					expect(operation.action?.input.command).toBe(keyed ? "npm test" : undefined);
					boundKey = operation.action;
					return actor();
				});
				const outcome = mode === "binding-error" ? expect(pending).rejects.toBe(problem) : expect(pending).resolves.toHaveProperty("content.0.text", "built");
				await bindingGate.entered; profile = "next"; mutableArgs.command = "changed during binding";
				bindingGate.release();
				await outcome;
				await host.finishTurn("turn-1", true);
				expect(resolveInvocation).toHaveBeenCalledOnce(); expect(actor).toHaveBeenCalledTimes(mode === "binding-error" ? 0 : 1);
				if (keyed) { expect(settled).toHaveBeenCalledOnce(); expect(settled.mock.calls[0][0].action).toBe(boundKey); }
			} finally { bindingGate.release(); await host.dispose(); }
		}
	});

	it("uses one Actor fallback for opaque inputs and bindings while retaining exact plain-data reuse", async () => {
		const cwd = await temporaryWorkspace();
		const actionSemantics = new ActionSemanticsRegistry([{ ...PI_ACTION_SEMANTICS.definition("read")!, tool: "inspect",
			canonicalize: (input) => ({ input: input as Record<string, unknown>, resources: [] }) }]);
		const shapes: Array<[string, (value: number) => unknown, (value: unknown) => number]> = [
			["Date", (value) => new Date(value), (value) => (value as Date).getTime()],
			["Map", (value) => new Map([["value", value]]), (value) => (value as Map<string, number>).get("value")!],
			["Set", (value) => new Set([value]), (value) => [...value as Set<number>][0]!],
			["undefined field", (value) => value ? {} : { present: undefined }, (value) => Object.hasOwn(value as object, "present") ? 0 : 1],
			["signed zero", (value) => value ? 0 : -0, (value) => Object.is(value, -0) ? 0 : 1],
			["nonfinite", (value) => value ? null : NaN, (value) => value === null ? 1 : 0],
			["sparse", (value) => value ? [null] : Array(1), (value) => 0 in (value as unknown[]) ? 1 : 0],
			["array property", (value) => Object.assign([0], { extra: value }), (value) => (value as { extra: number }).extra],
			["cycle", (value) => { const node = { value, self: {} }; node.self = node; return node; }, (value) => (value as { value: number }).value],
			["alias", (value) => { const child = { value: 0 }; return value ? { left: { value: 0 }, right: { value: 0 } } : { left: child, right: child }; },
				(value) => { const node = value as { left: object; right: object }; return node.left === node.right ? 0 : 1; }],
			["plain", (value) => ({ nested: { value } }), (value) => (value as { nested: { value: number } }).nested.value],
		];
		for (const [shape, make, inspect] of shapes) for (const boundary of ["input", "identity", "process"] as const) for (const next of [0, 1]) {
			let profile = 0;
			const ready = deferred<void>(), disposed = vi.fn();
			const result = (value: number) => textResult(String(value));
			const actor = vi.fn(async (input: { value: unknown }) => result(boundary === "input" ? inspect(input.value) : profile));
			const tool: AgentTool<typeof mockToolSchema> = { name: "inspect", label: "inspect", description: "Pure fixture inspection",
				parameters: mockToolSchema, prepareArguments: (input) => { const { value } = input as { value: number }; return { value: boundary === "input" ? make(value) : value }; },
				execute: async (_id, input) => actor(input) };
			const execute = vi.fn((context: Parameters<SpeculativeAgentExecutionWorld["speculation"]["execute"]>[0]) => {
				const binding = context.action.executionContext as { identity?: { value: unknown }; process?: { value: unknown } };
				return { result: result(inspect(boundary === "input" ? (context.args as { value: unknown }).value : binding[boundary]!.value)), isError: false };
			});
			const resolveInvocation = vi.fn(() => boundary === "input" ? undefined : { executor: "fixture", ...(boundary === "identity"
				? { identity: { value: make(profile) } } : { process: { command: "inspect", cwd, environment: {}, shell: process.execPath,
					shellArgs: [], commandTransport: "argv" as const, value: make(profile) } }) });
			const { host } = drafterHost("shapes", { cwd, actionSemantics,
				getSettings: () => ({ ...settings(), drafterGateEnabled: false, drafterMaxDepth: 0, resourceCacheMaxEntries: 0, tools: ["inspect"] }),
				complete: async () => drafterCall({ value: 0 }, "inspect", "draft"),
				resolveInvocation, executionWorlds: [mockRuntimeWorld(execute, disposed)],
				onEvent: (event) => { if (shape === "plain" ? event.type === "candidate" && event.state.status === "succeeded" : event.type === "source_request") ready.resolve(); },
			});
			try {
				await host.startTurn(startInput(tool)); await ready.promise;
				await nextTurn(); profile = next;
				const args = { value: boundary === "input" ? make(next) : 0 };
				const output = await host.execute({ turnID: "turn-1", id: "actor", tool: "inspect", args, tools: [tool] }, undefined,
					(operation) => actor(operation.input as { value: unknown }));
				expect(output, `${shape}/${boundary}/${next}`).toEqual(result(next));
				expect(actor).toHaveBeenCalledTimes(shape === "plain" && next === 0 ? 0 : 1);
				expect(execute).toHaveBeenCalledTimes(shape === "plain" ? 1 : 0);
				expect(resolveInvocation).toHaveBeenCalledTimes(2);
			} finally { await host.dispose(); }
			expect(disposed).toHaveBeenCalledOnce();
		}
	});

	it.each(["running", "completed"].flatMap((phase) =>
		(phase === "running" ? ["suffix", "preflight", "missing", "recheck"] : ["suffix", "preflight", "recheck"]).map((mode) => [phase, mode])))
	("keeps %s Bash on exactly one Actor fallback when %s rejects reuse", async (phase, mode) => {
		const cwd = await temporaryWorkspace(), executionGate = gated(), completed = deferred<void>();
		const actor = vi.fn(async () => textResult("tail arguments: -n 2"));
		const tool: AgentTool<typeof bashSchema> = { name: "bash", label: "bash", description: "bash", parameters: bashSchema, execute: actor };
		const dispose = vi.fn();
		const sandbox = mockRuntimeWorld(async () => {
			await executionGate.wait();
			return { result: textResult("tail arguments: -n 3"), isError: false };
		}, dispose);
		let allowed = mode !== "preflight";
		const preflight = vi.fn(({ signal }: { signal: AbortSignal }) => {
			expect(signal).toBeInstanceOf(AbortSignal);
			return phase === "running" ? allowed : allowed ? { ok: true as const } : { ok: false as const, reason: "host_denied", detail: "restricted" };
		});
		const { host, events } = drafterHost("session", {
			cwd, getSettings: () => ({ ...settings(), tools: ["bash"], drafterMaxDepth: 0 }),
			complete: vi.fn().mockResolvedValueOnce(drafterCall({ command: "printf data 2>&1 | tail -n 3" }, "bash", "draft-bash")).mockResolvedValue(assistant([], "stop")),
			preflight: mode === "missing" ? undefined : preflight, executionWorlds: [sandbox, sandbox],
			resolveInvocation: (name, args) => resolvePiToolInvocation(name, args, { cwd, environment: {}, shellPath: process.execPath }),
			onEvent: (event) => {
				if (event.type === "candidate" && event.state.status === "succeeded" || event.type === "prediction" && event.settlement.observation === "unobserved") completed.resolve();
			},
		});
		try {
			await host.startTurn(startInput(tool));
			if (["preflight", "missing"].includes(mode!)) await completed.promise;
			else { await executionGate.entered; if (phase === "completed") { executionGate.release(); await completed.promise; } }
			if (mode === "recheck") allowed = false;
			const output = await host.execute({ turnID: "turn-1", id: "actor-bash", tool: "bash",
				args: { command: `printf data 2>&1 | tail -n ${mode === "suffix" ? 2 : 3}` }, tools: [tool] }, undefined, actor);
			expect(output.content).toEqual([{ type: "text", text: "tail arguments: -n 2" }]);
			expect(actor).toHaveBeenCalledOnce();
			await waitFor(() => events.some((event) => event.type === "actor_action"));
			expect(preflight).toHaveBeenCalledTimes(mode === "missing" ? 0 : mode === "preflight" || mode === "suffix" ? 1 : 2);
			if (mode === "recheck") expect(events.find((event) => event.type === "actor_action")).toMatchObject({ settlement: {
				rejections: [{ cause: { stage: "authorization", code: "permission_or_policy_changed", ...(phase === "completed" ? { detail: "restricted" } : {}) } }],
			} });
			if (mode === "preflight") expect(events.find((event) => event.type === "prediction")).toMatchObject({ settlement: {
				cause: { stage: "admission", code: phase === "running" ? "permission_or_policy" : "host_denied" },
			} });
		} finally { executionGate.release(); await host.dispose(); }
		expect(dispose).toHaveBeenCalledOnce();
	});

	it.each(["shared", "epoch", "directory", "projection"])("owns Pattern contracts and partitions %s learning", async (partition) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd), configuration = patternAwareSettings({ enabled: true });
		const canonicalize = vi.fn(PI_ACTION_SEMANTICS.definition("read")!.canonicalize);
		const registry = (epoch: string) => new ActionSemanticsRegistry([{ ...PI_ACTION_SEMANTICS.definition("read")!, epoch, canonicalize }]);
		const input = { sessionID: "first", cwd, stateDirectory: cwd, workspaceIdentity: cwd,
			actionSemantics: registry("original.read"), projectionRules: [] as typeof PI_READ_RANGE_PROJECTION_RULE[] };
		const peer = { ...input, sessionID: "peer", cwd: partition === "directory" ? path.join(cwd, "nested") : cwd,
			actionSemantics: registry(partition === "epoch" ? "peer.read" : "original.read"),
			projectionRules: partition === "projection" ? [PI_READ_RANGE_PROJECTION_RULE] : [] };
		const controllers = [createPatternPlanSource(input), createPatternPlanSource(peer)];
		const predict = vi.spyOn(PatternAwareStore.prototype, "predict"), stores: PatternAwareStore[] = [];
		Object.assign(input, { actionSemantics: registry("replaced.read"), cwd: path.join(cwd, "replaced") });
		input.projectionRules.push(PI_READ_RANGE_PROJECTION_RULE);
		try {
			for (const [index, controller] of controllers.entries()) {
				await controller.source.propose(patternRequest(tool, configuration, String(index)));
				stores.push(predict.mock.contexts.at(-1) as PatternAwareStore);
			}
			expect(stores[0] === stores[1]).toBe(partition === "shared");
			stores[0]!.observe({ sessionID: "learned", turnID: "read", tool: "read", input: { path: "notes.txt" }, outcome: "success", durationMs: 1 });
			expect(canonicalize).toHaveBeenCalledWith({ path: "notes.txt" }, cwd);
			expect(stores[1]!.recent("learned")).toHaveLength(partition === "shared" ? 1 : 0);
		} finally { await Promise.all(controllers.map(controller => controller.dispose())); }
		const reopened = await acquirePatternAwareStore(cwd, configuration, cwd, patternAwareActionSemantics(registry("original.read"), cwd));
		try { expect(reopened.store).not.toBe(stores[0]); } finally { await reopened.release(); }
	});

	it("owns an admitted Pattern learning configuration and batch through immediate disposal", async () => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const patternAware = patternAwareSettings({ enabled: true, multiStepEnabled: false });
		const lease = await patternStoreLease(cwd, patternAware), request = patternRequest(tool, patternAware, "session", { read: "schema" });
		const controller = createPatternPlanSource({ sessionID: "session", cwd, stateDirectory: cwd,
			actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
		try {
			await controller.source.observe!({ ...request,
				consumeInput: { sessionID: "session", turnID: request.startInput.turnID, tool: "read", args: { path: "notes.txt" }, tools: [tool] },
				tool: "read", concrete: { path: "notes.txt" }, output: { result: textResult("one"), isError: false }, durationMs: 1, order: 0 });
			controller.turnFinished(request.startInput, request.settings, false);
			request.settings.sourceConfig.patternAware = patternAwareSettings({ ...patternAware, maxContextLength: 3 });
			await controller.dispose();
			expect(lease.store.recent("session")).toMatchObject([{ tool: "read", input: { path: "notes.txt" }, outcome: "success", schemaHash: "schema" }]);
		} finally { await controller.dispose(); await lease.release(); }
	});

	it.each([false, true])("retires failed internal bindings without training misses or discarding a newer observation (recurring=%s)", async (recurring) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const patternAware = patternAwareSettings({ enabled: true, multiStepEnabled: false, beamWidth: 4 });
		const store = new PatternAwareStore(patternAware, undefined, patternAwareActionSemantics(PI_ACTION_SEMANTICS, cwd));
		const request = patternRequest(tool, patternAware, "session", { read: "schema" });
		const controller = createPatternPlanSource({ sessionID: "session", cwd, store, actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
		const concrete = { path: "rare.txt" }, action = PI_ACTION_SEMANTICS.buildKey("read", concrete, cwd, "schema")!;
		const binding = (identity: string, executionMs: number, permissionHash = action.hash, available = () => true) =>
			Object.freeze({ backend: "test", identity, executionMs, expectedDurationMs: executionMs + 10, permissionHash,
				get available() { return available(); } });
		let slowMs = 8;
		const slow = Object.freeze({ ...binding("slow", slowMs), get executionMs() { return slowMs; },
			get expectedDurationMs() { return slowMs + 10; } }), fast = binding("fast", 3);
		const observe = (operations: readonly ReturnType<typeof binding>[]) => controller.source.observe!({ ...request, action, operations,
			consumeInput: { sessionID: "session", turnID: request.startInput.turnID, tool: "read", args: concrete, tools: [tool] },
			tool: "read", concrete, output: { result: textResult("ready"), isError: false }, durationMs: 20, order: 0 });
		let proposedBindings: unknown[] = [];
		const proposed = async () => {
			const plan = await controller.source.propose(request);
			if (!plan) return undefined;
			if (!("actions" in plan)) throw new Error("Expected a Pattern proposal");
			proposedBindings = plan.actions.flatMap(action => action.type === "operation" ? [action.operation] : []);
			const internal = plan.actions.find(action => action.type === "operation");
			return internal && { ...internal, proposalID: plan.id, actionID: internal.id, feedback: internal.feedback };
		};
		try {
			for (const turnID of ["common-1", "common-2"]) store.observe({ sessionID: "session", turnID, tool: "read",
				input: { path: "notes.txt" }, outcome: "success", durationMs: 20, schemaHash: "schema" });
			if (recurring) store.observe({ sessionID: "session", turnID: "discovery", tool: "read",
				input: concrete, outcome: "success", durationMs: 20, schemaHash: "schema" });
			await observe([fast, slow, binding("wrong permission", 100, "foreign")]);
			controller.turnFinished(request.startInput, request.settings, false);
			const internal = (await proposed())!;
			expect(internal.operation).toBe(slow);
			expect(proposedBindings).toEqual([slow, fast]);
			slowMs = 1;
			expect((await proposed())!.operation).toBe(fast);
			slowMs = 12;
			expect(await proposed()).toMatchObject({ operation: slow, expectedDurationMs: 22 });
			expect(internal.expectedDurationMs).toBe(18); // Issued plans keep their admission estimate.
			const snapshot = store.snapshot(), prediction = { id: "internal", source: "pattern_aware", proposalID: internal.proposalID, actionID: internal.id };
			await controller.source.onIssued!(internal);
			const settle = (stage: "matching" | "execution") => controller.source.onSettled!({ ...internal,
				settlement: { prediction, observation: "unobserved", cause: { stage, code: stage === "matching" ? "operation_not_observed" : "candidate_failed" } } });
			await settle("matching");
			expect((await proposed())!.operation).toBe(slow);
			await settle("execution");
			expect((await proposed())!.operation).toBe(fast);
			let available = true;
			const refreshed = binding("slow", 9, action.hash, () => available);
			await observe([refreshed]); await settle("execution");
			expect((await proposed())!.operation).toBe(refreshed);
			available = false;
			expect((await proposed())!.operation).toBe(fast);
			expect(store.snapshot()).toEqual(snapshot);
			await controller.finishSession();
			expect(await proposed()).toBeUndefined();
		} finally { await controller.dispose(); }
	});

	it.each([false, true])("owns late Pattern feedback across configuration replacement and return=%s", async (returning) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const older = patternAwareSettings({ enabled: true, maxContextLength: 2 }), newer = patternAwareSettings({ ...older, maxContextLength: 3 });
		const bootstrap = await patternStoreLease(cwd, older), oldStore = bootstrap.store;
		const controller = createPatternPlanSource({ sessionID: "probe", cwd, stateDirectory: cwd,
			actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
		const propose = (patternAware: typeof older) => controller.source.propose(patternRequest(tool, patternAware, "probe", { read: "schema" }));
		try {
			for (let index = 0; index < 3; index++) {
				const sessionID = `training-${index}`, file = `file-${index}.txt`;
				oldStore.observe({ sessionID, turnID: "scan", tool: "grep", input: { pattern: "one", path: "." }, outputPaths: [file], outcome: "success", durationMs: 1 });
				oldStore.observe({ sessionID, turnID: "read", tool: "read", input: { path: file }, outcome: "success", durationMs: 1, schemaHash: "schema" });
				oldStore.finishSession(sessionID);
			}
			oldStore.observe({ sessionID: "probe", turnID: "before", tool: "grep", input: { pattern: "one", path: "." }, outputPaths: ["notes.txt"], outcome: "success", durationMs: 1 });
			const plan = await propose(older);
			if (!plan || !("actions" in plan)) throw new Error("Expected a learned Pattern proposal");
			const action = plan.actions.find(action => (action.feedback as { patternIDs: string[] }).patternIDs.length)!;
			const feedback = { proposalID: plan.id, actionID: action.id, feedback: action.feedback };
			const patternID = (action.feedback as { patternIDs: string[] }).patternIDs[0]!;
			await controller.source.onIssued!(feedback); await bootstrap.release(); await propose(newer);
			if (returning) {
				await propose(older);
				const active = await patternStoreLease(cwd, older);
				try { expect(active.store).toBe(oldStore); } finally { await active.release(); }
			}
			const before = oldStore.snapshot().find(pattern => pattern.id === patternID)!.feedback.observed;
			const settlement = { prediction: { id: "prediction", source: "pattern_aware", proposalID: plan.id, actionID: action.id },
				observation: "observed" as const, actorAction: { id: "actor", sequence: 0, turnID: "turn-1" }, match: { matched: false as const } };
			await controller.source.onSettled!({ ...feedback, settlement });
			await controller.finishSession(); await controller.dispose();
			const closed = oldStore.snapshot();
			await controller.source.onIssued!(feedback); await controller.source.onSettled!({ ...feedback, settlement });
			expect(oldStore.snapshot()).toEqual(closed);
			const fresh = await patternStoreLease(cwd, older);
			try {
				expect(fresh.store).not.toBe(oldStore);
				expect(fresh.store.snapshot().find(pattern => pattern.id === patternID)!.feedback.observed).toBe(before + 1);
			} finally { await fresh.release(); }
		} finally { await controller.dispose(); await bootstrap.release(); await oldStore.flush(); }
	});

	it("joins an admitted Pattern request during disposal without releasing an external Store", async () => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd), available = deferred<PatternAwareStore>();
		const patternAware = patternAwareSettings({ enabled: true }), store = new PatternAwareStore(patternAware);
		const flush = vi.spyOn(store, "flush"), controller = createPatternPlanSource({ sessionID: "session", cwd, store: available.promise,
			actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
		const pending = Promise.resolve(controller.source.propose(patternRequest(tool, patternAware)));
		try {
			await nextTurn();
			let closed = false;
			const closing = controller.dispose().then(() => { closed = true; });
			await nextTurn(); expect(closed).toBe(false);
			available.resolve(store); await expect(pending).resolves.toBeUndefined(); await closing;
			expect(flush).not.toHaveBeenCalled();
		} finally { available.resolve(store); await pending; await controller.dispose(); }
	});

	it.each(["concurrent", "closing", "load failure", "flush failure", "replaced", "replaced load failure"])("owns Pattern analyzer replacement through %s", async (phase) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd), stores: PatternAwareStore[] = [];
		const older = patternAwareSettings({ enabled: true, maxContextLength: 2 });
		const newer = patternAwareSettings({ ...older, maxContextLength: 3 });
		const replacing = phase.startsWith("replaced"), newest = replacing ? patternAwareSettings({ ...older, maxContextLength: 4 }) : newer;
		const loading = gated();
		const load = PatternAwareStore.prototype.load;
		const observer = vi.spyOn(PatternAwareStore.prototype, "load").mockImplementation(async function (this: PatternAwareStore) {
			stores.push(this);
			if (stores.length === 2) {
				await loading.wait();
				if (phase.includes("load failure")) throw new Error("load failed");
			}
			return load.call(this);
		});
		const controller = createPatternPlanSource({ sessionID: "session", cwd, stateDirectory: cwd,
			actionSemantics: PI_ACTION_SEMANTICS, projectionRules: [] });
		const propose = (patternAware: typeof older) => {
			const configuration = { ...patternAware }, pending = controller.source.propose(patternRequest(tool, configuration));
			Object.assign(configuration, { enabled: false, maxContextLength: 99 });
			return pending;
		};
		let pending: Promise<PromiseSettledResult<unknown>[]> | undefined, closing: Promise<void> | undefined;
		try {
			await propose(older);
			expect(stores).toHaveLength(1);
			if (phase === "flush failure") vi.spyOn(stores[0]!, "flush").mockRejectedValueOnce(new Error("flush failed"));
			pending = Promise.allSettled([propose(newer), propose(newest)]);
			await loading.entered;
			if (phase === "closing") {
				let closed = false;
				closing = controller.dispose(); void closing.then(() => { closed = true; });
				await nextTurn();
				expect(closed).toBe(false);
				expect(controller.dispose()).toBe(closing);
			}
			loading.release();
			expect((await pending).map(result => result.status)).toEqual([
				phase.includes("load failure") ? "rejected" : "fulfilled", phase === "load failure" ? "rejected" : "fulfilled",
			]);
			if (phase !== "closing") {
				await expect(propose(newest)).resolves.toBeUndefined();
				const gate = gated(), store = stores.at(-1)!, flush = store.flush.bind(store);
				if (phase === "flush failure") vi.spyOn(store, "flush").mockImplementationOnce(async () => {
					await gate.wait(); await flush();
				});
				let finished = false;
				const finishing = controller.finishSession().then(() => { finished = true; });
				try {
					if (phase === "flush failure") { await gate.entered; await nextTurn(); expect(finished).toBe(false); }
				} finally { gate.release(); await finishing; }
			}
			await controller.dispose();
			await expect(propose(newest)).rejects.toThrow("closed");
			expect(stores).toHaveLength(replacing || phase === "load failure" ? 3 : 2);
			const retired = [...stores];
			for (const configuration of new Set([older, newer, newest])) {
				const fresh = await patternStoreLease(cwd, configuration);
				try { expect(retired).not.toContain(fresh.store); } finally { await fresh.release(); }
			}
		} finally {
			loading.release(); await pending; await closing; await controller.dispose();
			observer.mockRestore(); await Promise.allSettled(stores.map(store => store.flush()));
		}
	});

	it.each(["actor", "drafter", "closing", "preparing", "rejected", "carried", "revised"] as const)("rebases PatternAware across an authoritative %s result", async (origin) => {
		const { cwd, patternSettings, patternStore, grepTool, readTool: learnedReadTool, materialized } = await patternRebaseFixture();
		const carried = origin === "carried" || origin === "revised";
		const retained = carried || origin === "preparing";
		const readTool = carried ? createReadTool(cwd) : learnedReadTool;
		const tools = [grepTool, readTool], ready = deferred<void>(), routeGate = deferred<void>();
		const available = deferred<PatternAwareStore>(), nextRequest = deferred<string>(), feedbackGate = deferred<void>();
		const actorTool = origin === "actor" ? { ...grepTool, parameters: Type.Object({ ...grepSchema.properties,
			flags: Type.Optional(Type.String()) }) } : grepTool;
		let actorSchema = "";
		let allowRead = origin !== "rejected";
		const predictAfterBatch = vi.spyOn(patternStore, "predictAfterBatch");
		const issued = vi.spyOn(patternStore, "issued");
		const world = toolRuntimeWorld();
		const fingerprint = vi.fn<NonNullable<SpeculativeAgentExecutionWorld["speculation"]["fingerprint"]>>(async (request) => {
			if (origin === "actor") throw new Error("Fixture world unavailable");
			if (origin === "preparing" && request.action?.tool === "read") { ready.resolve(); await routeGate.promise; }
			return world.speculation.fingerprint!(request);
		});
		const { host, events } = drafterHost("probe", {
			cwd,
			getSettings: () => ({ ...settings(origin === "drafter" ? 1 : 4), drafterEnabled: origin === "drafter",
				drafterMaxDepth: 0, tools: ["grep", "read"], patternAware: patternSettings }),
			patternStore: origin === "closing" ? available.promise : patternStore,
			preflight: ({ tool }) => tool.name !== "read" || allowRead,
			complete: async () => drafterCall({ pattern: "one", path: "." }, "grep", "draft-grep"),
			resolveInvocation: (tool, input) => carried && tool === "read" ? resolvePiToolInvocation(tool, input, { cwd, environment: {} }) : undefined,
			executionWorlds: carried ? [createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: ["read"], maxBytes: () => 1024 * 1024 })]
				: [{ ...world, speculation: { ...world.speculation, fingerprint } }],
			onCandidateMaterialized: (candidate) => { materialized.push(candidate); },
			onActorActionMaterialized: ({ action }) => { actorSchema = action.schemaHash; },
			onActorActionSettled: async () => { if (origin === "closing") ready.resolve(); await feedbackGate.promise; },
			onEvent: (event) => {
				if (event.type === "candidate" && event.candidate.source === "drafter" && event.state.status === "succeeded") ready.resolve();
				if (carried && event.type === "candidate" && event.state.status === "succeeded" ||
					origin === "rejected" && event.type === "prediction" && event.settlement.observation === "unobserved") ready.resolve();
				if (event.type === "source_request" && event.turnID === "next") nextRequest.resolve(event.request.settlement.status);
			},
		});
		const call = { turnID: "probe:turn", id: "actor-grep", tool: "grep", args: { pattern: "one", path: "." }, tools: [actorTool, readTool] };
		try {
			await host.startTurn({ ...startInput(grepTool, call.turnID),
				context: { systemPrompt: "system", messages: [], tools }, tools });
			expect(fingerprint).not.toHaveBeenCalled();
			if (origin === "drafter") { await ready.promise; expect(fingerprint).toHaveBeenCalled(); }
			else expect(materialized).toHaveLength(0);
			const execute = vi.fn(() => grepTool.execute(call.id, call.args));
			const result = await host.execute(call, undefined, execute);
			expect(result.content).toEqual((await grepTool.execute("oracle", call.args)).content);
			Object.assign(result.content[0]!, { text: "caller.txt:1:one" }); feedbackGate.resolve();
			expect(execute).toHaveBeenCalledTimes(origin === "drafter" ? 0 : 1);
			if (origin === "closing") {
				await ready.promise; await nextTurn();
				const closing = host.finishTurn(call.turnID);
				await nextTurn(); available.resolve(patternStore); await closing;
				expect(materialized).toHaveLength(0);
			} else {
				await waitFor(() => materialized.some((candidate) => candidate.source === "pattern_aware" && candidate.tool === "read"));
				expect(actorSchema).not.toBe("");
				expect(predictAfterBatch.mock.calls[0]?.[1][0]?.schemaHash).toBe(actorSchema);
				expect(materialized).toContainEqual(expect.objectContaining({
					sessionID: "probe", turnID: call.turnID, expectedDecisionSequence: 2, latestDecisionSequence: 2,
					source: "pattern_aware", tool: "read", input: { path: "notes.txt" },
				}));
				expect(patternStore.recent("probe")).toHaveLength(0);
				if (origin === "preparing" || origin === "rejected" || carried) await ready.promise;
				if (origin === "revised") {
					const before = issued.mock.calls.length;
					await host.execute({ ...call, id: "another-grep" }, undefined, execute);
					await waitFor(() => predictAfterBatch.mock.calls.length === 2); await nextTurn();
					expect(issued).toHaveBeenCalledTimes(before);
				}
				await host.finishTurn(call.turnID);
			}
			if (origin === "closing" || origin === "preparing" || origin === "rejected" || carried) {
				allowRead = true;
				await host.startTurn({ ...startInput(readTool, "next"),
					context: { systemPrompt: "system", messages: [], tools }, tools });
				expect(await nextRequest.promise).toBe(retained ? "empty" : "produced");
				if (!retained) await waitFor(() => materialized.some((candidate) => candidate.turnID === "next" && candidate.tool === "read"));
			}
			if (retained) {
				routeGate.resolve();
				await waitFor(() => events.some(event => event.type === "candidate" && event.candidate.source === "pattern_aware" && event.state.status === "succeeded"));
				expect(materialized.filter(candidate => candidate.source === "pattern_aware" && candidate.tool === "read")).toHaveLength(1);
				const args = { path: "notes.txt", ...(carried ? { offset: 2, limit: 1 } : {}) };
				const native = vi.fn(() => readTool.execute("native", args));
				expect(await host.execute({ turnID: "next", id: "narrow-read", tool: "read", args, tools }, undefined, native))
					.toEqual(await readTool.execute("control", args));
				await host.finishTurn("next");
				expect(native).not.toHaveBeenCalled();
				expect(events.filter(event => event.type === "prediction")).toContainEqual(expect.objectContaining({ settlement:
					expect.objectContaining({ observation: "observed", actorAction: expect.objectContaining({ turnID: "next" }), match:
						expect.objectContaining({ matched: true, relation: expect.objectContaining(carried ? { kind: "projected", projector: "read.range" } : { kind: "exact" }),
							adoption: expect.objectContaining({ status: "adopted" }) }) }) }));
			}
		} finally { feedbackGate.resolve(); routeGate.resolve(); available.resolve(patternStore); await host.dispose(); }
	});

	it("turns one sidecar fork batch into safe parallel actions with real execution ahead", async () => {
		const cwd = await temporaryWorkspace();
		const permissionGate = gated();
		await writeFile(path.join(cwd, "wrong.txt"), "wrong", "utf8");
		await writeFile(path.join(cwd, "actor-miss.txt"), "actor", "utf8");
		const events: SpeculativeActionEvent<string>[] = [];
		const materialized: MaterializedSpeculativeCandidate<string>[] = [];
		const actorForkPlans = createActorForkPlanSource();
		const prepare = vi.fn(async () => {}), world = toolRuntimeWorld();
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
		const tool: AgentTool<typeof readSchema> = { name: "read", label: "read", description: "read", parameters: readSchema,
			execute: async (_id, input) => { await new Promise((resolve) => setTimeout(resolve, 80)); return textResult(input.path); } };
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
			preflight: async ({ args }) => {
				if ((args as { path: string }).path === "notes.txt.sibling") {
					await permissionGate.wait();
				}
				return true;
			},
			executionWorlds: [{ ...world, speculation: { ...world.speculation, prepare } }],
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
			prepare.mockClear();
			await host.startTurn(startInput(tool, turnID));
			expect(prepare).not.toHaveBeenCalled();
			coordinator.decorateActorPayload({ prompt: "P" });
			coordinator.observeActorOutput({ type: "text_delta", contentIndex: 0, delta: "x", partial: undefined as never });
			if (actionSourceEnabled) await waitFor(() => prepare.mock.calls.length > 0);
			expect(prepare.mock.calls.length > 0).toBe(actionSourceEnabled);
		};
		const finishTurn = async (turnID: string) => {
			await host.finishTurn(turnID);
			coordinator.endTurn();
		};

		await triggerFork("fork-hit");
		try {
			await permissionGate.entered;
			await waitFor(() => events.some((event) => event.type === "candidate" && event.turnID === "fork-hit" && event.state.status === "succeeded"));
			expect(events.filter((event) => event.type === "candidate" && event.turnID === "fork-hit" && event.state.status === "running")).toHaveLength(1);
		} finally { permissionGate.release(); }
		await waitFor(
			() => materialized.filter((candidate) => candidate.turnID === "fork-hit" && candidate.source === "self-speculation").length === 2,
		);
		const forkBatch = materialized.filter(
			(candidate) => candidate.turnID === "fork-hit" && candidate.source === "self-speculation",
		);
		expect(new Set(forkBatch.map((candidate) => candidate.proposalID)).size).toBe(1);
		expect(forkBatch.map((candidate) => candidate.actionID)).toEqual(["0:fork", "1:fork"]);
		const hit = await host.execute({
			turnID: "fork-hit",
			id: "actor-hit",
			tool: "read",
			args: { path: "notes.txt" },
			tools: [tool],
		}, undefined, async () => { throw new Error("Unexpected Actor fallback"); });
		expect(hit.content).toEqual([{ type: "text", text: "notes.txt" }]);
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
		const missed = vi.fn(async () => textResult("actor-miss.txt"));
		expect((await host.execute({
			turnID: "fork-miss",
			id: "actor-miss",
			tool: "read",
			args: { path: "actor-miss.txt" },
			tools: [tool],
		}, undefined, missed)).content).toEqual([{ type: "text", text: "actor-miss.txt" }]);
		expect(missed).toHaveBeenCalledOnce();
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

	it.each(["tools", "context", "model", "options", "empty", "invalid", "rejected"] as const)("releases an ineligible Drafter after %s without changing Actor history", async (phase) => {
		const cwd = await temporaryWorkspace();
		const gate = gated(), settled = deferred<void>();
		const rejected = phase === "invalid" || phase === "rejected", warms = phase === "empty" || rejected;
		const complete = vi.fn(async () => {
			if (warms) await gate.entered;
			if (phase === "empty") return assistant([], "stop");
			return drafterCall(phase === "invalid" ? {} : { path: "notes.txt" });
		});
		const tool = createReadTool(cwd);
		const world = toolRuntimeWorld(), prepare = vi.fn(async (_input: { signal?: AbortSignal }) => {
			if (warms && prepare.mock.calls.length === 1) { await gate.wait(); }
		});
		const getDraftOptions = vi.fn(async () => {
			if (phase === "options") { await gate.wait(); }
			return {};
		});
		const draftModel = vi.fn(async () => {
			if (phase === "model") { await gate.wait(); }
			return phase === "context" ? { ...model("short"), contextWindow: 32, maxTokens: 16 } : model("draft");
		});
		const { host } = drafterHost("session", {
			cwd,
			getSettings: () => ({ ...settings(), tools: [phase === "tools" ? "bash" : "read"] }),
			draftModel,
			getDraftOptions,
			complete,
			executionWorlds: [{ ...world, speculation: { ...world.speculation, prepare } }],
			preflight: () => phase !== "rejected",
			onEvent: (event) => { if (event.type === (rejected ? "prediction" : "source_request")) settled.resolve(); },
		});
		let closing: Promise<void> | undefined, closed = false;
		try {
			await host.startTurn({
				...startInput(tool),
				context: { systemPrompt: "x".repeat(128), messages: [], tools: [tool] },
			});
			if (phase === "context" || phase === "tools") await settled.promise;
			else {
				await gate.entered;
				if (warms) {
					await settled.promise; await nextTurn();
					expect(prepare.mock.calls[0]![0].signal?.aborted, "unusable results retire preparation before Actor arrival").toBe(true);
				}
				closing = host.dispose().then(() => { closed = true; });
				await nextTurn();
				expect(closed).toBe(false);
				gate.release(); await closing;
			}
			expect(complete).toHaveBeenCalledTimes(warms ? 1 : 0);
			expect(prepare).toHaveBeenCalledTimes(phase === "rejected" ? 2 : warms ? 1 : 0);
			expect(draftModel).toHaveBeenCalledTimes(phase === "tools" ? 0 : 1);
			expect(getDraftOptions).toHaveBeenCalledTimes(["model", "tools", "context"].includes(phase) ? 0 : 1);
		} finally { gate.release(); await closing; await host.dispose(); }
		if (phase === "context" || phase === "tools" || warms) return;

		const sharing = gated(), owners = [new AbortController(), new AbortController()];
		const waitStage = async (stage: string) => { if (stage === phase) { await sharing.wait(); } };
		const selectModel = vi.fn(async () => { await waitStage("model"); return model("draft"); });
		const options = vi.fn(async ({ signal }: { signal: AbortSignal }) => {
			await waitStage("options"); signal.throwIfAborted(); return {};
		});
		const prepareExecution = vi.fn(), shared = createDrafterPlanSource({ sessionID: "shared", draftModel: selectModel, getDraftOptions: options, complete });
		const propose = (owner: AbortController, proposalIndex: number, turnID = "turn-1") => shared.source.propose({
			startInput: { ...startInput(tool, turnID), sessionID: "shared" },
			settings: { ...settings(2), resourceCacheMaxEntries: 4, predictionTimeoutMs: 1000 },
			data: { tools: new Map([["read", tool]]), schemaHashes: {}, prepareExecution }, definitions: [], candidateNames: ["read"],
			proposalIndex, proposalCount: owners.length, signal: owner.signal,
		});
		const proposals = owners.map((owner, index) => propose(owner, index));
		try {
			await sharing.entered; owners[0]!.abort();
			if (phase === "options") expect(options.mock.calls[0]![0].signal.aborted).toBe(false);
			sharing.release();
			const [cancelled, surviving] = await Promise.all(proposals);
			expect(cancelled).toBeUndefined(); expect(surviving).toMatchObject({ actions: [{ tool: "read" }] });
			expect(selectModel).toHaveBeenCalledOnce(); expect(options).toHaveBeenCalledOnce(); expect(complete).toHaveBeenCalledOnce();
			shared.finishTurn("shared", "turn-1");
			expect(options.mock.calls[0]![0].signal.aborted).toBe(true);
			const later = [new AbortController(), new AbortController()];
			await Promise.all(later.map((owner, index) => propose(owner, index, "turn-2")));
			const warming = prepareExecution.mock.calls.at(-1)![1] as AbortSignal;
			expect(prepareExecution).toHaveBeenCalledTimes(2);
			later[0]!.abort(); expect(warming.aborted).toBe(false);
			later[1]!.abort(); expect(warming.aborted).toBe(true);
			const completionGate = gated(), peer = new AbortController();
			const peers = [peer, phase === "model" ? peer : new AbortController()];
			complete.mockImplementationOnce(async () => assistant([], "stop"));
			complete.mockImplementationOnce(async () => { await completionGate.wait(); return drafterCall({ path: "notes.txt" }); });
			const next = peers.map((owner, index) => propose(owner, index, "turn-3"));
			try {
				await completionGate.entered; expect(await next[0]).toBeUndefined();
				const warming = prepareExecution.mock.calls.at(-1)![1] as AbortSignal;
				expect(warming.aborted, "an empty response cannot retire its live peer").toBe(false);
				completionGate.release(); expect(await next[1]).toMatchObject({ actions: [{ tool: "read" }] });
				expect(warming.aborted).toBe(false);
				shared.finishTurn("shared", "turn-3"); expect(warming.aborted).toBe(true);
			} finally { completionGate.release(); peers.forEach(owner => owner.abort()); await Promise.allSettled(next); }
		} finally {
			sharing.release(); owners.forEach(owner => owner.abort()); shared.finishSession(); await Promise.allSettled(proposals);
		}
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
			fingerprint: () => "runtime",
			execute: async (context) => {
				const output = await execute(context);
				return testBranch(output, {
					backend: "runtime",
					executionFingerprint: context.action.executionFingerprint,
					validate: async () => ({ status: "valid", metrics: { durationMs: 0, bytesRead: 0, filesRead: 0, mode: "exact" } }), // Fixed fixture inputs.
				});
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
	const grepTool: AgentTool<typeof grepSchema> = { name: "grep", label: "grep", description: "grep", parameters: grepSchema, execute: async () => textResult("notes.txt:1:one") };
	const readTool: AgentTool<typeof readSchema> = { name: "read", label: "read", description: "read", parameters: readSchema, execute: async () => textResult("one") };
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
