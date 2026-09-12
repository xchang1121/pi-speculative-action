import { deferred, nextTurn } from "./async.ts";
import { writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import { testModel as model } from "./model.ts";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, SimpleStreamOptions, ThinkingLevel } from "@earendil-works/pi-ai";
import { createLsTool, createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { createThinkThreadExecutionWorld } from "../src/thinkthread/execution-world.ts";
import { withThinkThreadProfileLifecycle } from "../src/thinkthread/profile-extension.ts";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionSemanticsRegistry, KEYABLE_TOOLS, PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld } from "../src/agent-execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createDrafterPlanSource } from "../src/drafter-plan-source.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { PI_READ_RANGE_PROJECTION_RULE, withPiProjectionCoverage } from "../src/pi-read-projection.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { MaterializedSpeculativeCandidate, SpeculativeActionEvent } from "../src/runtime.ts";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
import type { ToolSettlement } from "../src/tool-settlement.ts";
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
			const binding = deferred(), bindingEntered = deferred();
			const identities: object[] = [], sameIdentity: boolean[] = [];
			const complete = vi.fn(async () => { throw new Error("unexpected inference"); });
			const host = createSpeculativeActionHost("session", { cwd, complete, executionWorlds: [],
				resolveInvocation: async (_tool, input) => {
					if ((input as { path: string }).path === "notes.txt") { bindingEntered.resolve(); await binding.promise; }
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
				await bindingEntered.promise;
				const deadline = deferred<boolean>(), timer = setTimeout(() => deadline.resolve(false), 2000);
				try {
					expect(await Promise.race([gates[1]!.entered.promise.then(() => true), deadline.promise])).toBe(true);
				} finally { clearTimeout(timer); }
				expect(native.mock.calls).toEqual([[1]]);
				binding.resolve(); await Promise.all(gates.map(({ entered }) => entered.promise));
				for (const index of order) {
					gates[index]!.done.resolve();
					expect(await results[index]).toEqual(await tool.execute("oracle", inputs[index]!));
				}
				await host.finishTurn("turn-1", true);
				expect(feedback).toEqual(order.map((index) => index + 1));
				expect(sameIdentity).toEqual([true, true]);
				expect(native.mock.calls).toEqual([[1], [0]]); expect(complete).not.toHaveBeenCalled();
			} finally {
				binding.resolve();
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
				const finished = [deferred(), deferred(), deferred()], order: number[] = [], events: SpeculativeActionEvent<string>[] = [];
				const complete = vi.fn<Parameters<typeof createDrafterPlanSource>[0]["complete"]>(async () => message);
				const host = createSpeculativeActionHost("session", { cwd, complete, preflight: () => true,
					draftModel: { ...model("draft"), reasoning: supported, thinkingLevelMap: { xhigh: "high", max: "max" } },
					...(requested === undefined ? {} : { getDraftOptions: () => options }),
					getSettings: () => ({ ...settings(), drafterMaxTokens: 128, drafterMaxDepth: 1, maxConcurrentActions: supported ? 1 : 3 }),
					onEvent: (event) => { events.push(event); },
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
		for (const content of [[], [valid, valid], [valid, { type: "toolCall" as const, id: "disabled", name: "bash", arguments: {} }]]) {
			reply = assistant(content, "toolUse");
			expect(await controller.source.propose(request)).toBeUndefined();
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
				const prepareWorld = vi.fn(async (_input: { signal?: AbortSignal }) => {});
				let predictions = origin === "prediction";
				const host = createSpeculativeActionHost(`session-${turnID}`, {
					cwd,
					getSettings: () => ({ ...settings(), drafterEnabled: predictions, drafterMaxDepth: 0, tools: [toolName] }),
					draftModel: model("draft"),
					complete: async () =>
						assistant([{ type: "toolCall", id: `draft-${toolName}`, name: toolName, arguments: proposal }], "toolUse"),
					preflight: (request) => { permissions.push(request); return true; },
					resolveInvocation: () => resourceExecution ? { ...invocation!, filesystem: async (view, request) => {
						await speculativeExecution();
						return resourceExecution(view, request);
					} } : invocation,
					executionWorlds: [{ ...sandbox, speculation: { ...sandbox.speculation!, prepare: prepareWorld } }],
					onEvent: (event) => {
						events.push(event);
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

	it.each(["validation", "reader", "opaque", "closing"])("owns an output-only projection through %s and Actor settlement", async (phase) => {
		const cwd = await temporaryWorkspace(), tool = createReadTool(cwd);
		const args = { path: "notes.txt", offset: 2, limit: 1 };
		const expected = await tool.execute("control", args);
		const ready = deferred<void>(), entered = deferred<void>(), release = deferred<void>();
		const worldDisposed = vi.fn(), committed = vi.fn();
		const actor = vi.fn(() => tool.execute("actor", args));
		const events: SpeculativeActionEvent<string>[] = [];
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
		const host = createSpeculativeActionHost("session", {
			cwd, getSettings: () => ({ ...settings(), drafterMaxDepth: 0 }), draftModel: model("draft"),
			complete: async () => drafterCall({ path: "notes.txt" }), preflight: () => true,
			projectionRules: [rule], executionWorlds: [world],
			onEvent: (event) => { events.push(event); if (event.type === "candidate" && event.state.status === "succeeded") ready.resolve(); },
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
			const fallback = phase === "opaque" || phase === "closing";
			expect(actor).toHaveBeenCalledTimes(fallback ? 1 : 0);
			expect(committed).toHaveBeenCalledTimes(fallback ? 0 : 1);
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

	it.each([false, true])("only promotes proven host observations, independently of prediction (ThinkThread=%s)", async (thinkthread) => {
		const cwd = await temporaryWorkspace(process.env.THINKTHREAD_FS ?? path.join(process.cwd(), "bench")), file = path.join(cwd, "notes.txt");
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
				expect(actor, turnID).toHaveBeenCalledTimes(calls + (process.platform === "win32" ? 1 : calls > 1 ? -1 : 0));
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
				await bindingStarted.promise; profile = "next"; mutableArgs.command = "changed during binding";
				releaseBinding.resolve();
				await outcome;
				await host.finishTurn("turn-1", true);
				expect(resolveInvocation).toHaveBeenCalledOnce(); expect(actor).toHaveBeenCalledTimes(mode === "binding-error" ? 0 : 1);
				if (keyed) { expect(settled).toHaveBeenCalledOnce(); expect(settled.mock.calls[0][0].action).toBe(boundKey); }
			} finally { releaseBinding.resolve(); await host.dispose(); }
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
			const result = (value: number) => ({ content: [{ type: "text" as const, text: String(value) }], details: {} });
			const actor = vi.fn(async (input: { value: unknown }) => result(boundary === "input" ? inspect(input.value) : profile));
			const tool: AgentTool<typeof mockToolSchema> = { name: "inspect", label: "inspect", description: "Pure fixture inspection",
				parameters: mockToolSchema, prepareArguments: (input) => { const { value } = input as { value: number }; return { value: boundary === "input" ? make(value) : value }; },
				execute: async (_id, input) => actor(input) };
			const execute = vi.fn((context: Parameters<SpeculativeAgentExecutionWorld["speculation"]["execute"]>[0]) => {
				const binding = context.action.executionContext as { identity?: { value: unknown }; process?: { value: unknown } };
				return { result: result(inspect(boundary === "input" ? (context.args as { value: unknown }).value : binding[boundary]!.value)), isError: false };
			});
			const resolveInvocation = vi.fn(() => boundary === "input" ? undefined : { executor: "fixture.v1", ...(boundary === "identity"
				? { identity: { value: make(profile) } } : { process: { command: "inspect", cwd, environment: {}, shell: process.execPath,
					shellArgs: [], commandTransport: "argv" as const, value: make(profile) } }) });
			const host = createSpeculativeActionHost("shapes", { cwd, actionSemantics,
				getSettings: () => ({ ...settings(), drafterGateEnabled: false, drafterMaxDepth: 0, resourceCacheMaxEntries: 0, tools: ["inspect"] }),
				draftModel: model("draft"), complete: async () => assistant([{ type: "toolCall", id: "draft", name: "inspect", arguments: { value: 0 } }], "toolUse"),
				resolveInvocation, preflight: () => true, executionWorlds: [mockRuntimeWorld(execute, disposed)],
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
		const cwd = await temporaryWorkspace(), started = deferred<void>(), finish = deferred<void>(), completed = deferred<void>();
		const actor = vi.fn(async () => ({ content: [{ type: "text" as const, text: "tail arguments: -n 2" }], details: {} }));
		const tool: AgentTool<typeof bashSchema> = { name: "bash", label: "bash", description: "bash", parameters: bashSchema, execute: actor };
		const dispose = vi.fn();
		const sandbox = mockRuntimeWorld(async () => {
			started.resolve(); await finish.promise;
			return { result: { content: [{ type: "text", text: "tail arguments: -n 3" }], details: {} }, isError: false };
		}, dispose);
		let allowed = mode !== "preflight";
		const preflight = vi.fn(({ signal }: { signal: AbortSignal }) => {
			expect(signal).toBeInstanceOf(AbortSignal);
			return phase === "running" ? allowed : allowed ? { ok: true as const } : { ok: false as const, reason: "host_denied", detail: "restricted" };
		});
		const events: SpeculativeActionEvent<string>[] = [];
		const host = createSpeculativeActionHost("session", {
			cwd, getSettings: () => ({ ...settings(), tools: ["bash"], drafterMaxDepth: 0 }), draftModel: model("draft"),
			complete: vi.fn().mockResolvedValueOnce(assistant([{ type: "toolCall", id: "draft-bash", name: "bash",
				arguments: { command: "printf data 2>&1 | tail -n 3" } }], "toolUse")).mockResolvedValue(assistant([], "stop")),
			preflight: mode === "missing" ? undefined : preflight, executionWorlds: [sandbox, sandbox],
			resolveInvocation: (name, args) => resolvePiToolInvocation(name, args, { cwd, environment: {}, shellPath: process.execPath }),
			onEvent: (event) => {
				events.push(event);
				if (event.type === "candidate" && event.state.status === "succeeded" || event.type === "prediction" && event.settlement.observation === "unobserved") completed.resolve();
			},
		});
		try {
			await host.startTurn(startInput(tool));
			if (["preflight", "missing"].includes(mode!)) await completed.promise;
			else { await started.promise; if (phase === "completed") { finish.resolve(); await completed.promise; } }
			if (mode === "recheck") allowed = false;
			const output = await host.execute({ turnID: "turn-1", id: "actor-bash", tool: "bash",
				args: { command: `printf data 2>&1 | tail -n ${mode === "suffix" ? 2 : 3}` }, tools: [tool] }, undefined, actor);
			expect(output.content).toEqual([{ type: "text", text: "tail arguments: -n 2" }]);
			expect(actor).toHaveBeenCalledOnce();
			await waitFor(() => events.some((event) => event.type === "actor_action"));
			expect(preflight).toHaveBeenCalledTimes(mode === "missing" ? 0 : mode === "preflight" || mode === "suffix" && phase === "running" ? 1 : 2);
			if (mode === "recheck") expect(events.find((event) => event.type === "actor_action")).toMatchObject({ settlement: {
				rejections: [{ cause: { stage: "authorization", code: "permission_or_policy_changed", ...(phase === "completed" ? { detail: "restricted" } : {}) } }],
			} });
			if (mode === "preflight") expect(events.find((event) => event.type === "prediction")).toMatchObject({ settlement: {
				cause: { stage: "admission", code: phase === "running" ? "permission_or_policy" : "host_denied" },
			} });
		} finally { finish.resolve(); await host.dispose(); }
		expect(dispose).toHaveBeenCalledOnce();
	});

	it.each(["actor", "drafter"] as const)("rebases PatternAware from an authoritative %s result within the same turn", async (origin) => {
		const { cwd, patternSettings, patternStore, grepTool, readTool, materialized } = await patternRebaseFixture();
		const tools = [grepTool, readTool], ready = deferred<void>();
		const world = toolRuntimeWorld();
		const fingerprint = vi.fn(origin === "drafter" ? world.speculation.fingerprint : () => { throw new Error("Fixture world unavailable"); });
		const host = createSpeculativeActionHost("probe", {
			cwd,
			getSettings: () => ({ ...settings(origin === "drafter" ? 1 : 4), drafterEnabled: origin === "drafter",
				drafterMaxDepth: 0, tools: ["grep", "read"], patternAware: patternSettings }),
			patternStore, draftModel: model("draft"), preflight: () => true,
			complete: async () => assistant([{ type: "toolCall", id: "draft-grep", name: "grep",
				arguments: { pattern: "one", path: "." } }], "toolUse"),
			executionWorlds: [{ ...world, speculation: { ...world.speculation, fingerprint } }],
			onCandidateMaterialized: (candidate) => { materialized.push(candidate); },
			onEvent: (event) => {
				if (event.type === "candidate" && event.candidate.source === "drafter" && event.state.status === "succeeded") ready.resolve();
			},
		});
		const call = { turnID: "probe:turn", id: "actor-grep", tool: "grep", args: { pattern: "one", path: "." }, tools };
		try {
			await host.startTurn({ ...startInput(grepTool, call.turnID),
				context: { systemPrompt: "system", messages: [], tools }, tools });
			expect(fingerprint).not.toHaveBeenCalled();
			if (origin === "drafter") { await ready.promise; expect(fingerprint).toHaveBeenCalled(); }
			else expect(materialized).toHaveLength(0);
			const execute = vi.fn(() => grepTool.execute(call.id, call.args));
			const result = await host.execute(call, undefined, execute);
			expect(result.content).toEqual((await grepTool.execute("oracle", call.args)).content);
			expect(execute).toHaveBeenCalledTimes(origin === "drafter" ? 0 : 1);
			await waitFor(() => materialized.some((candidate) => candidate.source === "pattern_aware" && candidate.tool === "read"));
			expect(materialized).toContainEqual(expect.objectContaining({
				sessionID: "probe", turnID: call.turnID, expectedDecisionSequence: 2, latestDecisionSequence: 2,
				source: "pattern_aware", tool: "read", input: { path: "notes.txt" },
			}));
			expect(patternStore.recent("probe")).toHaveLength(0);
			await host.finishTurn(call.turnID);
		} finally { await host.dispose(); }
	});

	it("turns one sidecar fork batch into safe parallel actions with real execution ahead", async () => {
		const cwd = await temporaryWorkspace();
		const permissionEntered = deferred<void>(), permissionReleased = deferred<void>();
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
			preflight: async ({ args }) => {
				if ((args as { path: string }).path === "notes.txt.sibling") {
					permissionEntered.resolve(); await permissionReleased.promise;
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
			await permissionEntered.promise;
			await waitFor(() => events.some((event) => event.type === "candidate" && event.turnID === "fork-hit" && event.state.status === "succeeded"));
			expect(events.filter((event) => event.type === "candidate" && event.turnID === "fork-hit" && event.state.status === "running")).toHaveLength(1);
		} finally { permissionReleased.resolve(); }
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
		const missed = vi.fn(async () => ({ content: [{ type: "text" as const, text: "actor-miss.txt" }], details: {} }));
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

	it.each(["context", "model", "options"] as const)("skips an ineligible Drafter after %s preparation without changing Actor history", async (phase) => {
		const cwd = await temporaryWorkspace();
		const entered = deferred<void>(), release = deferred<void>(), settled = deferred<void>();
		const complete = vi.fn(async () => drafterCall({ path: "notes.txt" }));
		const tool = createReadTool(cwd);
		const world = toolRuntimeWorld(), prepare = vi.fn(async () => {});
		const host = createSpeculativeActionHost("session", {
			cwd,
			getSettings: settings,
			draftModel: async () => {
				if (phase === "model") { entered.resolve(); await release.promise; }
				return phase === "context" ? { ...model("short"), contextWindow: 32, maxTokens: 16 } : model("draft");
			},
			getDraftOptions: async () => {
				if (phase === "options") { entered.resolve(); await release.promise; }
				return {};
			},
			complete,
			executionWorlds: [{ ...world, speculation: { ...world.speculation, prepare } }],
			preflight: () => true,
			onEvent: (event) => { if (event.type === "source_request") settled.resolve(); },
		});
		let closing: Promise<void> | undefined, closed = false;
		try {
			await host.startTurn({
				...startInput(tool),
				context: { systemPrompt: "x".repeat(128), messages: [], tools: [tool] },
			});
			if (phase === "context") await settled.promise;
			else {
				await entered.promise;
				closing = host.dispose().then(() => { closed = true; });
				await nextTurn();
				expect(closed).toBe(false);
				release.resolve(); await closing;
			}
			expect(complete).not.toHaveBeenCalled();
			expect(prepare).not.toHaveBeenCalled();
		} finally { release.resolve(); await closing; await host.dispose(); }
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

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for speculative runtime");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}
