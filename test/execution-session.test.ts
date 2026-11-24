import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect, test, vi } from "vitest";
import { fauxText, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createReadTool, createWriteTool } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LocalExecutionSession } from "../src/runtime-engine.ts";
import type { ExecutionCall, ExecutionSession, RunCompletion } from "../src/execution-session.ts";
import type { SpeculativeExecutionRoute, WorldBranch, ToolSettlement } from "../src/execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createActorForkPlanSource } from "../src/self-speculation.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { cause, zeroValidationMetrics } from "../src/settlement.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { resourceWorld, systemHost } from "./agent-system-fixture.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { testModel, toolUse } from "./model.ts";
import { resourceBudgetError } from "../src/primitives.ts";

const read = (name: string): ExecutionCall => ({ tool: "read", input: { path: name } });
const shared: SpeculativeExecutionRoute = {
	backend: "memory",
	fingerprint: "memory",
	scope: "fallback",
	isolation: "resource_snapshot",
	reuse: "shared_result",
};
const exclusive: SpeculativeExecutionRoute = {
	...shared,
	isolation: "workspace_branch",
	reuse: "exclusive_branch",
};

test("skips unrelated file snapshots and attempts a budget-limited current query once", async () => {
	const attempts: string[] = [];
	const session = new LocalExecutionSession<string>({
		settings: () => ({ enabled: true, maxConcurrentActions: 3, resourceCacheMaxEntries: 20 }),
		key: async (call) => PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/workspace", "memory", { fingerprint: "test" }),
		route: async () => shared, authorize: async () => ({ ok: true }), capture: async () => undefined,
		native: async () => "native",
		fork: async (call) => {
			const file = String(call.input.path);
			return { backend: "memory", output: file, resources: [], executionMetrics: {}, capturedBytes: 1,
				inputResources: [{ path: path.resolve("/workspace", file === "disjoint" ? file : "target") }],
				compatibility: { status: "compatible", executionFingerprint: "test", backend: "memory" },
				reconstructionScope: "current_action", reconstruct: async () => {
					attempts.push(file); throw resourceBudgetError("test byte limit");
				},
				validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
				commit: async () => file, dispose() {} };
		},
	});
	try {
		for (const file of ["first", "second", "disjoint"]) await session.speculate(read(file)).completion;
		expect(await session.execute(read("target"))).toMatchObject({ output: "native", served: false });
		expect(attempts).toHaveLength(1);
		expect(attempts).not.toContain("disjoint");
		expect(await session.execute(read("first"))).toMatchObject({ served: true, reuseKind: "exact" });
	} finally {
		await session.close();
	}
});

test("joins independent handles at capacity, admits an Actor, and validates retained work across detours in both directions", async () => {
	const gate = Promise.withResolvers<void>();
	const started = Promise.withResolvers<void>();
	const files = new Map<string, string>([["A", "original"]]);
	const key = async (call: ExecutionCall) =>
		PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/workspace", "memory", { fingerprint: "test" });
	const capture = (call: ExecutionCall, output: string): WorldBranch<string> => ({
		backend: "memory",
		resources: [],
		capturedBytes: output.length,
		executionMetrics: {},
		output,
		compatibility: { status: "compatible", executionFingerprint: "test", backend: "memory" },
		validate: async () =>
			(files.get(String(call.input.path)) ?? "") === output
				? { status: "valid", metrics: zeroValidationMetrics() }
				: { status: "stale", cause: cause("freshness", "changed"), metrics: zeroValidationMetrics() },
		commit: async () => output,
		dispose() {},
	});
	let forks = 0;
	const opened = Promise.withResolvers<void>();
	const captureGate = Promise.withResolvers<void>();
	const actorStarted = Promise.withResolvers<void>();
	const actorGate = Promise.withResolvers<void>();
	const native = vi.fn(async (call: ExecutionCall) => {
		if (call.input.path === "actor-first") {
			actorStarted.resolve();
			await actorGate.promise;
		}
		return files.get(String(call.input.path)) ?? "";
	});
	const session = new LocalExecutionSession<string>({
		settings: () => ({ enabled: true, maxConcurrentActions: 1, resourceCacheMaxEntries: 32 }),
		key,
		route: async () => shared,
		authorize: async () => ({ ok: true }),
		fork: async (call) => {
			forks++;
			started.resolve();
			await gate.promise;
			return capture(call, files.get(String(call.input.path)) ?? "");
		},
		capture: async (call) => {
			if (call.input.path === "actor-first") {
				opened.resolve();
				await captureGate.promise;
			}
			return { route: shared, seal: async (output) => capture(call, output!), dispose() {} };
		},
		native,
	});
	try {
		const first = session.speculate(read("A"));
		const second = session.speculate(read("A"));
		await started.promise;
		expect(await session.speculate(read("B")).completion).toMatchObject({
			status: "rejected",
			reason: "concurrency_limit",
		});
		expect(await session.speculate(read("dependent"), { after: first.runID }).completion).toMatchObject({
			status: "rejected",
			reason: "concurrency_limit",
		});
		session.release(first.runID);
		session.release(first.runID);
		expect(await first.completion).toMatchObject({ status: "cancelled" });
		expect(await session.execute(read("B"))).toMatchObject({ served: false });
		const actor = session.execute(read("A"));
		gate.resolve();
		expect(await second.completion).toMatchObject({ status: "completed", output: "original" });
		const produced = await actor;
		expect(produced).toMatchObject({ served: true, reuseKind: "exact", output: "original" });
		session.release(second.runID);
		for (let i = 0; i < 12; i++) await session.execute(read(String(i)));
		expect(await session.execute(read("A"))).toMatchObject({
			served: true,
			producerRunID: produced.producerRunID,
		});
		expect(await session.speculate(read("B")).completion).toMatchObject({
			status: "completed",
			served: true,
			reuseKind: "exact",
		});
		const authoritative = session.execute(read("actor-first"));
		await opened.promise;
		const secondActor = session.execute(read("actor-first"));
		const follower = session.speculate(read("actor-first"));
		captureGate.resolve();
		await actorStarted.promise;
		actorGate.resolve();
		expect(await authoritative).toMatchObject({ served: false });
		expect(await secondActor).toMatchObject({ served: true, reuseKind: "exact" });
		expect(native.mock.calls.filter(([call]) => call.input.path === "actor-first")).toHaveLength(1);
		expect(await follower.completion).toMatchObject({
			status: "completed",
			served: true,
			reuseKind: "exact",
		});
		expect(forks).toBe(1);
		files.set("A", "changed");
		expect(await session.execute(read("A"))).toMatchObject({ served: false, output: "changed" });
		expect(await session.speculate(read("A")).completion).toMatchObject({
			status: "completed",
			served: true,
			output: "changed",
		});
	} finally {
		gate.resolve();
		captureGate.resolve();
		actorGate.resolve();
		await session.close();
	}
});

test("trimming an unreleased handle retains its shared result after the remaining alias releases", async () => {
	const branch: WorldBranch<string> = {
		backend: "memory",
		output: "file",
		resources: [],
		capturedBytes: 4,
		executionMetrics: {},
		compatibility: { status: "compatible", executionFingerprint: "memory", backend: "memory" },
		validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
		commit: async () => "file",
		dispose: vi.fn(),
	};
	const session = new LocalExecutionSession<string>({
		settings: () => ({ resourceCacheMaxEntries: 1 }),
		key: async (call) =>
			PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/root", "memory", { fingerprint: "memory" }),
		route: async () => shared,
		authorize: async () => ({ ok: true }),
		fork: async () => branch,
		capture: async () => undefined,
		native: async () => "native",
	});
	try {
		const owner = session.speculate(read("one"));
		await owner.completion;
		const alias = session.speculate(read("one"));
		expect(await alias.completion).toMatchObject({ served: true, producerRunID: owner.runID });
		session.release(alias.runID);
		expect(await session.execute(read("one"))).toMatchObject({ served: true, producerRunID: owner.runID });
	} finally {
		await session.close();
	}
	expect(branch.dispose).toHaveBeenCalledOnce();
});

test("sealing an authoritative mutation keeps its committed inputs and retires its effect branch", async () => {
	const takeInputs = vi.fn(async () => undefined);
	const dispose = vi.fn();
	const commit = vi.fn();
	const session = new LocalExecutionSession<number>({
		settings: () => ({ resourceCacheMaxEntries: 4 }),
		key: async (call) => PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/root", "memory", { fingerprint: "memory" }),
		route: async () => exclusive,
		authorize: async () => ({ ok: true }),
		fork: vi.fn(),
		native: async () => 1,
		capture: async () => ({
			route: exclusive,
			dispose() {},
			seal: () => ({
				backend: "memory", output: 1, resources: [], capturedBytes: 0, executionMetrics: {},
				compatibility: { status: "compatible", executionFingerprint: "memory", backend: "memory" },
				validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
				commit,
				dispose,
				takeCommittedInputs: takeInputs,
			}),
		}),
	});
	try {
		const write = { tool: "write", input: { path: "A", content: "data" } };
		expect(await session.execute(write)).toMatchObject({ served: false });
		expect(session.snapshot().branchEntries).toBe(0);
		expect(await session.execute(write)).toMatchObject({ served: false });
		expect(commit).not.toHaveBeenCalled();
		expect(takeInputs).toHaveBeenCalledTimes(2);
		expect(dispose).toHaveBeenCalledTimes(2);
	} finally {
		await session.close();
	}
});

test("after reads private writes; only a proved Actor adoption exposes their effects", async () => {
	let disk = "before";
	const images = new WeakMap<WorldBranch<string>, string>();
	const disposed = vi.fn<(id: string) => void>();
	const write: ExecutionCall = { tool: "write", input: { path: "A", content: "after" } };
	const session = new LocalExecutionSession<string>({
		settings: () => ({ enabled: true, maxConcurrentActions: 2, resourceCacheMaxEntries: 16 }),
		key: async (call) =>
			PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/root", "schema", { fingerprint: "memory" }),
		route: async (call, _key, _signal, parent) => (call.tool === "write" || parent ? exclusive : shared),
		authorize: async () => ({ ok: true }),
		fork: async (call, _key, _route, id, _signal, _resources, parent) => {
			const baseline = parent ? images.get(parent)! : disk;
			const value = call.tool === "write" ? String(call.input.content) : baseline;
			const branch: WorldBranch<string> = {
				backend: "memory",
				output: value,
				capturedBytes: 1,
				resources: ["A"],
				executionMetrics: {},
				compatibility: { status: "compatible", executionFingerprint: "memory", backend: "memory" },
				checkpoint: { backend: "memory", id, lineage: "A", depth: parent ? 1 : 0 },
				validate: async () =>
					disk === baseline
						? { status: "valid", metrics: zeroValidationMetrics() }
						: { status: "stale", cause: cause("freshness", "changed"), metrics: zeroValidationMetrics() },
				commit: async () => {
					if (call.tool === "write") disk = value;
					return value;
				},
				dispose() {
					disposed(id);
					images.delete(branch);
				},
			};
			images.set(branch, value);
			return branch;
		},
		capture: async () => undefined,
		native: async () => disk,
	});
	try {
		const parent = session.speculate(write);
		const child = session.speculate(read("A"), { after: parent.runID });
		expect(await child.completion).toMatchObject({ status: "completed", output: "after" });
		expect(await session.execute(read("A"))).toMatchObject({ served: false, output: "before" });
		expect(disk).toBe("before");
		expect(await session.execute(write)).toMatchObject({ served: true, output: "after" });
		expect(disposed).toHaveBeenCalledWith(parent.runID);
		expect(await session.execute(read("A"))).toMatchObject({ served: true, output: "after" });
		expect(session.snapshot()).toMatchObject({ branchEntries: 0, exclusiveCandidates: 0 });
		disk = "later";
		const late = session.speculate(read("late"), { after: parent.runID });
		expect(await late.completion).toMatchObject({ status: "completed", output: "later" });
		expect(disposed.mock.calls).toEqual([[parent.runID], [child.runID]]);
	} finally {
		await session.close();
	}
});

test.each([true, false])("credits two exact predictions by the returned producer (merged: %s)", async (merged) => {
	const tools = [createReadTool("/virtual")];
	const finished = Promise.withResolvers<void>();
	const actorWaiting = Promise.withResolvers<void>();
	const bothIssued = Promise.withResolvers<void>();
	const aliasReceipt = Promise.withResolvers<void>();
	const credited = Promise.withResolvers<void>();
	const feedback: Array<[string, string, boolean]> = [];
	const value = {
		output: { isError: false, result: { content: [fauxText("file")], details: {} } },
		toolExecution: new TimelineInterval(performance.now(), performance.now()),
		producerRunID: "run-1",
		reuseKind: "exact" as const,
	};
	let handles = 0;
	const session: ExecutionSession<ToolSettlement> = {
		speculate() {
			const second = ++handles === 2;
			if (second) bothIssued.resolve();
			return {
				runID: "run-" + handles,
				completion: finished.promise.then(async () => {
					if (second) await aliasReceipt.promise;
					const producerRunID = second && !merged ? "run-2" : value.producerRunID;
					return { ...value, status: "completed", served: second && merged, producerRunID };
				}),
			};
		},
		async execute() {
			actorWaiting.resolve();
			await finished.promise;
			return { ...value, served: true };
		},
		release() {},
		async flush() {},
		async close() {},
	};
	const fork = createActorForkPlanSource();
	const host = createSpeculativeActionHost("coalesced", {
		cwd: "/virtual",
		executionSession: session,
		actorForkPlanSource: fork,
		getSettings: () => ({
			enabled: true,
			drafterEnabled: true,
			candidateLimit: 1,
			drafterMaxDepth: 0,
			selfSpeculation: { enabled: true, forkEnabled: true, forkActionEnabled: true, forkTransport: "sidecar" },
		}),
		complete: async () => toolUse("read", { path: "same" }),
	});
	host.runtime.subscribe({
		prediction_feedback: ({ settlement, served }) => {
			if (settlement.observation === "observed" && settlement.match.matched) {
				feedback.push([settlement.prediction.source, settlement.match.adoption.status, served]);
				if (feedback.length === 2) credited.resolve();
			}
		},
	});
	try {
		fork.startTurn("turn");
		await host.startTurn({
			turnID: "turn",
			tools,
			actorModel: testModel(),
			actorOptions: {},
			context: { messages: [], tools },
		});
		fork.publish("turn", [
			{ id: "peer", calls: [{ id: "forked", index: 0, tool: "read", input: { path: "same" } }] },
		]);
		await bothIssued.promise;
		const actor = host.execute({ tool: "read", turnID: "turn", tools, args: { path: "same" } });
		await actorWaiting.promise;
		finished.resolve();
		await actor;
		await host.finishTurn("turn");
		aliasReceipt.resolve();
		await credited.promise;
		expect(feedback.map(([source]) => source).sort()).toEqual(["drafter", "self-speculation"]);
		expect(feedback.map(([, status, served]) => [status, served]).sort()).toEqual([
			["adopted", true],
			merged ? ["adopted", true] : ["rejected", false],
		]);
	} finally {
		finished.resolve();
		aliasReceipt.resolve();
		await host.dispose();
	}
});

test("session close awaits async or rejecting release before close and drains source-request delivery", async () => {
	const entered = Promise.withResolvers<void>();
	const finish = Promise.withResolvers<void>();
	const crossDone = Promise.withResolvers<RunCompletion<ToolSettlement>>();
	const releaseGate = Promise.withResolvers<void>();
	const sessionClosed = Promise.withResolvers<void>();
	const order: string[] = [];
	const unhandled: unknown[] = [];
	const onUnhandled = (reason: unknown) => unhandled.push(reason);
	process.on("unhandledRejection", onUnhandled);
	const tools = [createReadTool("/virtual")];
	const fork = createActorForkPlanSource();
	vi.spyOn(fork.source, "start").mockImplementation((turn) => {
		if (turn.startInput.turnID === "t-first")
			void turn.issue({ source: "self-speculation", group: "g", id: "c", tool: "read", input: { path: "hold-cross.txt" }, horizon: 2 });
		if (turn.startInput.turnID === "turn")
			void turn.request("drafter", "continuation", turn.signal, async () => 1);
	});
	const okCompletion = (runID: string): RunCompletion<ToolSettlement> => ({
		status: "completed",
		output: { isError: false, result: { content: [fauxText("ok")], details: {} } },
		served: false,
		producerRunID: runID,
		toolExecution: new TimelineInterval(0, 0),
	});
	let activeSlot: string | undefined;
	const slotAttempts = new Map<string, number>();
	const host = createSpeculativeActionHost("drain", {
		cwd: "/virtual",
		actorForkPlanSource: fork,
		complete: vi.fn(),
		executionSession: {
			speculate(call) {
				const runID = String(call.input.path);
				if (runID === "hold-cross.txt") return { runID, completion: crossDone.promise };
				if (runID.startsWith("slot-")) {
					if (activeSlot) return { runID, completion: Promise.resolve({ status: "rejected", reason: "concurrency_limit" }) };
					if (runID === "slot-later.txt" || runID === "slot-old.txt") activeSlot = runID;
				}
				return { runID, completion: Promise.resolve(okCompletion(runID)) };
			},
			execute: vi.fn(),
			async release(id) {
				if (id.startsWith("slot-")) {
					if (id === "slot-later.txt" || id === "slot-old.txt") {
						const n = (slotAttempts.get(id) ?? 0) + 1;
						slotAttempts.set(id, n);
						if (n === 1) throw new Error("transient disconnect");
						if (activeSlot === id) activeSlot = undefined;
					}
					return;
				}
				if (id === "retry-hang.txt") {
					const n = (slotAttempts.get(id) ?? 0) + 1;
					slotAttempts.set(id, n);
					if (n === 1) {
						order.push("retry_1");
						throw new Error("first release failed");
					}
					order.push("retry_2_start");
					await releaseGate.promise;
					order.push("retry_2_end");
					return;
				}
				if (id === "reject.txt") {
					await new Promise<void>((resolve) => setImmediate(resolve));
					throw new Error("release transport closed");
				}
				order.push("release_start");
				await releaseGate.promise;
				order.push("release_end");
			},
			async flush() {},
			async close() {
				order.push("close");
				sessionClosed.resolve();
			},
		},
		getSettings: () => ({ enabled: true, drafterEnabled: false }),
	});
	host.runtime.subscribe({
		source_request: async ({ request }) => {
			if (request.request.kind !== "continuation" || request.request.turnID !== "turn") return;
			entered.resolve();
			await finish.promise;
			order.push("continuation");
		},
		turn_finished: ({ startInput }) => {
			if (startInput.turnID === "t-second") order.push("finish_second");
			if (startInput.turnID === "t-hang-2") order.push("finish_hang_2");
		},
		session_closed: () => { order.push("closed"); },
	});
	const start = (turnID: string) =>
		host.startTurn({ turnID, tools, actorModel: testModel(), actorOptions: {}, context: { messages: [], tools } });
	const preview = (turnID: string, id: string, p: string) =>
		host.previewActorCall({ turnID, id, tool: "read", args: { path: p }, tools });
	try {
		await start("t-retry-1");
		await (await preview("t-retry-1", "pr", "slot-later.txt"))?.completion;
		await host.finishTurn("t-retry-1");
		expect(slotAttempts.get("slot-later.txt")).toBe(1);
		await start("t-retry-2");
		expect((await (await preview("t-retry-2", "pr2", "slot-probe.txt"))?.completion)?.status).toBe("rejected");
		await host.finishTurn("t-retry-2");
		expect(slotAttempts.get("slot-later.txt")).toBe(2);

		await start("t-slot");
		expect((await (await preview("t-slot", "ps", "slot-old.txt"))?.completion)?.status).toBe("completed");
		expect((await (await preview("t-slot", "ps", "slot-repl.txt"))?.completion)?.status).toBe("rejected");
		await expect.poll(() => slotAttempts.get("slot-old.txt")).toBe(1);
		await host.finishTurn("t-slot");
		expect(slotAttempts.get("slot-old.txt")).toBe(2);

		await start("t-hang-1");
		await (await preview("t-hang-1", "ph", "retry-hang.txt"))?.completion;
		await host.finishTurn("t-hang-1");
		await start("t-hang-2");
		await host.finishTurn("t-hang-2");
		expect(order).toEqual(["retry_1", "retry_2_start", "finish_hang_2"]);

		await start("t-first");
		await host.finishTurn("t-first", true);
		await start("t-second");
		crossDone.resolve(okCompletion("hold-cross.txt"));
		await expect.poll(() => order).toContain("release_start");
		await host.finishTurn("t-second");
		expect(order).toEqual(["retry_1", "retry_2_start", "finish_hang_2", "release_start", "finish_second"]);

		await start("turn");
		await entered.promise;
		expect((await (await preview("turn", "pn", "slot-next.txt"))?.completion)?.status).toBe("completed");
		await (await preview("turn", "rej", "reject.txt"))?.completion;
		const closed = host.dispose();
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(order).not.toContain("close");
		releaseGate.resolve();
		await sessionClosed.promise;
		await new Promise<void>((resolve) => setImmediate(resolve));
		finish.resolve();
		await closed;
		expect(order.indexOf("close")).toBeGreaterThan(order.indexOf("retry_2_end"));
		expect(order.indexOf("close")).toBeGreaterThan(order.indexOf("release_end"));
		expect(order.slice(-2)).toEqual(["continuation", "closed"]);
		expect(unhandled).toEqual([]);
	} finally {
		process.off("unhandledRejection", onUnhandled);
		crossDone.resolve({ status: "cancelled", reason: "closed" });
		releaseGate.resolve();
		finish.resolve();
		await host.dispose();
	}
});

test("issues both Drafter calls before a slow scope reply and continues from their ready results", async () => {
	const tools = [createReadTool("/virtual")];
	const classify = Promise.withResolvers<void>();
	const entered = Promise.withResolvers<void>();
	const calls: unknown[] = [];
	let forecasts = 0, continuations = 0;
	const host = createSpeculativeActionHost("scope", {
		cwd: "/virtual",
		executionSession: {
			speculate(call) {
				calls.push(call.input.path);
				return { runID: String(call.input.path), completion: Promise.resolve({
					status: "completed", served: false, output: { result: { content: [fauxText("done")], details: {} }, isError: false },
					toolExecution: new TimelineInterval(performance.now(), performance.now()),
				}) };
			},
			execute: async () => { throw new Error("unexpected Actor call"); },
			release() {}, async flush() {}, async close() {},
		},
		getSettings: () => ({ enabled: true, drafterEnabled: true, candidateLimit: 1, drafterMaxDepth: 1 }),
		complete: async (_model, context) => {
			const privateTool = context.tools?.[0]?.name;
			if (privateTool === "speculative_continuations") {
				entered.resolve();
				await classify.promise;
				return toolUse(privateTool, { scopes: [["a"], ["b"]] });
			}
			if (privateTool === "speculative_observations") forecasts++;
			if (context.messages.some((m) => m.role === "toolResult")) {
				continuations++;
				return fauxAssistantMessage("complete");
			}
			return fauxAssistantMessage(["a", "b"].map((id) => fauxToolCall("read", { path: id }, { id })));
		},
	});
	try {
		await host.startTurn({ turnID: "turn", tools, actorModel: testModel(), actorOptions: {}, context: { messages: [], tools } });
		await entered.promise;
		expect(calls).toEqual(["a", "b"]);
		classify.resolve();
		await expect.poll(() => continuations).toBe(2);
		expect(forecasts).toBe(0);
	} finally {
		classify.resolve();
		await host.dispose();
	}
});

test("a five-method session drives all call generators and streaming preview, including Drafter correction", async () => {
	const tools = [createReadTool("/virtual")];
	const output = (text: string): ToolSettlement => ({
		isError: false,
		result: { content: [fauxText(text)], details: {} },
	});
	const interval = () => new TimelineInterval(performance.now() - 1, performance.now());
	const calls = new Map<string, string>();
	const releases = new Set<string>();
	const root = Promise.withResolvers<RunCompletion<ToolSettlement>>();
	const session: ExecutionSession<ToolSettlement> = {
		speculate(call, opts = {}) {
			expect(Object.keys(call).sort()).toEqual(["id", "input", "tool"]);
			expect(Object.keys(opts).every((k) => k === "after" || k === "signal")).toBe(true);
			const name = String(call.input.path);
			const runID = "r" + calls.size;
			calls.set(runID, name);
			return {
				runID,
				completion:
					name === "root"
						? root.promise
						: Promise.resolve({
								status: "completed",
								output: output(name),
								served: false,
								producerRunID: runID,
								toolExecution: interval(),
							}),
			};
		},
		execute: vi.fn(async (call) => ({
			output: output(String(call.input.path)),
			served: true,
			reuseKind: "exact" as const,
			producerRunID: [...calls].find(([, path]) => path === call.input.path)?.[0],
			toolExecution: interval(),
		})),
		release: (id) => {
			releases.add(id);
		},
		flush: vi.fn(async () => {}),
		close: vi.fn(async () => {}),
	};
	const fork = createActorForkPlanSource();
	const policy = {
		...PATTERN_AWARE_DEFAULTS,
		enabled: true,
		maxFutureGap: 0,
		maxContextLength: 1,
		multiStepEnabled: false,
	};
	const store = new PatternAwareStore(policy);
	for (let i = 0; i < 8; i++) {
		const fact = { sessionID: String(i), turnID: "learn", durationMs: 1, outcome: "success" as const };
		store.observe({ ...fact, tool: "catalog", input: {}, outputPaths: [i + ".txt"] });
		store.observe({ ...fact, tool: "read", input: { path: i + ".txt" } });
		store.finishSession(String(i));
	}
	store.observe({
		sessionID: "live",
		turnID: "previous",
		tool: "catalog",
		input: {},
		durationMs: 1,
		outcome: "success",
		outputPaths: ["learned.txt"],
	});
	const credit = vi.spyOn(store, "settled");
	const host = createSpeculativeActionHost("live", {
		cwd: "/virtual",
		executionSession: session,
		patternStore: store,
		actorForkPlanSource: fork,
		getSettings: () => ({
			enabled: true,
			drafterEnabled: true,
			candidateLimit: 1,
			drafterMaxDepth: 1,
			patternAware: policy,
			selfSpeculation: {
				enabled: true,
				forkEnabled: true,
				forkActionEnabled: true,
				forkTransport: "sidecar",
			},
		}),
		complete: async (_model, context) => {
			if (context.tools?.[0]?.name === "speculative_observations")
				return toolUse("speculative_observations", {
					results: [{ callID: "r", isError: false, content: [fauxText("guess")] }],
				});
			const fact = context.messages.findLast((m) => m.role === "toolResult");
			if (!fact) return toolUse("read", { path: "root" }, { id: "r" });
			if (fact.role !== "toolResult" || fact.toolCallId !== "r") return fauxAssistantMessage("done");
			return toolUse("read", {
				path: fact.content[0]?.type === "text" && fact.content[0].text === "guess" ? "wrong" : "correct",
			});
		},
	});
	try {
		fork.startTurn("turn");
		await host.startTurn({
			turnID: "turn",
			tools,
			actorModel: testModel(),
			actorOptions: {},
			context: { messages: [], tools },
		});
		fork.publish("turn", [
			{ id: "fork", calls: [{ id: "forked", index: 0, tool: "read", input: { path: "sidecar" } }] },
		]);
		await host.previewActorCall({
			turnID: "turn",
			tool: "read",
			id: "actor",
			args: { path: "stream" },
			tools,
		});
		await expect
			.poll(() => [...calls.values()])
			.toEqual(expect.arrayContaining(["wrong", "sidecar", "stream", "learned.txt"]));
		const rootID = [...calls].find(([, p]) => p === "root")![0];
		root.resolve({
			status: "completed",
			output: output("actual"),
			served: false,
			producerRunID: rootID,
			toolExecution: interval(),
		});
		await expect.poll(() => [...calls.values()]).toContain("correct");
		expect(releases.has([...calls].find(([, p]) => p === "wrong")![0])).toBe(true);
		await host.execute({ tool: "read", turnID: "turn", tools, args: { path: "learned.txt" } });
		expect(credit).toHaveBeenCalledWith(
			expect.any(String),
			expect.objectContaining({ match: expect.objectContaining({ matched: true }) }),
			true,
		);
		expect(host.drafterBudgetSnapshot().observations).toEqual({ forecasted: 1, compared: 1, matched: 0 });
	} finally {
		root.resolve({ status: "cancelled", reason: "closed" });
		await host.dispose();
	}
	expect(session.close).toHaveBeenCalledOnce();
});

test("handles unadoptable exclusive branches, preserves active parent branches, and enforces coalesced-handle and cache-admission invariants", async () => {
	const unadoptable: SpeculativeExecutionRoute = { ...exclusive, adoptable: () => false };
	const disposed: string[] = [];
	const makeBranch = (id: string, output: string, capturedBytes = 4, reconstruct?: WorldBranch<string>["reconstruct"]): WorldBranch<string> => ({
		backend: "memory", output, resources: ["shared.txt"], capturedBytes,
		checkpoint: { id, backend: "memory", lineage: id, depth: 1 }, executionMetrics: {},
		compatibility: { status: "compatible", executionFingerprint: "test", backend: "memory" },
		validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }),
		commit: async () => output, reconstruct, dispose() { disposed.push(id); },
	});

	let maxEntries = 8;
	let flightBuilds = 0;
	let bashStarted = Promise.withResolvers<void>();
	let bashFinish = Promise.withResolvers<void>();
	const flightStart = Promise.withResolvers<void>(), flightEnd = Promise.withResolvers<void>();
	const authEntered = Promise.withResolvers<void>(), authGate = Promise.withResolvers<void>();
	const session = new LocalExecutionSession<string>({
		settings: () => ({ maxConcurrentActions: 4, resourceCacheMaxEntries: maxEntries, resourceCacheMaxBytes: 4096 }),
		effect: (key) => PI_ACTION_SEMANTICS.effect(key),
		adoptable: (call) => call.tool !== "bash",
		key: async (call) => PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, "/workspace", "memory", { fingerprint: "test" }),
		route: async (call) => (call.tool === "bash" ? unadoptable : call.tool === "write" ? exclusive : shared),
		authorize: async (call, _key, _route, _signal, actor) => {
			if (actor && call.input.path === "oversize-flight.txt") {
				authEntered.resolve();
				await authGate.promise;
			}
			return { ok: true };
		},
		capture: async () => undefined,
		native: async (call) => {
			if (call.tool === "bash") return "native-actor";
			if (call.input.path === "oversize-flight.txt") flightBuilds++;
			return `native:${JSON.stringify(call.input)}`;
		},
		fork: async (call, _key, _route, runID, signal) => {
			if (call.tool === "bash") {
				bashStarted.resolve();
				await new Promise<void>((resolve, reject) => {
					bashFinish.promise.then(resolve);
					signal.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")), { once: true });
				});
				return makeBranch(runID, "spec:bash");
			}
			const pathArg = String((call.input as { path?: string }).path ?? "");
			if (pathArg === "oversize-flight.txt") {
				flightBuilds++;
				flightStart.resolve();
				await flightEnd.promise;
			}
			if (pathArg.startsWith("oversize")) return makeBranch(runID, "x".repeat(32768), 32768);
			if (pathArg.startsWith("snap-")) {
				const exhaustedSnap = pathArg === "snap-exhausted.txt";
				return {
					...makeBranch(runID, `snap:${pathArg}`, 64, async (req) => {
						const target = String((req.args as { path?: string }).path ?? "");
						if (exhaustedSnap) throw resourceBudgetError("resource_snapshot_budget_exceeded", true);
						if (target === "huge-view.txt") return { output: "v".repeat(32768), capturedBytes: 32768 };
						return { output: `rebuilt:${target}`, capturedBytes: 32 };
					}),
					reconstructionScope: "current_action",
					inputResources: [{ path: "/workspace", descendants: true }],
				};
			}
			return makeBranch(runID, `ok:${pathArg}`, 128);
		},
	});
	try {
		for (const withChild of [false, true]) {
			bashStarted = Promise.withResolvers<void>();
			bashFinish = Promise.withResolvers<void>();
			const parent = session.speculate({ tool: "bash", input: { command: `gen-${withChild}` } });
			const child = withChild ? session.speculate({ tool: "write", input: { path: "out.txt", content: "next" } }, { after: parent.runID }) : undefined;
			await bashStarted.promise;
			expect(await session.execute({ tool: "bash", input: { command: `gen-${withChild}` } })).toMatchObject({ output: "native-actor", served: false });
			bashFinish.resolve();
			if (withChild) {
				expect(await parent.completion).toMatchObject({ status: "completed", output: "spec:bash" });
				expect(await child!.completion).toMatchObject({ status: "completed", output: "ok:out.txt" });
				session.release(child!.runID);
			} else expect(await parent.completion).toMatchObject({ status: "rejected", reason: "candidate_failed" });
			session.release(parent.runID);
		}
		await session.flush();
		disposed.length = 0;
		maxEntries = 1;

		const h1 = session.speculate({ tool: "write", input: { path: "shared.txt", content: "v1" } });
		expect((await h1.completion).status).toBe("completed");
		const h2 = session.speculate({ tool: "write", input: { path: "shared.txt", content: "v1" } });
		expect(await h2.completion).toMatchObject({ status: "completed", served: true });
		const detour = session.speculate({ tool: "write", input: { path: "detour.txt", content: "v1" } });
		expect((await detour.completion).status).toBe("completed");
		expect(session.snapshot().branchEntries).toBe(1);
		expect(disposed).not.toContain(detour.runID);
		const fromDetour = session.speculate({ tool: "write", input: { path: "detour-2.txt", content: "v2" } }, { after: detour.runID });
		expect(await fromDetour.completion).toMatchObject({ status: "completed", output: "ok:detour-2.txt" });
		const childFromFirst = session.speculate({ tool: "write", input: { path: "shared.txt", content: "v2" } }, { after: h1.runID });
		expect(await childFromFirst.completion).toMatchObject({ status: "completed", output: "ok:shared.txt" });
		session.release(fromDetour.runID);
		session.release(detour.runID);
		await session.flush();
		expect(disposed).toContain(detour.runID);
		session.release(childFromFirst.runID);
		session.release(h1.runID);
		session.release(h2.runID);
		await session.flush();

		const flight = session.speculate(read("oversize-flight.txt"));
		await flightStart.promise;
		const joined = session.execute(read("oversize-flight.txt"));
		await authEntered.promise;
		flightEnd.resolve();
		expect((await flight.completion).status).toBe("completed");
		session.release(flight.runID);
		expect(session.snapshot()).toMatchObject({ resultEntries: 0, resultBytes: 0 });
		authGate.resolve();
		expect(await joined).toMatchObject({ output: "x".repeat(32768), served: true });
		expect(flightBuilds).toBe(1);
		expect((await session.execute(read("oversize-flight.txt"))).served).toBe(false);
		expect(flightBuilds).toBe(2);

		maxEntries = 4;
		for (const p of ["small-1.txt", "snap-good.txt", "snap-exhausted.txt"]) {
			await session.speculate(read(p)).completion;
			expect((await session.execute(read(p))).served).toBe(true);
		}
		expect(await session.execute(read("derived-small.txt"))).toMatchObject({ output: "rebuilt:derived-small.txt", served: true });
		expect((await session.execute(read("huge-view.txt"))).served).toBe(true);
		const bigSpec = session.speculate(read("oversize.txt"));
		await bigSpec.completion;
		session.release(bigSpec.runID);
		expect(await session.execute(read("small-1.txt"))).toMatchObject({ output: "ok:small-1.txt", served: true });
		expect(await session.execute(read("derived-small.txt"))).toMatchObject({ output: "rebuilt:derived-small.txt", served: true, reusedInterval: true });
	} finally {
		bashFinish.resolve();
		flightEnd.resolve();
		authGate.resolve();
		await session.close();
	}

	const dir = await mkdtemp(path.join(os.tmpdir(), "pi-oversize-preview-"));
	const names = ["alpha", "beta"];
	for (const name of names) await writeFile(path.join(dir, name), name[0]!.repeat(32768));
	await writeFile(path.join(dir, "small.txt"), "hello\n");
	let hostCap = 1;
	let hostBytes = 8192;
	let useDrafter = false;
	const spiedRead = createReadTool(dir);
	const readSpy = vi.spyOn(spiedRead, "execute");
	const sandbox = new WorkspaceSandboxService();
	const fork = createActorForkPlanSource();
	const host = systemHost(
		dir,
		[createWriteTool(dir), spiedRead],
		{
			actorForkPlanSource: fork,
			executionWorlds: [sandbox.createExecutionWorld(), resourceWorld()],
			complete: async (_model, context) => {
				if (context.tools?.[0]?.name === "speculative_observations") return fauxAssistantMessage("wait");
				const results = context.messages.filter((m) => m.role === "toolResult");
				if (!results.length) return toolUse("write", { path: "dep.txt", content: "chained\n" }, { id: "dw" });
				if (results.length === 1) return toolUse("read", { path: "dep.txt" }, { id: "dr" });
				return fauxAssistantMessage("done");
			},
			getSettings: () => ({
				enabled: true, drafterEnabled: useDrafter, drafterMaxDepth: 1, candidateLimit: 2, maxConcurrentActions: 2,
				resourceCacheMaxEntries: hostCap, resourceCacheMaxBytes: hostBytes,
				patternAware: { enabled: false }, prepareObservedOperations: false,
				selfSpeculation: { enabled: true, forkEnabled: true, forkActionEnabled: true, forkTransport: "sidecar" },
			}),
		},
	);
	try {
		await host.start("turn-1");
		const previewSignal = new AbortController();
		for (const name of names) expect((await (await host.preview("read", { path: name }, previewSignal.signal, `p-${name}`))?.completion)?.status).toBe("completed");
		previewSignal.abort();
		await host.finish();
		expect(host.events.at(-1)?.cache).toMatchObject({ resultEntries: 0, resultBytes: 0 });
		await host.start("turn-2");
		for (const name of names) await host.execute("read", { path: name }, undefined, `a-${name}`);
		expect(host.fallbacks).toHaveLength(2);
		await host.finish();

		hostBytes = 65536;
		readSpy.mockClear();
		host.fallbacks.length = 0;
		fork.startTurn("t-wr");
		await host.start("t-wr");
		fork.publish("t-wr", [{ id: "g", calls: [{ id: "w", index: 0, tool: "write", input: { path: "note.txt", content: "fresh\n" } }] }]);
		await host.ready(["note.txt"]);
		await host.execute("write", { path: "note.txt", content: "fresh\n" }, undefined, "aw");
		expect((await host.execute("read", { path: "note.txt" }, undefined, "ar")).content[0]).toMatchObject({ type: "text", text: expect.stringContaining("fresh") });
		expect(readSpy).not.toHaveBeenCalled();
		expect(host.actors.at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "inputs" } });
		await host.finish();

		hostCap = 512;
		fork.startTurn("t-roots");
		await host.start("t-roots");
		fork.publish("t-roots", [{
			id: "roots",
			calls: [
				{ id: "w1", index: 0, tool: "write", input: { path: "r1.txt", content: "one\n" } },
				{ id: "w2", index: 1, tool: "write", input: { path: "r2.txt", content: "two\n" } },
			],
		}]);
		await host.ready(["r1.txt", "r2.txt"]);
		await host.execute("write", { path: "r1.txt", content: "one\n" }, undefined, "aw1");
		expect((await (await host.preview("read", { path: "small.txt" }, undefined, "ps"))?.completion)?.status).toBe("completed");
		await host.finish();
		expect(host.events.at(-1)?.cache).toMatchObject({ branchEntries: 1, resultEntries: 3 });
		await host.start("t-roots-2");
		await host.execute("read", { path: "small.txt" }, undefined, "as");
		await host.execute("write", { path: "r2.txt", content: "two\n" }, undefined, "aw2");
		expect(host.fallbacks).toEqual([]);
		expect(host.actors.at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } });
		await host.finish();

		useDrafter = true;
		await host.start("t-dep");
		await host.ready(2, "drafter");
		await host.finish();
		expect(host.events.at(-1)?.cache.branchEntries).toBe(2);
		useDrafter = false;
		await host.start("t-dep-2");
		await host.execute("write", { path: "dep.txt", content: "chained\n" }, undefined, "adw");
		await host.execute("read", { path: "dep.txt" }, undefined, "adr");
		expect(host.actors.at(-1)?.settlement.provider).toMatchObject({ kind: "speculative", match: { kind: "exact" } });
		expect(host.actors.at(-1)?.computation?.hiddenComputeMs).toBeGreaterThan(0);

		const injRead = createReadTool(dir);
		const injSpy = vi.spyOn(injRead, "execute");
		const injected = new LocalExecutionSession<ToolSettlement>({
			settings: () => ({ maxConcurrentActions: 2, resourceCacheMaxEntries: 2, resourceCacheMaxBytes: 65536 }),
			effect: (key) => PI_ACTION_SEMANTICS.effect(key),
			key: async (call) => PI_ACTION_SEMANTICS.buildKey(call.tool, call.input, dir, "memory", { fingerprint: "inj" }),
			route: async () => shared, authorize: async () => ({ ok: true }), capture: async () => undefined,
			native: async (call, _key, signal) => ({ result: await injRead.execute(call.id ?? "a", call.input as never, signal), isError: false }),
			fork: async (call, _key, _route, runID, signal) => {
				const out = { result: await injRead.execute(runID, call.input as never, signal), isError: false };
				return {
					backend: "memory", output: out, resources: [path.join(dir, String(call.input.path))], capturedBytes: 6,
					executionMetrics: {}, compatibility: { status: "compatible", executionFingerprint: "inj", backend: "memory" },
					validate: async () => ({ status: "valid", metrics: zeroValidationMetrics() }), commit: async () => out, dispose() {},
				};
			},
		});
		const injHost = systemHost(dir, [injRead], { executionSession: injected }, { resourceCacheMaxEntries: 2 }, "inj");
		try {
			await injHost.start("ti1");
			expect((await (await injHost.preview("read", { path: "small.txt" }, undefined, "p"))?.completion)?.status).toBe("completed");
			await injHost.finish();
			expect(injected.snapshot().resultEntries).toBe(1);
			await injHost.start("ti2");
			await injHost.execute("read", { path: "small.txt" }, undefined, "a");
			expect(injSpy).toHaveBeenCalledTimes(1);
			expect(injHost.fallbacks).toEqual([]);
		} finally {
			await injHost.dispose();
		}
	} finally {
		await host.dispose();
		await sandbox.dispose();
		await rm(dir, { recursive: true, force: true });
	}
});
