import { expect, test, vi } from "vitest";
import { fauxText, fauxAssistantMessage } from "@earendil-works/pi-ai";
import { createReadTool } from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { LocalExecutionSession } from "../src/runtime-engine.ts";
import type { ExecutionCall, ExecutionSession, RunCompletion } from "../src/execution-session.ts";
import type { SpeculativeExecutionRoute, WorldBranch, ToolSettlement } from "../src/execution-world.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { createActorForkPlanSource } from "../src/self-speculation.ts";
import { PATTERN_AWARE_DEFAULTS, PatternAwareStore } from "../src/pattern-aware.ts";
import { cause, zeroValidationMetrics } from "../src/settlement.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { testModel, toolUse } from "./model.ts";

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

test("after reads private writes; only a proved Actor adoption exposes their effects", async () => {
	let disk = "before";
	const images = new WeakMap<WorldBranch<string>, string>();
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
				dispose() {},
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
		expect(await session.execute(read("A"))).toMatchObject({ served: true, output: "after" });
	} finally {
		await session.close();
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
