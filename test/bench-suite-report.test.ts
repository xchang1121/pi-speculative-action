import { EventEmitter } from "node:events";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { nearestRank, type SuiteBenchmarkRun, summarizePairs, summarizeSuite } from "../bench/suite-report.ts";
import type { SpeculativeActionExtensionDependencies } from "../src/extension.ts";
import { emptyWorldReuseMetrics } from "../src/execution-world.ts";
import { emptySpeculativeTraceSummary } from "../src/trace-summary.ts";

describe("ablation suite report", () => {
	it.each([
		["exit", "Benchmark runner exited with 7", false, 7],
		["exit-result", "Benchmark runner exited with 7", true, 7],
		["invalid-result", "Incomplete benchmark result", false, 0],
		["benchmark", "Benchmark failed: cleanup failed", true, 0],
		["existing-output", "EEXIST", false, 0],
	] as const)("preserves completed and failed runs after a runner %s", async (failure, message, complete, exitCode) => {
		const originalArgv = process.argv;
		const files = new Map<string, string>();
		const previous = JSON.stringify({ metadata: { implementationCommit: "old" }, summary: run("failed", 1, {}).summary });
		const failedOutput = path.resolve("offline-results", "repeat-1", "failed.json");
		if (failure === "existing-output") files.set(failedOutput, previous);
		let attempts = 0;
		vi.resetModules();
		vi.doMock("node:fs/promises", () => ({
			mkdir: async () => {},
			readFile: async (file: string) => file.endsWith("suite.json")
				? JSON.stringify({ offline: ["first", "failed", "unstarted"] }) : files.get(file),
			writeFile: async (file: string, data: string, options?: { flag?: string } | string) => {
				if (typeof options === "object" && options.flag === "wx" && files.has(file)) throw new Error("EEXIST");
				files.set(file, data);
			},
		}));
		vi.doMock("node:child_process", () => ({ spawn: (_file: string, args: string[]) => {
			const child = new EventEmitter();
			const instance = args[args.indexOf("--instance") + 1]!;
			const output = args[args.indexOf("--output") + 1]!;
			attempts++;
			queueMicrotask(() => {
				if (instance === "first" || failure !== "exit") files.set(output, instance === "first" || complete ? JSON.stringify({
					metadata: { implementationCommit: `${instance}-commit` }, summary: run(instance, 1, instance === "first" ? {} : {
						patchCandidate: false, benchmarkErrors: { hostDispose: "cleanup failed" },
					}).summary,
				}) : "{}");
				child.emit("exit", instance === "first" ? 0 : exitCode, null);
			});
			return child;
		} }));
		const stdout = vi.spyOn(process.stdout, "write").mockReturnValue(true);
		process.argv = [process.execPath, "suite.ts", "--suite", "offline", "--output-root", "offline-results"];
		try {
			await expect(import("../bench/suite.ts")).rejects.toThrow(message);
			expect(attempts).toBe(failure === "existing-output" ? 1 : 2);
			const report = JSON.parse(files.get(path.resolve("offline-results", "suite-result.json")) ?? "null");
			expect(report).toMatchObject({ runs: 2, patchCandidates: 1, allRunsScreenedIn: false,
				pooled: { runs: complete ? 2 : 1, toolSpeedup: 1 }, byInstance: { failed: complete ? { runs: 1 } : null },
				unmeasuredRuns: complete ? 0 : 1,
				invalidRuns: [{ instance: "failed", repeat: 1, error: expect.stringContaining(message) }],
			});
			expect(report.runOutputs.map((value: { instance: string }) => value.instance)).toEqual(["first", "failed"]);
			expect(report.invalidRuns[0].reasons).toContain(complete ? "benchmark_error" : "unavailable_summary");
			expect(report.implementationCommits).toEqual(["first-commit", ...(complete ? ["failed-commit"] : [])]);
			if (failure === "existing-output") expect(files.get(failedOutput)).toBe(previous);
		} finally {
			process.argv = originalArgv;
			stdout.mockRestore();
			vi.doUnmock("node:fs/promises"); vi.doUnmock("node:child_process"); vi.resetModules();
		}
	});

	it.each([false, true])("retains measured timing and usage through failures with final metrics %s", async reported => {
		const originalArgv = process.argv, files = new Map<string, string>(), phases: string[] = [];
		let now = 0, emit = (_event: unknown): void => {};
		let reportMetrics = (): void => {};
		const actorProcessReuse = { ...emptyWorldReuseMetrics(), requests: 2, hits: 1, wholeCommandRequests: 1 };
		const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
		const fail = (phase: string) => { phases.push(phase); throw new Error(`${phase} failed`); };
		vi.resetModules();
		vi.doMock("node:fs/promises", () => ({
			mkdir: async () => {}, mkdtemp: async () => path.resolve("offline-task"), stat: async () => ({}), readFile: async () => { throw new Error("ENOENT"); },
			writeFile: async (file: string, data: string) => { files.set(file, data); },
		}));
		vi.doMock("node:child_process", () => ({ execFile: (_file: string, args: string[], _options: unknown,
			callback: (error: null, stdout: string, stderr: string) => void) => {
			callback(null, args.includes("--name-only") ? "src/file.ts\n" : "commit", "");
		} }));
		vi.doMock("@earendil-works/pi-ai/compat", () => ({
			getProviders: () => ["offline"], getModels: () => [{ provider: "offline", id: "model" }],
		}));
		vi.doMock("@earendil-works/pi-coding-agent", () => ({
			createAgentSession: async () => ({ session: {
				messages: [{ role: "assistant", usage: { cost: { total: 2 }, totalTokens: 13, input: 10, output: 3, cacheRead: 0, cacheWrite: 0 } }],
				state: {}, subscribe(listener: typeof emit) { emit = listener; }, bindExtensions: async () => {}, prompt: async () => {
					now = 10; emit({ type: "tool_execution_start", toolName: "read", toolCallId: "first" });
					now = 20; emit({ type: "tool_execution_start", toolName: "read", toolCallId: "second" });
					now = 40; emit({ type: "tool_execution_end", toolCallId: "second" });
					now = 60; emit({ type: "tool_execution_end", toolCallId: "first" });
					fail("prompt");
				},
				extensionRunner: { emit: async () => { reportMetrics(); fail("shutdown"); } }, dispose: () => fail("dispose"),
			} }),
			DefaultResourceLoader: class { async reload() {} }, ModelRuntime: { create: async ({ refreshOnCreate }: { refreshOnCreate?: boolean }) => ({
				getAvailableSnapshot: () => refreshOnCreate === false ? [] : [{ provider: "offline", id: "model" }],
			}) },
			SessionManager: { inMemory: () => ({}) }, SettingsManager: { inMemory: () => ({}) },
		}));
		vi.doMock("../src/agent-integration.ts", () => ({ createSpeculativeActionHost: () => ({}) }));
		vi.doMock("../src/extension.ts", () => ({ createSpeculativeActionExtension: (options: SpeculativeActionExtensionDependencies) => {
			reportMetrics = () => { if (reported) options.onMetrics?.({ ...emptySpeculativeTraceSummary(), actorProcessReuse }, {
				worlds: [], primaryIDs: new Set(["linux_process"]), actorProcessReplay: { state: "ready", detail: "installed Actor route" },
			}); };
			return {};
		} }));
		vi.stubGlobal("fetch", async () => ({ ok: true, json: async () => ({ rows: [{ row: {
			instance_id: "offline", repo: "offline/repo", base_commit: "commit", patch: "diff --git a/src/file.ts b/src/file.ts",
			test_patch: "", problem_statement: "offline", language: "TypeScript", source_dataset: "offline",
			FAIL_TO_PASS: [], PASS_TO_PASS: [],
		} }] }) }));
		const stdout = vi.spyOn(process.stdout, "write").mockReturnValue(true);
		process.argv = [process.execPath, "run.ts", "--instance", "offline", "--actor", "offline/model",
			"--drafter", "offline/model", "--drafter-max-depth", "2", "--output", "offline-result.json"];
		try {
			await expect(import("../bench/run.ts")).rejects.toThrow("Benchmark failed:");
			expect(phases).toEqual(["prompt", "shutdown", "dispose"]);
			const { metadata, summary, traces } = JSON.parse(files.get(path.resolve("offline-result.json"))!);
			expect(metadata).toMatchObject({ actor: "offline/model", drafter: "offline/model", drafterMaxDepth: 2, monotonicTimeOrigin: performance.timeOrigin });
			expect(metadata.executionRoutes).toEqual(reported ? { worlds: [], primaryIDs: ["linux_process"],
				actorProcessReplay: { state: "ready", detail: "installed Actor route" } } : null);
			expect(summary.actorProcessReuse).toEqual(reported ? actorProcessReuse : null);
			expect(summary.processReuse).toEqual(emptyWorldReuseMetrics());
			expect(traces.toolWaits).toEqual([{ id: "first", startedAt: performance.timeOrigin + 10, completedAt: performance.timeOrigin + 60 },
				{ id: "second", startedAt: performance.timeOrigin + 20, completedAt: performance.timeOrigin + 40 }]);
			for (const key of ["repoCache", "runRoot", "output", "prepareOnly"]) expect(metadata).not.toHaveProperty(key);
			expect(summary).toMatchObject({ patchCandidate: false, actorCost: 2, actorTokens: 13, toolWaitMs: 50, toolSpeedup: 1,
				changedFiles: ["src/file.ts"], benchmarkErrors: Object.fromEntries(phases.map(phase => [phase, `Error: ${phase} failed`])),
			});
			expect(summary.actualEndToEndMs).toBeGreaterThanOrEqual(summary.agentPromptMs);
			expect(summary.actualEndToEndMs).toBeCloseTo(summary.setupMs + summary.agentPromptMs + summary.teardownMs, 6);
		} finally {
			process.argv = originalArgv; stdout.mockRestore(); clock.mockRestore(); vi.unstubAllGlobals();
			vi.doUnmock("node:fs/promises"); vi.doUnmock("node:child_process");
			vi.doUnmock("@earendil-works/pi-ai/compat"); vi.doUnmock("@earendil-works/pi-coding-agent");
			vi.doUnmock("../src/agent-integration.ts"); vi.doUnmock("../src/extension.ts"); vi.resetModules();
		}
	});

	it("includes repeated and slow failed tasks in timing while exposing every screening failure", () => {
		const report = summarizeSuite([
			run("task-a", 1, {
				actualEndToEndMs: 100,
				toolWaitMs: 100, hiddenLatencyMs: 20,
				actorActions: 10,
				speculativeHits: 2,
			}),
			run("task-b", 1, {
				actualEndToEndMs: 300,
				toolWaitMs: 300, hiddenLatencyMs: 30,
				actorActions: 30,
				speculativeHits: 3,
			}),
			run("task-b", 2, {
				actualEndToEndMs: 1000,
				toolWaitMs: 1000, hiddenLatencyMs: 0,
				actorActions: 0,
				patchCandidate: false,
				timedOut: true,
				patchClean: false,
				changedFiles: [],
				coveredGoldFiles: [],
			}),
		]);

		expect(report).toMatchObject({
			runs: 3,
			patchCandidates: 2,
			allRunsScreenedIn: false,
			statistics: {
				primaryEstimator: "ratio_of_means",
				baseline: "same_run_tool_wait_plus_hidden_latency",
				samplePolicy: "all_measured_runs",
			},
			implementationCommits: ["commit"],
			pooled: {
				runs: 3,
				instanceClusters: 2,
				actualEndToEndMs: 1400,
				actualEndToEndP95Ms: 1000,
				toolWaitMs: 1400, hiddenLatencyMs: 50,
				toolWaitP95Ms: 1000,
				actualEndToEndMeanMs: 1400 / 3,
				toolWaitMeanMs: 1400 / 3,
				toolSpeedup: 1450 / 1400,
				actorActions: 40,
				speculativeHits: 5,
				hitRate: 0.125,
			},
			byInstance: {
				"task-a": { runs: 1, toolSpeedup: 1.2, hitRate: 0.2 },
				"task-b": { runs: 2, toolSpeedup: 1330 / 1300, hitRate: 0.1 },
			},
		});
		expect(nearestRank([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.95)).toBe(10);
		expect(report.invalidRuns).toEqual([
			{
				instance: "task-b",
				repeat: 2,
				output: "task-b-2.json",
				reasons: ["timed_out", "patch_not_clean", "no_changed_files", "no_gold_file_overlap"],
			},
		]);
	});

	it("pairs speculation on and off per instance and repeat, including a slower speculative arm", () => {
		const report = summarizePairs([{ ...run("a", 1, { toolWaitMs: 80 }), arm: "on" }, { ...run("a", 1, { toolWaitMs: 100 }), arm: "off" },
			{ ...run("a", 2, { toolWaitMs: 120 }), arm: "on" }, { ...run("a", 2, { toolWaitMs: 100 }), arm: "off" }, { ...run("b", 1, {}), arm: "on" }]);
		expect(report).toMatchObject({ pairedToolSpeedup: 1, on: { runs: 3 }, off: { runs: 2 } });
		expect(report.pairs).toEqual([{ instance: "a", repeat: 1, onToolWaitMs: 80, offToolWaitMs: 100 }, { instance: "a", repeat: 2, onToolWaitMs: 120, offToolWaitMs: 100 }]);
	});

	it("retains zero-tool tasks without inventing a speedup", () => {
		const report = summarizeSuite([run("empty", 1, { toolWaitMs: 0 })]);
		expect(report).toMatchObject({ unmeasuredRuns: 0, pooled: { toolSpeedup: null, hiddenLatencyMs: 0, toolWaitMs: 0 } });
		expect(report.pooled).not.toHaveProperty("accelerationRatio");
	});

	it.each([-1, NaN, Infinity])("weights unequal repeats and exposes unavailable timing %s", (invalid) => {
		const report = summarizeSuite([
			run("short", 1, { toolWaitMs: 0.5, hiddenLatencyMs: 0.5 }),
			run("long", 1, { toolWaitMs: 200, hiddenLatencyMs: 100 }),
			run("long", 2, { toolWaitMs: 600, hiddenLatencyMs: 300 }),
			run("missing", 1, { toolWaitMs: invalid }),
		]);
		expect(report.pooled?.toolSpeedup).toBeCloseTo(1201 / 800.5, 12);
		expect(report.pooled).not.toHaveProperty("savingsAccelerationRatio");
		expect(report.pooled?.hiddenLatencyMs).toBe(400.5);
		expect(report).toMatchObject({ runs: 4, unmeasuredRuns: 1, pooled: { runs: 3 }, byInstance: { missing: null },
			invalidRuns: [{ instance: "missing", reasons: ["unavailable_timing"] }] });
	});
});

function run(instance: string, repeat: number, overrides: Partial<SuiteBenchmarkRun["summary"]>): SuiteBenchmarkRun {
	return {
		instance,
		repeat,
		output: `${instance}-${repeat}.json`,
		implementationCommit: "commit",
		summary: {
			actualEndToEndMs: 1,
			toolWaitMs: 1,
			hiddenLatencyMs: 0,
			actorActions: 1,
			speculativeHits: 0,
			actorCost: 0,
			drafterCost: 0,
			patchCandidate: true,
			timedOut: false,
			turnLimitReached: false,
			patchClean: true,
			changedFiles: ["src/file.ts"],
			coveredGoldFiles: ["src/file.ts"],
			...overrides,
		},
	};
}
