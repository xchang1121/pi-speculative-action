import { EventEmitter } from "node:events";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
	nearestRank,
	type SuiteBenchmarkRun,
	summarizeSuite,
} from "../bench/suite-report.ts";

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
				pooled: { runs: complete ? 2 : 1, accelerationRatio: 1 }, byInstance: { failed: complete ? { runs: 1 } : null },
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

	it("retains prompt and every disposal failure with measured timing and usage", async () => {
		const originalArgv = process.argv, files = new Map<string, string>(), phases: string[] = [];
		const fail = (phase: string) => { phases.push(phase); throw new Error(`${phase} failed`); };
		vi.resetModules();
		vi.doMock("node:fs/promises", () => ({
			mkdir: async () => {}, mkdtemp: async () => path.resolve("offline-task"), stat: async () => ({}),
			writeFile: async (file: string, data: string) => { files.set(file, data); },
		}));
		vi.doMock("node:child_process", () => ({ execFile: (_file: string, args: string[], _options: unknown,
			callback: (error: null, stdout: string, stderr: string) => void) => {
			callback(null, args.includes("--name-only") ? "src/file.ts\n" : "commit", "");
		} }));
		vi.doMock("@earendil-works/pi-agent-core", () => ({ Agent: class {
			state = { messages: [{ role: "assistant", usage: {
				cost: { total: 2 }, totalTokens: 13, input: 10, output: 3, cacheRead: 0, cacheWrite: 0,
			} }] };
			prompt: () => Promise<never>;
			constructor(input: { streamFn: () => Promise<unknown> }) {
				this.prompt = async () => { await input.streamFn(); return fail("prompt"); };
			}
			subscribe() {}
		} }));
		vi.doMock("@earendil-works/pi-ai/compat", () => ({
			getProviders: () => ["offline"], getModels: () => [{ provider: "offline", id: "model" }], streamSimple: () => ({}),
		}));
		vi.doMock("@earendil-works/pi-coding-agent", () => ({ VERSION: "0.84.1", ...Object.fromEntries(
			["Read", "Write", "Edit", "Ls", "Bash", "Find", "Grep"].flatMap(name => ["", "Definition"].map(suffix =>
				[`create${name}Tool${suffix}`, () => ({ name: name.toLowerCase() })])),
		) }));
		vi.doMock("../src/agent-integration.ts", () => ({ createSpeculativeActionHost: () => ({
			startTurn: async () => {}, finishTurn: () => fail("finishTurn"), dispose: () => fail("hostDispose"),
		}) }));
		vi.doMock("../src/agent-execution-world.ts", () => ({ createResourceSnapshotExecutionWorld: () => ({}) }));
		vi.doMock("../src/workspace-sandbox.ts", () => ({ WorkspaceSandboxService: class {
			createExecutionWorld() { return {}; }
			dispose() { return fail("workspaceDispose"); }
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
			expect(phases).toEqual(["prompt", "finishTurn", "hostDispose", "workspaceDispose"]);
			const { metadata, summary } = JSON.parse(files.get(path.resolve("offline-result.json"))!);
			expect(metadata).toMatchObject({ actor: "offline/model", drafter: "offline/model", drafterMaxDepth: 2 });
			for (const key of ["repoCache", "runRoot", "output", "prepareOnly"]) expect(metadata).not.toHaveProperty(key);
			expect(summary).toMatchObject({ patchCandidate: false, actorCost: 2, actorTokens: 13, accelerationRatio: 1,
				changedFiles: ["src/file.ts"], benchmarkErrors: Object.fromEntries(phases.map(phase => [phase, `Error: ${phase} failed`])),
			});
			expect(summary.actualEndToEndMs).toBeGreaterThanOrEqual(summary.agentPromptMs);
			expect(summary.actualEndToEndMs).toBeCloseTo(summary.setupMs + summary.agentPromptMs + summary.teardownMs, 6);
		} finally {
			process.argv = originalArgv; stdout.mockRestore(); vi.unstubAllGlobals();
			vi.doUnmock("node:fs/promises"); vi.doUnmock("node:child_process");
			vi.doUnmock("@earendil-works/pi-agent-core"); vi.doUnmock("@earendil-works/pi-ai/compat");
			vi.doUnmock("@earendil-works/pi-coding-agent"); vi.doUnmock("../src/agent-integration.ts");
			vi.doUnmock("../src/agent-execution-world.ts"); vi.doUnmock("../src/workspace-sandbox.ts"); vi.resetModules();
		}
	});

	it("includes slow failed tasks in timing while exposing every screening failure", () => {
		const report = summarizeSuite([
			run("task-a", 1, {
				actualEndToEndMs: 100,
				serializedCounterfactualMs: 120,
				actorActions: 10,
				speculativeHits: 2,
			}),
			run("task-b", 1, {
				actualEndToEndMs: 300,
				serializedCounterfactualMs: 330,
				actorActions: 30,
				speculativeHits: 3,
			}),
			run("task-b", 2, {
				actualEndToEndMs: 1000,
				serializedCounterfactualMs: 1000,
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
				baseline: "same_run_serialized_counterfactual",
				samplePolicy: "all_measured_runs",
			},
			implementationCommits: ["commit"],
			pooled: {
				runs: 3,
				actualEndToEndMs: 1400,
				serializedCounterfactualMs: 1450,
				actualEndToEndMeanMs: 1400 / 3,
				serializedCounterfactualMeanMs: 1450 / 3,
				accelerationRatio: 1450 / 1400,
				actorActions: 40,
				speculativeHits: 5,
				hitRate: 0.125,
			},
			byInstance: {
				"task-a": { runs: 1, accelerationRatio: 1.2, hitRate: 0.2 },
				"task-b": { runs: 2, accelerationRatio: 1330 / 1300, hitRate: 0.1 },
			},
		});
		expect(report.invalidRuns).toEqual([
			{
				instance: "task-b",
				repeat: 2,
				output: "task-b-2.json",
				reasons: ["timed_out", "patch_not_clean", "no_changed_files", "no_gold_file_overlap"],
			},
		]);
	});

	it("retains repeats in nearest-rank p95 and total-time ratios", () => {
		const report = summarizeSuite(
			[
				run("task-a", 1, { actualEndToEndMs: 5, serializedCounterfactualMs: 10 }),
				run("task-a", 2, { actualEndToEndMs: 10, serializedCounterfactualMs: 20 }),
				run("task-b", 1, { actualEndToEndMs: 15, serializedCounterfactualMs: 30 }),
				run("task-b", 2, { actualEndToEndMs: 20, serializedCounterfactualMs: 40 }),
			],
		);

		expect(nearestRank([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.95)).toBe(10);
		expect(report.pooled).toMatchObject({
			instanceClusters: 2,
			accelerationRatio: 2,
			actualEndToEndP95Ms: 20,
			serializedCounterfactualP95Ms: 40,
		});
	});

	it.each([0, -1, NaN, Infinity])("weights unequal repeats and exposes unavailable timing %s", (invalid) => {
		const report = summarizeSuite([
			run("short", 1, { serializedCounterfactualMs: 1, actualEndToEndMs: 0.5 }),
			run("long", 1, { serializedCounterfactualMs: 100, actualEndToEndMs: 200 }),
			run("long", 2, { serializedCounterfactualMs: 300, actualEndToEndMs: 600 }),
			run("missing", 1, { actualEndToEndMs: invalid }),
		]);
		expect(report.pooled?.accelerationRatio).toBeCloseTo(401 / 800.5, 12);
		expect(report.pooled?.meanLatencyDifferenceMs).toBeCloseTo(399.5 / 3, 12);
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
			serializedCounterfactualMs: 1,
			hiddenLatencyMs: 0,
			executionAheadMs: 0,
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
