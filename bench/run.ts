import { execFile } from "node:child_process";
import { mkdtemp, mkdir, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";
import { Agent, type AgentMessage, type AgentTool } from "@earendil-works/pi-agent-core";
import { type Api, type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import { getModels, getProviders, streamSimple } from "@earendil-works/pi-ai/compat";
import {
	createBashTool,
	createEditTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	createWriteTool,
} from "@earendil-works/pi-coding-agent";
import { createSpeculativeActionHost, type SpeculativeAgentSettingsInput } from "../src/agent-integration.ts";
import { createResourceSnapshotExecutionWorld } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { ActorStreamPreviewTracker } from "../src/actor-stream-preview.ts";
import { DEFAULTS } from "../src/common.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";

const DATASET_ROWS =
	"https://datasets-server.huggingface.co/rows?dataset=TokenRhythm%2FClaw-SWE-Bench&config=lite&split=test&offset=0&length=100";

interface DatasetRow {
	readonly instance_id: string;
	readonly repo: string;
	readonly base_commit: string;
	readonly patch: string;
	readonly test_patch: string;
	readonly problem_statement: string;
	readonly language: string;
	readonly source_dataset: string;
	readonly FAIL_TO_PASS: readonly string[];
	readonly PASS_TO_PASS: readonly string[];
}

interface PreparedTask {
	readonly row: DatasetRow;
	readonly runDirectory: string;
	readonly workspace: string;
}

interface ToolCounters {
	readonly executions: Record<string, number>;
	readonly serviceMs: Record<string, number>;
}

interface BenchmarkOptions {
	readonly instance: string;
	readonly label: string;
	readonly actor: Model<Api>;
	readonly actorMaxTokens: number;
	readonly actorTemperature: number;
	readonly drafter: Model<Api>;
	readonly drafterMaxDepth: number;
	readonly candidateLimit: number;
	readonly drafterMaxTokens?: number;
	readonly drafterDeterministicCandidates: number;
	readonly drafterTemperatureMin: number;
	readonly drafterTemperatureMax: number;
	readonly maxConcurrentActions: number;
	readonly maxTurns: number;
	readonly timeoutMs: number;
	readonly repoCache: string;
	readonly runRoot: string;
	readonly output?: string;
	readonly patternState?: string;
	readonly drafterEnabled: boolean;
	readonly speculationEnabled: boolean;
	readonly patternAware: boolean;
	readonly prepareOnly: boolean;
}

interface CommandResult {
	readonly stdout: string;
	readonly stderr: string;
}

const { values } = parseArgs({
	options: {
		instance: { type: "string" },
		label: { type: "string", default: "baseline" },
		actor: { type: "string", default: "deepseek/deepseek-v4-pro" },
		"actor-max-tokens": { type: "string", default: "8192" },
		"actor-temperature": { type: "string", default: "0" },
		drafter: { type: "string", default: "deepseek/deepseek-v4-flash" },
		"drafter-max-depth": { type: "string", default: String(DEFAULTS.drafterMaxDepth) },
		"candidate-limit": { type: "string", default: String(DEFAULTS.candidateLimit) },
		"drafter-max-tokens": { type: "string" },
		"drafter-deterministic-candidates": {
			type: "string",
			default: String(DEFAULTS.drafterDeterministicCandidates),
		},
		"drafter-temperature-min": { type: "string", default: String(DEFAULTS.drafterTemperatureMin) },
		"drafter-temperature-max": { type: "string", default: String(DEFAULTS.drafterTemperatureMax) },
		"max-concurrent-actions": { type: "string", default: String(DEFAULTS.maxConcurrentActions) },
		"max-turns": { type: "string", default: "128" },
		"timeout-ms": { type: "string", default: "900000" },
		"repo-cache": { type: "string" },
		"run-root": { type: "string" },
		output: { type: "string" },
		"pattern-state": { type: "string" },
		"drafter-disabled": { type: "boolean", default: false },
		"speculation-disabled": { type: "boolean", default: false },
		"pattern-aware": { type: "boolean", default: false },
		"prepare-only": { type: "boolean", default: false },
	},
	strict: true,
});

const instance = required(values.instance, "--instance");
const repoCache = path.resolve(values["repo-cache"] ?? path.join(os.tmpdir(), "pi-speculative-ablation-cache"));
const runRoot = path.resolve(values["run-root"] ?? path.join(os.tmpdir(), "pi-speculative-ablation-runs"));
const options: BenchmarkOptions = {
	instance,
	label: values.label ?? "baseline",
	actor: model(values.actor ?? "deepseek/deepseek-v4-pro"),
	actorMaxTokens: positiveInteger(values["actor-max-tokens"], "--actor-max-tokens"),
	actorTemperature: nonNegativeNumber(values["actor-temperature"], "--actor-temperature"),
	drafter: model(values.drafter ?? "deepseek/deepseek-v4-flash"),
	drafterMaxDepth: nonNegativeInteger(values["drafter-max-depth"], "--drafter-max-depth"),
	candidateLimit: positiveInteger(values["candidate-limit"], "--candidate-limit"),
	...(values["drafter-max-tokens"] !== undefined
		? { drafterMaxTokens: positiveInteger(values["drafter-max-tokens"], "--drafter-max-tokens") }
		: {}),
	drafterDeterministicCandidates: nonNegativeInteger(
		values["drafter-deterministic-candidates"],
		"--drafter-deterministic-candidates",
	),
	drafterTemperatureMin: nonNegativeNumber(values["drafter-temperature-min"], "--drafter-temperature-min"),
	drafterTemperatureMax: nonNegativeNumber(values["drafter-temperature-max"], "--drafter-temperature-max"),
	maxConcurrentActions: positiveInteger(values["max-concurrent-actions"], "--max-concurrent-actions"),
	maxTurns: positiveInteger(values["max-turns"], "--max-turns"),
	timeoutMs: positiveInteger(values["timeout-ms"], "--timeout-ms"),
	repoCache,
	runRoot,
	...(values.output ? { output: path.resolve(values.output) } : {}),
	...(values["pattern-state"] ? { patternState: path.resolve(values["pattern-state"]) } : {}),
	drafterEnabled: !(values["drafter-disabled"] ?? false),
	speculationEnabled: !values["speculation-disabled"],
	patternAware: values["pattern-aware"] ?? false,
	prepareOnly: values["prepare-only"] ?? false,
};
if (options.drafterTemperatureMin > options.drafterTemperatureMax) {
	throw new Error("--drafter-temperature-min must not exceed --drafter-temperature-max");
}

const prepared = await prepareTask(options);
if (options.prepareOnly) {
	process.stdout.write(
		`${JSON.stringify({ instance: prepared.row.instance_id, repo: prepared.row.repo, workspace: prepared.workspace }, null, 2)}\n`,
	);
} else {
	if (!process.env.DEEPSEEK_API_KEY && (options.actor.provider === "deepseek" || options.drafter.provider === "deepseek")) {
		throw new Error("DEEPSEEK_API_KEY is required for DeepSeek benchmark models");
	}
	const result = await runTask(prepared, options);
	const output = options.output ?? path.join(prepared.runDirectory, "result.json");
	await mkdir(path.dirname(output), { recursive: true });
	await writeFile(output, `${JSON.stringify(result, null, 2)}\n`, "utf8");
	process.stdout.write(`${JSON.stringify({ output, ...result.summary }, null, 2)}\n`);
}

async function prepareTask(input: BenchmarkOptions): Promise<PreparedTask> {
	const row = await datasetRow(input.instance);
	await Promise.all([mkdir(input.repoCache, { recursive: true }), mkdir(input.runRoot, { recursive: true })]);
	const cache = path.join(input.repoCache, `${safeName(row.repo)}.git`);
	if (!(await exists(cache))) {
		await command("git", ["init", "--bare", cache]);
		await command("git", ["-C", cache, "remote", "add", "origin", `https://github.com/${row.repo}.git`]);
		await command("git", ["-C", cache, "config", "core.longpaths", "true"]);
	}
	const benchmarkRef = `refs/bench/${safeName(row.instance_id)}`;
	await command("git", [
		"-C",
		cache,
		"fetch",
		"--force",
		"--depth=1",
		"origin",
		`+${row.base_commit}:${benchmarkRef}`,
	]);
	const runDirectory = await mkdtemp(path.join(input.runRoot, `${safeName(row.instance_id)}-`));
	const workspace = path.join(runDirectory, "workspace");
	await command("git", ["clone", "--no-checkout", cache, workspace]);
	await command("git", ["-C", workspace, "config", "core.longpaths", "true"]);
	await command("git", ["-C", workspace, "fetch", "--depth=1", "origin", benchmarkRef]);
	await command("git", ["-C", workspace, "checkout", "--detach", row.base_commit]);
	return { row, runDirectory, workspace };
}

async function runTask(task: PreparedTask, input: BenchmarkOptions) {
	const implementationCommit = (await command("git", ["rev-parse", "HEAD"], process.cwd())).stdout.trim();
	const taskStartedAt = performance.now();
	const events: SpeculativeActionEvent<string>[] = [];
	const counters: ToolCounters = { executions: {}, serviceMs: {} };
	const shellEnvironment = benchmarkShellEnvironment();
	const tools = [
		createReadTool(task.workspace),
		createGrepTool(task.workspace),
		createFindTool(task.workspace),
		createLsTool(task.workspace),
		createBashTool(task.workspace, {
			exposeSessionEnvironment: false,
			spawnHook: (context) => ({ ...context, env: shellEnvironment }),
		}),
		createEditTool(task.workspace),
		createWriteTool(task.workspace),
	];
	const workspaceSandbox = new WorkspaceSandboxService(), sandbox = workspaceSandbox.createExecutionWorld();
	const resolveInvocation = (tool: string, args: unknown) =>
		resolvePiToolInvocation(tool, args, { cwd: task.workspace, environment: shellEnvironment });
	const drafterStopReasons: Record<string, number> = {};
	const drafterToolCalls: Record<string, number> = {};
	const drafterNoToolStopReasons: Record<string, number> = {};
	const drafterPredictionTrace: Array<{
		readonly requestSessionID?: string;
		readonly stopReason: string;
		readonly usage: AssistantMessage["usage"];
		readonly calls: readonly { readonly tool: string; readonly input: unknown }[];
	}> = [];
	const settings: SpeculativeAgentSettingsInput = {
		enabled: input.speculationEnabled,
		drafterEnabled: input.drafterEnabled,
		drafterMaxDepth: input.drafterMaxDepth,
		drafterMaxTokens: input.drafterMaxTokens,
		drafterDeterministicCandidates: input.drafterDeterministicCandidates,
		drafterTemperatureMin: input.drafterTemperatureMin,
		drafterTemperatureMax: input.drafterTemperatureMax,
		candidateLimit: input.candidateLimit,
		maxConcurrentActions: input.maxConcurrentActions,
		predictionTimeoutMs: input.timeoutMs,
		patternAware: { enabled: input.patternAware },
		tools: ["read", "grep", "find", "ls", "bash", "edit", "write"],
	};
	const sessionID = `${input.label}:${task.row.instance_id}:${Date.now()}`;
	const host = createSpeculativeActionHost(sessionID, {
		cwd: task.workspace,
		getSettings: () => settings,
		draftModel: input.drafter,
		getDraftOptions: ({ signal }) => ({ signal }),
		complete: async (draftModel, context, streamOptions) => {
			const message = await streamSimple(draftModel, context, streamOptions).result();
			increment(drafterStopReasons, message.stopReason);
			const calls = message.content.filter((item) => item.type === "toolCall");
			drafterPredictionTrace.push({
				...(streamOptions?.sessionId ? { requestSessionID: streamOptions.sessionId } : {}),
				stopReason: message.stopReason,
				usage: message.usage,
				calls: calls.map((call) => ({ tool: call.name, input: call.arguments })),
			});
			for (const call of calls) increment(drafterToolCalls, call.name);
			if (!calls.length) increment(drafterNoToolStopReasons, message.stopReason);
			return message;
		},
		preflight: () => true,
		resolveInvocation,
		executionWorlds: [sandbox, createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, {
			tools: PI_OPERATION_TOOLS.resources, maxBytes: () => DEFAULTS.resourceCacheMaxBytes,
		})],
		patternStateDirectory: input.patternState ?? path.join(task.runDirectory, "patterns"),
		...(input.patternState
			? { patternWorkspaceIdentity: path.join(input.repoCache, "pattern-workspaces", safeName(task.row.repo)) }
			: {}),
		onEvent: (event) => {
			events.push(event);
		},
	});
	let currentTurnID: string | undefined;
	let lastTurnID: string | undefined;
	let turnSequence = 0;
	const actorStream = new ActorStreamPreviewTracker();
	const toolIntentMs: number[] = [];
	const actorActionsByTool: Record<string, number> = {};
	const actorTools = tools.map(
		(base): AgentTool => ({
			...base,
			execute: async (callID, args, signal, onUpdate) => {
				const turnID = currentTurnID;
				if (!turnID) throw new Error("Actor tool executed outside an active turn");
				const intentStartedAt = performance.now();
				increment(actorActionsByTool, base.name);
				try {
					return await host.execute(
						{ turnID, id: callID, tool: base.name, args, tools }, signal,
						async (operation) => {
							const startedAt = performance.now();
							increment(counters.executions, base.name);
							try {
								return await base.execute(callID, operation.input as never, operation.signal, onUpdate as never);
							} finally {
								counters.serviceMs[base.name] = (counters.serviceMs[base.name] ?? 0) + performance.now() - startedAt;
							}
						},
					);
				} finally {
					toolIntentMs.push(performance.now() - intentStartedAt);
				}
			},
		}),
	);
	const agent = new Agent({
		streamFn: async (actorModel, context, streamOptions) => {
			actorStream.clear();
			currentTurnID = `turn-${++turnSequence}`;
			lastTurnID = currentTurnID;
			await host.startTurn(
				{ turnID: currentTurnID, actorModel, context: { ...context, tools }, actorOptions: streamOptions, tools },
				streamOptions?.signal,
			);
			return streamSimple(actorModel, context, {
				...streamOptions,
				temperature: input.actorTemperature,
				maxTokens: input.actorMaxTokens,
			});
		},
		sessionId: sessionID,
		shouldStopAfterTurn: () => turnSequence >= input.maxTurns,
		initialState: {
			model: input.actor,
			thinkingLevel: "high",
			systemPrompt:
				"You are a coding agent working directly in the current repository. Inspect the relevant implementation and tests, reproduce the reported issue when practical, implement the smallest complete fix, and run focused validation. Use tools instead of guessing. Do not merely describe a patch: edit the workspace.",
			tools: actorTools,
		},
	});
	const prompt: AgentMessage = {
		role: "user",
		content: `${task.row.problem_statement}\n\nWork in the checked-out repository and finish the implementation. Do not use network access to look up the answer.`,
		timestamp: Date.now(),
	};
	agent.subscribe(async (event, signal) => {
		if (event.type === "message_update") {
			for (const preview of actorStream.observe(event.assistantMessageEvent)) {
				if (!currentTurnID) continue;
				if (preview.type === "tool") {
					void host.previewActorTool({ turnID: currentTurnID, tool: preview.tool }, signal).catch(() => {});
				} else {
					void host.previewActorCall(
						{
							turnID: currentTurnID,
							id: preview.call.id,
							tool: preview.call.name,
							args: preview.call.arguments,
							tools,
						},
						signal,
					).catch(() => {});
				}
			}
		}
		if (event.type === "turn_end" && currentTurnID) {
			const turnID = currentTurnID;
			currentTurnID = undefined;
			await host.finishTurn(turnID, false);
		}
	});

	const agentStartedAt = performance.now();
	let agentCompletedAt: number;
	let taskCompletedAt: number;
	let timedOut = false;
	const timeout = setTimeout(() => {
		timedOut = true;
		agent.abort();
	}, input.timeoutMs);
	try {
		await agent.prompt(prompt);
	} finally {
		agentCompletedAt = performance.now();
		clearTimeout(timeout);
		try {
			if (lastTurnID) await host.finishTurn(lastTurnID, true);
		} finally {
			try { await host.dispose(); } finally { await workspaceSandbox.dispose(); }
		}
		taskCompletedAt = performance.now();
	}
	const summary = summarizeSpeculativeTrace(events);
	const sourceRequestKinds: Record<string, number> = {};
	const sourceRequestsBySource: Record<string, number> = {};
	const predictionsBySource: Record<
		string,
		{ settled: number; observed: number; matched: number; adopted: number }
	> = {};
	const candidateStartsBySource: Record<string, number> = {};
	const candidateStartsByTool: Record<string, number> = {};
	const candidateStartsByDepth: Record<string, number> = {};
	const speculativeHitsByDepth: Record<string, number> = {};
	const speculativeHitsByTool: Record<string, number> = {};
	const actorFallbacksByTool: Record<string, number> = input.speculationEnabled ? {} : { ...actorActionsByTool };
	const actorPreviewsByTool: Record<string, number> = {};
	const speculativeHitsByRelation: Record<string, number> = {};
	const speculativeHitProvidersBySource: Record<string, number> = {};
	const actorActionMatchesByPredictionSource: Record<string, number> = {};
	const candidateStartTrace: Array<{
		readonly turnID: string;
		readonly source: string;
		readonly tool: string;
		readonly depth: number;
		readonly action: string;
	}> = [];
	const actorActionTrace: Array<{
		readonly turnID: string;
		readonly sequence: number;
		readonly tool: string;
		readonly action: string;
		readonly provider: "actor" | "speculative";
		readonly matchedPredictionSources: readonly string[];
		readonly candidateSource?: string;
		readonly predictedAction?: string;
	}> = [];
	for (const event of events) {
		if (event.type === "source_request") {
			increment(sourceRequestKinds, event.request.request.kind);
			increment(sourceRequestsBySource, event.request.request.source);
			continue;
		}
		if (event.type === "prediction") {
			const source = event.settlement.prediction.source;
			const counters = (predictionsBySource[source] ??= { settled: 0, observed: 0, matched: 0, adopted: 0 });
			counters.settled++;
			if (event.settlement.observation === "unobserved") continue;
			counters.observed++;
			if (!event.settlement.match.matched) continue;
			counters.matched++;
			if (event.settlement.match.adoption.status === "adopted") counters.adopted++;
			continue;
		}
		if (event.type === "candidate") {
			if (event.state.status !== "running") continue;
			increment(candidateStartsBySource, event.candidate.source);
			increment(candidateStartsByTool, event.candidate.tool);
			increment(candidateStartsByDepth, String(event.candidate.depth));
			candidateStartTrace.push({
				turnID: event.turnID,
				source: event.candidate.source,
				tool: event.candidate.tool,
				depth: event.candidate.depth,
				action: event.candidate.predictedAction,
			});
			continue;
		}
		if (event.type !== "actor_action") continue;
		const { provider, tool } = event.settlement;
		const matchedPredictionSources = [
			...new Set(event.settlement.matchedPredictions.map((prediction) => prediction.source)),
		];
		for (const source of matchedPredictionSources) {
			increment(actorActionMatchesByPredictionSource, source);
		}
		actorActionTrace.push({
			turnID: event.turnID,
			sequence: event.settlement.actorAction.sequence,
			tool,
			action: event.actualAction,
			provider: provider.kind,
			matchedPredictionSources,
			...(event.candidate
				? { candidateSource: event.candidate.source, predictedAction: event.candidate.predictedAction }
				: {}),
		});
		if (provider.kind === "actor") {
			increment(provider.origin === "preview" ? actorPreviewsByTool : actorFallbacksByTool, tool);
			continue;
		}
		increment(speculativeHitProvidersBySource, event.candidate?.source ?? "cache");
		increment(speculativeHitsByTool, tool);
		increment(speculativeHitsByDepth, String(event.candidate?.depth ?? 0));
		const relation = provider.match.kind === "exact" ? "exact" : `projected:${provider.match.projector}`;
		increment(speculativeHitsByRelation, relation);
	}
	const actualEndToEndMs = taskCompletedAt - taskStartedAt;
	const hiddenLatencyMs = summary.hiddenLatencyMs;
	const serializedCounterfactualMs = actualEndToEndMs + hiddenLatencyMs;
	const nonToolMs = Math.max(0, serializedCounterfactualMs - summary.toolExecutionMs);
	const actorUsage = summarizeUsage(agent.state.messages.filter((message) => message.role === "assistant"));
	const drafterUsage = summarizeUsage(drafterPredictionTrace);
	const changedFiles = lines((await command("git", ["-C", task.workspace, "diff", "--name-only"])).stdout);
	const goldFiles = patchFiles(task.row.patch);
	const testPatchFiles = patchFiles(task.row.test_patch);
	let patchClean = true;
	try {
		await command("git", ["-C", task.workspace, "diff", "--check"]);
	} catch {
		patchClean = false;
	}
	const coveredGoldFiles = goldFiles.filter((file) => changedFiles.includes(file));
	const turnLimitReached = turnSequence >= input.maxTurns;
	return {
		metadata: {
			label: input.label,
			implementationCommit,
			instance: task.row.instance_id,
			repo: task.row.repo,
			baseCommit: task.row.base_commit,
			language: task.row.language,
			sourceDataset: task.row.source_dataset,
			actor: `${input.actor.provider}/${input.actor.id}`,
			actorMaxTokens: input.actorMaxTokens,
			actorTemperature: input.actorTemperature,
			drafter: `${input.drafter.provider}/${input.drafter.id}`,
			candidateLimit: input.candidateLimit,
			drafterMaxTokens: input.drafterMaxTokens,
			drafterDeterministicCandidates: input.drafterDeterministicCandidates,
			drafterTemperatureMin: input.drafterTemperatureMin,
			drafterTemperatureMax: input.drafterTemperatureMax,
			drafterEnabled: input.drafterEnabled,
			maxConcurrentActions: input.maxConcurrentActions,
			maxTurns: input.maxTurns,
			timeoutMs: input.timeoutMs,
			patternAware: input.patternAware,
			speculationEnabled: input.speculationEnabled,
			timingScope: "setup, Agent prompt, terminal settlement, host and workspace disposal",
			patternState: input.patternState ?? "isolated-per-run",
			executionBoundary: {
				priority: ["runtime_sandbox", "local_fallback", "actor_fallback"],
				local: {
					observation: "resource_version",
					workspaceMutation: "git_worktree",
					unbounded: "unavailable",
				},
			},
			workspace: task.workspace,
		},
		summary: {
			...summary,
			actualEndToEndMs,
			setupMs: agentStartedAt - taskStartedAt,
			agentPromptMs: agentCompletedAt - agentStartedAt,
			teardownMs: taskCompletedAt - agentCompletedAt,
			serializedCounterfactualMs,
			nonToolMs,
			authoritativeToolMs: summary.toolExecutionMs,
			accelerationRatio: actualEndToEndMs > 0 ? serializedCounterfactualMs / actualEndToEndMs : 1,
			actorActions: toolIntentMs.length,
			actorActionsByTool,
			speculativeHitsByDepth,
			speculativeHitsByTool,
			speculativeHitsByRelation,
			actorFallbacks: input.speculationEnabled ? summary.actorFallbacks : toolIntentMs.length,
			actorPreviewsByTool,
			actorFallbacksByTool,
			hitRate: toolIntentMs.length ? summary.speculativeHits / toolIntentMs.length : 0,
			sourceRequestKinds,
			sourceRequestsBySource,
			predictionsBySource,
			candidateStartsBySource,
			candidateStartsByTool,
			candidateStartsByDepth,
			speculativeHitProvidersBySource,
			actorActionMatchesByPredictionSource,
			actorCost: actorUsage.cost,
			drafterCost: drafterUsage.cost,
			actorTokens: actorUsage.tokens,
			drafterTokens: drafterUsage.tokens,
			actorInputTokens: actorUsage.inputTokens,
			actorOutputTokens: actorUsage.outputTokens,
			actorCacheReadTokens: actorUsage.cacheReadTokens,
			actorCacheWriteTokens: actorUsage.cacheWriteTokens,
			drafterInputTokens: drafterUsage.inputTokens,
			drafterOutputTokens: drafterUsage.outputTokens,
			drafterCacheReadTokens: drafterUsage.cacheReadTokens,
			drafterCacheWriteTokens: drafterUsage.cacheWriteTokens,
			drafterStopReasons,
			drafterToolCalls,
			drafterNoToolStopReasons,
			turns: turnSequence,
			turnLimitReached,
			timedOut,
			agentError: agent.state.errorMessage,
			toolIntentMs,
			rawActorToolExecutions: counters.executions,
			rawActorToolServiceMs: counters.serviceMs,
			changedFiles,
			goldFiles,
			testPatchFiles,
			failToPassTests: task.row.FAIL_TO_PASS,
			passToPassTests: task.row.PASS_TO_PASS,
			coveredGoldFiles,
			goldFileRecall: goldFiles.length ? coveredGoldFiles.length / goldFiles.length : 0,
			patchClean,
			patchCandidate:
				!timedOut &&
				!turnLimitReached &&
				!agent.state.errorMessage &&
				patchClean &&
				changedFiles.length > 0 &&
				coveredGoldFiles.length > 0,
		},
		traces: {
			drafterPredictions: drafterPredictionTrace,
			candidateStarts: candidateStartTrace,
			actorActions: actorActionTrace,
		},
	};
}

function summarizeUsage(messages: readonly { readonly usage: AssistantMessage["usage"] }[]) {
	return messages.reduce((total, { usage }) => ({ cost: total.cost + usage.cost.total,
		tokens: total.tokens + usage.totalTokens, inputTokens: total.inputTokens + usage.input,
		outputTokens: total.outputTokens + usage.output, cacheReadTokens: total.cacheReadTokens + usage.cacheRead,
		cacheWriteTokens: total.cacheWriteTokens + usage.cacheWrite,
	}), { cost: 0, tokens: 0, inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 });
}

function benchmarkShellEnvironment(): Record<string, string> {
	return Object.fromEntries(
		Object.entries(process.env).filter(
			(entry): entry is [string, string] =>
				entry[1] !== undefined && !/(?:API_KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)/i.test(entry[0]),
		),
	);
}

async function datasetRow(instanceID: string): Promise<DatasetRow> {
	const response = await fetch(DATASET_ROWS);
	if (!response.ok) throw new Error(`Dataset request failed with HTTP ${response.status}`);
	const value: unknown = await response.json();
	if (!value || typeof value !== "object" || !("rows" in value) || !Array.isArray(value.rows)) {
		throw new Error("Dataset response has no rows");
	}
	for (const item of value.rows) {
		if (!item || typeof item !== "object" || !("row" in item)) continue;
		const row = validDatasetRow(item.row);
		if (row?.instance_id === instanceID) return row;
	}
	throw new Error(`Claw-SWE-Bench Lite instance not found: ${instanceID}`);
}

function validDatasetRow(value: unknown): DatasetRow | undefined {
	if (!value || typeof value !== "object") return undefined;
	const row = value as Partial<Record<keyof DatasetRow, unknown>>;
	for (const key of [
		"instance_id",
		"repo",
		"base_commit",
		"patch",
		"test_patch",
		"problem_statement",
		"language",
		"source_dataset",
	] as const) {
		if (typeof row[key] !== "string") return undefined;
	}
	for (const key of ["FAIL_TO_PASS", "PASS_TO_PASS"] as const) {
		if (!Array.isArray(row[key]) || !row[key].every((item) => typeof item === "string")) return undefined;
	}
	return row as DatasetRow;
}

function model(value: string): Model<Api> {
	const separator = value.indexOf("/");
	if (separator <= 0 || separator === value.length - 1) throw new Error(`Invalid model ${value}; expected provider/id`);
	const providerName = value.slice(0, separator);
	const provider = getProviders().find((candidate) => candidate === providerName);
	if (!provider) throw new Error(`Unknown model provider ${providerName}`);
	const modelID = value.slice(separator + 1);
	const resolved = getModels(provider).find((candidate) => candidate.id === modelID);
	if (!resolved) throw new Error(`Unknown model ${value}`);
	return resolved;
}

function patchFiles(patch: string): string[] {
	return [
		...new Set(
			patch
				.split(/\r?\n/)
				.flatMap((line) => (line.startsWith("diff --git a/") ? [line.slice("diff --git a/".length).split(" b/")[0]!] : [])),
		),
	];
}

function lines(value: string): string[] {
	return value
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);
}

function increment(counts: Record<string, number>, key: string): void {
	counts[key] = (counts[key] ?? 0) + 1;
}

function positiveInteger(value: string | undefined, option: string): number {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${option} must be a positive integer`);
	return parsed;
}

function nonNegativeInteger(value: string | undefined, option: string): number {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed < 0) throw new Error(`${option} must be a non-negative integer`);
	return parsed;
}

function nonNegativeNumber(value: string | undefined, option: string): number {
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`${option} must be a non-negative number`);
	return parsed;
}

function required(value: string | undefined, option: string): string {
	if (!value?.trim()) throw new Error(`${option} is required`);
	return value.trim();
}

function safeName(value: string): string {
	return value.replaceAll(/[^A-Za-z0-9._-]/g, "_");
}

async function exists(value: string): Promise<boolean> {
	try {
		await stat(value);
		return true;
	} catch {
		return false;
	}
}

function command(file: string, args: readonly string[], cwd?: string): Promise<CommandResult> {
	return new Promise((resolve, reject) => {
		execFile(file, args, { cwd, env: benchmarkShellEnvironment(), maxBuffer: 32 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) {
				reject(new Error(`${file} ${args.join(" ")} failed: ${stderr || error.message}`));
				return;
			}
			resolve({ stdout, stderr });
		});
	});
}
