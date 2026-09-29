import { safeName } from "./suite-report.ts";
import { benchmarkTraceReport } from "./trace-report.ts";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";
import { type Api, type AssistantMessage, type CredentialStore, type Model } from "@earendil-works/pi-ai";
import { getModels, getProviders } from "@earendil-works/pi-ai/compat";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { DEFAULTS } from "../src/common.ts";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import type { SpeculativeActionEvent } from "../src/runtime.ts";
import { summarizeSpeculativeTrace } from "../src/trace-summary.ts";

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

type PreparedTask = Readonly<Awaited<ReturnType<typeof prepareTask>>>;

type BenchmarkOptions = Readonly<typeof options>;

const SECRET_VARIABLE = /(?:API_KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)/i;

interface CommandResult { readonly stdout: string; readonly stderr: string; }

const { values } = parseArgs({
	options: {
		instance: { type: "string" },
		label: { type: "string", default: "baseline" },
		actor: { type: "string", default: "deepseek/deepseek-v4-pro" },
		drafter: { type: "string", default: "deepseek/deepseek-v4-flash" },
		// Every model request goes through this endpoint instead (a pi-llm-tape recorder or replayer).
		"model-base-url": { type: "string" },
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
		"self-speculation": { type: "boolean", default: false },
		"drafter-pattern-hints": { type: "boolean", default: false },
		"prepare-only": { type: "boolean", default: false },
		"keep-session": { type: "boolean", default: false },
	},
	strict: true,
});

const instance = required(values.instance, "--instance");
const repoCache = path.resolve(values["repo-cache"] ?? path.join(os.tmpdir(), "pi-speculative-ablation-cache"));
const runRoot = path.resolve(values["run-root"] ?? path.join(os.tmpdir(), "pi-speculative-ablation-runs"));
const options = {
	instance,
	label: values.label ?? "baseline",
	actor: model(values.actor ?? "deepseek/deepseek-v4-pro"),
	drafter: model(values.drafter ?? "deepseek/deepseek-v4-flash"),
	drafterMaxDepth: nonNegativeInteger(values["drafter-max-depth"], "--drafter-max-depth"),
	candidateLimit: positiveInteger(values["candidate-limit"], "--candidate-limit"),
	...(values["drafter-max-tokens"] !== undefined ? { drafterMaxTokens: positiveInteger(values["drafter-max-tokens"], "--drafter-max-tokens") } : {}),
	drafterDeterministicCandidates: nonNegativeInteger(values["drafter-deterministic-candidates"], "--drafter-deterministic-candidates"),
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
	selfSpeculation: values["self-speculation"] ?? false,
	drafterPatternHints: values["drafter-pattern-hints"] ?? false,
	prepareOnly: values["prepare-only"] ?? false,
	keepSession: values["keep-session"] ?? false,
} as const;
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
	if (Object.keys(result.summary.benchmarkErrors).length) {
		throw new Error(`Benchmark failed: ${JSON.stringify(result.summary.benchmarkErrors)}`);
	}
}

async function prepareTask(input: BenchmarkOptions) {
	await Promise.all([mkdir(input.repoCache, { recursive: true }), mkdir(input.runRoot, { recursive: true })]);
	const row = await datasetRow(input.instance, path.join(input.repoCache, "claw-swe-bench-lite.json"));
	const cache = path.join(input.repoCache, `${safeName(row.repo)}.git`);
	if (!(await exists(cache))) {
		await command("git", ["init", "--bare", cache]);
		await command("git", ["-C", cache, "remote", "add", "origin", `https://github.com/${row.repo}.git`]);
		await command("git", ["-C", cache, "config", "core.longpaths", "true"]);
	}
	// A cached base commit needs no network; the checkout below still pins the exact commit.
	const benchmarkRef = `refs/bench/${safeName(row.instance_id)}`;
	await command("git", ["-C", cache, "rev-parse", "--verify", "--quiet", `${benchmarkRef}^{commit}`]).catch(() =>
		command("git", ["-C", cache, "fetch", "--force", "--depth=1", "origin", `+${row.base_commit}:${benchmarkRef}`]));
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
	const drafterPredictionTrace: Array<{
		readonly requestSessionID?: string;
		readonly stopReason: string;
		readonly usage: AssistantMessage["usage"];
		readonly calls: readonly { readonly tool: string; readonly input: unknown }[];
	}> = [];
	// The installed extension owns every route (Linux process reuse, sandbox, snapshots); a shared state directory
	// persists its pattern and command history across runs, as a user's agent directory does.
	const agentDir = input.patternState ?? path.join(task.runDirectory, "agent");
	await mkdir(agentDir, { recursive: true });
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify({
		enabled: input.speculationEnabled, drafterEnabled: input.drafterEnabled, drafterMaxDepth: input.drafterMaxDepth,
		...(input.drafterMaxTokens !== undefined ? { drafterMaxTokens: input.drafterMaxTokens } : {}),
		drafterDeterministicCandidates: input.drafterDeterministicCandidates, drafterTemperatureMin: input.drafterTemperatureMin, drafterPatternHints: input.drafterPatternHints,
		drafterTemperatureMax: input.drafterTemperatureMax, candidateLimit: input.candidateLimit, maxConcurrentActions: input.maxConcurrentActions,
		predictionTimeoutMs: input.timeoutMs, patternAware: { enabled: input.patternAware },
		...(input.selfSpeculation ? { selfSpeculation: { enabled: true, forkTransport: "drafter" } } : {}), draftModel: `${input.drafter.provider}/${input.drafter.id}`,
	}));
	process.env.PI_CODING_AGENT_DIR = agentDir;
	// The Actor's shell inherits this environment: credentials move into Pi's in-memory store first.
	const key = process.env.DEEPSEEK_API_KEY, credentials: CredentialStore = {
		read: async (provider) => provider === "deepseek" && key ? { type: "api_key", key } : undefined,
		list: async () => key ? [{ providerId: "deepseek", type: "api_key" }] : [],
		modify: (provider) => credentials.read(provider), delete: async () => {},
	};
	for (const name of Object.keys(process.env)) if (SECRET_VARIABLE.test(name)) delete process.env[name];
	const extension = createSpeculativeActionExtension({
		createHost: (id, options) => createSpeculativeActionHost(id, {
			...options,
			complete: async (draftModel, context, streamOptions) => {
				const message = await options.complete(draftModel, context, streamOptions);
				drafterPredictionTrace.push({
					...(streamOptions?.sessionId ? { requestSessionID: streamOptions.sessionId } : {}),
					stopReason: message.stopReason,
					usage: message.usage,
					calls: message.content.flatMap((item) => item.type === "toolCall" ? [{ tool: item.name, input: item.arguments }] : []),
				});
				return message;
			},
			onEvent: (event) => { events.push(event); options.onEvent?.(event); },
		}),
	});
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const sessionManager = SessionManager.inMemory(task.workspace);
	const resourceLoader = new DefaultResourceLoader({ cwd: task.workspace, agentDir, settingsManager, noExtensions: true, extensionFactories: [extension] });
	await resourceLoader.reload();
	const { session } = await createAgentSession({
		cwd: task.workspace, agentDir, model: input.actor, modelRuntime: await ModelRuntime.create({ credentials, modelsPath: null, refreshOnCreate: false }), thinkingLevel: "high", resourceLoader, settingsManager,
		tools: ["read", "grep", "find", "ls", "bash", "edit", "write"], sessionManager,
	});
	await session.bindExtensions({ mode: "print" });
	let turns = 0;
	const actorActionsByTool: Record<string, number> = {}, toolWallMs = new Map<string, number>();
	session.subscribe((event) => {
		if (event.type === "tool_execution_start") { increment(actorActionsByTool, event.toolName); toolWallMs.set(event.toolCallId, -performance.now()); }
		if (event.type === "tool_execution_end") toolWallMs.set(event.toolCallId, performance.now() + (toolWallMs.get(event.toolCallId) ?? 0));
		if (event.type === "turn_end" && ++turns >= input.maxTurns) void session.abort();
	});

	const agentStartedAt = performance.now();
	let agentCompletedAt = agentStartedAt;
	const benchmarkErrors: Record<string, string> = {};
	let timedOut = false;
	const timeout = setTimeout(() => {
		timedOut = true;
		void session.abort();
	}, input.timeoutMs);
	for (const [phase, operation] of [
		["prompt", () => session.prompt(`${task.row.problem_statement}\n\nWork in the checked-out repository and finish the implementation. Do not use network access to look up the answer.`)],
		["shutdown", () => session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" })],
		["dispose", () => session.dispose()],
	] as const) {
		try {
			await operation();
		} catch (error) {
			benchmarkErrors[phase] = String(error);
		}
		if (phase === "prompt") {
			agentCompletedAt = performance.now();
			clearTimeout(timeout);
		}
	}
	const taskCompletedAt = performance.now();
	// The Actor's transcript, calls with their results, for offline replay (bench/pattern-replay.ts).
	if (input.keepSession && input.output) await writeFile(input.output.replace(/(\.json)?$/, ".session.jsonl"),
		[sessionManager.getHeader(), ...sessionManager.getEntries()].map((entry) => JSON.stringify(entry)).join("\n"));
	const actorActions =Object.values(actorActionsByTool).reduce((sum, count) => sum + count, 0);
	const summary = summarizeSpeculativeTrace(events);
	const { candidateStartTrace, actorActionTrace, ...dimensions } = benchmarkTraceReport(events, actorActionsByTool, input.speculationEnabled);
	const actualEndToEndMs = taskCompletedAt - taskStartedAt;
	const hiddenLatencyMs = summary.hiddenLatencyMs;
	const serializedCounterfactualMs = actualEndToEndMs + hiddenLatencyMs;
	const nonToolMs = Math.max(0, serializedCounterfactualMs - summary.toolExecutionMs);
	const actorUsage = summarizeUsage(session.messages.filter((message) => message.role === "assistant"));
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
	const turnLimitReached = turns >= input.maxTurns;
	const { actor, drafter, repoCache, runRoot, output, prepareOnly, ...configuration } = input;
	return {
		metadata: {
			...configuration,
			implementationCommit,
			instance: task.row.instance_id,
			repo: task.row.repo,
			baseCommit: task.row.base_commit,
			language: task.row.language,
			sourceDataset: task.row.source_dataset,
			actor: `${actor.provider}/${actor.id}`,
			drafter: `${drafter.provider}/${drafter.id}`,
			timingScope: "setup, Agent prompt, terminal settlement, extension shutdown",
			patternState: input.patternState ?? "isolated-per-run",
			executionBoundary: "installed extension routes",
			workspace: task.workspace,
		},
		summary: {
			...summary,
			...dimensions,
			actualEndToEndMs,
			setupMs: agentStartedAt - taskStartedAt,
			agentPromptMs: agentCompletedAt - agentStartedAt,
			teardownMs: taskCompletedAt - agentCompletedAt,
			serializedCounterfactualMs,
			nonToolMs,
			authoritativeToolMs: summary.toolExecutionMs,
			accelerationRatio: actualEndToEndMs > 0 ? serializedCounterfactualMs / actualEndToEndMs : 1,
			estimatedSavingsMs: summary.estimatedSavingsMs,
			savingsAccelerationRatio: actualEndToEndMs > 0 ? 1 + summary.savingsMs / actualEndToEndMs : undefined,
			netAccelerationRatio: actualEndToEndMs > 0 ? 1 + summary.estimatedSavingsMs / actualEndToEndMs : undefined,
			actorActions,
			actorActionsByTool,
			actorFallbacks: input.speculationEnabled ? summary.actorFallbacks : actorActions,
			hitRate: actorActions ? summary.speculativeHits / actorActions : 0,
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
			turns,
			turnLimitReached,
			timedOut,
			agentError: session.state.errorMessage,
			benchmarkErrors,
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
				!session.state.errorMessage &&
				!Object.keys(benchmarkErrors).length &&
				patchClean &&
				changedFiles.length > 0 &&
				coveredGoldFiles.length > 0,
		},
		traces: {
			drafterPredictions: drafterPredictionTrace,
			candidateStarts: candidateStartTrace,
			// The Actor's wall time per call, beside its native service time, shows speculation's cost on its path.
			actorActions: actorActionTrace.map((action) => ({ ...action, wallMs: toolWallMs.get(action.id) })),
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
				entry[1] !== undefined && !SECRET_VARIABLE.test(entry[0]),
		),
	);
}

/** The dataset rows are fetched once per repository cache: every run of a suite reads the same rows. */
async function datasetRow(instanceID: string, cache: string): Promise<DatasetRow> {
	let value: unknown = await readFile(cache, "utf8").then(JSON.parse, () => undefined);
	if (!value) {
		const response = await fetch(DATASET_ROWS);
		if (!response.ok) throw new Error(`Dataset request failed with HTTP ${response.status}`);
		await writeFile(cache, JSON.stringify(value = await response.json()));
	}
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
	for (const key of ["instance_id", "repo", "base_commit", "patch", "test_patch", "problem_statement", "language", "source_dataset"] as const) {
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
	return values["model-base-url"] ? { ...resolved, baseUrl: values["model-base-url"] } : resolved;
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

function lines(value: string): string[] { return value .split(/\r?\n/) .map((line) => line.trim()) .filter(Boolean); }

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

async function exists(value: string): Promise<boolean> { try { await stat(value); return true; } catch { return false; } }

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
