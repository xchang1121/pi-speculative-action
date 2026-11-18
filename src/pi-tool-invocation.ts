import {
	createBashToolDefinition,
	createEditToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
	createWriteToolDefinition,
	formatSize,
	getAgentDir,
	getShellConfig,
	truncateHead,
	truncateLine,
	VERSION,
	type ExtensionContext,
	type ToolsOptions,
} from "@earendil-works/pi-coding-agent";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import { serialize } from "node:v8";
import path from "node:path";
import process from "node:process";
import type { Worker } from "node:worker_threads";

import {
	BASH_TIMEOUT_ACTION_KEY_PROJECTOR,
	GREP_LITERAL_ACTION_KEY_PROJECTOR,
	grepLiteralMatches,
	PI_ACTION_SEMANTICS,
	resolvePiToolPath,
	type ActionProjectionRule,
} from "./action-semantics.ts";
import { bindResourceOperation } from "./agent-execution-world.ts";
import { speculativeConfig } from "./config-schema.ts";
import {
	RESOURCE_OBSERVATION_EFFECTS,
	toolErrorSettlement,
	type ToolInvocation,
	type ToolSettlement,
} from "./execution-world.ts";
import { captureStableFile } from "./filesystem-evidence.ts";
import {
	asRecord,
	execHostFile,
	isRecord,
	isSafeCount,
	relativeFilesystemPath,
	resolveHostExecutable,
	slash,
} from "./primitives.ts";
import {
	captureResourceVersion,
	createSerializablePreparationBinding,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { SharedAbortScope } from "./runtime-lifecycle.ts";
const requirePi = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
const { Minimatch } = requirePi("minimatch") as {
	Minimatch: new (
		pattern: string,
		options: object,
	) => {
		match(name: string, partial?: boolean): boolean;
		globParts: string[][];
	};
};
const ignore: (options: object) => import("ignore").Ignore = requirePi("ignore");
function qualifiedPi(): boolean {
	const { strictHostVersionCheck, hostVersions } = speculativeConfig().runtime;
	return !strictHostVersionCheck || hostVersions.includes(VERSION);
}
export const PI_CLOSED_SEARCH_TOOLS: readonly string[] = ["find", "grep"];
// Pi's read resolver/sniffer are private APIs. Other versions retain observation, not assumed authority.
export const PI_OPERATION_TOOLS: Readonly<Record<"resources" | "workspace" | "process", readonly string[]>> =
	{
		resources: ["read", "ls"],
		workspace: ["write", "edit"],
		process: ["bash"],
	};
const toolFactories = {
	read: createReadToolDefinition,
	bash: createBashToolDefinition,
	edit: createEditToolDefinition,
	write: createWriteToolDefinition,
	grep: createGrepToolDefinition,
	find: createFindToolDefinition,
	ls: createLsToolDefinition,
};
type PiToolName = keyof typeof toolFactories;
export type PiToolDefinition = ReturnType<(typeof toolFactories)[PiToolName]>;
/** Stock definitions and their public operation seams; shared by Actor and isolated runners. */
export function createPiToolDefinitions(
	cwd: string,
	options: ToolsOptions = {},
): Map<string, PiToolDefinition> {
	return new Map(
		(Object.keys(toolFactories) as PiToolName[]).map((tool) => [
			tool,
			toolFactories[tool](cwd, options[tool] as never),
		]),
	);
}
export interface PiToolInvocationOptions {
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly shellPath?: string;
	readonly shellCommandPrefix?: string;
	readonly autoResizeImages?: boolean;
	readonly modelSupportsImages?: boolean;
}
/** Exact stock-Pi process identity shared by K(a) construction and isolated replay. */
export function resolvePiToolInvocation(
	tool: string,
	input: unknown,
	options: PiToolInvocationOptions,
): ToolInvocation | undefined {
	if (!qualifiedPi()) return;
	if ([...PI_OPERATION_TOOLS.resources, ...PI_OPERATION_TOOLS.workspace].includes(tool)) {
		const { cwd, autoResizeImages = true, modelSupportsImages = true } = options;
		const executor = "pi.filesystem.local";
		const requested = tool === "read" ? asRecord(input)?.path : undefined;
		const filesystem: NonNullable<ToolInvocation["filesystem"]> = async (view, request) => {
			const denied = (): never => {
				throw new Error("Filesystem operation is not authorized by this execution world");
			};
			const operations = {
				...view,
				access: (target: string) => view.access(target, tool === "edit"),
				exists: view.exists ?? denied,
				stat: view.stat ? (target: string) => view.stat!(target, "type", false) : denied,
				readdir: view.readdir ?? denied,
				writeFile: view.writeFile ?? denied,
				mkdir: view.mkdir ?? denied,
				detectImageMimeType: async (target: string) => {
					const mime = await import(
						new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
					);
					return mime.detectSupportedImageMimeType(
						await view.readFile(target, speculativeConfig().search.imageSniffBytes),
					);
				},
			};
			const definition = toolFactories[tool as PiToolName](cwd, { autoResizeImages, operations } as never);
			// The qualified stock read executor consults only model.input, never other context fields.
			const context = {
				model: { input: modelSupportsImages ? ["image"] : [] },
			} as ExtensionContext;
			const args =
				tool === "read" && typeof requested === "string"
					? { ...(request.args as object), path: resolvePiToolPath(requested, cwd) }
					: request.args;
			// Stock cancellation can reject before its internal operation and image worker finish.
			const result = await settleFilesystemOperation<ToolSettlement["result"]>(
				() => definition.execute(request.callID, args as never, undefined, undefined, context),
				request.signal,
			);
			return { result, isError: false };
		};
		return {
			executor,
			identity: { executor, cwd, autoResizeImages, modelSupportsImages },
			...(typeof requested === "string" ? { filesystemResources: [resolvePiToolPath(requested, cwd)] } : {}),
			...bindResourceOperation(
				filesystem,
				path.parse(cwd).root,
				undefined,
				PI_ACTION_SEMANTICS.definition(tool)?.effect === "observation",
			),
		};
	}
	const record = asRecord(input);
	if (!PI_OPERATION_TOOLS.process.includes(tool) || typeof record?.command !== "string") return;
	const shell = getShellConfig(options.shellPath);
	const executor = "pi.bash.local";
	const shared = {
		cwd: options.cwd,
		environment: options.environment,
		shell: shell.shell,
		shellArgs: [...shell.args],
		commandTransport: shell.commandTransport ?? "argv",
	};
	return {
		executor,
		identity: {
			executor,
			...shared,
			...(options.shellCommandPrefix ? { commandPrefix: options.shellCommandPrefix } : {}),
		},
		process: {
			...shared,
			command: options.shellCommandPrefix
				? `${options.shellCommandPrefix}\n${record.command}`
				: record.command,
			...(typeof record.timeout === "number" ? { timeout: record.timeout } : {}),
		},
	};
}

/** Only a finished command projects: Pi reports a timeout or non-zero exit as an error, which no other timeout reproduces. */
export const PI_BASH_TIMEOUT_PROJECTION_RULE: ActionProjectionRule<ToolSettlement> = {
	...BASH_TIMEOUT_ACTION_KEY_PROJECTOR,
	captureCoverage: (action, output) => (action.tool === "bash" && !output.isError ? true : undefined),
	projectOutput: ({ output }) => (output.isError ? undefined : output),
};
/** Pi prints each match of a complete literal search once, as `path:line: text`; the matches of a longer literal are a subset of those lines. */
export const PI_GREP_LITERAL_PROJECTION_RULE: ActionProjectionRule<ToolSettlement> = {
	...GREP_LITERAL_ACTION_KEY_PROJECTOR,
	// Pi reports a match limit, byte limit or cut line in details: only a result without any covers a refinement.
	captureCoverage: (action, output) =>
		action.tool === "grep" && !output.isError && output.result.details === undefined ? true : undefined,
	projectOutput: ({ speculative, actor, output }) => {
		const [content, ...rest] = output.result.content;
		const found = String(speculative.input.pattern);
		const sought = String(actor.input.pattern);
		if (output.isError || output.result.details !== undefined || rest.length || content?.type !== "text")
			return undefined;
		if (content.text === "No matches found") return output;
		const kept: string[] = [];
		const holds = (text: string, literal: string) =>
			grepLiteralMatches(text, literal, actor.input.ignoreCase === true);
		for (const line of content.text.split("\n")) {
			// A path may itself contain `:N: `: every split whose text holds the found literal must agree.
			const verdicts = new Set(
				[...line.matchAll(/:\d+: /g)]
					.map((split) => line.slice(split.index + split[0].length))
					.filter((text) => holds(text, found))
					.map((text) => holds(text, sought)),
			);
			if (verdicts.size !== 1) return undefined;
			if (verdicts.has(true)) kept.push(line);
		}
		// At its limit the Actor's own search stops early and says so.
		if (kept.length >= Number(actor.input.limit)) return undefined;
		const text = kept.length ? kept.join("\n") : "No matches found";
		return { ...output, result: { ...output.result, content: [{ type: "text", text }] } };
	},
};
/** Pi's bash throws once a completed command exits non-zero, appending this status last (aborts and timeouts append others). */
export function piToolErrorSettlement(tool: string, error: unknown): ToolSettlement {
	const code =
		tool === "bash" && error instanceof Error
			? /(?:^|\n\n)Command exited with code (\d+)$/.exec(error.message)?.[1]
			: undefined;
	return code === undefined
		? toolErrorSettlement(error)
		: { ...toolErrorSettlement(error), exitCode: Number(code) };
}
const filesystemCalls = new AsyncLocalStorage<Promise<void>[]>();
let filesystemExecutions = 0;
const ownFilesystemWorker = (worker: Worker) => {
	filesystemCalls.getStore()?.push(new Promise<void>((resolve) => worker.once("exit", () => resolve())));
};
/** Only the qualified stock filesystem contract runs here; unrelated Actor workers keep their owner. */
async function settleFilesystemOperation<Result>(
	operation: () => Promise<Result>,
	signal?: AbortSignal,
): Promise<Result> {
	signal?.throwIfAborted();
	const workers: Promise<void>[] = [];
	if (filesystemExecutions++ === 0) process.on("worker", ownFilesystemWorker);
	let result: Result;
	try {
		result = await filesystemCalls.run(workers, operation);
	} finally {
		// Node queues its worker-created event on nextTick, even if the tool already resolved.
		await new Promise<void>((resolve) => process.nextTick(resolve));
		while (workers.length) await Promise.all(workers.splice(0));
		if (--filesystemExecutions === 0) process.off("worker", ownFilesystemWorker);
	}
	signal?.throwIfAborted();
	return result;
}
export async function createClosedSearchProfile(cwd: string) {
	const profile = {
		id: "pi.captured-search",
		platform: process.platform,
		limits: speculativeConfig().search,
	};
	const root = path.parse(cwd).root;
	if (!qualifiedPi()) throw new Error(`Unqualified Pi version: ${VERSION}`);
	const unavailable: string[] = [];
	const engine = await prepareGrepEngine().catch((error) => {
		unavailable.push(String(error));
		return undefined;
	});
	const invocations = new Map<string, ToolInvocation>();
	for (const tool of PI_CLOSED_SEARCH_TOOLS) {
		if (tool === "grep" && !engine) continue;
		const semantics = PI_ACTION_SEMANTICS.definition(tool)!;
		const execute = async (
			request: Parameters<NonNullable<ToolInvocation["authoritative"]>>[0],
			provided?: ToolFilesystemOperations,
		): Promise<ToolSettlement> => {
			const capture = provided
				? undefined
				: await captureResourceVersion(undefined, root, PI_ACTION_SEMANTICS, profile.limits.maxInputBytes);
			const view = provided ?? capture!.view!;
			try {
				if (tool === "find") return await runCapturedFind(view, cwd, request.args, request.signal);
				return await runCapturedGrep(view, cwd, request.args as GrepInput, request.signal, engine!);
			} finally {
				await capture?.release();
			}
		};
		invocations.set(tool, {
			executor: profile.id,
			identity: {
				profile,
				cwd,
				home: os.homedir(),
				...(tool === "grep" ? { engine: engine!.identity } : {}),
			},
			semantics: {
				...semantics,
				epoch: profile.id,
				effect: "observation",
				requirements: RESOURCE_OBSERVATION_EFFECTS,
				resourceScope: "captured_inputs",
				canonicalize: (input) => {
					const action = semantics.canonicalize(input, cwd);
					return action && { ...action, input: asRecord(input)! };
				},
			},
			...bindResourceOperation(
				(view, request) => execute(request, view),
				root,
				(request) => execute(request),
				true,
			),
		});
	}
	return { profile, unavailable, invocations, dispose: async () => engine?.release() };
}

/** Select names exclusively through the captured view; ignored directories are never enumerated. */
async function walkSearchInputs(
	view: ToolFilesystemOperations,
	cwd: string,
	target: string,
	pattern: string | undefined,
	signal: AbortSignal,
	engine?: Awaited<ReturnType<typeof prepareGrepEngine>>,
	expression?: string,
): Promise<string[]> {
	const limits = speculativeConfig().search;
	const grep = !!engine;
	const original = target;
	if (grep) {
		const root = await Promise.resolve(view.stat!(target, "type", false)).catch(() => {
			throw new Error(`Path not found: ${target}`);
		});
		if (root.isDirectory()) target = root.realPath!;
		if (process.platform !== "win32") cwd = (await view.stat!(cwd, "type", false)).realPath!;
	}
	const options = {
		dot: true,
		matchBase: true,
		nocomment: true,
		nonegate: true,
		platform: "linux" as const,
		braceExpandMax: limits.braceExpandMax,
	};
	const negative = grep && pattern?.startsWith("!");
	if (grep)
		pattern = pattern
			?.replace(/^(!?)\//u, "$1")
			.replace(/(?<!\\)\{([^{}]*)\}/gu, (_, alternatives: string) => {
				const parts = alternatives.split(/(?<!\\),/u).filter(Boolean);
				return parts.length > 1 ? `{${parts.join(",")}}` : (parts[0] ?? "");
			});
	const directoryPattern = grep && pattern?.endsWith("/");
	if (directoryPattern) pattern = pattern!.slice(0, -1);
	const matcher = pattern ? new Minimatch(negative ? pattern.slice(1) : pattern, options) : undefined;
	type Rule = { directory: string; rank: number; matcher: import("ignore").Ignore };
	const layers = new Map<string, Rule[]>();
	const selected: string[] = [];
	let entries = 0;
	let bytes = 0;
	let nameBytes = 0;
	const repositories = new Set<string>();
	const text = async (file: string) => {
		if (!(await view.exists!(file))) return undefined;
		const content = await view.readFile(file);
		assert.ok((bytes += content.length) <= limits.maxInputBytes, "search input byte budget");
		return content.toString("utf8");
	};
	const rules = async (directory: string): Promise<Rule[]> => {
		const cached = layers.get(directory);
		if (cached) return cached;
		const parent = path.dirname(directory);
		let inherited = directory === parent || (!grep && directory === cwd) ? [] : await rules(parent);
		let exclude = path.join(directory, ".git/info/exclude");
		if (grep) {
			const git = path.join(directory, ".git");
			const hasGit = await view.exists!(git);
			if (hasGit || (engine!.jj && (await view.exists!(path.join(directory, ".jj"))))) {
				repositories.add(directory);
				inherited = inherited.filter((rule) => rule.rank > 1);
				if (hasGit && !(await view.stat!(git, "type", false)).isDirectory()) {
					const pointer = (await text(git))?.split(/\r?\n/u)[0];
					if (pointer?.startsWith("gitdir: ")) {
						const gitdir = path.resolve(cwd, pointer.slice(8));
						const common = (await text(path.join(gitdir, "commondir")))?.split(/\r?\n/u)[0];
						if (common) exclude = path.resolve(common.startsWith(".") ? gitdir : cwd, common, "info/exclude");
					}
				}
			} else if (repositories.has(parent)) repositories.add(directory);
		}
		const names = grep ? [exclude, ".gitignore", ".ignore", ".rgignore"] : [".gitignore"];
		const local: Rule[] = [];
		for (const [rank, name] of names.entries()) {
			if (grep && rank < 2 && !repositories.has(directory)) continue;
			const content = await text(path.resolve(directory, name));
			if (content !== undefined) {
				const matcher = ignore({ ignorecase: false }).add(content);
				if (grep && relativeFilesystemPath(directory, target) !== undefined)
					for (let name = target; name !== directory; name = path.dirname(name))
						matcher.add("!" + slash(path.relative(directory, name)).replace(/[\\*?\[\]!# ]/gu, "\\$&") + "/");
				local.push({ directory, rank, matcher });
			}
		}
		const result = [...inherited, ...local].sort((a, b) => a.rank - b.rank);
		layers.set(directory, result);
		return result;
	};
	const walk = async (file: string): Promise<void> => {
		signal.throwIfAborted();
		assert.ok(entries++ < limits.maxEntries, "search entry budget");
		if (grep) assert.ok(entries <= limits.maxDirectoryEntries, "metadata entry budget");
		const stat = await view.stat!(file, "entry", false);
		const root = file === target;
		if (!root && (stat.type === "symlink" || stat.type === "special")) {
			if (process.platform === "win32" && stat.type === "symlink")
				await Promise.resolve(view.stat!(file, "type", false)).catch(async (error) => {
					if (engine) await engine.execute(engine.root, ["--", expression!, "missing"], signal, file);
					throw error;
				});
			return;
		}
		const directory = (stat.type === "symlink" ? await view.stat!(file, "type", false) : stat).isDirectory();
		const relative = slash(path.relative(target, file));
		const name = grep
			? slash(path.relative(cwd, path.join(original, relative)))
			: relative + (directory ? "/" : "");
		const spellings = grep ? [name] : [name, `./${name}`, slash(file) + (directory ? "/" : "")];
		const matches = (!directoryPattern || directory) && spellings.some((name) => matcher?.match(name));
		let ignored = false;
		if (!root) {
			if ((directory && path.basename(file) === ".git") || (!grep && path.basename(file) === "node_modules"))
				return;
			for (const rule of await rules(path.dirname(file))) {
				const result = rule.matcher.test(slash(path.relative(rule.directory, file)) + (directory ? "/" : ""));
				if (result.ignored || result.unignored) ignored = result.ignored;
			}
			if (grep && matcher && matches) ignored = !!negative;
			if (ignored) return;
		}
		if (directory) {
			if (!grep && relative && matches) selected.push(file + path.sep);
			if (
				!grep &&
				relative &&
				matcher?.globParts.every((parts) => parts.length > 1) &&
				!spellings.some((name) => matcher.match(name.replace(/\/$/u, ""), true))
			)
				return;
			await rules(file);
			for (const name of await view.readdir!(file)) {
				assert.ok(path.basename(name) === name && name !== "." && name !== "..", "invalid directory entry");
				await walk(path.join(file, name));
			}
		} else if (grep ? root || !matcher || negative || matches : matches) {
			if (grep)
				assert.ok(
					(nameBytes += Buffer.byteLength(file) + 1) <= limits.maxResultBytes,
					"selected name budget",
				);
			selected.push(file);
		}
	};
	await walk(target);
	return (grep ? selected.map((file) => path.join(original, path.relative(target, file))) : selected).sort(
		(a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)),
	);
}

async function runCapturedFind(
	view: ToolFilesystemOperations,
	cwd: string,
	args: unknown,
	signal: AbortSignal,
): Promise<ToolSettlement> {
	const { execute } = createFindToolDefinition(cwd, {
		operations: {
			exists: view.exists!,
			glob: async (pattern, target, { limit }) =>
				(await walkSearchInputs(view, cwd, target, pattern, signal)).slice(0, limit),
		},
	});
	const result = await execute("captured-find", args as never, signal, undefined, {} as ExtensionContext);
	return { result, isError: false };
}

async function prepareGrepEngine() {
	const limits = speculativeConfig().search;
	const { getToolPath } = await import(
		new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
	);
	const selected: string | null = getToolPath("rg");
	const name = process.platform === "win32" ? "rg.exe" : "rg";
	const binary = await captureStableFile(
		await resolveHostExecutable(
			limits.executable || (selected && path.isAbsolute(selected) ? selected : undefined),
			"rg",
			[path.join(getAgentDir(), "bin", name)],
			process.platform === "win32" ? [name] : [],
		),
		speculativeConfig().processReuse.privateFileBytes,
		true,
	);
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-captured-search-"));
	const executable = path.join(root, name);
	const probeRoot = path.join(root, "qualification");
	const release = () => fs.rm(root, { recursive: true, force: true });
	const environment = {
		HOME: os.homedir(),
		LC_ALL: "C",
		...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
	};
	const flags = [
		"--no-config",
		"--sort=path",
		"--no-ignore",
		"--json",
		"--line-number",
		"--color=never",
		"--hidden",
	];
	const execute = async (cwd: string, args: string[], signal: AbortSignal, missing?: string) => {
		const result = await execHostFile(executable, [...flags, ...args], {
			cwd,
			env: environment,
			signal,
			windowsHide: true,
			encoding: "utf8",
			maxBuffer: cwd === probeRoot ? limits.maxResultBytes : limits.maxInputBytes,
		}).catch((error) => {
			if (error.code === 1) return { stdout: "", stderr: "" };
			if (error.stderr) {
				const message = String(error.stderr).trim();
				throw new Error(
					missing
						? message.replace(
								/^rg: missing:(?: IO error for operation on missing:)?/u,
								() => `rg: ${missing}:`,
							)
						: message,
				);
			}
			throw error;
		});
		signal.throwIfAborted();
		const matches = new Map<string, GrepMatch[]>();
		for (const line of result.stdout.split("\n")) {
			if (!line) continue;
			const { type, data } = JSON.parse(line);
			if (type !== "match") continue;
			const key = path.basename(data.path.text);
			if (!matches.has(key)) matches.set(key, []);
			matches.get(key)!.push({ line_number: data.line_number, lines: data.lines });
		}
		return matches;
	};
	try {
		await fs.writeFile(executable, binary.content!, { flag: "wx", mode: 0o500 });
		await fs.mkdir(path.join(probeRoot, ".jj"), { recursive: true });
		for (const file of ["probe", "ignored"])
			await fs.writeFile(path.join(probeRoot, file), "capability-probe\n");
		await fs.writeFile(path.join(probeRoot, ".gitignore"), "ignored\n");
		const args = ["--ignore", "--no-ignore-parent", "--no-ignore-global", "--", "capability-probe", "."];
		const probe = await execute(
			probeRoot,
			args,
			AbortSignal.timeout(speculativeConfig().subcomputation.socketTimeoutMs),
		);
		assert.equal(probe.get("probe")?.[0]?.line_number, 1);
		const jj = !probe.has("ignored");
		await fs.rm(probeRoot, { recursive: true });
		const identity = {
			sha256: binary.hash,
			platform: process.platform,
			arch: process.arch,
			environment,
			flags,
		};
		const matchesBinding = createSerializablePreparationBinding(
			{ engine: identity, namespace: "pi.captured-grep" },
			acceptsGrepMatches,
		);
		return { root, identity, matchesBinding, execute, release, jj };
	} catch (error) {
		await release();
		throw error;
	}
}

type GrepInput = Parameters<ReturnType<typeof createGrepToolDefinition>["execute"]>[1];
type GrepMatch = { line_number: number; lines: { text?: string; bytes?: string } };
function acceptsGrepMatches(value: unknown): boolean {
	return (
		Array.isArray(value) &&
		value.every(
			(match) =>
				isRecord(match) &&
				isSafeCount(match.line_number, 1) &&
				isRecord(match.lines) &&
				(typeof match.lines.text === "string" || typeof match.lines.bytes === "string"),
		)
	);
}

async function runCapturedGrep(
	view: ToolFilesystemOperations,
	cwd: string,
	query: GrepInput,
	signal: AbortSignal,
	engine: Awaited<ReturnType<typeof prepareGrepEngine>>,
): Promise<ToolSettlement> {
	const limits = speculativeConfig().search;
	const target = resolvePiToolPath(query.path || ".", cwd);
	const files = await walkSearchInputs(view, cwd, target, query.glob, signal, engine, query.pattern);
	let pending: { source: string; inputs: ToolFilesystemOperations; signal: AbortSignal }[] = [];
	let batch: Promise<GrepMatch[][]> | undefined;
	const enqueue = (source: string, inputs: ToolFilesystemOperations, signal: AbortSignal) => {
		const index = pending.push({ source, inputs, signal }) - 1;
		batch ??= new Promise<void>(setImmediate).then(async () => {
			const inputs = pending;
			pending = [];
			batch = undefined;
			const root = await fs.mkdtemp(path.join(engine.root, "matches-"));
			const scope = new SharedAbortScope();
			const release = inputs.map((item) => scope.acquire(item.signal));
			let bytes = 0;
			try {
				for (const [index, item] of inputs.entries()) {
					scope.signal.throwIfAborted();
					const content = item.source ? await item.inputs.readFile(item.source) : Buffer.alloc(0);
					assert.ok((bytes += content.length) <= limits.maxInputBytes, "search input byte budget");
					await fs.writeFile(path.join(root, String(index)), content);
				}
				const args = ["--", query.pattern, root];
				if (query.ignoreCase) args.unshift("--ignore-case");
				if (query.literal) args.unshift("--fixed-strings");
				const byFile = await engine.execute(root, args, scope.signal);
				return inputs.map((_, index) => byFile.get(String(index)) ?? []);
			} finally {
				for (const withdraw of release) withdraw();
				await fs.rm(root, { recursive: true, force: true });
			}
		});
		return batch.then((results) => {
			signal.throwIfAborted();
			return results[index]!;
		});
	};
	const jobs = (files.length ? files : [""]).map((source) => {
		const build = async (inputs: ToolFilesystemOperations, signal: AbortSignal) => {
			const value = await enqueue(source, inputs, signal);
			return { value, bytes: serialize(value).byteLength, dispose: () => {} };
		};
		const key = JSON.stringify([source, query.pattern, !!query.ignoreCase, !!query.literal]);
		return source && view.prepare
			? view.prepare(engine.matchesBinding, key, build, async (value) => value, source, signal)
			: build(view, signal).then((result) => result.value);
	});
	const matches = await Promise.all(jobs).finally(() => Promise.allSettled(jobs));
	const limit = Math.max(1, query.limit ?? limits.grepLimit);
	const context = Math.max(0, query.context ?? 0);
	const directory = (await view.stat!(target, "type", false)).isDirectory();
	const output: string[] = [];
	let linesTruncated = false;
	let count = 0;
	for (const [index, rows] of matches.entries()) {
		if (count >= limit) break;
		if (!rows.length) continue;
		const file = files[index]!;
		const relative = path.relative(target, file);
		const label = directory && relative && !relative.startsWith("..") ? slash(relative) : path.basename(file);
		let lines: string[] | undefined;
		for (const match of rows.slice(0, limit - count)) {
			count++;
			const row = match.line_number;
			const inline = context
				? undefined
				: match.lines.text?.replace(/\r\n/g, "\n").replace(/\r/g, "").replace(/\n$/, "");
			if (inline === undefined)
				lines ??= (await view.readFile(file)).toString("utf8").replace(/\r\n?/g, "\n").split("\n");
			for (let i = Math.max(1, row - context); i <= Math.min(lines?.length ?? row, row + context); i++) {
				const line = truncateLine(
					inline ?? lines![i - 1] ?? "",
					inline === undefined ? limits.lineCharacters : undefined,
				);
				linesTruncated ||= line.wasTruncated;
				const delimiter = i === row ? ":" : "-";
				output.push(`${label}${delimiter}${i}${delimiter} ${line.text}`);
			}
		}
	}
	const truncation = truncateHead(output.join("\n"), {
		maxLines: Number.MAX_SAFE_INTEGER,
		maxBytes: limits.displayBytes,
	});
	const details: Record<string, unknown> = {};
	const notices: string[] = [];
	if (count >= limit) {
		details.matchLimitReached = limit;
		notices.push(`${limit} matches limit reached. Use limit=${limit * 2} for more, or refine pattern`);
	}
	if (truncation.truncated) {
		details.truncation = truncation;
		notices.push(`${formatSize(limits.displayBytes)} limit reached`);
	}
	if (linesTruncated) {
		details.linesTruncated = true;
		notices.push(`Some lines truncated to ${limits.lineCharacters} chars. Use read tool to see full lines`);
	}
	const text = count
		? truncation.content + (notices.length ? `\n\n[${notices.join(". ")}]` : "")
		: "No matches found";
	return {
		result: {
			content: [{ type: "text", text }],
			details: Object.keys(details).length ? details : undefined,
		},
		isError: false,
	};
}
