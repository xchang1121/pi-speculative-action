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
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import { serialize } from "node:v8";
import path from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { Worker } from "node:worker_threads";
import type { ActionProjectionRule } from "./action-semantics.ts";
import {
	BASH_TIMEOUT_ACTION_KEY_PROJECTOR,
	GREP_LITERAL_ACTION_KEY_PROJECTOR,
	grepLiteralMatches,
	PI_ACTION_SEMANTICS,
	type ActionSemanticsDefinition,
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
import { asRecord, isSafeCount, relativeFilesystemPath, resolveHostExecutable, slash } from "./primitives.ts";
import {
	captureResourceVersion,
	createSerializablePreparationBinding,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
import { SharedAbortScope } from "./runtime-lifecycle.ts";
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
	if (
		speculativeConfig().runtime.strictHostVersionCheck &&
		!speculativeConfig().runtime.hostVersions.includes(VERSION)
	)
		return;
	if ([...PI_OPERATION_TOOLS.resources, ...PI_OPERATION_TOOLS.workspace].includes(tool)) {
		const cwd = options.cwd;
		const autoResizeImages = options.autoResizeImages ?? true;
		const modelSupportsImages = options.modelSupportsImages ?? true;
		const executor = "pi.filesystem.local";
		const requested = tool === "read" ? asRecord(input)?.path : undefined;
		const invocation: ToolInvocation = {
			executor,
			filesystemRoot: path.parse(cwd).root,
			...(typeof requested === "string" ? { filesystemResources: readResourcePaths(requested, cwd) } : {}),
			identity: {
				executor,
				cwd,
				version: VERSION,
				autoResizeImages,
				modelSupportsImages,
			},
			filesystem: async (view, request) => {
				const denied = (): never => {
					throw new Error("Filesystem operation is not authorized by this execution world");
				};
				let scoped: ToolsOptions;
				if (tool === "read") {
					scoped = {
						read: {
							autoResizeImages,
							operations: {
								access: view.access,
								readFile: view.readFile,
								detectImageMimeType: async (target) => {
									const mime = await import(
										new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
									);
									return mime.detectSupportedImageMimeType(
										await view.readFile(target, speculativeConfig().search.imageSniffBytes),
									);
								},
							},
						},
					};
				} else {
					if (tool === "ls") {
						scoped = {
							ls: {
								operations: {
									exists: view.exists ?? denied,
									stat: view.stat ? (target) => view.stat!(target, "type", false) : denied,
									readdir: view.readdir ?? denied,
								},
							},
						};
					} else {
						if (tool === "write") {
							scoped = {
								write: {
									operations: {
										writeFile: view.writeFile ?? denied,
										mkdir: view.mkdir ?? denied,
									},
								},
							};
						} else {
							scoped = {
								edit: {
									operations: {
										readFile: view.readFile,
										access: (target) => view.access(target, true),
										writeFile: view.writeFile ?? denied,
									},
								},
							};
						}
					}
				}
				const factory = toolFactories[tool as PiToolName];
				const definition = factory(cwd, scoped[tool as PiToolName] as never);
				// The qualified stock read executor consults only model.input, never other context fields.
				const context = {
					model: { input: modelSupportsImages ? ["image"] : [] },
				} as ExtensionContext;
				// Stock cancellation can reject before its internal operation and image worker finish.
				const result = await settleFilesystemOperation<ToolSettlement["result"]>(
					view,
					() =>
						definition.execute(
							request.callID,
							(tool === "read" && typeof requested === "string"
								? {
										...(request.args as object),
										path: readResourcePaths(requested, cwd)[0],
									}
								: request.args) as never,
							undefined,
							undefined,
							context,
						),
					request.signal,
				);
				return { result, isError: false };
			},
		};
		return {
			...invocation,
			...bindResourceOperation(
				invocation.filesystem!,
				invocation.filesystemRoot!,
				undefined,
				PI_ACTION_SEMANTICS.definition(tool)?.effect === "observation",
			),
		};
	}
	if (
		!PI_OPERATION_TOOLS.process.includes(tool) ||
		!input ||
		typeof input !== "object" ||
		Array.isArray(input)
	)
		return undefined;
	const record = input as Record<string, unknown>;
	if (typeof record.command !== "string") return undefined;
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
		return {
			result: {
				...output.result,
				content: [
					{
						type: "text",
						text: kept.length ? kept.join("\n") : "No matches found",
					},
				],
			},
			isError: false,
		};
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
type FilesystemCall = {
	workers: Promise<void>[];
	exists?: ToolFilesystemOperations["exists"];
	paths?: string[];
	failure?: unknown;
};
const filesystemCalls = new AsyncLocalStorage<FilesystemCall>();
let filesystemExecutions = 0;
const ownFilesystemWorker = (worker: Worker) => {
	filesystemCalls
		.getStore()
		?.workers.push(new Promise<void>((resolve) => worker.once("exit", () => resolve())));
};
function readResourcePaths(input: string, cwd: string): readonly string[] {
	const requirePi = createRequire(import.meta.url);
	const { resolvePath } = requirePi(
		fileURLToPath(new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent"))),
	);
	return [
		resolvePath(input, cwd, {
			homeDir: os.homedir(),
			normalizeUnicodeSpaces: true,
			stripAtPrefix: true,
		}),
	];
}
/** Only the qualified stock filesystem contract runs here; unrelated Actor workers keep their owner. */
async function settleFilesystemOperation<Result>(
	view: ToolFilesystemOperations,
	operation: () => Promise<Result>,
	signal?: AbortSignal,
): Promise<Result> {
	signal?.throwIfAborted();
	const workers: Promise<void>[] = [];
	const call: FilesystemCall = { workers, exists: view.exists };
	if (filesystemExecutions++ === 0) process.on("worker", ownFilesystemWorker);
	let result: Result;
	try {
		result = await filesystemCalls.run(call, operation);
	} finally {
		// Node queues its worker-created event on nextTick, even if the tool already resolved.
		await new Promise<void>((resolve) => process.nextTick(resolve));
		while (workers.length) await Promise.all(workers.splice(0));
		if (--filesystemExecutions === 0) process.off("worker", ownFilesystemWorker);
		// Stock pathExists catches access errors; an unproven observation must still reject the whole call.
		if ("failure" in call) throw call.failure;
	}
	signal?.throwIfAborted();
	return result;
}
const capturedSearchProfile = () =>
	Object.freeze({
		id: "pi.captured-search.explicit-v2",
		pi: VERSION,
		platform: process.platform,
		find: {
			platform: "linux",
			nocase: false,
			dot: true,
			matchBase: true,
			nocomment: true,
			nonegate: true,
			braceExpandMax: speculativeConfig().search.braceExpandMax,
		},
		grep: { flags: ["--no-config", "--sort=path", "--no-ignore-global"] },
		limits: {
			inputBytes: speculativeConfig().search.maxInputBytes,
			entries: speculativeConfig().search.maxEntries,
		},
	});
export async function createClosedSearchProfile(cwd: string) {
	if (
		speculativeConfig().runtime.strictHostVersionCheck &&
		!speculativeConfig().runtime.hostVersions.includes(VERSION)
	)
		throw new Error(`Unqualified Pi version: ${VERSION}`);
	const home = os.homedir();
	const profile = capturedSearchProfile();
	const { resolvePath } = await import(
		new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
	);
	const unavailable: string[] = [];
	const grep = await prepareGrepEngine(home, profile.grep, (reason) => unavailable.push(String(reason)));
	const invocations = new Map<string, ToolInvocation>();
	try {
		for (const tool of PI_CLOSED_SEARCH_TOOLS) {
			if (tool === "grep" && !grep) continue;
			const semantics = await createClosedSearchSemantics(tool, cwd, home);
			const execute = async (
				request: Parameters<NonNullable<ToolInvocation["authoritative"]>>[0],
				provided?: ToolFilesystemOperations,
			): Promise<ToolSettlement> => {
				const capture = provided
					? undefined
					: await captureResourceVersion(undefined, cwd, PI_ACTION_SEMANTICS, profile.limits.inputBytes);
				const view = provided ?? capture!.view!;
				try {
					if (tool === "find") return await runCapturedFind(view, cwd, request.args, request.signal);
					const engine = grep!;
					const query = request.args as GrepInput;
					const target = resolvePath(query.path || ".", cwd, {
						homeDir: home,
						normalizeUnicodeSpaces: true,
						stripAtPrefix: true,
					});
					const build = async (inputs: ToolFilesystemOperations, signal: AbortSignal) => {
						const root = await fs.mkdtemp(path.join(engine.root, "inputs-"));
						const dispose = async () => {
							assert.equal(path.dirname(root), engine.root);
							await fs.rm(root, { recursive: true, force: true });
						};
						try {
							const prepared = await prepareCapturedGrep(
								inputs,
								cwd,
								root,
								{ ...query, path: target },
								signal,
								(directory, args, stop, emit) => engine.execute("selection", directory, args, stop, emit),
							);
							return {
								value: { root, ...prepared },
								bytes: prepared.bytes,
								dispose,
							};
						} catch (error) {
							await dispose();
							throw error;
						}
					};
					const run = (prepared: PreparedGrep) =>
						formatCapturedGrep(view, engine, prepared, { ...query, path: target }, request.signal);
					if (view.prepare)
						return await view.prepare(
							engine,
							JSON.stringify([target, query.glob]),
							build,
							run,
							target,
							request.signal,
						);
					const prepared = await build(view, request.signal);
					try {
						return await run(prepared.value);
					} finally {
						await prepared.dispose();
					}
				} finally {
					await capture?.release();
				}
			};
			invocations.set(
				tool,
				Object.freeze({
					executor: profile.id,
					identity: {
						profile,
						cwd,
						home,
						...(tool === "grep" ? { engine: grep!.identity } : {}),
					},
					semantics,
					...bindResourceOperation(
						(view, request) => execute(request, view),
						path.parse(cwd).root,
						(request) => execute(request),
						true,
					),
				} satisfies ToolInvocation),
			);
		}
		return {
			profile,
			unavailable,
			invocations: invocations as ReadonlyMap<string, ToolInvocation>,
			dispose: async () => {
				await grep?.release();
			},
		};
	} catch (error) {
		await grep?.release();
		throw error;
	}
}
async function runCapturedFind(
	view: ToolFilesystemOperations,
	root: string,
	args: unknown,
	signal: AbortSignal,
): Promise<ToolSettlement> {
	const requirePi = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
	interface Matcher {
		match(value: string, partial?: boolean): boolean;
		globParts: readonly (readonly string[])[];
	}
	const { Minimatch } = requirePi("minimatch") as {
		Minimatch: new (pattern: string, options: object) => Matcher;
	};
	const ignore = requirePi("ignore") as (options: object) => {
		add(text: string): {
			test(value: string): {
				ignored: boolean;
				unignored: boolean;
			};
		};
	};
	const namespace = path.posix;
	const profile = capturedSearchProfile();
	const input = new Map<string, unknown>();
	const read = async (operation: string, target: string): Promise<unknown> => {
		signal.throwIfAborted();
		const key = JSON.stringify([operation, target]);
		if (!input.has(key)) {
			assert.ok(input.size < profile.limits.entries, "input entry budget");
			input.set(key, await readClosedSearchInput(view, root, operation, target, profile.limits.inputBytes));
		}
		return input.get(key);
	};
	type IgnoreRule = {
		directory: string;
		matcher: ReturnType<ReturnType<typeof ignore>["add"]>;
	};
	const rules = new Map<string, IgnoreRule[]>();
	const decisions = new Map<string, boolean>();
	const layers = async (directory: string): Promise<IgnoreRule[]> => {
		const cached = rules.get(directory);
		if (cached) return cached;
		const inherited = directory === "/workspace" ? [] : await layers(namespace.dirname(directory));
		let text = "";
		try {
			text = Buffer.from(
				(await read("readFile", namespace.join(directory, ".gitignore"))) as Uint8Array,
			).toString("utf8");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const selected = [...inherited, { directory, matcher: ignore({ ignorecase: false }).add(text) }];
		rules.set(directory, selected);
		return selected;
	};
	const ignored = async (target: string, directory: boolean): Promise<boolean> => {
		if (target === "/workspace") return false;
		const cached = decisions.get(target);
		if (cached !== undefined) return cached;
		const parent = namespace.dirname(target);
		let excluded = await ignored(parent, true);
		if (!excluded) {
			for (const layer of await layers(parent)) {
				const relative = namespace.relative(layer.directory, target) + (directory ? "/" : "");
				const match = layer.matcher.test(relative);
				if (match.ignored || match.unignored) excluded = match.ignored;
			}
		}
		decisions.set(target, excluded);
		return excluded;
	};
	const tool = createFindToolDefinition(root, {
		operations: {
			exists: async (target) => {
				try {
					await read("stat", (await read("resolve", target)) as string);
					return true;
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
					throw error;
				}
			},
			glob: async (pattern, cwd, options) => {
				const base = (await read("resolve", cwd)) as string;
				const matches: string[] = [];
				const matcher = new Minimatch(pattern, profile.find);
				const excluded = options.ignore.map((pattern) => new Minimatch(pattern, profile.find));
				const walk = async (target: string): Promise<void> => {
					const { directory } = (await read("stat", target)) as {
						directory: boolean;
					};
					const relative = namespace.relative(base, target);
					if (
						(await ignored(target, directory)) ||
						excluded.some((rule) => rule.match(target + (directory ? "/" : "")))
					)
						return;
					const spellings = [relative, `./${relative}`, target];
					if (relative && spellings.some((value) => matcher.match(value + (directory ? "/" : "")))) {
						matches.push(
							path.resolve(root, namespace.relative("/workspace", target)) + (directory ? path.sep : ""),
						);
					}
					if (
						directory &&
						(!relative ||
							matcher.globParts.some((parts) => parts.length === 1) ||
							spellings.some((value) => matcher.match(value, true)))
					) {
						for (const name of (await read("readdir", target)) as string[])
							await walk(namespace.join(target, name));
					}
				};
				await walk(base);
				return matches.sort().slice(0, options.limit);
			},
		},
	});
	return {
		result: await tool.execute("captured-find", args as never, signal, undefined, {} as ExtensionContext),
		isError: false,
	};
}
async function formatCapturedGrep(
	view: ToolFilesystemOperations,
	engine: NonNullable<Awaited<ReturnType<typeof prepareGrepEngine>>>,
	prepared: PreparedGrep,
	query: GrepInput & {
		path: string;
	},
	signal: AbortSignal,
): Promise<ToolSettlement> {
	type Match = {
		path: {
			text: string;
		};
		line_number: number;
		lines: {
			text?: string;
		};
	};
	const matches: Match[] = [];
	await matchCapturedGrep(view, engine, prepared, query, signal, ({ fd, data }) => {
		if (fd !== 1) return;
		for (const line of data.toString("utf8").split("\n")) {
			if (!line) continue;
			const event = JSON.parse(line);
			if (event.type === "match") matches.push(event.data);
		}
	});
	if (!matches.length)
		return {
			result: {
				content: [{ type: "text", text: "No matches found" }],
				details: undefined,
			},
			isError: false,
		};
	const limit = Math.max(1, query.limit ?? speculativeConfig().search.grepLimit);
	const context = Math.max(0, query.context ?? 0);
	const directory = !!view.stat && (await view.stat(query.path, "type", false)).isDirectory();
	const target = fileURLToPath(prepared.path);
	const output: string[] = [];
	let linesTruncated = false;
	for (const match of matches.slice(0, limit)) {
		const file = prepared.files.find((file) => file.path === match.path.text)!;
		const relative = path.relative(target, file.path);
		const label =
			directory && relative && !relative.startsWith("..") ? slash(relative) : path.basename(file.path);
		if (!context && match.lines.text !== undefined) {
			const line = truncateLine(
				match.lines.text.replace(/\r\n/g, "\n").replace(/\r/g, "").replace(/\n$/, ""),
			);
			linesTruncated ||= line.wasTruncated;
			output.push(`${label}:${match.line_number}: ${line.text}`);
			continue;
		}
		const lines = (await view.readFile(file.source))
			.toString("utf8")
			.replace(/\r\n/g, "\n")
			.replace(/\r/g, "\n")
			.split("\n");
		for (
			let current = Math.max(1, match.line_number - context);
			current <= Math.min(lines.length, match.line_number + context);
			current++
		) {
			const line = truncateLine(lines[current - 1] ?? "", speculativeConfig().search.lineCharacters);
			linesTruncated ||= line.wasTruncated;
			output.push(
				current === match.line_number
					? `${label}:${current}: ${line.text}`
					: `${label}-${current}- ${line.text}`,
			);
		}
	}
	const truncation = truncateHead(output.join("\n"), {
		maxLines: Number.MAX_SAFE_INTEGER,
		maxBytes: speculativeConfig().search.displayBytes,
	});
	const details: Record<string, unknown> = {};
	const notices: string[] = [];
	if (matches.length >= limit) {
		details.matchLimitReached = limit;
		notices.push(`${limit} matches limit reached. Use limit=${limit * 2} for more, or refine pattern`);
	}
	if (truncation.truncated) {
		details.truncation = truncation;
		notices.push(`${formatSize(speculativeConfig().search.displayBytes)} limit reached`);
	}
	if (linesTruncated) {
		details.linesTruncated = true;
		notices.push(
			`Some lines truncated to ${speculativeConfig().search.lineCharacters} chars. Use read tool to see full lines`,
		);
	}
	const text = truncation.content + (notices.length ? `\n\n[${notices.join(". ")}]` : "");
	return {
		result: {
			content: [{ type: "text", text }],
			details: Object.keys(details).length ? details : undefined,
		},
		isError: false,
	};
}
/** Pin only an already-installed qualified rg; failure leaves the independent find executor available. */
async function prepareGrepEngine(
	home: string,
	profile: {
		readonly flags: readonly string[];
	},
	unavailable: (error: unknown) => void,
) {
	let root: string | undefined;
	try {
		const { getToolPath } = await import(
			new URL("./utils/tools-manager.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
		);
		const selected: string | null = getToolPath("rg");
		const existing = path.join(getAgentDir(), "bin", process.platform === "win32" ? "rg.exe" : "rg");
		const binary = await captureStableFile(
			await resolveHostExecutable(
				speculativeConfig().search.executable ||
					(selected && path.isAbsolute(selected) ? selected : undefined),
				"rg",
				[existing],
				process.platform === "win32" ? ["rg.exe"] : [],
			),
			speculativeConfig().processReuse.privateFileBytes,
			true,
		);
		root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-captured-search-"));
		const owned = root;
		const executable = path.join(owned, process.platform === "win32" ? "rg.exe" : "rg");
		await fs.writeFile(executable, binary.content!, {
			flag: "wx",
			mode: 0o500,
		});
		const environment = Object.freeze({
			HOME: home,
			LC_ALL: "C",
			...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
		});
		const flags = Object.freeze([...profile.flags]);
		const version: Buffer[] = [];
		const signal = AbortSignal.timeout(speculativeConfig().subcomputation.socketTimeoutMs);
		let bytes = 0;
		await fs.writeFile(path.join(owned, "probe.txt"), "capability-probe\n");
		const result = await runCapturedSearchProcess(
			executable,
			[...flags, "--json", "--fixed-strings", "capability-probe", "probe.txt"],
			owned,
			environment,
			signal,
			({ data }) => {
				assert.ok((bytes += data.length) <= speculativeConfig().search.maxResultBytes, "rg probe budget");
				version.push(data);
			},
		);
		signal.throwIfAborted();
		assert.equal(result.code, 0);
		assert.ok(
			Buffer.concat(version)
				.toString()
				.split("\n")
				.filter(Boolean)
				.map((line) => JSON.parse(line))
				.some((event) => event.type === "match" && event.data.line_number === 1),
			"rg JSON capability",
		);
		await fs.rm(path.join(owned, "probe.txt"));
		const identity = Object.freeze({
			sha256: binary.hash,
			platform: process.platform,
			arch: process.arch,
			environment,
			flags,
		});
		const matchesBinding: object = createSerializablePreparationBinding(
			{ engine: identity, namespace: "pi.captured-grep", version: "2" },
			acceptsGrepMatches,
		);
		return {
			root: owned,
			identity,
			matchesBinding,
			execute: (
				stage: "actor" | "selection" | "producer",
				cwd: string,
				args: readonly string[],
				signal: AbortSignal,
				emit: (output: SearchOutput) => void,
			) =>
				runCapturedSearchProcess(
					executable,
					[
						...flags,
						...(stage === "actor" ? [] : ["--no-ignore-parent"]),
						...(stage === "producer" ? ["--no-ignore"] : []),
						...args,
					],
					cwd,
					environment,
					signal,
					emit,
				),
			release: async () => {
				assert.equal(path.dirname(owned), path.resolve(os.tmpdir()));
				await fs.rm(owned, { recursive: true, force: true });
			},
		};
	} catch (error) {
		unavailable(error);
		if (root) {
			assert.equal(path.dirname(root), path.resolve(os.tmpdir()));
			await fs.rm(root, { recursive: true, force: true });
		}
		return undefined;
	}
}
/** A borrowed native request settles only after process/stream closure, including cancellation or output failure. */
export async function runCapturedSearchProcess(
	executable: string,
	args: readonly string[],
	cwd: string,
	environment: Readonly<Record<string, string>>,
	signal: AbortSignal,
	emit: (output: SearchOutput) => void,
	onSpawn?: () => void,
) {
	signal.throwIfAborted();
	const child = spawn(executable, args, {
		cwd,
		env: environment,
		stdio: ["ignore", "pipe", "pipe"],
		windowsHide: true,
	});
	return new Promise<{
		code: number | null;
		signal: NodeJS.Signals | null;
	}>((resolve, reject) => {
		let failure:
			| {
					reason: unknown;
			  }
			| undefined;
		const stop = () => {
			child.kill("SIGKILL");
		};
		signal.addEventListener("abort", stop, { once: true });
		if (signal.aborted) stop();
		if (onSpawn) child.once("spawn", onSpawn);
		for (const [fd, output] of [
			[1, child.stdout],
			[2, child.stderr],
		] as const)
			output.on("data", (data: Buffer) => {
				if (!failure)
					try {
						emit({ fd, data });
					} catch (error) {
						failure = { reason: error };
						stop();
					}
			});
		child.once("error", (error) => {
			failure ??= { reason: error };
		});
		child.once("close", (code, signalValue) => {
			signal.removeEventListener("abort", stop);
			if (failure) reject(failure.reason);
			else resolve({ code, signal: signalValue });
		});
	});
}
/** Index paths with the bound resolver; replay the exact prepared arguments, never a second normalization. */
export async function createClosedSearchSemantics(
	tool: string,
	cwd: string,
	homeDir: string,
): Promise<ActionSemanticsDefinition> {
	assert.ok(tool === "find" || tool === "grep");
	const profile = capturedSearchProfile();
	const { resolvePath } = await import(
		new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href
	);
	return Object.freeze({
		...PI_ACTION_SEMANTICS.definition(tool)!,
		epoch: profile.id,
		effect: "observation",
		requirements: RESOURCE_OBSERVATION_EFFECTS,
		resourceScope: "captured_inputs",
		canonicalize: (input: unknown) => {
			const record = asRecord(input);
			if (
				!record ||
				typeof record.pattern !== "string" ||
				(record.path !== undefined && typeof record.path !== "string")
			)
				return undefined;
			const target = resolvePath(record.path || ".", cwd, {
				homeDir,
				normalizeUnicodeSpaces: true,
				stripAtPrefix: true,
			});
			const relative = relativeFilesystemPath(cwd, target);
			return relative === undefined ? undefined : { input: record, resources: [slash(relative || ".")] };
		},
	});
}
/** Translate a closed namespace using only captured positive/negative evidence, never ambient host fs. */
export async function readClosedSearchInput(
	source: ToolFilesystemOperations,
	root: string,
	operation: string,
	target: string,
	maxBytes: number,
): Promise<unknown> {
	const fail = (code: string): never => {
		throw Object.assign(new Error(`${code}: ${target}`), { code });
	};
	if (operation === "resolve") {
		const relative = relativeFilesystemPath(root, target);
		return relative === undefined ? fail("ENOENT") : path.posix.join("/workspace", slash(relative));
	}
	assert.ok(
		source.stat &&
			source.readdir &&
			["stat", "readdir", "readFile"].includes(operation) &&
			path.posix.isAbsolute(target),
		"input operation denied",
	);
	const normalized = path.posix.normalize(target);
	if (normalized === "/")
		return operation === "stat"
			? { directory: true }
			: operation === "readdir"
				? ["workspace"]
				: fail("EISDIR");
	const relative = path.posix.relative("/workspace", normalized);
	if (relative === ".." || relative.startsWith("../")) return fail("ENOENT");
	let physical = root;
	for (const segment of relative ? relative.split("/") : []) {
		if (!(await source.stat(physical, "type", false)).isDirectory()) return fail("ENOTDIR");
		if (!(await source.readdir(physical)).includes(segment)) return fail("ENOENT");
		physical = path.join(physical, segment);
	}
	const stat = await source.stat(physical, "type", operation === "readFile");
	const directory = stat.isDirectory();
	if (operation === "stat") return { directory };
	if (operation === "readdir") return directory ? source.readdir(physical) : fail("ENOTDIR");
	if (directory) return fail("EISDIR");
	// A special inode's size does not bound the bytes readable from it.
	assert.ok(
		(stat.type === undefined || stat.type === "file") && isSafeCount(stat.size),
		"file size is not proven by retained content",
	);
	assert.ok(stat.size! <= maxBytes, "input byte budget");
	return source.readFile(physical);
}
type GrepInput = Parameters<ReturnType<typeof createGrepToolDefinition>["execute"]>[1];
type SearchOutput = {
	readonly fd: 1 | 2;
	readonly data: Buffer;
};
type SearchSelection = (
	cwd: string,
	args: readonly string[],
	signal: AbortSignal,
	emit: (output: SearchOutput) => void,
) => Promise<{
	readonly code: number | null;
}>;
type PreparedGrep = {
	readonly root: string;
	readonly cwd: string;
	readonly path: string;
	readonly files: readonly {
		readonly source: string;
		readonly path: string;
	}[];
};
function acceptsGrepMatches(value: unknown): boolean {
	return (
		Array.isArray(value) &&
		value.every(
			(match) =>
				match &&
				typeof match === "object" &&
				Object.keys(match).length === 2 &&
				Number.isSafeInteger(match.line_number) &&
				match.line_number > 0 &&
				match.lines &&
				typeof match.lines === "object" &&
				Object.keys(match.lines).length === 1 &&
				((typeof match.lines.text === "string" && match.lines.bytes === undefined) ||
					(typeof match.lines.bytes === "string" && match.lines.text === undefined)),
		)
	);
}
/** Match content once per explicit regex contract; Pi still owns limits, context and result formatting. */
async function matchCapturedGrep(
	view: ToolFilesystemOperations,
	engine: NonNullable<Awaited<ReturnType<typeof prepareGrepEngine>>>,
	prepared: PreparedGrep,
	query: GrepInput,
	signal: AbortSignal,
	emit: (output: SearchOutput) => void,
) {
	type Match = {
		readonly line_number: number;
		readonly lines: {
			readonly text?: string;
			readonly bytes?: string;
		};
	};
	let pending: {
		readonly source: string;
		readonly inputs: ToolFilesystemOperations;
		readonly signal: AbortSignal;
	}[] = [];
	let queued: Promise<Match[][]> | undefined;
	const enqueue = (source: string, inputs: ToolFilesystemOperations, signal: AbortSignal) => {
		const index = pending.push({ source, inputs, signal }) - 1;
		return (queued ??= new Promise<void>(setImmediate).then(async () => {
			const batch = pending;
			pending = [];
			queued = undefined;
			const scope = new SharedAbortScope();
			const release = batch.filter((item) => !item.signal.aborted).map((item) => scope.acquire(item.signal));
			const signal = scope.signal;
			if (!release.length) scope.abort();
			let root: string | undefined;
			try {
				signal.throwIfAborted();
				root = await fs.mkdtemp(path.join(engine.root, "matches-"));
				const files = new Map<string, Match[]>();
				let inputBytes = 0;
				let outputBytes = 0;
				for (const [index, item] of batch.entries()) {
					signal.throwIfAborted();
					if (item.signal.aborted) continue;
					const bytes = await item.inputs.readFile(item.source);
					assert.ok(
						(inputBytes += bytes.length) <= speculativeConfig().search.maxInputBytes,
						"search input byte budget",
					);
					const file = path.join(root, String(index));
					files.set(file, []);
					await fs.writeFile(file, bytes);
				}
				const output: Buffer[] = [];
				const diagnostic: Buffer[] = [];
				const result = await engine.execute(
					"producer",
					root,
					[
						"--json",
						"--line-number",
						"--color=never",
						"--hidden",
						...(query.ignoreCase ? ["--ignore-case"] : []),
						...(query.literal ? ["--fixed-strings"] : []),
						"--",
						query.pattern,
						root,
					],
					signal,
					({ fd, data }) => {
						assert.ok(
							(outputBytes += data.length) <= speculativeConfig().search.maxInputBytes,
							"search match byte budget",
						);
						(fd === 1 ? output : diagnostic).push(data);
					},
				);
				signal.throwIfAborted();
				assert.ok(result.code === 0 || result.code === 1, Buffer.concat(diagnostic).toString());
				for (const line of Buffer.concat(output).toString("utf8").split("\n")) {
					if (!line) continue;
					const event = JSON.parse(line);
					if (event.type !== "match") continue;
					const matches = files.get(event.data?.path?.text);
					assert.ok(matches, "unowned grep match");
					matches.push({
						line_number: event.data.line_number,
						lines: event.data.lines,
					});
				}
				return batch.map((_, index) => files.get(path.join(root!, String(index)))!);
			} finally {
				for (const withdraw of release) withdraw();
				if (root) {
					assert.equal(path.dirname(root), engine.root);
					await fs.rm(root, { recursive: true, force: true });
				}
			}
		})).then((results) => {
			signal.throwIfAborted();
			return results[index]!;
		});
	};
	if (!prepared.files.length) {
		// Even an empty selection must compile the pattern and preserve native errors.
		await enqueue("", { ...view, readFile: async () => Buffer.alloc(0) }, signal);
		return { code: 1, signal: null };
	}
	const jobs = prepared.files.map((file) => {
		const build = async (inputs: ToolFilesystemOperations, signal: AbortSignal) => {
			const value = await enqueue(file.source, inputs, signal);
			return { value, bytes: serialize(value).byteLength, dispose: () => {} };
		};
		return view.prepare
			? view.prepare(
					engine.matchesBinding,
					JSON.stringify([file.source, query.pattern, !!query.ignoreCase, !!query.literal]),
					build,
					async (value) => value,
					file.source,
					signal,
				)
			: build(view, signal).then((resource) => resource.value);
	});
	try {
		const results = await Promise.all(jobs);
		signal.throwIfAborted();
		for (const [index, matches] of results.entries())
			for (const match of matches)
				emit({
					fd: 1,
					data: Buffer.from(
						JSON.stringify({
							type: "match",
							data: { ...match, path: { text: prepared.files[index]!.path } },
						}) + "\n",
					),
				});
		return {
			code: results.some((matches) => matches.length) ? 0 : 1,
			signal: null,
		};
	} finally {
		await Promise.allSettled(jobs);
	}
}
/** rg selects its own names/configuration over captured inputs; native reads never reach the source tree. */
async function prepareCapturedGrep(
	view: ToolFilesystemOperations,
	cwd: string,
	destination: string,
	query: GrepInput & {
		path: string;
	},
	signal: AbortSignal,
	execute: SearchSelection,
) {
	const { exists, readdir } = view;
	assert.ok(view.stat && exists && readdir, "search input capabilities required");
	const stat = (target: string, fields: "type" | "entry") => view.stat!(target, fields, false);
	const originalCwd = cwd;
	const originalTarget = path.resolve(cwd, query.path);
	const rootStat = await stat(originalTarget, "type");
	const directory = rootStat.isDirectory();
	const volume = path.parse(cwd).root;
	const privateVolume = path.join(destination, "volume");
	const realCwd = (await stat(cwd, "type")).realPath;
	assert.ok(realCwd && rootStat.realPath, "resolved input names require captured evidence");
	// Windows retains the cwd spelling; POSIX reports its physical path. Recreate the alias for rg itself.
	if (process.platform !== "win32") cwd = realCwd;
	const sourceTarget = directory ? rootStat.realPath : originalTarget;
	const map = (source: string) => {
		const relative = relativeFilesystemPath(volume, source);
		assert.ok(relative !== undefined, "input crossed its declared filesystem root");
		return path.join(privateVolume, relative);
	};
	const target = map(sourceTarget);
	const logicalTarget = map(originalTarget);
	const privateCwd = map(originalCwd);
	const marker = "pi-directory-" + randomUUID();
	const files = new Map<string, string | undefined>();
	const loaded = new Set<string>();
	const pending = new Map<string, string>();
	const linkedConfigurations = new Set<string>();
	const directories = new Set<string>();
	const mkdir = async (target: string) => {
		if (directories.has(target)) return;
		await fs.mkdir(target, { recursive: true });
		directories.add(target);
	};
	const linkDirectory = async (source: string, target: string) => {
		if (source === target) return;
		await mkdir(path.dirname(target));
		const existing = await fs.realpath(target).catch((error) => {
			if (error.code !== "ENOENT") throw error;
		});
		if (existing !== (await fs.realpath(source)))
			await fs.symlink(source, target, process.platform === "win32" ? "junction" : "dir");
	};
	let entries = 0;
	let inputBytes = 0;
	const load = async (source: string, target: string) => {
		signal.throwIfAborted();
		const bytes = await view.readFile(source);
		assert.ok(
			(inputBytes += bytes.length) <= speculativeConfig().search.maxInputBytes,
			"search input byte budget",
		);
		await fs.writeFile(target, bytes);
		loaded.add(target);
	};
	const file = async (source: string, target: string, configuration: boolean) => {
		if (!files.has(target)) {
			await mkdir(path.dirname(target));
			await fs.writeFile(target, "", { flag: "wx" });
			files.set(target, source);
		}
		if (configuration) {
			if ((await stat(source, "entry")).type === "symlink") linkedConfigurations.add(target);
			if (!loaded.has(target)) await load(source, target);
		}
	};
	const line = async (source: string) => {
		const bytes = await fs.readFile(source, { signal });
		if (!bytes.length) return undefined;
		return new TextDecoder("utf-8", { fatal: true })
			.decode(bytes.subarray(0, bytes.indexOf(10) < 0 ? bytes.length : bytes.indexOf(10)))
			.replace(/\r$/, "");
	};
	const admitDirectory = async (parent: string, pattern: string) => {
		const control = path.join(parent, ".rgignore");
		if (!files.has(control)) files.set(control, undefined);
		await fs.appendFile(control, `\n!/${pattern}/\n`);
	};
	const rules = async (source: string, target: string) => {
		await mkdir(target);
		for (const name of [".gitignore", ".ignore", ".rgignore"]) {
			if (await exists(path.join(source, name)))
				await file(path.join(source, name), path.join(target, name), true);
		}
		const jj = path.join(source, ".jj");
		const privateJj = path.join(target, ".jj");
		if (await exists(jj)) {
			if ((await stat(jj, "type")).isDirectory()) await mkdir(privateJj);
			else await file(jj, privateJj, false);
			if ((await stat(jj, "entry")).type === "symlink") linkedConfigurations.add(privateJj);
		}
		const git = path.join(source, ".git");
		const privateGit = path.join(target, ".git");
		if (!(await exists(git))) return;
		const entry = await stat(git, "entry");
		const gitDirectory = (await stat(git, "type")).isDirectory();
		if (gitDirectory) {
			await mkdir(privateGit);
			if (await exists(path.join(git, "info/exclude")))
				await file(path.join(git, "info/exclude"), path.join(privateGit, "info/exclude"), true);
			return;
		}
		assert.equal(entry.type, "file", "git pointer entry is not qualified");
		await file(git, privateGit, true);
		const pointer = await line(privateGit);
		if (!pointer?.startsWith("gitdir: ")) return;
		// This follows the pinned ignore engine's pointer resolution, not Git's broader config grammar.
		const gitdir = path.resolve(cwd, pointer.slice(8));
		const common = path.join(gitdir, "commondir");
		const control = path.join(destination, "git-" + randomUUID());
		await fs.mkdir(control);
		await fs.writeFile(privateGit, "gitdir: " + control + "\n");
		if (!(await exists(common))) return;
		await file(common, map(common), true);
		const commonLine = await line(map(common));
		if (commonLine === undefined) return;
		const commonDirectory = path.resolve(commonLine.startsWith(".") ? gitdir : cwd, commonLine);
		await mkdir(map(commonDirectory));
		await fs.writeFile(path.join(control, "commondir"), map(commonDirectory) + "\n");
		const exclude = path.join(commonDirectory, "info/exclude");
		if (await exists(exclude)) await file(exclude, map(exclude), true);
	};
	const walk = async (source: string, target: string) => {
		signal.throwIfAborted();
		assert.ok(entries++ < speculativeConfig().search.maxDirectoryEntries, "metadata entry budget");
		if (path.basename(source) === ".git" && source !== sourceTarget) return;
		const entry = await stat(source, "entry");
		const configuration = [".gitignore", ".ignore", ".rgignore"].includes(path.basename(source));
		if (entry.type === "symlink" && !configuration && source !== sourceTarget) {
			if (process.platform === "win32") await stat(source, "type"); // Native search opens discovered junction metadata.
			return;
		}
		if (entry.type === "special") {
			assert.ok(!configuration && source !== sourceTarget, "special search input");
			return;
		}
		if ((entry.type === "symlink" ? await stat(source, "type") : entry).isDirectory()) {
			await rules(source, target);
			pending.set(target, source);
		} else await file(source, target, configuration);
	};
	const expand = async (target: string) => {
		const source = pending.get(target);
		assert.ok(source !== undefined);
		pending.delete(target);
		for (const name of await readdir(source)) {
			assert.equal(path.basename(name), name);
			assert.ok(name !== "." && name !== ".." && name !== marker);
			await walk(path.join(source, name), path.join(target, name));
		}
	};
	const select = async (targets: readonly string[], flags: readonly string[] = []) => {
		const output: Buffer[] = [];
		const diagnostic: Buffer[] = [];
		let bytes = 0;
		const { code } = await execute(
			privateCwd,
			["--files", "--null", "--hidden", ...flags, "--", ...targets],
			signal,
			({ fd, data }) => {
				assert.ok(
					(bytes += data.length) <= speculativeConfig().search.maxResultBytes,
					"selected name budget",
				);
				(fd === 1 ? output : diagnostic).push(data);
			},
		);
		assert.ok(code === 0 || code === 1, Buffer.concat(diagnostic).toString());
		const raw = Buffer.concat(output);
		const decoded = raw.toString("utf8");
		assert.ok(Buffer.from(decoded).equals(raw), "filename encoding must round-trip without replacement");
		return new Set(
			decoded
				.split("\0")
				.filter(Boolean)
				.map((selected) => {
					const resolved = path.resolve(selected);
					assert.ok(
						relativeFilesystemPath(privateVolume, resolved) !== undefined,
						"rg selected an unowned input",
					);
					return resolved;
				}),
		);
	};
	if (directory)
		for (let source = sourceTarget; ; source = path.dirname(source)) {
			if (source !== sourceTarget) {
				await rules(source, map(source));
				await admitDirectory(map(source), "*");
			}
			if (source === volume) break;
		}
	await mkdir(map(realCwd));
	await linkDirectory(map(realCwd), privateCwd);
	const parents = new Set([target]);
	const deny = path.join(destination, "glob-deny");
	if (query.glob) await fs.writeFile(deny, "*\n", { flag: "wx" });
	const selectedFiles = async (flags: readonly string[] = []) => {
		const ordinary = await select([privateVolume], flags);
		let selected = ordinary;
		if (query.glob) {
			const classify = async (extra: readonly string[]) =>
				new Set(
					[
						...(await select(
							[...parents].map((parent) => path.join(logicalTarget, path.relative(target, parent))),
							["--no-ignore", "--max-depth=2", "--glob", query.glob!, ...extra, ...flags],
						)),
					].map((file) => {
						const relative = relativeFilesystemPath(logicalTarget, file);
						assert.ok(relative !== undefined, "glob classification escaped its query");
						return path.join(target, relative);
					}),
				);
			// Native decisions under neutral/denied defaults distinguish explicit overrides without parsing a glob.
			const allowed = await classify([]);
			selected = await classify(["--ignore-file", deny]);
			for (const file of ordinary) if (allowed.has(file)) selected.add(file);
		}
		return new Set(
			[...selected].filter(
				(file) =>
					relativeFilesystemPath(target, file) !== undefined &&
					!linkedConfigurations.has(file) &&
					(!files.has(file) || files.get(file) !== undefined),
			),
		);
	};
	await walk(sourceTarget, target);
	if (directory) {
		await expand(target);
		await linkDirectory(target, logicalTarget);
	}
	while (pending.size) {
		const frontier = [...pending.keys()];
		for (const parent of frontier) await fs.writeFile(path.join(parent, marker), "", { flag: "wx" });
		const admitted = await selectedFiles(["--glob", "**/" + marker]);
		for (const parent of frontier) {
			await fs.unlink(path.join(parent, marker));
			if (admitted.has(path.join(parent, marker))) {
				if (query.glob) {
					const name = path.basename(parent);
					assert.ok(!/[\r\n]/.test(name), "directory rule name cannot contain a line ending");
					await admitDirectory(path.dirname(parent), name.replace(/[\\*?\[\] ]/g, "\\$&"));
				}
				parents.add(parent);
				await expand(parent);
			} else pending.delete(parent);
		}
	}
	const selected = directory ? await selectedFiles() : new Set([target]);
	const selectedInputs: PreparedGrep["files"][number][] = [];
	for (const [file, source] of files) {
		if (source !== undefined && selected.has(file))
			selectedInputs.push({
				source,
				path: path.join(logicalTarget, path.relative(target, file)),
			});
		else {
			assert.ok(relativeFilesystemPath(privateVolume, file) !== undefined);
			await fs.unlink(file);
		}
	}
	selectedInputs.sort((left, right) => Buffer.compare(Buffer.from(left.path), Buffer.from(right.path)));
	return {
		cwd: privateCwd,
		path: pathToFileURL(logicalTarget).href,
		files: selectedInputs,
		bytes:
			inputBytes +
			[...files.keys(), ...parents, privateCwd, logicalTarget, destination].reduce(
				(sum, name) => sum + Buffer.byteLength(name),
				0,
			) +
			selectedInputs.reduce((sum, file) => sum + Buffer.byteLength(JSON.stringify(file)), 0),
	};
}
