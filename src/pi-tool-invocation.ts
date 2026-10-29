import { isSafeCount } from "./number-utils.ts";
import { createReadToolDefinition, createBashToolDefinition, createEditToolDefinition,
	createWriteToolDefinition, createGrepToolDefinition, createFindToolDefinition, createLsToolDefinition,
	getAgentDir, getShellConfig, VERSION, type ExtensionContext, type ToolsOptions } from "@earendil-works/pi-coding-agent";
import { toolErrorSettlement, type ToolInvocation, type ToolSettlement } from "./tool-settlement.ts";
import { BASH_TIMEOUT_ACTION_KEY_PROJECTOR, GREP_LITERAL_ACTION_KEY_PROJECTOR, grepLiteralMatches, PI_ACTION_SEMANTICS, type ActionSemanticsDefinition } from "./action-semantics.ts";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { asRecord } from "./stable-json.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "./effect-model.ts";
import { captureResourceVersion, createSerializablePreparationBinding, type ToolFilesystemOperations } from "./resource-version.ts";
import { relativeFilesystemPath, slash } from "./path-utils.ts";
import fs from "node:fs/promises";
import process from "node:process";
import { AsyncLocalStorage } from "node:async_hooks";
import { createRequire, registerHooks } from "node:module";
import type { Worker } from "node:worker_threads";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { captureStableFile } from "./filesystem-evidence.ts";
import { resolveHostExecutable } from "./executable-path.ts";
import { bindResourceOperation } from "./agent-execution-world.ts";
import { SharedAbortScope } from "./runtime-lifecycle.ts";

export const PI_CLOSED_SEARCH_TOOLS: readonly string[] = ["find", "grep"];

// Pi's read resolver/sniffer are private APIs. Other versions retain observation, not assumed authority.
export const PI_OPERATION_TOOLS: Readonly<Record<"resources" | "workspace" | "process", readonly string[]>> = {
	resources: VERSION === "0.84.1" ? ["read", "ls"] : [],
	workspace: VERSION === "0.84.1" ? ["write", "edit"] : [],
	process: ["bash"],
};

const toolFactories = {
	read: createReadToolDefinition, bash: createBashToolDefinition, edit: createEditToolDefinition,
	write: createWriteToolDefinition, grep: createGrepToolDefinition, find: createFindToolDefinition, ls: createLsToolDefinition,
};
type PiToolName = keyof typeof toolFactories;
export type PiToolDefinition = ReturnType<(typeof toolFactories)[PiToolName]>;

/** Stock definitions and their public operation seams; shared by Actor and isolated runners. */
export function createPiToolDefinitions(cwd: string, options: ToolsOptions = {}): Map<string, PiToolDefinition> {
	return new Map((Object.keys(toolFactories) as PiToolName[]).map((tool) => [tool, toolFactories[tool](cwd, options[tool] as never)]));
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
	if ([...PI_OPERATION_TOOLS.resources, ...PI_OPERATION_TOOLS.workspace].includes(tool)) {
		const cwd = options.cwd;
		const autoResizeImages = options.autoResizeImages ?? true;
		const modelSupportsImages = options.modelSupportsImages ?? true;
		// Shared with the ThinkThread runner's binding check.
		const executor = "pi.filesystem.local";
		const requested = tool === "read" ? asRecord(input)?.path : undefined;
		const invocation: ToolInvocation = {
			executor,
			filesystemRoot: path.parse(cwd).root,
			...(typeof requested === "string" ? { filesystemResources: readResourcePaths(requested, cwd) } : {}),
			identity: { executor, cwd, version: VERSION, autoResizeImages, modelSupportsImages },
			filesystem: async (view, request) => {
				const denied = (): never => { throw new Error("Filesystem operation is not authorized by this execution world"); };
				const scoped: ToolsOptions = tool === "read" ? {
					read: { autoResizeImages, operations: {
						access: view.access,
						readFile: view.readFile,
						detectImageMimeType: async (target) => {
							const mime = await import(new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
							return mime.detectSupportedImageMimeType(await view.readFile(target, 4100));
						},
					} },
				} : tool === "ls" ? {
					ls: { operations: { exists: view.exists ?? denied, stat: view.stat ? (target) => view.stat!(target, "type", false) : denied, readdir: view.readdir ?? denied } },
				} : tool === "write" ? {
					write: { operations: { writeFile: view.writeFile ?? denied, mkdir: view.mkdir ?? denied } },
				} : {
					edit: { operations: { readFile: view.readFile, access: (target) => view.access(target, true), writeFile: view.writeFile ?? denied } },
				};
				const factory = tool === "read" ? resourceReadBindings().factory : toolFactories[tool as PiToolName];
				const definition = factory(cwd, scoped[tool as PiToolName] as never);
				// The qualified stock read executor consults only model.input, never other context fields.
				const context = { model: { input: modelSupportsImages ? ["image"] : [] } } as ExtensionContext;
				// Stock cancellation can reject before its internal operation and image worker finish.
				const result = await settleFilesystemOperation<ToolSettlement["result"]>(view, () =>
					definition.execute(request.callID, request.args as never, undefined, undefined, context), request.signal);
				return { result, isError: false };
			},
		};
		return { ...invocation, ...bindResourceOperation(invocation.filesystem!, invocation.filesystemRoot!, undefined,
			PI_ACTION_SEMANTICS.definition(tool)?.effect === "observation") };
	}
	if (!PI_OPERATION_TOOLS.process.includes(tool) || !input || typeof input !== "object" || Array.isArray(input)) return undefined;
	const record = input as Record<string, unknown>;
	if (typeof record.command !== "string") return undefined;
	const shell = getShellConfig(options.shellPath);
	const executor = "pi.bash.local";
	const shared = { cwd: options.cwd, environment: options.environment, shell: shell.shell, shellArgs: [...shell.args], commandTransport: shell.commandTransport ?? "argv" };
	return {
		executor,
		identity: { executor, ...shared, ...(options.shellCommandPrefix ? { commandPrefix: options.shellCommandPrefix } : {}) },
		process: { ...shared, command: options.shellCommandPrefix ? `${options.shellCommandPrefix}\n${record.command}` : record.command,
			...(typeof record.timeout === "number" ? { timeout: record.timeout } : {}) },
	};
}

/** Only a finished command projects: Pi reports a timeout or non-zero exit as an error, which no other timeout reproduces. */
export const PI_BASH_TIMEOUT_PROJECTION_RULE: ActionProjectionRule<ToolSettlement> = {
	...BASH_TIMEOUT_ACTION_KEY_PROJECTOR,
	captureCoverage: (action, output) => action.tool === "bash" && !output.isError ? true : undefined,
	projectOutput: ({ output }) => output.isError ? undefined : output,
};

/** Pi prints each match of a complete literal search once, as `path:line: text`; the matches of a longer literal are a subset of those lines. */
export const PI_GREP_LITERAL_PROJECTION_RULE: ActionProjectionRule<ToolSettlement> = {
	...GREP_LITERAL_ACTION_KEY_PROJECTOR,
	// Pi reports a match limit, byte limit or cut line in details: only a result without any covers a refinement.
	captureCoverage: (action, output) => action.tool === "grep" && !output.isError && output.result.details === undefined ? true : undefined,
	projectOutput: ({ speculative, actor, output }) => {
		const [content, ...rest] = output.result.content, found = String(speculative.input.pattern), sought = String(actor.input.pattern);
		if (output.isError || output.result.details !== undefined || rest.length || content?.type !== "text") return undefined;
		if (content.text === "No matches found") return output;
		const kept: string[] = [], holds = (text: string, literal: string) => grepLiteralMatches(text, literal, actor.input.ignoreCase === true);
		for (const line of content.text.split("\n")) {
			// A path may itself contain `:N: `: every split whose text holds the found literal must agree.
			const verdicts = new Set([...line.matchAll(/:\d+: /g)].map((split) => line.slice(split.index + split[0].length))
				.filter((text) => holds(text, found)).map((text) => holds(text, sought)));
			if (verdicts.size !== 1) return undefined;
			if (verdicts.has(true)) kept.push(line);
		}
		// At its limit the Actor's own search stops early and says so.
		if (kept.length >= Number(actor.input.limit)) return undefined;
		return { result: { ...output.result, content: [{ type: "text", text: kept.length ? kept.join("\n") : "No matches found" }] }, isError: false };
	},
};

/** Pi's bash throws once a completed command exits non-zero, appending this status last (aborts and timeouts append others). */
export function piToolErrorSettlement(tool: string, error: unknown): ToolSettlement {
	const code = tool === "bash" && error instanceof Error ? /(?:^|\n\n)Command exited with code (\d+)$/.exec(error.message)?.[1] : undefined;
	return code === undefined ? toolErrorSettlement(error) : { ...toolErrorSettlement(error), exitCode: Number(code) };
}

type FilesystemCall = { workers: Promise<void>[]; exists?: ToolFilesystemOperations["exists"]; paths?: string[]; failure?: unknown };
const filesystemCalls = new AsyncLocalStorage<FilesystemCall>();
let filesystemExecutions = 0;
const ownFilesystemWorker = (worker: Worker) => {
	filesystemCalls.getStore()?.workers.push(new Promise<void>((resolve) => worker.once("exit", () => resolve())));
};

function readResourcePaths(input: string, cwd: string): readonly string[] {
	const paths: string[] = [];
	filesystemCalls.run({ workers: [], paths }, () => resourceReadBindings().resolveReadPath(input, cwd));
	return Object.freeze([...new Set(paths)]);
}

let boundRead: { factory: typeof createReadToolDefinition; resolveReadPath: (input: string, cwd: string) => string } | undefined;
/** Bind the installed resolver's host dependencies; its selection and stock output code stay unchanged. */
function resourceReadBindings(): NonNullable<typeof boundRead> {
	if (boundRead) return boundRead;
	const original = new URL("./core/tools/read.js", import.meta.resolve("@earendil-works/pi-coding-agent"));
	const paths = new URL("./path-utils.js", original), slot = `piResourceRead_${randomUUID()}`, entry = `pi-resource-read:${slot}`;
	// require() drops URL queries from its cache key, so these instances need distinct pathnames.
	const read = new URL(`./${slot}-read.mjs`, original).href, resolver = new URL(`./${slot}-paths.mjs`, original).href;
	const capability = new URL(`./${slot}-access.mjs`, original).href;
	Object.defineProperty(globalThis, slot, { configurable: true, value: { accessSync: (target: string, mode: number) => {
		const paths = filesystemCalls.getStore()?.paths; assert.ok(paths, "read path enumeration has no owner");
		assert.equal(mode, fs.constants.F_OK); paths.push(target); throw new Error("enumerate potential paths");
	}, access: async (target: string, mode: number) => {
		const call = filesystemCalls.getStore(); assert.ok(call, "read path has no owning invocation");
		if ("failure" in call) throw call.failure;
		let exists: boolean;
		try {
			assert.equal(mode, fs.constants.F_OK);
			if (!call.exists) throw new Error("Filesystem operation is not authorized by this execution world");
			exists = await call.exists(target);
		} catch (error) { call.failure = error; throw error; }
		if (!exists) throw Object.assign(new Error("missing resource"), { code: "ENOENT" });
	} } });
	let hooks: ReturnType<typeof registerHooks> | undefined;
	try {
		hooks = registerHooks({ resolve(specifier, context, next) {
			const url = specifier === entry ? read : specifier === `${entry}:paths` || context.parentURL === read && specifier === "./path-utils.js" ? resolver
				: context.parentURL === resolver && ["node:fs/promises", "node:fs"].includes(specifier) ? capability : undefined;
			return url ? { url, format: "module", shortCircuit: true } : next(specifier, context);
		}, load(url, context, next) {
			if (url === capability) return { format: "module", source: `export { constants } from 'node:fs'; export const { access, accessSync } = globalThis[${JSON.stringify(slot)}];`, shortCircuit: true };
			return next(url === read ? original.href : url === resolver ? paths.href : url, context);
		} });
		const require = createRequire(import.meta.url);
		return boundRead = { factory: require(entry).createReadToolDefinition, resolveReadPath: require(`${entry}:paths`).resolveReadPath };
	} finally { hooks?.deregister(); Reflect.deleteProperty(globalThis, slot); }
}

/** Only the qualified stock filesystem contract runs here; unrelated Actor workers keep their owner. */
async function settleFilesystemOperation<Result>(view: ToolFilesystemOperations, operation: () => Promise<Result>, signal?: AbortSignal): Promise<Result> {
	signal?.throwIfAborted();
	const workers: Promise<void>[] = [];
	const call: FilesystemCall = { workers, exists: view.exists };
	if (filesystemExecutions++ === 0) process.on("worker", ownFilesystemWorker);
	let result: Result;
	try { result = await filesystemCalls.run(call, operation); }
	finally {
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

/** Bind captured-input searches without installing anything or reading workspace inputs. */
export async function createClosedSearchProfile(cwd: string) {
	const home = os.homedir();
	const { CLOSED_SEARCH_PROFILE: profile, loadSearchEngines } = await import(new URL("./closed-search-kernel.mjs", import.meta.url).href) as {
		CLOSED_SEARCH_PROFILE: Readonly<{ id: string; pi: string; limits: { inputBytes: number }; grep: { versions: Readonly<Record<string, string>>; flags: readonly string[] } }>;
		loadSearchEngines(): Promise<unknown>;
	};
	assert.equal(VERSION, profile.pi, "Closed search requires its qualified Pi version");
	const { resolvePath } = await import(new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	const { ClosedSearchProcessPool } = await import(new URL("./closed-search-process.mjs", import.meta.url).href);
	let grep: Awaited<ReturnType<typeof prepareGrepEngine>>;
	const pool = new ClosedSearchProcessPool(() => grep?.release()) as {
		prepare(): void;
		run(role: "actor" | "producer", operation: (worker: {
			request(input: unknown, options?: { signal?: AbortSignal; onInput: (operation: string, target: unknown, signal: AbortSignal, emit: (output: SearchOutput) => void) => Promise<unknown> }): Promise<ToolSettlement>;
			dispose(): Promise<void>;
		}, signal: AbortSignal) => Promise<ToolSettlement>, signal?: AbortSignal): Promise<ToolSettlement>;
		dispose(): Promise<void>;
	};
	try {
		pool.prepare();
		await loadSearchEngines();
		const semantics = new Map(await Promise.all(PI_CLOSED_SEARCH_TOOLS.map(async (tool) => [tool, await createClosedSearchSemantics(tool, cwd, home)] as const)));
		grep = await prepareGrepEngine(home, profile.grep);
		const invocations = new Map<string, ToolInvocation>();
		for (const tool of PI_CLOSED_SEARCH_TOOLS) {
			const engine = tool === "grep" ? grep : undefined;
			if (tool === "grep" && !engine) continue;
			const execute = (request: Parameters<NonNullable<ToolInvocation["authoritative"]>>[0], view?: ToolFilesystemOperations) => pool.run(view ? "producer" : "actor", async (worker, signal) => {
				const capture = view || engine ? undefined : await captureResourceVersion(undefined, cwd, PI_ACTION_SEMANTICS, profile.limits.inputBytes);
				const run = (prepared?: PreparedGrep) => {
					const directory = prepared?.cwd ?? cwd, args = prepared ? { ...request.args as GrepInput, path: prepared.path } : request.args;
					return worker.request({ kind: tool, root: directory, home, args }, {
						signal, onInput: async (operation, target, signal, emit) => {
							if (engine && operation === "process") {
								const command = target as { file: string; args: string[]; options: unknown };
								assert.equal(command.file, "rg"); assert.deepEqual(command.options, { stdio: ["ignore", "pipe", "pipe"] });
								if (prepared) return matchCapturedGrep(view!, engine, prepared, request.args as GrepInput, signal, emit);
								return engine.execute(view ? "producer" : "actor", directory, command.args, signal, emit);
							}
							assert.equal(typeof target, "string");
							if (!engine) return readClosedSearchInput((view ?? capture?.view)!, cwd, operation, target as string, profile.limits.inputBytes);
							assert.ok(operation === "stat" || operation === "readFile", "grep input operation denied");
							if (prepared) assert.ok(relativeFilesystemPath(prepared.root, target as string) !== undefined, "grep input escaped its owned tree");
							if (prepared && operation === "readFile") {
								const file = prepared.files.find(file => file.path === target); assert.ok(file, "unselected grep context input");
								return view!.readFile(file.source);
							}
							return operation === "stat" ? { directory: (await fs.stat(target as string)).isDirectory() } : fs.readFile(target as string, { signal });
						},
					});
				};
				try {
					if (!engine || !view) return await run();
					const query = request.args as GrepInput;
					const target = resolvePath(query.path || ".", cwd, { homeDir: home, normalizeUnicodeSpaces: true, stripAtPrefix: true });
					const build = async (inputs: ToolFilesystemOperations, signal: AbortSignal) => {
						const root = await fs.mkdtemp(path.join(engine.root, "inputs-"));
						const dispose = async () => { assert.equal(path.dirname(root), engine.root); await fs.rm(root, { recursive: true, force: true }); };
						try {
							const prepared = await prepareCapturedGrep(inputs, cwd, root, { ...query, path: target }, signal,
								(directory, args, signal, emit) => engine.execute("selection", directory, args, signal, emit));
							return { value: { root, ...prepared }, bytes: prepared.bytes, dispose };
						} catch (error) { await dispose(); throw error; }
					};
					if (view.prepare) return await view.prepare(engine, JSON.stringify([target, query.glob]), build, run, target, signal);
					const prepared = await build(view, signal);
					try { return await run(prepared.value); } finally { await prepared.dispose(); }
				} finally { await capture?.release(); }
			}, request.signal);
			invocations.set(tool, Object.freeze({
				executor: profile.id, identity: Object.freeze({ profile, cwd, home, ...(engine ? { engine: engine.identity } : {}) }),
				semantics: semantics.get(tool),
				...bindResourceOperation((view, request) => execute(request, view), path.parse(cwd).root, request => execute(request), true),
			} satisfies ToolInvocation));
		}
		return { profile, pool, invocations: invocations as ReadonlyMap<string, ToolInvocation> };
	} catch (error) { await pool.dispose(); throw error; }
}

/** Pin only an already-installed qualified rg; failure leaves the independent find executor available. */
async function prepareGrepEngine(home: string, profile: { readonly versions: Readonly<Record<string, string>>; readonly flags: readonly string[] }) {
	let root: string | undefined;
	try {
		const qualifiedVersion = profile.versions[`${process.platform}:${process.arch}`]; if (!qualifiedVersion) return undefined;
		const existing = path.join(getAgentDir(), "bin", process.platform === "win32" ? "rg.exe" : "rg");
		const binary = await captureStableFile(await resolveHostExecutable(existing, "rg", [], process.platform === "win32" ? ["rg.exe"] : []), 32 * 1024 * 1024, true);
		root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-captured-search-"));
		const owned = root, executable = path.join(owned, process.platform === "win32" ? "rg.exe" : "rg");
		await fs.writeFile(executable, binary.content!, { flag: "wx", mode: 0o500 });
		const environment = Object.freeze({ HOME: home, LC_ALL: "C", ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) });
		const flags = Object.freeze([...profile.flags]), version: Buffer[] = [], signal = AbortSignal.timeout(5000); let bytes = 0;
		const result = await runCapturedSearchProcess(executable, ["--version"], owned, environment, signal,
			({ data }) => { assert.ok((bytes += data.length) <= 8192, "rg version budget"); version.push(data); });
		signal.throwIfAborted(); assert.equal(result.code, 0);
		assert.equal(Buffer.concat(version).toString().match(/^ripgrep (\S+)/)?.[1], qualifiedVersion, "rg version requires qualification");
		const identity = Object.freeze({ sha256: binary.hash, platform: process.platform, arch: process.arch, environment, flags });
		const matchesBinding: object = createSerializablePreparationBinding({ engine: identity, node: process.version,
			loaded: [matchCapturedGrep, runCapturedSearchProcess, prepareGrepEngine].map(fn => Function.prototype.toString.call(fn)) }, acceptsGrepMatches);
		return { root: owned, identity, matchesBinding,
			execute: (stage: "actor" | "selection" | "producer", cwd: string, args: readonly string[], signal: AbortSignal, emit: (output: SearchOutput) => void) =>
				runCapturedSearchProcess(executable, [...flags, ...(stage === "actor" ? [] : ["--no-ignore-parent"]), ...(stage === "producer" ? ["--no-ignore"] : []), ...args], cwd, environment, signal, emit),
			release: async () => { assert.equal(path.dirname(owned), path.resolve(os.tmpdir())); await fs.rm(owned, { recursive: true, force: true }); },
		};
	} catch {
		if (root) { assert.equal(path.dirname(root), path.resolve(os.tmpdir())); await fs.rm(root, { recursive: true, force: true }); }
		return undefined;
	}
}

/** A borrowed native request settles only after process/stream closure, including cancellation or output failure. */
export async function runCapturedSearchProcess(executable: string, args: readonly string[], cwd: string,
	environment: Readonly<Record<string, string>>, signal: AbortSignal, emit: (output: SearchOutput) => void, onSpawn?: () => void) {
	signal.throwIfAborted();
	const child = spawn(executable, args, { cwd, env: environment, stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
	return new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
		let failure: { reason: unknown } | undefined;
		const stop = () => { child.kill("SIGKILL"); };
		signal.addEventListener("abort", stop, { once: true }); if (signal.aborted) stop();
		if (onSpawn) child.once("spawn", onSpawn);
		for (const [fd, output] of [[1, child.stdout], [2, child.stderr]] as const) output.on("data", (data: Buffer) => {
			if (!failure) try { emit({ fd, data }); } catch (error) { failure = { reason: error }; stop(); }
		});
		child.once("error", (error) => { failure ??= { reason: error }; });
		child.once("close", (code, signalValue) => {
			signal.removeEventListener("abort", stop);
			if (failure) reject(failure.reason); else resolve({ code, signal: signalValue });
		});
	});
}

/** Index paths with the bound resolver; replay the exact prepared arguments, never a second normalization. */
export async function createClosedSearchSemantics(tool: string, cwd: string, homeDir: string): Promise<ActionSemanticsDefinition> {
	assert.ok(tool === "find" || tool === "grep");
	const { CLOSED_SEARCH_PROFILE: profile } = await import(new URL("./closed-search-kernel.mjs", import.meta.url).href);
	const { resolvePath } = await import(new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	return Object.freeze({ ...PI_ACTION_SEMANTICS.definition(tool)!, epoch: profile.id,
		effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "captured_inputs",
		canonicalize: (input: unknown) => {
			const record = asRecord(input);
			if (!record || typeof record.pattern !== "string" || (record.path !== undefined && typeof record.path !== "string")) return undefined;
			const target = resolvePath(record.path || ".", cwd, { homeDir, normalizeUnicodeSpaces: true, stripAtPrefix: true });
			const relative = relativeFilesystemPath(cwd, target);
			return relative === undefined ? undefined : { input: record, resources: [slash(relative || ".")] };
		},
	});
}

/** Translate a closed namespace using only captured positive/negative evidence, never ambient host fs. */
export async function readClosedSearchInput(source: ToolFilesystemOperations, root: string, operation: string, target: string, maxBytes: number): Promise<unknown> {
	const fail = (code: string): never => { throw Object.assign(new Error(`${code}: ${target}`), { code }); };
	if (operation === "resolve") {
		const relative = relativeFilesystemPath(root, target);
		return relative === undefined ? fail("ENOENT") : path.posix.join("/workspace", slash(relative));
	}
	assert.ok(source.stat && source.readdir && ["stat", "readdir", "readFile"].includes(operation) && path.posix.isAbsolute(target), "input operation denied");
	const normalized = path.posix.normalize(target);
	if (normalized === "/") return operation === "stat" ? { directory: true } : operation === "readdir" ? ["workspace"] : fail("EISDIR");
	const relative = path.posix.relative("/workspace", normalized);
	if (relative === ".." || relative.startsWith("../")) return fail("ENOENT");
	let physical = root;
	for (const segment of relative ? relative.split("/") : []) {
		if (!(await source.stat(physical, "type", false)).isDirectory()) return fail("ENOTDIR");
		if (!(await source.readdir(physical)).includes(segment)) return fail("ENOENT");
		physical = path.join(physical, segment);
	}
	const stat = await source.stat(physical, "type", operation === "readFile"), directory = stat.isDirectory();
	if (operation === "stat") return { directory };
	if (operation === "readdir") return directory ? source.readdir(physical) : fail("ENOTDIR");
	if (directory) return fail("EISDIR");
	// A special inode's size does not bound the bytes readable from it.
	assert.ok((stat.type === undefined || stat.type === "file") && isSafeCount(stat.size), "file size is not proven by retained content");
	assert.ok(stat.size! <= maxBytes, "input byte budget");
	return source.readFile(physical);
}

type GrepInput = Parameters<ReturnType<typeof createGrepToolDefinition>["execute"]>[1];
type SearchOutput = { readonly fd: 1 | 2; readonly data: Buffer };
type SearchSelection = (cwd: string, args: readonly string[], signal: AbortSignal, emit: (output: SearchOutput) => void) => Promise<{ readonly code: number | null }>;
type PreparedGrep = { readonly root: string; readonly cwd: string; readonly path: string;
	readonly files: readonly { readonly source: string; readonly path: string }[] };

function acceptsGrepMatches(value: unknown): boolean {
	return Array.isArray(value) && value.every(match => match && typeof match === "object" && Object.keys(match).length === 2 &&
		Number.isSafeInteger(match.line_number) && match.line_number > 0 && match.lines && typeof match.lines === "object" &&
		Object.keys(match.lines).length === 1 && (typeof match.lines.text === "string" && match.lines.bytes === undefined ||
			typeof match.lines.bytes === "string" && match.lines.text === undefined));
}

/** Match content once per explicit regex contract; Pi still owns limits, context and result formatting. */
async function matchCapturedGrep(view: ToolFilesystemOperations, engine: NonNullable<Awaited<ReturnType<typeof prepareGrepEngine>>>,
	prepared: PreparedGrep, query: GrepInput, signal: AbortSignal, emit: (output: SearchOutput) => void) {
	type Match = { readonly line_number: number; readonly lines: { readonly text?: string; readonly bytes?: string } };
	let pending: { readonly source: string; readonly inputs: ToolFilesystemOperations; readonly signal: AbortSignal }[] = [];
	let queued: Promise<Match[][]> | undefined;
	const enqueue = (source: string, inputs: ToolFilesystemOperations, signal: AbortSignal) => {
		const index = pending.push({ source, inputs, signal }) - 1;
		return (queued ??= new Promise<void>(setImmediate).then(async () => {
			const batch = pending; pending = []; queued = undefined;
			const scope = new SharedAbortScope(), release = batch.filter(item => !item.signal.aborted).map(item => scope.acquire(item.signal)), signal = scope.signal;
			if (!release.length) scope.abort();
			let root: string | undefined;
			try {
				signal.throwIfAborted(); root = await fs.mkdtemp(path.join(engine.root, "matches-"));
				const files = new Map<string, Match[]>(); let inputBytes = 0, outputBytes = 0;
				for (const [index, item] of batch.entries()) {
					signal.throwIfAborted(); if (item.signal.aborted) continue;
					const bytes = await item.inputs.readFile(item.source);
					assert.ok((inputBytes += bytes.length) <= 8 * 1024 * 1024, "search input byte budget");
					const file = path.join(root, String(index)); files.set(file, []); await fs.writeFile(file, bytes);
				}
				const output: Buffer[] = [], diagnostic: Buffer[] = [];
				const result = await engine.execute("producer", root, ["--json", "--line-number", "--color=never", "--hidden",
					...(query.ignoreCase ? ["--ignore-case"] : []), ...(query.literal ? ["--fixed-strings"] : []), "--", query.pattern, root], signal,
					({ fd, data }) => { assert.ok((outputBytes += data.length) <= 8 * 1024 * 1024, "search match byte budget"); (fd === 1 ? output : diagnostic).push(data); });
				signal.throwIfAborted(); assert.ok(result.code === 0 || result.code === 1, Buffer.concat(diagnostic).toString());
				for (const line of Buffer.concat(output).toString("utf8").split("\n")) {
					if (!line) continue; const event = JSON.parse(line);
					if (event.type !== "match") continue;
					const matches = files.get(event.data?.path?.text); assert.ok(matches, "unowned grep match");
					matches.push({ line_number: event.data.line_number, lines: event.data.lines });
				}
				return batch.map((_, index) => files.get(path.join(root!, String(index)))!);
			}
			finally { for (const withdraw of release) withdraw(); if (root) { assert.equal(path.dirname(root), engine.root); await fs.rm(root, { recursive: true, force: true }); } }
		})).then(results => { signal.throwIfAborted(); return results[index]!; });
	};
	if (!prepared.files.length) {
		// Even an empty selection must compile the pattern and preserve native errors.
		await enqueue("", { ...view, readFile: async () => Buffer.alloc(0) }, signal);
		return { code: 1, signal: null };
	}
	const jobs = prepared.files.map(file => {
		const build = async (inputs: ToolFilesystemOperations, signal: AbortSignal) => {
			const value = await enqueue(file.source, inputs, signal);
			return { value, bytes: JSON.stringify(value).length * 2 + value.length * 64, dispose: () => {} };
		};
		return view.prepare ? view.prepare(engine.matchesBinding, JSON.stringify([file.source, query.pattern, !!query.ignoreCase, !!query.literal]),
			build, async value => value, file.source, signal) : build(view, signal).then(resource => resource.value);
	});
	try {
		const results = await Promise.all(jobs); signal.throwIfAborted();
		for (const [index, matches] of results.entries()) for (const match of matches)
			emit({ fd: 1, data: Buffer.from(JSON.stringify({ type: "match", data: { ...match, path: { text: prepared.files[index]!.path } } }) + "\n") });
		return { code: results.some(matches => matches.length) ? 0 : 1, signal: null };
	} finally { await Promise.allSettled(jobs); }
}

/** rg selects its own names/configuration over captured inputs; native reads never reach the source tree. */
async function prepareCapturedGrep(view: ToolFilesystemOperations, cwd: string, destination: string, query: GrepInput & { path: string },
	signal: AbortSignal, execute: SearchSelection) {
	const { exists, readdir } = view; assert.ok(view.stat && exists && readdir, "search input capabilities required");
	const stat = (target: string, fields: "type" | "entry") => view.stat!(target, fields, false);
	const originalCwd = cwd, originalTarget = path.resolve(cwd, query.path), rootStat = await stat(originalTarget, "type");
	const directory = rootStat.isDirectory(), volume = path.parse(cwd).root, privateVolume = path.join(destination, "volume");
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
	const target = map(sourceTarget), logicalTarget = map(originalTarget), privateCwd = map(originalCwd), marker = "pi-directory-" + randomUUID();
	const files = new Map<string, string | undefined>(), loaded = new Set<string>(), pending = new Map<string, string>(), linkedConfigurations = new Set<string>();
	const directories = new Set<string>();
	const mkdir = async (target: string) => {
		if (directories.has(target)) return;
		await fs.mkdir(target, { recursive: true }); directories.add(target);
	};
	const linkDirectory = async (source: string, target: string) => {
		if (source === target) return;
		await mkdir(path.dirname(target));
		const existing = await fs.realpath(target).catch(error => { if (error.code !== "ENOENT") throw error; });
		if (existing !== await fs.realpath(source)) await fs.symlink(source, target, process.platform === "win32" ? "junction" : "dir");
	};
	let entries = 0, inputBytes = 0;
	const load = async (source: string, target: string) => {
		signal.throwIfAborted();
		const bytes = await view.readFile(source); assert.ok((inputBytes += bytes.length) <= 8 * 1024 * 1024, "search input byte budget");
		await fs.writeFile(target, bytes); loaded.add(target);
	};
	const file = async (source: string, target: string, configuration: boolean) => {
		if (!files.has(target)) {
			await mkdir(path.dirname(target));
			await fs.writeFile(target, "", { flag: "wx" }); files.set(target, source);
		}
		if (configuration) {
			if ((await stat(source, "entry")).type === "symlink") linkedConfigurations.add(target);
			if (!loaded.has(target)) await load(source, target);
		}
	};
	const line = async (source: string) => {
		const bytes = await fs.readFile(source, { signal });
		if (!bytes.length) return undefined;
		return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, bytes.indexOf(10) < 0 ? bytes.length : bytes.indexOf(10))).replace(/\r$/, "");
	};
	const admitDirectory = async (parent: string, pattern: string) => {
		const control = path.join(parent, ".rgignore");
		if (!files.has(control)) files.set(control, undefined);
		await fs.appendFile(control, `\n!/${pattern}/\n`);
	};
	const rules = async (source: string, target: string) => {
		await mkdir(target);
		for (const name of [".gitignore", ".ignore", ".rgignore"]) {
			if (await exists(path.join(source, name))) await file(path.join(source, name), path.join(target, name), true);
		}
		const jj = path.join(source, ".jj"), privateJj = path.join(target, ".jj");
		if (await exists(jj)) {
			if ((await stat(jj, "type")).isDirectory()) await mkdir(privateJj);
			else await file(jj, privateJj, false);
			if ((await stat(jj, "entry")).type === "symlink") linkedConfigurations.add(privateJj);
		}
		const git = path.join(source, ".git"), privateGit = path.join(target, ".git");
		if (!await exists(git)) return;
		const entry = await stat(git, "entry"), gitDirectory = (await stat(git, "type")).isDirectory();
		if (gitDirectory) {
			await mkdir(privateGit);
			if (await exists(path.join(git, "info/exclude"))) await file(path.join(git, "info/exclude"), path.join(privateGit, "info/exclude"), true);
			return;
		}
		assert.equal(entry.type, "file", "git pointer entry is not qualified");
		await file(git, privateGit, true);
		const pointer = await line(privateGit);
		if (!pointer?.startsWith("gitdir: ")) return;
		// This follows the pinned ignore engine's pointer resolution, not Git's broader config grammar.
		const gitdir = path.resolve(cwd, pointer.slice(8)), common = path.join(gitdir, "commondir");
		const control = path.join(destination, "git-" + randomUUID()); await fs.mkdir(control);
		await fs.writeFile(privateGit, "gitdir: " + control + "\n");
		if (!await exists(common)) return;
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
		signal.throwIfAborted(); assert.ok(entries++ < 4096, "metadata entry budget");
		if (path.basename(source) === ".git" && source !== sourceTarget) return;
		const entry = await stat(source, "entry"), configuration = [".gitignore", ".ignore", ".rgignore"].includes(path.basename(source));
		if (entry.type === "symlink" && !configuration && source !== sourceTarget) {
			if (process.platform === "win32") await stat(source, "type"); // Native search opens discovered junction metadata.
			return;
		}
		if (entry.type === "special") { assert.ok(!configuration && source !== sourceTarget, "special search input"); return; }
		if ((entry.type === "symlink" ? await stat(source, "type") : entry).isDirectory()) {
			await rules(source, target); pending.set(target, source);
		} else await file(source, target, configuration);
	};
	const expand = async (target: string) => {
		const source = pending.get(target); assert.ok(source !== undefined); pending.delete(target);
		for (const name of await readdir(source)) {
			assert.equal(path.basename(name), name); assert.ok(name !== "." && name !== ".." && name !== marker);
			await walk(path.join(source, name), path.join(target, name));
		}
	};
	const select = async (targets: readonly string[], flags: readonly string[] = []) => {
		const output: Buffer[] = [], diagnostic: Buffer[] = []; let bytes = 0;
		const { code } = await execute(privateCwd, ["--files", "--null", "--hidden", ...flags, "--", ...targets], signal,
			({ fd, data }) => { assert.ok((bytes += data.length) <= 1024 * 1024, "selected name budget"); (fd === 1 ? output : diagnostic).push(data); });
		assert.ok(code === 0 || code === 1, Buffer.concat(diagnostic).toString());
		const raw = Buffer.concat(output), decoded = raw.toString("utf8");
		assert.ok(Buffer.from(decoded).equals(raw), "filename encoding must round-trip without replacement");
		return new Set(decoded.split("\0").filter(Boolean).map((selected) => {
			const resolved = path.resolve(selected);
			assert.ok(relativeFilesystemPath(privateVolume, resolved) !== undefined, "rg selected an unowned input"); return resolved;
		}));
	};
	if (directory) for (let source = sourceTarget;; source = path.dirname(source)) {
		if (source !== sourceTarget) { await rules(source, map(source)); await admitDirectory(map(source), "*"); }
		if (source === volume) break;
	}
	await mkdir(map(realCwd)); await linkDirectory(map(realCwd), privateCwd);
	const parents = new Set([target]), deny = path.join(destination, "glob-deny");
	if (query.glob) await fs.writeFile(deny, "*\n", { flag: "wx" });
	const selectedFiles = async (flags: readonly string[] = []) => {
		const ordinary = await select([privateVolume], flags);
		let selected = ordinary;
		if (query.glob) {
			const classify = async (extra: readonly string[]) => new Set([...await select([...parents].map((parent) => path.join(logicalTarget, path.relative(target, parent))),
				["--no-ignore", "--max-depth=2", "--glob", query.glob!, ...extra, ...flags])].map((file) => {
				const relative = relativeFilesystemPath(logicalTarget, file); assert.ok(relative !== undefined, "glob classification escaped its query");
				return path.join(target, relative);
			}));
			// Native decisions under neutral/denied defaults distinguish explicit overrides without parsing a glob.
			const allowed = await classify([]); selected = await classify(["--ignore-file", deny]);
			for (const file of ordinary) if (allowed.has(file)) selected.add(file);
		}
		return new Set([...selected].filter((file) => relativeFilesystemPath(target, file) !== undefined && !linkedConfigurations.has(file) && (!files.has(file) || files.get(file) !== undefined)));
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
					const name = path.basename(parent); assert.ok(!/[\r\n]/.test(name), "directory rule name cannot contain a line ending");
					await admitDirectory(path.dirname(parent), name.replace(/[\\*?\[\] ]/g, "\\$&"));
				}
				parents.add(parent); await expand(parent);
			} else pending.delete(parent);
		}
	}
	const selected = directory ? await selectedFiles() : new Set([target]);
	const selectedInputs: PreparedGrep["files"][number][] = [];
	for (const [file, source] of files) {
		if (source !== undefined && selected.has(file)) selectedInputs.push({ source, path: path.join(logicalTarget, path.relative(target, file)) });
		else { assert.ok(relativeFilesystemPath(privateVolume, file) !== undefined); await fs.unlink(file); }
	}
	selectedInputs.sort((left, right) => Buffer.compare(Buffer.from(left.path), Buffer.from(right.path)));
	return { cwd: privateCwd, path: pathToFileURL(logicalTarget).href,
		files: selectedInputs,
		bytes: inputBytes + [...files.keys(), ...parents, privateCwd, logicalTarget, destination].reduce((sum, name) => sum + name.length * 2 + 128, 0) +
			selectedInputs.reduce((sum, file) => sum + (file.source.length + file.path.length) * 2 + 64, 0) };
}
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
