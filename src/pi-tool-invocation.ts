import {
	createReadToolDefinition, createBashToolDefinition, createEditToolDefinition,
	createWriteToolDefinition, createGrepToolDefinition, createFindToolDefinition, createLsToolDefinition,
	getAgentDir, getShellConfig, VERSION, type ExtensionContext, type ToolsOptions,
} from "@earendil-works/pi-coding-agent";
import type { ToolFilesystemOperations, ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import { asRecord, PI_ACTION_SEMANTICS, type ActionSemanticsDefinition } from "./action-semantics.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "./effect-model.ts";
import { captureResourceVersion } from "./resource-version.ts";
import { relativeFilesystemPath, slash } from "./path-utils.ts";
import fs from "node:fs/promises";
import process from "node:process";
import { AsyncLocalStorage } from "node:async_hooks";
import type { Worker } from "node:worker_threads";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { captureStableFile } from "./filesystem-evidence.ts";
import { resolveHostExecutable } from "./executable-path.ts";

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

function createPiToolDefinition(tool: PiToolName, cwd: string, options: ToolsOptions): PiToolDefinition {
	return toolFactories[tool](cwd, options[tool] as never);
}

/** Stock definitions and their public operation seams; shared by Actor and isolated runners. */
export function createPiToolDefinitions(cwd: string, options: ToolsOptions = {}): Map<string, PiToolDefinition> {
	return new Map((Object.keys(toolFactories) as PiToolName[]).map((tool) => [tool, createPiToolDefinition(tool, cwd, options)]));
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
		const executor = "pi.filesystem.local.v2";
		return {
			executor,
			identity: { executor, cwd, version: VERSION, autoResizeImages, modelSupportsImages },
			filesystem: async (view, request) => {
				const denied = (): never => { throw new Error("Filesystem operation is not authorized by this execution world"); };
				const scoped: ToolsOptions = tool === "read" ? {
					read: { autoResizeImages, operations: {
						access: view.access, readFile: view.readFile,
						detectImageMimeType: async (target) => {
							const mime = await import(new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
							return mime.detectSupportedImageMimeType(await view.readFile(target, 4100));
						},
					} },
				} : tool === "ls" ? {
					ls: { operations: { exists: view.exists ?? denied, stat: view.stat ? (target) => view.stat!(target, "type") : denied, readdir: view.readdir ?? denied } },
				} : tool === "write" ? {
					write: { operations: { writeFile: view.writeFile ?? denied, mkdir: view.mkdir ?? denied } },
				} : {
					edit: { operations: { readFile: view.readFile, access: (target) => view.access(target, true), writeFile: view.writeFile ?? denied } },
				};
				const definition = createPiToolDefinition(tool as PiToolName, cwd, scoped);
				// The qualified stock read executor consults only model.input, never other context fields.
				const context = { model: { input: modelSupportsImages ? ["image"] : [] } } as ExtensionContext;
				// Stock cancellation can reject before its internal operation and image worker finish.
				const result = await settleFilesystemOperation<ToolSettlement["result"]>(() =>
					definition.execute(request.callID, request.args as never, undefined, undefined, context), request.signal);
				return { result, isError: false };
			},
		};
	}
	if (!PI_OPERATION_TOOLS.process.includes(tool) || !input || typeof input !== "object" || Array.isArray(input)) return undefined;
	const record = input as Record<string, unknown>;
	if (typeof record.command !== "string") return undefined;
	const shell = getShellConfig(options.shellPath);
	const executor = "pi.bash.local.v2";
	const commandTransport = shell.commandTransport ?? "argv";
	return {
		executor,
		identity: {
			executor,
			cwd: options.cwd,
			environment: options.environment,
			shell: shell.shell,
			shellArgs: [...shell.args],
			commandTransport,
			...(options.shellCommandPrefix ? { commandPrefix: options.shellCommandPrefix } : {}),
		},
		process: {
			command: options.shellCommandPrefix ? `${options.shellCommandPrefix}\n${record.command}` : record.command,
			cwd: options.cwd,
			environment: options.environment,
			shell: shell.shell,
			shellArgs: [...shell.args],
			commandTransport,
			...(typeof record.timeout === "number" ? { timeout: record.timeout } : {}),
		},
	};
}

const filesystemWorkers = new AsyncLocalStorage<Promise<void>[]>();
let filesystemExecutions = 0;
const ownFilesystemWorker = (worker: Worker) => {
	filesystemWorkers.getStore()?.push(new Promise<void>((resolve) => worker.once("exit", () => resolve())));
};

/** Only the qualified stock filesystem contract runs here; unrelated Actor workers keep their owner. */
async function settleFilesystemOperation<Result>(operation: () => Promise<Result>, signal?: AbortSignal): Promise<Result> {
	signal?.throwIfAborted();
	const workers: Promise<void>[] = [];
	if (filesystemExecutions++ === 0) process.on("worker", ownFilesystemWorker);
	let result: Result;
	try { result = await filesystemWorkers.run(workers, operation); }
	finally {
		// Node queues its worker-created event on nextTick, even if the tool already resolved.
		await new Promise<void>((resolve) => process.nextTick(resolve));
		while (workers.length) await Promise.all(workers.splice(0));
		if (--filesystemExecutions === 0) process.off("worker", ownFilesystemWorker);
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
	await loadSearchEngines();
	const { resolvePath } = await import(new URL("./utils/paths.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
	const { ClosedSearchProcessPool } = await import(new URL("./closed-search-process.mjs", import.meta.url).href);
	const semantics = new Map(await Promise.all(PI_CLOSED_SEARCH_TOOLS.map(async (tool) => [tool, await createClosedSearchSemantics(tool, cwd, home)] as const)));
	const grep = await prepareGrepEngine(home, profile.grep);
	const pool = new ClosedSearchProcessPool(grep?.release) as {
		run(role: "actor" | "producer", operation: (worker: {
			request(input: unknown, options?: { signal?: AbortSignal; onInput: (operation: string, target: unknown, signal: AbortSignal, emit: (output: SearchOutput) => void) => Promise<unknown> }): Promise<ToolSettlement>;
			dispose(): Promise<void>;
		}, signal: AbortSignal) => Promise<ToolSettlement>, signal?: AbortSignal): Promise<ToolSettlement>;
		dispose(): Promise<void>;
	};
	const invocations = new Map<string, ToolInvocation>();
	for (const tool of PI_CLOSED_SEARCH_TOOLS) {
		const engine = tool === "grep" ? grep : undefined;
		if (tool === "grep" && !engine) continue;
		const execute = (request: Parameters<NonNullable<ToolInvocation["authoritative"]>>[0], view?: ToolFilesystemOperations) => pool.run(view ? "producer" : "actor", async (worker, signal) => {
			const capture = view || engine ? undefined : await captureResourceVersion(undefined, cwd, PI_ACTION_SEMANTICS, profile.limits.inputBytes);
			let privateRoot: string | undefined, directory = cwd, args = request.args;
			try {
				if (engine && view) {
					privateRoot = await fs.mkdtemp(path.join(engine.root, "inputs-"));
					const query = request.args as GrepInput;
					const prepared = await prepareCapturedGrep(view, cwd, privateRoot, { ...query,
						path: resolvePath(query.path || ".", cwd, { homeDir: home, normalizeUnicodeSpaces: true, stripAtPrefix: true }) }, signal,
						(directory, args, signal, emit) => engine.execute("selection", directory, args, signal, emit));
					directory = prepared.cwd; args = prepared.args;
				}
				return await worker.request({ kind: tool, root: directory, home, args }, {
					signal, onInput: async (operation, target, signal, emit) => {
						if (engine && operation === "process") {
							const command = target as { file: string; args: string[]; options: unknown };
							assert.equal(command.file, "rg"); assert.deepEqual(command.options, { stdio: ["ignore", "pipe", "pipe"] });
							return engine.execute(view ? "producer" : "actor", directory, command.args, signal, emit);
						}
						assert.equal(typeof target, "string");
						if (!engine) return readClosedSearchInput((view ?? capture?.view)!, cwd, operation, target as string, profile.limits.inputBytes);
						assert.ok(operation === "stat" || operation === "readFile", "grep input operation denied");
						if (privateRoot) assert.ok(relativeFilesystemPath(privateRoot, target as string) !== undefined, "grep input escaped its owned tree");
						return operation === "stat" ? { directory: (await fs.stat(target as string)).isDirectory() } : fs.readFile(target as string, { signal });
					},
				});
			} finally {
				await capture?.release();
				if (privateRoot) { assert.equal(path.dirname(privateRoot), engine!.root); await fs.rm(privateRoot, { recursive: true, force: true }); }
			}
		}, request.signal);
		invocations.set(tool, Object.freeze({
			executor: profile.id, identity: Object.freeze({ profile, cwd, home, ...(engine ? { engine: engine.identity } : {}) }),
			...(engine ? { filesystemRoot: path.parse(cwd).root } : {}),
			semantics: semantics.get(tool),
			authoritative: (request) => execute(request), filesystem: (view, request) => execute(request, view),
		} satisfies ToolInvocation));
	}
	return { profile, pool, invocations: invocations as ReadonlyMap<string, ToolInvocation> };
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
		return { root: owned, identity: Object.freeze({ sha256: binary.hash, platform: process.platform, arch: process.arch, environment, flags }),
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
		if (!(await source.stat(physical, "type")).isDirectory()) return fail("ENOTDIR");
		if (!(await source.readdir(physical)).includes(segment)) return fail("ENOENT");
		physical = path.join(physical, segment);
	}
	const stat = await source.stat(physical, operation === "readFile" ? undefined : "type"), directory = stat.isDirectory();
	if (operation === "stat") return { directory };
	if (operation === "readdir") return directory ? source.readdir(physical) : fail("ENOTDIR");
	if (directory) return fail("EISDIR");
	assert.ok(Number.isSafeInteger(stat.size) && stat.size! >= 0, "file size is not proven by retained content");
	assert.ok(stat.size! <= maxBytes, "input byte budget");
	return source.readFile(physical);
}

type GrepInput = Parameters<ReturnType<typeof createGrepToolDefinition>["execute"]>[1];
type SearchOutput = { readonly fd: 1 | 2; readonly data: Buffer };
type SearchSelection = (cwd: string, args: readonly string[], signal: AbortSignal, emit: (output: SearchOutput) => void) => Promise<{ readonly code: number | null }>;

/** rg selects its own names/configuration over captured inputs; native reads never reach the source tree. */
async function prepareCapturedGrep(view: ToolFilesystemOperations, cwd: string, destination: string, query: GrepInput & { path: string },
	signal: AbortSignal, execute: SearchSelection) {
	const { stat, exists, readdir } = view; assert.ok(stat && exists && readdir, "search input capabilities required");
	const originalCwd = cwd, originalTarget = path.resolve(cwd, query.path), rootStat = await stat(originalTarget, "type");
	const directory = rootStat.isDirectory(), volume = path.parse(cwd).root, privateVolume = path.join(destination, "volume");
	const realCwd = (await stat(cwd, "type")).realPath;
	assert.ok(realCwd && rootStat.realPath, "resolved input names require captured evidence"); cwd = realCwd;
	// A changed cwd spelling can change caller-glob matching even when contents are equal.
	if (directory && query.glob) assert.equal(cwd, originalCwd, "glob cwd alias namespace is not qualified");
	const sourceTarget = directory ? rootStat.realPath : originalTarget;
	const map = (source: string) => {
		const relative = relativeFilesystemPath(volume, source);
		assert.ok(relative !== undefined, "input crossed its declared filesystem root");
		return path.join(privateVolume, relative);
	};
	const target = map(sourceTarget), logicalTarget = map(originalTarget), privateCwd = map(cwd), marker = "pi-directory-" + randomUUID();
	const files = new Map<string, string | undefined>(), loaded = new Set<string>(), pending = new Map<string, string>(), linkedConfigurations = new Set<string>();
	let entries = 0, inputBytes = 0;
	const load = async (source: string, target: string) => {
		signal.throwIfAborted();
		const bytes = await view.readFile(source); assert.ok((inputBytes += bytes.length) <= 8 * 1024 * 1024, "search input byte budget");
		await fs.writeFile(target, bytes); loaded.add(target);
	};
	const file = async (source: string, target: string, configuration: boolean) => {
		if (!files.has(target)) {
			await fs.mkdir(path.dirname(target), { recursive: true });
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
		await fs.mkdir(target, { recursive: true });
		for (const name of [".gitignore", ".ignore", ".rgignore"]) {
			if (await exists(path.join(source, name))) await file(path.join(source, name), path.join(target, name), true);
		}
		const jj = path.join(source, ".jj"), privateJj = path.join(target, ".jj");
		if (await exists(jj)) {
			if ((await stat(jj, "type")).isDirectory()) await fs.mkdir(privateJj, { recursive: true });
			else await file(jj, privateJj, false);
			if ((await stat(jj, "entry")).type === "symlink") linkedConfigurations.add(privateJj);
		}
		const git = path.join(source, ".git"), privateGit = path.join(target, ".git");
		if (!await exists(git)) return;
		const entry = await stat(git, "entry"), gitDirectory = (await stat(git, "type")).isDirectory();
		if (gitDirectory) {
			await fs.mkdir(privateGit, { recursive: true });
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
		await fs.mkdir(map(commonDirectory), { recursive: true });
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
	await fs.mkdir(privateCwd, { recursive: true });
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
		if (logicalTarget !== target) {
			await fs.mkdir(path.dirname(logicalTarget), { recursive: true });
			await fs.symlink(target, logicalTarget, process.platform === "win32" ? "junction" : "dir");
		}
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
	for (const [target, source] of files) {
		if (source !== undefined && selected.has(target)) await load(source, target); // Restore raw configuration after private-only transport.
		else { assert.ok(relativeFilesystemPath(privateVolume, target) !== undefined); await fs.unlink(target); }
	}
	return { cwd: privateCwd, args: { ...query, path: pathToFileURL(logicalTarget).href } };
}
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
