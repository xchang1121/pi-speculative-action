import {
	createReadToolDefinition, createBashToolDefinition, createEditToolDefinition,
	createWriteToolDefinition, createGrepToolDefinition, createFindToolDefinition, createLsToolDefinition,
	getShellConfig, VERSION, type ExtensionContext, type ToolsOptions,
} from "@earendil-works/pi-coding-agent";
import type { ToolFilesystemOperations, ToolInvocation, ToolSettlement } from "./tool-settlement.ts";
import { PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { RESOURCE_OBSERVATION_EFFECTS } from "./effect-model.ts";
import { captureResourceVersion } from "./resource-version.ts";
import { relativeFilesystemPath, slash } from "./path-utils.ts";

export const PI_CLOSED_SEARCH_TOOLS: readonly string[] = ["find"];

// Pi's read resolver/sniffer are private APIs. Other versions retain observation, not assumed authority.
export const PI_OPERATION_TOOLS: Readonly<Record<"resources" | "workspace" | "process", readonly string[]>> = {
	resources: VERSION === "0.84.1" ? ["read", "ls"] : [],
	workspace: VERSION === "0.84.1" ? ["write", "edit"] : [],
	process: ["bash"],
};

export type PiToolDefinition = ReturnType<
	| typeof createReadToolDefinition | typeof createBashToolDefinition | typeof createEditToolDefinition
	| typeof createWriteToolDefinition | typeof createGrepToolDefinition | typeof createFindToolDefinition | typeof createLsToolDefinition
>;

/** Stock definitions and their public operation seams; shared by Actor and isolated runners. */
export function createPiToolDefinitions(cwd: string, options: ToolsOptions = {}): Map<string, PiToolDefinition> {
	const definitions = [
		createReadToolDefinition(cwd, options.read),
		createBashToolDefinition(cwd, options.bash),
		createEditToolDefinition(cwd, options.edit),
		createWriteToolDefinition(cwd, options.write),
		createGrepToolDefinition(cwd, options.grep),
		createFindToolDefinition(cwd, options.find),
		createLsToolDefinition(cwd, options.ls),
	];
	return new Map(definitions.map((definition) => [definition.name, definition]));
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
				const writeFile = view.writeFile ?? denied;
				const definitions = createPiToolDefinitions(cwd, {
					read: { autoResizeImages, operations: {
						access: view.access, readFile: view.readFile,
						detectImageMimeType: async (target) => {
							const mime = await import(new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
							return mime.detectSupportedImageMimeType(await view.readFile(target, 4100));
						},
					} },
					ls: { operations: { exists: view.exists ?? denied, stat: view.stat ? (target) => view.stat!(target, "type") : denied, readdir: view.readdir ?? denied } },
					write: { operations: { writeFile, mkdir: view.mkdir ?? denied } },
					edit: { operations: { readFile: view.readFile, access: (target) => view.access(target, true), writeFile } },
				});
				// The qualified stock read executor consults only model.input, never other context fields.
				const context = { model: { input: modelSupportsImages ? ["image"] : [] } } as ExtensionContext;
				const result = await definitions.get(tool)!.execute(request.callID, request.args as never, request.signal, undefined, context);
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

/** Bind captured-input searches without installing anything or reading workspace inputs. */
export async function createClosedSearchProfile(cwd: string) {
	const { CLOSED_SEARCH_PROFILE: profile, loadSearchEngines } = await import(new URL("./closed-search-kernel.mjs", import.meta.url).href) as {
		CLOSED_SEARCH_PROFILE: Readonly<{ id: string; pi: string; limits: { inputBytes: number } }>;
		loadSearchEngines(): Promise<unknown>;
	};
	assert.equal(VERSION, profile.pi, "Closed search requires its qualified Pi version");
	await loadSearchEngines();
	const { ClosedSearchProcessPool } = await import(new URL("./closed-search-process.mjs", import.meta.url).href);
	const pool = new ClosedSearchProcessPool() as {
		request(role: "actor" | "producer", input: unknown, options: { signal?: AbortSignal; onInput: (operation: string, target: string) => Promise<unknown> }): Promise<ToolSettlement>;
		dispose(): Promise<void>;
	};
	const invocations = new Map<string, ToolInvocation>();
	for (const tool of PI_CLOSED_SEARCH_TOOLS) {
		const execute = async (request: Parameters<NonNullable<ToolInvocation["authoritative"]>>[0], view?: ToolFilesystemOperations) => {
			const capture = view ? undefined : await captureResourceVersion(undefined, cwd, PI_ACTION_SEMANTICS, profile.limits.inputBytes);
			try {
				const input = view ?? capture?.view;
				if (!input) throw new Error("closed search input capture unavailable");
				return await pool.request(view ? "producer" : "actor", { kind: tool, root: cwd, args: request.args }, {
					signal: request.signal, onInput: (operation, target) => readClosedSearchInput(input, cwd, operation, target, profile.limits.inputBytes),
				});
			} finally { capture?.release(); }
		};
		invocations.set(tool, Object.freeze({
			executor: profile.id, identity: Object.freeze({ profile, cwd }),
			semantics: Object.freeze({ ...PI_ACTION_SEMANTICS.definition(tool)!, epoch: profile.id,
				effect: "observation", requirements: RESOURCE_OBSERVATION_EFFECTS, resourceScope: "captured_inputs" }),
			authoritative: (request) => execute(request), filesystem: (view, request) => execute(request, view),
		} satisfies ToolInvocation));
	}
	return { profile, pool, invocations: invocations as ReadonlyMap<string, ToolInvocation> };
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
import assert from "node:assert/strict";
import path from "node:path";
