import {
	createReadToolDefinition, createBashToolDefinition, createEditToolDefinition,
	createWriteToolDefinition, createGrepToolDefinition, createFindToolDefinition, createLsToolDefinition,
	getShellConfig, VERSION, type ExtensionContext, type ToolsOptions,
} from "@earendil-works/pi-coding-agent";
import type { ToolInvocation } from "./tool-settlement.ts";
import { withPiProjectionCoverage } from "./pi-read-projection.ts";

// Pi's read resolver/sniffer are private APIs. Other versions retain observation, not assumed authority.
export const PI_OPERATION_TOOLS: Readonly<Record<"resources" | "process", readonly string[]>> = {
	resources: VERSION === "0.84.1" ? ["read", "ls"] : [],
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
	if (PI_OPERATION_TOOLS.resources.includes(tool)) {
		const cwd = options.cwd;
		const autoResizeImages = options.autoResizeImages ?? true;
		const modelSupportsImages = options.modelSupportsImages ?? true;
		const executor = "pi.file-operations.local.v1";
		return {
			executor,
			identity: { executor, cwd, version: VERSION, autoResizeImages, modelSupportsImages },
			resources: async (view, request) => {
				const definitions = createPiToolDefinitions(cwd, {
					read: { autoResizeImages, operations: {
						access: view.access, readFile: view.readFile,
						detectImageMimeType: async (target) => {
							const mime = await import(new URL("./utils/mime.js", import.meta.resolve("@earendil-works/pi-coding-agent")).href);
							return mime.detectSupportedImageMimeType(await view.readFile(target, 4100));
						},
					} },
					ls: { operations: view },
				});
				// The qualified stock read executor consults only model.input, never other context fields.
				const context = { model: { input: modelSupportsImages ? ["image"] : [] } } as ExtensionContext;
				const result = await definitions.get(tool)!.execute(request.callID, request.args as never, request.signal, undefined, context);
				view.assertComplete();
				return { result: withPiProjectionCoverage(tool, request.args, result), isError: false };
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
