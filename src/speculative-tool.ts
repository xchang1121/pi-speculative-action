import { hash, randomUUID } from "node:crypto";
import path from "node:path";
import { serialize } from "node:v8";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Static, TSchema } from "@earendil-works/pi-ai";
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import { bindResourceOperation } from "./agent-execution-world.ts";
import { RESOURCE_OBSERVATION_EFFECTS, UNRESTRICTED_PROCESS_EFFECTS, WORKSPACE_PATH_MUTATION_EFFECTS } from "./effect-model.ts";
import { relativeFilesystemPath } from "./path-utils.ts";
import { preparationDataBytes, snapshotPreparation, type ToolFilesystemOperations, type ToolFilesystemStat } from "./resource-version.ts";
import { asRecord, cloneSharedData } from "./stable-json.ts";
import type { ToolInvocation } from "./tool-settlement.ts";

declare const stepTypes: unique symbol;
/** An immutable definition, not a cached value. Share this definition between tools to share work. */
export interface SpeculativeStep<Input, Output> {
	readonly id: string;
	readonly version: string;
	readonly [stepTypes]: { readonly input: Input; readonly output: Output };
}
type Steps = Readonly<Record<string, SpeculativeStep<any, any>>>;
type InputOf<Step> = Step extends SpeculativeStep<infer Input, any> ? Input : never;
type OutputOf<Step> = Step extends SpeculativeStep<any, infer Output> ? Output : never;
export type SpeculativeToolEffect = "observation" | "workspace_mutation" | "actor";
export interface SpeculativeFiles {
	readFile(target: string, maxBytes?: number): Promise<Buffer>;
	access(target: string): Promise<void>;
	exists(target: string): Promise<boolean>;
	stat(target: string, fields?: "type" | "entry", includeSize?: boolean): Promise<ToolFilesystemStat>;
	readdir(target: string): Promise<string[]>;
}
export interface SpeculativeWritableFiles extends SpeculativeFiles {
	writeFile(target: string, content: string): Promise<void>;
	mkdir(target: string): Promise<void>;
}
export interface SpeculativeStepContext<Dependencies extends Steps = Steps> {
	readonly cwd: string;
	readonly signal: AbortSignal;
	readonly fs: SpeculativeFiles;
	step<Name extends keyof Dependencies>(name: Name, input: InputOf<Dependencies[Name]>): Promise<OutputOf<Dependencies[Name]>>;
}
export type SpeculativeToolContext<Dependencies extends Steps, Effect extends SpeculativeToolEffect> =
	Omit<SpeculativeStepContext<Dependencies>, "fs"> & { readonly callID: string;
		readonly fs: Effect extends "observation" ? SpeculativeFiles : SpeculativeWritableFiles };

type StepDefinition = { readonly dependencies: Steps; readonly run: (context: SpeculativeStepContext, input: any) => Promise<unknown> | unknown };
const stepDefinitions = new WeakMap<object, StepDefinition>();
const toolInvocations = new WeakMap<Function, (cwd: string) => ToolInvocation>();
const versioned = (id: string, version: string) => {
	if (!id?.trim() || !version?.trim()) throw new Error("Speculative definitions require an id and version");
};
const dependencies = (steps: Steps): Steps => {
	const snapshot = Object.freeze({ ...steps });
	for (const step of Object.values(snapshot)) if (!stepDefinitions.has(step)) throw new Error("Unknown speculative step definition");
	return snapshot;
};
const data = (value: unknown) => {
	const captured = snapshotPreparation(value);
	if (!captured) throw new Error("Speculative step data exceeds the preparation budget");
	return captured;
};
const targetPath = (cwd: string, target: string) => {
	const resolved = path.resolve(cwd, target);
	if (relativeFilesystemPath(cwd, resolved) === undefined) throw new Error("Speculative tool path is outside its workspace");
	return resolved;
};

/** The author supplies a deterministic, effect-free calculation. All data enters through input,
 * declared children or fs reads; external library/configuration changes require a new definition. */
export function defineSpeculativeStep<Input, Output, Dependencies extends Steps = {}>(definition: {
	readonly id: string; readonly version: string; readonly dependencies: Dependencies;
	readonly run: (context: SpeculativeStepContext<Dependencies>, input: Readonly<Input>) => Output | Promise<Output>;
}): SpeculativeStep<Input, Output> {
	versioned(definition.id, definition.version);
	const step = Object.freeze({ id: definition.id, version: definition.version }) as SpeculativeStep<Input, Output>;
	stepDefinitions.set(step, { dependencies: dependencies(definition.dependencies), run: definition.run as StepDefinition["run"] });
	return step;
}

function context(view: ToolFilesystemOperations, cwd: string, signal: AbortSignal, steps: Steps, writable = false): SpeculativeStepContext {
	const required = <Value>(operation: Value | undefined): Value => {
		if (!operation) throw new Error("Filesystem operation is unavailable in this execution world");
		return operation;
	};
	const target = (name: string) => { signal.throwIfAborted(); return targetPath(cwd, name); };
	const fs: SpeculativeFiles = Object.freeze({
		readFile: (name: string, limit?: number) => view.readFile(target(name), limit),
		access: (name: string) => view.access(target(name)),
		exists: async (name: string) => required(view.exists)(target(name)),
		stat: async (name: string, fields?: "type" | "entry", size?: boolean) => required(view.stat)(target(name), fields, size),
		readdir: async (name: string) => required(view.readdir)(target(name)),
		...(writable ? { writeFile: (name: string, content: string) => required(view.writeFile)(target(name), content),
			mkdir: (name: string) => required(view.mkdir)(target(name)) } : {}),
	});
	return Object.freeze({ cwd, signal, fs, step: async (name: string, input: unknown) => {
		signal.throwIfAborted();
		if (!Object.hasOwn(steps, name)) throw new Error(`Undeclared speculative step: ${name}`);
		const step = steps[name]!, definition = stepDefinitions.get(step)!, owned = data(input);
		const key = hash("sha256", serialize([cwd, owned.value])); // Property order remains observable to a data calculation.
		const build = async (inputs: ToolFilesystemOperations, sharedSignal: AbortSignal) => {
			sharedSignal.throwIfAborted();
			const output = await definition.run(context(inputs, cwd, sharedSignal, definition.dependencies), owned.value);
			sharedSignal.throwIfAborted();
			const captured = data(output);
			return { value: captured.value, bytes: preparationDataBytes(captured.data.length), dispose: () => {} };
		};
		const consume = async (value: unknown) => { signal.throwIfAborted(); return cloneSharedData(value); };
		if (view.prepare) return view.prepare(step, key, build, consume, undefined, signal);
		const resource = await build(view, signal);
		try { return await consume(resource.value); } finally { await resource.dispose(); }
	} });
}

/** Register the returned definition with ordinary pi.registerTool. Every file access must use
 * context.fs; steps obey defineSpeculativeStep's contract. Only actor mode may use ambient effects. */
export function defineSpeculativeTool<Parameters extends TSchema, Details, Dependencies extends Steps, Effect extends SpeculativeToolEffect>(
	definition: Omit<ToolDefinition<Parameters, Details>, "execute"> & {
		readonly version: string; readonly effect: Effect; readonly steps: Dependencies;
		/** Pure ordering hints only. Actual reads, not these paths, authorize adoption. */
		readonly resources?: (input: Static<Parameters>) => readonly string[];
		readonly execute: (context: SpeculativeToolContext<Dependencies, Effect>, input: Static<Parameters>) => Promise<AgentToolResult<Details>>;
	},
): ToolDefinition<Parameters, Details> {
	const { version, effect, steps, resources, execute, ...tool } = definition;
	versioned(tool.name, version);
	if (!["observation", "workspace_mutation", "actor"].includes(effect)) throw new Error("Unknown speculative tool effect");
	const selected = dependencies(steps), identity = Object.freeze({ definition: randomUUID(), version });
	const invocation = (directory: string): ToolInvocation => {
		const cwd = path.resolve(directory);
		const filesystem: NonNullable<ToolInvocation["filesystem"]> = async (view, request) => {
			request.signal.throwIfAborted();
			const scoped = { ...context(view, cwd, request.signal, selected, effect !== "observation"), callID: request.callID };
			const result = await execute(scoped as SpeculativeToolContext<Dependencies, Effect>, request.args as Static<Parameters>);
			request.signal.throwIfAborted(); return { result, isError: false };
		};
		return { executor: "pi.speculative-tool", identity, ...(effect === "actor" ? { authoritativeOnly: true as const } : {}),
			semantics: { tool: tool.name, epoch: `pi.speculative-tool:${version}`, effect: effect === "actor" ? "unbounded" : effect,
				requirements: effect === "observation" ? RESOURCE_OBSERVATION_EFFECTS : effect === "workspace_mutation" ? WORKSPACE_PATH_MUTATION_EFFECTS
					: { capabilities: ["invocation.host_function", ...UNRESTRICTED_PROCESS_EFFECTS.capabilities] },
				...(effect === "observation" ? { resourceScope: "captured_inputs" as const } : {}),
				canonicalize: input => asRecord(input) ? { input: asRecord(input)!, resources: resources?.(input as Static<Parameters>).map(name => targetPath(cwd, name)) ?? [] } : undefined },
			...bindResourceOperation(filesystem, cwd, undefined, effect === "observation"),
		};
	};
	const callback: ToolDefinition<Parameters, Details>["execute"] = async (callID, args, signal, _update, actor) =>
		(await invocation(actor.cwd).authoritative!({ args, callID, signal: signal ?? new AbortController().signal })).result as AgentToolResult<Details>;
	toolInvocations.set(callback, invocation);
	return Object.freeze({ ...tool, execute: callback });
}

/** SDK bridge: only the exact registered callback carries the explicit contract. */
export const speculativeToolInvocation = (callback: Function, cwd: string): ToolInvocation | undefined => toolInvocations.get(callback)?.(cwd);
