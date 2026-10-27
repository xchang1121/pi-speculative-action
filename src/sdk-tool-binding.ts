import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { AgentMessage, AgentTool } from "@earendil-works/pi-agent-core";
import { AgentSession, VERSION, type ExtensionContext, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import { stableValueHash } from "./stable-value-hash.ts";
import { asRecord } from "./stable-json.ts";
import { UNRESTRICTED_PROCESS_EFFECTS } from "./effect-model.ts";
import type { ToolInvocation } from "./tool-settlement.ts";

type Execute = (tool: AgentTool, args: Parameters<AgentTool["execute"]>) => ReturnType<AgentTool["execute"]>;
type Turn = { readonly tools: readonly AgentTool[]; execute?: Execute; preparing?: Promise<void>; closed?: boolean };
type Run = { readonly session: AgentSession; turn?: Turn };
type Definition = { readonly definition: ToolDefinition; readonly execute: ToolDefinition["execute"]; readonly schema: string; readonly stock: boolean };
type Registered = Definition & { readonly executeTool: AgentTool["execute"]; readonly prepare: AgentTool["prepareArguments"] };

/** A shared broker survives extension reloads; only attached Agent instances get execution wrappers. */
const brokerKey = Symbol.for("pi-speculative-action.sdk-tools");
const prototype = AgentSession.prototype as AgentSession & { [brokerKey]?: ReturnType<typeof createBroker> };
const broker = prototype[brokerKey] ??= createBroker();
function createBroker() {
	const sessions = new AsyncLocalStorage<AgentSession>(), runs = new AsyncLocalStorage<Run>();
	const definitions = new WeakMap<ToolDefinition, Definition>(), tools = new WeakMap<AgentTool, Registered>();
	for (const name of ["bindExtensions", "reload"] as const) {
		const original = prototype[name];
		prototype[name] = function (this: AgentSession, ...args: never[]) { return sessions.run(this, () => Reflect.apply(original, this, args)); };
	}
	const activate = prototype.setActiveToolsByName;
	prototype.setActiveToolsByName = function (names) {
		activate.call(this, names);
		const sources = new Map(this.getAllTools().map(tool => [tool.name, tool.sourceInfo.source]));
		for (const tool of this.agent.state.tools) {
			const definition = this.getToolDefinition(tool.name);
			if (!definition) continue;
			let binding = definitions.get(definition);
			if (!binding) {
				// In the qualified SDK, baseToolsOverride definitions discard renderers; custom tools have their own source.
				binding = { definition, execute: definition.execute, schema: stableValueHash(definition.parameters),
					stock: VERSION === "0.84.1-speculative.1" && sources.get(tool.name) === "builtin" && !!definition.renderCall && !!definition.renderResult };
				definitions.set(definition, binding);
			}
			tools.set(tool, { ...binding, executeTool: tool.execute, prepare: tool.prepareArguments });
		}
	};
	return { sessions, runs, tools };
}

/** Keep opaque callbacks available for child-operation learning, never for executing a host function ahead. */
const callbackTools = new WeakMap<AgentTool, { readonly execute: AgentTool["execute"]; readonly tool: AgentTool; readonly invocation: ToolInvocation }>();
export function sdkCallbackTool(tool: AgentTool): AgentTool {
	let entry = callbackTools.get(tool);
	if (!entry || entry.execute !== tool.execute) {
		// The SDK already prepared Actor arguments. An arbitrary preparation callback cannot run ahead on the host.
		entry = { execute: tool.execute, tool: { ...tool, prepareArguments: undefined, execute: tool.execute.bind(tool) },
			invocation: { executor: "pi.sdk.callback", identity: { callback: randomUUID() }, semantics: {
				tool: tool.name, epoch: "pi.sdk.callback:1", effect: "unbounded",
				requirements: { capabilities: ["invocation.host_function", ...UNRESTRICTED_PROCESS_EFFECTS.capabilities] },
				canonicalize: input => { const record = asRecord(input); return record && { input: record, resources: [] }; },
			} } };
		callbackTools.set(tool, entry);
		callbackTools.set(entry.tool, entry);
	}
	return entry.tool;
}
export function sdkToolInvocation(tool: AgentTool): ToolInvocation {
	return callbackTools.get(tool)?.invocation ?? sdkToolInvocation(sdkCallbackTool(tool));
}

export function isStockSdkTool(tool: AgentTool): boolean {
	const binding = broker.tools.get(tool), definition = binding?.definition;
	return !!binding?.stock && definition?.execute === binding.execute && tool.execute === binding.executeTool &&
		tool.prepareArguments === binding.prepare && definition.prepareArguments === binding.prepare &&
		stableValueHash(tool.parameters) === binding.schema && stableValueHash(definition.parameters) === binding.schema;
}

export function currentSdkTurn(context: ExtensionContext): Turn | undefined {
	const run = broker.runs.getStore();
	return run?.session.sessionManager === context.sessionManager && run.session.agent.signal === context.signal ? run.turn : undefined;
}

export function attachSdkTools(context: ExtensionContext, initialize?: (messages: AgentMessage[]) => Promise<void>) {
	const session = broker.sessions.getStore();
	if (!session || session.sessionManager !== context.sessionManager) return { tools: (): readonly AgentTool[] => [], dispose: () => {} };
	const agent = session.agent, restores: (() => void)[] = [];
	let attached = true;
	for (const name of ["prompt", "continue", "resumeTools"] as const) {
		const descriptor = Object.getOwnPropertyDescriptor(agent, name), original = agent[name];
		const wrapped = async function (this: typeof agent, ...args: never[]) {
			if (!attached || this !== agent || agent.signal) return Reflect.apply(original, this, args);
			const state = agent.state, property = Object.getOwnPropertyDescriptor(state, "tools");
			if (!property?.configurable || !property.get || !property.set) return Reflect.apply(original, this, args);
			const run: Run = { session };
			const wrap = (tools: AgentTool[]) => {
				if (run.turn) run.turn.closed = true;
				const turn: Turn = { tools: tools.slice() }; run.turn = turn;
				return tools.map(tool => ({ ...tool, prepareArguments: tool.prepareArguments?.bind(tool),
					execute: async (...args: Parameters<AgentTool["execute"]>) => {
						if (attached && !turn.closed) {
							if (!turn.execute && initialize) turn.preparing ??= initialize(session.messages);
							await turn.preparing;
						}
						return attached && !turn.closed && turn.execute ? turn.execute(tool, args) : tool.execute(...args);
					} }));
			};
			return broker.runs.run(run, async () => {
				const next = agent.prepareNextTurnWithContext, fallback = agent.prepareNextTurn;
				const prepare: NonNullable<typeof next> = async (turn, signal) => {
					const update = await (next ? next.call(agent, turn, signal) : fallback?.call(agent, signal));
					const context = update?.context ?? turn.context;
					return { ...update, context: { ...context, tools: wrap(context.tools ?? []) } };
				};
				agent.prepareNextTurnWithContext = prepare;
				// SDK assignment copies arrays. A transient accessor preserves the original (possibly frozen) array and every tool identity.
				const snapshot = wrap(state.tools); let changed = false;
				const get = () => changed ? property.get!.call(state) : snapshot;
				Object.defineProperty(state, "tools", { ...property, get, set: tools => { property.set!.call(state, tools); changed = true; } });
				try {
					let pending;
					try { pending = Reflect.apply(original, agent, args); }
					finally { if (Object.getOwnPropertyDescriptor(state, "tools")?.get === get) Object.defineProperty(state, "tools", property); }
					return await pending;
				} finally {
					if (run.turn) run.turn.closed = true;
					if (agent.prepareNextTurnWithContext === prepare) agent.prepareNextTurnWithContext = next;
				}
			});
		};
		Object.defineProperty(agent, name, { configurable: true, writable: true, value: wrapped });
		restores.push(() => { if (agent[name] === wrapped) { if (descriptor) Object.defineProperty(agent, name, descriptor); else delete (agent as Partial<typeof agent>)[name]; } });
	}
	return { tools: () => agent.state.tools, dispose: () => { attached = false; for (const restore of restores) restore(); } };
}
