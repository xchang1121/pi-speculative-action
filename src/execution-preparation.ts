import path from "node:path";
import type { ActionSemanticsRegistry } from "./action-semantics.ts";
import type { AgentPlanSource, AgentRuntime } from "./agent-runtime-types.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { DEFAULTS } from "./common.ts";
import type { ExecutionOperationBinding } from "./execution-world.ts";
import type { PlanAction } from "./plan-proposal.ts";
import { observeResourceChanges, type ResourceVersionToken } from "./resource-version.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { SpeculativeActionSettings } from "./runtime.ts";

type Operation = { readonly key: string; readonly parent: string; readonly binding: ExecutionOperationBinding };
type Request = { readonly revision: number; readonly kind: "observed" | "workspace" | "edit" };
type Command = { readonly parent: string; readonly tool: string; readonly input: Readonly<Record<string, unknown>>;
	readonly schemaHash?: string; request?: Request };
type Preparation = { changes?: Promise<ResourceVersionToken | undefined>; preparing?: Promise<PlanAction[]> };

/** Backend-issued capabilities retain their real parent call and use the existing runtime admission and resource ledger. */
export function createExecutionPreparation(cwd: string, semantics: ActionSemanticsRegistry) {
	const lifetime = new RuntimeLifecycleLane(), operations = new BoundedRecencyMap<string, Operation>(128), commands = new BoundedRecencyMap<string, Command>(4);
	const feedback = new WeakMap<object, { command: Command; request: Request; operation?: Operation }>();
	let state: Preparation | undefined, revision = 0;
	const current = (binding: ExecutionOperationBinding) => binding.available !== false && binding.preparation === "current_workspace" && !binding.fed && !!binding.stale;
	const operationID = (binding: ExecutionOperationBinding) => `${binding.backend}:${binding.identity}`;
	const release = (owner = state) => {
		return owner?.changes && lifetime.track(owner.changes.then(token => token?.release()));
	};
	const configure = (settings: SpeculativeActionSettings) => {
		if (lifetime.sealed || !settings.enabled || settings.reuseEnabled === false || !(settings.prepareObservedOperations ?? DEFAULTS.prepareObservedOperations)) { release(); state = undefined; }
		else state ??= {};
		return state;
	};
	const children = (parent: string) => [...operations.values()].filter(operation => {
		if (operation.binding.available !== false) return operation.parent === parent;
		operations.delete(operation.key); return false;
	});
	const observeWorkspace = async (owner: Preparation) => {
		if (state !== owner || ![...operations.values()].some(operation => current(operation.binding))) return;
		// Cursor notifications schedule work; every execution and adoption still proves its inputs.
		owner.changes = (owner.changes ?? Promise.resolve(undefined)).then(async previous => {
			const next = await observeResourceChanges(cwd).catch(() => undefined), changed = previous?.manager.changesSince(previous);
			await previous?.release();
			if (state !== owner) { await next?.release(); return undefined; }
			if (changed && !changed.uncertain && changed.paths.some(target => !path.relative(cwd, target).split(path.sep).includes(".git")))
				for (const command of commands.values()) command.request = { revision: ++revision, kind: command.request?.kind ?? "workspace" };
			return next;
		});
		await owner.changes;
	};
	const prepare = async (owner: Preparation, schemas: Readonly<Record<string, string>>): Promise<PlanAction[]> => {
		const pending = (owner.preparing ?? Promise.resolve()).catch(() => {}).then(async () => {
			const actions = new Map<string, PlanAction | undefined>();
			for (const command of [...commands.values()].reverse()) {
				const request = command.request;
				if (state !== owner) return [];
				if (!request || command.schemaHash !== schemas[command.tool]) continue;
				const observedOnly = request.kind === "workspace", bindings = children(command.parent).filter(operation => !observedOnly || current(operation.binding));
				const stale = (await Promise.all(bindings.map(async operation => {
					const changed = await operation.binding.stale?.();
					return (observedOnly ? changed === true : changed !== false) ? operation : undefined;
				}))).filter((operation): operation is Operation => !!operation && operation.binding.available !== false);
				if (state !== owner) return [];
				if (command.request !== request || ![...commands.values()].includes(command)) continue;
				if (!stale.length) { command.request = undefined; continue; }
				const selections = stale.filter(({ binding }) => request.kind === "observed" || binding.preparation === "current_workspace" && !binding.fed);
				// Replaying the parent establishes captured inputs in order and covers its independent children.
				if (!selections.length) for (const { binding } of bindings) if (!binding.fed) actions.set(operationID(binding), undefined);
				for (const selected of selections.length ? selections : [undefined]) {
					const operation = selected?.binding, key = operation ? operationID(operation) : command.parent;
					if (operation && actions.has(key)) continue;
					const token = {};
					feedback.set(token, { command, request, operation: selected });
					actions.set(key, { id: `prepare:${request.revision}:${key}`, type: operation ? "operation" : "tool_call", ...(operation ? { operation } : {}),
						tool: command.tool, input: command.input, horizon: 0, producesOperations: true, mode: "observed-work", feedback: token,
						...(operation?.fed ? { background: true } : {}) });
				}
			}
			return [...actions.values()].filter((action): action is PlanAction => !!action);
		});
		owner.preparing = pending;
		try { return await pending; } finally { if (owner.preparing === pending) owner.preparing = undefined; }
	};
	const proposal = async (owner: Preparation, turn: string, schemas: Readonly<Record<string, string>>, signal?: AbortSignal,
		reserve?: (id: string, minimum: number) => number) => {
		await owner.changes;
		const actions = signal?.aborted ? [] : await prepare(owner, schemas);
		if (state !== owner || signal?.aborted || !actions.length) return;
		const id = `prepare:${turn}`, next = ++revision;
		return { id, source: "execution_preparation", revision: reserve?.(id, next) ?? next, actions };
	};
	const source: AgentPlanSource = {
		id: "execution_preparation", kind: "preparation", requestLifetime: "actor_decision",
		enabled: settings => !!configure(settings),
		propose: ({ settings, startInput, data, signal }) => {
			const owner = configure(settings);
			return owner ? lifetime.admit(async () => {
				if (!owner.changes) await observeWorkspace(owner);
				return proposal(owner, startInput.turnID, data.schemaHashes, signal);
			}) : undefined;
		},
	};
	const clear = async () => { await lifetime.drain(); const owner = state; state = undefined; await release(owner); operations.clear(); commands.clear(); };
	return { source, connect: (runtime: AgentRuntime) => runtime.subscribe({
		actor_observed: event => lifetime.admit(async () => {
				const { settings, action, tool, concrete, operations: observed, consumeInput, data, signal } = event;
				const owner = configure(settings);
				const bindings = observed?.filter(binding => binding.available !== false && binding.permissionHash === action?.hash) ?? [];
				if (bindings.length) {
					const schemaHash = action!.schemaHash, parent = action!.hash;
					for (const binding of bindings) { const key = `${parent}:${operationID(binding)}`; operations.set(key, { key, parent, binding }); }
					commands.set(parent, { parent, tool, input: structuredClone(concrete), schemaHash, request: { revision: ++revision, kind: "observed" } });
				}
				if (action && semantics.effect(action) === "workspace_mutation")
					for (const command of commands.values()) command.request = { revision: ++revision, kind: "edit" };
				if (owner) {
					await observeWorkspace(owner);
					await runtime.submitPlan(event.startInput, await proposal(owner, `${consumeInput.turnID}:after`, data.schemaHashes, signal,
						(id, minimum) => runtime.reservePlanRevision(event.sessionID, id, minimum)));
				}
			}),
		prediction_admitted: ({ feedback: token }) => {
			const prepared = feedback.get(token as object);
			if (prepared && prepared.command.request === prepared.request) prepared.command.request = undefined;
		},
		prediction_feedback: ({ feedback: token, settlement }) => {
			const operation = feedback.get(token as object)?.operation;
			if (operation && settlement.observation === "unobserved" && settlement.cause.stage === "execution" &&
				!current(operation.binding) && operations.get(operation.key) === operation) operations.delete(operation.key);
		},
		session_closed: () => lifetime.run(clear),
	}), dispose: () => lifetime.close(clear) };
}
