import { isSafeCount, nonNegativeFinite } from "./number-utils.ts";
import { closeSync, existsSync } from "node:fs";
import { AsyncLocalStorage } from "node:async_hooks";
import { withFilesystemReadView, type FilesystemReadView } from "./filesystem-evidence.ts";
import path from "node:path";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ActionProjectionRule } from "./action-key-projection.ts";
import { type ActionSemanticsRegistry } from "./action-semantics.ts";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { agentBatchKey, type AgentPlanSource, type AgentStartInput, type AgentRuntime, type AgentRuntimeEvents } from "./agent-runtime-types.ts";
import { acquirePatternAwareStore, type OutputLocation, type PatternAwareCandidate,
	type PatternAwareEventInput, type PatternAwareSettings, type PatternAwareStore, type PatternAwareStoreLease,
	patternAwareActionSemantics, patternAwareAnalyzerKey, patternAwareSettings, failureClass,
	projectPatternAwareObservation } from "./pattern-aware.ts";
import type { PlanAction } from "./plan-proposal.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";
import type { SpeculativeActionSettings, SpeculativeCandidate } from "./runtime.ts";
import { stableValueHash } from "./stable-value-hash.ts";
import type { ToolSettlement } from "./tool-settlement.ts";

type PatternPlanFeedback = Pick<PatternAwareCandidate, "continuation" | "supportingPatternIDs"> & { readonly store: PatternAwareStore };
type CarriedPrediction = { readonly signature: string; readonly pending: Set<PatternPlanFeedback>; abandoned: boolean };
type PatternTurn = { readonly events: Map<number, PatternAwareEventInput>; revision: number; observationRevision: number };
type FailedCommand = { readonly id: string; readonly tool: string; readonly input: Readonly<Record<string, unknown>>; readonly schemaHash?: string;
	readonly paths: readonly string[]; requested: boolean };

const predictionReads = new AsyncLocalStorage<FilesystemReadView>();
const predict = <Value>(operation: () => Value): Promise<Value> => process.platform === "linux"
	? withFilesystemReadView(undefined, async view => predictionReads.run(view, operation)) : Promise.resolve(operation());
const predictionFileExists = (target: string): boolean => {
	if (process.platform !== "linux") return existsSync(target);
	const view = predictionReads.getStore();
	if (!view) throw new Error("pattern_prediction_read_owner_missing");
	try { closeSync(view.open(target, 0x200000)); return true; } catch { return false; }
};

export interface PatternPlanSourceController {
	readonly source: AgentPlanSource;
	readonly connect: (runtime: AgentRuntime) => () => void;
	/** The calls PatternAware expects next, without proposing them. */
	readonly hints: (input: { readonly sessionID: string; readonly schemaHashes: Readonly<Record<string, string>>; readonly settings: SpeculativeActionSettings })
		=> Promise<readonly Pick<PatternAwareCandidate, "tool" | "input" | "horizon">[]>;
	readonly dispose: () => Promise<void>;
}

export function createPatternPlanSource({
	sessionID, cwd, actionSemantics, projectionRules, stateDirectory, reservedRoot, workspaceIdentity, store: providedStore,
}: {
	readonly sessionID: string;
	readonly cwd: string;
	readonly actionSemantics: ActionSemanticsRegistry;
	readonly projectionRules: readonly ActionProjectionRule<ToolSettlement>[];
	readonly stateDirectory?: string;
	readonly reservedRoot?: boolean;
	readonly workspaceIdentity?: string;
	readonly store?: PatternAwareStore | Promise<PatternAwareStore>;
}): PatternPlanSourceController {
	cwd = path.resolve(cwd);
	const patternActionSemantics = patternAwareActionSemantics(actionSemantics, cwd, projectionRules, predictionFileExists);
	let openedStore: { readonly key: string; readonly lease: Promise<PatternAwareStoreLease> } | undefined;
	const ownedStores = new Map<string, Promise<PatternAwareStoreLease>>();
	const lifecycle = new RuntimeLifecycleLane();
	const turns = new Map<string, PatternTurn>();
	const carriedPredictions = new Map<string, CarriedPrediction>();
	const feedbacks = new WeakMap<object, { readonly context?: PatternPlanFeedback; carried?: CarriedPrediction; readonly retry?: FailedCommand }>();
	const failedCommands = new BoundedRecencyMap<string, FailedCommand>(4);
	let analysisTail: Promise<void> = Promise.resolve();
	const sourceSettings = (settings: SpeculativeActionSettings): PatternAwareSettings => {
		const pattern = patternAwareSettings(settings.sourceConfig?.patternAware);
		if (!pattern.presets.includes("retry-failed-command")) failedCommands.clear();
		return pattern;
	};
	const admit = <Value>(settings: SpeculativeActionSettings, operation: (settings: PatternAwareSettings) => Promise<Value>): Promise<Value> => {
		try { const pattern = sourceSettings(settings); return lifecycle.admit(() => operation(pattern)); }
		catch (error) { return Promise.reject(error); }
	};
	const turnState = (sessionID: string, turnID: string): PatternTurn => {
		const key = agentBatchKey(sessionID, turnID);
		let turn = turns.get(key);
		if (!turn) turns.set(key, turn = { events: new Map(), revision: -1, observationRevision: -1 });
		return turn;
	};
	const resolveStore = async (patternSettings: PatternAwareSettings): Promise<PatternAwareStore> => {
		if (providedStore) return providedStore;
		const configurationKey = patternAwareAnalyzerKey(patternSettings);
		if (!openedStore || openedStore.key !== configurationKey) {
			const previous = openedStore;
			const retained = ownedStores.get(configurationKey);
			const opening = { key: configurationKey, lease: Promise.resolve().then(async () => {
				if (previous) await previous.lease.then(({ store }) => store.finishSession(sessionID))
					.catch(() => undefined); // Failed loading cannot poison the next analyzer.
				return retained ?? acquirePatternAwareStore(workspaceIdentity ?? cwd, patternSettings, stateDirectory, patternActionSemantics, reservedRoot);
			}) };
			// Predictions retain their analyzer for late feedback, including after returning to this configuration.
			if (!retained) ownedStores.set(configurationKey, opening.lease);
			openedStore = opening;
			void opening.lease.catch(() => {
				if (ownedStores.get(configurationKey) === opening.lease) ownedStores.delete(configurationKey);
				if (openedStore === opening) openedStore = undefined;
			});
		}
		return (await openedStore.lease).store;
	};
	const predictionStore = async (settings: PatternAwareSettings, signal?: AbortSignal) => {
		await analysisTail;
		if (signal?.aborted) return;
		const store = await resolveStore(settings);
		return signal?.aborted ? undefined : store;
	};
	const flushStores = async (finish = false): Promise<void> => {
		await analysisTail;
		const stores = providedStore ? [Promise.resolve(providedStore)] : [...ownedStores.values()].map(async lease => (await lease).store);
		const results = await Promise.allSettled(stores.map(async pending => {
			const store = await pending;
			if (finish) store.finishSession(sessionID);
			await store.flush();
		}));
		const failure = results.find(result => result.status === "rejected");
		if (failure) throw failure.reason;
	};
	const eventData = (tool: string, input: Readonly<Record<string, unknown>>, output: ToolSettlement | undefined, durationMs: number) => ({
		tool, input: structuredClone(input), outcome: output?.isError ? "failure" as const : "success" as const,
		...projectPatternAwareObservation(output?.result, extractOutputPaths(tool, input, output?.result), cwd, extractOutputLocations(tool, input, output?.result)),
		...(output?.isError ? { errorClass: failureClass(output.result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n")) } : {}),
		durationMs,
		...(typeof input.operation === "string" ? { operation: input.operation } : {}),
	});
	const predictedEvent = (startInput: AgentStartInput, action: Pick<SpeculativeCandidate, "key" | "input">,
		output: ToolSettlement, durationMs: number): PatternAwareEventInput => {
		if (!action.key) throw new Error("prediction_key_required");
		return { sessionID: startInput.sessionID, turnID: startInput.turnID,
			...eventData(action.key.tool, action.input, output, durationMs), schemaHash: action.key.schemaHash, learnTarget: false };
	};
	const resourcePath = (target: string) => patternActionSemantics.actionKey("read", { path: target })?.resources[0];
	const planActions = (candidates: readonly PatternAwareCandidate[], store: PatternAwareStore,
		dependsOn?: PlanAction["dependsOn"], parentID?: string) => candidates.map(candidate => {
			const action = patternPlanAction(candidate, store, patternPlanActionID(candidate.actionIdentity, parentID), dependsOn);
			feedbacks.set(action.feedback, { context: action.feedback }); return action;
		});
	const retries = (schemas: Readonly<Record<string, string>>): PlanAction[] => [...failedCommands.values()].flatMap(command => {
		if (!command.requested || command.schemaHash !== schemas[command.tool]) return [];
		const feedback = {};
		feedbacks.set(feedback, { retry: command });
		return [{ id: `retry:${command.id}`, type: "tool_call", tool: command.tool, input: command.input, horizon: 0,
			mode: "retry-failed-command", empiricalProbability: 0.25, conditionalProbability: 0.25, feedback }];
	});

	const source: AgentPlanSource = {
		id: "pattern_aware",
		enabled: (settings) => !lifecycle.sealed && sourceSettings(settings).enabled,
		multiStepEnabled: (settings) => sourceSettings(settings).multiStepEnabled,
		requestLifetime: "actor_decision",
		propose: ({ startInput, data, settings, signal }) => admit(settings, async (patternSettings) => {
			if (!patternSettings.enabled) return undefined;
			const turn = turnState(startInput.sessionID, startInput.turnID);
			const store = await predictionStore(patternSettings, signal);
			if (!store) return undefined;
			const candidates = await predict(() => store.predict(startInput.sessionID, data.schemaHashes, patternSettings));
			const carried = carriedPredictions.get(startInput.sessionID), retry = retries(data.schemaHashes);
			if (signal.aborted) return undefined;
			carriedPredictions.delete(startInput.sessionID);
			const repeated = !candidates.length || carried?.signature === patternPredictionSignature(candidates) && !carried.pending.size && !carried.abandoned;
			if (repeated && !retry.length) return undefined;
			return { id: `pattern:${startInput.turnID}`, source: "pattern_aware", revision: ++turn.revision,
				actions: [...repeated ? [] : planActions(candidates, store), ...retry] };
		}),
		continueFrom: ({ startInput, data, settings, batch, signal }) => admit(settings, async (patternSettings) => {
			const store = await predictionStore(patternSettings, signal);
			if (!store) return undefined;
			const id = `pattern:peer:${stableValueHash(batch.map(({ identity }) => identity.id))}`;
			const candidates = await predict(() => store.predictAfterBatch(startInput.sessionID,
				batch.map(({ candidate, output }) => predictedEvent(startInput, candidate, output, nonNegativeFinite(candidate.executionMs))),
				data.schemaHashes, patternSettings,
				// No calibrated joint confidence is supplied for the foreign batch; use a neutral prior.
				{ visitedPatternIDs: [id], pathProbability: 0.5 }));
			if (!candidates.length) return undefined;
			const dependencies = batch.map(({ identity }) => ({ proposalID: identity.proposalID, actionID: identity.actionID,
				identity: identity.id, condition: "execution_succeeded" as const }));
			return { id, source: "pattern_aware", revision: 0, actions: planActions(candidates, store, dependencies) };
		}),
		continue: ({ startInput, data, settings, candidate, adoptedAction, proposalID, actionID, revision, feedback, output, trigger, signal }) => admit(settings, async (patternSettings) => {
			if (signal.aborted || output === undefined) return undefined;
			const context = feedbacks.get(feedback as object)?.context;
			if (!context) return undefined;
			const next = await predict(() => context.store.continue(context.continuation, predictedEvent(startInput, adoptedAction ?? candidate, output, nonNegativeFinite(candidate.executionMs)),
				data.schemaHashes, trigger === "actor_adopted", patternSettings));
			if (!next.length) return undefined;
			return { proposalID, source: "pattern_aware", revision,
				upsert: planActions(next, context.store, [{ actionID, condition: "execution_succeeded" }], actionID),
			};
		}),
	};
	const observe = ({ data, settings, consumeInput, action, tool, concrete, output, durationMs, order, signal }: AgentRuntimeEvents["actor_observed"],
		reserveRevision: (id: string, minimum: number) => number) => admit(settings, async (patternSettings) => {
			const schemaHash = action?.schemaHash ?? data.schemaHashes[tool];
			const observed = eventData(tool, concrete, output, durationMs);
			if (patternSettings.presets.includes("retry-failed-command")) {
				if (tool === "bash") {
					const id = stableValueHash([tool, concrete, schemaHash]);
					// Command arguments are hints; only reported failures establish the retry relation.
					const paths = output?.isError ? [...new Set([...(extractOutputPaths(tool, {}, output.result) ?? []), ...(observed.outputLocations ?? []).map(location => location.path)]
						.flatMap(target => resourcePath(target) ?? []))] : [];
					if (paths.length) failedCommands.set(id, { id, tool, input: observed.input, schemaHash, paths, requested: false });
					else failedCommands.delete(id);
				}
				const edited = action && actionSemantics.effect(action) === "workspace_mutation" && typeof concrete.path === "string" && output && !output.isError ? resourcePath(concrete.path) : undefined;
				if (edited) for (const command of failedCommands.values()) if (command.paths.includes(edited)) command.requested = true;
			}
			const turn = turnState(consumeInput.sessionID, consumeInput.turnID), batch = turn.events;
			batch.set(order, { sessionID: consumeInput.sessionID, turnID: consumeInput.turnID, ...observed,
				...(schemaHash === undefined ? {} : { schemaHash }),
				learnTarget: data.tools.has(tool),
			});
			if (!settings.enabled || !patternSettings.enabled) return undefined;
			let actions = retries(data.schemaHashes);
			if (!patternSettings.multiStepEnabled && !actions.length || signal?.aborted) return undefined;
			if (patternSettings.multiStepEnabled) {
				const store = await predictionStore(patternSettings, signal);
				if (!store) return undefined;
				const ordered = [...batch.entries()].sort(([left], [right]) => left - right).map(([, item]) => item);
				const candidates = await predict(() => store.predictAfterBatch(consumeInput.sessionID, ordered, data.schemaHashes, patternSettings));
				const predicted = planActions(candidates, store);
				// An observation can finish after its turn closes, or lose individual actions during admission.
				const carried = { signature: patternPredictionSignature(candidates), pending: new Set(predicted.map((action) => action.feedback)), abandoned: false };
				for (const action of predicted) feedbacks.get(action.feedback)!.carried = carried;
				carriedPredictions.set(consumeInput.sessionID, carried);
				actions = [...predicted, ...actions];
			}
			// Its own namespace never supersedes the turn's still-pending predictions; the runtime's revision covers continuations.
			const id = `pattern:${consumeInput.turnID}:after`, minimum = ++turn.observationRevision;
			const revision = reserveRevision(id, minimum);
			turn.observationRevision = revision;
			return { id, source: "pattern_aware", revision, actions, retainDescendants: true as const };
		});

	const observeTurn = (startInput: AgentStartInput, settings: SpeculativeActionSettings, terminal?: boolean): void => {
		if (lifecycle.sealed) return;
		const key = agentBatchKey(startInput.sessionID, startInput.turnID);
		const batch = turns.get(key)?.events;
		turns.delete(key);
		if (terminal) carriedPredictions.delete(startInput.sessionID);
		const patternSettings = sourceSettings(settings);
		if (!settings.enabled || !patternSettings.enabled) carriedPredictions.delete(startInput.sessionID);
		// Only a completed turn contributes its authoritative batch; entry discards any stale batch.
		const events = terminal !== undefined && batch?.size ? [...batch.entries()].sort(([left], [right]) => left - right).map(([, event]) => event) : [];
		analysisTail = analysisTail.then(() => new Promise<void>(setImmediate))
			.then(async () => {
				const store = await resolveStore(patternSettings);
				if (events.length) store.observeBatch(events);
				store.observeTurn();
				if (terminal) store.finishSession(startInput.sessionID);
			})
			.catch(() => {
				// Optional learning cannot poison later observations or the Actor lifecycle.
			});
	};
	const clearSession = async () => {
		await lifecycle.drain();
		for (const entries of [carriedPredictions, failedCommands]) entries.clear();
		for (const [key, turn] of turns) {
			turn.revision = turn.observationRevision = -1;
			if (!turn.events.size || turn.events.values().next().value?.sessionID === sessionID) turns.delete(key);
		}
	};
	return {
		source,
		connect: runtime => runtime.subscribe({
			actor_observed: async event => {
				const update = await observe(event, (id, minimum) => runtime.reservePlanRevision(event.sessionID, id, minimum));
				await runtime.submitPlan(event.startInput, update);
			},
			prediction_admitted: ({ feedback, issued }) => {
				if (lifecycle.sealed) return;
				const state = feedbacks.get(feedback as object);
				if (state?.retry) state.retry.requested = false;
				if (state?.context) {
					state.carried?.pending.delete(state.context);
					if (issued) for (const support of [state.context.continuation, ...state.context.supportingPatternIDs]) state.context.store.issued(support);
				}
			},
			prediction_feedback: ({ feedback, settlement, served }) => {
				if (lifecycle.sealed) return;
				const { context, carried } = feedbacks.get(feedback as object) ?? {};
				if (carried && settlement.observation === "unobserved") carried.abandoned = true;
				if (context) for (const support of [context.continuation, ...context.supportingPatternIDs]) context.store.settled(support, settlement, served);
			},
			turn_started: ({ startInput, settings }) => observeTurn(startInput, settings),
			turn_finished: async ({ startInput, settings, terminal }) => {
				observeTurn(startInput, settings, terminal);
				await lifecycle.run(() => flushStores());
			},
			session_closed: () => lifecycle.run(async () => {
				await clearSession();
				await flushStores(true);
			}),
		}),
		hints: ({ sessionID, schemaHashes, settings }) => admit(settings, async (patternSettings) => {
			if (!settings.enabled || !patternSettings.enabled) return [];
			const store = await predictionStore(patternSettings);
			return (await predict(() => store!.predict(sessionID, schemaHashes, patternSettings))).slice(0, 4).map(({ tool, input, horizon }) => ({ tool, input, horizon }));
		}),
		dispose: () => lifecycle.close(async () => {
			await analysisTail;
			await clearSession();
			const leases = [...ownedStores.values()];
			ownedStores.clear();
			openedStore = undefined;
			turns.clear();
			await Promise.allSettled(leases.map(async lease => (await lease).release()));
		}),
	};
}

export function patternPlanActionID(actionIdentity: string, parentActionID = "root"): string {
	return `pattern:${stableValueHash({ actionIdentity, parentActionID }).slice(0, 16)}`;
}

function patternPredictionSignature(candidates: readonly PatternAwareCandidate[]): string {
	return JSON.stringify(
		candidates.map((candidate) => [candidate.actionIdentity, candidate.horizon, candidate.latestHorizon] as const)
			.sort(([left], [right]) => left.localeCompare(right)),
	);
}

function patternPlanAction(candidate: PatternAwareCandidate, store: PatternAwareStore, id: string,
	dependsOn?: PlanAction["dependsOn"]): PlanAction & { readonly feedback: PatternPlanFeedback } {
	const { tool, input, horizon, latestHorizon, empiricalProbability, conditionalProbability, confidence, background, depth, presetID, supportingPatternIDs } = candidate;
	return { id, type: "tool_call", tool, input, horizon, latestHorizon,
		empiricalProbability, conditionalProbability, confidence, depth, ...(presetID ? { mode: presetID } : {}),
		...(background ? { background: true } : {}), ...(dependsOn?.length ? { dependsOn } : {}),
		feedback: { store, continuation: candidate.continuation, supportingPatternIDs },
	};
}

/** Files a run's text names as compilers, test runners and stack traces print them: with a directory, or with a line after them. */
const RUN_OUTPUT_LOCATION = /(?<![\w.@/\\:-])((?:[A-Za-z]:)?[\w.@-]{0,128}(?:[\\/][\w.@-]{1,128}){0,16}\.[A-Za-z]\w{0,9})(?::(\d+)|\((\d+)|",? line (\d+))/gu;
const RUN_OUTPUT_PATH = /(?<![\w.@/\\:-])((?:[A-Za-z]:)?[\w.@-]{0,128}(?:[\\/][\w.@-]{1,128}){0,16}\.[A-Za-z]\w{0,9})(:\d|\(\d|",? line \d)?/gu;

function outputPath(tool: string, input: Readonly<Record<string, unknown>>, file: string): string {
	const root = typeof input.path === "string" && input.path ? input.path : ".";
	return tool === "bash" || path.isAbsolute(file) ? file : tool === "grep" && path.basename(root) === file ? root : path.join(root, file);
}

/** `path:line` facts: grep's matches, and a command's reported locations (compiler errors, stack traces, test failures). */
function extractOutputLocations(tool: string, actionInput: Readonly<Record<string, unknown>>, result: AgentToolResult<unknown> | undefined): OutputLocation[] {
	if ((tool !== "grep" && tool !== "bash") || !result) return [];
	const text = result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n");
	const located = tool === "grep" ? [...text.matchAll(/^(.+?):(\d+)(?::\d+)?:/gmu)] : [...text.matchAll(RUN_OUTPUT_LOCATION)];
	return located.slice(0, 256).flatMap(([, file, ...lines]) => {
		const line = lines.find(Boolean);
		const number = Number(line);
		if (!file || !isSafeCount(number, 1)) return [];
		return [{ path: outputPath(tool, actionInput, file), line: number }];
	});
}

function extractOutputPaths(
	tool: string,
	actionInput: Readonly<Record<string, unknown>>,
	result: AgentToolResult<unknown> | undefined,
): readonly string[] | undefined {
	if ((tool !== "find" && tool !== "grep" && tool !== "bash") || !result) return undefined;
	const text = result.content.flatMap((item) => item.type === "text" ? [item.text] : []).join("\n");
	if (tool === "bash") { // The files a command names as it runs, and those its output reports.
		// A git diff names each file as a/path and b/path.
		const diff = /^(?:diff --git|\+\+\+|---) [ab]\//mu.test(text), named = [...`${actionInput.command}\n${text}`.matchAll(RUN_OUTPUT_PATH)]
			.flatMap((match) => match[2] || /[\\/]/u.test(match[1]!) ? [diff ? match[1]!.replace(/^[ab]\//u, "") : match[1]!] : []);
		return named.length ? [...new Set(named)].slice(0, 32) : undefined;
	}
	const paths = text.split(/\r?\n/).flatMap(line => {
		const trimmed = line.trim(), file = tool === "find" ? trimmed : /^(.*?):\d+(?::\d+)?:/.exec(trimmed)?.[1];
		return file && !trimmed.startsWith("[") && !/^No files found\b/.test(trimmed) ? [outputPath(tool, actionInput, file)] : [];
	});
	return paths.length ? [...new Set(paths)] : undefined;
}
