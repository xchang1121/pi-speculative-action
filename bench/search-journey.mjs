import assert from "node:assert/strict";
import { createFauxCore, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createSpeculativeActionHost } from "../dist/agent-integration.js";

/** Common Host lifecycle for captured-search qualification; callers own inputs and assertions. */
export function searchJourney({ cwd, name, tools, args, invocation, world, settings = {} }) {
	const model = createFauxCore({ provider: "qualification", models: [{ id: "qualification", reasoning: false }] }).getModel();
	const candidate = Promise.withResolvers(), authorized = Promise.withResolvers();
	let prediction = true, turnID, actorWaiting = false, actorCalls = 0, feedback;
	const host = createSpeculativeActionHost("search-qualification", {
		cwd, getSettings: () => ({ enabled: true, drafterEnabled: prediction, drafterGateEnabled: false,
			drafterMaxDepth: 0, candidateLimit: 1, maxConcurrentActions: 1, tools: prediction ? [name] : [],
			patternAware: { enabled: false }, selfSpeculation: { enabled: false }, ...settings }),
		draftModel: model, complete: async () => fauxAssistantMessage(fauxToolCall(name, args), { stopReason: "toolUse" }),
		resolveInvocation: (tool) => tool === name ? invocation : undefined,
		preflight: () => { if (actorWaiting) authorized.resolve(); return true; },
		executionWorlds: [world],
		onEvent: (event) => {
			if (event.type === "candidate" && event.candidate.origin === "prediction" && event.state.status !== "running") candidate.resolve(event.state);
			if (event.type === "prediction" && event.settlement.observation === "unobserved") candidate.resolve(event.settlement);
		},
		onActorActionSettled: ({ settlement }) => feedback?.resolve(settlement),
	});
	return { host, candidate: candidate.promise, authorized: authorized.promise, actorCalls: () => actorCalls,
		start: async (id, predict = true) => {
			if (turnID) await host.finishTurn(turnID);
			turnID = id; prediction = predict;
			await host.startTurn({ turnID, actorModel: model, actorOptions: undefined, tools,
				context: { systemPrompt: "qualification", messages: [], tools } });
		},
		actor: async (id, query = args) => {
			actorWaiting = true; feedback = Promise.withResolvers();
			const before = actorCalls;
			try {
				const output = await host.execute({ turnID, id, tool: name, args: query, tools }, undefined, async (operation) => {
					assert.deepEqual(operation.invocation?.identity, invocation.identity, "Actor fallback must retain the selected executor independently of K(a)");
					actorCalls++;
					return (await operation.invocation.authoritative({ args: operation.input, signal: operation.signal, callID: operation.callID })).result;
				});
				const settlement = await bounded(feedback.promise, "Actor settlement");
				assert.equal(actorCalls - before, settlement.provider.kind === "actor" ? 1 : 0, "each fallback executes the original Actor exactly once");
				return { output, settlement };
			} finally { actorWaiting = false; }
		},
	};
}

export function bounded(promise, label) {
	let timer;
	return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label + " deadline")), 15_000); })])
		.finally(() => clearTimeout(timer));
}
