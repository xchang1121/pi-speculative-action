import { createServer } from "node:http";
import { afterEach, expect, test, vi } from "vitest";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
import { SelfSpeculationCoordinator, SELF_SPECULATION_DEFAULTS } from "../src/self-speculation.ts";
import { testModel } from "./model.ts";

afterEach(() => vi.unstubAllEnvs());

test.each(["valid", "low confidence", "HTTP failure"])("runs a sidecar fork over HTTP and drains the request (%s)", async mode => {
	const requests: { path: string; auth: string | undefined; body: Record<string, unknown> }[] = [];
	const server = createServer(async (request, response) => {
		const parts: Buffer[] = []; for await (const part of request) parts.push(Buffer.from(part));
		const bytes = Buffer.concat(parts).toString();
		requests.push({ path: request.url!, auth: request.headers.authorization, body: bytes ? JSON.parse(bytes) : {} });
		response.setHeader("content-type", "application/json");
		const fork = request.url === SELF_SPECULATION_DEFAULTS.forkPath;
		if (fork && mode === "HTTP failure") { response.writeHead(503); response.end('{}'); return; }
		response.end(JSON.stringify(fork ? { details: { bundle: { candidates: [{
			sources: ["self-speculation"],
			tool_calls: [{ name: "read", arguments: { path: "notes.txt" } }],
			fork: { logprobs: { token_count: 2, mean: -0.03, tool_name: { minimum_probability: mode === "low confidence" ? 0.1 : 0.95 } } },
		}] } } } : { capabilities: { fork: true, logprobs: true, provider: true }, ok: true }));
	});
	await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
	const address = server.address(); if (!address || typeof address === "string") throw new Error("missing HTTP address");
	vi.stubEnv("PI_TEST_SIDECAR_TOKEN", "test-only-token");
	const source = createActorForkPlanSource({ maxAttempts: 1 });
	const settings = { ...SELF_SPECULATION_DEFAULTS, enabled: true, forkTransport: "sidecar" as const, forkMaxAttempts: 1,
		endpoint: `http://127.0.0.1:${address.port}`, apiKeyEnv: "PI_TEST_SIDECAR_TOKEN" };
	const coordinator = new SelfSpeculationCoordinator({ settings: () => settings, actorForkPlanSource: source, requestID: () => "actor-request" });
	try {
		coordinator.startTurn("turn", testModel("actor", { baseUrl: "https://hosted.example.invalid/v1" }), { messages: [] }, 1);
		const batches = source.waitForBatches("turn", new AbortController().signal);
		const payload = { model: "actor", messages: [{ role: "user", content: "inspect notes" }] };
		expect(coordinator.decorateActorPayload(payload)).toEqual(payload);
		coordinator.observeActorOutput({ type: "text_delta", contentIndex: 0, delta: "Inspect the file.", partial: undefined as never });
		const calls = (await batches).flatMap(batch => batch.calls.map(({ tool, input }) => ({ tool, input })));
		expect(calls).toEqual(mode === "valid" ? [{ tool: "read", input: { path: "notes.txt" } }] : []);
		settings.enabled = false; coordinator.reset();
		coordinator.observePredictionSettlement({ sessionID: "session", turnID: "turn", tool: "read", settlement: {
			prediction: { id: "prediction", source: "self-speculation", proposalID: "proposal", actionID: "action" },
			observation: "observed", actorAction: { id: "call", sequence: 1, decisionSequence: 1, turnID: "turn" }, match: { matched: false },
		} });
		expect(coordinator.snapshot().actionEvidenceObservations).toBe(1);
		await coordinator.dispose();
		const fork = requests.find(request => request.path === settings.forkPath);
		expect(fork?.body).toMatchObject({ request_id: "actor-request", context: { provider_payload: payload } });
		expect(requests.every(request => request.auth === "Bearer test-only-token")).toBe(true);
		expect(requests.at(-1)).toMatchObject({ path: settings.clearPath, body: { request_id: "actor-request" } });
	} finally { await coordinator.dispose(); server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
});
