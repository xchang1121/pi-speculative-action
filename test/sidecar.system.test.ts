import { createServer } from "node:http";
import { afterEach, expect, test, vi } from "vitest";
import { createActorForkPlanSource, EvidenceLedger } from "../src/self-speculation.ts";
import { SelfSpeculationCoordinator, normalizeSelfSpeculationSettings } from "../src/self-speculation.ts";
import { nextTurn } from "./async.ts";
import { testModel } from "./model.ts";

afterEach(() => vi.unstubAllEnvs());

test("expires calibration contexts and reverses old successes as recent trials drift", () => {
	const ledger = new EvidenceLedger(2, 1, 3);
	const cold = { tool: "cold" },
		active = { tool: "active" };
	ledger.observe(cold, 20, 20);
	ledger.observe(active, 2, 2);
	ledger.observe(active, 2, 2);
	expect(ledger.probability([active])).toBeCloseTo(5 / 6);
	for (let i = 0; i < 3; i++) ledger.observe(active, 2, 0);
	expect(ledger.probability([cold])).toBe(0.5);
	expect(ledger.probability([active])).toBeCloseTo(1 / 8);
	expect(ledger.snapshot()).toEqual({ contexts: 1, observations: 3, trials: 6, successes: 0 });
	for (let i = 0; i < 20; i++) ledger.observe({ i }, 1, 1);
	expect(ledger.snapshot()).toEqual({ contexts: 3, observations: 3, trials: 3, successes: 3 });
	expect(ledger.probability([active])).toBe(0.5);
});

test.each(["valid", "low confidence", "HTTP failure"])(
	"runs a sidecar fork over HTTP and drains the request (%s)",
	async (mode) => {
		const defaults = normalizeSelfSpeculationSettings({});
		const requests: {
			path: string;
			auth: string | undefined;
			body: Record<string, unknown>;
		}[] = [];
		const server = createServer(async (request, response) => {
			const parts: Buffer[] = [];
			for await (const part of request) parts.push(Buffer.from(part));
			const bytes = Buffer.concat(parts).toString();
			requests.push({
				path: request.url!,
				auth: request.headers.authorization,
				body: bytes ? JSON.parse(bytes) : {},
			});
			response.setHeader("content-type", "application/json");
			const fork = request.url === defaults.forkPath;
			if (fork && mode === "HTTP failure") {
				response.writeHead(503);
				response.end("{}");
				return;
			}
			response.end(
				JSON.stringify(
					fork
						? {
								details: {
									bundle: {
										candidates: [
											{
												sources: ["self-speculation"],
												tool_calls: [{ name: "read", arguments: { path: "notes.txt" } }],
												fork: {
													logprobs: {
														token_count: 2,
														mean: -0.03,
														tool_name: {
															minimum_probability: mode === "low confidence" ? 0.1 : 0.95,
														},
													},
												},
											},
										],
									},
								},
							}
						: {
								capabilities: { fork: true, logprobs: true, provider: true },
								ok: true,
							},
				),
			);
		});
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("missing HTTP address");
		vi.stubEnv("PI_TEST_SIDECAR_TOKEN", "test-only-token");
		const source = createActorForkPlanSource({ maxAttempts: 1 });
		const settings = {
			...defaults,
			enabled: true,
			forkTransport: "sidecar" as const,
			forkMaxAttempts: 1,
			endpoint: `http://127.0.0.1:${address.port}`,
			apiKeyEnv: "PI_TEST_SIDECAR_TOKEN",
		};
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => settings,
			actorForkPlanSource: source,
			requestID: () => "actor-request",
		});
		try {
			coordinator.startTurn(
				"turn",
				testModel("actor", { baseUrl: "https://hosted.example.invalid/v1" }),
				{ messages: [] },
				1,
			);
			const batches = source.waitForBatches("turn", new AbortController().signal);
			const payload = {
				model: "actor",
				messages: [{ role: "user", content: "inspect notes" }],
			};
			expect(coordinator.decorateActorPayload(payload)).toEqual(payload);
			coordinator.observeActorOutput({
				type: "text_delta",
				contentIndex: 0,
				delta: "Inspect the file.",
				partial: undefined as never,
			});
			const calls = (await batches).flatMap((batch) =>
				batch.calls.map(({ tool, input }) => ({ tool, input })),
			);
			expect(calls).toEqual(mode === "valid" ? [{ tool: "read", input: { path: "notes.txt" } }] : []);
			settings.enabled = false;
			coordinator.reset();
			coordinator.observePredictionSettlement({
				sessionID: "session",
				turnID: "turn",
				tool: "read",
				settlement: {
					prediction: {
						id: "prediction",
						source: "self-speculation",
						proposalID: "proposal",
						actionID: "action",
					},
					observation: "observed",
					actorAction: {
						id: "call",
						sequence: 1,
						decisionSequence: 1,
						turnID: "turn",
					},
					match: { matched: false },
				},
			});
			expect(coordinator.snapshot().actionEvidenceObservations).toBe(1);
			await coordinator.dispose();
			const fork = requests.find((request) => request.path === settings.forkPath);
			expect(fork?.body).toMatchObject({
				request_id: "actor-request",
				context: { provider_payload: payload },
			});
			expect(requests.map(({ body }) => Object.hasOwn(body, "version"))).not.toContain(true);
			expect(requests.every((request) => request.auth === "Bearer test-only-token")).toBe(true);
			expect(requests.at(-1)).toMatchObject({
				path: settings.clearPath,
				body: { request_id: "actor-request" },
			});
		} finally {
			await coordinator.dispose();
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	},
);

test.each(["resolve", "reject"])(
	"a late %s cannot settle a replacement fork with the same turn ID",
	async (outcome) => {
		const pending = [0, 1].map(() =>
			Promise.withResolvers<{ tool: string; input: { revision: number } }[]>(),
		);
		let calls = 0;
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => normalizeSelfSpeculationSettings({ enabled: true, forkTransport: "drafter" }),
			draftFork: () => pending[calls++]!.promise,
		});
		const begin = (revision: number) => {
			coordinator.startTurn("same", testModel("actor"), { messages: [] }, revision);
			coordinator.decorateActorPayload({});
			coordinator.observeActorOutput({
				type: "text_delta",
				contentIndex: 0,
				delta: "read",
				partial: undefined as never,
			});
			return coordinator.actorForkPlanSource.waitForBatches("same", new AbortController().signal);
		};
		try {
			const old = begin(1);
			const current = begin(2);
			await expect(old).resolves.toEqual([]);
			if (outcome === "resolve") pending[0]!.resolve([{ tool: "read", input: { revision: 1 } }]);
			else pending[0]!.reject(new Error("retired request"));
			await nextTurn();
			pending[1]!.resolve([{ tool: "read", input: { revision: 2 } }]);
			expect(await current).toMatchObject([{ calls: [{ input: { revision: 2 } }] }]);
		} finally {
			await coordinator.dispose();
		}
	},
);
