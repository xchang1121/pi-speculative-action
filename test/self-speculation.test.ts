import { createHash } from "node:crypto";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import type { ActionKey } from "../src/action-semantics.ts";
import type { MaterializedSpeculativeCandidate, PredictionFeedback } from "../src/runtime.ts";
import { SelfSpeculationActionBridge } from "../src/self-speculation-action-bridge.ts";
import {
	normalizeSelfSpeculationSettings,
	SELF_SPECULATION_DEFAULTS,
	SelfSpeculationCoordinator,
	type SelfSpeculationSettings,
} from "../src/self-speculation.ts";

describe("self-speculation control plane", () => {
	it("normalizes opt-in settings without weakening bounded defaults", () => {
		expect(normalizeSelfSpeculationSettings(undefined)).toEqual(SELF_SPECULATION_DEFAULTS);
		expect(
			normalizeSelfSpeculationSettings({
				enabled: true,
				endpoint: "http://localhost:9000///",
				candidatePath: "not-a-path",
				maxCandidates: 0,
				forkTransport: "sidecar",
				forkTemperature: -1,
				forkActionMinConfidence: 2,
				forkForcedPrefix: "",
				apiKeyEnv: " TOKEN_ENV ",
			}),
		).toEqual({
			...SELF_SPECULATION_DEFAULTS,
			enabled: true,
			endpoint: "http://localhost:9000",
			forkTransport: "sidecar",
			apiKeyEnv: "TOKEN_ENV",
		});
		expect(normalizeSelfSpeculationSettings({ forkActionMinConfidence: 0 }).forkActionMinConfidence).toBe(0);
	});

	it("buffers every source, merges the same K(a), and submits one ordered Actor bundle", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 0.7));
		coordinator.addCandidate(candidate("pattern-aware", "key-a", "hash-a", "read", { path: "a.txt" }, 0.9));
		coordinator.addCandidate(candidate("drafter", "key-b", "hash-b", "read", { path: "b.txt" }, 0.8));

		expect(requests).toHaveLength(0);
		expect(coordinator.decorateActorPayload({ model: "actor" })).toEqual(
			expect.objectContaining({
				request_id: "actor-request",
				self_speculation: expect.objectContaining({ role: "actor", fork: false }),
			}),
		);
		await coordinator.dispose();

		const bundle = requests.find((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundle?.body).toMatchObject({
			version: 2,
			request_id: "actor-request",
			max_draft_tokens: SELF_SPECULATION_DEFAULTS.maxDraftTokens,
			format: "tagged_json",
			boundary: "<tool_call>",
		});
		expect(bundle?.body.candidates).toEqual([
			expect.objectContaining({
				id: actionIdentity("key-a"),
				action_identity: {
					version: 1,
					predicted_action_id: actionIdentity("key-a"),
					execution_action_id: actionIdentity("key-a"),
					projected: false,
				},
				sources: ["drafter", "pattern-aware"],
				tool_call: { name: "read", arguments: { path: "a.txt" } },
				score: expect.objectContaining({ conditional_probability: 0.9 }),
			}),
			expect.objectContaining({ id: actionIdentity("key-b"), sources: ["drafter"] }),
		]);
		expect(requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath)).toHaveLength(1);
		expect(requests.at(-1)).toMatchObject({
			path: SELF_SPECULATION_DEFAULTS.clearPath,
			body: { version: 1, request_id: "actor-request" },
		});
	});

	it("keeps distinct predicted actions when they share one covering execution", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(
			candidate("drafter", "predicted-a", "unused-a", "read", { path: "a.txt", offset: 1 }, 0.8, 1, 1, "covering"),
		);
		coordinator.addCandidate(
			candidate("pattern-aware", "predicted-b", "unused-b", "read", { path: "a.txt", offset: 5 }, 0.7, 1, 1, "covering"),
		);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundle = requests.find((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundle?.body.candidates).toEqual([
			expect.objectContaining({
				id: actionIdentity("predicted-a"),
				action_identity: expect.objectContaining({
					execution_action_id: actionIdentity("covering"),
					projected: true,
				}),
			}),
			expect.objectContaining({
				id: actionIdentity("predicted-b"),
				action_identity: expect.objectContaining({
					execution_action_id: actionIdentity("covering"),
					projected: true,
				}),
			}),
		]);
	});

	it("isolates Drafter request controls from the stable Actor request identity", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, {}, ["actor-request", "drafter-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);

		const actor = coordinator.decorateActorPayload({ model: "actor" }) as Record<string, unknown>;
		const drafter = coordinator.decorateDrafterPayload({ model: "drafter" }) as Record<string, unknown>;
		const secondActor = coordinator.decorateActorPayload({ model: "actor-retry" });

		expect(actor.request_id).toBe("actor-request");
		expect(actor.self_speculation).toEqual(expect.objectContaining({ role: "actor", fork: true }));
		expect(drafter.request_id).toBe("drafter-request");
		expect(drafter.self_speculation).toEqual(expect.objectContaining({ role: "drafter", fork: true }));
		expect(secondActor).toEqual({ model: "actor-retry" });
		await coordinator.dispose();
	});

	it("records clear-time target verification without confusing registration receipts", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(
			requests,
			{ forkEnabled: false },
			["actor-request"],
			(request) =>
				request.path === SELF_SPECULATION_DEFAULTS.clearPath
					? {
							status: "cleared",
							verification: {
								num_spec_steps: 1,
								num_draft_tokens: 3,
								num_accepted_draft_tokens: 2,
								num_rejected_draft_tokens: 1,
								draft_acceptance_rate: 2 / 3,
								mean_acceptance_length: 3,
								steps: [
									{
										candidate_index: 0,
										candidate_id: actionIdentity("key-a"),
										drafted_tokens: 3,
										accepted_tokens: 2,
										rejected_tokens: 1,
									},
								],
								unresolved_proposals: 0,
								unresolved_draft_tokens: 0,
							},
						}
					: {
							registered: true,
							draft_token_count: 3,
							accepted_token_count: 3,
							details: {
								bundle: {
									candidates: [
										{
											candidate_ids: [actionIdentity("key-a")],
											sources: ["drafter", "pattern-aware"],
										},
									],
								},
							},
						},
		);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 0.8));
		coordinator.addCandidate(
			candidate("pattern-aware", "key-a", "hash-a", "read", { path: "a.txt" }, 0.9),
		);
		coordinator.decorateActorPayload({ model: "actor" });

		await coordinator.dispose();

		expect(coordinator.snapshot()).toMatchObject({
			submittedDraftTokens: 3,
			acceptedDraftTokens: 3,
			verificationRequests: 1,
			verifiedDraftProposals: 1,
			verifiedDraftTokens: 3,
			verifiedAcceptedDraftTokens: 2,
			verifiedRejectedDraftTokens: 1,
			verifiedDraftAcceptanceRate: 2 / 3,
			unresolvedDraftProposals: 0,
			unresolvedDraftTokens: 0,
			lastVerification: {
				requestID: "actor-request",
				speculativeSteps: 1,
				draftedTokens: 3,
				acceptedTokens: 2,
				rejectedTokens: 1,
				steps: [
					expect.objectContaining({
						candidateIndex: 0,
						candidateID: actionIdentity("key-a"),
						sources: ["drafter", "pattern-aware"],
					}),
				],
			},
		});
	});

	it("uses target verification to calibrate the next decision without changing action probabilities", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(
			requests,
			{ forkEnabled: false },
			["actor-1", "actor-2"],
			(request) =>
				request.path === SELF_SPECULATION_DEFAULTS.clearPath && request.body.request_id === "actor-1"
					? {
							verification: {
								num_spec_steps: 2,
								num_draft_tokens: 20,
								num_accepted_draft_tokens: 10,
								num_rejected_draft_tokens: 10,
								steps: [
									{
										candidate_index: 0,
										candidate_id: actionIdentity("drafter-1"),
										candidate_ids: [actionIdentity("drafter-1")],
										sources: ["drafter"],
										drafted_tokens: 10,
										accepted_tokens: 0,
										rejected_tokens: 10,
									},
									{
										candidate_index: 1,
										candidate_id: actionIdentity("pattern-1"),
										candidate_ids: [actionIdentity("pattern-1")],
										sources: ["pattern-aware"],
										drafted_tokens: 10,
										accepted_tokens: 10,
										rejected_tokens: 0,
									},
								],
							},
						}
					: { ok: true },
		);

		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "drafter-1", "unused", "read", { path: "a.txt" }, 0.9));
		coordinator.addCandidate(
			candidate("pattern-aware", "pattern-1", "unused", "read", { path: "b.txt" }, 0.8),
		);
		coordinator.decorateActorPayload({ model: "actor" });
		coordinator.endTurn();
		await vi.waitFor(() => expect(coordinator.snapshot().decoderVerificationSteps).toBe(2));

		coordinator.startTurn("turn-2", model(), context(), 2);
		coordinator.addCandidate(
			candidate("drafter", "drafter-2", "unused", "read", { path: "c.txt" }, 0.9, 2),
		);
		coordinator.addCandidate(
			candidate("pattern-aware", "pattern-2", "unused", "read", { path: "d.txt" }, 0.8, 2),
		);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundles = requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundles.at(-1)?.body.candidates.map((item: Record<string, any>) => item.sources)).toEqual([
			["pattern-aware"],
			["drafter"],
		]);
		expect(bundles.at(-1)?.body.candidates[0].score.decoder_acceptance_probability).toBeGreaterThan(
			bundles.at(-1)?.body.candidates[1].score.decoder_acceptance_probability,
		);
		expect(coordinator.snapshot()).toMatchObject({ decoderEvidenceContexts: 2, decoderVerificationSteps: 2 });
	});

	it("lets independent Actor adoption evidence feed decoder candidate ordering", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-2"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		for (let index = 0; index < 3; index++) {
			coordinator.observePredictionSettlement(predictionFeedback("drafter", false, index));
			coordinator.observePredictionSettlement(predictionFeedback("pattern-aware", true, index));
		}
		coordinator.endTurn();

		coordinator.startTurn("turn-2", model(), context(), 2);
		coordinator.addCandidate(
			candidate("drafter", "drafter-action", "unused", "read", { path: "a.txt" }, 0.95, 2),
		);
		coordinator.addCandidate(
			candidate("pattern-aware", "pattern-action", "unused", "read", { path: "b.txt" }, 0.6, 2),
		);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundle = requests.find((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundle?.body.candidates.map((item: Record<string, any>) => item.sources)).toEqual([
			["pattern-aware"],
			["drafter"],
		]);
		expect(bundle?.body.candidates[0].score.action_adoption_probability).toBeGreaterThan(
			bundle?.body.candidates[1].score.action_adoption_probability,
		);
		expect(coordinator.snapshot()).toMatchObject({
			actionEvidenceContexts: 2,
			actionEvidenceObservations: 6,
			actionEvidenceAdoptions: 3,
		});
	});

	it("contains malformed optional verification without breaking cleanup", async () => {
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => enabledSettings({ forkEnabled: false }),
			requestID: () => "actor-request",
			fetch: vi.fn(async () =>
				Response.json({
					verification: {
						num_draft_tokens: 2,
						num_accepted_draft_tokens: 2,
						num_rejected_draft_tokens: 1,
					},
				}),
			),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.decorateActorPayload({ model: "actor" });

		await expect(coordinator.dispose()).resolves.toBeUndefined();

		expect(coordinator.snapshot()).toMatchObject({
			verificationRequests: 0,
			failures: 1,
			lastError: "self-speculation verification token counts are inconsistent",
		});
	});

	it("requests one sidecar fork from the first Actor output snapshot", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(
			requests,
			{ forkTransport: "sidecar" },
			["actor-request"],
			(request) =>
				request.path === SELF_SPECULATION_DEFAULTS.forkPath
					? forkReceipt("write", { path: "out.txt" })
					: { registered: true, draft_token_count: 8 },
		);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "fork-write", "unused", "write", { path: "out.txt" }, 0.9));
		expect(coordinator.decorateActorPayload({ model: "actor", prompt: "PROMPT" })).toEqual({
			model: "actor",
			prompt: "PROMPT",
			request_id: "actor-request",
		});
		expect(coordinator.decorateDrafterPayload({ model: "drafter" })).toEqual({ model: "drafter" });
		coordinator.observeActorOutput(delta("thinking_delta", "reason"));
		coordinator.observeActorOutput(delta("text_delta", "later"));
		await vi.waitFor(() => expect(coordinator.snapshot().forkCompletions).toBe(1));
		coordinator.addCandidate(
			candidate("self-speculation", "fork-write", "unused", "write", { path: "out.txt" }, 1),
		);
		coordinator.observeActorAction(action("fork-write", "unused", "write", { path: "out.txt" }));
		await coordinator.dispose();

		const forks = requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.forkPath);
		expect(forks).toHaveLength(1);
		expect(forks[0]?.body).toMatchObject({
			request_id: "actor-request",
			context: { provider_payload: { model: "actor", prompt: "PROMPT" } },
			snapshot: {
				generated_text: "reason",
				content: "",
				reasoning: "reason",
				chunk_count: 1,
			},
			options: { decoder: "auto", forced_prefix: "<tool_call>" },
		});
		expect(coordinator.snapshot()).toEqual(
			expect.objectContaining({
				forkRequests: 1,
				forkCompletions: 1,
				forkCandidates: 1,
				forkAgreements: 1,
				forkExactMatches: 1,
				submittedDraftTokens: 28,
				acceptedDraftTokens: 3,
				forkLatencyMs: 25,
				forkLogprobTokens: 2,
				forkMeanLogprob: -0.03,
			}),
		);
	});

	it("publishes only unique, keyable sidecar fork actions as bounded alternatives", async () => {
		const actionBridge = new SelfSpeculationActionBridge();
		const coordinator = new SelfSpeculationCoordinator({
			settings: () =>
				enabledSettings({ forkTransport: "sidecar", forkActionMinConfidence: 0, maxCandidates: 2 }),
			requestID: () => "actor-request",
			actionBridge,
			fetch: vi.fn(async (input) =>
				Response.json(
					new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath
						? {
								details: {
									bundle: {
										candidates: [
											{ sources: ["self-speculation"], tool_calls: [{ name: "read", arguments: { path: "a.txt" } }] },
											{ sources: ["self-speculation"], tool_calls: [{ name: "read", arguments: { path: "a.txt" } }] },
											{ sources: ["self-speculation"], tool_calls: [{ name: "read", arguments: "bad" }] },
											{ sources: ["drafter"], tool_calls: [{ name: "read", arguments: { path: "ignored.txt" } }] },
											{ sources: ["self-speculation"], tool_calls: [{ name: "write", arguments: { path: "b.txt" } }] },
										],
									},
								},
							}
						: {},
				),
			),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		const actions = actionBridge.waitForCandidates("turn-1", new AbortController().signal);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));

		expect(await actions).toEqual([
			{ tool: "read", input: { path: "a.txt" } },
			{ tool: "write", input: { path: "b.txt" } },
		]);
		await coordinator.dispose();
	});

	it.each([
		["exact-boundary", { token_count: 2, mean: Math.log(0.9), minimum: Math.log(0.9) }, true],
		["next-lower", { token_count: 2, mean: Math.log(0.9), minimum: Math.log(0.8999999999999999) }, false],
		["missing", { token_count: 2, mean: -0.03 }, false],
		["malformed", { token_count: 2, mean: -0.03, minimum: "bad" }, false],
		["positive", { token_count: 2, mean: -0.03, minimum: 0.01 }, false],
		["infinite", { token_count: 2, mean: -0.03, minimum: Number.POSITIVE_INFINITY }, false],
	])("applies the fork confidence gate to %s evidence", async (_label, logprobs, admitted) => {
		const actionBridge = new SelfSpeculationActionBridge();
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => enabledSettings({ forkTransport: "sidecar" }),
			requestID: () => "actor-request",
			actionBridge,
			fetch: vi.fn(async (input) =>
				Response.json(
					new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath
						? forkReceipt("read", { path: "not-executed.txt" }, logprobs)
						: {},
				),
			),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		const actions = actionBridge.waitForCandidates("turn-1", new AbortController().signal);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));

		expect(await actions).toEqual(admitted ? [{ tool: "read", input: { path: "not-executed.txt" } }] : []);
		await coordinator.dispose();
	});

	it.each([
		["disabled", false, forkReceipt("read", { path: "a.txt" })],
		["malformed", true, null],
	])("releases action waiters for %s fork output", async (_label, forkActionEnabled, receipt) => {
		const actionBridge = new SelfSpeculationActionBridge();
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => enabledSettings({ forkTransport: "sidecar", forkActionEnabled }),
			requestID: () => "actor-request",
			actionBridge,
			fetch: vi.fn(async (input) =>
				Response.json(new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath ? receipt : {}),
			),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		const actions = actionBridge.waitForCandidates("turn-1", new AbortController().signal);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));
		expect(await actions).toEqual([]);
		await coordinator.dispose();
	});

	it("matches a fork that completes after the authoritative Actor action", async () => {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => enabledSettings({ forkTransport: "sidecar" }),
			requestID: () => "actor-request",
			fetch: vi.fn(async (input) => {
				if (new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath) await gate;
				return Response.json(forkReceipt("read", { path: "late.txt" }));
			}),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));
		release();
		await vi.waitFor(() => expect(coordinator.snapshot().forkCompletions).toBe(1));
		coordinator.addCandidate(
			candidate("self-speculation", "late-read", "unused", "read", { path: "late.txt" }, 1),
		);
		coordinator.observeActorAction(action("late-read", "unused", "read", { path: "late.txt" }));
		await coordinator.dispose();

		expect(coordinator.snapshot().forkExactMatches).toBe(1);
	});

	it("gates persistently negative forks after warm-up without blocking bounded probes", async () => {
		let requestSequence = 0;
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => enabledSettings({ forkTransport: "sidecar" }),
			requestID: () => `actor-${++requestSequence}`,
			fetch: vi.fn(async (input) =>
				Response.json(
					new URL(String(input)).pathname === SELF_SPECULATION_DEFAULTS.forkPath
						? forkReceipt("read", { path: "never-used.txt" })
						: { ok: true },
				),
			),
		});
		for (let decision = 1; decision <= 4; decision++) {
			coordinator.startTurn(`turn-${decision}`, model(), context(), decision);
			coordinator.decorateActorPayload({ prompt: "P" });
			coordinator.observeActorOutput(delta("thinking_delta", "reason"));
			await vi.waitFor(() => expect(coordinator.snapshot().forkCompletions).toBe(decision));
			coordinator.endTurn();
		}

		coordinator.startTurn("turn-5", model(), context(), 5);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("thinking_delta", "reason"));
		expect(coordinator.snapshot()).toMatchObject({ forkRequests: 4, forkGateSkips: 1, forkGateSamples: 4 });
		await coordinator.dispose();
	});

	it("contains control-plane failures instead of rejecting Actor cleanup", async () => {
		const settings = enabledSettings({ forkEnabled: false });
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => settings,
			requestID: () => "actor-request",
			fetch: vi.fn(async () => new Response("failure", { status: 503 })),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 1));
		coordinator.decorateActorPayload({});

		await expect(coordinator.dispose()).resolves.toBeUndefined();
		expect(coordinator.snapshot()).toEqual(
			expect.objectContaining({ failures: 2, lastError: "self-speculation control plane returned HTTP 503" }),
		);
	});

	it("waits for an in-flight sidecar fork before clearing its request", async () => {
		const paths: string[] = [];
		let releaseFork!: () => void;
		const forkGate = new Promise<void>((resolve) => {
			releaseFork = resolve;
		});
		const settings = enabledSettings({ forkTransport: "sidecar" });
		const coordinator = new SelfSpeculationCoordinator({
			settings: () => settings,
			requestID: () => "actor-request",
			fetch: vi.fn(async (input) => {
				const path = new URL(String(input)).pathname;
				paths.push(path);
				if (path === SELF_SPECULATION_DEFAULTS.forkPath) await forkGate;
				return Response.json({ ok: true });
			}),
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));
		const disposed = coordinator.dispose();
		await vi.waitFor(() => expect(paths).toContain(SELF_SPECULATION_DEFAULTS.forkPath));
		expect(paths).not.toContain(SELF_SPECULATION_DEFAULTS.clearPath);

		releaseFork();
		await disposed;
		expect(paths.at(-1)).toBe(SELF_SPECULATION_DEFAULTS.clearPath);
	});

	it("routes future K(a) only to its expected Actor decision", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-1", "actor-2"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.decorateActorPayload({ model: "actor" });
		coordinator.addCandidate(candidate("pattern-aware", "key-a", "hash-a", "read", { path: "a.txt" }, 0.9, 2));
		await Promise.resolve();

		expect(
			requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath),
		).toHaveLength(0);

		coordinator.endTurn();
		coordinator.startTurn("turn-2", model(), context(), 2);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundles = requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundles).toHaveLength(1);
		expect(bundles[0]?.body).toMatchObject({
			request_id: "actor-2",
			candidates: [
				expect.objectContaining({
					id: actionIdentity("key-a"),
					score: expect.objectContaining({ expected_decision_sequence: 2, latest_decision_sequence: 2 }),
				}),
			],
		});
	});

	it("carries the same decision bundle across a provider retry", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-1", "actor-retry"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 0.8));
		coordinator.decorateActorPayload({ model: "actor" });
		coordinator.endTurn();

		coordinator.startTurn("turn-retry", model(), context(), 1);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundles = requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundles.map((request) => request.body.request_id).sort()).toEqual(["actor-1", "actor-retry"]);
	});
});

interface CapturedRequest {
	readonly path: string;
	readonly body: Record<string, any>;
}

function coordinatorFixture(
	requests: CapturedRequest[],
	overrides: Partial<SelfSpeculationSettings>,
	requestIDs: string[],
	response?: (request: CapturedRequest) => unknown,
): SelfSpeculationCoordinator {
	const settings = enabledSettings(overrides);
	return new SelfSpeculationCoordinator({
		settings: () => settings,
		requestID: () => requestIDs.shift() ?? "unexpected-request",
		fetch: vi.fn(async (input, init) => {
			const request = {
				path: new URL(String(input)).pathname,
				body: JSON.parse(String(init?.body)) as Record<string, any>,
			};
			requests.push(request);
			return Response.json(response?.(request) ?? { ok: true });
		}),
	});
}

function forkReceipt(
	tool: string,
	input: Record<string, unknown>,
	logprobs: unknown = { token_count: 2, mean: -0.03, minimum: Math.log(0.95) },
): Record<string, unknown> {
	return {
		registered: true,
		draft_token_count: 12,
		accepted_token_count: 3,
		details: {
			bundle: {
				candidates: [
					{
						sources: ["drafter", "self-speculation"],
						tool_calls: [{ name: tool, arguments: input }],
						fork: { total_ms: 25, logprobs },
					},
				],
			},
		},
	};
}

function enabledSettings(overrides: Partial<SelfSpeculationSettings>): SelfSpeculationSettings {
	return { ...SELF_SPECULATION_DEFAULTS, enabled: true, ...overrides };
}

function candidate(
	source: string,
	key: string,
	hash: string,
	tool: string,
	input: Record<string, unknown>,
	conditionalProbability: number,
	expectedDecisionSequence = 1,
	latestDecisionSequence = expectedDecisionSequence,
	executionKey = key,
): MaterializedSpeculativeCandidate<string> {
	return {
		sessionID: "session-1",
		turnID: "turn-1",
		expectedDecisionSequence,
		latestDecisionSequence,
		source,
		proposalID: `proposal-${source}`,
		actionID: `action-${source}`,
		tool,
		input,
		predictedAction: action(key, hash, tool, input),
		executionAction: action(executionKey, hash, tool, input),
		depth: 0,
		horizon: 0,
		conditionalProbability,
		empiricalProbability: conditionalProbability,
		expectedLatencyBenefitMs: 100,
		expectedDurationMs: 200,
	};
}

function actionIdentity(key: string): string {
	return `action:v1:${createHash("sha256").update(key).digest("hex")}`;
}

function predictionFeedback(source: string, adopted: boolean, sequence: number): PredictionFeedback<string> {
	const base = {
		sessionID: "session-1",
		turnID: "turn-1",
		tool: "read",
	};
	const settlementBase = {
		prediction: {
			id: `${source}:${sequence}`,
			source,
			proposalID: `${source}:proposal`,
			actionID: `${source}:action`,
		},
		observation: "observed" as const,
		actorAction: { id: `actor:${sequence}`, sequence, decisionSequence: 1, turnID: "turn-1" },
	};
	return adopted
		? {
				...base,
				settlement: {
					...settlementBase,
					match: {
						matched: true,
						relation: { kind: "exact", distance: 0 },
						adoption: { status: "adopted", candidateID: `candidate:${sequence}` },
					},
				},
			}
		: { ...base, settlement: { ...settlementBase, match: { matched: false } } };
}

function action(key: string, hash: string, tool: string, input: Record<string, unknown>): ActionKey {
	return {
		key,
		hash,
		tool,
		input,
		resources: [],
		semanticsEpoch: "test",
		schemaHash: "schema",
		executionFingerprint: "executor",
	};
}

function model(): Model<Api> {
	return {
		id: "actor-model",
		name: "Actor",
		api: "openai-responses",
		provider: "openai",
		baseUrl: "http://localhost:8000/v1",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 8_192,
		maxTokens: 1_024,
	};
}

function context(): Context {
	return { systemPrompt: "system", messages: [], tools: [] };
}

function delta(type: "text_delta" | "thinking_delta", value: string): AssistantMessageEvent {
	return { type, contentIndex: 0, delta: value, partial: undefined as never };
}
