import { deferred } from "./async.ts";
import { createHash } from "node:crypto";
import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import type { ActionKey } from "../src/action-semantics.ts";
import type { MaterializedSpeculativeCandidate, PredictionFeedback } from "../src/runtime.ts";
import { createActorForkPlanSource } from "../src/actor-fork-plan-source.ts";
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
		expect(
			normalizeSelfSpeculationSettings({
				draftBoundary: "[TOOLS]",
				forkForcedPrefix: "[TOOLS] name=",
			}),
		).toMatchObject({ draftBoundary: "[TOOLS]", forkForcedPrefix: "[TOOLS] name=" });
		expect(
			normalizeSelfSpeculationSettings({
				actorProfile: " qwen35_xml ",
			}),
		).toMatchObject({
			actorProfile: "qwen35_xml",
			draftFormat: "auto",
		});
	});

	it.each([false, true])("buffers an ordered Actor bundle while preserving predicted identity (covering=%s)", async (covering) => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		const candidates = covering ? [
			candidate("drafter", "predicted-a", "unused-a", "read", { path: "a.txt", offset: 1 }, 0.8, 1, 1, "covering"),
			candidate("pattern-aware", "predicted-b", "unused-b", "read", { path: "a.txt", offset: 5 }, 0.7, 1, 1, "covering"),
		] : [
			candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 0.7),
			candidate("pattern-aware", "key-a", "hash-a", "read", { path: "a.txt" }, 0.9),
			candidate("drafter", "key-b", "hash-b", "read", { path: "b.txt" }, 0.8),
		];
		for (const value of candidates) coordinator.addCandidate(value);

		expect(requests).toHaveLength(0);
		const actorPayload = coordinator.decorateActorPayload({ model: "actor" }) as Record<string, unknown>;
		expect(actorPayload).toEqual(
			expect.objectContaining({
				request_id: "actor-request",
				self_speculation: expect.objectContaining({ fork: false }),
			}),
		);
		expect(actorPayload.self_speculation).not.toHaveProperty("role");
		await coordinator.dispose();

		const bundle = requests.find((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundle?.body).toMatchObject({
			version: 2,
			request_id: "actor-request",
			max_draft_tokens: SELF_SPECULATION_DEFAULTS.maxDraftTokens,
			actor_profile: "auto",
		});
		expect(bundle?.body).not.toHaveProperty("format");
		expect(bundle?.body).not.toHaveProperty("boundary");
		expect(bundle?.body.candidates).toEqual(covering ? ["predicted-a", "predicted-b"].map((key) => expect.objectContaining({
			id: actionIdentity(key),
			action_identity: expect.objectContaining({ execution_action_id: actionIdentity("covering"), projected: true }),
		})) : [
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

	it("binds the provider self-fork contract once to the stable Actor request", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, {}, ["actor-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);

		const actor = coordinator.decorateActorPayload({ model: "actor" }) as Record<string, unknown>;
		const secondActor = coordinator.decorateActorPayload({ model: "actor-retry" });

		expect(actor.request_id).toBe("actor-request");
		expect(actor.self_speculation).toEqual(
			expect.objectContaining({
				version: 2,
				fork: true,
				actor_profile: "auto",
				d2: {
					confidence_metric: "minimum_tool_name_probability",
					confidence_threshold: 0.9,
					max_attempts: 5,
					retry_token_step: 50,
				},
			}),
		);
		expect(actor.self_speculation).not.toHaveProperty("role");
		expect(actor.self_speculation).not.toHaveProperty("draft_profile");
		expect(actor.self_speculation).not.toHaveProperty("draft_format");
		expect(actor.self_speculation).not.toHaveProperty("draft_boundary");
		expect(actor.self_speculation).not.toHaveProperty("fork_forced_prefix");
		expect(secondActor).toEqual({ model: "actor-retry" });
		await coordinator.dispose();
	});

	it("preserves an explicitly configured legacy tool-call format", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(
			requests,
			{ forkEnabled: false, draftFormat: "qwen_xml" },
			["actor-request"],
		);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "unused", "read", { path: "a.txt" }, 0.9));
		const actor = coordinator.decorateActorPayload({ model: "actor" }) as Record<string, any>;
		await coordinator.dispose();

		expect(actor.self_speculation.draft_format).toBe("qwen_xml");
		expect(actor.self_speculation.version).toBe(1);
		expect(actor.self_speculation).not.toHaveProperty("actor_profile");
		const candidateRequest = requests.find(
			(request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath,
		);
		expect(candidateRequest?.body.format).toBe("qwen_xml");
	});

	it("passes an explicit Actor Profile through every control path", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(
			requests,
			{
				actorProfile: "qwen35_xml",
				forkTransport: "sidecar",
			},
			["actor-request"],
			(request) =>
				request.path === SELF_SPECULATION_DEFAULTS.forkPath
					? forkReceipt("read", { path: "a.txt" }, undefined, "qwen35_xml")
					: { registered: true, draft_token_count: 3 },
		);
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.addCandidate(candidate("drafter", "key-a", "unused", "read", { path: "a.txt" }, 0.9));
		coordinator.decorateActorPayload({ model: "actor" });
		coordinator.observeActorOutput(delta("text_delta", "reason"));
		await coordinator.dispose();

		const candidateRequest = requests.find(
			(request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath,
		);
		const forkRequest = requests.find((request) => request.path === SELF_SPECULATION_DEFAULTS.forkPath);
		expect(candidateRequest?.body).toMatchObject({
			actor_profile: "qwen35_xml",
		});
		expect(forkRequest?.body.options).toMatchObject({
			actor_profile: "qwen35_xml",
		});
		expect(coordinator.snapshot()).toMatchObject({
			resolvedActorProfile: "qwen35_xml",
			profileResolutionSource: "explicit",
		});
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

	it.each(["malformed verification", "HTTP failure"])("contains %s without breaking cleanup", async (scenario) => {
		const malformed = scenario === "malformed verification";
		const coordinator = coordinatorFixture([], { forkEnabled: false }, ["actor-request"], () => malformed
			? { verification: { num_draft_tokens: 2, num_accepted_draft_tokens: 2, num_rejected_draft_tokens: 1 } }
			: new Response("failure", { status: 503 }));
		coordinator.startTurn("turn-1", model(), context(), 1);
		if (!malformed) coordinator.addCandidate(candidate("drafter", "key-a", "hash-a", "read", { path: "a.txt" }, 1));
		coordinator.decorateActorPayload({ model: "actor" });

		await expect(coordinator.dispose()).resolves.toBeUndefined();

		expect(coordinator.snapshot()).toMatchObject({
			verificationRequests: 0,
			failures: malformed ? 1 : 2,
			lastError: malformed ? "self-speculation verification token counts are inconsistent"
				: "self-speculation control plane returned HTTP 503",
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
			options: {
				actor_profile: "auto",
				decoder: "auto",
			},
		});
		expect(forks[0]?.body.options).not.toHaveProperty("forced_prefix");
		expect(forks[0]?.body.options).not.toHaveProperty("draft_boundary");
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

	it("deduplicates valid sidecar batches while preserving their calls and candidate evidence", async () => {
		const candidate = {
			candidate_ids: ["fork-candidate"], sources: ["self-speculation", "drafter"],
			provenance: [{ proposalID: "p", actionID: "a" }], action_identities: [{ predicted_action_id: "predicted" }],
			draft_token_count: 18, score: { joint_speculation_probability: 0.72 },
			tool_calls: [
				{ name: "read", arguments: { path: "a.txt" }, index: 0, call_id: "call-a", format: "structured" },
				{ name: "read", arguments: { path: "b.txt" }, index: 1 },
			],
			fork: { total_ms: 25, logprobs: { token_count: 2, mean: Math.log(0.96), minimum: Math.log(0.96),
				tool_name: { token_count: 1, matched_calls: 1, minimum_probability: 0.96 } } },
		};
		const receipt = { details: { bundle: { candidates: [
			candidate, candidate,
			{ sources: ["self-speculation"], tool_calls: [{ name: "read", arguments: "bad" }] },
			{ sources: ["self-speculation"], tool_calls: [
				{ name: "read", arguments: { path: "partial.txt" } }, { name: "read", arguments: "bad" },
			] },
			{ sources: ["drafter"], tool_calls: [{ name: "read", arguments: { path: "ignored.txt" } }] },
			{ sources: ["self-speculation"], tool_calls: [{ name: "write", arguments: { path: "b.txt" } }] },
		] } } };
		const { actions, batches, coordinator } = await forkActionFixture({ forkActionMinConfidence: 0, maxCandidates: 2 }, receipt);
		try {
			expect(actions).toEqual([
				{ tool: "read", input: { path: "a.txt" } }, { tool: "read", input: { path: "b.txt" } },
				{ tool: "write", input: { path: "b.txt" } },
			]);
			expect(batches).toHaveLength(2);
			expect(batches[0]!.calls).toEqual([
				{ id: "0:fork", index: 0, callID: "call-a", format: "structured", tool: "read", input: { path: "a.txt" } },
				{ id: "1:fork", index: 1, tool: "read", input: { path: "b.txt" } },
			]);
			expect(batches[0]!.evidence).toHaveLength(2);
			for (const evidence of batches[0]!.evidence) expect(evidence).toMatchObject({
				candidateIDs: ["fork-candidate"], sources: ["self-speculation", "drafter"],
				provenance: [{ proposalID: "p", actionID: "a" }], actionIdentities: [{ predicted_action_id: "predicted" }],
				draftTokenCount: 18, score: { joint_speculation_probability: 0.72 }, fork: { total_ms: 25 },
			});
			expect(batches[0]!.evidence[0]!.confidence).toBeCloseTo(0.96);
		} finally { await coordinator.dispose(); }
	});

	it.each([
		["exact-boundary", { token_count: 2, mean: Math.log(0.9), tool_name: { minimum_probability: 0.9 } }, true],
		["next-lower", { token_count: 2, mean: Math.log(0.9), tool_name: { minimum_probability: 0.8999999999999999 } }, false],
		["missing", { token_count: 2, mean: -0.03 }, false],
		["malformed", { token_count: 2, mean: -0.03, tool_name: { minimum_probability: "bad" } }, false],
		["above-one", { token_count: 2, mean: -0.03, tool_name: { minimum_probability: 1.01 } }, false],
		["infinite", { token_count: 2, mean: -0.03, tool_name: { minimum_probability: Number.POSITIVE_INFINITY } }, false],
	])("applies the fork confidence gate to %s evidence", async (_label, logprobs, admitted) => {
		const { actions, coordinator } = await forkActionFixture(
			{},
			forkReceipt("read", { path: "not-executed.txt" }, logprobs),
		);

		expect(actions).toEqual(admitted ? [{ tool: "read", input: { path: "not-executed.txt" } }] : []);
		await coordinator.dispose();
	});

	it("re-probes the newest Actor snapshot after low-confidence D2 output", async () => {
		const requests: CapturedRequest[] = [];
		const actorForkPlans = createActorForkPlanSource({ maxAttempts: 3, retryStreamUpdates: 1 });
		let probe = 0;
		const coordinator = coordinatorFixture(requests, { forkTransport: "sidecar" }, ["actor-request"], (request) => {
			if (request.path !== SELF_SPECULATION_DEFAULTS.forkPath) return {};
			probe++;
			return forkReceipt("read", { path: probe === 1 ? "early.txt" : "stable.txt" }, {
				token_count: 2,
				mean: -0.1,
				tool_name: { minimum_probability: probe === 1 ? 0.5 : 0.95 },
			});
		}, actorForkPlans);
		coordinator.startTurn("turn-d2", model(), context(), 1);
		const pending = actorForkPlans.waitForBatches("turn-d2", new AbortController().signal);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("thinking_delta", "first"));
		await vi.waitFor(() => expect(coordinator.snapshot().forkCompletions).toBe(1));
		coordinator.observeActorOutput(delta("thinking_delta", " later"));

		const batches = await pending;
		expect(batches[0]?.calls[0]?.input).toEqual({ path: "stable.txt" });
		expect(requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.forkPath).map((request) => request.body.snapshot)).toMatchObject([
			{ attempt: 1, generated_text: "first" },
			{ attempt: 2, generated_text: "first later" },
		]);
		expect(coordinator.snapshot()).toMatchObject({ forkRequests: 2, forkCompletions: 2, forkRetries: 1 });
		await coordinator.dispose();
	});

	it.each([
		["disabled", false, forkReceipt("read", { path: "a.txt" })],
		["malformed", true, null],
	])("releases action waiters for %s fork output", async (_label, forkActionEnabled, receipt) => {
		const { actions, coordinator } = await forkActionFixture({ forkActionEnabled }, receipt);
		expect(actions).toEqual([]);
		await coordinator.dispose();
	});

	it("keeps censored adoptions eligible and gates measured negative forks after warm-up", async () => {
		for (const expectedActorMs of [undefined, 0]) {
			const coordinator = coordinatorFixture([], { forkTransport: "sidecar" },
				Array.from({ length: 5 }, (_, index) => `actor-${index + 1}`),
				(request) => request.path === SELF_SPECULATION_DEFAULTS.forkPath ? forkReceipt("read", { path: "a.txt" }) : {});
			for (let decision = 1; decision <= 4; decision++) {
				coordinator.startTurn(`turn-${decision}`, model(), context(), decision);
				coordinator.decorateActorPayload({ prompt: "P" });
				coordinator.observeActorOutput(delta("thinking_delta", "reason"));
				await vi.waitFor(() => expect(coordinator.snapshot().forkCompletions).toBe(decision));
				coordinator.observeActorSettlement({
					actorAction: { id: `actor-${decision}`, sequence: decision, turnID: `turn-${decision}` }, tool: "read", rejections: [],
					matchedPredictions: [predictionFeedback("self-speculation", true, decision).settlement.prediction],
					provider: { kind: "speculative", candidateID: "candidate", match: { kind: "exact", distance: 0 },
						timing: { executionAheadMs: 10000, attemptLeadMs: 20000, hitLatencyMs: 100, expectedActorMs },
						toolExecution: { startedAt: 0, completedAt: 10000 } },
				});
				coordinator.endTurn();
			}

			coordinator.startTurn("turn-5", model(), context(), 5);
			coordinator.decorateActorPayload({ prompt: "P" });
			coordinator.observeActorOutput(delta("thinking_delta", "reason"));
			await vi.waitFor(() => expect(coordinator.snapshot()).toMatchObject({
				forkRequests: expectedActorMs === undefined ? 5 : 4, forkGateSkips: expectedActorMs === undefined ? 0 : 1, forkGateSamples: 4,
			}));
			await coordinator.dispose();
		}
	});

	it("waits for an in-flight sidecar fork before clearing its request", async () => {
		const requests: CapturedRequest[] = [];
		const { promise: forkGate, resolve: releaseFork } = deferred();
		const coordinator = coordinatorFixture(requests, { forkTransport: "sidecar" }, ["actor-request"], async (request) => {
			if (request.path === SELF_SPECULATION_DEFAULTS.forkPath) await forkGate;
		});
		coordinator.startTurn("turn-1", model(), context(), 1);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));
		const disposed = coordinator.dispose();
		await vi.waitFor(() => expect(requests).toContainEqual(expect.objectContaining({ path: SELF_SPECULATION_DEFAULTS.forkPath })));
		expect(requests).not.toContainEqual(expect.objectContaining({ path: SELF_SPECULATION_DEFAULTS.clearPath }));

		releaseFork();
		await disposed;
		expect(requests.at(-1)?.path).toBe(SELF_SPECULATION_DEFAULTS.clearPath);
	});

	it("cancels an in-flight sidecar probe when Runtime stops waiting for its source", async () => {
		const actorForkPlans = createActorForkPlanSource();
		const coordinator = coordinatorFixture([], { forkTransport: "sidecar" }, ["actor-request"], async (request, init) => {
			if (request.path !== SELF_SPECULATION_DEFAULTS.forkPath) return {};
			return await new Promise<Response>((_resolve, reject) => {
				init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
			});
		}, actorForkPlans);
		coordinator.startTurn("turn-1", model(), context(), 1);
		const runtime = new AbortController();
		const pending = actorForkPlans.waitForBatches("turn-1", runtime.signal);
		coordinator.decorateActorPayload({ prompt: "P" });
		coordinator.observeActorOutput(delta("text_delta", "x"));
		await vi.waitFor(() => expect(coordinator.snapshot().forkRequests).toBe(1));

		runtime.abort();
		expect(await pending).toEqual([]);
		await coordinator.dispose();
		expect(coordinator.snapshot()).toMatchObject({ failures: 0, forkCompletions: 0 });
	});

	it.each(["future", "retry"] as const)("retains a decision bundle until its %s Actor request", async (mode) => {
		const requests: CapturedRequest[] = [], future = mode === "future";
		const nextID = future ? "actor-2" : "actor-retry";
		const coordinator = coordinatorFixture(requests, { forkEnabled: false }, ["actor-1", nextID]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		if (future) coordinator.decorateActorPayload({ model: "actor" });
		coordinator.addCandidate(candidate(future ? "pattern-aware" : "drafter", "key-a", "hash-a", "read",
			{ path: "a.txt" }, 0.9, future ? 2 : 1));
		if (future) {
			await Promise.resolve();
			expect(requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath)).toHaveLength(0);
		} else coordinator.decorateActorPayload({ model: "actor" });
		coordinator.endTurn();
		coordinator.startTurn(future ? "turn-2" : "turn-retry", model(), context(), future ? 2 : 1);
		coordinator.decorateActorPayload({ model: "actor" });
		await coordinator.dispose();

		const bundles = requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath);
		expect(bundles.map((request) => request.body.request_id).sort()).toEqual(future ? [nextID] : ["actor-1", nextID]);
		if (future) expect(bundles[0]!.body.candidates).toMatchObject([{
			id: actionIdentity("key-a"), score: { expected_decision_sequence: 2, latest_decision_sequence: 2 },
		}]);
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
	response?: (request: CapturedRequest, init?: RequestInit) => unknown,
	actorForkPlanSource?: ReturnType<typeof createActorForkPlanSource>,
): SelfSpeculationCoordinator {
	const settings = enabledSettings(overrides);
	return new SelfSpeculationCoordinator({
		settings: () => settings,
		requestID: () => requestIDs.shift() ?? "unexpected-request",
		actorForkPlanSource,
		fetch: async (input, init) => {
			const request = {
				path: new URL(String(input)).pathname,
				body: JSON.parse(String(init?.body)),
			};
			requests.push(request);
			const result = await response?.(request, init);
			return result instanceof Response ? result : Response.json(result ?? { ok: true });
		},
	});
}

async function forkActionFixture(overrides: Partial<SelfSpeculationSettings>, receipt: unknown) {
	const actorForkPlans = createActorForkPlanSource({ maxAttempts: 1 });
	const coordinator = coordinatorFixture([], { forkTransport: "sidecar", ...overrides }, ["actor-request"],
		(request) => request.path === SELF_SPECULATION_DEFAULTS.forkPath ? receipt : {}, actorForkPlans);
	coordinator.startTurn("turn-1", model(), context(), 1);
	const pending = actorForkPlans.waitForBatches("turn-1", new AbortController().signal);
	coordinator.decorateActorPayload({ prompt: "P" });
	coordinator.observeActorOutput(delta("text_delta", "x"));
	const batches = await pending;
	return { actions: batches.flatMap((batch) => batch.calls.map(({ tool, input }) => ({ tool, input }))), batches, coordinator };
}

function forkReceipt(
	tool: string,
	input: Record<string, unknown>,
	logprobs: unknown = { token_count: 2, mean: -0.03, tool_name: { minimum_probability: 0.95 } },
	profile?: string,
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
						...(profile
							? { profile: { profile: { id: profile }, source: "explicit" } }
							: {}),
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
	return {
		sessionID: "session-1",
		turnID: "turn-1",
		tool: "read",
		settlement: {
			prediction: { id: `${source}:${sequence}`, source, proposalID: `${source}:proposal`, actionID: `${source}:action` },
			observation: "observed",
			actorAction: { id: `actor:${sequence}`, sequence, decisionSequence: 1, turnID: "turn-1" },
			...(adopted ? { match: {
				matched: true,
				relation: { kind: "exact", distance: 0 },
				adoption: { status: "adopted", candidateID: `candidate:${sequence}` },
			} } : { match: { matched: false } }),
		},
	};
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
