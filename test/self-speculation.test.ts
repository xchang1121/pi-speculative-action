import type { Api, AssistantMessageEvent, Context, Model } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import type { ActionKey } from "../src/action-semantics.ts";
import type { MaterializedSpeculativeCandidate } from "../src/runtime.ts";
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
			version: 1,
			request_id: "actor-request",
			max_draft_tokens: SELF_SPECULATION_DEFAULTS.maxDraftTokens,
			format: "tagged_json",
			boundary: "<tool_call>",
		});
		expect(bundle?.body.candidates).toEqual([
			expect.objectContaining({
				id: "hash-a",
				sources: ["drafter", "pattern-aware"],
				tool_call: { name: "read", arguments: { path: "a.txt" } },
				score: expect.objectContaining({ conditional_probability: 0.9 }),
			}),
			expect.objectContaining({ id: "hash-b", sources: ["drafter"] }),
		]);
		expect(requests.filter((request) => request.path === SELF_SPECULATION_DEFAULTS.candidatePath)).toHaveLength(1);
		expect(requests.at(-1)).toMatchObject({
			path: SELF_SPECULATION_DEFAULTS.clearPath,
			body: { version: 1, request_id: "actor-request" },
		});
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

	it("requests one sidecar fork from the first Actor output snapshot", async () => {
		const requests: CapturedRequest[] = [];
		const coordinator = coordinatorFixture(requests, { forkTransport: "sidecar" }, ["actor-request"]);
		coordinator.startTurn("turn-1", model(), context(), 1);
		expect(coordinator.decorateActorPayload({ model: "actor", prompt: "PROMPT" })).toEqual({
			model: "actor",
			prompt: "PROMPT",
			request_id: "actor-request",
		});
		expect(coordinator.decorateDrafterPayload({ model: "drafter" })).toEqual({ model: "drafter" });
		coordinator.observeActorOutput(delta("thinking_delta", "reason"));
		coordinator.observeActorOutput(delta("text_delta", "later"));
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
		expect(coordinator.snapshot().forkRequests).toBe(1);
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
					id: "hash-a",
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
): SelfSpeculationCoordinator {
	const settings = enabledSettings(overrides);
	return new SelfSpeculationCoordinator({
		settings: () => settings,
		requestID: () => requestIDs.shift() ?? "unexpected-request",
		fetch: vi.fn(async (input, init) => {
			requests.push({
				path: new URL(String(input)).pathname,
				body: JSON.parse(String(init?.body)) as Record<string, any>,
			});
			return Response.json({ ok: true });
		}),
	});
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
		action: action(key, hash, tool, input),
		depth: 0,
		horizon: 0,
		conditionalProbability,
		empiricalProbability: conditionalProbability,
		expectedLatencyBenefitMs: 100,
		expectedDurationMs: 200,
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
