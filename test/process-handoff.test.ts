import { describe, expect, it, vi } from "vitest";
import { ProcessHandoffRegistry } from "../src/process-handoff.ts";
import {
	createExecPrototype,
	sealProcessCertificate,
	sha256Digest,
	type ProcessProvenanceCertificate,
} from "../src/provenance-certificate.ts";

const SCOPE = { sessionID: "session", turnID: "turn" };
const OTHER_SCOPE = { sessionID: "session", turnID: "other" };

describe("ProcessHandoffRegistry", () => {
	it("finds a candidate completed after persistent lookup began", async () => {
		const fixture = await producer();
		const lookupStarted = deferred<void>();
		const releaseLookup = deferred<void>();
		const lookup = vi.fn(async (live?: ProcessProvenanceCertificate) => {
			if (live) return live.id;
			lookupStarted.resolve();
			await releaseLookup.promise;
			return undefined;
		});
		const actor = fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup,
			admitCompleted: () => true,
			waitForRunning: async () => "miss",
		});

		await lookupStarted.promise;
		await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => true);
		releaseLookup.resolve();

		await expect(actor).resolves.toMatchObject({ kind: "hit", plan: fixture.certificate.id, joined: false });
		expect(lookup).toHaveBeenCalledTimes(2);
	});

	it.each([
		["already present", false, undefined],
		["failed", undefined, new Error("store unavailable")],
	] as const)("publishes memory before persistence when the store is %s", async (_name, stored, failure) => {
		const fixture = await producer();
		const persistenceStarted = deferred<void>();
		const persistence = deferred<boolean>();
		const publishing = fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, () => {
			persistenceStarted.resolve();
			return persistence.promise;
		});
		await persistenceStarted.promise;

		const actor = await fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup: async (live) => live?.id,
			admitCompleted: () => true,
			waitForRunning: async () => "miss",
		});
		expect(actor).toMatchObject({ kind: "hit", plan: fixture.certificate.id });

		if (failure) {
			persistence.reject(failure);
			await expect(publishing).rejects.toBe(failure);
		} else {
			persistence.resolve(stored!);
			await expect(publishing).resolves.toBe(stored);
		}
	});

	it("joins only a running handoff in the same scope", async () => {
		const fixture = await producer();
		const parallelProducer = await fixture.registry.acquire({
			key: fixture.key, scope: SCOPE, role: "producer", lookup: async () => undefined,
		});
		expect(parallelProducer.kind).toBe("work");
		if (parallelProducer.kind === "work") fixture.registry.complete(fixture.key, parallelProducer.work);
		const waitEntered = deferred<void>();
		const releaseWait = deferred<void>();
		const waitForRunning = vi.fn(async () => {
			waitEntered.resolve();
			await releaseWait.promise;
			return "completed" as const;
		});

		await expect(fixture.registry.acquire({
			key: fixture.key,
			scope: OTHER_SCOPE,
			role: "actor",
			lookup: async () => undefined,
			admitCompleted: () => true,
			waitForRunning,
		})).resolves.toEqual({ kind: "miss", joined: false });
		expect(waitForRunning).not.toHaveBeenCalled();

		const sameScope = fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup: async (live) => live?.id,
			admitCompleted: () => true,
			waitForRunning,
		});
		await waitEntered.promise;
		await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => false);
		releaseWait.resolve();
		await expect(sameScope).resolves.toMatchObject({ kind: "hit", joined: true });
	});

	it.each(["completion", "deadline"] as const)("settles a running join when %s wins", async (winner) => {
		const fixture = await producer();
		const waitEntered = deferred<void>();
		const deadline = deferred<void>();
		let executions = 1;
		const actor = fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup: async (live) => live?.id,
			admitCompleted: () => true,
			waitForRunning: async (handoff) => {
				waitEntered.resolve();
				if (winner === "completion") {
					await handoff.completion;
					return "completed";
				}
				await deadline.promise;
				return "miss";
			},
		});
		await waitEntered.promise;

		if (winner === "completion") await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => false);
		else deadline.resolve();
		const acquired = await actor;
		if (acquired.kind === "miss") executions++;
		expect(acquired.kind).toBe(winner === "completion" ? "hit" : "miss");
		expect(executions).toBe(winner === "completion" ? 1 : 2);
		if (winner === "deadline") fixture.registry.complete(fixture.key, fixture.work);
	});
});

async function producer() {
	const registry = new ProcessHandoffRegistry(8);
	const certificate = processCertificate();
	const key = certificate.weakKey;
	const acquired = await registry.acquire({
		key,
		scope: SCOPE,
		role: "producer",
		lookup: async () => undefined,
	});
	if (acquired.kind !== "work") throw new Error("expected process work");
	return { certificate, key, registry, work: acquired.work };
}

function processCertificate() {
	const digest = (value: string) => sha256Digest(value);
	return sealProcessCertificate({
		prototype: createExecPrototype({
			executablePath: "/usr/bin/tool",
			executableDigest: digest("tool"),
			argv: ["tool"],
			logicalCwd: "/workspace",
			environment: {},
			umask: 0o22,
			processContextDigest: digest("context"),
			stdin: { type: "closed", eof: true },
			fileDescriptorTableComplete: true,
			inheritedFDs: [],
			platformFingerprint: "linux",
		}),
		producer: {
			observer: { provider: "test", fingerprint: digest("observer") },
			execution: { authority: "speculative", confinement: { provider: "test", fingerprint: digest("sandbox") } },
		},
		dependencyCertificate: { complete: true, dependencies: [], taints: [] },
		result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
	});
}

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((accept, fail) => {
		resolve = accept;
		reject = fail;
	});
	return { promise, resolve, reject };
}
