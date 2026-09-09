import { describe, expect, it, vi } from "vitest";
import { ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
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
		const actor = acquireActor(fixture, lookup);

		await lookupStarted.promise;
		await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => true);
		releaseLookup.resolve();

		await expect(actor).resolves.toMatchObject({ kind: "hit", plan: fixture.certificate.id, joined: false });
		expect(lookup).toHaveBeenCalledTimes(2);
	});

	it("publishes memory before noncreating or failed persistence outcomes", async () => {
		for (const [stored, failure] of [[false, undefined], [undefined, new Error("store unavailable")]] as const) {
			const fixture = await producer();
			const persistenceStarted = deferred<void>();
			const persistence = deferred<boolean>();
			const publishing = fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, () => {
				persistenceStarted.resolve();
				return persistence.promise;
			});
			await persistenceStarted.promise;

			const actor = await acquireActor(fixture);
			expect(actor).toMatchObject({ kind: "hit", plan: fixture.certificate.id });

			if (failure) {
				persistence.reject(failure);
				await expect(publishing).rejects.toBe(failure);
			} else {
				persistence.resolve(stored!);
				await expect(publishing).resolves.toBe(stored);
			}
		}
	});

	it("arbitrates whole and child ownership across validation and commit, retaining repeatable results", async () => {
		for (const oneShot of [true, false]) for (const wholeFirst of [true, false]) {
			const fixture = await producer(oneShot);
			await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => false);
			const validating = deferred<void>(), release = deferred<void>();
			const child = acquireActor(fixture, async (live) => {
				if (!live) return undefined;
				validating.resolve(); await release.promise; return live.id;
			});
			await validating.promise;
			const effects = vi.fn(async () => "whole");
			if (wholeFirst) await expect(fixture.ownership.commit(effects)).resolves.toBe("whole");
			release.resolve();
			await expect(child).resolves.toMatchObject({ kind: oneShot && wholeFirst ? "miss" : "hit" });
			if (!wholeFirst) {
				const whole = fixture.ownership.commit(effects);
				if (oneShot) await expect(whole).rejects.toMatchObject({ disposition: "recoverable" });
				else await expect(whole).resolves.toBe("whole");
			}
			expect(effects).toHaveBeenCalledTimes(oneShot && !wholeFirst ? 0 : 1);
		}
		for (const disposition of ["recoverable", "poisoned", undefined] as const) {
			const fixture = await producer(true), release = deferred<void>();
			await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => false);
			const failure = new Error("commit failed");
			const whole = fixture.ownership.commit(async () => {
				await release.promise; throw disposition ? effectCommitFailure(failure, disposition) : failure;
			});
			await expect(acquireActor(fixture)).resolves.toMatchObject({ kind: "miss" });
			release.resolve(); await expect(whole).rejects.toThrow("commit failed");
			await expect(acquireActor(fixture)).resolves.toMatchObject({ kind: disposition === "recoverable" ? "hit" : "miss" });
		}
	});

	it("joins only a running handoff in the same scope", async () => {
		const fixture = await producer();
		const parallelProducer = await fixture.registry.acquire({
			key: fixture.key, scope: SCOPE, role: "producer", ownership: new ProcessHandoffOwnership(), lookup: async () => undefined,
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
			waitForRunning,
		})).resolves.toEqual({ kind: "miss", joined: false });
		expect(waitForRunning).not.toHaveBeenCalled();

		const sameScope = fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup: async (live) => live?.id,
			waitForRunning,
		});
		await waitEntered.promise;
		await fixture.registry.publish(fixture.key, fixture.work, fixture.certificate, async () => false);
		releaseWait.resolve();
		await expect(sameScope).resolves.toMatchObject({ kind: "hit", joined: true });
	});

	it("returns an Actor miss when the running-join deadline wins", async () => {
		const fixture = await producer();
		const waitEntered = deferred<void>();
		const deadline = deferred<void>();
		const actor = fixture.registry.acquire({
			key: fixture.key,
			scope: SCOPE,
			role: "actor",
			lookup: async (live) => live?.id,
			waitForRunning: async () => {
				waitEntered.resolve();
				await deadline.promise;
				return "miss";
			},
		});
		await waitEntered.promise;

		deadline.resolve();
		await expect(actor).resolves.toEqual({ kind: "miss", joined: false });
		fixture.registry.complete(fixture.key, fixture.work);
	});
});

async function producer(oneShot = false) {
	const registry = new ProcessHandoffRegistry(8);
	const ownership = new ProcessHandoffOwnership(), certificate = processCertificate(oneShot);
	const key = certificate.weakKey;
	const acquired = await registry.acquire({
		key,
		scope: SCOPE,
		role: "producer",
		ownership,
		lookup: async () => undefined,
	});
	if (acquired.kind !== "work") throw new Error("expected process work");
	return { certificate, key, registry, ownership, work: acquired.work };
}

function acquireActor(fixture: Awaited<ReturnType<typeof producer>>, lookup = async (live?: ProcessProvenanceCertificate) => live?.id) {
	return fixture.registry.acquire({ key: fixture.key, scope: SCOPE, role: "actor", lookup,
		waitForRunning: async (running) => { await running.completion; return "completed"; } });
}

function processCertificate(oneShot: boolean) {
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
		dependencyCertificate: { complete: true, dependencies: [], taints: oneShot ? ["random"] : [] },
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
