import { deferred } from "./async.ts";
import { describe, expect, it, vi } from "vitest";
import { type ProcessHandoff, ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import {
	createExecPrototype,
	sealProcessCertificate,
	sha256Digest as digest,
	type ProcessProvenanceCertificate,
} from "../src/provenance-certificate.ts";

const SCOPE = { sessionID: "session", turnID: "turn" };
const OTHER_SCOPE = { sessionID: "session", turnID: "other" };
const livePlan = async (live?: readonly ProcessProvenanceCertificate[]) => live?.[0] && { certificate: live[0] };

describe("ProcessHandoffRegistry", () => {
	it("finds a candidate completed after persistent lookup began", async () => {
		const fixture = await producer();
		const lookupStarted = deferred<void>();
		const releaseLookup = deferred<void>();
		const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[]) => {
			if (live) return livePlan(live);
			lookupStarted.resolve();
			await releaseLookup.promise;
			return undefined;
		});
		const actor = acquireActor(fixture, lookup);

		await lookupStarted.promise;
		await fixture.publish(async () => true);
		releaseLookup.resolve();

		await expect(actor).resolves.toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate }, joined: false });
		expect(lookup).toHaveBeenCalledTimes(2);
	});

	it("publishes memory before noncreating or failed persistence outcomes", async () => {
		for (const [stored, failure] of [[false, undefined], [undefined, new Error("store unavailable")]] as const) {
			const fixture = await producer();
			const persistenceStarted = deferred<void>();
			const persistence = deferred<boolean>();
			const publishing = fixture.publish(() => {
				persistenceStarted.resolve();
				return persistence.promise;
			});
			await persistenceStarted.promise;

			const lookup = vi.fn(livePlan);
			const actor = await acquireActor(fixture, lookup);
			expect(actor).toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate } });
			expect(lookup.mock.calls).toEqual([[[fixture.certificate]]]);

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
			await fixture.publish();
			const validating = deferred<void>(), release = deferred<void>();
			const child = acquireActor(fixture, async (live) => {
				if (!live) return undefined;
				validating.resolve(); await release.promise; return livePlan(live);
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
			await fixture.publish();
			const failure = new Error("commit failed");
			const whole = fixture.ownership.commit(async () => {
				await release.promise; throw disposition ? effectCommitFailure(failure, disposition) : failure;
			});
			await expect(acquireActor(fixture)).resolves.toMatchObject({ kind: "miss" });
			release.resolve(); await expect(whole).rejects.toThrow("commit failed");
			await expect(acquireActor(fixture)).resolves.toMatchObject({ kind: disposition === "recoverable" ? "hit" : "miss" });
		}
		for (const winner of ["whole", "child"]) {
			const first = await producer(true);
			await first.publish();
			const second = await producer(true, first.registry, 1), entered = deferred(), release = deferred();
			await second.publish();
			const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[]) => {
				entered.resolve(); await release.promise; return livePlan(live);
			});
			const pending = acquireActor(first, lookup);
			await entered.promise;
			if (winner === "whole") await second.ownership.commit(async () => undefined);
			else await expect(acquireActor(second)).resolves.toMatchObject({ kind: "hit", plan: { certificate: second.certificate } });
			release.resolve();
			await expect(pending).resolves.toMatchObject({ kind: "hit", plan: { certificate: first.certificate } });
			expect(lookup.mock.calls).toEqual([[[second.certificate, first.certificate]], [[first.certificate]]]);
			await expect(acquireActor(first)).resolves.toMatchObject({ kind: "miss" });
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

		await expect(acquireActor(fixture, async () => undefined, waitForRunning, OTHER_SCOPE))
			.resolves.toEqual({ kind: "miss", joined: false });
		expect(waitForRunning).not.toHaveBeenCalled();

		const lookup = vi.fn(livePlan);
		const sameScope = acquireActor(fixture, lookup, waitForRunning);
		await waitEntered.promise;
		await fixture.publish();
		releaseWait.resolve();
		await expect(sameScope).resolves.toMatchObject({ kind: "hit", joined: true });
		expect(lookup.mock.calls).toEqual([[], [[fixture.certificate]]]);
	});

	it("returns an Actor miss when the running-join deadline wins", async () => {
		const fixture = await producer();
		const waitEntered = deferred<void>();
		const deadline = deferred<void>();
		const actor = acquireActor(fixture, undefined, async () => {
			waitEntered.resolve();
			await deadline.promise;
			return "miss";
		});
		await waitEntered.promise;

		deadline.resolve();
		await expect(actor).resolves.toEqual({ kind: "miss", joined: false });
		fixture.registry.complete(fixture.key, fixture.work);
	});
});

async function producer(oneShot = false, registry = new ProcessHandoffRegistry(8), exitCode = 0) {
	const ownership = new ProcessHandoffOwnership(), certificate = processCertificate(oneShot, exitCode);
	const key = certificate.weakKey;
	const acquired = await registry.acquire({
		key,
		scope: SCOPE,
		role: "producer",
		ownership,
		lookup: async () => undefined,
	});
	if (acquired.kind !== "work") throw new Error("expected process work");
	return { certificate, key, registry, ownership, work: acquired.work,
		publish: (persist = async () => false) => registry.publish(key, acquired.work, certificate, persist) };
}

function acquireActor(fixture: Awaited<ReturnType<typeof producer>>, lookup = livePlan,
	waitForRunning: (running: ProcessHandoff) => Promise<"completed" | "miss"> = running => running.completion.then(() => "completed"), scope = SCOPE) {
	return fixture.registry.acquire({ key: fixture.key, scope, role: "actor", lookup, waitForRunning });
}

function processCertificate(oneShot: boolean, code: number) {
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
		result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code } },
	});
}
