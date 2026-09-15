import { gated, deferred } from "./async.ts";
import { processPrototype, processCertificate as sealFixture } from "./process-fixture.ts";
import { describe, expect, it, vi } from "vitest";
import { type ProcessHandoff, ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import {
	sha256Digest as digest,
	type ProcessProvenanceCertificate,
	type Sha256Digest,
} from "../src/provenance-certificate.ts";

const SCOPE = { sessionID: "session", turnID: "turn" };
const OTHER_SCOPE = { sessionID: "session", turnID: "other" };
const livePlan = async (live?: readonly ProcessProvenanceCertificate[]) => live?.[0] && { certificate: live[0] };

describe("ProcessHandoffRegistry", () => {
	it("excludes attempted disk copies and still finds a candidate completed during history lookup", async () => {
		const previous = await producer();
		await previous.publish();
		const fixture = await producer(false, previous.registry, 1);
		const lookupGate = gated();
		const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[], excluded?: ReadonlySet<Sha256Digest>) => {
			if (live) return live[0] === fixture.certificate ? livePlan(live) : undefined;
			expect(excluded).toEqual(new Set([previous.certificate.id]));
			await lookupGate.wait();
			return undefined;
		});
		const actor = acquireActor(fixture, lookup);

		await lookupGate.entered;
		await fixture.publish(async () => true);
		lookupGate.release();

		await expect(actor).resolves.toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate }, joined: false });
		expect(lookup).toHaveBeenCalledTimes(3);
		await expect(acquireActor(previous, live => livePlan(live?.filter(candidate => candidate === previous.certificate))))
			.resolves.toMatchObject({ kind: "hit", plan: { certificate: previous.certificate } });
	});

	it("publishes memory before noncreating or failed persistence outcomes", async () => {
		for (const scope of [SCOPE, OTHER_SCOPE]) for (const [stored, failure] of [[false, undefined], [undefined, new Error("store unavailable")]] as const) {
			const fixture = await producer();
			expect(fixture.registry.hasResults).toBe(true);
			const persistenceStarted = deferred<void>();
			const persistence = deferred<boolean>();
			const publishing = fixture.publish(() => {
				persistenceStarted.resolve();
				return persistence.promise;
			});
			await persistenceStarted.promise;
			expect(fixture.registry.hasResults).toBe(true);

			const lookup = vi.fn(livePlan);
			const actor = await acquireActor(fixture, lookup, undefined, scope);
			expect(actor).toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate }, producer: fixture.work });
			expect(fixture.registry.hasResults).toBe(true);
			expect(lookup.mock.calls).toEqual([[[fixture.certificate]]]);

			if (failure) {
				persistence.reject(failure);
				await expect(publishing).rejects.toBe(failure);
			} else {
				persistence.resolve(stored!);
				await expect(publishing).resolves.toBe(stored);
			}
			await expect(acquireActor(fixture, lookup, undefined, scope)).resolves.toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate } });
			fixture.registry.clearCompleted();
			expect(fixture.registry.hasResults).toBe(false);
			await expect(acquireActor(fixture)).resolves.toMatchObject({ kind: "miss" });
		}
	});

	it("retains the selected physical producer when identical evidence is published during validation", async () => {
		const scope = { ...SCOPE }, pending = producer(false, undefined, 0, scope);
		scope.turnID = OTHER_SCOPE.turnID;
		const first = await pending;
		const second = await producer(false, first.registry, 0, OTHER_SCOPE), gate = gated();
		expect(second.certificate.id).toBe(first.certificate.id);
		expect(second.work).not.toBe(first.work);
		await first.publish();
		const actor = acquireActor(first, async live => { await gate.wait(); return livePlan(live); });
		await gate.entered;
		scope.turnID = "later";
		await second.publish(); gate.release();
		const result = await actor;
		expect(result).toMatchObject({ kind: "hit", plan: { certificate: first.certificate }, producer: { scope: SCOPE } });
		if (result.kind !== "hit") throw new Error("expected completed handoff");
		expect(result.producer).toBe(first.work);
		expect(Object.isFrozen(result.producer!.scope)).toBe(true);
		first.registry.clearCompleted();
		await expect(acquireActor(first, async () => ({ certificate: first.certificate })))
			.resolves.toEqual({ kind: "hit", plan: { certificate: first.certificate }, joined: false });
		first.registry.dispose();
	});

	it.each(([
		["clear", "completed"], ["trim", "completed"], ["dispose", "completed"], ["dispose", "history"],
	] as const).flatMap(([operation, phase]) => (phase === "completed" ? [false, true] : [false]).map(oneShot => ({ operation, phase, oneShot }))))(
		"revokes $operation during a pending $phase lookup (one-shot $oneShot)", async ({ operation, phase, oneShot }) => {
		const completed = phase === "completed";
		const fixture = await producer(oneShot), gate = gated();
		if (completed) await fixture.publish();
		const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[]) => {
			if (completed && !live) return undefined;
			await gate.wait();
			return completed ? livePlan(live) : { certificate: fixture.certificate };
		});
		const actor = acquireActor(fixture, lookup);
		await gate.entered;
		if (operation === "clear") fixture.registry.clearCompleted();
		else if (operation === "trim") fixture.registry.configure(0);
		else fixture.registry.dispose();
		gate.release();
		await expect(actor).resolves.toEqual({ kind: "miss", joined: false });
		if (completed) await expect(fixture.ownership.commit(async () => "whole")).resolves.toBe("whole");
		else {
			await expect(acquireActor(fixture, lookup)).resolves.toEqual({ kind: "miss", joined: false });
			await expect(fixture.work.completion).resolves.toBeUndefined();
		}
		expect(lookup).toHaveBeenCalledTimes(operation === "dispose" ? 1 : 2);
	});

	it("arbitrates whole and child ownership across validation and commit, retaining repeatable results", async () => {
		for (const oneShot of [true, false]) for (const wholeFirst of [true, false]) {
			const fixture = await producer(oneShot);
			await fixture.publish();
			const validating = deferred<void>(), release = deferred<void>();
			let validations = 0;
			const children = Array.from({ length: 2 }, () => acquireActor(fixture, async (live) => {
				if (!live) return undefined;
				if (++validations === 2) validating.resolve();
				await release.promise; return livePlan(live);
			}));
			await validating.promise;
			const effects = vi.fn(async () => "whole");
			if (wholeFirst) await expect(fixture.ownership.commit(effects)).resolves.toBe("whole");
			release.resolve();
			expect((await Promise.all(children)).map(child => child.kind)).toEqual(oneShot ? [wholeFirst ? "miss" : "hit", "miss"] : ["hit", "hit"]);
			expect(validations).toBe(2);
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
			const second = await producer(true, first.registry, 1), gate = gated();
			await second.publish();
			const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[]) => {
				await gate.wait(); return livePlan(live);
			});
			const pending = acquireActor(first, lookup);
			await gate.entered;
			if (winner === "whole") await second.ownership.commit(async () => undefined);
			else await expect(acquireActor(second)).resolves.toMatchObject({ kind: "hit", plan: { certificate: second.certificate } });
			gate.release();
			await expect(pending).resolves.toMatchObject({ kind: "hit", plan: { certificate: first.certificate } });
			expect(lookup.mock.calls).toEqual([[[second.certificate, first.certificate]], [[first.certificate]]]);
			await expect(acquireActor(first)).resolves.toMatchObject({ kind: "miss" });
		}
	});

	it.each([false, true].flatMap(oneShot => [SCOPE, OTHER_SCOPE].map(scope => ({ oneShot, scope }))))(
		"waits in $scope.turnID and preserves one-shot ownership ($oneShot)", async ({ oneShot, scope }) => {
		const fixture = await producer(oneShot);
		const parallelProducer = await fixture.registry.acquire({
			key: fixture.key, scope: OTHER_SCOPE, role: "producer", ownership: new ProcessHandoffOwnership(), lookup: async () => undefined,
		});
		if (parallelProducer.kind !== "work") throw new Error("independent producers must not wait for each other");
		const waitGate = gated();
		const waitForRunning = vi.fn(async (running: ProcessHandoff) => {
			if (running === parallelProducer.work) {
				fixture.registry.complete(fixture.key, running); // Failed same-scope work must yield to the repeatable candidate.
				return "completed" as const;
			}
			await waitGate.wait();
			return "completed" as const;
		});

		for (const foreign of [undefined, { sessionID: "other", turnID: "turn" }]) await expect(fixture.registry.acquire({
			key: fixture.key, scope: foreign, role: "actor", lookup: async () => undefined, waitForRunning,
		})).resolves.toEqual({ kind: "miss", joined: false });
		expect(waitForRunning).not.toHaveBeenCalled();

		const lookup = vi.fn(livePlan);
		const requestScope = { ...scope };
		const actor = acquireActor(fixture, lookup, waitForRunning, requestScope);
		await waitGate.entered;
		expect(waitForRunning.mock.calls[0]![0]).toBe(scope === SCOPE ? fixture.work : parallelProducer.work);
		requestScope.turnID = scope === SCOPE ? OTHER_SCOPE.turnID : SCOPE.turnID;
		await fixture.publish();
		waitGate.release();
		const transferable = !oneShot || scope === SCOPE;
		await expect(actor).resolves.toMatchObject({ kind: transferable ? "hit" : "miss", joined: true });
		expect(lookup.mock.calls).toEqual([[undefined, new Set()], ...(scope === OTHER_SCOPE ? [[undefined, new Set()]] : []),
			transferable ? [[fixture.certificate]] : [undefined, new Set()]]);
		const whole = fixture.ownership.commit(async () => "whole");
		if (oneShot && transferable) await expect(whole).rejects.toMatchObject({ disposition: "recoverable" });
		else await expect(whole).resolves.toBe("whole");
		fixture.registry.dispose();
	});

	it.each(["deadline", "producer failure", "disposal"].flatMap(phase => [SCOPE, OTHER_SCOPE].map(scope => ({ phase, scope }))))(
		"returns a miss after $phase while waiting in $scope.turnID", async ({ phase, scope }) => {
		const fixture = await producer();
		const deadline = gated();
		const actor = acquireActor(fixture, undefined, async () => {
			await deadline.wait();
			return phase === "deadline" ? "miss" : "completed";
		}, scope);
		await deadline.entered;

		if (phase === "producer failure") fixture.registry.complete(fixture.key, fixture.work);
		else if (phase === "disposal") fixture.registry.dispose();
		deadline.release();
		await expect(actor).resolves.toEqual({ kind: "miss", joined: phase !== "deadline" });
		fixture.registry.dispose();
	});
});

async function producer(oneShot = false, registry = new ProcessHandoffRegistry(8), exitCode = 0, scope = SCOPE) {
	const ownership = new ProcessHandoffOwnership(), certificate = processCertificate(oneShot, exitCode);
	const key = certificate.weakKey;
	const acquired = await registry.acquire({
		key,
		scope,
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
	return sealFixture(processPrototype({
		executablePath: "/usr/bin/tool",
		environment: {},
		processContextDigest: digest("context"),
	}), {
		producer: {
			observer: { provider: "test", fingerprint: digest("observer") },
			execution: { authority: "speculative", confinement: { provider: "test", fingerprint: digest("sandbox") } },
		},
		dependencyCertificate: { complete: true, dependencies: [], taints: oneShot ? ["random"] : [] },
		result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code } },
	});
}
