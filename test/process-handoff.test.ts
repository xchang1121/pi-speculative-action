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
	it.each(["completed", "running"])("transfers %s work across a live consumer's turn and rechecks revocation", async phase => {
		let active = false;
		const acceptScope = (scope: typeof SCOPE) => active && scope.turnID === OTHER_SCOPE.turnID;
		const fixture = await producer(true, undefined, 0, SCOPE, new ProcessHandoffOwnership(undefined, acceptScope));
		if (phase === "completed") await fixture.publish();
		await expect(fixture.actor(undefined, async () => "miss", OTHER_SCOPE)).resolves.toMatchObject({ kind: "miss" });
		active = true;
		await expect(fixture.actor(undefined, undefined, { ...OTHER_SCOPE, sessionID: "foreign" })).resolves.toMatchObject({ kind: "miss" });
		const validation = gated();
		const actor = fixture.actor(async live => {
			if (!live) return;
			await validation.wait(); return livePlan(live);
		}, async () => { await fixture.publish(); return "completed"; }, OTHER_SCOPE);
		await validation.entered;
		active = false; validation.release();
		await expect(actor).resolves.toMatchObject({ kind: "miss" });
		active = true;
		await expect(fixture.actor(undefined, undefined, OTHER_SCOPE)).resolves.toMatchObject({ kind: "hit", producer: fixture.work });
		await expect(fixture.actor(undefined, undefined, OTHER_SCOPE)).resolves.toMatchObject({ kind: "miss" });
		await expect(fixture.ownership.commit(async () => "whole")).rejects.toMatchObject({ disposition: "recoverable" });
		fixture.registry.dispose();
	});
	it("owns bounded launch bindings without persisting secrets or granting result adoption", async () => {
		for (const source of ["sealed", "consumed", "native"]) for (const revoke of ["clear", "count", "bytes", "dispose"] as const) {
			const consumed = source === "consumed", native = source === "native";
			const fixture = await producer(consumed, new ProcessHandoffRegistry<unknown>(8, 100));
			const invocation = { argv: ["sensitive argument"], environment: { TOKEN: "sensitive value" } };
			fixture.registry.bind(fixture.key, fixture.work, invocation);
			expect(fixture.registry.bindings(SCOPE)).toEqual([]); // Unsealed execution is not a binding source.
			if (native) fixture.registry.complete(fixture.key, fixture.work);
			else await fixture.publish();
			const computation = fixture.work.computation;
			expect(Boolean(computation)).toBe(!native);
			if (consumed) {
				await expect(fixture.actor()).resolves.toMatchObject({ kind: "hit" });
				await expect(fixture.actor()).resolves.toMatchObject({ kind: "miss" });
				expect(fixture.work.computation).toBe(computation);
				expect(fixture.registry.hasResults).toBe(false);
				expect(fixture.registry.mayHaveExecutable(fixture.certificate.prototype.executablePath)).toBe(false);
			}
			// Consumption may precede binding publication; a launch capability grants no second result transfer.
			if (native) {
				const observe = (value: unknown, duration = 12) => fixture.registry.observe(fixture.key,
					fixture.certificate.prototype.executablePath, SCOPE, value, duration);
				for (const duration of [-1, NaN, Infinity]) expect(observe(invocation, duration)).toBeUndefined();
				expect(() => observe({ callback: () => {} })).toThrow();
				expect(observe(new Map())).toBeUndefined();
				expect(fixture.registry.bindings(SCOPE)).toEqual([]);
				expect(fixture.registry.hasResults).toBe(false);
				observe(invocation);
			}
			else fixture.registry.bind(fixture.key, fixture.work, invocation);
			const [binding] = fixture.registry.bindings(OTHER_SCOPE);
			expect(binding).toBeDefined();
			expect(binding).not.toHaveProperty("certificate");
			expect(binding!.executionMs).toBe(native ? 12 : fixture.certificate.result.observedProcessMs ?? 0);
			if (native) {
				expect(fixture.registry.hasResults).toBe(false);
				expect(fixture.registry.mayHaveExecutable(fixture.certificate.prototype.executablePath)).toBe(false);
				await expect(fixture.actor()).resolves.toMatchObject({ kind: "miss" });
			}
			expect(binding!.available).toBe(true);
			const owned = fixture.registry.resolveBinding(binding!, OTHER_SCOPE);
			expect(owned).toEqual(invocation);
			invocation.argv[0] = "changed"; invocation.environment.TOKEN = "changed";
			expect(JSON.stringify(owned)).toContain("sensitive value");
			expect(JSON.stringify([binding, fixture.work])).not.toContain("sensitive");
			expect(fixture.registry.resolveBinding({ ...binding! }, SCOPE)).toBeUndefined();
			expect(fixture.registry.resolveBinding(binding!, { sessionID: "other", turnID: "turn" })).toBeUndefined();
			expect(fixture.registry.bindings({ sessionID: "other", turnID: "turn" })).toEqual([]);
			if (revoke === "clear") fixture.registry.clearCompleted();
			else if (revoke === "count") fixture.registry.configure(0);
			else if (revoke === "bytes") {
				const second = await producer(false, fixture.registry, 1);
				await second.publish(); fixture.registry.bind(second.key, second.work, invocation);
				expect(fixture.registry.bindings(SCOPE)).toHaveLength(1);
				fixture.registry.configure(8, 0);
			} else fixture.registry.dispose();
			expect(fixture.registry.resolveBinding(binding!, SCOPE)).toBeUndefined();
			expect(binding!.available).toBe(false);
			expect(fixture.registry.bindings(SCOPE)).toEqual([]);
			expect(JSON.stringify(owned)).toContain("sensitive value"); // An admitted consumer owns its immutable copy.
			if (revoke !== "dispose") {
				fixture.registry.configure(1, 100);
				expect(fixture.registry.observe(digest("refilled"), "/usr/bin/other", SCOPE, invocation, 1)?.available).toBe(true);
			}
			fixture.registry.dispose();
		}
	});

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
		const actor = fixture.actor(lookup);

		await lookupGate.entered;
		await fixture.publish(async () => true);
		lookupGate.release();

		await expect(actor).resolves.toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate }, joined: false });
		expect(lookup).toHaveBeenCalledTimes(3);
		await expect(previous.actor(live => livePlan(live?.filter(candidate => candidate === previous.certificate))))
			.resolves.toMatchObject({ kind: "hit", plan: { certificate: previous.certificate } });
	});

	it("publishes memory before noncreating or failed persistence outcomes", async () => {
		for (const scope of [SCOPE, OTHER_SCOPE]) for (const [stored, failure] of [[false, undefined], [undefined, new Error("store unavailable")]] as const) {
			const fixture = await producer();
			expect(fixture.registry.hasResults).toBe(true);
			expect(fixture.registry.mayHaveExecutable(fixture.certificate.prototype.executablePath)).toBe(true);
			expect(fixture.registry.mayHaveExecutable("/unrelated/executable")).toBe(false);
			const persistenceStarted = deferred<void>();
			const persistence = deferred<boolean>();
			const publishing = fixture.publish(() => {
				persistenceStarted.resolve();
				return persistence.promise;
			});
			await persistenceStarted.promise;
			expect(fixture.registry.hasResults).toBe(true);

			const lookup = vi.fn(livePlan);
			const actor = await fixture.actor(lookup, undefined, scope);
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
			await expect(fixture.actor(lookup, undefined, scope)).resolves.toMatchObject({ kind: "hit", plan: { certificate: fixture.certificate } });
			fixture.registry.clearCompleted();
			expect(fixture.registry.hasResults).toBe(false);
			expect(fixture.registry.mayHaveExecutable(fixture.certificate.prototype.executablePath)).toBe(false);
			await expect(fixture.actor()).resolves.toMatchObject({ kind: "miss" });
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
		const actor = first.actor(async live => { await gate.wait(); return livePlan(live); });
		await gate.entered;
		scope.turnID = "later";
		await second.publish(); gate.release();
		const result = await actor;
		expect(result).toMatchObject({ kind: "hit", plan: { certificate: first.certificate }, producer: { scope: SCOPE } });
		if (result.kind !== "hit") throw new Error("expected completed handoff");
		expect(result.producer).toBe(first.work);
		expect(Object.isFrozen(result.producer!.scope)).toBe(true);
		first.registry.clearCompleted();
		await expect(first.actor(async () => ({ certificate: first.certificate })))
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
		const actor = fixture.actor(lookup);
		await gate.entered;
		if (operation === "clear") fixture.registry.clearCompleted();
		else if (operation === "trim") fixture.registry.configure(0);
		else fixture.registry.dispose();
		gate.release();
		await expect(actor).resolves.toEqual({ kind: "miss", joined: false });
		if (completed) await expect(fixture.ownership.commit(async () => "whole")).resolves.toBe("whole");
		else {
			await expect(fixture.actor(lookup)).resolves.toEqual({ kind: "miss", joined: false });
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
			const children = Array.from({ length: 2 }, () => fixture.actor(async (live) => {
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
			await expect(fixture.actor()).resolves.toMatchObject({ kind: "miss" });
			release.resolve(); await expect(whole).rejects.toThrow("commit failed");
			await expect(fixture.actor()).resolves.toMatchObject({ kind: disposition === "recoverable" ? "hit" : "miss" });
		}
		for (const winner of ["whole", "child"]) {
			const first = await producer(true);
			await first.publish();
			const second = await producer(true, first.registry, 1), gate = gated();
			await second.publish();
			const lookup = vi.fn(async (live?: readonly ProcessProvenanceCertificate[]) => {
				await gate.wait(); return livePlan(live);
			});
			const pending = first.actor(lookup);
			await gate.entered;
			if (winner === "whole") await second.ownership.commit(async () => undefined);
			else await expect(second.actor()).resolves.toMatchObject({ kind: "hit", plan: { certificate: second.certificate } });
			gate.release();
			await expect(pending).resolves.toMatchObject({ kind: "hit", plan: { certificate: first.certificate } });
			expect(lookup.mock.calls).toEqual([[[second.certificate, first.certificate]], [[first.certificate]]]);
			await expect(first.actor()).resolves.toMatchObject({ kind: "miss" });
		}
	});

	it.each(["completed", "deadline", "producer failure", "disposal"].flatMap(phase =>
		(phase === "completed" ? [false, true] : [false]).flatMap(oneShot => [SCOPE, OTHER_SCOPE].map(scope => ({ phase, oneShot, scope })))))(
		"settles $phase in $scope.turnID and preserves one-shot ownership ($oneShot)", async ({ phase, oneShot, scope }) => {
		const fixture = await producer(oneShot);
		const parallelProducer = phase === "completed" ? await fixture.registry.acquire({
			key: fixture.key, scope: OTHER_SCOPE, role: "producer", ownership: new ProcessHandoffOwnership(), lookup: async () => undefined,
			executablePath: fixture.certificate.prototype.executablePath,
		}) : undefined;
		if (parallelProducer && parallelProducer.kind !== "work") throw new Error("independent producers must not wait for each other");
		const waitGate = gated();
		const waitForRunning = vi.fn(async (running: ProcessHandoff) => {
			if (running === parallelProducer?.work) {
				fixture.registry.complete(fixture.key, running); // Failed same-scope work must yield to the repeatable candidate.
				return "completed" as const;
			}
			await waitGate.wait();
			return phase === "deadline" ? "miss" as const : "completed" as const;
		});

		for (const foreign of [undefined, { sessionID: "other", turnID: "turn" }]) await expect(fixture.registry.acquire({
			key: fixture.key, scope: foreign, role: "actor", lookup: async () => undefined, waitForRunning,
		})).resolves.toEqual({ kind: "miss", joined: false });
		expect(waitForRunning).not.toHaveBeenCalled();

		const lookup = vi.fn(livePlan);
		const requestScope = { ...scope };
		const actor = fixture.actor(lookup, waitForRunning, requestScope);
		await waitGate.entered;
		expect(waitForRunning.mock.calls[0]![0]).toBe(scope === OTHER_SCOPE && parallelProducer ? parallelProducer.work : fixture.work);
		requestScope.turnID = scope === SCOPE ? OTHER_SCOPE.turnID : SCOPE.turnID;
		if (phase === "completed") await fixture.publish();
		else if (phase === "producer failure") fixture.registry.complete(fixture.key, fixture.work);
		else if (phase === "disposal") fixture.registry.dispose();
		waitGate.release();
		const transferable = phase === "completed" && (!oneShot || scope === SCOPE);
		await expect(actor).resolves.toMatchObject({ kind: transferable ? "hit" : "miss", joined: phase !== "deadline" });
		if (phase === "completed") expect(lookup.mock.calls).toEqual([[undefined, new Set()], ...(scope === OTHER_SCOPE ? [[undefined, new Set()]] : []),
			transferable ? [[fixture.certificate]] : [undefined, new Set()]]);
		const whole = fixture.ownership.commit(async () => "whole");
		if (oneShot && transferable) await expect(whole).rejects.toMatchObject({ disposition: "recoverable" });
		else await expect(whole).resolves.toBe("whole");
		fixture.registry.dispose();
	});

	it("revokes running input lookups and lets a rejected candidate yield without cancelling its owner", async () => {
		for (const revoke of ["release", "publish", "failure", "dispose"]) {
			const fixture = await producer(), changed = vi.fn(async () => true);
			const release = fixture.registry.observeInputs(fixture.key, fixture.work, changed);
			const borrowed = fixture.work.inputsChanged!;
			await expect(borrowed()).resolves.toBe(true);
			if (revoke === "release") release();
			else if (revoke === "publish") await fixture.publish();
			else if (revoke === "failure") fixture.registry.complete(fixture.key, fixture.work);
			else fixture.registry.dispose();
			await expect(borrowed()).resolves.toBe(false);
			expect(changed).toHaveBeenCalledTimes(1);
			fixture.registry.dispose();
		}
		const stale = await producer(), valid = await producer(false, stale.registry, 1);
		stale.registry.observeInputs(stale.key, stale.work, async () => true);
		await expect(stale.actor(undefined, async running => {
			if (await running.inputsChanged?.()) return "rejected";
			await valid.publish(); return "completed";
		})).resolves.toMatchObject({ kind: "hit", producer: valid.work, joined: true });
		await expect(stale.work.inputsChanged!()).resolves.toBe(true);
		await expect(stale.ownership.commit(async () => "still owned")).resolves.toBe("still owned");
		stale.registry.dispose();
	});
});

async function producer(oneShot = false, registry = new ProcessHandoffRegistry<unknown>(8), exitCode = 0, scope = SCOPE, ownership = new ProcessHandoffOwnership()) {
	const certificate = processCertificate(oneShot, exitCode);
	const key = certificate.weakKey;
	const acquired = await registry.acquire({
		key,
		scope,
		role: "producer",
		executablePath: certificate.prototype.executablePath,
		ownership,
		lookup: async () => undefined,
	});
	if (acquired.kind !== "work") throw new Error("expected process work");
	return { certificate, key, registry, ownership, work: acquired.work,
		publish: (persist = async () => false) => registry.publish(key, acquired.work, certificate, persist),
		actor: (lookup = livePlan, waitForRunning: (running: ProcessHandoff) => Promise<"completed" | "miss" | "rejected"> =
			running => running.completion.then(() => "completed"), scope = SCOPE) => registry.acquire({ key, scope, role: "actor", lookup, waitForRunning }),
	};
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
