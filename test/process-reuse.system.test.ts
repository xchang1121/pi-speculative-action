import { readdir, unlink, utimes, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { ArtifactCAS, captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts, sealProcessCertificate,
	type ProcessProvenanceCertificate } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { waitForCompletion } from "../src/scheduler.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { legacyCertificate, processPrototype, processCertificate, storeMetadata, storePeer } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred, gated, nextTurn } from "./async.ts";

vi.mock("node:fs/promises", { spy: true });
const directories = temporaryDirectories("pi-reuse-system-");
const registries = new Set<ProcessHandoffRegistry>();
const stores = new Set<ProvenanceCertificateStore>();
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
	for (const registry of registries) registry.dispose(); registries.clear();
	await Promise.all(cleanup.splice(0).map(close => close()));
	await Promise.all([...stores].map(store => store.drain())); stores.clear();
	vi.restoreAllMocks(); await directories.dispose();
});
const storeAt = (root: string, options?: ConstructorParameters<typeof ProvenanceCertificateStore>[1]) => { const store = new ProvenanceCertificateStore(root, options); stores.add(store); return store; };
const hold = () => { const pause = gated(); cleanup.push(pause.release); return pause; };
const track = <T>(pending: Promise<T>): Promise<T> => { const settled = pending.catch(() => undefined); cleanup.push(() => settled); return pending; };
const peer = (...args: Parameters<typeof storePeer>) => { const owner = storePeer(...args); cleanup.push(() => { owner.child.kill(); return owner.completed.catch(() => undefined); }); return owner; };
function watchInventory(store: ProvenanceCertificateStore, pause?: ReturnType<typeof hold>) {
	const target = store as unknown as { inventory(...args: unknown[]): Promise<unknown> }, read = target.inventory.bind(target);
	return vi.spyOn(target, "inventory").mockImplementationOnce(async (...args) => { const snapshot = await read(...args); await pause?.wait(); return snapshot; });
}

async function fixture(options: ConstructorParameters<typeof ProvenanceCertificateStore>[1] = {}) {
	const root = await directories.create(), input = path.join(root, "input"), store = storeAt(root, options), prototype = processPrototype();
	await writeFile(input, "current input");
	const request = { weakKey: processCertificate(prototype).weakKey, executablePath: prototype.executablePath,
		contract: { sink: "buffered" as const, orderedJournal: true, transactionalEffects: true }, validation: { resolvePath: () => input } };
	const produce = async (output: string | readonly string[], createdAt = 1, oneShot = false, artifacts = store.artifacts) => {
		const { dependency } = await captureFileDependency(input, "/workspace/input");
		const journal = await Promise.all((typeof output === "string" ? [output] : output).map(async (value, sequence) =>
			({ sequence, kind: "output" as const, fd: 1 as const, data: await artifacts.put(value) })));
		return processCertificate(prototype, { createdAt, dependencyCertificate: { complete: true, dependencies: [dependency], taints: oneShot ? ["clock"] : [] },
			result: { replayProfile: "buffered_noninteractive", journal, exit: { kind: "code", code: 0 } } });
	};
	const planner = new ProcessReusePlanner({ store }), registry = new ProcessHandoffRegistry(8); registries.add(registry);
	const lookup = async (certificates?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<ProcessProvenanceCertificate["id"]>) => {
		const plan = await planner.plan({ ...request, excludedCertificates, ...(certificates ? { live: { certificate: certificates, acceptedTaints: ["clock"] } } : {}) });
		return plan.kind === "completed_replay" ? plan : undefined;
	};
	const output = (plan: Awaited<ReturnType<ProcessReusePlanner["plan"]>> | undefined) => plan?.kind === "completed_replay" ? plan.certificate.result.journal.map(event =>
		event.kind === "output" ? plan.artifacts.read(event.data).toString() : "").join("") : undefined;
	const common = { key: request.weakKey, scope: { sessionID: "system", turnID: "one" }, lookup };
	const reserve = async (select = lookup, ownership = new ProcessHandoffOwnership()) => {
		const result = await registry.acquire({ ...common, lookup: select, ownership, role: "producer", executablePath: request.executablePath });
		if (result.kind !== "work") throw new Error("expected producer");
		return result.work;
	};
	const actor = (options: { signal?: AbortSignal; lookup?: typeof lookup;
		waitForRunning?: (work: ProcessHandoff, signal: AbortSignal) => Promise<"completed" | "miss" | "rejected"> } = {}) =>
		registry.acquire({ ...common, role: "actor", waitForRunning: async () => "miss", ...options });
	const publish = async (work: Awaited<ReturnType<typeof reserve>>, value: string, createdAt = 1, oneShot = true) => {
		const certificate = await produce(value, createdAt, oneShot); await registry.publish(request.weakKey, work, certificate, async () => false); return certificate;
	};
	const consume = async (pending: ReturnType<typeof actor>, expected: string | undefined) => {
		const result = await pending; expect(result.kind).toBe(expected === undefined ? "miss" : "hit");
		if (result.kind === "hit") { expect(output(result.plan)).toBe(expected); result.settle?.(true); } return result;
	};
	return { root, input, store, request, produce, lookup, output, registry, reserve, actor, publish, consume };
}

test("reopens, validates, repairs and collects persisted history without stale observations", async () => {
	const f = await fixture({ maxCertificates: 257, orphanGraceMs: 0 }), certificates: ProcessProvenanceCertificate[] = [], legacy = new ArtifactCAS(path.join(f.root, "cas"));
	const original = new TimelineInterval(10, 110, [{ computation: new TimelineInterval(20, 40), overhead: true },
		...Array.from({ length: 4096 }, (_, index) => ({ computation: new TimelineInterval(200 + index * 50, 250 + index * 50), reused: true }))]);
	for (let index = 1; index <= 256; index++) {
		await writeFile(f.input, `input ${index}`);
		const certificate = await f.produce(`output ${index}`, index, false, index === 1 ? legacy : undefined); certificates.push(certificate);
		if (index === 1) await legacyCertificate(f.root, certificate);
		else await f.store.put(index === 256 ? sealProcessCertificate({ ...certificate, result: { ...certificate.result, computation: TimelineInterval.serialize(original) } }) : certificate);
	}
	await f.store.put(sealProcessCertificate({ ...certificates.at(-1)!, createdAt: 9999 }));
	const store = storeAt(f.root, { maxCertificates: 257 }), reads = vi.spyOn(store, "get"), planner = new ProcessReusePlanner({ store });
	for (const version of [256, 1, 0]) {
		await writeFile(f.input, `input ${version}`); reads.mockClear(); const plan = await planner.plan(f.request);
		expect(f.output(plan)).toBe(version ? `output ${version}` : undefined);
		expect(reads).toHaveBeenCalledTimes(version === 256 ? 1 : 256); expect(plan.lookup.pathsetsValidated).toBe(1);
		if (plan.kind === "completed_replay") {
			expect(plan.certificate.id).toBe(certificates[version - 1]!.id);
			if (version === 256) expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(plan.certificate.result.computation)!, 1_000_000, true)).toMatchObject({ toolComputeMs: 204880, hiddenComputeMs: 204880 });
		}
	}
	expect((await store.findByWeakKey(f.request.weakKey, f.request.executablePath)).map(certificate => certificate.createdAt)).toEqual(Array.from({ length: 256 }, (_, index) => 256 - index));
	await writeFile(f.input, "input 256"); const bad = await f.produce("other result", 257), artifactPath = f.store.artifacts.artifactPath(referencedArtifacts(bad)[0]!.digest); await f.store.put(bad);
	for (const damage of ["certificate JSON", "certificate digest", "artifact bytes", "artifact length", "missing artifact"]) {
		if (damage.startsWith("certificate")) storeMetadata(f.root, database => {
			const value = { ...bad, result: { ...bad.result, exit: { kind: "code", code: 1 } } };
			database.prepare("UPDATE certificates SET value = ? WHERE id = ?").run(damage === "certificate JSON" ? "{" : JSON.stringify(value), bad.id);
		}); else if (damage === "missing artifact") await unlink(artifactPath);
		else await writeFile(artifactPath, damage === "artifact bytes" ? "wrong result" : "wrong");
		const reopened = new ProcessReusePlanner({ store: storeAt(f.root) });
		for (const [input, expected, repair] of [["input 256", "output 256", false], ["input 256", "other result", true], ["changed input", undefined, false]] as const) {
			await writeFile(f.input, input); if (repair) { await f.store.artifacts.put("other result"); await f.store.put(bad); }
			expect(f.output(await reopened.plan(f.request)), damage).toBe(expected);
		}
	}
	const pause = hold(); watchInventory(f.store, pause); const pending = track(f.store.stats(true));
	await pause.entered; await f.store.put(await f.produce("published during stats", 258)); pause.release(); expect((await pending).certificates).toBe(257);
	for (const owner of [f.store, store]) expect(await owner.stats()).toMatchObject({ certificates: 258, overBudget: true });
	storeMetadata(f.root, database => database.exec("UPDATE certificates SET value = '{'"));
	const pages = () => storeMetadata(f.root, database => Number(database.prepare("PRAGMA page_count").get()!.page_count)), beforeGC = pages();
	await f.store.gc(); expect(pages()).toBeLessThan(beforeGC); expect(await store.stats()).toMatchObject({ certificates: 0, artifacts: 0 });
	for (const executable of [undefined, f.request.executablePath]) expect(await f.store.mayHaveCertificates(executable)).toBe(false);
	expect(await readdir(path.join(f.root, "indexes"), { recursive: true }).catch(() => [])).toEqual([]);
	reads.mockClear(); expect(f.output(await planner.plan(f.request))).toBeUndefined(); expect(reads).not.toHaveBeenCalled();
});

test.each(["scan", "deletion", "clear"])("preserves publications from both owners across %s, collection and process exit", async phase => {
	const f = await fixture({ orphanGraceMs: 0 }), pause = hold(), old = await f.produce("old", 1), orphan = await f.store.artifacts.put("orphan"), shared = await f.store.artifacts.put("shared");
	await f.store.put(old); for (let version = 2; version < 64; version++) await f.store.put(await f.produce(`output ${version}`, version));
	const pending = await f.produce("next", 65), remote = await f.produce(["shared", "next"], 73), certificates = [old, pending, remote];
	// Only the remote publication references "shared"; local renewal cannot mask its lost closure.
	const target = f.store.artifacts.artifactPath(shared.digest); await utimes(target, new Date(0), new Date(0));
	const scans = watchInventory(f.store, phase === "deletion" ? undefined : pause), original = (await vi.importActual<typeof filesystem>("node:fs/promises")).rm;
	const removals = vi.spyOn(filesystem, "rm").mockImplementation(async (file, options) => {
		if (phase === "deletion" && String(file) === target) await pause.wait(); return original(file, options);
	});
	let published = false, drained = false; f.store.configure({ maxCertificates: 1 });
	// Keep "next" available while its new certificate waits behind the deletion transaction.
	track(f.store.put(await f.produce(["ready", "next"], 64)).then(() => { published = true; }));
	await pause.entered; await nextTurn(); expect(published).toBe(true);
	if (phase === "clear") { await f.store.clear(); await f.store.artifacts.put("next"); }
	const count = removals.mock.calls.length, concurrent = track(f.store.put(pending)), publisher = peer(f.root, remote, phase === "scan" ? [] : ["shared", "next"]);
	await publisher.noticed; if (phase === "deletion") pause.release(); await concurrent;
	if (phase === "deletion") expect(removals.mock.calls.length).toBeLessThanOrEqual(count + 1);
	for (let version = 66; version <= 72; version++) {
		const certificate = await f.produce(`output ${version}`, version); certificates.push(certificate); await f.store.put(certificate);
	}
	await f.store.artifacts.put("old"); await f.store.put(old); await f.store.artifacts.put("orphan"); await publisher.completed;
	const closing = track(f.store.drain().then(() => { drained = true; }));
	if (phase !== "deletion") { await nextTurn(); expect(drained).toBe(false); } pause.release(); await closing;
	expect(scans).toHaveBeenCalledTimes(phase === "clear" ? 2 : 1);
	for (const certificate of certificates) {
		expect(await f.store.get(certificate.id)).toEqual(certificate); expect(await f.store.artifacts.load(referencedArtifacts(certificate))).toBeDefined();
	}
	expect((await f.store.artifacts.get(orphan))?.toString()).toBe("orphan"); expect(f.output(await f.lookup())).toBe("sharednext");
	await f.store.gc(); expect((await f.store.stats(true)).certificates).toBe(1); expect(await f.store.artifacts.get(orphan)).toBeUndefined();
	const owner = peer(f.root); expect(await owner.noticed).toBe("held"); owner.child.kill("SIGKILL"); await owner.completed.catch(() => undefined);
	await f.store.put(remote); expect(f.output(await f.lookup())).toBe("sharednext");
});

test.each(["existing", "new"])("selects a %s ready producer, then falls through damaged live results to fresh persisted evidence", async peer => {
	const f = await fixture(), first = await f.reserve(), entered = deferred(), stopped = deferred();
	let second = peer === "existing" ? await f.reserve() : undefined;
	const actor = track(f.actor({ waitForRunning: async (running, signal) => {
		entered.resolve(); const result = await waitForCompletion(running.completion, signal); if (signal.aborted) stopped.resolve();
		return result.status === "completed" ? "completed" : "miss";
	} }));
	await entered.promise; second ??= await f.reserve(); await f.publish(second, "fast result");
	expect(await f.consume(actor, "fast result")).toMatchObject({ producer: second }); await stopped.promise;
	const slow = await f.publish(first, "slow result", 2, false); await f.consume(f.actor(), "slow result");
	await f.store.put(await f.produce("saved", 3));
	await writeFile(f.store.artifacts.artifactPath(referencedArtifacts(slow)[0]!.digest), "damage");
	await f.consume(f.actor(), "saved"); await writeFile(f.input, "changed input"); await f.consume(f.actor(), undefined);
});

test.each(["released", "consumed", "cancelled", "raced", "whole-released", "whole-consumed"])("settles a %s one-shot claim without losing another Actor's result", async outcome => {
	const f = await fixture(), ownership = new ProcessHandoffOwnership(), releaseWhole = hold();
	await f.publish(await f.reserve(f.lookup, ownership), "computed result");
	const checked = deferred(), racing = hold(), controller = new AbortController(), consumed = outcome.endsWith("consumed");
	cleanup.push(() => controller.abort()); let settled = false;
	const next = () => track(f.actor({ signal: controller.signal, lookup: async (candidates, excluded) => {
		const plan = await f.lookup(candidates, excluded); if (outcome === "raced" && candidates) { checked.resolve(); await racing.wait(); }
		if (!candidates) checked.resolve(); return plan;
	} }).then(result => { settled = true; return result; }));
	const earlier = outcome === "raced" ? next() : undefined; if (earlier) await checked.promise;
	const whole = outcome.startsWith("whole") ? track(ownership.commit(async () => {
		await releaseWhole.wait(); if (!consumed) throw effectCommitFailure(new Error("unchanged effects"), "recoverable");
	}).catch(error => { expect(error.disposition).toBe("recoverable"); })) : undefined;
	const first = whole ? undefined : await f.actor(); if (first && first.kind !== "hit") throw new Error("expected first claim");
	const second = earlier ?? next(); racing.release(); await checked.promise; await nextTurn(); expect(settled).toBe(false);
	if (outcome === "cancelled") controller.abort(); else { first?.settle?.(consumed); releaseWhole.release(); }
	await f.consume(second, consumed || outcome === "cancelled" ? undefined : "computed result");
	first?.settle?.(false); releaseWhole.release(); await whole;
	await f.consume(f.actor(), outcome === "cancelled" ? "computed result" : undefined);
});
