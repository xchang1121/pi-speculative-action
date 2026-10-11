import { readFile, readdir, rename, unlink, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts, sealProcessCertificate,
	type ProcessProvenanceCertificate } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { waitForCompletion } from "../src/scheduler.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { processPrototype, processCertificate } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred, nextTurn } from "./async.ts";

vi.mock("node:fs/promises", { spy: true });
const directories = temporaryDirectories("pi-reuse-system-");
const registries = new Set<ProcessHandoffRegistry>();
const stores = new Set<ProvenanceCertificateStore>();
afterEach(async () => { for (const registry of registries) registry.dispose(); registries.clear(); await Promise.all([...stores].map(store => store.drain())); stores.clear(); await directories.dispose(); });

async function fixture(options: ConstructorParameters<typeof ProvenanceCertificateStore>[1] = {}) {
	const root = await directories.create(), input = path.join(root, "input");
	await writeFile(input, "current input");
	const store = new ProvenanceCertificateStore(root, options), prototype = processPrototype(); stores.add(store);
	const request = { weakKey: processCertificate(prototype).weakKey, executablePath: prototype.executablePath,
		contract: { sink: "buffered" as const, orderedJournal: true, transactionalEffects: true },
		validation: { resolvePath: () => input } };
	const produce = async (output: string, createdAt = 1, oneShot = false) => {
		const { dependency } = await captureFileDependency(input, "/workspace/input");
		const data = await store.artifacts.put(output);
		return processCertificate(prototype, { createdAt, dependencyCertificate: { complete: true, dependencies: [dependency], taints: oneShot ? ["clock"] : [] },
			result: { replayProfile: "buffered_noninteractive", journal: [{ sequence: 0, kind: "output", fd: 1, data }], exit: { kind: "code", code: 0 } } });
	};
	const planner = new ProcessReusePlanner({ store });
	const lookup = async (certificates?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<ProcessProvenanceCertificate["id"]>) => {
		const plan = await planner.plan({ ...request, excludedCertificates,
			...(certificates ? { live: { certificate: certificates, acceptedTaints: ["clock"] } } : {}) });
		return plan.kind === "completed_replay" ? plan : undefined;
	};
	const output = (plan: NonNullable<Awaited<ReturnType<typeof lookup>>>) => plan.certificate.result.journal.map(event =>
		event.kind === "output" ? plan.artifacts.read(event.data).toString() : "").join("");
	const registry = new ProcessHandoffRegistry(8), scope = { sessionID: "system", turnID: "one" };
	registries.add(registry);
	const common = { key: request.weakKey, scope, lookup };
	const reserve = async (select = lookup, ownership = new ProcessHandoffOwnership()) => {
		const result = await registry.acquire({ ...common, lookup: select, ownership, role: "producer", executablePath: request.executablePath });
		if (result.kind !== "work") throw new Error("expected producer");
		return result.work;
	};
	const actor = (options: { signal?: AbortSignal; lookup?: typeof lookup;
		waitForRunning?: (work: ProcessHandoff, signal: AbortSignal) => Promise<"completed" | "miss" | "rejected"> } = {}) =>
		registry.acquire({ ...common, role: "actor", waitForRunning: async () => "miss", ...options });
	const publish = async (work: ProcessHandoff, output: string, createdAt = 1) =>
		registry.publish(request.weakKey, work, await produce(output, createdAt, true), async () => false);
	return { root, input, store, request, produce, lookup, output, registry, reserve, actor, publish };
}

test("reopens history incrementally with calculation graphs, fresh inputs, legacy indexes and damaged objects", async () => {
	const f = await fixture(), certificates: ProcessProvenanceCertificate[] = [];
	const inputs = Array.from({ length: 4096 }, (_, index) => ({ computation: new TimelineInterval(200 + index * 50, 250 + index * 50), reused: true }));
	const original = new TimelineInterval(10, 110, [{ computation: new TimelineInterval(20, 40), overhead: true }, ...inputs]);
	for (let index = 1; index <= 256; index++) {
		await writeFile(f.input, `input ${index}`);
		const certificate = await f.produce(`output ${index}`, index); certificates.push(certificate);
		await f.store.put(index === 256 ? sealProcessCertificate({ ...certificate, result: { ...certificate.result, computation: TimelineInterval.serialize(original) } }) : certificate);
	}
	const references = await readdir(path.join(f.root, "indexes"), { recursive: true }), first = certificates[0]!, latest = certificates.at(-1)!;
	const legacy = path.join(f.root, "indexes", references.find(name => name.endsWith(`${first.id.slice(7)}.ref`))!);
	await rename(legacy, path.join(path.dirname(legacy), `${first.id.slice(7)}.ref`));
	await f.store.put(sealProcessCertificate({ ...latest, createdAt: 9999 }));
	expect((await readdir(path.dirname(legacy))).length).toBe(256); // Duplicate publication keeps the stored metadata and one reference.
	const store = new ProvenanceCertificateStore(f.root), reads = vi.spyOn(store, "get"), planner = new ProcessReusePlanner({ store });
	for (const version of [256, 1, 0]) {
		await writeFile(f.input, `input ${version}`); reads.mockClear();
		const plan = await planner.plan(f.request);
		expect(reads).toHaveBeenCalledTimes(version === 256 ? 1 : 256);
		expect(plan.lookup.pathsetsValidated).toBe(1);
		if (version === 0) expect(plan.kind).toBe("miss");
		else {
			if (plan.kind !== "completed_replay") throw new Error("verified history was lost");
			expect(f.output(plan)).toBe(`output ${version}`); expect(plan.certificate.id).toBe(certificates[version - 1]!.id);
			if (version === 256) expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(plan.certificate.result.computation)!, 1_000_000, true))
				.toMatchObject({ toolComputeMs: 204880, hiddenComputeMs: 204880 });
		}
	}
	expect((await store.findByWeakKey(f.request.weakKey, f.request.executablePath)).map(certificate => certificate.createdAt))
		.toEqual(Array.from({ length: 256 }, (_, index) => 256 - index));
	await writeFile(f.input, "input 256");
	const bad = await f.produce("other result", 257), hex = bad.id.slice(7), digest = referencedArtifacts(bad)[0]!.digest.slice(7);
	const certificatePath = path.join(f.root, "certificates", hex.slice(0, 2), hex.slice(2) + ".json"), artifactPath = path.join(f.root, "cas", "sha256", digest.slice(0, 2), digest.slice(2));
	await f.store.put(bad);
	for (const damage of ["certificate JSON", "certificate digest", "artifact bytes", "artifact length", "missing artifact"]) {
		await writeFile(f.input, "input 256");
		if (damage === "certificate JSON") await writeFile(certificatePath, "{");
		else if (damage === "certificate digest") {
			const bytes = JSON.parse(await readFile(certificatePath, "utf8")); bytes.result.exit.code++;
			await writeFile(certificatePath, JSON.stringify(bytes));
		} else if (damage === "artifact bytes") await writeFile(artifactPath, "wrong result");
		else if (damage === "artifact length") await writeFile(artifactPath, "wrong");
		else await unlink(artifactPath);
		const reopened = new ProcessReusePlanner({ store: new ProvenanceCertificateStore(f.root) }), plan = await reopened.plan(f.request);
		if (plan.kind !== "completed_replay") throw new Error(`${damage}: valid history was lost`);
		expect(f.output(plan), damage).toBe("output 256");
		await f.store.artifacts.put("other result"); await f.store.put(bad);
		const restored = await reopened.plan(f.request);
		if (restored.kind !== "completed_replay") throw new Error(`${damage}: publication did not repair the object`);
		expect(f.output(restored), damage).toBe("other result");
		await writeFile(f.input, "changed input"); expect((await reopened.plan(f.request)).kind, damage).toBe("miss");
	}
});

test.each(["scan", "deletion", "clear"])("bounds maintenance and protects concurrent publications during %s", async phase => {
	const f = await fixture({ orphanGraceMs: 0 }), entered = deferred(), release = deferred();
	const old = await f.produce("old", 1), orphan = await f.store.artifacts.put("orphan"); await f.store.put(old);
	for (let version = 2; version < 64; version++) await f.store.put(await f.produce(`output ${version}`, version));
	const pending = await f.produce("next", 65), original = (await vi.importActual<typeof filesystem>("node:fs/promises")).rm;
	const scanning = f.store as unknown as { inventory(): Promise<unknown> }, inventory = scanning.inventory.bind(scanning);
	let scans = 0, removals = 0, published = false, drained = false;
	scanning.inventory = async () => { const snapshot = await inventory(); if (++scans === 1 && phase !== "deletion") { entered.resolve(); await release.promise; } return snapshot; };
	const removing = vi.spyOn(filesystem, "rm").mockImplementation(async (target, options) => {
		if (phase === "deletion" && String(target).endsWith(".json") && ++removals === 2) { entered.resolve(); await release.promise; }
		return original(target, options);
	});
	f.store.configure({ maxCertificates: 1 });
	const publishing = f.store.put(await f.produce("ready", 64)).then(() => { published = true; });
	let concurrent: Promise<unknown> | undefined;
	try {
		await entered.promise; await nextTurn(); expect(published).toBe(true);
		if (phase === "clear") { await f.store.clear(); await f.store.artifacts.put("next"); }
		concurrent = f.store.put(pending);
		if (phase === "deletion") release.resolve();
		await concurrent;
		if (phase === "deletion") expect(removals).toBeLessThanOrEqual(3); // One object can delay delivery; the remaining tail cannot.
		for (let version = 66; version <= 72; version++) await f.store.put(await f.produce(`output ${version}`, version));
		await f.store.artifacts.put("old"); await f.store.put(old); await f.store.artifacts.put("orphan");
		const closing = f.store.drain().then(() => { drained = true; });
		if (phase !== "deletion") { await nextTurn(); expect(drained).toBe(false); }
		release.resolve(); await closing;
		expect(scans).toBe(phase === "clear" ? 2 : 1); // Clear has its own inventory; publications never restart the collection.
		expect(await f.store.get(old.id)).toEqual(old); expect((await f.store.artifacts.get(orphan))?.toString()).toBe("orphan");
		expect(await f.store.get(pending.id)).toEqual(pending); expect(await f.store.artifacts.load(referencedArtifacts(pending))).toBeDefined();
		const first = await f.lookup(); if (!first) throw new Error("concurrent publication lost its artifact closure");
		expect(f.output(first)).toBe("output 72");
		await f.store.gc(); // A quiet cycle also collects the objects protected by the preceding cycle.
		expect((await f.store.stats(true)).certificates).toBe(1);
		const replay = await f.lookup(); if (!replay) throw new Error("concurrent publication was pruned");
		expect(f.output(replay)).toBe("output 72"); expect(await f.store.artifacts.get(orphan)).toBeUndefined();
	} finally { release.resolve(); await Promise.allSettled([publishing, concurrent]); await f.store.drain(); removing.mockRestore(); }
});

test.each(["existing", "new"])("delivers a %s producer while an older producer is still running", async peer => {
	const f = await fixture(), first = await f.reserve(), entered = deferred(), stopped = deferred();
	let second = peer === "existing" ? await f.reserve() : undefined;
	const actor = f.actor({
		waitForRunning: async (running, signal) => {
			entered.resolve();
			const result = await waitForCompletion(running.completion, signal);
			if (signal.aborted) stopped.resolve();
			return result.status === "completed" ? "completed" : "miss";
		} });
	try {
		await entered.promise;
		second ??= await f.reserve();
		await f.publish(second, "fast result");
		const hit = await actor;
		if (hit.kind !== "hit") throw new Error("ready producer was not adopted");
		expect(f.output(hit.plan)).toBe("fast result");
		expect(hit.producer).toBe(second);
		hit.settle?.(true);
		await stopped.promise;
		// The slower producer remains independent and can still supply a later request.
		await f.publish(first, "slow result", 2);
		const later = await f.actor();
		if (later.kind !== "hit") throw new Error("slower producer was lost");
		expect(f.output(later.plan)).toBe("slow result"); later.settle?.(true);
	} finally { f.registry.dispose(); await actor; }
});

test("falls through a rejected live result to persisted evidence without accepting changed inputs", async () => {
	const f = await fixture(), saved = await f.produce("saved"); await f.store.put(saved);
	const pending = await f.reserve(async () => undefined);
	const broken = await f.produce("broken", 2), digest = referencedArtifacts(broken)[0]!.digest.slice(7);
	await writeFile(path.join(f.root, "cas", "sha256", digest.slice(0, 2), digest.slice(2)), "damage");
	await f.registry.publish(f.request.weakKey, pending, broken, async () => false);
	const adopted = await f.actor();
	if (adopted.kind !== "hit") throw new Error("persisted fallback was lost");
	expect(f.output(adopted.plan)).toBe("saved"); adopted.settle?.(true);
});

test.each(["released", "consumed", "cancelled", "raced", "whole-released", "whole-consumed"])("settles a %s one-shot claim without losing another Actor's result", async outcome => {
	const f = await fixture(), ownership = new ProcessHandoffOwnership(), releaseWhole = deferred();
	await f.publish(await f.reserve(f.lookup, ownership), "computed result");
	const checked = deferred(), racing = deferred(), controller = new AbortController(), consumed = outcome.endsWith("consumed");
	let settled = false;
	const next = () => f.actor({ signal: controller.signal, lookup: async (candidates, excluded) => {
		const plan = await f.lookup(candidates, excluded);
		if (outcome === "raced" && candidates) { checked.resolve(); await racing.promise; }
		if (!candidates) checked.resolve();
		return plan;
	} }).then(result => { settled = true; return result; });
	const earlier = outcome === "raced" ? next() : undefined;
	if (earlier) await checked.promise;
	const whole = outcome.startsWith("whole") ? ownership.commit(async () => {
		await releaseWhole.promise;
		if (!consumed) throw effectCommitFailure(new Error("unchanged effects"), "recoverable");
	}).catch(error => { expect(error.disposition).toBe("recoverable"); }) : undefined;
	const first = whole ? undefined : await f.actor();
	if (first && first.kind !== "hit") throw new Error("expected first claim");
	const second = earlier ?? next(); racing.resolve();
	try {
		await checked.promise; await new Promise<void>(setImmediate);
		expect(settled).toBe(false);
		if (outcome === "cancelled") controller.abort(); else { first?.settle?.(consumed); releaseWhole.resolve(); }
		const result = await second;
		expect(result.kind).toBe(consumed || outcome === "cancelled" ? "miss" : "hit");
		if (result.kind === "hit") { expect(f.output(result.plan)).toBe("computed result"); result.settle?.(true); }
		first?.settle?.(false); releaseWhole.resolve(); await whole;
		const later = await f.actor();
		expect(later.kind).toBe(outcome === "cancelled" ? "hit" : "miss");
		if (later.kind === "hit") { expect(f.output(later.plan)).toBe("computed result"); later.settle?.(true); }
	} finally { controller.abort(); releaseWhole.resolve(); first?.settle?.(false); f.registry.dispose(); await Promise.all([whole, second]); }
});
