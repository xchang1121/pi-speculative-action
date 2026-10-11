import { readFile, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test } from "vitest";
import { captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts, sealProcessCertificate,
	type ProcessProvenanceCertificate } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { waitForCompletion } from "../src/scheduler.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { processPrototype, processCertificate } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred } from "./async.ts";

const directories = temporaryDirectories("pi-reuse-system-");
const registries = new Set<ProcessHandoffRegistry>();
afterEach(async () => { for (const registry of registries) registry.dispose(); registries.clear(); await directories.dispose(); });

async function fixture() {
	const root = await directories.create(), input = path.join(root, "input");
	await writeFile(input, "current input");
	const store = new ProvenanceCertificateStore(root), prototype = processPrototype();
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

test.each(["certificate JSON", "certificate digest", "artifact bytes", "artifact length", "missing artifact"])(
	"reopens a cache with %s damage and delivers another verified result", async damage => {
	const f = await fixture(), good = await f.produce("valid result", 1), bad = await f.produce("other result", 2);
	await f.store.put(good); await f.store.put(bad);
	const hex = bad.id.slice(7), certificatePath = path.join(f.root, "certificates", hex.slice(0, 2), hex.slice(2) + ".json");
	const artifact = referencedArtifacts(bad)[0]!, digest = artifact.digest.slice(7);
	const artifactPath = path.join(f.root, "cas", "sha256", digest.slice(0, 2), digest.slice(2));
	if (damage === "certificate JSON") await writeFile(certificatePath, "{");
	else if (damage === "certificate digest") {
		const bytes = JSON.parse(await readFile(certificatePath, "utf8")); bytes.result.exit.code++;
		await writeFile(certificatePath, JSON.stringify(bytes));
	} else if (damage === "artifact bytes") await writeFile(artifactPath, "wrong result");
	else if (damage === "artifact length") await writeFile(artifactPath, "wrong");
	else await unlink(artifactPath);
	const store = new ProvenanceCertificateStore(f.root), reopened = new ProcessReusePlanner({ store });
	const plan = await reopened.plan(f.request);
	expect(plan.kind).toBe("completed_replay");
	if (plan.kind !== "completed_replay") throw new Error("valid result was lost");
	expect(f.output(plan)).toBe("valid result");
	await store.put(await f.produce("other result", 2));
	const restored = await reopened.plan(f.request);
	if (restored.kind !== "completed_replay") throw new Error("fresh publication did not repair the damaged object");
	expect(f.output(restored)).toBe("other result");
	await writeFile(f.input, "changed input");
	expect((await reopened.plan(f.request)).kind).toBe("miss");
});

test("reopens a completed result with its full calculation graph and preserves reported reuse", async () => {
	const f = await fixture(), plain = await f.produce("timed result");
	const inputs = Array.from({ length: 4096 }, (_, index) => ({ computation: new TimelineInterval(200 + index * 50, 250 + index * 50), reused: true }));
	const original = new TimelineInterval(10, 110, [{ computation: new TimelineInterval(20, 40), overhead: true }, ...inputs]);
	const measured = sealProcessCertificate({ ...plain, result: { ...plain.result, computation: TimelineInterval.serialize(original) } });
	await f.store.put(measured);
	const replay = await new ProcessReusePlanner({ store: new ProvenanceCertificateStore(f.root) }).plan(f.request);
	if (replay.kind !== "completed_replay") throw new Error("completed result was lost");
	expect(f.output(replay)).toBe("timed result");
	expect(replay.certificate.id).toBe(plain.id);
	expect(new TaskTimeline(0).recordTool(TimelineInterval.restore(replay.certificate.result.computation)!, 1_000_000, true))
		.toMatchObject({ toolComputeMs: 204880, hiddenComputeMs: 204880 });
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
