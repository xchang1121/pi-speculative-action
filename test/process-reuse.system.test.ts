import { readFile, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test } from "vitest";
import { captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts, sealProcessCertificate,
	type ProcessProvenanceCertificate } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { waitForCompletion } from "../src/scheduler.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { processPrototype, processCertificate } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred } from "./async.ts";

const directories = temporaryDirectories("pi-reuse-system-");
afterEach(directories.dispose);

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
	return { root, input, store, request, produce, lookup, output };
}

test.each(["certificate JSON", "certificate digest", "artifact bytes", "missing artifact"])(
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
	else await unlink(artifactPath);
	const reopened = new ProcessReusePlanner({ store: new ProvenanceCertificateStore(f.root) });
	const plan = await reopened.plan(f.request);
	expect(plan.kind).toBe("completed_replay");
	if (plan.kind !== "completed_replay") throw new Error("valid result was lost");
	expect(f.output(plan)).toBe("valid result");
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
	const f = await fixture(), registry = new ProcessHandoffRegistry(8), scope = { sessionID: "system", turnID: "one" };
	const reserve = async () => {
		const result = await registry.acquire({ key: f.request.weakKey, scope, role: "producer", executablePath: f.request.executablePath,
			ownership: new ProcessHandoffOwnership(), lookup: f.lookup });
		if (result.kind !== "work") throw new Error("expected a new producer");
		return result.work;
	};
	const first = await reserve(), entered = deferred(), stopped = deferred();
	let second = peer === "existing" ? await reserve() : undefined;
	const actor = registry.acquire({ key: f.request.weakKey, scope, role: "actor", lookup: f.lookup,
		waitForRunning: async (running, signal) => {
			entered.resolve();
			const result = await waitForCompletion(running.completion, signal);
			if (signal.aborted) stopped.resolve();
			return result.status === "completed" ? "completed" : "miss";
		} });
	try {
		await entered.promise;
		second ??= await reserve();
		const certificate = await f.produce("fast result", 1, true);
		await registry.publish(f.request.weakKey, second, certificate, async () => false);
		const hit = await actor;
		if (hit.kind !== "hit") throw new Error("ready producer was not adopted");
		expect(f.output(hit.plan)).toBe("fast result");
		expect(hit.producer).toBe(second);
		hit.settle?.(true);
		await stopped.promise;
		// The slower producer remains independent and can still supply a later request.
		await registry.publish(f.request.weakKey, first, await f.produce("slow result", 2, true), async () => false);
		const later = await registry.acquire({ key: f.request.weakKey, scope, role: "actor", lookup: f.lookup,
			waitForRunning: async () => "miss" });
		if (later.kind !== "hit") throw new Error("slower producer was lost");
		expect(f.output(later.plan)).toBe("slow result"); later.settle?.(true);
	} finally { registry.dispose(); await actor; }
});

test("falls through a rejected live result to persisted evidence without accepting changed inputs", async () => {
	const f = await fixture(), saved = await f.produce("saved"); await f.store.put(saved);
	const registry = new ProcessHandoffRegistry(4), scope = { sessionID: "system", turnID: "one" };
	const pending = await registry.acquire({ key: f.request.weakKey, scope, role: "producer", executablePath: f.request.executablePath,
		ownership: new ProcessHandoffOwnership(), lookup: async () => undefined });
	if (pending.kind !== "work") throw new Error("expected producer");
	const broken = await f.produce("broken", 2), digest = referencedArtifacts(broken)[0]!.digest.slice(7);
	await writeFile(path.join(f.root, "cas", "sha256", digest.slice(0, 2), digest.slice(2)), "damage");
	await registry.publish(f.request.weakKey, pending.work, broken, async () => false);
	try {
		const adopted = await registry.acquire({ key: f.request.weakKey, scope, role: "actor", lookup: f.lookup,
			waitForRunning: async () => "miss" });
		if (adopted.kind !== "hit") throw new Error("persisted fallback was lost");
		expect(f.output(adopted.plan)).toBe("saved"); adopted.settle?.(true);
	} finally { registry.dispose(); }
});
