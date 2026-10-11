import { readdir, unlink, utimes, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, expect, test, vi } from "vitest";
import { ArtifactCAS, captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts, sealProcessCertificate, sha256Digest,
	type ProcessProvenanceCertificate, type VerifiedArtifactClosure } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry, type ProcessHandoff } from "../src/process-handoff.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { waitForCompletion } from "../src/scheduler.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { legacyCertificate, PROCESS_CERTIFICATE, storeMetadata, storePeer } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { deferred, gated, nextTurn } from "./async.ts";

vi.mock("node:fs/promises", { spy: true });
const directories = temporaryDirectories("pi-reuse-system-");
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
	await Promise.all(cleanup.splice(0).map(close => close()));
	vi.restoreAllMocks(); await directories.dispose();
});
const own = <T extends { dispose(): unknown }>(resource: T): T => { cleanup.push(() => resource.dispose()); return resource; };
const storeAt = (...args: ConstructorParameters<typeof ProvenanceCertificateStore>) => own(new ProvenanceCertificateStore(...args));
const hold = () => { const pause = gated(); cleanup.push(pause.release); return pause; };
const track = <T>(pending: Promise<T>): Promise<T> => { const settled = pending.catch(() => undefined); cleanup.push(() => settled); return pending; };
const peer = (...args: Parameters<typeof storePeer>) => { const owner = storePeer(...args); cleanup.push(() => { owner.child.kill(); return owner.completed.catch(() => undefined); }); return owner; };
function watchInventory(store: ProvenanceCertificateStore, pause?: ReturnType<typeof hold>) {
	const target = store as unknown as { inventory(...args: unknown[]): Promise<unknown> }, read = target.inventory.bind(target);
	return vi.spyOn(target, "inventory").mockImplementationOnce(async (...args) => { const snapshot = await read(...args); await pause?.wait(); return snapshot; });
}

async function fixture(options: ConstructorParameters<typeof ProvenanceCertificateStore>[1] = {}) {
	const root = await directories.create(), input = path.join(root, "input"), store = storeAt(root, options);
	await writeFile(input, "current input");
	const request = { weakKey: PROCESS_CERTIFICATE.weakKey, executablePath: PROCESS_CERTIFICATE.prototype.executablePath,
		contract: { sink: "buffered" as const, orderedJournal: true, transactionalEffects: true }, validation: { resolvePath: () => input } };
	const produce = async (output: string | readonly string[], createdAt = 1, oneShot = false, artifacts = store.artifacts) => {
		const { dependency } = await captureFileDependency(input, "/workspace/input");
		const journal = await Promise.all((typeof output === "string" ? [output] : output).map(async (value, sequence) => {
			const bytes = Buffer.from(value), pending = artifacts.put(bytes); bytes.fill(0);
			return { sequence, kind: "output" as const, fd: 1 as const, data: await pending };
		}));
		return sealProcessCertificate({ ...PROCESS_CERTIFICATE, createdAt, dependencyCertificate: { complete: true, dependencies: [dependency], taints: oneShot ? ["clock"] : [] },
			result: { replayProfile: "buffered_noninteractive", journal, exit: { kind: "code", code: 0 } } });
	};
	const planner = new ProcessReusePlanner({ store }), registry = own(new ProcessHandoffRegistry(store, 1024));
	const lookup = async (certificates?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<ProcessProvenanceCertificate["id"]>, artifacts?: ReadonlyMap<ProcessProvenanceCertificate["id"], VerifiedArtifactClosure>) => {
		const plan = await planner.plan({ ...request, excludedCertificates, ...(certificates ? { live: { certificate: certificates, artifacts, acceptedTaints: ["clock"] } } : {}) });
		return plan.kind === "completed_replay" ? plan : undefined;
	};
	const output = (plan: Awaited<ReturnType<ProcessReusePlanner["plan"]>> | undefined) => plan?.kind === "completed_replay" ? plan.certificate.result.journal.map(event =>
		event.kind === "output" ? plan.artifacts.read(event.data).toString() : "").join("") : undefined;
	const common = { key: request.weakKey, scope: { sessionID: "system", turnID: "one" }, lookup };
	const reserve = async (ownership = new ProcessHandoffOwnership()) => {
		const result = await registry.acquire({ ...common, ownership, role: "producer", executablePath: request.executablePath });
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
		}); else if (damage === "missing artifact") {
			await unlink(artifactPath);
			const versions = () => storeMetadata(f.root, database => database.prepare("SELECT count(*) AS count FROM changes").get()!.count), before = versions();
			for (let code = 1; code <= 4; code++) await expect(f.store.put(sealProcessCertificate({ ...bad, result: { ...bad.result, continuation: undefined, exit: { kind: "code", code } } }))).rejects.toThrow("missing artifact");
			expect(versions()).toBe(before);
		}
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
	storeMetadata(f.root, database => database.exec("UPDATE certificates SET value = '{'; INSERT INTO changes VALUES ('certificate:abandoned', 0), ('cas/abandoned', 0)"));
	const pages = () => storeMetadata(f.root, database => Number(database.prepare("PRAGMA page_count").get()!.page_count)), beforeGC = pages();
	await f.store.gc(); expect(pages()).toBeLessThan(beforeGC); expect(await store.stats()).toMatchObject({ certificates: 0, artifacts: 0 });
	expect(storeMetadata(f.root, database => database.prepare("SELECT path FROM changes WHERE path <> ''").all())).toEqual([]);
	for (const executable of [undefined, f.request.executablePath]) expect(await f.store.mayHaveCertificates(executable)).toBe(false);
	expect(await readdir(path.join(f.root, "indexes"), { recursive: true }).catch(() => [])).toEqual([]);
	reads.mockClear(); expect(f.output(await planner.plan(f.request))).toBeUndefined(); expect(reads).not.toHaveBeenCalled();
	const bytes = Buffer.alloc(64 * 1024 * 1024), controller = new AbortController(), opened: filesystem.FileHandle[] = [];
	await writeFile(f.input, bytes);
	const certificate = sealProcessCertificate({ ...PROCESS_CERTIFICATE, dependencyCertificate: { complete: true, taints: [],
		dependencies: [{ kind: "file", role: "input", path: "/workspace/input", contentDigest: sha256Digest(bytes) }] } });
	const native = await vi.importActual<typeof filesystem>("node:fs/promises"); let readBytes = 0;
	vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
		const handle = await native.open(...args), read = handle.read; opened.push(handle);
		vi.spyOn(handle, "read").mockImplementation(async (...args) => {
			const result = await Reflect.apply(read, handle, args); readBytes += result.bytesRead; controller.abort(); return result;
		}); return handle;
	});
	await expect(planner.plan({ ...f.request, signal: controller.signal, live: { certificate, acceptedTaints: [] } })).rejects.toThrow();
	expect(readBytes).toBe(1024 * 1024); expect(opened.length).toBeGreaterThan(0); expect(opened.every(handle => handle.fd === -1)).toBe(true);
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
	const crash = peer(f.root, undefined, [], phase === "scan" ? "publish" : phase === "deletion" ? "clear" : "legacy-clear");
	await crash.completed.catch(() => undefined); expect(crash.child.signalCode).toBe("SIGKILL");
	expect((await f.store.stats()).artifacts).toBe(phase === "scan" ? 3 : phase === "clear" ? 2 : 0);
	const recovered = storeAt(f.root, { orphanGraceMs: 0 });
	await recovered.gc(); await recovered.gc();
	expect((await readdir(f.root, { recursive: true })).filter(file => file.includes(".clear-") || file.endsWith(".tmp"))).toEqual([]);
	await recovered.artifacts.put("shared"); await recovered.artifacts.put("next"); await recovered.put(remote);
	expect(f.output(await f.lookup())).toBe("sharednext");
	const owner = peer(f.root); expect(await owner.noticed).toBe("held");
	expect((await waitForCompletion(owner.completed, undefined, 30)).status).toBe("deadline");
	const backend = own(new LinuxProcessReuseBackend({ storeRoot: f.root })), commands = vi.spyOn(DatabaseSync.prototype, "exec");
	expect(await backend.store.get(remote.id)).toEqual(remote);
	for (let read = 0; read < 3; read++) expect((await backend.store.stats()).certificates).toBe(1);
	expect(commands).not.toHaveBeenCalledWith("BEGIN IMMEDIATE");
	const blocked = track(backend.store.artifacts.put("waiting for writer"));
	await expect.poll(() => commands.mock.results.some(result => result.type === "throw" && [5, 6].includes(Number(result.value.errcode) & 0xff))).toBe(true);
	const controller = new AbortController(), host = vi.fn(async (request: { signal?: AbortSignal }) => { request.signal?.throwIfAborted(); return { exitCode: 0 }; });
	const route = await backend.prepareActorReplay({ execute: host }, { sourceRoot: f.root, invocation: () => undefined });
	expect(route.state === "unavailable").toBe(process.platform !== "linux");
	const executor = route.state !== "unavailable" ? route.executor : { execute: async (request: { signal?: AbortSignal }) => {
		await backend.store.mayHaveCertificates(undefined, request.signal); return host(request);
	} };
	const entered = deferred(), hint = backend.store.mayHaveCertificates.bind(backend.store);
	vi.spyOn(backend.store, "mayHaveCertificates").mockImplementation((...args) => { entered.resolve(); return hint(...args); });
	const actor = track(executor.execute({ command: "true", cwd: f.root, signal: controller.signal, onData() {}, environment: {} })
		.then(() => "completed", error => { if (controller.signal.aborted) return "aborted"; throw error; })); await entered.promise;
	if (phase === "clear") expect(await waitForCompletion(blocked.catch(error => error.name), undefined, 1500)).toEqual({ status: "completed", value: "TimeoutError" });
	if (phase === "scan") {
		controller.abort(); expect(await waitForCompletion(actor, undefined, 200)).toEqual({ status: "completed", value: "aborted" });
	}
	expect(await waitForCompletion(Promise.all([actor, backend.dispose()]), undefined, 200))
		.toEqual({ status: "completed", value: [phase === "scan" ? "aborted" : "completed", undefined] });
	await expect(blocked).rejects.toThrow(); expect(host).toHaveBeenCalledOnce();
	owner.child.kill("SIGKILL"); await owner.completed.catch(() => undefined); expect(owner.child.signalCode).toBe("SIGKILL");
	const writer = storeAt(f.root), writing = hold(), rename = (await vi.importActual<typeof filesystem>("node:fs/promises")).rename;
	vi.spyOn(filesystem, "rename").mockImplementationOnce(async (...args) => { await writing.wait(); return rename(...args); });
	const publication = track(writer.artifacts.put("admitted write")); await writing.entered;
	const queued = track(writer.artifacts.put("queued write"));
	expect(await waitForCompletion(queued.catch(error => error.name), undefined, 1500)).toEqual({ status: "completed", value: "TimeoutError" });
	const disposed = writer.dispose(); expect((await waitForCompletion(disposed, undefined, 30)).status).toBe("deadline");
	writing.release(); const reference = await publication; await disposed;
	expect((await recovered.artifacts.get(reference))?.toString()).toBe("admitted write");
	await expect(writer.put(remote)).rejects.toThrow("closed");
});

test.each(["existing", "new"])("selects a %s ready producer and retains its complete result through collection, borrowing and eviction", async arrival => {
	const f = await fixture(), first = await f.reserve(), entered = deferred(), stopped = deferred();
	let second = arrival === "existing" ? await f.reserve() : undefined;
	const actor = track(f.actor({ waitForRunning: async (running, signal) => {
		entered.resolve(); const result = await waitForCompletion(running.completion, signal); if (signal.aborted) stopped.resolve();
		return result.status === "completed" ? "completed" : "miss";
	} }));
	await entered.promise; const broken = await f.produce("broken", 0, true);
	await unlink(f.store.artifacts.artifactPath(referencedArtifacts(broken)[0]!.digest));
	await expect(f.registry.publish(f.request.weakKey, first, broken, async () => false)).rejects.toThrow("complete owned result");
	second ??= await f.reserve(); await f.publish(second, "fast result");
	expect(await f.consume(actor, "fast result")).toMatchObject({ producer: second }); await stopped.promise;
	const slow = await f.publish(first, "slow result", 2, false); await f.consume(f.actor(), "slow result");
	await f.store.put(await f.produce("saved", 3));
	const reference = referencedArtifacts(slow)[0]!, target = f.store.artifacts.artifactPath(reference.digest), borrowed = await f.actor();
	if (borrowed.kind !== "hit") throw new Error("expected owned result");
	await writeFile(target, "damage"); await utimes(target, new Date(0), new Date(0)); await peer(f.root, undefined, [], "collect").completed;
	expect(await f.store.artifacts.get(reference)).toBeUndefined(); expect(f.output(borrowed.plan)).toBe("slow result");
	borrowed.plan.artifacts.read(reference).fill(0); expect(f.output(borrowed.plan)).toBe("slow result");
	const mutable = Buffer.from("slow result"), source = { ...borrowed.plan.artifacts, read: () => { queueMicrotask(() => mutable.fill(0)); return mutable; } };
	expect(f.output(await f.lookup([slow], undefined, new Map([[slow.id, source]])))).toBe("slow result");
	expect(await f.lookup([slow], undefined, new Map([[slow.id, { ...borrowed.plan.artifacts, read: () => Buffer.alloc(reference.size) }]]))).toBeUndefined();
	f.registry.configure(8, 1); expect(f.registry.results(f.request.weakKey, { sessionID: "system", turnID: "one" })).toHaveLength(1);
	borrowed.settle?.(false); expect(f.registry.hasResults).toBe(false);
	await f.consume(f.actor(), "saved"); await writeFile(f.input, "changed input"); await f.consume(f.actor(), undefined);
});

test.each(["released", "consumed", "cancelled", "raced", "whole-released", "whole-consumed"])("settles a %s one-shot claim without losing another Actor's result", async outcome => {
	const f = await fixture(), ownership = new ProcessHandoffOwnership(), releaseWhole = hold();
	const certificate = await f.publish(await f.reserve(ownership), "computed result");
	const checked = deferred(), racing = hold(), controller = new AbortController(), consumed = outcome.endsWith("consumed");
	cleanup.push(() => controller.abort()); let settled = false;
	const next = () => track(f.actor({ signal: controller.signal, lookup: async (...args) => {
		const [candidates] = args, plan = await f.lookup(...args); if (outcome === "raced" && candidates) { checked.resolve(); await racing.wait(); }
		if (!candidates) checked.resolve(); return plan;
	} }).then(result => { settled = true; return result; }));
	const earlier = outcome === "raced" ? next() : undefined; if (earlier) await checked.promise;
	const whole = outcome.startsWith("whole") ? track(ownership.commit(async () => {
		await releaseWhole.wait(); if (!consumed) throw effectCommitFailure(new Error("unchanged effects"), "recoverable");
	}).catch(error => { expect(error.disposition).toBe("recoverable"); })) : undefined;
	const first = whole ? undefined : await f.actor(); if (first && first.kind !== "hit") throw new Error("expected first claim");
	const second = earlier ?? next(); racing.release(); await checked.promise; await nextTurn(); expect(settled).toBe(false);
	for (const reference of referencedArtifacts(certificate)) await utimes(f.store.artifacts.artifactPath(reference.digest), new Date(0), new Date(0));
	await peer(f.root, undefined, [], "collect").completed;
	expect(await f.store.artifacts.load(referencedArtifacts(certificate))).toBeUndefined();
	if (outcome === "cancelled") controller.abort(); else { first?.settle?.(consumed); releaseWhole.release(); }
	await f.consume(second, consumed || outcome === "cancelled" ? undefined : "computed result");
	first?.settle?.(false); releaseWhole.release(); await whole;
	await f.consume(f.actor(), outcome === "cancelled" ? "computed result" : undefined);
});
