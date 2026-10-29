import { readdir, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { captureFileDependency, ProcessReusePlanner, ProvenanceCertificateStore, sealProcessCertificate, sha256Digest, type ProcessProvenanceCertificate, type VerifiedArtifactClosure } from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { CapturedFilesystemObject, cachedCapture, captureFileDigest, captureStableFile, hashExecutableFile } from "../src/filesystem-evidence.ts";
import { ResourceObjectInputs } from "../src/agent-execution-world.ts";
import { observeStrace, traceTail, writesWithin } from "../src/strace-observer.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { PROCESS_CERTIFICATE, storeMetadata, storePeer } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { nextTurn } from "./async.ts";

vi.mock("node:fs/promises", { spy: true });
const directories = temporaryDirectories("pi-reuse-system-");
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
	await Promise.all(cleanup.splice(0).map(close => close()));
	vi.restoreAllMocks(); await directories.dispose();
});
const own = <T extends { dispose(): unknown }>(resource: T): T => { cleanup.push(() => resource.dispose()); return resource; };
const storeAt = (...args: ConstructorParameters<typeof ProvenanceCertificateStore>) => own(new ProvenanceCertificateStore(...args));
const peer = (...args: Parameters<typeof storePeer>) => { const owner = storePeer(...args); cleanup.push(() => { owner.child.kill(); return owner.completed.catch(() => undefined); }); return owner; };

async function processTrace(content: string, suffix = "101") {
	const prefix = path.join(await directories.create(), "process"), trace = `${prefix}.${suffix}`, tail = traceTail(prefix, "/work");
	await writeFile(trace, content);
	return { prefix, trace, tail, append: async (content: string) => { await filesystem.appendFile(trace, content); return tail.read(); } };
}

test("distinguishes adding a sibling from removing and restoring a pathname binding", async () => {
	const { tail, append } = await processTrace('1.000000 openat(AT_FDCWD, "input", O_RDONLY) = 3</work/input>\n1.000001 openat(AT_FDCWD, "output", O_WRONLY|O_CREAT, 0600) = 4</work/output>\n');
	expect((await tail.read()).unsafe).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 0, 2000)).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/output"]), 0, 2000)).toBe(true);
	expect((await append('1.000002 rename("/work", "/held") = -1 EACCES (Permission denied)\n')).unsafe).toBe(false);
	await append('1.000003 rename("/work", "/held") = 0\n1.000004 rename("/held", "/work") = 0\n');
	expect(writesWithin(tail.writes, new Set(["/work"]), 0, 2000)).toBe(true);
	expect((await tail.read()).writes).toHaveLength(3);
});

test.each([
	{ reason: "unmodeled_metadata:getdents64:101", calls: ["getdents64(3, [], 4096) = 0"] },
	{ reason: "unparsed_metadata:stat:101", calls: ['stat("/work/input", {st_mode=S_IFREG|0644}) = 0'] },
	{ reason: "metadata_changed:/work/input", calls: [1, 2].map(size => `statx(AT_FDCWD, "/work/input", AT_SYMLINK_NOFOLLOW, STATX_SIZE, {stx_mask=STATX_SIZE, stx_size=${size}}) = 0`) },
	{ reason: "metadata_changed:/work/accessed", calls: [1, 2].map(time => `statx(AT_FDCWD, "/work/accessed", 0, STATX_ATIME, {stx_mask=STATX_ATIME, stx_atime={tv_sec=${time}, tv_nsec=0}}) = 0`) },
])("retains incomplete traces and metadata conflicts for transaction qualification ($reason)", async ({ calls, reason }) => {
	const { prefix } = await processTrace(['1.000000 execve("/tools/worker", ["worker"], []) = 0', ...calls.map((call, index) => `1.00000${index + 1} ${call}`), "1.000009 +++ exited with 0 +++\n"].join("\n"));
	const observed = await observeStrace(prefix, "/tools/worker", "/work");
	if (reason.startsWith("metadata_changed:")) {
		expect(observed.metadataChanges).toEqual([reason.slice("metadata_changed:".length)]);
		expect(observed.complete).toBe(true);
	} else {
		expect(observed.incompleteReasons).toContain(reason);
		expect(observed.complete).toBe(false);
		expect(observed.taints).toContain("trace_incomplete");
	}
});

test("keeps file admission and change detection after a digest is cached", async () => {
	const input = path.join(await directories.create(), "input"); await writeFile(input, "original");
	await new Promise(resolve => setTimeout(resolve, 2100)); // Settle even a filesystem with coarse timestamps.
	const bytes = await captureStableFile(input, 1024, true, { digest: false });
	expect(bytes.content?.toString()).toBe("original"); expect(cachedCapture(bytes.stat, input)).toBeUndefined();
	const captured = await captureStableFile(input, 1024);
	expect(await captureFileDigest(input)).toMatchObject({ hash: captured.hash, shared: true });
	if (process.platform === "linux") {
		const alias = `${input}-alias`; await filesystem.symlink(input, alias);
		await expect(captureFileDigest(alias)).rejects.toThrow("not_regular_file");
		expect(await hashExecutableFile(alias)).toBe(`sha256:${captured.hash}`);
	}
	await writeFile(input, "modified");
	expect((await captureFileDigest(input)).hash).toBe(sha256Digest("modified").slice("sha256:".length));
	await expect(captureFileDigest(input, 7)).rejects.toThrow("file_too_large");
	await expect(captureFileDigest(input, 1024, AbortSignal.abort(new Error("cancelled")))).rejects.toThrow("cancelled");
});

test.skipIf(process.platform !== "linux").each(["fresh", "changed-before", "changed-after", "revoked", "budget", "rejected"])("keeps input loans owned through transfer and exact invalidation (%s)", async mode => {
	const input = path.join(await directories.create(), "input"); await writeFile(input, "original");
	const captured = await captureStableFile(input, 1024, true, { retainObject: true }), source = own(captured.object!);
	const loan = source.lend()!, transfer = structuredClone(loan.transfer); cleanup.push(loan.release);
	const pins = async () => (await Promise.all((await readdir("/proc/self/fd")).map(fd => filesystem.readlink(`/proc/self/fd/${fd}`).catch(() => "")))).filter(target => target === input).length;
	expect(await pins()).toBe(1);
	let closed = false; const closing = source.dispose().then(() => { closed = true; });
	await nextTurn(); expect(closed).toBe(false); expect(source.lend()).toBeUndefined();
	if (mode === "changed-before") await writeFile(input, "modified");
	if (mode === "revoked") { loan.release(); await closing; }
	const received = own(ResourceObjectInputs.receive([transfer], mode === "budget" ? 0 : 4096));
	if (mode === "changed-after") await writeFile(input, "modified");
	const expected = await filesystem.stat(input, { bigint: true });
	const { output: result, dependencies } = await TimelineInterval.collect(() => received.inputs.borrow(input, expected, 1024, () => mode !== "rejected"));
	expect(result?.content?.toString()).toBe(mode === "fresh" ? "original" : undefined);
	expect(dependencies.length > 0).toBe(mode === "fresh");
	if (result) expect(TimelineInterval.serialize(result.computation!)?.root).toBe(TimelineInterval.serialize(captured.computation!)?.root);
	loan.release(); await closing; expect(closed).toBe(true);
	expect(CapturedFilesystemObject.receive(transfer)).toBeUndefined();
	if (mode === "fresh") expect((await received.inputs.borrow(input, captured.stat, 1024))?.content?.toString()).toBe("original");
	await received.dispose(); expect(await pins()).toBe(0);
	expect(await received.inputs.borrow(input, captured.stat, 1024)).toBeUndefined();
});


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
	const planner = new ProcessReusePlanner({ store }), registry = own(new ProcessHandoffRegistry<string>(store, 64 * 1024));
	const lookup = async (certificates?: readonly ProcessProvenanceCertificate[], excludedCertificates?: ReadonlySet<ProcessProvenanceCertificate["id"]>, artifacts?: ReadonlyMap<ProcessProvenanceCertificate["id"], VerifiedArtifactClosure>) => {
		const plan = await planner.plan({ ...request, excludedCertificates, ...(certificates ? { live: { certificate: certificates, artifacts, acceptedTaints: ["clock"] } } : {}) });
		return plan.kind === "completed_replay" ? plan : undefined;
	};
	const output = (plan: Awaited<ReturnType<ProcessReusePlanner["plan"]>> | undefined) => plan?.kind === "completed_replay" ? plan.certificate.result.journal.map(event =>
		event.kind === "output" ? plan.artifacts.read(event.data).toString() : "").join("") : undefined;
	const common = { key: request.weakKey, scope: { sessionID: "system", turnID: "one" }, lookup };
	const reserve = async (ownership = new ProcessHandoffOwnership()) => {
		const result = await registry.acquire({ ...common, producer: { ownership, executablePath: request.executablePath } });
		if (result.kind !== "work") throw new Error("expected producer");
		registry.observeRunning(common.key, result.work, async () => false);
		return result.work;
	};
	const actor = (options: { signal?: AbortSignal; lookup?: typeof lookup } = {}) => registry.acquire({ ...common, ...options });
	const publish = (work: Awaited<ReturnType<typeof reserve>>, value: string, createdAt = 1, oneShot = true, computation?: TimelineInterval) => store.withPublication(async () => {
		const produced = await produce(value, createdAt, oneShot), certificate = computation ? sealProcessCertificate({ ...produced,
			result: { ...produced.result, computation: TimelineInterval.serialize(computation) } }) : produced;
		await registry.publish(request.weakKey, work, certificate, () => planner.publishCompleted(certificate), undefined, computation); return certificate;
	});
	const consume = async (pending: ReturnType<typeof actor>, expected: string | undefined) => {
		const result = await pending; expect(result.kind).toBe(expected === undefined ? "miss" : "hit");
		if (result.kind === "hit") { expect(output(result.plan)).toBe(expected); result.settle?.(true); } return result;
	};
	return { root, input, store, request, common, produce, lookup, output, registry, reserve, actor, publish, consume };
}

test("requires current owner authority for a one-shot result in its original turn", async () => {
	const f = await fixture();
	let permitted = true;
	const acceptScope = vi.fn(() => permitted), owner = new ProcessHandoffOwnership(undefined, acceptScope), work = await f.reserve(owner);
	await f.publish(work, "owned result");
	permitted = false;
	await f.consume(f.actor(), undefined);
	permitted = true;
	await f.consume(f.actor(), "owned result");
	await f.consume(f.actor(), undefined);
	expect(acceptScope).toHaveBeenCalledWith(f.common.scope, true); // Keep the live owner's weakly held callback alive.
});

test.each(["released", "consumed", "lost", "raced", "clear", "gc", "copy", "damaged"])("preserves checkpoint claim ownership across store reopen (%s)", async mode => {
	const f = await fixture({ orphanGraceMs: 0 }), computation = TimelineInterval.serialize(new TimelineInterval(0, 20))!;
	const image = await f.store.artifacts.put("saved process memory"), produced = await f.produce("prefix output", 1, true);
	const certificate = sealProcessCertificate({ ...produced, result: { replayProfile: "buffered_noninteractive", journal: produced.result.journal,
		continuation: { imageDigest: image.digest, imageBytes: image.size }, computation } });
	const body = { sessionID: "session", sourceRoot: f.root, physicalRoot: f.root, context: sha256Digest("boot and native build"),
		producer: { pid: process.pid, start: "1" }, certificate, computation, image };
	let ticket = "";
	await f.store.withPublication(async () => { ticket = await f.store.issueProcessCheckpoint(); await f.store.putProcessCheckpoint(ticket, body); });
	await f.store.dispose(); const reopened = storeAt(f.root, { orphanGraceMs: 0 });
	const candidates = async (store = reopened) => { const result = []; for await (const candidate of store.processCheckpoints(certificate.weakKey, certificate.prototype.executablePath, "session")) result.push(candidate); return result; };
	expect(await candidates()).toMatchObject([{ ticket, certificate: { id: certificate.id } }]);
	expect((await reopened.stats()).certificates).toBe(1);
	await expect(reopened.claimProcessCheckpoint(ticket, sha256Digest("other certificate"))).resolves.toBeUndefined();
	if (mode === "copy") {
		await reopened.drain(); const copy = await directories.create(); await filesystem.cp(f.root, copy, { recursive: true });
		const cloned = storeAt(copy); expect(await candidates(cloned)).toEqual([]);
		await expect(cloned.claimProcessCheckpoint(ticket, certificate.id)).resolves.toBeUndefined();
		await expect(cloned.issueProcessCheckpoint()).rejects.toThrow("issuer is unavailable"); return;
	}
	if (mode === "damaged") {
		storeMetadata(f.root, database => database.prepare("UPDATE process_checkpoints SET value=? WHERE ticket=?").run("{}", ticket));
		expect(await candidates()).toEqual([]); await expect(reopened.claimProcessCheckpoint(ticket, certificate.id)).resolves.toBeUndefined();
		await reopened.gc(); expect((await reopened.stats()).certificates).toBe(0); return;
	}
	if (mode === "lost") {
		const owner = peer(f.root, certificate, { ticket, action: "hold" }); expect(await owner.noticed).toBe("claimed");
		const competing = peer(f.root, certificate, { ticket, action: "consume" }); expect(await competing.noticed).toBe("miss"); await competing.completed;
		owner.child.kill("SIGKILL"); await expect(owner.completed).rejects.toThrow("store peer exited");
		await expect(reopened.claimProcessCheckpoint(ticket, certificate.id)).resolves.toBeUndefined(); expect(await candidates()).toEqual([]);
		await expect(reopened.putProcessCheckpoint(ticket, body)).rejects.toThrow("no longer publishable"); return;
	}
	if (mode === "raced") {
		const owners = [0, 1].map(() => peer(f.root, certificate, { ticket, action: "consume" }));
		expect((await Promise.all(owners.map(owner => owner.noticed))).sort()).toEqual(["claimed", "miss"]);
		await Promise.all(owners.map(owner => owner.completed)); expect(await candidates()).toEqual([]); return;
	}
	const claim = await reopened.claimProcessCheckpoint(ticket, certificate.id); expect(claim).toBeDefined();
	await expect(storeAt(f.root).claimProcessCheckpoint(ticket, certificate.id)).resolves.toBeUndefined();
	if (mode === "clear") await reopened.clear();
	if (mode === "gc") { reopened.configure({ maxBytes: 1 }); await reopened.gc(); }
	await claim!(mode === "consumed"); await claim!(false);
	if (mode === "released") { expect(await candidates()).toHaveLength(1); await (await reopened.claimProcessCheckpoint(ticket, certificate.id))!(true); }
	expect(await candidates()).toEqual([]);
	await expect(reopened.putProcessCheckpoint(ticket, body)).rejects.toThrow("no longer publishable");
	const next = await reopened.issueProcessCheckpoint(); expect(next).not.toBe(ticket);
	expect(Number(next.split(":").at(-1))).toBeGreaterThan(Number(ticket.split(":").at(-1)));
});
