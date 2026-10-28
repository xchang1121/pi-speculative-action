import { readdir, unlink, utimes, writeFile } from "node:fs/promises";
import { closeSync, constants, fstatSync, openSync } from "node:fs";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, expect, test, vi } from "vitest";
import {
	ArtifactCAS, captureFileDependency, createExecPrototype, processWeakKey, ProcessReusePlanner, ProvenanceCertificateStore, referencedArtifacts,
	sealProcessCertificate, sha256Digest, type ProcessProvenanceCertificate, type VerifiedArtifactClosure,
} from "../src/process-reuse.ts";
import { ProcessHandoffOwnership, ProcessHandoffRegistry } from "../src/process-handoff.ts";
import { effectCommitFailure } from "../src/effect-transaction.ts";
import { CapturedFilesystemObject, FilesystemObjectLease, cachedCapture, captureFileDigest, captureStableFile, hashExecutableFile, openFilesystemReadView, type FilesystemReadView } from "../src/filesystem-evidence.ts";
import { ResourceObjectInputs } from "../src/agent-execution-world.ts";
import { waitForCompletion } from "../src/runtime-lifecycle.ts";
import { observeStrace, traceTail, writesWithin } from "../src/strace-observer.ts";
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

test.each(["partial", "unparsed", "unstamped", "read-error", "short-read", "truncated"])("keeps incomplete namespace evidence unsafe after retry (%s)", async mode => {
	const { trace, tail, append } = await processTrace(mode === "partial" ? '1.000000 rename("/work"' : mode === "unparsed" ? "1.000000 ???\n" : mode === "unstamped" ? "???\n" : "1.000000 +++ exited with 0 +++\n");
	if (mode === "read-error") {
		vi.spyOn(filesystem, "open").mockRejectedValueOnce(new Error("trace unavailable"));
		await expect(tail.read()).rejects.toThrow("trace unavailable");
	} else if (mode === "short-read") {
		const handle = await filesystem.open(trace, "r");
		vi.spyOn(handle, "read").mockResolvedValueOnce({ bytesRead: 0, buffer: Buffer.alloc(0) } as never);
		vi.spyOn(filesystem, "open").mockResolvedValueOnce(handle);
		await expect(tail.read()).rejects.toThrow("trace changed while reading");
	} else if (mode === "truncated") {
		await tail.read(); await writeFile(trace, "");
		await expect(tail.read()).rejects.toThrow("trace shrank");
	} else expect((await tail.read()).unsafe).toBe(true);
	expect((await append(mode === "partial" ? ', "/held") = 0\n' : "1.000001 +++ exited with 0 +++\n")).unsafe).toBe(true);
});

test.each([
	["write", '3</work/input>, "new", 3', "3"], ["writev", '3</work/input>, [{iov_base="new", iov_len=3}], 1', "3"],
	["pwrite64", '3</work/input>, "new", 3, 0', "3"], ["pwritev", '3</work/input>, [{iov_base="new", iov_len=3}], 1, 0', "3"],
	["pwritev2", '3</work/input>, [{iov_base="new", iov_len=3}], 1, 0, 0', "3"], ["ftruncate", "3</work/input>, 3", "0"],
	["fchmod", "3</work/input>, 0600", "0"], ["fchown", "3</work/input>, 1000, 1000", "0"], ["fallocate", "3</work/input>, 0, 0, 4096", "0"],
	["sendfile", "3</work/input>, 4</work/source>, NULL, 3", "3"], ["copy_file_range", "4</work/source>, NULL, 3</work/input>, NULL, 3, 0", "3"],
	["splice", "4<pipe:[17]>, NULL, 3</work/input>, NULL, 3, 0", "3"],
	["mmap", "NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, 3</work/input>, 0", "0x7f000000"],
	["mmap", "NULL, 4096, PROT_READ, MAP_SHARED, 3</work/input>, 0", "0x7f000000"],
])("tracks writes through an already open file description (%s, %s)", async (call, args, result) => {
	const { tail } = await processTrace(`1.000000 ${call}(${args}) = ${result}\n`);
	await tail.read(); expect(tail.unsafe).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 0, 2000)).toBe(true);
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 1500, 2000)).toBe(call === "mmap");
	expect(writesWithin(tail.writes, new Set(["/work/other"]), 0, 2000)).toBe(false);
});

test.each(["3", "3<pipe:[17]>", '3</work/input>'])("keeps unresolved descriptor writes conservative (%s)", async descriptor => {
	const { tail } = await processTrace(`1.000000 write(${descriptor}, "new", 3) = 3\n`); await tail.read();
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 0, 2000)).toBe(descriptor !== "3<pipe:[17]>");
	expect(writesWithin(tail.writes, new Set(["/unrelated"]), 0, 2000)).toBe(descriptor === "3");
});

test("keeps an empty process trace in execution root selection", async () => {
	const { prefix } = await processTrace('1.000000 execve("/tools/worker", ["worker"], []) = 0\n1.000001 +++ exited with 0 +++\n');
	await writeFile(`${prefix}.100`, "");
	expect((await observeStrace(prefix, "/tools/worker", "/elsewhere")).incompleteReasons).toEqual(["trace_root_ambiguous:100,101"]);
});

test.each(["wait4", "waitid", "waitpid"].flatMap(call => ["partial", "resumed"].map(mode => ({ call, mode }))))("does not mistake a pending process wait for namespace mutation ($call, $mode)", async ({ call, mode }) => {
	const begin = `101 1.000000 ${call}(${call === "waitid" ? "P_PID, " : ""}42, `, end = `NULL, ${call === "waitid" ? "WEXITED" : "0"}${call === "waitpid" ? "" : ", NULL"}) = ${call === "waitid" ? 0 : 42}\n`;
	const { tail, append } = await processTrace(begin + (mode === "resumed" ? "<unfinished ...>\n102 1.000001 +++ exited with 0 +++\n" : ""), "stream");
	expect((await tail.read()).unsafe).toBe(false);
	expect((await append((mode === "resumed" ? `101 1.000002 <... ${call} resumed>` : "") + end)).unsafe).toBe(false);
	expect(tail.writes).toEqual([]);
	expect((await append('101 1.000003 rename("/work"')).unsafe).toBe(true);
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

test.each(['3</tools>, "worker", ["worker"], [], 0', '3</tools/worker>, "", ["worker"], [], AT_EMPTY_PATH'])("selects execveat by its directory or descriptor binding (%s)", async args => {
	const { prefix } = await processTrace(`1.000000 execveat(${args}) = 0\n1.000001 +++ exited with 0 +++\n`);
	const observed = await observeStrace(prefix, "/tools/worker", "/elsewhere");
	expect(observed.incompleteReasons).toEqual([]);
	expect(observed.paths).toContainEqual({ path: "/tools/worker", role: "executable" });
});

test.each([false, true])("attributes private external writes only to an owned brokered child (%s)", async owned => {
	const target = "/tmp/compiler.s", query = (size: number) => `statx(AT_FDCWD, "${target}", 0, STATX_SIZE, {stx_mask=STATX_SIZE, stx_size=${size}}) = 0`;
	const { prefix } = await processTrace([
		'execve("/tools/worker", ["worker"], []) = 0', query(1), query(2), "+++ exited with 0 +++\n",
	].join("\n"));
	const observed = await observeStrace(prefix, "/tools/worker", "/work", {
		brokeredWrites: pid => pid === (owned ? 101 : 102) ? [target] : undefined,
	});
	expect(observed.complete).toBe(true);
	expect(observed.external).toEqual(owned ? [target] : []);
	expect(observed.metadataChanges ?? []).toEqual(owned ? [] : [target]);
});

test("tracks shared cwd and resumed writes across growing ordered trace chunks", async () => {
	const { tail, append } = await processTrace('101 1.000000 clone(flags=CLONE_FS|SIGCHLD) = 102\n102 1.000001 chdir("/new") = 0\n101 1.000002 openat(AT_FDCWD, "output", O_WRONLY|O_CREAT, 0600) = 3</new/output>\n', "stream");
	await tail.read();
	expect(writesWithin(tail.writes, new Set(["/new/output"]), 0, 2000)).toBe(true);
	await append('102 1.000003 renameat(AT_FDCWD, "/new", AT_FDCWD, "/held" <unfinished ...>\n');
	await append('102 1.000004 <... renameat resumed>) = 0\n');
	expect(writesWithin(tail.writes, new Set(["/held"]), 0, 2000)).toBe(true);
	expect(tail.writes).toHaveLength(2);
	expect(tail.unsafe).toBe(true);
});

test.each(["MODE", "SHLVL", "OLDPWD"])("includes %s in an arbitrary executable's identity", name => {
	const keys = [undefined, "", "before", "after"].map(value => processWeakKey(createExecPrototype({
		...PROCESS_CERTIFICATE.prototype, argv: ["tool"], environment: { [name]: value },
	})));
	expect(new Set(keys).size).toBe(4);
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

test.skipIf(process.platform !== "linux").for(["pin", "metadata", "content"])("keeps a closed object handle separate from a reused descriptor (%s)", async mode => {
	const input = path.join(await directories.create(), "input"), moved = `${input}-moved`, readable = mode === "content";
	const entered = deferred(), release = deferred(), gaps: number[] = [];
	let view: FilesystemReadView | undefined, source: FilesystemObjectLease | undefined, receiver: FilesystemObjectLease | undefined;
	let loan: ReturnType<FilesystemObjectLease["lend"]>, replacement: number | undefined, borrowing: Promise<void> | undefined, closing: Promise<void> | undefined;
	try {
		await writeFile(input, "original");
		let handle, identity;
		if (mode === "pin") { view = await openFilesystemReadView(); handle = await view.pin(input); identity = await handle.stat({ bigint: true }); }
		else {
			const opened = await filesystem.open(input, readable ? constants.O_RDONLY | constants.O_NOATIME : 0x200000);
			identity = await opened.stat({ bigint: true }); source = new FilesystemObjectLease(opened, identity, readable);
			loan = source.lend(); expect(loan).toBeDefined();
			if (!readable) expect(FilesystemObjectLease.receive(structuredClone(loan!.transfer), true)).toBeUndefined();
			expect(FilesystemObjectLease.receive({ ...structuredClone(loan!.transfer), process: process.pid + 1 }, readable)).toBeUndefined();
			const received = FilesystemObjectLease.receive(structuredClone(loan!.transfer), readable); expect(received).toBeDefined(); handle = received!.handle;
		}
		const original = handle, descriptor = original.fd;
		receiver = new FilesystemObjectLease(original, identity, readable);
		await filesystem.rename(input, moved); await writeFile(input, "replacement");
		let closed = false;
		borrowing = receiver.borrow(async handle => {
			entered.resolve(); await release.promise;
			const current = await handle.stat({ bigint: true }); expect([current.dev, current.ino]).toEqual([identity.dev, identity.ino]);
			if (readable) { const bytes = Buffer.alloc(8); expect((await handle.read!(bytes, 0, bytes.length, 0)).bytesRead).toBe(8); expect(bytes.toString()).toBe("original"); }
		});
		closing = receiver.dispose().then(() => { closed = true; });
		await entered.promise; await nextTurn(); expect(closed).toBe(false); expect(receiver.lend()).toBeUndefined();
		release.resolve(); await borrowing; await closing; expect(closed).toBe(true);
		replacement = openSync(input, constants.O_RDONLY);
		while (replacement < descriptor) { gaps.push(replacement); replacement = openSync(input, constants.O_RDONLY); }
		expect(replacement).toBe(descriptor);
		const identityAfterReuse = fstatSync(replacement, { bigint: true }); expect(identityAfterReuse.ino).not.toBe(identity.ino);
		const queries = await Promise.allSettled([original.stat({ bigint: true }), ...readable ? [original.read!(Buffer.alloc(11), 0, 11, 0)] : []]);
		let duplicateClose = "ok", replacementAlive = false;
		try { await original.close(); } catch (error) { duplicateClose = (error as NodeJS.ErrnoException).code ?? String(error); }
		try { replacementAlive = fstatSync(replacement, { bigint: true }).ino === identityAfterReuse.ino; } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EBADF") throw error; replacement = undefined; }
		expect({ queries: queries.map(result => result.status === "rejected" ? result.reason.code : "fulfilled"), duplicateClose, replacementAlive })
			.toEqual({ queries: readable ? ["EBADF", "EBADF"] : ["EBADF"], duplicateClose: "ok", replacementAlive: true });
		expect((await filesystem.readFile(input)).toString()).toBe("replacement");
	} finally {
		release.resolve(); await Promise.allSettled([borrowing, closing]);
		if (replacement !== undefined) closeSync(replacement);
		for (const descriptor of gaps) closeSync(descriptor);
		await receiver?.dispose(); loan?.release(); await source?.dispose(); await view?.close();
	}
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
	const owner = new ProcessHandoffOwnership(undefined, () => permitted), work = await f.reserve(owner);
	await f.publish(work, "owned result");
	permitted = false;
	await f.consume(f.actor(), undefined);
	permitted = true;
	await f.consume(f.actor(), "owned result");
	await f.consume(f.actor(), undefined);
});

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
	const bytes = Buffer.alloc(64 * 1024 * 1024);
	await writeFile(f.input, bytes);
	const certificate = sealProcessCertificate({ ...PROCESS_CERTIFICATE, dependencyCertificate: { complete: true, taints: [],
		dependencies: [{ kind: "file", role: "input", path: "/workspace/input", contentDigest: sha256Digest(bytes) }] } });
	for (const close of [false, true]) {
		const paused = hold(), controller = new AbortController(), opened: filesystem.FileHandle[] = [], native = await vi.importActual<typeof filesystem>("node:fs/promises"); let readBytes = 0;
		vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
			const handle = await native.open(...args), read = handle.read; opened.push(handle);
			vi.spyOn(handle, "read").mockImplementation(async (...args) => {
				const result = await Reflect.apply(read, handle, args); readBytes += result.bytesRead; await paused.wait(); return result;
			}); return handle;
		});
		const pending = track(planner.plan({ ...f.request, signal: controller.signal, live: { certificate, acceptedTaints: [] } })); await paused.entered;
		const closing = close ? store.dispose() : (controller.abort(), pending.then(() => undefined, () => undefined));
		expect((await waitForCompletion(closing, undefined, 30)).status).toBe("deadline");
		if (close) await expect(planner.plan(f.request)).rejects.toThrow("closed");
		paused.release(); await expect(pending).rejects.toThrow(); await closing;
		expect(readBytes).toBe(1024 * 1024); expect(opened.length).toBeGreaterThan(0); expect(opened.every(handle => handle.fd === -1)).toBe(true);
	}
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
	let drained = false; f.store.configure({ maxCertificates: 1 });
	// Keep "next" available while its new certificate waits behind the deletion transaction.
	await f.store.put(await f.produce(["ready", "next"], 64)); track(f.store.gc());
	await pause.entered;
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
	const writer = storeAt(f.root), writing = hold(), owner = phase === "scan" ? peer(f.root) : undefined;
	if (owner) expect(await owner.noticed).toBe("held");
	const commands = vi.spyOn(DatabaseSync.prototype, "exec");
	const publication = track(writer.withPublication(async () => {
		const certificate = await f.produce("protected until sealed", 100, false, writer.artifacts);
		for (const reference of referencedArtifacts(certificate)) await utimes(writer.artifacts.artifactPath(reference.digest), new Date(0), new Date(0));
		await writing.wait(); await writer.put(certificate); return certificate;
	}));
	if (owner) await expect.poll(() => commands.mock.results.some(result => result.type === "throw" && [5, 6].includes(Number(result.value.errcode) & 0xff))).toBe(true);
	else await writing.entered;
	const lookup = new ProcessReusePlanner({ store: writer }).plan(f.request);
	expect(await waitForCompletion(lookup, undefined, 200)).toMatchObject({ status: "completed", value: { kind: "completed_replay" } });
	expect(f.output(await lookup)).toBe("sharednext"); expect((await writer.stats()).certificates).toBe(1);
	const queued = track(writer.artifacts.put("queued write"));
	if (phase !== "scan") expect(await waitForCompletion(queued.catch(error => error.name), undefined, 1500)).toEqual({ status: "completed", value: "TimeoutError" });
	const collecting = owner ? undefined : peer(f.root, undefined, [], "collect");
	if (collecting) { await collecting.noticed; expect((await waitForCompletion(collecting.completed, undefined, 30)).status).toBe("deadline"); }
	const disposed = writer.dispose(); expect((await waitForCompletion(disposed, undefined, owner ? 200 : 30)).status).toBe(owner ? "completed" : "deadline");
	writing.release();
	if (owner) { await expect(publication).rejects.toThrow(); owner.child.kill("SIGKILL"); await owner.completed.catch(() => undefined); }
	else {
		const certificate = await publication; await collecting!.completed;
		expect(await recovered.get(certificate.id)).toEqual(certificate); expect(f.output(await f.lookup())).toBe("protected until sealed");
	}
	await disposed; await expect(queued).rejects.toThrow(); await expect(writer.put(remote)).rejects.toThrow("closed");
});

test.each(["existing", "new", "borrowed"])("selects a %s ready producer and retains its complete result through collection, borrowing and eviction", async arrival => {
	const f = await fixture(), first = await f.reserve(), entered = hold();
	let second = arrival === "existing" ? await f.reserve() : undefined;
	f.registry.observeRunning(f.request.weakKey, first, async () => { await entered.wait(); return true; });
	const loan = arrival === "borrowed" ? f.registry.lend(f.common.scope) : undefined;
	const receiver = loan && own(new ProcessHandoffRegistry<string>(f.store, 64 * 1024));
	if (loan) cleanup.push(loan.settle);
	const actor = track(receiver && loan ? receiver.acquire({ ...f.common, signal: AbortSignal.timeout(5000), borrow: async (key, excluded, signal) => {
		const borrowed = await loan.borrow(key, excluded, signal); if (!borrowed) return;
		const received = await receiver.receive(structuredClone(borrowed.transfer), f.common.scope, () => {}); cleanup.push(received);
		return { records: received.records, joined: borrowed.joined };
	} }) : f.actor({ signal: AbortSignal.timeout(5000) }));
	await entered.entered; const broken = await f.produce("broken", 0, true);
	await unlink(f.store.artifacts.artifactPath(referencedArtifacts(broken)[0]!.digest));
	await expect(f.registry.publish(f.request.weakKey, first, broken, async () => false)).rejects.toThrow("complete owned result");
	second ??= await f.reserve(); f.registry.observeRunning(f.request.weakKey, second, async () => { await f.publish(second!, "fast result"); return true; });
	expect(await f.consume(actor, "fast result")).toMatchObject({ producer: receiver ? { scope: second.scope } : second, waiting: [expect.any(TimelineInterval)] });
	const retired = loan?.borrow(f.common.key, new Set()); await nextTurn(); loan?.settle();
	expect(await retired).toBeUndefined(); expect(first.signal.aborted).toBe(false); receiver?.dispose();
	entered.release(); const later = await f.reserve();
	const graph = new TimelineInterval(0, 10, Array.from({ length: 8192 }, (_, index) => ({ computation: new TimelineInterval(index, index + 1), reused: true })));
	f.registry.configure(8, 8 * 1024 * 1024);
	const slow = await f.publish(first, "slow result", 2, false, graph); await f.consume(f.registry.acquire({ ...f.common, producer: { ownership: first.ownership, executablePath: f.request.executablePath } }), "slow result");
	await writeFile(f.input, "changed input"); const changed = await f.produce("changed result", 3, true), lookup = vi.fn(f.lookup);
	await f.consume(f.actor({ lookup }), undefined); expect(lookup).toHaveBeenCalledTimes(2);
	const restore = async () => {
		await writeFile(f.input, "current input"); await f.registry.publish(f.request.weakKey, later, changed, async () => false);
	};
	// Reconsider the old result after publication during join preparation or history lookup.
	f.registry.observeRunning(f.request.weakKey, later, async () => { await restore(); return true; });
	await f.consume(f.actor({ lookup: async (...args) => {
		const plan = await f.lookup(...args); if (!args[0] && arrival === "new") await restore(); return plan;
	} }), "slow result");
	await f.store.put(await f.produce("saved", 3));
	const reference = referencedArtifacts(slow)[0]!, target = f.store.artifacts.artifactPath(reference.digest), borrowed = await f.actor();
	if (borrowed.kind !== "hit") throw new Error("expected owned result");
	expect(borrowed.plan.certificate.result.computation?.nodes).toHaveLength(8193);
	await writeFile(target, "damage"); await utimes(target, new Date(0), new Date(0)); await peer(f.root, undefined, [], "collect").completed;
	expect(await f.store.artifacts.get(reference)).toBeUndefined(); expect(f.output(borrowed.plan)).toBe("slow result");
	borrowed.plan.artifacts.read(reference).fill(0); expect(f.output(borrowed.plan)).toBe("slow result");
	const mutable = Buffer.from("slow result"), contentLoan = vi.spyOn(borrowed.plan.artifacts, "lend").mockImplementation(() => {
		queueMicrotask(() => mutable.fill(0)); return { transfer: [{ reference, bytes: Buffer.from(mutable) }], release: () => {} };
	});
	expect(f.output(await f.lookup([slow], undefined, new Map([[slow.id, borrowed.plan.artifacts]])))).toBe("slow result");
	contentLoan.mockImplementation(() => ({ transfer: [{ reference, bytes: Buffer.alloc(reference.size) }], release: () => {} }));
	expect(await f.lookup([slow], undefined, new Map([[slow.id, borrowed.plan.artifacts]]))).toBeUndefined(); contentLoan.mockRestore();
	f.registry.configure(8, 1); expect(f.registry.results(f.request.weakKey, { sessionID: "system", turnID: "one" })).toHaveLength(1);
	expect(first.computation).toBe(graph);
	borrowed.settle?.(false); expect(f.registry.hasResults).toBe(false);
	expect(first.computation).toBeUndefined();
	await f.consume(f.actor(), "saved"); await writeFile(f.input, "changed input"); await f.consume(f.actor(), undefined);
	f.registry.configure(8, arrival === "existing" ? 1024 : 64 * 1024);
	const oversized = await f.reserve(), metadata = sealProcessCertificate({ ...PROCESS_CERTIFICATE,
		result: { ...PROCESS_CERTIFICATE.result, computation: arrival === "existing" ? TimelineInterval.serialize(graph) : undefined } });
	await f.registry.publish(f.request.weakKey, oversized, metadata, async () => false, undefined, arrival !== "existing" ? graph : undefined);
	expect(f.registry.hasResults).toBe(false); expect(oversized.computation).toBeUndefined();
	f.registry.configure(8, 64 * 1024); await f.publish(await f.reserve(), "bounded"); await f.consume(f.actor(), "bounded");
	f.registry.clearCompleted(); f.store.configure({ maxBytes: 8192 });
	for (let index = 0; index < 3; index++) {
		const acquired = await f.registry.acquire({ ...f.common, lookup: async () => undefined,
			producer: { ownership: new ProcessHandoffOwnership(), executablePath: f.request.executablePath } });
		if (acquired.kind !== "work") throw new Error("expected producer");
		await f.registry.publish(f.common.key, acquired.work, await f.produce(String(index).repeat(4096), index + 10, true), async () => false);
	}
	const retained = () => f.registry.results(f.common.key, f.common.scope);
	expect(retained()).toHaveLength(2); expect(retained().map(certificate => certificate.createdAt)).toEqual([12, 11]);
	expect(retained().flatMap(referencedArtifacts).reduce((sum, reference) => sum + reference.size, 0)).toBe(8192);
	f.store.configure({ maxBytes: 4096 }); f.registry.configure(8);
	expect(retained()).toHaveLength(1);
	await f.consume(f.actor(), "2".repeat(4096));
});

test.each(["released", "consumed", "cancelled", "raced", "whole-released", "whole-consumed", "forwarded", "forwarded-stale", "forwarded-whole", "producer-consumed", "producer-released",
	"loan-released", "loan-consumed", "loan-lost", "loan-corrupt", "loan-stale", "loan-foreign", "loan-call", "loan-double", "loan-competing", "loan-retired", "loan-nested", "loan-nested-retired", "loan-local", "loan-whole", "loan-whole-raced", "loan-nested-whole"])("settles a %s one-shot claim without losing another Actor's result", async outcome => {
	const f = await fixture({ gcIntervalMs: 0, orphanGraceMs: 0, maxBytes: 128 * 1024 }), ownership = new ProcessHandoffOwnership(), releaseWhole = hold();
	const work = await f.reserve(ownership), certificate = await f.publish(work, "computed result", 1, true, new TimelineInterval(0, 10));
	const repeated = await f.reserve(ownership); f.registry.complete(f.common.key, repeated);
	const binding = f.registry.bind(f.common.key, work, "learned invocation"), forward = outcome.startsWith("forwarded"), destination = new ProcessHandoffOwnership();
	if (outcome.startsWith("loan-")) {
		const whole = outcome.includes("whole"), raced = outcome === "loan-whole-raced";
		const scope = outcome === "loan-foreign" ? { ...f.common.scope, sessionID: "foreign" } : f.common.scope;
		const competing = outcome === "loan-competing" ? f.registry.lend({ ...scope, callID: "unrelated" }) : undefined;
		const loan = f.registry.lend(scope); cleanup.push(loan.settle, () => competing?.settle());
		let transfer = structuredClone(loan.transfer);
		expect(await loan.borrow(f.common.key, new Set())).toBeUndefined(); // A lending lifetime cannot wait for its own reservations.
		expect(loan.binding(0)).toBe(outcome === "loan-foreign" ? undefined : binding);
		const receiver = own(new ProcessHandoffRegistry<string>(f.store, 64 * 1024));
		if (outcome.startsWith("loan-nested")) {
			const relay = own(new ProcessHandoffRegistry<string>(f.store, 64 * 1024)); cleanup.push(await relay.receive(transfer, scope, () => {}));
			const nested = relay.lend(scope); cleanup.push(nested.settle); transfer = structuredClone(nested.transfer);
		}
		if (outcome === "loan-corrupt") {
			const artifact = transfer.results[0]!.artifacts[0]!;
			if ("bytes" in artifact) artifact.bytes.fill(0);
			else {
				const corrupt = own((await f.store.loadArtifacts([await f.store.artifacts.put(Buffer.alloc(artifact.reference.size))]))!), loan = corrupt.lend(); cleanup.push(loan.release);
				Object.assign(artifact, loan.transfer[0], { reference: artifact.reference });
			}
		}
		if (outcome === "loan-stale") await writeFile(f.input, "changed input");
		const receiving = receiver.receive(transfer, scope, () => { throw new Error("a lookup cannot issue adoption feedback"); });
		if (outcome === "loan-corrupt") await expect(receiving).rejects.toThrow("integrity check failed");
		else cleanup.push(await receiving);
		const duplicate = outcome === "loan-double" ? own(new ProcessHandoffRegistry<string>(f.store, 64 * 1024)) : undefined;
		if (duplicate) cleanup.push(await duplicate.receive(transfer, scope, () => {}));
		expect(receiver.bindings(scope)).toEqual([]);
		const apply = vi.fn(async () => { if (!whole) throw effectCommitFailure(new Error("unchanged effects"), "recoverable"); return "whole branch"; });
		if (whole && !raced) await expect(ownership.commit(apply)).resolves.toBe("whole branch");
		else if (!raced) await expect(ownership.commit(apply)).rejects.toMatchObject({ disposition: "recoverable" });
		if (outcome.endsWith("retired")) loan.settle();
		if (outcome === "loan-local") await f.consume(f.actor(), "computed result");
		const result = await receiver.acquire({ ...f.common, scope: outcome === "loan-call" ? { ...scope, callID: "another-call" } : scope, lookup: async (...args) => {
			const plan = await f.lookup(...args); if (raced && plan) await expect(ownership.commit(apply)).resolves.toBe("whole branch"); return plan;
		} }),
			accepted = ["loan-released", "loan-consumed", "loan-lost", "loan-double", "loan-competing", "loan-nested"].includes(outcome);
		expect(result.kind).toBe(accepted ? "hit" : "miss");
		if (result.kind === "hit") {
			expect(f.output(result.plan)).toBe("computed result"); expect(result.producer?.computation).toBeDefined();
			await expect(ownership.commit(async () => {})).rejects.toMatchObject({ disposition: "recoverable" });
			if (duplicate) expect((await duplicate.acquire(f.common)).kind).toBe("miss");
			competing?.settle();
			if (outcome !== "loan-lost") result.settle?.(outcome !== "loan-released");
		}
		receiver.dispose(); loan.settle(); loan.settle(); // A vanished receiver cannot return its held claim as unused.
		const used = accepted && outcome !== "loan-released" || outcome === "loan-local" || whole;
		if (whole) await expect(ownership.commit(apply)).resolves.toBe("whole branch");
		else if (used) await expect(ownership.commit(async () => {})).rejects.toThrow("partially consumed");
		expect(apply).toHaveBeenCalledOnce();
		await writeFile(f.input, "current input"); await f.consume(f.actor(), used ? undefined : "computed result");
		expect(f.registry.resolveBinding(binding!, f.common.scope)).toBe("learned invocation");
		return;
	}
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
	const first = whole ? undefined : await (forward || outcome.startsWith("producer") ? f.registry.acquire({ ...f.common, producer: { ownership: destination, executablePath: f.request.executablePath } }) : f.actor());
	if (first && first.kind !== "hit") throw new Error("expected first claim");
	const second = earlier ?? next(); racing.release(); await checked.promise; await nextTurn(); expect(settled).toBe(false);
	vi.spyOn(f.store, "gc").mockRejectedValueOnce(new Error("transient collection failure"));
	await expect(f.store.withPublication(async () => {
		for (let index = 0; index < 16; index++) await f.store.artifacts.put(`${index}`.padEnd(64 * 1024, "x"));
		throw new Error("abandoned before certificate");
	})).rejects.toThrow("abandoned before certificate");
	await expect.poll(async () => (await f.store.stats()).artifacts).toBe(0);
	expect(await f.store.artifacts.load(referencedArtifacts(certificate))).toBeUndefined();
	if (outcome === "forwarded-stale") await writeFile(f.input, "changed input");
	if (outcome === "forwarded-whole") await destination.commit(async () => {});
	if (outcome === "cancelled") controller.abort(); else { first?.settle?.(consumed || forward, outcome !== "producer-consumed"); releaseWhole.release(); }
	const result = await f.consume(second, consumed || outcome === "cancelled" || outcome === "forwarded-stale" || outcome === "forwarded-whole" ? undefined : "computed result");
	if (result.kind === "hit") expect(result.plan.certificate).toEqual(certificate);
	if (forward) {
		expect(f.registry.bindings(f.common.scope)).toEqual([binding]); expect(f.registry.resolveBinding(binding!, f.common.scope)).toBe("learned invocation");
		await expect(ownership.commit(async () => {})).rejects.toThrow("partially consumed");
		if (outcome === "forwarded") await expect(destination.commit(async () => {})).rejects.toThrow("partially consumed");
	}
	first?.settle?.(false); releaseWhole.release(); await whole;
	await f.consume(f.actor(), outcome === "cancelled" ? "computed result" : undefined);
});
