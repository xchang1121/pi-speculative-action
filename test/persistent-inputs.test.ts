import { afterEach, expect, test, vi } from "vitest";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { mkdir, mkdtemp, open, link, lstat, lutimes, rm, symlink, utimes, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import type { ArtifactReference } from "../src/artifact-reference.ts";
import type { Sha256Digest } from "../src/stable-value-hash.ts";
import { ContentChunkPool, CONTENT_CHUNK_BYTES, captureContent } from "../src/content-chunks.ts";
import { FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";
import * as evidence from "../src/filesystem-evidence.ts";
import { ResourceVersionManager, captureResourceVersion, createSerializablePreparationBinding, validateResourceVersion, type ResourceVersionToken } from "../src/resource-version.ts";
import { TimelineInterval } from "../src/task-timing.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { waitForCompletion } from "../src/runtime-lifecycle.ts";
import { gated } from "./async.ts";
import { storeMetadata, storePeer } from "./process-fixture.ts";

vi.mock("node:fs/promises", { spy: true });
afterEach(() => vi.restoreAllMocks());
const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
const memoryBudget = 256 * 1024;
const metadata = async (targets: readonly string[]) => Promise.all(targets.map(async target => {
	const stat = await lstat(target, { bigint: true }); return FILESYSTEM_METADATA_FIELDS.map(field => String(stat[field]));
}));
async function fixture() {
	const base = await mkdtemp(path.join(os.tmpdir(), "pi-persistent-inputs-")), root = path.join(base, "source"), cache = path.join(base, "cache");
	await mkdir(root); const store = new ProvenanceCertificateStore(cache, { orphanGraceMs: 0 });
	const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store } });
	const tokens: ResourceVersionToken[] = [];
	return { root, cache, store, manager, tokens, close: async () => {
		for (const token of tokens) await token.release(); manager.close(); await store.dispose(); await rm(base, { recursive: true, force: true });
	} };
}
const searchFixture = async (f: Awaited<ReturnType<typeof fixture>>, mode: string, id = "") => JSON.parse((await promisify(execFile)(process.execPath,
	["--import", "tsx", fileURLToPath(new URL("./persistent-preparation-fixture.ts", import.meta.url)), mode, f.root, f.cache, id])).stdout);

linux("recovers a pure search computation under a fresh native binding", { timeout: 120_000 }, async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	const run = (mode: string, id = "") => searchFixture(f, mode, id);
	try {
		await writeFile(file, "one\ntwo\nthree\ntwo again\n");
		for (const target of [f.root, file]) await utimes(target, 915148800, 946684800);
		const before = await metadata([f.root, file]), captured = await run("produce");
		expect(captured).toMatchObject({ matches: 1, nativeMatches: 1, expired: false });
		const restored = await run("restore", captured.id);
		expect(restored.pid).not.toBe(captured.pid); expect(restored.output).toEqual(captured.output);
		expect(restored, JSON.stringify({ captured, restored })).toMatchObject({ restored: true, prepared: 1, matches: 0, nativeMatches: 0, materializations: 1, expired: false, watching: false });
		const discovered = await run("discover");
		expect(discovered.pid).not.toBe(captured.pid); expect(discovered.output).toEqual(captured.output);
		expect(discovered).toMatchObject({ id: captured.id, restored: true, prepared: 1, matches: 0, nativeMatches: 0, materializations: 1, expired: false, watching: false });
		const variants = await run("variants", captured.id);
		expect(variants.variants.map((item: { matches: number; nativeMatches: number }) => [item.matches, item.nativeMatches])).toEqual([[0, 0], [1, 1], [1, 1], [1, 1]]);
		expect(variants.variants[0].output).not.toEqual(captured.output); expect(variants.variants[1].output).not.toEqual(captured.output);
		for (const mode of ["environment", "cancel", "clear", "release"]) {
			const next = await run(mode, captured.id);
			expect(next, mode).toMatchObject({ restored: true, matches: mode === "environment" ? 1 : 0, nativeMatches: mode === "environment" ? 1 : 0, released: mode === "release" });
			if (mode !== "release") expect(next.output).toEqual(captured.output);
			if (mode === "clear") { expect(next.prepared).toBe(1); captured.id = next.id; }
		}
		await f.store.clear();
		const composed = await run("composed-produce"), recovered = await run("composed-discover");
		expect(composed).toMatchObject({ prepared: 1, matches: 1, nativeMatches: 1 });
		expect(recovered).toMatchObject({ id: composed.id, prepared: 1, matches: 0, nativeMatches: 0, materializations: 1, expired: false });
		expect(recovered.pid).not.toBe(composed.pid); expect(recovered.output).toEqual(composed.output); expect(composed.output).toEqual(captured.output);
		expect(await metadata([f.root, file])).toEqual(before);
	} finally { await f.close(); }
});

linux("closes composed preparations over the original and destination versions after the original view retires", async () => {
	const f = await fixture(), first = path.join(f.root, "first"), second = path.join(f.root, "second");
	const binding = createSerializablePreparationBinding("composed-input-version-contract", value => typeof value === "string");
	const proof = (token: ResourceVersionToken, keys?: ReadonlySet<string>) =>
		[{ ...token, view: undefined, observations: new Map([...token.observations].filter(([key]) => !keys || keys.has(key))) }];
	try {
		await writeFile(second, ":tail");
		for (const state of ["complete", "missing", "changed"] as const) {
			await writeFile(first, "before"); let before = await metadata([f.root, first, second]);
			const origin = await f.manager.capture([{ path: first, scope: "content" }], memoryBudget);
			const destination = await f.manager.capture(undefined, memoryBudget); f.tokens.push(origin, destination);
			const result = await origin.view!.evaluate(view => view.prepare(binding, state, async inputs => ({
				value: (await inputs.readFile(first)).toString() + (await inputs.readFile(second)).toString(), bytes: 32, dispose() {} }),
				async value => value, first), keys => proof(origin, keys), f.root, undefined,
				async () => ({ view: destination.view!, observed: keys => proof(destination, keys) }), proofs => proofs,
				async proofs => { expect((await validateResourceVersion(proofs)).expired).toBe(false); });
			expect(result).toBe("before:tail"); await origin.release();
			if (state === "changed") { await writeFile(first, "after"); before = await metadata([f.root, first, second]); }
			if (state !== "missing") await destination.view!.readFile(first);
			destination.view!.seal(); const id = await f.manager.persist(destination), record = (await f.store.getInputVersion(id))!;
			const restored = (await f.manager.restore(id, memoryBudget))!; f.tokens.push(restored);
			expect(restored).toBeDefined();
			if (state !== "missing") {
				let builds = 0;
				const value = await restored.view!.prepare(binding, state, async inputs => { builds++; return {
					value: (await inputs.readFile(first)).toString() + (await inputs.readFile(second)).toString(), bytes: 32, dispose() {} }; }, async value => value, first);
				expect(value).toBe(state === "changed" ? "after:tail" : "before:tail"); expect(builds).toBe(Number(state === "changed"));
			}
			expect((record.payload as { preparations: unknown[] }).preparations).toHaveLength(Number(state === "complete"));
			expect(await metadata([f.root, first, second])).toEqual(before);
		}
	} finally { await f.close(); }
});

linux("discovers a finite set of current input closures and releases undispatched leases on cancellation or close", async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	try {
		await writeFile(file, "discoverable input");
		const initial = await metadata([path.dirname(f.root), f.root, file]);
		await expect(f.manager.recover(memoryBudget).next()).resolves.toMatchObject({ done: true });
		await expect(f.manager.restore(`sha256:${"0".repeat(64)}`, memoryBudget)).resolves.toBeUndefined();
		await expect(lstat(f.cache)).rejects.toMatchObject({ code: "ENOENT" });
		expect(await metadata([path.dirname(f.root), f.root, file])).toEqual(initial);
		await f.store.withPublication(async () => {}); await f.store.drain();
		storeMetadata(f.cache, database => database.exec("DROP TABLE input_versions; PRAGMA user_version=2"));
		await expect(f.manager.recover(memoryBudget).next()).resolves.toMatchObject({ done: true });
		expect(storeMetadata(f.cache, database => database.prepare("PRAGMA user_version").get()!.user_version)).toBe(2);
		storeMetadata(f.cache, database => database.exec("PRAGMA user_version=4"));
		await expect(f.manager.recover(memoryBudget).next()).rejects.toThrow(/unsupported provenance store version 4/);
		storeMetadata(f.cache, database => database.exec("PRAGMA user_version=2"));
		const original = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(original);
		let id = await f.manager.persist(original); const record = (await f.store.getInputVersion(id))!;
		// One store can be both a publisher and a reader. Its SQLite SHM must not
		// be borrowed by internal reads, even when all new descriptors are read-only.
		const state = async () => {
			const paths = [f.cache, ...["", "-wal", "-shm", "-journal"].map(suffix => path.join(f.cache, "metadata.sqlite" + suffix))];
			const fields = await Promise.all(paths.map(async target => {
				try { return (await metadata([target]))[0]; } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; return null; }
			}));
			// Closing an ordinary source FD in this process would release SQLite's POSIX locks.
			const { stdout } = await promisify(execFile)(process.execPath, ["--input-type=module", "-e", `
				import { openSync, readFileSync, closeSync, constants } from 'node:fs'; import { createHash } from 'node:crypto';
				console.log(JSON.stringify(process.argv.slice(1).map(target => { let fd;
					try { fd = openSync(target, constants.O_RDONLY | constants.O_NOATIME); return createHash('sha256').update(readFileSync(fd)).digest('hex'); }
					catch (error) { if (error.code !== 'ENOENT') throw error; return null; } finally { if (fd !== undefined) closeSync(fd); }
				})));`, ...paths.slice(1)]);
			return { fields, bytes: JSON.parse(stdout) as (string | null)[] };
		};
		for (const phase of ["cold", "warm", "transaction", "stale-reader"] as const) {
			const pause = gated(); let held: Promise<void> | undefined, old: DatabaseSync | undefined;
			try {
				if (phase === "cold") await f.store.drain();
				else if (phase === "warm") await f.store.withPublication(async () => {});
				else if (phase === "transaction") {
					held = f.store.withPublication(async () => {
						await f.store.putInputVersion({ uncommitted: "x".repeat(180000) }, []); await pause.wait(); throw new Error("read fixture rollback");
					}); void held.catch(() => {}); await pause.entered;
				} else {
					old = new DatabaseSync(path.join(f.cache, "metadata.sqlite")); old.exec("BEGIN");
					old.prepare("SELECT revision FROM changes WHERE path = ''").get();
					await f.store.putInputVersion({ isolationProbe: true }, []);
				}
				for (const target of [f.cache, ...["", "-wal", "-shm"].map(suffix => path.join(f.cache, "metadata.sqlite" + suffix))])
					await utimes(target, 915148800, 946684800).catch(error => { if (error.code !== "ENOENT") throw error; });
				const before = await state(), restored = await f.manager.restore(id, memoryBudget); f.tokens.push(restored!);
				expect((await restored!.view!.readFile(file)).toString()).toBe("discoverable input");
				for await (const value of f.store.candidatesInputVersions(8)) expect(value.payload).not.toHaveProperty("uncommitted");
				expect(await state(), phase).toEqual(before);
			} finally { pause.release(); await held?.catch(error => { expect(error.message).toBe("read fixture rollback"); }); old?.close(); }
		}
		await f.store.clear(); id = await f.manager.persist(original);
		const revision = () => storeMetadata(f.cache, database => database.prepare("SELECT revision FROM changes WHERE path = ''").get()!.revision);
		const beforeRevision = revision(); await f.store.drain();
		const publisher = storePeer(f.cache);
		try {
			expect(await publisher.noticed).toBe("held");
			const restored = await f.manager.restore(id, memoryBudget); f.tokens.push(restored!);
			expect((await restored!.view!.readFile(file)).toString()).toBe("discoverable input");
			expect(revision()).toBe(beforeRevision);
		} finally { publisher.child.kill("SIGKILL"); await publisher.completed.catch(() => undefined); }
		const openSource = evidence.openFilesystemFile;
		for (const mode of ["missing", "acquired", "cancel", "close"] as const) {
			const current = await f.manager.persist(original), pause = gated(), controller = new AbortController();
			const store = mode === "close" ? new ProvenanceCertificateStore(f.cache) : f.store;
			const manager = mode === "close" ? new ResourceVersionManager(f.root, { watch: false, persistentInputs: { store } }) : f.manager;
			let waiting = true, handle: filesystem.FileHandle | undefined, closing: Promise<void> | undefined;
			const opening = vi.spyOn(evidence, "openFilesystemFile").mockImplementation(async (...args) => {
				if (!waiting || !args[0].startsWith(f.store.artifacts.root)) return openSource(...args);
				waiting = false; const opened = mode === "missing" ? undefined : await openSource(...args); handle = opened?.handle;
				await pause.wait(); return opened ?? openSource(...args);
			});
			const restoring = manager.restore(current, memoryBudget, controller.signal);
			const outcome = restoring.then(token => ({ token, error: undefined }), error => ({ token: undefined, error }));
			try {
				await pause.entered;
				if (mode === "cancel") controller.abort();
				else if (mode === "close") { closing = store.dispose(); expect((await waitForCompletion(closing, undefined, 30)).status).toBe("deadline"); }
				else await f.store.clear();
				pause.release();
				const result = await outcome;
				if (mode === "acquired") {
					expect(result.error).toBeUndefined(); f.tokens.push(result.token!);
					expect((await result.token!.view!.readFile(file)).toString()).toBe("discoverable input");
				} else { expect(result.token).toBeUndefined(); expect(String(result.error)).toMatch(mode === "missing" ? /unavailable/ : mode === "cancel" ? /abort/i : /closed/); }
				await closing; if (handle) expect(handle.fd).toBe(-1);
			} finally { pause.release(); await outcome; opening.mockRestore(); if (store !== f.store) { manager.close(); await store.dispose(); } }
		}
		await f.store.clear(); id = await f.manager.persist(original);
		const payload = record.payload as Record<string, unknown>;
		await f.store.putInputVersion({ ...payload, root: path.dirname(f.root) }, record.artifacts);
		await f.store.putInputVersion({ ...payload, observations: [] }, record.artifacts);
		const corrupt = await f.store.putInputVersion({ ...payload, corrupt: true }, record.artifacts);
		storeMetadata(f.cache, database => database.prepare("UPDATE input_versions SET value = '{' WHERE id = ?").run(corrupt.id));
		await expect(f.store.getInputVersion(corrupt.id)).rejects.toThrow(/integrity/);
		await expect(f.manager.recover(memoryBudget, { maxCandidates: 1 }).next()).resolves.toMatchObject({ done: true });
		for (const maxCandidates of [0, -1, 1.5, 65537]) await expect(f.manager.recover(memoryBudget, { maxCandidates }).next()).rejects.toThrow(/limit/);
		const iterator = f.manager.recover(memoryBudget), first = await iterator.next();
		expect(first.done).toBe(false); expect(first.value!.id).toBe(id);
		const recovered = first.value!.token; f.tokens.push(recovered);
		expect((await recovered.view!.readFile(file)).toString()).toBe("discoverable input");
		await f.store.putInputVersion({ ...payload, later: true }, record.artifacts);
		await expect(iterator.next()).resolves.toMatchObject({ done: true });
		const recovery = f.manager.restore.bind(f.manager);
		for (const mode of ["cancel", "close"] as const) {
			const entered = Promise.withResolvers<void>(), resume = Promise.withResolvers<void>(), controller = new AbortController();
			let pendingToken: ResourceVersionToken | undefined;
			const spy = vi.spyOn(f.manager, "restore").mockImplementationOnce(async (...args) => {
				pendingToken = await recovery(...args); entered.resolve(); await resume.promise; return pendingToken;
			});
			const pending = f.manager.recover(memoryBudget, { signal: controller.signal }).next();
			try {
				await entered.promise;
				if (mode === "cancel") controller.abort(); else f.manager.close();
				resume.resolve();
				await expect(pending).rejects.toThrow(mode === "cancel" ? /abort/i : /owner changed/);
				expect(pendingToken).toBeDefined(); await expect(pendingToken!.view!.readFile(file)).rejects.toThrow(/disposed/);
			} finally { resume.resolve(); await pending.catch(() => undefined); spy.mockRestore(); }
		}
		await f.store.clear();
		expect((await recovered.view!.readFile(file)).toString()).toBe("discoverable input");
		expect(await metadata([f.root, file])).toEqual(initial.slice(1));
	} finally { await f.close(); }
});

linux("rejects incomplete, corrupt, over-budget and stale persistent preparation closures", async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	try {
		await writeFile(file, "one\ntwo\nthree\n");
		const captured = await searchFixture(f, "produce"), record = (await f.store.getInputVersion(captured.id))!;
		type Payload = { preparations: { binding: Sha256Digest; key: string; resource?: string; dependencies: string[]; value: ArtifactReference }[] };
		const preparation = (record.payload as Payload).preparations[0]!;
		expect(preparation).toBeDefined(); expect(record.artifacts).toContainEqual(preparation.value);
		await storePeer(f.cache, undefined, [], "collect").completed;
		expect(await f.store.artifacts.get(preparation.value)).toBeDefined();
		for (const field of ["dependency", "duplicate", "artifact", "resource", "codec"] as const) {
			const payload = structuredClone(record.payload) as Payload, value = payload.preparations[0]!;
			let artifacts = [...record.artifacts];
			if (field === "dependency") value.dependencies.push("content:unknown");
			else if (field === "duplicate") payload.preparations.push(value);
			else if (field === "resource") value.resource = path.dirname(f.root);
			else {
				artifacts = artifacts.filter(reference => reference.digest !== value.value.digest);
				if (field === "codec") { value.value = await f.store.artifacts.put('{"x":1,"x":2}'); artifacts.push(value.value); }
			}
			const proposed = await f.store.putInputVersion(payload, artifacts);
			await expect(f.manager.restore(proposed.id, 8 * 1024 * 1024), field).rejects.toThrow(/closure|data_invalid/);
		}
		await expect(f.manager.restore(captured.id, 1024)).rejects.toThrow(/budget/);
		const restored = (await f.manager.restore(captured.id, 8 * 1024 * 1024))!; f.tokens.push(restored);
		expect(restored.view!.hasPreparedInputs).toBe(true);
		await expect(restored.view!.restorePreparations((record.payload as Payload).preparations, f.store)).rejects.toThrow(/not_capturing/);
		const artifact = f.store.artifacts.artifactPath(preparation.value.digest), original = (await f.store.artifacts.get(preparation.value))!;
		await writeFile(artifact, Buffer.alloc(original.length, 32));
		await expect(f.manager.restore(captured.id, 8 * 1024 * 1024)).rejects.toThrow(/integrity/);
		const oversized = await open(artifact, "r+"); try { await oversized.truncate(64 * CONTENT_CHUNK_BYTES); } finally { await oversized.close(); }
		const starting = process.memoryUsage().rss; let peak = starting;
		const sample = setInterval(() => { peak = Math.max(peak, process.memoryUsage().rss); }, 1);
		try { await expect(f.manager.restore(captured.id, 8 * 1024 * 1024)).rejects.toThrow(/integrity/); }
		finally { clearInterval(sample); }
		const growth = Math.max(peak, process.memoryUsage().rss) - starting;
		console.info(JSON.stringify({ oversizedPreparationBytes: 64 * CONTENT_CHUNK_BYTES, expectedBytes: original.length, peakRSSGrowth: growth }));
		expect(growth).toBeLessThan(32 * CONTENT_CHUNK_BYTES);
		await rm(artifact); await expect(f.manager.restore(captured.id, 8 * 1024 * 1024)).rejects.toThrow(/missing/);
		await f.store.artifacts.put(original); await writeFile(file, "changed source\n");
		expect(await f.manager.restore(captured.id, 8 * 1024 * 1024)).toBeUndefined();
		expect((await f.manager.validate(restored)).expired).toBe(true);
		await restored.release(); expect(restored.view!.hasPreparedInputs).toBe(false);
	} finally { await f.close(); }
});

linux("recovers actual input bytes, names, aliases and current proofs in a cold process", async () => {
	const f = await fixture(), file = path.join(f.root, "input"), alias = path.join(f.root, "alias");
	try {
		await writeFile(file, "immutable input\n"); await symlink("input", alias);
		for (const target of [f.root, file]) await utimes(target, 915148800, 946684800);
		const before = await metadata([f.root, file, alias]);
		const token = await f.manager.capture([{ path: f.root, scope: "tree_content" }], memoryBudget); f.tokens.push(token);
		expect((await token.view!.readFile(alias)).toString()).toBe("immutable input\n");
		const id = await f.manager.persist(token); await token.release(); f.manager.close(); await f.store.dispose();
		const source = `
			import { ResourceVersionManager } from './src/resource-version.ts';
			import { ProvenanceCertificateStore } from './src/reuse-store.ts';
			const [root, cache, id] = process.argv.slice(2), store = new ProvenanceCertificateStore(cache);
			const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store } });
			const token = await manager.restore(id, 262144);
			if (!token) throw new Error('cold input restore missed');
			try { console.log(JSON.stringify({ value: (await token.view.readFile(root + '/alias')).toString(),
				names: await token.view.readdir(root), expired: (await manager.validate(token)).expired,
				window: (await manager.seal(token)).expired, watching: token.watching })); }
			finally { await token.release(); manager.close(); await store.dispose(); }
		`;
		const { stdout } = await promisify(execFile)(process.execPath, ["--import", "tsx", "--input-type=module", "-e", source, "input-version-worker", f.root, f.cache, id]);
		expect(JSON.parse(stdout)).toEqual({ value: "immutable input\n", names: ["alias", "input"], expired: false, window: true, watching: false });
		expect(await metadata([f.root, file, alias])).toEqual(before);
	} finally { await f.close(); }
});

linux("keeps active content across clear, republishes its roots, and rejects stale or corrupt recovery", async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	try {
		await writeFile(file, "original content");
		const token = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(token);
		const first = await f.manager.persist(token);
		expect((await f.store.stats()).inputVersions).toBe(1);
		await storePeer(f.cache, undefined, [], "collect").completed; expect(await f.store.getInputVersion(first)).toBeDefined();
		await f.store.clear(); expect(await f.store.getInputVersion(first)).toBeUndefined();
		expect((await token.view!.readFile(file)).toString()).toBe("original content");
		const second = await f.manager.persist(token), restored = await f.manager.restore(second, memoryBudget); f.tokens.push(restored!);
		expect((await restored!.view!.readFile(file)).toString()).toBe("original content");
		await writeFile(file, "changed contents");
		expect((await f.manager.validate(restored!)).expired).toBe(true); expect(await f.manager.restore(second, memoryBudget)).toBeUndefined();
		await expect(f.manager.recover(memoryBudget).next()).resolves.toMatchObject({ done: true });
		expect((await restored!.view!.readFile(file)).toString()).toBe("original content");
		await writeFile(file, "original content");
		const record = (await f.store.getInputVersion(second))!, target = f.store.artifacts.artifactPath(record.artifacts[0]!.digest);
		await writeFile(target, "corrupted bytes!");
		await expect(f.manager.restore(second, memoryBudget)).rejects.toThrow(/integrity/);
		expect((await restored!.view!.readFile(file)).toString()).toBe("original content");
		await restored!.release(); await expect(restored!.view!.readFile(file)).rejects.toThrow(/disposed/);
	} finally { await f.close(); }
});

linux("drains admitted chunk reads before releasing pins and immediately revokes further reads", async () => {
	const f = await fixture(), file = path.join(f.root, "input"), pause = gated(), opened: filesystem.FileHandle[] = [];
	const native = await vi.importActual<typeof filesystem>("node:fs/promises");
	vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
		const handle = await native.open(...args);
		if (args[1] === "r" && (String(args[0]).startsWith(f.store.artifacts.root) || String(args[0]).startsWith("/proc/self/fd/"))) opened.push(handle);
		return handle;
	});
	try {
		await writeFile(file, "admitted input");
		const token = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(token);
		const active = opened.filter(handle => handle.fd !== -1); expect(active).toHaveLength(1);
		for (const handle of active) {
			const read = handle.read;
			vi.spyOn(handle, "read").mockImplementation(async (...args) => { const result = await Reflect.apply(read, handle, args); await pause.wait(); return result; });
		}
		const reading = token.view!.readFile(file); await pause.entered;
		const closing = Promise.resolve(token.release());
		expect((await waitForCompletion(closing, undefined, 30)).status).toBe("deadline");
		await expect(token.view!.readFile(file)).rejects.toThrow(/disposed/); await f.store.clear();
		pause.release(); expect((await reading).toString()).toBe("admitted input"); await closing;
		expect(opened.every(handle => handle.fd === -1)).toBe(true);
		const pool = new ContentChunkPool(f.store), staging = gated(), block = Buffer.alloc(CONTENT_CHUNK_BYTES, 7);
		const capturing = pool.capture(async sink => { await sink(block); await staging.wait();
			return { bytesRead: block.length, hash: createHash("sha256").update(block).digest("hex") }; });
		try {
			await staging.entered; expect(pool.openChunks).toBe(1);
			const stopping = f.store.dispose();
			expect((await waitForCompletion(stopping, undefined, 30)).status).toBe("deadline");
			await expect(captureContent(pool, "late")).rejects.toThrow(/closed/);
			const cancelled = expect(capturing).rejects.toThrow(/closed/); staging.release(); await cancelled; await stopping;
		} finally { staging.release(); await capturing.catch(() => undefined); await pool.dispose(); }
		expect(pool.openChunks).toBe(0); expect(opened.every(handle => handle.fd === -1)).toBe(true);
		const publisher = new ProvenanceCertificateStore(f.cache), publishedPool = new ContentChunkPool(publisher), admitted = gated();
		const publication = publisher.withPublication(async () => { await admitted.wait();
			const content = await captureContent(publishedPool, "admitted publication"); await content.publish(); return content; });
		try {
			await admitted.entered; const stopping = publisher.dispose();
			expect((await waitForCompletion(stopping, undefined, 30)).status).toBe("deadline");
			admitted.release(); const content = await publication; await stopping;
			expect((await content.read()).toString()).toBe("admitted publication");
			expect(await filesystem.readFile(publisher.artifacts.artifactPath(content.manifest.chunks[0]!.digest), "utf8")).toBe("admitted publication");
		} finally { admitted.release(); await publication.catch(() => undefined); await publishedPool.dispose(); await publisher.dispose(); }
		expect(opened.every(handle => handle.fd === -1)).toBe(true);
	} finally { pause.release(); await f.close(); }
});

linux("shares immutable chunk versions across publication, borrowing, damage, collection and cold loading", async () => {
	const f = await fixture(), pool = new ContentChunkPool(f.store), receiver = new ContentChunkPool(f.store), release = pool.retain();
	const opened: filesystem.FileHandle[] = [], native = await vi.importActual<typeof filesystem>("node:fs/promises"), pause = gated();
	vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
		const handle = await native.open(...args); opened.push(handle); return handle;
	});
	try {
		const text = "x".repeat(CONTENT_CHUNK_BYTES - 1) + "😀end";
		const file = path.join(f.root, "input"), bytes = Buffer.from(text), computation = new TimelineInterval(0, 1);
		await writeFile(file, bytes);
		for (const target of [f.root, file]) await utimes(target, 915148800, 946684800);
		const before = await metadata([f.root, file]), beforeCapture = await metadata([path.dirname(f.root), f.root, file]);
		const captured = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(captured);
		expect((await captured.view!.readFile(file)).toString()).toBe(text);
		const content = await captureContent(pool, text);
		expect((await content.read()).toString()).toBe(text);
		const loan = pool.lend(); let borrowed;
		try { borrowed = await receiver.restore(content.manifest, loan.transfer); } finally { loan.release(); }
		expect((await borrowed.read()).toString()).toBe(text);
		await expect(lstat(f.cache)).rejects.toMatchObject({ code: "ENOENT" });
		expect(await metadata([path.dirname(f.root), f.root, file])).toEqual(beforeCapture);
		for (const input of [bytes, content, { content: bytes, computation }, { content, computation }]) {
			const token = await captureResourceVersion(undefined, f.root, undefined, 2 * CONTENT_CHUNK_BYTES, new Map([[file, input]]));
			f.tokens.push(token); const view = token.view!; view.seal();
			expect(token.watching).toBe(false); expect((await token.manager.validate(token)).expired).toBe(false);
			expect((await view.readFile(file)).toString()).toBe(text); expect(await view.readFile(file, 0)).toEqual(Buffer.alloc(0));
			expect((await view.readFileRange(file, CONTENT_CHUNK_BYTES - 1, 4)).toString()).toBe("😀");
			expect((await view.readFileRange(file, bytes.length - 3, 20)).toString()).toBe("end");
			expect(await view.readFileRange(file, bytes.length + 1, 20)).toEqual(Buffer.alloc(0));
			for (const invalid of [-1, 0.5, NaN, Infinity]) {
				await expect(view.readFile(file, invalid)).rejects.toThrow(/read budget/);
				await expect(view.readFileRange(file, invalid, 1)).rejects.toThrow(/read budget/);
				await expect(view.readFileRange(file, 0, invalid)).rejects.toThrow(/read budget/);
			}
			const borrow = vi.fn(async () => 1); expect(await view.borrowObject(file, borrow)).toBeUndefined(); expect(borrow).not.toHaveBeenCalled();
		}
		expect(await metadata([f.root, file])).toEqual(before);
		const capturing = pool.capture(async sink => { await pause.wait(); await sink(Buffer.from("overlap")); return { bytesRead: 7, hash: createHash("sha256").update("overlap").digest("hex") }; });
		const publishing = f.store.withPublication(async () => { pause.release(); await content.publish(); });
		await Promise.all([capturing, publishing]);
		const references = content.manifest.chunks.filter((value): value is ArtifactReference => value !== null);
		await writeFile(f.store.artifacts.artifactPath(references[0]!.digest), "damaged");
		expect((await content.read()).toString()).toBe(text); expect((await borrowed.read()).toString()).toBe(text);
		const rejected = new ContentChunkPool(f.store);
		try { await expect(rejected.restore(content.manifest)).rejects.toThrow(/integrity/); } finally { await rejected.dispose(); }
		await f.store.clear(); await release(); expect(pool.openChunks).toBeGreaterThan(0);
		for (const token of f.tokens) expect((await token.view!.readFileRange(file, CONTENT_CHUNK_BYTES - 1, 7)).toString()).toBe("😀end");
		expect(await metadata([f.root, file])).toEqual(before);
		await writeFile(file, "changed"); const changed = await metadata([f.root, file]);
		for (const token of f.tokens) expect((await token.manager.validate(token)).expired).toBe(true);
		expect(await metadata([f.root, file])).toEqual(changed);
		for (const token of f.tokens) { await token.release(); await expect(token.view!.readFile(file)).rejects.toThrow(/disposed/); }
		expect(pool.openChunks).toBe(0);
		expect((await borrowed.read()).toString()).toBe(text);
		const saved = await f.store.withPublication(async () => { await borrowed.publish(); return f.store.putInputVersion(borrowed.manifest, references); });
		for (const route of ["file", "hardlink", "symlink"] as const) {
			const targets = [path.dirname(f.root), f.root, file];
			for (const [index, reference] of references.entries()) {
				const target = f.store.artifacts.artifactPath(reference.digest);
				const source = route === "file" ? target : path.join(path.dirname(f.root), `cas-${route}-${index}`);
				if (route !== "file") {
					await writeFile(source, bytes.subarray(index * CONTENT_CHUNK_BYTES, (index + 1) * CONTENT_CHUNK_BYTES));
					await rm(target); await (route === "hardlink" ? link : symlink)(source, target);
				}
				await utimes(source, 915148800, 946684800);
				if (route === "symlink") await lutimes(target, 915148800, 946684800);
				targets.push(source, target, path.dirname(target));
			}
			const beforeReads = await metadata(targets);
			for (const [index, reference] of references.entries()) {
				expect(await f.store.artifacts.has(reference)).toBe(true);
				expect(await metadata(targets), `${route}: availability`).toEqual(beforeReads);
				expect(await f.store.artifacts.get(reference)).toEqual(bytes.subarray(index * CONTENT_CHUNK_BYTES, (index + 1) * CONTENT_CHUNK_BYTES));
				expect(await metadata(targets), `${route}: direct read`).toEqual(beforeReads);
			}
			const closure = await f.store.artifacts.load(references);
			try { expect(Buffer.concat(references.map(reference => closure!.read(reference)))).toEqual(bytes); }
			finally { await closure?.dispose(); }
			const cold = JSON.parse((await promisify(execFile)(process.execPath, ["--input-type=module", "-e", `
				import { ContentChunkPool } from ${JSON.stringify(new URL("../src/content-chunks.ts", import.meta.url).href)};
				import { ProvenanceCertificateStore } from ${JSON.stringify(new URL("../src/reuse-store.ts", import.meta.url).href)};
				const store = new ProvenanceCertificateStore(${JSON.stringify(f.cache)}), pool = new ContentChunkPool(store);
				try { const record = await store.getInputVersion(${JSON.stringify(saved.id)}), content = await pool.restore(record.payload);
					console.log(JSON.stringify({ pid: process.pid, text: (await content.read()).toString(), bytes: pool.storedBytes }));
				} finally { await pool.dispose(); await store.dispose(); }
			`], { maxBuffer: 4 * CONTENT_CHUNK_BYTES })).stdout);
			expect(cold.pid).not.toBe(process.pid); expect(cold.text).toBe(text); expect(cold.bytes).toBe(Buffer.byteLength(text));
			expect(await metadata(targets), `${route}: direct, closure and cold reads`).toEqual(beforeReads);
		}
		await receiver.dispose(); expect(receiver.openChunks).toBe(0);
		expect(opened.length).toBeGreaterThan(0); expect(opened.every(handle => handle.fd === -1)).toBe(true);
	} finally { pause.release(); await pool.dispose(); await receiver.dispose(); await f.close(); }
});

linux("binds restored input values to their observations even when a publisher seals inconsistent data", async () => {
	const f = await fixture(), file = path.join(f.root, "input"), alias = path.join(f.root, "alias");
	try {
		await writeFile(file, "original content"); await symlink("input", alias);
		const token = await f.manager.capture([{ path: f.root, scope: "tree_content" }], memoryBudget); f.tokens.push(token);
		const record = (await f.store.getInputVersion(await f.manager.persist(token)))!;
		for (const field of ["content", "names", "alias", "size", "omitted", "extra"] as const) {
			const payload = structuredClone(record.payload) as { entries: [string, Record<string, any>][] };
			let artifacts = record.artifacts;
			const entry = (target: string) => payload.entries.find(([name]) => name === target)![1];
			if (field === "content") {
				const reference = await f.store.artifacts.put("tampered content");
				entry(file).content = { size: reference.size, digest: reference.digest, chunks: [reference] }; artifacts = [reference];
			} else if (field === "names") entry(f.root).entries = ["invented"];
			else if (field === "alias") entry(alias).target = path.join(f.root, "invented");
			else if (field === "size") entry(f.root).metadata = { ...entry(f.root).metadata, size: 1 };
			else if (field === "omitted") payload.entries = payload.entries.filter(([name]) => name !== alias);
			else payload.entries.push([path.join(f.root, "invented"), { ...entry(file) }]);
			const proposed = await f.store.putInputVersion(payload, artifacts);
			expect(await f.manager.restore(proposed.id, memoryBudget), field).toBeUndefined();
		}
		const restored = await f.manager.restore(record.id, memoryBudget); f.tokens.push(restored!);
		expect((await restored!.view!.readFile(alias)).toString()).toBe("original content");
	} finally { await f.close(); }
});

linux("streams a 600 MiB logical input with independent storage, read and metadata budgets", { timeout: 60_000 }, async () => {
	const f = await fixture(), file = path.join(f.root, "large"), size = 600 * CONTENT_CHUNK_BYTES + 7;
	const manager = new ResourceVersionManager(f.root, { watch: false, persistentInputs: { store: f.store,
		limits: { maxLogicalBytes: size, maxStoredBytes: 2 * CONTENT_CHUNK_BYTES, maxOpenChunks: 2, maxReadBytes: 2 * CONTENT_CHUNK_BYTES } } });
	try {
		const handle = await open(file, "wx");
		try { await handle.truncate(size); await handle.write(Buffer.from("header!"), 0, 7, 0); await handle.write(Buffer.from("footer!"), 0, 7, size - 7); } finally { await handle.close(); }
		await utimes(file, 915148800, 946684800); const before = await metadata([f.root, file]), starting = process.memoryUsage().rss;
		let peak = starting; const sample = setInterval(() => { peak = Math.max(peak, process.memoryUsage().rss); }, 10);
		try {
			const token = await manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(token);
			expect(token.view!.bytes).toBeLessThan(memoryBudget);
			expect((await token.view!.readFile(file, 7)).toString()).toBe("header!");
			expect((await token.view!.readFileRange(file, size - 7, 20)).toString()).toBe("footer!");
			expect(await token.view!.readFileRange(file, CONTENT_CHUNK_BYTES - 3, 9)).toEqual(Buffer.alloc(9));
			await expect(token.view!.readFile(file)).rejects.toThrow(/read budget/);
			const id = await manager.persist(token), restored = await manager.restore(id, memoryBudget); f.tokens.push(restored!);
			expect((await restored!.view!.readFileRange(file, size - 7, 7)).toString()).toBe("footer!");
			const record = (await f.store.getInputVersion(id))!;
			expect(record.artifacts.reduce((sum, reference) => sum + reference.size, 0)).toBe(CONTENT_CHUNK_BYTES + 7);
			expect(await metadata([f.root, file])).toEqual(before);
			expect(peak - starting).toBeLessThan(160 * CONTENT_CHUNK_BYTES);
			console.info(JSON.stringify({ logicalBytes: size, storedBytes: CONTENT_CHUNK_BYTES + 7, metadataBytes: token.view!.bytes, peakRSSGrowth: peak - starting }));
		} finally { clearInterval(sample); }
	} finally { manager.close(); await f.close(); }
});

linux("rejects source-overlapping caches and each exhausted input budget without retaining authority", async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	try {
		await writeFile(file, Buffer.alloc(CONTENT_CHUNK_BYTES + 1, 7));
		for (const limits of [{ maxLogicalBytes: CONTENT_CHUNK_BYTES }, { maxStoredBytes: 1 }, { maxOpenChunks: 0 }]) {
			const manager = new ResourceVersionManager(f.root, { watch: false, persistentInputs: { store: f.store, limits } });
			try { await expect(manager.capture([{ path: file, scope: "content" }], memoryBudget)).rejects.toThrow(/too_large|budget/); } finally { manager.close(); }
		}
		const cache = new ProvenanceCertificateStore(path.join(f.root, "cache")), manager = new ResourceVersionManager(f.root, { persistentInputs: { store: cache } });
		try {
			await expect(manager.capture([{ path: file, scope: "content" }], memoryBudget)).rejects.toThrow(/outside its source/);
			await expect(manager.recover(memoryBudget).next()).rejects.toThrow(/outside its source/);
		}
		finally { manager.close(); await cache.dispose(); }
		await expect(lstat(path.join(f.root, "cache"))).rejects.toMatchObject({ code: "ENOENT" });
		const pool = new ContentChunkPool(f.store); await pool.dispose();
		await expect(pool.restore({ size: 0, digest: "sha256:" + "0".repeat(64), chunks: [] })).rejects.toThrow(/closed/);
	} finally { await f.close(); }
});
