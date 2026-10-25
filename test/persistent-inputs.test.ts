import { afterEach, expect, test, vi } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, open, lstat, rm, symlink, utimes, writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { ContentChunkPool, CONTENT_CHUNK_BYTES } from "../src/content-chunks.ts";
import { FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";
import { ResourceVersionManager, type ResourceVersionToken } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { waitForCompletion } from "../src/runtime-lifecycle.ts";
import { gated } from "./async.ts";
import { storePeer } from "./process-fixture.ts";

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
		expect((await restored!.view!.readFile(file)).toString()).toBe("original content");
		await writeFile(file, "original content");
		const record = (await f.store.getInputVersion(second))!, target = f.store.artifacts.artifactPath(record.artifacts[0]!.digest);
		await writeFile(target, "corrupted bytes!");
		await expect(f.manager.restore(second, memoryBudget)).rejects.toThrow(/integrity/);
		await expect(restored!.view!.readFile(file)).rejects.toThrow(/integrity/);
		await restored!.release(); await expect(restored!.view!.readFile(file)).rejects.toThrow(/disposed/);
	} finally { await f.close(); }
});

linux("drains admitted chunk reads before releasing pins and immediately revokes further reads", async () => {
	const f = await fixture(), file = path.join(f.root, "input"), pause = gated(), opened: filesystem.FileHandle[] = [];
	const native = await vi.importActual<typeof filesystem>("node:fs/promises");
	vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
		const handle = await native.open(...args);
		if (String(args[0]).startsWith(f.store.artifacts.root) && args[1] === "r") opened.push(handle);
		return handle;
	});
	try {
		await writeFile(file, "admitted input");
		const token = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget); f.tokens.push(token);
		expect(opened.length).toBe(1);
		for (const handle of opened) {
			const read = handle.read;
			vi.spyOn(handle, "read").mockImplementation(async (...args) => { const result = await Reflect.apply(read, handle, args); await pause.wait(); return result; });
		}
		const reading = token.view!.readFile(file); await pause.entered;
		const closing = Promise.resolve(token.release());
		expect((await waitForCompletion(closing, undefined, 30)).status).toBe("deadline");
		await expect(token.view!.readFile(file)).rejects.toThrow(/disposed/); await f.store.clear();
		pause.release(); expect((await reading).toString()).toBe("admitted input"); await closing;
		expect(opened.every(handle => handle.fd === -1)).toBe(true);
	} finally { pause.release(); await f.close(); }
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
		try { await expect(manager.capture([{ path: file, scope: "content" }], memoryBudget)).rejects.toThrow(/outside its source/); }
		finally { manager.close(); await cache.dispose(); }
		await expect(lstat(path.join(f.root, "cache"))).rejects.toMatchObject({ code: "ENOENT" });
		const pool = new ContentChunkPool(f.store); await pool.dispose();
		await expect(pool.restore({ size: 0, digest: "sha256:" + "0".repeat(64), chunks: [] })).rejects.toThrow(/closed/);
	} finally { await f.close(); }
});
