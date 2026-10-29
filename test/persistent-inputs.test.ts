import { afterEach, expect, test, vi } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, open, lstat, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ArtifactReference } from "../src/artifact-reference.ts";
import type { Sha256Digest } from "../src/stable-value-hash.ts";
import { CONTENT_CHUNK_BYTES } from "../src/content-chunks.ts";
import { FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";
import { ResourceVersionManager, type ResourceVersionToken } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
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
const searchFixture = async (f: Awaited<ReturnType<typeof fixture>>, mode: string, id = "") => JSON.parse((await promisify(execFile)(process.execPath,
	[fileURLToPath(new URL("./persistent-preparation-fixture.ts", import.meta.url)), mode, f.root, f.cache, id])).stdout);

linux("rejects incomplete, corrupt, over-budget and stale persistent preparation closures", async () => {
	const f = await fixture(), file = path.join(f.root, "input");
	try {
		await writeFile(file, "one\ntwo\nthree\n");
		const captured = await searchFixture(f, "produce"), record = (await f.store.getInputVersion(captured.id))!;
		type Payload = { preparations: { binding: Sha256Digest; key: string; resource?: string; dependencies: string[]; value: ArtifactReference }[] };
		const preparation = (record.payload as Payload).preparations[0]!;
		expect(preparation).toBeDefined(); expect(record.artifacts).toContainEqual(preparation.value);
		await storePeer(f.cache).completed;
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
