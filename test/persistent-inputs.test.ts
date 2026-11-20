import { afterEach, expect, test, vi } from "vitest";
import { execFile } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { promisify } from "node:util";
import { mkdir, mkdtemp, open, lstat, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ArtifactReference } from "../src/content-chunks.ts";
import { digestObject, type Sha256Digest } from "../src/primitives.ts";
import { captureFileContent, CONTENT_CHUNK_BYTES, ContentChunkPool, readContent } from "../src/content-chunks.ts";
import { evaluateResourceQuery, ResourceObjectInputs } from "../src/agent-execution-world.ts";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { SpeculativeConfiguration } from "../src/config-schema.ts";
import { WorkspaceSandboxService, WorkspaceStructureCapture } from "../src/workspace-sandbox.ts";
import { FILESYSTEM_METADATA_FIELDS } from "../src/filesystem-evidence.ts";
import {
	ResourceInputSession,
	ResourceVersionManager,
	createSerializablePreparationBinding,
	type ResourceVersionToken,
} from "../src/resource-version.ts";
import { ArtifactReadView, ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { storeMetadata, storePeer } from "./process-fixture.ts";

vi.mock("node:fs/promises", { spy: true });
afterEach(() => vi.restoreAllMocks());
const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
const memoryBudget = 256 * 1024;
const metadata = async (targets: readonly string[]) =>
	Promise.all(
		targets.map(async (target) => {
			const stat = await lstat(target, { bigint: true });
			return FILESYSTEM_METADATA_FIELDS.map((field) => String(stat[field]));
		}),
	);
async function fixture() {
	const base = await mkdtemp(path.join(os.tmpdir(), "pi-persistent-inputs-"));
	const root = path.join(base, "source");
	const cache = path.join(base, "cache");
	await mkdir(root);
	const store = new ProvenanceCertificateStore(cache, { orphanGraceMs: 0 });
	const manager = new ResourceVersionManager(root, {
		watch: false,
		persistentInputs: { store },
	});
	const tokens: ResourceVersionToken[] = [];
	return {
		root,
		cache,
		store,
		manager,
		tokens,
		close: async () => {
			for (const token of tokens) await token.release();
			manager.close();
			await store.dispose();
			await rm(base, { recursive: true, force: true });
		},
	};
}
const searchFixture = async (f: Awaited<ReturnType<typeof fixture>>, mode: string, id = "") =>
	JSON.parse(
		(
			await promisify(execFile)(process.execPath, [
				fileURLToPath(new URL("./persistent-preparation-fixture.ts", import.meta.url)),
				mode,
				f.root,
				f.cache,
				id,
			])
		).stdout,
	);

linux("native workspace captures and filesystem tools exchange independent file versions", async () => {
	const f = await fixture();
	const root = path.parse(f.root).root;
	const tool = new ResourceVersionManager(root, { watch: false, persistentInputs: { store: f.store, internalCache: true } });
	const signal = new AbortController().signal;
	const target = path.join(f.root, "header");
	const other = path.join(f.root, "second");
	try {
		await writeFile(target, "format_as");
		await writeFile(other, "another input");
		const now = Date.now();
		vi.spyOn(Date, "now").mockReturnValue(now + 3000);
		const source = await tool.capture([{ path: target, scope: "content" }], memoryBudget);
		try {
			await tool.persist(source);
		} finally {
			await source.release();
		}
		await using native = new ResourceObjectInputs(f.root, f.store, signal);
		const borrowed = await native.borrow(target, await lstat(target, { bigint: true }), native.contents);
		expect(borrowed?.shared).toBe(true);
		expect((await readContent(borrowed!.content)).toString()).toBe("format_as");
		expect(borrowed!.computation).toBeDefined();
		for (const name of [target, other])
			native.remember(name, await captureFileContent(name, memoryBudget, native.contents));
		await writeFile(target, "format_is");
		await native.publish();
		expect(await native.borrow(target, await lstat(target, { bigint: true }), native.contents)).toBeUndefined();
		const reader = new ResourceInputSession({ store: f.store, internalCache: true }, memoryBudget);
		try {
			const values: string[] = [];
			for await (const token of reader.reader(root, signal)(other)) {
				const input = token.view?.snapshotInput(other);
				if (input && "content" in input) values.push((await readContent(input.content)).toString());
			}
			expect(values).toEqual(["another input"]);
		} finally {
			await reader.dispose();
		}
	} finally {
		tool.close();
		await f.close();
	}
});

test("a native file reader can reuse one verified file after a sibling in its persisted closure changes", async () => {
	const f = await fixture();
	const root = path.parse(f.root).root;
	const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store: f.store, internalCache: true } });
	const signal = new AbortController().signal;
	const target = path.join(f.root, "used");
	const sibling = path.join(f.root, "sibling");
	const second = path.join(f.root, "also-used");
	try {
		await writeFile(target, "first");
		await writeFile(sibling, "other");
		await writeFile(second, "second");
		vi.spyOn(Date, "now").mockReturnValue(Date.now() + 3000);
		const source = await manager.capture([target, sibling, second].map((path) => ({ path, scope: "content" })), memoryBudget);
		try {
			await manager.persist(source);
		} finally {
			await source.release();
		}
		await writeFile(sibling, "newer");
		await using native = new ResourceObjectInputs(f.root, f.store, signal);
		const fetch = vi.spyOn(f.store, "getInputVersion");
		const borrow = async (file = target) => native.borrow(file, await lstat(file, { bigint: true }), native.contents);
		const hit = await borrow();
		expect(hit?.shared).toBe(true);
		expect((await readContent(hit!.content)).toString()).toBe("first");
		expect(hit!.computation).toBeDefined();
		native.remember(target, hit!);
		expect(await readContent((await borrow(second))!.content)).toEqual(Buffer.from("second"));
		expect(fetch).toHaveBeenCalledOnce();
		expect(await native.publish()).toBe(0);
		await writeFile(target, "later");
		expect(await borrow()).toBeUndefined();
	} finally {
		manager.close();
		await f.close();
	}
});

test("a single verified CAS block supplies its file digest without a second read", async () => {
	const f = await fixture();
	const pool = new ContentChunkPool(f.store);
	const release = pool.retain();
	try {
		const bytes = Buffer.from("one block");
		const reference = await f.store.artifacts.put(bytes);
		const manifest = { ...reference, chunks: [reference] };
		const reads = vi.spyOn(ArtifactReadView.prototype, "readRange");
		const file = await pool.restore(manifest);
		expect(reads).not.toHaveBeenCalled();
		expect(await readContent(file)).toEqual(bytes);
		await expect(pool.restore({ ...manifest, digest: digestObject("other") })).rejects.toThrow("integrity");
	} finally {
		await release();
		await f.close();
	}
});

test("required workspace snapshots reject excess names and bytes before reading file bodies", async () => {
	const f = await fixture();
	const inputs = new ResourceObjectInputs(f.root, f.store, new AbortController().signal);
	const borrow = vi.spyOn(inputs, "borrow");
	const content = { maxBytes: 8, maxFileBytes: 8, pool: inputs.contents, required: true as const };
	try {
		await writeFile(path.join(f.root, "a"), "1234");
		await writeFile(path.join(f.root, "b"), "56789");
		for (const maxFiles of [1, 2]) {
			const capture = new WorkspaceStructureCapture(f.root, { content, maxFiles });
			await expect(capture.captureVersions(undefined, inputs)).rejects.toThrow("workspace snapshot exceeds configured limits");
			expect(borrow).not.toHaveBeenCalled();
		}
		await writeFile(path.join(f.root, "b"), "5678");
		const captured = await new WorkspaceStructureCapture(f.root, { content }).captureVersions(undefined, inputs);
		expect((await readContent(captured.files.get("b")!.content)).toString()).toBe("5678");
		expect(borrow).toHaveBeenCalledTimes(2);
	} finally {
		await inputs[Symbol.asyncDispose]();
		await f.close();
	}
});

test("a private workspace materializes distinct blocks beyond its open descriptor budget", async () => {
	const f = await fixture();
	const configuration = new SpeculativeConfiguration({ filesystem: { maxOpenChunks: 2 } });
	const sandbox = configuration.run(() => new WorkspaceSandboxService(f.store));
	try {
		for (let i = 0; i < 8; i++) await writeFile(path.join(f.root, String(i)), String(i));
		await sandbox.withWorkspace(f.root, async (workspace) => {
			for (let i = 0; i < 8; i++) {
				const file = workspace.baselineFile!(String(i))!;
				expect((await readContent(file.content)).toString()).toBe(String(i));
			}
			await writeFile(path.join(workspace.sandboxRoot, "7"), "private");
			expect((await readContent(workspace.baselineFile!("7")!.content)).toString()).toBe("7");
		});
	} finally {
		await sandbox.dispose();
		await f.close();
	}
});

test("preparation lookup uses ordered SQLite keys, exact scopes, and cascading eviction", async () => {
	const f = await fixture();
	try {
		const put = (root: string, binding: string, key: string, generation: number) =>
			f.store.putInputVersion({ root, preparations: [{ binding, key }], generation }, []);
		const clock = vi.spyOn(Date, "now");
		const now = Date.now();
		clock.mockReturnValue(now);
		const earlier = await put(f.root, "transform", "same", 1);
		await put(f.root, "other", "same", 2);
		await put(f.root, "transform", "different", 3);
		await put(f.root + "-other", "transform", "same", 4);
		clock.mockReturnValue(now + 1);
		const latest = await put(f.root, "transform", "same", 5);
		clock.mockRestore();
		const query = { root: f.root, binding: "transform", prepKey: digestObject("same") };
		const queries = vi.spyOn(DatabaseSync.prototype, "prepare");
		expect(await f.store.findPreparedInputCandidates(query)).toEqual([latest, earlier]);
		expect(await f.store.findPreparedInputCandidates({ ...query, limit: 1 })).toEqual([latest]);
		for await (const candidate of f.store.candidatesInputVersions(f.root, 1)) expect(candidate).toEqual(latest);
		const statements = queries.mock.calls.map(([sql]) => sql);
		queries.mockRestore();
		const join = statements.find((sql) => sql.includes("JOIN input_versions"));
		const byRoot = statements.find((sql) => sql.includes("FROM input_versions WHERE root"));
		expect(join).toBeDefined();
		expect(byRoot).toBeDefined();
		storeMetadata(f.cache, (db) => {
			const plan = db.prepare("EXPLAIN QUERY PLAN " + join).all(f.root, "transform", query.prepKey, 1)
				.map((row) => row.detail).join("\n");
			expect(plan).toMatch(/SEARCH p USING COVERING INDEX input_preparations_recent/);
			expect(plan).toMatch(/SEARCH i USING INDEX/);
			expect(plan).not.toMatch(/SCAN|TEMP B-TREE/);
			expect(db.prepare("EXPLAIN QUERY PLAN " + byRoot).get(f.root, 1)?.detail).toContain("input_versions_root");
		});
		const collector = new ProvenanceCertificateStore(f.cache, { maxCertificates: 1 });
		try {
			expect((await collector.gc()).removedInputVersions).toBe(4);
			expect(await f.store.findPreparedInputCandidates(query)).toEqual([latest]);
			await collector.clear();
			expect(await f.store.findPreparedInputCandidates(query)).toEqual([]);
			expect(storeMetadata(f.cache, (db) => db.prepare("SELECT COUNT(*) AS n FROM input_preparations").get()?.n)).toBe(0);
		} finally {
			await collector.dispose();
		}
	} finally {
		await f.close();
	}
});

test("reads and publication reject incompatible metadata and preserve the original database", async () => {
	const f = await fixture();
	try {
		await mkdir(f.cache);
		storeMetadata(f.cache, (db) => db.exec("CREATE TABLE input_versions(id TEXT); INSERT INTO input_versions VALUES ('keep')"));
		const error = "incompatible provenance store schema";
		await expect(f.store.findPreparedInputCandidates({ root: f.root, binding: "tool", prepKey: "key" })).rejects.toThrow(error);
		await expect(f.store.stats()).rejects.toThrow(error);
		await expect(f.store.mayHaveCertificates()).rejects.toThrow(error);
		await expect(f.store.putInputVersion({ root: f.root, preparations: [] }, [])).rejects.toThrow(error);
		expect(storeMetadata(f.cache, (db) => db.prepare("SELECT id FROM input_versions").get()?.id)).toBe("keep");
	} finally {
		await f.close();
	}
});

test("reads leave an empty SQLite database untouched and reject a missing current index", async () => {
	const f = await fixture();
	try {
		await mkdir(f.cache);
		storeMetadata(f.cache, () => {});
		const lookup = () => f.store.findPreparedInputCandidates({ root: f.root, binding: "tool", prepKey: "key" });
		expect(await lookup()).toEqual([]);
		expect(storeMetadata(f.cache, (db) => db.prepare("SELECT name FROM sqlite_master").all())).toEqual([]);
		await f.store.putInputVersion({ root: f.root, preparations: [] }, []);
		await f.store.drain();
		expect(await lookup()).toEqual([]); // Open the reusable reader before another connection changes the schema.
		storeMetadata(f.cache, (db) => db.exec("DROP INDEX input_preparations_recent"));
		await expect(lookup()).rejects.toThrow("incompatible provenance store schema");
		await expect(f.store.putInputVersion({ root: f.root, preparations: [] }, [])).rejects.toThrow("incompatible provenance store schema");
	} finally {
		await f.close();
	}
});

linux("stops persistent recovery after a consumed input and releases its lease", async () => {
	const f = await fixture();
	const session = new ResourceInputSession({ store: f.store }, memoryBudget);
	try {
		for (const name of ["first", "second"]) {
			const file = path.join(f.root, name);
			await writeFile(file, name);
			const token = await f.manager.capture([{ path: file, scope: "content" }], memoryBudget);
			f.tokens.push(token);
			await f.manager.persist(token);
		}
		const restore = vi.spyOn(ResourceVersionManager.prototype, "restore");
		await session.run(async () => {
			for await (const token of session.reader(f.root, new AbortController().signal)()) {
				expect((await token.manager.validate(token)).expired).toBe(false);
				break;
			}
		});
		expect(restore).toHaveBeenCalledTimes(1);
		restore.mockClear();
		for await (const token of session.reader(f.root, new AbortController().signal)(path.join(f.root, "first"))) {
			expect((await token.view!.readFile(path.join(f.root, "first"))).toString()).toBe("first");
		}
		expect(restore).toHaveBeenCalledTimes(1);
		restore.mockClear();
		for await (const _token of session.reader(f.root, new AbortController().signal)(undefined, { key: "absent" }))
			throw new Error("no prepared values were stored");
		expect(restore).not.toHaveBeenCalled();
	} finally {
		await session.dispose();
		await f.close();
	}
	expect(session.contents.closed).toBe(true);
	expect(session.contents.openChunks).toBe(0);
});

linux("reads one path catalog and restores a shared closure once for concurrent queries", async () => {
	const f = await fixture();
	const session = new ResourceInputSession({ store: f.store }, memoryBudget);
	try {
		const paths = ["a", "b"].map((name) => path.join(f.root, name));
		for (const file of paths) await writeFile(file, file);
		const token = await f.manager.capture(paths.map((path) => ({ path, scope: "content" })), memoryBudget);
		f.tokens.push(token);
		await f.manager.persist(token);
		const catalog = vi.spyOn(f.store, "inputPathIndex");
		const fetch = vi.spyOn(f.store, "getInputVersion");
		const restore = vi.spyOn(ResourceVersionManager.prototype, "restore");
		const read = session.reader(f.root, new AbortController().signal);
		const buffers = await Promise.all([...paths, path.join(f.root, "absent")].map(async (file) => {
			for await (const token of read(file)) return (await token.view!.readFile(file)).toString();
		}));
		expect(buffers).toEqual([...paths, undefined]);
		for (const operation of [catalog, fetch, restore]) expect(operation).toHaveBeenCalledOnce();
		const signal = new AbortController().signal;
		const nextRead = session.reader(f.root, signal);
		const manifest = session.fileContentReader(f.root, signal);
		const first = async <T>(entries: AsyncIterable<T>) => {
			for await (const entry of entries) return entry;
		};
		fetch.mockRejectedValueOnce(new Error("metadata temporarily unavailable"));
		await expect(nextRead(paths[0]).next()).rejects.toThrow("metadata temporarily unavailable");
		expect(await first(nextRead(paths[0]))).toBeDefined();
		fetch.mockRejectedValueOnce(new Error("metadata temporarily unavailable"));
		expect(await first(manifest(paths[0]))).toBeUndefined();
		expect(await first(manifest(paths[0]))).toBeDefined();
	} finally {
		await session.dispose();
		await f.close();
	}
	expect(session.contents.openChunks).toBe(0);
});

linux("a prepared step borrowed from a shared view retains only the inputs it reads", async () => {
	const f = await fixture();
	const sibling = path.join(f.root, "sibling");
	const input = path.join(f.root, "input");
	const binding = createSerializablePreparationBinding("fixture.read-one", (value) => typeof value === "string");
	const signal = AbortSignal.timeout(15000);
	const owners: object[] = [];
	const session = new ResourceInputSession({ store: f.store }, 8 * 1024 * 1024);
	try {
		await writeFile(sibling, "before");
		await writeFile(input, "content");
		let builds = 0;
		const query = async (seed: boolean) => {
			const run = await evaluateResourceQuery(undefined, {
				signal, inputs: () => owners, onInputSource: (owner) => owners.push(owner),
			}, f.root, async (view) => {
				await view.readFile(sibling);
				if (seed) return (await view.readFile(input)).toString();
				return view.prepare(binding, "one", async (inputs) => {
					builds++;
					const value = (await inputs.readFile(input)).toString();
					return { value, bytes: value.length, dispose: () => {} };
				}, async (value) => value, input, signal);
			}, PI_ACTION_SEMANTICS, session.maxBytes, true, "output", undefined, session);
			f.tokens.push(...run!.versions);
			return run!.output;
		};
		await query(true);
		expect(await query(false)).toBe("content");
		await writeFile(sibling, "after");
		expect(await query(false)).toBe("content");
		expect(builds).toBe(1);
		await writeFile(input, "changed");
		expect(await query(false)).toBe("changed");
		expect(builds).toBe(2);
	} finally {
		await session.dispose();
		await f.close();
	}
});

linux("rejects incomplete, corrupt, over-budget and stale persistent preparation closures", async () => {
	const f = await fixture();
	const file = path.join(f.root, "input");
	try {
		await writeFile(file, "one\ntwo\nthree\n");
		const captured = await searchFixture(f, "produce");
		const record = (await f.store.getInputVersion(captured.id))!;
		type Payload = {
			preparations: {
				binding: Sha256Digest;
				key: string;
				resource?: string;
				dependencies: string[];
				value: ArtifactReference;
			}[];
		};
		const preparation = (record.payload as Payload).preparations[0]!;
		const getAgain = vi.spyOn(f.store, "getInputVersion");
		const enumerate = vi.spyOn(f.store, "candidatesInputVersions");
		const recovered = [];
		for await (const { id, token } of f.manager.recover(8 * 1024 * 1024, { preparation })) {
			recovered.push(id);
			await token.release();
		}
		expect(recovered).toEqual([record.id]);
		expect(getAgain).not.toHaveBeenCalled();
		expect(enumerate).not.toHaveBeenCalled();
		expect(record.artifacts).toContainEqual(preparation.value);
		await storePeer(f.cache).completed;
		expect(await f.store.artifacts.get(preparation.value)).toBeDefined();
		for (const field of ["dependency", "duplicate", "artifact", "resource", "codec"] as const) {
			const payload = structuredClone(record.payload) as Payload;
			const value = payload.preparations[0]!;
			let artifacts = [...record.artifacts];
			if (field === "dependency") value.dependencies.push("content:unknown");
			else if (field === "duplicate") payload.preparations.push(value);
			else if (field === "resource") value.resource = path.dirname(f.root);
			else {
				artifacts = artifacts.filter((reference) => reference.digest !== value.value.digest);
				if (field === "codec") {
					value.value = await f.store.artifacts.put('{"x":1,"x":2}');
					artifacts.push(value.value);
				}
			}
			const proposed = await f.store.putInputVersion(payload, artifacts);
			await expect(f.manager.restore(proposed, 8 * 1024 * 1024), field).rejects.toThrow(
				/closure|data_invalid/,
			);
		}
		await expect(f.manager.restore(record, 1024)).rejects.toThrow(/budget/);
		const restored = (await f.manager.restore(record, 8 * 1024 * 1024))!;
		f.tokens.push(restored);
		expect(restored.view!.hasPreparedInputs).toBe(true);
		await expect(
			restored.view!.restorePreparations((record.payload as Payload).preparations, f.store),
		).rejects.toThrow(/not_capturing/);
		const artifact = f.store.artifacts.artifactPath(preparation.value.digest);
		const original = (await f.store.artifacts.get(preparation.value))!;
		await writeFile(artifact, Buffer.alloc(original.length, 32));
		await expect(f.manager.restore(record, 8 * 1024 * 1024)).rejects.toThrow(/integrity/);
		const oversized = await open(artifact, "r+");
		try {
			await oversized.truncate(64 * CONTENT_CHUNK_BYTES);
		} finally {
			await oversized.close();
		}
		const starting = process.memoryUsage().rss;
		let peak = starting;
		const sample = setInterval(() => {
			peak = Math.max(peak, process.memoryUsage().rss);
		}, 1);
		try {
			await expect(f.manager.restore(record, 8 * 1024 * 1024)).rejects.toThrow(/integrity/);
		} finally {
			clearInterval(sample);
		}
		const growth = Math.max(peak, process.memoryUsage().rss) - starting;
		console.info(
			JSON.stringify({
				oversizedPreparationBytes: 64 * CONTENT_CHUNK_BYTES,
				expectedBytes: original.length,
				peakRSSGrowth: growth,
			}),
		);
		expect(growth).toBeLessThan(32 * CONTENT_CHUNK_BYTES);
		await rm(artifact);
		await expect(f.manager.restore(record, 8 * 1024 * 1024)).rejects.toThrow(/missing/);
		await f.store.artifacts.put(original);
		await writeFile(file, "changed source\n");
		expect(await f.manager.restore(record, 8 * 1024 * 1024)).toBeUndefined();
		expect((await f.manager.validate(restored)).expired).toBe(true);
		await restored.release();
		expect(restored.view!.hasPreparedInputs).toBe(false);
	} finally {
		await f.close();
	}
});

linux("a failed file input batch rolls back every row and releases the captured tokens", async () => {
	const f = await fixture();
	try {
		const abort = new AbortController();
		await using files = new ResourceObjectInputs(f.root, f.store, abort.signal);
		for (const name of ["one", "two"]) {
			const target = path.join(f.root, name);
			await writeFile(target, name);
			files.remember(target, await captureFileContent(target, memoryBudget, files.contents));
		}
		const capture = ResourceInputSession.prototype.capture;
		const released: ReturnType<typeof vi.fn>[] = [];
		vi.spyOn(ResourceInputSession.prototype, "capture").mockImplementation(async function (this: ResourceInputSession, ...args) {
			const token = await capture.apply(this, args);
			const release = vi.fn(token.release);
			released.push(release);
			if (released.length === 4) abort.abort(new Error("capture cancelled"));
			return { ...token, release };
		});
		const put = f.store.putInputVersion.bind(f.store);
		let inserted = 0;
		vi.spyOn(f.store, "putInputVersion").mockImplementation(async (...args) => {
			const record = await put(...args);
			if (++inserted === 2) throw new Error("injected publication failure");
			return record;
		});
		await expect(files.publish()).rejects.toThrow("file input publication failed");
		expect(inserted).toBe(2);
		expect((await f.store.stats()).inputVersions).toBe(0);
		expect(released).toHaveLength(2);
		await expect(files.publish()).rejects.toThrow("capture cancelled");
		expect(released).toHaveLength(4);
		expect(inserted).toBe(2);
		for (const release of released) expect(release).toHaveBeenCalledOnce();
	} finally {
		await f.close();
	}
});

linux("borrows persisted file versions on cold start without redundant SQLite writes and batches new publications", async () => {
	const f = await fixture();
	const root = path.parse(f.root).root;
	const tool = new ResourceVersionManager(root, { watch: false, persistentInputs: { store: f.store, internalCache: true } });
	const first = path.join(f.root, "first");
	const second = path.join(f.root, "second");
	const third = path.join(f.root, "third");
	const fourth = path.join(f.root, "fourth");
	try {
		for (const [target, text] of [
			[first, "alpha\n"],
			[second, "beta\n"],
			[third, "gamma\n"],
			[fourth, "delta\n"],
		] as const) {
			await writeFile(target, text);
			await utimes(target, 915148800, 946684800);
		}
		for (const target of [first, second]) {
			const token = await tool.capture([{ path: target, scope: "content" }], memoryBudget);
			try {
				await tool.persist(token);
			} finally {
				await token.release();
			}
		}
		tool.close();
		await f.manager.close();
		await f.store.dispose();

		const worker = `
			import { DatabaseSync } from 'node:sqlite';
			import { lstat, writeFile, utimes } from 'node:fs/promises';
			import { captureFileContent, readContent } from './src/content-chunks.ts';
			import { ResourceObjectInputs } from './src/agent-execution-world.ts';
			import { ResourceInputSession } from './src/resource-version.ts';
			import { ProvenanceCertificateStore } from './src/reuse-store.ts';
			import { SpeculativeConfiguration } from './src/config-schema.ts';
			const [workspace, cache, first, second, third, fourth] = process.argv.slice(2);
			await new SpeculativeConfiguration({ filesystem: {
				maxDigestEntries: 0, timestampQuietMs: 0, timestampQuietByFilesystem: {}
			} }).run(async () => {
			const store = new ProvenanceCertificateStore(cache);
			const signal = new AbortController().signal;
			let commits = 0;
			const exec = DatabaseSync.prototype.exec;
			DatabaseSync.prototype.exec = function (sql, ...rest) {
				if (typeof sql === 'string' && sql === 'COMMIT') commits++;
				return exec.call(this, sql, ...rest);
			};
			try {
				const beforeStats = await store.stats();
				let borrowedValues = [];
				let republished = -1;
				let hits = 0;
				{
					await using native = new ResourceObjectInputs(workspace, store, signal);
					for (const target of [first, second]) {
						const borrowed = await native.borrow(target, await lstat(target, { bigint: true }), native.contents);
						if (!borrowed?.shared || !borrowed.computation) throw new Error('cold borrow missed ' + target);
						borrowedValues.push((await readContent(borrowed.content)).toString());
						if (!native.remember(target, borrowed)) throw new Error('remember rejected borrowed input');
					}
					hits = native.hits;
					republished = await native.publish();
				}
				const afterBorrowStats = await store.stats();
				const borrowCommits = commits;
				commits = 0;
				let batchPublished = 0;
				{
					await using native = new ResourceObjectInputs(workspace, store, signal);
					for (const target of [third, fourth])
						native.remember(target, await captureFileContent(target, 262144, native.contents));
					batchPublished = await native.publish();
				}
				const batchCommits = commits;
				await writeFile(third, 'mutated\\n');
				await utimes(third, 915148800, 946684900);
				const session = new ResourceInputSession({ store, internalCache: true }, 262144);
				const surviving = [];
				try {
					for (const target of [third, fourth]) {
						for await (const token of session.reader('/', signal)(target)) {
							const item = token.view?.snapshotInput(target);
							if (item && 'content' in item) surviving.push([target, (await readContent(item.content)).toString()]);
						}
					}
				} finally {
					await session.dispose();
				}
				console.log(JSON.stringify({
					borrowedValues,
					hits,
					republished,
					addedOnBorrow: afterBorrowStats.inputVersions - beforeStats.inputVersions,
					borrowCommits,
					batchPublished,
					batchCommits,
					surviving,
				}));
			} finally {
				await store.dispose();
			}
			});
		`;
		const { stdout } = await promisify(execFile)(process.execPath, [
			"--input-type=module",
			"-e",
			worker,
			"cold-borrow-worker",
			f.root,
			f.cache,
			first,
			second,
			third,
			fourth,
		]);
		expect(JSON.parse(stdout)).toEqual({
			borrowedValues: ["alpha\n", "beta\n"],
			hits: 2,
			republished: 0,
			addedOnBorrow: 0,
			borrowCommits: 0,
			batchPublished: 2,
			batchCommits: 1,
			surviving: [[fourth, "delta\n"]],
		});
	} finally {
		tool.close();
		await f.close();
	}
});

linux("recovers actual input bytes, names, aliases and current proofs in a cold process", async () => {
	const f = await fixture();
	const file = path.join(f.root, "input");
	const alias = path.join(f.root, "alias");
	try {
		await writeFile(file, "immutable input\n");
		await symlink("input", alias);
		for (const target of [f.root, file]) await utimes(target, 915148800, 946684800);
		const before = await metadata([f.root, file, alias]);
		const token = await f.manager.capture([{ path: f.root, scope: "tree_content" }], memoryBudget);
		f.tokens.push(token);
		expect((await token.view!.readFile(alias)).toString()).toBe("immutable input\n");
		const id = await f.manager.persist(token);
		await token.release();
		f.manager.close();
		await f.store.dispose();
		const source = `
			import { ResourceVersionManager } from './src/resource-version.ts';
			import { ProvenanceCertificateStore } from './src/reuse-store.ts';
			const [root, cache, id] = process.argv.slice(2), store = new ProvenanceCertificateStore(cache);
			const manager = new ResourceVersionManager(root, { watch: false, persistentInputs: { store } });
			const token = await manager.restore(await store.getInputVersion(id), 262144);
			if (!token) throw new Error('cold input restore missed');
			try { console.log(JSON.stringify({ value: (await token.view.readFile(root + '/alias')).toString(),
				names: await token.view.readdir(root), expired: (await manager.validate(token)).expired,
				window: (await manager.seal(token)).expired, watching: token.watching })); }
			finally { await token.release(); manager.close(); await store.dispose(); }
		`;
		const { stdout } = await promisify(execFile)(process.execPath, [
			"--import",
			"tsx",
			"--input-type=module",
			"-e",
			source,
			"input-version-worker",
			f.root,
			f.cache,
			id,
		]);
		expect(JSON.parse(stdout)).toEqual({
			value: "immutable input\n",
			names: ["alias", "input"],
			expired: false,
			window: true,
			watching: false,
		});
		expect(await metadata([f.root, file, alias])).toEqual(before);
	} finally {
		await f.close();
	}
});

linux(
	"binds restored input values to their observations even when a publisher seals inconsistent data",
	async () => {
		const f = await fixture();
		const file = path.join(f.root, "input");
		const alias = path.join(f.root, "alias");
		try {
			await writeFile(file, "original content");
			await symlink("input", alias);
			const token = await f.manager.capture([{ path: f.root, scope: "tree_content" }], memoryBudget);
			f.tokens.push(token);
			const record = (await f.store.getInputVersion(await f.manager.persist(token)))!;
			for (const field of ["content", "names", "alias", "size", "omitted", "extra"] as const) {
				const payload = structuredClone(record.payload) as {
					entries: [string, Record<string, any>][];
				};
				let artifacts = record.artifacts;
				const entry = (target: string) => payload.entries.find(([name]) => name === target)![1];
				if (field === "content") {
					const reference = await f.store.artifacts.put("tampered content");
					entry(file).content = {
						size: reference.size,
						digest: reference.digest,
						chunks: [reference],
					};
					artifacts = [reference];
				} else if (field === "names") entry(f.root).entries = ["invented"];
				else if (field === "alias") entry(alias).target = path.join(f.root, "invented");
				else if (field === "size") entry(f.root).metadata = { ...entry(f.root).metadata, size: 1 };
				else if (field === "omitted") payload.entries = payload.entries.filter(([name]) => name !== alias);
				else payload.entries.push([path.join(f.root, "invented"), { ...entry(file) }]);
				const proposed = await f.store.putInputVersion(payload, artifacts);
				expect(await f.manager.restore(proposed, memoryBudget), field).toBeUndefined();
			}
			const restored = await f.manager.restore(record, memoryBudget);
			f.tokens.push(restored!);
			expect((await restored!.view!.readFile(alias)).toString()).toBe("original content");
		} finally {
			await f.close();
		}
	},
);
