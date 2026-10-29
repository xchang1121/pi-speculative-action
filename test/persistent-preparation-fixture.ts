import assert from "node:assert/strict";
import path from "node:path";
import { createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { ResourceVersionManager } from "../src/resource-version.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";

const [mode, root, cache] = process.argv.slice(2);
assert.equal(mode, "produce");
const store = new ProvenanceCertificateStore(cache, { orphanGraceMs: 0 });
const manager = new ResourceVersionManager(root!, { watch: false, persistentInputs: { store } });
const profile = await createClosedSearchProfile(root!), invocation = profile.invocations.get("grep");
const token = await manager.capture(undefined, 8 * 1024 * 1024);
try {
	assert.ok(invocation?.filesystem, "qualified native grep is required for this contract");
	const output = await invocation.filesystem(token.view!, { args: { pattern: "two", path: path.join(root!, "input"),
		limit: 20, ignoreCase: false, literal: false }, callID: "cold-search", signal: new AbortController().signal });
	assert.equal(output.isError, false);
	token.view!.seal();
	const id = await manager.persist(token);
	const record = await store.getInputVersion(id);
	assert.ok((record?.payload as { preparations?: unknown[] }).preparations?.length);
	console.log(JSON.stringify({ id }));
} finally {
	await token.release(); manager.close(); await profile.pool.dispose(); await store.dispose();
}
