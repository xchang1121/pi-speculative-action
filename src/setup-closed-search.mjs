#!/usr/bin/env node

import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, rename, rm } from "node:fs/promises";
import path from "node:path";
import { brotliDecompressSync, gunzipSync } from "node:zlib";
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import { CLOSED_SEARCH_PROFILE } from "./closed-search-kernel.mjs";

// Explicit setup only: no npm install, CLI links, PATH edits, or network on the tool-call path.
const destination = path.resolve(process.argv[2] ?? path.join(getAgentDir(), "speculative-action", "closed-search.wasm"));
const response = await fetch("https://registry.npmjs.org/ripgrep/-/ripgrep-0.3.1.tgz", { signal: AbortSignal.timeout(30_000) });
assert.ok(response.ok && response.body, `Search engine download failed: HTTP ${response.status}`);
const chunks = []; let length = 0;
for await (const chunk of response.body) {
	assert.ok((length += chunk.length) <= 1024 * 1024, "Search engine archive exceeds its download budget");
	chunks.push(chunk);
}
const archive = Buffer.concat(chunks);
assert.equal(createHash("sha512").update(archive).digest("base64"),
	"6bDtNIBh1qPviVIU685/4uv0Ap5t8eS4wiJhy/tR2LdIeIey9CVasENlGS+ul3HnTmGANIp7AjnfsztsRmALfQ==",
	"Search engine archive integrity mismatch");

// Read just the pinned data module from the verified archive; never extract paths onto the host.
const tar = gunzipSync(archive, { maxOutputLength: 2 * 1024 * 1024 });
let encoded;
for (let offset = 0; offset + 512 <= tar.length;) {
	const size = Number.parseInt(tar.subarray(offset + 124, offset + 136).toString(), 8) || 0;
	if (tar.subarray(offset, offset + 100).toString().replace(/\0.*$/su, "") === "package/lib/_rg.wasm.mjs") {
		encoded = tar.subarray(offset + 512, offset + 512 + size); break;
	}
	offset += 512 + Math.ceil(size / 512) * 512;
}
assert.ok(encoded, "Pinned search data module is missing");
const { getCompressedBytes } = await import(`data:text/javascript;base64,${encoded.toString("base64")}`);
const moduleBytes = brotliDecompressSync(getCompressedBytes(), { maxOutputLength: 4 * 1024 * 1024 });
assert.equal(createHash("sha256").update(moduleBytes).digest("hex"), CLOSED_SEARCH_PROFILE.rg, "Search module integrity mismatch");

await mkdir(path.dirname(destination), { recursive: true });
const staged = `${destination}.${randomUUID()}.tmp`;
const publication = await open(staged, "wx", 0o600);
try {
	try { await publication.writeFile(moduleBytes); }
	finally { await publication.close(); }
	await rename(staged, destination);
} finally { await rm(staged, { force: true }); }
console.log(`Closed search engine installed: ${destination}\nNo Actor executor or settings were changed.`);
