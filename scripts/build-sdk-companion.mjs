#!/usr/bin/env node
// Rebuild the unpublished companion from a fixed upstream checkout and verified model data.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const inputs = fileURLToPath(new URL("../vendor/pi-sdk/", import.meta.url));
const source = JSON.parse(readFileSync(path.join(inputs, "source.json"), "utf8"));
const [upstream, modelData, build, output] = process.argv.slice(2).map(value => path.resolve(value));
assert(upstream && modelData && build && output,
	"usage: node scripts/build-sdk-companion.mjs UPSTREAM_CHECKOUT MODEL_DATA_ARCHIVE NEW_BUILD_DIR NEW_OUTPUT_DIR");
assert(!existsSync(build) && !existsSync(output), "build and output directories must not exist");
const sha256 = file => createHash("sha256").update(readFileSync(file)).digest("hex");
const patch = path.join(inputs, source.patch.file);
assert.equal(sha256(patch), source.patch.sha256, "source patch checksum");
assert.equal(sha256(modelData), source.modelData.sha256, "model data archive checksum");
mkdirSync(output, { recursive: true });
let step = 0;
function run(command, args, cwd = build, echo = true) {
	console.log(`\n[${++step}] ${command} ${args.join(" ")}`);
	const npmCli = path.join(path.dirname(process.execPath), "node_modules/npm/bin/npm-cli.js");
	const windowsNpm = command === "npm" && process.platform === "win32";
	const result = spawnSync(windowsNpm ? process.execPath : command, windowsNpm ? [npmCli, ...args] : args, {
		cwd, env: { ...process.env, PI_OFFLINE: "1" }, encoding: "utf8", maxBuffer: 64 * 1024 * 1024,
	});
	writeFileSync(path.join(output, `${String(step).padStart(2, "0")}.log`), (result.stdout ?? "") + (result.stderr ?? ""));
	if (echo) process.stdout.write(result.stdout ?? "");
	process.stderr.write(result.stderr ?? "");
	if (result.error) throw result.error;
	assert.equal(result.status, 0, `${command} failed (${result.signal ?? result.status})`);
	return result.stdout;
}
const npm = "npm";
const runtime = { node: process.version, npm: run(npm, ["--version"], upstream).trim(), platform: process.platform, arch: process.arch };
run("git", ["clone", "--quiet", "--no-hardlinks", "--no-checkout", upstream, build], path.dirname(build));
run("git", ["checkout", "--detach", source.commit]);
assert.equal(run("git", ["rev-parse", "HEAD"]).trim(), source.commit);
run("git", ["apply", "--check", patch]);
run("git", ["apply", patch]);
run("tar", ["-xf", modelData, "--strip-components=1", "-C", build, source.modelData.member]);
const readJson = file => JSON.parse(readFileSync(path.join(build, file), "utf8"));
const writeJson = (file, value) => writeFileSync(path.join(build, file), JSON.stringify(value, null, "\t") + "\n");
const originalLock = readJson("package-lock.json");
run(npm, ["ci", "--ignore-scripts", "--no-audit", "--no-fund"]);
for (const pkg of source.packages) {
	const file = `packages/${pkg.directory}/package.json`;
	const metadata = readJson(file);
	assert.equal(metadata.name, pkg.name);
	metadata.version = source.version;
	writeJson(file, metadata);
}
run(process.execPath, ["scripts/sync-versions.js"]);
run(npm, ["install", "--package-lock-only", "--ignore-scripts", "--no-audit", "--no-fund"]);
const updatedLock = readJson("package-lock.json");
const externalPackages = lock => Object.fromEntries(Object.entries(lock.packages)
	.filter(([name, entry]) => name.includes("node_modules/") && !entry.link)
	.map(([name, entry]) => [name, { version: entry.version, resolved: entry.resolved, integrity: entry.integrity }]));
assert.deepEqual(externalPackages(updatedLock), externalPackages(originalLock), "external dependency drift");
run(process.execPath, ["scripts/generate-coding-agent-shrinkwrap.mjs"]);
run(process.execPath, ["scripts/generate-coding-agent-install-lock.mjs"]);
run(npm, ["run", "check"]);
run(npm, ["run", "build:offline"]);
const vitest = path.join(build, "node_modules/vitest/dist/cli.js");
run(process.execPath, [vitest, "--run", "test/agent-loop.test.ts"], path.join(build, "packages/agent"));
run(process.execPath, [vitest, "--run",
	"test/suite/agent-session-execution.test.ts", "test/suite/agent-session-prompt.test.ts",
	"test/suite/agent-session-queue.test.ts", "test/suite/agent-session-retry-events.test.ts",
	"test/suite/regressions/1717-2113-agent-session-event-settlement.test.ts",
	"test/suite/regressions/6363-agent-settled-event.test.ts", "test/suite/regressions/5998-blocked-tool-terminate.test.ts",
], path.join(build, "packages/coding-agent"));
const artifacts = [];
for (const pkg of source.packages) {
	const directory = path.join(build, "packages", pkg.directory);
	const [packed] = JSON.parse(run(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", output], directory, false));
	assert.equal(packed.name, pkg.name);
	assert.equal(packed.version, source.version);
	const file = path.join(output, packed.filename);
	artifacts.push({ name: pkg.name, version: source.version, file: packed.filename, digest: `sha256:${sha256(file)}`, bytes: statSync(file).size });
}
const dependencies = Object.fromEntries(artifacts.map(pkg => [pkg.name, `file:${pkg.file}`]));
writeFileSync(path.join(output, "package.json"), JSON.stringify({
	name: "pi-speculative-sdk-companion", version: source.version, private: true,
	description: "Companion package inventory; use the project root lockfile for installation.",
	dependencies, overrides: dependencies,
}, null, 2) + "\n");
writeFileSync(path.join(output, "manifest.json"), JSON.stringify({
	source, runtime, rootLockSha256: sha256(path.join(build, "package-lock.json")), artifacts,
}, null, 2) + "\n");
console.log(JSON.stringify({ output, version: source.version, artifacts: artifacts.length }));
