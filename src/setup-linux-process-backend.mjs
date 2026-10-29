#!/usr/bin/env node

import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { qualifyNativeStartup } from "./process-startup.mjs";

const SANDLOCK_REVISION = "f6a3e39b31afa80f66609c8af8ae5b2582f628e8";
const SANDLOCK_REPOSITORY = "https://github.com/multikernel/sandlock.git";
const SANDLOCK_PATCH = fileURLToPath(new URL("./sandlock-transparent-exec.patch", import.meta.url));
const SANDLOCK_DIRECTORY = fileURLToPath(new URL("./sandlock-directory.rs", import.meta.url));
const SQLITE_HEADERS = {
	"sqlite3.h": "919e7f2e8ed1d8f56ac17b412b8971c76aa5d1a879752cc6058f75e7d5910e1d",
	"sqlite3ext.h": "ac9645e5c9ff0cf176efdd6e75cb5e98f46295d38e02db5c4d208826a39ab4be",
};
const ARCHIVES = {
	strace: {
		target: "pi-speculative-strace", url: "https://strace.io/files/6.8/strace-6.8.tar.xz",
		sha256: "ba6950a96824cdf93a584fa04f0a733896d2a6bc5f0ad9ffe505d9b41e970149", patch: "strace-handoff.patch",
		configure: ["--enable-mpers=no", "--without-libunwind", "--without-libdw"], artifact: "src/strace",
	},
	overlayfs: {
		target: "fuse-overlayfs", url: "https://codeload.github.com/containers/fuse-overlayfs/tar.gz/refs/tags/v1.18",
		sha256: "fdd1896c8de35a15eb14444d7880be81d635fcbbc4ad162d8bc3ccf5627aa8c7", patch: "fuse-overlayfs.patch",
		configure: [], artifact: "fuse-overlayfs", environment: { LDFLAGS: "-static", LIBS: "-ldl" },
	},
};

if (process.platform !== "linux") {
	throw new Error("The process-reuse backend must be installed from Linux or WSL 2.");
}

const localRoot = path.join(os.homedir(), ".local");
const localBin = path.join(localRoot, "bin");
const sandlock = path.join(localBin, "pi-speculative-sandlock");
const heldExec = path.join(localBin, "pi-speculative-held-exec");
if (process.argv[1] === fileURLToPath(import.meta.url)) await setup();
async function setup() {
	await mkdir(localBin, { recursive: true });
	const helper = await optional("Native execution helper", installHeldExec);
	const held = helper && await optional("Actor child handoff", () => qualifyNativeStartup(heldExec));
	const tracing = helper && await optional("Running process handoff tracer", installStrace);
	const missing = await missingExecutables([...(tracing ? [] : ["strace"]), "git"]);
	let producer = false;
	if (!helper)
		console.warn("Speculative Bash producer unavailable because its transparent exec boundary is unavailable.");
	else if (missing.length)
		console.warn(`Speculative Bash producer unavailable (missing ${missing.join(", ")}); cached Actor replay remains usable.`);
	else producer = await optional("Speculative Bash producer", installSandlock);
	const overlay = await optional("OverlayFS workspace optimization", installFuseOverlayfs);
	console.log(
		`Linux reuse setup complete: cached command replay ready; child handoff ${held ? "ready" : "unavailable"}; running process capture ${tracing ? "ready" : "unavailable"}; speculative producer ${producer ? "ready" : "unavailable"}; OverlayFS ${overlay ? "ready" : "unavailable"}.`,
	);
}

async function installSandlock() {
	return installNative(sandlock, sha256(SANDLOCK_REVISION, await readFile(SANDLOCK_PATCH), await readFile(SANDLOCK_DIRECTORY)), qualifySandlock, async (buildRoot, temporary) => {
		const cargo = await executable([path.join(os.homedir(), ".cargo", "bin", "cargo"), "cargo"]).catch(() => {
			throw new Error("Rust stable is required to build Sandlock. Install it from https://rustup.rs and retry.");
		});
		const git = await executable(["git"]), source = path.join(buildRoot, "source"), targetRoot = path.join(buildRoot, "target");
		console.log(`Building Sandlock ${SANDLOCK_REVISION.slice(0, 12)} into ${localRoot} ...`);
		await run(git, ["clone", "--quiet", "--no-checkout", SANDLOCK_REPOSITORY, source]);
		await run(git, ["-C", source, "checkout", "--quiet", "--detach", SANDLOCK_REVISION]);
		await run(git, ["-C", source, "apply", "--whitespace=error-all", SANDLOCK_PATCH]);
		await copyFile(SANDLOCK_DIRECTORY, path.join(source, "crates/sandlock-core/src/cow/dispatch.rs"));
		await run(cargo, [
			"build", "--locked", "--release", "-p", "sandlock-cli",
			"--manifest-path", path.join(source, "Cargo.toml"), "--target-dir", targetRoot,
		]);
		await copyFile(path.join(targetRoot, "release", "sandlock"), temporary, fsConstants.COPYFILE_EXCL);
	});
}

async function qualifySandlock(binary) {
	await run(binary, ["check"]);
	await run(binary, [
		"run", "--chroot", "/", "--fs-read", "/", "--", heldExec, "--probe-clean-fds",
	]);
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-dispatch-"));
	try {
		const view = path.join(root, "view");
		const image = path.join(view, "false");
		await mkdir(view);
		await writeFile(image, "#!/bin/sh\nexit 42\n", { mode: 0o700 });
		await run(binary, [
			"run", "--chroot", "/", "--fs-read", "/", "--", process.execPath, "-e", `
				const fs = require('node:fs'), assert = require('node:assert/strict');
				assert.throws(() => fs.openSync('/pi-speculative-action/not/present', 'r'), { code: 'ENOENT' });
				for (const tail of ['/package.json', '/../false']) {
					assert.throws(() => fs.statSync(process.argv[1] + tail), { code: 'ENOTDIR' });
					assert.throws(() => fs.openSync(process.argv[1] + tail, 'r'), { code: 'ENOTDIR' });
				}
				for (const p of [process.argv[1], require('node:path').dirname(process.argv[1])]) {
					assert.throws(() => fs.readlinkSync(p), { code: 'EINVAL' });
					assert.equal(fs.realpathSync.native(p), fs.realpathSync(p));
				}
			`, image,
		]);
		await run(binary, ["run", "--chroot", "/", "--fs-read", "/", "--exec-mount", `/bin/false:${image}`, "--", "/bin/false"], 42);
		await copyFile(heldExec, image);
		await chmod(image, 0o755);
		// Qualify the same stopped-image route used by the producer; the broker replaces /bin/false's exit.
		const broker = net.createServer({ allowHalfOpen: true }, socket => socket.resume().on("end", () => socket.end("x 0\n")));
		await writeFile(`${image}.configuration`, `${path.join(root, "broker")}\ntoken\n`, { mode: 0o600 });
		await new Promise(resolve => broker.listen(path.join(root, "broker"), resolve));
		try { await run(path.join(localBin, "pi-speculative-strace"), ["--kill-on-exit", "--trace-children-only", "-f", "-q", "-e", "trace=none", "-o", "/dev/null",
			`--process-library=${heldExec}.so`, `--exec-context-source=${image}`, "--",
			binary, "run", "--chroot", "/", "--fs-read", "/", "--fs-write", path.join(root, "broker"), "--exec-mount", `/:${image}`, "--", "/bin/false"]); }
		finally { broker.close(); }
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

export async function installHeldExec(target = heldExec) {
	const source = fileURLToPath(new URL("./linux-held-exec.c", import.meta.url));
	const sqlite = fileURLToPath(new URL("./sqlite-read-vfs.c", import.meta.url));
	return installNative(target, sha256("static-pthread+shared-image+native-startup+authority-observer+sqlite-read", JSON.stringify(SQLITE_HEADERS), await readFile(source), await readFile(sqlite)), qualifyHeldExec, async (root, temporary) => {
		const compiler = await executable(["cc", "gcc", "clang"]);
		for (const [suffix, flags] of [["", ["-static"]], [".observer", ["-static", "-DPI_SPEC_AUTHORITY_ONLY=1"]], [".so", ["-shared", "-fPIC"]]])
			await run(compiler, [...flags, "-pthread", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", temporary + suffix]);
		await copyFile(`${temporary}.so`, `${temporary}.node`, fsConstants.COPYFILE_EXCL);
		for (const [name, digest] of Object.entries(SQLITE_HEADERS))
			await writeFile(path.join(root, name), await downloadPinned(`https://raw.githubusercontent.com/nodejs/node/v24.20.0/deps/sqlite/${name}`, digest));
		await run(compiler, ["-shared", "-fPIC", "-pthread", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", "-I", root, sqlite, "-o", `${temporary}.sqlite`]);
	}, [".so", ".node", ".sqlite", ".observer", ""]);
}

async function qualifyHeldExec(binary) {
	await run(binary, ["--exec", "21", "pi-exec-probe", binary, "--probe-clean-fds"]);
	await qualifyNativeStartup(binary, { observer: `${binary}.observer`, confined: true });
	await run(process.execPath, ["--input-type=module", "-e", "import{DatabaseSync}from'node:sqlite';const db=new DatabaseSync(':memory:',{allowExtension:true});try{db.loadExtension(process.argv[1]);db.prepare('SELECT sqlite_version()').get()}finally{db.close()}", `${binary}.sqlite`]);
}

async function installStrace() {
	if (process.arch !== "x64") throw new Error("running process capture requires x86-64 Linux");
	return installArchive(ARCHIVES.strace, binary => run(binary, ["--kill-on-exit", "-f", "-q", "-e", "trace=none", "-o", "/dev/null",
		`--process-library=${heldExec}.so`, "--handoff-image=/dev/null", "--", "/bin/true"]));
}

async function installFuseOverlayfs() {
	await access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK);
	await executable(["fusermount3", "fusermount"]);
	return installArchive(ARCHIVES.overlayfs, binary => run(binary, ["--version"]));
}

/** Pin both upstream bytes and the local patch, with the same build and replacement lifecycle. */
async function installArchive(specification, qualify) {
	const patch = fileURLToPath(new URL(specification.patch, import.meta.url));
	return installNative(path.join(localBin, specification.target), sha256(JSON.stringify(specification), await readFile(patch)), qualify, async (root, temporary) => {
		const [git, tar, make] = await Promise.all(["git", "tar", "make"].map(name => executable([name])));
		const archive = path.join(root, "source.archive"), source = path.join(root, "source");
		await writeFile(archive, await downloadPinned(specification.url, specification.sha256));
		await mkdir(source);
		await run(tar, ["-xf", archive, "-C", source, "--strip-components=1", "--no-same-owner", "--no-same-permissions"]);
		await run(git, ["-C", source, "apply", "--whitespace=error-all", patch]);
		if (!await access(path.join(source, "configure")).then(() => true, () => false))
			await run(await executable(["autoreconf"]), ["-fiv"], 0, source);
		await run(path.join(source, "configure"), specification.configure, 0, source, specification.environment);
		await run(make, ["-j", String(Math.min(4, os.availableParallelism()))], 0, source, specification.environment);
		await copyFile(path.join(source, specification.artifact), temporary, fsConstants.COPYFILE_EXCL);
	});
}

/** Qualify a complete artifact set before replacement; publish the executable after its libraries. */
async function installNative(target, sourceDigest, qualify, build, suffixes = [""]) {
	const stamp = `${target}.sha256`, digest = async binary => sha256(...await Promise.all(suffixes.map(suffix => readFile(binary + suffix))));
	try {
		const current = await digest(target);
		if ((await readFile(stamp, "utf8")).trim() !== `${sourceDigest}:${current}`) throw new Error("installation changed");
		await qualify(target); console.log(`${path.basename(target)} ready: ${target}`);
		return;
	} catch { /* Rebuild the exact pinned source and packaged patch. */ }
	const root = await mkdtemp(path.join(os.tmpdir(), `${path.basename(target)}-`));
	const temporary = `${target}.${process.pid}.tmp`;
	try {
		await build(root, temporary);
		await chmod(temporary, 0o755);
		await qualify(temporary);
		const installed = await digest(temporary);
		for (const suffix of suffixes) await rename(temporary + suffix, target + suffix);
		await writeFile(stamp, `${sourceDigest}:${installed}\n`, { mode: 0o600 });
	} finally {
		await Promise.all([rm(root, { recursive: true, force: true }), ...suffixes.map(suffix => rm(temporary + suffix, { force: true }))]);
	}
	console.log(`${path.basename(target)} ready: ${target}`);
}

async function executable(candidates) {
	for (const candidate of candidates) {
		const targets = candidate.includes(path.sep) ? [candidate] : (process.env.PATH ?? "").split(path.delimiter).filter(Boolean).map(directory => path.join(directory, candidate));
		for (const target of targets) try { await access(target); return target; } catch { /* Continue the same ordered lookup. */ }
	}
	throw new Error(`Executable not found: ${candidates.join(" or ")}`);
}

async function missingExecutables(candidates) {
	const results = await Promise.all(candidates.map((candidate) =>
		executable([candidate]).then(() => undefined, () => candidate)));
	return results.filter(Boolean);
}

async function optional(label, operation) {
	try {
		await operation();
		return true;
	} catch (error) {
		console.warn(`${label} unavailable: ${error instanceof Error ? error.message : String(error)}`);
		return false;
	}
}

function sha256(...values) {
	const hash = createHash("sha256");
	for (const value of values) hash.update(value);
	return hash.digest("hex");
}

async function downloadPinned(url, expected) {
	const response = await fetch(url, { redirect: "follow" });
	if (!response.ok) throw new Error(`download failed (${response.status} ${response.statusText})`);
	const bytes = Buffer.from(await response.arrayBuffer()), digest = sha256(bytes);
	if (digest !== expected) throw new Error(`download checksum mismatch: ${digest}`);
	return bytes;
}

async function run(command, args, expected = 0, cwd, environment) {
	const [code, signal] = await once(spawn(command, args, { stdio: "inherit", cwd, env: { ...process.env, ...environment } }), "exit");
	if (code !== expected || signal !== null) throw new Error(`${command} failed (${signal ?? code ?? "unknown"})`);
}
