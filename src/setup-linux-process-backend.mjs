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
const STRACE_RELEASE = "6.8";
const STRACE_SHA256 = "ba6950a96824cdf93a584fa04f0a733896d2a6bc5f0ad9ffe505d9b41e970149";
const STRACE_PATCH = fileURLToPath(new URL("./strace-handoff.patch", import.meta.url));
const FUSE_OVERLAYFS_RELEASE = "v1.18";
const FUSE_OVERLAYFS_ASSETS = Object.freeze({
	x64: {
		name: "fuse-overlayfs-x86_64",
		sha256: "56b0ae0aeb8abb308b068af2f137ed8d1bd239f4f27e21672ff0def861eea1e8",
	},
	arm64: {
		name: "fuse-overlayfs-aarch64",
		sha256: "82fed736197b2a881a822e5357b488796f654e8371ce8573a1592331510a0133",
	},
});

if (process.platform !== "linux") {
	throw new Error("The process-reuse backend must be installed from Linux or WSL 2.");
}

const localRoot = path.join(os.homedir(), ".local");
const localBin = path.join(localRoot, "bin");
const sandlock = path.join(localBin, "pi-speculative-sandlock");
const heldExec = path.join(localBin, "pi-speculative-held-exec");
await mkdir(localBin, { recursive: true });
const held = await optional("Actor child handoff", installHeldExec);
const tracing = held && await optional("Running process handoff tracer", installStrace);
const missing = await missingExecutables([...(tracing ? [] : ["strace"]), "git"]);
let producer = false;
if (!held)
	console.warn("Speculative Bash producer unavailable because its transparent exec boundary is unavailable.");
else if (missing.length)
	console.warn(`Speculative Bash producer unavailable (missing ${missing.join(", ")}); cached Actor replay remains usable.`);
else producer = await optional("Speculative Bash producer", installSandlock);
const overlay = await optional("OverlayFS workspace optimization", installFuseOverlayfs);
console.log(
	`Linux reuse setup complete: cached command replay ready; child handoff ${held ? "ready" : "unavailable"}; running process capture ${tracing ? "ready" : "unavailable"}; speculative producer ${producer ? "ready" : "unavailable"}; OverlayFS ${overlay ? "ready" : "unavailable"}.`,
);

async function installSandlock() {
	return installNative(sandlock, sha256(SANDLOCK_REVISION, await readFile(SANDLOCK_PATCH)), qualifySandlock, async (buildRoot, temporary) => {
		const cargo = await executable([path.join(os.homedir(), ".cargo", "bin", "cargo"), "cargo"]).catch(() => {
			throw new Error("Rust stable is required to build Sandlock. Install it from https://rustup.rs and retry.");
		});
		const git = await executable(["git"]), source = path.join(buildRoot, "source"), targetRoot = path.join(buildRoot, "target");
		console.log(`Building Sandlock ${SANDLOCK_REVISION.slice(0, 12)} into ${localRoot} ...`);
		await run(git, ["clone", "--quiet", "--no-checkout", SANDLOCK_REPOSITORY, source]);
		await run(git, ["-C", source, "checkout", "--quiet", "--detach", SANDLOCK_REVISION]);
		await run(git, ["-C", source, "apply", "--whitespace=error-all", SANDLOCK_PATCH]);
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
		// The launcher reaches a broker from inside the sandbox, which answers with a zero exit in place of /bin/false.
		const configuration = path.join(root, "configuration"), broker = net.createServer({ allowHalfOpen: true }, socket => socket.resume().on("end", () => socket.end("x 0\n")));
		await writeFile(configuration, `${path.join(root, "broker")}\ntoken\n`);
		await writeFile(path.join(view, ".pi-spec-dispatch"), `PI_SPEC_DISPATCH\n${configuration}\n/bin\n/bin\n`, { mode: 0o600 });
		await new Promise(resolve => broker.listen(path.join(root, "broker"), resolve));
		try { await run(binary, ["run", "--chroot", "/", "--fs-read", "/", "--fs-write", path.join(root, "broker"), "--exec-mount", `/bin/false:${image}`, "--", "/bin/false"]); }
		finally { broker.close(); }
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

async function installHeldExec() {
	const source = fileURLToPath(new URL("./linux-held-exec.c", import.meta.url));
	return installNative(heldExec, sha256("static-pthread+shared-image+native-startup", await readFile(source)), qualifyHeldExec, async (_root, temporary) => {
		const compiler = await executable(["cc", "gcc", "clang"]);
		await run(compiler, ["-static", "-pthread", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", temporary]);
		await run(compiler, ["-shared", "-fPIC", "-pthread", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", `${temporary}.so`]);
		await copyFile(`${temporary}.so`, `${temporary}.node`, fsConstants.COPYFILE_EXCL);
	}, [".so", ".node", ""]);
}

async function qualifyHeldExec(binary) {
	await run(binary, ["--exec", "21", "pi-exec-probe", binary, "--probe-clean-fds"]);
	await qualifyNativeStartup(binary);
}

async function installStrace() {
	if (process.arch !== "x64") throw new Error("running process capture requires x86-64 Linux");
	const patch = await readFile(STRACE_PATCH), sourceDigest = sha256(STRACE_SHA256, patch);
	const qualify = async binary => {
		await run(binary, ["--kill-on-exit", "-f", "-q", "-e", "trace=none", "-o", "/dev/null",
			`--handoff-admission-library=${heldExec}.so`, "--handoff-image=/dev/null", "--", "/bin/true"]);
	};
	return installNative(path.join(localBin, "pi-speculative-strace"), sourceDigest, qualify, async (buildRoot, temporary) => {
		const git = await executable(["git"]), tar = await executable(["tar"]), make = await executable(["make"]);
		const archive = path.join(buildRoot, "source.tar.xz"), source = path.join(buildRoot, `strace-${STRACE_RELEASE}`);
		await writeFile(archive, await downloadPinned(`https://strace.io/files/${STRACE_RELEASE}/strace-${STRACE_RELEASE}.tar.xz`, STRACE_SHA256));
		await run(tar, ["-xJf", archive, "-C", buildRoot]);
		await run(git, ["-C", source, "apply", "--whitespace=error-all", STRACE_PATCH]);
		await run(path.join(source, "configure"), ["--enable-mpers=no", "--without-libunwind", "--without-libdw"], 0, source);
		await run(make, ["-j", String(Math.min(4, os.availableParallelism()))], 0, source);
		await copyFile(path.join(source, "src", "strace"), temporary, fsConstants.COPYFILE_EXCL);
	});
}

async function installFuseOverlayfs() {
	const target = path.join(localBin, "fuse-overlayfs");
	const asset = FUSE_OVERLAYFS_ASSETS[process.arch];
	if (!asset) throw new Error(`no pinned fuse-overlayfs asset for ${process.arch}`);
	return installNative(target, asset.sha256, binary => run(binary, ["--version"]), async (_root, temporary) => {
		await access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK);
		await executable(["fusermount3", "fusermount"]);
		const url = `https://github.com/containers/fuse-overlayfs/releases/download/${FUSE_OVERLAYFS_RELEASE}/${asset.name}`;
		console.log(`Installing fuse-overlayfs ${FUSE_OVERLAYFS_RELEASE} into ${localRoot} ...`);
		await writeFile(temporary, await downloadPinned(url, asset.sha256), { mode: 0o700, flag: "wx" });
	}, [""], true);
}

/** Qualify a complete artifact set before replacement; publish the executable after its libraries. */
async function installNative(target, sourceDigest, qualify, build, suffixes = [""], pinned = false) {
	const stamp = `${target}.sha256`, digest = async binary => sha256(...await Promise.all(suffixes.map(suffix => readFile(binary + suffix))));
	try {
		const current = await digest(target);
		if (pinned ? current !== sourceDigest : (await readFile(stamp, "utf8")).trim() !== `${sourceDigest}:${current}`) throw new Error("installation changed");
		await qualify(target); console.log(`${path.basename(target)} ready: ${target}`);
		return;
	} catch { /* Rebuild the exact packaged source or download the pinned artifact. */ }
	const root = await mkdtemp(path.join(os.tmpdir(), `${path.basename(target)}-`));
	const temporary = `${target}.${process.pid}.tmp`;
	try {
		await build(root, temporary);
		await chmod(temporary, 0o755);
		await qualify(temporary);
		const installed = await digest(temporary);
		if (pinned && installed !== sourceDigest) throw new Error("artifact changed after qualification");
		for (const suffix of suffixes) await rename(temporary + suffix, target + suffix);
		if (!pinned) await writeFile(stamp, `${sourceDigest}:${installed}\n`, { mode: 0o600 });
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

async function run(command, args, expected = 0, cwd) {
	const [code, signal] = await once(spawn(command, args, { stdio: "inherit", cwd }), "exit");
	if (code !== expected || signal !== null) throw new Error(`${command} failed (${signal ?? code ?? "unknown"})`);
}
