#!/usr/bin/env node

import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const SANDLOCK_REVISION = "f6a3e39b31afa80f66609c8af8ae5b2582f628e8";
const SANDLOCK_REPOSITORY = "https://github.com/multikernel/sandlock.git";
const SANDLOCK_PATCH = fileURLToPath(new URL("./sandlock-transparent-exec.patch", import.meta.url));
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
const missing = await missingExecutables(["strace", "git"]);
let producer = false;
if (!held)
	console.warn("Speculative Bash producer unavailable because its transparent exec boundary is unavailable.");
else if (missing.length)
	console.warn(`Speculative Bash producer unavailable (missing ${missing.join(", ")}); cached Actor replay remains usable.`);
else producer = await optional("Speculative Bash producer", installSandlock);
const overlay = await optional("OverlayFS workspace optimization", installFuseOverlayfs);
console.log(
	`Linux reuse setup complete: cached command replay ready; child handoff ${held ? "ready" : "unavailable"}; speculative producer ${producer ? "ready" : "unavailable"}; OverlayFS ${overlay ? "ready" : "unavailable"}.`,
);

async function installSandlock() {
	const patch = await readFile(SANDLOCK_PATCH);
	const sourceDigest = sha256(SANDLOCK_REVISION, patch);
	const stamp = `${sandlock}.sha256`;
	let installedMatches = false;
	try {
		const [installedStamp, installed] = await Promise.all([readFile(stamp, "utf8"), readFile(sandlock)]);
		installedMatches = installedStamp.trim() === `${sourceDigest}:${sha256(installed)}`;
	} catch {
		// Install the pinned source revision below.
	}
	if (installedMatches) {
		await qualifySandlock(sandlock);
		console.log(`Speculative Bash producer ready: ${sandlock}`);
		return;
	}
	const cargo = await executable([path.join(os.homedir(), ".cargo", "bin", "cargo"), "cargo"]).catch(() => {
		throw new Error("Rust stable is required to build Sandlock. Install it from https://rustup.rs and retry.");
	});
	const git = await executable(["git"]);
	const buildRoot = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-sandlock-"));
	const source = path.join(buildRoot, "source");
	const targetRoot = path.join(buildRoot, "target");
	const temporary = `${sandlock}.${process.pid}.tmp`;
	console.log(`Building Sandlock ${SANDLOCK_REVISION.slice(0, 12)} into ${localRoot} ...`);
	try {
		await run(git, ["clone", "--quiet", "--no-checkout", SANDLOCK_REPOSITORY, source]);
		await run(git, ["-C", source, "checkout", "--quiet", "--detach", SANDLOCK_REVISION]);
		await run(git, ["-C", source, "apply", "--whitespace=error-all", SANDLOCK_PATCH]);
		await run(cargo, [
			"build", "--locked", "--release", "-p", "sandlock-cli",
			"--manifest-path", path.join(source, "Cargo.toml"), "--target-dir", targetRoot,
		]);
		await copyFile(path.join(targetRoot, "release", "sandlock"), temporary, fsConstants.COPYFILE_EXCL);
		await chmod(temporary, 0o755);
		await qualifySandlock(temporary);
		await rename(temporary, sandlock);
		await writeFile(stamp, `${sourceDigest}:${sha256(await readFile(sandlock))}\n`, { mode: 0o600 });
	} finally {
		await Promise.all([
			rm(buildRoot, { recursive: true, force: true }).catch(() => undefined),
			rm(temporary, { force: true }).catch(() => undefined),
		]);
	}
	console.log(`Speculative Bash producer ready: ${sandlock}`);
}

async function qualifySandlock(binary) {
	await run(binary, ["check"]);
	await run(binary, [
		"run", "--chroot", "/", "--fs-read", "/", "--exec-mount", "/bin/false:/bin/true", "--", "/bin/false",
	]);
	await run(binary, [
		"run", "--chroot", "/", "--fs-read", "/", "--", heldExec, "--probe-clean-fds",
	]);
	await run(binary, [
		"run", "--chroot", "/", "--fs-read", "/", "--", process.execPath, "-e",
		"const fs=require('node:fs');try{fs.openSync('/pi-speculative-action/not/present','r');process.exit(65)}catch(e){process.exit(e.code==='ENOENT'?0:66)}",
	]);
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-speculative-dispatch-"));
	try {
		const view = path.join(root, "view");
		const image = path.join(view, "false");
		await mkdir(view);
		await copyFile(heldExec, image);
		await chmod(image, 0o755);
		await writeFile(
			path.join(view, ".pi-spec-dispatch-v1"),
			["PI_SPEC_DISPATCH_V1", "/bin/true", "/bin/true", "/dev/null", "/bin", "/bin", ""].join("\n"),
			{ mode: 0o600 },
		);
		await run(binary, ["run", "--chroot", "/", "--fs-read", "/", "--exec-mount", `/bin/false:${image}`, "--", "/bin/false"]);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

async function installHeldExec() {
	const source = fileURLToPath(new URL("./linux-held-exec.c", import.meta.url));
	const content = await readFile(source);
	const sourceDigest = sha256("static-v1", content);
	const target = heldExec;
	const stamp = `${target}.sha256`;
	try {
		const [installedStamp, installed] = await Promise.all([readFile(stamp, "utf8"), readFile(target)]);
		if (installedStamp.trim() !== `${sourceDigest}:${sha256(installed)}`) throw new Error("installation changed");
		await run(target, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"], 42);
		console.log(`Held-exec Actor boundary ready: ${target}`);
		return;
	} catch {
		// Build and functionally qualify the exact packaged source below.
	}
	const compiler = await executable(["cc", "gcc", "clang"]);
	const temporary = `${target}.${process.pid}.tmp`;
	try {
		await run(compiler, ["-static", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", temporary]);
		await chmod(temporary, 0o755);
		await run(temporary, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"], 42);
		await rename(temporary, target);
		await writeFile(stamp, `${sourceDigest}:${sha256(await readFile(target))}\n`, { mode: 0o600 });
	} finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
	console.log(`Held-exec Actor boundary ready: ${target}`);
}

async function installFuseOverlayfs() {
	const target = path.join(localBin, "fuse-overlayfs");
	const asset = FUSE_OVERLAYFS_ASSETS[process.arch];
	if (!asset) throw new Error(`no pinned fuse-overlayfs asset for ${process.arch}`);
	try {
		const installed = await readFile(target);
		const digest = sha256(installed);
		if (digest !== asset.sha256) throw new Error(`installed fuse-overlayfs checksum mismatch: ${digest}`);
		await run(target, ["--version"]);
		console.log(`OverlayFS workspace driver ready: ${target}`);
		return;
	} catch {
		// Install a hash-pinned official static release below.
	}
	await access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK);
	await executable(["fusermount3", "fusermount"]);
	const url = `https://github.com/containers/fuse-overlayfs/releases/download/${FUSE_OVERLAYFS_RELEASE}/${asset.name}`;
	console.log(`Installing fuse-overlayfs ${FUSE_OVERLAYFS_RELEASE} into ${localRoot} ...`);
	const response = await fetch(url, { redirect: "follow" });
	if (!response.ok) throw new Error(`download failed (${response.status} ${response.statusText})`);
	const bytes = Buffer.from(await response.arrayBuffer());
	const digest = sha256(bytes);
	if (digest !== asset.sha256) throw new Error(`fuse-overlayfs checksum mismatch: ${digest}`);
	const temporary = `${target}.${process.pid}.tmp`;
	try {
		await writeFile(temporary, bytes, { mode: 0o700, flag: "wx" });
		await chmod(temporary, 0o755);
		await rename(temporary, target);
	} finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
	await run(target, ["--version"]);
	console.log(`OverlayFS workspace driver ready: ${target}`);
}

async function executable(candidates) {
	for (const candidate of candidates) {
		if (candidate.includes(path.sep)) {
			try {
				await access(candidate);
				return candidate;
			} catch {
				continue;
			}
		}
		for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
			if (!directory) continue;
			const target = path.join(directory, candidate);
			try {
				await access(target);
				return target;
			} catch {
				// Continue PATH lookup.
			}
		}
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

function run(command, args, expected = 0) {
	return new Promise((resolve, reject) => {
		const child = spawn(command, args, { stdio: "inherit" });
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === expected && signal === null) resolve();
			else reject(new Error(`${command} failed (${signal ?? code ?? "unknown"})`));
		});
	});
}
