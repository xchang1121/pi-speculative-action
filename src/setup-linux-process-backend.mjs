#!/usr/bin/env node

import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const SANDLOCK_REVISION = "f6a3e39b31afa80f66609c8af8ae5b2582f628e8";
const SANDLOCK_REPOSITORY = "https://github.com/multikernel/sandlock.git";
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
const sandlock = path.join(localBin, "sandlock");
await mkdir(localBin, { recursive: true });
await installHeldExec().catch((error) => {
	console.warn(`Held-exec Actor reuse unavailable; completed whole-command replay remains active: ${error.message}`);
});
const missing = [];
for (const command of ["strace", "unshare", "git"]) {
	try {
		await executable([command]);
	} catch {
		missing.push(command);
	}
}
if (missing.length) {
	throw new Error(
		`Missing Linux dependencies: ${missing.join(", ")}. On Ubuntu/WSL run: sudo apt-get install strace util-linux git`,
	);
}

let ready = false;
try {
	await run(sandlock, ["check"]);
	ready = true;
} catch {
	// Install the pinned source revision below.
}
if (!ready) {
	let cargo;
	try {
		cargo = await executable([path.join(os.homedir(), ".cargo", "bin", "cargo"), "cargo"]);
	} catch {
		throw new Error("Rust stable is required to build Sandlock. Install it from https://rustup.rs and retry.");
	}
	console.log(`Building Sandlock ${SANDLOCK_REVISION.slice(0, 12)} into ${localRoot} ...`);
	await run(cargo, [
		"install",
		"--locked",
		"--git",
		SANDLOCK_REPOSITORY,
		"--rev",
		SANDLOCK_REVISION,
		"--root",
		localRoot,
		"sandlock-cli",
	]);
}
await run(sandlock, ["check"]);
console.log(`Linux process-reuse backend ready: ${sandlock}`);
await installFuseOverlayfs().catch((error) => {
	console.warn(`OverlayFS optimization unavailable; the safe Git workspace driver remains active: ${error.message}`);
});

async function installHeldExec() {
	const source = fileURLToPath(new URL("./linux-held-exec.c", import.meta.url));
	const content = await readFile(source);
	const digest = createHash("sha256").update(content).digest("hex");
	const target = path.join(localBin, "pi-speculative-held-exec");
	const stamp = `${target}.sha256`;
	try {
		if ((await readFile(stamp, "utf8")).trim() !== digest) throw new Error("source changed");
		await run(target, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"], 42);
		console.log(`Held-exec Actor boundary ready: ${target}`);
		return;
	} catch {
		// Build and functionally qualify the exact packaged source below.
	}
	const compiler = await executable(["cc", "gcc", "clang"]);
	const temporary = `${target}.${process.pid}.tmp`;
	try {
		await run(compiler, ["-O2", "-std=c11", "-Wall", "-Wextra", "-Werror", source, "-o", temporary]);
		await chmod(temporary, 0o755);
		await run(temporary, ["--skip-code", "42", "/bin/sh", "-c", "exec /bin/true"], 42);
		await rename(temporary, target);
		await writeFile(stamp, `${digest}\n`, { mode: 0o600 });
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
		const digest = createHash("sha256").update(installed).digest("hex");
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
	const digest = createHash("sha256").update(bytes).digest("hex");
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
