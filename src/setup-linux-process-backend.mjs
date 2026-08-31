#!/usr/bin/env node

import { access, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";

const SANDLOCK_REVISION = "f6a3e39b31afa80f66609c8af8ae5b2582f628e8";
const SANDLOCK_REPOSITORY = "https://github.com/multikernel/sandlock.git";

if (process.platform !== "linux") {
	throw new Error("The process-reuse backend must be installed from Linux or WSL 2.");
}

const localRoot = path.join(os.homedir(), ".local");
const localBin = path.join(localRoot, "bin");
const sandlock = path.join(localBin, "sandlock");
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

await mkdir(localBin, { recursive: true });
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

function run(command, args) {
	return new Promise((resolve, reject) => {
		const child = spawn(command, args, { stdio: "inherit" });
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === 0) resolve();
			else reject(new Error(`${command} failed (${signal ?? code ?? "unknown"})`));
		});
	});
}
