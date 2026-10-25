#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Runs under Linux/WSL with an existing MinGW toolchain; no package or system changes.
if (process.platform !== "linux") throw new Error("Build the Windows provider from Linux or WSL with MinGW-w64.");
const toolchain = process.env.PI_SPEC_WINDOWS_TOOLCHAIN ?? "/usr";
const compiler = path.join(toolchain, "bin/x86_64-w64-mingw32-gcc-posix"), dlltool = path.join(toolchain, "bin/x86_64-w64-mingw32-dlltool");
const source = fileURLToPath(new URL("./windows-filesystem.c", import.meta.url)), sourceBytes = await readFile(source);
const destination = fileURLToPath(new URL("../vendor/", import.meta.url)), temporary = await mkdtemp(path.join(os.tmpdir(), "pi-windows-filesystem-"));
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const flags = ["-std=c11", "-shared", "-O2", "-s", "-Wall", "-Wextra", "-Werror", "-static-libgcc", "-Wl,--no-insert-timestamp", "-Wl,--image-base,0x180000000"];
function run(command, args) {
	const result = spawnSync(command, args, { encoding: "utf8", env: { ...process.env, PATH: `${path.join(toolchain, "bin")}:${process.env.PATH ?? ""}` } });
	if (result.error) throw result.error;
	if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr || result.stdout}`);
	return result.stdout.trim();
}
try {
	const symbols = [...new Set(sourceBytes.toString().match(/\bnapi_\w+(?=\s*\()/g))].sort();
	const definition = path.join(temporary, "node.def"), archive = path.join(temporary, "libnode.a"), binary = path.join(temporary, "filesystem.node");
	await writeFile(definition, `LIBRARY node.exe\nEXPORTS\n${symbols.join("\n")}\n`);
	run(dlltool, ["-d", definition, "-l", archive]);
	run(compiler, [...flags, "-I", path.join(toolchain, "include/node"), source, archive, "-o", binary]);
	const manifest = { platform: "win32", architecture: "x64", napi: 8, sourceSha256: digest(sourceBytes), binarySha256: digest(await readFile(binary)),
		compiler: run(compiler, ["--version"]).split("\n")[0], flags, imports: symbols };
	await mkdir(destination, { recursive: true });
	// Stage on the destination filesystem so publication does not depend on cross-device rename.
	const pending = path.join(destination, `filesystem-win32-x64.${process.pid}.tmp`);
	const file = await open(pending, "wx");
	try {
		try { await file.writeFile(await readFile(binary)); } finally { await file.close(); }
		await rename(pending, path.join(destination, "filesystem-win32-x64.node"));
	}
	finally { await rm(pending, { force: true }); }
	await writeFile(path.join(destination, "filesystem-win32-x64.json"), `${JSON.stringify(manifest, null, 2)}\n`);
	console.log(JSON.stringify(manifest));
} finally { await rm(temporary, { recursive: true, force: true }); }
