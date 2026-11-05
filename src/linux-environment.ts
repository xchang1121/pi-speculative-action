import { constants as fsConstants } from "node:fs";
import { access, readFile, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { stripVTControlCharacters } from "node:util";
import { errorMessage } from "./error-utils.ts";
import { execHostText, resolveHostExecutable } from "./executable-path.ts";
import { executionDomainCollector, resolveLinuxExecHelper } from "./linux-held-exec.ts";
import { qualifyNativeStartup } from "./process-startup.mjs";

export interface LinuxEnvironmentReport { readonly text: string; readonly summary: string; readonly warnings: boolean; }

/** os-release is data: never source it, expand variables, or evaluate command substitutions. */
export function parseLinuxOsRelease(text: string): Readonly<Record<string, string>> {
	const result: Record<string, string> = Object.create(null);
	for (const line of text.split(/\r?\n/)) {
		const match = /^([A-Z][A-Z0-9_]*)=(.*)$/.exec(line.trim());
		if (!match) continue;
		const [, key, raw] = match;
		let value = raw!;
		if (value.startsWith('"') || value.startsWith("'")) {
			const quote = value[0]!;
			if (!value.endsWith(quote) || value.length < 2) continue;
			value = value.slice(1, -1);
			if (quote === '"') value = value.replace(/\\(["\\$`])/g, "$1");
		} else if (/\s|["']/.test(value)) continue;
		result[key!] = clean(value);
	}
	return result;
}

/** Inventory plus the runtime's own bounded admission/observer probes. Every temporary owner is closed. */
export async function inspectLinuxEnvironment(root = process.cwd()): Promise<LinuxEnvironmentReport> {
	if (process.platform !== "linux") {
		const text = "Linux environment checks require Pi running inside Linux or WSL 2.";
		return { text, summary: text, warnings: true };
	}
	const release = parseLinuxOsRelease(await readFile("/etc/os-release", "utf8").catch(() => "")), bin = path.join(os.homedir(), ".local", "bin");
	const host = `${release.PRETTY_NAME ?? release.ID ?? "Linux"}; ${process.arch}; ${clean(os.release())}`;
	const lines = ["Linux environment Doctor", `Host: ${host}`], issues: string[] = [];
	const check = async <Value>(label: string, inspect: () => Promise<Value>, describe: (value: Value) => string = String) => {
		try { const value = await inspect(); lines.push(`${label}: ${clean(describe(value))}`); return value; }
		catch (error) { const detail = `${label}: ${clean(errorMessage(error))}`; issues.push(detail); lines.push(detail); return undefined; }
	};
	const program = async (name: string, alternates: readonly string[] = [], fallbacks: readonly string[] = [], explicit?: string, version = false) => {
		// Toolchain shims dispatch by argv[0]; retain the selected symlink when testing launchability.
		const file = await resolveHostExecutable(explicit, name, fallbacks, alternates, version);
		if (!(await stat(file)).isFile()) throw new Error("not a regular executable");
		if (version) await query(file, ["--version"]);
		return file;
	};
	lines.push("Build tools: launchability only; compilation/linking is verified by setup:linux.");
	let compiler: string | undefined;
	for (const [label, name, alternates, fallbacks] of [
		["C compiler", "cc", ["gcc", "clang"], []], ["Clang BPF compiler", "clang", [], []], ["Git", "git", [], []],
		["Cargo", "cargo", [], [path.join(os.homedir(), ".cargo/bin/cargo")]], ["Rust compiler", "rustc", [], [path.join(os.homedir(), ".cargo/bin/rustc")]],
		["make", "make", [], []], ["tar", "tar", [], []], ["xz", "xz", [], []],
	] as const) {
		const file = await check(label, () => program(name, alternates, fallbacks, undefined, true));
		if (name === "cc") compiler = file;
	}
	if (compiler) for (const [library, header] of [["libc.a", ""], ["libbpf.so", "bpf/libbpf.h"], ["libelf.so", "libelf.h"], ["libz.so", "zlib.h"]])
		await check(library!, async () => {
			const file = (await query(compiler!, [`-print-file-name=${library}`])).trim();
			if (!path.isAbsolute(file) || !(await stat(file)).isFile()) throw new Error("development library not located; install its development package");
			if (header) await access(path.join("/usr/include", header), fsConstants.R_OK);
			return `${file}; linking not tested here`;
		});
	await check("cgroup v2", () => access("/sys/fs/cgroup/cgroup.controllers", fsConstants.R_OK).then(() => "present"));
	await check("Kernel BTF", () => access("/sys/kernel/btf/vmlinux", fsConstants.R_OK).then(() => "present; hooks checked by the execution domain"));
	if (process.arch !== "x64") await check("Process architecture", async () => { throw new Error("x86-64 required"); });
	lines.push("Installed components: locating an executable does not qualify its behavior.");
	for (const [label, name, explicit, fallback] of [["Sandlock", "pi-speculative-sandlock", process.env.PI_SPEC_SANDLOCK, "pi-speculative-sandlock"],
		["Process tracer", "strace", process.env.PI_SPEC_STRACE, "pi-speculative-strace"], ["Execution domain launcher", "pi-speculative-domain", undefined, "pi-speculative-domain"]] as const)
		await check(label, () => program(name, [], [path.join(bin, fallback)], explicit));
	const helper = await check("Native helper", () => resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC));
	if (helper) {
		const collector = await check("Execution domain", () => executionDomainCollector(helper), () => "active; protocol and source fingerprint match");
		if (collector) await check("Workspace admission", async () => {
			collector.check(root); return "available; operation dependencies and effects remain checked at use";
		});
		else lines.push("Workspace admission: untested (execution domain unavailable)");
		await check("Native startup observation", () => qualifyNativeStartup(helper), () => "qualified; process state and ownership remain checked at use");
	}
	lines.push("Optional OverlayFS: Git workspace fallback remains available.");
	await check("fuse-overlayfs", () => program("fuse-overlayfs", [], [path.join(bin, "fuse-overlayfs")], undefined, true));
	await check("FUSE mount helper", () => program("fusermount3", ["fusermount"], [], undefined, true));
	await check("/dev/fuse", () => access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK).then(() => "read/write access; mount qualification remains separate"));
	lines.push("WSL requires WSL 2 and a Linux-native checkout. Rebuild helpers after upgrading; start Pi through the foreground execution domain and keep the separately qualified observer.",
		"Use Refresh and show capabilities for enabled provider qualification. Missing evidence declines reuse; ordinary tools still execute. No packages, persistent services or permissions were changed.");
	return { text: lines.join("\n"), summary: `Host: ${host}\n${issues.length ? issues.join("\n") : "Prerequisite and admission checks passed; individual operations still require proof."}`, warnings: issues.length > 0 };
}

function query(file: string, args: readonly string[]): Promise<string> {
	return execHostText(file, args, { cwd: "/", env: { ...process.env, RUSTUP_AUTO_INSTALL: "0", CARGO_NET_OFFLINE: "true" },
		timeout: 2_000, killSignal: "SIGKILL", maxBuffer: 16_384 });
}
function clean(text: string): string { return stripVTControlCharacters(text).replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "").slice(0, 320); }
