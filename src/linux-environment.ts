import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	BorderedLoader,
	createLocalBashOperations,
	truncateToVisualLines,
} from "@earendil-works/pi-coding-agent";
import { constants as fsConstants } from "node:fs";
import { access, readFile, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";
import { speculativeConfig } from "./config-schema.ts";
import { executionDomainCollector, resolveLinuxExecHelper } from "./linux-held-exec.ts";
import { errorMessage, execHostText, resolveHostExecutable } from "./primitives.ts";
import { qualifyNativeStartup } from "./process-startup.mjs";

export interface LinuxEnvironmentReport {
	readonly text: string;
	readonly summary: string;
	readonly warnings: boolean;
}

/** os-release is data: never source it, expand variables, or evaluate command substitutions. */
export function parseLinuxOsRelease(text: string): Readonly<Record<string, string>> {
	const result: Record<string, string> = Object.create(null);
	for (const line of text.split(/\r?\n/)) {
		const match = /^([A-Z][A-Z0-9_]*)=(.*)$/.exec(line.trim());
		if (!match) continue;
		const [key, raw] = match;
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
	const release = parseLinuxOsRelease(await readFile("/etc/os-release", "utf8").catch(() => ""));
	const bin = path.join(os.homedir(), ".local", "bin");
	const host = `${release.PRETTY_NAME ?? release.ID ?? "Linux"}; ${process.arch}; ${clean(os.release())}`;
	const lines = ["Linux environment Doctor", `Host: ${host}`];
	const issues: string[] = [];
	const check = async <Value>(
		label: string,
		inspect: () => Promise<Value>,
		describe: (value: Value) => string = String,
	) => {
		try {
			const value = await inspect();
			lines.push(`${label}: ${clean(describe(value))}`);
			return value;
		} catch (error) {
			const detail = `${label}: ${clean(errorMessage(error))}`;
			issues.push(detail);
			lines.push(detail);
			return undefined;
		}
	};
	const program = async (
		name: string,
		alternates: readonly string[] = [],
		fallbacks: readonly string[] = [],
		explicit?: string,
		version = false,
	) => {
		// Toolchain shims dispatch by argv[0]; retain the selected symlink when testing launchability.
		const file = await resolveHostExecutable(explicit, name, fallbacks, alternates, version);
		if (!(await stat(file)).isFile()) throw new Error("not a regular executable");
		if (version) await query(file, ["--version"]);
		return file;
	};
	lines.push("Build tools: launchability only; compilation/linking is verified by setup:linux.");
	let compiler: string | undefined;
	for (const [label, name, alternates, fallbacks] of [
		["C compiler", "cc", ["gcc", "clang"], []],
		["Clang BPF compiler", "clang", [], []],
		["Git", "git", [], []],
		["Cargo", "cargo", [], [path.join(os.homedir(), ".cargo/bin/cargo")]],
		["Rust compiler", "rustc", [], [path.join(os.homedir(), ".cargo/bin/rustc")]],
		["make", "make", [], []],
		["tar", "tar", [], []],
		["xz", "xz", [], []],
	] as const) {
		const file = await check(label, () => program(name, alternates, fallbacks, undefined, true));
		if (name === "cc") compiler = file;
	}
	if (compiler)
		for (const [library, header] of [
			["libc.a", ""],
			["libbpf.so", "bpf/libbpf.h"],
			["libelf.so", "libelf.h"],
			["libz.so", "zlib.h"],
		])
			await check(library!, async () => {
				const file = (await query(compiler!, [`-print-file-name=${library}`])).trim();
				if (!path.isAbsolute(file) || !(await stat(file)).isFile())
					throw new Error("development library not located; install its development package");
				if (header) await access(path.join("/usr/include", header), fsConstants.R_OK);
				return `${file}; linking not tested here`;
			});
	await check("cgroup v2", () =>
		access("/sys/fs/cgroup/cgroup.controllers", fsConstants.R_OK).then(() => "present"),
	);
	await check("Kernel BTF", () =>
		access("/sys/kernel/btf/vmlinux", fsConstants.R_OK).then(
			() => "present; hooks checked by the execution domain",
		),
	);
	if (process.arch !== "x64")
		await check("Process architecture", async () => {
			throw new Error("x86-64 required");
		});
	lines.push("Installed components: locating an executable does not qualify its behavior.");
	for (const [label, name, explicit, fallback] of [
		["Sandlock", "pi-speculative-sandlock", process.env.PI_SPEC_SANDLOCK, "pi-speculative-sandlock"],
		["Execution domain launcher", "pi-speculative-domain", undefined, "pi-speculative-domain"],
	] as const)
		await check(label, () => program(name, [], [path.join(bin, fallback)], explicit));
	const helper = await check("Native helper", () => resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC));
	if (helper) {
		const collector = await check(
			"Execution domain",
			() => executionDomainCollector(helper),
			() => "active; protocol and source fingerprint match",
		);
		if (collector)
			await check("Workspace admission", async () => {
				collector.check(root);
				return "available; operation dependencies and effects remain checked at use";
			});
		else lines.push("Workspace admission: untested (execution domain unavailable)");
		await check(
			"Native startup observation",
			() => qualifyNativeStartup(helper),
			() => "qualified; process state and ownership remain checked at use",
		);
	}
	lines.push("Optional OverlayFS: Git workspace fallback remains available.");
	await check("fuse-overlayfs", () =>
		program("fuse-overlayfs", [], [path.join(bin, "fuse-overlayfs")], undefined, true),
	);
	await check("FUSE mount helper", () => program("fusermount3", ["fusermount"], [], undefined, true));
	await check("/dev/fuse", () =>
		access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK).then(
			() => "read/write access; mount qualification remains separate",
		),
	);
	lines.push(
		"WSL requires WSL 2 and a Linux-native checkout. Rebuild helpers after upgrading; start Pi through the foreground execution domain and keep the separately qualified observer.",
		"Use Refresh and show capabilities for enabled provider qualification. Missing evidence declines reuse; ordinary tools still execute. No packages, persistent services or permissions were changed.",
	);
	return {
		text: lines.join("\n"),
		summary: `Host: ${host}\n${issues.length ? issues.join("\n") : "Prerequisite and admission checks passed; individual operations still require proof."}`,
		warnings: issues.length > 0,
	};
}

function query(file: string, args: readonly string[]): Promise<string> {
	return execHostText(file, args, {
		cwd: "/",
		env: {
			...process.env,
			RUSTUP_AUTO_INSTALL: "0",
			CARGO_NET_OFFLINE: "true",
		},
		timeout: speculativeConfig().processReuse.diagnosticTimeoutMs,
		killSignal: "SIGKILL",
		maxBuffer: speculativeConfig().filesystem.diagnosticBytes,
	});
}

function sanitizeDiagnosticText(text: string): string {
	return stripVTControlCharacters(text).replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "");
}

function clean(text: string): string {
	return sanitizeDiagnosticText(text).slice(0, speculativeConfig().processReuse.diagnosticCharacters);
}

function requireIdleLinuxContext(ctx: ExtensionContext, action: string, linuxMessage: string): boolean {
	if (process.platform !== "linux") {
		ctx.ui.notify(linuxMessage, "warning");
		return false;
	}
	if (!ctx.isIdle()) {
		ctx.ui.notify(`Wait for the current task to finish before ${action}.`, "warning");
		return false;
	}
	return true;
}

function ensureStillIdle(ctx: ExtensionContext, message: string): boolean {
	if (ctx.isIdle()) return true;
	ctx.ui.notify(`The task is now running. ${message} after it finishes.`, "warning");
	return false;
}

/** Show host prerequisites separately from the controller's authoritative runtime diagnostics. */
export async function checkLinuxEnvironment(
	ctx: ExtensionContext,
	refresh: () => Promise<void>,
): Promise<void> {
	if (
		!requireIdleLinuxContext(
			ctx,
			"checking the Linux environment",
			"Linux environment checks require Pi running inside Linux or WSL 2. Open this menu there.",
		)
	)
		return;
	const report = await inspectLinuxEnvironment(ctx.cwd);
	ctx.ui.notify(report.text, report.warnings ? "warning" : "info");
	if (!ensureStillIdle(ctx, "Refresh execution diagnostics")) return;
	await refresh();
}

/** The installer and its native sources ship in src for both TypeScript and dist entry points. */
export async function installLinuxDependencies(
	ctx: ExtensionContext,
	refresh: () => Promise<void>,
): Promise<void> {
	if (
		!requireIdleLinuxContext(
			ctx,
			"installing Linux dependencies",
			"Linux dependencies must be installed from Pi running inside Linux or WSL 2. Open this menu there.",
		)
	)
		return;
	const report = await inspectLinuxEnvironment(ctx.cwd);
	if (!ensureStillIdle(ctx, "Retry installation")) return;
	if (
		!(await ctx.ui.confirm(
			"Install / update Linux dependencies?",
			`${report.summary}\n\nDownload and build the packaged Sandlock and process helpers, plus optional fuse-overlayfs, in ~/.local/bin. ` +
				"Requires Git, C/Rust toolchains, Clang, libbpf/libelf/zlib development files, make, tar and xz; FUSE needs fusermount and /dev/fuse. " +
				"Missing prerequisites may leave individual components unavailable. System packages and permissions are not changed. Building may take several minutes.",
		))
	)
		return;
	if (!ensureStillIdle(ctx, "Retry installation")) return;
	const script = fileURLToPath(new URL("../src/setup-linux-process-backend.mjs", import.meta.url));
	const quote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
	const result = await ctx.ui.custom<{
		output: string;
		failed: boolean;
		cancelled: boolean;
	}>((tui, theme, _keys, done) => {
		const loader = new BorderedLoader(tui, theme, "Installing Linux dependencies…");
		const lifetime = new AbortController();
		const decoder = new StringDecoder("utf8");
		const signal = AbortSignal.any([loader.signal, lifetime.signal]);
		let output = "";
		const append = (text: string) => {
			output = (output + sanitizeDiagnosticText(text.replace(/\r/g, "\n"))).slice(-16384);
			tui.requestRender();
		};
		loader.addChild({
			render: (width) => truncateToVisualLines(output, 10, width).visualLines,
			invalidate() {},
		});
		void createLocalBashOperations({ shellPath: "/bin/sh" })
			.exec(`exec ${quote(process.execPath)} ${quote(script)}`, path.dirname(script), {
				signal,
				onData: (data) => append(decoder.write(data)),
			})
			.then(
				({ exitCode }) => (exitCode === 0 ? undefined : `Installer exited with ${exitCode ?? "a signal"}.`),
				errorMessage,
			)
			.then((failure) => {
				append(decoder.end());
				if (failure !== undefined) append(`\n${failure}\n`);
				done({ output, failed: failure !== undefined, cancelled: signal.aborted });
			});
		return {
			render: (width) => loader.render(width),
			invalidate: () => loader.invalidate(),
			handleInput: (data) => loader.handleInput(data),
			dispose() {
				lifetime.abort();
				loader.dispose();
			},
		};
	});
	const partial = /\bunavailable\b/i.test(result.output);
	const status = result.cancelled
		? "Installation cancelled; completed components remain installed."
		: result.failed
			? "Linux dependency installation failed."
			: partial
				? "Installation finished with unavailable components."
				: "Linux dependency installation finished.";
	const detail = result.output.trim().split("\n").slice(-14).join("\n");
	ctx.ui.notify(
		`${status}\n${detail}\nInstaller output describes installation checks; runtime qualification is reported separately. Restart Pi to load updated helpers and recheck workspace drivers.`,
		result.failed && !result.cancelled ? "error" : result.cancelled || partial ? "warning" : "info",
	);
	await refresh();
}
