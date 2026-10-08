import { execFile } from "node:child_process";
import { constants as fsConstants } from "node:fs";
import { access, readFile, realpath, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { stripVTControlCharacters } from "node:util";
import { errorMessage } from "./error-utils.ts";
import { resolveHostExecutable } from "./executable-path.ts";

export interface LinuxEnvironmentReport { readonly text: string; readonly summary: string; readonly warnings: boolean; }

interface ExecutableCheck {
	readonly label: string;
	readonly state: "missing" | "blocked" | "executable" | "runnable";
	readonly file?: string;
	readonly detail: string;
}

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

/** Read-only host inventory. Only bounded version/archive-location queries execute here; no builds or mounts. */
export async function inspectLinuxEnvironment(): Promise<LinuxEnvironmentReport> {
	if (process.platform !== "linux") {
		const text = "Linux environment checks require Pi running inside Linux or WSL 2.";
		return { text, summary: text, warnings: true };
	}
	const release = parseLinuxOsRelease(await textFile("/etc/os-release") ?? await textFile("/usr/lib/os-release") ?? "");
	const kernel = clean(os.release()), home = os.homedir(), localBin = path.join(home, ".local", "bin");
	const tools: ExecutableCheck[] = [];
	// Run these tiny queries serially so the Doctor does not compete with the Actor for CPU.
	for (const [label, name, alternates, fallbacks] of [
		["Git", "git", [], []], ["C compiler", "cc", ["gcc", "clang"], []],
		["Cargo", "cargo", [], [path.join(home, ".cargo", "bin", "cargo")]],
		["Rust compiler", "rustc", [], [path.join(home, ".cargo", "bin", "rustc")]],
		["make", "make", [], []], ["tar", "tar", [], []], ["xz (strace archive)", "xz", [], []],
	] as const) tools.push(await executableCheck(label, name, { alternates, fallbacks, version: true }));
	const components = [
		await executableCheck("Sandlock", "pi-speculative-sandlock", {
			explicit: process.env.PI_SPEC_SANDLOCK, fallbacks: [path.join(localBin, "pi-speculative-sandlock")],
		}),
		await executableCheck("Process tracer", "strace", {
			explicit: process.env.PI_SPEC_STRACE, fallbacks: [path.join(localBin, "pi-speculative-strace")],
		}),
		await executableCheck("Held-exec", "pi-speculative-held-exec", {
			strictPath: process.env.PI_SPEC_HELD_EXEC ?? path.join(localBin, "pi-speculative-held-exec"),
		}),
		await executableCheck("fuse-overlayfs", "fuse-overlayfs", { fallbacks: [path.join(localBin, "fuse-overlayfs")], version: true }),
		await executableCheck("FUSE mount helper", "fusermount3", { alternates: ["fusermount"], version: true }),
	];
	const compiler = tools.find(tool => tool.label === "C compiler");
	let staticLibc = "not checked (C compiler unavailable)", staticLibcLocated = false;
	if (compiler?.state === "runnable") {
		try {
			const archive = (await query(compiler.file!, ["-print-file-name=libc.a"])).trim();
			if (!path.isAbsolute(archive) || !(await stat(archive)).isFile()) throw new Error("compiler did not locate libc.a");
			await access(archive, fsConstants.R_OK);
			staticLibcLocated = true;
			staticLibc = `archive found: ${clean(archive)}; static pthread linking is verified during installation`;
		} catch { staticLibc = "not located; obtain static libc/development files for this compiler target before building held-exec"; }
	}
	const [fuse, status, ptrace, userns, maxUserns, lsm] = await Promise.all([
		access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK).then(() => "read/write access", () => "missing or inaccessible"),
		textFile("/proc/self/status"), textFile("/proc/sys/kernel/yama/ptrace_scope"),
		textFile("/proc/sys/kernel/unprivileged_userns_clone"), textFile("/proc/sys/user/max_user_namespaces"),
		textFile("/sys/kernel/security/lsm"),
	]);
	const held = components.find(component => component.label === "Held-exec");
	const library = held?.file ? await access(`${held.file}.so`, fsConstants.R_OK).then(() => "present", () => "missing or unreadable") : "not located";
	const container = (await textFile("/run/systemd/container") ?? process.env.container)?.trim();
	const wsl = /microsoft|wsl/i.test(kernel) || Boolean(process.env.WSL_INTEROP || process.env.WSL_DISTRO_NAME);
	const issues = tools.filter(tool => tool.state !== "runnable");
	const unavailable = components.filter(component => component.state === "missing" || component.state === "blocked");
	const tracingRestricted = ["2", "3"].includes(scalar(ptrace)) || Number(statusField(status, "TracerPid")) > 0;
	const distribution = release.PRETTY_NAME ?? release.NAME ?? release.ID ?? "unknown distribution";
	const lines = [
		"Linux environment Doctor",
		`Host: ${distribution} (ID=${release.ID ?? "unknown"}, version=${release.VERSION_ID ?? "unknown"}); Node arch=${process.arch}; kernel=${kernel}; machine=${clean(os.machine())}`,
		`Environment: ${wsl ? "WSL detected; use WSL 2 and a Linux-native checkout" : "Linux"}${container ? `; container=${clean(container)}` : ""}`,
		"Build prerequisites (a version query checks launchability, not compiler/linker qualification):",
		...tools.map(formatExecutable),
		`Static libc: ${staticLibc}`,
		"Installed components (permission/version checks do not establish runtime qualification):",
		...components.map(formatExecutable),
		`Held-exec shared library: ${library}`,
		`/dev/fuse: ${fuse}; OverlayFS also requires a working fusermount and full mount/copy-up/whiteout/unmount probe.`,
		`Architecture: running process capture ${process.arch === "x64" ? "has an x86-64 target; runtime probe still required" : "requires x86-64; unavailable on this Node architecture"}; pinned fuse-overlayfs download ${["x64", "arm64"].includes(process.arch) ? "exists for this architecture" : "is unavailable for this architecture"}.`,
		`Kernel policy clues: ptrace_scope=${scalar(ptrace)}; Seccomp=${statusField(status, "Seccomp")}; NoNewPrivs=${statusField(status, "NoNewPrivs")}; TracerPid=${statusField(status, "TracerPid")}.`,
		`User namespaces: unprivileged_userns_clone=${scalar(userns)}; max_user_namespaces=${scalar(maxUserns)}; active LSMs=${scalar(lsm)}.`,
		"Unreadable policy files mean unknown. Kernel versions and these clues do not prove Landlock/seccomp/ptrace support; runtime behavior probes decide qualification.",
	];
	if (issues.length) lines.push(`Action: prepare ${issues.map(tool => tool.label).join(", ")} before rebuilding affected components; fix non-launchable toolchains or PATH first.`);
	if (tools.some(tool => ["Cargo", "Rust compiler"].includes(tool.label) && tool.state !== "runnable")) {
		lines.push("Rust: install/configure a stable toolchain using https://rustup.rs, then reopen Pi so its environment can find Cargo and rustc.");
	}
	if (release.ID?.toLowerCase() === "openeuler" || release.ID_LIKE?.toLowerCase().split(/\s+/).includes("openeuler")) {
		lines.push("openEuler: ask the administrator to look up missing providers in repositories for this release/architecture, then install the selected packages. Suggested lookups (not executed): dnf provides '*/gcc', dnf provides '*/libc.a', dnf provides '*/fusermount3'; use dnf search for Git, make, tar, xz and Rust.",
			"openEuler package guidance: https://docs.openeuler.org/en/docs/25.03/server/administration/administrator/using-dnf-to-manage-software-packages.html ; provider lookup: https://dnf.readthedocs.io/en/stable/command_ref.html#provides-command");
	} else if (issues.length || !staticLibcLocated) {
		lines.push("Use this distribution's package manager to locate the missing programs and static libc/development files; package names vary by release and architecture.");
	}
	if (fuse !== "read/write access" || components.at(-1)?.state !== "runnable") {
		lines.push("FUSE action: ask the host/container administrator to provide fusermount and read/write access to /dev/fuse, then rerun this check. Git workspace fallback remains available.");
	}
	if (tracingRestricted || container) {
		lines.push("Process action: host tracing/container policy may restrict ptrace or seccomp. Ask the administrator to review the runtime probe failure; this check does not change security policy.");
	}
	lines.push("Next: install/update missing helpers, then refresh execution diagnostics. Only qualified enabled runtime providers can be used; disabled providers remain untested. No system packages, permissions or settings were changed.");
	// Pi's confirmation message is a selector title, so keep paths and the full inventory in the Doctor report.
	const summary = [`Host: ${distribution.slice(0, 64)}; ${process.arch}`,
		`Build tools: ${issues.length ? `needs attention: ${issues.map(tool => tool.label).join(", ")}` : "version checks passed"}.`,
		`Static libc: ${staticLibcLocated ? "archive found; linking not tested" : "not located; development files may be needed"}.`,
		`Missing/blocked helpers: ${unavailable.map(component => component.label).join(", ") || "none"}; shared library ${library}.`,
		`/dev/fuse: ${fuse}.${tracingRestricted ? " Tracing policy may restrict reuse." : ""}`,
		...(process.arch !== "x64" ? ["Running process capture requires x86-64."] : []),
		"Check Linux environment shows guidance and runtime qualification separately.",
	].join("\n");
	return { text: lines.join("\n"), summary, warnings: Boolean(issues.length || !staticLibcLocated || library !== "present" ||
		unavailable.length || fuse !== "read/write access" || process.arch !== "x64" || tracingRestricted) };
}

async function executableCheck(label: string, name: string, options: {
	readonly explicit?: string; readonly fallbacks?: readonly string[]; readonly alternates?: readonly string[];
	readonly strictPath?: string; readonly version?: boolean;
} = {}): Promise<ExecutableCheck> {
	let file: string | undefined;
	const candidates = options.strictPath !== undefined ? [options.strictPath] : [options.explicit, ...options.fallbacks ?? [],
		...[name, ...options.alternates ?? []].flatMap(candidate => (process.env.PATH ?? "").split(path.delimiter).filter(Boolean).map(directory => path.join(directory, candidate)))];
	try {
		file = options.strictPath !== undefined ? await realpath(options.strictPath) : await resolveHostExecutable(options.explicit, name, options.fallbacks, options.alternates);
		// rustup and other toolchain shims dispatch by argv[0]; invoking the resolved symlink target would test the shim instead of Cargo/rustc.
		if (options.version) for (const candidate of candidates) {
			if (candidate && await realpath(candidate).then(resolved => resolved === file, () => false)) {
				file = path.isAbsolute(candidate) ? candidate : path.resolve(candidate);
				break;
			}
		}
		if (!(await stat(file)).isFile()) throw new Error("not a regular file");
		await access(file, fsConstants.X_OK);
	} catch {
		for (const candidate of candidates) {
			if (candidate && await stat(candidate).then(entry => entry.isFile(), () => false)) {
				return { label, state: "blocked", file: candidate, detail: "installed but not executable; check permissions and the selected path" };
			}
		}
		return { label, state: "missing", detail: "not found at the runtime lookup paths" };
	}
	if (!options.version) return { label, state: "executable", file, detail: "installed, executable permissions; behavior not tested here" };
	try {
		const versions = clean(await query(file, ["--version"])).trim().split("\n");
		const version = versions.find(line => line.toLowerCase().startsWith(name.toLowerCase())) ?? versions[0];
		return { label, state: "runnable", file, detail: `installed, launches${version ? ` (${version})` : ""}; feature qualification pending` };
	} catch (error) { return { label, state: "blocked", file, detail: `installed but version query failed: ${clean(errorMessage(error))}` }; }
}

function query(file: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(file, args, { encoding: "utf8", cwd: "/", env: { ...process.env, RUSTUP_AUTO_INSTALL: "0", CARGO_NET_OFFLINE: "true" },
			timeout: 2_000, killSignal: "SIGKILL", maxBuffer: 16_384 }, (error, stdout, stderr) => {
			if (error) reject(new Error(stderr.trim() || error.message)); else resolve(`${stdout}${stderr}`);
		});
	});
}

function formatExecutable(check: ExecutableCheck): string { return `${check.label}: ${check.detail}${check.file ? ` — ${clean(check.file)}` : ""}`; }
function clean(text: string): string { return stripVTControlCharacters(text).replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "").slice(0, 320); }
function scalar(text: string | undefined): string { return text === undefined ? "unknown" : clean(text.trim()); }
function statusField(text: string | undefined, key: string): string { return scalar(new RegExp(`^${key}:\\s*(\\d+)`, "m").exec(text ?? "")?.[1]); }
async function textFile(file: string): Promise<string | undefined> { return readFile(file, "utf8").catch(() => undefined); }
