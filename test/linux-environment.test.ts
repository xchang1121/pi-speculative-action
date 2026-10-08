import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { inspectLinuxEnvironment, parseLinuxOsRelease } from "../src/linux-environment.ts";

const mocks = vi.hoisted(() => ({
	readFile: vi.fn(), access: vi.fn(), stat: vi.fn(), realpath: vi.fn(), resolve: vi.fn(), execFile: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({ readFile: mocks.readFile, access: mocks.access, stat: mocks.stat, realpath: mocks.realpath }));
vi.mock("node:child_process", () => ({ execFile: mocks.execFile }));
vi.mock("node:os", () => ({ default: { release: () => "6.6.0-test", homedir: () => "/home/test", machine: () => "x86_64" } }));
vi.mock("../src/executable-path.ts", () => ({ resolveHostExecutable: mocks.resolve }));

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const arch = Object.getOwnPropertyDescriptor(process, "arch")!;
const executables = new Map<string, string>(), files = new Map<string, string>(), existing = new Set<string>(), denied = new Set<string>();
const missing = () => Object.assign(new Error("ENOENT"), { code: "ENOENT" });

function install(name: string, file = path.join("/usr/bin", name)): void { executables.set(name, file); existing.add(file); }

beforeEach(() => {
	vi.clearAllMocks();
	vi.stubEnv("PATH", "/usr/bin");
	for (const key of ["PI_SPEC_HELD_EXEC", "PI_SPEC_SANDLOCK", "PI_SPEC_STRACE", "container", "WSL_INTEROP", "WSL_DISTRO_NAME"]) vi.stubEnv(key, undefined);
	Object.defineProperty(process, "platform", { ...platform, value: "linux" });
	Object.defineProperty(process, "arch", { ...arch, value: "x64" });
	executables.clear(); files.clear(); existing.clear(); denied.clear();
	files.set("/etc/os-release", 'ID=openEuler\nNAME="openEuler"\nPRETTY_NAME="openEuler 24.03 LTS"\nVERSION_ID="24.03"');
	files.set("/proc/self/status", "Seccomp:\t2\nNoNewPrivs:\t0\nTracerPid:\t0\n");
	files.set("/proc/sys/kernel/yama/ptrace_scope", "1\n");
	files.set("/proc/sys/kernel/unprivileged_userns_clone", "1\n");
	files.set("/proc/sys/user/max_user_namespaces", "1024\n");
	files.set("/sys/kernel/security/lsm", "landlock,lockdown,yama,integrity,selinux\n");
	for (const name of ["git", "cc", "cargo", "rustc", "make", "tar", "xz", "pi-speculative-sandlock", "strace", "fuse-overlayfs", "fusermount3"]) install(name);
	for (const file of [path.join("/home/test", ".local", "bin", "pi-speculative-held-exec"),
		`${path.join("/home/test", ".local", "bin", "pi-speculative-held-exec")}.so`, "/dev/fuse", "/usr/lib/libc.a"]) existing.add(file);
	mocks.readFile.mockImplementation(async (file: string) => { if (!files.has(file)) throw missing(); return files.get(file); });
	mocks.stat.mockImplementation(async (file: string) => { if (!existing.has(file)) throw missing(); return { isFile: () => true }; });
	mocks.access.mockImplementation(async (file: string) => { if (!existing.has(file) || denied.has(file)) throw missing(); });
	mocks.realpath.mockImplementation(async (file: string) => { if (!existing.has(file)) throw missing(); return file; });
	mocks.resolve.mockImplementation(async (explicit: string | undefined, name: string) => {
		const file = explicit || executables.get(name);
		if (!file || denied.has(file)) throw missing();
		return file;
	});
	mocks.execFile.mockImplementation((file: string, args: readonly string[], _options: unknown, callback: (error: Error | null, stdout: string, stderr: string) => void) => {
		callback(null, args[0] === "-print-file-name=libc.a" ? "/usr/lib/libc.a\n" :
			path.basename(file) === "fuse-overlayfs" ? "fusermount3 version: 3.14.0\nfuse-overlayfs: version 1.17\n" : `${path.basename(file)} 1.0\n`, "");
	});
});

afterEach(() => {
	Object.defineProperty(process, "platform", platform);
	Object.defineProperty(process, "arch", arch);
	vi.unstubAllEnvs();
});

describe("Linux environment inventory", () => {
	it("parses release values as data without shell expansion or terminal controls", () => {
		const release = parseLinuxOsRelease('ID=openEuler\nPRETTY_NAME="openEuler \\"quoted\\" $HOME $(touch /tmp/no) `id`"\nNAME=\'literal\'\nINVALID=unquoted space\n# ignored\nVERSION_ID="24.03\u001b[31m"');
		expect(release.ID).toBe("openEuler");
		expect(release.PRETTY_NAME).toBe('openEuler "quoted" $HOME $(touch /tmp/no) `id`');
		expect(release.NAME).toBe("literal");
		expect(release.VERSION_ID).toBe("24.03");
		expect(release.INVALID).toBeUndefined();
		expect(mocks.execFile).not.toHaveBeenCalled();
	});

	it("reports openEuler, tools and policy without claiming runtime qualification", async () => {
		const report = await inspectLinuxEnvironment();
		expect(report.text).toContain("openEuler 24.03 LTS");
		expect(report.text).toContain("Node arch=x64; kernel=6.6.0-test");
		expect(report.text).toContain("ptrace_scope=1; Seccomp=2; NoNewPrivs=0; TracerPid=0");
		expect(report.text).toContain("Static libc: archive found");
		expect(report.text).toContain("dnf provides '*/libc.a'");
		expect(report.text).toContain("fuse-overlayfs: installed, launches (fuse-overlayfs: version 1.17)");
		expect(report.text).toContain("full mount/copy-up/whiteout/unmount probe");
		expect(report.text).toContain("behavior not tested here");
		expect(report.text).not.toMatch(/ready|qualified successfully/i);
		expect(report.warnings).toBe(false);
		for (const [, args, options] of mocks.execFile.mock.calls) {
			expect([["--version"], ["-print-file-name=libc.a"]]).toContainEqual(args);
			expect(options).toMatchObject({ timeout: 2_000, maxBuffer: 16_384, killSignal: "SIGKILL", cwd: "/", env: { RUSTUP_AUTO_INSTALL: "0", CARGO_NET_OFFLINE: "true" } });
		}
	});

	it("preserves the Cargo proxy invocation name instead of merely checking rustup", async () => {
		const cargo = path.join("/home/test", ".cargo", "bin", "cargo"), rustup = path.join("/home/test", ".cargo", "bin", "rustup");
		existing.add(cargo); existing.add(rustup); executables.set("cargo", rustup);
		mocks.realpath.mockImplementation(async (file: string) => {
			if (!existing.has(file)) throw missing();
			return file === cargo ? rustup : file;
		});
		await inspectLinuxEnvironment();
		expect(mocks.execFile.mock.calls.some(([file]) => file === cargo)).toBe(true);
		expect(mocks.execFile.mock.calls.some(([file]) => file === rustup)).toBe(false);
	});

	it("keeps the installation confirmation short while the Doctor retains detailed paths", async () => {
		files.set("/etc/os-release", `ID=openEuler\nPRETTY_NAME="${"openEuler ".repeat(80)}"`);
		const report = await inspectLinuxEnvironment();
		expect(report.summary.length).toBeLessThan(700);
		expect(report.summary.split("\n")).toHaveLength(6);
		expect(report.summary).not.toContain("/usr/lib/libc.a");
		expect(report.summary).not.toContain("kernel=");
		expect(report.text).toContain("/usr/lib/libc.a");
	});

	it("distinguishes missing programs, non-executable files and failed launches", async () => {
		existing.delete(executables.get("cargo")!); executables.delete("cargo");
		denied.add(executables.get("git")!);
		mocks.execFile.mockImplementation((file: string, _args: readonly string[], _options: unknown, callback: (error: Error | null, stdout: string, stderr: string) => void) => {
			callback(new Error("toolchain unavailable"), "", `${path.basename(file)}: toolchain unavailable`);
		});
		const report = await inspectLinuxEnvironment();
		expect(report.warnings).toBe(true);
		expect(report.text).toContain("Git: installed but not executable");
		expect(report.text).toContain("Cargo: not found");
		expect(report.text).toContain("Rust compiler: installed but version query failed");
		expect(report.text).toContain("https://rustup.rs");
		expect(report.text).toContain("prepare Git, C compiler, Cargo");
	});

	it("reports unavailable FUSE access and missing static libc without changing the system", async () => {
		denied.add("/dev/fuse"); existing.delete("/usr/lib/libc.a");
		const report = await inspectLinuxEnvironment();
		expect(report.warnings).toBe(true);
		expect(report.text).toContain("Static libc: not located");
		expect(report.text).toContain("/dev/fuse: missing or inaccessible");
		expect(report.text).toContain("ask the host/container administrator");
		expect(report.text).toContain("No system packages, permissions or settings were changed");
	});

	it.each(["arm64", "riscv64"])("separates architecture support for %s from installation", async architecture => {
		Object.defineProperty(process, "arch", { ...arch, value: architecture });
		const report = await inspectLinuxEnvironment();
		expect(report.text).toContain("running process capture requires x86-64; unavailable on this Node architecture");
		expect(report.text).toContain(`pinned fuse-overlayfs download ${architecture === "arm64" ? "exists" : "is unavailable"}`);
		expect(report.warnings).toBe(true);
	});

	it("falls back to usr/lib os-release and leaves inaccessible policy unknown", async () => {
		files.delete("/etc/os-release"); files.set("/usr/lib/os-release", 'ID=other\nPRETTY_NAME="Other Linux"');
		files.delete("/proc/self/status"); files.delete("/proc/sys/kernel/yama/ptrace_scope");
		const report = await inspectLinuxEnvironment();
		expect(report.text).toContain("Other Linux");
		expect(report.text).toContain("ptrace_scope=unknown; Seccomp=unknown");
		expect(report.text).not.toContain("dnf provides");
	});

	it("respects the strict held-exec override and reports container tracing constraints", async () => {
		vi.stubEnv("PI_SPEC_HELD_EXEC", "/missing/held-exec");
		files.set("/run/systemd/container", "podman\n");
		files.set("/proc/sys/kernel/yama/ptrace_scope", "3\n");
		const report = await inspectLinuxEnvironment();
		expect(report.text).toContain("Held-exec: not found");
		expect(report.text).toContain("container=podman");
		expect(report.text).toContain("host tracing/container policy may restrict ptrace or seccomp");
	});

	it("flags a restrictive ptrace policy even when installed prerequisites are present", async () => {
		files.set("/proc/sys/kernel/yama/ptrace_scope", "3\n");
		const report = await inspectLinuxEnvironment();
		expect(report.warnings).toBe(true);
		expect(report.summary).toContain("Tracing policy may restrict reuse");
		expect(report.text).toContain("runtime behavior probes decide qualification");
	});
});
