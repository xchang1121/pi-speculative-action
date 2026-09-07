import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { filesystemObservationDigest } from "../src/provenance-certificate.ts";
import { observeStrace, type StraceObservationOptions } from "../src/strace-observer.ts";

const EXEC = 'execve("/usr/bin/example", ["example"], 0x0) = 0';
const STAT = "{st_dev=makedev(0, 1), st_ino=42, st_mode=S_IFREG|0644, st_nlink=1, st_uid=0, st_gid=0, st_rdev=0, st_size=4, st_blksize=4096, st_blocks=8, st_atime=10, st_atime_nsec=1, st_mtime=11, st_mtime_nsec=2, st_ctime=12, st_ctime_nsec=3}";
const STAT_DIGEST = filesystemObservationDigest({
	dev: 1n, ino: 42n, mode: 0o100644n, nlink: 1n, uid: 0n, gid: 0n, rdev: 0n,
	size: 4n, blksize: 4096n, blocks: 8n,
	atimeNs: 10_000_000_001n, mtimeNs: 11_000_000_002n, ctimeNs: 12_000_000_003n,
});

/** Owns a complete per-PID transcript, including its filesystem lifetime. */
async function observe(processes: Record<number, readonly string[]>, options?: StraceObservationOptions) {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-observer-"));
	const prefix = path.join(root, "process");
	try {
		await Promise.all(Object.entries(processes).map(([pid, lines]) => fs.writeFile(prefix + "." + pid, lines.join("\n"))));
		return await observeStrace(prefix, "/usr/bin/example", "/work", options);
	} finally {
		await fs.rm(root, { recursive: true, force: true });
	}
}

describe("strace provenance decoder", () => {
	test("separates pathname and descriptor data from syscall evidence", async () => {
		for (const name of ["st_ino=99", "AT_SYMLINK_NOFOLLOW", "<unfinished ...>", 'nested(,){ }[ ] "quote"', "café", "result=-1", "ending-"]) {
			const target = "/work/" + name;
			const quoted = JSON.stringify(target).replace("é", "\\303\\251");
			const descriptor = target.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("<", "\\074").replaceAll(">", "\\076").replace("é", "\\xc3\\xa9");
			const observation = await observe({ 100: [
				EXEC,
				"newfstatat(AT_FDCWD, " + quoted + ", " + STAT + ", 0) = 0",
				"fstat(3<" + descriptor + ">, " + STAT + ") = 0",
			] });
			expect(observation, name).toMatchObject({ complete: true, taints: [], incompleteReasons: [] });
			expect(observation.paths, name).toContainEqual({ path: target, role: "metadata", followSymlinks: true, digest: STAT_DIGEST });
		}
		const failed = await observe({ 100: [EXEC, 'newfstatat(AT_FDCWD, "/work/result=0", 0xabc, 0) = -1 ENOENT (No such file or directory)'] });
		expect(failed).toMatchObject({ complete: true, taints: [], incompleteReasons: [] });
		expect(failed.paths).toContainEqual({ path: "/work/result=0", role: "input" });
		await expect(observe({ 100: [EXEC, 'openat(AT_FDCWD, "/work/\\377", O_RDONLY) = 3'] })).rejects.toThrow();
	});

	test("follows target descendants and descriptor-relative metadata without confusing data with flags", async () => {
		const observation = await observe({
			100: [EXEC, 'openat(AT_FDCWD, "/work/input.txt", O_RDONLY) = 3</work/input.txt>',
				"fstat(3</work/input.txt>, " + STAT + ") = 0", 'chdir("/work/sub") = 0',
				'openat(AT_FDCWD, "/work/final", O_RDONLY|O_DIRECTORY) = 4</work/final>',
				"fchdir(4</work/final>) = 0", "clone(child_stack=NULL, flags=SIGCHLD) = 101", "+++ exited with 0 +++"],
			101: ['execve("/usr/bin/child", ["child"], 0x0) = 0',
				'newfstatat(AT_FDCWD, "relative.dat", ' + STAT + ", 0) = 0",
				'newfstatat(5</work/other>, "link", ' + STAT + ", AT_SYMLINK_NOFOLLOW) = 0", "+++ exited with 0 +++"],
		});
		expect(observation).toMatchObject({ complete: true, tracedProcesses: 2, taints: [] });
		expect(observation.paths).toEqual(expect.arrayContaining([
			{ path: "/usr/bin/example", role: "executable" }, { path: "/usr/bin/child", role: "executable" },
			{ path: "/work/input.txt", role: "input" }, { path: "/work/final", role: "input" },
			{ path: "/work/input.txt", role: "metadata", followSymlinks: true, digest: STAT_DIGEST },
			{ path: "/work/final/relative.dat", role: "metadata", followSymlinks: true, digest: STAT_DIGEST },
			{ path: "/work/other/link", role: "metadata", followSymlinks: false, digest: STAT_DIGEST },
		]));
	});

	test("reassembles completed syscalls before extracting dependencies and children", async () => {
		const observation = await observe({
			150: [EXEC, 'openat(AT_FDCWD, "/work/input.txt", O_RDONLY <unfinished ...>',
				"<... openat resumed>) = 3</work/input.txt>", "clone(child_stack=NULL, flags=SIGCHLD <unfinished ...>",
				"<... clone resumed>) = 151", "socket(AF_INET, SOCK_STREAM, IPPROTO_IP <unfinished ...>",
				"<... socket resumed>) = 4"],
			151: ['openat(AT_FDCWD, "/work/child.txt", O_RDONLY) = 3'],
		});
		expect(observation).toMatchObject({ complete: true, tracedProcesses: 2, incompleteReasons: [] });
		expect(observation.taints).toContain("network");
		expect(observation.paths).toEqual(expect.arrayContaining([
			{ path: "/work/input.txt", role: "input" }, { path: "/work/child.txt", role: "input" },
		]));
	});

	test("selects the shallowest matching exec from process topology", async () => {
		const observation = await observe({
			700: [EXEC, 'openat(AT_FDCWD, "/work/root.txt", O_RDONLY) = 3', "clone(child_stack=NULL, flags=SIGCHLD) = 600"],
			600: [EXEC, 'openat(AT_FDCWD, "/work/child.txt", O_RDONLY) = 3'],
		});
		expect(observation.complete).toBe(true);
		expect(observation.paths).toContainEqual({ path: "/work/root.txt", role: "input" });
	});

	test("fails closed on missing process evidence or malformed syntax", async () => {
		for (const [lines, reasons] of [
			[["<... openat resumed>) = 3</work/lost.txt>"], ["resumed_without_unfinished:100:openat"]],
			[["fchdir(9) = 0", "clone(child_stack=NULL, flags=SIGCHLD) = 201"], ["child_trace_missing:201", "fchdir_unparsed:100"]],
			[['openat(AT_FDCWD, "file", O_RDONLY <unfinished ...>'], ["unfinished:100:openat"]],
			[['newfstatat(AT_FDCWD, "file", {st_ino=42], 0) = 0'], ["syscall_unparsed:newfstatat"]],
			[['openat(AT_FDCWD, "unterminated, O_RDONLY) = 0'], ["syscall_unparsed:openat"]],
		]) {
			const observation = await observe({ 100: [EXEC, ...lines!] });
			expect(observation).toMatchObject({ complete: false, incompleteReasons: reasons });
			expect(observation.taints).toContain("trace_incomplete");
		}
	});

	test("cuts dispatcher subtrees but resumes provenance at a descriptor-preserving native exec", async () => {
		for (const bypass of [false, true]) {
			const observation = await observe({
				300: [EXEC, 'chdir("/usr/bin") = 0', "clone(child_stack=NULL, flags=SIGCHLD) = 301"],
				301: ['execve("./tool", ["tool"], 0x0) = 0', "getpid() = 301",
					'openat(AT_FDCWD, "/private/launcher", O_RDONLY) = 4',
					...(bypass ? ['execve("/private/original/tool", ["tool"], 0x0) = 0',
						'openat(AT_FDCWD, "/work/input", O_RDONLY) = 4'] : ["socket(AF_INET, SOCK_STREAM, IPPROTO_IP) = 3"])],
			}, { interposedExecutables: [["/usr/bin/tool", "/private/original/tool"]] });
			expect(observation).toMatchObject({ complete: true, taints: [] });
			expect(observation.paths).not.toContainEqual({ path: "/usr/bin/tool", role: "executable" });
			expect(observation.paths).not.toContainEqual({ path: "/private/launcher", role: "input" });
			if (bypass) expect(observation.paths).toEqual(expect.arrayContaining([
				{ path: "/private/original/tool", role: "executable" }, { path: "/work/input", role: "input" },
			]));
		}
	});

	test("classifies effects from syscall arguments and results, never embedded strings", async () => {
		for (const [line, taints] of [
			['prctl(PR_SET_NAME, "worker socket(AF_UNIX) = -1 EPERM") = 0', []],
			['prlimit64(0, RLIMIT_STACK, NULL, {rlim_cur=8388608, rlim_max=RLIM64_INFINITY}) = 0', []],
			['setrlimit(RLIMIT_CORE, {rlim_cur=0, rlim_max=0}) = 0', ["unsupported_syscall"]],
			['socket(AF_UNIX, SOCK_STREAM, 0) = 3<UNIX-STREAM:[1->2]>', ["network"]],
			['getsockname(1, {sa_family=AF_UNIX, sun_path="/private/output"}, [110 => 18]) = 0', ["network"]],
			['getpeername(1, {sa_family=AF_UNIX}, [110 => 2]) = 0', ["network"]],
			['getpeername(0</dev/null<char 1:3>>, 0x123, [16]) = -1 ENOTSOCK (Socket operation on non-socket)', []],
			['getpeername(7, 0x123, [16]) = -1 ENOTSOCK (Socket operation on non-socket)', ["network"]],
			['getsockopt(1, SOL_SOCKET, SO_PEERCRED, {pid=42, uid=1000, gid=1000}, [12]) = 0', ["network"]],
			['clock_gettime(CLOCK_REALTIME, {tv_sec=1, tv_nsec=2}) = 0', ["clock"]],
			['getrandom("abc", 3, 0) = 3', ["random"]],
			['getpid() = 2', ["pid_observation"]],
			['fstat(1<pipe:[7]>, ' + STAT + ') = 0', ["descriptor_observation"]],
			['prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) = 1', ["confinement_observation"]],
			['openat(AT_FDCWD, "/root/secret", O_RDONLY) = -1 EACCES (Permission denied)', ["confinement_observation"]],
			['clone(child_stack=NULL, flags=SIGCHLD) = -1 EAGAIN (Resource temporarily unavailable)', ["confinement_observation"]],
			['setxattr("/work/output", "user.pi", "x", 1, 0) = 0', ["unsupported_syscall"]],
			['getxattr("/work/input", "user.pi", NULL, 0) = -1 ENODATA (No data available)', ["unsupported_syscall"]],
			['utimensat(AT_FDCWD, "/work/output", NULL, 0) = 0', ["unsupported_syscall"]],
			['fallocate(3</work/output>, 0, 0, 4096) = 0', ["unsupported_syscall"]],
			['ioctl(3</work/output>, FS_IOC_SETFLAGS, [FS_NODUMP_FL]) = 0', ["unsupported_syscall"]],
			['ioctl(1</dev/null<char 1:3>>, TCGETS, 0x7fff0000) = -1 ENOTTY (Inappropriate ioctl for device)', []],
		] as const) {
			const observation = await observe({ 100: [EXEC, line] });
			expect(observation, line).toMatchObject({ complete: true, taints, incompleteReasons: [] });
			if (line.includes('"/work/input"')) expect(observation.paths).toContainEqual({ path: "/work/input", role: "input" });
			if (line.startsWith("setxattr")) expect(observation.paths).toContainEqual({ path: "/work/output", role: "input" });
		}
	});

	test("fails closed on COW-driver semantic gaps inside the workspace only", async () => {
		const observation = await observe({ 502: [EXEC,
			'rename("source", "moved") = -1 EXDEV (Invalid cross-device link)',
			'openat(AT_FDCWD, ".", O_RDWR|O_TMPFILE, 0600) = -1 EOPNOTSUPP (Operation not supported)',
			'rename("/outside/source", "/outside/moved") = -1 EXDEV (Invalid cross-device link)',
		] }, { guardFilesystemSemanticsWithin: ["/work"] });
		expect(observation.complete).toBe(false);
		expect(observation.taints).toEqual(expect.arrayContaining(["unsupported_syscall", "trace_incomplete"]));
		expect(observation.incompleteReasons).toEqual(["filesystem_semantics:openat:502", "filesystem_semantics:rename:502"]);
	});
});
