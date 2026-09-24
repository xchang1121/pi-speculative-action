import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { filesystemObservationDigest } from "../src/provenance-certificate.ts";
import { observeStrace, straceCommand, type StraceObservationOptions } from "../src/strace-observer.ts";

const EXEC = 'execve("/usr/bin/example", ["example"], 0x0) = 0';
const STAT = "{st_dev=makedev(0, 1), st_ino=42, st_mode=S_IFREG|0644, st_nlink=1, st_uid=0, st_gid=0, st_rdev=0, st_size=4, st_blksize=4096, st_blocks=8, st_atime=10, st_atime_nsec=1, st_mtime=11, st_mtime_nsec=2, st_ctime=12, st_ctime_nsec=3}";
const STAT_DIGEST = filesystemObservationDigest({
	dev: 1n, ino: 42n, mode: 0o100644n, nlink: 1n, uid: 0n, gid: 0n, rdev: 0n,
	size: 4n, blksize: 4096n, blocks: 8n,
	atimeNs: 10_000_000_001n, mtimeNs: 11_000_000_002n, ctimeNs: 12_000_000_003n,
});

/** Owns a complete per-PID transcript, including its filesystem lifetime. */
async function observe(processes: Record<number, readonly string[]>, options?: StraceObservationOptions, terminated = true) {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-observer-"));
	const prefix = path.join(root, "process");
	try {
		await Promise.all(Object.entries(processes).map(([pid, lines]) =>
			fs.writeFile(prefix + "." + pid, [...lines, ...(terminated ? ["+++ exited with 0 +++"] : [])].join("\n"))));
		return await observeStrace(prefix, "/usr/bin/example", "/work", options);
	} finally {
		await fs.rm(root, { recursive: true, force: true });
	}
}

describe("strace provenance decoder", () => {
	test("accepts directory enumeration only with its exact broker image and successful result", async () => {
		for (const syscall of ["getdents", "getdents64"]) for (const configured of [false, true]) for (const failed of [false, true]) {
			const observation = await observe({ 100: [EXEC, `${syscall}(10</work/anchor>, [], 512) = ${failed ? "-1 EINVAL (Invalid argument)" : "0"}`] }, {
				inheritedDirectoryImages: configured ? ["/work/anchor"] : [],
			});
			expect(observation.taints.includes("unsupported_syscall"), `${syscall}:${configured}:${failed}`).toBe(configured === failed);
		}
	});
	test("releases stream references after aliases, copied tables and shared tables close", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-stream-lifetime-")), prefix = path.join(root, "process");
		try {
			for (const shared of [false, true]) {
				await fs.writeFile(prefix + ".stream", [
					`100 ${EXEC}`, '100 dup(3<UNIX-STREAM:[91->92]>) = 5<UNIX-STREAM:[91->92]>',
					`100 clone(child_stack=NULL, flags=${shared ? "CLONE_FILES|" : ""}SIGCHLD) = 101`,
					'100 close(3<UNIX-STREAM:[91->92]>) = 0', '100 close(5<UNIX-STREAM:[91->92]>) = 0',
					...(!shared ? ['101 close(3<UNIX-STREAM:[91->92]>) = 0', '101 close(5<UNIX-STREAM:[91->92]>) = 0'] : []),
					'101 read(4<UNIX-STREAM:[92->91]>, "", 1) = 0', '101 +++ exited with 0 +++', '100 +++ exited with 0 +++',
				].join("\n"));
				const observed = await observeStrace(prefix, "/usr/bin/example", "/work", { inheritedStreams: ["91", "92"],
					inheritedHandles: [{ fd: 3, inode: "91", flags: 2, outside: 0 }, { fd: 4, inode: "92", flags: 2, outside: 3 }] });
				expect(observed).toMatchObject({ complete: true, taints: ["clock", "random"], resourceJournal: [
					{ inode: "91", kind: "release", data: Buffer.from([3]) }, { inode: "92", kind: "consume", data: Buffer.alloc(0) },
				] });
			}
		} finally { await fs.rm(root, { recursive: true, force: true }); }
	});
	test("orders inherited stream transfers across processes and preserves binary iovecs", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-stream-order-")), prefix = path.join(root, "process");
		try {
			await fs.writeFile(prefix + ".stream", [
				`100 ${EXEC}`, "100 clone(child_stack=NULL, flags=SIGCHLD) = 101",
				'101 read(4<UNIX-STREAM:[91->92]>,  <unfinished ...>',
				'100 writev(3<UNIX-STREAM:[92->91]>, [{iov_base="\\000", iov_len=1}, {iov_base="\\377", iov_len=1}], 2) = 2',
				'101 <... read resumed>"\\000\\377", 2) = 2',
				'101 shutdown(4<UNIX-STREAM:[91->92]>, SHUT_WR) = 0',
				'101 +++ exited with 0 +++', '100 +++ exited with 0 +++',
			].join("\n"));
			const observed = await observeStrace(prefix, "/usr/bin/example", "/work", { inheritedStreams: ["91", "92"] });
			expect(observed).toMatchObject({ complete: true, taints: ["clock", "random"], resourceJournal: [
				{ inode: "92", kind: "produce", data: Buffer.from([0, 255]) },
				{ inode: "91", kind: "consume", data: Buffer.from([0, 255]) },
				{ inode: "91", kind: "shutdown", data: Buffer.from([2]) },
			] });
		} finally { await fs.rm(root, { recursive: true, force: true }); }
	});
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
			expect(observation, name).toMatchObject({ complete: true, taints: ["clock", "random"], incompleteReasons: [] });
			expect(observation.paths, name).toContainEqual({ path: target, role: "metadata", followSymlinks: true, digest: STAT_DIGEST });
		}
		const failed = await observe({ 100: [EXEC, 'newfstatat(AT_FDCWD, "/work/result=0", 0xabc, 0) = -1 ENOENT (No such file or directory)'] });
		expect(failed).toMatchObject({ complete: true, taints: ["clock", "random"], incompleteReasons: [] });
		expect(failed.paths).toContainEqual({ path: "/work/result=0", role: "input" });
		await expect(observe({ 100: [EXEC, 'openat(AT_FDCWD, "/work/\\377", O_RDONLY) = 3'] })).rejects.toThrow();
		// A failed lookup from no known directory read nothing; a successful one leaves the transcript incomplete.
		expect(await observe({ 100: [EXEC, 'openat(8, "unresolved", O_RDONLY) = -1 EBADF (Bad file descriptor)', "statx(0<pipe:[8]>, NULL, 0, STATX_ALL, NULL) = -1 EFAULT (Bad address)"] }))
			.toMatchObject({ complete: true, paths: [{ path: "/usr/bin/example", role: "executable" }], incompleteReasons: [] });
		expect(await observe({ 100: [EXEC, 'openat(8, "unresolved", O_RDONLY) = 3'] })).toMatchObject({ complete: false, incompleteReasons: ["unresolved_pathname:openat:100"] });
	});

	test("reads statx as stat, and an empty or NULL name as its descriptor", async () => {
		const STATX = "{stx_mask=STATX_BASIC_STATS|STATX_MNT_ID, stx_blksize=4096, stx_attributes=0, stx_nlink=1, stx_uid=0, stx_gid=0, stx_mode=S_IFREG|0644, stx_ino=42, stx_size=4, stx_blocks=8, " +
			"stx_attributes_mask=STATX_ATTR_DAX, stx_atime={tv_sec=10, tv_nsec=1} /* 1970-01-01T00:00:10.000000001+0000 */, stx_ctime={tv_sec=12, tv_nsec=3}, stx_mtime={tv_sec=11, tv_nsec=2}, " +
			"stx_rdev_major=0, stx_rdev_minor=0, stx_dev_major=0, stx_dev_minor=1, stx_mnt_id=0x52}";
		const observation = await observe({ 100: [EXEC, // As node, rg and coreutils call it under strace 6.8
			`statx(AT_FDCWD</work>, "file.txt", AT_STATX_SYNC_AS_STAT, STATX_ALL, ${STATX}) = 0`,
			`statx(3</work/link>, "", AT_STATX_SYNC_AS_STAT|AT_SYMLINK_NOFOLLOW|AT_EMPTY_PATH, STATX_ALL, ${STATX}) = 0`,
			`statx(1<pipe:[7]>, "", AT_STATX_SYNC_AS_STAT|AT_EMPTY_PATH, STATX_ALL, ${STATX}) = 0`,
			'statx(AT_FDCWD</work>, "missing.txt", AT_STATX_SYNC_AS_STAT, STATX_ALL, 0x7ffd4527f9f0) = -1 ENOENT (No such file or directory)',
			'readlinkat(4</work/alias>, "", "target", 4096) = 6', 'linkat(5</work/anonymous>, "", AT_FDCWD</work>, "named", AT_EMPTY_PATH) = 0'] });
		expect(observation).toMatchObject({ complete: true, taints: ["clock", "descriptor_observation", "random"], incompleteReasons: [] });
		expect(observation.paths).toEqual([{ path: "/usr/bin/example", role: "executable" },
			...["/work/alias", "/work/anonymous", "/work/missing.txt", "/work/named"].map(path => ({ path, role: "input" })),
			{ path: "/work/link", role: "metadata", followSymlinks: false, digest: STAT_DIGEST }, { path: "/work/file.txt", role: "metadata", followSymlinks: true, digest: STAT_DIGEST }]);
	});

	test("binds reassembled descendants to copied or proven-stable shared cwd contexts", async () => {
		const calls = new Map([
			['renameat(5</work/a>, "source", 6</work/b>, "renamed") = 0', ["/work/a/source", "/work/b/renamed"]],
			['renameat2(AT_FDCWD, "/work/absolute", -1, "/work/replaced", RENAME_NOREPLACE) = 0', ["/work/absolute", "/work/replaced"]],
			['linkat(7</work/c>, "original", 8</work/d>, "linked", 0) = 0', ["/work/c/original", "/work/d/linked"]],
			['symlinkat("literal-not-an-input", 9</work/e>, "alias") = 0', ["/work/e/alias"]],
			['openat(AT_FDCWD</work/recorded>, "anchor", O_RDONLY) = 3', ["/work/recorded/anchor"]],
		]);
		for (const [spawn, mutation, complete, parentMutation = ""] of [
			["fork()", 'chdir("child") = 0', true],
			["vfork()", 'chdir("child") = 0', true],
			["clone(child_stack=NULL, flags=SIGCHLD)", 'chdir("child") = 0', true],
			["clone(child_stack=NULL, flags=CLONE_FS|SIGCHLD)", "", true],
			["clone3({flags=CLONE_FS, exit_signal=SIGCHLD}, 88)", 'chdir("child") = -1 ENOENT (No such file)', true],
			["clone(child_stack=NULL, flags=CLONE_FS|SIGCHLD)", 'chdir("child") = 0', false],
			["clone3({flags=0x211, exit_signal=SIGCHLD}, 88)", "fchdir(9</work/elsewhere>) = 0", false],
			["clone(child_stack=NULL, flags=CLONE_FS|SIGCHLD)", "", false, 'chdir("parent") = 0'],
			["clone(child_stack=NULL, flags=UNKNOWN)", "", false],
		] as const) {
			const observation = await observe({
				800: ['execve("/usr/bin/launcher", [], 0x0) = 0', "clone(child_stack=NULL, flags=SIGCHLD) = 700"],
				700: [EXEC, 'chdir("/work/sub") = 0', "fchdir(4</work/final>) = 0", spawn.slice(0, -1) + " <unfinished ...>",
					`<... ${spawn.split("(")[0]} resumed>) = 600`, parentMutation, 'open("input", O_RDONLY) = 3',
					'openat(AT_FDCWD, "/work/input.txt", O_RDONLY <unfinished ...>', "<... openat resumed>) = 3</work/input.txt>",
					"socket(AF_INET, SOCK_STREAM, IPPROTO_IP <unfinished ...>", "<... socket resumed>) = 4"],
				600: [EXEC, mutation,
					'newfstatat(AT_FDCWD, "relative.dat", ' + STAT + ", 0) = 0",
					'newfstatat(5</work/other>, "link", ' + STAT + ", AT_SYMLINK_NOFOLLOW) = 0", ...calls.keys()],
			});
			expect(observation, spawn + mutation + parentMutation).toMatchObject({ complete, tracedProcesses: 2,
				incompleteReasons: complete ? [] : [spawn.includes("UNKNOWN") ? "clone_flags_unparsed:700" : "shared_cwd_mutation"] });
			if (!complete) { expect(observation.taints).toContain("trace_incomplete"); continue; }
			const childCwd = mutation.endsWith("= 0") ? "/work/final/child" : "/work/final";
			expect(observation.taints).toEqual(["clock", "network", "random"]);
			expect(observation.paths).toEqual(expect.arrayContaining([
				...[...calls.values(), ["/work/final", "/work/final/input", "/work/input.txt"]].flat().map((path) => ({ path, role: "input" })),
				{ path: childCwd + "/relative.dat", role: "metadata", followSymlinks: true, digest: STAT_DIGEST },
				{ path: "/work/other/link", role: "metadata", followSymlinks: false, digest: STAT_DIGEST },
			]));
			expect(observation.paths.some(({ path }) => /literal-not-an-input|\/child\/child/.test(path))).toBe(false);
		}
	});

	test("fails closed on missing process evidence or malformed syntax", async () => {
		for (const [lines, reasons, others, terminated = true] of [
			[[EXEC], ["process_exit_unproven:100"], {}, false],
			[[EXEC, "+++ unfinished +++"], ["process_exit_unproven:100"], {}, false],
			[[EXEC, "+++ exited with 0 +++", 'open("later", O_RDONLY) = 3'], ["process_exit_unproven:100"], {}, false],
			[[EXEC, "fork() = 101", "+++ exited with 0 +++"], ["process_exit_unproven:101"], { 101: [EXEC] }, false],
			[[EXEC, "<... openat resumed>) = 3</work/lost.txt>"], ["resumed_without_unfinished:100:openat"]],
			[[EXEC, "fchdir(9) = 0", "clone(child_stack=NULL, flags=SIGCHLD) = 201"], ["child_trace_missing:201", "fchdir_unparsed:100"]],
			[[EXEC, 'openat(AT_FDCWD, "file", O_RDONLY <unfinished ...>'], ["unfinished:100:openat"]],
			[[EXEC, 'newfstatat(AT_FDCWD, "file", {st_ino=42], 0) = 0'], ["syscall_unparsed:newfstatat"]],
			[[EXEC, 'openat(AT_FDCWD, "unterminated, O_RDONLY) = 0'], ["syscall_unparsed:openat"]],
			[[EXEC], ["target_filesystem_context_unproven"], { 99: ["clone3({flags=CLONE_FS}, 88) = 100"] }],
			[["clone(child_stack=NULL, flags=CLONE_FS|SIGCHLD) = 101", EXEC], ["target_filesystem_context_unproven"], { 101: ['chdir("elsewhere") = 0'] }],
		] as const) {
			const observation = await observe({ 100: lines, ...others }, undefined, terminated);
			expect(observation).toMatchObject({ complete: false, incompleteReasons: reasons });
			expect(observation.taints).toContain("trace_incomplete");
		}
		const prefix = [EXEC, 'open("known", O_RDONLY) = 3', ""].join("\n");
		for (const terminated of [true, false]) {
			const processes = { 100: [prefix, 'open("later", O_RDONLY) = 4'] };
			const preview = await observe(processes, { previewBytes: Buffer.byteLength(prefix) }, terminated);
			expect(preview.complete).toBe(false);
			expect(preview.incompleteReasons).toContain("preview_only");
			expect(preview.paths).toContainEqual({ path: "/work/known", role: "input" });
			expect(preview.paths.some(({ path }) => path === "/work/later")).toBe(false);
			expect((await observe(processes, { previewBytes: 4096 }, terminated)).complete).toBe(false);
		}
		await expect(observe({ 100: [EXEC] }, { previewBytes: 0 })).resolves.toMatchObject({ complete: false, paths: [] });
		await expect(observe({ 100: [EXEC] }, { previewBytes: -1 })).rejects.toThrow("budget");
	});

	test("cuts dispatcher subtrees but resumes provenance at a descriptor-preserving native exec", async () => {
		const native = ['execve("/private/original/tool", ["tool"], 0x0) = 0', 'openat(AT_FDCWD, "/work/input", O_RDONLY) = 4'];
		const inPlace = ['execve("/usr/bin/node", ["node"], 0x0) = 0', "socket(AF_INET, SOCK_STREAM, IPPROTO_IP) = 3", 'execve("/usr/bin/tool", ["tool"], 0x0) = 0', ...native];
		for (const [lines, interpreter, bypass] of [[native, undefined, true], [inPlace, "/usr/bin/node", true], // Directly, or back through the dispatcher.
			[["socket(AF_INET, SOCK_STREAM, IPPROTO_IP) = 3"], undefined, false], [inPlace, undefined, false], [inPlace, "/usr/bin/python3", false]] as const) {
			const observation = await observe({
				300: [EXEC, 'chdir("/usr/bin") = 0', "clone(child_stack=NULL, flags=SIGCHLD) = 301"],
				301: ['execve("./tool", ["tool"], 0x0) = 0', "getpid() = 301", 'openat(AT_FDCWD, "/private/launcher", O_RDONLY) = 4', ...lines],
			}, { interposedExecutables: [["/usr/bin/tool", "/private/original/tool"]], ...(interpreter ? { interpositionInterpreter: interpreter } : {}) });
			expect(observation).toMatchObject({ complete: true, taints: ["clock", "random"] });
			expect(observation.paths).not.toContainEqual({ path: "/usr/bin/tool", role: "executable" });
			expect(observation.paths).not.toContainEqual({ path: "/private/launcher", role: "input" });
			expect(observation.resumedInterpositions).toEqual(bypass ? [301] : undefined);
			if (bypass) expect(observation.paths).toEqual(expect.arrayContaining([
				{ path: "/private/original/tool", role: "executable" }, { path: "/work/input", role: "input" },
			]));
			else expect(observation.paths.some(({ path }) => path === "/work/input")).toBe(false);
		}
	});

	test("classifies effects from syscall arguments and results, never embedded strings", async () => {
		for (const operand of ["3</work/input>", "9</work/input>", "3</work/other>", "3<pipe:[7]>", "9<pipe:[7]>", "3<pipe:[8]>", "3", "3</work/input (deleted)>"]) {
			const line = `fcntl(${operand}, F_GETFL) = 0x8000 (flags O_RDONLY|O_LARGEFILE)`;
			expect((await observe({ 100: [EXEC, line] })).taints).toContain("unsupported_syscall");
			const captured = await observe({ 100: [EXEC, line] }, { inheritedFileImages: ["/work/input", "pipe:[7]"] });
			expect(captured.taints.includes("unsupported_syscall")).toBe(!operand.endsWith("</work/input>") && !operand.endsWith("<pipe:[7]>"));
		}
		const filter = straceCommand("strace", "/trace", ["program"]).find(value => value.startsWith("trace="))!;
		for (const syscall of ["fcntl", "fcntl64", "flock"]) expect(filter.split(/[,=]/)).toContain(syscall);
		for (const [line, taints, semanticGap] of [
			['pipe2([3, 4], O_DIRECT|O_CLOEXEC) = 0', ["unsupported_syscall"]],
			['splice(3<pipe:[7]>, NULL, 4<pipe:[8]>, NULL, 1, 0) = 1', ["unsupported_syscall"]],
			['tee(3<pipe:[7]>, 4<pipe:[8]>, 1, 0) = 1', ["unsupported_syscall"]],
			['fcntl(3</work/input>, F_GETLK, {l_type=F_UNLCK, l_whence=SEEK_SET, l_start=0, l_len=0}) = 0', ["ipc"]],
			['fcntl64(3</work/input>, F_OFD_GETLK, {l_type=F_WRLCK, l_pid=-1}) = 0', ["ipc"]],
			['fcntl(3</work/input>, F_SETLK, {l_type=F_WRLCK}) = -1 EAGAIN (Resource temporarily unavailable)', ["ipc"]],
			['flock(3</work/input>, LOCK_EX|LOCK_NB) = 0', ["ipc"]],
			['fcntl(1<pipe:[7]>, F_SETFL, O_WRONLY|O_NONBLOCK) = 0', ["unsupported_syscall"]],
			['fcntl(3</work/input>, F_SETFL, O_RDONLY|O_NONBLOCK|O_APPEND|O_LARGEFILE|O_DIRECTORY) = 0', []],
			['fcntl(3</work/input>, F_SETFL, O_RDONLY) = 0', []],
			['fcntl(3</work/input>, F_SETFL, O_RDONLY|O_DIRECT) = 0', ["unsupported_syscall"]],
			['fcntl(3</work/input>, F_SETFL, O_ASYNC) = 0', ["unsupported_syscall"]],
			['fcntl(3</work/input>, F_SETFL, O_NONBLOCK) = -1 EINVAL (Invalid argument)', ["unsupported_syscall"]],
			['fcntl(3</outside/input>, F_SETFL, O_NONBLOCK) = 0', ["unsupported_syscall"]],
			['fcntl(3</work/input>, F_GETLEASE) = 2 (F_UNLCK)', ["unsupported_syscall"]],
			['fcntl(1<pipe:[7]>, 0xffff /* F_??? */, 0) = -1 EINVAL (Invalid argument)', ["unsupported_syscall"]],
			['fcntl(1<pipe:[7]>, F_GETFD) = 0', []],
			['fcntl(3</work/input>, F_SETFD, FD_CLOEXEC) = 0', []],
			['fcntl(1<pipe:[7]>, F_DUPFD_CLOEXEC, 10) = 10<pipe:[7]>', []],
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
			['fstat(6</tmp/null<char 1:3>>, ' + STAT + ') = 0', ["descriptor_observation"]],
			['newfstatat(6</tmp/null<char 1:3>>, "", ' + STAT + ', AT_EMPTY_PATH) = 0', ["descriptor_observation"]],
			['prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) = 1', ["confinement_observation"]],
			['openat(AT_FDCWD, "/root/secret", O_RDONLY) = -1 EACCES (Permission denied)', ["confinement_observation"]],
			['clone(child_stack=NULL, flags=SIGCHLD) = -1 EAGAIN (Resource temporarily unavailable)', ["confinement_observation"]],
			['setxattr("/work/output", "user.pi", "x", 1, 0) = 0', ["unsupported_syscall"]],
			['getxattr("/work/input", "user.pi", NULL, 0) = -1 ENODATA (No data available)', ["unsupported_syscall"]],
			['utimensat(3</work/output>, NULL, NULL, 0) = 0', ["unsupported_syscall"]],
			['fallocate(3</work/output>, 0, 0, 4096) = 0', ["unsupported_syscall"]],
			['ioctl(3</work/output>, FS_IOC_SETFLAGS, [FS_NODUMP_FL]) = 0', ["unsupported_syscall"]],
			['ioctl(1</dev/null<char 1:3>>, TCGETS, 0x7fff0000) = -1 ENOTTY (Inappropriate ioctl for device)', []],
			['rename("source", "moved") = -1 EXDEV (Invalid cross-device link)', ["trace_incomplete", "unsupported_syscall"], "rename"],
			['openat(AT_FDCWD, ".", O_RDWR|O_TMPFILE, 0600) = -1 EOPNOTSUPP (Operation not supported)', ["trace_incomplete", "unsupported_syscall"], "openat"],
			['rename("/outside/source", "/outside/moved") = -1 EXDEV (Invalid cross-device link)', []],
		] as const) {
			const observation = await observe({ 100: [EXEC, line] }, { guardFilesystemSemanticsWithin: ["/work"], inheritedFileImages: ["/work/input"] });
			expect(observation, line).toMatchObject({ complete: !semanticGap, taints: [...new Set(["clock", "random", ...taints])].sort(),
				incompleteReasons: semanticGap ? [`filesystem_semantics:${semanticGap}:100`] : [] });
			if (line.includes('"/work/input"')) expect(observation.paths).toContainEqual({ path: "/work/input", role: "input" });
			if (line.startsWith("setxattr")) expect(observation.paths).toContainEqual({ path: "/work/output", role: "input" });
		}
	});
});
