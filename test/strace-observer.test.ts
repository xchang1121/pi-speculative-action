import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, test } from "vitest";
import { observeStrace } from "../src/strace-observer.ts";

describe("strace provenance decoder", () => {
	test("follows the target exec and recursively identified descendants", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-observer-"));
		const prefix = path.join(root, "process");
		try {
			await Promise.all([
				fs.writeFile(
					`${prefix}.100`,
					[
						'execve("/usr/bin/example", ["example"], 0x0) = 0',
						'openat(AT_FDCWD, "/work/input.txt", O_RDONLY) = 3</work/input.txt>',
						"clone(child_stack=NULL, flags=SIGCHLD) = 101",
						"+++ exited with 0 +++",
					].join("\n"),
				),
				fs.writeFile(
					`${prefix}.101`,
					[
						'execve("/usr/bin/child", ["child"], 0x0) = 0',
						'newfstatat(AT_FDCWD, "relative.dat", {st_mode=S_IFREG|0644}, 0) = 0',
						"+++ exited with 0 +++",
					].join("\n"),
				),
			]);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.complete).toBe(true);
			expect(observation.tracedProcesses).toBe(2);
			expect(observation.paths).toEqual(
				expect.arrayContaining([
					{ path: "/usr/bin/example", role: "executable" },
					{ path: "/usr/bin/child", role: "executable" },
					{ path: "/work/input.txt", role: "input" },
					{ path: "/work/relative.dat", role: "input" },
				]),
			);
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("fails closed when a traced child transcript is absent", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-incomplete-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(
				`${prefix}.200`,
				'execve("/usr/bin/example", ["example"], 0x0) = 0\nclone(child_stack=NULL, flags=SIGCHLD) = 201\n',
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.complete).toBe(false);
			expect(observation.taints).toContain("trace_incomplete");
			expect(observation.incompleteReasons).toContain("child_trace_missing:201");
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("cuts dispatcher implementation subtrees at an interposed exec", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-dispatcher-"));
		const prefix = path.join(root, "process");
		try {
			await Promise.all([
				fs.writeFile(
					`${prefix}.300`,
					'execve("/bin/bash", ["bash"], 0x0) = 0\nclone(child_stack=NULL, flags=SIGCHLD) = 301\n',
				),
				fs.writeFile(
					`${prefix}.301`,
					[
						'newfstatat(AT_FDCWD, "/usr/bin/sleep", {st_mode=S_IFREG|0755}, 0) = 0',
						'execve("/usr/bin/sleep", ["sleep", "1"], 0x0) = 0',
						'socket(AF_INET, SOCK_STREAM, IPPROTO_IP) = 3',
					].join("\n"),
				),
			]);
			const observation = await observeStrace(prefix, "/bin/bash", "/work", {
				ignoredExecutablePaths: ["/usr/bin/sleep"],
			});
			expect(observation.complete).toBe(true);
			expect(observation.taints).not.toContain("network");
			expect(observation.paths).not.toContainEqual({ path: "/usr/bin/sleep", role: "executable" });
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("distinguishes local broker sockets from external nondeterminism", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-taints-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(
				`${prefix}.400`,
				[
					'execve("/usr/bin/example", ["example"], 0x0) = 0',
					'socket(AF_UNIX, SOCK_STREAM, 0) = 3<UNIX-STREAM:[1]>',
					'socket(AF_INET, SOCK_STREAM, IPPROTO_IP) = 4',
					'clock_gettime(CLOCK_REALTIME, {tv_sec=1, tv_nsec=2}) = 0',
					'getrandom("abc", 3, 0) = 3',
					'getpid() = 2',
				].join("\n"),
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.taints).toEqual(expect.arrayContaining(["network", "clock", "random", "pid_observation"]));
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("treats Unix-domain communication as external input unless a provider removes it from the trace", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-unix-socket-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(`${prefix}.450`, [
				'execve("/usr/bin/example", ["example"], 0x0) = 0',
				'socket(AF_UNIX, SOCK_STREAM, 0) = 3<UNIX-STREAM:[1]>',
			].join("\n"));
			expect((await observeStrace(prefix, "/usr/bin/example", "/work")).taints).toContain("network");
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("taints persistent file semantics outside the typed transaction", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-file-semantics-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(
				`${prefix}.500`,
				[
					'execve("/usr/bin/example", ["example"], 0x0) = 0',
					'setxattr("/work/output", "user.pi", "x", 1, 0) = 0',
					'getxattr("/work/input", "user.pi", NULL, 0) = -1 ENODATA (No data available)',
					'prlimit64(0, RLIMIT_CORE, NULL, {rlim_cur=0, rlim_max=RLIM64_INFINITY}) = 0',
					'utimensat(AT_FDCWD, "/work/output", NULL, 0) = 0',
					'fallocate(3</work/output>, 0, 0, 4096) = 0',
					'ioctl(3</work/output>, FS_IOC_SETFLAGS, [FS_NODUMP_FL]) = 0',
				].join("\n"),
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.taints).toContain("unsupported_syscall");
			expect(observation.paths).toEqual(
				expect.arrayContaining([
					{ path: "/work/input", role: "input" },
					{ path: "/work/output", role: "input" },
				]),
			);
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("does not taint a failed file-descriptor ioctl", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-failed-ioctl-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(
				`${prefix}.501`,
				[
					'execve("/usr/bin/example", ["example"], 0x0) = 0',
					'ioctl(1</dev/null<char 1:3>>, TCGETS, 0x7fff0000) = -1 ENOTTY (Inappropriate ioctl for device)',
				].join("\n"),
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.taints).not.toContain("unsupported_syscall");
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});

	test("fails closed on COW-driver semantic gaps inside the workspace only", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-strace-driver-gap-"));
		const prefix = path.join(root, "process");
		try {
			await fs.writeFile(
				`${prefix}.502`,
				[
					'execve("/usr/bin/example", ["example"], 0x0) = 0',
					'rename("source", "moved") = -1 EXDEV (Invalid cross-device link)',
					'openat(AT_FDCWD, ".", O_RDWR|O_TMPFILE, 0600) = -1 EOPNOTSUPP (Operation not supported)',
					'rename("/outside/source", "/outside/moved") = -1 EXDEV (Invalid cross-device link)',
				].join("\n"),
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work", {
				guardFilesystemSemanticsWithin: ["/work"],
			});
			expect(observation.complete).toBe(false);
			expect(observation.taints).toEqual(expect.arrayContaining(["unsupported_syscall", "trace_incomplete"]));
			expect(observation.incompleteReasons).toEqual([
				"filesystem_semantics:openat:502",
				"filesystem_semantics:rename:502",
			]);
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});
});
