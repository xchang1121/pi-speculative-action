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
				].join("\n"),
			);
			const observation = await observeStrace(prefix, "/usr/bin/example", "/work");
			expect(observation.taints).toEqual(expect.arrayContaining(["network", "clock", "random"]));
		} finally {
			await fs.rm(root, { recursive: true, force: true });
		}
	});
});
