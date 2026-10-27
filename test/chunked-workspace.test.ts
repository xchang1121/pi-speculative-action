import { constants } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { link, lstat, mkdir, open } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, vi } from "vitest";
import { captureStableFile, FILESYSTEM_OBSERVATION_FIELDS } from "../src/filesystem-evidence.ts";
import { disposable } from "./async.ts";
import { cHeaders, executeProcess, processTest, recordProcessPublications, textOutput } from "./linux-process-fixture.ts";

const MiB = 1024 * 1024;
vi.mock("node:fs/promises", { spy: true });
afterEach(() => vi.restoreAllMocks());
const metadata = async (target: string) => {
	const stat = await lstat(target, { bigint: true });
	return Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field, String(stat[field])]));
};

processTest.for([{ megabytes: 2, dense: false, label: "2 MiB" }, { megabytes: 600, dense: false, label: "600 MiB" },
	{ megabytes: 513, dense: true, label: "513 MiB dense" }])("captures and actually reuses native chunk transitions ($label)", { timeout: 180_000 }, async ({ megabytes, dense }, { benchmark }) => {
	const fixture = await benchmark("pi-chunked-workspace-", "git"), size = megabytes * MiB;
	const retainedFiles = async () => {
		const files = await Promise.all((await filesystem.readdir("/proc/self/fd")).map(async fd => {
			const target = `/proc/self/fd/${fd}`, name = await filesystem.readlink(target).catch(() => "");
			if (!name.startsWith(fixture.root + "/") || !/\/#\d+ \(deleted\)$/.test(name)) return;
			const info = await filesystem.stat(target).catch(() => undefined);
			return info?.isFile() && info.nlink === 0 ? { object: `${info.dev}:${info.ino}`, logical: info.size, physical: info.blocks * 512 } : undefined;
		}));
		return files.filter(file => file !== undefined);
	};
	const publications = await recordProcessPublications(fixture);
	await fixture.compile("writer", { code: `#define _GNU_SOURCE\n${cHeaders("fcntl.h unistd.h stdlib.h string.h sys/mman.h")}
int main(int argc, char **argv) {
 if (argc != 3) return 1;
 off_t size = (off_t)strtoll(argv[2], NULL, 10), source = 0, destination = size / 2 + 8192;
 int fd = open(argv[1], O_RDWR); if (fd < 0) return 2;
 if (pwrite(fd, "changed!", 8, 0) != 8 || ftruncate(fd, size / 2) || ftruncate(fd, size)) return 3;
 if (copy_file_range(fd, &source, fd, &destination, 8, 0) != 8) return 4;
 char *mapped = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
 if (mapped == MAP_FAILED) return 5;
 memcpy(mapped + 16, "mapped!", 7);
 if (munmap(mapped, 4096) || pwrite(fd, "footer!", 7, size - 7) != 7 || close(fd)) return 6;
 return write(1, "done\\n", 5) != 5;
}\n` });
	const bare = path.join(fixture.root, "bare"), denseBlock = dense ? randomBytes(MiB) : undefined; await mkdir(bare);
	for (const root of [fixture.workspace, bare]) {
		const handle = await open(path.join(root, "blob"), "wx");
		try {
			await handle.truncate(size);
			if (denseBlock) for (let index = 0; index < megabytes; index++) {
				denseBlock.writeUInt32LE(index); await handle.write(denseBlock, 0, MiB, index * MiB);
			}
			await handle.write(Buffer.from("original"), 0, 8, 0);
			await handle.write(Buffer.from("discarded"), 0, 9, size * 3 / 4);
			await handle.sync();
		} finally { await handle.close(); }
		await link(path.join(root, "blob"), path.join(root, "alias"));
	}
	const expected = execFileSync(path.join(fixture.workspace, "writer"), ["blob", String(size)], { cwd: bare, encoding: "utf8" });
	const oracle = await captureStableFile(path.join(bare, "blob"), size, false);
	const names = [fixture.workspace, path.join(fixture.workspace, "blob"), path.join(fixture.workspace, "alias")];
	const initial = await Promise.all(names.map(metadata));
	const source = await captureStableFile(names[1]!, size, false);
	const held = await open(names[1]!, constants.O_RDONLY | constants.O_NOATIME);
	const capture = fixture.workspaceSandbox.capture.bind(fixture.workspaceSandbox);
	let firstWriteBaseline = false, adopting = false, phase = "prepare";
	fixture.workspaceSandbox.capture = (options, parent) => capture({ ...options, execute: async workspace => {
		const git = spawn("git", ["--git-dir", Reflect.get(workspace, "repository"), "cat-file", "blob", `${Reflect.get(workspace, "commit")}:blob`],
			{ stdio: ["ignore", "pipe", "pipe"] });
		const exited = new Promise<void>((resolve, reject) => { git.once("error", reject); git.once("exit", code => code === 0 ? resolve() : reject(new Error(`baseline Git exited ${code}`))); });
		let bytes = 0; const digest = createHash("sha256");
		for await (const chunk of git.stdout) { digest.update(chunk); bytes += chunk.length; }
		await exited; expect(bytes).toBe(size); expect(digest.digest("hex")).toBe(source.hash);
		firstWriteBaseline = true; return options.execute(workspace);
	} }, parent);
	try {
		await fixture.prepare(); const scope = { sessionID: "chunked-workspace", turnID: "actor" };
		const command = `./writer blob ${size}; : complete`;
		phase = "private execution and capture";
		await using branch = disposable(await fixture.fork(`: producer; ${command}`, { executionScope: scope }));
		expect(textOutput(branch.output.result)).toBe(expected);
		const certificates = await publications(), chunks = new Map<string, number>();
		for (const certificate of certificates) for (const event of certificate.result.journal) if (event.kind === "workspace")
			for (const state of [event.before, event.after]) if (state.kind === "file") {
				expect("chunks" in state.data).toBe(true);
				if ("chunks" in state.data) for (const reference of state.data.chunks) if (reference) chunks.set(reference.digest, reference.size);
			}
		const storedEffectBytes = [...chunks.values()].reduce((a, b) => a + b, 0);
		expect(chunks.size).toBeGreaterThan(0); expect(storedEffectBytes).toBeLessThan((dense ? megabytes + 10 : 10) * MiB);
		if (dense) expect(storedEffectBytes).toBeGreaterThanOrEqual(size);
		expect(await Promise.all(names.map(metadata))).toEqual(initial);
		expect((await captureStableFile(names[1]!, size, false)).hash).toBe(source.hash);
		phase = "validation";
		expect(await branch.validate?.(), JSON.stringify(fixture.backend.metrics())).toMatchObject({ status: "valid" });
		const route = await fixture.prepareActorReplay(), before = fixture.backend.actorMetrics().hits;
		let writtenBytes = 0;
		const native = await vi.importActual<typeof filesystem>("node:fs/promises");
		vi.spyOn(filesystem, "open").mockImplementation(async (...args) => {
			const handle = await native.open(...args), write = handle.write;
			if (names.includes(String(args[0]))) vi.spyOn(handle, "write").mockImplementation(async (...args) => {
				const result = await Reflect.apply(write, handle, args); writtenBytes += result.bytesWritten; return result;
			}); return handle;
		});
		await fixture.inOwner((state, names) => {
			state.writtenBytes = 0; const open = state.fs.open;
			state.method(state.fs, "open", async (...args: any[]) => {
				const handle = await open(...args), write = handle.write;
				if (names.includes(String(args[0]))) state.method(handle, "write", async (...args: unknown[]) => {
					const result = await Reflect.apply(write, handle, args) as { bytesWritten: number }; state.writtenBytes += result.bytesWritten; return result;
				}); return handle;
			});
		}, names);
		adopting = true; phase = "Actor reuse and adoption";
		expect(await executeProcess(fixture, route.executor, `: actor; ${command}`, { scope })).toEqual({ exitCode: 0, output: expected });
		expect(fixture.backend.actorMetrics().hits - before, JSON.stringify(fixture.backend.metrics())).toBeGreaterThan(0);
		writtenBytes += await fixture.inOwner(state => state.writtenBytes as number);
		expect(writtenBytes).toBe((dense ? Math.ceil(megabytes / 2) + 1 : Math.min(4, megabytes)) * MiB);
		const handles = await retainedFiles(), objects = [...new Map(handles.map(file => [file.object, file])).values()];
		expect(objects.length).toBeGreaterThan(0);
		process.stdout.write(`chunked-measurement ${JSON.stringify({ logicalBytes: size, storedEffectBytes, writtenBytes,
			anonymousObjects: objects.length, anonymousDescriptors: handles.length, anonymousLogicalBytes: objects.reduce((sum, file) => sum + file.logical, 0),
			anonymousPhysicalBytes: objects.reduce((sum, file) => sum + file.physical, 0) })}\n`);
		for (const name of names.slice(1)) {
			const actual = await captureStableFile(name, size, false);
			expect(actual.hash).toBe(oracle.hash); expect(actual.stat.ino).toBe(source.stat.ino);
		}
		const prefix = Buffer.alloc(23); await held.read(prefix, 0, prefix.length, 0);
		expect(prefix.subarray(0, 8).toString()).toBe("changed!"); expect(prefix.subarray(16).toString()).toBe("mapped!");
	} catch (error) { throw new Error(`${megabytes} MiB at ${phase}: ${String(error)}; ${JSON.stringify(fixture.backend.metrics())}`, { cause: error }); }
	finally {
		await held.close(); expect(firstWriteBaseline).toBe(true);
		if (!adopting) {
			expect(await Promise.all(names.map(metadata))).toEqual(initial);
			expect((await captureStableFile(names[1]!, size, false)).hash).toBe(source.hash);
		}
		await fixture.dispose(); expect(await retainedFiles()).toEqual([]);
	}
});
