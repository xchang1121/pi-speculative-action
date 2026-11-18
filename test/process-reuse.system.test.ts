import { writeFile } from "node:fs/promises";
import * as filesystem from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test, vi } from "vitest";
import { sha256Digest } from "../src/process-reuse.ts";
import {
	cachedCapture,
	captureFileDigest,
	captureStableFile,
	hashExecutableFile,
} from "../src/filesystem-evidence.ts";
import { executionRecords, observeExecution, traceTail, writesWithin } from "../src/strace-observer.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { effectCommitFailure } from "../src/execution-world.ts";
import {
	ProcessHandoffOwnership,
	sealProcessCertificate,
	type ProcessResultRecord,
} from "../src/process-reuse.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";

vi.mock("node:fs/promises", { spy: true });
const directories = temporaryDirectories("pi-reuse-system-");
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
	await Promise.all(cleanup.splice(0).map((close) => close()));
	vi.restoreAllMocks();
	await directories.dispose();
});

test("rejects malformed process exit outcomes at the public certificate boundary", () => {
	for (const exit of [
		{ kind: "code", code: -1 },
		{ kind: "code", code: 256 },
		{ kind: "signal", signal: 0, coreDumped: false },
		{ kind: "signal", signal: 65, coreDumped: false },
		{ kind: "signal", signal: 9 },
		{ kind: "unknown", code: 0 },
	])
		expect(() =>
			sealProcessCertificate({
				...PROCESS_CERTIFICATE,
				result: { ...PROCESS_CERTIFICATE.result, exit } as ProcessResultRecord,
			}),
		).toThrow("invalid process exit outcome");
});

function processTrace() {
	const frames: Buffer[] = [];
	const native = executionRecords(() => Buffer.concat(frames.splice(0)), "/private");
	const tail = traceTail(native, "/work");
	cleanup.push(() => tail[Symbol.asyncDispose]());
	let id = 0;
	let stamp = 0;
	const frame = (kind: number, key: number, data: Buffer, pid = 101) => {
		const record = Buffer.alloc((48 + data.length + 7) & ~7);
		record.writeBigUInt64LE(1n);
		record.writeBigUInt64LE(BigInt(pid), 8);
		record.writeBigUInt64LE(BigInt(++stamp) * 1000n, 16);
		record.writeBigUInt64LE(BigInt(key), 24);
		record.writeBigUInt64LE(BigInt(kind), 32);
		record.writeUInt32LE(data.length, 40);
		data.copy(record, 48);
		frames.push(record);
	};
	const begin = (
		nr: number,
		layout: number,
		args: number[],
		paths: string[],
		anchor: number | { inode: number; mode: number } = 0,
		pid = 101,
	) => {
		const key = ++id;
		const data = Buffer.alloc(104);
		data.writeUInt32LE(nr);
		data.writeUInt32LE(layout, 4);
		args.forEach((arg, i) => data.writeBigInt64LE(BigInt(arg), 8 + 8 * i));
		if (anchor) {
			const { inode, mode } = typeof anchor === "number" ? { inode: anchor, mode: 0xc000 } : anchor;
			data.writeBigUInt64LE(BigInt(inode), 56);
			data.writeUInt16LE(mode, 68);
		}
		frame(16, key, data, pid);
		paths.forEach((path, index) => frame(17 + index, key, Buffer.from(path + "\0"), pid));
		return key;
	};
	const finish = (key: number, result: number, pid = 101, inode = 0, mode = 0) => {
		const data = Buffer.alloc(64);
		data.writeBigInt64LE(BigInt(result));
		data.writeBigUInt64LE(BigInt(inode), 8);
		data.writeUInt16LE(mode, 20);
		frame(20, key, data, pid);
	};
	const image = (name: string, pid = 101) => {
		const head = Buffer.alloc(32);
		head.writeUInt32LE(pid, 24);
		frame(14, ++id, Buffer.concat([head, Buffer.from(name + "\0")]), pid);
	};
	const exit = (pid = 101) => frame(15, ++id, Buffer.alloc(4), pid);
	return { frame, tail, begin, finish, image, exit };
}

test.for([undefined, "/tmp/other/socket", "/tmp/pi-step-test/socket"])(
	"only the authenticated subcompute endpoint bypasses socket taint (%s)",
	async (controlledSocket) => {
		const { frame, tail, begin, finish, image, exit } = processTrace();
		image("/tools/client");
		finish(begin(41, 1, [1, 1], []), 3);
		const connect = begin(42, 0x1001, [3], [], 31);
		frame(22, connect, Buffer.concat([Buffer.from([1, 0]), Buffer.from("/tmp/pi-step-test/socket\0")]));
		finish(connect, 0);
		finish(begin(1, 0x1001, [3], [], 31), 7);
		finish(begin(0, 0x1001, [3], [], 31), 5);
		exit();
		const observation = await observeExecution(tail, "/tools/client", "/work", { controlledSocket });
		const authenticated = controlledSocket === "/tmp/pi-step-test/socket";
		expect(observation.taints.includes("network")).toBe(!authenticated);
		if (authenticated) {
			expect(observation.complete, observation.incompleteReasons.join(",")).toBe(true);
			expect(observation.paths.some((input) => input.path.includes("pi-step-test"))).toBe(false);
		}
	},
);

test("distinguishes adding a sibling from removing and restoring a pathname binding", async () => {
	const { tail, begin, finish, image, frame, exit } = processTrace();
	finish(begin(257, 0x1021, [-100, 0, 0], ["input"]), 3);
	finish(begin(257, 0x1021, [-100, 0, 65], ["output"]), 4);
	expect((await tail.read()).unsafe).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 0, Infinity)).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/output"]), 0, Infinity)).toBe(true);
	finish(begin(82, 0x211, [], ["/work", "/held"]), -13);
	expect((await tail.read()).unsafe).toBe(false);
	finish(begin(82, 0x211, [], ["/work", "/held"]), 0);
	finish(begin(82, 0x211, [], ["/held", "/work"]), 0);
	expect((await tail.read()).writes).toHaveLength(3);
	expect(writesWithin(tail.writes, new Set(["/work"]), 0, Infinity)).toBe(true);

	image("/tools/parent");
	image("/tools/child", 102);
	const child = traceTail(tail, "/work", { execution: { pid: 102, start: 1 } });
	finish(begin(257, 0x1021, [-100, 0, 65], ["foreign"]), 5);
	const data = Buffer.alloc(70_000, "x");
	const writing = begin(1, 0x1001, [1, 0, data.length], [], 21, 102);
	expect((await child.read()).unsafe).toBe(true);
	for (let offset = 0; offset < data.length; offset += 4096)
		frame(21, writing, data.subarray(offset, offset + 4096), 102);
	finish(writing, data.length, 102);
	finish(begin(257, 0x1021, [-100, 0, 65], ["owned"], 0, 102), 5, 102);
	exit(102);
	const observation = await observeExecution(child, "/tools/child", "/work", {
		execution: { pid: 102, start: 0 },
		captureOutput: true,
		outputEndpoints: ["socket:[21]", "socket:[23]"],
		guardFilesystemSemanticsWithin: ["/work"],
	});
	expect(observation.complete, observation.incompleteReasons.join(",")).toBe(true);
	expect(observation.output).toEqual([{ fd: 1, data }]);
	expect(observation.written).toEqual(["owned"]);
	expect((await child.read()).unsafe).toBe(false);
	expect(writesWithin(child.writes, new Set(["/work/foreign"]), 0, Infinity)).toBe(false);
});

test.each([
	{ reason: "unmodeled_metadata:getdents64:101" },
	{ reason: "unparsed_metadata:stat:101" },
	{ reason: "metadata_changed:/work/input", metadata: { mask: 512, offset: 40 } },
	{ reason: "metadata_changed:/work/accessed", metadata: { mask: 32, offset: 64 } },
])(
	"retains incomplete traces and metadata conflicts for transaction qualification ($reason)",
	async ({ reason, metadata }) => {
		const { tail, frame, image, begin, finish, exit } = processTrace();
		image("/tools/worker");
		for (const result of [-512, -2]) finish(begin(262, 0x1021, [-100, 0, 0, 0], ["/tools/missing"]), result);
		if (metadata) {
			for (const value of [1, 2]) {
				const key = begin(332, 0x1021, [-100, 0, 0, metadata.mask], ["/work/alias"]);
				const request = Buffer.alloc(80);
				request.writeUInt32LE(101, 8);
				request.writeInt32LE(332, 16);
				frame(0, key, request);
				frame(1, key, Buffer.from(reason.slice("metadata_changed:".length)));
				frame(4, key, Buffer.alloc(24));
				const returned = Buffer.alloc(288);
				returned.writeBigUInt64LE(332n);
				returned.writeUInt32LE(256, 24);
				returned.writeUInt32LE(metadata.mask, 32);
				returned.writeBigUInt64LE(BigInt(value), 32 + metadata.offset);
				frame(6, key, returned);
			}
		} else if (reason.includes("getdents64")) {
			finish(begin(217, 0x1001, [3], []), 0);
		} else {
			finish(begin(4, 0x11, [], ["/work/input"]), 0);
		}
		exit();
		const observed = await observeExecution(tail, "/tools/worker", "/work");
		expect(observed.paths).toContainEqual({ path: "/tools/missing", role: "input", lookupFailure: "ENOENT" });
		if (metadata) {
			expect(observed.metadataChanges).toEqual([reason.slice("metadata_changed:".length)]);
			expect(observed.complete).toBe(true);
		} else {
			expect(observed.incompleteReasons).toContain(reason);
			expect(observed.complete).toBe(false);
			expect(observed.taints).toContain("trace_incomplete");
		}
	},
);

test("keeps lookup errno, cwd and the forked child's exit proof in one journal", async () => {
	const { frame, tail, begin, finish, image, exit } = processTrace();
	image("/tools/walk");
	finish(begin(80, 0x11, [], ["/work/inside"]), 0);
	finish(begin(257, 0x1021, [-100], ["subdirectory"]), 3, 101, 41, 0x4000);
	const fork = Buffer.alloc(16);
	fork.writeUInt32LE(102);
	fork.writeUInt32LE(102, 4);
	frame(13, 41, fork);
	finish(begin(262, 0x1021, [3, 0, 0, 0], ["regular/missing"], { inode: 41, mode: 0x4000 }, 102), -20, 102);
	finish(begin(262, 0x1021, [-100, 0, 0, 0], ["absent"], 0, 102), -2, 102);
	exit();
	const pending = await observeExecution(tail, "/tools/walk", "/work");
	expect(pending.incompleteReasons).toContain("process_exit_unproven:102");
	exit(102);
	const complete = await observeExecution(tail, "/tools/walk", "/work");
	expect(complete.complete).toBe(true);
	expect(complete.paths).toEqual(
		expect.arrayContaining([
			{ path: "/work/inside/subdirectory/regular/missing", role: "input", lookupFailure: "ENOTDIR" },
			{ path: "/work/inside/absent", role: "input", lookupFailure: "ENOENT" },
		]),
	);
});

test("marks a successful unmodeled file ioctl as unsupported even when it returns a value", async () => {
	const { tail, begin, finish, image, exit } = processTrace();
	image("/tools/device");
	finish(begin(16, 0x1001, [3, 0x1234], [], { inode: 51, mode: 0x8000 }), 1);
	exit();
	const observation = await observeExecution(tail, "/tools/device", "/work");
	expect(observation.taints).toContain("unsupported_syscall");
	expect(observation.unsupportedSyscalls).toEqual(["ioctl"]);
	expect((await tail.read()).unsafe).toBe(true);
});

test("uses the kernel order and returned length for writes through output aliases", async () => {
	const { frame, tail, begin, finish, image, exit } = processTrace();
	image("/tools/emit");
	for (const [socket, fd, bytes] of [
		[21, 7, "one"],
		[23, 2, "two"],
		[21, 1, "three"],
	] as const) {
		const key = begin(20, 0x1001, [fd], [], socket);
		frame(21, key, Buffer.from(bytes));
		finish(key, bytes.length);
	}
	exit();
	const result = await observeExecution(tail, "/tools/emit", "/work", {
		captureOutput: true,
		outputEndpoints: ["socket:[21]", "socket:[23]"],
	});
	expect(result.complete).toBe(true);
	expect(result.output?.map(({ fd, data }) => [fd, data.toString()])).toEqual([
		[1, "one"],
		[2, "two"],
		[1, "three"],
	]);
});

test("rejects orphan and truncated binary records before they can qualify an execution", () => {
	const orphan = Buffer.alloc(48);
	orphan.writeBigUInt64LE(1n, 8);
	orphan.writeBigUInt64LE(1n, 24);
	orphan.writeBigUInt64LE(20n, 32);
	for (const bytes of [orphan, orphan.subarray(0, 47)]) {
		const records = executionRecords(() => bytes);
		expect(() => records.read()).toThrow();
		expect(() => records.read()).toThrow();
	}
});

test("keeps file admission and change detection after a digest is cached", async () => {
	const input = path.join(await directories.create(), "input");
	await writeFile(input, "original");
	await new Promise((resolve) => setTimeout(resolve, 2100)); // Settle even a filesystem with coarse timestamps.
	const bytes = await captureStableFile(input, 1024, true, { digest: false });
	expect(bytes.content?.toString()).toBe("original");
	expect(cachedCapture(bytes.stat, input)).toBeUndefined();
	const captured = await captureStableFile(input, 1024);
	expect(await captureFileDigest(input)).toMatchObject({
		hash: captured.hash,
		shared: true,
	});
	if (process.platform === "linux") {
		const alias = `${input}-alias`;
		await filesystem.symlink(input, alias);
		await expect(captureFileDigest(alias)).rejects.toThrow("not_regular_file");
		expect(await hashExecutableFile(alias)).toBe(`sha256:${captured.hash}`);
	}
	await writeFile(input, "modified");
	expect((await captureFileDigest(input)).hash).toBe(sha256Digest("modified").slice("sha256:".length));
	await expect(captureFileDigest(input, 7)).rejects.toThrow("file_too_large");
	await expect(captureFileDigest(input, 1024, AbortSignal.abort(new Error("cancelled")))).rejects.toThrow(
		"cancelled",
	);
});

test("a whole handoff waits for all child claims and only retries recoverable commits", async () => {
	const owner = new ProcessHandoffOwnership();
	const first = owner.claimChild()!;
	const second = owner.claimChild()!;
	first(false);
	first(true);
	await expect(owner.commit(async () => 0)).rejects.toThrow("already claimed");
	second(false);
	await expect(
		owner.commit(async () => {
			throw effectCommitFailure(new Error("stale"), "recoverable");
		}),
	).rejects.toThrow();
	const result = Promise.withResolvers<number>();
	const apply = vi.fn(() => result.promise);
	const runs = [owner.commit(apply), owner.commit(apply)];
	expect(owner.claimChild()).toBeUndefined();
	expect(owner.pendingWhole).toBeDefined();
	result.resolve(42);
	expect(await Promise.all(runs)).toEqual([42, 42]);
	expect(apply).toHaveBeenCalledTimes(1);
	expect(owner.pendingWhole).toBeUndefined();
	expect(owner.wholeClaimed).toBe(true);
	const partial = new ProcessHandoffOwnership();
	partial.claimChild()!(true);
	await expect(partial.commit(apply)).rejects.toThrow("partially consumed");
});
