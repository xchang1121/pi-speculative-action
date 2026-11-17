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
import { nativeMetadataTrace, observeStrace, traceTail, writesWithin } from "../src/strace-observer.ts";
import { temporaryDirectories } from "./filesystem.ts";
import { sealProcessCertificate, type ProcessResultRecord } from "../src/process-reuse.ts";
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

async function processTrace(content: string, suffix = "101") {
	const prefix = path.join(await directories.create(), "process");
	const trace = `${prefix}.stream`;
	const tail = traceTail(prefix, "/work");
	const ordered = (text: string) => (suffix === "stream" ? text : text.replace(/^(.+)$/gm, `${suffix} $1`));
	cleanup.push(() => tail[Symbol.asyncDispose]());
	await writeFile(trace, ordered(content));
	return {
		prefix,
		trace,
		tail,
		append: async (content: string) => {
			await filesystem.appendFile(trace, ordered(content));
			return tail.read();
		},
	};
}

test("distinguishes adding a sibling from removing and restoring a pathname binding", async () => {
	const { tail, append } = await processTrace(
		'1.000000 openat(AT_FDCWD, "input", O_RDONLY) = 3</work/input>\n1.000001 openat(AT_FDCWD, "output", O_WRONLY|O_CREAT, 0600) = 4</work/output>\n',
	);
	expect((await tail.read()).unsafe).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/input"]), 0, 2000)).toBe(false);
	expect(writesWithin(tail.writes, new Set(["/work/output"]), 0, 2000)).toBe(true);
	expect((await append('1.000002 rename("/work", "/held") = -1 EACCES (Permission denied)\n')).unsafe).toBe(
		false,
	);
	await append('1.000003 rename("/work", "/held") = 0\n1.000004 rename("/held", "/work") = 0\n');
	expect(writesWithin(tail.writes, new Set(["/work"]), 0, 2000)).toBe(true);
	expect((await tail.read()).writes).toHaveLength(3);
	const shared = await processTrace(
		'101 2.000000 execve("/tools/parent", ["parent"], []) = 0\n102 2.000001 execve("/tools/child", ["child"], []) = 0\n',
		"stream",
	);
	const child = traceTail(shared.tail, "/work", {
		execution: { pid: 102, start: 1 },
	});
	const bytes = "x".repeat(70_000);
	await shared.append(
		'101 2.000002 openat(AT_FDCWD, "foreign", O_WRONLY|O_CREAT, 0600) = 5</work/foreign>\n102 2.000003 write(1<UNIX-STREAM:[21->22,"/proc/self/fd/9/output"]>, ',
	);
	expect((await child.read()).unsafe).toBe(true);
	await shared.append(
		`"${bytes}", ${bytes.length}) = ${bytes.length}\n102 2.000004 openat(AT_FDCWD, "owned", O_WRONLY|O_CREAT, 0600) = 5</work/owned>\n102 2.000005 pi_domain_exit(0) = 0\n`,
	);
	const observation = await observeStrace(child, "/tools/child", "/work", {
		execution: { pid: 102, start: 0 },
		captureOutput: true,
		outputEndpoints: ["socket:[21]", "socket:[23]"],
		guardFilesystemSemanticsWithin: ["/work"],
	});
	expect(observation.complete, observation.incompleteReasons.join(",")).toBe(true);
	expect(observation.output).toEqual([{ fd: 1, data: Buffer.from(bytes) }]);
	expect(observation.written).toEqual(["owned"]);
	expect((await child.read()).unsafe).toBe(false);
	expect(writesWithin(child.writes, new Set(["/work/foreign"]), 0, 3000)).toBe(false);
});

test.each([
	{
		reason: "unmodeled_metadata:getdents64:101",
		calls: ["getdents64(3, [], 4096) = 0"],
	},
	{
		reason: "unparsed_metadata:stat:101",
		calls: ['stat("/work/input", {st_mode=S_IFREG|0644}) = 0'],
	},
	{
		reason: "metadata_changed:/work/input",
		calls: [],
		metadata: { mask: 512, offset: 40 },
	},
	{
		reason: "metadata_changed:/work/accessed",
		calls: [],
		metadata: { mask: 32, offset: 64 },
	},
])(
	"retains incomplete traces and metadata conflicts for transaction qualification ($reason)",
	async ({ calls, reason, metadata }) => {
		const records: Buffer[] = [];
		if (metadata)
			for (const value of [1, 2]) {
				const entry = Buffer.alloc(56);
				const request = Buffer.alloc(80);
				const returned = Buffer.alloc(288);
				entry.writeBigUInt64LE(332n, 8);
				request.writeUInt32LE(101, 8);
				request.writeInt32LE(332, 16);
				returned.writeBigUInt64LE(332n);
				returned.writeUInt32LE(256, 24);
				returned.writeUInt32LE(metadata.mask, 32);
				returned.writeBigUInt64LE(BigInt(value), 32 + metadata.offset);
				for (const [kind, data] of [
					[8, entry],
					[0, request],
					[1, Buffer.from(reason.slice("metadata_changed:".length))],
					[4, Buffer.alloc(24)],
					[6, returned],
				] as const) {
					const record = Buffer.alloc((48 + data.length + 7) & ~7);
					record.writeBigUInt64LE(101n, 8);
					record.writeBigUInt64LE(BigInt(1000 + value) * 1_000_000n, 16);
					record.writeBigUInt64LE(BigInt(value), 24);
					record.writeBigUInt64LE(BigInt(kind), 32);
					record.writeUInt32LE(data.length, 40);
					data.copy(record, 48);
					records.push(record);
				}
			}
		let native = Buffer.concat(records);
		const metadataTrace = nativeMetadataTrace(() => {
			const bytes = native;
			native = Buffer.alloc(0);
			return bytes;
		}, "/private");
		const { prefix } = await processTrace(
			[
				'1.000000@1.000000 execve("/tools/worker", ["worker"], []) = 0',
				'1.000000@1.000001 newfstatat(AT_FDCWD, "/tools/missing", 0x123, 0) = ? ERESTARTSYS (To be restarted if SA_RESTART is set)',
				'1.000000@1.000002 newfstatat(AT_FDCWD, "/tools/missing", 0x123, 0) = -1 ENOENT (No such file or directory)',
				...calls.map((call, index) => `1.00000${index + 1}@1.00${index + 1} ${call}`),
				"1.000009@1.009 +++ exited with 0 +++\n",
			].join("\n"),
		);
		const observed = await observeStrace(prefix, "/tools/worker", "/work", {
			metadata: metadataTrace,
		});
		expect(observed.paths).toContainEqual({
			path: "/tools/missing",
			role: "input",
			lookupFailure: "ENOENT",
		});
		if (reason.startsWith("metadata_changed:")) {
			expect(observed.metadataChanges).toEqual([reason.slice("metadata_changed:".length)]);
			expect(observed.complete).toBe(true);
		} else {
			expect(observed.incompleteReasons).toContain(reason);
			expect(observed.complete).toBe(false);
			expect(observed.taints).toContain("trace_incomplete");
		}
	},
);

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
