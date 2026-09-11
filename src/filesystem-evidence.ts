import { createHash } from "node:crypto";
import { constants } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, slash } from "./path-utils.ts";

const IDENTITY_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "mtimeNs", "ctimeNs"] as const;

export type StableFileCapture = {
	readonly hash: string;
	readonly bytesRead: number;
	readonly realPath: string;
	readonly stat: import("node:fs").BigIntStats;
	readonly content?: Buffer;
};

export function sameFilesystemIdentity(
	left: import("node:fs").BigIntStats,
	right: import("node:fs").BigIntStats,
): boolean {
	return IDENTITY_FIELDS.every((field) => left[field] === right[field]);
}

/** Fence workspace timestamps with a private descriptor; elapsed budgets must not use wall time. */
export async function advanceFilesystemClock(
	clock: import("node:fs/promises").FileHandle,
	boundary: number,
	identity: Pick<import("node:fs").Stats, "dev" | "ino" | "nlink">,
): Promise<void> {
	const deadline = performance.now() + 100;
	const stamp = async () => {
		const current = await clock.stat();
		if (!current.isFile() || current.dev !== identity.dev || current.ino !== identity.ino || current.nlink !== identity.nlink) {
			throw new Error("workspace transaction clock identity changed");
		}
		return current.ctimeMs;
	};
	for (let sequence = 0; ; sequence++) {
		await stamp();
		await clock.truncate(0);
		await clock.write(`${sequence}\n`, 0, "utf8");
		const current = await stamp();
		if (current > boundary) return;
		if (performance.now() >= deadline) throw new Error(`filesystem change clock did not advance: boundary=${boundary}, clock=${current}`);
		await new Promise<void>((resolve) => setTimeout(resolve, 1));
	}
}

/** One regular-file identity owns admission, descriptor reads, and the final path proof. */
export async function captureStableFile(
	target: string,
	maxBytes = Number.POSITIVE_INFINITY,
	retainContent = false,
	observed?: Pick<StableFileCapture, "stat" | "realPath">,
): Promise<StableFileCapture> {
	// Linux O_PATH pins the inode without opening a raced-in FIFO or device for I/O.
	const binding = process.platform === "linux" && (process.arch === "x64" || process.arch === "arm64")
		? await fs.open(target, 0x200000 | constants.O_NOFOLLOW) : undefined;
	let handle: import("node:fs/promises").FileHandle | undefined;
	try {
		const before = binding ? await binding.stat({ bigint: true }) : observed?.stat ?? await fs.lstat(target, { bigint: true });
		if (!before.isFile()) throw new Error("not_regular_file");
		if (observed && !sameFilesystemIdentity(observed.stat, before)) throw new Error("file_changed_during_capture");
		const beforePath = observed?.realPath ?? await fs.realpath(target);
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes))) {
			throw new Error(`file_too_large:${before.size}`);
		}
		handle = await fs.open(binding ? `/proc/self/fd/${binding.fd}` : target,
			constants.O_RDONLY | (binding ? 0 : constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
		if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true }))) throw new Error("file_changed_during_capture");

		const hash = createHash("sha256");
		const content = retainContent ? Buffer.allocUnsafe(Number(before.size)) : undefined;
		const buffer = Buffer.allocUnsafe(content ? 1 : Math.max(1, Math.min(Number(before.size), 1024 * 1024)));
		let bytesRead = 0;
		for (;;) {
			const chunk = content && bytesRead < content.length ? content.subarray(bytesRead) : buffer;
			const { bytesRead: size } = await handle.read(chunk);
			if (size === 0) break;
			bytesRead += size;
			if (bytesRead > maxBytes) throw new Error(`file_too_large:${bytesRead}`);
			if (bytesRead > Number(before.size)) throw new Error("file_changed_during_capture");
			hash.update(chunk.subarray(0, size));
		}

		const after = await handle.stat({ bigint: true });
		const [afterPath, pathStat] = await Promise.all([fs.realpath(target), fs.lstat(target, { bigint: true })]);
		if (
			bytesRead !== Number(before.size) ||
			beforePath !== afterPath ||
			!sameFilesystemIdentity(before, after) ||
			!sameFilesystemIdentity(after, pathStat)
		) {
			throw new Error("file_changed_during_capture");
		}
		return { hash: hash.digest("hex"), bytesRead, realPath: afterPath, stat: after,
			...(content ? { content } : {}) };
	} finally {
		try { await handle?.close(); } finally { await binding?.close(); }
	}
}

export async function assertNoSymlinkPath(root: string, target: string): Promise<void> {
	const resolvedRoot = path.resolve(root);
	const resolvedTarget = path.resolve(target);
	if (!containsFilesystemPath(resolvedRoot, resolvedTarget)) {
		throw new Error(`sandbox path escapes workspace: ${resolvedTarget}`);
	}
	try {
		const rootInfo = await fs.lstat(resolvedRoot);
		if (rootInfo.isSymbolicLink() || !rootInfo.isDirectory()) {
			throw new Error("sandbox workspace root must be a real directory");
		}
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") throw new Error(`sandbox workspace root does not exist: ${resolvedRoot}`, { cause: error });
		throw error;
	}
	const relative = path.relative(resolvedRoot, resolvedTarget);
	let current = resolvedRoot;
	for (const segment of relative === "" ? [] : relative.split(path.sep)) {
		current = path.join(current, segment);
		try {
			const stats = await fs.lstat(current);
			if (stats.isSymbolicLink()) {
				throw new Error(`sandbox path contains symlink: ${slash(path.relative(resolvedRoot, current))}`);
			}
			if (!stats.isFile() && !stats.isDirectory()) throw new Error("sandbox path contains a special file");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") break;
			throw error;
		}
	}
}
