import { createHash } from "node:crypto";
import { constants } from "node:fs";
import fs from "node:fs/promises";

const IDENTITY_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "mtimeNs", "ctimeNs"] as const;

export type StableFileCapture = {
	readonly hash: string;
	readonly bytesRead: number;
	readonly realPath: string;
	readonly stat: import("node:fs").BigIntStats;
};

export function sameFilesystemIdentity(
	left: import("node:fs").BigIntStats,
	right: import("node:fs").BigIntStats,
): boolean {
	return IDENTITY_FIELDS.every((field) => left[field] === right[field]);
}

/** Hash one regular file through a single descriptor and prove its path still names that descriptor. */
export async function captureStableFile(
	target: string,
	maxBytes = Number.POSITIVE_INFINITY,
): Promise<StableFileCapture> {
	const beforePath = await fs.realpath(target);
	const handle = await fs.open(target, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	try {
		const before = await handle.stat({ bigint: true });
		if (!before.isFile()) throw new Error("not_regular_file");
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes))) {
			throw new Error(`file_too_large:${before.size}`);
		}

		const hash = createHash("sha256");
		let bytesRead = 0;
		for await (const chunk of handle.createReadStream({ autoClose: false })) {
			bytesRead += chunk.byteLength;
			if (bytesRead > maxBytes) throw new Error(`file_too_large:${bytesRead}`);
			hash.update(chunk);
		}

		const after = await handle.stat({ bigint: true });
		const [afterPath, pathStat] = await Promise.all([fs.realpath(target), fs.lstat(target, { bigint: true })]);
		if (
			beforePath !== afterPath ||
			!sameFilesystemIdentity(before, after) ||
			pathStat.isSymbolicLink() ||
			pathStat.dev !== after.dev ||
			pathStat.ino !== after.ino
		) {
			throw new Error("file_changed_during_capture");
		}
		return { hash: hash.digest("hex"), bytesRead, realPath: afterPath, stat: after };
	} finally {
		await handle.close();
	}
}
