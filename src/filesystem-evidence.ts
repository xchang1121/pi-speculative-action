import { createHash, randomUUID } from "node:crypto";
import { constants, type BigIntStats, type Stats } from "node:fs";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import { containsFilesystemPath, slash } from "./path-utils.ts";

const IDENTITY_FIELDS = ["dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "mtimeNs", "ctimeNs"] as const;
export const FILESYSTEM_CONCURRENCY = 12;

/** Bound independent filesystem work and drain every admitted operation before propagating failure. */
export async function mapFilesystem<Input, Output>(
	values: ReadonlyArray<Input>,
	run: (value: Input) => Promise<Output>,
) {
	const output: Output[] = [];
	let cursor = 0;
	const pending = Array.from({ length: Math.min(FILESYSTEM_CONCURRENCY, values.length) }, async () => {
		while (cursor < values.length) {
			const index = cursor++;
			output[index] = await run(values[index]);
		}
	});
	try { await Promise.all(pending); }
	catch (error) { cursor = values.length; await Promise.allSettled(pending); throw error; }
	return output;
}

export type StableFileCapture = {
	readonly hash: string;
	readonly bytesRead: number;
	readonly realPath: string;
	readonly stat: BigIntStats;
	readonly content?: Buffer;
};

export function sameFilesystemIdentity(
	left: BigIntStats,
	right: BigIntStats,
): boolean {
	return IDENTITY_FIELDS.every((field) => left[field] === right[field]);
}

/** Fence workspace timestamps with a private descriptor; elapsed budgets must not use wall time. */
export async function advanceFilesystemClock(
	clock: FileHandle,
	boundary: number,
	identity: Pick<Stats, "dev" | "ino" | "nlink">,
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
export function captureStableFile(
	target: string,
	maxBytes = Number.POSITIVE_INFINITY,
	retainContent = false,
	observed?: Pick<StableFileCapture, "stat" | "realPath">,
): Promise<StableFileCapture> {
	return captureFile(target, maxBytes, retainContent, true, observed);
}

/** Follow executable aliases (including /proc/PID/exe), then hash the complete pinned image. */
export async function hashExecutableFile(target: string, observation?: {
	readonly pinned: () => void; readonly signal: AbortSignal;
}): Promise<`sha256:${string}`> {
	return `sha256:${(await captureFile(target, Infinity, false, false, undefined, observation)).hash}`;
}

async function captureFile(
	target: string,
	maxBytes: number,
	retainContent: boolean,
	verifyPath: boolean,
	observed?: Pick<StableFileCapture, "stat" | "realPath">,
	observation?: { readonly pinned: () => void; readonly signal: AbortSignal },
): Promise<StableFileCapture> {
	// O_PATH pins even executable aliases without admitting I/O on a raced-in FIFO or device.
	const binding = process.platform === "linux" && (process.arch === "x64" || process.arch === "arm64")
		? await fs.open(target, 0x200000 | (verifyPath ? constants.O_NOFOLLOW : 0)) : undefined;
	let handle: FileHandle | undefined;
	try {
		const before = binding ? await binding.stat({ bigint: true })
			: observed?.stat ?? await (verifyPath ? fs.lstat : fs.stat)(target, { bigint: true });
		if (!before.isFile()) throw new Error("not_regular_file");
		if (observed && !sameFilesystemIdentity(observed.stat, before)) throw new Error("file_changed_during_capture");
		const beforePath = verifyPath ? observed?.realPath ?? await fs.realpath(target) : target;
		if (Number.isFinite(maxBytes) && before.size > BigInt(Math.floor(maxBytes))) {
			throw new Error(`file_too_large:${before.size}`);
		}
		handle = await fs.open(binding ? `/proc/self/fd/${binding.fd}` : target,
			constants.O_RDONLY | (binding || !verifyPath ? 0 : constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
		if (!sameFilesystemIdentity(before, await handle.stat({ bigint: true }))) throw new Error("file_changed_during_capture");
		observation?.pinned();
		observation?.signal.throwIfAborted();
		let captured: Omit<StableFileCapture, "realPath">;
		if (verifyPath) captured = await readFileContents(handle, before, maxBytes, retainContent);
		else {
			const key = IDENTITY_FIELDS.map(field => String(before[field])).join(":"), borrower = { signal: observation?.signal };
			let pending = executableCaptures.get(key);
			const joined = !!pending;
			if (!pending) {
				const borrowers = new Set([borrower]), reader = handle;
				pending = { borrowers, result: Promise.resolve().then(() => readFileContents(reader, before, maxBytes, false, () => {
					for (const borrower of borrowers) if (!borrower.signal?.aborted) return;
					borrowers.values().next().value?.signal?.throwIfAborted();
				})).finally(() => { if (executableCaptures.get(key) === pending) executableCaptures.delete(key); }) };
				executableCaptures.set(key, pending);
			} else pending.borrowers.add(borrower);
			try {
				captured = await pending.result;
				if (joined && !sameFilesystemIdentity(before, await handle.stat({ bigint: true }))) throw new Error("file_changed_during_capture");
			} finally { pending.borrowers.delete(borrower); }
		}
		observation?.signal.throwIfAborted();
		const after = captured.stat;
		if (verifyPath) {
			const [afterPath, pathStat] = await Promise.all([fs.realpath(target), fs.lstat(target, { bigint: true })]);
			if (beforePath !== afterPath || !sameFilesystemIdentity(after, pathStat)) throw new Error("file_changed_during_capture");
		}
		return { ...captured, realPath: beforePath };
	} catch (error) { observation?.signal.throwIfAborted(); throw error; } finally {
		try { await handle?.close(); } finally { await binding?.close(); }
	}
}

/** Share only ongoing image reads; every borrower pins and fences its own descriptor. */
const executableCaptures = new Map<string, {
	readonly borrowers: Set<{ readonly signal: AbortSignal | undefined }>;
	readonly result: Promise<Omit<StableFileCapture, "realPath">>;
}>();

async function readFileContents(handle: FileHandle, before: BigIntStats, maxBytes: number, retainContent: boolean, check?: () => void) {
	const hash = createHash("sha256");
	const content = retainContent ? Buffer.allocUnsafe(Number(before.size)) : undefined;
	const buffer = Buffer.allocUnsafe(content ? 1 : Math.max(1, Math.min(Number(before.size), 1024 * 1024)));
	let bytesRead = 0;
	for (;;) {
		check?.();
		const chunk = content && bytesRead < content.length ? content.subarray(bytesRead) : buffer;
		const { bytesRead: size } = await handle.read(chunk);
		if (size === 0) break;
		bytesRead += size;
		if (bytesRead > maxBytes) throw new Error(`file_too_large:${bytesRead}`);
		if (bytesRead > Number(before.size)) throw new Error("file_changed_during_capture");
		hash.update(chunk.subarray(0, size));
	}
	const after = await handle.stat({ bigint: true });
	check?.();
	if (bytesRead !== Number(before.size) || !sameFilesystemIdentity(before, after)) {
		throw new Error("file_changed_during_capture");
	}
	return { hash: hash.digest("hex"), bytesRead, stat: after, ...(content ? { content } : {}) };
}

/** Own a directory listing or link target together with its stable entry identity. */
export async function captureFilesystemEntry(target: string, read?: "directory" | "identity") {
	const before = await fs.lstat(target, { bigint: true });
	const link = before.isSymbolicLink() ? await fs.readlink(target) : undefined;
	const entries = read === "directory" && before.isDirectory() ? await fs.readdir(target, { withFileTypes: true }) : undefined;
	const info = link !== undefined || read !== undefined ? await fs.lstat(target, { bigint: true }) : before;
	if (!sameFilesystemIdentity(before, info)) throw new Error(`${link !== undefined ? "symlink" : "directory"}_changed_during_capture`);
	return { info, link, entries };
}

/** Resolve link targets component by component, retaining each stable namespace observation. */
export async function* walkFilesystemPath(target: string, options: {
	readonly start?: string;
	readonly followFinal?: boolean;
	readonly capture?: typeof captureFilesystemEntry;
} = {}) {
	const start = options.start ?? path.parse(target).root;
	let current = start, links = 0;
	const pending = target.slice(start.length).split(path.sep).filter(Boolean);
	for (;;) {
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>> | undefined;
		try { captured = await (options.capture ?? captureFilesystemEntry)(current); }
		catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const { info, link } = captured ?? {}, terminal = pending.length === 0 && (link === undefined || options.followFinal === false);
		if (links && (!info || terminal)) {
			// Magic links name kernel handles; their displayed pathname need not identify that object.
			const actual = await (options.followFinal === false ? fs.lstat : fs.stat)(target, { bigint: true }).catch((error: NodeJS.ErrnoException) => {
				if (error.code !== "ENOENT") throw error;
			});
			if (info ? !actual || !sameFilesystemIdentity(info, actual) : actual) throw new Error("filesystem_link_resolution_changed");
		}
		yield { path: current, info, link, terminal };
		if (!info || terminal) return;
		if (link !== undefined) {
			if (++links > 40) throw new Error(`resource_symlink_cycle:${target}`);
			const root = path.parse(link).root;
			pending.unshift(...link.slice(root.length).split(path.sep));
			current = root || path.dirname(current);
			continue;
		}
		const component = pending.shift();
		if (component === undefined) return;
		if (!info.isDirectory()) throw new Error("filesystem path component is not a directory");
		current = path.resolve(current, component);
	}
}

export async function assertNoSymlinkPath(root: string, target: string): Promise<void> {
	const resolvedRoot = path.resolve(root), resolvedTarget = path.resolve(target);
	if (!containsFilesystemPath(resolvedRoot, resolvedTarget)) throw new Error(`sandbox path escapes workspace: ${resolvedTarget}`);
	for await (const entry of walkFilesystemPath(resolvedTarget, { start: resolvedRoot })) {
		const first = entry.path === resolvedRoot, info = entry.info;
		if (!info) {
			if (first) throw new Error(`sandbox workspace root does not exist: ${resolvedRoot}`);
			break;
		}
		if (first && (info.isSymbolicLink() || !info.isDirectory())) throw new Error("sandbox workspace root must be a real directory");
		if (info.isSymbolicLink()) throw new Error(`sandbox path contains symlink: ${slash(path.relative(resolvedRoot, entry.path))}`);
		if (!info.isFile() && !info.isDirectory()) throw new Error("sandbox path contains a special file");
	}
}

/** Publish an owned JSON stage without deleting the previous snapshot on failure. */
export async function writeJsonFile(file: string, value: unknown, space?: number): Promise<void> {
	if (value === undefined) return fs.rm(file, { force: true });
	await fs.mkdir(path.dirname(file), { recursive: true });
	const temporary = `${file}.${randomUUID()}.tmp`;
	const handle = await fs.open(temporary, "wx");
	try {
		try { await handle.writeFile(`${JSON.stringify(value, null, space)}\n`, "utf8"); }
		finally { await handle.close(); }
		await fs.rename(temporary, file);
	} finally { await fs.rm(temporary, { force: true }).catch(() => undefined); }
}
