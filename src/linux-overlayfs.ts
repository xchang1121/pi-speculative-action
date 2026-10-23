import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { link, lstat, mkdir, mkdtemp, open, readFile, rename, rm, writeFile, type FileHandle } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BoundedRecencyMap } from "./bounded-recency-map.ts";
import { execHostText, resolveHostExecutable } from "./executable-path.ts";
import { advanceFilesystemClock, hashExecutableFile, mapFilesystem } from "./filesystem-evidence.ts";
import { errorMessage, isMissing } from "./error-utils.ts";
import { waitForCompletion } from "./runtime-lifecycle.ts";

const OVERLAY_OPTIONS_EPOCH = "fuse-overlayfs-hardlinks-cow";
const OVERLAY_READY_TIMEOUT_MS = 5_000;
const OVERLAY_EXIT_TIMEOUT_MS = 2_000;
const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
const DEFAULT_REQUEST_CACHE_CAPACITY = 32;
const DEFAULT_RESOLVED_CACHE_CAPACITY = 8;
const DEFAULT_DEGRADED_CAPACITY = 16;
const DEFAULT_NEGATIVE_CACHE_TTL_MS = 30_000;
// asm-generic/fcntl.h: __O_TMPFILE (020000000) | O_DIRECTORY (00200000).
const LINUX_O_TMPFILE = 0o20200000;

export interface LinuxOverlayfsOptions { readonly overlayfsBinary?: string; readonly fusermountBinary?: string; }

export type LinuxOverlayfsCapability =
	| { readonly available: true; readonly binary: string; readonly fusermountBinary: string; readonly fingerprint: string; readonly detail: string; }
	| { readonly available: false; readonly detail: string; };

export interface LinuxOverlayfsMount {
	readonly root: string;
	readonly upperRoot: string;
	readonly workRoot: string;
	readonly close: () => Promise<void>;
}

/** The backing directories must remain alive because a FUSE mount could not be proven closed. */
export class LinuxOverlayfsUnsafeCleanupError extends Error {
	constructor(message: string, cause: unknown) { super(message, { cause }); this.name = "LinuxOverlayfsUnsafeCleanupError"; }
}

/** Open an unnamed regular inode in private backing storage without exposing a control path. */
export async function openLinuxAnonymousWorkspaceFile(root: string): Promise<FileHandle> {
	if (process.platform !== "linux") throw new Error("anonymous workspace files require Linux");
	return open(root, fsConstants.O_WRONLY | LINUX_O_TMPFILE, 0o600);
}

type ResolvedOverlayfs = Omit<Extract<LinuxOverlayfsCapability, { readonly available: true }>, "available">;

interface LinuxOverlayfsCapabilityCacheEntry { readonly pending: Promise<LinuxOverlayfsCapability>; expiresAt?: number; }

/** Lifecycle-owned, bounded capability memoization with fail-closed driver quarantine. */
export class LinuxOverlayfsCapabilityRegistry {
	private readonly requests = new BoundedRecencyMap<string, LinuxOverlayfsCapabilityCacheEntry>(DEFAULT_REQUEST_CACHE_CAPACITY);
	private readonly resolved = new BoundedRecencyMap<string, LinuxOverlayfsCapabilityCacheEntry>(DEFAULT_RESOLVED_CACHE_CAPACITY);
	private readonly degraded = new Map<string, string>();
	private disabledDetail?: string;
	private disposed = false;

	async capability(options: LinuxOverlayfsOptions = {}): Promise<LinuxOverlayfsCapability> {
		if (this.disposed) throw new Error("Linux OverlayFS capability registry is disposed");
		if (this.disabledDetail) return { available: false, detail: this.disabledDetail };
		const capability = await this.memoized(this.requests, `${options.overlayfsBinary ?? "auto"}\0${options.fusermountBinary ?? "auto"}`, async () => {
			if (process.platform !== "linux") return { available: false, detail: "Linux host required for OverlayFS" };
			let resolved: ResolvedOverlayfs;
			try { resolved = await resolveOverlayfs(options); } catch (error) { return { available: false, detail: errorMessage(error) }; }
			return this.memoized(this.resolved, availableCapabilityKey(resolved), () => probeLinuxOverlayfs(resolved));
		});
		const detail = this.disabledDetail ?? (capability.available ? this.degraded.get(availableCapabilityKey(capability)) : undefined);
		return detail ? { available: false, detail } : capability;
	}

	markDegraded(capability: Extract<LinuxOverlayfsCapability, { readonly available: true }>, detail: string): void {
		if (this.disposed || this.disabledDetail) return;
		const key = availableCapabilityKey(capability);
		const disabled = `fuse-overlayfs driver disabled: ${detail}`;
		if (!this.degraded.has(key) && this.degraded.size >= DEFAULT_DEGRADED_CAPACITY) {
			this.disabledDetail = `fuse-overlayfs registry disabled after ${DEFAULT_DEGRADED_CAPACITY + 1} driver degradations`;
			this.requests.clear();
			this.resolved.clear();
			this.degraded.clear();
			return;
		}
		this.degraded.set(key, disabled);
	}

	dispose(): void { if (this.disposed) return; this.disposed = true; this.requests.clear(); this.resolved.clear(); this.degraded.clear(); }

	private memoized(
		cache: BoundedRecencyMap<string, LinuxOverlayfsCapabilityCacheEntry>,
		key: string,
		load: () => Promise<LinuxOverlayfsCapability>,
	): Promise<LinuxOverlayfsCapability> {
		const now = Date.now();
		const cached = cache.get(key);
		if (cached && (cached.expiresAt === undefined || cached.expiresAt > now)) return cached.pending;
		if (cached) cache.delete(key);
		const entry: LinuxOverlayfsCapabilityCacheEntry = { pending: Promise.resolve().then(load) };
		cache.set(key, entry);
		void entry.pending.then(
			(capability) => { if (!capability.available) entry.expiresAt = Date.now() + DEFAULT_NEGATIVE_CACHE_TTL_MS; },
			() => { entry.expiresAt = Date.now() + DEFAULT_NEGATIVE_CACHE_TTL_MS; },
		);
		return entry.pending;
	}
}

const defaultCapabilityRegistry = new LinuxOverlayfsCapabilityRegistry();

/** Probe the complete mount/read/copy-up/whiteout/unmount lifecycle; version output alone is insufficient. */
export function linuxOverlayfsCapability(
	options: LinuxOverlayfsOptions = {},
): Promise<LinuxOverlayfsCapability> {
	return defaultCapabilityRegistry.capability(options);
}

/** Mount a private copy-on-write view in the caller's mount namespace. */
export async function mountLinuxOverlayfs(input: {
	readonly lowerRoot: string;
	readonly privateRoot: string;
	readonly options?: LinuxOverlayfsOptions;
	readonly capabilityRegistry?: LinuxOverlayfsCapabilityRegistry;
}): Promise<LinuxOverlayfsMount> {
	const capabilityRegistry = input.capabilityRegistry ?? defaultCapabilityRegistry;
	const capability = await capabilityRegistry.capability(input.options);
	if (!capability.available) throw new Error(capability.detail);
	return startLinuxOverlayfs({ ...capability, ...input, onDegraded: detail => capabilityRegistry.markDegraded(capability, detail) });
}

async function probeLinuxOverlayfs(resolved: ResolvedOverlayfs): Promise<LinuxOverlayfsCapability> {
	const probeRoot = await mkdtemp(path.join(os.tmpdir(), "pi-fuse-overlayfs-probe-"));
	const lowerRoot = path.join(probeRoot, "lower");
	const privateRoot = path.join(probeRoot, "private");
	const marker = randomUUID();
	let mounted: LinuxOverlayfsMount | undefined;
	let safeToRemove = true;
	let degradedDetail: string | undefined;
	let failure: string | undefined;
	try {
		await mkdir(lowerRoot);
		await mapFilesystem([["copy-up.txt", `${marker}\n`], ["whiteout.txt", "remove\n"]],
			([name, content]) => writeFile(path.join(lowerRoot, name), content, "utf8"));
		await link(path.join(lowerRoot, "copy-up.txt"), path.join(lowerRoot, "copy-up-alias.txt"));
		await mkdir(path.join(lowerRoot, "replaced"));
		await writeFile(path.join(lowerRoot, "replaced", "lower.txt"), "lower\n", "utf8");
		mounted = await startLinuxOverlayfs({ ...resolved, lowerRoot, privateRoot, onDegraded: detail => { degradedDetail = detail; } });
		const { root, upperRoot } = mounted;
		await expectContent(path.join(root, "copy-up.txt"), `${marker}\n`);
		const [lowerIdentity, mountedIdentity, mountedRoot] = await Promise.all([
			lstat(path.join(lowerRoot, "copy-up.txt"), { bigint: true }), lstat(path.join(root, "copy-up.txt"), { bigint: true }), lstat(root, { bigint: true }),
		]);
		if (lowerIdentity.ino !== mountedIdentity.ino || mountedRoot.dev !== mountedIdentity.dev || mountedIdentity.nlink !== 2n) throw new Error("OverlayFS inode projection is unavailable");
		await rename(path.join(root, "copy-up.txt"), path.join(root, "copy-up-alias.txt"));
		// Open descriptors survive whiteouts and removals, with no remaining visible links.
		await mapFilesystem(["whiteout.txt", "replaced"], async name => {
			const target = path.join(root, name);
			await using held = await open(target, fsConstants.O_RDONLY);
			await rm(target, { recursive: true });
			if ((await held.stat()).nlink !== 0) throw new Error("OverlayFS retained an unlinked object's link count");
		});
		await mapFilesystem([["copy-up.txt", "changed\n"], ["created.txt", "created\n"]],
			([name, content]) => writeFile(path.join(root, name), content));
		await mkdir(path.join(root, "replaced"));
		await writeFile(path.join(root, "replaced", "created.txt"), "created\n", "utf8");
		await expectContent(path.join(lowerRoot, "copy-up.txt"), `${marker}\n`);
		await expectContent(path.join(root, "copy-up.txt"), "changed\n");
		await expectContent(path.join(root, "copy-up-alias.txt"), "changed\n");
		const copiedIdentity = await lstat(path.join(root, "copy-up.txt"), { bigint: true });
		if (copiedIdentity.dev !== mountedIdentity.dev || copiedIdentity.ino !== mountedIdentity.ino) throw new Error("OverlayFS copy-up changed object identity");
		{
			await using clock = await openLinuxAnonymousWorkspaceFile(upperRoot);
			const [workspace, anonymous, ...beforeEntries] = await Promise.all([lstat(root), clock.stat(),
				...["copy-up.txt", "created.txt", "replaced"].map(name => lstat(path.join(root, name)))]);
			if (!workspace.isDirectory() || !anonymous.isFile() || anonymous.nlink !== 0 ||
				(await lstat(upperRoot)).dev !== anonymous.dev)
				throw new Error("OverlayFS backing roots do not share an anonymous transaction-clock filesystem");
			const priorBoundary = Math.max(workspace.ctimeMs, ...beforeEntries.map((entry) => entry.ctimeMs));
			await advanceFilesystemClock(clock, priorBoundary, anonymous);
			const orderProbe = path.join(root, "clock-order-probe.txt");
			await writeFile(orderProbe, "ordered\n", "utf8");
			const [changedRoot, changedFile] = await Promise.all([lstat(root), lstat(orderProbe)]);
			if (changedRoot.ctimeMs <= priorBoundary || changedFile.ctimeMs <= priorBoundary) {
				throw new Error("OverlayFS merged timestamps are not ordered by the private backing clock");
			}
			await advanceFilesystemClock(clock, Math.max(changedRoot.ctimeMs, changedFile.ctimeMs), anonymous);
			await rm(orderProbe);
		}
		const removed = await lstat(path.join(root, "whiteout.txt")).catch(error => { if (!isMissing(error)) throw error; });
		if (removed) throw new Error("OverlayFS whiteout remained visible");
		const whiteout = await lstat(path.join(upperRoot, "whiteout.txt"));
		if (!whiteout.isCharacterDevice() || whiteout.rdev !== 0) throw new Error("OverlayFS whiteout encoding is unsupported");
		if (!(await lstat(path.join(upperRoot, "replaced", ".wh..wh..opq"))).isFile()) {
			throw new Error("OverlayFS opaque-directory encoding is unsupported");
		}
	} catch (error) {
		safeToRemove = !(error instanceof LinuxOverlayfsUnsafeCleanupError);
		failure = `fuse-overlayfs probe failed: ${errorMessage(error)}`;
	} finally {
		try { await mounted?.close(); } catch (error) {
			safeToRemove = false;
			failure = `fuse-overlayfs probe cleanup failed: ${errorMessage(error)}`;
		}
		if (degradedDetail) failure = `fuse-overlayfs probe cleanup degraded: ${degradedDetail}`;
		if (safeToRemove) await rm(probeRoot, { recursive: true, force: true }).catch(() => undefined);
	}
	return failure ? { available: false, detail: failure } : { available: true, ...resolved };
}

async function startLinuxOverlayfs(input: ResolvedOverlayfs & {
	readonly privateRoot: string; readonly lowerRoot: string;
	readonly onDegraded?: (detail: string) => void;
}): Promise<LinuxOverlayfsMount> {
	const { privateRoot, lowerRoot, binary, fusermountBinary, onDegraded } = input;
	const root = path.join(privateRoot, "workspace"), upperRoot = path.join(privateRoot, "upper"), workRoot = path.join(privateRoot, "work");
	for (const value of [lowerRoot, upperRoot, workRoot]) assertOverlayOptionPath(value);
	await mapFilesystem([root, upperRoot, workRoot], directory => mkdir(directory, { recursive: true }));
	const backing = await Promise.all([lowerRoot, upperRoot, workRoot].map(directory => lstat(directory)));
	if (!backing.every(directory => directory.isDirectory() && directory.dev === backing[0]!.dev))
		throw new Error("OverlayFS lower, upper, and work roots must share one backing filesystem");
	const child = spawn(binary, ["-f", "-o", `clone_fd,preserve_hardlinks,lowerdir=${lowerRoot},upperdir=${upperRoot},workdir=${workRoot}`, root],
		{ stdio: ["ignore", "pipe", "pipe"] });
	let processError: Error | undefined, diagnostics = "", closing: Promise<void> | undefined;
	child.on("error", error => { processError ??= error; });
	const processClosed = new Promise<void>(resolve => child.once("close", () => resolve()));
	for (const stream of [child.stdout, child.stderr])
		stream.setEncoding("utf8").on("data", (chunk: string) => { diagnostics = (diagnostics + chunk).slice(-MAX_DIAGNOSTIC_BYTES); });
	const failure = (reason: string) => new Error(`fuse-overlayfs ${reason}; ${diagnostics.trim()}`);
	const exited = async () => (await waitForCompletion(processClosed, undefined, OVERLAY_EXIT_TIMEOUT_MS)).status === "completed";
	// Startup failure and ordinary disposal share the same idempotent teardown.
	const close = (): Promise<void> => closing ??= (async () => {
		const failures: string[] = [];
		const unmount = async (phase: string) => {
			if (await mountedAsFuseOverlayfs(root)) try { await execText(fusermountBinary, ["-u", root]); }
			catch (error) { failures.push(`${phase}: ${errorMessage(error)}`); return false; }
			return true;
		};
		try {
			const normal = await unmount("normal unmount failed and required process termination");
			if (!normal) child.kill("SIGKILL");
			if (!await exited()) {
				child.kill("SIGKILL");
				if (!await exited()) failures.push("FUSE process did not report closure after SIGKILL");
			}
			if (!normal) await unmount("post-termination unmount failed");
			if (!await waitForMountState(root, false, OVERLAY_EXIT_TIMEOUT_MS))
				throw failure(["remained mounted after process shutdown", ...failures].join("; "));
			if (failures.length) onDegraded?.(failures.join("; "));
		} catch (error) { onDegraded?.(`unsafe unmount failure: ${errorMessage(error)}`); throw error; }
	})();
	try {
		const ready = await waitForMountState(root, true, OVERLAY_READY_TIMEOUT_MS, () => {
			if (processError) throw failure(`failed before mount was ready: ${errorMessage(processError)}`);
			if (child.exitCode !== null || child.signalCode !== null) throw failure("exited before mount was ready");
		});
		if (!ready) throw failure("mount timed out");
		return { root, upperRoot, workRoot, close };
	} catch (error) {
		onDegraded?.(`mount startup failed: ${errorMessage(error)}`);
		try { await close(); } catch (cleanupError) {
			throw new LinuxOverlayfsUnsafeCleanupError("fuse-overlayfs startup failed and its backing storage could not be released safely",
				new AggregateError([error, cleanupError]));
		}
		throw error;
	}
}

async function waitForMountState(mountRoot: string, mounted: boolean, timeout: number, pending?: () => void): Promise<boolean> {
	const deadline = performance.now() + timeout;
	for (;;) {
		if (await mountedAsFuseOverlayfs(mountRoot) === mounted) return true;
		pending?.();
		if (performance.now() >= deadline) return false;
		await new Promise<void>((resolve) => setTimeout(resolve, 10));
	}
}

async function mountedAsFuseOverlayfs(root: string): Promise<boolean> {
	const target = path.resolve(root);
	const mountInfo = await readFile("/proc/self/mountinfo", "utf8");
	for (const line of mountInfo.split("\n")) {
		const separator = line.indexOf(" - ");
		if (separator === -1) continue;
		const left = line.slice(0, separator).split(" ");
		const right = line.slice(separator + 3).split(" ");
		if (decodeMountInfoPath(left[4] ?? "") !== target) continue;
		return right[0] === "fuse.fuse-overlayfs" || right[0] === "fuse-overlayfs";
	}
	return false;
}

function decodeMountInfoPath(value: string): string {
	return value.replace(/\\([0-7]{3})/g, (_match, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));
}

async function resolveOverlayfs(options: LinuxOverlayfsOptions): Promise<ResolvedOverlayfs> {
	const binary = await resolveHostExecutable(
		options.overlayfsBinary,
		"fuse-overlayfs",
		[path.join(os.homedir(), ".local", "bin", "fuse-overlayfs")],
	);
	const fusermountBinary = await resolveHostExecutable(options.fusermountBinary, "fusermount3", [], ["fusermount"]);
	const [version, kernel, binaryDigest] = await Promise.all([execText(binary, ["--version"]), readFile("/proc/sys/kernel/osrelease", "utf8"), hashExecutableFile(binary)]);
	return { binary, fusermountBinary,
		fingerprint: [OVERLAY_OPTIONS_EPOCH, process.arch, kernel.trim(), version.trim().replaceAll(/\s+/g, " "), binaryDigest].join(":"),
		detail: `ready (${version.split(/\r?\n/)[0]?.trim() ?? "fuse-overlayfs"})` };
}

function availableCapabilityKey(
	capability: ResolvedOverlayfs,
): string {
	return `${capability.binary}\0${capability.fusermountBinary}\0${capability.fingerprint}`;
}

const execText = (executable: string, args: readonly string[]) => execHostText(executable, args, { maxBuffer: 4 * 1024 * 1024 });

async function expectContent(target: string, expected: string): Promise<void> {
	if (await readFile(target, "utf8") !== expected) throw new Error(`OverlayFS content mismatch: ${target}`);
}

function assertOverlayOptionPath(value: string): void {
	if (!path.isAbsolute(value) || /[,\n\r\0:]/.test(value)) throw new Error(`OverlayFS path cannot be encoded safely: ${value}`);
}
