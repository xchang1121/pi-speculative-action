import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
	access,
	lstat,
	mkdir,
	mkdtemp,
	open,
	readFile,
	realpath,
	rm,
	writeFile,
	type FileHandle,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const OVERLAY_OPTIONS_EPOCH = "fuse-overlayfs-cow-v3";
const OVERLAY_READY_TIMEOUT_MS = 5_000;
const OVERLAY_EXIT_TIMEOUT_MS = 2_000;
const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
// asm-generic/fcntl.h: __O_TMPFILE (020000000) | O_DIRECTORY (00200000).
const LINUX_O_TMPFILE = 0o20200000;

export interface LinuxOverlayfsOptions {
	readonly overlayfsBinary?: string;
	readonly fusermountBinary?: string;
}

export type LinuxOverlayfsCapability =
	| {
			readonly available: true;
			readonly binary: string;
			readonly fusermountBinary: string;
			readonly fingerprint: string;
			readonly detail: string;
	  }
	| {
			readonly available: false;
			readonly detail: string;
	  };

export interface LinuxOverlayfsMount {
	readonly root: string;
	readonly upperRoot: string;
	readonly workRoot: string;
	readonly close: () => Promise<void>;
}

/** The backing directories must remain alive because a FUSE mount could not be proven closed. */
export class LinuxOverlayfsUnsafeCleanupError extends Error {
	constructor(message: string, cause: unknown) {
		super(message, { cause });
		this.name = "LinuxOverlayfsUnsafeCleanupError";
	}
}

/** Open an unnamed regular inode in private backing storage without exposing a control path. */
export async function openLinuxAnonymousWorkspaceFile(root: string): Promise<FileHandle> {
	if (process.platform !== "linux") throw new Error("anonymous workspace files require Linux");
	return open(root, fsConstants.O_WRONLY | LINUX_O_TMPFILE, 0o600);
}

interface ResolvedOverlayfs {
	readonly binary: string;
	readonly fusermountBinary: string;
	readonly version: string;
	readonly kernel: string;
}

const capabilityCache = new Map<string, Promise<LinuxOverlayfsCapability>>();

/** Probe the complete mount/read/copy-up/whiteout/unmount lifecycle; version output alone is insufficient. */
export function linuxOverlayfsCapability(
	options: LinuxOverlayfsOptions = {},
): Promise<LinuxOverlayfsCapability> {
	const key = overlayfsCapabilityKey(options);
	let pending = capabilityCache.get(key);
	if (!pending) {
		pending = probeLinuxOverlayfs(options);
		capabilityCache.set(key, pending);
	}
	return pending;
}

/** Mount a private copy-on-write view in the caller's mount namespace. */
export async function mountLinuxOverlayfs(input: {
	readonly lowerRoot: string;
	readonly privateRoot: string;
	readonly options?: LinuxOverlayfsOptions;
}): Promise<LinuxOverlayfsMount> {
	const capabilityKey = overlayfsCapabilityKey(input.options ?? {});
	const capability = await linuxOverlayfsCapability(input.options);
	if (!capability.available) throw new Error(capability.detail);
	const upperRoot = path.join(input.privateRoot, "upper");
	const workRoot = path.join(input.privateRoot, "work");
	const root = path.join(input.privateRoot, "workspace");
	await Promise.all([
		mkdir(upperRoot, { recursive: true }),
		mkdir(workRoot, { recursive: true }),
		mkdir(root, { recursive: true }),
	]);
	const [lower, upper, work] = await Promise.all([lstat(input.lowerRoot), lstat(upperRoot), lstat(workRoot)]);
	if (
		!lower.isDirectory() ||
		!upper.isDirectory() ||
		!work.isDirectory() ||
		lower.dev !== upper.dev ||
		upper.dev !== work.dev
	) {
		throw new Error("OverlayFS lower, upper, and work roots must share one backing filesystem");
	}
	return startLinuxOverlayfs({
		binary: capability.binary,
		fusermountBinary: capability.fusermountBinary,
		lowerRoot: input.lowerRoot,
		upperRoot,
		workRoot,
		root,
		onDegraded: (detail) => {
			capabilityCache.set(
				capabilityKey,
				Promise.resolve({ available: false, detail: `fuse-overlayfs driver disabled: ${detail}` }),
			);
		},
	});
}

async function probeLinuxOverlayfs(options: LinuxOverlayfsOptions): Promise<LinuxOverlayfsCapability> {
	if (process.platform !== "linux") return { available: false, detail: "Linux host required for OverlayFS" };
	let resolved: ResolvedOverlayfs;
	try {
		resolved = await resolveOverlayfs(options);
	} catch (error) {
		return { available: false, detail: errorMessage(error) };
	}
	const probeRoot = await mkdtemp(path.join(os.tmpdir(), "pi-fuse-overlayfs-probe-"));
	const lowerRoot = path.join(probeRoot, "lower");
	const privateRoot = path.join(probeRoot, "private");
	const upperRoot = path.join(privateRoot, "upper");
	const workRoot = path.join(privateRoot, "work");
	const root = path.join(privateRoot, "workspace");
	const marker = randomUUID();
	let mounted: LinuxOverlayfsMount | undefined;
	let safeToRemove = true;
	let degradedDetail: string | undefined;
	let outcome: LinuxOverlayfsCapability;
	try {
		await Promise.all([mkdir(lowerRoot), mkdir(privateRoot)]);
		await Promise.all([mkdir(upperRoot), mkdir(workRoot), mkdir(root)]);
		await Promise.all([
			writeFile(path.join(lowerRoot, "copy-up.txt"), `${marker}\n`, "utf8"),
			writeFile(path.join(lowerRoot, "whiteout.txt"), "remove\n", "utf8"),
		]);
		await mkdir(path.join(lowerRoot, "replaced"));
		await writeFile(path.join(lowerRoot, "replaced", "lower.txt"), "lower\n", "utf8");
		mounted = await startLinuxOverlayfs({
			binary: resolved.binary,
			fusermountBinary: resolved.fusermountBinary,
			lowerRoot,
			upperRoot,
			workRoot,
			root,
			onDegraded: (detail) => {
				degradedDetail = detail;
			},
		});
		if ((await readFile(path.join(root, "copy-up.txt"), "utf8")) !== `${marker}\n`) {
			throw new Error("OverlayFS lower view did not preserve file content");
		}
		await Promise.all([
			writeFile(path.join(root, "copy-up.txt"), "changed\n", "utf8"),
			rm(path.join(root, "whiteout.txt")),
			writeFile(path.join(root, "created.txt"), "created\n", "utf8"),
		]);
		await rm(path.join(root, "replaced"), { recursive: true });
		await mkdir(path.join(root, "replaced"));
		await writeFile(path.join(root, "replaced", "created.txt"), "created\n", "utf8");
		if ((await readFile(path.join(lowerRoot, "copy-up.txt"), "utf8")) !== `${marker}\n`) {
			throw new Error("OverlayFS mutated its immutable lower directory");
		}
		if ((await readFile(path.join(root, "copy-up.txt"), "utf8")) !== "changed\n") {
			throw new Error("OverlayFS copy-up was not visible");
		}
		const clock = await openLinuxAnonymousWorkspaceFile(upperRoot);
		try {
			const [lower, upper, work, workspace, anonymous, ...beforeEntries] = await Promise.all([
				lstat(lowerRoot),
				lstat(upperRoot),
				lstat(workRoot),
				lstat(root),
				clock.stat(),
				lstat(path.join(root, "copy-up.txt")),
				lstat(path.join(root, "created.txt")),
				lstat(path.join(root, "replaced")),
			]);
			if (
				!lower.isDirectory() ||
				!upper.isDirectory() ||
				!work.isDirectory() ||
				!workspace.isDirectory() ||
				!anonymous.isFile() ||
				anonymous.nlink !== 0 ||
				lower.dev !== anonymous.dev ||
				upper.dev !== anonymous.dev ||
				work.dev !== anonymous.dev
			) {
				throw new Error("OverlayFS backing roots do not share an anonymous transaction-clock filesystem");
			}
			const priorBoundary = Math.max(workspace.ctimeMs, ...beforeEntries.map((entry) => entry.ctimeMs));
			await advanceAnonymousClock(clock, priorBoundary);
			const orderProbe = path.join(root, "clock-order-probe.txt");
			await writeFile(orderProbe, "ordered\n", "utf8");
			const [changedRoot, changedFile] = await Promise.all([lstat(root), lstat(orderProbe)]);
			if (changedRoot.ctimeMs <= priorBoundary || changedFile.ctimeMs <= priorBoundary) {
				throw new Error("OverlayFS merged timestamps are not ordered by the private backing clock");
			}
			await advanceAnonymousClock(clock, Math.max(changedRoot.ctimeMs, changedFile.ctimeMs));
			await rm(orderProbe);
		} finally {
			await clock.close();
		}
		await expectMissing(path.join(root, "whiteout.txt"));
		const whiteout = await lstat(path.join(upperRoot, "whiteout.txt"));
		if (!whiteout.isCharacterDevice() || whiteout.rdev !== 0) {
			throw new Error("OverlayFS whiteout encoding is unsupported");
		}
		if (!(await lstat(path.join(upperRoot, "replaced", ".wh..wh..opq"))).isFile()) {
			throw new Error("OverlayFS opaque-directory encoding is unsupported");
		}
		const fingerprint = [
			OVERLAY_OPTIONS_EPOCH,
			process.arch,
			resolved.kernel.trim(),
			resolved.version.trim().replaceAll(/\s+/g, " "),
		].join(":");
		outcome = {
			available: true,
			binary: resolved.binary,
			fusermountBinary: resolved.fusermountBinary,
			fingerprint,
			detail: `ready (${resolved.version.split(/\r?\n/)[0]?.trim() ?? "fuse-overlayfs"})`,
		};
	} catch (error) {
		safeToRemove = !(error instanceof LinuxOverlayfsUnsafeCleanupError);
		outcome = { available: false, detail: `fuse-overlayfs probe failed: ${errorMessage(error)}` };
	} finally {
		if (mounted) {
			try {
				await mounted.close();
			} catch (error) {
				safeToRemove = false;
				outcome = { available: false, detail: `fuse-overlayfs probe cleanup failed: ${errorMessage(error)}` };
			}
		}
		if (degradedDetail) {
			outcome = { available: false, detail: `fuse-overlayfs probe cleanup degraded: ${degradedDetail}` };
		}
		if (safeToRemove) await rm(probeRoot, { recursive: true, force: true }).catch(() => undefined);
	}
	return outcome;
}

async function advanceAnonymousClock(clock: FileHandle, boundary: number): Promise<number> {
	const deadline = Date.now() + 100;
	let sequence = 0;
	for (;;) {
		await clock.truncate(0);
		await clock.write(`${++sequence}\n`, 0, "utf8");
		const changedAt = (await clock.stat()).ctimeMs;
		if (changedAt > boundary) return changedAt;
		if (Date.now() >= deadline) throw new Error("anonymous transaction clock did not advance");
		await new Promise<void>((resolve) => setTimeout(resolve, 1));
	}
}

async function startLinuxOverlayfs(input: {
	readonly binary: string;
	readonly fusermountBinary: string;
	readonly lowerRoot: string;
	readonly upperRoot: string;
	readonly workRoot: string;
	readonly root: string;
	readonly onDegraded?: (detail: string) => void;
}): Promise<LinuxOverlayfsMount> {
	for (const value of [input.lowerRoot, input.upperRoot, input.workRoot]) assertOverlayOptionPath(value);
	const child = spawn(
		input.binary,
		[
			"-f",
			"-o",
			`lowerdir=${input.lowerRoot},upperdir=${input.upperRoot},workdir=${input.workRoot}`,
			input.root,
		],
		{ stdio: ["pipe", "pipe", "pipe"] },
	);
	let processError: Error | undefined;
	child.on("error", (error) => {
		processError ??= error;
	});
	child.stdin.on("error", () => {
		// The FUSE process does not consume stdin; a concurrent exit may close the pipe first.
	});
	const processClosed = new Promise<void>((resolve) => {
		child.once("close", () => resolve());
	});
	let diagnostics = "";
	for (const stream of [child.stdout, child.stderr]) {
		stream.setEncoding("utf8");
		stream.on("data", (chunk: string) => {
			diagnostics = `${diagnostics}${chunk}`.slice(-MAX_DIAGNOSTIC_BYTES);
		});
	}
	try {
		await waitForMount(child, input.root, () => processError, () => diagnostics);
	} catch (error) {
		input.onDegraded?.(`mount startup failed: ${errorMessage(error)}`);
		try {
			await closeMount(child, processClosed, input.fusermountBinary, input.root, () => diagnostics);
		} catch (cleanupError) {
			throw new LinuxOverlayfsUnsafeCleanupError(
				"fuse-overlayfs startup failed and its backing storage could not be released safely",
				new AggregateError([error, cleanupError]),
			);
		}
		throw error;
	}
	let closed: Promise<void> | undefined;
	return {
		root: input.root,
		upperRoot: input.upperRoot,
		workRoot: input.workRoot,
		close: () => {
			closed ??= (async () => {
				try {
					const degraded = await closeMount(
						child,
						processClosed,
						input.fusermountBinary,
						input.root,
						() => diagnostics,
					);
					if (degraded) input.onDegraded?.(degraded);
				} catch (error) {
					input.onDegraded?.(`unsafe unmount failure: ${errorMessage(error)}`);
					throw error;
				}
			})();
			return closed;
		},
	};
}

async function waitForMount(
	child: ChildProcessWithoutNullStreams,
	mountRoot: string,
	processError: () => Error | undefined,
	diagnostics: () => string,
): Promise<void> {
	const deadline = Date.now() + OVERLAY_READY_TIMEOUT_MS;
	for (;;) {
		if (await mountedAsFuseOverlayfs(mountRoot)) return;
		const failure = processError();
		if (failure) {
			throw new Error(`fuse-overlayfs failed before mount was ready: ${errorMessage(failure)}; ${diagnostics().trim()}`);
		}
		if (child.exitCode !== null || child.signalCode !== null) {
			throw new Error(`fuse-overlayfs exited before mount was ready: ${diagnostics().trim()}`);
		}
		if (Date.now() >= deadline) throw new Error(`fuse-overlayfs mount timed out: ${diagnostics().trim()}`);
		await new Promise<void>((resolve) => setTimeout(resolve, 10));
	}
}

async function closeMount(
	child: ChildProcessWithoutNullStreams,
	processClosed: Promise<void>,
	fusermountBinary: string,
	mountRoot: string,
	diagnostics: () => string,
): Promise<string | undefined> {
	let unmountError: unknown;
	let recoveryUnmountError: unknown;
	if (await mountedAsFuseOverlayfs(mountRoot)) {
		try {
			await execText(fusermountBinary, ["-u", mountRoot]);
		} catch (error) {
			unmountError = error;
			child.kill("SIGKILL");
		}
	}
	child.stdin.end();
	const exited = await waitForProcessClose(processClosed);
	let processShutdownDegraded = false;
	if (!exited) {
		child.kill("SIGKILL");
		processShutdownDegraded = !(await waitForProcessClose(processClosed));
	}
	if (unmountError && (await mountedAsFuseOverlayfs(mountRoot))) {
		try {
			await execText(fusermountBinary, ["-u", mountRoot]);
		} catch (error) {
			recoveryUnmountError = error;
		}
	}
	if (!(await waitForUnmount(mountRoot))) {
		const normalFailure = unmountError ? `; normal unmount failed: ${errorMessage(unmountError)}` : "";
		const recoveryFailure = recoveryUnmountError
			? `; post-termination unmount failed: ${errorMessage(recoveryUnmountError)}`
			: "";
		throw new Error(
			`fuse-overlayfs remained mounted after process shutdown${normalFailure}${recoveryFailure}; ${diagnostics().trim()}`,
		);
	}
	const degraded = [
		unmountError ? `normal unmount failed and required process termination: ${errorMessage(unmountError)}` : undefined,
		processShutdownDegraded ? "FUSE process did not report closure after SIGKILL" : undefined,
	].filter((detail): detail is string => Boolean(detail));
	return degraded.length ? degraded.join("; ") : undefined;
}

async function waitForProcessClose(processClosed: Promise<void>): Promise<boolean> {
	return Promise.race([
		processClosed.then(() => true),
		new Promise<false>((resolve) => setTimeout(() => resolve(false), OVERLAY_EXIT_TIMEOUT_MS)),
	]);
}

async function waitForUnmount(mountRoot: string): Promise<boolean> {
	const deadline = Date.now() + OVERLAY_EXIT_TIMEOUT_MS;
	for (;;) {
		if (!(await mountedAsFuseOverlayfs(mountRoot))) return true;
		if (Date.now() >= deadline) return false;
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
	const binary = await resolveExecutable(
		options.overlayfsBinary,
		"fuse-overlayfs",
		[path.join(os.homedir(), ".local", "bin", "fuse-overlayfs")],
	);
	const fusermountBinary = await resolveExecutable(options.fusermountBinary, "fusermount3", [], ["fusermount"]);
	const [version, kernel] = await Promise.all([
		execText(binary, ["--version"]),
		readFile("/proc/sys/kernel/osrelease", "utf8"),
	]);
	return { binary, fusermountBinary, version, kernel };
}

function overlayfsCapabilityKey(options: LinuxOverlayfsOptions): string {
	return `${options.overlayfsBinary ?? "auto"}\0${options.fusermountBinary ?? "auto"}`;
}

async function resolveExecutable(
	explicit: string | undefined,
	name: string,
	fallbacks: readonly string[],
	alternateNames: readonly string[] = [],
): Promise<string> {
	for (const candidate of [explicit, ...fallbacks].filter((value): value is string => Boolean(value))) {
		try {
			await access(candidate, fsConstants.X_OK);
			return await realpath(candidate);
		} catch {
			// Continue with PATH lookup.
		}
	}
	for (const executable of [name, ...alternateNames]) {
		for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
			if (!directory) continue;
			const candidate = path.join(directory, executable);
			try {
				await access(candidate, fsConstants.X_OK);
				return await realpath(candidate);
			} catch {
				// Continue.
			}
		}
	}
	throw new Error(`Executable not found: ${[name, ...alternateNames].join(" or ")}`);
}

function execText(executable: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable} failed: ${stderr.trim() || error.message}`, { cause: error }));
			else resolve(`${stdout}${stderr}`);
		});
	});
}

async function expectMissing(target: string): Promise<void> {
	try {
		await lstat(target);
		throw new Error(`expected path to be absent: ${target}`);
	} catch (error) {
		if (isMissing(error)) return;
		throw error;
	}
}

function assertOverlayOptionPath(value: string): void {
	if (!path.isAbsolute(value) || /[,\n\r\0:]/.test(value)) {
		throw new Error(`OverlayFS path cannot be encoded safely: ${value}`);
	}
}

function isMissing(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
