// @ts-check
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { access, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { AsyncLocalStorage } from "node:async_hooks";
import os from "node:os";
import path from "node:path";

/** @type {AsyncLocalStorage<{skipRoot?: boolean, launch: (child: import('node:child_process').ChildProcess, start: () => number) => number, synchronous?: (start: () => unknown) => unknown}>} */
export const startupScope = new AsyncLocalStorage();

const requireNative = createRequire(import.meta.url), qualification = fileURLToPath(import.meta.url);
/** @type {((fd: number, buffer?: Buffer, adopt?: (fd: number) => number) => number) | undefined} */
export let nativeFilesystem;
/** @type {string | undefined} */
let configuredFilesystemBinary;
/** @type {string | undefined} */
let loadedFilesystemBinary;
/** @type {Promise<NonNullable<typeof nativeFilesystem>> | undefined} */
let filesystemInitialization;

/** @param {string | undefined} [binary] */
export function configureNativeFilesystem(binary) {
	if (binary !== undefined && !nativeFilesystem) configuredFilesystemBinary = binary;
}

export function nativeFilesystemBinary() { return loadedFilesystemBinary ?? configuredFilesystemBinary; }

/** Resolve the native helper shared by resource capture, transparent dispatch and Actor handoff.
 * @param {string | undefined} [binary]
 */
export async function resolveLinuxExecHelper(binary) {
	if (process.platform !== "linux") throw new Error("Linux required");
	const resolved = await realpath(binary ?? path.join(os.homedir(), ".local", "bin", "pi-speculative-held-exec"));
	await access(resolved, constants.X_OK); return resolved;
}

/** Linux source capture cannot fall back to ordinary reads before a backend happens to initialize. */
export async function ensureNativeFilesystem() {
	if (process.platform !== "linux") return undefined;
	const qualify = () => {
		if (!nativeFilesystem || nativeFilesystem(-11) !== 2) throw new Error("qualified native filesystem reader unavailable; rebuild the Linux backend");
		return nativeFilesystem;
	};
	if (nativeFilesystem) return qualify();
	return filesystemInitialization ??= resolveLinuxExecHelper(configuredFilesystemBinary ?? process.env.PI_SPEC_HELD_EXEC).then(binary => {
		if (!nativeFilesystem) nativeProcessLibrary(binary);
		return qualify();
	}).finally(() => { filesystemInitialization = undefined; });
}
/** @param {string} binary */
export function nativeProcessLibrary(binary) {
	const startup = requireNative(`${binary}.node`);
	if (startup(-1, Buffer.alloc(24)) !== -9) throw new Error("native directory reader unavailable");
	nativeFilesystem = startup; loadedFilesystemBinary = binary; return startup;
}

/** Qualify the same Node/libuv path in a disposable group without occupying the host's launch gate.
 * @param {string} binary
 * @param {{observer?: string, signal?: AbortSignal, confined?: boolean}} [options]
 * @returns {Promise<{startup: (operation: number) => number, observer: string}>}
 */
export async function qualifyNativeStartup(binary, options = {}) {
	const signal = AbortSignal.any([AbortSignal.timeout(10_000), ...(options.signal ? [options.signal] : [])]); signal.throwIfAborted();
	const observer = await realpath(options.observer ?? process.env.PI_SPEC_HELD_EXEC_OBSERVER ?? await access(`${binary}.observer`).then(() => `${binary}.observer`, () => binary));
	await access(observer, constants.X_OK); signal.throwIfAborted();
	const child = spawn(process.execPath, [qualification, binary, observer, options.confined ? "confined" : "native"], { detached: true, stdio: "ignore" });
	const closed = new Promise(resolve => child.once("close", resolve)); child.on("error", () => {});
	let force;
	const abort = () => { const pid = child.pid; if (pid && child.exitCode === null && child.signalCode === null) {
		try { process.kill(-pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
		force ??= setTimeout(() => { try { process.kill(-pid, "SIGKILL"); } catch { child.kill("SIGKILL"); } }, 1000);
	} };
	signal.addEventListener("abort", abort, { once: true });
	try {
		const code = await closed;
		if (code !== 0) throw new Error(code === 77 ? "native observation requires a qualified PI_SPEC_HELD_EXEC_OBSERVER or inherited no_new_privs" : "native startup interception unavailable");
		signal.throwIfAborted(); return { startup: nativeProcessLibrary(binary), observer };
	} finally { signal.removeEventListener("abort", abort); abort(); await closed; clearTimeout(force); }
}

if (process.argv[1] === qualification) {
	// Group cancellation kills the probe children; this parent stays to reap them.
	process.on("SIGTERM", () => {});
	const binary = process.argv[2], startup = nativeProcessLibrary(binary), observerBinary = process.argv[3];
	if (process.argv[4] === "confined" && startup(-2) !== 0) throw new Error("private startup qualification unavailable");
	const probe = spawnSync(observerBinary, ["--probe-observation"], { stdio: "ignore" });
	if (probe.status !== 0) process.exit(probe.status ?? 70);
	const gate = startup(0);
	if (!Number.isInteger(gate) || gate < 0) throw new Error("native startup gate unavailable");
	const observer = spawn(observerBinary, ["--attach-skip-code", "42"], { stdio: ["pipe", "ignore", "ignore", gate] });
	const closed = new Promise(resolve => observer.once("close", resolve)); observer.on("error", () => {}); observer.stdin?.on("error", () => {});
	try {
		if (!observer.pid || startup(observer.pid) !== 0 || spawnSync("/bin/true", [], { stdio: "ignore" }).status !== 42) throw new Error("native startup gate unavailable");
	} finally { startup(-1); observer.stdin?.end(); await closed; }
	if (await closed !== 42) throw new Error("native startup observer unavailable");
}
