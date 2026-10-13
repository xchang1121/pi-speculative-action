// @ts-check
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { access, realpath } from "node:fs/promises";
import { constants } from "node:fs";

const requireNative = createRequire(import.meta.url), qualification = fileURLToPath(import.meta.url);

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
		signal.throwIfAborted(); return { startup: requireNative(`${binary}.node`), observer };
	} finally { signal.removeEventListener("abort", abort); abort(); await closed; clearTimeout(force); }
}

if (process.argv[1] === qualification) {
	// Group cancellation kills the probe children; this parent stays to reap them.
	process.on("SIGTERM", () => {});
	const binary = process.argv[2], startup = requireNative(`${binary}.node`), observerBinary = process.argv[3];
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
