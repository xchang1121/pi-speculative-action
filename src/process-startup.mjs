// @ts-check
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const requireNative = createRequire(import.meta.url), qualification = fileURLToPath(import.meta.url);

/** Qualify the same Node/libuv path in a disposable group without occupying the host's launch gate.
 * @param {string} binary
 * @param {AbortSignal} [owner]
 * @returns {Promise<(operation: number) => number>}
 */
export async function qualifyNativeStartup(binary, owner) {
	const signal = AbortSignal.any([AbortSignal.timeout(10_000), ...(owner ? [owner] : [])]); signal.throwIfAborted();
	const child = spawn(process.execPath, [qualification, binary], { detached: true, stdio: "ignore" });
	const closed = new Promise(resolve => child.once("close", resolve)); child.on("error", () => {});
	let force;
	const abort = () => { const pid = child.pid; if (pid && child.exitCode === null && child.signalCode === null) {
		try { process.kill(-pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
		force ??= setTimeout(() => { try { process.kill(-pid, "SIGKILL"); } catch { child.kill("SIGKILL"); } }, 1000);
	} };
	signal.addEventListener("abort", abort, { once: true });
	try {
		if (await closed !== 0) throw new Error("native startup interception unavailable");
		signal.throwIfAborted(); return requireNative(`${binary}.node`);
	} finally { signal.removeEventListener("abort", abort); abort(); await closed; clearTimeout(force); }
}

if (process.argv[1] === qualification) {
	// Group cancellation kills the probe children; this parent stays to reap them.
	process.on("SIGTERM", () => {});
	const binary = process.argv[2], startup = requireNative(`${binary}.node`), gate = startup(0);
	if (!Number.isInteger(gate) || gate < 0) throw new Error("native startup gate unavailable");
	const observer = spawn(binary, ["--attach-skip-code", "42"], { stdio: ["pipe", "ignore", "ignore", gate] });
	const closed = new Promise(resolve => observer.once("close", resolve)); observer.on("error", () => {}); observer.stdin?.on("error", () => {});
	try {
		if (!observer.pid || startup(observer.pid) !== 0 || spawnSync("/bin/true", [], { stdio: "ignore" }).status !== 42) throw new Error("native startup gate unavailable");
	} finally { startup(-1); observer.stdin?.end(); await closed; }
	if (await closed !== 42) throw new Error("native startup observer unavailable");
}
