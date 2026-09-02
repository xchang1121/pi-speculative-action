import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { argument, compileBenchmarkHelper, linuxBenchmarkHost, writeBenchmarkReport } from "./linux-process-harness.ts";

interface Outcome { readonly code: number | null; readonly signal: NodeJS.Signals | null; readonly stdout: Buffer; readonly stderr: Buffer; readonly durationMs: number }
type Command = readonly [string, ...string[]];

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const output = argument("--output");
const rounds = Number.parseInt(argument("--rounds") ?? "20", 10);
if (!Number.isSafeInteger(rounds) || rounds < 1) throw new Error("--rounds must be a positive integer");
const root = await mkdtemp(path.join(os.tmpdir(), "pi-exec-boundary-"));
const tracer = path.join(root, "exec-events");
try {
	await compileBenchmarkHelper(root, {
		source: fileURLToPath(new URL("./linux-exec-boundary-probe.c", import.meta.url)),
		output: path.basename(tracer),
		arguments: ["-Werror"],
	});
	const modes = {
		direct: (command: Command): Command => command,
		execEventPtrace: (command: Command): Command => [tracer, ...command],
		straceProcess: (command: Command): Command => ["strace", "-f", "-qq", "-e", "trace=%process", "-o", "/dev/null", ...command],
		straceSeccompBpf: (command: Command): Command => ["strace", "--seccomp-bpf", "-f", "-qq", "-e", "trace=%process", "-o", "/dev/null", ...command],
	};
	const equivalence: Command = ["/bin/bash", "-c", "printf out; printf err >&2; exit 7"];
	const direct = await run(equivalence);
	for (const wrap of Object.values(modes)) assertSame(direct, await run(wrap(equivalence)));
	const cases: Readonly<Record<string, { readonly command: Command; readonly rounds?: number }>> = {
		shellBuiltin: { command: ["/bin/bash", "-c", "true"] },
		oneChild: { command: ["/bin/bash", "-c", "/bin/true"] },
		sleep100ms: { command: ["/bin/bash", "-c", "/bin/sleep 0.1"] },
		syscallHeavy: { command: ["/bin/bash", "-c", "dd if=/dev/zero of=/dev/null bs=4K count=4096 status=none"] },
	};
	const measurements: Record<string, unknown> = {};
	for (const [name, workload] of Object.entries(cases)) {
		measurements[name] = Object.fromEntries(await Promise.all(Object.entries(modes).map(async ([mode, wrap]) => [
			mode, summarize(await samples(wrap(workload.command), workload.rounds ?? rounds)),
		])));
	}
	const substitution = process.arch === "x64"
		? await run([tracer, "--skip-code", "42", "/bin/bash", "-c", "exec /bin/sleep 5"])
		: undefined;
	if (substitution && (substitution.code !== 42 || substitution.durationMs >= 1_000))
		throw new Error(`held exec substitution failed: ${JSON.stringify(substitution)}`);
	const nativeTracer = await run(["/bin/bash", "-c", "grep '^TracerPid:' /proc/self/status"]);
	const ptraceTracer = await run([tracer, "/bin/bash", "-c", "grep '^TracerPid:' /proc/self/status"]);
	if (!nativeTracer.stdout.toString().endsWith("\t0\n") || ptraceTracer.stdout.toString().endsWith("\t0\n"))
		throw new Error("ptrace observability probe failed");
	await writeBenchmarkReport({
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(),
		rounds,
		measurements,
		heldExecSubstitution: substitution ? { exitCode: substitution.code, durationMs: substitution.durationMs } : { unsupportedArchitecture: process.arch },
		semanticDifference: { direct: nativeTracer.stdout.toString().trim(), ptrace: ptraceTracer.stdout.toString().trim() },
	}, output);
} finally {
	await rm(root, { recursive: true, force: true });
}

async function samples(command: Command, count: number): Promise<number[]> {
	const values: number[] = [];
	for (let index = 0; index < count; index++) {
		const result = await run(command);
		if (result.code !== 0 || result.signal) throw new Error(`benchmark command failed: ${command[0]}`);
		values.push(result.durationMs);
	}
	return values;
}

function summarize(values: readonly number[]) {
	const ordered = [...values].sort((left, right) => left - right);
	return { minMs: ordered[0], medianMs: percentile(ordered, 0.5), p95Ms: percentile(ordered, 0.95), maxMs: ordered.at(-1) };
}

function percentile(values: readonly number[], fraction: number): number {
	return values[Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)]!;
}

function assertSame(expected: Outcome, actual: Outcome): void {
	if (expected.code !== actual.code || expected.signal !== actual.signal || !expected.stdout.equals(actual.stdout) || !expected.stderr.equals(actual.stderr))
		throw new Error("pass-through tracer changed the command result");
}

function run([executable, ...args]: Command): Promise<Outcome> {
	return new Promise((resolve, reject) => {
		const started = performance.now();
		const child = spawn(executable, args, { stdio: ["ignore", "pipe", "pipe"] });
		const stdout: Buffer[] = [], stderr: Buffer[] = [];
		child.stdout.on("data", (value: Buffer) => stdout.push(Buffer.from(value)));
		child.stderr.on("data", (value: Buffer) => stderr.push(Buffer.from(value)));
		child.once("error", reject);
		child.once("exit", (code, signal) => resolve({ code, signal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr), durationMs: performance.now() - started }));
	});
}
