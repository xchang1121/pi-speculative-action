import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { argument, compileBenchmarkHelper, linuxBenchmarkHost, writeBenchmarkReport } from "./linux-process-harness.ts";

interface Outcome { readonly code: number | null; readonly signal: NodeJS.Signals | null; readonly stdout: Buffer; readonly stderr: Buffer; readonly durationMs: number }
interface HeldOutcome extends Outcome { readonly execEvents: number; readonly replayedExecs: number }
type HeldDecision =
	| { readonly kind: "continue" }
	| { readonly kind: "replay"; readonly code: number; readonly stdout: Buffer; readonly stderr: Buffer; readonly commit: () => Promise<void> };
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
	const childConversion = process.arch === "x64" ? await conversionAblation(root, tracer) : undefined;
	await writeBenchmarkReport({
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(),
		rounds,
		measurements,
		heldExecSubstitution: substitution ? { exitCode: substitution.code, durationMs: substitution.durationMs } : { unsupportedArchitecture: process.arch },
		childConversion: childConversion ?? { unsupportedArchitecture: process.arch },
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

async function conversionAblation(root: string, tracer: string) {
	const worker = "#!/bin/bash\nset -e\nvalue=$(cat input.txt)\n/bin/sleep 0.3\nprintf 'artifact:%s\\n' \"$value\" > result.txt\nprintf 'worker:%s\\n' \"$value\"\n";
	const workspace = async (name: string, value = "v1") => {
		const cwd = path.join(root, name);
		await mkdir(cwd);
		await Promise.all([
			writeFile(path.join(cwd, "input.txt"), `${value}\n`),
			writeFile(path.join(cwd, "worker.sh"), worker, { mode: 0o755 }),
		]);
		return cwd;
	};
	const certificate = async (cwd: string) => {
		const outcome = await run(["./worker.sh"], cwd);
		if (outcome.code !== 0 || outcome.signal) throw new Error("speculative child failed");
		return {
			input: digest(await readFile(path.join(cwd, "input.txt"))),
			worker: digest(await readFile(path.join(cwd, "worker.sh"))),
			result: await readFile(path.join(cwd, "result.txt")),
			outcome,
		};
	};
	const valid = async (cwd: string, cached: Awaited<ReturnType<typeof certificate>>) =>
		digest(await readFile(path.join(cwd, "input.txt"))) === cached.input &&
		digest(await readFile(path.join(cwd, "worker.sh"))) === cached.worker;
	const replay = (cwd: string, cached: Awaited<ReturnType<typeof certificate>>): HeldDecision => ({
		kind: "replay",
		code: cached.outcome.code ?? 125,
		stdout: cached.outcome.stdout,
		stderr: cached.outcome.stderr,
		commit: () => atomicWrite(path.join(cwd, "result.txt"), cached.result),
	});
	const actorCommand: Command = ["/bin/bash", "-c", ": actor-parent; ./worker.sh; cat result.txt"];
	const expectedCwd = await workspace("expected");
	const expected = await run(actorCommand, expectedCwd);
	const expectedResult = await readFile(path.join(expectedCwd, "result.txt"));

	const completedCertificate = await certificate(await workspace("completed-speculation"));
	const completedCwd = await workspace("completed-actor");
	const completed = await runHeld(tracer, actorCommand, completedCwd, async (event) =>
		event === 1 && await valid(completedCwd, completedCertificate)
			? replay(completedCwd, completedCertificate)
			: { kind: "continue" },
	);
	await assertConversion(expected, expectedResult, completed, completedCwd, 1);

	const joiningSpeculation = certificate(await workspace("joining-speculation"));
	await new Promise((resolve) => setTimeout(resolve, 100));
	const joiningCwd = await workspace("joining-actor");
	let waitedMs = 0;
	const joining = await runHeld(tracer, actorCommand, joiningCwd, async (event) => {
		if (event !== 1) return { kind: "continue" };
		const started = performance.now();
		const cached = await joiningSpeculation;
		waitedMs = performance.now() - started;
		return await valid(joiningCwd, cached) ? replay(joiningCwd, cached) : { kind: "continue" };
	});
	await assertConversion(expected, expectedResult, joining, joiningCwd, 1);

	const staleExpectedCwd = await workspace("stale-expected", "v2");
	const staleExpected = await run(actorCommand, staleExpectedCwd);
	const staleResult = await readFile(path.join(staleExpectedCwd, "result.txt"));
	const staleCwd = await workspace("stale-actor", "v2");
	const stale = await runHeld(tracer, actorCommand, staleCwd, async (event) =>
		event === 1 && await valid(staleCwd, completedCertificate)
			? replay(staleCwd, completedCertificate)
			: { kind: "continue" },
	);
	await assertConversion(staleExpected, staleResult, stale, staleCwd, 0);
	return {
		directMs: expected.durationMs,
		completed: { durationMs: completed.durationMs, execEvents: completed.execEvents, replayedExecs: completed.replayedExecs },
		joining: { actorDurationMs: joining.durationMs, waitedMs, speculationLeadMs: 100, execEvents: joining.execEvents, replayedExecs: joining.replayedExecs },
		changedInputMiss: { durationMs: stale.durationMs, execEvents: stale.execEvents, replayedExecs: stale.replayedExecs },
	};
}

async function assertConversion(expected: Outcome, expectedResult: Buffer, actual: HeldOutcome, cwd: string, hits: number) {
	assertSame(expected, actual);
	if (!expectedResult.equals(await readFile(path.join(cwd, "result.txt"))) || actual.replayedExecs !== hits)
		throw new Error("held child conversion changed the final workspace or reuse decision");
}

async function atomicWrite(target: string, content: Buffer): Promise<void> {
	const temporary = `${target}.replay-${process.pid}`;
	try {
		await writeFile(temporary, content);
		await rename(temporary, target);
	} finally {
		await rm(temporary, { force: true });
	}
}

function digest(content: Buffer): string {
	return createHash("sha256").update(content).digest("hex");
}

function runHeld(
	tracer: string,
	command: Command,
	cwd: string,
	decide: (event: number) => HeldDecision | Promise<HeldDecision>,
): Promise<HeldOutcome> {
	return new Promise((resolve, reject) => {
		const started = performance.now();
		const child = spawn(tracer, ["--broker", ...command], { cwd, stdio: ["ignore", "pipe", "pipe", "pipe", "pipe"] });
		const childStdout = child.stdout!;
		const childStderr = child.stderr!;
		const eventStream = child.stdio[3] as Readable;
		const decisionStream = child.stdio[4] as Writable;
		const stdout: Buffer[] = [], stderr: Buffer[] = [];
		let execEvents = 0, replayedExecs = 0, failure: unknown;
		let decisions = Promise.resolve();
		childStdout.on("data", (value: Buffer) => stdout.push(Buffer.from(value)));
		childStderr.on("data", (value: Buffer) => stderr.push(Buffer.from(value)));
		eventStream.on("data", (value: Buffer) => {
			for (const marker of value) {
				if (marker !== 0x45) { failure ??= new Error("invalid held-exec event"); continue; }
				const event = ++execEvents;
				decisions = decisions.then(async () => {
					let decision: HeldDecision = { kind: "continue" };
					try { decision = await decide(event); } catch (error) { failure ??= error; }
					if (decision.kind === "replay") {
						await decision.commit();
						stdout.push(decision.stdout); stderr.push(decision.stderr); replayedExecs++;
					}
					await new Promise<void>((settle, fail) => decisionStream.write(
						Buffer.from([decision.kind === "replay" ? 0x53 : 0x43, decision.kind === "replay" ? decision.code : 0]),
						(error?: Error | null) => error ? fail(error) : settle(),
					));
				}).catch((error) => { failure ??= error; decisionStream.destroy(); });
			}
		});
		child.once("error", reject);
		child.once("close", (code, signal) => void decisions.then(() => {
			if (failure) reject(failure);
			else resolve({ code, signal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr), durationMs: performance.now() - started, execEvents, replayedExecs });
		}, reject));
	});
}

function run([executable, ...args]: Command, cwd?: string): Promise<Outcome> {
	return new Promise((resolve, reject) => {
		const started = performance.now();
		const child = spawn(executable, args, { ...(cwd ? { cwd } : {}), stdio: ["ignore", "pipe", "pipe"] });
		const stdout: Buffer[] = [], stderr: Buffer[] = [];
		child.stdout.on("data", (value: Buffer) => stdout.push(Buffer.from(value)));
		child.stderr.on("data", (value: Buffer) => stderr.push(Buffer.from(value)));
		child.once("error", reject);
		child.once("exit", (code, signal) => resolve({ code, signal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr), durationMs: performance.now() - started }));
	});
}
