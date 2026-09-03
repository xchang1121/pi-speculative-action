import { spawn } from "node:child_process";
import { lstat, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import {
	argument,
	assert,
	commitBenchmarkFixture,
	compileBenchmarkHelper,
	createLinuxProcessBenchmark,
	executeDirectBash,
	forkReusableBash,
	linuxBenchmarkHost,
	metricDelta,
	prepareLinuxProcessReuse,
	textOutput,
	type LinuxProcessBenchmark,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

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
		source: fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)),
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
	const jobControl = await run([tracer, "/bin/bash", "-c", "(sleep 0.05; kill -CONT $$) & kill -STOP $$; printf resumed"]);
	if (jobControl.code !== 0 || jobControl.stdout.toString() !== "resumed") throw new Error("ptrace changed job-control stops");
	const detached = await run([tracer, "/bin/bash", "-c", "sleep 1 >/dev/null 2>&1 &"]);
	if (detached.code !== 0 || detached.durationMs > 500) throw new Error("ptrace waited for a detached shell child");
	const childConversion = process.arch === "x64" ? await conversionAblation() : undefined;
	await writeBenchmarkReport({
		schemaVersion: 1,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(),
		rounds,
		measurements,
		heldExecSubstitution: substitution ? { exitCode: substitution.code, durationMs: substitution.durationMs } : { unsupportedArchitecture: process.arch },
		childConversion: childConversion ?? { unsupportedArchitecture: process.arch },
		semanticDifference: {
			direct: nativeTracer.stdout.toString().trim(),
			ptrace: ptraceTracer.stdout.toString().trim(),
			jobControlPreserved: true,
			detachedChildReturnMs: detached.durationMs,
		},
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

async function conversionAblation() {
	const fixture = await createLinuxProcessBenchmark("pi-held-production-");
	const replayBackend = new LinuxProcessReuseBackend({
		storeRoot: fixture.storeRoot,
		sandlockBinary: "/pi-dependency-disabled/sandlock",
		straceBinary: "/pi-dependency-disabled/strace",
	});
	try {
		await writeFile(path.join(fixture.workspace, "input.txt"), "v1\n");
		await writeFile(path.join(fixture.workspace, "worker.c"), String.raw`
#include <fcntl.h>
#include <sys/random.h>
#include <sys/prctl.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
int main(int argc, char **argv) {
	if (argc > 2) {
		if (*argv[2] == 'v') {
			struct timespec now;
			unsigned char random;
			if (clock_gettime(CLOCK_REALTIME, &now) < 0 || getrandom(&random, 1, 0) != 1 || getpid() <= 0) return 64;
		} else if (*argv[2] == 'i') {
			struct stat state;
			char result[] = "0000000000000000\n";
			if (stat("input.txt", &state) < 0) return 70;
			for (int index = 15; index >= 0; index--) {
				unsigned digit = (unsigned)(state.st_ino & 15);
				result[index] = (char)(digit < 10 ? '0' + digit : 'a' + digit - 10);
				state.st_ino >>= 4;
			}
			return write(1, result, sizeof(result) - 1) == sizeof(result) - 1 ? 0 : 71;
		} else {
			char result[] = "nnp:?\n";
			int value = prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0);
			if (value < 0 || value > 9) return 68;
			result[4] = (char)('0' + value);
			return write(1, result, sizeof(result) - 1) == sizeof(result) - 1 ? 0 : 69;
		}
	}
  char value[32] = {0};
  const char *output_path = argc > 1 ? argv[1] : "result.txt";
  int input = open("input.txt", O_RDONLY);
  ssize_t length;
  if (input < 0 || (length = read(input, value, sizeof(value))) <= 0) return 65;
  close(input);
  volatile unsigned long long digest = 1;
  for (unsigned long long index = 0; index < 240000000; index++) digest = digest * 1664525 + 1013904223;
  int output = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (output < 0 || write(output, "artifact:", 9) != 9 || write(output, value, (size_t)length) != length) return 66;
  close(output);
  return write(1, "worker:", 7) == 7 && write(1, value, (size_t)length) == length ? 0 : 67;
}
`);
		await compileBenchmarkHelper(fixture.workspace, { source: "worker.c", output: "worker" });
		await commitBenchmarkFixture(fixture.workspace, "Pi Held Exec Benchmark");
		const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
		const actorCommand = "printf 'actor-parent\\n'; worker result.txt";
		const direct = await executeDirectBash(fixture, { label: "held-direct", command: actorCommand });
		const expectedOutput = textOutput(direct.output);
		const expectedResult = await readFile(path.join(fixture.workspace, "result.txt"));
		await rm(path.join(fixture.workspace, "result.txt"));
		const branch = await forkReusableBash(fixture, {
			label: "held-producer",
			command: ": speculative-parent; worker result.txt",
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		try {
			assert(!branch.output.isError, `speculative child failed: ${textOutput(branch.output.result)} ${JSON.stringify(fixture.backend.metrics())}`);
			assert(fixture.backend.metrics().published > 0, `speculative child did not publish a reusable certificate: ${JSON.stringify(fixture.backend.metrics())}`);
		} finally {
			await branch.dispose();
		}
		const actor = await heldActor(fixture, replayBackend);
		const beforeHit = replayBackend.metrics();
		const hitStarted = performance.now();
		const hit = await actor.execute("held-hit", { command: actorCommand }, new AbortController().signal);
		const hitMs = performance.now() - hitStarted;
		const hitMetrics = metricDelta(beforeHit, replayBackend.metrics());
		assert(textOutput(hit) === expectedOutput, "held child changed Actor output");
		assert((await readFile(path.join(fixture.workspace, "result.txt"))).equals(expectedResult), "held child changed workspace result");
		assert(
			hitMetrics.hits === 1 && hitMetrics.timedHits === 0 && hitMetrics.avoidedProcessMs === 0,
			`uncalibrated held child reported invented savings: ${JSON.stringify(hitMetrics)}`,
		);
		const joiningActor = await heldActor(fixture, fixture.backend);

		const cwdProducerBefore = fixture.backend.metrics();
		const cwdBranch = await forkReusableBash(fixture, {
			label: "held-cwd-producer",
			command: "/bin/pwd",
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		let cwdHits = -1;
		try {
			const cwdProduced = metricDelta(cwdProducerBefore, fixture.backend.metrics());
			assert(textOutput(cwdBranch.output.result) === `${fixture.workspace}\n`, "speculative child observed a private cwd");
			const cwdBefore = fixture.backend.metrics();
			const cwdActor = await joiningActor.execute(
				"held-cwd-actor",
				{ command: "printf 'actor-cwd\\n'; /bin/pwd" },
				new AbortController().signal,
			);
			const cwdMetrics = metricDelta(cwdBefore, fixture.backend.metrics());
			cwdHits = cwdMetrics.hits;
			assert(textOutput(cwdActor) === `actor-cwd\n${fixture.workspace}\n`, "transferred child observed a non-Actor cwd");
			assert(
				cwdMetrics.hits === 1,
				`absolute PATH alias was not transferred: producer=${JSON.stringify(cwdProduced)} actor=${JSON.stringify(cwdMetrics)}`,
			);
		} finally {
			await cwdBranch.dispose();
		}

		const securityBefore = fixture.backend.metrics();
		const securityBranch = await forkReusableBash(fixture, {
			label: "held-security-producer",
			command: ": speculative-security; worker unused probe",
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		try {
			assert(textOutput(securityBranch.output.result).includes("nnp:1"), "producer confinement probe was not active");
		} finally {
			await securityBranch.dispose();
		}
		const securityProduced = metricDelta(securityBefore, fixture.backend.metrics());
		assert(securityProduced.tainted === 1 && securityProduced.published === 1, "confinement evidence was not retained");
		const securityActorBefore = replayBackend.metrics();
		const securityActor = await actor.execute(
			"held-security-actor",
			{ command: ": actor-security; worker unused probe" },
			new AbortController().signal,
		);
		const securityMetrics = metricDelta(securityActorBefore, replayBackend.metrics());
		assert(textOutput(securityActor).includes("nnp:0"), "Actor did not retain its native security context");
		assert(
			securityMetrics.hits === 0 && securityMetrics.misses >= 1 && securityMetrics.lastError?.includes("certificate_tainted"),
			`confinement-sensitive result was reused: ${JSON.stringify(securityMetrics)}`,
		);

		const inodeBranch = await forkReusableBash(fixture, {
			label: "held-inode-producer",
			command: ": speculative-inode; worker unused inode",
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		const inodeBefore = replayBackend.metrics();
		const expectedInode = (await lstat(path.join(fixture.workspace, "input.txt"), { bigint: true })).ino
			.toString(16).padStart(16, "0");
		let inodeHits = -1;
		try {
			const inodeActor = await actor.execute(
				"held-inode-actor",
				{ command: ": actor-inode; worker unused inode" },
				new AbortController().signal,
			);
			const inodeMetrics = metricDelta(inodeBefore, replayBackend.metrics());
			inodeHits = inodeMetrics.hits;
			assert(
				textOutput(inodeActor).trim() === expectedInode,
				`Actor observed speculative inode metadata: expected ${expectedInode}, got ${JSON.stringify(textOutput(inodeActor).trim())}; ${JSON.stringify(inodeMetrics)}`,
			);
			assert(inodeMetrics.hits === 0 && inodeMetrics.misses >= 1, "non-equivalent inode metadata was reused");
		} finally {
			await inodeBranch.dispose();
		}

		await Promise.all([
			writeFile(path.join(fixture.workspace, "input.txt"), "v2\n"),
			rm(path.join(fixture.workspace, "result.txt")),
		]);
		const beforeMiss = fixture.backend.metrics();
		const missStarted = performance.now();
		const miss = await joiningActor.execute("held-stale", { command: actorCommand }, new AbortController().signal);
		const missMs = performance.now() - missStarted;
		const missMetrics = metricDelta(beforeMiss, fixture.backend.metrics());
		assert(textOutput(miss).includes("worker:v2"), "changed-input miss did not execute the Actor child");
		assert(missMetrics.hits === 0 && missMetrics.misses >= 1, "changed input was incorrectly reused");

		const joinBefore = fixture.backend.metrics();
		const joiningTask = forkReusableBash(fixture, {
			label: "held-joining-producer",
			command: ": speculative-join; worker joined.txt",
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		let joiningBranch: Awaited<typeof joiningTask> | undefined;
		let joiningOutput: Awaited<ReturnType<typeof actor.execute>> | undefined;
		let joiningMs = 0;
		const leadMs = 400;
		try {
			await waitUntil(() => fixture.backend.metrics().misses > joinBefore.misses);
			await delay(leadMs);
			const joiningStarted = performance.now();
			joiningOutput = await joiningActor.execute(
				"held-joining",
				{ command: "printf 'actor-join\\n'; worker joined.txt" },
				new AbortController().signal,
			);
			joiningMs = performance.now() - joiningStarted;
			joiningBranch = await joiningTask;
			assert(!joiningBranch.output.isError, `joining producer failed: ${textOutput(joiningBranch.output.result)}`);
		} finally {
			joiningBranch ??= await joiningTask.catch(() => undefined);
			await joiningBranch?.dispose();
		}
		const joinMetrics = metricDelta(joinBefore, fixture.backend.metrics());
		assert(textOutput(joiningOutput!).includes("actor-join\nworker:v2"), "joined child changed Actor output");
		assert((await readFile(path.join(fixture.workspace, "joined.txt"))).toString() === "artifact:v2\n", "joined child changed workspace result");
		assert(
			joinMetrics.hits === 1 && joinMetrics.joinedHits === 1 && joinMetrics.timedHits === 1 && joinMetrics.avoidedProcessMs > 0,
			`Actor did not join measured in-flight work: ${JSON.stringify(joinMetrics)}`,
		);

		const completedChild = "worker completed.txt volatile";
		const completedBefore = fixture.backend.metrics();
		const completedBranch = await forkReusableBash(fixture, {
			label: "held-completed-producer",
			command: `: speculative-completed; ${completedChild}`,
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		try {
			assert(!completedBranch.output.isError, `completed producer failed: ${textOutput(completedBranch.output.result)}`);
			const completedProduced = metricDelta(completedBefore, fixture.backend.metrics());
			assert(completedProduced.tainted === 1 && completedProduced.published === 0, "completed child did not remain ephemeral");
			const completedActor = await joiningActor.execute(
				"held-completed",
				{ command: `printf 'actor-completed\n'; ${completedChild}` },
				new AbortController().signal,
			);
			const completedMetrics = metricDelta(completedBefore, fixture.backend.metrics());
			assert(textOutput(completedActor).includes("actor-completed\nworker:v2"), "completed child transfer changed Actor output");
			assert((await readFile(path.join(fixture.workspace, "completed.txt"))).toString() === "artifact:v2\n", "completed child transfer changed its effect");
			assert(completedMetrics.hits === 1 && completedMetrics.joinedHits === 0 && completedMetrics.sameTurnHits === 1,
				`Actor did not claim completed same-turn work: ${JSON.stringify(completedMetrics)}`);
		} finally {
			await completedBranch.dispose();
		}
		const lateChild = "worker late.txt volatile";
		const lateBefore = fixture.backend.metrics();
		const lateTask = forkReusableBash(fixture, {
			label: "held-late-producer",
			command: `: speculative-late; ${lateChild}`,
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		let lateBranch: Awaited<typeof lateTask> | undefined;
		try {
			await waitUntil(() => fixture.backend.metrics().misses > lateBefore.misses);
			const laterActor = await heldActor(fixture, fixture.backend, () => ({ sessionID: "benchmark", turnID: "later" }));
			const rejectCrossTurn = async (callID: string) => {
				const before = fixture.backend.metrics();
				const output = await laterActor.execute(callID, { command: lateChild }, new AbortController().signal);
				const metrics = metricDelta(before, fixture.backend.metrics());
				assert(textOutput(output).includes("worker:v2") && metrics.hits === 0 && metrics.joinedHits === 0 && metrics.misses >= 1,
					`${callID} crossed its turn boundary: ${JSON.stringify(metrics)}`);
			};
			await rejectCrossTurn("held-late-running");
			lateBranch = await lateTask;
			assert(!lateBranch.output.isError, `late producer failed: ${textOutput(lateBranch.output.result)}`);
			await rm(path.join(fixture.workspace, "late.txt"));
			await rejectCrossTurn("held-late-completed");
		} finally {
			lateBranch ??= await lateTask.catch(() => undefined);
			await lateBranch?.dispose();
		}

		const descriptorCommand = "exec 3>descriptor.txt; sh -c 'date +%s >/dev/null; printf descriptor >&3'; exec 3>&-; printf descriptor-ok";
		const descriptorProducerBefore = fixture.backend.metrics();
		const descriptorBranch = await forkReusableBash(fixture, {
			label: "held-descriptor-producer",
			command: descriptorCommand,
			actionNamespace: "pi-held-exec-production.v1",
			executionFingerprint,
		});
		try {
			assert(
				!descriptorBranch.output.isError && textOutput(descriptorBranch.output.result) === "descriptor-ok",
				`native descriptor bypass changed output: ${JSON.stringify(descriptorBranch.output)}`,
			);
			const descriptorValidation = await descriptorBranch.validate?.();
			assert(
				descriptorValidation?.status === "indeterminate" &&
				JSON.stringify(descriptorValidation).includes("unparsed_metadata:fstat"),
				`descriptor metadata was not rejected exactly: ${JSON.stringify(descriptorValidation)}`,
			);
			const produced = metricDelta(descriptorProducerBefore, fixture.backend.metrics());
			assert(produced.wholeCommandPublished === 0, "descriptor command unexpectedly entered persistent history");
			const descriptorBefore = fixture.backend.metrics();
			const descriptorActor = await fixture.tool.execute("held-descriptor-actor", { command: descriptorCommand }, new AbortController().signal);
			const descriptorMetrics = metricDelta(descriptorBefore, fixture.backend.metrics());
			assert(textOutput(descriptorActor) === "descriptor-ok", "completed descriptor transfer changed output");
			assert((await readFile(path.join(fixture.workspace, "descriptor.txt"))).toString() === "descriptor", "completed descriptor transfer changed its effect");
			assert(
				descriptorMetrics.wholeCommandHits === 0 && descriptorMetrics.wholeCommandMisses >= 1,
				`descriptor metadata did not force Actor execution: ${JSON.stringify(descriptorMetrics)}`,
			);
		} finally {
			await descriptorBranch.dispose();
		}
		return {
			directMs: direct.totalMs,
			completed: {
				actorMs: hitMs,
				hits: hitMetrics.hits,
				avoidedProcessMs: hitMetrics.avoidedProcessMs,
				producerDependenciesDisabledAtReplay: ["sandlock", "strace"],
			},
			joining: {
				actorMs: joiningMs,
				leadMs,
				hits: joinMetrics.hits,
				joinedHits: joinMetrics.joinedHits,
				estimatedActorMs: joinMetrics.avoidedProcessMs,
				estimatedSavedMs: joinMetrics.avoidedProcessMs - joinMetrics.timedHitOverheadMs,
			},
			completedHandoff: { hits: 1, sameTurnHits: 1, crossTurnCompletedRejected: true, crossTurnRunningRejected: true },
			logicalCwd: { actorMatchedSource: true, absolutePathAliasHits: cwdHits },
			inheritedDescriptor: { completedHandoffHits: 0, exactMetadataFallback: true },
			changedInputMiss: { actorMs: missMs, hits: missMetrics.hits, misses: missMetrics.misses },
			confinementMismatch: {
				producer: "nnp:1",
				actor: "nnp:0",
				hits: securityMetrics.hits,
				rejection: securityMetrics.lastError,
			},
			metadataMismatch: {
				speculativeDiffers: textOutput(inodeBranch.output.result).trim() !== expectedInode,
				actorMatchedSource: true,
				hits: inodeHits,
			},
		};
	} finally {
		await replayBackend.dispose();
		await fixture.dispose();
	}
}

async function heldActor(
	fixture: LinuxProcessBenchmark,
	backend: LinuxProcessReuseBackend,
	scope = () => ({ sessionID: "benchmark", turnID: "benchmark" }),
) {
	const route = await backend.heldExecActorReplay({
		sourceRoot: fixture.workspace,
		realShell: fixture.shellPath,
		enabled: () => true,
		scope,
	});
	const operations = createLocalBashOperations({ shellPath: route.shellPath });
	const coordinator = new ProcessExecutionCoordinator(route.executor(adaptProcessToolOperations(operations)));
	return createBashTool(fixture.workspace, {
		operations: coordinator.operations,
		shellPath: fixture.shellPath,
		exposeSessionEnvironment: false,
		spawnHook: (context) => ({ ...context, env: { ...fixture.environment } }),
	});
}

async function waitUntil(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
	const deadline = performance.now() + timeoutMs;
	while (!predicate()) {
		if (performance.now() >= deadline) throw new Error("timed out waiting for speculative child execution");
		await delay(5);
	}
}

function delay(milliseconds: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
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
