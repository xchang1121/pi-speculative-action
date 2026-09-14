import { spawn } from "node:child_process";
import { lstat, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { createBashTool, createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { EffectCommitFailure } from "../src/effect-transaction.ts";
import { LinuxProcessReuseBackend } from "../src/linux-process-backend.ts";
import { adaptProcessToolOperations, ProcessExecutionCoordinator } from "../src/process-execution.ts";
import {
	argument,
	assert,
	BENCHMARK_SCOPE,
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
	waitUntil,
	writeBenchmarkReport,
} from "./linux-process-harness.ts";

interface Outcome { readonly code: number | null; readonly signal: NodeJS.Signals | null; readonly stdout: Buffer; readonly stderr: Buffer; readonly durationMs: number }
type Command = readonly [string, ...string[]];

if (process.platform !== "linux") throw new Error("Run this benchmark inside Linux or WSL 2");
const output = argument("--output");
const root = await mkdtemp(path.join(os.tmpdir(), "pi-exec-boundary-"));
const tracer = path.join(root, "exec-events");
try {
	await compileBenchmarkHelper(root, {
		source: fileURLToPath(new URL("../src/linux-held-exec.c", import.meta.url)),
		output: path.basename(tracer),
		arguments: ["-Werror", "-pthread"],
	});
	const equivalence: Command = ["/bin/bash", "-c", "printf out; printf err >&2; exit 7"];
	assertSame(await run(equivalence), await run([tracer, ...equivalence]));
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
	if (detached.code !== 0 || detached.durationMs < 900) throw new Error("ptrace released an owned child after parent exit");
	const childConversion = process.arch === "x64" ? await conversionAblation(tracer) : undefined;
	await writeBenchmarkReport({
		schemaVersion: 2,
		measuredAt: new Date().toISOString(),
		host: await linuxBenchmarkHost(),
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

function assertSame(expected: Outcome, actual: Outcome): void {
	if (expected.code !== actual.code || expected.signal !== actual.signal || !expected.stdout.equals(actual.stdout) || !expected.stderr.equals(actual.stderr))
		throw new Error("pass-through tracer changed the command result");
}

async function conversionAblation(heldExecBinary: string) {
	const fixture = await createLinuxProcessBenchmark("pi-held-production-");
	const replayBackend = new LinuxProcessReuseBackend({
		storeRoot: fixture.storeRoot,
		heldExecBinary,
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
		const produce = (label: string, command: string) => forkReusableBash(fixture, {
			label, command, actionNamespace: "pi-held-exec-production.v1", executionFingerprint,
		});
		async function withProducer<Value>(label: string, command: string,
			inspect: (production: ReturnType<typeof produce>) => Value | Promise<Value>) {
			const production = produce(label, command);
			const settled = production.catch(() => undefined);
			try {
				return await inspect(production);
			} catch (error) {
				// Capture evidence while the branch is still owned; a failed run has no success report.
				const branch = await settled;
				const [validation, storage] = await Promise.all([
					branch?.validate?.().catch((reason: unknown) => ({ error: String(reason) })),
					fixture.backend.store.stats().catch((reason: unknown) => ({ error: String(reason) })),
				]);
				await writeBenchmarkReport({
					schemaVersion: 1, status: "failed", label, command, scope: BENCHMARK_SCOPE,
					error: error instanceof Error ? error.stack : String(error),
					output: branch?.output, execution: branch?.executionMetrics,
					producerAndActor: fixture.backend.metrics(), actor: fixture.backend.actorMetrics(),
					validation, storage,
				}, output ? `${output}.failure.json` : undefined).catch((reason: unknown) => {
					process.stderr.write(`Could not save process failure evidence: ${String(reason)}\n`);
				});
				throw error;
			} finally {
				await (await settled)?.dispose();
			}
		}
		const actorCommand = "printf 'actor-parent\\n'; worker result.txt";
		const direct = await executeDirectBash(fixture, { label: "held-direct", command: actorCommand });
		const expectedOutput = textOutput(direct.output);
		const expectedResult = await readFile(path.join(fixture.workspace, "result.txt"));
		await rm(path.join(fixture.workspace, "result.txt"));
		await withProducer("held-producer", ": speculative-parent; worker result.txt", async (production) => {
			const branch = await production;
			assert(!branch.output.isError, `speculative child failed: ${textOutput(branch.output.result)} ${JSON.stringify(fixture.backend.metrics())}`);
			assert(fixture.backend.metrics().published > 0, `speculative child did not publish a reusable certificate: ${JSON.stringify(fixture.backend.metrics())}`);
		});
		const actor = await heldActor(fixture, replayBackend);
		const { output: hit, totalMs: hitMs, metrics: hitMetrics } = await measureActor(replayBackend, actor, "held-hit", actorCommand);
		assert(textOutput(hit) === expectedOutput, "held child changed Actor output");
		assert((await readFile(path.join(fixture.workspace, "result.txt"))).equals(expectedResult), "held child changed workspace result");
		assert(
			hitMetrics.hits === 1 && hitMetrics.actorTimedHits === 0 && hitMetrics.actorBaselineMs === 0 &&
				hitMetrics.reusedProcessMs > 0,
			`uncalibrated held child did not separate reused work from Actor timing: ${JSON.stringify(hitMetrics)}`,
		);
		const joiningActor = await heldActor(fixture, fixture.backend);

		const cwdProducerBefore = fixture.backend.metrics();
		const cwdHits = await withProducer("held-cwd-producer", "/bin/pwd", async (production) => {
			const cwdBranch = await production;
			const cwdProduced = metricDelta(cwdProducerBefore, fixture.backend.metrics());
			assert(textOutput(cwdBranch.output.result) === `${fixture.workspace}\n`, "speculative child observed a private cwd");
			const { output: cwdActor, metrics: cwdMetrics } = await measureActor(
				fixture.backend, joiningActor, "held-cwd-actor", "printf 'actor-cwd\\n'; /bin/pwd");
			assert(textOutput(cwdActor) === `actor-cwd\n${fixture.workspace}\n`, "transferred child observed a non-Actor cwd");
			assert(
				cwdMetrics.hits === 1,
				`absolute PATH alias was not transferred: producer=${JSON.stringify(cwdProduced)} actor=${JSON.stringify(cwdMetrics)}`,
			);
			return cwdMetrics.hits;
		});

		const securityBefore = fixture.backend.metrics();
		await withProducer("held-security-producer", ": speculative-security; worker unused probe", async (production) => {
			const securityBranch = await production;
			assert(textOutput(securityBranch.output.result).includes("nnp:1"), "producer confinement probe was not active");
			const produced = metricDelta(securityBefore, fixture.backend.metrics());
			assert(produced.tainted === 1 && produced.published === 1,
				`confinement evidence was not retained: ${JSON.stringify(produced)}; validation=${JSON.stringify(await securityBranch.validate?.())}`);
		});
		const { output: securityActor, metrics: securityMetrics } = await measureActor(
			replayBackend, actor, "held-security-actor", ": actor-security; worker unused probe");
		assert(textOutput(securityActor).includes("nnp:0"), "Actor did not retain its native security context");
		assert(
			securityMetrics.hits === 0 && securityMetrics.misses >= 1 && securityMetrics.lastError?.includes("certificate_tainted"),
			`confinement-sensitive result was reused: ${JSON.stringify(securityMetrics)}`,
		);

		const metadataMismatch = await withProducer("held-inode-producer", ": speculative-inode; worker unused inode", async (production) => {
			const inodeBranch = await production;
			const expectedInode = (await lstat(path.join(fixture.workspace, "input.txt"), { bigint: true })).ino
				.toString(16).padStart(16, "0");
			const { output: inodeActor, metrics: inodeMetrics } = await measureActor(
				replayBackend, actor, "held-inode-actor", ": actor-inode; worker unused inode");
			assert(
				textOutput(inodeActor).trim() === expectedInode,
				`Actor observed speculative inode metadata: expected ${expectedInode}, got ${JSON.stringify(textOutput(inodeActor).trim())}; ${JSON.stringify(inodeMetrics)}`,
			);
			assert(inodeMetrics.hits === 0 && inodeMetrics.misses >= 1, "non-equivalent inode metadata was reused");
			return { speculativeDiffers: textOutput(inodeBranch.output.result).trim() !== expectedInode,
				actorMatchedSource: true, hits: inodeMetrics.hits };
		});

		await Promise.all([
			writeFile(path.join(fixture.workspace, "input.txt"), "v2\n"),
			rm(path.join(fixture.workspace, "result.txt")),
		]);
		const joinBefore = fixture.backend.metrics();
		const leadMs = 400;
		const joining = await withProducer("held-joining-producer", ": speculative-join; worker joined.txt", async (production) => {
			await waitUntil(() => fixture.backend.metrics().misses > joinBefore.misses, 5_000, 5);
			await delay(leadMs);
			const result = await measureActor(fixture.backend, joiningActor, "held-joining", "printf 'actor-join\\n'; worker joined.txt");
			const joiningBranch = await production;
			assert(!joiningBranch.output.isError, `joining producer failed: ${textOutput(joiningBranch.output.result)}`);
			assert(textOutput(result.output) === "actor-join\nworker:v2\n", "Actor child output was lost or executed more than once");
			assert((await readFile(path.join(fixture.workspace, "joined.txt"))).toString() === "artifact:v2\n", "joined child changed workspace result");
			const metrics = result.metrics;
			assert(metrics.requests === 1 && metrics.hits === 1 && metrics.joinedHits === 1 &&
				metrics.actorTimedHits === 0 && metrics.actorBaselineMs === 0 && metrics.reusedProcessMs > 0,
				`Uncalibrated Actor did not join its child exactly once: ${JSON.stringify(metrics)}`);
			return result;
		});
		const { output: miss, totalMs: missMs, metrics: missMetrics } = await measureActor(fixture.backend, joiningActor, "held-stale", actorCommand);
		assert(textOutput(miss).includes("worker:v2"), "changed-input miss did not execute the Actor child");
		assert(missMetrics.hits === 0 && missMetrics.misses >= 1, "changed input was incorrectly reused");

		const completedChild = "worker completed.txt volatile";
		const completedBefore = fixture.backend.metrics();
		await withProducer("held-completed-producer", `: speculative-completed; ${completedChild}`, async (production) => {
			const completedBranch = await production;
			assert(!completedBranch.output.isError, `completed producer failed: ${textOutput(completedBranch.output.result)}`);
			const completedProduced = metricDelta(completedBefore, fixture.backend.metrics());
			assert(completedProduced.tainted === 1 && completedProduced.published === 0,
				`completed child did not remain ephemeral: ${JSON.stringify(completedProduced)}`);
			const { output: completedActor, metrics: completedMetrics } = await measureActor(
				fixture.backend, joiningActor, "held-completed", `printf 'actor-completed\n'; ${completedChild}`);
			assert(textOutput(completedActor).includes("actor-completed\nworker:v2"), "completed child transfer changed Actor output");
			assert((await readFile(path.join(fixture.workspace, "completed.txt"))).toString() === "artifact:v2\n", "completed child transfer changed its effect");
			assert(completedMetrics.hits === 1 && completedMetrics.joinedHits === 0 && completedMetrics.sameTurnHits === 1,
				`Actor did not claim completed same-turn work: ${JSON.stringify(completedMetrics)}`);
			assert(await completedBranch.commit().then(() => false, (error) =>
				error instanceof EffectCommitFailure && error.disposition === "recoverable" && error.message.includes("partially consumed")),
				"enclosing branch retained adoption authority after its one-shot child was consumed");
		});
		const lateChild = "worker late.txt volatile";
		const lateBefore = fixture.backend.metrics();
		await withProducer("held-late-producer", `: speculative-late; ${lateChild}`, async (production) => {
			await waitUntil(() => fixture.backend.metrics().misses > lateBefore.misses, 5_000, 5);
			const laterActor = await heldActor(fixture, fixture.backend, () => ({ sessionID: "benchmark", turnID: "later" }));
			const rejectCrossTurn = async (callID: string) => {
				const { output, metrics } = await measureActor(fixture.backend, laterActor, callID, lateChild);
				assert(textOutput(output).includes("worker:v2") && metrics.hits === 0 && metrics.joinedHits === 0 && metrics.misses >= 1,
					`${callID} crossed its turn boundary: ${JSON.stringify(metrics)}`);
			};
			await rejectCrossTurn("held-late-running");
			const lateBranch = await production;
			assert(!lateBranch.output.isError, `late producer failed: ${textOutput(lateBranch.output.result)}`);
			await rm(path.join(fixture.workspace, "late.txt"));
			await rejectCrossTurn("held-late-completed");
		});

		const descriptorCommand = "exec 3>descriptor.txt; sh -c 'date +%s >/dev/null; printf descriptor >&3'; exec 3>&-; printf descriptor-ok";
		const descriptorProducerBefore = fixture.backend.metrics();
		await withProducer("held-descriptor-producer", descriptorCommand, async (production) => {
			const descriptorBranch = await production;
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
			const { output: descriptorActor, metrics: descriptorMetrics } = await measureActor(
				fixture.backend, fixture.tool, "held-descriptor-actor", descriptorCommand);
			assert(textOutput(descriptorActor) === "descriptor-ok", "completed descriptor transfer changed output");
			assert((await readFile(path.join(fixture.workspace, "descriptor.txt"))).toString() === "descriptor", "completed descriptor transfer changed its effect");
			assert(
				descriptorMetrics.wholeCommandHits === 0 && descriptorMetrics.wholeCommandMisses >= 1,
				`descriptor metadata did not force Actor execution: ${JSON.stringify(descriptorMetrics)}`,
			);
		});
		return {
			directMs: direct.totalMs,
			completed: {
				actorMs: hitMs,
				hits: hitMetrics.hits,
				reusedProcessMs: hitMetrics.reusedProcessMs,
				actorTimingAvailable: hitMetrics.actorTimedHits > 0,
				producerDependenciesDisabledAtReplay: ["sandlock", "strace"],
			},
			joining: {
				// A fixed arrival lead is a workload parameter, not a promise that joining beats fallback.
				disposition: "joined",
				actorMs: joining.totalMs,
				leadMs,
				hits: joining.metrics.hits,
				joinedHits: joining.metrics.joinedHits,
				estimatedActorMs: null,
				estimatedSavedMs: null,
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
			metadataMismatch,
		};
	} finally {
		await replayBackend.dispose();
		await fixture.dispose();
	}
}

async function measureActor(backend: LinuxProcessReuseBackend, actor: LinuxProcessBenchmark["tool"], callID: string, command: string) {
	const before = backend.actorMetrics(), started = performance.now();
	const output = await actor.execute(callID, { command }, new AbortController().signal);
	return { output, totalMs: performance.now() - started, metrics: metricDelta(before, backend.actorMetrics()) };
}

async function heldActor(
	fixture: LinuxProcessBenchmark,
	backend: LinuxProcessReuseBackend,
	scope = () => ({ sessionID: "benchmark", turnID: "benchmark" }),
) {
	const route = await backend.prepareActorReplay(adaptProcessToolOperations(createLocalBashOperations()), {
		sourceRoot: fixture.workspace,
		invocation: () => undefined,
		held: {
			realShell: fixture.shellPath,
			executor: (shellPath) => adaptProcessToolOperations(createLocalBashOperations({ shellPath })),
			scope,
		},
	});
	if (!("executor" in route)) throw new Error(route.detail);
	const coordinator = new ProcessExecutionCoordinator(route.executor);
	return createBashTool(fixture.workspace, {
		operations: coordinator.operations,
		shellPath: fixture.shellPath,
		exposeSessionEnvironment: false,
		spawnHook: (context) => ({ ...context, env: { ...fixture.environment } }),
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
		child.once("close", (code, signal) => resolve({ code, signal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr), durationMs: performance.now() - started }));
	});
}
