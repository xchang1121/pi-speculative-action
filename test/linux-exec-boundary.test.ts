import { disposable, onExit } from "./async.ts";
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { lstat, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { expect } from "vitest";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";
import { resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { cHeaders, commitBenchmarkFixture, compileBenchmarkHelper, processTest,
	executeProcess, forkReusableBash, holdOwnedProcessPublication, metricDelta, prepareLinuxProcessReuse, textOutput } from "./linux-process-fixture.ts";

(process.arch === "x64" ? processTest : processTest.skip).for(["git", "auto"] as const)("preserves held-child output, effects and one-shot authority across parent commands (%s)", { timeout: 30_000 }, async (driver, { benchmark, skip }) => {
	const fixture = await benchmark("pi-held-production-", driver);
	const { backend, workspace } = fixture;
	await writeFile(path.join(workspace, "input.txt"), "before\n");
	await compileBenchmarkHelper(workspace, { source: "worker.c", output: "worker", code: String.raw`
${cHeaders("fcntl.h sys/random.h sys/prctl.h time.h sys/stat.h unistd.h")}
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
` });
	await mkdir(path.join(workspace, "d", "e"), { recursive: true });
	await Promise.all([writeFile(path.join(workspace, "d", "e", ".keep"), ""), writeFile(path.join(workspace, "data.txt"), "v1\n"), writeFile(path.join(workspace, "d", "data.txt"), "nested\n"),
		symlink("data.txt", path.join(workspace, "data.link")), symlink("d/e", path.join(workspace, "sub"))]);
	await commitBenchmarkFixture(workspace, "Pi Held Exec Qualification");
	const { executionFingerprint } = await prepareLinuxProcessReuse(fixture, { skip });
	const route = await fixture.prepareActorReplay();
	const produce = (command: string) => forkReusableBash(fixture, {
		label: "producer", command, actionNamespace: "held-production", executionFingerprint,
	});
	const actor = async (command: string, turnID = "benchmark", held: boolean | "sync" = true) => {
		const before = backend.actorMetrics();
		const scope = { sessionID: "benchmark", turnID };
		const output = held === "sync" ? await backend.observeNative(workspace, scope, undefined, async () => execFileSync(fixture.shellPath, ["-c", command],
			{ cwd: workspace, env: { ...fixture.environment }, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }))
			: held ? (await executeProcess(fixture, route.executor, command, { scope })).output : textOutput(await fixture.tool.execute("actor", { command }));
		return { output, metrics: metricDelta(before, backend.actorMetrics()) };
	};
	let discovered: Awaited<ReturnType<typeof produce>> | undefined;
	await backend.store.put(PROCESS_CERTIFICATE); // Unrelated history activates held execution before any matching producer exists.
	await fixture.inOwner(async state => {
		const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), decide = LinuxProcessReuseBackend.prototype.decideHeldExec;
		state.reached = state.deferred(); state.release = state.deferred(); state.decisions = 0;
		state.deciding = state.method(LinuxProcessReuseBackend.prototype, "decideHeldExec", async function (this: unknown, ...args: unknown[]) {
			state.backend = this; const decision = await decide.apply(this, args);
			if (++state.decisions === 1) { state.reached.resolve(decision.kind); await state.release.promise; }
			return decision;
		});
	});
	const discovering = actor("/bin/cat input.txt; /bin/cat input.txt");
	try {
		expect(await fixture.inOwner(state => state.reached.promise)).toBe("continue"); discovered = await produce(": speculative-late; /bin/cat input.txt");
		await fixture.inOwner(state => state.release.resolve());
		expect(await discovering).toMatchObject({ output: "before\nbefore\n", metrics: { hits: 1 } });
		expect(await fixture.inOwner(state => state.decisions)).toBe(2); expect((await actor("/bin/cat input.txt")).metrics.hits).toBe(0);
	} finally { await fixture.inOwner(state => { state.release.resolve(); state.deciding.mock.restore(); }); await discovering; await discovered?.dispose(); }
	const descriptor = "exec 3>descriptor.txt; sh -c 'date +%s >/dev/null; printf descriptor >&3'; exec 3>&-; printf descriptor-ok";
	const hit = { hits: 1, joinedHits: 0, sameTurnHits: 1 }, miss = { hits: 0, joinedHits: 0 };
	const cases = [
		{ name: "disposed", command: "worker disposed.txt", expected: "worker:before\n", file: "disposed.txt", metrics: hit },
		{ name: "completed", command: "worker completed.txt volatile", expected: "worker:before\n", file: "completed.txt", metrics: hit },
		{ name: "competing", command: "worker competing.txt volatile", expected: "worker:before\n", file: "competing.txt", metrics: hit },
		{ name: "running", command: "worker joined.txt", expected: "worker:before\n", file: "joined.txt", metrics: { ...hit, joinedHits: 1 } },
		{ name: "stale", command: "worker stale.txt", expected: "worker:after\n", file: "stale.txt", metrics: miss },
		{ name: "cwd", command: "/bin/pwd", expected: `${workspace}\n`, metrics: hit },
		{ name: "security", command: "worker unused probe", expected: "nnp:0\n", metrics: { ...miss, lastError: expect.stringContaining("certificate_tainted") } },
		{ name: "inode", command: "worker unused inode", expected: (await lstat(path.join(workspace, "input.txt"), { bigint: true })).ino.toString(16).padStart(16, "0") + "\n", metrics: hit },
		{ name: "descriptor", command: descriptor, expected: "descriptor-ok", file: "descriptor.txt", metrics: { wholeCommandHits: 0 } },
		{ name: "link", command: "head -n 1 data.link", expected: "v2\n", metrics: miss },
		{ name: "dotdot", command: "head -n 1 sub/../data.txt", expected: "nested\n", metrics: miss },
		{ name: "foreign", command: "worker late.txt volatile", expected: "worker:after\n", file: "late.txt", metrics: miss },
	];
	for (const scenario of cases) {
		const before = backend.metrics();
		const ownerBefore = await fixture.inOwner(state => state.backend.metrics());
		const older = scenario.name === "running" ? await produce(`: earlier-input; ${scenario.command}`) : undefined;
		if (older) await writeFile(path.join(workspace, "input.txt"), "changed while waiting\n");
		const publication = older ? await holdOwnedProcessPublication(fixture, () => writeFile(path.join(workspace, "input.txt"), "before\n")) : undefined;
		const pending = produce(`: speculative-parent; ${scenario.name === "competing" ? "/bin/true" : scenario.command}`), settled = pending.catch(() => undefined);
		await using _cleanup = onExit(async () => { await publication?.close(); await (await settled)?.dispose(); await older?.dispose(); });
		for (const phase of scenario.name === "foreign" ? ["running", "completed"] : [publication ? "running" : "completed"]) {
			if (phase === "running") {
				if (publication) await publication.ready();
				else await expect.poll(() => fixture.inOwner(state => state.backend.metrics().misses as number)).toBeGreaterThan(ownerBefore.misses);
			} else {
				const branch = await pending, produced = metricDelta(before, backend.metrics());
				if (["disposed", "completed", "security"].includes(scenario.name)) {
					expect(produced.tainted).toBeGreaterThan(0);
					expect(produced.published).toBe(0);
				}
				if (scenario.name === "security") expect(textOutput(branch.output.result)).toBe("nnp:1\n");
				if (scenario.name === "cwd") expect(textOutput(branch.output.result)).toBe(scenario.expected);
				if (scenario.name === "descriptor") {
					expect(textOutput(branch.output.result)).toBe("descriptor-ok");
					expect((await branch.validate?.())?.status).toBe("valid"); // The clock read by date makes this a one-shot transfer.
					expect(produced.wholeCommandPublished).toBe(0);
				}
				if (scenario.name === "disposed") await branch.dispose();
				if (scenario.name === "stale" || scenario.name === "link") await writeFile(path.join(workspace, scenario.name === "stale" ? "input.txt" : "data.txt"), scenario.name === "stale" ? "after\n" : "v2\n");
				if (scenario.name === "foreign") await rm(path.join(workspace, scenario.file!));
			}
			if (scenario.name === "completed") {
				await backend.store.clear(); // Live effects survive disk loss and a recoverable native commit failure.
				await fixture.inOwner(async state => {
					const { LinuxProcessReuseBackend } = await state.load("../src/linux-process-backend.ts"), decide = LinuxProcessReuseBackend.prototype.decideHeldExec;
					state.deciding = state.method(LinuxProcessReuseBackend.prototype, "decideHeldExec", async function (this: unknown, ...args: unknown[]) {
						const decision = await decide.apply(this, args);
						return decision.kind === "replay" ? { ...decision, descriptorOffsets: [{ fd: 63, device: "0", inode: "0", flags: 0, before: 0, after: 0 }] } : decision;
					});
				});
				try { expect((await actor(scenario.command)).metrics.hits).toBe(0); }
				finally { await fixture.inOwner(state => state.deciding.mock.restore()); }
				await rm(path.join(workspace, scenario.file!));
			}
			const consume = () => actor(`printf 'actor-parent\\n'; ${scenario.command}`, scenario.name === "foreign" ? "later" : "benchmark", scenario.name === "competing" ? "sync" : scenario.name !== "descriptor");
			const result = ["completed", "running", "competing"].includes(scenario.name) ? await fixture.workspaceSandbox.withWorkspace(workspace, async privateWorkspace => {
				const command = scenario.name === "competing" ? `: local-parent; ${scenario.command}` : "/bin/true";
				const invocation = resolvePiToolInvocation("bash", { command }, { cwd: workspace, environment: fixture.environment, shellPath: fixture.shellPath })!.process!;
				await using local = disposable(await backend.open({ sourceRoot: workspace, workspace: privateWorkspace, invocation, scope: { sessionID: "benchmark", turnID: "benchmark" } }));
				expect((await executeProcess(fixture, local.executor, command)).exitCode).toBe(0);
				if (scenario.name !== "competing") return await consume();
				return backend.observeNative(workspace, { sessionID: "benchmark", turnID: "benchmark", callID: "unrelated" }, undefined, async () => {
					const child = spawn("/bin/sleep", ["30"], { cwd: workspace, env: { ...fixture.environment }, stdio: "ignore" }), closed = once(child, "close");
					try { return await consume(); } finally { child.kill(); await closed; }
				});
			}) : await consume();
			if (publication) expect(await publication.joined()).toBe(true);
			expect(result.output, scenario.name).toBe(`actor-parent\n${scenario.expected}`);
			if (scenario.file) expect(await readFile(path.join(workspace, scenario.file), "utf8"))
				.toBe(scenario.name === "descriptor" ? "descriptor" : `artifact:${["stale", "foreign"].includes(scenario.name) ? "after" : "before"}\n`);
			expect(result.metrics, scenario.name).toMatchObject(scenario.metrics);
			if ("hits" in scenario.metrics && scenario.metrics.hits) expect(result.metrics.reusedProcessMs).toBeGreaterThan(0);
			else expect(result.metrics[scenario.name === "descriptor" ? "wholeCommandMisses" : "misses"]).toBeGreaterThan(0);
			const branch = await pending;
			expect(branch.output.isError, `${scenario.name}: ${textOutput(branch.output.result)}`).toBe(false);
			if (scenario.name === "completed") await expect(branch.commit()).rejects.toMatchObject({
				disposition: "recoverable", message: expect.stringContaining("partially consumed"),
			});
			if (scenario.name === "cwd") for (let repeat = 0; repeat < 3; repeat++) expect((await actor(scenario.command)).metrics.hits).toBe(0);
		}
	}
});
