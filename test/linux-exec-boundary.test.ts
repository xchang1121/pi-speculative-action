import { onExit } from "./async.ts";
import { lstat, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { expect } from "vitest";
import type { HeldExecProcess, HeldExecDecision } from "../src/linux-held-exec.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";
import { cHeaders, commitBenchmarkFixture, compileBenchmarkHelper, processTest,
	executeProcess, forkReusableBash, holdProcessPublication, metricDelta, prepareLinuxProcessReuse, textOutput } from "./linux-process-fixture.ts";

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
	const actor = async (command: string, turnID = "benchmark", held = true) => {
		const before = backend.actorMetrics();
		const output = held ? (await executeProcess(fixture, route.executor, command, { scope: { sessionID: "benchmark", turnID } })).output : textOutput(await fixture.tool.execute("actor", { command }));
		return { output, metrics: metricDelta(before, backend.actorMetrics()) };
	};
	const decide = Reflect.get(backend, "decideHeldExec") as (process: HeldExecProcess, scope?: HeldExecProcess["scope"]) => Promise<HeldExecDecision>;
	let discovered: Awaited<ReturnType<typeof produce>> | undefined, decisions = 0;
	await backend.store.put(PROCESS_CERTIFICATE); // Unrelated history activates held execution before any matching producer exists.
	Reflect.set(backend, "decideHeldExec", async (...args: Parameters<typeof decide>) => {
		const decision = await decide.apply(backend, args); decisions++;
		if (decisions === 1) { expect(decision.kind).toBe("continue"); discovered = await produce(": speculative-late; /bin/cat input.txt"); }
		return decision;
	});
	try {
		expect(await actor("/bin/cat input.txt; /bin/cat input.txt")).toMatchObject({ output: "before\nbefore\n", metrics: { hits: 1 } });
		expect(decisions).toBe(2); expect((await actor("/bin/cat input.txt")).metrics.hits).toBe(0);
	} finally { Reflect.set(backend, "decideHeldExec", decide); await discovered?.dispose(); }
	const descriptor = "exec 3>descriptor.txt; sh -c 'date +%s >/dev/null; printf descriptor >&3'; exec 3>&-; printf descriptor-ok";
	const hit = { hits: 1, joinedHits: 0, sameTurnHits: 1 }, miss = { hits: 0, joinedHits: 0 };
	const cases = [
		{ name: "disposed", command: "worker disposed.txt", expected: "worker:before\n", file: "disposed.txt", metrics: hit },
		{ name: "completed", command: "worker completed.txt volatile", expected: "worker:before\n", file: "completed.txt", metrics: hit },
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
		const older = scenario.name === "running" ? await produce(`: earlier-input; ${scenario.command}`) : undefined;
		if (older) await writeFile(path.join(workspace, "input.txt"), "changed while waiting\n");
		const publication = older ? holdProcessPublication(backend, 1500, () => writeFile(path.join(workspace, "input.txt"), "before\n")) : undefined;
		const pending = produce(`: speculative-parent; ${scenario.command}`), settled = pending.catch(() => undefined);
		await using _cleanup = onExit(async () => { publication?.close(); await (await settled)?.dispose(); await older?.dispose(); });
		for (const phase of scenario.name === "foreign" ? ["running", "completed"] : [publication ? "running" : "completed"]) {
			if (phase === "running") {
				if (publication) await expect.poll(publication.reached, { timeout: 5000 }).toBe(true);
				else await expect.poll(() => backend.metrics().misses).toBeGreaterThan(before.misses);
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
				Reflect.set(backend, "decideHeldExec", async (...args: Parameters<typeof decide>) => {
					const decision = await decide.apply(backend, args);
					return decision.kind === "replay" ? { ...decision, descriptorOffsets: [{ fd: 63, device: "0", inode: "0", flags: 0, before: 0, after: 0 }] } : decision;
				});
				try { expect((await actor(scenario.command)).metrics.hits).toBe(0); }
				finally { Reflect.set(backend, "decideHeldExec", decide); }
				await rm(path.join(workspace, scenario.file!));
			}
			const result = await actor(`printf 'actor-parent\\n'; ${scenario.command}`, scenario.name === "foreign" ? "later" : "benchmark", scenario.name !== "descriptor");
			if (publication) expect(publication.joined()).toBe(true);
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
