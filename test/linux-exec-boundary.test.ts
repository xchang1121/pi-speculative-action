import { lstat, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { createLocalBashOperations } from "@earendil-works/pi-coding-agent";
import { expect, test } from "vitest";
import { adaptProcessToolOperations } from "../src/process-execution.ts";
import {
	commitBenchmarkFixture, compileBenchmarkHelper, createLinuxProcessBenchmark,
	forkReusableBash, holdProcessPublication, metricDelta, prepareLinuxProcessReuse, textOutput,
} from "./linux-process-fixture.ts";

test("preserves held-child output, effects and one-shot authority across parent commands", { timeout: 30_000 }, async ({ skip }) => {
	if (process.platform !== "linux" || process.arch !== "x64") return skip("x86-64 Linux only");
	const fixture = await createLinuxProcessBenchmark("pi-held-production-");
	const { backend, workspace } = fixture;
	try {
		const status = await backend.check(true);
		if (status.state !== "ready") return skip(status.detail);
		await writeFile(path.join(workspace, "input.txt"), "before\n");
		await writeFile(path.join(workspace, "worker.c"), String.raw`
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
		await compileBenchmarkHelper(workspace, { source: "worker.c", output: "worker" });
		await commitBenchmarkFixture(workspace, "Pi Held Exec Qualification");
		const { executionFingerprint } = await prepareLinuxProcessReuse(fixture);
		const route = await backend.prepareActorReplay(adaptProcessToolOperations(createLocalBashOperations()), {
			sourceRoot: workspace, invocation: () => undefined, held: { realShell: fixture.shellPath,
				executor: shellPath => adaptProcessToolOperations(createLocalBashOperations({ shellPath })) },
		});
		if (!("executor" in route)) throw new Error(route.detail);
		const produce = (command: string) => forkReusableBash(fixture, {
			label: "producer", command, actionNamespace: "held-production", executionFingerprint,
		});
		const actor = async (command: string, turnID = "benchmark", held = true) => {
			const before = backend.actorMetrics();
			const execute = () => fixture.tool.execute("actor", { command });
			const output = held ? await fixture.coordinator.runWith({ execute: request => route.executor.execute({ ...request,
				scope: { sessionID: "benchmark", turnID } }) }, execute) : await execute();
			return { output: textOutput(output), metrics: metricDelta(before, backend.actorMetrics()) };
		};
		const descriptor = "exec 3>descriptor.txt; sh -c 'date +%s >/dev/null; printf descriptor >&3'; exec 3>&-; printf descriptor-ok";
		const cases = [
			{ name: "disposed", command: "worker disposed.txt", expected: "worker:before\n", file: "disposed.txt" },
			{ name: "completed", command: "worker completed.txt volatile", expected: "worker:before\n", file: "completed.txt" },
			{ name: "running", command: "worker joined.txt", expected: "worker:before\n", file: "joined.txt" },
			{ name: "stale", command: "worker stale.txt", expected: "worker:after\n", file: "stale.txt" },
			{ name: "cwd", command: "/bin/pwd", expected: `${workspace}\n` },
			{ name: "security", command: "worker unused probe", expected: "nnp:0\n" },
			{ name: "inode", command: "worker unused inode", expected: (await lstat(path.join(workspace, "input.txt"), { bigint: true })).ino.toString(16).padStart(16, "0") + "\n" },
			{ name: "descriptor", command: descriptor, expected: "descriptor-ok", file: "descriptor.txt" },
		];
		for (const scenario of cases) {
			const before = backend.metrics();
			const publication = scenario.name === "running" ? holdProcessPublication(backend) : undefined;
			const pending = produce(`: speculative-parent; ${scenario.command}`);
			// Observe rejection immediately, while keeping the branch owned until every Actor has returned.
			const settled = pending.catch(() => undefined);
			try {
				if (scenario.name === "running") {
					await expect.poll(publication!.reached, { timeout: 5000 }).toBe(true);
				} else {
					const branch = await pending;
					expect(branch.output.isError, scenario.name).toBe(false);
					const produced = metricDelta(before, backend.metrics());
					if (["disposed", "completed", "security"].includes(scenario.name)) {
						expect(produced.tainted).toBeGreaterThan(0);
						expect(produced.published).toBe(0);
					}
					if (scenario.name === "security") expect(textOutput(branch.output.result)).toBe("nnp:1\n");
					if (scenario.name === "cwd") expect(textOutput(branch.output.result)).toBe(scenario.expected);
					if (scenario.name === "descriptor") {
						expect(textOutput(branch.output.result)).toBe("descriptor-ok");
						const validation = await branch.validate?.();
						expect(validation?.status).toBe("indeterminate");
						expect(produced.wholeCommandPublished).toBe(0);
					}
					if (scenario.name === "disposed") await branch.dispose();
					if (scenario.name === "stale") await writeFile(path.join(workspace, "input.txt"), "after\n");
				}
				const result = await actor(`printf 'actor-parent\\n'; ${scenario.command}`, "benchmark", scenario.name !== "descriptor");
				expect(result.output, scenario.name).toBe(`actor-parent\n${scenario.expected}`);
				if (scenario.file) expect(await readFile(path.join(workspace, scenario.file), "utf8"))
					.toBe(scenario.name === "descriptor" ? "descriptor" : `artifact:${scenario.name === "stale" ? "after" : "before"}\n`);
				if (["disposed", "completed", "running", "cwd"].includes(scenario.name)) {
					expect(result.metrics, `${scenario.name}: ${JSON.stringify(result.metrics)}`).toMatchObject({ hits: 1, joinedHits: Number(scenario.name === "running"), sameTurnHits: 1,
						reusedProcessMs: expect.any(Number) });
					expect(result.metrics.reusedProcessMs).toBeGreaterThan(0);
				} else if (scenario.name === "descriptor") {
					expect(result.metrics.wholeCommandHits).toBe(0);
					expect(result.metrics.wholeCommandMisses).toBeGreaterThan(0);
				} else {
					expect(result.metrics.hits).toBe(0);
					expect(result.metrics.misses).toBeGreaterThan(0);
					if (scenario.name === "security") expect(result.metrics.lastError).toContain("certificate_tainted");
				}
				const branch = await pending;
				expect(branch.output.isError, scenario.name).toBe(false);
				if (scenario.name === "completed") await expect(branch.commit()).rejects.toMatchObject({
					disposition: "recoverable", message: expect.stringContaining("partially consumed"),
				});
			} finally { publication?.close(); await (await settled)?.dispose(); }
		}
		const before = backend.metrics(), pending = produce(": speculative-late; worker late.txt volatile");
		const settled = pending.catch(() => undefined);
		try {
			await expect.poll(() => backend.metrics().misses).toBeGreaterThan(before.misses);
			for (const state of ["running", "completed"]) {
				if (state === "completed") {
					expect((await pending).output.isError).toBe(false);
					await rm(path.join(workspace, "late.txt"));
				}
				const result = await actor("worker late.txt volatile", "later");
				expect(result.output).toBe("worker:after\n");
				expect(await readFile(path.join(workspace, "late.txt"), "utf8")).toBe("artifact:after\n");
				expect(result.metrics).toMatchObject({ hits: 0, joinedHits: 0 });
				expect(result.metrics.misses).toBeGreaterThan(0);
			}
		} finally { await (await settled)?.dispose(); }
	} finally { await fixture.dispose(); }
});
