import { lstat, readFile, utimes } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect } from "vitest";
import { FILESYSTEM_OBSERVATION_FIELDS } from "../src/filesystem-evidence.ts";
import { createCompletedProcessExecutionWorld } from "../src/linux-process-world.ts";
import { onExit } from "./async.ts";
import { patternProcessHost, processTest, textOutput } from "./linux-process-fixture.ts";
import { idleResources, systemHost } from "./agent-system-fixture.ts";

const cases = [
	{ kind: "query", command: "cat words.txt; wc -l < words.txt", output: "pear\napple\npear\nbanana\n4\n" },
	{ kind: "text", command: "sort words.txt | uniq -c", output: "      1 apple\n      1 banana\n      2 pear\n" },
	{ kind: "build", command: "cc -O1 -Wall -Wextra main.c -o app && ./app", output: "built:42\n" },
	{ kind: "compile", command: "cc -O1 -Wall -Wextra -c main.c -o main.o", output: "(no output)" },
	{ kind: "test", command: "python3 -B check.py", output: "tests passed\n" },
	{ kind: "isolated-test", command: "python3 -I -S -B check.py", output: "tests passed\n" },
	{ kind: "script", command: "node report.mjs", output: "report:6\n" },
];

processTest.for(cases.flatMap(workload => [false, true].map(publicHost => ({ ...workload, publicHost }))))(
	"validates a real Bash workload ($kind, public=$publicHost)", { timeout: 60_000 }, async ({ kind, command, output, publicHost }, { benchmark }) => {
	// Runtime ancestor probes must not depend on unrelated processes creating entries in /tmp.
	const f = await benchmark(`pi-bash-${kind}-`, "git", {}, path.join(os.homedir(), ".cache", "pi-speculative-action", "bash-qualification"));
	const files = {
		"words.txt": "pear\napple\npear\nbanana\n",
		"main.c": '#include <stdio.h>\nint main(void) { puts("built:42"); return 0; }\n',
		"check.py": 'import json\nwith open("data.json") as f: values = json.load(f)\nassert sum(values) == 6\nassert sorted(values) == [1,2,3]\nprint("tests passed")\n',
		"data.json": "[3,1,2]\n",
		"report.mjs": 'import fs from "node:fs"; const values=JSON.parse(fs.readFileSync("data.json", "utf8")); const sum=values.reduce((a,b)=>a+b,0); fs.writeFileSync("report.txt", String(sum)); console.log("report:"+sum);\n',
	};
	for (const [name, contents] of Object.entries(files)) await f.write(name, contents);
	expect(textOutput(await f.tool.execute("seed", { command }))).toBe(output);
	const artifact = kind === "compile" ? "main.o" : kind === "build" ? "app" : kind === "script" ? "report.txt" : undefined;
	const expectedArtifact = artifact && await readFile(path.join(f.workspace, artifact));
	const paths = [f.workspace, ...[...Object.keys(files), ...artifact ? [artifact] : []].map(name => path.join(f.workspace, name))];
	const snapshot = () => Promise.all(paths.map(async target => {
		const stat = await lstat(target, { bigint: true });
		return Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field, String(stat[field])]));
	}));
	const before = await snapshot();
	if (publicHost) {
		const world = createCompletedProcessExecutionWorld({ coordinator: f.coordinator, tools: ["bash"], storeRoot: path.join(f.root, "finished") });
		const h = systemHost(f.workspace, [f.tool], { executionWorlds: [world], resolveInvocation: (_tool, input) => f.invocation(input as Record<string, unknown>) });
		await using _host = onExit(h.dispose);
		await h.start(); await h.preview("bash", { command }); await h.ready();
		expect(await snapshot(), "public producer changed the source").toEqual(before);
		expect(textOutput(await h.execute("bash", { command })), JSON.stringify(h.events)).toBe(output);
		expect(h.fallbacks, JSON.stringify(h.events)).toEqual([]);
		if (artifact) expect(await readFile(path.join(f.workspace, artifact))).toEqual(expectedArtifact);
		expect(textOutput(await h.execute("bash", { command }))).toBe(output);
		expect(h.fallbacks).toEqual(["bash"]);
		await h.finish(); return;
	}
	await f.prepare();
	const branch = await f.fork(command);
	await using _branch = onExit(async () => { await branch.dispose?.(); });
	expect(await snapshot(), "producer changed the source").toEqual(before);
	expect(textOutput(branch.output.result), JSON.stringify(f.backend.metrics())).toBe(output);
	const validation = await branch.validate!();
	expect(validation, JSON.stringify({ validation, metrics: f.backend.metrics() })).toMatchObject({ status: "valid" });
	expect(textOutput((await branch.commit()).result)).toBe(output);
	if (artifact) expect(await readFile(path.join(f.workspace, artifact))).toEqual(expectedArtifact);
});

processTest.for([false, true])("adopts compiled work once and rejects changed source (%s)", { timeout: 60_000 }, async (changed, { benchmark }) => {
	const f = await benchmark("pi-bash-compile-adoption-", "git"), command = "cc -O1 -c unit.c -o unit.o";
	await f.write("unit.c", "int answer(void) { return 42; }\n");
	// Establish an ordinary output oracle and warm system executable atimes before qualification.
	await f.tool.execute("oracle", { command });
	const expected = await readFile(path.join(f.workspace, "unit.o"));
	await f.prepare();
	const h = patternProcessHost(f, "compile", { patternAware: { enabled: false }, prepareObservedOperations: false }, { resources: idleResources(2) });
	await using _host = onExit(() => h.host.dispose());
	await h.start("actor");
	const call = h.call("actor", command);
	await h.host.previewActorCall(call);
	await h.waitFor(() => h.events.some(event => event.type === "candidate" && event.state.status === "succeeded"));
	if (changed) await f.write("unit.c", "int answer(void) { return 43; }\n");
	let executions = 0;
	const execute = () => h.host.execute(call, undefined, async () => { executions++; return f.tool.execute("actor", { command }); });
	expect(textOutput(await execute())).toBe("(no output)");
	expect(executions, JSON.stringify(h.events)).toBe(changed ? 1 : 0);
	const actual = await readFile(path.join(f.workspace, "unit.o"));
	if (changed) expect(actual).not.toEqual(expected); else expect(actual).toEqual(expected);
	// The finished native run includes one-shot inputs; a second call cannot consume it again.
	expect(textOutput(await execute())).toBe("(no output)");
	expect(executions).toBe(changed ? 2 : 1);
	await h.host.finishTurn("actor");
});

processTest.for([{ appeared: false, retained: false }, { appeared: true, retained: false }, { appeared: false, retained: true }])(
	"validates private temporary inputs (appeared=$appeared, retained=$retained)", { timeout: 60_000 }, async ({ appeared, retained }, { benchmark }) => {
	const f = await benchmark("pi-bash-scratch-", "git"), scratch = path.join(f.root, "scratch");
	const command = `printf data > '${scratch}'; stat -c %s '${scratch}'${retained ? "" : `; rm '${scratch}'`}`;
	expect(textOutput(await f.tool.execute("oracle", { command: `${command}; rm -f '${scratch}'` }))).toBe("4\n");
	await f.prepare();
	const branch = await f.fork(command);
	await using _branch = onExit(async () => { await branch.dispose?.(); });
	expect(await lstat(scratch).catch(error => error.code)).toBe("ENOENT");
	if (appeared) await f.write("../scratch", "another owner's input");
	const validation = await branch.validate!();
	expect(validation, JSON.stringify({ validation, metrics: f.backend.metrics() })).toMatchObject({ status: retained ? "indeterminate" : appeared ? "stale" : "valid" });
	if (appeared) expect(await readFile(scratch, "utf8")).toBe("another owner's input");
	else if (!retained) expect(textOutput((await branch.commit()).result)).toBe("4\n");
});

processTest.for(["mv", "ln"])("keeps metadata proof when a temporary object survives under an alias (%s)", { timeout: 60_000 }, async (operation, { benchmark }) => {
	const f = await benchmark("pi-bash-scratch-alias-", "git"), scratch = path.join(f.root, "scratch"), kept = path.join(f.root, "kept");
	const command = `printf data > '${scratch}'; stat -c %i '${scratch}'; ${operation} '${scratch}' '${kept}'; rm -f '${scratch}'`;
	await f.tool.execute("oracle", { command: `${command}; rm '${kept}'` });
	await f.prepare();
	const branch = await f.fork(command);
	await using _branch = onExit(async () => { await branch.dispose?.(); });
	expect(await lstat(kept).catch(error => error.code)).toBe("ENOENT");
	const validation = await branch.validate!();
	expect(validation.status, JSON.stringify({ validation, metrics: f.backend.metrics() })).not.toBe("valid");
});

processTest.for(["warm", "cold", "expired", "explicit"])("preserves native access-time decisions (%s)", { timeout: 60_000 }, async (mode, { benchmark }) => {
	const f = await benchmark("pi-bash-access-", "git", {}, os.homedir());
	await f.compile("access-time", { code: `#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
int main(int argc, char **argv) {
  struct stat before, after; char value;
  if (stat(argv[1], &before)) return 1;
  if (argc > 2) { struct timespec t[2] = {{123, 456}, {0, UTIME_OMIT}}; if (utimensat(AT_FDCWD, argv[1], t, 0)) return 2; }
  int fd = open(argv[1], O_RDONLY | (argc > 2 ? O_NOATIME : 0));
  if (fd < 0 || read(fd, &value, 1) != 1 || close(fd) || stat(argv[1], &after)) return 3;
  printf("%d\\n", before.st_atim.tv_sec != after.st_atim.tv_sec || before.st_atim.tv_nsec != after.st_atim.tv_nsec);
  return 0;
}\n` });
	await f.write("input", "value");
	const target = path.join(f.workspace, "input"), command = `./access-time input${mode === "explicit" ? " write" : ""}`;
	await f.tool.execute("warm-runtime", { command: "./access-time input" });
	if (mode === "cold" || mode === "expired") await utimes(target, mode === "cold" ? 0 : Date.now() / 1000 - 86401, 0);
	const snapshot = async () => {
		const stat = await lstat(target, { bigint: true });
		return Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map(field => [field, String(stat[field])]));
	};
	const before = await snapshot();
	await f.prepare();
	const branch = await f.fork(command);
	await using _branch = onExit(async () => { await branch.dispose?.(); });
	expect(await snapshot()).toEqual(before);
	expect(textOutput(branch.output.result), JSON.stringify(f.backend.metrics())).toBe(mode === "warm" ? "0\n" : "1\n");
	const validation = await branch.validate!();
	if (mode === "warm") expect(validation.status, JSON.stringify(validation)).toBe("valid");
	else expect(validation.status, JSON.stringify(validation)).not.toBe("valid");
	expect(textOutput(branch.output.result)).toBe(textOutput(await f.tool.execute("oracle", { command })));
});

processTest.for(["failure", "timeout", "cancel"])("preserves default Bash failure and cancellation (%s)", { timeout: 30_000 }, async (mode, { benchmark }) => {
	const f = await benchmark("pi-bash-default-failure-", "git", {}, os.homedir());
	const world = createCompletedProcessExecutionWorld({ coordinator: f.coordinator, tools: ["bash"], storeRoot: path.join(f.root, "finished") });
	const h = systemHost(f.workspace, [f.tool], { executionWorlds: [world], resolveInvocation: (_tool, input) => f.invocation(input as Record<string, unknown>) });
	await using _host = onExit(h.dispose);
	await h.start();
	const args = mode === "failure" ? { command: "printf expected; exit 7" } : { command: "sleep 5; printf unexpected > effect", timeout: 0.1 };
	const controller = new AbortController();
	await h.preview("bash", args, controller.signal);
	if (mode === "failure") {
		await h.ready();
		await expect(h.execute("bash", args)).rejects.toThrow("7");
		expect(h.fallbacks, JSON.stringify(h.events)).toEqual([]);
	} else {
		if (mode === "cancel") controller.abort();
		await expect.poll(() => h.events.some(event => event.type === "candidate" && ["failed", "cancelled"].includes(event.state.status)), { timeout: 10_000 }).toBe(true);
		if (mode === "timeout") await expect(h.execute("bash", args)).rejects.toThrow(/timed out/i);
		expect(await lstat(path.join(f.workspace, "effect")).catch(error => error.code)).toBe("ENOENT");
	}
	await h.finish();
});
