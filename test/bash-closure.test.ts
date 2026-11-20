import { link, lstat, lutimes, mkdir, open, readFile, symlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { expect, vi } from "vitest";
import { WorkspaceSandboxService } from "../src/workspace-sandbox.ts";
import { FILESYSTEM_OBSERVATION_FIELDS } from "../src/filesystem-evidence.ts";
import { createCompletedProcessExecutionWorld } from "../src/linux-process-world.ts";
import { onExit } from "./async.ts";
import {
	patternProcessHost,
	processFixtureConfig,
	processTest,
	textOutput,
} from "./linux-process-fixture.ts";
import { systemHost } from "./agent-system-fixture.ts";

const cases = [
	{
		kind: "query",
		command: "cat words.txt; wc -l < words.txt",
		output: "pear\napple\npear\nbanana\n4\n",
	},
	{
		kind: "text",
		command: "sort words.txt | uniq -c",
		output: "      1 apple\n      1 banana\n      2 pear\n",
	},
	{
		kind: "build",
		command: "cc -O1 -Wall -Wextra main.c -o app && ./app",
		output: "built:42\n",
	},
	{
		kind: "compile",
		command: "cc -O1 -Wall -Wextra -c main.c -o main.o",
		output: "(no output)",
	},
	{ kind: "test", command: "python3 -B check.py", output: "tests passed\n" },
	{
		kind: "isolated-test",
		command: "python3 -I -S -B check.py",
		output: "tests passed\n",
	},
	{ kind: "script", command: "python3 -I -S -B report.py", output: "report:6\n" },
	{ kind: "node", command: "node report.mjs", output: "report:6\n" },
];

processTest.for(
	cases.flatMap((workload) => [false, true].map((publicHost) => ({ ...workload, publicHost }))),
)(
	"validates a real Bash workload ($kind, public=$publicHost)",
	{ timeout: 60_000 },
	async ({ kind, command, output, publicHost }, { benchmark }) => {
		// Runtime ancestor probes must not depend on unrelated processes creating entries in /tmp.
		const f = await benchmark(
			`pi-bash-${kind}-`,
			"in_memory",
			{},
			path.join(os.homedir(), ".cache", "pi-speculative-action", "bash-qualification"),
		);
		const files = {
			"words.txt": "pear\napple\npear\nbanana\n",
			"main.c": '#include <stdio.h>\nint main(void) { puts("built:42"); return 0; }\n',
			"check.py":
				'import json\nwith open("data.json") as f: values = json.load(f)\nassert sum(values) == 6\nassert sorted(values) == [1,2,3]\nprint("tests passed")\n',
			"data.json": "[3,1,2]\n",
			"report.py": [
				"import json, os",
				'with open("data.json") as f: total = sum(json.load(f))',
				'fd = os.open("report.txt", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)',
				"os.write(fd, str(total).encode())",
				"os.close(fd)",
				'print(f"report:{total}")',
			].join("\n"),
			"report.mjs":
				'import fs from "node:fs"; const values=JSON.parse(fs.readFileSync("data.json", "utf8")); const sum=values.reduce((a,b)=>a+b,0); fs.writeFileSync("report.txt", String(sum)); console.log("report:"+sum);\n',
		};
		for (const [name, contents] of Object.entries(files)) await f.write(name, contents);
		expect(textOutput(await f.tool.execute("seed", { command }))).toBe(output);
		const artifacts: Readonly<Record<string, string>> = {
			compile: "main.o",
			build: "app",
			script: "report.txt",
			node: "report.txt",
		};
		const artifact = artifacts[kind];
		const expectedArtifact = artifact && (await readFile(path.join(f.workspace, artifact)));
		const paths = [
			f.workspace,
			...[...Object.keys(files), ...(artifact ? [artifact] : [])].map((name) => path.join(f.workspace, name)),
		];
		const snapshot = () =>
			Promise.all(
				paths.map(async (target) => {
					const stat = await lstat(target, { bigint: true });
					return Object.fromEntries(
						FILESYSTEM_OBSERVATION_FIELDS.map((field) => [field, String(stat[field])]),
					);
				}),
			);
		const before = await snapshot();
		if (publicHost) {
			const world = createCompletedProcessExecutionWorld({
				coordinator: f.coordinator,
				tools: ["bash"],
				storeRoot: path.join(f.root, "finished"),
			});
			const h = systemHost(f.workspace, [f.tool], {
				config: processFixtureConfig,
				executionWorlds: [world],
				resolveInvocation: (_tool, input) => f.invocation(input as Record<string, unknown>),
			});
			await using _host = onExit(h.dispose);
			await h.start();
			await h.preview("bash", { command });
			await h.ready();
			expect(await snapshot(), "public producer changed the source").toEqual(before);
			expect(textOutput(await h.execute("bash", { command })), JSON.stringify(h.events)).toBe(output);
			expect(h.fallbacks, JSON.stringify(h.events)).toEqual([]);
			if (artifact) expect(await readFile(path.join(f.workspace, artifact))).toEqual(expectedArtifact);
			expect(textOutput(await h.execute("bash", { command }))).toBe(output);
			expect(h.fallbacks).toEqual(["bash"]);
			await h.finish();
			return;
		}
		await f.prepare();
		const branch = await f.fork(command);
		await using _branch = onExit(async () => {
			await branch.dispose?.();
		});
		expect(await snapshot(), "producer changed the source").toEqual(before);
		expect(textOutput(branch.output.result), JSON.stringify(f.backend.metrics())).toBe(output);
		const validation = await branch.validate!();
		expect(validation, JSON.stringify({ validation, metrics: f.backend.metrics() })).toMatchObject({
			status: "valid",
		});
		expect(textOutput((await branch.commit()).result)).toBe(output);
		if (artifact) expect(await readFile(path.join(f.workspace, artifact))).toEqual(expectedArtifact);
	},
);

processTest("an oversized checkout seals only directories demanded by the process and invalidates changed inputs", { timeout: 60_000 }, async ({ benchmark }) => {
	const f = await benchmark("pi-large-workspace-", "in_memory", {},
		path.join(os.homedir(), ".cache", "pi-speculative-action", "bash-qualification"));
	for (const dir of ["unrelated", "src", "src/nested"]) await mkdir(path.join(f.workspace, dir));
	const large = await open(path.join(f.workspace, "unrelated", "payload"), "wx");
	try { await large.truncate(513 * 1024 * 1024); } finally { await large.close(); }
	await f.write("src/nested/input", "original\n");
	const alias = path.join(f.workspace, "alias");
	await symlink("src/nested", alias);
	await lutimes(alias, new Date(Date.now() + 60_000), new Date(0));
	const command = "printf '%s\\n' src/nested/*; cat src/nested/input";
	await f.tool.execute("oracle", { command });
	await f.prepare();
	const branch = await f.fork(command);
	await using _release = onExit(async () => { await branch.dispose?.(); });
	expect(textOutput(branch.output.result)).toBe("src/nested/input\noriginal\n");
	expect((await branch.validate!()).status).toBe("valid");
	const linked = await f.fork("printf '%s\\n' alias/*");
	await using _linked = onExit(async () => { await linked.dispose?.(); });
	expect(textOutput(linked.output.result)).toBe("alias/input\n");
	expect((await linked.validate!()).status).toBe("valid");
	await mkdir(path.join(f.workspace, "src", "nested", "unused"));
	await link(path.join(f.workspace, "unrelated", "payload"), path.join(f.workspace, "src", "nested", "unused", "payload"));
	const listing = await f.fork("printf '%s\\n' src/nested/*");
	await using _listing = onExit(async () => { await listing.dispose?.(); });
	expect(textOutput(listing.output.result)).toBe("src/nested/input\nsrc/nested/unused\n");
	expect((await listing.validate!()).status).toBe("valid");
	await f.write("src/nested/input", "modified\n");
	expect((await branch.validate!()).status).toBe("stale");
	await expect(f.fork("printf private > newly-created")).rejects.toThrow();
	await expect(lstat(path.join(f.workspace, "newly-created"))).rejects.toMatchObject({ code: "ENOENT" });
});

processTest("one private retry covers recursive enumeration of a newly observed directory", { timeout: 60_000 }, async ({ benchmark }) => {
	const f = await benchmark("pi-recursive-read-", "in_memory");
	await mkdir(path.join(f.workspace, "tree", "one", "two", "three"), { recursive: true });
	await f.write("tree/one/two/three/file", "hello");
	const command = 'walk(){ local p; for p in "$1"/*; do if [ -d "$p" ]; then walk "$p"; else printf "%s\\n" "$p"; fi; done; }; walk tree';
	await f.tool.execute("oracle", { command });
	await f.prepare();
	const capture = vi.spyOn(WorkspaceSandboxService.prototype, "capture");
	try {
		const branch = await f.fork(command);
		await using _branch = onExit(async () => { await branch.dispose?.(); });
		expect(textOutput(branch.output.result)).toBe("tree/one/two/three/file\n");
		expect((await branch.validate!()).status).toBe("valid");
		expect(capture).toHaveBeenCalledTimes(2);
	} finally {
		capture.mockRestore();
	}
});

processTest.for([false, true])(
	"adopts compiled work once and rejects changed source (%s)",
	{ timeout: 60_000 },
	async (changed, { benchmark }) => {
		const f = await benchmark("pi-bash-compile-adoption-", "in_memory");
		const command = "cc -O1 -c unit.c -o unit.o";
		await f.write("unit.c", "int answer(void) { return 42; }\n");
		// Establish an ordinary output oracle and warm system executable atimes before qualification.
		await f.tool.execute("oracle", { command });
		const expected = await readFile(path.join(f.workspace, "unit.o"));
		await f.prepare();
		const h = patternProcessHost(
			f,
			"compile",
			{ patternAware: { enabled: false }, prepareObservedOperations: false },
			{},
		);
		await using _host = onExit(() => h.host.dispose());
		await h.start("actor");
		const call = h.call("actor", command);
		await h.host.previewActorCall(call);
		await h.waitFor(() =>
			h.events.some((event) => event.type === "candidate" && event.state.status === "succeeded"),
		);
		if (changed) await f.write("unit.c", "int answer(void) { return 43; }\n");
		let executions = 0;
		const execute = () =>
			h.host.execute(call, undefined, async () => {
				executions++;
				return f.tool.execute("actor", { command });
			});
		expect(textOutput(await execute())).toBe("(no output)");
		expect(executions, JSON.stringify(h.events)).toBe(changed ? 1 : 0);
		const actual = await readFile(path.join(f.workspace, "unit.o"));
		if (changed) expect(actual).not.toEqual(expected);
		else expect(actual).toEqual(expected);
		// The finished native run includes one-shot inputs; a second call cannot consume it again.
		expect(textOutput(await execute())).toBe("(no output)");
		expect(executions).toBe(changed ? 2 : 1);
		await h.host.finishTurn("actor");
	},
);

processTest.for([false, true])(
	"revalidates prepared inputs at whole-branch commit (%s)",
	{ timeout: 30_000 },
	async (changed, { benchmark }) => {
		const f = await benchmark("pi-commit-admission-", "in_memory", {}, os.homedir());
		const command = "./step";
		await f.write("input.txt", "before\n");
		await f.write("marker", "");
		await f.compile("step", {
			code: '#include <fcntl.h>\n#include <unistd.h>\nint main(void){char b[64];int f=open("input.txt",O_RDONLY);if(f<0)return 1;int n=read(f,b,sizeof(b));close(f);if(n<0)return 2;f=open("marker",O_WRONLY|O_APPEND);if(f<0)return 3;int ok=write(f,"once",4)==4;close(f);return !ok||write(1,b,n)!=n;}\n',
		});
		await f.prepare();
		const branch = await f.fork(command);
		await using _branch = onExit(async () => {
			await branch.dispose();
		});
		const validation = await branch.validate!();
		expect(validation, JSON.stringify({ validation, metrics: f.backend.metrics() })).toMatchObject({
			status: "valid",
		});
		if (changed) {
			await f.write("input.txt", "after\n");
			await expect(branch.commit()).rejects.toMatchObject({
				disposition: "recoverable",
			});
			expect(await f.read("marker")).toBe("");
			expect(textOutput(await f.tool.execute("fallback", { command }))).toBe("after\n");
		} else {
			expect(textOutput((await branch.commit()).result)).toBe("before\n");
			await branch.commit();
		}
		expect(await f.read("marker")).toBe("once");
	},
);

processTest.for(["failure", "timeout", "cancel"])(
	"preserves default Bash failure and cancellation (%s)",
	{ timeout: 30_000 },
	async (mode, { benchmark }) => {
		const f = await benchmark("pi-bash-default-failure-", "in_memory", {}, os.homedir());
		const world = createCompletedProcessExecutionWorld({
			coordinator: f.coordinator,
			tools: ["bash"],
			storeRoot: path.join(f.root, "finished"),
		});
		const h = systemHost(f.workspace, [f.tool], {
			config: processFixtureConfig,
			executionWorlds: [world],
			resolveInvocation: (_tool, input) => f.invocation(input as Record<string, unknown>),
		});
		await using _host = onExit(h.dispose);
		await h.start();
		const args =
			mode === "failure"
				? { command: "printf expected; exit 7" }
				: { command: "sleep 5; printf unexpected > effect", timeout: 0.1 };
		const controller = new AbortController();
		const run = await h.preview("bash", args, controller.signal);
		if (mode === "failure") {
			await h.ready();
			await expect(h.execute("bash", args)).rejects.toThrow("7");
			expect(h.fallbacks, JSON.stringify(h.events)).toEqual([]);
		} else {
			if (mode === "cancel") {
				controller.abort();
				expect(await run!.completion).toMatchObject({status:"cancelled"});
			} else await expect
				.poll(
					() =>
						h.events.some(
							(event) => event.type === "candidate" && ["failed", "cancelled"].includes(event.state.status),
						),
					{ timeout: 10_000 },
				)
				.toBe(true);
			if (mode === "timeout") await expect(h.execute("bash", args)).rejects.toThrow(/timed out/i);
			expect(await lstat(path.join(f.workspace, "effect")).catch((error) => error.code)).toBe("ENOENT");
		}
		await h.finish();
	},
);
