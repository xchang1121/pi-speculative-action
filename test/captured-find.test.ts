import { mkdir, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test } from "vitest";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { evaluateResourceQuery } from "../src/agent-execution-world.ts";
import { resolveSpeculativeConfig, withSpeculativeConfig } from "../src/config-schema.ts";
import { createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { captureResourceVersion, type ResourceVersionToken } from "../src/resource-version.ts";
import { TaskTimeline, TimelineInterval } from "../src/task-timing.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-captured-find-");
afterEach(directories.dispose);

test("different grep expressions consume the same proven selection and recheck names and ignore files", async () => {
	const cwd = await directories.create();
	await mkdir(path.join(cwd, ".git"));
	await writeFile(path.join(cwd, ".gitignore"), "hidden.txt\n");
	await writeFile(path.join(cwd, "a.txt"), "first\nsecond\n");
	await writeFile(path.join(cwd, "hidden.txt"), "second\n");
	const profile = await createClosedSearchProfile(cwd);
	const invocation = profile.invocations.get("grep")!;
	const sources: object[] = [];
	const versions: ResourceVersionToken[] = [];
	const search = async (pattern: string, limit?: number) => {
		const signal = AbortSignal.timeout(30000);
		const request = { callID: pattern, args: { pattern, limit }, signal };
		let walks = 0;
		const issued = performance.now();
		const run = await TimelineInterval.measure(() => evaluateResourceQuery(
			undefined, { signal, inputs: () => sources, onInputSource: (source) => sources.push(source) },
			path.parse(cwd).root, (view) => invocation.filesystem!({
				...view,
				prepare: (binding, key, build, use, hint, signal) => view.prepare(binding, key, (inputs, signal) => {
					if (hint === cwd) walks++;
					return build(inputs, signal);
				}, use, hint, signal),
			}, request), PI_ACTION_SEMANTICS, 16 * 1024 * 1024, true, "output",
		));
		versions.push(...run.output!.versions);
		expect(run.output!.output).toEqual(await invocation.authoritative!(request));
		return { walks, result: run.output!.output.result.content, timing: new TaskTimeline(0).recordTool(run.computation, issued) };
	};
	try {
		expect((await search("first")).walks).toBe(1);
		const next = await search("second");
		expect(next.walks).toBe(0);
		expect(next.timing.hiddenComputeMs).toBeGreaterThan(0);
		expect(next.result[0]).toMatchObject({ text: "a.txt:2: second" });
		await writeFile(path.join(cwd, "b.txt"), "second\n");
		const added = await search("second");
		expect(added.walks).toBe(1);
		expect(added.result[0]).toMatchObject({ text: expect.stringContaining("b.txt:1: second") });
		await writeFile(path.join(cwd, ".gitignore"), "a.txt\n");
		const changed = await search("second");
		expect(changed.walks).toBe(1);
		expect(changed.result[0]).toMatchObject({ text: expect.stringContaining("hidden.txt:1: second") });
		await withSpeculativeConfig(resolveSpeculativeConfig({ search: { maxInputBytes: 18 } }), async () => {
			expect((await search("^sec", 1)).walks).toBe(1);
			await expect(search("second", 2)).rejects.toMatchObject({ code: "ERR_RESOURCE_BUDGET" });
		});
		await unlink(path.join(cwd, "hidden.txt"));
		const removed = await search("second");
		expect(removed.walks).toBe(1);
		expect(removed.result[0]).toMatchObject({ text: "b.txt:1: second" });
	} finally {
		await Promise.all(versions.map((v) => v.release()));
		await profile.dispose();
	}
});

test("large live searches skip evidence collection and preserve the native Actor result", async () => {
	const cwd = await directories.create();
	await mkdir(path.join(cwd, ".git"));
	await mkdir(path.join(cwd, "ignored"));
	await writeFile(path.join(cwd, ".gitignore"), "ignored/\n");
	for (const name of ["a", "b", ...Array.from({ length: 8 }, (_, i) => `ignored/${i}`)])
		await writeFile(path.join(cwd, name), "match\n");
	await withSpeculativeConfig(resolveSpeculativeConfig({ search: { maxDirectoryEntries: 4 } }), async () => {
		const profile = await createClosedSearchProfile(cwd);
		try {
			const grep = profile.invocations.get("grep")!;
			const request = { callID: "large", args: { pattern: "match" }, signal: AbortSignal.timeout(10000) };
			await grep.checkCaptureBudget!(request);
			await writeFile(path.join(cwd, "c"), "match\n");
			await expect(grep.checkCaptureBudget!(request)).rejects.toMatchObject({ code: "ERR_RESOURCE_BUDGET" });
			const capture = grep.captureInputs!(undefined, 1024 * 1024, request.callID);
			try {
				const output = await grep.authoritative!(request);
				expect(output.result.content).toEqual([{ type: "text", text: "a:1: match\nb:1: match\nc:1: match" }]);
				await expect(capture.seal(output)).rejects.toThrow("resource_inputs_empty");
			} finally {
				await capture.dispose();
			}
		} finally {
			await profile.dispose();
		}
	});
});

test("a retained directory limit is rechecked when the observed directory shrinks", async () => {
	const cwd = await directories.create();
	for (const name of ["a", "b"]) await writeFile(path.join(cwd, name), "needle\n");
	await withSpeculativeConfig(resolveSpeculativeConfig({ search: { maxDirectoryEntries: 2 } }), async () => {
		const profile = await createClosedSearchProfile(cwd);
		const grep = profile.invocations.get("grep")!;
		const owners: object[] = [];
		const versions: ResourceVersionToken[] = [];
		let builds = 0;
		const search = (pattern: string) => {
			const signal = AbortSignal.timeout(10000);
			return evaluateResourceQuery(undefined, {
				signal, inputs: () => owners, onInputSource: (owner) => owners.push(owner),
			}, path.parse(cwd).root, (view) => grep.filesystem!({
				...view, prepare: (binding, key, build, use, hint, signal) => view.prepare(binding, key, (inputs, signal) => {
					if (hint === cwd) builds++;
					return build(inputs, signal);
				}, use, hint, signal),
			}, { args: { pattern }, callID: pattern, signal }), PI_ACTION_SEMANTICS, 8 * 1024 * 1024, true,
			"output", (owned) => { versions.push(...owned); return true; });
		};
		try {
			await expect(search("needle")).rejects.toMatchObject({ code: "ERR_RESOURCE_BUDGET" });
			await expect(search("different")).rejects.toMatchObject({ code: "ERR_RESOURCE_BUDGET" });
			expect(builds).toBe(1);
			await unlink(path.join(cwd, "b"));
			expect((await search("needle"))?.output.result.content).toEqual([{ type: "text", text: "a:1: needle" }]);
			expect(builds).toBe(2);
		} finally {
			await Promise.all(versions.map((version) => version.release()));
			await profile.dispose();
		}
	});
});

test("captured find preserves relative glob spellings and terminates outside the working directory", async () => {
	const root = await directories.create();
	const cwd = path.join(root, "project");
	const inputs = path.join(root, "inputs");
	await mkdir(cwd);
	await mkdir(path.join(inputs, "nested"), { recursive: true });
	for (const file of ["visible.txt", "ignored.txt", "nested/child.txt"])
		await writeFile(path.join(inputs, file), "input");
	await writeFile(path.join(inputs, ".gitignore"), "ignored.txt\n");
	const profile = await createClosedSearchProfile(cwd);
	try {
		for (const [pattern, expected] of [
			["./*.txt", "visible.txt"],
			["./nested/*.txt", "nested/child.txt"],
			["**/*.txt", "nested/child.txt\nvisible.txt"],
		]) {
			const output = await profile.invocations.get("find")!.authoritative!({
				callID: "outside",
				args: { path: "../inputs", pattern },
				signal: AbortSignal.timeout(5000),
			});
			expect(output.result.content).toEqual([{ type: "text", text: expected }]);
		}
	} finally {
		await profile.dispose();
	}
});

test("grep reads each captured file once for all its context lines", async () => {
	const cwd = await directories.create();
	for (const name of ["a", "b"]) await writeFile(path.join(cwd, name), "hit\nmid\nhit");
	const profile = await createClosedSearchProfile(cwd);
	const capture = await captureResourceVersion(undefined, path.parse(cwd).root, undefined, 8 * 1024 * 1024);
	try {
		const view = capture.view!;
		const reads: string[] = [];
		const result = await profile.invocations.get("grep")!.filesystem!(
			{
				...view,
				readFile: (target, limit) => {
					if (path.dirname(target) === cwd) reads.push(path.basename(target));
					return view.readFile(target, limit);
				},
			},
			{ callID: "context", args: { pattern: "hit", context: 1 }, signal: AbortSignal.timeout(10000) },
		);
		expect(result.result.content).toEqual([
			{
				type: "text",
				text: ["a", "b"]
					.flatMap((file) => [`${file}:1: hit`, `${file}-2- mid`, `${file}-2- mid`, `${file}:3: hit`])
					.join("\n"),
			},
		]);
		expect(reads).toEqual(["a", "b"]);
	} finally {
		await capture.release();
		await profile.dispose();
	}
});

test("a limited grep consumes only the matching prefix and agrees with native ordered search", async () => {
	const cwd = await directories.create();
	await mkdir(path.join(cwd, ".git"));
	await writeFile(path.join(cwd, ".gitignore"), "ignored.txt\n");
	await writeFile(path.join(cwd, "00.txt"), "no match\n");
	for (const file of ["aa.txt", "zz.txt", "ignored.txt", ".git/internal.txt"]) await writeFile(path.join(cwd, file), "hit\n");
	const profile = await createClosedSearchProfile(cwd);
	const input = await captureResourceVersion(undefined, path.parse(cwd).root, undefined, 8 * 1024 * 1024);
	try {
		const contents: string[] = [];
		const view = input.view!;
		const invocation = profile.invocations.get("grep")!;
		const request = { callID: "limited", args: { pattern: "hit", glob: "*.txt", limit: 1 }, signal: AbortSignal.timeout(5000) };
		const result = await invocation.filesystem!({ ...view, prepare: undefined, readFile: async (file, limit) => {
			contents.push(path.basename(file));
			return view.readFile(file, limit);
		} }, request);
		expect(contents).toContain("aa.txt");
		expect(contents).toContain("00.txt");
		expect(contents).not.toContain("zz.txt");
		expect(result.result.content[0]).toMatchObject({ text: expect.stringContaining("aa.txt:1: hit") });
		expect(await invocation.authoritative!(request)).toEqual(result);
	} finally {
		await input.release();
		await profile.dispose();
	}
});
