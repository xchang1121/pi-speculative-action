import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { afterEach, expect, test } from "vitest";
import { createClosedSearchProfile } from "../src/pi-tool-invocation.ts";
import { captureResourceVersion } from "../src/resource-version.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-captured-find-");
afterEach(directories.dispose);

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
