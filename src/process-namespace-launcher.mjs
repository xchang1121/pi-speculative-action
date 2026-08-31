#!/usr/bin/env node

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";

const planPath = process.argv[2];
const command = process.argv.slice(3);
if (!planPath || command.length === 0) throw new Error("invalid process namespace launch");
const plan = JSON.parse(await readFile(planPath, "utf8"));
if (!validPlan(plan)) throw new Error("invalid process namespace mount plan");

await run(plan.mountBinary, ["--bind", plan.workspace.source, plan.workspace.target]);
for (const directory of plan.directories) {
	await run(plan.mountBinary, ["--bind", directory.source, directory.shadow]);
}
const mountBinary = escapeExecutable(plan.mountBinary, plan.directories);
for (const directory of plan.directories) {
	await run(mountBinary, ["--bind", directory.view, directory.target]);
}

const child = spawn(command[0], command.slice(1), {
	cwd: process.cwd(),
	env: process.env,
	stdio: "inherit",
});
const handlers = new Map(
	["SIGINT", "SIGTERM", "SIGHUP"].map((signal) => [signal, () => child.kill(signal)]),
);
for (const [signal, handler] of handlers) process.on(signal, handler);
const outcome = await new Promise((resolve, reject) => {
	child.once("error", reject);
	child.once("exit", (code, signal) => resolve({ code, signal }));
});
for (const [signal, handler] of handlers) process.off(signal, handler);
if (outcome.signal) process.kill(process.pid, outcome.signal);
else process.exitCode = outcome.code ?? 125;

function run(executable, args) {
	return new Promise((resolve, reject) => {
		const child = spawn(executable, args, { stdio: ["ignore", "ignore", "pipe"] });
		let errorText = "";
		child.stderr.on("data", (chunk) => {
			errorText += chunk.toString("utf8");
		});
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === 0) resolve();
			else reject(new Error(`mount failed (${signal ?? code ?? "unknown"}): ${errorText.trim()}`));
		});
	});
}

function escapeExecutable(executable, directories) {
	for (const directory of directories) {
		if (path.resolve(path.dirname(executable)) === path.resolve(directory.target)) {
			return path.join(directory.shadow, path.basename(executable));
		}
	}
	return executable;
}

function validPlan(value) {
	const validPath = (candidate) => typeof candidate === "string" && path.isAbsolute(candidate);
	const validDirectory = (directory) =>
		directory &&
		validPath(directory.source) &&
		validPath(directory.target) &&
		validPath(directory.shadow) &&
		validPath(directory.view);
	return Boolean(
		value &&
			value.version === 1 &&
			validPath(value.mountBinary) &&
			value.workspace &&
			validPath(value.workspace.source) &&
			validPath(value.workspace.target) &&
			Array.isArray(value.directories) &&
			value.directories.every(validDirectory),
	);
}
