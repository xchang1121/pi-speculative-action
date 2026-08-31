#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";

const configuration = globalThis.__PI_SPEC_PROCESS_DISPATCHER__;
const socketPath = configuration?.socketPath;
const token = configuration?.token;
const invokedPath = process.argv[1] ?? "";
const invoked = path.basename(invokedPath);
const args = process.argv.slice(2);
const stdinTarget = readDescriptorTarget(0);
const buffered = !process.stdin.isTTY && !process.stdout.isTTY && !process.stderr.isTTY;
const stdinClosed = stdinTarget === "/dev/null";

const environment = { ...process.env };

if (!validConfiguration(configuration) || !invoked || !buffered || !stdinClosed) {
	await fallback();
} else {
	try {
		const response = await exchange({
			version: 1,
			token,
			name: invoked,
			invokedPath,
			args,
			cwd: process.cwd(),
			environment,
			umask: process.umask(),
			stdin: "closed",
		});
		if (!response || response.version !== 1 || response.kind === "bypass") {
			await fallback(response?.executable);
		} else {
			for (const event of response.output ?? []) {
				if ((event.fd !== 1 && event.fd !== 2) || typeof event.data !== "string") throw new Error("bad output event");
				fs.writeSync(event.fd, Buffer.from(event.data, "base64"));
			}
			if (response.exit?.kind === "signal") {
				process.kill(process.pid, response.exit.signal);
			} else {
				process.exitCode = Number.isSafeInteger(response.exit?.code) ? response.exit.code : 125;
			}
		}
	} catch (error) {
		if (error?.outcomeUncertain) {
			process.stderr.write(`${invoked}: broker outcome unavailable; refusing unsafe re-execution\n`);
			process.exitCode = 125;
		} else {
			await fallback();
		}
	}
}

async function fallback(explicitExecutable) {
	const unresolved = explicitExecutable ?? invokedPath ?? resolveExecutable(invoked, environment.PATH ?? "");
	const executable = escapeExecutable(unresolved);
	if (!executable) {
		process.stderr.write(`${invoked}: command not found\n`);
		process.exitCode = 127;
		return;
	}
	const child = spawn(executable, args, { cwd: process.cwd(), env: environment, stdio: "inherit" });
	const outcome = await new Promise((resolve, reject) => {
		child.once("error", reject);
		child.once("exit", (code, signal) => resolve({ code, signal }));
	});
	if (outcome.signal) process.kill(process.pid, outcome.signal);
	else process.exitCode = outcome.code ?? 125;
}

function escapeExecutable(executable) {
	if (!executable || !path.isAbsolute(executable)) return executable;
	for (const directory of configuration?.directories ?? []) {
		if (path.resolve(path.dirname(executable)) === path.resolve(directory.target)) {
			return path.join(directory.shadow, path.basename(executable));
		}
	}
	return executable;
}

function validConfiguration(value) {
	return Boolean(
		value &&
			value.version === 1 &&
			typeof value.socketPath === "string" &&
			typeof value.token === "string" &&
			Array.isArray(value.directories) &&
			value.directories.every(
				(directory) =>
					directory && typeof directory.target === "string" && typeof directory.shadow === "string",
			),
	);
}

function exchange(request) {
	return new Promise((resolve, reject) => {
		const socket = net.createConnection(socketPath);
		let body = "";
		let settled = false;
		let outcomeUncertain = false;
		const finish = (error, value) => {
			if (settled) return;
			settled = true;
			socket.destroy();
			if (error && outcomeUncertain && typeof error === "object") error.outcomeUncertain = true;
			error ? reject(error) : resolve(value);
		};
		socket.setTimeout(24 * 60 * 60 * 1000, () => finish(new Error("broker timeout")));
		socket.once("error", (error) => finish(error));
		socket.once("connect", () => {
			outcomeUncertain = true;
			socket.end(`${JSON.stringify(request)}\n`);
		});
		socket.on("data", (chunk) => {
			body += chunk.toString("utf8");
		});
		socket.once("end", () => {
			try {
				finish(undefined, JSON.parse(body.trim()));
			} catch (error) {
				finish(error);
			}
		});
	});
}

function resolveExecutable(name, pathValue) {
	if (!name || name.includes("/")) return undefined;
	for (const directory of pathValue.split(":")) {
		if (!directory) continue;
		const candidate = path.join(directory, name);
		try {
			fs.accessSync(candidate, fs.constants.X_OK);
			if (fs.statSync(candidate).isFile()) return candidate;
		} catch {
			// Continue PATH lookup.
		}
	}
	return undefined;
}

function readDescriptorTarget(fd) {
	try {
		return fs.readlinkSync(`/proc/self/fd/${fd}`);
	} catch {
		return undefined;
	}
}
