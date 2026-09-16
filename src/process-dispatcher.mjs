#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { captureProcessContext } from "./process-context.mjs";

const nativeRequested = process.argv[2] === "--native-dispatch";
const native = nativeInvocation();
const configuration = globalThis.__PI_SPEC_PROCESS_DISPATCHER__ ?? native?.configuration;
const socketPath = configuration?.socketPath;
const token = configuration?.token;
const invokedPath = native?.invokedPath ?? process.argv[1] ?? "";
const invoked = path.basename(invokedPath);
const argv0 = native?.argv0 ?? invoked;
const args = native?.args ?? process.argv.slice(2);
const environment = { ...process.env };
// Node ignores SIGXFSZ at startup; an exec outlet must preserve the shell's default disposition.
const resetXfsz = () => {};
process.on("SIGXFSZ", resetXfsz);
process.off("SIGXFSZ", resetXfsz);

if (nativeRequested && (!native || !validConfiguration(configuration))) {
	process.stderr.write("invalid native dispatch configuration\n");
	process.exitCode = 125;
} else if (!configuration && args.length === 3 && args[0] === "--probe-context" && process.cwd() === args[1]) {
	await run(args[2], [], args[2]);
	if (process.exitCode !== 42) throw new Error("sandbox script read position is not preserved");
	process.exitCode = 0;
	fs.writeSync(1, JSON.stringify(await captureProcessContext("self", ["0", "1", "2"])));
} else if (!validConfiguration(configuration) || !invoked) {
	await fallback();
} else {
	try {
		const response = await exchange({
			version: 2,
			token,
			name: invoked,
			invokedPath,
			argv0,
			args,
			cwd: process.cwd(),
			environment,
			context: await captureProcessContext("self", ["0", "1", "2"]),
		});
		if (!response || response.version !== 2 || response.kind === "bypass") {
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
	} catch {
		process.stderr.write(`${invoked}: broker unavailable; refusing unobserved execution\n`);
		process.exitCode = 125;
	}
}

async function fallback(explicitExecutable) {
	const unresolved = explicitExecutable ?? invokedPath;
	const executable = escapeExecutable(unresolved);
	if (!executable) {
		process.stderr.write(`${invoked}: command not found\n`);
		process.exitCode = 127;
		return;
	}
	await run(executable, args, argv0);
}

async function run(executable, commandArgs, argv0) {
	const child = spawn(executable, commandArgs, { argv0, cwd: process.cwd(), env: environment, stdio: "inherit" });
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
		if ([directory.target, directory.view].some((candidate) => path.resolve(path.dirname(executable)) === path.resolve(candidate))) {
			return path.join(directory.shadow, path.basename(executable));
		}
	}
	return executable;
}

function nativeInvocation() {
	if (process.argv[2] !== "--native-dispatch" || process.argv.length < 6) return undefined;
	try {
		return {
			configuration: JSON.parse(fs.readFileSync(process.argv[3], "utf8")),
			invokedPath: process.argv[4],
			argv0: process.argv[5],
			args: process.argv.slice(6),
		};
	} catch {
		return undefined;
	}
}

function validConfiguration(value) {
	return Boolean(
		value &&
			value.version === 2 &&
			typeof value.socketPath === "string" &&
			typeof value.token === "string" &&
			Array.isArray(value.directories) &&
			value.directories.every(
				(directory) =>
					directory && typeof directory.target === "string" && typeof directory.view === "string" &&
					typeof directory.shadow === "string",
			),
	);
}

async function exchange(request) {
	const socket = net.createConnection(socketPath).setEncoding("utf8");
	socket.setTimeout(24 * 60 * 60 * 1000, () => socket.destroy(new Error("broker timeout")));
	try {
		await once(socket, "connect");
		socket.end(`${JSON.stringify(request)}\n`);
		let body = "";
		for await (const chunk of socket) body += chunk;
		return JSON.parse(body.trim());
	} finally {
		socket.destroy();
	}
}
