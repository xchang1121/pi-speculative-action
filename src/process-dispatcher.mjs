#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { captureProcessContext } from "./process-context.mjs";

const environment = { ...process.env };
// Node ignores SIGXFSZ at startup; an exec outlet must preserve the shell's default disposition.
const resetXfsz = () => {};
process.on("SIGXFSZ", resetXfsz);
process.off("SIGXFSZ", resetXfsz);

if (process.argv.length === 5 && process.argv[2] === "--probe-context" && process.cwd() === process.argv[3]) {
	await run(process.argv[4], [], process.argv[4]);
	if (process.exitCode !== 42) throw new Error("sandbox script read position is not preserved");
	process.exitCode = 0;
	fs.writeSync(1, JSON.stringify(await captureProcessContext("self", ["0", "1", "2"])));
} else {
	let invoked = "process dispatcher";
	try {
		if (process.argv[2] !== "--native-dispatch" || process.argv.length < 6) throw new Error("invalid native dispatch invocation");
		const configuration = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
		if (!validConfiguration(configuration)) throw new Error("invalid native dispatch configuration");
		const invokedPath = process.argv[4], argv0 = process.argv[5], args = process.argv.slice(6);
		invoked = path.basename(invokedPath);
		if (!invoked) throw new Error("missing native dispatch target");
		const response = await exchange({
			token: configuration.token,
			name: invoked,
			invokedPath,
			argv0,
			args,
			cwd: process.cwd(),
			environment,
			context: await captureProcessContext("self", ["0", "1", "2"]),
		}, configuration.socketPath);
		if (!response || !["bypass", "hit", "executed", "suspended"].includes(response.kind)) throw new Error("invalid dispatch response");
		if (response.kind === "bypass") {
			if (typeof response.executable !== "string") throw new Error("missing bypass executable");
			let executable = response.executable;
			if (path.isAbsolute(executable)) {
				const directory = configuration.directories.find(({ target, view }) =>
					[target, view].some(candidate => path.resolve(path.dirname(executable)) === path.resolve(candidate)));
				if (directory) executable = path.join(directory.shadow, path.basename(executable));
			}
			await run(executable, args, argv0);
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

async function run(executable, commandArgs, argv0) {
	const child = spawn(executable, commandArgs, { argv0, cwd: process.cwd(), env: environment, stdio: "inherit" });
	const outcome = await new Promise((resolve, reject) => {
		child.once("error", reject);
		child.once("exit", (code, signal) => resolve({ code, signal }));
	});
	if (outcome.signal) process.kill(process.pid, outcome.signal);
	else process.exitCode = outcome.code ?? 125;
}

function validConfiguration(value) {
	return Boolean(
		value &&
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

async function exchange(request, socketPath) {
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
