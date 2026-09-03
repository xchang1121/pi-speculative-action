#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";

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
} else if (!configuration && args[0] === "--exec" && /^[12]{2}$/.test(args[1] ?? "") && args.length >= 4) {
	await run(args[3], args.slice(4), args[2], [Number(args[1][0]), Number(args[1][1])]);
} else if (!configuration && args.length === 2 && args[0] === "--probe-context" && process.cwd() === args[1]) {
	fs.writeSync(1, JSON.stringify(executionContext()));
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
			context: executionContext(),
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
	const unresolved = explicitExecutable ?? invokedPath ?? resolveExecutable(invoked, environment.PATH ?? "");
	const executable = escapeExecutable(unresolved);
	if (!executable) {
		process.stderr.write(`${invoked}: command not found\n`);
		process.exitCode = 127;
		return;
	}
	await run(executable, args, argv0);
}

async function run(executable, commandArgs, argv0, outputRoute) {
	const stdio = outputRoute ? [0, outputRoute[0], outputRoute[1]] : "inherit";
	const child = spawn(executable, commandArgs, { argv0, cwd: process.cwd(), env: environment, stdio });
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

function executionContext() {
	const status = Object.fromEntries(
		fs.readFileSync("/proc/self/status", "utf8")
			.split("\n")
			.map((line) => line.split(/:\s*/, 2))
			.filter(([name]) => ["Cpus_allowed_list", "Mems_allowed_list", "SigBlk", "SigIgn"].includes(name)),
	);
	const aliases = new Map();
	const descriptors = [0, 1, 2].map((fd) => descriptor(fd, aliases));
	const umask = process.umask();
	const semantic = {
		executionDomain: "ptrace-v1",
		rlimits: fs.readFileSync("/proc/self/limits", "utf8").split("\n").slice(1)
			.map((line) => line.trim().split(/\s{2,}/).slice(0, 2)),
		credentials: {
			uid: process.getuid(), euid: process.geteuid(), gid: process.getgid(), egid: process.getegid(),
			groups: process.getgroups().sort((left, right) => left - right),
		},
		systemMetadata: statIdentity("/bin/sh"),
		signals: { blocked: status.SigBlk, ignored: status.SigIgn },
		scheduling: {
			nice: os.getPriority(), cpus: status.Cpus_allowed_list, memoryNodes: status.Mems_allowed_list,
		},
		descriptors: descriptors.map(({ endpoint, ...value }) => ({
			...value, flags: value.flags & ~0o2000000, ...(value.type === "device" ? { endpoint } : {}),
		})),
	};
	return {
		key: JSON.stringify(semantic),
		launchKey: JSON.stringify({
			...semantic,
			credentials: { ...semantic.credentials, groups: "broker-preserved" },
			signals: "broker-normalized",
		}),
		umask,
		descriptorTypes: descriptors.map(({ type }) => type),
		outputEndpoints: [descriptors[1]?.endpoint ?? "", descriptors[2]?.endpoint ?? ""],
	};
}

function statIdentity(target) {
	const value = fs.statSync(target, { bigint: true });
	return Object.fromEntries(["dev", "ino", "mode", "uid", "gid", "rdev"].map((name) => [name, String(value[name])]));
}

function descriptor(fd, aliases) {
	const stat = fs.fstatSync(fd, { bigint: true });
	const endpoint = readDescriptorTarget(fd);
	const identity = `${stat.dev}:${stat.ino}`;
	if (!aliases.has(identity)) aliases.set(identity, aliases.size);
	const flags = /^flags:\s*([0-7]+)/m.exec(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))?.[1];
	if (!flags) throw new Error(`descriptor ${fd} flags unavailable`);
	return {
		fd,
		type: stat.isFile() ? "regular" : stat.isFIFO() ? "pipe" : stat.isSocket() ? "socket" :
			stat.isCharacterDevice() ? (endpoint?.startsWith("/dev/pts/") ? "tty" : "device") : "other",
		flags: Number.parseInt(flags, 8),
		alias: aliases.get(identity),
		...(endpoint ? { endpoint } : {}),
	};
}

function exchange(request) {
	return new Promise((resolve, reject) => {
		const socket = net.createConnection(socketPath);
		let body = "";
		let settled = false;
		const finish = (error, value) => {
			if (settled) return;
			settled = true;
			socket.destroy();
			error ? reject(error) : resolve(value);
		};
		socket.setTimeout(24 * 60 * 60 * 1000, () => finish(new Error("broker timeout")));
		socket.once("error", (error) => finish(error));
		socket.once("connect", () => {
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
