// @ts-check

import { closeSync, openSync, readFileSync } from "node:fs";
import { readFile, readlink, stat } from "node:fs/promises";
import { speculativeConfig } from "./config-schema.ts";
/** @typedef {import("./provenance-certificate.js").InheritedFileDescriptor["type"]} DescriptorType */
/** @typedef {{
 * readonly key: string, readonly launchKey: string, readonly umask: number,
 * readonly descriptorTypes: readonly [DescriptorType, DescriptorType, DescriptorType],
 * readonly outputEndpoints: readonly [string, string]
 * }} ProcessExecutionContext */
/** @typedef {{ readonly outputRoute: readonly [0 | 1 | 2, 0 | 1 | 2], readonly closeStdin?: boolean,
 * readonly outputPipes?: readonly [boolean, boolean], readonly outputFlags?: readonly [number, number], readonly stackLimit?: string }} ProcessLaunchOptions */
/** @typedef {Record<"dev" | "ino" | "mode" | "uid" | "gid" | "rdev", string>} RawStat */
/**
 * Read the kernel context while the Actor image is stopped or its dispatcher waits for the owner.
 * The caller owns the inherited table: ptrace observes it after exec. Endpoint aliases below describe
 * buffered stdio routing, not general open-file-description identity.
 * @param {number} pid
 * @param {readonly string[]} inheritedDescriptors
 * @returns {Promise<ProcessExecutionContext>}
 */
export async function captureProcessContext(pid, inheritedDescriptors) {
	if (
		inheritedDescriptors.some((name) => !/^\d+$/.test(name)) ||
		!inheritedDescriptors.includes("1") ||
		!inheritedDescriptors.includes("2") ||
		inheritedDescriptors.some((name) => Number(name) > 2)
	) {
		throw new Error("held process has unmodeled inherited descriptors");
	}
	const root = `/proc/${pid}`;
	/** @param {import("node:fs").BigIntStats} metadata @returns {RawStat} */
	const fields = (metadata) =>
		/** @type {RawStat} */ (
			Object.fromEntries(
				["dev", "ino", "mode", "uid", "gid", "rdev"].map((name) => [
					name,
					String(metadata[/** @type {keyof typeof metadata} */ (name)]),
				]),
			)
		);
	const [status, limits, processStat, shell, reported] = await Promise.all([
		readFile(`${root}/status`, "utf8"),
		readFile(`${root}/limits`, "utf8"),
		readFile(`${root}/stat`, "utf8"),
		stat("/bin/sh", { bigint: true }),
		Promise.all(
			[...new Set(inheritedDescriptors.map(Number))]
				.sort((a, b) => a - b)
				.map(async (fd) => {
					const [metadata, endpoint, fdinfo] = await Promise.all([
						stat(`${root}/fd/${fd}`, { bigint: true }),
						readlink(`${root}/fd/${fd}`),
						readFile(`${root}/fdinfo/${fd}`, "utf8"),
					]);
					return { fd, stat: fields(metadata), endpoint, fdinfo };
				}),
		),
	]);
	const descriptors = [...(reported[0]?.fd === 0 ? [] : [undefined]), ...reported].map((item) => {
		if (!item)
			return {
				fd: 0,
				endpoint: undefined,
				type: /** @type {DescriptorType} */ ("closed"),
				identity: "closed:0",
				flags: 0,
			};
		const { fd, stat: metadata, endpoint, fdinfo: info } = item;
		const format = BigInt(metadata.mode) & 61440n;
		const flags = /^flags:\s*([0-7]+)/m.exec(info)?.[1];
		if (!flags) throw new Error(`held descriptor ${fd} flags unavailable`);
		/** @type {DescriptorType} */
		let type;
		if (format === 4096n) {
			type = "pipe";
		} else {
			if (format === 49152n) {
				type = "socket";
			} else {
				if (format === 8192n) {
					type = "device";
				} else {
					type = "other";
				}
			}
		}
		return {
			fd,
			endpoint,
			type,
			// Unproven null devices share one identity: a sandbox may serve its own /dev/null inode.
			identity:
				type === "device" && endpoint === "/dev/null" ? "/dev/null" : `${metadata.dev}:${metadata.ino}`,
			flags: Number.parseInt(flags, 8) & ~0o2000000,
		};
	});
	const [, output, error] = descriptors;
	if (!output || output.fd !== 1 || !error || error.fd !== 2)
		throw new Error("reported process lacks its output streams");
	/** @param {string} name */
	const field = (name) => {
		const value = new RegExp(`^${name}:\\s*(.*)$`, "m").exec(status)?.[1];
		if (value === undefined) throw new Error(`held process status lacks ${name}`);
		return value.trim();
	};
	const uid = numbers(field("Uid"));
	const gid = numbers(field("Gid"));
	const groups = numbers(field("Groups"));
	if (uid.length !== 4 || gid.length !== 4) throw new Error("held process credentials are incomplete");
	if (!groups.includes(gid[1])) groups.push(gid[1]);
	groups.sort((left, right) => left - right);
	const semantic = {
		executionDomain: "ptrace",
		// Core's hard cap is confinement; observing it is guarded by the trace.
		rlimits: limits
			.trim()
			.split("\n")
			.slice(1)
			.map((line) =>
				line
					.trim()
					.split(/\s{2,}/)
					.slice(0, line.startsWith("Max core file size") ? 2 : 3),
			),
		credentials: {
			uid: uid[0],
			euid: uid[1],
			gid: gid[0],
			egid: gid[1],
			groups,
		},
		systemMetadata: fields(shell),
		signals: { blocked: field("SigBlk"), ignored: field("SigIgn") },
		scheduling: {
			nice: Number(
				processStat
					.slice(processStat.lastIndexOf(") ") + 2)
					.trim()
					.split(/\s+/)[16],
			),
			cpus: field("Cpus_allowed_list"),
			memoryNodes: field("Mems_allowed_list"),
		},
		descriptors: descriptors.map(({ endpoint, identity, ...descriptor }) => {
			// routedProcessIdentity reproduces this key order, including its /dev/null discard outlet.
			return {
				fd: descriptor.fd,
				type: descriptor.type,
				flags: descriptor.flags,
				alias: identity,
				...(descriptor.type === "device" ? { endpoint } : {}),
			};
		}),
	};
	return {
		...contextKeys(semantic),
		umask: Number.parseInt(field("Umask"), 8),
		outputEndpoints: [output.endpoint ?? "", error.endpoint ?? ""],
	};
}
/** Status flags a launcher sets again on a target's routed outputs (its parent's own, like make's O_APPEND); the rest pin the stream. */
const OUTPUT_STATUS_FLAGS = 0o2000 | 0o4000;
/** Reproducible launch state, shared by held-child dispatch and learned bindings. @param {ProcessExecutionContext} context @param {readonly string[]} [endpoints] @returns {ProcessLaunchOptions | undefined} */
export function processLaunchOptions(context, endpoints) {
	const semantic = JSON.parse(context.key);
	const [, output, error] = semantic.descriptors;
	const flags = /** @type {[number, number]} */ ([
		output.flags & OUTPUT_STATUS_FLAGS,
		error.flags & OUTPUT_STATUS_FLAGS,
	]);
	const pipes = /** @type {[boolean, boolean]} */ (
		context.outputEndpoints.map(
			(endpoint) => /^pipe:\[\d+\]$/.test(endpoint) && !endpoints?.includes(endpoint),
		)
	);
	const outlets = endpoints ?? context.outputEndpoints;
	/** @param {string} endpoint @param {1 | 2} fd */
	const route = (endpoint, fd) => {
		if (endpoint === "/dev/null") {
			return 0;
		} else {
			if (pipes[fd - 1]) {
				if (fd === 2 && endpoint === context.outputEndpoints[0]) {
					return 1;
				} else {
					return fd;
				}
			} else {
				if (endpoint === outlets[0]) {
					return 1;
				} else {
					if (endpoint === outlets[1]) {
						return 2;
					} else {
						return undefined;
					}
				}
			}
		}
	};
	const stdout = route(context.outputEndpoints[0], 1);
	const stderr = route(context.outputEndpoints[1], 2);
	if (stdout === undefined || stderr === undefined) return;
	return {
		outputRoute: [stdout, stderr],
		closeStdin: context.descriptorTypes[0] === "closed",
		...(pipes.some(Boolean) ? { outputPipes: pipes } : {}),
		...(flags.some(Boolean) ? { outputFlags: flags } : {}),
		stackLimit: semantic.rlimits.find(
			/** @param {string[]} limit */ /** @param {string[]} limit */ (limit) => limit[0] === "Max stack size",
		)[1],
	};
}
/** The launch's identity after restoring its streams and stack limit. @param {ProcessExecutionContext} context @param {ProcessLaunchOptions} launch */
export function routedProcessIdentity(
	context,
	{ outputRoute: route, closeStdin, outputPipes, outputFlags, stackLimit },
) {
	const semantic = JSON.parse(context.key);
	if (
		!semantic.credentials ||
		!semantic.signals ||
		![semantic.signals.blocked, semantic.signals.ignored].every(
			(value) => typeof value === "string" && /^[0-9a-f]+$/i.test(value),
		) ||
		!Array.isArray(semantic.descriptors) ||
		semantic.descriptors.length !== 3
	)
		throw new Error("invalid probed execution context");
	// Aliases number identities by first use; as in capture, every unproven null device shares one.
	const descriptors = [0, ...route].map((outlet, fd) => {
		const descriptor =
			!fd && closeStdin
				? { fd, type: "closed", flags: 0, alias: 0 }
				: fd && !outlet
					? {
							fd,
							type: "device",
							flags: discardFlags(),
							alias: 0,
							endpoint: "/dev/null",
						}
					: semantic.descriptors[outlet];
		const entry = {
			...descriptor,
			fd,
			alias:
				descriptor.type === "device" && descriptor.endpoint === "/dev/null"
					? "/dev/null"
					: fd
						? `outlet:${outlet}`
						: 0,
		};
		if (fd && outputPipes?.[fd - 1]) Object.assign(entry, { type: "pipe", flags: 1 });
		if (fd && outputFlags)
			entry.flags = (entry.flags & ~OUTPUT_STATUS_FLAGS) | (outputFlags[fd - 1] & OUTPUT_STATUS_FLAGS);
		return entry;
	});
	// libuv resets the signal mask and dispositions for every spawned target.
	const signals = {
		blocked: semantic.signals.blocked.replace(/[0-9a-f]/gi, "0"),
		ignored: semantic.signals.ignored.replace(/[0-9a-f]/gi, "0"),
	};
	if (stackLimit !== undefined)
		semantic.rlimits.find(
			/** @param {string[]} limit */ /** @param {string[]} limit */ (limit) => limit[0] === "Max stack size",
		)[1] = stackLimit;
	return {
		...contextKeys({
			...semantic,
			executionDomain: "ptrace",
			signals,
			descriptors,
		}),
		umask: context.umask,
	};
}
/** @param {unknown} value @returns {value is ProcessExecutionContext} */
export function validProcessContext(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const context = /** @type {Partial<ProcessExecutionContext>} */ (value);
	const types = context.descriptorTypes;
	return (
		typeof context.key === "string" &&
		context.key.length > 0 &&
		Buffer.byteLength(context.key) <= speculativeConfig().processReuse.contextKeyBytes &&
		typeof context.launchKey === "string" &&
		context.launchKey.length > 0 &&
		Buffer.byteLength(context.launchKey) <= speculativeConfig().processReuse.contextKeyBytes &&
		typeof context.umask === "number" &&
		Number.isSafeInteger(context.umask) &&
		context.umask >= 0 &&
		context.umask <= 0o777 &&
		Array.isArray(types) &&
		types.length === 3 &&
		["device", "closed"].includes(types[0]) &&
		Array.isArray(context.outputEndpoints) &&
		context.outputEndpoints.length === 2 &&
		// Output reaches a stream, or is discarded into /dev/null.
		context.outputEndpoints.every(
			(endpoint, index) =>
				typeof endpoint === "string" &&
				endpoint.length <= speculativeConfig().processReuse.endpointCharacters &&
				(["pipe", "socket"].includes(types[index + 1]) ||
					(types[index + 1] === "device" && endpoint === "/dev/null")),
		)
	);
}
/** @type {number | undefined} */
let nullOutputFlags;
/** Status flags of a write-only /dev/null, as `2>/dev/null` and the dispatcher's discard route open it. */
function discardFlags() {
	if (nullOutputFlags === undefined) {
		const fd = openSync("/dev/null", "w");
		let flags;
		try {
			flags = /^flags:\s*([0-7]+)/m.exec(readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))?.[1];
		} finally {
			closeSync(fd);
		}
		if (!flags) throw new Error("null device flags unavailable");
		nullOutputFlags = Number.parseInt(flags, 8) & ~0o2000000;
	}
	return nullOutputFlags;
}
/** @param {{ credentials: Record<string, unknown>, descriptors: readonly { type: DescriptorType, alias: unknown }[], [key: string]: unknown }} semantic */
function contextKeys(semantic) {
	const aliases = new Map();
	for (const descriptor of semantic.descriptors) {
		if (!aliases.has(descriptor.alias)) aliases.set(descriptor.alias, aliases.size);
		descriptor.alias = aliases.get(descriptor.alias);
	}
	const launch = {
		...semantic,
		credentials: { ...semantic.credentials, groups: "broker-preserved" },
		signals: "broker-normalized",
	};
	return {
		key: JSON.stringify(semantic),
		launchKey: JSON.stringify(launch),
		descriptorTypes: /** @type {[DescriptorType, DescriptorType, DescriptorType]} */ (
			semantic.descriptors.slice(0, 3).map(({ type }) => type)
		),
	};
}
/** @param {string} value */
function numbers(value) {
	const result = value ? value.split(/\s+/).map(Number) : [];
	if (result.some((item) => !Number.isSafeInteger(item) || item < 0))
		throw new Error("invalid held process status numbers");
	return result;
}
/** @param {Pick<ProcessExecutionContext, "key">} context */
export function byteStreamContextKey(context) {
	const semantic = JSON.parse(context.key);
	const sigpipe = 1n << 12n;
	if ((BigInt("0x" + semantic.signals.blocked) | BigInt("0x" + semantic.signals.ignored)) & sigpipe) return;
	for (const descriptor of semantic.descriptors)
		if (descriptor.fd === 1 || descriptor.fd === 2) {
			if (!["pipe", "socket"].includes(descriptor.type)) return;
			descriptor.type = "byte-stream";
			descriptor.flags &= ~(3 | 0x8000);
			delete descriptor.endpoint;
		}
	return JSON.stringify(semantic);
}
