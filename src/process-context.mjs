// @ts-check
import { fstatSync, readFileSync, readlinkSync, statSync } from "node:fs";
import { readFile, readlink, stat } from "node:fs/promises";

/** @typedef {import("./provenance-certificate.js").InheritedFileDescriptor["type"]} DescriptorType */
/** @typedef {import("./linux-held-exec.js").HeldFileDescriptor} HeldFileDescriptor */
/** @typedef {{
 * readonly key: string, readonly launchKey: string, readonly umask: number,
 * readonly descriptorTypes: readonly [DescriptorType, DescriptorType, DescriptorType],
 * readonly outputEndpoints: readonly [string, string]
 * readonly regularDescriptors?: readonly Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type">[]
 * }} ProcessExecutionContext */

/**
 * Read the same kernel context for a stopped Actor image and the isolated dispatcher.
 * The caller owns the inherited table: ptrace observes it after exec; the native dispatcher
 * verifies it before Node opens its private descriptors. Endpoint aliases below describe
 * buffered stdio routing, not general open-file-description identity.
 * Capture does not authorize reuse: the dispatcher reports unsupported streams for native fallback.
 * @param {number | "self"} pid
 * @param {readonly string[]} inheritedDescriptors
 * @param {readonly HeldFileDescriptor[]} [regularDescriptors] Native OFD evidence held for this inspection.
 * @returns {Promise<ProcessExecutionContext>}
 */
export async function captureProcessContext(pid, inheritedDescriptors, regularDescriptors) {
	if (inheritedDescriptors.some(name => !/^\d+$/.test(name)) ||
		!inheritedDescriptors.includes("1") || !inheritedDescriptors.includes("2") ||
		inheritedDescriptors.some(name => Number(name) > 2 && !regularDescriptors?.some(({ fd }) => fd === Number(name))) ||
		regularDescriptors?.some(({ fd }) => !inheritedDescriptors.includes(String(fd)))) {
		throw new Error("held process has unmodeled inherited descriptors");
	}
	const root = `/proc/${pid}`;
	// Keep the dispatcher on its original synchronous path; remote inspection stays nonblocking.
	const statPath = pid === "self" ? statSync : stat;
	/** @param {string} target */
	const text = target => pid === "self" ? readFileSync(target, "utf8") : readFile(target, "utf8");
	const [status, limits, processStat, shell, descriptors] = await Promise.all([
		text(`${root}/status`),
		text(`${root}/limits`),
		text(`${root}/stat`),
		statPath("/bin/sh", { bigint: true }),
		Promise.all([...new Set([0, ...inheritedDescriptors.map(Number)])].sort((a, b) => a - b).map(async fd => {
			if (fd === 0 && !inheritedDescriptors.includes("0")) return {
				fd, endpoint: undefined, type: /** @type {DescriptorType} */ ("closed"), identity: "closed:0", flags: 0,
			};
			const [metadata, endpoint, info] = await Promise.all([
				// Inside the sandbox inspect the inherited handle, without resolving its proc magic link.
				pid === "self" ? fstatSync(fd, { bigint: true }) : stat(`${root}/fd/${fd}`, { bigint: true }),
				descriptorTarget(pid, fd),
				text(`${root}/fdinfo/${fd}`),
			]);
			const flags = /^flags:\s*([0-7]+)/m.exec(info)?.[1];
			if (!flags) throw new Error(`held descriptor ${fd} flags unavailable`);
			const proof = regularDescriptors?.find(descriptor => descriptor.fd === fd);
			/** @type {DescriptorType} */
			const type = metadata.isFile() ? "regular" : metadata.isFIFO() ? "pipe" : metadata.isSocket() ? "socket" :
				metadata.isCharacterDevice() ? (proof?.type === "null" && metadata.rdev === 259n ? "null" : endpoint?.startsWith("/dev/pts/") ? "tty" : "device") : "other";
			if ((type === "regular" || type === "null") && pid !== "self" && (!proof || proof.device !== String(metadata.dev) || proof.inode !== String(metadata.ino) ||
				proof.flags !== (Number.parseInt(flags, 8) & ~0o2000000) || String(proof.offset) !== /^pos:\s*(\d+)/m.exec(info)?.[1])) {
				throw new Error(`held descriptor ${fd} lacks matching native OFD evidence`);
			}
			if (proof && type !== (proof.type ?? "regular")) throw new Error(`held descriptor ${fd} changed type`);
			return {
				fd, endpoint, type,
				identity: proof ? `ofd:${proof.alias}` : `${metadata.dev}:${metadata.ino}`,
				flags: Number.parseInt(flags, 8) & ~0o2000000,
			};
		})),
	]);
	const [input, output, error] = descriptors;
	/** @param {string} name */
	const field = name => {
		const value = new RegExp(`^${name}:\\s*(.*)$`, "m").exec(status)?.[1];
		if (value === undefined) throw new Error(`held process status lacks ${name}`);
		return value.trim();
	};
	const uid = numbers(field("Uid")), gid = numbers(field("Gid")), groups = numbers(field("Groups"));
	if (uid.length !== 4 || gid.length !== 4) throw new Error("held process credentials are incomplete");
	if (!groups.includes(gid[1])) groups.push(gid[1]);
	groups.sort((left, right) => left - right);
	/** @type {Map<string, number>} */
	const aliases = new Map();
	const semantic = {
		executionDomain: "ptrace",
		rlimits: limits.split("\n").slice(1).map(line => line.trim().split(/\s{2,}/).slice(0, 2)),
		credentials: { uid: uid[0], euid: uid[1], gid: gid[0], egid: gid[1], groups },
		systemMetadata: Object.fromEntries(["dev", "ino", "mode", "uid", "gid", "rdev"].map(name =>
			[name, String(shell[/** @type {keyof typeof shell} */ (name)])])),
		signals: { blocked: field("SigBlk"), ignored: field("SigIgn") },
		scheduling: {
			nice: Number(processStat.slice(processStat.lastIndexOf(") ") + 2).trim().split(/\s+/)[16]),
			cpus: field("Cpus_allowed_list"), memoryNodes: field("Mems_allowed_list"),
		},
		descriptors: descriptors.map(({ endpoint, identity, ...descriptor }) => {
			if (!aliases.has(identity)) aliases.set(identity, aliases.size);
			return {
				fd: descriptor.fd, type: descriptor.type, flags: descriptor.flags,
				alias: aliases.get(identity),
				...(descriptor.type === "device" ? { endpoint } : {}),
			};
		}),
	};
	return {
		...contextKeys(semantic),
		umask: Number.parseInt(field("Umask"), 8),
		descriptorTypes: [input.type, output.type, error.type],
		outputEndpoints: [output.endpoint ?? "", error.endpoint ?? ""],
		...(regularDescriptors?.length ? { regularDescriptors } : {}),
	};
}

/** @param {ProcessExecutionContext} context @param {readonly [1 | 2, 1 | 2]} route @param {boolean} closeStdin @param {ProcessExecutionContext["regularDescriptors"]} [regularDescriptors] @returns {ProcessExecutionContext} */
export function routedProcessContext(context, route, closeStdin = false, regularDescriptors) {
	const semantic = JSON.parse(context.key);
	if (!semantic.credentials || !semantic.signals ||
		![semantic.signals.blocked, semantic.signals.ignored].every(value => typeof value === "string" && /^[0-9a-f]+$/i.test(value)) ||
		!Array.isArray(semantic.descriptors) || semantic.descriptors.length !== 3) throw new Error("invalid probed execution context");
	const descriptors = [
		closeStdin ? { fd: 0, type: "closed", flags: 0, alias: 0 } : semantic.descriptors[0],
		{ ...semantic.descriptors[route[0]], fd: 1, alias: 1 },
		{ ...semantic.descriptors[route[1]], fd: 2, alias: route[0] === route[1] ? 1 : 2 },
	];
	if (regularDescriptors?.length) {
		for (const descriptor of regularDescriptors) {
			const entry = { fd: descriptor.fd, type: descriptor.type ?? "regular", flags: descriptor.flags, alias: `ofd:${descriptor.alias}` };
			if (descriptor.fd === 0) descriptors[0] = entry;
			else if (descriptor.fd > 2) descriptors.push(entry);
			else throw new Error("inherited output descriptor cannot use buffered routing");
		}
		const aliases = new Map();
		for (const descriptor of descriptors) {
			if (!aliases.has(descriptor.alias)) aliases.set(descriptor.alias, aliases.size);
			descriptor.alias = aliases.get(descriptor.alias);
		}
	}
	// libuv resets the signal mask and dispositions for every spawned target.
	const signals = {
		blocked: semantic.signals.blocked.replace(/[0-9a-f]/gi, "0"),
		ignored: semantic.signals.ignored.replace(/[0-9a-f]/gi, "0"),
	};
	return {
		...context,
		...contextKeys({ ...semantic, executionDomain: "ptrace", signals, descriptors }),
		descriptorTypes: [regularDescriptors?.find(({ fd }) => fd === 0)?.type ?? (regularDescriptors?.some(({ fd }) => fd === 0) ? "regular" : closeStdin ? "closed" : context.descriptorTypes[0]), context.descriptorTypes[route[0]], context.descriptorTypes[route[1]]],
		...(regularDescriptors?.length ? { regularDescriptors } : {}),
	};
}

/** @param {unknown} value @returns {value is ProcessExecutionContext} */
export function validProcessContext(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const context = /** @type {Partial<ProcessExecutionContext>} */ (value);
	return typeof context.key === "string" && context.key.length > 0 && context.key.length <= 64 * 1024 &&
		typeof context.launchKey === "string" && context.launchKey.length > 0 && context.launchKey.length <= 64 * 1024 &&
		typeof context.umask === "number" && Number.isSafeInteger(context.umask) && context.umask >= 0 && context.umask <= 0o777 &&
		Array.isArray(context.descriptorTypes) && context.descriptorTypes.length === 3 &&
		(["device", "closed"].includes(context.descriptorTypes[0]) ||
			(["regular", "null"].includes(context.descriptorTypes[0]) && context.regularDescriptors?.some(({ fd }) => fd === 0) === true)) && ["pipe", "socket"].includes(context.descriptorTypes[1]) &&
		["pipe", "socket"].includes(context.descriptorTypes[2]) &&
		Array.isArray(context.outputEndpoints) && context.outputEndpoints.length === 2 &&
		context.outputEndpoints.every(endpoint => typeof endpoint === "string" && endpoint.length <= 4096);
}

/** @param {number | "self"} pid @param {number} fd */
function descriptorTarget(pid, fd) {
	const target = `/proc/${pid}/fd/${fd}`;
	if (pid !== "self") return readlink(target);
	try {
		return readlinkSync(target);
	} catch {
		return undefined; // The broker rejects missing output endpoints and preserves native fallback.
	}
}

/** @param {{ credentials: Record<string, unknown>, [key: string]: unknown }} semantic */
function contextKeys(semantic) {
	return {
		key: JSON.stringify(semantic),
		launchKey: JSON.stringify({
			...semantic,
			credentials: { ...semantic.credentials, groups: "broker-preserved" },
			signals: "broker-normalized",
		}),
	};
}

/** @param {string} value */
function numbers(value) {
	const result = value ? value.split(/\s+/).map(Number) : [];
	if (result.some(item => !Number.isSafeInteger(item) || item < 0)) throw new Error("invalid held process status numbers");
	return result;
}
