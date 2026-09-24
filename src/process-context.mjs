// @ts-check
import { closeSync, fstatSync, openSync, readFileSync, readlinkSync, statSync } from "node:fs";
import { readFile, readlink, stat } from "node:fs/promises";

/** @typedef {import("./provenance-certificate.js").InheritedFileDescriptor["type"]} DescriptorType */
/** @typedef {import("./linux-held-exec.js").HeldFileDescriptor} HeldFileDescriptor */
/** @typedef {{
 * readonly key: string, readonly launchKey: string, readonly umask: number,
 * readonly descriptorTypes: readonly [DescriptorType, DescriptorType, DescriptorType],
 * readonly outputEndpoints: readonly [string, string]
 * readonly regularDescriptors?: readonly (Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type"> & { readonly image?: number })[]
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
				fd, endpoint: undefined, type: /** @type {DescriptorType} */ ("closed"), identity: "closed:0", flags: 0, queue: undefined,
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
			const type = endpoint === "anon_inode:[eventfd]" ? "eventfd" : metadata.isFile() ? "regular" : metadata.isDirectory() ? "directory" : metadata.isFIFO() ? "pipe" : metadata.isSocket() ? "socket" :
				metadata.isCharacterDevice() ? (proof?.type === "null" && metadata.rdev === 259n ? "null" : endpoint?.startsWith("/dev/pts/") ? "tty" : "device") : "other";
			if ((["regular", "null", "directory", "eventfd"].includes(type) || (type === "pipe" || type === "socket") && proof) && pid !== "self" && (!proof || proof.device !== String(metadata.dev) || proof.inode !== String(metadata.ino) ||
				proof.flags !== (Number.parseInt(flags, 8) & ~0o2000000) || String(proof.offset) !== /^pos:\s*(\d+)/m.exec(info)?.[1])) {
				throw new Error(`held descriptor ${fd} lacks matching native OFD evidence`);
			}
			if (proof && type !== (proof.type ?? "regular")) throw new Error(`held descriptor ${fd} changed type`);
			return {
				fd, endpoint, type,
				// Unproven null devices share one identity: a sandbox may serve its own /dev/null inode.
				identity: proof ? `ofd:${proof.alias}` : type === "device" && endpoint === "/dev/null" ? "/dev/null" : `${metadata.dev}:${metadata.ino}`,
				...((type === "pipe" || type === "socket") && proof ? { queue: `${metadata.dev}:${metadata.ino}` } : {}),
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
	/** @type {Map<string, number>} */
	const queues = new Map();
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
			if (descriptor.queue && !queues.has(descriptor.queue)) queues.set(descriptor.queue, descriptor.fd);
			// routedProcessContext reproduces this key order, including its /dev/null discard outlet.
			return { fd: descriptor.fd, type: descriptor.type, flags: descriptor.flags, alias: aliases.get(identity),
				...(descriptor.queue ? { queue: queues.get(descriptor.queue) } : {}), ...(descriptor.type === "device" ? { endpoint } : {}) };
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

/** @param {ProcessExecutionContext} context @param {readonly [0 | 1 | 2, 0 | 1 | 2]} route 0 discards into /dev/null @param {boolean} closeStdin @param {ProcessExecutionContext["regularDescriptors"]} [regularDescriptors] @param {readonly [boolean, boolean]} [outputPipes] @returns {ProcessExecutionContext} */
export function routedProcessContext(context, route, closeStdin = false, regularDescriptors, outputPipes) {
	const semantic = JSON.parse(context.key);
	if (!semantic.credentials || !semantic.signals ||
		![semantic.signals.blocked, semantic.signals.ignored].every(value => typeof value === "string" && /^[0-9a-f]+$/i.test(value)) ||
		!Array.isArray(semantic.descriptors) || semantic.descriptors.length !== 3) throw new Error("invalid probed execution context");
	/** @param {0 | 1 | 2} outlet */
	const output = outlet => outlet ? semantic.descriptors[outlet] : { fd: 0, type: "device", flags: discardFlags(), alias: 0, endpoint: "/dev/null" };
	// Aliases number identities by first use; as in capture, every unproven null device shares one.
	/** @param {{ type?: string, endpoint?: string }} descriptor @param {unknown} identity */
	const identity = (descriptor, identity) => descriptor.type === "device" && descriptor.endpoint === "/dev/null" ? "/dev/null" : identity;
	const descriptors = [
		closeStdin ? { fd: 0, type: "closed", flags: 0, alias: 0 } : { ...semantic.descriptors[0], alias: identity(semantic.descriptors[0], 0) },
		{ ...output(route[0]), fd: 1, alias: identity(output(route[0]), `outlet:${route[0]}`) },
		{ ...output(route[1]), fd: 2, alias: identity(output(route[1]), `outlet:${route[1]}`) },
	];
	for (let index = 0; index < 2; index++) if (outputPipes?.[index]) Object.assign(descriptors[index + 1], { type: "pipe", flags: 1 });
	for (const descriptor of regularDescriptors ?? []) {
		const entry = { fd: descriptor.fd, type: descriptor.type ?? "regular", flags: descriptor.flags, alias: `ofd:${descriptor.alias}`,
			...(descriptor.type === "pipe" || descriptor.type === "socket" ? { queue: descriptor.image } : {}) };
		if (descriptor.fd === 0) descriptors[0] = entry;
		else if (descriptor.fd > 2) descriptors.push(entry);
		else throw new Error("inherited output descriptor cannot use buffered routing");
	}
	const aliases = new Map();
	for (const descriptor of descriptors) {
		if (!aliases.has(descriptor.alias)) aliases.set(descriptor.alias, aliases.size);
		descriptor.alias = aliases.get(descriptor.alias);
	}
	// libuv resets the signal mask and dispositions for every spawned target.
	const signals = { blocked: semantic.signals.blocked.replace(/[0-9a-f]/gi, "0"), ignored: semantic.signals.ignored.replace(/[0-9a-f]/gi, "0") };
	return {
		...context,
		...contextKeys({ ...semantic, executionDomain: "ptrace", signals, descriptors }),
		descriptorTypes: [regularDescriptors?.find(({ fd }) => fd === 0)?.type ?? (regularDescriptors?.some(({ fd }) => fd === 0) ? "regular" : closeStdin ? "closed" : context.descriptorTypes[0]),
			outputPipes?.[0] ? "pipe" : route[0] ? context.descriptorTypes[route[0]] : "device", outputPipes?.[1] ? "pipe" : route[1] ? context.descriptorTypes[route[1]] : "device"],
		...(regularDescriptors?.length ? { regularDescriptors } : {}),
	};
}

/** @param {unknown} value @returns {value is ProcessExecutionContext} */
export function validProcessContext(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const context = /** @type {Partial<ProcessExecutionContext>} */ (value), types = context.descriptorTypes;
	return typeof context.key === "string" && context.key.length > 0 && context.key.length <= 64 * 1024 &&
		typeof context.launchKey === "string" && context.launchKey.length > 0 && context.launchKey.length <= 64 * 1024 &&
		typeof context.umask === "number" && Number.isSafeInteger(context.umask) && context.umask >= 0 && context.umask <= 0o777 &&
		Array.isArray(types) && types.length === 3 && (["device", "closed"].includes(types[0]) ||
			(["regular", "null", "directory", "pipe", "socket", "eventfd"].includes(types[0]) && context.regularDescriptors?.some(({ fd }) => fd === 0) === true)) &&
		Array.isArray(context.outputEndpoints) && context.outputEndpoints.length === 2 &&
		// Output reaches a stream, or is discarded into /dev/null.
		context.outputEndpoints.every((endpoint, index) => typeof endpoint === "string" && endpoint.length <= 4096 &&
			(["pipe", "socket"].includes(types[index + 1]) || types[index + 1] === "device" && endpoint === "/dev/null"));
}

/** @type {number | undefined} */
let nullOutputFlags;
/** Status flags of a write-only /dev/null, as `2>/dev/null` and the dispatcher's discard route open it. */
function discardFlags() {
	if (nullOutputFlags === undefined) {
		const fd = openSync("/dev/null", "w");
		let flags;
		try { flags = /^flags:\s*([0-7]+)/m.exec(readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))?.[1]; } finally { closeSync(fd); }
		if (!flags) throw new Error("null device flags unavailable");
		nullOutputFlags = Number.parseInt(flags, 8) & ~0o2000000;
	}
	return nullOutputFlags;
}

/** @param {number | "self"} pid @param {number} fd */
function descriptorTarget(pid, fd) {
	const target = `/proc/${pid}/fd/${fd}`;
	if (pid !== "self") return readlink(target);
	// The broker rejects missing output endpoints and preserves native fallback.
	try { return readlinkSync(target); } catch { return undefined; }
}

/** @param {{ credentials: Record<string, unknown>, [key: string]: unknown }} semantic */
function contextKeys(semantic) {
	const launch = { ...semantic, credentials: { ...semantic.credentials, groups: "broker-preserved" }, signals: "broker-normalized" };
	return { key: JSON.stringify(semantic), launchKey: JSON.stringify(launch) };
}

/** @param {string} value */
function numbers(value) {
	const result = value ? value.split(/\s+/).map(Number) : [];
	if (result.some(item => !Number.isSafeInteger(item) || item < 0)) throw new Error("invalid held process status numbers");
	return result;
}
