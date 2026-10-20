// @ts-check
import { closeSync, openSync, readFileSync } from "node:fs";
import { readFile, readlink, stat } from "node:fs/promises";

/** @typedef {import("./provenance-certificate.js").InheritedFileDescriptor["type"]} DescriptorType */
/** @typedef {import("./linux-held-exec.js").HeldFileDescriptor} HeldFileDescriptor */
/** @typedef {{
 * readonly key: string, readonly launchKey: string, readonly umask: number,
 * readonly descriptorTypes: readonly [DescriptorType, DescriptorType, DescriptorType],
 * readonly outputEndpoints: readonly [string, string]
 * readonly regularDescriptors?: readonly (Pick<HeldFileDescriptor, "fd" | "alias" | "flags" | "offset" | "type"> & { readonly image?: number })[]
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
	/** @param {import("node:fs").BigIntStats} metadata @returns {RawStat} */
	const fields = metadata => /** @type {RawStat} */ (Object.fromEntries(["dev", "ino", "mode", "uid", "gid", "rdev"].map(name =>
		[name, String(metadata[/** @type {keyof typeof metadata} */ (name)])])));
	const [status, limits, processStat, shell, reported] = await Promise.all([
		readFile(`${root}/status`, "utf8"), readFile(`${root}/limits`, "utf8"), readFile(`${root}/stat`, "utf8"), stat("/bin/sh", { bigint: true }),
		Promise.all([...new Set(inheritedDescriptors.map(Number))].sort((a, b) => a - b).map(async fd => {
			const [metadata, endpoint, fdinfo] = await Promise.all([stat(`${root}/fd/${fd}`, { bigint: true }), readlink(`${root}/fd/${fd}`), readFile(`${root}/fdinfo/${fd}`, "utf8")]);
			return { fd, stat: fields(metadata), endpoint, fdinfo };
		})),
	]);
	const descriptors = [...(reported[0]?.fd === 0 ? [] : [undefined]), ...reported].map(item => {
		if (!item) return { fd: 0, endpoint: undefined, type: /** @type {DescriptorType} */ ("closed"), identity: "closed:0", flags: 0, queue: undefined };
		const { fd, stat: metadata, endpoint, fdinfo: info } = item, format = BigInt(metadata.mode) & 0o170000n;
		const flags = /^flags:\s*([0-7]+)/m.exec(info)?.[1];
		if (!flags) throw new Error(`held descriptor ${fd} flags unavailable`);
		const proof = regularDescriptors?.find(descriptor => descriptor.fd === fd);
		/** @type {DescriptorType} */
		const type = endpoint === "anon_inode:[eventfd]" ? "eventfd" : format === 0o100000n ? "regular" : format === 0o040000n ? "directory" : format === 0o010000n ? "pipe" : format === 0o140000n ? "socket" :
			format === 0o020000n ? (proof?.type === "null" && BigInt(metadata.rdev) === 259n ? "null" : endpoint?.startsWith("/dev/pts/") ? "tty" : "device") : "other";
		if ((["regular", "null", "directory", "eventfd"].includes(type) || (type === "pipe" || type === "socket") && proof) && regularDescriptors && (!proof || proof.device !== metadata.dev || proof.inode !== metadata.ino ||
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
	});
	const [, output, error] = descriptors;
	if (!output || output.fd !== 1 || !error || error.fd !== 2) throw new Error("reported process lacks its output streams");
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
	const queues = new Map();
	const semantic = {
		executionDomain: "ptrace",
		// Core's hard cap is confinement; observing it is guarded by the trace.
		rlimits: limits.trim().split("\n").slice(1).map(line => line.trim().split(/\s{2,}/).slice(0, line.startsWith("Max core file size") ? 2 : 3)),
		credentials: { uid: uid[0], euid: uid[1], gid: gid[0], egid: gid[1], groups },
		systemMetadata: fields(shell),
		signals: { blocked: field("SigBlk"), ignored: field("SigIgn") },
		scheduling: {
			nice: Number(processStat.slice(processStat.lastIndexOf(") ") + 2).trim().split(/\s+/)[16]),
			cpus: field("Cpus_allowed_list"), memoryNodes: field("Mems_allowed_list"),
		},
		descriptors: descriptors.map(({ endpoint, identity, ...descriptor }) => {
			if (descriptor.queue && !queues.has(descriptor.queue)) queues.set(descriptor.queue, descriptor.fd);
			// routedProcessIdentity reproduces this key order, including its /dev/null discard outlet.
			return { fd: descriptor.fd, type: descriptor.type, flags: descriptor.flags, alias: identity,
				...(descriptor.queue ? { queue: queues.get(descriptor.queue) } : {}), ...(descriptor.type === "device" ? { endpoint } : {}) };
		}),
	};
	return {
		...contextKeys(semantic),
		umask: Number.parseInt(field("Umask"), 8),
		outputEndpoints: [output.endpoint ?? "", error.endpoint ?? ""],
		...(regularDescriptors?.length ? { regularDescriptors } : {}),
	};
}

/** Status flags a launcher sets again on a target's routed outputs (its parent's own, like make's O_APPEND); the rest pin the stream. */
const OUTPUT_STATUS_FLAGS = 0o2000 | 0o4000;

/** Reproducible launch state, shared by held-child dispatch and learned bindings. @param {ProcessExecutionContext} context @param {readonly string[]} [endpoints] @returns {ProcessLaunchOptions | undefined} */
export function processLaunchOptions(context, endpoints) {
	const semantic = JSON.parse(context.key), [, output, error] = semantic.descriptors;
	const flags = /** @type {[number, number]} */ ([output.flags & OUTPUT_STATUS_FLAGS, error.flags & OUTPUT_STATUS_FLAGS]);
	const pipes = /** @type {[boolean, boolean]} */ (context.outputEndpoints.map(endpoint => /^pipe:\[\d+\]$/.test(endpoint) && !endpoints?.includes(endpoint)));
	const outlets = endpoints ?? context.outputEndpoints;
	/** @param {string} endpoint @param {1 | 2} fd */
	const route = (endpoint, fd) => endpoint === "/dev/null" ? 0 : pipes[fd - 1] ? fd === 2 && endpoint === context.outputEndpoints[0] ? 1 : fd
		: endpoint === outlets[0] ? 1 : endpoint === outlets[1] ? 2 : undefined;
	const stdout = route(context.outputEndpoints[0], 1), stderr = route(context.outputEndpoints[1], 2);
	if (stdout === undefined || stderr === undefined) return;
	return { outputRoute: [stdout, stderr], closeStdin: context.descriptorTypes[0] === "closed",
		...(pipes.some(Boolean) ? { outputPipes: pipes } : {}), ...(flags.some(Boolean) ? { outputFlags: flags } : {}),
		stackLimit: semantic.rlimits.find(/** @param {string[]} limit */ limit => limit[0] === "Max stack size")[1] };
}

/** The launch's identity after restoring its streams and stack limit. @param {ProcessExecutionContext} context @param {ProcessLaunchOptions} launch @param {ProcessExecutionContext["regularDescriptors"]} [regularDescriptors] */
export function routedProcessIdentity(context, { outputRoute: route, closeStdin, outputPipes, outputFlags, stackLimit }, regularDescriptors) {
	const semantic = JSON.parse(context.key);
	if (!semantic.credentials || !semantic.signals ||
		![semantic.signals.blocked, semantic.signals.ignored].every(value => typeof value === "string" && /^[0-9a-f]+$/i.test(value)) ||
		!Array.isArray(semantic.descriptors) || semantic.descriptors.length !== 3) throw new Error("invalid probed execution context");
	// Aliases number identities by first use; as in capture, every unproven null device shares one.
	const descriptors = [0, ...route].map((outlet, fd) => {
		const descriptor = !fd && closeStdin ? { fd, type: "closed", flags: 0, alias: 0 } : fd && !outlet
			? { fd, type: "device", flags: discardFlags(), alias: 0, endpoint: "/dev/null" } : semantic.descriptors[outlet];
		const entry = { ...descriptor, fd, alias: descriptor.type === "device" && descriptor.endpoint === "/dev/null" ? "/dev/null" : fd ? `outlet:${outlet}` : 0 };
		if (fd && outputPipes?.[fd - 1]) Object.assign(entry, { type: "pipe", flags: 1 });
		if (fd && outputFlags) entry.flags = entry.flags & ~OUTPUT_STATUS_FLAGS | outputFlags[fd - 1] & OUTPUT_STATUS_FLAGS;
		return entry;
	});
	for (const descriptor of regularDescriptors ?? []) {
		const entry = { fd: descriptor.fd, type: descriptor.type ?? "regular", flags: descriptor.flags, alias: `ofd:${descriptor.alias}`,
			...(descriptor.type === "pipe" || descriptor.type === "socket" ? { queue: descriptor.image } : {}) };
		if (descriptor.fd === 0) descriptors[0] = entry;
		else if (descriptor.fd > 2) descriptors.push(entry);
		else throw new Error("inherited output descriptor cannot use buffered routing");
	}
	// libuv resets the signal mask and dispositions for every spawned target.
	const signals = { blocked: semantic.signals.blocked.replace(/[0-9a-f]/gi, "0"), ignored: semantic.signals.ignored.replace(/[0-9a-f]/gi, "0") };
	if (stackLimit !== undefined) semantic.rlimits.find(/** @param {string[]} limit */ limit => limit[0] === "Max stack size")[1] = stackLimit;
	return { ...contextKeys({ ...semantic, executionDomain: "ptrace", signals, descriptors }), umask: context.umask };
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

/** @param {{ credentials: Record<string, unknown>, descriptors: readonly { type: DescriptorType, alias: unknown }[], [key: string]: unknown }} semantic */
function contextKeys(semantic) {
	const aliases = new Map();
	for (const descriptor of semantic.descriptors) {
		if (!aliases.has(descriptor.alias)) aliases.set(descriptor.alias, aliases.size);
		descriptor.alias = aliases.get(descriptor.alias);
	}
	const launch = { ...semantic, credentials: { ...semantic.credentials, groups: "broker-preserved" }, signals: "broker-normalized" };
	return { key: JSON.stringify(semantic), launchKey: JSON.stringify(launch),
		descriptorTypes: /** @type {[DescriptorType, DescriptorType, DescriptorType]} */ (semantic.descriptors.slice(0, 3).map(({ type }) => type)) };
}

/** @param {string} value */
function numbers(value) {
	const result = value ? value.split(/\s+/).map(Number) : [];
	if (result.some(item => !Number.isSafeInteger(item) || item < 0)) throw new Error("invalid held process status numbers");
	return result;
}
