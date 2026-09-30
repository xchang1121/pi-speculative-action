import { directoryEntriesDigest } from "./process-observation.ts";
import { lstat, readFile, stat } from "node:fs/promises";
import path from "node:path";
import { captureFileDigest, captureFilesystemEntry, mapFilesystem, sameFilesystemIdentity } from "./filesystem-evidence.ts";
import { errorMessage, isMissing as missing } from "./error-utils.ts";
import { type DynamicDependency, type DynamicDependencyCertificate, filesystemMetadataDigest, filesystemObservationDigest,
	type FilesystemObservationField, type ProcessProvenanceCertificate, processStrongKey, type ProvenanceTaint, sha256Digest,
	type Sha256Digest } from "./provenance-certificate.ts";

export interface ProvenanceValidationContext {
	/** Map a stable logical path from the certificate into the current physical execution world. */
	readonly resolvePath?: (logicalPath: string) => string | undefined;
	/** Current inherited readable-FD content identities. Missing entries fail closed. */
	readonly fileDescriptors?: ReadonlyMap<number, { readonly contentDigest: Sha256Digest; readonly eof: boolean }>;
	readonly maxFileBytes?: number;
	/** Taints whose semantics the consumer proves equivalent in its execution domain. */
	readonly acceptedTaints?: readonly ProvenanceTaint[];
}

export type DynamicDependencyValidation = (
	| { readonly status: "valid"; readonly dependencies: readonly DynamicDependency[]; }
	| {
			readonly status: "stale";
			readonly changed: readonly string[];
			/** Current evidence, when the original observation shape could still be captured. */
			readonly dependencies: readonly DynamicDependency[];
	  }
	| { readonly status: "indeterminate"; readonly reason: string; }
) & { readonly filesRead: number; readonly bytesRead: number; readonly durationMs: number };

export type ProvenanceValidation =
	| (Extract<DynamicDependencyValidation, { status: "valid" }> & { readonly strongKey: Sha256Digest })
	| Exclude<DynamicDependencyValidation, { status: "valid" }>;

const OBSERVATION_FIELDS = {
	file: ["contentDigest", "metadataDigest", "aliases"],
	directory: ["entriesDigest", "metadataDigest"],
	absence: ["parentEntriesDigest"],
	symlink: ["targetDigest", "target"],
	metadata: ["digest"],
	fd: ["contentDigest", "eof"],
	lock: ["exclusive"],
} satisfies { [Kind in DynamicDependency["kind"]]: readonly (keyof Extract<DynamicDependency, { kind: Kind }>)[] };

export async function validateProcessCertificate(
	certificate: ProcessProvenanceCertificate,
	context: ProvenanceValidationContext = {},
): Promise<ProvenanceValidation> {
	const { weakKey, strongKey: expectedKey } = certificate;
	const validation = await validateDynamicDependencyCertificate(certificate.dependencyCertificate, context);
	if (validation.status !== "valid") return validation;
	const strongKey = processStrongKey(weakKey, { complete: true, dependencies: validation.dependencies, taints: [] });
	if (strongKey !== expectedKey) return { ...validation, status: "stale", changed: Object.freeze(["strong_key"]) };
	return { ...validation, strongKey };
}

/** Validate reusable dynamic evidence without requiring a persisted process result. */
export async function validateDynamicDependencyCertificate(
	certificate: DynamicDependencyCertificate,
	context: ProvenanceValidationContext = {},
): Promise<DynamicDependencyValidation> {
	const startedAt = performance.now();
	let filesRead = 0;
	let bytesRead = 0;
	const metrics = () => ({ filesRead, bytesRead, durationMs: Math.max(0, performance.now() - startedAt) });
	const indeterminate = (reason: string): DynamicDependencyValidation => ({ status: "indeterminate", reason, ...metrics() });
	if (!certificate.complete) return indeterminate("trace_incomplete");
	const acceptedTaints = new Set(context.acceptedTaints ?? []);
	const blockingTaints = certificate.taints.filter((taint) => !acceptedTaints.has(taint));
	if (blockingTaints.length) return indeterminate(`tainted:${blockingTaints.join(",")}`);

	const current: DynamicDependency[] = [];
	const changed: string[] = [];
	// Independent observations: checked concurrently, settled in certificate order (the strong key hashes that order).
	const expectations = structuredClone(certificate.dependencies), listings = new Map<string, ReturnType<typeof captureFilesystemEntry>>();
	// Names missing from one directory, or its own entries, share one listing of it within a validation.
	const list = (target: string) => listings.get(target) ?? listings.set(target, captureFilesystemEntry(target, "directory")).get(target)!;
	const checks = await mapFilesystem(expectations, async (expected): Promise<{ observed?: DynamicDependency; missing?: true } | string> => {
		try {
			if (expected.kind === "fd") {
				const descriptor = context.fileDescriptors?.get(expected.fd);
				return descriptor ? { observed: { kind: "fd", fd: expected.fd, ...descriptor } } : `fd_unavailable:${expected.fd}`;
			}
			const physicalPath = context.resolvePath ? context.resolvePath(expected.path) : path.isAbsolute(expected.path) ? path.resolve(expected.path) : undefined;
			if (!physicalPath) return `path_unmapped:${expected.path}`;
			switch (expected.kind) {
				case "file": {
					const captured = await captureFileDependency(physicalPath, expected.path, expected.role, {
						includeMetadata: expected.metadataDigest !== undefined, maxFileBytes: context.maxFileBytes, aliases: expected.aliases, resolvePath: context.resolvePath });
					filesRead += captured.filesRead;
					bytesRead += captured.bytesRead;
					return { observed: captured.dependency };
				}
				case "directory": return { observed: await captureDirectoryDependency(physicalPath, expected.path, expected.metadataDigest !== undefined,
					expected.excludedEntries, expected.entriesDigest !== undefined, list) };
				case "absence": return { observed: await captureAbsenceDependency(physicalPath, expected.path, expected.parentEntriesDigest !== undefined,
					expected.parentExcludedEntries, list) };
				case "symlink": return { observed: await captureSymlinkDependency(physicalPath, expected.path) };
				case "metadata": return { observed: await captureMetadataDependency(physicalPath, expected.path, expected.followSymlinks, expected.fields) };
				case "lock": return { observed: await probeLock(physicalPath, expected.path, expected.exclusive) };
			}
		} catch (error) {
			if (missing(error)) return { missing: true };
			return `validation_error:${expected.kind === "fd" ? expected.fd : expected.path}:${errorMessage(error)}`;
		}
	});
	for (const [index, check] of checks.entries()) {
		if (typeof check === "string") return indeterminate(check);
		const expected = expectations[index]!, { observed } = check;
		if (observed) current.push(observed);
		if (!observed || OBSERVATION_FIELDS[expected.kind].some(field => Reflect.get(observed, field) !== Reflect.get(expected, field))) {
			changed.push(expected.kind === "fd" ? `fd:${expected.fd}` : expected.path);
		}
	}

	return { ...(changed.length ? { status: "stale", changed: Object.freeze([...new Set(changed)]) } : { status: "valid" }), dependencies: Object.freeze(current), ...metrics() };
}

/** A lock still holds as a dependency while no one holds one it would conflict with: /proc/locks lists every flock, record and
 * open-file lock by device and inode. A missing file is gone, not made. */
async function probeLock(physicalPath: string, logicalPath: string, exclusive: boolean): Promise<Extract<DynamicDependency, { kind: "lock" }> | undefined> {
	const info = await lstat(physicalPath, { bigint: true }), { dev } = info, hex = (value: bigint) => value.toString(16).padStart(2, "0");
	const object = `${hex(dev >> 8n & 0xfffn | dev >> 32n & ~0xfffn)}:${hex(dev & 0xffn | dev >> 12n & ~0xffn)}:${info.ino}`;
	const held = (await readFile("/proc/locks", "utf8")).split("\n").some(line => { const [, access, locked] = /^\d+:\s+\w+\s+\w+\s+(READ|WRITE)\s+\S+\s+(\S+)\s/.exec(line) ?? [];
		return locked === object && (exclusive || access === "WRITE"); });
	return info.isFile() && !held ? { kind: "lock", path: logicalPath, exclusive } : undefined;
}

export async function captureMetadataDependency(
	physicalPath: string,
	logicalPath: string,
	followSymlinks: boolean,
	fields?: readonly FilesystemObservationField[],
): Promise<Extract<DynamicDependency, { kind: "metadata" }>> {
	const observed = await (followSymlinks ? stat : lstat)(physicalPath, { bigint: true });
	return { kind: "metadata", path: logicalPath, followSymlinks, digest: filesystemObservationDigest(observed, fields), ...(fields ? { fields } : {}) };
}

export async function captureFileDependency(
	physicalPath: string,
	logicalPath: string,
	role: Extract<DynamicDependency, { kind: "file" }>["role"] = "input",
	options: { readonly includeMetadata?: boolean; readonly maxFileBytes?: number; readonly aliases?: readonly string[];
		readonly resolvePath?: ProvenanceValidationContext["resolvePath"] } = {},
): Promise<{ readonly dependency: Extract<DynamicDependency, { kind: "file" }>; readonly bytesRead: number; readonly filesRead: number }> {
	const maxBytes = finiteLimit(options.maxFileBytes ?? Number.POSITIVE_INFINITY);
	const content = await captureFileDigest(physicalPath, maxBytes);
	let aliases = options.aliases;
	if (aliases) {
		const states = await Promise.all(aliases.map(async logical => {
			const physical = options.resolvePath ? options.resolvePath(logical) : path.resolve(logical);
			if (!physical) throw new Error(`alias_unmapped:${logical}`);
			return lstat(physical, { bigint: true });
		}));
		if (content.stat.nlink !== BigInt(aliases.length) || states.some(state => !state.isFile() || !sameFilesystemIdentity(content.stat, state))) aliases = undefined; // Changed topology: a plain file entry.
	}
	return {
		dependency: {
			kind: "file",
			path: logicalPath,
			role,
			contentDigest: `sha256:${content.hash}`,
			...(options.includeMetadata ? { metadataDigest: filesystemMetadataDigest(content.stat) } : {}),
			...(aliases ? { aliases } : {}),
		},
		bytesRead: content.shared ? 0 : content.bytesRead,
		filesRead: content.shared ? 0 : 1,
	};
}

export async function captureDirectoryDependency(
	physicalPath: string,
	logicalPath: string,
	includeMetadata = false,
	excludedEntries: readonly string[] = [],
	listed = true,
	list = (target: string) => captureFilesystemEntry(target, "directory"),
): Promise<Extract<DynamicDependency, { kind: "directory" }>> {
	if (!listed) {
		const info = await lstat(physicalPath, { bigint: true });
		if (!info.isDirectory()) throw new Error("not_directory");
		return { kind: "directory", path: logicalPath, metadataDigest: filesystemMetadataDigest(info) };
	}
	const excluded = new Set(excludedEntries);
	const { info, entries } = await list(physicalPath);
	if (!entries) throw new Error("not_directory");
	return {
		kind: "directory",
		path: logicalPath,
		entriesDigest: directoryEntriesDigest(entries.filter((entry) => !excluded.has(entry.name))),
		...(includeMetadata ? { metadataDigest: filesystemMetadataDigest(info) } : {}),
		...(excluded.size ? { excludedEntries: Object.freeze([...excluded].sort()) } : {}),
	};
}

export async function captureAbsenceDependency(
	physicalPath: string,
	logicalPath: string,
	captureParent = true,
	parentExcludedEntries: readonly string[] = [],
	list?: Parameters<typeof captureDirectoryDependency>[5],
): Promise<Extract<DynamicDependency, { kind: "absence" }> | undefined> {
	try { await lstat(physicalPath); return undefined; } catch (error) {
		if (!missing(error)) throw error;
	}
	if (!captureParent) return { kind: "absence", path: logicalPath };
	const parentPhysical = path.dirname(physicalPath);
	const parentLogical = path.posix.dirname(logicalPath.replaceAll("\\", "/"));
	const parent = await captureDirectoryDependency(parentPhysical, parentLogical, false, parentExcludedEntries, true, list);
	return { kind: "absence", path: logicalPath, parentEntriesDigest: parent.entriesDigest, ...(parent.excludedEntries ? { parentExcludedEntries: parent.excludedEntries } : {}) };
}

export async function captureSymlinkDependency(
	physicalPath: string,
	logicalPath: string,
): Promise<Extract<DynamicDependency, { kind: "symlink" }>> {
	const { link: target } = await captureFilesystemEntry(physicalPath);
	if (target === undefined) throw new Error("not_symlink");
	return { kind: "symlink", path: logicalPath, target, targetDigest: sha256Digest(Buffer.from(target, "utf8")) };
}

function finiteLimit(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : Number.POSITIVE_INFINITY;
}
