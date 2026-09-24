import { directoryEntriesDigest } from "./process-observation.ts";
import { lstat, stat } from "node:fs/promises";
import path from "node:path";
import { captureFilesystemEntry, captureStableFile, sameFilesystemIdentity } from "./filesystem-evidence.ts";
import { errorMessage, isMissing as missing } from "./error-utils.ts";
import {
	type DynamicDependency,
	type DynamicDependencyCertificate,
	filesystemMetadataDigest,
	filesystemObservationDigest,
	type ProcessProvenanceCertificate,
	processStrongKey,
	type ProvenanceTaint,
	sha256Digest,
	type Sha256Digest,
} from "./provenance-certificate.ts";

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
	| {
			readonly status: "valid";
			readonly dependencies: readonly DynamicDependency[];
	  }
	| {
			readonly status: "stale";
			readonly changed: readonly string[];
			/** Current evidence, when the original observation shape could still be captured. */
			readonly dependencies: readonly DynamicDependency[];
	  }
	| {
			readonly status: "indeterminate";
			readonly reason: string;
	  }
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
	if (!certificate.complete) {
		return indeterminate("trace_incomplete");
	}
	const acceptedTaints = new Set(context.acceptedTaints ?? []);
	const blockingTaints = certificate.taints.filter((taint) => !acceptedTaints.has(taint));
	if (blockingTaints.length) {
		return indeterminate(`tainted:${blockingTaints.join(",")}`);
	}

	const current: DynamicDependency[] = [];
	const changed: string[] = [];
	for (const expected of structuredClone(certificate.dependencies)) {
		try {
			let observed: DynamicDependency | undefined;
			if (expected.kind === "fd") {
				const descriptor = context.fileDescriptors?.get(expected.fd);
				if (!descriptor) return indeterminate(`fd_unavailable:${expected.fd}`);
				observed = { kind: "fd", fd: expected.fd, ...descriptor };
			} else {
				const physicalPath = context.resolvePath
					? context.resolvePath(expected.path)
					: path.isAbsolute(expected.path) ? path.resolve(expected.path) : undefined;
				if (!physicalPath) return indeterminate(`path_unmapped:${expected.path}`);
				switch (expected.kind) {
					case "file": {
						const captured = await captureFileDependency(physicalPath, expected.path, expected.role, {
							includeMetadata: expected.metadataDigest !== undefined,
							maxFileBytes: context.maxFileBytes,
							aliases: expected.aliases, resolvePath: context.resolvePath,
						});
						filesRead += captured.filesRead;
						bytesRead += captured.bytesRead;
						observed = captured.dependency;
						break;
					}
					case "directory":
						observed = await captureDirectoryDependency(
							physicalPath,
							expected.path,
							expected.metadataDigest !== undefined,
							expected.excludedEntries,
						);
						break;
					case "absence":
						observed = await captureAbsenceDependency(
							physicalPath,
							expected.path,
							expected.parentEntriesDigest !== undefined,
							expected.parentExcludedEntries,
						);
						break;
					case "symlink":
						observed = await captureSymlinkDependency(physicalPath, expected.path);
						break;
					case "metadata":
						observed = await captureMetadataDependency(physicalPath, expected.path, expected.followSymlinks);
						break;
				}
			}
			if (observed) current.push(observed);
			if (!observed || OBSERVATION_FIELDS[expected.kind].some(field => Reflect.get(observed, field) !== Reflect.get(expected, field))) {
				changed.push(expected.kind === "fd" ? `fd:${expected.fd}` : expected.path);
			}
		} catch (error) {
			if (missing(error)) {
				changed.push(expected.kind === "fd" ? `fd:${expected.fd}` : expected.path);
				continue;
			}
			return indeterminate(`validation_error:${expected.kind === "fd" ? expected.fd : expected.path}:${errorMessage(error)}`);
		}
	}

	return {
		...(changed.length ? { status: "stale", changed: Object.freeze([...new Set(changed)]) } : { status: "valid" }),
		dependencies: Object.freeze(current),
		...metrics(),
	};
}

export async function captureMetadataDependency(
	physicalPath: string,
	logicalPath: string,
	followSymlinks: boolean,
): Promise<Extract<DynamicDependency, { kind: "metadata" }>> {
	const observed = await (followSymlinks ? stat : lstat)(physicalPath, { bigint: true });
	return {
		kind: "metadata",
		path: logicalPath,
		followSymlinks,
		digest: filesystemObservationDigest(observed),
	};
}

export async function captureFileDependency(
	physicalPath: string,
	logicalPath: string,
	role: Extract<DynamicDependency, { kind: "file" }>["role"] = "input",
	options: { readonly includeMetadata?: boolean; readonly maxFileBytes?: number; readonly aliases?: readonly string[];
		readonly resolvePath?: ProvenanceValidationContext["resolvePath"] } = {},
): Promise<{ readonly dependency: Extract<DynamicDependency, { kind: "file" }>; readonly bytesRead: number; readonly filesRead: number }> {
	const maxBytes = finiteLimit(options.maxFileBytes ?? Number.POSITIVE_INFINITY);
	const content = await captureStableFile(physicalPath, maxBytes);
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
): Promise<Extract<DynamicDependency, { kind: "directory" }>> {
	const excluded = new Set(excludedEntries);
	const { info, entries } = await captureFilesystemEntry(physicalPath, "directory");
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
): Promise<Extract<DynamicDependency, { kind: "absence" }> | undefined> {
	try {
		await lstat(physicalPath);
		return undefined;
	} catch (error) {
		if (!missing(error)) throw error;
	}
	if (!captureParent) return { kind: "absence", path: logicalPath };
	const parentPhysical = path.dirname(physicalPath);
	const parentLogical = path.posix.dirname(logicalPath.replaceAll("\\", "/"));
	const parent = await captureDirectoryDependency(parentPhysical, parentLogical, false, parentExcludedEntries);
	return {
		kind: "absence",
		path: logicalPath,
		parentEntriesDigest: parent.entriesDigest,
		...(parent.excludedEntries ? { parentExcludedEntries: parent.excludedEntries } : {}),
	};
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
