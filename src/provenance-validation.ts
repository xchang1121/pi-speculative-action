import { lstat, readFile, readdir, readlink } from "node:fs/promises";
import path from "node:path";
import {
	type DynamicDependency,
	type DynamicDependencyCertificate,
	digestObject,
	filesystemEntryType,
	filesystemMetadataDigest,
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
	/** One-shot inputs that the caller proves are transferred rather than replayed. */
	readonly acceptedTaints?: readonly ProvenanceTaint[];
}

export type ProvenanceValidation =
	| {
			readonly status: "valid";
			readonly strongKey: Sha256Digest;
			readonly dependencies: readonly DynamicDependency[];
			readonly filesRead: number;
			readonly bytesRead: number;
			readonly durationMs: number;
	  }
	| {
			readonly status: "stale";
			readonly changed: readonly string[];
			/** Current evidence, when the original observation shape could still be captured. */
			readonly dependencies: readonly DynamicDependency[];
			readonly filesRead: number;
			readonly bytesRead: number;
			readonly durationMs: number;
	  }
	| {
			readonly status: "indeterminate";
			readonly reason: string;
			readonly filesRead: number;
			readonly bytesRead: number;
			readonly durationMs: number;
	  };

export type DynamicDependencyValidation =
	| {
			readonly status: "valid";
			readonly dependencies: readonly DynamicDependency[];
			readonly filesRead: number;
			readonly bytesRead: number;
			readonly durationMs: number;
	  }
	| Extract<ProvenanceValidation, { readonly status: "stale" | "indeterminate" }>;

export async function validateProcessCertificate(
	certificate: ProcessProvenanceCertificate,
	context: ProvenanceValidationContext = {},
): Promise<ProvenanceValidation> {
	const validation = await validateDynamicDependencyCertificate(certificate.dependencyCertificate, context);
	if (validation.status !== "valid") return validation;
	const dependencyCertificate: DynamicDependencyCertificate = {
		complete: true,
		dependencies: validation.dependencies,
		taints: [],
	};
	const strongKey = processStrongKey(certificate.weakKey, dependencyCertificate);
	if (strongKey !== certificate.strongKey) {
		return {
			status: "stale",
			changed: Object.freeze(["strong_key"]),
			dependencies: validation.dependencies,
			filesRead: validation.filesRead,
			bytesRead: validation.bytesRead,
			durationMs: validation.durationMs,
		};
	}
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
	if (!certificate.complete) {
		return indeterminate("trace_incomplete", startedAt, filesRead, bytesRead);
	}
	const acceptedTaints = new Set(context.acceptedTaints ?? []);
	const blockingTaints = certificate.taints.filter((taint) => !acceptedTaints.has(taint));
	if (blockingTaints.length) {
		return indeterminate(
			`tainted:${blockingTaints.join(",")}`,
			startedAt,
			filesRead,
			bytesRead,
		);
	}

	const current: DynamicDependency[] = [];
	const changed: string[] = [];
	for (const expected of certificate.dependencies) {
		try {
			if (expected.kind === "fd") {
				const descriptor = context.fileDescriptors?.get(expected.fd);
				if (!descriptor) return indeterminate(`fd_unavailable:${expected.fd}`, startedAt, filesRead, bytesRead);
				const observed: DynamicDependency = { kind: "fd", fd: expected.fd, ...descriptor };
				current.push(observed);
				if (descriptor.contentDigest !== expected.contentDigest || descriptor.eof !== expected.eof) {
					changed.push(`fd:${expected.fd}`);
				}
				continue;
			}

			const physicalPath = resolveEvidencePath(expected.path, context);
			if (!physicalPath) return indeterminate(`path_unmapped:${expected.path}`, startedAt, filesRead, bytesRead);
			switch (expected.kind) {
				case "file": {
					const observed = await captureFileDependency(physicalPath, expected.path, expected.role, {
						includeMetadata: expected.metadataDigest !== undefined,
						maxFileBytes: context.maxFileBytes,
					});
					filesRead++;
					bytesRead += observed.bytesRead;
					current.push(observed.dependency);
					if (
						observed.dependency.contentDigest !== expected.contentDigest ||
						observed.dependency.metadataDigest !== expected.metadataDigest
					) {
						changed.push(expected.path);
					}
					break;
				}
				case "directory": {
					const dependency = await captureDirectoryDependency(
						physicalPath,
						expected.path,
						expected.metadataDigest !== undefined,
						expected.excludedEntries,
					);
					current.push(dependency);
					if (
						dependency.entriesDigest !== expected.entriesDigest ||
						dependency.metadataDigest !== expected.metadataDigest
					) {
						changed.push(expected.path);
					}
					break;
				}
				case "absence": {
					const dependency = await captureAbsenceDependency(
						physicalPath,
						expected.path,
						expected.parentEntriesDigest !== undefined,
						expected.parentExcludedEntries,
					);
					if (!dependency) {
						changed.push(expected.path);
						break;
					}
					current.push(dependency);
					if (dependency.parentEntriesDigest !== expected.parentEntriesDigest) changed.push(expected.path);
					break;
				}
				case "symlink": {
					const dependency = await captureSymlinkDependency(physicalPath, expected.path);
					current.push(dependency);
					if (dependency.targetDigest !== expected.targetDigest || dependency.target !== expected.target) {
						changed.push(expected.path);
					}
					break;
				}
			}
		} catch (error) {
			if (missing(error)) {
				changed.push(expected.kind === "fd" ? `fd:${expected.fd}` : expected.path);
				continue;
			}
			return indeterminate(
				`validation_error:${expected.kind === "fd" ? expected.fd : expected.path}:${errorMessage(error)}`,
				startedAt,
				filesRead,
				bytesRead,
			);
		}
	}

	if (changed.length) {
		return {
			status: "stale",
			changed: Object.freeze([...new Set(changed)]),
			dependencies: Object.freeze(current),
			filesRead,
			bytesRead,
			durationMs: elapsed(startedAt),
		};
	}
	return {
		status: "valid",
		dependencies: Object.freeze(current),
		filesRead,
		bytesRead,
		durationMs: elapsed(startedAt),
	};
}

export async function captureFileDependency(
	physicalPath: string,
	logicalPath: string,
	role: Extract<DynamicDependency, { kind: "file" }>["role"] = "input",
	options: { readonly includeMetadata?: boolean; readonly maxFileBytes?: number } = {},
): Promise<{ readonly dependency: Extract<DynamicDependency, { kind: "file" }>; readonly bytesRead: number }> {
	const stat = await lstat(physicalPath);
	if (!stat.isFile()) throw new Error("not_regular_file");
	const maxBytes = finiteLimit(options.maxFileBytes ?? Number.POSITIVE_INFINITY);
	if (stat.size > maxBytes) throw new Error(`file_too_large:${stat.size}`);
	const content = await readFile(physicalPath);
	return {
		dependency: {
			kind: "file",
			path: logicalPath,
			role,
			contentDigest: sha256Digest(content),
			...(options.includeMetadata ? { metadataDigest: filesystemMetadataDigest(stat) } : {}),
		},
		bytesRead: content.byteLength,
	};
}

export async function captureDirectoryDependency(
	physicalPath: string,
	logicalPath: string,
	includeMetadata = false,
	excludedEntries: readonly string[] = [],
): Promise<Extract<DynamicDependency, { kind: "directory" }>> {
	const excluded = new Set(excludedEntries);
	const [entries, stat] = await Promise.all([
		readdir(physicalPath, { withFileTypes: true }),
		includeMetadata ? lstat(physicalPath) : undefined,
	]);
	const normalized = entries
		.filter((entry) => !excluded.has(entry.name))
		.map((entry) => `${filesystemEntryType(entry)}\0${entry.name}`)
		.sort();
	return {
		kind: "directory",
		path: logicalPath,
		entriesDigest: digestObject(normalized),
		...(stat ? { metadataDigest: filesystemMetadataDigest(stat) } : {}),
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
	const target = await readlink(physicalPath);
	return { kind: "symlink", path: logicalPath, target, targetDigest: sha256Digest(Buffer.from(target, "utf8")) };
}

function resolveEvidencePath(logicalPath: string, context: ProvenanceValidationContext): string | undefined {
	if (context.resolvePath) return context.resolvePath(logicalPath);
	return path.isAbsolute(logicalPath) ? path.resolve(logicalPath) : undefined;
}

function indeterminate(
	reason: string,
	startedAt: number,
	filesRead: number,
	bytesRead: number,
): ProvenanceValidation {
	return { status: "indeterminate", reason, filesRead, bytesRead, durationMs: elapsed(startedAt) };
}

function elapsed(startedAt: number): number {
	return Math.max(0, performance.now() - startedAt);
}

function finiteLimit(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : Number.POSITIVE_INFINITY;
}

function missing(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
