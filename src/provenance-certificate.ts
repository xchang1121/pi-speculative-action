import { createHash } from "node:crypto";
import { cloneSharedData, stableEqual, stableStringify } from "./stable-json.ts";

export const PROCESS_CERTIFICATE_VERSION = 7 as const;
export type Sha256Digest = `sha256:${string}`;

export interface FilesystemTypeEvidence {
	readonly isFile: () => boolean;
	readonly isDirectory: () => boolean;
	readonly isSymbolicLink: () => boolean;
	readonly isSocket: () => boolean;
	readonly isFIFO: () => boolean;
	readonly isCharacterDevice: () => boolean;
	readonly isBlockDevice: () => boolean;
}

export interface FilesystemMetadataEvidence {
	readonly mode: number | bigint;
	readonly uid: number | bigint;
	readonly gid: number | bigint;
	readonly size: number | bigint;
	readonly nlink: number | bigint;
	readonly isFile: () => boolean;
	readonly isDirectory: () => boolean;
	readonly isSymbolicLink: () => boolean;
}

const FILESYSTEM_OBSERVATION_FIELDS = [
	"dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size", "blksize", "blocks", "atimeNs", "mtimeNs", "ctimeNs",
] as const;
export type FilesystemObservationEvidence = { readonly [Field in typeof FILESYSTEM_OBSERVATION_FIELDS[number]]: bigint };

export interface ArtifactReference {
	readonly digest: Sha256Digest;
	readonly size: number;
}

export interface SemanticEnvironmentEntry {
	readonly name: string;
	readonly present: boolean;
	readonly valueDigest?: Sha256Digest;
}

export interface InheritedFileDescriptor {
	readonly fd: number;
	readonly type: "closed" | "regular" | "pipe" | "socket" | "tty" | "device" | "other";
	readonly flagsDigest: Sha256Digest;
	readonly endpointDigest?: Sha256Digest;
	readonly contentDigest?: Sha256Digest;
	readonly offset?: number;
	readonly eof?: boolean;
}

/** Static identity known immediately before one actual exec, independent of its parent shell. */
export interface ExecPrototype {
	readonly executablePath: string;
	readonly executableDigest: Sha256Digest;
	/** Digest of exact argv bytes; raw arguments are deliberately not persisted. */
	readonly argvDigest: Sha256Digest;
	readonly logicalCwd: string;
	/** Complete semantic environment; values are stored only as digests. */
	readonly environment: readonly SemanticEnvironmentEntry[];
	readonly environmentComplete: true;
	readonly umask: number;
	readonly processContextDigest: Sha256Digest;
	readonly stdin: {
		readonly type: "closed" | "bytes";
		readonly digest?: Sha256Digest;
		readonly eof: boolean;
	};
	readonly fileDescriptorTableComplete: true;
	readonly inheritedFDs: readonly InheritedFileDescriptor[];
	readonly platformFingerprint: string;
}

/** How the producer established evidence; deliberately excluded from semantic process identity. */
export interface ProcessProducerProof {
	readonly observer: {
		readonly provider: string;
		readonly fingerprint: Sha256Digest;
	};
	readonly execution:
		| { readonly authority: "actor" }
		| {
				readonly authority: "speculative";
				readonly confinement: {
					readonly provider: string;
					readonly fingerprint: Sha256Digest;
				};
		  };
}

export type DependencyRole = "input" | "executable" | "shared_object";

export type DynamicDependency =
	| {
			readonly kind: "file";
			readonly path: string;
			readonly role: DependencyRole;
			readonly contentDigest: Sha256Digest;
			readonly metadataDigest?: Sha256Digest;
	  }
	| {
			readonly kind: "directory";
			readonly path: string;
			readonly entriesDigest: Sha256Digest;
			readonly metadataDigest?: Sha256Digest;
			/** Backend-private names omitted from both capture and validation. */
			readonly excludedEntries?: readonly string[];
	  }
	| {
			readonly kind: "absence";
			readonly path: string;
			/** Optional parent enumeration observed at lookup time. */
			readonly parentEntriesDigest?: Sha256Digest;
			readonly parentExcludedEntries?: readonly string[];
	  }
	| {
			readonly kind: "symlink";
			readonly path: string;
			readonly target: string;
			readonly targetDigest: Sha256Digest;
	  }
	| {
			/** Exact successful stat(2) result; content equality alone cannot prove this observation. */
			readonly kind: "metadata";
			readonly path: string;
			readonly followSymlinks: boolean;
			readonly digest: Sha256Digest;
	  }
	| {
			readonly kind: "fd";
			readonly fd: number;
			readonly contentDigest: Sha256Digest;
			readonly eof: boolean;
	  };

export type ProvenanceTaint =
	| "network"
	| "ipc"
	| "clock"
	| "random"
	| "pid_observation"
	| "descriptor_observation"
	| "interactive_io"
	| "untracked_fd"
	| "confinement_observation"
	| "unsupported_syscall"
	| "escaped_sandbox"
	| "mutable_input"
	| "trace_incomplete";

export interface DynamicDependencyCertificate {
	readonly complete: boolean;
	readonly dependencies: readonly DynamicDependency[];
	readonly taints: readonly ProvenanceTaint[];
}

/** Exact state on one side of a replayable workspace transition. */
export type WorkspaceEffectState =
	| { readonly kind: "absent" }
	| { readonly kind: "file"; readonly data: ArtifactReference; readonly mode: number }
	| {
			readonly kind: "directory";
			readonly entriesDigest: Sha256Digest;
			readonly mode: number;
			readonly uid: number;
			readonly gid: number;
	  };

export type OrderedEffectEvent =
	| { readonly sequence: number; readonly kind: "output"; readonly fd: 1 | 2; readonly data: ArtifactReference }
	| {
			readonly sequence: number;
			readonly kind: "workspace";
			readonly path: string;
			readonly before: WorkspaceEffectState;
			readonly after: WorkspaceEffectState;
	  };

export type ExitOutcome =
	| { readonly kind: "code"; readonly code: number }
	| { readonly kind: "signal"; readonly signal: number; readonly coreDumped: boolean };

export interface ProcessResultRecord {
	readonly replayProfile: "buffered_noninteractive";
	/** Producer process wall time; observational only and never used to authorize replay. */
	readonly observedProcessMs?: number;
	/** Globally ordered output and filesystem effects. */
	readonly journal: readonly OrderedEffectEvent[];
	readonly exit: ExitOutcome;
}

/** Immutable completed-execution evidence indexed by WeakKey and validated into StrongKey. */
export interface ProcessProvenanceCertificate {
	readonly version: typeof PROCESS_CERTIFICATE_VERSION;
	readonly id: Sha256Digest;
	readonly weakKey: Sha256Digest;
	readonly strongKey: Sha256Digest;
	readonly prototype: ExecPrototype;
	readonly producer: ProcessProducerProof;
	readonly dependencyCertificate: DynamicDependencyCertificate;
	readonly result: ProcessResultRecord;
	readonly createdAt: number;
}

export interface ProcessPrototypeInput extends Omit<ExecPrototype, "argvDigest" | "environment" | "environmentComplete"> {
	readonly argv: readonly string[] | Uint8Array;
	readonly environment: Readonly<Record<string, string | undefined>>;
}

export function createExecPrototype(input: ProcessPrototypeInput): ExecPrototype {
	const { argv, environment: rawEnvironment, ...identity } = input;
	const argvDigest = sha256Digest(argv instanceof Uint8Array ? argv : Buffer.from(stableStringify(argv), "utf8"));
	return normalizePrototype({
		...identity, argvDigest, environmentComplete: true,
		environment: Object.entries(rawEnvironment).map(([name, value]): SemanticEnvironmentEntry =>
			value === undefined
				? { name, present: false }
				: { name, present: true, valueDigest: sha256Digest(Buffer.from(value, "utf8")) },
		),
	});
}

export function processWeakKey(prototype: ExecPrototype): Sha256Digest {
	return digestObject({ version: PROCESS_CERTIFICATE_VERSION, prototype: normalizePrototype(prototype) });
}

export function dependencyPathsetKey(certificate: DynamicDependencyCertificate): Sha256Digest {
	return digestObject(
		normalizeDependencies(certificate.dependencies).map((dependency) => {
			switch (dependency.kind) {
				case "file":
					return {
						kind: dependency.kind,
						path: dependency.path,
						role: dependency.role,
						metadata: dependency.metadataDigest !== undefined,
					};
				case "directory":
					return {
						kind: dependency.kind,
						path: dependency.path,
						metadata: dependency.metadataDigest !== undefined,
						excludedEntries: dependency.excludedEntries ?? [],
					};
				case "absence":
					return {
						kind: dependency.kind,
						path: dependency.path,
						captureParent: dependency.parentEntriesDigest !== undefined,
						parentExcludedEntries: dependency.parentExcludedEntries ?? [],
					};
				case "symlink":
					return { kind: dependency.kind, path: dependency.path };
				case "metadata":
					return { kind: dependency.kind, path: dependency.path, followSymlinks: dependency.followSymlinks };
				case "fd":
					return { kind: dependency.kind, fd: dependency.fd };
			}
		}),
	);
}

export function processStrongKey(
	weakKey: Sha256Digest,
	certificate: DynamicDependencyCertificate,
): Sha256Digest {
	return digestObject({ weakKey, dependencies: normalizeDependencies(certificate.dependencies) });
}

export function sealProcessCertificate(input: {
	readonly prototype: ExecPrototype;
	readonly producer: ProcessProducerProof;
	readonly dependencyCertificate: DynamicDependencyCertificate;
	readonly result: ProcessResultRecord;
	readonly createdAt?: number;
}): ProcessProvenanceCertificate {
	const prototype = normalizePrototype(input.prototype);
	const producer = normalizeProducerProof(input.producer);
	const evidence = input.dependencyCertificate;
	const dependencyCertificate: DynamicDependencyCertificate = deepFreeze({
		complete: evidence.complete === true,
		dependencies: normalizeDependencies(evidence.dependencies),
		taints: [...new Set(evidence.taints)].sort(),
	});
	const result = normalizeResult(input.result);
	// These records are already captured and validated; public key functions still normalize raw callers.
	const weakKey = digestObject({ version: PROCESS_CERTIFICATE_VERSION, prototype });
	const strongKey = digestObject({ weakKey, dependencies: dependencyCertificate.dependencies });
	const createdAt = finiteTimestamp(input.createdAt ?? Date.now());
	const body = {
		version: PROCESS_CERTIFICATE_VERSION,
		weakKey,
		strongKey,
		prototype,
		producer,
		dependencyCertificate,
		result,
		createdAt,
	};
	return deepFreeze({ ...body, id: certificateContentKey(body) });
}

export function parseProcessCertificate(value: unknown): ProcessProvenanceCertificate | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	const candidate = value as Partial<ProcessProvenanceCertificate>;
	if (
		candidate.version !== PROCESS_CERTIFICATE_VERSION ||
		!isSha256Digest(candidate.id) ||
		!candidate.prototype ||
		!candidate.producer ||
		!candidate.dependencyCertificate ||
		!candidate.result
	) {
		return undefined;
	}
	try {
		const sealed = sealProcessCertificate({
			prototype: candidate.prototype,
			producer: candidate.producer,
			dependencyCertificate: candidate.dependencyCertificate,
			result: candidate.result,
			createdAt: candidate.createdAt,
		});
		if (
			sealed.id !== candidate.id ||
			sealed.weakKey !== candidate.weakKey ||
			sealed.strongKey !== candidate.strongKey
		) {
			return undefined;
		}
		return sealed;
	} catch {
		return undefined;
	}
}

function certificateContentKey(
	certificate: Omit<ProcessProvenanceCertificate, "id">,
): Sha256Digest {
	const { createdAt: _createdAt, result, ...content } = certificate;
	// Observational timing must not split otherwise identical reusable results.
	const { observedProcessMs: _observedProcessMs, ...semanticResult } = result;
	return digestObject({ ...content, result: semanticResult });
}

export function referencedArtifacts(certificate: ProcessProvenanceCertificate): readonly ArtifactReference[] {
	const unique = new Map<Sha256Digest, ArtifactReference>();
	for (const event of certificate.result.journal) {
		if (event.kind === "output") unique.set(event.data.digest, event.data);
		else for (const state of [event.before, event.after]) {
			if (state.kind === "file") unique.set(state.data.digest, state.data);
		}
	}
	return [...unique.values()];
}

export function certificateReplayable(
	certificate: ProcessProvenanceCertificate,
	acceptedTaints: readonly ProvenanceTaint[] = [],
): boolean {
	const stdinReplayable =
		certificate.prototype.stdin.type === "closed" ||
		(certificate.prototype.stdin.eof && isSha256Digest(certificate.prototype.stdin.digest));
	const accepted = new Set(acceptedTaints);
	return (
		certificate.dependencyCertificate.complete &&
		certificate.dependencyCertificate.taints.every((taint) => accepted.has(taint)) &&
		certificate.prototype.environmentComplete &&
		certificate.prototype.fileDescriptorTableComplete &&
		stdinReplayable
	);
}

export function sha256Digest(value: string | Uint8Array): Sha256Digest {
	return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

export function digestObject(value: unknown): Sha256Digest {
	return sha256Digest(Buffer.from(stableStringify(value), "utf8"));
}

export function filesystemEntryType(entry: FilesystemTypeEvidence): string {
	return entry.isFile()
		? "file"
		: entry.isDirectory()
			? "directory"
			: entry.isSymbolicLink()
				? "symlink"
				: entry.isSocket()
					? "socket"
					: entry.isFIFO()
						? "fifo"
						: entry.isCharacterDevice()
							? "char"
							: entry.isBlockDevice()
								? "block"
								: "other";
}

export function filesystemMetadataDigest(stat: FilesystemMetadataEvidence): Sha256Digest {
	return digestObject({
		mode: Number(stat.mode),
		uid: Number(stat.uid),
		gid: Number(stat.gid),
		...(stat.isFile() ? { size: Number(stat.size), links: Number(stat.nlink) } : {}),
		type: stat.isFile() ? "file" : stat.isDirectory() ? "directory" : stat.isSymbolicLink() ? "symlink" : "other",
	});
}

export function filesystemObservationDigest(stat: FilesystemObservationEvidence): Sha256Digest {
	return digestObject(Object.fromEntries(FILESYSTEM_OBSERVATION_FIELDS.map((field) => [field, String(stat[field])])));
}

export function isSha256Digest(value: unknown): value is Sha256Digest {
	return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}

// Only this module's owned, validated records may skip capture, never arbitrary frozen inputs.
const normalizedPrototypes = new WeakSet<ExecPrototype>();

function normalizePrototype(input: ExecPrototype): ExecPrototype {
	if (normalizedPrototypes.has(input)) return input;
	const {
		executablePath, executableDigest, argvDigest, logicalCwd, platformFingerprint, umask, processContextDigest,
		environmentComplete, fileDescriptorTableComplete, environment: rawEnvironment, inheritedFDs: rawDescriptors, stdin: rawStdin,
	} = input;
	const stdin = cloneSharedData({ ...rawStdin });
	if (!environmentComplete) throw new Error("process prototype requires a complete environment");
	if (!fileDescriptorTableComplete) throw new Error("process prototype requires a complete descriptor table");
	if (!validLogicalPath(executablePath) || !validLogicalPath(logicalCwd) || typeof platformFingerprint !== "string" || !platformFingerprint) {
		throw new Error("process prototype identity is incomplete");
	}
	if (!Number.isSafeInteger(umask) || umask < 0 || umask > 0o777) {
		throw new Error("process prototype umask is invalid");
	}
	for (const digest of [executableDigest, argvDigest, processContextDigest]) {
		if (!isSha256Digest(digest)) throw new Error("process prototype contains an invalid digest");
	}
	const environmentNames = new Set<string>();
	const environment = [...rawEnvironment]
		.map((entry) => {
			const { name, present } = entry;
			const valueDigest = present ? entry.valueDigest : undefined;
			if (!name || name.includes("=") || name.includes("\0") || (present && !isSha256Digest(valueDigest))) {
				throw new Error("process prototype environment is incomplete");
			}
			if (environmentNames.has(name)) throw new Error(`duplicate semantic environment entry ${name}`);
			environmentNames.add(name);
			return present ? { name, present: true as const, valueDigest: valueDigest! } : { name, present: false as const };
		})
		.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
	const descriptors = new Set<number>();
	const inheritedFDs = [...rawDescriptors]
		.map((source) => {
			const fd = cloneSharedData({ ...source });
			if (!Number.isSafeInteger(fd.fd) || fd.fd < 0 || descriptors.has(fd.fd) || !isSha256Digest(fd.flagsDigest)) {
				throw new Error("process prototype descriptor table is invalid");
			}
			descriptors.add(fd.fd);
			for (const digest of [fd.endpointDigest, fd.contentDigest]) {
				if (digest !== undefined && !isSha256Digest(digest)) throw new Error("process descriptor digest is invalid");
			}
			return fd;
		})
		.sort((left, right) => left.fd - right.fd);
	if (
		!rawStdin ||
		(stdin.type === "bytes" && !isSha256Digest(stdin.digest)) ||
		(stdin.digest !== undefined && !isSha256Digest(stdin.digest))
	) {
		throw new Error("process stdin identity is invalid");
	}
	const prototype: ExecPrototype = deepFreeze({
		executablePath,
		executableDigest,
		argvDigest,
		logicalCwd,
		environment,
		environmentComplete: true,
		umask,
		processContextDigest,
		stdin,
		fileDescriptorTableComplete: true,
		inheritedFDs,
		platformFingerprint,
	});
	normalizedPrototypes.add(prototype);
	return prototype;
}

function normalizeProducerProof(proof: ProcessProducerProof): ProcessProducerProof {
	if (
		!proof ||
		!validProvider(proof.observer?.provider) ||
		!isSha256Digest(proof.observer?.fingerprint) ||
		(proof.execution?.authority !== "actor" && proof.execution?.authority !== "speculative")
	) {
		throw new Error("process producer proof is incomplete");
	}
	if (proof.execution.authority === "actor") {
		return deepFreeze({ observer: { ...proof.observer }, execution: { authority: "actor" } });
	}
	if (
		!validProvider(proof.execution.confinement?.provider) ||
		!isSha256Digest(proof.execution.confinement?.fingerprint)
	) {
		throw new Error("speculative process producer requires confinement proof");
	}
	return deepFreeze({
		observer: { ...proof.observer },
		execution: {
			authority: "speculative",
			confinement: { ...proof.execution.confinement },
		},
	});
}

function validProvider(value: unknown): value is string {
	return typeof value === "string" && value.length > 0 && !value.includes("\0");
}

function normalizeDependencies(dependencies: readonly DynamicDependency[]): DynamicDependency[] {
	const seen = new Map<string, DynamicDependency>();
	for (const source of dependencies) {
		const dependency = { ...source };
		if (dependency.kind === "directory" && Array.isArray(dependency.excludedEntries)) {
			dependency.excludedEntries = Object.freeze([...new Set(dependency.excludedEntries)].sort());
		}
		if (dependency.kind === "absence" && Array.isArray(dependency.parentExcludedEntries)) {
			dependency.parentExcludedEntries = Object.freeze([...new Set(dependency.parentExcludedEntries)].sort());
		}
		validateDependency(dependency);
		const identity = dynamicDependencyIdentity(dependency);
		const existing = seen.get(identity);
		if (existing !== undefined && !stableEqual(existing, dependency)) throw new Error(`conflicting dependency evidence for ${identity}`);
		seen.set(identity, dependency);
	}
	return [...seen.keys()].sort().map((identity) => seen.get(identity)!);
}

export function dynamicDependencyIdentity(dependency: DynamicDependency): string {
	if (dependency.kind === "fd") return `fd:${dependency.fd}`;
	return dependency.kind === "metadata"
		? `${dependency.kind}:${dependency.followSymlinks ? "follow" : "nofollow"}:${dependency.path}`
		: `${dependency.kind}:${dependency.path}`;
}

function validateDependency(dependency: DynamicDependency): void {
	if (!dependency || typeof dependency !== "object") throw new Error("invalid dynamic dependency");
	if (dependency.kind === "fd") {
		if (!Number.isSafeInteger(dependency.fd) || dependency.fd < 0 || !isSha256Digest(dependency.contentDigest)) {
			throw new Error("invalid descriptor dependency");
		}
		return;
	}
	if (!validLogicalPath(dependency.path)) throw new Error("invalid dependency path");
	switch (dependency.kind) {
		case "file":
			if (
				!["input", "executable", "shared_object"].includes(dependency.role) ||
				!isSha256Digest(dependency.contentDigest) ||
				(dependency.metadataDigest !== undefined && !isSha256Digest(dependency.metadataDigest))
			) {
				throw new Error("invalid file dependency");
			}
			break;
		case "directory":
			if (
				!isSha256Digest(dependency.entriesDigest) ||
				(dependency.metadataDigest !== undefined && !isSha256Digest(dependency.metadataDigest)) ||
				!validExcludedEntries(dependency.excludedEntries)
			) {
				throw new Error("invalid directory dependency");
			}
			break;
		case "absence":
			if (
				(dependency.parentEntriesDigest !== undefined && !isSha256Digest(dependency.parentEntriesDigest)) ||
				!validExcludedEntries(dependency.parentExcludedEntries)
			) {
				throw new Error("invalid negative dependency");
			}
			break;
		case "symlink":
			if (!dependency.target || dependency.target.includes("\0") || !isSha256Digest(dependency.targetDigest)) {
				throw new Error("invalid symlink dependency");
			}
			break;
		case "metadata":
			if (typeof dependency.followSymlinks !== "boolean" || !isSha256Digest(dependency.digest)) {
				throw new Error("invalid metadata dependency");
			}
			break;
		default:
			throw new Error("invalid dynamic dependency kind");
	}
}

function validExcludedEntries(entries: readonly string[] | undefined): boolean {
	return (
		entries === undefined ||
		(Array.isArray(entries) &&
			entries.every(
				(entry) => typeof entry === "string" && entry.length > 0 && entry !== "." && entry !== ".." && !entry.includes("/") && !entry.includes("\0"),
			))
	);
}

function normalizeResult(result: ProcessResultRecord): ProcessResultRecord {
	if (result.replayProfile !== "buffered_noninteractive") throw new Error("unsupported replay profile");
	if (
		result.observedProcessMs !== undefined &&
		(!Number.isFinite(result.observedProcessMs) || result.observedProcessMs < 0)
	) {
		throw new Error("invalid observed process duration");
	}
	const journal = [...result.journal]
		.map((event) => ({ ...event }))
		.sort((left, right) => left.sequence - right.sequence);
	const artifactSizes = new Map<Sha256Digest, number>();
	for (let index = 0; index < journal.length; index++) {
		const event = journal[index]!;
		if (!Number.isSafeInteger(event.sequence) || event.sequence < 0) throw new Error("invalid effect sequence");
		if (index > 0 && event.sequence === journal[index - 1]!.sequence) throw new Error("duplicate effect sequence");
		if (event.kind === "output") validateArtifact(event.data, artifactSizes);
		else {
			if (!validLogicalPath(event.path)) throw new Error("invalid effect path");
			const before = normalizeWorkspaceEffectState(event.before, artifactSizes);
			const after = normalizeWorkspaceEffectState(event.after, artifactSizes);
			if (before.kind === after.kind && stableStringify(before) === stableStringify(after)) {
				throw new Error("workspace effect does not change state");
			}
			journal[index] = { ...event, before, after };
		}
	}
	return deepFreeze({
		replayProfile: result.replayProfile,
		...(result.observedProcessMs !== undefined ? { observedProcessMs: result.observedProcessMs } : {}),
		journal,
		exit: { ...result.exit },
	});
}

function normalizeWorkspaceEffectState(
	state: WorkspaceEffectState,
	artifactSizes: Map<Sha256Digest, number>,
): WorkspaceEffectState {
	if (state.kind === "absent") return { kind: "absent" };
	if (!Number.isSafeInteger(state.mode) || state.mode < 0 || state.mode > 0o777) {
		throw new Error("invalid workspace effect mode");
	}
	if (state.kind === "file") {
		validateArtifact(state.data, artifactSizes);
		return { ...state, data: { ...state.data } };
	}
	if (
		!isSha256Digest(state.entriesDigest) ||
		!Number.isSafeInteger(state.uid) ||
		state.uid < 0 ||
		!Number.isSafeInteger(state.gid) ||
		state.gid < 0
	) {
		throw new Error("invalid directory effect state");
	}
	return { ...state };
}

function validateArtifact(reference: ArtifactReference, sizes: Map<Sha256Digest, number>): void {
	if (!reference || !isSha256Digest(reference.digest) || !Number.isSafeInteger(reference.size) || reference.size < 0) {
		throw new Error("invalid effect artifact");
	}
	const previousSize = sizes.get(reference.digest);
	if (previousSize !== undefined && previousSize !== reference.size) {
		throw new Error("conflicting effect artifact sizes");
	}
	sizes.set(reference.digest, reference.size);
}

function validLogicalPath(value: string): boolean {
	return typeof value === "string" && value.startsWith("/") && !value.includes("\0");
}

function finiteTimestamp(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

function deepFreeze<Value>(value: Value): Value {
	if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
	for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child);
	return Object.freeze(value);
}
