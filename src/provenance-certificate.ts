import { createHash } from "node:crypto";
import { stableStringify } from "./stable-json.ts";

export const PROCESS_CERTIFICATE_VERSION = 2 as const;
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
	readonly mode: number;
	readonly uid: number;
	readonly gid: number;
	readonly size: number;
	readonly nlink: number;
	readonly isFile: () => boolean;
	readonly isDirectory: () => boolean;
	readonly isSymbolicLink: () => boolean;
}

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
	readonly rlimitsDigest: Sha256Digest;
	readonly signalDispositionsDigest: Sha256Digest;
	readonly credentialsDigest: Sha256Digest;
	readonly schedulingDigest: Sha256Digest;
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

export type DependencyRole = "input" | "executable" | "shared_object" | "metadata";

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
	| "interactive_io"
	| "untracked_fd"
	| "unsupported_syscall"
	| "escaped_sandbox"
	| "mutable_input"
	| "trace_incomplete";

export interface DynamicDependencyCertificate {
	readonly complete: boolean;
	readonly dependencies: readonly DynamicDependency[];
	readonly taints: readonly ProvenanceTaint[];
}

export type OrderedEffectEvent =
	| { readonly sequence: number; readonly kind: "output"; readonly fd: 1 | 2; readonly data: ArtifactReference }
	| {
			readonly sequence: number;
			readonly kind: "write";
			readonly path: string;
			readonly data: ArtifactReference;
			readonly mode: number;
	  }
	| { readonly sequence: number; readonly kind: "delete"; readonly path: string }
	| {
			readonly sequence: number;
			readonly kind: "mkdir";
			readonly path: string;
			readonly entriesDigest: Sha256Digest;
			readonly mode: number;
			readonly uid: number;
			readonly gid: number;
	  }
	| { readonly sequence: number; readonly kind: "rmdir"; readonly path: string }
	| { readonly sequence: number; readonly kind: "rename"; readonly from: string; readonly to: string }
	| { readonly sequence: number; readonly kind: "symlink"; readonly path: string; readonly target: string }
	| { readonly sequence: number; readonly kind: "hardlink"; readonly from: string; readonly to: string };

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
	const argvBytes = input.argv instanceof Uint8Array ? input.argv : Buffer.from(stableStringify(input.argv), "utf8");
	const { argv: _argv, environment: rawEnvironment, ...identity } = input;
	const environment = Object.entries(rawEnvironment)
		.map(([name, value]): SemanticEnvironmentEntry =>
			value === undefined
				? { name, present: false }
				: { name, present: true, valueDigest: sha256Digest(Buffer.from(value, "utf8")) },
		)
		.sort((left, right) => left.name.localeCompare(right.name));
	return deepFreeze({
		...identity,
		argvDigest: sha256Digest(argvBytes),
		environment,
		environmentComplete: true as const,
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
	const dependencyCertificate = normalizeDependencyCertificate(input.dependencyCertificate);
	const result = normalizeResult(input.result);
	const weakKey = processWeakKey(prototype);
	const strongKey = processStrongKey(weakKey, dependencyCertificate);
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
		const { id: _id, ...legacyBody } = sealed;
		const legacyID = digestObject(legacyBody);
		if (
			(sealed.id !== candidate.id && legacyID !== candidate.id) ||
			sealed.weakKey !== candidate.weakKey ||
			sealed.strongKey !== candidate.strongKey
		) {
			return undefined;
		}
		return sealed.id === candidate.id ? sealed : deepFreeze({ ...sealed, id: candidate.id });
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
		if (event.kind === "output" || event.kind === "write") unique.set(event.data.digest, event.data);
	}
	return [...unique.values()];
}

export function certificateReplayable(certificate: ProcessProvenanceCertificate): boolean {
	const stdinReplayable =
		certificate.prototype.stdin.type === "closed" ||
		(certificate.prototype.stdin.eof && isSha256Digest(certificate.prototype.stdin.digest));
	return (
		certificate.dependencyCertificate.complete &&
		certificate.dependencyCertificate.taints.length === 0 &&
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
		mode: stat.mode,
		uid: stat.uid,
		gid: stat.gid,
		...(stat.isFile() ? { size: stat.size, links: stat.nlink } : {}),
		type: stat.isFile() ? "file" : stat.isDirectory() ? "directory" : stat.isSymbolicLink() ? "symlink" : "other",
	});
}

export function isSha256Digest(value: unknown): value is Sha256Digest {
	return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}

function normalizePrototype(prototype: ExecPrototype): ExecPrototype {
	if (!prototype.environmentComplete) throw new Error("process prototype requires a complete environment");
	if (!prototype.fileDescriptorTableComplete) throw new Error("process prototype requires a complete descriptor table");
	if (
		!validLogicalPath(prototype.executablePath) ||
		!validLogicalPath(prototype.logicalCwd) ||
		!prototype.platformFingerprint
	) {
		throw new Error("process prototype identity is incomplete");
	}
	if (!Number.isSafeInteger(prototype.umask) || prototype.umask < 0 || prototype.umask > 0o777) {
		throw new Error("process prototype umask is invalid");
	}
	for (const digest of [
		prototype.executableDigest,
		prototype.argvDigest,
		prototype.rlimitsDigest,
		prototype.signalDispositionsDigest,
		prototype.credentialsDigest,
		prototype.schedulingDigest,
	]) {
		if (!isSha256Digest(digest)) throw new Error("process prototype contains an invalid digest");
	}
	const environmentNames = new Set<string>();
	const environment = [...prototype.environment]
		.map((entry) => {
			if (!entry.name || entry.name.includes("=") || entry.name.includes("\0") || (entry.present && !isSha256Digest(entry.valueDigest))) {
				throw new Error("process prototype environment is incomplete");
			}
			if (environmentNames.has(entry.name)) throw new Error(`duplicate semantic environment entry ${entry.name}`);
			environmentNames.add(entry.name);
			return entry.present
				? { name: entry.name, present: true as const, valueDigest: entry.valueDigest! }
				: { name: entry.name, present: false as const };
		})
		.sort((left, right) => left.name.localeCompare(right.name));
	const descriptors = new Set<number>();
	const inheritedFDs = [...prototype.inheritedFDs]
		.map((fd) => {
			if (!Number.isSafeInteger(fd.fd) || fd.fd < 0 || descriptors.has(fd.fd) || !isSha256Digest(fd.flagsDigest)) {
				throw new Error("process prototype descriptor table is invalid");
			}
			descriptors.add(fd.fd);
			for (const digest of [fd.endpointDigest, fd.contentDigest]) {
				if (digest !== undefined && !isSha256Digest(digest)) throw new Error("process descriptor digest is invalid");
			}
			return { ...fd };
		})
		.sort((left, right) => left.fd - right.fd);
	if (
		(prototype.stdin.type === "bytes" && !isSha256Digest(prototype.stdin.digest)) ||
		(prototype.stdin.digest !== undefined && !isSha256Digest(prototype.stdin.digest))
	) {
		throw new Error("process stdin identity is invalid");
	}
	return deepFreeze({
		executablePath: prototype.executablePath,
		executableDigest: prototype.executableDigest,
		argvDigest: prototype.argvDigest,
		logicalCwd: prototype.logicalCwd,
		environment,
		environmentComplete: true,
		umask: prototype.umask,
		rlimitsDigest: prototype.rlimitsDigest,
		signalDispositionsDigest: prototype.signalDispositionsDigest,
		credentialsDigest: prototype.credentialsDigest,
		schedulingDigest: prototype.schedulingDigest,
		stdin: { ...prototype.stdin },
		fileDescriptorTableComplete: true,
		inheritedFDs,
		platformFingerprint: prototype.platformFingerprint,
	});
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

function normalizeDependencyCertificate(certificate: DynamicDependencyCertificate): DynamicDependencyCertificate {
	return deepFreeze({
		complete: certificate.complete === true,
		dependencies: normalizeDependencies(certificate.dependencies),
		taints: [...new Set(certificate.taints)].sort(),
	});
}

function normalizeDependencies(dependencies: readonly DynamicDependency[]): DynamicDependency[] {
	const normalized = dependencies.map((dependency) => {
		validateDependency(dependency);
		if (dependency.kind === "directory" && dependency.excludedEntries) {
			return { ...dependency, excludedEntries: Object.freeze([...new Set(dependency.excludedEntries)].sort()) };
		}
		if (dependency.kind === "absence" && dependency.parentExcludedEntries) {
			return {
				...dependency,
				parentExcludedEntries: Object.freeze([...new Set(dependency.parentExcludedEntries)].sort()),
			};
		}
		return { ...dependency };
	});
	normalized.sort((left, right) => dependencyIdentity(left).localeCompare(dependencyIdentity(right)));
	const seen = new Map<string, string>();
	for (const dependency of normalized) {
		const identity = dependencyIdentity(dependency);
		const encoded = stableStringify(dependency);
		const existing = seen.get(identity);
		if (existing !== undefined && existing !== encoded) throw new Error(`conflicting dependency evidence for ${identity}`);
		seen.set(identity, encoded);
	}
	return normalized.filter((dependency, index) => index === 0 || dependencyIdentity(dependency) !== dependencyIdentity(normalized[index - 1]!));
}

function dependencyIdentity(dependency: DynamicDependency): string {
	return dependency.kind === "fd"
		? `fd:${dependency.fd}`
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
				!["input", "executable", "shared_object", "metadata"].includes(dependency.role) ||
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
		if ((event.kind === "output" || event.kind === "write") && !isSha256Digest(event.data.digest)) {
			throw new Error("invalid effect artifact");
		}
		if (
			(event.kind === "output" || event.kind === "write") &&
			(!Number.isSafeInteger(event.data.size) || event.data.size < 0)
		) {
			throw new Error("invalid effect artifact size");
		}
		if (event.kind === "output" || event.kind === "write") {
			const previousSize = artifactSizes.get(event.data.digest);
			if (previousSize !== undefined && previousSize !== event.data.size) {
				throw new Error("conflicting effect artifact sizes");
			}
			artifactSizes.set(event.data.digest, event.data.size);
		}
		if (event.kind === "write" && (!Number.isSafeInteger(event.mode) || event.mode < 0 || event.mode > 0o777)) {
			throw new Error("invalid write effect mode");
		}
		if (
			event.kind === "mkdir" &&
			(!isSha256Digest(event.entriesDigest) ||
				!Number.isSafeInteger(event.mode) ||
				event.mode < 0 ||
				event.mode > 0o777 ||
				!Number.isSafeInteger(event.uid) ||
				event.uid < 0 ||
				!Number.isSafeInteger(event.gid) ||
				event.gid < 0)
		) {
			throw new Error("invalid mkdir effect state");
		}
		for (const effectPath of effectPaths(event)) {
			if (!validLogicalPath(effectPath)) throw new Error("invalid effect path");
		}
	}
	return deepFreeze({
		replayProfile: result.replayProfile,
		...(result.observedProcessMs !== undefined ? { observedProcessMs: result.observedProcessMs } : {}),
		journal,
		exit: { ...result.exit },
	});
}

function effectPaths(event: OrderedEffectEvent): readonly string[] {
	switch (event.kind) {
		case "output":
			return [];
		case "rename":
		case "hardlink":
			return [event.from, event.to];
		default:
			return [event.path];
	}
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
