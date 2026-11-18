import { readFile } from "node:fs/promises";
import path from "node:path";
import { speculativeConfig } from "./config-schema.ts";
import type { ExecutionOperationAdoption, ExecutionScope } from "./execution-world.ts";
import { EffectCommitFailure, effectCommitFailure, snapshotExecutionScope } from "./execution-world.ts";
import type { FilesystemObservationField } from "./filesystem-evidence.ts";
import {
	captureFileDigest,
	captureFilesystemEntry,
	filesystemMetadataDigest,
	filesystemObservationDigest,
	FilesystemReadView,
	mapFilesystem,
	sameFilesystemIdentity,
	sharedWalk,
	withFilesystemReadView,
} from "./filesystem-evidence.ts";
import type { Sha256Digest } from "./primitives.ts";
import {
	errorMessage,
	releaseOnce,
	immutableSnapshot,
	isImmutableSnapshot,
	isSha256Digest,
	isMissing as missing,
	sha256Digest,
	stableEqual,
} from "./primitives.ts";
import type {
	DependencyRole,
	DynamicDependency,
	DynamicDependencyCertificate,
	ProcessCertificateInput,
	ProcessProducerProof,
	ProcessProvenanceCertificate,
	ProvenanceTaint,
} from "./provenance-certificate.ts";
import {
	certificateReplayable,
	dependencyPathsetKey,
	processStrongKey,
	referencedArtifacts,
	sealProcessCertificate,
} from "./provenance-certificate.ts";
import type { ArtifactSource, VerifiedArtifactClosure } from "./reuse-store.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import { TimelineInterval } from "./task-timing.ts";
import { directoryEntriesDigest } from "./workspace-sandbox.ts";
export interface ProvenanceValidationContext {
	readonly signal?: AbortSignal;
	/** Map a stable logical path from the certificate into the current physical execution world. */
	readonly resolvePath?: (logicalPath: string) => string | undefined;
	readonly maxFileBytes?: number;
	/** Borrow only a view owned by the current execution world; other paths use this validation's source view. */
	readonly readView?: FilesystemReadView | ((physicalPath: string) => FilesystemReadView | undefined);
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
) & {
	readonly filesRead: number;
	readonly bytesRead: number;
};
type ValidatedProcessInputs = Extract<DynamicDependencyValidation, { status: "valid" }> & {
	readonly strongKey: Sha256Digest;
};
export type ProvenanceValidation = ValidatedProcessInputs | Exclude<DynamicDependencyValidation, { status: "valid" }>;
/** One lookup, one projection: share current observations, never a previous certificate's validity or a previous lookup's inputs. */
export function observeProcessInputs(
	weakKey: Sha256Digest,
	capture: (evidence: DynamicDependencyCertificate) => Promise<DynamicDependencyValidation | undefined>,
) {
	const observe = sharedWalk(
		(evidence: DynamicDependencyCertificate) =>
			capture(evidence).then((result) =>
				result && result.status !== "indeterminate"
					? {
							status: "valid" as const,
							strongKey: processStrongKey(weakKey, {
								complete: true,
								dependencies: result.dependencies,
								taints: [],
							}),
							dependencies: result.dependencies,
							filesRead: result.filesRead,
							bytesRead: result.bytesRead,
						}
					: undefined,
			),
		(evidence) => `${evidence.taints.join(",")}:${dependencyPathsetKey(evidence)}`,
	);
	return (evidence: DynamicDependencyCertificate) =>
		evidence.complete ? observe(evidence) : Promise.resolve(undefined);
}
type DependencyField = {
	[Kind in DynamicDependency["kind"]]: keyof DependencyOf<Kind>;
}[DynamicDependency["kind"]];
const OBSERVATION_FIELDS = [
	"contentDigest",
	"metadataDigest",
	"aliases",
	"entriesDigest",
	"parentEntriesDigest",
	"targetDigest",
	"target",
	"digest",
	"exclusive",
] satisfies readonly DependencyField[];
/** Validate reusable dynamic evidence without requiring a persisted process result. */
export async function validateDynamicDependencyCertificate(
	certificate: DynamicDependencyCertificate,
	context: ProvenanceValidationContext = {},
): Promise<DynamicDependencyValidation> {
	context.signal?.throwIfAborted();
	let filesRead = 0;
	let bytesRead = 0;
	const metrics = () => ({ filesRead, bytesRead });
	const indeterminate = (reason: string): DynamicDependencyValidation => ({
		status: "indeterminate",
		reason,
		...metrics(),
	});
	if (!certificate.complete) return indeterminate("trace_incomplete");
	const acceptedTaints = new Set(context.acceptedTaints ?? []);
	const blockingTaints = certificate.taints.filter((taint) => !acceptedTaints.has(taint));
	if (blockingTaints.length) return indeterminate(`tainted:${blockingTaints.join(",")}`);
	const borrowed = typeof context.readView === "function" ? undefined : context.readView;
	return withFilesystemReadView(
		certificate.dependencies.length ? borrowed : FilesystemReadView.ambient(),
		async (source): Promise<DynamicDependencyValidation> => {
			const current: DynamicDependency[] = [];
			const changed: string[] = [];
			// Independent observations: checked concurrently, settled in certificate order (the strong key hashes that order).
			const expectations = structuredClone(certificate.dependencies);
			// Names missing from one directory, or its own entries, share one listing of it within a validation.
			const readView = (target: string) =>
				(typeof context.readView === "function" ? context.readView(target) : borrowed) ?? source;
			const list = sharedWalk((target) => captureFilesystemEntry(target, "directory", readView(target)));
			const checks = await mapFilesystem(
				expectations,
				async (expected): Promise<DynamicDependency | string | undefined> => {
					try {
						const physicalPath = context.resolvePath
							? context.resolvePath(expected.path)
							: path.isAbsolute(expected.path)
								? path.resolve(expected.path)
								: undefined;
						if (!physicalPath) return `path_unmapped:${expected.path}`;
						const view = readView(physicalPath);
						switch (expected.kind) {
							case "file": {
								const captured = await captureFileDependency(physicalPath, expected.path, expected.role, {
									includeMetadata: expected.metadataDigest !== undefined,
									maxFileBytes: context.maxFileBytes,
									aliases: expected.aliases,
									resolvePath: context.resolvePath,
									signal: context.signal,
									view,
								});
								filesRead += captured.filesRead;
								bytesRead += captured.bytesRead;
								return captured.dependency;
							}
							case "directory":
								return await captureDirectoryDependency(
									physicalPath,
									expected.path,
									expected.metadataDigest !== undefined,
									expected.excludedEntries,
									expected.entriesDigest !== undefined,
									list,
									view,
								);
							case "absence":
								return await captureAbsenceDependency(
									physicalPath,
									expected.path,
									expected.parentEntriesDigest !== undefined,
									expected.parentExcludedEntries,
									list,
									view,
								);
							case "symlink":
								return await captureSymlinkDependency(physicalPath, expected.path, view);
							case "metadata":
								return await captureMetadataDependency(
									physicalPath,
									expected.path,
									expected.followSymlinks,
									expected.fields,
									view,
								);
							case "lock":
								return await probeLock(physicalPath, expected.path, expected.exclusive, view);
						}
					} catch (error) {
						context.signal?.throwIfAborted();
						if (missing(error)) return undefined;
						return `validation_error:${expected.path}:${errorMessage(error)}`;
					}
				},
				context.signal,
			);
			for (const [index, observed] of checks.entries()) {
				if (typeof observed === "string") return indeterminate(observed);
				const expected = expectations[index]!;
				if (observed) current.push(observed);
				if (
					!observed ||
					OBSERVATION_FIELDS.some((field) => Reflect.get(observed, field) !== Reflect.get(expected, field))
				) {
					changed.push(expected.path);
				}
			}
			await borrowed?.verify();
			return {
				...(changed.length
					? { status: "stale", changed: Object.freeze([...new Set(changed)]) }
					: { status: "valid" }),
				dependencies: Object.freeze(current),
				...metrics(),
			};
		},
	).catch((error) => {
		context.signal?.throwIfAborted();
		return indeterminate(`validation_error:filesystem_view:${errorMessage(error)}`);
	});
}
/** A lock still holds as a dependency while no one holds one it would conflict with: /proc/locks lists every flock, record and
 * open-file lock by device and inode. A missing file is gone, not made. */
type DependencyOf<Kind extends DynamicDependency["kind"]> = Extract<DynamicDependency, { kind: Kind }>;
async function probeLock(
	physicalPath: string,
	logicalPath: string,
	exclusive: boolean,
	view: FilesystemReadView,
): Promise<DependencyOf<"lock"> | undefined> {
	const info = await view.stat(physicalPath);
	const { dev } = info;
	const hex = (value: bigint) => value.toString(16).padStart(2, "0");
	const object = `${hex(((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn))}:${hex((dev & 0xffn) | ((dev >> 12n) & ~0xffn))}:${info.ino}`;
	const held = (await readFile("/proc/locks", "utf8")).split("\n").some((line) => {
		const [, access, locked] = /^\d+:\s+\w+\s+\w+\s+(READ|WRITE)\s+\S+\s+(\S+)\s/.exec(line) ?? [];
		return locked === object && (exclusive || access === "WRITE");
	});
	return info.isFile() && !held ? { kind: "lock", path: logicalPath, exclusive } : undefined;
}
export async function captureMetadataDependency(
	physicalPath: string,
	logicalPath: string,
	followSymlinks: boolean,
	fields?: readonly FilesystemObservationField[],
	view?: FilesystemReadView,
): Promise<DependencyOf<"metadata">> {
	return withFilesystemReadView(view, async (view) => {
		const observed = await view.stat(physicalPath, followSymlinks);
		return {
			kind: "metadata",
			path: logicalPath,
			followSymlinks,
			digest: filesystemObservationDigest(observed, fields),
			...(fields ? { fields } : {}),
		};
	});
}
export async function captureFileDependency(
	physicalPath: string,
	logicalPath: string,
	role: DependencyRole = "input",
	options: {
		readonly includeMetadata?: boolean;
		readonly maxFileBytes?: number;
		readonly aliases?: readonly string[];
		readonly signal?: AbortSignal;
		readonly resolvePath?: ProvenanceValidationContext["resolvePath"];
		readonly view?: FilesystemReadView;
	} = {},
): Promise<{
	readonly dependency: DependencyOf<"file">;
	readonly bytesRead: number;
	readonly filesRead: number;
}> {
	return withFilesystemReadView(options.view, async (view) => {
		const limit = options.maxFileBytes ?? Infinity;
		const maxBytes = Number.isFinite(limit) ? Math.max(0, limit) : Infinity;
		const content = await captureFileDigest(physicalPath, maxBytes, options.signal, true, view);
		let aliases = options.aliases;
		if (aliases) {
			const states = await mapFilesystem(
				aliases,
				async (logical) => {
					const physical = options.resolvePath ? options.resolvePath(logical) : path.resolve(logical);
					if (!physical) throw new Error(`alias_unmapped:${logical}`);
					return view.stat(physical);
				},
				options.signal,
			);
			if (
				content.stat.nlink !== BigInt(aliases.length) ||
				states.some((state) => !state.isFile() || !sameFilesystemIdentity(content.stat, state))
			)
				aliases = undefined; // Changed topology: a plain file entry.
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
	});
}
export async function captureDirectoryDependency(
	physicalPath: string,
	logicalPath: string,
	includeMetadata = false,
	excludedEntries: readonly string[] = [],
	listed = true,
	list?: (target: string) => ReturnType<typeof captureFilesystemEntry>,
	view?: FilesystemReadView,
): Promise<DependencyOf<"directory">> {
	return withFilesystemReadView(view, async (view) => {
		const { info, entries } = await (listed && list
			? list(physicalPath)
			: captureFilesystemEntry(physicalPath, listed ? "directory" : undefined, view));
		if (!info.isDirectory() || (listed && !entries)) throw new Error("not_directory");
		const excluded = new Set(excludedEntries);
		return {
			kind: "directory",
			path: logicalPath,
			...(listed
				? {
						entriesDigest: directoryEntriesDigest(entries!.filter((entry) => !excluded.has(entry.name))),
					}
				: {}),
			...(includeMetadata || !listed ? { metadataDigest: filesystemMetadataDigest(info) } : {}),
			...(listed && excluded.size ? { excludedEntries: Object.freeze([...excluded].sort()) } : {}),
		};
	});
}
export async function captureAbsenceDependency(
	physicalPath: string,
	logicalPath: string,
	captureParent = true,
	parentExcludedEntries: readonly string[] = [],
	list?: Parameters<typeof captureDirectoryDependency>[5],
	view?: FilesystemReadView,
): Promise<DependencyOf<"absence"> | undefined> {
	return withFilesystemReadView(view, async (view) => {
		try {
			await view.stat(physicalPath);
			return undefined;
		} catch (error) {
			if (!missing(error)) throw error;
		}
		const parent = captureParent
			? await captureDirectoryDependency(
					path.dirname(physicalPath),
					path.posix.dirname(logicalPath.replaceAll("\\", "/")),
					false,
					parentExcludedEntries,
					true,
					list,
					view,
				)
			: undefined;
		return {
			kind: "absence",
			path: logicalPath,
			...(parent
				? {
						parentEntriesDigest: parent.entriesDigest,
						...(parent.excludedEntries ? { parentExcludedEntries: parent.excludedEntries } : {}),
					}
				: {}),
		};
	});
}
export async function captureSymlinkDependency(
	physicalPath: string,
	logicalPath: string,
	view?: FilesystemReadView,
): Promise<DependencyOf<"symlink">> {
	return withFilesystemReadView(view, async (view) => {
		const { link: target } = await captureFilesystemEntry(physicalPath, undefined, view);
		if (target === undefined) throw new Error("not_symlink");
		return {
			kind: "symlink",
			path: logicalPath,
			target,
			targetDigest: sha256Digest(target),
		};
	});
}
export interface ReplayObservationContract {
	readonly sink: "buffered" | "pipe" | "tty" | "interactive";
	readonly orderedJournal: boolean;
	readonly transactionalEffects: boolean;
}
export interface ProcessReuseRequest {
	readonly signal?: AbortSignal;
	/** Static exec identity already derived from the caller's bound invocation; not replay authority. */
	readonly weakKey: Sha256Digest;
	/** Coarse index partition; the full weak key and current evidence still authorize reuse. */
	readonly executablePath: string;
	readonly contract: ReplayObservationContract;
	readonly validation?: ProvenanceValidationContext;
	/** Optional host policy for accepting proof produced under a different execution authority. */
	readonly acceptProducer?: (proof: ProcessProducerProof) => boolean;
	/** Already attempted certificate identities; skipping them grants no replay authority. */
	readonly excludedCertificates?: ReadonlySet<Sha256Digest>;
	/** Sealed handoff candidates; accepting tainted inputs requires a same-scope transfer. */
	readonly live?: {
		readonly certificate: ProcessProvenanceCertificate | readonly ProcessProvenanceCertificate[];
		readonly acceptedTaints: readonly ProvenanceTaint[];
		readonly artifacts?: ReadonlyMap<Sha256Digest, VerifiedArtifactClosure>;
	};
}
export type ProcessReuseMissReason =
	| "no_candidate_pathset"
	| "certificate_tainted"
	| "producer_guarantee_incompatible"
	| "dependency_changed"
	| "validation_indeterminate"
	| "artifact_missing"
	| "artifact_unreadable"
	| "observation_contract_incompatible";
export interface ProcessReuseLookupMetrics {
	readonly candidateCertificates: number;
	readonly eligibleCertificates: number;
	readonly pathsetsValidated: number;
	readonly filesRead: number;
	readonly bytesRead: number;
	readonly artifactsLoaded: number;
	readonly artifactBytesRead: number;
}
interface CompletedProcessPlan {
	readonly kind: "completed_replay";
	readonly source: "live" | "l2";
	readonly weakKey: Sha256Digest;
	readonly certificate: ProcessProvenanceCertificate;
	readonly validation: ValidatedProcessInputs;
	readonly artifacts: VerifiedArtifactClosure;
	readonly lookup: ProcessReuseLookupMetrics;
}
export type ProcessReusePlan =
	| CompletedProcessPlan
	| {
			readonly kind: "miss";
			readonly weakKey: Sha256Digest;
			readonly reasons: readonly ProcessReuseMissReason[];
			readonly changedDependencies?: readonly string[];
			readonly lookup: ProcessReuseLookupMetrics;
	  };
/** BuildXL-style weak pathset lookup followed by eager current-world strong validation. */
export class ProcessReusePlanner {
	private readonly store: ProvenanceCertificateStore;
	constructor(options: { readonly store: ProvenanceCertificateStore }) {
		this.store = options.store;
	}
	plan(request: ProcessReuseRequest): Promise<ProcessReusePlan> {
		return this.store.read((signal) => this.lookup(request, signal), request.signal);
	}
	private async lookup(request: ProcessReuseRequest, signal: AbortSignal): Promise<ProcessReusePlan> {
		const metrics = {
			candidateCertificates: 0,
			eligibleCertificates: 0,
			pathsetsValidated: 0,
			filesRead: 0,
			bytesRead: 0,
			artifactsLoaded: 0,
			artifactBytesRead: 0,
		};
		const lookup = (): ProcessReuseLookupMetrics => Object.freeze({ ...metrics });
		const weakKey = request.weakKey;
		if (!isSha256Digest(weakKey)) throw new Error("invalid process weak key");
		const live = request.live
			? [request.live.certificate].flat().filter((candidate) => candidate.weakKey === weakKey)
			: [];
		const acceptedTaints = [
			...new Set([
				...(request.validation?.acceptedTaints ?? []),
				...(live.length ? request.live!.acceptedTaints : []),
			]),
		];
		const certificates = live.length
			? live
			: this.store.candidatesByWeakKey(weakKey, request.executablePath, request.excludedCertificates, signal);
		const reasons = new Set<ProcessReuseMissReason>();
		const changedDependencies = new Set<string>();
		const currentInputs = observeProcessInputs(weakKey, async (evidence) => {
			metrics.pathsetsValidated++;
			const observation = await validateDynamicDependencyCertificate(evidence, {
				...request.validation,
				acceptedTaints,
				signal,
			});
			metrics.filesRead += observation.filesRead;
			metrics.bytesRead += observation.bytesRead;
			if (observation.status === "stale")
				for (const changed of observation.changed) changedDependencies.add(changed);
			return observation;
		});
		for await (const certificate of certificates) {
			signal.throwIfAborted();
			metrics.candidateCertificates++;
			if (request.acceptProducer && !request.acceptProducer(certificate.producer)) {
				reasons.add("producer_guarantee_incompatible");
				continue;
			}
			if (!certificateReplayable(certificate, acceptedTaints)) {
				reasons.add("certificate_tainted");
				continue;
			}
			if (!contractCompatible(request.contract, certificate)) {
				reasons.add("observation_contract_incompatible");
				continue;
			}
			metrics.eligibleCertificates++;
			const validation = await currentInputs(certificate.dependencyCertificate);
			if (!validation) {
				reasons.add("validation_indeterminate");
				continue;
			}
			if (certificate.strongKey !== validation.strongKey) {
				reasons.add("dependency_changed");
				continue;
			}
			let artifacts: VerifiedArtifactClosure | undefined;
			try {
				artifacts = await this.store.loadArtifacts(
					referencedArtifacts(certificate),
					signal,
					request.live?.artifacts?.get(certificate.id),
				);
			} catch {
				reasons.add("artifact_unreadable");
				continue;
			}
			if (signal.aborted) {
				await artifacts?.dispose();
				signal.throwIfAborted();
			}
			if (!artifacts) {
				reasons.add("artifact_missing");
				continue;
			}
			metrics.artifactsLoaded += artifacts.artifacts;
			metrics.artifactBytesRead += artifacts.bytes;
			return {
				kind: "completed_replay",
				source: live.length ? "live" : "l2",
				weakKey,
				certificate,
				validation,
				artifacts,
				lookup: lookup(),
			};
		}
		signal.throwIfAborted();
		return {
			kind: "miss",
			weakKey,
			reasons: Object.freeze(reasons.size ? [...reasons] : ["no_candidate_pathset"]),
			...(changedDependencies.size
				? {
						changedDependencies: Object.freeze([...changedDependencies].sort()),
					}
				: {}),
			lookup: lookup(),
		};
	}
	/** Publish unmatched replayable executions so useful work survives branch discard. */
	async publishCompleted(
		certificate: ProcessProvenanceCertificate,
		acceptedTaints: readonly ProvenanceTaint[] = [],
	): Promise<boolean> {
		return certificateReplayable(certificate, acceptedTaints) && this.store.put(certificate);
	}
}
function contractCompatible(
	contract: ReplayObservationContract,
	certificate: ProcessProvenanceCertificate,
): boolean {
	return (
		contract.orderedJournal &&
		contract.transactionalEffects &&
		contract.sink === "buffered" &&
		certificate.result.replayProfile === "buffered_noninteractive"
	);
}
/** One-shot children and their enclosing branch share adoption authority. */
export class ProcessHandoffOwnership {
	private children = 0;
	private claim?:
		| "partial"
		| {
				readonly apply: WeakRef<() => Promise<unknown>>;
				readonly result: Promise<unknown>;
				pending: boolean;
		  };
	private readonly observer?: WeakRef<(adoption: ExecutionOperationAdoption) => void>;
	private readonly scopeOwner?: WeakRef<(scope: ExecutionScope, salvage?: boolean) => boolean>;
	private readonly consumer?: ExecutionScope;
	constructor(
		observer?: (adoption: ExecutionOperationAdoption) => void,
		acceptScope?: (scope: ExecutionScope, salvage?: boolean) => boolean,
		consumer?: ExecutionScope,
	) {
		this.consumer = snapshotExecutionScope(consumer);
		if (observer) this.observer = new WeakRef(observer);
		if (acceptScope) this.scopeOwner = new WeakRef(acceptScope);
	}
	/** A live owner may transfer this computation beyond its production turn; age grants no reuse authority. */
	acceptsScope(
		producer: ExecutionScope | undefined,
		consumer: ExecutionScope | undefined,
		salvage = false,
	): boolean {
		if (!producer || !consumer || producer.sessionID !== consumer.sessionID) return false;
		if (this.consumer) return sameScope(this.consumer, consumer) && this.consumer.callID === consumer.callID;
		return this.scopeOwner
			? this.scopeOwner.deref()?.(consumer, salvage) === true
			: sameScope(producer, consumer);
	}
	/** Observational only; a retained certificate must not keep an expired runtime alive. */
	adopted(adoption: ExecutionOperationAdoption): void {
		this.observer?.deref()?.(adoption);
	}
	get wholeClaimed(): boolean {
		return typeof this.claim === "object";
	}
	get pendingWhole(): Promise<unknown> | undefined {
		return typeof this.claim === "object" && this.claim.pending ? this.claim.result : undefined;
	}
	claimChild(): ((consumed: boolean) => void) | undefined {
		if (this.wholeClaimed) return;
		this.children++;
		return releaseOnce((consumed: boolean) => {
			this.children--;
			if (consumed) this.claim = "partial";
		});
	}
	async commit<T>(apply: () => Promise<T>): Promise<T> {
		const previous = this.claim;
		if (typeof previous === "object" && previous.apply.deref() === apply)
			return previous.result as Promise<T>;
		if (previous || this.children)
			throw effectCommitFailure(
				new Error(
					previous === "partial"
						? "process execution was partially consumed"
						: "process execution was already claimed",
				),
				"recoverable",
			);
		const claim = {
			apply: new WeakRef(apply),
			pending: true,
			result: Promise.resolve()
				.then(apply)
				.catch((error) => {
					if (error instanceof EffectCommitFailure && error.disposition === "recoverable")
						this.claim = undefined;
					throw error;
				})
				.finally(() => {
					claim.pending = false;
				}),
		};
		this.claim = claim;
		return claim.result;
	}
}
export interface ProcessHandoff {
	/** The registry owns production; disposal revokes work as well as its lookup capability. */
	readonly signal: AbortSignal;
	readonly ownership: ProcessHandoffOwnership;
	readonly binding?: ProcessExecutionBinding;
	readonly computation?: TimelineInterval;
	readonly completion: Promise<void>;
	readonly scope: ExecutionScope | undefined;
	readonly startedAt: number;
	/** Advance owned work toward a result. False declines this join; true still needs a sealed, validated result. */
	readonly advance?: () => Promise<boolean>;
}
/** In-memory capability for another isolated execution, never a proof of result equivalence. */
export interface ProcessExecutionBinding {
	readonly key: Sha256Digest;
	/** Latest completed native observation, or the owned result's execution time. */
	readonly executionMs: number;
	readonly scope: ExecutionScope;
	readonly available: boolean;
}
type HandoffState =
	| {
			readonly status: "running";
			advance?: () => Promise<boolean>;
	  }
	| {
			readonly status: "completed" | "retained";
			readonly result?: {
				readonly artifacts: VerifiedArtifactClosure;
				readonly candidate: ProcessProvenanceCertificate;
				readonly complete: boolean;
				readonly executionMs: number;
				readonly bytes: number;
				readonly computation: TimelineInterval;
			};
	  };
type HandoffRecord = ReturnType<ProcessHandoffRegistry["reserve"]>;
export type ProcessHandoffAcquisition<Plan> =
	| {
			readonly kind: "hit";
			readonly plan: Plan;
			readonly joined: boolean;
			readonly producer?: ProcessHandoff;
			readonly waiting?: readonly TimelineInterval[];
			readonly settle?: (consumed: boolean, forward?: boolean) => void;
	  }
	| {
			readonly kind: "work";
			readonly work: ProcessHandoff;
			readonly joined: boolean;
	  }
	| {
			readonly kind: "miss";
			readonly joined: boolean;
	  };
export type ProcessHandoffLookup<Plan> = (
	candidates?: readonly ProcessProvenanceCertificate[],
	excludedCertificates?: ReadonlySet<Sha256Digest>,
	artifacts?: ReadonlyMap<Sha256Digest, VerifiedArtifactClosure>,
) => Promise<Plan | undefined>;
interface AcquireOptions<Plan> {
	readonly key: Sha256Digest;
	readonly scope?: ExecutionScope;
	readonly signal?: AbortSignal;
	readonly lookup: ProcessHandoffLookup<Plan>;
	readonly join?: boolean;
	readonly excludeHandoff?: (handoff: ProcessHandoff) => boolean;
	readonly producer?: {
		readonly ownership: ProcessHandoffOwnership;
		readonly executablePath: string;
	};
}
/** Owns completed process evidence and pending producers. */
export class ProcessHandoffRegistry<Invocation = never> {
	private readonly executions = new Map<ProcessHandoff, HandoffRecord>();
	private readonly byKey = new Map<Sha256Digest, readonly HandoffRecord[]>();
	private readonly waiters = new Map<Sha256Digest, Set<() => void>>();
	private readonly invocations = new WeakMap<
		ProcessExecutionBinding,
		{
			readonly value: Invocation;
			readonly bytes: number;
			executionMs: number;
		}
	>();
	private readonly store: ProvenanceCertificateStore;
	private maxCompleted: number;
	private maxRetainedBytes: number;
	private retainedBytes = 0;
	private completedCount = 0;
	private readonly lifetime = new RuntimeLifecycleLane();
	constructor(store: ProvenanceCertificateStore, maxRetainedBytes: number) {
		this.store = store;
		this.maxCompleted = store.limits.maxCertificates;
		this.maxRetainedBytes = maxRetainedBytes;
	}
	configure(maxCompleted: number, maxRetainedBytes = this.maxRetainedBytes): void {
		this.maxCompleted = maxCompleted;
		this.maxRetainedBytes = maxRetainedBytes;
		this.trim();
	}
	/** Keep secrets only with their existing handoff owner; the returned capability contains no raw arguments. */
	bind(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		invocation: Invocation,
	): ProcessExecutionBinding | undefined {
		const record = this.record(key, handoff);
		if (!record || record.state.status === "running" || !record.state.result?.complete) return;
		return this.retainBinding(key, record, invocation, record.state.result.executionMs);
	}
	/** A completed native launch teaches preparation only; it has no result or adoption authority. */
	observe(
		key: Sha256Digest,
		executablePath: string,
		scope: ExecutionScope,
		invocation: Invocation,
		executionMs: number,
	): ProcessExecutionBinding | undefined {
		if (this.lifetime.sealed || !Number.isFinite(executionMs) || executionMs < 0) return;
		const record = this.reserve(
			key,
			executablePath,
			new ProcessHandoffOwnership(),
			snapshotExecutionScope(scope),
		);
		record.state = { status: "retained" };
		this.completedCount++;
		record.settle();
		let binding: ProcessExecutionBinding | undefined;
		try {
			return (binding = this.retainBinding(key, record, invocation, executionMs));
		} finally {
			if (!binding) this.remove(record);
		}
	}
	private retainBinding(
		key: Sha256Digest,
		record: HandoffRecord,
		invocation: Invocation,
		executionMs: number,
	): ProcessExecutionBinding | undefined {
		if (!record.scope || record.binding || this.maxRetainedBytes <= 0) return;
		const value = immutableSnapshot(invocation);
		if (!isImmutableSnapshot(value)) return;
		const bytes = Buffer.byteLength(JSON.stringify(value));
		if (bytes > this.maxRetainedBytes) return;
		// Repeated native learning shares its launch capability; result owners remain distinct.
		if (record.state.status === "retained" && !record.state.result)
			for (const previous of this.executions.values()) {
				if (
					previous.key === key &&
					previous !== record &&
					previous.state.status === "retained" &&
					!previous.state.result &&
					previous.scope?.sessionID === record.scope.sessionID &&
					previous.executablePath === record.executablePath &&
					previous.binding &&
					stableEqual(this.invocations.get(previous.binding)?.value, value)
				) {
					this.invocations.get(previous.binding)!.executionMs = executionMs;
					this.remove(record);
					return previous.binding;
				}
			}
		const owner = new WeakRef(this.invocations);
		const binding = Object.freeze({
			key,
			scope: record.scope,
			get executionMs(): number {
				return owner.deref()?.get(this)?.executionMs ?? 0;
			},
			get available(): boolean {
				return owner.deref()?.has(this) ?? false;
			},
		});
		this.invocations.set(binding, { value, bytes, executionMs });
		record.binding = binding;
		this.retainedBytes += bytes;
		this.trim();
		return this.invocations.has(binding) ? binding : undefined;
	}
	/** Results of `key` this session produced, newest first: what a run of it could still reuse, never an adoption. */
	results(key: Sha256Digest, scope: ExecutionScope): readonly ProcessProvenanceCertificate[] {
		return [...this.candidates(key)]
			.reverse()
			.flatMap((record) =>
				record.state.status === "completed" &&
				record.state.result &&
				record.scope?.sessionID === scope.sessionID
					? record.state.result.candidate
					: [],
			);
	}
	bindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return Object.freeze(
			[...this.executions.values()].flatMap((record) =>
				record.binding?.scope.sessionID === scope.sessionID ? [record.binding] : [],
			),
		);
	}
	/** Copying a digest/descriptor cannot mint a capability. Revocation affects subsequent admissions. */
	resolveBinding(
		binding: ProcessExecutionBinding,
		scope: ExecutionScope | undefined,
	): Invocation | undefined {
		return binding.available && scope?.sessionID === binding.scope.sessionID
			? this.invocations.get(binding)?.value
			: undefined;
	}
	/** Conservative availability hint; scope, ownership and evidence still decide acquisition. */
	get hasResults(): boolean {
		for (const record of this.executions.values()) if (record.state.status !== "retained") return true;
		return false;
	}
	hasJoinableWork(ownership: ProcessHandoffOwnership): boolean {
		for (const record of this.executions.values())
			if (record.ownership === ownership && !record.signal.aborted && record.advance) return true;
		return false;
	}
	/** Retrieval hint for both running and completed records; it grants no adoption authority. */
	mayHaveExecutable(executablePath: string): boolean {
		for (const record of this.executions.values())
			if (record.state.status !== "retained" && record.executablePath === executablePath) return true;
		return false;
	}
	/** Running capabilities are observable state; completion or withdrawal revokes even a borrowed callback. */
	observeRunning(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		invoke: NonNullable<ProcessHandoff["advance"]>,
	): () => void {
		const record = this.record(key, handoff);
		const state = record?.state;
		if (!record || state?.status !== "running") return () => {};
		const owned = () =>
			record.state === state && state.advance === owned ? invoke() : Promise.resolve(false);
		state.advance = owned;
		this.wake(key);
		return () => {
			if (state.advance === owned) {
				state.advance = undefined;
				this.wake(key);
			}
		};
	}
	async acquire<
		Plan extends {
			readonly certificate: ProcessProvenanceCertificate;
			readonly artifacts?: VerifiedArtifactClosure;
		},
	>({ scope, producer, ...request }: AcquireOptions<Plan>): Promise<ProcessHandoffAcquisition<Plan>> {
		scope = snapshotExecutionScope(scope);
		const waits: {
			readonly handoff: ProcessHandoff;
			readonly interval: TimelineInterval;
		}[] = [];
		const considered = new Map<HandoffRecord, Sha256Digest>();
		let changed = new AbortController();
		let historyChecked = false;
		const wake = () => changed.abort();
		const waiters = this.waiters.get(request.key) ?? new Set<() => void>();
		waiters.add(wake);
		this.waiters.set(request.key, waiters);
		try {
			while (!this.lifetime.sealed && !request.signal?.aborted) {
				// One subscription spans the lookup; a change invalidates only this pass's negative evidence.
				if (changed.signal.aborted) {
					changed = new AbortController();
					historyChecked = false;
					considered.clear();
				}
				const stop = request.signal ? AbortSignal.any([changed.signal, request.signal]) : changed.signal;
				const records = this.candidates(request.key).filter((record) => !request.excludeHandoff?.(record));
				const completed = [...records].reverse().find((record) => {
					const state = record.state;
					return state.status === "completed" && state.result && !considered.has(record);
				});
				const state = completed?.state;
				const result = state?.status !== "running" ? state?.result : undefined;
				const candidate = result?.candidate;
				if (candidate || !historyChecked) {
					// Preserve live producer attribution without waiting for unfinished work ahead of history.
					historyChecked ||= !candidate;
					if (candidate) considered.set(completed!, candidate.id);
					const plan = candidate
						? await request.lookup([candidate], undefined, new Map([[candidate.id, result!.artifacts]]))
						: await request.lookup(undefined, new Set(considered.values()));
					if (!plan) continue;
					let unused = plan.artifacts;
					try {
						if (this.lifetime.sealed || request.signal?.aborted) break;
						if (
							candidate &&
							(!completed ||
								candidate !== plan.certificate ||
								completed.state !== state ||
								!this.executions.has(completed))
						)
							continue;
						const claim = completed && this.claim(completed);
						if (completed && !claim) continue;
						const hit: ProcessHandoffAcquisition<Plan> = {
							kind: "hit",
							plan,
							joined: waits.length > 0,
							...(completed
								? {
										producer: completed,
										waiting: waits.filter((wait) => wait.handoff === completed).map((wait) => wait.interval),
									}
								: {}),
							settle: () => {
								try {
									claim?.();
								} finally {
									void this.lifetime.release(plan.artifacts);
								}
							},
						};
						unused = undefined;
						return hit;
					} finally {
						if (unused) await unused.dispose();
					}
				}
				if (producer)
					return {
						kind: "work",
						work: this.reserve(request.key, producer.executablePath, producer.ownership, scope),
						joined: waits.length > 0,
					};
				if (request.join === false) break;
				// Each producer advances independently; readiness never grants transfer authority.
				await Promise.allSettled([
					...records
						.filter(
							(record) =>
								record.ownership.acceptsScope(record.scope, scope, true) &&
								(record.state.status === "running" || record.ownership.pendingWhole),
						)
						.map(async (record) => {
							const running = record.state.status === "running";
							const startedAt = performance.now();
							const ready = running
								? (record.advance?.() ?? Promise.resolve(true)).then(
										(advanced) => advanced && record.completion.then(() => true),
									)
								: (record.ownership.pendingWhole?.then(
										() => true,
										() => true,
									) ?? new Promise<never>(() => {}));
							const advanced = await waitForCompletion(ready, stop);
							if (advanced.status === "completed" && !advanced.value) return;
							waits.push({
								handoff: record,
								interval: new TimelineInterval(startedAt, performance.now()),
							});
							changed.abort();
						}),
				]);
				if (!stop.aborted) break;
			}
			return { kind: "miss", joined: waits.length > 0 };
		} finally {
			waiters.delete(wake);
			if (!waiters.size) this.waiters.delete(request.key);
			changed.abort();
		}
	}
	private claim(record: HandoffRecord): (() => void) | undefined {
		if (record.state.status !== "completed") return;
		record.borrowers++;
		return releaseOnce(() => {
			record.borrowers--;
			this.trim();
		});
	}
	/** Register the producer's retained versions; extending their lifetime into history is an explicit store operation. */
	async publish(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		input: ProcessCertificateInput,
		computation?: TimelineInterval,
		source?: ArtifactSource,
		signal = handoff.signal,
	): Promise<boolean> {
		if (this.record(key, handoff)?.state.status !== "running")
			throw new Error("process handoff is no longer running");
		if (!certificateReplayable(input, speculativeConfig().processReuse.replayAcceptedTaints)) {
			this.complete(key, handoff);
			return false;
		}
		computation ??= new TimelineInterval(handoff.startedAt, performance.now());
		const candidate = sealProcessCertificate({
			...input,
			result: {
				...input.result,
				computation: TimelineInterval.serialize(computation),
			},
		});
		const metadata = Buffer.byteLength(JSON.stringify(candidate));
		const artifacts = metadata <= this.maxRetainedBytes
			? await this.store.loadArtifacts(
					referencedArtifacts(candidate),
					AbortSignal.any([handoff.signal, signal]),
					source,
				)
			: undefined;
		const bytes = metadata + (artifacts?.memoryBytes ?? 0);
		if (!artifacts || bytes > this.maxRetainedBytes) {
			if (artifacts) await artifacts.dispose();
			this.complete(key, handoff);
			return false;
		}
		if (
			!this.complete(key, handoff, {
				artifacts,
				bytes,
				computation,
				candidate,
				complete: candidate.dependencyCertificate.complete,
				executionMs: candidate.result.observedProcessMs ?? 0,
			})
		) {
			await artifacts.dispose();
			throw new Error("process handoff has no complete owned result");
		}
		return true;
	}
	complete(
		key: Sha256Digest,
		handoff: ProcessHandoff,
		result?: Extract<
			HandoffState,
			{
				status: "completed" | "retained";
			}
		>["result"],
	): boolean {
		const record = this.record(key, handoff);
		if (!record || record.state.status !== "running") return false;
		this.retainedBytes += result?.bytes ?? 0;
		record.state = { status: "completed", result };
		this.completedCount++;
		record.settle();
		this.wake(key);
		if (!result || !record.scope) this.remove(record);
		else this.trim();
		return true;
	}
	clearCompleted(): void {
		for (const record of this.executions.values()) if (record.state.status !== "running") this.remove(record);
	}
	dispose(): Promise<void> {
		if (this.lifetime.sealed) return this.lifetime.dispose();
		const closing = this.lifetime.dispose();
		for (const key of this.waiters.keys()) this.wake(key);
		for (const record of this.executions.values()) {
			if (record.state.status === "running") record.controller.abort(new Error("process handoff disposed"));
			if (!this.complete(record.key, record)) this.remove(record);
		}
		this.completedCount = 0;
		return closing;
	}
	private reserve(
		key: Sha256Digest,
		executablePath: string,
		ownership: ProcessHandoffOwnership,
		scope?: ExecutionScope,
	) {
		if (this.lifetime.sealed) throw new Error("process handoff registry is disposed");
		const { promise: completion, resolve: settle } = Promise.withResolvers<void>();
		const controller: AbortController = new AbortController();
		const record = {
			key,
			completion,
			executablePath,
			scope,
			ownership,
			settle,
			startedAt: performance.now(),
			state: { status: "running" } as HandoffState,
			controller,
			signal: controller.signal as AbortSignal,
			borrowers: 0,
			binding: undefined as ProcessExecutionBinding | undefined,
			get advance(): ProcessHandoff["advance"] {
				return record.state.status === "running" ? record.state.advance : undefined;
			},
			get computation(): TimelineInterval | undefined {
				return record.state.status !== "running" ? record.state.result?.computation : undefined;
			},
		};
		this.executions.set(record, record);
		this.byKey.delete(key);
		return record;
	}
	private record(key: Sha256Digest, handoff: ProcessHandoff): HandoffRecord | undefined {
		const record = this.executions.get(handoff);
		return record?.key === key ? record : undefined;
	}
	/** Queries index existing executions; publication and release never create another lifetime. */
	private candidates(key: Sha256Digest): readonly HandoffRecord[] {
		const records =
			this.byKey.get(key) ?? [...this.executions.values()].filter((record) => record.key === key);
		if (records.length) this.byKey.set(key, records);
		return records;
	}
	private wake(key: Sha256Digest): void {
		for (const waiter of this.waiters.get(key) ?? []) waiter();
	}
	private remove(record: HandoffRecord): void {
		this.releaseResult(record);
		if (record.binding) {
			this.retainedBytes -= this.invocations.get(record.binding)?.bytes ?? 0;
			this.invocations.delete(record.binding);
			record.binding = undefined;
		}
		if (this.executions.delete(record)) {
			if (record.state.status !== "running") this.completedCount--;
			this.wake(record.key);
		}
		this.byKey.delete(record.key);
	}
	private trim(limit = this.maxCompleted): void {
		const records = [...this.executions.values()];
		let stored = records.reduce(
			(sum, record) =>
				sum + (record.state.status === "running" ? 0 : (record.state.result?.artifacts.bytes ?? 0)),
			0,
		);
		for (const record of records) {
			if (
				this.completedCount <= limit &&
				this.retainedBytes <= this.maxRetainedBytes &&
				stored <= this.store.limits.maxBytes
			)
				break;
			if (!record.borrowers && record.state.status !== "running") {
				stored -= record.state.result?.artifacts.bytes ?? 0;
				this.remove(record);
			}
		}
	}
	private releaseResult(record: HandoffRecord): void {
		if (record.state.status !== "running") {
			this.retainedBytes -= record.state.result?.bytes ?? 0;
			void this.lifetime.release(record.state.result?.artifacts);
			record.state = { status: "retained" };
		}
	}
}
export function sameScope(left: ExecutionScope | undefined, right: ExecutionScope | undefined): boolean {
	return Boolean(left && right && left.sessionID === right.sessionID && left.turnID === right.turnID);
}
/** Provenance, planning, and content-addressed storage without any Pi host dependency. */
export {
	certificateReplayable,
	createExecPrototype,
	dependencyPathsetKey,
	parseProcessCertificate,
	processStrongKey,
	processWeakKey,
	referencedArtifacts,
	sealProcessCertificate,
	type ArtifactReference,
	type DependencyRole,
	type DynamicDependency,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type ExitOutcome,
	type InheritedFileDescriptor,
	type OrderedEffectEvent,
	type ProcessProducerProof,
	type ProcessPrototypeInput,
	type ProcessProvenanceCertificate,
	type ProcessResultRecord,
	type ProvenanceTaint,
	type SemanticEnvironmentEntry,
	type WorkspaceEffectState,
} from "./provenance-certificate.ts";
export { digestObject, isSha256Digest, sha256Digest, type Sha256Digest } from "./primitives.ts";
export * from "./reuse-store.ts";
