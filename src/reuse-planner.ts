import {
	certificateReplayable,
	dependencyPathsetKey,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type OrderedEffectEvent,
	type ProcessProducerProof,
	processStrongKey,
	processWeakKey,
	type ProcessProvenanceCertificate,
	type ProvenanceTaint,
	referencedArtifacts,
	type Sha256Digest,
} from "./provenance-certificate.ts";
import {
	type ProvenanceValidation,
	type ProvenanceValidationContext,
	validateDynamicDependencyCertificate,
} from "./provenance-validation.ts";
import { ProvenanceCertificateStore, type VerifiedArtifactClosure } from "./reuse-store.ts";

export interface ReplayObservationContract {
	readonly sink: "buffered" | "pipe" | "tty" | "interactive";
	readonly orderedJournal: boolean;
	readonly transactionalEffects: boolean;
	readonly mode: "completed_replay" | "artifact_seed";
	/** Explicit consumer contract required before file artifacts may be seeded into a later execution. */
	readonly seed?: {
		readonly acceptedPaths: readonly string[];
		readonly preconditionsValidated: true;
	};
}

export interface ProcessReuseRequest {
	readonly prototype: ExecPrototype;
	readonly contract: ReplayObservationContract;
	readonly validation?: ProvenanceValidationContext;
	/** Optional host policy for accepting proof produced under a different execution authority. */
	readonly acceptProducer?: (proof: ProcessProducerProof) => boolean;
	/** One already-reserved running result; accepted taints are never written into persistent history. */
	readonly live?: {
		readonly certificate: ProcessProvenanceCertificate;
		readonly acceptedTaints: readonly ProvenanceTaint[];
	};
}

export type ProcessReuseMissReason =
	| "no_candidate_pathset"
	| "certificate_tainted"
	| "producer_guarantee_incompatible"
	| "dependency_changed"
	| "validation_indeterminate"
	| "artifact_missing"
	| "observation_contract_incompatible"
	| "no_seedable_effects";

export interface ProcessReuseLookupMetrics {
	readonly candidateCertificates: number;
	readonly eligibleCertificates: number;
	readonly pathsetsValidated: number;
	readonly filesRead: number;
	readonly bytesRead: number;
	readonly artifactsLoaded: number;
	readonly artifactBytesRead: number;
	readonly durationMs: number;
}

export type ProcessReusePlan =
	| {
			readonly kind: "completed_replay";
			readonly source: "live" | "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
			readonly artifacts: VerifiedArtifactClosure;
			readonly lookup: ProcessReuseLookupMetrics;
	  }
	| {
			readonly kind: "artifact_seed";
			readonly source: "live" | "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly effects: readonly Extract<OrderedEffectEvent, { kind: "workspace" }>[];
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
			readonly artifacts: VerifiedArtifactClosure;
			readonly lookup: ProcessReuseLookupMetrics;
	  }
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

	async plan(request: ProcessReuseRequest): Promise<ProcessReusePlan> {
		const startedAt = performance.now();
		let candidateCertificates = 0;
		let eligibleCertificates = 0;
		let pathsetsValidated = 0;
		let filesRead = 0;
		let bytesRead = 0;
		let artifactsLoaded = 0;
		let artifactBytesRead = 0;
		const lookup = (): ProcessReuseLookupMetrics =>
			Object.freeze({
				candidateCertificates,
				eligibleCertificates,
				pathsetsValidated,
				filesRead,
				bytesRead,
				artifactsLoaded,
				artifactBytesRead,
				durationMs: Math.max(0, performance.now() - startedAt),
			});
		const weakKey = processWeakKey(request.prototype);
		const live = request.live?.certificate.weakKey === weakKey ? request.live : undefined;
		const acceptedTaints = [...new Set([
			...(request.validation?.acceptedTaints ?? []),
			...(live?.acceptedTaints ?? []),
		])];
		const certificates = live ? [live.certificate] : await this.store.findByWeakKey(weakKey);
		candidateCertificates = certificates.length;
		if (!certificates.length) {
			return { kind: "miss", weakKey, reasons: ["no_candidate_pathset"], lookup: lookup() };
		}
		const reasons = new Set<ProcessReuseMissReason>();
		const changedDependencies = new Set<string>();
		const pathsets = new Map<Sha256Digest, ProcessProvenanceCertificate[]>();
		for (const certificate of certificates) {
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
			eligibleCertificates++;
			const pathset = dependencyPathsetKey(certificate.dependencyCertificate);
			const grouped = pathsets.get(pathset);
			if (grouped) grouped.push(certificate);
			else pathsets.set(pathset, [certificate]);
		}

		for (const grouped of pathsets.values()) {
			const representative = grouped[0]!;
			pathsetsValidated++;
			const observation = await validateDynamicDependencyCertificate(
				representative.dependencyCertificate,
				{ ...request.validation, acceptedTaints },
			);
			filesRead += observation.filesRead;
			bytesRead += observation.bytesRead;
			if (observation.status === "indeterminate") {
				reasons.add("validation_indeterminate");
				continue;
			}
			if (observation.status === "stale") {
				for (const changed of observation.changed) changedDependencies.add(changed);
			}
			const current: DynamicDependencyCertificate = {
				complete: true,
				dependencies: observation.dependencies,
				taints: [],
			};
			const strongKey = processStrongKey(weakKey, current);
			const matching = grouped.filter((certificate) => certificate.strongKey === strongKey);
			if (!matching.length) {
				reasons.add("dependency_changed");
				continue;
			}
			const validation: Extract<ProvenanceValidation, { status: "valid" }> = {
				status: "valid",
				strongKey,
				dependencies: observation.dependencies,
				filesRead: observation.filesRead,
				bytesRead: observation.bytesRead,
				durationMs: observation.durationMs,
			};
			for (const certificate of matching) {
				const artifacts = await this.store.artifacts.load(referencedArtifacts(certificate));
				if (!artifacts) {
					reasons.add("artifact_missing");
					continue;
				}
				artifactsLoaded += artifacts.artifacts;
				artifactBytesRead += artifacts.bytes;
				if (request.contract.mode === "completed_replay") {
					return {
						kind: "completed_replay",
						source: live ? "live" : "l2",
						weakKey,
						certificate,
						validation,
						artifacts,
						lookup: lookup(),
					};
				}
				const accepted = new Set(request.contract.seed?.acceptedPaths ?? []);
				const effects = certificate.result.journal.filter(
					(event): event is Extract<OrderedEffectEvent, { kind: "workspace" }> =>
						event.kind === "workspace" && event.after.kind === "file" && accepted.has(event.path),
				);
				if (!effects.length) {
					reasons.add("no_seedable_effects");
					continue;
				}
				return {
					kind: "artifact_seed",
					source: live ? "live" : "l2",
					weakKey,
					certificate,
					effects,
					validation,
					artifacts,
					lookup: lookup(),
				};
			}
		}
		return {
			kind: "miss",
			weakKey,
			reasons: Object.freeze(reasons.size ? [...reasons] : ["no_candidate_pathset"]),
			...(changedDependencies.size
				? { changedDependencies: Object.freeze([...changedDependencies].sort()) }
				: {}),
			lookup: lookup(),
		};
	}

	/** Publish unmatched replayable executions so useful work survives branch discard. */
	async publishCompleted(
		certificate: ProcessProvenanceCertificate,
		acceptedTaints: readonly ProvenanceTaint[] = [],
	): Promise<boolean> {
		if (!certificateReplayable(certificate, acceptedTaints)) return false;
		return this.store.put(certificate);
	}
}

function contractCompatible(
	contract: ReplayObservationContract,
	certificate: ProcessProvenanceCertificate,
): boolean {
	if (!contract.orderedJournal || !contract.transactionalEffects) return false;
	if (contract.sink !== "buffered") return false;
	if (
		contract.mode === "artifact_seed" &&
		(!contract.seed?.preconditionsValidated || contract.seed.acceptedPaths.length === 0)
	) {
		return false;
	}
	return certificate.result.replayProfile === "buffered_noninteractive";
}
