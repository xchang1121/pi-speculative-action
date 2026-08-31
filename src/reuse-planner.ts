import {
	certificateReplayable,
	dependencyPathsetKey,
	type DynamicDependencyCertificate,
	type ExecPrototype,
	type OrderedEffectEvent,
	processStrongKey,
	processWeakKey,
	type ProcessProvenanceCertificate,
	type Sha256Digest,
} from "./provenance-certificate.ts";
import {
	type ProvenanceValidation,
	type ProvenanceValidationContext,
	validateDynamicDependencyCertificate,
} from "./provenance-validation.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";

export interface MemoryReuseProvider<Hit> {
	/** Optional adapter over the Runtime-owned L1 ResultCache; the planner never owns a second L1. */
	readonly lookup: (weakKey: Sha256Digest) => Hit | undefined | Promise<Hit | undefined>;
}

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
}

export type ProcessReuseMissReason =
	| "no_candidate_pathset"
	| "certificate_tainted"
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
	readonly durationMs: number;
}

export type ProcessReusePlan<MemoryHit = never> =
	| {
			readonly kind: "memory_hit";
			readonly source: "l1";
			readonly weakKey: Sha256Digest;
			readonly hit: MemoryHit;
			readonly lookup: ProcessReuseLookupMetrics;
	  }
	| {
			readonly kind: "completed_replay";
			readonly source: "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
			readonly lookup: ProcessReuseLookupMetrics;
	  }
	| {
			readonly kind: "artifact_seed";
			readonly source: "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly effects: readonly Exclude<OrderedEffectEvent, { kind: "output" }>[];
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
			readonly lookup: ProcessReuseLookupMetrics;
	  }
	| {
			readonly kind: "miss";
			readonly weakKey: Sha256Digest;
			readonly reasons: readonly ProcessReuseMissReason[];
			readonly lookup: ProcessReuseLookupMetrics;
	  };

/** BuildXL-style weak pathset lookup followed by eager current-world strong validation. */
export class ProcessReusePlanner<MemoryHit = never> {
	private readonly store: ProvenanceCertificateStore;
	private readonly memory?: MemoryReuseProvider<MemoryHit>;

	constructor(options: {
		readonly store: ProvenanceCertificateStore;
		readonly memory?: MemoryReuseProvider<MemoryHit>;
	}) {
		this.store = options.store;
		this.memory = options.memory;
	}

	async plan(request: ProcessReuseRequest): Promise<ProcessReusePlan<MemoryHit>> {
		const startedAt = performance.now();
		let candidateCertificates = 0;
		let eligibleCertificates = 0;
		let pathsetsValidated = 0;
		let filesRead = 0;
		let bytesRead = 0;
		const lookup = (): ProcessReuseLookupMetrics =>
			Object.freeze({
				candidateCertificates,
				eligibleCertificates,
				pathsetsValidated,
				filesRead,
				bytesRead,
				durationMs: Math.max(0, performance.now() - startedAt),
			});
		const weakKey = processWeakKey(request.prototype);
		const memoryHit = await this.memory?.lookup(weakKey);
		if (memoryHit !== undefined) return { kind: "memory_hit", source: "l1", weakKey, hit: memoryHit, lookup: lookup() };

		const certificates = await this.store.findByWeakKey(weakKey);
		candidateCertificates = certificates.length;
		if (!certificates.length) {
			return { kind: "miss", weakKey, reasons: ["no_candidate_pathset"], lookup: lookup() };
		}
		const reasons = new Set<ProcessReuseMissReason>();
		const pathsets = new Map<Sha256Digest, ProcessProvenanceCertificate[]>();
		for (const certificate of certificates) {
			if (!certificateReplayable(certificate)) {
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
				request.validation,
			);
			filesRead += observation.filesRead;
			bytesRead += observation.bytesRead;
			if (observation.status === "indeterminate") {
				reasons.add("validation_indeterminate");
				continue;
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
				if (!(await artifactsAvailable(this.store, certificate))) {
					reasons.add("artifact_missing");
					continue;
				}
				if (request.contract.mode === "completed_replay") {
					return {
						kind: "completed_replay",
						source: "l2",
						weakKey,
						certificate,
						validation,
						lookup: lookup(),
					};
				}
				const accepted = new Set(request.contract.seed?.acceptedPaths ?? []);
				const effects = certificate.result.journal.filter(
					(event): event is Extract<OrderedEffectEvent, { kind: "write" }> =>
						event.kind === "write" && accepted.has(event.path),
				);
				if (!effects.length) {
					reasons.add("no_seedable_effects");
					continue;
				}
				return {
					kind: "artifact_seed",
					source: "l2",
					weakKey,
					certificate,
					effects,
					validation,
					lookup: lookup(),
				};
			}
		}
		return {
			kind: "miss",
			weakKey,
			reasons: Object.freeze(reasons.size ? [...reasons] : ["no_candidate_pathset"]),
			lookup: lookup(),
		};
	}

	/** Publish even unmatched speculative executions so useful work survives branch discard. */
	publishCompleted(certificate: ProcessProvenanceCertificate): Promise<void> {
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

async function artifactsAvailable(
	store: ProvenanceCertificateStore,
	certificate: ProcessProvenanceCertificate,
): Promise<boolean> {
	for (const event of certificate.result.journal) {
		if ((event.kind === "output" || event.kind === "write") && !(await store.artifacts.has(event.data))) return false;
	}
	return true;
}
