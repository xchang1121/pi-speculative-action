import {
	certificateReplayable,
	type ExecPrototype,
	type OrderedEffectEvent,
	processWeakKey,
	type ProcessProvenanceCertificate,
	type Sha256Digest,
} from "./provenance-certificate.ts";
import {
	type ProvenanceValidation,
	type ProvenanceValidationContext,
	validateProcessCertificate,
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

export type ProcessReusePlan<MemoryHit = never> =
	| { readonly kind: "memory_hit"; readonly source: "l1"; readonly weakKey: Sha256Digest; readonly hit: MemoryHit }
	| {
			readonly kind: "completed_replay";
			readonly source: "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
	  }
	| {
			readonly kind: "artifact_seed";
			readonly source: "l2";
			readonly weakKey: Sha256Digest;
			readonly certificate: ProcessProvenanceCertificate;
			readonly effects: readonly Exclude<OrderedEffectEvent, { kind: "output" }>[];
			readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
	  }
	| {
			readonly kind: "miss";
			readonly weakKey: Sha256Digest;
			readonly reasons: readonly ProcessReuseMissReason[];
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
		const weakKey = processWeakKey(request.prototype);
		const memoryHit = await this.memory?.lookup(weakKey);
		if (memoryHit !== undefined) return { kind: "memory_hit", source: "l1", weakKey, hit: memoryHit };

		const certificates = await this.store.findByWeakKey(weakKey);
		if (!certificates.length) return { kind: "miss", weakKey, reasons: ["no_candidate_pathset"] };
		const reasons = new Set<ProcessReuseMissReason>();
		for (const certificate of certificates) {
			if (!certificateReplayable(certificate)) {
				reasons.add("certificate_tainted");
				continue;
			}
			if (!contractCompatible(request.contract, certificate)) {
				reasons.add("observation_contract_incompatible");
				continue;
			}
			const validation = await validateProcessCertificate(certificate, request.validation);
			if (validation.status === "stale") {
				reasons.add("dependency_changed");
				continue;
			}
			if (validation.status === "indeterminate") {
				reasons.add("validation_indeterminate");
				continue;
			}
			if (!(await artifactsAvailable(this.store, certificate))) {
				reasons.add("artifact_missing");
				continue;
			}
			if (request.contract.mode === "completed_replay") {
				return { kind: "completed_replay", source: "l2", weakKey, certificate, validation };
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
			return { kind: "artifact_seed", source: "l2", weakKey, certificate, effects, validation };
		}
		return {
			kind: "miss",
			weakKey,
			reasons: Object.freeze(reasons.size ? [...reasons] : ["no_candidate_pathset"]),
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
