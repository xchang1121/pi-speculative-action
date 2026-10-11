import { certificateReplayable, isSha256Digest, type ProcessProducerProof,
	type ProcessProvenanceCertificate, type ProvenanceTaint, referencedArtifacts, type Sha256Digest } from "./provenance-certificate.ts";
import { observeProcessInputs, type ProvenanceValidation, type ProvenanceValidationContext, validateDynamicDependencyCertificate } from "./provenance-validation.ts";
import { ProvenanceCertificateStore, type VerifiedArtifactClosure } from "./reuse-store.ts";

export interface ReplayObservationContract {
	readonly sink: "buffered" | "pipe" | "tty" | "interactive";
	readonly orderedJournal: boolean;
	readonly transactionalEffects: boolean;
	/** Only a held native exec can consume a same-scope continuation. */
	readonly continuation?: true;
}

export interface ProcessReuseRequest {
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
	};
}

export type ProcessReuseMissReason = "no_candidate_pathset" | "certificate_tainted" | "producer_guarantee_incompatible" | "dependency_changed"
	| "validation_indeterminate" | "artifact_missing" | "artifact_unreadable" | "observation_contract_incompatible";

export interface ProcessReuseLookupMetrics {
	readonly candidateCertificates: number; readonly eligibleCertificates: number; readonly pathsetsValidated: number;
	readonly filesRead: number; readonly bytesRead: number;
	readonly artifactsLoaded: number; readonly artifactBytesRead: number;
}

interface ReadyProcessPlan {
	readonly source: "live" | "l2"; readonly weakKey: Sha256Digest; readonly certificate: ProcessProvenanceCertificate;
	readonly validation: Extract<ProvenanceValidation, { status: "valid" }>;
	readonly artifacts: VerifiedArtifactClosure; readonly lookup: ProcessReuseLookupMetrics;
}
export type ProcessReusePlan =
	| (ReadyProcessPlan & { readonly kind: "completed_replay" })
	| (ReadyProcessPlan & { readonly kind: "running_resume" })
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

	constructor(options: { readonly store: ProvenanceCertificateStore }) { this.store = options.store; }

	async plan(request: ProcessReuseRequest): Promise<ProcessReusePlan> {
		const metrics = { candidateCertificates: 0, eligibleCertificates: 0, pathsetsValidated: 0, filesRead: 0, bytesRead: 0, artifactsLoaded: 0, artifactBytesRead: 0 };
		const lookup = (): ProcessReuseLookupMetrics => Object.freeze({ ...metrics });
		const weakKey = request.weakKey;
		if (!isSha256Digest(weakKey)) throw new Error("invalid process weak key");
		const live = request.live ? [request.live.certificate].flat().filter(candidate => candidate.weakKey === weakKey) : [];
		const acceptedTaints = [...new Set([...(request.validation?.acceptedTaints ?? []), ...(live.length ? request.live!.acceptedTaints : [])])];
		const certificates = live.length ? live : this.store.candidatesByWeakKey(weakKey, request.executablePath, request.excludedCertificates);
		const reasons = new Set<ProcessReuseMissReason>(), changedDependencies = new Set<string>();
		const currentInputs = observeProcessInputs(weakKey, async evidence => {
			metrics.pathsetsValidated++;
			const observation = await validateDynamicDependencyCertificate(evidence, { ...request.validation, acceptedTaints });
			metrics.filesRead += observation.filesRead; metrics.bytesRead += observation.bytesRead;
			if (observation.status === "stale") for (const changed of observation.changed) changedDependencies.add(changed);
			return observation;
		});
		for await (const certificate of certificates) {
			metrics.candidateCertificates++;
			if (request.acceptProducer && !request.acceptProducer(certificate.producer)) { reasons.add("producer_guarantee_incompatible"); continue; }
			if (!certificateReplayable(certificate, acceptedTaints)) { reasons.add("certificate_tainted"); continue; }
			if (!contractCompatible(request.contract, certificate) || certificate.result.continuation && !live.includes(certificate)) {
				reasons.add("observation_contract_incompatible");
				continue;
			}
			metrics.eligibleCertificates++;
			const validation = await currentInputs(certificate.dependencyCertificate);
			if (!validation) { reasons.add("validation_indeterminate"); continue; }
			if (certificate.strongKey !== validation.strongKey) { reasons.add("dependency_changed"); continue; }
			let artifacts: VerifiedArtifactClosure | undefined;
			try { artifacts = await this.store.artifacts.load(referencedArtifacts(certificate)); }
			catch { reasons.add("artifact_unreadable"); continue; }
			if (!artifacts) { reasons.add("artifact_missing"); continue; }
			metrics.artifactsLoaded += artifacts.artifacts; metrics.artifactBytesRead += artifacts.bytes;
			const kind = certificate.result.continuation ? "running_resume" : "completed_replay";
			return { kind, source: live.length ? "live" : "l2", weakKey, certificate, validation, artifacts, lookup: lookup() };
		}
		return { kind: "miss", weakKey, reasons: Object.freeze(reasons.size ? [...reasons] : ["no_candidate_pathset"]),
			...(changedDependencies.size ? { changedDependencies: Object.freeze([...changedDependencies].sort()) } : {}), lookup: lookup() };
	}

	/** Publish unmatched replayable executions so useful work survives branch discard. */
	async publishCompleted(certificate: ProcessProvenanceCertificate, acceptedTaints: readonly ProvenanceTaint[] = []): Promise<boolean> {
		return !certificate.result.continuation && certificateReplayable(certificate, acceptedTaints) && this.store.put(certificate);
	}
}

function contractCompatible(contract: ReplayObservationContract, certificate: ProcessProvenanceCertificate): boolean {
	return contract.orderedJournal && contract.transactionalEffects && contract.sink === "buffered" && certificate.result.replayProfile === "buffered_noninteractive" &&
		(!certificate.result.continuation || contract.continuation === true && !certificate.dependencyCertificate.taints.some(
			taint => taint === "pid_observation" || taint === "descriptor_observation"));
}
