import type { PredictionAdoption, PredictionSettlement, ResolutionStage } from "../src/settlement.ts";

function prediction() {
	return { id: "prediction", source: "pattern_aware", proposalID: "proposal", actionID: "action" };
}

function observedSettlement(adoption?: PredictionAdoption): PredictionSettlement {
	return {
		prediction: prediction(),
		observation: "observed",
		actorAction: { id: "actor", sequence: 1, turnID: "turn" },
		...(adoption
			? { match: { matched: true, relation: { kind: "exact", distance: 0 }, adoption } }
			: { match: { matched: false } }),
	};
}

export function unobservedSettlement(stage: ResolutionStage, code: string): PredictionSettlement {
	return { prediction: prediction(), observation: "unobserved", cause: { stage, code } };
}

export function unmatchedSettlement(): PredictionSettlement {
	return observedSettlement();
}

export function rejectedSettlement(stage: ResolutionStage, code: string): PredictionSettlement {
	return observedSettlement({ status: "rejected", candidateID: "candidate", cause: { stage, code } });
}

export function adoptedSettlement(): PredictionSettlement {
	return observedSettlement({ status: "adopted", candidateID: "candidate" });
}
