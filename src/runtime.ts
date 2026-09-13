import { nonNegativeFinite } from "./number-utils.ts";
import { makeStructuralSpeculativeActionRuntime } from "./runtime-engine.ts";
import type { SpeculativeCandidate } from "./runtime-contracts.ts";

export { candidateToolNames } from "./common.ts";
export { diagnosticAction, diagnosticJson, redactDiagnostics } from "./diagnostics.ts";
export type * from "./runtime-contracts.ts";

export const makeSpeculativeActionRuntime = makeStructuralSpeculativeActionRuntime;

export function candidateExecutionMs(candidate: SpeculativeCandidate): number {
	return nonNegativeFinite(candidate.work?.execution?.executionMs);
}
