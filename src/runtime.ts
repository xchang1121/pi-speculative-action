import { nonNegativeFinite } from "./number-utils.ts";
import type { SpeculativeCandidate } from "./runtime-contracts.ts";

export { makeSpeculativeActionRuntime } from "./runtime-engine.ts";
export { candidateToolNames } from "./common.ts";
export { diagnosticAction, diagnosticJson, redactDiagnostics } from "./diagnostics.ts";
export type * from "./runtime-contracts.ts";

export function candidateExecutionMs(candidate: SpeculativeCandidate): number {
	return nonNegativeFinite(candidate.work?.execution?.executionMs);
}
