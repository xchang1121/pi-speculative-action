import { nonNegativeFinite } from "./number-utils.ts";
import { type ActionSemanticsRegistry, PI_ACTION_SEMANTICS } from "./action-semantics.ts";
import { makeStructuralSpeculativeActionRuntime } from "./runtime-engine.ts";
import type { SpeculativeActionSettings, SpeculativeCandidate } from "./runtime-contracts.ts";

export { diagnosticAction, diagnosticJson, redactDiagnostics } from "./diagnostics.ts";
export type * from "./runtime-contracts.ts";

export const makeSpeculativeActionRuntime = makeStructuralSpeculativeActionRuntime;

export function candidateToolNames(
	settings: SpeculativeActionSettings,
	semantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): readonly string[] {
	const known = new Set(semantics.toolNames());
	return [...new Set(settings.tools)].filter((tool) => known.has(tool));
}

export function candidateExecutionMs(candidate: SpeculativeCandidate): number {
	return nonNegativeFinite(candidate.work?.execution?.executionMs);
}
