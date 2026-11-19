/** Host-neutral speculative runtime surface. Pi adapters live in the root and extension entries. */

export {
	ActionSemanticsRegistry,
	PI_ACTION_SEMANTICS,
	actionKeyMatch,
	buildActionKey,
	type ActionEffect,
	type ActionKey,
	type ActionKeyMatch,
	type ActionSemanticsDefinition,
	type CanonicalAction,
	type ExactActionKeyMatch,
	type ResourceDependencyScope,
	type ResourceInputMatch,
} from "./action-semantics.ts";
export {
	SCHEDULING_DEFAULTS,
	normalizeSchedulingSettings,
	type SchedulingSettings,
} from "./config-schema.ts";
export type { DrafterToolDefinition } from "./config-schema.ts";
export type { ContentChunkLimits } from "./content-chunks.ts";
export * from "./execution-world.ts";
export {
	ResourceVersionManager,
	captureResourceVersion,
	releaseResourceVersion,
	resourceDependencies,
	validateResourceVersion,
	type ResourceChangeSet,
	type ResourceDependency,
	type ResourceInputPersistence,
	type ResourceValidationMetrics,
	type ResourceVersionToken,
	type ResourceVersionValidation,
} from "./resource-version.ts";
export {
	diagnosticAction,
	diagnosticJson,
	makeSpeculativeActionRuntime,
	redactDiagnostics,
	type ActorActionFeedback,
	type ActorObservation,
	type AuthoritativeResultCapture,
	type CandidatePreflight,
	type MaterializedActorAction,
	type MaterializedSpeculativeCandidate,
	type PredictionFeedback,
	type RuntimeEventMap,
	type RuntimeTurnContext,
	type SpeculativeActionRuntime,
	type SpeculativeActionRuntimeAdapter,
	type SpeculativeActionSettings,
	type SpeculativeCandidate,
	type SpeculativeDraftCandidate,
	type SpeculativePlanSource,
} from "./runtime-engine.ts";
export type * from "./settlement.ts";
export { cause, zeroValidationMetrics } from "./settlement.ts";
export * from "./task-timing.ts";
