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
export type * from "./execution-session.ts";
export { LocalExecutionSession, type LocalExecutionHooks } from "./runtime-engine.ts";
export type * from "./settlement.ts";
export {
	cause,
	diagnosticAction,
	diagnosticJson,
	redactDiagnostics,
	zeroValidationMetrics,
} from "./settlement.ts";
export * from "./task-timing.ts";
